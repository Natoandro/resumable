use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use resumable::{
    Engine, EngineConfig, ExecutionId, JsonCodec, NewExecution, OperationKey, RunError, Store,
    Timestamp, ValueCodec,
};
use resumable_store_postgres_sqlx::{PostgresStore, migrate, validate_schema};
use resumable_worker::{
    ExecutionRunner, RunnableExecution, RunnableSource, WorkerFuture, WorkerSettings, start,
};
use sqlx::PgPool;
use tokio::{sync::Notify, time::timeout};

struct PostgresSource(PostgresStore);

impl RunnableSource for PostgresSource {
    fn unsupported_operations<'a>(
        &'a self,
        supported: &'a [OperationKey],
    ) -> WorkerFuture<'a, Result<Vec<OperationKey>, String>> {
        Box::pin(async move {
            self.0
                .unsupported_runnable_operations(supported)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn runnable_executions<'a>(
        &'a self,
        supported: &'a [OperationKey],
        limit: u32,
    ) -> WorkerFuture<'a, Result<Vec<RunnableExecution>, String>> {
        Box::pin(async move {
            let candidates = self
                .0
                .runnable_executions(supported, limit)
                .await
                .map_err(|error| error.to_string())?;
            Ok(candidates
                .into_iter()
                .map(|candidate| RunnableExecution {
                    id: candidate.id,
                    operation: candidate.operation,
                })
                .collect())
        })
    }
}

struct Runner {
    engine: Engine<PostgresStore>,
    operation: OperationKey,
    expected_id: ExecutionId,
    interrupt: bool,
    completed: Notify,
}

impl ExecutionRunner for Runner {
    fn supported_operations(&self) -> WorkerFuture<'_, Vec<OperationKey>> {
        Box::pin(async { vec![self.operation.clone()] })
    }

    fn run(
        &self,
        id: ExecutionId,
        operation: OperationKey,
    ) -> WorkerFuture<'_, Result<(), RunError>> {
        Box::pin(async move {
            if operation != self.operation {
                return Err(RunError::OperationConflict);
            }

            let result = self
                .engine
                .resume_with_context(id, &operation, |input: u32, ctx| async move {
                    let doubled = ctx
                        .step("double", |_| async move {
                            assert!(self.interrupt || id != self.expected_id, "double ran again");
                            println!("Running double");
                            Ok::<_, &str>(input * 2)
                        })
                        .await?;
                    ctx.step("increment", |_| async move {
                        if self.interrupt && id == self.expected_id {
                            eprintln!("Exiting after double committed, during increment");
                            std::process::exit(42);
                        }
                        println!("Running increment");
                        Ok::<_, &str>(doubled + 1)
                    })
                    .await
                })
                .await?;

            if id == self.expected_id {
                assert_eq!(result, 43);
                println!("Execution {id} completed with result {result}");
                self.completed.notify_one();
            }
            Ok(())
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    let [_, mode, id] = args.as_slice() else {
        return Err("usage: postgres_worker <interrupt|resume> <execution-uuid>".into());
    };
    let interrupt = match mode.as_str() {
        "interrupt" => true,
        "resume" => false,
        _ => return Err("mode must be interrupt or resume".into()),
    };
    let id = ExecutionId::from_uuid(id.parse()?);
    let pool = PgPool::connect(&std::env::var("RESUMABLE_TEST_DATABASE_URL")?).await?;
    migrate(&pool).await?;
    validate_schema(&pool).await?;
    let store = PostgresStore::new(pool);
    let operation = OperationKey::new("walkthrough.postgres-worker", 1)?;

    if interrupt {
        let created_at = Timestamp(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_millis()
                .try_into()?,
        );
        store
            .create_execution(
                NewExecution::new(id, operation.clone(), created_at)
                    .with_input(JsonCodec.encode(&21_u32)?),
            )
            .await?;
    }

    let runner = Arc::new(Runner {
        engine: Engine::with_config(
            store.clone(),
            JsonCodec,
            EngineConfig {
                lease_duration: Duration::from_secs(3),
                heartbeat_interval: Duration::from_secs(1),
                ..EngineConfig::default()
            },
        ),
        operation,
        expected_id: id,
        interrupt,
        completed: Notify::new(),
    });
    let worker = start(
        Arc::new(PostgresSource(store)),
        runner.clone(),
        WorkerSettings {
            enabled: true,
            poll_interval: Duration::from_millis(100),
            ..WorkerSettings::default()
        },
    )
    .expect("worker settings should be valid");

    let completed = timeout(Duration::from_secs(20), runner.completed.notified()).await;
    worker.shutdown().await;
    completed?;
    Ok(())
}
