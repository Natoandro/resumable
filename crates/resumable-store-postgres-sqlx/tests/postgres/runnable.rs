use resumable::{
    ClaimOutcome, Engine, ExecutionId, FailStep, FailedExecutionPolicy, FailureClass,
    FailureRecord, JsonCodec, MutationId, NewExecution, OperationKey, RequestCancellation,
    RetryPolicyRecord, StepName, Store, Timestamp, ValueCodec, WorkerId,
};
use resumable_store_postgres_sqlx::{PostgresStore, SCHEMA_VERSION, validate_schema_version};
use resumable_worker::{
    ExecutionRunner, RunnableExecution, RunnableSource, WorkerFuture, WorkerSettings, start,
};
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;

pub(super) async fn verify_runnable_discovery(store: &PostgresStore, pool: &PgPool) {
    set_clock(pool, "2026-01-01 00:00:00+00").await;
    let operation_name = format!("fair-dispatch-{}", ExecutionId::new());
    let supported = OperationKey::new(&operation_name, 1).unwrap();
    let unsupported = OperationKey::new(&operation_name, 2).unwrap();
    let unknown_id = create(store, unsupported.clone(), 0, None).await;
    let execution_id = create(store, supported.clone(), 1, None).await;
    assert!(
        validate_schema_version(pool, SCHEMA_VERSION + 1)
            .await
            .is_err()
    );
    assert_eq!(
        store
            .load_execution(execution_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        resumable::ExecutionStatus::Pending
    );

    assert!(
        store
            .unsupported_runnable_operations(
                std::slice::from_ref(&supported),
                FailedExecutionPolicy::Reject
            )
            .await
            .unwrap()
            .contains(&unsupported)
    );
    assert_eq!(
        store
            .runnable_executions(
                std::slice::from_ref(&supported),
                FailedExecutionPolicy::Reject,
                1
            )
            .await
            .unwrap(),
        vec![resumable_store_postgres_sqlx::RunnableExecution {
            id: execution_id,
            operation: supported.clone(),
        }]
    );
    assert!(store.load_execution(unknown_id).await.unwrap().is_some());

    assert!(matches!(
        store
            .acquire_execution(execution_id, WorkerId::new(), MutationId::new(), 1_000)
            .await
            .unwrap(),
        ClaimOutcome::Claimed(_)
    ));
    assert!(
        store
            .runnable_executions(
                std::slice::from_ref(&supported),
                FailedExecutionPolicy::Reject,
                1
            )
            .await
            .unwrap()
            .is_empty()
    );

    set_clock(pool, "2026-01-01 00:00:01+00").await;
    assert_eq!(
        store
            .runnable_executions(
                std::slice::from_ref(&supported),
                FailedExecutionPolicy::Reject,
                1
            )
            .await
            .unwrap()
            .first()
            .map(|execution| execution.id),
        Some(execution_id)
    );
    assert!(matches!(
        store
            .acquire_execution(execution_id, WorkerId::new(), MutationId::new(), 1_000)
            .await
            .unwrap(),
        ClaimOutcome::Claimed(_)
    ));
    verify_scheduled_start(store, pool).await;
    verify_failed_execution_does_not_starve(store, pool).await;
    reset_clock(pool).await;
    verify_cancellation_recovery_through_worker(store).await;
}

async fn verify_failed_execution_does_not_starve(store: &PostgresStore, pool: &PgPool) {
    let operation = OperationKey::new(format!("starvation-{}", ExecutionId::new()), 1).unwrap();
    let failed_id = ExecutionId::new();
    let engine = Engine::new(store.clone());
    assert!(
        engine
            .start_with_id_and_context::<(), (), &'static str, _, _>(
                failed_id,
                operation.clone(),
                (),
                |_, _| async { Err("expected failure") },
            )
            .await
            .is_err()
    );
    let eligible_id = create(store, operation.clone(), 1, None).await;
    sqlx::query(
        "UPDATE resumable_executions SET created_at = TIMESTAMPTZ '2000-01-01 00:00:00+00' WHERE id = $1",
    )
    .bind(*failed_id.as_uuid())
    .execute(pool)
    .await
    .unwrap();

    let discovered = store
        .runnable_executions(&[operation], FailedExecutionPolicy::Reject, 1)
        .await
        .unwrap();
    assert_eq!(discovered.len(), 1);
    assert_eq!(discovered[0].id, eligible_id);
}

async fn verify_cancellation_recovery_through_worker(store: &PostgresStore) {
    let base_ms = 1_800_000_000_000;
    set_clock(store.pool(), "2027-01-15 08:00:00+00").await;
    let operation = OperationKey::new(format!("cancel-worker-{}", ExecutionId::new()), 1).unwrap();
    let id = ExecutionId::new();
    store
        .create_execution(
            NewExecution::new(id, operation.clone(), Timestamp(base_ms))
                .with_input(JsonCodec.encode(&()).unwrap()),
        )
        .await
        .unwrap();
    let claim = match store
        .acquire_execution(id, WorkerId::new(), MutationId::new(), 1_000)
        .await
        .unwrap()
    {
        ClaimOutcome::Claimed(claim) => claim,
        other => panic!("expected initial claim, got {other:?}"),
    };
    fail_step_for_test(
        store,
        id,
        &claim,
        "future-retry",
        FailureRecord {
            class: FailureClass::Retryable,
            diagnostic: None,
            occurred_at: Timestamp(base_ms),
        },
        Some(Timestamp(base_ms + 60_000)),
        RetryPolicyRecord {
            max_attempts: 3,
            backoff: resumable::BackoffRecord::None,
            retryable: true,
        },
    )
    .await;
    fail_step_for_test(
        store,
        id,
        &claim,
        "permanent-failure",
        FailureRecord {
            class: FailureClass::Permanent,
            diagnostic: None,
            occurred_at: Timestamp(base_ms),
        },
        None,
        RetryPolicyRecord {
            max_attempts: 1,
            backoff: resumable::BackoffRecord::None,
            retryable: false,
        },
    )
    .await;
    store
        .request_cancellation(RequestCancellation {
            execution_id: id,
            at: Timestamp(base_ms),
        })
        .await
        .unwrap();
    set_clock(store.pool(), "2027-01-15 08:00:01.001+00").await;

    let runner = Arc::new(CancelRecoveryRunner {
        engine: Engine::new(store.clone()),
        operation: operation.clone(),
        completed: Notify::new(),
    });
    let worker = start(
        Arc::new(PostgresRunnableSource(store.clone())),
        runner.clone(),
        WorkerSettings {
            enabled: true,
            poll_interval: Duration::from_millis(5),
            ..WorkerSettings::default()
        },
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), runner.completed.notified())
        .await
        .expect("worker should recover pending cancellation");
    worker.shutdown().await;
    assert_eq!(
        store.load_execution(id).await.unwrap().unwrap().status,
        resumable::ExecutionStatus::Cancelled
    );
    reset_clock(store.pool()).await;
}

async fn fail_step_for_test(
    store: &PostgresStore,
    id: ExecutionId,
    claim: &resumable::ExecutionClaim,
    step_name: &str,
    failure: FailureRecord,
    retry_at: Option<Timestamp>,
    retry_policy: RetryPolicyRecord,
) {
    let name = StepName::new(step_name).unwrap();
    let current = store.load_execution(id).await.unwrap().unwrap();
    let guard = resumable::LeaseGuard {
        execution_id: id,
        worker: claim.worker,
        token: claim.token,
        generation: claim.generation,
        expected_revision: current.revision,
    };
    let step = store
        .begin_step(resumable::BeginStep {
            lease: guard.clone(),
            name: name.clone(),
            retry_policy: retry_policy.clone(),
            mutation_id: MutationId::new(),
            at: failure.occurred_at,
        })
        .await
        .unwrap();
    let current = store.load_execution(id).await.unwrap().unwrap();
    store
        .fail_step(FailStep {
            lease: resumable::LeaseGuard {
                expected_revision: current.revision,
                ..guard
            },
            name,
            expected_attempt: step.attempt,
            mutation_id: MutationId::new(),
            failure,
            retry_at,
            retry_policy,
        })
        .await
        .unwrap();
}

struct PostgresRunnableSource(PostgresStore);

impl RunnableSource for PostgresRunnableSource {
    fn unsupported_operations<'a>(
        &'a self,
        supported: &'a [OperationKey],
        policy: FailedExecutionPolicy,
    ) -> WorkerFuture<'a, Result<Vec<OperationKey>, String>> {
        Box::pin(async move {
            self.0
                .unsupported_runnable_operations(supported, policy)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn runnable_executions<'a>(
        &'a self,
        supported: &'a [OperationKey],
        policy: FailedExecutionPolicy,
        limit: u32,
    ) -> WorkerFuture<'a, Result<Vec<RunnableExecution>, String>> {
        Box::pin(async move {
            self.0
                .runnable_executions(supported, policy, limit)
                .await
                .map(|items| {
                    items
                        .into_iter()
                        .map(|item| RunnableExecution {
                            id: item.id,
                            operation: item.operation,
                        })
                        .collect()
                })
                .map_err(|error| error.to_string())
        })
    }
}

struct CancelRecoveryRunner {
    engine: Engine<PostgresStore>,
    operation: OperationKey,
    completed: Notify,
}

impl ExecutionRunner for CancelRecoveryRunner {
    fn supported_operations(&self) -> WorkerFuture<'_, Vec<OperationKey>> {
        Box::pin(async move { vec![self.operation.clone()] })
    }

    fn run(
        &self,
        id: ExecutionId,
        operation: OperationKey,
    ) -> WorkerFuture<'_, Result<(), resumable::RunError>> {
        Box::pin(async move {
            let result = self
                .engine
                .resume_with_context::<(), (), String, _, _>(id, &operation, |_, _| async {
                    panic!("cancellation recovery must not invoke operation code")
                })
                .await;
            self.completed.notify_one();
            result.map(|_| ())
        })
    }
}

async fn verify_scheduled_start(store: &PostgresStore, pool: &PgPool) {
    let operation = OperationKey::new(format!("scheduled-{}", ExecutionId::new()), 1).unwrap();
    let run_at = Timestamp(1_769_904_000_000); // 2026-02-01 00:00:00 UTC
    let id = create(store, operation.clone(), 0, Some(run_at)).await;
    assert_eq!(
        store.load_execution(id).await.unwrap().unwrap().run_at,
        Some(run_at)
    );
    assert!(
        store
            .runnable_executions(
                std::slice::from_ref(&operation),
                FailedExecutionPolicy::Reject,
                1
            )
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        !store
            .unsupported_runnable_operations(&[], FailedExecutionPolicy::Reject)
            .await
            .unwrap()
            .contains(&operation)
    );
    assert!(matches!(
        store
            .acquire_execution(id, WorkerId::new(), MutationId::new(), 1_000)
            .await
            .unwrap(),
        ClaimOutcome::Busy
    ));

    set_clock(pool, "2026-02-01 00:00:00+00").await;
    assert_eq!(
        store
            .runnable_executions(&[operation], FailedExecutionPolicy::Reject, 1)
            .await
            .unwrap()[0]
            .id,
        id
    );
    assert!(matches!(
        store
            .acquire_execution(id, WorkerId::new(), MutationId::new(), 1_000)
            .await
            .unwrap(),
        ClaimOutcome::Claimed(_)
    ));
}

async fn create(
    store: &PostgresStore,
    operation: OperationKey,
    created_at: i64,
    run_at: Option<Timestamp>,
) -> ExecutionId {
    let id = ExecutionId::new();
    let mut execution = NewExecution::new(id, operation, Timestamp(created_at));
    if let Some(run_at) = run_at {
        execution = execution.with_run_at(run_at);
    }
    store.create_execution(execution).await.unwrap();
    id
}

async fn set_clock(pool: &PgPool, at: &str) {
    let statement = format!(
        "CREATE OR REPLACE FUNCTION resumable_now() RETURNS TIMESTAMPTZ
		LANGUAGE SQL VOLATILE AS $$ SELECT TIMESTAMPTZ '{at}' $$"
    );
    sqlx::raw_sql(&statement).execute(pool).await.unwrap();
}

async fn reset_clock(pool: &PgPool) {
    sqlx::raw_sql(
        "CREATE OR REPLACE FUNCTION resumable_now() RETURNS TIMESTAMPTZ
		LANGUAGE SQL VOLATILE AS $$ SELECT clock_timestamp() $$",
    )
    .execute(pool)
    .await
    .unwrap();
}
