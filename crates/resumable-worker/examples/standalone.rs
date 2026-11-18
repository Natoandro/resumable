use std::{collections::VecDeque, sync::Arc, time::Duration};

use resumable::{ExecutionId, OperationKey, RunError};
use resumable_worker::{
    ExecutionRunner, OperationRegistry, RunnableExecution, RunnableSource, WorkerFuture,
    WorkerSettings, start,
};
use serde_json::{Value, json};
use tokio::{sync::Notify, time::timeout};

struct InMemorySource(tokio::sync::Mutex<VecDeque<RunnableExecution>>);

impl RunnableSource for InMemorySource {
    fn unsupported_operations<'a>(
        &'a self,
        _supported: &'a [OperationKey],
    ) -> WorkerFuture<'a, Result<Vec<OperationKey>, String>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn runnable_executions<'a>(
        &'a self,
        _supported: &'a [OperationKey],
        limit: u32,
    ) -> WorkerFuture<'a, Result<Vec<RunnableExecution>, String>> {
        Box::pin(async move {
            let mut queue = self.0.lock().await;
            let count = (limit as usize).min(queue.len());
            Ok(queue.drain(..count).collect())
        })
    }
}

struct RegistryRunner {
    registry: Arc<OperationRegistry<(), ()>>,
    completed: Arc<Notify>,
}

impl ExecutionRunner for RegistryRunner {
    fn supported_operations(&self) -> WorkerFuture<'_, Vec<OperationKey>> {
        Box::pin(self.registry.keys())
    }

    fn run(
        &self,
        _id: ExecutionId,
        operation: OperationKey,
    ) -> WorkerFuture<'_, Result<(), RunError>> {
        let registry = self.registry.clone();
        let completed = self.completed.clone();
        Box::pin(async move {
            let handler = registry
                .handler(&operation)
                .await
                .ok_or(RunError::OperationConflict)?;
            handler(json!({ "message": "hello" }), (), ())
                .await
                .map_err(RunError::Operation)?;
            completed.notify_one();
            Ok(())
        })
    }
}

#[tokio::main]
async fn main() {
    let operation = OperationKey::new("example.greeting", 1).unwrap();
    let registry = Arc::new(OperationRegistry::<(), ()>::default());
    registry
        .register(operation.clone(), |input, (), ()| async move {
            let message = input["message"].as_str().unwrap_or("hello");
            println!("operation says: {message}");
            Ok(Value::Null)
        })
        .await
        .unwrap();
    let completed = Arc::new(Notify::new());
    let worker = start(
        Arc::new(InMemorySource(tokio::sync::Mutex::new(VecDeque::from([
            RunnableExecution {
                id: ExecutionId::new(),
                operation: operation.clone(),
            },
        ])))),
        Arc::new(RegistryRunner {
            registry,
            completed: completed.clone(),
        }),
        WorkerSettings {
            enabled: true,
            poll_interval: Duration::from_millis(20),
            ..WorkerSettings::default()
        },
    )
    .unwrap();
    timeout(Duration::from_secs(1), completed.notified())
        .await
        .expect("worker should dispatch the queued execution");
    worker.shutdown().await;
}
