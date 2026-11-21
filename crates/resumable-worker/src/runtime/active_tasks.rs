use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use resumable::{ExecutionId, RunError};
use tokio::task::{Id, JoinError, JoinSet};

use super::{ExecutionRunner, RunnableExecution};

pub(super) struct ActiveTasks {
    tasks: JoinSet<Result<(), RunError>>,
    executions_by_task: HashMap<Id, ExecutionId>,
    active_ids: HashSet<ExecutionId>,
}

impl ActiveTasks {
    pub(super) fn new() -> Self {
        Self {
            tasks: JoinSet::new(),
            executions_by_task: HashMap::new(),
            active_ids: HashSet::new(),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.tasks.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub(super) fn dispatch(
        &mut self,
        runner: Arc<dyn ExecutionRunner>,
        candidate: RunnableExecution,
    ) {
        let RunnableExecution { id, operation } = candidate;
        if !self.active_ids.insert(id) {
            return;
        }
        tracing::info!(execution_id = %id, ?operation, "resumable execution started");
        let task = self
            .tasks
            .spawn(async move { runner.run(id, operation).await });
        self.executions_by_task.insert(task.id(), id);
    }

    pub(super) fn reap_ready(&mut self) {
        while let Some(result) = self.tasks.try_join_next_with_id() {
            self.reap(result);
        }
    }

    pub(super) async fn reap_next(&mut self) {
        if let Some(result) = self.tasks.join_next_with_id().await {
            self.reap(result);
        }
    }

    pub(super) async fn drain(&mut self) {
        while !self.is_empty() {
            self.reap_next().await;
        }
    }

    pub(super) async fn abort_and_drain(&mut self) {
        self.tasks.abort_all();
        self.drain().await;
    }

    fn reap(&mut self, result: Result<(Id, Result<(), RunError>), JoinError>) {
        let task_id = match &result {
            Ok((id, _)) => *id,
            Err(error) => error.id(),
        };
        let Some(id) = self.executions_by_task.remove(&task_id) else {
            tracing::error!(
                ?task_id,
                ?result,
                "resumable task completed without active task mapping"
            );
            return;
        };
        self.active_ids.remove(&id);

        match result {
            Ok((_, result)) => report_result(id, result),
            Err(error) if error.is_cancelled() => {
                tracing::debug!(execution_id = %id, "resumable execution task was cancelled");
            }
            Err(error) => {
                tracing::error!(?error, execution_id = %id, "resumable execution task panicked");
            }
        }
    }
}

fn report_result(id: ExecutionId, result: Result<(), RunError>) {
    match result {
        Ok(()) => {
            tracing::info!(execution_id = %id, "resumable execution completed");
        }
        Err(RunError::Busy) => {
            tracing::debug!(execution_id = %id, "resumable execution is owned by another worker");
        }
        Err(error) => {
            tracing::error!(execution_id = %id, ?error, "resumable execution failed");
        }
    }
}
