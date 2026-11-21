use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::Arc,
};

use resumable::{ExecutionId, FailedExecutionPolicy, OperationKey, RunError};
use tokio::{
    task::{JoinHandle, JoinSet},
    time::{MissedTickBehavior, interval},
};

use crate::WorkerSettings;

/// A boxed future returned by a discovery or execution adapter.
pub type WorkerFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone, Debug, PartialEq, Eq)]
/// A candidate execution and its stored operation version.
pub struct RunnableExecution {
    pub id: ExecutionId,
    pub operation: OperationKey,
}

/// Persistence-backed discovery of candidates eligible to run.
pub trait RunnableSource: Send + Sync + 'static {
    /// Report runnable operation versions absent from the supplied set.
    fn unsupported_operations<'a>(
        &'a self,
        supported: &'a [OperationKey],
        failed_execution_policy: FailedExecutionPolicy,
    ) -> WorkerFuture<'a, Result<Vec<OperationKey>, String>>;

    /// Return at most `limit` runnable candidates for the supported operation versions.
    fn runnable_executions<'a>(
        &'a self,
        supported: &'a [OperationKey],
        failed_execution_policy: FailedExecutionPolicy,
        limit: u32,
    ) -> WorkerFuture<'a, Result<Vec<RunnableExecution>, String>>;
}

/// Application dispatch for operations understood by this worker.
pub trait ExecutionRunner: Send + Sync + 'static {
    /// Return the exact operation names and versions this runner can execute.
    fn supported_operations(&self) -> WorkerFuture<'_, Vec<OperationKey>>;
    /// Acquire and run the execution through the application engine.
    fn run(
        &self,
        id: ExecutionId,
        operation: OperationKey,
    ) -> WorkerFuture<'_, Result<(), RunError>>;
}

/// Handle for stopping a running polling task.
pub struct ResumableWorkerHandle {
    stop: tokio::sync::oneshot::Sender<()>,
    join: JoinHandle<()>,
}

impl ResumableWorkerHandle {
    /// Stop polling and await in-flight work, aborting it after the configured timeout.
    pub async fn shutdown(self) {
        let _ = self.stop.send(());
        if let Err(error) = self.join.await {
            tracing::error!(?error, "resumable worker task failed during shutdown");
        }
    }
}

/// Start polling on the current Tokio runtime. Returns `None` when disabled.
///
/// Use a runtime with time enabled and a nonzero poll interval.
pub fn start(
    source: Arc<dyn RunnableSource>,
    runner: Arc<dyn ExecutionRunner>,
    settings: WorkerSettings,
) -> Option<ResumableWorkerHandle> {
    if !settings.enabled {
        return None;
    }
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let worker = Worker::new(source, runner, settings);
    let join = tokio::spawn(worker.run(stopped));
    Some(ResumableWorkerHandle { stop, join })
}

struct Worker {
    source: Arc<dyn RunnableSource>,
    runner: Arc<dyn ExecutionRunner>,
    settings: WorkerSettings,
    tasks: ActiveTasks,
    reported_unknown: HashSet<OperationKey>,
}

impl Worker {
    fn new(
        source: Arc<dyn RunnableSource>,
        runner: Arc<dyn ExecutionRunner>,
        settings: WorkerSettings,
    ) -> Self {
        Self {
            source,
            runner,
            settings,
            tasks: ActiveTasks::new(),
            reported_unknown: HashSet::new(),
        }
    }

    async fn run(mut self, mut stopped: tokio::sync::oneshot::Receiver<()>) {
        let mut poll = interval(self.settings.poll_interval);
        poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                Some(result) = self.tasks.join_next(), if !self.tasks.is_empty() => self.tasks.reap(result),
                _ = poll.tick() => {
                    self.tasks.reap_ready();
                    let capacity = self.settings.concurrency.saturating_sub(self.tasks.len());
                    if capacity == 0 { continue; }
                    tokio::select! {
                        _ = &mut stopped => break,
                        _ = self.discover_and_dispatch(capacity) => {}
                    }
                }
            }
        }
        self.drain().await;
    }

    async fn discover_and_dispatch(&mut self, capacity: usize) {
        let supported = self.runner.supported_operations().await;
        match self
            .source
            .unsupported_operations(&supported, self.settings.failed_execution_policy)
            .await
        {
            Ok(unknown) => {
                for key in unknown {
                    if self.reported_unknown.insert(key.clone()) {
                        tracing::warn!(operation = ?key, "runnable execution has no compatible handler");
                    }
                }
            }
            Err(error) => {
                tracing::error!(?error, "could not inspect unsupported resumable operations")
            }
        }
        match self
            .source
            .runnable_executions(
                &supported,
                self.settings.failed_execution_policy,
                capacity as u32,
            )
            .await
        {
            Ok(candidates) => {
                for candidate in candidates {
                    self.tasks.dispatch(self.runner.clone(), candidate);
                }
            }
            Err(error) => tracing::error!(?error, "resumable runnable discovery failed"),
        }
    }

    async fn drain(&mut self) {
        let drain = async {
            while let Some(result) = self.tasks.join_next().await {
                self.tasks.reap(result);
            }
        };
        if tokio::time::timeout(self.settings.shutdown_timeout, drain)
            .await
            .is_err()
        {
            tracing::warn!("resumable worker shutdown deadline elapsed; leaving leases to expire");
            self.tasks.abort_all();
            while let Some(result) = self.tasks.join_next().await {
                self.tasks.reap(result);
            }
        }
    }
}

struct ActiveTasks {
    tasks: JoinSet<Result<(), RunError>>,
    task_ids: HashMap<tokio::task::Id, ExecutionId>,
    active_ids: HashSet<ExecutionId>,
}

impl ActiveTasks {
    fn new() -> Self {
        Self {
            tasks: JoinSet::new(),
            task_ids: HashMap::new(),
            active_ids: HashSet::new(),
        }
    }
    fn len(&self) -> usize {
        self.tasks.len()
    }
    fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    fn dispatch(&mut self, runner: Arc<dyn ExecutionRunner>, candidate: RunnableExecution) {
        if !self.active_ids.insert(candidate.id) {
            return;
        }
        let id = candidate.id;
        tracing::info!(execution_id = %id, operation = ?candidate.operation, "resumable execution started");
        let task = self
            .tasks
            .spawn(async move { runner.run(id, candidate.operation).await });
        self.task_ids.insert(task.id(), id);
    }

    fn reap_ready(&mut self) {
        while let Some(result) = self.tasks.try_join_next_with_id() {
            self.reap(result);
        }
    }

    fn reap(
        &mut self,
        result: Result<(tokio::task::Id, Result<(), RunError>), tokio::task::JoinError>,
    ) {
        let task_id = match &result {
            Ok((id, _)) => *id,
            Err(error) => error.id(),
        };
        let execution_id = self.task_ids.remove(&task_id);
        match result {
            Ok((_, result)) => report_result(execution_id, result),
            Err(error) => {
                if error.is_cancelled() {
                    tracing::debug!(execution_id = ?execution_id, "resumable execution task was cancelled");
                } else {
                    tracing::error!(?error, execution_id = ?execution_id, "resumable execution task panicked");
                }
            }
        }
        if let Some(id) = execution_id {
            self.active_ids.remove(&id);
        }
    }

    async fn join_next(
        &mut self,
    ) -> Option<Result<(tokio::task::Id, Result<(), RunError>), tokio::task::JoinError>> {
        self.tasks.join_next_with_id().await
    }
    fn abort_all(&mut self) {
        self.tasks.abort_all();
    }
}

fn report_result(id: Option<ExecutionId>, result: Result<(), RunError>) {
    let Some(id) = id else {
        tracing::error!("resumable task completed without active task mapping");
        return;
    };
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

#[cfg(test)]
mod tests;
