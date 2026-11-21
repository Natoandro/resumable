use std::{collections::HashSet, future::Future, pin::Pin, sync::Arc};

use resumable::{ExecutionId, FailedExecutionPolicy, OperationKey, RunError};
use tokio::{
    task::JoinHandle,
    time::{MissedTickBehavior, interval},
};

use crate::WorkerSettings;

mod active_tasks;

use active_tasks::ActiveTasks;

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
                _ = self.tasks.reap_next(), if !self.tasks.is_empty() => {}
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
        if tokio::time::timeout(self.settings.shutdown_timeout, self.tasks.drain())
            .await
            .is_err()
        {
            tracing::warn!("resumable worker shutdown deadline elapsed; leaving leases to expire");
            self.tasks.abort_and_drain().await;
        }
    }
}

#[cfg(test)]
mod tests;
