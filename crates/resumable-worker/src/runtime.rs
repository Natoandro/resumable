use std::{collections::HashSet, future::Future, pin::Pin, sync::Arc};

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
    let join = tokio::spawn(run(source, runner, settings, stopped));
    Some(ResumableWorkerHandle { stop, join })
}

async fn run(
    source: Arc<dyn RunnableSource>,
    runner: Arc<dyn ExecutionRunner>,
    settings: WorkerSettings,
    mut stopped: tokio::sync::oneshot::Receiver<()>,
) {
    let mut poll = interval(settings.poll_interval);
    poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut tasks: JoinSet<(ExecutionId, Result<(), RunError>)> = JoinSet::new();
    let mut active_ids = HashSet::new();
    let mut reported_unknown = HashSet::new();
    loop {
        tokio::select! {
            _ = &mut stopped => break,
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(id) = report_result(result) {
                    active_ids.remove(&id);
                } else {
                    active_ids.clear();
                }
            },
            _ = poll.tick() => {
                while let Some(result) = tasks.try_join_next() {
                    if let Some(id) = report_result(result) {
                        active_ids.remove(&id);
                    } else {
                        active_ids.clear();
                    }
                }
                let capacity = settings.concurrency.saturating_sub(tasks.len());
                if capacity == 0 {
                    continue;
                }
                let supported = runner.supported_operations().await;
                match source.unsupported_operations(&supported, settings.failed_execution_policy).await {
                    Ok(unknown) => for key in unknown {
                        if reported_unknown.insert(key.clone()) {
                            tracing::warn!(operation = ?key, "runnable execution has no compatible handler");
                        }
                    },
                    Err(error) => tracing::error!(?error, "could not inspect unsupported resumable operations"),
                }
                match source.runnable_executions(&supported, settings.failed_execution_policy, capacity as u32).await {
                    Ok(candidates) => for candidate in candidates {
                        if !active_ids.insert(candidate.id) {
                            continue;
                        }
                        tracing::info!(
                            execution_id = %candidate.id,
                            operation = ?candidate.operation,
                            "resumable execution started"
                        );
                        let runner = runner.clone();
                        tasks.spawn(async move {
                            let result = runner.run(candidate.id, candidate.operation).await;
                            (candidate.id, result)
                        });
                    },
                    Err(error) => tracing::error!(?error, "resumable runnable discovery failed"),
                }
            }
        }
    }
    let drain = async {
        while let Some(result) = tasks.join_next().await {
            report_result(result);
        }
    };
    if tokio::time::timeout(settings.shutdown_timeout, drain)
        .await
        .is_err()
    {
        tracing::warn!("resumable worker shutdown deadline elapsed; leaving leases to expire");
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
}

fn report_result(
    result: Result<(ExecutionId, Result<(), RunError>), tokio::task::JoinError>,
) -> Option<ExecutionId> {
    match result {
        Ok((id, Ok(()))) => {
            tracing::info!(execution_id = %id, "resumable execution completed");
            Some(id)
        }
        Ok((id, Err(RunError::Busy))) => {
            tracing::debug!(execution_id = %id, "resumable execution is owned by another worker");
            Some(id)
        }
        Ok((id, Err(error))) => {
            tracing::error!(execution_id = %id, ?error, "resumable execution failed");
            Some(id)
        }
        Err(error) => {
            tracing::error!(?error, "resumable execution task panicked");
            None
        }
    }
}

#[cfg(test)]
mod tests;
