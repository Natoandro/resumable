use resumable::FailedExecutionPolicy;
use std::time::Duration;

/// Controls polling, concurrency, and graceful shutdown.
#[derive(Clone, Copy, Debug)]
pub struct WorkerSettings {
    /// Whether to spawn a worker. Defaults to false.
    pub enabled: bool,
    /// Maximum simultaneous executions in this worker.
    pub concurrency: usize,
    /// Time between discovery ticks. Must be nonzero when enabled.
    pub poll_interval: Duration,
    /// Time to drain active executions before aborting their tasks.
    pub shutdown_timeout: Duration,
    /// Whether previously failed executions may be resumed by the runner.
    pub failed_execution_policy: FailedExecutionPolicy,
}

impl Default for WorkerSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            concurrency: 4,
            poll_interval: Duration::from_secs(2),
            shutdown_timeout: Duration::from_secs(30),
            failed_execution_policy: FailedExecutionPolicy::Reject,
        }
    }
}
