use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub struct WorkerSettings {
    pub enabled: bool,
    pub concurrency: usize,
    pub poll_interval: Duration,
    pub shutdown_timeout: Duration,
}

impl Default for WorkerSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            concurrency: 4,
            poll_interval: Duration::from_secs(2),
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}
