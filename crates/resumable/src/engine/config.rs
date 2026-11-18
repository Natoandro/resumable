use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailedExecutionPolicy {
    Reject,
    Resume,
}

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub worker_id: WorkerId,
    pub lease_duration: Duration,
    pub heartbeat_interval: Duration,
    pub failed_execution_policy: FailedExecutionPolicy,
    /// Persist application error text in failure records; disabled because errors may be sensitive.
    pub capture_error_diagnostics: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            worker_id: WorkerId::new(),
            lease_duration: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(10),
            failed_execution_policy: FailedExecutionPolicy::Reject,
            capture_error_diagnostics: false,
        }
    }
}

#[derive(Debug, Error)]
pub enum RunError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Value(#[from] ValueError),
    #[error("execution was not found")]
    NotFound,
    #[error("execution already has an active runner")]
    Busy,
    #[error("execution is already terminal")]
    Terminal,
    #[error("execution was cancelled")]
    Cancelled,
    #[error("execution failed; explicit resume policy is required")]
    Failed,
    #[error("execution operation identity does not match")]
    OperationConflict,
    #[error("operation '{name}' version {requested} cannot resume stored version {stored}")]
    UnsupportedOperationVersion {
        name: String,
        stored: u32,
        requested: u32,
    },
    #[error("execution has no persisted input")]
    MissingInput,
    #[error("operation failed: {0}")]
    Operation(String),
}

#[derive(Debug, Error)]
pub enum StepRunError<E: std::fmt::Display + std::fmt::Debug> {
    #[error("invalid step name: {0}")]
    InvalidName(#[from] crate::InvalidStepName),
    #[error("step store operation failed: {0}")]
    Store(#[from] StoreError),
    #[error("step value serialization failed: {0}")]
    Value(#[from] ValueError),
    #[error("step '{name}' failed: {source}")]
    Operation { name: String, source: E },
    #[error("completed step '{0}' has no stored result")]
    MissingResult(String),
    #[error("step retry is not eligible until timestamp {0}")]
    RetryNotEligible(i64),
}
