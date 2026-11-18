use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StateTransitionError {
    #[error("cannot transition {record} from {from} to {to}")]
    Illegal {
        record: &'static str,
        from: String,
        to: String,
    },
    #[error("attempt number overflow")]
    AttemptOverflow,
    #[error("revision overflow")]
    RevisionOverflow,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StoreError {
    #[error("store operation failed: {0}")]
    Unavailable(String),
    #[error("concurrent state change: {0}")]
    Conflict(String),
    #[error("stored state is invalid: {0}")]
    Corrupt(String),
}

#[derive(Debug, Error)]
pub enum ResumableError<E: std::error::Error + 'static> {
    #[error("operation failed: {0}")]
    Operation(#[source] E),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("serialization failed: {0}")]
    Serialization(String),
    #[error("execution lease was lost")]
    LeaseLost,
    #[error("execution was cancelled")]
    Cancelled,
    #[error("execution identity conflicts with an existing record")]
    Conflict,
    #[error("operation is invalid in the current state")]
    InvalidState,
}
