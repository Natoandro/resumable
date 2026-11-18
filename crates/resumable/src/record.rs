mod execution;
mod step;

pub use execution::{ExecutionRecord, ExecutionStatus, Lease};
pub use step::{
    BackoffRecord, FailureClass, FailureRecord, RetryPolicyRecord, StepRecord, StepStatus,
};

use serde::{Deserialize, Serialize};

/// Unix milliseconds. The persistence adapter's clock remains authoritative for lease decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(pub i64);
