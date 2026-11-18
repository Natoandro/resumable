//! Portable primitives for durable, resumable asynchronous operations.
//!
//! The core has no dependency on Planum, a database, or an asynchronous runtime. Persistence and
//! scheduling are supplied by the caller through the store contract and application worker.

#![forbid(unsafe_code)]

pub mod conformance;
mod engine;
mod error;
mod identity;
pub mod limits;
mod metadata;
mod record;
pub mod reference;
mod store;
mod value;

pub use engine::{
    Backoff, Engine, EngineConfig, ExecutionContext, ExecutionInspection, FailedExecutionPolicy,
    InspectedStep, RetryPolicy, RetryPolicyError, RunError, StepContext, StepOptions, StepRunError,
};
pub use error::{ResumableError, StateTransitionError, StoreError};
pub use identity::{
    ExecutionId, IdempotencyKey, InvalidOperationKey, InvalidStepName, LeaseToken, OperationKey,
    Revision, StepId, StepName, WorkerId,
};
pub use metadata::{ExecutionMetadata, MetadataError};
pub use record::{
    BackoffRecord, ExecutionRecord, ExecutionStatus, FailureClass, FailureRecord, Lease,
    RetryPolicyRecord, StepRecord, StepStatus, Timestamp,
};
pub use store::{
    BeginStep, ClaimOutcome, CompleteExecution, CompleteStep, ExecutionClaim, FailExecution,
    FailStep, LeaseGuard, MutationId, NewExecution, Progress, ProgressUpdate, RequestCancellation,
    Store,
};
pub use value::{JsonCodec, StoredValue, ValueCodec, ValueError};
