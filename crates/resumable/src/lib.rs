//! Portable primitives for durable, resumable asynchronous operations.
//!
//! An [`Engine`] runs versioned operations against an application-provided [`Store`].
//! Use [`reference::MemoryStore`] for tests and examples. Use the
//! [PostgreSQL adapter](https://docs.rs/resumable-store-postgres-sqlx) for durable persistence
//! and the [worker crate](https://docs.rs/resumable-worker) for polling.
//!
//! # Quick start
//!
//! ```
//! use resumable::{Engine, ExecutionId, OperationKey, RunError, Timestamp, reference::MemoryStore};
//!
//! async fn run() -> Result<(), RunError> {
//!     let engine = Engine::new(MemoryStore::new(Timestamp(0)));
//!     let operation = OperationKey::new("example.double", 1).unwrap();
//!     let id = ExecutionId::new();
//!     let result = engine
//!         .start_with_id_and_context(id, operation.clone(), 21_u32, |input, ctx| async move {
//!             ctx.step("double", |_| async move { Ok::<_, &str>(input * 2) }).await
//!         })
//!         .await?;
//!     assert_eq!(result, 42);
//!
//!     let cached: u32 = engine
//!         .resume(id, &operation, |_: u32| -> std::future::Ready<Result<u32, &str>> {
//!             panic!("completed executions return their stored result")
//!         })
//!         .await?;
//!     assert_eq!(cached, 42);
//!     Ok(())
//! }
//! ```
#![doc = include_str!("../docs/usage.md")]
#![doc = include_str!("../docs/value-storage.md")]
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
