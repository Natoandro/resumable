//! Tokio polling and operation registration for [`resumable`].
//!
//! Implement [`RunnableSource`] using your persistence layer and [`ExecutionRunner`] using
//! your engine and handlers. The runner advertises the exact operation versions it supports.
//! For PostgreSQL, wrap the
//! [adapter's discovery API](https://docs.rs/resumable-store-postgres-sqlx/latest/resumable_store_postgres_sqlx/struct.PostgresStore.html)
//! in a source and map its candidates to [`RunnableExecution`]. Discovery leaves claiming
//! to the engine, so multiple workers may discover the same execution.
//!
//! Run [`start`] inside a Tokio runtime with time enabled and set [`WorkerSettings::enabled`]
//! to true. Call [`ResumableWorkerHandle::shutdown`] to stop discovery and drain in-flight
//! work up to the configured timeout. Aborted work can be picked up after its lease expires.
//!
//! See the [PostgreSQL worker walkthrough](https://docs.rs/resumable/latest/resumable/#run-and-resume-a-postgresql-backed-worker)
//! for an example that persists work and resumes it after a process restart.
//!
//! # Standalone example
//!
//! This example wires an in-memory source, operation registry, and runner together.
#![doc = concat!("```rust\n", include_str!("../examples/standalone.rs"), "\n```")]

mod registry;
mod runtime;
mod settings;

pub use registry::{OperationHandler, OperationHandlerFuture, OperationRegistry};
pub use runtime::{
    ExecutionRunner, ResumableWorkerHandle, RunnableExecution, RunnableSource, WorkerFuture, start,
};
pub use settings::WorkerSettings;
