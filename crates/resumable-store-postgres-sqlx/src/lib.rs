//! PostgreSQL persistence for the [`resumable`] execution engine.
//!
//! # Database setup
//!
//! Run [`migrate`] with a pool that can apply schema changes, then call [`validate_schema`]
//! before starting workers. The adapter tracks its migrations in `resumable_migrations`.
//! [`PostgresStore::new`] wraps an existing SQLx pool.
//!
//! ```
//! use resumable::StoreError;
//! use resumable_store_postgres_sqlx::{PostgresStore, migrate, validate_schema};
//! use sqlx::PgPool;
//!
//! async fn initialize(pool: PgPool) -> Result<PostgresStore, StoreError> {
//!     migrate(&pool).await?;
//!     validate_schema(&pool).await?;
//!     Ok(PostgresStore::new(pool))
//! }
//! ```
//!
//! # Dispatch and transactions
//!
//! Use [`PostgresStore::runnable_executions`] to discover candidates for the operation versions
//! your application handles. Pass each candidate to the engine to acquire a lease and resume
//! with its persisted input. PostgreSQL evaluates schedule, lease, and retry eligibility.
//!
//! Use [`PostgresStore::create_execution_in_transaction`] to enqueue work atomically with a
//! domain write. Use [`PostgresStore::complete_step_in_transaction`] to commit a step result
//! with changes in the same PostgreSQL transaction. Build the completion request with
//! [`resumable::StepContext::completion_request`] and encode the result with the engine's codec.
//! The caller owns the commit or rollback. If commit is uncertain, retry the identical request
//! in a fresh transaction so the store can recognize its mutation receipt.
//! External effects still need application idempotency or reconciliation.

#![forbid(unsafe_code)]

mod postgres;

pub use postgres::{
    PostgresStore, RunnableExecution, SCHEMA_VERSION, migrate, validate_schema,
    validate_schema_version,
};
