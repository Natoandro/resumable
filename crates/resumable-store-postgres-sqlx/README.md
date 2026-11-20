# resumable-store-postgres-sqlx

PostgreSQL persistence for the [resumable](https://docs.rs/resumable) execution engine. The
adapter stores execution state and step results so work can resume after a process restart.
It also discovers executions eligible to run and supports operations inside application-owned
SQLx transactions.

## Set up a store

Add `resumable-store-postgres-sqlx`, `resumable`, and `sqlx` to your application. Create a
PostgreSQL pool, apply the adapter's migrations, validate the schema, and wrap the pool:

```rust
use resumable::StoreError;
use resumable_store_postgres_sqlx::{PostgresStore, migrate, validate_schema};
use sqlx::PgPool;

async fn initialize(pool: PgPool) -> Result<PostgresStore, StoreError> {
    migrate(&pool).await?;
    validate_schema(&pool).await?;
    Ok(PostgresStore::new(pool))
}
```

The pool used for migrations needs permission to make schema changes. The adapter tracks its
migration history in `resumable_migrations`. Pass the resulting store to the execution engine;
see the [core API documentation](https://docs.rs/resumable) for running operations.

## Discover and resume work

`PostgresStore::runnable_executions` finds candidates for the operation versions your application
handles. Pass candidates to the engine to acquire a lease and resume using their stored input.
PostgreSQL evaluates scheduling, lease, and retry eligibility. The
[PostgreSQL worker walkthrough](https://docs.rs/resumable/latest/resumable/#run-and-resume-a-postgresql-backed-worker)
shows how to enqueue work, interrupt a worker, and resume the execution in a new process.

## Application transactions

Use `create_execution_in_transaction` to enqueue work together with an application database
write, or `complete_step_in_transaction` to commit a step result alongside other changes. The
caller owns the transaction's commit or rollback. If a commit is uncertain, retry the identical
completion request in a fresh transaction so the store can recognize its mutation receipt.
External effects still require application-level idempotency or reconciliation. See the
[adapter API documentation](https://docs.rs/resumable-store-postgres-sqlx) for details.

For development and test instructions, see [CONTRIBUTING.md](../../CONTRIBUTING.md).
