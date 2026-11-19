
# Run and resume a PostgreSQL-backed worker

This walkthrough runs a two-step operation using the execution engine, the Tokio worker,
and the PostgreSQL adapter. The first process exits after committing the `double` step.
A second process discovers the same execution after its lease expires, reuses the stored
result of `double`, and completes the `increment` step.

## Set up the database and enqueue work

Use a disposable PostgreSQL database and a checkout of the workspace. Point
`RESUMABLE_TEST_DATABASE_URL` at the database with credentials that can apply migrations.

The example opens an SQLx pool, runs `migrate` and `validate_schema`, then wraps the pool
in `PostgresStore`. In `interrupt` mode it inserts a [`NewExecution`] with the supplied
execution ID, the key `walkthrough.postgres-worker` at version 1, and the input `21_u32`
encoded by [`JsonCodec`]. Calling [`Store::create_execution`] queues the execution for the
polling worker. The engine uses the same codec to decode the input on resume.

## Wire discovery to execution

The example's `PostgresSource` implements the worker's `RunnableSource` by forwarding the
supported operation keys and batch limit to the adapter. The adapter returns candidate IDs and
operation keys. The runner advertises the one exact version it handles and dispatches each
candidate to [`Engine::resume_with_context`], which loads the stored input and acquires the lease.
For multiple handlers, see the worker's `OperationRegistry`.

The operation calls [`ExecutionContext::step`] twice. The first step doubles the input.
The second adds one to the first step's result. The step names and operation version are
durable identifiers, so both processes use the same names and version.

## Interrupt and restart the worker

From the workspace root, choose a fresh execution UUID and run both modes with the same ID.
For example, in Bash:

```sh
export SQLX_OFFLINE=true
export RESUMABLE_TEST_DATABASE_URL=postgres://user:password@localhost/resumable_demo
execution_id=7a7ff784-1c1d-46a2-a652-f4e3c098ab64

status=0
cargo run -p resumable-store-postgres-sqlx --example postgres_worker -- \
  interrupt "$execution_id" || status=$?
test "$status" -eq 42 && \
  cargo run -p resumable-store-postgres-sqlx --example postgres_worker -- \
    resume "$execution_id"
```

The first process prints `Running double` and exits with status 42 inside `increment`.
The second process polls until PostgreSQL considers the old lease expired. It prints
`Running increment` and reports result `43` without running the body of `double` again.
You can start it immediately. The example uses a three-second lease and polls every 100 ms
so the wait is short. After completion, the second process shuts the worker down gracefully.

An interrupted step can run again, as `increment` does here. If a step writes to an external
system, use an idempotency key or reconcile its previous attempt before repeating that write.

[Runnable example source](https://github.com/Natoandro/resumable/blob/HEAD/crates/resumable-store-postgres-sqlx/examples/postgres_worker.rs) ·
[Worker API](https://docs.rs/resumable-worker) ·
[PostgreSQL adapter API](https://docs.rs/resumable-store-postgres-sqlx)
