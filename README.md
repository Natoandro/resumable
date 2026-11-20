# Resumable

Resumable runs asynchronous Rust operations as named, durable steps. When an execution resumes,
the operation starts again with its stored input and reuses completed step results. An interrupted
step can run again, so use idempotency keys or reconcile external effects before repeating them.

## How it works

An operation has a name and version, and each execution has an ID and stored input. The engine
records step results in a store. On resume, the operation runs again from the beginning, but
completed steps return their stored results. The engine coordinates ownership through leases so
another worker can recover an execution after an interruption.

A step that was interrupted before its result was recorded can run again. External side effects
therefore need their own idempotency or reconciliation strategy. Keep operation versions and step
names stable for executions that may resume later.

## Quick start

Add `resumable` to your Rust project. This example uses the in-memory store, which is useful for
experimentation and tests but does not persist across process restarts:

```rust
use resumable::{Engine, ExecutionId, OperationKey, RunError, Timestamp, reference::MemoryStore};

async fn run() -> Result<(), RunError> {
    let engine = Engine::new(MemoryStore::new(Timestamp(0)));
    let operation = OperationKey::new("example.double", 1).unwrap();
    let id = ExecutionId::new();
    let result = engine
        .start_with_id_and_context(id, operation.clone(), 21_u32, |input, ctx| async move {
            ctx.step("double", |_| async move { Ok::<_, &str>(input * 2) }).await
        })
        .await?;
    assert_eq!(result, 42);

    let cached: u32 = engine
        .resume(id, &operation, |_: u32| -> std::future::Ready<Result<u32, &str>> {
            panic!("completed executions return their stored result")
        })
        .await?;
    assert_eq!(cached, 42);
    Ok(())
}
```

For persistence across restarts, use the PostgreSQL adapter. The worker crate can poll for
runnable executions and dispatch registered operations. See the
[core API documentation](https://docs.rs/resumable) for configuration, retries, cancellation,
and value storage.

## Crates

| Crate | Role |
| --- | --- |
| [`resumable`](https://docs.rs/resumable) | Execution engine, store contract, and in-memory reference store |
| [`resumable-worker`](https://docs.rs/resumable-worker) | Tokio polling runtime and versioned operation registry |
| [`resumable-store-postgres-sqlx`](https://docs.rs/resumable-store-postgres-sqlx) | Durable PostgreSQL persistence, migrations, and runnable discovery |

Start with the core crate to define operations and steps. Use the in-memory store to try the API,
the PostgreSQL adapter to persist executions across process restarts, and the worker when your
application needs polling. The crate documentation includes setup examples and API contracts.

Follow the [PostgreSQL worker walkthrough](https://docs.rs/resumable/latest/resumable/#run-and-resume-a-postgresql-backed-worker)
to enqueue an operation, interrupt its worker, and resume it in a new process.

## Examples

Try the examples from a checkout:

```sh
cargo run -p resumable --example artifact_reconciliation
cargo run -p resumable-worker --example standalone
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development and releases. Released under the
[MIT license](LICENSE).
