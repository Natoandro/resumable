# Resumable

Resumable runs asynchronous Rust operations as named, durable steps. When an execution resumes,
the operation starts again with its stored input and reuses completed step results. An interrupted
step can run again, so use idempotency keys or reconcile external effects before repeating them.

| Crate | Role |
| --- | --- |
| [`resumable`](https://docs.rs/resumable) | Execution engine, store contract, and in-memory reference store |
| [`resumable-worker`](https://docs.rs/resumable-worker) | Tokio polling runtime and versioned operation registry |
| [`resumable-store-postgres-sqlx`](https://docs.rs/resumable-store-postgres-sqlx) | Durable PostgreSQL persistence, migrations, and runnable discovery |

Start with the core crate to define operations and steps. Use the in-memory store to try the API,
the PostgreSQL adapter to persist executions across process restarts, and the worker when your
application needs polling. The crate documentation includes setup examples and API contracts.

Try the examples from a checkout:

```sh
cargo run -p resumable --example artifact_reconciliation
cargo run -p resumable-worker --example standalone
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for development and releases. Released under the
[MIT license](LICENSE).
