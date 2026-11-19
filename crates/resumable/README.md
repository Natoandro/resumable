# resumable

Generic Rust crate for durable, resumable asynchronous operations composed of named steps.

The [v0.1 specification](docs/specification.md) defines the library contract, and the
[architecture](docs/architecture.md) defines its execution model, persistence transitions, and
recovery behavior. The [HTML guide](docs/site/index.html) covers the concepts for integrators
and implementation audiences. See [stored value and metadata guidance](docs/value-storage.md)
for codec and size-limit contracts.

## Documentation

The [crate documentation](https://docs.rs/resumable) covers direct execution, durable steps,
retries, cancellation, and the store contract. The [worker](https://docs.rs/resumable-worker)
provides polling, and the [PostgreSQL adapter](https://docs.rs/resumable-store-postgres-sqlx)
provides durable storage.

## Portable examples and release checks

Runnable examples use fake object storage and a fake message provider, and run without external services or an async runtime:

```bash
cargo run -p resumable --example artifact_reconciliation
cargo run -p resumable --example backup_snapshot
cargo run -p resumable --example message_delivery
```

The artifact example interrupts after an object write and resumes against the same `MemoryStore`
and fake object store. It shows why the outer operation runs again and how a deterministic object key
lets the application reconcile the effect. The message example models a provider idempotency key
that expires; after that window, the application needs provider reconciliation or duplicate
tolerance. `MemoryStore` models process restart only when the same store instance is retained; it is
not durable across an actual machine restart.

See [core release checks](docs/core-release-checks.md) for formatting, clippy, tests, docs, MSRV,
and standalone-build commands, and [requirement evidence](docs/core-requirements-evidence.md) for
the invariant and failure-boundary map.
