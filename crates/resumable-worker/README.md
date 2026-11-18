# resumable-worker

Portable polling and operation-registry runtime for the [`resumable`](../resumable) execution
engine. The crate does not choose a database, validate application schema, authorize domain work,
or implement operation bodies. Applications provide runnable discovery and execution adapters.

## Use

See [`examples/standalone.rs`](examples/standalone.rs) for a complete in-memory source, generic
operation registry, runner, worker startup, and graceful shutdown. Run it with:

```sh
cargo run -p resumable-worker --example standalone
```

The dependency direction and extension points are described in
[`docs/architecture.md`](docs/architecture.md). The `resumable-store-postgres-sqlx` crate remains
an independent adapter over `resumable`; applications can combine that adapter with this worker
runtime without introducing a dependency cycle.
