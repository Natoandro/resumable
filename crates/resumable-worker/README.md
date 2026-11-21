# resumable-worker

Portable polling and operation-registry runtime for the [`resumable`](../resumable) execution
engine. The crate does not choose a database, validate application schema, authorize domain work,
or implement operation bodies. Applications provide runnable discovery and execution adapters.

See the [worker job macro proposal](docs/job-macro-proposal.md) for possible registration APIs.

## Use
See the [crate documentation](https://docs.rs/resumable-worker) for setup and API reference.


See [`examples/standalone.rs`](examples/standalone.rs) for a complete in-memory source, generic
operation registry, runner, worker startup, and graceful shutdown. Run it with:

```sh
cargo run -p resumable-worker --example standalone
```

The dependency direction and extension points are described in
[`docs/architecture.md`](docs/architecture.md). The `resumable-store-postgres-sqlx` crate remains
an independent adapter over `resumable`; applications can combine that adapter with this worker
runtime without introducing a dependency cycle.
