# Worker architecture and lifecycle

`resumable-worker` depends on `resumable` for execution identifiers, operation keys, and run
errors. It provides portable runnable-source and execution-runner contracts, an asynchronous
operation registry, worker settings, and the polling loop. It has no dependency on Planum, SQLx,
PostgreSQL, or application schema.

```text
Planum application ──> resumable-worker ──> resumable
       │                                      ▲
       └──> resumable-store-postgres-sqlx ─────┘
```

The application implements `RunnableSource` with its chosen persistence adapter and implements
`ExecutionRunner` with its engine, operation handlers, and application context. The generic
`OperationRegistry<Context, ExecutionContext>` stores JSON operation handlers while leaving both
context types under caller control. The PostgreSQL adapter depends only on the portable core; it
does not depend on the worker crate.

## Discovery and operation versions

The source receives the runner's sorted supported operation keys on every discovery call. It
returns runnable candidates for those exact keys, bounded by the requested capacity. It separately
reports persisted operation keys for which no compatible handler is registered. The runtime logs
each unsupported key once per worker lifetime and never dispatches it. Applications own version
compatibility policy and should register every version they are prepared to execute.

The runtime ignores duplicate execution identifiers while they are active and dispatches no more
than the configured concurrency. Runner errors and panics are logged; a failed task does not stop
other work. Persistence adapters remain responsible for claims, leases, and recovery semantics.

## Shutdown

`ResumableWorkerHandle::shutdown` stops polling and waits for active runner tasks up to
`shutdown_timeout`. If the deadline expires, remaining tasks are aborted and their persisted leases
are left for the adapter's recovery policy. The caller owns signal handling and must await shutdown
before tearing down resources used by its runner or source.

Environment variables and application schema validation belong to the application integration,
not this crate. The standalone example demonstrates startup and graceful shutdown without assuming
any particular persistence implementation.
