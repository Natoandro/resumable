# resumable

Generic Rust crate for durable, resumable asynchronous operations composed of named steps.

The [v0.1 specification](docs/specification.md) defines the library contract, and the
[architecture](docs/architecture.md) defines its execution model, persistence transitions, and
recovery behavior. The [HTML guide](docs/site/index.html) covers the concepts for integrators
and implementation audiences. See [stored value and metadata guidance](docs/value-storage.md)
for codec and size-limit contracts.

The portable async persistence API is the [`Store` contract](src/store.rs). Store methods describe
atomic postconditions and fencing requirements; concrete adapters own transactions, database
queries, worker discovery, and dispatch.

The core owns durability semantics and a portable Store contract. It has no dependency on
Planum, Axum, SQLx, an email provider, an artifact storage service, or an async runtime. Generic
examples and store conformance support belong with this crate so they can accompany its later
extraction into a separate repository.

## Direct execution and restart

`Engine` supports direct application-owned dispatch. Call `start` for a generated identity,
`start_with_id` when the application has a durable identity, and `resume` after loading an ID from
application-owned state or a queue. The engine always loads the stored input before invoking the
operation closure, so a restart uses the original input and operation version. Reusing an ID with a
different operation or encoded input returns a conflict. Completed executions return their stored
result without calling application code.

Each active runner must use a stable worker ID for the duration of its lease. `EngineConfig` rejects
failed executions by default; applications can opt into `FailedExecutionPolicy::Resume`. A claimed
failed execution restarts under the same execution ID and retained input. Applications remain
responsible for dispatching pending and expired executions after process restart; resumable does
not provide a scheduler or queue.

Store acquisition also carries a unique claim ID per invocation. The Engine reuses that ID only
when retrying an ambiguous acquisition response; another call observes Busy even if it uses the
same configured worker ID. Stores retain the last fencing generation independently of the active
lease, so failure and resume cannot reuse an earlier external fencing token. Adapters persist each
Running step's attempt generation and reject a second begin under the same generation.

Before invoking a begun step body, the Engine reloads the attempt and confirms the lease with a
Store heartbeat. A recovered cancellation is finalized without running operation code, clearing
the lease even when the interrupted execution still has incomplete steps.

Pass a scheduled execution to the store with
`NewExecution::new(id, operation, created_at).with_run_at(deadline)`. The deadline is a Unix
timestamp in milliseconds. The PostgreSQL worker discovers the job once the database clock
reaches that deadline, and the store rejects direct claims before then. Pending jobs can be
cancelled during the wait. Omit `with_run_at` for immediate eligibility. Reusing an execution ID
requires the same operation, input, and schedule.

Operation names, versions, and named step identifiers are durable data. Keep them stable across
deployments, and increment the operation version when code can no longer safely interpret an older
execution's persisted data.

Use `start_with_context` / `start_with_id_and_context` or `resume_with_context` when the operation
needs durable steps. `ExecutionContext::step` records the attempt before calling its reusable `Fn`
body and stores its serialized result before returning. On resume, a completed step returns its
stored value without calling the body. Step calls must be sequential within an execution. Reusing a
name with a result type that cannot decode the stored value returns a value error. Step bodies can
use `StepContext` for the execution ID, step ID, attempt number, and stable step/attempt idempotency
keys. `StepContext::is_cancelled()` and `ExecutionContext::is_cancelled()` read the durable
cancellation request. `Engine::cancel(id)` records a request independently of the worker; pending
executions become cancelled immediately, while running work observes the request cooperatively.
The runner never starts another step after a request commits. Already running work may finish and
persist its step result, then the execution becomes cancelled. Cancellation does not undo external
side effects. Cancelled executions stay terminal; restart by creating a new execution and use
application idempotency or reconciliation where effects may already have happened. `step_with`
accepts `StepOptions`. Retry policies count total attempts including the first;
body errors are permanent by default. Set `retryable: true` to opt into bounded retries with
`max_attempts` and fixed or capped exponential backoff. Retry deadlines and a policy snapshot are
stored with the failed step. Calls before the deadline return control to the caller; resumable
never holds a worker lease while sleeping.

`StepContext::fencing_token()` exposes the execution lease generation. External systems that
support conditional writes can store the largest accepted generation and reject older requests.
Store fencing prevents stale workers from changing resumable state; it cannot stop an already
started request to a system that does not check the token. Use stable idempotency keys and reconcile
external state for those effects.

Long-running operations can call `ExecutionContext::heartbeat()`. The context exposes
`heartbeat_interval()` as the recommended
cadence; configure `EngineConfig::lease_duration` to bound how long ownership remains available
after a worker stops. The portable core does not spawn a timer or heartbeat task. If the application
runs heartbeats in a background task, it owns that task and must cancel and join it when the
operation finishes. Coordinate heartbeat calls with step mutations so their revision checks do not
race. Treat any heartbeat error as ownership loss and stop the operation; the context also rejects
later durable mutations. A step that blocks the executor thread cannot be supervised
by an async heartbeat task on that same thread, so run blocking work on the application's blocking
pool and heartbeat from a separately scheduled task.

Graceful shutdown should stop dispatching new executions and allow in-flight work to finish within
the shutdown window. If a runner future is dropped or the process exits, the execution remains
incomplete with its lease; after lease expiry another owner can resume it and reuse completed steps.
Do not record successful completion as part of shutdown.

## Reference store and conformance

`reference::MemoryStore` is a mutex-protected in-memory implementation for development and tests.
Pass a `ManualClock` to control lease and retry eligibility, and use `FaultInjector` to simulate a
failure before commit or a committed write whose response was lost. It is not durable storage.

Adapter test suites can call `conformance::run_store_scenarios(&store)` against a fresh adapter
instance. The scenario matrix and reference-store-specific concurrency and recovery tests are
listed in [the conformance coverage document](../../docs/resumable/reference-store-conformance.md).

Build the core by itself from the repository root:

```bash
cargo build -p resumable
```

## Portable examples and release checks

Runnable examples use fake object storage and a fake message provider, so they need no Planum
services or async runtime:

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
