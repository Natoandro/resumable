---
title: "resumable - Specification"
subtitle: "Durable, resumable multi-step operations for Rust"
author: "Project Specification"
date: "2026-10-11"
geometry: margin=1in
fontsize: 10.5pt
header-includes:
  - |
    ```{=latex}
    \usepackage{microtype}
    \usepackage{longtable}
    \usepackage{booktabs}
    \usepackage{array}
    \usepackage{xcolor}
    \usepackage{fvextra}
    \DefineVerbatimEnvironment{Highlighting}{Verbatim}{breaklines,breakanywhere=true,commandchars=\\\{\}}
    ```
---

# 1. Status and scope

This document specifies **resumable**, a lightweight Rust library for executing multi-step asynchronous operations durably and resuming them after process failure, worker failure, restart, retry, or interruption.

The library is intended to provide a reusable correctness layer between application code and external side effects. It is not intended to be a distributed workflow platform, task queue product, deterministic replay runtime, or application-specific job framework.

The central abstraction is a **durable execution** composed of explicitly named **durable steps**.

A minimal execution looks like:

```text
Execution
   |
   +-- step: prepare       completed
   +-- step: render        completed
   +-- step: upload        running
   +-- step: publish       pending
```

If the process stops after `upload` completes but before `publish` starts, the same execution can be resumed. Completed steps are not intentionally re-executed, and unfinished steps can be retried according to policy.

This specification defines the generic features of the library only. Examples use neutral operations such as artifact generation, object-storage uploads, backups, and email delivery. No application domain is part of the core design.

## 1.1 Version target

This specification targets **resumable v0.1**.

The v0.1 goal is deliberately narrow:

> Provide durable, resumable asynchronous operations composed of explicitly named steps, with persistent execution state, retry and recovery semantics, stable idempotency identities, progress reporting, cancellation, and safe concurrent ownership.

# 2. Terminology

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** indicate requirement strength in this specification.

**Execution**  
A single durable invocation of an operation. An execution has a stable identity and lifecycle independent of any individual process or worker attempt.

**Operation**  
Application code that coordinates one or more durable steps. Examples include generating an artifact, creating a backup, restoring state, exporting data, or delivering a message.

**Step**  
A named unit of work within an execution whose completion state is persisted by resumable.

**Attempt**  
One invocation of a step body. A step may have multiple attempts because of retries, lease expiry, worker failure, or explicit rerun policy.

**Store**  
The persistence implementation used by resumable to record executions, steps, leases, results, progress, and state transitions.

**Lease**  
A time-bounded claim granting one worker the right to actively operate on an execution or step.

**Checkpoint**  
A durable fact indicating that work has reached a known state. In v0.1, completion of a named step is the primary checkpoint mechanism.

**Idempotency identity**  
A stable identity derived from the durable execution and step, suitable for deduplicating external side effects.

**Worker**  
A process or task running an execution. resumable does not prescribe how workers are created, scheduled, or transported.

# 3. Design goals

resumable SHOULD optimize for the following properties.

## 3.1 Correctness after interruption

An execution MUST be able to survive interruption between durable steps and continue later from persisted state.

The library MUST make process restart a normal execution path rather than an exceptional recovery mode.

## 3.2 Explicit durability

Durable boundaries MUST be visible in application code. v0.1 MUST NOT depend on transparent capture of arbitrary Rust futures, stack frames, or deterministic replay of user code.

For example:

```rust
let archive = ctx.step("archive", |step| async move {
    create_archive().await
}).await?;

let object = ctx.step("upload", |step| async move {
    upload_archive(step, archive).await
}).await?;
```

The named calls to `step` are the durability boundaries.

## 3.3 Infrastructure neutrality

The core crate MUST NOT require a specific database, object store, queue, message broker, web framework, runtime service, LLM provider, email provider, or cloud platform.

Persistence MUST be abstracted behind a trait.

## 3.4 Stable identity

Executions and steps MUST have stable identities that do not change between retry attempts or process restarts.

These identities MUST be available to application code so external effects can be made idempotent.

## 3.5 At-least-once reality

resumable MUST NOT claim exactly-once execution of arbitrary external side effects.

The system MUST be designed around the fact that this sequence is always possible:

```text
external side effect succeeds
          |
          v
process crashes before step completion is persisted
```

A later retry may therefore invoke the step body again.

The library MUST expose stable idempotency data so applications can reconcile this case safely.

## 3.6 Small embeddable core

The library SHOULD be usable inside a normal Rust service without requiring a separate orchestration server.

The core SHOULD remain small enough to be understandable, auditable, and testable independently of application code.

# 4. Non-goals for v0.1

The following are explicitly outside v0.1.

- Deterministic replay of arbitrary workflow code.
- Serialization or restoration of Rust futures or stack state.
- Exactly-once execution of arbitrary external effects.
- A general distributed task queue.
- A message broker.
- A built-in worker transport protocol.
- A web dashboard.
- A workflow definition language.
- General DAG scheduling.
- Child workflows.
- Human approval primitives.
- Durable signals or arbitrary event subscriptions.
- Cron scheduling.
- Agent, LLM, PDF, email, backup, storage, or accounting-specific APIs.
- Mandatory PostgreSQL, SQLite, Redis, Kafka, NATS, S3, or other vendor integrations.
- Automatic compensation or saga semantics.
- Transparent migration of incompatible workflow code across versions.

These capabilities MAY be added later as separate layers or crates if real use cases justify them.

# 5. Conceptual model

The core model is:

```text
                 +----------------------+
                 |      Application     |
                 |   operation code     |
                 +----------+-----------+
                            |
                            v
                 +----------------------+
                 |       resumable      |
                 |                      |
                 | executions           |
                 | named steps          |
                 | retries              |
                 | leases               |
                 | progress             |
                 | cancellation         |
                 +----------+-----------+
                            |
                         Store trait
                            |
            +---------------+---------------+
            |               |               |
            v               v               v
         SQLite          PostgreSQL       custom
```

External effects remain application-owned:

```text
step body
   |
   +-- filesystem
   +-- object storage
   +-- email provider
   +-- subprocess
   +-- HTTP API
   +-- AI service
   +-- application database
```

resumable records whether the step is durably known to have completed. It does not attempt to make every external system transactional with the resumable store.

# 6. Execution lifecycle

An execution MUST have a stable `ExecutionId` and one of the following states:

```rust
pub enum ExecutionStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}
```

An implementation MAY internally represent additional transient states, but the public state model SHOULD remain small.

## 6.1 State transitions

The intended lifecycle is:

```text
Pending ---> Running ---> Completed
                |
                +-------> Failed
                |
                +-------> Cancelled
```

A failed execution MAY be explicitly resumed or retried if policy permits. Such a resume SHOULD retain the same `ExecutionId` unless the caller intentionally creates a new logical execution.

## 6.2 Terminal states

`Completed` and `Cancelled` are terminal by default.

`Failed` is terminal with respect to automatic progress when retry policy has been exhausted, but the API MAY allow an operator or application to resume the same execution explicitly.

## 6.3 Execution identity

The library MUST support caller-provided execution IDs in addition to generated IDs.

Caller-provided IDs are important when the application's own domain entity already provides a natural durable identity.

Example:

```rust
engine
    .start_with_id(operation_id, "artifact-generation", input)
    .await?;
```

Attempting to create the same execution identity twice MUST have deterministic behavior. Creation is idempotent only when operation name, operation version, input codec, input schema version, and encoded input bytes match exactly. No input is compatible only with no input. A mismatch MUST return a conflict and MUST NOT overwrite the existing record. Metadata and retry policy do not define identity: the first successful create stores them, and a compatible duplicate returns that record unchanged. The default codec is `serde-json`, schema version `1`; input identity is the persisted envelope, not a re-serialization of decoded input.

# 7. Step model

Each execution contains zero or more named steps.

A step MUST be uniquely addressable within an execution by a stable `StepId` or stable step key.

For v0.1, the recommended logical key is:

```text
(execution_id, step_name)
```

The store MUST enforce uniqueness of this key or its equivalent. Step names are compared as exact UTF-8 bytes without Unicode normalization. A step name is a persistent identifier; changing it creates a different logical step.

## 7.1 Step state

A step SHOULD expose the following public states:

```rust
pub enum StepStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}
```

`Pending` MAY be implicit before a step is first persisted.

## 7.2 Step completion

A completed step MUST have durable completion metadata before resumable reports it as complete to future resumes.

A subsequent sequential call to the same named step within the same execution MUST return the recorded result without running the body when codec and schema match. A mismatch MUST return a conflict or decoding error and MUST NOT rerun the completed effect. Concurrent duplicate step calls within one execution are unsupported.

Conceptually:

```text
ctx.step("render", body)
        |
        v
Does completed step exist?
        |
   +----+----+
   |         |
  yes        no
   |         |
return      acquire ownership
stored       |
result       v
          run body
             |
             v
        persist completion
             |
             v
         return result
```

## 7.3 Step name stability

Step names form part of durable history. Once executions exist in production, renaming a step can change resume behavior.

The documentation MUST warn users that step names SHOULD be treated as stable persistent identifiers, not presentation strings.

The library MAY support a separate human-readable label.

# 8. Step API

A representative API is:

```rust
impl<S: Store> ExecutionContext<S> {
    pub async fn step<T, E, F, Fut>(
        &self,
        name: impl Into<StepName>,
        f: F,
    ) -> Result<T, StepRunError<E>>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
        F: Fn(StepContext) -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        E: StepError + Send + Sync + 'static;
}
```

An ergonomic overload SHOULD exist for default options:

```rust
let value = ctx
    .step("render", |step| async move {
        render_document(step).await
    })
    .await?;
```

The exact trait bounds are implementation details and MAY differ, but v0.1 MUST support persisted step results. `step(name, body)` uses `StepOptions::default()`; `step_with(name, options, body)` supplies explicit options. The body factory is reusable (`Fn`), and each attempt creates a fresh future. Captured values must be reusable across calls; non-recreatable values belong in an earlier completed step or other durable state. The operation closure itself is invoked once per run call and is not persisted.

# 9. Persisted step results

Persisting step results enables later steps to resume without recomputing completed work.

Example:

```rust
let object_key = ctx
    .step("upload", |step| async move {
        let key = format!("artifacts/{}/result.pdf", step.execution_id());
        storage.put(&key, bytes).await?;
        Ok(key)
    })
    .await?;

ctx.step("publish", |_| async move {
    publish_download(&object_key).await
}).await?;
```

After restart, `upload` can return the persisted `object_key` without re-uploading.

## 9.1 Serialization

The core MUST NOT require a specific application data format beyond a serialization contract.

A default implementation MAY use Serde and a binary or JSON encoding. The default codec identifier is `serde-json` with schema version `1`. Applications MAY provide other codecs through the value codec interface; codec identifiers and schema versions MUST continue to select a compatible decoder after restart.

The storage abstraction SHOULD treat step result payloads as opaque bytes plus codec/version metadata.

Example internal representation:

```rust
pub struct StoredValue {
    pub codec: String,
    pub schema_version: u32,
    pub bytes: Vec<u8>,
}
```

## 9.2 Result size

The specification SHOULD discourage storing large artifacts directly as step results.

Large data SHOULD normally be stored externally, while the step result stores a durable reference such as an object key, URI, content hash, or application identifier.

# 10. Idempotency semantics

Idempotency support is a core feature.

Every execution and step MUST expose a stable idempotency identity.

A canonical derivation is:

```text
execution key = execution_id
step key      = execution_id + step_name
attempt key   = execution_id + step_name + attempt_number
```

The library SHOULD expose all three because external systems have different semantics.

Representative API:

```rust
pub struct StepContext {
    pub execution_id: ExecutionId,
    pub step_id: StepId,
    pub step_name: StepName,
    pub attempt: u32,
}

impl StepContext {
    pub fn idempotency_key(&self) -> IdempotencyKey;
    pub fn attempt_key(&self) -> IdempotencyKey;
}
```

The stable step idempotency key MUST remain unchanged across retry attempts.

The attempt key MUST change for each attempt.

In v0.1, encode identity fields as UTF-8 or ID bytes in a versioned, domain-separated, length-prefixed tuple, hash with SHA-256, then encode as unpadded base64url with an `rs1_` prefix. Step keys include execution ID and exact step-name bytes. Attempt keys also include the unsigned 32-bit big-endian attempt number. This avoids exposing input data and makes tuple encoding unambiguous; hash collisions remain theoretically possible. Provider deduplication lasts only as long as the provider retains its idempotency record, so applications must reconcile effects beyond that window.

## 10.1 Example: object storage

A resumable upload SHOULD prefer a deterministic object key:

```text
artifacts/<execution-id>/document.pdf
```

rather than generating a new random object key on every attempt.

If an upload succeeds and the process crashes before completion is persisted, a retry can detect, validate, replace, or conditionally reuse the same object instead of creating an orphaned duplicate.

## 10.2 Example: external API

When an external API accepts an idempotency header, the stable step key SHOULD be used:

```text
Idempotency-Key: <execution-id>:deliver-message
```

## 10.3 Reconciliation

A step MAY need to reconcile resumable's state with external reality.

For example:

```text
resumable store: upload not completed
object store:    object already exists
```

The library MUST allow application code to detect this condition and return success without repeating the side effect.

resumable MUST NOT assume that absence of a local completion record proves the external side effect did not occur.

# 11. Failure and retry semantics

A step failure MUST be recorded with at least:

- step identity;
- attempt number;
- failure classification;
- timestamp;
- retry decision;
- optional diagnostic data.

The core SHOULD distinguish retryable and non-retryable failures.

Representative abstraction:

```rust
pub enum FailureClass {
    Retryable,
    Permanent,
    Cancelled,
}
```

## 11.1 Retry policy

A generic retry policy SHOULD support:

```rust
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub backoff: Backoff,
}

pub enum Backoff {
    None,
    Fixed(Duration),
    Exponential {
        initial: Duration,
        factor: f64,
        max: Duration,
        jitter: bool,
    },
}
```

The default policy is one permanent attempt. In v0.1, `max_attempts` counts total attempts including the first, numbered from 1. Retry policy is snapshotted atomically when the step is first begun and cannot be changed on a later attempt. A retryable failure is eligible only when attempts remain and the persisted retry time has arrived; permanent failure or exhaustion fails the execution. A scheduled retry releases ownership, records the execution as `Failed`, and returns the operation error. The caller must explicitly resume the execution after the retry time; the core does not run a scheduler or sleep until the deadline.

## 11.2 Retry schedule

The next eligible retry time MUST be durable if retries may outlive the current process.

A process restart MUST NOT reset the retry attempt count or backoff schedule.

## 11.3 Attempts

Attempt numbers MUST be monotonically increasing per step.

A retry MUST retain the same execution ID and step identity. Explicit resume of a failed execution is allowed only when operation identity and input still match, no step has a permanent or exhausted failure, and every scheduled retry is eligible by store time. Resume preserves completed steps, failure records, the original retry policy, and attempt numbers; it does not bypass retry timing or exhaustion. Completed and cancelled executions cannot be resumed. Every resume invokes the outer operation from its beginning; completed steps reuse stored results, so ordinary code outside steps MUST be safe to repeat. Compatibility of old operation code remains the application's responsibility.

# 12. Leases and concurrent ownership

The library MUST protect against multiple workers unintentionally running the same step concurrently.

The persistence contract MUST support leases or equivalent compare-and-swap ownership. An execution has one lease and executes steps sequentially; parallel step execution is unsupported in v0.1. Every owner mutation MUST provide the execution ID, current lease token and fencing generation, and expected execution revision. The store checks these atomically. A lease is lost as soon as store time reaches its expiry, even if no other worker has claimed it.

Each acquisition invocation MUST use a fresh claim ID. An ambiguous acquisition response is retried
with the same claim ID, worker identity, and requested duration. Only an identical request can recover
its current live claim; a different invocation MUST observe Busy even when its worker ID matches.
The last fencing generation MUST survive lease removal and increase on failure/resume as well as
expired-lease takeover. An adapter MUST persist the generation that started each step attempt and
reject restarting a Running attempt under the same generation.

A lease contains at least:

```rust
pub struct Lease {
    pub owner: WorkerId,
    pub token: LeaseToken,
    pub expires_at: Timestamp,
}
```

## 12.1 Lease acquisition

A worker MUST atomically acquire ownership before running a step if concurrent workers may observe the same runnable execution.

## 12.2 Lease expiry

If a worker stops heartbeating and the lease expires, another worker MAY claim the step.

This means two attempts can overlap around lease expiry in pathological network or scheduling conditions. Therefore leases reduce duplicate execution but MUST NOT replace idempotency.

## 12.3 Fencing

Stores SHOULD provide a monotonically increasing lease token or version that can act as a fencing token.

Where practical, application adapters MAY propagate this token to systems that support conditional writes.

# 13. Optimistic concurrency

Persistent state transitions SHOULD use optimistic concurrency.

A representative store method is:

```rust
async fn update_execution(
    &self,
    id: ExecutionId,
    expected_revision: Revision,
    change: ExecutionChange,
) -> Result<Revision, StoreError>;
```

Example:

```text
worker A reads revision 12
worker B reads revision 12

A writes expected=12 -> revision 13  success
B writes expected=12 -> conflict     reload required
```

The core SHOULD handle ordinary revision conflicts internally where safe.

# 14. Cancellation

Cancellation is a core lifecycle feature.

An execution MUST support durable cancellation requests.

The public model SHOULD distinguish:

- cancellation requested;
- cancellation observed by active code;
- execution terminally cancelled.

A minimal public API MAY expose only `cancel()` and terminal `Cancelled`, while retaining more detail internally.

## 14.1 Cooperative cancellation

Step code SHOULD be able to observe cancellation:

```rust
if step.is_cancelled().await? {
    return Err(MyError::Cancelled);
}
```

or through a cancellation token exposed by the context.

resumable MUST NOT promise to forcibly stop arbitrary blocking or external work. The runner checks cancellation before acquiring or beginning each step and exposes it through the step context. Work already in a body may finish. On success, the step result may be committed before the runner stops; on failure, the failure is recorded and cancellation determines terminal execution status. No later step begins after cancellation is observed. Cancellation and successful finish are serialized store transitions: whichever commits first determines the outcome. Repeated cancellation requests are idempotent; terminal executions are returned unchanged.

## 14.2 Completed steps

Cancellation MUST NOT erase already completed step history.

A later explicit restart policy MAY choose either to resume the cancelled execution or create a new execution.

# 15. Progress reporting

Progress is useful for long-running operations and is generic enough to belong in the core.

Progress updates MUST be optional and MUST NOT affect correctness of step completion.

Representative API:

```rust
step.progress(Progress {
    current: Some(3),
    total: Some(6),
    message: Some("Rendering output".into()),
}).await?;
```

The model SHOULD support both numeric and message-only progress:

```rust
pub struct Progress {
    pub current: Option<u64>,
    pub total: Option<u64>,
    pub message: Option<String>,
}
```

Applications MAY translate this into percentages, stage displays, logs, notifications, or UI updates.

A progress write MAY be lossy under extreme failure without compromising correctness. Step completion and execution state MUST NOT be lossy. The default `progress(...)` call is best-effort: store errors are reported to tracing diagnostics without backend error text and do not fail the step or operation. An explicit `progress_checked(...)` method MAY return the store error. Progress messages are limited to 1 KiB. Applications can inspect an execution and at most 64 caller-selected steps, including each step's latest progress. The core does not provide presentation labels or an embedded dashboard.

# 16. Heartbeats

Long-running steps SHOULD be able to heartbeat.

A heartbeat serves two purposes:

1. extend a lease;
2. optionally publish progress or liveness metadata.

Representative API:

```rust
execution.heartbeat().await?;
```

Lease duration and heartbeat interval SHOULD be configurable.

The system MUST document that a step which performs a long blocking operation without heartbeat can have its lease expire and may be retried elsewhere. Store time is authoritative for lease expiry, heartbeat, and retry eligibility; local worker time cannot authorize a claim or extend an expired lease. A heartbeat cannot revive an expired lease. If a write response is ambiguous, the runner reloads and validates status, attempt, revision, and fencing generation before deciding whether the transition committed. It MUST confirm a possibly committed `begin_step` before invoking the body. If the outcome remains unknown, it returns the store error for later recovery; it never repeats a body to resolve a store conflict.

Before invoking a begun step body, the runner confirms the current attempt and validates its lease
through a Store heartbeat. If a cancellation request survives worker loss, the replacement runner
MUST persist terminal cancellation and clear its lease without invoking operation code. Incomplete
steps or retry deadlines MUST NOT prevent acquiring expired ownership solely to finalize cancellation.

The portable core exposes explicit heartbeat calls and a recommended heartbeat interval on the
execution context. It MUST NOT spawn runtime-owned background tasks. Applications that supervise
heartbeats in a task MUST cancel and join that task when the operation ends, and MUST stop treating
the operation as successful when a heartbeat fails. A failed heartbeat latches ownership loss in
the context; subsequent core mutations fail even if application code ignores the heartbeat error.
Dropping an in-flight runner during graceful shutdown leaves its durable execution and lease
untouched. The lease expires and a later owner resumes the incomplete work; shutdown MUST NOT mark
interrupted work completed.

# 17. Execution input and metadata

An execution MAY have persisted input.

Input SHOULD follow the same opaque serialized-value model as step results.

The execution SHOULD also support small metadata suitable for filtering and diagnostics, for example:

```rust
pub struct ExecutionMetadata {
    pub labels: BTreeMap<String, String>,
}
```

Metadata MUST NOT be required for correctness unless the application explicitly uses it as such.

The core SHOULD place size limits on labels and metadata to prevent accidental use as blob storage. The core limits each encoded value to 16 MiB and metadata to 64 labels, 64 UTF-8 bytes per key, 256 UTF-8 bytes per value, and 8 KiB total key and value bytes. These bounds apply to construction and deserialization.

# 18. Store abstraction

The persistence layer is the most important external abstraction.

A v0.1 store trait SHOULD expose semantics equivalent to the following, even if the final API groups methods differently:

```rust
#[async_trait]
pub trait Store: Send + Sync + 'static {
    async fn create_execution(
        &self,
        new: NewExecution,
    ) -> Result<ExecutionRecord, StoreError>;

    async fn load_execution(
        &self,
        id: ExecutionId,
    ) -> Result<Option<ExecutionRecord>, StoreError>;

    async fn acquire_execution(
        &self,
        id: ExecutionId,
        request: LeaseRequest,
    ) -> Result<AcquiredExecution, StoreError>;

    async fn load_step(
        &self,
        execution: ExecutionId,
        name: &StepName,
    ) -> Result<Option<StepRecord>, StoreError>;

    async fn begin_step(
        &self,
        request: BeginStep,
    ) -> Result<StepRecord, StoreError>;

    async fn complete_step(
        &self,
        request: CompleteStep,
    ) -> Result<StepRecord, StoreError>;

    async fn fail_step(
        &self,
        request: FailStep,
    ) -> Result<StepRecord, StoreError>;

    async fn update_progress(
        &self,
        request: ProgressUpdate,
    ) -> Result<(), StoreError>;

    async fn request_cancellation(
        &self,
        execution: ExecutionId,
    ) -> Result<(), StoreError>;

    async fn heartbeat(
        &self,
        request: Heartbeat,
    ) -> Result<Lease, StoreError>;
}
```

The exact method set MAY change during implementation. The semantic requirements below are normative.

## 18.1 Store atomicity requirements

The store MUST atomically enforce:

- execution identity uniqueness;
- step identity uniqueness within an execution;
- lease acquisition ownership;
- monotonic attempt numbers;
- legal state transitions;
- optimistic revision or equivalent concurrency checks;
- step completion recording.

A store implementation that cannot provide these guarantees is not conformant.

## 18.2 Transactions

The core SHOULD NOT require the application and resumable store to share the same database transaction.

However, a store adapter MAY expose extension points allowing an application to enqueue or create an execution inside an existing transaction when both use the same database.

This can support patterns such as:

```text
application transaction
   |
   +-- write domain record
   +-- create durable execution
   |
 commit atomically
```

Such integration is adapter-specific and MUST NOT leak into the portable core API.

# 19. Runner and scheduling boundary

v0.1 MUST NOT require a built-in distributed scheduler.

The library SHOULD support at least two usage styles.

## 19.1 Direct execution

The application starts or resumes an execution directly:

```rust
engine.run(execution_id, operation).await?;
```

This is suitable when the application already controls worker scheduling.

## 19.2 Polling worker

An adapter or helper MAY query runnable executions and process them:

```text
poll runnable executions
        |
        v
acquire lease
        |
        v
run operation
        |
        v
complete / retry / release
```

A convenience worker module MAY exist outside the minimal core.

The distinction is important: **durable execution semantics belong in the core; job distribution does not.**

# 20. Recovery behavior

Recovery MUST be ordinary execution against persisted state.

On restart, the engine SHOULD:

1. load the execution;
2. validate lifecycle state;
3. acquire or renew ownership;
4. invoke operation code;
5. for each named step, reuse persisted completed results;
6. retry eligible incomplete steps;
7. preserve retry attempt counters and timing;
8. respect cancellation;
9. complete the execution when operation code returns successfully.

Example:

```text
Before crash
------------
prepare     completed
render      completed
upload      completed
publish     running

After restart
-------------
prepare     reused
render      reused
upload      reused
publish     retried/reconciled
```

# 21. Crash windows and guarantees

The library documentation MUST describe crash windows explicitly.

## 21.1 Before a side effect

```text
step begins
process crashes
external effect never occurs
```

Retry is safe if the step itself is safe to retry.

## 21.2 After side effect, before durable completion

```text
step begins
external effect succeeds
process crashes
completion not persisted
```

This is the critical ambiguity. A retry MAY repeat the step body.

The application MUST use idempotency or reconciliation when duplication would be harmful.

## 21.3 After durable completion

```text
external effect succeeds
completion persisted
process crashes
```

On resume, the step SHOULD return the persisted result without intentionally invoking the body again.

# 22. Error model

The public error model SHOULD distinguish at least:

```rust
pub enum ResumableError<E> {
    Operation(E),
    Store(StoreError),
    Serialization(SerializationError),
    LeaseLost,
    Cancelled,
    Conflict,
    InvalidState,
}
```

Step bodies SHOULD be able to classify operation errors as retryable or permanent.

Store errors SHOULD distinguish transient infrastructure errors from semantic conflicts when possible.

Internal state corruption or impossible transitions MUST surface clearly and MUST NOT be silently converted into ordinary retries.

# 23. Time abstraction

Retry scheduling, leases, and timestamps depend on time.

The core SHOULD abstract its clock sufficiently to support deterministic tests.

For example:

```rust
pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
}
```

A production implementation uses system time. Tests can use a manually advanced clock.

The clock abstraction SHOULD NOT attempt to solve distributed clock synchronization. Store-side time MAY be preferable for lease comparisons in some adapters.

# 24. Observability

resumable SHOULD integrate with the Rust tracing ecosystem but MUST NOT require a specific telemetry backend.

Useful structured fields include:

- execution ID;
- operation kind;
- step name;
- step ID;
- attempt number;
- lease owner;
- execution revision;
- retry delay;
- duration;
- final status.

The library SHOULD emit tracing spans around execution and step attempts. Spans may include execution/operation/step identity, attempt, revision, retry delay, duration, and status, but MUST NOT include serialized inputs/results or application error text by default. Persisted failure diagnostics are omitted unless the application explicitly enables them.

Metrics MAY be supplied by an optional integration crate or feature.

# 25. Data retention

Completed execution history can grow indefinitely.

The core SHOULD expose enough information for applications or adapters to implement retention policies.

v0.1 MAY include generic deletion or purge primitives, but automatic retention policy is not required.

A purge MUST NOT occur while an execution is running or may still require completed step results for correct resume behavior.

The core purge primitive removes only completed or cancelled executions and their step and
progress history in one store operation. Pending, running, and failed executions are rejected;
failed executions remain eligible for explicit resume under application policy. Store adapters
must serialize purge with claims and cancellation transitions.

Applications SHOULD decide when terminal executions are safe to remove. Retention policy should
account for audit and cancellation history, sensitive persisted input and results, backups, and any
external artifact references. Purging core history does not delete external files or revoke links;
applications own that cleanup. An execution ID may identify new work after its old record is
purged, so applications SHOULD NOT reuse IDs while provider idempotency keys, artifact references,
or other deduplication records derived from that ID may still be live.

# 26. Versioning and code evolution

Explicit named steps reduce replay complexity but do not eliminate compatibility concerns.

Consider an execution persisted with:

```text
prepare -> render -> upload -> publish
```

and later code changed to:

```text
prepare -> transform -> render -> upload -> publish
```

An old execution resumed under new code may now encounter a new step.

In v0.1, the library MAY permit this and execute the new step, but the behavior MUST be documented as application-controlled compatibility.

An operation identity includes a version:

```rust
OperationKey {
    name: "generate-artifact",
    version: 2,
}
```

Applications MAY use versions to route old executions to compatible operation implementations.

`resume` MUST reject a version mismatch with an explicit unsupported-version error before invoking
the supplied operation body. Applications that retain compatible older implementations can route
the stored `(name, version)` to that implementation and resume with its matching key. The core does
not include a dispatch registry or silently run a newer version.

Step names are durable identifiers. Keep names stable across deployments; renaming one creates a
new step and can repeat its effects. Adding a step in a newer operation version causes that step to
run when the compatible operation resumes an execution that has not completed it. Applications
own compatibility of input, result, and step schemas and should retain compatible decoders and
operation code for as long as their retention policy permits resuming old work.

Automatic workflow migration is outside v0.1.

# 27. Security considerations

The core library MUST treat persisted data as potentially sensitive.

It SHOULD avoid logging serialized inputs, results, or errors by default.

Store adapters SHOULD document whether data is encrypted at rest by the underlying system.

The library MUST NOT assume that step outputs are safe to expose to UIs or logs.

Idempotency keys SHOULD avoid embedding secrets. Stable opaque IDs are preferred over raw user data.

Cancellation, retry, and manual resume APIs SHOULD be callable only through application authorization boundaries. Authorization itself is not a core responsibility.

# 28. Suggested crate structure

A minimal repository could begin with:

```text
resumable/
+-- Cargo.toml
`-- src/
    +-- lib.rs
    +-- engine.rs
    +-- execution.rs
    +-- step.rs
    +-- retry.rs
    +-- lease.rs
    +-- progress.rs
    +-- store.rs
    +-- clock.rs
    +-- codec.rs
    `-- error.rs
```

Storage adapters SHOULD preferably be separate crates:

```text
resumable
resumable-sqlite
resumable-postgres
```

An application MAY instead implement `Store` directly.

# 29. Representative public API

The following is illustrative rather than final syntax.

```rust
use resumable::{
    Engine,
    ExecutionId,
    RetryPolicy,
    StepOptions,
};

async fn generate_artifact(
    engine: &Engine<impl resumable::Store>,
    execution_id: ExecutionId,
) -> anyhow::Result<String> {
    engine
        .run(execution_id, "generate-artifact", |ctx| async move {
            let source = ctx
                .step("prepare", |step| async move {
                    prepare_source(step).await
                })
                .await?;

            let artifact = ctx
                .step_with(
                    "render",
                    StepOptions::default()
                        .retry(RetryPolicy::exponential(3)),
                    |step| async move {
                        render(step, source).await
                    },
                )
                .await?;

            let object_key = ctx
                .step("upload", |step| async move {
                    let key = format!(
                        "artifacts/{}/output.bin",
                        step.execution_id()
                    );

                    // Application-owned idempotency/reconciliation.
                    upload_or_reuse(&key, artifact).await?;
                    Ok(key)
                })
                .await?;

            ctx.step("publish", |_step| async move {
                mark_available(&object_key).await
            })
            .await?;

            Ok(object_key)
        })
        .await
}
```

The important semantics are:

- the execution has a stable identity;
- each step has a stable name and identity;
- completed step results are durable;
- retries do not create new logical steps;
- external side effects can use stable idempotency keys;
- interruption does not require starting the operation from zero.

# 30. Example: backup-style operation

This example demonstrates reconciliation around object storage.

```rust
let archive = ctx
    .step("archive", |step| async move {
        create_archive(step).await
    })
    .await?;

let object_key = ctx
    .step("upload", |step| async move {
        let key = format!(
            "snapshots/{}/archive.bin",
            step.execution_id()
        );

        if storage.exists_and_matches(&key, archive.hash()).await? {
            return Ok(key);
        }

        storage.put(&key, archive.bytes()).await?;
        Ok(key)
    })
    .await?;

ctx.step("finalize", |_step| async move {
    record_available_snapshot(&object_key).await
}).await?;
```

Possible failure:

```text
archive       completed
upload        external upload completed
              process crashes before resumable records completion
finalize      never started
```

After restart, `upload` runs again because resumable cannot prove it finished. The step checks the deterministic object key, validates the existing object, and returns success without creating a duplicate.

# 31. Example: document-style operation

A long-running artifact pipeline might be:

```text
collect-input
     |
     v
draft
     |
     v
review
     |
     v
render
     |
     v
upload
     |
     v
publish
```

The application can report progress independently of durable completion:

```rust
ctx.step("draft", |step| async move {
    step.progress(Progress::message("Drafting sections")).await?;
    let draft = generate_draft().await?;
    step.progress(Progress::message("Draft complete")).await?;
    Ok(draft)
}).await?;
```

If review succeeds but rendering later fails, the drafting and review results remain reusable on resume.

# 32. Example: message delivery

Delivery is a useful illustration of external idempotency.

```rust
ctx.step("deliver", |step| async move {
    provider
        .send_message(SendRequest {
            idempotency_key: step.idempotency_key().to_string(),
            destination,
            body,
        })
        .await
}).await?;
```

If the provider supports idempotency, a retry after an ambiguous crash can safely return the result of the original delivery instead of duplicating the message.

If the provider does not support idempotency, the application must choose its own reconciliation or duplicate-tolerance strategy.

# 33. Minimal persistence schema example

The core does not mandate a relational database, but a relational adapter might use records conceptually equivalent to:

```text
executions
--------------------------------------------------
id                 primary key
operation_name
operation_version
status
revision
input
result
cancel_requested
lease_owner
lease_token
lease_expires_at
created_at
updated_at
completed_at

steps
--------------------------------------------------
execution_id       foreign key
name
status
attempt
result
error
next_attempt_at
lease_owner
lease_token
lease_expires_at
started_at
completed_at
updated_at

UNIQUE(execution_id, name)
```

Progress MAY be stored on the step row or in a separate append-only or latest-value table.

Attempt history MAY be stored separately if detailed diagnostics are desired.

# 34. Core invariants

A conforming implementation MUST preserve the following invariants.

1. An execution identity refers to at most one logical execution.
2. A step name refers to at most one logical step within an execution.
3. A completed step remains completed unless an explicit administrative reset API is invoked.
4. A retry increments the attempt number but retains the same logical step identity.
5. A stable step idempotency key does not change between attempts.
6. Step completion is persisted before future resumes are allowed to treat the step as complete.
7. A worker cannot legitimately mutate a leased step after losing its lease or fencing token.
8. Cancellation state is durable.
9. Retry scheduling state is durable.
10. Progress failure cannot turn a completed step into an incomplete step.
11. Store concurrency conflicts cannot silently overwrite a newer state.
12. The library never infers that an external side effect did not occur solely because local completion is absent.

# 35. Testing requirements

The core SHOULD have exhaustive tests for failure boundaries.

At minimum, tests SHOULD cover crashes or simulated interruption:

- before step body invocation;
- during a step body;
- after a side effect but before completion recording;
- after completion recording;
- during retry scheduling;
- during lease renewal;
- after cancellation request;
- during concurrent resume attempts.

A reusable conformance test suite SHOULD be provided for `Store` implementations.

A store adapter SHOULD pass the same semantic tests regardless of backend.

Property tests are particularly suitable for state-machine invariants.

# 36. Suggested v0.1 implementation phases

## Phase 1 - Core state model

Implement:

- IDs;
- execution state;
- step state;
- serialization envelope;
- store trait;
- in-memory store for tests;
- explicit step API.

## Phase 2 - Recovery and retries

Implement:

- persisted step results;
- retry classification;
- backoff;
- retry timestamps;
- resume behavior.

## Phase 3 - Concurrency safety

Implement:

- leases;
- heartbeats;
- fencing/revisions;
- concurrent resume tests.

## Phase 4 - Lifecycle features

Implement:

- cancellation;
- progress;
- operation versioning metadata;
- retention hooks.

## Phase 5 - First production adapter

Implement one real persistence adapter and run the conformance suite against it.

The adapter choice SHOULD be based on the first consuming applications rather than hard-coded into the core design.

# 37. Deferred candidates for future versions

The following features are reasonable future candidates but SHOULD remain outside v0.1 until justified by concrete use cases.

- Durable waiting for external events.
- Signals.
- Human approval steps.
- Parallel branches.
- DAG dependencies.
- Child executions.
- Sagas and compensation.
- Built-in scheduling and cron.
- Distributed worker transport.
- Worker discovery.
- Execution priority.
- Rate limiting.
- Resource classes.
- Step timeouts enforced by supervisors.
- Streaming step output.
- Administrative step reset.
- Execution migration helpers.
- Web UI and inspection API.
- First-party SQLite and PostgreSQL adapters.

# 38. Summary

`resumable` is a small durable-execution kernel for Rust applications that need reliable multi-step operations without adopting a full workflow platform.

Its defining characteristics are:

- explicit named durable steps;
- stable execution and step identities;
- persisted step results;
- crash recovery and resume;
- durable retry state;
- leases and concurrency protection;
- cooperative cancellation;
- progress and heartbeats;
- stable idempotency keys;
- persistence behind a generic store trait;
- no deterministic replay requirement;
- no mandatory scheduler, server, queue, database, or cloud service.

The library's principal guarantee is intentionally modest and useful:

> Once a step is durably recorded as completed, future resumes of the same execution can reuse that completion instead of intentionally repeating the step. When completion is ambiguous because failure occurred around an external side effect, the library provides stable identities and state needed for the application to reconcile or deduplicate safely.

That boundary keeps resumable generic enough for independent reuse while remaining substantially simpler than a general workflow runtime.
