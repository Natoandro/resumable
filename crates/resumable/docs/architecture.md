# Resumable v0.1 architecture

This document defines the concrete v0.1 design of the resumable library. The
[specification](specification.md) describes product scope and required capabilities; this architecture
sets the execution model, API behavior, persistence transitions, and recovery rules that implement
that contract. These statements describe the desired system and are maintained with the design.

## Execution identity and creation

- Execution IDs are stable, may be supplied by callers, and identify at most
  one logical execution. Creating a compatible duplicate is idempotent; an incompatible duplicate
  conflicts (sections 6.3 and 34).
- `start` encodes input once as a `StoredValue`. For a caller-provided execution
  ID, compatibility requires exact equality of operation name, operation version, input codec,
  input schema version, and encoded input bytes. No input is compatible only with no input. A
  mismatch returns `Conflict`; it never overwrites the original record. Metadata and retry policy
  are not part of creation identity. The first successful create stores them, and later compatible
  creates return that existing record unchanged.
- The default codec is `serde-json`, schema version `1`. Callers may provide a
  different codec and schema version through the value codec interface. Input identity is the exact
  persisted envelope, not a re-serialization of a decoded value.

## Ownership and step ordering

- Concurrent workers must not legitimately mutate the same leased work;
  writes after lease loss must be rejected (sections 12 and 34).
- An execution has one lease and runs its operation sequentially. All steps inherit
  the execution lease token and fencing generation. There is no parallel step execution in v0.1.
  This keeps ordering and cancellation behavior explicit and makes the execution the ownership
  boundary.
- Every owner mutation supplies the execution ID, lease token/generation, and
  expected execution revision. The store checks these atomically. A stale owner cannot begin,
  fail, complete, or update progress for a step, nor finish the execution. A lease is considered
  lost as soon as store time reaches its expiry, even if no other worker has claimed it yet.

Acquisition has an invocation-specific claim ID in addition to the stable worker ID. A retry of an
ambiguous claim reuses the same ID, worker, and requested duration; a distinct invocation observes
Busy even when the worker matches. The execution record retains `fencing_generation` after lease
removal, and every new ownership generation advances that counter. The lease also retains the claim
ID and original requested duration for identical-request validation. Each Running step attempt stores
its starting generation in adapter-owned state so a live attempt cannot restart under the same lease.

## Atomic transitions

Every transition below is one atomic store operation. Each mutation rechecks the current status and
revision in that operation (with compare-and-swap or an equivalent transaction/row lock). “Owner”
means the current execution lease token/generation and expected revision are valid and the lease
has not expired according to the store's clock.

| Transition                    | Preconditions                                                                                  | Atomic effects                                                                                                                                                                                                                                                                                                                                                                    |
| ----------------------------- | ---------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Create execution              | ID absent                                                                                      | Insert `Pending`, revision 0, immutable operation identity and input envelope. A duplicate ID returns the existing record only when the creation compatibility fields match; otherwise conflict.                                                                                                                                                                                  |
| Acquire execution             | `Pending`; eligible `Failed` with no permanent/exhausted step; or `Running` with expired lease | Claim or replace the lease, increment fencing generation and revision, and set `Running`. A failed record is moved through `Pending` in the same atomic operation. A live lease cannot be stolen. Completed and cancelled executions cannot be acquired.                                                                                                                          |
| Begin step                    | Owner; execution running; step absent or eligible for a new attempt                            | Insert/update the unique `(execution_id, step_name)` row as `Running`; persist the initial retry policy if absent, reject a changed policy, and increment the monotonic attempt number and execution revision. A prior `Running` step from an expired execution lease is eligible for a new attempt under the new fencing generation. Reject a live step and an ineligible retry. |
| Reuse completed step          | Step is `Completed`; requested codec/schema matches stored result                              | Read/decode its stored result without invoking the body or mutating state. Codec/schema mismatch is a conflict or decode error.                                                                                                                                                                                                                                                   |
| Complete step                 | Owner; step running under this generation and attempt                                          | Persist result envelope and completion time; set step `Completed`; increment execution revision. This is the durable checkpoint.                                                                                                                                                                                                                                                  |
| Fail step                     | Owner; step running under this generation and attempt                                          | Validate the supplied policy against the first-begin snapshot. Persist failure classification/diagnostic and optional `next_attempt_at`; increment execution revision. The runner returns the step error and records execution failure in a separate transition, which sets the execution to `Failed` and clears its lease.                                                       |
| Request cancellation          | Execution is nonterminal                                                                       | For `Pending`, set `Cancelled` immediately. For `Running`, set the durable cancellation-requested flag. Increment revision. Repeated requests are idempotent. A terminal execution is returned unchanged.                                                                                                                                                                         |
| Update progress               | Owner; step belongs to the execution                                                           | Replace the latest progress value and increment revision. It does not change step or execution status.                                                                                                                                                                                                                                                                            |
| Heartbeat                     | Owner; current lease is unexpired                                                              | Extend lease using store time and increment revision. An expired lease or stale generation returns `LeaseLost`; heartbeat cannot revive it.                                                                                                                                                                                                                                       |
| Finish execution              | Owner; execution running                                                                       | If cancellation was requested, set `Cancelled`; otherwise set `Completed`. Increment revision and clear the lease. This is serialized with cancellation requests, so whichever store transition commits first determines the outcome.                                                                                                                                             |
| Fail operation outside a step | Owner; operation returned an error                                                             | Persist execution failure and time. Set `Cancelled` if cancellation was requested, otherwise `Failed`; increment revision and clear the lease.                                                                                                                                                                                                                                    |

Step identity uniqueness is enforced by the store on `(execution_id, exact UTF-8 step-name bytes)`.
Names are persistent identifiers; renaming a name changes history. Step-name comparison does not
apply Unicode normalization.

## Retry, resume, and operation replay

- Attempt numbers increase monotonically; retry timing and attempt counts
  survive restart; completed results are reused (sections 11, 20, and 34).
- `max_attempts` counts total attempts in the current execution, including the
  first. Attempts are numbered from 1. A retryable failure is eligible only when attempts remain and its persisted
  `next_attempt_at` has arrived. The runner returns the body error, records execution failure, and
  releases its lease. A later explicit resume can claim the failed execution when all pending retries
  are eligible. The Store rejects acquisition when a failed step is permanent or exhausted. Retry
  policy is persisted atomically with the first `begin_step`; later calls cannot silently change it,
  including when the first attempt was interrupted before reporting a failure.
- Explicit `resume` may reacquire a `Failed` execution only when its operation
  identity and input envelope still match, no step has a permanent or exhausted failure, and any
  pending retry is eligible by store time. It preserves completed steps, failure records, and
  attempt numbers. It does not bypass a step's retry schedule or exhausted attempt limit. An
  exhausted execution therefore needs a new execution ID for another attempt in v0.1. `Completed`
  and `Cancelled` are terminal and cannot be resumed.
- Every resume invokes the outer operation function from its beginning. Calls to
  previously completed steps return their decoded stored result without invoking the body. Ordinary
  code outside a step can run again and must be safe to repeat. A new step name added by a newer
  operation version runs as new work; routing old executions to compatible operation code remains
  the application's responsibility.
- `resume` requires an exact operation name and version match. A matching name with an unsupported
  requested version returns `UnsupportedOperationVersion` before operation code runs; the core has
  no dispatch registry. Step names and their data schemas are application compatibility contracts.
- A repeated sequential call to the same step name returns its stored completed
  result if the requested codec/schema matches. A mismatched value type/codec is a serialization or
  conflict error. Concurrent duplicate calls are unsupported because operation steps are
  sequential.
- `ExecutionContext::step` and `step_with` use an `Fn(StepContext)` body
  factory, record attempts before body invocation, and persist encoded results before returning.
  Mutation IDs are retried once after an unavailable Store response, then the execution revision is
  refreshed before the next transition. Revision or ownership conflicts are returned without
  retrying the mutation; the runner never repeats a step body to resolve a store conflict.
  `StepOptions` defaults to one permanent attempt. Retryable policies and their calculated deadline
  are persisted with a failed attempt.

After `begin_step` returns (including replay after response loss), the runner validates the current
Running execution's owner, token, and generation, reloads the current step identity/attempt, and
heartbeats through the Store before invoking the body. Ownership changes or expired leases stop
execution. An already committed begin receipt alone does not authorize an external effect.

## Rust API and retry closure ownership

- Provide a default-options `step` and an options-taking `step_with`; Rust
  cannot overload methods by argument count (section 8 and illustrative section 29).
- `step(name, body)` delegates to `step_with(name, StepOptions::default(), body)`.
  The body factory is reusable (`Fn(StepContext) -> Fut`), and a fresh future is created on every
  attempt. It is not `FnOnce`. Captured values must be reusable across calls (for example, clone an
  `Arc` inside the factory); values that cannot be recreated should be produced by an earlier
  completed step or otherwise made durable. The operation passed to `run` is invoked once per run
  call and is not persisted as a closure.
- The core uses native async methods in traits. Where the runner requires
  multithread-safe futures, public `Store` methods return `impl Future<Output = T> + Send`; concrete
  implementations may use `async fn`. The trait is statically dispatched in v0.1, so it does not
  need `dyn` compatibility or boxed futures.

## Cancellation and lease loss

- Cancellation requests are durable and completed steps remain recorded. Arbitrary futures and external work are not forcibly stopped (section 14).
- The Store persists cancellation requests. A pending execution becomes `Cancelled` immediately;
  a running execution records the request, and its finish transition produces `Cancelled`.
  Execution and step contexts expose `is_cancelled()` for cooperative observation. After worker loss,
  a replacement runner may acquire expired ownership despite leftover step failures or retry deadlines
  to finalize cancellation without invoking operation code. It persists `Cancelled` and clears the lease.
- Cancellation and successful execution completion are linearized by their atomic
  store transitions. If cancellation commits first, finish produces `Cancelled`; if finish commits
  first, a later cancellation is a no-op and the execution remains `Completed`.
- A Store rejects further writes from an owner after lease loss, even if its future continues. The current runner surfaces the Store conflict rather than mapping it to the public `ResumableError::LeaseLost` variant. An already-started external effect may still complete; applications use the stable step key to deduplicate or reconcile it.

## Clock authority and ambiguous writes

- Time is abstractable for tests; lease and retry decisions must be durable
  and correct across restarts (sections 11, 12, 16, and 23).
- Store time is authoritative for lease expiry, heartbeat, and retry eligibility.
  Adapters obtain it inside the atomic operation (for example, PostgreSQL `clock_timestamp()`). A
  current runner calculates a proposed retry deadline from local time and persists it with the
  failure; the Store clock decides when that deadline is due. A worker's local clock cannot
  authorize a claim or extend an expired lease.
- If a write returns `Unavailable`, the runner retries supported mutations once with the same
  request and mutation ID. The Store resolves an identical committed request to its recorded result
  before checking current ownership; reuse of that ID with different data conflicts. After success,
  the runner reloads the execution revision before its next owner mutation. If the response remains
  unavailable, the runner returns the store error and does not invoke a body whose `begin_step`
  outcome is unknown. A later run recovers from durable state after a new valid claim.

## Idempotency key encoding

- Execution and step keys are stable across attempts; attempt keys change;
  secrets should not be embedded (sections 10 and 27).
- Encode identity fields as UTF-8/ID bytes in a versioned, domain-separated,
  length-prefixed tuple; hash the tuple with SHA-256; encode the digest using unpadded base64url
  with an `rs1_` prefix. Step keys include execution ID and exact step-name bytes. Attempt keys
  include those fields and the unsigned 32-bit big-endian attempt number. Length prefixes make
  tuples unambiguous; domain tags separate execution, step, and attempt keys. This produces compact
  provider-friendly values without exposing input data. The chosen hash makes collision risk
  negligible, not mathematically impossible.
- Provider idempotency is only as durable as the provider's retention window.
  After that window, callers must reconcile external state or use a new deliberate identity; the
  library does not claim exactly-once effects.

## Progress data

- Progress is optional and observational. `StepContext::progress` is best-effort;
  `progress_checked` exposes store errors. Both use the current lease and revision, and a
  potentially committed heartbeat response is reconciled against the same owner and generation.
- Progress messages are limited to 1 KiB. Inspection loads one execution and at most 64
  caller-selected steps, including each step's latest progress. Presentation labels belong to
  consuming applications.
