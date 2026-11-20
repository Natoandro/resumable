# Resumable project assessment

The workspace has a solid architecture for durable, versioned Rust operations. The portable engine, Tokio worker, and PostgreSQL adapter have clear responsibilities. Lease fencing, mutation receipts, and shared store conformance tests are worth preserving. The recovery and shutdown findings below should take priority before relying on the bundled worker for unattended processing.

## Highest-priority findings

2. **A concurrent failure can bypass `FailedExecutionPolicy::Reject`.** The engine checks a loaded snapshot, then claims by ID. If another owner fails the execution between those operations, the store can resume it. Enforce the policy as part of the atomic claim. [Engine](crates/resumable/src/engine/run.rs#L16) · [PostgreSQL claim](crates/resumable-store-postgres-sqlx/src/postgres/claims.rs#L122)

3. **Progress and transactional checkpoints fail when composed.** `StepContext::completion_request()` uses the revision captured at step entry. A successful `progress_checked()` advances that revision, so PostgreSQL rejects a later checkpoint even when both calls are sequential. Build the request with the current lease revision and verify progress followed by a domain write and checkpoint in one transaction. [Step context](crates/resumable/src/engine/step_context.rs#L29) · [Lease validation](crates/resumable-store-postgres-sqlx/src/postgres/mod.rs#L145)

4. **Worker shutdown can wait indefinitely during discovery.** The worker awaits discovery inside the polling branch, where it no longer polls the stop signal. The drain timeout starts after that branch returns. If an execution task panics, a separate bookkeeping issue clears every active ID and permits duplicate dispatch of unrelated work. Make discovery interruptible and remove only the failed task’s execution ID. [Worker loop and shutdown](crates/resumable-worker/src/runtime.rs#L92)

5. **Default worker logs can expose application error text.** The engine omits persisted diagnostics by default but returns raw text in `RunError::Operation(String)`. The worker logs that error using `Debug`. Log error categories and identifiers by default, with detailed diagnostics available by opt-in. [Engine error path](crates/resumable/src/engine/run.rs#L87) · [Worker logging](crates/resumable-worker/src/runtime.rs#L159)

## Assessment by area

### 1. API design

`Engine<S, C = JsonCodec>` provides a straightforward default and a codec extension point. The `Store` trait uses static dispatch with `Send` futures, while boxed worker interfaces support runtime composition. Naming is mostly consistent, although `ClaimOutcome::Terminal` can carry a record with status `Running`. `NewExecution::new().with_*()` works well for the required and optional inputs. Public mutable records and exhaustive enums make API evolution harder. Resume inputs only need to be deserialized, and step bodies only need `FnOnce` because each call invokes the body at most once. Generated-ID starts should preserve the ID on failure so callers can recover work after durable creation. The tagged `v0.1.1` to `v0.1.2` production source changes were documentation-only. [Store](crates/resumable/src/store.rs#L12) · [Request builder](crates/resumable/src/store/requests.rs#L34) · [Resume](crates/resumable/src/engine/resume.rs#L4) · [Step API](crates/resumable/src/engine/steps.rs#L4) · [Generated-ID starts](crates/resumable/src/engine/create.rs#L4)

### 2. Correctness

Checked counters, stable step identities, and deterministic discovery ordering are good defaults. The reference store accepts completion of a predecessor’s running attempt with a fresh takeover guard where PostgreSQL rejects it. Exponential backoff accepts a `Duration::MAX` configuration whose float conversion panics. A standalone Rust probe reproduced the conversion failure. Retry deadlines start from the worker clock and are evaluated against the store clock, so skew changes the effective delay. [Reference step completion](crates/resumable/src/reference/store/steps.rs#L119) · [PostgreSQL step completion](crates/resumable-store-postgres-sqlx/src/postgres/steps/complete.rs#L42) · [Backoff](crates/resumable/src/engine/retry.rs#L57) · [Retry timestamp](crates/resumable/src/engine/step_body.rs#L61)

### 3. Ownership and memory

`Arc` sharing fits the engine and worker, and lifetimes are mostly local. The easiest copy to remove is the completed-result clone before decoding because codecs already accept a borrowed envelope. Resume also loads the full execution twice. The reference store clones records under a global mutex, which fits its role in tests and examples. [Result decoding](crates/resumable/src/engine/mod.rs#L104) · [Resume](crates/resumable/src/engine/resume.rs#L37) · [Reference store reads](crates/resumable/src/reference/store/executions.rs#L27)

### 4. Error handling

`StoreError` distinguishes conflicts, corruption, and backend unavailability, but string conversion loses structured causes and makes retry decisions coarse. The execution-failure path skips the same-ID retry used by other mutations after an ambiguous response. It can also lose a race with cancellation and leave an execution running until takeover. Validate worker settings synchronously so a zero poll interval produces a startup error instead of a spawned-task panic. [Error types](crates/resumable/src/error.rs#L17) · [Failure path](crates/resumable/src/engine/run.rs#L87) · [Worker startup](crates/resumable-worker/src/runtime.rs#L65)

### 5. Rust idiomaticity

Validated identifier newtypes, `Result`, pattern matching, and standard-library synchronization fit the code well. The biggest type-design opportunity is state validation. Public records can represent contradictory combinations of status, lease, and result. PostgreSQL’s decode helpers return deserialized records without checking those cross-field invariants. [Execution record](crates/resumable/src/record/execution.rs#L36) · [PostgreSQL decoding](crates/resumable-store-postgres-sqlx/src/postgres/mod.rs#L46)

### 6. Architecture

Polling and dispatch live in the worker, SQLx transactions live in the adapter, and the core owns durable state semantics. The main maintenance cost is expressing eligibility and transition rules across the engine, the reference store, and multiple SQL queries. Make the intended behavior explicit in the contract and enforce parity through shared scenarios. [Store contract](crates/resumable/src/store.rs#L12)

### 7. Concurrency and async

The core is runtime-independent and uses short synchronous mutex sections. Heartbeats are application-managed, and concurrent heartbeat and step mutations can conflict on revision. Keep the documented sequential execution model and define how heartbeat coordination interacts with checkpoints. Validate worker concurrency at startup. The current `usize` is narrowed to `u32`, and PostgreSQL rejects discovery limits above 1,000. [Heartbeat guidance](crates/resumable/docs/usage.md#L62) · [Capacity conversion](crates/resumable-worker/src/runtime.rs#L110) · [PostgreSQL limit](crates/resumable-store-postgres-sqlx/src/postgres/runnable.rs#L23)

### 8. Performance

The largest likely cost is database work. Claims fetch and deserialize every step record under an execution-row lock, including completed outputs. Writes duplicate payloads across record storage and receipts, heartbeats rewrite execution snapshots, and inspection performs two sequential reads per named step. Benchmark claim latency, heartbeat write volume, and polling under backlog before tuning smaller Rust allocations. The review found no benchmark harness and measured no throughput. [Claim path](crates/resumable-store-postgres-sqlx/src/postgres/claims.rs#L26) · [Receipts](crates/resumable-store-postgres-sqlx/src/postgres/mod.rs#L100) · [Heartbeat persistence](crates/resumable-store-postgres-sqlx/src/postgres/steps/progress.rs#L41) · [Inspection](crates/resumable/src/engine/inspection.rs#L16)

### 9. Safety and security

The review found no first-party `unsafe` or FFI. Core and adapter forbid unsafe code. Payload limits bound accepted values, but JSON serialization and deserialization can allocate before enforcement. The adapter leaves TLS feature selection to the consumer’s SQLx setup, which deserves explicit guidance for remote databases. The locked Tokio and SQLx versions contain the fixes for the advisories checked during the review. A full dependency audit remains unverified. [Value limits](crates/resumable/src/value.rs#L27) · [Adapter manifest](crates/resumable-store-postgres-sqlx/Cargo.toml#L13) · [Tokio interval advisory](https://rustsec.org/advisories/RUSTSEC-2026-0384.html) · [Tokio I/O advisory](https://rustsec.org/advisories/RUSTSEC-2026-0386.html) · [SQLx advisory](https://rustsec.org/advisories/RUSTSEC-2024-0363.html)

### 10. Testing

Fault injection, fencing, receipt replay, and real PostgreSQL transactions give the project good failure-path coverage. Add regressions for the findings above and property tests for transition sequences. Serialized-record validation is a useful fuzz target. The PostgreSQL suite groups many scenarios into one test and falls back to an unrelated local database URL before running migrations. Require an explicit disposable database and split cases where isolation permits. [PostgreSQL test harness](crates/resumable-store-postgres-sqlx/tests/postgres/mod.rs#L12)

### 11. Dependencies and compilation

The dependency split is sensible. The adapter appears to have unused direct `thiserror` and `tracing` declarations. SQLx macros are isolated to the adapter, with offline query metadata included in the package. CI checks the declared Rust 1.85 MSRV on Linux. This review verified Rust 1.100 locally. Other platforms and clean compile times remain unmeasured. `std` is integral to the current core, so `no_std` would be a separate design effort. [Workspace manifest](Cargo.toml#L9) · [Adapter manifest](crates/resumable-store-postgres-sqlx/Cargo.toml#L13) · [CI](.github/workflows/ci.yml#L36)

### 12. Documentation

The README, rustdoc, architecture notes, and restart walkthrough explain crate boundaries and the need for idempotency or reconciliation. The biggest gaps are method-level guarantees for configuration errors and panics, inspection consistency, and how progress, heartbeat, and transactional completion compose. The specification also omits scheduled start from duplicate-creation identity even though the implementation and usage guide include it. [README](README.md) · [Core usage](crates/resumable/docs/usage.md#L26) · [Specification](crates/resumable/docs/specification.md#L266) · [PostgreSQL walkthrough](crates/resumable/docs/postgres-worker.md)

### 13. Distribution and maintenance

MIT licensing, crate metadata, version checks, and CI are in place. The publish workflow should gate on successful verification and skip versions already published when recovering from a partial release. Define compatibility and deprecation expectations for public Rust types, stored data, and schema upgrades. CI exercises a fresh PostgreSQL schema, so upgrading a previous release’s database is an important additional release check. [License](LICENSE) · [Publish workflow](.github/workflows/publish.yml#L25) · [PostgreSQL CI](.github/workflows/ci.yml#L17)

## Verification

- Formatting, the offline workspace build, and Clippy with warnings denied passed.
- All 66 unit tests and three doctests passed. Rustdoc built with warnings denied.
- The PostgreSQL conformance suite and interrupt-and-resume walkthrough passed on a disposable PostgreSQL 17.11 instance. The standalone worker example also passed.
- Isolated probes reproduced stalled worker shutdown and two revision issues through the public API. With the reference store, progress made the step’s completion request stale, and cancellation followed by a body error left the execution running with a durable cancellation request.
- The remaining high-priority scenarios were identified from source review and were outside the exercised suite.
