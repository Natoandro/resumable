# Resumable functional review

Workspace version 0.1.2 · 20 November 2026

## Executive summary

Resumable already provides the core of an embeddable durable executor. It persists explicitly named checkpoints, reuses completed results on restart, and gives applications stable keys for reconciling effects. Its portable core, Tokio poller, and PostgreSQL adapter form a coherent architecture. Caller-owned SQLx transactions are already available for atomic submission and for committing a domain write with a step checkpoint.

Two recovery gaps prevent unattended use of the default engine and bundled PostgreSQL polling path for operations that opt into retries. A retryable step error that escapes the operation body ordinarily becomes a failed execution, which the default engine refuses to resume. Separately, work discovery, claims, and finalization disagree on when work can advance. An interrupted execution with a permanently failed step can remain recorded as running, and a cancellation can remain pending after its owner disappears. Older failed records can also consume every slot in a bounded poll. Fix retry outcomes and lifecycle eligibility together before adding scheduling or administrative features.

The next production priorities are safe lease renewal during long work, composable transaction checkpoints, and bounded worker shutdown with payload-safe outcome reporting. Typed submission and dispatch, operator inspection, and retention controls come after those guarantees. A second durable backend or a durable wait should require evidence of a consumer with a concrete need.

## Library purpose and scope

Resumable is for Rust application teams that need to restart multi-step asynchronous work after a process failure, interrupted deployment, or transient dependency error. Representative consumers include artifact pipelines, backup jobs, delivery integrations, and services coordinating database writes with external APIs. Store authors are a second audience. They implement atomic transitions behind the portable `Store` contract and can run shared conformance scenarios.

- An execution has a stable UUID, a versioned operation key, and persisted input. On resume, application code runs from the beginning with stored input. Named sequential steps replay their stored results. Applications must keep step identities stable and put replay-sensitive work inside explicit boundaries.
- A store owns lease expiry, fencing, revisions, and mutation receipt reconciliation. A worker discovers candidates and delegates claims and execution to an application runner. The core depends on neither Tokio nor SQLx. The PostgreSQL adapter uses a caller-supplied pool.
- The guarantee is durable checkpoints with at-least-once reality for interrupted external effects. Applications own provider idempotency, reconciliation, authorization, resource handles, code compatibility, and business policy. A PostgreSQL domain write can share the checkpoint transaction if both use the same database transaction. [README](README.md) · [Core specification](crates/resumable/docs/specification.md) · [Core architecture](crates/resumable/docs/architecture.md) · [Worker architecture](crates/resumable-worker/docs/architecture.md) · [Adapter architecture](crates/resumable-store-postgres-sqlx/docs/architecture.md)

### Comparable libraries and domain practices

| Reference | Relevant practice | Fit for Resumable |
| --- | --- | --- |
| Underway | In-process Rust jobs with sequential steps and PostgreSQL transaction participation. | A close operational benchmark for enqueue and transaction setup. Resumable's named checkpoints and portable store support a different integration model from a prescribed chain of steps. [Underway](https://docs.rs/underway/latest/underway/) |
| Apalis | Background task and message processing with worker lifecycle controls and middleware for concerns such as timeouts and rate limits. | Validate worker controls and extension ergonomics. Queue, middleware, and dashboard parity would broaden the product considerably. [Apalis](https://docs.rs/apalis/latest/apalis/) |
| Temporal | Durable workflow message passing and explicit workflow versioning in a broader orchestration system. | Useful for thinking about bounded wait semantics and version retirement. Replicating its workflow runtime is outside the embeddable checkpoint use case. [Message passing](https://docs.temporal.io/develop/go/workflows/message-passing) · [Workflow versioning](https://docs.temporal.io/develop/go/workflows/versioning) |

## Existing feature inventory

“Implemented” means the scoped capability is present and has repository evidence. “Partial” covers an available capability with a material completeness or composition gap. “Deferred” means a documented candidate, with no delivery commitment inferred.

| Area | Status | Implemented behavior and limits |
| --- | --- | --- |
| Submission and dispatch | Implemented for direct execution, partial for hosted use | Caller-supplied IDs, generated-ID start, persisted input and result, exact-version resume, and idempotent creation by ID, operation, encoded input, and scheduled time. Metadata and one-off `run_at` use `NewExecution`. The worker registry stores JSON handlers keyed by exact operation version. Applications wire submission and dispatch. [Engine creation](crates/resumable/src/engine/create.rs) · [Store requests](crates/resumable/src/store/requests.rs) · [Registry](crates/resumable-worker/src/registry.rs) |
| Named checkpoints and effects | Implemented | Completed results short-circuit step bodies. Step and attempt keys, attempt numbers, and fencing tokens support reconciliation and downstream conditional writes. Included examples cover snapshot, delivery, and artifact recovery. [Step API](crates/resumable/src/engine/steps.rs) · [Step context](crates/resumable/src/engine/step_context.rs) · [Core examples](crates/resumable/examples/) |
| Ownership and ambiguous writes | Implemented mechanisms, partial recovery | Claims, takeovers, revision checks, and mutation receipts support guarded persistence. Reference fault injection covers before- and after-commit failures. Failure finalization and some ownership paths diverge or are missing coverage. [Store](crates/resumable/src/store.rs) · [Conformance](crates/resumable/src/conformance/mod.rs) · [PostgreSQL recovery tests](crates/resumable-store-postgres-sqlx/tests/postgres/transaction_guarantees/) |
| Retry and scheduling | Partial | Step policy is pinned when first begun. Fixed, zero, and exponential backoff with optional jitter are available. Retry deadlines persist and eligibility uses store time. One-off delayed starts work at submission. Retry classification is static and the default operation outcome can strand a scheduled retry. [Retry policy](crates/resumable/src/engine/retry.rs) · [Run path](crates/resumable/src/engine/run.rs) · [Scheduled starts migration](crates/resumable-store-postgres-sqlx/migrations/0002_scheduled_starts.sql) |
| Leases, progress and cancellation | Partial | Explicit heartbeat, latest bounded step progress, durable cancellation requests, immediate cancellation of pending executions, and cooperative observation inside bodies. Takeover, cancellation, and heartbeats have composition gaps. [Progress](crates/resumable/src/engine/progress.rs) · [Execution state](crates/resumable/src/record/execution.rs) · [Discovery](crates/resumable-store-postgres-sqlx/src/postgres/runnable.rs) |
| Values and evolution | Implemented primitives, partial operations | Bounded opaque value envelopes, JSON default, codec extension point, bounded metadata, and exact operation versioning. Applications supply compatible decoding and handlers. The adapter ships migrations, package-local SQLx metadata, and exact schema validation. [Value codec](crates/resumable/src/value.rs) · [Schema validation](crates/resumable-store-postgres-sqlx/src/postgres/migration.rs) |
| PostgreSQL transactions | Implemented primitives, partial composition | Atomic store transitions, caller-owned enqueue and checkpoint transactions, attempt records, rollback and unknown-commit tests. A step completion request can carry a stale revision after progress. [Transaction contract](crates/resumable-store-postgres-sqlx/docs/architecture.md#connections-and-transactions) · [Step completion](crates/resumable-store-postgres-sqlx/src/postgres/steps/complete.rs) |
| Polling and worker lifecycle | Partial | Local concurrency, active-ID tracking, version-filtered discovery, unsupported-version warnings, and a timeout on draining runner tasks. Discovery can block shutdown and a task panic clears tracking for unrelated tasks. [Worker runtime](crates/resumable-worker/src/runtime.rs) |
| Inspection, telemetry and retention | Partial | Read by execution ID and caller-supplied names, tracing, opt-in persisted diagnostics, purge by ID for completed or cancelled executions. PostgreSQL records attempt history internally. There is no public paged inspection, retirement of failed work, or bounded retention selection. [Inspection](crates/resumable/src/engine/inspection.rs) · [Lifecycle](crates/resumable-store-postgres-sqlx/src/postgres/lifecycle.rs) · [Initial schema](crates/resumable-store-postgres-sqlx/migrations/0001_resumable.sql) |
| Broader orchestration and backends | Deferred or out of scope | Durable waits, signals, child executions, parallel branches, priority classes, cron and SQLite appear as deferred candidates. PostgreSQL appears in the same historical list and has since shipped. The HTML guide also trails current cancellation and progress APIs. [Deferred candidates](crates/resumable/docs/specification.md#37-deferred-candidates-for-future-versions) · [HTML guide](crates/resumable/docs/site/using-the-core.html#support-status) |

## Functional completeness assessment

The simplest supported path is a caller-identified execution with short sequential steps and idempotent effects. The repository demonstrates a real process exit during a PostgreSQL-backed step, followed by recovery of the interrupted step and reuse of the earlier committed result. The default retry path and recovery from interruptions between step failure and execution finalization need separate verification from that successful restart path.

The issue is the agreement between three layers. The engine decides which execution outcomes are eligible for resume, the adapter decides atomic claim eligibility, and the worker relies on the source's bounded candidate set. Recovery requires an eligible or finalizable disposition to survive any interruption between writes and to be discoverable. More worker concurrency cannot repair disagreement between those layers.

All three crates' source, public documentation and examples, store scenarios, adapter migrations and SQLx metadata, tests, manifests, and CI and release configuration informed this review. The pre-existing `PROJECT_ASSESSMENT.md` was useful for cross-checking shared findings and remains separate. Verification used Rust 1.100.0 and a fresh disposable PostgreSQL 17.11 database. Offline all-target build, formatting, Clippy with warnings denied, 66 unit tests, three doctests, and the PostgreSQL conformance and recovery integration test passed. The PostgreSQL restart walkthrough passed. Public-API probes reproduced the default retry failure, PostgreSQL discovery starvation, and the failure to checkpoint after progress. Other edge cases are source findings unless explicitly marked as reproduced. Mixed-version upgrades, throughput, and clock skew under production conditions were not measured.

## Missing essential features

### E1. Durable retry disposition

**Category** Essential · **Current state** Partial · **Priority** P0 · **Complexity** High

**Problem and use cases.** An explicit retry policy with attempts remaining can leave work blocked after a transient error, such as an unavailable artifact store. `run_step` persists a retryable step, but a propagated error goes through the generic operation failure path as permanent. Default `Reject` then prevents resuming that execution. The reference retry tests use `Resume`; opting into it globally also changes how unrelated failed operations are treated. Two writes create a second failure boundary if the process exits after recording the failed step. Timing compounds the ambiguity: the engine creates absolute retry deadlines from process time, which the store evaluates against its own clock. The two stores check attempt budgets for failed attempts, while expired running attempts can restart under a later generation. A repeatedly crashing body can consume attempts without invoking the failed-attempt budget check.

**Evidence.** [Step outcome](crates/resumable/src/engine/step_body.rs) · [Outer operation outcome](crates/resumable/src/engine/run.rs) · [Default policy](crates/resumable/src/engine/config.rs) · [Retry fixture](crates/resumable/src/reference/tests/engine/mod.rs) · [PostgreSQL attempt admission](crates/resumable-store-postgres-sqlx/src/postgres/steps/begin.rs) · [Reference attempt admission](crates/resumable/src/reference/store/steps.rs)

**Proposed solution.** Introduce a persisted disposition for a due or future retry, separate from a failed execution requiring explicit action. Make the transition that records an uncaught retryable failure and releases the lease atomic, or make recovery derive the same disposition from the committed step when that transition was interrupted. Keep an application handler's intentional error distinct from an engine control outcome and provide a typed operation adapter that preserves it through `?`. Compute the due time from store time plus a bounded delay. Define whether interrupted attempts share the error budget, and keep completion and cancellation rules consistent with that decision. Preserve existing completed checkpoints and use an explicit compatibility rule for historical ambiguous failures.

**API sketch.** `RunOutcome<T>::{Completed(T), RetryScheduled { at: Timestamp }, Cancelled}`, with `ClaimOutcome::Deferred { until: Timestamp }` and a typed operation error that can carry the step's disposition. The state names in storage can be decided independently.

**Alternatives and impact.** Applications can opt into `Resume` and write their own supervisor, or retry an individual provider request inside a step. The latter consumes that step's runtime and lease and loses its delay on restart. A durable disposition makes retryable work advance by default and removes the need to enable broad failed-execution resume.

**Compatibility and dependencies.** Likely a Store protocol and schema change. Adding variants to public exhaustive enums breaks matches, so consider additive entry points. Upgrade historical rows conservatively and specify old-reader behavior. Develop jointly with E2. Introduce per-error classification under X1 once this contract is stable.

### E2. Consistent discovery, claims and terminalization

**Category** Essential · **Current state** Insufficient · **Priority** P0 · **Complexity** High

**Problem and use cases.** PostgreSQL returns failed rows regardless of runner policy. With oldest-first ordering and a bounded limit, enough old failures rejected by the default engine can starve later jobs. Checking `Reject` before claiming also races with a different owner committing failure. Discovery excludes executions with a future retry or permanent step failure even when cancellation is pending. Direct claims can bypass the step filters for cancellation, but engine dispatch checks failed status and decodes input before claiming. Consequently a missing handler or bad input can prevent cancellation from finishing through normal polling. If a process dies after recording a permanent step failure and before recording the outer failure, claim can report `Terminal` while storage still says `Running` and discovery can omit the row indefinitely. A concurrent cancellation can also invalidate the revision used by the engine's error path, which lacks the completion path's reconciliation and same-ID retry.

**Evidence.** [Runnable SQL](crates/resumable-store-postgres-sqlx/src/postgres/runnable.rs) · [Atomic claim](crates/resumable-store-postgres-sqlx/src/postgres/claims.rs) · [Reference claim](crates/resumable/src/reference/store/executions.rs) · [Engine run and failure](crates/resumable/src/engine/run.rs) · [Completion reconciliation](crates/resumable/src/engine/completion.rs) · [Runnability tests](crates/resumable-store-postgres-sqlx/tests/postgres/runnable.rs)

**Proposed solution.** Define an eligibility matrix for pending, leased, interrupted, retry-delayed, permanently blocked and cancelling executions. Apply it in discovery and under the claim lock. Treat discovery as a hint and pass explicit claim intent to the atomic operation. Reconcile orphaned cancellations and terminal step failures under the store lock without dispatching user code when the persisted state is sufficient. Use replayable mutation IDs for failure finalization, refresh and recheck ownership on the relevant cancellation race, and make terminal outcomes correspond to a persisted state. Test the default engine with the actual PostgreSQL worker wiring, a batch of rejected failures ahead of healthy work, cancellation during backoff, and a crash between failure writes. Add takeover conformance coverage. The reference store checks execution ownership and attempt number during completion without the PostgreSQL check that the current attempt was begun under that generation.

**API sketch.** `ClaimRequest { id, worker, mutation_id, lease_duration, intent: ClaimIntent }`, with distinct scheduled-work and explicit-recovery intents. A bounded maintenance pass could finalize expired cancellation requests even when a handler is absent.

**Alternatives and impact.** Custom discovery can avoid some starvation, and an operator can use an ID-specific recovery path for some stuck rows. Only a shared transition contract addresses claim races and durable convergence. This restores progress for unaffected executions and makes status reflect the recoverable lifecycle state.

**Compatibility and dependencies.** Changing `Store::acquire_execution` affects custom adapters. Prefer a versioned contract or an additive method whose guarantees adapters can actually uphold. Define compatibility for old discovery queries and persisted statuses alongside E1.

### E3. Safe lease renewal for long work

**Category** Essential · **Current state** Partial · **Priority** P1 · **Complexity** High

**Problem and use cases.** The bundled worker can run a long export or quiet provider call past the default 30-second lease while a second worker takes over. `heartbeat_interval` is a hint. Applications must interleave renewals manually. A supervisor racing with guarded mutations can use a stale revision and mark the context lost. A heartbeat result that arrives out of order can also overwrite the revision adopted locally by a newer operation. Progress is another path that renews a lease.

**Evidence.** [Engine configuration](crates/resumable/src/engine/config.rs) · [Heartbeat and progress](crates/resumable/src/engine/progress.rs) · [Revision adoption](crates/resumable/src/engine/context.rs) · [Worker runner interface](crates/resumable-worker/src/runtime.rs) · [Heartbeat tests](crates/resumable/src/reference/tests/engine/heartbeat.rs)

**Proposed solution.** Establish a core ownership protocol that safely composes renewals, guarded writes, and progress. One option is to decouple pure lease renewal from state revisions while keeping token, generation, and store-time checks. Another is to serialize the relevant short store operations for each run. Use a Tokio supervisor at the worker integration boundary, with validated lease cadence, cancellation propagation, and explicit lease-loss outcome. The embedding API must specify how a transaction in flight interacts with renewal. Dropping a Rust future may leave a remote request running. Keep reconciliation mandatory for externally observed effects.

**API sketch.** `runner.with_lease_supervision(LeaseSupervision { renew_every, loss_policy })`, backed by a lease handle scoped to the claimed run.

**Alternatives and impact.** A longer lease or hand-written heartbeats can support known workloads today. A longer lease also delays recovery. Shared renewal handles quiet I/O without requiring each consumer to build its own ownership protocol.

**Compatibility and dependencies.** An opt-in worker helper can be additive. Changing the relationship between renewal and revision requires Store compatibility work. Coordinate with E2, E4 and X2.

### E4. Transactional checkpoint composition

**Category** Essential · **Current state** Insufficient · **Priority** P1 · **Complexity** Medium

**Problem and use cases.** A step may report progress before persisting a domain row and its result in the same PostgreSQL transaction. `StepContext::completion_request` captures a step-entry lease revision. Checked progress advances that revision, so a subsequent transaction checkpoint can conflict even with purely sequential calls. The caller also encodes output and manages a stable mutation request across an uncertain commit. Receipt replay of a checkpoint cannot undo unrelated SQL already repeated in the new transaction.

**Evidence.** [Step context](crates/resumable/src/engine/step_context.rs) · [Progress](crates/resumable/src/engine/progress.rs) · [PostgreSQL lease validation](crates/resumable-store-postgres-sqlx/src/postgres/mod.rs) · [Checkpoint integration](crates/resumable-store-postgres-sqlx/src/postgres/steps/complete.rs) · [Caller-owned transaction tests](crates/resumable-store-postgres-sqlx/tests/postgres/transaction_guarantees/)

**Proposed solution.** Build the checkpoint request from current owned state with attempt and revision checks intact. Specify guard lifetime and how renewal coordinates with checkpointing. Keep SQLx outside the core. Provide an adapter-side receipt reconciliation path or explicitly documented sequence for uncertain commits so callers can verify the original checkpoint before deciding whether to repeat their domain writes. Test progress then completion, rollback, takeover, and commit with a lost reply. If an adapter helper accepts a domain closure, make transaction and lock order explicit.

**API sketch.** `let checkpoint = step.prepare_completion(&codec, &output)?;` followed by `store.complete_step_in_transaction(&mut tx, checkpoint.request().clone()).await?`. A checkpoint handle preserves the mutation identity until the caller knows the commit outcome.

**Alternatives and impact.** Consumers can avoid in-step progress and manually keep the request, or use domain-level idempotency or an outbox. Correcting the checkpoint builder closes the composition bug while preserving PostgreSQL's strength as a co-transactional adapter.

**Compatibility and dependencies.** An additive helper can coexist with the current request builder. Coordinate the concurrency design with E3. Cross-database or remote calls still require an application-level effect protocol.

## Missing expected features

### X1. Per-error retry decisions and structured outcomes

**Category** Expected · **Current state** Partial · **Priority** P2 · **Complexity** Medium

**Problem and use cases.** A delivery integration may retry rate limiting or dependency downtime, fail immediately on invalid input, and honor a provider's requested delay. The step's Boolean classifies all its body errors together until the budget runs out. A delay computed by the provider cannot be fed into the persisted retry decision through the engine. Some expected states are returned as generic conflicts, and propagating errors through the legacy operation boundary loses their type.

**Evidence.** [Step options](crates/resumable/src/engine/retry.rs) · [Failure classification](crates/resumable/src/engine/step_body.rs) · [Run errors](crates/resumable/src/engine/config.rs)

**Proposed solution and API sketch.** Add a classified step form with an error mapper `Fn(&E) -> FailureAction`, where `FailureAction::Retry { after: Option<Duration> }` uses the persisted budget and a bounded delay, and `FailureAction::Fail` records a permanent outcome. Keep the classifier in application code and persist its concrete decisions. Keep the existing policy as the default behavior for callers of `step_with`.

**Alternatives and impact.** Consumers can wrap provider requests with short in-step retry loops or split phases with different static policies. The classified form handles mixed outcomes and provider-directed delays in one durable step without introducing a provider-specific integration.

**Compatibility and dependencies.** Add an API alongside existing methods. Depends on E1 and typed deferral from the claim contract in E2.

### X2. Bounded worker lifecycle and payload-safe reporting

**Category** Expected · **Current state** Insufficient · **Priority** P1 · **Complexity** Medium

**Problem and use cases.** A service shutting down during a stalled database call can exceed its termination deadline because discovery awaits inside the selected polling branch. A panicked runner clears all active IDs, enabling duplicate dispatch of unrelated work. Zero poll interval can panic in the spawned task, zero concurrency leaves a worker idle, and the capacity cast and PostgreSQL discovery limit require validation or bounded requests. The worker logs errors with debug formatting, including application text surfaced as `RunError::Operation(String)`, despite the core's default exclusion of persisted diagnostics. A service also needs to distinguish retry scheduling, cancellation, lease contention, and real failure without parsing log text.

**Evidence.** [Poll, drain, and logging paths](crates/resumable-worker/src/runtime.rs) · [Worker settings](crates/resumable-worker/src/settings.rs) · [Runtime tests](crates/resumable-worker/src/runtime/tests.rs) · [PostgreSQL limit](crates/resumable-store-postgres-sqlx/src/postgres/runnable.rs) · [Observability specification](crates/resumable/docs/specification.md#24-observability)

**Proposed solution and API sketch.** Add `try_start(...) -> Result<Option<WorkerHandle>, WorkerStartError>` and a shutdown method returning a structured drain outcome. Put discovery and stop handling under an interruptible deadline, enforce local capacity, and associate each spawned task ID with its execution ID. Emit stable, redacted tracing fields for outcome class and phase. Keep execution and operation identifiers available for correlation and keep high-cardinality IDs out of aggregate metrics. Let the application opt into detailed diagnostics. Document that a blocking task which never yields can exceed a cooperative shutdown budget.

**Alternatives and impact.** A custom polling loop can bound discovery and implement safe logging today. Improving the stock loop earns its place as a production integration and avoids divergent behavior across consumers.

**Compatibility and dependencies.** Settings validation and an additive startup entry point can ship early. Full outcome classification benefits from E1–E2. Preserve a migration path for callers parsing current logs or relying on the existing handle.

### X3. Operator inspection and bounded retention

**Category** Expected · **Current state** Partial · **Priority** P2 · **Complexity** High

**Problem and use cases.** Support staff can inspect a known ID if they supply exact step names, but cannot enumerate failed work, list dynamically named steps, or see an attempt timeline through supported APIs. `inspect` may issue one execution read plus two reads per requested step. The reads can observe different revisions. An operator evaluating an old handler for retirement needs counts or pages of unfinished executions by version. Purge requires known IDs and rejects failed executions indefinitely, even when an operator has decided that they should never resume.

**Evidence.** [Engine inspection](crates/resumable/src/engine/inspection.rs) · [Portable Store surface](crates/resumable/src/store.rs) · [PostgreSQL history and cascade tables](crates/resumable-store-postgres-sqlx/migrations/0001_resumable.sql) · [PostgreSQL purge](crates/resumable-store-postgres-sqlx/src/postgres/lifecycle.rs)

**Proposed solution and API sketch.** Start with adapter-side `list_executions(filter, cursor, limit)` and `list_steps(id, cursor, limit)` returning payload-free summaries by default. Include attempt lookup where retained and specify snapshot semantics per call. Add a guarded `retire_failed(id, expected_revision)` transition that forbids later claims and allows the existing purge contract to expand safely. Use a bounded terminal selector and recheck eligibility when purging. Document receipt lifetime and execution-ID reuse. Applications provide tenancy, authorization, policy, automation, and external artifact cleanup.

**Alternatives and impact.** Consumers can use a read-only SQL view and retain an application-side index of execution IDs. A supported adapter API protects them from coupling this operational plane to internal tables and gives the application explicit, observable retirement semantics.

**Compatibility and dependencies.** Read-only methods are additive. Retirement needs a compatible state extension or a persisted flag under the claim lock. Depends on E2, and the inspection filters are useful for X4.

### X4. Version retirement and schema upgrade contract

**Category** Expected · **Current state** Partial · **Priority** P2 · **Complexity** Medium

**Problem and use cases.** Versioned dispatch lets adjacent handlers coexist against the same supported schema. It does not tell a deployment whether an old handler can be removed safely, including delayed work and executions that could still be explicitly resumed. The adapter validates an exact schema at startup, and the example separately calls validation. A schema-changing rolling upgrade has no documented compatible overlap. Existing CI covers a fresh schema and restart, without a fixture from an earlier release containing live histories.

**Evidence.** [Versioned resume](crates/resumable/src/engine/resume.rs) · [Adapter validation](crates/resumable-store-postgres-sqlx/src/postgres/migration.rs) · [Adapter compatibility plan](crates/resumable-store-postgres-sqlx/docs/architecture.md#schema-and-release-compatibility) · [CI](.github/workflows/ci.yml)

**Proposed solution and API sketch.** Publish a supported stop-and-upgrade sequence now, including the order for stopping workers, migrating, validating, and restoring compatible handlers and codecs. Test a previous schema populated with pending, leased, retryable, and completed histories and retained receipts. Use `list_executions(filter.for_operation(old_key).resumable(), cursor, limit)` for the drain check. Add an explicit reader/writer compatibility window for selected additive migrations only after a consumer needs overlapping binaries and a fixture can prove compatibility.

**Alternatives and impact.** Keep exact-version validation and coordinate a brief brownout during schema changes. This remains a reasonable default. A tested upgrade and retirement contract gives operators reliable procedures and prevents jobs from silently losing their compatible handler.

**Compatibility and dependencies.** Documented upgrades and fixtures are additive. A true overlap policy affects schema design and release gates and must account for E1–E2. Depends on X3 for application-facing drain visibility.

## Desirable features

### D1. Typed submission and registry-backed dispatch

**Category** Desirable · **Current state** Partial · **Priority** P2 · **Complexity** Medium

**Problem and use cases.** A web service that registers multiple typed operations must assemble `NewExecution`, invoke the codec, map source candidates, load the right handler, and convert registry values through JSON. High-level generated-ID starts return the ID only on success, leaving a caller unable to find its own possibly committed submission after a start error. A custom runner can support a custom codec today at the cost of more integration code.

**Evidence.** [Generated and explicit starts](crates/resumable/src/engine/create.rs) · [New execution builder](crates/resumable/src/store/requests.rs) · [JSON registry](crates/resumable-worker/src/registry.rs) · [Example adapters](crates/resumable-store-postgres-sqlx/examples/postgres_worker.rs)

**Proposed solution and API sketch.** Offer `engine.prepare_execution(id, operation, &input, options) -> Result<NewExecution, ValueError>` with metadata and `run_at`, reusable with either the Store or its transaction entry point. Add a typed registration helper whose erased handler calls the engine using its configured codec. Make the submitted ID available through a receipt or typed start error even when durable creation is uncertain. Keep existing source and runner traits available for application integrations.

**Alternatives and impact.** Generate the ID before calling the current API and keep a small per-service registry adapter. A preferred integration path reduces boilerplate for standard applications while preserving typed decoding and transaction interoperability.

**Compatibility and dependencies.** Additive methods and adapters can preserve existing flows. The recommended runner uses E1's outcomes and X2's worker contract. A PostgreSQL convenience bridge can live at the integration boundary if the crate dependency split is important.

### D2. Measured PostgreSQL overhead reduction

**Category** Desirable · **Current state** Insufficient · **Priority** P3 · **Complexity** Medium

**Problem and use cases.** Long imports with many completed steps can make a PostgreSQL claim read and decode all step snapshots while holding an execution-row lock. Discovered IDs can overlap across workers, and the poller queries unsupported versions at every eligible tick. Those paths could consume a shared pool under backlog. There is no workload evidence yet establishing the dominant cost.

**Evidence.** [Claim reads](crates/resumable-store-postgres-sqlx/src/postgres/claims.rs) · [Discovery SQL](crates/resumable-store-postgres-sqlx/src/postgres/runnable.rs) · [Worker polling](crates/resumable-worker/src/runtime.rs) · [Receipt and snapshot schema](crates/resumable-store-postgres-sqlx/migrations/0001_resumable.sql)

**Proposed solution and API sketch.** Benchmark a large per-execution history and a contended backlog first. Measure query count, rows and bytes read, lock hold time, queue lag, and pool wait. If the costs are material, query compact eligibility fields under the lock, rate-limit unsupported-version checks, and expose worker settings such as `unsupported_probe_interval` only when operators need the knob. Consider an adapter-owned wake hint with periodic polling as fallback if latency or idle query cost justifies it.

**Alternatives and impact.** Applications can retain polling, tune the interval, budget pool connections, and store references instead of large results. Targeted changes can improve tail behavior without introducing a queue or weakening integrity validation.

**Compatibility and dependencies.** Most changes can stay internal. Query or indexing changes require migrations or regenerated SQLx metadata as applicable. Depends on E2 for stable eligibility semantics. Retain conformance checks when replacing snapshot-based eligibility tests.

## Differentiating opportunities

### D3. Composable external-effect recipes

**Category** Differentiating · **Current state** Partial · **Priority** P3 · **Complexity** Low

**Problem and use cases.** The supplied effect identities are strong primitives. Consumers implementing backups, webhooks, or payment-provider reconciliation still have to decide when to look up an existing effect, issue a conditional write, and record a provider reference. Provider key expiry can outlive a worker restart and be shorter than the application's recovery horizon. The standalone examples demonstrate cases in isolation.

**Evidence.** [Step effect identities](crates/resumable/src/engine/step_context.rs) · [Artifact reconciliation example](crates/resumable/examples/artifact_reconciliation.rs) · [Delivery example](crates/resumable/examples/message_delivery.rs) · [Value storage guide](crates/resumable/docs/value-storage.md)

**Proposed solution.** Extend the examples into copyable contracts for storing a receipt or independently verifiable effect reference, looking up before repeating, handling expired provider deduplication, and combining an external effect with a local SQL checkpoint. Show the crash after the external effect and before the local checkpoint, and show which provider guarantees each recipe assumes. Treat artifact references as application data with a lifetime at least as long as the required recovery window.

**API sketch.** Existing APIs suffice. For example, `ctx.step("upload", |step| reconcile_or_upload(step.idempotency_key(), artifact_ref.clone()))`, where the application owns the reconciliation function.

**Alternatives and impact.** Keep the existing standalone examples if users can readily translate them. Improved recipes would make Resumable easier to adopt where consumers already have effect-specific guarantees and need explicit, testable crash recovery.

**Compatibility and dependencies.** Documentation and examples only. Use the final E1 and E4 semantics in production-facing examples.

## Experimental features

### L1. Durable suspension and single correlated wake

**Category** Experimental · **Current state** Missing · **Priority** P4 · **Complexity** High

**Problem and use cases.** A vendor may return a job reference and deliver a webhook hours later. Keeping a future alive consumes a slot, and repeated lease takeovers or retry-budget use conflate waiting with failure. One-off `run_at` delays submission and cannot suspend an execution mid-operation.

**Evidence.** [Execution states](crates/resumable/src/record/execution.rs) · [Scheduled starts migration](crates/resumable-store-postgres-sqlx/migrations/0002_scheduled_starts.sql) · [Deferred waits](crates/resumable/docs/specification.md#37-deferred-candidates-for-future-versions)

**Proposed solution and API sketch.** Prototype one durable, named wait with a deadline and a deduplicated wake delivered by application code. For example, `ctx.wait_for("vendor-result", correlation, deadline).await?` and `engine.wake(id, correlation, event_id, payload).await?`. Prove that wake-before-park and park-before-wake converge, that duplicate wakes are harmless, and that cancel, timeout, and wake have a defined order. Release the lease and worker slot while waiting.

**Alternatives and impact.** Keep a callback inbox in the application database and enqueue a subsequent execution, or schedule a later follow-up. Those are viable today if each stage can have its own identity. A wait primitive is worthwhile only if several consumers need prior checkpoint context under the same ID.

**Compatibility and dependencies.** Adds Store transitions and probably persisted state. Depends on E1–E2, application ownership of correlation and authorization, and a clear retention policy. Prototype against the reference adapter before committing to a public API.

### L2. SQLite adapter for local deployments

**Category** Experimental · **Current state** Missing · **Priority** P4 · **Complexity** High

**Problem and use cases.** A desktop application, local CLI, or edge process needs restart durability without operating PostgreSQL. The reference store loses its state with the process. The portable trait allows an external adapter but implementing recovery correctly requires substantial backend work.

**Evidence.** [Reference store](crates/resumable/src/reference/mod.rs) · [Shared conformance suite](crates/resumable/src/conformance/mod.rs) · [SQLite adapter notes](crates/resumable-store-postgres-sqlx/docs/architecture.md#sqlite-and-additional-implementations)

**Proposed solution and API sketch.** If there is a concrete consumer, prototype a separate `SqliteStore::open(path, options)`. Validate compare-and-swap fencing, concurrent processes, lock timeout behavior, time authority, crash recovery, and safe schema upgrades. Define the supported connection and file durability settings using measured behavior.

**Alternatives and impact.** Continue with PostgreSQL, support a community adapter, or use the in-memory backend when restart durability is unnecessary. A successful prototype would test the portability contract in production and open local use cases.

**Compatibility and dependencies.** Separate package with its own migrations and maintenance burden. Depends on E1–E2 and expanded conformance. Transaction extensions can be considered after the core adapter is viable.

## Incomplete existing features

| Existing capability | Where it falls short | Generalization or action |
| --- | --- | --- |
| Step retry and operation resume | Static errors, two failure writes, policy checked before claim, and store versus process time used for different parts of scheduling. | One durable run disposition and store-side eligibility contract, followed by error classification. E1, E2, X1. |
| Long steps and progress | Manual heartbeats, concurrent revision conflicts, and checkpoint requests carrying the step-entry revision. | A lease protocol shared by progress, ordinary completion, and transaction extensions. E3, E4. |
| Cancellation | Store transitions handle common cases, while recovery through polling can be blocked by retry, step failure, or dispatch prerequisites. | Reconcile cancellation from persisted state under ownership rules. E2. |
| Result and error reporting | A generic operation error path hides scheduling disposition. Generated-ID starts can lose the caller's handle on error. | Typed submission identity and structured run outcomes. E1, X2, D1. |
| Operation registry and versioning | Exact keys work, but JSON erasure, source glue, and lack of drain visibility make larger deployments do extra work. | Add typed adapters and version filters, with upgrade fixtures. D1, X3, X4. |
| Bounds and repeatability | Exponential validation accepts durations that can overflow the floating-point conversion. Persisted retry durations use milliseconds, while the public policy accepts finer precision. | Normalize or reject durations at the boundary and exercise extreme settings and store-clock deadlines during E1. [Retry validation and delay](crates/resumable/src/engine/retry.rs) |
| Effect ordering and failed-step handling | APIs are callable from overlapping futures even though sequential use is the contract. Returning success after catching a failed step cannot normally complete the execution with an incomplete step. | Document and test sequential boundaries as part of E1–E3. Encode accepted domain outcomes as successful typed step results when the outcome itself is a completed checkpoint. Keep parallel branches and general reset outside the initial repair. [Execution completion check](crates/resumable-store-postgres-sqlx/src/postgres/lifecycle.rs) · [Core architecture](crates/resumable/docs/architecture.md#ownership-and-step-ordering) |

## Unsupported use cases and consumer fit

| Consumer and goal | Current support | Boilerplate, workaround, or library change |
| --- | --- | --- |
| CLI or internal Rust application resumes a short backup or import against an existing PostgreSQL database | Supported with known IDs, stable steps, and idempotent effects. | Direct engine use suffices. Persist the execution ID before starting, retain artifact references, and provide a compatible decoder. A process-local demo can use the reference store. |
| Web application submits a delayed job in the same transaction as a domain write | Supported with PostgreSQL. | Prepare the input envelope and `NewExecution` manually, then use `create_execution_in_transaction`. Wire source and runner adapters. D1 reduces glue while retaining the transaction. |
| Messaging or artifact service retries transient errors and tolerates worker exit at any point | Interrupted-step restart is supported. Reliable default deferred retry is incomplete. | Use `Resume` and a custom supervisor or implement application retry inside a step. Correct behavior through the stock path requires E1–E2. Provider deduplication and reconciliation stay in the application. |
| Multi-worker service runs minutes-long work, reports progress, commits domain state, and drains on termination | Partial. Short steps and transaction checkpoints work on their own. | Schedule manual heartbeats, avoid the stale checkpoint sequence, and use custom shutdown orchestration. Safe composition through the stock worker and adapter requires E3–E4 and X2. |
| Production team investigates failed work, deletes expired histories, and rolls adjacent versions | Partial. Exact operation version dispatch and per-ID purge exist. | Build SQL inspection and retention tools and coordinate schema downtime. An operator can maintain a known-ID index, but supported retirement of permanently failed history and a tested upgrade contract need X3–X4. |
| Multi-tenant service guarantees separation, per-tenant limits, and fair resource use | Supported as an application integration. | Enforce authorization and quotas in application admission and discovery, with isolated pools or stores as needed. Global capacity per worker is the only bundled concurrency control. Confirm contention or starvation after E2 before exploring shared quotas. A library fork is unnecessary if admission and routing remain application-owned. |
| Callback-based operation pauses for hours and later continues with its existing checkpoints | No durable suspension primitive. | Use an application inbox with a second execution or persist a small state machine in the application. Suspending and waking the same execution safely requires L1 or a different workflow runtime. |
| Local-first product needs durable file-backed execution with no PostgreSQL service | No first-party implementation. | Build or adopt a conforming store, or use L2 if the prototype succeeds. A backend can be developed outside the core. |
| Workflow platform needs distributed branches, human approvals, and automatic cross-service compensation | Outside the present execution model. | Use explicit application orchestration, separate executions, or an existing platform. Extending this core to provide those guarantees would change its scope. O1–O2. |

## Features that should deliberately remain out of scope

### O1. General orchestration and queue platform

**Category** Out of scope · **Current state** Missing as a platform · **Priority** P4, excluded from the roadmap · **Complexity** Very high

**Problem and use cases.** General DAGs, child execution trees, recurring workflow scheduling, pub/sub, workflow definitions, and an operator web UI serve teams that need a shared orchestration product. They would require branch dependency and cancellation semantics, new discovery and isolation models, and service interfaces beyond the current library. **Evidence.** [v0.1 non-goals](crates/resumable/docs/specification.md#4-non-goals-for-v01) · [Sequential ownership contract](crates/resumable/docs/specification.md#12-leases-and-concurrent-ownership)

**Proposed boundary, API and alternatives.** Keep no new platform API in the core. Use existing `NewExecution::with_run_at` for one-off starts, an external scheduler for recurrence, and applications or existing orchestrators for dependencies and UI. A bounded wait experiment does not require introducing a general event bus.

**Impact, compatibility and dependencies.** Exclusion preserves simple ownership, maintenance costs, and the current Store model. Inclusion would require new transition, transport, authorization, schema, and compatibility contracts beyond the proposed roadmap.

### O2. Transparent replay, universal exactly-once effects and automatic compensation

**Category** Out of scope · **Current state** Missing by design · **Priority** P4, excluded from the roadmap · **Complexity** Very high, with universal guarantees infeasible for arbitrary providers

**Problem and use cases.** Applications may want a process to continue an arbitrary Rust future, a remote charge to execute exactly once after a lost response, or a sequence of external actions to undo itself automatically. These guarantees depend on deterministic replay or provider-supported effect semantics. A local checkpoint cannot establish the outcome of an arbitrary remote action.

**Evidence.** [Explicit durability and effects](crates/resumable/docs/specification.md#32-explicit-durability) · [Crash windows](crates/resumable/docs/specification.md#21-crash-windows-and-guarantees) · [Store transaction boundary](crates/resumable/src/store.rs)

**Proposed boundary, API and alternatives.** Preserve explicit `ctx.step(...)` boundaries and use stable effect keys, provider lookup, conditional writes, caller-owned same-database transactions, or application-written compensating steps. The API remains the existing checkpoint and effect interface, strengthened by E4 and demonstrated in D3.

**Impact, compatibility and dependencies.** This keeps application effects auditable and the guarantees defensible. Specialized alternatives require provider participation, supported transaction scope, or a different execution model. No compatibility changes are needed to maintain the boundary.

## Prioritized implementation roadmap

| ID | Recommendation | Category | Priority | Complexity | Dependencies |
| --- | --- | --- | --- | --- | --- |
| E1 | Durable retry disposition | Essential | P0 | High | Joint eligibility design with E2 |
| E2 | Consistent discovery, claims and terminalization | Essential | P0 | High | Joint outcome design with E1 |
| E3 | Safe lease renewal for long work | Essential | P1 | High | E2, coordination with E4 and X2 |
| E4 | Transactional checkpoint composition | Essential | P1 | Medium | E3 for concurrent renewal |
| X1 | Per-error retry decisions and structured outcomes | Expected | P2 | Medium | E1, E2 |
| X2 | Bounded worker lifecycle and payload-safe reporting | Expected | P1 | Medium | E1, E2 for complete outcome reporting |
| X3 | Operator inspection and bounded retention | Expected | P2 | High | E2 for retirement |
| X4 | Version retirement and schema upgrade contract | Expected | P2 | Medium | E1 and E2 schema changes, X3 for drain visibility |
| D1 | Typed submission and registry-backed dispatch | Desirable | P2 | Medium | E1 and X2 for integrated runner |
| D2 | Measured PostgreSQL overhead reduction | Desirable | P3 | Medium | E2, measured bottleneck |
| D3 | Composable external-effect recipes | Differentiating | P3 | Low | E1 and E4 examples |
| L1 | Durable suspension and single correlated wake | Experimental | P4 | High | E1, E2, demonstrated same-ID demand |
| L2 | SQLite adapter for local deployments | Experimental | P4 | High | E1, E2, expanded conformance, adopter |
| O1 | General orchestration and queue platform | Out of scope | P4, excluded | Very high | Separate execution and service architecture |
| O2 | Transparent replay, universal exactly-once effects and automatic compensation | Out of scope | P4, excluded | Very high | Different execution model or provider guarantees |

### Immediate priorities

- Ship E1 and E2 as one recovery change. The release gate is a default-config PostgreSQL worker that retries a transient error at the recorded time, keeps healthy jobs moving past old failures, and finalizes orphaned cancellation or permanent failure without rerunning a completed effect. Include crash-between-write and same-request replay cases.
- Remove raw error text from default worker reporting and make discovery interruptible. Keep those X2 fixes independently shippable while the new outcome protocol is developed.
- Establish the upgrade behavior for stored 0.1.2 records before changing the state model. Carry the first compatibility fixture from X4 into this work.

### Short-term improvements

- Complete E3–E4 and X2. Verify a long quiet step with two workers, renewal during ordinary progress, progress followed by a transaction checkpoint, ambiguous commit reconciliation, and shutdown under a stalled discovery call. Exercise these combinations on PostgreSQL as well as with store fakes where relevant.
- Add X1 and the D1 submission and dispatch helpers in that order. Demonstrate one delayed typed submission and one typed operation handling a retryable provider response.
- Update the Rustdoc walkthrough and HTML support-status descriptions alongside the shipped behavior.

### Medium-term development

- Deliver X3's read-only queries first. Use them to support X4's handler retirement and add guarded retirement and bounded purge after the state transitions are settled.
- Publish D3 using the finalized retry and transaction behavior. Benchmark D2 against representative histories and backlogs before changing polling or query design. A measured contention problem can justify moving the specific remedy earlier.

### Long-term opportunities

- Evaluate L1 and L2 independently. Each needs a named consumer, a reason existing application composition is inadequate, and tests that establish recovery semantics.
- Keep O1 and O2 excluded. Treat larger platform requirements as integration choices.

## Open questions requiring maintainer decisions

1. Should expiry, panic, or shutdown consume the same attempt budget as a reported operation error? Decide this before persistently encoding the new retry disposition.
2. Which interrupted or failed records can safely be recovered from existing histories? Should older ambiguous records require explicit operator action when the available data cannot distinguish retry intent from a deliberate failure?
3. Can a run maintain the sequential step contract through a dedicated owned handle, and should pure renewal use a separate ownership field or ordered access to the current revision?
4. Should cancellation remove the lease as soon as the current operation yields, or wait for explicit acknowledgement when a domain transaction may be committing? Which states can be finalized without a compatible handler?
5. What level of backwards compatibility should apply to custom Store implementations, persisted records, operation codecs, and SQL migrations during the 0.1 series?
6. How long must mutation receipts and execution identity remain queryable after completion or retirement? Where should audit and external-provider deduplication requirements be enforced?
7. Which deployment constraints justify schema-version overlap, and which justify SQLite or a durable wait? Exact handler versioning and stop-and-upgrade deployment can remain the documented default until real requirements establish the need.

## Verification details

- The workspace build with all targets, formatting, and Clippy passed in offline mode where applicable. The workspace tests passed with 66 unit tests, three doctests, and one PostgreSQL integration test that groups conformance and recovery scenarios. The three core examples, standalone worker, and PostgreSQL process-restart walkthrough all ran successfully.
- A temporary public-API probe using `MemoryStore` reproduced E1 with a two-attempt step and the default engine. The first error left a retryable step alongside an execution in `Failed`. Resume returned `RunError::Failed` without dispatching the operation. The probe used zero backoff to isolate execution-policy behavior from clock behavior.
- Two PostgreSQL public-API probes reproduced E2 and E4. Three consecutive polls with a limit of one returned the same failed ID, which the default engine rejected each time while the younger execution stayed pending. A successful `progress_checked` was followed by a transaction checkpoint that returned a lease conflict, after which the probe rolled back the transaction and ordinary step completion succeeded. These probes exercised the adapter and engine directly. The full worker starvation sequence and E2's other race cases remain source findings. Lease-renewal races and mixed-version compatibility were not reproduced. No throughput figures are asserted. [PostgreSQL test entry point](crates/resumable-store-postgres-sqlx/tests/postgres/mod.rs) · [Restart walkthrough](crates/resumable/docs/postgres-worker.md)
