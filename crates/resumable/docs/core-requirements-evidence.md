# Core requirement evidence

This map ties resumable v0.1 requirements to the portable implementation and the reference-store
evidence. The core's durability guarantees are conditional on a Store implementation honoring the
atomicity, lease, revision, and replay contract. `MemoryStore` is an executable reference, not a
production persistence guarantee. Adapters must run `conformance::run_store_scenarios` and add
backend-specific interruption and transaction tests.

## Specification sections

| Sections | Contract                                                     | Implementation and evidence                                                                                                            |
| -------- | ------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------- |
| 1–4      | Scope, goals, non-goals, terminology                         | `specification.md`; package boundary in `Cargo.toml` and `src/lib.rs`                                                                  |
| 5–10     | Execution identity, lifecycle, steps, values, metadata       | `identity.rs`, `record/`, `value.rs`, `metadata.rs`; reference tests in `reference/tests/`                                             |
| 11–16    | Retry, leases, fencing, cancellation, progress, heartbeat    | `engine/`, `store.rs`, `reference/store/`; `reference/tests/engine/`, `ownership/`, `recovery.rs`, and `conformance/`                  |
| 17–21    | Input, metadata, Store contract, recovery, retention hooks   | `store.rs`, `store/requests.rs`, `conformance/`; reference and adapter conformance scenarios                                           |
| 22–27    | Lifecycle, time, tracing, retention, versions, security      | `engine/`, `identity.rs`, `reference/store/lifecycle.rs`; tests listed below; architecture and security sections in `specification.md` |
| 28–33    | Crate layout, API, examples, schema                          | Three runnable examples under `examples/`; public README and specification                                                             |
| 34       | Core invariants                                              | Invariant table below, with executable scenarios and API contracts                                                                     |
| 35       | Failure-boundary tests                                       | `reference/tests/engine/`, `ownership/`, `recovery.rs`, and artifact interruption example                                              |
| 36–38    | Implementation phases, deferred features, guarantee boundary | `specification.md`; release gates in `docs/core-release-checks.md`                                                                     |

## Invariant evidence (specification section 34)

| #   | Invariant                                                  | Evidence                                                                                            |
| --- | ---------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| 1   | One execution per identity                                 | `reference/store/executions.rs`; compatible and incompatible create scenarios in `conformance/`     |
| 2   | One step per execution/name                                | `reference/store/steps.rs`; repeated begin scenarios in `conformance/`                              |
| 3   | Completion remains durable                                 | `record/step.rs`; `reference/tests/basic.rs` terminal step transition scenario                      |
| 4   | Retry increments attempts but retains step identity        | `record/step.rs`; state sequence in `reference/tests/basic.rs`                                      |
| 5   | Stable step key across attempts                            | `identity.rs` tests and retry scenario in `conformance/`                                            |
| 6   | Completion is persisted before reuse                       | `engine/`; `persisted_completion_is_reused_after_lost_write_response`                               |
| 7   | Stale lease owners cannot mutate                           | `reference/store.rs` lease checks; `reference/tests/ownership/` and conformance fencing scenario    |
| 8   | Cancellation is durable                                    | `reference/store/lifecycle.rs`; cancellation tests in `reference/tests/engine/`                     |
| 9   | Retry schedule and initial policy are durable              | `record/step.rs`; retry resume and interrupted-first-attempt policy tests                           |
| 10  | Progress failure cannot invalidate completion              | `engine/`; `best_effort_progress_failure_does_not_prevent_completion`                               |
| 11  | Conflicts cannot overwrite newer state                     | Store revision checks; `conformance/transactions.rs` rejects stale revision under current ownership |
| 12  | Missing completion does not prove an effect did not happen | `artifact_reconciliation.rs` interrupts after object write and verifies reuse on resume             |

## Failure-boundary evidence (specification section 35)

| Boundary                                           | Evidence                                                                                       |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Before step body invocation                        | `step_body_runs_only_after_begin_is_persisted` with an injected begin failure                  |
| During the step body                               | `interrupted_body_can_resume_with_the_same_step_identity`                                      |
| After external effect, before completion           | `artifact_reconciliation` fake object store and process-loss scenario                          |
| After completion is committed but response is lost | `persisted_completion_is_reused_after_lost_write_response` and store mutation replay scenarios |
| During retry scheduling                            | `retry_deadline_and_policy_survive_resume` and injected fail-step recovery scenarios           |
| During lease renewal                               | `failed_heartbeat_prevents_completion_and_leaves_work_recoverable`                             |
| After cancellation request                         | cancellation and finish-race scenarios in `reference/tests/engine/`                            |
| Concurrent resume                                  | barrier-synchronized claim scenario in `reference/tests/ownership/`                            |

The reference store's clock and fault injector make these scenarios deterministic. They model the
core's contract; they do not substitute for validating a concrete database adapter.

## Ownership regression evidence

`reference/tests/p1/` covers overlapping invocations using one Engine or two Engines with the same
worker identity; claim response loss; begin response loss followed by takeover or expiry; increasing
external fencing tokens after failure/resume; and cancellation finalization after worker loss with
incomplete or permanently failed steps. These cases reproduce the ownership regressions found
during the original correctness review and now covered by the reference suite.

The shared conformance suite checks same-worker exclusion for distinct claim IDs, identical claim
and begin replay, changed-duration claim conflict, live-attempt restart rejection, unchanged state
after rejected begin, and increasing fencing generations across failure/resume. It also verifies
completion mutation replay and changed-request rejection, repeated cancellation, and stale-revision
rejection with otherwise current ownership. Engine interruption and fault-injection scenarios remain
reference-specific.
