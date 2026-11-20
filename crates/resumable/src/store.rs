//! Portable atomic persistence contract. Adapters own transaction and clock implementations.

use crate::{
    ExecutionId, ExecutionRecord, FailedExecutionPolicy, StepName, StepRecord, StoreError, WorkerId,
};
use std::future::Future;

mod requests;
pub use requests::{
    BeginStep, ClaimOutcome, CompleteExecution, CompleteStep, ExecutionClaim, FailExecution,
    FailStep, LeaseGuard, MutationId, NewExecution, Progress, ProgressUpdate, RequestCancellation,
};

/// Atomic durable state operations required by the resumable core.
///
/// Implementations may use any backend. Each method must commit the documented state change as
/// one atomic operation and validate persisted records before returning them, reporting malformed
/// data as `StoreError::Corrupt`. Conflicts (including stale revisions, tokens, and expired leases)
/// use `StoreError::Conflict`; backend failures use `StoreError::Unavailable`. For every mutation
/// ID, an adapter must first resolve a previously committed identical request and return its stored
/// outcome even if its lease has since expired. Reusing that ID with different request data
/// conflicts. This ordering makes a lost completion response safely recoverable.
/// Application dispatch, shared domain transactions, and runnable discovery remain outside this
/// portable contract.
pub trait Store: Send + Sync + 'static {
    /// Atomically create an execution. A retry is compatible only when operation key and exact
    /// optional input envelope and `run_at` match; metadata and `created_at` are ignored after the first create.
    /// Compatible identity returns its original record; incompatible reuse returns `Conflict`. It
    /// never overwrites an existing execution. The record is returned only after durable creation.
    fn create_execution(
        &self,
        new: NewExecution,
    ) -> impl Future<Output = Result<ExecutionRecord, StoreError>> + Send;

    /// Load one execution snapshot, including input, result, lease, metadata, and failure details.
    /// A missing identity is `Ok(None)`; invalid persisted state is `Corrupt`.
    fn load_execution(
        &self,
        id: ExecutionId,
    ) -> impl Future<Output = Result<Option<ExecutionRecord>, StoreError>> + Send;

    /// Atomically acquire or take over an expired lease. A live lease or a future `run_at`
    /// returns `Busy`;
    /// terminal records are returned as `Terminal`; absent identities return `NotFound`. A
    /// successful claim increments fencing generation and revision before returning the record and
    /// ownership proof. Claiming a failed execution explicitly resumes it to Pending before moving
    /// it to Running.
    /// Each invocation uses a fresh claim ID. Retrying an ambiguous claim uses the same ID, worker,
    /// and duration; only that identical request may recover its current live claim. A separate
    /// claim ID must return Busy even for the same worker. Generation must increase across failures
    /// and lease removal, not only expired-lease takeover.
    fn acquire_execution(
        &self,
        id: ExecutionId,
        worker: WorkerId,
        claim_id: MutationId,
        lease_duration_ms: u64,
    ) -> impl Future<Output = Result<ClaimOutcome, StoreError>> + Send;

    /// Atomically acquire an execution while enforcing the failed execution policy. With
    /// `Reject`, a failed record must be returned as `Terminal` without changing it. This check
    /// must serialize with the claim transition.
    fn acquire_execution_with_policy(
        &self,
        id: ExecutionId,
        worker: WorkerId,
        claim_id: MutationId,
        lease_duration_ms: u64,
        policy: FailedExecutionPolicy,
    ) -> impl Future<Output = Result<ClaimOutcome, StoreError>> + Send;

    /// Load the unique step identified by `(execution, name)`, including result, failure, attempt,
    /// retry eligibility, and timestamps. Missing steps return `Ok(None)`.
    fn load_step(
        &self,
        execution: ExecutionId,
        name: &StepName,
    ) -> impl Future<Output = Result<Option<StepRecord>, StoreError>> + Send;

    /// Load the latest observational progress for one step, if any.
    fn load_progress(
        &self,
        execution: ExecutionId,
        name: &StepName,
    ) -> impl Future<Output = Result<Option<Progress>, StoreError>> + Send;

    /// Atomically create or start an eligible step attempt under a live execution lease. The
    /// initial retry policy is persisted with the step on its first begin and cannot later change.
    /// Implementations must persist it atomically with the attempt and validate it on replay/retry.
    /// Repeating a request with the same mutation ID after an ambiguous response must not increment
    /// attempts twice, even if the original response was lost and the lease has since expired. An
    /// attempt can begin only when persisted `next_attempt_at` is due according to the adapter's
    /// authoritative clock. A failed step can restart only when its persisted failure is retryable,
    /// its persisted policy permits retries, and its attempt limit is not exhausted. These checks
    /// must be atomic with the attempt increment. It increments execution revision and step attempt,
    /// clears prior failure/result eligibility, and returns the committed step. Completed or
    /// cancelled steps conflict. It must also conflict after an execution cancellation request has
    /// committed, so a new step cannot start after cancellation.
    /// A Running attempt can restart only under a later fencing generation. Its original generation
    /// must be persisted by the adapter; another mutation under the same generation must conflict.
    fn begin_step(
        &self,
        request: BeginStep,
    ) -> impl Future<Output = Result<StepRecord, StoreError>> + Send;

    /// Atomically persist step result and completion before reporting success. It requires the named
    /// current attempt to be running. `mutation_id` makes retry after an ambiguous write safe: the
    /// same ID and request returns the already committed result even after lease expiry. It
    /// increments execution revision and conflicts for stale leases, attempts, or a different
    /// completion mutation.
    fn complete_step(
        &self,
        request: CompleteStep,
    ) -> impl Future<Output = Result<StepRecord, StoreError>> + Send;

    /// Atomically persist failure, attempt/retry decision, and next eligibility. The requested
    /// retry policy must equal the policy persisted at first begin. Retry class, attempt number, and
    /// `retry_at` are committed together. Repeating the same mutation is safe; a different or stale
    /// mutation conflicts. A replay with the same ID returns the committed failure even after lease
    /// expiry. It increments execution revision and returns the committed step.
    fn fail_step(
        &self,
        request: FailStep,
    ) -> impl Future<Output = Result<StepRecord, StoreError>> + Send;

    /// Atomically complete or cancel an execution, optionally persisting its result. All steps must
    /// be terminal/completed according to operation policy. The mutation ID allows safe replay when
    /// the commit succeeds but its response is lost, including after lease expiry. The record and
    /// result become visible together; it increments revision and clears the lease.
    fn complete_execution(
        &self,
        request: CompleteExecution,
    ) -> impl Future<Output = Result<ExecutionRecord, StoreError>> + Send;

    /// Persist execution failure under its current lease and expected revision. The mutation ID makes
    /// ambiguous retries safe, including after lease expiry; failure and terminal status commit
    /// together, revision increments, and the lease clears.
    fn fail_execution(
        &self,
        request: FailExecution,
    ) -> impl Future<Output = Result<ExecutionRecord, StoreError>> + Send;

    /// Durably request cancellation. Idempotent; completed/cancelled executions remain unchanged.
    /// A pending execution becomes cancelled immediately; a running execution records the request
    /// for cooperative observation. This does not discard completed step history. Successful
    /// completion and cancellation serialize as atomic transitions, so whichever commits first
    /// determines the terminal status.
    fn request_cancellation(
        &self,
        request: RequestCancellation,
    ) -> impl Future<Output = Result<ExecutionRecord, StoreError>> + Send;

    /// Extend a currently valid lease, optionally storing lossy progress. A stale or expired owner
    /// conflicts. Lease expiry checks use the adapter's authoritative clock. Progress may be dropped
    /// or fail without changing completion state; successful heartbeat returns the new revision and
    /// expiry needed for subsequent guarded mutations.
    fn heartbeat(
        &self,
        request: ProgressUpdate,
    ) -> impl Future<Output = Result<ExecutionRecord, StoreError>> + Send;

    /// Delete a completed or cancelled execution, its steps, progress, and mutation receipts
    /// containing that execution's requests or outcomes atomically. Pending,
    /// running, and failed executions must be rejected because they may still be resumed. Returns
    /// false when absent. Calls must serialize with claims and cancellation transitions.
    fn purge_execution(
        &self,
        id: ExecutionId,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;
}
