use super::*;

#[derive(Clone)]
pub struct StepContext {
    pub(super) execution_id: ExecutionId,
    pub(super) step_id: StepId,
    pub(super) name: StepName,
    pub(super) attempt: u32,
    pub(super) fencing_token: u64,
    pub(super) lease_provider: Arc<dyn Fn() -> crate::LeaseGuard + Send + Sync>,
    pub(super) cancellation_check: CancellationCheck,
    pub(super) progress_update: ProgressUpdateFn,
}

pub(super) type StoreFuture<T> = Pin<Box<dyn Future<Output = Result<T, StoreError>> + Send>>;
pub(super) type CancellationCheck = Arc<dyn Fn() -> StoreFuture<bool> + Send + Sync>;
pub(super) type ProgressUpdateFn = Arc<dyn Fn(crate::Progress) -> StoreFuture<()> + Send + Sync>;

impl StepContext {
    /// Store progress on a best-effort basis. Failures are observable in tracing and do not
    /// affect step completion.
    pub async fn progress(&self, progress: crate::Progress) {
        if (self.progress_update)(progress).await.is_err() {
            tracing::warn!(execution_id = %self.execution_id, step = %self.name,
                attempt = self.attempt, "progress update failed");
        }
    }

    /// Store progress and return any ownership or backend error to the caller.
    pub async fn progress_checked(&self, progress: crate::Progress) -> Result<(), StoreError> {
        (self.progress_update)(progress).await
    }
    /// Read the durable cancellation request. Cancellation is cooperative; the body must check it.
    pub async fn is_cancelled(&self) -> Result<bool, StoreError> {
        (self.cancellation_check)().await
    }
    pub fn execution_id(&self) -> ExecutionId {
        self.execution_id
    }
    pub fn step_id(&self) -> StepId {
        self.step_id
    }
    pub fn step_name(&self) -> &StepName {
        &self.name
    }
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Build the request an adapter can commit with domain writes in its own transaction.
    /// Encode the same operation result with the engine's configured codec before calling this.
    pub fn completion_request(&self, result: crate::StoredValue) -> crate::CompleteStep {
        crate::CompleteStep {
            lease: (self.lease_provider)(),
            name: self.name.clone(),
            expected_attempt: self.attempt,
            mutation_id: crate::MutationId::new(),
            result: Some(result),
            at: super::now(),
        }
    }
    /// Monotonically increasing execution ownership generation. External systems that support
    /// conditional writes can reject requests carrying an older generation.
    pub fn fencing_token(&self) -> u64 {
        self.fencing_token
    }
    pub fn idempotency_key(&self) -> IdempotencyKey {
        IdempotencyKey::for_step(self.execution_id, &self.name)
    }
    pub fn attempt_key(&self) -> IdempotencyKey {
        IdempotencyKey::for_attempt(self.execution_id, &self.name, self.attempt)
    }
}
