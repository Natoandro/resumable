use super::*;
use std::sync::atomic::AtomicBool;

#[derive(Clone, Copy)]
pub(super) enum Interruption {
    Takeover,
    Expiry,
    ResponseLoss,
    FailAfterLoad,
}

pub(super) struct InterruptedBeginStore {
    pub(super) store: MemoryStore,
    interruption: Interruption,
    once: AtomicBool,
}

impl InterruptedBeginStore {
    pub(super) fn new(store: MemoryStore, interruption: Interruption) -> Self {
        Self {
            store,
            interruption,
            once: AtomicBool::new(true),
        }
    }
}

impl Store for InterruptedBeginStore {
    async fn create_execution(&self, request: NewExecution) -> Result<ExecutionRecord, StoreError> {
        self.store.create_execution(request).await
    }

    async fn load_execution(&self, id: ExecutionId) -> Result<Option<ExecutionRecord>, StoreError> {
        let record = self.store.load_execution(id).await?;
        if matches!(self.interruption, Interruption::FailAfterLoad)
            && self.once.swap(false, Ordering::SeqCst)
        {
            let guard = claim(&self.store, id, 30_000);
            self.store
                .fail_execution(FailExecution {
                    lease: guard,
                    mutation_id: MutationId::new(),
                    failure: FailureRecord {
                        class: FailureClass::Permanent,
                        diagnostic: None,
                        occurred_at: Timestamp(0),
                    },
                })
                .await?;
        }
        Ok(record)
    }

    async fn acquire_execution(
        &self,
        id: ExecutionId,
        worker: WorkerId,
        claim_id: MutationId,
        duration: u64,
    ) -> Result<ClaimOutcome, StoreError> {
        self.store
            .acquire_execution(id, worker, claim_id, duration)
            .await
    }

    async fn acquire_execution_with_policy(
        &self,
        id: ExecutionId,
        worker: WorkerId,
        claim_id: MutationId,
        duration: u64,
        policy: FailedExecutionPolicy,
    ) -> Result<ClaimOutcome, StoreError> {
        self.store
            .acquire_execution_with_policy(id, worker, claim_id, duration, policy)
            .await
    }

    async fn load_step(
        &self,
        id: ExecutionId,
        name: &StepName,
    ) -> Result<Option<StepRecord>, StoreError> {
        self.store.load_step(id, name).await
    }

    async fn load_progress(
        &self,
        id: ExecutionId,
        name: &StepName,
    ) -> Result<Option<Progress>, StoreError> {
        self.store.load_progress(id, name).await
    }

    async fn begin_step(&self, request: BeginStep) -> Result<StepRecord, StoreError> {
        let id = request.lease.execution_id;
        let result = self.store.begin_step(request).await?;
        if self.once.swap(false, Ordering::SeqCst) {
            match self.interruption {
                Interruption::FailAfterLoad => unreachable!(),
                Interruption::Takeover => {
                    self.store.clock().advance(30_001);
                    let replacement = claim(&self.store, id, 30_000);
                    self.store
                        .begin_step(BeginStep {
                            lease: replacement,
                            name: result.name,
                            retry_policy: result.retry_policy.unwrap(),
                            mutation_id: MutationId::new(),
                            at: Timestamp(30_001),
                        })
                        .await?;
                }
                Interruption::Expiry => self.store.clock().advance(30_001),
                Interruption::ResponseLoss => {}
            }
            return Err(StoreError::Unavailable("begin response was lost".into()));
        }
        Ok(result)
    }

    async fn complete_step(&self, request: CompleteStep) -> Result<StepRecord, StoreError> {
        self.store.complete_step(request).await
    }

    async fn fail_step(&self, request: FailStep) -> Result<StepRecord, StoreError> {
        self.store.fail_step(request).await
    }

    async fn complete_execution(
        &self,
        request: CompleteExecution,
    ) -> Result<ExecutionRecord, StoreError> {
        self.store.complete_execution(request).await
    }

    async fn fail_execution(&self, request: FailExecution) -> Result<ExecutionRecord, StoreError> {
        self.store.fail_execution(request).await
    }

    async fn request_cancellation(
        &self,
        request: RequestCancellation,
    ) -> Result<ExecutionRecord, StoreError> {
        self.store.request_cancellation(request).await
    }

    async fn heartbeat(&self, request: ProgressUpdate) -> Result<ExecutionRecord, StoreError> {
        self.store.heartbeat(request).await
    }

    async fn purge_execution(&self, id: ExecutionId) -> Result<bool, StoreError> {
        self.store.purge_execution(id).await
    }
}
