use super::*;

mod executions;
mod lifecycle;
mod steps;

impl<C: Clock> Store for MemoryStore<C> {
    async fn create_execution(&self, new: NewExecution) -> Result<ExecutionRecord, StoreError> {
        self.create_execution_inner(new).await
    }

    async fn load_execution(&self, id: ExecutionId) -> Result<Option<ExecutionRecord>, StoreError> {
        self.load_execution_inner(id).await
    }

    async fn acquire_execution(
        &self,
        id: ExecutionId,
        worker: WorkerId,
        claim_id: MutationId,
        lease_duration_ms: u64,
    ) -> Result<ClaimOutcome, StoreError> {
        self.acquire_execution_inner(id, worker, claim_id, lease_duration_ms)
            .await
    }

    async fn load_step(
        &self,
        execution: ExecutionId,
        name: &StepName,
    ) -> Result<Option<StepRecord>, StoreError> {
        self.load_step_inner(execution, name).await
    }

    async fn load_progress(
        &self,
        execution: ExecutionId,
        name: &StepName,
    ) -> Result<Option<Progress>, StoreError> {
        let state = self.lock()?;
        Ok(state.progress.get(&(execution, name.clone())).cloned())
    }

    async fn begin_step(&self, request: BeginStep) -> Result<StepRecord, StoreError> {
        self.begin_step_inner(request).await
    }

    async fn complete_step(&self, request: CompleteStep) -> Result<StepRecord, StoreError> {
        self.complete_step_inner(request).await
    }

    async fn fail_step(&self, request: FailStep) -> Result<StepRecord, StoreError> {
        self.fail_step_inner(request).await
    }

    async fn complete_execution(
        &self,
        request: CompleteExecution,
    ) -> Result<ExecutionRecord, StoreError> {
        self.complete_execution_inner(request).await
    }

    async fn fail_execution(&self, request: FailExecution) -> Result<ExecutionRecord, StoreError> {
        self.fail_execution_inner(request).await
    }

    async fn request_cancellation(
        &self,
        request: RequestCancellation,
    ) -> Result<ExecutionRecord, StoreError> {
        self.request_cancellation_inner(request).await
    }

    async fn heartbeat(&self, request: ProgressUpdate) -> Result<ExecutionRecord, StoreError> {
        self.heartbeat_inner(request).await
    }

    async fn purge_execution(&self, id: ExecutionId) -> Result<bool, StoreError> {
        self.purge_execution_inner(id).await
    }
}
