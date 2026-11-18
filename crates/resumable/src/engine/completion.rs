use super::*;

impl<S: Store, C> ExecutionContext<S, C> {
    pub(super) async fn finish_execution(
        &self,
        result: Option<StoredValue>,
    ) -> Result<ExecutionRecord, StoreError> {
        self.refresh_revision().await?;
        let request = CompleteExecution {
            lease: self.lease(),
            mutation_id: MutationId::new(),
            result,
            at: now(),
        };
        match self.store.complete_execution(request.clone()).await {
            Err(StoreError::Unavailable(_)) => self.store.complete_execution(request).await,
            Err(StoreError::Conflict(error)) => {
                // Cancellation may have committed after the revision snapshot. Finalize it only
                // after confirming that this context still owns the running execution.
                if !self.is_cancelled().await? {
                    return Err(StoreError::Conflict(error));
                }
                self.store
                    .complete_execution(CompleteExecution {
                        lease: self.lease(),
                        mutation_id: MutationId::new(),
                        ..request
                    })
                    .await
            }
            outcome => outcome,
        }
    }
}
