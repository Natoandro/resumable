use super::*;

impl<S: Store, C: ValueCodec> Engine<S, C> {
    /// Resume by ID. The operation closure receives the input loaded from durable storage.
    pub async fn resume<I, O, E, F, Fut>(
        &self,
        id: ExecutionId,
        operation: &OperationKey,
        body: F,
    ) -> Result<O, RunError>
    where
        I: Serialize + DeserializeOwned,
        O: Serialize + DeserializeOwned,
        E: std::fmt::Display,
        F: FnOnce(I) -> Fut,
        Fut: Future<Output = Result<O, E>>,
    {
        let record = self
            .store
            .load_execution(id)
            .await?
            .ok_or(RunError::NotFound)?;
        check_operation(&record.operation, operation)?;
        if record.status == ExecutionStatus::Failed
            && self.config.failed_execution_policy == FailedExecutionPolicy::Reject
        {
            return Err(RunError::Failed);
        }
        self.run_loaded(id, move |input, _context| body(input))
            .await
    }

    pub async fn resume_with_context<I, O, E, F, Fut>(
        &self,
        id: ExecutionId,
        operation: &OperationKey,
        body: F,
    ) -> Result<O, RunError>
    where
        I: Serialize + DeserializeOwned,
        O: Serialize + DeserializeOwned,
        E: std::fmt::Display,
        F: FnOnce(I, ExecutionContext<S, C>) -> Fut,
        Fut: Future<Output = Result<O, E>>,
    {
        let record = self
            .store
            .load_execution(id)
            .await?
            .ok_or(RunError::NotFound)?;
        check_operation(&record.operation, operation)?;
        if record.status == ExecutionStatus::Failed
            && self.config.failed_execution_policy == FailedExecutionPolicy::Reject
        {
            return Err(RunError::Failed);
        }
        self.run_loaded(id, body).await
    }
}
