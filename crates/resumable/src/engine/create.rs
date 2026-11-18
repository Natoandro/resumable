use super::*;

impl<S: Store, C: ValueCodec> Engine<S, C> {
    /// Create with a generated ID, then execute using the persisted input.
    pub async fn start<I, O, E, F, Fut>(
        &self,
        operation: OperationKey,
        input: I,
        body: F,
    ) -> Result<(ExecutionId, O), RunError>
    where
        I: Serialize + DeserializeOwned,
        O: Serialize + DeserializeOwned,
        E: std::fmt::Display,
        F: FnOnce(I) -> Fut,
        Fut: Future<Output = Result<O, E>>,
    {
        let id = ExecutionId::new();
        self.start_with_id(id, operation, input, body)
            .await
            .map(|result| (id, result))
    }

    pub async fn start_with_context<I, O, E, F, Fut>(
        &self,
        operation: OperationKey,
        input: I,
        body: F,
    ) -> Result<(ExecutionId, O), RunError>
    where
        I: Serialize + DeserializeOwned,
        O: Serialize + DeserializeOwned,
        E: std::fmt::Display,
        F: FnOnce(I, ExecutionContext<S, C>) -> Fut,
        Fut: Future<Output = Result<O, E>>,
    {
        let id = ExecutionId::new();
        self.start_with_id_and_context(id, operation, input, body)
            .await
            .map(|result| (id, result))
    }

    /// Idempotently create a caller-identified execution and run its stored input.
    pub async fn start_with_id<I, O, E, F, Fut>(
        &self,
        id: ExecutionId,
        operation: OperationKey,
        input: I,
        body: F,
    ) -> Result<O, RunError>
    where
        I: Serialize + DeserializeOwned,
        O: Serialize + DeserializeOwned,
        E: std::fmt::Display,
        F: FnOnce(I) -> Fut,
        Fut: Future<Output = Result<O, E>>,
    {
        let encoded = self.codec.encode(&input)?;
        self.store
            .create_execution(NewExecution::new(id, operation, now()).with_input(encoded))
            .await?;
        self.run_loaded(id, move |input, _context| body(input))
            .await
    }

    pub async fn start_with_id_and_context<I, O, E, F, Fut>(
        &self,
        id: ExecutionId,
        operation: OperationKey,
        input: I,
        body: F,
    ) -> Result<O, RunError>
    where
        I: Serialize + DeserializeOwned,
        O: Serialize + DeserializeOwned,
        E: std::fmt::Display,
        F: FnOnce(I, ExecutionContext<S, C>) -> Fut,
        Fut: Future<Output = Result<O, E>>,
    {
        let encoded = self.codec.encode(&input)?;
        self.store
            .create_execution(NewExecution::new(id, operation, now()).with_input(encoded))
            .await?;
        self.run_loaded(id, body).await
    }
}
