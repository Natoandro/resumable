use super::*;

impl<S: Store, C: ValueCodec + 'static> ExecutionContext<S, C> {
    /// Run a named durable step, returning its stored result on later resumes.
    pub async fn step<T, E, F, Fut>(
        &self,
        name: impl Into<String>,
        body: F,
    ) -> Result<T, StepRunError<E>>
    where
        T: Serialize + DeserializeOwned,
        E: std::fmt::Display + std::fmt::Debug,
        F: Fn(StepContext) -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        self.step_with(name, StepOptions::default(), body).await
    }

    /// Run a named step using explicit retry options.
    ///
    /// Keep the retry options consistent across resumes. An interrupted attempt can run again,
    /// so use the identifiers in [`StepContext`] to reconcile or deduplicate external effects.
    pub async fn step_with<T, E, F, Fut>(
        &self,
        name: impl Into<String>,
        options: StepOptions,
        body: F,
    ) -> Result<T, StepRunError<E>>
    where
        T: Serialize + DeserializeOwned,
        E: std::fmt::Display + std::fmt::Debug,
        F: Fn(StepContext) -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        self.ensure_owned()?;
        let name = StepName::new(name)?;
        options
            .retry_policy
            .validate()
            .map_err(|e| StepRunError::Store(StoreError::Conflict(e.to_string())))?;
        let execution_id = self.execution_id();
        if let Some(existing) = self.store.load_step(execution_id, &name).await? {
            if existing
                .retry_policy
                .as_ref()
                .is_some_and(|policy| policy != &options.retry_policy.record(options.retryable))
            {
                return Err(StepRunError::Store(StoreError::Conflict(
                    "step retry policy differs from persisted policy".into(),
                )));
            }
            if existing.status == crate::StepStatus::Completed {
                let value = existing
                    .result
                    .as_ref()
                    .ok_or_else(|| StepRunError::MissingResult(name.to_string()))?;
                return self.codec.decode(value).map_err(StepRunError::Value);
            }
        }

        let mutation_id = MutationId::new();
        let request = BeginStep {
            lease: self.lease(),
            name: name.clone(),
            retry_policy: options.retry_policy.record(options.retryable),
            mutation_id,
            at: now(),
        };
        self.ensure_owned()?;
        let step = match self.store.begin_step(request.clone()).await {
            Ok(step) => step,
            Err(StoreError::Unavailable(_)) => self.store.begin_step(request).await?,
            Err(error) => return Err(StepRunError::Store(error)),
        };
        self.refresh_revision().await?;
        self.confirm_step(&step).await?;
        self.run_step(step, options, body).await
    }

    async fn confirm_step(&self, step: &crate::StepRecord) -> Result<(), StoreError> {
        let current = self
            .store
            .load_step(self.execution_id(), &step.name)
            .await?
            .ok_or_else(|| StoreError::Conflict("step disappeared".into()))?;
        if current.id != step.id
            || current.attempt != step.attempt
            || current.status != crate::StepStatus::Running
        {
            return Err(StoreError::Conflict("step attempt was replaced".into()));
        }
        // The Store's authoritative clock must confirm that the recovered lease is still live.
        self.heartbeat().await
    }
}
