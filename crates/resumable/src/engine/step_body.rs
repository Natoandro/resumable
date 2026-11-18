use super::*;

impl<S: Store, C: ValueCodec + 'static> ExecutionContext<S, C> {
    pub(super) async fn run_step<T, E, F, Fut>(
        &self,
        step: crate::StepRecord,
        options: StepOptions,
        body: F,
    ) -> Result<T, StepRunError<E>>
    where
        T: Serialize + DeserializeOwned,
        E: std::fmt::Display + std::fmt::Debug,
        F: Fn(StepContext) -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let execution_id = self.execution_id();
        let name = step.name.clone();
        let context = self.step_context(&step);
        let span = tracing::info_span!("resumable.step_attempt",
            execution_id = %execution_id, step_id = %step.id, step = %name,
            attempt = step.attempt, revision = self.lease().expected_revision.0,
            retry_delay_ms = tracing::field::Empty,
            duration_ms = tracing::field::Empty,
            status = tracing::field::Empty,
        );
        let started = std::time::Instant::now();
        let output = match body(context).instrument(span.clone()).await {
            Ok(output) => output,
            Err(source) => {
                span.record("status", "failed");
                span.record("duration_ms", started.elapsed().as_millis() as u64);
                self.ensure_owned()?;
                let failure = FailureRecord {
                    class: if options.retryable && step.attempt < options.retry_policy.max_attempts
                    {
                        FailureClass::Retryable
                    } else {
                        FailureClass::Permanent
                    },
                    diagnostic: self.engine_diagnostics(source.to_string()),
                    occurred_at: now(),
                };
                let retry_delay_ms =
                    if options.retryable && step.attempt < options.retry_policy.max_attempts {
                        options
                            .retry_policy
                            .delay(
                                step.attempt,
                                execution_id
                                    .to_string()
                                    .bytes()
                                    .fold(step.attempt as u64, |n, b| {
                                        n.wrapping_mul(31).wrapping_add(b as u64)
                                    }),
                            )
                            .as_millis()
                            .min(u64::MAX as u128) as u64
                    } else {
                        0
                    };
                span.record("retry_delay_ms", retry_delay_ms);
                let request = crate::FailStep {
                    lease: self.lease(),
                    name: name.clone(),
                    expected_attempt: step.attempt,
                    mutation_id: MutationId::new(),
                    failure,
                    retry_at: if retry_delay_ms > 0 {
                        Some(Timestamp(
                            now()
                                .0
                                .saturating_add(retry_delay_ms.min(i64::MAX as u64) as i64),
                        ))
                    } else {
                        None
                    },
                    retry_policy: options.retry_policy.record(options.retryable),
                };
                match self.store.fail_step(request.clone()).await {
                    Ok(_) => {
                        self.refresh_revision().await?;
                    }
                    Err(StoreError::Unavailable(_)) => {
                        self.store.fail_step(request).await?;
                        self.refresh_revision().await?;
                    }
                    Err(error) => return Err(StepRunError::Store(error)),
                }
                if retry_delay_ms > 0 {
                    tracing::info!(
                        execution_id = %execution_id,
                        step = %name,
                        attempt = step.attempt,
                        retry_delay_ms,
                        "resumable step retry scheduled"
                    );
                }
                return Err(StepRunError::Operation {
                    name: name.to_string(),
                    source,
                });
            }
        };
        span.record("status", "completed");
        span.record("duration_ms", started.elapsed().as_millis() as u64);
        let result = self.codec.encode(&output)?;
        self.ensure_owned()?;
        // An adapter callback may have committed the domain write and step receipt in the same
        // caller-owned transaction. Observe that committed checkpoint before issuing the normal
        // Store completion, which would otherwise conflict with the already completed step.
        if let Some(completed) = self
            .store
            .load_step(execution_id, &name)
            .await
            .map_err(|error| StepRunError::Store(error))?
        {
            if completed.attempt == step.attempt
                && completed.status == crate::StepStatus::Completed
                && completed.result.as_ref() == Some(&result)
            {
                self.refresh_revision().await?;
                return Ok(output);
            }
        }
        let request = CompleteStep {
            lease: self.lease(),
            name: name.clone(),
            expected_attempt: step.attempt,
            mutation_id: MutationId::new(),
            result: Some(result),
            at: now(),
        };
        match self.store.complete_step(request.clone()).await {
            Ok(_) => {
                self.refresh_revision().await?;
            }
            Err(StoreError::Unavailable(_)) => {
                self.store.complete_step(request).await?;
                self.refresh_revision().await?;
            }
            Err(error) => return Err(StepRunError::Store(error)),
        }
        tracing::info!(
            execution_id = %execution_id,
            step = %name,
            attempt = step.attempt,
            duration_ms = started.elapsed().as_millis() as u64,
            "resumable step completed"
        );
        Ok(output)
    }
}
