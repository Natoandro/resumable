use super::*;

impl<S: Store, C: ValueCodec> Engine<S, C> {
    pub(super) async fn run_loaded<I, O, E, F, Fut>(
        &self,
        id: ExecutionId,
        body: F,
    ) -> Result<O, RunError>
    where
        I: DeserializeOwned,
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
        if record.status == ExecutionStatus::Cancelled {
            return Err(RunError::Cancelled);
        }
        if let Some(result) = terminal_result(&record)? {
            return self.codec.decode(&result).map_err(RunError::Value);
        }
        let input = record.input.as_ref().ok_or(RunError::MissingInput)?;
        let input: I = self.codec.decode(input)?;
        let claim_id = MutationId::new();
        let duration = self.config.lease_duration.as_millis().min(u64::MAX as u128) as u64;
        let claim = self
            .store
            .acquire_execution_with_policy(
                id,
                self.config.worker_id,
                claim_id,
                duration,
                self.config.failed_execution_policy,
            )
            .await;
        let claim = match claim {
            Err(StoreError::Unavailable(_)) => {
                self.store
                    .acquire_execution_with_policy(
                        id,
                        self.config.worker_id,
                        claim_id,
                        duration,
                        self.config.failed_execution_policy,
                    )
                    .await?
            }
            result => result?,
        };
        let claim = match claim {
            ClaimOutcome::Claimed(claim) => claim,
            ClaimOutcome::Busy => return Err(RunError::Busy),
            ClaimOutcome::Terminal(record) => {
                if record.status == ExecutionStatus::Failed {
                    return Err(RunError::Failed);
                }
                if let Some(result) = terminal_result(&record)? {
                    return self.codec.decode(&result).map_err(RunError::Value);
                }
                return Err(RunError::Terminal);
            }
            ClaimOutcome::NotFound => return Err(RunError::NotFound),
        };
        let context = ExecutionContext {
            store: self.store.clone(),
            codec: self.codec.clone(),
            lease: Arc::new(Mutex::new(LeaseState {
                guard: LeaseGuard {
                    execution_id: id,
                    worker: claim.worker,
                    token: claim.token,
                    generation: claim.generation,
                    expected_revision: claim.record.revision,
                },
                lost: None,
            })),
            lease_duration: self.config.lease_duration,
            heartbeat_interval: self.config.heartbeat_interval,
            capture_error_diagnostics: self.config.capture_error_diagnostics,
        };
        if context.is_cancelled().await? {
            context.finish_execution(None).await?;
            return Err(RunError::Cancelled);
        }
        context.ensure_owned()?;
        let span = tracing::info_span!("resumable.execution",
            execution_id = %id, operation = ?record.operation,
            revision = claim.record.revision.0, status = tracing::field::Empty,
            duration_ms = tracing::field::Empty,
        );
        let started = std::time::Instant::now();
        let output = match body(input, context.clone()).instrument(span.clone()).await {
            Ok(output) => output,
            Err(error) => {
                span.record("status", "failed");
                span.record("duration_ms", started.elapsed().as_millis() as u64);
                context.ensure_owned()?;
                let failed_at = now();
                self.store
                    .fail_execution(crate::FailExecution {
                        lease: context.lease(),
                        mutation_id: MutationId::new(),
                        failure: FailureRecord {
                            class: FailureClass::Permanent,
                            diagnostic: self.engine_diagnostics(error.to_string()),
                            occurred_at: failed_at,
                        },
                    })
                    .await?;
                return Err(RunError::Operation(error.to_string()));
            }
        };
        context.ensure_owned()?;
        let result = self.codec.encode(&output)?;
        let record = context.finish_execution(Some(result)).await?;
        if record.status == ExecutionStatus::Cancelled {
            span.record("status", "cancelled");
            return Err(RunError::Cancelled);
        }
        span.record("status", "completed");
        span.record("duration_ms", started.elapsed().as_millis() as u64);
        Ok(output)
    }
}
