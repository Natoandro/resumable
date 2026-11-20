use super::super::*;

impl<C: Clock> MemoryStore<C> {
    pub(super) async fn create_execution_inner(
        &self,
        new: NewExecution,
    ) -> Result<ExecutionRecord, StoreError> {
        self.fault_before(FaultPoint::CreateExecution)?;
        let mut state = self.lock()?;
        let record = if let Some(existing) = state.executions.get(&new.id) {
            if existing.operation != new.operation
                || existing.input != new.input
                || existing.run_at != new.run_at
            {
                return Err(StoreError::Conflict("execution identity reused".into()));
            }
            existing.clone()
        } else {
            let record = ExecutionRecord::from(new);
            state.executions.insert(record.id, record.clone());
            record
        };
        drop(state);
        self.fault_after(FaultPoint::CreateExecution, record)
    }

    pub(super) async fn load_execution_inner(
        &self,
        id: ExecutionId,
    ) -> Result<Option<ExecutionRecord>, StoreError> {
        Ok(self.lock()?.executions.get(&id).cloned())
    }

    pub(super) async fn acquire_execution_inner(
        &self,
        id: ExecutionId,
        worker: WorkerId,
        claim_id: MutationId,
        lease_duration_ms: u64,
    ) -> Result<ClaimOutcome, StoreError> {
        self.acquire_execution_policy_inner(
            id,
            worker,
            claim_id,
            lease_duration_ms,
            crate::FailedExecutionPolicy::Resume,
        )
        .await
    }

    pub(super) async fn acquire_execution_policy_inner(
        &self,
        id: ExecutionId,
        worker: WorkerId,
        claim_id: MutationId,
        lease_duration_ms: u64,
        policy: crate::FailedExecutionPolicy,
    ) -> Result<ClaimOutcome, StoreError> {
        self.fault_before(FaultPoint::AcquireExecution)?;
        let mut state = self.lock()?;
        let now = self.clock.now();
        let cancelling = state
            .executions
            .get(&id)
            .is_some_and(|record| record.cancellation_requested_at.is_some());
        if !cancelling
            && state.steps.iter().any(|((execution, _), step)| {
                *execution == id && step.next_attempt_at.is_some_and(|at| at > now)
            })
        {
            return Ok(ClaimOutcome::Busy);
        }
        if !cancelling
            && state.steps.iter().any(|((execution, _), step)| {
                *execution == id
                    && step.status == StepStatus::Failed
                    && step
                        .failure
                        .as_ref()
                        .is_some_and(|failure| failure.class != FailureClass::Retryable)
            })
        {
            return Ok(ClaimOutcome::Terminal(
                state.executions.get(&id).unwrap().clone(),
            ));
        }
        let Some(record) = state.executions.get_mut(&id) else {
            return Ok(ClaimOutcome::NotFound);
        };
        if record.status.is_terminal() {
            return Ok(ClaimOutcome::Terminal(record.clone()));
        }
        if record.status == ExecutionStatus::Failed
            && policy == crate::FailedExecutionPolicy::Reject
        {
            return Ok(ClaimOutcome::Terminal(record.clone()));
        }
        if record.run_at.is_some_and(|at| at > now) {
            return Ok(ClaimOutcome::Busy);
        }
        if let Some(lease) = record.lease.as_ref().filter(|lease| lease.expires_at > now) {
            return if lease.owner == worker && lease.claim_id == claim_id {
                if lease.duration_ms != lease_duration_ms {
                    return Err(StoreError::Conflict(
                        "claim ID reused with different duration".into(),
                    ));
                }
                Ok(ClaimOutcome::Claimed(ExecutionClaim {
                    record: record.clone(),
                    worker,
                    token: lease.token,
                    generation: lease.generation,
                }))
            } else {
                Ok(ClaimOutcome::Busy)
            };
        }
        let generation = record
            .fencing_generation
            .checked_add(1)
            .ok_or_else(|| StoreError::Corrupt("lease fencing generation overflow".into()))?;
        let token = LeaseToken::new();
        let lease = Lease {
            owner: worker,
            claim_id,
            duration_ms: lease_duration_ms,
            token,
            generation,
            expires_at: Timestamp(
                now.0
                    .saturating_add(lease_duration_ms.min(i64::MAX as u64) as i64),
            ),
        };
        if record.status == ExecutionStatus::Failed {
            record
                .resume(now)
                .map_err(|e| StoreError::Conflict(e.to_string()))?;
            record
                .start(lease.clone(), now)
                .map_err(|e| StoreError::Conflict(e.to_string()))?;
        } else if record.status == ExecutionStatus::Pending {
            record
                .start(lease.clone(), now)
                .map_err(|e| StoreError::Conflict(e.to_string()))?;
        } else {
            Self::bump(record, now)?;
            record.fencing_generation = generation;
            record.lease = Some(lease.clone());
        }
        let claim = ExecutionClaim {
            record: record.clone(),
            worker,
            token,
            generation,
        };
        drop(state);
        self.fault_after(FaultPoint::AcquireExecution, ClaimOutcome::Claimed(claim))
    }

    pub(super) async fn load_step_inner(
        &self,
        execution: ExecutionId,
        name: &StepName,
    ) -> Result<Option<StepRecord>, StoreError> {
        Ok(self.lock()?.steps.get(&(execution, name.clone())).cloned())
    }
}
