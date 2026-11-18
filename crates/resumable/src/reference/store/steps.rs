use super::super::*;

impl<C: Clock> MemoryStore<C> {
    pub(super) async fn begin_step_inner(
        &self,
        request: BeginStep,
    ) -> Result<StepRecord, StoreError> {
        self.fault_before(FaultPoint::BeginStep)?;
        let mut state = self.lock()?;
        let now = self.clock.now();
        if let Some(step) = Self::replay(
            &state,
            request.mutation_id,
            MutationRequest::Begin(request.clone()),
            |value| match value {
                MutationValue::Step(step) => Some(step),
                _ => None,
            },
        )? {
            return Ok(step);
        }
        Self::check_lease(&state, &request.lease, now)?;
        Self::ensure_revision_capacity(&state, request.lease.execution_id)?;
        if state
            .executions
            .get(&request.lease.execution_id)
            .is_some_and(|execution| execution.cancellation_requested_at.is_some())
        {
            return Err(StoreError::Conflict("cancellation was requested".into()));
        }
        let key = (request.lease.execution_id, request.name.clone());
        if let Some(step) = state.steps.get(&key) {
            if step
                .retry_policy
                .as_ref()
                .is_some_and(|policy| policy != &request.retry_policy)
            {
                return Err(StoreError::Conflict(
                    "step retry policy differs from persisted policy".into(),
                ));
            }
            if step.status == StepStatus::Running
                && state.step_generations.get(&key) == Some(&request.lease.generation)
            {
                return Err(StoreError::Conflict(
                    "step attempt is already running under this lease".into(),
                ));
            }
            if step.status == StepStatus::Completed {
                return Err(StoreError::Conflict("completed step cannot restart".into()));
            }
            if step.status == StepStatus::Cancelled {
                return Err(StoreError::Conflict("cancelled step cannot restart".into()));
            }
            if step.status == StepStatus::Failed {
                let policy = step.retry_policy.as_ref().ok_or_else(|| {
                    StoreError::Corrupt("failed step has no persisted retry policy".into())
                })?;
                let failure = step.failure.as_ref().ok_or_else(|| {
                    StoreError::Corrupt("failed step has no persisted failure".into())
                })?;
                if failure.class != FailureClass::Retryable
                    || !policy.retryable
                    || step.attempt >= policy.max_attempts
                {
                    return Err(StoreError::Conflict(
                        "step failure is permanent or retry attempts are exhausted".into(),
                    ));
                }
            }
            if step.next_attempt_at.is_some_and(|due| due > now) {
                return Err(StoreError::Conflict("retry is not eligible yet".into()));
            }
        }
        let step = state
            .steps
            .entry(key.clone())
            .or_insert_with(|| StepRecord::new(StepId::new(), key.0, key.1.clone(), request.at));
        step.set_retry_policy(request.retry_policy.clone())
            .map_err(|e| StoreError::Conflict(e.to_string()))?;
        step.begin_attempt(request.at)
            .map_err(|e| StoreError::Conflict(e.to_string()))?;
        let result = step.clone();
        state.step_generations.insert(key, request.lease.generation);
        state.receipts.insert(
            request.mutation_id,
            (
                MutationRequest::Begin(request.clone()),
                MutationValue::Step(result.clone()),
            ),
        );
        let execution = state
            .executions
            .get_mut(&request.lease.execution_id)
            .unwrap();
        Self::bump(execution, request.at)?;
        drop(state);
        self.fault_after(FaultPoint::BeginStep, result)
    }

    pub(super) async fn complete_step_inner(
        &self,
        request: CompleteStep,
    ) -> Result<StepRecord, StoreError> {
        self.fault_before(FaultPoint::CompleteStep)?;
        let mut state = self.lock()?;
        let now = self.clock.now();
        if let Some(step) = Self::replay(
            &state,
            request.mutation_id,
            MutationRequest::CompleteStep(request.clone()),
            |value| match value {
                MutationValue::Step(step) => Some(step),
                _ => None,
            },
        )? {
            return Ok(step);
        }
        Self::check_lease(&state, &request.lease, now)?;
        Self::ensure_revision_capacity(&state, request.lease.execution_id)?;
        let key = (request.lease.execution_id, request.name.clone());
        let step = state
            .steps
            .get_mut(&key)
            .ok_or_else(|| StoreError::Conflict("step not found".into()))?;
        if step.attempt != request.expected_attempt {
            return Err(StoreError::Conflict("stale step attempt".into()));
        }
        step.complete_with_result(request.result.clone(), request.at)
            .map_err(|e| StoreError::Conflict(e.to_string()))?;
        let result = step.clone();
        state.receipts.insert(
            request.mutation_id,
            (
                MutationRequest::CompleteStep(request.clone()),
                MutationValue::Step(result.clone()),
            ),
        );
        Self::bump(
            state
                .executions
                .get_mut(&request.lease.execution_id)
                .unwrap(),
            request.at,
        )?;
        drop(state);
        self.fault_after(FaultPoint::CompleteStep, result)
    }

    pub(super) async fn fail_step_inner(
        &self,
        request: FailStep,
    ) -> Result<StepRecord, StoreError> {
        self.fault_before(FaultPoint::FailStep)?;
        let mut state = self.lock()?;
        let now = self.clock.now();
        if let Some(step) = Self::replay(
            &state,
            request.mutation_id,
            MutationRequest::FailStep(request.clone()),
            |value| match value {
                MutationValue::Step(step) => Some(step),
                _ => None,
            },
        )? {
            return Ok(step);
        }
        Self::check_lease(&state, &request.lease, now)?;
        Self::ensure_revision_capacity(&state, request.lease.execution_id)?;
        let key = (request.lease.execution_id, request.name.clone());
        let step = state
            .steps
            .get_mut(&key)
            .ok_or_else(|| StoreError::Conflict("step not found".into()))?;
        if step.attempt != request.expected_attempt {
            return Err(StoreError::Conflict("stale step attempt".into()));
        }
        step.set_retry_policy(request.retry_policy.clone())
            .map_err(|e| StoreError::Conflict(e.to_string()))?;
        step.fail(request.failure.clone(), request.retry_at)
            .map_err(|e| StoreError::Conflict(e.to_string()))?;
        let result = step.clone();
        state.receipts.insert(
            request.mutation_id,
            (
                MutationRequest::FailStep(request.clone()),
                MutationValue::Step(result.clone()),
            ),
        );
        Self::bump(
            state
                .executions
                .get_mut(&request.lease.execution_id)
                .unwrap(),
            result.updated_at,
        )?;
        drop(state);
        self.fault_after(FaultPoint::FailStep, result)
    }
}
