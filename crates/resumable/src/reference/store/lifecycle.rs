use super::super::*;

impl<C: Clock> MemoryStore<C> {
    pub(super) async fn complete_execution_inner(
        &self,
        request: CompleteExecution,
    ) -> Result<ExecutionRecord, StoreError> {
        self.fault_before(FaultPoint::CompleteExecution)?;
        let mut state = self.lock()?;
        let now = self.clock.now();
        if let Some(record) = Self::replay(
            &state,
            request.mutation_id,
            MutationRequest::CompleteExecution(request.clone()),
            |value| match value {
                MutationValue::Execution(record) => Some(record),
                _ => None,
            },
        )? {
            return Ok(record);
        }
        Self::check_lease(&state, &request.lease, now)?;
        Self::ensure_revision_capacity(&state, request.lease.execution_id)?;
        let cancellation_requested = state
            .executions
            .get(&request.lease.execution_id)
            .is_some_and(|record| record.cancellation_requested_at.is_some());
        if !cancellation_requested
            && state.steps.iter().any(|((execution_id, _), step)| {
                *execution_id == request.lease.execution_id && step.status != StepStatus::Completed
            })
        {
            return Err(StoreError::Conflict(
                "execution has steps that are not completed".into(),
            ));
        }
        let record = state
            .executions
            .get_mut(&request.lease.execution_id)
            .unwrap();
        record
            .finish_with_result(request.result.clone(), request.at)
            .map_err(|e| StoreError::Conflict(e.to_string()))?;
        let result = record.clone();
        state.receipts.insert(
            request.mutation_id,
            (
                MutationRequest::CompleteExecution(request.clone()),
                MutationValue::Execution(result.clone()),
            ),
        );
        drop(state);
        self.fault_after(FaultPoint::CompleteExecution, result)
    }

    pub(super) async fn fail_execution_inner(
        &self,
        request: FailExecution,
    ) -> Result<ExecutionRecord, StoreError> {
        self.fault_before(FaultPoint::FailExecution)?;
        let mut state = self.lock()?;
        let now = self.clock.now();
        if let Some(record) = Self::replay(
            &state,
            request.mutation_id,
            MutationRequest::FailExecution(request.clone()),
            |value| match value {
                MutationValue::Execution(record) => Some(record),
                _ => None,
            },
        )? {
            return Ok(record);
        }
        Self::check_lease(&state, &request.lease, now)?;
        Self::ensure_revision_capacity(&state, request.lease.execution_id)?;
        let record = state
            .executions
            .get_mut(&request.lease.execution_id)
            .unwrap();
        record
            .fail(request.failure.clone())
            .map_err(|e| StoreError::Conflict(e.to_string()))?;
        let result = record.clone();
        state.receipts.insert(
            request.mutation_id,
            (
                MutationRequest::FailExecution(request.clone()),
                MutationValue::Execution(result.clone()),
            ),
        );
        drop(state);
        self.fault_after(FaultPoint::FailExecution, result)
    }

    pub(super) async fn request_cancellation_inner(
        &self,
        request: RequestCancellation,
    ) -> Result<ExecutionRecord, StoreError> {
        self.fault_before(FaultPoint::Cancellation)?;
        let mut state = self.lock()?;
        let record = state
            .executions
            .get_mut(&request.execution_id)
            .ok_or_else(|| StoreError::Conflict("execution not found".into()))?;
        record
            .request_cancellation(request.at)
            .map_err(|e| StoreError::Conflict(e.to_string()))?;
        let result = record.clone();
        drop(state);
        self.fault_after(FaultPoint::Cancellation, result)
    }

    pub(super) async fn heartbeat_inner(
        &self,
        request: ProgressUpdate,
    ) -> Result<ExecutionRecord, StoreError> {
        self.fault_before(FaultPoint::Heartbeat)?;
        let mut state = self.lock()?;
        let now = self.clock.now();
        Self::check_lease(&state, &request.lease, now)?;
        Self::ensure_revision_capacity(&state, request.lease.execution_id)?;
        let record = state
            .executions
            .get_mut(&request.lease.execution_id)
            .unwrap();
        let lease = record.lease.as_mut().unwrap();
        lease.expires_at = Timestamp(
            now.0
                .saturating_add(request.lease_duration_ms.min(i64::MAX as u64) as i64),
        );
        Self::bump(record, request.at)?;
        if let (Some(step), Some(progress)) = (request.step, request.progress) {
            state
                .progress
                .insert((request.lease.execution_id, step), progress);
        }
        let result = state
            .executions
            .get(&request.lease.execution_id)
            .unwrap()
            .clone();
        drop(state);
        self.fault_after(FaultPoint::Heartbeat, result)
    }

    pub(super) async fn purge_execution_inner(&self, id: ExecutionId) -> Result<bool, StoreError> {
        self.fault_before(FaultPoint::Purge)?;
        let mut state = self.lock()?;
        let Some(record) = state.executions.get(&id) else {
            return Ok(false);
        };
        if !matches!(
            record.status,
            ExecutionStatus::Completed | ExecutionStatus::Cancelled
        ) {
            return Err(StoreError::Conflict(
                "only completed or cancelled executions can be purged".into(),
            ));
        }
        state.executions.remove(&id);
        state.steps.retain(|(execution, _), _| *execution != id);
        state
            .step_generations
            .retain(|(execution, _), _| *execution != id);
        state.progress.retain(|(execution, _), _| *execution != id);
        state.receipts.retain(|_, (request, _)| {
            let execution_id = match request {
                MutationRequest::Begin(request) => request.lease.execution_id,
                MutationRequest::CompleteStep(request) => request.lease.execution_id,
                MutationRequest::FailStep(request) => request.lease.execution_id,
                MutationRequest::CompleteExecution(request) => request.lease.execution_id,
                MutationRequest::FailExecution(request) => request.lease.execution_id,
            };
            execution_id != id
        });
        drop(state);
        self.fault_after(FaultPoint::Purge, true)
    }
}
