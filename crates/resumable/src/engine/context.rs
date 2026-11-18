use super::*;

impl<S, C> ExecutionContext<S, C> {
    pub(super) fn engine_diagnostics(&self, message: String) -> Option<String> {
        self.capture_error_diagnostics.then_some(message)
    }
    pub fn execution_id(&self) -> ExecutionId {
        self.lease
            .lock()
            .expect("execution context poisoned")
            .guard
            .execution_id
    }

    /// Check the durable cancellation request while coordinating work outside a step.
    pub async fn is_cancelled(&self) -> Result<bool, StoreError>
    where
        S: Store,
    {
        let record = self
            .store
            .load_execution(self.execution_id())
            .await?
            .ok_or_else(|| StoreError::Conflict("execution disappeared".into()))?;
        self.adopt_revision(&record)?;
        Ok(record.cancellation_requested_at.is_some())
    }

    pub(super) fn lease(&self) -> LeaseGuard {
        let state = self.lease.lock().expect("execution context poisoned");
        state.guard.clone()
    }

    pub(super) async fn refresh_revision(&self) -> Result<(), StoreError>
    where
        S: Store,
    {
        let execution_id = self.execution_id();
        let record = self
            .store
            .load_execution(execution_id)
            .await?
            .ok_or_else(|| StoreError::Conflict("execution disappeared".into()))?;
        self.adopt_revision(&record)
    }

    pub(super) fn adopt_revision(&self, record: &ExecutionRecord) -> Result<(), StoreError> {
        self.ensure_owned()?;
        let mut state = self.lease.lock().expect("execution context poisoned");
        let guard = &state.guard;
        if record.status != ExecutionStatus::Running
            || !record.lease.as_ref().is_some_and(|lease| {
                lease.owner == guard.worker
                    && lease.token == guard.token
                    && lease.generation == guard.generation
            })
        {
            state.lost = Some("execution ownership changed".into());
            return Err(StoreError::Conflict("execution ownership changed".into()));
        }
        if state.lost.is_some() {
            return Err(StoreError::Conflict("lease ownership was lost".into()));
        }
        state.guard.expected_revision = record.revision;
        Ok(())
    }

    pub(super) fn ensure_owned(&self) -> Result<(), StoreError> {
        let state = self.lease.lock().expect("execution context poisoned");
        if let Some(reason) = &state.lost {
            return Err(StoreError::Conflict(format!(
                "lease ownership lost: {reason}"
            )));
        }
        Ok(())
    }
}
