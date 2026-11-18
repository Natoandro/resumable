use super::*;

impl<S: Store, C: ValueCodec + 'static> ExecutionContext<S, C> {
    pub(super) async fn update_progress(
        &self,
        step: StepName,
        progress: crate::Progress,
    ) -> Result<(), StoreError> {
        if progress
            .message
            .as_ref()
            .is_some_and(|m| m.len() > crate::limits::MAX_PROGRESS_MESSAGE_BYTES)
        {
            return Err(StoreError::Conflict(
                "progress message exceeds size limit".into(),
            ));
        }
        self.ensure_owned()?;
        let request = crate::ProgressUpdate {
            lease: self.lease(),
            step: Some(step),
            progress: Some(progress),
            lease_duration_ms: self.lease_duration.as_millis().min(u64::MAX as u128) as u64,
            at: now(),
        };
        let record = match self.store.heartbeat(request).await {
            Ok(record) => record,
            Err(error) => {
                // A backend may have committed the heartbeat and lost its response. Refresh the
                // revision only when the same lease still owns the running execution.
                if matches!(error, StoreError::Unavailable(_) | StoreError::Conflict(_)) {
                    if let Some(record) = self.store.load_execution(self.execution_id()).await? {
                        self.adopt_revision(&record)?;
                    }
                }
                return Err(error);
            }
        };
        self.adopt_revision(&record)?;
        Ok(())
    }

    /// Extend the current lease. A failed heartbeat permanently marks this context as lost.
    /// Call this from long-running work at intervals shorter than the configured lease duration.
    pub async fn heartbeat(&self) -> Result<(), StoreError> {
        self.ensure_owned()?;
        let request = crate::ProgressUpdate {
            lease: self.lease(),
            step: None,
            progress: None,
            lease_duration_ms: self.lease_duration.as_millis().min(u64::MAX as u128) as u64,
            at: now(),
        };
        match self.store.heartbeat(request).await {
            Ok(record) => {
                self.adopt_revision(&record)?;
                Ok(())
            }
            Err(error) => {
                let mut state = self.lease.lock().expect("execution context poisoned");
                state.lost = Some(error.to_string());
                Err(error)
            }
        }
    }

    /// Recommended cadence for application-managed heartbeat supervision.
    pub fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }
}
