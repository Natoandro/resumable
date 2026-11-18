use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionInspection {
    pub execution: ExecutionRecord,
    pub steps: Vec<InspectedStep>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InspectedStep {
    pub step: Option<crate::StepRecord>,
    pub progress: Option<crate::Progress>,
}

impl<S: Store, C: ValueCodec> Engine<S, C> {
    /// Load an execution and a bounded set of named steps for application status APIs.
    pub async fn inspect(
        &self,
        id: ExecutionId,
        step_names: &[StepName],
    ) -> Result<ExecutionInspection, RunError> {
        if step_names.len() > crate::limits::MAX_INSPECTED_STEPS {
            return Err(RunError::Store(StoreError::Conflict(
                "inspection step limit exceeded".into(),
            )));
        }
        let execution = self
            .store
            .load_execution(id)
            .await?
            .ok_or(RunError::NotFound)?;
        let mut steps = Vec::with_capacity(step_names.len());
        for name in step_names {
            let step = self.store.load_step(id, name).await?;
            let progress = self.store.load_progress(id, name).await?;
            steps.push(InspectedStep { step, progress });
        }
        Ok(ExecutionInspection { execution, steps })
    }

    /// Durably request cancellation. Pending executions become terminal immediately.
    pub async fn cancel(&self, id: ExecutionId) -> Result<ExecutionRecord, RunError> {
        self.store
            .request_cancellation(crate::RequestCancellation {
                execution_id: id,
                at: now(),
            })
            .await
            .map_err(RunError::Store)
    }
}
