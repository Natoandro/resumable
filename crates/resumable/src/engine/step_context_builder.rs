use super::*;

impl<S: Store, C: ValueCodec + 'static> ExecutionContext<S, C> {
    pub(super) fn step_context(&self, step: &crate::StepRecord) -> StepContext {
        StepContext {
            execution_id: self.execution_id(),
            step_id: step.id,
            name: step.name.clone(),
            attempt: step.attempt,
            fencing_token: self.lease().generation,
            lease: self.lease(),
            cancellation_check: {
                let execution = self.clone();
                Arc::new(move || {
                    let execution = execution.clone();
                    Box::pin(async move { execution.is_cancelled().await })
                })
            },
            progress_update: {
                let execution = self.clone();
                let step = step.name.clone();
                Arc::new(move |progress| {
                    let execution = execution.clone();
                    let step = step.clone();
                    Box::pin(async move { execution.update_progress(step, progress).await })
                })
            },
        }
    }
}
