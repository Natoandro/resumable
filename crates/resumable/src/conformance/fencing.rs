use super::*;

pub(super) async fn run<S: Store>(store: &S) -> Result<(), StoreError> {
    let fenced_id = ExecutionId::new();
    store
        .create_execution(NewExecution::new(
            fenced_id,
            OperationKey::new("conformance-fencing", 1).unwrap(),
            Timestamp(20),
        ))
        .await?;
    let first = match store
        .acquire_execution(fenced_id, WorkerId::new(), MutationId::new(), 60_000)
        .await?
    {
        ClaimOutcome::Claimed(claim) => claim,
        _ => {
            return Err(StoreError::Corrupt(
                "conformance failure: initial lease not granted".into(),
            ));
        }
    };
    let step_name = StepName::new("delayed-write").unwrap();
    let first_step = store
        .begin_step(BeginStep {
            lease: LeaseGuard {
                execution_id: fenced_id,
                worker: first.worker,
                token: first.token,
                generation: first.generation,
                expected_revision: first.record.revision,
            },
            name: step_name.clone(),
            retry_policy: RetryPolicyRecord::default(),
            mutation_id: MutationId::new(),
            at: Timestamp(21),
        })
        .await?;
    let current = store.load_execution(fenced_id).await?.unwrap();
    let old_guard = LeaseGuard {
        execution_id: fenced_id,
        worker: first.worker,
        token: first.token,
        generation: first.generation,
        expected_revision: current.revision,
    };
    store
        .heartbeat(ProgressUpdate {
            lease: old_guard.clone(),
            step: None,
            progress: None,
            lease_duration_ms: 0,
            at: Timestamp(22),
        })
        .await?;
    let second = match store
        .acquire_execution(fenced_id, WorkerId::new(), MutationId::new(), 60_000)
        .await?
    {
        ClaimOutcome::Claimed(claim) => claim,
        _ => {
            return Err(StoreError::Corrupt(
                "conformance failure: expired lease not replaced".into(),
            ));
        }
    };
    assertion(
        second.generation > first.generation,
        "takeover must advance fencing generation",
    )?;
    assertion(
        matches!(
            store
                .complete_step(CompleteStep {
                    lease: old_guard.clone(),
                    name: step_name.clone(),
                    expected_attempt: first_step.attempt,
                    mutation_id: MutationId::new(),
                    result: None,
                    at: Timestamp(23),
                })
                .await,
            Err(StoreError::Conflict(_))
        ),
        "replaced owner cannot complete a delayed step",
    )?;
    assertion(
        matches!(
            store
                .heartbeat(ProgressUpdate {
                    lease: old_guard,
                    step: Some(step_name),
                    progress: None,
                    lease_duration_ms: 60_000,
                    at: Timestamp(24),
                })
                .await,
            Err(StoreError::Conflict(_))
        ),
        "replaced owner cannot renew or publish progress",
    )?;

    Ok(())
}
