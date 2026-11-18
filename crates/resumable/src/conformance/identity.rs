use super::*;

pub(super) async fn run<S: Store>(store: &S) -> Result<(), StoreError> {
    let id = ExecutionId::new();
    let operation = OperationKey::new("conformance", 1).unwrap();
    let create = NewExecution::new(id, operation.clone(), Timestamp(10));
    let original = store.create_execution(create.clone()).await?;
    let replay = store.create_execution(create).await?;
    assertion(
        original == replay,
        "compatible create must preserve identity",
    )?;
    let incompatible = store
        .create_execution(NewExecution::new(
            id,
            OperationKey::new("different", 1).unwrap(),
            Timestamp(10),
        ))
        .await;
    assertion(
        matches!(incompatible, Err(StoreError::Conflict(_))),
        "identity reuse must conflict",
    )?;

    let worker = WorkerId::new();
    let claim = match store
        .acquire_execution(id, worker, MutationId::new(), 60_000)
        .await?
    {
        ClaimOutcome::Claimed(claim) => claim,
        _ => {
            return Err(StoreError::Corrupt(
                "conformance failure: first claim was not granted".into(),
            ));
        }
    };
    assertion(
        matches!(
            store
                .acquire_execution(id, WorkerId::new(), MutationId::new(), 60_000)
                .await?,
            ClaimOutcome::Busy
        ),
        "concurrent claimant must observe busy",
    )?;
    let name = StepName::new("single-step").unwrap();
    let guard = LeaseGuard {
        execution_id: id,
        worker,
        token: claim.token,
        generation: claim.generation,
        expected_revision: claim.record.revision,
    };
    let first_attempt = store
        .begin_step(BeginStep {
            lease: guard,
            name: name.clone(),
            retry_policy: crate::RetryPolicyRecord {
                max_attempts: 2,
                backoff: crate::BackoffRecord::None,
                retryable: true,
            },
            mutation_id: MutationId::new(),
            at: Timestamp(11),
        })
        .await?;
    let failed = store
        .fail_step(FailStep {
            lease: LeaseGuard {
                execution_id: id,
                worker,
                token: claim.token,
                generation: claim.generation,
                expected_revision: store.load_execution(id).await?.unwrap().revision,
            },
            name: name.clone(),
            expected_attempt: first_attempt.attempt,
            mutation_id: MutationId::new(),
            failure: FailureRecord {
                class: FailureClass::Retryable,
                diagnostic: Some("conformance retry".into()),
                occurred_at: Timestamp(12),
            },
            retry_at: Some(Timestamp(0)),
            retry_policy: crate::RetryPolicyRecord {
                max_attempts: 2,
                backoff: crate::BackoffRecord::None,
                retryable: true,
            },
        })
        .await?;
    let current = store.load_execution(id).await?.unwrap();
    assertion(
        matches!(
            store
                .begin_step(BeginStep {
                    lease: LeaseGuard {
                        execution_id: id,
                        worker,
                        token: claim.token,
                        generation: claim.generation,
                        expected_revision: current.revision,
                    },
                    name: name.clone(),
                    retry_policy: crate::RetryPolicyRecord {
                        max_attempts: 3,
                        backoff: crate::BackoffRecord::None,
                        retryable: true,
                    },
                    mutation_id: MutationId::new(),
                    at: Timestamp(13),
                })
                .await,
            Err(StoreError::Conflict(_))
        ),
        "a retry cannot change the policy persisted at first begin",
    )?;
    assertion(
        store.load_step(id, &name).await?.unwrap() == failed,
        "a rejected policy change cannot alter the failed attempt",
    )?;
    let step = store
        .begin_step(BeginStep {
            lease: LeaseGuard {
                execution_id: id,
                worker,
                token: claim.token,
                generation: claim.generation,
                expected_revision: current.revision,
            },
            name: name.clone(),
            retry_policy: crate::RetryPolicyRecord {
                max_attempts: 2,
                backoff: crate::BackoffRecord::None,
                retryable: true,
            },
            mutation_id: MutationId::new(),
            at: Timestamp(13),
        })
        .await?;
    assertion(
        step.id == first_attempt.id && step.attempt == failed.attempt + 1,
        "retry must retain step identity and increment attempt",
    )?;
    let current = store
        .load_execution(id)
        .await?
        .ok_or_else(|| StoreError::Corrupt("execution disappeared".into()))?;
    let guard = LeaseGuard {
        execution_id: id,
        worker,
        token: claim.token,
        generation: claim.generation,
        expected_revision: current.revision,
    };
    let done = store
        .complete_step(CompleteStep {
            lease: guard,
            name: name.clone(),
            expected_attempt: step.attempt,
            mutation_id: MutationId::new(),
            result: None,
            at: Timestamp(12),
        })
        .await?;
    assertion(
        done.status == StepStatus::Completed,
        "completion must persist before return",
    )?;

    let current = store.load_execution(id).await?.unwrap();
    let guard = LeaseGuard {
        execution_id: id,
        worker,
        token: claim.token,
        generation: claim.generation,
        expected_revision: current.revision,
    };
    let completed = store
        .complete_execution(CompleteExecution {
            lease: guard,
            mutation_id: MutationId::new(),
            result: None,
            at: Timestamp(13),
        })
        .await?;
    assertion(
        completed.status == ExecutionStatus::Completed,
        "execution must complete",
    )?;
    assertion(
        matches!(
            store
                .acquire_execution(id, WorkerId::new(), MutationId::new(), 100)
                .await?,
            ClaimOutcome::Terminal(_)
        ),
        "terminal execution must not be claimed",
    )?;

    Ok(())
}
