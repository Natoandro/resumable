use super::*;

pub(super) async fn run<S: Store>(store: &S) -> Result<(), StoreError> {
    let id = ExecutionId::new();
    store
        .create_execution(NewExecution::new(
            id,
            OperationKey::new("conformance-claim-identity", 1).unwrap(),
            Timestamp(0),
        ))
        .await?;
    let worker = WorkerId::new();
    let claim_id = MutationId::new();
    let claim = match store
        .acquire_execution(id, worker, claim_id, 60_000)
        .await?
    {
        ClaimOutcome::Claimed(claim) => claim,
        _ => {
            return Err(StoreError::Corrupt(
                "conformance: claim was not granted".into(),
            ));
        }
    };
    assertion(
        matches!(
            store
                .acquire_execution(id, worker, MutationId::new(), 60_000)
                .await?,
            ClaimOutcome::Busy
        ),
        "same worker with a distinct claim ID must observe busy",
    )?;
    assertion(
        matches!(store.acquire_execution(id, worker, claim_id, 60_000).await?, ClaimOutcome::Claimed(ref replay) if replay == &claim),
        "identical claim replay must retain token and generation",
    )?;
    assertion(
        matches!(
            store.acquire_execution(id, worker, claim_id, 30_000).await,
            Err(StoreError::Conflict(_))
        ),
        "claim ID cannot recover a changed request",
    )?;
    let name = StepName::new("live-attempt").unwrap();
    let begin = BeginStep {
        lease: LeaseGuard {
            execution_id: id,
            worker,
            token: claim.token,
            generation: claim.generation,
            expected_revision: claim.record.revision,
        },
        name,
        retry_policy: crate::RetryPolicyRecord::default(),
        mutation_id: MutationId::new(),
        at: Timestamp(1),
    };
    let step = store.begin_step(begin.clone()).await?;
    assertion(
        store.begin_step(begin.clone()).await? == step,
        "begin replay must retain attempt",
    )?;
    let current = store.load_execution(id).await?.unwrap();
    assertion(
        matches!(
            store
                .begin_step(BeginStep {
                    lease: LeaseGuard {
                        expected_revision: current.revision,
                        ..begin.lease.clone()
                    },
                    mutation_id: MutationId::new(),
                    ..begin.clone()
                })
                .await,
            Err(StoreError::Conflict(_))
        ),
        "a new mutation cannot restart a live running attempt",
    )?;
    assertion(
        store.load_execution(id).await?.unwrap() == current,
        "rejected begin cannot alter execution",
    )?;
    assertion(
        store.load_step(id, &step.name).await?.unwrap() == step,
        "rejected begin cannot alter attempt",
    )?;
    let failed = store
        .fail_execution(FailExecution {
            lease: LeaseGuard {
                expected_revision: current.revision,
                ..begin.lease
            },
            mutation_id: MutationId::new(),
            failure: FailureRecord {
                class: FailureClass::Permanent,
                diagnostic: None,
                occurred_at: Timestamp(2),
            },
        })
        .await?;
    assertion(
        failed.lease.is_none() && failed.fencing_generation == claim.generation,
        "failure must retain the last generation after lease removal",
    )?;
    let resumed = match store
        .acquire_execution(id, worker, MutationId::new(), 60_000)
        .await?
    {
        ClaimOutcome::Claimed(claim) => claim,
        _ => {
            return Err(StoreError::Corrupt(
                "conformance: failure resume was not granted".into(),
            ));
        }
    };
    assertion(
        resumed.generation > claim.generation,
        "failure resume must increase generation",
    )?;
    Ok(())
}
