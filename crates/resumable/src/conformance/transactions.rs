use super::*;

pub(super) async fn run<S: Store>(store: &S) -> Result<(), StoreError> {
    let id = ExecutionId::new();
    store
        .create_execution(NewExecution::new(
            id,
            OperationKey::new("conformance-mutation-replay", 1).unwrap(),
            Timestamp(0),
        ))
        .await?;
    let claim = match store
        .acquire_execution(id, WorkerId::new(), MutationId::new(), 60_000)
        .await?
    {
        ClaimOutcome::Claimed(claim) => claim,
        _ => {
            return Err(StoreError::Corrupt(
                "conformance: claim was not granted".into(),
            ));
        }
    };
    let name = StepName::new("replayed-completion").unwrap();
    let step = store
        .begin_step(BeginStep {
            lease: LeaseGuard {
                execution_id: id,
                worker: claim.worker,
                token: claim.token,
                generation: claim.generation,
                expected_revision: claim.record.revision,
            },
            name: name.clone(),
            retry_policy: crate::RetryPolicyRecord::default(),
            mutation_id: MutationId::new(),
            at: Timestamp(1),
        })
        .await?;
    let current = store.load_execution(id).await?.unwrap();
    let result = crate::StoredValue::new(
        crate::limits::DEFAULT_CODEC_ID,
        crate::limits::DEFAULT_SCHEMA_VERSION,
        b"stored-result".to_vec(),
    )
    .unwrap();
    let completion = CompleteStep {
        lease: LeaseGuard {
            execution_id: id,
            worker: claim.worker,
            token: claim.token,
            generation: claim.generation,
            expected_revision: current.revision,
        },
        name: name.clone(),
        expected_attempt: step.attempt,
        mutation_id: MutationId::new(),
        result: Some(result.clone()),
        at: Timestamp(2),
    };
    let completed = store.complete_step(completion.clone()).await?;
    assertion(
        store.complete_step(completion.clone()).await? == completed,
        "identical completion replay must return the committed outcome",
    )?;
    let changed_request = CompleteStep {
        result: None,
        ..completion
    };
    assertion(
        matches!(
            store.complete_step(changed_request).await,
            Err(StoreError::Conflict(_))
        ),
        "a mutation ID cannot replay a changed completion request",
    )?;

    let current = store.load_execution(id).await?.unwrap();
    let stale_revision = current.revision;
    store
        .heartbeat(ProgressUpdate {
            lease: LeaseGuard {
                execution_id: id,
                worker: claim.worker,
                token: claim.token,
                generation: claim.generation,
                expected_revision: current.revision,
            },
            step: None,
            progress: None,
            lease_duration_ms: 60_000,
            at: Timestamp(3),
        })
        .await?;
    let before = store.load_execution(id).await?.unwrap();
    let stale_begin = BeginStep {
        lease: LeaseGuard {
            execution_id: id,
            worker: claim.worker,
            token: claim.token,
            generation: claim.generation,
            expected_revision: stale_revision,
        },
        name: StepName::new("stale-revision").unwrap(),
        retry_policy: crate::RetryPolicyRecord::default(),
        mutation_id: MutationId::new(),
        at: Timestamp(4),
    };
    assertion(
        matches!(
            store.begin_step(stale_begin).await,
            Err(StoreError::Conflict(_))
        ),
        "a stale revision must conflict while owner, token, generation, and lease remain current",
    )?;
    assertion(
        store.load_execution(id).await?.unwrap() == before,
        "stale-revision rejection cannot change execution state",
    )?;
    assertion(
        store
            .load_step(id, &StepName::new("stale-revision").unwrap())
            .await?
            .is_none(),
        "stale-revision rejection cannot create a step",
    )
}
