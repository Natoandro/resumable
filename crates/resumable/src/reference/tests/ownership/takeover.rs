use super::*;

#[test]
fn replaced_owner_cannot_complete_or_renew_after_takeover() {
    let store = MemoryStore::new(Timestamp(0));
    let id = ExecutionId::new();
    block_on(store.create_execution(NewExecution::new(
        id,
        OperationKey::new("delayed-write", 1).unwrap(),
        Timestamp(0),
    )))
    .unwrap();
    let first = match block_on(store.acquire_execution(id, WorkerId::new(), MutationId::new(), 10))
        .unwrap()
    {
        ClaimOutcome::Claimed(claim) => claim,
        _ => panic!("first claim should succeed"),
    };
    let name = StepName::new("effect").unwrap();
    let first_step = block_on(store.begin_step(BeginStep {
        lease: LeaseGuard {
            execution_id: id,
            worker: first.worker,
            token: first.token,
            generation: first.generation,
            expected_revision: first.record.revision,
        },
        name: name.clone(),
        retry_policy: crate::RetryPolicyRecord::default(),
        mutation_id: MutationId::new(),
        at: Timestamp(1),
    }))
    .unwrap();
    let old_revision = block_on(store.load_execution(id))
        .unwrap()
        .unwrap()
        .revision;
    store.clock().advance(10);
    let second = match block_on(store.acquire_execution(id, WorkerId::new(), MutationId::new(), 10))
        .unwrap()
    {
        ClaimOutcome::Claimed(claim) => claim,
        _ => panic!("expired lease should be taken over"),
    };
    assert!(second.generation > first.generation);
    let stale = LeaseGuard {
        execution_id: id,
        worker: first.worker,
        token: first.token,
        generation: first.generation,
        expected_revision: old_revision,
    };
    assert!(matches!(
        block_on(store.complete_step(CompleteStep {
            lease: stale.clone(),
            name: name.clone(),
            expected_attempt: first_step.attempt,
            mutation_id: MutationId::new(),
            result: None,
            at: Timestamp(11),
        })),
        Err(StoreError::Conflict(_))
    ));
    assert!(matches!(
        block_on(store.heartbeat(ProgressUpdate {
            lease: stale,
            step: Some(name),
            progress: None,
            lease_duration_ms: 10,
            at: Timestamp(11),
        })),
        Err(StoreError::Conflict(_))
    ));
}
