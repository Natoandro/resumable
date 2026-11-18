use super::*;

#[test]
fn committed_completion_replays_after_response_loss_and_lease_expiry() {
    let store = MemoryStore::new(Timestamp(0));
    let id = ExecutionId::new();
    block_on(store.create_execution(NewExecution::new(
        id,
        OperationKey::new("ambiguous-completion", 1).unwrap(),
        Timestamp(0),
    )))
    .unwrap();
    let claim = match block_on(store.acquire_execution(id, WorkerId::new(), MutationId::new(), 10))
        .unwrap()
    {
        ClaimOutcome::Claimed(claim) => claim,
        _ => panic!("claim should succeed"),
    };
    let request = CompleteExecution {
        lease: LeaseGuard {
            execution_id: id,
            worker: claim.worker,
            token: claim.token,
            generation: claim.generation,
            expected_revision: claim.record.revision,
        },
        mutation_id: MutationId::new(),
        result: None,
        at: Timestamp(1),
    };
    let before = block_on(store.load_execution(id)).unwrap().unwrap();
    store
        .faults()
        .inject_once(FaultPoint::CompleteExecution, FaultMode::BeforeCommit);
    assert!(matches!(
        block_on(store.complete_execution(request.clone())),
        Err(StoreError::Unavailable(_))
    ));
    assert_eq!(block_on(store.load_execution(id)).unwrap().unwrap(), before);
    store
        .faults()
        .inject_once(FaultPoint::CompleteExecution, FaultMode::AfterCommit);
    assert!(matches!(
        block_on(store.complete_execution(request.clone())),
        Err(StoreError::Unavailable(_))
    ));
    store.clock().advance(11);
    let replay = block_on(store.complete_execution(request)).unwrap();
    assert_eq!(replay.status, ExecutionStatus::Completed);
}

#[test]
fn ambiguous_claim_is_returned_to_the_same_worker() {
    let store = MemoryStore::new(Timestamp(0));
    let id = ExecutionId::new();
    block_on(store.create_execution(NewExecution::new(
        id,
        OperationKey::new("ambiguous-claim", 1).unwrap(),
        Timestamp(0),
    )))
    .unwrap();
    let worker = WorkerId::new();
    let claim_id = MutationId::new();
    let before = block_on(store.load_execution(id)).unwrap().unwrap();
    store
        .faults()
        .inject_once(FaultPoint::AcquireExecution, FaultMode::BeforeCommit);
    assert!(matches!(
        block_on(store.acquire_execution(id, worker, claim_id, 100)),
        Err(StoreError::Unavailable(_))
    ));
    assert_eq!(block_on(store.load_execution(id)).unwrap().unwrap(), before);
    store
        .faults()
        .inject_once(FaultPoint::AcquireExecution, FaultMode::AfterCommit);
    assert!(matches!(
        block_on(store.acquire_execution(id, worker, claim_id, 100)),
        Err(StoreError::Unavailable(_))
    ));
    let retried = block_on(store.acquire_execution(id, worker, claim_id, 100)).unwrap();
    assert!(matches!(retried, ClaimOutcome::Claimed(_)));
}

#[test]
fn fail_execution_before_commit_is_atomic_and_after_commit_replays() {
    let store = MemoryStore::new(Timestamp(0));
    let id = ExecutionId::new();
    block_on(store.create_execution(NewExecution::new(
        id,
        OperationKey::new("ambiguous-failure", 1).unwrap(),
        Timestamp(0),
    )))
    .unwrap();
    let claim = match block_on(store.acquire_execution(id, WorkerId::new(), MutationId::new(), 100))
        .unwrap()
    {
        ClaimOutcome::Claimed(claim) => claim,
        _ => panic!("claim should succeed"),
    };
    let request = FailExecution {
        lease: LeaseGuard {
            execution_id: id,
            worker: claim.worker,
            token: claim.token,
            generation: claim.generation,
            expected_revision: claim.record.revision,
        },
        mutation_id: MutationId::new(),
        failure: FailureRecord {
            class: FailureClass::Permanent,
            diagnostic: None,
            occurred_at: Timestamp(1),
        },
    };
    store
        .faults()
        .inject_once(FaultPoint::FailExecution, FaultMode::BeforeCommit);
    assert!(matches!(
        block_on(store.fail_execution(request.clone())),
        Err(StoreError::Unavailable(_))
    ));
    let unchanged = block_on(store.load_execution(id)).unwrap().unwrap();
    assert_eq!(unchanged, claim.record);
    store
        .faults()
        .inject_once(FaultPoint::FailExecution, FaultMode::AfterCommit);
    assert!(matches!(
        block_on(store.fail_execution(request.clone())),
        Err(StoreError::Unavailable(_))
    ));
    let committed = block_on(store.load_execution(id)).unwrap().unwrap();
    assert_eq!(committed.status, ExecutionStatus::Failed);
    assert!(committed.lease.is_none());
    assert_eq!(block_on(store.fail_execution(request)).unwrap(), committed);
}
