use super::*;

pub(super) fn running_step() -> (MemoryStore, ExecutionId, LeaseGuard, StepName, StepRecord) {
    let store = MemoryStore::new(Timestamp(0));
    let id = ExecutionId::new();
    let name = StepName::new("fault-boundary").unwrap();
    block_on(store.create_execution(NewExecution::new(
        id,
        OperationKey::new("fault-boundary", 1).unwrap(),
        Timestamp(0),
    )))
    .unwrap();
    let claim = match block_on(store.acquire_execution(id, WorkerId::new(), MutationId::new(), 100))
        .unwrap()
    {
        ClaimOutcome::Claimed(claim) => claim,
        _ => panic!("claim should succeed"),
    };
    let first_guard = LeaseGuard {
        execution_id: id,
        worker: claim.worker,
        token: claim.token,
        generation: claim.generation,
        expected_revision: claim.record.revision,
    };
    let step = block_on(store.begin_step(BeginStep {
        lease: first_guard.clone(),
        name: name.clone(),
        retry_policy: crate::RetryPolicyRecord::default(),
        mutation_id: MutationId::new(),
        at: Timestamp(1),
    }))
    .unwrap();
    let execution = block_on(store.load_execution(id)).unwrap().unwrap();
    let guard = LeaseGuard {
        expected_revision: execution.revision,
        ..first_guard
    };
    (store, id, guard, name, step)
}

#[test]
fn begin_step_fault_boundaries_preserve_atomicity_and_replay() {
    let store = MemoryStore::new(Timestamp(0));
    let id = ExecutionId::new();
    block_on(store.create_execution(NewExecution::new(
        id,
        OperationKey::new("begin-boundary", 1).unwrap(),
        Timestamp(0),
    )))
    .unwrap();
    let claim = match block_on(store.acquire_execution(id, WorkerId::new(), MutationId::new(), 100))
        .unwrap()
    {
        ClaimOutcome::Claimed(claim) => claim,
        _ => panic!("claim should succeed"),
    };
    let request = BeginStep {
        lease: LeaseGuard {
            execution_id: id,
            worker: claim.worker,
            token: claim.token,
            generation: claim.generation,
            expected_revision: claim.record.revision,
        },
        name: StepName::new("begin").unwrap(),
        retry_policy: crate::RetryPolicyRecord::default(),
        mutation_id: MutationId::new(),
        at: Timestamp(1),
    };
    let before = block_on(store.load_execution(id)).unwrap().unwrap();
    store
        .faults()
        .inject_once(FaultPoint::BeginStep, FaultMode::BeforeCommit);
    assert!(block_on(store.begin_step(request.clone())).is_err());
    assert_eq!(block_on(store.load_step(id, &request.name)).unwrap(), None);
    assert_eq!(block_on(store.load_execution(id)).unwrap().unwrap(), before);
    store
        .faults()
        .inject_once(FaultPoint::BeginStep, FaultMode::AfterCommit);
    assert!(block_on(store.begin_step(request.clone())).is_err());
    let committed = block_on(store.load_step(id, &request.name))
        .unwrap()
        .unwrap();
    assert_eq!(committed.attempt, 1);
    assert_eq!(block_on(store.begin_step(request)).unwrap(), committed);
    assert_eq!(committed.attempt, 1);
}

#[test]
fn complete_step_fault_boundaries_preserve_atomicity_and_replay() {
    let (store, id, guard, name, step) = running_step();
    let request = CompleteStep {
        lease: guard,
        name: name.clone(),
        expected_attempt: step.attempt,
        mutation_id: MutationId::new(),
        result: None,
        at: Timestamp(2),
    };
    store
        .faults()
        .inject_once(FaultPoint::CompleteStep, FaultMode::BeforeCommit);
    assert!(block_on(store.complete_step(request.clone())).is_err());
    assert_eq!(block_on(store.load_step(id, &name)).unwrap().unwrap(), step);
    store
        .faults()
        .inject_once(FaultPoint::CompleteStep, FaultMode::AfterCommit);
    assert!(block_on(store.complete_step(request.clone())).is_err());
    let committed = block_on(store.complete_step(request)).unwrap();
    assert_eq!(committed.status, StepStatus::Completed);
}

#[test]
fn fail_step_fault_boundaries_preserve_atomicity_and_replay() {
    let (store, id, guard, name, step) = running_step();
    let request = FailStep {
        lease: guard,
        name: name.clone(),
        expected_attempt: step.attempt,
        mutation_id: MutationId::new(),
        failure: FailureRecord {
            class: FailureClass::Permanent,
            diagnostic: None,
            occurred_at: Timestamp(2),
        },
        retry_at: None,
        retry_policy: RetryPolicyRecord {
            max_attempts: 1,
            backoff: crate::BackoffRecord::None,
            retryable: false,
        },
    };
    store
        .faults()
        .inject_once(FaultPoint::FailStep, FaultMode::BeforeCommit);
    assert!(block_on(store.fail_step(request.clone())).is_err());
    assert_eq!(block_on(store.load_step(id, &name)).unwrap().unwrap(), step);
    store
        .faults()
        .inject_once(FaultPoint::FailStep, FaultMode::AfterCommit);
    assert!(block_on(store.fail_step(request.clone())).is_err());
    let committed = block_on(store.fail_step(request)).unwrap();
    assert_eq!(committed.status, StepStatus::Failed);
}
