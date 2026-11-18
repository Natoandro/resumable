use super::*;

#[test]
fn retries_expiry_fencing_and_stale_revisions_are_enforced() {
    let store = MemoryStore::new(Timestamp(0));
    let id = ExecutionId::new();
    block_on(store.create_execution(NewExecution::new(
        id,
        OperationKey::new("lease-test", 1).unwrap(),
        Timestamp(0),
    )))
    .unwrap();
    let first = match block_on(store.acquire_execution(id, WorkerId::new(), MutationId::new(), 10))
        .unwrap()
    {
        ClaimOutcome::Claimed(claim) => claim,
        _ => panic!("first claim should succeed"),
    };
    assert!(matches!(
        block_on(store.acquire_execution(id, WorkerId::new(), MutationId::new(), 10)).unwrap(),
        ClaimOutcome::Busy
    ));
    let name = StepName::new("retryable").unwrap();
    let step = block_on(store.begin_step(BeginStep {
        lease: LeaseGuard {
            execution_id: id,
            worker: first.worker,
            token: first.token,
            generation: first.generation,
            expected_revision: first.record.revision,
        },
        name: name.clone(),
        retry_policy: crate::RetryPolicyRecord {
            max_attempts: 2,
            backoff: crate::BackoffRecord::None,
            retryable: true,
        },
        mutation_id: MutationId::new(),
        at: Timestamp(1),
    }))
    .unwrap();
    let current = block_on(store.load_execution(id)).unwrap().unwrap();
    block_on(store.fail_step(FailStep {
        lease: LeaseGuard {
            execution_id: id,
            worker: first.worker,
            token: first.token,
            generation: first.generation,
            expected_revision: current.revision,
        },
        name: name.clone(),
        expected_attempt: step.attempt,
        mutation_id: MutationId::new(),
        failure: FailureRecord {
            class: FailureClass::Retryable,
            diagnostic: None,
            occurred_at: Timestamp(2),
        },
        retry_at: Some(Timestamp(20)),
        retry_policy: crate::RetryPolicyRecord {
            max_attempts: 2,
            backoff: crate::BackoffRecord::None,
            retryable: true,
        },
    }))
    .unwrap();
    let current = block_on(store.load_execution(id)).unwrap().unwrap();
    let retry_guard = LeaseGuard {
        execution_id: id,
        worker: first.worker,
        token: first.token,
        generation: first.generation,
        expected_revision: current.revision,
    };
    assert!(matches!(
        block_on(store.begin_step(BeginStep {
            lease: retry_guard.clone(),
            name: name.clone(),
            retry_policy: crate::RetryPolicyRecord {
                max_attempts: 2,
                backoff: crate::BackoffRecord::None,
                retryable: true,
            },
            mutation_id: MutationId::new(),
            at: Timestamp(3),
        })),
        Err(StoreError::Conflict(_))
    ));

    store.clock().advance(20);
    let second =
        match block_on(store.acquire_execution(id, WorkerId::new(), MutationId::new(), 100))
            .unwrap()
        {
            ClaimOutcome::Claimed(claim) => claim,
            _ => panic!("expired lease should be taken over"),
        };
    assert!(second.generation > first.generation);
    assert!(matches!(
        block_on(store.begin_step(BeginStep {
            lease: retry_guard,
            name: name.clone(),
            retry_policy: crate::RetryPolicyRecord {
                max_attempts: 2,
                backoff: crate::BackoffRecord::None,
                retryable: true,
            },
            mutation_id: MutationId::new(),
            at: Timestamp(20),
        })),
        Err(StoreError::Conflict(_))
    ));
    let retry = block_on(store.begin_step(BeginStep {
        lease: LeaseGuard {
            execution_id: id,
            worker: second.worker,
            token: second.token,
            generation: second.generation,
            expected_revision: second.record.revision,
        },
        name: name.clone(),
        retry_policy: crate::RetryPolicyRecord {
            max_attempts: 2,
            backoff: crate::BackoffRecord::None,
            retryable: true,
        },
        mutation_id: MutationId::new(),
        at: Timestamp(20),
    }))
    .unwrap();
    assert_eq!(retry.id, step.id);
    assert_eq!(retry.attempt, 2);

    let current = block_on(store.load_execution(id)).unwrap().unwrap();
    let stale_guard = LeaseGuard {
        execution_id: id,
        worker: second.worker,
        token: second.token,
        generation: second.generation,
        expected_revision: current.revision,
    };
    let completed = block_on(store.complete_step(CompleteStep {
        lease: stale_guard.clone(),
        name: name.clone(),
        expected_attempt: retry.attempt,
        mutation_id: MutationId::new(),
        result: None,
        at: Timestamp(21),
    }))
    .unwrap();
    assert_eq!(completed.status, StepStatus::Completed);
    let before = block_on(store.load_execution(id)).unwrap().unwrap();
    store
        .faults()
        .inject_once(FaultPoint::Heartbeat, FaultMode::BeforeCommit);
    assert!(matches!(
        block_on(store.heartbeat(ProgressUpdate {
            lease: LeaseGuard {
                expected_revision: before.revision,
                ..stale_guard
            },
            step: Some(name.clone()),
            progress: Some(Progress {
                current: Some(1),
                total: Some(1),
                message: None
            }),
            lease_duration_ms: 100,
            at: Timestamp(22),
        })),
        Err(StoreError::Unavailable(_))
    ));
    assert_eq!(
        block_on(store.load_step(id, &name))
            .unwrap()
            .unwrap()
            .status,
        StepStatus::Completed
    );

    let exhausted_name = StepName::new("exhausted").unwrap();
    let current = block_on(store.load_execution(id)).unwrap().unwrap();
    let exhausted = block_on(store.begin_step(BeginStep {
        lease: LeaseGuard {
            expected_revision: current.revision,
            ..stale_guard.clone()
        },
        name: exhausted_name.clone(),
        retry_policy: crate::RetryPolicyRecord {
            max_attempts: 1,
            backoff: crate::BackoffRecord::None,
            retryable: true,
        },
        mutation_id: MutationId::new(),
        at: Timestamp(23),
    }))
    .unwrap();
    let current = block_on(store.load_execution(id)).unwrap().unwrap();
    block_on(store.fail_step(FailStep {
        lease: LeaseGuard {
            expected_revision: current.revision,
            ..stale_guard.clone()
        },
        name: exhausted_name.clone(),
        expected_attempt: exhausted.attempt,
        mutation_id: MutationId::new(),
        failure: FailureRecord {
            class: FailureClass::Retryable,
            diagnostic: None,
            occurred_at: Timestamp(24),
        },
        retry_at: Some(Timestamp(20)),
        retry_policy: crate::RetryPolicyRecord {
            max_attempts: 1,
            backoff: crate::BackoffRecord::None,
            retryable: true,
        },
    }))
    .unwrap();
    let current = block_on(store.load_execution(id)).unwrap().unwrap();
    assert!(matches!(
        block_on(store.begin_step(BeginStep {
            lease: LeaseGuard {
                expected_revision: current.revision,
                ..stale_guard
            },
            name: exhausted_name.clone(),
            retry_policy: crate::RetryPolicyRecord {
                max_attempts: 1,
                backoff: crate::BackoffRecord::None,
                retryable: true,
            },
            mutation_id: MutationId::new(),
            at: Timestamp(25),
        })),
        Err(StoreError::Conflict(_))
    ));
    assert_eq!(
        block_on(store.load_step(id, &exhausted_name))
            .unwrap()
            .unwrap()
            .attempt,
        1
    );
}
