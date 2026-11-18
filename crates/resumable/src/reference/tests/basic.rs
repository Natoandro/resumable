use super::*;

#[test]
fn shared_store_scenarios_pass() {
    let store = MemoryStore::new(Timestamp(0));
    block_on(crate::conformance::run_store_scenarios(&store)).unwrap();
}

#[test]
fn state_machine_sequences_preserve_identity_and_terminal_completion() {
    let store = MemoryStore::new(Timestamp(0));
    for sequence in 0..64 {
        let id = ExecutionId::new();
        block_on(store.create_execution(NewExecution::new(
            id,
            OperationKey::new("state-sequence", 1).unwrap(),
            Timestamp(sequence),
        )))
        .unwrap();
        let claim =
            match block_on(store.acquire_execution(id, WorkerId::new(), MutationId::new(), 1_000))
                .unwrap()
            {
                ClaimOutcome::Claimed(claim) => claim,
                _ => panic!("new execution must be claimable"),
            };
        let name = StepName::new(format!("step-{sequence}")).unwrap();
        let first = block_on(store.begin_step(BeginStep {
            lease: LeaseGuard {
                execution_id: id,
                worker: claim.worker,
                token: claim.token,
                generation: claim.generation,
                expected_revision: claim.record.revision,
            },
            name: name.clone(),
            retry_policy: crate::RetryPolicyRecord {
                max_attempts: 2,
                backoff: crate::BackoffRecord::None,
                retryable: true,
            },
            mutation_id: MutationId::new(),
            at: Timestamp(sequence),
        }))
        .unwrap();
        let current = block_on(store.load_execution(id)).unwrap().unwrap();
        block_on(store.fail_step(FailStep {
            lease: LeaseGuard {
                execution_id: id,
                worker: claim.worker,
                token: claim.token,
                generation: claim.generation,
                expected_revision: current.revision,
            },
            name: name.clone(),
            expected_attempt: first.attempt,
            mutation_id: MutationId::new(),
            failure: FailureRecord {
                class: FailureClass::Retryable,
                diagnostic: None,
                occurred_at: Timestamp(sequence),
            },
            retry_at: Some(Timestamp(0)),
            retry_policy: crate::RetryPolicyRecord {
                max_attempts: 2,
                backoff: crate::BackoffRecord::None,
                retryable: true,
            },
        }))
        .unwrap();
        let current = block_on(store.load_execution(id)).unwrap().unwrap();
        let retry = block_on(store.begin_step(BeginStep {
            lease: LeaseGuard {
                execution_id: id,
                worker: claim.worker,
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
            at: Timestamp(sequence),
        }))
        .unwrap();
        assert_eq!(retry.id, first.id);
        assert_eq!(retry.attempt, first.attempt + 1);
        let current = block_on(store.load_execution(id)).unwrap().unwrap();
        block_on(store.complete_step(CompleteStep {
            lease: LeaseGuard {
                execution_id: id,
                worker: claim.worker,
                token: claim.token,
                generation: claim.generation,
                expected_revision: current.revision,
            },
            name: name.clone(),
            expected_attempt: retry.attempt,
            mutation_id: MutationId::new(),
            result: None,
            at: Timestamp(sequence),
        }))
        .unwrap();
        let current = block_on(store.load_execution(id)).unwrap().unwrap();
        assert!(matches!(
            block_on(store.begin_step(BeginStep {
                lease: LeaseGuard {
                    execution_id: id,
                    worker: claim.worker,
                    token: claim.token,
                    generation: claim.generation,
                    expected_revision: current.revision,
                },
                name,
                retry_policy: crate::RetryPolicyRecord {
                    max_attempts: 2,
                    backoff: crate::BackoffRecord::None,
                    retryable: true,
                },
                mutation_id: MutationId::new(),
                at: Timestamp(sequence),
            })),
            Err(StoreError::Conflict(_))
        ));
    }
}

#[test]
fn clock_advances_deterministically_and_faults_straddle_commit() {
    let store = MemoryStore::new(Timestamp(50));
    assert_eq!(store.clock().now(), Timestamp(50));
    store.clock().advance(25);
    assert_eq!(store.clock().now(), Timestamp(75));

    let new = NewExecution::new(
        ExecutionId::new(),
        OperationKey::new("fault-test", 1).unwrap(),
        store.clock().now(),
    );
    store
        .faults()
        .inject_once(FaultPoint::CreateExecution, FaultMode::BeforeCommit);
    assert!(matches!(
        block_on(store.create_execution(new.clone())),
        Err(StoreError::Unavailable(_))
    ));
    assert!(block_on(store.load_execution(new.id)).unwrap().is_none());

    store
        .faults()
        .inject_once(FaultPoint::CreateExecution, FaultMode::AfterCommit);
    assert!(matches!(
        block_on(store.create_execution(new.clone())),
        Err(StoreError::Unavailable(_))
    ));
    assert!(block_on(store.load_execution(new.id)).unwrap().is_some());
    assert!(block_on(store.create_execution(new)).is_ok());
}

#[test]
fn purge_rejects_resumable_work_and_removes_cancelled_history() {
    let store = MemoryStore::new(Timestamp(0));
    let id = ExecutionId::new();
    block_on(store.create_execution(NewExecution::new(
        id,
        OperationKey::new("purge-safety", 1).unwrap(),
        Timestamp(0),
    )))
    .unwrap();
    assert!(matches!(
        block_on(store.purge_execution(id)),
        Err(StoreError::Conflict(_))
    ));
    block_on(store.request_cancellation(RequestCancellation {
        execution_id: id,
        at: Timestamp(1),
    }))
    .unwrap();
    assert!(block_on(store.purge_execution(id)).unwrap());
    assert!(block_on(store.load_execution(id)).unwrap().is_none());
    assert!(!block_on(store.purge_execution(id)).unwrap());
}

#[test]
fn purge_removes_completion_receipts_with_sensitive_results() {
    let store = MemoryStore::new(Timestamp(0));
    let id = ExecutionId::new();
    block_on(store.create_execution(NewExecution::new(
        id,
        OperationKey::new("purge-result", 1).unwrap(),
        Timestamp(0),
    )))
    .unwrap();
    let claim =
        match block_on(store.acquire_execution(id, WorkerId::new(), MutationId::new(), 10_000))
            .unwrap()
        {
            ClaimOutcome::Claimed(claim) => claim,
            _ => panic!("execution must be claimable"),
        };
    let secret = crate::StoredValue::new(
        crate::limits::DEFAULT_CODEC_ID,
        crate::limits::DEFAULT_SCHEMA_VERSION,
        b"secret-result".to_vec(),
    )
    .unwrap();
    let request = CompleteExecution {
        lease: LeaseGuard {
            execution_id: id,
            worker: claim.worker,
            token: claim.token,
            generation: claim.generation,
            expected_revision: claim.record.revision,
        },
        mutation_id: MutationId::new(),
        result: Some(secret),
        at: Timestamp(1),
    };
    block_on(store.complete_execution(request.clone())).unwrap();
    assert!(block_on(store.purge_execution(id)).unwrap());
    assert!(matches!(
        block_on(store.complete_execution(request)),
        Err(StoreError::Conflict(_))
    ));
}

#[test]
fn reference_store_samples_authoritative_time_inside_its_state_lock() {
    use std::sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Clone)]
    struct ProbedClock {
        now: Arc<Mutex<Timestamp>>,
        state_lock_probe: Arc<OnceLock<Arc<dyn Fn() -> bool + Send + Sync>>>,
        samples_outside_state_lock: Arc<AtomicUsize>,
    }

    impl crate::reference::Clock for ProbedClock {
        fn now(&self) -> Timestamp {
            if !self
                .state_lock_probe
                .get()
                .expect("state lock probe is installed")()
            {
                self.samples_outside_state_lock
                    .fetch_add(1, Ordering::SeqCst);
            }
            *self.now.lock().unwrap()
        }
    }

    let clock = ProbedClock {
        now: Arc::new(Mutex::new(Timestamp(0))),
        state_lock_probe: Arc::new(OnceLock::new()),
        samples_outside_state_lock: Arc::new(AtomicUsize::new(0)),
    };
    let store = MemoryStore::with_clock(clock.clone());
    let state = Arc::downgrade(&store.state);
    let probe: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(move || {
        state.upgrade().is_some_and(|state| {
            matches!(state.try_lock(), Err(std::sync::TryLockError::WouldBlock))
        })
    });
    assert!(clock.state_lock_probe.set(probe).is_ok());

    let id = ExecutionId::new();
    block_on(store.create_execution(NewExecution::new(
        id,
        OperationKey::new("clock-lock-order", 1).unwrap(),
        Timestamp(0),
    )))
    .unwrap();
    let claim = match block_on(store.acquire_execution(id, WorkerId::new(), MutationId::new(), 10))
        .unwrap()
    {
        ClaimOutcome::Claimed(claim) => claim,
        _ => panic!("execution must be claimable"),
    };
    assert_eq!(clock.samples_outside_state_lock.load(Ordering::SeqCst), 0);
    assert_eq!(claim.record.lease.unwrap().expires_at, Timestamp(10));
}
