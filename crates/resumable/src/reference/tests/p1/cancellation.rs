use super::*;

#[test]
fn cancellation_after_worker_loss_becomes_terminal_even_with_incomplete_steps() {
    for failure_class in [
        None,
        Some(FailureClass::Permanent),
        Some(FailureClass::Retryable),
    ] {
        let store = MemoryStore::new(Timestamp(0));
        let id = ExecutionId::new();
        create(&store, id);
        let guard = claim(&store, id, 100);
        let name = StepName::new("interrupted").unwrap();
        let retryable = failure_class == Some(FailureClass::Retryable);
        let step = block_on(store.begin_step(BeginStep {
            lease: guard.clone(),
            name: name.clone(),
            retry_policy: RetryPolicyRecord {
                max_attempts: if retryable { 2 } else { 1 },
                retryable,
                ..Default::default()
            },
            mutation_id: MutationId::new(),
            at: Timestamp(1),
        }))
        .unwrap();
        if let Some(class) = failure_class {
            let current = block_on(store.load_execution(id)).unwrap().unwrap();
            block_on(store.fail_step(FailStep {
                lease: LeaseGuard {
                    expected_revision: current.revision,
                    ..guard
                },
                name,
                expected_attempt: step.attempt,
                mutation_id: MutationId::new(),
                failure: FailureRecord {
                    class,
                    diagnostic: None,
                    occurred_at: Timestamp(2),
                },
                retry_at: (class == FailureClass::Retryable).then_some(Timestamp(60_000)),
                retry_policy: RetryPolicyRecord {
                    max_attempts: if class == FailureClass::Retryable {
                        2
                    } else {
                        1
                    },
                    retryable: class == FailureClass::Retryable,
                    ..Default::default()
                },
            }))
            .unwrap();
        }
        let engine = Engine::new(store.clone());
        block_on(engine.cancel(id)).unwrap();
        let busy = block_on(
            engine.resume::<(), (), String, _, _>(id, &operation(), |_| async {
                panic!("cancellation cannot steal a live lease")
            }),
        );
        assert!(matches!(busy, Err(RunError::Busy)));
        store.clock().advance(101);
        store
            .faults()
            .inject_once(FaultPoint::CompleteExecution, FaultMode::AfterCommit);
        for _ in 0..2 {
            let result = block_on(engine.resume::<(), (), String, _, _>(
                id,
                &operation(),
                |_| async { panic!("cancelled recovery must not invoke operation code") },
            ));
            assert!(matches!(result, Err(RunError::Cancelled)));
            let record = block_on(store.load_execution(id)).unwrap().unwrap();
            assert_eq!(record.status, ExecutionStatus::Cancelled);
            assert!(record.lease.is_none());
            assert!(record.result.is_none());
        }
        assert!(block_on(store.purge_execution(id)).unwrap());
    }
}

#[test]
fn cancellation_during_body_finalizes_without_an_explicit_observation_call() {
    let store = MemoryStore::new(Timestamp(0));
    let engine = Engine::new(store.clone());
    let id = ExecutionId::new();
    let cancellation_store = &store;
    let result =
        block_on(
            engine.start_with_id_and_context(id, operation(), (), |_, context| async move {
                context
                    .step("effect", |_| async {
                        cancellation_store
                            .request_cancellation(RequestCancellation {
                                execution_id: id,
                                at: Timestamp(1),
                            })
                            .await
                            .unwrap();
                        Ok::<_, String>(())
                    })
                    .await
                    .map_err(|error| error.to_string())
            }),
        );
    // The step completion may conflict with cancellation's revision update. Recovery must settle it.
    assert!(result.is_err());
    store.clock().advance(30_001);
    let result = block_on(
        engine.resume::<(), (), String, _, _>(id, &operation(), |_| async {
            panic!("cancelled recovery must not replay an effect")
        }),
    );
    assert!(matches!(result, Err(RunError::Cancelled)));
    assert_eq!(
        block_on(store.load_execution(id)).unwrap().unwrap().status,
        ExecutionStatus::Cancelled
    );
}
