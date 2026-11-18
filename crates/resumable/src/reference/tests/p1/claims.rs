use super::*;

#[test]
fn overlapping_runs_with_the_same_worker_do_not_invoke_a_second_body() {
    let store = MemoryStore::new(Timestamp(0));
    let config = EngineConfig::default();
    let runner = Engine::with_config(store.clone(), JsonCodec, config.clone());
    let other_runner = Engine::with_config(store.clone(), JsonCodec, config);
    let effects = AtomicU32::new(0);
    let id = ExecutionId::new();
    let mut first =
        Box::pin(
            runner.start_with_id_and_context(id, operation(), (), |_, context| {
                let effects = &effects;
                async move {
                    context
                        .step("effect", |_| async {
                            effects.fetch_add(1, Ordering::SeqCst);
                            std::future::pending::<Result<(), String>>().await
                        })
                        .await
                        .map_err(|error| error.to_string())
                }
            }),
        );
    assert!(poll_once(first.as_mut()).is_pending());
    let initial_step = block_on(store.load_step(id, &StepName::new("effect").unwrap()))
        .unwrap()
        .unwrap();
    for engine in [&runner, &other_runner] {
        let result = block_on(engine.resume_with_context::<(), (), String, _, _>(
            id,
            &operation(),
            |_, context| {
                let effects = &effects;
                async move {
                    context
                        .step("effect", |_| async {
                            effects.fetch_add(1, Ordering::SeqCst);
                            Ok::<_, String>(())
                        })
                        .await
                        .map_err(|error| error.to_string())
                }
            },
        ));
        assert!(matches!(result, Err(RunError::Busy)));
    }
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(
        block_on(store.load_step(id, &initial_step.name))
            .unwrap()
            .unwrap(),
        initial_step
    );
    drop(first);
    store.clock().advance(30_001);
    block_on(runner.resume_with_context::<(), (), String, _, _>(
        id,
        &operation(),
        |_, context| async move {
            context
                .step("effect", |_| async { Ok::<_, String>(()) })
                .await
                .map_err(|error| error.to_string())
        },
    ))
    .unwrap();
    let recovered = block_on(store.load_step(id, &initial_step.name))
        .unwrap()
        .unwrap();
    assert_eq!(recovered.id, initial_step.id);
    assert_eq!(recovered.attempt, 2);
}

#[test]
fn engine_retries_an_ambiguous_claim_without_creating_another_generation() {
    let store = MemoryStore::new(Timestamp(0));
    store
        .faults()
        .inject_once(FaultPoint::AcquireExecution, FaultMode::AfterCommit);
    let engine = Engine::new(store.clone());
    let effects = AtomicU32::new(0);
    let (id, ()) = block_on(engine.start(operation(), (), |_| async {
        effects.fetch_add(1, Ordering::SeqCst);
        Ok::<_, String>(())
    }))
    .unwrap();
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(
        block_on(store.load_execution(id))
            .unwrap()
            .unwrap()
            .fencing_generation,
        1
    );
}
