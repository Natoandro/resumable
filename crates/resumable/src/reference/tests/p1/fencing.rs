use super::*;

#[test]
fn failure_resume_retains_generation_after_takeover_and_rejects_old_guards() {
    let store = MemoryStore::new(Timestamp(0));
    let id = ExecutionId::new();
    create(&store, id);
    let first = claim(&store, id, 10);
    store.clock().advance(10);
    let second = claim(&store, id, 100);
    assert_eq!(second.generation, first.generation + 1);
    let failed = block_on(store.fail_execution(FailExecution {
        lease: second.clone(),
        mutation_id: MutationId::new(),
        failure: FailureRecord {
            class: FailureClass::Permanent,
            diagnostic: None,
            occurred_at: Timestamp(11),
        },
    }))
    .unwrap();
    assert!(failed.lease.is_none());
    assert_eq!(failed.fencing_generation, second.generation);
    let third = claim(&store, id, 100);
    assert_eq!(third.generation, second.generation + 1);
    for stale in [first, second] {
        assert!(matches!(
            block_on(store.complete_execution(CompleteExecution {
                lease: stale,
                mutation_id: MutationId::new(),
                result: None,
                at: Timestamp(12),
            })),
            Err(StoreError::Conflict(_))
        ));
    }
    assert_eq!(
        block_on(store.load_execution(id))
            .unwrap()
            .unwrap()
            .fencing_generation,
        third.generation
    );
}

#[test]
fn engine_failure_resume_exposes_increasing_external_fencing_tokens() {
    let store = MemoryStore::new(Timestamp(0));
    let engine = Engine::with_config(
        store.clone(),
        JsonCodec,
        EngineConfig {
            failed_execution_policy: FailedExecutionPolicy::Resume,
            ..Default::default()
        },
    );
    let tokens = Mutex::new(Vec::new());
    let id = ExecutionId::new();
    let result = block_on(
        engine.start_with_id_and_context(id, operation(), (), |_, context| {
            let tokens = &tokens;
            async move {
                context
                    .step("first", |step| async move {
                        tokens.lock().unwrap().push(step.fencing_token());
                        Ok::<_, String>(())
                    })
                    .await
                    .map_err(|error| error.to_string())?;
                Err::<(), String>("failure outside a completed step".into())
            }
        }),
    );
    assert!(matches!(result, Err(RunError::Operation(_))));
    block_on(
        engine.resume_with_context::<(), (), String, _, _>(id, &operation(), |_, context| {
            let tokens = &tokens;
            async move {
                context
                    .step("second", |step| async move {
                        tokens.lock().unwrap().push(step.fencing_token());
                        Ok::<_, String>(())
                    })
                    .await
                    .map_err(|error| error.to_string())
            }
        }),
    )
    .unwrap();
    assert_eq!(*tokens.lock().unwrap(), vec![1, 2]);
}
