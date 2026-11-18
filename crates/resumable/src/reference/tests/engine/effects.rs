use super::*;

#[test]
fn external_success_is_reconciled_by_stable_step_key_after_interruption() {
    let store = MemoryStore::new(Timestamp(0));
    let engine = engine(store.clone());
    let id = ExecutionId::new();
    let op = operation();
    let effects = Arc::new(Mutex::new(std::collections::HashSet::new()));
    let effects_first = effects.clone();
    let first = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        block_on(engine.start_with_id_and_context(
            id,
            op.clone(),
            (),
            move |_, context| async move {
                context
                    .step::<String, String, _, _>("external", move |step| {
                        let effects = effects_first.clone();
                        async move {
                            effects.lock().unwrap().insert(step.idempotency_key());
                            panic!("interrupted after external success");
                            #[allow(unreachable_code)]
                            Ok(String::new())
                        }
                    })
                    .await
                    .map_err(|error| error.to_string())
            },
        ))
    }));
    assert!(first.is_err());
    assert_eq!(
        block_on(store.load_execution(id)).unwrap().unwrap().status,
        ExecutionStatus::Running
    );
    store.clock().advance(2);
    let effects_resume = effects.clone();
    block_on(engine.resume_with_context::<(), String, String, _, _>(
        id,
        &op,
        move |_, context| async move {
            context
                .step_with("external", StepOptions::default(), move |step| {
                    let effects = effects_resume.clone();
                    async move {
                        effects.lock().unwrap().insert(step.idempotency_key());
                        Ok::<_, String>(step.idempotency_key().to_string())
                    }
                })
                .await
                .map_err(|error| error.to_string())
        },
    ))
    .unwrap();
    assert_eq!(effects.lock().unwrap().len(), 1);
}

#[test]
fn persisted_completion_is_reused_after_lost_write_response() {
    let store = MemoryStore::new(Timestamp(0));
    store
        .faults()
        .inject_once(FaultPoint::CompleteStep, FaultMode::AfterCommit);
    let engine = engine(store.clone());
    let body_calls = Arc::new(Mutex::new(0));
    let calls = body_calls.clone();
    let (_, result) =
        block_on(
            engine.start_with_context(operation(), (), move |_, context| async move {
                context
                    .step("completed", move |_| {
                        let calls = calls.clone();
                        async move {
                            *calls.lock().unwrap() += 1;
                            Ok::<_, String>("saved".to_owned())
                        }
                    })
                    .await
                    .map_err(|error| error.to_string())
            }),
        )
        .unwrap();
    assert_eq!(result, "saved");
    assert_eq!(*body_calls.lock().unwrap(), 1);
}
