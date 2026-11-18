use super::*;

#[test]
fn step_body_runs_only_after_begin_is_persisted() {
    let store = MemoryStore::new(Timestamp(0));
    store
        .faults()
        .inject_once(FaultPoint::BeginStep, FaultMode::BeforeCommit);
    let engine = engine(store.clone());
    let body_calls = Arc::new(Mutex::new(0));
    let calls = body_calls.clone();
    let store_in_body = store.clone();
    block_on(
        engine.start_with_context(operation(), (), move |_, context| async move {
            let execution_id = context.execution_id();
            Ok::<_, String>(
                context
                    .step("prepare", move |_| {
                        let calls = calls.clone();
                        let store = store_in_body.clone();
                        async move {
                            *calls.lock().unwrap() += 1;
                            let step = store
                                .load_step(execution_id, &StepName::new("prepare").unwrap())
                                .await
                                .unwrap()
                                .unwrap();
                            assert_eq!(step.status, StepStatus::Running);
                            Ok::<_, String>(7_u32)
                        }
                    })
                    .await
                    .unwrap(),
            )
        }),
    )
    .unwrap();
    assert_eq!(*body_calls.lock().unwrap(), 1);
}

#[test]
fn interrupted_body_can_resume_with_the_same_step_identity() {
    let store = MemoryStore::new(Timestamp(0));
    let engine = engine(store.clone());
    let id = ExecutionId::new();
    let op = operation();
    let first = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        block_on(
            engine.start_with_id_and_context(id, op.clone(), (), |_, context| async move {
                context
                    .step::<u32, String, _, _>("during-body", |_| async {
                        panic!("simulated worker interruption");
                        #[allow(unreachable_code)]
                        Ok(0)
                    })
                    .await
                    .map_err(|error| error.to_string())
            }),
        )
    }));
    assert!(first.is_err());
    assert_eq!(
        block_on(store.load_execution(id)).unwrap().unwrap().status,
        ExecutionStatus::Running
    );
    store.clock().advance(2);
    let step_id = block_on(engine.resume_with_context::<(), StepId, String, _, _>(
        id,
        &op,
        |_, context| async move {
            context
                .step("during-body", |step| async move {
                    assert_eq!(step.attempt(), 2);
                    assert_eq!(step.fencing_token(), 2);
                    Ok::<_, String>(step.step_id())
                })
                .await
                .map_err(|error| error.to_string())
        },
    ))
    .unwrap();
    let record = block_on(store.load_step(id, &StepName::new("during-body").unwrap()))
        .unwrap()
        .unwrap();
    assert_eq!(record.id, step_id);
    assert_eq!(record.status, StepStatus::Completed);
}
