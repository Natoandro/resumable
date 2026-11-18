use super::*;

#[test]
fn pending_cancellation_is_durable_and_restart_does_not_run_the_body() {
    let store = MemoryStore::new(Timestamp(0));
    let runner = engine(store.clone());
    let id = ExecutionId::new();
    block_on(
        store.create_execution(
            crate::NewExecution::new(id, operation(), Timestamp(0))
                .with_input(crate::JsonCodec.encode(&()).unwrap()),
        ),
    )
    .unwrap();
    assert_eq!(
        block_on(runner.cancel(id)).unwrap().status,
        ExecutionStatus::Cancelled
    );
    let calls = Arc::new(Mutex::new(0));
    let called = calls.clone();
    let result =
        block_on(
            runner.resume::<(), (), String, _, _>(id, &operation(), move |_| async move {
                *called.lock().unwrap() += 1;
                Ok(())
            }),
        );
    assert!(matches!(result, Err(crate::RunError::Cancelled)));
    assert_eq!(*calls.lock().unwrap(), 0);
}

#[test]
fn active_step_observes_durable_cancellation_and_finish_race_cancels() {
    let store = MemoryStore::new(Timestamp(0));
    let runner = engine(store.clone());
    let id = ExecutionId::new();
    let operation = operation();
    let in_step_store = store.clone();
    let result = block_on(runner.start_with_id_and_context(
        id,
        operation,
        (),
        move |_, context| async move {
            context
                .step("work", move |step| {
                    let store = in_step_store.clone();
                    async move {
                        store
                            .request_cancellation(crate::RequestCancellation {
                                execution_id: id,
                                at: Timestamp(1),
                            })
                            .await
                            .unwrap();
                        assert!(step.is_cancelled().await.unwrap());
                        Ok::<_, String>(())
                    }
                })
                .await
                .map_err(|error| error.to_string())?;
            Ok::<_, String>(())
        },
    ));
    assert!(matches!(result, Err(crate::RunError::Cancelled)));
    let record = block_on(store.load_execution(id)).unwrap().unwrap();
    assert_eq!(record.status, ExecutionStatus::Cancelled);
    assert_eq!(
        block_on(store.load_step(id, &StepName::new("work").unwrap()))
            .unwrap()
            .unwrap()
            .status,
        StepStatus::Completed
    );
}
