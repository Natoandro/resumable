use super::*;

#[test]
fn best_effort_progress_failure_does_not_prevent_completion() {
    let store = MemoryStore::new(Timestamp(0));
    let runner = engine(store.clone());
    let id = crate::ExecutionId::new();
    let progress_store = store.clone();
    block_on(
        runner.start_with_id_and_context(id, operation(), (), |_, context| async move {
            context
                .step("progressing", |step| {
                    let store = progress_store.clone();
                    async move {
                        store
                            .faults()
                            .inject_once(FaultPoint::Heartbeat, FaultMode::AfterCommit);
                        step.progress(crate::Progress {
                            current: Some(1),
                            total: Some(1),
                            message: Some("done".into()),
                        })
                        .await;
                        Ok::<_, String>(())
                    }
                })
                .await
                .map_err(|error| error.to_string())?;
            Ok::<_, String>(())
        }),
    )
    .unwrap();
    assert_eq!(
        block_on(store.load_execution(id)).unwrap().unwrap().status,
        crate::ExecutionStatus::Completed
    );
}

#[test]
fn unsupported_operation_version_is_rejected_before_operation_code() {
    let store = MemoryStore::new(Timestamp(0));
    let runner = engine(store);
    let id = ExecutionId::new();
    let operation = operation();
    block_on(runner.start_with_id(id, operation.clone(), (), |_| async { Ok::<_, String>(()) }))
        .unwrap();
    let calls = Arc::new(Mutex::new(0));
    let body_calls = calls.clone();
    let requested =
        OperationKey::new(operation.name().to_owned(), operation.version() + 1).unwrap();
    let result =
        block_on(
            runner.resume::<(), (), String, _, _>(id, &requested, move |_| async move {
                *body_calls.lock().unwrap() += 1;
                Ok(())
            }),
        );
    assert!(matches!(
        result,
        Err(crate::RunError::UnsupportedOperationVersion { .. })
    ));
    assert_eq!(*calls.lock().unwrap(), 0);
}
