use super::*;

#[test]
fn heartbeat_renews_lease_and_advances_the_guard_revision() {
    let store = MemoryStore::new(Timestamp(0));
    let engine = Engine::with_config(
        store.clone(),
        crate::JsonCodec,
        EngineConfig {
            lease_duration: std::time::Duration::from_millis(100),
            heartbeat_interval: std::time::Duration::from_millis(25),
            ..EngineConfig::default()
        },
    );
    let clock_store = store.clone();
    block_on(
        engine.start_with_context(operation(), (), |_, context| async move {
            assert_eq!(
                context.heartbeat_interval(),
                std::time::Duration::from_millis(25)
            );
            clock_store.clock().advance(90);
            context
                .heartbeat()
                .await
                .map_err(|error| error.to_string())?;
            clock_store.clock().advance(90);
            context
                .step("after-heartbeat", |_| async { Ok::<_, String>(()) })
                .await
                .map_err(|error| error.to_string())?;
            Ok::<_, String>(())
        }),
    )
    .unwrap();
}

#[test]
fn failed_heartbeat_prevents_completion_and_leaves_work_recoverable() {
    let store = MemoryStore::new(Timestamp(0));
    store
        .faults()
        .inject_once(FaultPoint::Heartbeat, FaultMode::BeforeCommit);
    let runner = engine(store.clone());
    let id = ExecutionId::new();
    let op = operation();
    let result =
        block_on(
            runner.start_with_id_and_context(id, op.clone(), (), |_, context| async move {
                assert!(context.heartbeat().await.is_err());
                Ok::<_, String>(())
            }),
        );
    assert!(result.is_err());
    assert_eq!(
        block_on(store.load_execution(id)).unwrap().unwrap().status,
        ExecutionStatus::Running
    );
    store.clock().advance(2);
    let resumed = engine(store.clone());
    block_on(
        resumed.resume_with_context::<(), (), String, _, _>(id, &op, |_, _| async {
            Ok::<_, String>(())
        }),
    )
    .unwrap();
}

#[test]
fn expired_lease_is_reported_as_lost_before_later_mutations() {
    let store = MemoryStore::new(Timestamp(0));
    let runner = engine(store.clone());
    let id = ExecutionId::new();
    let clock_store = store.clone();
    let result = block_on(runner.start_with_id_and_context(
        id,
        operation(),
        (),
        move |_, context| async move {
            clock_store.clock().advance(2);
            assert!(context.heartbeat().await.is_err());
            context
                .step("after-expiry", |_| async { Ok::<_, String>(()) })
                .await
                .map_err(|error| error.to_string())?;
            Ok::<_, String>(())
        },
    ));
    assert!(result.is_err());
    assert_eq!(
        block_on(store.load_execution(id)).unwrap().unwrap().status,
        ExecutionStatus::Running
    );
}
