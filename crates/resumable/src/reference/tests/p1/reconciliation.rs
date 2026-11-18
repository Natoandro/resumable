use super::takeover_store::{InterruptedBeginStore, Interruption};
use super::*;

#[test]
fn ambiguous_begin_never_invokes_a_body_after_takeover_or_expiry() {
    for interruption in [Interruption::Takeover, Interruption::Expiry] {
        let store = MemoryStore::new(Timestamp(0));
        let engine = Engine::new(InterruptedBeginStore::new(store.clone(), interruption));
        let calls = AtomicU32::new(0);
        let id = ExecutionId::new();
        let result =
            block_on(
                engine.start_with_id_and_context(id, operation(), (), |_, context| {
                    let calls = &calls;
                    async move {
                        context
                            .step("effect", |_| async {
                                calls.fetch_add(1, Ordering::SeqCst);
                                Ok::<_, String>(())
                            })
                            .await
                            .map_err(|error| error.to_string())
                    }
                }),
            );
        assert!(matches!(
            result,
            Err(RunError::Store(StoreError::Conflict(_)))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let execution = block_on(store.load_execution(id)).unwrap().unwrap();
        assert_eq!(execution.status, ExecutionStatus::Running);
        let step = block_on(store.load_step(id, &StepName::new("effect").unwrap()))
            .unwrap()
            .unwrap();
        assert_eq!(step.status, StepStatus::Running);
        assert!(step.result.is_none());
    }
}

#[test]
fn ambiguous_begin_under_current_ownership_invokes_the_original_attempt_once() {
    let store = MemoryStore::new(Timestamp(0));
    let engine = Engine::new(InterruptedBeginStore::new(
        store.clone(),
        Interruption::ResponseLoss,
    ));
    let calls = AtomicU32::new(0);
    let (id, ()) = block_on(engine.start_with_context(operation(), (), |_, context| {
        let calls = &calls;
        async move {
            context
                .step("effect", |step| async move {
                    assert_eq!(step.attempt(), 1);
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, String>(())
                })
                .await
                .map_err(|error| error.to_string())
        }
    }))
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        block_on(store.load_step(id, &StepName::new("effect").unwrap()))
            .unwrap()
            .unwrap()
            .attempt,
        1
    );
}
