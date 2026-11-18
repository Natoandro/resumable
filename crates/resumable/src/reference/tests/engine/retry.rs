use super::*;

#[test]
fn retry_deadline_and_policy_survive_resume() {
    use crate::{Backoff, RetryPolicy};
    let started_at = Timestamp(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
    );
    let store = MemoryStore::new(started_at);
    store
        .faults()
        .inject_once(FaultPoint::FailStep, FaultMode::AfterCommit);
    let engine = engine(store.clone());
    let id = ExecutionId::new();
    let op = operation();
    let options = StepOptions {
        retryable: true,
        retry_policy: RetryPolicy {
            max_attempts: 2,
            backoff: Backoff::Fixed(std::time::Duration::from_millis(500)),
        },
    };
    let result = block_on(engine.start_with_id_and_context(
        id,
        op.clone(),
        (),
        move |_, context| async move {
            context
                .step_with("retry", options, |_| async { Err::<(), _>("temporary") })
                .await
                .map_err(|error| error.to_string())
        },
    ));
    assert!(result.is_err());
    let failed = block_on(store.load_step(id, &StepName::new("retry").unwrap()))
        .unwrap()
        .unwrap();
    assert_eq!(failed.attempt, 1);
    assert_eq!(
        failed.next_attempt_at.unwrap().0 - failed.failure.as_ref().unwrap().occurred_at.0,
        500
    );
    assert_eq!(failed.retry_policy.as_ref().unwrap().max_attempts, 2);
    assert_eq!(
        failed.failure.as_ref().unwrap().class,
        FailureClass::Retryable
    );

    store.clock().advance(1_000);
    let changed_policy = StepOptions::default();
    let resumed = block_on(engine.resume_with_context::<(), (), String, _, _>(
        id,
        &op,
        move |_, context| async move {
            context
                .step_with("retry", changed_policy, |_| async { Ok::<(), String>(()) })
                .await
                .map_err(|error| error.to_string())
        },
    ));
    assert!(resumed.is_err());
    let after_resume = block_on(store.load_step(id, &StepName::new("retry").unwrap()))
        .unwrap()
        .unwrap();
    assert_eq!(after_resume.attempt, 1);
}

#[test]
fn retry_policy_is_fixed_before_the_first_body_failure() {
    use crate::{Backoff, RetryPolicy};
    let store = MemoryStore::new(Timestamp(0));
    let engine = engine(store.clone());
    let id = ExecutionId::new();
    let op = operation();
    let original_policy = StepOptions {
        retryable: true,
        retry_policy: RetryPolicy {
            max_attempts: 3,
            backoff: Backoff::Fixed(std::time::Duration::from_secs(1)),
        },
    };
    let interrupted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        block_on(engine.start_with_id_and_context(
            id,
            op.clone(),
            (),
            move |_, context| async move {
                context
                    .step_with("policy", original_policy, |_| async {
                        panic!("interrupt before the first failure");
                        #[allow(unreachable_code)]
                        Ok::<(), String>(())
                    })
                    .await
                    .map_err(|error| error.to_string())
            },
        ))
    }));
    assert!(interrupted.is_err());
    store.clock().advance(2);

    let changed_policy = StepOptions::default();
    let body_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let result =
        block_on(
            engine.resume_with_context::<(), (), String, _, _>(id, &op, |_, context| {
                let changed_policy = changed_policy.clone();
                let body_calls = body_calls.clone();
                async move {
                    context
                        .step_with("policy", changed_policy, move |_| {
                            let body_calls = body_calls.clone();
                            async move {
                                body_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                Ok::<(), String>(())
                            }
                        })
                        .await
                        .map_err(|error| error.to_string())
                }
            }),
        );
    assert!(result.is_err());
    assert_eq!(body_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    let step = block_on(store.load_step(id, &StepName::new("policy").unwrap()))
        .unwrap()
        .unwrap();
    assert_eq!(step.attempt, 1);
    assert_eq!(step.retry_policy.unwrap().max_attempts, 3);
}

#[test]
fn a_caught_permanent_failure_cannot_restart_in_the_same_operation() {
    let store = MemoryStore::new(Timestamp(0));
    let engine = engine(store.clone());
    let id = ExecutionId::new();
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let body_calls = calls.clone();
    let result = block_on(engine.start_with_id_and_context(
        id,
        operation(),
        (),
        move |_, context| async move {
            let _ = context
                .step::<(), _, _, _>("permanent", move |_| {
                    let calls = body_calls.clone();
                    async move {
                        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Err("permanent")
                    }
                })
                .await;
            let _ = context
                .step::<(), _, _, _>("permanent", |_| async { Ok::<(), &str>(()) })
                .await;
            Ok::<(), String>(())
        },
    ));
    assert!(result.is_err());
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        block_on(store.load_step(id, &StepName::new("permanent").unwrap()))
            .unwrap()
            .unwrap()
            .attempt,
        1
    );
}

#[test]
fn retry_eligibility_uses_the_store_clock_after_resume() {
    use crate::{Backoff, RetryPolicy};
    let wall_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let store = MemoryStore::new(Timestamp(wall_time));
    let engine = engine(store.clone());
    let id = ExecutionId::new();
    let op = operation();
    let options = StepOptions {
        retryable: true,
        retry_policy: RetryPolicy {
            max_attempts: 2,
            backoff: Backoff::Fixed(std::time::Duration::from_secs(60)),
        },
    };
    let failed = block_on(engine.start_with_id_and_context(
        id,
        op.clone(),
        (),
        move |_, context| async move {
            context
                .step_with("store-clock", options, |_| async {
                    Err::<(), _>("temporary")
                })
                .await
                .map_err(|error| error.to_string())
        },
    ));
    assert!(failed.is_err());
    store.clock().advance(61_000);
    let resumed = block_on(engine.resume_with_context::<(), (), String, _, _>(
        id,
        &op,
        |_, context| async move {
            context
                .step_with(
                    "store-clock",
                    StepOptions {
                        retryable: true,
                        retry_policy: RetryPolicy {
                            max_attempts: 2,
                            backoff: Backoff::Fixed(std::time::Duration::from_secs(60)),
                        },
                    },
                    |step| async move {
                        assert_eq!(step.attempt(), 2);
                        Ok::<_, String>(())
                    },
                )
                .await
                .map_err(|error| error.to_string())
        },
    ));
    assert!(resumed.is_ok());
}
