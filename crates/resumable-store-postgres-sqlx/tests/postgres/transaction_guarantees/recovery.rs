use super::support::{
    claimed, domain_write_count, guard, insert_domain_write, new_execution, reset_clock,
    reset_domain_write, set_clock,
};
use resumable::{
    BeginStep, Engine, EngineConfig, ExecutionId, JsonCodec, MutationId, OperationKey, StepName,
    Store, Timestamp, ValueCodec,
};
use resumable_store_postgres_sqlx::PostgresStore;
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

pub(super) async fn verify_completion_survives_restart(pool: PgPool) -> PgPool {
    set_clock(&pool, "2026-01-01 00:00:00+00").await;
    let store = PostgresStore::new(pool.clone());
    let id = ExecutionId::new();
    let operation = OperationKey::new("transactional-restart", 1).unwrap();
    let mut execution = new_execution(id);
    execution.operation = operation.clone();
    execution.input = Some(JsonCodec.encode(&"input".to_owned()).unwrap());
    store.create_execution(execution).await.unwrap();
    let claim = claimed(&store, id).await;
    let name = StepName::new("domain-and-checkpoint").unwrap();
    let step = store
        .begin_step(BeginStep {
            lease: guard(id, &claim),
            name: name.clone(),
            retry_policy: Default::default(),
            mutation_id: MutationId::new(),
            at: Timestamp(20),
        })
        .await
        .unwrap();
    let current = store.load_execution(id).await.unwrap().unwrap();
    let request = resumable::CompleteStep {
        lease: resumable::LeaseGuard {
            expected_revision: current.revision,
            ..guard(id, &claim)
        },
        name: name.clone(),
        expected_attempt: step.attempt,
        mutation_id: MutationId::new(),
        result: Some(JsonCodec.encode(&"saved-result".to_owned()).unwrap()),
        at: Timestamp(21),
    };
    let mut transaction = pool.begin().await.unwrap();
    insert_domain_write(&mut transaction, id).await;
    store
        .complete_step_in_transaction(&mut transaction, request)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    pool.close().await;

    let reopened = PgPoolOptions::new()
        .max_connections(4)
        .connect(&super::super::test_database_url())
        .await
        .unwrap();
    set_clock(&reopened, "2026-01-01 00:01:00+00").await;
    let resumed_store = PostgresStore::new(reopened.clone());
    let invoked = Arc::new(AtomicBool::new(false));
    let engine = Engine::with_config(
        resumed_store.clone(),
        JsonCodec,
        EngineConfig {
            lease_duration: Duration::from_secs(60),
            heartbeat_interval: Duration::from_secs(10),
            failed_execution_policy: resumable::FailedExecutionPolicy::Resume,
            ..EngineConfig::default()
        },
    );
    let callback_invoked = invoked.clone();
    let result = engine
        .resume_with_context::<String, String, String, _, _>(
            id,
            &operation,
            move |_, context| async move {
                context
                    .step(name.as_str().to_owned(), move |_| {
                        callback_invoked.store(true, Ordering::SeqCst);
                        async { Ok::<_, String>("unexpected".to_owned()) }
                    })
                    .await
                    .map_err(|error| error.to_string())
            },
        )
        .await
        .unwrap();
    assert_eq!(result, "saved-result");
    assert!(!invoked.load(Ordering::SeqCst));
    assert_eq!(domain_write_count(&reopened, id).await, 1);
    assert_eq!(
        resumed_store
            .load_execution(id)
            .await
            .unwrap()
            .unwrap()
            .status,
        resumable::ExecutionStatus::Completed
    );
    reset_clock(&reopened).await;
    reset_domain_write(&reopened, id).await;
    reopened
}

pub(super) async fn verify_callback_completion_observation(pool: &PgPool) {
    let store = PostgresStore::new(pool.clone());
    let engine = Engine::new(store.clone());
    let id = ExecutionId::new();
    let operation = OperationKey::new("callback-transactional-completion", 1).unwrap();
    let callback_store = store.clone();
    let callback_pool = pool.clone();
    let result = engine
        .start_with_id_and_context::<String, String, String, _, _>(
            id,
            operation,
            "input".to_owned(),
            move |_, context| async move {
                context
                    .step("atomic-callback-completion", move |step_context| {
                        let store = callback_store.clone();
                        let pool = callback_pool.clone();
                        async move {
                            let result = "callback-result".to_owned();
                            let completion =
                                step_context.completion_request(JsonCodec.encode(&result).unwrap());
                            let mut transaction = pool.begin().await.unwrap();
                            insert_domain_write(&mut transaction, id).await;
                            store
                                .complete_step_in_transaction(&mut transaction, completion)
                                .await
                                .map_err(|error| error.to_string())?;
                            transaction
                                .commit()
                                .await
                                .map_err(|error| error.to_string())?;
                            Ok::<_, String>(result)
                        }
                    })
                    .await
                    .map_err(|error| error.to_string())
            },
        )
        .await
        .unwrap();
    assert_eq!(result, "callback-result");
    assert_eq!(domain_write_count(pool, id).await, 1);
    assert_eq!(
        store.load_execution(id).await.unwrap().unwrap().status,
        resumable::ExecutionStatus::Completed
    );
    reset_domain_write(pool, id).await;
}
