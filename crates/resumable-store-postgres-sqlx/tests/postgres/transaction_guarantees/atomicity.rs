use super::support::{claimed, guard};
use super::support::{
    domain_write_count, insert_domain_write, new_execution, reset_clock, reset_domain_write,
    set_clock,
};
use resumable::{BeginStep, ExecutionId, MutationId, StepName, Store, Timestamp, WorkerId};
use resumable_store_postgres_sqlx::PostgresStore;
use sqlx::PgPool;

pub(super) async fn verify_creation_atomicity(pool: &PgPool) {
    let store = PostgresStore::new(pool.clone());
    let committed_id = ExecutionId::new();
    let mut transaction = pool.begin().await.unwrap();
    store
        .create_execution_in_transaction(&mut transaction, new_execution(committed_id))
        .await
        .unwrap();
    insert_domain_write(&mut transaction, committed_id).await;
    transaction.commit().await.unwrap();
    assert!(store.load_execution(committed_id).await.unwrap().is_some());
    assert_eq!(domain_write_count(pool, committed_id).await, 1);

    let rolled_back_id = ExecutionId::new();
    let mut transaction = pool.begin().await.unwrap();
    store
        .create_execution_in_transaction(&mut transaction, new_execution(rolled_back_id))
        .await
        .unwrap();
    insert_domain_write(&mut transaction, rolled_back_id).await;
    transaction.rollback().await.unwrap();
    assert!(
        store
            .load_execution(rolled_back_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(domain_write_count(pool, rolled_back_id).await, 0);
    reset_domain_write(pool, committed_id).await;
}

pub(super) async fn verify_lease_loss_rolls_back_domain_write(pool: &PgPool) {
    set_clock(pool, "2026-01-01 00:00:00+00").await;
    let store = PostgresStore::new(pool.clone());
    let id = ExecutionId::new();
    store.create_execution(new_execution(id)).await.unwrap();
    let claim = claimed(&store, id).await;
    let name = StepName::new("lease-loss-atomic-write").unwrap();
    let step = store
        .begin_step(BeginStep {
            lease: guard(id, &claim),
            name: name.clone(),
            retry_policy: Default::default(),
            mutation_id: MutationId::new(),
            at: Timestamp(10),
        })
        .await
        .unwrap();
    let current = store.load_execution(id).await.unwrap().unwrap();
    let request = resumable::CompleteStep {
        lease: resumable::LeaseGuard {
            expected_revision: current.revision,
            ..guard(id, &claim)
        },
        name,
        expected_attempt: step.attempt,
        mutation_id: MutationId::new(),
        result: None,
        at: Timestamp(11),
    };
    let mut transaction = pool.begin().await.unwrap();
    insert_domain_write(&mut transaction, id).await;
    set_clock(pool, "2026-01-01 00:01:00+00").await;
    let result = store
        .complete_step_in_transaction(&mut transaction, request)
        .await;
    assert!(matches!(result, Err(resumable::StoreError::Conflict(_))));
    transaction.rollback().await.unwrap();
    assert_eq!(domain_write_count(pool, id).await, 0);
    assert!(matches!(
        store
            .acquire_execution(id, WorkerId::new(), MutationId::new(), 60_000)
            .await
            .unwrap(),
        resumable::ClaimOutcome::Claimed(_)
    ));
    reset_clock(pool).await;
}

pub(super) async fn verify_completion_lock_blocks_takeover(pool: &PgPool) {
    set_clock(pool, "2026-01-01 00:00:00+00").await;
    let store = PostgresStore::new(pool.clone());
    let id = ExecutionId::new();
    store.create_execution(new_execution(id)).await.unwrap();
    let claim = claimed(&store, id).await;
    let name = StepName::new("lock-through-domain-commit").unwrap();
    let step = store
        .begin_step(BeginStep {
            lease: guard(id, &claim),
            name: name.clone(),
            retry_policy: Default::default(),
            mutation_id: MutationId::new(),
            at: Timestamp(15),
        })
        .await
        .unwrap();
    let current = store.load_execution(id).await.unwrap().unwrap();
    let mut transaction = pool.begin().await.unwrap();
    insert_domain_write(&mut transaction, id).await;
    store
        .complete_step_in_transaction(
            &mut transaction,
            resumable::CompleteStep {
                lease: resumable::LeaseGuard {
                    expected_revision: current.revision,
                    ..guard(id, &claim)
                },
                name,
                expected_attempt: step.attempt,
                mutation_id: MutationId::new(),
                result: None,
                at: Timestamp(16),
            },
        )
        .await
        .unwrap();
    let lock_attempt = sqlx::query_scalar::<_, uuid::Uuid>(
        "SELECT id FROM resumable_executions WHERE id = $1 FOR UPDATE NOWAIT",
    )
    .bind(*id.as_uuid())
    .fetch_one(pool)
    .await;
    assert_eq!(
        lock_attempt
            .unwrap_err()
            .as_database_error()
            .unwrap()
            .code()
            .as_deref(),
        Some("55P03")
    );
    transaction.commit().await.unwrap();
    set_clock(pool, "2026-01-01 00:01:00+00").await;
    assert!(matches!(
        store
            .acquire_execution(id, WorkerId::new(), MutationId::new(), 60_000)
            .await
            .unwrap(),
        resumable::ClaimOutcome::Claimed(_)
    ));
    assert_eq!(domain_write_count(pool, id).await, 1);
    reset_domain_write(pool, id).await;
    reset_clock(pool).await;
}
