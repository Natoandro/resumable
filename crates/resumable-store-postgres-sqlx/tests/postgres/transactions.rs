use super::support::{claimed, create, guard, retry_policy};
use resumable::{
    BeginStep, ClaimOutcome, CompleteStep, ExecutionId, FailExecution, FailStep, FailureClass,
    FailureRecord, LeaseGuard, MutationId, NewExecution, OperationKey, StepName, Store, StoreError,
    Timestamp, WorkerId,
};
use resumable_store_postgres_sqlx::PostgresStore;
use sqlx::PgPool;

pub(super) async fn verify_caller_owned_completion(store: &PostgresStore, pool: &PgPool) {
    let rolled_back_id = ExecutionId::new();
    let mut transaction = pool.begin().await.unwrap();
    store
        .create_execution_in_transaction(
            &mut transaction,
            NewExecution::new(
                rolled_back_id,
                OperationKey::new("caller-create-rollback", 1).unwrap(),
                Timestamp(39),
            ),
        )
        .await
        .unwrap();
    transaction.rollback().await.unwrap();
    assert!(
        store
            .load_execution(rolled_back_id)
            .await
            .unwrap()
            .is_none()
    );

    let id = create(store, "caller-transaction").await;
    let claim = claimed(store, id).await;
    let name = StepName::new("atomic-domain-write").unwrap();
    let step = store
        .begin_step(BeginStep {
            lease: guard(id, &claim),
            name: name.clone(),
            retry_policy: Default::default(),
            mutation_id: MutationId::new(),
            at: Timestamp(40),
        })
        .await
        .unwrap();
    let current = store.load_execution(id).await.unwrap().unwrap();
    let request = CompleteStep {
        lease: LeaseGuard {
            expected_revision: current.revision,
            ..guard(id, &claim)
        },
        name: name.clone(),
        expected_attempt: step.attempt,
        mutation_id: MutationId::new(),
        result: None,
        at: Timestamp(41),
    };
    let mut transaction = pool.begin().await.unwrap();
    store
        .complete_step_in_transaction(&mut transaction, request.clone())
        .await
        .unwrap();
    transaction.rollback().await.unwrap();
    assert_eq!(store.load_execution(id).await.unwrap().unwrap(), current);
    assert_eq!(store.load_step(id, &name).await.unwrap().unwrap(), step);

    let mut transaction = pool.begin().await.unwrap();
    store
        .complete_step_in_transaction(&mut transaction, request)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(
        store.load_step(id, &name).await.unwrap().unwrap().status,
        resumable::StepStatus::Completed
    );
}

pub(super) async fn verify_rollback(store: &PostgresStore, pool: &PgPool) {
    let id = create(store, "atomic-rollback").await;
    let claim = claimed(store, id).await;
    let name = StepName::new("rollback-step").unwrap();
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
    let before = store.load_step(id, &name).await.unwrap().unwrap();
    let mutation_id = MutationId::new();
    sqlx::raw_sql(
        "CREATE OR REPLACE FUNCTION resumable_test_fail_attempt() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected failure'; END $$;
		CREATE TRIGGER resumable_test_fail_attempt BEFORE UPDATE ON resumable_attempts
		FOR EACH ROW EXECUTE FUNCTION resumable_test_fail_attempt()",
    )
    .execute(pool)
    .await
    .unwrap();
    let result = store
        .complete_step(CompleteStep {
            lease: LeaseGuard {
                expected_revision: current.revision,
                ..guard(id, &claim)
            },
            name: name.clone(),
            expected_attempt: step.attempt,
            mutation_id,
            result: None,
            at: Timestamp(21),
        })
        .await;
    assert!(matches!(result, Err(StoreError::Unavailable(_))));
    sqlx::raw_sql(
        "DROP TRIGGER resumable_test_fail_attempt ON resumable_attempts;
		DROP FUNCTION resumable_test_fail_attempt()",
    )
    .execute(pool)
    .await
    .unwrap();
    assert_eq!(store.load_step(id, &name).await.unwrap().unwrap(), before);
    assert_eq!(store.load_execution(id).await.unwrap().unwrap(), current);
    assert!(store.load_progress(id, &name).await.unwrap().is_none());
    let receipts = sqlx::query_scalar!(
        "SELECT count(*)::bigint AS \"count!\"
		FROM resumable_mutation_receipts
		WHERE execution_id = $1 AND mutation_id = $2",
        *id.as_uuid(),
        *mutation_id.as_uuid(),
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(receipts, 0);
}

pub(super) async fn verify_retry_clock(store: &PostgresStore, pool: &PgPool) {
    set_clock(pool, "2026-01-01 00:00:00+00").await;
    let id = create(store, "retry-clock").await;
    let claim = claimed(store, id).await;
    let name = StepName::new("retry-at-deadline").unwrap();
    let step = store
        .begin_step(BeginStep {
            lease: guard(id, &claim),
            name: name.clone(),
            retry_policy: retry_policy(),
            mutation_id: MutationId::new(),
            at: Timestamp(30),
        })
        .await
        .unwrap();
    let current = store.load_execution(id).await.unwrap().unwrap();
    store
        .fail_step(FailStep {
            lease: LeaseGuard {
                expected_revision: current.revision,
                ..guard(id, &claim)
            },
            name,
            expected_attempt: step.attempt,
            mutation_id: MutationId::new(),
            failure: FailureRecord {
                class: FailureClass::Retryable,
                diagnostic: None,
                occurred_at: Timestamp(31),
            },
            retry_at: Some(Timestamp(1_800_000_000_000)),
            retry_policy: retry_policy(),
        })
        .await
        .unwrap();
    let current = store.load_execution(id).await.unwrap().unwrap();
    store
        .fail_execution(FailExecution {
            lease: LeaseGuard {
                expected_revision: current.revision,
                ..guard(id, &claim)
            },
            mutation_id: MutationId::new(),
            failure: FailureRecord {
                class: FailureClass::Permanent,
                diagnostic: None,
                occurred_at: Timestamp(32),
            },
        })
        .await
        .unwrap();
    assert!(matches!(
        store
            .acquire_execution(id, WorkerId::new(), MutationId::new(), 60_000)
            .await
            .unwrap(),
        ClaimOutcome::Busy
    ));
    set_clock(pool, "2027-02-01 00:00:00+00").await;
    assert!(matches!(
        store
            .acquire_execution(id, WorkerId::new(), MutationId::new(), 60_000)
            .await
            .unwrap(),
        ClaimOutcome::Claimed(_)
    ));
    reset_clock(pool).await;
}

async fn set_clock(pool: &PgPool, at: &str) {
    let statement = format!(
        "CREATE OR REPLACE FUNCTION resumable_now() RETURNS TIMESTAMPTZ
		LANGUAGE SQL VOLATILE AS $$ SELECT TIMESTAMPTZ '{at}' $$"
    );
    sqlx::raw_sql(&statement).execute(pool).await.unwrap();
}

async fn reset_clock(pool: &PgPool) {
    sqlx::raw_sql(
        "CREATE OR REPLACE FUNCTION resumable_now() RETURNS TIMESTAMPTZ
		LANGUAGE SQL VOLATILE AS $$ SELECT clock_timestamp() $$",
    )
    .execute(pool)
    .await
    .unwrap();
}
