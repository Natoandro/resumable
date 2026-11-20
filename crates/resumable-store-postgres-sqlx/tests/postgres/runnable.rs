use resumable::{
    ClaimOutcome, ExecutionId, FailedExecutionPolicy, MutationId, NewExecution, OperationKey,
    Store, Timestamp, WorkerId,
};
use resumable_store_postgres_sqlx::{PostgresStore, SCHEMA_VERSION, validate_schema_version};
use sqlx::PgPool;

pub(super) async fn verify_runnable_discovery(store: &PostgresStore, pool: &PgPool) {
    set_clock(pool, "2026-01-01 00:00:00+00").await;
    let operation_name = format!("fair-dispatch-{}", ExecutionId::new());
    let supported = OperationKey::new(&operation_name, 1).unwrap();
    let unsupported = OperationKey::new(&operation_name, 2).unwrap();
    let unknown_id = create(store, unsupported.clone(), 0, None).await;
    let execution_id = create(store, supported.clone(), 1, None).await;
    assert!(
        validate_schema_version(pool, SCHEMA_VERSION + 1)
            .await
            .is_err()
    );
    assert_eq!(
        store
            .load_execution(execution_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        resumable::ExecutionStatus::Pending
    );

    assert!(
        store
            .unsupported_runnable_operations(
                std::slice::from_ref(&supported),
                FailedExecutionPolicy::Reject
            )
            .await
            .unwrap()
            .contains(&unsupported)
    );
    assert_eq!(
        store
            .runnable_executions(
                std::slice::from_ref(&supported),
                FailedExecutionPolicy::Reject,
                1
            )
            .await
            .unwrap(),
        vec![resumable_store_postgres_sqlx::RunnableExecution {
            id: execution_id,
            operation: supported.clone(),
        }]
    );
    assert!(store.load_execution(unknown_id).await.unwrap().is_some());

    assert!(matches!(
        store
            .acquire_execution(execution_id, WorkerId::new(), MutationId::new(), 1_000)
            .await
            .unwrap(),
        ClaimOutcome::Claimed(_)
    ));
    assert!(
        store
            .runnable_executions(
                std::slice::from_ref(&supported),
                FailedExecutionPolicy::Reject,
                1
            )
            .await
            .unwrap()
            .is_empty()
    );

    set_clock(pool, "2026-01-01 00:00:01+00").await;
    assert_eq!(
        store
            .runnable_executions(
                std::slice::from_ref(&supported),
                FailedExecutionPolicy::Reject,
                1
            )
            .await
            .unwrap()
            .first()
            .map(|execution| execution.id),
        Some(execution_id)
    );
    assert!(matches!(
        store
            .acquire_execution(execution_id, WorkerId::new(), MutationId::new(), 1_000)
            .await
            .unwrap(),
        ClaimOutcome::Claimed(_)
    ));
    verify_scheduled_start(store, pool).await;
    reset_clock(pool).await;
}

async fn verify_scheduled_start(store: &PostgresStore, pool: &PgPool) {
    let operation = OperationKey::new(format!("scheduled-{}", ExecutionId::new()), 1).unwrap();
    let run_at = Timestamp(1_769_904_000_000); // 2026-02-01 00:00:00 UTC
    let id = create(store, operation.clone(), 0, Some(run_at)).await;
    assert_eq!(
        store.load_execution(id).await.unwrap().unwrap().run_at,
        Some(run_at)
    );
    assert!(
        store
            .runnable_executions(
                std::slice::from_ref(&operation),
                FailedExecutionPolicy::Reject,
                1
            )
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        !store
            .unsupported_runnable_operations(&[], FailedExecutionPolicy::Reject)
            .await
            .unwrap()
            .contains(&operation)
    );
    assert!(matches!(
        store
            .acquire_execution(id, WorkerId::new(), MutationId::new(), 1_000)
            .await
            .unwrap(),
        ClaimOutcome::Busy
    ));

    set_clock(pool, "2026-02-01 00:00:00+00").await;
    assert_eq!(
        store
            .runnable_executions(&[operation], FailedExecutionPolicy::Reject, 1)
            .await
            .unwrap()[0]
            .id,
        id
    );
    assert!(matches!(
        store
            .acquire_execution(id, WorkerId::new(), MutationId::new(), 1_000)
            .await
            .unwrap(),
        ClaimOutcome::Claimed(_)
    ));
}

async fn create(
    store: &PostgresStore,
    operation: OperationKey,
    created_at: i64,
    run_at: Option<Timestamp>,
) -> ExecutionId {
    let id = ExecutionId::new();
    let mut execution = NewExecution::new(id, operation, Timestamp(created_at));
    if let Some(run_at) = run_at {
        execution = execution.with_run_at(run_at);
    }
    store.create_execution(execution).await.unwrap();
    id
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
