use resumable::{
    ClaimOutcome, ExecutionClaim, ExecutionId, LeaseGuard, MutationId, NewExecution, OperationKey,
    Store, Timestamp, WorkerId,
};
use resumable_store_postgres_sqlx::PostgresStore;
use sqlx::PgPool;

pub(super) async fn claimed(store: &PostgresStore, id: ExecutionId) -> ExecutionClaim {
    match store
        .acquire_execution(id, WorkerId::new(), MutationId::new(), 60_000)
        .await
        .unwrap()
    {
        ClaimOutcome::Claimed(claim) => claim,
        other => panic!("expected claim, got {other:?}"),
    }
}

pub(super) fn guard(id: ExecutionId, claim: &ExecutionClaim) -> LeaseGuard {
    LeaseGuard {
        execution_id: id,
        worker: claim.worker,
        token: claim.token,
        generation: claim.generation,
        expected_revision: claim.record.revision,
    }
}

pub(super) fn new_execution(id: ExecutionId) -> NewExecution {
    NewExecution::new(
        id,
        OperationKey::new("transaction-atomicity", 1).unwrap(),
        Timestamp(0),
    )
}

pub(super) async fn ensure_domain_table(pool: &PgPool) {
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS resumable_test_domain_writes (
			execution_id UUID PRIMARY KEY,
			value TEXT NOT NULL
		)",
    )
    .execute(pool)
    .await
    .unwrap();
}

pub(super) async fn drop_domain_table(pool: &PgPool) {
    sqlx::raw_sql("DROP TABLE resumable_test_domain_writes")
        .execute(pool)
        .await
        .unwrap();
}

pub(super) async fn insert_domain_write(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: ExecutionId,
) {
    sqlx::query(
        "INSERT INTO resumable_test_domain_writes (execution_id, value) VALUES ($1, 'written')",
    )
    .bind(*id.as_uuid())
    .execute(&mut **transaction)
    .await
    .unwrap();
}

pub(super) async fn domain_write_count(pool: &PgPool, id: ExecutionId) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM resumable_test_domain_writes WHERE execution_id = $1")
        .bind(*id.as_uuid())
        .fetch_one(pool)
        .await
        .unwrap()
}

pub(super) async fn reset_domain_write(pool: &PgPool, id: ExecutionId) {
    sqlx::query("DELETE FROM resumable_test_domain_writes WHERE execution_id = $1")
        .bind(*id.as_uuid())
        .execute(pool)
        .await
        .unwrap();
}

pub(super) async fn set_clock(pool: &PgPool, at: &str) {
    let statement = format!(
        "CREATE OR REPLACE FUNCTION resumable_now() RETURNS TIMESTAMPTZ
		LANGUAGE SQL VOLATILE AS $$ SELECT TIMESTAMPTZ '{at}' $$"
    );
    sqlx::raw_sql(&statement).execute(pool).await.unwrap();
}

pub(super) async fn reset_clock(pool: &PgPool) {
    sqlx::raw_sql(
        "CREATE OR REPLACE FUNCTION resumable_now() RETURNS TIMESTAMPTZ
		LANGUAGE SQL VOLATILE AS $$ SELECT clock_timestamp() $$",
    )
    .execute(pool)
    .await
    .unwrap();
}
