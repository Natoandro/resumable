mod conformance;
mod recovery;
mod retry;
mod runnable;
mod support;
mod transaction_guarantees;
mod transactions;

use resumable_store_postgres_sqlx::{PostgresStore, migrate, validate_schema};
use sqlx::postgres::PgPoolOptions;

fn test_database_url() -> String {
    std::env::var("RESUMABLE_TEST_DATABASE_URL")
        .expect("set RESUMABLE_TEST_DATABASE_URL to a disposable PostgreSQL database")
}

#[tokio::test]
async fn postgres_store_conformance_and_recovery() {
    let url = test_database_url();
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .expect("connect to PostgreSQL");
    migrate(&pool).await.expect("apply adapter migration");
    validate_schema(&pool)
        .await
        .expect("validate adapter schema");
    let store = PostgresStore::new(pool.clone());
    resumable::conformance::run_store_scenarios(&store)
        .await
        .expect("shared Store conformance scenarios");
    conformance::verify_concurrent_claims(&store).await;
    runnable::verify_runnable_discovery(&store, &pool).await;
    let pool = recovery::verify_restart_and_receipt_replay(pool).await;
    let store = PostgresStore::new(pool.clone());
    transactions::verify_rollback(&store, &pool).await;
    transactions::verify_caller_owned_completion(&store, &pool).await;
    transactions::verify_retry_clock(&store, &pool).await;
    let pool = transaction_guarantees::verify_transaction_guarantees(pool).await;
    let store = PostgresStore::new(pool.clone());
    retry::verify_exhausted_retry_remains_claimable(&store).await;
    pool.close().await;
}
