use super::support::create;
use resumable::{ClaimOutcome, MutationId, Store, WorkerId};
use resumable_store_postgres_sqlx::PostgresStore;

pub(super) async fn verify_concurrent_claims(store: &PostgresStore) {
    let id = create(store, "concurrent-claim").await;
    let first_store = store.clone();
    let second_store = store.clone();
    let first = async move {
        first_store
            .acquire_execution(id, WorkerId::new(), MutationId::new(), 60_000)
            .await
            .expect("first claim")
    };
    let second = async move {
        second_store
            .acquire_execution(id, WorkerId::new(), MutationId::new(), 60_000)
            .await
            .expect("second claim")
    };
    let (first, second) = tokio::join!(first, second);
    assert!(matches!(
        (&first, &second),
        (ClaimOutcome::Claimed(_), ClaimOutcome::Busy)
            | (ClaimOutcome::Busy, ClaimOutcome::Claimed(_))
    ));
}
