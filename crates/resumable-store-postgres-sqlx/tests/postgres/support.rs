use resumable::{
    ClaimOutcome, ExecutionId, LeaseGuard, MutationId, NewExecution, OperationKey,
    RetryPolicyRecord, Store, Timestamp, WorkerId,
};
use resumable_store_postgres_sqlx::PostgresStore;

pub(super) async fn create(store: &PostgresStore, name: &str) -> ExecutionId {
    let id = ExecutionId::new();
    store
        .create_execution(NewExecution::new(
            id,
            OperationKey::new(name, 1).unwrap(),
            Timestamp(0),
        ))
        .await
        .unwrap();
    id
}

pub(super) async fn claimed(store: &PostgresStore, id: ExecutionId) -> resumable::ExecutionClaim {
    match store
        .acquire_execution(id, WorkerId::new(), MutationId::new(), 60_000)
        .await
        .unwrap()
    {
        ClaimOutcome::Claimed(claim) => claim,
        other => panic!("expected claim, got {other:?}"),
    }
}

pub(super) fn guard(id: ExecutionId, claim: &resumable::ExecutionClaim) -> LeaseGuard {
    LeaseGuard {
        execution_id: id,
        worker: claim.worker,
        token: claim.token,
        generation: claim.generation,
        expected_revision: claim.record.revision,
    }
}

pub(super) fn retry_policy() -> RetryPolicyRecord {
    RetryPolicyRecord {
        max_attempts: 2,
        backoff: resumable::BackoffRecord::None,
        retryable: true,
    }
}
