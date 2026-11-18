use super::support::{claimed, create, guard};
use resumable::{
    BeginStep, ClaimOutcome, FailExecution, FailStep, FailureClass, FailureRecord, LeaseGuard,
    MutationId, StepName, Store, StoreError, Timestamp, WorkerId,
};
use resumable_store_postgres_sqlx::PostgresStore;

pub(super) async fn verify_exhausted_retry_remains_claimable(store: &PostgresStore) {
    let id = create(store, "exhausted-retry-claim").await;
    let first_claim = claimed(store, id).await;
    let name = StepName::new("single-attempt").unwrap();
    let policy = resumable::RetryPolicyRecord {
        max_attempts: 1,
        backoff: resumable::BackoffRecord::None,
        retryable: true,
    };
    let step = store
        .begin_step(BeginStep {
            lease: guard(id, &first_claim),
            name: name.clone(),
            retry_policy: policy.clone(),
            mutation_id: MutationId::new(),
            at: Timestamp(40),
        })
        .await
        .unwrap();
    let current = store.load_execution(id).await.unwrap().unwrap();
    store
        .fail_step(FailStep {
            lease: LeaseGuard {
                expected_revision: current.revision,
                ..guard(id, &first_claim)
            },
            name: name.clone(),
            expected_attempt: step.attempt,
            mutation_id: MutationId::new(),
            failure: FailureRecord {
                class: FailureClass::Retryable,
                diagnostic: None,
                occurred_at: Timestamp(41),
            },
            retry_at: Some(Timestamp(0)),
            retry_policy: policy.clone(),
        })
        .await
        .unwrap();
    let current = store.load_execution(id).await.unwrap().unwrap();
    store
        .fail_execution(FailExecution {
            lease: LeaseGuard {
                expected_revision: current.revision,
                ..guard(id, &first_claim)
            },
            mutation_id: MutationId::new(),
            failure: FailureRecord {
                class: FailureClass::Permanent,
                diagnostic: None,
                occurred_at: Timestamp(42),
            },
        })
        .await
        .unwrap();
    let retry_claim = match store
        .acquire_execution(id, WorkerId::new(), MutationId::new(), 60_000)
        .await
        .unwrap()
    {
        ClaimOutcome::Claimed(claim) => claim,
        other => panic!("expected claim despite exhausted retry, got {other:?}"),
    };
    let result = store
        .begin_step(BeginStep {
            lease: guard(id, &retry_claim),
            name,
            retry_policy: policy,
            mutation_id: MutationId::new(),
            at: Timestamp(43),
        })
        .await;
    assert!(matches!(result, Err(StoreError::Conflict(_))));
}
