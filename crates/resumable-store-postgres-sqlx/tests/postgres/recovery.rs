use super::support::{claimed, create, guard};
use resumable::{
    BeginStep, ClaimOutcome, CompleteExecution, CompleteStep, ExecutionId, ExecutionStatus,
    FailExecution, FailStep, FailureClass, FailureRecord, LeaseGuard, MutationId, NewExecution,
    OperationKey, RequestCancellation, StepName, Store, Timestamp, WorkerId,
};
use resumable_store_postgres_sqlx::PostgresStore;
use sqlx::postgres::PgPoolOptions;

pub(super) async fn verify_restart_and_receipt_replay(pool: sqlx::PgPool) -> sqlx::PgPool {
    let store = PostgresStore::new(pool.clone());
    let id = create(&store, "restart-recovery").await;
    let claim = claimed(&store, id).await;
    let name = StepName::new("durable-step").unwrap();
    let step = store
        .begin_step(BeginStep {
            lease: guard(id, &claim),
            name: name.clone(),
            retry_policy: Default::default(),
            mutation_id: MutationId::new(),
            at: Timestamp(10),
        })
        .await
        .expect("begin durable step");
    let current = store.load_execution(id).await.unwrap().unwrap();
    let completion = CompleteStep {
        lease: resumable::LeaseGuard {
            expected_revision: current.revision,
            ..guard(id, &claim)
        },
        name,
        expected_attempt: step.attempt,
        mutation_id: MutationId::new(),
        result: Some(resumable::StoredValue::new("serde-json", 1, b"recovered".to_vec()).unwrap()),
        at: Timestamp(11),
    };
    let expected = store.complete_step(completion.clone()).await.unwrap();
    let current = store.load_execution(id).await.unwrap().unwrap();
    let terminal = store
        .complete_execution(CompleteExecution {
            lease: LeaseGuard {
                expected_revision: current.revision,
                ..guard(id, &claim)
            },
            mutation_id: MutationId::new(),
            result: None,
            at: Timestamp(12),
        })
        .await
        .unwrap();
    assert_eq!(terminal.status, ExecutionStatus::Completed);
    let retry_id = create(&store, "retry-recovery").await;
    let retry_claim = claimed(&store, retry_id).await;
    let retry_name = StepName::new("pending-retry").unwrap();
    let retry_step = store
        .begin_step(BeginStep {
            lease: guard(retry_id, &retry_claim),
            name: retry_name.clone(),
            retry_policy: super::support::retry_policy(),
            mutation_id: MutationId::new(),
            at: Timestamp(13),
        })
        .await
        .unwrap();
    let current = store.load_execution(retry_id).await.unwrap().unwrap();
    store
        .fail_step(FailStep {
            lease: LeaseGuard {
                expected_revision: current.revision,
                ..guard(retry_id, &retry_claim)
            },
            name: retry_name,
            expected_attempt: retry_step.attempt,
            mutation_id: MutationId::new(),
            failure: FailureRecord {
                class: FailureClass::Retryable,
                diagnostic: None,
                occurred_at: Timestamp(14),
            },
            retry_at: Some(Timestamp(0)),
            retry_policy: super::support::retry_policy(),
        })
        .await
        .unwrap();
    let current = store.load_execution(retry_id).await.unwrap().unwrap();
    store
        .fail_execution(FailExecution {
            lease: LeaseGuard {
                expected_revision: current.revision,
                ..guard(retry_id, &retry_claim)
            },
            mutation_id: MutationId::new(),
            failure: FailureRecord {
                class: FailureClass::Permanent,
                diagnostic: None,
                occurred_at: Timestamp(15),
            },
        })
        .await
        .unwrap();
    let cancelled_id = ExecutionId::new();
    store
        .create_execution(NewExecution::new(
            cancelled_id,
            OperationKey::new("cancel-recovery", 1).unwrap(),
            Timestamp(16),
        ))
        .await
        .unwrap();
    store
        .request_cancellation(RequestCancellation {
            execution_id: cancelled_id,
            at: Timestamp(17),
        })
        .await
        .unwrap();
    pool.close().await;
    let reopened = PgPoolOptions::new()
        .max_connections(4)
        .connect(&super::test_database_url())
        .await
        .unwrap();
    let recovered = PostgresStore::new(reopened.clone());
    assert_eq!(
        recovered.load_step(id, &completion.name).await.unwrap(),
        Some(expected.clone())
    );
    assert_eq!(recovered.complete_step(completion).await.unwrap(), expected);
    assert_eq!(
        recovered.load_execution(id).await.unwrap().unwrap(),
        terminal
    );
    assert_eq!(
        recovered
            .load_execution(cancelled_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ExecutionStatus::Cancelled
    );
    let claim = match recovered
        .acquire_execution(retry_id, WorkerId::new(), MutationId::new(), 60_000)
        .await
        .unwrap()
    {
        ClaimOutcome::Claimed(claim) => claim,
        other => panic!("expected retry execution claim, got {other:?}"),
    };
    let next_attempt = recovered
        .begin_step(BeginStep {
            lease: guard(retry_id, &claim),
            name: StepName::new("pending-retry").unwrap(),
            retry_policy: super::support::retry_policy(),
            mutation_id: MutationId::new(),
            at: Timestamp(18),
        })
        .await
        .unwrap();
    assert_eq!(next_attempt.attempt, 2);
    reopened
}
