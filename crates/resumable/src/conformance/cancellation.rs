use super::*;

pub(super) async fn run<S: Store>(store: &S) -> Result<(), StoreError> {
    let cancelled_id = ExecutionId::new();
    store
        .create_execution(NewExecution::new(
            cancelled_id,
            OperationKey::new("conformance-cancel", 1).unwrap(),
            Timestamp(20),
        ))
        .await?;
    let cancelled = store
        .request_cancellation(RequestCancellation {
            execution_id: cancelled_id,
            at: Timestamp(21),
        })
        .await?;
    assertion(
        cancelled.status == ExecutionStatus::Cancelled,
        "pending cancellation must be durable",
    )?;
    let repeated = store
        .request_cancellation(RequestCancellation {
            execution_id: cancelled_id,
            at: Timestamp(22),
        })
        .await?;
    assertion(
        repeated == cancelled,
        "repeated cancellation must preserve the original terminal record",
    )?;
    Ok(())
}
