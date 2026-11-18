use super::*;
use resumable::{CompleteExecution, ExecutionRecord, FailExecution, RequestCancellation};

impl PostgresStore {
    pub(super) async fn complete_execution_inner(
        &self,
        request: CompleteExecution,
    ) -> Result<ExecutionRecord, StoreError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let request_value = request_json(&request)?;
        let mut record = lock_execution(&mut transaction, request.lease.execution_id).await?;
        if let Some(receipt) = replay_receipt(
            &mut transaction,
            *request.lease.execution_id.as_uuid(),
            *request.mutation_id.as_uuid(),
            "complete_execution",
            &request_value,
        )
        .await?
        {
            let record = outcome_from(receipt)?;
            transaction.commit().await.map_err(store_error)?;
            return Ok(record);
        }
        let now = resumable::Timestamp(transaction_now(&mut transaction).await?);
        check_lease(&record, &request.lease, now)?;
        if record.cancellation_requested_at.is_none() {
            let incomplete = sqlx::query_scalar!(
                "SELECT count(*)::bigint AS \"count!\"
				FROM resumable_steps
				WHERE execution_id = $1 AND status <> 'completed'",
                *request.lease.execution_id.as_uuid(),
            )
            .fetch_one(&mut *transaction)
            .await
            .map_err(store_error)?;
            if incomplete > 0 {
                return Err(conflict("execution has steps that are not completed"));
            }
        }
        record
            .finish_with_result(request.result.clone(), request.at)
            .map_err(|error| conflict(error.to_string()))?;
        Self::persist_execution(&mut transaction, &record).await?;
        save_record_receipt(
            &mut transaction,
            &request,
            "complete_execution",
            &request_value,
            &record,
        )
        .await?;
        transaction.commit().await.map_err(store_error)?;
        Ok(record)
    }

    pub(super) async fn fail_execution_inner(
        &self,
        request: FailExecution,
    ) -> Result<ExecutionRecord, StoreError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let request_value = request_json(&request)?;
        let mut record = lock_execution(&mut transaction, request.lease.execution_id).await?;
        if let Some(receipt) = replay_receipt(
            &mut transaction,
            *request.lease.execution_id.as_uuid(),
            *request.mutation_id.as_uuid(),
            "fail_execution",
            &request_value,
        )
        .await?
        {
            let record = outcome_from(receipt)?;
            transaction.commit().await.map_err(store_error)?;
            return Ok(record);
        }
        let now = resumable::Timestamp(transaction_now(&mut transaction).await?);
        check_lease(&record, &request.lease, now)?;
        record
            .fail(request.failure.clone())
            .map_err(|error| conflict(error.to_string()))?;
        Self::persist_execution(&mut transaction, &record).await?;
        save_record_receipt(
            &mut transaction,
            &request,
            "fail_execution",
            &request_value,
            &record,
        )
        .await?;
        transaction.commit().await.map_err(store_error)?;
        Ok(record)
    }

    pub(super) async fn request_cancellation_inner(
        &self,
        request: RequestCancellation,
    ) -> Result<ExecutionRecord, StoreError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let mut record = lock_execution(&mut transaction, request.execution_id).await?;
        record
            .request_cancellation(request.at)
            .map_err(|error| conflict(error.to_string()))?;
        Self::persist_execution(&mut transaction, &record).await?;
        transaction.commit().await.map_err(store_error)?;
        Ok(record)
    }

    pub(super) async fn purge_execution_inner(
        &self,
        id: resumable::ExecutionId,
    ) -> Result<bool, StoreError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let row = sqlx::query!(
            "SELECT status FROM resumable_executions WHERE id = $1 FOR UPDATE",
            *id.as_uuid(),
        )
        .fetch_optional(&mut *transaction)
        .await
        .map_err(store_error)?;
        let Some(row) = row else {
            transaction.commit().await.map_err(store_error)?;
            return Ok(false);
        };
        if row.status != "completed" && row.status != "cancelled" {
            return Err(conflict(
                "only completed or cancelled executions can be purged",
            ));
        }
        sqlx::query!(
            "DELETE FROM resumable_executions WHERE id = $1",
            *id.as_uuid(),
        )
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
        transaction.commit().await.map_err(store_error)?;
        Ok(true)
    }
}

async fn save_record_receipt<T: serde::Serialize>(
    transaction: &mut Transaction<'_, Postgres>,
    request: &T,
    kind: &str,
    request_value: &serde_json::Value,
    record: &ExecutionRecord,
) -> Result<(), StoreError> {
    let request = request_json(request)?;
    let value = outcome_json(record)?;
    let (execution_id, mutation_id) = receipt_identity(&request)?;
    if request != *request_value {
        return Err(corrupt("mutation request changed during receipt creation"));
    }
    save_receipt(
        transaction,
        execution_id,
        mutation_id,
        kind,
        request_value,
        &value,
    )
    .await
}

fn receipt_identity(value: &serde_json::Value) -> Result<(uuid::Uuid, uuid::Uuid), StoreError> {
    let lease = value
        .get("lease")
        .ok_or_else(|| corrupt("execution mutation has no lease identity"))?;
    let execution_id = lease
        .get("execution_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| corrupt("invalid execution mutation identity"))?;
    let mutation_id = value
        .get("mutation_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| corrupt("invalid mutation identity"))?;
    Ok((
        uuid::Uuid::parse_str(execution_id).map_err(|error| corrupt(error.to_string()))?,
        uuid::Uuid::parse_str(mutation_id).map_err(|error| corrupt(error.to_string()))?,
    ))
}
