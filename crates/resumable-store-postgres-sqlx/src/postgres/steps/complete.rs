use super::super::*;
use super::*;
use resumable::{CompleteStep, StepRecord};

impl PostgresStore {
    pub(in crate::postgres) async fn complete_step_inner(
        &self,
        request: CompleteStep,
    ) -> Result<StepRecord, StoreError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let step = self
            .complete_step_in_transaction(&mut transaction, request)
            .await?;
        transaction.commit().await.map_err(store_error)?;
        Ok(step)
    }

    /// Record a fenced step completion in the caller's PostgreSQL transaction.
    ///
    /// The execution row lock is held until the caller commits or rolls back. The caller owns
    /// transaction lifecycle and must not run unrelated work before committing this mutation.
    pub async fn complete_step_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        request: CompleteStep,
    ) -> Result<StepRecord, StoreError> {
        let request_value = request_json(&request)?;
        let mut execution = lock_execution(transaction, request.lease.execution_id).await?;
        if let Some(receipt) = replay_receipt(
            transaction,
            *request.lease.execution_id.as_uuid(),
            *request.mutation_id.as_uuid(),
            "complete_step",
            &request_value,
        )
        .await?
        {
            let step = outcome_from(receipt)?;
            return Ok(step);
        }
        let now = resumable::Timestamp(transaction_now(transaction).await?);
        check_lease(&execution, &request.lease, now)?;
        let row = sqlx::query_as!(
            StepStateRow,
            "SELECT
				record AS \"record: Json<StepRecord>\",
				active_attempt_generation
			FROM resumable_steps
			WHERE execution_id = $1 AND name = $2
			FOR UPDATE",
            *request.lease.execution_id.as_uuid(),
            request.name.as_str(),
        )
        .fetch_optional(&mut **transaction)
        .await
        .map_err(store_error)?
        .ok_or_else(|| conflict("step not found"))?;
        if row.active_attempt_generation != i64::try_from(request.lease.generation).ok() {
            return Err(conflict(
                "step attempt belongs to a stale fencing generation",
            ));
        }
        let mut step = decode_step(row.record)?;
        if step.attempt != request.expected_attempt {
            return Err(conflict("stale step attempt"));
        }
        step.complete_with_result(request.result.clone(), request.at)
            .map_err(|error| conflict(error.to_string()))?;
        Self::persist_step(transaction, &step, None).await?;
        update_attempt(transaction, &step, "completed", None, None, request.at.0).await?;
        bump_revision(&mut execution, request.at)?;
        Self::persist_execution(transaction, &execution).await?;
        let outcome = outcome_json(&step)?;
        save_receipt(
            transaction,
            *request.lease.execution_id.as_uuid(),
            *request.mutation_id.as_uuid(),
            "complete_step",
            &request_value,
            &outcome,
        )
        .await?;
        Ok(step)
    }
}

pub(super) async fn update_attempt(
    transaction: &mut Transaction<'_, Postgres>,
    step: &StepRecord,
    status: &str,
    failure_class: Option<&str>,
    failure_diagnostic: Option<&str>,
    finished_at: i64,
) -> Result<(), StoreError> {
    let result = sqlx::query!(
        "UPDATE resumable_attempts SET
			status = $3,
			finished_at = (TIMESTAMPTZ 'epoch' + $4::bigint * INTERVAL '1 millisecond'),
			failure_class = $5,
			failure_diagnostic = $6
		WHERE step_id = $1 AND attempt_number = $2 AND status = 'running'",
        *step.id.as_uuid(),
        i64::from(step.attempt),
        status,
        finished_at,
        failure_class,
        failure_diagnostic,
    )
    .execute(&mut **transaction)
    .await
    .map_err(store_error)?;
    if result.rows_affected() != 1 {
        return Err(corrupt("active step attempt record is missing"));
    }
    Ok(())
}
