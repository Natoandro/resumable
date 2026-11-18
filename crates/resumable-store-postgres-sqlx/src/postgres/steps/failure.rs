use super::super::*;
use super::complete::update_attempt;
use super::*;
use resumable::{FailStep, FailureClass, StepRecord};

impl PostgresStore {
    pub(in crate::postgres) async fn fail_step_inner(
        &self,
        request: FailStep,
    ) -> Result<StepRecord, StoreError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let request_value = request_json(&request)?;
        let mut execution = lock_execution(&mut transaction, request.lease.execution_id).await?;
        if let Some(receipt) = replay_receipt(
            &mut transaction,
            *request.lease.execution_id.as_uuid(),
            *request.mutation_id.as_uuid(),
            "fail_step",
            &request_value,
        )
        .await?
        {
            let step = outcome_from(receipt)?;
            transaction.commit().await.map_err(store_error)?;
            return Ok(step);
        }
        let now = resumable::Timestamp(transaction_now(&mut transaction).await?);
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
        .fetch_optional(&mut *transaction)
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
        step.set_retry_policy(request.retry_policy.clone())
            .map_err(|error| conflict(error.to_string()))?;
        step.fail(request.failure.clone(), request.retry_at)
            .map_err(|error| conflict(error.to_string()))?;
        Self::persist_step(&mut transaction, &step, None).await?;
        let class_name = failure_name(request.failure.class)?;
        update_attempt(
            &mut transaction,
            &step,
            if request.failure.class == FailureClass::Cancelled {
                "cancelled"
            } else {
                "failed"
            },
            Some(&class_name),
            request.failure.diagnostic.as_deref(),
            request.failure.occurred_at.0,
        )
        .await?;
        bump_revision(&mut execution, step.updated_at)?;
        Self::persist_execution(&mut transaction, &execution).await?;
        let outcome = outcome_json(&step)?;
        save_receipt(
            &mut transaction,
            *request.lease.execution_id.as_uuid(),
            *request.mutation_id.as_uuid(),
            "fail_step",
            &request_value,
            &outcome,
        )
        .await?;
        transaction.commit().await.map_err(store_error)?;
        Ok(step)
    }
}

fn failure_name(class: FailureClass) -> Result<String, StoreError> {
    serde_json::to_value(class)
        .map_err(|error| corrupt(error.to_string()))?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| corrupt("invalid failure class"))
}
