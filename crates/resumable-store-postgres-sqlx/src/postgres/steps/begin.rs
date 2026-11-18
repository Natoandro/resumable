use super::super::*;
use super::complete::update_attempt;
use super::*;
use resumable::{BeginStep, StepRecord, StepStatus};

impl PostgresStore {
    pub(in crate::postgres) async fn begin_step_inner(
        &self,
        request: BeginStep,
    ) -> Result<StepRecord, StoreError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let request_value = request_json(&request)?;
        let mut execution = lock_execution(&mut transaction, request.lease.execution_id).await?;
        if let Some(receipt) = replay_receipt(
            &mut transaction,
            *request.lease.execution_id.as_uuid(),
            *request.mutation_id.as_uuid(),
            "begin_step",
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
        if execution.cancellation_requested_at.is_some() {
            return Err(conflict("cancellation was requested"));
        }
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
        .map_err(store_error)?;
        let existed = row.is_some();
        let mut step = if let Some(row) = row {
            let step = decode_step(row.record)?;
            if step
                .retry_policy
                .as_ref()
                .is_some_and(|policy| policy != &request.retry_policy)
            {
                return Err(conflict("step retry policy differs from persisted policy"));
            }
            if step.status == StepStatus::Running
                && row.active_attempt_generation == i64::try_from(request.lease.generation).ok()
            {
                return Err(conflict("step is already running under this generation"));
            }
            if matches!(step.status, StepStatus::Completed | StepStatus::Cancelled) {
                return Err(conflict("completed or cancelled step cannot restart"));
            }
            if step.status == StepStatus::Failed {
                let policy = step
                    .retry_policy
                    .as_ref()
                    .ok_or_else(|| corrupt("failed step has no persisted retry policy"))?;
                let failure = step
                    .failure
                    .as_ref()
                    .ok_or_else(|| corrupt("failed step has no persisted failure"))?;
                if failure.class != resumable::FailureClass::Retryable
                    || !policy.retryable
                    || step.attempt >= policy.max_attempts
                {
                    return Err(conflict(
                        "step retry policy does not permit another attempt",
                    ));
                }
            }
            if step.next_attempt_at.is_some_and(|at| at > now) {
                return Err(conflict("retry is not eligible yet"));
            }
            if step.status == StepStatus::Running {
                update_attempt(
                    &mut transaction,
                    &step,
                    "failed",
                    Some("retryable"),
                    Some("attempt interrupted after lease expiry"),
                    now.0,
                )
                .await?;
            }
            step
        } else {
            StepRecord::new(
                resumable::StepId::new(),
                request.lease.execution_id,
                request.name.clone(),
                request.at,
            )
        };
        step.set_retry_policy(request.retry_policy.clone())
            .map_err(|error| conflict(error.to_string()))?;
        step.begin_attempt(request.at)
            .map_err(|error| conflict(error.to_string()))?;
        if existed {
            Self::persist_step(&mut transaction, &step, Some(request.lease.generation)).await?;
        } else {
            Self::insert_step(&mut transaction, &step, request.lease.generation).await?;
        }
        let generation = i64::try_from(request.lease.generation)
            .map_err(|_| corrupt("fencing generation exceeds PostgreSQL range"))?;
        sqlx::query!(
            "INSERT INTO resumable_attempts (
				step_id, attempt_number, fencing_generation, status, started_at
			) VALUES (
				$1, $2, $3, 'running',
				(TIMESTAMPTZ 'epoch' + $4::bigint * INTERVAL '1 millisecond')
			)",
            *step.id.as_uuid(),
            i64::from(step.attempt),
            generation,
            request.at.0,
        )
        .execute(&mut *transaction)
        .await
        .map_err(|error| {
            if let sqlx::Error::Database(database) = &error {
                if database.code().as_deref() == Some("23505") {
                    return conflict("attempt number already exists for this step");
                }
            }
            store_error(error)
        })?;
        bump_revision(&mut execution, request.at)?;
        Self::persist_execution(&mut transaction, &execution).await?;
        let outcome = outcome_json(&step)?;
        save_receipt(
            &mut transaction,
            *request.lease.execution_id.as_uuid(),
            *request.mutation_id.as_uuid(),
            "begin_step",
            &request_value,
            &outcome,
        )
        .await?;
        transaction.commit().await.map_err(store_error)?;
        Ok(step)
    }
}
