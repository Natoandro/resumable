use super::super::*;
use resumable::{ExecutionRecord, Progress, ProgressUpdate, StepName};

impl PostgresStore {
    pub(in crate::postgres) async fn load_progress_inner(
        &self,
        execution: resumable::ExecutionId,
        name: &StepName,
    ) -> Result<Option<Progress>, StoreError> {
        let row = sqlx::query!(
            "SELECT p.current_value::text AS current_value,
				p.total_value::text AS total_value,
				p.message
			FROM resumable_progress AS p
			INNER JOIN resumable_steps AS s ON s.id = p.step_id
			WHERE s.execution_id = $1 AND s.name = $2",
            *execution.as_uuid(),
            name.as_str(),
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(store_error)?;
        row.map(|row| {
            Ok(Progress {
                current: parse_value(row.current_value)?,
                total: parse_value(row.total_value)?,
                message: row.message,
            })
        })
        .transpose()
    }

    pub(in crate::postgres) async fn heartbeat_inner(
        &self,
        request: ProgressUpdate,
    ) -> Result<ExecutionRecord, StoreError> {
        if request.lease_duration_ms > i64::MAX as u64 {
            return Err(conflict("lease duration is outside the PostgreSQL range"));
        }
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let mut execution = lock_execution(&mut transaction, request.lease.execution_id).await?;
        let now = resumable::Timestamp(transaction_now(&mut transaction).await?);
        check_lease(&execution, &request.lease, now)?;
        let lease = execution
            .lease
            .as_mut()
            .ok_or_else(|| conflict("execution has no active lease"))?;
        lease.expires_at = resumable::Timestamp(
            now.0
                .saturating_add(i64::try_from(request.lease_duration_ms).unwrap_or(i64::MAX)),
        );
        bump_revision(&mut execution, request.at)?;
        Self::persist_execution(&mut transaction, &execution).await?;
        if let (Some(name), Some(progress)) = (&request.step, &request.progress) {
            let current = progress.current.map(|value| value.to_string());
            let total = progress.total.map(|value| value.to_string());
            let result = sqlx::query!(
                "INSERT INTO resumable_progress (
					step_id, current_value, total_value, message, updated_at
				)
				SELECT s.id, $3::text::numeric, $4::text::numeric, $5,
					(TIMESTAMPTZ 'epoch' + $6::bigint * INTERVAL '1 millisecond')
				FROM resumable_steps AS s
				WHERE s.execution_id = $1 AND s.name = $2
				ON CONFLICT (step_id) DO UPDATE SET
					current_value = EXCLUDED.current_value,
					total_value = EXCLUDED.total_value,
					message = EXCLUDED.message,
					updated_at = EXCLUDED.updated_at",
                *request.lease.execution_id.as_uuid(),
                name.as_str(),
                current.as_deref(),
                total.as_deref(),
                progress.message.as_deref(),
                request.at.0,
            )
            .execute(&mut *transaction)
            .await
            .map_err(store_error)?;
            if result.rows_affected() == 0 {
                return Err(conflict("progress step does not exist"));
            }
        }
        transaction.commit().await.map_err(store_error)?;
        Ok(execution)
    }
}

fn parse_value(value: Option<String>) -> Result<Option<u64>, StoreError> {
    value
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|error| corrupt(format!("invalid persisted progress value: {error}")))
        })
        .transpose()
}
