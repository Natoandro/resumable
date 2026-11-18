use super::super::*;
use resumable::{FailureClass, StepRecord, StepStatus, StoredValue};

impl PostgresStore {
    pub(super) async fn insert_step(
        transaction: &mut Transaction<'_, Postgres>,
        step: &StepRecord,
        generation: u64,
    ) -> Result<(), StoreError> {
        let status = step_status(step.status)?;
        let generation = i64::try_from(generation)
            .map_err(|_| corrupt("fencing generation exceeds PostgreSQL range"))?;
        let attempt = i64::from(step.attempt);
        let retry_policy = step.retry_policy.as_ref().map(encode).transpose()?;
        let snapshot = encode(step)?;
        let started_at = step.started_at.map(|at| at.0);
        sqlx::query!(
            "INSERT INTO resumable_steps (
				id, execution_id, name, status, attempt_count,
				active_attempt_generation, retry_policy, record,
				created_at, updated_at, started_at
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8,
				(TIMESTAMPTZ 'epoch' + $9::bigint * INTERVAL '1 millisecond'),
				(TIMESTAMPTZ 'epoch' + $10::bigint * INTERVAL '1 millisecond'),
				(TIMESTAMPTZ 'epoch' + $11::bigint * INTERVAL '1 millisecond')
			)",
            *step.id.as_uuid(),
            *step.execution_id.as_uuid(),
            step.name.as_str(),
            status,
            attempt,
            Some(generation),
            retry_policy,
            snapshot,
            step.created_at.0,
            step.updated_at.0,
            started_at,
        )
        .execute(&mut **transaction)
        .await
        .map_err(store_error)?;
        Ok(())
    }

    pub(super) async fn persist_step(
        transaction: &mut Transaction<'_, Postgres>,
        step: &StepRecord,
        active_generation: Option<u64>,
    ) -> Result<(), StoreError> {
        let status = step_status(step.status)?;
        let attempt = i64::from(step.attempt);
        let generation = active_generation
            .map(i64::try_from)
            .transpose()
            .map_err(|_| corrupt("fencing generation exceeds PostgreSQL range"))?;
        let retry_policy = step.retry_policy.as_ref().map(encode).transpose()?;
        let result_codec = step.result.as_ref().map(StoredValue::codec);
        let result_schema_version = step
            .result
            .as_ref()
            .map(|result| i64::from(result.schema_version()));
        let result_bytes = step.result.as_ref().map(StoredValue::bytes);
        let failure_class = step
            .failure
            .as_ref()
            .map(|failure| failure_class(failure.class))
            .transpose()?;
        let failure_diagnostic = step
            .failure
            .as_ref()
            .and_then(|failure| failure.diagnostic.clone());
        let updated_at = step.updated_at.0;
        let started_at = step.started_at.map(|at| at.0);
        let completed_at = step.completed_at.map(|at| at.0);
        let next_attempt_at = step.next_attempt_at.map(|at| at.0);
        let snapshot = encode(step)?;
        sqlx::query!(
            "UPDATE resumable_steps SET
				status = $2,
				attempt_count = $3,
				active_attempt_generation = $4,
				result_codec = $5,
				result_schema_version = $6,
				result_bytes = $7,
				retry_policy = $8,
				failure_class = $9,
				failure_diagnostic = $10,
				updated_at = (TIMESTAMPTZ 'epoch' + $11::bigint * INTERVAL '1 millisecond'),
				started_at = (TIMESTAMPTZ 'epoch' + $12::bigint * INTERVAL '1 millisecond'),
				completed_at = (TIMESTAMPTZ 'epoch' + $13::bigint * INTERVAL '1 millisecond'),
				next_attempt_at = (TIMESTAMPTZ 'epoch' + $14::bigint * INTERVAL '1 millisecond'),
				record = $15
			WHERE id = $1",
            *step.id.as_uuid(),
            status,
            attempt,
            generation,
            result_codec,
            result_schema_version,
            result_bytes,
            retry_policy,
            failure_class,
            failure_diagnostic,
            updated_at,
            started_at,
            completed_at,
            next_attempt_at,
            snapshot,
        )
        .execute(&mut **transaction)
        .await
        .map_err(store_error)?;
        Ok(())
    }
}

fn step_status(status: StepStatus) -> Result<String, StoreError> {
    serde_json::to_value(status)
        .map_err(|error| corrupt(error.to_string()))?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| corrupt("invalid step status"))
}

fn failure_class(class: FailureClass) -> Result<String, StoreError> {
    serde_json::to_value(class)
        .map_err(|error| corrupt(error.to_string()))?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| corrupt("invalid failure class"))
}
