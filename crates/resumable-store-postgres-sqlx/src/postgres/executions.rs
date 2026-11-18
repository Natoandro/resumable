use super::*;
use resumable::{ExecutionId, ExecutionRecord, NewExecution, StoredValue};

struct ExecutionRow {
    record: Json<ExecutionRecord>,
}

impl PostgresStore {
    /// Create or reconcile an execution inside the caller's PostgreSQL transaction.
    /// This method never starts, commits, or rolls back the transaction.
    pub async fn create_execution_in_transaction(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        new: NewExecution,
    ) -> Result<ExecutionRecord, StoreError> {
        let record = ExecutionRecord::from(new.clone());
        let stored_record = encode(&record)?;
        let metadata = encode(&record.metadata)?;
        let operation_name = new.operation.name();
        let operation_version = i64::from(new.operation.version());
        let input_codec = new.input.as_ref().map(StoredValue::codec);
        let input_schema_version = new
            .input
            .as_ref()
            .map(|input| i64::from(input.schema_version()));
        let input_bytes = new.input.as_ref().map(StoredValue::bytes);
        let created_at = new.created_at.0;
        let run_at = new.run_at.map(|at| at.0);
        let inserted = sqlx::query_as!(
            ExecutionRow,
            "INSERT INTO resumable_executions (
				id, operation_name, operation_version, input_codec,
				input_schema_version, input_bytes, metadata, record,
				status, revision, fencing_generation, created_at, updated_at, run_at
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, 'pending', 0, 0,
				(TIMESTAMPTZ 'epoch' + $9::bigint * INTERVAL '1 millisecond'),
				(TIMESTAMPTZ 'epoch' + $9::bigint * INTERVAL '1 millisecond'),
				(TIMESTAMPTZ 'epoch' + $10::bigint * INTERVAL '1 millisecond')
			)
			ON CONFLICT (id) DO NOTHING
			RETURNING record AS \"record: Json<ExecutionRecord>\"",
            *new.id.as_uuid(),
            operation_name,
            operation_version,
            input_codec,
            input_schema_version,
            input_bytes,
            metadata,
            stored_record,
            created_at,
            run_at,
        )
        .fetch_optional(&mut **transaction)
        .await
        .map_err(store_error)?;
        if let Some(row) = inserted {
            return decode_execution(row.record);
        }
        let existing = sqlx::query_as!(
            ExecutionRow,
            "SELECT record AS \"record: Json<ExecutionRecord>\"
			FROM resumable_executions
			WHERE id = $1",
            *new.id.as_uuid(),
        )
        .fetch_optional(&mut **transaction)
        .await
        .map_err(store_error)?
        .ok_or_else(|| corrupt("conflicting execution disappeared"))?;
        let existing = decode_execution(existing.record)?;
        if existing.operation != new.operation
            || existing.input != new.input
            || existing.run_at != new.run_at
        {
            return Err(conflict("execution identity reused"));
        }
        Ok(existing)
    }

    pub(super) async fn create_execution_inner(
        &self,
        new: NewExecution,
    ) -> Result<ExecutionRecord, StoreError> {
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let record = self
            .create_execution_in_transaction(&mut transaction, new)
            .await?;
        transaction.commit().await.map_err(store_error)?;
        Ok(record)
    }

    pub(super) async fn load_execution_inner(
        &self,
        id: ExecutionId,
    ) -> Result<Option<ExecutionRecord>, StoreError> {
        let row = sqlx::query_as!(
            ExecutionRow,
            "SELECT record AS \"record: Json<ExecutionRecord>\"
			FROM resumable_executions
			WHERE id = $1",
            *id.as_uuid(),
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(store_error)?;
        row.map(|row| decode_execution(row.record)).transpose()
    }

    pub(super) async fn persist_execution(
        transaction: &mut Transaction<'_, Postgres>,
        record: &ExecutionRecord,
    ) -> Result<(), StoreError> {
        let status = serde_json::to_value(record.status)
            .map_err(|error| corrupt(error.to_string()))?
            .as_str()
            .ok_or_else(|| corrupt("invalid execution status"))?
            .to_owned();
        let worker_id = record.lease.as_ref().map(|lease| *lease.owner.as_uuid());
        let lease_token = record.lease.as_ref().map(|lease| *lease.token.as_uuid());
        let claim_id = record.lease.as_ref().map(|lease| *lease.claim_id.as_uuid());
        let lease_duration_ms = record
            .lease
            .as_ref()
            .map(|lease| i64::try_from(lease.duration_ms))
            .transpose()
            .map_err(|_| conflict("lease duration exceeds PostgreSQL range"))?;
        let lease_expires_at = record.lease.as_ref().map(|lease| lease.expires_at.0);
        let failure_class = record
            .failure
            .as_ref()
            .map(|failure| {
                serde_json::to_value(failure.class).map_err(|error| corrupt(error.to_string()))
            })
            .transpose()?
            .map(|value| value.as_str().unwrap_or_default().to_owned());
        let failure_diagnostic = record
            .failure
            .as_ref()
            .and_then(|failure| failure.diagnostic.clone());
        let result_codec = record.result.as_ref().map(StoredValue::codec);
        let result_schema_version = record
            .result
            .as_ref()
            .map(|result| i64::from(result.schema_version()));
        let result_bytes = record.result.as_ref().map(StoredValue::bytes);
        let revision = i64::try_from(record.revision.0)
            .map_err(|_| corrupt("execution revision exceeds PostgreSQL range"))?;
        let generation = i64::try_from(record.fencing_generation)
            .map_err(|_| corrupt("fencing generation exceeds PostgreSQL range"))?;
        let updated_at = record.updated_at.0;
        let started_at = record.started_at.map(|at| at.0);
        let completed_at = record.completed_at.map(|at| at.0);
        let cancellation_requested_at = record.cancellation_requested_at.map(|at| at.0);
        let snapshot = encode(record)?;
        sqlx::query!(
            "UPDATE resumable_executions SET
				status = $2,
				revision = $3,
				fencing_generation = $4,
				worker_id = $5,
				lease_token = $6,
				claim_id = $7,
				lease_duration_ms = $8,
				lease_expires_at =
					(TIMESTAMPTZ 'epoch' + $9::bigint * INTERVAL '1 millisecond'),
				failure_class = $10,
				failure_diagnostic = $11,
				result_codec = $12,
				result_schema_version = $13,
				result_bytes = $14,
				updated_at = (TIMESTAMPTZ 'epoch' + $15::bigint * INTERVAL '1 millisecond'),
				started_at = (TIMESTAMPTZ 'epoch' + $16::bigint * INTERVAL '1 millisecond'),
				completed_at = (TIMESTAMPTZ 'epoch' + $17::bigint * INTERVAL '1 millisecond'),
				cancellation_requested_at =
					(TIMESTAMPTZ 'epoch' + $18::bigint * INTERVAL '1 millisecond'),
				record = $19
			WHERE id = $1",
            *record.id.as_uuid(),
            status,
            revision,
            generation,
            worker_id,
            lease_token,
            claim_id,
            lease_duration_ms,
            lease_expires_at,
            failure_class,
            failure_diagnostic,
            result_codec,
            result_schema_version,
            result_bytes,
            updated_at,
            started_at,
            completed_at,
            cancellation_requested_at,
            snapshot,
        )
        .execute(&mut **transaction)
        .await
        .map_err(store_error)?;
        Ok(())
    }
}
