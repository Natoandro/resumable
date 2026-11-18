mod claims;
mod executions;
mod lifecycle;
mod migration;
mod runnable;
mod steps;
mod store;

use resumable::{ExecutionRecord, StepRecord, StoreError};
use sha2::{Digest, Sha256};
use sqlx::{Acquire, PgPool, Postgres, Transaction, types::Json};
use uuid::Uuid;

pub use migration::{SCHEMA_VERSION, migrate, validate_schema, validate_schema_version};
pub use runnable::RunnableExecution;
pub use store::PostgresStore;

fn store_error(error: sqlx::Error) -> StoreError {
    match error {
        sqlx::Error::ColumnDecode { .. } | sqlx::Error::Decode(_) => {
            StoreError::Corrupt(error.to_string())
        }
        sqlx::Error::Database(ref database) if database.code().as_deref() == Some("23514") => {
            StoreError::Conflict(error.to_string())
        }
        sqlx::Error::Database(ref database) if database.code().as_deref() == Some("23505") => {
            StoreError::Conflict(error.to_string())
        }
        _ => StoreError::Unavailable(error.to_string()),
    }
}

fn conflict(message: impl Into<String>) -> StoreError {
    StoreError::Conflict(message.into())
}

fn corrupt(message: impl Into<String>) -> StoreError {
    StoreError::Corrupt(message.into())
}

fn encode<T: serde::Serialize>(value: &T) -> Result<serde_json::Value, StoreError> {
    serde_json::to_value(value)
        .map_err(|error| corrupt(format!("cannot serialize persisted record: {error}")))
}

fn decode_execution(value: Json<ExecutionRecord>) -> Result<ExecutionRecord, StoreError> {
    Ok(value.0)
}

fn decode_step(value: Json<StepRecord>) -> Result<StepRecord, StoreError> {
    Ok(value.0)
}

async fn transaction_now(transaction: &mut Transaction<'_, Postgres>) -> Result<i64, StoreError> {
    let row = sqlx::query!(
        "SELECT floor(extract(epoch FROM resumable_now()) * 1000)::bigint AS \"now_ms!\""
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(store_error)?;
    Ok(row.now_ms)
}

#[derive(sqlx::FromRow)]
struct ReceiptRow {
    mutation_kind: String,
    request_fingerprint: Vec<u8>,
    outcome: Json<serde_json::Value>,
}

async fn replay_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    execution_id: Uuid,
    mutation_id: Uuid,
    kind: &str,
    request: &serde_json::Value,
) -> Result<Option<Json<serde_json::Value>>, StoreError> {
    let fingerprint = fingerprint(kind, request)?;
    let row = sqlx::query_as!(
        ReceiptRow,
        "SELECT mutation_kind, request_fingerprint,
			outcome AS \"outcome: Json<serde_json::Value>\"
		FROM resumable_mutation_receipts
		WHERE execution_id = $1 AND mutation_id = $2",
        execution_id,
        mutation_id,
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(store_error)?;
    let Some(row) = row else {
        return Ok(None);
    };
    if row.mutation_kind != kind || row.request_fingerprint != fingerprint {
        return Err(conflict("mutation ID reused for different request"));
    }
    Ok(Some(row.outcome))
}

async fn save_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    execution_id: Uuid,
    mutation_id: Uuid,
    kind: &str,
    request: &serde_json::Value,
    outcome: &serde_json::Value,
) -> Result<(), StoreError> {
    let fingerprint = fingerprint(kind, request)?;
    sqlx::query!(
        "INSERT INTO resumable_mutation_receipts (
			execution_id, mutation_id, mutation_kind, request_fingerprint, outcome
		) VALUES ($1, $2, $3, $4, $5)",
        execution_id,
        mutation_id,
        kind,
        &fingerprint[..],
        outcome.clone(),
    )
    .execute(&mut **transaction)
    .await
    .map_err(store_error)?;
    Ok(())
}

fn fingerprint(kind: &str, value: &serde_json::Value) -> Result<Vec<u8>, StoreError> {
    let request = serde_json::json!({ "kind": kind, "request": value });
    let bytes = serde_json::to_vec(&request).map_err(|error| corrupt(error.to_string()))?;
    Ok(Sha256::digest(bytes).to_vec())
}

fn request_json<T: serde::Serialize>(value: &T) -> Result<serde_json::Value, StoreError> {
    serde_json::to_value(value).map_err(|error| corrupt(error.to_string()))
}

fn outcome_json<T: serde::Serialize>(value: &T) -> Result<serde_json::Value, StoreError> {
    serde_json::to_value(value).map_err(|error| corrupt(error.to_string()))
}

fn outcome_from<T: serde::de::DeserializeOwned>(
    value: Json<serde_json::Value>,
) -> Result<T, StoreError> {
    serde_json::from_value(value.0).map_err(|error| corrupt(error.to_string()))
}

fn check_lease(
    record: &ExecutionRecord,
    guard: &resumable::LeaseGuard,
    now: resumable::Timestamp,
) -> Result<(), StoreError> {
    let lease = record
        .lease
        .as_ref()
        .ok_or_else(|| conflict("execution has no active lease"))?;
    if record.revision != guard.expected_revision
        || lease.owner != guard.worker
        || lease.token != guard.token
        || lease.generation != guard.generation
        || lease.expires_at <= now
    {
        return Err(conflict("stale or expired execution lease"));
    }
    Ok(())
}

struct ExecutionSnapshot {
    record: Json<ExecutionRecord>,
}

async fn lock_execution(
    transaction: &mut Transaction<'_, Postgres>,
    id: resumable::ExecutionId,
) -> Result<ExecutionRecord, StoreError> {
    let row = sqlx::query_as!(
        ExecutionSnapshot,
        "SELECT record AS \"record: Json<ExecutionRecord>\"
		FROM resumable_executions
		WHERE id = $1
		FOR UPDATE",
        *id.as_uuid(),
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(store_error)?
    .ok_or_else(|| conflict("execution not found"))?;
    decode_execution(row.record)
}

fn bump_revision(record: &mut ExecutionRecord, at: resumable::Timestamp) -> Result<(), StoreError> {
    record.revision.0 = record
        .revision
        .0
        .checked_add(1)
        .ok_or_else(|| corrupt("execution revision overflow"))?;
    record.updated_at = at;
    Ok(())
}
