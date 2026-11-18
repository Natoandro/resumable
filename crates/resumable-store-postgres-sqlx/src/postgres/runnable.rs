use super::*;
use resumable::{ExecutionId, OperationKey};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunnableExecution {
    pub id: ExecutionId,
    pub operation: OperationKey,
}

struct RunnableRow {
    id: Uuid,
    operation_name: String,
    operation_version: i64,
}

struct UnsupportedOperationRow {
    operation_name: String,
    operation_version: i64,
}

/// Find eligible executions supported by this worker release. Claiming remains a separate,
/// atomic Store operation; multiple workers may discover the same ID safely.
pub async fn runnable_executions(
    pool: &PgPool,
    supported: &[OperationKey],
    limit: u32,
) -> Result<Vec<RunnableExecution>, StoreError> {
    if limit == 0 || limit > 1000 {
        return Err(conflict(
            "runnable discovery limit must be between 1 and 1000",
        ));
    }
    if supported.is_empty() {
        return Ok(Vec::new());
    }
    let names = supported
        .iter()
        .map(|key| key.name().to_owned())
        .collect::<Vec<_>>();
    let versions = supported
        .iter()
        .map(|key| i64::from(key.version()))
        .collect::<Vec<_>>();
    let limit = i64::from(limit);
    let rows = sqlx::query_as!(
        RunnableRow,
        "WITH clock AS (SELECT resumable_now() AS now)
		SELECT e.id, e.operation_name, e.operation_version
		FROM resumable_executions e
		CROSS JOIN clock
		WHERE (
			e.status IN ('pending', 'failed')
			OR (e.status = 'running' AND e.lease_expires_at <= clock.now)
		)
		AND (e.run_at IS NULL OR e.run_at <= clock.now)
		AND NOT EXISTS (
			SELECT 1
			FROM resumable_steps s
			WHERE s.execution_id = e.id
				AND s.next_attempt_at > clock.now
		)
		AND NOT EXISTS (
			SELECT 1
			FROM resumable_steps s
			WHERE s.execution_id = e.id
				AND s.status = 'failed'
				AND s.failure_class <> 'retryable'
		)
		AND EXISTS (
			SELECT 1
			FROM unnest($1::text[], $2::bigint[]) AS supported(name, version)
			WHERE supported.name = e.operation_name
				AND supported.version = e.operation_version
		)
		ORDER BY e.created_at, e.id
		LIMIT $3",
        &names,
        &versions,
        limit,
    )
    .fetch_all(pool)
    .await
    .map_err(store_error)?;
    rows.into_iter()
        .map(|row| {
            Ok(RunnableExecution {
                id: ExecutionId::from_uuid(row.id),
                operation: OperationKey::new(
                    row.operation_name,
                    u32::try_from(row.operation_version)
                        .map_err(|_| corrupt("operation version is outside the supported range"))?,
                )
                .map_err(|error| corrupt(error.to_string()))?,
            })
        })
        .collect()
}

/// List operation versions with eligible work that this release cannot dispatch.
pub async fn unsupported_runnable_operations(
    pool: &PgPool,
    supported: &[OperationKey],
) -> Result<Vec<OperationKey>, StoreError> {
    let names = supported
        .iter()
        .map(|key| key.name().to_owned())
        .collect::<Vec<_>>();
    let versions = supported
        .iter()
        .map(|key| i64::from(key.version()))
        .collect::<Vec<_>>();
    let rows = sqlx::query_as!(
        UnsupportedOperationRow,
        "WITH clock AS (SELECT resumable_now() AS now)
		SELECT DISTINCT e.operation_name, e.operation_version
		FROM resumable_executions e
		CROSS JOIN clock
		WHERE (
			e.status IN ('pending', 'failed')
			OR (e.status = 'running' AND e.lease_expires_at <= clock.now)
		)
		AND (e.run_at IS NULL OR e.run_at <= clock.now)
		AND NOT EXISTS (
			SELECT 1
			FROM resumable_steps s
			WHERE s.execution_id = e.id
				AND s.next_attempt_at > clock.now
		)
		AND NOT EXISTS (
			SELECT 1
			FROM resumable_steps s
			WHERE s.execution_id = e.id
				AND s.status = 'failed'
				AND s.failure_class <> 'retryable'
		)
		AND NOT EXISTS (
			SELECT 1
			FROM unnest($1::text[], $2::bigint[]) AS supported(name, version)
			WHERE supported.name = e.operation_name
				AND supported.version = e.operation_version
		)
		ORDER BY e.operation_name, e.operation_version
		",
        &names,
        &versions,
    )
    .fetch_all(pool)
    .await
    .map_err(store_error)?;
    rows.into_iter()
        .map(|row| {
            OperationKey::new(
                row.operation_name,
                u32::try_from(row.operation_version)
                    .map_err(|_| corrupt("operation version is outside the supported range"))?,
            )
            .map_err(|error| corrupt(error.to_string()))
        })
        .collect()
}
