use super::*;

const MIGRATIONS: &[(&str, &str)] = &[
    (
        "initial resumable store",
        include_str!("../../migrations/0001_resumable.sql"),
    ),
    (
        "scheduled starts",
        include_str!("../../migrations/0002_scheduled_starts.sql"),
    ),
];
pub const SCHEMA_VERSION: u32 = MIGRATIONS.len() as u32;
const MIGRATION_LOCK_KEY: i64 = 0x52534d42504144;

/// Apply the adapter's independent, transactional migration sequence.
pub async fn migrate(pool: &PgPool) -> Result<(), StoreError> {
    let mut connection = pool.acquire().await.map_err(store_error)?;
    let mut transaction = connection.begin().await.map_err(store_error)?;
    sqlx::query!("SELECT pg_advisory_xact_lock($1)", MIGRATION_LOCK_KEY)
        .execute(&mut *transaction)
        .await
        .map_err(store_error)?;
    migrate_locked(&mut transaction).await?;
    transaction.commit().await.map_err(store_error)
}

async fn migrate_locked(transaction: &mut Transaction<'_, Postgres>) -> Result<(), StoreError> {
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS resumable_migrations (
			version BIGINT PRIMARY KEY,
			description TEXT NOT NULL,
			checksum BYTEA NOT NULL CHECK (octet_length(checksum) = 32),
			applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
		)",
    )
    .execute(&mut **transaction)
    .await
    .map_err(store_error)?;
    let applied =
        sqlx::query!("SELECT version, checksum FROM resumable_migrations ORDER BY version")
            .fetch_all(&mut **transaction)
            .await
            .map_err(store_error)?;
    if applied.len() > MIGRATIONS.len() {
        return Err(corrupt(
            "adapter migration history contains an unknown version",
        ));
    }
    for (index, (description, sql)) in MIGRATIONS.iter().enumerate() {
        let version = index as i64 + 1;
        let checksum = Sha256::digest(sql.as_bytes());
        if let Some(row) = applied.get(index) {
            if row.version != version || row.checksum.as_slice() != checksum.as_slice() {
                return Err(corrupt(
                    "applied adapter migration version or checksum does not match",
                ));
            }
            continue;
        }
        sqlx::raw_sql(sql)
            .execute(&mut **transaction)
            .await
            .map_err(store_error)?;
        sqlx::query!(
            "INSERT INTO resumable_migrations (version, description, checksum)
		    VALUES ($1, $2, $3)",
            version,
            *description,
            &checksum[..],
        )
        .execute(&mut **transaction)
        .await
        .map_err(store_error)?;
    }
    Ok(())
}

/// Confirm the adapter schema and its full migration history are installed.
pub async fn validate_schema(pool: &PgPool) -> Result<(), StoreError> {
    let row = sqlx::query!(
        "SELECT
			to_regclass('resumable_executions') IS NOT NULL AS \"executions!\",
			to_regclass('resumable_steps') IS NOT NULL AS \"steps!\",
			to_regclass('resumable_attempts') IS NOT NULL AS \"attempts!\",
			to_regclass('resumable_progress') IS NOT NULL AS \"progress!\",
			to_regclass('resumable_mutation_receipts') IS NOT NULL AS \"receipts!\",
			to_regclass('resumable_migrations') IS NOT NULL AS \"history!\""
    )
    .fetch_one(pool)
    .await
    .map_err(store_error)?;
    if !(row.executions && row.steps && row.attempts && row.progress && row.receipts && row.history)
    {
        return Err(corrupt("adapter schema is incomplete; run migrate first"));
    }
    let applied =
        sqlx::query!("SELECT version, checksum FROM resumable_migrations ORDER BY version")
            .fetch_all(pool)
            .await
            .map_err(store_error)?;
    if applied.len() != MIGRATIONS.len() {
        return Err(corrupt(
            "adapter schema version is unsupported; run migrate first",
        ));
    }
    for (index, (row, (_, sql))) in applied.iter().zip(MIGRATIONS).enumerate() {
        let checksum = Sha256::digest(sql.as_bytes());
        if row.version != index as i64 + 1 || row.checksum.as_slice() != checksum.as_slice() {
            return Err(corrupt(
                "adapter migration version or checksum does not match",
            ));
        }
    }
    Ok(())
}

/// Validate that the installed adapter schema exactly matches a worker release's schema version.
pub async fn validate_schema_version(pool: &PgPool, expected: u32) -> Result<(), StoreError> {
    if expected != SCHEMA_VERSION {
        return Err(corrupt(format!(
            "worker requires adapter schema {expected}, this release supports {SCHEMA_VERSION}"
        )));
    }
    validate_schema(pool).await
}
