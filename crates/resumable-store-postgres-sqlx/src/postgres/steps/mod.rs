mod begin;
mod complete;
mod failure;
mod persistence;
mod progress;

use super::*;
use resumable::{StepName, StepRecord};

struct StepRow {
    record: Json<StepRecord>,
}

struct StepStateRow {
    record: Json<StepRecord>,
    active_attempt_generation: Option<i64>,
}

impl PostgresStore {
    pub(super) async fn load_step_inner(
        &self,
        execution: resumable::ExecutionId,
        name: &StepName,
    ) -> Result<Option<StepRecord>, StoreError> {
        let row = sqlx::query_as!(
            StepRow,
            "SELECT record AS \"record: Json<StepRecord>\"
			FROM resumable_steps
			WHERE execution_id = $1 AND name = $2",
            *execution.as_uuid(),
            name.as_str(),
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(store_error)?;
        row.map(|row| decode_step(row.record)).transpose()
    }
}
