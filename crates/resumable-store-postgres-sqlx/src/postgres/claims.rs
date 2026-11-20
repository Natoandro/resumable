use super::*;
use resumable::{
    ClaimOutcome, ExecutionClaim, ExecutionId, ExecutionRecord, ExecutionStatus, Lease, LeaseToken,
    MutationId, StepRecord, StepStatus, Timestamp, WorkerId,
};

struct ExecutionRow {
    record: Json<ExecutionRecord>,
}

struct StepRow {
    record: Json<StepRecord>,
}

impl PostgresStore {
    pub(super) async fn acquire_execution_inner(
        &self,
        id: ExecutionId,
        worker: WorkerId,
        claim_id: MutationId,
        lease_duration_ms: u64,
    ) -> Result<ClaimOutcome, StoreError> {
        self.acquire_execution_policy_inner(
            id,
            worker,
            claim_id,
            lease_duration_ms,
            resumable::FailedExecutionPolicy::Resume,
        )
        .await
    }

    pub(super) async fn acquire_execution_policy_inner(
        &self,
        id: ExecutionId,
        worker: WorkerId,
        claim_id: MutationId,
        lease_duration_ms: u64,
        policy: resumable::FailedExecutionPolicy,
    ) -> Result<ClaimOutcome, StoreError> {
        if lease_duration_ms == 0 || lease_duration_ms > i64::MAX as u64 {
            return Err(conflict("lease duration is outside the PostgreSQL range"));
        }
        let mut transaction = self.pool.begin().await.map_err(store_error)?;
        let row = sqlx::query_as!(
            ExecutionRow,
            "SELECT record AS \"record: Json<ExecutionRecord>\"
			FROM resumable_executions
			WHERE id = $1
			FOR UPDATE",
            *id.as_uuid(),
        )
        .fetch_optional(&mut *transaction)
        .await
        .map_err(store_error)?;
        let Some(row) = row else {
            transaction.commit().await.map_err(store_error)?;
            return Ok(ClaimOutcome::NotFound);
        };
        let mut record = decode_execution(row.record)?;
        if record.status.is_terminal() {
            transaction.commit().await.map_err(store_error)?;
            return Ok(ClaimOutcome::Terminal(record));
        }
        if record.status == ExecutionStatus::Failed
            && policy == resumable::FailedExecutionPolicy::Reject
        {
            transaction.commit().await.map_err(store_error)?;
            return Ok(ClaimOutcome::Terminal(record));
        }
        let now = Timestamp(transaction_now(&mut transaction).await?);
        if record.run_at.is_some_and(|at| at > now) {
            transaction.commit().await.map_err(store_error)?;
            return Ok(ClaimOutcome::Busy);
        }
        let live_lease = record.lease.as_ref().and_then(|lease| {
            (lease.expires_at > now).then_some((
                lease.owner,
                lease.claim_id,
                lease.duration_ms,
                lease.token,
                lease.generation,
            ))
        });
        if let Some((owner, old_claim_id, old_duration, token, generation)) = live_lease {
            let result = if owner == worker && old_claim_id == claim_id {
                if old_duration != lease_duration_ms {
                    return Err(conflict("claim ID reused with different duration"));
                }
                ClaimOutcome::Claimed(ExecutionClaim {
                    record,
                    worker,
                    token,
                    generation,
                })
            } else {
                ClaimOutcome::Busy
            };
            transaction.commit().await.map_err(store_error)?;
            return Ok(result);
        }
        if record.cancellation_requested_at.is_none() {
            let rows = sqlx::query_as!(
                StepRow,
                "SELECT record AS \"record: Json<StepRecord>\"
				FROM resumable_steps
				WHERE execution_id = $1",
                *id.as_uuid(),
            )
            .fetch_all(&mut *transaction)
            .await
            .map_err(store_error)?;
            for row in rows {
                let step = decode_step(row.record)?;
                if step.next_attempt_at.is_some_and(|at| at > now) {
                    transaction.commit().await.map_err(store_error)?;
                    return Ok(ClaimOutcome::Busy);
                }
                if step.status == StepStatus::Failed
                    && step
                        .failure
                        .as_ref()
                        .is_some_and(|failure| failure.class != resumable::FailureClass::Retryable)
                {
                    transaction.commit().await.map_err(store_error)?;
                    return Ok(ClaimOutcome::Terminal(record));
                }
            }
        }
        let generation = record
            .fencing_generation
            .checked_add(1)
            .ok_or_else(|| corrupt("lease fencing generation overflow"))?;
        let token = LeaseToken::new();
        let lease = Lease {
            owner: worker,
            claim_id,
            duration_ms: lease_duration_ms,
            token,
            generation,
            expires_at: Timestamp(
                now.0
                    .saturating_add(i64::try_from(lease_duration_ms).unwrap_or(i64::MAX)),
            ),
        };
        if record.status == ExecutionStatus::Failed {
            record
                .resume(now)
                .map_err(|error| conflict(error.to_string()))?;
            record
                .start(lease.clone(), now)
                .map_err(|error| conflict(error.to_string()))?;
        } else if record.status == ExecutionStatus::Pending {
            record
                .start(lease.clone(), now)
                .map_err(|error| conflict(error.to_string()))?;
        } else if record.status == ExecutionStatus::Running {
            record.revision.0 = record
                .revision
                .0
                .checked_add(1)
                .ok_or_else(|| corrupt("execution revision overflow"))?;
            record.fencing_generation = generation;
            record.lease = Some(lease.clone());
            record.updated_at = now;
        } else {
            return Err(conflict("execution cannot be claimed in its current state"));
        }
        PostgresStore::persist_execution(&mut transaction, &record).await?;
        transaction.commit().await.map_err(store_error)?;
        Ok(ClaimOutcome::Claimed(ExecutionClaim {
            record,
            worker,
            token,
            generation,
        }))
    }
}
