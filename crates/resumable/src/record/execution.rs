use serde::{Deserialize, Serialize};

use super::{FailureRecord, Timestamp};
use crate::{
    ExecutionId, ExecutionMetadata, LeaseToken, OperationKey, Revision, StateTransitionError,
    StoredValue, WorkerId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}
impl ExecutionStatus {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub owner: WorkerId,
    /// Invocation identity used to recover an ambiguous claim without admitting a second runner.
    pub claim_id: crate::MutationId,
    /// Original acquisition duration; subsequent heartbeats do not change the claim request.
    pub duration_ms: u64,
    pub token: LeaseToken,
    pub generation: u64,
    pub expires_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionRecord {
    pub id: ExecutionId,
    pub operation: OperationKey,
    pub input: Option<StoredValue>,
    pub result: Option<StoredValue>,
    pub metadata: ExecutionMetadata,
    pub status: ExecutionStatus,
    pub revision: Revision,
    /// Last issued generation; retained when the active lease is cleared.
    pub fencing_generation: u64,
    pub created_at: Timestamp,
    #[serde(default)]
    pub run_at: Option<Timestamp>,
    pub updated_at: Timestamp,
    pub started_at: Option<Timestamp>,
    pub completed_at: Option<Timestamp>,
    pub cancellation_requested_at: Option<Timestamp>,
    pub lease: Option<Lease>,
    pub failure: Option<FailureRecord>,
}

impl ExecutionRecord {
    pub fn new(id: ExecutionId, operation: OperationKey, at: Timestamp) -> Self {
        Self::new_with_input(id, operation, None, ExecutionMetadata::default(), at)
    }
    pub fn new_with_input(
        id: ExecutionId,
        operation: OperationKey,
        input: Option<StoredValue>,
        metadata: ExecutionMetadata,
        at: Timestamp,
    ) -> Self {
        Self {
            id,
            operation,
            input,
            result: None,
            metadata,
            status: ExecutionStatus::Pending,
            revision: Revision(0),
            fencing_generation: 0,
            created_at: at,
            run_at: None,
            updated_at: at,
            started_at: None,
            completed_at: None,
            cancellation_requested_at: None,
            lease: None,
            failure: None,
        }
    }
    pub fn start(&mut self, lease: Lease, at: Timestamp) -> Result<(), StateTransitionError> {
        self.require(ExecutionStatus::Pending, ExecutionStatus::Running)?;
        self.bump_revision()?;
        self.status = ExecutionStatus::Running;
        self.started_at.get_or_insert(at);
        self.fencing_generation = lease.generation;
        self.lease = Some(lease);
        self.updated_at = at;
        Ok(())
    }
    pub fn resume(&mut self, at: Timestamp) -> Result<(), StateTransitionError> {
        self.require(ExecutionStatus::Failed, ExecutionStatus::Pending)?;
        self.bump_revision()?;
        self.status = ExecutionStatus::Pending;
        self.failure = None;
        self.updated_at = at;
        Ok(())
    }
    pub fn request_cancellation(&mut self, at: Timestamp) -> Result<(), StateTransitionError> {
        if self.status.is_terminal() || self.cancellation_requested_at.is_some() {
            return Ok(());
        }
        self.bump_revision()?;
        self.cancellation_requested_at = Some(at);
        if self.status == ExecutionStatus::Pending {
            self.status = ExecutionStatus::Cancelled;
            self.completed_at = Some(at);
        }
        self.updated_at = at;
        Ok(())
    }
    pub fn finish(&mut self, at: Timestamp) -> Result<(), StateTransitionError> {
        self.finish_with_result(None, at)
    }
    pub fn finish_with_result(
        &mut self,
        result: Option<StoredValue>,
        at: Timestamp,
    ) -> Result<(), StateTransitionError> {
        self.require(ExecutionStatus::Running, ExecutionStatus::Completed)?;
        self.bump_revision()?;
        self.status = if self.cancellation_requested_at.is_some() {
            ExecutionStatus::Cancelled
        } else {
            ExecutionStatus::Completed
        };
        self.result = if self.status == ExecutionStatus::Completed {
            result
        } else {
            None
        };
        self.completed_at = Some(at);
        self.lease = None;
        self.updated_at = at;
        Ok(())
    }
    pub fn fail(&mut self, failure: FailureRecord) -> Result<(), StateTransitionError> {
        self.require(ExecutionStatus::Running, ExecutionStatus::Failed)?;
        self.bump_revision()?;
        self.status = if self.cancellation_requested_at.is_some() {
            ExecutionStatus::Cancelled
        } else {
            ExecutionStatus::Failed
        };
        self.updated_at = failure.occurred_at;
        self.completed_at = Some(failure.occurred_at);
        self.failure = Some(failure);
        self.lease = None;
        Ok(())
    }
    fn require(
        &self,
        from: ExecutionStatus,
        to: ExecutionStatus,
    ) -> Result<(), StateTransitionError> {
        if self.status != from {
            return Err(StateTransitionError::Illegal {
                record: "execution",
                from: format!("{:?}", self.status),
                to: format!("{to:?}"),
            });
        }
        Ok(())
    }
    fn bump_revision(&mut self) -> Result<(), StateTransitionError> {
        self.revision.0 = self
            .revision
            .0
            .checked_add(1)
            .ok_or(StateTransitionError::RevisionOverflow)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
