use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    ExecutionId, ExecutionMetadata, ExecutionRecord, FailureRecord, LeaseToken, OperationKey,
    RetryPolicyRecord, Revision, StepName, StoredValue, Timestamp, WorkerId,
};

/// Stable identifier for retrying an operation after an ambiguous storage response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MutationId(Uuid);

impl MutationId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for MutationId {
    fn default() -> Self {
        Self::new()
    }
}

/// Immutable create parameters. Compatible retries have equal operation key and exact optional
/// input envelope and scheduled start; metadata and `created_at` are ignored after the first successful creation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct NewExecution {
    pub id: ExecutionId,
    pub operation: OperationKey,
    pub input: Option<StoredValue>,
    pub metadata: ExecutionMetadata,
    pub created_at: Timestamp,
    /// Earliest time the execution can be claimed. None makes it eligible immediately.
    #[serde(default)]
    pub run_at: Option<Timestamp>,
}

impl NewExecution {
    pub fn new(id: ExecutionId, operation: OperationKey, created_at: Timestamp) -> Self {
        Self {
            id,
            operation,
            created_at,
            input: None,
            metadata: ExecutionMetadata::default(),
            run_at: None,
        }
    }

    pub fn with_input(mut self, input: StoredValue) -> Self {
        self.input = Some(input);
        self
    }

    pub fn with_metadata(mut self, metadata: ExecutionMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_run_at(mut self, run_at: Timestamp) -> Self {
        self.run_at = Some(run_at);
        self
    }
}

impl From<NewExecution> for ExecutionRecord {
    fn from(value: NewExecution) -> Self {
        let mut record = Self::new_with_input(
            value.id,
            value.operation,
            value.input,
            value.metadata,
            value.created_at,
        );
        record.run_at = value.run_at;
        record
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClaimOutcome {
    Claimed(ExecutionClaim),
    Busy,
    Terminal(ExecutionRecord),
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionClaim {
    pub record: ExecutionRecord,
    pub worker: WorkerId,
    pub token: LeaseToken,
    pub generation: u64,
}

/// Proof required for every leased mutation. Store checks owner, token, generation, expiry,
/// and expected revision together in the same atomic write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseGuard {
    pub execution_id: ExecutionId,
    pub worker: WorkerId,
    pub token: LeaseToken,
    pub generation: u64,
    pub expected_revision: Revision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeginStep {
    pub lease: LeaseGuard,
    pub name: StepName,
    pub retry_policy: RetryPolicyRecord,
    pub mutation_id: MutationId,
    pub at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompleteStep {
    pub lease: LeaseGuard,
    pub name: StepName,
    pub expected_attempt: u32,
    pub mutation_id: MutationId,
    pub result: Option<StoredValue>,
    pub at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailStep {
    pub lease: LeaseGuard,
    pub name: StepName,
    pub expected_attempt: u32,
    pub mutation_id: MutationId,
    pub failure: FailureRecord,
    pub retry_at: Option<Timestamp>,
    pub retry_policy: RetryPolicyRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompleteExecution {
    pub lease: LeaseGuard,
    pub mutation_id: MutationId,
    pub result: Option<StoredValue>,
    pub at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailExecution {
    pub lease: LeaseGuard,
    pub mutation_id: MutationId,
    pub failure: FailureRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestCancellation {
    pub execution_id: ExecutionId,
    pub at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Progress {
    pub current: Option<u64>,
    pub total: Option<u64>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgressUpdate {
    pub lease: LeaseGuard,
    pub step: Option<StepName>,
    pub progress: Option<Progress>,
    pub lease_duration_ms: u64,
    pub at: Timestamp,
}
