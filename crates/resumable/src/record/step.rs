use serde::{Deserialize, Serialize};

use super::Timestamp;
use crate::{ExecutionId, StateTransitionError, StepId, StepName, StoredValue};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}
impl StepStatus {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    Retryable,
    Permanent,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BackoffRecord {
    None,
    Fixed {
        delay_ms: u64,
    },
    Exponential {
        initial_ms: u64,
        factor_bits: u64,
        max_ms: u64,
        jitter: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicyRecord {
    pub max_attempts: u32,
    pub backoff: BackoffRecord,
    pub retryable: bool,
}

impl Default for RetryPolicyRecord {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            backoff: BackoffRecord::None,
            retryable: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureRecord {
    pub class: FailureClass,
    pub diagnostic: Option<String>,
    pub occurred_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepRecord {
    pub id: StepId,
    pub execution_id: ExecutionId,
    pub name: StepName,
    pub status: StepStatus,
    pub attempt: u32,
    pub result: Option<StoredValue>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub started_at: Option<Timestamp>,
    pub completed_at: Option<Timestamp>,
    pub next_attempt_at: Option<Timestamp>,
    pub failure: Option<FailureRecord>,
    pub retry_policy: Option<RetryPolicyRecord>,
}

impl StepRecord {
    pub fn new(id: StepId, execution_id: ExecutionId, name: StepName, at: Timestamp) -> Self {
        Self {
            id,
            execution_id,
            name,
            status: StepStatus::Pending,
            attempt: 0,
            result: None,
            created_at: at,
            updated_at: at,
            started_at: None,
            completed_at: None,
            next_attempt_at: None,
            failure: None,
            retry_policy: None,
        }
    }
    /// Begin an attempt after initial scheduling, retry eligibility, or lease recovery.
    pub fn begin_attempt(&mut self, at: Timestamp) -> Result<u32, StateTransitionError> {
        if !matches!(
            self.status,
            StepStatus::Pending | StepStatus::Failed | StepStatus::Running
        ) {
            return Err(StateTransitionError::Illegal {
                record: "step",
                from: format!("{:?}", self.status),
                to: "Running".into(),
            });
        }
        self.attempt = self
            .attempt
            .checked_add(1)
            .ok_or(StateTransitionError::AttemptOverflow)?;
        self.status = StepStatus::Running;
        self.started_at = Some(at);
        self.completed_at = None;
        self.next_attempt_at = None;
        self.failure = None;
        self.updated_at = at;
        Ok(self.attempt)
    }

    pub fn set_retry_policy(
        &mut self,
        policy: RetryPolicyRecord,
    ) -> Result<(), StateTransitionError> {
        if let Some(existing) = &self.retry_policy {
            if existing != &policy {
                return Err(StateTransitionError::Illegal {
                    record: "step retry policy",
                    from: "persisted policy".into(),
                    to: "different policy".into(),
                });
            }
        } else {
            self.retry_policy = Some(policy);
        }
        Ok(())
    }
    pub fn complete(&mut self, at: Timestamp) -> Result<(), StateTransitionError> {
        self.complete_with_result(None, at)
    }
    pub fn complete_with_result(
        &mut self,
        result: Option<StoredValue>,
        at: Timestamp,
    ) -> Result<(), StateTransitionError> {
        self.require_running(StepStatus::Completed)?;
        self.status = StepStatus::Completed;
        self.result = result;
        self.completed_at = Some(at);
        self.updated_at = at;
        Ok(())
    }
    pub fn fail(
        &mut self,
        failure: FailureRecord,
        retry_at: Option<Timestamp>,
    ) -> Result<(), StateTransitionError> {
        self.require_running(StepStatus::Failed)?;
        self.status = if failure.class == FailureClass::Cancelled {
            StepStatus::Cancelled
        } else {
            StepStatus::Failed
        };
        self.updated_at = failure.occurred_at;
        self.completed_at = Some(failure.occurred_at);
        self.next_attempt_at = retry_at;
        self.failure = Some(failure);
        Ok(())
    }
    fn require_running(&self, to: StepStatus) -> Result<(), StateTransitionError> {
        if self.status != StepStatus::Running {
            return Err(StateTransitionError::Illegal {
                record: "step",
                from: format!("{:?}", self.status),
                to: format!("{to:?}"),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn time(n: i64) -> Timestamp {
        Timestamp(n)
    }
    #[test]
    fn attempts_increase_and_completed_steps_cannot_restart() {
        let mut step = StepRecord::new(
            StepId::new(),
            ExecutionId::new(),
            StepName::new("upload").unwrap(),
            time(0),
        );
        assert_eq!(step.begin_attempt(time(1)).unwrap(), 1);
        step.fail(
            FailureRecord {
                class: FailureClass::Retryable,
                diagnostic: None,
                occurred_at: time(2),
            },
            Some(time(3)),
        )
        .unwrap();
        assert_eq!(step.begin_attempt(time(3)).unwrap(), 2);
        step.complete(time(4)).unwrap();
        assert!(step.begin_attempt(time(5)).is_err());
    }
}
