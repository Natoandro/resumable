//! Deterministic in-memory Store for examples, tests, and adapter conformance work.
mod clock;
mod store;
#[cfg(test)]
mod tests;
use crate::store::*;
use crate::*;
pub use clock::{Clock, FaultInjector, FaultMode, FaultPoint, ManualClock};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct State {
    executions: HashMap<ExecutionId, ExecutionRecord>,
    steps: HashMap<(ExecutionId, StepName), StepRecord>,
    step_generations: HashMap<(ExecutionId, StepName), u64>,
    progress: HashMap<(ExecutionId, StepName), Progress>,
    receipts: HashMap<MutationId, (MutationRequest, MutationValue)>,
}

#[derive(Clone, PartialEq, Eq)]
enum MutationRequest {
    Begin(BeginStep),
    CompleteStep(CompleteStep),
    FailStep(FailStep),
    CompleteExecution(CompleteExecution),
    FailExecution(FailExecution),
}

#[derive(Clone)]
enum MutationValue {
    Step(StepRecord),
    Execution(ExecutionRecord),
}

/// An in-memory implementation intended for development and Store conformance tests.
#[derive(Clone)]
pub struct MemoryStore<C = ManualClock> {
    state: Arc<Mutex<State>>,
    clock: C,
    faults: FaultInjector,
}

impl MemoryStore<ManualClock> {
    pub fn new(now: Timestamp) -> Self {
        Self::with_clock(ManualClock::new(now))
    }
}

impl<C: Clock> MemoryStore<C> {
    pub fn with_clock(clock: C) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            clock,
            faults: FaultInjector::default(),
        }
    }

    pub fn clock(&self) -> &C {
        &self.clock
    }

    pub fn faults(&self) -> &FaultInjector {
        &self.faults
    }

    fn fault_before(&self, point: FaultPoint) -> Result<(), StoreError> {
        if self.faults.take(point, FaultMode::BeforeCommit) {
            Err(StoreError::Unavailable(format!(
                "injected before {point:?}"
            )))
        } else {
            Ok(())
        }
    }

    fn fault_after<T>(&self, point: FaultPoint, value: T) -> Result<T, StoreError> {
        if self.faults.take(point, FaultMode::AfterCommit) {
            Err(StoreError::Unavailable(format!("injected after {point:?}")))
        } else {
            Ok(value)
        }
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, State>, StoreError> {
        self.state
            .lock()
            .map_err(|_| StoreError::Unavailable("memory store poisoned".into()))
    }

    fn check_lease(state: &State, guard: &LeaseGuard, now: Timestamp) -> Result<(), StoreError> {
        let record = state
            .executions
            .get(&guard.execution_id)
            .ok_or_else(|| StoreError::Conflict("execution not found".into()))?;
        let lease = record
            .lease
            .as_ref()
            .ok_or_else(|| StoreError::Conflict("execution has no active lease".into()))?;
        if record.revision != guard.expected_revision
            || lease.owner != guard.worker
            || lease.token != guard.token
            || lease.generation != guard.generation
            || lease.expires_at <= now
        {
            return Err(StoreError::Conflict(
                "stale or expired execution lease".into(),
            ));
        }
        Ok(())
    }

    fn bump(record: &mut ExecutionRecord, at: Timestamp) -> Result<(), StoreError> {
        record.revision.0 = record
            .revision
            .0
            .checked_add(1)
            .ok_or_else(|| StoreError::Corrupt("execution revision overflow".into()))?;
        record.updated_at = at;
        Ok(())
    }

    fn ensure_revision_capacity(state: &State, id: ExecutionId) -> Result<(), StoreError> {
        let revision = state
            .executions
            .get(&id)
            .ok_or_else(|| StoreError::Conflict("execution not found".into()))?
            .revision
            .0;
        if revision == u64::MAX {
            return Err(StoreError::Corrupt("execution revision overflow".into()));
        }
        Ok(())
    }

    fn replay<T>(
        state: &State,
        id: MutationId,
        request: MutationRequest,
        convert: impl FnOnce(MutationValue) -> Option<T>,
    ) -> Result<Option<T>, StoreError> {
        let Some((previous, value)) = state.receipts.get(&id) else {
            return Ok(None);
        };
        if *previous != request {
            return Err(StoreError::Conflict(
                "mutation ID reused for different request".into(),
            ));
        }
        convert(value.clone())
            .map(Some)
            .ok_or_else(|| StoreError::Corrupt("mutation receipt has wrong result type".into()))
    }
}
