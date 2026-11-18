use crate::Timestamp;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> Timestamp;
}

#[derive(Clone)]
pub struct ManualClock(Arc<Mutex<Timestamp>>);

impl Default for ManualClock {
    fn default() -> Self {
        Self::new(Timestamp(0))
    }
}

impl ManualClock {
    pub fn new(now: Timestamp) -> Self {
        Self(Arc::new(Mutex::new(now)))
    }

    pub fn advance(&self, millis: i64) {
        let mut now = self.0.lock().expect("manual clock poisoned");
        now.0 = now.0.saturating_add(millis);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Timestamp {
        *self.0.lock().expect("manual clock poisoned")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FaultPoint {
    CreateExecution,
    AcquireExecution,
    BeginStep,
    CompleteStep,
    FailStep,
    CompleteExecution,
    FailExecution,
    Cancellation,
    Heartbeat,
    Purge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultMode {
    BeforeCommit,
    AfterCommit,
}

/// One-shot fault injection. `AfterCommit` models a committed write whose response was lost.
#[derive(Clone, Default)]
pub struct FaultInjector(Arc<Mutex<HashMap<FaultPoint, FaultMode>>>);

impl FaultInjector {
    pub fn inject_once(&self, point: FaultPoint, mode: FaultMode) {
        self.0
            .lock()
            .expect("fault injector poisoned")
            .insert(point, mode);
    }

    pub(super) fn take(&self, point: FaultPoint, mode: FaultMode) -> bool {
        let mut faults = self.0.lock().expect("fault injector poisoned");
        if faults.get(&point) == Some(&mode) {
            faults.remove(&point);
            true
        } else {
            false
        }
    }
}
