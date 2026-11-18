use super::super::*;
use std::{
    collections::VecDeque,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;

pub(super) struct FakeSource {
    candidates: Mutex<VecDeque<RunnableExecution>>,
    unsupported: Vec<OperationKey>,
    pub unsupported_calls: AtomicUsize,
}

impl FakeSource {
    pub(super) fn new(candidates: impl IntoIterator<Item = RunnableExecution>) -> Self {
        Self::with_unsupported(candidates, Vec::new())
    }

    pub(super) fn with_unsupported(
        candidates: impl IntoIterator<Item = RunnableExecution>,
        unsupported: Vec<OperationKey>,
    ) -> Self {
        Self {
            candidates: Mutex::new(candidates.into_iter().collect()),
            unsupported,
            unsupported_calls: AtomicUsize::new(0),
        }
    }
}

impl RunnableSource for FakeSource {
    fn unsupported_operations<'a>(
        &'a self,
        _supported: &'a [OperationKey],
    ) -> WorkerFuture<'a, Result<Vec<OperationKey>, String>> {
        Box::pin(async move {
            self.unsupported_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.unsupported.clone())
        })
    }

    fn runnable_executions<'a>(
        &'a self,
        _supported: &'a [OperationKey],
        limit: u32,
    ) -> WorkerFuture<'a, Result<Vec<RunnableExecution>, String>> {
        Box::pin(async move {
            let mut candidates = self.candidates.lock().unwrap();
            let count = usize::min(limit as usize, candidates.len());
            Ok(candidates.drain(..count).collect())
        })
    }
}

pub(super) struct FakeRunner {
    operation: OperationKey,
    pub calls: AtomicUsize,
    active: AtomicUsize,
    pub max_active: AtomicUsize,
    delay: Duration,
    mode: RunnerMode,
    pub started: Notify,
}

#[derive(Clone, Copy)]
pub(super) enum RunnerMode {
    Succeed,
    Fail,
    Panic,
}

impl FakeRunner {
    pub(super) fn new(delay: Duration) -> Self {
        Self::with_mode(delay, RunnerMode::Succeed)
    }

    pub(super) fn with_mode(delay: Duration, mode: RunnerMode) -> Self {
        Self {
            operation: OperationKey::new("fake", 1).unwrap(),
            calls: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            delay,
            mode,
            started: Notify::new(),
        }
    }
}

impl ExecutionRunner for FakeRunner {
    fn supported_operations(&self) -> WorkerFuture<'_, Vec<OperationKey>> {
        Box::pin(async move { vec![self.operation.clone()] })
    }

    fn run(
        &self,
        _id: ExecutionId,
        _operation: OperationKey,
    ) -> WorkerFuture<'_, Result<(), RunError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.mode {
                RunnerMode::Fail => return Err(RunError::Operation("expected failure".into())),
                RunnerMode::Panic => panic!("expected runner panic"),
                RunnerMode::Succeed => {}
            }
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            self.started.notify_one();
            tokio::time::sleep(self.delay).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

pub(super) fn candidate(id: ExecutionId, operation: OperationKey) -> RunnableExecution {
    RunnableExecution { id, operation }
}

pub(super) fn settings(concurrency: usize) -> WorkerSettings {
    WorkerSettings {
        enabled: true,
        concurrency,
        poll_interval: Duration::from_millis(5),
        shutdown_timeout: Duration::from_millis(50),
    }
}

pub(super) async fn wait_for_calls(runner: &FakeRunner, count: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while runner.calls.load(Ordering::SeqCst) < count {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("worker should run discovered candidates");
}
