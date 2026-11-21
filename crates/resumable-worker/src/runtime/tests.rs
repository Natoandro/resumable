use super::*;
mod fakes;

use fakes::{FakeRunner, FakeSource, RunnerMode, candidate, settings, wait_for_calls};
use std::{
    future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;

struct BlockingDiscovery {
    entered: Notify,
}

impl RunnableSource for BlockingDiscovery {
    fn unsupported_operations<'a>(
        &'a self,
        _supported: &'a [OperationKey],
        _failed_execution_policy: FailedExecutionPolicy,
    ) -> WorkerFuture<'a, Result<Vec<OperationKey>, String>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn runnable_executions<'a>(
        &'a self,
        _supported: &'a [OperationKey],
        _failed_execution_policy: FailedExecutionPolicy,
        _limit: u32,
    ) -> WorkerFuture<'a, Result<Vec<RunnableExecution>, String>> {
        Box::pin(async move {
            self.entered.notify_one();
            future::pending::<Result<Vec<RunnableExecution>, String>>().await
        })
    }
}

#[tokio::test]
async fn shutdown_interrupts_blocked_discovery() {
    let source = Arc::new(BlockingDiscovery {
        entered: Notify::new(),
    });
    let runner = Arc::new(FakeRunner::new(Duration::ZERO));
    let worker = start(source.clone(), runner, settings(1)).unwrap();
    tokio::time::timeout(Duration::from_secs(1), source.entered.notified())
        .await
        .expect("worker should enter discovery");
    tokio::time::timeout(Duration::from_secs(1), worker.shutdown())
        .await
        .expect("shutdown should interrupt discovery");
}

struct RepeatingSource {
    panic_id: ExecutionId,
    other_id: ExecutionId,
    panic_candidate_returned: AtomicBool,
}

impl RunnableSource for RepeatingSource {
    fn unsupported_operations<'a>(
        &'a self,
        _supported: &'a [OperationKey],
        _failed_execution_policy: FailedExecutionPolicy,
    ) -> WorkerFuture<'a, Result<Vec<OperationKey>, String>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn runnable_executions<'a>(
        &'a self,
        _supported: &'a [OperationKey],
        _failed_execution_policy: FailedExecutionPolicy,
        limit: u32,
    ) -> WorkerFuture<'a, Result<Vec<RunnableExecution>, String>> {
        Box::pin(async move {
            let operation = OperationKey::new("fake", 1).unwrap();
            let mut candidates = Vec::new();
            if !self.panic_candidate_returned.swap(true, Ordering::SeqCst) {
                candidates.push(candidate(self.panic_id, operation.clone()));
            }
            candidates.push(candidate(self.other_id, operation));
            candidates.truncate(limit as usize);
            Ok(candidates)
        })
    }
}

struct PanicOnceRunner {
    panic_id: ExecutionId,
    other_calls: AtomicUsize,
}

impl ExecutionRunner for PanicOnceRunner {
    fn supported_operations(&self) -> WorkerFuture<'_, Vec<OperationKey>> {
        Box::pin(async { vec![OperationKey::new("fake", 1).unwrap()] })
    }

    fn run(
        &self,
        id: ExecutionId,
        _operation: OperationKey,
    ) -> WorkerFuture<'_, Result<(), RunError>> {
        Box::pin(async move {
            if id == self.panic_id {
                panic!("expected runner panic");
            }
            self.other_calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(())
        })
    }
}

#[tokio::test]
async fn task_panic_only_clears_its_own_active_id() {
    let panic_id = ExecutionId::new();
    let other_id = ExecutionId::new();
    let source = Arc::new(RepeatingSource {
        panic_id,
        other_id,
        panic_candidate_returned: AtomicBool::new(false),
    });
    let runner = Arc::new(PanicOnceRunner {
        panic_id,
        other_calls: AtomicUsize::new(0),
    });
    let worker = start(source, runner.clone(), settings(2)).unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while runner.other_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    })
    .await
    .expect("worker should dispatch the unrelated execution");
    assert_eq!(runner.other_calls.load(Ordering::SeqCst), 1);
    worker.shutdown().await;
}

#[tokio::test]
async fn worker_runs_fake_source_and_respects_capacity() {
    let operation = OperationKey::new("fake", 1).unwrap();
    let source = Arc::new(FakeSource::new(
        (0..6).map(|_| candidate(ExecutionId::new(), operation.clone())),
    ));
    let runner = Arc::new(FakeRunner::new(Duration::from_millis(20)));
    let worker = start(source.clone(), runner.clone(), settings(2)).unwrap();
    wait_for_calls(&runner, 6).await;
    worker.shutdown().await;
    assert_eq!(runner.max_active.load(Ordering::SeqCst), 2);
    assert!(source.unsupported_calls.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn worker_ignores_duplicate_active_candidate_ids() {
    let id = ExecutionId::new();
    let operation = OperationKey::new("fake", 1).unwrap();
    let source = Arc::new(FakeSource::new([
        candidate(id, operation.clone()),
        candidate(id, operation),
    ]));
    let runner = Arc::new(FakeRunner::new(Duration::from_millis(20)));
    let worker = start(source, runner.clone(), settings(2)).unwrap();
    wait_for_calls(&runner, 1).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    worker.shutdown().await;
    assert_eq!(runner.calls.load(Ordering::SeqCst), 1);
}

struct SameCandidateSource(RunnableExecution);

impl RunnableSource for SameCandidateSource {
    fn unsupported_operations<'a>(
        &'a self,
        _supported: &'a [OperationKey],
        _failed_execution_policy: FailedExecutionPolicy,
    ) -> WorkerFuture<'a, Result<Vec<OperationKey>, String>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn runnable_executions<'a>(
        &'a self,
        _supported: &'a [OperationKey],
        _failed_execution_policy: FailedExecutionPolicy,
        _limit: u32,
    ) -> WorkerFuture<'a, Result<Vec<RunnableExecution>, String>> {
        Box::pin(async move { Ok(vec![self.0.clone()]) })
    }
}

#[tokio::test]
async fn successful_completion_allows_same_id_to_be_dispatched_again() {
    let id = ExecutionId::new();
    let operation = OperationKey::new("fake", 1).unwrap();
    let source = Arc::new(SameCandidateSource(candidate(id, operation)));
    let runner = Arc::new(FakeRunner::new(Duration::from_millis(1)));
    let worker = start(source, runner.clone(), settings(1)).unwrap();
    wait_for_calls(&runner, 2).await;
    worker.shutdown().await;
    assert!(runner.calls.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn disabled_worker_does_not_start() {
    let source = Arc::new(FakeSource::new([]));
    let runner = Arc::new(FakeRunner::new(Duration::ZERO));
    assert!(start(source.clone(), runner.clone(), WorkerSettings::default()).is_none());
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(source.unsupported_calls.load(Ordering::SeqCst), 0);
    assert_eq!(runner.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unknown_operations_are_reported_without_dispatch() {
    let unknown = OperationKey::new("not-registered", 7).unwrap();
    let source = Arc::new(FakeSource::with_unsupported([], vec![unknown]));
    let runner = Arc::new(FakeRunner::new(Duration::ZERO));
    let worker = start(source.clone(), runner.clone(), settings(1)).unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while source.unsupported_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("worker should inspect unsupported operations");
    worker.shutdown().await;
    assert_eq!(runner.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn runner_errors_and_panics_allow_same_id_to_be_dispatched_again() {
    for mode in [RunnerMode::Fail, RunnerMode::Panic] {
        let operation = OperationKey::new("fake", 1).unwrap();
        let source = Arc::new(SameCandidateSource(candidate(
            ExecutionId::new(),
            operation,
        )));
        let runner = Arc::new(FakeRunner::with_mode(Duration::ZERO, mode));
        let worker = start(source, runner.clone(), settings(1)).unwrap();
        wait_for_calls(&runner, 2).await;
        worker.shutdown().await;
    }
}

#[tokio::test]
async fn shutdown_deadline_aborts_a_stuck_runner() {
    let operation = OperationKey::new("fake", 1).unwrap();
    let source = Arc::new(FakeSource::new([candidate(ExecutionId::new(), operation)]));
    let runner = Arc::new(FakeRunner::new(Duration::from_secs(10)));
    let worker = start(source, runner.clone(), settings(1)).unwrap();
    tokio::time::timeout(Duration::from_secs(1), runner.started.notified())
        .await
        .expect("runner should start");
    tokio::time::timeout(Duration::from_secs(1), worker.shutdown())
        .await
        .expect("shutdown should respect its deadline");
    assert_eq!(runner.calls.load(Ordering::SeqCst), 1);
}
