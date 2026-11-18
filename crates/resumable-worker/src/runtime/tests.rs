use super::*;
mod fakes;

use fakes::{FakeRunner, FakeSource, RunnerMode, candidate, settings, wait_for_calls};
use std::{sync::atomic::Ordering, time::Duration};

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
async fn runner_errors_and_panics_do_not_stop_the_worker() {
    for mode in [RunnerMode::Fail, RunnerMode::Panic] {
        let operation = OperationKey::new("fake", 1).unwrap();
        let source = Arc::new(FakeSource::new([candidate(ExecutionId::new(), operation)]));
        let runner = Arc::new(FakeRunner::with_mode(Duration::ZERO, mode));
        let worker = start(source, runner.clone(), settings(1)).unwrap();
        wait_for_calls(&runner, 1).await;
        worker.shutdown().await;
        assert_eq!(runner.calls.load(Ordering::SeqCst), 1);
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
