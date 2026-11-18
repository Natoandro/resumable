use super::*;
use crate::{Engine, EngineConfig, FailedExecutionPolicy, JsonCodec, RunError, ValueCodec};
use std::sync::atomic::{AtomicU32, Ordering};

mod cancellation;
mod claims;
mod fencing;
mod reconciliation;
mod takeover_store;

fn operation() -> OperationKey {
    OperationKey::new("ownership-regression", 1).unwrap()
}

fn create(store: &MemoryStore, id: ExecutionId) {
    block_on(store.create_execution(
        NewExecution::new(id, operation(), Timestamp(0)).with_input(JsonCodec.encode(&()).unwrap()),
    ))
    .unwrap();
}

fn claim(store: &MemoryStore, id: ExecutionId, duration: u64) -> LeaseGuard {
    match block_on(store.acquire_execution(id, WorkerId::new(), MutationId::new(), duration))
        .unwrap()
    {
        ClaimOutcome::Claimed(claim) => LeaseGuard {
            execution_id: id,
            worker: claim.worker,
            token: claim.token,
            generation: claim.generation,
            expected_revision: claim.record.revision,
        },
        _ => panic!("execution should be claimable"),
    }
}

fn poll_once<F: std::future::Future>(future: std::pin::Pin<&mut F>) -> Poll<F::Output> {
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    future.poll(&mut Context::from_waker(&waker))
}
