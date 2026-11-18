//! Backend-neutral semantic scenarios for Store adapters.
use crate::*;

fn assertion(ok: bool, message: &str) -> Result<(), StoreError> {
    if ok {
        Ok(())
    } else {
        Err(StoreError::Corrupt(format!(
            "conformance failure: {message}"
        )))
    }
}

mod cancellation;
mod fencing;
mod identity;
mod ownership;
mod transactions;

/// Run core identity, claim, step, retry, completion, and cancellation scenarios against a Store.
/// Adapter test suites can call this with a fresh store instance for each run.
pub async fn run_store_scenarios<S: Store>(store: &S) -> Result<(), StoreError> {
    identity::run(store).await?;
    fencing::run(store).await?;
    ownership::run(store).await?;
    transactions::run(store).await?;
    cancellation::run(store).await
}
