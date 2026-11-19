use crate::{
    BeginStep, ClaimOutcome, CompleteExecution, CompleteStep, ExecutionId, ExecutionRecord,
    ExecutionStatus, FailureClass, FailureRecord, IdempotencyKey, JsonCodec, LeaseGuard,
    MutationId, NewExecution, OperationKey, StepId, StepName, Store, StoreError, StoredValue,
    Timestamp, ValueCodec, ValueError, WorkerId,
};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tracing::Instrument;

mod completion;
mod config;
mod context;
mod create;
mod inspection;
mod progress;
mod resume;
mod retry;
mod run;
mod step_body;
mod step_context;
mod step_context_builder;
mod steps;
pub use config::{EngineConfig, FailedExecutionPolicy, RunError, StepRunError};
pub use inspection::{ExecutionInspection, InspectedStep};
pub use retry::{Backoff, RetryPolicy, RetryPolicyError, StepOptions};
pub use step_context::StepContext;

/// Access to durable steps and lease management for the active execution.
///
/// Run step calls sequentially. Cloning the context shares the same lease.
pub struct ExecutionContext<S, C> {
    store: Arc<S>,
    codec: Arc<C>,
    lease: Arc<Mutex<LeaseState>>,
    lease_duration: Duration,
    heartbeat_interval: Duration,
    capture_error_diagnostics: bool,
}

struct LeaseState {
    guard: LeaseGuard,
    lost: Option<String>,
}

impl<S, C> Clone for ExecutionContext<S, C> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            codec: self.codec.clone(),
            lease: self.lease.clone(),
            lease_duration: self.lease_duration,
            heartbeat_interval: self.heartbeat_interval,
            capture_error_diagnostics: self.capture_error_diagnostics,
        }
    }
}

/// Direct execution runner. Applications own worker startup and dispatch on restart.
pub struct Engine<S, C = JsonCodec> {
    store: Arc<S>,
    codec: Arc<C>,
    config: EngineConfig,
}

impl<S: Store> Engine<S, JsonCodec> {
    /// Create an engine with the JSON codec and default execution settings.
    pub fn new(store: S) -> Self {
        Self::with_config(store, JsonCodec, EngineConfig::default())
    }
}

impl<S, C> Engine<S, C> {
    /// Create an engine with an explicit codec and execution settings.
    pub fn with_config(store: S, codec: C, config: EngineConfig) -> Self {
        Self {
            store: Arc::new(store),
            codec: Arc::new(codec),
            config,
        }
    }

    /// Borrow the underlying persistence adapter.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Borrow the codec used for inputs, step results, and execution results.
    pub fn codec(&self) -> &C {
        &self.codec
    }

    fn engine_diagnostics(&self, message: String) -> Option<String> {
        self.config.capture_error_diagnostics.then_some(message)
    }
}

fn terminal_result(record: &ExecutionRecord) -> Result<Option<StoredValue>, RunError> {
    match record.status {
        ExecutionStatus::Completed => record.result.clone().map(Some).ok_or(RunError::Terminal),
        ExecutionStatus::Cancelled => Err(RunError::Cancelled),
        _ => Ok(None),
    }
}

fn check_operation(stored: &OperationKey, requested: &OperationKey) -> Result<(), RunError> {
    if stored.name() != requested.name() {
        return Err(RunError::OperationConflict);
    }
    if stored.version() != requested.version() {
        return Err(RunError::UnsupportedOperationVersion {
            name: stored.name().to_owned(),
            stored: stored.version(),
            requested: requested.version(),
        });
    }
    Ok(())
}

fn now() -> Timestamp {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    Timestamp(millis.min(i64::MAX as u128) as i64)
}
