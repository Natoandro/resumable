use super::*;
use crate::{Engine, EngineConfig, FailedExecutionPolicy, StepOptions};
use std::sync::{Arc, Mutex};

fn engine(store: MemoryStore) -> Engine<MemoryStore> {
    Engine::with_config(
        store,
        crate::JsonCodec,
        EngineConfig {
            lease_duration: std::time::Duration::from_millis(1),
            failed_execution_policy: FailedExecutionPolicy::Resume,
            ..EngineConfig::default()
        },
    )
}

fn operation() -> OperationKey {
    OperationKey::new("named-step-faults", 1).unwrap()
}

mod cancellation;
mod effects;
mod heartbeat;
mod inspection;
mod retry;
mod security;
mod steps;
