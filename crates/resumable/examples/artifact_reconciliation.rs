mod support;

use resumable::{
    Engine, EngineConfig, ExecutionId, FailedExecutionPolicy, JsonCodec, OperationKey, StepContext,
    Timestamp, reference::MemoryStore,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Clone, Default)]
struct ObjectStore(Arc<Mutex<HashMap<String, Vec<u8>>>>);

impl ObjectStore {
    fn put_if_absent(&self, key: &str, value: &[u8]) {
        self.0
            .lock()
            .expect("object store poisoned")
            .entry(key.into())
            .or_insert_with(|| value.into());
    }

    fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.0
            .lock()
            .expect("object store poisoned")
            .get(key)
            .cloned()
    }

    fn object_count(&self) -> usize {
        self.0.lock().expect("object store poisoned").len()
    }
}

async fn workflow(
    context: resumable::ExecutionContext<MemoryStore, JsonCodec>,
    objects: ObjectStore,
    key: String,
    crash_after_put: bool,
) -> Result<String, String> {
    context
        .step("upload-report", move |step: StepContext| {
            let objects = objects.clone();
            let key = key.clone();
            async move {
                let payload = b"generated report";
                if objects.get(&key).as_deref() != Some(payload) {
                    objects.put_if_absent(&key, payload);
                }
                if crash_after_put {
                    panic!("simulated process loss after object write");
                }
                let _stable_provider_key = step.idempotency_key();
                Ok::<_, String>(key)
            }
        })
        .await
        .map_err(|error| error.to_string())
}

fn main() {
    let store = MemoryStore::new(Timestamp(0));
    let engine = Engine::with_config(
        store.clone(),
        resumable::JsonCodec,
        EngineConfig {
            lease_duration: std::time::Duration::from_millis(5),
            failed_execution_policy: FailedExecutionPolicy::Resume,
            ..EngineConfig::default()
        },
    );
    let objects = ObjectStore::default();
    let id = ExecutionId::new();
    let operation = OperationKey::new("artifact-report", 1).unwrap();
    let outer_calls = Arc::new(Mutex::new(0));
    std::panic::set_hook(Box::new(|_| {}));
    let first_calls = outer_calls.clone();
    let first_objects = objects.clone();
    let first_key = format!("artifacts/{id}/report.bin");
    let interrupted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        support::block_on(engine.start_with_id_and_context(
            id,
            operation.clone(),
            (),
            move |_, context| {
                *first_calls.lock().unwrap() += 1;
                workflow(context, first_objects, first_key, true)
            },
        ))
    }));
    assert!(interrupted.is_err());
    assert_eq!(objects.object_count(), 1);

    store.clock().advance(10);
    let resumed_engine = Engine::with_config(
        store.clone(),
        resumable::JsonCodec,
        EngineConfig {
            lease_duration: std::time::Duration::from_millis(5),
            failed_execution_policy: FailedExecutionPolicy::Resume,
            ..EngineConfig::default()
        },
    );
    let resume_calls = outer_calls.clone();
    let resume_objects = objects.clone();
    let key = format!("artifacts/{id}/report.bin");
    let result = support::block_on(
        resumed_engine.resume_with_context::<(), String, String, _, _>(
            id,
            &operation,
            move |_, context| {
                *resume_calls.lock().unwrap() += 1;
                workflow(context, resume_objects, key, false)
            },
        ),
    )
    .unwrap();
    assert_eq!(result, format!("artifacts/{id}/report.bin"));
    assert_eq!(*outer_calls.lock().unwrap(), 2);
    assert_eq!(objects.object_count(), 1);
    println!("reconciled existing artifact at {result}");
}
