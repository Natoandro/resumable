mod support;

use resumable::{
    Engine, ExecutionId, IdempotencyKey, OperationKey, StepName, Timestamp, reference::MemoryStore,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Clone, Default)]
struct FakeProvider(Arc<Mutex<HashMap<String, (String, u64)>>>);

impl FakeProvider {
    const RETENTION_MS: u64 = 86_400_000;

    fn send_once_at(
        &self,
        provider_key: &str,
        recipient: &str,
        now_ms: u64,
    ) -> Result<String, String> {
        let mut deliveries = self.0.lock().map_err(|_| "provider state poisoned")?;
        if let Some((id, sent_at)) = deliveries.get(provider_key) {
            if now_ms.saturating_sub(*sent_at) < Self::RETENTION_MS {
                return Ok(id.clone());
            }
        }
        let id = format!("message-at-{now_ms}-to-{recipient}");
        deliveries.insert(provider_key.into(), (id.clone(), now_ms));
        Ok(id)
    }
}

fn main() {
    let engine = Engine::new(MemoryStore::new(Timestamp(0)));
    let provider = FakeProvider::default();
    let provider_for_run = provider.clone();
    let id = ExecutionId::new();
    let result = support::block_on(engine.start_with_id_and_context(
        id,
        OperationKey::new("send-notice", 1).unwrap(),
        "person@example.invalid".to_owned(),
        move |recipient, context| {
            let provider = provider_for_run.clone();
            async move {
                context
                    .step("deliver-notice", move |step| {
                        let provider = provider.clone();
                        let recipient = recipient.clone();
                        async move {
                            provider.send_once_at(step.idempotency_key().as_str(), &recipient, 0)
                        }
                    })
                    .await
                    .map_err(|error| error.to_string())
            }
        },
    ))
    .unwrap();
    println!("provider accepted {result}");
    let key = IdempotencyKey::for_step(id, &StepName::new("deliver-notice").unwrap());
    assert_eq!(
        provider
            .send_once_at(key.as_str(), "person@example.invalid", 1)
            .unwrap(),
        result
    );
    let after_retention = provider
        .send_once_at(
            key.as_str(),
            "person@example.invalid",
            FakeProvider::RETENTION_MS + 1,
        )
        .unwrap();
    assert_ne!(after_retention, result);
    println!("the fake provider can deduplicate only within its retention window");
}
