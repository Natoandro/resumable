mod support;

use resumable::{Engine, ExecutionId, OperationKey, Timestamp, reference::MemoryStore};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Clone, Default)]
struct SnapshotStorage(Arc<Mutex<HashMap<String, Vec<u8>>>>);

impl SnapshotStorage {
    fn put_if_matches_or_absent(&self, key: &str, snapshot: &[u8]) -> Result<(), String> {
        let mut objects = self.0.lock().map_err(|_| "snapshot storage poisoned")?;
        match objects.get(key) {
            Some(existing) if existing == snapshot => Ok(()),
            Some(_) => Err("snapshot key already contains different bytes".into()),
            None => {
                objects.insert(key.into(), snapshot.into());
                Ok(())
            }
        }
    }
}

fn main() {
    let store = MemoryStore::new(Timestamp(0));
    let engine = Engine::new(store);
    let storage = SnapshotStorage::default();
    let id = ExecutionId::new();
    let key = format!("snapshots/{id}/database.snapshot");
    let snapshot = b"consistent point-in-time snapshot".to_vec();
    let result = support::block_on(engine.start_with_id_and_context(
        id,
        OperationKey::new("database-backup", 1).unwrap(),
        snapshot.clone(),
        move |input, context| {
            let storage = storage.clone();
            let key = key.clone();
            async move {
                let frozen = context
                    .step("freeze-snapshot", move |_| {
                        let input = input.clone();
                        async move { Ok::<_, String>(input) }
                    })
                    .await
                    .map_err(|error| error.to_string())?;
                context
                    .step("store-snapshot", move |_| {
                        let storage = storage.clone();
                        let key = key.clone();
                        let frozen = frozen.clone();
                        async move {
                            storage.put_if_matches_or_absent(&key, &frozen)?;
                            Ok::<_, String>(key)
                        }
                    })
                    .await
                    .map_err(|error| error.to_string())
            }
        },
    ))
    .unwrap();
    println!("stored verified backup at {result}");
}
