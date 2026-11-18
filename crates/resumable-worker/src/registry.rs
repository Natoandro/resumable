use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use resumable::OperationKey;
use serde_json::Value;
use tokio::sync::RwLock;

pub type OperationHandlerFuture = Pin<Box<dyn Future<Output = Result<Value, String>> + Send>>;

pub type OperationHandler<Context, ExecutionContext> =
    Arc<dyn Fn(Value, Context, ExecutionContext) -> OperationHandlerFuture + Send + Sync>;

pub struct OperationRegistry<Context, ExecutionContext> {
    operations: RwLock<HashMap<OperationKey, OperationHandler<Context, ExecutionContext>>>,
}

impl<Context, ExecutionContext> Default for OperationRegistry<Context, ExecutionContext> {
    fn default() -> Self {
        Self {
            operations: RwLock::new(HashMap::new()),
        }
    }
}

impl<Context, ExecutionContext> std::fmt::Debug for OperationRegistry<Context, ExecutionContext> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OperationRegistry")
            .finish_non_exhaustive()
    }
}

impl<Context, ExecutionContext> OperationRegistry<Context, ExecutionContext> {
    pub async fn register<F, Fut>(&self, key: OperationKey, handler: F) -> Result<(), String>
    where
        F: Fn(Value, Context, ExecutionContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, String>> + Send + 'static,
    {
        let mut operations = self.operations.write().await;
        if operations.contains_key(&key) {
            return Err(format!("resumable operation {key:?} is already registered"));
        }
        operations.insert(
            key,
            Arc::new(move |input, context, execution_context| {
                Box::pin(handler(input, context, execution_context))
            }),
        );
        Ok(())
    }

    pub async fn keys(&self) -> Vec<OperationKey> {
        let mut keys = self
            .operations
            .read()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        keys.sort_by(|left, right| {
            left.name()
                .cmp(right.name())
                .then(left.version().cmp(&right.version()))
        });
        keys
    }

    pub async fn handler(
        &self,
        key: &OperationKey,
    ) -> Option<OperationHandler<Context, ExecutionContext>> {
        self.operations.read().await.get(key).cloned()
    }
}

#[cfg(test)]
mod tests;
