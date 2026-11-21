use std::{collections::HashMap, fmt::Display, future::Future, pin::Pin, sync::Arc};

use resumable::OperationKey;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::sync::RwLock;

/// An asynchronous handler result with JSON output or an application error.
pub type OperationHandlerFuture = Pin<Box<dyn Future<Output = Result<Value, String>> + Send>>;

/// A shareable callback taking JSON input and the two caller-defined contexts.
pub type OperationHandler<Context, ExecutionContext> =
    Arc<dyn Fn(Value, Context, ExecutionContext) -> OperationHandlerFuture + Send + Sync>;

/// An asynchronous registry keyed by exact operation name and version.
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
    /// Register a handler. Returns an error if the same key is already registered.
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

    /// Register a handler that accepts typed input and returns typed output.
    ///
    /// On dispatch, the registry deserializes the JSON input before calling the
    /// handler and serializes its successful output back to JSON. Conversion
    /// failures and handler errors are returned as strings by the registered
    /// handler. Registering an already registered key returns an error.
    pub async fn register_typed<I, O, E, F, Fut>(
        &self,
        key: OperationKey,
        handler: F,
    ) -> Result<(), String>
    where
        I: DeserializeOwned,
        O: Serialize,
        E: Display,
        F: Fn(I, Context, ExecutionContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O, E>> + Send + 'static,
    {
        self.register(key, move |input, context, execution_context| {
            let future = serde_json::from_value(input)
                .map(|input| handler(input, context, execution_context))
                .map_err(|error| format!("failed to deserialize operation input: {error}"));
            async move {
                let output = future?.await.map_err(|error| error.to_string())?;
                serde_json::to_value(output)
                    .map_err(|error| format!("failed to serialize operation output: {error}"))
            }
        })
        .await
    }

    /// Return the registered keys sorted by name and then version.
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

    /// Look up a handler for the exact operation key.
    pub async fn handler(
        &self,
        key: &OperationKey,
    ) -> Option<OperationHandler<Context, ExecutionContext>> {
        self.operations.read().await.get(key).cloned()
    }
}

#[cfg(test)]
mod tests;
