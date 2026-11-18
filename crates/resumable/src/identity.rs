use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize, de};
use sha2::{Digest, Sha256};
use std::{fmt, str::FromStr};
use thiserror::Error;
use uuid::Uuid;

macro_rules! uuid_identity {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);
        impl $name {
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }
            pub const fn from_uuid(value: Uuid) -> Self {
                Self(value)
            }
            pub const fn as_uuid(&self) -> &Uuid {
                &self.0
            }
        }
        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
        impl From<Uuid> for $name {
            fn from(value: Uuid) -> Self {
                Self(value)
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

uuid_identity!(ExecutionId);
uuid_identity!(StepId);
uuid_identity!(WorkerId);
uuid_identity!(LeaseToken);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Revision(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct StepName(String);

impl<'de> Deserialize<'de> for StepName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("step name must contain between 1 and 200 UTF-8 bytes")]
pub struct InvalidStepName;

impl StepName {
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidStepName> {
        let value = value.into();
        if value.is_empty() || value.len() > 200 {
            return Err(InvalidStepName);
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StepName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl TryFrom<String> for StepName {
    type Error = InvalidStepName;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl FromStr for StepName {
    type Err = InvalidStepName;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct OperationKey {
    name: String,
    version: u32,
}

impl<'de> Deserialize<'de> for OperationKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct WireOperationKey {
            name: String,
            version: u32,
        }

        let value = WireOperationKey::deserialize(deserializer)?;
        Self::new(value.name, value.version).map_err(de::Error::custom)
    }
}

impl OperationKey {
    pub fn new(name: impl Into<String>, version: u32) -> Result<Self, InvalidOperationKey> {
        let name = name.into();
        if name.is_empty() || name.len() > 200 {
            return Err(InvalidOperationKey);
        }
        Ok(Self { name, version })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn version(&self) -> u32 {
        self.version
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("operation name must contain between 1 and 200 UTF-8 bytes")]
pub struct InvalidOperationKey;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    pub fn for_step(execution: ExecutionId, name: &StepName) -> Self {
        Self(encode_key(
            b"resumable:step:v1",
            &[execution.as_uuid().as_bytes(), name.as_str().as_bytes()],
        ))
    }
    pub fn for_attempt(execution: ExecutionId, name: &StepName, attempt: u32) -> Self {
        let attempt = attempt.to_be_bytes();
        Self(encode_key(
            b"resumable:attempt:v1",
            &[
                execution.as_uuid().as_bytes(),
                name.as_str().as_bytes(),
                &attempt,
            ],
        ))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn encode_key(domain: &[u8], parts: &[&[u8]]) -> String {
    let mut hash = Sha256::new();
    hash.update((domain.len() as u32).to_be_bytes());
    hash.update(domain);
    for part in parts {
        hash.update((part.len() as u32).to_be_bytes());
        hash.update(part);
    }
    format!("rs1_{}", URL_SAFE_NO_PAD.encode(hash.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn step_keys_are_stable_per_step_and_attempt_keys_change() {
        let execution = ExecutionId::new();
        let name = StepName::new("deliver").unwrap();
        let stable = IdempotencyKey::for_step(execution, &name);
        assert_eq!(stable, IdempotencyKey::for_step(execution, &name));
        assert_ne!(
            IdempotencyKey::for_attempt(execution, &name, 1),
            IdempotencyKey::for_attempt(execution, &name, 2)
        );
        assert_ne!(
            stable,
            IdempotencyKey::for_step(execution, &StepName::new("deliver ").unwrap())
        );
    }

    #[test]
    fn step_keys_include_execution_identity() {
        let name = StepName::new("same-name").unwrap();
        assert_ne!(
            IdempotencyKey::for_step(ExecutionId::new(), &name),
            IdempotencyKey::for_step(ExecutionId::new(), &name)
        );
    }

    #[test]
    fn deserialization_enforces_identifier_limits() {
        assert!(serde_json::from_str::<StepName>("\"\"").is_err());
        assert!(serde_json::from_str::<StepName>(&format!("\"{}\"", "x".repeat(201))).is_err());
        assert!(serde_json::from_str::<OperationKey>(r#"{"name":"","version":1}"#).is_err());
        assert!(
            serde_json::from_str::<OperationKey>(&format!(
                "{{\"name\":\"{}\",\"version\":1}}",
                "x".repeat(201)
            ))
            .is_err()
        );
    }

    #[test]
    fn malformed_nested_identifiers_are_rejected_as_persisted_record_data() {
        let record = serde_json::json!({
            "id": ExecutionId::new(),
            "execution_id": ExecutionId::new(),
            "name": "",
            "status": "pending",
            "attempt": 0,
            "result": null,
            "created_at": 0,
            "updated_at": 0,
            "started_at": null,
            "completed_at": null,
            "next_attempt_at": null,
            "failure": null,
            "retry_policy": null
        });
        assert!(serde_json::from_value::<crate::StepRecord>(record).is_err());
    }
}
