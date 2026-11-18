use crate::limits::{
    MAX_METADATA_KEY_BYTES, MAX_METADATA_LABELS, MAX_METADATA_TOTAL_BYTES, MAX_METADATA_VALUE_BYTES,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MetadataError {
    #[error("metadata has {actual} labels; maximum is {MAX_METADATA_LABELS}")]
    TooManyLabels { actual: usize },
    #[error("metadata key length {actual} exceeds {MAX_METADATA_KEY_BYTES} bytes")]
    KeyTooLong { actual: usize },
    #[error("metadata value length {actual} exceeds {MAX_METADATA_VALUE_BYTES} bytes")]
    ValueTooLong { actual: usize },
    #[error("metadata uses {actual} bytes; maximum is {MAX_METADATA_TOTAL_BYTES}")]
    TotalTooLarge { actual: usize },
    #[error("metadata keys must not be empty")]
    EmptyKey,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "MetadataWire")]
pub struct ExecutionMetadata {
    labels: BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct MetadataWire {
    labels: BTreeMap<String, String>,
}

impl TryFrom<MetadataWire> for ExecutionMetadata {
    type Error = String;
    fn try_from(wire: MetadataWire) -> Result<Self, Self::Error> {
        Self::new(wire.labels).map_err(|error| error.to_string())
    }
}

impl ExecutionMetadata {
    pub fn new(labels: BTreeMap<String, String>) -> Result<Self, MetadataError> {
        validate_labels(&labels)?;
        Ok(Self { labels })
    }
    pub fn labels(&self) -> &BTreeMap<String, String> {
        &self.labels
    }
}

fn validate_labels(labels: &BTreeMap<String, String>) -> Result<(), MetadataError> {
    if labels.len() > MAX_METADATA_LABELS {
        return Err(MetadataError::TooManyLabels {
            actual: labels.len(),
        });
    }
    let mut total = 0;
    for (key, value) in labels {
        if key.is_empty() {
            return Err(MetadataError::EmptyKey);
        }
        if key.len() > MAX_METADATA_KEY_BYTES {
            return Err(MetadataError::KeyTooLong { actual: key.len() });
        }
        if value.len() > MAX_METADATA_VALUE_BYTES {
            return Err(MetadataError::ValueTooLong {
                actual: value.len(),
            });
        }
        total += key.len() + value.len();
    }
    if total > MAX_METADATA_TOTAL_BYTES {
        return Err(MetadataError::TotalTooLarge { actual: total });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn metadata_limits_are_deterministic() {
        let too_many = (0..=MAX_METADATA_LABELS)
            .map(|i| (format!("k{i}"), "v".into()))
            .collect();
        assert!(matches!(
            ExecutionMetadata::new(too_many),
            Err(MetadataError::TooManyLabels { .. })
        ));
        let too_long = [("key".into(), "x".repeat(MAX_METADATA_VALUE_BYTES + 1))].into();
        assert!(matches!(
            ExecutionMetadata::new(too_long),
            Err(MetadataError::ValueTooLong { .. })
        ));
        let empty_key = [(String::new(), "value".into())].into();
        assert!(matches!(
            ExecutionMetadata::new(empty_key),
            Err(MetadataError::EmptyKey)
        ));

        let invalid =
            serde_json::json!({ "labels": { "key": "x".repeat(MAX_METADATA_VALUE_BYTES + 1) } });
        assert!(serde_json::from_value::<ExecutionMetadata>(invalid).is_err());

        let valid = ExecutionMetadata::new([("kind".into(), "backup".into())].into()).unwrap();
        let encoded = serde_json::to_vec(&valid).unwrap();
        assert_eq!(
            serde_json::from_slice::<ExecutionMetadata>(&encoded).unwrap(),
            valid
        );
    }
}
