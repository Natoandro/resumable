use crate::limits::{
    DEFAULT_CODEC_ID, DEFAULT_SCHEMA_VERSION, MAX_CODEC_ID_BYTES, MAX_STORED_VALUE_BYTES,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ValueError {
    #[error("codec identifier must contain between 1 and {MAX_CODEC_ID_BYTES} bytes")]
    InvalidCodecId,
    #[error("stored value is {actual} bytes; maximum is {MAX_STORED_VALUE_BYTES}")]
    PayloadTooLarge { actual: usize },
    #[error("unknown value codec: {0}")]
    UnknownCodec(String),
    #[error("schema version mismatch for {codec}: expected {expected}, got {actual}")]
    SchemaMismatch {
        codec: String,
        expected: u32,
        actual: u32,
    },
    #[error("value serialization failed: {0}")]
    Encode(String),
    #[error("stored value decoding failed: {0}")]
    Decode(String),
}

/// Opaque serialized bytes with the information required to select a decoder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "StoredValueWire")]
pub struct StoredValue {
    codec: String,
    schema_version: u32,
    bytes: Vec<u8>,
}

#[derive(Deserialize)]
struct StoredValueWire {
    codec: String,
    schema_version: u32,
    bytes: Vec<u8>,
}

impl TryFrom<StoredValueWire> for StoredValue {
    type Error = String;
    fn try_from(wire: StoredValueWire) -> Result<Self, Self::Error> {
        Self::new(wire.codec, wire.schema_version, wire.bytes).map_err(|error| error.to_string())
    }
}

impl StoredValue {
    pub fn new(
        codec: impl Into<String>,
        schema_version: u32,
        bytes: Vec<u8>,
    ) -> Result<Self, ValueError> {
        let codec = codec.into();
        if codec.is_empty() || codec.len() > MAX_CODEC_ID_BYTES {
            return Err(ValueError::InvalidCodecId);
        }
        if bytes.len() > MAX_STORED_VALUE_BYTES {
            return Err(ValueError::PayloadTooLarge {
                actual: bytes.len(),
            });
        }
        Ok(Self {
            codec,
            schema_version,
            bytes,
        })
    }
    pub fn codec(&self) -> &str {
        &self.codec
    }
    pub const fn schema_version(&self) -> u32 {
        self.schema_version
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// A static codec contract. Implementations should keep identifiers stable across restarts.
pub trait ValueCodec: Send + Sync {
    fn codec_id(&self) -> &str;
    fn schema_version(&self) -> u32;
    fn encode_bytes<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, ValueError>;
    fn decode_bytes<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, ValueError>;

    fn encode<T: Serialize>(&self, value: &T) -> Result<StoredValue, ValueError> {
        StoredValue::new(
            self.codec_id(),
            self.schema_version(),
            self.encode_bytes(value)?,
        )
    }
    fn decode<T: DeserializeOwned>(&self, value: &StoredValue) -> Result<T, ValueError> {
        if value.bytes().len() > MAX_STORED_VALUE_BYTES {
            return Err(ValueError::PayloadTooLarge {
                actual: value.bytes().len(),
            });
        }
        if value.codec() != self.codec_id() {
            return Err(ValueError::UnknownCodec(value.codec().to_owned()));
        }
        if value.schema_version() != self.schema_version() {
            return Err(ValueError::SchemaMismatch {
                codec: value.codec().to_owned(),
                expected: self.schema_version(),
                actual: value.schema_version(),
            });
        }
        self.decode_bytes(value.bytes())
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct JsonCodec;

impl ValueCodec for JsonCodec {
    fn codec_id(&self) -> &str {
        DEFAULT_CODEC_ID
    }
    fn schema_version(&self) -> u32 {
        DEFAULT_SCHEMA_VERSION
    }
    fn encode_bytes<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, ValueError> {
        serde_json::to_vec(value).map_err(|error| ValueError::Encode(error.to_string()))
    }
    fn decode_bytes<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, ValueError> {
        serde_json::from_slice(bytes).map_err(|error| ValueError::Decode(error.to_string()))
    }
}

#[cfg(test)]
mod tests;
