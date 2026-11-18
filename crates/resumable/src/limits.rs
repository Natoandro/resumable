//! Public size limits and default format identifiers for persisted data.

pub const DEFAULT_CODEC_ID: &str = "serde-json";
pub const DEFAULT_SCHEMA_VERSION: u32 = 1;

pub const MAX_CODEC_ID_BYTES: usize = 64;
pub const MAX_STORED_VALUE_BYTES: usize = 16 * 1024 * 1024;

pub const MAX_METADATA_LABELS: usize = 64;
pub const MAX_METADATA_KEY_BYTES: usize = 64;
pub const MAX_METADATA_VALUE_BYTES: usize = 256;
pub const MAX_METADATA_TOTAL_BYTES: usize = 8192;
pub const MAX_PROGRESS_MESSAGE_BYTES: usize = 1024;
pub const MAX_INSPECTED_STEPS: usize = 64;
