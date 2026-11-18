use super::*;
use crate::{
    ExecutionId, Lease, LeaseToken, StepId, StepName, StepRecord, StepStatus, Timestamp, WorkerId,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Payload {
    name: String,
    count: u32,
}

#[test]
fn json_values_round_trip_as_opaque_envelopes() {
    let input = Payload {
        name: "artifact".into(),
        count: 2,
    };
    let stored = JsonCodec.encode(&input).unwrap();
    assert_eq!(stored.codec(), DEFAULT_CODEC_ID);
    assert_eq!(stored.schema_version(), DEFAULT_SCHEMA_VERSION);
    assert_eq!(JsonCodec.decode::<Payload>(&stored).unwrap(), input);
    let envelope = serde_json::to_vec(&stored).unwrap();
    assert_eq!(
        serde_json::from_slice::<StoredValue>(&envelope).unwrap(),
        stored
    );
    let invalid = serde_json::json!({ "codec": "", "schema_version": 1, "bytes": [] });
    assert!(serde_json::from_value::<StoredValue>(invalid).is_err());
}

#[test]
fn unknown_codecs_and_record_value_round_trips_are_explicit() {
    let custom = StoredValue::new("cbor-v1", 1, vec![1]).unwrap();
    assert!(matches!(
        JsonCodec.decode::<Payload>(&custom),
        Err(ValueError::UnknownCodec(_))
    ));
    let encoded = JsonCodec
        .encode(&Payload {
            name: "input".into(),
            count: 3,
        })
        .unwrap();
    let mut execution = crate::ExecutionRecord::new_with_input(
        ExecutionId::new(),
        crate::OperationKey::new("render", 1).unwrap(),
        Some(encoded.clone()),
        crate::ExecutionMetadata::default(),
        Timestamp(0),
    );
    execution
        .start(
            Lease {
                owner: WorkerId::new(),
                claim_id: crate::MutationId::new(),
                duration_ms: 50,
                token: LeaseToken::new(),
                generation: 1,
                expires_at: Timestamp(10),
            },
            Timestamp(1),
        )
        .unwrap();
    execution
        .finish_with_result(Some(encoded.clone()), Timestamp(2))
        .unwrap();
    let mut step = StepRecord::new(
        StepId::new(),
        execution.id,
        StepName::new("render").unwrap(),
        Timestamp(0),
    );
    step.begin_attempt(Timestamp(1)).unwrap();
    step.complete_with_result(Some(encoded.clone()), Timestamp(2))
        .unwrap();
    assert_eq!(execution.input, Some(encoded.clone()));
    assert_eq!(execution.result, Some(encoded.clone()));
    assert_eq!(step.result, Some(encoded));
    let execution_bytes = serde_json::to_vec(&execution).unwrap();
    assert_eq!(
        serde_json::from_slice::<crate::ExecutionRecord>(&execution_bytes).unwrap(),
        execution
    );
    let step_bytes = serde_json::to_vec(&step).unwrap();
    assert_eq!(
        serde_json::from_slice::<StepRecord>(&step_bytes).unwrap(),
        step
    );
}

#[test]
fn decode_failures_do_not_change_completed_step_history() {
    let mut step = StepRecord::new(
        StepId::new(),
        ExecutionId::new(),
        StepName::new("render").unwrap(),
        Timestamp(0),
    );
    step.begin_attempt(Timestamp(1)).unwrap();
    step.complete(Timestamp(2)).unwrap();
    let before = step.clone();
    let corrupt =
        StoredValue::new(DEFAULT_CODEC_ID, DEFAULT_SCHEMA_VERSION, b"{".to_vec()).unwrap();
    assert!(JsonCodec.decode::<Payload>(&corrupt).is_err());
    assert_eq!(step, before);
    assert_eq!(step.status, StepStatus::Completed);
    let mismatch = StoredValue::new(DEFAULT_CODEC_ID, 2, b"{}".to_vec()).unwrap();
    assert!(matches!(
        JsonCodec.decode::<Payload>(&mismatch),
        Err(ValueError::SchemaMismatch { .. })
    ));
}

#[test]
fn oversized_payload_is_rejected_with_diagnostic() {
    let bytes = vec![0; MAX_STORED_VALUE_BYTES + 1];
    assert!(matches!(
        StoredValue::new("custom", 1, bytes),
        Err(ValueError::PayloadTooLarge { .. })
    ));
    let error: crate::ResumableError<std::io::Error> =
        crate::ResumableError::Serialization("bad bytes".into());
    assert_eq!(error.to_string(), "serialization failed: bad bytes");
}
