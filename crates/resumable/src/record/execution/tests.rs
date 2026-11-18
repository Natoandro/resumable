use super::*;
use crate::OperationKey;

fn time(n: i64) -> Timestamp {
    Timestamp(n)
}

fn lease() -> Lease {
    Lease {
        owner: WorkerId::new(),
        claim_id: crate::MutationId::new(),
        duration_ms: 50,
        token: LeaseToken::new(),
        generation: 1,
        expires_at: time(50),
    }
}

#[test]
fn transitions_enforce_terminal_state_and_cancellation_race() {
    let mut record = ExecutionRecord::new(
        ExecutionId::new(),
        OperationKey::new("export", 1).unwrap(),
        time(1),
    );
    assert!(record.finish(time(2)).is_err());
    record.start(lease(), time(2)).unwrap();
    record.request_cancellation(time(3)).unwrap();
    record
        .finish_with_result(
            Some(StoredValue::new("codec", 1, vec![1]).unwrap()),
            time(4),
        )
        .unwrap();
    assert_eq!(record.status, ExecutionStatus::Cancelled);
    assert_eq!(record.result, None);
    assert!(record.resume(time(5)).is_err());
}
