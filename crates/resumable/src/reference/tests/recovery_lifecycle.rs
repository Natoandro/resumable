use super::*;

#[test]
fn cancellation_fault_boundaries_leave_a_durable_request() {
    let (store, id, _, _, _) = super::recovery_steps::running_step();
    let request = RequestCancellation {
        execution_id: id,
        at: Timestamp(2),
    };
    let before = block_on(store.load_execution(id)).unwrap().unwrap();
    store
        .faults()
        .inject_once(FaultPoint::Cancellation, FaultMode::BeforeCommit);
    assert!(block_on(store.request_cancellation(request.clone())).is_err());
    assert_eq!(block_on(store.load_execution(id)).unwrap().unwrap(), before);
    store
        .faults()
        .inject_once(FaultPoint::Cancellation, FaultMode::AfterCommit);
    assert!(block_on(store.request_cancellation(request.clone())).is_err());
    let committed = block_on(store.load_execution(id)).unwrap().unwrap();
    assert_eq!(committed.cancellation_requested_at, Some(Timestamp(2)));
    assert_eq!(
        block_on(store.request_cancellation(request)).unwrap(),
        committed
    );
}

#[test]
fn heartbeat_fault_boundaries_preserve_revision_and_progress_rules() {
    let (store, id, guard, name, _) = super::recovery_steps::running_step();
    let request = ProgressUpdate {
        lease: guard,
        lease_duration_ms: 100,
        step: Some(name.clone()),
        progress: Some(Progress {
            current: Some(2),
            total: Some(5),
            message: Some("checkpoint".into()),
        }),
        at: Timestamp(2),
    };
    let before = block_on(store.load_execution(id)).unwrap().unwrap();
    store
        .faults()
        .inject_once(FaultPoint::Heartbeat, FaultMode::BeforeCommit);
    assert!(block_on(store.heartbeat(request.clone())).is_err());
    assert_eq!(block_on(store.load_execution(id)).unwrap().unwrap(), before);
    assert_eq!(block_on(store.load_progress(id, &name)).unwrap(), None);
    store
        .faults()
        .inject_once(FaultPoint::Heartbeat, FaultMode::AfterCommit);
    assert!(block_on(store.heartbeat(request)).is_err());
    let committed = block_on(store.load_execution(id)).unwrap().unwrap();
    assert!(committed.revision > before.revision);
    assert_eq!(
        block_on(store.load_progress(id, &name))
            .unwrap()
            .unwrap()
            .current,
        Some(2)
    );
}

#[test]
fn purge_fault_boundaries_remove_or_preserve_all_records_atomically() {
    let (store, id, guard, name, step) = super::recovery_steps::running_step();
    const SECRET: &str = "purge-payload-sentinel";
    let step_request = CompleteStep {
        lease: guard.clone(),
        name: name.clone(),
        expected_attempt: step.attempt,
        mutation_id: MutationId::new(),
        result: Some(
            StoredValue::new(
                crate::limits::DEFAULT_CODEC_ID,
                crate::limits::DEFAULT_SCHEMA_VERSION,
                SECRET.as_bytes().to_vec(),
            )
            .unwrap(),
        ),
        at: Timestamp(2),
    };
    let completed_step = block_on(store.complete_step(step_request.clone())).unwrap();
    assert_eq!(
        completed_step.result.as_ref().unwrap().bytes(),
        SECRET.as_bytes()
    );
    let execution = block_on(store.load_execution(id)).unwrap().unwrap();
    let execution_request = CompleteExecution {
        lease: LeaseGuard {
            expected_revision: execution.revision,
            ..guard
        },
        mutation_id: MutationId::new(),
        result: Some(
            StoredValue::new(
                crate::limits::DEFAULT_CODEC_ID,
                crate::limits::DEFAULT_SCHEMA_VERSION,
                SECRET.as_bytes().to_vec(),
            )
            .unwrap(),
        ),
        at: Timestamp(3),
    };
    let completed = block_on(store.complete_execution(execution_request.clone())).unwrap();
    assert_eq!(
        completed.result.as_ref().unwrap().bytes(),
        SECRET.as_bytes()
    );
    assert_eq!(completed_step.status, StepStatus::Completed);
    store
        .faults()
        .inject_once(FaultPoint::Purge, FaultMode::BeforeCommit);
    assert!(block_on(store.purge_execution(id)).is_err());
    assert_eq!(
        block_on(store.load_execution(id)).unwrap().unwrap(),
        completed
    );
    assert!(block_on(store.load_step(id, &name)).unwrap().is_some());
    store
        .faults()
        .inject_once(FaultPoint::Purge, FaultMode::AfterCommit);
    assert!(block_on(store.purge_execution(id)).is_err());
    assert_eq!(block_on(store.load_execution(id)).unwrap(), None);
    assert_eq!(block_on(store.load_step(id, &name)).unwrap(), None);
    assert!(matches!(
        block_on(store.complete_step(step_request)),
        Err(StoreError::Conflict(_))
    ));
    assert!(matches!(
        block_on(store.complete_execution(execution_request)),
        Err(StoreError::Conflict(_))
    ));
    assert!(!block_on(store.purge_execution(id)).unwrap());
}
