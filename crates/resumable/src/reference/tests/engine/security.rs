use super::*;

#[derive(Clone)]
struct TraceBuffer(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for TraceBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn error_diagnostics_are_omitted_by_default_and_captured_only_when_enabled() {
    const SECRET: &str = "sensitive-error-sentinel";
    for capture in [false, true] {
        let store = MemoryStore::new(Timestamp(0));
        let engine = Engine::with_config(
            store.clone(),
            crate::JsonCodec,
            EngineConfig {
                lease_duration: std::time::Duration::from_millis(1),
                capture_error_diagnostics: capture,
                ..EngineConfig::default()
            },
        );
        let id = ExecutionId::new();
        let result = block_on(engine.start_with_id_and_context(
            id,
            operation(),
            (),
            |_, _| async { Err::<(), _>(SECRET.to_owned()) },
        ));
        assert!(result.is_err());
        let record = block_on(store.load_execution(id)).unwrap().unwrap();
        let diagnostic = record.failure.unwrap().diagnostic;
        if capture {
            assert_eq!(diagnostic.as_deref(), Some(SECRET));
        } else {
            assert_eq!(diagnostic, None);
        }
    }
}

#[test]
fn sensitive_values_never_enter_default_tracing_fields() {
    const SECRET: &str = "trace-secret-sentinel";
    for capture in [false, true] {
        let store = MemoryStore::new(Timestamp(0));
        let engine = Engine::with_config(
            store.clone(),
            crate::JsonCodec,
            EngineConfig {
                lease_duration: std::time::Duration::from_millis(1),
                capture_error_diagnostics: capture,
                ..EngineConfig::default()
            },
        );
        let bytes = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer_bytes = bytes.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_writer(move || TraceBuffer(writer_bytes.clone()))
            .finish();
        let id = ExecutionId::new();
        let result = tracing::subscriber::with_default(subscriber, || {
            block_on(engine.start_with_id_and_context(
                id,
                operation(),
                SECRET.to_owned(),
                |_, context| async move {
                    context
                        .step("private-value", |step| async move {
                            step.progress(crate::Progress {
                                current: Some(1),
                                total: Some(1),
                                message: Some(format!(
                                    "{SECRET}{}",
                                    "x".repeat(crate::limits::MAX_PROGRESS_MESSAGE_BYTES)
                                )),
                            })
                            .await;
                            Err::<(), _>(SECRET.to_owned())
                        })
                        .await
                        .map_err(|error| error.to_string())
                },
            ))
        });
        assert!(result.is_err());
        let trace = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert!(
            !trace.contains(SECRET),
            "trace leaked a sensitive value: {trace}"
        );
        let record = block_on(store.load_execution(id)).unwrap().unwrap();
        let diagnostic = record.failure.unwrap().diagnostic;
        if capture {
            assert!(diagnostic.as_deref().unwrap().contains(SECRET));
        } else {
            assert_eq!(diagnostic, None);
        }
    }
}

#[test]
fn progress_message_limit_accepts_the_boundary_and_rejects_oversize() {
    let store = MemoryStore::new(Timestamp(0));
    let engine = engine(store.clone());
    let id = ExecutionId::new();
    let allowed = "a".repeat(crate::limits::MAX_PROGRESS_MESSAGE_BYTES);
    let rejected = "b".repeat(crate::limits::MAX_PROGRESS_MESSAGE_BYTES + 1);
    assert!(
        block_on(engine.start_with_id_and_context(
            id,
            operation(),
            (),
            move |_, context| async move {
                context
                    .step("progress", move |step| {
                        let allowed = allowed.clone();
                        let rejected = rejected.clone();
                        async move {
                            step.progress(crate::Progress {
                                current: Some(1),
                                total: Some(3),
                                message: Some(allowed),
                            })
                            .await;
                            step.progress_checked(crate::Progress {
                                current: Some(2),
                                total: Some(3),
                                message: Some(rejected),
                            })
                            .await
                            .map_err(|error| error.to_string())?;
                            Ok::<(), String>(())
                        }
                    })
                    .await
                    .map_err(|error| error.to_string())
            },
        ))
        .is_err()
    );
    let progress = block_on(store.load_progress(id, &StepName::new("progress").unwrap()))
        .unwrap()
        .unwrap();
    let expected = "a".repeat(crate::limits::MAX_PROGRESS_MESSAGE_BYTES);
    assert_eq!(progress.current, Some(1));
    assert_eq!(progress.message.as_deref(), Some(expected.as_str()));
}

#[test]
fn corrupted_persisted_input_stops_before_operation_dispatch() {
    let store = MemoryStore::new(Timestamp(0));
    let engine = engine(store.clone());
    let id = ExecutionId::new();
    let interrupted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        block_on(
            engine.start_with_id_and_context(id, operation(), (), |_, _| async {
                panic!("interrupt before execution completion");
                #[allow(unreachable_code)]
                Ok::<(), String>(())
            }),
        )
    }));
    assert!(interrupted.is_err());
    {
        let mut state = store.state.lock().unwrap();
        state.executions.get_mut(&id).unwrap().input = Some(
            crate::StoredValue::new(
                crate::limits::DEFAULT_CODEC_ID,
                crate::limits::DEFAULT_SCHEMA_VERSION,
                b"{".to_vec(),
            )
            .unwrap(),
        );
    }
    store.clock().advance(2);
    let calls = std::sync::atomic::AtomicUsize::new(0);
    let result = block_on(
        engine.resume::<(), (), String, _, _>(id, &operation(), |_| {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async { Ok(()) }
        }),
    );
    assert!(matches!(result, Err(RunError::Value(_))));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[test]
fn corrupted_completed_step_is_not_replayed_during_resume() {
    let store = MemoryStore::new(Timestamp(0));
    let engine = engine(store.clone());
    let id = ExecutionId::new();
    let effects = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let first_effects = effects.clone();
    let interrupted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        block_on(engine.start_with_id_and_context(
            id,
            operation(),
            (),
            move |_, context| async move {
                context
                    .step("external", move |_| {
                        let effects = first_effects.clone();
                        async move {
                            effects.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            Ok::<_, String>("completed".to_owned())
                        }
                    })
                    .await
                    .map_err(|error| error.to_string())?;
                panic!("interrupt after durable step completion");
                #[allow(unreachable_code)]
                Ok::<(), String>(())
            },
        ))
    }));
    assert!(interrupted.is_err());
    let name = StepName::new("external").unwrap();
    {
        let mut state = store.state.lock().unwrap();
        state.steps.get_mut(&(id, name.clone())).unwrap().result = Some(
            crate::StoredValue::new(
                crate::limits::DEFAULT_CODEC_ID,
                crate::limits::DEFAULT_SCHEMA_VERSION,
                b"{".to_vec(),
            )
            .unwrap(),
        );
    }
    store.clock().advance(2);
    let resumed_effects = effects.clone();
    let resumed = block_on(engine.resume_with_context::<(), (), String, _, _>(
        id,
        &operation(),
        move |_, context| async move {
            context
                .step("external", move |_| {
                    let effects = resumed_effects.clone();
                    async move {
                        effects.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok::<_, String>(())
                    }
                })
                .await
                .map_err(|error| error.to_string())
        },
    ));
    assert!(resumed.is_err());
    assert_eq!(effects.load(std::sync::atomic::Ordering::SeqCst), 1);
}
