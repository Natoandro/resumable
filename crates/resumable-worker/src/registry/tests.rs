use super::*;

#[tokio::test]
async fn typed_registration_dispatches_json_with_both_contexts() {
    let registry = OperationRegistry::<u32, u32>::default();
    let key = OperationKey::new("sample", 1).unwrap();
    registry
        .register_typed(
            key.clone(),
            |input: u32, context, execution_context| async move {
                input
                    .checked_add(context)
                    .and_then(|sum| sum.checked_mul(execution_context))
                    .ok_or("overflow")
            },
        )
        .await
        .unwrap();

    let handler = registry.handler(&key).await.unwrap();
    assert_eq!(
        handler(Value::from(5), 2, 3).await.unwrap(),
        Value::from(21)
    );
    assert!(
        handler(Value::from("invalid"), 2, 3)
            .await
            .unwrap_err()
            .contains("failed to deserialize operation input")
    );
    assert_eq!(
        handler(Value::from(u32::MAX), 2, 3).await.unwrap_err(),
        "overflow"
    );
}

#[tokio::test]
async fn registry_rejects_duplicate_keys_and_orders_supported_operations() {
    let registry = OperationRegistry::<(), ()>::default();
    let later = OperationKey::new("sample", 2).unwrap();
    let earlier_name = OperationKey::new("alpha", 1).unwrap();
    let earlier_version = OperationKey::new("sample", 1).unwrap();
    registry
        .register(later.clone(), |_, (), ()| async { Ok(Value::Null) })
        .await
        .unwrap();
    assert!(
        registry
            .register(later.clone(), |_, (), ()| async { Ok(Value::Null) })
            .await
            .is_err()
    );
    registry
        .register(earlier_name.clone(), |_, (), ()| async { Ok(Value::Null) })
        .await
        .unwrap();
    registry
        .register(earlier_version.clone(), |_, (), ()| async {
            Ok(Value::Null)
        })
        .await
        .unwrap();
    assert_eq!(
        registry.keys().await,
        [earlier_name, earlier_version, later]
    );
}
