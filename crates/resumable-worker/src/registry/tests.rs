use super::*;

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
