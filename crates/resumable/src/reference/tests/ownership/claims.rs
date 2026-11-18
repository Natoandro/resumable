use super::*;

#[test]
fn concurrent_claims_have_a_single_winner() {
    let store = MemoryStore::new(Timestamp(0));
    let id = ExecutionId::new();
    block_on(store.create_execution(NewExecution::new(
        id,
        OperationKey::new("claim-race", 1).unwrap(),
        Timestamp(0),
    )))
    .unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let workers: Vec<_> = (0..2)
        .map(|_| {
            let store = store.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                block_on(store.acquire_execution(id, WorkerId::new(), MutationId::new(), 100))
                    .unwrap()
            })
        })
        .collect();
    barrier.wait();
    let outcomes: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, ClaimOutcome::Claimed(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, ClaimOutcome::Busy))
            .count(),
        1
    );
}
