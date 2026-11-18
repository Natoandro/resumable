mod atomicity;
mod recovery;
mod support;

use sqlx::PgPool;

pub(super) async fn verify_transaction_guarantees(pool: PgPool) -> PgPool {
    support::ensure_domain_table(&pool).await;
    atomicity::verify_creation_atomicity(&pool).await;
    atomicity::verify_lease_loss_rolls_back_domain_write(&pool).await;
    atomicity::verify_completion_lock_blocks_takeover(&pool).await;
    recovery::verify_callback_completion_observation(&pool).await;
    let pool = recovery::verify_completion_survives_restart(pool).await;
    support::drop_domain_table(&pool).await;
    pool
}
