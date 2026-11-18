# resumable-store-postgres-sqlx

Planum adapter for the generic [resumable](../resumable/README.md) crate.

The [adapter architecture draft](docs/architecture.md) describes package naming, persistence,
transaction integration, independent migrations, and future database implementations.

This crate implements the PostgreSQL Store for the portable contract, an independent transactional
migration runner, and explicit schema validation. It accepts a caller-provided `PgPool` and does not
depend on the Planum server crate.

It depends on resumable and accepts explicit pools/configuration. It owns SQLx/PostgreSQL query
metadata in its local `.sqlx/` directory, separate from `server/.sqlx/`. After adding checked SQLx
queries, prepare and check that cache from this directory with:

```bash
cd crates/resumable-store-postgres-sqlx
DATABASE_URL=postgres://planum:planum@localhost:5432/planum cargo sqlx prepare -- --all-targets
SQLX_OFFLINE=true cargo check --all-targets
```

Create the schema explicitly before constructing application workers:

```rust
use resumable_store_postgres_sqlx::{migrate, validate_schema, PostgresStore};

async fn initialize(pool: sqlx::PgPool) -> Result<PostgresStore, resumable::StoreError> {
	migrate(&pool).await?;
	validate_schema(&pool).await?;
	Ok(PostgresStore::new(pool))
}
```

The migration history table is fixed as `resumable_migrations`. Store construction does not run DDL.
The PostgreSQL integration suite uses the disposable `planum_test` database created by the root
Compose setup by default. Set `RESUMABLE_TEST_DATABASE_URL` to override it when running the suite
against a different disposable database:

```bash
cargo test -p resumable-store-postgres-sqlx --test postgres
```

Planum supplies authorization,
operation callbacks, provider clients, domain transactions, dispatch, and API presentation through
server services. The adapter must not depend on the Planum server crate.

The generic core and its adapters will be extracted together into one repository later. Until then,
this package remains a path dependency in the DACB workspace.
