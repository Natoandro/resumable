# resumable-store-postgres-sqlx

PostgreSQL persistence for the [resumable](https://docs.rs/resumable) engine. See the
[crate documentation](https://docs.rs/resumable-store-postgres-sqlx) for database setup,
runnable discovery, and application transaction integration.

## Development

After changing a checked query, set `DATABASE_URL` to a migrated development database and
refresh the package-local `.sqlx/` cache from the adapter directory:

```sh
cargo sqlx prepare -- --all-targets
SQLX_OFFLINE=true cargo check --all-targets
```

Set `RESUMABLE_TEST_DATABASE_URL` to a disposable PostgreSQL database and run the integration
suite from the workspace root:

```sh
SQLX_OFFLINE=true cargo test -p resumable-store-postgres-sqlx --test postgres
```
