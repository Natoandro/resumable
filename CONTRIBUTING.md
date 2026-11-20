# Contributing

This repository is a Cargo workspace containing the execution engine (`crates/resumable`),
the polling worker (`crates/resumable-worker`), and the PostgreSQL adapter
(`crates/resumable-store-postgres-sqlx`). Keep application-specific code out of these crates,
and keep polling and database concerns in their respective crates.

## Building and testing

Run workspace checks from the repository root. The checked-in SQLx metadata allows builds and
checks without a database; the PostgreSQL integration suite requires the setup below.

```sh
SQLX_OFFLINE=true cargo build --workspace
cargo fmt --all -- --check
SQLX_OFFLINE=true cargo clippy --workspace --all-targets
cargo test -p resumable -p resumable-worker
cargo run -p resumable-worker --example standalone
```

## Changes and pull requests

Use Rust 2024 and run rustfmt. Keep tests near the code they exercise, with PostgreSQL
integration tests under the adapter's `tests/postgres/`. Use checked SQLx query macros and
format SQL with major clauses and predicates on separate lines, within 100 characters.

Use focused Conventional Commits, such as `feat(resumable): support scheduled execution starts`.
In pull requests, explain the change and its purpose, list the checks run, and link related
issues when available.

## PostgreSQL integration tests

Start the disposable local PostgreSQL database and run the integration suite from the workspace
root. Copy `.env.example` to `.env` and export its variables before running the tests.
Cargo does not load `.env` automatically:

```sh
docker compose up -d --wait
set -a; . ./.env; set +a
SQLX_OFFLINE=true cargo test -p resumable-store-postgres-sqlx --test postgres
docker compose down
```

## Refreshing the SQLx offline cache

After changing a checked query or the schema it uses, refresh the package-local `.sqlx/`
cache and commit the resulting changes. Install `sqlx-cli` with PostgreSQL support first.
From the workspace root, start PostgreSQL and export the URL as described above. Set
`DATABASE_URL` for SQLx and ensure the database has the current adapter schema. The
integration test applies the adapter migrations when the existing cache can compile it:

```sh
export DATABASE_URL="$RESUMABLE_TEST_DATABASE_URL"
SQLX_OFFLINE=true cargo test -p resumable-store-postgres-sqlx --test postgres
cd crates/resumable-store-postgres-sqlx
cargo sqlx prepare -- --all-targets
SQLX_OFFLINE=true cargo check --all-targets
```

If the cache is already stale and the test cannot compile, prepare a separate disposable
database for SQLx by applying the SQL files in `crates/resumable-store-postgres-sqlx/migrations/` in order and
creating the `resumable_migrations` table defined in the adapter’s
`src/postgres/migration.rs`. This supplies the schema
for query checking; it does not populate the adapter's migration history and should not be
used as the integration test database. Point `DATABASE_URL` at that database, then run the
prepare and check commands above. Unset `SQLX_OFFLINE` when running `cargo sqlx prepare`.

## Versioning and releases

All three crates in this workspace use one shared version. Use `scripts/version.py` to bump their
versions together and update the workspace entries in `Cargo.lock`:

```sh
scripts/version.py bump patch
scripts/version.py bump minor
scripts/version.py bump major
scripts/version.py bump v1.2.3
scripts/version.py bump patch --tag
scripts/version.py bump minor --tag --push
scripts/version.py ensure-version v1.2.3
```

The explicit version may also be written without the `v` prefix. It must be greater than the
current version for `bump`; `ensure-version` checks that the supplied version matches every crate
and its `Cargo.lock` entry. The three-part version must match across every crate. The script
requires Python 3.

With `--tag`, the script requires a clean worktree, commits the version changes with
`chore: bump version to <version>`, and creates a matching `v<version>` tag. Add `--push` to push
tags after creating the commit and tag. Without `--tag`, the script only changes the version files.

After a version bump, review the changes and run the workspace checks from `AGENTS.md`. The publish workflow
validates the tag and publishes `resumable` before the two crates that depend on it. Configure the
repository's `CARGO_REGISTRY_TOKEN` secret before publishing.
