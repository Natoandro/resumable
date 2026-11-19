# Repository Guidelines

## Project Structure

This repository is a standalone Cargo workspace with three Rust crates.

- `crates/resumable/` owns the execution engine, `Store` contract, reference store, and shared conformance scenarios.
- `crates/resumable-worker/` provides the polling runtime and operation registry. Its `examples/standalone.rs` demonstrates runtime setup.
- `crates/resumable-store-postgres-sqlx/` implements the PostgreSQL store and owns its migrations, integration tests, and SQLx query cache.

Each crate has `src/` and `docs/` directories. Keep all three crates independent of consuming applications, and keep database and polling concerns in their respective crates.

## Build and Test

Run these checks from the workspace root.

- `SQLX_OFFLINE=true cargo build --workspace` builds the workspace using the checked-in SQLx metadata.
- `cargo fmt --all -- --check` checks formatting. `SQLX_OFFLINE=true cargo clippy --workspace --all-targets` runs Clippy.
- `cargo test -p resumable -p resumable-worker` runs tests without PostgreSQL. `cargo run -p resumable-worker --example standalone` runs the worker example.
- Set `RESUMABLE_TEST_DATABASE_URL` to a disposable PostgreSQL database, then run `SQLX_OFFLINE=true cargo test -p resumable-store-postgres-sqlx --test postgres` for conformance and transaction recovery checks.

## Rust and SQL Style

Use Rust 2024 and rustfmt's four-space indentation. Use `snake_case` for modules, functions, and behavior-based test names. Group related concerns in child modules, such as `src/engine/`, with a `mod.rs` entry point. Keep unit tests near the code and PostgreSQL integration tests under the adapter's `tests/postgres/`.

Use SQLx checked query macros and format SQL with one major clause, join, or predicate per line. Keep SQL lines within 100 characters. When a checked query changes, run `cargo sqlx prepare -- --all-targets` from the adapter directory with `DATABASE_URL` pointing to a migrated database. Commit the updated package-local `.sqlx/` cache.

## Commits and Pull Requests

Follow the Conventional Commits format, for example `feat(resumable): support scheduled execution starts`. Keep each commit focused. In pull requests, explain the change and why it is needed, list the checks you ran, and link related issues when available.

## Codex and Git Tools

When using Codex or similar coding agents, run all Git commands outside the sandbox.
