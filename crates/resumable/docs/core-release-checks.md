# Core release checks

Run these checks from the repository root. They apply to `resumable` and do not require Planum
services.

```bash
cargo fmt --all -- --check
cargo clippy -p resumable --all-targets --offline -- -D warnings
cargo test -p resumable --offline
cargo test -p resumable --doc --offline
RUSTDOCFLAGS='-D warnings' cargo doc -p resumable --no-deps --offline
cargo check -p resumable --examples --offline
```

The declared minimum Rust version is 1.85. When that toolchain is installed, check all targets with
the minimum compiler:

```bash
cargo +1.85.0 check -p resumable --all-targets --offline
```

Build the crate as a standalone workspace, with only its source and declared dependencies:

```bash
./crates/resumable/scripts/check-standalone.sh
```

Run all three public fake-system examples as smoke scenarios:

```bash
cargo run -p resumable --example artifact_reconciliation --offline
cargo run -p resumable --example backup_snapshot --offline
cargo run -p resumable --example message_delivery --offline
```

These checks establish portability and reference-store behavior. They do not establish that a
production adapter meets the Store contract; each adapter must run the shared conformance scenarios
and cover its backend-specific crash and transaction boundaries.
