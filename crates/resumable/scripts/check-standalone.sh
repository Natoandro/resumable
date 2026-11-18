#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
temp_root=$(mktemp -d)
trap 'rm -rf "$temp_root"' EXIT

cp -R "$repo_root/crates/resumable" "$temp_root/resumable"
cat > "$temp_root/Cargo.toml" <<'EOF'
[workspace]
members = ["resumable"]
resolver = "3"

[workspace.package]
edition = "2024"
rust-version = "1.85"

[workspace.dependencies]
base64 = "0.22"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
sha2 = "0.10"
thiserror = "2"
tracing = "0.1"
uuid = { version = "1", features = ["serde", "v4"] }
EOF

cargo check --manifest-path "$temp_root/Cargo.toml" -p resumable --all-targets --offline
