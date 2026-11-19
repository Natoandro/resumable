# Contributing

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
