# Contributing

## Versioning and releases

All three crates in this workspace use one shared version. Use `scripts/version.py` to bump their
versions together and update the workspace entries in `Cargo.lock`:

```sh
scripts/version.py bump patch
scripts/version.py bump minor
scripts/version.py bump major
scripts/version.py bump v1.2.3
scripts/version.py ensure-version v1.2.3
```

The explicit version may also be written without the `v` prefix. It must be greater than the
current version for `bump`; `ensure-version` checks that the supplied version matches every crate
and its `Cargo.lock` entry. The three-part version must match across every crate. The script
requires Python 3.

After a version bump, review the changes and run the workspace checks from `AGENTS.md`. Commit the
version changes, then push a tag matching that version, such as `v1.2.3`. The publish workflow
validates the tag and publishes `resumable` before the two crates that depend on it. Configure the
repository's `CARGO_REGISTRY_TOKEN` secret before publishing.
