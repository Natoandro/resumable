#!/usr/bin/env python3
import os
import re
import sys
import tempfile
from pathlib import Path

if len(sys.argv) != 3 or sys.argv[1] not in {"bump", "ensure-version"}:
    raise SystemExit(
        f"Usage: {sys.argv[0]} <bump <major|minor|patch|vX.Y.Z> | ensure-version vX.Y.Z>"
    )

root = Path(__file__).resolve().parent.parent
request = sys.argv[2]
manifest_paths = [
    root / "crates/resumable/Cargo.toml",
    root / "crates/resumable-worker/Cargo.toml",
    root / "crates/resumable-store-postgres-sqlx/Cargo.toml",
]
lock_path = root / "Cargo.lock"
version_pattern = re.compile(r'(?m)^version = "([^"]+)"$')
semver_pattern = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")

manifests = []
versions = []
for path in manifest_paths:
    content = path.read_text()
    match = version_pattern.search(content)
    if not match:
        raise SystemExit(f"No package version found in {path}")
    manifests.append((path, content, match))
    versions.append(match.group(1))

if len(set(versions)) != 1:
    raise SystemExit(f"Crate versions differ: {', '.join(versions)}")
current = versions[0]
current_match = semver_pattern.fullmatch(current)
if not current_match:
    raise SystemExit(f"Unsupported current version: {current}")

if request in {"major", "minor", "patch"}:
    major, minor, patch = map(int, current_match.groups())
    if request == "major":
        target = f"{major + 1}.0.0"
    elif request == "minor":
        target = f"{major}.{minor + 1}.0"
    else:
        target = f"{major}.{minor}.{patch + 1}"
else:
    requested = request.removeprefix("v")
    if not semver_pattern.fullmatch(requested):
        raise SystemExit("Version must be major.minor.patch, optionally prefixed with 'v'")
    if sys.argv[1] == "bump" and tuple(map(int, requested.split("."))) <= tuple(map(int, current.split("."))):
        raise SystemExit(f"New version {requested} must be greater than current version {current}")
    target = requested

lock = lock_path.read_text()
package_names = {
    "resumable",
    "resumable-worker",
    "resumable-store-postgres-sqlx",
}
package_block = re.compile(r"(?ms)^\[\[package\]\]\n.*?(?=^\[\[package\]\]|\Z)")
seen = set()

def update_lock_block(match):
    block = match.group(0)
    name_match = re.search(r'(?m)^name = "([^"]+)"$', block)
    if not name_match or name_match.group(1) not in package_names:
        return block
    name = name_match.group(1)
    version_match = re.search(r'(?m)^version = "([^"]+)"$', block)
    if not version_match or version_match.group(1) != current:
        raise SystemExit(f"Unexpected locked version for {name}")
    seen.add(name)
    return block[:version_match.start(1)] + target + block[version_match.end(1):]

updated_lock = package_block.sub(update_lock_block, lock)
if seen != package_names:
    raise SystemExit(f"Cargo.lock is missing workspace packages: {', '.join(sorted(package_names - seen))}")

if sys.argv[1] == "ensure-version":
    expected = request.removeprefix("v")
    if not semver_pattern.fullmatch(expected):
        raise SystemExit("Expected version must be major.minor.patch, optionally prefixed with 'v'")
    if current != expected:
        raise SystemExit(f"Workspace version is {current}; expected {expected}")
    print(f"All workspace crate and lockfile versions match {expected}")
    raise SystemExit(0)

changes = [(path, content[:match.start(1)] + target + content[match.end(1):])
           for path, content, match in manifests]
changes.append((lock_path, updated_lock))

for path, content in changes:
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "w") as output:
            output.write(content)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)

print(f"Bumped all workspace crates from {current} to {target}")
