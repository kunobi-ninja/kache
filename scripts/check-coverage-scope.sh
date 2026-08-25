#!/usr/bin/env bash
# Assert that an llvm-cov JSON report contains product source from every Cargo
# workspace member. A percentage over only the default package is otherwise a
# plausible-looking but incomplete workspace gate.
#
# Membership and source roots come from `cargo metadata`, not a mirrored list,
# so adding a fifth member makes this check fail until coverage reports it too.
#
# Usage: scripts/check-coverage-scope.sh [coverage-json]
# Exit: 0 when every member is represented; 1 on missing/malformed input.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
coverage_json="${1:-tmp/llvm-cov/coverage.json}"

ROOT="$root" COVERAGE_JSON="$coverage_json" python3 - <<'PY'
import json
import os
from pathlib import Path
import subprocess
import sys

root = Path(os.environ["ROOT"]).resolve()
coverage_path = Path(os.environ["COVERAGE_JSON"])
if not coverage_path.is_absolute():
    coverage_path = root / coverage_path


def fail(message, details=()):
    print(f"coverage scope FAILED: {message}", file=sys.stderr)
    for detail in details:
        print(f"  - {detail}", file=sys.stderr)
    sys.exit(1)


try:
    with coverage_path.open(encoding="utf-8") as stream:
        coverage = json.load(stream)
except FileNotFoundError:
    fail(f"report not found: {coverage_path}")
except (OSError, UnicodeError, json.JSONDecodeError) as error:
    fail(f"cannot read {coverage_path}: {error}")
if not isinstance(coverage, dict):
    fail(f"{coverage_path} root must be a JSON object")

try:
    metadata_process = subprocess.run(
        [
            "cargo",
            "metadata",
            "--locked",
            "--no-deps",
            "--format-version",
            "1",
        ],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    metadata = json.loads(metadata_process.stdout)
except (OSError, subprocess.CalledProcessError, json.JSONDecodeError) as error:
    fail(f"cannot resolve Cargo workspace metadata: {error}")
if not isinstance(metadata, dict):
    fail("cargo metadata root is not a JSON object")

data = coverage.get("data")
if not isinstance(data, list) or not data:
    fail(f"{coverage_path} has no llvm-cov data entries")


def normalized(raw_path):
    path = Path(raw_path)
    if not path.is_absolute():
        path = root / path
    return os.path.normcase(os.path.realpath(path))


covered_files = set()
for entry in data:
    if not isinstance(entry, dict):
        continue
    files = entry.get("files", [])
    if not isinstance(files, list):
        continue
    for file_entry in files:
        if not isinstance(file_entry, dict):
            continue
        filename = file_entry.get("filename")
        if isinstance(filename, str) and filename:
            covered_files.add(normalized(filename))
if not covered_files:
    fail(f"{coverage_path} contains no covered source files")

raw_workspace_ids = metadata.get("workspace_members", [])
if not isinstance(raw_workspace_ids, list) or not raw_workspace_ids:
    fail("cargo metadata contains no workspace members")
workspace_ids = {
    package_id for package_id in raw_workspace_ids if isinstance(package_id, str)
}
if len(workspace_ids) != len(raw_workspace_ids):
    fail("cargo metadata contains malformed workspace member IDs")

raw_packages = metadata.get("packages", [])
if not isinstance(raw_packages, list):
    fail("cargo metadata packages field is not a list")
packages_by_id = {
    package.get("id"): package
    for package in raw_packages
    if isinstance(package, dict) and package.get("id")
}
missing_metadata = sorted(workspace_ids - packages_by_id.keys())
if missing_metadata:
    fail("cargo metadata omitted workspace package records", missing_metadata)

primary_kinds = {"bin", "lib", "rlib", "dylib", "cdylib", "staticlib", "proc-macro"}


def is_within(path, directory):
    try:
        return os.path.commonpath((path, directory)) == directory
    except ValueError:
        # Different Windows drives cannot contain one another.
        return False


missing_members = []
represented = []
for package_id in workspace_ids:
    package = packages_by_id[package_id]
    source_roots = {
        os.path.dirname(normalized(target["src_path"]))
        for target in package.get("targets", [])
        if isinstance(target, dict)
        and isinstance(target.get("src_path"), str)
        and primary_kinds.intersection(target.get("kind", []))
    }
    if not source_roots:
        fail(f"workspace member {package['name']!r} has no primary source target")

    matches = sorted(
        filename
        for filename in covered_files
        if any(is_within(filename, source_root) for source_root in source_roots)
    )
    if matches:
        represented.append((package["name"], matches[0]))
    else:
        roots = ", ".join(sorted(source_roots))
        missing_members.append(f"{package['name']}: expected a source file under {roots}")

if missing_members:
    fail(
        "llvm-cov JSON omits workspace member source; report every package explicitly",
        sorted(missing_members),
    )

print(f"coverage scope OK: {len(represented)}/{len(workspace_ids)} workspace members present")
for name, filename in sorted(represented):
    try:
        display = Path(filename).relative_to(root)
    except ValueError:
        display = Path(filename)
    print(f"  - {name}: {display}")
PY
