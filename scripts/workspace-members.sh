#!/usr/bin/env bash
# Print the Cargo workspace members, one per line, read from `cargo metadata`.
# Recipes use this instead of a hand-written list, which silently goes stale
# when a crate is added: `check-coverage-scope.sh` already reads membership
# from `cargo metadata`, so a stale list fails the coverage gate.
#
# Usage: scripts/workspace-members.sh packages|dirs
#   packages  package names
#   dirs      manifest directories relative to the repository root ("." for the root)
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mode="${1:-}"
case "$mode" in
  packages | dirs) ;;
  *)
    echo "usage: $0 packages|dirs" >&2
    exit 2
    ;;
esac

cd "$root"
cargo metadata --locked --no-deps --format-version 1 |
  ROOT="$root" MODE="$mode" python3 -c '
import json
import os
import sys

root = os.path.realpath(os.environ["ROOT"])
packages = json.load(sys.stdin)["packages"]
if os.environ["MODE"] == "packages":
    lines = [package["name"] for package in packages]
else:
    lines = [
        os.path.relpath(os.path.dirname(os.path.realpath(package["manifest_path"])), root)
        for package in packages
    ]
for line in sorted(lines):
    print(line)
'
