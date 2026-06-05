#!/usr/bin/env bash
# Assert the workspace version is internally consistent, and — when given a
# release tag — that the tag agrees with the manifests, BEFORE anything
# irreversible happens (binary build, GitHub Release, crates.io publish,
# container image).
#
# Single source of truth = Cargo.toml; the git tag is a *checked mirror* of it
# (the binary's --version comes from KACHE_VERSION=<tag>, see src/main.rs).
#
# Scope = what actually ships:
#   - `kache`        crate (crates.io) + binary
#   - `kache-core`   crate (crates.io)
#   - the `kache-core` dependency pin in the root manifest (the one
#     non-derived value that can silently diverge from the crate version)
# `kache-service` and `kache-e2e` are publish=false (the service image version
# comes from the tag; kache-e2e is test/benchmark only), so they are
# intentionally NOT gated — an intentional version skew there must not be able
# to block a release.
#
# Hermetic: pure manifest reads via python tomllib — no cargo, no nix, no
# network — so the gate is fast (~hundreds of ms) and has no installer/registry
# failure surface.
#
# Usage:
#   check-version-consistency.sh            # internal mode: the 3 values agree
#   check-version-consistency.sh vX.Y.Z     # tag mode: the 3 values == tag base
#   check-version-consistency.sh vX.Y.Z-rc.N  # RC: compared against the BASE (X.Y.Z)
#
# Exit: 0 consistent; 1 on a mismatch / malformed tag; fail-closed.
set -euo pipefail

tag="${1:-}"

base=""
if [ -n "$tag" ]; then
  case "$tag" in
    v*) : ;;
    *) echo "release tag must look like vX.Y.Z, got: $tag" >&2; exit 1 ;;
  esac
  # base = tag without the leading 'v' and without a prerelease suffix:
  # -rc.N / -rcN / -alpha.N / -beta.N — both the dotted (v0.5.0-rc.1) and the
  # historical no-dot (v0.3.0-rc2) forms.
  base="${tag#v}"
  base="$(printf '%s' "$base" | sed -E 's/-(rc|alpha|beta)\.?[0-9]+$//')"
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

BASE="$base" ROOT="$root" python3 - <<'PY'
import os, sys, tomllib, pathlib

base = os.environ["BASE"]
root = pathlib.Path(os.environ["ROOT"])


def load(p):
    with open(p, "rb") as f:
        return tomllib.load(f)


root_toml = load(root / "Cargo.toml")
core_toml = load(root / "crates" / "kache-core" / "Cargo.toml")

kache_version = root_toml["package"]["version"]
core_version = core_toml["package"]["version"]

# The kache-core dependency requirement in the root manifest. Accept both the
# table form ({ version = "..", path = .. }) and a bare string ("..").
deps = root_toml.get("dependencies", {})
if "kache-core" not in deps:
    print("kache-core is not a [dependencies] entry in the root Cargo.toml", file=sys.stderr)
    sys.exit(1)
entry = deps["kache-core"]
dep_pin = entry if isinstance(entry, str) else entry.get("version")
if dep_pin is None:
    print("kache-core dependency has no `version` requirement to check", file=sys.stderr)
    sys.exit(1)

errors = []

# (1) Internal consistency — always enforced. A half-applied bump (the crate
# bumped but the dep-pin forgotten, or vice-versa) fails here on every PR.
if core_version != dep_pin:
    errors.append(
        f"kache-core crate version {core_version!r} != kache-core dependency pin "
        f"{dep_pin!r} (root Cargo.toml [dependencies.kache-core].version)"
    )
if kache_version != core_version:
    errors.append(
        f"kache version {kache_version!r} != kache-core version {core_version!r} "
        "(the workspace is bumped in lockstep)"
    )

# (2) Tag agreement — only when a tag is supplied (tag pushes / publish).
if base:
    if kache_version != base:
        errors.append(f"kache version {kache_version!r} != tag base {base!r}")
    if core_version != base:
        errors.append(f"kache-core version {core_version!r} != tag base {base!r}")

if errors:
    scope = f"tag base {base}" if base else "the workspace manifests"
    print(f"version consistency FAILED for {scope}:", file=sys.stderr)
    for e in errors:
        print("  - " + e, file=sys.stderr)
    fix = base or kache_version
    print(f"Fix: run `just bump {fix}` so the manifests agree, then re-tag if needed.", file=sys.stderr)
    sys.exit(1)

if base:
    print(f"version consistency OK: tag base {base} == kache == kache-core == kache-core dep-pin")
else:
    print(f"version consistency OK: kache == kache-core == kache-core dep-pin == {kache_version}")
PY
