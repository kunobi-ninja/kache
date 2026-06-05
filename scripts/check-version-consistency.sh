#!/usr/bin/env bash
# Assert the workspace version is internally consistent, and — when given a
# release tag — that the tag agrees with the manifests, BEFORE anything
# irreversible happens (binary build, GitHub Release, crates.io publish,
# container image).
#
# Single source of truth = Cargo.toml; the git tag is a *checked mirror* of it
# (the binary's --version comes from KACHE_VERSION=<tag>, see src/main.rs).
#
# Release-candidates publish to crates.io (Policy B), so the prerelease lives in
# the manifest: a `0.5.0-rc.4` release means Cargo.toml says `0.5.0-rc.4` and the
# tag is `v0.5.0-rc.4`. The full version (suffix included) must match — there is
# NO suffix stripping. (Cargo serves prereleases only on an explicit `--version`
# request, so this never affects a normal `cargo add`/`cargo install`.)
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
#   check-version-consistency.sh                # internal mode: the 3 values agree
#   check-version-consistency.sh v0.5.0         # tag mode: the 3 values == 0.5.0
#   check-version-consistency.sh v0.5.0-rc.4    # tag mode: the 3 values == 0.5.0-rc.4
#
# Exit: 0 consistent; 1 on a mismatch / malformed tag; fail-closed.
set -euo pipefail

tag="${1:-}"

tag_version=""
if [ -n "$tag" ]; then
  case "$tag" in
    v*) : ;;
    *) echo "release tag must look like vX.Y.Z[-rc.N], got: $tag" >&2; exit 1 ;;
  esac
  # The full version, prerelease suffix included — must equal the manifest
  # exactly (no stripping; the manifest carries the prerelease under Policy B).
  tag_version="${tag#v}"
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

TAG_VERSION="$tag_version" ROOT="$root" python3 - <<'PY'
import os, re, sys, tomllib, pathlib

tag_version = os.environ["TAG_VERSION"]
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

# (0) Reject a no-dot prerelease identifier in the version that will publish
# (e.g. 0.5.0-rc4). crates.io is permanent and semver orders the no-dot form
# LEXICALLY (rc.2 would sort after rc.10). Require the dotted form (-rc.N). This
# runs in CI (version-consistency job) and at the publish floor, so it is
# enforced even for a hand-pushed tag — not only the `just bump` local guard.
m = re.search(r"-(rc|alpha|beta)[0-9]", kache_version)
if m:
    errors.append(
        f"version {kache_version!r} uses a no-dot prerelease ({m.group(0)[1:]}…); "
        "use the dotted form (e.g. -rc.4) — the no-dot form sorts lexically on crates.io"
    )

# (1) Internal consistency — always enforced. A half-applied bump (the crate
# bumped but the dep-pin forgotten, or vice-versa) fails here on every PR. The
# dep-pin must be the EXACT version string (which `cargo set-version` writes),
# prerelease included — a hand-edited caret/range would fail closed here.
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

# (2) Tag agreement — only when a tag is supplied (tag pushes / publish). The
# full version must match, prerelease suffix included.
if tag_version:
    if kache_version != tag_version:
        errors.append(f"kache version {kache_version!r} != tag version {tag_version!r}")
    if core_version != tag_version:
        errors.append(f"kache-core version {core_version!r} != tag version {tag_version!r}")

if errors:
    scope = f"tag {tag_version}" if tag_version else "the workspace manifests"
    print(f"version consistency FAILED for {scope}:", file=sys.stderr)
    for e in errors:
        print("  - " + e, file=sys.stderr)
    fix = tag_version or kache_version
    print(f"Fix: run `just bump {fix}` so the manifests agree, then re-tag if needed.", file=sys.stderr)
    sys.exit(1)

if tag_version:
    print(f"version consistency OK: tag {tag_version} == kache == kache-core == kache-core dep-pin")
else:
    print(f"version consistency OK: kache == kache-core == kache-core dep-pin == {kache_version}")
PY
