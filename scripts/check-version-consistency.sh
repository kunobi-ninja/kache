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
# Scope: every publishable workspace crate and its local dependency pins.
# Unpublished service, proof, and test packages may carry separate versions.
# Chart versions still follow the binary release tag.
#
# Hermetic: pure manifest reads via python tomllib — no cargo, no nix, no
# network — so the gate is fast (~hundreds of ms) and has no installer/registry
# failure surface.
#
# Usage:
#   check-version-consistency.sh                # internal mode: publishable versions agree
#   check-version-consistency.sh v0.5.0         # tag mode: publishable versions == 0.5.0
#   check-version-consistency.sh v0.5.0-rc.4    # tag mode: publishable versions == 0.5.0-rc.4
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
kache_version = root_toml["package"]["version"]
manifests = {root: root_toml}
for member in root_toml["workspace"]["members"]:
    for directory in root.glob(member):
        manifest = load(directory / "Cargo.toml")
        if manifest["package"].get("publish") is not False:
            manifests[directory.resolve()] = manifest

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

# Every crate that ships follows the release version. A local dependency
# must pin that version so the published manifest resolves the same crate.
for directory, manifest in manifests.items():
    package = manifest["package"]
    if package["version"] != kache_version:
        errors.append(
            f"{package['name']} version {package['version']!r} != kache version {kache_version!r}"
        )
    dependency_tables = [manifest.get(kind, {}) for kind in ("dependencies", "build-dependencies")]
    for target in manifest.get("target", {}).values():
        dependency_tables.extend(target.get(kind, {}) for kind in ("dependencies", "build-dependencies"))
    for dependencies in dependency_tables:
        for name, dependency in dependencies.items():
            if not isinstance(dependency, dict) or "path" not in dependency:
                continue
            target_dir = (directory / dependency["path"]).resolve()
            target_manifest = manifests.get(target_dir)
            if target_manifest is None:
                errors.append(f"{package['name']} depends on unpublished local crate {name}")
            elif dependency.get("version") != target_manifest["package"]["version"]:
                errors.append(
                    f"{package['name']} dependency pin for {name} {dependency.get('version')!r} "
                    f"!= {target_manifest['package']['version']!r}"
                )

# (1b) Chart.yaml — both fields, because the chart ships from the same `v*` tag
# (see the publish-chart job). `appVersion` names the app the chart deploys and
# `version` is the chart's own identity in the registry; kobe learned to keep
# both on the release tag rather than a separate track, so a chart-only fix is a
# patch release of the whole thing. Chart versions are immutable once pushed, so
# a mismatch has to fail here — before the push, not after.
#
# Plain line reads rather than a yaml dependency: this gate stays hermetic.
chart_path = root / "packaging" / "charts" / "kache-service" / "Chart.yaml"
chart = chart_path.read_text()


def chart_field(name):
    mm = re.search(rf'(?m)^{name}:\s*"?([^"\s]+)"?\s*$', chart)
    return mm.group(1) if mm else None


for field in ("version", "appVersion"):
    value = chart_field(field)
    if value is None:
        errors.append(f"packaging/charts/kache-service/Chart.yaml has no `{field}:`")
    elif value != kache_version:
        errors.append(
            f"Chart.yaml {field} {value!r} != workspace version {kache_version!r}"
        )

# (2) Tag agreement — only when a tag is supplied (tag pushes / publish). The
# full version must match, prerelease suffix included.
if tag_version:
    if kache_version != tag_version:
        errors.append(f"kache version {kache_version!r} != tag version {tag_version!r}")

if errors:
    scope = f"tag {tag_version}" if tag_version else "the workspace manifests"
    print(f"version consistency FAILED for {scope}:", file=sys.stderr)
    for e in errors:
        print("  - " + e, file=sys.stderr)
    fix = tag_version or kache_version
    print(f"Fix: run `just bump {fix}` so the manifests agree, then re-tag if needed.", file=sys.stderr)
    sys.exit(1)

scope = f"tag {tag_version}" if tag_version else "workspace"
print(f"version consistency OK: {scope}, {len(manifests)} publishable crates at {kache_version}")
PY
