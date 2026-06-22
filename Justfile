# Don't use kache to build kache (bootstrapping problem).
export RUSTC_WRAPPER := ""

# On Windows, `just` runs each recipe line via `sh`, which Git for Windows
# provides but does not put on PATH — so recipes (e.g. `just bench`) fail with
# "could not find the shell". Point it at Git bash at its default install path.
# Unix is unaffected: `windows-shell` only applies on Windows. If Git is
# installed elsewhere, override this path (or put Git's `usr\bin` on PATH).
set windows-shell := ["C:/Program Files/Git/usr/bin/sh.exe", "-cu"]

default:
  @just --list

# Run all local quality checks.
[group('dev')]
check: fmt-check lint test

# Mirror the repo CI verification flow.
[group('dev')]
ci: fmt-check lint image-service-print helm-lint coverage

# Auto-fix formatting and clippy warnings.
[group('dev')]
fix:
  cargo fmt --all
  cargo clippy --fix --allow-dirty --allow-staged --workspace --all-targets -- -D warnings

# Install kache to ~/.cargo/bin and register the daemon service.
[group('dev')]
install:
  cargo install --path .
  kache daemon install

# Build the release binary.
[group('build')]
build:
  cargo build --release

# Build the remote service binary.
[group('build')]
build-service:
  cargo build --release -p kache-service

# Build the service container image locally.
[group('docker')]
image-service:
  docker buildx bake -f docker-bake.hcl service

# Print the resolved service image bake plan.
[group('docker')]
image-service-print:
  docker buildx bake -f docker-bake.hcl --print service

# Build and push the release service image.
[group('docker')]
image-service-release:
  docker buildx bake -f docker-bake.hcl release

# Run the full workspace test suite.
[group('dev')]
test:
  cargo test --workspace

# Audit dependencies against the RustSec advisory database. Two
# upstream-blocked findings are ignored via `.cargo/audit.toml`; the
# file documents the rationale and re-evaluation trigger.
[group('dev')]
audit:
  cargo audit

# Run the end-to-end harness against every e2e scenario in scenarios/.
# Builds kache + harness in release mode, drives each fixture through
# cold → warm → noop, asserts per-fixture contracts against
# `kache report --format json`. `suite:e2e` picks fixture scenarios;
# `tier:gate` selects the fast checked-in fixture scenarios.
# Writes tmp/e2e/results.json.
[group('dev')]
e2e:
  cargo build --release -p kache
  cargo build --release -p kache-e2e
  ./target/release/kache-scenario \
    --kache ./target/release/kache \
    --scenarios ./scenarios \
    --select suite:e2e \
    --select tier:gate \
    --out tmp/e2e/results.json

# Run the same gate e2e harness inside a Linux container — CI-equivalent
# results from a non-Linux host (e.g. macOS), where the host clang behaves
# differently from CI's Linux clang. Useful to validate quick compile-cache
# "cases" (e.g. the issue-#411 Firefox-flag fixture) on both OSes without
# the slow full benchmarks.
#
# Runs against a copy of the tree on a CONTAINER-NATIVE named volume, not a
# bind mount: kache restores hits via hardlink and the harness relocates
# source trees, both of which break on Docker Desktop's macOS bind-mount fs
# (cross-device hardlinks; virtiofs copy perms). The volume also keeps the
# multi-GB cargo target OFF the host — only results.json is copied back to
# tmp/e2e/. The work volume persists, so rebuilds are incremental; reset it
# with `docker volume rm kache-e2e-work`. Extra ARGS pass through to
# kache-scenario (e.g. `just e2e-docker --select name:e2e-cc-cl-xclang-deps`).
[group('dev')]
e2e-docker *ARGS:
  docker build -f docker/e2e.Dockerfile -t kache-e2e:local .
  mkdir -p {{justfile_directory()}}/tmp/e2e
  docker run --rm \
    -v {{justfile_directory()}}:/src:ro \
    -v kache-e2e-work:/work \
    -v {{justfile_directory()}}/tmp/e2e:/out \
    -v kache-e2e-cargo-registry:/usr/local/cargo/registry \
    kache-e2e:local \
    bash -euo pipefail -c '\
      rsync -a --delete --exclude=.git --exclude=/target --exclude=/tmp /src/ /work/ && \
      cd /work && \
      cargo build --release -p kache && \
      cargo build --release -p kache-e2e && \
      ./target/release/kache-scenario \
        --kache ./target/release/kache \
        --scenarios ./scenarios \
        --select suite:e2e \
        --select tier:gate \
        --out /out/results.json {{ARGS}}'

# Verify the `KACHE_FALLBACK` wrapper delegates to — and is cached by —
# a real sccache. Builds an excluded rlib through kache twice and
# asserts the rebuild is an sccache cache hit. Skips if sccache is not
# installed.
[group('dev')]
sccache-check:
  cargo build --release -p kache
  ./scripts/sccache-fallback-check.sh ./target/release/kache

# Builds a benchmark scenario (see scenarios/) twice against one shared kache
# cache — cold (empty cache) then warm (cache populated by cold) — and
# reports cold/warm wall-clock, speedup, hit rate, and a correctness verdict.
# Tens of minutes to hours, tens of GB of disk; NOT run in CI. Flags pass through
# (`just bench firefox --skip-clone`). See scenarios/README.md.
# Omit PROFILE to list the matching benchmark profiles.
# PROFILE is a name filter — e.g. `firefox` matches `bench-firefox`.
# Scratch lives under ./tmp/bench/<scenario> (per-scenario; override with --work-dir).
[group('bench')]
bench PROFILE="" *ARGS:
  @if [ -z "{{PROFILE}}" ]; then \
    cargo build -q --release -p kache-e2e --bin kache-scenario; \
    ./target/release/kache-scenario --list --select suite:bench --select backend:kache; \
  else \
    cargo build --release -p kache; \
    cargo build --release -p kache-e2e --bin kache-scenario; \
    ./target/release/kache-scenario --kache ./target/release/kache --select suite:bench --select backend:kache --profile "{{PROFILE}}" {{ARGS}}; \
  fi

# Retry the warm phase only — restores the cold-state cache snapshot
# saved by the previous full run and re-measures warm against it. Skips
# the cold rebuild. Requires a prior successful run for the same scenario.
[group('bench')]
bench-retry PROFILE="" *ARGS:
  @if [ -z "{{PROFILE}}" ]; then \
    cargo build -q --release -p kache-e2e --bin kache-scenario; \
    ./target/release/kache-scenario --list --select suite:bench --select backend:kache; \
  else \
    cargo build --release -p kache; \
    cargo build --release -p kache-e2e --bin kache-scenario; \
    ./target/release/kache-scenario --kache ./target/release/kache --select suite:bench --select backend:kache --profile "{{PROFILE}}" --retry {{ARGS}}; \
  fi

# Full bench with `kache::cache_key=trace` enabled in both phases. After
# warm, the bench diffs the two phases' key-input traces per crate and
# writes `key-diff.{json,md}` listing what diverged across clones — the
# actionable signal when key stability drops below 100%. Trace logs grow
# by ~50–100 MB per phase.
[group('bench')]
bench-trace PROFILE="" *ARGS:
  @if [ -z "{{PROFILE}}" ]; then \
    cargo build -q --release -p kache-e2e --bin kache-scenario; \
    ./target/release/kache-scenario --list --select suite:bench --select backend:kache; \
  else \
    cargo build --release -p kache; \
    cargo build --release -p kache-e2e --bin kache-scenario; \
    ./target/release/kache-scenario --kache ./target/release/kache --select suite:bench --select backend:kache --profile "{{PROFILE}}" --trace-keys {{ARGS}}; \
  fi

# Same cold/warm clone benchmark, but with sccache as the compiler cache.
# Omit PROFILE to list sccache-backed profiles.
# Use `just bench-sccache firefox` for the Firefox comparison.
[group('bench')]
bench-sccache PROFILE="" *ARGS:
  @if [ -z "{{PROFILE}}" ]; then \
    cargo build -q --release -p kache-e2e --bin kache-scenario; \
    ./target/release/kache-scenario --list --cache-backend sccache --select suite:bench --select backend:sccache; \
  else \
    cargo build --release -p kache-e2e --bin kache-scenario; \
    ./target/release/kache-scenario --cache-backend sccache --select suite:bench --select backend:sccache --profile "{{PROFILE}}" {{ARGS}}; \
  fi

# Run clippy with deny warnings.
[group('dev')]
lint:
  cargo clippy --workspace --all-targets -- -D warnings

# Format the workspace.
[group('dev')]
fmt:
  cargo fmt --all

# Check formatting without changing files.
[group('dev')]
fmt-check:
  cargo fmt --all -- --check

# Lint the deployable Helm chart.
[group('deploy')]
helm-lint:
  helm lint charts/kache-service

# Run cargo-llvm-cov and emit JSON + HTML reports under tmp/llvm-cov/.
# JSON drives the CI threshold check; HTML is uploaded as a CI artifact
# (and opened locally by `coverage-open`). `--no-report` collects
# coverage once; the two `report` invocations then emit the formats
# from that single test run.
[group('coverage')]
coverage:
  cargo llvm-cov --all-features --workspace --no-report
  cargo llvm-cov report --html --output-dir tmp/llvm-cov
  cargo llvm-cov report --json --output-path tmp/llvm-cov/coverage.json

# Run cargo-llvm-cov and open the HTML report locally.
[group('coverage')]
coverage-open:
  cargo llvm-cov --all-features --workspace --html --output-dir tmp/llvm-cov
  open tmp/llvm-cov/html/index.html || \
    xdg-open tmp/llvm-cov/html/index.html || true

# Show kache CI cache metrics from GitHub Actions.
[group('ops')]
monitor *ARGS:
  ./scripts/ci-monitor.sh {{ARGS}}

# Bump the workspace version everywhere in one shot — all 4 member manifests,
# the kache-core dep-pin, and Cargo.lock — via `cargo set-version` (NOT a broad
# `cargo update`, so the pinned kunobi-* git deps and the hand-maintained nix
# `outputHashes` stay valid; the flake derives `version` from Cargo.toml, so no
# hash change is needed). The VERSION is the full version, prerelease included:
# `just bump 0.5.0` for a final, `just bump 0.5.0-rc.4` for a candidate — both
# publish to crates.io (a prerelease is only served on an explicit --version).
# Then commit, open a PR, and merge; cut the tag from the merged commit with
# `just release` (never re-typed). Auto-installs `cargo-edit` on first use — it
# is not pinned in mise.toml because that compiles it in every CI run.
#
# Use a DOTTED-numeric prerelease (`-rc.4`, not `-rc4`): crates.io is permanent
# and semver orders the no-dot form lexically (`rc.2` would sort after `rc.10`).
# And don't bump back into an `-rc` for a version whose final already shipped
# (e.g. a `0.5.0-rc.5` after `0.5.0` is published) — the version gate checks
# tag==manifest, not crates.io monotonicity, so that would publish a permanent
# "prerelease of an already-released version".
# Usage: `just bump 0.5.0`  /  `just bump 0.5.0-rc.4`
[group('release')]
bump VERSION:
  #!/usr/bin/env bash
  set -euo pipefail
  # Reject the no-dot prerelease form (-rc4): it sorts lexically on crates.io,
  # which is permanent. Require -rc.4 / -alpha.2 / -beta.1 (dotted numeric).
  case "{{VERSION}}" in
    *-rc[0-9]*|*-alpha[0-9]*|*-beta[0-9]*)
      echo "use a dotted prerelease (e.g. 0.5.0-rc.4), not the no-dot form — semver sorts no-dot lexically on crates.io" >&2
      exit 1 ;;
  esac
  if ! command -v cargo-set-version >/dev/null 2>&1; then
    echo "cargo-edit (cargo set-version) not found — installing it…"
    if command -v cargo-binstall >/dev/null 2>&1; then
      cargo binstall -y cargo-edit   # prebuilt, fast
    else
      cargo install cargo-edit       # source build (one-time)
    fi
  fi
  cargo set-version --workspace {{VERSION}}
  # NO --locked: set-version rewrites the lock's version entries, so --locked
  # would error "lock file needs updating". Plain check settles the lock for the
  # local crates only (it does not advance kunobi-* / registry deps).
  cargo check --workspace
  ./scripts/check-version-consistency.sh
  echo "Bumped to {{VERSION}}. Commit + open a PR; after merge, cut the tag with 'just release'."

# Refuses unless the tree is releasable (clean, on `main`, in sync with
# origin/main, so a tag is never cut from a dirty / off-main / un-pulled
# commit), runs the consistency gate, then pushes the tag → gated pipeline →
# crates.io. The version (final OR prerelease, e.g. 0.5.0-rc.4) comes from the
# merged manifest — never re-typed.
# Cut the release tag for the merged manifest version. Usage: `just release`
[group('release')]
release:
  #!/usr/bin/env bash
  set -euo pipefail
  [ -z "$(git status --porcelain)" ] || { echo "working tree is dirty — commit or stash first" >&2; exit 1; }
  branch="$(git rev-parse --abbrev-ref HEAD)"
  [ "$branch" = "main" ] || { echo "not on main (on '$branch') — releases are cut from main" >&2; exit 1; }
  git fetch --quiet origin main
  [ "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)" ] || { echo "local main is not in sync with origin/main — pull/push first" >&2; exit 1; }
  version="$(cargo metadata --no-deps --format-version 1 \
    | python3 -c 'import json,sys; print(next(p["version"] for p in json.load(sys.stdin)["packages"] if p["name"]=="kache"))')"
  tag="v${version}"
  ./scripts/check-version-consistency.sh "$tag"
  git rev-parse -q --verify "refs/tags/$tag" >/dev/null && { echo "tag $tag already exists" >&2; exit 1; } || true
  git tag -a "$tag" -m "$tag"
  git push origin "$tag"
  echo "pushed $tag — the gated release pipeline will run; watch CI."

# Remove build artifacts.
clean:
  cargo clean
