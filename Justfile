# Don't use kache to build kache (bootstrapping problem).
export RUSTC_WRAPPER := ""

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

# Run the end-to-end harness against every fixture in test-projects/.
# Builds kache + harness in release mode, drives each fixture through
# cold → warm → noop, asserts per-fixture contracts against
# `kache report --format json`. Writes tmp/e2e/results.json.
[group('dev')]
e2e:
  cargo build --release -p kache
  cargo build --release -p kache-e2e
  ./target/release/kache-e2e \
    --kache ./target/release/kache \
    --fixtures ./test-projects \
    --out tmp/e2e/results.json

# Verify the `KACHE_FALLBACK` wrapper delegates to — and is cached by —
# a real sccache. Builds an excluded rlib through kache twice and
# asserts the rebuild is an sccache cache hit. Skips if sccache is not
# installed.
[group('dev')]
sccache-check:
  cargo build --release -p kache
  ./scripts/sccache-fallback-check.sh ./target/release/kache

# Builds a project (see bench-profiles/) twice against one shared kache
# cache — cold (empty cache) then warm (cache populated by cold) — and
# reports cold/warm wall-clock, speedup, and hit rate. Tens of minutes to
# hours, tens of GB of disk; NOT run in CI. Flags pass through
# (`just bench firefox --skip-clone`). See bench-profiles/README.md.
# PROFILE is required — e.g. `just bench firefox`, `just bench substrate`.
[group('bench')]
bench PROFILE *ARGS:
  cargo build --release -p kache
  cargo build --release -p kache-e2e --bin kache-bench
  ./target/release/kache-bench --kache ./target/release/kache --profile {{PROFILE}} {{ARGS}}

# Retry the warm phase only — restores the cold-state cache snapshot
# saved by the previous full run and re-measures warm against it. Skips
# the cold rebuild. Requires a prior successful run for the same profile.
[group('bench')]
bench-retry PROFILE *ARGS:
  cargo build --release -p kache
  cargo build --release -p kache-e2e --bin kache-bench
  ./target/release/kache-bench --kache ./target/release/kache --profile {{PROFILE}} --retry {{ARGS}}

# Full bench with `kache::cache_key=trace` enabled in both phases. After
# warm, the bench diffs the two phases' key-input traces per crate and
# writes `key-diff.{json,md}` listing what diverged across clones — the
# actionable signal when key stability drops below 100%. Trace logs grow
# by ~50–100 MB per phase.
[group('bench')]
bench-trace PROFILE *ARGS:
  cargo build --release -p kache
  cargo build --release -p kache-e2e --bin kache-bench
  ./target/release/kache-bench --kache ./target/release/kache --profile {{PROFILE}} --trace-keys {{ARGS}}

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

# Remove build artifacts.
clean:
  cargo clean
