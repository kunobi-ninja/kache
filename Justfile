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

# Run the end-to-end harness against every fixture in test-projects/.
# Builds kache + harness in release mode, drives each fixture through
# cold → warm → noop, asserts per-fixture contracts against
# `kache report --format json`. Writes e2e-results/results.json.
[group('dev')]
e2e:
  cargo build --release -p kache
  cargo build --release -p kache-e2e
  ./target/release/kache-e2e \
    --kache ./target/release/kache \
    --fixtures ./test-projects \
    --out e2e-results/results.json

# Verify the `KACHE_FALLBACK` wrapper delegates to — and is cached by —
# a real sccache. Builds an excluded rlib through kache twice and
# asserts the rebuild is an sccache cache hit. Skips if sccache is not
# installed.
[group('dev')]
sccache-check:
  cargo build --release -p kache
  ./scripts/sccache-fallback-check.sh ./target/release/kache

# Builds Firefox twice against one shared kache cache — cold (empty cache)
# then warm (cache populated by cold) — and reports cold/warm wall-clock,
# speedup, and hit rate. Tens of minutes to hours, ~50 GB of disk; NOT run
# in CI. Flags pass through (`just bench-firefox --skip-clone`).
# Manual Firefox compile-cache benchmark — see crates/kache-e2e (kache-bench).
[group('bench')]
bench-firefox *ARGS:
  cargo build --release -p kache
  cargo build --release -p kache-e2e --bin kache-bench
  ./target/release/kache-bench --kache ./target/release/kache {{ARGS}}

# Retry the warm phase only — restores the cold-state cache snapshot
# saved by the previous full run and re-measures warm against it. Skips
# the cold rebuild (~25 min saved). Requires a prior successful run.
[group('bench')]
bench-firefox-retry *ARGS:
  cargo build --release -p kache
  cargo build --release -p kache-e2e --bin kache-bench
  ./target/release/kache-bench --kache ./target/release/kache --retry {{ARGS}}

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

# Run cargo-llvm-cov and emit JSON + HTML reports under
# target/llvm-cov/. JSON drives the CI threshold check; HTML is
# uploaded as a CI artifact (and opened locally by `coverage-open`).
# `--no-report` collects coverage once; the two `report` invocations
# then emit the formats from that single test run.
[group('coverage')]
coverage:
  cargo llvm-cov --all-features --workspace --no-report
  cargo llvm-cov report --html --output-dir target/llvm-cov
  cargo llvm-cov report --json --output-path target/llvm-cov/coverage.json

# Run cargo-llvm-cov and open the HTML report locally.
[group('coverage')]
coverage-open:
  cargo llvm-cov --all-features --workspace --html --output-dir target/llvm-cov
  open target/llvm-cov/html/index.html || \
    xdg-open target/llvm-cov/html/index.html || true

# Show kache CI cache metrics from GitHub Actions.
[group('ops')]
monitor *ARGS:
  ./scripts/ci-monitor.sh {{ARGS}}

# Remove build artifacts.
clean:
  cargo clean
