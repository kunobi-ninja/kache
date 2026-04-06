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

# Run tarpaulin coverage and emit JSON.
[group('coverage')]
coverage:
  cargo tarpaulin --engine llvm --all-features --workspace --out Json

# Run tarpaulin coverage and open the HTML report locally.
[group('coverage')]
coverage-open:
  cargo tarpaulin --engine llvm --all-features --workspace --out Html
  open tarpaulin-report.html || xdg-open tarpaulin-report.html || true

# Show kache CI cache metrics from GitHub Actions.
[group('ops')]
monitor *ARGS:
  ./scripts/ci-monitor.sh {{ARGS}}

# Remove build artifacts.
clean:
  cargo clean
