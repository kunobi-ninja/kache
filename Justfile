# Don't use kache to build kache (bootstrapping problem).
export RUSTC_WRAPPER := ""

default:
  @just --list

# Run all local quality checks.
[group('dev')]
check: fmt-check lint test

# Mirror the repo CI verification flow.
[group('dev')]
ci: fmt-check lint coverage

# Auto-fix formatting and clippy warnings.
[group('dev')]
fix:
  cargo fmt
  cargo clippy --fix --allow-dirty --allow-staged -- -D warnings

# Install kache to ~/.cargo/bin and register the daemon service.
[group('dev')]
install:
  cargo install --path .
  kache daemon install

# Build the release binary.
[group('build')]
build:
  cargo build --release

# Run all tests.
[group('dev')]
test:
  cargo test

# Run clippy with deny warnings.
[group('dev')]
lint:
  cargo clippy -- -D warnings

# Format code.
[group('dev')]
fmt:
  cargo fmt

# Check formatting without changing files.
[group('dev')]
fmt-check:
  cargo fmt -- --check

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
