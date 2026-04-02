.PHONY: install build build-service image-service test check fix lint fmt fmt-check coverage coverage-open clean monitor help

# Don't use kache to build kache (bootstrapping problem)
export RUSTC_WRAPPER=

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

check: fmt-check lint test ## Run all checks (format, lint, test)

fix: ## Auto-fix formatting and clippy warnings
	cargo fmt
	cargo clippy --fix --allow-dirty --allow-staged -- -D warnings

install: ## Install kache to ~/.cargo/bin and register daemon service
	cargo install --path .
	kache daemon install

build: ## Build release binary
	cargo build --release

build-service: ## Build the remote service binary
	cargo build --release -p kache-service

image-service: ## Build the service container image locally
	docker buildx bake -f docker-bake.hcl service

test: ## Run all tests
	cargo test --workspace

lint: ## Run clippy with deny warnings
	cargo clippy --workspace --all-targets -- -D warnings

fmt: ## Format code
	cargo fmt --all

fmt-check: ## Check formatting (CI)
	cargo fmt --all -- --check

coverage: ## Run tests with tarpaulin coverage (JSON output)
	cargo tarpaulin --engine llvm --all-features --workspace --out Json

coverage-open: ## Run coverage and open HTML report
	cargo tarpaulin --engine llvm --all-features --workspace --out Html && \
		(open tarpaulin-report.html || xdg-open tarpaulin-report.html || true)

monitor: ## Show kache CI cache metrics from GitHub Actions
	@./scripts/ci-monitor.sh $(ARGS)

clean: ## Remove build artifacts
	cargo clean
