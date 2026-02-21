.PHONY: install build test check fix lint fmt fmt-check coverage coverage-open clean help

# Don't use kache to build kache (bootstrapping problem)
export RUSTC_WRAPPER=

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

check: fmt-check lint test ## Run all checks (format, lint, test)

fix: ## Auto-fix formatting and clippy warnings
	cargo fmt --all
	cargo clippy --workspace --fix --allow-dirty --allow-staged -- -D warnings

install: ## Install kache to ~/.cargo/bin and register daemon service
	cargo install --path kache
	kache daemon install

build: ## Build release binary
	cargo build --workspace --release

test: ## Run all tests
	cargo test --workspace

lint: ## Run clippy with deny warnings
	cargo clippy --workspace -- -D warnings

fmt: ## Format code
	cargo fmt --all

fmt-check: ## Check formatting (CI)
	cargo fmt --all -- --check

coverage: ## Run tests with tarpaulin coverage (JSON output)
	cargo tarpaulin --engine llvm --all-features --workspace --out Json

coverage-open: ## Run coverage and open HTML report
	cargo tarpaulin --engine llvm --all-features --workspace --out Html && \
		(open tarpaulin-report.html || xdg-open tarpaulin-report.html || true)

clean: ## Remove build artifacts
	cargo clean
