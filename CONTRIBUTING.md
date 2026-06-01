# Contributing to kache

Thank you for your interest in contributing to kache! This document covers the development setup, coding conventions, and pull request process.

## Getting started

### Prerequisites

- [mise](https://mise.jdx.dev/) (recommended)
- Rust **1.95+**
- `just`

### Clone and build

```sh
git clone https://github.com/kunobi-ninja/kache.git
cd kache
mise install
just build
```

### Install locally

```sh
just install   # installs to ~/.cargo/bin and registers the daemon service
```

> **Note:** The `Justfile` exports `RUSTC_WRAPPER=` to avoid a bootstrapping loop (kache building itself through kache).

## Development workflow

All common tasks live in the `Justfile` — prefer these over raw `cargo` commands:

```sh
just check          # fmt + clippy + tests (run before every PR)
just ci             # mirrors the GitHub Actions verification flow
just test           # run all tests
just lint           # clippy with -D warnings
just fmt            # auto-format code
just fix            # auto-fix formatting + clippy warnings
just coverage       # tests with tarpaulin coverage (JSON)
just coverage-open  # coverage with HTML report
just clean          # remove build artifacts
```

## Code style

- **Edition**: Rust 2024
- **Formatting**: `cargo fmt` (default rustfmt settings)
- **Linting**: `cargo clippy -- -D warnings` — all warnings are errors
- **Error handling**: Use `anyhow::Result` with `.context()` / `.with_context()` for descriptive errors. Avoid bare `.unwrap()` on I/O or network operations.
- **Unsafe code**: Avoid unless strictly necessary (OS-level FFI). Document safety invariants with `// SAFETY:` comments.

## Testing

- **Unit tests**: Place `#[cfg(test)]` modules at the bottom of source files
- **Integration tests**: Add to `tests/` — these run real binaries against temp directories
- **Test fixtures**: Reusable test projects live in `test-projects/`
- **Coverage threshold**: CI enforces a minimum of 25% via `cargo-tarpaulin`

Run the full check suite before submitting a PR:

```sh
just check
```

## Pull request process

1. **Fork** the repository and create a feature branch from `main`
2. Make your changes — keep commits focused and use [conventional commit](https://www.conventionalcommits.org/) messages (e.g., `feat:`, `fix:`, `test:`, `docs:`)
3. Run `just check` and ensure it passes
4. Open a pull request against `main` (see [Branching and releases](#branching-and-releases))
5. Describe what the PR does and why — link related issues if any

### PR guidelines

- Keep PRs small and focused on a single change
- Add tests for new functionality
- Don't bundle unrelated refactors with feature work
- CI must pass before merge (fmt, clippy, tests, coverage threshold)

## Branching and releases

kache is trunk-based: `main` is the single long-lived branch. All work lands on
`main` through reviewed PRs, and releases are tagged directly on `main`. There is
no `dev` branch.

| Branch | Role |
|---|---|
| `main` | The trunk. All feature/fix PRs land here and CI runs on every PR. Release tags `vX.Y.Z` point at commits on `main`. |
| `release/X.Y` | *(rare, short-lived)* Cut from a release tag only to back-port fixes to an already-shipped line after `main` has moved past it. |

### Cutting a release (maintainers)

```sh
# 1. Bump the workspace version in a PR: edit Cargo.toml + crates/*/Cargo.toml
#    version = "X.Y.Z", refresh Cargo.lock, `just check`. Merge it into main.

# 2. Push the tag — let CI create the release. Do NOT draft the release by
#    hand in the UI (see note below).
git tag vX.Y.Z && git push origin vX.Y.Z
#    The tag triggers CI; once check + nix + e2e + macOS tests pass, the
#    release job builds the binaries and creates the GitHub Release. Publishing
#    the release then triggers the crates.io workflow
#    (.github/workflows/publish-crates.yaml), which publishes kache-core then
#    kache in dependency order.
```

The version is bumped *in* the release PR, so `main` advertises a number only
once the commit that carries it is on the trunk.

**Why push the tag instead of drafting in the UI:** the release job only runs
after the full CI suite passes, so a CI-created release is gated on green CI. A
hand-drafted release fires `release: published` immediately, racing CI. As a
safety net the publish job re-checks (`scripts/require-ci-green.sh`) that CI
passed for the tagged commit and refuses to publish otherwise — but pushing the
tag is the intended, friction-free path.

### Publishing to crates.io

Publishing is automated by `.github/workflows/publish-crates.yaml`, triggered when
a GitHub **Release** is published. It uses crates.io **Trusted Publishing** (OIDC,
no stored token), publishes `kache-core` then `kache` (in dependency order), and
is idempotent (skips versions already on crates.io).

**First publish of a *new* crate is manual.** Trusted Publishing tokens **cannot
create a new crate** — crates.io requires the first publish to claim ownership with
a personal token. When adding a crate, bootstrap it once:

```sh
git checkout vX.Y.Z              # publish from the tag, never a moving branch
cargo login                      # personal token from crates.io → Account → API Tokens
cargo publish -p <new-crate> --locked
```

Then configure Trusted Publishing for that crate on crates.io (repo +
`publish-crates.yaml` workflow, matching any Environment the others use) so all
later releases publish automatically. Crates are published **individually** in
dependency order — publishing `kache` does **not** publish `kache-core`; the
dependency must already be on crates.io first.

## Project structure

```
src/
├── main.rs          CLI entry point, daemon management
├── wrapper.rs       RUSTC_WRAPPER hot path
├── cache_key.rs     blake3 cache key computation
├── store.rs         SQLite-backed local cache
├── daemon.rs        Background daemon + S3 sync
├── remote.rs        S3 operations
├── cli.rs           Subcommand implementations
├── config.rs        Configuration loading
├── config_tui.rs    TUI config editor
├── tui.rs           Live monitoring dashboard
├── args.rs          rustc argument parsing
├── compile.rs       rustc invocation
├── link.rs          Hardlink/reflink strategies
├── events.rs        Build event tracking
├── service.rs       launchd/systemd integration
└── shards.rs        Content-addressed shard management
tests/               Integration tests
test-projects/       Fixture projects for testing
```

## Reporting issues

Found a bug or have a feature request? Open an [issue](https://github.com/kunobi-ninja/kache/issues). Include:

- kache version (`kache --version`)
- OS and Rust toolchain version
- Steps to reproduce
- Relevant logs (`KACHE_LOG=kache=debug`)

## License

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
