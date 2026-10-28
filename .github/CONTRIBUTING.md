# Contributing to kache

Thank you for your interest in contributing to kache! This document covers the development setup, coding conventions, and pull request process.

## Getting started

### Prerequisites

- [mise](https://mise.jdx.dev/) (recommended)
- Rust, at the version pinned in `rust-toolchain.toml` (rustup and `mise install` both read it)
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
just install   # installs to $CARGO_HOME/bin (default ~/.cargo/bin) and registers the daemon service
```

> **Note:** The `Justfile` exports `RUSTC_WRAPPER=` to avoid a bootstrapping loop (kache building itself through kache).

## Development workflow

Development and test builds omit debug information and strip symbols. To
inspect Kache with a debugger or get symbolized backtraces, override both:

```sh
CARGO_PROFILE_DEV_DEBUG=2 CARGO_PROFILE_DEV_STRIP=none cargo build
```

All common tasks live in the `Justfile` — prefer these over raw `cargo` commands:

```sh
just check          # fmt + clippy + tests
just pr             # check + local changed-line mutants (optional; needs a capable Linux host)
just ci             # mirrors the GitHub Actions verification flow
just test           # run all tests
just lint           # clippy with -D warnings
just fmt            # auto-format code
just fix            # auto-fix formatting + clippy warnings
just coverage       # tests with cargo-llvm-cov coverage
just coverage-open  # coverage with HTML report
just mutants-core   # mutation-test all kache-core behavior
just e2e-s3         # S3 remote tests against a throwaway local RustFS
just clean          # remove build artifacts
```

### Hardening GitHub Actions

Every action in `.github/` is pinned to a full commit SHA rather than a mutable
`@vN` tag, so a compromised or repointed tag can't inject code into CI or the
release pipeline. When you add or bump an action, run:

```sh
just pin-actions        # rewrite uses: owner/repo@vN -> @<sha> # vN, then commit
just pin-actions-check  # verify everything is pinned (no edits); used to guard it
```

Both use [pinact](https://github.com/suzuki-shunsuke/pinact) (pinned in
`mise.toml`) and need a GitHub token to resolve tags — they fall back to
`gh auth token`. `dtolnay/rust-toolchain` has no semver tags, so it's pinned to
a commit SHA by hand (with a `toolchain: stable` input) and excluded from pinact
in `.pinact.yaml`; bump its SHA manually when you want a newer release.

## Code style

- **Edition**: Rust 2024
- **Formatting**: `cargo fmt` (default rustfmt settings)
- **Linting**: `cargo clippy -- -D warnings` — all warnings are errors
- **Error handling**: Use `anyhow::Result` with `.context()` / `.with_context()` for descriptive errors. Avoid bare `.unwrap()` on I/O or network operations.
- **Unsafe code**: Avoid unless strictly necessary (OS-level FFI). Document safety invariants with `// SAFETY:` comments.

## Writing docs

Docs, the README, PR descriptions and release notes should read like a maintainer explaining kache to a colleague who is about to use it.

- Start with the task, then the command or setting that does it. Edge cases and design reasons come after the normal path.
- One idea per sentence. Use active voice and the real names of commands, paths and settings.
- Match the code on `main`: commands, flags, defaults, paths, limits and platform support. A PR that changes one of these updates the page that describes it.
- State limits plainly: what doesn't work, and on which platform.

Some habits make text vague or padded. Prefer the plain form:

| Instead of | Write |
| --- | --- |
| an em dash joining two clauses | a full stop, a colon or parentheses |
| "seamless", "robust", "powerful", "simply" | what actually happens |
| "not X, but Y" | Y |
| a list of three added for rhythm | only the items that matter |
| a bold label before every bullet | plain bullets, or a table |
| an "Overview" heading or a closing summary | a heading that names the task, and an end when the content ends |

Read a change aloud before opening the PR. If it sounds like marketing copy or a chat reply, rewrite it.

### Keep the published docs in sync

`docs/` is the canonical product documentation. The Ninja website imports it
into `apps/ninja/content/docs/kache` in `Zondax/kunobi-web`, rewriting `/docs/...`
links to `/docs/kache/...`. After a documentation change merges, refresh that
copy with the website's `pnpm docs:sync` command and review the resulting diff.
Edit the upstream pages here rather than maintaining a second version there.

For behavior changes, check release notes and the PR, then confirm the current
implementation. A release-preparation commit is not necessarily the release
tag: use the tag's actual contents to decide which version contains a feature.
Keep historical benchmark versions and measurements attached to their original
runs.

Check the README, the task guide, configuration and command references, and any
affected crate README. CLI routing and options come from
`crates/kache-client/src/commands.rs`; `kache --json --help` describes the visible
command tree. Run `bash scripts/check-docs.sh` for routes, navigation, commands,
and configuration-field coverage.

## Testing

- **Unit tests**: Place `#[cfg(test)]` modules at the bottom of source files
- **Integration tests**: Add to `tests/` — these run real binaries against temp directories
- **Scenarios**: E2E fixture scenarios live under `scenarios/e2e-*`; benchmark scenarios live under `scenarios/bench-*`
- **Coverage threshold**: CI enforces a minimum of 88% via `cargo-llvm-cov`
- **Mutation testing**: CI mutates all `kache-core` behavior plus changed Rust
  lines in each PR. Lines a PR only moves, which git detects as moved blocks,
  do not count as changed (`scripts/mutation-diff.py`), so splitting a file
  mutates only what the split edits. Reports are uploaded from `tmp/mutants/`.

For local mutation runs, install the same version as CI:

```sh
cargo install --locked cargo-mutants --version 27.1.0
just mutants-core
```

Coding agents: read [AGENTS.md](../AGENTS.md) before opening a PR. The usual CI failure is Mutation testing after a push that only ran `just check`.

Run `just check` before opening a PR. It checks formatting, clippy with
`-D warnings`, and workspace tests. CI also mutation-tests changed Rust lines.
Open the PR as a draft, inspect missed mutations in the uploaded reports, and
mark it ready when the required checks pass.

```sh
just check
bash scripts/check-docs.sh  # when documentation or configuration changes
```

`just pr` also runs the mutation diff locally. Use it on a Linux host with enough
cores; laptop runs can take hours. Install the same cargo-mutants version as CI
first (`cargo install --locked cargo-mutants --version 27.1.0`). A docs-only diff
skips mutation tests in CI. Do not run cargo-mutants through Kache itself.

Clippy on macOS does not type-check `#[cfg(target_os = "linux")]` functions. Identity conversions (`u64::try_from` on a `u64`) and unused Unix-only test helpers fail on Linux CI under `-D warnings` even when `just lint` is green locally.

## Pull request process

1. **Fork** the repository and create a feature branch from `main`
2. Make your changes — keep commits focused and use [conventional commit](https://www.conventionalcommits.org/) messages (e.g., `feat:`, `fix:`, `test:`, `docs:`)
3. Run `just check` and the checks specific to the change
4. Open a draft pull request against `main` (see [Branching and releases](#branching-and-releases)); mark it ready after required CI passes
5. Describe what the PR does and why — link related issues if any

### PR guidelines

- Keep PRs small and focused on a single change
- Add tests for new functionality
- Don't bundle unrelated refactors with feature work
- CI must pass before merge (fmt, clippy, tests, coverage, changed-line mutants)

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
# 1. Bump the workspace version in a PR, then merge it into main.
just bump X.Y.Z

# 2. From a tree that is origin/main, push the tag — let CI create the
#    release. Do NOT draft the release by hand in the UI (see note below).
just release
#    The tag triggers CI; once check + nix + e2e + macOS tests pass, the
#    release job builds the binaries and creates the GitHub Release. Publishing
#    the release then triggers the crates.io workflow
#    (.github/workflows/publish-crates.yaml), which publishes every
#    publishable workspace crate in dependency order.
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
no stored token), publishes every publishable workspace crate in dependency
order (discovered from the manifests — adding a crate does not require a
workflow edit), and is idempotent (skips versions already on crates.io).

**First publish of a *new* crate is still a one-time claim.** Trusted Publishing
tokens **cannot create a new crate** — crates.io requires the first publish to
claim ownership with a personal token. From a clean worktree at the release tag:

```sh
# Token scopes: publish-new + trusted-publishing
# (CARGO_REGISTRY_TOKEN, or `cargo login`)
just crates-status           # what is missing
just crates-bootstrap-plan   # plan (no writes)
just crates-bootstrap        # first-publish + Trusted Publishing
```

That publishes only crates that do not exist yet, attaches Trusted Publishing
for `kunobi-ninja/kache` + `publish-crates.yaml` (no GitHub Environment, matching
the publish job), and enables `trustpub_only`. Re-run the Publish crates
workflow afterwards if it failed on the create. Later versions of that crate
publish automatically.

## Project structure

```
src/
├── frontend.rs      Small kache executable, through kache-client
├── engine.rs        kache-engine executable entry point
├── main.rs          Application library and engine dispatch
├── wrapper.rs       RUSTC_WRAPPER hot path
├── cache_key.rs     blake3 cache key computation
├── store.rs         Application adapter for kache-store
├── daemon.rs        Background daemon + S3 sync
├── remote.rs        Remote cache orchestration
├── cli.rs           Subcommand implementations
├── config.rs        kache-config re-exports
├── config_tui.rs    TUI config editor
├── tui.rs           Live monitoring dashboard
├── args.rs          rustc argument parsing
├── compile.rs       rustc invocation
├── link.rs          Hardlink/reflink strategies
├── events.rs        Build event tracking
├── service.rs       launchd/systemd integration
└── shards.rs        Content-addressed shard management
crates/              Shared client, config, protocol, IPC, storage, and filesystem libraries
docs/                Canonical product documentation imported by the website
tests/               Integration tests
scenarios/           E2E and benchmark scenarios
test-projects/       Integration-test helper projects
```

## Reporting issues

Found a bug or have a feature request? Open an [issue](https://github.com/kunobi-ninja/kache/issues). Include:

- kache version (`kache --version`)
- OS and Rust toolchain version
- Steps to reproduce
- Relevant logs (`KACHE_LOG=kache=debug`)

## License

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](../LICENSE).
