[![CI](https://github.com/kunobi-ninja/kache/actions/workflows/ci.yml/badge.svg)](https://github.com/kunobi-ninja/kache/actions/workflows/ci.yml)
[![Bench](https://github.com/kunobi-ninja/kache/actions/workflows/bench.yml/badge.svg)](https://github.com/kunobi-ninja/kache/actions/workflows/bench.yml)
[![Crates.io](https://img.shields.io/crates/v/kache.svg)](https://crates.io/crates/kache)
[![Documentation](https://img.shields.io/badge/docs-kunobi.ninja-blue)](https://kunobi.ninja/docs/kache)

# Kache

Kache is a local-first compiler cache for Rust and C/C++. It stores build outputs by content, reuses them across worktrees, and can copy them to S3-compatible or filesystem remotes.

## Try it without changing your setup

Install Kache:

```bash
cargo install kache
```

Run two clean builds from a Rust project:

```bash
RUSTC_WRAPPER=kache cargo build
cargo clean
KACHE_PROGRESS=hits RUSTC_WRAPPER=kache cargo build
kache stats
```

`cargo clean` is only for this demonstration. Kache normally helps when Cargo would otherwise compile an input it has seen before, such as in another worktree or after changing toolchains and changing back.

The commands above do not edit Cargo configuration or install a service. To keep Kache enabled:

```bash
kache init
```

Review the proposed changes without applying them:

```bash
kache init --check
```

Use `kache init --no-service` if you want persistent Cargo configuration without an OS service.

## What Kache caches

| Workload | Status | Notes |
| --- | --- | --- |
| Rust libraries and build scripts | Supported | Use `RUSTC_WRAPPER=kache` or `kache init` |
| Rust executables | Supported on Linux and macOS | Disabled by default on Windows |
| C and C++ object files | Supported | Use compiler shims or set the compiler launcher |
| Local storage | Built in | Content-addressed store with garbage collection |
| S3-compatible remote storage | Built in | Includes AWS S3, MinIO, and Cloudflare R2 |
| Filesystem remote storage | Built in | Useful for shared disks and CI volumes |

Need to choose between compiler caches? Read [Kache or sccache?](https://kunobi.ninja/docs/kache/getting-started/comparison).

## Tested nightly on real projects

The scheduled [benchmark workflow](https://github.com/kunobi-ninja/kache/actions/workflows/bench.yml) runs real cold/warm builds of Firefox, LLVM, Substrate, SurrealDB, and Lance on Linux, compares Firefox with sccache, and exercises Firefox on Windows. It also measures how much of a Firefox build survives a source update.

Each run checks its own measurement validity and uploads reports, traces, and logs for 30 days. Treat timing or hit-rate numbers as evidence only when the individual job succeeds and its benchmark verdict is `ok`.

## CI

The official action installs Kache and wires it into the build:

```yaml
- uses: kunobi-ninja/kache-action@v1

- run: cargo build --locked
```

See the [CI guide](https://kunobi.ninja/docs/kache/remote-cache/ci) for GitHub Actions and shell-based CI examples.

## C and C++

Create compiler-name shims, then put their directory first in `PATH`:

```bash
kache install-shims ~/.local/lib/kache/shims
export PATH="$HOME/.local/lib/kache/shims:$PATH"
cmake -S . -B build
cmake --build build
```

Kache inspects the real compiler invocation. Unsupported or unsafe invocations pass through to the compiler instead of being cached.

## Storage and remotes

The default local cache is:

- Linux: `$XDG_CACHE_HOME/kache` or `~/.cache/kache`
- macOS: `~/Library/Caches/kache`
- Windows: `%LOCALAPPDATA%\kache`

Open the configuration editor with `kache config`, or edit the TOML file directly. A minimal S3-compatible remote looks like this:

```toml
[cache.remote]
type = "s3"
bucket = "my-build-cache"
region = "us-east-1"
```

Credentials come from the standard AWS environment variables or credential chain. See [S3 setup](https://kunobi.ninja/docs/kache/remote-cache/s3-setup) and [filesystem setup](https://kunobi.ninja/docs/kache/remote-cache/filesystem-setup).

## Useful commands

```bash
kache monitor                 # live build and cache activity
kache stats                   # non-interactive summary
kache doctor                  # setup and integrity checks
kache why-miss <crate>        # explain the latest miss
kache list                    # inspect cached entries
kache gc                      # enforce cache limits
kache sync                    # pull from and push to the configured remote
kache daemon status           # inspect the background service
```

Run `kache help <command>` for exact flags. The [command reference](https://kunobi.ninja/docs/kache/commands/reference) covers every top-level command.

## Documentation

- [Install Kache](https://kunobi.ninja/docs/kache/getting-started/installation)
- [Quick start](https://kunobi.ninja/docs/kache/getting-started/quick-start)
- [Configuration](https://kunobi.ninja/docs/kache/getting-started/configuration)
- [How cache keys work](https://kunobi.ninja/docs/kache/how-it-works/cache-key)
- [Daemon lifecycle](https://kunobi.ninja/docs/kache/daemon/lifecycle)
- [Benchmarks](https://kunobi.ninja/docs/kache/benchmarks)

## Questions and gaps

- [Open a bug report](https://github.com/kunobi-ninja/kache/issues/new?template=bug_report.md) when Kache behaves differently from the documentation.
- [Request a feature](https://github.com/kunobi-ninja/kache/issues/new?template=feature_request.md) for a missing compiler, remote backend, or build workflow.

## Development

```bash
git clone https://github.com/kunobi-ninja/kache.git
cd kache
cargo test --workspace --all-features
```

See [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request.

Kache is licensed under the [Apache License 2.0](LICENSE).
