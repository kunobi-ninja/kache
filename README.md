# kache

[![Release](https://img.shields.io/github/v/release/kunobi-ninja/kache?label=release&sort=semver&color=blue)](https://github.com/kunobi-ninja/kache/releases/latest)
[![CI](https://github.com/kunobi-ninja/kache/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/kunobi-ninja/kache/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.95-blue.svg)](Cargo.toml)

Zero-copy, content-addressed Rust build cache. No copies, no wasted disk — just hardlinks locally and S3 for sharing.

A drop-in `RUSTC_WRAPPER` that caches Rust compilation artifacts. Cache keys are blake3 hashes of normalized rustc invocations; cache hits restore via hardlinks, and identical blobs are stored once and shared. Optional S3 sync (AWS, Ceph, MinIO, R2) shares the cache across machines.

Local caching and direct S3 sync are stable today.

**SOON:** a remote planner that prefetches from workspace manifests, dependency history, and build intent — warming the right artifacts before rustc asks for them.

![kache: cold build populates the store, then `cargo clean && cargo build` restores every artifact via hardlinks](assets/demo.gif)

> Cold compile populates kache's store, `cargo clean` wipes `target/`, and the second build pulls every artifact back via hardlinks. The recording is reproducible — see [`assets/demo/`](assets/demo/) for the Dockerfile and tape script.

## Why local kache is fast

kache is useful even before remote cache is configured:

- Local hits are restored with hardlinks into `target/`, so artifact bytes are not copied.
- The store is content-addressed by blake3 hash, so identical artifact blobs are stored once and linked many times.
- Misses compile normally, then kache records the outputs for future builds.
- The daemon is optional for local caching. If it is not running, local hits and misses still work; remote checks, uploads, and prefetching degrade gracefully.
- Incremental compilation is disabled while kache wraps rustc, because artifact caching replaces that path and avoids APFS-related corruption on macOS.

## Screenshots

`kache monitor` — live cache dashboard (Build / Projects / Store / Transfer / Passthrough tabs):

![kache monitor TUI cycling through tabs against a populated cache](assets/monitor.gif)

`kache clean` — find target/ dirs and see what's already in the kache store:

![kache clean TUI listing target/ dirs with cached percentages](assets/clean.gif)

## Install

```sh
# mise (recommended)
mise use -g github:kunobi-ninja/kache@latest

# cargo (build from source)
cargo install --git https://github.com/kunobi-ninja/kache kache

# cargo-binstall (downloads pre-built binary, requires cargo-binstall)
cargo binstall --git https://github.com/kunobi-ninja/kache kache
```

## Quick start

```sh
# Interactive setup: configures ~/.cargo/config.toml, installs the
# background daemon as a login service, and starts it.
kache init

# Or accept all defaults non-interactively:
kache init -y

# Verify with:
kache doctor
```

`kache init` is idempotent — re-run it any time to repair configuration. If you prefer to configure things by hand, just export `RUSTC_WRAPPER=kache` or add it to `~/.cargo/config.toml` under `[build]`.

## Use in CI

[`kache-action`](https://github.com/kunobi-ninja/kache-action) installs kache, wires it as `RUSTC_WRAPPER`, and persists the cache between runs. Drop one line into your workflow:

```yaml
- uses: kunobi-ninja/kache-action@v1
```

That uses GitHub Actions cache by default. For S3-backed caching shared across repos or runners, pass `s3-bucket` plus credentials — see the action's README for the full input list.

## Development

```sh
mise install
just
just check
just ci
```

The repo uses `just` as its single task runner. `mise.toml` pins the local Rust baseline and the `just` binary, while the `Justfile` keeps `RUSTC_WRAPPER` empty so kache never tries to build itself through kache.

## Commands

| Command | Description |
|---|---|
| `kache` | Print help (bare invocation) |
| `kache init [-y] [--no-service] [--check]` | Interactive setup: cargo wrapper + service install + daemon start |
| `kache doctor [--fix [--purge-sccache]] [--verify]` | Diagnose setup; `--fix` migrates from sccache, `--verify` checks cache integrity |
| `kache monitor [--since <dur>]` | Live TUI dashboard showing build events, cache stats, and project breakdown |
| `kache stats [--since <dur>]` | Non-interactive cache stats summary |
| `kache list [<crate>] [--sort name\|size\|hits\|age]` | List cached entries, or show details for a specific crate |
| `kache why-miss <crate>` | Explain why a specific crate missed the cache |
| `kache report [--format text\|json\|markdown\|github] [--since <dur>] [--output <path>]` | Generate a detailed build report |
| `kache sync [--pull] [--push] [--all] [--dry-run]` | Synchronize local cache with S3 remote (pull + push) |
| `kache save-manifest [--namespace <ns>]` | Save a build manifest for future prefetch warming |
| `kache gc [--max-age <dur>]` | Garbage collect — LRU eviction or age-based cleanup |
| `kache purge [--crate-name <name>]` | Wipe entire cache or entries for a specific crate |
| `kache clean [--dry-run]` | Find and delete `target/` directories with cache breakdown |
| `kache config` | Open the TUI configuration editor |
| `kache daemon` | Show daemon and service status |
| `kache daemon run` | Start the persistent background daemon (foreground) |
| `kache daemon start` | Start daemon in background (returns immediately) |
| `kache daemon stop` | Stop a running daemon |
| `kache daemon restart` | Restart daemon (via launchd/systemd if installed, else manual) |
| `kache daemon install` | Install daemon as a system service (launchd/systemd) |
| `kache daemon uninstall` | Remove the daemon service |
| `kache daemon log` | Stream daemon logs |

Durations use human-friendly format: `7d`, `24h`, `30m`.

## Remote cache and configuration

`kache sync` can pull from and push to S3-compatible storage directly, without the daemon. Pulls are filtered by the current workspace's `Cargo.lock` by default. See [Sync](docs/remote-cache/sync.mdx) for the full command behavior and S3 layout.

Configuration is available through `kache config`, environment variables, or config files. Environment variables win over config files, and project-local `.kache.toml` files are supported. See [Configuration](docs/getting-started/configuration.mdx) for the full reference.

## Architecture

- **Wrapper**: `RUSTC_WRAPPER` intercepts rustc calls, computes blake3 cache keys, restores hits via hardlinks
- **Daemon**: Background process handles async S3 uploads, remote checks, and prefetch. Auto-restarts when binary is updated
- **Store**: SQLite index + content-addressed blobs under `{cache_dir}/store/`; cache hits hardlink those blobs into `target/`
- **Cache keys**: Deterministic blake3 hash of rustc version, crate name, source, dependencies, and normalized flags — portable across machines

## Remote service

**SOON:** server-side kache is the next milestone. The deployment model, auth integration, and HA behavior are still hardening — treat the planner service and chart as a preview today.

An optional remote planner service lives in [`crates/kache-service`](crates/kache-service). It persists planner state in an embedded SurrealDB database, serves planner endpoints over HTTP, and safely returns `use_fallback` when the database has no matching candidates.

Useful commands:

```sh
just build-service
just image-service
just image-service-release
cargo run -p kache-service
helm upgrade --install kache-service ./charts/kache-service
```

The chart in [`charts/kache-service`](charts/kache-service) is intentionally small: one `Deployment`, one `Service`, optional `PersistentVolumeClaim`, security defaults, health probes, optional `kunobi-auth` bearer-token wiring through an existing `Secret`, and optional `kunobi-ha` Lease-based leader election. It does not bundle ingress or cluster-level policy.

Bearer-token auth is enabled by pointing the chart at an existing secret. Clients must send the same token through `KACHE_PLANNER_TOKEN`.

```yaml
auth:
  existingSecret: kache-planner-token
  existingSecretKey: token
```

The service stores its embedded planner database at `/var/lib/kache/planner.db` by default. The chart supports either ephemeral storage for preview/dev environments or a PVC for persisted state:

```yaml
planner:
  dbPath: /var/lib/kache/planner.db
  persistence:
    enabled: true
    type: pvc
    mountPath: /var/lib/kache
    size: 10Gi
```

For bootstrap/migration only, the service can still import a legacy JSON planner snapshot on startup via `KACHE_PLANNER_SEED_STATE_FILE`.

For highly available deployments, enable leader election and raise the replica count. Followers stay healthy but not ready until they acquire the Kubernetes Lease:

```yaml
replicaCount: 2
ha:
  enabled: true
  leaseName: kache-service
```

When combining HA with PVC-backed planner state, use storage that can be mounted by all scheduled replicas, or keep `replicaCount: 1`.

## Contributing

Bug reports, feature ideas, and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for the dev setup, coding conventions, and PR process. To report a security vulnerability privately, follow [SECURITY.md](SECURITY.md).
