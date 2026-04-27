# kache

[![CI](https://github.com/kunobi-ninja/kache/actions/workflows/ci.yml/badge.svg)](https://github.com/kunobi-ninja/kache/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Rust](https://img.shields.io/badge/rust-1.95%2B-orange.svg)](https://www.rust-lang.org)

Zero-copy, content-addressed Rust build cache. No copies, no wasted disk — just hardlinks locally and S3 for sharing.

A drop-in `RUSTC_WRAPPER` that caches compilation artifacts using blake3 hashing, shares them via hardlinks to save disk space, and optionally syncs to S3-compatible storage (AWS, Ceph, MinIO, R2) for distributed caching across machines.

## Install

```sh
# mise (recommended)
mise use -g github:kunobi-ninja/kache@latest

# cargo-binstall (downloads pre-built binary)
cargo binstall kache

# cargo (build from source)
cargo install --git https://github.com/kunobi-ninja/kache
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

## Why local kache is fast

kache is useful even before remote cache is configured:

- Local hits are restored with hardlinks into `target/`, so artifact bytes are not copied.
- The store is content-addressed by blake3 hash, so identical artifact blobs are stored once and linked many times.
- Misses compile normally, then kache records the outputs for future builds.
- The daemon is optional for local caching. If it is not running, local hits and misses still work; remote checks, uploads, and prefetching degrade gracefully.
- Incremental compilation is disabled while kache wraps rustc, because artifact caching replaces that path and avoids APFS-related corruption on macOS.

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

## Screenshots

`kache monitor`:

```text
 [1] Build   [2] Projects  [3] Store   [4] Transfer
┌ kache monitor ────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  Store: 45.0 GiB / 50.0 GiB [ 90.1%]    11004 entries                                                             │
│  Hit rate: 61% count | 42% weighted | 79% miss-time    Remote: not configured                                     │
│  Dedup: 8.2 GiB saved (18.1%)    Blobs: 36.9 GiB physical    Hardlinks: 4.9 GiB via 7023 hardlinks    Scan: idle  │
│  Transfer: ↑ 0 uploading  ↓ 0 downloading                                                                         │
│  rustc-wrapper=kache via ~/.cargo/config.toml ✓    unknown                                                        │
│  kache v0.1.0 (epoch 1777305862)    daemon: v0.1.0 (epoch 1777305862)    Cache: ~/Library/Caches/kache            │
│                                                                                                                   │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
┌ Live Build ───────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                                                   │
│                                                                                                                   │
│                                                                                                                   │
│                                                                                                                   │
│                                                                                                                   │
│                                                                                                                   │
│                                                                                                                   │
│                                                                                                                   │
│                                                                                                                   │
│                                                                                                                   │
│                                                                                                                   │
│                                                                                                                   │
│                                                                                                                   │
│                                                                                                                   │
│                                                                                                                   │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
┌ Hit Rate (recent) ────────────────────────────────────────────────────────────────────────────────────────────────┐
│  No data yet                                                                                                      │
│                                                                                                                   │
│                                                                                                                   │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
  q: quit  f: filter  ↑↓: scroll  Tab: next  c: clear  1/2/3/4: tabs
```

`kache clean`:

```text
┌ kache clean ──────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ 8 dirs (73.2 GiB total, 7.4 GiB cached)    Selected: 0 (0 B)                                                      │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
┌ Select directories to remove ─────────────────────────────────────────────────────────────────────────────────────┐
│ [ ]  workspace/compiler-core/target                                             34.7 GiB    5.7 GiB [debug, release]│
│ [ ]  workspace/build-cache/target                                               15.4 GiB   49.1 MiB [debug, release]│
│ [ ]  workspace/desktop-app/crates/app/target                                    13.8 GiB    9.4 MiB [debug]         │
│ [ ]  workspace/service-api/target                                                3.0 GiB  239.2 MiB [debug]         │
│ [ ]  workspace/frontend/packages/graph-core/target                               2.2 GiB  635.6 MiB [debug]         │
│ [ ]  workspace/auth-service/target                                               1.5 GiB  727.4 MiB [debug]         │
│ [ ]  workspace/metrics/target                                                    1.5 GiB        0 B [debug]         │
│ [ ]  workspace/frontend/packages/ipc-plugin/target                               1.1 GiB  152.8 MiB [debug]         │
│                                                                                                                   │
│                                                                                                                   │
│                                                                                                                   │
│                                                                                                                   │
│                                                                                                                   │
│                                                                                                                   │
│                                                                                                                   │
│                                                                                                                   │
│                                                                                                                   │
│                                                                                                                   │
│                                                                                                                   │
│                                                                                                                   │
│                                                                                                                   │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
┌ workspace/compiler-core/target — 34.7 GiB total, 5.7 GiB cached (16%) ────────────────────────────────────────────┐
│  incremental:        0 B   build:  433.3 MiB   deps (local):   28.4 GiB                                           │
│  fingerprint:    5.9 MiB   binaries: 239.5 MiB   other:           6.9 KiB                                         │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ space: toggle  a: select all  n: select none  enter: delete selected  q: cancel                                   │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Remote cache and configuration

`kache sync` can pull from and push to S3-compatible storage directly, without the daemon. Pulls are filtered by the current workspace's `Cargo.lock` by default. See [Sync](docs/kache-docs/remote-cache/sync.mdx) for the full command behavior and S3 layout.

Configuration is available through `kache config`, environment variables, or config files. Environment variables win over config files, and project-local `.kache.toml` files are supported. See [Configuration](docs/kache-docs/getting-started/configuration.mdx) for the full reference.

## Architecture

- **Wrapper**: `RUSTC_WRAPPER` intercepts rustc calls, computes blake3 cache keys, restores hits via hardlinks
- **Daemon**: Background process handles async S3 uploads, remote checks, and prefetch. Auto-restarts when binary is updated
- **Store**: SQLite index + content-addressed blobs under `{cache_dir}/store/`; cache hits hardlink those blobs into `target/`
- **Cache keys**: Deterministic blake3 hash of rustc version, crate name, source, dependencies, and normalized flags — portable across machines

## Remote service

Warning: server-side kache is still work in progress. Treat the planner service and chart as experimental until the deployment model, auth integration, and HA behavior are hardened.

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
