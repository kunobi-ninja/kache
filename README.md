# kache

[![CI](https://github.com/kunobi-ninja/kache/actions/workflows/ci.yml/badge.svg)](https://github.com/kunobi-ninja/kache/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Rust](https://img.shields.io/badge/rust-1.93%2B-orange.svg)](https://www.rust-lang.org)

Zero-copy, content-addressed Rust build cache. No copies, no wasted disk — just hardlinks locally and S3 for sharing.

A drop-in `RUSTC_WRAPPER` that caches compilation artifacts using blake3 hashing, shares them via hardlinks to save disk space, and optionally syncs to S3-compatible storage (AWS, Ceph, MinIO, R2) for distributed caching across machines.

## Quick start

```sh
cargo install --git https://github.com/kunobi-ninja/kache

# Set as your rustc wrapper
export RUSTC_WRAPPER=kache

# Or persist it in ~/.cargo/config.toml:
# [build]
# rustc-wrapper = "kache"
```

## Commands

| Command | Description |
|---|---|
| `kache` | Open the live TUI monitor (default when no subcommand) |
| `kache monitor [--since <dur>]` | Live TUI dashboard showing build events, cache stats, and project breakdown |
| `kache list [<crate>] [--sort name\|size\|hits\|age]` | List cached entries, or show details for a specific crate |
| `kache sync [--pull] [--push] [--all] [--dry-run]` | Synchronize local cache with S3 remote (pull + push) |
| `kache gc [--max-age <dur>]` | Garbage collect — LRU eviction or age-based cleanup |
| `kache purge [--crate-name <name>]` | Wipe entire cache or entries for a specific crate |
| `kache clean [--dry-run]` | Find and delete `target/` directories with cache breakdown |
| `kache doctor [--fix [--purge-sccache]]` | Diagnose setup; `--fix` migrates from sccache |
| `kache config` | Open the TUI configuration editor |
| `kache daemon` | Show daemon and service status |
| `kache daemon run` | Start the persistent background daemon (foreground) |
| `kache daemon start` | Start daemon in background (returns immediately) |
| `kache daemon stop` | Stop a running daemon |
| `kache daemon install` | Install daemon as a system service (launchd/systemd) |
| `kache daemon uninstall` | Remove the daemon service |
| `kache daemon log` | Stream daemon logs |

Durations use human-friendly format: `7d`, `24h`, `30m`.

### Sync

`kache sync` works directly against S3 (no daemon required). By default, it filters pulls to crates found in the current workspace's `Cargo.lock`:

```sh
kache sync              # pull missing deps + push local artifacts
kache sync --pull       # download only
kache sync --push       # upload only
kache sync --all        # pull all artifacts (ignore Cargo.lock filter)
kache sync --dry-run    # preview what would transfer
```

S3 layout: `{prefix}/{crate_name}/{cache_key}.tar.zst` — organized by crate for efficient filtered listing.

## Configuration

Configure via the TUI editor (`kache config`), environment variables, or the config file directly.

Config file: `~/.config/kache/config.toml` (respects `XDG_CONFIG_HOME`).

Environment variables take highest priority, then config file, then defaults.

| Variable | Config key | Default | Description |
|---|---|---|---|
| `KACHE_CACHE_DIR` | `cache.local_store` | `~/.cache/kache` | Local store directory |
| `KACHE_MAX_SIZE` | `cache.local_max_size` | `50GB` | Max store size |
| `KACHE_S3_BUCKET` | `cache.remote.bucket` | — | S3 bucket for remote cache |
| `KACHE_S3_ENDPOINT` | `cache.remote.endpoint` | — | Custom S3 endpoint (required for Ceph/MinIO/R2) |
| `KACHE_S3_REGION` | `cache.remote.region` | `us-east-1` | AWS region |
| `KACHE_S3_PREFIX` | `cache.remote.prefix` | `artifacts` | S3 key prefix for all artifacts |
| `KACHE_S3_PROFILE` | `cache.remote.profile` | — | AWS profile name for credentials (e.g. `ceph`) |
| `KACHE_S3_ACCESS_KEY` | — | — | Explicit S3 access key (overrides profile) |
| `KACHE_S3_SECRET_KEY` | — | — | Explicit S3 secret key (overrides profile) |
| `KACHE_CACHE_EXECUTABLES` | `cache.cache_executables` | `false` | Cache bin/dylib/cdylib outputs |
| `KACHE_CLEAN_INCREMENTAL` | `cache.clean_incremental` | `true` | Auto-clean incremental dirs during GC |
| `KACHE_COMPRESSION_LEVEL` | `cache.compression_level` | `3` | Zstd compression level (1-22) |
| `KACHE_S3_CONCURRENCY` | `cache.s3_concurrency` | `8` | Max concurrent S3 operations |
| `KACHE_DISABLED` | — | `0` | Disable caching entirely |
| `KACHE_LOG` | — | `kache=warn` | Log level |

### Credential resolution order

1. `KACHE_S3_ACCESS_KEY` + `KACHE_S3_SECRET_KEY` (explicit override)
2. AWS profile from config (`profile` field or `KACHE_S3_PROFILE`)
3. AWS default chain (`AWS_ACCESS_KEY_ID`, `~/.aws/credentials [default]`, IAM roles)

### Example config (Ceph)

```toml
[cache.remote]
type = "s3"
bucket = "build-cache"
endpoint = "https://s3.example.com"
profile = "ceph"
```

## Architecture

- **Wrapper**: `RUSTC_WRAPPER` intercepts rustc calls, computes blake3 cache keys, restores hits via hardlinks
- **Daemon**: Background process handles async S3 uploads, remote checks, and prefetch. Auto-restarts when binary is updated
- **Store**: SQLite index + content-addressed directories under `{cache_dir}/store/`
- **Cache keys**: Deterministic blake3 hash of rustc version, crate name, source, dependencies, and normalized flags — portable across machines

Incremental compilation is automatically disabled when kache wraps rustc (`CARGO_INCREMENTAL=0`), since kache's artifact caching subsumes it and avoids APFS-related corruption on macOS.
