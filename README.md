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
cargo install kache

# cargo-binstall (downloads pre-built binary, requires cargo-binstall)
cargo binstall kache
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

## C/C++ caching (experimental)

Alongside the rustc wrapper, kache can cache **C/C++ object compiles** as a `cc` / `c++` wrapper. It recognizes `cc`, `c++`, `gcc`, `g++`, `clang`, and `clang++` on POSIX, and `clang-cl` (clang in MSVC driver mode) on Windows:

```sh
# POSIX (gcc / clang)
export CC="kache cc"
export CXX="kache c++"
```

```bat
:: Windows (clang-cl) — e.g. the CC env var a mozconfig / the cc crate reads
set "CC=kache clang-cl"
```

**What's cached today:** single-source `-c` object compiles (e.g. `cc -c foo.c -o foo.o`, or `clang-cl -c foo.c -Fofoo.obj`). The cache key is the preprocessor expansion (`cc -E -P` for gcc/clang, `clang-cl /EP` for clang-cl, with `SOURCE_DATE_EPOCH` pinned) plus compiler identity, target arch, and codegen flags — so any header change invalidates it. On a hit the object (`.o`, or `.obj` for clang-cl) and its `.d` dep-info restore without re-running the compiler. Any flag kache hasn't classified is **refused** — the invocation passes through to the real compiler rather than risk a silently-wrong object.

For **gcc/clang** the key is portable across machines and worktrees (paths are normalized via `-ffile-prefix-map`). For **clang-cl** the key is currently **machine-local**: clang-cl ignores `-ffile-prefix-map`, so kache keeps literal paths in the key — correct local hits, but no cross-machine sharing yet.

**Not cached yet** (these pass through): link / whole-program steps, multi-source and multi-arch invocations, precompiled headers, modules, coverage, and split-DWARF; for clang-cl also **debug info** (`-Z7` / `-g`), `-bigobj`, and `-showIncludes`. C/C++ caching is also **local-only** for now — the Rust path's S3 sharing doesn't extend to `cc` artifacts yet.

It's experimental and conservative by design: when in doubt, kache misses rather than serve a wrong artifact. Scope and remaining work are tracked in [#49](https://github.com/kunobi-ninja/kache/issues/49).

## Development

```sh
mise install
just
just check
just ci
```

The repo uses `just` as its single task runner. `mise.toml` pins the local Rust baseline and the `just` binary, while the `Justfile` keeps `RUSTC_WRAPPER` empty so kache never tries to build itself through kache.

## Benchmarks

**Work in progress.** The `kache-scenario` runner drives kache through real benchmark scenarios such as Firefox (cold + warm builds against one shared cache, cross-clone key-stability check, leak detection, honest verdict gate). It's the tool that surfaced the linker-path key leak fixed in v7 of the cache-key version.

It is intentionally a hard scenario. Firefox combines a large Rust workspace with extensive C/C++ via mozbuild's bootstrapped toolchain, LTO, and per-objdir cargo-linker shims — and by compile count the build is dominated by C/C++, not Rust (~85% of the compiles in a Firefox build are C/C++ units). **Caching Firefox end-to-end is therefore a long-term objective**: kache's Rust path is mature, but its C/C++ path (`cc.rs`-driven invocations, clang/gcc arg parsing, embedded-build-system flags) is still maturing. Until that side catches up, Firefox's C/C++ compiles passthrough uncached and bound the achievable warm speedup, regardless of how well the Rust side caches.

So the current bench measures progress against that long-term objective rather than a finished product. The verdict reliably reports `ok` for cache-key portability on the Rust side after v7 (88% cross-clone stability), but two known limitations keep the weighted speedup modest:

1. **C/C++ caching maturity** — C/C++ dominates the Firefox build (~85% of compiles), and so far only single-source `-c` object compiles are cached (see [C/C++ caching](#cc-caching-experimental)). Link/whole-program steps, multi-source units, and any still-unmodeled flag pass through, and `cc` caching is local-only — so most of Firefox's C/C++ work isn't warm-cached yet. (0.4.0 broadened the `cc` flag coverage considerably — Gecko/Darwin/clang and C++ ABI flags — but the bottleneck is now the broader scope above, not a single rejected flag.) This is the bottleneck; closing it is the multi-step C/C++ maturity work.
2. **Workspace-local Rust crate instability** — a handful of Mozilla-local crates (`gkrust_shared`, `style`, `webrender`, …) still miss across clones for a non-path-leak reason, separate from the v7 linker fix.

Treat the benchmark as a diagnostic platform first and a headline-number tool second — the structural improvements come out of running it, not today's speedup figure.

```sh
just bench firefox             # full cold + warm (tens of minutes to hours, ~50 GB)
just bench-retry firefox       # restore cold-state snapshot, re-measure warm only (~25 min)
just bench substrate           # polkadot node cold + warm (tens of min to ~1.5h, ~20-40 GB)
just bench-retry substrate     # restore cold-state snapshot, re-measure warm only
```

Each benchmark scenario uses a **per-scenario scratch dir** — `./tmp/bench/<scenario>` by default (override with `--work-dir`) — so firefox, substrate, and any other scenario **coexist** without clobbering each other's clones, cache, or logs; `rm -rf tmp/bench` cleans them all. A `work_dir` lock refuses a second run pointed at the same scratch dir. Two caveats: (1) **concurrent runs on one host invalidate the wall-clock numbers** (CPU/IO/RAM contention) — for valid timing run sequentially or on separate hosts; (2) this per-scenario path is a change from the old shared `./tmp/bench`, so a pre-existing `--retry`/`--skip-clone` snapshot won't be found under the new path — your **first run after upgrading must be a full run** (delete the old `tmp/bench` and `tmp/bench-clone-ref` first).

Each project is described by a [scenario](scenarios/) (`scenarios/bench-*/scenario.toml`) — repo/ref, how to wire kache in, how to build — so adding a workload is dropping a scenario directory, not editing Rust. Both run the same cold→warm, two-clones-at-different-paths benchmark. The Substrate probe is `RUSTC_WRAPPER`-only: it caches the Rust compile surface — the polkadot node's dependency tree plus the nested wasm-runtime compiles — which is the workload where sccache/cachepot never solved cross-clone path-independence. Native C deps (rocksdb, secp256k1) compile outside kache's view by design. Needs `protoc`, `clang`, `cmake`, `pkg-config` installed.

See [`scenarios/README.md`](scenarios/README.md) for the scenario format.

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
| `kache report [--format text\|json\|markdown\|github] [--since <dur>] [--output <path>]` | Generate a detailed hit/dup/miss build report |
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
