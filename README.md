# kache

[![Crates.io](https://img.shields.io/crates/v/kache.svg)](https://crates.io/crates/kache)
[![CI](https://github.com/kunobi-ninja/kache/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/kunobi-ninja/kache/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.95-blue.svg)](Cargo.toml)

Zero-copy, content-addressed build cache for Rust and C/C++ object compiles. No copies, no wasted disk — reflinks where the filesystem supports them, hardlinks or copies otherwise, plus S3 for Rust artifact sharing.

A drop-in `RUSTC_WRAPPER` for Rust and a `cc` / `c++` compiler wrapper for C/C++ object compiles. Cache keys are blake3 hashes of normalized compiler inputs; cache hits restore zero-copy — a reflink (copy-on-write clone) where the filesystem supports it (APFS, btrfs, XFS-with-reflink), and a hardlink or copy otherwise — and identical blobs are stored once and shared. Optional S3 sync (AWS, Ceph, MinIO, R2) shares Rust artifacts across machines.

Local Rust caching, local C/C++ object caching, and direct S3 sync are working today. C/C++ artifacts are local-only for now; unsupported compiler shapes pass through to the real compiler.

**PREVIEW:** a remote planner that prefetches from workspace manifests, dependency history, and build intent — warming the right artifacts before rustc asks for them. The daemon already calls a planner when `KACHE_PLANNER_ENDPOINT` is set; the hosted service is still preview.

![kache: cold build populates the store, then `cargo clean && cargo build` restores every artifact zero-copy](assets/demo.gif)

> Cold compile populates kache's store, `cargo clean` wipes `target/`, and the second build pulls every artifact back zero-copy. The recording is reproducible — see [`assets/demo/`](assets/demo/) for the Dockerfile and tape script.

## Why local kache is fast

kache is useful even before remote cache is configured:

- Local hits are restored zero-copy into `target/` — a reflink (copy-on-write clone) where the filesystem supports it, a hardlink or copy otherwise — so artifact bytes are not duplicated.
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

### OS package managers

**Homebrew (macOS):**

```sh
# Stable
brew install kunobi-ninja/kunobi/kache

# Pre-release channel (RC/beta)
brew install kunobi-ninja/kunobi/kache-unstable
```

**APT (Debian/Ubuntu):**

```sh
# Add the signing key (KMS-rooted OpenPGP cert), dearmored into a keyring
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://r2.kunobi.com/kache/apt/gpg.key \
  | sudo gpg --dearmor -o /etc/apt/keyrings/kache.gpg

# Add the repository — use `stable` for releases, `unstable` for RC/beta builds
echo "deb [signed-by=/etc/apt/keyrings/kache.gpg] https://r2.kunobi.com/kache/apt stable main" \
  | sudo tee /etc/apt/sources.list.d/kache.list

sudo apt update
sudo apt install kache
```

**winget (Windows):**

```powershell
# Stable
winget install kunobi-ninja.kache

# Pre-release channel (RC/beta)
winget install kunobi-ninja.kache.Unstable
```

## Quick start

```sh
# Interactive setup: configures $CARGO_HOME/config.toml (default ~/.cargo),
# installs the background daemon as a login service, and starts it.
kache init

# Or accept all defaults non-interactively:
kache init -y

# Verify with:
kache doctor
```

`kache init` is idempotent — re-run it any time to repair configuration. Use `kache init --check` to preview the changes without touching any files. If you prefer to configure things by hand, just export `RUSTC_WRAPPER=kache` or add it to `$CARGO_HOME/config.toml` (usually `~/.cargo`) under `[build]`.

## Use in CI

[`kache-action`](https://github.com/kunobi-ninja/kache-action) installs kache, wires it as `RUSTC_WRAPPER`, and persists the cache between runs. Drop one line into your workflow:

```yaml
- uses: kunobi-ninja/kache-action@v1
```

That uses GitHub Actions cache by default. For S3-backed caching shared across repos or runners, pass `s3-bucket` plus credentials — see the action's README for the full input list.

## C/C++ caching

Alongside the rustc wrapper, kache can cache **C/C++ object compiles** as a `cc` / `c++` wrapper. It recognizes `cc`, `c++`, `gcc`, `g++`, `clang`, and `clang++` (plus versioned variants like `gcc-13`), and `clang-cl` or any `--driver-mode=cl` invocation (clang in MSVC driver mode) — on every OS, not just Windows, since recognition keys off the driver mode rather than the host. clang-cl is what mozconfigs and the `cc` crate use on Windows:

```sh
# POSIX (gcc / clang)
export CC="kache cc"
export CXX="kache c++"
```

```bat
:: Windows (clang-cl) — e.g. the CC env var a mozconfig / the cc crate reads
set "CC=kache clang-cl"
```

**What's cached today:** single-source `-c` object compiles (e.g. `cc -c foo.c -o foo.o`, `c++ -c foo.cpp -o foo.o`, or `clang-cl -c foo.c -Fofoo.obj`). The cache key is the preprocessor expansion (`cc -E -P` for gcc/clang, `clang-cl /EP` for clang-cl, with `SOURCE_DATE_EPOCH` forced to `0` so `__DATE__`/`__TIME__` expand deterministically) plus compiler identity, target arch, and codegen flags — so any header change invalidates it. On a hit the object (`.o`, or `.obj` for clang-cl) and its `.d` dep-info restore without re-running the compiler. Any flag kache hasn't classified is **refused** — the invocation passes through to the real compiler rather than risk a silently-wrong object.

For **gcc/clang** the key is portable across machines and worktrees (paths are normalized via `-ffile-prefix-map`). Set `KACHE_BASE_DIR` to a user-declared root that gets stripped from the key, covering paths the derived source/build roots miss — e.g. objdir-built units whose `__FILE__` points above the derived root — for cross-checkout hits the automatic roots can't reach. If path normalization ever miscaches, `KACHE_CC_PATH_NORMALIZE=0` is the escape hatch: keys become path-literal (no cross-machine sharing, zero normalization risk). For **clang-cl** the key is already **machine-local**: clang-cl ignores `-ffile-prefix-map`, so kache keeps literal paths in the key — correct local hits, but no cross-machine sharing yet. clang-cl **debug** compiles (`/Z7`, `/Zi`, `-Z7`, or a `-g` form) are cached too: clang-cl embeds debug info straight into the `.obj` (there's no separate compile-time PDB), so kache folds the CodeView path inputs that object embeds — source path, output name, and compilation dir — into the same machine-local key.

**Not cached yet** (these pass through): link / whole-program steps, multi-source and multi-arch invocations, response-file (`@file`) invocations, precompiled headers, modules, coverage, and split-DWARF; for clang-cl also `-bigobj` and `-showIncludes`. C/C++ caching is also **local-only** for now — the Rust path's S3 sharing doesn't extend to `cc` artifacts yet.

C/C++ caching is live but still conservative by design: when in doubt, kache misses rather than serve a wrong artifact. See [C/C++ caching](docs/getting-started/c-cpp.mdx) for setup, status, and limitations. Scope and remaining work are tracked in [#49](https://github.com/kunobi-ninja/kache/issues/49).

## Development

```sh
mise install
just
just check
just ci
```

The repo uses `just` as its single task runner. `mise.toml` pins the local Rust baseline and the `just` binary, while the `Justfile` keeps `RUSTC_WRAPPER` empty so kache never tries to build itself through kache.

## Benchmarks

`kache-scenario` is the native benchmark and reporting harness. It builds real
projects twice against one shared compiler cache: a cold build in one checkout,
then a warm build in a second checkout at a different absolute path. That shape
exercises the work kache is designed for: fresh CI workspaces, branch switches,
worktrees, and agent-created clones that should reuse the same compiled bytes.

The benchmark is self-diagnosing. For kache runs it captures cache reports,
wrapper logs, build logs, Perfetto/Chrome trace JSON, cross-clone key-stability,
path-leak samples, passthrough reasons, storage accounting, and an explicit
verdict. For sccache comparison runs it isolates the sccache daemon/cache,
validates the cache location/base-dir wiring, and stores sccache stats beside
the same cold/warm build logs.

Firefox is the headline stress case: a huge mixed Rust + C/C++ build driven by
mozbuild. kache intentionally still passes through unsupported shapes such as
link/whole-program steps, multi-source units, preprocessor-only probes, and any
still-unmodeled flags; those passthroughs are reported so the benchmark shows
both today's wins and the next optimization targets. Adding another workload is a
scenario-file addition, not a runner rewrite — the LLVM scenario is one such
almost-pure-C/C++ counterpart.

```sh
just bench                 # list kache-backed benchmark profiles
just bench firefox         # full cold + warm Firefox benchmark (tens of min to hours, ~50 GB)
just bench-retry firefox   # restore the cold snapshot, re-measure warm only (~25 min)
just bench-trace firefox   # also emit key-diff.{json,md}
just bench-sccache         # list sccache-backed benchmark profiles
just bench-sccache firefox # same Firefox shape, with sccache
just bench substrate       # Rust-heavy polkadot-sdk benchmark (tens of min to ~1.5h, ~20-40 GB)
just bench llvm            # almost-pure C/C++ CMake benchmark (tens of min, large scratch)
```

Each run writes root-level "latest run" artifacts under
`tmp/bench/<scenario>/`:

- `report-{cold,warm}.json` and `.md` from `kache report`
- `trace-{cold,warm}.json`, a native Perfetto/Chrome trace (`traceEvents`)
- `build-{cold,warm}.log` and `wrapper-{cold,warm}.log`
- `<scenario>.json`, the benchmark summary including tool versions
- `key-diff.{json,md}` when `just bench-trace` is used
- `report-*.sccache.json` and `report-*.sccache-adv.txt` for sccache runs

Those root files are overwritten by the next run so `--retry` has a stable
latest snapshot. Every completed run is also archived to
`tmp/bench/<scenario>/runs/<YYYYMMDDTHHMMSSZ>-<backend>-<pid>/`, so repeated
kache and sccache measurements accumulate without clobbering each other.

The trace files can be opened directly in Perfetto or Chrome's trace viewer.
They already contain kache-native timings, outcomes, routes, reasons, cache
keys, byte counts, and compiler/preprocessor/probe counts. External traces from
the build system can be merged into the same Perfetto view when they share, or
are normalized to, the same timebase; they are useful for target-level context
but not required to understand kache hit/miss behavior.

Each benchmark scenario uses a **per-scenario scratch dir** — `./tmp/bench/<scenario>` by default (override with `--work-dir`) — so firefox, firefox-sccache, substrate, and any other scenario **coexist** without clobbering each other's clones, cache, or logs; `rm -rf tmp/bench` cleans them all. A `work_dir` lock refuses a second run pointed at the same scratch dir. Concurrent runs on one host invalidate the wall-clock numbers (CPU/IO/RAM contention), so run benchmarks sequentially or on separate hosts.

Each project is described by a [scenario](scenarios/) (`scenarios/bench-*/scenario.toml`) — repo/ref, how to wire kache or sccache in, and how to build. The Substrate probe is `RUSTC_WRAPPER`-only: it caches the Rust compile surface — the polkadot node's dependency tree plus the nested wasm-runtime compiles — while native C deps (rocksdb, secp256k1) compile outside kache's view by design. Needs `protoc`, `clang`, `cmake`, `pkg-config` installed.

See [`scenarios/README.md`](scenarios/README.md) for the scenario format.

## Commands

| Command | Description |
|---|---|
| `kache` | Print help (bare invocation) |
| `kache init [-y] [--no-service] [--check]` | Interactive setup: cargo wrapper + service install + daemon start |
| `kache doctor [--fix [--purge-sccache]] [--verify] [--checksums] [--repair]` | Diagnose setup; `--fix` migrates from sccache, `--verify` checks cache integrity, `--checksums` also hashes blobs, `--repair` deletes corrupted entries |
| `kache monitor [--since <dur>]` | Live TUI dashboard showing build events, cache stats, and project breakdown |
| `kache stats [--since <dur>]` | Non-interactive cache stats summary |
| `kache list [<crate>] [--sort name\|size\|hits\|age]` | List cached entries, or show details for a specific crate |
| `kache why-miss <crate>` | Explain why a specific crate missed the cache |
| `kache report [--format text\|json\|markdown\|github\|perfetto\|chrome-trace] [--since <dur>] [--top <n>] [--output <path>]` | Generate a detailed hit/dup/miss build report or Perfetto/Chrome trace (`--top` defaults to 10) |
| `kache sync [--manifest-path <path>] [--pull] [--push] [--all] [--workspace] [--dry-run]` | Synchronize local cache with S3 remote (pull + push); `--manifest-path` points the Cargo.lock pull filter at a non-cwd manifest; `--workspace` scopes the pull to workspace members only (one LIST per member) |
| `kache save-manifest [--manifest-key <key>] [--namespace <ns>]` | Save a build manifest for future prefetch warming; `--manifest-key` overrides the default host-target-triple key |
| `kache gc [--max-age <dur>]` | Garbage collect — LRU eviction or age-based cleanup |
| `kache purge [--crate-name <name>]` | Wipe entire cache or entries for a specific crate |
| `kache clean [-n \| --dry-run] [-y \| --yes]` | Find and delete `target/` directories with cache breakdown (interactive; `-n` previews, `-y` removes all non-interactively for scripts/cron) |
| `kache config` | Open the TUI configuration editor |
| `kache completions <shell>` | Print shell completion script (bash, zsh, fish, elvish, powershell) |
| `kache daemon` | Show daemon and service status |
| `kache daemon run` | Start the persistent background daemon (foreground) |
| `kache daemon start` | Start daemon in background (returns immediately) |
| `kache daemon stop` | Stop a running daemon |
| `kache daemon restart` | Restart daemon (via launchd/systemd if installed, else manual) |
| `kache daemon install` | Install daemon as a system service (launchd/systemd) |
| `kache daemon uninstall` | Remove the daemon service |
| `kache daemon log` | Stream daemon logs |

Durations use days, hours, or bare hours: `7d`, `24h`, `1h`, `48`.

## Remote cache and configuration

`kache sync` can pull from and push to S3-compatible storage directly, without the daemon. Pulls are filtered by the current workspace's `Cargo.lock` by default. See [Sync](docs/remote-cache/sync.mdx) for the full command behavior and S3 layout.

Configuration is available through `kache config`, environment variables, or config files. Environment variables win over config files, and project-local `.kache.toml` files are supported. See [Configuration](docs/getting-started/configuration.mdx) for the full reference.

## Architecture

- **Wrapper**: `RUSTC_WRAPPER` intercepts rustc calls, computes blake3 cache keys, restores hits zero-copy (reflink where supported, else hardlink or copy)
- **Daemon**: Background process handles async S3 uploads, remote checks, and prefetch. Auto-restarts when binary is updated
- **Store**: content-addressed blobs under `{cache_dir}/store/blobs/<prefix>/<hash>`, indexed by a SQLite DB; cache hits reflink or hardlink those blobs into `target/`
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
