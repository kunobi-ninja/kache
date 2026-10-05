[![CI](https://github.com/kunobi-ninja/kache/actions/workflows/ci.yml/badge.svg)](https://github.com/kunobi-ninja/kache/actions/workflows/ci.yml)
[![Bench](https://github.com/kunobi-ninja/kache/actions/workflows/bench.yml/badge.svg)](https://github.com/kunobi-ninja/kache/actions/workflows/bench.yml)
[![Crates.io](https://img.shields.io/crates/v/kache.svg)](https://crates.io/crates/kache)
[![Documentation](https://img.shields.io/badge/docs-kunobi.ninja-blue)][docs-badge]

# Kache

**Build once. Reuse the work.**

Kache is a compiler cache for Rust, C/C++, and CUDA on Linux, macOS, and Windows.
It restores matching compiler outputs across worktrees and CI runs, so you spend
less time rebuilding and less disk space storing the same bytes.

Built by [Kunobi][kunobi-brand].

[Get started][first-reuse] · [Benchmarks][nav-benchmarks] · [Kache vs sccache][nav-comparison] · [CI setup][nav-ci]

## Install

```bash
cargo install kache --locked
kache init
```

`kache init` shows the changes before applying them. It configures Cargo's compiler
wrapper and offers a background service. On Unix it also sets up C/C++ compiler
shims. Run `kache init --check` for a preview.

Then use Cargo as usual in your project. Kache caches eligible compiler work
automatically.

Cargo installation needs Rust 1.95 or newer. Prefer a prebuilt package?
[Homebrew, APT, Windows packages, mise, and Nix →](https://kunobi.ninja/docs/kache/getting-started/installation)

## See it reuse a build

Build the same revision in two worktrees with separate target directories.
The second can restore compatible outputs from the first.
[Try the walkthrough][first-reuse].

<picture>
  <source media="(prefers-reduced-motion: reduce)" srcset="assets/demo-36381adc.png">
  <img src="assets/demo-36381adc.gif" alt="A recorded demo of a second worktree restoring cached crates, followed by its build report.">
</picture>

[View the still image](assets/demo-36381adc.png).

A new cache needs a build to fill it. Reuse requires matching inputs, toolchain,
and build settings. Unsupported compiler invocations run normally.
[What can be cached →](https://kunobi.ninja/docs/kache#current-support)

## Why use Kache?

- Reuse eligible Rust libraries, executables, and build scripts across worktrees.
  It also caches GCC, Clang, clang-cl, and single-source CUDA object compilations.
- Keep identical output bytes once in a cache with a disk budget. On filesystems
  with copy-on-write cloning, restored outputs share disk blocks with the cache.
- Run builds together. Compiles with the same cache key share one in-flight
  build, while the scheduler paces compiler processes.
- Share work through S3-compatible storage, Google Cloud Storage, a shared
  filesystem, or an OCI registry. The local cache works on its own.
- See what happened. Inspect hits and misses, then ask `kache explain` what changed.

[![Four Firefox worktrees sharing cached outputs through copy-on-write clones.](https://raw.githubusercontent.com/kunobi-ninja/kache/main/assets/store-worktrees.svg)][hero-image]

Rust executables and build-script runs are cached by default on Linux and macOS.
See [platform support and caching limits](https://kunobi.ninja/docs/kache#current-support)
for Windows and other workload details.

## Less disk per worktree

[![A measured second Firefox worktree adds about 3 GB with Kache on APFS, compared with 16.7 GB with sccache.](https://raw.githubusercontent.com/kunobi-ninja/kache/main/assets/worktree-cost.svg)][storage-chart]

In a Firefox 151 benchmark with Kache 0.7.0 on macOS/APFS, the second worktree
added about 3 GB of new data. [Read the measurements and methodology →][storage-report]

Nightly workflows measure cold and warm builds of real projects, including
Firefox, LLVM, and SurrealDB. [Browse the benchmark reports][benchmark-guide]
to see the workload, platform, and validation behind each result.

## Watch a build and explain a miss

```bash
kache monitor             # live build and cache activity
kache stats --last-build  # latest recorded build session
kache explain             # why the latest build missed
kache doctor              # check your setup
```

<picture>
  <source media="(prefers-reduced-motion: reduce)" srcset="assets/monitor-9dd7cee8.png">
  <img src="assets/monitor-9dd7cee8.gif" alt="Kache's monitor showing builds, miss explanations, projects, and stored outputs.">
</picture>

[View the still image](assets/monitor-9dd7cee8.png).

[See the current dashboard and controls →](https://kunobi.ninja/docs/kache/monitor)

## Add it to CI

Install your Rust toolchain, then add the official action before your build:

```yaml
- uses: kunobi-ninja/kache-action@v1
- run: cargo build --locked
```

The action can persist the local store through GitHub's cache service.
For remote storage, credentials, and pull-request policy, follow the [CI guide][nav-ci].

## Star History

<a href="https://www.star-history.com/?repos=kunobi-ninja%2Fkache&type=date&legend=top-left">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/chart?repos=kunobi-ninja/kache&type=date&theme=dark&legend=top-left" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/chart?repos=kunobi-ninja/kache&type=date&legend=top-left" />
   <img alt="Star History Chart" src="https://api.star-history.com/chart?repos=kunobi-ninja/kache&type=date&legend=top-left" />
 </picture>
</a>


## Find your next step

| I want to… | Guide |
| --- | --- |
| Confirm my first cache hits | [Quick start][first-reuse] |
| Cache Make, CMake, or CUDA builds | [C/C++](https://kunobi.ninja/docs/kache/getting-started/c-cpp) · [CUDA](https://kunobi.ninja/docs/kache/getting-started/cuda) |
| Share a cache between machines | [Remote cache](https://kunobi.ninja/docs/kache/remote-cache/overview) |
| Change storage limits or build policy | [Configuration](https://kunobi.ninja/docs/kache/getting-started/configuration) |
| Investigate unexpected misses | [Troubleshooting](https://kunobi.ninja/docs/kache/getting-started/troubleshooting) |
| Understand cache keys and correctness | [How it works](https://kunobi.ninja/docs/kache/how-it-works/architecture) |
| Look up a command or flag | [Command reference](https://kunobi.ninja/docs/kache/commands/reference) |

## Help improve Kache

[Report a bug](https://github.com/kunobi-ninja/kache/issues/new?template=bug_report.md),
[request a feature](https://github.com/kunobi-ninja/kache/issues/new?template=feature_request.md),
or read [CONTRIBUTING.md](.github/CONTRIBUTING.md) to contribute.

Licensed under [Apache 2.0](LICENSE).

[docs-badge]: https://kunobi.ninja/docs/kache?utm_source=github&utm_medium=readme&utm_campaign=kache&utm_content=docs_badge
[kunobi-brand]: https://kunobi.ninja/?utm_source=github&utm_medium=readme&utm_campaign=kache&utm_content=brand
[nav-benchmarks]: https://kunobi.ninja/docs/kache/benchmarks?utm_source=github&utm_medium=readme&utm_campaign=kache&utm_content=nav_benchmarks
[nav-comparison]: https://kunobi.ninja/docs/kache/getting-started/comparison?utm_source=github&utm_medium=readme&utm_campaign=kache&utm_content=nav_comparison
[nav-ci]: https://kunobi.ninja/docs/kache/remote-cache/ci?utm_source=github&utm_medium=readme&utm_campaign=kache&utm_content=nav_ci
[hero-image]: https://kunobi.ninja/product/kache?utm_source=github&utm_medium=readme&utm_campaign=kache&utm_content=hero_image
[storage-chart]: https://kunobi.ninja/blog/kache-storage-worktrees?utm_source=github&utm_medium=readme&utm_campaign=kache&utm_content=storage_chart
[storage-report]: https://kunobi.ninja/blog/kache-storage-worktrees?utm_source=github&utm_medium=readme&utm_campaign=kache&utm_content=storage_report
[benchmark-guide]: https://kunobi.ninja/docs/kache/benchmarks?utm_source=github&utm_medium=readme&utm_campaign=kache&utm_content=benchmark_guide
[first-reuse]: https://kunobi.ninja/docs/kache/getting-started/quick-start?utm_source=github&utm_medium=readme&utm_campaign=kache&utm_content=first_reuse
[kunobi-desktop]: https://kunobi.ninja/product/desktop?utm_source=github&utm_medium=readme&utm_campaign=kache&utm_content=desktop
