# E2E feature coverage

A map of kache's user-facing features to the e2e scenarios that exercise them,
and the gaps where a feature has no end-to-end scenario. The functional suite
(`just e2e`, `suite:e2e`) currently has **29 scenarios** (the `bench-*` cases are
load benchmarks, not feature tests).

This was produced by a cross-family audit (Claude + codex, independent passes):
both ranked **remote/S3** the #1 gap and converged on the daemon, restore-mode,
and `modified_input_guard` gaps; the config-toggle / `sync` / refusal-class gaps
came from the second pass.

## Well covered (feature → scenario)

| Feature / capability | Scenario(s) |
|---|---|
| Rust lifecycle (cold/warm/noop) | `e2e-multi-dep`, `e2e-rust-debug` |
| `cargo check` / `.rmeta`-only output | `e2e-rust-check` |
| Rust flag/key inputs (flag soup) | `e2e-rust-flag-soup` |
| Dependency artifact cascade | `e2e-rust-dep-cascade`, `e2e-multi-dep` |
| Cargo workspaces / proc-macro member | `e2e-rust-workspace`, `e2e-manifest-dir-runtime-workspace` |
| Cross-path key stability / relocation | `e2e-rust-dep-cascade`, `e2e-rust-out-of-tree-target`, `e2e-rust-symlinked-target` |
| Runtime path env deps must miss after relocate (`OUT_DIR`) | `e2e-out-dir-runtime`, `e2e-out-dir-dual-pattern` |
| Runtime `CARGO_MANIFEST_DIR` must miss after relocate | `e2e-manifest-dir-runtime-workspace` |
| `extra_inputs` (#220) | `e2e-rust-extra-inputs` |
| User `--remap-path-prefix` | `e2e-rust-remap-prefix` |
| Out-of-tree / symlinked target dir | `e2e-rust-out-of-tree-target`, `e2e-rust-symlinked-target` |
| Rust + C FFI (`cc` crate via build.rs) | `e2e-rust-c-ffi` |
| Cached `--test` executable permission contract (#298) | `e2e-rust-test-exec` |
| Exclude rules (`.kache.toml [cache].exclude`) | `e2e-exclude-rust`, `e2e-exclude-c` |
| Fallback wrapper (`KACHE_FALLBACK`, #109) | `e2e-rust-fallback`, `e2e-rust-sccache` |
| C / C++ object + depfile caching, header invalidation | `e2e-c-hello`, `e2e-cpp-hello`, `e2e-c-depinfo` |
| Flag modeling (gcc / clang / clang-cl) | `e2e-cc-bench-flags`, `e2e-cc-bench-flags-gnu`, `e2e-cc-cl-debug`, `e2e-cc-cl-xclang-deps` |
| Realistic flag-soup canaries | `e2e-cc-flag-soup`, `e2e-rust-flag-soup`, `e2e-cmake-ninja-flagset` |
| `__FILE__` / out-of-tree base-dir handling (#410) | `e2e-cc-file-macro-oot`, `e2e-cmake-file-macro-oot` |
| CMake launcher + Ninja generator | `e2e-cmake-out-of-tree`, `e2e-cmake-file-macro-oot`, `e2e-cmake-ninja-flagset` |
| Unsupported-flag passthrough | `e2e-c-passthrough` |
| Parallel cache access (`make -j`) race gate | `e2e-cc-parallel` |
| Restore content correctness (byte-for-byte) | `[diff]` in C/C++/CMake/Rust-FFI/workspace scenarios |

## Gaps — features with NO e2e scenario

| Feature / capability | Notes |
|---|---|
| **Remote / S3 cache** | upload/download round-trip, sigv4a auth, v1/v2/v3 pack formats, zstd compression, blob dedup, HEAD checks — *entirely untested e2e* |
| **`sync` / `save-manifest`** | user-facing remote population without a compiler invocation |
| **Daemon path** | prefetch/warming (shards), hash-files cache, remote-check HIT, upload queue |
| **Cross-machine dedup** | scenarios test cross-*path* (same host); nothing tests cross-*machine* via remote — kache's headline value prop |
| **Config behaviors as toggles** | `key_salt`, `ignore_env`, `path_only_env_vars`, `cc_extra_allowlist_flags`, `local_only`, `modified_input_guard` (#324) — none asserted to flip hit/miss e2e |
| **Refusal classes** | response files, multi-source, `-E`/`-S`, PCH/modules, stdout output (only `-c`+flag refusal covered) |
| **Platform restore modes** | reflink vs hardlink vs copy; Windows NTFS hardlink / ReFS block-clone (#435); macOS codesign of restored executables — restores happen but the *mode* is never asserted |
| **Rust edge keys** | custom target JSON, native `-L/-l`, `-Z`/`RUSTC_BOOTSTRAP`, sysroot, double-wrapper detection |
| Admin CLI around builds (gc/purge/doctor/stats/why-miss/clean) | out of e2e scope — covered by `tests/cli_commands_test.rs` instead (listed for completeness, not a true gap) |

## Prioritized missing scenarios

1. **Remote S3 round-trip** (local MinIO/localstack, or the existing wire-mock): cold miss → upload → fresh clone → remote HIT → download + restore. Largest load-bearing untested surface; covers pack format, compression, hash validation.
2. **Cross-machine / clone dedup via remote** — the product's core promise; closest existing test only does cross-path.
3. **Daemon prefetch + remote-check HIT** — the production async path (batching, upload queue, warmed cache).
4. **Config-toggle scenario** — flip `key_salt` / `ignore_env` / `path_only_env_vars` and assert hit ↔ miss changes.
5. **`modified_input_guard` (#324)** — mutate a tracked input mid-build; assert the result is NOT stored.
6. **Platform restore** — Windows hardlink/ReFS (#435) and macOS codesign-after-restore.
7. **Refusal/fallback bundle** — response file / multi-source / `-E` → passthrough, no cache entry.
8. **Rust edge-key** — custom target JSON + native search paths, to catch under-keying.
