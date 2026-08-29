# E2E feature coverage

A map of kache's user-facing features to the e2e scenarios that exercise them,
and the gaps where a feature has no end-to-end scenario. The functional suite
(`just e2e`, `suite:e2e`) currently has **44 scenarios** (the `bench-*` cases are
load benchmarks, not feature tests).

This was produced by a cross-family audit (Claude + codex, independent passes):
both ranked **remote/S3** the #1 gap and converged on the daemon, restore-mode,
and `modified_input_guard` gaps; the config-toggle / `sync` / refusal-class gaps
came from the second pass.

## Well covered (feature → scenario)

| Feature / capability | Scenario(s) / process test |
|---|---|
| Rust lifecycle (cold/warm/noop) | `e2e-multi-dep`, `e2e-rust-debug` |
| `cargo check` / `.rmeta`-only output | `e2e-rust-check` |
| `cargo check --all-targets` zero-byte `.rmeta` (#624) | `e2e-rust-check-all-targets` |
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
| Bypass rules (`[cache].bypass_env` / `_argv` / `_crates`, #222) | `e2e-bypass-rules` |
| Fallback wrapper (`KACHE_FALLBACK`, #109) | `e2e-rust-fallback`, `e2e-rust-sccache` |
| C / C++ object + depfile caching, header invalidation | `e2e-c-hello`, `e2e-cpp-hello`, `e2e-c-depinfo` |
| C compiler-name shims on PATH (no `CC=`) | `e2e-c-shims` |
| Flag modeling (gcc / clang / clang-cl) | `e2e-cc-bench-flags`, `e2e-cc-bench-flags-gnu`, `e2e-cc-cl-debug`, `e2e-cc-cl-xclang-deps` |
| Realistic flag-soup canaries | `e2e-cc-flag-soup`, `e2e-rust-flag-soup`, `e2e-cmake-ninja-flagset` |
| `__FILE__` / out-of-tree base-dir handling (#410) | `e2e-cc-file-macro-oot`, `e2e-cmake-file-macro-oot` |
| CMake launcher + Ninja generator | `e2e-cmake-out-of-tree`, `e2e-cmake-file-macro-oot`, `e2e-cmake-ninja-flagset` |
| Unsupported-flag passthrough | `e2e-c-passthrough` |
| Refusal by invocation shape (multi-source, response file) | `e2e-cc-multi-source`, `e2e-cc-response-file` |
| Parallel cache access (`make -j`) race gate | `e2e-cc-parallel` |
| Restore mtime convergence + cross-tree active-reader isolation (#677/#680/#794) | `e2e-rust-cross-tree` |
| In-flight coalescing across simultaneous cargo invocations (#646) | `e2e-rust-parallel-coalesce` |
| Restore content correctness (byte-for-byte) | `[diff]` in C/C++/CMake/Rust-FFI/workspace scenarios |
| S3 v3 sync across isolated caches (#695) | `tests/s3_remote_test.rs` (signed OpenDAL requests against a deterministic local wire store) |
| Daemon async upload + on-demand remote HIT across isolated clients (#696) | `tests/filesystem_remote_test.rs` (separate caches/source trees, no explicit sync, compiler-free byte-identical restore) |

## Gaps — features with NO e2e scenario

| Feature / capability | Notes |
|---|---|
| **S3 provider/auth variants** | The required process test covers signed v3 manifest/pack upload, list, download, import, zstd, and restore. Live AWS/R2/Ceph/MinIO credentials and legacy v1/v2 layouts remain outside the hermetic PR gate; SigV4a is not supported. |
| **`save-manifest`** | `sync --push/--pull --all` is process-tested; build-manifest and shard publication is not. |
| **Daemon path** | prefetch/warming (shards), hash-files cache |
| **Config behaviors as toggles** | `key_salt`, `ignore_env`, `path_only_env_vars`, `cc_extra_allowlist_flags`, `local_only`, `modified_input_guard` (#324) — none asserted to flip hit/miss e2e |
| **Refusal classes** | multi-source and response files now covered (`e2e-cc-multi-source`, `e2e-cc-response-file`); still open: `-E`/`-S`, PCH/modules, multi-arch fat binaries, stdout output |
| **Platform restore modes** | reflink vs hardlink vs copy; Windows NTFS hardlink / ReFS block-clone (#435); macOS codesign of restored executables — restores happen but the *mode* is never asserted |
| **Rust edge keys** | custom target JSON, native `-L/-l`, `-Z`/`RUSTC_BOOTSTRAP`, sysroot, double-wrapper detection |
| Admin CLI around builds (gc/purge/doctor/stats/why-miss/clean) | out of e2e scope — covered by `tests/cli_commands_test.rs` instead (listed for completeness, not a true gap) |

## Not expressible as e2e scenarios (belong in unit/integration tests)

The harness phase model is fixed — `cold` / `warm` / `noop` / `relocate` /
`relocate_modified` / `relocate_noop` — built for the build lifecycle plus
relocation. It has no "rebuild with a different config" phase and no
deterministic "modify an input within the build-start margin" hook, so two of
the originally-listed gaps do **not** fit and should stay where they are:

- **Config toggles** (`key_salt`, `ignore_env`, `path_only_env_vars`, …): proving
  a toggle flips hit↔miss needs a config change *between* otherwise-identical
  builds, which the phase model can't express. `key_salt`/`ignore_env` effects on
  the cache key are already unit-tested in `src/cache_key.rs`; CLI plumbing is in
  `tests/cli_commands_test.rs`.
- **`modified_input_guard` (#324)**: the too-new guard is mtime-vs-build-start
  timing-dependent, which can't be made deterministic in a fixture build. Keep it
  a unit test.

## Prioritized missing scenarios

1. **Platform restore** — Windows hardlink/ReFS (#435) and macOS codesign-after-restore.
2. **Rust edge-key** — custom target JSON + native search paths, to catch under-keying.
3. **Remaining refusal classes** — `-E`/`-S`, PCH/modules, multi-arch, stdout output → passthrough, no cache entry (extends `e2e-cc-multi-source` / `e2e-cc-response-file`).
4. **Optional live-provider qualification** — scheduled/non-blocking AWS, R2, Ceph, or MinIO smoke coverage for provider-specific credentials and behavior; the required PR gate stays hermetic.

### Done (this branch)

- ✅ **Refusal: multi-source + response file** — `e2e-cc-multi-source`, `e2e-cc-response-file` (verified green via the gate harness).
- ✅ **S3 v3 sync round-trip across isolated caches** — `tests/s3_remote_test.rs` drives cold compile → signed push → fresh-cache pull/import → compiler-free restore and verifies the manifest, compressed pack, and artifact bytes (#695).
- ✅ **Daemon async upload + on-demand remote HIT** — `tests/filesystem_remote_test.rs` drives a cold compile through the producer daemon's durable upload queue, waits for v3 publication without `sync`, and requires a fresh consumer daemon to restore the identical artifact with `compiler_runs = 0` (#696).
