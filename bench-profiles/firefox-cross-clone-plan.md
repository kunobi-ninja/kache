# Firefox cross-checkout cache convergence — plan

## Goal

Make Firefox's compile cache hit across **two checkouts at different absolute paths**
(the cross-machine / cross-clone case that real CI shares), by eliminating every
*real* cache-key divergence source. The split is deliberate:

- **Cache-agnostic Firefox patches** — stop baking absolute paths / per-build values
  into build artifacts. These are pure reproducible-builds wins, help *any* cache
  (sccache included), and are upstreamable with **no dependency on kache**.
- **kache input-normalization** — normalize path-only build inputs in the key, gated
  so it can never cause a false hit. Lives entirely in *our* config; Firefox never
  references kache.
- **Bench harness / env** — fix a measurement bug and pin one nondeterministic value.

**Target metrics** (from the current baseline of 119 diverging crates / 1.29x warm
speedup, warm wall dominated by `gkrust` at 791s):
- Real diverging crates: 119 -> < ~5
- Warm speedup: 1.29x -> ~4-5x (once `gkrust` + the mozbuild cascade hit)
- No false hits (every normalization gated + proven)

## Root-cause map (complete — all 119 classified)

| Class | ~Count | Real? | Root cause | Fix layer | Cache-agnostic | Status |
|---|---|---|---|---|---|---|
| **mozbuild cascade** | 57 | yes | absolute objdir baked into `mozbuild.rlib` (consts + macro literals) -> `extern:mozbuild` diverges for every linker | FF rlib-clean patch + kache `KACHE_PATH_ONLY_ENV_VARS=BUILDCONFIG_RS` | FF part: yes | **DONE (validated)** |
| **cc `resolved_token`** | 50 | **no — harness artifact** | key-diff pairs cold/warm by crate_name; Firefox unified-build chunk membership / `Unified_cpp_*N` numbering shifts across clones, so it compares *mismatched* TUs. Shared TUs are byte-identical after masking. | fix the **bench key-diff harness** (match TUs by clone-invariant identity) | n/a | diagnosed |
| **cc `preprocessed`** (`buildid.cpp`, `ApplicationData.cpp`) | 2 | semi | `MOZ_BUILDID` wall-clock timestamp baked into generated source; the two clones built minutes apart | pin `MOZ_BUILD_DATE` in the build env (or accept) | yes | diagnosed |
| **generated `source:`** (`webrender`, `khronos_api`) | 2 | yes | `build.rs` bakes absolute `include_str!`/`include_bytes!` paths into generated `.rs` | FF: emit OUT_DIR-relative paths | yes | diagnosed |
| **env_dep path-only** (`crashping` CONVERSIONS_FILE, `mtu` BINDINGS, `typenum` TYPENUM_BUILD_CONSTS) | ~4 | yes | build scripts emit absolute `<objdir|OUT_DIR>/x.rs` consumed only via `include!(env!())` | kache: add to `KACHE_PATH_ONLY_ENV_VARS` (OUT_DIR contract) | no (kache) | diagnosed |
| **env_dep `CARGO_MANIFEST_DIR`** (`remote_settings`) | 1 | yes | `include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/dumps/..."))` — value-baked, the #167 danger; must NOT normalize | FF (application-services: OUT_DIR-relative dumps) or accept | no | diagnosed |

**Key insight:** ~50 of the 119 are phantom (a harness matching bug), and 57 are the
mozbuild cascade we already solved. The real residual after those two is **~12 crates**,
each with a small, well-understood fix.

## Workstreams

### A. Firefox cache-agnostic patches (upstreamable, no kache tie)
Each developed against a clean FF checkout, one concern per patch:
1. **`mozbuild` path-clean** — DONE, validated (`firefox-mozbuild-path-clean.patch`).
   Open items before upstreaming: vendored-crate `.cargo-checksum.json`; `nspr_cflags`/
   `nss_cflags` system-NSS guard; bare-`cargo`/rust-analyzer fallback; `windows_rs` (Windows).
2. **`webrender` build.rs** — emit `shaders.rs` `include_str!` paths relative to OUT_DIR
   (or inline the shader source; path-include is only a code-size optimization).
3. **`khronos_api` build.rs** — emit `webgl_exts.rs` `include_bytes!` paths relative to
   OUT_DIR instead of `env::current_dir()` (vendored gl-rs crate; vendor patch or upstream).
4. **`remote_settings` dumps** — resolve packaged dumps relative to OUT_DIR instead of
   `env!("CARGO_MANIFEST_DIR")` (application-services). Lower priority; or accept.

### B. kache features (our side, gated)
1. **`KACHE_PATH_ONLY_ENV_VARS`** — DONE (Track A). Generalizes the OUT_DIR path-only
   exception to a config-driven allowlist, gated by `path_is_only_used_for_includes`.
   Follow-ups: `[cache] path_only_env_vars` config-file knob + a dedicated unit test.
2. Allowlist set for Firefox: `BUILDCONFIG_RS, MOZ_TOPOBJDIR, MOZ_TOPSRCDIR,
   CONVERSIONS_FILE, GLEAN_METRICS_FILE, BINDINGS, TYPENUM_BUILD_CONSTS`. (Explicitly
   NOT `CARGO_MANIFEST_DIR` — #167.)

### C. bench harness + env
1. **Fix key-diff TU matching** — match cold/warm TUs by a clone-invariant identity
   (normalized output object path, or the masked resolved-token-stream hash), and drop
   unmatched/off-by-one unified chunks instead of slotting them under one crate_name.
   Removes the ~50 phantom `resolved_token` divergences.
2. **Pin `MOZ_BUILD_DATE`** in `firefox.toml` (a fixed 14-digit `YYYYMMDDHHMMSS`) so
   `buildid.cpp`/`ApplicationData.cpp` are byte-identical across clones.
3. **Wire `firefox.toml`**: `KACHE_PATH_ONLY_ENV_VARS=<allowlist>`, the `[[file]]
   mode=patch` refs for the FF patches, keep `KACHE_BASE_DIR=@TOPSRCDIR@`.

## Execution phases

1. **Clean-room the patches.** From a clean FF checkout (not the bench clone), re-derive
   the validated `mozbuild` patch + the new `webrender`/`khronos_api` ones, addressing the
   open items (vendored checksums, system-NSS, bare-cargo fallback). One patch per concern.
2. **Land kache Track A properly.** Config-file knob + tests; PR off `main` independent of
   the bench.
3. **Fix the bench harness** (key-diff TU matching) + pin `MOZ_BUILD_DATE` + wire
   `firefox.toml`.
4. **Full build + re-bench** with everything wired. Confirm: 119 -> < ~5 diverging,
   `gkrust` hits, speedup ~4-5x, zero leak warnings, zero false hits.
5. **Upstream** the cache-agnostic FF patches as reproducible-builds improvements (framed
   on their own merit, never "for a cache").

## Success criteria
- Re-bench shows diverging crates < ~5 and warm speedup >= ~4x.
- Every kache normalization is gated and has a regression test; `--negative-control` clean.
- Each FF patch builds a clean tree and is justified purely by reproducibility.

## Clean branch/PR structure
- `feat/path-only-env-vars` (off `main`) — kache Track A + config + tests. Independent PR.
- FF patches — clean patch files in `bench-profiles/`, one per concern, each upstreamable.
- `bench/firefox-cross-clone` — bench wiring (`firefox.toml`, harness fix) + this plan +
  the patch refs. The current validated commits are the reference.

## Reference (validated so far, on `bench/firefox-cross-clone`)
- `mozbuild.rlib` proven byte-identical cross-checkout; `mozbuild` hits cross-clone with
  FF patch + `KACHE_PATH_ONLY_ENV_VARS=BUILDCONFIG_RS`.
- Commits: `feat(cache-key): KACHE_PATH_ONLY_ENV_VARS`, `bench(firefox): cross-clone
  mozbuild convergence`.
