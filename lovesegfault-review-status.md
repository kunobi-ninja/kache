# kache — lovesegfault rio-build#51 review status

Status of every point from lovesegfault's review:
https://github.com/lovesegfault/rio-build/pull/51#issuecomment-4674696268

Legend: ✅ merged · 🟡 open PR · 📋 designed (not built) · ❌ not started · ✅* already fixed pre-session

## Implemented & merged into `main`
- **`--unpretty`/`--pretty` unparsed (re #218)** — ✅ Merged (#354): these now pass through instead of being miscached.
- **Unset `$VAR` in `extra_inputs` patterns** — ✅ Merged (#355): silently-folded replayable key now warns.
- **Loud size-string validation** (`KACHE_MAX_SIZE` etc. silently degrading) — ✅ Merged (#356).
- **Salt observability** (`apply_key_salt` had no tracing; `why-miss` ignored salt) — ✅ Merged (#357): trace line + active-salt shown in `why-miss`.
- **#242 passthrough cleans nothing** when crate_name/extra-filename can't be parsed — ✅ Merged (#358): pre-clean in passthrough + debug line.
- **extra_inputs warm-target limitation (point 1)** — ✅ Merged (#359): `kache doctor` warns when a declaring crate lacks a matching `rerun-if-changed`.
- **Docs gaps** (extra_inputs timing caveat, `RUSTC_WORKSPACE_WRAPPER` chaining, missing env vars) — ✅ Merged (#360).
- **`ignore_env` env-lockdown for pinned configs** (the stray-`KACHE_KEY_SALT` footgun) — ✅ Merged (#364).
- **Config epoch for the daemon** (restart on config change; kill the "`kache daemon stop` after editing max_size" step) — ✅ Merged (#366).

(#355–#360 landed bundled as #362.)

## Designed, not yet built (tracked on issue #368)
- **extra_inputs dependent re-keying (point 2, the "central table")** — 📋 Designed, not built.
- **extra_inputs workspace-root declarations / workspace-relative folding (point 3)** — 📋 Designed, not built (folded into #368).

## Already fixed before this session (no work needed)
- **`-Z` unstable flags** — ✅* keyed in cache-key v11.
- **`RUSTC_WORKSPACE_WRAPPER` chaining** — ✅* already implemented; we only added the missing docs (#360).
- **`KACHE_DISABLED` accepted values documented** — ✅* already documented (confirmed; env table also completed in #360).

## Not started
- **Lint flags not keyed** (`-W/-D/-A`, `CLIPPY_ARGS` → stale verdicts replayed) — ❌ Not started.
- **Salt lifecycle** (record salt per entry via DB migration, per-salt stats in `kache stats`, `gc --stale-salts` sweep) — ❌ Not started (note: the salt *observability* half is already done in #357).

---

**Scorecard:** ~14 distinct points → 10 implemented+merged, 0 open PRs, 2 designed/issue (#368), 3 already-fixed pre-session, 2 untouched (lint-flag keying, salt lifecycle).
