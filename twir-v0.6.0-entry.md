# This Week in Rust — kache 0.6.0 entry

## Context (publishing history)

kache has appeared in TWIR under **Project/Tooling Updates** as a single bullet, each
release linking to a kunobi.ninja blog post:

- 2026-05-06 — kache 0.2.0: zero-copy, content-addressed Rust build cache (RUSTC_WRAPPER) → GitHub release
- 2026-05-20 — kache 0.3.0: zero-copy efficient worktree compilation → blog
- 2026-05-27 — Content-addressed Rust builds (or, what kache actually caches) → blog
- 2026-06-10 — kache 0.5.0: designing a correct compile-cache key → blog

0.4.x was skipped, so 0.6.0 follows 0.5.0 directly. 0.5.0 was about *designing* a correct
cache key. 0.6.0 is the payoff: it **proves the cache is shareable across independent clones
of a huge real-world tree (Firefox)**, extends caching to **C/C++ via clang-cl on Windows**,
and hardens the store so that sharing is safe.

---

## The submission (single bullet, Project/Tooling Updates) — FINAL

This release links the **GitHub release tag** (no blog post this cycle), as the 0.2.0 entry did.

```markdown
* [kache 0.6.0: cross-clone build-cache convergence on Firefox (8.45×), now with clang-cl/Windows C/C++ caching](https://github.com/kunobi-ninja/kache/releases/tag/v0.6.0)
```

> Heads-up: the linked release page has no Firefox writeup to back the 8.45× figure. If a
> reviewer pushes back, point them to PR #258 or add a backing link (blog/bench README).

---

## Highlights (what the release tag covers)

51 commits since v0.5.0, ordered by what matters most to a reader.

### 1. The headline: cross-clone cache convergence on Firefox
- A new Firefox benchmark builds the tree from **two independent checkouts** and measures how
  much of the cache is shared. kache converges **117 of 119** cross-checkout divergences —
  only **2 genuine ones remain** (a build-id timestamp) — for an **8.45× speedup** on the
  second clone (#258).
- The 8.45× is the **second-clone benchmark result**, not a blanket "every build is 8.45×
  faster" claim — keep the framing tied to the benchmark.

### 2. The reach: C/C++ caching via clang-cl on Windows
(Largest product surface this release — ~2,800 lines in `src/compiler`.)
- **clang-cl support** — dialect tables, `/EP` probe, cacheable `cl` + the Firefox flag set;
  caches clang-cl debug compiles (#285, #295, #312). kache is now a build cache for C/C++,
  not just Rust — which is what makes the Firefox result above possible.
- Drop `aws-lc-sys` (use `ring`) — **unblocks `aarch64-pc-windows-msvc`** (#257).
- Windows-aware cc path sentinels; full Windows compiler-path detection; `clippy-driver.exe`
  under `cargo clippy`; binstall `.zip` packaging on Windows (#311).
- Portable cc keys: map the Apple SDK path to a `<SDKROOT>` sentinel (#78); splice
  `-ffile-prefix-map` before the `--` separator (#300).

### 3. Cross-checkout cache sharing (path-normalized keys — the mechanism behind #1)
- Generalized path normalization so the same cache hits across **separate checkouts and
  machines**, not just locally: `KACHE_BASE_DIR` plus an opt-in `KACHE_PATH_ONLY_ENV_VARS`
  allowlist that lets per-checkout objdir/build-location env vars normalize to a sentinel
  instead of poisoning the key (#258).

### 4. The safety net: a hardened, shareable store
- Opt-in **content verification on local restore** — re-hash blobs before handing them back
  (#332).
- Fold **name / size / exec-bit / inode** into the content-dedup hash, length-prefixed, so
  identical bytes with different metadata can't collide (#324, #340, #341); cache format → **v16**.
- Opt-in **too-new-input guard** — refuse to store a compile whose inputs changed mid-build
  (#324); **never cache a degraded dep-info pre-pass** (#323); refuse `rustc @argfile`
  invocations; validate untrusted `cache_key` / `crate_name` before path/key interpolation.
- **Cross-process `gc.lock`** so concurrent GC drivers don't double-scan a shared store
  (#326, #347); offload `Gc`/`Stats`/`HashFiles` + periodic GC to `spawn_blocking` (#281);
  reclaim orphaned blobs via a GC + doctor sweep.

### 5. Also notable
- Strict **local-only mode** (`KACHE_LOCAL_ONLY` / `[cache] local_only`) (#221).
- Release-candidates now published to crates.io (Policy B).

---

## How to submit

TWIR entries land via PR against `rust-lang/this-week-in-rust`, editing the **upcoming draft**
file at `draft/<next-date>-this-week-in-rust.md`, adding the bullet under the
`### Project/Tooling Updates` heading (keep it alphabetical-ish / grouped with other release
announcements). Open the PR before that week's issue is cut.
