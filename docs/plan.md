# kache — work plan

Living roadmap for kache. The bias is **architectural prevention over fix-by-fix**:
when a class of bug shows up repeatedly, refactor the structure so the class
becomes hard to express, then delete the patches.

## Status snapshot

- **Merged**: PR #65 (Compiler trait, ArtifactKind, PostRestoreAction,
  classify_by_filename, classify_crate_type as single sources of truth)
- **Open**: #70 (cc-family wrapper skeleton + e2e fixtures), #71 (unified
  Rust e2e harness on top of #70)
- **Open draft**: #64 (interprocess transport, handed off to teammate)

## Why an architectural plan

Four bug classes were observed on a fork of kache (AlJohri/kache combined-fixes).
Each was a one-line patch; each pointed at a structural weakness:

| Fork commit | Symptom | Class |
|---|---|---|
| 572f321 | `.rcgu.o` files codesigned by accident | output-classification scattered |
| 59866c0 | re-signing mutates already-signed binary | post-restore actions ad-hoc |
| e422e55 | target dir paths leak into cache key | platform/path normalization scattered |
| c354ce4 | dep-info store path inconsistent with restore | per-kind store dispatch ad-hoc |
| e3f64ec | `oso_prefix` not stripped on macOS | platform-specific normalization buried |

PR #65 closed three of these by extracting the trait surface (Compiler,
ArtifactKind, PostRestoreAction) and the two single sources of truth
(`classify_by_filename`, `classify_crate_type`). The rest land as the
remaining PRs in this plan close the remaining axes.

## Next PRs (decoupling first, then features)

### PR2 — Platform abstraction
**Goal**: a `Platform` trait that owns macOS-specific behavior (codesign, oso_prefix
stripping, .dylib handling). Today that logic lives in `src/compile.rs` behind
`#[cfg(target_os)]` arms; making it an abstraction lets the same code run from
Linux against a macOS cache (cross-platform CI) without conditional compilation
poisoning every call site.

**Closes**: e3f64ec class (oso_prefix stripping). Sets the surface that real C/C++
caching will need for `__DATE__` / `__TIME__` neutralization (`SOURCE_DATE_EPOCH`).

**Test surface**: e2e harness already runs against macOS arm64 locally; CI runs
Linux. PR2 adds a unit-test matrix that exercises both platform impls from the
same machine via dependency injection (no real cross-compile needed for the
bug fix itself).

### PR3 — PathNormalizer
**Goal**: a `PathNormalizer` that strips machine-local prefixes from cache keys
(target dir, build root, $HOME). Today this is open-coded in `src/args.rs`;
hoisting it to a struct with a documented contract closes the e422e55 class
(target dir leak) by structure: any new path that flows into the cache key has
to go through the normalizer or fail review.

**Closes**: e422e55 class.

**Test surface**: e2e fixture additions — same multi-dep / rust-c-ffi project
built from two different absolute paths must produce identical cache keys.
This is a strong assertion, easy to run in the harness, and would have caught
the original fork bug.

### PR4 — Per-kind store dispatch
**Goal**: store/restore paths dispatched by `ArtifactKind`, not by string-matching
filenames inside `Compiler::store`. Today `RustcCompiler` and `CcCompiler` would
both grow `if name.ends_with(".d") ...` ladders if we're not careful; PR4 makes
the dispatch table the single source of truth (parallel to `classify_by_filename`).

**Closes**: c354ce4 class (dep-info store inconsistent with restore).

**Test surface**: e2e harness gains an assertion that for every artifact stored
during cold, an equivalent restore happens during warm — verified by diffing
file lists, not just hit counts.

### PR5+ — Real C/C++ caching
Skeleton lands in #70; real caching is sequenced as separate PRs against the
same e2e fixtures (c-hello, cpp-hello, rust-c-ffi). Each PR removes one
`RefuseReason::Unsupported` arm and lights up the corresponding assertions in
`kache-fixture.toml` (the `max_entries_after = 0` constraints flip to `min_*`).

Order of attack:
1. **Argument parser** (top ~10 flags first — `-c`, `-o`, `-I`, `-D`, `-O*`,
   `-std=`, `-g`, `-fPIC`, `-MMD/-MD`, `-MF`). Refuse the rest until proven
   safe; refuse-list grows as fixtures expose edge cases.
2. **Preprocessor-based cache key**. `cc -E` + blake3 + `SOURCE_DATE_EPOCH`
   injection to neutralize `__DATE__` / `__TIME__`. PR3's PathNormalizer
   handles the path side; PR2's Platform handles the date macro side.
3. **Output discovery**: `.o`, `.d`, executables, `.dylib` / `.so` / `.dll`.
   `classify_by_filename` already covers the extensions; PR4's dispatch table
   handles the storage side.
4. **Refuse-to-cache list**: response files, multi-arch (`-arch x86_64
   -arch arm64`), `--coverage`, `-gsplit-dwarf`, PCH, modules.

Each step ships with its own fixture (or extends an existing one) and flips a
declared assertion. The harness's structure means no script changes are needed
to land any of these — the `kache-fixture.toml` is the contract.

## Server side

`crates/kache-service` already exists as a planning service. C/C++ support
needs nothing new server-side until **multi-machine sharing** of C/C++ artifacts
is in scope (PR4-equivalent for cc); at that point the service grows the same
artifact-kind dispatch the local store has.

## E2E harness as the verification surface

PR #71 makes the harness the single source of truth for "kache works
end-to-end". Every PR in this plan extends one of:
- a fixture's `[assertions.<phase>]` (most common — declarative contract diff)
- a new fixture under `test-projects/<name>/` (when the new behavior needs
  its own minimal repro)
- the `Phase` enum in `crates/kache-e2e/src/runner.rs` (rare — only if the
  lifecycle itself grows a new universal phase)

This means each PR's review carries a visible "what changed about kache's
contract" diff in the toml, separate from the implementation diff in `src/`.

## Out of scope here

- Windows porting (#45) — tracked separately; the wrapper flock and daemon
  paths need their own arms before C/C++ caching is portable.
- Remote service feature parity for C/C++ — see "Server side" above.
- Coverage of build systems beyond cargo + make (cmake, ninja, bazel) — easy to
  add via new fixtures the day a real consumer needs them; the harness already
  treats build commands as opaque shell.
