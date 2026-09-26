use crate::args::RustcArgs;
use crate::path_normalizer::{PathNormalizer, check_for_path_leak};
use anyhow::{Context, Result};
pub(crate) use kache_format::{is_valid_cache_key, is_valid_crate_name};
pub(crate) use kache_store::file_hash::*;
use std::borrow::Cow;
use std::cell::{Cell, OnceCell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use crate::key_env::KeyEnv;

/// Bump this when cache key logic changes in a way that could have produced
/// incorrect entries. All entries from previous versions become unreachable.
///
/// v3: PathNormalizer replaces the ad-hoc `normalize_flags` (CWD-only,
/// fooled by macOS `/tmp` ↔ `/private/tmp` symlinks). Strips $HOME,
/// $CARGO_HOME, $CARGO_TARGET_DIR and the workspace root with stable
/// sentinels.
///
/// v4: `--remap-path-prefix` injection switched from a single
/// CWD-based mapping to multi-prefix using PathNormalizer's full rule
/// set. Output binaries now embed sentinel paths in DWARF / PDB
/// instead of machine-local prefixes — bytes are byte-incompatible
/// with v3 single-prefix outputs, so the bump invalidates v3 entries.
///
/// v5: `--emit` is now hashed. `cargo check` emits `metadata`
/// (`.rmeta`); `cargo build` emits `link` (`.rlib`). Same crate with
/// everything else the key hashed identical → same key, so a check's
/// metadata-only entry could be served to a build needing the
/// `.rlib`. The composition changed, so v4 entries are invalidated.
///
/// v6: `PathNormalizer` gained a rule for the rustc working directory
/// → `<WORKSPACE>`, so `--remap-path-prefix` now also rewrites DWARF
/// `DW_AT_comp_dir` (rustc records the raw CWD there). Debug builds
/// previously leaked the build path through `comp_dir`; the remapped
/// output is byte-incompatible with v5, so the bump invalidates it.
///
/// v7: `-Clinker=<path>` is no longer part of the key. mozbuild (and any
/// build that points rustc at a bootstrapped toolchain) sets
/// `-Clinker=/abs/path/to/clang++`, which previously baked the
/// machine-local path into the key — every clone produced a distinct
/// key for the same crate (Firefox bench measured 0.2% cross-clone key
/// stability). The linker's *identity* is still hashed via
/// `linker:<--version output>` (see `get_linker_identity`), which is
/// path-independent. Existing v6 entries become unreachable.
///
/// v8: `RUSTFLAGS` is whitespace-normalized before hashing. Cargo / mach
/// assemble the env value with cosmetically-varying whitespace across
/// compile profiles (extra spaces between flags, trailing spaces); the
/// raw string previously produced different cache keys for
/// semantically-identical flag sets. Observed on the Firefox bench as
/// the dominant source of "leaf" cache-key divergence — fixing it
/// stabilizes ~18 leaf crates and their non-mozbuild dependents.
///
/// v9: dep-info blobs use an explicit kache sentinel instead of `./`
/// for stored project-root paths. The old marker was ambiguous with
/// ordinary make depfile paths such as `../foo.h`, whose second dot
/// contains a `./` substring and could be expanded incorrectly on
/// restore.
///
/// v10: source files are hashed in content-hash order instead of
/// absolute-path order (a build-script-generated file under `OUT_DIR`
/// sorted differently once the build tree moved, leaking path-order into
/// the key and breaking relocated cache hits — #201). The update order
/// changes on EVERY platform, not just Windows, so the same crate hashes
/// to a different key; bump to invalidate v9 entries cleanly rather than
/// leave a silent partial invalidation. (Env-dep values are also now
/// un-escaped, which can change Windows OUT_DIR keys.)
///
/// v11: previously-unkeyed codegen-affecting inputs are now folded in —
/// `--sysroot`, native link flags (`-L`/`-l`), `-Z` flags, and the
/// CONTENTS of a custom `--target` JSON spec. Also unifies the cc recipe
/// onto this same constant (was a separate `CC_CACHE_KEY_VERSION`).
///
/// v12: cc resolved `-###` tokens are path-normalized through the same
/// prefix maps as the preprocessor stdout before hashing (were hashed
/// raw). Absolute build paths in those tokens — `-I` dirs, `-D` defines
/// like `FIREFOX_ICO="/abs/.../firefox.ico"`, input/`-o` paths — made the
/// cc key path-dependent, so two builds of the same TU at different paths
/// missed cross-machine / cross-clone (Firefox bench: `resolved_token`
/// was a top cross-clone divergence).
///
/// v13: cc prefix-map roots now also derive from the `-I` include dirs,
/// not just (cwd, source-dir). Objdir-generated TUs (`Unified_cpp_*`)
/// compile a source that lives IN the build dir, so the old derivation
/// collapsed to a narrow objdir subdir and leaked `__FILE__` paths into
/// `dist/include` + the source tree (Firefox bench: `preprocessed` was the
/// top cross-clone divergence, ~1000 TUs). The include dirs span the repo,
/// so their common ancestor with cwd reaches the repo root, making
/// cross-checkout cc caching work automatically. (`KACHE_BASE_DIR` is an
/// explicit override; `KACHE_CC_PATH_NORMALIZE=0` disables it all.)
///
/// v14: the per-checkout `from` side of `--remap-path-prefix` (and the clang
/// `-f*-prefix-map` family) is collapsed to a `<REMAP_FROM>` sentinel in the
/// RUSTFLAGS / CARGO_ENCODED_RUSTFLAGS key inputs. A build system's own path
/// remapping (Firefox `--enable-path-remapping`) emits
/// `--remap-path-prefix=/abs/clone-a/=/topsrcdir/` — the flag that makes the
/// *artifact* path-portable was itself making the *key* path-dependent, since
/// FROM is the checkout path. Keying on the stable TO target (and scrubbing
/// FROM) lets the build's declared remap and kache's key agree (Firefox bench:
/// `RUSTFLAGS` was the top cross-clone divergence, 392 crates, after remapping
/// fixed the source/include! leak).
///
/// Single source of truth for both the rustc recipe (this module) and
/// the cc recipe ([`crate::compiler::cc`]). The two hash distinct labels
/// (`key_version:` vs `cc_key_version:`) and disjoint field layouts, so
/// their entries never collide regardless of this number — the version
/// only controls *invalidation*. One constant, one bump.
// v16 (kunobi-ninja/kache#324): length-prefix the free-text key fields (cfg,
// env-dep, codegen flag args) so a value containing the old `\n`/`=` delimiter
// can't be confused with an adjacent field's boundary.
//
// v17 (kunobi-ninja/kache#399): the `--remap-path-prefix` SENTINEL SET is no
// longer folded into the key — only the remap on/off choice (multi-prefix vs
// none) is. The set's membership depended on which machine-local dirs existed
// relative to the build, so it varied across machines and across relocations
// when the build tree lived inside one of those dirs (an out-of-tree build
// under the system tempdir dropped the <TMPDIR> rule by de-dupe, diverging the
// key). It was also redundant with the already-keyed normalized path fields.
// Dropping it fixes out-of-tree relocate misses on Windows and improves
// cross-machine key stability. Removing the per-sentinel fold changes the key
// bytes for every crate, so bump to invalidate v16 entries cleanly.
//
// v18 (kunobi-ninja/kache#431): a build-script `cargo:rustc-env=VAR=<path under
// OUT_DIR>` used purely as an `include!(env!("VAR"))` locator (e.g. typenum's
// TYPENUM_BUILD_CONSTS → `$OUT_DIR/consts.rs`) is now path-normalized in the
// key, like OUT_DIR itself. Previously only the literal var `OUT_DIR` (or a
// user-allowlisted name) qualified, so typenum — a foundational dep of the
// whole substrate/crypto stack — kept an absolute build path in its key and
// re-keyed per checkout, missing cross-clone. Normalizing it changes typenum's
// (and any such crate's) key bytes, so bump to invalidate v17 entries cleanly.
//
// v19 (kunobi-ninja/kache#471): a `-l static=` GNU archive is now folded via a
// build-path-PORTABLE member-content hash ([`crate::native_archive`]) instead of
// a whole-file hash. The `cc` crate names archive members by a hash of the
// absolute build path (`cafca65b…-quickjs.o` vs `4af22b2a…`) while the object
// bytes are identical, so the whole-file hash re-keyed per checkout and missed
// cross-clone (rquickjs-sys, wasm-opt-cc, …). The portable hash ignores those
// names; at v19, non-GNU/unparseable archives fell back to the whole-file hash.
// v24 revises that identity definition and adds bounded BSD parsing. Either way
// the static-lib key bytes changed here, so v19 invalidated v18 entries cleanly.
//
// v20 (kunobi-ninja/kache#480 follow-up): coverage builds now fold the raw local
// path identity into the key, like the `KACHE_RUSTC_PATH_NORMALIZE=0` opt-out
// already did. Coverage skips `--remap-path-prefix` (llvm-cov / tarpaulin need
// real paths in the profraw), so it bakes machine-local paths into DWARF while
// the rest of the key normalized its path inputs — two checkouts computed the
// same `remap:none` key and a shared cache could serve one checkout's real-path
// coverage artifact to another. Folding [`fold_unremapped_path_identity`] for
// coverage (not just the opt-out) changes coverage key bytes, so bump to
// invalidate v19 coverage entries cleanly.
//
// v21 (kunobi-ninja/kache#485): remap-path-prefix TARGETS changed from
// angle-bracket sentinels (`<WORKSPACE>`, `<CARGO_HOME>`, `<CC_ROOT>`, …) to
// resolvable absolute paths (`/proc/self/cwd` on Linux, `/rustc/<hash>`,
// `/kache/*`) so samply / the Firefox Profiler and debuggers can resolve cached
// sources without configuration. The targets are baked into DWARF/PDB and into
// functional bytes (rustc `file!()` / `#[track_caller]` / panic locations; the
// clang/gcc `__FILE__` via `-ffile-prefix-map`), so the produced artifacts
// differ even though the cache-key `normalize()` sentinels are unchanged. The
// rustc sentinel set is not folded into the key (v17/#399) and the cc target
// strings ARE hashed, so a single bump covers both and prevents new builds from
// being served old-sentinel artifacts. Bump to invalidate v20 entries cleanly.
//
// v22 (kunobi-ninja/kache#521): target_dir and workspace_root derivation for
// cross-compilation. Stripping the target triple from target_dir() when cross-compiling
// alters the path remapping prefix and dep-info rewriting anchor. Bumping the key version
// invalidates v21 cross-compiled cache entries cleanly.
//
// #647 deliberately needs no bump: rustc response files were refused under
// every existing version, while their expanded effective arguments key exactly
// like the equivalent inline argv and leave ordinary invocation keys unchanged.
//
// v23 (kunobi-ninja/kache#330): env-dep values under the build's own OUT_DIR
// (the literal OUT_DIR and typenum-style locator vars) now normalize to an
// `<OUT_DIR:{unit-dir}>`-relative sentinel instead of running through the
// generic prefix rules. The generic rules kept per-location components inside
// the value when `CARGO_TARGET_DIR` sits outside the workspace (the derived
// workspace root is the target dir's parent, so `<WORKSPACE>` swallowed the
// target path), diverging keys across build locations for content-identical
// compiles. Cargo's per-unit directory component stays in the sentinel — a
// generated file can observe its own remapped path via `file!()`, so units
// differing only by unit hash must not collide. Bump so v22 entries with the
// old spelling invalidate cleanly.
//
// v24 (kunobi-ninja/kache#691): BSD / Darwin archives gain a bounded structural
// parser, while both GNU and BSD digests now retain exact effective member
// names and timestamps. rustc preserves member names when bundling archives
// into rlibs, and linkers can observe `archive-path(member)`, so ignoring `cc`'s
// path-derived prefixes was not safe across producer/consumer invocations.
// Unsupported non-thin archives use a lexical-path-bound digest; thin archives
// make the invocation uncacheable because their external bytes are absent from
// the container. This intentionally gives up #471/#691 cross-clone reuse when
// member names differ, preferring false misses over false hits.
//
// v25 (kunobi-ninja/kache#730): the Windows dep-info rewrite (#733) is escape-
// aware per line, but the entries the OLD rewrite already stored under v24 stay
// reachable, and they are not merely stale — they are build-breakers. The old
// whole-content `Relativize` could split an escaped `\\` pair while anchoring,
// so the CORRUPTION IS BAKED INTO THE STORED BYTES: the fixed `Expand` cannot
// repair an orphan escape, and cargo hard-rejects the restored `.d` with
// "unknown escape character", failing the compile on every hit (the nightly
// Firefox/Windows bench reproduced exactly this). The mixed fleet is exposed
// both ways too — a pre-#733 client restoring a correctly stored entry runs the
// old unescaped `Expand` and re-corrupts it. Both directions share key v24, so
// only a bump makes them unreachable. Cost is one cold rebuild, which v0.13.0
// users (key v22) already pay crossing to this release regardless.
//
// v26 (kunobi-ninja/kache#760): source identity now folds each normalized path
// together with its content hash. v25 retained only the sorted multiset of
// contents, so swapping two module bodies (or renaming an include_dir asset)
// could preserve the key while changing the compiled program. The same bump
// also makes old dep-info blobs that retain donor-worktree absolute paths
// unreachable; v26 stores separate target/cwd sentinels and re-roots both.
//
// v27 (kunobi-ninja/kache#808): generated dep-info sources beneath an
// in-workspace Cargo target now belong to the effective target before the
// higher-ranked workspace source root. v26 could store
// `__kache_workspace__/target_1/.../OUT_DIR/private.rs`; another concurrent
// Cargo process using target_2 expanded that donor suffix, failed
// validate-on-hit, evicted the entry, and recompiled. The stored dep-info bytes
// change, so the bump makes every incorrectly owned v26 blob unreachable.
//
// v28: every rustc lint-setting flag and `--check-cfg` now participates in
// the outcome key. v27 ignored `-W`/`-A` and check-cfg expectation sets, so a
// successful compile could be replayed after an allow was removed, a warning
// was enabled beneath a deny group, or accepted cfg values were tightened.
// v0.16.0 shipped v27, so the bump also protects mixed fleets from consuming
// already-persisted false-success entries.
//
// v29: native host-loaded links now pin the objects the driver actually
// places (Linux CRT/startup + libc hashes) and the macOS SDK identity
// (version + build), failing closed to passthrough when those essentials
// cannot be resolved. macOS debug links also inject ld64 `-oso_prefix` so
// `N_OSO` paths are relative to `--out-dir` rather than checkout-local.
// WASM admission is explicit for rustc-bundled self-contained targets.
// Shared with the cc recipe, so local and remote Rust/C entries from v28
// are unreachable; `kache gc --stale-schema` reclaims them.
//
// v30: native Windows MSVC links now pin the validated link.exe/lld-link and
// cl banners, selected architecture, MSVC/SDK/UCRT versions, and hashes of
// the selected CRT/UCRT libraries and of every `-l` library resolved through
// `-L`/`/LIBPATH`/LIB. Explicit `-C link-arg` input files (`.lib`, `.a`,
// `.obj`, `.o`, `.res`, `.def`, `.exp`, `.manifest`) and file-carrying LINK
// options (`/DEF`, `/DEFAULTLIB`, `/MANIFESTINPUT`, `/MANIFESTFILE`,
// `/PDBSTRIPPED`, ...) are keyed only as text, so they fail closed to
// passthrough. Windows GNU, cross-target, and metadata invocations remain
// unprobed. Existing Windows linked-output keys did not contain this
// identity, so invalidate them rather than mix schemas.
//
// v32: target_dir() now recognizes Cargo's new build-dir layout, where
// `--out-dir` is `<target>/<profile>/build/<pkg>/<hash>/out`. Before, it
// walked up two levels to `<target>/<profile>/build/<pkg>`, so `<TARGET>`
// missed other packages' OUT_DIR paths in `-L native=` and dep-info was
// anchored one package deep. The path remapping prefix and dep-info anchor
// change for every new-layout unit, as they did for cross builds in v22.
// Unreleased with it, the rest of kache learned the same layout: a cc
// compile maps its own and other units' OUT_DIRs, a macOS link strips the
// profile directory from N_OSO paths, and native archives under the target
// dir are keyed by their portable digest. Each changes new-layout keys only.
pub(crate) use kache_format::CACHE_KEY_VERSION;

/// Collapse runs of ASCII whitespace into single spaces and trim
/// leading / trailing whitespace.
///
/// `RUSTFLAGS` is whitespace-tokenized by rustc when it interprets the
/// env var, so `"-C a    -C b"` and `"-C a -C b"` produce the same
/// compile result. But cargo / mach assemble the value with
/// cosmetically-varying whitespace across compile profiles, and the
/// raw string would otherwise hash to different cache keys for
/// semantically-identical flag sets — observed on the Firefox bench
/// as the dominant source of "leaf" cache-key divergence (~18 crates
/// missing in warm despite cold having cached them).
///
/// Order is preserved: `-Cfoo=a -Cfoo=b` and `-Cfoo=b -Cfoo=a` produce
/// distinct strings because later flags override earlier ones in
/// rustc's parser, so they MUST keep distinct keys.
fn normalize_rustflags(rustflags: &str) -> String {
    rustflags.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Sentinel that replaces the volatile "from" path of a compiler path-remap
/// flag in the cache key.
const REMAP_FROM_SENTINEL: &str = "<REMAP_FROM>";

/// Collapse the per-checkout "from" side of compiler path-remap flags to a
/// fixed sentinel so two builds at different checkout paths hash identically.
///
/// `--remap-path-prefix=FROM=TO` (and the clang `-f*-prefix-map` family, which
/// can ride in RUSTFLAGS via `-Clink-arg`) carry a FROM that is the per-checkout
/// build path — e.g. Firefox's `--enable-path-remapping` emits
/// `--remap-path-prefix=/abs/clone-a/=/topsrcdir/`. FROM is *exactly* the path
/// the remap erases from the compiler's output, so it must not make the key
/// path-dependent; otherwise the very flag that makes the artifact portable
/// makes the key un-portable (Firefox bench: `--remap-path-prefix` left a
/// `clone-a`/`clone-b` residual that diverged 392 crates). We keep the flag and
/// the stable TO target and replace only FROM with [`REMAP_FROM_SENTINEL`], so
/// adding/removing a remap or changing TO still diverges the key. This mirrors
/// the cc recipe, which keys on the prefix-map `to` sentinel and scrubs the
/// `from` build path ([`crate::compiler::cc`]).
fn scrub_remap_from_prefixes<'a, I>(tokens: I) -> Vec<String>
where
    I: IntoIterator<Item = &'a str>,
{
    // Equals-form flags whose value is `FROM=TO`.
    const EQ_FLAGS: [&str; 4] = [
        "--remap-path-prefix=",
        "-ffile-prefix-map=",
        "-fdebug-prefix-map=",
        "-fmacro-prefix-map=",
    ];
    let mut out = Vec::new();
    let mut iter = tokens.into_iter();
    while let Some(tok) = iter.next() {
        if let Some(flag) = EQ_FLAGS.iter().find(|f| tok.starts_with(**f)) {
            out.push(format!("{flag}{}", scrub_remap_value(&tok[flag.len()..])));
        } else if tok == "--remap-path-prefix" {
            // Space-separated form: the value is the next token.
            out.push(tok.to_string());
            if let Some(value) = iter.next() {
                out.push(scrub_remap_value(value));
            }
        } else {
            out.push(tok.to_string());
        }
    }
    out
}

/// Replace the FROM half of a `FROM=TO` remap value with [`REMAP_FROM_SENTINEL`],
/// keeping TO. Splits on the LAST `=` to match rustc/clang (both let FROM
/// contain `=`). A value with no `=` is malformed and left untouched.
fn scrub_remap_value(value: &str) -> String {
    match value.rsplit_once('=') {
        Some((_from, to)) => format!("{REMAP_FROM_SENTINEL}={to}"),
        None => value.to_string(),
    }
}

/// Normalize only machine-local prefixes known to [`PathNormalizer`] on the
/// FROM side of a direct rustc remap. Unlike environment-provided build-system
/// remaps, an arbitrary direct FROM may not match this invocation at all, so
/// erasing it unconditionally would collide with a mapping that does match.
fn normalize_direct_remap_value(value: &str, path_normalizer: &PathNormalizer) -> String {
    match value.rsplit_once('=') {
        Some((from, to)) => format!("{}={to}", path_normalizer.normalize(from)),
        None => value.to_string(),
    }
}

/// Fold a user-declared salt into an already-computed cache key.
///
/// The salt captures toolchain divergence kache cannot observe from the
/// invocation itself — a glibc/mold/linker bump, a Nix store rebuild,
/// anything that changes compiled output without changing a tool's
/// `--version` banner (which is all the linker identity the key sees,
/// see [`get_linker_identity`]). Hashing the base key together with the
/// salt yields a distinct key per salt value while leaving the unsalted
/// case **byte-identical** to today: `None`/empty returns `base`
/// untouched, so no `CACHE_KEY_VERSION` bump is needed and a project
/// that never sets it is unaffected.
///
/// Shared by every compiler family (rustc and cc) so the salt applies
/// uniformly regardless of which adapter produced `base`.
///
/// `label` is the crate/source name used in the `[key:…]` trace line so a
/// salt-induced miss is visible under `KACHE_LOG=trace` alongside the other key
/// components — previously the salt was the one key part that folded silently,
/// so a miss caused by a (stray or rotated) salt was invisible to the
/// `why-miss` grep recipe.
pub(crate) fn apply_key_salt(base: String, salt: Option<&str>, label: &str) -> String {
    match salt {
        Some(salt) if !salt.is_empty() => {
            let keyed = fold_labeled(base, "key_salt", salt);
            tracing::trace!(
                "[key:{label}] key_salt={salt:?} -> {}",
                &keyed[..keyed.len().min(16)]
            );
            keyed
        }
        _ => base,
    }
}

/// Fold user-declared environment variables into an already-computed key
/// (kunobi-ninja/kache#635).
///
/// rustc records an env var in dep-info only when the crate reads it through
/// `env!`/`option_env!`. A **proc macro** that branches on `std::env::var`
/// while expanding is invisible to every input kache observes: the rustc
/// command line, the source hashes, and the `--extern` set are byte-identical
/// whether or not the var is set, yet the emitted artifact differs. Both
/// compiles then key the same and the second one restores the first one's
/// expansion. `proc_macro::tracked_env` would surface this properly, but it is
/// still unstable, so the var has to be declared.
///
/// Matching is by exact name, or by prefix when the pattern ends in `*`
/// (`BOLTFFI_*`), ASCII case-insensitive so a Windows environment — where the
/// OS itself treats names case-insensitively — behaves the same as a Unix one.
/// A `*` anywhere but the end is a literal character (a Unix process *can*
/// carry a variable named `A*B`), so `A*B` matches only that exact name.
///
/// Two things are folded, and both are load-bearing:
/// - the declared patterns, so *turning the feature on re-keys the crate*.
///   Without this, the build that leaves the vars unset would fold nothing,
///   land on its old key, and restore the very entry the declaration was meant
///   to escape — the poisoned entry is already in the cache by the time anyone
///   notices they need this setting. `Config::load` upper-cases and sorts the
///   patterns first, so two spellings that select the same variables cannot
///   split the cache.
/// - the matched `NAME=VALUE` pairs, which is what separates the two modes from
///   each other going forward.
///
/// Values are folded **exactly**, as their raw OS bytes — deliberately *not*
/// through the [`PathNormalizer`], and not via a lossy UTF-8 conversion. A
/// declared variable is an opaque semantic input: a macro is free to paste its
/// value straight into the code it emits, so two checkout paths that normalize
/// to the same sentinel can still produce different artifacts, and two distinct
/// non-UTF-8 values that both lossy-convert to `U+FFFD` can too. Either
/// shortcut trades the exact miscompile this function exists to prevent for hit
/// rate. The cost is real and is the right way round: a declared variable
/// holding a machine-local path makes that crate's key machine-specific. Declare
/// the switch a macro actually branches on, not a glob that sweeps in path
/// variables. This matches the policy [`env_dep_path_only_decision`] already
/// applies to reported `env!` deps — normalize only where a value is *proven*
/// to be nothing but a locator.
///
/// Empty pattern list = feature off, key byte-identical to the undeclared case.
/// The fold is union-only: a misdeclared pattern can cost a cache miss, never
/// restore a wrong artifact.
pub(crate) fn apply_key_env_vars(base: String, patterns: &[String], label: &str) -> String {
    if patterns.is_empty() {
        return base;
    }

    let (matched, matched_names) = matching_key_env_vars(patterns);
    let keyed = fold_labeled(base, "key_env_vars", &key_env_digest(patterns, matched));
    // Names only, never values: a declared var may legitimately hold a token or
    // another secret, and the trace log is what users paste into bug reports.
    tracing::trace!(
        "[key:{label}] key_env_vars patterns={patterns:?} matched={matched_names:?} -> {}",
        &keyed[..keyed.len().min(16)]
    );
    keyed
}

/// Digest the configured `key_env_vars` patterns and their current raw values
/// for an adaptive-incremental unit identity. A changed value must select a new
/// rustc state directory before the full artifact key is computed.
pub(crate) fn key_env_guard(patterns: &[String]) -> Option<String> {
    (!patterns.is_empty()).then(|| {
        let (matched, _) = matching_key_env_vars(patterns);
        key_env_digest(patterns, matched)
    })
}

type RawEnvPair = (Vec<u8>, Vec<u8>);

fn matching_key_env_vars(patterns: &[String]) -> (Vec<RawEnvPair>, Vec<String>) {
    // Matching runs on the lossy name because patterns are UTF-8; folding runs
    // on the raw bytes below, so a lossy collision here can only mis-select a
    // variable (a miss), never merge two variables into one key component.
    let mut matched: Vec<RawEnvPair> = Vec::new();
    let mut matched_names: Vec<String> = Vec::new();
    for (name, value) in std::env::vars_os() {
        let lossy = name.to_string_lossy();
        if !key_env_var_matches(patterns, &lossy) {
            continue;
        }
        matched_names.push(lossy.into_owned());
        matched.push((env_name_key_bytes(&name), env_os_key_bytes(&value)));
    }
    (matched, matched_names)
}

/// Digest the declared patterns plus the matched `(name, value)` byte pairs.
///
/// Split out from [`apply_key_env_vars`] because the ordering rules here decide
/// whether two environments key the same, and they are only testable if they
/// don't require rewriting the process environment to exercise.
fn key_env_digest(patterns: &[String], mut matched: Vec<RawEnvPair>) -> String {
    // `vars_os` iteration order is platform-defined, so sort for a digest that
    // is stable across processes and hosts. The exception is a process whose
    // environment carries the same name twice — only constructible by handing
    // execve a hand-built envp, since the `set_var` APIs replace. `getenv` then
    // returns the *first* occurrence, which makes the order semantically
    // observable, and sorting would erase it. Keep environ order in that case.
    let mut names_seen = std::collections::HashSet::new();
    let has_duplicate_names = matched
        .iter()
        .any(|(name, _)| !names_seen.insert(name.clone()));
    if !has_duplicate_names {
        matched.sort();
    }

    let mut hasher = blake3::Hasher::new();
    for pattern in patterns {
        fold_field(&mut hasher, b"key_env_pattern:", pattern.as_bytes());
    }
    for (name, value) in &matched {
        fold_field(&mut hasher, b"key_env_name:", name);
        fold_field(&mut hasher, b"key_env_val:", value);
    }
    hasher.finalize().to_hex().to_string()
}

/// An env var *value* as key bytes: the OS's own representation, losslessly.
///
/// `to_string_lossy` would map every distinct invalid sequence onto `U+FFFD`,
/// merging values a macro reading `var_os` can still tell apart.
fn env_os_key_bytes(value: &std::ffi::OsStr) -> Vec<u8> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        value.as_bytes().to_vec()
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        value.encode_wide().flat_map(u16::to_le_bytes).collect()
    }
    #[cfg(not(any(unix, windows)))]
    {
        value.to_string_lossy().into_owned().into_bytes()
    }
}

/// An env var *name* as key bytes.
///
/// Same lossless rule as [`env_os_key_bytes`], plus case folding on Windows:
/// there `PATH` and `Path` are one variable, so folding whichever casing the OS
/// happened to report would split the cache between two machines describing the
/// same environment. On Unix the two are genuinely different variables and the
/// case is preserved.
///
/// Windows compares names by an *uppercase* mapping, and that mapping is not
/// ASCII-only, so fold through `str::to_uppercase` (full Unicode) whenever the
/// name is representable — which is every name the OS actually produces. The
/// fallback for an unpaired surrogate keeps the raw units; it can only cost a
/// miss, and getting there at all means the name is not a real Windows name.
/// Both arms emit UTF-16LE so the two encodings can never be confused.
fn env_name_key_bytes(name: &std::ffi::OsStr) -> Vec<u8> {
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        // Tail expression, not `return`: on Windows the `cfg(not(windows))` arm
        // below is compiled out, so this block IS the function body and clippy
        // rejects the `return` under `-D warnings`.
        match name.to_str() {
            Some(name) => name
                .to_uppercase()
                .encode_utf16()
                .flat_map(u16::to_le_bytes)
                .collect(),
            None => name.encode_wide().flat_map(u16::to_le_bytes).collect(),
        }
    }
    #[cfg(not(windows))]
    env_os_key_bytes(name)
}

/// Env text as key bytes: the exact UTF-8 bytes when the text is valid
/// UTF-8, or `0xFF`-tagged lossless OS bytes when it isn't.
///
/// The valid arm is byte-identical to hashing the `String` that
/// `std::env::vars()` used to yield, so keys for all-UTF-8 environments
/// (every one cargo itself constructs) are unchanged. The tag byte never
/// occurs in valid UTF-8, so the two arms cannot collide — and unlike
/// `to_string_lossy`, distinct invalid sequences stay distinct instead
/// of merging under U+FFFD (see [`env_os_key_bytes`]).
fn env_text_key_bytes(text: &std::ffi::OsStr) -> Vec<u8> {
    match text.to_str() {
        Some(utf8) => utf8.as_bytes().to_vec(),
        None => {
            let mut bytes = vec![0xff];
            bytes.extend(env_os_key_bytes(text));
            bytes
        }
    }
}

/// The `CARGO_CFG_*` pairs of an environment, sorted by name.
///
/// Takes `vars_os` pairs rather than `vars()`, which panics if *any*
/// environment variable holds non-UTF-8 — even one this filter would
/// discard. Pairs stay `OsString` so the hasher can fold them
/// losslessly via [`env_text_key_bytes`].
///
/// The primary sort key is the lossy name — the same order the old
/// `String` sort produced for every valid-UTF-8 environment. The
/// lossless tiebreak pins two names that collide under U+FFFD to a
/// deterministic order instead of platform iteration order.
fn cargo_cfg_pairs(
    vars: impl Iterator<Item = (std::ffi::OsString, std::ffi::OsString)>,
) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
    let mut pairs: Vec<(std::ffi::OsString, std::ffi::OsString)> = vars
        .filter(|(name, _)| name.to_string_lossy().starts_with("CARGO_CFG_"))
        .collect();
    pairs.sort_by_cached_key(|(name, _)| {
        (name.to_string_lossy().into_owned(), env_os_key_bytes(name))
    });
    pairs
}

/// Does any `key_env_vars` pattern select the env var `name`?
///
/// Exact match, or prefix match when the pattern ends in `*`. ASCII
/// case-insensitive (see [`apply_key_env_vars`]).
fn key_env_var_matches(patterns: &[String], name: &str) -> bool {
    // Byte slicing, not `&name[..n]`: a lossy `vars_os` conversion can leave a
    // multi-byte replacement char in the name, and a prefix length landing
    // mid-character would panic on a str slice.
    let name = name.as_bytes();
    patterns
        .iter()
        .any(|pattern| match pattern.strip_suffix('*') {
            Some(prefix) => {
                let prefix = prefix.as_bytes();
                name.len() >= prefix.len() && name[..prefix.len()].eq_ignore_ascii_case(prefix)
            }
            None => name.eq_ignore_ascii_case(pattern.as_bytes()),
        })
}

/// Fold a labeled `value` into an already-computed key by re-hashing
/// `label:value\x1f base`. Used by post-hoc key components (the salt,
/// user-declared extra inputs) folded after [`compute_cache_key`] at the
/// per-compiler seam rather than inside it. Distinct labels can never
/// collide, and a component that produces no value simply isn't folded —
/// leaving the key byte-identical to the unaugmented case.
pub(crate) fn fold_labeled(base: String, label: &str, value: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(label.as_bytes());
    hasher.update(b":");
    hasher.update(value.as_bytes());
    hasher.update(b"\x1f");
    hasher.update(base.as_bytes());
    hasher.finalize().to_hex().to_string()
}

/// Fold `label` followed by a length-prefixed `value` into the cache-key hasher.
/// The length prefix removes field-boundary ambiguity: a free-text value that
/// contains the old `\n`/`=` delimiter (build-script cfgs, env-dep values,
/// codegen flag arguments) can no longer be confused with an adjacent field
/// (kunobi-ninja/kache#324).
fn fold_field<H: KeyFold>(hasher: &mut H, label: &[u8], value: &[u8]) {
    hasher.update(label);
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

/// The byte-fold surface shared by [`blake3::Hasher`] and [`GroupedHasher`],
/// so the key-fold helpers (and their tests, which drive a plain hasher) stay
/// agnostic to whether per-group tee-hashing is active.
trait KeyFold {
    fn update(&mut self, bytes: &[u8]);
}

impl KeyFold for blake3::Hasher {
    fn update(&mut self, bytes: &[u8]) {
        blake3::Hasher::update(self, bytes);
    }
}

impl KeyFold for GroupedHasher {
    fn update(&mut self, bytes: &[u8]) {
        GroupedHasher::update(self, bytes);
    }
}

/// Hex-prefix length persisted per key-field group — enough to make an
/// accidental collision between "changed" and "unchanged" implausible while
/// keeping the per-event cost ~a couple hundred bytes.
const KEY_FIELD_HEX: usize = 16;

/// A blake3 hasher that TEES every update into the current key-field group's
/// sub-hasher alongside the main key hasher (kunobi-ninja/kache#131). The
/// main digest is byte-for-byte what a plain `blake3::Hasher` fed the same
/// update sequence produces — grouping cannot change the cache key by
/// construction (`grouped_hasher_main_digest_matches_plain_blake3` pins it).
///
/// The per-group digests power the `explain_miss` diagnostics: persisted on
/// each event, then diffed on a miss to name WHICH input group changed.
/// `set_group` may name the same group across non-contiguous segments; the
/// sub-hasher just keeps accumulating.
struct GroupedHasher {
    main: blake3::Hasher,
    groups: std::collections::BTreeMap<&'static str, blake3::Hasher>,
    current: &'static str,
}

impl GroupedHasher {
    fn new(initial_group: &'static str) -> Self {
        GroupedHasher {
            main: blake3::Hasher::new(),
            groups: std::collections::BTreeMap::new(),
            current: initial_group,
        }
    }

    fn set_group(&mut self, group: &'static str) {
        self.current = group;
    }

    fn update(&mut self, bytes: &[u8]) {
        self.main.update(bytes);
        self.groups.entry(self.current).or_default().update(bytes);
    }

    /// Final key digest + the per-group hex prefixes for event persistence.
    fn finalize_with_fields(self) -> (blake3::Hash, std::collections::BTreeMap<String, String>) {
        let fields = self
            .groups
            .into_iter()
            .map(|(group, hasher)| {
                (
                    group.to_string(),
                    hasher.finalize().to_hex()[..KEY_FIELD_HEX].to_string(),
                )
            })
            .collect();
        (self.main.finalize(), fields)
    }
}

thread_local! {
    /// Per-group digests of the most recent [`compute_cache_key`] run on this
    /// thread, for the wrapper's event logging (one compile per wrapper
    /// process, same stash pattern as `link.rs`'s toggles). `None` until a key
    /// is computed (cc compiles, passthroughs).
    ///
    /// Thread-local rather than process-global (kunobi-ninja/kache#777): the
    /// write and every read are one wrapper invocation on one thread, so
    /// per-thread storage costs the production path nothing and makes the
    /// take-once contract hold under `cargo test`, where libtest runs each test
    /// on its own thread and a concurrent key computation would otherwise
    /// consume or overwrite another test's stash.
    static LAST_KEY_FIELDS: std::cell::RefCell<Option<std::collections::BTreeMap<String, String>>> =
        const { std::cell::RefCell::new(None) };
}

/// Clone the per-group key digests without consuming them.
///
/// Adaptive incremental policy needs the same input-group evidence as miss
/// diagnostics before event logging takes the thread-local stash.
pub fn peek_last_key_fields() -> Option<std::collections::BTreeMap<String, String>> {
    LAST_KEY_FIELDS
        .try_with(|stash| stash.borrow().clone())
        .ok()
        .flatten()
}

/// Take (consume) the per-group key digests of the last computed rustc key.
pub fn take_last_key_fields() -> Option<std::collections::BTreeMap<String, String>> {
    LAST_KEY_FIELDS
        .try_with(|stash| stash.borrow_mut().take())
        .ok()
        .flatten()
}

thread_local! {
    /// Per-EXTERN artifact digests of the most recent [`compute_cache_key`] run
    /// (kunobi-ninja/kache#609), stashed like [`LAST_KEY_FIELDS`].
    ///
    /// The `externs` group digest says only THAT some dependency's artifact
    /// changed. In an `extern:` cascade — one native `-sys` crate's `.a`
    /// diverging and re-keying everything above it — every downstream crate
    /// reports the same undifferentiated "externs changed", which is exactly
    /// the case that has to be diagnosed by hand today. Keeping the
    /// per-dependency hashes lets `why-miss` name WHICH dependency moved, then
    /// follow that dependency's own events to the root of the chain.
    ///
    /// The value is the dependency artifact's own content hash (the same bytes
    /// folded into the key), truncated to [`KEY_FIELD_HEX`] — not a digest of
    /// the folded segment. Same discriminating power, and it can be compared
    /// against hashes recorded elsewhere.
    static LAST_KEY_EXTERNS: std::cell::RefCell<Option<std::collections::BTreeMap<String, String>>> =
        const { std::cell::RefCell::new(None) };
}

/// The native archives a key hashed, with the unit's native search dirs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeyedNativeArchives {
    /// Archives whose content the key folded.
    pub archives: Vec<PathBuf>,
    /// The keyed archives the unit's rlib carries, through a bundled `-l
    /// static` spec. The other keyed archives are not in the rlib.
    pub bundled: Vec<BundledArchive>,
    /// On an rlib, the unit's `native=`, `all=` and bare `-L` dirs the scan
    /// reads (see [`NativeLinkContext::scanned_dirs`]), without repeats:
    /// where the bundle audit looks for unkeyed archives. Empty on other
    /// units.
    pub dirs: Vec<PathBuf>,
}

/// A keyed archive an rlib bundles, and how rustc stores it there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BundledArchive {
    pub path: PathBuf,
    /// Stored as one member named after the archive file (`+whole-archive`)
    /// rather than member by member.
    pub packed: bool,
}

thread_local! {
    /// [`KeyedNativeArchives`] of the most recent [`compute_cache_key`] run,
    /// stashed like [`LAST_KEY_EXTERNS`] for the wrapper's store-time bundle
    /// audit.
    static LAST_KEY_NATIVE_ARCHIVES: RefCell<Option<KeyedNativeArchives>> =
        const { RefCell::new(None) };
}

/// Take (consume) the native archives the last computed rustc key hashed.
pub fn take_last_key_native_archives() -> Option<KeyedNativeArchives> {
    LAST_KEY_NATIVE_ARCHIVES
        .try_with(|stash| stash.borrow_mut().take())
        .ok()
        .flatten()
}

/// Marker recorded for an extern whose artifact could not be hashed — a
/// sysroot crate (`std`, `core`), whose identity rides on rustc version + name
/// instead. Distinct from any real hash, so it never reads as a content match.
pub const EXTERN_UNREADABLE: &str = "(sysroot)";

/// Take (consume) the per-extern artifact digests of the last computed rustc
/// key. `None` for cc compiles and passthroughs, which compute no rustc key.
pub fn take_last_key_externs() -> Option<std::collections::BTreeMap<String, String>> {
    LAST_KEY_EXTERNS
        .try_with(|stash| stash.borrow_mut().take())
        .ok()
        .flatten()
}

thread_local! {
    /// Producing-unit identity per extern, teed off the same loop that computes
    /// [`LAST_KEY_EXTERNS`] (kunobi-ninja/kache#627).
    ///
    /// Keyed by the name the CONSUMER used, which under Cargo's
    /// `package = "..."` renaming is an alias (`foo_old` for a crate whose own
    /// events say `foo`). The value is the producer's `-C extra-filename`,
    /// recovered from the artifact path — the one identity visible from both
    /// sides, so `why-miss` can join a changed dependency to the exact unit
    /// that produced it instead of guessing by name.
    ///
    /// Absent for an extern whose path carries no such suffix (sysroot crates,
    /// non-cargo invocations); the walk then falls back to matching by name.
    static LAST_KEY_EXTERN_UNITS: std::cell::RefCell<
        Option<std::collections::BTreeMap<String, String>>,
    > = const { std::cell::RefCell::new(None) };
}

/// Take (consume) the per-extern producing-unit ids of the last computed rustc
/// key. Always taken alongside [`take_last_key_externs`] so a stale map cannot
/// outlive its digests.
pub fn take_last_key_extern_units() -> Option<std::collections::BTreeMap<String, String>> {
    LAST_KEY_EXTERN_UNITS
        .try_with(|stash| stash.borrow_mut().take())
        .ok()
        .flatten()
}

thread_local! {
    /// The compiling unit's own identity, stashed at key computation so the
    /// event writer needs no extra plumbing — the same pattern the per-group
    /// digests use (kunobi-ninja/kache#131). Set unconditionally at the top of
    /// [`compute_cache_key`], including to `None` when cargo passed no
    /// `-C extra-filename`, so it can never carry over from a previous compile.
    static LAST_KEY_UNIT_ID: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
}

/// Take (consume) the unit id of the last computed rustc key.
pub fn take_last_key_unit_id() -> Option<String> {
    LAST_KEY_UNIT_ID
        .try_with(|stash| stash.borrow_mut().take())
        .ok()
        .flatten()
}

thread_local! {
    /// The input closure the last [`compute_cache_key`] on this thread
    /// discovered, stashed like the per-group digests above.
    ///
    /// The wrapper needs it to record a prediction, but only once the compile
    /// or restore it belongs to has actually succeeded — which happens far
    /// below the key computation, past the store, the scheduler and the
    /// compiler. Threading a `DepInfo` through all of that would touch every
    /// caller of `compute_cache_key`; the stash is the pattern the other
    /// key by-products already use.
    static LAST_KEY_DEP_INFO: std::cell::RefCell<Option<DepInfo>> =
        const { std::cell::RefCell::new(None) };
}

/// Put a closure in the stash as a key computation would, so the wrapper's
/// recording gate can be tested without spawning a compiler.
#[cfg(test)]
pub(crate) fn stash_last_dep_info_for_test(dep_info: DepInfo) {
    let _ = LAST_KEY_DEP_INFO.try_with(|stash| *stash.borrow_mut() = Some(dep_info));
}

thread_local! {
    /// The tree digest the last guarded key computation on this thread used,
    /// so the record made from it carries the same digest.
    static LAST_KEY_TREE_DIGEST: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
}

/// Put a tree digest in the stash as a guarded key computation would.
#[cfg(test)]
pub(crate) fn stash_last_tree_digest_for_test(tree: &str) {
    let _ = LAST_KEY_TREE_DIGEST.try_with(|stash| *stash.borrow_mut() = Some(tree.to_string()));
}

thread_local! {
    /// Did the last key computed on this thread keep an OUT_DIR path (OUT_DIR
    /// itself, or a value under it) as a literal? A lib whose key does is one
    /// whose consumers are worth recording (see `out_dir_alias`).
    static LAST_KEY_BAKES_OUT_DIR: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Take (consume) whether the last computed rustc key keeps an OUT_DIR path.
pub(crate) fn take_last_key_bakes_out_dir() -> bool {
    LAST_KEY_BAKES_OUT_DIR
        .try_with(|stash| stash.replace(false))
        .unwrap_or(false)
}

/// Take (consume) the tree digest of the last computed rustc key: the crate
/// tree of a proc-macro-dependent unit, or the `OUT_DIR` guard of a registry
/// unit that looked for a relocated record.
pub(crate) fn take_last_tree_digest() -> Option<String> {
    LAST_KEY_TREE_DIGEST
        .try_with(|stash| stash.borrow_mut().take())
        .ok()
        .flatten()
}

/// Cap on entries digested for the tree guard. A crate directory past this is
/// a build tree or a monorepo root, and the pre-pass stays cheaper than
/// digesting it.
const CRATE_TREE_MAX_ENTRIES: usize = 20_000;

/// A content digest of everything under the crate directory and its
/// `OUT_DIR`, the two places a proc macro reads from by convention
/// (`CARGO_MANIFEST_DIR`-relative paths and generated files).
///
/// Content rather than metadata, so a fresh checkout matches a record made
/// from another one. Every file hashes through the persistent content cache,
/// so an unchanged tree costs stats, not reads. `None` when the crate
/// directory is unknown, unreadable, or too large to digest, which leaves the
/// unit on the pre-pass.
pub(crate) fn crate_tree_digest(file_hasher: &FileHasher<'_>) -> Option<String> {
    let manifest_dir = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR")?);
    let out_dir = std::env::var_os("OUT_DIR").map(PathBuf::from);
    crate_tree_digest_in(manifest_dir, out_dir, file_hasher)
}

/// [`crate_tree_digest`] for the package at `manifest_dir`.
fn crate_tree_digest_in(
    manifest_dir: PathBuf,
    out_dir: Option<PathBuf>,
    file_hasher: &FileHasher<'_>,
) -> Option<String> {
    // Only for published crates, whose package directory is immutable and
    // self-contained: a macro in one can only read files under the package or
    // its OUT_DIR. A workspace crate can point a macro at `../assets`, which
    // no digest of its own directory would notice, so it keeps the pre-pass.
    if !is_registry_package(&manifest_dir) {
        return None;
    }
    let mut roots = vec![(manifest_dir, &b"manifest_dir"[..], MANIFEST_DIR_SKIPPED)];
    if let Some(out_dir) = out_dir {
        roots.push((out_dir, &b"out_dir"[..], &[]));
    }
    tree_digest(roots, file_hasher, CRATE_TREE_MAX_ENTRIES)
}

/// Names the tree guard skips directly under the crate directory. A build
/// directory or a git checkout there is not what a macro reads, and `target`
/// in particular is rewritten by the build this key belongs to. `OUT_DIR`
/// skips nothing: a build script wrote all of it before this unit, and a
/// macro scanning it reads a nested `target` like any other file.
const MANIFEST_DIR_SKIPPED: &[&str] = &["target", ".git"];

/// Cap on entries digested for the `OUT_DIR` guard. Generated code is a few
/// files; a directory past this is a build tree, and the pre-pass stays
/// cheaper than digesting it.
const OUT_DIR_TREE_MAX_ENTRIES: usize = 256;

/// A content digest of `out_dir` alone: the guard for a relocated record of
/// a unit with no proc-macro dependency. Such a unit reads nothing it does
/// not name, so the package needs no digest; `OUT_DIR` does, because the
/// record says which generated files were read, not what else was generated.
pub(crate) fn out_dir_tree_digest(out_dir: &Path, file_hasher: &FileHasher<'_>) -> Option<String> {
    tree_digest(
        vec![(out_dir.to_path_buf(), &b"out_dir"[..], &[])],
        file_hasher,
        OUT_DIR_TREE_MAX_ENTRIES,
    )
}

/// Each root comes with the names directly under it to skip.
fn tree_digest(
    roots: Vec<(PathBuf, &[u8], &[&str])>,
    file_hasher: &FileHasher<'_>,
    max_entries: usize,
) -> Option<String> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"kache-crate-tree-v1\n");
    let mut budget = max_entries;
    // Roots are named by role, not by path: the identity the record is filed
    // under already knows the path, and the guard is about content.
    for (root, role, skipped) in roots {
        fold_field(&mut hasher, b"root:", role);
        let excluded: Vec<PathBuf> = skipped.iter().map(|name| root.join(name)).collect();
        crate_tree_fold(
            &root,
            &root,
            &excluded,
            file_hasher,
            &mut hasher,
            &mut budget,
        )?;
    }
    Some(hasher.finalize().to_hex().to_string())
}

/// Is `manifest_dir` an extracted registry package (`<CARGO_HOME>/registry/src/<index>/<pkg>`)?
pub(crate) fn is_registry_package(manifest_dir: &Path) -> bool {
    let mut components = manifest_dir.components().rev();
    let _package = components.next();
    let _index = components.next();
    let src = components.next();
    let registry = components.next();
    matches!(
        (src, registry),
        (Some(std::path::Component::Normal(src)), Some(std::path::Component::Normal(registry)))
            if src == "src" && registry == "registry"
    )
}

/// `<CARGO_HOME>/registry/src` for a registry package, the directory every
/// extracted package lives under whichever checkout builds it.
fn registry_src_root(manifest_dir: &Path) -> Option<&Path> {
    if !is_registry_package(manifest_dir) {
        return None;
    }
    manifest_dir.parent()?.parent()
}

fn crate_tree_fold(
    root: &Path,
    directory: &Path,
    excluded: &[PathBuf],
    file_hasher: &FileHasher<'_>,
    hasher: &mut blake3::Hasher,
    budget: &mut usize,
) -> Option<()> {
    let mut entries: Vec<_> = std::fs::read_dir(directory)
        .ok()?
        .collect::<std::io::Result<_>>()
        .ok()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        if excluded.contains(&path) {
            continue;
        }
        *budget = budget.checked_sub(1)?;
        let relative = path.strip_prefix(root).ok()?;
        fold_field(hasher, b"path:", relative.as_os_str().as_encoded_bytes());
        let metadata = std::fs::symlink_metadata(&path).ok()?;
        if metadata.file_type().is_symlink() {
            let target = std::fs::read_link(&path).ok()?;
            fold_field(hasher, b"symlink:", target.as_os_str().as_encoded_bytes());
        } else if metadata.is_dir() {
            fold_field(hasher, b"dir:", b"");
            crate_tree_fold(root, &path, excluded, file_hasher, hasher, budget)?;
        } else if metadata.is_file() {
            fold_field(hasher, b"file:", file_hasher.hash(&path).ok()?.as_bytes());
        } else {
            fold_field(hasher, b"other:", b"");
        }
    }
    Some(())
}

thread_local! {
    /// Did the last key computed on this thread derive its input set from a
    /// record rather than from the pre-pass?
    ///
    /// The wrapper needs it for the one rule that keeps stores sound: a
    /// derived key that misses locally must be recomputed the slow way before
    /// anything reaches the remote, the scheduler or the store.
    static LAST_KEY_USED_PREDICTION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// May the key refuse to discover a closure it has no record of, so the
    /// wrapper compiles first and keys from the dep-info rustc emits?
    static DEFER_DISCOVERY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// A closure handed in by the wrapper after such a compile: the next key
    /// computation uses it instead of a record or a pre-pass.
    static PROVIDED_DEP_INFO: std::cell::RefCell<Option<(DepInfo, Option<String>)>> = const { std::cell::RefCell::new(None) };
}

/// The key stopped before discovering the closure: no record, and the wrapper
/// allowed compiling first (see [`set_defer_discovery`]).
#[derive(Debug)]
pub struct DeferredDiscovery;

impl std::fmt::Display for DeferredDiscovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("closure discovery deferred until after the compile")
    }
}

impl std::error::Error for DeferredDiscovery {}

/// Allow (or forbid) deferring closure discovery for keys computed on this
/// thread. With no record and no remote to consult, a miss is certain, so
/// the dep-info pre-pass would only repeat what the compile is about to
/// emit; the wrapper compiles, hands the emitted closure to
/// [`provide_dep_info`] and keys again.
pub fn set_defer_discovery(allowed: bool) {
    DEFER_DISCOVERY.with(|cell| cell.set(allowed));
}

/// Use `dep_info` for the next key computed on this thread.
pub fn provide_dep_info(dep_info: DepInfo) {
    // Rekeying clears the per-key stashes. Carry the tree observed before
    // compilation with its emitted closure, so a changed tree still rejects
    // that prediction rather than blessing old inputs with a new digest.
    let tree = take_last_tree_digest();
    PROVIDED_DEP_INFO.with(|cell| *cell.borrow_mut() = Some((dep_info, tree)));
}

/// The closure rustc wrote to `path` during the compile whose crate root is
/// `source_file`: the same content the pre-pass reads from its own output.
pub fn dep_info_from_emitted(path: &Path, source_file: &Path) -> Result<DepInfo> {
    let content = read_dep_info_file(path)?;
    let mut source_files = parse_dep_info(&content);
    if source_files.is_empty() {
        source_files.push(source_file.to_path_buf());
    }
    let env_deps = parse_env_dep_info(&content);
    Ok(DepInfo {
        source_files,
        env_deps,
    })
}

/// Take (consume) whether the last computed rustc key came from a prediction.
pub(crate) fn take_last_key_used_prediction() -> bool {
    LAST_KEY_USED_PREDICTION
        .try_with(|stash| stash.replace(false))
        .unwrap_or(false)
}

/// Take (consume) the input closure of the last computed rustc key.
///
/// `None` for cc compiles, passthroughs, and any invocation with no source
/// file — none of which discovered a closure to remember.
pub(crate) fn take_last_dep_info() -> Option<DepInfo> {
    LAST_KEY_DEP_INFO
        .try_with(|stash| stash.borrow_mut().take())
        .ok()
        .flatten()
}

/// Stable identity for one dep-info source path.
///
/// Cargo/worktree-local roots use the same rule rustc receives through
/// `--remap-path-prefix`. Keep the dep-info spelling intact: canonicalizing a
/// symlink would merge two spellings even though rustc may embed them
/// differently through `file!()` or debug info. An unmodeled spelling stays
/// deliberately path-local through an opaque, lossless-OS-byte digest.
fn source_path_identity(file: &Path, path_normalizer: &PathNormalizer) -> Result<Vec<u8>> {
    if let Some(identity) = path_normalizer.source_path_identity(file) {
        return Ok(identity);
    }

    // Preserve the exact dep-info OS representation before hashing. Lossy
    // UTF-8, NFC normalization, or canonicalization can merge distinct source
    // spellings and recreate #760.
    let mut opaque = blake3::Hasher::new();
    opaque.update(b"kache-source-path-v1\0");
    opaque.update(&env_os_key_bytes(file.as_os_str()));
    Ok(format!("<OPAQUE_PATH>/{}", opaque.finalize().to_hex()).into_bytes())
}

/// The prediction identity of one rustc invocation, or `None` when there is
/// nothing to predict.
///
/// `None` when the invocation names no source file (no closure to discover),
/// or when the compiler's own version cannot be read — without it two
/// toolchains would share one record, and their closures need not match.
pub(crate) fn rustc_prediction_identity(args: &RustcArgs) -> Option<String> {
    rustc_prediction_identity_in_env(args, std::env::vars_os().collect())
}

fn rustc_prediction_identity_in_env(
    args: &RustcArgs,
    vars: Vec<(std::ffi::OsString, std::ffi::OsString)>,
) -> Option<String> {
    rustc_prediction_identity_with_args(args, vars, None)
}

fn rustc_prediction_identity_with_args(
    args: &RustcArgs,
    vars: Vec<(std::ffi::OsString, std::ffi::OsString)>,
    closure_args: Option<Vec<String>>,
) -> Option<String> {
    let source_file = args.source_file.as_ref()?;
    let rustc_version = get_rustc_version(&args.rustc).ok()?;
    Some(prediction_identity_in_env(
        &PredictionIdentityParts {
            rustc_version: &rustc_version,
            inner_rustc: args.inner_rustc.as_deref(),
            current_dir: std::env::current_dir().ok().as_deref(),
            source_file,
            closure_args: &closure_args
                .unwrap_or_else(|| closure_shaping_args(source_file, &args.all_args)),
            skip_path_remap: args.skip_path_remap(),
        },
        vars,
    ))
}

/// Share a prediction across Cargo target directories without relocating any
/// source. Only dependency-search and extern paths are virtualized; cwd,
/// source paths, cfg values and environment retain their original identity.
/// A record containing a source under target is never published here.
pub(crate) fn rustc_shared_prediction_identity(args: &RustcArgs) -> Option<String> {
    rustc_shared_prediction_identity_in(args, std::env::vars_os().collect())
}

/// [`rustc_shared_prediction_identity`] against an environment snapshot.
fn rustc_shared_prediction_identity_in(
    args: &RustcArgs,
    vars: Vec<(std::ffi::OsString, std::ffi::OsString)>,
) -> Option<String> {
    let target = args.target_dir()?;
    let manifest_dir = env_var_in(&vars, "CARGO_MANIFEST_DIR").map(Path::new);
    if !target.is_absolute() || !shared_prediction_eligible(&args.externs, manifest_dir) {
        return None;
    }
    let source = args.source_file.as_ref()?;
    let closure_args =
        shared_prediction_args(&closure_shaping_args(source, &args.all_args), &target);
    let identity = rustc_prediction_identity_with_args(
        args,
        shared_prediction_vars(vars.into_iter(), &target),
        Some(closure_args),
    )?;
    Some(format!("{SHARED_PREDICTION_PREFIX}{identity}"))
}

const SHARED_PREDICTION_PREFIX: &str = "shared-target-v2:";

/// Rows whose `OUT_DIR` sources are written relative to `OUT_DIR`
/// ([`PortablePrediction`]). The hash is the shared one, so a row here and
/// a row there always describe the same unit.
const RELOCATABLE_PREDICTION_PREFIX: &str = "shared-out-dir-v1:";

/// May a unit's record be shared across target directories?
///
/// A unit with no proc-macro dependency can: its closure is everything it
/// reads. A registry unit with one can too, because a macro there reads its
/// package and `OUT_DIR`, and the record carries a digest of both that the
/// reader checks before use. The target paths that differ between checkouts
/// only reach the key as extern content, which the key hashes.
fn shared_prediction_eligible(
    externs: &[crate::args::ExternDep],
    manifest_dir: Option<&Path>,
) -> bool {
    prediction_applies(externs) || manifest_dir.is_some_and(is_registry_package)
}

/// The value of `name` in an environment snapshot.
fn env_var_in<'a>(
    vars: &'a [(std::ffi::OsString, std::ffi::OsString)],
    name: &str,
) -> Option<&'a std::ffi::OsStr> {
    vars.iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_os_str())
}

/// The identity of a registry unit's relocated record: the shared hash
/// under its own prefix. `None` for any other unit, and for one with no
/// `OUT_DIR` to relocate.
fn relocatable_prediction_identity(
    args: &RustcArgs,
    vars: Vec<(std::ffi::OsString, std::ffi::OsString)>,
) -> Option<String> {
    let manifest_dir = Path::new(env_var_in(&vars, "CARGO_MANIFEST_DIR")?);
    if !is_registry_package(manifest_dir) || env_var_in(&vars, "OUT_DIR").is_none() {
        return None;
    }
    let shared = rustc_shared_prediction_identity_in(args, vars)?;
    let hash = shared.strip_prefix(SHARED_PREDICTION_PREFIX)?;
    Some(format!("{RELOCATABLE_PREDICTION_PREFIX}{hash}"))
}

/// The environment a shared record is identified by, with this build's own
/// `OUT_DIR` written relative to the target directory.
///
/// A build script's `OUT_DIR` is `<target>/<profile>/build/<unit>/out`, so
/// folding it verbatim gave one unit a different record in every build
/// directory: six Cargo jobs sharing a store each discovered `libc` from
/// scratch, and a warm build in another checkout found nothing. Relative, the
/// same unit keeps one record wherever it is built. The unit part still
/// carries Cargo's metadata hash, so two feature sets stay apart.
///
/// A value outside the target directory is left alone: it is not this
/// build's own output and nothing says another checkout would spell it the
/// same way.
fn shared_prediction_vars(
    vars: impl Iterator<Item = (std::ffi::OsString, std::ffi::OsString)>,
    target: &Path,
) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
    vars.map(|(name, value)| {
        if name != "OUT_DIR" {
            return (name, value);
        }
        let relative = target_relative_env_value(&value, target);
        (name, relative.unwrap_or(value))
    })
    .collect()
}

/// `value` rewritten relative to `target`, tagged so a literal value cannot
/// impersonate a rewritten one. `None` when it does not sit under `target`.
fn target_relative_env_value(value: &std::ffi::OsStr, target: &Path) -> Option<std::ffi::OsString> {
    let relative = Path::new(value).strip_prefix(target).ok()?;
    if relative
        .components()
        .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return None;
    }
    Some(std::ffi::OsString::from(format!(
        "kache-target-relative:{}",
        relative.to_str()?
    )))
}

fn shared_prediction_args(args: &[String], target: &Path) -> Vec<String> {
    let mut kind = None;
    args.iter()
        .map(|arg| {
            let path_arg = match kind.take() {
                Some(flag) => Some((flag, arg.as_str())),
                None => {
                    if arg == "--extern" || arg == "-L" {
                        kind = Some(arg.as_str());
                    }
                    arg.strip_prefix("--extern=")
                        .map(|value| ("--extern=", value))
                        .or_else(|| {
                            arg.strip_prefix("-L")
                                .filter(|s| !s.is_empty())
                                .map(|value| ("-L", value))
                        })
                }
            };
            let mapped = path_arg.and_then(|(flag, value)| {
                let (name, path) = value.split_once('=').unwrap_or(("", value));
                let relative = Path::new(path).strip_prefix(target).ok()?;
                if relative
                    .components()
                    .any(|c| !matches!(c, std::path::Component::Normal(_)))
                {
                    return None;
                }
                Some(("target", flag, name, relative.to_str()?))
            });
            // Tag every argument, including literals, so a user-supplied path
            // or cfg string cannot impersonate an encoded target-relative one.
            serde_json::to_string(&mapped.unwrap_or(("literal", "", "", arg))).unwrap()
        })
        .collect()
}

pub(crate) fn shared_prediction_can_record(args: &RustcArgs, dep_info: &DepInfo) -> bool {
    let manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR").map(PathBuf::from);
    shared_prediction_can_record_in(args, dep_info, manifest_dir.as_deref())
}

/// [`shared_prediction_can_record`] for the package at `manifest_dir`.
fn shared_prediction_can_record_in(
    args: &RustcArgs,
    dep_info: &DepInfo,
    manifest_dir: Option<&Path>,
) -> bool {
    args.target_dir().is_some_and(|target| {
        shared_sources_can_record(&dep_info.source_files, &target, manifest_dir)
    })
}

/// No source under `target`, and for a registry unit, every source under the
/// registry. A registry file is the same file for every checkout; anything
/// else a registry unit read, such as a workspace file a macro found through
/// a target path, may name a different file in the next checkout, so that
/// record stays with this one.
fn shared_sources_can_record(
    sources: &[PathBuf],
    target: &Path,
    manifest_dir: Option<&Path>,
) -> bool {
    let registry_src = manifest_dir.and_then(registry_src_root);
    sources.iter().all(|source| {
        !source.starts_with(target)
            && registry_src.is_none_or(|root| under_registry_src(source, root))
    })
}

/// Is `source` spelled as a file under the registry? A `..` may climb back
/// up inside the package the path names, as in the `<pkg>/src/../README.md`
/// rustc reports for `include_str!("../README.md")`, but not out of it. The
/// index and the package are the two levels below the root it has to stay in.
fn under_registry_src(source: &Path, registry_src: &Path) -> bool {
    suffix_within(source.as_os_str(), registry_src, 2).is_some_and(|suffix| !suffix.is_empty())
}

/// The bytes of `value` after `root`, for a path that stays inside `root`.
fn out_dir_suffix(value: &std::ffi::OsStr, root: &Path) -> Option<String> {
    suffix_within(value, root, 0)
}

/// The bytes of `value` after `root`, when `value` is `root` itself (an
/// empty suffix) or `root`, a separator, and components that never leave the
/// directory `floor` levels below `root` ([`stays_below`]). `None` for a
/// longer name that merely starts the same (`/o2` against `/o`), an empty
/// component, a `..` that climbs too far, or a suffix that is not UTF-8. The
/// suffix keeps its `.` and `..`, because a record has to reproduce the
/// spelling rustc reported.
///
/// `\` separates components on Windows only, so the walk has to hold both
/// with and without it: from `root`, `a\b/..` is one level down on Windows
/// and back at `root` on Unix.
fn suffix_within(value: &std::ffi::OsStr, root: &Path, floor: usize) -> Option<String> {
    let rest = value
        .as_encoded_bytes()
        .strip_prefix(root.as_os_str().as_encoded_bytes())?;
    let suffix = std::str::from_utf8(rest).ok()?;
    let Some(components) = suffix.strip_prefix(['/', '\\']) else {
        return suffix.is_empty().then(String::new);
    };
    (stays_below(components.split(['/', '\\']), floor) && stays_below(components.split('/'), floor))
        .then(|| suffix.to_string())
}

/// Does a walk down `components` stay inside the directory it reaches after
/// the first `floor` of them? A `..` is only taken from deeper than that. A
/// `.` stays where it is, as in the `src/./init.js` rustc reports for
/// `include_str!("./init.js")`.
///
/// The walk is lexical. It asks which directory the spelling names, and
/// rustc and the key both open the recorded spelling itself, so a symlink
/// inside an unpacked package resolves the same way for both. The link is
/// part of the package, the same in every checkout that shares the Cargo
/// home, just as it is for a path with no `..`. Resolving links here would
/// cost a syscall per path without changing what either one reads.
fn stays_below<'a>(components: impl IntoIterator<Item = &'a str>, floor: usize) -> bool {
    let mut depth = 0usize;
    components.into_iter().all(|component| match component {
        "" => false,
        "." => true,
        ".." if depth > floor => {
            depth -= 1;
            true
        }
        ".." => false,
        _ => {
            depth += 1;
            true
        }
    })
}

/// How often to check a prediction against the pre-pass it replaced.
///
/// The rules in [`validate_prediction`] are an argument, and this is the
/// measurement of that argument on real code. `sampled` pays one pre-pass per
/// [`VERIFY_PREDICTION_RATE`] units to keep the argument honest; `always` is
/// for a nightly, where the point is to count disagreements rather than to be
/// fast. Any disagreement means the prediction is used nowhere: the pre-pass
/// result wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VerifyPredictions {
    Off,
    Sampled,
    Always,
}

/// One in this many predictions is checked under `sampled`.
const VERIFY_PREDICTION_RATE: usize = 64;

fn parse_verify_predictions(value: Option<&str>) -> VerifyPredictions {
    match value {
        Some(v) if v.eq_ignore_ascii_case("sampled") => VerifyPredictions::Sampled,
        Some(v)
            if v.eq_ignore_ascii_case("always") || v == "1" || v.eq_ignore_ascii_case("true") =>
        {
            VerifyPredictions::Always
        }
        _ => VerifyPredictions::Off,
    }
}

/// Does THIS prediction get checked against the pre-pass it replaced?
///
/// Sampling is decided from the unit's own identity, not from a counter. A
/// rolling counter is what the restore verifier uses, and it works there
/// because the daemon is one long-lived process. The wrapper is not: it is a
/// fresh process per compile, so a process-global counter starts at zero every
/// time and `0 % rate == 0` makes every unit verify. That turned `sampled`
/// into `always`, and a nightly measured the cost of both paths at once
/// instead of the saving.
///
/// Hashing the identity also makes the sample stable: the same units are
/// checked on every build, so a disagreement is reproducible rather than a
/// one-off nobody can chase. Different units are covered across a graph
/// because the identities differ, not because a counter advanced.
/// [`verifies_prediction`] passes [`prediction_sample_identity`], so every
/// checkout of a project checks the same units too.
fn should_verify_this_prediction(mode: VerifyPredictions, identity: &str) -> bool {
    match mode {
        VerifyPredictions::Off => false,
        VerifyPredictions::Always => true,
        VerifyPredictions::Sampled => sampled_by_identity(identity, VERIFY_PREDICTION_RATE),
    }
}

/// One identity in `rate` selects, decided by its own bytes so that no shared
/// state and no ordering is involved.
fn sampled_by_identity(identity: &str, rate: usize) -> bool {
    if rate <= 1 {
        return true;
    }
    let digest = blake3::hash(identity.as_bytes());
    let bucket = u64::from_le_bytes(digest.as_bytes()[..8].try_into().unwrap_or([0; 8]));
    bucket % (rate as u64) == 0
}

/// Is this validated prediction checked against the pre-pass it replaced?
///
/// The caller reads the working directory and the environment; everything
/// that turns them into the decision is here, where tests reach it.
fn verifies_prediction(
    mode: VerifyPredictions,
    args: &RustcArgs,
    current_dir: Option<&Path>,
    env: impl Fn(&str) -> Option<std::ffi::OsString>,
) -> bool {
    should_verify_this_prediction(mode, &prediction_sample_identity(args, current_dir, env))
}

/// The unit as every checkout of the project names it. The sample is drawn
/// by this.
///
/// Not the prediction identity: that folds the working directory and the
/// absolute extern and search paths, so two checkouts of one project checked
/// different units. A perf gate builds base and head in separate checkouts,
/// so it counted pre-passes that the change under test did not cause.
///
/// Cargo's package name and version separate units that share a crate name,
/// such as every `build_script_build`. Its `-C metadata` would too, but that
/// folds the absolute path of a path dependency outside the workspace root,
/// and every unit depending on one inherits it. A target spec is folded by
/// name because Cargo passes its absolute path.
fn prediction_sample_identity(
    args: &RustcArgs,
    current_dir: Option<&Path>,
    env: impl Fn(&str) -> Option<std::ffi::OsString>,
) -> String {
    let manifest_dir = env("CARGO_MANIFEST_DIR").map(PathBuf::from);
    let source = args.source_file.as_deref().map(|source| {
        checkout_relative(source, &[manifest_dir.as_deref(), current_dir]).to_string_lossy()
    });
    let package = ["CARGO_PKG_NAME", "CARGO_PKG_VERSION"]
        .map(|var| env(var).map(|value| value.to_string_lossy().into_owned()));
    serde_json::to_string(&(
        args.crate_name.as_deref(),
        &args.crate_types,
        args.is_test,
        args.target.as_deref().map(target_name),
        package,
        &args.cfgs,
        source,
    ))
    .unwrap()
}

/// The name rustc knows a target by: a spec file goes by its stem.
fn target_name(target: &str) -> &str {
    target
        .strip_suffix(".json")
        .and_then(|spec| Path::new(spec).file_name()?.to_str())
        .unwrap_or(target)
}

/// `path` relative to the first of `roots` it lies under.
///
/// Cargo runs rustc from the workspace root for a member, whose source is
/// already relative, and from the package root for a dependency, whose source
/// is absolute. Both come out the same in every checkout. The roots include
/// Cargo's package root because Cargo spells it as it spells the source,
/// while the working directory comes back with symlinks resolved (`/tmp`
/// reads as `/private/tmp` on macOS).
fn checkout_relative<'a>(path: &'a Path, roots: &[Option<&Path>]) -> &'a Path {
    roots
        .iter()
        .flatten()
        .find_map(|root| path.strip_prefix(root).ok())
        .unwrap_or(path)
}

/// Do the two closures agree on what rustc reads?
///
/// Source order is not compared: the key sorts sources before folding them,
/// so two orderings of the same set produce the same key. Env deps are
/// compared as a set for the same reason.
fn closures_agree(predicted: &DepInfo, discovered: &DepInfo) -> bool {
    let mut a = predicted.source_files.clone();
    let mut b = discovered.source_files.clone();
    a.sort();
    b.sort();
    let mut ea = predicted.env_deps.clone();
    let mut eb = discovered.env_deps.clone();
    ea.sort();
    eb.sort();
    a == b && ea == eb
}

/// The closure a prior build recorded for this unit, if every rule still
/// holds against the tree as it is now.
///
/// Nothing here is trusted on its word: the record supplies candidate paths
/// and env values, and each one is re-checked. Any doubt is a `Rejection`,
/// and every `Rejection` means the same thing to the caller — spawn the
/// pre-pass and discover the closure for real.
fn predicted_key_inputs(
    args: &RustcArgs,
    file_hasher: &FileHasher<'_>,
) -> std::result::Result<DepInfo, Rejection> {
    let _trace = crate::phase_trace::phase("prediction_validate");
    if !file_hasher.uses_input_predictions() {
        return Err(Rejection::Disabled);
    }
    let vars: Vec<_> = std::env::vars_os().collect();
    let workspace = workspace_roots(args, &vars);
    // A unit with a proc-macro dependency is only predictable under the tree
    // guard: the record must carry the tree digest and it must still match.
    // The tree is the package for a registry unit and the whole workspace for
    // a workspace one. Computed once here and stashed, because the same digest
    // is what a record made from this invocation has to carry.
    let tree = if prediction_applies(&args.externs) {
        None
    } else {
        let _trace = crate::phase_trace::phase("crate_tree");
        let digest = match &workspace {
            Some(workspace) => workspace_tree_digest(workspace, file_hasher),
            None => crate_tree_digest(file_hasher),
        }
        .ok_or(Rejection::NotEligible)?;
        let _ = LAST_KEY_TREE_DIGEST.try_with(|stash| *stash.borrow_mut() = Some(digest.clone()));
        Some(digest)
    };
    let identity = rustc_prediction_identity(args).ok_or(Rejection::Disabled)?;
    let shared = rustc_shared_prediction_identity(args);
    // Filled when the record came from the remote, so a record that checks
    // out is kept here and not asked for again.
    let mut from_remote = None;
    let Some(record) = file_hasher
        .input_prediction(&identity)
        .or_else(|| file_hasher.input_prediction(shared.as_deref()?))
        .or_else(|| {
            let shared = shared.as_deref()?;
            let record = remote_plain_row(shared, &registry_src_of(&vars)?)?;
            from_remote = Some(shared);
            Some(record)
        })
    else {
        return match &workspace {
            Some(workspace) => workspace_key_inputs(args, file_hasher, workspace, vars, tree),
            None => relocated_key_inputs(args, file_hasher, tree),
        };
    };
    if let Some(tree) = &tree {
        match &record.tree {
            Some(recorded) if recorded == tree => {}
            Some(_) => return Err(Rejection::TreeChanged),
            None => return Err(Rejection::NoRecord),
        }
    }
    let validated = validate_prediction(
        &record,
        |path| std::fs::metadata(path).ok(),
        |path| path.exists(),
        |var| std::env::var(var).ok(),
    );
    if let Some(shared) = from_remote {
        keep_remote_plain_row(file_hasher, shared, args, &record, &validated);
    }
    validated
}

/// Keep a registry unit's shared row that came from the remote and checked
/// out, so the next build finds it locally.
fn keep_remote_plain_row(
    file_hasher: &FileHasher<'_>,
    identity: &str,
    args: &RustcArgs,
    record: &InputPrediction,
    validated: &std::result::Result<DepInfo, Rejection>,
) {
    crate::phase_trace::decision(
        "remote_prediction",
        if validated.is_ok() { "used" } else { "refused" },
    );
    if let Ok(dep_info) = validated {
        file_hasher.record_input_prediction(
            identity,
            args.crate_name.as_deref(),
            dep_info,
            record.tree.clone(),
        );
    }
}

/// The third lookup, after this checkout's row and the shared one both
/// missed: a registry unit's record from another target directory, with
/// `OUT_DIR` relocated to this one.
///
/// The guard is `tree` for a proc-macro dependent and a digest of `OUT_DIR`
/// otherwise. It is stashed before the lookup, so a record made from this
/// invocation, after a deferred compile included, carries the digest taken
/// before rustc ran. Only this row is checked against it.
fn relocated_key_inputs(
    args: &RustcArgs,
    file_hasher: &FileHasher<'_>,
    tree: Option<String>,
) -> std::result::Result<DepInfo, Rejection> {
    let vars: Vec<_> = std::env::vars_os().collect();
    let out_dir = env_var_in(&vars, "OUT_DIR")
        .and_then(std::ffi::OsStr::to_str)
        .map(str::to_string);
    let registry = registry_src_of(&vars);
    let identity = relocatable_prediction_identity(args, vars).ok_or(Rejection::NoRecord)?;
    let out_dir = out_dir.ok_or(Rejection::NoRecord)?;
    let guard = match tree {
        Some(tree) => tree,
        None => {
            let _trace = crate::phase_trace::phase("out_dir_tree");
            out_dir_tree_digest(Path::new(&out_dir), file_hasher).ok_or(Rejection::NoRecord)?
        }
    };
    let _ = LAST_KEY_TREE_DIGEST.try_with(|stash| *stash.borrow_mut() = Some(guard.clone()));
    let places = Places {
        out_dir: Some(&out_dir),
        workspace: None,
        registry: registry.as_deref().and_then(Path::to_str),
    };
    portable_key_inputs(file_hasher, &identity, args, &guard, &places)
}

/// Use this machine's row for `identity`, or failing that the remote's
/// ([`set_remote_rows`]), checked the same way. A remote row that passes is
/// kept locally, so the next build does not ask again.
fn portable_key_inputs(
    file_hasher: &FileHasher<'_>,
    identity: &str,
    args: &RustcArgs,
    guard: &str,
    places: &Places<'_>,
) -> std::result::Result<DepInfo, Rejection> {
    let validate = |record: &PortablePrediction| {
        validate_portable_prediction(
            record,
            guard,
            places,
            |path| std::fs::metadata(path).ok(),
            |path| path.exists(),
            |var| std::env::var(var).ok(),
        )
    };
    if let Some(record) = file_hasher.portable_prediction(identity) {
        return validate(&record);
    }
    let record = remote_portable_row(identity).ok_or(Rejection::NoRecord)?;
    let validated = validate(&record);
    crate::phase_trace::decision(
        "remote_prediction",
        if validated.is_ok() { "used" } else { "refused" },
    );
    if validated.is_ok() {
        file_hasher.record_portable_prediction(identity, args.crate_name.as_deref(), &record);
    }
    validated
}

thread_local! {
    /// Where a portable row comes from when this machine has none: the
    /// daemon's remote, installed by the wrapper when a remote is configured.
    static REMOTE_ROWS: std::cell::RefCell<Option<RemoteRows>> =
        const { std::cell::RefCell::new(None) };
}

/// Asks the remote for the row filed under an identity.
pub(crate) type RemoteRows = Box<dyn Fn(&str) -> Option<crate::prediction_share::SharedPrediction>>;

/// Let the key ask `rows` for a portable row this machine lacks, or stop
/// asking with `None` (kunobi-ninja/kache#1011).
pub(crate) fn set_remote_rows(rows: Option<RemoteRows>) {
    REMOTE_ROWS.with(|slot| *slot.borrow_mut() = rows);
}

fn remote_row(identity: &str) -> Option<crate::prediction_share::SharedPrediction> {
    REMOTE_ROWS.with(|slot| slot.borrow().as_ref().and_then(|rows| rows(identity)))
}

fn remote_portable_row(identity: &str) -> Option<PortablePrediction> {
    match remote_row(identity)? {
        crate::prediction_share::SharedPrediction::Portable(row) => Some(row),
        crate::prediction_share::SharedPrediction::Plain(_) => None,
    }
}

fn remote_plain_row(identity: &str, registry: &Path) -> Option<InputPrediction> {
    match remote_row(identity)? {
        crate::prediction_share::SharedPrediction::Plain(row) => {
            crate::prediction_share::plain_from_remote(&row, registry)
        }
        crate::prediction_share::SharedPrediction::Portable(_) => None,
    }
}

/// The bytes of `value` after `registry`, for a path inside one package under
/// it ([`under_registry_src`]), or `None`.
pub(crate) fn registry_suffix(value: &str, registry: &Path) -> Option<String> {
    suffix_within(std::ffi::OsStr::new(value), registry, 2).filter(|suffix| !suffix.is_empty())
}

/// Is a shared row filed under `identity`? A registry unit's may travel
/// (see [`crate::prediction_share`]).
pub(crate) fn is_shared_target_identity(identity: &str) -> bool {
    identity.starts_with(SHARED_PREDICTION_PREFIX)
}

/// `<CARGO_HOME>/registry/src` for a registry unit, from its
/// `CARGO_MANIFEST_DIR`.
pub(crate) fn registry_src_of(
    vars: &[(std::ffi::OsString, std::ffi::OsString)],
) -> Option<PathBuf> {
    registry_src_root(Path::new(env_var_in(vars, "CARGO_MANIFEST_DIR")?)).map(Path::to_path_buf)
}

/// Is a row filed under `identity` free of machine-specific paths, so that it
/// may travel through the remote? The workspace and relocated `OUT_DIR` rows.
pub(crate) fn is_portable_identity(identity: &str) -> bool {
    identity.starts_with(WORKSPACE_PREDICTION_PREFIX)
        || identity.starts_with(RELOCATABLE_PREDICTION_PREFIX)
}

/// The fourth lookup, for a workspace or path unit (kunobi-ninja/kache#1005):
/// a record made in another checkout of the same workspace, with its sources
/// written relative to the workspace root and `OUT_DIR`.
///
/// The guard is a digest of the whole workspace (less the target directory
/// and `.git`) and of `OUT_DIR`, taken before rustc runs. A record is used
/// only when both are byte for byte what the recorder had, so a macro that
/// scans the workspace, even one reached through an rlib rather than a
/// direct proc-macro dependency, finds the same files here.
fn workspace_key_inputs(
    args: &RustcArgs,
    file_hasher: &FileHasher<'_>,
    workspace: &WorkspaceRoots,
    vars: Vec<(std::ffi::OsString, std::ffi::OsString)>,
    tree: Option<String>,
) -> std::result::Result<DepInfo, Rejection> {
    let identity =
        workspace_prediction_identity(args, vars, workspace).ok_or(Rejection::NoRecord)?;
    let guard = match tree {
        Some(tree) => tree,
        None => {
            let _trace = crate::phase_trace::phase("workspace_tree");
            workspace_tree_digest(workspace, file_hasher).ok_or(Rejection::NoRecord)?
        }
    };
    let _ = LAST_KEY_TREE_DIGEST.try_with(|stash| *stash.borrow_mut() = Some(guard.clone()));
    let places = workspace.places().ok_or(Rejection::NoRecord)?;
    portable_key_inputs(file_hasher, &identity, args, &guard, &places)
}

/// The directories a workspace unit's record is relocated against.
#[derive(Debug, Clone)]
pub(crate) struct WorkspaceRoots {
    pub(crate) root: PathBuf,
    /// The working directory rustc resolves relative paths against, as the
    /// bytes after `root` (`""` at the root itself).
    pub(crate) cwd: String,
    pub(crate) canonical_root: PathBuf,
    pub(crate) target: PathBuf,
    pub(crate) canonical_target: PathBuf,
    pub(crate) out_dir: Option<PathBuf>,
}

impl WorkspaceRoots {
    /// Where this invocation puts a record's relative entries.
    fn places(&self) -> Option<Places<'_>> {
        Some(Places {
            out_dir: match &self.out_dir {
                Some(out_dir) => Some(out_dir.to_str()?),
                None => None,
            },
            workspace: Some(self.root.to_str()?),
            registry: None,
        })
    }
}

/// This invocation's workspace, when its package is a workspace or path
/// package inside the workspace Cargo builds and the target directory is
/// that workspace's own.
///
/// `None` for a registry package (see [`relocated_key_inputs`]), a package
/// outside the workspace, a target directory elsewhere, and any root that
/// cannot be canonicalized: those units keep checkout-local records.
fn workspace_roots(
    args: &RustcArgs,
    vars: &[(std::ffi::OsString, std::ffi::OsString)],
) -> Option<WorkspaceRoots> {
    workspace_roots_in(args, vars, &std::env::current_dir().ok()?)
}

/// [`workspace_roots`] for rustc running in `current_dir`. The root is
/// [`RustcArgs::verified_workspace_root`]: absolute, the parent of the target
/// directory, and holding a manifest.
fn workspace_roots_in(
    args: &RustcArgs,
    vars: &[(std::ffi::OsString, std::ffi::OsString)],
    current_dir: &Path,
) -> Option<WorkspaceRoots> {
    let manifest_dir = Path::new(env_var_in(vars, "CARGO_MANIFEST_DIR")?);
    if is_registry_package(manifest_dir) {
        return None;
    }
    let root = args.verified_workspace_root(current_dir)?;
    let target = args.target_dir()?;
    suffix_within(manifest_dir.as_os_str(), &root, 0)?;
    let canonical_root = std::fs::canonicalize(&root).ok()?;
    // The working directory may be spelled through either root: on macOS
    // `current_dir` reports `/private/var/...` for a root Cargo spells
    // `/var/...`.
    let cwd = suffix_within(current_dir.as_os_str(), &root, 0)
        .or_else(|| suffix_within(current_dir.as_os_str(), &canonical_root, 0))?;
    Some(WorkspaceRoots {
        cwd,
        canonical_root,
        canonical_target: std::fs::canonicalize(&target).ok()?,
        out_dir: env_var_in(vars, "OUT_DIR").map(PathBuf::from),
        root,
        target,
    })
}

/// Rows of workspace and path units, filed under an identity with the
/// checkout taken out ([`workspace_prediction_identity`]).
const WORKSPACE_PREDICTION_PREFIX: &str = "shared-workspace-v1:";

/// The identity of a workspace unit's relocated record: the ordinary
/// identity with the working directory, the crate root and
/// `CARGO_MANIFEST_DIR` written relative to the workspace root, and target
/// paths relative to the target directory as the shared identity writes
/// them. Everything else stays as spelled, so an argument or variable that
/// still names the checkout only keeps two checkouts apart.
fn workspace_prediction_identity(
    args: &RustcArgs,
    vars: Vec<(std::ffi::OsString, std::ffi::OsString)>,
    workspace: &WorkspaceRoots,
) -> Option<String> {
    let source = args.source_file.as_ref()?;
    let root = &workspace.root;
    let within = |path: &Path| suffix_within(path.as_os_str(), root, 0);
    let source_spelling = if source.is_absolute() {
        format!("kache-workspace:{}", within(source)?)
    } else {
        format!("kache-cwd:{}", source.to_str()?)
    };
    let current_dir = format!("kache-workspace:{}", workspace.cwd);
    let mut closure_args = shared_prediction_args(
        &closure_shaping_args(source, &args.all_args),
        &workspace.target,
    );
    // The crate root leads the closure arguments; spell it as above.
    *closure_args.first_mut()? =
        serde_json::to_string(&("source", "", "", source_spelling.as_str())).ok()?;
    let vars = shared_prediction_vars(vars.into_iter(), &workspace.target)
        .into_iter()
        .map(|(name, value)| {
            if name != "CARGO_MANIFEST_DIR" {
                return Some((name, value));
            }
            let relative = within(Path::new(&value))?;
            Some((name, format!("kache-workspace:{relative}").into()))
        })
        .collect::<Option<Vec<_>>>()?;
    let rustc_version = get_rustc_version(&args.rustc).ok()?;
    let identity = prediction_identity_in_env(
        &PredictionIdentityParts {
            rustc_version: &rustc_version,
            inner_rustc: args.inner_rustc.as_deref(),
            current_dir: Some(Path::new(&current_dir)),
            source_file: Path::new(&source_spelling),
            closure_args: &closure_args,
            skip_path_remap: args.skip_path_remap(),
        },
        vars,
    );
    Some(format!("{WORKSPACE_PREDICTION_PREFIX}{identity}"))
}

/// A content digest of the workspace, less its target directory and `.git`,
/// and of `OUT_DIR`: the guard for a workspace unit's records.
fn workspace_tree_digest(
    workspace: &WorkspaceRoots,
    file_hasher: &FileHasher<'_>,
) -> Option<String> {
    workspace_tree_digest_within(workspace, file_hasher, CRATE_TREE_MAX_ENTRIES)
}

/// [`workspace_tree_digest`] with its entry budget supplied.
fn workspace_tree_digest_within(
    workspace: &WorkspaceRoots,
    file_hasher: &FileHasher<'_>,
    max_entries: usize,
) -> Option<String> {
    let target = suffix_within(workspace.target.as_os_str(), &workspace.root, 0)?;
    let target = target.trim_start_matches(['/', '\\']);
    let skipped = [target, ".git"];
    let mut roots = vec![(workspace.root.clone(), &b"workspace"[..], &skipped[..])];
    if let Some(out_dir) = &workspace.out_dir {
        roots.push((out_dir.clone(), &b"out_dir"[..], &[][..]));
    }
    tree_digest(roots, file_hasher, max_entries)
}

/// Join only when an eligible unit needs discovery. A peer holds the lock
/// until it has published a successful prediction and artifacts; this caller
/// then validates the record and computes its own complete key as usual.
fn prediction_discovery_identity(args: &RustcArgs, file_hasher: &FileHasher<'_>) -> Option<String> {
    if !file_hasher.uses_input_predictions() || !prediction_applies(&args.externs) {
        return None;
    }
    rustc_shared_prediction_identity(args).or_else(|| rustc_prediction_identity(args))
}

/// The flight two processes discovering the same unit share. With
/// predictions on it is the record identity where one applies, so the
/// waiter can read what the owner publishes. Otherwise the same identity
/// is only a name for the unit: the waiter finds the owner's entry in the
/// store instead. A unit with a proc-macro dependency gets that name too;
/// its record needs the crate-tree guard, but a flight is only a lock, and
/// compiling before keying never reads a record.
fn discovery_flight_identity(args: &RustcArgs, file_hasher: &FileHasher<'_>) -> Option<String> {
    prediction_discovery_identity(args, file_hasher)
        .or_else(|| rustc_shared_prediction_identity(args))
        .or_else(|| rustc_prediction_identity(args))
}

/// Discover the source closure that feeds the key.
///
/// The dep-info pre-pass enumerates the real closure. If it fails we must NOT
/// fabricate a crate-root-only `DepInfo` and key off it: that under-specifies
/// the inputs, so a later build whose transitive sources (`#[path]`,
/// `include_str!`, generated files) changed would produce the same key and
/// restore a stale artifact (kunobi-ninja/kache#323). Propagate the error so
/// the wrapper passes through to the real compiler and never stores under an
/// incomplete input set.
///
/// `None` when the invocation names no source file: there is no closure to
/// discover, and the key simply folds no source or env-dep group.
fn resolve_key_inputs(
    args: &RustcArgs,
    file_hasher: &FileHasher<'_>,
    crate_name: &str,
    env: &KeyEnv,
) -> Result<Option<DepInfo>> {
    if let Some((provided, tree)) = PROVIDED_DEP_INFO.with(|cell| cell.borrow_mut().take()) {
        let _ = LAST_KEY_TREE_DIGEST.try_with(|stash| *stash.borrow_mut() = tree);
        crate::phase_trace::decision("prediction", "emitted");
        tracing::trace!("[key:{}] inputs=emitted-dep-info", crate_name);
        return Ok(Some(provided));
    }
    if args.source_file.is_some() {
        let mut prediction = predicted_key_inputs(args, file_hasher);
        // Whether this process holds the unit's discovery flight. Only the
        // holder may compile before keying: a peer that also found nothing
        // would compile the same unit a second time instead of waiting for
        // the entry the holder stores.
        let mut owns_flight = false;
        if prediction.is_err()
            && let Some(cache_dir) = &file_hasher.prediction_flight_dir
            && let Some(identity) = discovery_flight_identity(args, file_hasher)
        {
            let flight = crate::scheduler::join_discovery_flight(cache_dir, &identity);
            owns_flight = flight.lock.is_some();
            *file_hasher.discovery_flight.borrow_mut() = flight.lock;
            // The previous owner may have published while this process
            // waited; a flight taken at once had no owner to publish.
            if flight.waited {
                prediction = predicted_key_inputs(args, file_hasher);
            }
        }
        match prediction {
            Ok(dep_info) => {
                crate::phase_trace::decision("prediction", "validated");
                let mode =
                    parse_verify_predictions(env.var("KACHE_VERIFY_INPUT_PREDICTIONS").as_deref());
                // Verification is the exceptional path: it runs the pre-pass
                // anyway and uses ITS answer, so a disagreement is reported
                // rather than acted on.
                if verifies_prediction(mode, args, env.cwd(), |var| env.var_os(var)) {
                    crate::phase_trace::decision("prediction", "verify-sampled");
                    let discovered = dep_info_pre_pass(args)?;
                    if discovered
                        .as_ref()
                        .is_some_and(|discovered| closures_agree(&dep_info, discovered))
                    {
                        tracing::trace!("[key:{}] inputs=predicted(verified)", crate_name);
                    } else {
                        tracing::warn!(
                            "[key:{}] input prediction disagreed with the dep-info pass; \
                             using the pass. Please report this with the crate and its \
                             dependencies (kunobi-ninja/kache).",
                            crate_name
                        );
                        crate::opcounts::record_prediction_mismatch();
                    }
                    return Ok(discovered);
                }
                tracing::trace!("[key:{}] inputs=predicted", crate_name);
                let _ = LAST_KEY_USED_PREDICTION.try_with(|stash| stash.set(true));
                return Ok(Some(dep_info));
            }
            // A missing prediction only describes this checkout. Another
            // checkout may have stored a portable entry, so compile first
            // only when the store has never held this unit.
            Err(Rejection::NoRecord | Rejection::Disabled | Rejection::NotEligible)
                if owns_flight
                    && DEFER_DISCOVERY.with(std::cell::Cell::get)
                    && file_hasher.store_lacks_unit(
                        crate_name,
                        args.get_codegen_opt("metadata").unwrap_or(""),
                    ) =>
            {
                crate::phase_trace::decision("prediction", "deferred-new-crate");
                tracing::trace!("[key:{}] inputs=deferred(new crate)", crate_name);
                return Err(anyhow::Error::new(DeferredDiscovery));
            }
            Err(reason) => {
                crate::phase_trace::decision("prediction", reason.as_str());
                tracing::trace!("[key:{}] inputs=dep-info({})", crate_name, reason.as_str())
            }
        }
    }
    dep_info_pre_pass(args)
}

/// Spawn rustc to enumerate the closure. The slow, authoritative answer.
fn dep_info_pre_pass(args: &RustcArgs) -> Result<Option<DepInfo>> {
    let _trace = crate::phase_trace::phase("dep-info");
    args.source_file
        .as_ref()
        .map(|source| {
            run_dep_info_pass(
                &args.rustc,
                args.inner_rustc.as_deref(),
                source,
                &args.all_args,
                args.has_expanded_argfiles(),
            )
            .with_context(|| {
                format!(
                    "dep-info pre-pass failed for {} — refusing to cache from an \
                     incomplete input set",
                    source.display()
                )
            })
        })
        .transpose()
}

/// Compute the blake3 cache key for a rustc invocation.
///
/// The key captures everything that affects compilation output:
/// - rustc version (full verbose string)
/// - target triple
/// - crate name and type
/// - emit kinds (metadata vs link — distinguishes check from build)
/// - codegen options (opt-level, lto, codegen-units, panic, etc.)
/// - feature flags (sorted)
/// - source file hash
/// - dependency artifact hashes
/// - RUSTFLAGS and relevant env vars
/// - linker identity (for bin/dylib caching)
pub fn compute_cache_key(
    args: &RustcArgs,
    file_hasher: &FileHasher<'_>,
    path_normalizer: &PathNormalizer,
    env: &KeyEnv,
) -> Result<String> {
    let _trace = crate::phase_trace::phase("key");
    // Grouped: the main digest is identical to a plain hasher's; the group
    // tee powers `explain_miss` (kunobi-ninja/kache#131).
    let mut hasher = GroupedHasher::new("compiler");
    let crate_name = args.crate_name.as_deref().unwrap_or("unknown");

    // Clear the extern stash up front (#609). It is written near the end of
    // this function, so a computation that bails before the externs group —
    // or one that never reaches the event writer — would otherwise leave a
    // previous invocation's dependency digests to be picked up as if they
    // belonged to this compile.
    let _ = LAST_KEY_EXTERNS.try_with(|stash| *stash.borrow_mut() = None);
    let _ = LAST_KEY_NATIVE_ARCHIVES.try_with(|stash| *stash.borrow_mut() = None);
    // Same reasoning for the unit ids (#627); both are cleared and written
    // together so the walk can never pair one compile's digests with another's
    // identities.
    let _ = LAST_KEY_EXTERN_UNITS.try_with(|stash| *stash.borrow_mut() = None);
    let _ = LAST_KEY_UNIT_ID.try_with(|stash| *stash.borrow_mut() = args.unit_id());
    // And the discovered closure, for the same reason: a computation that
    // bails before the pre-pass would otherwise leave the previous compile's
    // closure to be recorded against this one's identity.
    let _ = LAST_KEY_DEP_INFO.try_with(|stash| *stash.borrow_mut() = None);
    let _ = LAST_KEY_TREE_DIGEST.try_with(|stash| *stash.borrow_mut() = None);
    let _ = LAST_KEY_USED_PREDICTION.try_with(|stash| stash.set(false));
    let _ = LAST_KEY_BAKES_OUT_DIR.try_with(|stash| stash.set(false));

    // The unit's OUT_DIR, read here once and passed to everything below that
    // needs it.
    let out_dir = env.var_os("OUT_DIR");

    // key version — bump CACHE_KEY_VERSION to invalidate all prior entries
    hasher.update(b"key_version:");
    hasher.update(CACHE_KEY_VERSION.to_string().as_bytes());
    hasher.update(b"\n");
    tracing::trace!("[key:{}] key_version={}", crate_name, CACHE_KEY_VERSION);

    // Distinguish configured from unconfigured clients of this same release.
    // Only the stable sentinel/target SET (represented by its deterministic
    // count) is folded — never the machine-local prefix spellings — so two
    // hosts that relocate corresponding configured roots still share keys.
    let configured_base_dirs = path_normalizer.configured_base_dir_count();
    if configured_base_dirs != 0 {
        fold_field(
            &mut hasher,
            b"configured_base_dirs.v1:",
            configured_base_dirs.to_string().as_bytes(),
        );
        tracing::trace!(
            "[key:{}] configured_base_dirs={}",
            crate_name,
            configured_base_dirs
        );
    }

    // rustc version
    let rustc_version = get_rustc_version(&args.rustc)?;
    hasher.update(b"rustc_version:");
    hasher.update(rustc_version.as_bytes());
    hasher.update(b"\n");
    tracing::trace!(
        "[key:{}] rustc_version={}",
        crate_name,
        rustc_version.lines().next().unwrap_or("?")
    );

    // Clippy: the driver's own version, its configuration file and the lint
    // arguments Cargo hands it through the environment all change what a
    // successful compile prints, and hits replay diagnostics.
    if args.is_clippy_chain() {
        let identity = clippy_identity(&args.rustc, env)?;
        fold_field(&mut hasher, b"clippy.v1:", identity.as_bytes());
        tracing::trace!(
            "[key:{}] clippy={}",
            crate_name,
            identity.lines().next().unwrap_or("?")
        );
    }

    // target triple
    let target = args
        .target
        .as_deref()
        .unwrap_or_else(|| host_target_triple());
    // `--target=` can be a path to a custom target JSON spec (Firefox /
    // embedded toolchains do this) — flag any absolute machine-local path
    // that lands here unsentinelized.
    check_for_path_leak(target, "target");
    hasher.update(b"target:");
    hasher.update(target.as_bytes());
    hasher.update(b"\n");
    tracing::trace!("[key:{}] target={}", crate_name, target);

    // A `--target` value can be a path to a custom target JSON spec
    // (Firefox / embedded toolchains). That spec encodes data-layout,
    // target-cpu/features, linker, panic strategy, code-model — all
    // codegen-affecting — yet the dep-info pass never lists it, so only
    // the path string above would distinguish two builds. Hash the file
    // CONTENTS too, so editing the spec in place (or a different spec at
    // the same path on another machine) diverges the key. Built-in
    // triples aren't files, so they're unaffected.
    let target_path = Path::new(target);
    if target_path.is_file() {
        match hash_file(target_path) {
            Ok(spec_hash) => {
                hasher.update(b"target_spec:");
                hasher.update(spec_hash.as_bytes());
                hasher.update(b"\n");
                tracing::trace!("[key:{}] target_spec={}", crate_name, &spec_hash[..16]);
            }
            Err(e) => {
                tracing::warn!(
                    "[key:{}] failed to hash target spec {}: {}",
                    crate_name,
                    target,
                    e
                );
            }
        }
    }

    // crate identity
    hasher.set_group("crate");
    if let Some(name) = &args.crate_name {
        hasher.update(b"crate_name:");
        hasher.update(name.as_bytes());
        hasher.update(b"\n");
        tracing::trace!("[key:{}] crate_name={}", crate_name, name);
    }

    // crate types
    for ct in &args.crate_types {
        hasher.update(b"crate_type:");
        hasher.update(ct.as_bytes());
        hasher.update(b"\n");
        tracing::trace!("[key:{}] crate_type={}", crate_name, ct);
    }

    // edition
    if let Some(edition) = &args.edition {
        hasher.update(b"edition:");
        hasher.update(edition.as_bytes());
        hasher.update(b"\n");
        tracing::trace!("[key:{}] edition={}", crate_name, edition);
    }

    hasher.set_group("args");
    // emit kinds (sorted for determinism)
    //
    // `cargo check` runs `rustc --emit=metadata` (produces `.rmeta`);
    // `cargo build` runs `--emit=link` (produces `.rlib`). With every
    // other hashed input identical the two invocations would collide,
    // letting a check's metadata-only entry be served to a build that
    // needs the `.rlib` — a miscache. Hashing `emit` keeps the two
    // keyed apart by design rather than by cargo's incidental per-unit
    // `-C metadata` differing between the two.
    let mut emit: Vec<&String> = args.emit.iter().collect();
    emit.sort();
    for kind in &emit {
        hasher.update(b"emit:");
        hasher.update(kind.as_bytes());
        hasher.update(b"\n");
        tracing::trace!("[key:{}] emit:{}", crate_name, kind);
    }

    // Codegen options grouped by name for determinism. This is a stable sort,
    // so repeated last-wins options retain argv order. Rustc's `-O` and `-g`
    // shorthands are normalized into this same list during parsing.
    let mut codegen_opts: Vec<_> = args
        .codegen_opts
        .iter()
        .filter(|(k, _)| {
            // Skip incremental as it's path-dependent.
            // Skip linker because its value is a machine-local absolute
            // path on toolchain-bootstrapping builds (Firefox/mozbuild
            // sets `-Clinker=/abs/path/to/clang++`). The linker's
            // semantic identity is captured separately via
            // `get_linker_identity` (its `--version` output) which is
            // path-independent.
            k != "incremental" && k != "linker"
        })
        .collect();
    codegen_opts.sort_by_key(|(k, _)| k.as_str());
    for (key, value) in &codegen_opts {
        fold_field(&mut hasher, b"codegen_key:", key.as_bytes());
        if let Some(v) = value {
            // `-Clink-arg=`, `-Clink-args=…`, `-Cprofile-use=…`, etc. can
            // carry absolute paths. None of these go through
            // PathNormalizer (they're rustc-controlled flags, not env);
            // flag any leaked path so the field is identifiable.
            check_for_path_leak(v, &format!("codegen:{key}"));
            fold_field(&mut hasher, b"codegen_val:", v.as_bytes());
            tracing::trace!("[key:{}] codegen:{}={}", crate_name, key, v);
        } else {
            tracing::trace!("[key:{}] codegen:{}", crate_name, key);
        }
    }

    // feature flags (already sorted in args parsing)
    for feat in &args.features {
        hasher.update(b"feature:");
        hasher.update(feat.as_bytes());
        hasher.update(b"\n");
        tracing::trace!("[key:{}] feature:{}", crate_name, feat);
    }

    // cfg flags (non-feature, sorted)
    let mut cfgs: Vec<_> = args
        .cfgs
        .iter()
        .filter(|c| !c.starts_with("feature="))
        .collect();
    cfgs.sort();
    for cfg in &cfgs {
        // Build-script `cargo:rustc-cfg=…` lines reach us as raw strings;
        // mozbuild / embedded crates sometimes emit cfgs that embed
        // generated paths. None go through PathNormalizer — flag leaks.
        check_for_path_leak(cfg, "cfg");
        fold_field(&mut hasher, b"cfg:", cfg.as_bytes());
        tracing::trace!("[key:{}] cfg:{}", crate_name, cfg);
    }

    let dep_info = resolve_key_inputs(args, file_hasher, crate_name, env)?;
    // Keep the closure available to the wrapper, which records it as a
    // prediction only once the invocation it belongs to has succeeded. The
    // clone is one allocation per closure file against a whole rustc spawn.
    let _ = LAST_KEY_DEP_INFO.try_with(|stash| *stash.borrow_mut() = dep_info.clone());

    let mut externs: Vec<_> = args.externs.iter().filter(|e| e.path.is_some()).collect();
    externs.sort_by_key(|e| &e.name);

    let mut hash_paths = Vec::new();
    if let Some(dep_info) = &dep_info {
        hash_paths.extend(dep_info.source_files.iter().map(|p| p.as_path()));
    }
    hash_paths.extend(externs.iter().filter_map(|ext| ext.path.as_deref()));
    file_hasher.prefetch(&hash_paths);

    // ── Group A: source files + env deps (from dep-info pre-pass) ──
    hasher.set_group("sources");
    if let Some(dep_info) = &dep_info {
        // A source is identified by its stable normalized path and bytes.
        // Hashing only a sorted content multiset lets swapping modules/assets
        // preserve the key while changing semantics (#760). Matched paths use
        // stable sentinels for relocation (#201); unmatched paths stay local
        // rather than risk a false hit.
        let mut hashed: Vec<(Vec<u8>, String)> = Vec::with_capacity(dep_info.source_files.len());
        for file in &dep_info.source_files {
            let file_hash = file_hasher
                .hash(file)
                .with_context(|| format!("hashing source identity {}", file.display()))?;
            let normalized_path = source_path_identity(file, path_normalizer)?;
            hashed.push((normalized_path, file_hash));
        }
        hashed.sort();
        for (normalized_path, file_hash) in &hashed {
            fold_field(&mut hasher, b"source_path:", normalized_path);
            fold_field(&mut hasher, b"source_hash:", file_hash.as_bytes());
            tracing::trace!(
                "[key:{}] source:{}={}",
                crate_name,
                String::from_utf8_lossy(normalized_path),
                &file_hash[..16]
            );
        }

        hasher.set_group("env_deps");
        let aliased_out_dir = crate::out_dir_alias::active_alias();
        let env_dep_paths = EnvDepPaths::new(out_dir.as_deref(), &dep_info.source_files);
        let mut bakes_out_dir = false;
        for (var, val) in &dep_info.env_deps {
            let value = EnvDepValue::new(val);
            let normalized_env_dep = normalize_env_dep_value_with_hasher(
                crate_name,
                var,
                &value,
                &env_dep_paths,
                file_hasher,
                path_normalizer,
                aliased_out_dir,
            );
            bakes_out_dir |= env_dep_bakes_out_dir(var, normalized_env_dep.decision, || {
                env_dep_paths.value_is_under_out_dir(&value)
            });
            fold_field(&mut hasher, b"env_dep_var:", var.as_bytes());
            fold_field(
                &mut hasher,
                b"env_dep_val:",
                normalized_env_dep.value.as_bytes(),
            );
            tracing::trace!(
                "[key:{}] env_dep:{}={} ({})",
                crate_name,
                var,
                normalized_env_dep.value,
                normalized_env_dep.decision.as_str()
            );
        }
        let _ = LAST_KEY_BAKES_OUT_DIR.try_with(|stash| stash.set(bakes_out_dir));
    }

    // ── Group B: extern crate artifacts ──
    hasher.set_group("externs");
    // Per-dependency digests teed off the same hashes folded below, for
    // `why-miss`'s extern-chain walk (#609). Recording happens unconditionally
    // — it is one map insert per extern, no extra I/O, since the hash is
    // already in hand — while the decision to PERSIST it stays with the
    // wrapper's `explain_miss` gate.
    let mut extern_digests = std::collections::BTreeMap::new();
    // Producing-unit ids, from the artifact filename rather than the extern
    // name, so a renamed or duplicated dependency still joins to its producer
    // (kunobi-ninja/kache#627).
    let mut extern_units = std::collections::BTreeMap::new();
    for ext in &externs {
        if let Some(path) = &ext.path {
            if let Some(unit) = crate::args::unit_id_from_artifact_path(path) {
                extern_units.insert(ext.name.clone(), unit);
            }
            match file_hasher.hash(path) {
                Ok(dep_hash) => {
                    hasher.update(b"extern:");
                    hasher.update(ext.name.as_bytes());
                    hasher.update(b"=");
                    hasher.update(dep_hash.as_bytes());
                    hasher.update(b"\n");
                    extern_digests.insert(
                        ext.name.clone(),
                        dep_hash
                            .get(..KEY_FIELD_HEX)
                            .unwrap_or(dep_hash.as_str())
                            .to_string(),
                    );
                    tracing::trace!(
                        "[key:{}] extern:{}={}",
                        crate_name,
                        ext.name,
                        &dep_hash[..16]
                    );
                }
                Err(_) => {
                    // Sysroot crate (std, core, etc.) — identity is determined by
                    // rustc version + name, both already in the hash. Use a sentinel
                    // instead of the absolute path to enable cross-machine sharing.
                    hasher.update(b"extern_unreadable:");
                    hasher.update(ext.name.as_bytes());
                    hasher.update(b"\n");
                    extern_digests.insert(ext.name.clone(), EXTERN_UNREADABLE.to_string());
                    tracing::trace!("[key:{}] extern_unreadable:{}", crate_name, ext.name);
                }
            }
        }
    }
    let _ = LAST_KEY_EXTERNS.try_with(|stash| *stash.borrow_mut() = Some(extern_digests));
    let _ = LAST_KEY_EXTERN_UNITS.try_with(|stash| *stash.borrow_mut() = Some(extern_units));

    // RUSTFLAGS — normalize via PathNormalizer (canonical-prefix
    // sentinel substitution; supersedes the older CWD-only
    // `normalize_flags` for cache-key purposes), then collapse runs of
    // whitespace into single spaces. Cargo / mach assemble the env
    // value with cosmetically-varying whitespace (multiple spaces
    // between flags, trailing spaces) across compile profiles, which
    // produced different hash inputs for semantically-identical flag
    // sets. Order is preserved — `-Cfoo=a -Cfoo=b` differs from
    // `-Cfoo=b -Cfoo=a` because later flags override earlier ones in
    // rustc's parser.
    hasher.set_group("args");
    if let Some(rustflags) = env.var("RUSTFLAGS") {
        // Scrub the per-checkout `from` of any `--remap-path-prefix` BEFORE
        // sentinel normalization, so a checkout path the PathNormalizer would
        // only partially rewrite collapses to a single sentinel and clones
        // converge.
        let scrubbed = scrub_remap_from_prefixes(rustflags.split_whitespace()).join(" ");
        let normalized = normalize_rustflags(&path_normalizer.normalize(&scrubbed));
        hasher.update(b"RUSTFLAGS:");
        hasher.update(normalized.as_bytes());
        hasher.update(b"\n");
        tracing::trace!("[key:{}] RUSTFLAGS={}", crate_name, normalized);
    }

    // CARGO_ENCODED_RUSTFLAGS (cargo's way of passing flags)
    if let Some(flags) = env.var("CARGO_ENCODED_RUSTFLAGS") {
        // Same scrub as RUSTFLAGS; the encoded form is `\x1f`-separated, so
        // tokenize on that (a space-form `--remap-path-prefix` is its own unit
        // with the value in the next unit).
        let scrubbed = scrub_remap_from_prefixes(flags.split('\x1f')).join("\x1f");
        let normalized = path_normalizer.normalize(&scrubbed);
        hasher.update(b"CARGO_ENCODED_RUSTFLAGS:");
        hasher.update(normalized.as_bytes());
        hasher.update(b"\n");
        tracing::trace!(
            "[key:{}] CARGO_ENCODED_RUSTFLAGS={}",
            crate_name,
            normalized
        );
    }

    // Direct argv remaps are codegen inputs too: they alter `file!()`, panic
    // locations, and debug paths. Normalize known machine-local prefixes on
    // FROM, but retain unrelated FROM values because matching vs non-matching
    // mappings are semantically different. Keep TO verbatim because it is
    // embedded in the artifact, and preserve order because overlapping remaps
    // are order-sensitive.
    for value in &args.remap_path_prefixes {
        let normalized = normalize_direct_remap_value(value, path_normalizer);
        fold_field(
            &mut hasher,
            b"argv_remap_path_prefix.v1:",
            normalized.as_bytes(),
        );
        tracing::trace!(
            "[key:{}] argv --remap-path-prefix={}",
            crate_name,
            normalized
        );
    }

    // RUSTC_BOOTSTRAP changes what rustc accepts — nightly-only
    // `#![feature(...)]` and unstable `-Z` flags on a stable/beta toolchain —
    // so byte-identical source can compile differently (or succeed vs fail)
    // purely because this var is set. It's consumed by the driver and never
    // surfaces as a source env-dep, so the `-Z`/`#![feature]` bytes are keyed
    // but the var's presence was not. Fold it in only when set, so the key is
    // byte-identical for the common case (var unset): no CACHE_KEY_VERSION bump
    // and no cache invalidation for existing users.
    if let Some(bootstrap) = env.var("RUSTC_BOOTSTRAP")
        && !bootstrap.is_empty()
    {
        hasher.update(b"RUSTC_BOOTSTRAP:");
        hasher.update(bootstrap.as_bytes());
        hasher.update(b"\n");
        tracing::trace!("[key:{}] RUSTC_BOOTSTRAP={}", crate_name, bootstrap);
    }

    // Sysroot override (`--sysroot`). Selects which std/core/proc-macro
    // libs rustc links against, so two builds of the same rustc binary
    // with different sysroots (custom-built std, `-Zbuild-std`) must not
    // collide. Normalized so a standard rustup layout still shares
    // across machines while a genuinely different path diverges.
    hasher.set_group("link");
    if let Some(sysroot) = &args.sysroot {
        let normalized = path_normalizer.normalize(sysroot.to_string_lossy());
        hasher.update(b"sysroot:");
        hasher.update(normalized.as_bytes());
        hasher.update(b"\n");
        tracing::trace!("[key:{}] sysroot={}", crate_name, normalized);
    }

    // Native link search paths (`-L [KIND=]PATH`). cargo's own
    // `dependency=`/`crate=` entries are redundant with the
    // content-hashed `--extern` rlibs and are machine-local, so they're
    // skipped; build-script-supplied `native=`/`framework=`/bare paths
    // DO change a linked artifact and are kept (path-normalized for
    // cross-machine stability). Order is preserved — link order is
    // significant, and a stable argv from cargo keeps the key stable.
    const KNOWN_L_KINDS: [&str; 5] = ["dependency", "crate", "native", "framework", "all"];
    // Real (un-normalized) build-script search dirs, kept for resolving `-l`
    // static libs to a content hash below (#421). `native=`/bare entries are the
    // OUT_DIR dirs a `cc`/`cmake` build script emits; `dependency=`/`crate=` are
    // cargo's own rlib dirs (redundant with content-hashed externs).
    let mut native_search_dirs: Vec<PathBuf> = Vec::new();
    // `framework=` dirs, the only ones rustc hands the linker as `-F`.
    let mut framework_search_dirs: Vec<PathBuf> = Vec::new();
    for spec in &args.link_search {
        // Only split on a *recognized* kind so a path containing '='
        // isn't mis-parsed (matches rustc's own `-L` parsing).
        let (kind, path) = match spec.split_once('=') {
            Some((k, p)) if KNOWN_L_KINDS.contains(&k) => (Some(k), p),
            _ => (None, spec.as_str()),
        };
        if matches!(kind, Some("dependency") | Some("crate")) {
            continue;
        }
        // `all=` and bare/`native=` dirs all search native libs (rustc's `-L`
        // default kind is `all`); a `static=` lib can resolve in any of them.
        if matches!(kind, None | Some("native") | Some("all")) {
            native_search_dirs.push(PathBuf::from(path));
        }
        if kind == Some("framework") {
            framework_search_dirs.push(PathBuf::from(path));
        }
        let normalized = path_normalizer.normalize(path);
        hasher.update(b"link_search:");
        if let Some(k) = kind {
            hasher.update(k.as_bytes());
            hasher.update(b"=");
        }
        hasher.update(normalized.as_bytes());
        hasher.update(b"\n");
        tracing::trace!("[key:{}] link_search:{}", crate_name, normalized);
    }

    // Native libraries to link (`-l`). The name alone (machine-independent;
    // a build script repointing `-l` to a different lib is caught here) is
    // hashed raw, order preserved (static link order is significant).
    //
    // The name does NOT capture a `static=` lib whose *content* changed in
    // place — same `-l` name, same `-L` path, different bytes. rustc bundles a
    // `static=` archive INTO the produced rlib/binary, so its bytes are part of
    // the output: an unchanged key there is a stale-artifact false hit (#421).
    // [`fold_native_link_inputs`] hashes the archives a unit's output carries:
    // its `static` specs, the archives a Unix link picks for its other `-l`
    // specs, files named in its link arguments, and every archive in its `-L`
    // dirs outside Cargo's packages and the system library dirs. A `:RENAME`
    // or unknown modifier is uncacheable.
    // Direct command-line native Windows MSVC libraries are handled separately
    // below: their import-library bytes affect the executable and are hashed
    // as part of the host link identity, which refuses files handed to LINK
    // through `-C link-arg` (`.res`, `.def`, `.obj`, `/DEF:`, ...).
    // Linker order/section-order/map files and opaque response files require
    // side-input/output handling beyond the archive key. Fail closed instead
    // of caching an invocation whose auxiliary behavior cannot be reproduced.
    if native_linker_side_files_are_unmodeled(args) {
        anyhow::bail!("native linker order/map/response side files are not cacheable");
    }
    // Native MSVC links resolve their libraries and link-argument files in
    // the MSVC identity below, so the Unix rules here stay off for them.
    let native_windows_msvc = is_native_windows_msvc_link(
        args,
        &rustc_version,
        cfg!(target_os = "windows"),
        get_rustc_version,
    )?;
    // Only a link searches for libraries, so only a link reads the host.
    let link_rustc_version = if args.invokes_linker() {
        Some(rustc_version_for_native_link(
            args,
            &rustc_version,
            get_rustc_version,
        )?)
    } else {
        None
    };
    let rustc_host = link_rustc_version.as_deref().and_then(rustc_host_triple);
    let target = link_target(args.target.as_deref(), rustc_host);
    let unit_out_dir = out_dir.map(PathBuf::from);
    let native_archives = fold_native_link_inputs(
        &mut hasher,
        args,
        &NativeSearchDirs {
            native: &native_search_dirs,
            framework: &framework_search_dirs,
        },
        &NativeLinkContext {
            native_windows_msvc,
            target,
            default_dirs: default_library_dirs(
                target,
                rustc_host,
                env.var_os("LIBRARY_PATH").as_deref(),
            ),
            out_dir: unit_out_dir.as_deref(),
            build_tree: build_tree_roots(args),
            system_dirs: system_library_dirs(),
            path_normalizer,
        },
        file_hasher,
    )?;
    // An rlib that bundles an archive the key did not hash is refused at
    // store time (see the wrapper's bundle audit). Entries stored by clients
    // without that audit must not serve this one.
    if needs_native_bundle_audit(args, &native_archives.dirs) {
        hasher.set_group("native_bundle_audit");
        fold_field(&mut hasher, b"native_bundle_audit.v1", b"");
        tracing::trace!("[key:{}] native_bundle_audit", crate_name);
    }
    let _ = LAST_KEY_NATIVE_ARCHIVES.try_with(|stash| *stash.borrow_mut() = Some(native_archives));

    // Unstable `-Z` flags arriving on argv outside RUSTFLAGS. Can change
    // codegen (`-Zsanitizer`, `-Zshare-generics`, …); hashed raw.
    hasher.set_group("args");
    let backend_dylib = args.codegen_backend_dylib();
    for z in &args.unstable_flags {
        // A backend dylib is keyed by its content, not its path: the wrapper
        // only reaches here for one when the user trusts it, and rebuilding
        // the backend in place must change the key while another checkout's
        // identical backend must not.
        if let Some(path) = z
            .strip_prefix("codegen-backend=")
            .filter(|path| Some(*path) == backend_dylib)
        {
            let content = file_hasher
                .hash(Path::new(path))
                .with_context(|| format!("hashing codegen backend {path}"))?;
            fold_field(
                &mut hasher,
                b"codegen_backend_content.v1:",
                content.as_bytes(),
            );
            tracing::trace!("[key:{}] codegen_backend_content:{}", crate_name, content);
            continue;
        }
        hasher.update(b"unstable:");
        hasher.update(z.as_bytes());
        hasher.update(b"\n");
        tracing::trace!("[key:{}] unstable:{}", crate_name, z);
    }

    // Parallel frontend compilation changes the compiler execution mode and
    // can affect emitted artifacts. Preserve occurrence order because rustc
    // applies repeated values last-wins.
    for jobs in &args.frontend_jobs {
        fold_field(&mut hasher, b"frontend_jobs.v1:", jobs.as_bytes());
        tracing::trace!("[key:{}] frontend_jobs:{}", crate_name, jobs);
    }

    // Residual argv tokens (kunobi-ninja/kache#324): flags kache does not model
    // explicitly still reach rustc and can affect codegen (for example a
    // future flag), yet were previously invisible to the key — the `_ => {}`
    // catch-all in `args.rs` dropped them. Fold the NORMALIZED, sorted residual
    // under a versioned tag so an unmodeled codegen-affecting flag changes the
    // key. Diagnostics / lint / query / already-keyed path flags are stripped
    // during arg parsing, so they never reach here. Normalize via PathNormalizer
    // (a residual token can embed a machine-local path) and sort so argv order /
    // host paths don't perturb the key. Folded only when non-empty, so the
    // common case (no residual) is byte-identical and needs no
    // CACHE_KEY_VERSION bump (same precedent as RUSTC_BOOTSTRAP above).
    if !args.residual_args.is_empty() {
        let mut residual: Vec<String> = args
            .residual_args
            .iter()
            .map(|tok| path_normalizer.normalize(tok))
            .collect();
        residual.sort();
        for tok in &residual {
            check_for_path_leak(tok, "residual_arg");
            fold_field(&mut hasher, b"residual_args.v1:", tok.as_bytes());
            tracing::trace!("[key:{}] residual_arg:{}", crate_name, tok);
        }
        // Surface unmodeled ("exotic") rustc flags so it is visible which ones
        // appear in real builds (kunobi-ninja/kache#183). They are already folded
        // into the key above, so they cannot cause a false hit; this is a prompt
        // to model them explicitly for precise keying (or to report them). Raw
        // tokens (not the path-normalized form) so they match what was passed.
        // Fires per cacheable invocation, which is rare: direct-argv unmodeled
        // flags only, since -C/-Z and RUSTFLAGS are already modeled.
        let mut raw: Vec<&str> = args.residual_args.iter().map(String::as_str).collect();
        raw.sort_unstable();
        raw.dedup();
        tracing::warn!(
            "[key:{}] {} unmodeled rustc flag(s) folded into the cache key \
             (kache does not model these; keyed defensively so they cannot cause \
             a false hit, but model them for precise keying): {}",
            crate_name,
            raw.len(),
            raw.join(" "),
        );
    }

    // Outcome-affecting lint configuration (-A/-W/-D/-F, their long forms,
    // --force-warn, --cap-lints, and --check-cfg): two invocations differing
    // only here can disagree about whether compilation succeeded while
    // producing identical object bytes on success. In particular, allow/warn
    // levels interact with deny groups, and --check-cfg feeds unexpected_cfgs.
    // A hit replays success, which would flip a build that `-D warnings`
    // should have failed to green. The flags are captured during parsing
    // (see `OUTCOME_AFFECTING_VALUE_FLAGS` in args.rs) and folded here. v28
    // invalidates prior entries because v27 was released with these inputs
    // missing and old clients can still populate that shared schema.
    //
    // Folded in ARGV ORDER, deliberately unsorted. The captured vector is a
    // flat token stream (`-D`, `warnings`, `--force-warn`, `deprecated`), so
    // sorting would both break the flag↔value pairing — `-D unsafe_code -F
    // warnings` and `-F unsafe_code -D warnings` share a sorted multiset but
    // not an outcome — and erase order, which rustc itself treats as
    // meaningful (the last level named for a lint wins). A stable argv order
    // for a given build config means keeping it costs no hits.
    if !args.outcome_lint_flags.is_empty() {
        hasher.set_group("outcome_lints");
        for tok in &args.outcome_lint_flags {
            // Fold raw: check-cfg accepts arbitrary string values, including
            // path-looking text. Path normalization could collapse two
            // distinct accepted-value sets and reopen a false hit.
            // The leak check is observability-only; it never rewrites the key.
            check_for_path_leak(tok, "outcome_lint");
            fold_field(&mut hasher, b"outcome_lint.v1:", tok.as_bytes());
            tracing::trace!("[key:{}] outcome_lint:{}", crate_name, tok);
        }
    }

    // Relevant CARGO_CFG_* env vars (sorted for determinism —
    // environment iteration order is platform-defined and not stable)
    hasher.set_group("env_cfg");
    let cargo_cfgs = cargo_cfg_pairs(env.cargo_cfgs());
    tracing::trace!("[key:{}] cargo_cfg_count={}", crate_name, cargo_cfgs.len());
    for (key, value) in &cargo_cfgs {
        // Cargo derives CARGO_CFG_* from `--cfg` flags. Build scripts (and
        // mozbuild specifically) emit cfgs that can embed absolute paths;
        // those land here uncensored. Flag leaks so the offending var
        // name is visible in the warn. The lossy forms are diagnostic
        // only; the hash folds the lossless bytes.
        let key_lossy = key.to_string_lossy();
        check_for_path_leak(&value.to_string_lossy(), &format!("cargo_cfg:{key_lossy}"));
        hasher.update(&env_text_key_bytes(key));
        hasher.update(b"=");
        hasher.update(&env_text_key_bytes(value));
        hasher.update(b"\n");
    }

    // Linker identity for bin/dylib targets
    hasher.set_group("link");
    fold_generic_linker_identity(&mut hasher, args, native_windows_msvc, get_linker_identity);

    // A native Linux linked artifact also depends on the host libc ABI. The
    // rustc host triple does not include the libc version, so two machines with
    // the same rustc/linker versions could otherwise share an incompatible
    // bin/dylib through a remote cache (kunobi-ninja/kache#127). Cross targets
    // deliberately skip this HOST signal: their libc comes from the target
    // sysroot/toolchain, and poisoning those keys with the build host would
    // only destroy valid cross-machine hits without identifying that sysroot.
    // Probe failure is an error, making the wrapper pass through instead of
    // risking a shared-cache false hit.
    fold_native_host_libc_signature(
        &mut hasher,
        args,
        &rustc_version,
        cfg!(target_os = "linux"),
        probe_linux_libc_signature,
    )?;

    // Stronger than the version string above: hash the CRT/startup objects and
    // libc the driver actually places (Linux), and the SDK identity (macOS).
    // Two hosts with the same `cc --version` / libc version banner and
    // different object bytes then miss instead of sharing. If none of the
    // essentials resolve, fail closed — passthrough rather than a key that
    // claims to have pinned a runtime neither host identified. v29.
    fold_native_link_runtime_identity(
        &mut hasher,
        args,
        &rustc_version,
        cfg!(target_os = "linux"),
        cfg!(target_os = "macos"),
        |driver| {
            // Placements come from the memo while the searched directories
            // are unchanged; content hashes are still reused only with an
            // unchanged file fingerprint, through the same guards as other
            // key inputs.
            crate::native_link_key::probe_linux_crt_objects_memoized(
                &crate::config::probe_memo_dir(),
                driver,
                |path| file_hasher.hash(path),
            )
        },
        |sdkroot| match crate::native_link_key::sdk_identity_for(sdkroot)? {
            Some(identity) => Ok(identity),
            None => anyhow::bail!("the macOS SDK could not be identified"),
        },
        env,
    )?;

    // Native Windows MSVC links depend on the selected COFF linker/compiler,
    // the architecture-specific MSVC/SDK/UCRT environment, and the runtime
    // import/static libraries. The probe is strictly host-native: metadata,
    // cross-target links, and windows-gnu links must remain portable and must
    // not execute or inspect host tools. A failed probe bubbles out so the
    // wrapper passes through rather than sharing an unidentified executable;
    // so does any `-C link-arg` that names an input file the identity does
    // not hash (see `windows_native_link_search_dirs`).
    fold_native_windows_msvc_identity(
        &mut hasher,
        args,
        &rustc_version,
        cfg!(target_os = "windows"),
        |linker, architecture| {
            let search_dirs = windows_native_link_search_dirs(args)?;
            crate::native_link_key::probe_windows_msvc_identity_with_library_dirs(
                linker,
                architecture,
                &search_dirs.rustc,
                &search_dirs.linker,
                &args.link_libs,
                |path| file_hasher.hash_static_lib(path),
            )
            .map(|identity| identity.encode())
        },
    )?;

    // Path remapping status: kache injects multi-prefix
    // `--remap-path-prefix` flags (one per PathNormalizer rule) for
    // reproducible builds across machines — but skips them under
    // coverage instrumentation (tarpaulin / llvm-cov need original
    // paths in profraw to map coverage back to source) or when the user
    // opts out via `KACHE_RUSTC_PATH_NORMALIZE=0` (local profiler /
    // debugger source lookup needs real paths, kunobi-ninja/kache#480).
    // Since this produces different binaries, the key must reflect the
    // choice — the opt-out namespace hashes `remap:none`, so a build with
    // remapping disabled never collides with a default remapped artifact.
    // This uses the SAME `args.skip_path_remap()` decision (a parse-time
    // snapshot) that `RustcCompiler::execute` uses to gate injection, so the
    // key can never claim one remap state while the binary was built with the
    // other, breaking the byte-for-byte cache invariant.
    //
    // We hash the SENTINEL set (not the prefix paths) so the key
    // stays portable across machines — different hosts have
    // different `$HOME` / `$CARGO_HOME` prefixes but the same
    // sentinel categories, so the key is identical.
    hasher.set_group("remap");
    let remap = if args.skip_path_remap() {
        hasher.update(b"remap:none\n");
        // Whenever remap injection is skipped — the `KACHE_RUSTC_PATH_NORMALIZE=0`
        // opt-out OR a coverage build (llvm-cov / tarpaulin need real paths in
        // the profraw) — rustc bakes real machine-local paths into DWARF instead
        // of sentinels. Those paths are NOT otherwise in the key (path-bearing
        // inputs are still normalized and source is hashed by content), so
        // without this fold two different checkouts compute the same `remap:none`
        // key and a shared cache would serve one checkout's real-path artifact to
        // another (kunobi-ninja/kache#480 for the opt-out; the same hazard for
        // coverage). Fold the raw local prefixes that would have been remapped so
        // the key is path-local, matching the cc `KACHE_CC_PATH_NORMALIZE=0`
        // "keys become path-literal" contract.
        fold_unremapped_path_identity(&mut hasher, args, path_normalizer, env);
        "none".to_string()
    } else {
        hasher.update(b"remap:multi-prefix\n");
        // Only the remap on/off choice is keyed (above) — it is the
        // binary-affecting bit: coverage builds skip remapping because
        // tarpaulin / llvm-cov need original paths in the profraw. The
        // SPECIFIC sentinel set is deliberately NOT folded into the key.
        // Its membership depends on which machine-local dirs exist relative
        // to the build ($TMPDIR, %PROGRAMFILES%, $CARGO_HOME, …), so it
        // varied across machines and — when the build tree sat INSIDE one of
        // those dirs — across relocations: an out-of-tree build under the
        // system tempdir dropped the <TMPDIR> rule via the prefix de-dupe,
        // diverging the key and missing on relocate (kunobi-ninja/kache#399).
        // The set is also redundant: any path that actually reaches the
        // compile is already keyed through its normalized env-dep / source /
        // link field (rewritten to these same sentinels), and
        // `--remap-path-prefix` only neutralizes rustc-emitted file paths,
        // never `env!` runtime values (those are keyed separately). Rendered
        // here for the diagnostic trace only.
        let remap_args = path_normalizer.remap_args();
        let mut targets: Vec<String> = remap_args
            .iter()
            .filter_map(|a| a.split('=').next_back().map(str::to_string))
            .collect();
        targets.sort();
        targets.dedup();
        format!("multi-prefix({})", targets.join(","))
    };
    tracing::trace!("[key:{}] remap={}", crate_name, remap);

    let (hash, fields) = hasher.finalize_with_fields();
    let _ = LAST_KEY_FIELDS.try_with(|stash| *stash.borrow_mut() = Some(fields));
    let key = hash.to_hex().to_string();
    tracing::trace!("[key:{}] final={}", crate_name, &key[..16]);
    complete_key(env, key)
}

/// `key`, unless key computation read a variable its snapshot does not hold.
/// That input would have folded as unset whatever its value, so two builds
/// that differ in it could share the key; the invocation runs uncached.
fn complete_key(env: &KeyEnv, key: String) -> Result<String> {
    anyhow::ensure!(
        !env.read_undeclared(),
        "key computation read an environment variable missing from KEY_ENV_VARS"
    );
    Ok(key)
}

/// Fold the raw, un-normalized machine-local path prefixes into the key so any
/// unremapped (`remap:none`) build's key is path-local — both the
/// `KACHE_RUSTC_PATH_NORMALIZE=0` opt-out and coverage builds.
///
/// With remapping disabled rustc bakes real paths into DWARF (`comp_dir` = the
/// working directory; `decl_file`s under the workspace / `$CARGO_TARGET_DIR` /
/// `$CARGO_HOME` / `$RUSTUP_HOME` / `$HOME` / the tempdir / a build-script
/// `OUT_DIR`). Without this fold the rest of `compute_cache_key` normalizes
/// those path inputs to sentinels and hashes source by normalized path/content,
/// `remap:none` key path-independent and letting a shared cache hand one
/// checkout's real-path artifact to another (kunobi-ninja/kache#480 for the
/// opt-out; the same hazard for coverage).
///
/// The discriminator set is the normalizer's OWN [`PathNormalizer::raw_prefixes`]
/// — precisely the prefixes it would have remapped, so the key diverges whenever
/// the baked paths would, and the fold stays complete as normalizer rules evolve
/// (`<TARGET>`, `<BASE_DIR>`, the Windows roots, path-only env vars, …) rather
/// than tracking a hand-maintained env subset. cwd and the crate source path are
/// folded explicitly too: cargo passes a *relative* crate source, so `comp_dir`
/// (the cwd) is the load-bearing per-checkout discriminator, and this keeps the
/// fold meaningful even under a normalizer with no rules (tests / degraded env).
/// A path baked into DWARF that lies OUTSIDE every prefix is not normalized in
/// the key either, so it already reaches the key raw via its dep-info field — no
/// separate handling needed here.
fn fold_unremapped_path_identity<H: KeyFold>(
    hasher: &mut H,
    args: &RustcArgs,
    path_normalizer: &PathNormalizer,
    env: &KeyEnv,
) {
    hasher.update(b"unremapped_path_identity:v1\n");

    if let Some(cwd) = env.cwd() {
        fold_field(
            hasher,
            b"unremapped:cwd:",
            cwd.as_os_str().as_encoded_bytes(),
        );
    }
    if let Some(source) = &args.source_file {
        fold_field(
            hasher,
            b"unremapped:source:",
            source.as_os_str().as_encoded_bytes(),
        );
    }
    // Sort so the fold is order-stable regardless of rule-construction order.
    let mut prefixes: Vec<&str> = path_normalizer.raw_prefixes().collect();
    prefixes.sort_unstable();
    prefixes.dedup();
    for prefix in prefixes {
        fold_field(hasher, b"unremapped:prefix:", prefix.as_bytes());
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EnvDepNormalizationDecision {
    Unchanged,
    NormalizedPathOnly,
    /// Kept absolute: the var is not OUT_DIR, not allowlisted, and its value
    /// is not under OUT_DIR.
    KeptAbsoluteNotPathOnly,
    /// Kept absolute: CARGO_MANIFEST_DIR, which no allowlist entry can make
    /// path-only.
    KeptAbsoluteManifestDir,
    /// Kept absolute: dep-info lists no include under the value, or no Rust
    /// source shows the var inside an include argument (for example when the
    /// include comes from another crate's macro).
    KeptAbsoluteNoIncludeProof,
    /// Kept absolute: a source uses the var outside an include argument, or
    /// has an env macro whose var name the scanner cannot read.
    KeptAbsoluteRuntimeUse,
    /// Kept absolute: a source could not be read, or changed during the scan.
    KeptAbsoluteScanError,
    /// Normalized because the var (optionally crate-scoped) is in the
    /// user-asserted force list, bypassing the source scans.
    ForcedPathOnly,
    /// Kept raw: the value is at or under the shared read-only OUT_DIR this
    /// unit compiles with (see `out_dir_alias`), the same string in every
    /// checkout.
    AliasedOutDir,
}

impl EnvDepNormalizationDecision {
    /// Does the key hold the value as rustc sees it, rather than a sentinel?
    fn keeps_literal_value(self) -> bool {
        !matches!(self, Self::NormalizedPathOnly | Self::ForcedPathOnly)
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Unchanged => "unchanged",
            Self::NormalizedPathOnly => "normalized path-only",
            Self::KeptAbsoluteNotPathOnly => "kept absolute: not a path-only var",
            Self::KeptAbsoluteManifestDir => "kept absolute: CARGO_MANIFEST_DIR is never path-only",
            Self::KeptAbsoluteNoIncludeProof => "kept absolute: no include proof",
            Self::KeptAbsoluteRuntimeUse => "kept absolute: value use in source",
            Self::KeptAbsoluteScanError => "kept absolute: source scan failed",
            Self::ForcedPathOnly => "forced path-only (user-asserted)",
            Self::AliasedOutDir => "aliased OUT_DIR",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedEnvDep {
    value: String,
    decision: EnvDepNormalizationDecision,
}

/// Lexically collapse `.` / `..` components and unify path separators, without
/// touching the filesystem. Splits on BOTH `/` and `\` so it handles
/// Windows-style paths on any host, preserves a leading root / drive / UNC
/// anchor that `..` cannot escape, and rejoins with the platform separator so
/// the result lines up with the host-canonical rule prefixes the cache key
/// matches against.
///
/// Why: Windows cargo joins a relative `CARGO_TARGET_DIR` (`../oot-target`) onto
/// the package dir literally, so `OUT_DIR` arrives as
/// `C:\proj\pkg\..\oot-target\...` with mixed separators and an unresolved
/// `..`. The workspace-root prefix then only matches up to `C:\proj\pkg`,
/// leaving a `\..`-bearing residual that differs by build location and breaks
/// out-of-tree cross-location convergence (kunobi-ninja/kache#399). Resolving
/// the `..` first yields `C:\proj\oot-target\...`, which normalizes
/// consistently. On Linux cargo already resolves the target dir, so this is a
/// no-op there.
fn lexically_resolve_path(input: &str) -> String {
    let is_sep = |c: char| c == '/' || c == '\\';
    let sep = std::path::MAIN_SEPARATOR;
    let chars: Vec<char> = input.chars().collect();
    let n = chars.len();

    // Split off the un-poppable anchor (root / drive / UNC) and the index where
    // the resolvable component list starts.
    let (anchor, start) = if n >= 2 && is_sep(chars[0]) && is_sep(chars[1]) {
        // UNC: \\server\share — keep `\\` plus the next two components as root.
        let mut root = String::from(r"\\");
        let mut i = 2;
        let mut taken = 0;
        while i < n && taken < 2 {
            while i < n && is_sep(chars[i]) {
                i += 1;
            }
            let comp_start = i;
            while i < n && !is_sep(chars[i]) {
                i += 1;
            }
            if comp_start == i {
                break;
            }
            if taken == 1 {
                root.push(sep);
            }
            root.extend(&chars[comp_start..i]);
            taken += 1;
        }
        root.push(sep);
        (root, i)
    } else if n >= 2 && chars[1] == ':' && chars[0].is_ascii_alphabetic() {
        // Windows drive: `C:` optionally followed by a separator (absolute).
        let mut root: String = chars[..2].iter().collect();
        let mut i = 2;
        if i < n && is_sep(chars[i]) {
            root.push(sep);
            i += 1;
        }
        (root, i)
    } else if n >= 1 && is_sep(chars[0]) {
        (String::from(sep), 1) // Unix absolute
    } else {
        (String::new(), 0) // relative
    };

    let absolute = anchor.ends_with(sep);
    let tail: String = chars[start..].iter().collect();
    let mut stack: Vec<&str> = Vec::new();
    for comp in tail.split(is_sep).filter(|c| !c.is_empty()) {
        match comp {
            "." => {}
            ".." => match stack.last() {
                Some(&top) if top != ".." => {
                    stack.pop();
                }
                _ if absolute => {} // cannot escape the root
                _ => stack.push(".."),
            },
            other => stack.push(other),
        }
    }

    let joined = stack.join(&sep.to_string());
    match (anchor.is_empty(), joined.is_empty()) {
        (true, true) => ".".to_string(),
        (true, false) => joined,
        (false, true) => anchor,
        (false, false) => format!("{anchor}{joined}"),
    }
}

/// Resolve a `-l` spec to a `static` archive in one of `search_dirs` and
/// return `(path, content_hash)`, or `None` when it is not a `static` kind, no
/// candidate is found, or the unit neither links (`links`) nor bundles it
/// (`-bundle`). A `static` spec whose file cannot be modelled (`:RENAME`,
/// unknown modifier) is an error. Ambiguous/read/identity failures return an
/// error so the invocation passes through uncached. `usage` picks the digest
/// for the archive found.
/// Used to fold a native static lib's content into the cache key so an in-place
/// rebuild of `lib<name>.a` (same name, same path, changed bytes) no longer
/// produces a stale hit (#421).
fn resolve_native_static_lib(
    spec: &str,
    search_dirs: &[PathBuf],
    file_hasher: &FileHasher<'_>,
    links: bool,
    usage: impl Fn(&Path) -> StaticLibUse,
) -> Result<Option<(PathBuf, String)>> {
    let file_names = match static_lib_spec(spec) {
        StaticLibSpec::NotStatic => return Ok(None),
        // An rlib or staticlib leaves a `-bundle` archive out of its output;
        // the unit that links it later hashes it.
        StaticLibSpec::Archive { bundle: false, .. } if !links => return Ok(None),
        StaticLibSpec::Archive { files, .. } => files,
        // The archive is bundled or linked, but we cannot tell which file.
        StaticLibSpec::Unmodeled(spec) => {
            anyhow::bail!("native static library spec {spec:?} is not cacheable")
        }
    };
    // Probe the common platform conventions by existence (host-agnostic; the
    // file only exists where the build produced it). Build scripts can emit the
    // same search directory more than once (for example, one `cc::Build::compile`
    // call per archive), so repeated sightings of the same candidate are not
    // ambiguous. If more than one distinct candidate matches — `lib<name>.a`
    // and `<name>.lib`, or hits in two dirs — the choice is target-specific, so
    // fail the cache key rather than risk hashing the wrong file.
    let mut found: Option<PathBuf> = None;
    for dir in search_dirs {
        for filename in &file_names {
            let candidate = dir.join(filename);
            if candidate.is_file() {
                if found.as_ref().is_some_and(|path| path == &candidate) {
                    continue;
                }
                if found.is_some() {
                    anyhow::bail!("ambiguous native static library {spec:?}");
                }
                found = Some(candidate);
            }
        }
    }
    let Some(path) = found else {
        return Ok(None);
    };
    // Every parse/read failure is uncacheable, never name-only: this archive is
    // bundled into the output, so omitting an existing file would be a false hit.
    let hash = file_hasher.hash_static_lib_for(&path, usage(&path))?;
    Ok(Some((path, hash)))
}

/// How an invocation uses a `static=` archive. It decides whether an archive
/// with DWARF-bearing Mach-O members may share its structural digest across
/// checkouts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaticLibUse {
    /// The archive's path never reaches the output: rustc bundles it into its
    /// rlib (or staticlib) output, or ld64 strips it through the
    /// `-oso_prefix` kache injects. A later cached debug link names rlib
    /// members `<rlib>(member)` under `--out-dir`, which that prefix makes
    /// relative.
    Bundled,
    /// The invocation links the archive itself (bin, test, dylib, cdylib,
    /// proc-macro). ld64 writes the archive's absolute path into the `N_OSO`
    /// entry of every DWARF-bearing member.
    Linked,
}

impl StaticLibUse {
    /// Memo namespace. The two uses can hash one file differently, so they
    /// never share a row.
    fn memo_namespace(self) -> &'static str {
        match self {
            Self::Bundled => "static-ar-v7-bundled",
            Self::Linked => "static-ar-v7",
        }
    }
}

/// Whether `archive` is reached under the `-oso_prefix` root as spelled but
/// resolves into a sealed hermetic build-script `OUT_DIR`. The linker names
/// it by the spelled path, which the prefix strips, and the directory it
/// resolves to is read-only and has the same path in every target directory
/// that shares it, so its bytes decide the link as much as a copy under the
/// profile directory would.
fn hermetic_archive_under(archive: &Path, resolved: &Path, spelled_root: &Path) -> bool {
    std::path::absolute(archive).is_ok_and(|spelled| spelled.starts_with(spelled_root))
        && !resolved.starts_with(spelled_root)
        && crate::build_script::in_sealed_out_dir(resolved)
}

/// [`linked_archive_use`] for a link that gets `-oso_prefix` at
/// `spelled_root`, which resolves to `root`. Compared resolved, like OUT_DIR
/// in [`dirs_under`], except for an archive in a sealed hermetic `OUT_DIR`.
fn oso_archive_use(
    args: &RustcArgs,
    archive: &Path,
    spelled_root: &Path,
    root: &Path,
) -> StaticLibUse {
    let resolved = resolved_path(archive);
    if hermetic_archive_under(archive, &resolved, spelled_root) {
        linked_archive_use(args, archive, Some(spelled_root))
    } else {
        linked_archive_use(args, &resolved, Some(root))
    }
}

/// How this invocation uses `archive`. A unit that does not link bundles it.
/// A link keeps the archive's path in its debug map unless the `-oso_prefix`
/// kache injects (`oso_root`, from
/// [`crate::compiler::rustc::oso_prefix_root_for_key`]) strips it.
fn linked_archive_use(args: &RustcArgs, archive: &Path, oso_root: Option<&Path>) -> StaticLibUse {
    if !args.is_executable_output() || oso_root.is_some_and(|root| archive.starts_with(root)) {
        StaticLibUse::Bundled
    } else {
        StaticLibUse::Linked
    }
}

/// Whether a `static` spec that resolves in none of the unit's dirs refuses
/// the key. A linking unit hands the name to the linker, which may take a
/// copy from a system dir the key does not see; rustc itself fails an rlib or
/// staticlib that bundles a missing archive. Native MSVC links resolve the
/// name through `LIB` in their own identity.
fn unresolved_static_lib_is_error(links: bool, native_windows_msvc: bool) -> bool {
    links && !native_windows_msvc
}

/// The name and `+verbatim` flag of a kindless or `dylib` `-l` spec, which a
/// Unix linker may still satisfy with an archive. `None` for other kinds.
fn unix_library_request(spec: &str) -> Option<(&str, bool)> {
    let (kind, name) = spec.split_once('=').unwrap_or(("dylib", spec));
    let (kind, modifiers) = kind.split_once(':').unwrap_or((kind, ""));
    if kind != "dylib" || name.is_empty() {
        return None;
    }
    let verbatim = modifiers
        .split(',')
        .fold(false, |verbatim, modifier| match modifier {
            "+verbatim" => true,
            "-verbatim" => false,
            _ => verbatim,
        });
    Some((name, verbatim))
}

/// The archive a Unix linker takes for `-l name`, if it takes one. The first
/// dir with a candidate wins. It supplies the archive when it holds no shared
/// library for the name (`lib{name}.{ext}` for each of `shared_extensions`),
/// or when the link is static (`prefer_static`), where shared libraries are
/// not candidates. A verbatim name is its own only candidate.
fn resolve_unix_library(
    name: &str,
    verbatim: bool,
    dirs: &[PathBuf],
    prefer_static: bool,
    shared_extensions: &[&str],
    is_file: impl Fn(&Path) -> bool,
) -> Option<PathBuf> {
    let (archive, shared) = if verbatim {
        if !is_native_archive_name(name) {
            return None;
        }
        (name.to_string(), Vec::new())
    } else if prefer_static {
        (format!("lib{name}.a"), Vec::new())
    } else {
        let shared = shared_extensions
            .iter()
            .map(|extension| format!("lib{name}.{extension}"))
            .collect();
        (format!("lib{name}.a"), shared)
    };
    for dir in dirs {
        let has_archive = is_file(&dir.join(&archive));
        let has_shared = shared.iter().any(|file| is_file(&dir.join(file)));
        if has_archive || has_shared {
            return (!has_shared).then(|| dir.join(&archive));
        }
    }
    None
}

/// The shared libraries a Unix linker for `target` prefers to `lib{name}.a`
/// in the same dir: ld64 also reads `.tbd` and `.dylib`, ELF linkers only
/// `.so`.
fn shared_library_extensions(target: &str) -> &'static [&'static str] {
    if target.contains("-apple-") {
        &["tbd", "dylib", "so"]
    } else {
        &["so"]
    }
}

/// Whether the linker takes only archives for `-l`: the last `crt-static`
/// target feature is on, or none is given for a musl target, where it is on
/// by default.
fn prefers_static_libraries(target_features: &[&str], target: &str) -> bool {
    target_features
        .iter()
        .flat_map(|features| features.split(','))
        .filter_map(|feature| match feature.trim() {
            "+crt-static" => Some(true),
            "-crt-static" => Some(false),
            _ => None,
        })
        .next_back()
        .unwrap_or_else(|| target.contains("musl"))
}

/// The target a unit links for: `--target`, or else the wrapped rustc's host
/// (from `rustc -vV`), never the target kache itself was built for.
fn link_target<'a>(target: Option<&'a str>, rustc_host: Option<&'a str>) -> &'a str {
    target.or(rustc_host).unwrap_or("unknown")
}

/// The dirs a host-native Unix linker searches for `-l` after the `-L` dirs,
/// in its order: `LIBRARY_PATH`, then the system and local install dirs,
/// where a library the build names only by `-l` may live. A cross link
/// searches its own sysroot, and other targets resolve libraries elsewhere,
/// so both get none. The compiler's private library dir is left to its
/// identity.
fn default_library_dirs(
    target: &str,
    rustc_host: Option<&str>,
    library_path: Option<&std::ffi::OsStr>,
) -> Vec<PathBuf> {
    if rustc_host != Some(target) {
        return Vec::new();
    }
    let fixed: Vec<String> = if target.contains("-apple-") {
        vec!["/usr/lib".into(), "/usr/local/lib".into()]
    } else if target.contains("-linux-") {
        let multiarch = multiarch_name(target);
        vec![
            format!("/usr/local/lib/{multiarch}"),
            format!("/lib/{multiarch}"),
            format!("/usr/lib/{multiarch}"),
            "/usr/local/lib64".into(),
            "/lib64".into(),
            "/usr/lib64".into(),
            "/usr/local/lib".into(),
            "/lib".into(),
            "/usr/lib".into(),
        ]
    } else {
        return Vec::new();
    };
    library_path
        .map(std::env::split_paths)
        .into_iter()
        .flatten()
        .filter(|dir| !dir.as_os_str().is_empty())
        .chain(fixed.into_iter().map(PathBuf::from))
        .collect()
}

/// A Debian-style multiarch dir name: the triple without its vendor
/// (`x86_64-unknown-linux-gnu` is `x86_64-linux-gnu`).
fn multiarch_name(target: &str) -> String {
    match target.split('-').collect::<Vec<_>>()[..] {
        [arch, _vendor, os, env] => format!("{arch}-{os}-{env}"),
        _ => target.to_string(),
    }
}

/// Whether a file name marks a native archive: `*.a` or `*.lib`, in any case.
fn is_native_archive_name(name: &str) -> bool {
    crate::native_link_key::ends_with_ignore_ascii_case(name, ".a")
        || crate::native_link_key::ends_with_ignore_ascii_case(name, ".lib")
}

/// Whether a path, in the key's normalized spelling, lies inside a Cargo
/// registry package or git checkout. Cargo treats both as fixed for the
/// version or revision the path names, and so does the scan: the key holds
/// that path, so an archive shipped in such a package is not read.
fn is_cargo_package_dir(normalized: &str) -> bool {
    let Some(rest) = normalized.strip_prefix("<CARGO_HOME>") else {
        return false;
    };
    let mut parts = rest.split(['/', '\\']).filter(|part| !part.is_empty());
    matches!(
        (parts.next(), parts.next()),
        (Some("registry"), Some("src")) | (Some("git"), Some("checkouts"))
    )
}

/// `dirs` in order, without repeats.
fn unique_dirs<'a>(dirs: impl IntoIterator<Item = &'a PathBuf>) -> Vec<PathBuf> {
    let mut kept: Vec<PathBuf> = Vec::new();
    for dir in dirs {
        if !kept.contains(dir) {
            kept.push(dir.clone());
        }
    }
    kept
}

/// The dirs among `dirs` outside Cargo packages (see [`is_cargo_package_dir`]),
/// each spelled for the key by `normalize`.
fn dirs_outside_packages<'a>(
    dirs: &'a [PathBuf],
    normalize: impl Fn(&str) -> String + 'a,
) -> impl Iterator<Item = &'a PathBuf> {
    dirs.iter()
        .filter(move |dir| !is_cargo_package_dir(&normalize(&dir.to_string_lossy())))
}

/// The dirs among `dirs` inside `root`. Both are compared resolved: a build
/// script can spell its OUT_DIR through a symlink the environment does not,
/// and macOS reaches `/var` as `/private/var`.
fn dirs_under(dirs: &[PathBuf], root: &Path) -> Vec<PathBuf> {
    if dirs.is_empty() {
        return Vec::new();
    }
    let root = resolved_path(root);
    dirs.iter()
        .filter(|dir| resolved_path(dir).starts_with(&root))
        .cloned()
        .collect()
}

/// Whether `path` lies inside one of `roots`, compared as spelled.
fn is_under_any(path: &Path, roots: &[PathBuf]) -> bool {
    roots.iter().any(|root| path.starts_with(root))
}

/// Where the OS, system package managers and Apple's SDKs install libraries.
/// A `-L` dir under one of these and outside the build tree is not scanned
/// (see [`native_scan_dirs`]). Windows has none, so every dir outside Cargo's
/// packages is scanned there.
const SYSTEM_LIBRARY_DIRS: &[&str] = &[
    // The OS and its distribution packages.
    "/lib",
    "/lib32",
    "/lib64",
    "/libx32",
    "/usr/lib",
    "/usr/lib32",
    "/usr/lib64",
    "/usr/libx32",
    "/usr/local/lib",
    "/usr/local/lib64",
    // Homebrew, MacPorts, Nix and Guix.
    "/opt/homebrew",
    "/usr/local/Cellar",
    "/usr/local/opt",
    "/home/linuxbrew/.linuxbrew",
    "/opt/local",
    "/nix/store",
    "/gnu/store",
    // macOS and its frameworks, and Xcode with its SDKs and toolchains.
    "/System",
    "/Library",
    "/Applications",
];

/// [`SYSTEM_LIBRARY_DIRS`] as paths.
fn system_library_dirs() -> Vec<PathBuf> {
    SYSTEM_LIBRARY_DIRS.iter().map(PathBuf::from).collect()
}

/// The roots of this unit's build tree: Cargo's target dir above the profile
/// dir that holds `--out-dir`, and the workspace root the key normalizes
/// paths against. The scan reads a dir inside them even under a system
/// library dir (see [`NativeLinkContext::scanned_dirs`]).
fn build_tree_roots(args: &RustcArgs) -> Vec<PathBuf> {
    build_tree_roots_of(
        args.out_dir.as_deref(),
        args.target.as_deref(),
        args.path_normalization_root(),
    )
}

/// [`build_tree_roots`] from a unit's `--out-dir`, `--target` and workspace
/// root. An `--out-dir` outside Cargo's layout adds no root.
fn build_tree_roots_of(
    out_dir: Option<&Path>,
    target: Option<&str>,
    workspace: Option<&Path>,
) -> Vec<PathBuf> {
    out_dir
        .and_then(crate::compiler::platform::cargo_profile_dir)
        .map(|profile| cargo_target_dir(&profile, target))
        .into_iter()
        .chain(workspace.map(Path::to_path_buf))
        .collect()
}

/// Cargo's target dir above `profile`: its parent, or its grandparent when
/// the parent is the dir Cargo names after `--target`
/// (`<target>/<triple>/<profile>`). A target spec file names that dir by
/// its stem.
fn cargo_target_dir(profile: &Path, target: Option<&str>) -> PathBuf {
    let Some(parent) = profile.parent() else {
        return profile.to_path_buf();
    };
    let target_name = target.map(|target| match target.strip_suffix(".json") {
        Some(spec) => Path::new(spec).file_name().unwrap_or_default(),
        None => std::ffi::OsStr::new(target),
    });
    match (parent.parent(), target_name) {
        (Some(grandparent), Some(name)) if parent.file_name() == Some(name) => {
            grandparent.to_path_buf()
        }
        _ => parent.to_path_buf(),
    }
}

/// The regular `*.a` and `*.lib` files directly in `dir`, sorted. A missing
/// dir holds none; any other read failure is an error.
pub(crate) fn native_dir_archives(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut archives = Vec::new();
    for path in native_dir_entries(dir)? {
        if path
            .file_name()
            .is_some_and(|name| is_native_archive_name(&name.to_string_lossy()))
            && path.is_file()
        {
            archives.push(path);
        }
    }
    Ok(archives)
}

/// The static frameworks directly in `dir`, sorted: `Name.framework/Name`
/// when it is an `ar` archive. ld64 links such a framework into the output
/// like an archive; a dynamic one is referenced by name.
fn native_dir_static_frameworks(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut frameworks = Vec::new();
    for path in native_dir_entries(dir)? {
        let Some(name) = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_suffix(".framework"))
        else {
            continue;
        };
        let binary = path.join(name);
        if binary.is_file() && crate::native_archive::has_archive_magic(&binary)? {
            frameworks.push(binary);
        }
    }
    Ok(frameworks)
}

/// The framework a `framework` `-l` spec links: its rename when it has one.
/// `None` for other kinds.
fn framework_request(spec: &str) -> Option<&str> {
    let (kind, name) = spec.split_once('=')?;
    let kind = kind.split_once(':').map_or(kind, |(kind, _)| kind);
    let name = name.split_once(':').map_or(name, |(_, rename)| rename);
    (kind == "framework" && !name.is_empty()).then_some(name)
}

/// Whether a `-l` spec is of the `link-arg` kind (`-Z unstable-options`),
/// whose value rustc hands the linker as a raw argument.
fn is_link_arg_spec(spec: &str) -> bool {
    spec.split_once('=')
        .is_some_and(|(kind, _)| kind.split_once(':').map_or(kind, |(kind, _)| kind) == "link-arg")
}

/// The static framework ld64 takes for `-framework name` from `dirs`: the
/// first `name.framework/name` found, when it is an `ar` archive.
fn resolve_static_framework(name: &str, dirs: &[PathBuf]) -> Result<Option<PathBuf>> {
    for dir in dirs {
        let binary = dir.join(format!("{name}.framework")).join(name);
        if binary.is_file() {
            return Ok(crate::native_archive::has_archive_magic(&binary)?.then_some(binary));
        }
    }
    Ok(None)
}

/// The entries of `dir`, sorted. A missing dir has none; any other read
/// failure is an error.
fn native_dir_entries(dir: &Path) -> Result<Vec<PathBuf>> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("reading native search dir {}", dir.display()));
        }
    };
    let mut paths = Vec::new();
    for entry in entries {
        paths.push(
            entry
                .with_context(|| format!("reading native search dir {}", dir.display()))?
                .path(),
        );
    }
    paths.sort();
    Ok(paths)
}

/// Fold every archive `list` finds in `dirs` as `<dir index>/<path in the
/// dir>=<digest>`, and return the archives hashed.
fn fold_native_dir_archives<H: KeyFold>(
    hasher: &mut H,
    dirs: &[PathBuf],
    list: impl Fn(&Path) -> Result<Vec<PathBuf>>,
    hash: impl Fn(&Path) -> Result<String>,
) -> Result<Vec<PathBuf>> {
    let mut hashed = Vec::new();
    for (index, dir) in dirs.iter().enumerate() {
        for archive in list(dir)? {
            let digest = hash(&archive)?;
            let name = archive
                .strip_prefix(dir)
                .unwrap_or(&archive)
                .to_string_lossy();
            fold_field(
                hasher,
                b"native_dir_archive.v1:",
                format!("{index}/{name}={digest}").as_bytes(),
            );
            hashed.push(archive);
        }
    }
    Ok(hashed)
}

/// `path` with symlinks resolved, or made absolute when it does not exist.
fn resolved_path(path: &Path) -> PathBuf {
    std::fs::canonicalize(path)
        .or_else(|_| std::path::absolute(path))
        .unwrap_or_else(|_| path.to_path_buf())
}

/// Whether an rlib compile must pass the store-time bundle audit: it writes
/// an rlib and has a native dir rustc could bundle from.
pub(crate) fn needs_native_bundle_audit(args: &RustcArgs, native_dirs: &[PathBuf]) -> bool {
    args.emits_rlib() && !native_dirs.is_empty()
}

/// Whether `-Z packed-bundled-libs` makes rustc store every bundled archive
/// as one member. Its value is not read: counting the flag as on only
/// credits an rlib less (see the wrapper's bundle audit).
fn packs_bundled_libs(unstable_flags: &[String]) -> bool {
    unstable_flags.iter().any(|flag| {
        let name = flag.split_once('=').map_or(flag.as_str(), |(name, _)| name);
        name.replace('_', "-") == "packed-bundled-libs"
    })
}

/// Whether this compile's rlib carries the archive a `static` `spec`
/// resolves to, and if so whether packed as one member (`Some(true)`) or
/// member by member (`Some(false)`). `packs_all` is [`packs_bundled_libs`].
fn rlib_bundle(spec: &StaticLibSpec<'_>, emits_rlib: bool, packs_all: bool) -> Option<bool> {
    match spec {
        StaticLibSpec::Archive {
            bundle: true,
            whole_archive,
            ..
        } if emits_rlib => Some(*whole_archive || packs_all),
        _ => None,
    }
}

/// The `-L` dirs, input files and `-l` libraries of this unit's
/// `link-arg`/`link-args` values.
fn unix_link_arguments(args: &RustcArgs) -> Result<crate::native_link_key::LinkArgInputs> {
    let mut inputs = crate::native_link_key::LinkArgInputs::default();
    for (key, value) in &args.codegen_opts {
        if let ("link-arg" | "link-args", Some(value)) = (key.as_str(), value.as_deref()) {
            let parsed = crate::native_link_key::unix_link_arg_inputs(key, value)?;
            inputs.files.extend(parsed.files);
            inputs.dirs.extend(parsed.dirs);
            inputs.libs.extend(parsed.libs);
        }
    }
    Ok(inputs)
}

/// A unit's `-L` dirs that name native libraries: `native=`, `all=` and bare
/// (`native`), and `framework=` (`framework`).
struct NativeSearchDirs<'a> {
    native: &'a [PathBuf],
    framework: &'a [PathBuf],
}

/// What [`fold_native_link_inputs`] knows about a unit beyond its argv.
struct NativeLinkContext<'a> {
    /// A native Windows MSVC link, whose libraries and link-argument files
    /// the MSVC identity keys instead.
    native_windows_msvc: bool,
    /// The target the unit links for (see [`link_target`]).
    target: &'a str,
    /// Where a Unix linker looks for `-l` after the `-L` dirs (see
    /// [`default_library_dirs`]).
    default_dirs: Vec<PathBuf>,
    /// The unit's own OUT_DIR, which Cargo sets for a package with a build
    /// script.
    out_dir: Option<&'a Path>,
    /// The roots of the unit's build tree (see [`build_tree_roots`]).
    build_tree: Vec<PathBuf>,
    /// Where system libraries live (see [`SYSTEM_LIBRARY_DIRS`]), compared
    /// as spelled with a dir's resolved path.
    system_dirs: Vec<PathBuf>,
    /// Gives dirs their key spelling (see [`is_cargo_package_dir`]).
    path_normalizer: &'a PathNormalizer,
}

impl NativeLinkContext<'_> {
    /// The dirs among `dirs` whose every archive can key the unit, without
    /// repeats: those outside Cargo's packages and outside the system library
    /// dirs, or inside the build tree. Each dir is compared resolved, and the
    /// build tree is resolved only for a dir under a system library dir.
    fn scanned_dirs(&self, dirs: &[PathBuf]) -> Vec<PathBuf> {
        let unique = unique_dirs(dirs);
        let build_tree = std::cell::OnceCell::new();
        let resolved_build_tree = || {
            build_tree.get_or_init(|| {
                self.build_tree
                    .iter()
                    .map(|root| resolved_path(root))
                    .collect::<Vec<_>>()
            })
        };
        dirs_outside_packages(&unique, |dir| self.path_normalizer.normalize(dir))
            .filter(|dir| {
                let dir = resolved_path(dir);
                !is_under_any(&dir, &self.system_dirs) || is_under_any(&dir, resolved_build_tree())
            })
            .cloned()
            .collect()
    }
}

/// The `-L` dirs (`dirs`) whose every archive keys this unit. A unit whose
/// output carries the native closure (see [`RustcArgs::links_native_closure`])
/// takes those [`NativeLinkContext::scanned_dirs`] keeps. An rlib takes those
/// under its own OUT_DIR, which a `#[link(kind = "static")]` attribute can
/// bundle from with no `-l` on argv. Other units take none.
///
/// Every dir outside Cargo's packages is scanned, wherever it is: the build
/// tree, a sibling build, a monorepo's native build above the workspace, a
/// path dependency beside it, a vcpkg or conda install. The exception is a
/// dir under a system library dir (`/usr/lib`, `/opt/homebrew`, see
/// [`SYSTEM_LIBRARY_DIRS`]) outside the build tree: a package upgrade there
/// would re-key every link, and one thin archive would make every link
/// uncacheable. There only what an `-l` of this unit resolves to is keyed,
/// so an archive that reaches the output with no such `-l` (through a
/// dependency's rlib, a `#[link]` attribute or a `-bundle` spec) is not.
fn native_scan_dirs(
    args: &RustcArgs,
    dirs: &[PathBuf],
    context: &NativeLinkContext<'_>,
) -> Vec<PathBuf> {
    if args.links_native_closure() {
        context.scanned_dirs(dirs)
    } else if args.emits_rlib()
        && let Some(out_dir) = context.out_dir
    {
        dirs_under(&unique_dirs(dirs), out_dir)
    } else {
        Vec::new()
    }
}

/// Fold the native inputs that reach this unit's output and return what was
/// hashed, for the store-time bundle audit:
///
/// - each `-l` spec by name, and the archive a `static` spec names (a linking
///   unit that finds it in no `-L` dir refuses the key, since the linker may
///   take a system copy);
/// - on a Unix link, the archive the linker picks for a kindless or `dylib`
///   spec, and the files and `-l` libraries its link arguments name, looked
///   up in the `-L` dirs and then the linker's default dirs;
/// - on a linking unit, the static framework a `framework` spec names;
/// - every archive in the `-L` dirs [`native_scan_dirs`] picks, and on a
///   linking unit every static framework in the `framework=` dirs
///   [`NativeLinkContext::scanned_dirs`] keeps. Cargo hands a build script's
///   `-L` to every dependent, so this keys archives that reach the output
///   only through a dependency's rlib, whose own bytes no `--extern` of this
///   unit covers.
///
/// A linking unit with a `link-arg` `-l` spec is refused: rustc hands its
/// value to the linker as is, and no rule here reads it.
///
/// The scan keys a linked output to every archive its scanned `-L` dirs
/// hold, so an archive a build script rebuilds with different bytes each run
/// (a build date, say) re-keys every linked unit downstream, as its output
/// changes too. An archive whose members the portable digest does not admit
/// (wasm or COFF objects, LTO bitcode) keys with its path, so a linked output
/// that finds one in a build-tree dir hits only in the checkout that stored
/// it.
///
/// Not keyed: a library a dependency's metadata names and the linker finds
/// only in its default dirs, which no argv of this unit shows, and the
/// compiler's own library dirs, which its identity stands for.
fn fold_native_link_inputs<H: KeyFold>(
    hasher: &mut H,
    args: &RustcArgs,
    search: &NativeSearchDirs<'_>,
    context: &NativeLinkContext<'_>,
    file_hasher: &FileHasher<'_>,
) -> Result<KeyedNativeArchives> {
    let crate_name = args.crate_name.as_deref().unwrap_or("unknown");
    let links = args.invokes_linker();
    let unix_link = links && !context.native_windows_msvc;
    let link_arguments = if unix_link {
        unix_link_arguments(args)?
    } else {
        crate::native_link_key::LinkArgInputs::default()
    };
    let spelled_oso_root = crate::compiler::rustc::oso_prefix_root_for_key(args);
    let oso_root = spelled_oso_root.as_deref().map(resolved_path);
    let archive_use = |path: &Path| match (spelled_oso_root.as_deref(), oso_root.as_deref()) {
        (Some(spelled_root), Some(root)) => oso_archive_use(args, path, spelled_root, root),
        _ => linked_archive_use(args, path, None),
    };
    let hash_archive = |path: &Path| {
        let usage = archive_use(path);
        let digest = file_hasher.hash_static_lib_for(path, usage)?;
        tracing::trace!(
            "[key:{}] native_archive:{}={} ({usage:?})",
            crate_name,
            path.display(),
            &digest[..digest.len().min(24)]
        );
        Ok::<_, anyhow::Error>(digest)
    };
    let mut lib_dirs = search.native.to_vec();
    lib_dirs.extend(link_arguments.dirs.iter().cloned());
    // Where a Unix linker looks for an `-l`: the `-L` dirs, then its own.
    let mut library_dirs = lib_dirs.clone();
    library_dirs.extend(context.default_dirs.iter().cloned());
    let target_features: Vec<&str> = args
        .codegen_opts
        .iter()
        .filter(|(key, _)| key == "target-feature")
        .filter_map(|(_, value)| value.as_deref())
        .collect();
    let prefer_static = prefers_static_libraries(&target_features, context.target);
    let shared_extensions = shared_library_extensions(context.target);
    let packs_all = packs_bundled_libs(&args.unstable_flags);

    let mut archives = Vec::new();
    let mut bundled = Vec::new();
    for lib in &args.link_libs {
        hasher.update(b"link_lib:");
        hasher.update(lib.as_bytes());
        hasher.update(b"\n");
        tracing::trace!("[key:{}] link_lib:{}", crate_name, lib);
        if links && is_link_arg_spec(lib) {
            anyhow::bail!(
                "native library spec {lib:?} hands the linker a raw argument the key does not \
                 model"
            );
        }

        let dirs = if unix_link { &lib_dirs } else { search.native };
        let spec = static_lib_spec(lib);
        let mut resolved = resolve_native_static_lib(lib, dirs, file_hasher, links, archive_use)?;
        let is_static = matches!(spec, StaticLibSpec::Archive { .. });
        if resolved.is_none()
            && is_static
            && unresolved_static_lib_is_error(links, context.native_windows_msvc)
        {
            anyhow::bail!(
                "native static library {lib:?} is in no -L directory; the linker could take \
                 a copy the key does not hash"
            );
        }
        if let Some((path, _)) = &resolved
            && let Some(packed) = rlib_bundle(&spec, args.emits_rlib(), packs_all)
        {
            bundled.push(BundledArchive {
                path: path.clone(),
                packed,
            });
        }
        if resolved.is_none()
            && unix_link
            && let Some((name, verbatim)) = unix_library_request(lib)
            && let Some(path) = resolve_unix_library(
                name,
                verbatim,
                &library_dirs,
                prefer_static,
                shared_extensions,
                Path::is_file,
            )
        {
            let hash = hash_archive(&path)?;
            resolved = Some((path, hash));
        }
        if links
            && let Some(name) = framework_request(lib)
            && let Some(path) = resolve_static_framework(name, search.framework)?
        {
            let hash = hash_archive(&path)?;
            resolved = Some((path, hash));
        }
        if let Some((path, content_hash)) = resolved {
            hasher.update(b"link_lib_content:");
            hasher.update(content_hash.as_bytes());
            hasher.update(b"\n");
            tracing::trace!(
                "[key:{}] link_lib_content:{}={} ({})",
                crate_name,
                lib,
                &content_hash[..content_hash.len().min(16)],
                path.display()
            );
            archives.push(path);
        }
    }

    for (index, file) in link_arguments.files.iter().enumerate() {
        if !file.is_file() {
            anyhow::bail!("linker input {} is not a regular file", file.display());
        }
        let digest = if is_native_archive_name(&file.to_string_lossy()) {
            hash_archive(file)?
        } else {
            file_hasher
                .hash(file)
                .with_context(|| format!("hashing linker input {}", file.display()))?
        };
        fold_field(
            hasher,
            b"link_arg_input.v1:",
            format!("{index}={digest}").as_bytes(),
        );
        tracing::trace!("[key:{}] link_arg_input:{}", crate_name, file.display());
    }

    // A `-Bstatic` in the link arguments can make the linker skip a shared
    // library, so the first archive for the name counts.
    for (index, (name, verbatim)) in link_arguments.libs.iter().enumerate() {
        if let Some(path) = resolve_unix_library(
            name,
            *verbatim,
            &library_dirs,
            true,
            shared_extensions,
            Path::is_file,
        ) {
            let digest = hash_archive(&path)?;
            fold_field(
                hasher,
                b"link_arg_library.v1:",
                format!("{index}={digest}").as_bytes(),
            );
            tracing::trace!("[key:{}] link_arg_library:{}", crate_name, path.display());
            archives.push(path);
        }
    }

    let scan_dirs = native_scan_dirs(args, &lib_dirs, context);
    archives.extend(fold_native_dir_archives(
        hasher,
        &scan_dirs,
        native_dir_archives,
        hash_archive,
    )?);
    if links {
        archives.extend(fold_native_dir_archives(
            hasher,
            &context.scanned_dirs(search.framework),
            native_dir_static_frameworks,
            hash_archive,
        )?);
    }
    // Only an rlib's store runs the bundle audit.
    let audit_dirs = if args.emits_rlib() {
        context.scanned_dirs(search.native)
    } else {
        Vec::new()
    };

    Ok(KeyedNativeArchives {
        archives,
        bundled,
        dirs: audit_dirs,
    })
}

/// Auxiliary linker files are not yet captured/restored as cache artifacts.
/// Refuse the native-static-lib invocation rather than guess at their content.
fn native_linker_side_files_are_unmodeled(args: &RustcArgs) -> bool {
    let apple_target = match args.target.as_deref() {
        Some(target) => is_builtin_apple_target(target),
        None => cfg!(target_vendor = "apple"),
    };
    args.codegen_opts.iter().any(|(key, value)| {
        matches!(key.as_str(), "link-arg" | "link-args")
            && value
                .as_deref()
                .is_some_and(|value| linker_value_has_unmodeled_file(value, apple_target))
    })
}

fn is_builtin_apple_target(target: &str) -> bool {
    // Rust 1.95's built-in Apple targets. Unknown names can resolve arbitrary
    // JSON through RUST_TARGET_PATH, so additions must fail closed until
    // reviewed rather than inheriting ld64 token semantics from their spelling.
    matches!(
        target,
        "aarch64-apple-darwin"
            | "aarch64-apple-ios"
            | "aarch64-apple-ios-macabi"
            | "aarch64-apple-ios-sim"
            | "aarch64-apple-tvos"
            | "aarch64-apple-tvos-sim"
            | "aarch64-apple-visionos"
            | "aarch64-apple-visionos-sim"
            | "aarch64-apple-watchos"
            | "aarch64-apple-watchos-sim"
            | "arm64_32-apple-watchos"
            | "arm64e-apple-darwin"
            | "arm64e-apple-ios"
            | "arm64e-apple-tvos"
            | "armv7k-apple-watchos"
            | "armv7s-apple-ios"
            | "i386-apple-ios"
            | "i686-apple-darwin"
            | "x86_64-apple-darwin"
            | "x86_64-apple-ios"
            | "x86_64-apple-ios-macabi"
            | "x86_64-apple-tvos"
            | "x86_64-apple-watchos-sim"
            | "x86_64h-apple-darwin"
    )
}

fn linker_value_has_unmodeled_file(value: &str, apple_target: bool) -> bool {
    value.split([',', '=', ' ', '\t', '\n', '\r']).any(|token| {
        matches!(
            token,
            "-map"
                | "-Map"
                | "--Map"
                | "-order_file"
                | "-sectorder"
                | "--symbol-ordering-file"
                | "--call-graph-ordering-file"
                | "--section-ordering-file"
        ) || token.eq_ignore_ascii_case("/map")
            || ascii_prefix_eq_ignore_case(token, "/map:")
            || ascii_prefix_eq_ignore_case(token, "/mapinfo:")
            || ascii_prefix_eq_ignore_case(token, "/order:")
            || ascii_prefix_eq_ignore_case(token, "/call-graph-ordering-file:")
            || (token.starts_with('@')
                && !(apple_target
                    && (token == "@loader_path"
                        || token.starts_with("@loader_path/")
                        || token == "@rpath"
                        || token.starts_with("@rpath/")
                        || token == "@executable_path"
                        || token.starts_with("@executable_path/"))))
    })
}

fn ascii_prefix_eq_ignore_case(value: &str, prefix: &str) -> bool {
    value
        .get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
}

/// How a `-l` spec maps to an archive the cache key must hash.
#[derive(Debug, PartialEq, Eq)]
enum StaticLibSpec<'a> {
    /// Not a `static` kind (`dylib=`, `framework=`, bare `-l name`): referenced
    /// rather than bundled, so the name alone keys it.
    NotStatic,
    /// A `static` archive rustc looks up under these file names in the `-L`
    /// dirs. `+whole-archive` and `+as-needed` change how the archive is
    /// linked, not which file it is; the raw spec already keys them. `bundle`
    /// is the last `±bundle`: an rlib or staticlib leaves a `-bundle` archive
    /// out of its output. `whole_archive` is the last `±whole-archive`, under
    /// which an rlib stores the archive as one member.
    Archive {
        files: Vec<String>,
        bundle: bool,
        whole_archive: bool,
    },
    /// A `:RENAME` or an unknown modifier. Which file rustc reads is not
    /// modelled, so the invocation must not be cached on the name alone.
    Unmodeled(&'a str),
}

/// Classify a `-l` spec (`[KIND[:MODIFIERS]=]NAME[:RENAME]`). A rename is
/// unmodeled for every kind but `framework`: a kindless or `dylib` rename can
/// retarget a `#[link(kind = "static")]` attribute, which bundles.
fn static_lib_spec(spec: &str) -> StaticLibSpec<'_> {
    let (kind, name) = spec.split_once('=').unwrap_or(("", spec));
    let (kind, modifiers) = kind.split_once(':').unwrap_or((kind, ""));
    if kind != "framework" && name.contains(':') {
        return StaticLibSpec::Unmodeled(spec);
    }
    if kind != "static" {
        return StaticLibSpec::NotStatic;
    }
    if name.is_empty() {
        return StaticLibSpec::Unmodeled(spec);
    }
    let mut verbatim = false;
    let mut bundle = true;
    let mut whole_archive = false;
    for modifier in modifiers.split(',').filter(|m| !m.is_empty()) {
        match modifier {
            "+verbatim" => verbatim = true,
            "-verbatim" => verbatim = false,
            "+bundle" => bundle = true,
            "-bundle" => bundle = false,
            "+whole-archive" => whole_archive = true,
            "-whole-archive" => whole_archive = false,
            "+as-needed" | "-as-needed" => {}
            _ => return StaticLibSpec::Unmodeled(spec),
        }
    }
    let files = if verbatim {
        vec![name.to_string()]
    } else {
        vec![format!("lib{name}.a"), format!("{name}.lib")]
    };
    StaticLibSpec::Archive {
        files,
        bundle,
        whole_archive,
    }
}

/// The normalized key value for a path-only env dep: the `<OUT_DIR:unit>`
/// sentinel form when the value lives under the build's own OUT_DIR
/// (kunobi-ninja/kache#330 — the unit-hash component stays observable), else
/// the generic prefix-rule normalization.
fn sentinelized_env_dep_value(
    paths: &EnvDepPaths<'_>,
    resolved: &EnvDepValue<'_>,
    normalized: &str,
) -> String {
    if let Some(rel) = paths.out_dir_suffix(resolved) {
        let unit = out_dir_unit(paths.out_dir);
        if rel.is_empty() {
            format!("<OUT_DIR:{unit}>")
        } else {
            format!("<OUT_DIR:{unit}>/{}", rel.trim_start_matches('/'))
        }
    } else {
        normalized.to_string()
    }
}

fn normalize_env_dep_value_with_hasher(
    crate_name: &str,
    var: &str,
    value: &EnvDepValue<'_>,
    paths: &EnvDepPaths<'_>,
    file_hasher: &FileHasher<'_>,
    path_normalizer: &PathNormalizer,
    aliased_out_dir: Option<&Path>,
) -> NormalizedEnvDep {
    let val = value.raw;
    // The shared OUT_DIR is one string on this machine whatever the checkout,
    // and the artifact bakes exactly that string. A sentinel from a rule that
    // happens to cover the cache dir (`<HOME>`) would give one key to
    // artifacts that bake different strings, so the value stays raw.
    if aliased_out_dir.is_some_and(|dir| value_at_or_under(val, dir)) {
        return NormalizedEnvDep {
            value: val.to_string(),
            decision: EnvDepNormalizationDecision::AliasedOutDir,
        };
    }
    // Resolve the value to the SAME canonical form the rule prefixes use
    // (kunobi-ninja/kache#399). Windows cargo joins a relative CARGO_TARGET_DIR
    // literally, so an out-of-tree `OUT_DIR` arrives as `...\pkg\..\oot-target\...`
    // with mixed separators and an unresolved `..`. The PathNormalizer rules are
    // `canonicalize()`d (symlinks + `..` resolved, `\\?\` stripped, OS-native
    // separators, NFC), and `normalize` is a byte-literal substring replace — so
    // the raw value matches no rule and the build location stays in the key,
    // missing on relocate. Running the value through the rules' own
    // `canonical_string` puts it in matchable shape (an out-of-tree OUT_DIR
    // under the workspace / `$CARGO_TARGET_DIR` collapses to its
    // `<WORKSPACE>`/`<TARGET>` sentinel, converging across build locations). The
    // dir exists at key time (the build script already
    // wrote into it). Fall back to a lexical `.`/`..` collapse when the path is
    // absent. A no-op on Linux/macOS with a relative target dir, which cargo
    // canonicalizes before invoking rustc.
    let resolved = value
        .canonical()
        .and_then(crate::path_normalizer::canonical_form)
        .unwrap_or_else(|| lexically_resolve_path(val));
    let normalized = path_normalizer.normalize(&resolved);

    // Unchanged: resolution was a no-op AND no rule prefix matched. The value is
    // not a path kache models, so it enters the key verbatim.
    if resolved == val && normalized == val {
        return NormalizedEnvDep {
            value: val.to_string(),
            decision: EnvDepNormalizationDecision::Unchanged,
        };
    }

    // A `crate_name:VAR` entry in the path-only allowlist is the user-asserted
    // FORCE form: it bypasses the include-proof and runtime-value scans for
    // exactly that (crate, var) pair. Plain entries keep the scan-gated
    // semantics below. rustc crate-name form (underscores).
    // CARGO_MANIFEST_DIR is never forceable: rustc can embed it in crate
    // metadata and generated code, so erasing it from the key can restore an
    // rlib containing another checkout's path (#167).
    let forced = !is_manifest_dir_var(var)
        && path_normalizer.path_only_env_vars().iter().any(|entry| {
            matches!(entry.split_once(':'), Some((krate, v)) if krate == crate_name && v == var)
        });
    // The checks below read the resolved form. It is the raw value, and so
    // shares its canonical path, unless resolution changed the string.
    let resolved_value = (resolved != val).then(|| EnvDepValue::new(&resolved));
    let resolved_value = resolved_value.as_ref().unwrap_or(value);
    if forced {
        return NormalizedEnvDep {
            value: sentinelized_env_dep_value(paths, resolved_value, &normalized),
            decision: EnvDepNormalizationDecision::ForcedPathOnly,
        };
    }

    let decision = env_dep_path_only_decision(
        var,
        resolved_value,
        paths,
        path_normalizer.path_only_env_vars(),
        file_hasher,
    );
    if decision == EnvDepNormalizationDecision::NormalizedPathOnly {
        // A value under the build's own OUT_DIR normalizes relative to
        // OUT_DIR itself (kunobi-ninja/kache#330): the generic prefix rules
        // keep per-LOCATION path components inside the sentinel'd value
        // (see `out_dir_relative_suffix`), diverging keys across build
        // locations, while the include'd CONTENT the locator points at is
        // already content-hashed through the source list. Cargo's per-unit
        // directory (`<pkg>-<unit hash>`) stays IN the sentinel: a
        // generated file can observe its own path (`file!()`, panic
        // locations), and rustc's remap keeps the unit component in that
        // observable value, so two units whose OUT_DIRs differ only by
        // unit hash are not interchangeable (cross-model review finding).
        return NormalizedEnvDep {
            value: sentinelized_env_dep_value(paths, resolved_value, &normalized),
            decision: EnvDepNormalizationDecision::NormalizedPathOnly,
        };
    }

    // Keep-absolute branch: by design the raw path stays in the key
    // because the compiled artifact may embed it via `env!`. Do not
    // warn here: this is an intentional key discriminator, and Cargo
    // fingerprints RUSTC_WRAPPER stderr for build freshness. The decision
    // names the reason for the trace.
    NormalizedEnvDep {
        value: val.to_string(),
        decision,
    }
}

#[cfg(test)]
fn normalize_env_dep_value(
    crate_name: &str,
    var: &str,
    val: &str,
    source_files: &[std::path::PathBuf],
    path_normalizer: &PathNormalizer,
) -> NormalizedEnvDep {
    normalize_env_dep_value_in(crate_name, var, val, source_files, path_normalizer, None)
}

/// [`normalize_env_dep_value`] for a unit whose `OUT_DIR` is `out_dir`.
#[cfg(test)]
fn normalize_env_dep_value_in(
    crate_name: &str,
    var: &str,
    val: &str,
    source_files: &[std::path::PathBuf],
    path_normalizer: &PathNormalizer,
    out_dir: Option<&OsStr>,
) -> NormalizedEnvDep {
    normalize_env_dep_value_with_hasher(
        crate_name,
        var,
        &EnvDepValue::new(val),
        &EnvDepPaths::new(out_dir, source_files),
        &FileHasher::new(),
        path_normalizer,
        None,
    )
}

/// Is `value` the path `dir` or a path under it, by components?
pub(crate) fn value_at_or_under(value: &str, dir: &Path) -> bool {
    Path::new(value).starts_with(dir)
}

/// Does this env dep leave an OUT_DIR path in the key as a literal? `under`
/// answers whether the value sits under the unit's OUT_DIR; it is only asked
/// when the rest cannot settle it.
fn env_dep_bakes_out_dir(
    var: &str,
    decision: EnvDepNormalizationDecision,
    under: impl FnOnce() -> bool,
) -> bool {
    decision.keeps_literal_value() && (var == "OUT_DIR" || under())
}

/// Whether `var`'s value may be path-normalized in the cache key:
/// [`EnvDepNormalizationDecision::NormalizedPathOnly`], or the reason it stays
/// absolute. `allowlist` is the user-configured opt-in set
/// (`KACHE_PATH_ONLY_ENV_VARS` / `[cache] path_only_env_vars`); OUT_DIR is
/// always included.
fn env_dep_path_only_decision(
    var: &str,
    value: &EnvDepValue<'_>,
    paths: &EnvDepPaths<'_>,
    allowlist: &[String],
    file_hasher: &FileHasher<'_>,
) -> EnvDepNormalizationDecision {
    // OUT_DIR is the built-in path-only exception:
    //
    //   include!(concat!(env!("OUT_DIR"), "/foo"))
    //
    // splices file content into the AST and dep-info lists the generated
    // file under OUT_DIR. That dep-info shape is necessary but not sufficient:
    // a crate can also use `env!("OUT_DIR")` as a runtime value. Normalize only
    // when source inspection shows an env macro use inside an `include*!(...)`
    // path-locator context and no use outside one (see
    // [`env_dep_source_decision`]). Other vars with the same property —
    // e.g. a generated build-config path, or an objdir base used by an
    // `include!` macro — can be opted into `allowlist` by the build.
    //
    // It must stay an explicit allowlist: for CARGO_MANIFEST_DIR and arbitrary
    // user vars the path-only test alone is not valid (normal crate sources
    // already live under the manifest dir, so normalizing it would recreate
    // #167). The `path_is_only_used_for_includes` gate is then applied on top,
    // so an allowlisted var is still kept absolute when it is baked as a value
    // rather than used to locate a source file.
    //
    // Third built-in case (kunobi-ninja/kache#431): a build script can set
    // `cargo:rustc-env=VAR=<absolute path under OUT_DIR>` and the crate then does
    // `include!(env!("VAR"))` — e.g. typenum's TYPENUM_BUILD_CONSTS points at
    // `$OUT_DIR/consts.rs`. Such a var is functionally identical to OUT_DIR: its
    // value is build-generated, ephemeral, and only locates a generated include,
    // so it is exactly as safe to normalize. Keeping it absolute makes the crate
    // (typenum, a foundational substrate/crypto dep) re-key per checkout path,
    // missing cross-clone. We gate it on the value living UNDER the build's
    // OUT_DIR — the precise property that makes OUT_DIR safe and that
    // CARGO_MANIFEST_DIR (the #167 hazard) does NOT have — so it widens
    // eligibility without re-opening #167. The same include-only proof below
    // still applies, so a VAR pointing under OUT_DIR but baked as a runtime
    // value is still kept absolute.
    // CARGO_MANIFEST_DIR is refused in every form, listed or not: rustc can
    // embed it in crate metadata and generated code, and a crate's own sources
    // always live under it, so the include proof below is trivially satisfied
    // and normalizing it restores another checkout's path (#167).
    if is_manifest_dir_var(var) {
        return EnvDepNormalizationDecision::KeptAbsoluteManifestDir;
    }
    if !(var == "OUT_DIR"
        || allowlist.iter().any(|v| v == var)
        || paths.value_is_under_out_dir(value))
    {
        return EnvDepNormalizationDecision::KeptAbsoluteNotPathOnly;
    }
    if !path_is_only_used_for_includes(value.probe(), paths.source_probes()) {
        return EnvDepNormalizationDecision::KeptAbsoluteNoIncludeProof;
    }
    env_dep_source_decision(var, paths.source_files, file_hasher)
}

/// Whether `var` names Cargo's manifest dir. Windows matches environment
/// names case-insensitively, so dep-info can carry any spelling of it; on
/// Unix a differently-cased name is a different variable, and refusing it
/// costs misses only.
pub(crate) fn is_manifest_dir_var(var: &str) -> bool {
    var.eq_ignore_ascii_case("CARGO_MANIFEST_DIR")
}

/// The form a path is compared in: canonical when it resolves, else as given.
///
/// Canonical comparison keeps the macOS `/tmp` ↔ `/private/tmp` symlink from
/// producing a spurious mismatch. A path that cannot be canonicalized (file
/// moved, etc.) falls back to its raw components, and `Path::starts_with`
/// still compares whole components without requiring lexical matching.
fn path_probe(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

/// The probe form of `OUT_DIR`, or `None` when it is unset or its probe is
/// not absolute. An empty or relative OUT_DIR cannot anchor a meaningful
/// "under" test.
fn out_dir_probe(out_dir: Option<&OsStr>) -> Option<PathBuf> {
    let probe = path_probe(Path::new(out_dir?));
    probe.is_absolute().then_some(probe)
}

/// Cargo's per-unit directory name (`<pkg>-<unit hash>`), the parent of
/// `OUT_DIR`; empty when it has none.
fn out_dir_unit(out_dir: Option<&OsStr>) -> String {
    out_dir
        .map(Path::new)
        .and_then(Path::parent)
        .and_then(Path::file_name)
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// An env-dep value and its canonical path, resolved at most once however
/// many checks read it (kunobi-ninja/kache#560).
struct EnvDepValue<'a> {
    raw: &'a str,
    canonical: OnceCell<Option<PathBuf>>,
}

impl<'a> EnvDepValue<'a> {
    fn new(raw: &'a str) -> Self {
        Self {
            raw,
            canonical: OnceCell::new(),
        }
    }

    fn canonical(&self) -> Option<&Path> {
        self.canonical
            .get_or_init(|| std::fs::canonicalize(self.raw).ok())
            .as_deref()
    }

    /// The value in [`path_probe`] form.
    fn probe(&self) -> &Path {
        self.canonical().unwrap_or(Path::new(self.raw))
    }
}

/// The paths env-dep values are compared against, resolved once per key
/// computation rather than once per env dep (kunobi-ninja/kache#560).
///
/// `out_dir` is the invocation's `OUT_DIR`, passed in rather than read from
/// the process environment. Both probes resolve on first use, so a unit
/// whose env deps never reach a path check canonicalizes nothing.
struct EnvDepPaths<'a> {
    out_dir: Option<&'a OsStr>,
    out_probe: OnceCell<Option<PathBuf>>,
    source_files: &'a [PathBuf],
    source_probes: OnceCell<Vec<PathBuf>>,
}

impl<'a> EnvDepPaths<'a> {
    fn new(out_dir: Option<&'a OsStr>, source_files: &'a [PathBuf]) -> Self {
        Self {
            out_dir,
            out_probe: OnceCell::new(),
            source_files,
            source_probes: OnceCell::new(),
        }
    }

    fn out_probe(&self) -> Option<&Path> {
        self.out_probe
            .get_or_init(|| out_dir_probe(self.out_dir))
            .as_deref()
    }

    /// The dep-info source files in [`path_probe`] form, in the same order.
    fn source_probes(&self) -> &[PathBuf] {
        self.source_probes
            .get_or_init(|| self.source_files.iter().map(|f| path_probe(f)).collect())
    }

    /// The value's path relative to the build's own `OUT_DIR`; see
    /// [`out_dir_relative_suffix`]. `None` when `OUT_DIR` cannot anchor the
    /// test, which then never resolves the value.
    fn out_dir_suffix(&self, value: &EnvDepValue<'_>) -> Option<String> {
        out_dir_relative_suffix(value.probe(), self.out_probe()?)
    }

    /// True when the value is a path located under the current build's
    /// `OUT_DIR`. A build-script `cargo:rustc-env` var whose value points
    /// here (typenum's `TYPENUM_BUILD_CONSTS` → `$OUT_DIR/consts.rs`) is
    /// build-generated and ephemeral, so it shares OUT_DIR's safety for key
    /// path-normalization (kunobi-ninja/kache#431).
    ///
    /// False when `OUT_DIR` is unset (the crate has no build script, so no
    /// such var exists) or the value is not under it. In particular
    /// `CARGO_MANIFEST_DIR`, which lives above OUT_DIR, never qualifies here.
    fn value_is_under_out_dir(&self, value: &EnvDepValue<'_>) -> bool {
        self.out_dir_suffix(value).is_some()
    }
}

/// The value's path relative to the build's own `OUT_DIR`, when it lives
/// under it (`Some("")` for `OUT_DIR` itself). Both arguments are in
/// [`path_probe`] form. This is the anchor for the
/// `<OUT_DIR>` sentinel (kunobi-ninja/kache#330): an OUT_DIR-locator value
/// must normalize relative to OUT_DIR, not through the generic prefix rules
/// — an out-of-workspace `CARGO_TARGET_DIR` makes the derived workspace
/// root the target dir's PARENT, so the generic `<WORKSPACE>` rule matches
/// first and keeps the per-location target-dir component inside the
/// sentinel'd value, diverging the key across build locations. Anchoring on
/// OUT_DIR itself also drops cargo's per-unit hash from the value, which
/// the generic rules preserve.
fn out_dir_relative_suffix(value_probe: &Path, out_probe: &Path) -> Option<String> {
    value_probe
        .strip_prefix(out_probe)
        .ok()
        .map(|rel| rel.to_string_lossy().replace('\\', "/"))
}

/// Decide whether dep-info shows the env_dep value acting as the parent dir
/// of one or more `include!()`'d source files. This is only the path-shape
/// half of the proof; [`env_dep_source_decision`] rejects dual-pattern
/// crates that also bake the env value into the compiled artifact.
///
/// Background and contract: see the OUT_DIR comment in
/// [`compute_cache_key`] and issue kunobi-ninja/kache#75.
///
/// Both arguments are in [`path_probe`] form.
fn path_is_only_used_for_includes(value_probe: &Path, source_probes: &[PathBuf]) -> bool {
    source_probes.iter().any(|f| f.starts_with(value_probe))
}

/// Normalizes only when source text proves `var` is only an `include*!(...)`
/// path locator: at least one Rust file shows `env!(var)` / `option_env!(var)`
/// inside an include argument, and no file shows a use the scanner cannot
/// place there.
///
/// The proof must be positive. An env macro expanded from another crate's
/// `macro_rules!` resolves in this crate, so dep-info reports the env dep while
/// this crate's sources never name the var; without a visible include use, its
/// value may be baked into the artifact. Such crates keep the absolute value.
/// Proof comes only from files whose path ends in `.rs`, so an `include_str!`'d
/// README that quotes an include is not proof. The test is the path, not what
/// rustc did with the file: a `.rs` file read as text (`include_str!` of a
/// codegen template or a UI-test fixture) still counts. Every file, whatever
/// its extension, still counts AGAINST the var.
///
/// Residual gaps, where the text looks like a locator but the compiled crate
/// can still bake the value:
/// - another crate's macro, invoked alongside a visible include use, that
///   expands to a value use of the same var;
/// - a macro named `include`, `include_str` or `include_bytes` that is not the
///   builtin (a local `macro_rules!`, an import, or a path like
///   `mycrate::include!`);
/// - an include inside another macro's arguments, or on an item under an
///   attribute macro, which can move the tokens out of the include;
/// - a `.rs` file rustc only read as text, whose quoted include reads as proof.
///
/// The `.rs` rule also costs hits: an `include!`'d fragment named `.in`,
/// `.txt` or without an extension supplies no proof, so its crate keeps the
/// absolute value.
///
/// Missing or changed files fail closed.
fn env_dep_source_decision(
    var: &str,
    source_files: &[std::path::PathBuf],
    file_hasher: &FileHasher<'_>,
) -> EnvDepNormalizationDecision {
    let mut proven = false;
    for file in source_files {
        match file_hasher.env_dep_use(file, var) {
            Ok(SourceEnvDepUse::Unused) => {}
            Ok(SourceEnvDepUse::IncludeLocator) => {
                proven |= file.extension().is_some_and(|ext| ext == "rs");
            }
            Ok(SourceEnvDepUse::RuntimeValue) => {
                return EnvDepNormalizationDecision::KeptAbsoluteRuntimeUse;
            }
            Err(e) => {
                tracing::debug!(
                    "keeping env dep {var} absolute: failed to inspect source {}: {}",
                    file.display(),
                    e
                );
                return EnvDepNormalizationDecision::KeptAbsoluteScanError;
            }
        }
    }
    if proven {
        EnvDepNormalizationDecision::NormalizedPathOnly
    } else {
        EnvDepNormalizationDecision::KeptAbsoluteNoIncludeProof
    }
}

/// Version of [`source_env_dep_use`] answers in the persistent memo. Bump it
/// whenever the scanner can classify unchanged source text differently;
/// otherwise wrappers keep reusing the old answer for every file that did not
/// change.
///
/// 2: computed env var names, `[`/`{` delimiters, comments between tokens,
/// lifetimes, nested block comments and raw C strings; answers gained the
/// include-locator state that the positive proof needs.
///
/// 3: number suffixes, non-ASCII identifier bytes, and the non-ASCII
/// whitespace and byte-order mark rustc accepts between tokens.
const SOURCE_ENV_DEP_SCANNER_VERSION: u32 = 3;

/// How one source file's text uses an env var.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceEnvDepUse {
    /// No env macro names the var, and none has a name the scanner cannot read
    /// outside an include argument.
    Unused,
    /// At least one `env!(var)` / `option_env!(var)` inside an `include*!`
    /// argument, and no use outside one.
    IncludeLocator,
    /// `env!(var)` / `option_env!(var)` outside an include argument, or an env
    /// macro outside one whose name is not a plain string literal
    /// (`env!(concat!(..))`, `env!($name)` in a forwarding macro). Such a
    /// macro may read any var, so it counts against every var.
    RuntimeValue,
}

impl SourceEnvDepUse {
    fn memo_code(self) -> i64 {
        match self {
            Self::Unused => 0,
            Self::IncludeLocator => 1,
            Self::RuntimeValue => 2,
        }
    }

    fn from_memo_code(code: i64) -> Option<Self> {
        match code {
            0 => Some(Self::Unused),
            1 => Some(Self::IncludeLocator),
            2 => Some(Self::RuntimeValue),
            _ => None,
        }
    }
}

fn source_env_dep_use(source: &str, var: &str) -> SourceEnvDepUse {
    let bytes = source.as_bytes();
    let mut i = 0usize;
    // One entry per open delimiter, true when it opens an `include*!`
    // argument. Tracking every delimiter kind keeps `include!{..}` and
    // `env![..]` in step with their closing token; the depth counter keeps
    // the include test constant-time however deep the nesting goes.
    let mut groups: Vec<bool> = Vec::new();
    let mut include_depth = 0usize;
    let mut include_locator = false;

    // Every pass consumes at least one byte, so a scan needs no more passes
    // than the source has bytes. Each loop in the scanner carries that bound:
    // it makes a change that stops advancing end with a wrong answer instead
    // of running forever, which is the difference between a test that fails
    // and a test that never finishes.
    for _ in 0..bytes.len() {
        let Some(&byte) = bytes.get(i) else { break };
        let whitespace = rust_whitespace_len(bytes, i);
        if whitespace > 0 {
            i += whitespace;
            continue;
        }
        match byte {
            b'/' if comment_starts_at(bytes, i) => i = skip_comment(bytes, i),
            b'"' => i = skip_quoted_string(bytes, i + 1),
            b'\'' => i = skip_char_literal_or_lifetime(source, i),
            b'b' | b'c' | b'r' if raw_string_starts_at(bytes, i).is_some() => {
                i = skip_raw_string(bytes, i);
            }
            b'(' | b'[' | b'{' => {
                groups.push(false);
                i += 1;
            }
            b')' | b']' | b'}' => {
                if groups.pop() == Some(true) {
                    include_depth -= 1;
                }
                i += 1;
            }
            // A number with its suffix (`1u8`, `1r`), so a suffix cannot
            // start a raw string that hides the code after it.
            b'0'..=b'9' => i = skip_ident_bytes(bytes, i),
            b if is_ident_start(b) => {
                let ident_start = i;
                i = skip_ident_bytes(bytes, i);
                let ident = &bytes[ident_start..i];
                let Some(open) = parse_macro_open(bytes, i) else {
                    continue;
                };

                if matches!(ident, b"env" | b"option_env") {
                    match parse_env_macro_name(source, open + 1) {
                        Some(name) if name != var => {}
                        Some(_) if include_depth > 0 => include_locator = true,
                        None if include_depth > 0 => {}
                        _ => return SourceEnvDepUse::RuntimeValue,
                    }
                }
                let include = is_include_macro(ident);
                groups.push(include);
                include_depth += usize::from(include);
                i = open + 1;
            }
            _ => i += 1,
        }
    }

    if include_locator {
        SourceEnvDepUse::IncludeLocator
    } else {
        SourceEnvDepUse::Unused
    }
}

fn is_include_macro(name: &[u8]) -> bool {
    matches!(name, b"include" | b"include_str" | b"include_bytes")
}

/// The var an env macro names, when its first token is a plain string literal
/// without escapes. `None` means the name is computed or spelled in a form the
/// scanner does not decode (`concat!`, `$v`, `"OUT\x5FDIR"`, raw strings).
fn parse_env_macro_name(source: &str, after_open: usize) -> Option<&str> {
    let bytes = source.as_bytes();
    let start = skip_trivia(bytes, after_open);
    if bytes.get(start) != Some(&b'"') {
        return None;
    }
    let len = bytes[start + 1..]
        .iter()
        .position(|b| matches!(b, b'"' | b'\\'))?;
    let end = start + 1 + len;
    (bytes[end] == b'"').then(|| &source[start + 1..end])
}

/// Position of the opening delimiter when `after_ident` starts `! (`, `! [`
/// or `! {`, with whitespace or comments allowed between the tokens.
fn parse_macro_open(bytes: &[u8], after_ident: usize) -> Option<usize> {
    let bang = skip_trivia(bytes, after_ident);
    if bytes.get(bang) != Some(&b'!') {
        return None;
    }
    let open = skip_trivia(bytes, bang + 1);
    matches!(bytes.get(open), Some(b'(' | b'[' | b'{')).then_some(open)
}

fn skip_trivia(bytes: &[u8], mut i: usize) -> usize {
    for _ in 0..bytes.len() {
        let whitespace = rust_whitespace_len(bytes, i);
        if whitespace > 0 {
            i += whitespace;
        } else if comment_starts_at(bytes, i) {
            i = skip_comment(bytes, i);
        } else {
            break;
        }
    }
    i
}

fn comment_starts_at(bytes: &[u8], i: usize) -> bool {
    bytes.get(i) == Some(&b'/') && matches!(bytes.get(i + 1), Some(b'/' | b'*'))
}

/// Byte length of the Rust whitespace character at `i`, or 0. Rust also
/// accepts vertical tab and a few non-ASCII `Pattern_White_Space` characters
/// between tokens; reading those as identifier bytes would hide `env` from
/// the scanner. A byte-order mark counts as whitespace wherever it appears:
/// rustc strips one only at the head of a file and rejects the rest, so the
/// extra reach concerns files that do not compile.
fn rust_whitespace_len(bytes: &[u8], i: usize) -> usize {
    match bytes.get(i..).unwrap_or_default() {
        [b'\t' | b'\n' | b'\x0B' | b'\x0C' | b'\r' | b' ', ..] => 1,
        // U+0085
        [0xC2, 0x85, ..] => 2,
        // U+200E, U+200F, U+2028, U+2029
        [0xE2, 0x80, 0x8E | 0x8F | 0xA8 | 0xA9, ..] => 3,
        // U+FEFF, which rustc strips from the head of a file.
        // See the note above on accepting it anywhere.
        [0xEF, 0xBB, 0xBF, ..] => 3,
        _ => 0,
    }
}

/// Skip the comment starting at `i`. Block comments nest in Rust, so an inner
/// `*/` must not end the outer comment.
fn skip_comment(bytes: &[u8], mut i: usize) -> usize {
    if bytes.get(i + 1) == Some(&b'/') {
        for _ in 0..bytes.len() {
            match bytes.get(i) {
                Some(b'\n') | None => break,
                Some(_) => i += 1,
            }
        }
        return i;
    }
    let mut depth = 1usize;
    i += 2;
    for _ in 0..bytes.len() {
        match (bytes.get(i), bytes.get(i + 1)) {
            (Some(b'/'), Some(b'*')) => {
                depth += 1;
                i += 2;
            }
            (Some(b'*'), Some(b'/')) => {
                depth -= 1;
                i += 2;
                if depth == 0 {
                    return i;
                }
            }
            (Some(_), _) => i += 1,
            (None, _) => break,
        }
    }
    bytes.len()
}

fn skip_quoted_string(bytes: &[u8], mut i: usize) -> usize {
    for _ in 0..bytes.len() {
        match bytes.get(i) {
            Some(b'\\') => i += 2,
            Some(b'"') => return i + 1,
            Some(_) => i += 1,
            None => break,
        }
    }
    bytes.len()
}

/// Skip the char literal starting at the quote `quote`, or only the quote
/// when it starts a lifetime or label (`'a`, `'static`). Skipping to the next
/// quote after a lifetime would hide the code in between from the scanner.
fn skip_char_literal_or_lifetime(source: &str, quote: usize) -> usize {
    let bytes = source.as_bytes();
    if bytes.get(quote + 1) == Some(&b'\\') {
        return skip_char_literal(bytes, quote + 1);
    }
    let Some(ch) = source[quote + 1..].chars().next() else {
        return bytes.len();
    };
    let close = quote + 1 + ch.len_utf8();
    if bytes.get(close) == Some(&b'\'') {
        close + 1
    } else {
        quote + 1
    }
}

/// Skip a char literal from its first content byte through the closing quote.
fn skip_char_literal(bytes: &[u8], mut i: usize) -> usize {
    for _ in 0..bytes.len() {
        match bytes.get(i) {
            Some(b'\\') => i += 2,
            Some(b'\'') => return i + 1,
            Some(_) => i += 1,
            None => break,
        }
    }
    bytes.len()
}

fn raw_string_starts_at(bytes: &[u8], i: usize) -> Option<usize> {
    let mut cursor = i;
    if matches!(bytes.get(cursor), Some(b'b' | b'c')) {
        cursor += 1;
    }
    if bytes.get(cursor) != Some(&b'r') {
        return None;
    }
    cursor += 1;
    for _ in 0..bytes.len() {
        if bytes.get(cursor) != Some(&b'#') {
            break;
        }
        cursor += 1;
    }
    if bytes.get(cursor) == Some(&b'"') {
        Some(cursor)
    } else {
        None
    }
}

fn skip_raw_string(bytes: &[u8], i: usize) -> usize {
    let Some(open_quote) = raw_string_starts_at(bytes, i) else {
        return i + 1;
    };
    let hashes = open_quote - i - usize::from(bytes[i] != b'r') - 1;
    for cursor in open_quote + 1..bytes.len() {
        if bytes[cursor] == b'"'
            && cursor + hashes < bytes.len()
            && bytes[cursor + 1..cursor + 1 + hashes]
                .iter()
                .all(|b| *b == b'#')
        {
            return cursor + hashes + 1;
        }
    }
    bytes.len()
}

/// Non-ASCII bytes count as identifier bytes: reading `éinclude` as
/// `include` would invent an include context.
fn is_ident_start(byte: u8) -> bool {
    byte == b'_' || byte.is_ascii_alphabetic() || !byte.is_ascii()
}

/// Consume identifier bytes from `i`, which the caller has already read as
/// an identifier start or a digit.
fn skip_ident_bytes(bytes: &[u8], mut i: usize) -> usize {
    for _ in 0..bytes.len() {
        let Some(&byte) = bytes.get(i) else { break };
        if !(is_ident_start(byte) || byte.is_ascii_digit()) || rust_whitespace_len(bytes, i) > 0 {
            break;
        }
        i += 1;
    }
    i
}

// `normalize_flags` (CWD-only literal-replace) used to live here.
// Replaced by `PathNormalizer` (canonical-prefix sentinel
// substitution). The ad-hoc helper had two failure modes — see
// the `path_normalizer` module docs for the full story.

/// Compute a linked `static=` archive's cache-key digest. A proven GNU/BSD
/// archive gets the structural member-identity hash, unless the invocation
/// links an archive with DWARF-bearing Mach-O members itself. Every other
/// non-thin archive gets a digest of both its bytes and lexical absolute path
/// because linkers can expose `archive-path(member)`. Thin archives are
/// uncacheable: rustc reads external members whose bytes are absent from the
/// container.
fn compute_static_lib_hash(path: &Path, usage: StaticLibUse) -> Result<String> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    if bytes.starts_with(b"!<thin>\n") {
        anyhow::bail!(
            "thin static archive {} has external members that are not modeled",
            path.display()
        );
    }
    if let Some(identity) = crate::native_archive::portable_static_archive_identity(&bytes)
        && !(usage == StaticLibUse::Linked && identity.macho_dwarf_members)
    {
        return Ok(identity.digest);
    }

    let absolute = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
    let encoded_path = absolute.as_os_str().as_encoded_bytes();
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"kache.native-ar.path-bound-fallback.v1\0");
    hasher.update(&(encoded_path.len() as u64).to_le_bytes());
    hasher.update(encoded_path);
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(&bytes);
    Ok(format!("path-ar-v1:{}", hasher.finalize().to_hex()))
}

/// Result of a dep-info pre-pass. Contains all information discovered by
/// running `rustc --emit=dep-info`.
///
/// This is a struct (not a tuple) so we can add fields later without
/// breaking call sites. Future candidates: `target_json_hash`, timing metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepInfo {
    /// All source files the crate depends on (sorted, absolute paths).
    /// Includes the crate root, module files, `include!()` targets, etc.
    pub source_files: Vec<std::path::PathBuf>,
    /// Environment variables tracked by rustc (`env!()` / `option_env!()`).
    /// Values are RAW — `compute_cache_key` decides whether to
    /// path-normalize each one based on per-var safety (see
    /// `env_dep_path_only_decision`). Storing raw values keeps that
    /// decision available to the consumer; pre-normalizing here
    /// would erase the absolute-path information the discriminator
    /// needs to read.
    pub env_deps: Vec<(String, String)>,
}

/// Version of the prediction record's own logic and encoding.
///
/// Folded into the identity AND stored in the row, so changing how a closure
/// is recorded or validated orphans the old rows without touching
/// [`CACHE_KEY_VERSION`] and therefore without invalidating a single cache
/// entry. Precedent: `STATE_SCHEMA` / `POLICY_VERSION` in
/// `incremental_policy.rs`.
pub(crate) const PREDICTION_SCHEMA: u32 = 1;

/// The input closure a previous build of one unit discovered, remembered so a
/// later build of the same unit can skip re-discovering it.
///
/// This is a *prediction*, never an authority. It records what the dep-info
/// pre-pass found, and every field has to be re-validated against the current
/// tree before a key may be derived from it. Recording is all this commit
/// does; nothing reads a record back yet.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct InputPrediction {
    /// The [`PREDICTION_SCHEMA`] that wrote the row. A reader that does not
    /// recognise it must treat the record as absent.
    pub(crate) schema: u32,
    /// Exactly the spellings [`DepInfo::source_files`] carried, so a
    /// prediction reproduces the key's `sources` group byte for byte.
    pub(crate) sources: Vec<PathBuf>,
    /// `# env-dep:` pairs as raw values. The key normalises some of them
    /// (OUT_DIR-like values collapse to a sentinel), so the raw value is the
    /// only signal that an included file moved.
    pub(crate) env_deps: Vec<(String, String)>,
    /// Digest of the crate's own tree ([`crate_tree_digest`]) when the unit
    /// depends on a proc macro. Such a macro can read any file under the crate
    /// without it entering the closure, so the closure alone cannot say
    /// whether the record still applies; the tree can. Absent on records made
    /// for units that need no such guard, and on rows written before it
    /// existed, which the guard then treats as unusable. A unit with no proc
    /// macro may carry its `OUT_DIR` guard here, which nothing checks on this
    /// row.
    #[serde(default)]
    pub(crate) tree: Option<String>,
}

impl InputPrediction {
    pub(crate) fn from_dep_info(dep_info: &DepInfo, tree: Option<String>) -> Self {
        Self {
            schema: PREDICTION_SCHEMA,
            sources: dep_info.source_files.clone(),
            env_deps: dep_info.env_deps.clone(),
            tree,
        }
    }
}

/// Version of [`PortablePrediction`]. Far from [`PREDICTION_SCHEMA`] so the
/// two decoders can never accept each other's rows.
pub(crate) const PORTABLE_PREDICTION_SCHEMA: u32 = 101;

/// A registry unit's closure with its own `OUT_DIR` written as a placeholder,
/// so another target directory can use it.
///
/// Only a registry unit, whose other inputs are the same files in every
/// checkout, and only when `OUT_DIR` holds what it held for the recorder:
/// `tree` is a digest of it, and the reader refuses the row without an equal
/// one.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct PortablePrediction {
    pub(crate) schema: u32,
    pub(crate) sources: Vec<Portable>,
    pub(crate) env_deps: Vec<(String, Portable)>,
    pub(crate) tree: String,
}

/// One recorded path or env value: as spelled, or as the bytes after
/// `OUT_DIR` ([`out_dir_suffix`]) or after the workspace root.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum Portable {
    Literal(String),
    OutDir(String),
    Workspace(String),
    /// The bytes after `<CARGO_HOME>/registry/src`. Written only for a row
    /// that travels through the remote ([`crate::prediction_share`]), so it
    /// carries no local path.
    Registry(String),
}

/// Where a portable record's relative entries land for this invocation. A
/// record with an entry whose place this invocation lacks is not used.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Places<'a> {
    pub(crate) out_dir: Option<&'a str>,
    pub(crate) workspace: Option<&'a str>,
    pub(crate) registry: Option<&'a str>,
}

/// The directories a portable record is written against.
#[derive(Debug, Clone)]
pub(crate) struct PortableRoots {
    pub(crate) out_dir: PathBuf,
    pub(crate) target: PathBuf,
    pub(crate) canonical_target: PathBuf,
    pub(crate) registry_src: PathBuf,
}

/// The closure with `OUT_DIR` made relative, or `None` when any part of it
/// could name a different file in another target directory.
///
/// A source is either under `OUT_DIR` or under the registry. Anything else
/// is refused: another unit's output, a canonical spelling of the target, a
/// checkout path, a relative path, a `..` that leaves `OUT_DIR` or the
/// package. At least one source must be under `OUT_DIR`, or the plain shared
/// record already covers the unit. An env value under `OUT_DIR` is relocated
/// too; any other value spelling the target is refused, and the rest are
/// kept as they are.
pub(crate) fn portable_prediction(
    dep_info: &DepInfo,
    roots: &PortableRoots,
    tree: Option<&str>,
) -> Option<PortablePrediction> {
    let tree = tree?;
    let sources = dep_info
        .source_files
        .iter()
        .map(|source| portable_source(source, roots))
        .collect::<Option<Vec<_>>>()?;
    if !sources
        .iter()
        .any(|source| matches!(source, Portable::OutDir(_)))
    {
        return None;
    }
    let env_deps = dep_info
        .env_deps
        .iter()
        .map(|(name, value)| Some((name.clone(), portable_env_value(value, roots)?)))
        .collect::<Option<Vec<_>>>()?;
    Some(PortablePrediction {
        schema: PORTABLE_PREDICTION_SCHEMA,
        sources,
        env_deps,
        tree: tree.to_string(),
    })
}

fn portable_source(source: &Path, roots: &PortableRoots) -> Option<Portable> {
    if let Some(suffix) = out_dir_suffix(source.as_os_str(), &roots.out_dir) {
        return Some(Portable::OutDir(suffix));
    }
    if !under_registry_src(source, &roots.registry_src) {
        return None;
    }
    Some(Portable::Literal(source.to_str()?.to_string()))
}

fn portable_env_value(value: &str, roots: &PortableRoots) -> Option<Portable> {
    if let Some(suffix) = out_dir_suffix(std::ffi::OsStr::new(value), &roots.out_dir) {
        return Some(Portable::OutDir(suffix));
    }
    let spells = |root: &Path| {
        value
            .as_bytes()
            .starts_with(root.as_os_str().as_encoded_bytes())
    };
    if spells(&roots.target) || spells(&roots.canonical_target) {
        return None;
    }
    Some(Portable::Literal(value.to_string()))
}

impl PortablePrediction {
    /// The record this invocation would have made: each relocated entry gets
    /// its root's bytes in front of its suffix. The result is then checked
    /// like any other record, raw env values included. `None` when an entry
    /// is relative to a root `places` does not have.
    pub(crate) fn resolve(&self, places: &Places<'_>) -> Option<InputPrediction> {
        let place = |portable: &Portable| match portable {
            Portable::Literal(value) => Some(value.clone()),
            Portable::OutDir(suffix) => Some(format!("{}{suffix}", places.out_dir?)),
            Portable::Workspace(suffix) => Some(format!("{}{suffix}", places.workspace?)),
            Portable::Registry(suffix) => Some(format!("{}{suffix}", places.registry?)),
        };
        Some(InputPrediction {
            schema: PREDICTION_SCHEMA,
            sources: self
                .sources
                .iter()
                .map(|source| place(source).map(PathBuf::from))
                .collect::<Option<_>>()?,
            env_deps: self
                .env_deps
                .iter()
                .map(|(name, value)| Some((name.clone(), place(value)?)))
                .collect::<Option<_>>()?,
            tree: Some(self.tree.clone()),
        })
    }
}

/// Use a relocated record: its digest must equal the guard this invocation
/// computed, and the resolved closure must pass [`validate_prediction`].
fn validate_portable_prediction(
    record: &PortablePrediction,
    guard: &str,
    places: &Places<'_>,
    stat: impl Fn(&Path) -> Option<std::fs::Metadata>,
    exists: impl Fn(&Path) -> bool,
    env_value: impl Fn(&str) -> Option<String>,
) -> std::result::Result<DepInfo, Rejection> {
    if record.tree != guard {
        return Err(Rejection::TreeChanged);
    }
    let resolved = record.resolve(places).ok_or(Rejection::NoRecord)?;
    validate_prediction(&resolved, stat, exists, env_value)
}

/// Does `content` spell any of `roots`, as-is or with each `\` doubled the
/// way a string literal escapes it?
///
/// A generated file that names the target could make rustc read a file
/// relocation does not move, so such an `OUT_DIR` is not relocated.
pub(crate) fn mentions_root(content: &[u8], roots: &[&Path]) -> bool {
    roots.iter().any(|root| {
        let raw = root.as_os_str().as_encoded_bytes();
        let doubled: Vec<u8> = raw
            .iter()
            .flat_map(|&byte| std::iter::repeat_n(byte, if byte == b'\\' { 2 } else { 1 }))
            .collect();
        crate::build_script::find_bytes(content, raw).is_some()
            || crate::build_script::find_bytes(content, &doubled).is_some()
    })
}

/// True only when every entry under `out_dir` was read and none spells any
/// of `roots`: file content and symlink text alike. Past
/// [`OUT_DIR_TREE_MAX_ENTRIES`], or on any read error, the answer is false.
fn out_dir_spells_no_root(out_dir: &Path, roots: &[&Path]) -> bool {
    fn walk(directory: &Path, roots: &[&Path], budget: &mut usize) -> Option<()> {
        for entry in std::fs::read_dir(directory).ok()? {
            let path = entry.ok()?.path();
            *budget = budget.checked_sub(1)?;
            let metadata = std::fs::symlink_metadata(&path).ok()?;
            let content = if metadata.file_type().is_symlink() {
                std::fs::read_link(&path)
                    .ok()?
                    .into_os_string()
                    .into_encoded_bytes()
            } else if metadata.is_dir() {
                walk(&path, roots, budget)?;
                continue;
            } else if metadata.is_file() {
                std::fs::read(&path).ok()?
            } else {
                return None;
            };
            if mentions_root(&content, roots) {
                return None;
            }
        }
        Some(())
    }
    let mut budget = OUT_DIR_TREE_MAX_ENTRIES;
    walk(out_dir, roots, &mut budget).is_some()
}

/// The relocated record for this invocation and the identity to file it
/// under, or `None` when the unit or its closure is not relocatable.
pub(crate) fn relocatable_record(
    args: &RustcArgs,
    dep_info: &DepInfo,
    tree: Option<&str>,
) -> Option<(String, PortablePrediction)> {
    relocatable_record_in(args, dep_info, tree, std::env::vars_os().collect())
}

/// [`relocatable_record`] against an environment snapshot.
fn relocatable_record_in(
    args: &RustcArgs,
    dep_info: &DepInfo,
    tree: Option<&str>,
    vars: Vec<(std::ffi::OsString, std::ffi::OsString)>,
) -> Option<(String, PortablePrediction)> {
    let roots = portable_roots(args, &vars)?;
    let identity = relocatable_prediction_identity(args, vars)?;
    let record = portable_prediction(dep_info, &roots, tree)?;
    out_dir_spells_no_root(&roots.out_dir, &[&roots.target, &roots.canonical_target])
        .then_some((identity, record))
}

/// Is this invocation a workspace or path unit whose records the workspace
/// guard covers ([`workspace_roots`])?
pub(crate) fn is_workspace_unit(args: &RustcArgs) -> bool {
    workspace_roots(args, &std::env::vars_os().collect::<Vec<_>>()).is_some()
}

/// The guard this checkout's own row carries. A workspace unit whose closure
/// reaches past the workspace (`relocatable` false) keeps none: the guard
/// covers the workspace only, so a macro that scans a directory outside it
/// could find a new file there with the guard unchanged. Without a guard, a
/// row of a unit with a proc-macro dependency is never used, and that unit
/// keeps the pre-pass.
pub(crate) fn same_tree_guard(
    tree: Option<String>,
    workspace_unit: bool,
    relocatable: bool,
) -> Option<String> {
    if workspace_unit && !relocatable {
        None
    } else {
        tree
    }
}

/// A workspace unit's record for another checkout and the identity to file it
/// under (kunobi-ninja/kache#1005), or `None` when the unit or its closure is
/// not relocatable. `tree` is the workspace guard taken before rustc ran.
pub(crate) fn workspace_record(
    args: &RustcArgs,
    dep_info: &DepInfo,
    tree: Option<&str>,
) -> Option<(String, PortablePrediction)> {
    let vars: Vec<_> = std::env::vars_os().collect();
    let workspace = workspace_roots(args, &vars)?;
    let record = workspace_portable_prediction(dep_info, &workspace, tree)?;
    let identity = workspace_prediction_identity(args, vars, &workspace)?;
    let named = [
        workspace.root.as_path(),
        &workspace.canonical_root,
        &workspace.target,
        &workspace.canonical_target,
    ];
    if let Some(out_dir) = &workspace.out_dir
        && !out_dir_spells_no_root(out_dir, &named)
    {
        return None;
    }
    Some((identity, record))
}

/// The closure with every source written relative to `OUT_DIR` or the
/// workspace root, or `None` when any part could name a different file in
/// another checkout: a file outside the workspace, another unit's output
/// under the target directory, a `..` that leaves the workspace.
fn workspace_portable_prediction(
    dep_info: &DepInfo,
    workspace: &WorkspaceRoots,
    tree: Option<&str>,
) -> Option<PortablePrediction> {
    let tree = tree?;
    let sources = dep_info
        .source_files
        .iter()
        .map(|source| {
            if !source.has_root() {
                return workspace_relative_source(source, workspace);
            }
            let relocated = workspace_portable_value(source.as_os_str(), workspace)?;
            (!matches!(relocated, Portable::Literal(_))).then_some(relocated)
        })
        .collect::<Option<Vec<_>>>()?;
    let env_deps = dep_info
        .env_deps
        .iter()
        .map(|(name, value)| {
            let value = workspace_portable_value(std::ffi::OsStr::new(value), workspace)?;
            Some((name.clone(), value))
        })
        .collect::<Option<Vec<_>>>()?;
    Some(PortablePrediction {
        schema: PORTABLE_PREDICTION_SCHEMA,
        sources,
        env_deps,
        tree: tree.to_string(),
    })
}

/// A source rustc reported relative to its working directory, as Cargo does
/// for a workspace member: kept as spelled, because the identity pins that
/// directory relative to the workspace root, when a lexical walk from it
/// never leaves the workspace.
fn workspace_relative_source(source: &Path, workspace: &WorkspaceRoots) -> Option<Portable> {
    let spelled = source.to_str()?;
    let from_root = format!("{}/{spelled}", workspace.cwd);
    let components = from_root.strip_prefix(['/', '\\'])?;
    (stays_below(components.split(['/', '\\']), 0) && stays_below(components.split('/'), 0))
        .then(|| Portable::Literal(spelled.to_string()))
}

/// One path or env value of a workspace unit: under `OUT_DIR`, under the
/// workspace (either spelling of its root), refused when it names the target
/// directory any other way, and otherwise kept as spelled.
fn workspace_portable_value(
    value: &std::ffi::OsStr,
    workspace: &WorkspaceRoots,
) -> Option<Portable> {
    if let Some(out_dir) = &workspace.out_dir
        && let Some(suffix) = out_dir_suffix(value, out_dir)
    {
        return Some(Portable::OutDir(suffix));
    }
    let bytes = value.as_encoded_bytes();
    let spells = |root: &Path| bytes.starts_with(root.as_os_str().as_encoded_bytes());
    if spells(&workspace.target) || spells(&workspace.canonical_target) {
        return None;
    }
    if let Some(suffix) = suffix_within(value, &workspace.root, 0)
        .or_else(|| suffix_within(value, &workspace.canonical_root, 0))
    {
        return Some(Portable::Workspace(suffix));
    }
    Some(Portable::Literal(value.to_str()?.to_string()))
}

/// This invocation's roots: its `OUT_DIR`, a target directory that exists,
/// and a registry package.
fn portable_roots(
    args: &RustcArgs,
    vars: &[(std::ffi::OsString, std::ffi::OsString)],
) -> Option<PortableRoots> {
    let out_dir = PathBuf::from(env_var_in(vars, "OUT_DIR")?);
    let target = args.target_dir()?;
    let registry_src = registry_src_root(Path::new(env_var_in(vars, "CARGO_MANIFEST_DIR")?))?;
    Some(PortableRoots {
        canonical_target: std::fs::canonicalize(&target).ok()?,
        registry_src: registry_src.to_path_buf(),
        out_dir,
        target,
    })
}

/// Why a recorded closure could not be used for this invocation.
///
/// Every variant means the same thing operationally — run the pre-pass — but
/// they are distinguished so the trace says which rule fired, and so the
/// tests can name the case they are pinning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rejection {
    /// Predictions are off, or there is no table to read one from. Also the
    /// deliberate case: the re-derivation after a predicted key misses turns
    /// them off so it discovers the closure for real.
    Disabled,
    /// The invocation is not the shape a prediction is sound for.
    NotEligible,
    /// No record, or one this build cannot read.
    NoRecord,
    /// A recorded file is gone. The pre-pass will fail too and the build will
    /// pass through to rustc's own error, which is today's behaviour.
    Missing,
    /// A recorded path is no longer a regular file.
    NotRegular,
    /// A recorded `# env-dep:` value is not what it was.
    EnvChanged,
    /// `mod foo;` now resolves ambiguously: both `foo.rs` and `foo/mod.rs`
    /// exist. rustc rejects that (E0761), and replaying a recorded success
    /// would restore an artifact for a build that should fail.
    Sibling,
    /// The unit depends on a proc macro and the crate tree is not the one the
    /// record was made against, so a file the macro reads may have changed.
    /// Also a relocated record whose `OUT_DIR` held something else.
    TreeChanged,
}

impl Rejection {
    fn as_str(self) -> &'static str {
        match self {
            Rejection::Disabled => "disabled",
            Rejection::NotEligible => "not-eligible",
            Rejection::NoRecord => "no-record",
            Rejection::Missing => "missing",
            Rejection::NotRegular => "not-regular",
            Rejection::EnvChanged => "env-changed",
            Rejection::Sibling => "sibling",
            Rejection::TreeChanged => "tree-changed",
        }
    }
}

/// Is this invocation the shape a prediction is sound for?
///
/// A proc macro can scan the filesystem and emit `include_str!` per entry, so
/// a file can enter the closure with nothing already in the closure changing.
/// The pre-pass sees the new file; a prediction would not, and would derive
/// the stored key: a false hit. Cargo does not make this assumption either —
/// it recompiles when a build script's `rerun-if-changed` directory fires
/// even if the bytes are identical.
///
/// The test is how cargo hands rustc a proc macro: as a dynamic library.
/// `dylib` crate-type dependencies get swept in too, which is
/// over-conservative and safe. This was the scoping rule of the closed
/// kunobi-ninja/kache#334, where it left 84% of units eligible.
pub(crate) fn prediction_applies(externs: &[crate::args::ExternDep]) -> bool {
    !externs.iter().any(|ext| {
        ext.path.as_deref().is_some_and(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    matches!(
                        crate::compiler::classify_by_filename(name),
                        crate::compiler::ArtifactKind::DynamicLibrary
                    )
                })
        })
    })
}

/// The other spelling of the same module, if this file is one half of a
/// `mod foo;` pair.
///
/// `src/foo.rs` and `src/foo/mod.rs` both answer `mod foo;`, and rustc
/// refuses to choose (E0761). A record made when only one existed must not
/// replay success after the other appears.
fn mod_sibling_candidate(file: &Path) -> Option<PathBuf> {
    let stem = file.file_stem()?.to_str()?;
    let parent = file.parent()?;
    if file.extension().and_then(|e| e.to_str()) != Some("rs") {
        return None;
    }
    match stem {
        // `foo/mod.rs`: the other spelling is `foo.rs` beside the directory.
        "mod" => Some(
            parent
                .parent()?
                .join(parent.file_name()?)
                .with_extension("rs"),
        ),
        // A crate root is named by argv, not by a `mod` item, so it has no
        // sibling spelling to be ambiguous with.
        "lib" | "main" => None,
        // `foo.rs`: the other spelling is `foo/mod.rs`.
        stem => Some(parent.join(stem).join("mod.rs")),
    }
}

/// Turn a recorded closure back into a `DepInfo` this invocation may key off,
/// or say why it cannot.
///
/// Everything is re-checked against the tree as it is now. The claim being
/// tested is narrow: *any edit that adds a file to the closure also changes a
/// file already in it*. Where that does not hold, one of the rules above
/// catches it, and where none of them does, the derived key misses and the
/// caller falls back before anything is stored.
pub(crate) fn validate_prediction(
    record: &InputPrediction,
    stat: impl Fn(&Path) -> Option<std::fs::Metadata>,
    exists: impl Fn(&Path) -> bool,
    env_value: impl Fn(&str) -> Option<String>,
) -> std::result::Result<DepInfo, Rejection> {
    for file in &record.sources {
        let Some(metadata) = stat(file) else {
            return Err(Rejection::Missing);
        };
        if !metadata.is_file() {
            return Err(Rejection::NotRegular);
        }
        if mod_sibling_candidate(file).is_some_and(|sibling| exists(&sibling)) {
            return Err(Rejection::Sibling);
        }
    }
    for (var, recorded) in &record.env_deps {
        // Raw values, because the key normalises OUT_DIR-like ones to a
        // sentinel: the raw value is the only signal that an included file
        // moved. A value that is no longer valid UTF-8 reads as changed,
        // which is the safe direction.
        //
        // The parser conflates an unset variable with an empty one (both
        // arrive as `""`), so validation must too: otherwise any crate
        // reading an unset `option_env!` would reject every record and pay
        // a pre-pass on every warm build.
        let matches = match env_value(var) {
            Some(value) => value == *recorded,
            None => recorded.is_empty(),
        };
        if !matches {
            return Err(Rejection::EnvChanged);
        }
    }
    Ok(DepInfo {
        source_files: record.sources.clone(),
        env_deps: record.env_deps.clone(),
    })
}

/// The parts of an invocation that decide which closure it will discover.
///
/// Everything here shapes what rustc reads: the compiler, the argv, the
/// directory paths resolve against, and the environment the key already
/// folds. Two invocations agreeing on all of it discover the same files, so
/// they may share a record. Anything that disagrees gets its own row rather
/// than a wrong answer.
///
/// Deliberately absent: extern *content* hashes. A changed dependency changes
/// the derived key on its own, and re-deriving is what refreshes the record.
/// Present but acknowledged: extern *paths*, which make a row target-dir
/// local. A fresh worktree therefore pays one pre-pass per unit until the
/// identity is path-normalised.
pub(crate) struct PredictionIdentityParts<'a> {
    pub(crate) rustc_version: &'a str,
    pub(crate) inner_rustc: Option<&'a Path>,
    pub(crate) current_dir: Option<&'a Path>,
    pub(crate) source_file: &'a Path,
    pub(crate) closure_args: &'a [String],
    pub(crate) skip_path_remap: bool,
}

/// Environment folded into a prediction identity, by name.
///
/// Named rather than wholesale, because the cc preprocessor memo spent a year
/// never hitting once for exactly this mistake: it folded the whole
/// environment, so `_`, `PWD` and the jobserver fds made every key unique
/// (kunobi-ninja/kache#927). Only variables that change what rustc reads
/// belong here.
const PREDICTION_ENV: &[&str] = &[
    "RUSTFLAGS",
    "CARGO_ENCODED_RUSTFLAGS",
    "RUSTC_BOOTSTRAP",
    "OUT_DIR",
    "CARGO_MANIFEST_DIR",
];

/// Identity of the unit whose closure a record describes.
///
/// Length-prefixed like the cache key's own fields, so no combination of
/// values can be re-read as a different combination.
///
/// Takes the environment rather than reading it, so the folding can be tested
/// against a fixed one. `vars_os` would otherwise be read twice in a test
/// binary whose other tests set and unset variables concurrently.
fn prediction_identity_in_env(
    parts: &PredictionIdentityParts<'_>,
    vars: Vec<(std::ffi::OsString, std::ffi::OsString)>,
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"kache-input-prediction-v1\n");
    fold_field(
        &mut hasher,
        b"prediction_schema:",
        PREDICTION_SCHEMA.to_string().as_bytes(),
    );
    fold_field(
        &mut hasher,
        b"key_version:",
        CACHE_KEY_VERSION.to_string().as_bytes(),
    );
    fold_field(
        &mut hasher,
        b"rustc_version:",
        parts.rustc_version.as_bytes(),
    );
    fold_field(
        &mut hasher,
        b"inner_rustc:",
        &parts
            .inner_rustc
            .map(|path| env_os_key_bytes(path.as_os_str()))
            .unwrap_or_default(),
    );
    // Relative paths in the argv resolve against the working directory, so two
    // directories are two closures even with identical arguments.
    fold_field(
        &mut hasher,
        b"current_dir:",
        &parts
            .current_dir
            .map(|path| env_os_key_bytes(path.as_os_str()))
            .unwrap_or_default(),
    );
    fold_field(
        &mut hasher,
        b"source_file:",
        &env_os_key_bytes(parts.source_file.as_os_str()),
    );
    fold_field(
        &mut hasher,
        b"closure_args_len:",
        parts.closure_args.len().to_string().as_bytes(),
    );
    for arg in parts.closure_args {
        fold_field(&mut hasher, b"closure_arg:", arg.as_bytes());
    }
    let by_name: std::collections::BTreeMap<Vec<u8>, &std::ffi::OsString> = vars
        .iter()
        .map(|(name, value)| (env_text_key_bytes(name), value))
        .collect();
    for name in PREDICTION_ENV {
        fold_field(&mut hasher, b"env_var:", name.as_bytes());
        match by_name.get(name.as_bytes()) {
            Some(value) => {
                fold_field(&mut hasher, b"env_set:", b"1");
                fold_field(&mut hasher, b"env_val:", &env_os_key_bytes(value));
            }
            // An unset variable is not an empty one: `env!` distinguishes them.
            None => fold_field(&mut hasher, b"env_set:", b"0"),
        }
    }
    for (name, value) in cargo_cfg_pairs(vars.iter().cloned()) {
        fold_field(&mut hasher, b"cargo_cfg_name:", &env_text_key_bytes(&name));
        fold_field(&mut hasher, b"cargo_cfg_val:", &env_os_key_bytes(&value));
    }
    fold_field(
        &mut hasher,
        b"skip_path_remap:",
        if parts.skip_path_remap { b"1" } else { b"0" },
    );
    hasher.finalize().to_hex().to_string()
}

/// Thin abstraction over file hashing.
///
/// When backed by the persistent index DB, hashes are memoized by
/// `(absolute path, mtime, ctime, size)` across wrapper processes. In a workspace
/// with 30 crates that all depend on `serde`, the serde rlib gets hashed once
/// instead of 30 times.
pub struct FileHasher<'db> {
    cache: Option<FileHashCache<'db>>,
    daemon_socket: Option<PathBuf>,
    use_input_predictions: bool,
    prediction_flight_dir: Option<PathBuf>,
    discovery_flight: RefCell<Option<crate::store::StoreLock>>,
    prefetched: RefCell<HashMap<FileFingerprint, PrefetchedHash>>,
    recent_hashes: RefCell<HashMap<PathBuf, RecentHash>>,
    env_dep_uses: RefCell<HashMap<(String, String), SourceEnvDepUse>>,
    stats: FileHashStatsCells,
    too_new: TooNewGuard,
    /// Fingerprints of every file hashed while the too-new guard was armed.
    /// Drained after the compile so the wrapper can prove clock-independently
    /// that none of them changed mid-build (see
    /// [`FileHasher::guarded_inputs_unchanged_since_hash`]).
    guard_inputs: RefCell<Vec<FileFingerprint>>,
    /// Memo rows for files hashed in this process, written in one transaction
    /// by [`FileHasher::flush_memo`] (and on drop). One autocommit write per
    /// file made every hit in a six-job cold cell wait for the index's write
    /// lock behind the misses' store transactions: 6 ms of hashing became
    /// 380 ms.
    pending_memo: RefCell<Vec<(FileFingerprint, String)>>,
}

impl Drop for FileHasher<'_> {
    fn drop(&mut self) {
        self.flush_memo();
    }
}

/// Optional "too-new input" guard (kunobi-ninja/kache#324). When armed, any
/// hashed input whose mtime/ctime falls within `margin_ns` of the build's start
/// is flagged: its content at hash time may differ from what the compiler reads,
/// so the wrapper treats the invocation as non-cacheable (it still looks up, but
/// refuses to store). Disabled when `invocation_start_ns == 0` (the default).
#[derive(Default)]
struct TooNewGuard {
    invocation_start_ns: i64,
    margin_ns: i64,
    saw_too_new: Cell<bool>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct FileHashStats {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub bytes_hashed: u64,
}

#[derive(Default)]
struct FileHashStatsCells {
    cache_hits: Cell<u64>,
    cache_misses: Cell<u64>,
    bytes_hashed: Cell<u64>,
}

#[derive(Debug, Clone)]
struct PrefetchedHash {
    hash: String,
    cache_hit: bool,
    bytes_hashed: u64,
}

#[derive(Clone)]
struct RecentHash {
    hash: String,
    fingerprint: Option<FileFingerprint>,
}

impl FileHasher<'static> {
    pub fn new() -> Self {
        FileHasher {
            cache: None,
            daemon_socket: None,
            use_input_predictions: false,
            prediction_flight_dir: None,
            discovery_flight: RefCell::new(None),
            prefetched: RefCell::new(HashMap::new()),
            recent_hashes: RefCell::new(HashMap::new()),
            env_dep_uses: RefCell::new(HashMap::new()),
            stats: FileHashStatsCells::default(),
            too_new: TooNewGuard::default(),
            guard_inputs: RefCell::new(Vec::new()),
            pending_memo: RefCell::new(Vec::new()),
        }
    }

    #[cfg(test)]
    pub fn persistent(index_db_path: &Path) -> Self {
        match FileHashCache::open(index_db_path) {
            Ok(cache) => FileHasher {
                cache: Some(cache),
                daemon_socket: None,
                use_input_predictions: false,
                prediction_flight_dir: None,
                discovery_flight: RefCell::new(None),
                prefetched: RefCell::new(HashMap::new()),
                recent_hashes: RefCell::new(HashMap::new()),
                env_dep_uses: RefCell::new(HashMap::new()),
                stats: FileHashStatsCells::default(),
                too_new: TooNewGuard::default(),
                guard_inputs: RefCell::new(Vec::new()),
                pending_memo: RefCell::new(Vec::new()),
            },
            Err(e) => {
                tracing::debug!(
                    "file hash cache disabled for {}: {e}",
                    index_db_path.display()
                );
                FileHasher::new()
            }
        }
    }
}

impl<'db> FileHasher<'db> {
    /// Write every memo row hashed so far in one transaction. The rows are an
    /// optimisation, so a busy index (another process holds the write lock
    /// for longer than the short wait here) drops them rather than stalling
    /// a hit; the next process hashes those files again.
    pub fn flush_memo(&self) {
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| {
                i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX)
            });
        self.flush_memo_at(now_ns);
    }

    /// [`flush_memo`](Self::flush_memo) with the clock supplied. A file that
    /// changed within [`HASH_SETTLE_NS`] of `now_ns` is left out:
    /// its hash was right for this process, but a second write in the same
    /// timestamp tick would leave a row that no stamp check could catch.
    /// Flush as if every pending file had been left alone for the settle
    /// window, for tests that write a file and then expect its row.
    #[cfg(test)]
    pub(crate) fn flush_memo_as_if_settled(&self) {
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| {
                i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX)
            });
        self.flush_memo_at(now_ns.saturating_add(HASH_SETTLE_NS));
    }

    pub(crate) fn flush_memo_at(&self, now_ns: i64) {
        let mut pending = std::mem::take(&mut *self.pending_memo.borrow_mut());
        pending.retain(|(fingerprint, _)| stamp_is_settled(fingerprint, now_ns));
        if pending.is_empty() {
            return;
        }
        let _trace = crate::phase_trace::phase("memo_flush");
        let Some(cache) = &self.cache else {
            return;
        };
        let db = cache.db();
        let _ = db.busy_timeout(std::time::Duration::from_millis(100));
        let written = (|| -> rusqlite::Result<()> {
            db.execute_batch("BEGIN IMMEDIATE")?;
            for (fingerprint, hash) in &pending {
                if let Err(error) = cache.put(fingerprint, hash) {
                    let _ = db.execute_batch("ROLLBACK");
                    return Err(error);
                }
            }
            db.execute_batch("COMMIT")
        })();
        let _ = db.busy_timeout(std::time::Duration::from_millis(5000));
        if let Err(error) = written {
            tracing::debug!(
                rows = pending.len(),
                "file hash memo not written (index busy): {error}"
            );
        }
    }

    pub(crate) fn from_cache(cache: FileHashCache<'db>) -> Self {
        FileHasher {
            cache: Some(cache),
            daemon_socket: None,
            use_input_predictions: false,
            prediction_flight_dir: None,
            discovery_flight: RefCell::new(None),
            prefetched: RefCell::new(HashMap::new()),
            recent_hashes: RefCell::new(HashMap::new()),
            env_dep_uses: RefCell::new(HashMap::new()),
            stats: FileHashStatsCells::default(),
            too_new: TooNewGuard::default(),
            guard_inputs: RefCell::new(Vec::new()),
            pending_memo: RefCell::new(Vec::new()),
        }
    }

    pub(crate) fn with_daemon(mut self, socket_path: PathBuf) -> Self {
        self.daemon_socket = Some(socket_path);
        self
    }

    /// Let key computation derive its input set from a recorded closure
    /// instead of spawning the dep-info pre-pass.
    ///
    /// A property of the hasher because the hasher is what reaches the
    /// prediction table: without an index DB there is nothing to read, and
    /// asking is always allowed to answer "run the pre-pass".
    pub(crate) fn with_input_predictions(mut self, enabled: bool) -> Self {
        self.use_input_predictions = enabled;
        self
    }

    pub(crate) fn with_prediction_flights(mut self, cache_dir: Option<PathBuf>) -> Self {
        self.prediction_flight_dir = cache_dir;
        self
    }

    pub(crate) fn take_discovery_flight(&self) -> Option<crate::store::StoreLock> {
        self.discovery_flight.borrow_mut().take()
    }

    /// May key computation derive its inputs from a record? Only when it was
    /// asked to AND there is a table to read.
    fn uses_input_predictions(&self) -> bool {
        self.use_input_predictions && self.cache.is_some()
    }

    /// Arm the too-new-input guard (kunobi-ninja/kache#324): flag any subsequently
    /// hashed input whose mtime/ctime is within `margin_ns` of `invocation_start_ns`
    /// (the build's wall-clock start). A `start` of 0 leaves the guard disabled.
    pub fn arm_too_new_guard(&mut self, invocation_start_ns: i64, margin_ns: i64) {
        self.too_new.invocation_start_ns = invocation_start_ns;
        self.too_new.margin_ns = margin_ns;
    }

    /// Whether any hashed input was "too new" since the guard was armed.
    pub fn too_new(&self) -> bool {
        self.too_new.saw_too_new.get()
    }

    /// Keep `fingerprint` for the post-compile revalidation, when the guard
    /// is armed.
    fn guard_input(&self, fingerprint: &FileFingerprint) {
        if self.too_new.invocation_start_ns > 0 {
            self.guard_inputs.borrow_mut().push(fingerprint.clone());
        }
    }

    /// Drain the fingerprints hashed while the guard was armed. The wrapper
    /// carries them past the compile and hands them to
    /// [`FileHasher::guarded_inputs_unchanged_since_hash`].
    pub fn take_guarded_inputs(&self) -> Vec<FileFingerprint> {
        std::mem::take(&mut *self.guard_inputs.borrow_mut())
    }

    /// Clock-independent proof that guarded inputs did not change since they
    /// were hashed: every recorded fingerprint still matches a fresh stat and
    /// carries a strong identity. Comparing a file's metadata against itself
    /// never orders either side against the host clock, so this stays valid
    /// when the filesystem lives in another clock domain (NFS skew, a fresh
    /// checkout with future mtimes) where the wall-clock guard misfires.
    ///
    /// Fails closed: an empty set, a missing or changed file, or an input
    /// without an inode (non-Unix, where replace-by-rename is invisible)
    /// never excuses a tripped guard.
    pub fn guarded_inputs_unchanged_since_hash(inputs: &[FileFingerprint]) -> bool {
        if inputs.is_empty() {
            return false;
        }
        inputs.iter().all(|expected| {
            expected.inode != 0
                && FileFingerprint::from_path(Path::new(&expected.path))
                    .is_ok_and(|current| current == *expected)
        })
    }

    fn note_too_new(&self, fingerprint: &FileFingerprint) {
        if self.too_new.invocation_start_ns > 0 {
            let threshold = self.too_new.invocation_start_ns - self.too_new.margin_ns;
            if fingerprint.mtime_ns >= threshold || fingerprint.ctime_ns >= threshold {
                self.too_new.saw_too_new.set(true);
            }
        }
    }

    pub fn stats(&self) -> FileHashStats {
        FileHashStats {
            cache_hits: self.stats.cache_hits.get(),
            cache_misses: self.stats.cache_misses.get(),
            bytes_hashed: self.stats.bytes_hashed.get(),
        }
    }

    /// Whether this hasher can persist C/C++ preprocessor memo records.
    pub(crate) fn supports_cc_preprocess_memo(&self) -> bool {
        self.cache.is_some()
    }

    /// Is there anywhere to keep a prediction record? Without the index DB
    /// (the daemon's store-free hasher) there is not, and the caller keeps
    /// running the pre-pass.
    pub(crate) fn supports_input_predictions(&self) -> bool {
        self.cache.is_some()
    }

    /// True only when the local store is known to hold no entry for this
    /// unit: `crate_name` under Cargo's `-C metadata` hash, or any unit of
    /// that name when the hash is absent. No store, or a failed query, is
    /// "unknown": false.
    fn store_lacks_unit(&self, crate_name: &str, unit: &str) -> bool {
        let _trace = crate::phase_trace::phase("crate_presence");
        let Some(cache) = self.cache.as_ref() else {
            return false;
        };
        match cache.has_entry_for_unit(crate_name, unit) {
            Ok(present) => !present,
            Err(error) => {
                tracing::debug!("crate presence lookup failed: {error}");
                false
            }
        }
    }

    /// Remember the closure this build discovered for `identity`.
    ///
    /// Best-effort: a record is an optimisation, so a database that will not
    /// take it costs a future pre-pass and nothing else. Never called with a
    /// closure the pre-pass failed to produce — that is the one input set
    /// that must not be remembered (kunobi-ninja/kache#323).
    pub(crate) fn record_input_prediction(
        &self,
        identity: &str,
        crate_name: Option<&str>,
        dep_info: &DepInfo,
        tree: Option<String>,
    ) {
        let Some(cache) = self.cache.as_ref() else {
            return;
        };
        let record = InputPrediction::from_dep_info(dep_info, tree);
        let json = match serde_json::to_string(&record) {
            Ok(json) => json,
            Err(error) => {
                tracing::debug!("input prediction encode failed: {error}");
                return;
            }
        };
        if let Err(error) = cache.put_input_prediction(identity, record.schema, crate_name, &json) {
            tracing::debug!("input prediction record failed: {error}");
        }
    }

    /// The closure recorded for `identity`, if this build can still read it.
    ///
    /// The Rust prediction path validates this closure before deriving a key.
    /// Missing rows, unknown schemas, and undecodable records return `None`,
    /// so the caller runs the pre-pass.
    ///
    pub(crate) fn input_prediction(&self, identity: &str) -> Option<InputPrediction> {
        let _trace = crate::phase_trace::phase("prediction_read");
        let cache = self.cache.as_ref()?;
        let (schema, json) = match cache.get_input_prediction(identity) {
            Ok(row) => row?,
            Err(error) => {
                tracing::debug!("input prediction lookup failed: {error}");
                return None;
            }
        };
        if schema != PREDICTION_SCHEMA {
            return None;
        }
        match serde_json::from_str::<InputPrediction>(&json) {
            Ok(record) if record.schema == PREDICTION_SCHEMA => Some(record),
            Ok(_) => None,
            Err(error) => {
                tracing::debug!("input prediction decode failed: {error}");
                None
            }
        }
    }

    /// Remember a relocated closure. Best-effort, like
    /// [`FileHasher::record_input_prediction`].
    pub(crate) fn record_portable_prediction(
        &self,
        identity: &str,
        crate_name: Option<&str>,
        record: &PortablePrediction,
    ) {
        let Some(cache) = self.cache.as_ref() else {
            return;
        };
        let json = match serde_json::to_string(record) {
            Ok(json) => json,
            Err(error) => {
                tracing::debug!("portable prediction encode failed: {error}");
                return;
            }
        };
        if let Err(error) = cache.put_input_prediction(identity, record.schema, crate_name, &json) {
            tracing::debug!("portable prediction record failed: {error}");
        }
    }

    /// The relocated closure recorded for `identity`, read under the same
    /// rules as [`FileHasher::input_prediction`].
    pub(crate) fn portable_prediction(&self, identity: &str) -> Option<PortablePrediction> {
        let _trace = crate::phase_trace::phase("prediction_read");
        let cache = self.cache.as_ref()?;
        let (schema, json) = match cache.get_input_prediction(identity) {
            Ok(row) => row?,
            Err(error) => {
                tracing::debug!("portable prediction lookup failed: {error}");
                return None;
            }
        };
        if schema != PORTABLE_PREDICTION_SCHEMA {
            return None;
        }
        match serde_json::from_str::<PortablePrediction>(&json) {
            Ok(record) if record.schema == PORTABLE_PREDICTION_SCHEMA => Some(record),
            Ok(_) => None,
            Err(error) => {
                tracing::debug!("portable prediction decode failed: {error}");
                None
            }
        }
    }

    /// Reuse a preprocessor-output hash only when every source and header the
    /// probe read still holds the same bytes.
    ///
    /// Metadata first, because identical metadata needs no read. When it
    /// differs the file is hashed and compared, so bytes that merely moved
    /// (another worktree, a fresh checkout) or were rewritten unchanged (a
    /// build script regenerating a header) still hit.
    ///
    /// The too-new guard deliberately does not apply. It exists because
    /// metadata cannot tell a file written a moment ago from one still being
    /// written; a content hash can, because a file that changes afterwards
    /// simply fails the next comparison. Any database, decoding, or metadata
    /// uncertainty is still a miss.
    /// Whether any memo is recorded under `memo_key`, whatever its inputs
    /// say now. Decides between compiling first (nothing recorded) and
    /// rediscovering the read set with the preprocessor (a stale record).
    pub(crate) fn cc_preprocess_memo_recorded(&self, memo_key: &str) -> bool {
        self.cache
            .as_ref()
            .is_some_and(|cache| matches!(cache.get_cc_preprocess_memo(memo_key), Ok(Some(_))))
    }

    pub(crate) fn cc_preprocess_memo_lookup(
        &self,
        memo_key: &str,
        resolve: impl Fn(&str) -> Vec<PathBuf>,
        mapped_content: &impl Fn(&Path) -> Option<String>,
    ) -> Option<(String, Vec<PathBuf>)> {
        let cache = self.cache.as_ref()?;
        let record = match cache.get_cc_preprocess_memo(memo_key) {
            Ok(record) => record?,
            Err(error) => {
                tracing::debug!("cc preprocess memo lookup failed: {error}");
                return None;
            }
        };
        // A `pb:` prefix marks a path-bound read set; the digest follows.
        let digest = record
            .preprocessed_hash
            .strip_prefix("pb:")
            .unwrap_or(&record.preprocessed_hash);
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            tracing::debug!("cc preprocess memo hash is invalid");
            return None;
        }
        let inputs = record.inputs;
        if inputs.is_empty() {
            return None;
        }
        // The paths that satisfied the memo are the inputs this invocation
        // would have discovered by preprocessing, and the caller needs them to
        // resolve include shadowing without a fresh dependency capture.
        let mut satisfied = Vec::with_capacity(inputs.len());
        for expected in &inputs {
            satisfied.push(self.memo_input_is_unchanged(expected, &resolve, mapped_content)?);
        }
        if record.needs_touch
            && let Err(error) = cache.touch_cc_preprocess_memo(memo_key)
        {
            tracing::debug!("cc preprocess memo touch failed: {error}");
        }
        Some((record.preprocessed_hash, satisfied))
    }

    /// Does this input still hold the bytes the memo was recorded against?
    ///
    /// Identical metadata answers yes without a read. Otherwise the file is
    /// hashed through the ordinary content cache, so a header shared by many
    /// translation units is read once per build rather than once per unit.
    fn memo_input_is_unchanged(
        &self,
        expected: &CcPreprocessMemoInput,
        resolve: &impl Fn(&str) -> Vec<PathBuf>,
        mapped_content: &impl Fn(&Path) -> Option<String>,
    ) -> Option<PathBuf> {
        // Only where THIS invocation resolves the recorded name. The path the
        // recording checkout used is not a candidate on its own merit: it may
        // still exist, unmodified, while the tree being compiled now has an
        // edited copy at the same mapped name. Trusting it let a modified
        // source hit, which the relocate-modified e2e phase exists to catch.
        // When the two trees are the same, the resolver returns that path
        // anyway and the metadata comparison below still avoids the read.
        let candidates = resolve(&expected.name);

        for path in &candidates {
            let Ok(current) = FileFingerprint::from_path(path) else {
                continue;
            };
            self.note_too_new(&current);
            // Cheapest first: identical metadata needs no read, identical raw
            // bytes come from the content cache, and only a file differing in
            // both is read through the maps.
            if current == expected.fingerprint {
                return Some(path.clone());
            }
            if self
                .hash(path)
                .is_ok_and(|content| content == expected.content)
            {
                return Some(path.clone());
            }
            if !expected.mapped.is_empty()
                && mapped_content(path).is_some_and(|mapped| mapped == expected.mapped)
            {
                return Some(path.clone());
            }
        }
        tracing::debug!(
            "cc preprocess memo input {} matched none of {} candidate paths",
            expected.name,
            candidates.len()
        );
        None
    }

    /// Capture the source/header metadata and contents observed immediately
    /// after a full preprocess probe. The caller revalidates this snapshot
    /// after a successful compile or restore before committing it.
    ///
    /// Hashing here is what the memo is validated against later. It is not
    /// free on a cold build, but every hash goes through the content cache,
    /// so a header included by many translation units is read once.
    /// Fingerprint every file a preprocessor run read, under its mapped name.
    ///
    /// The raw content hash comes from the file-hash memo by stamp. The
    /// mapped hash (the bytes with this invocation's prefix maps applied) is
    /// memoised by raw hash and map set in the same index, so the headers a
    /// build's translation units share are read and rewritten once per map
    /// set rather than once per unit; `maps_key` names the map set.
    pub(crate) fn cc_preprocess_fingerprints(
        &self,
        paths: &[(String, PathBuf)],
        maps_key: &str,
        mapped_content: &impl Fn(&Path) -> Option<String>,
    ) -> Option<Vec<CcPreprocessMemoInput>> {
        if paths.is_empty() {
            return None;
        }
        let _trace = crate::phase_trace::phase("cc_fingerprints");
        // Every stamp first, then one lookup for all of them. A translation
        // unit reads a couple of hundred headers, most of them the same ones
        // its neighbours read; opening and hashing each again cost more than
        // the compile's own header work did on macOS.
        let mut stamped: Vec<(&String, &PathBuf, FileFingerprint)> =
            Vec::with_capacity(paths.len());
        for (name, path) in paths {
            let fingerprint = match FileFingerprint::from_path(path) {
                Ok(fingerprint) => fingerprint,
                Err(error) => {
                    tracing::debug!(
                        "cc preprocess memo input {} could not be fingerprinted: {error}",
                        path.display()
                    );
                    return None;
                }
            };
            self.note_too_new(&fingerprint);
            stamped.push((name, path, fingerprint));
        }
        let memoised = self.memoised_hashes(stamped.iter().map(|(_, _, stamp)| stamp));
        let mut pending: Vec<(String, FileFingerprint, String, PathBuf)> =
            Vec::with_capacity(stamped.len());
        for (name, path, fingerprint) in stamped {
            let content = match self.header_hash(path, &fingerprint, &memoised) {
                Ok(content) => content,
                Err(error) => {
                    tracing::debug!(
                        "cc preprocess memo input {} could not be hashed: {error}",
                        path.display()
                    );
                    return None;
                }
            };
            pending.push((name.clone(), fingerprint, content, path.clone()));
        }
        let memo = self.cache.as_ref().filter(|_| !maps_key.is_empty());
        let known = match memo {
            Some(cache) => {
                let contents: Vec<&str> = pending.iter().map(|p| p.2.as_str()).collect();
                cache
                    .get_cc_mapped_hashes(maps_key, &contents)
                    .unwrap_or_else(|error| {
                        tracing::debug!("cc mapped hash memo lookup failed: {error}");
                        Default::default()
                    })
            }
            None => Default::default(),
        };
        let mut known = known;
        let mut learned: Vec<(String, String)> = Vec::new();
        let mut inputs = Vec::with_capacity(pending.len());
        for (name, fingerprint, content, path) in pending {
            let mapped = match known.get(&content) {
                Some(mapped) => mapped.clone(),
                None => {
                    let mapped = mapped_content(&path)?;
                    known.insert(content.clone(), mapped.clone());
                    learned.push((content.clone(), mapped.clone()));
                    mapped
                }
            };
            inputs.push(CcPreprocessMemoInput {
                name,
                fingerprint,
                content,
                mapped,
            });
        }
        if let Some(cache) = memo
            && let Err(error) = cache.put_cc_mapped_hashes(maps_key, &learned)
        {
            tracing::debug!("cc mapped hash memo update failed: {error}");
        }
        inputs.sort_by(|a, b| a.name.cmp(&b.name));
        inputs.dedup_by(|a, b| a.name == b.name);
        Some(inputs)
    }

    /// The first input whose raw text hides a file the assembler would read,
    /// scanning each distinct content once: the verdict is memoised by raw
    /// content hash, so a header shared by many units is read once. `scan`
    /// returns `None` for a file it cannot read (skipped, not recorded),
    /// `Some(None)` for a clean file, `Some(Some(construct))` otherwise.
    pub(crate) fn cc_inputs_hide_assembler_input(
        &self,
        inputs: &[CcPreprocessMemoInput],
        scan: &impl Fn(&Path) -> Option<Option<&'static str>>,
    ) -> Option<String> {
        let _trace = crate::phase_trace::phase("cc_asm_scan");
        let known = match &self.cache {
            Some(cache) => {
                let contents: Vec<&str> = inputs.iter().map(|i| i.content.as_str()).collect();
                cache.get_cc_asm_scans(&contents).unwrap_or_else(|error| {
                    tracing::debug!("cc assembler scan memo lookup failed: {error}");
                    Default::default()
                })
            }
            None => Default::default(),
        };
        let mut known = known;
        let mut learned: Vec<(String, String)> = Vec::new();
        let mut found: Option<String> = None;
        for input in inputs {
            let verdict = match known.get(&input.content) {
                Some(construct) => construct.clone(),
                None => {
                    let Some(scanned) = scan(Path::new(&input.fingerprint.path)) else {
                        continue;
                    };
                    let construct = scanned.unwrap_or("").to_string();
                    known.insert(input.content.clone(), construct.clone());
                    learned.push((input.content.clone(), construct.clone()));
                    construct
                }
            };
            if !verdict.is_empty() && found.is_none() {
                found = Some(verdict);
            }
        }
        if let Some(cache) = &self.cache
            && let Err(error) = cache.put_cc_asm_scans(&learned)
        {
            tracing::debug!("cc assembler scan memo update failed: {error}");
        }
        found
    }

    /// Commit a pending preprocessor memo after proving its inputs held the
    /// same bytes through the successful compiler/restore boundary.
    ///
    /// An input written during this build no longer blocks the record. What
    /// it was blocking is a torn read, and a torn read is caught where it
    /// matters: the recorded hash is of whatever bytes were there, so the
    /// finished file simply fails the next comparison and the expansion is
    /// recomputed. Refusing to record instead meant a fresh checkout — every
    /// CI runner, every new worktree — could never memoise anything at all.
    pub(crate) fn cc_preprocess_memo_record_if_unchanged(
        &self,
        memo_key: &str,
        preprocessed_hash: &str,
        inputs: &[CcPreprocessMemoInput],
        mapped_content: &impl Fn(&Path) -> Option<String>,
    ) {
        let Some(cache) = &self.cache else {
            return;
        };
        if inputs.is_empty() {
            return;
        }
        for expected in inputs {
            // Recording happens in the checkout that just ran the preprocess,
            // so each name resolves to the path it was captured from.
            let recorded_path = PathBuf::from(&expected.fingerprint.path);
            if self
                .memo_input_is_unchanged(
                    expected,
                    &|_: &str| vec![recorded_path.clone()],
                    mapped_content,
                )
                .is_none()
            {
                return;
            }
        }
        if let Err(error) = cache.put_cc_preprocess_memo_inputs(memo_key, preprocessed_hash, inputs)
        {
            tracing::debug!("cc preprocess memo update failed: {error}");
        }
    }

    pub fn prefetch(&self, paths: &[&Path]) {
        let _trace = crate::phase_trace::phase("input_hash_prefetch");
        let Some(socket_path) = &self.daemon_socket else {
            return;
        };

        let mut requests = Vec::new();
        for path in paths {
            let Ok(fingerprint) = FileFingerprint::from_path(path) else {
                continue;
            };
            if fingerprint.size < MIN_PERSISTED_HASH_BYTES
                || self.prefetched.borrow().contains_key(&fingerprint)
            {
                continue;
            }
            requests.push(crate::daemon::HashFileRequest {
                path: fingerprint.path,
                size: fingerprint.size,
                mtime_ns: fingerprint.mtime_ns,
                ctime_ns: fingerprint.ctime_ns,
                inode: fingerprint.inode,
            });
        }

        if requests.is_empty() {
            return;
        }

        match crate::daemon::send_hash_files_request(socket_path, requests) {
            Ok(results) => {
                let mut prefetched = self.prefetched.borrow_mut();
                for result in results {
                    let Some(hash) = result.hash else {
                        continue;
                    };
                    prefetched.insert(
                        FileFingerprint {
                            path: result.path,
                            size: result.size,
                            mtime_ns: result.mtime_ns,
                            ctime_ns: result.ctime_ns,
                            inode: result.inode,
                        },
                        PrefetchedHash {
                            hash,
                            cache_hit: result.cache_hit,
                            bytes_hashed: result.bytes_hashed,
                        },
                    );
                }
            }
            Err(e) => tracing::debug!("daemon file hash prefetch failed: {e}"),
        }
    }

    /// Hash a file's contents, using the persistent cache when available.
    pub fn hash(&self, path: &Path) -> Result<String> {
        let _trace = crate::phase_trace::phase("input_hash");
        let (hash, fingerprint) = self.hash_inner(path)?;
        if let Some(fingerprint) = &fingerprint {
            self.guard_input(fingerprint);
        }
        self.recent_hashes.borrow_mut().insert(
            absolute_path(path),
            RecentHash {
                hash: hash.clone(),
                fingerprint,
            },
        );
        Ok(hash)
    }

    fn hash_inner(&self, path: &Path) -> Result<(String, Option<FileFingerprint>)> {
        let Some(cache) = &self.cache else {
            if self.too_new.invocation_start_ns == 0 {
                let hash = hash_file(path)?;
                return Ok((hash, FileFingerprint::from_path(path).ok()));
            }
            let before = FileFingerprint::from_path(path).ok();
            if let Some(fingerprint) = &before {
                self.note_too_new(fingerprint);
            }
            let hash = hash_file(path)?;
            let after = FileFingerprint::from_path(path).ok();
            if let Some(fingerprint) = &after {
                self.note_too_new(fingerprint);
            }
            if before != after {
                self.too_new.saw_too_new.set(true);
            }
            return Ok((hash, after));
        };

        let fingerprint = match FileFingerprint::from_path(path) {
            Ok(fingerprint) => fingerprint,
            Err(e) => {
                tracing::debug!(
                    "file hash cache metadata lookup failed for {}: {e}",
                    path.display()
                );
                return hash_file(path).map(|hash| (hash, None));
            }
        };

        self.note_too_new(&fingerprint);

        if fingerprint.size < MIN_PERSISTED_HASH_BYTES {
            let hash = hash_file(path)?;
            self.record_miss(fingerprint.size);
            return Ok((hash, Some(fingerprint)));
        }

        if let Some(prefetched) = self.prefetched.borrow().get(&fingerprint) {
            if prefetched.cache_hit {
                self.record_hit();
            } else {
                self.record_miss_count();
                self.record_miss_bytes(prefetched.bytes_hashed);
            }
            return Ok((prefetched.hash.clone(), Some(fingerprint)));
        }

        match cache.get(&fingerprint) {
            Ok(Some(hash)) => {
                self.record_hit();
                return Ok((hash, Some(fingerprint)));
            }
            Ok(None) => {}
            Err(e) => {
                tracing::debug!("file hash cache lookup failed for {}: {e}", path.display());
            }
        }

        let hash = hash_file(path)?;
        self.record_miss(fingerprint.size);
        self.pending_memo
            .borrow_mut()
            .push((fingerprint.clone(), hash.clone()));
        Ok((hash, Some(fingerprint)))
    }

    /// Classify how this source uses `var` (see [`source_env_dep_use`]).
    /// Decisions are keyed by the already-computed content hash, so warm key
    /// construction can reuse them without opening the source again (#557).
    fn env_dep_use(&self, path: &Path, var: &str) -> Result<SourceEnvDepUse> {
        let absolute = absolute_path(path);
        let recent = self.recent_hashes.borrow().get(&absolute).cloned();
        let recent = match recent {
            Some(recent) => recent,
            None => {
                self.hash(path)?;
                self.recent_hashes
                    .borrow()
                    .get(&absolute)
                    .cloned()
                    .expect("a successful hash records its fingerprint")
            }
        };
        if let Some(expected) = recent.fingerprint {
            let current = FileFingerprint::from_path(path)
                .with_context(|| format!("revalidating {} before env-use scan", path.display()))?;
            if current != expected {
                anyhow::bail!(
                    "source {} changed between content hashing and env-use scan",
                    path.display()
                );
            }
            return self.env_dep_use_for_hash(path, var, &recent.hash);
        }

        // Without a trustworthy fingerprint, bypass memo lookup. The scan
        // still verifies the content hash before recording a reusable result.
        self.scan_env_dep_use(path, var, &recent.hash)
    }

    fn env_dep_use_for_hash(
        &self,
        path: &Path,
        var: &str,
        content_hash: &str,
    ) -> Result<SourceEnvDepUse> {
        let key = (content_hash.to_string(), var.to_string());
        if let Some(result) = self.env_dep_uses.borrow().get(&key) {
            return Ok(*result);
        }

        // Rows written by another scanner version are invisible, so a scanner
        // fix reclassifies files that did not change.
        if let Some(cache) = &self.cache {
            match cache.get_source_env_dep_use(content_hash, var, SOURCE_ENV_DEP_SCANNER_VERSION) {
                Ok(Some(code)) => {
                    if let Some(result) = SourceEnvDepUse::from_memo_code(code) {
                        self.env_dep_uses.borrow_mut().insert(key, result);
                        return Ok(result);
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::debug!("env-use cache lookup failed: {error}");
                }
            }
        }

        self.scan_env_dep_use(path, var, content_hash)
    }

    fn scan_env_dep_use(
        &self,
        path: &Path,
        var: &str,
        content_hash: &str,
    ) -> Result<SourceEnvDepUse> {
        let key = (content_hash.to_string(), var.to_string());
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading {} for env-use scan", path.display()))?;
        let observed_hash = blake3::hash(&bytes).to_hex().to_string();
        if observed_hash != content_hash {
            anyhow::bail!(
                "source {} changed between content hashing and env-use scan",
                path.display()
            );
        }

        let source = String::from_utf8_lossy(&bytes);
        let result = source_env_dep_use(&source, var);
        if let Some(cache) = &self.cache
            && let Err(error) = cache.put_source_env_dep_use(
                content_hash,
                var,
                SOURCE_ENV_DEP_SCANNER_VERSION,
                result.memo_code(),
            )
        {
            tracing::debug!("env-use cache update failed: {error}");
        }
        self.env_dep_uses.borrow_mut().insert(key, result);
        Ok(result)
    }

    /// Hash a linked `-l static=` archive for the cache key. Clean GNU/BSD
    /// archives use a structural digest that retains exact member identity.
    /// Other non-thin inputs use a path-bound fallback; thin archives error so
    /// the wrapper passes through without caching. The too-new guard (#324) is
    /// applied either way, and every scheme is domain-tagged.
    /// Scoped to `static=` (this method) on purpose — `.rlib`s are also `ar`
    /// archives but are hashed whole via [`Self::hash`].
    /// This is the strict [`StaticLibUse::Linked`] reading; rlib bundling
    /// goes through [`Self::hash_static_lib_for`].
    pub fn hash_static_lib(&self, path: &Path) -> Result<String> {
        self.hash_static_lib_for(path, StaticLibUse::Linked)
    }

    /// [`Self::hash_static_lib`] for a known [`StaticLibUse`].
    pub fn hash_static_lib_for(&self, path: &Path, usage: StaticLibUse) -> Result<String> {
        // Without a persistent cache (daemonless / tests), compute directly —
        // still honoring the too-new guard.
        let Some(cache) = &self.cache else {
            if let Ok(fingerprint) = FileFingerprint::from_path(path) {
                self.note_too_new(&fingerprint);
            }
            return compute_static_lib_hash(path, usage);
        };
        let fingerprint = match FileFingerprint::from_path(path) {
            Ok(fp) => fp,
            Err(e) => {
                tracing::debug!(
                    "static-lib hash metadata lookup failed for {}: {e}",
                    path.display()
                );
                return compute_static_lib_hash(path, usage);
            }
        };
        self.note_too_new(&fingerprint);

        // Small libs skip the persistent cache (same policy as `hash`); the
        // archive read is cheap and not worth a row.
        let size = fingerprint.size;
        if size < MIN_PERSISTED_HASH_BYTES {
            let hash = compute_static_lib_hash(path, usage)?;
            self.record_miss(size);
            return Ok(hash);
        }

        // Cache under a SCHEME-NAMESPACED key so a static-lib digest never shares
        // a row with a whole-file hash of the same path (they mean different
        // things — `hash` stores plain blake3, this stores a structural or
        // path-bound archive digest). This restores the warm-build fast path the whole-file
        // hasher had: an unchanged large `static=` archive (e.g. rocksdb) is not
        // re-read on every incremental build. `v7`: v1/v2 rows used older
        // identity definitions, v3 predates the fail-closed ELF gate, v4
        // predates the GCC Mach-O LTO gate, v5 predates admitting
        // DWARF-bearing Mach-O members, and v6 predates accepting the blank
        // `//` header GNU `ar` writes (a v5 or v6 row would keep serving the
        // path-bound digest of an unchanged archive). None may be served
        // after the final archive hardening. Bundled and linked uses get
        // separate rows because a DWARF archive hashes differently for each.
        let key = FileFingerprint {
            path: format!("{}\0{}", usage.memo_namespace(), fingerprint.path),
            size: fingerprint.size,
            mtime_ns: fingerprint.mtime_ns,
            ctime_ns: fingerprint.ctime_ns,
            inode: fingerprint.inode,
        };
        match cache.get(&key) {
            Ok(Some(hash)) => {
                self.record_hit();
                return Ok(hash);
            }
            Ok(None) => {}
            Err(e) => tracing::debug!("static-lib hash cache lookup failed: {e}"),
        }
        let hash = compute_static_lib_hash(path, usage)?;
        self.record_miss(size);
        self.pending_memo.borrow_mut().push((key, hash.clone()));
        Ok(hash)
    }

    /// Memoised hashes for many stamps in one lookup, keyed by path. Nothing
    /// memoised, or no index, reads as an empty map: every file is hashed.
    fn memoised_hashes<'a>(
        &self,
        stamps: impl Iterator<Item = &'a FileFingerprint>,
    ) -> HashMap<String, String> {
        let Some(cache) = &self.cache else {
            return HashMap::new();
        };
        let stamps: Vec<&FileFingerprint> = stamps.collect();
        cache.get_many(&stamps).unwrap_or_else(|error| {
            tracing::debug!("file hash memo batch lookup failed: {error}");
            HashMap::new()
        })
    }

    /// A header's content hash, from the daemon's prefetch or the memo when
    /// either has this exact stamp, else read and queued for the memo.
    ///
    /// Unlike [`hash`](Self::hash), small files are memoised too: a header
    /// is read by every unit that includes it, so the lookup is paid back
    /// many times. [`flush_memo`](Self::flush_memo) still holds back any file
    /// changed too recently to trust its stamp. The bookkeeping matches
    /// `hash`, so the too-new guard and later revalidation see these files.
    fn header_hash(
        &self,
        path: &Path,
        fingerprint: &FileFingerprint,
        memoised: &HashMap<String, String>,
    ) -> Result<String> {
        if self.cache.is_none() {
            return self.hash(path);
        }
        let prefetched = self.prefetched.borrow().get(fingerprint).map(|prefetched| {
            (
                prefetched.hash.clone(),
                prefetched.cache_hit,
                prefetched.bytes_hashed,
            )
        });
        let hash = if let Some((hash, cache_hit, bytes_hashed)) = prefetched {
            if cache_hit {
                self.record_hit();
            } else {
                self.record_miss_count();
                self.record_miss_bytes(bytes_hashed);
            }
            hash
        } else if let Some(hash) = memoised.get(&fingerprint.path) {
            self.record_hit();
            hash.clone()
        } else {
            let hash = hash_file(path)?;
            self.record_miss(fingerprint.size);
            self.pending_memo
                .borrow_mut()
                .push((fingerprint.clone(), hash.clone()));
            hash
        };
        self.guard_input(fingerprint);
        self.recent_hashes.borrow_mut().insert(
            absolute_path(path),
            RecentHash {
                hash: hash.clone(),
                fingerprint: Some(fingerprint.clone()),
            },
        );
        Ok(hash)
    }

    fn record_hit(&self) {
        self.stats.cache_hits.set(self.stats.cache_hits.get() + 1);
    }

    fn record_miss(&self, size: i64) {
        self.record_miss_count();
        if let Ok(size) = u64::try_from(size) {
            self.record_miss_bytes(size);
        }
    }

    fn record_miss_count(&self) {
        self.stats
            .cache_misses
            .set(self.stats.cache_misses.get() + 1);
    }

    fn record_miss_bytes(&self, bytes: u64) {
        self.stats
            .bytes_hashed
            .set(self.stats.bytes_hashed.get().saturating_add(bytes));
    }
}

/// `-C extra-filename=` in either spelling. rustc normalises `-` and `_` in
/// codegen option names at parse time, so `-C extra_filename=` is the same
/// option and must be dropped from the pre-pass just the same.
fn is_extra_filename_option(value: &str) -> bool {
    value.starts_with("extra-filename=") || value.starts_with("extra_filename=")
}

/// Build the argv for the dep-info pre-pass from the original rustc argv.
///
/// The pre-pass reuses everything that shapes the source closure (features,
/// cfgs, edition, target, `--extern`, codegen opts) and replaces only the
/// output configuration: `--emit dep-info -o <dep_file>`. Dropped on the way:
///
/// - `--emit` / `--out-dir` / `-o`, superseded by the pre-pass's own pair.
///   The joined spellings go too: `-oFILE` (which rustc accepts), a joined
///   `--out-dir=DIR`, and single-dash `-out-dir`, which rustc parses as `-o`
///   plus junk ("option `-o` has no space between flag name and value"). A
///   leftover joins the pre-pass's own `-o` and rustc rejects the pair with
///   "Option 'o' given more than once", exit 1 — a permanent passthrough for
///   every invocation using that spelling (kunobi-ninja/kache#896). The value
///   is inline, so unlike bare `-o` no following argument is consumed, and
///   the match is case-sensitive: `-O` (opt-level) is kept.
/// - `-C extra-filename`, which names output artifacts the pre-pass never
///   produces. rustc warns "ignoring -C extra-filename flag due to -o flag"
///   whenever both are present, and because that warning is emitted while the
///   session is built it lands *first* on stderr — ahead of any real
///   diagnostic. That made a failing pre-pass look like the flag combination
///   was the cause (kunobi-ninja/kache#896). `--out-dir` is the only other flag
///   rustc reports as ignored due to `-o`, and it is already dropped here.
/// - the source file, re-added as the leading positional argument.
/// - `-C incremental`, via the same canonical filter the real compilation path
///   uses.
fn dep_info_pass_args(source_file: &Path, rustc_args: &[String], dep_file: &Path) -> Vec<String> {
    let mut dep_args = closure_shaping_args(source_file, rustc_args);
    dep_args.push("--emit".to_string());
    dep_args.push("dep-info".to_string());
    dep_args.push("-o".to_string());
    dep_args.push(dep_file.to_string_lossy().into_owned());
    dep_args
}

/// The arguments that decide WHICH files rustc reads, with everything that
/// only decides where its output goes removed.
///
/// Shared by the pre-pass argv and the prediction identity, so the two can
/// never disagree about what shapes a closure. Two invocations of one crate
/// that differ only in `-C extra-filename` read the same files, and dropping
/// it is what lets them share a record.
fn closure_shaping_args(source_file: &Path, rustc_args: &[String]) -> Vec<String> {
    let source_str = source_file.to_string_lossy();
    let rustc_args = crate::compile::strip_incremental_flags(rustc_args);
    let mut dep_args = vec![source_str.to_string()];

    let mut remaining = rustc_args.iter().peekable();
    while let Some(arg) = remaining.next() {
        match arg.as_str() {
            "--emit" | "--out-dir" | "-o" => {
                remaining.next(); // drop the flag's value too
            }
            // Two-arg codegen form: `-C extra-filename=<hash>`.
            "-C" | "--codegen"
                if remaining
                    .peek()
                    .is_some_and(|value| is_extra_filename_option(value)) =>
            {
                remaining.next();
            }
            _ if arg.starts_with("--emit=") || arg.starts_with("--out-dir=") => {}
            // Joined output form: `-oFILE`. Value is inline — consume nothing
            // further. (Exact `-o` is handled above, with its value.)
            _ if arg.starts_with("-o") => {}
            // Joined codegen forms: `-Cextra-filename=…`, `--codegen=extra-filename=…`.
            _ if arg
                .strip_prefix("-C")
                .or_else(|| arg.strip_prefix("--codegen="))
                .is_some_and(is_extra_filename_option) => {}
            // Skip the source file — already added as the first positional arg.
            _ if arg.as_str() == source_str.as_ref() => {}
            _ => dep_args.push((*arg).clone()),
        }
    }

    dep_args
}

/// Pick the stderr line that explains why the dep-info pre-pass failed.
///
/// rustc writes diagnostics in the order it produces them, so session-level
/// warnings precede the error that actually aborted the run. Reporting
/// `stderr.lines().next()` therefore blames whichever warning happened to come
/// first: in kunobi-ninja/kache#896 the stated reason a pre-pass exited 1 was
/// "ignoring -C extra-filename flag due to -o flag", a warning that on every
/// rustc from 1.97 to 1.98 leaves the exit status at 0. The real error was
/// never printed, and the crate stayed a passthrough with no way to diagnose
/// it.
///
/// Prefer the first error-level line in either `--error-format=json` or human
/// form, and fall back to the first non-empty line when nothing looks like an
/// error (`rustc` can die on a signal, or a wrapper can fail before rustc runs).
pub(crate) fn first_rustc_error_line(stderr: &str) -> Option<&str> {
    let mut fallback = None;
    for line in stderr.lines() {
        if line.trim().is_empty() {
            continue;
        }
        // JSON: `{"$message_type":"diagnostic",…,"level":"error",…}`.
        // Human: `error: …` or `error[E0433]: …` at column 0 — indented lines
        // are snippet/note continuations, never the diagnostic header.
        if line.contains(r#""level":"error""#)
            || line.starts_with("error:")
            || line.starts_with("error[")
        {
            return Some(line);
        }
        fallback.get_or_insert(line);
    }
    fallback
}

/// Run `rustc --emit=dep-info` as a pre-pass to discover source files and env deps.
///
/// This is the I/O layer — it invokes rustc and reads the output file. Building
/// the pre-pass argv is delegated to `dep_info_pass_args()`, and parsing to
/// `parse_dep_info()` / `parse_env_dep_info()` (all pure functions).
///
/// Returns `Err` on any failure (rustc non-zero exit, missing/unreadable dep
/// file, etc.). The caller MUST treat that as non-cacheable: `compute_cache_key`
/// propagates the error so the wrapper passes through to the real compiler and
/// never stores an entry keyed off an incomplete input set (kunobi-ninja/kache#323).
pub fn run_dep_info_pass(
    rustc: &Path,
    inner_rustc: Option<&Path>,
    source_file: &Path,
    rustc_args: &[String],
    use_response_file: bool,
) -> Result<DepInfo> {
    let temp_dir = tempfile::Builder::new()
        .prefix("kache-depinfo")
        .tempdir()
        .context("creating temp dir for dep-info")?;
    let dep_file = temp_dir.path().join("deps.d");

    let mut cmd = std::process::Command::new(rustc);
    if let Some(inner_rustc) = inner_rustc {
        cmd.arg(inner_rustc);
    }

    let dep_args = dep_info_pass_args(source_file, rustc_args, &dep_file);

    let response_file = if use_response_file {
        let response = crate::compile::RustcResponseFile::new(
            dep_args.iter().map(std::string::String::as_str),
        )?;
        cmd.arg(response.argument());
        Some(response)
    } else {
        cmd.args(&dep_args);
        None
    };

    tracing::trace!("dep-info pre-pass: {:?}", cmd);

    let spawned = std::time::Instant::now();
    let output = cmd
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .output()
        .context("running rustc --emit=dep-info")?;
    // One extra rustc start per invocation, hit or miss. Counted and timed so
    // the event log can show what removing it would buy (`dep_info_runs`,
    // `dep_info_ms`); it stays inside `key_ms` too.
    crate::opcounts::record_dep_info_run(spawned.elapsed());
    drop(response_file);

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Do NOT fall back to a crate-root-only DepInfo: that under-specifies the
        // input set, so a later build whose transitive sources changed would hit
        // this key and restore a stale artifact (kunobi-ninja/kache#323). Fail
        // instead — the caller treats the invocation as non-cacheable and passes
        // through to the real compiler (which, for a genuinely broken crate, also
        // fails, so no real hit was being served anyway).
        anyhow::bail!(
            "dep-info pre-pass failed (exit {}): {}",
            output.status.code().unwrap_or(-1),
            first_rustc_error_line(&stderr).unwrap_or("(no output)")
        );
    }

    let dep_content = read_dep_info_file(&dep_file)?;

    let mut source_files = parse_dep_info(&dep_content);
    if source_files.is_empty() {
        source_files.push(source_file.to_path_buf());
    }
    let env_deps = parse_env_dep_info(&dep_content);

    tracing::trace!(
        "dep-info found {} source files, {} env deps for {}",
        source_files.len(),
        env_deps.len(),
        source_file.display()
    );

    Ok(DepInfo {
        source_files,
        env_deps,
    })
}

/// Read a dep-info file rustc just wrote.
///
/// Split out of [`run_dep_info_pass`] so the encoding failure gets its own
/// diagnosis: a non-UTF8 filename or env value in the source closure lands
/// verbatim in this file, and `read_to_string` would refuse it with only
/// "stream did not contain valid UTF-8" — no hint which input set stayed
/// uncached. Fail closed with the cause named, never lossy: a lossy path
/// would hash a filename that exists nowhere on disk.
fn read_dep_info_file(dep_file: &Path) -> Result<String> {
    let bytes = std::fs::read(dep_file).context("reading dep-info output")?;
    String::from_utf8(bytes).context("dep-info output is not valid UTF-8")
}

/// Parse a Makefile-style dep-info file to extract source file paths.
///
/// Format: `target: dep1 dep2 dep3`
/// Handles `\ ` escaped spaces in paths. Returns sorted paths.
pub(crate) fn parse_dep_info(dep_info: &str) -> Vec<std::path::PathBuf> {
    let line = match dep_info.lines().next() {
        Some(l) => l,
        None => return vec![],
    };

    let pos = match line.find(": ") {
        Some(p) => p,
        None => return vec![],
    };

    let mut deps = Vec::new();
    let mut current = String::new();
    let mut chars = line[pos + 2..].chars().peekable();

    loop {
        match chars.next() {
            Some('\\') if chars.peek() == Some(&' ') => {
                current.push(' ');
                chars.next();
            }
            Some('\\') => current.push('\\'),
            Some(' ') => {
                if !current.is_empty() {
                    deps.push(std::path::PathBuf::from(&current));
                    current.clear();
                }
            }
            Some(c) => current.push(c),
            None => {
                if !current.is_empty() {
                    deps.push(std::path::PathBuf::from(&current));
                }
                break;
            }
        }
    }

    deps.sort();
    deps
}

/// Parse `# env-dep:VAR=VALUE` lines from rustc's dep-info output.
///
/// Returns RAW values — does NOT path-normalize them. Normalization
/// is the consumer's call: `compute_cache_key` runs each value through
/// either `PathNormalizer::normalize` (safe-to-share crates, e.g.
/// serde-style `include!()` use of OUT_DIR) or keeps it absolute
/// (env!()-as-value pattern; see `path_is_only_used_for_includes`). Doing the
/// substitution here would erase the information `compute_cache_key`
/// needs to make that distinction.
fn parse_env_dep_info(dep_info: &str) -> Vec<(String, String)> {
    let mut env_deps = Vec::new();
    for line in dep_info.lines() {
        if let Some(env_dep) = line.strip_prefix("# env-dep:") {
            if let Some((var, val)) = env_dep.split_once('=') {
                env_deps.push((var.to_string(), unescape_env_dep_value(val)));
            } else {
                env_deps.push((env_dep.to_string(), String::new()));
            }
        }
    }
    env_deps.sort_by(|(a, _), (b, _)| a.cmp(b));
    env_deps
}

/// Reverse rustc's `# env-dep:` value escaping.
///
/// rustc writes env-dep values through `escape_dep_env`, which emits
/// `\` as `\\`, newline as `\n`, and carriage return as `\r` so the
/// value stays on one line. Without undoing it, a Windows path arrives
/// doubled (`C:\\foo\\bar`); the cache-key path normalizer's rules use
/// single backslashes, so the value never matched and `OUT_DIR` leaked
/// its absolute path into the key — defeating cross-path cache hits on
/// Windows (kunobi-ninja/kache#201). On Unix, paths rarely contain
/// backslashes, so this was latent.
fn unescape_env_dep_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('\\') => out.push('\\'),
            // Unknown escape: keep both bytes so the value round-trips
            // rather than silently dropping the backslash.
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}

/// Get rustc version string, cached to a file keyed by binary mtime.
///
/// Every wrapper invocation needs this, but the output only changes when rustc
/// itself is updated.  A file cache avoids spawning `rustc --version --verbose`
/// 300+ times per parallel build — the first invocation writes the file and the
/// rest read it back in <1 ms.
fn get_rustc_version(rustc: &Path) -> Result<String> {
    let _trace = crate::phase_trace::phase("compiler_identity");
    if let Some(cached) = read_tool_version_cache(rustc, "rustc-ver") {
        return Ok(cached);
    }

    let output = std::process::Command::new(rustc)
        .arg("--version")
        .arg("--verbose")
        .output()
        .context("running rustc --version --verbose")?;

    let version = String::from_utf8_lossy(&output.stdout).trim().to_string();
    write_tool_version_cache(rustc, "rustc-ver", &version);
    Ok(version)
}

/// `clippy-driver --version` (`clippy 0.1.98 (hash date)`), file-cached like
/// the rustc version. `-vV` only reports the underlying rustc.
fn get_clippy_version(driver: &Path) -> Result<String> {
    if let Some(cached) = read_tool_version_cache(driver, "clippy-ver") {
        return Ok(cached);
    }
    let output = std::process::Command::new(driver)
        .arg("--version")
        .output()
        .context("running clippy-driver --version")?;
    let version = String::from_utf8_lossy(&output.stdout).trim().to_string();
    anyhow::ensure!(
        !version.is_empty(),
        "clippy-driver --version printed nothing"
    );
    write_tool_version_cache(driver, "clippy-ver", &version);
    Ok(version)
}

/// Everything about a Clippy invocation that its argv does not carry: the
/// driver version, the configuration file Clippy would load (`.clippy.toml`
/// or `clippy.toml`, searched from `CLIPPY_CONF_DIR`, else the manifest
/// directory, upwards) and the lint arguments `cargo clippy` passes through
/// the environment. Content, not location, so two checkouts share keys.
pub(crate) fn clippy_identity(driver: &Path, env: &KeyEnv) -> Result<String> {
    clippy_identity_in(
        driver,
        |name| env.var_os(name),
        env.cwd().map(Path::to_path_buf),
    )
}

fn clippy_identity_in(
    driver: &Path,
    env: impl Fn(&str) -> Option<std::ffi::OsString>,
    current_dir: Option<PathBuf>,
) -> Result<String> {
    let mut identity = get_clippy_version(driver)?;
    identity.push('\n');
    // The `cargo` lint group reads the package manifest, which rustc's
    // dep-info never lists.
    if let Some(manifest_dir) = env("CARGO_MANIFEST_DIR") {
        let manifest = PathBuf::from(manifest_dir).join("Cargo.toml");
        match std::fs::read(&manifest) {
            Ok(content) => {
                identity.push_str(&format!("manifest:{}\n", blake3::hash(&content).to_hex()))
            }
            Err(_) => identity.push_str("manifest:none\n"),
        }
    }
    let start = env("CLIPPY_CONF_DIR")
        .or_else(|| env("CARGO_MANIFEST_DIR"))
        .map(PathBuf::from)
        .or(current_dir);
    match start.and_then(|start| selected_clippy_config(&start)) {
        Some(path) => {
            let content = std::fs::read(&path)
                .with_context(|| format!("reading Clippy configuration {}", path.display()))?;
            identity.push_str(&format!(
                "config:{}:{}\n",
                path.file_name()
                    .map(|n| n.to_string_lossy())
                    .unwrap_or_default(),
                blake3::hash(&content).to_hex()
            ));
        }
        None => identity.push_str("config:none\n"),
    }
    for name in ["CLIPPY_ARGS", "CLIPPY_DISABLE_DOCS_LINKS"] {
        match env(name) {
            Some(value) => identity.push_str(&format!("{name}={}\n", value.to_string_lossy())),
            None => identity.push_str(&format!("{name} unset\n")),
        }
    }
    Ok(identity)
}

/// The configuration file Clippy loads: the first `.clippy.toml` or
/// `clippy.toml` from `start` up to the filesystem root.
fn selected_clippy_config(start: &Path) -> Option<PathBuf> {
    let mut directory = std::fs::canonicalize(start).ok()?;
    loop {
        for name in [".clippy.toml", "clippy.toml"] {
            let candidate = directory.join(name);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
        if !directory.pop() {
            return None;
        }
    }
}

/// The toolchain commit hash from `rustc -vV`'s `commit-hash:` line, for the
/// `<RUST_SRC>` remap target (`/rustc/<hash>`).
///
/// Reuses the file-cached `-vV` output ([`get_rustc_version`]) — no extra
/// process spawn. Returns `None` for a locally-built rustc whose `commit-hash`
/// is absent or `unknown`; the `<RUST_SRC>` rule is then skipped and std paths
/// stay virtual (see [`crate::path_normalizer::PathNormalizer::with_rust_src_rule`]).
pub(crate) fn get_rustc_commit_hash(rustc: &Path) -> Option<String> {
    let vv = get_rustc_version(rustc).ok()?;
    vv.lines()
        .find_map(|l| l.strip_prefix("commit-hash:"))
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty() && h != "unknown")
}

/// The toolchain sysroot, used to locate `{sysroot}/lib/rustlib/src/rust` for
/// the `<RUST_SRC>` remap rule.
///
/// Prefers an explicit `--sysroot` (already parsed into [`RustcArgs::sysroot`]);
/// otherwise runs `rustc --print sysroot` once and file-caches it like the
/// version probe. The cache key folds the binary path + mtime plus the rustup
/// toolchain-selection state, so a shim redirected to another toolchain
/// re-probes instead of serving a stale sysroot (see
/// [`toolchain_selector_fingerprint`]).
pub(crate) fn get_rustc_sysroot(args: &RustcArgs) -> Option<PathBuf> {
    if let Some(sysroot) = &args.sysroot {
        return Some(sysroot.clone());
    }
    let rustc = &args.rustc;
    if let Some(cached) = read_tool_version_cache(rustc, "rustc-sysroot") {
        return Some(PathBuf::from(cached));
    }
    let output = std::process::Command::new(rustc)
        .arg("--print")
        .arg("sysroot")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let sysroot = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if sysroot.is_empty() {
        return None;
    }
    write_tool_version_cache(rustc, "rustc-sysroot", &sysroot);
    Some(PathBuf::from(sysroot))
}

/// Read a cached tool-version string.  Returns `None` on any failure (missing
/// file, stale mtime, I/O error) so the caller falls back to running the tool.
fn read_tool_version_cache(binary: &Path, prefix: &str) -> Option<String> {
    let cache_file = tool_version_cache_path(binary, prefix)?;
    std::fs::read_to_string(cache_file)
        .ok()
        .filter(|s| !s.is_empty())
}

/// Persist a tool-version string for later reads.  Best-effort — errors are
/// silently ignored because the fallback (running the tool) is always available.
fn write_tool_version_cache(binary: &Path, prefix: &str, version: &str) {
    if let Some(cache_file) = tool_version_cache_path(binary, prefix) {
        // The cache directory may not exist yet (a fresh machine, or a CI
        // runner whose store lives elsewhere); without it nothing was ever
        // persisted and every process re-ran the probe.
        crate::probe_memo::write_atomic(&cache_file, version);
    }
}

/// Build the cache-file path: `<cache_dir>/<prefix>-<hash>.txt` where the hash
/// is derived from the binary's canonical path + mtime so it auto-invalidates
/// when the toolchain is updated, plus the rustup toolchain-selection state
/// (see [`toolchain_selector_fingerprint`]).
fn tool_version_cache_path(binary: &Path, prefix: &str) -> Option<std::path::PathBuf> {
    let canon = std::fs::canonicalize(binary).ok()?;
    let mtime = std::fs::metadata(&canon)
        .ok()?
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs();
    let key = format!(
        "{}:{}:{}",
        canon.display(),
        mtime,
        toolchain_selector_fingerprint(
            std::env::var_os("RUSTUP_TOOLCHAIN").as_deref(),
            std::env::current_dir().ok().as_deref(),
            rustup_settings_path().as_deref(),
        )
    );
    let hash = blake3::hash(key.as_bytes()).to_hex();
    Some(crate::config::default_cache_dir().join(format!("{}-{}.txt", prefix, &hash[..16])))
}

/// The rustup toolchain-selection state that can redirect an unchanged shim
/// binary (`~/.cargo/bin/rustc` is rustup itself) to a different toolchain:
/// path + mtime alone then serve a stale version, sysroot, or linker string
/// across a `RUSTUP_TOOLCHAIN` change, an edited `rust-toolchain{,.toml}`,
/// or a `rustup default` switch (which rewrites `settings.toml`).
///
/// Selector files fold by content digest, not mtime: these are tiny files,
/// and a digest catches an edit within one mtime second or under a
/// preserved timestamp. The nearest directory with either toolchain-file
/// spelling contributes BOTH spellings, sidestepping rustup's precedence
/// rules entirely — whichever file actually wins, changing it changes the
/// fingerprint. For a non-shim binary all this costs is a cheap re-probe
/// on the rare occasions the selection state changes.
fn toolchain_selector_fingerprint(
    rustup_toolchain: Option<&std::ffi::OsStr>,
    cwd: Option<&Path>,
    rustup_settings: Option<&Path>,
) -> String {
    let mut fp = String::new();
    if let Some(toolchain) = rustup_toolchain {
        fp.push_str("env:");
        fp.push_str(&toolchain.to_string_lossy());
    }
    // Rustup resolves toolchain files from the cwd upward; the nearest
    // directory holding one ends the search.
    if let Some(cwd) = cwd {
        'search: for dir in cwd.ancestors() {
            let mut found = false;
            for name in ["rust-toolchain", "rust-toolchain.toml"] {
                let candidate = dir.join(name);
                if let Some(digest) = file_digest(&candidate) {
                    fp.push_str(";file:");
                    fp.push_str(&candidate.to_string_lossy());
                    fp.push(':');
                    fp.push_str(&digest);
                    found = true;
                }
            }
            if found {
                break 'search;
            }
        }
    }
    if let Some(settings) = rustup_settings
        && let Some(digest) = file_digest(settings)
    {
        fp.push_str(";default:");
        fp.push_str(&digest);
    }
    fp
}

/// `$RUSTUP_HOME/settings.toml` (or its `~/.rustup` default), which records
/// the `rustup default` toolchain.
fn rustup_settings_path() -> Option<std::path::PathBuf> {
    let home = std::env::var_os("RUSTUP_HOME")
        .map(std::path::PathBuf::from)
        .or_else(|| dirs::home_dir().map(|home| home.join(".rustup")))?;
    Some(home.join("settings.toml"))
}

/// Content digest of a small selector file, or `None` if unreadable.
fn file_digest(path: &Path) -> Option<String> {
    let bytes = std::fs::read(path).ok()?;
    Some(blake3::hash(&bytes).to_hex()[..16].to_string())
}

/// Get the host target triple.
fn host_target_triple() -> &'static str {
    option_env!("TARGET").unwrap_or("unknown")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LinuxLibcFamily {
    Gnu,
    Musl,
}

impl LinuxLibcFamily {
    fn key_name(self) -> &'static str {
        match self {
            Self::Gnu => "gnu-libc",
            Self::Musl => "musl",
        }
    }
}

/// Extract the wrapped rustc's native host triple from `rustc -vV`.
///
/// This must not use [`host_target_triple`]: release kache binaries are built
/// for musl, but commonly wrap a GNU rustc on a glibc host.
pub(crate) fn rustc_host_triple(rustc_version: &str) -> Option<&str> {
    rustc_version.lines().find_map(|line| {
        line.strip_prefix("host:")
            .map(str::trim)
            .filter(|host| !host.is_empty())
    })
}

fn linux_libc_family(target: &str) -> Option<LinuxLibcFamily> {
    let mut components = target.split('-');
    if !components.clone().any(|component| component == "linux") {
        return None;
    }
    components.find_map(|component| {
        if component.starts_with("gnu") {
            Some(LinuxLibcFamily::Gnu)
        } else if component.starts_with("musl") {
            Some(LinuxLibcFamily::Musl)
        } else {
            None
        }
    })
}

/// Return the native Linux libc family only when this invocation emits an
/// OS-loaded artifact for the wrapped rustc's own host target.
fn native_linux_libc_family(
    args: &RustcArgs,
    rustc_version: &str,
    running_on_linux: bool,
) -> Result<Option<LinuxLibcFamily>> {
    // An executable-shaped crate under `cargo check` emits metadata only; no
    // OS-loaded file exists and probing libc here would both fragment its key
    // and make a missing host utility disable otherwise-portable caching.
    let emits_link = args.emit.is_empty() || args.emit.iter().any(|kind| kind == "link");
    if !running_on_linux || !args.is_executable_output() || !emits_link {
        return Ok(None);
    }
    let host = rustc_host_triple(rustc_version)
        .context("wrapped rustc -vV output has no host triple; cannot key native Linux libc")?;
    let effective_target = args.target.as_deref().unwrap_or(host);
    if effective_target != host {
        return Ok(None);
    }
    if !host.split('-').any(|component| component == "linux") {
        return Ok(None);
    }
    linux_libc_family(host)
        .map(Some)
        .with_context(|| format!("unsupported native Linux libc in rustc host triple {host}"))
}

fn rustc_version_for_native_link<'a, F>(
    args: &RustcArgs,
    outer_rustc_version: &'a str,
    load_version: F,
) -> Result<Cow<'a, str>>
where
    F: FnOnce(&Path) -> Result<String>,
{
    match args.inner_rustc.as_deref() {
        Some(inner) => load_version(inner)
            .map(Cow::Owned)
            .context("reading inner rustc version for native host link key"),
        None => Ok(Cow::Borrowed(outer_rustc_version)),
    }
}

/// Fold the native host's libc signature into linked-output keys.
///
/// The injected probe keeps the gating independently testable without running
/// host tools or depending on the test runner's libc.
fn fold_native_host_libc_signature<H: KeyFold, F>(
    hasher: &mut H,
    args: &RustcArgs,
    rustc_version: &str,
    running_on_linux: bool,
    probe: F,
) -> Result<()>
where
    F: FnOnce(LinuxLibcFamily) -> Result<String>,
{
    // In a double-wrapper invocation (`clippy-driver rustc ...`), the outer
    // wrapper's version banner may not contain rustc's `host:` line. Read the
    // already-file-cached verbose version of the actual inner rustc, but only
    // for a Linux linked output where the host triple is needed.
    let emits_link = args.emit.is_empty() || args.emit.iter().any(|kind| kind == "link");
    if !running_on_linux || !args.is_executable_output() || !emits_link {
        return Ok(());
    }
    let rustc_version = rustc_version_for_native_link(args, rustc_version, get_rustc_version)?;

    let Some(family) = native_linux_libc_family(args, &rustc_version, running_on_linux)? else {
        return Ok(());
    };
    let signature = probe(family).with_context(|| {
        format!(
            "determining native Linux {} signature for cache key",
            family.key_name()
        )
    })?;
    fold_field(
        hasher,
        b"host_libc.v1:",
        format!("{}:{signature}", family.key_name()).as_bytes(),
    );
    tracing::trace!(
        "[key:{}] host_libc={}:{signature}",
        args.crate_name.as_deref().unwrap_or("unknown"),
        family.key_name()
    );
    Ok(())
}

/// Fold hashed CRT/startup/libc objects (Linux) and SDK identity (macOS)
/// into linked-output keys. Injected probes keep the gating testable without
/// running host tools.
fn fold_native_link_runtime_identity<H, Crt, Sdk>(
    hasher: &mut H,
    args: &RustcArgs,
    rustc_version: &str,
    running_on_linux: bool,
    running_on_macos: bool,
    crt_probe: Crt,
    sdk_probe: Sdk,
    env: &KeyEnv,
) -> Result<()>
where
    H: KeyFold,
    Crt: FnOnce(&Path) -> Result<BTreeMap<String, String>>,
    Sdk: FnOnce(Option<String>) -> Result<String>,
{
    let emits_link = args.emit.is_empty() || args.emit.iter().any(|kind| kind == "link");
    if !args.is_executable_output() || !emits_link {
        return Ok(());
    }
    if !running_on_linux && !running_on_macos {
        return Ok(());
    }
    let rustc_version = rustc_version_for_native_link(args, rustc_version, get_rustc_version)?;
    let host = rustc_host_triple(&rustc_version)
        .context("wrapped rustc -vV output has no host triple; cannot key native link runtime")?;
    let effective_target = args.target.as_deref().unwrap_or(host);
    if effective_target != host {
        return Ok(());
    }

    if running_on_linux && host.split('-').any(|component| component == "linux") {
        let driver = resolve_link_driver(args)
            .context("native Linux link has no cc/linker driver; cannot key CRT objects")?;
        let objects = crt_probe(&driver).context("determining native Linux CRT/libc identity")?;
        let encoded = crate::native_link_key::encode_crt_objects(&objects);
        fold_field(hasher, b"host_crt.v1:", encoded.as_bytes());
        tracing::trace!(
            "[key:{}] host_crt={}",
            args.crate_name.as_deref().unwrap_or("unknown"),
            encoded.replace('\n', ",")
        );
    }

    if running_on_macos && host.split('-').any(|component| component == "darwin") {
        let deployment_target = env.var("MACOSX_DEPLOYMENT_TARGET");
        let identity = sdk_probe(env.var("SDKROOT"))
            .context("determining macOS SDK identity for cache key")?;
        fold_field(hasher, b"host_sdk.v1:", identity.as_bytes());
        tracing::trace!(
            "[key:{}] host_sdk={}",
            args.crate_name.as_deref().unwrap_or("unknown"),
            identity
        );
        if let Some(target) = deployment_target.filter(|value| !value.is_empty()) {
            fold_field(hasher, b"host_deployment_target.v1:", target.as_bytes());
            tracing::trace!(
                "[key:{}] host_deployment_target={}",
                args.crate_name.as_deref().unwrap_or("unknown"),
                target
            );
        }
    }
    Ok(())
}

/// Extract the effective native library search directories that can shadow
/// the MSVC/SDK defaults. `-L native/all` entries are already parsed by rustc;
/// `/LIBPATH` is accepted only in an unambiguous single-argument form.
#[derive(Debug, PartialEq, Eq)]
struct WindowsNativeLinkSearchDirs {
    /// Directories rustc searches before passing a library name to LINK.
    rustc: Vec<PathBuf>,
    /// Directories emitted to LINK before the LIB environment paths.
    linker: Vec<PathBuf>,
}

fn windows_native_link_search_dirs(args: &RustcArgs) -> Result<WindowsNativeLinkSearchDirs> {
    const KNOWN_L_KINDS: [&str; 5] = ["dependency", "crate", "native", "framework", "all"];
    let mut rustc = Vec::new();
    for spec in &args.link_search {
        let (kind, path) = match spec.split_once('=') {
            Some((kind, path)) if KNOWN_L_KINDS.contains(&kind) => (Some(kind), path),
            _ => (None, spec.as_str()),
        };
        if matches!(kind, Some("dependency") | Some("crate")) {
            continue;
        }
        if matches!(kind, None | Some("native") | Some("all")) {
            rustc.push(PathBuf::from(path));
        }
    }
    let mut linker = rustc.clone();
    for (key, value) in &args.codegen_opts {
        if !matches!(key.as_str(), "link-arg" | "link-args") {
            continue;
        }
        let Some(value) = value.as_deref() else {
            continue;
        };
        if crate::native_link_key::windows_link_argument_has_unmodeled_input(value) {
            anyhow::bail!(
                "explicit Windows linker input files (.lib/.a/.obj/.o/.res/.def/.exp/.manifest) \
                 and file-carrying LINK options (/DEF, /DEFAULTLIB, /MANIFESTINPUT, \
                 /MANIFESTFILE, /PDBSTRIPPED, ...) are not hashed and are not cacheable"
            );
        }
        if let Some(path) = windows_libpath_argument(value)? {
            linker.push(PathBuf::from(path));
        }
    }
    Ok(WindowsNativeLinkSearchDirs { rustc, linker })
}

fn windows_libpath_argument(value: &str) -> Result<Option<String>> {
    let value = value.trim();
    let value = value.strip_prefix("-Wl,").unwrap_or(value);
    let upper = value.to_ascii_uppercase();
    let marker = ["/LIBPATH:", "/LIBPATH=", "-LIBPATH:", "-LIBPATH="]
        .into_iter()
        .find(|marker| upper.starts_with(*marker));
    let Some(marker) = marker else {
        if upper.contains("/LIBPATH") || upper.contains("-LIBPATH") {
            anyhow::bail!("ambiguous Windows /LIBPATH linker argument");
        }
        return Ok(None);
    };
    let path = value[marker.len()..].trim();
    let path = if let Some(quoted) = path.strip_prefix('"') {
        let closing = quoted
            .find('"')
            .context("unterminated quoted Windows /LIBPATH linker argument")?;
        if !quoted[closing + 1..].trim().is_empty() {
            anyhow::bail!("ambiguous Windows /LIBPATH linker argument");
        }
        quoted[..closing].trim()
    } else {
        if path.contains('"') || path.chars().any(char::is_whitespace) {
            anyhow::bail!("ambiguous Windows /LIBPATH linker argument");
        }
        path
    };
    if path.is_empty() {
        anyhow::bail!("empty Windows /LIBPATH linker argument");
    }
    Ok(Some(path.to_string()))
}

fn fold_generic_linker_identity<H, Get>(
    hasher: &mut H,
    args: &RustcArgs,
    native_windows_msvc: bool,
    get_identity: Get,
) where
    H: KeyFold,
    Get: FnOnce(&RustcArgs) -> Option<String>,
{
    if args.is_executable_output()
        && args.emits_link()
        && !native_windows_msvc
        && let Some(linker_id) = get_identity(args)
    {
        hasher.update(b"linker:");
        hasher.update(linker_id.as_bytes());
        hasher.update(b"\n");
    }
}

/// Native Windows MSVC links use the complete toolchain/runtime identity below
/// as their sole linker signal. Folding the generic `cc --version` probe too
/// would make the key depend on an unrelated Unix-style driver that happens to
/// be on PATH.
fn is_native_windows_msvc_link<Load>(
    args: &RustcArgs,
    rustc_version: &str,
    running_on_windows: bool,
    load_version: Load,
) -> Result<bool>
where
    Load: FnOnce(&Path) -> Result<String>,
{
    if !running_on_windows || !args.is_executable_output() || !args.emits_link() {
        return Ok(false);
    }
    let rustc_version = rustc_version_for_native_link(args, rustc_version, load_version)?;
    let host = rustc_host_triple(&rustc_version)
        .context("wrapped rustc -vV output has no host triple; cannot key native Windows link")?;
    let effective_target = args.target.as_deref().unwrap_or(host);
    Ok(effective_target == host && crate::native_link_key::is_windows_msvc_target(host))
}

/// Fold the selected native Windows MSVC link identity into a linked-output
/// key. The probe is injected so the admission gate can be tested without a
/// Windows toolchain; production supplies the real tool/library discovery.
fn fold_native_windows_msvc_identity<H, Probe>(
    hasher: &mut H,
    args: &RustcArgs,
    rustc_version: &str,
    running_on_windows: bool,
    probe: Probe,
) -> Result<()>
where
    H: KeyFold,
    Probe: FnOnce(Option<&Path>, &str) -> Result<String>,
{
    if !running_on_windows || !args.is_executable_output() || !args.emits_link() {
        return Ok(());
    }
    let rustc_version = rustc_version_for_native_link(args, rustc_version, get_rustc_version)?;
    let host = rustc_host_triple(&rustc_version)
        .context("wrapped rustc -vV output has no host triple; cannot key native Windows link")?;
    let effective_target = args.target.as_deref().unwrap_or(host);
    if effective_target != host {
        if crate::native_link_key::is_windows_msvc_target(effective_target) {
            anyhow::bail!(
                "cross-target Windows MSVC link identity is not modeled; passing through"
            );
        }
        return Ok(());
    }
    if !crate::native_link_key::is_windows_msvc_target(host) {
        return Ok(());
    }
    let architecture = crate::native_link_key::windows_msvc_architecture(host)
        .context("native Windows MSVC host has an unsupported architecture")?;
    let linker = args.get_codegen_opt("linker").map(Path::new);
    let identity =
        probe(linker, architecture).context("determining native Windows MSVC link identity")?;
    fold_field(&mut *hasher, b"host_windows_msvc.v1:", identity.as_bytes());
    tracing::trace!(
        "[key:{}] host_windows_msvc={}",
        args.crate_name.as_deref().unwrap_or("unknown"),
        identity.replace('\n', ",")
    );
    Ok(())
}

fn resolve_link_driver(args: &RustcArgs) -> Option<PathBuf> {
    let linker = args.get_codegen_opt("linker").unwrap_or("cc");
    let linker_path = Path::new(linker);
    if linker_path.is_absolute() {
        Some(linker_path.to_path_buf())
    } else {
        resolve_in_path(linker)
    }
}

fn is_libc_version(version: &str) -> bool {
    let mut parts = version.split('.');
    let Some(major) = parts.next() else {
        return false;
    };
    let Some(minor) = parts.next() else {
        return false;
    };
    !major.is_empty()
        && !minor.is_empty()
        && major.bytes().all(|b| b.is_ascii_digit())
        && minor.bytes().all(|b| b.is_ascii_digit())
        && parts.all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
}

fn parse_getconf_gnu_libc(stdout: &str) -> Option<String> {
    let mut fields = stdout.split_whitespace();
    let family = fields.next()?;
    let version = fields.next()?;
    if family == "glibc" && is_libc_version(version) && fields.next().is_none() {
        Some(version.to_string())
    } else {
        None
    }
}

fn parse_ldd_libc(text: &str) -> Option<(LinuxLibcFamily, String)> {
    let lower = text.to_ascii_lowercase();
    if lower.contains("musl") {
        let version = text.lines().find_map(|line| {
            let mut fields = line.split_whitespace();
            if !fields.next()?.eq_ignore_ascii_case("version") {
                return None;
            }
            let version = fields.next()?;
            is_libc_version(version).then(|| version.to_string())
        })?;
        return Some((LinuxLibcFamily::Musl, version));
    }

    if lower.contains("glibc") || lower.contains("gnu libc") || lower.contains("gnu c library") {
        let first_line = text.lines().find(|line| !line.trim().is_empty())?;
        let version = first_line
            .split_whitespace()
            .rev()
            .find(|field| is_libc_version(field))?;
        return Some((LinuxLibcFamily::Gnu, version.to_string()));
    }
    None
}

/// Probe the runtime libc selected by a native Linux toolchain.
///
/// GNU's `getconf` provides the stable, distro-independent version signal
/// requested by #127. `ldd --version` is a fallback for minimal GNU systems and
/// the primary musl signal. A family mismatch or unparseable result fails
/// closed; the caller then treats the compile as uncacheable.
fn probe_linux_libc_signature(expected: LinuxLibcFamily) -> Result<String> {
    let _trace = crate::phase_trace::phase("native_libc_signature");
    if expected == LinuxLibcFamily::Gnu
        && let Ok(output) = std::process::Command::new("getconf")
            .arg("GNU_LIBC_VERSION")
            .env("LC_ALL", "C")
            .env("LANG", "C")
            .output()
        && output.status.success()
        && let Some(version) = parse_getconf_gnu_libc(&String::from_utf8_lossy(&output.stdout))
    {
        return Ok(version);
    }

    if let Ok(output) = std::process::Command::new("ldd")
        .arg("--version")
        .env("LC_ALL", "C")
        .env("LANG", "C")
        .output()
    {
        // musl commonly writes its banner to stderr (and may return non-zero),
        // so parse both streams before considering the exit status.
        let text = format!(
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        if let Some((family, version)) = parse_ldd_libc(&text)
            && family == expected
        {
            return Ok(version);
        }
    }

    anyhow::bail!(
        "unable to identify native Linux {} (tried getconf/ldd)",
        expected.key_name()
    )
}

/// Get linker identity string for cache key, with file-based caching.
fn get_linker_identity(args: &RustcArgs) -> Option<String> {
    let _trace = crate::phase_trace::phase("linker_identity");
    let linker = args.get_codegen_opt("linker").unwrap_or("cc");
    let linker_path = Path::new(linker);

    // If it's already an absolute path, use it directly; otherwise try to
    // resolve via PATH so we can key the cache on the binary's mtime.
    let resolved = if linker_path.is_absolute() {
        linker_path.to_path_buf()
    } else {
        resolve_in_path(linker)?
    };

    if let Some(cached) = read_tool_version_cache(&resolved, "linker-ver") {
        return Some(cached);
    }

    let output = std::process::Command::new(linker)
        .arg("--version")
        .output()
        .ok()?;

    let version = String::from_utf8_lossy(&output.stdout);
    let first_line = version.lines().next()?.to_string();
    write_tool_version_cache(&resolved, "linker-ver", &first_line);
    Some(first_line)
}

/// Resolve a bare command name to a full path by searching PATH.
fn resolve_in_path(name: &str) -> Option<std::path::PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    std::env::split_paths(&path_var)
        .map(|dir| dir.join(name))
        .find(|p| p.is_file())
}

#[cfg(test)]
mod tests;
