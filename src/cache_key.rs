use crate::args::RustcArgs;
use crate::path_normalizer::{PathNormalizer, check_for_path_leak};
use anyhow::{Context, Result};
pub(crate) use kache_format::{is_valid_cache_key, is_valid_crate_name};
pub(crate) use kache_store::file_hash::*;
use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

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
                let mode = parse_verify_predictions(
                    std::env::var("KACHE_VERIFY_INPUT_PREDICTIONS")
                        .ok()
                        .as_deref(),
                );
                // Verification is the exceptional path: it runs the pre-pass
                // anyway and uses ITS answer, so a disagreement is reported
                // rather than acted on.
                let current_dir = std::env::current_dir().ok();
                if verifies_prediction(mode, args, current_dir.as_deref(), |var| {
                    std::env::var_os(var)
                }) {
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
        let identity = clippy_identity(&args.rustc)?;
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

    let dep_info = resolve_key_inputs(args, file_hasher, crate_name)?;
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
        let mut bakes_out_dir = false;
        for (var, val) in &dep_info.env_deps {
            let normalized_env_dep = normalize_env_dep_value_with_hasher(
                crate_name,
                var,
                val,
                &dep_info.source_files,
                file_hasher,
                path_normalizer,
                aliased_out_dir,
            );
            bakes_out_dir |= env_dep_bakes_out_dir(var, normalized_env_dep.decision, || {
                value_is_under_out_dir(val)
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
    if let Ok(rustflags) = std::env::var("RUSTFLAGS") {
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
    if let Ok(flags) = std::env::var("CARGO_ENCODED_RUSTFLAGS") {
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
    if let Ok(bootstrap) = std::env::var("RUSTC_BOOTSTRAP")
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
    let unit_out_dir = std::env::var_os("OUT_DIR").map(PathBuf::from);
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
                std::env::var_os("LIBRARY_PATH").as_deref(),
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
    let cargo_cfgs = cargo_cfg_pairs(std::env::vars_os());
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
        std::env::var("MACOSX_DEPLOYMENT_TARGET").ok(),
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
        fold_unremapped_path_identity(&mut hasher, args, path_normalizer);
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
) {
    hasher.update(b"unremapped_path_identity:v1\n");

    if let Ok(cwd) = std::env::current_dir() {
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
    // Compared resolved, like OUT_DIR in [`dirs_under`].
    let oso_root =
        crate::compiler::rustc::oso_prefix_root_for_key(args).map(|root| resolved_path(&root));
    let archive_use = |path: &Path| match oso_root.as_deref() {
        Some(root) => linked_archive_use(args, &resolved_path(path), Some(root)),
        None => linked_archive_use(args, path, None),
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
fn sentinelized_env_dep_value(resolved: &str, normalized: &str) -> String {
    if let Some(rel) = out_dir_relative_suffix(resolved) {
        let unit = std::env::var_os("OUT_DIR")
            .map(std::path::PathBuf::from)
            .as_deref()
            .and_then(|p| p.parent().and_then(|d| d.file_name().map(|n| n.to_owned())))
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
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
    val: &str,
    source_files: &[std::path::PathBuf],
    file_hasher: &FileHasher<'_>,
    path_normalizer: &PathNormalizer,
    aliased_out_dir: Option<&Path>,
) -> NormalizedEnvDep {
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
    let resolved = crate::path_normalizer::canonical_string(std::path::Path::new(val))
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
    if forced {
        return NormalizedEnvDep {
            value: sentinelized_env_dep_value(&resolved, &normalized),
            decision: EnvDepNormalizationDecision::ForcedPathOnly,
        };
    }

    let decision = env_dep_path_only_decision(
        var,
        &resolved,
        source_files,
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
            value: sentinelized_env_dep_value(&resolved, &normalized),
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
    normalize_env_dep_value_with_hasher(
        crate_name,
        var,
        val,
        source_files,
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
    val: &str,
    source_files: &[std::path::PathBuf],
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
    if !(var == "OUT_DIR" || allowlist.iter().any(|v| v == var) || value_is_under_out_dir(val)) {
        return EnvDepNormalizationDecision::KeptAbsoluteNotPathOnly;
    }
    if !path_is_only_used_for_includes(val, source_files) {
        return EnvDepNormalizationDecision::KeptAbsoluteNoIncludeProof;
    }
    env_dep_source_decision(var, source_files, file_hasher)
}

/// Whether `var` names Cargo's manifest dir. Windows matches environment
/// names case-insensitively, so dep-info can carry any spelling of it; on
/// Unix a differently-cased name is a different variable, and refusing it
/// costs misses only.
pub(crate) fn is_manifest_dir_var(var: &str) -> bool {
    var.eq_ignore_ascii_case("CARGO_MANIFEST_DIR")
}

/// True when `val` is an absolute path located under the current build's
/// `OUT_DIR`. A build-script `cargo:rustc-env` var whose value points here
/// (typenum's `TYPENUM_BUILD_CONSTS` → `$OUT_DIR/consts.rs`) is build-generated
/// and ephemeral, so it shares OUT_DIR's safety for key path-normalization
/// (kunobi-ninja/kache#431).
///
/// Canonical comparison so the macOS `/tmp` ↔ `/private/tmp` symlink doesn't
/// spuriously miss; falls back to the raw paths when either side can't be
/// canonicalized. Returns false when `OUT_DIR` is unset (the crate has no build
/// script, so no such var exists) or the value is not under it — in particular
/// `CARGO_MANIFEST_DIR`, which lives above OUT_DIR, never qualifies here.
fn value_is_under_out_dir(val: &str) -> bool {
    out_dir_relative_suffix(val).is_some()
}

/// The value's path relative to the build's own `OUT_DIR`, when it lives
/// under it (`Some("")` for `OUT_DIR` itself). This is the anchor for the
/// `<OUT_DIR>` sentinel (kunobi-ninja/kache#330): an OUT_DIR-locator value
/// must normalize relative to OUT_DIR, not through the generic prefix rules
/// — an out-of-workspace `CARGO_TARGET_DIR` makes the derived workspace
/// root the target dir's PARENT, so the generic `<WORKSPACE>` rule matches
/// first and keeps the per-location target-dir component inside the
/// sentinel'd value, diverging the key across build locations. Anchoring on
/// OUT_DIR itself also drops cargo's per-unit hash from the value, which
/// the generic rules preserve.
fn out_dir_relative_suffix(val: &str) -> Option<String> {
    let out_dir = std::env::var_os("OUT_DIR")?;
    let out_dir = Path::new(&out_dir);
    let out_canonical = std::fs::canonicalize(out_dir).ok();
    let out_probe = out_canonical.as_deref().unwrap_or(out_dir);
    // An empty/relative OUT_DIR can't anchor a meaningful "under" test.
    if !out_probe.is_absolute() {
        return None;
    }
    let val_path = Path::new(val);
    let val_canonical = std::fs::canonicalize(val_path).ok();
    let val_probe = val_canonical.as_deref().unwrap_or(val_path);
    val_probe
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
/// Compares canonical paths so the macOS `/tmp` ↔ `/private/tmp`
/// symlink case doesn't produce a spurious false. If either side
/// can't be canonicalized (file moved, etc.), falls back to the
/// raw path components — `Path::starts_with` handles partial
/// component prefixes correctly without requiring lexical matching.
fn path_is_only_used_for_includes(
    out_dir_value: &str,
    source_files: &[std::path::PathBuf],
) -> bool {
    let raw = Path::new(out_dir_value);
    let canonical = std::fs::canonicalize(raw).ok();
    let probe = canonical.as_deref().unwrap_or(raw);
    source_files.iter().any(|f| {
        let f_canonical = std::fs::canonicalize(f).ok();
        let f_probe = f_canonical.as_deref().unwrap_or(f.as_path());
        f_probe.starts_with(probe)
    })
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
pub(crate) fn clippy_identity(driver: &Path) -> Result<String> {
    clippy_identity_in(
        driver,
        |name| std::env::var_os(name),
        std::env::current_dir().ok(),
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
fn rustc_host_triple(rustc_version: &str) -> Option<&str> {
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
    deployment_target: Option<String>,
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
        let identity = sdk_probe(std::env::var("SDKROOT").ok())
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
mod tests {
    use super::*;
    use crate::args::RustcArgs;
    use crate::test_support::process_state_test_lock;

    const GNU_RUSTC_VERSION: &str =
        "rustc 1.90.0\nhost: x86_64-unknown-linux-gnu\nrelease: 1.90.0\n";
    const DARWIN_RUSTC_VERSION: &str =
        "rustc 1.90.0\nhost: aarch64-apple-darwin\nrelease: 1.90.0\n";

    #[test]
    fn source_identity_uses_a_stable_configured_root() {
        let dir = tempfile::tempdir().unwrap();
        let checkout = dir.path().join("checkout");
        let source = checkout.join("src/lib.rs");
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::write(&source, "pub fn value() {}\n").unwrap();
        let normalizer =
            PathNormalizer::empty().with_base_dirs(&[checkout.to_string_lossy().into_owned()]);

        let mut expected = b"<BASE_DIR_0>/".to_vec();
        expected.extend_from_slice(
            source
                .strip_prefix(&checkout)
                .unwrap()
                .to_string_lossy()
                .as_bytes(),
        );
        assert_eq!(
            source_path_identity(&source, &normalizer).unwrap(),
            expected
        );
    }

    #[test]
    fn source_identity_uses_a_configured_external_root_losslessly() {
        let dir = tempfile::tempdir().unwrap();
        let external = dir.path().join("external");
        let source = external.join("generated/value.rs");
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::write(&source, "pub const VALUE: u8 = 1;\n").unwrap();
        let normalizer =
            PathNormalizer::empty().with_base_dirs(&[external.to_string_lossy().into_owned()]);

        let mut expected = b"<BASE_DIR_0>/".to_vec();
        expected.extend_from_slice(
            source
                .strip_prefix(&external)
                .unwrap()
                .to_string_lossy()
                .as_bytes(),
        );
        assert_eq!(
            source_path_identity(&source, &normalizer).unwrap(),
            expected
        );
    }

    #[cfg(unix)]
    #[test]
    fn source_identity_keeps_distinct_symlink_spellings_of_one_inode() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real.rs");
        let alias = dir.path().join("alias.rs");
        std::fs::write(&real, "pub const VALUE: u8 = 1;\n").unwrap();
        std::os::unix::fs::symlink(&real, &alias).unwrap();

        let real_identity = source_path_identity(&real, &PathNormalizer::empty()).unwrap();
        let alias_identity = source_path_identity(&alias, &PathNormalizer::empty()).unwrap();
        assert_ne!(real_identity, alias_identity);
        assert!(real_identity.starts_with(b"<OPAQUE_PATH>/"));
        assert!(alias_identity.starts_with(b"<OPAQUE_PATH>/"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn source_identity_opaque_fallback_preserves_non_utf8_bytes() {
        use std::os::unix::ffi::OsStringExt;

        let dir = tempfile::tempdir().unwrap();
        let external = dir.path().join("external");
        std::fs::create_dir_all(&external).unwrap();
        let path_a = external.join(std::ffi::OsString::from_vec(vec![b'a', 0x80]));
        let path_b = external.join(std::ffi::OsString::from_vec(vec![b'a', 0x81]));
        std::fs::write(&path_a, b"same").unwrap();
        std::fs::write(&path_b, b"same").unwrap();
        let identity_a = source_path_identity(&path_a, &PathNormalizer::empty()).unwrap();
        let identity_b = source_path_identity(&path_b, &PathNormalizer::empty()).unwrap();
        assert_ne!(identity_a, identity_b);
        assert!(identity_a.starts_with(b"<OPAQUE_PATH>/"));
        assert!(identity_b.starts_with(b"<OPAQUE_PATH>/"));
    }

    /// #131 load-bearing invariant: the grouped tee must produce EXACTLY the
    /// digest a plain blake3 hasher produces over the same update sequence —
    /// per-field tracing can never change a cache key.
    #[test]
    fn grouped_hasher_main_digest_matches_plain_blake3() {
        let mut plain = blake3::Hasher::new();
        let mut grouped = GroupedHasher::new("compiler");
        for (group, chunk) in [
            ("compiler", b"rustc_version:1.90".as_slice()),
            ("args", b"emit:link\n"),
            ("sources", b"source:abc\n"),
            ("args", b"RUSTFLAGS:-Copt-level=3\n"),
            ("link", b"linker:ld64\n"),
        ] {
            plain.update(chunk);
            grouped.set_group(group);
            grouped.update(chunk);
        }
        let (hash, fields) = grouped.finalize_with_fields();
        assert_eq!(hash, plain.finalize(), "grouping must not perturb the key");
        assert_eq!(
            fields.keys().collect::<Vec<_>>(),
            ["args", "compiler", "link", "sources"],
            "only groups that received bytes appear",
        );
        assert!(fields.values().all(|v| v.len() == KEY_FIELD_HEX));
    }

    /// #131: bytes route to the CURRENT group, non-contiguous segments of the
    /// same group accumulate, and only the touched group's digest changes.
    #[test]
    fn grouped_hasher_isolates_changes_to_their_group() {
        let build = |rustflags: &[u8]| {
            let mut h = GroupedHasher::new("compiler");
            h.update(b"rustc_version:1.90\n");
            h.set_group("sources");
            h.update(b"source:abc\n");
            h.set_group("args");
            h.update(b"emit:link\n");
            h.update(rustflags);
            h.finalize_with_fields()
        };
        let (key_a, fields_a) = build(b"RUSTFLAGS:-Copt-level=3\n");
        let (key_b, fields_b) = build(b"RUSTFLAGS:-Copt-level=2\n");
        assert_ne!(key_a, key_b);
        assert_ne!(fields_a["args"], fields_b["args"], "args group must differ");
        assert_eq!(fields_a["compiler"], fields_b["compiler"]);
        assert_eq!(fields_a["sources"], fields_b["sources"]);
    }

    fn parsed_crate_type(crate_type: &str, target: Option<&str>) -> RustcArgs {
        let mut argv = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "probe".to_string(),
            "--crate-type".to_string(),
            crate_type.to_string(),
            "src/lib.rs".to_string(),
        ];
        if let Some(target) = target {
            argv.push("--target".to_string());
            argv.push(target.to_string());
        }
        RustcArgs::parse(&argv).unwrap()
    }

    fn libc_fold_key(
        args: &RustcArgs,
        rustc_version: &str,
        running_on_linux: bool,
        signature: &str,
    ) -> Result<String> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"base-key");
        fold_native_host_libc_signature(
            &mut hasher,
            args,
            rustc_version,
            running_on_linux,
            |_| Ok(signature.to_string()),
        )?;
        Ok(hasher.finalize().to_hex().to_string())
    }

    #[test]
    fn native_linux_linked_outputs_key_host_libc_version() {
        for crate_type in ["bin", "dylib", "cdylib", "proc-macro"] {
            let args = parsed_crate_type(crate_type, None);
            let old = libc_fold_key(&args, GNU_RUSTC_VERSION, true, "2.36").unwrap();
            let new = libc_fold_key(&args, GNU_RUSTC_VERSION, true, "2.39").unwrap();
            assert_ne!(old, new, "{crate_type} must re-key across libc versions");
        }
    }

    #[test]
    fn rlibs_and_cross_targets_do_not_key_host_libc() {
        let rlib = parsed_crate_type("rlib", None);
        assert_eq!(
            libc_fold_key(&rlib, GNU_RUSTC_VERSION, true, "2.36").unwrap(),
            libc_fold_key(&rlib, GNU_RUSTC_VERSION, true, "2.39").unwrap(),
            "portable rlibs must not be tied to the host libc"
        );

        let cross = parsed_crate_type("bin", Some("aarch64-unknown-linux-gnu"));
        assert_eq!(
            libc_fold_key(&cross, GNU_RUSTC_VERSION, true, "2.36").unwrap(),
            libc_fold_key(&cross, GNU_RUSTC_VERSION, true, "2.39").unwrap(),
            "cross-target output must not be tied to the build host libc"
        );

        let explicit_native = parsed_crate_type("bin", Some("x86_64-unknown-linux-gnu"));
        assert_ne!(
            libc_fold_key(&explicit_native, GNU_RUSTC_VERSION, true, "2.36").unwrap(),
            libc_fold_key(&explicit_native, GNU_RUSTC_VERSION, true, "2.39").unwrap(),
            "an explicit rustc-host target is still a native output"
        );
    }

    #[test]
    fn host_libc_probe_is_linux_only_and_fails_closed() {
        let bin = parsed_crate_type("bin", None);
        assert_eq!(
            libc_fold_key(&bin, GNU_RUSTC_VERSION, false, "2.36").unwrap(),
            libc_fold_key(&bin, GNU_RUSTC_VERSION, false, "2.39").unwrap(),
            "non-Linux hosts must not gain a Linux libc component"
        );

        let mut hasher = blake3::Hasher::new();
        let err =
            fold_native_host_libc_signature(&mut hasher, &bin, GNU_RUSTC_VERSION, true, |_| {
                anyhow::bail!("probe failed")
            })
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("determining native Linux gnu-libc")
        );

        let missing_host = "rustc 1.90.0\nrelease: 1.90.0\n";
        let err = libc_fold_key(&bin, missing_host, true, "2.39").unwrap_err();
        assert!(err.to_string().contains("no host triple"));
    }

    #[test]
    fn metadata_only_outputs_do_not_probe_or_key_host_libc() {
        let mut metadata = parsed_crate_type("bin", None);
        metadata.emit = vec!["metadata".to_string()];
        let mut hasher = blake3::Hasher::new();
        fold_native_host_libc_signature(&mut hasher, &metadata, GNU_RUSTC_VERSION, true, |_| {
            panic!("metadata-only output must not probe libc")
        })
        .unwrap();

        let baseline = blake3::Hasher::new().finalize().to_hex().to_string();
        assert_eq!(hasher.finalize().to_hex().to_string(), baseline);
    }

    #[test]
    fn double_wrapper_uses_inner_rustc_host_banner() {
        let mut bin = parsed_crate_type("bin", None);
        bin.inner_rustc = Some(PathBuf::from("/toolchain/bin/rustc"));
        let version = rustc_version_for_native_link(&bin, "clippy 0.1.90\n", |path| {
            assert_eq!(path, Path::new("/toolchain/bin/rustc"));
            Ok(GNU_RUSTC_VERSION.to_string())
        })
        .unwrap();

        assert_eq!(
            rustc_host_triple(&version),
            Some("x86_64-unknown-linux-gnu")
        );
    }

    fn dummy_absolute_linker() -> String {
        std::env::temp_dir()
            .join("kache-dummy-cc")
            .to_string_lossy()
            .into_owned()
    }

    fn parsed_linked_bin(target: Option<&str>) -> RustcArgs {
        let mut argv = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "probe".to_string(),
            "--crate-type".to_string(),
            "bin".to_string(),
            "src/lib.rs".to_string(),
            format!("-Clinker={}", dummy_absolute_linker()),
        ];
        if let Some(target) = target {
            argv.push("--target".to_string());
            argv.push(target.to_string());
        }
        RustcArgs::parse(&argv).unwrap()
    }

    fn crt_fold_key(
        args: &RustcArgs,
        rustc_version: &str,
        linux: bool,
        macos: bool,
        crt: &str,
        sdk: &str,
        deployment_target: Option<&str>,
    ) -> Result<String> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"base-key");
        fold_native_link_runtime_identity(
            &mut hasher,
            args,
            rustc_version,
            linux,
            macos,
            |_| {
                let mut objects = BTreeMap::new();
                for pair in crt.split(';').filter(|pair| !pair.is_empty()) {
                    let (name, digest) = pair.split_once('=').unwrap();
                    objects.insert(name.to_string(), digest.to_string());
                }
                Ok(objects)
            },
            |_| Ok(sdk.to_string()),
            deployment_target.map(str::to_string),
        )?;
        Ok(hasher.finalize().to_hex().to_string())
    }

    #[test]
    fn native_linux_linked_outputs_key_crt_object_hashes() {
        let args = parsed_linked_bin(None);
        let old = crt_fold_key(
            &args,
            GNU_RUSTC_VERSION,
            true,
            false,
            "crt1.o=aaa;libc.so.6=bbb",
            "",
            None,
        )
        .unwrap();
        let new = crt_fold_key(
            &args,
            GNU_RUSTC_VERSION,
            true,
            false,
            "crt1.o=aaa;libc.so.6=ccc",
            "",
            None,
        )
        .unwrap();
        assert_ne!(old, new, "libc object bytes must re-key the native link");
    }

    #[test]
    fn rlibs_and_cross_targets_do_not_key_crt_or_sdk() {
        let rlib = parsed_crate_type("rlib", None);
        assert_eq!(
            crt_fold_key(
                &rlib,
                GNU_RUSTC_VERSION,
                true,
                true,
                "crt1.o=aaa;libc.so.6=bbb",
                "14.0 (a)",
                Some("11.0"),
            )
            .unwrap(),
            crt_fold_key(
                &rlib,
                GNU_RUSTC_VERSION,
                true,
                true,
                "crt1.o=zzz;libc.so.6=yyy",
                "15.0 (b)",
                Some("12.0"),
            )
            .unwrap(),
            "portable rlibs must not be tied to CRT/SDK identity"
        );

        let cross = parsed_linked_bin(Some("aarch64-unknown-linux-gnu"));
        assert_eq!(
            crt_fold_key(
                &cross,
                GNU_RUSTC_VERSION,
                true,
                false,
                "crt1.o=aaa;libc.so.6=bbb",
                "",
                None,
            )
            .unwrap(),
            crt_fold_key(
                &cross,
                GNU_RUSTC_VERSION,
                true,
                false,
                "crt1.o=zzz;libc.so.6=yyy",
                "",
                None,
            )
            .unwrap(),
            "cross-target output must not be tied to the build host CRT"
        );
    }

    #[test]
    fn native_linux_crt_probe_fails_closed() {
        let bin = parsed_linked_bin(None);
        let mut hasher = blake3::Hasher::new();
        let err = fold_native_link_runtime_identity(
            &mut hasher,
            &bin,
            GNU_RUSTC_VERSION,
            true,
            false,
            |_| anyhow::bail!("no startup object"),
            |_| unreachable!("linux fold must not probe the macOS SDK"),
            None,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("determining native Linux CRT/libc identity")
        );
    }

    #[test]
    fn native_macos_linked_outputs_key_sdk_identity() {
        let args = parsed_linked_bin(None);
        let old = crt_fold_key(
            &args,
            DARWIN_RUSTC_VERSION,
            false,
            true,
            "",
            "14.0 (23A344)",
            None,
        )
        .unwrap();
        let new = crt_fold_key(
            &args,
            DARWIN_RUSTC_VERSION,
            false,
            true,
            "",
            "15.0 (24A348)",
            None,
        )
        .unwrap();
        assert_ne!(old, new, "SDK identity must re-key the native macOS link");

        let with_dt = crt_fold_key(
            &args,
            DARWIN_RUSTC_VERSION,
            false,
            true,
            "",
            "14.0 (23A344)",
            Some("11.0"),
        )
        .unwrap();
        assert_ne!(
            old, with_dt,
            "MACOSX_DEPLOYMENT_TARGET must re-key when set"
        );
    }

    const WINDOWS_RUSTC_VERSION: &str =
        "rustc 1.90.0\nhost: x86_64-pc-windows-msvc\nrelease: 1.90.0\n";
    const WINDOWS_GNU_RUSTC_VERSION: &str =
        "rustc 1.90.0\nhost: x86_64-pc-windows-gnu\nrelease: 1.90.0\n";

    fn windows_fold_key_on_host(
        args: &RustcArgs,
        version: &str,
        identity: &str,
        running_on_windows: bool,
    ) -> Result<String> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"base-key");
        fold_native_windows_msvc_identity(
            &mut hasher,
            args,
            version,
            running_on_windows,
            |_, _| Ok(identity.to_string()),
        )?;
        Ok(hasher.finalize().to_hex().to_string())
    }

    fn windows_fold_key(args: &RustcArgs, version: &str, identity: &str) -> Result<String> {
        windows_fold_key_on_host(args, version, identity, true)
    }

    #[test]
    fn native_windows_msvc_identity_keys_only_native_link_outputs() {
        let bin = parsed_crate_type("bin", None);
        let old = windows_fold_key(&bin, WINDOWS_RUSTC_VERSION, "toolset=14.4").unwrap();
        let new = windows_fold_key(&bin, WINDOWS_RUSTC_VERSION, "toolset=14.5").unwrap();
        assert_ne!(old, new);

        let mut metadata = bin.clone();
        metadata.emit = vec!["metadata".into()];
        assert_eq!(
            windows_fold_key(&metadata, WINDOWS_RUSTC_VERSION, "probe must not run").unwrap(),
            windows_fold_key(&metadata, WINDOWS_RUSTC_VERSION, "anything").unwrap()
        );

        let cross = parsed_crate_type("bin", Some("aarch64-pc-windows-msvc"));
        assert!(
            windows_fold_key(&cross, WINDOWS_RUSTC_VERSION, "probe must not run").is_err(),
            "cross-target Windows MSVC links must pass through until target identity is modeled"
        );

        let gnu = parsed_crate_type("bin", Some("x86_64-pc-windows-gnu"));
        assert_eq!(
            windows_fold_key(&gnu, WINDOWS_RUSTC_VERSION, "probe must not run").unwrap(),
            windows_fold_key(&gnu, WINDOWS_RUSTC_VERSION, "anything").unwrap()
        );

        let rlib = parsed_crate_type("rlib", None);
        assert_eq!(
            windows_fold_key(&rlib, WINDOWS_RUSTC_VERSION, "probe must not run").unwrap(),
            windows_fold_key(&rlib, WINDOWS_RUSTC_VERSION, "anything").unwrap()
        );

        assert_eq!(
            windows_fold_key_on_host(&bin, WINDOWS_RUSTC_VERSION, "probe must not run", false)
                .unwrap(),
            windows_fold_key_on_host(&bin, WINDOWS_RUSTC_VERSION, "anything", false).unwrap(),
            "a non-Windows host must not probe native MSVC inputs"
        );
    }

    #[test]
    fn native_windows_msvc_detection_requires_a_windows_linked_executable() {
        let bin = parsed_crate_type("bin", None);
        let mut metadata = bin.clone();
        metadata.emit = vec!["metadata".into()];
        let rlib = parsed_crate_type("rlib", None);

        for (args, running_on_windows) in [(&bin, false), (&metadata, true), (&rlib, true)] {
            assert!(
                !is_native_windows_msvc_link(
                    args,
                    WINDOWS_RUSTC_VERSION,
                    running_on_windows,
                    |_| unreachable!("the supplied rustc version must be reused"),
                )
                .unwrap()
            );
        }

        // Both halves of the admission must hold: the target must be the host
        // and the host must be MSVC. A cross target on an MSVC host and a
        // native build on a GNU host each fail one half.
        let cross = parsed_crate_type("bin", Some("x86_64-unknown-linux-gnu"));
        for (args, version) in [
            (&cross, WINDOWS_RUSTC_VERSION),
            (&bin, WINDOWS_GNU_RUSTC_VERSION),
        ] {
            assert!(
                !is_native_windows_msvc_link(args, version, true, |_| unreachable!(
                    "the supplied rustc version must be reused"
                ))
                .unwrap(),
                "{version:?} must not admit {:?}",
                args.target
            );
        }
    }

    #[test]
    fn native_windows_msvc_ignores_unrelated_generic_cc_identity() {
        let bin = parsed_crate_type("bin", None);
        assert!(
            is_native_windows_msvc_link(&bin, WINDOWS_RUSTC_VERSION, true, |_| unreachable!(
                "non-nested rustc must use the supplied version"
            ),)
            .unwrap()
        );

        let fold = |identity: &str| {
            let mut hasher = blake3::Hasher::new();
            hasher.update(b"base-key");
            fold_generic_linker_identity(&mut hasher, &bin, true, |_| Some(identity.to_string()));
            hasher.finalize()
        };
        assert_eq!(
            fold("unrelated MinGW cc"),
            fold("no cc installed"),
            "native MSVC keys must not depend on an unrelated generic cc probe"
        );

        let mut generic = blake3::Hasher::new();
        generic.update(b"base-key");
        fold_generic_linker_identity(&mut generic, &bin, false, |_| {
            Some("actual generic linker".into())
        });
        assert_ne!(generic.finalize(), fold("anything"));
    }

    #[test]
    fn native_windows_msvc_identity_probe_failure_is_cache_failure() {
        let bin = parsed_crate_type("bin", None);
        let mut hasher = blake3::Hasher::new();
        let error = fold_native_windows_msvc_identity(
            &mut hasher,
            &bin,
            WINDOWS_RUSTC_VERSION,
            true,
            |_, _| anyhow::bail!("ambiguous toolchain"),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("native Windows MSVC link identity")
        );
    }

    #[test]
    fn windows_native_link_search_dirs_include_l_and_libpath_and_reject_ambiguity() {
        let dir = tempfile::tempdir().unwrap();
        let native = dir.path().join("native");
        let libpath = dir.path().join("libpath");
        std::fs::create_dir_all(&native).unwrap();
        std::fs::create_dir_all(&libpath).unwrap();
        // `-l static=foo` resolves to `foo.lib` through the modeled search
        // dirs and is hashed by the identity; only `-C link-arg` inputs are
        // classified.
        let args = RustcArgs::parse(&[
            "rustc".into(),
            "--crate-type=bin".into(),
            "-l".into(),
            "static=foo".into(),
            "-L".into(),
            format!("native={}", native.display()),
            format!("-Clink-arg=/LIBPATH:{}", libpath.display()),
        ])
        .unwrap();
        assert_eq!(args.link_libs, vec!["static=foo".to_string()]);
        assert_eq!(
            windows_native_link_search_dirs(&args).unwrap(),
            WindowsNativeLinkSearchDirs {
                rustc: vec![native.clone()],
                linker: vec![native, libpath],
            }
        );

        let ambiguous = RustcArgs::parse(&[
            "rustc".into(),
            "--crate-type=bin".into(),
            "-Clink-args=/DEFAULTLIB:foo /LIBPATH".into(),
        ])
        .unwrap();
        assert!(windows_native_link_search_dirs(&ambiguous).is_err());

        for linker_arg in [
            "foo.lib",
            "/DEFAULTLIB:foo",
            "-defaultlib:foo",
            "/DEFAULTLIB:foo.lib",
            "app.res",
            "APP.RES",
            "extra.obj",
            "exports.exp",
            "/DEF:exports.def",
            "-def:exports.def",
            "-Wl,/def:exports.def",
            "/MANIFESTINPUT:extra.manifest",
            "/MANIFESTFILE:app.exe.manifest",
            "/PDBSTRIPPED:app.public.pdb",
        ] {
            let args = RustcArgs::parse(&[
                "rustc".into(),
                "--crate-type=bin".into(),
                format!("-Clink-arg={linker_arg}"),
            ])
            .unwrap();
            let error = windows_native_link_search_dirs(&args).unwrap_err();
            assert!(
                error.to_string().contains("not hashed"),
                "unmodeled Windows link input must fail closed: {linker_arg}: {error:#}"
            );
        }
    }

    #[test]
    fn windows_libpath_parser_accepts_one_exact_argument() {
        for (argument, expected) in [
            (r"/LIBPATH:C:\sdk\lib", r"C:\sdk\lib"),
            (r"/libpath=C:\sdk\lib", r"C:\sdk\lib"),
            (r"-LIBPATH:C:\lld\lib", r"C:\lld\lib"),
            (r"-libpath=C:\lld\lib", r"C:\lld\lib"),
            (
                r#"/LIBPATH:"C:\Program Files\SDK\lib""#,
                r"C:\Program Files\SDK\lib",
            ),
            (
                r#"-Wl,/LIBPATH:"C:\Program Files\SDK\lib""#,
                r"C:\Program Files\SDK\lib",
            ),
            (
                r#"-Wl,-LiBpAtH:"C:\Program Files\LLVM\lib""#,
                r"C:\Program Files\LLVM\lib",
            ),
        ] {
            assert_eq!(
                windows_libpath_argument(argument).unwrap().as_deref(),
                Some(expected),
                "{argument}"
            );
        }
        assert_eq!(windows_libpath_argument("/DEBUG").unwrap(), None);

        for argument in [
            "/LIBPATH:",
            r#"/LIBPATH:"C:\unterminated"#,
            r#"/LIBPATH:"C:\sdk\lib" /DEBUG"#,
            r"/LIBPATH:C:\Program Files\SDK\lib",
            r"/DEBUG /LIBPATH:C:\sdk\lib",
            r"/DEBUG -LIBPATH:C:\lld\lib",
            r"-Wl,/DEBUG,-LIBPATH:C:\lld\lib",
        ] {
            assert!(
                windows_libpath_argument(argument).is_err(),
                "ambiguous or empty argument must fail closed: {argument}"
            );
        }
    }

    #[test]
    fn windows_lld_libpath_preserves_shadowing_order() {
        let directory = tempfile::tempdir().unwrap();
        let first = directory.path().join("lld-first");
        let second = directory.path().join("link-second");
        std::fs::create_dir_all(&first).unwrap();
        std::fs::create_dir_all(&second).unwrap();
        std::fs::write(first.join("shadowed.lib"), b"first").unwrap();
        std::fs::write(second.join("shadowed.lib"), b"second").unwrap();
        let args = RustcArgs::parse(&[
            "rustc".into(),
            "--crate-type=bin".into(),
            format!("-Clink-arg=-LIBPATH:{}", first.display()),
            format!("-Clink-arg=/LIBPATH:{}", second.display()),
        ])
        .unwrap();

        assert_eq!(
            windows_native_link_search_dirs(&args).unwrap(),
            WindowsNativeLinkSearchDirs {
                rustc: Vec::new(),
                linker: vec![first, second],
            }
        );
    }

    #[test]
    fn windows_native_link_search_dirs_honor_only_linker_visible_l_kinds() {
        let directory = tempfile::tempdir().unwrap();
        let native = directory.path().join("native");
        let all = directory.path().join("all");
        let bare = directory.path().join("bare");
        let libpath = directory.path().join("path with spaces");
        let unknown = format!("custom={}", directory.path().join("unknown").display());
        for path in [&native, &all, &bare, &libpath] {
            std::fs::create_dir_all(path).unwrap();
        }
        let args = RustcArgs::parse(&[
            "rustc".into(),
            "--crate-type=bin".into(),
            "-L".into(),
            format!("native={}", native.display()),
            format!("-Lall={}", all.display()),
            format!("-L{}", bare.display()),
            format!("-L{unknown}"),
            format!(
                "-Ldependency={}",
                directory.path().join("dependency").display()
            ),
            format!("-Lcrate={}", directory.path().join("crate").display()),
            format!(
                "-Lframework={}",
                directory.path().join("framework").display()
            ),
            format!(r#"-Clink-arg=/LIBPATH:"{}""#, libpath.display()),
            "-Clink-arg=/DEBUG".into(),
        ])
        .unwrap();
        assert_eq!(
            windows_native_link_search_dirs(&args).unwrap(),
            WindowsNativeLinkSearchDirs {
                rustc: vec![
                    native.clone(),
                    all.clone(),
                    bare.clone(),
                    PathBuf::from(unknown.clone())
                ],
                linker: vec![native, all, bare, PathBuf::from(unknown), libpath],
            }
        );
    }

    #[test]
    fn native_macos_sdk_probe_fails_closed() {
        let bin = parsed_linked_bin(None);
        let mut hasher = blake3::Hasher::new();
        let err = fold_native_link_runtime_identity(
            &mut hasher,
            &bin,
            DARWIN_RUSTC_VERSION,
            false,
            true,
            |_| unreachable!("macOS fold must not probe Linux CRT"),
            |_| anyhow::bail!("sdk missing"),
            None,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("determining macOS SDK identity for cache key")
        );
    }

    #[test]
    fn metadata_only_outputs_do_not_probe_crt_or_sdk() {
        let mut metadata = parsed_linked_bin(None);
        metadata.emit = vec!["metadata".to_string()];
        let mut hasher = blake3::Hasher::new();
        fold_native_link_runtime_identity(
            &mut hasher,
            &metadata,
            GNU_RUSTC_VERSION,
            true,
            true,
            |_| panic!("metadata-only output must not probe CRT"),
            |_| panic!("metadata-only output must not probe SDK"),
            None,
        )
        .unwrap();
    }

    #[test]
    fn crt_fold_requires_linux_os_and_linux_rustc_host() {
        let args = parsed_linked_bin(None);
        let a = crt_fold_key(
            &args,
            DARWIN_RUSTC_VERSION,
            true,
            false,
            "crt1.o=aaa;libc.so.6=bbb",
            "",
            None,
        )
        .unwrap();
        let b = crt_fold_key(
            &args,
            DARWIN_RUSTC_VERSION,
            true,
            false,
            "crt1.o=zzz;libc.so.6=yyy",
            "",
            None,
        )
        .unwrap();
        assert_eq!(
            a, b,
            "a Darwin rustc hosted on Linux must not fold Linux CRT objects"
        );
    }

    #[test]
    fn sdk_fold_requires_macos_os_and_darwin_rustc_host() {
        let args = parsed_linked_bin(None);
        let a = crt_fold_key(&args, GNU_RUSTC_VERSION, false, true, "", "14.0 (a)", None).unwrap();
        let b = crt_fold_key(&args, GNU_RUSTC_VERSION, false, true, "", "15.0 (b)", None).unwrap();
        assert_eq!(
            a, b,
            "a GNU rustc hosted on macOS must not fold the Darwin SDK"
        );
    }

    #[test]
    fn windows_hosts_do_not_key_linux_crt_or_macos_sdk() {
        let bin = parsed_linked_bin(None);
        assert_eq!(
            crt_fold_key(
                &bin,
                GNU_RUSTC_VERSION,
                false,
                false,
                "crt1.o=aaa;libc.so.6=bbb",
                "14.0 (a)",
                Some("11.0"),
            )
            .unwrap(),
            crt_fold_key(
                &bin,
                DARWIN_RUSTC_VERSION,
                false,
                false,
                "crt1.o=zzz;libc.so.6=yyy",
                "15.0 (b)",
                Some("12.0"),
            )
            .unwrap(),
            "Windows hosts keep the existing linker --version identity only"
        );
    }

    #[test]
    fn libc_probe_output_parsing_is_strict_and_canonical() {
        assert_eq!(
            parse_getconf_gnu_libc("glibc 2.39\n").as_deref(),
            Some("2.39")
        );
        assert_eq!(parse_getconf_gnu_libc("musl 1.2.5\n"), None);
        assert_eq!(parse_getconf_gnu_libc("glibc unknown\n"), None);

        assert_eq!(
            parse_ldd_libc("ldd (Debian GLIBC 2.36-9) 2.36\nCopyright ..."),
            Some((LinuxLibcFamily::Gnu, "2.36".to_string()))
        );
        assert_eq!(
            parse_ldd_libc("musl libc (x86_64)\nVersion 1.2.5\nDynamic Program Loader"),
            Some((LinuxLibcFamily::Musl, "1.2.5".to_string()))
        );
        assert_eq!(parse_ldd_libc("BusyBox ldd\n"), None);
    }

    #[test]
    fn is_valid_cache_key_accepts_real_blake3_hex() {
        // A real key is 64 lowercase hex chars (blake3 to_hex).
        let key = fold_labeled("seed".into(), "label", "value");
        assert_eq!(key.len(), 64);
        assert!(is_valid_cache_key(&key));
        assert!(is_valid_cache_key(&"a".repeat(64)));
        assert!(is_valid_cache_key(&"0123456789abcdef".repeat(4)));
    }

    #[test]
    fn apply_key_salt_no_salt_is_identity() {
        let base = "deadbeef".to_string();
        // None and empty/whitespace are both treated as "unsalted" and
        // must return the base key byte-for-byte (no CACHE_KEY_VERSION
        // bump, no effect for projects that never set it).
        assert_eq!(apply_key_salt(base.clone(), None, "crate"), base);
        assert_eq!(apply_key_salt(base.clone(), Some(""), "crate"), base);
    }

    #[test]
    fn apply_key_salt_changes_key_and_is_salt_specific() {
        let base = "deadbeef".to_string();
        let a = apply_key_salt(base.clone(), Some("toolchain-A"), "crate");
        let b = apply_key_salt(base.clone(), Some("toolchain-B"), "crate");
        // A salt re-keys, and distinct salts produce distinct keys.
        assert_ne!(a, base);
        assert_ne!(b, base);
        assert_ne!(a, b);
        // Deterministic: same (base, salt) → same key.
        assert_eq!(a, apply_key_salt(base, Some("toolchain-A"), "crate"));
    }

    /// Set `name` for the duration of the guard, restoring the previous value
    /// (or absence) on drop. Callers must hold [`key_test_lock`]: the process
    /// environment is global and `apply_key_env_vars` reads all of it.
    struct ScopedEnv {
        name: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl ScopedEnv {
        fn set(name: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(name);
            // SAFETY: single-threaded test body under `key_test_lock`.
            unsafe { std::env::set_var(name, value) };
            Self { name, previous }
        }

        fn unset(name: &'static str) -> Self {
            let previous = std::env::var_os(name);
            // SAFETY: single-threaded test body under `key_test_lock`.
            unsafe { std::env::remove_var(name) };
            Self { name, previous }
        }
    }

    impl Drop for ScopedEnv {
        fn drop(&mut self) {
            // SAFETY: single-threaded test body under `key_test_lock`.
            unsafe {
                match self.previous.take() {
                    Some(value) => std::env::set_var(self.name, value),
                    None => std::env::remove_var(self.name),
                }
            }
        }
    }

    #[test]
    fn key_env_var_matches_exact_prefix_and_case() {
        let patterns = vec!["BOLTFFI_*".to_string(), "MODE".to_string()];
        // Trailing `*` is a prefix glob, including the degenerate zero-suffix case.
        assert!(key_env_var_matches(&patterns, "BOLTFFI_BINDING_EXPANSION"));
        assert!(key_env_var_matches(&patterns, "BOLTFFI_"));
        // A bare name matches only itself, not names it is a prefix of.
        assert!(key_env_var_matches(&patterns, "MODE"));
        assert!(!key_env_var_matches(&patterns, "MODE_EXTRA"));
        assert!(!key_env_var_matches(&patterns, "BOLTFF"));
        assert!(!key_env_var_matches(&patterns, "UNRELATED"));
        // ASCII case-insensitive, so a Windows environment behaves like a Unix one.
        assert!(key_env_var_matches(&patterns, "mode"));
        assert!(key_env_var_matches(&patterns, "boltffi_root"));
    }

    #[test]
    fn key_env_var_matches_treats_interior_star_literally() {
        // Only a trailing `*` globs; an interior one is a literal character that
        // no real env var name carries. `normalize_key_env_vars` warns about it.
        let patterns = vec!["A*B".to_string()];
        assert!(!key_env_var_matches(&patterns, "AXB"));
        assert!(!key_env_var_matches(&patterns, "AB"));
        assert!(key_env_var_matches(&patterns, "A*B"));
    }

    #[test]
    fn apply_key_env_vars_no_patterns_is_identity() {
        let base = "deadbeef".to_string();
        // Feature off must leave the key byte-for-byte unchanged — that is what
        // lets this ship without a CACHE_KEY_VERSION bump.
        assert_eq!(apply_key_env_vars(base.clone(), &[], "crate"), base);
        assert_eq!(key_env_guard(&[]), None);
    }

    #[test]
    fn adaptive_key_env_guard_changes_with_the_selected_value() {
        let _lock = key_test_lock();
        let patterns = vec!["KACHE_TEST_ADAPTIVE_ENV".to_string()];
        let first = {
            let _guard = ScopedEnv::set("KACHE_TEST_ADAPTIVE_ENV", "one");
            key_env_guard(&patterns).unwrap()
        };
        let second = {
            let _guard = ScopedEnv::set("KACHE_TEST_ADAPTIVE_ENV", "two");
            key_env_guard(&patterns).unwrap()
        };
        assert_ne!(first, second);
    }

    #[test]
    fn apply_key_env_vars_separates_set_from_unset() {
        let _lock = key_test_lock();
        let base = "deadbeef".to_string();
        let patterns = vec!["KACHE_TEST_EXPANSION".to_string()];

        let unset = {
            let _guard = ScopedEnv::unset("KACHE_TEST_EXPANSION");
            apply_key_env_vars(base.clone(), &patterns, "crate")
        };
        let set_empty = {
            let _guard = ScopedEnv::set("KACHE_TEST_EXPANSION", "");
            apply_key_env_vars(base.clone(), &patterns, "crate")
        };
        let set_one = {
            let _guard = ScopedEnv::set("KACHE_TEST_EXPANSION", "1");
            apply_key_env_vars(base.clone(), &patterns, "crate")
        };
        let set_two = {
            let _guard = ScopedEnv::set("KACHE_TEST_EXPANSION", "2");
            apply_key_env_vars(base.clone(), &patterns, "crate")
        };

        // The #635 case: a proc macro branching on this var produces different
        // artifacts from a byte-identical rustc command line, so the two modes
        // must not share a key.
        assert_ne!(unset, set_one);
        assert_ne!(set_one, set_two);
        // `var("X")` distinguishes unset from set-to-empty, so the key must too.
        assert_ne!(unset, set_empty);
        // Declaring the var re-keys even when it is unset. Without this the
        // unset build would land back on the poisoned entry that motivated
        // the declaration in the first place.
        assert_ne!(unset, base);
    }

    #[test]
    fn apply_key_env_vars_matches_by_prefix_glob() {
        let _lock = key_test_lock();
        let base = "deadbeef".to_string();
        let patterns = vec!["KACHE_TEST_PREFIX_*".to_string()];

        let none = {
            let _a = ScopedEnv::unset("KACHE_TEST_PREFIX_MODE");
            apply_key_env_vars(base.clone(), &patterns, "crate")
        };
        let one = {
            let _a = ScopedEnv::set("KACHE_TEST_PREFIX_MODE", "on");
            apply_key_env_vars(base.clone(), &patterns, "crate")
        };
        assert_ne!(none, one);
    }

    #[test]
    fn apply_key_env_vars_is_declaration_order_and_case_independent() {
        use crate::config::normalize_key_env_vars;

        let _lock = key_test_lock();
        let base = "deadbeef".to_string();
        let _a = ScopedEnv::set("KACHE_TEST_ORDER_A", "1");
        let _b = ScopedEnv::set("KACHE_TEST_ORDER_B", "2");

        // The patterns are folded, so any two lists that SELECT the same
        // variables have to reduce to the same bytes — otherwise the feature
        // splits the cache between teammates instead of correcting it.
        // `normalize_key_env_vars` (applied by `Config::load`) does the
        // canonicalizing; this pins that the fold actually depends on it.
        let canonical = normalize_key_env_vars(
            [
                "KACHE_TEST_ORDER_A".to_string(),
                "KACHE_TEST_ORDER_B".to_string(),
            ],
            "test",
        );
        let expected = apply_key_env_vars(base.clone(), &canonical, "crate");
        for spelling in [
            // reordered
            vec![
                "KACHE_TEST_ORDER_B".to_string(),
                "KACHE_TEST_ORDER_A".to_string(),
            ],
            // differently cased (matching is case-insensitive)
            vec![
                "kache_test_order_a".to_string(),
                "Kache_Test_Order_B".to_string(),
            ],
            // duplicated, with stray whitespace
            vec![
                " KACHE_TEST_ORDER_A ".to_string(),
                "KACHE_TEST_ORDER_A".to_string(),
                "KACHE_TEST_ORDER_B".to_string(),
            ],
        ] {
            let normalized = normalize_key_env_vars(spelling.clone(), "test");
            assert_eq!(
                apply_key_env_vars(base.clone(), &normalized, "crate"),
                expected,
                "equivalent declaration {spelling:?} must fold identically"
            );
        }
    }

    /// `(name, value)` pair from string literals, for the digest tests.
    fn pair(name: &str, value: &str) -> (Vec<u8>, Vec<u8>) {
        (name.as_bytes().to_vec(), value.as_bytes().to_vec())
    }

    #[test]
    fn key_env_digest_ignores_environ_order() {
        let patterns = vec!["X*".to_string()];
        // `vars_os` order is platform-defined. Two machines listing the same
        // variables in a different order describe the same environment and must
        // land on the same entry, or the feature splits the cache by host.
        let forward = vec![pair("XA", "1"), pair("XB", "2"), pair("XC", "3")];
        let shuffled = vec![pair("XC", "3"), pair("XA", "1"), pair("XB", "2")];
        assert_eq!(
            key_env_digest(&patterns, forward),
            key_env_digest(&patterns, shuffled)
        );
    }

    #[test]
    fn key_env_digest_preserves_order_when_a_name_repeats() {
        let patterns = vec!["X*".to_string()];
        // A duplicated name is only reachable through a hand-built `envp`, but
        // there `getenv` returns the FIRST occurrence — so the order is
        // semantically observable and sorting it away would be a wrong-hit.
        let first_wins = vec![pair("XA", "1"), pair("XA", "2")];
        let second_wins = vec![pair("XA", "2"), pair("XA", "1")];
        assert_ne!(
            key_env_digest(&patterns, first_wins),
            key_env_digest(&patterns, second_wins)
        );
    }

    #[test]
    fn key_env_digest_separates_names_from_values() {
        let patterns = vec!["X*".to_string()];
        // Swapping which name holds which value is a different environment.
        // Folding the pairs (rather than a bag of values) is what pins that.
        assert_ne!(
            key_env_digest(&patterns, vec![pair("XA", "1"), pair("XB", "2")]),
            key_env_digest(&patterns, vec![pair("XA", "2"), pair("XB", "1")])
        );
    }

    #[test]
    fn env_name_key_bytes_distinguishes_names() {
        use std::ffi::OsStr;
        let a = env_name_key_bytes(OsStr::new("KACHE_TEST_NAME_A"));
        let b = env_name_key_bytes(OsStr::new("KACHE_TEST_NAME_B"));
        // A name that folded to a constant would merge every declared variable
        // into one key component.
        assert!(!a.is_empty());
        assert_ne!(a, b);
    }

    #[test]
    fn env_name_key_bytes_case_policy_follows_the_platform() {
        use std::ffi::OsStr;
        let upper = env_name_key_bytes(OsStr::new("KACHE_TEST_CASE"));
        let mixed = env_name_key_bytes(OsStr::new("Kache_Test_Case"));
        if cfg!(windows) {
            // One variable on Windows: folding the OS's reported casing would
            // split the cache between machines describing the same environment.
            assert_eq!(upper, mixed);
        } else {
            // Two genuinely different variables on Unix.
            assert_ne!(upper, mixed);
        }
    }

    #[test]
    fn env_os_key_bytes_distinguishes_values() {
        use std::ffi::OsStr;
        assert_ne!(
            env_os_key_bytes(OsStr::new("expansion")),
            env_os_key_bytes(OsStr::new("normal"))
        );
        assert!(env_os_key_bytes(OsStr::new("")).is_empty());
    }

    /// Every rustup mechanism that can redirect an unchanged shim binary
    /// must move the tool-version memo key: `RUSTUP_TOOLCHAIN`, the
    /// nearest `rust-toolchain{,.toml}` up from the cwd, and the
    /// `settings.toml` behind `rustup default`.
    #[test]
    fn toolchain_selector_fingerprint_tracks_every_selection_source() {
        use std::ffi::OsStr;
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("workspace").join("member");
        std::fs::create_dir_all(&project).unwrap();

        let base = toolchain_selector_fingerprint(None, Some(&project), None);
        assert_eq!(base, "", "no selection state folds to a stable empty");

        let pinned =
            toolchain_selector_fingerprint(Some(OsStr::new("1.93.0")), Some(&project), None);
        assert_ne!(pinned, base);
        assert_ne!(
            toolchain_selector_fingerprint(Some(OsStr::new("nightly")), Some(&project), None),
            pinned,
            "different overrides must fingerprint differently"
        );

        // The nearest directory with a toolchain file ends the ancestor
        // walk, and BOTH spellings fold so rustup's precedence between
        // them never matters: editing either one moves the fingerprint.
        std::fs::write(dir.path().join("workspace").join("rust-toolchain"), "1.88").unwrap();
        std::fs::write(
            dir.path().join("workspace").join("rust-toolchain.toml"),
            "[toolchain]\nchannel = \"1.90\"\n",
        )
        .unwrap();
        let with_files = toolchain_selector_fingerprint(None, Some(&project), None);
        assert_eq!(with_files.matches(";file:").count(), 2, "{with_files}");
        std::fs::write(dir.path().join("workspace").join("rust-toolchain"), "1.89").unwrap();
        let with_edited = toolchain_selector_fingerprint(None, Some(&project), None);
        assert_ne!(
            with_edited, with_files,
            "editing the bare file must move the fingerprint even though \
             the .toml sibling is untouched"
        );

        let settings = dir.path().join("settings.toml");
        std::fs::write(&settings, "default_toolchain = \"stable\"").unwrap();
        let with_default = toolchain_selector_fingerprint(None, Some(&project), Some(&settings));
        assert!(with_default.contains("default:"), "{with_default}");
        assert_ne!(with_default, base);
        std::fs::write(&settings, "default_toolchain = \"beta\"").unwrap();
        assert_ne!(
            toolchain_selector_fingerprint(None, Some(&project), Some(&settings)),
            with_default,
            "a rustup default switch must move the fingerprint"
        );
    }

    /// Both halves have to hold: a build that asked for predictions but has no
    /// index DB (the daemon's store-free hasher) cannot read one.
    #[test]
    fn predictions_need_both_the_request_and_a_table() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.db");
        assert!(
            FileHasher::persistent(&db)
                .with_input_predictions(true)
                .uses_input_predictions()
        );
        assert!(
            !FileHasher::persistent(&db).uses_input_predictions(),
            "a hasher nobody asked must not read records"
        );
        assert!(
            !FileHasher::new()
                .with_input_predictions(true)
                .uses_input_predictions(),
            "asking is not enough without a table to read"
        );
        assert!(!FileHasher::new().uses_input_predictions());
    }

    /// Each refusal reaches a human only through the key trace, so the names
    /// have to be distinct and stable enough to grep a build log for.
    #[test]
    fn every_rejection_names_itself_distinctly() {
        let all = [
            Rejection::Disabled,
            Rejection::NotEligible,
            Rejection::NoRecord,
            Rejection::Missing,
            Rejection::NotRegular,
            Rejection::EnvChanged,
            Rejection::Sibling,
        ];
        let mut names: Vec<&str> = all.iter().map(|r| r.as_str()).collect();
        assert!(
            names.iter().all(|name| !name.is_empty()),
            "a nameless refusal tells a reader nothing: {names:?}"
        );
        let before = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(before, names.len(), "two refusals share a name: {names:?}");
        assert_eq!(Rejection::Disabled.as_str(), "disabled");
        assert_eq!(Rejection::Sibling.as_str(), "sibling");
    }

    /// The wrapper reads this to decide whether a key still owes a
    /// re-derivation, and a stale `true` would make it recompute a key that
    /// was never predicted.
    #[test]
    fn the_prediction_marker_is_taken_once() {
        assert!(
            !take_last_key_used_prediction(),
            "nothing has been predicted on this thread"
        );
        LAST_KEY_USED_PREDICTION.with(|stash| stash.set(true));
        assert!(take_last_key_used_prediction());
        assert!(
            !take_last_key_used_prediction(),
            "taking must clear it, or the next key inherits this one's answer"
        );
    }

    /// With no record and deferral allowed, the key stops instead of running
    /// the pre-pass; the closure the wrapper hands back afterwards is used as
    /// is.
    #[test]
    fn discovery_defers_to_the_compile_and_takes_the_emitted_closure() {
        let _lock = key_test_lock();
        // Exercise a unit without generated inputs. Cargo and nextest set
        // kache's own OUT_DIR on the test process, so clear it for this unit.
        let out_dir = std::env::var_os("OUT_DIR");
        // SAFETY: the key-test lock serialises environment edits.
        unsafe { std::env::remove_var("OUT_DIR") };
        struct RestoreOutDir(Option<std::ffi::OsString>);
        impl Drop for RestoreOutDir {
            fn drop(&mut self) {
                if let Some(value) = self.0.take() {
                    // SAFETY: still under the key-test lock, dropped first.
                    unsafe { std::env::set_var("OUT_DIR", value) };
                }
            }
        }
        let _restore = RestoreOutDir(out_dir);
        if get_rustc_version(Path::new("rustc")).is_err() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.db");
        let out_dir = dir.path().join("target/debug/deps");
        let args = RustcArgs::parse(
            &[
                "rustc",
                "--crate-name",
                "x",
                "src/lib.rs",
                "--edition",
                "2021",
                "--emit=dep-info,metadata",
                "--out-dir",
                &out_dir.display().to_string(),
            ]
            .iter()
            .map(|a| (*a).to_string())
            .collect::<Vec<_>>(),
        )
        .unwrap();
        assert!(
            rustc_shared_prediction_identity(&args).is_some(),
            "a target directory gives the unit a shared record identity"
        );
        rusqlite::Connection::open(&db)
            .unwrap()
            .execute_batch(
                "CREATE TABLE entries (cache_key TEXT PRIMARY KEY, crate_name TEXT NOT NULL);",
            )
            .unwrap();
        let on = FileHasher::persistent(&db)
            .with_input_predictions(true)
            .with_prediction_flights(Some(dir.path().join("cache")));

        set_defer_discovery(true);
        let deferred = resolve_key_inputs(&args, &on, "x");
        set_defer_discovery(false);
        let error = deferred.expect_err("no record and deferral allowed: no pre-pass");
        assert!(
            error.downcast_ref::<DeferredDiscovery>().is_some(),
            "{error:#}"
        );
        // A broken handoff must fail instead of waiting on our own flight.
        drop(on.take_discovery_flight());

        let emitted = dir.path().join("lib.d");
        std::fs::write(
            &emitted,
            "/w/target/debug/deps/lib.rmeta: /w/src/lib.rs /w/src/inner.rs\n\n\
             /w/src/lib.rs:\n/w/src/inner.rs:\n# env-dep:CARGO_PKG_NAME=lib\n",
        )
        .unwrap();
        let closure = dep_info_from_emitted(&emitted, Path::new("/w/src/lib.rs")).unwrap();
        assert_eq!(
            closure.source_files,
            vec![
                PathBuf::from("/w/src/inner.rs"),
                PathBuf::from("/w/src/lib.rs")
            ]
        );
        assert_eq!(
            closure.env_deps,
            vec![("CARGO_PKG_NAME".to_string(), "lib".to_string())]
        );
        provide_dep_info(closure.clone());
        let used = resolve_key_inputs(&args, &on, "x").unwrap();
        assert_eq!(used, Some(closure));
    }

    /// The discovery flight names the unit whether or not predictions are on,
    /// and stays off for a unit predictions cannot describe.
    #[test]
    fn discovery_flight_identity_names_the_unit_with_or_without_predictions() {
        // Identity helpers read cwd and environment, which other tests mutate.
        let _lock = key_test_lock();
        let _workspace = crate::config::tests::set_env_for_test(
            "CARGO_MANIFEST_DIR",
            Some(std::ffi::OsStr::new("/w/a")),
        );
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.db");
        let parse = |args: &[&str]| {
            RustcArgs::parse(&args.iter().map(|a| (*a).to_string()).collect::<Vec<_>>()).unwrap()
        };
        let out_dir = dir.path().join("target/debug/deps").display().to_string();
        let a = parse(&[
            "rustc",
            "--crate-name",
            "a",
            "src/a.rs",
            "--out-dir",
            &out_dir,
        ]);
        let b = parse(&[
            "rustc",
            "--crate-name",
            "b",
            "src/b.rs",
            "--out-dir",
            &out_dir,
        ]);
        let with_macro = parse(&[
            "rustc",
            "--crate-name",
            "a",
            "src/a.rs",
            "--out-dir",
            &out_dir,
            "--extern",
            "my_macro=/t/debug/deps/libmy_macro-3.so",
        ]);

        let off = FileHasher::persistent(&db);
        let a_off =
            discovery_flight_identity(&a, &off).expect("predictions off still names the unit");
        assert_eq!(
            a_off,
            rustc_shared_prediction_identity(&a).unwrap(),
            "the flight is the unit's shared identity"
        );
        assert_ne!(a_off, discovery_flight_identity(&b, &off).unwrap());
        assert!(rustc_shared_prediction_identity(&with_macro).is_none());
        assert_eq!(
            discovery_flight_identity(&with_macro, &off),
            rustc_prediction_identity(&with_macro),
            "a proc-macro dependent still gets a name: the flight is only a lock"
        );

        let on = FileHasher::persistent(&db).with_input_predictions(true);
        assert_eq!(
            discovery_flight_identity(&a, &on),
            prediction_discovery_identity(&a, &on),
            "with predictions on the flight is the record identity"
        );
        assert!(prediction_discovery_identity(&with_macro, &on).is_none());
        assert_eq!(
            discovery_flight_identity(&with_macro, &on),
            rustc_prediction_identity(&with_macro),
            "no record identity, so the unit's name serves as the flight"
        );

        // In a registry package the same unit shares its record under the
        // tree guard, so it flies under the shared identity.
        let _manifest = crate::config::tests::set_env_for_test(
            "CARGO_MANIFEST_DIR",
            Some(std::ffi::OsStr::new("/h/registry/src/index-1/a-1.0.0")),
        );
        let shared = rustc_shared_prediction_identity(&with_macro)
            .expect("a registry proc-macro dependent has a shared identity");
        assert_eq!(
            discovery_flight_identity(&with_macro, &off),
            Some(shared.clone())
        );
        assert_eq!(discovery_flight_identity(&with_macro, &on), Some(shared));
    }

    /// A crate the store has never held is a certain miss under any key, so
    /// discovery defers even where no record can vouch for it: an OUT_DIR
    /// unit, and a build with predictions off.
    #[test]
    fn a_crate_the_store_never_held_defers_discovery() {
        let _lock = key_test_lock();
        let out_dir = std::env::var_os("OUT_DIR");
        // SAFETY: the key-test lock serialises environment edits.
        unsafe { std::env::set_var("OUT_DIR", "/t/debug/build/x-1/out") };
        struct RestoreOutDir(Option<std::ffi::OsString>);
        impl Drop for RestoreOutDir {
            fn drop(&mut self) {
                // SAFETY: still under the key-test lock, dropped first.
                match self.0.take() {
                    Some(value) => unsafe { std::env::set_var("OUT_DIR", value) },
                    None => unsafe { std::env::remove_var("OUT_DIR") },
                }
            }
        }
        let _restore = RestoreOutDir(out_dir);
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.db");
        rusqlite::Connection::open(&db)
            .unwrap()
            .execute_batch(
                "CREATE TABLE entries (cache_key TEXT PRIMARY KEY, crate_name TEXT NOT NULL);",
            )
            .unwrap();
        let args = RustcArgs::parse(
            &[
                "rustc",
                "--crate-name",
                "x",
                "src/lib.rs",
                "--edition",
                "2021",
                "--emit=dep-info,metadata",
                "--out-dir",
                &dir.path().join("target/debug/deps").display().to_string(),
            ]
            .iter()
            .map(|a| (*a).to_string())
            .collect::<Vec<_>>(),
        )
        .unwrap();
        let deferred = |hasher: &FileHasher<'_>| {
            set_defer_discovery(true);
            let outcome = resolve_key_inputs(&args, hasher, "x");
            set_defer_discovery(false);
            outcome.is_err_and(|error| error.downcast_ref::<DeferredDiscovery>().is_some())
        };
        let flights = Some(dir.path().join("cache"));
        for predictions in [false, true] {
            let hasher = FileHasher::persistent(&db)
                .with_input_predictions(predictions)
                .with_prediction_flights(flights.clone());
            assert!(deferred(&hasher), "predictions={predictions}");
        }
        {
            // Scoped: the hasher holds the unit's discovery flight until it
            // is dropped, and the hashers below need to take it.
            let hasher = FileHasher::persistent(&db).with_prediction_flights(flights.clone());
            let not_allowed = resolve_key_inputs(&args, &hasher, "x");
            assert!(
                !not_allowed
                    .is_err_and(|error| error.downcast_ref::<DeferredDiscovery>().is_some()),
                "the wrapper did not allow deferral"
            );
        }
        assert!(
            !deferred(&FileHasher::persistent(&db)),
            "without a flight nobody owns the unit, so nobody compiles first"
        );

        rusqlite::Connection::open(&db)
            .unwrap()
            .execute(
                "INSERT INTO entries (cache_key, crate_name) VALUES ('k', 'x')",
                [],
            )
            .unwrap();
        for predictions in [false, true] {
            let hasher = FileHasher::persistent(&db)
                .with_input_predictions(predictions)
                .with_prediction_flights(flights.clone());
            assert!(
                !deferred(&hasher),
                "an entry for the crate may match: predictions={predictions}"
            );
        }
    }

    /// Every build script is `build_script_build`; Cargo's `-C metadata` hash
    /// is what tells them apart, and the presence probe must read it.
    #[test]
    fn a_unit_the_store_never_held_defers_even_when_its_name_is_taken() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.db");
        rusqlite::Connection::open(&db)
            .unwrap()
            .execute_batch(
                "CREATE TABLE entries (cache_key TEXT PRIMARY KEY, crate_name TEXT NOT NULL,
                                       unit_id TEXT NOT NULL DEFAULT '');
                 INSERT INTO entries VALUES ('k', 'build_script_build', 'unit-a');",
            )
            .unwrap();
        let parse = |unit: &str| {
            RustcArgs::parse(
                &[
                    "rustc",
                    "--crate-name",
                    "build_script_build",
                    "build.rs",
                    "--edition",
                    "2021",
                    "--emit=dep-info,link",
                    "-C",
                    &format!("metadata={unit}"),
                    "--out-dir",
                    &dir.path().join("target/debug/build").display().to_string(),
                ]
                .iter()
                .map(|a| (*a).to_string())
                .collect::<Vec<_>>(),
            )
            .unwrap()
        };
        let deferred = |args: &RustcArgs| {
            let hasher =
                FileHasher::persistent(&db).with_prediction_flights(Some(dir.path().join("cache")));
            set_defer_discovery(true);
            let outcome = resolve_key_inputs(args, &hasher, "build_script_build");
            set_defer_discovery(false);
            outcome.is_err_and(|error| error.downcast_ref::<DeferredDiscovery>().is_some())
        };
        assert!(
            deferred(&parse("unit-b")),
            "another unit of the same name is absent"
        );
        assert!(!deferred(&parse("unit-a")), "this unit was stored");
        rusqlite::Connection::open(&db)
            .unwrap()
            .execute_batch("INSERT INTO entries VALUES ('k2', 'build_script_build', '');")
            .unwrap();
        assert!(
            !deferred(&parse("unit-b")),
            "a row that never learned its unit stands for every unit of the name"
        );
    }

    /// The two gates in front of a record lookup, each refusing for its own
    /// reason so the trace can say which.
    #[test]
    fn a_record_is_only_consulted_for_an_eligible_invocation() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.db");
        let parse = |args: &[&str]| {
            RustcArgs::parse(&args.iter().map(|a| (*a).to_string()).collect::<Vec<_>>()).unwrap()
        };
        let plain = parse(&["rustc", "src/lib.rs", "--edition", "2021"]);
        let with_macro = parse(&[
            "rustc",
            "src/lib.rs",
            "--edition",
            "2021",
            "--extern",
            "my_macro=/t/debug/deps/libmy_macro-3.so",
        ]);

        let off = FileHasher::persistent(&db);
        assert_eq!(
            predicted_key_inputs(&plain, &off),
            Err(Rejection::Disabled),
            "predictions off must not touch the table"
        );

        let on = FileHasher::persistent(&db).with_input_predictions(true);
        assert_eq!(
            predicted_key_inputs(&with_macro, &on),
            Err(Rejection::NotEligible),
            "a proc-macro dependency outside a registry package is refused before any lookup"
        );
        if get_rustc_version(Path::new("rustc")).is_ok() {
            assert_eq!(
                predicted_key_inputs(&plain, &on),
                Err(Rejection::NoRecord),
                "an eligible invocation with nothing recorded falls back"
            );
        }
    }

    /// A proc macro can scan a directory and emit `include_str!` per entry, so
    /// a file can join the closure with nothing already in it changing. The
    /// pre-pass sees it; a prediction would not. Cargo hands rustc a proc
    /// macro as a dynamic library, and that is the test.
    #[test]
    fn predictions_do_not_apply_to_units_with_a_dynamic_library_dependency() {
        let dep = |path: &str| crate::args::ExternDep {
            name: "dep".to_string(),
            path: Some(PathBuf::from(path)),
        };
        let plain = vec![
            dep("/t/debug/deps/libserde-1.rlib"),
            dep("/t/debug/deps/libcore-2.rmeta"),
        ];
        assert!(
            prediction_applies(&plain),
            "rlib and rmeta dependencies cannot scan the filesystem"
        );
        assert!(
            prediction_applies(&[]),
            "a unit with no dependencies is eligible"
        );

        for macro_lib in [
            "/t/debug/deps/libmy_macro-3.so",
            "/t/debug/deps/libmy_macro-3.dylib",
            "/t/debug/deps/my_macro-3.dll",
        ] {
            let mut with_macro = plain.clone();
            with_macro.push(dep(macro_lib));
            assert!(
                !prediction_applies(&with_macro),
                "{macro_lib} may generate includes the record cannot know about"
            );
        }

        // A dependency cargo passed without a path tells us nothing either way
        // and must not be read as a proc macro.
        assert!(prediction_applies(&[crate::args::ExternDep {
            name: "std".to_string(),
            path: None,
        }]));
    }

    /// `src/foo.rs` and `src/foo/mod.rs` both answer `mod foo;`, and rustc
    /// refuses to choose. Only the two spellings of a module have a sibling;
    /// a crate root is named by argv, so it has none.
    #[test]
    fn mod_sibling_is_the_other_spelling_of_the_same_module() {
        assert_eq!(
            mod_sibling_candidate(Path::new("src/foo.rs")),
            Some(PathBuf::from("src/foo/mod.rs"))
        );
        assert_eq!(
            mod_sibling_candidate(Path::new("src/foo/mod.rs")),
            Some(PathBuf::from("src/foo.rs"))
        );
        for root in ["src/lib.rs", "src/main.rs"] {
            assert_eq!(
                mod_sibling_candidate(Path::new(root)),
                None,
                "{root} is named by argv, not by a mod item"
            );
        }
        assert_eq!(
            mod_sibling_candidate(Path::new("assets/data.json")),
            None,
            "an included asset is not a module"
        );
    }

    /// The verification knob is the measurement of the soundness argument on
    /// real code, so its parsing has to be exact about what turns it on.
    #[test]
    fn verify_predictions_parses_its_three_modes() {
        for on in ["always", "ALWAYS", "1", "true", "True"] {
            assert_eq!(
                parse_verify_predictions(Some(on)),
                VerifyPredictions::Always,
                "{on} must verify every prediction"
            );
        }
        for sampled in ["sampled", "SAMPLED"] {
            assert_eq!(
                parse_verify_predictions(Some(sampled)),
                VerifyPredictions::Sampled
            );
        }
        for off in [Some("off"), Some("0"), Some("false"), Some(""), None] {
            assert_eq!(
                parse_verify_predictions(off),
                VerifyPredictions::Off,
                "{off:?} must not cost a pre-pass"
            );
        }

        assert!(!should_verify_this_prediction(
            VerifyPredictions::Off,
            "unit"
        ));
        assert!(should_verify_this_prediction(
            VerifyPredictions::Always,
            "unit"
        ));

        // Sampling is decided by the identity, because the wrapper is a fresh
        // process per compile. A rolling counter would start at zero every
        // time and select every unit, which is how `sampled` silently became
        // `always` and made a nightly measure the cost of both paths.
        let identities: Vec<String> = (0..VERIFY_PREDICTION_RATE * 20)
            .map(|i| format!("identity-{i}"))
            .collect();
        let chosen = identities
            .iter()
            .filter(|id| should_verify_this_prediction(VerifyPredictions::Sampled, id))
            .count();
        assert!(
            (5..=40).contains(&chosen),
            "about 1 in {VERIFY_PREDICTION_RATE} of {} should be checked, got {chosen}",
            identities.len()
        );
        assert!(
            identities
                .iter()
                .any(|id| !should_verify_this_prediction(VerifyPredictions::Sampled, id)),
            "sampling that selects everything is not sampling"
        );

        // Stable: the same unit is checked on every build, so a disagreement
        // is reproducible instead of a one-off nobody can chase.
        let first = identities
            .iter()
            .find(|id| should_verify_this_prediction(VerifyPredictions::Sampled, id))
            .expect("some identity must be selected");
        for _ in 0..5 {
            assert!(should_verify_this_prediction(
                VerifyPredictions::Sampled,
                first
            ));
        }
    }

    /// Where a unit's package lives, which decides how Cargo runs rustc.
    #[derive(Clone, Copy)]
    enum SamplePlace {
        /// A workspace member: run from the workspace root, relative source.
        Member,
        /// A path dependency outside the workspace root: run from its package
        /// root, absolute source.
        OutsideWorkspace,
        /// A registry package under the Cargo home, run the same way.
        Registry,
    }

    /// One Cargo unit, placed in a checkout by [`SampleUnit::in_checkout`].
    struct SampleUnit {
        package: String,
        version: &'static str,
        place: SamplePlace,
        crate_name: &'static str,
        source: &'static str,
        crate_type: &'static str,
        target: Option<&'static str>,
    }

    /// What the wrapper sees for one unit: argv, the working directory as
    /// `getcwd` reports it, and the environment Cargo sets.
    struct SampleCall {
        args: RustcArgs,
        cwd: PathBuf,
        env: HashMap<&'static str, std::ffi::OsString>,
    }

    impl SampleCall {
        fn identity(&self) -> String {
            prediction_sample_identity(&self.args, Some(&self.cwd), |var| {
                self.env.get(var).cloned()
            })
        }

        fn verified(&self) -> bool {
            verifies_prediction(
                VerifyPredictions::Sampled,
                &self.args,
                Some(&self.cwd),
                |var| self.env.get(var).cloned(),
            )
        }
    }

    /// `path` as macOS `getcwd` spells it: `/tmp` is a symlink there.
    fn physical(path: &str) -> String {
        match path.strip_prefix("/tmp/") {
            Some(rest) => format!("/private/tmp/{rest}"),
            None => path.to_string(),
        }
    }

    impl SampleUnit {
        /// This unit in the checkout at `root`, whose workspace is `rust/`,
        /// with its registry under `cargo_home`.
        ///
        /// Cargo hashes a registry package id the same way everywhere, but a
        /// path dependency outside the workspace root by its absolute path,
        /// and every member depending on one inherits that in its
        /// `-C metadata`. The metadata here does the same. A target spec is
        /// passed as an absolute path, as Cargo does.
        fn in_checkout(&self, root: &str, cargo_home: &str) -> SampleCall {
            let workspace = format!("{root}/rust");
            let package_dir = match self.place {
                SamplePlace::Member => format!("{workspace}/{}", self.package),
                SamplePlace::OutsideWorkspace => format!("{root}/{}", self.package),
                SamplePlace::Registry => format!(
                    "{cargo_home}/registry/src/index.crates.io-0000/{}-{}",
                    self.package, self.version
                ),
            };
            let (cwd, source, hashed) = match self.place {
                SamplePlace::Member => (
                    workspace.clone(),
                    format!("{}/{}", self.package, self.source),
                    package_dir.clone(),
                ),
                SamplePlace::OutsideWorkspace => (
                    package_dir.clone(),
                    format!("{package_dir}/{}", self.source),
                    package_dir.clone(),
                ),
                SamplePlace::Registry => (
                    package_dir.clone(),
                    format!("{package_dir}/{}", self.source),
                    format!("{}-{}", self.package, self.version),
                ),
            };
            let metadata = blake3::hash(hashed.as_bytes()).to_hex()[..16].to_string();
            let deps = format!("{workspace}/target/debug/deps");
            let mut argv = vec![
                "rustc".to_string(),
                "--crate-name".to_string(),
                self.crate_name.to_string(),
                "--edition=2021".to_string(),
                source,
                "--error-format=json".to_string(),
                "--crate-type".to_string(),
                self.crate_type.to_string(),
                "--emit=dep-info,metadata,link".to_string(),
                "--cfg".to_string(),
                "feature=\"default\"".to_string(),
                "-C".to_string(),
                format!("metadata={metadata}"),
                "-C".to_string(),
                format!("extra-filename=-{metadata}"),
                "--out-dir".to_string(),
                deps.clone(),
                "-L".to_string(),
                format!("dependency={deps}"),
                "--extern".to_string(),
                format!("dep={deps}/libdep-0123456789abcdef.rmeta"),
            ];
            if let Some(target) = self.target {
                argv.push("--target".to_string());
                argv.push(if target.ends_with(".json") {
                    format!("{workspace}/{target}")
                } else {
                    target.to_string()
                });
            }
            let env = HashMap::from([
                ("CARGO_MANIFEST_DIR", package_dir.into()),
                ("CARGO_PKG_NAME", self.package.clone().into()),
                ("CARGO_PKG_VERSION", self.version.into()),
            ]);
            SampleCall {
                args: RustcArgs::parse(&argv).unwrap(),
                cwd: PathBuf::from(physical(&cwd)),
                env,
            }
        }
    }

    /// The units `sampled` picks in one checkout.
    fn sampled_units(units: &[SampleUnit], root: &str, cargo_home: &str) -> Vec<usize> {
        units
            .iter()
            .enumerate()
            .filter(|(_, unit)| unit.in_checkout(root, cargo_home).verified())
            .map(|(i, _)| i)
            .collect()
    }

    /// Two checkouts of one project, as a perf gate builds base and head. The
    /// base Cargo home sits behind a symlink, as anything under `/tmp` does on
    /// macOS.
    const SAMPLE_BASE: (&str, &str) = ("/scratch/base/hk", "/tmp/cargo");
    const SAMPLE_HEAD: (&str, &str) = ("/scratch/head/hk", "/home/ci/.cargo");

    /// A perf gate builds base and head in separate checkouts. Sampling by
    /// the prediction identity checked different units in each, because that
    /// identity folds the working directory and absolute paths: hk counted 7
    /// checks against 2, and eza showed a significant speedup that was only 3
    /// checks against none. Both checkouts must check the same units.
    #[test]
    fn prediction_sample_is_the_same_in_every_checkout() {
        let units: Vec<SampleUnit> = (0..VERIFY_PREDICTION_RATE * 8)
            .map(|i| SampleUnit {
                package: format!("pkg{i}"),
                version: "1.0.0",
                place: [
                    SamplePlace::Member,
                    SamplePlace::OutsideWorkspace,
                    SamplePlace::Registry,
                ][i % 3],
                crate_name: ["alpha", "beta", "build_script_build", "delta"][i % 4],
                source: ["src/lib.rs", "src/main.rs", "build.rs", "src/lib.rs"][i % 4],
                crate_type: ["lib", "bin", "bin", "lib"][i % 4],
                target: [
                    None,
                    Some("aarch64-apple-darwin"),
                    Some("specs/os.json"),
                    None,
                    None,
                ][i % 5],
            })
            .collect();
        for (i, unit) in units.iter().enumerate() {
            assert_eq!(
                unit.in_checkout(SAMPLE_BASE.0, SAMPLE_BASE.1).identity(),
                unit.in_checkout(SAMPLE_HEAD.0, SAMPLE_HEAD.1).identity(),
                "unit {i} must be sampled the same way in both checkouts"
            );
        }
        let in_base = sampled_units(&units, SAMPLE_BASE.0, SAMPLE_BASE.1);
        assert!(!in_base.is_empty(), "some unit must be checked");
        assert_eq!(in_base, sampled_units(&units, SAMPLE_HEAD.0, SAMPLE_HEAD.1));
    }

    /// A path dependency outside the workspace root, such as `rust/`
    /// depending on `../proto`, gets a `-C metadata` that names the checkout,
    /// and so does every member that depends on it. The sample must not read
    /// it.
    #[test]
    fn prediction_sample_ignores_metadata_that_names_the_checkout() {
        for place in [SamplePlace::OutsideWorkspace, SamplePlace::Member] {
            let unit = SampleUnit {
                package: "proto".to_string(),
                version: "0.1.0",
                place,
                crate_name: "proto",
                source: "src/lib.rs",
                crate_type: "lib",
                target: None,
            };
            let base = unit.in_checkout(SAMPLE_BASE.0, SAMPLE_BASE.1);
            let head = unit.in_checkout(SAMPLE_HEAD.0, SAMPLE_HEAD.1);
            assert_ne!(
                base.args.get_codegen_opt("metadata"),
                head.args.get_codegen_opt("metadata")
            );
            assert_eq!(base.identity(), head.identity());
        }
    }

    /// Cargo names a registry package's root and its source through the Cargo
    /// home as given, but `getcwd` resolves symlinks. Stripping only the
    /// working directory left the source absolute behind a symlinked Cargo
    /// home, and every registry unit sampled differently.
    #[test]
    fn prediction_sample_strips_the_package_root_as_cargo_spells_it() {
        let unit = SampleUnit {
            package: "serde".to_string(),
            version: "1.0.0",
            place: SamplePlace::Registry,
            crate_name: "serde",
            source: "src/lib.rs",
            crate_type: "lib",
            target: None,
        };
        let base = unit.in_checkout(SAMPLE_BASE.0, SAMPLE_BASE.1);
        assert!(base.cwd.starts_with("/private/tmp"));
        assert!(base.args.source_file.as_ref().unwrap().starts_with("/tmp"));
        assert_eq!(
            base.identity(),
            unit.in_checkout(SAMPLE_HEAD.0, SAMPLE_HEAD.1).identity()
        );
    }

    /// The same in every checkout must not mean one decision for a whole
    /// crate name: every registry build script is `build_script_build` from
    /// `build.rs` in its package root, and only its package tells them apart.
    #[test]
    fn prediction_sample_spreads_across_units_that_share_a_name() {
        let build_scripts: Vec<SampleUnit> = (0..VERIFY_PREDICTION_RATE * 20)
            .map(|i| SampleUnit {
                package: format!("pkg{i}"),
                version: "1.0.0",
                place: SamplePlace::Registry,
                crate_name: "build_script_build",
                source: "build.rs",
                crate_type: "bin",
                target: None,
            })
            .collect();
        let chosen = sampled_units(&build_scripts, SAMPLE_BASE.0, SAMPLE_BASE.1);
        assert!(
            (5..=40).contains(&chosen.len()),
            "about 1 in {VERIFY_PREDICTION_RATE} of {} should be checked, got {}",
            build_scripts.len(),
            chosen.len()
        );
        assert_eq!(
            chosen,
            sampled_units(&build_scripts, SAMPLE_HEAD.0, SAMPLE_HEAD.1),
            "another checkout checks the same build scripts"
        );

        // Each part of the unit's name separates it from its neighbours.
        let sample = |argv: &str, package: [&str; 2]| {
            let argv: Vec<String> = argv.split(' ').map(str::to_string).collect();
            let env = |var: &str| match var {
                "CARGO_PKG_NAME" => Some(package[0].into()),
                "CARGO_PKG_VERSION" => Some(package[1].into()),
                _ => None,
            };
            prediction_sample_identity(
                &RustcArgs::parse(&argv).unwrap(),
                Some(Path::new("/w")),
                env,
            )
        };
        let unit = "rustc --crate-name a a.rs --crate-type lib";
        let package = ["a", "1.0.0"];
        let base = sample(unit, package);
        for (part, other) in [
            ("package name", sample(unit, ["b", "1.0.0"])),
            ("package version", sample(unit, ["a", "2.0.0"])),
            (
                "crate type",
                sample(&format!("{unit} --crate-type rlib"), package),
            ),
            ("test harness", sample(&format!("{unit} --test"), package)),
            (
                "target",
                sample(&format!("{unit} --target wasm32-wasip1"), package),
            ),
            (
                "cfg",
                sample(&format!("{unit} --cfg feature=\"std\""), package),
            ),
            (
                "crate name",
                sample("rustc --crate-name b a.rs --crate-type lib", package),
            ),
            (
                "source",
                sample("rustc --crate-name a b.rs --crate-type lib", package),
            ),
        ] {
            assert_ne!(other, base, "{part} must separate units");
        }
        assert_ne!(
            sample(&format!("{unit} --target /w/a.json"), package),
            sample(&format!("{unit} --target /w/b.json"), package),
            "target specs must separate units"
        );
    }

    /// Cargo passes a target spec as an absolute path; rustc names the target
    /// by the file's stem.
    #[test]
    fn target_name_is_a_spec_file_stem() {
        assert_eq!(target_name("wasm32-wasip1"), "wasm32-wasip1");
        assert_eq!(target_name("/scratch/base/hk/specs/os.json"), "os");
        assert_eq!(target_name("os.json"), "os");
    }

    /// The source a unit is sampled by is the one every checkout spells the
    /// same way.
    #[test]
    fn checkout_relative_drops_the_first_root_the_path_is_under() {
        let package = Path::new("/tmp/cargo/registry/src/index/serde-1.0.0");
        let resolved = Path::new("/private/tmp/cargo/registry/src/index/serde-1.0.0");
        let source = package.join("src/lib.rs");
        let relative = Path::new("src/lib.rs");
        assert_eq!(
            checkout_relative(&source, &[Some(package), Some(resolved)]),
            relative
        );
        assert_eq!(
            checkout_relative(&source, &[Some(resolved), Some(package)]),
            relative
        );
        assert_eq!(checkout_relative(&source, &[None, Some(resolved)]), source);
        assert_eq!(checkout_relative(&source, &[]), source);
        assert_eq!(checkout_relative(relative, &[Some(package)]), relative);
        assert_eq!(
            checkout_relative(Path::new("/elsewhere/src/lib.rs"), &[Some(package)]),
            Path::new("/elsewhere/src/lib.rs")
        );
    }

    /// Two closures naming the same files agree however they are ordered: the
    /// key sorts both before folding, so order cannot change a key and must
    /// not be reported as a disagreement.
    #[test]
    fn closures_agree_on_content_not_order() {
        let dep = |sources: &[&str], env: &[(&str, &str)]| DepInfo {
            source_files: sources.iter().map(PathBuf::from).collect(),
            env_deps: env
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        };
        let a = dep(&["src/lib.rs", "src/helper.rs"], &[("OUT_DIR", "/t")]);
        let reordered = dep(&["src/helper.rs", "src/lib.rs"], &[("OUT_DIR", "/t")]);
        assert!(closures_agree(&a, &reordered));

        assert!(!closures_agree(
            &a,
            &dep(
                &["src/lib.rs", "src/helper.rs", "src/extra.rs"],
                &[("OUT_DIR", "/t")]
            )
        ));
        assert!(!closures_agree(
            &a,
            &dep(&["src/lib.rs"], &[("OUT_DIR", "/t")])
        ));
        assert!(!closures_agree(
            &a,
            &dep(&["src/lib.rs", "src/helper.rs"], &[])
        ));
        assert!(!closures_agree(
            &a,
            &dep(&["src/lib.rs", "src/helper.rs"], &[("OUT_DIR", "/other")])
        ));
    }

    /// The rules, one refusal at a time. Each is a claim that the recorded
    /// closure no longer describes what rustc would read.
    #[test]
    fn a_prediction_is_validated_against_the_tree_as_it_is_now() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        let lib = src.join("lib.rs");
        let helper = src.join("helper.rs");
        std::fs::write(&lib, "mod helper;\n").unwrap();
        std::fs::write(&helper, "pub fn n() {}\n").unwrap();

        let record = InputPrediction {
            schema: PREDICTION_SCHEMA,
            sources: vec![lib.clone(), helper.clone()],
            env_deps: vec![("OUT_DIR".to_string(), "/t/build/out".to_string())],
            tree: None,
        };
        let stat = |path: &Path| std::fs::metadata(path).ok();
        let exists = |path: &Path| path.exists();
        let env = |var: &str| (var == "OUT_DIR").then(|| "/t/build/out".to_string());

        let accepted = validate_prediction(&record, stat, exists, env)
            .expect("an unchanged tree must reuse the recorded closure");
        assert_eq!(accepted.source_files, record.sources);
        assert_eq!(accepted.env_deps, record.env_deps);

        // The value the key normalises to a sentinel still has to match raw:
        // a moved OUT_DIR is how an included generated file changes identity
        // without any recorded file changing.
        assert_eq!(
            validate_prediction(&record, stat, exists, |var| (var == "OUT_DIR")
                .then(|| "/t/build/other".to_string())),
            Err(Rejection::EnvChanged)
        );
        assert_eq!(
            validate_prediction(&record, stat, exists, |_| None),
            Err(Rejection::EnvChanged),
            "an unset variable is not the value that was recorded"
        );

        // A file that is gone: the pre-pass would fail too, and the build
        // passes through to rustc's own error.
        std::fs::remove_file(&helper).unwrap();
        assert_eq!(
            validate_prediction(&record, stat, exists, env),
            Err(Rejection::Missing)
        );

        // A path that is no longer a regular file.
        std::fs::create_dir(&helper).unwrap();
        assert_eq!(
            validate_prediction(&record, stat, exists, env),
            Err(Rejection::NotRegular)
        );
        std::fs::remove_dir(&helper).unwrap();
        std::fs::write(&helper, "pub fn n() {}\n").unwrap();

        // Both spellings of `mod helper;` present: rustc errors (E0761), so
        // replaying a recorded success would restore an artifact for a build
        // that should fail.
        std::fs::create_dir(src.join("helper")).unwrap();
        std::fs::write(src.join("helper/mod.rs"), "pub fn n() {}\n").unwrap();
        assert_eq!(
            validate_prediction(&record, stat, exists, env),
            Err(Rejection::Sibling)
        );
    }

    /// The dep-info parser emits `VAR=` both when the variable is unset and
    /// when it is empty, so validation must accept both for an empty record:
    /// otherwise any crate reading an unset `option_env!` would reject every
    /// record and pay a pre-pass on every warm build. A recorded value still
    /// rejects an unset variable.
    #[test]
    fn an_unset_variable_matches_an_empty_record() {
        let dir = tempfile::tempdir().unwrap();
        let lib = dir.path().join("lib.rs");
        std::fs::write(&lib, "pub fn f() {}\n").unwrap();
        let stat = |path: &Path| std::fs::metadata(path).ok();
        let exists = |path: &Path| path.exists();
        let empty = InputPrediction {
            schema: PREDICTION_SCHEMA,
            sources: vec![lib.clone()],
            env_deps: vec![("KACHE_PROBE_UNSET".to_string(), String::new())],
            tree: None,
        };
        assert!(
            validate_prediction(&empty, stat, exists, |_| None).is_ok(),
            "unset matches empty, exactly as the parser would fold it"
        );
        let valued = InputPrediction {
            schema: PREDICTION_SCHEMA,
            sources: vec![lib],
            env_deps: vec![("KACHE_PROBE_UNSET".to_string(), "1".to_string())],
            tree: None,
        };
        assert_eq!(
            validate_prediction(&valued, stat, exists, |_| None),
            Err(Rejection::EnvChanged)
        );
    }

    /// A prediction is only reusable by an invocation that would discover the
    /// same files. Every part of the identity is one such "would discover the
    /// same files" claim, so perturbing any of them has to produce a different
    /// row rather than a wrong answer.
    #[test]
    fn prediction_identity_separates_every_part_it_folds() {
        let base_args: Vec<String> = ["--edition", "2021", "--crate-name", "demo"]
            .iter()
            .map(|a| (*a).to_string())
            .collect();
        let base_env = |extra: &[(&str, &str)]| -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
            let mut env: Vec<(std::ffi::OsString, std::ffi::OsString)> = vec![(
                std::ffi::OsString::from("CARGO_CFG_TARGET_OS"),
                std::ffi::OsString::from("linux"),
            )];
            env.extend(
                extra
                    .iter()
                    .map(|(k, v)| (std::ffi::OsString::from(k), std::ffi::OsString::from(v))),
            );
            env
        };
        let base_parts = PredictionIdentityParts {
            rustc_version: "rustc 1.95.0",
            inner_rustc: None,
            current_dir: Some(Path::new("/w/one")),
            source_file: Path::new("src/lib.rs"),
            closure_args: &base_args,
            skip_path_remap: false,
        };
        let base = prediction_identity_in_env(&base_parts, base_env(&[]));

        assert_eq!(
            prediction_identity_in_env(&base_parts, base_env(&[])),
            base,
            "the same invocation twice is the same identity"
        );

        let other_args: Vec<String> = ["--edition", "2024", "--crate-name", "demo"]
            .iter()
            .map(|a| (*a).to_string())
            .collect();
        let perturbed: Vec<(&str, PredictionIdentityParts<'_>)> = vec![
            (
                "a different compiler build",
                PredictionIdentityParts {
                    rustc_version: "rustc 1.96.0",
                    ..base_parts
                },
            ),
            (
                "a wrapped inner compiler",
                PredictionIdentityParts {
                    inner_rustc: Some(Path::new("/usr/bin/rustc")),
                    ..base_parts
                },
            ),
            (
                "another working directory, which relative args resolve against",
                PredictionIdentityParts {
                    current_dir: Some(Path::new("/w/two")),
                    ..base_parts
                },
            ),
            (
                "another crate root",
                PredictionIdentityParts {
                    source_file: Path::new("src/main.rs"),
                    ..base_parts
                },
            ),
            (
                "an edition change, which changes what resolves",
                PredictionIdentityParts {
                    closure_args: &other_args,
                    ..base_parts
                },
            ),
            (
                "path remapping turned off",
                PredictionIdentityParts {
                    skip_path_remap: true,
                    ..base_parts
                },
            ),
        ];
        for (what, parts) in perturbed {
            assert_ne!(
                prediction_identity_in_env(&parts, base_env(&[])),
                base,
                "{what} must not reuse another invocation's record"
            );
        }

        // The environment the key already folds, one variable at a time.
        for var in PREDICTION_ENV {
            assert_ne!(
                prediction_identity_in_env(&base_parts, base_env(&[(var, "value")])),
                base,
                "{var} must separate identities"
            );
        }
        // Set-but-empty is not unset: `env!` can tell them apart.
        assert_ne!(
            prediction_identity_in_env(&base_parts, base_env(&[("RUSTFLAGS", "")])),
            base,
            "an empty RUSTFLAGS is not an absent one"
        );
        // A cfg change can add or remove whole modules.
        let mut other_cfg = base_env(&[]);
        other_cfg[0].1 = std::ffi::OsString::from("windows");
        assert_ne!(
            prediction_identity_in_env(&base_parts, other_cfg),
            base,
            "CARGO_CFG_* changes which modules compile"
        );
        // Unrelated environment must NOT separate identities: folding it
        // wholesale is what kept the cc preprocessor memo from ever hitting
        // (kunobi-ninja/kache#927).
        assert_eq!(
            prediction_identity_in_env(&base_parts, base_env(&[("PWD", "/somewhere/else")])),
            base,
            "variables that do not change what rustc reads must not be folded"
        );
    }

    /// An invocation with no crate root discovers no closure, so it has no
    /// identity — and two invocations that read different files must not share
    /// one.
    ///
    /// The comparisons run against one environment snapshot rather than the
    /// live process environment: other tests in this binary set and unset
    /// variables concurrently, which would otherwise decide the result.
    #[test]
    fn rustc_prediction_identity_needs_a_crate_root_and_separates_units() {
        // The identity also reads cwd, which other tests change under this lock.
        let _lock = crate::test_support::process_state_test_lock();
        let parse = |args: &[&str]| {
            RustcArgs::parse(&args.iter().map(|a| (*a).to_string()).collect::<Vec<_>>()).unwrap()
        };
        assert_eq!(
            rustc_prediction_identity(&parse(&["rustc", "--version"])),
            None,
            "a query invocation compiles nothing and predicts nothing"
        );

        if get_rustc_version(Path::new("rustc")).is_err() {
            return; // no compiler on this host; nothing to identify against
        }
        let env: Vec<(std::ffi::OsString, std::ffi::OsString)> = vec![(
            std::ffi::OsString::from("CARGO_CFG_TARGET_OS"),
            std::ffi::OsString::from("linux"),
        )];
        let identity = |args: &[&str]| {
            rustc_prediction_identity_in_env(&parse(args), env.clone())
                .expect("a crate root gives an identity")
        };

        let lib = identity(&["rustc", "src/lib.rs", "--edition", "2021"]);
        assert!(!lib.is_empty());
        assert_ne!(
            lib,
            identity(&["rustc", "src/main.rs", "--edition", "2021"]),
            "different crate roots read different files"
        );
        assert_ne!(
            lib,
            identity(&["rustc", "src/lib.rs", "--edition", "2024"]),
            "a different edition resolves differently"
        );

        // Output naming is not identity: these two read the same files.
        assert_eq!(
            lib,
            identity(&[
                "rustc",
                "src/lib.rs",
                "--edition",
                "2021",
                "-C",
                "extra-filename=-abc123",
            ]),
            "two units of one crate share a record"
        );
    }

    /// The identity is the argv that shapes the closure, not the argv that
    /// names the output. Two units of one crate differing only in where their
    /// artifacts land read exactly the same files.
    #[test]
    fn prediction_identity_ignores_output_naming_flags() {
        let closure = |extra: &[&str]| -> Vec<String> {
            let mut args: Vec<String> = ["--edition", "2021"]
                .iter()
                .map(|a| (*a).to_string())
                .collect();
            args.extend(extra.iter().map(|a| (*a).to_string()));
            closure_shaping_args(Path::new("src/lib.rs"), &args)
        };
        let plain = closure(&[]);
        for naming in [
            vec!["-C", "extra-filename=-abc123"],
            vec!["--out-dir", "/target/debug/deps"],
            vec!["--emit=metadata,link"],
        ] {
            assert_eq!(
                closure(&naming),
                plain,
                "{naming:?} decides where output goes, not what rustc reads"
            );
        }
        assert_ne!(
            closure(&["--cfg", "feature=\"extra\""]),
            plain,
            "a cfg can add a module, so it stays in the identity"
        );
    }

    /// A build script's `OUT_DIR` identifies the unit, not the build
    /// directory: the same unit in two target directories shares a record,
    /// two feature sets keep their own, and a value outside the target
    /// directory (or one that escapes it) is left exactly as it was.
    #[test]
    fn shared_predictions_place_out_dir_inside_its_target() {
        let vars = |target: &str, out_dir: &str| {
            let pairs = vec![
                (
                    std::ffi::OsString::from("OUT_DIR"),
                    std::ffi::OsString::from(out_dir),
                ),
                (
                    std::ffi::OsString::from("CARGO_MANIFEST_DIR"),
                    std::ffi::OsString::from("/registry/libc-0.2"),
                ),
            ];
            shared_prediction_vars(pairs.into_iter(), Path::new(target))
        };
        let value = |v: &[(std::ffi::OsString, std::ffi::OsString)], name: &str| {
            v.iter()
                .find(|(n, _)| n == name)
                .map(|(_, value)| value.to_string_lossy().into_owned())
                .unwrap()
        };

        let a = vars(
            "/a/target",
            "/a/target/debug/build/libc-abc123def4567890/out",
        );
        let b = vars(
            "/b/target",
            "/b/target/debug/build/libc-abc123def4567890/out",
        );
        assert_eq!(value(&a, "OUT_DIR"), value(&b, "OUT_DIR"));
        assert_eq!(
            value(&a, "OUT_DIR"),
            "kache-target-relative:debug/build/libc-abc123def4567890/out"
        );
        assert_eq!(
            value(&a, "CARGO_MANIFEST_DIR"),
            "/registry/libc-0.2",
            "only OUT_DIR is rewritten"
        );
        let other_features = vars(
            "/a/target",
            "/a/target/debug/build/libc-0123456789abcdef/out",
        );
        assert_ne!(
            value(&a, "OUT_DIR"),
            value(&other_features, "OUT_DIR"),
            "Cargo's metadata hash still separates two feature sets"
        );
        for outside in [
            "/elsewhere/build/libc-abc123def4567890/out",
            "/a/target/../sneaky/out",
        ] {
            assert_eq!(
                value(&vars("/a/target", outside), "OUT_DIR"),
                outside,
                "a value the target directory does not contain is left alone"
            );
        }
        assert!(
            target_relative_env_value(
                std::ffi::OsStr::new("/a/target/debug/build/x/out"),
                Path::new("/a/target")
            )
            .is_some()
        );
        assert!(
            target_relative_env_value(std::ffi::OsStr::new("/a/target"), Path::new("/a/target"))
                .is_some(),
            "the target directory itself is relative to itself"
        );
    }

    #[test]
    fn shared_predictions_only_virtualize_dependency_paths() {
        let map = |target: &str, args: &[&str]| {
            shared_prediction_args(
                &args.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                Path::new(target),
            )
        };
        for (left, right) in [
            (
                vec!["--extern", "dep=/a/target/debug/libdep.rlib"],
                vec!["--extern", "dep=/b/target/debug/libdep.rlib"],
            ),
            (
                vec!["--extern=dep=/a/target/debug/libdep.rmeta"],
                vec!["--extern=dep=/b/target/debug/libdep.rmeta"],
            ),
            (
                vec!["-L", "dependency=/a/target/debug/deps"],
                vec!["-L", "dependency=/b/target/debug/deps"],
            ),
            (
                vec!["-Lnative=/a/target/debug/build/out"],
                vec!["-Lnative=/b/target/debug/build/out"],
            ),
            (
                vec!["-L/a/target/debug/deps"],
                vec!["-L/b/target/debug/deps"],
            ),
        ] {
            assert_eq!(map("/a/target", &left), map("/b/target", &right));
        }
        for (left, right) in [
            (
                vec!["--cfg", "path=\"/a/target/value\""],
                vec!["--cfg", "path=\"/b/target/value\""],
            ),
            (
                vec!["/a/target/generated.rs"],
                vec!["/b/target/generated.rs"],
            ),
            (
                vec!["--extern", "a=/a/target/lib.rlib"],
                vec!["--extern", "b=/b/target/lib.rlib"],
            ),
            (
                vec!["--extern", "a=/a/target/lib.rlib"],
                vec!["--extern", "a=/b/target/other.rlib"],
            ),
            (
                vec!["-Lnative=/a/target/lib"],
                vec!["-Ldependency=/b/target/lib"],
            ),
            (
                vec!["--extern", "a=/a/target-extra/lib.rlib"],
                vec!["--extern", "a=/b/target-extra/lib.rlib"],
            ),
            (
                vec!["--extern", "a=/a/target/../lib.rlib"],
                vec!["--extern", "a=/b/target/../lib.rlib"],
            ),
        ] {
            assert_ne!(map("/a/target", &left), map("/b/target", &right));
        }
        let encoded = map("/a/target", &["-L/a/target/lib"]);
        assert_ne!(encoded, map("/a/target", &[&encoded[0]]));
    }

    #[test]
    fn shared_predictions_reject_target_sources() {
        let _lock = crate::test_support::process_state_test_lock();
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("target");
        let args = RustcArgs::parse(&[
            "rustc".into(),
            "src/lib.rs".into(),
            "--out-dir".into(),
            target.join("debug/deps").to_string_lossy().into_owned(),
        ])
        .unwrap();
        let mut dep = DepInfo {
            source_files: vec![root.path().join("src/lib.rs")],
            env_deps: vec![],
        };
        // A workspace package, whatever package runs this test.
        let can_record = |args: &RustcArgs, dep: &DepInfo| {
            shared_prediction_can_record_in(args, dep, Some(root.path()))
        };
        assert!(can_record(&args, &dep));
        dep.source_files
            .push(target.join("debug/build/pkg/out/generated.rs"));
        assert!(!can_record(&args, &dep));
        let no_target = RustcArgs::parse(&["rustc".into(), "src/lib.rs".into()]).unwrap();
        assert!(!can_record(&no_target, &dep));
    }

    const REGISTRY_PACKAGE_DIR: &str = "/h/registry/src/index-1/kt-1.0.0";

    fn os_vars(pairs: &[(&str, &str)]) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
        pairs
            .iter()
            .map(|(name, value)| (name.into(), value.into()))
            .collect()
    }

    /// An absolute target directory for checkout `name` on any platform.
    fn checkout_target(name: &str) -> String {
        if cfg!(windows) {
            format!("C:/{name}/target")
        } else {
            format!("/{name}/target")
        }
    }

    fn registry_unit_args(checkout: &str, externs: &[&str]) -> RustcArgs {
        let target = checkout_target(checkout);
        let mut argv = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "kt".to_string(),
            format!("{REGISTRY_PACKAGE_DIR}/src/lib.rs"),
            "--out-dir".to_string(),
            format!("{target}/debug/deps"),
        ];
        for name in externs {
            argv.push("--extern".to_string());
            argv.push(format!("{name}={target}/debug/deps/lib{name}-1.so"));
        }
        RustcArgs::parse(&argv).unwrap()
    }

    fn unit_vars(
        checkout: &str,
        manifest_dir: &str,
    ) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
        let target = checkout_target(checkout);
        os_vars(&[
            ("CARGO_MANIFEST_DIR", manifest_dir),
            ("OUT_DIR", &format!("{target}/debug/build/kt-1/out")),
        ])
    }

    #[test]
    fn shared_prediction_eligibility_takes_either_rule() {
        let dep = |path: &str| crate::args::ExternDep {
            name: "dep".to_string(),
            path: Some(PathBuf::from(path)),
        };
        let plain = [dep("/t/debug/deps/libserde-1.rlib")];
        let with_macro = [dep("/t/debug/deps/libpm-1.so")];
        let registry = Path::new(REGISTRY_PACKAGE_DIR);
        let workspace = Path::new("/w/kt");
        assert!(shared_prediction_eligible(&plain, Some(workspace)));
        assert!(shared_prediction_eligible(&plain, None));
        assert!(
            shared_prediction_eligible(&with_macro, Some(registry)),
            "a registry unit's record carries the tree guard"
        );
        assert!(!shared_prediction_eligible(&with_macro, Some(workspace)));
        assert!(!shared_prediction_eligible(&with_macro, None));
    }

    /// A proc-macro dependent in a registry package gets one shared identity
    /// whichever target directory builds it; in a workspace it gets none.
    #[test]
    fn shared_identity_covers_tree_guarded_registry_units() {
        let _lock = key_test_lock();
        if get_rustc_version(Path::new("rustc")).is_err() {
            return;
        }
        let identity = |checkout: &str, manifest_dir: &str| {
            rustc_shared_prediction_identity_in(
                &registry_unit_args(checkout, &["pm"]),
                unit_vars(checkout, manifest_dir),
            )
        };
        let a = identity("a", REGISTRY_PACKAGE_DIR);
        assert!(
            a.as_deref()
                .is_some_and(|a| a.starts_with("shared-target-v2:"))
        );
        assert_eq!(a, identity("b", REGISTRY_PACKAGE_DIR));
        assert_eq!(identity("a", "/w/kt"), None);
    }

    /// The relocated row is filed under the shared hash, the same for every
    /// target directory, and only for a registry unit with an `OUT_DIR`.
    #[test]
    fn a_relocatable_identity_is_the_shared_hash_of_a_registry_unit_with_an_out_dir() {
        let _lock = key_test_lock();
        if get_rustc_version(Path::new("rustc")).is_err() {
            return;
        }
        let args = registry_unit_args("a", &[]);
        let vars = unit_vars("a", REGISTRY_PACKAGE_DIR);
        let shared = rustc_shared_prediction_identity_in(&args, vars.clone()).unwrap();
        let relocatable = relocatable_prediction_identity(&args, vars).unwrap();
        assert_eq!(
            relocatable.strip_prefix("shared-out-dir-v1:"),
            shared.strip_prefix("shared-target-v2:")
        );
        assert_eq!(
            relocatable_prediction_identity(
                &registry_unit_args("b", &[]),
                unit_vars("b", REGISTRY_PACKAGE_DIR)
            ),
            Some(relocatable)
        );
        assert_eq!(
            relocatable_prediction_identity(
                &args,
                os_vars(&[("CARGO_MANIFEST_DIR", REGISTRY_PACKAGE_DIR)])
            ),
            None,
            "no OUT_DIR to relocate"
        );
        let workspace = unit_vars("a", "/w/kt");
        assert!(rustc_shared_prediction_identity_in(&args, workspace.clone()).is_some());
        assert_eq!(
            relocatable_prediction_identity(&args, workspace),
            None,
            "a workspace unit"
        );
    }

    #[test]
    fn the_registry_src_root_is_two_levels_above_a_registry_package() {
        assert_eq!(
            registry_src_root(Path::new(REGISTRY_PACKAGE_DIR)),
            Some(Path::new("/h/registry/src"))
        );
        assert_eq!(registry_src_root(Path::new("/w/kt")), None);
    }

    #[test]
    fn an_out_dir_suffix_is_the_bytes_after_the_directory() {
        let suffix =
            |value: &str, root: &str| out_dir_suffix(std::ffi::OsStr::new(value), Path::new(root));
        assert_eq!(
            suffix("/o", "/o").as_deref(),
            Some(""),
            "the directory itself"
        );
        assert_eq!(suffix("/o/x/y.rs", "/o").as_deref(), Some("/x/y.rs"));
        assert_eq!(suffix("/o\\x\\y.rs", "/o").as_deref(), Some("\\x\\y.rs"));
        assert_eq!(
            suffix("/o2/x", "/o"),
            None,
            "a longer name is another directory"
        );
        assert_eq!(suffix("/p/x", "/o"), None);
        assert_eq!(suffix("/o/../x", "/o"), None);
        assert_eq!(suffix("/o\\..\\x", "/o"), None);
        assert_eq!(suffix("/o/./x", "/o").as_deref(), Some("/./x"));
        assert_eq!(suffix("/o/./../x", "/o"), None);
        assert_eq!(suffix("/o//x", "/o"), None);
        assert_eq!(suffix("/o/", "/o"), None);
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            assert_eq!(
                out_dir_suffix(std::ffi::OsStr::from_bytes(b"/o/\xff"), Path::new("/o")),
                None,
                "a suffix that is not UTF-8"
            );
        }
        assert!(under_registry_src(
            Path::new("/r/src/i/p/lib.rs"),
            Path::new("/r/src")
        ));
        assert!(
            !under_registry_src(Path::new("/r/src"), Path::new("/r/src")),
            "the root is not a file under it"
        );
    }

    /// Every source of a registry unit's shared row is a registry file; the
    /// rule for other units is unchanged.
    #[test]
    fn a_registry_unit_shares_only_sources_under_the_registry() {
        let target = Path::new("/w/target");
        let can = |sources: &[&str], manifest_dir: Option<&str>| {
            let sources: Vec<PathBuf> = sources.iter().map(PathBuf::from).collect();
            shared_sources_can_record(&sources, target, manifest_dir.map(Path::new))
        };
        let lib = "/h/registry/src/index-1/kt-1.0.0/src/lib.rs";
        let registry = Some(REGISTRY_PACKAGE_DIR);
        assert!(can(
            &[lib, "/h/registry/src/index-2/other-1.0.0/x.rs"],
            registry
        ));
        assert!(!can(&[lib, "/w/Cargo.lock"], registry), "a checkout file");
        assert!(!can(&["/h/registry/src/index-1/../../x.rs"], registry));
        assert!(can(&["/w/kt/src/lib.rs"], Some("/w/kt")));
        assert!(can(&["/w/kt/src/lib.rs"], None));
        assert!(
            !can(&["/w/target/debug/build/kt-1/out/gen.rs"], Some("/w/kt")),
            "no unit shares a target source"
        );
    }

    /// A `..` counts only from deeper than `floor`, and a walk that leaves
    /// never comes back in.
    #[test]
    fn a_walk_stays_below_its_floor() {
        let walk = |path: &str, floor| stays_below(path.split('/'), floor);
        assert!(walk("a/../b", 0));
        assert!(!walk("../b", 0), "a `..` at the floor leaves it");
        assert!(!walk("a/../../a/b", 0), "leaving and coming back");
        assert!(walk("i/p/src/../README.md", 2));
        assert!(walk("i/p/src/a/b/../../x.rs", 2));
        assert!(!walk("i/p/src/../../q/x.rs", 2));
        assert!(!walk("i/../j/p/x.rs", 2), "below the floor too");
        assert!(walk("a/./b", 0), "a `.` stays where it is");
        assert!(walk("./a", 0));
        assert!(!walk("./../b", 0), "a `.` does not go down a level");
        assert!(!walk("i/p/./../q/x.rs", 2));
        assert!(!walk("a//b", 0));
    }

    /// rustc reports `include_str!("../README.md")` as `src/../README.md`
    /// and never normalizes it. Refusing every `..` kept such a package's
    /// record out of other checkouts; one that stays inside the directory is
    /// as portable as a path without it.
    #[test]
    fn an_out_dir_suffix_keeps_a_parent_component_that_stays_inside() {
        let suffix =
            |value: &str, root: &str| out_dir_suffix(std::ffi::OsStr::new(value), Path::new(root));
        assert_eq!(
            suffix("/o/x/../y.rs", "/o").as_deref(),
            Some("/x/../y.rs"),
            "spelled as rustc spelled it"
        );
        assert_eq!(
            suffix("/o\\x\\..\\y.rs", "/o").as_deref(),
            Some("\\x\\..\\y.rs")
        );
        assert_eq!(suffix("/o/x/../../y.rs", "/o"), None);
        assert_eq!(
            suffix("/o/x\\y/../../z.rs", "/o"),
            None,
            "on Unix `x\\y` is one level, so this leaves `/o`"
        );
        assert_eq!(
            suffix("/o/x\\..\\..\\y.rs", "/o"),
            None,
            "on Windows this leaves `/o`"
        );
    }

    /// Path shapes rustc reported for registry dependencies, all refused
    /// while any `..` or `.` was: the `..` ones from hk, `src/./init.js` from
    /// `include_str!("./init.js")`. A `..` that leaves the package is still
    /// refused, even into another registry package.
    #[test]
    fn a_registry_unit_shares_a_parent_path_that_stays_in_its_package() {
        let target = Path::new("/w/target");
        let can = |source: &str| {
            shared_sources_can_record(
                &[PathBuf::from(source)],
                target,
                Some(Path::new(REGISTRY_PACKAGE_DIR)),
            )
        };
        let index = "/h/registry/src/index-1";
        for inside in [
            "kt-1.0.0/src/../README.md",
            "kt-1.0.0/src/crypto/aws_lc_rs/../ring/hash.rs",
            "kt-1.0.0/src/new/glibc/sysdeps/nptl/bits/../../x86/mod.rs",
            "other-1.0.0/src/../data/mod.rs",
            "kt-1.0.0/src/./init.js",
        ] {
            assert!(can(&format!("{index}/{inside}")), "{inside}");
        }
        for outside in [
            "kt-1.0.0/src/../../other-1.0/lib.rs",
            "kt-1.0.0/../other-1.0/lib.rs",
            "kt-1.0.0/src/../../../../../w/src/lib.rs",
            "kt-1.0.0/./../other-1.0/lib.rs",
        ] {
            assert!(!can(&format!("{index}/{outside}")), "{outside}");
        }
    }

    fn portable_roots_for_test() -> PortableRoots {
        PortableRoots {
            out_dir: PathBuf::from("/t/debug/build/kt-1/out"),
            target: PathBuf::from("/t"),
            canonical_target: PathBuf::from("/private/t"),
            registry_src: PathBuf::from("/h/registry/src"),
        }
    }

    fn portable_test_record() -> PortablePrediction {
        PortablePrediction {
            schema: PORTABLE_PREDICTION_SCHEMA,
            sources: vec![
                Portable::Literal("/h/registry/src/index-1/kt-1.0.0/src/lib.rs".to_string()),
                Portable::OutDir("\\version.expr".to_string()),
                Portable::OutDir("/gen.rs".to_string()),
            ],
            env_deps: vec![
                ("OUT_DIR".to_string(), Portable::OutDir(String::new())),
                (
                    "CARGO_PKG_NAME".to_string(),
                    Portable::Literal("kt".to_string()),
                ),
            ],
            tree: "tree-1".to_string(),
        }
    }

    /// A workspace at `/w`, reached through `/private/w` as macOS spells
    /// temporary directories, with its target inside.
    fn workspace_test_roots() -> WorkspaceRoots {
        WorkspaceRoots {
            root: PathBuf::from("/w"),
            cwd: String::new(),
            canonical_root: PathBuf::from("/private/w"),
            target: PathBuf::from("/w/target"),
            canonical_target: PathBuf::from("/private/w/target"),
            out_dir: Some(PathBuf::from("/w/target/debug/build/kt-1/out")),
        }
    }

    #[test]
    fn a_workspace_row_keeps_its_guard_only_while_the_closure_stays_inside() {
        let tree = || Some("tree".to_string());
        assert_eq!(same_tree_guard(tree(), true, true), tree());
        assert_eq!(same_tree_guard(tree(), true, false), None);
        assert_eq!(
            same_tree_guard(tree(), false, false),
            tree(),
            "registry and other units"
        );
        assert_eq!(same_tree_guard(tree(), false, true), tree());
        assert_eq!(same_tree_guard(None, true, true), None);
    }

    /// A two-member-free workspace at `dir/<name>` with its target inside,
    /// and the member invocation Cargo would run there.
    fn workspace_invocation(dir: &Path, name: &str, crate_name: &str) -> (PathBuf, RustcArgs) {
        let root = dir.join(name);
        std::fs::create_dir_all(root.join("kt/src")).unwrap();
        std::fs::create_dir_all(root.join("target/debug/deps")).unwrap();
        std::fs::write(root.join("Cargo.toml"), "[workspace]\n").unwrap();
        let deps = root.join("target/debug/deps").display().to_string();
        let argv: Vec<String> = [
            "rustc",
            "--crate-name",
            crate_name,
            "--edition=2021",
            "kt/src/lib.rs",
            "--crate-type",
            "lib",
            "--out-dir",
            &deps,
            "-L",
            &format!("dependency={deps}"),
        ]
        .iter()
        .map(|arg| arg.to_string())
        .collect();
        (root, RustcArgs::parse(&argv).unwrap())
    }

    fn manifest_vars(manifest_dir: &Path) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
        vec![(
            "CARGO_MANIFEST_DIR".into(),
            manifest_dir.as_os_str().to_owned(),
        )]
    }

    #[test]
    fn only_a_package_inside_the_workspace_is_a_workspace_unit() {
        let dir = tempfile::tempdir().unwrap();
        let (root, args) = workspace_invocation(dir.path(), "a", "kt");
        let inside = workspace_roots_in(&args, &manifest_vars(&root.join("kt")), &root).unwrap();
        assert_eq!(inside.cwd, "");
        assert_eq!(inside.target, root.join("target"));
        let member_cwd =
            workspace_roots_in(&args, &manifest_vars(&root.join("kt")), &root.join("kt"));
        assert_eq!(
            member_cwd.map(|roots| roots.cwd),
            Some(format!("{}kt", std::path::MAIN_SEPARATOR))
        );
        assert!(
            workspace_roots_in(&args, &manifest_vars(&dir.path().join("elsewhere")), &root)
                .is_none(),
            "a package outside the workspace"
        );
        let registry = dir.path().join("home/registry/src/index-1/kt-1.0.0");
        assert!(workspace_roots_in(&args, &manifest_vars(&registry), &root).is_none());
        assert!(
            workspace_roots_in(&args, &[], &root).is_none(),
            "no package at all"
        );
    }

    #[test]
    fn a_workspace_identity_is_the_same_in_every_checkout_and_per_unit() {
        let dir = tempfile::tempdir().unwrap();
        let identity = |name: &str, crate_name: &str| {
            let (root, args) = workspace_invocation(dir.path(), name, crate_name);
            let vars = manifest_vars(&root.join("kt"));
            let roots = workspace_roots_in(&args, &vars, &root).unwrap();
            workspace_prediction_identity(&args, vars, &roots).unwrap()
        };
        let a = identity("a", "kt");
        assert!(a.starts_with(WORKSPACE_PREDICTION_PREFIX), "{a}");
        assert_eq!(a, identity("b", "kt"), "another checkout of the same unit");
        assert_ne!(a, identity("c", "other"), "another unit");
    }

    #[test]
    fn a_remote_row_is_kept_only_once_it_checks_out() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.db");
        let hasher = FileHasher::persistent(&db);
        let args = RustcArgs::parse(&[
            "rustc".to_string(),
            "--crate-name".to_string(),
            "kt".to_string(),
            "src/lib.rs".to_string(),
        ])
        .unwrap();
        let record = InputPrediction {
            schema: PREDICTION_SCHEMA,
            sources: vec![PathBuf::from("/r/i/kt-1.0.0/src/lib.rs")],
            env_deps: vec![],
            tree: None,
        };
        let dep_info = DepInfo {
            source_files: record.sources.clone(),
            env_deps: vec![],
        };
        keep_remote_plain_row(&hasher, "refused", &args, &record, &Err(Rejection::Missing));
        assert_eq!(hasher.input_prediction("refused"), None);
        keep_remote_plain_row(&hasher, "kept", &args, &record, &Ok(dep_info));
        assert_eq!(
            hasher.input_prediction("kept").map(|kept| kept.sources),
            Some(record.sources.clone())
        );
    }

    #[test]
    fn a_workspace_value_is_relocated_refused_or_kept() {
        let roots = workspace_test_roots();
        let value = |value: &str| workspace_portable_value(std::ffi::OsStr::new(value), &roots);
        assert_eq!(
            value("/w/target/debug/build/kt-1/out/gen.rs"),
            Some(Portable::OutDir("/gen.rs".to_string()))
        );
        assert_eq!(
            value("/w/target/debug/deps/libx.rlib"),
            None,
            "another unit's output"
        );
        assert_eq!(value("/private/w/target/debug/deps/libx.rlib"), None);
        assert_eq!(
            value("/w/kt/src/lib.rs"),
            Some(Portable::Workspace("/kt/src/lib.rs".to_string()))
        );
        assert_eq!(
            value("/private/w/assets/a.txt"),
            Some(Portable::Workspace("/assets/a.txt".to_string()))
        );
        assert_eq!(value("/w"), Some(Portable::Workspace(String::new())));
        assert_eq!(
            value("/w2/x"),
            Some(Portable::Literal("/w2/x".to_string())),
            "a sibling that merely starts the same is not the workspace"
        );
        assert_eq!(value("kt"), Some(Portable::Literal("kt".to_string())));
    }

    #[test]
    fn a_relative_source_must_stay_in_the_workspace() {
        let mut roots = workspace_test_roots();
        let source = |spelled: &str, roots: &WorkspaceRoots| {
            workspace_relative_source(Path::new(spelled), roots)
        };
        let kept = |spelled: &str| Some(Portable::Literal(spelled.to_string()));
        assert_eq!(source("kt/src/lib.rs", &roots), kept("kt/src/lib.rs"));
        assert_eq!(
            source("kt/src/../../assets/a.txt", &roots),
            kept("kt/src/../../assets/a.txt")
        );
        assert_eq!(source("../outside.txt", &roots), None);
        assert_eq!(source("kt//lib.rs", &roots), None);
        assert_eq!(
            source("kt\\..\\..\\outside.txt", &roots),
            None,
            "a Windows walk that leaves, which a `/` split alone would miss"
        );
        roots.cwd = "/kt".to_string();
        assert_eq!(source("../assets/a.txt", &roots), kept("../assets/a.txt"));
        assert_eq!(source("../../outside.txt", &roots), None);
    }

    #[test]
    fn a_workspace_record_needs_the_guard_and_every_source_inside() {
        let roots = workspace_test_roots();
        let dep_info = |sources: &[&str]| DepInfo {
            source_files: sources.iter().map(PathBuf::from).collect(),
            env_deps: vec![("CARGO_MANIFEST_DIR".to_string(), "/w/kt".to_string())],
        };
        let inside = dep_info(&["kt/src/lib.rs", "/w/target/debug/build/kt-1/out/gen.rs"]);
        assert_eq!(
            workspace_portable_prediction(&inside, &roots, Some("tree-1")),
            Some(PortablePrediction {
                schema: PORTABLE_PREDICTION_SCHEMA,
                sources: vec![
                    Portable::Literal("kt/src/lib.rs".to_string()),
                    Portable::OutDir("/gen.rs".to_string()),
                ],
                env_deps: vec![(
                    "CARGO_MANIFEST_DIR".to_string(),
                    Portable::Workspace("/kt".to_string())
                )],
                tree: "tree-1".to_string(),
            })
        );
        assert_eq!(workspace_portable_prediction(&inside, &roots, None), None);
        for outside in ["/elsewhere/x.rs", "../x.rs", "/w/target/debug/deps/x.rs"] {
            let closure = dep_info(&["kt/src/lib.rs", outside]);
            assert_eq!(
                workspace_portable_prediction(&closure, &roots, Some("t")),
                None,
                "{outside}"
            );
        }
        let mut env_in_target = dep_info(&["kt/src/lib.rs"]);
        env_in_target.env_deps = vec![("X".to_string(), "/w/target/debug/x".to_string())];
        assert_eq!(
            workspace_portable_prediction(&env_in_target, &roots, Some("t")),
            None
        );
    }

    #[test]
    fn a_workspace_entry_resolves_only_where_this_invocation_has_a_workspace() {
        let record = PortablePrediction {
            schema: PORTABLE_PREDICTION_SCHEMA,
            sources: vec![Portable::Workspace("/kt/src/lib.rs".to_string())],
            env_deps: vec![],
            tree: "t".to_string(),
        };
        let places = |out_dir, workspace| Places {
            out_dir,
            workspace,
            registry: None,
        };
        assert_eq!(record.resolve(&places(Some("/o"), None)), None);
        assert_eq!(
            record
                .resolve(&places(None, Some("/v")))
                .map(|resolved| resolved.sources),
            Some(vec![PathBuf::from("/v/kt/src/lib.rs")])
        );
        assert_eq!(
            portable_test_record().resolve(&places(None, Some("/v"))),
            None,
            "an OUT_DIR entry needs an OUT_DIR"
        );
    }

    #[test]
    fn the_workspace_guard_covers_everything_but_target_and_git() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("w");
        let write = |path: &str, content: &str| {
            let path = root.join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, content).unwrap();
        };
        write("Cargo.toml", "[workspace]\n");
        write("kt/src/lib.rs", "pub fn a() {}\n");
        write("assets/a.txt", "a\n");
        write("target/debug/build/kt-1/out/gen.rs", "// gen\n");
        let roots = WorkspaceRoots {
            root: root.clone(),
            cwd: String::new(),
            canonical_root: root.canonicalize().unwrap(),
            target: root.join("target"),
            canonical_target: root.join("target").canonicalize().unwrap(),
            out_dir: Some(root.join("target/debug/build/kt-1/out")),
        };
        let hasher = FileHasher::new();
        let digest = || workspace_tree_digest(&roots, &hasher).unwrap();
        let baseline = digest();

        write("target/debug/deps/libx.rlib", "x");
        write(".git/HEAD", "ref");
        assert_eq!(
            digest(),
            baseline,
            "target and .git are not what a macro reads"
        );

        write("docs/new.md", "new");
        assert_ne!(digest(), baseline, "a new file anywhere in the workspace");
        std::fs::remove_file(root.join("docs/new.md")).unwrap();
        std::fs::remove_dir(root.join("docs")).unwrap();
        assert_eq!(digest(), baseline);

        write("assets/a.txt", "changed");
        assert_ne!(digest(), baseline, "content");
        write("assets/a.txt", "a\n");
        write("target/debug/build/kt-1/out/gen.rs", "// other\n");
        assert_ne!(digest(), baseline, "OUT_DIR, though it lies under target");
    }

    #[test]
    fn the_workspace_guard_gives_up_past_its_entry_budget() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("w");
        std::fs::create_dir_all(root.join("target")).unwrap();
        for name in ["a", "b", "c"] {
            std::fs::write(root.join(name), name).unwrap();
        }
        let roots = WorkspaceRoots {
            root: root.clone(),
            cwd: String::new(),
            canonical_root: root.clone(),
            target: root.join("target"),
            canonical_target: root.join("target"),
            out_dir: None,
        };
        let hasher = FileHasher::new();
        // Three files; `target` is skipped before it is counted.
        assert!(workspace_tree_digest_within(&roots, &hasher, 3).is_some());
        assert!(workspace_tree_digest_within(&roots, &hasher, 2).is_none());
    }

    #[test]
    fn a_portable_record_relocates_out_dir_and_keeps_registry_paths() {
        let dep_info = DepInfo {
            source_files: vec![
                PathBuf::from("/h/registry/src/index-1/kt-1.0.0/src/lib.rs"),
                PathBuf::from("/t/debug/build/kt-1/out\\version.expr"),
                PathBuf::from("/t/debug/build/kt-1/out/gen.rs"),
            ],
            env_deps: vec![
                ("OUT_DIR".to_string(), "/t/debug/build/kt-1/out".to_string()),
                ("CARGO_PKG_NAME".to_string(), "kt".to_string()),
            ],
        };
        assert_eq!(
            portable_prediction(&dep_info, &portable_roots_for_test(), Some("tree-1")),
            Some(portable_test_record())
        );
    }

    #[test]
    fn a_portable_record_refuses_anything_that_could_name_another_file() {
        let roots = portable_roots_for_test();
        let lib = "/h/registry/src/index-1/kt-1.0.0/src/lib.rs";
        let generated = "/t/debug/build/kt-1/out/gen.rs";
        let portable = |sources: &[&str], env: &[(&str, &str)], tree: Option<&str>| {
            let dep_info = DepInfo {
                source_files: sources.iter().map(PathBuf::from).collect(),
                env_deps: env
                    .iter()
                    .map(|(name, value)| (name.to_string(), value.to_string()))
                    .collect(),
            };
            portable_prediction(&dep_info, &roots, tree)
        };
        assert!(portable(&[lib, generated], &[], Some("tree")).is_some());
        for (source, why) in [
            ("/t/debug/build/other-2/out/x.rs", "another unit's output"),
            (
                "/private/t/debug/build/kt-1/out/gen.rs",
                "a canonical spelling of the target",
            ),
            ("/w/src/lib.rs", "a checkout path"),
            ("src/lib.rs", "a relative path"),
            (
                "/t/debug/build/kt-1/out/../../other-2/out/x.rs",
                "a parent component that leaves OUT_DIR",
            ),
            (
                "/h/registry/src/../x.rs",
                "a parent component under the registry",
            ),
        ] {
            assert_eq!(
                portable(&[lib, generated, source], &[], Some("tree")),
                None,
                "{why}"
            );
        }
        assert_eq!(portable(&[lib, generated], &[], None), None, "no guard");
        assert_eq!(
            portable(&[lib], &[], Some("tree")),
            None,
            "nothing to relocate"
        );
        for value in ["/t/debug/deps", "/private/t/debug/build/kt-1/out"] {
            assert_eq!(
                portable(&[lib, generated], &[("X", value)], Some("tree")),
                None,
                "{value} spells the target"
            );
        }
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            let dep_info = DepInfo {
                source_files: vec![
                    PathBuf::from(generated),
                    PathBuf::from(std::ffi::OsStr::from_bytes(b"/h/registry/src/i/\xff.rs")),
                ],
                env_deps: Vec::new(),
            };
            assert_eq!(
                portable_prediction(&dep_info, &roots, Some("tree")),
                None,
                "a literal that is not UTF-8"
            );
        }
    }

    /// The relocated row follows the shared row's rule: a `..` inside the
    /// package stays a literal, one inside `OUT_DIR` is relocated as spelled,
    /// and one that leaves either refuses the row.
    #[test]
    fn a_portable_record_keeps_a_parent_path_inside_out_dir_or_the_package() {
        let roots = portable_roots_for_test();
        let readme = "/h/registry/src/index-1/kt-1.0.0/src/../README.md";
        let generated = "/t/debug/build/kt-1/out/sub/../gen.rs";
        let portable = |sources: &[&str]| {
            let dep_info = DepInfo {
                source_files: sources.iter().map(PathBuf::from).collect(),
                env_deps: Vec::new(),
            };
            portable_prediction(&dep_info, &roots, Some("tree"))
        };
        assert_eq!(
            portable(&[readme, generated]).map(|record| record.sources),
            Some(vec![
                Portable::Literal(readme.to_string()),
                Portable::OutDir("/sub/../gen.rs".to_string()),
            ])
        );
        for (source, why) in [
            (
                "/h/registry/src/index-1/kt-1.0.0/src/../../other-1.0/lib.rs",
                "leaves the package",
            ),
            ("/t/debug/build/kt-1/out/../out/gen.rs", "leaves OUT_DIR"),
        ] {
            assert_eq!(portable(&[generated, source]), None, "{why}");
        }
    }

    #[test]
    fn a_portable_record_resolves_to_this_out_dir_byte_for_byte() {
        assert_eq!(
            portable_test_record().resolve(&Places {
                out_dir: Some("/u/debug/build/kt-1/out"),
                workspace: None,
                registry: None,
            }),
            Some(InputPrediction {
                schema: PREDICTION_SCHEMA,
                sources: vec![
                    PathBuf::from("/h/registry/src/index-1/kt-1.0.0/src/lib.rs"),
                    PathBuf::from("/u/debug/build/kt-1/out\\version.expr"),
                    PathBuf::from("/u/debug/build/kt-1/out/gen.rs"),
                ],
                env_deps: vec![
                    ("OUT_DIR".to_string(), "/u/debug/build/kt-1/out".to_string()),
                    ("CARGO_PKG_NAME".to_string(), "kt".to_string()),
                ],
                tree: Some("tree-1".to_string()),
            })
        );
    }

    /// Relocation replaces the prefix only; the raw env value still has to
    /// agree, and the guard has to be the one the row was made with.
    #[test]
    fn a_relocated_record_is_checked_against_this_out_dir_and_guard() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let record = PortablePrediction {
            schema: PORTABLE_PREDICTION_SCHEMA,
            sources: vec![Portable::OutDir("/gen.rs".to_string())],
            env_deps: vec![("OUT_DIR".to_string(), Portable::OutDir(String::new()))],
            tree: "tree-1".to_string(),
        };
        let check = |guard: &str, env_out_dir: &str| {
            validate_portable_prediction(
                &record,
                guard,
                &Places {
                    out_dir: Some("/u/out"),
                    workspace: None,
                    registry: None,
                },
                |_| std::fs::metadata(file.path()).ok(),
                |_| false,
                |var| (var == "OUT_DIR").then(|| env_out_dir.to_string()),
            )
        };
        assert_eq!(
            check("tree-1", "/u/out"),
            Ok(DepInfo {
                source_files: vec![PathBuf::from("/u/out/gen.rs")],
                env_deps: vec![("OUT_DIR".to_string(), "/u/out".to_string())],
            })
        );
        assert_eq!(check("tree-1", "/v/out"), Err(Rejection::EnvChanged));
        assert_eq!(check("tree-2", "/u/out"), Err(Rejection::TreeChanged));
    }

    #[test]
    fn mentions_root_finds_raw_canonical_and_escaped_spellings() {
        let raw = Path::new("/t/target");
        let canonical = Path::new("/private/t/target");
        assert!(mentions_root(b"// built in /t/target/debug\n", &[raw]));
        assert!(
            mentions_root(
                b"x /private/t/target/y",
                &[Path::new("/nowhere"), canonical]
            ),
            "any root"
        );
        assert!(!mentions_root(b"pub fn generated() {}", &[raw, canonical]));
        let windows = Path::new("C:\\t\\target");
        assert!(mentions_root(b"C:\\t\\target\\x", &[windows]), "as-is");
        assert!(
            mentions_root(b"\"C:\\\\t\\\\target\\\\x.rs\"", &[windows]),
            "escaped in a string literal"
        );
        assert!(!mentions_root(b"C:/t/target", &[windows]));
    }

    #[test]
    fn an_out_dir_is_relocatable_only_when_no_file_names_the_target() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        let out = target.join("debug/build/kt-1/out");
        std::fs::create_dir_all(out.join("nested")).unwrap();
        std::fs::write(out.join("gen.rs"), "pub fn g() {}\n").unwrap();
        std::fs::write(out.join("nested/more.rs"), "pub fn h() {}\n").unwrap();
        let roots = [target.as_path()];
        assert!(out_dir_spells_no_root(&out, &roots));
        std::fs::write(
            out.join("nested/more.rs"),
            format!("// {}\n", target.display()),
        )
        .unwrap();
        assert!(!out_dir_spells_no_root(&out, &roots), "a nested file");
        std::fs::write(out.join("nested/more.rs"), "pub fn h() {}\n").unwrap();
        #[cfg(unix)]
        {
            let link = out.join("link.rs");
            std::os::unix::fs::symlink(target.join("elsewhere.rs"), &link).unwrap();
            assert!(!out_dir_spells_no_root(&out, &roots), "a symlink's text");
            std::fs::remove_file(link).unwrap();
        }
        assert!(out_dir_spells_no_root(&out, &roots));
        assert!(
            !out_dir_spells_no_root(&dir.path().join("absent"), &roots),
            "a directory that cannot be read"
        );
        // 3 entries so far; the cap counts every entry.
        for index in 3..OUT_DIR_TREE_MAX_ENTRIES {
            std::fs::write(out.join(format!("f{index}")), "x").unwrap();
        }
        assert!(out_dir_spells_no_root(&out, &roots), "exactly at the cap");
        std::fs::write(out.join("one-more"), "x").unwrap();
        assert!(!out_dir_spells_no_root(&out, &roots), "past the cap");
    }

    #[test]
    fn the_out_dir_guard_follows_content_and_stops_at_its_cap() {
        let dir = tempfile::tempdir().unwrap();
        let hasher = FileHasher::new();
        let a = dir.path().join("a/out");
        let b = dir.path().join("b/out");
        for out in [&a, &b] {
            std::fs::create_dir_all(out).unwrap();
            std::fs::write(out.join("gen.rs"), "pub fn g() {}\n").unwrap();
        }
        let guard = out_dir_tree_digest(&a, &hasher).unwrap();
        assert_eq!(
            out_dir_tree_digest(&b, &hasher).as_deref(),
            Some(guard.as_str())
        );
        std::fs::write(b.join("extra.rs"), "").unwrap();
        assert_ne!(
            out_dir_tree_digest(&b, &hasher).unwrap(),
            guard,
            "a new file"
        );
        for index in 2..OUT_DIR_TREE_MAX_ENTRIES {
            std::fs::write(b.join(format!("f{index}")), "x").unwrap();
        }
        assert!(
            out_dir_tree_digest(&b, &hasher).is_some(),
            "exactly at the cap"
        );
        std::fs::write(b.join("one-more"), "x").unwrap();
        assert_eq!(out_dir_tree_digest(&b, &hasher), None, "past the cap");
    }

    /// A build script can put anything in `OUT_DIR`, a nested `target` or a
    /// git checkout included, and a macro scanning `OUT_DIR` reads it all.
    /// Both guards that cover `OUT_DIR` digest it all; only the crate
    /// directory skips those names.
    #[test]
    fn the_out_dir_guards_cover_target_and_git_under_out_dir() {
        let dir = tempfile::tempdir().unwrap();
        let hasher = FileHasher::new();
        let package = dir.path().join("registry/src/index/kt-1.0.0");
        let out = dir.path().join("out");
        for directory in [&package, &out] {
            std::fs::create_dir_all(directory).unwrap();
            std::fs::write(directory.join("lib.rs"), "pub fn g() {}\n").unwrap();
        }
        let guards = || {
            (
                out_dir_tree_digest(&out, &hasher).unwrap(),
                crate_tree_digest_in(package.clone(), Some(out.clone()), &hasher).unwrap(),
            )
        };
        let before = guards();
        for name in ["target", ".git"] {
            std::fs::create_dir_all(package.join(name)).unwrap();
            std::fs::write(package.join(name).join("x"), "x").unwrap();
        }
        assert_eq!(guards().1, before.1, "the crate directory skips them");
        for name in ["target", ".git"] {
            let nested = out.join(name);
            std::fs::create_dir_all(&nested).unwrap();
            std::fs::write(nested.join("x"), "x").unwrap();
            let after = guards();
            assert_ne!(after.0, before.0, "{name} under OUT_DIR, OUT_DIR guard");
            assert_ne!(after.1, before.1, "{name} under OUT_DIR, crate tree guard");
            std::fs::remove_dir_all(nested).unwrap();
        }
        assert_eq!(guards(), before);
    }

    /// A relocated row round-trips, and neither decoder accepts the other's
    /// schema, whatever identity it is looked up under.
    #[test]
    fn a_portable_prediction_round_trips_and_no_other_decoder_reads_it() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.db");
        let hasher = FileHasher::persistent(&db);
        let record = portable_test_record();
        assert_eq!(hasher.portable_prediction("unit"), None);
        hasher.record_portable_prediction("unit", Some("kt"), &record);
        assert_eq!(
            FileHasher::persistent(&db).portable_prediction("unit"),
            Some(record)
        );
        assert_eq!(hasher.input_prediction("unit"), None);
        hasher.record_input_prediction(
            "plain",
            Some("kt"),
            &DepInfo {
                source_files: vec![PathBuf::from("/w/src/lib.rs")],
                env_deps: Vec::new(),
            },
            Some("tree".to_string()),
        );
        assert_eq!(hasher.portable_prediction("plain"), None);
        let cache = hasher.cache.as_ref().unwrap();
        cache
            .put_input_prediction("corrupt", PORTABLE_PREDICTION_SCHEMA, None, "not json")
            .unwrap();
        assert_eq!(hasher.portable_prediction("corrupt"), None);
        let stale = serde_json::to_string(&PortablePrediction {
            schema: PREDICTION_SCHEMA,
            ..portable_test_record()
        })
        .unwrap();
        cache
            .put_input_prediction("mismatched", PORTABLE_PREDICTION_SCHEMA, None, &stale)
            .unwrap();
        assert_eq!(hasher.portable_prediction("mismatched"), None);

        let no_db = FileHasher::new();
        no_db.record_portable_prediction("unit", None, &portable_test_record());
        assert_eq!(no_db.portable_prediction("unit"), None);
    }

    /// A tree the recorder wrote the relocated row from, in a temporary
    /// registry: the package, and target `a` with its OUT_DIR.
    fn relocatable_fixture() -> (tempfile::TempDir, PathBuf, PathBuf, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let package = dir.path().join("home/registry/src/index-1/kt-1.0.0");
        std::fs::create_dir_all(package.join("src")).unwrap();
        std::fs::write(package.join("src/lib.rs"), "include!(\"x\");\n").unwrap();
        let target = dir.path().join("a/target");
        let out = target.join("debug/build/kt-1/out");
        std::fs::create_dir_all(&out).unwrap();
        std::fs::create_dir_all(target.join("debug/deps")).unwrap();
        std::fs::write(out.join("gen.rs"), "pub fn g() {}\n").unwrap();
        (dir, package, target, out)
    }

    fn unit_args_in(package: &Path, target: &Path) -> RustcArgs {
        RustcArgs::parse(&[
            "rustc".to_string(),
            "--crate-name".to_string(),
            "kt".to_string(),
            package.join("src/lib.rs").display().to_string(),
            "--out-dir".to_string(),
            target.join("debug/deps").display().to_string(),
        ])
        .unwrap()
    }

    #[test]
    fn a_relocatable_record_needs_a_registry_unit_and_a_clean_out_dir() {
        let _lock = key_test_lock();
        if get_rustc_version(Path::new("rustc")).is_err() {
            return;
        }
        let (dir, package, target, out) = relocatable_fixture();
        let args = unit_args_in(&package, &target);
        let vars = |manifest_dir: &Path| {
            vec![
                ("CARGO_MANIFEST_DIR".into(), manifest_dir.into()),
                ("OUT_DIR".into(), out.clone().into()),
            ]
        };
        let dep_info = DepInfo {
            source_files: vec![package.join("src/lib.rs"), out.join("gen.rs")],
            env_deps: Vec::new(),
        };
        let (identity, record) =
            relocatable_record_in(&args, &dep_info, Some("tree"), vars(&package)).unwrap();
        assert_eq!(
            Some(identity),
            relocatable_prediction_identity(&args, vars(&package))
        );
        assert_eq!(
            record.sources,
            vec![
                Portable::Literal(package.join("src/lib.rs").display().to_string()),
                Portable::OutDir(format!("{}gen.rs", std::path::MAIN_SEPARATOR)),
            ]
        );
        assert_eq!(
            relocatable_record_in(
                &args,
                &dep_info,
                Some("tree"),
                vars(&dir.path().join("w/kt"))
            ),
            None,
            "a workspace unit"
        );
        let canonical = std::fs::canonicalize(&target).unwrap();
        for spelled in [&target, &canonical] {
            std::fs::write(out.join("gen.rs"), format!("// {}\n", spelled.display())).unwrap();
            assert_eq!(
                relocatable_record_in(&args, &dep_info, Some("tree"), vars(&package)),
                None,
                "{} is spelled in OUT_DIR",
                spelled.display()
            );
        }
    }

    /// Through a symlink, the canonical target shares nothing with the raw
    /// spelling, so the scan has to look for it on its own.
    #[cfg(unix)]
    #[test]
    fn an_out_dir_naming_only_the_canonical_target_is_not_relocated() {
        let _lock = key_test_lock();
        if get_rustc_version(Path::new("rustc")).is_err() {
            return;
        }
        let (dir, package, _, _) = relocatable_fixture();
        let real = std::fs::canonicalize(dir.path()).unwrap().join("real");
        std::fs::create_dir_all(real.join("debug/build/kt-1/out")).unwrap();
        std::fs::create_dir_all(real.join("debug/deps")).unwrap();
        let target = dir.path().join("b/target");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(&real, &target).unwrap();
        let out = target.join("debug/build/kt-1/out");
        let args = unit_args_in(&package, &target);
        let vars = || {
            vec![
                ("CARGO_MANIFEST_DIR".into(), package.clone().into()),
                ("OUT_DIR".into(), out.clone().into()),
            ]
        };
        let dep_info = DepInfo {
            source_files: vec![package.join("src/lib.rs"), out.join("gen.rs")],
            env_deps: Vec::new(),
        };
        std::fs::write(out.join("gen.rs"), "pub fn g() {}\n").unwrap();
        assert!(relocatable_record_in(&args, &dep_info, Some("tree"), vars()).is_some());

        let content = format!("// {}\n", real.display());
        assert!(!mentions_root(content.as_bytes(), &[&target]));
        std::fs::write(out.join("gen.rs"), content).unwrap();
        assert_eq!(
            relocatable_record_in(&args, &dep_info, Some("tree"), vars()),
            None
        );
    }

    /// A workspace unit never reads a relocated row, even one filed under the
    /// identity it would have: its package is not the same files in another
    /// checkout.
    #[test]
    fn a_workspace_unit_never_reads_a_relocated_record() {
        let _lock = key_test_lock();
        if get_rustc_version(Path::new("rustc")).is_err() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let package = dir.path().join("w/kt");
        std::fs::create_dir_all(package.join("src")).unwrap();
        std::fs::write(package.join("src/lib.rs"), "include!(\"x\");\n").unwrap();
        let target = dir.path().join("b/target");
        let out = target.join("debug/build/kt-1/out");
        std::fs::create_dir_all(&out).unwrap();
        std::fs::write(out.join("gen.rs"), "pub fn g() {}\n").unwrap();
        let _manifest =
            crate::config::tests::set_env_for_test("CARGO_MANIFEST_DIR", Some(package.as_os_str()));
        let _out = crate::config::tests::set_env_for_test("OUT_DIR", Some(out.as_os_str()));
        let args = unit_args_in(&package, &target);
        let hasher =
            FileHasher::persistent(&dir.path().join("index.db")).with_input_predictions(true);
        let shared = rustc_shared_prediction_identity(&args).unwrap();
        let would_be = format!(
            "{RELOCATABLE_PREDICTION_PREFIX}{}",
            shared.strip_prefix(SHARED_PREDICTION_PREFIX).unwrap()
        );
        let record = PortablePrediction {
            schema: PORTABLE_PREDICTION_SCHEMA,
            sources: vec![
                Portable::Literal(package.join("src/lib.rs").display().to_string()),
                Portable::OutDir("/gen.rs".to_string()),
            ],
            env_deps: vec![("OUT_DIR".to_string(), Portable::OutDir(String::new()))],
            tree: out_dir_tree_digest(&out, &hasher).unwrap(),
        };
        hasher.record_portable_prediction(&would_be, Some("kt"), &record);
        assert_eq!(
            predicted_key_inputs(&args, &hasher),
            Err(Rejection::NoRecord)
        );
    }

    /// After this checkout's row and the shared one miss, a registry unit
    /// reads the relocated row under its OUT_DIR guard, and stashes that guard
    /// before the lookup so the record it makes carries it.
    #[test]
    fn a_registry_unit_reads_a_relocated_record_under_its_out_dir_guard() {
        let _lock = key_test_lock();
        if get_rustc_version(Path::new("rustc")).is_err() {
            return;
        }
        let (dir, package, _, _) = relocatable_fixture();
        let target = dir.path().join("b/target");
        let out = target.join("debug/build/kt-1/out");
        std::fs::create_dir_all(&out).unwrap();
        std::fs::write(out.join("gen.rs"), "pub fn g() {}\n").unwrap();
        let _manifest =
            crate::config::tests::set_env_for_test("CARGO_MANIFEST_DIR", Some(package.as_os_str()));
        let _out = crate::config::tests::set_env_for_test("OUT_DIR", Some(out.as_os_str()));
        let args = unit_args_in(&package, &target);
        let hasher =
            FileHasher::persistent(&dir.path().join("index.db")).with_input_predictions(true);
        let guard = out_dir_tree_digest(&out, &hasher).unwrap();

        take_last_tree_digest();
        assert_eq!(
            predicted_key_inputs(&args, &hasher),
            Err(Rejection::NoRecord)
        );
        assert_eq!(
            take_last_tree_digest().as_deref(),
            Some(guard.as_str()),
            "a cold build records the guard it saw before compiling"
        );

        let identity =
            relocatable_prediction_identity(&args, std::env::vars_os().collect()).unwrap();
        let record = PortablePrediction {
            schema: PORTABLE_PREDICTION_SCHEMA,
            sources: vec![
                Portable::Literal(package.join("src/lib.rs").display().to_string()),
                Portable::OutDir("/gen.rs".to_string()),
            ],
            env_deps: vec![("OUT_DIR".to_string(), Portable::OutDir(String::new()))],
            tree: guard.clone(),
        };
        hasher.record_portable_prediction(&identity, Some("kt"), &record);
        let dep_info = predicted_key_inputs(&args, &hasher).unwrap();
        assert_eq!(
            dep_info.source_files,
            vec![package.join("src/lib.rs"), out.join("gen.rs")]
        );
        assert_eq!(
            dep_info.env_deps,
            vec![("OUT_DIR".to_string(), out.display().to_string())]
        );
        assert_eq!(take_last_tree_digest(), Some(guard));

        std::fs::write(out.join("extra.rs"), "").unwrap();
        assert_eq!(
            predicted_key_inputs(&args, &hasher),
            Err(Rejection::TreeChanged),
            "a file added under OUT_DIR changes the guard"
        );
    }

    /// An aliased `OUT_DIR` (see `out_dir_alias`) is one path outside every
    /// target directory, so the shared identity keeps it as it is. Two target
    /// directories that alias the unit read one shared row, and a build with
    /// its own `OUT_DIR` never reads that row.
    ///
    /// With `macro_dep` the unit links a proc macro, so the row is read under
    /// the crate tree guard, and the guard digests the alias as the unit's
    /// `OUT_DIR`.
    fn an_aliased_out_dir_takes_the_shared_row(macro_dep: bool) {
        let _lock = key_test_lock();
        if get_rustc_version(Path::new("rustc")).is_err() {
            return;
        }
        let (dir, package, target_a, _) = relocatable_fixture();
        let target_b = dir.path().join("b/target");
        let own_out_dir = target_b.join("debug/build/kt-1/out");
        std::fs::create_dir_all(&own_out_dir).unwrap();
        std::fs::create_dir_all(target_b.join("debug/deps")).unwrap();
        let alias = dir
            .path()
            .join("cache/out-dirs/v1/d/kt-0123456789abcdef/out");
        std::fs::create_dir_all(&alias).unwrap();
        let vars = |out_dir: &Path| {
            vec![
                ("CARGO_MANIFEST_DIR".into(), package.clone().into()),
                ("OUT_DIR".into(), out_dir.into()),
            ]
        };
        let args_in = |target: &Path| {
            let deps = target.join("debug/deps");
            let mut argv = vec![
                "rustc".to_string(),
                "--crate-name".to_string(),
                "kt".to_string(),
                package.join("src/lib.rs").display().to_string(),
                "--out-dir".to_string(),
                deps.display().to_string(),
            ];
            if macro_dep {
                argv.push("--extern".to_string());
                argv.push(format!("pm={}", deps.join("libpm-1.so").display()));
            }
            RustcArgs::parse(&argv).unwrap()
        };
        let args_a = args_in(&target_a);
        let args_b = args_in(&target_b);
        assert_eq!(prediction_applies(&args_a.externs), !macro_dep);
        let shared = |args: &RustcArgs, out_dir: &Path| {
            rustc_shared_prediction_identity_in(args, vars(out_dir)).unwrap()
        };
        assert_eq!(shared(&args_a, &alias), shared(&args_b, &alias));
        assert_ne!(shared(&args_b, &alias), shared(&args_b, &own_out_dir));

        let debug_dir = alias.join("debug");
        let dep_info = DepInfo {
            source_files: vec![package.join("src/lib.rs")],
            env_deps: vec![
                ("OUT_DIR".to_string(), alias.display().to_string()),
                (
                    "DEBUG_OUTPUT_DIR".to_string(),
                    debug_dir.display().to_string(),
                ),
            ],
        };
        for args in [&args_a, &args_b] {
            assert!(shared_prediction_can_record_in(
                args,
                &dep_info,
                Some(&package)
            ));
        }

        let _manifest =
            crate::config::tests::set_env_for_test("CARGO_MANIFEST_DIR", Some(package.as_os_str()));
        let _out = crate::config::tests::set_env_for_test("OUT_DIR", Some(alias.as_os_str()));
        let _debug =
            crate::config::tests::set_env_for_test("DEBUG_OUTPUT_DIR", Some(debug_dir.as_os_str()));
        let hasher =
            FileHasher::persistent(&dir.path().join("index.db")).with_input_predictions(true);
        let tree = macro_dep.then(|| crate_tree_digest(&hasher).unwrap());
        let identity = rustc_shared_prediction_identity(&args_a).unwrap();
        hasher.record_input_prediction(&identity, Some("kt"), &dep_info, tree);
        assert_eq!(predicted_key_inputs(&args_b, &hasher), Ok(dep_info));

        if macro_dep {
            std::fs::write(alias.join("stray.rs"), "").unwrap();
            assert_eq!(
                predicted_key_inputs(&args_b, &hasher),
                Err(Rejection::TreeChanged),
                "the tree guard reads the alias as OUT_DIR"
            );
            std::fs::remove_file(alias.join("stray.rs")).unwrap();
        }

        let _own = crate::config::tests::set_env_for_test("OUT_DIR", Some(own_out_dir.as_os_str()));
        assert_eq!(
            predicted_key_inputs(&args_b, &hasher),
            Err(Rejection::NoRecord),
            "a build with its own OUT_DIR has another identity"
        );
    }

    #[test]
    fn an_aliased_out_dir_takes_the_shared_row_without_a_macro_dependency() {
        an_aliased_out_dir_takes_the_shared_row(false);
    }

    #[test]
    fn an_aliased_out_dir_takes_the_shared_row_under_the_tree_guard() {
        an_aliased_out_dir_takes_the_shared_row(true);
    }

    /// What is written has to be exactly what comes back, or a prediction
    /// would reproduce a different `sources` group than the pre-pass did.
    #[test]
    fn input_prediction_round_trips_through_the_database() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.db");
        let dep_info = DepInfo {
            source_files: vec![
                PathBuf::from("/w/src/lib.rs"),
                PathBuf::from("/w/src/generated.rs"),
            ],
            env_deps: vec![("OUT_DIR".to_string(), "/target/build/out".to_string())],
        };

        let hasher = FileHasher::persistent(&db);
        assert!(hasher.supports_input_predictions());
        assert_eq!(
            hasher.input_prediction("unit"),
            None,
            "an identity never recorded has no prediction"
        );
        hasher.record_input_prediction("unit", Some("demo"), &dep_info, None);

        let record = FileHasher::persistent(&db)
            .input_prediction("unit")
            .expect("the recorded closure must survive a new process");
        assert_eq!(record.schema, PREDICTION_SCHEMA);
        assert_eq!(record.sources, dep_info.source_files);
        assert_eq!(record.env_deps, dep_info.env_deps);

        // Re-recording replaces rather than accumulates.
        let narrower = DepInfo {
            source_files: vec![PathBuf::from("/w/src/lib.rs")],
            env_deps: Vec::new(),
        };
        FileHasher::persistent(&db).record_input_prediction(
            "unit",
            Some("demo"),
            &narrower,
            Some("tree-digest".to_string()),
        );
        let record = FileHasher::persistent(&db)
            .input_prediction("unit")
            .unwrap();
        assert_eq!(record.sources, narrower.source_files);
        assert!(record.env_deps.is_empty());
        assert_eq!(record.tree.as_deref(), Some("tree-digest"));
    }

    /// The tree guard: a record for a proc-macro-dependent unit is only usable
    /// while the crate's files are exactly what they were, wherever the tree
    /// lives.
    #[test]
    fn crate_tree_digest_tracks_content_names_and_exclusions() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("registry/src/index-abc/pkg-1.0.0");
        std::fs::create_dir_all(root.join("src")).unwrap();
        std::fs::write(root.join("src/lib.rs"), "pub fn a() {}\n").unwrap();
        std::fs::write(root.join("Cargo.toml"), "[package]\n").unwrap();
        let hasher = FileHasher::new();
        // SAFETY: the process-state lock serialises environment edits.
        unsafe { std::env::set_var("CARGO_MANIFEST_DIR", &root) };
        unsafe { std::env::remove_var("OUT_DIR") };
        let baseline = crate_tree_digest(&hasher).unwrap();

        std::fs::write(root.join("src/lib.rs"), "pub fn b() {}\n").unwrap();
        assert_ne!(crate_tree_digest(&hasher).unwrap(), baseline, "content");
        std::fs::write(root.join("src/lib.rs"), "pub fn a() {}\n").unwrap();
        assert_eq!(
            crate_tree_digest(&hasher).unwrap(),
            baseline,
            "restored content"
        );

        std::fs::write(root.join("src/extra.txt"), "x").unwrap();
        assert_ne!(crate_tree_digest(&hasher).unwrap(), baseline, "a new file");
        std::fs::remove_file(root.join("src/extra.txt")).unwrap();

        std::fs::create_dir_all(root.join("target/debug")).unwrap();
        std::fs::write(root.join("target/debug/x"), "x").unwrap();
        std::fs::create_dir_all(root.join(".git")).unwrap();
        std::fs::write(root.join(".git/HEAD"), "ref").unwrap();
        assert_eq!(
            crate_tree_digest(&hasher).unwrap(),
            baseline,
            "build and git dirs"
        );

        // The same tree elsewhere digests the same: the guard follows content.
        let copy = dir.path().join("other/registry/src/index-def/pkg-1.0.0");
        std::fs::create_dir_all(copy.join("src")).unwrap();
        std::fs::write(copy.join("src/lib.rs"), "pub fn a() {}\n").unwrap();
        std::fs::write(copy.join("Cargo.toml"), "[package]\n").unwrap();
        unsafe { std::env::set_var("CARGO_MANIFEST_DIR", &copy) };
        assert_eq!(
            crate_tree_digest(&hasher).unwrap(),
            baseline,
            "relocated tree"
        );
        let workspace = dir.path().join("workspace/member");
        std::fs::create_dir_all(workspace.join("src")).unwrap();
        unsafe { std::env::set_var("CARGO_MANIFEST_DIR", &workspace) };
        assert!(
            crate_tree_digest(&hasher).is_none(),
            "a workspace crate keeps the pre-pass"
        );
        unsafe { std::env::remove_var("CARGO_MANIFEST_DIR") };
        assert!(
            crate_tree_digest(&hasher).is_none(),
            "no crate directory, no guard"
        );
    }

    #[test]
    fn emitted_closure_keeps_the_precompile_tree_guard() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let package = dir.path().join("registry/src/index/guarded-1.0.0");
        std::fs::create_dir_all(package.join("src")).unwrap();
        let source = package.join("src/lib.rs");
        std::fs::write(&source, "pub fn v() {}\n").unwrap();
        let macro_path = dir.path().join("libmacro.so");
        std::fs::write(&macro_path, "macro artifact").unwrap();
        let out = dir.path().join("target/debug/deps");
        std::fs::create_dir_all(&out).unwrap();
        let _manifest =
            crate::config::tests::set_env_for_test("CARGO_MANIFEST_DIR", Some(package.as_os_str()));
        let _out = crate::config::tests::set_env_for_test("OUT_DIR", None);
        let args = RustcArgs::parse(&[
            "rustc".to_string(),
            "--crate-name".to_string(),
            "guarded".to_string(),
            "--crate-type=lib".to_string(),
            "--emit=dep-info,metadata".to_string(),
            source.display().to_string(),
            "--out-dir".to_string(),
            out.display().to_string(),
            "--extern".to_string(),
            format!("my_macro={}", macro_path.display()),
        ])
        .unwrap();
        let db = dir.path().join("index.db");
        rusqlite::Connection::open(&db)
            .unwrap()
            .execute_batch(
                "CREATE TABLE entries (cache_key TEXT PRIMARY KEY, crate_name TEXT NOT NULL);",
            )
            .unwrap();
        let hasher = FileHasher::persistent(&db)
            .with_input_predictions(true)
            .with_prediction_flights(Some(dir.path().join("cache")));
        let original_tree = crate_tree_digest(&hasher).unwrap();
        set_defer_discovery(true);
        let deferred = compute_cache_key(&args, &hasher, &PathNormalizer::empty());
        set_defer_discovery(false);
        assert!(deferred.unwrap_err().is::<DeferredDiscovery>());
        // Let a broken handoff reach the pre-pass and fail, not self-deadlock.
        drop(hasher.take_discovery_flight());

        let closure = DepInfo {
            source_files: vec![source],
            env_deps: Vec::new(),
        };
        // A macro input changed during compilation. Recording the newer tree
        // would make the old emitted closure appear valid for those new files.
        std::fs::write(package.join("macro-input.txt"), "changed").unwrap();
        provide_dep_info(closure.clone());
        compute_cache_key(&args, &hasher, &PathNormalizer::empty()).unwrap();
        let tree = take_last_tree_digest();
        assert_eq!(tree.as_deref(), Some(original_tree.as_str()));
        assert!(take_last_tree_digest().is_none());
        let identity = rustc_prediction_identity(&args).unwrap();
        hasher.record_input_prediction(&identity, Some("guarded"), &closure, tree);
        assert_eq!(
            predicted_key_inputs(&args, &hasher),
            Err(Rejection::TreeChanged)
        );
        std::fs::remove_file(package.join("macro-input.txt")).unwrap();
        assert_eq!(predicted_key_inputs(&args, &hasher).unwrap(), closure);
        // An emitted closure without a prior guarded discovery must not
        // inherit the preceding invocation's tree.
        take_last_tree_digest();
        provide_dep_info(closure);
        compute_cache_key(&args, &hasher, &PathNormalizer::empty()).unwrap();
        assert!(take_last_tree_digest().is_none());
    }

    /// A row this build cannot vouch for reads as absent. The cost of that is
    /// one pre-pass; the cost of guessing at an unknown encoding is a key
    /// derived from someone else's rules.
    #[test]
    fn a_prediction_from_another_schema_is_not_read() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.db");
        let hasher = FileHasher::persistent(&db);
        let cache = hasher.cache.as_ref().unwrap();

        cache
            .put_input_prediction("future", PREDICTION_SCHEMA + 1, None, "{\"anything\":1}")
            .unwrap();
        assert_eq!(hasher.input_prediction("future"), None);

        // A row whose column says the right schema but whose bytes do not
        // decode is the same answer.
        cache
            .put_input_prediction("corrupt", PREDICTION_SCHEMA, None, "not json")
            .unwrap();
        assert_eq!(hasher.input_prediction("corrupt"), None);

        // And a row whose stored schema disagrees with its own payload.
        let stale = format!(
            "{{\"schema\":{},\"sources\":[],\"env_deps\":[]}}",
            PREDICTION_SCHEMA + 1
        );
        cache
            .put_input_prediction("mismatched", PREDICTION_SCHEMA, None, &stale)
            .unwrap();
        assert_eq!(hasher.input_prediction("mismatched"), None);
    }

    /// A hasher with no database has nowhere to keep a record. The daemon's
    /// store-free hasher is exactly this case, and it must not panic or
    /// pretend to have recorded anything.
    #[test]
    fn recording_without_a_database_is_a_no_op() {
        let hasher = FileHasher::new();
        assert!(!hasher.supports_input_predictions());
        hasher.record_input_prediction(
            "unit",
            None,
            &DepInfo {
                source_files: vec![PathBuf::from("/w/src/lib.rs")],
                env_deps: Vec::new(),
            },
            None,
        );
        assert_eq!(hasher.input_prediction("unit"), None);
    }

    #[test]
    fn cargo_cfg_pairs_filters_and_sorts() {
        use std::ffi::OsString;
        let pairs = cargo_cfg_pairs(
            [
                (OsString::from("CARGO_CFG_ZED"), OsString::from("1")),
                (OsString::from("PATH"), OsString::from("/usr/bin")),
                (OsString::from("CARGO_CFG_ABI"), OsString::from("eabi")),
                (OsString::from("CARGO_PKG_NAME"), OsString::from("x")),
            ]
            .into_iter(),
        );
        assert_eq!(
            pairs,
            vec![
                (OsString::from("CARGO_CFG_ABI"), OsString::from("eabi")),
                (OsString::from("CARGO_CFG_ZED"), OsString::from("1")),
            ]
        );
    }

    /// A non-UTF-8 variable anywhere in the environment used to panic the
    /// whole key computation via `std::env::vars()`; now the unrelated
    /// variable is filtered out and a `CARGO_CFG_*` pair survives with its
    /// exact bytes.
    #[cfg(unix)]
    #[test]
    fn cargo_cfg_pairs_tolerates_non_utf8_environments() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;
        let invalid = OsString::from_vec(vec![b'a', 0xff, b'b']);
        let pairs = cargo_cfg_pairs(
            [
                (OsString::from_vec(vec![0xff, 0xfe]), invalid.clone()),
                (OsString::from("CARGO_CFG_RAW"), invalid.clone()),
            ]
            .into_iter(),
        );
        assert_eq!(pairs, vec![(OsString::from("CARGO_CFG_RAW"), invalid)]);
    }

    /// Valid UTF-8 folds byte-identically to the old `vars()`-string
    /// hashing (no key change without a version bump); invalid sequences
    /// fold losslessly and distinctly instead of merging under U+FFFD.
    #[test]
    fn env_text_key_bytes_preserves_utf8_and_distinguishes_invalid() {
        use std::ffi::OsStr;
        assert_eq!(
            env_text_key_bytes(OsStr::new("target_os=\"linux\"")),
            b"target_os=\"linux\"".to_vec()
        );
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            let a = env_text_key_bytes(OsStr::from_bytes(&[b'x', 0xff]));
            let b = env_text_key_bytes(OsStr::from_bytes(&[b'x', 0xfe]));
            assert_ne!(a, b);
            // Tagged: cannot collide with any valid-UTF-8 value's bytes.
            assert_eq!(a[0], 0xff);
        }
    }

    /// The write persists where the read looks, creating the cache directory
    /// on a fresh machine. Skipped where that directory cannot be created
    /// (a sandboxed build with no writable home).
    #[test]
    fn tool_version_cache_write_is_read_back() {
        let cache_dir = crate::config::default_cache_dir();
        if std::fs::create_dir_all(&cache_dir).is_err()
            || tempfile::tempfile_in(&cache_dir).is_err()
        {
            eprintln!("skipping: {} is not writable", cache_dir.display());
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let binary = dir.path().join("rustc-probe");
        std::fs::write(&binary, b"not really rustc").unwrap();
        let prefix = "kache-test-version";
        let cache_file = tool_version_cache_path(&binary, prefix).unwrap();
        let _ = std::fs::remove_file(&cache_file);
        assert_eq!(read_tool_version_cache(&binary, prefix), None);

        write_tool_version_cache(&binary, prefix, "rustc 1.0.0 (test)");
        assert_eq!(
            read_tool_version_cache(&binary, prefix).as_deref(),
            Some("rustc 1.0.0 (test)")
        );
        assert_eq!(
            std::fs::read_to_string(&cache_file).unwrap(),
            "rustc 1.0.0 (test)",
            "the file is exactly the version string, as before"
        );
        let _ = std::fs::remove_file(&cache_file);
    }

    #[cfg(unix)]
    #[test]
    fn clippy_identity_follows_version_config_and_lint_arguments() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let driver = dir.path().join("clippy-driver");
        kache_fs::testutil::write_executable(
            &driver,
            "#!/bin/sh\necho 'clippy 0.1.98 (abc 2026-09-01)'\n",
        );
        let workspace = dir.path().join("ws");
        let member = workspace.join("member");
        std::fs::create_dir_all(&member).unwrap();
        let env = |vars: Vec<(&'static str, String)>| {
            move |name: &str| {
                vars.iter()
                    .find(|(n, _)| *n == name)
                    .map(|(_, v)| std::ffi::OsString::from(v))
            }
        };
        let manifest = vec![("CARGO_MANIFEST_DIR", member.display().to_string())];

        let bare = clippy_identity_in(&driver, env(manifest.clone()), None).unwrap();
        assert!(bare.starts_with("clippy 0.1.98"));
        assert!(bare.contains("config:none"));
        assert!(bare.contains("manifest:none"));

        // The package manifest feeds the `cargo` lint group.
        std::fs::write(member.join("Cargo.toml"), "[package]\nname = \"m\"\n").unwrap();
        let with_manifest = clippy_identity_in(&driver, env(manifest.clone()), None).unwrap();
        assert_ne!(with_manifest, bare);
        std::fs::write(
            member.join("Cargo.toml"),
            "[package]\nname = \"m\"\ndescription = \"d\"\n",
        )
        .unwrap();
        assert_ne!(
            clippy_identity_in(&driver, env(manifest.clone()), None).unwrap(),
            with_manifest
        );
        let bare = clippy_identity_in(&driver, env(manifest.clone()), None).unwrap();

        // A configuration file above the member is found and keyed by content.
        std::fs::write(workspace.join("clippy.toml"), "msrv = \"1.80\"\n").unwrap();
        let configured = clippy_identity_in(&driver, env(manifest.clone()), None).unwrap();
        assert_ne!(configured, bare);
        assert!(configured.contains("config:clippy.toml:"));
        std::fs::write(workspace.join("clippy.toml"), "msrv = \"1.85\"\n").unwrap();
        let edited = clippy_identity_in(&driver, env(manifest.clone()), None).unwrap();
        assert_ne!(edited, configured);

        // `CLIPPY_CONF_DIR` wins over the manifest directory, and the lint
        // arguments Cargo passes through the environment are part of it.
        let elsewhere = dir.path().join("conf");
        std::fs::create_dir_all(&elsewhere).unwrap();
        std::fs::write(elsewhere.join(".clippy.toml"), "").unwrap();
        let mut with_conf_dir = manifest.clone();
        with_conf_dir.push(("CLIPPY_CONF_DIR", elsewhere.display().to_string()));
        let redirected = clippy_identity_in(&driver, env(with_conf_dir.clone()), None).unwrap();
        assert!(redirected.contains("config:.clippy.toml:"));
        with_conf_dir.push(("CLIPPY_ARGS", "-Dclippy::all".to_string()));
        let with_args = clippy_identity_in(&driver, env(with_conf_dir), None).unwrap();
        assert_ne!(with_args, redirected);
        assert!(with_args.contains("CLIPPY_ARGS=-Dclippy::all"));
    }

    #[test]
    fn tool_version_cache_path_is_a_named_file_in_the_cache_dir() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let binary = dir.path().join("rustc");
        std::fs::write(&binary, b"test rustc").unwrap();

        let cache_file = tool_version_cache_path(&binary, "rustc-ver")
            .expect("a readable binary must produce a cache path");
        let cache_dir = crate::config::default_cache_dir();
        assert_eq!(cache_file.parent(), Some(cache_dir.as_path()));

        let file_name = cache_file
            .file_name()
            .expect("cache path must name a file")
            .to_string_lossy();
        let digest = file_name
            .strip_prefix("rustc-ver-")
            .and_then(|name| name.strip_suffix(".txt"))
            .expect("cache file must retain its prefix and extension");
        assert_eq!(digest.len(), 16, "cache file uses the short BLAKE3 digest");
        assert!(digest.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }

    #[test]
    #[ignore = "spawned by the explicit RUSTUP_HOME regression"]
    fn rustup_settings_path_explicit_home_fixture() {
        let expected_home = std::env::var_os("KACHE_TEST_RUSTUP_HOME")
            .map(std::path::PathBuf::from)
            .expect("fixture requires its isolated expected home");

        assert_eq!(
            rustup_settings_path(),
            Some(expected_home.join("settings.toml"))
        );
    }

    #[test]
    fn tool_version_cache_uses_settings_under_explicit_rustup_home() {
        let dir = tempfile::tempdir().unwrap();
        let output = std::process::Command::new(
            std::env::current_exe().expect("resolve cache-key test executable"),
        )
        .args([
            "--ignored",
            "--exact",
            "cache_key::tests::rustup_settings_path_explicit_home_fixture",
            "--test-threads=1",
        ])
        .env("RUSTUP_HOME", dir.path())
        .env("KACHE_TEST_RUSTUP_HOME", dir.path())
        .output()
        .expect("spawn isolated RUSTUP_HOME fixture");

        assert!(
            output.status.success(),
            "isolated RUSTUP_HOME fixture failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn apply_key_env_vars_keeps_distinct_path_values_distinct() {
        let _lock = key_test_lock();
        let base = "deadbeef".to_string();
        let patterns = vec!["KACHE_TEST_ROOT".to_string()];

        // Deliberately NOT path-normalized. A declared variable is an opaque
        // semantic input — a macro may paste its value straight into the code
        // it emits — so two checkout paths that would collapse to the same
        // `<BASE_DIR>` sentinel must still key apart. This costs cross-machine
        // hits for path-valued declarations and buys back the exact miscompile
        // the feature exists to prevent.
        let alice = {
            let _guard = ScopedEnv::set("KACHE_TEST_ROOT", "/home/alice/proj/src");
            apply_key_env_vars(base.clone(), &patterns, "crate")
        };
        let bob = {
            let _guard = ScopedEnv::set("KACHE_TEST_ROOT", "/srv/build/bob/checkout/src");
            apply_key_env_vars(base.clone(), &patterns, "crate")
        };
        assert_ne!(alice, bob, "declared env values must be folded exactly");
    }

    #[cfg(unix)]
    #[test]
    fn apply_key_env_vars_distinguishes_non_utf8_values() {
        use std::os::unix::ffi::OsStrExt;

        let _lock = key_test_lock();
        let base = "deadbeef".to_string();
        let patterns = vec!["KACHE_TEST_RAW".to_string()];

        // Both byte strings lossy-convert to the same U+FFFD text, and a proc
        // macro reading `var_os` can still tell them apart — so folding the
        // lossy form would be a wrong-hit path.
        let key_for = |bytes: &[u8]| {
            let previous = std::env::var_os("KACHE_TEST_RAW");
            // SAFETY: single-threaded test body under `key_test_lock`.
            unsafe { std::env::set_var("KACHE_TEST_RAW", std::ffi::OsStr::from_bytes(bytes)) };
            let key = apply_key_env_vars(base.clone(), &patterns, "crate");
            // SAFETY: as above.
            unsafe {
                match previous {
                    Some(value) => std::env::set_var("KACHE_TEST_RAW", value),
                    None => std::env::remove_var("KACHE_TEST_RAW"),
                }
            }
            key
        };
        assert_ne!(key_for(&[0xff]), key_for(&[0xfe]));
    }

    #[test]
    fn apply_key_salt_distinguishes_base_keys() {
        // The same salt over different base keys stays distinct (the
        // base is mixed into the hash, not just the salt).
        let salt = Some("nix-rev-abc");
        assert_ne!(
            apply_key_salt("aaaa".to_string(), salt, "crate"),
            apply_key_salt("bbbb".to_string(), salt, "crate"),
        );
    }

    #[test]
    fn source_scanner_detects_runtime_env_use_and_skips_literals() {
        use super::SourceEnvDepUse::{IncludeLocator, RuntimeValue, Unused};
        use super::source_env_dep_use as scan;

        // A bare runtime env! use is a real dependency.
        assert_eq!(
            scan(r#"const X: &str = env!("MYVAR");"#, "MYVAR"),
            RuntimeValue
        );
        assert_eq!(
            scan(r#"let v = option_env!("MYVAR");"#, "MYVAR"),
            RuntimeValue
        );
        assert_eq!(
            scan(r#"const N: usize = env!("MYVAR").len();"#, "MYVAR"),
            RuntimeValue
        );
        assert_eq!(
            scan(r#"const P: usize = env!("MYVAR").len() % 2;"#, "MYVAR"),
            RuntimeValue
        );
        // A different var name doesn't match, and proves nothing.
        assert_eq!(scan(r#"env!("OTHER")"#, "MYVAR"), Unused);
        assert_eq!(
            scan(r#"include!(concat!(env!("OTHER"), "/gen.rs"));"#, "MYVAR"),
            Unused
        );
        assert_eq!(scan("pub fn f() {}", "MYVAR"), Unused);

        // env! nested inside include!(concat!(...)) is a compile-time include,
        // not a runtime value, and is the positive proof normalization needs.
        assert_eq!(
            scan(r#"include!(concat!(env!("MYVAR"), "/gen.rs"));"#, "MYVAR"),
            IncludeLocator
        );
        assert_eq!(
            scan(r#"include_bytes![env!("MYVAR")];"#, "MYVAR"),
            IncludeLocator
        );
        assert_eq!(
            scan(r#"include_str! { env! { "MYVAR" } }"#, "MYVAR"),
            IncludeLocator
        );
        // The include context ends at the include's own closing delimiter.
        assert_eq!(
            scan(
                r#"include!{ concat!(env!("MYVAR"), "/gen.rs") } const X: &str = env!("MYVAR");"#,
                "MYVAR"
            ),
            RuntimeValue
        );
        assert_eq!(
            scan(r#"f(include!(env!("MYVAR"))); [env!("MYVAR")];"#, "MYVAR"),
            RuntimeValue
        );

        // A name the scanner cannot read counts against every var outside an
        // include, and proves nothing inside one.
        assert_eq!(
            scan(r#"const S: &str = env!(concat!("MY", "VAR"));"#, "MYVAR"),
            RuntimeValue
        );
        assert_eq!(
            scan(r#"const S: &str = env!(concat!("OT", "HER"));"#, "MYVAR"),
            RuntimeValue
        );
        assert_eq!(
            scan(
                r#"macro_rules! e { ($v:literal) => { env!($v) } } const X: &str = e!("MYVAR");"#,
                "MYVAR"
            ),
            RuntimeValue
        );
        assert_eq!(scan(r#"env!("MY\x56AR")"#, "MYVAR"), RuntimeValue);
        assert_eq!(scan(r##"env!(r#"MYVAR"#)"##, "MYVAR"), RuntimeValue);
        assert_eq!(scan(r#"env!("MYVAR"#, "MYVAR"), RuntimeValue);
        assert_eq!(scan("env!(", "MYVAR"), RuntimeValue);
        assert_eq!(
            scan(
                r#"include!(concat!(env!(concat!("MY", "VAR")), "/gen.rs"));"#,
                "MYVAR"
            ),
            Unused
        );

        // Whitespace and comments between the macro tokens do not hide a use.
        assert_eq!(
            scan(
                r#"env /* c */ ! // c
            ( "MYVAR" )"#,
                "MYVAR"
            ),
            RuntimeValue
        );
        // `!` that is not a macro bang opens no macro group.
        assert_eq!(
            scan(r#"if a != (b) { include!(env!("MYVAR")); }"#, "MYVAR"),
            IncludeLocator
        );

        // Occurrences inside string / char / raw-string literals and comments
        // are not real uses — the scanner must skip them.
        assert_eq!(scan(r#"let s = "env!(\"MYVAR\")";"#, "MYVAR"), Unused);
        assert_eq!(scan(r###"let s = r#"env!("MYVAR")"#;"###, "MYVAR"), Unused);
        assert_eq!(scan(r###"let s = br#"env!("MYVAR")"#;"###, "MYVAR"), Unused);
        assert_eq!(scan(r###"let s = cr#"env!("MYVAR")"#;"###, "MYVAR"), Unused);
        assert_eq!(scan(r#"// env!("MYVAR")"#, "MYVAR"), Unused);
        assert_eq!(scan(r#"/* env!("MYVAR") */"#, "MYVAR"), Unused);
        assert_eq!(scan(r#"/* /* */ env!("MYVAR") */"#, "MYVAR"), Unused);
        assert_eq!(scan(r#"/* unterminated env!("MYVAR")"#, "MYVAR"), Unused);

        // Nested block comments end at the matching `*/` only.
        assert_eq!(
            scan(
                r#"/* /* */ include!( */ const X: &str = env!("MYVAR");"#,
                "MYVAR"
            ),
            RuntimeValue
        );
        assert_eq!(scan(r#"/*/ env!("MYVAR") */"#, "MYVAR"), Unused);

        // A char literal earlier in the line must not derail scanning of a real
        // use that follows it.
        assert_eq!(
            scan(r#"let c = '"'; let x = env!("MYVAR");"#, "MYVAR"),
            RuntimeValue
        );
        assert_eq!(
            scan(r#"let c = '\''; let x = env!("MYVAR");"#, "MYVAR"),
            RuntimeValue
        );
        // An escape longer than one character, and a char literal whose
        // closing quote is the byte before another quote: both decide where
        // the literal ends, and ending it in the wrong place swallows the
        // code after it.
        assert_eq!(
            scan(r#"let c = '\u{41}'; let x = env!("MYVAR");"#, "MYVAR"),
            RuntimeValue
        );
        assert_eq!(
            scan(r#"let c = '\x41'; let x = env!("MYVAR");"#, "MYVAR"),
            RuntimeValue
        );
        assert_eq!(
            scan(r#"let v = ['\n','"']; env!("MYVAR");"#, "MYVAR"),
            RuntimeValue
        );
        assert_eq!(
            scan(r#"let c = b'\\'; let x = env!("MYVAR");"#, "MYVAR"),
            RuntimeValue
        );
        assert_eq!(
            scan(r#"let c = 'é'; let x = env!("MYVAR");"#, "MYVAR"),
            RuntimeValue
        );
        assert_eq!(
            scan(r#"let c = '"'; let s = "env!(\"MYVAR\")";"#, "MYVAR"),
            Unused
        );
        // A lifetime or label is not a char literal: skipping to the next quote
        // would hide the use.
        assert_eq!(
            scan(
                r#"fn f(_: &'static str) -> usize { env!("MYVAR").len() }"#,
                "MYVAR"
            ),
            RuntimeValue
        );
        assert_eq!(
            scan(
                r#"fn f<'a>(_: &'a str) { 'outer: loop { env!("MYVAR"); } }"#,
                "MYVAR"
            ),
            RuntimeValue
        );
        assert_eq!(scan("'", "MYVAR"), Unused);
        // A real use after a raw string is still found (exercises skip_raw_string).
        assert_eq!(
            scan(r###"let r = r#"noise"#; env!("MYVAR")"###, "MYVAR"),
            RuntimeValue
        );
        // A raw C string ending in a backslash has no escape to swallow the
        // closing quote.
        assert_eq!(
            scan(
                r#"let c = cr"\"; let x = env!("MYVAR"); let t = "";"#,
                "MYVAR"
            ),
            RuntimeValue
        );
    }

    #[test]
    fn source_scanner_tokenizes_operators_numbers_and_idents_like_rustc() {
        use super::SourceEnvDepUse::{IncludeLocator, RuntimeValue, Unused};
        use super::source_env_dep_use as scan;

        // A division is not a comment.
        assert_eq!(
            scan(r#"let q = a / b; let x = env!("MYVAR");"#, "MYVAR"),
            RuntimeValue
        );
        // Nor between macro tokens: `env / 2 */ !(..)` is no macro call.
        assert_eq!(scan(r#"let q = env / 2 */ !("MYVAR");"#, "MYVAR"), Unused);
        // An identifier that merely starts like a raw string prefix.
        assert_eq!(scan(r#"rinclude!(env!("MYVAR"))"#, "MYVAR"), RuntimeValue);
        // A variable named `env` compared with `!=` opens no macro.
        assert_eq!(scan("fn f(env: u8) -> bool { env != 0 }", "MYVAR"), Unused);
        // Plain groups inside an include keep the include context open.
        assert_eq!(
            scan(r#"include!(concat!(("a"), ("b"), env!("MYVAR")))"#, "MYVAR"),
            IncludeLocator
        );
        assert_eq!(
            scan(r#"include!(m!('a'('b')), env!("MYVAR"))"#, "MYVAR"),
            IncludeLocator
        );
        // Closing an include leaves its context; nested includes count down.
        assert_eq!(
            scan(r#"include!(()) const X: &str = env!("MYVAR");"#, "MYVAR"),
            RuntimeValue
        );
        assert_eq!(
            scan(r#"include!(include!("a"), env!("MYVAR"))"#, "MYVAR"),
            IncludeLocator
        );

        // Char literals: escaped quote, non-ASCII, and a lone quote at EOF.
        assert_eq!(
            scan(r#"let c = '\"'; let x = env!("MYVAR");"#, "MYVAR"),
            RuntimeValue
        );
        assert_eq!(
            scan(r#"let c = ['é','"']; let x = env!("MYVAR");"#, "MYVAR"),
            RuntimeValue
        );
        assert_eq!(scan("let s = '", "MYVAR"), Unused);

        // A number's suffix cannot start a raw string.
        assert_eq!(
            scan(
                r##"m!{ 1r#"x" } const P: &str = env!("MYVAR"); // "#"##,
                "MYVAR"
            ),
            RuntimeValue
        );
        // Non-ASCII identifier bytes belong to the identifier.
        assert_eq!(scan(r#"éinclude!(env!("MYVAR"))"#, "MYVAR"), RuntimeValue);
        assert_eq!(scan(r#"include!(envé!("MYVAR"))"#, "MYVAR"), Unused);
        // Rust whitespace beyond ASCII space separates tokens.
        for space in [
            "\x0B", "\x0C", "\u{85}", "\u{200E}", "\u{200F}", "\u{2028}", "\u{2029}",
        ] {
            assert_eq!(
                scan(&format!("env{space}!{space}(\"MYVAR\")"), "MYVAR"),
                RuntimeValue,
                "{space:?}"
            );
            assert_eq!(
                scan(&format!("{space}env!(\"MYVAR\")"), "MYVAR"),
                RuntimeValue,
                "leading {space:?}"
            );
        }
        // U+00A9 shares U+0085's lead byte but is not whitespace.
        assert_eq!(scan("env\u{A9}!(\"MYVAR\")", "MYVAR"), Unused);
        // A byte-order mark does not glue to the identifier after it.
        assert_eq!(scan("\u{FEFF}env!(\"MYVAR\")", "MYVAR"), RuntimeValue);

        // A file that ends inside an identifier or a number has no byte after
        // it, and a skipped token never hides the use that follows it.
        assert_eq!(scan("let n = 1", "MYVAR"), Unused);
        assert_eq!(
            scan(r#"const X: &str = env!("MYVAR"); mod tail"#, "MYVAR"),
            RuntimeValue
        );
        assert_eq!(scan(r#"let s = "x"; env!("MYVAR")"#, "MYVAR"), RuntimeValue);
        assert_eq!(
            scan(r#"let s = "a\"b"; env!("MYVAR")"#, "MYVAR"),
            RuntimeValue
        );
        assert_eq!(scan("// c\nenv!(\"MYVAR\")", "MYVAR"), RuntimeValue);
        assert_eq!(scan("/* c */ env!(\"MYVAR\")", "MYVAR"), RuntimeValue);
        assert_eq!(scan("/* /* c */ */ env!(\"MYVAR\")", "MYVAR"), RuntimeValue);
        assert_eq!(
            scan(r###"let r = r##"x"##; env!("MYVAR")"###, "MYVAR"),
            RuntimeValue
        );

        // Identifiers and numbers are consumed from their first byte, so a
        // one-byte token keeps its boundary.
        assert_eq!(scan(r#"m!(env!("MYVAR"))"#, "MYVAR"), RuntimeValue);
        assert_eq!(scan(r#"e!("MYVAR")"#, "MYVAR"), Unused);
        assert_eq!(scan(r#"let n = 1; env!("MYVAR");"#, "MYVAR"), RuntimeValue);
        assert_eq!(
            scan(r#"include!(1, env!("MYVAR"))"#, "MYVAR"),
            IncludeLocator
        );
    }

    #[test]
    fn unescape_env_dep_value_undoes_rustc_escaping() {
        // rustc's `escape_dep_env`: `\`→`\\`, newline→`\n`, CR→`\r`.
        // A Windows OUT_DIR arrives doubled; unescaping restores the
        // single-backslash path so the normalizer's rules can match it.
        assert_eq!(
            unescape_env_dep_value(r"C:\\actions-runner\\proj\\target\\out"),
            r"C:\actions-runner\proj\target\out"
        );
        assert_eq!(unescape_env_dep_value(r"a\nb\rc"), "a\nb\rc");
        // Forward-slash / plain values (the Unix case) are untouched.
        assert_eq!(
            unescape_env_dep_value("/home/u/proj/out"),
            "/home/u/proj/out"
        );
        assert_eq!(unescape_env_dep_value("plain-value"), "plain-value");
    }

    #[test]
    fn parse_env_dep_info_unescapes_windows_paths() {
        let dep = "# env-dep:OUT_DIR=C:\\\\proj\\\\build\\\\out\n";
        let deps = parse_env_dep_info(dep);
        assert_eq!(
            deps,
            vec![("OUT_DIR".to_string(), r"C:\proj\build\out".to_string())]
        );
    }

    #[test]
    fn test_cache_key_deterministic() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let args_vec: Vec<String> = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "mylib".to_string(),
            source.to_string_lossy().to_string(),
            "--crate-type".to_string(),
            "lib".to_string(),
            "--edition=2021".to_string(),
            "-C".to_string(),
            "opt-level=2".to_string(),
        ];

        let parsed1 = RustcArgs::parse(&args_vec).unwrap();
        let parsed2 = RustcArgs::parse(&args_vec).unwrap();

        let fh = FileHasher::new();
        let pn = PathNormalizer::empty();
        let key1 = compute_cache_key(&parsed1, &fh, &pn).unwrap();
        let key2 = compute_cache_key(&parsed2, &fh, &pn).unwrap();
        assert_eq!(key1, key2);
    }

    /// Regression for Finding B from the Firefox bench: mozbuild sets
    /// `-Clinker=/abs/path/to/clang++`, and v6 baked that path into the
    /// key — every clone hashed differently. The linker's semantic
    /// identity is still captured via `linker:<--version>` so the key
    /// stays sensitive to a different toolchain.
    #[test]
    fn cache_key_ignores_linker_path() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let mk = |linker: &str| -> Vec<String> {
            vec![
                "rustc".to_string(),
                "--crate-name".to_string(),
                "mylib".to_string(),
                source.to_string_lossy().to_string(),
                "--crate-type".to_string(),
                "lib".to_string(),
                "-C".to_string(),
                format!("linker={linker}"),
            ]
        };

        let fh = FileHasher::new();
        let pn = PathNormalizer::empty();
        let a = compute_cache_key(
            &RustcArgs::parse(&mk("/Users/alice/clang++")).unwrap(),
            &fh,
            &pn,
        )
        .unwrap();
        let b = compute_cache_key(
            &RustcArgs::parse(&mk("/home/runner/clang++")).unwrap(),
            &fh,
            &pn,
        )
        .unwrap();
        assert_eq!(a, b, "linker path must not affect the cache key");
    }

    /// Shared base argv for the flag-keying regression tests below.
    fn flag_base(source: &Path, extra: &[&str]) -> Vec<String> {
        let mut v = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "mylib".to_string(),
            source.to_string_lossy().to_string(),
            "--crate-type".to_string(),
            "cdylib".to_string(),
        ];
        v.extend(extra.iter().map(|s| s.to_string()));
        v
    }

    fn key_of(args: &[String]) -> String {
        let fh = FileHasher::new();
        let pn = PathNormalizer::empty();
        compute_cache_key(&RustcArgs::parse(args).unwrap(), &fh, &pn).unwrap()
    }

    /// Compute a key for tests that check whether a *flag* (sysroot, custom
    /// target spec, `-Z` codegen flag, cross `--target`, ...) affects the key,
    /// independent of source discovery. Such flags can make the real
    /// `--emit=dep-info` pre-pass fail (a bogus `--sysroot` where rustc can't
    /// find `std`, a `-Z` flag on a stable toolchain, an uninstalled
    /// cross-target's missing `std`); since kunobi-ninja/kache#323 a failing
    /// pre-pass is a hard error (the invocation becomes non-cacheable), so this
    /// clears `source_file` to exercise flag hashing directly.
    fn key_of_flags(args: &[String]) -> String {
        let fh = FileHasher::new();
        let pn = PathNormalizer::empty();
        let mut parsed = RustcArgs::parse(args).unwrap();
        parsed.source_file = None;
        compute_cache_key(&parsed, &fh, &pn).unwrap()
    }

    /// H1: build-script `-l` link libs reach rustc on argv (not via
    /// RUSTFLAGS); a different native lib must diverge the key.
    /// Generic `-l` keying, checked on hosts that do not probe native MSVC
    /// inputs. On a Windows host the library must exist (fail closed), so the
    /// Windows shape lives in the `windows_*` tests with an injected probe.
    #[cfg(not(windows))]
    #[test]
    fn link_lib_changes_key() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let none = key_of(&flag_base(&source, &[]));
        let ssl = key_of(&flag_base(&source, &["-l", "ssl"]));
        let crypto = key_of(&flag_base(&source, &["-l", "crypto"]));
        assert_ne!(none, ssl, "adding -l must change the key");
        assert_ne!(ssl, crypto, "a different -l lib must change the key");
        // Attached form parses identically to the separate form.
        assert_eq!(ssl, key_of(&flag_base(&source, &["-lssl"])));
    }

    #[test]
    fn static_lib_spec_models_kind_modifiers_and_rename() {
        let archive = |files: &[&str], bundle, whole_archive| StaticLibSpec::Archive {
            files: files.iter().map(|file| file.to_string()).collect(),
            bundle,
            whole_archive,
        };
        let plain = || archive(&["libfoo.a", "foo.lib"], true, false);
        let whole = || archive(&["libfoo.a", "foo.lib"], true, true);
        assert_eq!(static_lib_spec("static=foo"), plain());
        // Modifiers that change how the archive links, not which file it is.
        assert_eq!(static_lib_spec("static:+whole-archive=foo"), whole());
        // The last `±whole-archive` wins.
        assert_eq!(
            static_lib_spec("static:+whole-archive,-whole-archive=foo"),
            plain()
        );
        assert_eq!(
            static_lib_spec("static:-bundle=foo"),
            archive(&["libfoo.a", "foo.lib"], false, false)
        );
        assert_eq!(static_lib_spec("static:+bundle,-as-needed=foo"), plain());
        // The last `±bundle` wins.
        assert_eq!(static_lib_spec("static:-bundle,+bundle=foo"), plain());
        // `+verbatim` names the file exactly; a later `-verbatim` undoes it.
        assert_eq!(
            static_lib_spec("static:+whole-archive,+verbatim=foo.a"),
            archive(&["foo.a"], true, true)
        );
        assert_eq!(static_lib_spec("static:+verbatim,-verbatim=foo"), plain());
        // Rename, unknown modifiers and an empty name are not modelled.
        assert_eq!(
            static_lib_spec("static=foo:bar"),
            StaticLibSpec::Unmodeled("static=foo:bar")
        );
        assert_eq!(
            static_lib_spec("static:+link-arg=foo"),
            StaticLibSpec::Unmodeled("static:+link-arg=foo")
        );
        assert_eq!(
            static_lib_spec("static="),
            StaticLibSpec::Unmodeled("static=")
        );
        // Other kinds are referenced, not bundled.
        assert_eq!(static_lib_spec("dylib=foo"), StaticLibSpec::NotStatic);
        assert_eq!(
            static_lib_spec("dylib:+verbatim=foo"),
            StaticLibSpec::NotStatic
        );
        assert_eq!(static_lib_spec("foo"), StaticLibSpec::NotStatic);
        // A kindless or `dylib` rename can retarget an attribute's static
        // library, so it is refused like a static one. Frameworks cannot.
        assert_eq!(
            static_lib_spec("foo:bar"),
            StaticLibSpec::Unmodeled("foo:bar")
        );
        assert_eq!(
            static_lib_spec("dylib=foo:bar"),
            StaticLibSpec::Unmodeled("dylib=foo:bar")
        );
        assert_eq!(
            static_lib_spec("framework=Foo:Bar"),
            StaticLibSpec::NotStatic
        );
    }

    #[test]
    fn resolve_native_static_lib_hashes_only_unambiguous_static_archives() {
        let fh = FileHasher::new();
        let dir = tempfile::tempdir().unwrap();
        let lib = dir.path().join("libfoo.a");
        std::fs::write(&lib, b"v1 archive bytes").unwrap();
        let dirs = vec![dir.path().to_path_buf()];

        // A `static=` lib present in a search dir resolves and content-hashes.
        let (path, h1) =
            resolve_native_static_lib("static=foo", &dirs, &fh, false, |_| StaticLibUse::Bundled)
                .unwrap()
                .expect("static lib in a search dir must resolve");
        assert_eq!(path, lib);

        // `cc` emits the same OUT_DIR once per compiled archive. Repeating an
        // identical `-L native=...` must still resolve the one physical file.
        let duplicate_dirs = vec![dir.path().to_path_buf(), dir.path().to_path_buf()];
        let (duplicate_path, _) =
            resolve_native_static_lib("static=foo", &duplicate_dirs, &fh, false, |_| {
                StaticLibUse::Bundled
            })
            .unwrap()
            .expect("duplicate search dirs must not make one archive ambiguous");
        assert_eq!(duplicate_path, lib);

        // Changed bytes → different hash (this is the false hit we close).
        std::fs::write(&lib, b"v2 different bytes").unwrap();
        let (_, h2) =
            resolve_native_static_lib("static=foo", &dirs, &fh, false, |_| StaticLibUse::Bundled)
                .unwrap()
                .unwrap();
        assert_ne!(h1, h2, "content change must change the resolved hash");

        // `dylib=`/bare are referenced not bundled → never content-hashed, and
        // a missing lib does not resolve.
        assert!(
            resolve_native_static_lib("dylib=foo", &dirs, &fh, false, |_| StaticLibUse::Bundled)
                .unwrap()
                .is_none()
        );
        assert!(
            resolve_native_static_lib("foo", &dirs, &fh, false, |_| StaticLibUse::Bundled)
                .unwrap()
                .is_none()
        );
        assert!(
            resolve_native_static_lib("static=absent", &dirs, &fh, false, |_| {
                StaticLibUse::Bundled
            })
            .unwrap()
            .is_none()
        );

        // Distinct matches remain uncacheable, so we never hash a file other
        // than the one rustc actually picked.
        let other_dir = tempfile::tempdir().unwrap();
        std::fs::write(other_dir.path().join("libfoo.a"), b"different archive").unwrap();
        assert!(
            resolve_native_static_lib(
                "static=foo",
                &[dir.path().to_path_buf(), other_dir.path().to_path_buf()],
                &fh,
                false,
                |_| StaticLibUse::Bundled,
            )
            .is_err(),
            "distinct search-dir matches must fail closed"
        );

        // Both platform filename conventions in one directory are also
        // ambiguous.
        std::fs::write(dir.path().join("foo.lib"), b"msvc import lib").unwrap();
        assert!(
            resolve_native_static_lib("static=foo", &dirs, &fh, false, |_| StaticLibUse::Bundled)
                .is_err(),
            "ambiguous .a/.lib match must fail closed"
        );
    }

    #[test]
    fn resolve_native_static_lib_hashes_archives_named_with_modifiers() {
        let fh = FileHasher::new();
        let dir = tempfile::tempdir().unwrap();
        let dirs = vec![dir.path().to_path_buf()];
        let lib = dir.path().join("libfoo.a");
        std::fs::write(&lib, b"v1 archive bytes").unwrap();

        // `cargo:rustc-link-lib=static:+whole-archive=foo` bundles libfoo.a
        // just like `static=foo`, so a rebuilt archive must change the hash.
        let hash_of = |spec| {
            resolve_native_static_lib(spec, &dirs, &fh, false, |_| StaticLibUse::Bundled)
                .unwrap()
                .expect("modifier spec must resolve its archive")
        };
        let (path, h1) = hash_of("static:+whole-archive=foo");
        assert_eq!(path, lib);
        std::fs::write(&lib, b"v2 different bytes").unwrap();
        let (_, h2) = hash_of("static:+whole-archive=foo");
        assert_ne!(h1, h2, "rebuilt whole-archive lib must change the hash");

        // `+verbatim` resolves the exact file name and nothing else.
        std::fs::write(dir.path().join("foo.a"), b"verbatim bytes").unwrap();
        let (verbatim, _) = hash_of("static:+verbatim=foo.a");
        assert_eq!(verbatim, dir.path().join("foo.a"));

        // A rename is refused rather than keyed by its name.
        assert!(
            resolve_native_static_lib("static=foo:bar", &dirs, &fh, false, |_| {
                StaticLibUse::Bundled
            })
            .is_err()
        );
    }

    #[test]
    fn native_linker_side_files_fail_closed() {
        let parsed = RustcArgs::parse(&[
            "rustc".to_string(),
            "src/lib.rs".to_string(),
            "-C".to_string(),
            "link-arg=-Wl,-order_file,/tmp/order.txt".to_string(),
        ])
        .unwrap();
        assert!(native_linker_side_files_are_unmodeled(&parsed));

        let response_file = RustcArgs::parse(&[
            "rustc".to_string(),
            "src/lib.rs".to_string(),
            "-Clink-arg=@/tmp/ld.rsp".to_string(),
        ])
        .unwrap();
        assert!(native_linker_side_files_are_unmodeled(&response_file));

        for apple_dynamic_path in [
            "link-arg=-Wl,-rpath,@loader_path",
            "link-arg=-Wl,-rpath,@loader_path/../lib",
            "link-arg=-Wl,-rpath,@rpath",
            "link-arg=-Wl,-install_name,@executable_path",
            "link-arg=-Wl,-install_name,@executable_path/lib/libfoo.dylib",
        ] {
            let parsed = RustcArgs::parse(&[
                "rustc".to_string(),
                "src/lib.rs".to_string(),
                "-C".to_string(),
                apple_dynamic_path.to_string(),
                "--target=aarch64-apple-darwin".to_string(),
            ])
            .unwrap();
            assert!(
                !native_linker_side_files_are_unmodeled(&parsed),
                "Apple dynamic path is not a response file: {apple_dynamic_path}"
            );
        }

        let forwarded_response_file = RustcArgs::parse(&[
            "rustc".to_string(),
            "src/lib.rs".to_string(),
            "-C".to_string(),
            "link-arg=-Wl,@/tmp/ld.rsp".to_string(),
        ])
        .unwrap();
        assert!(native_linker_side_files_are_unmodeled(
            &forwarded_response_file
        ));

        for long_codegen_response_file in [
            "--codegen=link-arg=@/tmp/ld.rsp",
            "--codegen=link-args=-Wl,@/tmp/ld.rsp",
        ] {
            let parsed = RustcArgs::parse(&[
                "rustc".to_string(),
                "src/lib.rs".to_string(),
                long_codegen_response_file.to_string(),
            ])
            .unwrap();
            assert!(
                native_linker_side_files_are_unmodeled(&parsed),
                "long codegen response file must fail closed: {long_codegen_response_file}"
            );
        }

        let non_apple_at_rpath = RustcArgs::parse(&[
            "rustc".to_string(),
            "src/lib.rs".to_string(),
            "-Clink-arg=-Wl,@rpath".to_string(),
            "--target=x86_64-unknown-linux-gnu".to_string(),
        ])
        .unwrap();
        assert!(
            native_linker_side_files_are_unmodeled(&non_apple_at_rpath),
            "Apple dynamic-token exemptions must not hide non-Apple response files"
        );

        let custom_apple_named_target = RustcArgs::parse(&[
            "rustc".to_string(),
            "src/lib.rs".to_string(),
            "-Clink-arg=-Wl,@rpath".to_string(),
            "--target=/tmp/aarch64-apple-darwin.json".to_string(),
        ])
        .unwrap();
        assert!(
            native_linker_side_files_are_unmodeled(&custom_apple_named_target),
            "an Apple-looking custom target is not proof of Apple linker semantics"
        );

        let rust_target_path_name = RustcArgs::parse(&[
            "rustc".to_string(),
            "src/lib.rs".to_string(),
            "-Clink-arg=-Wl,@rpath".to_string(),
            "--target=aarch64-apple-custom".to_string(),
        ])
        .unwrap();
        assert!(
            native_linker_side_files_are_unmodeled(&rust_target_path_name),
            "a RUST_TARGET_PATH name is not proof of built-in Apple semantics"
        );

        let map_file = RustcArgs::parse(&[
            "rustc".to_string(),
            "src/lib.rs".to_string(),
            "-Clink-arg=-Wl,-map,/tmp/link.map".to_string(),
        ])
        .unwrap();
        assert!(native_linker_side_files_are_unmodeled(&map_file));

        let lld_map_file = RustcArgs::parse(&[
            "rustc".to_string(),
            "src/lib.rs".to_string(),
            "--codegen=link-arg=-Wl,--Map=/tmp/link.map".to_string(),
        ])
        .unwrap();
        assert!(native_linker_side_files_are_unmodeled(&lld_map_file));

        let coff_map_file = RustcArgs::parse(&[
            "rustc".to_string(),
            "src/lib.rs".to_string(),
            r"--codegen=link-arg=/MAP:C:\tmp\link.map".to_string(),
        ])
        .unwrap();
        assert!(native_linker_side_files_are_unmodeled(&coff_map_file));

        for ordering_file in [
            "--codegen=link-arg=-Wl,--symbol-ordering-file=/tmp/order.txt",
            r"--codegen=link-arg=/call-graph-ordering-file:C:\tmp\order.txt",
            r"--codegen=link-arg=/ORDER:@C:\tmp\order.txt",
        ] {
            let parsed = RustcArgs::parse(&[
                "rustc".to_string(),
                "src/lib.rs".to_string(),
                ordering_file.to_string(),
            ])
            .unwrap();
            assert!(
                native_linker_side_files_are_unmodeled(&parsed),
                "linker ordering files must fail closed: {ordering_file}"
            );
        }

        let ordinary = RustcArgs::parse(&[
            "rustc".to_string(),
            "src/lib.rs".to_string(),
            "-Copt-level=2".to_string(),
        ])
        .unwrap();
        assert!(!native_linker_side_files_are_unmodeled(&ordinary));

        let fh = FileHasher::new();
        let dir = tempfile::tempdir().unwrap();
        let lib = dir.path().join("libfoo.a");
        std::fs::write(&lib, gnu_ar_one_object(b"object")).unwrap();
        // Exact member identity is retained even without a side-file option,
        // because this archive may be bundled into an rlib and linked later.
        std::fs::write(&lib, gnu_ar_named_object("foo.o", b"same object")).unwrap();
        let named_foo = fh.hash_static_lib(&lib).unwrap();
        std::fs::write(&lib, gnu_ar_named_object("bar.o", b"same object")).unwrap();
        let named_bar = fh.hash_static_lib(&lib).unwrap();
        assert_ne!(named_foo, named_bar);

        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn probe() {}").unwrap();
        let guarded = RustcArgs::parse(&flag_base(
            &source,
            &[
                "-l",
                "static=foo",
                "-C",
                "link-arg=-Wl,-order_file,/tmp/order.txt",
            ],
        ))
        .unwrap();
        let error = compute_cache_key(&guarded, &fh, &PathNormalizer::empty()).unwrap_err();
        assert!(error.to_string().contains("side files are not cacheable"));
    }

    fn elf64le_relocatable(payload: &[u8]) -> Vec<u8> {
        let mut object = vec![0_u8; 64];
        object[..4].copy_from_slice(b"\x7fELF");
        object[4] = 2; // ELFCLASS64
        object[5] = 1; // ELFDATA2LSB
        object[6] = 1; // EV_CURRENT
        object[16..18].copy_from_slice(&1_u16.to_le_bytes()); // ET_REL
        object[18..20].copy_from_slice(&62_u16.to_le_bytes()); // EM_X86_64
        object[20..24].copy_from_slice(&1_u32.to_le_bytes());
        object[52..54].copy_from_slice(&64_u16.to_le_bytes());
        object[58..60].copy_from_slice(&64_u16.to_le_bytes());
        object[60..62].copy_from_slice(&3_u16.to_le_bytes());
        object[62..64].copy_from_slice(&2_u16.to_le_bytes());

        let payload_offset = object.len();
        object.extend_from_slice(payload);
        let names_offset = object.len();
        let names = b"\0.data\0.shstrtab\0";
        object.extend_from_slice(names);
        while !object.len().is_multiple_of(8) {
            object.push(0);
        }
        let section_offset = object.len();
        object.resize(section_offset + 3 * 64, 0);
        object[40..48].copy_from_slice(&(section_offset as u64).to_le_bytes());

        let payload_header = section_offset + 64;
        object[payload_header..payload_header + 4].copy_from_slice(&1_u32.to_le_bytes());
        object[payload_header + 4..payload_header + 8].copy_from_slice(&1_u32.to_le_bytes());
        object[payload_header + 24..payload_header + 32]
            .copy_from_slice(&(payload_offset as u64).to_le_bytes());
        object[payload_header + 32..payload_header + 40]
            .copy_from_slice(&(payload.len() as u64).to_le_bytes());
        object[payload_header + 48..payload_header + 56].copy_from_slice(&1_u64.to_le_bytes());

        let names_header = section_offset + 2 * 64;
        object[names_header..names_header + 4].copy_from_slice(&7_u32.to_le_bytes());
        object[names_header + 4..names_header + 8].copy_from_slice(&3_u32.to_le_bytes());
        object[names_header + 24..names_header + 32]
            .copy_from_slice(&(names_offset as u64).to_le_bytes());
        object[names_header + 32..names_header + 40]
            .copy_from_slice(&(names.len() as u64).to_le_bytes());
        object[names_header + 48..names_header + 56].copy_from_slice(&1_u64.to_le_bytes());
        object
    }

    /// A minimal single-object GNU `ar` archive (no symtab / long-name table).
    fn gnu_ar_raw_named_object(name: &str, object: &[u8]) -> Vec<u8> {
        assert!(!name.is_empty() && name.len() <= 15 && !name.contains('/'));
        let mut a = b"!<arch>\n".to_vec();
        let member_name = format!("{name}/");
        let mut h = format!("{member_name:<16}").into_bytes();
        h.extend_from_slice(format!("{:<12}", 0).as_bytes()); // mtime
        h.extend_from_slice(format!("{:<6}", 0).as_bytes()); // uid
        h.extend_from_slice(format!("{:<6}", 0).as_bytes()); // gid
        h.extend_from_slice(format!("{:<8}", "100644").as_bytes()); // mode
        h.extend_from_slice(format!("{:<10}", object.len()).as_bytes()); // size
        h.extend_from_slice(b"`\n");
        assert_eq!(h.len(), 60);
        a.extend_from_slice(&h);
        a.extend_from_slice(object);
        if object.len() % 2 == 1 {
            a.push(b'\n');
        }
        a
    }

    fn gnu_ar_one_object(payload: &[u8]) -> Vec<u8> {
        gnu_ar_named_object("object.o", payload)
    }

    fn gnu_ar_named_object(name: &str, payload: &[u8]) -> Vec<u8> {
        gnu_ar_raw_named_object(name, &elf64le_relocatable(payload))
    }

    #[test]
    fn hash_static_lib_caches_and_is_namespace_isolated() {
        let dir = tempfile::tempdir().unwrap();
        let fh = FileHasher::persistent(&dir.path().join("index.db"));
        let lib = dir.path().join("libbig.a");
        // >MIN_PERSISTED_HASH_BYTES so it takes the cached path.
        std::fs::write(&lib, gnu_ar_one_object(&vec![0x41u8; 70_000])).unwrap();

        let portable = fh.hash_static_lib(&lib).unwrap();
        assert!(
            portable.starts_with("gnu-ar-v2:"),
            "a GNU archive gets the structural member digest"
        );
        // Second call returns the SAME value (cache round-trip, not corruption).
        assert_eq!(fh.hash_static_lib(&lib).unwrap(), portable);

        // Namespace isolation: a whole-file hash of the SAME path is a plain-hex
        // digest under a different cache row — it must not be the static-lib value.
        let whole = fh.hash(&lib).unwrap();
        assert!(!whole.starts_with("gnu-ar-v2:"));
        assert_ne!(whole, portable);
        // And the static-lib digest is still the portable one after `hash` ran.
        assert_eq!(fh.hash_static_lib(&lib).unwrap(), portable);
    }

    #[test]
    fn hash_static_lib_gnu_longname_archive_is_checkout_independent() {
        // Every `cc` archive on Linux has a `//` table whose header GNU `ar`
        // leaves blank apart from name and size. Two checkouts that build the
        // same members must get the same structural digest, not a path-bound one.
        let bytes = crate::native_archive::gnu_crs_longname_archive_for_tests(b"payload");
        let fh = FileHasher::new();
        let mut digests = Vec::new();
        for checkout in ["checkout-a", "checkout-b"] {
            let dir = tempfile::tempdir().unwrap();
            let lib = dir.path().join(checkout).join("libprobe.a");
            std::fs::create_dir_all(lib.parent().unwrap()).unwrap();
            std::fs::write(&lib, &bytes).unwrap();
            for usage in [StaticLibUse::Bundled, StaticLibUse::Linked] {
                let digest = fh.hash_static_lib_for(&lib, usage).unwrap();
                assert!(digest.starts_with("gnu-ar-v2:"), "{usage:?}: {digest}");
                digests.push(digest);
            }
        }
        assert!(digests.windows(2).all(|pair| pair[0] == pair[1]));
    }

    #[test]
    fn hash_static_lib_ignores_legacy_namespaces() {
        let dir = tempfile::tempdir().unwrap();
        let fh = FileHasher::persistent(&dir.path().join("index.db"));
        let lib = dir.path().join("libbig.a");
        std::fs::write(&lib, gnu_ar_one_object(&vec![0x41_u8; 70_000])).unwrap();

        let fingerprint = FileFingerprint::from_path(&lib).unwrap();
        let legacy_key = FileFingerprint {
            path: format!("static-ar-v1\0{}", fingerprint.path),
            ..fingerprint.clone()
        };
        let legacy_v2_key = FileFingerprint {
            path: format!("static-ar-v2\0{}", fingerprint.path),
            ..fingerprint.clone()
        };
        let legacy_v3_key = FileFingerprint {
            path: format!("static-ar-v3\0{}", fingerprint.path),
            ..fingerprint.clone()
        };
        let legacy_v4_key = FileFingerprint {
            path: format!("static-ar-v4\0{}", fingerprint.path),
            ..fingerprint.clone()
        };
        let legacy_v5_key = FileFingerprint {
            path: format!("static-ar-v5\0{}", fingerprint.path),
            ..fingerprint.clone()
        };
        let legacy_v6_key = FileFingerprint {
            path: format!("static-ar-v6\0{}", fingerprint.path),
            ..fingerprint.clone()
        };
        let current_key = FileFingerprint {
            path: format!("static-ar-v7\0{}", fingerprint.path),
            ..fingerprint
        };
        let cache = fh.cache.as_ref().expect("persistent cache opens");
        cache
            .put(&legacy_key, "legacy-whole-file-sentinel")
            .unwrap();
        cache.put(&legacy_v2_key, "legacy-member-sentinel").unwrap();
        cache
            .put(&legacy_v3_key, "legacy-unguarded-object-sentinel")
            .unwrap();
        cache
            .put(&legacy_v4_key, "legacy-unguarded-macho-sentinel")
            .unwrap();
        cache
            .put(&legacy_v5_key, "legacy-path-bound-dwarf-sentinel")
            .unwrap();
        cache
            .put(&legacy_v6_key, "legacy-path-bound-blank-longname-sentinel")
            .unwrap();

        let computed = fh.hash_static_lib(&lib).unwrap();
        assert!(computed.starts_with("gnu-ar-v2:"));
        assert_ne!(computed, "legacy-whole-file-sentinel");
        assert_ne!(computed, "legacy-member-sentinel");
        assert_ne!(computed, "legacy-unguarded-object-sentinel");
        assert_ne!(computed, "legacy-unguarded-macho-sentinel");
        assert_ne!(computed, "legacy-path-bound-dwarf-sentinel");
        assert_ne!(computed, "legacy-path-bound-blank-longname-sentinel");
        fh.flush_memo_as_if_settled();
        assert_eq!(cache.get(&current_key).unwrap(), Some(computed.clone()));
        assert_eq!(fh.hash_static_lib(&lib).unwrap(), computed);
    }

    #[test]
    fn hash_static_lib_v3_memo_cannot_bypass_object_gate() {
        let dir = tempfile::tempdir().unwrap();
        let fh = FileHasher::persistent(&dir.path().join("index.db"));
        let lib = dir.path().join("libbitcode.a");
        let mut bitcode = vec![0_u8; 70_000];
        bitcode[..4].copy_from_slice(b"BC\xc0\xde");
        std::fs::write(&lib, gnu_ar_raw_named_object("bitcode.o", &bitcode)).unwrap();

        let fingerprint = FileFingerprint::from_path(&lib).unwrap();
        let stale_key = FileFingerprint {
            path: format!("static-ar-v3\0{}", fingerprint.path),
            ..fingerprint
        };
        let cache = fh.cache.as_ref().expect("persistent cache opens");
        cache.put(&stale_key, "gnu-ar-v2:unguarded").unwrap();

        let computed = fh.hash_static_lib(&lib).unwrap();
        assert!(computed.starts_with("path-ar-v1:"));
        assert_ne!(computed, "gnu-ar-v2:unguarded");
    }

    #[test]
    fn static_lib_fallback_binds_lexical_archive_path() {
        let dir = tempfile::tempdir().unwrap();
        let first_dir = dir.path().join("PerfUtils");
        let second_dir = dir.path().join("OtherName");
        std::fs::create_dir_all(&first_dir).unwrap();
        std::fs::create_dir_all(&second_dir).unwrap();
        let first = first_dir.join("libsame.a");
        let second = second_dir.join("libsame.a");
        std::fs::write(&first, b"unsupported but identical archive bytes").unwrap();
        std::fs::write(&second, b"unsupported but identical archive bytes").unwrap();

        let fh = FileHasher::new();
        let first_hash = fh.hash_static_lib(&first).unwrap();
        let second_hash = fh.hash_static_lib(&second).unwrap();
        assert!(first_hash.starts_with("path-ar-v1:"));
        assert!(second_hash.starts_with("path-ar-v1:"));
        assert_ne!(first_hash, second_hash);
    }

    /// A unit that does not link bundles the archive. A link reads it by path
    /// unless the injected `-oso_prefix` root covers it.
    #[test]
    fn linked_archive_use_follows_output_and_oso_root() {
        let root = Path::new("/w/target/debug");
        let under = Path::new("/w/target/debug/build/s-1/out/libfoo.a");
        let outside = Path::new("/opt/lib/libfoo.a");
        let use_of = |crate_type: &str, test: bool, archive: &Path, oso_root: Option<&Path>| {
            let mut argv = vec![
                "rustc".to_string(),
                "src/lib.rs".to_string(),
                "--crate-type".to_string(),
                crate_type.to_string(),
            ];
            if test {
                argv.push("--test".to_string());
            }
            linked_archive_use(&RustcArgs::parse(&argv).unwrap(), archive, oso_root)
        };
        for crate_type in ["lib", "rlib", "staticlib"] {
            assert_eq!(
                use_of(crate_type, false, outside, None),
                StaticLibUse::Bundled,
                "{crate_type}"
            );
        }
        for crate_type in ["bin", "cdylib", "proc-macro"] {
            assert_eq!(
                use_of(crate_type, false, outside, None),
                StaticLibUse::Linked,
                "{crate_type}"
            );
        }
        assert_eq!(use_of("lib", true, outside, None), StaticLibUse::Linked);
        assert_eq!(
            use_of("bin", false, under, Some(root)),
            StaticLibUse::Bundled,
            "the injected prefix strips an archive under its root"
        );
        assert_eq!(
            use_of("bin", false, outside, Some(root)),
            StaticLibUse::Linked
        );
        assert_eq!(use_of("bin", false, under, None), StaticLibUse::Linked);
    }

    /// A DWARF-bearing Mach-O archive shares its structural digest across
    /// checkouts only when rustc bundles it into an rlib. An invocation that
    /// links it itself writes the archive's absolute path into `N_OSO`, so
    /// the digest stays path-bound there.
    #[test]
    fn linked_dwarf_archive_stays_path_bound() {
        let dir = tempfile::tempdir().unwrap();
        let fh = FileHasher::new();
        let hash_in = |name: &str, bytes: &[u8], usage: StaticLibUse| {
            let lib_dir = dir.path().join(name);
            std::fs::create_dir_all(&lib_dir).unwrap();
            let lib = lib_dir.join("libprobe.a");
            std::fs::write(&lib, bytes).unwrap();
            fh.hash_static_lib_for(&lib, usage).unwrap()
        };

        let dwarf = crate::native_archive::dwarf_bsd_archive_for_tests(b"debug");
        let bundled = hash_in("a", &dwarf, StaticLibUse::Bundled);
        assert!(bundled.starts_with("bsd-ar-v2:"));
        assert_eq!(bundled, hash_in("b", &dwarf, StaticLibUse::Bundled));

        let linked = hash_in("a", &dwarf, StaticLibUse::Linked);
        assert!(linked.starts_with("path-ar-v1:"));
        assert_ne!(linked, hash_in("b", &dwarf, StaticLibUse::Linked));
        // The unqualified method is the linked reading.
        assert_eq!(
            fh.hash_static_lib(&dir.path().join("a/libprobe.a"))
                .unwrap(),
            linked
        );

        // Without DWARF, a linked archive keeps the structural digest.
        let plain = gnu_ar_one_object(b"object");
        let linked_plain = hash_in("c", &plain, StaticLibUse::Linked);
        assert!(linked_plain.starts_with("gnu-ar-v2:"));
        assert_eq!(linked_plain, hash_in("d", &plain, StaticLibUse::Linked));
    }

    #[test]
    fn hash_static_lib_memo_rows_are_per_use() {
        let dir = tempfile::tempdir().unwrap();
        let fh = FileHasher::persistent(&dir.path().join("index.db"));
        let lib = dir.path().join("libbig.a");
        // Large enough for the persistent memo.
        std::fs::write(
            &lib,
            crate::native_archive::dwarf_bsd_archive_for_tests(&vec![0x41_u8; 70_000]),
        )
        .unwrap();

        let bundled = fh.hash_static_lib_for(&lib, StaticLibUse::Bundled).unwrap();
        let linked = fh.hash_static_lib_for(&lib, StaticLibUse::Linked).unwrap();
        assert!(bundled.starts_with("bsd-ar-v2:"));
        assert!(linked.starts_with("path-ar-v1:"));
        assert_eq!(
            fh.hash_static_lib_for(&lib, StaticLibUse::Bundled).unwrap(),
            bundled
        );

        fh.flush_memo_as_if_settled();
        let fingerprint = FileFingerprint::from_path(&lib).unwrap();
        let cache = fh.cache.as_ref().expect("persistent cache opens");
        for (namespace, expected) in [
            ("static-ar-v7-bundled", &bundled),
            ("static-ar-v7", &linked),
        ] {
            let key = FileFingerprint {
                path: format!("{namespace}\0{}", fingerprint.path),
                ..fingerprint.clone()
            };
            assert_eq!(cache.get(&key).unwrap().as_ref(), Some(expected));
        }
    }

    /// Archives under `MIN_PERSISTED_HASH_BYTES` are hashed without a memo
    /// row; one of exactly that size gets a row.
    #[test]
    fn hash_static_lib_memo_threshold_is_exclusive() {
        let dir = tempfile::tempdir().unwrap();
        let fh = FileHasher::persistent(&dir.path().join("index.db"));
        let cache = fh.cache.as_ref().expect("persistent cache opens");
        let threshold = usize::try_from(MIN_PERSISTED_HASH_BYTES).unwrap();
        for (name, len, memoized) in [
            ("libsmall.a", threshold - 1, false),
            ("libexact.a", threshold, true),
        ] {
            let lib = dir.path().join(name);
            std::fs::write(&lib, vec![b'x'; len]).unwrap();
            let hash = fh.hash_static_lib(&lib).unwrap();
            fh.flush_memo_as_if_settled();
            let fingerprint = FileFingerprint::from_path(&lib).unwrap();
            let key = FileFingerprint {
                path: format!("static-ar-v7\0{}", fingerprint.path),
                ..fingerprint
            };
            let expected = memoized.then_some(hash);
            assert_eq!(cache.get(&key).unwrap(), expected, "{name}");
        }
    }

    #[test]
    fn thin_static_archive_is_uncacheable() {
        let dir = tempfile::tempdir().unwrap();
        let archive = dir.path().join("libthin.a");
        std::fs::write(&archive, b"!<thin>\n").unwrap();

        let error = FileHasher::new().hash_static_lib(&archive).unwrap_err();
        assert!(error.to_string().contains("external members"));
    }

    /// The cardinal #421 false hit: a `static=` native lib whose content changes
    /// in place (same `-l` name, same normalized `-L` path) must change the key.
    #[test]
    fn native_static_lib_content_change_changes_key() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();
        let libdir = dir.path().join("out");
        std::fs::create_dir_all(&libdir).unwrap();
        let lib = libdir.join("libfoo.a");
        std::fs::write(&lib, b"v1 archive bytes").unwrap();
        let search = format!("native={}", libdir.display());
        let flags = ["-L", search.as_str(), "-l", "static=foo"];

        let k1 = key_of(&flag_base(&source, &flags));
        std::fs::write(&lib, b"v2 archive bytes - DIFFERENT").unwrap();
        let k2 = key_of(&flag_base(&source, &flags));
        assert_ne!(
            k1, k2,
            "a native static lib content change must change the key (#421)"
        );
        // Content-addressed: the original bytes reproduce the original key.
        std::fs::write(&lib, b"v1 archive bytes").unwrap();
        let k3 = key_of(&flag_base(&source, &flags));
        assert_eq!(k1, k3, "identical bytes must reproduce the key");
    }

    /// `cc::Build::compile` emits its OUT_DIR for every archive it creates.
    /// Repeating that directory must not make the one archive uncacheable.
    #[test]
    fn duplicate_native_search_dir_keeps_static_lib_cacheable() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();
        let libdir = dir.path().join("out");
        std::fs::create_dir_all(&libdir).unwrap();
        std::fs::write(libdir.join("libfoo.a"), b"archive bytes").unwrap();
        let search = format!("native={}", libdir.display());
        let flags = [
            "-L",
            search.as_str(),
            "-L",
            search.as_str(),
            "-l",
            "static=foo",
        ];

        assert!(!key_of(&flag_base(&source, &flags)).is_empty());
    }

    /// A `dylib=` lib is referenced at runtime, not bundled into the output, so
    /// its content must NOT enter the key (guards against over-keying).
    /// Generic `-l` keying, checked on hosts that do not probe native MSVC
    /// inputs. On a Windows host the library must exist (fail closed), so the
    /// Windows shape lives in the `windows_*` tests with an injected probe.
    #[cfg(not(windows))]
    #[test]
    fn native_dylib_content_does_not_change_key() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();
        let libdir = dir.path().join("out");
        std::fs::create_dir_all(&libdir).unwrap();
        let lib = libdir.join("libfoo.so");
        std::fs::write(&lib, b"so v1").unwrap();
        let search = format!("native={}", libdir.display());
        let flags = ["-L", search.as_str(), "-l", "dylib=foo"];

        let k1 = key_of(&flag_base(&source, &flags));
        std::fs::write(&lib, b"so v2 changed").unwrap();
        let k2 = key_of(&flag_base(&source, &flags));
        assert_eq!(k1, k2, "a dynamic lib's content must not key the consumer");
    }

    /// rustc's default `-L` kind is `all`, which searches native libs too, so a
    /// `static=` lib found under `-L all=<dir>` must be content-keyed (#421).
    #[test]
    fn native_static_lib_in_all_search_dir_is_keyed() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();
        let libdir = dir.path().join("out");
        std::fs::create_dir_all(&libdir).unwrap();
        let lib = libdir.join("libfoo.a");
        std::fs::write(&lib, b"v1 archive bytes").unwrap();
        let search = format!("all={}", libdir.display());
        let flags = ["-L", search.as_str(), "-l", "static=foo"];

        let k1 = key_of(&flag_base(&source, &flags));
        std::fs::write(&lib, b"v2 archive bytes - DIFFERENT").unwrap();
        let k2 = key_of(&flag_base(&source, &flags));
        assert_ne!(
            k1, k2,
            "a static lib under `-L all=` must be content-keyed too (#421)"
        );
    }

    /// Argv for an rlib, where a build-script archive does its damage: rustc
    /// copies the archive into the rlib and every later link reads that copy.
    fn rlib_base(source: &Path, extra: &[&str]) -> Vec<String> {
        let mut args: Vec<String> = ["rustc", "--crate-name", "mylib", "--crate-type", "lib"]
            .map(String::from)
            .into();
        args.push(source.to_string_lossy().into_owned());
        args.extend(extra.iter().map(|s| s.to_string()));
        args
    }

    /// A build script can name its archive with link modifiers:
    /// `static:+whole-archive=foo` still reads `libfoo.a`, and
    /// `static:+verbatim=foo.a` reads `foo.a`. Either is bundled like a plain
    /// `static=foo`, so rebuilding the archive in place must change the key.
    #[test]
    fn native_static_lib_named_with_modifiers_is_content_keyed() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();
        let libdir = dir.path().join("out");
        std::fs::create_dir_all(&libdir).unwrap();
        let search = format!("native={}", libdir.display());

        for (spec, file) in [
            ("static:+whole-archive=foo", "libfoo.a"),
            ("static:+verbatim=foo.a", "foo.a"),
        ] {
            let lib = libdir.join(file);
            let flags = ["-L", search.as_str(), "-l", spec];
            std::fs::write(&lib, b"v1 archive bytes").unwrap();
            let k1 = key_of(&rlib_base(&source, &flags));
            std::fs::write(&lib, b"v2 archive bytes - DIFFERENT").unwrap();
            let k2 = key_of(&rlib_base(&source, &flags));
            assert_ne!(k1, k2, "{spec}: a rebuilt archive must change the key");
            std::fs::write(&lib, b"v1 archive bytes").unwrap();
            let k3 = key_of(&rlib_base(&source, &flags));
            assert_eq!(k1, k3, "{spec}: identical bytes must reproduce the key");
            std::fs::remove_file(&lib).unwrap();
        }
    }

    /// `static=foo:bar` makes rustc link `bar` wherever a `#[link]` attribute
    /// names `foo`. Which archive that reads is not modelled, and keying it by
    /// name could restore an rlib bundling an older archive, so the key fails.
    #[test]
    fn renamed_native_static_lib_is_not_cacheable() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();
        let libdir = dir.path().join("out");
        std::fs::create_dir_all(&libdir).unwrap();
        std::fs::write(libdir.join("libfoo.a"), b"archive bytes").unwrap();
        std::fs::write(libdir.join("libbar.a"), b"archive bytes").unwrap();
        let search = format!("native={}", libdir.display());
        let flags = ["-L", search.as_str(), "-l", "static=foo:bar"];

        let parsed = RustcArgs::parse(&rlib_base(&source, &flags)).unwrap();
        let error =
            compute_cache_key(&parsed, &FileHasher::new(), &PathNormalizer::empty()).unwrap_err();
        assert!(
            format!("{error:#}").contains("is not cacheable"),
            "a renamed static lib must fail the key: {error:#}"
        );
    }

    /// A `dylib` lib is referenced, not copied into the rlib, so its bytes
    /// stay out of the key while `static` specs with modifiers are hashed.
    /// `+verbatim` names the file itself, so a `dylib` wrongly taken for a
    /// `static` would be found and hashed.
    #[test]
    fn native_dylib_stays_name_only_for_an_rlib() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();
        let libdir = dir.path().join("out");
        std::fs::create_dir_all(&libdir).unwrap();
        let lib = libdir.join("libfoo.so");
        let search = format!("native={}", libdir.display());

        for spec in ["dylib=foo", "dylib:+verbatim=libfoo.so"] {
            let flags = ["-L", search.as_str(), "-l", spec];
            std::fs::write(&lib, b"so v1").unwrap();
            let k1 = key_of(&rlib_base(&source, &flags));
            std::fs::write(&lib, b"so v2 changed").unwrap();
            let k2 = key_of(&rlib_base(&source, &flags));
            assert_eq!(
                k1, k2,
                "{spec}: a dynamic lib's content must not key the rlib"
            );
        }
    }

    /// A Cargo build tree: `<tmp>/target/debug/deps` for outputs and a build
    /// script's `<tmp>/target/debug/build/s-1/out`, plus a dir outside it.
    struct NativeTree {
        _tmp: tempfile::TempDir,
        target: PathBuf,
        deps: PathBuf,
        out: PathBuf,
        elsewhere: PathBuf,
    }

    fn native_tree() -> NativeTree {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("target");
        let deps = target.join("debug/deps");
        let out = target.join("debug/build/s-1/out");
        let elsewhere = tmp.path().join("elsewhere/lib");
        for dir in [&deps, &out, &elsewhere] {
            std::fs::create_dir_all(dir).unwrap();
        }
        NativeTree {
            _tmp: tmp,
            target,
            deps,
            out,
            elsewhere,
        }
    }

    /// Argv for a `crate_type` unit (`test` for a `--test` harness) writing
    /// into `out_dir`. Keyed with [`key_of_flags`], so no source is read.
    fn unit_args(crate_type: &str, out_dir: &Path, extra: &[&str]) -> Vec<String> {
        let mut args: Vec<String> = ["rustc", "--crate-name", "mylib", "src/lib.rs"]
            .map(String::from)
            .into();
        args.push("--out-dir".to_string());
        args.push(out_dir.display().to_string());
        if crate_type == "test" {
            args.push("--test".to_string());
        } else {
            args.push(format!("--crate-type={crate_type}"));
        }
        args.extend(extra.iter().map(|s| s.to_string()));
        args
    }

    /// [`key_of_flags`] for a key that may fail.
    #[cfg(not(windows))]
    fn try_key_of_flags(args: &[String]) -> Result<String> {
        let mut parsed = RustcArgs::parse(args).unwrap();
        parsed.source_file = None;
        compute_cache_key(&parsed, &FileHasher::new(), &PathNormalizer::empty())
    }

    /// Key `argv`, rewrite `file` with new bytes, and key it again.
    #[cfg(not(windows))]
    fn keys_around_rewrite(argv: &[String], file: &Path) -> (String, String) {
        std::fs::write(file, b"v1 native bytes").unwrap();
        let before = key_of_flags(argv);
        std::fs::write(file, b"v2 native bytes, different").unwrap();
        (before, key_of_flags(argv))
    }

    /// A Unix link context for `target` with no default dirs, no OUT_DIR, no
    /// build tree, no system library dirs and no path rules.
    fn unix_context<'a>(target: &'a str, normalizer: &'a PathNormalizer) -> NativeLinkContext<'a> {
        NativeLinkContext {
            native_windows_msvc: false,
            target,
            default_dirs: Vec::new(),
            out_dir: None,
            build_tree: Vec::new(),
            system_dirs: Vec::new(),
            path_normalizer: normalizer,
        }
    }

    /// [`fold_native_link_inputs`] for `argv` with explicit search dirs and
    /// context: the fold's digest and what it reports.
    fn fold_inputs(
        argv: &[String],
        native: &[PathBuf],
        framework: &[PathBuf],
        context: &NativeLinkContext<'_>,
    ) -> Result<(String, KeyedNativeArchives)> {
        let args = RustcArgs::parse(argv).unwrap();
        let mut hasher = blake3::Hasher::new();
        let keyed = fold_native_link_inputs(
            &mut hasher,
            &args,
            &NativeSearchDirs { native, framework },
            context,
            &FileHasher::new(),
        )?;
        Ok((hasher.finalize().to_hex().to_string(), keyed))
    }

    /// Whether rewriting `file` with new `ar` bytes changes the fold.
    fn fold_rewrites(
        file: &Path,
        fold: impl Fn() -> Result<(String, KeyedNativeArchives)>,
    ) -> bool {
        std::fs::write(file, b"!<arch>\nv1 native bytes").unwrap();
        let before = fold().unwrap().0;
        std::fs::write(file, b"!<arch>\nv2 native bytes, different").unwrap();
        before != fold().unwrap().0
    }

    /// Cargo passes a build script's `-L` to every dependent, so a binary two
    /// crates above a sys crate sees its OUT_DIR with no `-l`. The archive
    /// there reaches the binary through the sys crate's rlib, whose bytes the
    /// binary's `--extern`s do not cover.
    #[cfg(not(windows))]
    #[test]
    fn linked_output_keys_build_tree_archives_without_link_spec() {
        let _lock = key_test_lock();
        let tree = native_tree();
        let search = format!("native={}", tree.out.display());
        for crate_type in ["bin", "test", "cdylib", "staticlib"] {
            let argv = unit_args(crate_type, &tree.deps, &["-L", &search]);
            let (before, after) = keys_around_rewrite(&argv, &tree.out.join("libfoo.a"));
            assert_ne!(before, after, "{crate_type}: a rebuilt archive must re-key");
        }
    }

    /// Every archive in a `-L` dir away from the system library dirs keys a
    /// linked output, inside the build tree or not: a monorepo's native build
    /// above the workspace, a path dependency beside it, or `OPENSSL_DIR`.
    /// Cargo hands a build script's `-L` to every dependent, and a
    /// dependency's rlib can carry those archives with no `-l` of this unit
    /// naming them. The same holds for a link-argument `-L` and for a static
    /// framework in a `framework=` dir.
    #[cfg(not(windows))]
    #[test]
    fn linked_output_scans_dirs_in_and_outside_the_workspace() {
        let _lock = key_test_lock();
        let tree = native_tree();
        let in_tree = tree.target.join("native");
        std::fs::create_dir_all(&in_tree).unwrap();
        for dir in [&in_tree, &tree.elsewhere] {
            let shown = dir.display();
            let archive = dir.join("libssl.a");
            let search = format!("native={shown}");
            for crate_type in ["bin", "test", "cdylib", "staticlib"] {
                let argv = unit_args(crate_type, &tree.deps, &["-L", &search]);
                let (before, after) = keys_around_rewrite(&argv, &archive);
                assert_ne!(before, after, "{shown}: {crate_type}");
            }
            let link_search = format!("-Clink-arg=-L{shown}");
            let (before, after) =
                keys_around_rewrite(&unit_args("bin", &tree.deps, &[&link_search]), &archive);
            assert_ne!(before, after, "{shown}: a link-argument -L dir");

            let bundle = dir.join("Foo.framework");
            std::fs::create_dir_all(&bundle).unwrap();
            let frameworks = format!("framework={shown}");
            let bin = unit_args("bin", &tree.deps, &["-L", &frameworks]);
            std::fs::write(bundle.join("Foo"), b"!<arch>\nv1 framework").unwrap();
            let before = key_of_flags(&bin);
            std::fs::write(bundle.join("Foo"), b"!<arch>\nv2 framework").unwrap();
            assert_ne!(before, key_of_flags(&bin), "{shown}: a static framework");
        }
    }

    /// A system library dir outside the build tree (`/opt/homebrew/lib`) is
    /// not scanned: rewriting an archive there that no `-l` names leaves a
    /// linked output's fold alone, and rewriting one an `-l` resolves to
    /// re-keys it, in each spelling of `-l`. A `-L` in the link arguments
    /// follows the same rule.
    #[test]
    fn system_dir_archives_key_only_by_name() {
        let tree = native_tree();
        let normalizer = PathNormalizer::empty();
        let context = NativeLinkContext {
            system_dirs: vec![resolved_path(&tree.elsewhere)],
            ..unix_context("x86_64-unknown-linux-gnu", &normalizer)
        };
        std::fs::write(tree.elsewhere.join("libssl.a"), b"!<arch>\nssl").unwrap();
        let archive = tree.elsewhere.join("libz.a");
        let rewrites = |crate_type: &str, native: &[PathBuf], extra: &[&str]| {
            let argv = unit_args(crate_type, &tree.deps, extra);
            fold_rewrites(&archive, || fold_inputs(&argv, native, &[], &context))
        };
        let native = [tree.elsewhere.clone()];
        for crate_type in ["bin", "test", "cdylib", "staticlib"] {
            assert!(!rewrites(crate_type, &native, &[]), "{crate_type}: no -l");
            assert!(
                rewrites(crate_type, &native, &["-l", "static=z"]),
                "{crate_type}: a static spec"
            );
        }
        for spec in ["z", "dylib=z"] {
            assert!(rewrites("bin", &native, &["-l", spec]), "{spec}");
        }

        let link_search = format!("-Clink-arg=-L{}", tree.elsewhere.display());
        assert!(!rewrites("bin", &[], &[&link_search]), "a link-argument -L");
        for link_arg in [
            "-Clink-arg=-lz",
            "-Clink-arg=-Wl,-hidden-lz",
            "-Clink-arg=-Wl,-force-lz",
        ] {
            assert!(
                rewrites("bin", &[], &[&link_search, link_arg]),
                "{link_arg}"
            );
        }
    }

    /// A thin archive or an unreadable dir under a system library dir refuses
    /// a link only when an `-l` resolves to it; anywhere else the scan reads
    /// it and refuses.
    #[test]
    fn system_dir_thin_archive_refuses_only_a_link_that_names_it() {
        let tree = native_tree();
        let normalizer = PathNormalizer::empty();
        let system = NativeLinkContext {
            system_dirs: vec![resolved_path(&tree.elsewhere)],
            ..unix_context("x86_64-unknown-linux-gnu", &normalizer)
        };
        let elsewhere = unix_context("x86_64-unknown-linux-gnu", &normalizer);
        std::fs::write(tree.elsewhere.join("libthin.a"), b"!<thin>\n").unwrap();
        let fold = |context: &NativeLinkContext<'_>, dir: &Path, extra: &[&str]| {
            let argv = unit_args("bin", &tree.deps, extra);
            fold_inputs(&argv, &[dir.to_path_buf()], &[], context)
        };
        let unreadable = tree.elsewhere.join("libthin.a");
        assert!(fold(&system, &tree.elsewhere, &[]).is_ok());
        assert!(fold(&system, &tree.elsewhere, &["-l", "static=thin"]).is_err());
        assert!(fold(&system, &unreadable, &[]).is_ok());
        assert!(fold(&elsewhere, &tree.elsewhere, &[]).is_err());
        assert!(fold(&elsewhere, &unreadable, &[]).is_err());
    }

    /// A dir under a system library dir is still scanned inside the build
    /// tree. Each is compared resolved: a dir is judged by where it leads,
    /// and a build tree spelled through a symlink (macOS reaches `/var` as
    /// `/private/var`) still holds the build script's OUT_DIR.
    #[cfg(unix)]
    #[test]
    fn system_dirs_and_the_build_tree_are_compared_resolved() {
        let tree = native_tree();
        let normalizer = PathNormalizer::empty();
        let system = tree.target.with_file_name("system");
        std::fs::create_dir_all(&system).unwrap();
        let leads_out = system.join("out");
        std::os::unix::fs::symlink(&tree.out, &leads_out).unwrap();
        let alias = tree.target.with_file_name("alias");
        std::os::unix::fs::symlink(&tree.target, &alias).unwrap();
        let bin = unit_args("bin", &tree.deps, &[]);
        let rewrites = |dir: &Path, system_dirs: &[&Path], build_tree: &[&Path]| {
            let context = NativeLinkContext {
                system_dirs: system_dirs.iter().map(|dir| resolved_path(dir)).collect(),
                build_tree: build_tree.iter().map(|root| root.to_path_buf()).collect(),
                ..unix_context("x86_64-unknown-linux-gnu", &normalizer)
            };
            let dirs = [dir.to_path_buf()];
            fold_rewrites(&tree.out.join("libfoo.a"), || {
                fold_inputs(&bin, &dirs, &[], &context)
            })
        };
        let target: &Path = &tree.target;
        assert!(rewrites(&leads_out, &[&system], &[]), "leads out of it");
        let leads_in = alias.join("debug/build/s-1/out");
        assert!(!rewrites(&leads_in, &[target], &[]), "leads into it");
        assert!(rewrites(&tree.out, &[target], &[&alias]), "an aliased tree");
        assert!(!rewrites(&tree.out, &[target], &[&tree.elsewhere]));
    }

    /// The scan keys linked outputs only, only archives, and no system
    /// library dir outside the build tree.
    #[test]
    fn archive_scan_leaves_other_units_and_files_alone() {
        let tree = native_tree();
        let normalizer = PathNormalizer::empty();
        let context = unix_context("x86_64-unknown-linux-gnu", &normalizer);
        let dirs = [tree.out.clone()];
        let rewrites = |argv: Vec<String>, file: &Path| {
            fold_rewrites(file, || fold_inputs(&argv, &dirs, &[], &context))
        };
        let archive = tree.out.join("libfoo.a");
        assert!(!rewrites(unit_args("rlib", &tree.deps, &[]), &archive));
        assert!(!rewrites(
            unit_args("bin", &tree.deps, &["--emit=metadata"]),
            &archive
        ));
        let bin = unit_args("bin", &tree.deps, &[]);
        assert!(rewrites(bin.clone(), &archive));
        assert!(!rewrites(bin.clone(), &tree.out.join("libfoo.so")));
        assert!(!rewrites(bin.clone(), &tree.out.join("foo.o")));

        let system = NativeLinkContext {
            system_dirs: vec![resolved_path(&tree.target)],
            ..unix_context("x86_64-unknown-linux-gnu", &normalizer)
        };
        assert!(!fold_rewrites(&archive, || fold_inputs(
            &bin,
            &dirs,
            &[],
            &system
        )));
    }

    /// A `#[link(kind = "static")]` attribute bundles an archive into an rlib
    /// with no `-l` on argv. The archives under the unit's own OUT_DIR key its
    /// rlib, so the audit need not refuse it; dirs elsewhere do not.
    #[test]
    fn rlib_keys_archives_under_its_own_out_dir() {
        let tree = native_tree();
        let normalizer = PathNormalizer::empty();
        let unit_out = tree.out.clone();
        let own = tree.out.join("x64");
        std::fs::create_dir_all(&own).unwrap();
        let dirs = [own.clone(), tree.elsewhere.clone()];
        let rlib = unit_args("rlib", &tree.deps, &[]);
        let fold_with = |argv: &[String], out_dir: Option<&Path>| {
            let context = NativeLinkContext {
                out_dir,
                ..unix_context("x86_64-unknown-linux-gnu", &normalizer)
            };
            fold_inputs(argv, &dirs, &[], &context)
        };
        let archive = own.join("libWebView2LoaderStatic.a");
        assert!(fold_rewrites(&archive, || fold_with(
            &rlib,
            Some(&unit_out)
        )));
        assert_eq!(
            fold_with(&rlib, Some(&unit_out)).unwrap().1.archives,
            std::slice::from_ref(&archive)
        );
        assert!(!fold_rewrites(&tree.elsewhere.join("libdep.a"), || {
            fold_with(&rlib, Some(&unit_out))
        }));
        assert!(!fold_rewrites(&archive, || fold_with(&rlib, None)));
        let metadata = unit_args("rlib", &tree.deps, &["--emit=metadata"]);
        assert!(!fold_rewrites(&archive, || fold_with(
            &metadata,
            Some(&unit_out)
        )));

        // Cargo's OUT_DIR and the build script's `-L` may spell the same dir
        // differently.
        #[cfg(unix)]
        {
            let alias = tree.elsewhere.join("alias");
            std::os::unix::fs::symlink(&unit_out, &alias).unwrap();
            assert!(fold_rewrites(&archive, || fold_with(&rlib, Some(&alias))));
        }
    }

    #[test]
    fn native_scan_dirs_follow_the_unit_and_skip_cargo_packages() {
        let tree = native_tree();
        let normalizer = PathNormalizer::empty();
        let unit_out = tree.out.parent().unwrap().to_path_buf();
        let context = NativeLinkContext {
            out_dir: Some(&unit_out),
            system_dirs: vec![resolved_path(&tree.elsewhere)],
            ..unix_context("x86_64-unknown-linux-gnu", &normalizer)
        };
        let native = tree.target.join("native");
        std::fs::create_dir_all(&native).unwrap();
        let dirs = [
            tree.out.clone(),
            tree.elsewhere.clone(),
            native.clone(),
            tree.out.clone(),
        ];
        let scan = |crate_type: &str| {
            let args = RustcArgs::parse(&unit_args(crate_type, &tree.deps, &[])).unwrap();
            native_scan_dirs(&args, &dirs, &context)
        };
        assert_eq!(scan("bin"), [tree.out.clone(), native.clone()]);
        assert_eq!(scan("rlib"), std::slice::from_ref(&tree.out));
        assert_eq!(scan("rlib,staticlib"), scan("bin"));
        assert_eq!(context.scanned_dirs(&dirs), scan("bin"));

        let package = PathBuf::from("/home/u/.cargo/registry/src/index-1/windows_x-0.52.6/lib");
        let dirs = [package.clone(), tree.elsewhere.clone()];
        let kept: Vec<&PathBuf> =
            dirs_outside_packages(&dirs, |dir| dir.replace("/home/u/.cargo", "<CARGO_HOME>"))
                .collect();
        assert_eq!(kept, [&tree.elsewhere]);
    }

    #[test]
    fn cargo_package_dirs_are_registry_sources_and_git_checkouts() {
        assert!(is_cargo_package_dir(
            "<CARGO_HOME>/registry/src/index.crates.io-1/foo-1.0.0/lib"
        ));
        assert!(is_cargo_package_dir(
            "<CARGO_HOME>\\registry\\src\\index.crates.io-1\\foo-1.0.0\\lib"
        ));
        assert!(is_cargo_package_dir(
            "<CARGO_HOME>/git/checkouts/foo-1a2b/3c4d/lib"
        ));
        assert!(!is_cargo_package_dir("<CARGO_HOME>/registry/cache/x"));
        assert!(!is_cargo_package_dir("<CARGO_HOME>/git/db/x"));
        assert!(!is_cargo_package_dir("<CARGO_HOME>/src/registry"));
        assert!(!is_cargo_package_dir("/w/registry/src/index/foo-1.0.0/lib"));
        assert!(!is_cargo_package_dir("<WORKSPACE>/registry/src/x"));
    }

    #[test]
    fn dirs_under_compare_resolved_paths() {
        let tree = native_tree();
        let root = tree.out.parent().unwrap();
        let dirs = [tree.out.clone(), tree.elsewhere.clone()];
        assert_eq!(dirs_under(&dirs, root), std::slice::from_ref(&tree.out));
        assert!(dirs_under(&dirs, &tree.deps).is_empty());
        assert!(dirs_under(&[], root).is_empty());
        assert_eq!(
            unique_dirs(&[tree.out.clone(), tree.elsewhere.clone(), tree.out.clone()]),
            [tree.out.clone(), tree.elsewhere.clone()]
        );

        // A root or a dir spelled through a symlink is compared by its target.
        #[cfg(unix)]
        {
            let alias = tree.target.with_file_name("alias");
            std::os::unix::fs::symlink(&tree.target, &alias).unwrap();
            assert_eq!(dirs_under(&dirs, &alias), std::slice::from_ref(&tree.out));
            let aliased = [alias.join("debug/build/s-1/out"), tree.elsewhere.clone()];
            assert_eq!(
                dirs_under(&aliased, &tree.target),
                std::slice::from_ref(&aliased[0])
            );
        }
    }

    #[test]
    fn build_tree_roots_are_the_target_dir_and_the_workspace() {
        let deps = Path::new("/w/target/debug/deps");
        let workspace = Path::new("/w");
        assert_eq!(
            build_tree_roots_of(Some(deps), None, Some(workspace)),
            [PathBuf::from("/w/target"), PathBuf::from("/w")]
        );
        assert_eq!(
            build_tree_roots_of(Some(Path::new("/w/target/debug/build/s-1")), None, None),
            [PathBuf::from("/w/target")]
        );
        assert_eq!(
            build_tree_roots_of(Some(Path::new("/tmp/out")), None, Some(workspace)),
            [PathBuf::from("/w")],
            "an --out-dir outside Cargo's layout"
        );
        assert!(build_tree_roots_of(None, None, None).is_empty());

        let triple = "x86_64-unknown-linux-gnu";
        let cross = Path::new("/w/target/x86_64-unknown-linux-gnu/debug");
        assert_eq!(
            cargo_target_dir(cross, Some(triple)),
            Path::new("/w/target")
        );
        assert_eq!(
            cargo_target_dir(Path::new("/w/target/spec/debug"), Some("targets/spec.json")),
            Path::new("/w/target"),
            "a target spec file names its dir by its stem"
        );
        assert_eq!(
            cargo_target_dir(cross, None),
            Path::new("/w/target/x86_64-unknown-linux-gnu")
        );
        assert_eq!(
            cargo_target_dir(Path::new("/w/target/debug"), Some(triple)),
            Path::new("/w/target"),
            "a host unit of a cross build"
        );
        assert_eq!(
            cargo_target_dir(Path::new("/debug"), Some("debug")),
            Path::new("/")
        );
        assert_eq!(cargo_target_dir(Path::new("/"), None), Path::new("/"));
    }

    #[test]
    fn build_tree_roots_read_the_unit_out_dir_and_target() {
        let args = RustcArgs::parse(&[
            "rustc".to_string(),
            "src/main.rs".to_string(),
            "--out-dir".to_string(),
            "/w/target/x86_64-unknown-linux-gnu/debug/deps".to_string(),
            "--target".to_string(),
            "x86_64-unknown-linux-gnu".to_string(),
        ])
        .unwrap();
        assert_eq!(
            build_tree_roots(&args).first(),
            Some(&PathBuf::from("/w/target")),
            "the target dir comes from --out-dir and --target"
        );
    }
    #[test]
    fn native_dir_archives_list_regular_archives_in_order() {
        let dir = tempfile::tempdir().unwrap();
        for name in [
            "libz.a",
            "foo.LIB",
            "liba.A",
            "libfoo.so",
            "foo.o",
            "notes.txt",
        ] {
            std::fs::write(dir.path().join(name), b"x").unwrap();
        }
        std::fs::create_dir(dir.path().join("sub.a")).unwrap();
        assert_eq!(
            native_dir_archives(dir.path()).unwrap(),
            ["foo.LIB", "liba.A", "libz.a"].map(|name| dir.path().join(name))
        );
        assert!(
            native_dir_archives(&dir.path().join("absent"))
                .unwrap()
                .is_empty(),
            "a missing dir holds no archives"
        );
        // Windows reports a file read as a dir as a missing path.
        #[cfg(unix)]
        assert!(
            native_dir_archives(&dir.path().join("libz.a")).is_err(),
            "a dir that cannot be read is an error"
        );
        assert!(is_native_archive_name("libfoo.a"));
        assert!(is_native_archive_name("FOO.Lib"));
        assert!(!is_native_archive_name("libfoo.rlib.bak"));
        assert!(!is_native_archive_name("libfoo.so"));
    }

    /// ld64 links a static framework's binary into the output like an
    /// archive; a dynamic framework, or a dir that is no framework, is not
    /// one.
    #[test]
    fn native_dir_static_frameworks_list_archive_binaries() {
        let dir = tempfile::tempdir().unwrap();
        for (framework, bytes) in [
            ("Static", &b"!<arch>\nmembers"[..]),
            ("Dynamic", b"\xcf\xfa\xed\xfe dylib"),
        ] {
            let bundle = dir.path().join(format!("{framework}.framework"));
            std::fs::create_dir_all(&bundle).unwrap();
            std::fs::write(bundle.join(framework), bytes).unwrap();
        }
        std::fs::create_dir_all(dir.path().join("Empty.framework")).unwrap();
        std::fs::create_dir_all(dir.path().join("Other")).unwrap();
        std::fs::write(dir.path().join("Other/Other"), b"!<arch>\nx").unwrap();
        assert_eq!(
            native_dir_static_frameworks(dir.path()).unwrap(),
            [dir.path().join("Static.framework/Static")]
        );
        assert!(
            native_dir_static_frameworks(&dir.path().join("absent"))
                .unwrap()
                .is_empty()
        );
    }

    /// A linking unit hashes the static frameworks in its `framework=` dirs,
    /// and under a system library dir the one a `framework` spec names; an
    /// rlib does not link them.
    #[test]
    fn linking_unit_keys_static_frameworks() {
        let tree = native_tree();
        let normalizer = PathNormalizer::empty();
        let context = NativeLinkContext {
            system_dirs: vec![resolved_path(&tree.elsewhere)],
            ..unix_context("aarch64-apple-darwin", &normalizer)
        };
        let rewrites = |dir: &Path, crate_type: &str, extra: &[&str]| {
            let bundle = dir.join("Foo.framework");
            std::fs::create_dir_all(&bundle).unwrap();
            let argv = unit_args(crate_type, &tree.deps, extra);
            let frameworks = [dir.to_path_buf()];
            fold_rewrites(&bundle.join("Foo"), || {
                fold_inputs(&argv, &[], &frameworks, &context)
            })
        };
        assert!(rewrites(&tree.out, "bin", &[]));
        assert!(!rewrites(&tree.out, "rlib", &[]));
        assert!(!rewrites(&tree.elsewhere, "bin", &[]));
        assert!(rewrites(&tree.elsewhere, "bin", &["-l", "framework=Foo"]));
        assert!(!rewrites(&tree.elsewhere, "rlib", &["-l", "framework=Foo"]));
    }

    /// A dir under a system library dir is scanned only inside the build
    /// tree; every other dir is scanned, once.
    #[test]
    fn scanned_dirs_skip_system_dirs_outside_the_build_tree() {
        let tree = native_tree();
        let normalizer = PathNormalizer::empty();
        let scanned = |system: &Path, build_tree: &[&Path]| {
            let context = NativeLinkContext {
                system_dirs: vec![resolved_path(system)],
                build_tree: build_tree.iter().map(|root| root.to_path_buf()).collect(),
                ..unix_context("x86_64-unknown-linux-gnu", &normalizer)
            };
            context.scanned_dirs(&[tree.out.clone(), tree.elsewhere.clone(), tree.out.clone()])
        };
        let tmp = tree.target.parent().unwrap();
        let target: &Path = &tree.target;
        assert_eq!(scanned(tmp, &[target]), std::slice::from_ref(&tree.out));
        assert!(scanned(tmp, &[]).is_empty());
        assert_eq!(
            scanned(&tree.elsewhere, &[]),
            std::slice::from_ref(&tree.out)
        );
        assert_eq!(
            scanned(&tree.deps, &[]),
            [tree.out.clone(), tree.elsewhere.clone()]
        );
    }

    #[test]
    fn system_library_dirs_hold_os_and_package_manager_prefixes() {
        let dirs = system_library_dirs();
        for dir in ["/usr/lib", "/usr/local/lib", "/opt/homebrew", "/nix/store"] {
            assert!(dirs.contains(&PathBuf::from(dir)), "{dir}");
        }
        for local in [
            "/home/u/mono/build/lib",
            "/usr/src/app/build",
            "/opt/app/lib",
        ] {
            assert!(!is_under_any(Path::new(local), &dirs), "{local}");
        }
    }

    #[test]
    fn is_under_any_compares_whole_components() {
        let roots = [PathBuf::from("/x"), PathBuf::from("/a")];
        assert!(is_under_any(Path::new("/a/b"), &roots));
        assert!(is_under_any(Path::new("/a"), &roots));
        assert!(!is_under_any(Path::new("/ab"), &roots));
        assert!(!is_under_any(Path::new("/a"), &[]));
    }

    /// The bundle audit reads an rlib's scanned `-L` dirs, so a system
    /// library dir outside the build tree cannot refuse every rlib that sees
    /// it. Other units report none.
    #[test]
    fn audit_dirs_are_an_rlibs_scanned_dirs() {
        let tree = native_tree();
        let normalizer = PathNormalizer::empty();
        let context = NativeLinkContext {
            system_dirs: vec![resolved_path(&tree.elsewhere)],
            ..unix_context("x86_64-unknown-linux-gnu", &normalizer)
        };
        let dirs = [tree.out.clone(), tree.elsewhere.clone(), tree.out.clone()];
        let audit_dirs = |crate_type: &str| {
            let argv = unit_args(crate_type, &tree.deps, &[]);
            fold_inputs(&argv, &dirs, &[], &context).unwrap().1.dirs
        };
        assert_eq!(audit_dirs("rlib"), std::slice::from_ref(&tree.out));
        assert!(audit_dirs("bin").is_empty());
    }

    /// rustc hands the value of a `link-arg` `-l` spec to the linker as is,
    /// so a linking unit with one is refused; an rlib only records it.
    #[test]
    fn link_arg_lib_spec_refuses_a_link() {
        let tree = native_tree();
        let normalizer = PathNormalizer::empty();
        let context = unix_context("x86_64-unknown-linux-gnu", &normalizer);
        let fold = |crate_type: &str, spec: &str| {
            let argv = unit_args(crate_type, &tree.deps, &["-l", spec]);
            fold_inputs(&argv, &[], &[], &context)
        };
        assert!(fold("bin", "link-arg=-lfoo").is_err());
        assert!(fold("cdylib", "link-arg:+verbatim=-Wl,-hidden-lfoo").is_err());
        assert!(fold("rlib", "link-arg=-lfoo").is_ok());
        assert!(fold("bin", "dylib=foo").is_ok());

        assert!(is_link_arg_spec("link-arg=-lfoo"));
        assert!(is_link_arg_spec("link-arg:+verbatim=-lfoo"));
        assert!(!is_link_arg_spec("static=link-arg"));
        assert!(!is_link_arg_spec("link-arg"));
        assert!(!is_link_arg_spec("dylib=foo"));
    }

    #[test]
    fn framework_request_reads_framework_specs() {
        assert_eq!(framework_request("framework=Foo"), Some("Foo"));
        assert_eq!(framework_request("framework:-as-needed=Foo"), Some("Foo"));
        assert_eq!(framework_request("framework=Foo:Bar"), Some("Bar"));
        assert_eq!(framework_request("framework="), None);
        assert_eq!(framework_request("dylib=Foo"), None);
        assert_eq!(framework_request("Foo"), None);
    }

    /// ld64 takes the first `Foo.framework/Foo` in its `-F` dirs; only an
    /// `ar` archive there is static.
    #[test]
    fn resolve_static_framework_takes_the_first_bundle() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let dirs = [first.path().to_path_buf(), second.path().to_path_buf()];
        let bundle = |dir: &Path, bytes: &[u8]| {
            let bundle = dir.join("Foo.framework");
            std::fs::create_dir_all(&bundle).unwrap();
            std::fs::write(bundle.join("Foo"), bytes).unwrap();
            bundle.join("Foo")
        };
        assert_eq!(resolve_static_framework("Foo", &dirs).unwrap(), None);
        std::fs::create_dir_all(first.path().join("Foo.framework")).unwrap();
        let later = bundle(second.path(), b"!<arch>\nstatic");
        assert_eq!(
            resolve_static_framework("Foo", &dirs).unwrap(),
            Some(later),
            "a bundle with no binary is skipped"
        );
        bundle(first.path(), b"\xcf\xfa\xed\xfe dylib");
        assert_eq!(
            resolve_static_framework("Foo", &dirs).unwrap(),
            None,
            "a dynamic framework first hides a later static one"
        );
        let static_first = bundle(first.path(), b"!<arch>\nstatic");
        assert_eq!(
            resolve_static_framework("Foo", &dirs).unwrap(),
            Some(static_first)
        );
    }

    /// Each scanned archive folds under its dir index and its path in the
    /// dir; a thin archive, whose members live elsewhere, refuses the key.
    #[test]
    fn fold_native_dir_archives_names_dir_index_and_file() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        std::fs::write(first.path().join("liba.a"), b"a").unwrap();
        std::fs::write(second.path().join("libb.a"), b"b").unwrap();
        let dirs = [first.path().to_path_buf(), second.path().to_path_buf()];
        let fold = |dirs: &[PathBuf]| {
            let mut hasher = blake3::Hasher::new();
            let hashed = fold_native_dir_archives(&mut hasher, dirs, native_dir_archives, |path| {
                Ok(std::fs::read_to_string(path)?)
            })
            .unwrap();
            (hasher.finalize(), hashed)
        };
        let mut expected = blake3::Hasher::new();
        fold_field(&mut expected, b"native_dir_archive.v1:", b"0/liba.a=a");
        fold_field(&mut expected, b"native_dir_archive.v1:", b"1/libb.a=b");
        let (digest, hashed) = fold(&dirs);
        assert_eq!(digest, expected.finalize());
        assert_eq!(
            hashed,
            [first.path().join("liba.a"), second.path().join("libb.a")]
        );
        assert_eq!(fold(&[]).0, blake3::Hasher::new().finalize());

        // A framework binary names its bundle too.
        let bundle = first.path().join("Foo.framework");
        std::fs::create_dir_all(&bundle).unwrap();
        std::fs::write(bundle.join("Foo"), b"!<arch>\nf").unwrap();
        let mut hasher = blake3::Hasher::new();
        fold_native_dir_archives(
            &mut hasher,
            &dirs[..1],
            native_dir_static_frameworks,
            |_| Ok("d".to_string()),
        )
        .unwrap();
        let mut expected = blake3::Hasher::new();
        let binary = Path::new("Foo.framework").join("Foo");
        fold_field(
            &mut expected,
            b"native_dir_archive.v1:",
            format!("0/{}=d", binary.display()).as_bytes(),
        );
        assert_eq!(hasher.finalize(), expected.finalize());

        std::fs::write(first.path().join("libthin.a"), b"!<thin>\n").unwrap();
        let mut hasher = blake3::Hasher::new();
        let thin = fold_native_dir_archives(&mut hasher, &dirs, native_dir_archives, |path| {
            FileHasher::new().hash_static_lib(path)
        });
        assert!(thin.is_err(), "a thin archive refuses the key");
    }

    /// An rlib or staticlib leaves a `-bundle` archive out of its output, so
    /// the archive must not key it by its spec; the unit that links it later
    /// does.
    #[test]
    fn unbundled_static_lib_keys_only_the_unit_that_links_it() {
        let tree = native_tree();
        let normalizer = PathNormalizer::empty();
        let context = unix_context("x86_64-unknown-linux-gnu", &normalizer);
        let dirs = [tree.elsewhere.clone()];
        let archive = tree.elsewhere.join("libfoo.a");
        let rewrites = |crate_type: &str, spec: &str| {
            let argv = unit_args(crate_type, &tree.deps, &["-l", spec]);
            fold_rewrites(&archive, || fold_inputs(&argv, &dirs, &[], &context))
        };
        assert!(!rewrites("rlib", "static:-bundle=foo"));
        assert!(rewrites("rlib", "static:-bundle,+bundle=foo"));
        assert!(rewrites("bin", "static:-bundle=foo"));
    }

    /// A Unix linker looks for `-l foo` or `-l dylib=foo` in the `-L` dirs,
    /// then in its default dirs (`/usr/local/lib`, ...), and takes
    /// `libfoo.a` when the first dir that has the name holds no shared
    /// library, or always under `+crt-static`.
    #[test]
    fn unix_link_keys_a_library_only_a_default_dir_holds() {
        let tree = native_tree();
        let normalizer = PathNormalizer::empty();
        let archive = tree.elsewhere.join("libfoo.a");
        let fold_with = |crate_type: &str, spec: &str, extra: &[&str], target: &str| {
            let mut flags = vec!["-l", spec];
            flags.extend_from_slice(extra);
            let argv = unit_args(crate_type, &tree.deps, &flags);
            let context = NativeLinkContext {
                default_dirs: vec![tree.elsewhere.clone()],
                ..unix_context(target, &normalizer)
            };
            let native = [tree.out.clone()];
            fold_rewrites(&archive, || fold_inputs(&argv, &native, &[], &context))
        };
        let gnu = "x86_64-unknown-linux-gnu";
        assert!(fold_with("bin", "foo", &[], gnu));
        assert!(fold_with("bin", "dylib=foo", &[], gnu));
        assert!(!fold_with("rlib", "foo", &[], gnu), "an rlib does not link");

        std::fs::write(tree.elsewhere.join("libfoo.so"), b"shared").unwrap();
        assert!(
            !fold_with("bin", "foo", &[], gnu),
            "the shared library wins in its dir"
        );
        assert!(fold_with(
            "bin",
            "foo",
            &["-Ctarget-feature=+crt-static"],
            gnu
        ));
        assert!(
            fold_with("bin", "foo", &[], "x86_64-unknown-linux-musl"),
            "musl links statically by default"
        );
        std::fs::remove_file(tree.elsewhere.join("libfoo.so")).unwrap();

        // A shared library in a `-L` dir comes first.
        std::fs::write(tree.out.join("libfoo.so"), b"shared").unwrap();
        assert!(!fold_with("bin", "foo", &[], gnu));
    }

    /// A library a link argument names is looked up like `-l`, and its
    /// first archive counts even beside a shared library, since the link
    /// arguments may say `-Bstatic`.
    #[test]
    fn unix_link_keys_libraries_named_in_link_arguments() {
        let tree = native_tree();
        let normalizer = PathNormalizer::empty();
        let context = NativeLinkContext {
            default_dirs: vec![tree.elsewhere.clone()],
            ..unix_context("x86_64-unknown-linux-gnu", &normalizer)
        };
        std::fs::write(tree.elsewhere.join("libfoo.so"), b"shared").unwrap();
        let rewrites = |crate_type: &str, flag: &str, file: &Path| {
            let argv = unit_args(crate_type, &tree.deps, &[flag]);
            fold_rewrites(file, || fold_inputs(&argv, &[], &[], &context))
        };
        let archive = tree.elsewhere.join("libfoo.a");
        assert!(rewrites(
            "bin",
            "-Clink-arg=-Wl,-Bstatic,-lfoo,-Bdynamic",
            &archive
        ));
        assert!(rewrites("bin", "-Clink-arg=-l:libfoo.a", &archive));
        assert!(!rewrites("rlib", "-Clink-arg=-lfoo", &archive));
        assert!(!rewrites(
            "bin",
            "-Clink-arg=-lbar",
            &tree.elsewhere.join("libbaz.a")
        ));
    }

    #[test]
    fn resolve_unix_library_follows_the_first_dir_with_a_candidate() {
        let first = PathBuf::from("/one");
        let second = PathBuf::from("/two");
        let dirs = [first.clone(), second.clone()];
        let all = ["so", "dylib", "tbd"];
        let resolve = |present: &[&str], name: &str, verbatim: bool, prefer_static: bool| {
            resolve_unix_library(name, verbatim, &dirs, prefer_static, &all, |path| {
                present.iter().any(|file| path == Path::new(file))
            })
        };
        let archive_in = |dir: &Path| Some(dir.join("libfoo.a"));

        assert_eq!(
            resolve(&["/two/libfoo.a"], "foo", false, false),
            archive_in(&second)
        );
        assert_eq!(
            resolve(&["/one/libfoo.a", "/two/libfoo.a"], "foo", false, false),
            archive_in(&first)
        );
        assert_eq!(resolve(&[], "foo", false, false), None);
        for shared in ["/one/libfoo.so", "/one/libfoo.dylib", "/one/libfoo.tbd"] {
            assert_eq!(
                resolve(&[shared, "/one/libfoo.a"], "foo", false, false),
                None,
                "{shared}"
            );
            assert_eq!(
                resolve(&[shared, "/one/libfoo.a"], "foo", false, true),
                archive_in(&first),
                "{shared} under a static link"
            );
        }
        // A shared library alone in the first dir hides a later archive,
        // except from a static link, which only looks for archives.
        let hidden = ["/one/libfoo.so", "/two/libfoo.a"];
        assert_eq!(resolve(&hidden, "foo", false, false), None);
        assert_eq!(resolve(&hidden, "foo", false, true), archive_in(&second));

        assert_eq!(
            resolve(&["/two/foo.a"], "foo.a", true, false),
            Some(second.join("foo.a"))
        );
        assert_eq!(resolve(&["/one/libfoo.so"], "libfoo.so", true, false), None);

        // An ELF linker does not read a stray `.dylib` beside the archive.
        let elf = resolve_unix_library(
            "foo",
            false,
            &dirs,
            false,
            shared_library_extensions("x86_64-unknown-linux-gnu"),
            |path| path == Path::new("/one/libfoo.dylib") || path == Path::new("/one/libfoo.a"),
        );
        assert_eq!(elf, archive_in(&first));
    }

    #[test]
    fn shared_library_extensions_follow_the_object_format() {
        assert_eq!(
            shared_library_extensions("aarch64-apple-darwin"),
            ["tbd", "dylib", "so"]
        );
        assert_eq!(
            shared_library_extensions("x86_64-unknown-linux-gnu"),
            ["so"]
        );
    }

    #[test]
    fn static_link_preference_follows_crt_static() {
        let gnu = "x86_64-unknown-linux-gnu";
        let musl = "x86_64-unknown-linux-musl";
        assert!(!prefers_static_libraries(&[], gnu));
        assert!(prefers_static_libraries(&["+crt-static"], gnu));
        assert!(prefers_static_libraries(&["+sse2, +crt-static"], gnu));
        assert!(!prefers_static_libraries(
            &["+crt-static", "-crt-static"],
            gnu
        ));
        assert!(prefers_static_libraries(&[], musl));
        assert!(!prefers_static_libraries(&["-crt-static"], musl));
        assert!(prefers_static_libraries(&["+sse2"], musl));
    }

    /// With no `--target`, a link is for the wrapped rustc's host, as `rustc
    /// -vV` reports it, not for whatever kache was built for: a musl host
    /// links statically by default.
    #[test]
    fn link_target_defaults_to_the_wrapped_rustc_host() {
        let version = "rustc 1.98.0\nhost: x86_64-unknown-linux-musl\nrelease: 1.98.0\n";
        let host = rustc_host_triple(version);
        assert_eq!(link_target(None, host), "x86_64-unknown-linux-musl");
        assert!(prefers_static_libraries(&[], link_target(None, host)));
        assert_eq!(
            link_target(Some("aarch64-unknown-linux-gnu"), host),
            "aarch64-unknown-linux-gnu"
        );
        assert_eq!(link_target(None, None), "unknown");
    }

    /// A host-native Unix link searches `LIBRARY_PATH` and then the system
    /// and local install dirs; a cross link and other targets search none of
    /// them.
    #[test]
    fn default_library_dirs_follow_a_host_native_unix_target() {
        let linux = "x86_64-unknown-linux-gnu";
        let dirs = |target: &str, host: &str, library_path: Option<&str>| {
            default_library_dirs(target, Some(host), library_path.map(std::ffi::OsStr::new))
        };
        let expected: Vec<PathBuf> = [
            "/usr/local/lib/x86_64-linux-gnu",
            "/lib/x86_64-linux-gnu",
            "/usr/lib/x86_64-linux-gnu",
            "/usr/local/lib64",
            "/lib64",
            "/usr/lib64",
            "/usr/local/lib",
            "/lib",
            "/usr/lib",
        ]
        .map(PathBuf::from)
        .into();
        assert_eq!(dirs(linux, linux, None), expected);
        #[cfg(unix)]
        {
            let mut with_path = vec![PathBuf::from("/opt/a"), PathBuf::from("/opt/b")];
            with_path.extend(expected.clone());
            assert_eq!(dirs(linux, linux, Some("/opt/a::/opt/b")), with_path);
        }
        let apple = "aarch64-apple-darwin";
        assert_eq!(
            dirs(apple, apple, None),
            [PathBuf::from("/usr/lib"), PathBuf::from("/usr/local/lib")]
        );
        assert!(dirs("aarch64-unknown-linux-gnu", linux, None).is_empty());
        assert!(default_library_dirs(linux, None, None).is_empty());
        let windows = "x86_64-pc-windows-gnu";
        assert!(dirs(windows, windows, Some("/opt/a")).is_empty());
        assert_eq!(
            multiarch_name("aarch64-unknown-linux-gnu"),
            "aarch64-linux-gnu"
        );
        assert_eq!(
            multiarch_name("x86_64-linux-android"),
            "x86_64-linux-android"
        );
    }

    #[test]
    fn unix_library_request_reads_kindless_and_dylib_specs() {
        assert_eq!(unix_library_request("foo"), Some(("foo", false)));
        assert_eq!(unix_library_request("dylib=foo"), Some(("foo", false)));
        assert_eq!(
            unix_library_request("dylib:-as-needed=foo"),
            Some(("foo", false))
        );
        assert_eq!(
            unix_library_request("dylib:+verbatim=libfoo.so"),
            Some(("libfoo.so", true))
        );
        assert_eq!(
            unix_library_request("dylib:+verbatim,-verbatim=foo"),
            Some(("foo", false))
        );
        assert_eq!(unix_library_request("static=foo"), None);
        assert_eq!(unix_library_request("framework=Foo"), None);
        assert_eq!(unix_library_request("dylib="), None);
    }

    /// Files a link argument names are linker inputs: rebuilding one in place
    /// must re-key the binary.
    #[cfg(not(windows))]
    #[test]
    fn unix_link_keys_files_named_in_link_arguments() {
        let _lock = key_test_lock();
        let tree = native_tree();
        let archive = tree.elsewhere.join("libfoo.a");
        let object = tree.elsewhere.join("extra.o");
        let a = archive.display().to_string();
        let o = object.display().to_string();
        for (flag, file) in [
            (format!("-Clink-arg={a}"), &archive),
            (format!("-Clink-arg=-Wl,--whole-archive,{a}"), &archive),
            (format!("-Clink-args=-force_load {a}"), &archive),
            (format!("-Clink-arg={o}"), &object),
        ] {
            let bin = unit_args("bin", &tree.deps, &[&flag]);
            let (before, after) = keys_around_rewrite(&bin, file);
            assert_ne!(before, after, "{flag}");
            let rlib = unit_args("rlib", &tree.deps, &[&flag]);
            let (before, after) = keys_around_rewrite(&rlib, file);
            assert_eq!(before, after, "an rlib does not link: {flag}");
        }

        // An archive is hashed as one, so a thin archive refuses the key; any
        // other input is hashed as a file, whatever its bytes.
        std::fs::write(tree.elsewhere.join("libthin.a"), b"!<thin>\n").unwrap();
        std::fs::write(tree.elsewhere.join("thin.o"), b"!<thin>\n").unwrap();
        let thin_object = format!("-Clink-arg={}", tree.elsewhere.join("thin.o").display());
        assert!(try_key_of_flags(&unit_args("bin", &tree.deps, &[&thin_object])).is_ok());

        for flag in [
            format!("-Clink-arg={}", tree.elsewhere.join("absent.a").display()),
            "-Clink-arg=relative/libfoo.a".to_string(),
            format!("-Clink-arg={}", tree.elsewhere.join("libthin.a").display()),
        ] {
            assert!(
                try_key_of_flags(&unit_args("bin", &tree.deps, &[&flag])).is_err(),
                "{flag}"
            );
        }
    }

    /// A linking unit's `static` lib that no `-L` dir holds may come from a
    /// system dir the key never sees. A `-L` in the link arguments counts.
    #[cfg(not(windows))]
    #[test]
    fn unresolved_static_lib_refuses_a_linking_unit() {
        let _lock = key_test_lock();
        let tree = native_tree();
        let error =
            try_key_of_flags(&unit_args("bin", &tree.deps, &["-l", "static=nope"])).unwrap_err();
        assert!(
            format!("{error:#}").contains("is in no -L directory"),
            "{error:#}"
        );
        assert!(try_key_of_flags(&unit_args("rlib", &tree.deps, &["-l", "static=nope"])).is_ok());

        let link_search = format!("-Clink-arg=-L{}", tree.elsewhere.display());
        let bin = unit_args("bin", &tree.deps, &[&link_search, "-l", "static=nope"]);
        let (before, after) = keys_around_rewrite(&bin, &tree.elsewhere.join("libnope.a"));
        assert_ne!(before, after, "found through the link argument and keyed");
    }

    #[test]
    fn unresolved_static_lib_is_an_error_only_for_non_msvc_links() {
        assert!(unresolved_static_lib_is_error(true, false));
        assert!(!unresolved_static_lib_is_error(true, true));
        assert!(!unresolved_static_lib_is_error(false, false));
    }

    #[test]
    fn packed_bundled_libs_flag_is_read_in_either_spelling() {
        let flags = |items: &[&str]| {
            items
                .iter()
                .map(|flag| flag.to_string())
                .collect::<Vec<_>>()
        };
        assert!(packs_bundled_libs(&flags(&["packed-bundled-libs"])));
        assert!(packs_bundled_libs(&flags(&[
            "share-generics",
            "packed_bundled_libs=yes"
        ])));
        assert!(!packs_bundled_libs(&flags(&[
            "share-generics=packed-bundled-libs"
        ])));
        assert!(!packs_bundled_libs(&[]));
    }

    /// An rlib carries a `static` spec's archive unless `-bundle`; it packs
    /// one under `+whole-archive` or `-Z packed-bundled-libs`.
    #[test]
    fn rlib_bundle_follows_bundle_and_packing() {
        let spec = static_lib_spec;
        assert_eq!(rlib_bundle(&spec("static=foo"), true, false), Some(false));
        assert_eq!(
            rlib_bundle(&spec("static:+whole-archive=foo"), true, false),
            Some(true)
        );
        assert_eq!(rlib_bundle(&spec("static=foo"), true, true), Some(true));
        assert_eq!(rlib_bundle(&spec("static:-bundle=foo"), true, false), None);
        assert_eq!(rlib_bundle(&spec("static=foo"), false, false), None);
        assert_eq!(rlib_bundle(&spec("foo"), true, false), None);
    }

    /// The bundle-audit marker keys rlibs with a native dir apart from
    /// entries stored without the audit, and the stash reports what the key
    /// hashed and what the rlib carries.
    ///
    /// A dir outside the workspace counts like one inside it (see
    /// [`audit_dirs_are_an_rlibs_scanned_dirs`] for system library dirs).
    #[test]
    fn native_bundle_audit_marks_rlibs_with_a_native_dir() {
        let _lock = key_test_lock();
        let tree = native_tree();
        std::fs::write(tree.out.join("libfoo.a"), b"archive").unwrap();
        std::fs::write(tree.elsewhere.join("libbar.a"), b"archive").unwrap();
        let search = format!("native={}", tree.out.display());
        let outside = format!("native={}", tree.elsewhere.display());
        let marked = |crate_type: &str, extra: &[&str]| {
            key_of_flags(&unit_args(crate_type, &tree.deps, extra));
            take_last_key_fields()
                .unwrap()
                .contains_key("native_bundle_audit")
        };
        assert!(marked("rlib", &["-L", &search]));
        assert!(!marked("rlib", &[]));
        assert!(marked("rlib", &["-L", &outside]));
        assert!(!marked("rlib", &["-L", &search, "--emit=metadata"]));
        #[cfg(not(windows))]
        assert!(!marked("bin", &["-L", &search]));

        key_of_flags(&unit_args(
            "rlib",
            &tree.deps,
            &[
                "-L",
                &search,
                "-L",
                &outside,
                "-L",
                &search,
                "-l",
                "static:+whole-archive=foo",
                "-l",
                "static=bar",
            ],
        ));
        assert_eq!(
            take_last_key_native_archives(),
            Some(KeyedNativeArchives {
                archives: vec![tree.out.join("libfoo.a"), tree.elsewhere.join("libbar.a")],
                bundled: vec![
                    BundledArchive {
                        path: tree.out.join("libfoo.a"),
                        packed: true,
                    },
                    BundledArchive {
                        path: tree.elsewhere.join("libbar.a"),
                        packed: false,
                    },
                ],
                dirs: vec![tree.out.clone(), tree.elsewhere.clone()],
            })
        );
        assert_eq!(take_last_key_native_archives(), None, "taken once");
    }

    /// Rustc's `-O` / `-g` shorthands must share keys with their exact `-C`
    /// equivalents rather than living in the unmodeled residual bucket.
    #[test]
    fn codegen_shorthands_match_explicit_forms() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let base = key_of_flags(&flag_base(&source, &[]));
        let debug = key_of_flags(&flag_base(&source, &["-g"]));
        let opt = key_of_flags(&flag_base(&source, &["-O"]));
        let explicit_debug = key_of_flags(&flag_base(&source, &["-Cdebuginfo=2"]));
        let explicit_opt = key_of_flags(&flag_base(&source, &["-Copt-level=3"]));
        let long_debug = key_of_flags(&flag_base(&source, &["--codegen=debuginfo=2"]));
        let long_opt = key_of_flags(&flag_base(&source, &["--codegen", "opt-level=3"]));
        assert_ne!(base, debug, "`-g` must change the key");
        assert_ne!(base, opt, "`-O` must change the key");
        assert_ne!(debug, opt, "`-g` and `-O` must produce distinct keys");
        assert_eq!(debug, explicit_debug, "`-g` is `-Cdebuginfo=2`");
        assert_eq!(opt, explicit_opt, "`-O` is `-Copt-level=3`");
        assert_eq!(debug, long_debug, "`--codegen` is the long `-C` alias");
        assert_eq!(opt, long_opt, "separated `--codegen` must match `-C`");
    }

    #[test]
    fn codegen_shorthand_override_order_changes_key() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let shorthand_then_explicit =
            key_of_flags(&flag_base(&source, &["-O", "--codegen=opt-level=0"]));
        let explicit_then_shorthand =
            key_of_flags(&flag_base(&source, &["--codegen", "opt-level=0", "-O"]));

        assert_ne!(
            shorthand_then_explicit, explicit_then_shorthand,
            "rustc applies optimization flags last-wins, so opposite orders must not collide"
        );
    }

    #[test]
    fn frontend_jobs_spellings_share_a_key_and_values_remain_ordered() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let none = key_of_flags(&flag_base(&source, &[]));
        let separated = key_of_flags(&flag_base(&source, &["--jobs-frontend", "16"]));
        let attached = key_of_flags(&flag_base(&source, &["--jobs-frontend=16"]));
        let different = key_of_flags(&flag_base(&source, &["--jobs-frontend=8"]));
        let order_4_8 = key_of_flags(&flag_base(
            &source,
            &["--jobs-frontend=4", "--jobs-frontend=8"],
        ));
        let order_8_4 = key_of_flags(&flag_base(
            &source,
            &["--jobs-frontend=8", "--jobs-frontend=4"],
        ));

        assert_ne!(none, attached, "frontend jobs must affect the key");
        assert_eq!(separated, attached, "both rustc spellings are equivalent");
        assert_ne!(attached, different, "worker count must affect the key");
        assert_ne!(
            order_4_8, order_8_4,
            "repeated last-wins values must preserve argv order"
        );
    }

    #[test]
    fn response_file_flags_share_inline_key_and_track_contents() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();
        let response = dir.path().join("rustc.args");
        let at_response = format!("@{}", response.display());

        std::fs::write(&response, "--cfg\nresponse_v1\n-C\nopt-level=1\n").unwrap();
        let inline = key_of_flags(&flag_base(
            &source,
            &["--cfg", "response_v1", "-C", "opt-level=1"],
        ));
        let response_v1 = key_of_flags(&flag_base(&source, &[&at_response]));
        assert_eq!(
            response_v1, inline,
            "transporting identical flags through @file must not change the key"
        );

        std::fs::write(&response, "--cfg\nresponse_v2\n-C\nopt-level=2\n").unwrap();
        let response_v2 = key_of_flags(&flag_base(&source, &[&at_response]));
        assert_ne!(
            response_v1, response_v2,
            "rewriting the same response-file path must change the effective key"
        );
    }

    /// kunobi-ninja/kache#324: residual tokens are sorted before folding, so
    /// argv order does not perturb the key (the fold is a coarse safety net for
    /// unmodeled flags, not an order-sensitive channel).
    #[test]
    fn residual_args_are_order_independent() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let a = key_of_flags(&flag_base(
            &source,
            &["--unmodeled-a", "alpha", "--unmodeled-b", "beta"],
        ));
        let b = key_of_flags(&flag_base(
            &source,
            &["--unmodeled-b", "beta", "--unmodeled-a", "alpha"],
        ));
        assert_eq!(a, b, "residual argv order must not change the key");
    }

    /// kunobi-ninja/kache#324: diagnostics / lint / query / already-keyed path
    /// flags are stripped during parsing, so they must NOT reach the residual
    /// fold and over-key the result. Guards the same invariant as the
    /// `key_matrix_*_does_not_change_key` tests for flags cargo passes routinely.
    ///
    /// Outcome-affecting lint configuration is deliberately NOT in this list:
    /// every lint level and `--check-cfg` must change the key —
    /// see `key_matrix_outcome_lint_configuration_changes_key`.
    #[test]
    fn residual_strips_diagnostic_and_query_flags() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let base = key_of_flags(&flag_base(&source, &[]));
        for extra in [
            vec!["--diagnostic-width=80"],
            vec!["--json=artifacts"],
            vec!["--color", "always"],
            vec!["--verbose"],
        ] {
            assert_eq!(
                base,
                key_of_flags(&flag_base(&source, &extra)),
                "diagnostics/query flag {extra:?} must not change the key"
            );
        }
    }

    /// kunobi-ninja/kache#399: lexical `.`/`..` collapse + separator unification.
    /// Output uses the host separator (so it lines up with canonical rule
    /// prefixes); built with MAIN_SEPARATOR so the test is host-independent.
    #[test]
    fn lexically_resolve_path_collapses_dot_dot() {
        let s = std::path::MAIN_SEPARATOR_STR;
        let j = |parts: &[&str]| parts.join(s);

        // Unix-style absolute.
        assert_eq!(lexically_resolve_path("/a/b/../c"), j(&["", "a", "c"]));
        assert_eq!(lexically_resolve_path("/a/./b"), j(&["", "a", "b"]));
        // `..` cannot escape the root.
        assert_eq!(lexically_resolve_path("/../a"), j(&["", "a"]));

        // Windows drive + mixed separators + unresolved `..` (the #399 input
        // shape: a relative CARGO_TARGET_DIR joined literally with `/`).
        assert_eq!(
            lexically_resolve_path(r"C:\proj\pkg\..\oot-target\x"),
            format!("C:{}", j(&["", "proj", "oot-target", "x"]))
        );
        assert_eq!(
            lexically_resolve_path(r"C:\u\src\../oot-target\rel\deps"),
            format!("C:{}", j(&["", "u", "oot-target", "rel", "deps"]))
        );

        // Relative paths keep a leading `..`.
        assert_eq!(lexically_resolve_path("../a/b"), j(&["..", "a", "b"]));
        assert_eq!(lexically_resolve_path("."), ".");

        // Already-resolved paths are unchanged on the host (the Linux case).
        let resolved = format!("{}home{}u{}oot{}out", s, s, s, s);
        assert_eq!(lexically_resolve_path(&resolved), resolved);
    }

    /// kunobi-ninja/kache#399 (the core property): two out-of-tree build paths
    /// that differ only in the package-dir component cancelled by `..` resolve
    /// so the suffix below their respective roots is identical. That suffix is
    /// what survives after the workspace-root prefix is stripped, so the cache
    /// key converges across build locations.
    #[test]
    fn lexically_resolve_path_makes_out_of_tree_suffix_converge() {
        // Cold and a relocate (different drive subtree, different package-dir
        // name) of the same out-of-tree build. The `..` cancels the package dir,
        // so oot-target attaches directly to the package's parent (= the
        // workspace root). The segment from oot-target onward is then identical,
        // which is what survives after the <WORKSPACE> prefix is stripped.
        let cold =
            lexically_resolve_path(r"C:\proj\scenario\source\..\oot-target\release\build\x\out");
        let reloc = lexically_resolve_path(r"C:\Temp\.tmpAB\..\oot-target\release\build\x\out");
        assert!(!cold.contains(".."), "unresolved .. in {cold}");
        assert!(!reloc.contains(".."), "unresolved .. in {reloc}");
        let from_oot = |p: &str| p[p.find("oot-target").unwrap()..].to_string();
        assert_eq!(from_oot(&cold), from_oot(&reloc));
        let s = std::path::MAIN_SEPARATOR_STR;
        assert_eq!(
            from_oot(&cold),
            ["oot-target", "release", "build", "x", "out"].join(s)
        );
    }

    /// kunobi-ninja/kache#399 end-to-end at the env-dep layer: an out-of-tree
    /// OUT_DIR that arrives with an unresolved `..` (as Windows cargo leaves it
    /// for a relative CARGO_TARGET_DIR) must normalize to the same
    /// <WORKSPACE>-anchored sentinel regardless of absolute build location, so
    /// the key converges and a relocated build hits. Regression for the bug
    /// where `normalize_env_dep_value` returned `Unchanged` before resolving:
    /// the raw `..`-bearing path matched no canonical rule prefix, so the build
    /// location leaked into the key and only the resolved-then-normalized form
    /// (matching the rules' own `canonical_string`) converges.
    #[test]
    fn out_of_tree_out_dir_env_dep_converges_across_locations() {
        let _lock = key_test_lock();

        // Build a real out-of-tree OUT_DIR under `root` with a `..` in the
        // path (root/pkg/../oot-target/...) and return its normalized env-dep
        // value, anchoring <WORKSPACE> at the oot-target dir.
        fn normalized(root: &std::path::Path) -> String {
            let target = root.join("oot-target");
            let out = target
                .join("release")
                .join("build")
                .join("pkg-0000000000000000")
                .join("out");
            std::fs::create_dir_all(&out).unwrap();
            // `pkg` must exist for canonicalize to traverse `pkg/..`.
            std::fs::create_dir_all(root.join("pkg")).unwrap();
            let generated = out.join("generated.rs");
            // Path-only include payload (no `env!("OUT_DIR")` runtime use), and
            // a crate root whose include proves the locator use, so the value
            // is safe to normalize.
            std::fs::write(&generated, b"pub fn marker() -> u8 { 7 }\n").unwrap();
            let lib = root.join("pkg").join("lib.rs");
            std::fs::write(
                &lib,
                r#"include!(concat!(env!("OUT_DIR"), "/generated.rs"));"#,
            )
            .unwrap();

            // The value as Windows cargo hands it over: the package dir is
            // cancelled by a literal `..` rather than pre-resolved.
            let value = root
                .join("pkg")
                .join("..")
                .join("oot-target")
                .join("release")
                .join("build")
                .join("pkg-0000000000000000")
                .join("out")
                .to_string_lossy()
                .into_owned();

            let pn = PathNormalizer::from_env(Some(&target));
            normalize_env_dep_value("test_crate", "OUT_DIR", &value, &[lib, generated], &pn).value
        }

        let cold = tempfile::tempdir().unwrap();
        let reloc = tempfile::tempdir().unwrap();
        let v_cold = normalized(cold.path());
        let v_reloc = normalized(reloc.path());

        assert_eq!(
            v_cold, v_reloc,
            "out-of-tree OUT_DIR must normalize identically across build locations"
        );
        assert!(
            v_cold.contains("<WORKSPACE>"),
            "expected the workspace sentinel, got `{v_cold}`"
        );
        assert!(
            !v_cold.contains(".."),
            "the `..` must be resolved away, got `{v_cold}`"
        );
    }

    /// H1: a build-script native search path must diverge the key, but
    /// cargo's redundant `-L dependency=` (covered by content-hashed
    /// `--extern`) must NOT — else every target-dir move busts the cache.
    /// Generic `-l` keying, checked on hosts that do not probe native MSVC
    /// inputs. On a Windows host the library must exist (fail closed), so the
    /// Windows shape lives in the `windows_*` tests with an injected probe.
    #[cfg(not(windows))]
    #[test]
    fn link_search_native_keys_but_dependency_does_not() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let a = key_of(&flag_base(&source, &["-L", "native=/opt/a/lib"]));
        let b = key_of(&flag_base(&source, &["-L", "native=/opt/b/lib"]));
        assert_ne!(a, b, "a different native -L must change the key");

        let dep_x = key_of(&flag_base(&source, &["-L", "dependency=/x/deps"]));
        let dep_y = key_of(&flag_base(&source, &["-L", "dependency=/y/deps"]));
        assert_eq!(
            dep_x, dep_y,
            "cargo's -L dependency= must not affect the key"
        );
    }

    /// With Cargo's new build-dir layout, a `-sys` crate's
    /// `cargo:rustc-link-search=native=$OUT_DIR` reaches every dependent as
    /// `<target>/<profile>/build/<pkg>/<hash>/out`. The target directory
    /// derived from the dependent's own `--out-dir` must cover it, or the
    /// checkout path stays in the key and the dependent misses in every other
    /// checkout. The unit is an rlib, as such dependents usually are: a linked
    /// output would run the native Windows MSVC link probe, which fails closed
    /// on the made-up search directories.
    #[test]
    fn link_search_into_new_layout_out_dir_is_the_same_in_every_checkout() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let key_in = |checkout: &str| {
            let target = checkout_target(checkout);
            let out_dir = format!("{target}/debug/build/mylib/0123456789abcdef/out");
            let native = format!("native={target}/debug/build/foo-sys/fedcba9876543210/out");
            let argv: Vec<String> = [
                "rustc",
                "--crate-name",
                "mylib",
                &source.to_string_lossy(),
                "--crate-type",
                "lib",
                "--out-dir",
                &out_dir,
                "-L",
                &native,
            ]
            .iter()
            .map(|s| s.to_string())
            .collect();
            let args = RustcArgs::parse(&argv).unwrap();
            let pn = PathNormalizer::empty().with_target_dir(args.target_dir().as_deref());
            compute_cache_key(&args, &FileHasher::new(), &pn).unwrap()
        };

        assert_eq!(key_in("checkout-a"), key_in("checkout-b"));
    }

    /// Executable (`bin`) outputs key the linker identity (a different linker
    /// can produce a different binary). A resolvable `-Clinker` is folded in;
    /// an unresolvable one isn't. Exercises compute_cache_key's
    /// is_executable_output() linker branch (716-723) + get_linker_identity.
    ///
    /// Unix-only: the test relies on `cc` resolving on PATH (it folds `cc
    /// --version`), which isn't guaranteed on the Windows CI runner — there
    /// both linkers fail to resolve and the keys match. The branch is still
    /// covered on Linux/macOS CI.
    #[cfg(unix)]
    #[test]
    fn bin_output_keys_linker_identity() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("main.rs");
        std::fs::write(&source, b"fn main() {}").unwrap();
        let bin = |extra: &[&str]| {
            let mut v = vec![
                "rustc".to_string(),
                "--crate-name".to_string(),
                "app".to_string(),
                source.to_string_lossy().to_string(),
                "--crate-type".to_string(),
                "bin".to_string(),
            ];
            v.extend(extra.iter().map(|s| s.to_string()));
            v
        };

        // A resolvable linker (cc on PATH) folds its version and, on Linux,
        // CRT identity. An unresolvable linker cannot place CRT/startup
        // objects, so the invocation is uncacheable rather than keyed with
        // an empty runtime identity.
        let fh = FileHasher::new();
        let pn = PathNormalizer::empty();
        let key = |args: Vec<String>| {
            let mut parsed = RustcArgs::parse(&args).unwrap();
            parsed.source_file = None;
            compute_cache_key(&parsed, &fh, &pn)
        };
        let with_cc = key(bin(&["-Clinker=cc"]));
        let with_missing = key(bin(&["-Clinker=/nonexistent/kache-linker-xyz"]));
        match (with_cc, with_missing) {
            (Ok(cc), Ok(missing)) => {
                assert_ne!(cc, missing, "linker choice must affect a bin's cache key");
                assert_eq!(cc, key(bin(&["-Clinker=cc"])).unwrap());
            }
            (Ok(_), Err(_)) => {}
            (Err(_), Err(_)) => {}
            (Err(err), Ok(_)) => {
                panic!("unresolvable linker produced a key while cc failed: {err:#}")
            }
        }
    }

    /// A readable `--extern name=path` rlib is content-hashed into the key (not
    /// path-hashed): the same path with different artifact bytes must diverge.
    /// Exercises compute_cache_key's extern Ok(dep_hash) branch (552-557).
    #[test]
    fn extern_artifact_content_changes_key() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();
        let dep = dir.path().join("libdep.rlib");
        let extern_arg = format!("foo={}", dep.to_str().unwrap());

        std::fs::write(&dep, b"rlib content A").unwrap();
        let key_a = key_of_flags(&flag_base(&source, &["--extern", &extern_arg]));

        std::fs::write(&dep, b"rlib content B (different)").unwrap();
        let key_b = key_of_flags(&flag_base(&source, &["--extern", &extern_arg]));
        assert_ne!(
            key_a, key_b,
            "extern artifact content must change the key (content-hashed)"
        );

        std::fs::write(&dep, b"rlib content A").unwrap();
        let key_a2 = key_of_flags(&flag_base(&source, &["--extern", &extern_arg]));
        assert_eq!(key_a, key_a2, "same extern content -> same key");
    }

    /// H1: `-Z` codegen flags arriving on argv must be keyed.
    #[test]
    fn fold_field_is_unambiguous_across_value_boundaries() {
        // kunobi-ninja/kache#324: length-prefixing free-text key fields removes
        // delimiter/boundary ambiguity that the old `\n`/`=` form allowed.
        let h = |parts: &[(&[u8], &[u8])]| {
            let mut hasher = blake3::Hasher::new();
            for (l, v) in parts {
                fold_field(&mut hasher, l, v);
            }
            hasher.finalize().to_hex().to_string()
        };

        // Same label, value bytes shifted across the boundary: ("a","bc") vs
        // ("ab","c") must not collide.
        assert_ne!(
            h(&[(b"x:", b"a"), (b"x:", b"bc")]),
            h(&[(b"x:", b"ab"), (b"x:", b"c")]),
        );

        // The exact old-encoding collision: a single cfg value that embeds the
        // `\n` delimiter must not equal two separate cfgs.
        assert_ne!(
            h(&[(b"cfg:", b"a\ncfg:b")]),
            h(&[(b"cfg:", b"a"), (b"cfg:", b"b")]),
        );
    }

    #[test]
    fn unstable_flag_changes_key() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let none = key_of_flags(&flag_base(&source, &[]));
        let san = key_of_flags(&flag_base(&source, &["-Z", "sanitizer=address"]));
        assert_ne!(none, san, "a -Z codegen flag must change the key");
        assert_eq!(
            san,
            key_of_flags(&flag_base(&source, &["-Zsanitizer=address"]))
        );
    }

    /// A `--target` value can be a path to a custom target JSON spec (Firefox /
    /// embedded toolchains). Its file CONTENT — data-layout, target features,
    /// linker, panic strategy — must be folded into the key, so the same path
    /// with different content diverges. Exercises compute_cache_key's
    /// `target_path.is_file()` spec-hashing branch.
    #[test]
    fn custom_target_spec_file_content_changes_key() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();
        let spec = dir.path().join("my-target.json");
        let spec_arg = format!("--target={}", spec.to_str().unwrap());

        std::fs::write(&spec, br#"{"llvm-target":"x","data-layout":"e-A"}"#).unwrap();
        let key_a = key_of_flags(&flag_base(&source, &[&spec_arg]));

        // Same --target path, different spec content -> different key.
        std::fs::write(&spec, br#"{"llvm-target":"x","data-layout":"e-B"}"#).unwrap();
        let key_b = key_of_flags(&flag_base(&source, &[&spec_arg]));
        assert_ne!(
            key_a, key_b,
            "custom target spec file content must change the key"
        );

        // Restoring the original content reproduces the original key.
        std::fs::write(&spec, br#"{"llvm-target":"x","data-layout":"e-A"}"#).unwrap();
        let key_a2 = key_of_flags(&flag_base(&source, &[&spec_arg]));
        assert_eq!(key_a, key_a2, "same spec content -> same key");
    }

    /// A trusted codegen backend dylib is keyed by content: a rebuild in place
    /// changes the key, the same bytes at another checkout's path do not.
    /// A toolchain backend name stays keyed as written.
    #[test]
    fn codegen_backend_dylib_is_keyed_by_content_not_path() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();
        let clone_a = dir.path().join("a/librustc_codegen_x.so");
        let clone_b = dir.path().join("b/librustc_codegen_x.so");
        for backend in [&clone_a, &clone_b] {
            std::fs::create_dir_all(backend.parent().unwrap()).unwrap();
            std::fs::write(backend, b"backend-v1").unwrap();
        }
        let flag = |backend: &Path| format!("-Zcodegen-backend={}", backend.display());

        let a = key_of_flags(&flag_base(&source, &[&flag(&clone_a)]));
        let b = key_of_flags(&flag_base(&source, &[&flag(&clone_b)]));
        assert_eq!(a, b, "identical backends at different paths share a key");

        std::fs::write(&clone_a, b"backend-v2").unwrap();
        let rebuilt = key_of_flags(&flag_base(&source, &[&flag(&clone_a)]));
        assert_ne!(a, rebuilt, "a rebuilt backend must change the key");

        let cranelift = key_of_flags(&flag_base(&source, &["-Zcodegen-backend=cranelift"]));
        let gcc = key_of_flags(&flag_base(&source, &["-Zcodegen-backend=gcc"]));
        assert_ne!(cranelift, gcc, "toolchain backend names stay keyed");
        assert_ne!(cranelift, a);

        let missing = dir.path().join("missing.so");
        let mut parsed = RustcArgs::parse(&flag_base(&source, &[&flag(&missing)])).unwrap();
        parsed.source_file = None;
        assert!(
            compute_cache_key(&parsed, &FileHasher::new(), &PathNormalizer::empty()).is_err(),
            "an unreadable backend must not produce a key"
        );
    }

    /// H2: `--sysroot` selects which std rustc links against; with the
    /// same rustc version, a different sysroot must diverge the key.
    #[test]
    fn sysroot_changes_key() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let a = key_of_flags(&flag_base(&source, &["--sysroot", "/opt/std-a"]));
        let b = key_of_flags(&flag_base(&source, &["--sysroot", "/opt/std-b"]));
        let none = key_of_flags(&flag_base(&source, &[]));
        assert_ne!(a, b, "a different --sysroot must change the key");
        assert_ne!(none, a, "adding --sysroot must change the key");
        assert_eq!(
            a,
            key_of_flags(&flag_base(&source, &["--sysroot=/opt/std-a"]))
        );
    }

    /// H3: a `--target` custom JSON spec must be keyed by its CONTENTS,
    /// so editing the spec in place diverges the key (path string alone
    /// would not).
    #[test]
    fn target_spec_contents_change_key() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();
        let spec = dir.path().join("custom.json");

        std::fs::write(&spec, br#"{"llvm-target":"x86_64","data-layout":"e-m:e"}"#).unwrap();
        let args = flag_base(&source, &["--target", &spec.to_string_lossy()]);
        let before = key_of_flags(&args);

        // Edit the spec in place — same path, different codegen contract.
        std::fs::write(
            &spec,
            br#"{"llvm-target":"x86_64","data-layout":"DIFFERENT"}"#,
        )
        .unwrap();
        let after = key_of_flags(&args);
        assert_ne!(before, after, "editing the target spec must change the key");
    }

    #[test]
    fn test_cache_key_changes_with_source() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");

        // First version
        std::fs::write(&source, b"pub fn hello() {}").unwrap();
        let args_vec: Vec<String> = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "mylib".to_string(),
            source.to_string_lossy().to_string(),
            "--crate-type".to_string(),
            "lib".to_string(),
        ];
        let fh = FileHasher::new();
        let pn = PathNormalizer::empty();
        let parsed1 = RustcArgs::parse(&args_vec).unwrap();
        let key1 = compute_cache_key(&parsed1, &fh, &pn).unwrap();

        // Modified source
        std::fs::write(&source, b"pub fn hello() { println!(\"hi\"); }").unwrap();
        let parsed2 = RustcArgs::parse(&args_vec).unwrap();
        let key2 = compute_cache_key(&parsed2, &fh, &pn).unwrap();

        assert_ne!(key1, key2);
    }

    #[test]
    fn test_unreadable_dep_produces_stable_key() {
        let _lock = key_test_lock();
        // Simulate unreadable deps (sysroot crates) from two different paths —
        // the cache key should be identical because we use a sentinel, not the path.
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        // Create two "dep" paths that both point to non-existent files (will fail hash_file)
        let dep_a =
            std::path::PathBuf::from("/home/runner/.rustup/toolchains/stable/lib/libstd.rlib");
        let dep_b =
            std::path::PathBuf::from("/Users/dev/.rustup/toolchains/stable/lib/libstd.rlib");

        let args_vec: Vec<String> = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "mylib".to_string(),
            source.to_string_lossy().to_string(),
            "--crate-type".to_string(),
            "lib".to_string(),
        ];

        let mut parsed_a = RustcArgs::parse(&args_vec).unwrap();
        parsed_a.externs.push(crate::args::ExternDep {
            name: "std".to_string(),
            path: Some(dep_a),
        });

        let mut parsed_b = RustcArgs::parse(&args_vec).unwrap();
        parsed_b.externs.push(crate::args::ExternDep {
            name: "std".to_string(),
            path: Some(dep_b),
        });

        let fh = FileHasher::new();
        let pn = PathNormalizer::empty();
        let key_a = compute_cache_key(&parsed_a, &fh, &pn).unwrap();
        let key_b = compute_cache_key(&parsed_b, &fh, &pn).unwrap();
        assert_eq!(
            key_a, key_b,
            "unreadable deps with different paths should produce the same key"
        );
    }

    #[test]
    fn path_is_only_used_for_includes_detects_include_pattern() {
        // serde-style include!() puts a build.rs-generated file into
        // dep-info source_files. The OUT_DIR value is the parent dir
        // of that file → safe to normalize.
        let dir = tempfile::tempdir().unwrap();
        let out_dir = dir.path().join("build/serde-abc/out");
        std::fs::create_dir_all(&out_dir).unwrap();
        let included = out_dir.join("private.rs");
        std::fs::write(&included, b"// generated").unwrap();
        let source_files = vec![std::path::PathBuf::from("/src/lib.rs"), included.clone()];
        assert!(
            path_is_only_used_for_includes(out_dir.to_str().unwrap(), &source_files),
            "OUT_DIR contains an included source file → safe to normalize"
        );
    }

    #[test]
    fn path_is_only_used_for_includes_rejects_env_value_pattern() {
        // out-dir-runtime fixture: const X: &str = env!("OUT_DIR");
        // No source file under OUT_DIR → conservatively keep
        // absolute so cache keys diverge across worktrees.
        let dir = tempfile::tempdir().unwrap();
        let out_dir = dir.path().join("build/foo/out");
        std::fs::create_dir_all(&out_dir).unwrap();
        // dep-info source_files contains only the crate root,
        // nothing under OUT_DIR.
        let source_files = vec![std::path::PathBuf::from("/src/main.rs")];
        assert!(
            !path_is_only_used_for_includes(out_dir.to_str().unwrap(), &source_files),
            "no source under OUT_DIR → unsafe to normalize"
        );
    }

    #[test]
    fn path_is_only_used_for_includes_handles_macos_symlink_form() {
        // The same canonicalization concern that motivated
        // PathNormalizer: on macOS the OUT_DIR value may be in
        // /private/tmp/... form while source paths report /tmp/...
        // (or vice versa). Canonical-path comparison must succeed
        // either way.
        if !cfg!(target_os = "macos") {
            return;
        }
        let unique = format!("kache-cache-key-test-{}", std::process::id());
        let real_out = std::path::Path::new("/tmp").join(&unique).join("out");
        std::fs::create_dir_all(&real_out).unwrap();
        let included = real_out.join("private.rs");
        std::fs::write(&included, b"// generated").unwrap();

        // OUT_DIR comes from cargo as /private/tmp/... form
        let out_dir_value = format!("/private/tmp/{unique}/out");
        // source_files reports /tmp/... (the symlink form)
        let source_files = vec![included];

        let result = path_is_only_used_for_includes(&out_dir_value, &source_files);
        let _ = std::fs::remove_dir_all(std::path::Path::new("/tmp").join(&unique));
        assert!(
            result,
            "canonical-path comparison must see through the symlink"
        );
    }

    #[test]
    fn source_env_dep_use_detector_allows_include_locators() {
        let source = r#"
include!(concat!(env!("OUT_DIR"), "/generated.rs"));
include_str!(concat!(env ! ( "OUT_DIR" ), "/template.txt"));
include_bytes!(env!("BLOB_PATH"));
"#;
        assert_eq!(
            source_env_dep_use(source, "OUT_DIR"),
            SourceEnvDepUse::IncludeLocator
        );
        assert_eq!(
            source_env_dep_use(source, "BLOB_PATH"),
            SourceEnvDepUse::IncludeLocator
        );
    }

    #[test]
    fn source_env_dep_use_detector_rejects_runtime_values() {
        let source = r#"
const OUT_DIR: &str = env!("OUT_DIR");
const MAYBE_OUT_DIR: Option<&str> = option_env!("OUT_DIR");
const PATH: &str = concat!(env!("OUT_DIR"), "/data.txt");
"#;
        assert_eq!(
            source_env_dep_use(source, "OUT_DIR"),
            SourceEnvDepUse::RuntimeValue
        );
    }

    #[test]
    fn source_env_dep_use_detector_rejects_dual_pattern() {
        let source = r#"
include!(concat!(env!("OUT_DIR"), "/generated.rs"));
pub const OUT_DIR_AT_COMPILE_TIME: &str = env!("OUT_DIR");
"#;
        assert_eq!(
            source_env_dep_use(source, "OUT_DIR"),
            SourceEnvDepUse::RuntimeValue
        );
    }

    #[test]
    fn source_env_dep_use_detector_ignores_comments_and_strings() {
        let source = r##"
// const X: &str = env!("OUT_DIR");
/* const Y: &str = env!("OUT_DIR"); */
const TEXT: &str = "env!(\"OUT_DIR\")";
const RAW: &str = r#"env!("OUT_DIR")"#;
include!(concat!(env!("OUT_DIR"), "/generated.rs"));
"##;
        assert_eq!(
            source_env_dep_use(source, "OUT_DIR"),
            SourceEnvDepUse::IncludeLocator
        );
    }

    #[test]
    fn env_dep_normalization_decision_trace_labels_are_stable() {
        for (decision, expected) in [
            (EnvDepNormalizationDecision::Unchanged, "unchanged"),
            (
                EnvDepNormalizationDecision::NormalizedPathOnly,
                "normalized path-only",
            ),
            (
                EnvDepNormalizationDecision::KeptAbsoluteNotPathOnly,
                "kept absolute: not a path-only var",
            ),
            (
                EnvDepNormalizationDecision::KeptAbsoluteManifestDir,
                "kept absolute: CARGO_MANIFEST_DIR is never path-only",
            ),
            (
                EnvDepNormalizationDecision::KeptAbsoluteNoIncludeProof,
                "kept absolute: no include proof",
            ),
            (
                EnvDepNormalizationDecision::KeptAbsoluteRuntimeUse,
                "kept absolute: value use in source",
            ),
            (
                EnvDepNormalizationDecision::KeptAbsoluteScanError,
                "kept absolute: source scan failed",
            ),
            (
                EnvDepNormalizationDecision::ForcedPathOnly,
                "forced path-only (user-asserted)",
            ),
            (
                EnvDepNormalizationDecision::AliasedOutDir,
                "aliased OUT_DIR",
            ),
        ] {
            assert_eq!(decision.as_str(), expected);
        }
    }

    /// The same unit in two checkouts gets two OUT_DIRs. Pointed at one
    /// shared alias, both its OUT_DIR and a `rustc-env` var under it key the
    /// same, and never through a sentinel.
    #[test]
    fn aliased_out_dir_values_key_the_same_in_every_checkout() {
        let alias = Path::new("/cache/out-dirs/v1/d/dmac-0123456789abcdef/out");
        // A rule over the cache dir must not turn the alias into a sentinel.
        let pn = PathNormalizer::empty().with_base_dirs(&["/cache".to_string()]);
        assert_ne!(
            pn.normalize(alias.to_str().unwrap()),
            alias.to_str().unwrap()
        );
        let keyed = |checkout: &str| {
            let real = PathBuf::from(format!(
                "/{checkout}/target/debug/build/dmac-0123456789abcdef/out"
            ));
            let env = vec![
                ("OUT_DIR".into(), real.clone().into_os_string()),
                ("DEBUG_OUTPUT_DIR".into(), real.clone().into_os_string()),
            ];
            let rewrites = crate::out_dir_alias::env_rewrites(&env, &[], &[&real], alias)
                .expect("nothing else mentions OUT_DIR");
            rewrites
                .iter()
                .map(|(var, value)| {
                    normalize_env_dep_value_with_hasher(
                        "dmac",
                        var.to_str().unwrap(),
                        value.to_str().unwrap(),
                        &[],
                        &FileHasher::new(),
                        &pn,
                        Some(alias),
                    )
                })
                .collect::<Vec<_>>()
        };
        let a = keyed("a");
        let b = keyed("b");
        assert_eq!(a, b);
        assert_eq!(a.len(), 2);
        for dep in &a {
            assert_eq!(dep.decision, EnvDepNormalizationDecision::AliasedOutDir);
            assert_eq!(dep.value, alias.to_str().unwrap());
        }
    }

    #[test]
    fn only_values_at_or_under_the_alias_take_the_aliased_decision() {
        let alias = Path::new("/c/d/out");
        let pn = PathNormalizer::empty();
        let decide = |value: &str, aliased: Option<&Path>| {
            normalize_env_dep_value_with_hasher(
                "dmac",
                "V",
                value,
                &[],
                &FileHasher::new(),
                &pn,
                aliased,
            )
            .decision
        };
        assert_eq!(
            decide("/c/d/out/gen.rs", Some(alias)),
            EnvDepNormalizationDecision::AliasedOutDir
        );
        assert_ne!(
            decide("/c/d/out2", Some(alias)),
            EnvDepNormalizationDecision::AliasedOutDir
        );
        assert_ne!(
            decide("/c/d/out", None),
            EnvDepNormalizationDecision::AliasedOutDir
        );
        assert!(value_at_or_under("/c/d/out", alias));
        assert!(value_at_or_under("/c/d/out/", alias));
        assert!(!value_at_or_under("/c/d", alias));
        assert!(!value_at_or_under("x/c/d/out", alias));
    }

    #[test]
    fn env_dep_bakes_out_dir_when_an_out_dir_path_stays_literal() {
        use EnvDepNormalizationDecision as D;
        let literal = [
            D::Unchanged,
            D::KeptAbsoluteNotPathOnly,
            D::KeptAbsoluteManifestDir,
            D::KeptAbsoluteNoIncludeProof,
            D::KeptAbsoluteRuntimeUse,
            D::KeptAbsoluteScanError,
            D::AliasedOutDir,
        ];
        for decision in literal {
            assert!(decision.keeps_literal_value(), "{decision:?}");
            assert!(env_dep_bakes_out_dir("OUT_DIR", decision, || false));
            assert!(env_dep_bakes_out_dir("GEN", decision, || true));
            assert!(!env_dep_bakes_out_dir("GEN", decision, || false));
        }
        for decision in [D::NormalizedPathOnly, D::ForcedPathOnly] {
            assert!(!decision.keeps_literal_value(), "{decision:?}");
            assert!(!env_dep_bakes_out_dir("OUT_DIR", decision, || true));
        }
    }

    /// The key says whether it kept an OUT_DIR path, and a later key that
    /// did not starts clean.
    #[test]
    fn key_computation_stashes_whether_it_bakes_out_dir() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, "pub fn f() {}").unwrap();
        let args = RustcArgs::parse(&[
            "rustc".to_string(),
            "--crate-name".to_string(),
            "helper".to_string(),
            source.to_str().unwrap().to_string(),
        ])
        .unwrap();
        let key_with = |env_deps: Vec<(String, String)>| {
            provide_dep_info(DepInfo {
                source_files: vec![source.clone()],
                env_deps,
            });
            compute_cache_key(&args, &FileHasher::new(), &PathNormalizer::empty()).unwrap();
        };

        key_with(vec![("OUT_DIR".into(), "/nowhere/out".into())]);
        assert!(take_last_key_bakes_out_dir());
        assert!(!take_last_key_bakes_out_dir(), "taken, not copied");

        key_with(vec![("CARGO_PKG_NAME".into(), "helper".into())]);
        assert!(!take_last_key_bakes_out_dir());

        // Any one baking value is enough, however many there are.
        key_with(vec![
            ("OUT_DIR".into(), "/nowhere/out".into()),
            ("OUT_DIR".into(), "/nowhere/out".into()),
        ]);
        assert!(take_last_key_bakes_out_dir());

        key_with(vec![("OUT_DIR".into(), "/nowhere/out".into())]);
        let mut no_source = args.clone();
        no_source.source_file = None;
        compute_cache_key(&no_source, &FileHasher::new(), &PathNormalizer::empty()).unwrap();
        assert!(!take_last_key_bakes_out_dir(), "reset by the next key");
    }

    #[test]
    fn env_dep_policy_normalizes_out_dir_include_pattern() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        let src = workspace.join("src");
        let out_dir = workspace.join("target/debug/build/pkg/out");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::create_dir_all(&out_dir).unwrap();
        let lib = src.join("lib.rs");
        std::fs::write(
            &lib,
            r#"include!(concat!(env!("OUT_DIR"), "/generated.rs"));"#,
        )
        .unwrap();
        let included = out_dir.join("generated.rs");
        std::fs::write(&included, b"pub fn generated() -> u8 { 1 }").unwrap();

        let source_files = vec![lib, included];
        let path_normalizer = PathNormalizer::from_env(Some(&workspace));
        let out_dir_value = out_dir
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .to_string();
        let env_dep = normalize_env_dep_value(
            "test_crate",
            "OUT_DIR",
            &out_dir_value,
            &source_files,
            &path_normalizer,
        );

        assert_eq!(
            env_dep.decision,
            EnvDepNormalizationDecision::NormalizedPathOnly
        );
        assert_ne!(env_dep.value, out_dir_value);
        assert!(
            env_dep.value.contains("<WORKSPACE>"),
            "OUT_DIR include pattern should normalize to the workspace sentinel: {env_dep:?}"
        );
    }

    #[test]
    fn env_dep_policy_keeps_out_dir_dual_pattern_absolute() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        let src = workspace.join("src");
        let out_dir = workspace.join("target/debug/build/pkg/out");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::create_dir_all(&out_dir).unwrap();
        let lib = src.join("lib.rs");
        std::fs::write(
            &lib,
            r#"
include!(concat!(env!("OUT_DIR"), "/generated.rs"));
pub const OUT_DIR_AT_COMPILE_TIME: &str = env!("OUT_DIR");
"#,
        )
        .unwrap();
        let included = out_dir.join("generated.rs");
        std::fs::write(&included, b"pub fn generated() -> u8 { 1 }").unwrap();

        let source_files = vec![lib, included];
        let path_normalizer = PathNormalizer::from_env(Some(&workspace));
        let out_dir_value = out_dir
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .to_string();
        let env_dep = normalize_env_dep_value(
            "test_crate",
            "OUT_DIR",
            &out_dir_value,
            &source_files,
            &path_normalizer,
        );

        assert_eq!(
            env_dep.decision,
            EnvDepNormalizationDecision::KeptAbsoluteRuntimeUse
        );
        assert_eq!(env_dep.value, out_dir_value);
    }

    /// The OUT_DIR decision for a crate whose root source is `lib_source` and
    /// whose dep-info lists a generated include under OUT_DIR, the shape that
    /// makes OUT_DIR a normalization candidate at all.
    fn out_dir_decision_for(
        lib_source: &str,
        file_hasher: &FileHasher<'_>,
    ) -> EnvDepNormalizationDecision {
        out_dir_decision_for_files(&[("src/lib.rs", Some(lib_source))], file_hasher)
    }

    /// Like [`out_dir_decision_for`], with every workspace-relative file in
    /// `files` listed in dep-info. A `None` body is listed but never written.
    fn out_dir_decision_for_files(
        files: &[(&str, Option<&str>)],
        file_hasher: &FileHasher<'_>,
    ) -> EnvDepNormalizationDecision {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        let out_dir = workspace.join("target/debug/build/pkg/out");
        std::fs::create_dir_all(&out_dir).unwrap();
        let mut source_files = Vec::new();
        for (relative, body) in files {
            let path = workspace.join(relative);
            if let Some(body) = body {
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(&path, body).unwrap();
            }
            source_files.push(path);
        }
        let included = out_dir.join("generated.rs");
        std::fs::write(&included, b"pub fn generated() -> u8 { 1 }").unwrap();
        source_files.push(included);
        let out_dir_value = out_dir
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .to_string();
        normalize_env_dep_value_with_hasher(
            "test_crate",
            "OUT_DIR",
            &out_dir_value,
            &source_files,
            file_hasher,
            &PathNormalizer::from_env(Some(&workspace)),
            None,
        )
        .decision
    }

    #[test]
    fn env_dep_policy_keeps_out_dir_absolute_for_uses_the_scanner_cannot_prove() {
        // Every source below includes a generated file through OUT_DIR, so
        // dep-info alone would allow normalization, and every one also derives
        // something from the absolute OUT_DIR that ends up in the artifact. A
        // normalized key would restore one checkout's artifact in another.
        const INCLUDE: &str = r#"include!(concat!(env!("OUT_DIR"), "/generated.rs"));"#;
        let value_uses: &[(&str, String)] = &[
            (
                "value length",
                format!(r#"{INCLUDE} pub const N: usize = env!("OUT_DIR").len();"#),
            ),
            (
                "value arithmetic",
                format!(r#"{INCLUDE} pub const P: usize = env!("OUT_DIR").len() % 2;"#),
            ),
            (
                "computed name",
                format!(r#"{INCLUDE} pub const S: &str = env!(concat!("OUT", "_DIR"));"#),
            ),
            (
                "computed name, option_env",
                format!(
                    r#"{INCLUDE} pub const S: Option<&str> = option_env!(concat!("OUT", "_DIR"));"#
                ),
            ),
            (
                "forwarding macro",
                format!(
                    r#"{INCLUDE}
macro_rules! e {{ ($v:literal) => {{ env!($v) }} }}
pub const X: &str = e!("OUT_DIR");"#
                ),
            ),
            (
                "brace delimiter",
                format!(r#"{INCLUDE} pub const S: &str = env!{{"OUT_DIR"}};"#),
            ),
            (
                "bracket delimiter",
                format!(r#"{INCLUDE} pub const S: &str = env!["OUT_DIR"];"#),
            ),
            (
                "escaped name",
                format!(r#"{INCLUDE} pub const S: &str = env!("OUT\x5FDIR");"#),
            ),
            (
                "raw string name",
                format!(r##"{INCLUDE} pub const S: &str = env!(r#"OUT_DIR"#);"##),
            ),
            (
                "comment before the bang",
                format!(r#"{INCLUDE} pub const S: &str = env /* x */ !("OUT_DIR");"#),
            ),
            (
                "non-ASCII whitespace before the bang",
                format!("{INCLUDE} pub const S: &str = env\u{200E}!(\"OUT_DIR\");"),
            ),
            (
                "vertical tab before the bang",
                format!("{INCLUDE} pub const S: &str = env\x0B!(\"OUT_DIR\");"),
            ),
            (
                "lifetime before the use",
                format!(
                    r#"{INCLUDE} pub fn f(_: &'static str) -> usize {{ env!("OUT_DIR").len() }}"#
                ),
            ),
            (
                "nested block comment",
                format!(
                    r#"{INCLUDE} /* /* */ include!( */ pub const N: usize = env!("OUT_DIR").len();"#
                ),
            ),
            (
                "raw C string ending in a backslash",
                format!(
                    r#"{INCLUDE} pub const C: &core::ffi::CStr = cr"\"; pub const N: usize = env!("OUT_DIR").len(); pub const T: &str = "";"#
                ),
            ),
            (
                "number suffix before a raw-string look-alike",
                format!(r##"{INCLUDE} m!{{ 1r#"x" }} pub const P: &str = env!("OUT_DIR"); // "#"##),
            ),
            (
                "non-ASCII prefix on an include look-alike",
                format!(
                    r#"{INCLUDE}
macro_rules! éinclude {{ ($e:expr) => {{ pub const P: &str = $e; }} }}
éinclude!(env!("OUT_DIR"));"#
                ),
            ),
        ];
        // No visible include use: the env dep may come from another crate's
        // macro that bakes the value.
        let unproven: &[(&str, String)] = &[
            (
                "no env! in the crate",
                "pub fn f() -> &'static str { some_dep::out_dir!() }".to_string(),
            ),
            (
                "only a computed name inside include",
                r#"include!(concat!(env!(concat!("OUT", "_DIR")), "/generated.rs"));"#.to_string(),
            ),
        ];
        let hasher = FileHasher::new();
        let wrong: Vec<(&str, EnvDepNormalizationDecision)> = value_uses
            .iter()
            .map(|case| (case, EnvDepNormalizationDecision::KeptAbsoluteRuntimeUse))
            .chain(unproven.iter().map(|case| {
                (
                    case,
                    EnvDepNormalizationDecision::KeptAbsoluteNoIncludeProof,
                )
            }))
            .filter_map(|((label, source), expected)| {
                let decision = out_dir_decision_for(source, &hasher);
                (decision != expected).then_some((*label, decision))
            })
            .collect();
        assert!(wrong.is_empty(), "unexpected OUT_DIR decisions: {wrong:?}");
    }

    #[test]
    fn env_dep_policy_takes_include_proof_only_from_rust_sources() {
        // `#![doc = include_str!("../README.md")]` puts the README in dep-info.
        // A code block in it that shows the include pattern is not code.
        let readme = r#"include!(concat!(env!("OUT_DIR"), "/generated.rs"));"#;
        let lib = r#"#![doc = include_str!("../README.md")] some_dep::out_dir!();"#;
        let hasher = FileHasher::new();
        assert_eq!(
            out_dir_decision_for_files(
                &[("src/lib.rs", Some(lib)), ("README.md", Some(readme))],
                &hasher
            ),
            EnvDepNormalizationDecision::KeptAbsoluteNoIncludeProof
        );
        // The same text in a Rust file is proof.
        assert_eq!(
            out_dir_decision_for_files(
                &[("src/lib.rs", Some(lib)), ("src/gen.rs", Some(readme))],
                &hasher
            ),
            EnvDepNormalizationDecision::NormalizedPathOnly
        );
        // A value use in any file still counts.
        assert_eq!(
            out_dir_decision_for_files(
                &[
                    ("src/lib.rs", Some(readme)),
                    (
                        "README.md",
                        Some(r#"const N: usize = env!("OUT_DIR").len();"#)
                    ),
                ],
                &hasher
            ),
            EnvDepNormalizationDecision::KeptAbsoluteRuntimeUse
        );
    }

    #[test]
    fn env_dep_policy_keeps_out_dir_absolute_when_a_source_cannot_be_scanned() {
        let include = r#"include!(concat!(env!("OUT_DIR"), "/generated.rs"));"#;
        assert_eq!(
            out_dir_decision_for_files(
                &[("src/lib.rs", Some(include)), ("src/missing.rs", None)],
                &FileHasher::new()
            ),
            EnvDepNormalizationDecision::KeptAbsoluteScanError
        );
    }

    #[test]
    fn env_dep_policy_normalizes_out_dir_proven_include_locators() {
        let cases: &[(&str, &str)] = &[
            (
                "include concat",
                r#"include!(concat!(env!("OUT_DIR"), "/generated.rs"));"#,
            ),
            (
                "include in a local macro",
                r#"macro_rules! generated { ($f:literal) => { include!(concat!(env!("OUT_DIR"), "/", $f)); } }
generated!("generated.rs");"#,
            ),
            (
                "brace-delimited include",
                r#"include!{ concat!(env!("OUT_DIR"), "/generated.rs") }"#,
            ),
            (
                "lifetime and char literals around the include",
                r#"pub fn f<'a>(x: &'a str) -> char { let _ = x; 'x' }
include!(concat!(env!("OUT_DIR"), "/generated.rs"));
pub fn g(_: &'static str) {}"#,
            ),
        ];
        let hasher = FileHasher::new();
        let kept: Vec<&str> = cases
            .iter()
            .filter(|(_, source)| {
                out_dir_decision_for(source, &hasher)
                    != EnvDepNormalizationDecision::NormalizedPathOnly
            })
            .map(|(label, _)| *label)
            .collect();
        assert!(kept.is_empty(), "OUT_DIR must normalize for: {kept:?}");
    }

    #[test]
    fn env_dep_policy_ignores_env_use_memo_rows_from_the_old_scanner() {
        // Before the scanner learned computed names, it recorded "no runtime
        // use" for this source. That row is keyed by content hash only, so an
        // upgraded wrapper would reuse it for the unchanged file unless the
        // memo is versioned.
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("idx.sqlite");
        let source = concat!(
            r#"include!(concat!(env!("OUT_DIR"), "/generated.rs"));"#,
            r#" pub const S: &str = env!(concat!("OUT", "_DIR"));"#
        );
        let content_hash = blake3::hash(source.as_bytes()).to_hex().to_string();
        drop(FileHasher::persistent(&db));
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS source_env_runtime_uses (
                content_hash    TEXT NOT NULL,
                env_var         TEXT NOT NULL,
                has_runtime_use INTEGER NOT NULL,
                updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (content_hash, env_var)
            );",
        )
        .unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO source_env_runtime_uses
             (content_hash, env_var, has_runtime_use) VALUES (?1, 'OUT_DIR', 0)",
            rusqlite::params![content_hash],
        )
        .unwrap();
        drop(conn);

        let hasher = FileHasher::persistent(&db);
        assert_eq!(
            out_dir_decision_for(source, &hasher),
            EnvDepNormalizationDecision::KeptAbsoluteRuntimeUse
        );
    }

    #[test]
    fn env_dep_policy_normalizes_allowlisted_var_but_not_unlisted() {
        // A non-OUT_DIR var that only locates an `include!`'d file (a source
        // file lives under it) is normalized ONLY when opted into the path-only
        // allowlist; otherwise kept absolute. This is the
        // KACHE_PATH_ONLY_ENV_VARS / `[cache] path_only_env_vars` contract.
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        let src = workspace.join("src");
        let gen_dir = workspace.join("objdir/build/rust/mozbuild");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::create_dir_all(&gen_dir).unwrap();
        let lib = src.join("lib.rs");
        std::fs::write(&lib, r#"include!(env!("BUILDCONFIG_RS"));"#).unwrap();
        let included = gen_dir.join("buildconfig.rs");
        std::fs::write(&included, b"pub const X: u8 = 1;").unwrap();
        let source_files = vec![lib, included.clone()];
        let value = included
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .to_string();

        // Not allowlisted -> kept absolute.
        let pn_off = PathNormalizer::from_env(Some(&workspace));
        let off = normalize_env_dep_value(
            "test_crate",
            "BUILDCONFIG_RS",
            &value,
            &source_files,
            &pn_off,
        );
        assert_eq!(
            off.decision,
            EnvDepNormalizationDecision::KeptAbsoluteNotPathOnly
        );
        assert_eq!(off.value, value);

        // Allowlisted -> normalized (the same gate as OUT_DIR still applies).
        let pn_on = PathNormalizer::from_env(Some(&workspace))
            .with_path_only_env_vars(vec!["BUILDCONFIG_RS".to_string()]);
        let on = normalize_env_dep_value(
            "test_crate",
            "BUILDCONFIG_RS",
            &value,
            &source_files,
            &pn_on,
        );
        assert_eq!(on.decision, EnvDepNormalizationDecision::NormalizedPathOnly);
        assert!(
            on.value.contains("<WORKSPACE>"),
            "allowlisted include locator should normalize: {on:?}"
        );
    }

    #[test]
    fn env_dep_policy_normalizes_rustc_env_var_pointing_under_out_dir() {
        // kunobi-ninja/kache#431, the typenum cascade root: a build script sets
        // `cargo:rustc-env=GEN_BUILD_CONSTS=$OUT_DIR/consts.rs` and the crate does
        // `include!(env!("GEN_BUILD_CONSTS"))`. The var is NOT named OUT_DIR and is
        // NOT allowlisted, but its value lives UNDER OUT_DIR and only locates a
        // generated include — so it must normalize like OUT_DIR (else typenum
        // re-keys per checkout and the whole substrate stack misses cross-clone).
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        let out_dir = workspace.join("target/release/build/genlib-abc123/out");
        let src = workspace.join("src");
        std::fs::create_dir_all(&out_dir).unwrap();
        std::fs::create_dir_all(&src).unwrap();
        let lib = src.join("lib.rs");
        std::fs::write(&lib, r#"include!(env!("GEN_BUILD_CONSTS"));"#).unwrap();
        let generated = out_dir.join("consts.rs");
        std::fs::write(&generated, b"pub const N: u32 = 42;").unwrap();
        let source_files = vec![lib, generated.clone()];
        let value = generated
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .to_string();
        let path_normalizer = PathNormalizer::from_env(Some(&workspace));

        let old_out_dir = std::env::var_os("OUT_DIR");
        // SAFETY: serialized by key_test_lock; restored below.
        unsafe { std::env::set_var("OUT_DIR", &out_dir) };
        let under = normalize_env_dep_value(
            "test_crate",
            "GEN_BUILD_CONSTS",
            &value,
            &source_files,
            &path_normalizer,
        );
        // With OUT_DIR unset there is no anchor, so the same var must stay
        // absolute — proves the gate is the under-OUT_DIR test, not the var name.
        unsafe { std::env::remove_var("OUT_DIR") };
        let no_anchor = normalize_env_dep_value(
            "test_crate",
            "GEN_BUILD_CONSTS",
            &value,
            &source_files,
            &path_normalizer,
        );
        restore_env_var("OUT_DIR", old_out_dir);

        assert_eq!(
            under.decision,
            EnvDepNormalizationDecision::NormalizedPathOnly,
            "a rustc-env var pointing under OUT_DIR, used only as an include locator, \
             must normalize: {under:?}"
        );
        let unit = out_dir
            .canonicalize()
            .unwrap()
            .parent()
            .unwrap()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        assert_eq!(
            under.value,
            format!("<OUT_DIR:{unit}>/consts.rs"),
            "an OUT_DIR-locator value normalizes relative to OUT_DIR (#330), keeping \
             the per-unit component (file!() observability) but not the location: {under:?}"
        );
        assert_eq!(
            no_anchor.decision,
            EnvDepNormalizationDecision::KeptAbsoluteNotPathOnly,
            "without an OUT_DIR anchor the same non-allowlisted var must stay absolute"
        );
    }

    #[test]
    fn env_dep_policy_keeps_out_dir_runtime_value_absolute() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        let out_dir = workspace.join("target/debug/build/pkg/out");
        std::fs::create_dir_all(&out_dir).unwrap();

        let source_files = vec![workspace.join("src/main.rs")];
        let path_normalizer = PathNormalizer::from_env(Some(&workspace));
        let out_dir_value = out_dir
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .to_string();
        let env_dep = normalize_env_dep_value(
            "test_crate",
            "OUT_DIR",
            &out_dir_value,
            &source_files,
            &path_normalizer,
        );

        assert_eq!(
            env_dep.decision,
            EnvDepNormalizationDecision::KeptAbsoluteNoIncludeProof
        );
        assert_eq!(env_dep.value, out_dir_value);
    }

    #[test]
    fn env_dep_policy_force_list_overrides_runtime_value_scan() {
        // A crate whose source uses env!("OUT_DIR") as a runtime value is
        // normally kept absolute — but a user-asserted force entry normalizes
        // it anyway (the deployment guarantees the embedding branch is dead).
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let out_dir = dir.path().join("out");
        std::fs::create_dir_all(&out_dir).unwrap();
        let src = dir.path().join("lib.rs");
        std::fs::write(&src, b"pub fn p() -> &'static str { env!(\"OUT_DIR\") }").unwrap();
        let out_dir_value = out_dir.to_string_lossy().to_string();
        let source_files = vec![src];

        let _out_dir = ScopedEnv::set("OUT_DIR", &out_dir_value);

        let pn_plain = PathNormalizer::from_env(Some(dir.path()));
        let kept = normalize_env_dep_value(
            "cef_dll_sys",
            "OUT_DIR",
            &out_dir_value,
            &source_files,
            &pn_plain,
        );

        let pn_forced = PathNormalizer::from_env(Some(dir.path()))
            .with_path_only_env_vars(vec!["cef_dll_sys:OUT_DIR".to_string()]);
        let forced = normalize_env_dep_value(
            "cef_dll_sys",
            "OUT_DIR",
            &out_dir_value,
            &source_files,
            &pn_forced,
        );

        assert_eq!(
            kept.decision,
            EnvDepNormalizationDecision::KeptAbsoluteNoIncludeProof
        );
        assert_eq!(
            forced.decision,
            EnvDepNormalizationDecision::ForcedPathOnly,
            "a force-listed var must normalize despite the runtime-value scan: {forced:?}"
        );
        assert!(
            forced.value.starts_with("<OUT_DIR:") || forced.value.starts_with("<WORKSPACE>"),
            "forced OUT_DIR normalizes to a location-free sentinel form              (either the #330 OUT_DIR sentinel or a generic prefix rule): {forced:?}"
        );
        assert!(
            !forced.value.contains(dir.path().to_string_lossy().as_ref()),
            "no absolute build location may survive in a forced value: {forced:?}"
        );
    }

    #[test]
    fn env_dep_policy_force_list_crate_scope_matches_only_that_crate() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let out_dir = dir.path().join("out");
        std::fs::create_dir_all(&out_dir).unwrap();
        let src = dir.path().join("lib.rs");
        std::fs::write(&src, b"pub fn p() -> &'static str { env!(\"OUT_DIR\") }").unwrap();
        let out_dir_value = out_dir.to_string_lossy().to_string();
        let source_files = vec![src];

        let _out_dir = ScopedEnv::set("OUT_DIR", &out_dir_value);

        let pn = PathNormalizer::from_env(Some(dir.path()))
            .with_path_only_env_vars(vec!["cef_dll_sys:OUT_DIR".to_string()]);
        let scoped_match =
            normalize_env_dep_value("cef_dll_sys", "OUT_DIR", &out_dir_value, &source_files, &pn);
        let scoped_other =
            normalize_env_dep_value("other_crate", "OUT_DIR", &out_dir_value, &source_files, &pn);

        assert_eq!(
            scoped_match.decision,
            EnvDepNormalizationDecision::ForcedPathOnly
        );
        assert_eq!(
            scoped_other.decision,
            EnvDepNormalizationDecision::KeptAbsoluteNoIncludeProof,
            "a crate-scoped force entry must not leak to other crates: {scoped_other:?}"
        );
    }

    #[test]
    fn env_dep_policy_refuses_manifest_dir_in_every_allowlist_form() {
        // A crate's own sources live under CARGO_MANIFEST_DIR, so the include
        // proof is trivially satisfied and only the refusal keeps #167 shut.
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        let manifest_dir = workspace.join("helper");
        let src = manifest_dir.join("src");
        std::fs::create_dir_all(&src).unwrap();
        let lib = src.join("lib.rs");
        std::fs::write(
            &lib,
            br#"include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/gen.rs"));"#,
        )
        .unwrap();

        let source_files = vec![lib];
        let manifest_dir_value = manifest_dir
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .to_string();
        for (entry, var) in [
            ("test_crate:CARGO_MANIFEST_DIR", "CARGO_MANIFEST_DIR"),
            ("CARGO_MANIFEST_DIR", "CARGO_MANIFEST_DIR"),
            // Windows resolves env names case-insensitively, so dep-info can
            // carry any spelling of the same variable.
            ("cargo_manifest_dir", "cargo_manifest_dir"),
        ] {
            let path_normalizer = PathNormalizer::from_env(Some(&workspace))
                .with_path_only_env_vars(vec![entry.to_string()]);
            let env_dep = normalize_env_dep_value(
                "test_crate",
                var,
                &manifest_dir_value,
                &source_files,
                &path_normalizer,
            );

            assert_eq!(
                env_dep.decision,
                EnvDepNormalizationDecision::KeptAbsoluteManifestDir,
                "CARGO_MANIFEST_DIR must stay absolute, listed as `{entry}`"
            );
            assert_eq!(env_dep.value, manifest_dir_value);
        }
    }

    #[test]
    fn env_dep_policy_keeps_user_path_env_absolute_when_normalized() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        let config_dir = workspace.join("config");
        std::fs::create_dir_all(&config_dir).unwrap();

        let source_files = vec![workspace.join("src/lib.rs")];
        let path_normalizer = PathNormalizer::from_env(Some(&workspace));
        let config_dir_value = config_dir
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .to_string();
        let env_dep = normalize_env_dep_value(
            "test_crate",
            "CUSTOM_CONFIG_DIR",
            &config_dir_value,
            &source_files,
            &path_normalizer,
        );

        assert_eq!(
            env_dep.decision,
            EnvDepNormalizationDecision::KeptAbsoluteNotPathOnly
        );
        assert_eq!(env_dep.value, config_dir_value);
    }

    // `test_normalize_flags` removed: normalize_flags itself is gone,
    // replaced by PathNormalizer (covered by tests in path_normalizer
    // module). The cache_key consumer-side normalization is exercised
    // via the e2e relocate phase + the `path_is_only_used_for_includes` tests.

    #[test]
    fn test_cache_key_changes_with_features() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let args1: Vec<String> = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "mylib".to_string(),
            source.to_string_lossy().to_string(),
            "--cfg".to_string(),
            "feature=\"std\"".to_string(),
        ];

        let mut args2 = args1.clone();
        args2.push("--cfg".to_string());
        args2.push("feature=\"derive\"".to_string());

        let parsed1 = RustcArgs::parse(&args1).unwrap();
        let parsed2 = RustcArgs::parse(&args2).unwrap();

        let fh = FileHasher::new();
        let pn = PathNormalizer::empty();
        let key1 = compute_cache_key(&parsed1, &fh, &pn).unwrap();
        let key2 = compute_cache_key(&parsed2, &fh, &pn).unwrap();

        assert_ne!(key1, key2);
    }

    #[test]
    fn test_cache_key_changes_with_instrument_coverage() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let args_normal: Vec<String> = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "mylib".to_string(),
            source.to_string_lossy().to_string(),
            "--crate-type".to_string(),
            "lib".to_string(),
        ];

        let mut args_coverage = args_normal.clone();
        args_coverage.push("-Cinstrument-coverage".to_string());

        let parsed_normal = RustcArgs::parse(&args_normal).unwrap();
        let parsed_coverage = RustcArgs::parse(&args_coverage).unwrap();

        assert!(!parsed_normal.has_coverage_instrumentation());
        assert!(parsed_coverage.has_coverage_instrumentation());

        let fh = FileHasher::new();
        let pn = PathNormalizer::empty();
        let key_normal = compute_cache_key(&parsed_normal, &fh, &pn).unwrap();
        let key_coverage = compute_cache_key(&parsed_coverage, &fh, &pn).unwrap();

        assert_ne!(
            key_normal, key_coverage,
            "coverage-instrumented builds must have different cache keys"
        );
    }

    #[test]
    fn test_cache_key_changes_with_instrument_coverage_two_arg() {
        let _lock = key_test_lock();
        // Same test but with -C instrument-coverage (two-arg form)
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let args_normal: Vec<String> = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "mylib".to_string(),
            source.to_string_lossy().to_string(),
            "--crate-type".to_string(),
            "lib".to_string(),
        ];

        let mut args_coverage = args_normal.clone();
        args_coverage.extend(["-C".to_string(), "instrument-coverage".to_string()]);

        let parsed_normal = RustcArgs::parse(&args_normal).unwrap();
        let parsed_coverage = RustcArgs::parse(&args_coverage).unwrap();

        assert!(parsed_coverage.has_coverage_instrumentation());

        let fh = FileHasher::new();
        let pn = PathNormalizer::empty();
        let key_normal = compute_cache_key(&parsed_normal, &fh, &pn).unwrap();
        let key_coverage = compute_cache_key(&parsed_coverage, &fh, &pn).unwrap();

        assert_ne!(
            key_normal, key_coverage,
            "two-arg form -C instrument-coverage must also produce different cache keys"
        );
    }

    #[test]
    fn test_cache_key_changes_with_tarpaulin_cfg() {
        let _lock = key_test_lock();
        // Tarpaulin also passes --cfg=tarpaulin; verify it affects the key
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let args_normal: Vec<String> = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "mylib".to_string(),
            source.to_string_lossy().to_string(),
            "--crate-type".to_string(),
            "lib".to_string(),
        ];

        let mut args_tarpaulin = args_normal.clone();
        args_tarpaulin.extend(["--cfg".to_string(), "tarpaulin".to_string()]);

        let parsed_normal = RustcArgs::parse(&args_normal).unwrap();
        let parsed_tarpaulin = RustcArgs::parse(&args_tarpaulin).unwrap();

        let fh = FileHasher::new();
        let pn = PathNormalizer::empty();
        let key_normal = compute_cache_key(&parsed_normal, &fh, &pn).unwrap();
        let key_tarpaulin = compute_cache_key(&parsed_tarpaulin, &fh, &pn).unwrap();

        assert_ne!(
            key_normal, key_tarpaulin,
            "--cfg=tarpaulin must produce a different cache key"
        );
    }

    #[test]
    fn test_coverage_keys_consistent_across_remap_forms() {
        let _lock = key_test_lock();
        // Both joined and two-arg forms of instrument-coverage should produce
        // the same cache key (both map to codegen opt "instrument-coverage")
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let args_joined: Vec<String> = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "mylib".to_string(),
            source.to_string_lossy().to_string(),
            "--crate-type".to_string(),
            "lib".to_string(),
            "-Cinstrument-coverage".to_string(),
        ];

        let mut args_two = args_joined[..6].to_vec();
        args_two.extend(["-C".to_string(), "instrument-coverage".to_string()]);

        let parsed_joined = RustcArgs::parse(&args_joined).unwrap();
        let parsed_two = RustcArgs::parse(&args_two).unwrap();

        let fh = FileHasher::new();
        let pn = PathNormalizer::empty();
        let key_joined = compute_cache_key(&parsed_joined, &fh, &pn).unwrap();
        let key_two = compute_cache_key(&parsed_two, &fh, &pn).unwrap();

        assert_eq!(
            key_joined, key_two,
            "joined and two-arg forms of instrument-coverage should produce identical keys"
        );
    }

    #[test]
    fn test_cache_key_version_affects_key() {
        let _lock = key_test_lock();
        // Verify that the key version is hashed by checking that the hasher
        // receives the version string. We do this indirectly: compute a key
        // and then verify the same inputs produce the same key (determinism).

        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let args_vec: Vec<String> = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "mylib".to_string(),
            source.to_string_lossy().to_string(),
            "--crate-type".to_string(),
            "lib".to_string(),
        ];

        // Compute twice — must be deterministic (version baked in)
        let parsed1 = RustcArgs::parse(&args_vec).unwrap();
        let parsed2 = RustcArgs::parse(&args_vec).unwrap();
        let fh = FileHasher::new();
        let pn = PathNormalizer::empty();
        let key1 = compute_cache_key(&parsed1, &fh, &pn).unwrap();
        let key2 = compute_cache_key(&parsed2, &fh, &pn).unwrap();
        assert_eq!(
            key1, key2,
            "key must be deterministic with version baked in"
        );

        // Prove that different version values produce different hashes by
        // simulating what compute_cache_key does with version=N vs version=N+1.
        // We can't change the const, but we can replicate the hashing logic
        // to prove the version input is material.
        let payload = b"rustc_version:1.80.0\n";
        for (v_a, v_b) in [(1u32, 2u32), (0, 1), (1, 100)] {
            let hash = |version: u32| {
                let mut h = blake3::Hasher::new();
                h.update(b"key_version:");
                h.update(version.to_string().as_bytes());
                h.update(b"\n");
                h.update(payload);
                h.finalize().to_hex().to_string()
            };
            assert_ne!(
                hash(v_a),
                hash(v_b),
                "version {} vs {} must produce different hashes",
                v_a,
                v_b
            );
        }
    }

    // --- parse_dep_info tests (pure parser, no I/O) ---

    #[test]
    fn test_parse_dep_info_basic() {
        let input = "target.d: src/lib.rs src/server.rs src/utils.rs\n";
        let files = parse_dep_info(input);
        assert_eq!(files.len(), 3);
        assert_eq!(files[0], std::path::PathBuf::from("src/lib.rs"));
        assert_eq!(files[1], std::path::PathBuf::from("src/server.rs"));
        assert_eq!(files[2], std::path::PathBuf::from("src/utils.rs"));
    }

    #[test]
    fn test_parse_dep_info_escaped_spaces() {
        let input = "target.d: src/my\\ file.rs src/lib.rs\n";
        let files = parse_dep_info(input);
        assert_eq!(files.len(), 2);
        assert!(
            files
                .iter()
                .any(|p| p == &std::path::PathBuf::from("src/my file.rs"))
        );
        assert!(
            files
                .iter()
                .any(|p| p == &std::path::PathBuf::from("src/lib.rs"))
        );
    }

    #[test]
    fn test_parse_dep_info_empty() {
        assert!(parse_dep_info("").is_empty());
        assert!(parse_dep_info("target.d:").is_empty());
        assert!(parse_dep_info("no colon here").is_empty());
    }

    #[test]
    fn test_parse_dep_info_single_file() {
        let input = "deps.d: src/main.rs\n";
        let files = parse_dep_info(input);
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], std::path::PathBuf::from("src/main.rs"));
    }

    #[test]
    fn test_parse_dep_info_absolute_paths() {
        let input = "deps.d: /home/user/project/src/lib.rs /home/user/project/src/mod.rs\n";
        let files = parse_dep_info(input);
        assert_eq!(files.len(), 2);
        assert_eq!(
            files[0],
            std::path::PathBuf::from("/home/user/project/src/lib.rs")
        );
        assert_eq!(
            files[1],
            std::path::PathBuf::from("/home/user/project/src/mod.rs")
        );
    }

    // --- parse_env_dep_info tests (pure parser, no I/O) ---

    #[test]
    fn test_parse_env_deps_basic() {
        let input =
            "deps.d: src/lib.rs\n# env-dep:CARGO_PKG_VERSION=1.0.0\n# env-dep:OUT_DIR=/tmp/out\n";
        let env_deps = parse_env_dep_info(input);
        assert_eq!(env_deps.len(), 2);
        assert!(
            env_deps
                .iter()
                .any(|(k, v)| k == "CARGO_PKG_VERSION" && v == "1.0.0")
        );
        assert!(env_deps.iter().any(|(k, _)| k == "OUT_DIR"));
    }

    #[test]
    fn test_parse_env_deps_returns_raw_values() {
        // Parser stores values verbatim; the normalization decision
        // belongs to `compute_cache_key` (which knows whether OUT_DIR
        // can be safely sentinelized — see `path_is_only_used_for_includes`).
        // Pre-normalizing here would erase the absolute-path
        // information the discriminator needs to read.
        let input = "deps.d: src/lib.rs\n# env-dep:OUT_DIR=/some/abs/path/target/debug/build/foo\n";
        let env_deps = parse_env_dep_info(input);
        assert_eq!(env_deps.len(), 1);
        assert_eq!(env_deps[0].0, "OUT_DIR");
        assert_eq!(env_deps[0].1, "/some/abs/path/target/debug/build/foo");
    }

    #[test]
    fn test_parse_env_deps_empty() {
        let input = "deps.d: src/lib.rs\n";
        let env_deps = parse_env_dep_info(input);
        assert!(env_deps.is_empty());
    }

    #[test]
    fn test_parse_env_deps_no_value() {
        let input = "deps.d: src/lib.rs\n# env-dep:UNSET_VAR\n";
        let env_deps = parse_env_dep_info(input);
        assert_eq!(env_deps.len(), 1);
        assert_eq!(env_deps[0].0, "UNSET_VAR");
    }

    // --- FileHasher tests ---

    #[test]
    fn test_file_hasher_deterministic() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.rs");
        std::fs::write(&file, b"fn main() {}").unwrap();

        let hasher = FileHasher::new();
        let hash1 = hasher.hash(&file).unwrap();
        let hash2 = hasher.hash(&file).unwrap();
        assert_eq!(hash1, hash2, "FileHasher must be deterministic");
    }

    #[test]
    fn env_dep_use_memo_reuses_every_scan_answer() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("idx.sqlite");
        let source = dir.path().join("lib.rs");
        std::fs::write(
            &source,
            br#"include!(env!("GEN")); pub const OUT: &str = env!("OUT_DIR"); pub const N: usize = 1;"#,
        )
        .unwrap();

        let hasher = FileHasher::persistent(&db);
        let content_hash = hasher.hash(&source).unwrap();
        let answers = [
            ("OUT_DIR", SourceEnvDepUse::RuntimeValue),
            ("GEN", SourceEnvDepUse::IncludeLocator),
            ("OTHER_DIR", SourceEnvDepUse::Unused),
        ];
        for (var, expected) in answers {
            assert_eq!(hasher.env_dep_use(&source, var).unwrap(), expected, "{var}");
        }
        drop(hasher);

        // A fresh wrapper can answer every decision from SQLite using the
        // content hash it already obtained while building the cache key. The
        // source is gone, so any attempted reread would fail this test.
        std::fs::remove_file(&source).unwrap();
        let fresh = FileHasher::persistent(&db);
        for (var, expected) in answers {
            assert_eq!(
                fresh
                    .env_dep_use_for_hash(&source, var, &content_hash)
                    .unwrap(),
                expected,
                "{var}"
            );
        }
    }

    #[test]
    fn env_dep_use_memo_rescans_rows_from_other_scanner_versions() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("idx.sqlite");
        let source = dir.path().join("lib.rs");
        std::fs::write(
            &source,
            br#"pub const S: &str = env!(concat!("OUT", "_DIR"));"#,
        )
        .unwrap();
        let content_hash = hash_file(&source).unwrap();

        let cache = FileHashCache::open(&db).unwrap();
        let unused = SourceEnvDepUse::Unused.memo_code();
        cache
            .put_source_env_dep_use(
                &content_hash,
                "OUT_DIR",
                SOURCE_ENV_DEP_SCANNER_VERSION - 1,
                unused,
            )
            .unwrap();
        cache
            .put_source_env_dep_use(&content_hash, "OTHER", SOURCE_ENV_DEP_SCANNER_VERSION, 7)
            .unwrap();
        drop(cache);

        let hasher = FileHasher::persistent(&db);
        assert_eq!(
            hasher
                .env_dep_use_for_hash(&source, "OUT_DIR", &content_hash)
                .unwrap(),
            SourceEnvDepUse::RuntimeValue,
            "an older scanner's answer must not be reused"
        );
        assert_eq!(
            hasher
                .env_dep_use_for_hash(&source, "OTHER", &content_hash)
                .unwrap(),
            SourceEnvDepUse::RuntimeValue,
            "an unknown stored code must be rescanned"
        );
        drop(hasher);

        let cache = FileHashCache::open(&db).unwrap();
        for var in ["OUT_DIR", "OTHER"] {
            assert_eq!(
                cache
                    .get_source_env_dep_use(&content_hash, var, SOURCE_ENV_DEP_SCANNER_VERSION)
                    .unwrap(),
                Some(SourceEnvDepUse::RuntimeValue.memo_code()),
                "the rescan replaces the stale row for {var}"
            );
        }
    }

    #[test]
    fn env_dep_use_memo_codes_round_trip() {
        for answer in [
            SourceEnvDepUse::Unused,
            SourceEnvDepUse::IncludeLocator,
            SourceEnvDepUse::RuntimeValue,
        ] {
            assert_eq!(
                SourceEnvDepUse::from_memo_code(answer.memo_code()),
                Some(answer)
            );
        }
        assert_eq!(
            [
                SourceEnvDepUse::Unused.memo_code(),
                SourceEnvDepUse::IncludeLocator.memo_code(),
                SourceEnvDepUse::RuntimeValue.memo_code(),
            ],
            [0, 1, 2]
        );
        assert_eq!(SourceEnvDepUse::from_memo_code(3), None);
        assert_eq!(SourceEnvDepUse::from_memo_code(-1), None);
    }

    #[test]
    fn runtime_env_use_scan_rejects_content_changed_after_hashing() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, br#"include!(concat!(env!("OUT_DIR"), "/x.rs"));"#).unwrap();

        let hasher = FileHasher::new();
        hasher.hash(&source).unwrap();
        std::fs::write(&source, br#"pub const OUT: &str = env!("OUT_DIR");"#).unwrap();

        let error = hasher.env_dep_use(&source, "OUT_DIR").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("changed between content hashing"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn cc_preprocess_memo_support_requires_persistent_cache() {
        assert!(!FileHasher::new().supports_cc_preprocess_memo());

        let dir = tempfile::tempdir().unwrap();
        let persistent = FileHasher::persistent(&dir.path().join("idx.sqlite"));
        assert!(persistent.supports_cc_preprocess_memo());
    }

    /// The mapped hash is memoised by raw content and map set: a second unit
    /// that reads the same bytes, at any path, takes the memo instead of
    /// reading and rewriting the file; another map set or an empty key does
    /// not; a memo error or a missing store still computes.
    #[test]
    fn mapped_hashes_are_memoised_by_content_and_map_set() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("idx.sqlite");
        let header = dir.path().join("header.h");
        let copy = dir.path().join("copy.h");
        let other = dir.path().join("other.h");
        // Above MIN_PERSISTED_HASH_BYTES so the raw hash itself is memoised.
        let body = "#define VALUE 1\n".repeat(64);
        std::fs::write(&header, &body).unwrap();
        std::fs::write(&copy, &body).unwrap();
        std::fs::write(&other, format!("{body}#define OTHER 2\n")).unwrap();
        let reads = std::cell::Cell::new(0usize);
        let counting = |path: &Path| -> Option<String> {
            reads.set(reads.get() + 1);
            let bytes = std::fs::read(path).ok()?;
            Some(
                blake3::hash(&[b"mapped:".as_slice(), &bytes].concat())
                    .to_hex()
                    .to_string(),
            )
        };
        let name = |path: &Path| (path.to_string_lossy().into_owned(), path.to_path_buf());

        let hasher = FileHasher::persistent(&db);
        let first = hasher
            .cc_preprocess_fingerprints(&[name(&header)], "maps-a", &counting)
            .unwrap();
        assert_eq!(reads.get(), 1);
        let second = hasher
            .cc_preprocess_fingerprints(&[name(&copy), name(&other)], "maps-a", &counting)
            .unwrap();
        assert_eq!(
            reads.get(),
            2,
            "the copy took the memo, the other file did not"
        );
        let by_name: std::collections::HashMap<_, _> =
            second.iter().map(|i| (i.name.as_str(), i)).collect();
        assert_eq!(by_name[name(&copy).0.as_str()].mapped, first[0].mapped);
        assert_ne!(by_name[name(&other).0.as_str()].mapped, first[0].mapped);

        // A fresh process (new hasher on the same index) still has the memo.
        let later = FileHasher::persistent(&db);
        later
            .cc_preprocess_fingerprints(&[name(&header)], "maps-a", &counting)
            .unwrap();
        assert_eq!(reads.get(), 2, "the memo survives the process");
        // Another map set rewrites bytes differently and computes again.
        later
            .cc_preprocess_fingerprints(&[name(&header)], "maps-b", &counting)
            .unwrap();
        assert_eq!(reads.get(), 3);
        // An empty key never memoises.
        later
            .cc_preprocess_fingerprints(&[name(&header)], "", &counting)
            .unwrap();
        later
            .cc_preprocess_fingerprints(&[name(&header)], "", &counting)
            .unwrap();
        assert_eq!(reads.get(), 5);
        // Without a store, every unit computes.
        let bare = FileHasher::new();
        bare.cc_preprocess_fingerprints(&[name(&header)], "maps-a", &counting)
            .unwrap();
        assert_eq!(reads.get(), 6);
    }

    /// The assembler scan runs once per distinct content and its verdict is
    /// memoised across processes; an unreadable file is skipped and not
    /// recorded.
    #[test]
    fn assembler_scans_are_memoised_by_content() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("idx.sqlite");
        let clean = dir.path().join("clean.h");
        let twin = dir.path().join("twin.h");
        let pasted = dir.path().join("pasted.h");
        let body = "#define VALUE 1\n".repeat(64);
        std::fs::write(&clean, &body).unwrap();
        std::fs::write(&twin, &body).unwrap();
        std::fs::write(
            &pasted,
            format!("{body}__asm__(\".incbin \\\"blob\\\"\");\n"),
        )
        .unwrap();
        let scans = std::cell::Cell::new(0usize);
        let scan = |path: &Path| -> Option<Option<&'static str>> {
            scans.set(scans.get() + 1);
            let text = std::fs::read_to_string(path).ok()?;
            Some(text.contains(".incbin").then_some(".incbin"))
        };
        let name = |path: &Path| (path.to_string_lossy().into_owned(), path.to_path_buf());
        let hasher = FileHasher::persistent(&db);
        let inputs = hasher
            .cc_preprocess_fingerprints(&[name(&clean), name(&twin)], "", &|_| Some(String::new()))
            .unwrap();
        assert_eq!(hasher.cc_inputs_hide_assembler_input(&inputs, &scan), None);
        assert_eq!(scans.get(), 1, "twins share one scan");
        let later = FileHasher::persistent(&db);
        let inputs =
            later
                .cc_preprocess_fingerprints(&[name(&clean), name(&pasted)], "", &|_| {
                    Some(String::new())
                })
                .unwrap();
        assert_eq!(
            later
                .cc_inputs_hide_assembler_input(&inputs, &scan)
                .as_deref(),
            Some(".incbin")
        );
        assert_eq!(
            scans.get(),
            2,
            "the clean verdict survived the process; only the new file was scanned"
        );
        assert_eq!(
            later
                .cc_inputs_hide_assembler_input(&inputs, &scan)
                .as_deref(),
            Some(".incbin"),
            "the construct verdict is memoised too"
        );
        assert_eq!(scans.get(), 2);
        // An unreadable file is skipped and left for the next run.
        let mut gone = inputs.clone();
        gone[0].fingerprint.path = dir.path().join("absent.h").to_string_lossy().into_owned();
        gone[0].content = "c".repeat(64);
        assert_eq!(
            later.cc_inputs_hide_assembler_input(&gone[..1], &scan),
            None
        );
        assert_eq!(
            later.cc_inputs_hide_assembler_input(&gone[..1], &scan),
            None
        );
        assert_eq!(
            scans.get(),
            4,
            "an unreadable file is scanned again next time"
        );
    }

    #[test]
    fn cc_preprocess_memo_requires_every_input_fingerprint_to_match() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("idx.sqlite");
        let source = dir.path().join("source.c");
        let header = dir.path().join("header.h");
        std::fs::write(&source, "#include \"header.h\"\n").unwrap();
        std::fs::write(&header, "#define VALUE 1\n").unwrap();

        let hasher = FileHasher::persistent(&db);
        let inputs = hasher
            .cc_preprocess_fingerprints(
                &[
                    (source.to_string_lossy().into_owned(), source.clone()),
                    (header.to_string_lossy().into_owned(), header.clone()),
                ],
                "",
                &no_mapping,
            )
            .unwrap();
        let pp_hash = "a".repeat(64);
        hasher.cc_preprocess_memo_record_if_unchanged("memo-key", &pp_hash, &inputs, &no_mapping);
        assert_eq!(
            hasher
                .cc_preprocess_memo_lookup("memo-key", no_remap, &no_mapping)
                .map(|(hash, _)| hash)
                .as_deref(),
            Some(pp_hash.as_str())
        );

        let inputs_json = serde_json::to_string(&inputs).unwrap();
        let cache = hasher.cache.as_ref().unwrap();
        cache
            .put_cc_preprocess_memo("short-hash", "a", &inputs_json)
            .unwrap();
        cache
            .put_cc_preprocess_memo("non-hex-hash", &"z".repeat(64), &inputs_json)
            .unwrap();
        assert_eq!(
            hasher.cc_preprocess_memo_lookup("short-hash", no_remap, &no_mapping),
            None
        );
        assert_eq!(
            hasher.cc_preprocess_memo_lookup("non-hex-hash", no_remap, &no_mapping),
            None
        );

        // An input written inside the invocation window is what a fresh
        // checkout looks like. It used to refuse both halves of the memo,
        // which meant CI could never memoise anything; the content hash makes
        // the recency irrelevant.
        let mut fresh_hasher = FileHasher::persistent(&db);
        fresh_hasher.arm_too_new_guard(i64::MAX, 0);
        assert_eq!(
            fresh_hasher
                .cc_preprocess_memo_lookup("memo-key", no_remap, &no_mapping)
                .map(|(hash, _)| hash)
                .as_deref(),
            Some(pp_hash.as_str()),
            "unchanged bytes must hit however recently they were written"
        );
        fresh_hasher.cc_preprocess_memo_record_if_unchanged(
            "too-new",
            &pp_hash,
            &inputs,
            &no_mapping,
        );
        assert!(
            fresh_hasher
                .cache
                .as_ref()
                .unwrap()
                .get_cc_preprocess_memo("too-new")
                .unwrap()
                .is_some(),
            "a fresh checkout must still be able to publish a memo"
        );

        std::fs::write(&header, "#define VALUE 12345\n").unwrap();
        assert_eq!(
            hasher.cc_preprocess_memo_lookup("memo-key", no_remap, &no_mapping),
            None,
            "a changed transitive header must force preprocessing"
        );
        hasher.cc_preprocess_memo_record_if_unchanged("changed", &pp_hash, &inputs, &no_mapping);
        assert!(
            hasher
                .cache
                .as_ref()
                .unwrap()
                .get_cc_preprocess_memo("changed")
                .unwrap()
                .is_none(),
            "changed inputs must not publish a memo"
        );
    }

    /// Tests that do not exercise prefix maps hash contents as they are.
    fn no_mapping(path: &Path) -> Option<String> {
        hash_file(path).ok()
    }

    /// A resolver for tests that record and read in one place: the recorded
    /// name is the path.
    fn no_remap(name: &str) -> Vec<PathBuf> {
        vec![PathBuf::from(name)]
    }

    /// The case the benchmark exposed: a generated header names its own build
    /// directory, so two checkouts hold different bytes that the prefix maps
    /// rewrite to the same thing. The expansion is hashed after mapping, so
    /// the key already treats them as equal; comparing raw bytes alone left
    /// the memo stricter than the key it feeds, and every `-sys` unit behind
    /// such a header preprocessed again in the second checkout.
    #[test]
    fn cc_preprocess_memo_compares_contents_as_the_expansion_sees_them() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("idx.sqlite");
        let a = dir.path().join("a");
        let b = dir.path().join("b");
        for tree in [&a, &b] {
            std::fs::create_dir_all(tree).unwrap();
        }
        // Each tree's header names its own directory, as a build script writes.
        std::fs::write(a.join("gen.h"), format!("#define P \"{}\"\n", a.display())).unwrap();
        std::fs::write(b.join("gen.h"), format!("#define P \"{}\"\n", b.display())).unwrap();

        // The maps each tree would use: its own root onto one shared sentinel.
        let map_under = |root: &std::path::Path| {
            let root = root.to_string_lossy().into_owned();
            move |path: &Path| -> Option<String> {
                let text = std::fs::read_to_string(path).ok()?;
                Some(
                    blake3::hash(text.replace(&root, "<root>").as_bytes())
                        .to_hex()
                        .to_string(),
                )
            }
        };

        let hasher = FileHasher::persistent(&db);
        let inputs = hasher
            .cc_preprocess_fingerprints(
                &[("<root>/gen.h".to_string(), a.join("gen.h"))],
                "",
                &map_under(&a),
            )
            .unwrap();
        assert_ne!(
            inputs[0].content, inputs[0].mapped,
            "raw and mapped hashes differ for a file that names its own path"
        );
        let pp_hash = "d".repeat(64);
        hasher.cc_preprocess_memo_record_if_unchanged(
            "memo-key",
            &pp_hash,
            &inputs,
            &map_under(&a),
        );

        let resolve_in_b = |name: &str| vec![b.join(name.trim_start_matches("<root>/"))];
        assert_eq!(
            FileHasher::persistent(&db)
                .cc_preprocess_memo_lookup("memo-key", resolve_in_b, &map_under(&b))
                .map(|(hash, _)| hash)
                .as_deref(),
            Some(pp_hash.as_str()),
            "the same header under another root must reuse the expansion"
        );

        // A real difference still misses, mapping or no mapping.
        std::fs::write(
            b.join("gen.h"),
            format!("#define P \"{}\"\n#define EXTRA 1\n", b.display()),
        )
        .unwrap();
        assert_eq!(
            FileHasher::persistent(&db).cc_preprocess_memo_lookup(
                "memo-key",
                resolve_in_b,
                &map_under(&b)
            ),
            None,
            "a header that gained a definition must force a fresh preprocess"
        );
    }

    /// The hole the relocate-modified e2e phase found: a second checkout with
    /// an edited copy must not be answered by the unedited original, which is
    /// still sitting on disk where the memo recorded it. Reusing that
    /// expansion would derive the unedited key and serve a stale artifact for
    /// modified source.
    #[test]
    fn cc_preprocess_memo_ignores_the_recording_tree_when_reading_another() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("idx.sqlite");
        let original = dir.path().join("a");
        let relocated = dir.path().join("b");
        for tree in [&original, &relocated] {
            std::fs::create_dir_all(tree).unwrap();
        }
        let source_of = |tree: &std::path::Path| tree.join("source.c");
        std::fs::write(source_of(&original), "int v(void) { return 1; }\n").unwrap();

        // Record in the original tree, under a mapped name both trees share.
        let hasher = FileHasher::persistent(&db);
        let inputs = hasher
            .cc_preprocess_fingerprints(
                &[("<root>/source.c".to_string(), source_of(&original))],
                "",
                &no_mapping,
            )
            .unwrap();
        let pp_hash = "c".repeat(64);
        hasher.cc_preprocess_memo_record_if_unchanged("memo-key", &pp_hash, &inputs, &no_mapping);

        // The relocated tree has an EDITED copy. The original still exists,
        // untouched, at the path the record names.
        std::fs::write(source_of(&relocated), "int v(void) { return 999; }\n").unwrap();
        let resolve_in_relocated =
            |name: &str| vec![relocated.join(name.trim_start_matches("<root>/"))];
        assert_eq!(
            FileHasher::persistent(&db).cc_preprocess_memo_lookup(
                "memo-key",
                resolve_in_relocated,
                &no_mapping
            ),
            None,
            "the edited copy must miss even though the original is unchanged"
        );

        // The same resolver on an identical copy is a hit: this is not just
        // rejecting everything from another tree.
        std::fs::write(source_of(&relocated), "int v(void) { return 1; }\n").unwrap();
        assert_eq!(
            FileHasher::persistent(&db)
                .cc_preprocess_memo_lookup("memo-key", resolve_in_relocated, &no_mapping)
                .map(|(hash, _)| hash)
                .as_deref(),
            Some(pp_hash.as_str()),
            "an identical copy in another tree must still reuse the expansion"
        );
    }

    /// The case the memo exists for and used to miss: the same bytes at new
    /// metadata. A second worktree gives every file a new inode and mtime,
    /// and a build script that regenerates a header rewrites it identically.
    /// Neither changes what the preprocessor would produce.
    #[test]
    fn cc_preprocess_memo_survives_new_metadata_for_unchanged_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("idx.sqlite");
        let source = dir.path().join("source.c");
        let header = dir.path().join("header.h");
        let source_bytes = "#include \"header.h\"\nint v(void) { return VALUE; }\n";
        let header_bytes = "#define VALUE 1\n";
        std::fs::write(&source, source_bytes).unwrap();
        std::fs::write(&header, header_bytes).unwrap();

        let hasher = FileHasher::persistent(&db);
        let inputs = hasher
            .cc_preprocess_fingerprints(
                &[
                    (source.to_string_lossy().into_owned(), source.clone()),
                    (header.to_string_lossy().into_owned(), header.clone()),
                ],
                "",
                &no_mapping,
            )
            .unwrap();
        assert!(
            inputs.iter().all(|input| input.content.len() == 64),
            "every recorded input carries a content hash"
        );
        let pp_hash = "b".repeat(64);
        hasher.cc_preprocess_memo_record_if_unchanged("memo-key", &pp_hash, &inputs, &no_mapping);

        // Rewrite identical bytes with different metadata. Filesystems can
        // reuse the inode and timestamp on an immediate rewrite, so set the
        // mtime explicitly to exercise content-based memo validation.
        for (path, bytes) in [(&source, source_bytes), (&header, header_bytes)] {
            std::fs::remove_file(path).unwrap();
            std::fs::write(path, bytes).unwrap();
            filetime::set_file_mtime(path, filetime::FileTime::from_unix_time(1, 0)).unwrap();
        }
        let rewritten = FileFingerprint::from_path(&header).unwrap();
        assert_ne!(
            rewritten, inputs[0].fingerprint,
            "the rewrite must actually change the metadata this test is about"
        );

        let reader = FileHasher::persistent(&db);
        assert_eq!(
            reader
                .cc_preprocess_memo_lookup("memo-key", no_remap, &no_mapping)
                .map(|(hash, _)| hash)
                .as_deref(),
            Some(pp_hash.as_str()),
            "identical bytes at new metadata must reuse the expansion"
        );

        // One byte of difference is still a miss, whatever the metadata says.
        std::fs::write(&header, "#define VALUE 2\n").unwrap();
        filetime::set_file_mtime(&header, filetime::FileTime::from_unix_time(2, 0)).unwrap();
        assert_eq!(
            FileHasher::persistent(&db).cc_preprocess_memo_lookup(
                "memo-key",
                no_remap,
                &no_mapping
            ),
            None,
            "changed bytes must force a fresh preprocess"
        );
    }

    #[test]
    fn too_new_guard_flags_inputs_modified_after_build_start() {
        // kunobi-ninja/kache#324: when armed, the guard flags any hashed input
        // whose mtime/ctime is at/after the build's start (its content is racy
        // vs what the compiler reads). Disabled by default.
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("idx.sqlite");
        let file = dir.path().join("input.rs");
        std::fs::write(&file, b"pub fn x() {}").unwrap();

        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;

        // Disabled (default) → never flagged.
        let off = FileHasher::persistent(&db);
        off.hash(&file).unwrap();
        assert!(!off.too_new(), "guard is off by default");

        // Build "started" in the future → the file predates it → not too-new.
        let mut before = FileHasher::persistent(&db);
        before.arm_too_new_guard(now_ns + 60_000_000_000, 0);
        before.hash(&file).unwrap();
        assert!(
            !before.too_new(),
            "a file modified before the build started is not too-new"
        );

        // Build "started" in the past → the file was modified after → too-new.
        let mut after = FileHasher::persistent(&db);
        after.arm_too_new_guard(now_ns - 60_000_000_000, 0);
        after.hash(&file).unwrap();
        assert!(
            after.too_new(),
            "a file modified after the build started must be flagged too-new"
        );

        // The store-free/daemon wrapper path uses `FileHasher::new()`. Its
        // guard must not silently become a no-op just because no local hash
        // memo is open.
        let mut cacheless = FileHasher::new();
        cacheless.arm_too_new_guard(now_ns - 60_000_000_000, 0);
        cacheless.hash(&file).unwrap();
        assert!(
            cacheless.too_new(),
            "a cacheless hasher must enforce the same too-new guard"
        );
    }

    #[test]
    fn guarded_inputs_record_only_while_armed() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("input.rs");
        std::fs::write(&file, b"pub fn x() {}").unwrap();

        let disarmed = FileHasher::new();
        disarmed.hash(&file).unwrap();
        assert!(
            disarmed.take_guarded_inputs().is_empty(),
            "a disarmed hasher records nothing to verify"
        );

        let mut armed = FileHasher::new();
        armed.arm_too_new_guard(1, 0);
        armed.hash(&file).unwrap();
        armed.hash(&file).unwrap();
        assert_eq!(
            armed.take_guarded_inputs().len(),
            2,
            "every hash while armed is recorded for post-compile verification"
        );
        assert!(
            armed.take_guarded_inputs().is_empty(),
            "taking the snapshot drains it"
        );
    }

    #[test]
    fn guarded_inputs_empty_set_never_excuses() {
        assert!(
            !FileHasher::guarded_inputs_unchanged_since_hash(&[]),
            "a vacuous check must not waive a tripped guard"
        );
    }

    #[test]
    fn guarded_inputs_reject_changed_or_missing_files() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("input.rs");
        std::fs::write(&file, b"pub fn x() {}").unwrap();
        let recorded = FileFingerprint::from_path(&file).unwrap();

        std::fs::write(&file, b"pub fn x() { 1 }").unwrap();
        assert!(
            !FileHasher::guarded_inputs_unchanged_since_hash(std::slice::from_ref(&recorded)),
            "rewritten bytes must fail verification even when the wall clock cannot tell"
        );

        std::fs::remove_file(&file).unwrap();
        assert!(
            !FileHasher::guarded_inputs_unchanged_since_hash(std::slice::from_ref(&recorded)),
            "a file that vanished mid-build must fail verification"
        );
    }

    #[test]
    fn guarded_inputs_reject_weak_identity() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("input.rs");
        std::fs::write(&file, b"pub fn x() {}").unwrap();
        let mut recorded = FileFingerprint::from_path(&file).unwrap();
        recorded.inode = 0;
        assert!(
            !FileHasher::guarded_inputs_unchanged_since_hash(std::slice::from_ref(&recorded)),
            "without an inode a replace-by-rename is invisible, so verification must fail closed"
        );
    }

    #[cfg(unix)]
    #[test]
    fn guarded_inputs_verify_despite_future_mtimes() {
        // The clock-domain case: the filesystem clock runs ahead of the host
        // (NFS skew, a fresh checkout stamped in the future), so the
        // wall-clock guard trips on files the build never touched. Identical
        // fingerprints before and after still prove nothing changed.
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("input.rs");
        std::fs::write(&file, b"pub fn x() {}").unwrap();
        filetime::set_file_mtime(&file, filetime::FileTime::from_unix_time(2_000_000_000, 0))
            .unwrap();

        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        let mut hasher = FileHasher::new();
        hasher.arm_too_new_guard(now_ns, 0);
        hasher.hash(&file).unwrap();
        assert!(
            hasher.too_new(),
            "a future mtime must still trip the wall-clock guard"
        );
        assert!(
            FileHasher::guarded_inputs_unchanged_since_hash(&hasher.take_guarded_inputs()),
            "untouched bytes verify despite the skewed clock"
        );
    }

    #[test]
    fn a_units_headers_come_from_one_memo_lookup_once_settled() {
        let dir = tempfile::tempdir().unwrap();
        // Small on purpose: `hash` never memoises these, the capture does.
        let headers: Vec<(String, PathBuf)> = (0..5)
            .map(|i| {
                let path = dir.path().join(format!("h{i}.h"));
                std::fs::write(&path, format!("#define H{i} {i}\n")).unwrap();
                (format!("h{i}.h"), path)
            })
            .collect();
        let db = rusqlite::Connection::open_in_memory().unwrap();
        ensure_file_hash_cache_schema(&db).unwrap();
        let mapped = |path: &Path| std::fs::read_to_string(path).ok();

        let mut first = FileHasher::from_cache(FileHashCache::Borrowed(&db));
        first.arm_too_new_guard(1, 0);
        let cold = first
            .cc_preprocess_fingerprints(&headers, "maps", &mapped)
            .unwrap();
        assert_eq!(first.stats().cache_misses, 5);
        assert_eq!(first.stats().cache_hits, 0);
        // Every header is registered for the post-compile revalidation and
        // remembered for later users in this process.
        assert_eq!(first.guard_inputs.borrow().len(), 5);
        for (_, path) in &headers {
            assert!(
                first
                    .recent_hashes
                    .borrow()
                    .contains_key(&absolute_path(path))
            );
        }
        first.flush_memo_as_if_settled();

        let second = FileHasher::from_cache(FileHashCache::Borrowed(&db));
        let warm = second
            .cc_preprocess_fingerprints(&headers, "maps", &mapped)
            .unwrap();
        assert_eq!(second.stats().cache_hits, 5, "all five from the memo");
        assert_eq!(second.stats().cache_misses, 0);
        assert_eq!(second.stats().bytes_hashed, 0);
        let hashes = |inputs: &[CcPreprocessMemoInput]| {
            inputs.iter().map(|i| i.content.clone()).collect::<Vec<_>>()
        };
        assert_eq!(hashes(&warm), hashes(&cold));

        // A header rewritten since then is hashed again, not served stale.
        std::fs::write(&headers[0].1, "#define H0 changed\n").unwrap();
        let third = FileHasher::from_cache(FileHashCache::Borrowed(&db));
        let after = third
            .cc_preprocess_fingerprints(&headers, "maps", &mapped)
            .unwrap();
        assert_eq!(third.stats().cache_misses, 1);
        assert_ne!(hashes(&after)[0], hashes(&cold)[0]);
    }

    #[test]
    fn an_unarmed_guard_keeps_no_headers_for_revalidation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("h.h");
        std::fs::write(&path, "#define H 1\n").unwrap();
        let headers = vec![("h.h".to_string(), path)];
        let mapped = |path: &Path| std::fs::read_to_string(path).ok();

        let hasher = FileHasher::new();
        hasher
            .cc_preprocess_fingerprints(&headers, "maps", &mapped)
            .unwrap();
        assert!(hasher.take_guarded_inputs().is_empty());
    }

    #[test]
    fn flushing_on_the_real_clock_keeps_a_long_settled_stamp() {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        ensure_file_hash_cache_schema(&db).unwrap();
        // Last changed at the epoch: settled by any clock this runs under.
        let stamp = FileFingerprint {
            path: "/old/header.h".to_string(),
            size: 12,
            mtime_ns: 1,
            ctime_ns: 1,
            inode: 7,
        };
        let hasher = FileHasher::from_cache(FileHashCache::Borrowed(&db));
        hasher
            .pending_memo
            .borrow_mut()
            .push((stamp.clone(), "hash".to_string()));
        hasher.flush_memo();

        let reader = FileHasher::from_cache(FileHashCache::Borrowed(&db));
        let memo = reader.memoised_hashes(std::iter::once(&stamp));
        assert_eq!(memo.get("/old/header.h").map(String::as_str), Some("hash"));
    }

    #[test]
    fn a_file_changed_within_the_settle_window_is_hashed_but_not_memoised() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("fresh.h");
        std::fs::write(&file, vec![1u8; 70 * 1024]).unwrap();
        let db = rusqlite::Connection::open_in_memory().unwrap();
        ensure_file_hash_cache_schema(&db).unwrap();
        let hasher = FileHasher::from_cache(FileHashCache::Borrowed(&db));
        let hash = hasher.hash(&file).unwrap();
        hasher.flush_memo();
        let stamp = FileFingerprint::from_path(&file).unwrap();
        let cache = FileHashCache::Borrowed(&db);
        assert_eq!(
            cache.get(&stamp).unwrap(),
            None,
            "a second write in this timestamp tick could reuse this stamp"
        );
        // Once settled, the same flush records it.
        hasher
            .pending_memo
            .borrow_mut()
            .push((stamp.clone(), hash.clone()));
        hasher.flush_memo_as_if_settled();
        assert_eq!(cache.get(&stamp).unwrap(), Some(hash));
    }

    #[test]
    fn test_file_hasher_persistent_cache_invalidates_on_metadata_change() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("index.db");
        let file = dir.path().join("large.rlib");
        std::fs::write(&file, vec![1u8; 70 * 1024]).unwrap();

        let hasher = FileHasher::persistent(&db_path);
        let first = hasher.hash(&file).unwrap();
        hasher.flush_memo_as_if_settled();
        let first_stats = hasher.stats();
        assert_eq!(first_stats.cache_hits, 0);
        assert_eq!(first_stats.cache_misses, 1);
        assert!(first_stats.bytes_hashed > 0);

        let second_hasher = FileHasher::persistent(&db_path);
        let second = second_hasher.hash(&file).unwrap();
        let second_stats = second_hasher.stats();
        assert_eq!(first, second);
        assert_eq!(second_stats.cache_hits, 1);
        assert_eq!(second_stats.cache_misses, 0);

        std::fs::write(&file, vec![2u8; 70 * 1024]).unwrap();
        let changed = FileHasher::persistent(&db_path).hash(&file).unwrap();
        assert_ne!(first, changed);
    }

    #[test]
    fn test_file_hasher_persistent_cache_skips_small_files() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("index.db");
        let file = dir.path().join("small.rs");
        std::fs::write(&file, b"fn main() {}").unwrap();

        let hasher = FileHasher::persistent(&db_path);
        let first = hasher.hash(&file).unwrap();
        let second = hasher.hash(&file).unwrap();
        let stats = hasher.stats();
        assert_eq!(first, second);
        assert_eq!(stats.cache_hits, 0);
        assert_eq!(stats.cache_misses, 2);
    }

    // --- dep-info pre-pass integration test ---

    #[test]
    fn test_dep_info_finds_modules() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();

        std::fs::write(src.join("lib.rs"), b"mod server;\npub fn hello() {}").unwrap();
        std::fs::write(src.join("server.rs"), b"pub fn serve() {}").unwrap();

        let rustc = std::path::PathBuf::from("rustc");
        let source = src.join("lib.rs");
        let args = vec![
            "--crate-name".to_string(),
            "testcrate".to_string(),
            "--crate-type".to_string(),
            "lib".to_string(),
            "--edition".to_string(),
            "2021".to_string(),
        ];

        let runs_before = crate::opcounts::dep_info_runs();
        let ms_before = crate::opcounts::dep_info_ms();
        let dep_info = run_dep_info_pass(&rustc, None, &source, &args, false).unwrap();
        // The pre-pass is a real rustc start; the event log must see it.
        assert!(crate::opcounts::dep_info_runs() > runs_before);
        assert!(
            crate::opcounts::dep_info_ms() > ms_before,
            "a rustc spawn takes more than a millisecond"
        );

        assert!(
            dep_info.source_files.len() >= 2,
            "expected at least 2 files, got {:?}",
            dep_info.source_files
        );
        assert!(dep_info.source_files.iter().any(|p| p.ends_with("lib.rs")));
        assert!(
            dep_info
                .source_files
                .iter()
                .any(|p| p.ends_with("server.rs"))
        );
    }

    #[test]
    fn run_dep_info_pass_errors_on_compile_failure() {
        // A failing dep-info pre-pass must return Err, NOT a crate-root-only
        // DepInfo: keying off an incomplete input set risks a stale-artifact
        // false hit (kunobi-ninja/kache#323). The wrapper turns this Err into a
        // passthrough (real compile, no store).
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        // Syntactically invalid Rust → rustc exits non-zero on the dep-info pass.
        std::fs::write(src.join("lib.rs"), b"fn broken( { this is not valid rust").unwrap();

        let rustc = std::path::PathBuf::from("rustc");
        let source = src.join("lib.rs");
        let args = vec![
            "--crate-name".to_string(),
            "testcrate".to_string(),
            "--crate-type".to_string(),
            "lib".to_string(),
            "--edition".to_string(),
            "2021".to_string(),
        ];

        let runs_before = crate::opcounts::dep_info_runs();
        let Err(err) = run_dep_info_pass(&rustc, None, &source, &args, false) else {
            panic!("expected Err on a failing dep-info pass (source has a syntax error)");
        };
        assert!(
            crate::opcounts::dep_info_runs() > runs_before,
            "a failed pre-pass still spawned rustc and must be counted"
        );
        // The error must CARRY rustc's own reason, not just say the pass
        // failed: the wrapper logs `{e:#}`, and a diagnostic that drops the
        // cause is how the substrate bench's 60 refusals stayed unexplained
        // for two months (kunobi-ninja/kache#431).
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("dep-info pre-pass failed (exit"),
            "the cause must name the failing exit status: {rendered}"
        );
        assert!(
            rendered.contains("error"),
            "the cause must carry rustc's own first stderr line: {rendered}"
        );
    }

    #[test]
    fn dep_info_pass_args_drops_output_naming_flags() {
        // Cargo's real lib-target argv, plus the `-C extra-filename` the
        // pre-pass used to carry into its own `-o` invocation
        // (kunobi-ninja/kache#896).
        let args: Vec<String> = [
            "--crate-name",
            "mylib",
            "--edition=2021",
            "mylib/src/lib.rs",
            "--error-format=json",
            "--crate-type",
            "lib",
            "--emit=dep-info,metadata,link",
            "-C",
            "metadata=1b2c9f1c31209a4a",
            "-C",
            "extra-filename=-02749d16b52ff8b3",
            "--out-dir",
            "/w/target/debug/deps",
            "-C",
            "incremental=/w/target/debug/incremental",
            "-L",
            "dependency=/w/target/debug/deps",
        ]
        .iter()
        .map(|arg| (*arg).to_string())
        .collect();

        let dep_args = dep_info_pass_args(
            Path::new("mylib/src/lib.rs"),
            &args,
            Path::new("/tmp/kache-depinfo/deps.d"),
        );

        assert_eq!(
            dep_args.first().map(String::as_str),
            Some("mylib/src/lib.rs"),
            "the source file leads the argv exactly once: {dep_args:?}"
        );
        assert_eq!(
            dep_args.iter().filter(|a| a.contains("lib.rs")).count(),
            1,
            "cargo's own positional source must not be re-added: {dep_args:?}"
        );
        assert!(
            !dep_args.iter().any(|a| a.contains("extra-filename")),
            "-C extra-filename names outputs the pre-pass discards, and rustc \
             warns about it as soon as -o is present: {dep_args:?}"
        );
        assert!(
            !dep_args.iter().any(|a| a.contains("incremental")),
            "incremental flags go through the canonical filter: {dep_args:?}"
        );
        assert!(
            !dep_args.iter().any(|a| a.starts_with("--emit=")),
            "cargo's --emit is superseded by the pre-pass's own: {dep_args:?}"
        );
        assert!(
            !dep_args.iter().any(|a| a == "--out-dir"),
            "--out-dir is the other flag rustc reports as ignored due to -o: {dep_args:?}"
        );
        assert!(
            !dep_args.iter().any(|a| a.starts_with("/w/target")),
            "--out-dir's value must go with it: {dep_args:?}"
        );

        // Everything that shapes the source closure survives.
        for kept in [
            "--crate-name",
            "mylib",
            "--edition=2021",
            "--error-format=json",
            "--crate-type",
            "lib",
            "-C",
            "metadata=1b2c9f1c31209a4a",
            "-L",
            "dependency=/w/target/debug/deps",
        ] {
            assert!(
                dep_args.iter().any(|a| a == kept),
                "{kept} shapes the input set and must survive: {dep_args:?}"
            );
        }
        assert_eq!(
            dep_args.iter().filter(|a| a.as_str() == "-C").count(),
            1,
            "only extra-filename's own -C is dropped, not every -C: {dep_args:?}"
        );

        let tail = &dep_args[dep_args.len() - 4..];
        assert_eq!(
            tail,
            [
                "--emit",
                "dep-info",
                "-o",
                "/tmp/kache-depinfo/deps.d".to_string().as_str()
            ]
            .map(String::from),
            "the pre-pass appends exactly one output configuration"
        );
    }

    #[test]
    fn dep_info_pass_args_drops_every_extra_filename_spelling() {
        // rustc accepts four spellings of a codegen option; RUSTFLAGS and
        // hand-rolled invocations use the joined ones cargo never emits.
        for spelling in [
            vec!["-C", "extra-filename=-abc123"],
            vec!["-Cextra-filename=-abc123"],
            vec!["--codegen", "extra-filename=-abc123"],
            vec!["--codegen=extra-filename=-abc123"],
            // rustc normalises `_` to `-` in option names, so the underscore
            // spelling reaches the same flag.
            vec!["-C", "extra_filename=-abc123"],
            vec!["-Cextra_filename=-abc123"],
            vec!["--codegen", "extra_filename=-abc123"],
            vec!["--codegen=extra_filename=-abc123"],
        ] {
            let mut args: Vec<String> = vec!["--crate-type".into(), "lib".into()];
            args.extend(spelling.iter().map(|arg| (*arg).to_string()));
            args.push("--crate-name".into());
            args.push("mylib".into());

            let dep_args =
                dep_info_pass_args(Path::new("src/lib.rs"), &args, Path::new("/tmp/deps.d"));

            assert!(
                !dep_args
                    .iter()
                    .any(|a| a.contains("extra-filename") || a.contains("extra_filename")),
                "{spelling:?} must be dropped: {dep_args:?}"
            );
            assert!(
                dep_args.iter().any(|a| a == "--crate-name"),
                "{spelling:?} must not swallow the following flag: {dep_args:?}"
            );
        }
    }

    #[test]
    fn dep_info_pass_args_keeps_bare_trailing_codegen_flag() {
        // A trailing `-C` with no value is malformed, but the pre-pass must
        // hand it to rustc unchanged rather than guess — rustc's own error is
        // the honest outcome.
        let args = vec![
            "--crate-type".to_string(),
            "lib".to_string(),
            "-C".to_string(),
        ];

        let dep_args = dep_info_pass_args(Path::new("src/lib.rs"), &args, Path::new("/tmp/deps.d"));

        assert!(
            dep_args.iter().any(|a| a == "-C"),
            "a valueless -C is not an extra-filename: {dep_args:?}"
        );
    }

    #[test]
    fn dep_info_pass_args_drops_joined_output_flag() {
        // rustc accepts `-o` with its value attached, and reads single-dash
        // `-out-dir` as `-o` plus junk ("option `-o` has no space between
        // flag name and value"). A leftover joins the pre-pass's own `-o`
        // and rustc exits 1 with "Option 'o' given more than once" — every
        // build using that spelling stays a passthrough
        // (kunobi-ninja/kache#896).
        let args: Vec<String> = [
            "--crate-name",
            "mylib",
            "-o/tmp/original.rlib",
            "--edition=2021",
            "-O",
            "--out-dir=/tmp/original-deps",
            "-out-dir",
            "/tmp/still-positional",
            "src/lib.rs",
        ]
        .iter()
        .map(|arg| (*arg).to_string())
        .collect();

        let dep_args = dep_info_pass_args(Path::new("src/lib.rs"), &args, Path::new("/tmp/deps.d"));

        let outputs: Vec<&String> = dep_args.iter().filter(|a| a.starts_with("-o")).collect();
        assert_eq!(
            outputs,
            ["-o"],
            "only the pre-pass's own -o may survive: {dep_args:?}"
        );
        for dropped in [
            "-o/tmp/original.rlib",
            "-out-dir",
            "--out-dir=/tmp/original-deps",
        ] {
            assert!(
                !dep_args.iter().any(|a| a == dropped),
                "{dropped} names an output the pre-pass discards: {dep_args:?}"
            );
        }
        assert!(
            dep_args.iter().any(|a| a == "/tmp/still-positional"),
            "single-dash -out-dir takes no separate value: rustc reads the next \
             token as a positional, and the pre-pass must fail on it exactly as \
             the real build does: {dep_args:?}"
        );
        assert!(
            dep_args.iter().any(|a| a == "-O"),
            "capital -O is opt-level, not output: {dep_args:?}"
        );
        assert!(
            dep_args.iter().any(|a| a == "--edition=2021"),
            "the token after a joined -o is a real flag, not its value: {dep_args:?}"
        );
        let tail = &dep_args[dep_args.len() - 4..];
        assert_eq!(
            tail,
            [
                "--emit",
                "dep-info",
                "-o",
                "/tmp/deps.d".to_string().as_str()
            ]
            .map(String::from),
            "the pre-pass appends exactly one output configuration"
        );
    }

    #[test]
    fn dep_info_pass_args_strips_every_incremental_spelling() {
        // All four `-C incremental` spellings rustc accepts must go through
        // the canonical filter before the pre-pass runs: a leftover would aim
        // the dep-info run at cargo's incremental dir
        // (kunobi-ninja/kache#896). A bare `-C incremental` (no `=value`) is
        // not valid rustc — it stays, so rustc rejects it exactly as it
        // rejects the real build ("requires a string").
        let args: Vec<String> = [
            "--crate-name",
            "mylib",
            "-Cincremental=/tmp/incr-joined",
            "-C",
            "incremental=/tmp/incr-split",
            "--codegen=incremental=/tmp/incr-long-joined",
            "--codegen",
            "incremental=/tmp/incr-long-split",
            "src/lib.rs",
        ]
        .iter()
        .map(|arg| (*arg).to_string())
        .collect();

        let dep_args = dep_info_pass_args(Path::new("src/lib.rs"), &args, Path::new("/tmp/deps.d"));

        assert!(
            !dep_args.iter().any(|a| a.contains("incremental")),
            "no incremental spelling may reach the pre-pass: {dep_args:?}"
        );
        assert!(
            dep_args.iter().any(|a| a == "mylib"),
            "stripping must not swallow neighbouring flags: {dep_args:?}"
        );

        let bare: Vec<String> = ["--crate-name", "mylib", "-C", "incremental", "src/lib.rs"]
            .iter()
            .map(|arg| (*arg).to_string())
            .collect();
        let dep_bare = dep_info_pass_args(Path::new("src/lib.rs"), &bare, Path::new("/tmp/deps.d"));

        assert!(
            dep_bare.iter().any(|a| a == "incremental"),
            "a valueless incremental is rustc's to reject, not the pre-pass's to guess: {dep_bare:?}"
        );
    }

    #[test]
    fn dep_info_pass_prepass_succeeds_through_response_file() {
        // The `use_response_file` path — taken whenever cargo's own argv
        // arrived via `@file` — had no coverage. A crate under a path with
        // spaces exercises the verbatim one-arg-per-line round-trip, which is
        // exactly what an expanded largest-crate argv looks like
        // (kunobi-ninja/kache#896).
        let dir = tempfile::Builder::new()
            .prefix("kache depinfo space ")
            .tempdir()
            .unwrap();
        let src = dir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("lib.rs"), b"mod server;\npub fn hello() {}").unwrap();
        std::fs::write(src.join("server.rs"), b"pub fn serve() {}").unwrap();

        let rustc = std::path::PathBuf::from("rustc");
        let source = src.join("lib.rs");
        let args = vec![
            "--crate-name".to_string(),
            "testcrate".to_string(),
            "--crate-type".to_string(),
            "lib".to_string(),
            "--edition".to_string(),
            "2021".to_string(),
        ];

        let dep_info = run_dep_info_pass(&rustc, None, &source, &args, true)
            .expect("the response-file pre-pass must match the direct one");

        assert!(
            dep_info.source_files.iter().any(|p| p.ends_with("lib.rs")),
            "expected the crate root: {:?}",
            dep_info.source_files
        );
        assert!(
            dep_info
                .source_files
                .iter()
                .any(|p| p.ends_with("server.rs")),
            "an incomplete source list is what makes a crate uncacheable: {:?}",
            dep_info.source_files
        );
    }

    #[test]
    fn read_dep_info_file_rejects_non_utf8() {
        // A non-UTF8 filename or env value in the source closure lands
        // verbatim in rustc's dep-info output. The pre-pass must refuse it
        // with the encoding named — not a bare "stream did not contain valid
        // UTF-8" against an unnamed input set (kunobi-ninja/kache#896).
        let dir = tempfile::tempdir().unwrap();
        let dep_file = dir.path().join("deps.d");
        std::fs::write(&dep_file, b"/tmp/x.d: src/lib.rs\n").unwrap();
        assert_eq!(
            read_dep_info_file(&dep_file).unwrap(),
            "/tmp/x.d: src/lib.rs\n"
        );

        std::fs::write(&dep_file, b"/tmp/x.d: src/\xfflib.rs\n").unwrap();
        let err = format!("{:#}", read_dep_info_file(&dep_file).unwrap_err());
        assert!(
            err.contains("not valid UTF-8"),
            "the refusal must name the encoding: {err}"
        );
    }

    #[test]
    fn dep_info_pass_prepass_succeeds_with_extra_filename() {
        // End-to-end regression for kunobi-ninja/kache#896: a lib-target argv
        // shaped like cargo's, carrying `-C extra-filename`, must produce a
        // clean pre-pass and the crate's full source closure — not a refusal.
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("lib.rs"), b"mod server;\npub fn hello() {}").unwrap();
        std::fs::write(src.join("server.rs"), b"pub fn serve() {}").unwrap();

        let rustc = std::path::PathBuf::from("rustc");
        let source = src.join("lib.rs");
        let out_dir = dir.path().join("deps");
        let args: Vec<String> = vec![
            "--crate-name".into(),
            "testcrate".into(),
            "--edition=2021".into(),
            source.to_string_lossy().into_owned(),
            "--error-format=json".into(),
            "--crate-type".into(),
            "lib".into(),
            "--emit=dep-info,metadata,link".into(),
            "-C".into(),
            "metadata=92ee3169a2c3b7b0".into(),
            "-C".into(),
            "extra-filename=-2cb6bc2ef2b88725".into(),
            "--out-dir".into(),
            out_dir.to_string_lossy().into_owned(),
            "-C".into(),
            "incremental=/nonexistent/incremental".into(),
        ];

        let dep_info = run_dep_info_pass(&rustc, None, &source, &args, false)
            .expect("a cargo-shaped lib argv must not fail the pre-pass");

        assert!(
            dep_info.source_files.iter().any(|p| p.ends_with("lib.rs")),
            "expected the crate root: {:?}",
            dep_info.source_files
        );
        assert!(
            dep_info
                .source_files
                .iter()
                .any(|p| p.ends_with("server.rs")),
            "an incomplete source list is what makes a crate uncacheable: {:?}",
            dep_info.source_files
        );
    }

    #[test]
    fn first_rustc_error_line_skips_leading_json_warnings() {
        // The exact shape of the kunobi-ninja/kache#896 report: rustc emits
        // session-level warnings before any crate diagnostic, so the first
        // line is never the reason the run aborted.
        let stderr = concat!(
            r#"{"$message_type":"diagnostic","message":"ignoring -C extra-filename flag due to -o flag","level":"warning"}"#,
            "\n",
            r#"{"$message_type":"diagnostic","message":"cannot find macro `frobnicate`","level":"error"}"#,
            "\n",
            r#"{"$message_type":"diagnostic","message":"aborting due to 1 previous error","level":"error"}"#,
            "\n",
        );

        let line = first_rustc_error_line(stderr).expect("an error line is present");

        assert!(
            line.contains("cannot find macro"),
            "the first error-level diagnostic wins: {line}"
        );
    }

    #[test]
    fn first_rustc_error_line_skips_leading_human_warnings() {
        let stderr = "warning: ignoring -C extra-filename flag due to -o flag\n\
                      \n\
                      error[E0433]: failed to resolve: use of undeclared crate `nope`\n\
                      error: aborting due to 1 previous error\n";

        let line = first_rustc_error_line(stderr).expect("an error line is present");

        assert_eq!(
            line,
            "error[E0433]: failed to resolve: use of undeclared crate `nope`"
        );
    }

    #[test]
    fn first_rustc_error_line_matches_unnumbered_human_errors() {
        let stderr = "warning: unused import: `std::io`\nerror: expected one of `!` or `::`\n";

        let line = first_rustc_error_line(stderr).expect("an error line is present");

        assert_eq!(line, "error: expected one of `!` or `::`");
    }

    #[test]
    fn first_rustc_error_line_falls_back_to_the_first_content_line() {
        // rustc killed by a signal, or a wrapper that failed before rustc ran,
        // leaves no error-level diagnostic. Report something rather than
        // "(no output)" — a refusal with no stated cause is what
        // kunobi-ninja/kache#431 was about.
        let line = first_rustc_error_line("\n\nwarning: something odd\nnote: more\n");

        assert_eq!(line, Some("warning: something odd"));
        assert_eq!(
            first_rustc_error_line("   \n \n"),
            None,
            "blank is no cause"
        );
        assert_eq!(first_rustc_error_line(""), None);
    }

    // --- cache key module-change detection test ---

    #[test]
    fn test_cache_key_changes_with_module_file() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();

        std::fs::write(src.join("lib.rs"), b"mod utils;\npub fn hello() {}").unwrap();
        std::fs::write(src.join("utils.rs"), b"pub fn helper() {}").unwrap();

        let args_vec: Vec<String> = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "mylib".to_string(),
            src.join("lib.rs").to_string_lossy().to_string(),
            "--crate-type".to_string(),
            "lib".to_string(),
            "--edition=2021".to_string(),
        ];

        let fh = FileHasher::new();
        let pn = PathNormalizer::empty();

        let parsed1 = RustcArgs::parse(&args_vec).unwrap();
        let key1 = compute_cache_key(&parsed1, &fh, &pn).unwrap();

        // Modify the module file (NOT lib.rs)
        std::fs::write(
            src.join("utils.rs"),
            b"pub fn helper() { println!(\"changed\"); }",
        )
        .unwrap();

        let parsed2 = RustcArgs::parse(&args_vec).unwrap();
        let key2 = compute_cache_key(&parsed2, &fh, &pn).unwrap();

        assert_ne!(
            key1, key2,
            "cache key must change when a module file changes"
        );
    }

    // --- cache key determinism with multiple source files ---

    #[test]
    fn test_cache_key_stable_with_module_files() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();

        std::fs::write(src.join("lib.rs"), b"mod a;\nmod b;\npub fn lib_fn() {}").unwrap();
        std::fs::write(src.join("a.rs"), b"pub fn a_fn() {}").unwrap();
        std::fs::write(src.join("b.rs"), b"pub fn b_fn() {}").unwrap();

        let args_vec: Vec<String> = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "testcrate".to_string(),
            src.join("lib.rs").to_string_lossy().to_string(),
            "--crate-type".to_string(),
            "lib".to_string(),
            "--edition=2021".to_string(),
        ];

        let fh = FileHasher::new();
        let pn = PathNormalizer::empty();

        let parsed1 = RustcArgs::parse(&args_vec).unwrap();
        let parsed2 = RustcArgs::parse(&args_vec).unwrap();

        let key1 = compute_cache_key(&parsed1, &fh, &pn).unwrap();
        let key2 = compute_cache_key(&parsed2, &fh, &pn).unwrap();

        assert_eq!(
            key1, key2,
            "cache key must be deterministic with multiple source files"
        );
    }

    // ────────────────────────────────────────────────────────────────
    // rustc cache-key correctness matrix
    //
    // Property under test: the key must react to every input that
    // changes rustc's *output artifact*, and must NOT react to inputs
    // that only affect diagnostics. A missed input is a miscache (a
    // false hit serving the wrong artifact); a spurious change is
    // over-keying (a missed hit, wasted work).
    //
    // The testable seam is `compute_cache_key` itself: it forks rustc
    // (`get_rustc_version`, the `--emit=dep-info` pre-pass) and reads
    // source files, so each case builds a real temp `.rs` file plus a
    // `RustcArgs` and compares two keys. Cases that depend on env vars
    // (`RUSTFLAGS`) mutate the process environment and are isolated
    // onto a serial mutex so parallel test threads can't interleave.
    // ────────────────────────────────────────────────────────────────

    /// Serializes every test that computes a cache key through the shared
    /// process-state lock also used by argument-parser tests.
    ///
    /// `compute_cache_key` reads process-wide env (`RUSTFLAGS`,
    /// `CARGO_ENCODED_RUSTFLAGS`, `CARGO_CFG_*`); the env-mutating
    /// matrix case below temporarily changes `RUSTFLAGS`. `cargo test`
    /// runs tests as parallel threads of one process, so without a
    /// shared lock any test's two key computations can straddle that
    /// mutation and observe a spurious key difference — that is exactly
    /// how the `assert_eq` tests `test_cache_key_deterministic` and
    /// `test_coverage_keys_consistent_across_remap_forms` flaked on CI.
    ///
    /// The lock is therefore NOT matrix-scoped: every test that calls
    /// `compute_cache_key` — matrix or not — holds it for its full
    /// duration. New key tests must do the same; that is the price of
    /// `compute_cache_key` reading process-global env directly.
    /// Keep the local name used throughout this large test module while the
    /// underlying lock remains shared with other process-state observers.
    fn key_test_lock() -> crate::test_support::ProcessStateTestGuard {
        process_state_test_lock()
    }

    /// Key computation stashes this compile's unit identity and its per-extern
    /// producer ids, and each is TAKEN — a second read yields `None`, so a
    /// compile that computes no rustc key cannot inherit the previous one's
    /// identities from the same process (kunobi-ninja/kache#627).
    #[test]
    fn key_computation_stashes_unit_identity_and_yields_it_once() {
        let _lock = key_test_lock();
        let args: Vec<String> = [
            "rustc",
            "--crate-name",
            "app",
            "src/lib.rs",
            "-C",
            "extra-filename=-843f02d6a46ebef1",
            "--extern",
            "foo_old=/w/target/debug/deps/libfoo-0532daf0ee3516f0.rlib",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let mut parsed = RustcArgs::parse(&args).unwrap();
        // Skip the dep-info pre-pass: this is about what the externs loop
        // records, not source discovery.
        parsed.source_file = None;

        compute_cache_key(&parsed, &FileHasher::new(), &PathNormalizer::empty()).unwrap();

        assert_eq!(
            take_last_key_unit_id().as_deref(),
            Some("843f02d6a46ebef1"),
            "the compile's own `-C extra-filename`"
        );
        assert_eq!(take_last_key_unit_id(), None, "taken, not copied");

        let units = take_last_key_extern_units().expect("recorded with the digests");
        assert_eq!(
            units.get("foo_old").map(String::as_str),
            // Keyed by the alias the consumer used; the value is the producer's
            // identity, recovered from the artifact filename even though that
            // file does not exist here.
            Some("0532daf0ee3516f0")
        );
        assert_eq!(take_last_key_extern_units(), None, "taken, not copied");
    }

    /// The key stashes are per-thread, so a concurrent key computation can
    /// neither overwrite nor consume another's (kunobi-ninja/kache#777).
    ///
    /// Before the stashes were thread-local this was the shape that made
    /// `key_computation_stashes_unit_identity_and_yields_it_once` flaky under
    /// the default `cargo test` parallelism: any other thread computing a key
    /// between a compute and its take would win the race. The outer
    /// `key_test_lock` still serializes this group against the env-mutating
    /// matrix; what runs concurrently here is the stash access itself.
    #[test]
    fn key_stashes_do_not_leak_across_threads() {
        let _lock = key_test_lock();

        // Distinct unit ids per thread, so a leak shows up as another thread's
        // value rather than as an absence.
        let units = ["aaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb", "cccccccccccccccc"];
        std::thread::scope(|scope| {
            for unit in units {
                scope.spawn(move || {
                    let extra = format!("extra-filename=-{unit}");
                    let extern_arg = format!("dep_{unit}=/w/target/debug/deps/libdep-{unit}.rlib");
                    let args: Vec<String> = [
                        "rustc",
                        "--crate-name",
                        "app",
                        "src/lib.rs",
                        "-C",
                        &extra,
                        "--extern",
                        &extern_arg,
                    ]
                    .iter()
                    .map(|s| s.to_string())
                    .collect();
                    let mut parsed = RustcArgs::parse(&args).unwrap();
                    // Skip the dep-info pre-pass, as above: forking rustc from
                    // every thread would swamp the point being made.
                    parsed.source_file = None;

                    // Loop so the interleaving windows overlap rather than each
                    // thread racing through its compute/take once.
                    for _ in 0..16 {
                        compute_cache_key(&parsed, &FileHasher::new(), &PathNormalizer::empty())
                            .unwrap();

                        assert_eq!(
                            take_last_key_unit_id().as_deref(),
                            Some(unit),
                            "each thread sees its own unit id"
                        );
                        assert_eq!(take_last_key_unit_id(), None, "taken, not copied");

                        let recorded =
                            take_last_key_extern_units().expect("recorded with the digests");
                        assert_eq!(
                            recorded.get(&format!("dep_{unit}")).map(String::as_str),
                            Some(unit),
                            "each thread sees its own extern units"
                        );
                        assert_eq!(take_last_key_extern_units(), None, "taken, not copied");

                        assert!(
                            take_last_key_externs().is_some(),
                            "digests ride the same thread as their identities"
                        );
                        assert!(take_last_key_fields().is_some(), "per-group digests too");
                    }
                });
            }
        });
    }

    /// True if a bare `rustc` is invocable. `compute_cache_key` forks
    /// rustc for the version probe and dep-info pre-pass; without it
    /// the key still computes (the pre-pass falls back) but the
    /// matrix's intent is to exercise the real path. Guard-skip when
    /// absent, consistent with other compiler-forking tests.
    fn rustc_available() -> bool {
        std::process::Command::new("rustc")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    #[cfg(unix)]
    fn rustc_exe_name() -> &'static str {
        "rustc"
    }

    #[cfg(unix)]
    fn rustc_path_on_path() -> Option<PathBuf> {
        let path_var = std::env::var_os("PATH")?;
        std::env::split_paths(&path_var)
            .map(|dir| dir.join(rustc_exe_name()))
            .find(|path| path.is_file())
    }

    #[cfg(unix)]
    fn shell_single_quote(path: &Path) -> String {
        format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"))
    }

    #[cfg(unix)]
    fn write_rustc_version_wrapper(root: &Path, subdir: &str, version: &str) -> PathBuf {
        let real_rustc = rustc_path_on_path().expect("rustc should be on PATH");
        let dir = root.join(subdir);
        std::fs::create_dir_all(&dir).unwrap();
        let wrapper = dir.join("rustc");
        let script = format!(
            "#!/bin/sh\n\
if [ \"$1\" = \"--version\" ] && [ \"$2\" = \"--verbose\" ]; then\n\
cat <<'KACHE_RUSTC_VERSION'\n\
{version}\n\
KACHE_RUSTC_VERSION\n\
exit 0\n\
fi\n\
exec {} \"$@\"\n",
            shell_single_quote(&real_rustc)
        );
        kache_fs::testutil::write_executable(&wrapper, script);
        wrapper
    }

    /// Build a minimal lib-crate arg vector around a temp source file.
    /// Callers push the dimension-under-test onto the returned vec.
    fn base_args(source: &Path) -> Vec<String> {
        vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "mxcrate".to_string(),
            source.to_string_lossy().to_string(),
            "--crate-type".to_string(),
            "lib".to_string(),
            "--edition=2021".to_string(),
        ]
    }

    /// Compute a key for an arg vector. The source file path is
    /// already embedded in `args` (positional) — `RustcArgs::parse`
    /// picks it up — so no separate source argument is needed.
    fn key_for(args: &[String]) -> String {
        let parsed = RustcArgs::parse(args).unwrap();
        let fh = FileHasher::new();
        let pn = PathNormalizer::empty();
        compute_cache_key(&parsed, &fh, &pn).unwrap()
    }

    fn restore_env_var(key: &str, old: Option<std::ffi::OsString>) {
        match old {
            Some(value) => unsafe { std::env::set_var(key, value) },
            None => unsafe { std::env::remove_var(key) },
        }
    }

    /// `KACHE_RUSTC_PATH_NORMALIZE=0` bakes real machine-local paths into DWARF
    /// (no `--remap-path-prefix`), so the opt-out key MUST be path-local:
    /// otherwise two checkouts at different paths compute the same key and a
    /// shared cache serves one checkout's real-path artifact to another
    /// (kunobi-ninja/kache#480). This pins the exact regression — two builds
    /// that differ ONLY in cwd (the DWARF `comp_dir`) must get different opt-out
    /// keys, while the default (remapped) build stays cwd-portable.
    #[test]
    fn opt_out_key_is_path_local_but_default_stays_portable() {
        let _lock = key_test_lock();
        if !rustc_available() {
            return;
        }
        let old_var = std::env::var_os("KACHE_RUSTC_PATH_NORMALIZE");
        let old_cwd = std::env::current_dir().unwrap();

        // Two identical crates at DIFFERENT paths; source arg is relative
        // ("lib.rs") so cwd is the only thing that varies — exactly what cargo
        // passes and what makes `comp_dir` the discriminator.
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        std::fs::write(dir_a.path().join("lib.rs"), "pub fn f() {}\n").unwrap();
        std::fs::write(dir_b.path().join("lib.rs"), "pub fn f() {}\n").unwrap();
        let args = base_args(Path::new("lib.rs"));

        // SAFETY: env access is serialized by the process-state test lock; restored below.
        unsafe { std::env::set_var("KACHE_RUSTC_PATH_NORMALIZE", "0") };
        std::env::set_current_dir(dir_a.path()).unwrap();
        let optout_a = key_for(&args);
        std::env::set_current_dir(dir_b.path()).unwrap();
        let optout_b = key_for(&args);

        restore_env_var("KACHE_RUSTC_PATH_NORMALIZE", None);
        std::env::set_current_dir(dir_a.path()).unwrap();
        let default_a = key_for(&args);
        std::env::set_current_dir(dir_b.path()).unwrap();
        let default_b = key_for(&args);

        std::env::set_current_dir(&old_cwd).unwrap();
        restore_env_var("KACHE_RUSTC_PATH_NORMALIZE", old_var);

        assert_ne!(
            optout_a, optout_b,
            "opt-out builds bake real paths, so keys must be cwd-local"
        );
        assert_eq!(
            default_a, default_b,
            "default (remapped) builds must stay cwd-portable"
        );
        assert_ne!(
            optout_a, default_a,
            "opt-out must be a separate namespace from remapped builds"
        );
    }

    /// The opt-out fold must cover EVERY prefix the normalizer would have
    /// remapped — not just cwd/$HOME. A build-script `OUT_DIR` lives under
    /// `$CARGO_TARGET_DIR`, which the default key normalizes to `<TARGET>`; if
    /// the opt-out fold missed that prefix, two builds under different target
    /// dirs (each baking a different real `OUT_DIR` path into DWARF) would
    /// collide on one opt-out key. Folding the normalizer's own `raw_prefixes`
    /// closes that gap by construction. Pins it: changing only
    /// `$CARGO_TARGET_DIR` changes the opt-out key but not the (portable)
    /// default key.
    #[test]
    fn opt_out_key_folds_all_normalizer_prefixes_not_just_home() {
        let _lock = key_test_lock();
        if !rustc_available() {
            return;
        }
        let old_var = std::env::var_os("KACHE_RUSTC_PATH_NORMALIZE");
        let old_target = std::env::var_os("CARGO_TARGET_DIR");

        let ws = tempfile::tempdir().unwrap();
        let src = ws.path().join("lib.rs");
        std::fs::write(&src, "pub fn f() {}\n").unwrap();
        let target_a = tempfile::tempdir().unwrap();
        let target_b = tempfile::tempdir().unwrap();
        // Absolute source (same for both variants) so the pre-pass finds it
        // regardless of cwd; $CARGO_TARGET_DIR is then the only thing that varies.
        let args = base_args(&src);

        // Build the normalizer AFTER setting CARGO_TARGET_DIR so its `<TARGET>`
        // rule reflects the current target dir (from_env reads the env).
        let key = || {
            let parsed = RustcArgs::parse(&args).unwrap();
            let fh = FileHasher::new();
            let pn = PathNormalizer::from_env(Some(ws.path()));
            compute_cache_key(&parsed, &fh, &pn).unwrap()
        };

        // SAFETY: env access is serialized by the process-state test lock; restored below.
        unsafe { std::env::set_var("KACHE_RUSTC_PATH_NORMALIZE", "0") };
        unsafe { std::env::set_var("CARGO_TARGET_DIR", target_a.path()) };
        let optout_a = key();
        unsafe { std::env::set_var("CARGO_TARGET_DIR", target_b.path()) };
        let optout_b = key();

        restore_env_var("KACHE_RUSTC_PATH_NORMALIZE", None);
        unsafe { std::env::set_var("CARGO_TARGET_DIR", target_a.path()) };
        let default_a = key();
        unsafe { std::env::set_var("CARGO_TARGET_DIR", target_b.path()) };
        let default_b = key();

        restore_env_var("KACHE_RUSTC_PATH_NORMALIZE", old_var);
        restore_env_var("CARGO_TARGET_DIR", old_target);

        assert_ne!(
            optout_a, optout_b,
            "opt-out key must fold the raw $CARGO_TARGET_DIR prefix (OUT_DIR lives under it)"
        );
        assert_eq!(
            default_a, default_b,
            "default build normalizes $CARGO_TARGET_DIR to <TARGET>, so it stays portable"
        );
    }

    /// Coverage builds skip remap injection too (llvm-cov / tarpaulin need real
    /// paths in the profraw), so they bake real machine-local paths into DWARF
    /// exactly like the `KACHE_RUSTC_PATH_NORMALIZE=0` opt-out. Their `remap:none`
    /// key must therefore be path-local as well, or a shared cache serves one
    /// checkout's real-path coverage artifact to another (the coverage analog of
    /// kunobi-ninja/kache#480). Pins it: two coverage builds differing only in
    /// cwd must get different keys.
    #[test]
    fn coverage_key_is_path_local() {
        let _lock = key_test_lock();
        if !rustc_available() {
            return;
        }
        let old_cwd = std::env::current_dir().unwrap();
        // Force the opt-out OFF so this test exercises the COVERAGE `remap:none`
        // path specifically. If `KACHE_RUSTC_PATH_NORMALIZE=0` were set in the
        // ambient env, `path_normalize_disabled` would be true and the fold
        // would fire via the opt-out — passing even if coverage regressed to the
        // old opt-out-only condition. Removing it pins the coverage path.
        let old_var = std::env::var_os("KACHE_RUSTC_PATH_NORMALIZE");
        restore_env_var("KACHE_RUSTC_PATH_NORMALIZE", None);

        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        std::fs::write(dir_a.path().join("lib.rs"), "pub fn f() {}\n").unwrap();
        std::fs::write(dir_b.path().join("lib.rs"), "pub fn f() {}\n").unwrap();
        let mut args = base_args(Path::new("lib.rs"));
        args.push("-Cinstrument-coverage".to_string());

        // Sanity-check the fixture is the coverage path, not the opt-out path.
        let parsed = RustcArgs::parse(&args).unwrap();
        assert!(parsed.has_coverage_instrumentation());
        assert!(
            !parsed.path_normalize_disabled,
            "test must exercise the coverage remap:none path, not the opt-out path"
        );

        std::env::set_current_dir(dir_a.path()).unwrap();
        let cov_a = key_for(&args);
        std::env::set_current_dir(dir_b.path()).unwrap();
        let cov_b = key_for(&args);

        std::env::set_current_dir(&old_cwd).unwrap();
        restore_env_var("KACHE_RUSTC_PATH_NORMALIZE", old_var);

        assert_ne!(
            cov_a, cov_b,
            "coverage builds bake real paths, so their keys must be cwd-local"
        );
    }

    #[test]
    fn key_rustc_bootstrap_presence_changes_key_but_empty_is_identity() {
        let _lock = key_test_lock();
        if !rustc_available() {
            return;
        }
        let old = std::env::var_os("RUSTC_BOOTSTRAP");

        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("lib.rs");
        std::fs::write(&src, "pub fn f() {}\n").unwrap();
        let args = base_args(&src);

        unsafe {
            std::env::remove_var("RUSTC_BOOTSTRAP");
        }
        let key_unset = key_for(&args);

        // Empty must hash identically to unset: that is what keeps existing
        // caches valid (no CACHE_KEY_VERSION bump for the common case).
        unsafe {
            std::env::set_var("RUSTC_BOOTSTRAP", "");
        }
        let key_empty = key_for(&args);

        unsafe {
            std::env::set_var("RUSTC_BOOTSTRAP", "1");
        }
        let key_set = key_for(&args);

        unsafe {
            std::env::set_var("RUSTC_BOOTSTRAP", "some_crate");
        }
        let key_other = key_for(&args);

        restore_env_var("RUSTC_BOOTSTRAP", old);

        assert_eq!(
            key_unset, key_empty,
            "empty RUSTC_BOOTSTRAP must equal unset"
        );
        assert_ne!(key_unset, key_set, "RUSTC_BOOTSTRAP=1 must change the key");
        assert_ne!(
            key_set, key_other,
            "different RUSTC_BOOTSTRAP values must differ"
        );
    }

    #[test]
    fn key_cargo_encoded_rustflags_changes_key() {
        // cargo passes flags via CARGO_ENCODED_RUSTFLAGS (\x1f-separated); they
        // affect codegen, so they must be folded into the key.
        let _lock = key_test_lock();
        if !rustc_available() {
            return;
        }
        let old = std::env::var_os("CARGO_ENCODED_RUSTFLAGS");
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("lib.rs");
        std::fs::write(&src, "pub fn f() {}\n").unwrap();
        let args = base_args(&src);

        unsafe {
            std::env::remove_var("CARGO_ENCODED_RUSTFLAGS");
        }
        let key_unset = key_for(&args);

        unsafe {
            std::env::set_var("CARGO_ENCODED_RUSTFLAGS", "-C\x1ftarget-cpu=native");
        }
        let key_set = key_for(&args);

        unsafe {
            std::env::set_var("CARGO_ENCODED_RUSTFLAGS", "-C\x1ftarget-cpu=x86-64-v3");
        }
        let key_other = key_for(&args);

        restore_env_var("CARGO_ENCODED_RUSTFLAGS", old);

        assert_ne!(
            key_unset, key_set,
            "setting CARGO_ENCODED_RUSTFLAGS must change the key"
        );
        assert_ne!(
            key_set, key_other,
            "different encoded rustflags must diverge the key"
        );
    }

    #[test]
    fn key_cargo_cfg_env_changes_key() {
        // CARGO_CFG_* vars (cargo's reflection of `--cfg`) are folded into the
        // key so a build-script cfg change diverges it.
        let _lock = key_test_lock();
        if !rustc_available() {
            return;
        }
        let var = "CARGO_CFG_KACHE_TEST_FLAG";
        let old = std::env::var_os(var);
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("lib.rs");
        std::fs::write(&src, "pub fn f() {}\n").unwrap();
        let args = base_args(&src);

        unsafe {
            std::env::remove_var(var);
        }
        let key_unset = key_for(&args);

        unsafe {
            std::env::set_var(var, "1");
        }
        let key_set = key_for(&args);

        unsafe {
            std::env::set_var(var, "2");
        }
        let key_other = key_for(&args);

        restore_env_var(var, old);

        assert_ne!(key_unset, key_set, "a CARGO_CFG_* var must change the key");
        assert_ne!(
            key_set, key_other,
            "a different CARGO_CFG_* value must diverge the key"
        );
    }

    // ── "should change" cases — varying a codegen-affecting input ──

    #[cfg(unix)]
    #[test]
    fn key_matrix_rustc_version_changes_key() {
        let _lock = key_test_lock();
        if !rustc_available() {
            return;
        }

        let dir = tempfile::tempdir().unwrap();
        let rustc_a = write_rustc_version_wrapper(
            dir.path(),
            "toolchain-a",
            "rustc 1.95.0-test-a\nbinary: test-a\ncommit-hash: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );
        let rustc_b = write_rustc_version_wrapper(
            dir.path(),
            "toolchain-b",
            "rustc 1.95.0-test-b\nbinary: test-b\ncommit-hash: bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        );

        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let mut args_a = base_args(&source);
        args_a[0] = rustc_a.to_string_lossy().into_owned();
        let mut args_b = base_args(&source);
        args_b[0] = rustc_b.to_string_lossy().into_owned();

        assert_ne!(
            key_for(&args_a),
            key_for(&args_b),
            "`rustc --version --verbose` output must affect the cache key"
        );
    }

    #[test]
    fn key_matrix_manifest_dir_runtime_env_path_changes_key_across_workspaces() {
        let _lock = key_test_lock();
        if !rustc_available() {
            return;
        }

        let old_manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR");
        let dir = tempfile::tempdir().unwrap();
        let workspace_a = dir.path().join("checkout-a");
        let workspace_b = dir.path().join("checkout-b");

        fn write_helper(workspace: &Path) -> PathBuf {
            let src = workspace.join("helper/src");
            std::fs::create_dir_all(&src).unwrap();
            let lib = src.join("lib.rs");
            std::fs::write(
                &lib,
                r#"pub fn manifest_dir() -> &'static str {
    env!("CARGO_MANIFEST_DIR")
}
"#,
            )
            .unwrap();
            lib
        }

        let source_a = write_helper(&workspace_a);
        let source_b = write_helper(&workspace_b);
        let fh = FileHasher::new();

        let manifest_a = workspace_a.join("helper").canonicalize().unwrap();
        unsafe {
            std::env::set_var("CARGO_MANIFEST_DIR", manifest_a);
        }
        let parsed_a = RustcArgs::parse(&base_args(&source_a)).unwrap();
        let pn_a = PathNormalizer::from_env(Some(&workspace_a));
        let key_a = compute_cache_key(&parsed_a, &fh, &pn_a).unwrap();

        let manifest_b = workspace_b.join("helper").canonicalize().unwrap();
        unsafe {
            std::env::set_var("CARGO_MANIFEST_DIR", manifest_b);
        }
        let parsed_b = RustcArgs::parse(&base_args(&source_b)).unwrap();
        let pn_b = PathNormalizer::from_env(Some(&workspace_b));
        let key_b = compute_cache_key(&parsed_b, &fh, &pn_b).unwrap();

        restore_env_var("CARGO_MANIFEST_DIR", old_manifest_dir);

        assert_ne!(
            key_a, key_b,
            "CARGO_MANIFEST_DIR is a runtime env! value and must stay checkout-specific"
        );
    }

    #[test]
    fn key_matrix_out_dir_include_pattern_stays_stable_across_workspaces() {
        let _lock = key_test_lock();
        if !rustc_available() {
            return;
        }

        let old_out_dir = std::env::var_os("OUT_DIR");
        let old_manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR");
        let dir = tempfile::tempdir().unwrap();
        let workspace_a = dir.path().join("checkout-a");
        let workspace_b = dir.path().join("checkout-b");

        fn write_generated_include_crate(workspace: &Path) -> (PathBuf, PathBuf) {
            let src = workspace.join("src");
            let out_dir = workspace.join("target/debug/build/include-crate/out");
            std::fs::create_dir_all(&src).unwrap();
            std::fs::create_dir_all(&out_dir).unwrap();

            let generated = out_dir.join("generated.rs");
            std::fs::write(&generated, b"pub fn generated() -> u8 { 7 }\n").unwrap();

            let lib = src.join("lib.rs");
            std::fs::write(
                &lib,
                r#"include!(concat!(env!("OUT_DIR"), "/generated.rs"));

pub fn value() -> u8 {
    generated()
}
"#,
            )
            .unwrap();
            (lib, out_dir)
        }

        let (source_a, out_a) = write_generated_include_crate(&workspace_a);
        let (source_b, out_b) = write_generated_include_crate(&workspace_b);
        let fh = FileHasher::new();

        let out_a = out_a.canonicalize().unwrap();
        unsafe {
            std::env::set_var("OUT_DIR", &out_a);
            std::env::set_var("CARGO_MANIFEST_DIR", &workspace_a);
        }
        let parsed_a = RustcArgs::parse(&base_args(&source_a)).unwrap();
        let pn_a = PathNormalizer::from_env(Some(&workspace_a));
        let key_a = compute_cache_key(&parsed_a, &fh, &pn_a).unwrap();

        let out_b = out_b.canonicalize().unwrap();
        unsafe {
            std::env::set_var("OUT_DIR", &out_b);
            std::env::set_var("CARGO_MANIFEST_DIR", &workspace_b);
        }
        let parsed_b = RustcArgs::parse(&base_args(&source_b)).unwrap();
        let pn_b = PathNormalizer::from_env(Some(&workspace_b));
        let key_b = compute_cache_key(&parsed_b, &fh, &pn_b).unwrap();

        restore_env_var("OUT_DIR", old_out_dir);
        restore_env_var("CARGO_MANIFEST_DIR", old_manifest_dir);

        assert_eq!(
            key_a, key_b,
            "OUT_DIR include!() paths should stay portable across workspaces"
        );
    }

    #[test]
    fn key_matrix_out_dir_dual_pattern_diverges_across_workspaces() {
        let _lock = key_test_lock();
        if !rustc_available() {
            return;
        }

        let old_out_dir = std::env::var_os("OUT_DIR");
        let dir = tempfile::tempdir().unwrap();
        let workspace_a = dir.path().join("checkout-a");
        let workspace_b = dir.path().join("checkout-b");

        fn write_dual_pattern_crate(workspace: &Path) -> (PathBuf, PathBuf) {
            let src = workspace.join("src");
            let out_dir = workspace.join("target/debug/build/dual-crate/out");
            std::fs::create_dir_all(&src).unwrap();
            std::fs::create_dir_all(&out_dir).unwrap();

            let generated = out_dir.join("generated.rs");
            std::fs::write(&generated, b"pub fn generated() -> u8 { 7 }\n").unwrap();

            let lib = src.join("lib.rs");
            std::fs::write(
                &lib,
                r#"include!(concat!(env!("OUT_DIR"), "/generated.rs"));

pub const OUT_DIR_AT_COMPILE_TIME: &str = env!("OUT_DIR");

pub fn value() -> (&'static str, u8) {
    (OUT_DIR_AT_COMPILE_TIME, generated())
}
"#,
            )
            .unwrap();
            (lib, out_dir)
        }

        let (source_a, out_a) = write_dual_pattern_crate(&workspace_a);
        let (source_b, out_b) = write_dual_pattern_crate(&workspace_b);
        let fh = FileHasher::new();

        let out_a = out_a.canonicalize().unwrap();
        unsafe {
            std::env::set_var("OUT_DIR", &out_a);
        }
        let parsed_a = RustcArgs::parse(&base_args(&source_a)).unwrap();
        let pn_a = PathNormalizer::from_env(Some(&workspace_a));
        let key_a = compute_cache_key(&parsed_a, &fh, &pn_a).unwrap();

        let out_b = out_b.canonicalize().unwrap();
        unsafe {
            std::env::set_var("OUT_DIR", &out_b);
        }
        let parsed_b = RustcArgs::parse(&base_args(&source_b)).unwrap();
        let pn_b = PathNormalizer::from_env(Some(&workspace_b));
        let key_b = compute_cache_key(&parsed_b, &fh, &pn_b).unwrap();

        restore_env_var("OUT_DIR", old_out_dir);

        assert_ne!(
            key_a, key_b,
            "OUT_DIR dual pattern must stay checkout-specific: include!() alone is path-only, \
             but env!(\"OUT_DIR\") as a runtime value bakes the absolute path into the artifact"
        );
    }

    #[test]
    fn key_matrix_emit_changes_key() {
        // `cargo check` runs rustc --emit=metadata (-> .rmeta);
        // `cargo build` runs --emit=link (-> .rlib). Same crate, same
        // everything else the key hashes => without hashing `emit` the
        // two collide and a check entry could be served to a build.
        if !rustc_available() {
            return;
        }
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let mut metadata = base_args(&source);
        metadata.push("--emit=metadata".to_string());
        let mut link = base_args(&source);
        link.push("--emit=link".to_string());

        assert_ne!(
            key_for(&metadata),
            key_for(&link),
            "`--emit=metadata` vs `--emit=link` must produce different keys"
        );
    }

    #[test]
    fn key_matrix_opt_level_changes_key() {
        if !rustc_available() {
            return;
        }
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let mut o0 = base_args(&source);
        o0.extend(["-C".to_string(), "opt-level=0".to_string()]);
        let mut o3 = base_args(&source);
        o3.extend(["-C".to_string(), "opt-level=3".to_string()]);

        assert_ne!(
            key_for(&o0),
            key_for(&o3),
            "`-C opt-level` must affect the key"
        );
    }

    #[test]
    fn key_matrix_debug_assertions_changes_key() {
        if !rustc_available() {
            return;
        }
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let mut on = base_args(&source);
        on.extend(["-C".to_string(), "debug-assertions=on".to_string()]);
        let mut off = base_args(&source);
        off.extend(["-C".to_string(), "debug-assertions=off".to_string()]);

        assert_ne!(
            key_for(&on),
            key_for(&off),
            "`-C debug-assertions` must affect the key"
        );
    }

    #[test]
    fn key_matrix_cfg_changes_key() {
        if !rustc_available() {
            return;
        }
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let base = base_args(&source);
        let mut with_cfg = base_args(&source);
        with_cfg.extend(["--cfg".to_string(), "extra_feature".to_string()]);

        assert_ne!(
            key_for(&base),
            key_for(&with_cfg),
            "a `--cfg` value must affect the key"
        );
    }

    #[test]
    fn key_matrix_feature_changes_key() {
        if !rustc_available() {
            return;
        }
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let mut std_feat = base_args(&source);
        std_feat.extend(["--cfg".to_string(), "feature=\"std\"".to_string()]);
        let mut both = std_feat.clone();
        both.extend(["--cfg".to_string(), "feature=\"derive\"".to_string()]);

        assert_ne!(
            key_for(&std_feat),
            key_for(&both),
            "adding a feature must affect the key"
        );
    }

    #[test]
    fn key_matrix_edition_changes_key() {
        if !rustc_available() {
            return;
        }
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let mut e2018 = base_args(&source);
        e2018.retain(|a| a != "--edition=2021");
        e2018.push("--edition=2018".to_string());
        let e2021 = base_args(&source); // already --edition=2021

        assert_ne!(
            key_for(&e2018),
            key_for(&e2021),
            "`--edition` must affect the key"
        );
    }

    #[test]
    fn key_matrix_target_changes_key() {
        if !rustc_available() {
            return;
        }
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        // Two distinct triples — neither needs to be installed; the
        // key hashes the `--target` string, it does not invoke a
        // cross-compile.
        let mut t1 = base_args(&source);
        t1.push("--target=x86_64-unknown-linux-gnu".to_string());
        let mut t2 = base_args(&source);
        t2.push("--target=aarch64-apple-darwin".to_string());

        assert_ne!(
            key_of_flags(&t1),
            key_of_flags(&t2),
            "`--target` must affect the key"
        );
    }

    #[test]
    fn key_matrix_crate_type_changes_key() {
        if !rustc_available() {
            return;
        }
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let rlib = base_args(&source); // --crate-type lib
        let mut staticlib = base_args(&source);
        for a in staticlib.iter_mut() {
            if a == "lib" {
                *a = "staticlib".to_string();
            }
        }

        assert_ne!(
            key_for(&rlib),
            key_for(&staticlib),
            "`--crate-type` must affect the key"
        );
    }

    #[test]
    fn key_matrix_rustflags_env_changes_key() {
        if !rustc_available() {
            return;
        }
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();
        let args = base_args(&source);

        let saved = std::env::var("RUSTFLAGS").ok();

        // SAFETY: env access is serialized by the process-state test lock; restored below.
        unsafe { std::env::remove_var("RUSTFLAGS") };
        let key_none = key_for(&args);

        unsafe { std::env::set_var("RUSTFLAGS", "-C target-cpu=native") };
        let key_set = key_for(&args);

        match saved {
            Some(v) => unsafe { std::env::set_var("RUSTFLAGS", v) },
            None => unsafe { std::env::remove_var("RUSTFLAGS") },
        }

        assert_ne!(key_none, key_set, "`RUSTFLAGS` env var must affect the key");
    }

    /// Direct test of the `normalize_rustflags` helper.
    #[test]
    fn normalize_rustflags_collapses_whitespace() {
        assert_eq!(normalize_rustflags("-C a -C b"), "-C a -C b");
        // Multiple spaces between tokens → single space.
        assert_eq!(normalize_rustflags("-C a    -C b"), "-C a -C b");
        // Leading / trailing whitespace stripped.
        assert_eq!(normalize_rustflags("  -C a   -C b  "), "-C a -C b");
        // Mixed whitespace (tabs, newlines) treated as whitespace.
        assert_eq!(normalize_rustflags("-C a\t\t-C b"), "-C a -C b");
        // Order is preserved (rustc resolves later flags over earlier ones).
        assert_eq!(normalize_rustflags("-Cfoo=b   -Cfoo=a"), "-Cfoo=b -Cfoo=a");
        assert_ne!(
            normalize_rustflags("-Cfoo=a -Cfoo=b"),
            normalize_rustflags("-Cfoo=b -Cfoo=a")
        );
    }

    /// Direct test of the `scrub_remap_from_prefixes` helper.
    #[test]
    fn scrub_remap_from_prefixes_collapses_from_keeps_to() {
        let scrub = |s: &str| scrub_remap_from_prefixes(s.split_whitespace()).join(" ");

        // The core case: two checkouts converge, TO preserved.
        assert_eq!(
            scrub("--remap-path-prefix=/abs/clone-a/=/topsrcdir/"),
            "--remap-path-prefix=<REMAP_FROM>=/topsrcdir/"
        );
        assert_eq!(
            scrub("--remap-path-prefix=/abs/clone-a/=/topsrcdir/"),
            scrub("--remap-path-prefix=/abs/clone-b/=/topsrcdir/"),
            "different checkout `from` paths must collapse identically"
        );

        // Space-separated form: value is the next token.
        assert_eq!(
            scrub("--remap-path-prefix /abs/clone-a/=/topsrcdir/"),
            "--remap-path-prefix <REMAP_FROM>=/topsrcdir/"
        );

        // The clang `-f*-prefix-map` family (equals form).
        for flag in [
            "-ffile-prefix-map",
            "-fdebug-prefix-map",
            "-fmacro-prefix-map",
        ] {
            assert_eq!(
                scrub(&format!("{flag}=/abs/clone-a/=/virt/")),
                format!("{flag}=<REMAP_FROM>=/virt/")
            );
        }

        // Split on the LAST `=` (FROM may contain `=`), matching rustc/clang.
        assert_eq!(
            scrub("--remap-path-prefix=/a=b/clone-a/=/topsrcdir/"),
            "--remap-path-prefix=<REMAP_FROM>=/topsrcdir/"
        );

        // Changing TO must NOT be scrubbed away — it stays in the result.
        assert_ne!(
            scrub("--remap-path-prefix=/abs/clone-a/=/topsrcdir/"),
            scrub("--remap-path-prefix=/abs/clone-a/=/other/")
        );

        // Non-remap flags pass through verbatim.
        assert_eq!(
            scrub("-C opt-level=2 -C debuginfo=2"),
            "-C opt-level=2 -C debuginfo=2"
        );
        // A remap value with no `=` is malformed and left untouched.
        assert_eq!(
            scrub("--remap-path-prefix=garbage"),
            "--remap-path-prefix=garbage"
        );
    }

    #[test]
    fn normalize_direct_remap_normalizes_known_from_but_keeps_to() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let workspace =
            PathBuf::from(crate::path_normalizer::canonical_string(&workspace).unwrap());
        let normalizer = PathNormalizer::from_env(Some(&workspace));
        let from = workspace.join("dir=with=equals");
        let to = workspace.join("literal-to");
        let value = format!("{}={}", from.display(), to.display());

        assert_eq!(
            normalize_direct_remap_value(&value, &normalizer),
            format!(
                "{}={}",
                Path::new("<WORKSPACE>").join("dir=with=equals").display(),
                to.display()
            ),
            "only FROM is normalized; TO remains verbatim"
        );
        assert_eq!(
            normalize_direct_remap_value("malformed", &normalizer),
            "malformed"
        );
    }

    #[test]
    fn key_matrix_direct_remap_path_prefix_is_keyed_portably_and_in_order() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let none = key_of_flags(&flag_base(&source, &[]));
        let separated = key_of_flags(&flag_base(
            &source,
            &["--remap-path-prefix", "/work/clone-a=/virtual/src"],
        ));
        let attached = key_of_flags(&flag_base(
            &source,
            &["--remap-path-prefix=/work/clone-a=/virtual/src"],
        ));
        let unrelated_from = key_of_flags(&flag_base(
            &source,
            &["--remap-path-prefix=/work/clone-b=/virtual/src"],
        ));
        let other_target = key_of_flags(&flag_base(
            &source,
            &["--remap-path-prefix=/work/clone-a=/virtual/other"],
        ));
        let remap_root = format!("--remap-path-prefix={}=/virtual/root", dir.path().display());
        let remap_source = format!("--remap-path-prefix={}=/virtual/source", source.display());
        let order_ab = key_of_flags(&flag_base(&source, &[&remap_root, &remap_source]));
        let order_ba = key_of_flags(&flag_base(&source, &[&remap_source, &remap_root]));

        assert_ne!(none, separated, "adding a direct remap must change the key");
        assert_eq!(separated, attached, "both rustc spellings are equivalent");
        assert_ne!(
            separated, unrelated_from,
            "without a matching normalization rule, FROM remains semantic"
        );
        assert_ne!(
            separated, other_target,
            "the remap target is embedded in artifacts and must remain key-visible"
        );
        assert_ne!(order_ab, order_ba, "overlapping remap order is semantic");

        let clone_a = dir.path().join("clone-a");
        let clone_b = dir.path().join("clone-b");
        std::fs::create_dir_all(&clone_a).unwrap();
        std::fs::create_dir_all(&clone_b).unwrap();
        let portable_key = |workspace: &Path| {
            let workspace = workspace.canonicalize().unwrap();
            let remap = format!("--remap-path-prefix={}=/virtual/src", workspace.display());
            let mut parsed = RustcArgs::parse(&flag_base(&source, &[&remap])).unwrap();
            parsed.source_file = None;
            compute_cache_key(
                &parsed,
                &FileHasher::new(),
                &PathNormalizer::from_env(Some(&workspace)),
            )
            .unwrap()
        };
        assert_eq!(
            portable_key(&clone_a),
            portable_key(&clone_b),
            "known workspace prefixes normalize portably across checkouts"
        );
    }

    /// The cross-checkout fix (v14): a build system's own `--remap-path-prefix`
    /// (Firefox `--enable-path-remapping`) carries the checkout path on its
    /// `from` side, so two clones at different paths must still hash identically
    /// — and changing the stable `to` target must still diverge the key.
    #[test]
    fn key_matrix_rustflags_remap_path_prefix_stable_across_checkouts() {
        if !rustc_available() {
            return;
        }
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();
        let args = base_args(&source);

        let saved = std::env::var("RUSTFLAGS").ok();
        let set = |v: &str| unsafe { std::env::set_var("RUSTFLAGS", v) };

        // SAFETY: env access is serialized by the process-state test lock; restored below.
        set("--remap-path-prefix=/work/clone-a/=/topsrcdir/");
        let key_a = key_for(&args);
        set("--remap-path-prefix=/work/clone-b/=/topsrcdir/");
        let key_b = key_for(&args);
        // Same flag, different stable target → must diverge.
        set("--remap-path-prefix=/work/clone-a/=/elsewhere/");
        let key_other_to = key_for(&args);

        match saved {
            Some(v) => unsafe { std::env::set_var("RUSTFLAGS", v) },
            None => unsafe { std::env::remove_var("RUSTFLAGS") },
        }

        assert_eq!(
            key_a, key_b,
            "different checkout paths under the same remap target must not change the key"
        );
        assert_ne!(
            key_a, key_other_to,
            "changing the remap target (`to`) must still change the key"
        );
    }

    /// Cosmetic whitespace differences between cargo / mach assemblies
    /// of the same logical RUSTFLAGS must not change the cache key.
    /// Firefox bench surfaced this as the dominant source of leaf
    /// cache-key divergence; this test pins the fix.
    #[test]
    fn key_matrix_rustflags_whitespace_does_not_change_key() {
        if !rustc_available() {
            return;
        }
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();
        let args = base_args(&source);

        let saved = std::env::var("RUSTFLAGS").ok();

        // SAFETY: env access is serialized by the process-state test lock; restored below.
        unsafe { std::env::set_var("RUSTFLAGS", "-C debuginfo=2 -C codegen-units=1") };
        let key_tight = key_for(&args);

        // Same flags, cosmetically different whitespace.
        unsafe { std::env::set_var("RUSTFLAGS", "-C debuginfo=2    -C codegen-units=1") };
        let key_loose = key_for(&args);

        // Same flags, leading whitespace.
        unsafe { std::env::set_var("RUSTFLAGS", "  -C debuginfo=2 -C codegen-units=1  ") };
        let key_padded = key_for(&args);

        match saved {
            Some(v) => unsafe { std::env::set_var("RUSTFLAGS", v) },
            None => unsafe { std::env::remove_var("RUSTFLAGS") },
        }

        assert_eq!(
            key_tight, key_loose,
            "RUSTFLAGS extra-whitespace must not change the key"
        );
        assert_eq!(
            key_tight, key_padded,
            "RUSTFLAGS leading/trailing whitespace must not change the key"
        );
    }

    // ── "should NOT change" cases — diagnostics-only inputs ──
    //
    // These flags steer only what rustc *prints*, never the emitted
    // artifact bytes. If the key changes for one of them, that is
    // over-keying (a missed hit) — the test will fail and surface it
    // rather than silently weakening the key.
    //
    // All lint configuration is deliberately absent from this section: it
    // cannot change successful artifact bytes, but it can change whether the
    // compile FAILS, and a hit replays success. See the tests below.

    #[test]
    fn key_matrix_outcome_lint_configuration_changes_key() {
        // `-D warnings` promotes warnings to hard errors: two builds
        // differing only here can disagree about whether the compile
        // succeeded while emitting byte-identical objects on success.
        // Since a hit replays success, the key MUST move (review
        // finding #2) — otherwise an entry stored without the gate
        // serves green to a build the gate should have failed.
        if !rustc_available() {
            return;
        }
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let base = base_args(&source);
        let mut with_lint = base_args(&source);
        with_lint.extend(["-D".to_string(), "warnings".to_string()]);
        let mut with_forbid = base_args(&source);
        with_forbid.extend(["--forbid".to_string(), "warnings".to_string()]);
        let mut with_cap = base_args(&source);
        with_cap.extend(["--cap-lints".to_string(), "allow".to_string()]);
        let mut with_attached = base_args(&source);
        with_attached.push("-Dwarnings".to_string());
        let mut with_warn = base_args(&source);
        with_warn.extend(["-W".to_string(), "unused".to_string()]);
        let mut with_allow = base_args(&source);
        with_allow.extend(["-A".to_string(), "dead_code".to_string()]);
        let mut with_check_cfg = base_args(&source);
        with_check_cfg.extend(["--check-cfg".to_string(), "cfg(foo)".to_string()]);
        let mut with_other_check_cfg = base_args(&source);
        with_other_check_cfg.push("--check-cfg=cfg(bar)".to_string());

        assert_ne!(
            key_for(&base),
            key_for(&with_lint),
            "an outcome-affecting lint gate (`-D warnings`) changes whether \
             the compile fails and MUST change the key"
        );
        assert_ne!(
            key_for(&base),
            key_for(&with_forbid),
            "`--forbid` is outcome-affecting and must change the key"
        );
        assert_ne!(
            key_for(&base),
            key_for(&with_cap),
            "`--cap-lints` re-levels every lint and must change the key"
        );
        assert_ne!(
            key_for(&with_lint),
            key_for(&with_attached),
            "separated (`-D warnings`) and attached (`-Dwarnings`) spellings \
             carry different tokens; each keys distinctly by design"
        );
        assert_ne!(
            key_for(&base),
            key_for(&with_warn),
            "-W can activate a lint that a deny group makes fatal"
        );
        assert_ne!(
            key_for(&base),
            key_for(&with_allow),
            "-A can relax an otherwise fatal lint"
        );
        assert_ne!(
            key_for(&base),
            key_for(&with_check_cfg),
            "--check-cfg controls the unexpected_cfgs outcome"
        );
        assert_ne!(
            key_for(&with_check_cfg),
            key_for(&with_other_check_cfg),
            "different accepted cfg sets must not share a key"
        );
    }

    #[test]
    fn check_cfg_values_are_not_path_normalized() {
        if !rustc_available() {
            return;
        }
        let _lock = key_test_lock();
        let root = tempfile::tempdir().unwrap();
        let key_at = |workspace: &Path, with_check_cfg: bool| {
            std::fs::create_dir_all(workspace).unwrap();
            let source = workspace.join("lib.rs");
            std::fs::write(&source, b"pub fn hello() {}").unwrap();
            let mut args = base_args(&source);
            if with_check_cfg {
                let semantic_path = workspace
                    .join("generated")
                    .to_string_lossy()
                    .replace('\\', "/");
                args.extend([
                    "--check-cfg".to_string(),
                    format!("cfg(build_path, values(\"{semantic_path}\"))"),
                ]);
            }
            let parsed = RustcArgs::parse(&args).unwrap();
            compute_cache_key(
                &parsed,
                &FileHasher::new(),
                &PathNormalizer::from_env(Some(workspace)),
            )
            .unwrap()
        };

        let workspace_a = root.path().join("checkout-a");
        let workspace_b = root.path().join("checkout-b");
        assert_eq!(
            key_at(&workspace_a, false),
            key_at(&workspace_b, false),
            "the control must prove ordinary workspace paths normalize portably"
        );
        assert_ne!(
            key_at(&workspace_a, true),
            key_at(&workspace_b, true),
            "path-looking check-cfg values are semantic strings and must stay raw"
        );
    }

    #[test]
    fn key_matrix_outcome_lint_gates_key_by_pairing_not_multiset() {
        // The gates are captured as a flat token stream, so folding them
        // sorted would collapse permutations that mean different things:
        // `-D unsafe_code -F warnings` denies unsafe_code (a `#[allow]` in
        // the crate can still re-allow it) and forbids warnings, while the
        // swap forbids unsafe_code (no `#[allow]` escape) and denies
        // warnings. Same token multiset, different outcomes — so the key
        // must distinguish them.
        if !rustc_available() {
            return;
        }
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let gated = |gates: &[&str]| {
            let mut args = base_args(&source);
            args.extend(gates.iter().map(|s| s.to_string()));
            key_for(&args)
        };

        assert_ne!(
            gated(&["-D", "unsafe_code", "-F", "warnings"]),
            gated(&["-F", "unsafe_code", "-D", "warnings"]),
            "swapping which lint is denied and which is forbidden changes \
             the outcome and MUST change the key"
        );
        // (`--force-warn` takes a single lint, never a group, so both sides
        // name concrete lints.)
        assert_ne!(
            gated(&["-D", "unused_mut", "--force-warn", "deprecated"]),
            gated(&["-D", "deprecated", "--force-warn", "unused_mut"]),
            "swapping the deny and force-warn targets changes the outcome \
             and MUST change the key"
        );
        // Argv order is kept as the conservative choice: rustc resolves
        // repeated levels for the same lint last-wins, so a reordered gate
        // list can be a different build. A given build config emits a stable
        // order, so preserving it costs no hits.
        assert_ne!(
            gated(&["-D", "warnings", "-A", "unused", "-D", "unused"]),
            gated(&["-D", "unused", "-A", "unused", "-D", "warnings"]),
            "gate order is preserved in the key"
        );
    }

    #[test]
    fn key_matrix_error_format_does_not_change_key() {
        // `--error-format=json` changes how diagnostics are rendered
        // (cargo always passes it) — never the artifact. The key must
        // not move.
        if !rustc_available() {
            return;
        }
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let base = base_args(&source);
        let mut with_fmt = base_args(&source);
        with_fmt.push("--error-format=json".to_string());

        assert_eq!(
            key_for(&base),
            key_for(&with_fmt),
            "`--error-format` is diagnostics-only and must NOT change \
             the key — a change here is over-keying"
        );
    }

    #[test]
    fn the_stashed_tree_digest_is_taken_once() {
        let _ = LAST_KEY_TREE_DIGEST.try_with(|stash| *stash.borrow_mut() = None);
        assert_eq!(take_last_tree_digest(), None);
        let _ =
            LAST_KEY_TREE_DIGEST.try_with(|stash| *stash.borrow_mut() = Some("tree-1".to_string()));
        assert_eq!(take_last_tree_digest().as_deref(), Some("tree-1"));
        assert_eq!(take_last_tree_digest(), None, "taken, not peeked");
    }

    #[test]
    fn a_deferred_discovery_says_so_when_displayed() {
        let text = DeferredDiscovery.to_string();
        assert!(text.contains("deferred"), "{text}");
        let error: anyhow::Error = DeferredDiscovery.into();
        assert!(error.downcast_ref::<DeferredDiscovery>().is_some());
    }

    #[test]
    fn a_shared_identity_needs_an_absolute_target_and_a_predictable_unit() {
        let _lock = key_test_lock();
        let _workspace = crate::config::tests::set_env_for_test(
            "CARGO_MANIFEST_DIR",
            Some(std::ffi::OsStr::new("/w/kt")),
        );
        let parse = |argv: &[&str]| {
            RustcArgs::parse(&argv.iter().map(|a| (*a).to_string()).collect::<Vec<_>>()).unwrap()
        };
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("debug").join("deps");
        std::fs::create_dir_all(&out).unwrap();
        let out_str = out.to_str().unwrap();
        let eligible = parse(&[
            "rustc",
            "--crate-name",
            "kt",
            "src/lib.rs",
            "--out-dir",
            out_str,
        ]);
        assert!(rustc_shared_prediction_identity(&eligible).is_some());
        let relative = parse(&[
            "rustc",
            "--crate-name",
            "kt",
            "src/lib.rs",
            "--out-dir",
            "target/debug/deps",
        ]);
        assert!(
            rustc_shared_prediction_identity(&relative).is_none(),
            "a relative target directory is not shareable"
        );
        let with_macro = parse(&[
            "rustc",
            "--crate-name",
            "kt",
            "src/lib.rs",
            "--out-dir",
            out_str,
            "--extern",
            "my_macro=/t/debug/deps/libmy_macro-3.so",
        ]);
        assert!(
            rustc_shared_prediction_identity(&with_macro).is_none(),
            "a proc-macro dependency outside the registry is not predictable by closure alone"
        );
    }

    #[test]
    fn a_discovery_identity_is_the_shared_one_when_predictions_are_on() {
        let _lock = key_test_lock();
        let parse = |argv: &[&str]| {
            RustcArgs::parse(&argv.iter().map(|a| (*a).to_string()).collect::<Vec<_>>()).unwrap()
        };
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("debug").join("deps");
        std::fs::create_dir_all(&out).unwrap();
        let out_str = out.to_str().unwrap();
        let eligible = parse(&[
            "rustc",
            "--crate-name",
            "kt",
            "src/lib.rs",
            "--out-dir",
            out_str,
        ]);
        let with_macro = parse(&[
            "rustc",
            "--crate-name",
            "kt",
            "src/lib.rs",
            "--out-dir",
            out_str,
            "--extern",
            "my_macro=/t/debug/deps/libmy_macro-3.so",
        ]);
        let db = dir.path().join("index.db");
        let off = FileHasher::persistent(&db);
        assert_eq!(prediction_discovery_identity(&eligible, &off), None);
        let on = FileHasher::persistent(&db).with_input_predictions(true);
        let identity = prediction_discovery_identity(&eligible, &on).unwrap();
        assert_eq!(
            Some(identity.clone()),
            rustc_shared_prediction_identity(&eligible),
            "the shared spelling comes first"
        );
        assert!(!identity.is_empty());
        assert_eq!(
            prediction_discovery_identity(&with_macro, &on),
            None,
            "not shareable and not predictable: no flight"
        );
    }

    /// A record for a unit under the tree guard is usable only while the
    /// crate tree it was made from is unchanged; a record without a tree
    /// digest is not usable at all for such a unit.
    #[test]
    fn a_guarded_record_is_refused_when_the_tree_changed() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let package = dir
            .path()
            .join("registry")
            .join("src")
            .join("index-1")
            .join("kt-1.0.0");
        std::fs::create_dir_all(package.join("src")).unwrap();
        std::fs::write(package.join("src/lib.rs"), "pub fn v() {}\n").unwrap();
        let parse = |argv: &[&str]| {
            RustcArgs::parse(&argv.iter().map(|a| (*a).to_string()).collect::<Vec<_>>()).unwrap()
        };
        let out = dir.path().join("debug").join("deps");
        std::fs::create_dir_all(&out).unwrap();
        let with_macro = parse(&[
            "rustc",
            "--crate-name",
            "kt",
            package.join("src/lib.rs").to_str().unwrap(),
            "--out-dir",
            out.to_str().unwrap(),
            "--extern",
            "my_macro=/t/debug/deps/libmy_macro-3.so",
        ]);
        let manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR");
        // SAFETY: the key test lock serialises environment edits.
        unsafe {
            std::env::set_var("CARGO_MANIFEST_DIR", &package);
            std::env::remove_var("OUT_DIR");
        }
        let on = FileHasher::persistent(&dir.path().join("index.db")).with_input_predictions(true);
        let tree = crate_tree_digest(&on).expect("a registry package has a tree digest");
        let identity = rustc_prediction_identity(&with_macro).unwrap();
        let closure = DepInfo {
            source_files: vec![package.join("src/lib.rs")],
            env_deps: Vec::new(),
        };

        on.record_input_prediction(&identity, Some("kt"), &closure, None);
        assert_eq!(
            predicted_key_inputs(&with_macro, &on),
            Err(Rejection::NoRecord),
            "a record without a tree digest cannot guard a proc-macro unit"
        );

        on.record_input_prediction(&identity, Some("kt"), &closure, Some("stale".to_string()));
        assert_eq!(
            predicted_key_inputs(&with_macro, &on),
            Err(Rejection::TreeChanged)
        );

        on.record_input_prediction(&identity, Some("kt"), &closure, Some(tree.clone()));
        let current = predicted_key_inputs(&with_macro, &on);
        assert_ne!(current, Err(Rejection::TreeChanged), "{current:?}");
        assert_ne!(current, Err(Rejection::NoRecord), "{current:?}");
        assert_eq!(take_last_tree_digest(), Some(tree));

        std::fs::write(package.join("extra.txt"), "read by the macro").unwrap();
        assert_eq!(
            predicted_key_inputs(&with_macro, &on),
            Err(Rejection::TreeChanged),
            "a file added under the package changes the tree"
        );
        unsafe {
            match manifest_dir {
                Some(value) => std::env::set_var("CARGO_MANIFEST_DIR", value),
                None => std::env::remove_var("CARGO_MANIFEST_DIR"),
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn clippy_identity_reads_the_process_environment() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let driver = dir.path().join("clippy-driver");
        kache_fs::testutil::write_executable(
            &driver,
            "#!/bin/sh\necho 'clippy 0.1.98 (abc 2026-09-01)'\n",
        );
        let member = dir.path().join("member");
        std::fs::create_dir_all(&member).unwrap();
        let manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR");
        // SAFETY: the key test lock serialises environment edits.
        unsafe {
            std::env::set_var("CARGO_MANIFEST_DIR", &member);
            std::env::remove_var("CLIPPY_CONF_DIR");
            std::env::remove_var("CLIPPY_ARGS");
        }
        let identity = clippy_identity(&driver).unwrap();
        let expected = clippy_identity_in(
            &driver,
            |name| std::env::var_os(name),
            std::env::current_dir().ok(),
        )
        .unwrap();
        unsafe {
            match manifest_dir {
                Some(value) => std::env::set_var("CARGO_MANIFEST_DIR", value),
                None => std::env::remove_var("CARGO_MANIFEST_DIR"),
            }
        }
        assert_eq!(identity, expected);
        assert!(identity.starts_with("clippy 0.1.98"), "{identity}");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn the_linux_libc_signature_names_the_running_libc() {
        let family = if cfg!(target_env = "musl") {
            LinuxLibcFamily::Musl
        } else {
            LinuxLibcFamily::Gnu
        };
        let signature = probe_linux_libc_signature(family).unwrap();
        assert!(!signature.is_empty());
        assert!(
            signature.chars().any(|c| c.is_ascii_digit()),
            "a version, not a label: {signature}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn linker_identity_is_the_first_version_line_of_the_configured_linker() {
        let _lock = key_test_lock();
        let dir = tempfile::tempdir().unwrap();
        // SAFETY: the key test lock serialises environment edits.
        unsafe { std::env::set_var("KACHE_CACHE_DIR", dir.path()) };
        let linker = dir.path().join("my-ld");
        kache_fs::testutil::write_executable(
            &linker,
            "#!/bin/sh\necho 'my-ld 9.9'\necho 'second line'\n",
        );
        let parse = |argv: &[&str]| {
            RustcArgs::parse(&argv.iter().map(|a| (*a).to_string()).collect::<Vec<_>>()).unwrap()
        };
        let args = parse(&[
            "rustc",
            "src/lib.rs",
            "-C",
            &format!("linker={}", linker.display()),
        ]);
        let identity = get_linker_identity(&args);
        let missing = get_linker_identity(&parse(&[
            "rustc",
            "src/lib.rs",
            "-C",
            &format!("linker={}", dir.path().join("absent").display()),
        ]));
        unsafe { std::env::remove_var("KACHE_CACHE_DIR") };
        assert_eq!(identity.as_deref(), Some("my-ld 9.9"));
        assert_eq!(missing, None, "a linker that cannot run has no identity");
    }
}
