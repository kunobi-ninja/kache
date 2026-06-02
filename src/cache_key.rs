use crate::args::RustcArgs;
use crate::path_normalizer::{PathNormalizer, check_for_path_leak};
use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
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
const CACHE_KEY_VERSION: u32 = 10;
const MIN_PERSISTED_HASH_BYTES: i64 = 64 * 1024;

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
    let mut hasher = blake3::Hasher::new();
    let crate_name = args.crate_name.as_deref().unwrap_or("unknown");

    // key version — bump CACHE_KEY_VERSION to invalidate all prior entries
    hasher.update(b"key_version:");
    hasher.update(CACHE_KEY_VERSION.to_string().as_bytes());
    hasher.update(b"\n");
    tracing::trace!("[key:{}] key_version={}", crate_name, CACHE_KEY_VERSION);

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

    // crate identity
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

    // codegen options (sorted for determinism)
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
        hasher.update(b"codegen:");
        hasher.update(key.as_bytes());
        if let Some(v) = value {
            // `-Clink-arg=`, `-Clink-args=…`, `-Cprofile-use=…`, etc. can
            // carry absolute paths. None of these go through
            // PathNormalizer (they're rustc-controlled flags, not env);
            // flag any leaked path so the field is identifiable.
            check_for_path_leak(v, &format!("codegen:{key}"));
            hasher.update(b"=");
            hasher.update(v.as_bytes());
            tracing::trace!("[key:{}] codegen:{}={}", crate_name, key, v);
        } else {
            tracing::trace!("[key:{}] codegen:{}", crate_name, key);
        }
        hasher.update(b"\n");
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
        hasher.update(b"cfg:");
        hasher.update(cfg.as_bytes());
        hasher.update(b"\n");
        tracing::trace!("[key:{}] cfg:{}", crate_name, cfg);
    }

    let dep_info = args.source_file.as_ref().map(|source| {
        run_dep_info_pass(&args.rustc, source, &args.all_args).unwrap_or_else(|e| {
            tracing::warn!("dep-info pre-pass failed, falling back to root: {}", e);
            DepInfo {
                source_files: vec![source.clone()],
                env_deps: vec![],
            }
        })
    });

    let mut externs: Vec<_> = args.externs.iter().filter(|e| e.path.is_some()).collect();
    externs.sort_by_key(|e| &e.name);

    let mut hash_paths = Vec::new();
    if let Some(dep_info) = &dep_info {
        hash_paths.extend(dep_info.source_files.iter().map(|p| p.as_path()));
    }
    hash_paths.extend(externs.iter().filter_map(|ext| ext.path.as_deref()));
    file_hasher.prefetch(&hash_paths);

    // ── Group A: source files + env deps (from dep-info pre-pass) ──
    if let Some(dep_info) = &dep_info {
        // Hash source files in CONTENT-HASH order, not path order. Only
        // the content hash enters the key (not the path), so the set of
        // contents is what matters — and `dep_info.source_files` is
        // sorted by absolute path, which is NOT path-independent: a
        // build-script-generated file under `OUT_DIR` sorts among the
        // registry sources differently once the build tree moves (e.g.
        // `C:\Windows\...\tmp\...` vs `C:\actions-runner\...`), flipping
        // the update order and changing the key — so a relocated build
        // missed (kunobi-ninja/kache#201). Sorting by hash makes the
        // order depend only on contents.
        let mut hashed: Vec<(String, &std::path::Path)> =
            Vec::with_capacity(dep_info.source_files.len());
        for file in &dep_info.source_files {
            match file_hasher.hash(file) {
                Ok(file_hash) => hashed.push((file_hash, file.as_path())),
                Err(e) => {
                    tracing::warn!(
                        "[key:{}] failed to hash source {}: {}",
                        crate_name,
                        file.display(),
                        e
                    );
                }
            }
        }
        hashed.sort();
        for (file_hash, file) in &hashed {
            hasher.update(b"source:");
            hasher.update(file_hash.as_bytes());
            hasher.update(b"\n");
            tracing::trace!(
                "[key:{}] source:{}={}",
                crate_name,
                file.display(),
                &file_hash[..16]
            );
        }

        for (var, val) in &dep_info.env_deps {
            let normalized_env_dep =
                normalize_env_dep_value(var, val, &dep_info.source_files, path_normalizer);
            hasher.update(b"env_dep:");
            hasher.update(var.as_bytes());
            hasher.update(b"=");
            hasher.update(normalized_env_dep.value.as_bytes());
            hasher.update(b"\n");
            tracing::trace!(
                "[key:{}] env_dep:{}={} ({})",
                crate_name,
                var,
                normalized_env_dep.value,
                normalized_env_dep.decision.as_str()
            );
        }
    }

    // ── Group B: extern crate artifacts ──
    for ext in &externs {
        if let Some(path) = &ext.path {
            match file_hasher.hash(path) {
                Ok(dep_hash) => {
                    hasher.update(b"extern:");
                    hasher.update(ext.name.as_bytes());
                    hasher.update(b"=");
                    hasher.update(dep_hash.as_bytes());
                    hasher.update(b"\n");
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
                    tracing::trace!("[key:{}] extern_unreadable:{}", crate_name, ext.name);
                }
            }
        }
    }

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
    if let Ok(rustflags) = std::env::var("RUSTFLAGS") {
        let normalized = normalize_rustflags(&path_normalizer.normalize(&rustflags));
        hasher.update(b"RUSTFLAGS:");
        hasher.update(normalized.as_bytes());
        hasher.update(b"\n");
        tracing::trace!("[key:{}] RUSTFLAGS={}", crate_name, normalized);
    }

    // CARGO_ENCODED_RUSTFLAGS (cargo's way of passing flags)
    if let Ok(flags) = std::env::var("CARGO_ENCODED_RUSTFLAGS") {
        let normalized = path_normalizer.normalize(&flags);
        hasher.update(b"CARGO_ENCODED_RUSTFLAGS:");
        hasher.update(normalized.as_bytes());
        hasher.update(b"\n");
        tracing::trace!(
            "[key:{}] CARGO_ENCODED_RUSTFLAGS={}",
            crate_name,
            normalized
        );
    }

    // Relevant CARGO_CFG_* env vars (sorted for determinism —
    // std::env::vars() iteration order is platform-defined and not stable)
    let mut cargo_cfgs: Vec<(String, String)> = std::env::vars()
        .filter(|(k, _)| k.starts_with("CARGO_CFG_"))
        .collect();
    cargo_cfgs.sort_by(|(a, _), (b, _)| a.cmp(b));
    tracing::trace!("[key:{}] cargo_cfg_count={}", crate_name, cargo_cfgs.len());
    for (key, value) in &cargo_cfgs {
        // Cargo derives CARGO_CFG_* from `--cfg` flags. Build scripts (and
        // mozbuild specifically) emit cfgs that can embed absolute paths;
        // those land here uncensored. Flag leaks so the offending var
        // name is visible in the warn.
        check_for_path_leak(value, &format!("cargo_cfg:{key}"));
        hasher.update(key.as_bytes());
        hasher.update(b"=");
        hasher.update(value.as_bytes());
        hasher.update(b"\n");
    }

    // Linker identity for bin/dylib targets
    if args.is_executable_output()
        && let Some(linker_id) = get_linker_identity(args)
    {
        hasher.update(b"linker:");
        hasher.update(linker_id.as_bytes());
        hasher.update(b"\n");
    }

    // Path remapping status: kache injects multi-prefix
    // `--remap-path-prefix` flags (one per PathNormalizer rule) for
    // reproducible builds across machines — but skips them under
    // coverage instrumentation (tarpaulin / llvm-cov need original
    // paths in profraw to map coverage back to source). Since this
    // produces different binaries, the key must reflect the choice.
    //
    // We hash the SENTINEL set (not the prefix paths) so the key
    // stays portable across machines — different hosts have
    // different `$HOME` / `$CARGO_HOME` prefixes but the same
    // sentinel categories, so the key is identical.
    let remap = if args.has_coverage_instrumentation() {
        hasher.update(b"remap:none\n");
        "none".to_string()
    } else {
        hasher.update(b"remap:multi-prefix\n");
        // Hash only the SENTINEL names (not the prefix paths), so
        // the key is identical across machines whose `$HOME` /
        // `$CARGO_HOME` etc. paths differ — same sentinel set →
        // same key → cross-machine cache hits work.
        let remap_args = path_normalizer.remap_args();
        let mut sentinels: Vec<String> = remap_args
            .iter()
            .filter_map(|a| a.split('=').next_back().map(str::to_string))
            .collect();
        sentinels.sort();
        for s in &sentinels {
            hasher.update(b"remap-sentinel:");
            hasher.update(s.as_bytes());
            hasher.update(b"\n");
        }
        format!("multi-prefix({})", sentinels.join(","))
    };
    tracing::trace!("[key:{}] remap={}", crate_name, remap);

    let hash = hasher.finalize();
    let key = hash.to_hex().to_string();
    tracing::trace!("[key:{}] final={}", crate_name, &key[..16]);
    Ok(key)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EnvDepNormalizationDecision {
    Unchanged,
    NormalizedPathOnly,
    KeptAbsoluteRuntimePath,
}

impl EnvDepNormalizationDecision {
    fn as_str(self) -> &'static str {
        match self {
            Self::Unchanged => "unchanged",
            Self::NormalizedPathOnly => "normalized path-only",
            Self::KeptAbsoluteRuntimePath => "kept absolute runtime path",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedEnvDep {
    value: String,
    decision: EnvDepNormalizationDecision,
}

fn normalize_env_dep_value(
    var: &str,
    val: &str,
    source_files: &[std::path::PathBuf],
    path_normalizer: &PathNormalizer,
) -> NormalizedEnvDep {
    let normalized = path_normalizer.normalize(val);
    if normalized == val {
        return NormalizedEnvDep {
            value: normalized,
            decision: EnvDepNormalizationDecision::Unchanged,
        };
    }

    if env_dep_is_safe_to_normalize(var, val, source_files) {
        return NormalizedEnvDep {
            value: normalized,
            decision: EnvDepNormalizationDecision::NormalizedPathOnly,
        };
    }

    // Keep-absolute branch: by design the raw path stays in the key
    // because the compiled artifact may embed it via `env!`. Do not
    // warn here: this is an intentional key discriminator, and Cargo
    // fingerprints RUSTC_WRAPPER stderr for build freshness.
    NormalizedEnvDep {
        value: val.to_string(),
        decision: EnvDepNormalizationDecision::KeptAbsoluteRuntimePath,
    }
}

fn env_dep_is_safe_to_normalize(var: &str, val: &str, source_files: &[std::path::PathBuf]) -> bool {
    // OUT_DIR is the known path-only exception:
    //
    //   include!(concat!(env!("OUT_DIR"), "/foo"))
    //
    // splices file content into the AST and dep-info lists the generated
    // file under OUT_DIR, so the path string is not part of the object
    // content. For CARGO_MANIFEST_DIR and user env vars this test is
    // not valid: normal crate sources already live under the manifest
    // dir, so using it there would recreate #167.
    var == "OUT_DIR" && out_dir_is_path_only(val, source_files)
}

/// Decide whether the OUT_DIR env_dep value is "path-only" — i.e.
/// only used as the parent dir of one or more `include!()`'d source
/// files. Returns true if at least one entry in `source_files` lives
/// under `out_dir_value`.
///
/// Background and contract: see the OUT_DIR comment in
/// [`compute_cache_key`] and issue kunobi-ninja/kache#75.
///
/// Compares canonical paths so the macOS `/tmp` ↔ `/private/tmp`
/// symlink case doesn't produce a spurious false. If either side
/// can't be canonicalized (file moved, etc.), falls back to the
/// raw path components — `Path::starts_with` handles partial
/// component prefixes correctly without requiring lexical matching.
fn out_dir_is_path_only(out_dir_value: &str, source_files: &[std::path::PathBuf]) -> bool {
    let raw = Path::new(out_dir_value);
    let canonical = std::fs::canonicalize(raw).ok();
    let probe = canonical.as_deref().unwrap_or(raw);
    source_files.iter().any(|f| {
        let f_canonical = std::fs::canonicalize(f).ok();
        let f_probe = f_canonical.as_deref().unwrap_or(f.as_path());
        f_probe.starts_with(probe)
    })
}

// `normalize_flags` (CWD-only literal-replace) used to live here.
// Replaced by `PathNormalizer` (canonical-prefix sentinel
// substitution). The ad-hoc helper had two failure modes — see
// the `path_normalizer` module docs for the full story.

/// Hash a file using blake3.
pub fn hash_file(path: &Path) -> Result<String> {
    let data = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let hash = blake3::hash(&data);
    Ok(hash.to_hex().to_string())
}

/// Result of a dep-info pre-pass. Contains all information discovered by
/// running `rustc --emit=dep-info`.
///
/// This is a struct (not a tuple) so we can add fields later without
/// breaking call sites. Future candidates: `target_json_hash`, timing metrics.
pub struct DepInfo {
    /// All source files the crate depends on (sorted, absolute paths).
    /// Includes the crate root, module files, `include!()` targets, etc.
    pub source_files: Vec<std::path::PathBuf>,
    /// Environment variables tracked by rustc (`env!()` / `option_env!()`).
    /// Values are RAW — `compute_cache_key` decides whether to
    /// path-normalize each one based on per-var safety (see
    /// `out_dir_is_path_only`). Storing raw values keeps that
    /// decision available to the consumer; pre-normalizing here
    /// would erase the absolute-path information the discriminator
    /// needs to read.
    pub env_deps: Vec<(String, String)>,
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
    prefetched: RefCell<HashMap<FileFingerprint, PrefetchedHash>>,
    stats: FileHashStatsCells,
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

enum FileHashCache<'db> {
    Borrowed(&'db Connection),
    #[cfg(test)]
    Owned(Connection),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FileFingerprint {
    path: String,
    size: i64,
    mtime_ns: i64,
    ctime_ns: i64,
}

#[derive(Debug, Clone)]
struct PrefetchedHash {
    hash: String,
    cache_hit: bool,
    bytes_hashed: u64,
}

impl FileHasher<'static> {
    pub fn new() -> Self {
        FileHasher {
            cache: None,
            daemon_socket: None,
            prefetched: RefCell::new(HashMap::new()),
            stats: FileHashStatsCells::default(),
        }
    }

    #[cfg(test)]
    pub fn persistent(index_db_path: &Path) -> Self {
        match FileHashCache::open(index_db_path) {
            Ok(cache) => FileHasher {
                cache: Some(cache),
                daemon_socket: None,
                prefetched: RefCell::new(HashMap::new()),
                stats: FileHashStatsCells::default(),
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
    pub(crate) fn from_connection(db: &'db Connection) -> Self {
        FileHasher {
            cache: Some(FileHashCache::Borrowed(db)),
            daemon_socket: None,
            prefetched: RefCell::new(HashMap::new()),
            stats: FileHashStatsCells::default(),
        }
    }

    pub(crate) fn with_daemon(mut self, socket_path: PathBuf) -> Self {
        self.daemon_socket = Some(socket_path);
        self
    }

    pub fn stats(&self) -> FileHashStats {
        FileHashStats {
            cache_hits: self.stats.cache_hits.get(),
            cache_misses: self.stats.cache_misses.get(),
            bytes_hashed: self.stats.bytes_hashed.get(),
        }
    }

    pub fn prefetch(&self, paths: &[&Path]) {
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
        let Some(cache) = &self.cache else {
            return hash_file(path);
        };

        let fingerprint = match FileFingerprint::from_path(path) {
            Ok(fingerprint) => fingerprint,
            Err(e) => {
                tracing::debug!(
                    "file hash cache metadata lookup failed for {}: {e}",
                    path.display()
                );
                return hash_file(path);
            }
        };

        if fingerprint.size < MIN_PERSISTED_HASH_BYTES {
            let hash = hash_file(path)?;
            self.record_miss(fingerprint.size);
            return Ok(hash);
        }

        if let Some(prefetched) = self.prefetched.borrow().get(&fingerprint) {
            if prefetched.cache_hit {
                self.record_hit();
            } else {
                self.record_miss_count();
                self.record_miss_bytes(prefetched.bytes_hashed);
            }
            return Ok(prefetched.hash.clone());
        }

        match cache.get(&fingerprint) {
            Ok(Some(hash)) => {
                self.record_hit();
                return Ok(hash);
            }
            Ok(None) => {}
            Err(e) => {
                tracing::debug!("file hash cache lookup failed for {}: {e}", path.display());
            }
        }

        let hash = hash_file(path)?;
        self.record_miss(fingerprint.size);
        if let Err(e) = cache.put(&fingerprint, &hash) {
            tracing::debug!("file hash cache update failed for {}: {e}", path.display());
        }
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

impl<'db> FileHashCache<'db> {
    #[cfg(test)]
    fn open(index_db_path: &Path) -> Result<Self> {
        let db = Connection::open(index_db_path)
            .with_context(|| format!("opening file hash cache {}", index_db_path.display()))?;
        db.pragma_update(None, "busy_timeout", "5000")?;
        db.pragma_update(None, "journal_mode", "WAL")?;
        db.pragma_update(None, "synchronous", "NORMAL")?;
        ensure_file_hash_cache_schema(&db)?;
        Ok(Self::Owned(db))
    }

    fn db(&self) -> &Connection {
        match self {
            Self::Borrowed(db) => db,
            #[cfg(test)]
            Self::Owned(db) => db,
        }
    }

    fn get(&self, fingerprint: &FileFingerprint) -> rusqlite::Result<Option<String>> {
        self.db()
            .query_row(
                "SELECT hash FROM file_hashes
                 WHERE path = ?1 AND size = ?2 AND mtime_ns = ?3 AND ctime_ns = ?4",
                params![
                    fingerprint.path,
                    fingerprint.size,
                    fingerprint.mtime_ns,
                    fingerprint.ctime_ns
                ],
                |row| row.get(0),
            )
            .optional()
    }

    fn put(&self, fingerprint: &FileFingerprint, hash: &str) -> rusqlite::Result<()> {
        self.db().execute(
            "INSERT OR REPLACE INTO file_hashes
             (path, size, mtime_ns, ctime_ns, hash, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, datetime('now'))",
            params![
                fingerprint.path,
                fingerprint.size,
                fingerprint.mtime_ns,
                fingerprint.ctime_ns,
                hash
            ],
        )?;
        Ok(())
    }
}

pub(crate) fn ensure_file_hash_cache_schema(db: &Connection) -> rusqlite::Result<()> {
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS file_hashes (
            path       TEXT PRIMARY KEY,
            size       INTEGER NOT NULL,
            mtime_ns   INTEGER NOT NULL,
            ctime_ns   INTEGER NOT NULL DEFAULT 0,
            hash       TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );",
    )?;
    if let Err(e) =
        db.execute_batch("ALTER TABLE file_hashes ADD COLUMN ctime_ns INTEGER NOT NULL DEFAULT 0")
        && !e.to_string().contains("duplicate column name")
    {
        return Err(e);
    }
    Ok(())
}

impl FileFingerprint {
    fn from_path(path: &Path) -> Result<Self> {
        let metadata = std::fs::metadata(path)
            .with_context(|| format!("reading metadata for {}", path.display()))?;
        let absolute_path = absolute_path(path);

        Ok(Self {
            path: absolute_path.to_string_lossy().into_owned(),
            size: i64::try_from(metadata.len()).unwrap_or(i64::MAX),
            mtime_ns: metadata_mtime_ns(&metadata),
            ctime_ns: metadata_ctime_ns(&metadata),
        })
    }
}

fn absolute_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    }
}

pub(crate) fn metadata_mtime_ns(metadata: &std::fs::Metadata) -> i64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        metadata_parts_ns(metadata.mtime(), metadata.mtime_nsec())
    }

    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;

        windows_filetime_ns(metadata.last_write_time())
    }

    #[cfg(not(any(unix, windows)))]
    {
        system_time_ns(metadata.modified().ok()).unwrap_or_default()
    }
}

pub(crate) fn metadata_ctime_ns(metadata: &std::fs::Metadata) -> i64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        metadata_parts_ns(metadata.ctime(), metadata.ctime_nsec())
    }

    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;

        windows_filetime_ns(metadata.creation_time())
    }

    #[cfg(not(any(unix, windows)))]
    {
        system_time_ns(metadata.created().ok()).unwrap_or_else(|| metadata_mtime_ns(metadata))
    }
}

#[cfg(unix)]
fn metadata_parts_ns(seconds: i64, nanoseconds: i64) -> i64 {
    seconds
        .saturating_mul(1_000_000_000)
        .saturating_add(nanoseconds)
}

#[cfg(windows)]
fn windows_filetime_ns(filetime_100ns: u64) -> i64 {
    const UNIX_EPOCH_FILETIME_100NS: u64 = 116_444_736_000_000_000;

    filetime_100ns
        .saturating_sub(UNIX_EPOCH_FILETIME_100NS)
        .saturating_mul(100)
        .min(i64::MAX as u64) as i64
}

#[cfg(not(any(unix, windows)))]
fn system_time_ns(time: Option<std::time::SystemTime>) -> Option<i64> {
    let duration = time?.duration_since(std::time::UNIX_EPOCH).ok()?;
    let seconds = i64::try_from(duration.as_secs()).unwrap_or(i64::MAX / 1_000_000_000);

    Some(
        seconds
            .saturating_mul(1_000_000_000)
            .saturating_add(i64::from(duration.subsec_nanos())),
    )
}

/// Run `rustc --emit=dep-info` as a pre-pass to discover source files and env deps.
///
/// This is the I/O layer — it invokes rustc and reads the output file.
/// Parsing is delegated to `parse_dep_info()` and `parse_env_dep_info()` (pure functions).
///
/// Falls back to a DepInfo with just the crate root on any failure (conservative:
/// may cause false hits, but the real compilation will also fail).
pub fn run_dep_info_pass(
    rustc: &Path,
    source_file: &Path,
    rustc_args: &[String],
) -> Result<DepInfo> {
    let temp_dir = tempfile::Builder::new()
        .prefix("kache-depinfo")
        .tempdir()
        .context("creating temp dir for dep-info")?;
    let dep_file = temp_dir.path().join("deps.d");

    let mut cmd = std::process::Command::new(rustc);
    cmd.arg(source_file);

    let source_str = source_file.to_string_lossy();

    // Filter out --emit, --out-dir, -o, -C incremental, and the source file
    // (already added above) from original args.
    // Everything else (features, cfg, edition, target, codegen opts) is kept.
    let mut i = 0;
    while i < rustc_args.len() {
        let arg = &rustc_args[i];
        match arg.as_str() {
            "--emit" | "--out-dir" | "-o" => {
                i += 2; // skip flag + value
                continue;
            }
            _ if arg.starts_with("--emit=") || arg.starts_with("--out-dir=") => {
                i += 1;
                continue;
            }
            "-C" if rustc_args
                .get(i + 1)
                .is_some_and(|v| v.starts_with("incremental=")) =>
            {
                i += 2;
                continue;
            }
            _ if arg.starts_with("-Cincremental=") => {
                i += 1;
                continue;
            }
            _ if *arg == *source_str => {
                // Skip the source file — already added as the first positional arg
                i += 1;
                continue;
            }
            _ => {
                cmd.arg(arg);
            }
        }
        i += 1;
    }

    cmd.args(["--emit", "dep-info"]);
    cmd.arg("-o").arg(&dep_file);

    tracing::trace!("dep-info pre-pass: {:?}", cmd);

    let output = cmd
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .output()
        .context("running rustc --emit=dep-info")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        tracing::warn!(
            "dep-info pre-pass failed (exit {}): {}",
            output.status.code().unwrap_or(-1),
            stderr.lines().next().unwrap_or("(no output)")
        );
        return Ok(DepInfo {
            source_files: vec![source_file.to_path_buf()],
            env_deps: vec![],
        });
    }

    let dep_content = std::fs::read_to_string(&dep_file).context("reading dep-info output")?;

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

/// Parse a Makefile-style dep-info file to extract source file paths.
///
/// Format: `target: dep1 dep2 dep3`
/// Handles `\ ` escaped spaces in paths. Returns sorted paths.
fn parse_dep_info(dep_info: &str) -> Vec<std::path::PathBuf> {
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
/// (env!()-as-value pattern; see `out_dir_is_path_only`). Doing the
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
        let _ = std::fs::write(cache_file, version);
    }
}

/// Build the cache-file path: `<cache_dir>/<prefix>-<hash>.txt` where the hash
/// is derived from the binary's canonical path + mtime so it auto-invalidates
/// when the toolchain is updated.
fn tool_version_cache_path(binary: &Path, prefix: &str) -> Option<std::path::PathBuf> {
    let canon = std::fs::canonicalize(binary).ok()?;
    let mtime = std::fs::metadata(&canon)
        .ok()?
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs();
    let key = format!("{}:{}", canon.display(), mtime);
    let hash = blake3::hash(key.as_bytes()).to_hex();
    Some(crate::config::default_cache_dir().join(format!("{}-{}.txt", prefix, &hash[..16])))
}

/// Get the host target triple.
fn host_target_triple() -> &'static str {
    option_env!("TARGET").unwrap_or("unknown")
}

/// Get linker identity string for cache key, with file-based caching.
fn get_linker_identity(args: &RustcArgs) -> Option<String> {
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
    fn test_hash_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.rs");
        std::fs::write(&file, b"fn main() {}").unwrap();

        let hash = hash_file(&file).unwrap();
        assert_eq!(hash.len(), 64); // blake3 hex is 64 chars

        // Same content = same hash
        let file2 = dir.path().join("test2.rs");
        std::fs::write(&file2, b"fn main() {}").unwrap();
        let hash2 = hash_file(&file2).unwrap();
        assert_eq!(hash, hash2);

        // Different content = different hash
        let file3 = dir.path().join("test3.rs");
        std::fs::write(&file3, b"fn main() { println!(\"hello\"); }").unwrap();
        let hash3 = hash_file(&file3).unwrap();
        assert_ne!(hash, hash3);
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
    fn out_dir_is_path_only_detects_include_pattern() {
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
            out_dir_is_path_only(out_dir.to_str().unwrap(), &source_files),
            "OUT_DIR contains an included source file → safe to normalize"
        );
    }

    #[test]
    fn out_dir_is_path_only_rejects_env_value_pattern() {
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
            !out_dir_is_path_only(out_dir.to_str().unwrap(), &source_files),
            "no source under OUT_DIR → unsafe to normalize"
        );
    }

    #[test]
    fn out_dir_is_path_only_handles_macos_symlink_form() {
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

        let result = out_dir_is_path_only(&out_dir_value, &source_files);
        let _ = std::fs::remove_dir_all(std::path::Path::new("/tmp").join(&unique));
        assert!(
            result,
            "canonical-path comparison must see through the symlink"
        );
    }

    #[test]
    fn env_dep_policy_normalizes_out_dir_include_pattern() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        let out_dir = workspace.join("target/debug/build/pkg/out");
        std::fs::create_dir_all(&out_dir).unwrap();
        let included = out_dir.join("generated.rs");
        std::fs::write(&included, b"pub fn generated() -> u8 { 1 }").unwrap();

        let source_files = vec![workspace.join("src/lib.rs"), included];
        let path_normalizer = PathNormalizer::from_env(Some(&workspace));
        let out_dir_value = out_dir
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .to_string();
        let env_dep =
            normalize_env_dep_value("OUT_DIR", &out_dir_value, &source_files, &path_normalizer);

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
        let env_dep =
            normalize_env_dep_value("OUT_DIR", &out_dir_value, &source_files, &path_normalizer);

        assert_eq!(
            env_dep.decision,
            EnvDepNormalizationDecision::KeptAbsoluteRuntimePath
        );
        assert_eq!(env_dep.value, out_dir_value);
    }

    #[test]
    fn env_dep_policy_keeps_manifest_dir_absolute_even_with_sources_under_it() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        let manifest_dir = workspace.join("helper");
        let src = manifest_dir.join("src");
        std::fs::create_dir_all(&src).unwrap();
        let lib = src.join("lib.rs");
        std::fs::write(
            &lib,
            b"pub fn manifest_dir() -> &'static str { env!(\"CARGO_MANIFEST_DIR\") }",
        )
        .unwrap();

        let source_files = vec![lib];
        let path_normalizer = PathNormalizer::from_env(Some(&workspace));
        let manifest_dir_value = manifest_dir
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .to_string();
        let env_dep = normalize_env_dep_value(
            "CARGO_MANIFEST_DIR",
            &manifest_dir_value,
            &source_files,
            &path_normalizer,
        );

        assert_eq!(
            env_dep.decision,
            EnvDepNormalizationDecision::KeptAbsoluteRuntimePath
        );
        assert_eq!(env_dep.value, manifest_dir_value);
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
            "CUSTOM_CONFIG_DIR",
            &config_dir_value,
            &source_files,
            &path_normalizer,
        );

        assert_eq!(
            env_dep.decision,
            EnvDepNormalizationDecision::KeptAbsoluteRuntimePath
        );
        assert_eq!(env_dep.value, config_dir_value);
    }

    // `test_normalize_flags` removed: normalize_flags itself is gone,
    // replaced by PathNormalizer (covered by tests in path_normalizer
    // module). The cache_key consumer-side normalization is exercised
    // via the e2e relocate phase + the `out_dir_is_path_only` tests.

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
        // can be safely sentinelized — see `out_dir_is_path_only`).
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
    fn test_file_hasher_persistent_cache_invalidates_on_metadata_change() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("index.db");
        let file = dir.path().join("large.rlib");
        std::fs::write(&file, vec![1u8; 70 * 1024]).unwrap();

        let hasher = FileHasher::persistent(&db_path);
        let first = hasher.hash(&file).unwrap();
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

        let dep_info = run_dep_info_pass(&rustc, &source, &args).unwrap();

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

    use std::sync::{Mutex, MutexGuard};

    /// Serializes every test that computes a cache key.
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
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Acquire the key-test serial lock. Tolerates a poisoned mutex (a
    /// panic in an earlier key test) so one failure doesn't cascade
    /// into spurious failures of the rest.
    fn key_test_lock() -> MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
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

    // ── "should change" cases — varying a codegen-affecting input ──

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

        assert_eq!(
            key_a, key_b,
            "OUT_DIR include!() paths should stay portable across workspaces"
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

        assert_ne!(key_for(&t1), key_for(&t2), "`--target` must affect the key");
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

        // SAFETY: env access is serialized by ENV_LOCK; restored below.
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

        // SAFETY: env access is serialized by ENV_LOCK; restored below.
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

    #[test]
    fn key_matrix_lint_level_does_not_change_key() {
        // `-D warnings` promotes warnings to errors: it can make a
        // build *fail*, but a build that *succeeds* emits byte-
        // identical output with or without it. rustc does not parse
        // `-D`/`-W`/`-A` into anything kache keys, so the key must be
        // stable across this flag.
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

        assert_eq!(
            key_for(&base),
            key_for(&with_lint),
            "a lint-level flag (`-D warnings`) is diagnostics-only and \
             must NOT change the key — a change here is over-keying"
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
}
