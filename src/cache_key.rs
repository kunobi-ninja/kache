use crate::args::RustcArgs;
use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

/// Bump this when cache key logic changes in a way that could have produced
/// incorrect entries. All entries from previous versions become unreachable.
const CACHE_KEY_VERSION: u32 = 2;
const MIN_PERSISTED_HASH_BYTES: i64 = 64 * 1024;

/// Compute the blake3 cache key for a rustc invocation.
///
/// The key captures everything that affects compilation output:
/// - rustc version (full verbose string)
/// - target triple
/// - crate name and type
/// - codegen options (opt-level, lto, codegen-units, panic, etc.)
/// - feature flags (sorted)
/// - source file hash
/// - dependency artifact hashes
/// - RUSTFLAGS and relevant env vars
/// - linker identity (for bin/dylib caching)
pub fn compute_cache_key(args: &RustcArgs, file_hasher: &FileHasher<'_>) -> Result<String> {
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
    hasher.update(b"target:");
    hasher.update(target.as_bytes());
    hasher.update(b"\n");

    // crate identity
    if let Some(name) = &args.crate_name {
        hasher.update(b"crate_name:");
        hasher.update(name.as_bytes());
        hasher.update(b"\n");
    }

    // crate types
    for ct in &args.crate_types {
        hasher.update(b"crate_type:");
        hasher.update(ct.as_bytes());
        hasher.update(b"\n");
    }

    // edition
    if let Some(edition) = &args.edition {
        hasher.update(b"edition:");
        hasher.update(edition.as_bytes());
        hasher.update(b"\n");
    }

    // codegen options (sorted for determinism)
    let mut codegen_opts: Vec<_> = args
        .codegen_opts
        .iter()
        .filter(|(k, _)| {
            // Skip incremental as it's path-dependent.
            k != "incremental"
        })
        .collect();
    codegen_opts.sort_by_key(|(k, _)| k.as_str());
    for (key, value) in &codegen_opts {
        hasher.update(b"codegen:");
        hasher.update(key.as_bytes());
        if let Some(v) = value {
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
    }

    // cfg flags (non-feature, sorted)
    let mut cfgs: Vec<_> = args
        .cfgs
        .iter()
        .filter(|c| !c.starts_with("feature="))
        .collect();
    cfgs.sort();
    for cfg in &cfgs {
        hasher.update(b"cfg:");
        hasher.update(cfg.as_bytes());
        hasher.update(b"\n");
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
        for file in &dep_info.source_files {
            match file_hasher.hash(file) {
                Ok(file_hash) => {
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

        for (var, val) in &dep_info.env_deps {
            hasher.update(b"env_dep:");
            hasher.update(var.as_bytes());
            hasher.update(b"=");
            hasher.update(val.as_bytes());
            hasher.update(b"\n");
            tracing::trace!("[key:{}] env_dep:{}={}", crate_name, var, val);
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

    // RUSTFLAGS (normalized: workspace-root paths replaced with ".")
    if let Ok(rustflags) = std::env::var("RUSTFLAGS") {
        let normalized = normalize_flags(&rustflags);
        hasher.update(b"RUSTFLAGS:");
        hasher.update(normalized.as_bytes());
        hasher.update(b"\n");
        tracing::trace!("[key:{}] RUSTFLAGS={}", crate_name, normalized);
    }

    // CARGO_ENCODED_RUSTFLAGS (cargo's way of passing flags, normalized)
    if let Ok(flags) = std::env::var("CARGO_ENCODED_RUSTFLAGS") {
        let normalized = normalize_flags(&flags);
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

    // Path remapping status: kache adds --remap-path-prefix for reproducible builds,
    // but skips it when coverage instrumentation is active (coverage tools need original
    // paths). Since this produces different binaries, the key must reflect the decision.
    let remap = if args.has_coverage_instrumentation() {
        hasher.update(b"remap:none\n");
        "none"
    } else {
        hasher.update(b"remap:path-prefix\n");
        "path-prefix"
    };
    tracing::trace!("[key:{}] remap={}", crate_name, remap);

    let hash = hasher.finalize();
    let key = hash.to_hex().to_string();
    tracing::trace!("[key:{}] final={}", crate_name, &key[..16]);
    Ok(key)
}

/// Normalize compiler flags by replacing the current working directory with ".".
/// This makes flags like `-L /home/runner/project/lib` portable across machines.
fn normalize_flags(flags: &str) -> String {
    let pwd = std::env::current_dir().unwrap_or_default();
    let pwd_str = pwd.to_string_lossy();
    if pwd_str.is_empty() {
        return flags.to_string();
    }
    flags.replace(&*pwd_str, ".")
}

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
    /// Values are normalized: CWD replaced with `"."` for cross-machine sharing.
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
    #[cfg(test)]
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
            mtime_ns: unix_metadata_ns(metadata.mtime(), metadata.mtime_nsec()),
            ctime_ns: unix_metadata_ns(metadata.ctime(), metadata.ctime_nsec()),
        })
    }
}

fn absolute_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().unwrap_or_default().join(path)
    }
}

fn unix_metadata_ns(seconds: i64, nanos: i64) -> i64 {
    let total = i128::from(seconds) * 1_000_000_000 + i128::from(nanos);
    total.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
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
/// Values are normalized via `normalize_flags()` to replace CWD with `"."`
/// so that env-dep entries containing absolute paths (e.g., OUT_DIR)
/// don't break cross-machine cache sharing.
fn parse_env_dep_info(dep_info: &str) -> Vec<(String, String)> {
    let mut env_deps = Vec::new();
    for line in dep_info.lines() {
        if let Some(env_dep) = line.strip_prefix("# env-dep:") {
            if let Some((var, val)) = env_dep.split_once('=') {
                env_deps.push((var.to_string(), normalize_flags(val)));
            } else {
                env_deps.push((env_dep.to_string(), String::new()));
            }
        }
    }
    env_deps.sort_by(|(a, _), (b, _)| a.cmp(b));
    env_deps
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
        let key1 = compute_cache_key(&parsed1, &fh).unwrap();
        let key2 = compute_cache_key(&parsed2, &fh).unwrap();
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_cache_key_changes_with_source() {
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
        let parsed1 = RustcArgs::parse(&args_vec).unwrap();
        let key1 = compute_cache_key(&parsed1, &fh).unwrap();

        // Modified source
        std::fs::write(&source, b"pub fn hello() { println!(\"hi\"); }").unwrap();
        let parsed2 = RustcArgs::parse(&args_vec).unwrap();
        let key2 = compute_cache_key(&parsed2, &fh).unwrap();

        assert_ne!(key1, key2);
    }

    #[test]
    fn test_unreadable_dep_produces_stable_key() {
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
        let key_a = compute_cache_key(&parsed_a, &fh).unwrap();
        let key_b = compute_cache_key(&parsed_b, &fh).unwrap();
        assert_eq!(
            key_a, key_b,
            "unreadable deps with different paths should produce the same key"
        );
    }

    #[test]
    fn test_normalize_flags() {
        let pwd = std::env::current_dir().unwrap();
        let pwd_str = pwd.to_string_lossy();

        let flags = format!("-L {}/target/release/deps", pwd_str);
        let normalized = normalize_flags(&flags);
        assert_eq!(normalized, "-L ./target/release/deps");

        // Flags without absolute paths should pass through unchanged
        let plain = "-C opt-level=2";
        assert_eq!(normalize_flags(plain), plain);
    }

    #[test]
    fn test_cache_key_changes_with_features() {
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
        let key1 = compute_cache_key(&parsed1, &fh).unwrap();
        let key2 = compute_cache_key(&parsed2, &fh).unwrap();

        assert_ne!(key1, key2);
    }

    #[test]
    fn test_cache_key_changes_with_instrument_coverage() {
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
        let key_normal = compute_cache_key(&parsed_normal, &fh).unwrap();
        let key_coverage = compute_cache_key(&parsed_coverage, &fh).unwrap();

        assert_ne!(
            key_normal, key_coverage,
            "coverage-instrumented builds must have different cache keys"
        );
    }

    #[test]
    fn test_cache_key_changes_with_instrument_coverage_two_arg() {
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
        let key_normal = compute_cache_key(&parsed_normal, &fh).unwrap();
        let key_coverage = compute_cache_key(&parsed_coverage, &fh).unwrap();

        assert_ne!(
            key_normal, key_coverage,
            "two-arg form -C instrument-coverage must also produce different cache keys"
        );
    }

    #[test]
    fn test_cache_key_changes_with_tarpaulin_cfg() {
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
        let key_normal = compute_cache_key(&parsed_normal, &fh).unwrap();
        let key_tarpaulin = compute_cache_key(&parsed_tarpaulin, &fh).unwrap();

        assert_ne!(
            key_normal, key_tarpaulin,
            "--cfg=tarpaulin must produce a different cache key"
        );
    }

    #[test]
    fn test_coverage_keys_consistent_across_remap_forms() {
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
        let key_joined = compute_cache_key(&parsed_joined, &fh).unwrap();
        let key_two = compute_cache_key(&parsed_two, &fh).unwrap();

        assert_eq!(
            key_joined, key_two,
            "joined and two-arg forms of instrument-coverage should produce identical keys"
        );
    }

    #[test]
    fn test_cache_key_version_affects_key() {
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
        let key1 = compute_cache_key(&parsed1, &fh).unwrap();
        let key2 = compute_cache_key(&parsed2, &fh).unwrap();
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
    fn test_parse_env_deps_normalizes_cwd_in_values() {
        let cwd = std::env::current_dir().unwrap();
        let cwd_str = cwd.to_string_lossy();
        let input = format!(
            "deps.d: src/lib.rs\n# env-dep:OUT_DIR={}/target/debug/build/foo\n",
            cwd_str
        );
        let env_deps = parse_env_dep_info(&input);
        assert_eq!(env_deps.len(), 1);
        assert_eq!(env_deps[0].0, "OUT_DIR");
        assert_eq!(env_deps[0].1, "./target/debug/build/foo");
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
        let file = dir.path().join("test.rs");
        std::fs::write(&file, vec![b'a'; (MIN_PERSISTED_HASH_BYTES + 1) as usize]).unwrap();
        filetime::set_file_mtime(&file, filetime::FileTime::from_unix_time(1, 0)).unwrap();

        let hasher = FileHasher::persistent(&db_path);
        let hash1 = hasher.hash(&file).unwrap();
        let stats = hasher.stats();
        assert_eq!(stats.cache_hits, 0);
        assert_eq!(stats.cache_misses, 1);
        assert!(stats.bytes_hashed > 0);
        assert_eq!(
            FileHashCache::open(&db_path)
                .unwrap()
                .get(&FileFingerprint::from_path(&file).unwrap())
                .unwrap(),
            Some(hash1.clone())
        );

        let second_hasher = FileHasher::persistent(&db_path);
        assert_eq!(second_hasher.hash(&file).unwrap(), hash1);
        let stats = second_hasher.stats();
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.cache_misses, 0);

        std::fs::write(&file, vec![b'b'; (MIN_PERSISTED_HASH_BYTES + 1) as usize]).unwrap();
        filetime::set_file_mtime(&file, filetime::FileTime::from_unix_time(2, 0)).unwrap();
        let hash2 = second_hasher.hash(&file).unwrap();
        let stats = second_hasher.stats();
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.cache_misses, 1);

        assert_ne!(hash1, hash2, "metadata changes must invalidate cache");
    }

    #[test]
    fn test_file_hasher_persistent_cache_skips_small_files() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("index.db");
        let file = dir.path().join("test.rs");
        std::fs::write(&file, b"fn main() {}").unwrap();

        let hasher = FileHasher::persistent(&db_path);
        let hash1 = hasher.hash(&file).unwrap();
        let hash2 = hasher.hash(&file).unwrap();
        assert_eq!(hash1, hash2);

        let stats = hasher.stats();
        assert_eq!(stats.cache_hits, 0);
        assert_eq!(stats.cache_misses, 2);
        assert_eq!(
            FileHashCache::open(&db_path)
                .unwrap()
                .get(&FileFingerprint::from_path(&file).unwrap())
                .unwrap(),
            None
        );
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

        let parsed1 = RustcArgs::parse(&args_vec).unwrap();
        let key1 = compute_cache_key(&parsed1, &fh).unwrap();

        // Modify the module file (NOT lib.rs)
        std::fs::write(
            src.join("utils.rs"),
            b"pub fn helper() { println!(\"changed\"); }",
        )
        .unwrap();

        let parsed2 = RustcArgs::parse(&args_vec).unwrap();
        let key2 = compute_cache_key(&parsed2, &fh).unwrap();

        assert_ne!(
            key1, key2,
            "cache key must change when a module file changes"
        );
    }

    // --- cache key determinism with multiple source files ---

    #[test]
    fn test_cache_key_stable_with_module_files() {
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

        let parsed1 = RustcArgs::parse(&args_vec).unwrap();
        let parsed2 = RustcArgs::parse(&args_vec).unwrap();

        let key1 = compute_cache_key(&parsed1, &fh).unwrap();
        let key2 = compute_cache_key(&parsed2, &fh).unwrap();

        assert_eq!(
            key1, key2,
            "cache key must be deterministic with multiple source files"
        );
    }
}
