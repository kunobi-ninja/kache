use anyhow::{Context, Result};
use rusqlite::{Connection, Error as SqlError, ErrorCode, params};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::config::Config;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorePutResult {
    pub output_blobs: u32,
    pub duplicate_blobs: u32,
    pub new_blobs: u32,
}

/// An entry whose remote bytes and declared artifact hashes were verified in
/// the same pass that extracted them. Construction stays inside the remote
/// transport boundary; the store still re-checks metadata, paths and lengths,
/// but deliberately does not read every artifact a second time.
#[derive(Debug, Clone)]
pub(crate) struct VerifiedRestoredEntry {
    pub cache_key: String,
    pub meta: EntryMeta,
}

impl StorePutResult {
    pub fn is_full_dup(self) -> bool {
        self.output_blobs > 0 && self.duplicate_blobs == self.output_blobs
    }
}

/// Mark a blob read-only so accidental writes can't corrupt the shared,
/// content-addressed copy. Best-effort.
fn set_blob_readonly(blob: &Path) {
    let _ = set_blob_readonly_checked(blob);
}

/// Mark a blob read-only, reporting failure. The hardlink ingest path needs
/// the result: there the guard is a correctness requirement (the blob shares
/// an inode with the build's own output), not a courtesy.
fn set_blob_readonly_checked(blob: &Path) -> std::io::Result<()> {
    let meta = fs::metadata(blob)?;
    let mut perms = meta.permissions();
    perms.set_readonly(true);
    fs::set_permissions(blob, perms)
}

/// Is `name` exactly a content-blob filename: 64 lowercase hex chars (a
/// blake3 digest)? Used by the orphan sweep so it only ever unlinks files
/// that look like a blob — never an in-progress temp (`.{hash}.{pid}.{n}.tmp`)
/// or any stray file.
fn is_blob_hash_name(name: &str) -> bool {
    name.len() == 64
        && name
            .bytes()
            .all(|b| b.is_ascii_digit() || matches!(b, b'a'..=b'f'))
}

/// Best-effort unlink of a blob file (clears read-only first).
fn unlink_blob(blob: &Path) {
    if blob.exists() {
        if let Ok(meta) = fs::metadata(blob) {
            let mut perms = meta.permissions();
            perms.set_readonly(false);
            let _ = fs::set_permissions(blob, perms);
        }
        if fs::remove_file(blob).is_err() && blob.exists() {
            // Removal can fail transiently on Windows (sharing violation /
            // delete-pending). The surviving blob may share an inode with a
            // live build output (insert/restore hardlinks), so re-arm the
            // read-only guard rather than leaving a writable blob behind.
            set_blob_readonly(blob);
        }
    }
}

/// May a freshly-compiled output be HARDLINKED into the blob store when the
/// filesystem has no CoW reflink?
///
/// Mirrors the restore side's [`ArtifactKind::link_strategy`] reasoning:
/// immutable kinds (`.rlib` / `.rmeta` / `.o` / …) may share an inode with the
/// store blob because the build never mutates them in place — the same
/// contract a warm restore already imposes, where these outputs *are*
/// read-only hardlinks of blobs (and `prepare_output_paths` pre-cleans them
/// before a recompile). Mutable kinds (executables, dylibs — codesigning,
/// stripping) must stay independent so a post-build mutation can't reach the
/// content-addressed blob.
///
/// Three insert-side tightenings versus restore:
/// - `DepInfo` is excluded: the wrapper rewrites the build's `.d` in place
///   *after* `put` (`DepInfoMode::Expand`), and restore never hardlinks `.d`
///   either (it materializes via `write_restored`).
/// - restore classifies with full compile context; here only the stored
///   filename is available, so an extensionless name (a bin executable by
///   rustc's Unix convention) or anything carrying an executable mode bit is
///   excluded rather than defaulted to hardlink.
/// - on Windows a hardlink would propagate the blob's read-only attribute to
///   the build's own output (shared MFT record, #429), so insert hardlinks
///   only under the same `[cache] windows_hardlink` opt-in as restore.
fn hardlink_eligible(store_name: &str, executable: bool) -> bool {
    use crate::compiler::{ArtifactKind, classify_by_filename};
    if executable {
        return false;
    }
    #[cfg(windows)]
    if !crate::link::windows_hardlink_enabled() {
        return false;
    }
    match classify_by_filename(store_name) {
        ArtifactKind::DepInfo | ArtifactKind::Other("extensionless") => false,
        kind => kind.link_strategy() == crate::link::LinkStrategy::Hardlink,
    }
}

fn source_hardlink_allowed(
    allow_source_hardlinks: bool,
    store_name: &str,
    executable: bool,
) -> bool {
    allow_source_hardlinks && hardlink_eligible(store_name, executable)
}

/// How a new blob was staged into the store before publish. Counters are
/// recorded only when this call actually publishes (`atomic_write_and_replace`
/// returns `true`); a concurrent winner already accounted for their ingest,
/// and counting a discarded temp would over-claim zero-copy sharing.
#[derive(Clone, Copy)]
enum StoreIngest {
    Reflink,
    Hardlink,
    Copy,
}

/// Durably materialize `source` into the content-addressed store at `blob`,
/// unless the blob already exists: clone (or copy) to a unique temp, fsync,
/// atomic rename, mark read-only. Idempotent — when the blob is present this
/// is just a `stat`.
///
/// The temp is created by a CoW reflink first. Where the filesystem has no
/// copy-on-write (ext4 without reflink, tmpfs), `allow_hardlink` — decided
/// per file by [`hardlink_eligible`] — permits a hardlink fallback: the blob
/// then shares an inode with the build's own output, exactly the state a warm
/// restore produces for these kinds, and `set_blob_readonly` below applies to
/// both names. Only when neither zero-copy path is available (or allowed)
/// does the blob become a genuine second physical copy. On APFS / btrfs /
/// XFS-with-reflink the reflink wins and the blob shares physical blocks with
/// the build's output — storing costs ~no extra disk. Whichever path runs is
/// recorded **after a successful publish** (`record_store_reflinked` /
/// `record_store_hardlinked` / `record_store_copied`) so `kache report` can
/// account for disk honestly, mirroring the restore side in `link.rs`.
/// Counters are best-effort under concurrent put/remove: a phase-2
/// rematerialize after a reclaim may count the same logical ingest again.
/// Returns `Ok(true)` when this call published the blob (the caller may then
/// want to verify its digest), `Ok(false)` when it was already present.
fn materialize_blob(source: &Path, blob: &Path, allow_hardlink: bool) -> Result<bool> {
    if blob.is_file() {
        return Ok(false);
    }
    fs::create_dir_all(blob.parent().unwrap()).context("creating blob shard directory")?;
    let bytes = fs::metadata(source).map(|m| m.len()).unwrap_or(0);
    let ingest = std::cell::Cell::new(StoreIngest::Copy);
    let ro_failed = std::cell::Cell::new(false);

    // CoW reflink first; then a hardlink where the artifact kind allows sharing
    // an inode; only then a real copy. The hardlink is refused for a symlink
    // source: hashing followed the link, but `hard_link` would link the symlink
    // itself, and a blob must never be a pointer into mutable external state.
    //
    // Hardlink RO is applied in `after_fsync` (not in the write step): Windows
    // needs a writable handle to flush (#196). On RO failure we demote to a
    // full copy rather than publishing a writable shared inode.
    let published = match crate::atomic::atomic_write_and_replace_with(
        blob,
        true,
        |tmp| {
            if crate::link::try_reflink(source, tmp).is_ok() {
                ingest.set(StoreIngest::Reflink);
            } else if allow_hardlink
                && fs::symlink_metadata(source).is_ok_and(|m| m.file_type().is_file())
                && fs::hard_link(source, tmp).is_ok()
            {
                ingest.set(StoreIngest::Hardlink);
            } else {
                fs::copy(source, tmp)
                    .with_context(|| format!("copying {} to blob store", source.display()))?;
                ingest.set(StoreIngest::Copy);
            }
            Ok(())
        },
        |tmp| {
            if matches!(ingest.get(), StoreIngest::Hardlink)
                && let Err(e) = set_blob_readonly_checked(tmp)
            {
                tracing::debug!(
                    "read-only guard failed on hardlinked blob temp ({e}); \
                     falling back to copy: {}",
                    source.display()
                );
                ro_failed.set(true);
                anyhow::bail!("read-only guard failed on hardlinked blob temp");
            }
            Ok(())
        },
    ) {
        Ok(published) => published,
        Err(_e) if ro_failed.get() => {
            // Temp already cleaned by atomic_write_and_replace_with.
            return materialize_blob(source, blob, false);
        }
        Err(e) => {
            // Hardlink path may have marked the source RO via the shared temp
            // inode; undo that if we never published a blob that shares it.
            // On Windows, remove_file_robust may also have cleared RO on a
            // shared published blob — re-arm if the blob is present.
            if matches!(ingest.get(), StoreIngest::Hardlink) {
                if blob.is_file() {
                    set_blob_readonly(blob);
                } else {
                    restore_source_writable_if_unshared(source, blob);
                }
            }
            return Err(e);
        }
    };

    if published {
        match ingest.get() {
            StoreIngest::Reflink => crate::opcounts::record_store_reflinked(bytes),
            StoreIngest::Hardlink => crate::opcounts::record_store_hardlinked(bytes),
            StoreIngest::Copy => crate::opcounts::record_store_copied(bytes),
        }
        set_blob_readonly(blob);
    } else if matches!(ingest.get(), StoreIngest::Hardlink) {
        // Concurrent winner already published. Our temp was removed; if the
        // published blob does not share the source inode, clear the provisional
        // RO bit we applied before the race was lost. If it does share, re-arm
        // RO in case Windows cleanup cleared the shared attribute.
        if paths_share_inode(source, blob) {
            set_blob_readonly(blob);
        } else {
            restore_source_writable_if_unshared(source, blob);
        }
    }
    Ok(published)
}
/// Process-wide monotonic counter behind staging file names. Paired with the
/// pid it makes every in-flight staging path unique *by construction*: two
/// threads never draw the same nonce, and two live processes never share a
/// pid. That is what lets [`free_staging_path`] hand the ingest a path that
/// does not exist yet — see the warning there.
static STAGE_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// How many nonces to try before giving up on finding a free staging name.
/// Only stale leftovers from a crashed process whose pid has since been
/// recycled can occupy one, so a handful of attempts is already generous;
/// the bound keeps a pathological staging directory failing fast instead of
/// spinning forever.
const STAGING_NAME_ATTEMPTS: u32 = 16;

/// How long a staging snapshot must sit untouched before a sweep may reclaim
/// it. A snapshot belonging to a put running in another process is
/// indistinguishable from a crash leftover, and unlinking one fails that put
/// at publish time — so the grace has to outlast any plausible in-flight put.
/// Shared by the daemon's GC sweep and `doctor --repair` so neither can
/// undercut the other.
pub const STAGING_SWEEP_GRACE: Duration = Duration::from_secs(3600);

/// Pick a staging path that does not exist yet, skipping past any stale
/// leftover, and return it WITHOUT creating it.
///
/// Not creating it is the whole point: `clonefile(2)` (macOS) and `link(2)`
/// (everywhere) both fail with `EEXIST` when their destination already
/// exists, so reserving the name with a placeholder file would make both
/// zero-copy ingests fail and silently demote every put to a full byte copy.
/// Uniqueness comes from pid + [`STAGE_NONCE`] instead of from `create_new`,
/// which is stronger than a placeholder anyway: no live stager can draw this
/// name, so there is nothing to reserve it against.
///
/// Extracted from [`Store::stage_blob_from_source`] so the skip-and-retry
/// branch is unit-testable with injected names.
fn free_staging_path(mut name_for_nonce: impl FnMut(u64) -> PathBuf) -> std::io::Result<PathBuf> {
    for _ in 0..STAGING_NAME_ATTEMPTS {
        let candidate = name_for_nonce(STAGE_NONCE.fetch_add(1, Ordering::Relaxed));
        match fs::symlink_metadata(&candidate) {
            // Occupied by a crash leftover: leave it for the staging sweep
            // and take the next nonce.
            Ok(_) => continue,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(candidate),
            // Anything else (unreadable or missing staging directory) is a
            // real fault, not a collision: surface it instead of spinning.
            Err(e) => return Err(e),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "no free staging name",
    ))
}

/// Whether `a` and `b` name the same inode (hardlinked). Used after a lost
/// hardlink publish race to decide if the build output still shares the
/// store blob (keep RO) or is an independent file we marked RO by mistake
/// (restore writable).
fn paths_share_inode(a: &Path, b: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        match (fs::metadata(a), fs::metadata(b)) {
            (Ok(ma), Ok(mb)) => ma.dev() == mb.dev() && ma.ino() == mb.ino(),
            _ => false,
        }
    }
    #[cfg(windows)]
    {
        // `std::os::windows::fs::MetadataExt::{file_index,volume_serial_number}`
        // are still unstable (`windows_by_handle`). Use the same stable
        // `GetFileInformationByHandle` path as `events::get_file_identity`.
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
        };
        fn identity(path: &Path) -> Option<(u32, u32, u32)> {
            let file = fs::File::open(path).ok()?;
            let handle = file.as_raw_handle();
            let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
            let ok = unsafe { GetFileInformationByHandle(handle as _, &mut info) };
            if ok != 0 {
                Some((
                    info.dwVolumeSerialNumber,
                    info.nFileIndexHigh,
                    info.nFileIndexLow,
                ))
            } else {
                None
            }
        }
        match (identity(a), identity(b)) {
            (Some(ia), Some(ib)) => ia == ib,
            _ => false,
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (a, b);
        false
    }
}

fn metadata_is_readonly_regular(metadata: &fs::Metadata) -> bool {
    metadata.file_type().is_file() && metadata.permissions().readonly()
}

/// After a hardlink ingest that did not publish, clear read-only on `source`
/// unless it still shares an inode with the published `blob` (in which case
/// RO is the correct shared state, as on warm restore).
fn restore_source_writable_if_unshared(source: &Path, blob: &Path) {
    if paths_share_inode(source, blob) {
        return;
    }
    if let Ok(meta) = fs::metadata(source) {
        let mut perms = meta.permissions();
        if perms.readonly() {
            perms.set_readonly(false);
            let _ = fs::set_permissions(source, perms);
        }
    }
}

pub(crate) fn blob_path_in_store_dir(store_dir: &Path, hash: &str) -> PathBuf {
    // Defensive slice: a malformed hash (e.g. from a hand-edited or malicious
    // remote `meta.json`) must not panic. Hash shape is validated at the
    // remote trust boundary (`extract_entry_pack`), so a bad hash never gets
    // stored; this keeps the local path build panic-free even if one slips
    // through (#211).
    let prefix = hash.get(..2).unwrap_or(hash);
    store_dir.join("blobs").join(prefix).join(hash)
}

/// Outcome of a read-only local-hit probe (kunobi-ninja/kache#565).
///
/// `Fallback` covers every state the probe cannot serve without writing:
/// legacy layout needing migration, missing/short blobs (evict-and-miss),
/// unreadable meta, verify-restores mode, index read errors. The wrapper's
/// fully local path owns repair and eviction for all of those, so the daemon
/// answers "run the local path yourself" instead of mutating the store from a
/// read-only connection.
#[derive(Debug)]
pub(crate) enum ProbeOutcome {
    /// Committed, blob-complete entry: safe to restore from this meta.
    Hit(Box<EntryMeta>),
    /// No committed entry for this key (authoritative miss).
    Miss,
    /// Not servable read-only; the wrapper must run today's local path.
    Fallback(&'static str),
}

/// Read-only equivalent of the lookup half of [`Store::get`]: same
/// committed-row check, `meta.json` parse, legacy-layout detection, and
/// blob existence/size validation — but with every write side effect
/// (lazy migration, evict-and-miss, hit accounting) replaced by
/// [`ProbeOutcome::Fallback`]. Runs on a read-only connection so parallel
/// probes never contend on the daemon's store mutex (#565).
pub(crate) fn probe_entry_readonly(
    db: &Connection,
    store_dir: &Path,
    cache_key: &str,
) -> ProbeOutcome {
    let committed = db.query_row(
        "SELECT committed FROM entries WHERE cache_key = ?1",
        params![cache_key],
        |row| row.get::<_, bool>(0),
    );
    match committed {
        Ok(true) => {}
        Ok(false) => return ProbeOutcome::Miss,
        Err(SqlError::QueryReturnedNoRows) => return ProbeOutcome::Miss,
        Err(_) => return ProbeOutcome::Fallback("index read failed"),
    }

    // Content verification (KACHE_VERIFY_RESTORES) re-hashes blobs and evicts
    // on mismatch — a write path. Delegate to the wrapper so verify semantics
    // stay identical whether or not the daemon path is enabled.
    if !matches!(verify_restores_mode(), VerifyRestores::Off) {
        return ProbeOutcome::Fallback("verify_restores enabled");
    }

    let entry_dir = store_dir.join(cache_key);
    let meta_path = entry_dir.join("meta.json");
    let content = match fs::read_to_string(&meta_path) {
        Ok(content) => content,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return ProbeOutcome::Miss,
        Err(_) => return ProbeOutcome::Fallback("meta.json unreadable"),
    };
    let meta: EntryMeta = match serde_json::from_str(&content) {
        Ok(meta) => meta,
        Err(_) => return ProbeOutcome::Fallback("meta.json unparseable"),
    };

    // Poisoned (no files) entries and legacy in-entry-dir artifacts both need
    // store writes (evict / migrate) that `Store::get` performs lazily.
    if meta.files.is_empty() {
        return ProbeOutcome::Fallback("entry has no files");
    }
    if meta.files.iter().any(|f| entry_dir.join(&f.name).exists()) {
        return ProbeOutcome::Fallback("legacy entry needs migration");
    }

    for cached_file in &meta.files {
        let blob = blob_path_in_store_dir(store_dir, &cached_file.hash);
        match fs::metadata(&blob) {
            Ok(file_meta) if file_meta.is_file() && file_meta.len() == cached_file.size => {}
            _ => return ProbeOutcome::Fallback("blob missing or size mismatch"),
        }
    }

    ProbeOutcome::Hit(Box::new(meta))
}

/// Open the index database read-only for probe connections (#565). No schema
/// work, no WAL/synchronous pragma churn — `query_only` hard-refuses any
/// accidental write, and the busy timeout is half the daemon's 50 ms lookup
/// deadline so a contended probe still answers (`Fallback`) inside budget.
pub(crate) fn open_index_db_readonly(db_path: &Path) -> Result<Connection> {
    let db = Connection::open_with_flags(
        db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("opening index read-only {}", db_path.display()))?;
    db.pragma_update(None, "busy_timeout", "25")?;
    db.pragma_update(None, "query_only", "ON")?;
    Ok(db)
}

/// How long the background `tmutil addexclusion` child may run before being
/// killed. During an active Time Machine backup session, `addexclusion` on a
/// not-yet-excluded directory can block for minutes (kunobi-ninja/kache#588);
/// the exclusion is best-effort housekeeping, not worth a lingering child.
#[cfg(target_os = "macos")]
const TMUTIL_TIMEOUT: Duration = Duration::from_secs(30);

/// Exclude the cache dir from Spotlight indexing and Time Machine backups.
///
/// The Spotlight sentinel is a cheap synchronous file create. The Time Machine
/// exclusion shells out to `tmutil addexclusion`, which can hang for minutes
/// while a backup session is active — and this runs on the daemon's startup
/// path between socket bind and accept loop, so a synchronous call produced a
/// daemon that listened but never answered (kunobi-ninja/kache#588). Instead:
/// skip entirely when the exclusion xattr is already present (the warm case —
/// a syscall, no subprocess), else run `tmutil` on a detached thread with a
/// kill-after-[`TMUTIL_TIMEOUT`] so readiness never gates on backupd.
///
/// Returns the background thread's handle so tests can join it; production
/// callers drop it (the thread never outlives its bounded wait by more than
/// the child kill).
#[cfg(target_os = "macos")]
pub(crate) fn exclude_from_indexing(dir: &Path) -> Option<std::thread::JoinHandle<()>> {
    // Spotlight: .metadata_never_index sentinel
    let sentinel = dir.join(".metadata_never_index");
    if !sentinel.exists() {
        let _ = fs::File::create(&sentinel);
    }

    if backup_exclusion_xattr_present(dir) {
        return None;
    }
    let dir = dir.display().to_string();
    std::thread::Builder::new()
        .name("kache-tmutil".into())
        .spawn(move || run_tmutil_addexclusion_bounded(&dir))
        .ok()
}

/// Does `dir` already carry Time Machine's exclusion xattr
/// (`com.apple.metadata:com_apple_backup_excludeItem`)? A direct `getxattr`
/// syscall — unlike `tmutil isexcluded`, it cannot block on backupd. Errors
/// (including ENOATTR) read as "not excluded", which only costs a redundant
/// background `tmutil` run.
#[cfg(target_os = "macos")]
fn backup_exclusion_xattr_present(dir: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;
    let Ok(path) = std::ffi::CString::new(dir.as_os_str().as_bytes()) else {
        return false;
    };
    let name = c"com.apple.metadata:com_apple_backup_excludeItem";
    // Size-probe call (null buffer): >= 0 means the xattr exists.
    let len =
        unsafe { libc::getxattr(path.as_ptr(), name.as_ptr(), std::ptr::null_mut(), 0, 0, 0) };
    len >= 0
}

/// Run `tmutil addexclusion <dir>`, killing the child if it outlives
/// [`TMUTIL_TIMEOUT`] (it can wedge behind an active backup session, #588).
/// Best-effort throughout: every failure is debug-logged and swallowed.
#[cfg(target_os = "macos")]
fn run_tmutil_addexclusion_bounded(dir: &str) {
    let child = std::process::Command::new("tmutil")
        .args(["addexclusion", dir])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
    let Ok(mut child) = child else {
        return;
    };
    let started = std::time::Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) if started.elapsed() >= TMUTIL_TIMEOUT => {
                tracing::debug!(
                    "tmutil addexclusion still running after {}s (active backup?) — killing it; \
                     the exclusion will be retried on the next daemon start",
                    TMUTIL_TIMEOUT.as_secs()
                );
                let _ = child.kill();
                let _ = child.wait();
                return;
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(250)),
            Err(_) => return,
        }
    }
}

/// Metadata stored alongside cached artifacts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EntryMeta {
    pub cache_key: String,
    /// Cache-key recipe version that produced `cache_key`.
    ///
    /// Entries written before this field existed deserialize as `0` (unknown),
    /// so an explicit stale-schema sweep can reclaim them without making old
    /// stores unreadable during an ordinary upgrade.
    #[serde(default)]
    pub key_schema: u32,
    pub crate_name: String,
    pub crate_types: Vec<String>,
    pub files: Vec<CachedFile>,
    pub stdout: String,
    pub stderr: String,
    #[serde(default)]
    pub features: Vec<String>,
    #[serde(default)]
    pub target: String,
    #[serde(default)]
    pub profile: String,
    #[serde(default)]
    pub compile_time_ms: u64,
    /// Canonical rustc `--emit` kinds this entry actually contains, derived
    /// from the stored output files at put time (kunobi-ninja/kache#325). Lookup
    /// uses it to reject an entry that doesn't cover what the invocation's
    /// `--emit` requested. `#[serde(default)]` keeps pre-gate `meta.json` (no
    /// field) deserializable — an empty set means "unknown", so the lookup gate
    /// skips the check rather than mass-invalidating old entries.
    #[serde(default)]
    pub emit_kinds: Vec<String>,
}

impl EntryMeta {
    /// Whether this entry's recorded outputs cover every `--emit` kind the
    /// caller requested (kunobi-ninja/kache#325). Superset-tolerant: an entry
    /// that contains more kinds than requested still covers it (a lib
    /// `--emit=link` legitimately also produces `.rmeta`).
    ///
    /// Returns `true` when `emit_kinds` is empty — pre-gate entries recorded no
    /// coverage, so the check is skipped rather than mass-invalidating them.
    /// Requested kinds that map to no stored file class (e.g. an exotic emit
    /// kache doesn't model) are ignored so the gate never rejects on a kind it
    /// can't reason about.
    pub fn covers_requested_emit(&self, requested: &[String]) -> bool {
        if self.emit_kinds.is_empty() {
            return true;
        }
        requested
            .iter()
            .filter(|kind| crate::compiler::GATED_EMIT_KINDS.contains(&kind.as_str()))
            .all(|kind| self.emit_kinds.iter().any(|have| have == kind))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CachedFile {
    /// Filename relative to the cache entry directory
    pub name: String,
    /// Size in bytes
    pub size: u64,
    /// blake3 hash of file content
    pub hash: String,
    /// Whether the source file had the executable bit set at store time.
    /// Folded into the local content-dedup hash so two entries differing only
    /// by which file is executable can't collide (kunobi-ninja/kache#324).
    /// `#[serde(default)]` keeps old `meta.json` (no field) deserializable.
    #[serde(default)]
    pub executable: bool,
}

/// Statistics returned by GC operations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GcStats {
    pub entries_evicted: usize,
    pub bytes_freed: u64,
    pub blobs_removed: usize,
    pub duration_ms: u64,
    #[serde(default)]
    pub skipped: bool,
    /// Entries eviction selected but could not remove because they were
    /// accessed within [`EVICTION_IDLE_GRACE`] — a live build may be mid-restore
    /// on them (#326, #182) — or have a durable remote-upload intent whose
    /// local payload must survive until that intent is retired.
    ///
    /// Recorded so the CLI can explain "evicted 0" while the store is over its
    /// limit. Without it that reads as "GC is broken", which is what #509 was
    /// filed about and plausibly what turned #497 into a 113 GB bug report.
    #[serde(default)]
    pub entries_pinned: usize,
}

/// Registered blob bytes and blob rows an entry removal released — blobs
/// whose last reference went away, not the entry's logical size
/// (kunobi-ninja/kache#608). Denominated in `blobs` TABLE bytes, the same
/// unit as [`Store::physical_size`], so eviction's running budget stays
/// consistent with its trigger; the file unlink itself is best-effort
/// (Windows can defer it), so this is not a guarantee about the disk.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RemovalReclaim {
    pub(crate) freed_bytes: u64,
    pub(crate) blobs_unlinked: usize,
}

/// One pass of `remove_entry_guarded_with_hooks`: either a settled outcome,
/// or the instruction to run again because a republication replaced the
/// generation this pass was waiting on.
enum RemovalAttempt {
    Done(Option<RemovalReclaim>),
    Republished,
}

/// A shadow policy's would-evict set for one size-driven sweep
/// (kunobi-ninja/kache#594): the keys it would remove for the same byte
/// budget the live policy is sweeping toward.
struct ShadowSelection {
    policy: &'static str,
    victims: std::collections::HashSet<String>,
}

/// Post-eviction demand, split by whether the shadow policy agreed with the
/// live one about each evicted entry (kunobi-ninja/kache#594).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShadowDemandSplit {
    /// Evicted entries the shadow policy would also have evicted.
    pub agreed: usize,
    /// …of which were later asked for again.
    pub agreed_demanded: usize,
    /// Evicted entries the shadow policy would have KEPT.
    pub shadow_kept: usize,
    /// …of which were later asked for again — the shadow's saves, had it
    /// been live.
    pub shadow_kept_demanded: usize,
}

/// Statistics returned by [`Store::sweep_orphan_blobs`].
#[derive(Debug, Clone, Copy, Default)]
pub struct OrphanSweepStats {
    /// Blob-shaped files inspected on disk.
    pub scanned: usize,
    /// Orphan blobs (no `blobs` row) unlinked.
    pub removed: usize,
    /// Bytes reclaimed by the sweep.
    pub bytes_reclaimed: u64,
}

/// Difference between the derived SQLite blob index and committed entry
/// metadata, which is the store's authoritative reference graph (#819).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BlobIndexDrift {
    /// `entry_blobs` rows that are missing, stale, or have the wrong count.
    pub entry_mappings: usize,
    /// `blobs` rows that are missing, stale, or have the wrong size/refcount.
    pub blobs: usize,
}

impl BlobIndexDrift {
    pub fn total(self) -> usize {
        self.entry_mappings + self.blobs
    }
}

#[derive(Default)]
struct AuthoritativeBlobIndex {
    entry_mappings: std::collections::BTreeMap<(String, String), i64>,
    blobs: std::collections::BTreeMap<String, (i64, i64)>,
}

/// The local content-addressed store.
pub struct Store {
    config: Config,
    db: Connection,
}

/// How recently an entry must have been accessed for eviction to treat it as
/// "pinned by a live build" and skip it (kunobi-ninja/kache#326, #182).
///
/// A cache hit bumps `last_accessed` immediately before the wrapper hardlinks
/// the entry's blobs into the build, so any entry touched within this window may
/// be **mid-restore**. The window only has to outlast a single restore
/// (hardlink/reflink/read — milliseconds; once linked, the target file owns its
/// own inode and is immune to a later blob unlink), so 2 minutes is generous
/// headroom on a slow disk while staying far below any sensible cache lifetime.
pub(crate) const EVICTION_IDLE_GRACE: Duration = Duration::from_secs(120);

/// Entries backfilled with their rebuild cost per GC sweep
/// (kunobi-ninja/kache#594).
///
/// The backfill runs while the daemon holds the store mutex, so an unbounded
/// pass is the thing to avoid: measured on a real 52k-entry store, reading
/// every `meta.json` is ~6 s (~0.11 ms per entry). At this batch size one
/// sweep adds roughly a second — negligible against a sweep that already scans
/// the whole store — and a 50k-entry store converges in a handful of sweeps
/// rather than dozens.
const COMPILE_TIME_BACKFILL_BATCH: i64 = 10_000;

/// How long a post-eviction demand record is kept (kunobi-ninja/kache#594).
///
/// A tombstone earns its keep by answering "was this key wanted again soon
/// after we dropped it". Two weeks comfortably covers the branch-switch and
/// dependency-bump cycles that make a key go permanently dead, after which the
/// row is only consuming space. One row is ~100 bytes, so even a store
/// evicting tens of thousands of entries a fortnight stays in the low
/// megabytes.
pub(crate) const TOMBSTONE_RETENTION_DAYS: u64 = 14;

/// Lock guard for a cache key. Dropping it releases the lock.
pub struct KeyLock {
    path: PathBuf,
}

/// Result of claiming responsibility for a cache miss.
pub enum BuildClaim {
    /// This process owns the key and may compile it.
    Acquired(KeyLock),
    /// A peer committed the key after the caller's cache lookup.
    Committed(Box<EntryMeta>),
    /// Another process currently owns the key.
    Contended,
}

/// A fully-written key lock waiting to be published at its canonical path.
/// Keeping preparation separate from publication prevents contenders from
/// observing an empty lock file and mistaking an in-progress owner for stale.
struct PreparedKeyLock {
    path: PathBuf,
    temp: tempfile::NamedTempFile,
}

impl PreparedKeyLock {
    fn new(path: PathBuf) -> Result<Self> {
        let parent = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("key lock has no parent: {}", path.display()))?;
        fs::create_dir_all(parent)?;
        let mut temp = tempfile::NamedTempFile::new_in(parent)?;
        use std::io::Write;
        write!(temp, "{}", std::process::id())?;
        Ok(Self { path, temp })
    }

    /// Atomically publish without replacing an existing owner.
    fn publish(self) -> Result<Option<KeyLock>> {
        match self.temp.persist_noclobber(&self.path) {
            Ok(_) => Ok(Some(KeyLock { path: self.path })),
            Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => Ok(None),
            Err(e) => Err(e.error.into()),
        }
    }
}

/// Lock guard for store-wide GC. Dropping it releases the OS lock.
pub struct GcLock {
    file: Option<fs::File>,
}

impl Drop for KeyLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

impl Drop for GcLock {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            let _ = file.unlock();
            drop(file);
        }
    }
}

/// How aggressively a local cache hit re-hashes its blobs against their content
/// address before serving them, to catch silent on-disk corruption / bit rot /
/// a memo collision before it reaches the compiler as a wrong artifact
/// (kunobi-ninja/kache#332).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VerifyRestores {
    /// Never re-hash (size check only). Default — verifying every hit costs an
    /// extra full read per blob.
    Off,
    /// Re-hash a deterministic 1-in-N fraction of hits, for cheap always-on
    /// background coverage that amortizes the cost across many restores.
    Sampled,
    /// Re-hash every blob on every hit.
    Always,
}

/// One in this many hits is verified under [`VerifyRestores::Sampled`] (~6%).
const VERIFY_SAMPLE_RATE: u64 = 16;

/// Rolling counter that drives `Sampled` selection. Process-global so coverage
/// accrues over time (temporal sampling) rather than always (not) checking the
/// same entries.
static VERIFY_SAMPLE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Parse the restore-verification mode from `KACHE_VERIFY_RESTORES`. Read per
/// call (cheap, off the hot path) so tests can toggle it. Back-compatible: the
/// old boolean `1`/`true` maps to `Always`, unset/`0`/`false`/`off` to `Off`.
pub(crate) fn verify_restores_mode() -> VerifyRestores {
    parse_verify_restores(std::env::var("KACHE_VERIFY_RESTORES").ok().as_deref())
}

/// Pure mapping from the env value to a mode (split out so it can be unit-tested
/// without touching process env).
fn parse_verify_restores(value: Option<&str>) -> VerifyRestores {
    match value {
        Some(v) if v.eq_ignore_ascii_case("sampled") => VerifyRestores::Sampled,
        Some(v)
            if v.eq_ignore_ascii_case("always") || v == "1" || v.eq_ignore_ascii_case("true") =>
        {
            VerifyRestores::Always
        }
        _ => VerifyRestores::Off,
    }
}

/// Whether THIS hit should be content-verified, given the configured mode.
/// `Sampled` advances the rolling counter so ~1/[`VERIFY_SAMPLE_RATE`] of hits
/// verify.
fn should_verify_this_restore(mode: VerifyRestores) -> bool {
    match mode {
        VerifyRestores::Off => false,
        VerifyRestores::Always => true,
        VerifyRestores::Sampled => VERIFY_SAMPLE_COUNTER
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(VERIFY_SAMPLE_RATE),
    }
}

/// Optional cap (bytes) on the compiler diagnostics stored in an entry, from
/// `KACHE_MAX_DIAGNOSTICS_BYTES`. `None` (default) stores them in full — a cache
/// hit replays exactly what the compile emitted, so warning gates behave
/// identically on a hit vs a miss (kunobi-ninja/kache#336). The cap is an opt-in
/// safety valve against a pathological stream (e.g. a noisy proc-macro) bloating
/// `meta.json`, accepting reduced fidelity only above the chosen size.
fn max_diagnostics_bytes() -> Option<usize> {
    std::env::var("KACHE_MAX_DIAGNOSTICS_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
}

/// Truncate diagnostics to `max` bytes (at a UTF-8 char boundary) with a marker
/// noting how much was dropped. Returns the input unchanged when under the cap
/// or uncapped (kunobi-ninja/kache#336).
fn cap_diagnostics(s: &str, max: Option<usize>) -> String {
    match max {
        Some(limit) if s.len() > limit => {
            let mut end = limit;
            while end > 0 && !s.is_char_boundary(end) {
                end -= 1;
            }
            let omitted = s.len() - end;
            format!(
                "{}\n[kache: diagnostics truncated, {omitted} bytes omitted (#336)]\n",
                &s[..end]
            )
        }
        _ => s.to_string(),
    }
}

fn is_executable(metadata: &fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        false
    }
}

/// Whether an empty file of this kind is a legitimate compiler output rather
/// than the signature of a truncated / failed write.
///
/// `put` otherwise refuses zero-byte outputs, because for every linkable kind an
/// empty artifact means the compiler died mid-write (or the disk filled) and
/// caching it would hand that corruption to every later build (#8).
///
/// An `.rmeta` from a unit with no metadata to emit is the exception:
/// `cargo check` / `cargo clippy --all-targets` compile test and bin units with
/// `--emit=metadata`, and rustc creates the `.rmeta` cargo expects but leaves it
/// zero bytes. Refusing it made those units permanently uncacheable, and because
/// the guard aborts the whole `put`, it also discarded the entry's non-empty
/// siblings (kunobi-ninja/kache#624).
///
/// The crate-type test is what keeps this from re-opening the hole the guard
/// exists to close: a `lib` / `rlib` / `dylib` / `proc-macro` unit *does* have
/// metadata, so an empty `.rmeta` there is a truncated write and stays refused.
/// A `--test` unit passes no `--crate-type`, hence the empty-slice case.
fn zero_byte_is_valid_output(store_name: &str, crate_types: &[String]) -> bool {
    matches!(
        crate::compiler::classify_by_filename(store_name),
        crate::compiler::ArtifactKind::Metadata
    ) && !crate_types
        .iter()
        .any(|ct| crate::compiler::rustc::crate_type_produces_metadata(ct))
}

/// Length-prefix a field before folding it into a hasher, so adjacent fields
/// cannot be transposed without changing the digest.
fn fold_field(h: &mut blake3::Hasher, bytes: &[u8]) {
    h.update(&(bytes.len() as u64).to_le_bytes());
    h.update(bytes);
}

/// Derive the deduplicated, sorted set of canonical rustc `--emit` kinds an
/// entry covers, from its stored output filenames (kunobi-ninja/kache#325).
/// Files that map to no emit kind (debug sidecars, etc.) are ignored.
fn emit_kinds_for_files(files: &[CachedFile]) -> Vec<String> {
    let mut kinds: Vec<String> = files
        .iter()
        .filter_map(|f| crate::compiler::emit_kind_for_filename(&f.name))
        .map(str::to_string)
        .collect();
    kinds.sort();
    kinds.dedup();
    kinds
}

/// Compute a LOCAL content-dedup hash for an entry (the `content_hash` column,
/// used only by `evict_duplicate_entries`; never crosses the remote wire).
///
/// Folds a deterministically sorted list of `(relative name, content hash, size,
/// exec-bit)`, each field length-prefixed. The previous version folded only the
/// bare blob hashes and truncated to 16 hex, so two distinct entries differing
/// only by a name↔hash transposition or by which file carried the exec-bit could
/// collide — and dedup-by-content would then keep the wrong survivor
/// (kunobi-ninja/kache#324). Returns the full blake3 hex.
fn compute_content_hash(files: &[CachedFile]) -> String {
    let mut sorted: Vec<&CachedFile> = files.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.hash.cmp(&b.hash)));
    let mut h = blake3::Hasher::new();
    for f in &sorted {
        fold_field(&mut h, f.name.as_bytes());
        fold_field(&mut h, f.hash.as_bytes());
        fold_field(&mut h, &f.size.to_le_bytes());
        fold_field(&mut h, &[u8::from(f.executable)]);
    }
    h.finalize().to_hex().to_string()
}

const STORE_OPEN_MAX_ATTEMPTS: u32 = 6;
const STORE_OPEN_RETRY_DELAYS_MS: [u64; 5] = [25, 50, 100, 200, 250];

fn sqlite_open_retry_delay(attempt: u32) -> Duration {
    let idx = attempt.saturating_sub(1) as usize;
    Duration::from_millis(*STORE_OPEN_RETRY_DELAYS_MS.get(idx).unwrap_or(&250))
}

fn is_retryable_sqlite_open_error(err: &SqlError) -> bool {
    match err {
        SqlError::SqliteFailure(code, _) => matches!(
            code.code,
            ErrorCode::CannotOpen
                | ErrorCode::DatabaseBusy
                | ErrorCode::DatabaseLocked
                | ErrorCode::SystemIoFailure
        ),
        _ => false,
    }
}

fn initialize_db(db: &Connection) -> rusqlite::Result<()> {
    db.pragma_update(None, "journal_mode", "WAL")?;
    db.pragma_update(None, "synchronous", "NORMAL")?;
    // Let concurrent writers retry for up to 5 s instead of failing immediately
    // with SQLITE_BUSY -- critical when 300+ wrapper processes hit the DB in parallel.
    db.pragma_update(None, "busy_timeout", "5000")?;

    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS entries (
            cache_key TEXT PRIMARY KEY,
            crate_name TEXT NOT NULL,
            size INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            last_accessed TEXT NOT NULL DEFAULT (datetime('now')),
            hit_count INTEGER NOT NULL DEFAULT 0,
            committed INTEGER NOT NULL DEFAULT 0
        );",
    )?;

    // Migrations (idempotent -- ignore "duplicate column" errors)
    let _ = db.execute_batch("ALTER TABLE entries ADD COLUMN crate_type TEXT NOT NULL DEFAULT ''");
    let _ = db.execute_batch("ALTER TABLE entries ADD COLUMN profile TEXT NOT NULL DEFAULT ''");
    let _ =
        db.execute_batch("ALTER TABLE entries ADD COLUMN num_features INTEGER NOT NULL DEFAULT 0");
    let _ = db.execute_batch("ALTER TABLE entries ADD COLUMN content_hash TEXT");
    // What a miss on this entry would cost to rebuild (kunobi-ninja/kache#594).
    // Recorded in every entry's meta.json since long before this column, so
    // pre-existing rows are backfilled by `backfill_compile_times` rather than
    // being stuck at the 0 default. Eviction cannot see meta.json, so without
    // this column the cache has no way to weigh what it is about to destroy.
    let _ = db
        .execute_batch("ALTER TABLE entries ADD COLUMN compile_time_ms INTEGER NOT NULL DEFAULT 0");
    // Cache-key recipe version for targeted reclamation after a key bump
    // (kunobi-ninja/kache#750). Legacy rows are `0` = unknown and remain usable
    // until the user explicitly requests a stale-schema sweep.
    let _ =
        db.execute_batch("ALTER TABLE entries ADD COLUMN key_schema INTEGER NOT NULL DEFAULT 0");

    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS blobs (
            hash     TEXT PRIMARY KEY,
            size     INTEGER NOT NULL,
            refcount INTEGER NOT NULL DEFAULT 1
        );",
    )?;

    // Which blobs each entry references (kunobi-ninja/kache#608). The mapping
    // otherwise lives only in per-entry meta.json files, which eviction cannot
    // afford to read for every candidate on every sweep. `refs` counts
    // references per *file*, not per unique hash (an entry listing the same
    // hash twice holds two of the blob's refcounts — see `adopt`/`remove`),
    // so "this entry holds the blob's last references" is `refs = refcount`.
    // Equality deliberately fails closed if refcounts and mappings ever drift
    // (e.g. the same-key republication races of #670): a drifted blob is
    // simply not counted reclaimable, never over-promised.
    // Pre-existing rows are backfilled by `backfill_entry_blobs` from the GC
    // sweep; ranking treats a not-yet-backfilled entry as it did before #608.
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS entry_blobs (
            cache_key TEXT NOT NULL,
            hash      TEXT NOT NULL,
            refs      INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (cache_key, hash)
        );
        CREATE INDEX IF NOT EXISTS idx_entry_blobs_hash ON entry_blobs(hash);",
    )?;

    // Post-eviction demand tracking (kunobi-ninja/kache#594).
    //
    // The question a cache eviction policy must answer is "will this key be
    // requested again", and a snapshot of the live store cannot answer it: the
    // entries it evicted are exactly the ones missing from it. So record what
    // was evicted, with the features the decision was made on, and mark the
    // row if a later lookup asks for that key. `demanded_at` NULL means "not
    // (yet) asked for since eviction".
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS eviction_tombstones (
            cache_key       TEXT PRIMARY KEY,
            evicted_at      TEXT NOT NULL DEFAULT (datetime('now')),
            policy          TEXT NOT NULL DEFAULT '',
            size            INTEGER NOT NULL DEFAULT 0,
            hit_count       INTEGER NOT NULL DEFAULT 0,
            idle_hours      REAL NOT NULL DEFAULT 0,
            compile_time_ms INTEGER NOT NULL DEFAULT 0,
            demanded_at     TEXT
        );",
    )?;
    // Shadow-policy verdict per eviction (kunobi-ninja/kache#594): which
    // candidate policy shadowed the sweep, and whether it agreed this entry
    // should go. NULL on rows from sweeps without a shadow. Idempotent
    // migrations, same pattern as the entries columns above.
    let _ = db.execute_batch("ALTER TABLE eviction_tombstones ADD COLUMN shadow_policy TEXT");
    let _ =
        db.execute_batch("ALTER TABLE eviction_tombstones ADD COLUMN shadow_would_evict INTEGER");

    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS incremental_dirs (
            path      TEXT PRIMARY KEY,
            last_seen TEXT NOT NULL DEFAULT (datetime('now'))
        );",
    )?;

    crate::cache_key::ensure_file_hash_cache_schema(db)?;

    Ok(())
}

/// Replace `cache_key`'s rows in `entry_blobs` with one row per unique hash
/// in `files`, `refs` counting per-file references (kunobi-ninja/kache#608).
/// Must run inside the caller's registration transaction so the mapping
/// commits atomically with the entry row and the blob refcounts it mirrors.
fn record_entry_blobs(
    conn: &rusqlite::Connection,
    cache_key: &str,
    files: &[CachedFile],
) -> rusqlite::Result<()> {
    conn.execute(
        "DELETE FROM entry_blobs WHERE cache_key = ?1",
        params![cache_key],
    )?;
    for file in files {
        conn.execute(
            "INSERT INTO entry_blobs (cache_key, hash, refs) VALUES (?1, ?2, 1)
             ON CONFLICT(cache_key, hash) DO UPDATE SET refs = refs + 1",
            params![cache_key, file.hash],
        )?;
    }
    Ok(())
}

pub(crate) fn open_index_db(db_path: &Path) -> Result<Connection> {
    open_index_db_reporting_recovery(db_path).map(|(db, _)| db)
}

/// Like [`open_index_db`], but also reports whether the index had to be
/// recreated from scratch.
///
/// [`Store::open`] needs to know: a freshly quarantined index has no rows, while
/// the blobs and every entry's `meta.json` are still on disk, so it can rebuild
/// the rows instead of silently presenting a cold cache (#415). Callers that
/// only need a connection use the wrapper above.
pub(crate) fn open_index_db_reporting_recovery(db_path: &Path) -> Result<(Connection, bool)> {
    match try_open_index_db(db_path) {
        Ok(db) => Ok((db, false)),
        // The index is a derived, rebuildable cache — the blobs plus each
        // entry's meta.json are the source of truth — so a corrupt index must
        // not brick every command (the #412 report: macOS + Linux writing one
        // WAL index on a shared home dir left it SQLITE_CORRUPT, and every
        // command then hard-failed). Recover under a lock (#415).
        Err(err) if is_corruption_error(&err) => recover_corrupt_index(db_path, &err),
        Err(err) => Err(err.into()),
    }
}

/// Recover a corrupt index: quarantine the unusable files and recreate a fresh,
/// empty index so stats/report/compiles degrade gracefully instead of bricking
/// every command.
///
/// Returns `(connection, recovered)`. `recovered == true` tells [`Store::open`]
/// the row set was lost and should be rebuilt from the entry `meta.json` files
/// still on disk, so the user does not silently drop to a cold cache (#415).
///
/// Serialized by a cross-process lock so two processes that both observed the
/// corrupt DB cannot clobber each other — without it, one could heal and write
/// entries while the other then renames that healthy DB aside (re-emptying it
/// and orphaning the just-written blobs). Under the lock we re-check first: a
/// peer may have already healed it, in which case we simply open the fresh DB
/// and report `false`, because that peer owns the rebuild.
fn recover_corrupt_index(db_path: &Path, err: &SqlError) -> Result<(Connection, bool)> {
    // OS file lock, released automatically when the handle drops / the process
    // exits. Best-effort: on any lock failure we proceed unlocked, still guarded
    // by the re-check below.
    let _lock = acquire_index_recovery_lock(db_path);

    // Re-check under the lock: a peer may have healed it while we waited.
    match try_open_index_db(db_path) {
        Ok(db) => return Ok((db, false)),
        Err(e) if is_corruption_error(&e) => {} // still corrupt: we heal it
        Err(e) => return Err(e.into()),
    }

    let quarantined = quarantine_corrupt_index(db_path)
        .with_context(|| format!("quarantining corrupt index {}", db_path.display()))?;
    tracing::warn!(
        path = %db_path.display(),
        quarantined = %quarantined.display(),
        "index database is corrupt ({err}); quarantined it and recreated an empty index. \
         Rebuilding the entry rows from the store; run `kache doctor` to inspect."
    );
    let db = try_open_index_db(db_path)
        .map_err(anyhow::Error::from)
        .with_context(|| {
            format!(
                "recreating index database after quarantine {}",
                db_path.display()
            )
        })?;
    Ok((db, true))
}

/// Best-effort blocking lock that serializes index recovery across processes.
/// Returns the locked file handle (the lock lives as long as it is held); on any
/// error returns `None` and the caller proceeds unlocked.
fn acquire_index_recovery_lock(db_path: &Path) -> Option<fs::File> {
    let lock_path = index_sidecar_path(db_path, ".recovery-lock");
    let file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .ok()?;
    file.lock().ok()?;
    Some(file)
}

/// Open the index DB, retrying only *transient* open failures. Returns the raw
/// [`SqlError`] so [`open_index_db`] can distinguish corruption (which it
/// self-heals) from a genuine failure.
fn try_open_index_db(db_path: &Path) -> std::result::Result<Connection, SqlError> {
    let mut last_error: Option<SqlError> = None;

    for attempt in 1..=STORE_OPEN_MAX_ATTEMPTS {
        match Connection::open(db_path).and_then(|db| {
            initialize_db(&db)?;
            Ok(db)
        }) {
            Ok(db) => return Ok(db),
            Err(err)
                if attempt < STORE_OPEN_MAX_ATTEMPTS && is_retryable_sqlite_open_error(&err) =>
            {
                let delay = sqlite_open_retry_delay(attempt);
                tracing::debug!(
                    path = %db_path.display(),
                    attempt,
                    ?delay,
                    "retrying transient SQLite open failure: {err}"
                );
                last_error = Some(err);
                std::thread::sleep(delay);
            }
            Err(err) => {
                last_error = Some(err);
                break;
            }
        }
    }

    Err(last_error.expect("try_open_index_db must record an error before returning"))
}

/// Whether a SQLite error means the database file itself is unusable
/// (`SQLITE_CORRUPT` / `SQLITE_NOTADB`) — the rebuildable-index case
/// [`open_index_db`] self-heals, as opposed to a transient open failure.
fn is_corruption_error(err: &SqlError) -> bool {
    matches!(
        err,
        SqlError::SqliteFailure(code, _)
            if matches!(code.code, ErrorCode::DatabaseCorrupt | ErrorCode::NotADatabase)
    )
}

/// Move a corrupt index and its WAL/SHM sidecars aside (to
/// `<name>.corrupt-<millis>-<pid>`) so a fresh index can be created in place.
/// The corrupt files are kept, not deleted, for forensics. The pid suffix keeps
/// concurrent self-healers from colliding on the same quarantine name.
fn quarantine_corrupt_index(db_path: &Path) -> Result<PathBuf> {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let file_name = db_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("index.db");
    let quarantine = db_path.with_file_name(format!(
        "{file_name}.corrupt-{millis}-{}",
        std::process::id()
    ));
    fs::rename(db_path, &quarantine)
        .with_context(|| format!("renaming corrupt index {} aside", db_path.display()))?;
    // Best-effort: move the WAL/SHM sidecars too so the fresh DB starts clean.
    for ext in ["-wal", "-shm"] {
        let from = index_sidecar_path(db_path, ext);
        if from.exists() {
            let _ = fs::rename(&from, index_sidecar_path(&quarantine, ext));
        }
    }
    Ok(quarantine)
}

/// The path of a SQLite sidecar (`-wal` / `-shm`): the suffix is appended to the
/// whole DB filename, not its extension.
fn index_sidecar_path(db_path: &Path, suffix: &str) -> PathBuf {
    let mut name = db_path
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_default();
    name.push(suffix);
    db_path.with_file_name(name)
}

/// Outcome of [`Store::rebuild_index_from_store`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RebuildStats {
    /// Entries whose rows were reconstructed from `meta.json`.
    pub entries_rebuilt: usize,
    /// Entry dirs that could not be registered: unreadable or unparseable
    /// `meta.json`, a missing or wrong-sized blob, or a row that already existed.
    pub entries_skipped: usize,
    /// Blob references registered (one per `meta.files` element, not per
    /// unique hash).
    pub blobs_registered: usize,
}

/// One prior build of a crate on this machine, from the local store's index
/// (kunobi-ninja/kache#617). Replaces a bare `(key, crate, dir)` tuple so the
/// planner can rank by rebuild cost and size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrateHistoryEntry {
    pub cache_key: String,
    pub crate_name: String,
    pub entry_dir: PathBuf,
    /// `None` when the index has no value: these columns default to 0 for rows
    /// predating their migrations, and a 0 read as a real measurement would
    /// rank an un-backfilled entry as worthless.
    pub compile_time_ms: Option<u64>,
    pub size_bytes: Option<u64>,
}

/// A non-positive SQLite column value means "not recorded", not "zero".
fn positive_or_none(value: i64) -> Option<u64> {
    (value > 0).then_some(value as u64)
}

impl Store {
    pub fn open(config: &Config) -> Result<Self> {
        fs::create_dir_all(&config.cache_dir)
            .with_context(|| format!("creating cache directory {}", config.cache_dir.display()))?;
        let store_dir = config.store_dir();
        fs::create_dir_all(&store_dir)
            .with_context(|| format!("creating store directory {}", store_dir.display()))?;

        let db_path = config.index_db_path();
        let (db, recovered) = open_index_db_reporting_recovery(&db_path)
            .with_context(|| format!("opening index database {}", db_path.display()))?;

        let store = Store {
            config: config.clone(),
            db,
        };

        // A quarantined index comes back empty, but the blobs and every entry's
        // meta.json are still on disk, so the row set is reconstructible. Without
        // this the user silently drops from a warm cache to a cold one and
        // recompiles (or re-downloads) everything the old index knew about (#415).
        //
        // Best-effort: a rebuild failure must not turn a recovered-but-empty
        // index back into a hard open failure, which is the exact brick-every-
        // command behaviour recovery exists to prevent.
        if recovered {
            match store.rebuild_index_from_store() {
                Ok(stats) if stats.entries_rebuilt > 0 || stats.entries_skipped > 0 => {
                    tracing::warn!(
                        rebuilt = stats.entries_rebuilt,
                        skipped = stats.entries_skipped,
                        blobs = stats.blobs_registered,
                        "rebuilt the index from the store after corruption"
                    );
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(
                    "could not rebuild the index from the store after corruption: {e:#}"
                ),
            }
        }

        Ok(store)
    }

    pub fn file_hasher(&self) -> crate::cache_key::FileHasher<'_> {
        crate::cache_key::FileHasher::from_connection(&self.db)
    }

    pub fn file_hasher_with_daemon(
        &self,
        socket_path: PathBuf,
    ) -> crate::cache_key::FileHasher<'_> {
        self.file_hasher().with_daemon(socket_path)
    }

    /// Persistent-cache lookup for one file's content hash — DB read only, no
    /// blake3. Lets the daemon's `HashFiles` path release the store lock before
    /// the expensive file read (#281). See [`crate::cache_key::FileHashLookup`].
    pub fn file_hash_lookup(&self, path: &Path) -> crate::cache_key::FileHashLookup {
        self.file_hasher().lookup_cached(path)
    }

    /// Record a freshly-computed file content hash (the miss arm of
    /// [`Self::file_hash_lookup`]); best-effort.
    pub fn file_hash_record(&self, fingerprint: &crate::cache_key::FileFingerprint, hash: &str) {
        self.file_hasher().record_cached(fingerprint, hash);
    }

    /// Associate an already-known content hash with the exact fingerprint the
    /// caller observed it at, avoiding a redundant read when the file becomes a
    /// compiler input (kunobi-ninja/kache#540). Unlike
    /// [`Self::record_known_file_hash`] this never re-stats the path, so a file
    /// overwritten between observation and this call cannot inherit the old
    /// content's hash.
    pub fn record_verified_file_hash(
        &self,
        fingerprint: &crate::cache_key::FileFingerprint,
        hash: &str,
    ) {
        self.file_hasher().record_verified(fingerprint, hash);
    }

    /// Associate a stable file with its already-known content hash, avoiding a
    /// redundant read when it becomes a compiler input. Call this only after
    /// every store-side operation that may change the file's fingerprint and
    /// while the compiler-owned output is stable.
    pub fn record_known_file_hash(&self, path: &Path, hash: &str) {
        if let crate::cache_key::FileHashLookup::NeedsHash(fingerprint) =
            self.file_hasher().lookup_cached(path)
        {
            self.file_hasher().record_cached(&fingerprint, hash);
        }
    }

    /// Check if a committed entry exists for this cache key.
    pub fn contains(&self, cache_key: &str) -> bool {
        let entry_dir = self.entry_dir(cache_key);
        let meta_path = entry_dir.join("meta.json");

        if !meta_path.exists() {
            return false;
        }

        // Check if it's committed in the database
        self.db
            .query_row(
                "SELECT committed FROM entries WHERE cache_key = ?1",
                params![cache_key],
                |row| row.get::<_, bool>(0),
            )
            .unwrap_or(false)
    }

    /// Load metadata for a cached entry and record a hit.
    pub fn get(&self, cache_key: &str) -> Result<Option<EntryMeta>> {
        if !self.contains(cache_key) {
            // If we previously evicted this key, this miss is the demand
            // signal an eviction policy needs and a live-store snapshot can
            // never show (kunobi-ninja/kache#594). Read-only unless a
            // not-yet-demanded tombstone actually matches.
            self.note_tombstone_demand(cache_key);
            return Ok(None);
        }

        let entry_dir = self.entry_dir(cache_key);
        let meta_path = entry_dir.join("meta.json");
        let content = fs::read_to_string(&meta_path).context("reading entry meta.json")?;
        let meta: EntryMeta = serde_json::from_str(&content).context("parsing entry meta.json")?;

        // Lazy migration: if legacy artifacts still live in the entry dir, migrate them
        let needs_migration = meta.files.iter().any(|f| entry_dir.join(&f.name).exists());
        if needs_migration && let Err(e) = self.migrate_entry_to_blobs(&meta) {
            tracing::warn!(
                "lazy migration failed for {}: {e}",
                &cache_key[..16.min(cache_key.len())]
            );
        }

        // Decide once per hit whether to content-verify, so all of an entry's
        // blobs are checked together (or none) and `Sampled` advances its
        // counter once per hit, not once per blob (kunobi-ninja/kache#332).
        let verify_content = should_verify_this_restore(verify_restores_mode());

        // Verify all cached blobs still exist on disk and match expected size
        for cached_file in &meta.files {
            let blob = self.blob_path(&cached_file.hash);
            if !blob.is_file() {
                tracing::warn!(
                    "cache entry {} missing blob {} for file {}, evicting",
                    cache_key.get(..16).unwrap_or(cache_key),
                    &cached_file.hash[..16],
                    cached_file.name
                );
                let _ = self.remove_entry(cache_key);
                return Ok(None);
            }

            // Size validation: catches truncated/corrupt artifacts (e.g. LLVM
            // "truncated or malformed object") without the cost of re-hashing.
            if let Ok(file_meta) = fs::metadata(&blob)
                && file_meta.len() != cached_file.size
            {
                tracing::warn!(
                    "cache entry {} file {} size mismatch (expected {}, got {}), evicting",
                    cache_key.get(..16).unwrap_or(cache_key),
                    cached_file.name,
                    cached_file.size,
                    file_meta.len(),
                );
                let _ = self.remove_entry(cache_key);
                return Ok(None);
            }

            // Content verification (KACHE_VERIFY_RESTORES=off|sampled|always):
            // re-hash the blob against its content address to catch silent
            // corruption / bit rot before it reaches the compiler. A mismatch is
            // routed through the same evict-and-miss path as a missing blob, so
            // the build recompiles rather than consuming a poisoned artifact.
            // `sampled` amortizes the extra read across ~1/16 of hits; `always`
            // checks every hit; `off` (default) relies on the size check above
            // (kunobi-ninja/kache#332).
            if verify_content {
                match crate::cache_key::hash_file(&blob) {
                    Ok(actual) if actual == cached_file.hash => {}
                    Ok(actual) => {
                        tracing::warn!(
                            "cache entry {} file {} content mismatch (expected {}, got {}), evicting",
                            cache_key.get(..16).unwrap_or(cache_key),
                            cached_file.name,
                            &cached_file.hash[..16.min(cached_file.hash.len())],
                            &actual[..16.min(actual.len())],
                        );
                        let _ = self.remove_entry(cache_key);
                        return Ok(None);
                    }
                    Err(e) => {
                        tracing::warn!(
                            "cache entry {} file {} unreadable for verification ({e}), evicting",
                            cache_key.get(..16).unwrap_or(cache_key),
                            cached_file.name,
                        );
                        let _ = self.remove_entry(cache_key);
                        return Ok(None);
                    }
                }
            }
        }

        // Update access time and hit count
        self.db.execute(
            "UPDATE entries SET last_accessed = datetime('now'), hit_count = hit_count + 1 WHERE cache_key = ?1",
            params![cache_key],
        )?;

        Ok(Some(meta))
    }

    /// Acquire a build lock for a cache key. Returns None if another process holds it.
    pub fn try_lock(&self, cache_key: &str) -> Result<Option<KeyLock>> {
        self.try_acquire_lock(self.entry_dir(cache_key).with_extension("lock"))
    }

    /// Claim a cache miss, re-checking the store after acquiring the key lock.
    ///
    /// The re-check closes the window where a peer can commit and release its
    /// lock between this process's cache lookup and lock acquisition.
    pub fn claim_build(&self, cache_key: &str) -> Result<BuildClaim> {
        let Some(lock) = self.try_lock(cache_key)? else {
            return Ok(BuildClaim::Contended);
        };
        match self.get(cache_key)? {
            Some(meta) if meta.files.is_empty() => {
                tracing::warn!("cache entry {cache_key} has no files, evicting before build");
                self.remove_entry(cache_key)?;
                Ok(BuildClaim::Acquired(lock))
            }
            Some(meta) => Ok(BuildClaim::Committed(Box::new(meta))),
            None => Ok(BuildClaim::Acquired(lock)),
        }
    }

    /// Acquire the cross-process GC lock so concurrent GC drivers — a manual
    /// `kache gc`, the daemon's periodic sweep, `maybe_evict_after_upload`, or a
    /// second daemon — don't double-scan and contend. Returns `None` if another
    /// GC already holds it (the caller should skip).
    pub fn try_gc_lock(&self) -> Result<Option<GcLock>> {
        self.try_acquire_file_lock(self.config.store_dir().join("gc.lock"))
    }

    /// Block until the cross-process GC lock is held.
    ///
    /// Durable upload-intent publication uses the same lock as every
    /// production GC driver: either GC finishes first and publication
    /// revalidates that the payload survived, or the intent becomes durable
    /// before GC snapshots its protected keys.
    pub(crate) fn acquire_gc_lock(&self) -> Result<GcLock> {
        self.acquire_file_lock(self.config.store_dir().join("gc.lock"))
    }

    /// Publish a fully-written `lock_path` exclusively, stale-recovering a lock
    /// left by a dead process. `None` means another live process holds it.
    fn try_acquire_lock(&self, lock_path: PathBuf) -> Result<Option<KeyLock>> {
        if let Some(lock) = PreparedKeyLock::new(lock_path.clone())?.publish()? {
            return Ok(Some(lock));
        }

        // Avoid the recovery lock on the ordinary live-owner contention path.
        if !self.is_lock_stale(&lock_path)? {
            return Ok(None);
        }

        // Serialize the rare stale check/remove/publish sequence. Without this,
        // two reclaimers can both approve removal of the old marker; the second
        // can then delete the first reclaimer's newly-published live marker.
        // The per-key build lock itself remains PID-file based.
        let _recovery_guard =
            self.acquire_file_lock(self.config.store_dir().join("build-lock-recovery.lock"))?;

        // The previous owner may have exited while we waited. Acquire directly
        // if the canonical path is now free, or re-check the current marker so
        // a reclaimer that won before us cannot be mistaken for the stale one.
        if let Some(lock) = PreparedKeyLock::new(lock_path.clone())?.publish()? {
            return Ok(Some(lock));
        }
        if !self.is_lock_stale(&lock_path)? {
            return Ok(None);
        }

        match fs::remove_file(&lock_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        PreparedKeyLock::new(lock_path)?.publish()
    }

    /// Block until an OS-backed coordination lock is held.
    fn acquire_file_lock(&self, lock_path: PathBuf) -> Result<GcLock> {
        fs::create_dir_all(lock_path.parent().unwrap())?;
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        file.lock()?;

        use std::io::{Seek, SeekFrom, Write};
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        write!(file, "{}", std::process::id())?;
        Ok(GcLock { file: Some(file) })
    }

    /// Acquire an exclusive OS lock on `lock_path` and write the holder PID for
    /// debugging. The OS releases this lock if the process exits, so no PID
    /// stale-recovery or age timeout is needed.
    fn try_acquire_file_lock(&self, lock_path: PathBuf) -> Result<Option<GcLock>> {
        fs::create_dir_all(lock_path.parent().unwrap())?;
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;

        match file.try_lock() {
            Ok(()) => {
                use std::io::{Seek, SeekFrom, Write};
                file.set_len(0)?;
                file.seek(SeekFrom::Start(0))?;
                let _ = write!(file, "{}", std::process::id());
                Ok(Some(GcLock { file: Some(file) }))
            }
            Err(std::fs::TryLockError::WouldBlock) => Ok(None),
            Err(std::fs::TryLockError::Error(e)) => Err(e.into()),
        }
    }

    /// Wait for a cache key to become committed (another process is building it).
    pub fn wait_for_committed(&self, cache_key: &str) -> Result<bool> {
        let lock_path = self.entry_dir(cache_key).with_extension("lock");
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(600); // 10 min max

        while lock_path.exists() && start.elapsed() < timeout {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        // After the lock is gone, check if it was committed
        Ok(self.contains(cache_key))
    }

    /// Store compilation outputs under the cache key.
    ///
    /// Artifact files are stored in the content-addressed blob store
    /// (`store/blobs/{hash[0..2]}/{hash}`). The entry directory only
    /// contains `meta.json`. Identical content is deduplicated via
    /// reference counting in the `blobs` table.
    #[allow(dead_code)]
    pub fn put(
        &self,
        cache_key: &str,
        crate_name: &str,
        crate_types: &[String],
        features: &[String],
        target: &str,
        profile: &str,
        output_files: &[(PathBuf, String)], // (source_path, filename_in_store)
        stdout: &str,
        stderr: &str,
    ) -> Result<StorePutResult> {
        self.put_with_compile_time(
            cache_key,
            crate_name,
            crate_types,
            features,
            target,
            profile,
            output_files,
            stdout,
            stderr,
            0,
        )
    }

    pub fn put_with_compile_time(
        &self,
        cache_key: &str,
        crate_name: &str,
        crate_types: &[String],
        features: &[String],
        target: &str,
        profile: &str,
        output_files: &[(PathBuf, String)], // (source_path, filename_in_store)
        stdout: &str,
        stderr: &str,
        compile_time_ms: u64,
    ) -> Result<StorePutResult> {
        self.put_with_compile_time_policy(
            cache_key,
            crate_name,
            crate_types,
            features,
            target,
            profile,
            output_files,
            stdout,
            stderr,
            compile_time_ms,
            true,
        )
    }

    /// Store outputs without ever sharing the compiler output inode with the
    /// read-only blob. Reflinks remain eligible because they provide CoW
    /// isolation; the fallback is a byte copy rather than a hardlink.
    pub(crate) fn put_with_compile_time_independent(
        &self,
        cache_key: &str,
        crate_name: &str,
        crate_types: &[String],
        features: &[String],
        target: &str,
        profile: &str,
        output_files: &[(PathBuf, String)],
        stdout: &str,
        stderr: &str,
        compile_time_ms: u64,
    ) -> Result<StorePutResult> {
        self.put_with_compile_time_policy(
            cache_key,
            crate_name,
            crate_types,
            features,
            target,
            profile,
            output_files,
            stdout,
            stderr,
            compile_time_ms,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn put_with_compile_time_policy(
        &self,
        cache_key: &str,
        crate_name: &str,
        crate_types: &[String],
        features: &[String],
        target: &str,
        profile: &str,
        output_files: &[(PathBuf, String)],
        stdout: &str,
        stderr: &str,
        compile_time_ms: u64,
        allow_source_hardlinks: bool,
    ) -> Result<StorePutResult> {
        let entry_dir = self.entry_dir(cache_key);

        // Phase 1: stage every output into a private snapshot and hash THE
        // SNAPSHOT — never the live build output — before any committed entry
        // can reference it. The digest is computed over exactly the bytes
        // that will be published under it, so a post-build mutator (strip,
        // codesign, wasm post-processing) changing the file between staging
        // and hashing cannot store content X under address H(Y) (review
        // finding #3). No DB writes happen here, so a crash leaves at most an
        // unpublished staging file (`sweep_stale_staging`) or orphan blob
        // files (`sweep_orphan_blobs`), never a half-registered entry.
        // `sources` is kept so Phase 2 can re-materialize a blob if a
        // concurrent remove unlinks it.
        let mut cached_files = Vec::new();
        let mut sources: Vec<(PathBuf, bool)> = Vec::new();
        let mut seen_output_blobs = std::collections::HashSet::new();
        let mut put_result = StorePutResult::default();
        let mut total_size = 0u64;
        for (source_path, store_name) in output_files {
            // Eligibility decision only; the staged snapshot's own metadata
            // is authoritative for what gets recorded below.
            let source_executable = fs::metadata(source_path)
                .map(|meta| is_executable(&meta))
                .unwrap_or(false);
            let use_source_hardlink =
                source_hardlink_allowed(allow_source_hardlinks, store_name, source_executable);

            let (staged, ingest) = self.stage_blob_from_source(source_path, use_source_hardlink)?;
            let staged_meta = match fs::metadata(&staged) {
                Ok(meta) => meta,
                Err(e) => {
                    Self::discard_staged_blob(&staged);
                    return Err(anyhow::Error::new(e)
                        .context(format!("stating staged blob for {store_name}")));
                }
            };
            let size = staged_meta.len();
            let executable = is_executable(&staged_meta);
            if size == 0 && !zero_byte_is_valid_output(store_name, crate_types) {
                Self::discard_staged_blob(&staged);
                anyhow::bail!("refusing to cache zero-byte artifact: {}", store_name);
            }
            total_size += size;

            let hash = crate::cache_key::hash_file(&staged)?;
            if seen_output_blobs.insert(hash.clone()) {
                put_result.output_blobs += 1;
                if self.blob_path(&hash).is_file() {
                    put_result.duplicate_blobs += 1;
                } else {
                    put_result.new_blobs += 1;
                }
            }

            self.publish_staged_blob(&staged, ingest, &hash, size)?;

            cached_files.push(CachedFile {
                name: store_name.clone(),
                size,
                hash,
                executable,
            });
            sources.push((source_path.clone(), use_source_hardlink));
        }

        let content_hash = compute_content_hash(&cached_files);

        // Record which rustc `--emit` kinds this entry actually contains, derived
        // from the stored output files (kunobi-ninja/kache#325). Lookup rejects an
        // entry that doesn't cover what the invocation's `--emit` requested.
        let emit_kinds = emit_kinds_for_files(&cached_files);

        // Capture the compiler's diagnostics so a cache hit can replay them
        // verbatim — warning gates / `-D warnings` then behave identically on a
        // hit vs a miss (kunobi-ninja/kache#336). Optionally capped against
        // pathological streams; uncapped by default for full fidelity.
        let diag_cap = max_diagnostics_bytes();

        // Write metadata (only meta.json in the entry directory)
        let meta = EntryMeta {
            cache_key: cache_key.to_string(),
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            crate_name: crate_name.to_string(),
            crate_types: crate_types.to_vec(),
            files: cached_files,
            stdout: cap_diagnostics(stdout, diag_cap),
            stderr: cap_diagnostics(stderr, diag_cap),
            features: features.to_vec(),
            target: target.to_string(),
            profile: profile.to_string(),
            compile_time_ms,
            emit_kinds,
        };
        let meta_json =
            serde_json::to_string_pretty(&meta).context("serializing entry metadata")?;
        let meta_path = entry_dir.join("meta.json");

        // Phase 2: register the entry and all of its blob references in a single
        // transaction, flipping `committed = 1` only once every blob is durable
        // on disk. Either the whole entry (with correct refcounts) becomes
        // visible, or none of it does — no refcount drift, no half-written row.
        //
        // `meta.json` is written INSIDE this transaction (#670), after the
        // write lock is held, so its appearance on disk is serialized against
        // `remove_entry_guarded`'s locked cleanup pass. Written before the
        // lock, a fresh meta could land between a racing removal's committed
        // row delete and its cleanup pass — whose republication check sees no
        // row for this key yet and deletes the directory, fresh meta included
        // — stranding this put's committed row with no artifacts and leaking
        // its refcounts until doctor or an index rebuild.
        let crate_type_str = crate_types.join(",");
        let num_features = features.len() as i64;
        let tx = self.db.unchecked_transaction()?;
        for (file, (source, use_source_hardlink)) in meta.files.iter().zip(sources.iter()) {
            let inserted = tx.execute(
                "INSERT OR IGNORE INTO blobs (hash, size, refcount) VALUES (?1, ?2, 1)",
                params![file.hash, file.size as i64],
            )?;
            if inserted == 0 {
                tx.execute(
                    "UPDATE blobs SET refcount = refcount + 1 WHERE hash = ?1",
                    params![file.hash],
                )?;
            }
            // Race guard: the INSERT/UPDATE above holds the write lock, and
            // `remove_entry` only unlinks a blob while holding that same lock,
            // so a concurrent reclaim cannot interleave here. If a remove
            // unlinked this blob between Phase 1 and now, re-materialize it
            // before we commit a reference to it — and verify the digest,
            // since the re-ingest reads the LIVE source (review finding #3).
            self.rematerialize_and_verify(source, &file.hash, &file.name, *use_source_hardlink)?;
        }
        record_entry_blobs(&tx, cache_key, &meta.files)?;
        // The write lock is held from the statements above (record_entry_blobs
        // always issues at least the DELETE). Materialize meta.json under it:
        // a concurrent removal's cleanup pass takes the same lock before it
        // deletes anything, so it runs either entirely before this write (this
        // put then re-creates the directory) or entirely after this
        // transaction commits (its republication check then sees this row and
        // leaves the directory alone). The staged write + atomic rename means
        // no reader — locked or not — can ever observe a truncated or
        // partially written meta.json, and the rename's parent-directory
        // fsync makes the new name durable alongside the contents.
        fs::create_dir_all(&entry_dir).context("creating entry directory")?;
        crate::atomic::atomic_replace(&meta_path, meta_json.as_bytes())
            .context("writing entry metadata")?;
        tx.execute(
            "INSERT OR REPLACE INTO entries (cache_key, crate_name, crate_type, profile, num_features, size, content_hash, compile_time_ms, key_schema, committed) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1)",
            params![cache_key, crate_name, crate_type_str, profile, num_features, total_size as i64, content_hash, compile_time_ms as i64, crate::cache_key::CACHE_KEY_VERSION],
        )?;
        tx.commit()?;

        // Both ingest phases may hardlink a compiler output to the immutable
        // blob and mark the shared inode read-only, changing ctime. Seed only
        // after the transaction's final rematerialization. Independent (C/C++)
        // puts use disposable staging paths and deliberately do not seed them.
        if allow_source_hardlinks {
            for (file, (source, _)) in meta.files.iter().zip(&sources) {
                // The Rust wrapper expands dep-info back to absolute paths
                // immediately after `put`, so it is not stable here.
                if !matches!(
                    crate::compiler::classify_by_filename(&file.name),
                    crate::compiler::ArtifactKind::DepInfo
                ) {
                    self.record_known_file_hash(source, &file.hash);
                }
            }
        }

        Ok(put_result)
    }

    /// Import a remotely downloaded entry into the database.
    ///
    /// Downloaded entries arrive as tar archives extracted into the entry
    /// directory (old format: artifact files alongside meta.json). This
    /// method moves the artifact files into the content-addressed blob
    /// store and records them in the `blobs` table, leaving only
    /// `meta.json` in the entry directory.
    pub fn import_downloaded_entry(&self, cache_key: &str) -> Result<()> {
        let entry_dir = self.entry_dir(cache_key);
        let meta_path = entry_dir.join("meta.json");
        let content = fs::read_to_string(&meta_path).context("reading downloaded meta.json")?;
        let meta: EntryMeta =
            serde_json::from_str(&content).context("parsing downloaded meta.json")?;

        // Remote `meta.json` is untrusted (a shared / MITM'd bucket can poison
        // its `files[]`), so validate the trust boundary before any field reaches
        // path construction, the blob store, or the user's `target/` (#211).
        let short_key = cache_key.get(..16).unwrap_or(cache_key);
        for cached_file in &meta.files {
            // C: a malformed `hash` becomes a shard path component (`&hash[..2]`)
            // — reject anything that isn't a 64-char blake3 hex digest so it can
            // never panic a slice or escape the blob shard.
            if !crate::remote_layout::is_blob_hash(&cached_file.hash) {
                anyhow::bail!(
                    "downloaded entry {short_key}: rejecting file {} — malformed blob hash {:?}",
                    cached_file.name,
                    cached_file.hash,
                );
            }
            // B: a `name` that is absolute or contains `..` escapes the entry dir
            // on join — require a single normal component.
            if !crate::remote_layout::is_safe_artifact_name(&cached_file.name) {
                anyhow::bail!(
                    "downloaded entry {short_key}: rejecting unsafe artifact name {:?}",
                    cached_file.name,
                );
            }

            let file_path = entry_dir.join(&cached_file.name);
            if !file_path.is_file() {
                anyhow::bail!(
                    "downloaded entry {short_key} missing file: {}",
                    cached_file.name
                );
            }
            let file_meta = fs::metadata(&file_path).with_context(|| {
                format!("downloaded entry {short_key}: stat {}", cached_file.name)
            })?;
            if file_meta.len() != cached_file.size {
                anyhow::bail!(
                    "downloaded entry {short_key} file {} size mismatch (expected {}, got {})",
                    cached_file.name,
                    cached_file.size,
                    file_meta.len(),
                );
            }
            // A: re-hash the bytes and reject if they don't match the claimed
            // address. Size-only is insufficient for untrusted content — a
            // same-length substituted/corrupted object would otherwise be
            // installed under its claimed hash and hardlinked into the build as
            // if content-verified. blake3 is fast; do it before any rename/INSERT.
            let actual = crate::cache_key::hash_file(&file_path).with_context(|| {
                format!(
                    "downloaded entry {short_key}: hashing {} for trust-boundary check",
                    cached_file.name
                )
            })?;
            if actual != cached_file.hash {
                anyhow::bail!(
                    "downloaded entry {short_key}: content hash mismatch for {} \
                     (claimed {}, actual {})",
                    cached_file.name,
                    cached_file.hash,
                    actual,
                );
            }
        }

        // Phase 1: move each *new* blob into the content-addressed store and make
        // it durable. For blobs that already exist (shared), keep the downloaded
        // copy in the entry dir for now — it's the fallback Phase 2 restores from
        // if a concurrent remove unlinks the blob.
        for cached_file in &meta.files {
            let blob = self.blob_path(&cached_file.hash);
            if !blob.is_file() {
                let file_path = entry_dir.join(&cached_file.name);
                fs::create_dir_all(blob.parent().unwrap())
                    .context("creating blob shard directory")?;
                fs::rename(&file_path, &blob).with_context(|| {
                    format!(
                        "moving downloaded artifact {} to blob store",
                        file_path.display()
                    )
                })?;
                crate::atomic::fsync_file(&blob).context("flushing downloaded blob to disk")?;
                set_blob_readonly(&blob);
            }
        }

        let total_size: u64 = meta.files.iter().map(|f| f.size).sum();

        let content_hash = compute_content_hash(&meta.files);

        // Phase 2: register blob references and the entry row atomically, so the
        // entry only becomes visible once every blob is in place. The write lock
        // the INSERT/UPDATE holds also serializes us against `remove_entry`'s
        // unlink, so we can safely restore a blob a concurrent remove reclaimed.
        let crate_type_str = meta.crate_types.join(",");
        let num_features = meta.features.len() as i64;
        let tx = self.db.unchecked_transaction()?;
        for cached_file in &meta.files {
            let inserted = tx.execute(
                "INSERT OR IGNORE INTO blobs (hash, size, refcount) VALUES (?1, ?2, 1)",
                params![cached_file.hash, cached_file.size as i64],
            )?;
            if inserted == 0 {
                tx.execute(
                    "UPDATE blobs SET refcount = refcount + 1 WHERE hash = ?1",
                    params![cached_file.hash],
                )?;
            }
            let blob = self.blob_path(&cached_file.hash);
            if !blob.is_file() {
                // A concurrent remove unlinked this shared blob; restore it from
                // the downloaded copy kept in Phase 1 (still under the lock).
                let file_path = entry_dir.join(&cached_file.name);
                if !file_path.is_file() {
                    anyhow::bail!(
                        "downloaded blob {} vanished during import",
                        &cached_file.hash[..16.min(cached_file.hash.len())]
                    );
                }
                fs::create_dir_all(blob.parent().unwrap())
                    .context("creating blob shard directory")?;
                fs::rename(&file_path, &blob).with_context(|| {
                    format!(
                        "restoring downloaded artifact {} to blob store",
                        file_path.display()
                    )
                })?;
                crate::atomic::fsync_file(&blob).context("flushing downloaded blob to disk")?;
                set_blob_readonly(&blob);
            }
        }
        record_entry_blobs(&tx, cache_key, &meta.files)?;
        tx.execute(
            "INSERT OR REPLACE INTO entries (cache_key, crate_name, crate_type, profile, num_features, size, content_hash, compile_time_ms, key_schema, committed) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1)",
            params![cache_key, meta.crate_name, crate_type_str, meta.profile, num_features, total_size as i64, content_hash, meta.compile_time_ms as i64, meta.key_schema],
        )?;
        tx.commit()?;

        // Remove any downloaded duplicates kept as fallbacks but not needed
        // (their blob already existed and survived).
        for cached_file in &meta.files {
            let file_path = entry_dir.join(&cached_file.name);
            if file_path.is_file() {
                let _ = fs::remove_file(&file_path);
            }
        }

        Ok(())
    }

    /// Import a restored entry into the local store.
    ///
    /// This is the format-agnostic seam future remote layouts should call.
    /// Today it is equivalent to `import_downloaded_entry()`.
    pub fn import_restored_entry(&self, cache_key: &str) -> Result<()> {
        self.import_downloaded_entry(cache_key)
    }

    /// Install one already-verified artifact into the content-addressed blob
    /// store. A missing source is a distinct integrity race, not a generic
    /// rename failure, so keep that decision directly testable.
    fn install_verified_blob(&self, entry_dir: &Path, file: &CachedFile) -> Result<()> {
        let blob = self.blob_path(&file.hash);
        if !blob.is_file() {
            let artifact = entry_dir.join(&file.name);
            if !artifact.is_file() {
                anyhow::bail!("verified restored blob vanished during batch import");
            }
            fs::create_dir_all(blob.parent().expect("blob path has a parent"))?;
            fs::rename(&artifact, &blob)?;
            crate::atomic::fsync_file(&blob)?;
            set_blob_readonly(&blob);
        }
        Ok(())
    }

    /// Import already stream-verified restored entries with one SQLite
    /// transaction for the whole batch.
    ///
    /// Artifact content is not hashed again: callers may only construct
    /// [`VerifiedRestoredEntry`] after extraction verified each byte against
    /// `meta.files[].hash`. This method still performs a complete metadata and
    /// on-disk length preflight before moving blobs or opening the transaction.
    pub(crate) fn import_verified_restored_entries(
        &self,
        entries: &[VerifiedRestoredEntry],
    ) -> Result<usize> {
        let mut cache_keys = std::collections::HashSet::new();
        for entry in entries {
            if !crate::cache_key::is_valid_cache_key(&entry.cache_key)
                || entry.meta.cache_key != entry.cache_key
            {
                anyhow::bail!("verified restore has an invalid cache-key binding");
            }
            if entry.meta.key_schema != crate::cache_key::CACHE_KEY_VERSION {
                anyhow::bail!(
                    "verified restore {} uses incompatible key schema {}",
                    &entry.cache_key[..16],
                    entry.meta.key_schema
                );
            }
            if !crate::cache_key::is_valid_crate_name(&entry.meta.crate_name) {
                anyhow::bail!("verified restore has an unsafe crate name");
            }
            if !cache_keys.insert(entry.cache_key.as_str()) {
                anyhow::bail!("verified restore batch contains a duplicate cache key");
            }

            let entry_dir = self.entry_dir(&entry.cache_key);
            let meta_bytes = fs::read(entry_dir.join("meta.json"))
                .context("reading stream-verified entry metadata")?;
            let disk_meta: EntryMeta = serde_json::from_slice(&meta_bytes)
                .context("parsing stream-verified entry metadata")?;
            if disk_meta != entry.meta {
                anyhow::bail!("verified restore metadata changed after extraction");
            }

            let mut artifact_names = std::collections::HashSet::new();
            for file in &entry.meta.files {
                if !crate::remote_layout::is_safe_artifact_name(&file.name)
                    || !crate::cache_key::is_valid_cache_key(&file.hash)
                    || !artifact_names.insert(file.name.as_str())
                {
                    anyhow::bail!(
                        "verified restore contains unsafe or duplicate artifact metadata"
                    );
                }
                let artifact = entry_dir.join(&file.name);
                let actual_size = fs::metadata(&artifact)
                    .with_context(|| format!("stat verified artifact {}", file.name))?
                    .len();
                if actual_size != file.size {
                    anyhow::bail!(
                        "verified artifact {} size mismatch (expected {}, got {})",
                        file.name,
                        file.size,
                        actual_size
                    );
                }
            }
        }

        // Make every content-addressed blob durable before the database can
        // advertise a reference to it. Existing blobs leave the extracted copy
        // in place as the in-transaction race fallback below.
        for entry in entries {
            let entry_dir = self.entry_dir(&entry.cache_key);
            for file in &entry.meta.files {
                let blob = self.blob_path(&file.hash);
                if !blob.is_file() {
                    let artifact = entry_dir.join(&file.name);
                    fs::create_dir_all(blob.parent().expect("blob path has a parent"))
                        .context("creating verified blob shard directory")?;
                    fs::rename(&artifact, &blob).with_context(|| {
                        format!(
                            "moving verified artifact {} to blob store",
                            artifact.display()
                        )
                    })?;
                    crate::atomic::fsync_file(&blob)
                        .context("flushing verified restored blob to disk")?;
                    set_blob_readonly(&blob);
                }
            }
        }

        let tx = self.db.unchecked_transaction()?;
        let mut imported = 0usize;
        for entry in entries {
            let meta = &entry.meta;
            let total_size: u64 = meta.files.iter().map(|file| file.size).sum();
            let crate_type = meta.crate_types.join(",");
            let content_hash = compute_content_hash(&meta.files);
            // A crash or legacy importer may have left an uncommitted row.
            // It is not a cache hit and must not permanently block a verified
            // replacement through INSERT OR IGNORE. Undo any partial mapping
            // bookkeeping in this same transaction before replacing it.
            tx.execute(
                "UPDATE blobs
                 SET refcount = MAX(0, refcount - COALESCE((
                     SELECT refs FROM entry_blobs
                     WHERE cache_key = ?1 AND hash = blobs.hash
                 ), 0))
                 WHERE hash IN (
                     SELECT hash FROM entry_blobs WHERE cache_key = ?1
                 ) AND EXISTS (
                     SELECT 1 FROM entries
                     WHERE cache_key = ?1 AND committed = 0
                 )",
                params![entry.cache_key],
            )?;
            tx.execute(
                "DELETE FROM entry_blobs
                 WHERE cache_key = ?1 AND EXISTS (
                     SELECT 1 FROM entries
                     WHERE cache_key = ?1 AND committed = 0
                 )",
                params![entry.cache_key],
            )?;
            tx.execute(
                "DELETE FROM entries WHERE cache_key = ?1 AND committed = 0",
                params![entry.cache_key],
            )?;
            tx.execute("DELETE FROM blobs WHERE refcount <= 0", [])?;
            let inserted = tx.execute(
                "INSERT OR IGNORE INTO entries (cache_key, crate_name, crate_type, profile, num_features, size, content_hash, compile_time_ms, key_schema, committed) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0)",
                params![
                    entry.cache_key,
                    meta.crate_name,
                    crate_type,
                    meta.profile,
                    meta.features.len() as i64,
                    total_size as i64,
                    content_hash,
                    meta.compile_time_ms as i64,
                    meta.key_schema
                ],
            )?;
            if inserted == 0 {
                continue;
            }

            let entry_dir = self.entry_dir(&entry.cache_key);
            for file in &meta.files {
                let added = tx.execute(
                    "INSERT OR IGNORE INTO blobs (hash, size, refcount) VALUES (?1, ?2, 1)",
                    params![file.hash, file.size as i64],
                )?;
                if added == 0 {
                    tx.execute(
                        "UPDATE blobs SET refcount = refcount + 1 WHERE hash = ?1",
                        params![file.hash],
                    )?;
                }
                self.install_verified_blob(&entry_dir, file)?;
            }
            record_entry_blobs(&tx, &entry.cache_key, &meta.files)?;
            tx.execute(
                "UPDATE entries SET committed = 1 WHERE cache_key = ?1",
                params![entry.cache_key],
            )?;
            imported += 1;
        }
        tx.commit()?;

        for entry in entries {
            let entry_dir = self.entry_dir(&entry.cache_key);
            for file in &entry.meta.files {
                let artifact = entry_dir.join(&file.name);
                if artifact.is_file() {
                    let _ = fs::remove_file(artifact);
                }
            }
        }
        Ok(imported)
    }

    /// Read the authoritative blob reference graph from committed entry
    /// metadata. The caller must hold SQLite's write lock so a publisher or
    /// remover cannot change the row/meta pairing during the scan (#819).
    fn authoritative_blob_index(&self, conn: &Connection) -> Result<AuthoritativeBlobIndex> {
        let keys: Vec<String> = {
            let mut stmt = conn
                .prepare("SELECT cache_key FROM entries WHERE committed = 1 ORDER BY cache_key")?;
            stmt.query_map([], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut index = AuthoritativeBlobIndex::default();
        for key in keys {
            let meta_path = self.entry_dir(&key).join("meta.json");
            let content = fs::read_to_string(&meta_path)
                .with_context(|| format!("entry {key}: reading authoritative meta.json"))?;
            let meta: EntryMeta = serde_json::from_str(&content)
                .with_context(|| format!("entry {key}: parsing authoritative meta.json"))?;
            for file in &meta.files {
                if !crate::remote_layout::is_blob_hash(&file.hash)
                    || !crate::remote_layout::is_safe_artifact_name(&file.name)
                {
                    anyhow::bail!("entry {key}: invalid blob metadata");
                }
                let blob_path = self.blob_path(&file.hash);
                let actual_size = fs::metadata(&blob_path)
                    .with_context(|| format!("entry {key}: reading blob {}", file.hash))?
                    .len();
                if actual_size != file.size {
                    anyhow::bail!(
                        "entry {key}: blob {} size mismatch (expected {}, got {})",
                        file.hash,
                        file.size,
                        actual_size
                    );
                }

                *index
                    .entry_mappings
                    .entry((key.clone(), file.hash.clone()))
                    .or_insert(0) += 1;
                match index.blobs.entry(file.hash.clone()) {
                    std::collections::btree_map::Entry::Vacant(slot) => {
                        slot.insert((file.size as i64, 1));
                    }
                    std::collections::btree_map::Entry::Occupied(mut slot) => {
                        let (size, refs) = slot.get_mut();
                        if *size != file.size as i64 {
                            anyhow::bail!(
                                "blob {} has conflicting sizes in committed metadata",
                                file.hash
                            );
                        }
                        *refs += 1;
                    }
                }
            }
        }
        Ok(index)
    }

    fn indexed_blob_graph(&self, conn: &Connection) -> Result<AuthoritativeBlobIndex> {
        let entry_mappings = {
            let mut stmt = conn.prepare("SELECT cache_key, hash, refs FROM entry_blobs")?;
            stmt.query_map([], |row| Ok(((row.get(0)?, row.get(1)?), row.get(2)?)))?
                .collect::<Result<std::collections::BTreeMap<_, _>, _>>()?
        };
        let blobs = {
            let mut stmt = conn.prepare("SELECT hash, size, refcount FROM blobs")?;
            stmt.query_map([], |row| Ok((row.get(0)?, (row.get(1)?, row.get(2)?))))?
                .collect::<Result<std::collections::BTreeMap<_, _>, _>>()?
        };
        Ok(AuthoritativeBlobIndex {
            entry_mappings,
            blobs,
        })
    }

    fn compare_blob_indexes(
        expected: &AuthoritativeBlobIndex,
        actual: &AuthoritativeBlobIndex,
    ) -> BlobIndexDrift {
        let entry_mappings = expected
            .entry_mappings
            .keys()
            .chain(actual.entry_mappings.keys())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .filter(|key| expected.entry_mappings.get(*key) != actual.entry_mappings.get(*key))
            .count();
        let blobs = expected
            .blobs
            .keys()
            .chain(actual.blobs.keys())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .filter(|hash| expected.blobs.get(*hash) != actual.blobs.get(*hash))
            .count();
        BlobIndexDrift {
            entry_mappings,
            blobs,
        }
    }

    /// Verify that `entry_blobs` and `blobs` exactly match committed entry
    /// metadata. The write lock makes the filesystem/SQLite comparison stable.
    pub fn blob_index_drift(&self) -> Result<BlobIndexDrift> {
        self.db.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| {
            let expected = self.authoritative_blob_index(&self.db)?;
            let actual = self.indexed_blob_graph(&self.db)?;
            Ok(Self::compare_blob_indexes(&expected, &actual))
        })();
        match result {
            Ok(drift) => {
                self.db.execute_batch("COMMIT")?;
                Ok(drift)
            }
            Err(error) => {
                let _ = self.db.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    /// Rebuild only the derived blob graph from committed entry metadata.
    /// Physical orphan reclamation deliberately happens after this transaction
    /// through [`Self::sweep_orphan_blobs`], never while SQL can roll back.
    pub fn reconcile_blob_index(&self) -> Result<BlobIndexDrift> {
        self.db.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<BlobIndexDrift> {
            let expected = self.authoritative_blob_index(&self.db)?;
            let actual = self.indexed_blob_graph(&self.db)?;
            let drift = Self::compare_blob_indexes(&expected, &actual);
            if drift.total() == 0 {
                return Ok(drift);
            }

            self.db.execute("DELETE FROM entry_blobs", [])?;
            self.db.execute("DELETE FROM blobs", [])?;
            for ((cache_key, hash), refs) in &expected.entry_mappings {
                self.db.execute(
                    "INSERT INTO entry_blobs (cache_key, hash, refs) VALUES (?1, ?2, ?3)",
                    params![cache_key, hash, refs],
                )?;
            }
            for (hash, (size, refcount)) in &expected.blobs {
                self.db.execute(
                    "INSERT INTO blobs (hash, size, refcount) VALUES (?1, ?2, ?3)",
                    params![hash, size, refcount],
                )?;
            }
            Ok(drift)
        })();
        match result {
            Ok(drift) => {
                self.db.execute_batch("COMMIT")?;
                Ok(drift)
            }
            Err(error) => {
                let _ = self.db.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    /// Rebuild the `entries` and `blobs` rows by scanning the store's per-entry
    /// `meta.json` files (kunobi-ninja/kache#415).
    ///
    /// The index is derived state: the blobs plus each entry's `meta.json` are
    /// the source of truth. So when the index is lost — quarantined after
    /// corruption, or deleted — the cache itself is still on disk and the rows
    /// can be reconstructed. Without this, recovery is needlessly lossy: a
    /// warm 100 GB cache silently becomes cold and every artifact is recompiled
    /// or re-downloaded even though the bytes never went anywhere.
    ///
    /// Only registers an entry when **every** file it claims resolves to a blob
    /// that is present and the right size. A partially-present entry is skipped
    /// rather than registered, because a registered entry pointing at a missing
    /// blob is a false hit — strictly worse than a miss.
    ///
    /// Idempotent, so it is safe to run on a populated index: entry rows are
    /// `INSERT OR IGNORE`d, and an entry already present contributes no blob
    /// refcounts. That matters because otherwise re-running would inflate every
    /// refcount and permanently leak blobs past their last referrer.
    ///
    /// Deliberately does **not** re-hash blob contents. This is local, already
    /// content-addressed data, not the untrusted remote payload
    /// `import_downloaded_entry` validates; hashing a whole store would make
    /// recovery cost hours. `doctor --verify --checksums` remains the surface
    /// for content verification.
    pub fn rebuild_index_from_store(&self) -> Result<RebuildStats> {
        let store_dir = self.config.store_dir();
        let mut stats = RebuildStats::default();

        let dir = match fs::read_dir(&store_dir) {
            Ok(dir) => dir,
            // No store dir yet (fresh cache): nothing to rebuild, not an error.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(stats),
            Err(e) => {
                return Err(e).with_context(|| format!("scanning store {}", store_dir.display()));
            }
        };

        for entry in dir {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    tracing::debug!("skipping unreadable store dir entry: {e}");
                    continue;
                }
            };
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            // `blobs/` is the content-addressed store, a sibling of the entry
            // dirs rather than one of them.
            if name == "blobs" {
                continue;
            }
            // Entry dirs are named by cache key. Anything else under store/ is
            // not ours to interpret, and an unvalidated name would be a path
            // component we then join (see `is_valid_cache_key`).
            if !crate::cache_key::is_valid_cache_key(name) {
                continue;
            }

            match self.rebuild_one_entry(name, &path) {
                Ok(Some(blobs)) => {
                    stats.entries_rebuilt += 1;
                    stats.blobs_registered += blobs;
                }
                Ok(None) => stats.entries_skipped += 1,
                Err(e) => {
                    tracing::debug!(
                        "skipping entry {} during index rebuild: {e:#}",
                        &name[..16.min(name.len())]
                    );
                    stats.entries_skipped += 1;
                }
            }
        }

        Ok(stats)
    }

    /// Register one entry dir's rows. Returns the number of blob references
    /// registered, or `None` when the entry is not fully present on disk.
    fn rebuild_one_entry(&self, cache_key: &str, entry_dir: &Path) -> Result<Option<usize>> {
        let meta_path = entry_dir.join("meta.json");
        if !meta_path.is_file() {
            return Ok(None);
        }
        let content = fs::read_to_string(&meta_path).context("reading entry meta.json")?;
        let meta: EntryMeta = serde_json::from_str(&content).context("parsing entry meta.json")?;

        // Validate the whole entry before writing anything, so a half-present
        // entry never lands as a row that would resolve to a missing blob.
        for file in &meta.files {
            if !crate::remote_layout::is_blob_hash(&file.hash)
                || !crate::remote_layout::is_safe_artifact_name(&file.name)
            {
                return Ok(None);
            }
            let blob = self.blob_path(&file.hash);
            match fs::metadata(&blob) {
                Ok(m) if m.len() == file.size => {}
                // Present but the wrong length, or absent: either way this entry
                // cannot be served, so do not advertise it.
                _ => return Ok(None),
            }
        }

        let total_size: u64 = meta.files.iter().map(|f| f.size).sum();
        let content_hash = compute_content_hash(&meta.files);
        let crate_type_str = meta.crate_types.join(",");
        let num_features = meta.features.len() as i64;

        let tx = self.db.unchecked_transaction()?;
        // Claim the entry row first. If it is already there, a concurrent or
        // earlier rebuild owns this entry's refcounts and we must not add more.
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO entries (cache_key, crate_name, crate_type, profile, num_features, size, content_hash, compile_time_ms, key_schema, committed) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1)",
            params![
                cache_key,
                meta.crate_name,
                crate_type_str,
                meta.profile,
                num_features,
                total_size as i64,
                content_hash,
                meta.compile_time_ms as i64,
                meta.key_schema
            ],
        )?;
        if inserted == 0 {
            tx.commit()?;
            return Ok(None);
        }

        // One reference per *file*, not per unique hash: `remove_entry` decrements
        // once per `meta.files` element, so an entry listing the same hash twice
        // must hold two references or removal would drop it below zero.
        for file in &meta.files {
            let added = tx.execute(
                "INSERT OR IGNORE INTO blobs (hash, size, refcount) VALUES (?1, ?2, 1)",
                params![file.hash, file.size as i64],
            )?;
            if added == 0 {
                tx.execute(
                    "UPDATE blobs SET refcount = refcount + 1 WHERE hash = ?1",
                    params![file.hash],
                )?;
            }
        }
        record_entry_blobs(&tx, cache_key, &meta.files)?;
        tx.commit()?;

        Ok(Some(meta.files.len()))
    }

    /// Look up cache keys for the given crate names (most recent per crate).
    pub fn keys_for_crates(&self, crate_names: &[String]) -> Result<Vec<CrateHistoryEntry>> {
        if crate_names.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders: Vec<&str> = crate_names.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT cache_key, crate_name, compile_time_ms, size FROM entries WHERE committed = 1 AND crate_name IN ({}) ORDER BY last_accessed DESC",
            placeholders.join(",")
        );
        let mut stmt = self.db.prepare(&sql)?;
        let params: Vec<&dyn rusqlite::ToSql> = crate_names
            .iter()
            .map(|n| n as &dyn rusqlite::ToSql)
            .collect();
        let rows = stmt.query_map(params.as_slice(), |row| {
            let key: String = row.get(0)?;
            let cn: String = row.get(1)?;
            let compile_time_ms: i64 = row.get(2)?;
            let size: i64 = row.get(3)?;
            Ok((key, cn, compile_time_ms, size))
        })?;
        let mut results = Vec::new();
        for row in rows {
            let (cache_key, crate_name, compile_time_ms, size) = row?;
            let entry_dir = self.entry_dir(&cache_key);
            results.push(CrateHistoryEntry {
                cache_key,
                crate_name,
                entry_dir,
                // Both columns default to 0 for rows written before their
                // migrations, so 0 has to mean "unknown" rather than "free to
                // fetch and worthless to have" (kunobi-ninja/kache#617).
                compile_time_ms: positive_or_none(compile_time_ms),
                size_bytes: positive_or_none(size),
            });
        }
        Ok(results)
    }

    /// Resolve the filesystem path for a content-addressed blob.
    /// Layout: store/blobs/{first 2 hex chars}/{full hash}
    pub fn blob_path(&self, hash: &str) -> PathBuf {
        blob_path_in_store_dir(&self.config.store_dir(), hash)
    }

    /// Return the content-addressed blob whose inode a read-only output still
    /// shares, if any.
    ///
    /// Older C/C++ restores could hardlink a build output to a read-only store
    /// blob. Passing that pathname to a compiler as root can truncate the blob
    /// and corrupt every cache entry that references it. This check is strictly
    /// read-only; callers fail closed instead of trying a racy path-based unlink.
    pub(crate) fn matching_readonly_blob_inode(
        store_dir: &Path,
        output: &Path,
    ) -> Result<Option<PathBuf>> {
        let Ok(initial) = fs::metadata(output) else {
            return Ok(None);
        };
        if !metadata_is_readonly_regular(&initial) {
            return Ok(None);
        }

        let hash = crate::cache_key::hash_file(output)
            .with_context(|| format!("hashing possible legacy output {}", output.display()))?;
        let blob = blob_path_in_store_dir(store_dir, &hash);
        if !blob.is_file() {
            return Ok(None);
        }

        // Recheck after hashing. A concurrent path swap can only make this
        // check fail closed; no pathname is ever mutated here.
        let Ok(current) = fs::metadata(output) else {
            return Ok(None);
        };
        if !metadata_is_readonly_regular(&current) {
            return Ok(None);
        }
        Ok(paths_share_inode(output, &blob).then_some(blob))
    }

    /// Directory containing all blobs.
    #[allow(dead_code)] // used in tests
    pub fn blobs_dir(&self) -> PathBuf {
        self.config.store_dir().join("blobs")
    }

    /// Directory holding in-progress put-phase staging files
    /// ([`Store::stage_blob_from_source`]). Lives under the store root but
    /// outside `blobs/`, so [`Self::sweep_orphan_blobs`] (which only considers
    /// hash-named files inside blob shards) never sees it; stale entries are
    /// reclaimed by [`Store::sweep_stale_staging`].
    fn staging_dir(&self) -> PathBuf {
        self.config.store_dir().join("staging")
    }

    /// Stage one put-phase artifact into a private snapshot under the store.
    ///
    /// The snapshot — not the live build output — is what gets hashed and
    /// published, which is what upholds the content-address invariant: a file
    /// that changes after this point cannot end up stored under another file's
    /// digest (review finding #3). Ingest order mirrors [`materialize_blob`]:
    /// reflink first, then hardlink where the kind allows inode sharing, then a
    /// real copy. The returned path must be consumed by
    /// [`Self::publish_staged_blob`] or removed by [`discard_staged_blob`].
    ///
    /// Hardlink read-only semantics match `materialize_blob`: the guard is
    /// applied only after the fsync (Windows needs a writable handle to flush,
    /// #196), and a failed demotes to a full copy rather than publishing a
    /// writable shared inode.
    ///
    /// The staging path is chosen but NOT created ([`free_staging_path`]): the
    /// reflink and hardlink ingests can only write to a destination that does
    /// not exist yet, so pre-creating it would cost a full byte copy per
    /// artifact on every filesystem.
    fn stage_blob_from_source(
        &self,
        source: &Path,
        allow_hardlink: bool,
    ) -> Result<(PathBuf, StoreIngest)> {
        let dir = self.staging_dir();
        fs::create_dir_all(&dir)
            .with_context(|| format!("creating staging directory {}", dir.display()))?;

        // Unique by construction (pid + process-wide nonce), so the path can
        // be left free for the zero-copy ingests below.
        let pid = std::process::id();
        let tmp = free_staging_path(|nonce| dir.join(format!("stage-{pid}-{nonce}.tmp")))
            .with_context(|| format!("reserving a staging name in {}", dir.display()))?;

        let stage = |tmp: &Path, allow_hardlink: bool| -> Result<(StoreIngest, bool)> {
            let ingest = if crate::link::try_reflink(source, tmp).is_ok() {
                StoreIngest::Reflink
            } else if allow_hardlink
                && fs::symlink_metadata(source).is_ok_and(|m| m.file_type().is_file())
                && fs::hard_link(source, tmp).is_ok()
            {
                // Refused for symlink sources: hashing followed the link, but a
                // hardlink would link the symlink itself — a pointer into mutable
                // external state, never valid for a blob (same rule as
                // `materialize_blob`).
                StoreIngest::Hardlink
            } else {
                fs::copy(source, tmp)
                    .with_context(|| format!("copying {} into store staging", source.display()))?;
                StoreIngest::Copy
            };
            crate::atomic::fsync_file(tmp).context("flushing staged blob")?;
            let mut ro_guard_failed = false;
            if matches!(ingest, StoreIngest::Hardlink) && set_blob_readonly_checked(tmp).is_err() {
                // The guard is a correctness requirement on a shared inode; a
                // failure demotes to a full copy rather than publishing a
                // writable shared blob (same recovery as `materialize_blob`).
                ro_guard_failed = true;
            }
            Ok((ingest, ro_guard_failed))
        };

        match stage(&tmp, allow_hardlink) {
            Ok((ingest, false)) => Ok((tmp, ingest)),
            Ok((_ingest, true)) => {
                // Hardlink succeeded but the read-only guard did not. The temp
                // shares the source inode and we may have flipped it read-only:
                // discard the temp (clearing the shared RO bit), restore the
                // source writable if the blob never got published under it, and
                // restage as an independent copy.
                Self::drop_tmp_restore_source(source, &tmp);
                self.stage_blob_from_source(source, false).map_err(|_| {
                    anyhow::anyhow!("read-only guard failed on hardlinked staging temp")
                })
            }
            Err(first_err) => {
                unlink_blob(&tmp);
                Err(first_err)
            }
        }
    }

    /// Discard a hardlinked staging temp and undo any read-only bit it may have
    /// left on the shared source inode.
    fn drop_tmp_restore_source(source: &Path, tmp: &Path) {
        unlink_blob(tmp);
        // `restore_source_writable_if_unshared` already no-ops when the two
        // paths still share an inode; after the unlink they never do, so the
        // call is unconditional by construction.
        restore_source_writable_if_unshared(source, tmp);
    }

    /// Publish a staged snapshot onto its content-addressed path. Idempotent:
    /// when the blob already exists the staged file is discarded and `Ok(false)`
    /// is returned. The staged bytes are exactly what was hashed, so a rename
    /// onto `blob_path(hash)` can never contradict the recorded digest.
    fn publish_staged_blob(
        &self,
        staged: &Path,
        ingest: StoreIngest,
        hash: &str,
        size_bytes: u64,
    ) -> Result<bool> {
        let blob = self.blob_path(hash);
        if blob.is_file() {
            Self::discard_staged_blob(staged);
            return Ok(false);
        }
        fs::create_dir_all(blob.parent().unwrap()).context("creating blob shard directory")?;
        match fs::rename(staged, &blob) {
            Ok(()) => {}
            Err(e) if blob.is_file() => {
                // Concurrent winner published the identical-content blob first;
                // same digest means same bytes, so losing the race is benign.
                Self::discard_staged_blob(staged);
                let _ = e;
                return Ok(false);
            }
            Err(e) => {
                Self::discard_staged_blob(staged);
                return Err(e).context("publishing staged blob");
            }
        }
        let _ = crate::atomic::fsync_dir(blob.parent().unwrap());
        match ingest {
            StoreIngest::Reflink => crate::opcounts::record_store_reflinked(size_bytes),
            StoreIngest::Hardlink => crate::opcounts::record_store_hardlinked(size_bytes),
            StoreIngest::Copy => crate::opcounts::record_store_copied(size_bytes),
        }
        set_blob_readonly(&blob);
        Ok(true)
    }

    /// Discard a staging snapshot (best effort; the staging sweep reclaims any
    /// file this fails on).
    fn discard_staged_blob(staged: &Path) {
        unlink_blob(staged);
    }

    /// Phase-2 race recovery: if a concurrent remove unlinked this blob after
    /// phase 1, re-materialize it from the live source — but only under its
    /// recorded digest. The re-ingest reads the source, which may have been
    /// mutated since phase 1's snapshot; storing those bytes under the old
    /// address would poison the store, so a mismatch bails (rolling back the
    /// transaction) instead.
    fn rematerialize_and_verify(
        &self,
        source: &Path,
        hash: &str,
        store_name: &str,
        allow_hardlink: bool,
    ) -> Result<()> {
        let blob_path = self.blob_path(hash);
        if materialize_blob(source, &blob_path, allow_hardlink)? {
            let actual = crate::cache_key::hash_file(&blob_path)?;
            if actual != hash {
                anyhow::bail!(
                    "re-materialized blob for {} hashes to {} but entry records {}; \
                     refusing to commit",
                    store_name,
                    actual,
                    hash
                );
            }
        }
        Ok(())
    }

    /// Reclaim crash-orphaned staging files older than `min_age`. A put killed
    /// between staging and publish leaves its snapshot here; unlike an orphaned
    /// blob it has no DB row to consult, so age is the only liveness signal —
    /// see [`STAGING_SWEEP_GRACE`] for why every caller wants the same one.
    pub fn sweep_stale_staging(&self, min_age: Duration) -> OrphanSweepStats {
        let mut stats = OrphanSweepStats::default();
        let dir = self.staging_dir();
        let Ok(entries) = fs::read_dir(&dir) else {
            return stats;
        };
        let now = std::time::SystemTime::now();
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            if !meta.is_file() {
                continue;
            }
            let age_ok = meta
                .modified()
                .ok()
                .and_then(|m| now.duration_since(m).ok())
                .is_some_and(|age| age >= min_age);
            if !age_ok {
                continue;
            }
            stats.scanned += 1;
            let size = meta.len();
            // Staging temps may be hardlinked (and therefore read-only); clear
            // that before unlinking. Counted only when the file is really gone,
            // so Windows sharing violations don't over-claim reclaimed bytes.
            let removed = (|| -> std::io::Result<()> {
                let mut perms = meta.permissions();
                perms.set_readonly(false);
                fs::set_permissions(entry.path(), perms)?;
                fs::remove_file(entry.path())
            })()
            .is_ok();
            if removed {
                stats.removed += 1;
                stats.bytes_reclaimed += size;
            }
        }
        stats
    }

    /// Get the directory for a cache entry.
    pub fn entry_dir(&self, cache_key: &str) -> PathBuf {
        self.config.store_dir().join(cache_key)
    }

    /// Get the full path to a cached file (legacy entry-based layout).
    #[allow(dead_code)]
    pub fn cached_file_path(&self, cache_key: &str, filename: &str) -> PathBuf {
        self.entry_dir(cache_key).join(filename)
    }

    /// Calculate the total size of the store.
    pub fn total_size(&self) -> Result<u64> {
        let size: i64 =
            self.db
                .query_row("SELECT COALESCE(SUM(size), 0) FROM entries", [], |row| {
                    row.get(0)
                })?;
        Ok(size as u64)
    }

    /// Registered blob content bytes: `SUM(blobs.size)`, each deduplicated
    /// blob counted once. This — not [`Self::total_size`]'s logical
    /// per-entry sum — is what `max_size` bounds and what size pressure is
    /// measured against: the two diverge by exactly the dedup savings, which
    /// is largest in the cross-clone/worktree stores kache is aimed at
    /// (kunobi-ninja/kache#608). Not literally every byte under the cache
    /// dir: SQLite, meta.json files, and any blob whose best-effort unlink
    /// was deferred sit outside this sum.
    pub fn physical_size(&self) -> Result<u64> {
        let size: i64 =
            self.db
                .query_row("SELECT COALESCE(SUM(size), 0) FROM blobs", [], |row| {
                    row.get(0)
                })?;
        Ok(size as u64)
    }

    /// Get the number of entries in the store.
    pub fn entry_count(&self) -> Result<usize> {
        let count: i64 = self
            .db
            .query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0))?;
        Ok(count as usize)
    }

    /// Remember an incremental compilation directory seen by the wrapper.
    pub fn remember_incremental_dir(&self, path: &Path) -> Result<()> {
        let path = path.to_string_lossy().into_owned();
        self.db.execute(
            "INSERT OR REPLACE INTO incremental_dirs (path, last_seen) VALUES (?1, datetime('now'))",
            params![path],
        )?;
        Ok(())
    }

    /// Remove registered incremental directories and prune stale registry rows.
    pub fn clean_registered_incremental_dirs(&self) -> Result<usize> {
        let paths: Vec<String> = {
            let mut stmt = self
                .db
                .prepare("SELECT path FROM incremental_dirs ORDER BY last_seen ASC")?;
            stmt.query_map([], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?
        };

        let mut cleaned = 0;
        for path_str in paths {
            let path = PathBuf::from(&path_str);
            if !path.exists() {
                self.db.execute(
                    "DELETE FROM incremental_dirs WHERE path = ?1",
                    params![path_str],
                )?;
                continue;
            }

            if !path.is_dir() {
                tracing::warn!(
                    "registered incremental path is not a directory, pruning: {}",
                    path.display()
                );
                self.db.execute(
                    "DELETE FROM incremental_dirs WHERE path = ?1",
                    params![path_str],
                )?;
                continue;
            }

            match fs::remove_dir_all(&path) {
                Ok(()) => {
                    self.db.execute(
                        "DELETE FROM incremental_dirs WHERE path = ?1",
                        params![path_str],
                    )?;
                    cleaned += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        "failed to remove registered incremental dir {}: {}",
                        path.display(),
                        e
                    );
                }
            }
        }

        Ok(cleaned)
    }

    /// Materialize every entry's eviction-relevant features in one pass.
    ///
    /// Selection used to be three separate `SELECT`s embedded in three removal
    /// loops; it is now a pure function over these features
    /// (kunobi-ninja/kache#595). The size-pressure sweep already loaded every
    /// row, so this is the same I/O shape it always had.
    pub(crate) fn eviction_candidates(&self) -> Result<Vec<crate::eviction::EntryFeatures>> {
        let mut stmt = self.db.prepare(
            "SELECT cache_key, size, hit_count, content_hash, committed,
                    (julianday('now') - julianday(last_accessed)) * 24.0,
                    compile_time_ms,
                    (SELECT COALESCE(SUM(b.size), 0)
                       FROM entry_blobs eb JOIN blobs b ON b.hash = eb.hash
                      WHERE eb.cache_key = entries.cache_key
                        AND eb.refs = b.refcount),
                    EXISTS(SELECT 1 FROM entry_blobs eb2
                            WHERE eb2.cache_key = entries.cache_key)
             FROM entries",
        )?;
        let rows = stmt
            .query_map([], |row| {
                // Bytes this entry would actually free: blobs where it holds
                // every remaining reference (#608). Entries not yet backfilled
                // into entry_blobs report None and rank on logical size as
                // before.
                let has_blob_rows: bool = row.get(8)?;
                let reclaimable_bytes = if has_blob_rows {
                    Some(row.get::<_, i64>(7)?)
                } else {
                    None
                };
                Ok(crate::eviction::EntryFeatures {
                    key: row.get(0)?,
                    size: row.get(1)?,
                    hit_count: row.get(2)?,
                    content_hash: row.get(3)?,
                    committed: row.get(4)?,
                    // NULL/unparseable timestamps yield NULL from julianday();
                    // treat those as "just accessed" so a malformed row is
                    // never evicted ahead of a genuinely stale one.
                    idle_hours: row.get::<_, Option<f64>>(5)?.unwrap_or(0.0),
                    compile_time_ms: row.get(6)?,
                    reclaimable_bytes,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Cache keys whose local payload backs a durable upload intent.
    ///
    /// The spool file is the durability boundary: once `<key>.json` exists,
    /// every eviction policy must retain that entry until the upload path
    /// retires the file. Read errors abort the sweep rather than treating an
    /// unreadable spool as empty and destroying data needed for replay.
    fn durable_upload_keys(&self) -> Result<std::collections::HashSet<String>> {
        let dir = self.config.upload_spool_dir();
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(std::collections::HashSet::new());
            }
            Err(error) => {
                return Err(error).with_context(|| format!("reading {}", dir.display()));
            }
        };
        Self::durable_upload_keys_from_names(
            entries.map(|entry| entry.map(|entry| entry.file_name())),
            crate::config::UPLOAD_SPOOL_MAX_JOBS,
        )
        .with_context(|| format!("reading {}", dir.display()))
    }

    fn durable_upload_keys_from_names<I>(
        names: I,
        max_jobs: usize,
    ) -> Result<std::collections::HashSet<String>>
    where
        I: IntoIterator<Item = std::io::Result<std::ffi::OsString>>,
    {
        let mut keys = std::collections::HashSet::new();
        for (index, file_name) in names.into_iter().enumerate() {
            if index >= max_jobs {
                anyhow::bail!("upload spool exceeds {max_jobs} jobs; refusing eviction");
            }
            let file_name = file_name.context("reading upload spool entry")?;
            let Some(file_name) = file_name.to_str() else {
                continue;
            };
            let Some(key) = file_name.strip_suffix(".json") else {
                continue;
            };
            if crate::cache_key::is_valid_cache_key(key) {
                keys.insert(key.to_string());
            }
        }
        Ok(keys)
    }

    /// Remove a policy's selection, in order, under the active/durable pin guards.
    ///
    /// This is the *mechanism* half: the grace check, blob refcount decrement,
    /// and refuse-on-corrupt-meta guard all live in `remove_entry_guarded` and
    /// are deliberately not reachable from a policy. `stop_at` bounds a
    /// size-driven sweep; `None` removes everything selected.
    fn apply_eviction(
        &self,
        order: &[String],
        by_key: &std::collections::HashMap<&str, &crate::eviction::EntryFeatures>,
        policy: &str,
        stop_at: Option<(u64, u64)>,
        shadow: Option<&ShadowSelection>,
        durable_upload_keys: &std::collections::HashSet<String>,
    ) -> GcStats {
        let mut stats = GcStats::default();
        let (mut current_size, target) = match stop_at {
            Some((current, target)) => (current, Some(target)),
            None => (0, None),
        };

        for key in order {
            if let Some(target) = target
                && current_size <= target
            {
                break;
            }
            if durable_upload_keys.contains(key) {
                stats.entries_pinned += 1;
                tracing::debug!(
                    key = key.as_str(),
                    "gc: retaining entry that backs a durable upload intent"
                );
                continue;
            }
            let features = by_key.get(key.as_str()).copied();
            match self.remove_entry_guarded(key, Some(EVICTION_IDLE_GRACE)) {
                Ok(Some(reclaim)) => {
                    stats.entries_evicted += 1;
                    // Budget on bytes the removal *actually* freed on disk, not
                    // the entry's logical size: evicting an entry whose blobs
                    // are all shared frees nothing, and the sweep must keep
                    // going rather than stop believing it reached the target
                    // (#608).
                    stats.bytes_freed += reclaim.freed_bytes;
                    stats.blobs_removed += reclaim.blobs_unlinked;
                    current_size = current_size.saturating_sub(reclaim.freed_bytes);
                    // Telemetry, deliberately outside remove_entry_guarded so
                    // the removal mechanism stays free of it (#595). Recorded
                    // after the fact rather than in the delete transaction: a
                    // tombstone lost to a crash costs one observation, not
                    // correctness.
                    if let Some(f) = features {
                        let verdict = shadow.map(|s| (s.policy, s.victims.contains(key.as_str())));
                        self.record_tombstone(f, policy, verdict);
                    }
                }
                // Pinned by a recent access — a live build may be mid-restore
                // on it (kunobi-ninja/kache#326, #182) — or lost the removal
                // race to a concurrent remover. Leave it for next round, but
                // count it so the caller can say *why* nothing was evicted
                // instead of reporting a bare "0" (#509).
                Ok(None) => {
                    stats.entries_pinned += 1;
                    continue;
                }
                Err(e) => {
                    // A corrupt entry (unloadable meta.json) refuses removal to
                    // avoid leaking blob refcounts (#276); skip it and keep
                    // evicting the rest rather than aborting the whole sweep.
                    tracing::warn!("gc: skipping eviction of {key}: {e:#}");
                    continue;
                }
            }
        }
        stats
    }

    /// Run one eviction policy over the current store.
    ///
    /// `stop_at` is `Some((current_size, target))` for size-driven sweeps and
    /// `None` when the policy's whole selection should be removed.
    ///
    /// Size-driven sweeps are shadowed by the #594 value-density candidate:
    /// it ranks the same candidate set for the same byte budget, and each
    /// tombstone records whether it agreed — while the live policy alone
    /// decides what actually goes. The demand stream then compares the two
    /// on real reuse, the evidence step 5 of #594 is gated on.
    fn evict_with(
        &self,
        policy: &dyn crate::eviction::EvictionPolicy,
        stop_at: Option<(u64, u64)>,
    ) -> Result<GcStats> {
        let candidates = self.eviction_candidates()?;
        let order = policy.select(&candidates);
        if order.is_empty() {
            return Ok(GcStats::default());
        }
        // Rebuild cost about to be destroyed. The current policy does not
        // consider this when ranking (#594) — surfacing it is how we find out
        // whether that matters in practice, on real stores, before changing
        // any behavior. `0` for entries not yet backfilled.
        let selected: std::collections::HashSet<&str> = order.iter().map(|k| k.as_str()).collect();
        let cost_ms: i64 = candidates
            .iter()
            .filter(|e| selected.contains(e.key.as_str()))
            .map(|e| e.compile_time_ms)
            .sum();
        tracing::debug!(
            policy = policy.name(),
            candidates = candidates.len(),
            selected = order.len(),
            selected_compile_time_ms = cost_ms,
            "gc: eviction selection"
        );
        let shadow = stop_at.map(|(current, target)| {
            use crate::eviction::EvictionPolicy as _;
            let candidate = crate::eviction::ValueDensityPolicy;
            let shadow_order = candidate.select(&candidates);
            ShadowSelection {
                policy: candidate.name(),
                victims: crate::eviction::would_evict_for_budget(
                    &candidates,
                    &shadow_order,
                    current.saturating_sub(target),
                ),
            }
        });
        let by_key: std::collections::HashMap<&str, &crate::eviction::EntryFeatures> =
            candidates.iter().map(|e| (e.key.as_str(), e)).collect();
        let durable_upload_keys = self.durable_upload_keys()?;
        Ok(self.apply_eviction(
            &order,
            &by_key,
            policy.name(),
            stop_at,
            shadow.as_ref(),
            &durable_upload_keys,
        ))
    }

    /// Weighted eviction: remove entries with lowest priority score until under the size limit.
    /// Prefers evicting old, rarely-accessed entries that actually free bytes.
    ///
    /// Fires at `max_size` and evicts down to 90% of it — a real hysteresis
    /// band, not the single 90% line that used to serve as both trigger and
    /// target. The threshold lives here rather than at each call site so
    /// `kache gc`, the daemon's periodic sweep, and the post-upload check all
    /// get the same band (see [`crate::eviction::over_eviction_trigger`]).
    pub fn evict(&self) -> Result<GcStats> {
        let target = crate::eviction::eviction_target(self.config.max_size);
        // Trigger, budget, and stop condition are all physical bytes on disk
        // (`SUM(blobs.size)`), not the logical `SUM(entries.size)`: on a
        // dedup-heavy store the logical figure over-reports by exactly the
        // dedup savings, firing GC while the disk is comfortable and
        // destroying rebuild value without reclaiming space (#608).
        let size_before = self.physical_size()?;
        if !crate::eviction::over_eviction_trigger(size_before, self.config.max_size) {
            return Ok(GcStats::default());
        }

        // The ranking is computed once and walked while deleting: each entry's
        // score is independent of the others, so the order stays valid as rows
        // disappear. The walk subtracts the bytes each removal actually freed
        // (last-reference blobs), so the stop condition tracks the physical
        // store without re-querying. A removal that frees less than its
        // ranked `reclaimable_bytes` promised (a twin evicted earlier in the
        // same sweep) only makes the sweep continue longer — never stop early.
        self.evict_with(
            &crate::eviction::SizePressurePolicy,
            Some((size_before, target)),
        )
    }

    /// Evict entries older than the given duration.
    pub fn evict_older_than(&self, hours: u64) -> Result<GcStats> {
        self.evict_with(&crate::eviction::OlderThanPolicy { hours }, None)
    }

    /// Remove entries written by a different (or unknown legacy) cache-key
    /// recipe while retaining every entry from the running recipe.
    ///
    /// This is deliberately explicit rather than part of ordinary GC: rows
    /// created before key-schema recording use `0`, and an upgrade must not
    /// discard a still-reachable cache merely because its metadata predates
    /// this field. `kache gc --stale-schema` is the user's opt-in boundary.
    pub fn evict_stale_key_schemas(&self, current_schema: u32) -> Result<GcStats> {
        let keys = {
            let mut stmt = self.db.prepare(
                "SELECT cache_key FROM entries
                 WHERE committed = 1 AND key_schema != ?1
                 ORDER BY cache_key",
            )?;
            stmt.query_map(params![current_schema], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let candidates = self.eviction_candidates()?;
        let by_key = candidates
            .iter()
            .map(|entry| (entry.key.as_str(), entry))
            .collect::<std::collections::HashMap<_, _>>();
        let durable_upload_keys = self.durable_upload_keys()?;
        Ok(self.apply_eviction(
            &keys,
            &by_key,
            "stale_schema",
            None,
            None,
            &durable_upload_keys,
        ))
    }

    /// Evict duplicate entries that share the same content_hash.
    /// Keeps the most recently accessed entry for each content_hash group
    /// (consistent with LRU eviction policy).
    /// Returns GcStats with eviction metrics.
    ///
    /// Gated on the same size-pressure trigger as [`Self::evict`]. Reclaiming
    /// space is the only justification for spending a duplicate key's hit
    /// history, so a comfortable store declines the sweep. Once triggered,
    /// the same physical-byte target bounds this pass; the following ordinary
    /// size sweep recomputes pressure if duplicate removal was insufficient.
    pub fn evict_duplicate_entries(&self) -> Result<GcStats> {
        let size_before = self.physical_size()?;
        if !crate::eviction::over_eviction_trigger(size_before, self.config.max_size) {
            return Ok(GcStats {
                skipped: true,
                ..Default::default()
            });
        }
        self.evict_with(
            &crate::eviction::DuplicatePolicy,
            Some((
                size_before,
                crate::eviction::eviction_target(self.config.max_size),
            )),
        )
    }

    /// Reclaim orphaned blob files — content-addressed files on disk with no
    /// row in the `blobs` table. They accumulate when a crash interrupts a
    /// `put`/import between materialize (Phase 1) and the commit transaction
    /// (Phase 2), or when `remove_entry` runs against an entry whose
    /// `meta.json` is gone (so its blob hashes can't be decremented). Nothing
    /// else reclaims them: `evict*`/`remove_entry` only touch blobs reachable
    /// from an entry, and `total_size()` doesn't count them — so they leak
    /// invisibly to size-based eviction.
    ///
    /// Only blobs whose file mtime is older than `min_age` are swept, so a blob
    /// a concurrent `put` is materializing (it renames the file into place just
    /// before inserting its row) is never reclaimed out from under it. Unlinks
    /// run while holding the SQLite write lock (`BEGIN IMMEDIATE`), upholding
    /// the store invariant that a blob is only ever removed under that lock —
    /// so even if this races a `put` adopting a long-lived orphan, that put's
    /// Phase 2 re-materializes the blob before committing a reference to it.
    pub fn sweep_orphan_blobs(&self, min_age: Duration) -> Result<OrphanSweepStats> {
        let blobs_dir = self.config.store_dir().join("blobs");
        if !blobs_dir.exists() {
            return Ok(OrphanSweepStats::default());
        }

        // Phase A (no lock): enumerate blob-shaped files old enough to sweep.
        // The directory walk is the slow part and holds no lock.
        let now = std::time::SystemTime::now();
        let mut candidates: Vec<(String, PathBuf, u64)> = Vec::new();
        let mut scanned = 0usize;
        for shard in fs::read_dir(&blobs_dir)?.flatten() {
            if !shard.path().is_dir() {
                continue;
            }
            let Ok(files) = fs::read_dir(shard.path()) else {
                continue;
            };
            for file in files.flatten() {
                let path = file.path();
                let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                if !is_blob_hash_name(name) {
                    continue;
                }
                let Ok(meta) = file.metadata() else { continue };
                if !meta.is_file() {
                    continue;
                }
                scanned += 1;
                let old_enough = meta
                    .modified()
                    .ok()
                    .and_then(|m| now.duration_since(m).ok())
                    .map(|age| age >= min_age)
                    .unwrap_or(false);
                if old_enough {
                    candidates.push((name.to_string(), path, meta.len()));
                }
            }
        }

        let mut stats = OrphanSweepStats {
            scanned,
            ..Default::default()
        };
        if candidates.is_empty() {
            return Ok(stats);
        }

        // Phase B (write lock held): re-check each candidate against the live
        // `blobs` table and unlink the unreferenced ones. `BEGIN IMMEDIATE`
        // takes the write lock up front so the unlinks serialize with any
        // `put`/`remove_entry` mutating the same blob.
        self.db.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            let referenced: std::collections::HashSet<String> = {
                let mut stmt = self.db.prepare("SELECT hash FROM blobs")?;
                stmt.query_map([], |row| row.get::<_, String>(0))?
                    .filter_map(|r| r.ok())
                    .collect()
            };
            for (hash, path, size) in &candidates {
                if referenced.contains(hash) {
                    continue;
                }
                unlink_blob(path);
                stats.removed += 1;
                stats.bytes_reclaimed += *size;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.db.execute_batch("COMMIT")?;
                Ok(stats)
            }
            Err(e) => {
                let _ = self.db.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    /// Backfill content_hash for entries that don't have one.
    /// Reads meta.json from each entry to get file hashes.
    /// Returns the number of entries updated.
    pub fn backfill_content_hashes(&self) -> Result<usize> {
        let keys: Vec<String> = {
            let mut stmt = self.db.prepare(
                "SELECT cache_key FROM entries WHERE content_hash IS NULL AND committed = 1",
            )?;
            stmt.query_map([], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?
        };

        let mut updated = 0;
        for key in &keys {
            let meta_path = self.entry_dir(key).join("meta.json");
            if let Ok(content) = fs::read_to_string(&meta_path)
                && let Ok(meta) = serde_json::from_str::<EntryMeta>(&content)
            {
                let content_hash = compute_content_hash(&meta.files);
                self.db.execute(
                    "UPDATE entries SET content_hash = ?1 WHERE cache_key = ?2",
                    params![content_hash, key],
                )?;
                updated += 1;
            }
        }
        Ok(updated)
    }

    /// Backfill `compile_time_ms` for entries written before it was indexed
    /// (kunobi-ninja/kache#594), reading each entry's `meta.json` — the same
    /// shape as [`Self::backfill_content_hashes`], and run from the same GC
    /// sweep.
    ///
    /// Only rows still at the `0` default are touched, so this converges: once
    /// an entry is backfilled it is never re-read. A genuinely zero-cost
    /// compile is indistinguishable from "not yet backfilled" here, which is
    /// harmless — it just gets re-read on the next sweep and stays 0.
    ///
    /// Bounded to [`COMPILE_TIME_BACKFILL_BATCH`] entries per call. Measured on
    /// a real 52k-entry store, an unbounded pass is ~6 s of `meta.json` reads —
    /// and this runs inside the daemon's GC sweep while the store mutex is
    /// held, so a first-GC-after-upgrade stall of that size is worth avoiding.
    /// Spreading it over successive sweeps costs nothing: eviction ranking
    /// treats a not-yet-backfilled entry exactly as it does today.
    pub fn backfill_compile_times(&self) -> Result<usize> {
        self.backfill_compile_times_limited(COMPILE_TIME_BACKFILL_BATCH)
    }

    /// Backfill `entry_blobs` rows for entries written before the table
    /// existed (kunobi-ninja/kache#608), reading each entry's `meta.json` —
    /// the same shape and GC-sweep call site as
    /// [`Self::backfill_compile_times`], and bounded the same way so a
    /// first-GC-after-upgrade never stalls on a 50k-entry store.
    ///
    /// Converges: an entry gains rows once and is never re-read. Eviction
    /// ranks a not-yet-backfilled entry on its logical size, exactly as it
    /// did before the table existed. Entries whose meta.json is unreadable
    /// (or lists no files) can never gain rows and stay in the pre-#608
    /// ranking regime; they are the same entries `remove_entry` already
    /// refuses to touch (#276). Selection is randomized so a batch of such
    /// entries cannot permanently starve the valid keys behind it.
    pub fn backfill_entry_blobs(&self) -> Result<usize> {
        self.backfill_entry_blobs_limited(COMPILE_TIME_BACKFILL_BATCH)
    }

    /// [`Self::backfill_entry_blobs`] with an explicit per-call bound.
    fn backfill_entry_blobs_limited(&self, limit: i64) -> Result<usize> {
        let keys: Vec<String> = {
            let mut stmt = self.db.prepare(
                "SELECT cache_key FROM entries
                 WHERE committed = 1
                   AND cache_key NOT IN (SELECT cache_key FROM entry_blobs)
                 ORDER BY RANDOM()
                 LIMIT ?1",
            )?;
            stmt.query_map(params![limit], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?
        };

        let mut updated = 0;
        for key in &keys {
            let meta_path = self.entry_dir(key).join("meta.json");
            if let Ok(content) = fs::read_to_string(&meta_path)
                && let Ok(meta) = serde_json::from_str::<EntryMeta>(&content)
                && !meta.files.is_empty()
            {
                let tx = self.db.unchecked_transaction()?;
                // Re-check under the write lock: a concurrent put/import may
                // have registered this entry's rows since the SELECT above —
                // and the entry row must still exist, or a concurrent removal
                // would leave a ghost mapping for a dead entry.
                let still_wanted: i64 = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM entries WHERE cache_key = ?1)
                            AND NOT EXISTS(SELECT 1 FROM entry_blobs WHERE cache_key = ?1)",
                    params![key],
                    |row| row.get(0),
                )?;
                if still_wanted != 0 {
                    record_entry_blobs(&tx, key, &meta.files)?;
                    updated += 1;
                }
                tx.commit()?;
            }
        }
        Ok(updated)
    }

    /// [`Self::backfill_compile_times`] with an explicit per-call bound, so the
    /// batching behavior can be tested without materializing a batch-sized
    /// store.
    fn backfill_compile_times_limited(&self, limit: i64) -> Result<usize> {
        let keys: Vec<String> = {
            let mut stmt = self.db.prepare(
                "SELECT cache_key FROM entries WHERE compile_time_ms = 0 AND committed = 1
                 LIMIT ?1",
            )?;
            stmt.query_map(params![limit], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?
        };

        let mut updated = 0;
        for key in &keys {
            let meta_path = self.entry_dir(key).join("meta.json");
            if let Ok(content) = fs::read_to_string(&meta_path)
                && let Ok(meta) = serde_json::from_str::<EntryMeta>(&content)
                && meta.compile_time_ms > 0
            {
                self.db.execute(
                    "UPDATE entries SET compile_time_ms = ?1 WHERE cache_key = ?2",
                    params![meta.compile_time_ms as i64, key],
                )?;
                updated += 1;
            }
        }
        Ok(updated)
    }

    /// Record that an entry was evicted, with the features the decision was
    /// made on (kunobi-ninja/kache#594). `shadow` carries the shadow policy's
    /// verdict on the same entry — `(policy_name, it_would_evict_this_too)` —
    /// so later demand on the key splits by whether the candidate policy
    /// agreed with the live one.
    ///
    /// Best-effort: telemetry must never fail or slow an eviction, so errors
    /// are logged at debug and swallowed.
    fn record_tombstone(
        &self,
        features: &crate::eviction::EntryFeatures,
        policy: &str,
        shadow: Option<(&str, bool)>,
    ) {
        let result = self.db.execute(
            "INSERT OR REPLACE INTO eviction_tombstones
                (cache_key, evicted_at, policy, size, hit_count, idle_hours, compile_time_ms,
                 demanded_at, shadow_policy, shadow_would_evict)
             VALUES (?1, datetime('now'), ?2, ?3, ?4, ?5, ?6, NULL, ?7, ?8)",
            params![
                features.key,
                policy,
                features.size,
                features.hit_count,
                features.idle_hours,
                features.compile_time_ms,
                shadow.map(|(name, _)| name),
                shadow.map(|(_, would)| would),
            ],
        );
        if let Err(e) = result {
            tracing::debug!("gc: could not record tombstone: {e}");
        }
    }

    /// Note that a key was requested after being evicted — the observation the
    /// live store cannot provide, since the entries it evicted are precisely
    /// the ones missing from it (kunobi-ninja/kache#594).
    ///
    /// Sits on the cache-miss path, so the common case (a key that was never
    /// cached at all) must stay read-only: the existence probe is a primary-key
    /// lookup, and only a hit on a not-yet-demanded tombstone takes the write.
    /// Only the *first* demand is recorded — that is the interval the reuse
    /// question is about.
    fn note_tombstone_demand(&self, cache_key: &str) {
        let pending: Result<i64, _> = self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM eviction_tombstones
                           WHERE cache_key = ?1 AND demanded_at IS NULL)",
            params![cache_key],
            |row| row.get(0),
        );
        if !matches!(pending, Ok(1)) {
            return;
        }
        let updated = self.db.execute(
            "UPDATE eviction_tombstones SET demanded_at = datetime('now')
             WHERE cache_key = ?1 AND demanded_at IS NULL",
            params![cache_key],
        );
        match updated {
            Ok(_) => tracing::debug!(
                cache_key = &cache_key[..16.min(cache_key.len())],
                "gc: evicted entry was demanded again"
            ),
            Err(e) => tracing::debug!("gc: could not record tombstone demand: {e}"),
        }
    }

    /// Drop tombstones older than `keep_days`, bounding the table.
    ///
    /// Run from the GC sweep. A tombstone's value is the demand signal in the
    /// window after eviction; past that it is only taking up space.
    pub fn prune_tombstones(&self, keep_days: u64) -> Result<usize> {
        let removed = self.db.execute(
            "DELETE FROM eviction_tombstones WHERE evicted_at < datetime('now', ?1)",
            params![format!("-{keep_days} days")],
        )?;
        Ok(removed)
    }

    /// `(tracked, demanded)` — how many evictions are being observed, and how
    /// many of those keys were later asked for again.
    ///
    /// The ratio is the headline number for #594: a high rate means eviction is
    /// discarding entries the build still wants.
    pub fn tombstone_stats(&self) -> Result<(usize, usize)> {
        let row = self.db.query_row(
            "SELECT COUNT(*), COUNT(demanded_at) FROM eviction_tombstones",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )?;
        Ok((row.0.max(0) as usize, row.1.max(0) as usize))
    }

    /// Post-eviction demand split by the shadow policy's verdict
    /// (kunobi-ninja/kache#594): of the entries the live policy evicted, how
    /// often was each cohort — "shadow agreed" vs "shadow would have kept" —
    /// later asked for again? A markedly higher demand rate on the
    /// would-have-kept cohort flags live-policy mistakes the shadow avoids.
    /// Both cohorts come from the same evicted population, so the comparison
    /// avoids the inventory-value circularity the issue warns about.
    ///
    /// This is a **live-victim diagnostic**, not flip evidence on its own:
    /// the shadow's own victims that the live policy KEPT are invisible here
    /// (their reuse shows up only as ordinary hits), rates are right-censored
    /// by tombstone age, and a flip decision needs the cost-weighted
    /// objective, not raw demand counts. Rows whose `compile_time_ms` is
    /// still 0 are recorded but excluded from the headline numbers: the
    /// density shadow ranks unknown-cost entries as worthless by
    /// construction, and freshness correlates with not-yet-backfilled, so
    /// counting them would bias the kept cohort with young, high-demand
    /// keys.
    pub fn shadow_demand_split(&self) -> Result<ShadowDemandSplit> {
        let row = self.db.query_row(
            "SELECT
                COUNT(CASE WHEN shadow_would_evict = 1 THEN 1 END),
                COUNT(CASE WHEN shadow_would_evict = 1 AND demanded_at IS NOT NULL THEN 1 END),
                COUNT(CASE WHEN shadow_would_evict = 0 THEN 1 END),
                COUNT(CASE WHEN shadow_would_evict = 0 AND demanded_at IS NOT NULL THEN 1 END)
             FROM eviction_tombstones
             WHERE shadow_policy = 'value-density' AND compile_time_ms > 0",
            [],
            |row| {
                Ok(ShadowDemandSplit {
                    agreed: row.get::<_, i64>(0)?.max(0) as usize,
                    agreed_demanded: row.get::<_, i64>(1)?.max(0) as usize,
                    shadow_kept: row.get::<_, i64>(2)?.max(0) as usize,
                    shadow_kept_demanded: row.get::<_, i64>(3)?.max(0) as usize,
                })
            },
        )?;
        Ok(row)
    }

    /// Remove a single cache entry (files + DB record).
    ///
    /// The entry row, its blob refcounts, **and** the unlink of any blob whose
    /// last reference is gone all happen inside one transaction. Because a blob
    /// file is only ever mutated while holding the SQLite write lock (here, and
    /// in `put`/`import`'s materialize step), the unlink can't race a concurrent
    /// adopter: either we run first (the adopter re-materializes the file under
    /// the same lock) or it runs first (our decrement won't reach zero).
    pub fn remove_entry(&self, cache_key: &str) -> Result<()> {
        self.remove_entry_guarded(cache_key, None).map(|_| ())
    }

    /// Like [`remove_entry`](Self::remove_entry), but when `skip_if_idle_lt` is
    /// `Some(grace)` the removal is abandoned — returning `Ok(false)` without
    /// touching the DB or any blob — if the entry was last accessed within
    /// `grace` of now.
    ///
    /// This is the active-pin guard for eviction (kunobi-ninja/kache#326, #182):
    /// a cache hit bumps `last_accessed` (`get`, store.rs) right before the
    /// wrapper hardlinks the entry's blobs into the build, so a "recently
    /// accessed" entry is one a live build may be **mid-restore** on. The
    /// recency check runs INSIDE the same write-locked transaction that unlinks
    /// the blobs, so it serializes against that `last_accessed` bump: either the
    /// bump commits first (and we skip the eviction), or we delete first (and
    /// the racing restore reads a now-gone blob → ENOENT → clean recompile,
    /// never a false hit). Returns `Ok(Some(_))` when this call removed the
    /// entry, with the *physical* bytes and blob files actually reclaimed —
    /// zero when every blob is still referenced by another entry (#608).
    ///
    /// `None` (the plain `remove_entry` path) always removes — explicit purge /
    /// `doctor` must not be blocked by recency.
    ///
    /// Concurrent same-key *publication* is guarded too (#670): the entry's
    /// references are decremented only if `meta.json` is byte-identical, under
    /// the write transaction, to what was read before it — a republication in
    /// between rolls the removal back — and a remover that deleted no row
    /// never touches the entry directory, since a fresh `meta.json` there may
    /// belong to a publisher whose row registration has not committed yet.
    fn remove_entry_guarded(
        &self,
        cache_key: &str,
        skip_if_idle_lt: Option<Duration>,
    ) -> Result<Option<RemovalReclaim>> {
        self.remove_entry_guarded_with_hook(cache_key, skip_if_idle_lt, || {})
    }

    /// [`Self::remove_entry_guarded`] with a test seam: `after_meta_read` runs
    /// between the pre-transaction `meta.json` read and the write transaction,
    /// which is exactly the window the #670 republication guard defends.
    fn remove_entry_guarded_with_hook(
        &self,
        cache_key: &str,
        skip_if_idle_lt: Option<Duration>,
        after_meta_read: impl FnOnce(),
    ) -> Result<Option<RemovalReclaim>> {
        self.remove_entry_guarded_with_hooks(cache_key, skip_if_idle_lt, after_meta_read, || {})
    }

    /// [`Self::remove_entry_guarded_with_hook`] with a second seam:
    /// `before_dir_cleanup` runs inside the cleanup transaction — after the
    /// logical removal has committed, holding the write lock, immediately
    /// before the republication check and directory removal. That is the
    /// residual #670 window where a publisher's fresh `meta.json` used to be
    /// deleted out from under its registration.
    fn remove_entry_guarded_with_hooks(
        &self,
        cache_key: &str,
        skip_if_idle_lt: Option<Duration>,
        after_meta_read: impl FnOnce(),
        before_dir_cleanup: impl FnOnce(),
    ) -> Result<Option<RemovalReclaim>> {
        // Boxed so the republication-retry loop below stays non-generic; the
        // production closures are zero-sized, so no allocation happens.
        let mut after_meta_read: Option<Box<dyn FnOnce() + '_>> = Some(Box::new(after_meta_read));
        let mut before_dir_cleanup: Option<Box<dyn FnOnce() + '_>> =
            Some(Box::new(before_dir_cleanup));
        loop {
            match self.remove_entry_attempt(
                cache_key,
                skip_if_idle_lt,
                after_meta_read.take(),
                before_dir_cleanup.take(),
            )? {
                RemovalAttempt::Done(outcome) => return Ok(outcome),
                // A republication landed while this attempt waited out a
                // concurrent removal: the row belongs to a fresh generation
                // whose meta is back. The caller asked to remove whatever is
                // currently published, so run again against the new
                // generation. Each pass requires another full republication
                // inside the window, so this cannot spin on its own.
                RemovalAttempt::Republished => {}
            }
        }
    }

    fn remove_entry_attempt(
        &self,
        cache_key: &str,
        skip_if_idle_lt: Option<Duration>,
        after_meta_read: Option<Box<dyn FnOnce() + '_>>,
        before_dir_cleanup: Option<Box<dyn FnOnce() + '_>>,
    ) -> Result<RemovalAttempt> {
        let entry_dir = self.entry_dir(cache_key);
        let meta_path = entry_dir.join("meta.json");

        // Load the blob hashes this entry references. If `meta.json` exists but
        // can't be read or parsed, we CANNOT know which blobs to decrement —
        // deleting the entry row anyway permanently orphans those refcounts (the
        // blobs keep their DB row and evade size-based eviction forever). Refuse
        // the removal so a corrupt entry never silently leaks (#276); callers
        // (GC / purge / `doctor --repair`) log and move on, and the entry stays
        // accounted-for until a fresh `put` (INSERT OR REPLACE) overwrites it.
        let meta_content: String;
        let hashes: Vec<String> = match fs::read_to_string(&meta_path) {
            Ok(content) => {
                let meta: EntryMeta = serde_json::from_str(&content).with_context(|| {
                    format!(
                        "entry {cache_key}: meta.json unparseable — refusing removal so blob \
                         refcounts are not leaked (#276)"
                    )
                })?;
                meta_content = content;
                meta.files.iter().map(|f| f.hash.clone()).collect()
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // No meta.json. A same-key operation may be mid-flight: a
                // publisher materializes meta inside its registration
                // transaction, and a removal's cleanup pass runs in its own
                // locked transaction (#670) — so "meta missing, row present"
                // can be a healthy transient, not only the stranded-entry
                // shape. Bounce off the write lock — the no-op write statement
                // waits (busy_timeout) until any in-flight writer commits or
                // rolls back — then judge the settled state. Both the row and
                // the meta are checked while the lock is still held: after
                // dropping it another writer could move the pairing again and
                // a healthy already-absent state would misreport as #276
                // corruption.
                let tx = self.db.unchecked_transaction()?;
                tx.execute("UPDATE entries SET cache_key = cache_key WHERE 1 = 0", [])?;
                let row_exists: i64 = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM entries WHERE cache_key = ?1)",
                    params![cache_key],
                    |row| row.get(0),
                )?;
                // fs::metadata, not Path::exists: exists() swallows every
                // error as false, and a permission failure must refuse like
                // the unreadable-meta arm below, not report already-absent.
                let meta_is_back = match fs::metadata(&meta_path) {
                    Ok(_) => true,
                    Err(e) => {
                        if e.kind() != std::io::ErrorKind::NotFound {
                            return Err(e).with_context(|| {
                                format!(
                                    "entry {cache_key}: checking republished meta.json — \
                                     refusing removal so blob refcounts are not leaked (#276)"
                                )
                            });
                        }
                        false
                    }
                };
                drop(tx);
                if row_exists == 0 {
                    // The concurrent removal won (or the key never existed);
                    // nothing left to remove.
                    return Ok(RemovalAttempt::Done(None));
                }
                if meta_is_back {
                    // A republication landed while we waited: the row belongs
                    // to a fresh generation whose meta is back. The caller
                    // asked to remove whatever is currently published, so
                    // retry against the new generation. Each retry requires
                    // another full republication in the window, so this
                    // cannot spin on its own.
                    return Ok(RemovalAttempt::Republished);
                }
                // Settled: a row with no meta.json. Its blob list is unknown
                // and deleting the row would leak the refcounts — refuse, so
                // a corrupt entry never silently leaks (#276).
                anyhow::bail!(
                    "entry {cache_key}: meta.json missing but DB row present — refusing \
                     removal so blob refcounts are not leaked (#276)"
                );
            }
            Err(e) => {
                return Err(e).with_context(|| {
                    format!(
                        "entry {cache_key}: reading meta.json — refusing removal so blob \
                         refcounts are not leaked (#276)"
                    )
                });
            }
        };

        if let Some(hook) = after_meta_read {
            hook();
        }

        let tx = self.db.unchecked_transaction()?;

        // Active-pin guard (kunobi-ninja/kache#326, #182): bail out — under
        // the write lock, before any decrement or unlink — if the entry was
        // accessed within the grace window. Serializes against `get`'s
        // `last_accessed` bump so an in-flight restore is never deleted out
        // from under itself. Dropping `tx` here rolls back (nothing ran yet).
        if let Some(grace) = skip_if_idle_lt {
            let recently_accessed: i64 = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM entries \
                     WHERE cache_key = ?1 AND last_accessed >= datetime('now', ?2))",
                params![cache_key, format!("-{} seconds", grace.as_secs())],
                |row| row.get(0),
            )?;
            if recently_accessed != 0 {
                return Ok(RemovalAttempt::Done(None));
            }
        }

        // Delete the entry row first. If rows_affected is 0, another remover
        // already released this entry's references; we skip the decrements so
        // two removers can never double-decrement a shared blob's refcount
        // and unlink a blob a live entry still points at (#510). This gate —
        // not `gc.lock` — is what makes concurrent removal safe; the lock is
        // defence in depth for bulk sweeps.
        let rows_affected = tx.execute(
            "DELETE FROM entries WHERE cache_key = ?1",
            params![cache_key],
        )?;

        // A remover that deleted no row releases nothing and must not touch
        // the directory either (#670): a fresh `meta.json` there may belong
        // to a publisher whose registration has not committed yet. Reporting
        // `None` also keeps callers (eviction stats, tombstones) from
        // double-counting one entry as two removals (#510).
        if rows_affected == 0 {
            return Ok(RemovalAttempt::Done(None));
        }

        // Republication guard (#670): the row just deleted may belong to a
        // NEWER publication than the meta.json this removal read its hash
        // list from — decrementing the old hashes against the new row's
        // refcounts corrupts the store. `put` materializes meta.json inside
        // its own registration transaction, so under the write lock this
        // transaction holds the pairing cannot move: any difference means a
        // republication won, and the removal rolls back untouched. A
        // meta.json that vanished or went corrupt in the window takes the
        // same rollback; the NEXT removal attempt reports it properly
        // through the #276 guards above.
        let still_ours = matches!(fs::read_to_string(&meta_path), Ok(now) if now == meta_content);
        if !still_ours {
            return Ok(RemovalAttempt::Done(None));
        }
        tx.execute(
            "DELETE FROM entry_blobs WHERE cache_key = ?1",
            params![cache_key],
        )?;
        // Decrement in the DB but defer every physical unlink to the cleanup
        // pass after commit: while this transaction can still roll back, the
        // blob files its refcounts describe must remain on disk.
        let mut reclaim = RemovalReclaim::default();
        let mut unlink = Vec::new();
        for hash in &hashes {
            tx.execute(
                "UPDATE blobs SET refcount = refcount - 1 WHERE hash = ?1",
                params![hash],
            )?;
            let row: Option<(i64, i64)> = tx
                .query_row(
                    "SELECT refcount, size FROM blobs WHERE hash = ?1",
                    params![hash],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .ok();
            if let Some((rc, size)) = row
                && rc <= 0
            {
                tx.execute("DELETE FROM blobs WHERE hash = ?1", params![hash])?;
                unlink.push((hash.clone(), size));
            }
        }

        // Commit the LOGICAL removal before touching the filesystem (#670).
        // SQLite can roll back SQL; it cannot restore a deleted meta.json or
        // an unlinked blob — so any structure that deletes files inside this
        // transaction turns a crash or commit failure after the deletions
        // into a committed row whose artifacts are gone, the exact phantom
        // this function exists to prevent. Committing first inverts every
        // crash window into the recoverable direction: a crash from here on
        // leaves at worst an unindexed directory (a later put or index
        // rebuild reclaims it) or orphaned blob files (the orphan sweep
        // reclaims those), never a live row without its files.
        tx.commit()?;

        // Cleanup pass: a second short transaction whose only purpose is the
        // write lock. Serializing the filesystem deletions against same-key
        // writers is what closes the original #670 window — an unlocked
        // cleanup could delete a meta.json that a publisher materialized
        // (inside its own registration transaction) between our commit above
        // and this pass.
        let cleanup_tx = self.db.unchecked_transaction()?;
        cleanup_tx.execute("UPDATE entries SET cache_key = cache_key WHERE 1 = 0", [])?;

        if let Some(hook) = before_dir_cleanup {
            hook();
        }

        // A publisher may have republished this key between the commit above
        // and this lock. The directory then belongs to the new generation:
        // leave it untouched. Its blob adoption also re-inserted any of our
        // zero-ref rows it needed, which the per-blob guard below observes.
        let republished: i64 = cleanup_tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM entries WHERE cache_key = ?1)",
            params![cache_key],
            |row| row.get(0),
        )?;

        if republished == 0 {
            // Remove the entry directory (just meta.json in new format, may
            // have artifacts in legacy entries). Windows can surface external
            // interference as delete-pending errors (sharing violations from
            // readers mid-hardlink), so on any error re-check whether the
            // directory is actually gone, with a brief bounded retry (worst
            // case 50ms of extra lock hold). A directory that persists past
            // the retries is a real failure (permissions, open handles): it
            // propagates (#510). The logical removal is already committed, so
            // the failure leaves only an unindexed directory — recoverable —
            // never a live row whose files are gone.
            if let Ok(entries) = fs::read_dir(&entry_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if let Ok(meta) = fs::metadata(&path) {
                        let mut perms = meta.permissions();
                        perms.set_readonly(false);
                        let _ = fs::set_permissions(&path, perms);
                    }
                }
            }
            let mut result = Ok(());
            for _ in 0..5 {
                result = match fs::remove_dir_all(&entry_dir) {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        // Benign exactly when the directory is gone: NotFound
                        // is the Unix shape of losing the race, and Windows
                        // surfaces a competitor's in-flight delete as
                        // delete-pending errors instead.
                        if !entry_dir.exists() { Ok(()) } else { Err(e) }
                    }
                };
                if result.is_ok() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            result.with_context(|| format!("entry {cache_key}: removing entry directory"))?;
        }

        // Unlink dead blobs under the write lock so a concurrent adopter
        // can't commit a reference to a file we're deleting — re-checked
        // per blob, because a publisher that won the lock between our two
        // transactions may have re-inserted some of the rows the first
        // transaction deleted. Only bytes whose last reference went away are
        // physically freed — that, not the entry's logical size, is what
        // eviction budgets on (#608).
        for (hash, size) in unlink {
            let readopted: i64 = cleanup_tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM blobs WHERE hash = ?1)",
                params![hash],
                |row| row.get(0),
            )?;
            if readopted == 0 {
                unlink_blob(&self.blob_path(&hash));
                reclaim.freed_bytes += size.max(0) as u64;
                reclaim.blobs_unlinked += 1;
            }
        }
        cleanup_tx.commit()?;
        Ok(RemovalAttempt::Done(Some(reclaim)))
    }

    /// Test-only: insert a bare committed entry row, for tests that stage a
    /// synthetic `meta.json` and need removal to own the directory (#670
    /// made directory cleanup conditional on owning the row).
    #[cfg(test)]
    pub(crate) fn insert_entry_row_for_test(&self, cache_key: &str) {
        self.db
            .execute(
                "INSERT OR REPLACE INTO entries (cache_key, crate_name, size, committed) \
                 VALUES (?1, 'test', 1, 1)",
                params![cache_key],
            )
            .expect("test entry row insert");
    }

    /// Test-only: backdate an entry's `last_accessed` (via a SQLite datetime
    /// modifier like `"-1 hour"`) so eviction tests can move an entry past the
    /// active-pin grace without sleeping (kunobi-ninja/kache#326).
    #[cfg(test)]
    pub(crate) fn set_last_accessed_for_test(&self, cache_key: &str, sql_modifier: &str) {
        self.db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', ?2) WHERE cache_key = ?1",
                params![cache_key, sql_modifier],
            )
            .unwrap();
    }

    /// Clear the entire store.
    pub fn clear(&self) -> Result<()> {
        let store_dir = self.config.store_dir();
        if store_dir.exists() {
            // Make everything writable recursively, then remove all subdirs
            for entry in fs::read_dir(&store_dir)?.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    Self::make_writable_recursive(&path);
                    let _ = fs::remove_dir_all(&path);
                }
            }
        }
        self.db.execute("DELETE FROM entries", [])?;
        self.db.execute("DELETE FROM entry_blobs", [])?;
        self.db.execute("DELETE FROM blobs", [])?;
        self.db.execute("DELETE FROM incremental_dirs", [])?;
        Ok(())
    }

    /// Recursively make all files in a directory writable so they can be deleted.
    fn make_writable_recursive(dir: &Path) {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    Self::make_writable_recursive(&path);
                } else if let Ok(meta) = fs::metadata(&path) {
                    let mut perms = meta.permissions();
                    perms.set_readonly(false);
                    let _ = fs::set_permissions(&path, perms);
                }
            }
        }
    }

    /// List all entries for display.
    pub fn list_entries(&self, sort_by: &str) -> Result<Vec<EntryInfo>> {
        let order_clause = match sort_by {
            "size" => "size DESC",
            "hits" => "hit_count DESC",
            "age" => "created_at ASC",
            _ => "crate_name ASC",
        };

        let mut stmt = self.db.prepare(&format!(
            "SELECT cache_key, crate_name, crate_type, profile, size, created_at, last_accessed, hit_count, content_hash FROM entries WHERE committed = 1 ORDER BY {order_clause}"
        ))?;

        let entries = stmt
            .query_map([], |row| {
                Ok(EntryInfo {
                    cache_key: row.get(0)?,
                    crate_name: row.get(1)?,
                    crate_type: row.get(2)?,
                    profile: row.get(3)?,
                    size: row.get::<_, i64>(4)? as u64,
                    created_at: row.get(5)?,
                    last_accessed: row.get(6)?,
                    hit_count: row.get::<_, i64>(7)? as u64,
                    content_hash: row.get(8)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(entries)
    }

    /// Migrate a single legacy entry's artifacts into the blob store.
    fn migrate_entry_to_blobs(&self, meta: &EntryMeta) -> Result<()> {
        let entry_dir = self.entry_dir(&meta.cache_key);
        for cached_file in &meta.files {
            let artifact_path = entry_dir.join(&cached_file.name);
            if !artifact_path.exists() {
                continue; // Already migrated
            }
            let blob = self.blob_path(&cached_file.hash);
            let blob_dir = blob.parent().unwrap();
            fs::create_dir_all(blob_dir)?;

            // Check if blob already exists
            let existing: Option<i64> = self
                .db
                .query_row(
                    "SELECT refcount FROM blobs WHERE hash = ?1",
                    params![cached_file.hash],
                    |row| row.get(0),
                )
                .ok();

            if existing.is_some() {
                // Blob exists — delete artifact, bump refcount
                if let Ok(m) = fs::metadata(&artifact_path) {
                    let mut perms = m.permissions();
                    perms.set_readonly(false);
                    let _ = fs::set_permissions(&artifact_path, perms);
                }
                fs::remove_file(&artifact_path)?;
                self.db.execute(
                    "UPDATE blobs SET refcount = refcount + 1 WHERE hash = ?1",
                    params![cached_file.hash],
                )?;
            } else {
                // New blob — rename artifact into blob store
                if let Ok(m) = fs::metadata(&artifact_path) {
                    let mut perms = m.permissions();
                    if !perms.readonly() {
                        perms.set_readonly(true);
                        fs::set_permissions(&artifact_path, perms)?;
                    }
                }
                fs::rename(&artifact_path, &blob)?;
                self.db.execute(
                    "INSERT OR IGNORE INTO blobs (hash, size, refcount) VALUES (?1, ?2, 1)",
                    params![cached_file.hash, cached_file.size as i64],
                )?;
                if self.db.changes() == 0 {
                    self.db.execute(
                        "UPDATE blobs SET refcount = refcount + 1 WHERE hash = ?1",
                        params![cached_file.hash],
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Bulk-migrate all legacy entries' artifacts into the blob store.
    pub fn migrate_to_blobs(&self, progress: impl Fn(usize, usize)) -> Result<MigrationStats> {
        let store_dir = self.config.store_dir();
        let mut stats = MigrationStats::default();

        let mut entry_dirs = Vec::new();
        if let Ok(entries) = fs::read_dir(&store_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() && path.file_name().is_some_and(|n| n != "blobs") {
                    let meta_path = path.join("meta.json");
                    if meta_path.exists() {
                        let has_artifacts = fs::read_dir(&path)
                            .into_iter()
                            .flatten()
                            .flatten()
                            .any(|e| e.file_name() != "meta.json");
                        if has_artifacts {
                            entry_dirs.push(path);
                        }
                    }
                }
            }
        }

        let total = entry_dirs.len();
        for (i, entry_dir) in entry_dirs.iter().enumerate() {
            progress(i, total);
            stats.entries_scanned += 1;

            let meta_path = entry_dir.join("meta.json");
            let content = match fs::read_to_string(&meta_path) {
                Ok(c) => c,
                Err(_) => {
                    stats.entries_skipped += 1;
                    continue;
                }
            };
            let meta: EntryMeta = match serde_json::from_str(&content) {
                Ok(m) => m,
                Err(_) => {
                    stats.entries_skipped += 1;
                    continue;
                }
            };

            match self.migrate_entry_to_blobs(&meta) {
                Ok(()) => stats.entries_migrated += 1,
                Err(_) => stats.entries_skipped += 1,
            }
        }

        progress(total, total);
        Ok(stats)
    }

    fn is_lock_stale(&self, lock_path: &Path) -> Result<bool> {
        let content = fs::read_to_string(lock_path).unwrap_or_default();
        if let Ok(pid) = content.trim().parse::<u32>() {
            // Check if the process is still alive
            if !crate::platform::is_process_alive(pid) {
                return Ok(true); // Process doesn't exist
            }
            // Check if lock file is older than 1 hour (safety net)
            if let Ok(meta) = fs::metadata(lock_path)
                && let Ok(age) = meta.modified()?.elapsed()
                && age > std::time::Duration::from_secs(3600)
            {
                return Ok(true);
            }
            Ok(false)
        } else {
            Ok(true) // Can't parse PID, consider stale
        }
    }

    /// Return content-dedup statistics: unique blobs, physical vs logical size.
    pub fn blob_stats(&self) -> Result<BlobStats> {
        let total_blobs: i64 = self
            .db
            .query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get(0))?;
        let total_blob_size: i64 =
            self.db
                .query_row("SELECT COALESCE(SUM(size), 0) FROM blobs", [], |row| {
                    row.get(0)
                })?;
        let total_logical_size: i64 =
            self.db
                .query_row("SELECT COALESCE(SUM(size), 0) FROM entries", [], |row| {
                    row.get(0)
                })?;
        Ok(BlobStats {
            total_blobs: total_blobs as usize,
            total_blob_size: total_blob_size as u64,
            total_logical_size: total_logical_size as u64,
            savings: (total_logical_size as u64).saturating_sub(total_blob_size as u64),
        })
    }
}

/// Content-dedup statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct BlobStats {
    pub total_blobs: usize,
    pub total_blob_size: u64,
    pub total_logical_size: u64,
    pub savings: u64,
}

/// Statistics from a blob migration run.
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct MigrationStats {
    pub entries_scanned: usize,
    pub entries_migrated: usize,
    pub entries_skipped: usize,
    pub blobs_created: usize,
    pub blobs_reused: usize,
    pub bytes_saved: u64,
}

#[derive(Debug, Clone)]
pub struct EntryInfo {
    pub cache_key: String,
    pub crate_name: String,
    pub crate_type: String,
    pub profile: String,
    pub size: u64,
    pub created_at: String,
    pub last_accessed: String,
    pub hit_count: u64,
    pub content_hash: Option<String>,
}

#[cfg(test)]
mod tests {

    /// `0` and negatives mean "not recorded", not a measured zero
    /// (kunobi-ninja/kache#617). Load-bearing: the `size` and
    /// `compile_time_ms` columns default to 0 for rows written before their
    /// migrations, and a 0 read as a measurement would rank an un-backfilled
    /// entry as free to fetch and worthless to have.
    #[test]
    fn test_positive_or_none_treats_non_positive_as_unknown() {
        assert_eq!(positive_or_none(0), None, "0 is unknown, not Some(0)");
        assert_eq!(positive_or_none(-1), None, "a negative is unknown");
        assert_eq!(
            positive_or_none(1),
            Some(1),
            "the smallest real value survives"
        );
        assert_eq!(positive_or_none(4200), Some(4200));
        assert_eq!(positive_or_none(i64::MAX), Some(i64::MAX as u64));
    }

    use super::*;
    use crate::eviction::EvictionPolicy as _;

    #[test]
    fn readonly_regular_metadata_requires_both_properties() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("artifact");
        fs::write(&file, b"artifact").unwrap();

        assert!(!metadata_is_readonly_regular(&fs::metadata(&file).unwrap()));

        let mut permissions = fs::metadata(&file).unwrap().permissions();
        permissions.set_readonly(true);
        fs::set_permissions(&file, permissions).unwrap();
        assert!(metadata_is_readonly_regular(&fs::metadata(&file).unwrap()));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let readonly_dir = dir.path().join("readonly-dir");
            fs::create_dir(&readonly_dir).unwrap();
            fs::set_permissions(&readonly_dir, fs::Permissions::from_mode(0o555)).unwrap();
            assert!(!metadata_is_readonly_regular(
                &fs::metadata(&readonly_dir).unwrap()
            ));
        }
    }

    // Regression guard for #324: the local content-dedup hash must distinguish
    // entries that the old (bare-hash, 16-hex-truncated) fold could collide —
    // otherwise `evict_duplicate_entries` keeps the wrong survivor.
    #[test]
    fn content_hash_distinguishes_transposition_exec_bit_and_is_order_independent() {
        let cf = |name: &str, hash: &str, executable: bool| CachedFile {
            name: name.to_string(),
            size: 10,
            hash: hash.to_string(),
            executable,
        };

        // Same multiset of blob hashes, but the (name -> hash) mapping is swapped:
        // the old hash-only fold collided these; the new fold must not.
        let a = vec![cf("a.rlib", "H1", false), cf("b.rlib", "H2", false)];
        let swapped = vec![cf("a.rlib", "H2", false), cf("b.rlib", "H1", false)];
        assert_ne!(
            compute_content_hash(&a),
            compute_content_hash(&swapped),
            "a name<->hash transposition must change the content hash"
        );

        // Identical names/hashes/sizes; only which file is executable differs.
        let exec_a = vec![cf("a.rlib", "H1", true), cf("b.rlib", "H2", false)];
        let exec_b = vec![cf("a.rlib", "H1", false), cf("b.rlib", "H2", true)];
        assert_ne!(
            compute_content_hash(&exec_a),
            compute_content_hash(&exec_b),
            "moving the exec-bit to a different file must change the content hash"
        );

        // Deterministic and independent of input order.
        let reordered = vec![cf("b.rlib", "H2", false), cf("a.rlib", "H1", false)];
        assert_eq!(
            compute_content_hash(&a),
            compute_content_hash(&reordered),
            "content hash must not depend on file order"
        );
    }

    /// Which stored filenames may share an inode with the store blob on
    /// insert. Mirrors the restore-side `link_strategy` split, minus the
    /// insert-only exclusions documented on `hardlink_eligible`.
    #[test]
    fn hardlink_eligibility_mirrors_restore_strategy_with_insert_exclusions() {
        // On Windows the gate additionally requires the `windows_hardlink`
        // opt-in, which is off in tests — eligibility is all-false there.
        let gate_open = !cfg!(windows);

        // Immutable kinds the restore side hardlinks: eligible.
        for name in [
            "libserde-abc123.rlib",
            "libserde-abc123.rmeta",
            "foo.rcgu.o",
            "foo.obj",
            "foo.dwo",
        ] {
            assert_eq!(
                hardlink_eligible(name, false),
                gate_open,
                "{name} should be hardlink-eligible on insert (behind the Windows gate)"
            );
        }

        // Mutable kinds (Copy strategy on restore): never eligible.
        assert!(!hardlink_eligible("libfoo.dylib", false));
        assert!(!hardlink_eligible("libfoo.so", false));
        assert!(!hardlink_eligible("foo.exe", false));

        // Insert-only exclusions: `.d` is rewritten in place after `put`
        // (Expand), extensionless names are bin executables by rustc's Unix
        // convention, and an executable mode bit wins over the filename.
        assert!(!hardlink_eligible("serde-abc123.d", false));
        assert!(!hardlink_eligible("my-binary", false));
        assert!(!hardlink_eligible("libserde-abc123.rlib", true));
    }

    #[test]
    fn source_hardlink_policy_honors_independent_storage() {
        assert!(!source_hardlink_allowed(false, "foo.o", false));
        assert_eq!(
            source_hardlink_allowed(true, "foo.o", false),
            hardlink_eligible("foo.o", false)
        );
    }

    #[cfg(unix)]
    #[test]
    fn independent_put_never_hardlinks_or_marks_source_readonly() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let output = dir.path().join("compiler-output.o");
        fs::write(&output, b"independent cc artifact").unwrap();
        fs::set_permissions(&output, fs::Permissions::from_mode(0o660)).unwrap();

        store
            .put_with_compile_time_independent(
                "cc-independent",
                "foo.c",
                &[],
                &[],
                "x86_64-unknown-linux-gnu",
                "",
                &[(output.clone(), "foo.o".to_string())],
                "",
                "",
                1,
            )
            .unwrap();

        let meta = store.get("cc-independent").unwrap().unwrap();
        let blob = store.blob_path(&meta.files[0].hash);
        let output_meta = fs::metadata(&output).unwrap();
        let blob_meta = fs::metadata(&blob).unwrap();
        assert_eq!(output_meta.permissions().mode() & 0o777, 0o660);
        assert!(!output_meta.permissions().readonly());
        assert_ne!(
            (output_meta.dev(), output_meta.ino()),
            (blob_meta.dev(), blob_meta.ino()),
            "independent ingest must never share the compiler output inode"
        );
        assert!(blob_meta.permissions().readonly());
    }

    /// A mutable-kind blob must never share an inode with the build's output:
    /// mutating the output post-put (codesigning, stripping) must not be able
    /// to reach the content-addressed blob. Deterministic on every
    /// filesystem — reflink yields an independent inode, and the copy
    /// fallback trivially does; only a hardlink would fail this.
    #[cfg(unix)]
    #[test]
    fn put_keeps_mutable_kind_blobs_inode_independent_from_the_source() {
        use std::os::unix::fs::MetadataExt;

        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let source = dir.path().join("libfoo.dylib");
        fs::write(&source, b"dylib bytes").unwrap();

        store
            .put(
                "key-dylib",
                "foo",
                &["dylib".to_string()],
                &[],
                "host",
                "dev",
                &[(source.clone(), "libfoo.dylib".to_string())],
                "",
                "",
            )
            .unwrap();

        let hash = crate::cache_key::hash_file(&source).unwrap();
        let blob = store.blob_path(&hash);
        assert_ne!(
            fs::metadata(&blob).unwrap().ino(),
            fs::metadata(&source).unwrap().ino(),
            "a mutable-kind blob must not share an inode with the build output"
        );
    }

    /// Contract for immutable-kind ingest: the blob's content matches, the
    /// blob is read-only, and IF the filesystem fell back to a hardlink
    /// (no CoW — e.g. ext4 in CI) the build's own output is now the same
    /// read-only inode, exactly the state a warm restore leaves behind.
    /// Which zero-copy mechanism ran is filesystem-dependent, so the test
    /// asserts the contract, not the mechanism.
    #[cfg(unix)]
    #[test]
    fn put_ingests_immutable_kinds_zero_copy_where_the_filesystem_allows() {
        use std::os::unix::fs::MetadataExt;

        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let source = dir.path().join("libfoo-abc.rlib");
        fs::write(&source, b"rlib bytes").unwrap();

        store
            .put(
                "key-rlib",
                "foo",
                &["rlib".to_string()],
                &[],
                "host",
                "dev",
                &[(source.clone(), "libfoo-abc.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        let hash = crate::cache_key::hash_file(&source).unwrap();
        let blob = store.blob_path(&hash);
        assert_eq!(fs::read(&blob).unwrap(), b"rlib bytes");
        assert!(
            fs::metadata(&blob).unwrap().permissions().readonly(),
            "store blob must be read-only"
        );
        if fs::metadata(&blob).unwrap().ino() == fs::metadata(&source).unwrap().ino() {
            // Hardlink fallback ran: the source shares the blob's inode and
            // therefore its read-only mode — the same state a warm restore
            // produces, handled by the pre-compile read-only clean.
            assert!(
                fs::metadata(&source).unwrap().permissions().readonly(),
                "a hardlinked source must carry the blob's read-only mode"
            );
        }
    }

    /// A symlinked source must never produce a symlink "blob": hashing
    /// follows the link, so the blob must hold the target's bytes as a
    /// regular file. Reflink and copy both follow the link; only the
    /// hardlink fallback could capture the symlink itself, and the
    /// eligibility guard refuses it (`symlink_metadata` check).
    #[cfg(unix)]
    #[test]
    fn put_never_stores_a_symlink_as_a_blob() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let target = dir.path().join("real-artifact.rlib");
        fs::write(&target, b"real artifact bytes").unwrap();
        let symlink = dir.path().join("linked.rlib");
        std::os::unix::fs::symlink(&target, &symlink).unwrap();

        store
            .put(
                "key-symlink",
                "foo",
                &["rlib".to_string()],
                &[],
                "host",
                "dev",
                &[(symlink.clone(), "linked.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        let hash = crate::cache_key::hash_file(&symlink).unwrap();
        let blob = store.blob_path(&hash);
        let meta = fs::symlink_metadata(&blob).unwrap();
        assert!(
            meta.file_type().is_file(),
            "blob must be a regular file, not a symlink"
        );
        assert_eq!(fs::read(&blob).unwrap(), b"real artifact bytes");
    }

    #[test]
    fn materialize_blob_errors_when_source_cannot_be_copied() {
        // Covers materialize_blob copy-fallback error branch.
        let dir = tempfile::tempdir().unwrap();
        let hash = "a".repeat(64);
        let source = dir.path().join("missing.rlib");
        let blob = dir.path().join("blobs").join("aa").join(&hash);

        let err = materialize_blob(&source, &blob, false).unwrap_err();

        assert!(
            err.to_string().contains("copying"),
            "expected copy context, got: {err:#}"
        );
        assert!(!blob.exists());
    }

    #[test]
    fn materialize_blob_removes_tmp_when_atomic_rename_fails() {
        // Covers materialize_blob atomic-rename failure cleanup branch.
        let dir = tempfile::tempdir().unwrap();
        let hash = "b".repeat(64);
        let source = dir.path().join("source.rlib");
        fs::write(&source, b"blob bytes").unwrap();
        let blob = dir.path().join("blobs").join("bb").join(&hash);
        fs::create_dir_all(&blob).unwrap();

        let err = materialize_blob(&source, &blob, false).unwrap_err();

        assert!(
            err.to_string().contains("atomic rename"),
            "expected rename context, got: {err:#}"
        );
        let tmp_left = fs::read_dir(blob.parent().unwrap())
            .unwrap()
            .flatten()
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"))
            .count();
        assert_eq!(tmp_left, 0, "failed rename must remove its temp file");
        assert!(blob.is_dir(), "the conflicting destination dir remains");
    }

    /// A failed publish after a provisional hardlink must not leave the
    /// build's output read-only: the RO chmod was applied on the shared temp
    /// inode before rename, and the temp is discarded on failure. (On CoW
    /// filesystems the hardlink path is never taken — source stays writable
    /// either way.)
    #[test]
    fn materialize_blob_failure_does_not_leave_source_readonly() {
        let dir = tempfile::tempdir().unwrap();
        let hash = "c".repeat(64);
        let source = dir.path().join("source.rlib");
        fs::write(&source, b"blob bytes").unwrap();
        // Destination is a directory so rename fails after staging.
        let blob = dir.path().join("blobs").join("cc").join(&hash);
        fs::create_dir_all(&blob).unwrap();

        let err = materialize_blob(&source, &blob, true).unwrap_err();
        assert!(
            err.to_string().contains("atomic rename"),
            "expected rename context, got: {err:#}"
        );
        assert!(
            !fs::metadata(&source).unwrap().permissions().readonly(),
            "failed hardlink ingest must restore a writable build output"
        );
    }

    // ── stage → hash → publish (review finding #3) ──────────────────────

    /// The put path must hash the STAGED snapshot, not the live build
    /// output: the bytes published under a digest must be exactly the bytes
    /// that were hashed, so a post-build mutator changing the file after the
    /// snapshot can never store content X under address H(Y).
    ///
    /// Uses independent (never-hardlink) storage deliberately: on a
    /// non-CoW filesystem the hardlink ingest shares the output's inode
    /// with the blob, so mutating the output afterwards would both hit the
    /// read-only guard and legitimately move the shared blob. Independent
    /// storage (reflink/copy) gives the snapshot byte-isolation on every
    /// filesystem, which is the property under test.
    #[test]
    fn put_stores_snapshot_bytes_matching_recorded_digest() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output_file = dir.path().join("out.rlib");
        let original = b"artifact-bytes-v1";
        fs::write(&output_file, original).unwrap();

        store
            .put_with_compile_time_independent(
                "snapshot_key",
                "snapshot_crate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(output_file.clone(), "libout.rlib".to_string())],
                "",
                "",
                0,
            )
            .unwrap();

        // Simulate a post-put mutator (strip / codesign / wasm tooling):
        // rewrite the build output in place with different content.
        fs::write(
            &output_file,
            b"mutated-after-put-with-a-much-longer-payload",
        )
        .unwrap();

        let meta = store.get("snapshot_key").unwrap().unwrap();
        assert_eq!(meta.files.len(), 1);
        let blob = store.blob_path(&meta.files[0].hash);
        let stored = fs::read(&blob).unwrap();
        assert_eq!(
            stored, original,
            "stored blob must be byte-identical to what was hashed at put time"
        );
        assert_eq!(meta.files[0].size, original.len() as u64);
    }

    /// A completed put must leave nothing behind in the staging area.
    #[test]
    fn successful_put_leaves_staging_dir_empty() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output_file = dir.path().join("out.rlib");
        fs::write(&output_file, b"artifact").unwrap();
        store
            .put(
                "staging_clean_key",
                "crate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(output_file, "libout.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        let staging = dir.path().join("staging");
        if staging.exists() {
            let leftovers: Vec<_> = fs::read_dir(&staging).unwrap().flatten().collect();
            assert!(leftovers.is_empty(), "staging litter: {leftovers:?}");
        }
    }

    /// A refused zero-byte artifact must clean up its staged snapshot; a
    /// crash-refusal that leaked it would otherwise sit until GC.
    #[test]
    fn zero_byte_refusal_cleans_up_staged_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // A zero-byte `.rlib` is never valid output for a lib crate.
        let output_file = dir.path().join("empty.rlib");
        fs::write(&output_file, b"").unwrap();

        let result = store.put(
            "zero_key",
            "crate",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(output_file, "libout.rlib".to_string())],
            "",
            "",
        );
        assert!(result.is_err(), "zero-byte rlib must be refused");
        let staging = dir.path().join("staging");
        if staging.exists() {
            let leftovers: Vec<_> = fs::read_dir(&staging).unwrap().flatten().collect();
            assert!(leftovers.is_empty(), "refused put left staging litter");
        }
    }

    /// Publishing onto an already-present blob discards the staged snapshot
    /// and reports `false` — same-digest means same-bytes, so losing the
    /// publish race is benign and must not double-count ingest.
    #[test]
    fn publish_staged_blob_is_idempotent_when_blob_exists() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let source = dir.path().join("out.rlib");
        fs::write(&source, b"identical-content").unwrap();

        let (staged_a, ingest_a) = store.stage_blob_from_source(&source, false).unwrap();
        let hash = crate::cache_key::hash_file(&staged_a).unwrap();
        assert!(
            store
                .publish_staged_blob(&staged_a, ingest_a, &hash, 17)
                .unwrap(),
            "first publish should win"
        );

        let (staged_b, _ingest_b) = store.stage_blob_from_source(&source, false).unwrap();
        assert_ne!(staged_a, staged_b, "each stage gets its own temp");
        assert!(
            !store
                .publish_staged_blob(&staged_b, ingest_a, &hash, 17)
                .unwrap(),
            "second publish of the same digest must be a no-op"
        );

        let staging_leftovers = fs::read_dir(store.staging_dir()).unwrap().flatten().count();
        assert_eq!(staging_leftovers, 0, "discarded stage must not linger");
    }

    /// Crash-orphaned staging files are reclaimed only once older than the
    /// grace period — a concurrent put's fresh snapshot is never touched.
    #[test]
    fn sweep_stale_staging_respects_min_age() {
        use std::time::Duration;

        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let stale = store.staging_dir().join("stage-old-1.tmp");
        let fresh = store.staging_dir().join("stage-new-1.tmp");
        fs::create_dir_all(store.staging_dir()).unwrap();
        fs::write(&stale, b"abandoned").unwrap();
        fs::write(&fresh, b"in-flight").unwrap();
        let old = filetime::FileTime::from_unix_time(0, 0);
        filetime::set_file_mtime(&stale, old).unwrap();
        filetime::set_file_atime(&stale, old).unwrap();

        let stats = store.sweep_stale_staging(Duration::from_secs(3600));
        assert_eq!(stats.removed, 1, "only the aged-out file is swept");
        assert_eq!(stats.bytes_reclaimed, b"abandoned".len() as u64);
        assert!(!stale.exists());
        assert!(fresh.exists(), "fresh staging file must survive the sweep");

        // Once it ages out, it goes too.
        let stats = store.sweep_stale_staging(Duration::ZERO);
        assert_eq!(stats.removed, 1);
        assert_eq!(stats.bytes_reclaimed, b"in-flight".len() as u64);
        assert!(!fresh.exists());
    }

    /// The grace both sweepers share (daemon GC and `doctor --repair`) must
    /// outlast an in-flight put. A snapshot another process is still filling
    /// is indistinguishable from a crash leftover, and reclaiming it fails
    /// that put at publish time — so a fresh snapshot has to survive.
    #[test]
    fn staging_sweep_grace_spares_an_in_flight_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        fs::create_dir_all(store.staging_dir()).unwrap();
        let in_flight = store.staging_dir().join("stage-1-0.tmp");
        fs::write(&in_flight, b"mid-put").unwrap();

        let stats = store.sweep_stale_staging(STAGING_SWEEP_GRACE);
        assert_eq!(stats.removed, 0, "no sweeper may reclaim a live snapshot");
        assert!(in_flight.exists());
        assert!(
            STAGING_SWEEP_GRACE >= Duration::from_secs(3600),
            "the grace must stay long enough to outlast a slow put"
        );
    }

    /// The staging name search must SKIP an occupied candidate (a crash
    /// leftover) and hand back the next free one — and hand it back
    /// uncreated, which is what keeps the zero-copy ingests usable.
    #[test]
    fn free_staging_path_skips_occupied_names() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("cand-a");
        let second = dir.path().join("cand-b");
        fs::write(&first, b"taken").unwrap();

        // First candidate always collides; the next one is free.
        let names: Vec<PathBuf> = vec![first.clone(), second.clone()];
        let mut calls = 0usize;
        let got = free_staging_path(|_| {
            let p = names[calls].clone();
            calls += 1;
            p
        })
        .unwrap();
        assert_eq!(got, second, "must skip the taken candidate");
        assert!(
            !got.exists(),
            "the chosen path must NOT exist: clonefile(2)/link(2) fail with \
             EEXIST on an existing destination, which would demote every put \
             to a full byte copy"
        );
    }

    /// A non-collision error must propagate as itself, not be swallowed by
    /// the skip branch and reported as an exhausted name search.
    #[test]
    fn free_staging_path_propagates_real_errors() {
        let dir = tempfile::tempdir().unwrap();
        // A FILE used as the parent path. Unix stats that as ENOTDIR;
        // Windows reports it with the same shape as a free name.
        let not_a_dir = dir.path().join("not-a-dir");
        fs::write(&not_a_dir, b"").unwrap();
        match free_staging_path(|n| not_a_dir.join(format!("x-{n}"))) {
            Err(err) => assert_ne!(
                err.kind(),
                std::io::ErrorKind::AlreadyExists,
                "a real fault must surface as itself, not as a name collision: {err}"
            ),
            // Where the platform cannot tell the fault from a free name, the
            // search hands the candidate back and the ingest is what fails —
            // what must never happen either way is spinning through every
            // attempt and calling it a collision.
            Ok(candidate) => assert!(
                candidate.starts_with(&not_a_dir),
                "expected the first candidate, got {}",
                candidate.display()
            ),
        }
    }

    /// Exhausting every candidate reports a bounded failure rather than
    /// spinning: an unbounded search is a hang no test can kill.
    #[test]
    fn free_staging_path_gives_up_after_bounded_attempts() {
        let dir = tempfile::tempdir().unwrap();
        let taken = dir.path().join("always-taken");
        fs::write(&taken, b"taken").unwrap();

        let mut calls = 0u32;
        let err = free_staging_path(|_| {
            calls += 1;
            taken.clone()
        })
        .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(calls, STAGING_NAME_ATTEMPTS, "search must be bounded");
    }

    /// Staging must reach the store by reflink or hardlink, never by writing
    /// the artifact's bytes a second time.
    ///
    /// The ingest destination has to be a path that does not exist yet:
    /// `clonefile(2)` and `link(2)` both fail with `EEXIST` otherwise, so a
    /// staging file that is pre-created (to reserve its name, say) turns a
    /// metadata-only clone into a full copy of every artifact — still
    /// correct, but it doubles put I/O and stops store blobs from sharing
    /// blocks with the build output they came from.
    #[cfg(unix)]
    #[test]
    fn staging_ingests_zero_copy_not_a_byte_copy() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let source = dir.path().join("out.rlib");
        fs::write(&source, b"artifact-bytes").unwrap();

        // Same filesystem as the store, so a hardlink is always available
        // even where the filesystem has no reflink support (ext4, tmpfs).
        let (staged, ingest) = store.stage_blob_from_source(&source, true).unwrap();
        assert!(
            !matches!(ingest, StoreIngest::Copy),
            "staging fell back to a byte copy where a reflink or hardlink \
             was available — the ingest destination must not exist yet"
        );
        assert_eq!(fs::read(&staged).unwrap(), b"artifact-bytes");
        Store::drop_tmp_restore_source(&source, &staged);
    }

    /// A symlinked source must never be hardlinked into the store: hashing
    /// follows the link, but a hardlink would publish a pointer to mutable
    /// external state. The staged snapshot must be a regular file carrying
    /// the target's content.
    #[cfg(unix)]
    #[test]
    fn staging_refuses_to_hardlink_a_symlink_source() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let target = dir.path().join("real.rlib");
        fs::write(&target, b"target-bytes").unwrap();
        let link = dir.path().join("link.rlib");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        // allow_hardlink=true is the interesting case: only the symlink check
        // stands between the link and an inode-sharing blob.
        let (staged, _ingest) = store.stage_blob_from_source(&link, true).unwrap();
        let meta = fs::symlink_metadata(&staged).unwrap();
        assert!(
            meta.is_file(),
            "staged snapshot must be a regular file, never a symlink"
        );
        assert_eq!(fs::read(&staged).unwrap(), b"target-bytes");
        // Whatever ingest was chosen, the store side of the deal is read-only;
        // the symlink TARGET itself must stay owner-writable when the
        // snapshot did not share its inode (copy/reflink).
        if !paths_share_inode(&target, &staged) {
            let mode = fs::metadata(&target).unwrap().permissions().mode();
            assert_eq!(
                mode & 0o200,
                0o200,
                "an isolated snapshot must not flip the symlink target read-only"
            );
        }
    }

    /// Lost-race semantics of `publish_staged_blob`: a rename failure while
    /// the destination already exists as a file is the benign
    /// concurrent-winner case and must report `Ok(false)`.
    #[test]
    fn publish_reports_false_when_rename_fails_on_existing_blob() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let hash = "d".repeat(64);
        let source = dir.path().join("out.rlib");
        fs::write(&source, b"content").unwrap();

        // Publish the winner so the destination exists as a file...
        let (staged_a, ingest_a) = store.stage_blob_from_source(&source, false).unwrap();
        assert!(
            store
                .publish_staged_blob(&staged_a, ingest_a, &hash, 7)
                .unwrap()
        );

        // ...then force the rename to fail: a DIRECTORY cannot be renamed
        // onto an existing regular file. The staged argument being a
        // directory guarantees the error without touching permissions.
        let bogus_staged = store.staging_dir().join("not-a-file");
        fs::create_dir_all(&bogus_staged).unwrap();
        let result = store.publish_staged_blob(&bogus_staged, ingest_a, &hash, 7);
        assert!(!result.unwrap(), "lost race must report Ok(false)");
    }

    /// A rename failure with NO existing destination is a genuine error, not
    /// a lost race, and must propagate.
    #[test]
    fn publish_errors_when_rename_fails_without_existing_blob() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let hash = "e".repeat(64);
        let source = dir.path().join("out.rlib");
        fs::write(&source, b"content").unwrap();
        let (staged, ingest) = store.stage_blob_from_source(&source, false).unwrap();

        // Put a directory in the way of the destination: renaming a file
        // onto a directory fails even though `blob.is_file()` is false.
        let blob = store.blob_path(&hash);
        fs::create_dir_all(blob.parent().unwrap()).unwrap();
        fs::create_dir_all(&blob).unwrap();

        let result = store.publish_staged_blob(&staged, ingest, &hash, 7);
        assert!(result.is_err(), "genuine rename errors must propagate");
    }

    /// Phase-2 recovery re-materializes from the LIVE source; a source that
    /// still hashes to the recorded digest commits cleanly.
    #[test]
    fn rematerialize_accepts_untouched_source() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let source = dir.path().join("out.rlib");
        fs::write(&source, b"stable-content").unwrap();
        let hash = crate::cache_key::hash_file(&source).unwrap();

        store
            .rematerialize_and_verify(&source, &hash, "out.rlib", false)
            .unwrap();
        assert_eq!(fs::read(store.blob_path(&hash)).unwrap(), b"stable-content");
    }

    /// ...but a source mutated after phase 1 must NEVER be stored under the
    /// recorded address: the verification must refuse the commit.
    #[test]
    fn rematerialize_refuses_mutated_source() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let source = dir.path().join("out.rlib");
        fs::write(&source, b"original").unwrap();
        let hash = crate::cache_key::hash_file(&source).unwrap();

        // Simulate a post-build mutator racing between phase 1 and recovery.
        fs::write(&source, b"mutated-after-snapshot").unwrap();

        let err = store
            .rematerialize_and_verify(&source, &hash, "out.rlib", false)
            .unwrap_err();
        assert!(
            err.to_string().contains("refusing to commit"),
            "expected digest-mismatch refusal, got: {err:#}"
        );
    }

    /// The read-only probe (#565) must mirror `Store::get`'s servable/hit
    /// decision without any write side effect: a committed, blob-complete
    /// entry probes `Hit` and leaves `hit_count` untouched; an unknown key is
    /// an authoritative `Miss`; anything needing repair probes `Fallback`.
    #[test]
    fn probe_entry_readonly_hit_miss_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output_file = dir.path().join("out.rlib");
        fs::write(&output_file, b"artifact-bytes").unwrap();
        store
            .put(
                "probe_key",
                "probe_crate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(output_file, "libout.rlib".to_string())],
                "out",
                "err",
            )
            .unwrap();

        let ro = open_index_db_readonly(&config.index_db_path()).unwrap();
        let store_dir = config.store_dir();

        let meta = match probe_entry_readonly(&ro, &store_dir, "probe_key") {
            ProbeOutcome::Hit(meta) => meta,
            other => panic!("expected hit, got {other:?}"),
        };
        assert_eq!(meta.cache_key, "probe_key");
        assert_eq!(meta.stdout, "out");
        assert_eq!(meta.files.len(), 1);
        let hits: i64 = store
            .db
            .query_row(
                "SELECT hit_count FROM entries WHERE cache_key = 'probe_key'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            hits, 0,
            "a probe must not record a hit — the pin writer does"
        );

        assert!(matches!(
            probe_entry_readonly(&ro, &store_dir, "no_such_key"),
            ProbeOutcome::Miss
        ));

        // A blob deleted out from under the entry needs evict-and-miss (a
        // write) — the probe must delegate that to the wrapper's local path.
        let blob = store.blob_path(&meta.files[0].hash);
        fs::remove_file(&blob).unwrap();
        assert!(matches!(
            probe_entry_readonly(&ro, &store_dir, "probe_key"),
            ProbeOutcome::Fallback(_)
        ));
    }

    /// `query_only` must make accidental writes through a probe connection a
    /// hard error rather than a silent store mutation.
    #[test]
    fn probe_connection_refuses_writes() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let _store = Store::open(&config).unwrap();
        let ro = open_index_db_readonly(&config.index_db_path()).unwrap();
        assert!(
            ro.execute("DELETE FROM entries", []).is_err(),
            "read-only probe connection must reject writes"
        );
    }

    fn test_config(dir: &Path) -> Config {
        Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            remote_readonly: false,
            modified_input_guard: false,
            local_hit_daemon: false,
            windows_hardlink: false,
            auto_gc: true,
            storage_layout_advice: true,
            heartbeat_secs: 30,
            explain_miss: false,
            path_only_env_vars: Vec::new(),
            incremental_crates: Vec::new(),
            key_env_vars: Vec::new(),
            base_dirs: Vec::new(),
            cache_dir: dir.to_path_buf(),
            runtime_dir: dir.to_path_buf(),
            max_size: 1024 * 1024, // 1 MiB
            remote: None,
            remote_error: None,
            socket_path_override: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            preserve_incremental: false,
            adaptive_incremental: true,
            event_log_max_size: 1024 * 1024,
            event_log_keep_lines: 100,
            compression_level: 3,
            s3_concurrency: 16,
            prefetch_enabled: crate::config::DEFAULT_PREFETCH_ENABLED,
            remote_key_cache_refresh_secs: crate::config::DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS,
            prefetch_max_keys: crate::config::DEFAULT_PREFETCH_MAX_KEYS,
            prefetch_max_bytes: crate::config::DEFAULT_PREFETCH_MAX_BYTES,
            prefetch_deadline_secs: crate::config::DEFAULT_PREFETCH_DEADLINE_SECS,
            min_store_compile_ms: crate::config::DEFAULT_MIN_STORE_COMPILE_MS,
            gc_max_age_hours: crate::config::DEFAULT_GC_MAX_AGE_HOURS,
            daemon_idle_timeout_secs: crate::config::DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: crate::config::DEFAULT_S3_POOL_IDLE_SECS,
            remote_restore_timeout_secs: crate::config::DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS,
            remote_negative_ttl_secs: crate::config::DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
        }
    }

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    static ENV_VAR_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(key);
            unsafe { std::env::set_var(key, value) };
            Self { key, previous }
        }

        fn remove(key: &'static str) -> Self {
            let previous = std::env::var_os(key);
            unsafe { std::env::remove_var(key) };
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => unsafe { std::env::set_var(self.key, value) },
                None => unsafe { std::env::remove_var(self.key) },
            }
        }
    }

    /// kunobi-ninja/kache#336: diagnostics are stored in full by default (so a
    /// hit replays exactly what a miss emitted), and only truncated — at a char
    /// boundary, with a marker — when an explicit cap is set.
    #[test]
    fn cap_diagnostics_is_lossless_by_default_and_truncates_when_capped() {
        let warnings = "warning: unused variable `x`\nwarning: dead code\n";
        // Uncapped: byte-identical replay.
        assert_eq!(cap_diagnostics(warnings, None), warnings);
        // Cap above length: unchanged.
        assert_eq!(cap_diagnostics(warnings, Some(10_000)), warnings);
        // Cap below length: truncated with a marker, original tail dropped.
        let capped = cap_diagnostics(warnings, Some(20));
        assert!(capped.starts_with("warning: unused vari"));
        assert!(capped.contains("diagnostics truncated"));
        assert!(capped.len() < warnings.len() + 80);
        // Multi-byte safety: never split a char.
        let unicode = "wörning: ".repeat(20);
        let capped = cap_diagnostics(&unicode, Some(5));
        assert!(std::str::from_utf8(capped.as_bytes()).is_ok());
    }

    #[test]
    fn put_records_known_hash_only_for_stable_outputs() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&test_config(dir.path())).unwrap();
        let artifact = dir.path().join("artifact.rlib");
        std::fs::write(&artifact, vec![b'x'; 64 * 1024]).unwrap();
        let expected = crate::cache_key::hash_file(&artifact).unwrap();

        store
            .put(
                "known-hash-stable",
                "artifact",
                &["rlib".to_string()],
                &[],
                "host",
                "dev",
                &[(artifact.clone(), "libartifact.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        let hasher = store.file_hasher();
        assert_eq!(hasher.hash(&artifact).unwrap(), expected);
        let stats = hasher.stats();
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.cache_misses, 0);
        assert_eq!(stats.bytes_hashed, 0);

        let dep_info = dir.path().join("artifact.d");
        std::fs::write(&dep_info, vec![b'd'; 64 * 1024]).unwrap();
        store
            .put(
                "known-hash-dep-info",
                "artifact",
                &[],
                &[],
                "host",
                "dev",
                &[(dep_info.clone(), "artifact.d".to_string())],
                "",
                "",
            )
            .unwrap();
        assert!(matches!(
            store.file_hash_lookup(&dep_info),
            crate::cache_key::FileHashLookup::NeedsHash(_)
        ));

        let independent = dir.path().join("independent.o");
        std::fs::write(&independent, vec![b'o'; 64 * 1024]).unwrap();
        store
            .put_with_compile_time_independent(
                "known-hash-independent",
                "artifact.c",
                &[],
                &[],
                "host",
                "dev",
                &[(independent.clone(), "independent.o".to_string())],
                "",
                "",
                1,
            )
            .unwrap();
        assert!(matches!(
            store.file_hash_lookup(&independent),
            crate::cache_key::FileHashLookup::NeedsHash(_)
        ));
    }

    #[test]
    fn test_store_put_and_get() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Create a fake output file
        let output_file = dir.path().join("output.rlib");
        std::fs::write(&output_file, b"fake rlib content").unwrap();

        store
            .put(
                "abc123",
                "mylib",
                &["lib".to_string()],
                &["std".to_string()],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(output_file, "libmylib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        assert!(store.contains("abc123"));
        let meta = store.get("abc123").unwrap().unwrap();
        assert_eq!(meta.crate_name, "mylib");
        assert_eq!(meta.files.len(), 1);
        assert_eq!(meta.files[0].name, "libmylib.rlib");
    }

    #[test]
    fn sweep_orphan_blobs_removes_unreferenced_files_only() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // A real entry → its blob is referenced (has a `blobs` row).
        let output_file = dir.path().join("output.rlib");
        std::fs::write(&output_file, b"real rlib content").unwrap();
        store
            .put(
                "abc123",
                "mylib",
                &["lib".to_string()],
                &["std".to_string()],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(output_file, "libmylib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        // An orphan blob: a 64-hex file on disk with no `blobs` row, as a
        // crash mid-put would leave behind.
        let orphan_hash = "f".repeat(64);
        let orphan_path = store.blob_path(&orphan_hash);
        std::fs::create_dir_all(orphan_path.parent().unwrap()).unwrap();
        std::fs::write(&orphan_path, b"orphaned bytes").unwrap();
        // A `.tmp` in-progress file must never be touched by the sweep.
        let tmp_path = orphan_path.with_file_name(format!(".{orphan_hash}.123.0.tmp"));
        std::fs::write(&tmp_path, b"in-progress").unwrap();

        // min_age 0 → sweep the freshly-created orphan immediately.
        let stats = store.sweep_orphan_blobs(std::time::Duration::ZERO).unwrap();

        assert_eq!(stats.removed, 1, "only the orphan should be removed");
        // The put blob + the orphan are blob-shaped; the `.tmp` is excluded.
        assert_eq!(stats.scanned, 2);
        assert_eq!(stats.bytes_reclaimed, b"orphaned bytes".len() as u64);
        assert!(!orphan_path.exists(), "orphan blob must be unlinked");
        assert!(tmp_path.exists(), "in-progress .tmp must be left alone");
        // The referenced entry's blob survived: get() still restores it.
        assert!(store.get("abc123").unwrap().is_some());
    }

    #[test]
    fn sweep_orphan_blobs_respects_min_age() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let orphan_hash = "a".repeat(64);
        let orphan_path = store.blob_path(&orphan_hash);
        std::fs::create_dir_all(orphan_path.parent().unwrap()).unwrap();
        std::fs::write(&orphan_path, b"fresh orphan").unwrap();

        // A freshly written orphan is younger than the grace period, so a
        // concurrent put materializing it would be protected: not swept.
        let stats = store
            .sweep_orphan_blobs(std::time::Duration::from_secs(3600))
            .unwrap();
        assert_eq!(stats.removed, 0);
        assert!(orphan_path.exists());
    }

    #[test]
    fn reconcile_blob_index_repairs_refcounts_and_stale_rows_idempotently() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let payload = b"shared authoritative blob";

        for key in ["repair_a", "repair_b"] {
            let output = dir.path().join(format!("{key}.rlib"));
            fs::write(&output, payload).unwrap();
            store
                .put(
                    key,
                    "repairlib",
                    &["lib".to_string()],
                    &[],
                    "host",
                    "dev",
                    &[(output, format!("lib{key}.rlib"))],
                    "",
                    "",
                )
                .unwrap();
        }
        let hash = store.get("repair_a").unwrap().unwrap().files[0]
            .hash
            .clone();
        let stale_hash = "f".repeat(64);
        let stale_path = store.blob_path(&stale_hash);
        fs::create_dir_all(stale_path.parent().unwrap()).unwrap();
        fs::write(&stale_path, b"stale indexed blob").unwrap();

        store
            .db
            .execute(
                "UPDATE blobs SET refcount = 41 WHERE hash = ?1",
                params![hash],
            )
            .unwrap();
        store
            .db
            .execute(
                "UPDATE entry_blobs SET refs = 7 WHERE cache_key = 'repair_a'",
                [],
            )
            .unwrap();
        store
            .db
            .execute(
                "INSERT INTO blobs (hash, size, refcount) VALUES (?1, ?2, 9)",
                params![stale_hash, b"stale indexed blob".len() as i64],
            )
            .unwrap();

        assert_eq!(
            store.blob_index_drift().unwrap(),
            BlobIndexDrift {
                entry_mappings: 1,
                blobs: 2,
            }
        );
        assert_eq!(
            store.reconcile_blob_index().unwrap(),
            BlobIndexDrift {
                entry_mappings: 1,
                blobs: 2,
            }
        );
        assert_eq!(store.blob_index_drift().unwrap(), BlobIndexDrift::default());
        assert_eq!(
            store.reconcile_blob_index().unwrap(),
            BlobIndexDrift::default()
        );

        let refcount: i64 = store
            .db
            .query_row(
                "SELECT refcount FROM blobs WHERE hash = ?1",
                params![hash],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(refcount, 2);
        assert_eq!(
            store
                .db
                .query_row(
                    "SELECT COUNT(*) FROM blobs WHERE hash = ?1",
                    params![stale_hash],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        let swept = store.sweep_orphan_blobs(Duration::ZERO).unwrap();
        assert_eq!(swept.removed, 1);
        assert!(!stale_path.exists());
    }

    #[test]
    fn reconcile_blob_index_fails_closed_on_unreadable_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let output = dir.path().join("output.rlib");
        fs::write(&output, b"authoritative bytes").unwrap();
        store
            .put(
                "repair_bad_meta",
                "repairlib",
                &["lib".to_string()],
                &[],
                "host",
                "dev",
                &[(output, "librepair.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        let hash = store.get("repair_bad_meta").unwrap().unwrap().files[0]
            .hash
            .clone();
        store
            .db
            .execute(
                "UPDATE blobs SET refcount = 9 WHERE hash = ?1",
                params![hash],
            )
            .unwrap();
        fs::write(
            store.entry_dir("repair_bad_meta").join("meta.json"),
            b"not json",
        )
        .unwrap();

        let error = store.reconcile_blob_index().unwrap_err().to_string();
        assert!(error.contains("parsing authoritative meta.json"), "{error}");
        let refcount: i64 = store
            .db
            .query_row(
                "SELECT refcount FROM blobs WHERE hash = ?1",
                params![hash],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(refcount, 9, "failed repair must leave the index untouched");
    }

    #[test]
    fn reconcile_blob_index_rejects_each_invalid_metadata_dimension() {
        for invalid_hash in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let config = test_config(dir.path());
            let store = Store::open(&config).unwrap();
            let output = dir.path().join("output.rlib");
            fs::write(&output, b"valid blob bytes").unwrap();
            store
                .put(
                    "repair_invalid_metadata",
                    "repairlib",
                    &["lib".to_string()],
                    &[],
                    "host",
                    "dev",
                    &[(output, "librepair.rlib".to_string())],
                    "",
                    "",
                )
                .unwrap();
            let meta_path = store.entry_dir("repair_invalid_metadata").join("meta.json");
            let mut meta: EntryMeta =
                serde_json::from_str(&fs::read_to_string(&meta_path).unwrap()).unwrap();
            if invalid_hash {
                meta.files[0].hash = "not-a-content-hash".to_string();
            } else {
                meta.files[0].name = "../unsafe.rlib".to_string();
            }
            fs::write(&meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();

            let error = store.reconcile_blob_index().unwrap_err().to_string();
            assert!(error.contains("invalid blob metadata"), "{error}");
        }
    }

    #[test]
    fn store_ingest_accounts_new_blob_bytes_by_mechanism() {
        // A new-blob put must record the artifact's bytes against exactly one
        // store-ingest counter — reflink, hardlink, or copy depending on the
        // filesystem and artifact kind. The counters are process-global and
        // monotonic, so a delta of at least the artifact size is a safe
        // assertion under parallel test execution.
        let cache_dir = tempfile::tempdir().unwrap();
        let config = test_config(cache_dir.path());
        let store = Store::open(&config).unwrap();

        // Unique content so this is genuinely a new blob, not a dup of a blob
        // some concurrent test happened to store (which would skip ingest).
        let payload = b"store-ingest-accounting-unique-artifact-bytes-0xC0FFEE".repeat(64);
        let output_file = cache_dir.path().join("output.rlib");
        std::fs::write(&output_file, &payload).unwrap();

        let before = crate::opcounts::store_reflinked_bytes()
            + crate::opcounts::store_hardlinked_bytes()
            + crate::opcounts::store_copied_bytes();
        let put_result = store
            .put(
                "ingest_key",
                "ingestlib",
                &["lib".to_string()],
                &[],
                "host",
                "dev",
                &[(output_file, "libingest.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        assert_eq!(put_result.new_blobs, 1, "expected a genuinely new blob");

        let after = crate::opcounts::store_reflinked_bytes()
            + crate::opcounts::store_hardlinked_bytes()
            + crate::opcounts::store_copied_bytes();
        assert!(
            after >= before + payload.len() as u64,
            "store ingest must account the new blob's bytes (delta {} < {})",
            after - before,
            payload.len()
        );
    }

    #[test]
    fn test_store_put_reports_full_dup_for_existing_blob() {
        let cache_dir = tempfile::tempdir().unwrap();
        let config = test_config(cache_dir.path());
        let store = Store::open(&config).unwrap();

        let output_file = cache_dir.path().join("output.rlib");
        std::fs::write(&output_file, b"fake rlib content").unwrap();

        let put_result = store
            .put(
                "first_key",
                "mylib",
                &["lib".to_string()],
                &[],
                "host",
                "dev",
                &[(output_file.clone(), "libmylib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        assert_eq!(put_result.output_blobs, 1);
        assert_eq!(put_result.duplicate_blobs, 0);
        assert_eq!(put_result.new_blobs, 1);
        assert!(!put_result.is_full_dup());

        let meta = store.get("first_key").unwrap().unwrap();
        let hash = meta.files[0].hash.clone();
        assert!(store.blob_path(&hash).is_file());

        let duplicate_output = cache_dir.path().join("duplicate-output.rlib");
        std::fs::write(&duplicate_output, b"fake rlib content").unwrap();
        let second_put = store
            .put(
                "second_key",
                "mylib",
                &["lib".to_string()],
                &[],
                "host",
                "dev",
                &[(duplicate_output, "libmylib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        assert_eq!(second_put.output_blobs, 1);
        assert_eq!(second_put.duplicate_blobs, 1);
        assert_eq!(second_put.new_blobs, 0);
        assert!(second_put.is_full_dup());

        store.remove_entry("first_key").unwrap();
        assert!(store.blob_path(&hash).exists());
        store.remove_entry("second_key").unwrap();
        assert!(!store.blob_path(&hash).exists());
    }

    #[test]
    fn test_retryable_sqlite_open_error_for_missing_parent() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("missing").join("index.db");

        let err = open_index_db(&db_path).unwrap_err();
        let sql_err = err.downcast_ref::<SqlError>().unwrap();

        assert!(is_retryable_sqlite_open_error(sql_err));
    }

    #[test]
    fn is_corruption_error_flags_a_non_sqlite_file() {
        let dir = tempfile::tempdir().unwrap();
        let garbage = dir.path().join("garbage.db");
        fs::write(&garbage, b"definitely not a sqlite database").unwrap();
        let err = try_open_index_db(&garbage).unwrap_err();
        assert!(
            is_corruption_error(&err),
            "a non-sqlite file must classify as corruption: {err}"
        );

        // A transient open failure (missing parent → CannotOpen) is NOT
        // corruption and must not be self-healed.
        let missing = dir.path().join("missing").join("index.db");
        let err = try_open_index_db(&missing).unwrap_err();
        assert!(!is_corruption_error(&err));
    }

    /// A realistic 64-hex cache key, since the rebuild scan only adopts entry
    /// dirs whose name is a well-formed key.
    fn key(seed: u8) -> String {
        blake3::hash(&[seed]).to_hex().to_string()
    }

    /// Put one single-file entry and return its key.
    fn put_entry(store: &Store, dir: &Path, seed: u8, crate_name: &str, content: &[u8]) -> String {
        let k = key(seed);
        let src = dir.join(format!("out-{seed}.rlib"));
        std::fs::write(&src, content).unwrap();
        store
            .put(
                &k,
                crate_name,
                &["lib".to_string()],
                &["std".to_string()],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(src, format!("lib{crate_name}.rlib"))],
                "",
                "",
            )
            .unwrap();
        k
    }

    #[test]
    fn rebuild_index_from_store_recovers_entries_after_the_index_is_lost() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let (k1, k2) = {
            let store = Store::open(&config).unwrap();
            let k1 = put_entry(&store, dir.path(), 1, "alpha", b"alpha rlib content");
            let k2 = put_entry(&store, dir.path(), 2, "beta", b"beta rlib content");
            (k1, k2)
        };

        // Lose the index entirely, keeping the store (blobs + meta.json) intact.
        // This is what quarantining a corrupt index leaves behind.
        std::fs::remove_file(config.index_db_path()).unwrap();

        let store = Store::open(&config).unwrap();
        assert_eq!(
            store.entry_count().unwrap(),
            0,
            "a fresh index starts with no rows"
        );

        let stats = store.rebuild_index_from_store().unwrap();
        assert_eq!(
            stats.entries_rebuilt, 2,
            "both entries are adopted: {stats:?}"
        );
        assert_eq!(stats.blobs_registered, 2);

        // The cache is warm again: both keys resolve and restore.
        for k in [&k1, &k2] {
            assert!(store.contains(k), "entry {k} must be usable after rebuild");
            let meta = store.get(k).unwrap().unwrap();
            assert_eq!(meta.files.len(), 1);
        }
        assert_eq!(store.entry_count().unwrap(), 2);
    }

    #[test]
    fn rebuild_index_is_idempotent_and_does_not_inflate_refcounts() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let k = put_entry(&store, dir.path(), 3, "gamma", b"gamma rlib content");

        let hash: String = store
            .db
            .query_row("SELECT hash FROM blobs", [], |r| r.get(0))
            .unwrap();
        let refcount_before: i64 = store
            .db
            .query_row(
                "SELECT refcount FROM blobs WHERE hash = ?1",
                params![hash],
                |r| r.get(0),
            )
            .unwrap();

        // Running against an already-populated index must be a no-op. If it
        // added refcounts, the blob would outlive its last referrer and leak.
        for _ in 0..3 {
            let stats = store.rebuild_index_from_store().unwrap();
            assert_eq!(
                stats.entries_rebuilt, 0,
                "an already-registered entry is not re-adopted"
            );
        }

        let refcount_after: i64 = store
            .db
            .query_row(
                "SELECT refcount FROM blobs WHERE hash = ?1",
                params![hash],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            refcount_after, refcount_before,
            "repeated rebuilds must not inflate refcounts"
        );

        // And removal still reclaims the blob, proving the refcount is truthful.
        store.remove_entry(&k).unwrap();
        let remaining: i64 = store
            .db
            .query_row(
                "SELECT COUNT(*) FROM blobs WHERE hash = ?1",
                params![hash],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            remaining, 0,
            "blob must be reclaimed on removal, not stranded by an inflated refcount"
        );
    }

    #[test]
    fn rebuild_index_skips_entries_whose_blobs_are_missing_or_wrong_size() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let (good, gone, truncated) = {
            let store = Store::open(&config).unwrap();
            let good = put_entry(&store, dir.path(), 4, "good", b"good content here");
            let gone = put_entry(&store, dir.path(), 5, "gone", b"vanishing content");
            let truncated = put_entry(&store, dir.path(), 6, "trunc", b"truncated content");
            (good, gone, truncated)
        };

        // Break two of the three blobs, then lose the index.
        let meta_of = |k: &str| -> EntryMeta {
            let p = config.store_dir().join(k).join("meta.json");
            serde_json::from_str(&std::fs::read_to_string(p).unwrap()).unwrap()
        };
        let gone_hash = meta_of(&gone).files[0].hash.clone();
        let trunc_hash = meta_of(&truncated).files[0].hash.clone();
        let blob_of = |h: &str| blob_path_in_store_dir(&config.store_dir(), h);
        let gone_blob = blob_of(&gone_hash);
        let trunc_blob = blob_of(&trunc_hash);
        // Blobs are stored read-only (`set_blob_readonly`). Windows refuses to
        // delete or write a read-only file, so clear the bit before doing either
        // — on Unix `remove_file` would have succeeded regardless, which is why
        // omitting it passed locally and only failed on Windows CI.
        let make_writable = |p: &Path| {
            let mut perms = std::fs::metadata(p).unwrap().permissions();
            #[allow(clippy::permissions_set_readonly_false)]
            perms.set_readonly(false);
            std::fs::set_permissions(p, perms).unwrap();
        };
        make_writable(&gone_blob);
        make_writable(&trunc_blob);
        std::fs::remove_file(&gone_blob).unwrap();
        std::fs::write(&trunc_blob, b"short").unwrap();
        std::fs::remove_file(config.index_db_path()).unwrap();

        let store = Store::open(&config).unwrap();
        let stats = store.rebuild_index_from_store().unwrap();

        // Only the intact entry is advertised. Registering an entry whose blob is
        // absent or the wrong length would be a false hit: worse than a miss.
        assert_eq!(stats.entries_rebuilt, 1, "only the intact entry: {stats:?}");
        assert_eq!(stats.entries_skipped, 2);
        assert!(store.contains(&good));
        assert!(
            !store.contains(&gone),
            "an entry with a missing blob must not be registered"
        );
        assert!(
            !store.contains(&truncated),
            "an entry with a wrong-sized blob must not be registered"
        );
    }

    #[test]
    fn rebuild_index_ignores_the_blobs_dir_and_foreign_names() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        // Scoped so the connection is closed before index.db is deleted: Windows
        // refuses to remove a file another handle still has open.
        {
            let store = Store::open(&config).unwrap();
            put_entry(&store, dir.path(), 7, "delta", b"delta rlib content");
        }

        // Non-key directories under store/ must be left alone rather than
        // interpreted as entries: `blobs/` is the content-addressed store, and a
        // stray name is not ours (and would be an unvalidated path component).
        std::fs::create_dir_all(config.store_dir().join("not-a-cache-key")).unwrap();
        std::fs::create_dir_all(config.store_dir().join("0123456789")).unwrap();
        std::fs::remove_file(config.index_db_path()).unwrap();

        let store = Store::open(&config).unwrap();
        let stats = store.rebuild_index_from_store().unwrap();
        assert_eq!(
            stats.entries_rebuilt, 1,
            "only the real entry dir is adopted: {stats:?}"
        );
    }

    #[test]
    fn store_open_rebuilds_automatically_after_quarantining_a_corrupt_index() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let k = {
            let store = Store::open(&config).unwrap();
            put_entry(&store, dir.path(), 8, "epsilon", b"epsilon rlib content")
        };

        // Corrupt the index the way #412 did, then just open the store: recovery
        // must both heal the DB *and* bring the cached entry back, rather than
        // silently presenting a cold cache while the artifacts sit on disk.
        std::fs::write(config.index_db_path(), b"not a sqlite database at all").unwrap();
        for ext in ["-wal", "-shm"] {
            let p = index_sidecar_path(&config.index_db_path(), ext);
            let _ = std::fs::remove_file(p);
        }

        let store = Store::open(&config).expect("corrupt index must self-heal");
        assert!(
            store.contains(&k),
            "the entry must be recovered by Store::open, not lost to an empty index"
        );
        assert_eq!(store.entry_count().unwrap(), 1);
    }

    #[test]
    fn open_index_db_self_heals_a_corrupt_index() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("index.db");
        fs::write(
            &db_path,
            b"this is not a sqlite database; it is garbage bytes",
        )
        .unwrap();

        // The corrupt index must NOT brick the command: it is quarantined and a
        // fresh, usable index is recreated in place (#415).
        let db = open_index_db(&db_path).expect("a corrupt index must self-heal, not brick");
        let count: i64 = db
            .query_row("SELECT COUNT(*) FROM entries", [], |r| r.get(0))
            .expect("recreated index must be queryable");
        assert_eq!(count, 0, "the recreated index starts empty");

        assert!(db_path.is_file(), "a fresh index.db is recreated in place");
        let quarantined: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().contains(".corrupt-"))
            .collect();
        assert_eq!(
            quarantined.len(),
            1,
            "the corrupt index is quarantined (kept for forensics), not silently deleted"
        );
    }

    #[test]
    fn quarantine_corrupt_index_moves_wal_and_shm_sidecars() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("index.db");
        fs::write(&db_path, b"corrupt").unwrap();
        fs::write(dir.path().join("index.db-wal"), b"wal").unwrap();
        fs::write(dir.path().join("index.db-shm"), b"shm").unwrap();

        let quarantined = quarantine_corrupt_index(&db_path).unwrap();
        assert!(quarantined.is_file());
        assert!(!db_path.exists(), "the corrupt db is moved aside");
        assert!(
            !dir.path().join("index.db-wal").exists(),
            "the -wal sidecar is moved aside"
        );
        assert!(
            !dir.path().join("index.db-shm").exists(),
            "the -shm sidecar is moved aside"
        );
        assert!(index_sidecar_path(&quarantined, "-wal").exists());
        assert!(index_sidecar_path(&quarantined, "-shm").exists());
    }

    #[test]
    fn recover_corrupt_index_reuses_a_peer_healed_db_without_requarantine() {
        // Models the concurrency race: a peer already healed the index (the DB
        // at db_path is now a valid empty index). recover_corrupt_index must
        // re-check under the lock, find it healthy, and use it WITHOUT
        // quarantining a healthy DB (which would re-empty it and orphan blobs).
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("index.db");

        // A corruption-shaped error to pass in (as the original open would have).
        let garbage = dir.path().join("garbage.db");
        fs::write(&garbage, b"not a sqlite database").unwrap();
        let err = try_open_index_db(&garbage).unwrap_err();

        // The peer's freshly-healed, valid empty index now lives at db_path.
        drop(try_open_index_db(&db_path).unwrap());

        let (db, recovered) = recover_corrupt_index(&db_path, &err).unwrap();
        let count: i64 = db
            .query_row("SELECT COUNT(*) FROM entries", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
        assert!(
            !recovered,
            "adopting a peer's healed DB must not claim the rebuild: the peer that \
             quarantined it owns that, and two processes rebuilding at once would \
             double-count blob refcounts"
        );

        let quarantined = fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().contains(".corrupt-"))
            .count();
        assert_eq!(
            quarantined, 0,
            "a healthy DB on re-check must not be quarantined"
        );
    }

    #[test]
    fn test_store_open_creates_cache_root() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("nested").join("cache");
        let config = test_config(&cache_dir);

        let _store = Store::open(&config).unwrap();

        assert!(cache_dir.is_dir());
        assert!(config.store_dir().is_dir());
        assert!(config.index_db_path().is_file());
    }

    #[test]
    fn test_store_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 100; // Very small limit to trigger eviction

        let store = Store::open(&config).unwrap();

        // Put a large-ish entry
        let output_file = dir.path().join("big.rlib");
        std::fs::write(&output_file, vec![0u8; 200]).unwrap();

        store
            .put(
                "key1",
                "big_crate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(output_file, "libbig.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        // Age the entry past the active-pin grace so size-pressure eviction can
        // claim it (a just-put entry is "recently accessed" and is now pinned
        // against eviction for EVICTION_IDLE_GRACE — kunobi-ninja/kache#326).
        store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour') WHERE cache_key = 'key1'",
                [],
            )
            .unwrap();

        let stats = store.evict().unwrap();
        assert!(stats.entries_evicted > 0);
        assert!(!store.contains("key1"));
    }

    #[test]
    fn durable_upload_intent_pins_payload_across_every_eviction_policy_until_retired() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 100;
        let store = Store::open(&config).unwrap();
        let pending_key = "a".repeat(64);
        let newer_twin_key = "b".repeat(64);
        let pending_output = dir.path().join("pending.rlib");
        let newer_output = dir.path().join("newer.rlib");
        fs::write(&pending_output, vec![0u8; 200]).unwrap();
        fs::write(&newer_output, vec![1u8; 200]).unwrap();

        store
            .put(
                &pending_key,
                "pending",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(pending_output, "libshared.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        store.set_last_accessed_for_test(&pending_key, "-48 hours");
        let duplicate_group = store
            .db
            .query_row(
                "SELECT content_hash FROM entries WHERE cache_key = ?1",
                params![pending_key.as_str()],
                |row| row.get::<_, String>(0),
            )
            .unwrap();
        store
            .put(
                &newer_twin_key,
                "newer",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(newer_output, "libshared.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        // Form a duplicate group while retaining distinct refcount-1 blobs.
        // Healthy identical entries have zero marginal reclaim and are
        // correctly excluded before the durable-upload pin is consulted.
        store
            .db
            .execute(
                "UPDATE entries SET content_hash = ?1 WHERE cache_key = ?2",
                params![duplicate_group, newer_twin_key.as_str()],
            )
            .unwrap();

        let spool_dir = config.upload_spool_dir();
        fs::create_dir_all(&spool_dir).unwrap();
        let intent = spool_dir.join(format!("{pending_key}.json"));
        // Protection is keyed by the durable filename, not JSON parsing. A
        // malformed intent must fail closed and keep its only upload payload.
        fs::write(&intent, b"{malformed").unwrap();

        let size = store.evict().unwrap();
        assert!(size.entries_pinned >= 1);
        assert!(store.contains(&pending_key));

        let age = store.evict_older_than(24).unwrap();
        assert_eq!(age.entries_pinned, 1);
        assert!(store.contains(&pending_key));

        let duplicate = store.evict_duplicate_entries().unwrap();
        assert_eq!(duplicate.entries_pinned, 1);
        assert!(store.contains(&pending_key));

        fs::remove_file(intent).unwrap();
        let retired = store.evict_duplicate_entries().unwrap();
        assert_eq!(retired.entries_evicted, 1);
        assert!(!store.contains(&pending_key));
        assert!(store.contains(&newer_twin_key));
    }

    #[test]
    fn durable_upload_key_enumeration_is_bounded_and_fails_closed_on_read_error() {
        let key = "c".repeat(64);
        let names = [Ok::<_, std::io::Error>(std::ffi::OsString::from(format!(
            "{key}.json"
        )))];
        let keys = Store::durable_upload_keys_from_names(names, 1).unwrap();
        assert_eq!(keys, std::collections::HashSet::from([key]));

        let overflow = Store::durable_upload_keys_from_names(
            [
                Ok::<_, std::io::Error>(std::ffi::OsString::from("junk")),
                Ok::<_, std::io::Error>(std::ffi::OsString::from("more-junk")),
            ],
            1,
        )
        .unwrap_err();
        assert!(format!("{overflow:#}").contains("exceeds 1 jobs"));

        let unreadable = Store::durable_upload_keys_from_names(
            [Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "injected unreadable spool entry",
            ))],
            1,
        )
        .unwrap_err();
        assert!(format!("{unreadable:#}").contains("injected unreadable spool entry"));

        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        assert!(
            store.durable_upload_keys().unwrap().is_empty(),
            "a missing spool directory is the one empty-set case"
        );

        fs::write(config.upload_spool_dir(), b"not a directory").unwrap();
        let blocked = store.durable_upload_keys().unwrap_err();
        assert!(
            format!("{blocked:#}").contains("reading"),
            "a non-directory spool path must fail closed: {blocked:#}"
        );
    }

    /// #594 step 2, end to end: evicting an entry records a tombstone with the
    /// features the decision used, and a later lookup for that key marks it as
    /// demanded. That demand is the observation a live-store snapshot can never
    /// provide, because the entries it evicted are exactly the ones missing.
    #[test]
    fn eviction_records_a_tombstone_and_a_later_lookup_marks_demand() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 100; // force size pressure
        let store = Store::open(&config).unwrap();

        let out = dir.path().join("big.rlib");
        fs::write(&out, vec![0u8; 4096]).unwrap();
        store
            .put_with_compile_time(
                "doomed",
                "c",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(out, "libbig.rlib".to_string())],
                "",
                "",
                2500,
            )
            .unwrap();
        // Age it past the active-pin grace so it is actually evictable.
        store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-2 hours')",
                [],
            )
            .unwrap();

        assert!(
            store.evict().unwrap().entries_evicted > 0,
            "expected eviction"
        );

        let (key, policy, cost, demanded): (String, String, i64, Option<String>) = store
            .db
            .query_row(
                "SELECT cache_key, policy, compile_time_ms, demanded_at FROM eviction_tombstones",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(key, "doomed");
        assert_eq!(policy, "size-pressure", "records which policy chose it");
        assert_eq!(cost, 2500, "records the rebuild cost that was destroyed");
        assert!(demanded.is_none(), "not demanded yet");
        assert_eq!(store.tombstone_stats().unwrap(), (1, 0));

        // The build asks for it again — exactly the case eviction got wrong.
        assert!(store.get("doomed").unwrap().is_none());
        assert_eq!(store.tombstone_stats().unwrap(), (1, 1));

        // A miss on a key that was never cached must not fabricate a record.
        assert!(store.get("never_existed").unwrap().is_none());
        assert_eq!(store.tombstone_stats().unwrap(), (1, 1));
    }

    /// Only the first demand is recorded — the question is how long after
    /// eviction the key was wanted, so a later repeat must not overwrite it.
    #[test]
    fn tombstone_demand_records_only_the_first_request() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        store
            .db
            .execute(
                "INSERT INTO eviction_tombstones (cache_key, evicted_at, demanded_at)
                 VALUES ('k', datetime('now','-1 hour'), NULL)",
                [],
            )
            .unwrap();

        store.note_tombstone_demand("k");
        let first: String = store
            .db
            .query_row(
                "SELECT demanded_at FROM eviction_tombstones WHERE cache_key='k'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        store.note_tombstone_demand("k");
        let second: String = store
            .db
            .query_row(
                "SELECT demanded_at FROM eviction_tombstones WHERE cache_key='k'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(first, second, "first demand must not be overwritten");
    }

    /// The table is bounded: records age out, and a re-eviction of the same key
    /// starts a fresh observation rather than colliding on the primary key.
    #[test]
    fn tombstones_are_pruned_by_age_and_re_eviction_resets_the_record() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        store
            .db
            .execute(
                "INSERT INTO eviction_tombstones (cache_key, evicted_at) VALUES
                   ('old', datetime('now','-30 days')),
                   ('recent', datetime('now','-1 day'))",
                [],
            )
            .unwrap();

        assert_eq!(store.prune_tombstones(14).unwrap(), 1);
        assert_eq!(store.tombstone_stats().unwrap().0, 1, "recent one survives");

        // Re-evicting a key already demanded must clear the demand so the new
        // observation window starts clean.
        store
            .db
            .execute(
                "UPDATE eviction_tombstones SET demanded_at = datetime('now') WHERE cache_key='recent'",
                [],
            )
            .unwrap();
        let features = crate::eviction::EntryFeatures {
            key: "recent".into(),
            size: 1,
            hit_count: 0,
            idle_hours: 5.0,
            content_hash: None,
            committed: true,
            compile_time_ms: 10,
            reclaimable_bytes: None,
        };
        store.record_tombstone(&features, "size-pressure", Some(("value-density", false)));
        assert_eq!(
            store.tombstone_stats().unwrap(),
            (1, 0),
            "re-eviction restarts the observation"
        );
    }

    /// #594 step 1: rebuild cost must reach the index on write, and reach
    /// eviction through `EntryFeatures` — the whole point is that a policy can
    /// finally see what it is about to destroy.
    #[test]
    fn put_records_compile_time_and_eviction_can_see_it() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let out = dir.path().join("out.rlib");
        fs::write(&out, b"artifact").unwrap();
        store
            .put_with_compile_time(
                "costly",
                "c",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(out, "libout.rlib".to_string())],
                "",
                "",
                4321,
            )
            .unwrap();

        let indexed: i64 = store
            .db
            .query_row(
                "SELECT compile_time_ms FROM entries WHERE cache_key = 'costly'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(indexed, 4321, "put must index the rebuild cost");

        let features = store.eviction_candidates().unwrap();
        let entry = features.iter().find(|e| e.key == "costly").unwrap();
        assert_eq!(
            entry.compile_time_ms, 4321,
            "eviction must see rebuild cost (#594)"
        );
    }

    /// Entries written before the column existed sit at the `0` default; the
    /// GC sweep backfills them from `meta.json`, which has always carried the
    /// value. Converges: a backfilled row is never re-read.
    #[test]
    fn backfill_compile_times_recovers_pre_index_entries() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let out = dir.path().join("out.rlib");
        fs::write(&out, b"artifact").unwrap();
        store
            .put_with_compile_time(
                "legacy",
                "c",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(out, "libout.rlib".to_string())],
                "",
                "",
                7777,
            )
            .unwrap();
        // Simulate a row written before the column existed. meta.json still
        // has the real value — that is what makes recovery possible.
        store
            .db
            .execute("UPDATE entries SET compile_time_ms = 0", [])
            .unwrap();

        assert_eq!(store.backfill_compile_times().unwrap(), 1);
        let restored: i64 = store
            .db
            .query_row(
                "SELECT compile_time_ms FROM entries WHERE cache_key = 'legacy'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(restored, 7777);

        // Second pass finds nothing left to do.
        assert_eq!(store.backfill_compile_times().unwrap(), 0);
    }

    /// The backfill is bounded per sweep so a first GC after upgrade on a large
    /// store cannot stall the daemon while it holds the store mutex; successive
    /// sweeps converge.
    #[test]
    fn backfill_compile_times_is_bounded_per_sweep_and_converges() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Bound under test, not the production constant: the property is
        // "one sweep stops at the limit and the rest converges", which does
        // not depend on the constant's magnitude.
        const LIMIT: i64 = 3;
        let total = LIMIT + 2;
        for i in 0..total {
            let out = dir.path().join(format!("o{i}.rlib"));
            fs::write(&out, format!("artifact-{i}")).unwrap();
            store
                .put_with_compile_time(
                    &format!("k{i}"),
                    "c",
                    &["lib".to_string()],
                    &[],
                    "x86_64-unknown-linux-gnu",
                    "dev",
                    &[(out, format!("libo{i}.rlib"))],
                    "",
                    "",
                    100,
                )
                .unwrap();
        }
        store
            .db
            .execute("UPDATE entries SET compile_time_ms = 0", [])
            .unwrap();

        let first = store.backfill_compile_times_limited(LIMIT).unwrap();
        assert_eq!(
            first, LIMIT as usize,
            "one sweep must not backfill the whole store"
        );
        let second = store.backfill_compile_times_limited(LIMIT).unwrap();
        assert_eq!(second, 2, "the remainder converges on the next sweep");
        assert_eq!(store.backfill_compile_times_limited(LIMIT).unwrap(), 0);
    }

    /// #595 equivalence guard: the Rust `SizePressurePolicy` ranking must match
    /// the SQL `ORDER BY` it replaced, entry for entry. This is the property
    /// that makes the refactor a no-op — if someone later changes the scoring
    /// formula, this test is what tells them they changed behavior, not just
    /// structure. Deliberately uses awkward inputs (zero size, zero idle,
    /// equal scores) since those are where the SQL's MAX() clamps mattered.
    #[test]
    fn size_pressure_policy_matches_the_sql_ordering_it_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // (key, size, hit_count, hours idle)
        let seed = [
            ("huge_stale", 600 * 1024 * 1024_i64, 0_i64, 15.0_f64),
            ("small_hot", 14 * 1024, 9, 0.1),
            ("mid", 5 * 1024 * 1024, 2, 48.0),
            ("zero_size", 0, 0, 3.0),
            ("just_touched", 1024 * 1024, 1, 0.0),
            ("ancient_tiny", 512, 0, 5000.0),
            ("twin_a", 2 * 1024 * 1024, 3, 12.0),
            ("twin_b", 2 * 1024 * 1024, 3, 12.0),
        ];
        for (key, size, hits, idle) in seed {
            store
                .db
                .execute(
                    "INSERT INTO entries (cache_key, crate_name, size, hit_count, committed, last_accessed)
                     VALUES (?1, 'c', ?2, ?3, 1, datetime('now', ?4))",
                    params![key, size, hits, format!("-{} seconds", (idle * 3600.0) as i64)],
                )
                .unwrap();
        }

        // The exact query this refactor removed from `evict()`.
        let sql_order: Vec<String> = {
            let mut stmt = store
                .db
                .prepare(
                    "SELECT cache_key FROM entries
                     ORDER BY
                       CAST((hit_count + 1) AS REAL)
                       / (MAX((julianday('now') - julianday(last_accessed)) * 24.0, 0.01)
                          * MAX(size / 1048576.0, 0.001))
                       ASC",
                )
                .unwrap();
            stmt.query_map([], |r| r.get(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };

        let candidates = store.eviction_candidates().unwrap();
        let policy_order = crate::eviction::SizePressurePolicy.select(&candidates);

        // Compare by score rather than raw position: SQLite and Rust may break
        // exact ties (twin_a/twin_b) in either order, and that is not a
        // behavior difference. Any genuine ranking divergence still fails.
        let score_of: std::collections::HashMap<&str, f64> = candidates
            .iter()
            .map(|e| (e.key.as_str(), crate::eviction::size_pressure_score(e)))
            .collect();
        let seq =
            |order: &[String]| -> Vec<f64> { order.iter().map(|k| score_of[k.as_str()]).collect() };
        assert_eq!(
            seq(&sql_order),
            seq(&policy_order),
            "policy ranking diverged from the SQL it replaced\n  sql:    {sql_order:?}\n  policy: {policy_order:?}"
        );
        assert_eq!(policy_order.len(), seed.len(), "every entry must be ranked");
    }

    /// Age must agree with its former SQL. Duplicate eviction additionally
    /// requires proven marginal bytes, so legacy rows without `entry_blobs`
    /// deliberately fail closed instead of matching the former SQL.
    #[test]
    fn older_than_matches_former_sql_while_duplicate_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        for (key, idle_h, hash) in [
            ("stale", 100.0_f64, Some("h1")),
            ("fresh", 1.0, Some("h1")),
            ("boundary", 24.0, None),
            ("lonely", 200.0, Some("h2")),
        ] {
            store
                .db
                .execute(
                    "INSERT INTO entries (cache_key, crate_name, size, committed, content_hash, last_accessed)
                     VALUES (?1, 'c', 100, 1, ?2, datetime('now', ?3))",
                    params![key, hash, format!("-{} seconds", (idle_h * 3600.0) as i64)],
                )
                .unwrap();
        }

        let candidates = store.eviction_candidates().unwrap();

        let sql_old: Vec<String> = {
            let mut stmt = store
                .db
                .prepare("SELECT cache_key FROM entries WHERE last_accessed < datetime('now', '-24 hours')")
                .unwrap();
            stmt.query_map([], |r| r.get(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };
        let mut policy_old = crate::eviction::OlderThanPolicy { hours: 24 }.select(&candidates);
        policy_old.sort();

        // Away from the cutoff the two agree exactly. Do not assert boundary
        // membership here: insertion and selection evaluate separate
        // `datetime('now')` calls, so a second rollover can move `boundary`
        // across the old SQL cutoff. Strict cutoff behavior is covered
        // deterministically by `OlderThanPolicy`'s pure unit test.
        let unambiguous = |v: &[String]| -> Vec<String> {
            let mut v: Vec<String> = v.iter().filter(|k| *k != "boundary").cloned().collect();
            v.sort();
            v
        };
        assert_eq!(
            unambiguous(&policy_old),
            unambiguous(&sql_old),
            "older-than selection diverged away from the cutoff boundary"
        );

        let sql_dup: Vec<String> = {
            let mut stmt = store
                .db
                .prepare(
                    "SELECT e.cache_key FROM entries e
                     JOIN (SELECT content_hash, MAX(last_accessed) AS newest
                           FROM entries WHERE content_hash IS NOT NULL AND committed = 1
                           GROUP BY content_hash HAVING COUNT(*) > 1) d
                       ON e.content_hash = d.content_hash
                     WHERE e.last_accessed < d.newest AND e.committed = 1",
                )
                .unwrap();
            stmt.query_map([], |r| r.get(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };
        let mut policy_dup = crate::eviction::DuplicatePolicy.select(&candidates);
        let mut sql_dup_sorted = sql_dup.clone();
        policy_dup.sort();
        sql_dup_sorted.sort();
        assert_eq!(
            sql_dup_sorted,
            vec!["stale"],
            "former SQL selected the older twin without proving reclaimed bytes"
        );
        assert!(
            policy_dup.is_empty(),
            "unmapped legacy victims must fail closed on unknown marginal bytes"
        );
    }

    /// kunobi-ninja/kache#326, #182: size-pressure eviction must NOT delete an
    /// entry a live build just accessed (it may be mid-restore — the active-pin
    /// guard keys off `last_accessed`, which `get` bumps before the wrapper
    /// hardlinks the blobs). A recently-accessed entry survives; aging it past
    /// the grace window lets it be evicted.
    #[test]
    fn evict_skips_recently_accessed_entry() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 100; // tiny limit → over capacity → wants to evict

        let store = Store::open(&config).unwrap();
        let output_file = dir.path().join("big.rlib");
        std::fs::write(&output_file, vec![0u8; 200]).unwrap();
        store
            .put(
                "live_key",
                "live_crate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(output_file, "libbig.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        // Fresh put → last_accessed = now → within the grace window → pinned.
        let stats = store.evict().unwrap();
        assert_eq!(
            stats.entries_evicted, 0,
            "a recently-accessed entry must be pinned against eviction"
        );
        assert_eq!(
            stats.entries_pinned, 1,
            "and it must be COUNTED as held back — that count is the whole \
             difference between `evicted 0 entries` reading as a broken GC and \
             explaining itself (#509)"
        );
        assert!(store.contains("live_key"));

        // Age it past the grace window → no longer pinned → evictable.
        store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour') WHERE cache_key = 'live_key'",
                [],
            )
            .unwrap();
        let stats = store.evict().unwrap();
        assert!(stats.entries_evicted > 0);
        assert!(!store.contains("live_key"));
    }

    /// kunobi-ninja/kache#326: the recency guard is eviction-only. Explicit
    /// `remove_entry` (purge / `doctor`) must remove a just-accessed entry.
    #[test]
    fn remove_entry_ignores_recency_guard() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let output_file = dir.path().join("x.rlib");
        std::fs::write(&output_file, b"content").unwrap();
        store
            .put(
                "rk",
                "c",
                &["lib".to_string()],
                &[],
                "",
                "dev",
                &[(output_file, "libx.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        // Just put → recent. The guarded path skips it…
        assert!(
            store
                .remove_entry_guarded("rk", Some(EVICTION_IDLE_GRACE))
                .unwrap()
                .is_none(),
            "guarded removal must skip a recently-accessed entry"
        );
        assert!(store.contains("rk"));

        // …but the unguarded public path removes it regardless of recency.
        store.remove_entry("rk").unwrap();
        assert!(!store.contains("rk"));
    }

    #[test]
    fn test_incremental_dir_registry_deduplicates_and_cleans() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let incremental_dir = dir.path().join("target/debug/incremental");
        std::fs::create_dir_all(&incremental_dir).unwrap();
        std::fs::write(incremental_dir.join("junk"), b"tmp").unwrap();

        store.remember_incremental_dir(&incremental_dir).unwrap();
        store.remember_incremental_dir(&incremental_dir).unwrap();
        store
            .remember_incremental_dir(&dir.path().join("missing/incremental"))
            .unwrap();

        let count_before: i64 = store
            .db
            .query_row("SELECT COUNT(*) FROM incremental_dirs", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count_before, 2);

        let cleaned = store.clean_registered_incremental_dirs().unwrap();
        assert_eq!(cleaned, 1);
        assert!(!incremental_dir.exists());

        let count_after: i64 = store
            .db
            .query_row("SELECT COUNT(*) FROM incremental_dirs", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count_after, 0);
    }

    #[test]
    fn clean_registered_incremental_dirs_prunes_a_non_directory_path() {
        // A registered incremental path that now points at a *file* (not a dir)
        // is pruned without being counted as cleaned. Covers the
        // `!path.is_dir()` branch of clean_registered_incremental_dirs.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let bogus = dir.path().join("not-a-dir");
        std::fs::write(&bogus, b"i am a file").unwrap();
        store.remember_incremental_dir(&bogus).unwrap();

        let cleaned = store.clean_registered_incremental_dirs().unwrap();
        assert_eq!(cleaned, 0, "a non-directory is pruned, not cleaned");
        // The file is left in place (we only remove directories), but its row is gone.
        assert!(bogus.exists(), "the non-directory file is not deleted");
        let remaining: i64 = store
            .db
            .query_row("SELECT COUNT(*) FROM incremental_dirs", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(remaining, 0, "the bogus registration was pruned");
    }

    #[cfg(unix)]
    #[test]
    fn clean_registered_incremental_dirs_keeps_row_when_remove_fails() {
        // Covers clean_registered_incremental_dirs remove_dir_all error branch.
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let parent = dir.path().join("readonly-parent");
        let incremental_dir = parent.join("incremental");
        std::fs::create_dir_all(&incremental_dir).unwrap();
        std::fs::write(incremental_dir.join("junk"), b"tmp").unwrap();
        store.remember_incremental_dir(&incremental_dir).unwrap();

        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o500)).unwrap();
        let cleaned = store.clean_registered_incremental_dirs().unwrap();
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o700)).unwrap();

        assert_eq!(cleaned, 0, "failed removals are not counted as cleaned");
        assert!(incremental_dir.exists(), "failed removal leaves the dir");
        let remaining: i64 = store
            .db
            .query_row("SELECT COUNT(*) FROM incremental_dirs", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(remaining, 1, "failed removal keeps the registry row");
    }

    #[test]
    fn test_store_locking() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let lock1 = match store.claim_build("testkey").unwrap() {
            BuildClaim::Acquired(lock) => lock,
            BuildClaim::Committed(_) | BuildClaim::Contended => {
                panic!("first build claim should acquire the key")
            }
        };

        assert!(matches!(
            store.claim_build("testkey").unwrap(),
            BuildClaim::Contended
        ));

        drop(lock1);

        assert!(matches!(
            store.claim_build("testkey").unwrap(),
            BuildClaim::Acquired(_)
        ));
    }

    #[test]
    fn claim_build_rechecks_entry_after_acquiring_lock() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let peer = Store::open(&config).unwrap();
        let waiter = Store::open(&config).unwrap();
        let cache_key = "committed_during_claim_race";

        let peer_lock = match peer.claim_build(cache_key).unwrap() {
            BuildClaim::Acquired(lock) => lock,
            BuildClaim::Committed(_) | BuildClaim::Contended => {
                panic!("peer should acquire the initial build claim")
            }
        };
        assert!(matches!(
            waiter.claim_build(cache_key).unwrap(),
            BuildClaim::Contended
        ));

        let output = dir.path().join("lib.rlib");
        fs::write(&output, b"peer output").unwrap();
        peer.put(
            cache_key,
            "peer",
            &["rlib".to_string()],
            &[],
            "host",
            "dev",
            &[(output, "lib.rlib".to_string())],
            "",
            "",
        )
        .unwrap();
        drop(peer_lock);

        match waiter.claim_build(cache_key).unwrap() {
            BuildClaim::Committed(meta) => assert_eq!(meta.cache_key, cache_key),
            BuildClaim::Acquired(_) => panic!("committed entry must prevent a duplicate compile"),
            BuildClaim::Contended => panic!("peer already released the build lock"),
        }
        assert!(
            waiter.try_lock(cache_key).unwrap().is_some(),
            "serving the committed entry must release the claim"
        );
    }

    #[test]
    fn claim_build_evicts_empty_committed_entry() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let cache_key = "empty_committed_entry";
        let entry_dir = store.entry_dir(cache_key);
        fs::create_dir_all(&entry_dir).unwrap();
        let meta = EntryMeta {
            cache_key: cache_key.to_string(),
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            crate_name: "empty".to_string(),
            crate_types: vec!["rlib".to_string()],
            files: vec![],
            stdout: String::new(),
            stderr: String::new(),
            features: vec![],
            target: "host".to_string(),
            profile: "dev".to_string(),
            compile_time_ms: 0,
            emit_kinds: Vec::new(),
        };
        fs::write(
            entry_dir.join("meta.json"),
            serde_json::to_string_pretty(&meta).unwrap(),
        )
        .unwrap();
        store
            .db
            .execute(
                "INSERT INTO entries (cache_key, crate_name, size, committed) VALUES (?1, ?2, 0, 1)",
                params![cache_key, "empty"],
            )
            .unwrap();

        match store.claim_build(cache_key).unwrap() {
            BuildClaim::Acquired(_) => {}
            BuildClaim::Committed(_) => panic!("empty entry must not be served"),
            BuildClaim::Contended => panic!("no peer owns the build lock"),
        }
        assert!(store.get(cache_key).unwrap().is_none());
        assert!(!entry_dir.exists());
        assert!(store.try_lock(cache_key).unwrap().is_some());
    }

    #[test]
    fn prepared_key_lock_is_complete_before_atomic_publication() -> anyhow::Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("entry.lock");
        let first = PreparedKeyLock::new(lock_path.clone()).unwrap();

        assert!(
            !lock_path.exists(),
            "the canonical path must stay absent while PID metadata is prepared"
        );
        assert_eq!(
            fs::read_to_string(first.temp.path()).unwrap(),
            std::process::id().to_string()
        );

        let winner = PreparedKeyLock::new(lock_path.clone())?
            .publish()?
            .expect("one prepared contender should publish");
        assert_eq!(
            fs::read_to_string(&lock_path).unwrap(),
            std::process::id().to_string(),
            "a visible lock must already contain a complete PID"
        );
        assert!(
            first.publish()?.is_none(),
            "noclobber publication must preserve the existing owner"
        );

        drop(winner);
        assert!(!lock_path.exists());
        Ok(())
    }

    #[test]
    fn concurrent_stale_lock_recovery_has_one_winner() {
        const CONTENDERS: usize = 16;
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let lock_path = store.entry_dir("stale-race").with_extension("lock");
        fs::create_dir_all(lock_path.parent().unwrap()).unwrap();
        fs::write(&lock_path, b"not-a-pid").unwrap();

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(CONTENDERS));
        let mut handles = Vec::new();
        for _ in 0..CONTENDERS {
            let config = test_config(dir.path());
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                let store = Store::open(&config).unwrap();
                barrier.wait();
                store.try_lock("stale-race").unwrap()
            }));
        }

        let guards: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        assert_eq!(
            guards.iter().filter(|guard| guard.is_some()).count(),
            1,
            "serialized stale recovery must publish exactly one live guard"
        );
    }

    #[test]
    fn try_lock_recovers_unparseable_stale_lock() {
        // Covers stale lock removal and retry-acquire branch.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let lock_path = store.entry_dir("stale_key").with_extension("lock");
        fs::create_dir_all(lock_path.parent().unwrap()).unwrap();
        fs::write(&lock_path, b"not-a-pid").unwrap();

        let lock = store.try_lock("stale_key").unwrap();

        assert!(lock.is_some(), "stale lock should be replaced");
        assert_eq!(
            fs::read_to_string(&lock_path).unwrap(),
            std::process::id().to_string()
        );
        drop(lock);
        assert!(!lock_path.exists(), "dropping the guard removes the lock");
    }

    #[test]
    fn test_store_clear() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output_file = dir.path().join("out.rlib");
        std::fs::write(&output_file, b"content").unwrap();

        store
            .put(
                "k1",
                "c1",
                &["lib".to_string()],
                &[],
                "",
                "dev",
                &[(output_file.clone(), "lib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        assert!(store.contains("k1"));
        store.clear().unwrap();
        assert!(!store.contains("k1"));
    }

    #[test]
    fn test_store_entry_dir() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let entry_dir = store.entry_dir("abc123");
        assert!(entry_dir.to_string_lossy().contains("store"));
        assert!(entry_dir.to_string_lossy().contains("abc123"));
    }

    #[test]
    fn test_store_cached_file_path() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let path = store.cached_file_path("key1", "libfoo.rlib");
        assert!(path.to_string_lossy().contains("key1"));
        assert!(path.to_string_lossy().ends_with("libfoo.rlib"));
    }

    #[test]
    fn test_store_total_size_empty() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        assert_eq!(store.total_size().unwrap(), 0);
    }

    #[test]
    fn test_store_entry_count_empty() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        assert_eq!(store.entry_count().unwrap(), 0);
    }

    #[test]
    fn test_store_entry_count_after_put() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("a.rlib");
        std::fs::write(&output, b"data").unwrap();
        store
            .put(
                "k1",
                "c1",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output.clone(), "a.rlib".into())],
                "",
                "",
            )
            .unwrap();

        rewrite_source(&output, b"data2");
        store
            .put(
                "k2",
                "c2",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "b.rlib".into())],
                "",
                "",
            )
            .unwrap();

        assert_eq!(store.entry_count().unwrap(), 2);
    }

    #[test]
    fn test_store_contains_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        assert!(!store.contains("nonexistent_key"));
    }

    #[test]
    fn test_store_get_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        assert!(store.get("nonexistent_key").unwrap().is_none());
    }

    #[test]
    fn test_store_remove_entry() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        std::fs::write(&output, b"content").unwrap();
        store
            .put(
                "rem1",
                "c1",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        assert!(store.contains("rem1"));

        store.remove_entry("rem1").unwrap();
        assert!(!store.contains("rem1"));
        assert_eq!(store.entry_count().unwrap(), 0);
    }

    /// #276: removing an entry whose meta.json is unparseable must NOT delete
    /// the entry row or silently drop blob refcounts — that orphans the blobs
    /// forever (they keep a DB row and evade size-based eviction). It must
    /// refuse, leaving the entry and its refcounts intact.
    #[test]
    fn remove_entry_refuses_on_corrupt_meta_no_refcount_leak() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        std::fs::write(&output, b"content").unwrap();
        store
            .put(
                "corrupt1",
                "c1",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        let refcount_sum = |s: &Store| -> i64 {
            s.db.query_row("SELECT COALESCE(SUM(refcount), 0) FROM blobs", [], |r| {
                r.get(0)
            })
            .unwrap()
        };
        let row_present = |s: &Store| -> i64 {
            s.db.query_row(
                "SELECT COUNT(*) FROM entries WHERE cache_key = 'corrupt1'",
                [],
                |r| r.get(0),
            )
            .unwrap()
        };
        assert_eq!(refcount_sum(&store), 1, "one blob at refcount 1 after put");
        assert_eq!(row_present(&store), 1);

        // Corrupt the entry's meta.json so its blob list can't be loaded.
        let meta_path = store.entry_dir("corrupt1").join("meta.json");
        std::fs::write(&meta_path, b"{ not valid json").unwrap();

        assert!(
            store.remove_entry("corrupt1").is_err(),
            "remove_entry must error on unparseable meta.json rather than leak"
        );
        assert_eq!(
            row_present(&store),
            1,
            "corrupt entry row must survive a refused removal"
        );
        assert_eq!(
            refcount_sum(&store),
            1,
            "blob refcounts must be unchanged — no orphan"
        );
    }

    /// #276: a missing meta.json while the DB row persists is the same hazard.
    #[test]
    fn remove_entry_refuses_when_meta_missing_but_row_present() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let output = dir.path().join("lib.rlib");
        std::fs::write(&output, b"x").unwrap();
        store
            .put(
                "m1",
                "c1",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        std::fs::remove_file(store.entry_dir("m1").join("meta.json")).unwrap();
        let err = store.remove_entry("m1").unwrap_err();
        // The message identifies WHICH state was diagnosed: the settled
        // missing-meta shape, not the transient-recheck refusal — a removal
        // that misclassifies the settled state would route corrupt entries
        // through the wrong recovery advice.
        assert!(
            format!("{err:#}").contains("meta.json missing but DB row present"),
            "wrong refusal shape: {err:#}"
        );
        let still_there: i64 = store
            .db
            .query_row(
                "SELECT COUNT(*) FROM entries WHERE cache_key = 'm1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(still_there, 1, "entry row must survive a refused removal");
    }

    /// An unreadable meta.json (EACCES, not NotFound) must refuse through the
    /// unreadable-meta arm — "reading meta.json" — not be misread as missing
    /// and routed into the missing-meta bounce, whose diagnostics describe a
    /// different state (#276, #670). The distinction is the arm's NotFound
    /// guard; this pins it against being widened to every error.
    #[cfg(unix)]
    #[test]
    fn remove_entry_refuses_unreadable_meta_as_a_read_error() {
        use std::os::unix::fs::PermissionsExt;
        if unsafe { libc::geteuid() } == 0 {
            eprintln!("skipping: running as root, mode 000 does not deny access");
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let output = dir.path().join("lib.rlib");
        std::fs::write(&output, b"x").unwrap();
        store
            .put(
                "locked",
                "c1",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        let entry_dir = store.entry_dir("locked");
        std::fs::set_permissions(&entry_dir, std::fs::Permissions::from_mode(0o000)).unwrap();
        let err = store.remove_entry("locked").unwrap_err();
        std::fs::set_permissions(&entry_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(
            format!("{err:#}").contains("reading meta.json"),
            "an unreadable meta must refuse as a read error, got: {err:#}"
        );
        assert!(
            store.contains("locked"),
            "entry row must survive a refused removal"
        );
    }

    /// #211: blob path construction must be panic-safe for a malformed (short)
    /// hash that bypasses validation; it must not slice `[..2]` on `len < 2`.
    #[test]
    fn blob_path_is_panic_safe_for_short_hash() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        // Would panic on `&hash[..2]` before the fix.
        let _ = store.blob_path("a");
        let _ = store.blob_path("");
    }

    #[test]
    fn test_store_remove_entry_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Should not error
        store.remove_entry("nonexistent").unwrap();
    }

    #[test]
    fn test_store_list_entries_empty() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let entries = store.list_entries("name").unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_store_list_entries_sort_by() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let out1 = dir.path().join("a.rlib");
        std::fs::write(&out1, vec![0u8; 100]).unwrap();
        store
            .put(
                "k1",
                "alpha",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(out1, "a.rlib".into())],
                "",
                "",
            )
            .unwrap();

        let out2 = dir.path().join("b.rlib");
        std::fs::write(&out2, vec![0u8; 200]).unwrap();
        store
            .put(
                "k2",
                "beta",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(out2, "b.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // Sort by name
        let entries = store.list_entries("name").unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].crate_name, "alpha");

        // Sort by size
        let entries = store.list_entries("size").unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries[0].size >= entries[1].size);

        // Sort by hits
        let entries = store.list_entries("hits").unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn list_entries_errors_on_non_integer_size_row() {
        // Covers list_entries row decoding error branch.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        store
            .db
            .execute(
                "INSERT INTO entries \
                 (cache_key, crate_name, crate_type, profile, size, committed) \
                 VALUES ('bad_size', 'bad', 'lib', 'dev', x'01', 1)",
                [],
            )
            .unwrap();

        let err = store.list_entries("name").unwrap_err();

        assert!(
            err.to_string().contains("Invalid column type"),
            "expected SQLite type error, got: {err}"
        );
    }

    #[test]
    fn test_store_evict_older_than() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        std::fs::write(&output, b"content").unwrap();
        store
            .put(
                "k1",
                "c1",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // Backdate the entry so eviction is deterministic (not timing-dependent)
        store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-48 hours') WHERE cache_key = 'k1'",
                [],
            )
            .unwrap();

        // Evict entries older than 24 hours — our backdated entry qualifies
        let stats = store.evict_older_than(24).unwrap();
        assert_eq!(stats.entries_evicted, 1);
        assert!(!store.contains("k1"));
    }

    #[test]
    fn test_store_evict_older_than_keeps_recent() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        std::fs::write(&output, b"content").unwrap();
        store
            .put(
                "k1",
                "c1",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // Evict entries older than 9999 hours — nothing should be evicted
        let stats = store.evict_older_than(9999).unwrap();
        assert_eq!(stats.entries_evicted, 0);
        assert!(store.contains("k1"));
    }

    #[test]
    fn evict_stale_key_schemas_keeps_only_the_running_schema() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        for (key, content) in [
            ("current", b"current artifact".as_slice()),
            ("old", b"old artifact".as_slice()),
            ("legacy", b"legacy artifact".as_slice()),
        ] {
            let output = dir.path().join(format!("{key}.rlib"));
            std::fs::write(&output, content).unwrap();
            store
                .put(
                    key,
                    key,
                    &["lib".into()],
                    &[],
                    "",
                    "dev",
                    &[(output, format!("{key}.rlib"))],
                    "",
                    "",
                )
                .unwrap();
        }

        let prior_schema = crate::cache_key::CACHE_KEY_VERSION.saturating_sub(1);
        store
            .db
            .execute(
                "UPDATE entries
                 SET key_schema = ?1, last_accessed = datetime('now', '-1 day')
                 WHERE cache_key = 'old'",
                params![prior_schema],
            )
            .unwrap();
        store
            .db
            .execute(
                "UPDATE entries
                 SET key_schema = 0, last_accessed = datetime('now', '-1 day')
                 WHERE cache_key = 'legacy'",
                [],
            )
            .unwrap();

        let stats = store
            .evict_stale_key_schemas(crate::cache_key::CACHE_KEY_VERSION)
            .unwrap();
        assert_eq!(stats.entries_evicted, 2);
        assert!(stats.bytes_freed > 0);
        assert_eq!(stats.blobs_removed, 2);
        assert_eq!(stats.entries_pinned, 0);
        assert!(store.contains("current"));
        assert!(!store.contains("old"));
        assert!(!store.contains("legacy"));
        assert_eq!(store.entry_count().unwrap(), 1);

        let second = store
            .evict_stale_key_schemas(crate::cache_key::CACHE_KEY_VERSION)
            .unwrap();
        assert_eq!(second.entries_evicted, 0);
    }

    #[test]
    fn entry_meta_key_schema_defaults_to_unknown_for_legacy_json() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let output = dir.path().join("lib.rlib");
        std::fs::write(&output, b"artifact").unwrap();
        store
            .put(
                "key",
                "crate",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        let content = std::fs::read_to_string(store.entry_dir("key").join("meta.json")).unwrap();
        let current: EntryMeta = serde_json::from_str(&content).unwrap();
        assert_eq!(current.key_schema, crate::cache_key::CACHE_KEY_VERSION);

        let mut legacy: serde_json::Value = serde_json::from_str(&content).unwrap();
        legacy.as_object_mut().unwrap().remove("key_schema");
        let parsed: EntryMeta = serde_json::from_value(legacy).unwrap();
        assert_eq!(parsed.key_schema, 0);
    }

    #[test]
    fn test_store_import_downloaded_entry() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Create a fake downloaded entry directory
        let entry_dir = config.store_dir().join("downloaded_key");
        std::fs::create_dir_all(&entry_dir).unwrap();

        let artifact_content = b"fake artifact";
        std::fs::write(entry_dir.join("lib.rlib"), artifact_content).unwrap();
        // Real content hash — the import trust boundary re-hashes and rejects a
        // mismatch (kunobi-ninja/kache#211).
        let hash = crate::cache_key::hash_file(&entry_dir.join("lib.rlib")).unwrap();
        let prior_schema = crate::cache_key::CACHE_KEY_VERSION.saturating_sub(1);
        let meta = EntryMeta {
            cache_key: "downloaded_key".to_string(),
            key_schema: prior_schema,
            crate_name: "downloaded_crate".to_string(),
            crate_types: vec!["lib".to_string()],
            files: vec![CachedFile {
                name: "lib.rlib".to_string(),
                size: artifact_content.len() as u64,
                hash,
                executable: false,
            }],
            stdout: String::new(),
            stderr: String::new(),
            features: vec!["std".to_string()],
            target: "x86_64-unknown-linux-gnu".to_string(),
            profile: "dev".to_string(),
            compile_time_ms: 0,
            emit_kinds: Vec::new(),
        };
        let meta_json = serde_json::to_string_pretty(&meta).unwrap();
        std::fs::write(entry_dir.join("meta.json"), meta_json).unwrap();

        store.import_downloaded_entry("downloaded_key").unwrap();
        assert!(store.contains("downloaded_key"));
        assert_eq!(store.entry_count().unwrap(), 1);
        let indexed_schema: u32 = store
            .db
            .query_row(
                "SELECT key_schema FROM entries WHERE cache_key = 'downloaded_key'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(indexed_schema, prior_schema);
    }

    #[test]
    fn test_store_import_downloaded_entry_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Create entry directory with meta.json but NO artifact file
        let entry_dir = config.store_dir().join("incomplete_key");
        std::fs::create_dir_all(&entry_dir).unwrap();

        let meta = EntryMeta {
            cache_key: "incomplete_key".to_string(),
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            crate_name: "incomplete_crate".to_string(),
            crate_types: vec!["lib".to_string()],
            files: vec![CachedFile {
                name: "lib.rlib".to_string(),
                size: 42,
                // Valid-shaped hash so validation reaches the missing-file check.
                hash: "a".repeat(64),
                executable: false,
            }],
            stdout: String::new(),
            stderr: String::new(),
            features: vec![],
            target: String::new(),
            profile: "dev".to_string(),
            compile_time_ms: 0,
            emit_kinds: Vec::new(),
        };
        let meta_json = serde_json::to_string_pretty(&meta).unwrap();
        std::fs::write(entry_dir.join("meta.json"), meta_json).unwrap();
        // Deliberately NOT creating lib.rlib

        let err = store.import_downloaded_entry("incomplete_key").unwrap_err();
        assert!(
            err.to_string().contains("missing file"),
            "expected 'missing file' error, got: {err}"
        );
        assert!(!store.contains("incomplete_key"));
    }

    #[test]
    fn test_import_downloaded_entry_creates_blobs() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Simulate a downloaded entry (old tar format: files in entry dir)
        let entry_dir = config.store_dir().join("dl_key");
        fs::create_dir_all(&entry_dir).unwrap();
        fs::write(entry_dir.join("lib.rlib"), b"artifact data").unwrap();

        let hash = crate::cache_key::hash_file(&entry_dir.join("lib.rlib")).unwrap();
        let meta = EntryMeta {
            cache_key: "dl_key".to_string(),
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            crate_name: "dl_crate".to_string(),
            crate_types: vec!["lib".to_string()],
            files: vec![CachedFile {
                name: "lib.rlib".to_string(),
                size: 13,
                hash: hash.clone(),
                executable: false,
            }],
            stdout: String::new(),
            stderr: String::new(),
            features: vec![],
            target: String::new(),
            profile: "dev".to_string(),
            compile_time_ms: 0,
            emit_kinds: Vec::new(),
        };
        fs::write(
            entry_dir.join("meta.json"),
            serde_json::to_string_pretty(&meta).unwrap(),
        )
        .unwrap();

        store.import_downloaded_entry("dl_key").unwrap();

        // Blob should exist
        let blob = store.blob_path(&hash);
        assert!(
            blob.exists(),
            "blob should be created from downloaded artifact"
        );

        // Entry dir artifact should be gone (only meta.json remains)
        assert!(
            !entry_dir.join("lib.rlib").exists(),
            "artifact should have been moved to blob store"
        );
        assert!(
            entry_dir.join("meta.json").exists(),
            "meta.json should remain"
        );

        // Blob should be read-only
        let perms = fs::metadata(&blob).unwrap().permissions();
        assert!(perms.readonly(), "imported blob should be read-only");

        // Refcount should be 1 in the blobs table
        let refcount: i64 = store
            .db
            .query_row(
                "SELECT refcount FROM blobs WHERE hash = ?1",
                params![&hash],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(refcount, 1);

        // Entry should be committed
        assert!(store.contains("dl_key"));
    }

    #[test]
    fn test_store_get_evicts_entry_with_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Put a valid entry
        let output = dir.path().join("lib.rlib");
        std::fs::write(&output, b"content").unwrap();
        store
            .put(
                "damaged_key",
                "damaged_crate",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        assert!(store.contains("damaged_key"));

        // Simulate corruption: delete the blob file from the store
        let meta_content =
            std::fs::read_to_string(store.entry_dir("damaged_key").join("meta.json")).unwrap();
        let meta: EntryMeta = serde_json::from_str(&meta_content).unwrap();
        let blob = store.blob_path(&meta.files[0].hash);
        // Make writable so we can delete
        let mut perms = std::fs::metadata(&blob).unwrap().permissions();
        perms.set_readonly(false);
        std::fs::set_permissions(&blob, perms).unwrap();
        std::fs::remove_file(&blob).unwrap();

        // get() should detect the missing file, evict, and return None
        let result = store.get("damaged_key").unwrap();
        assert!(
            result.is_none(),
            "expected None for entry with missing file"
        );
        assert!(
            !store.contains("damaged_key"),
            "entry should have been evicted"
        );
    }

    #[test]
    fn test_store_get_evicts_entry_with_corrupted_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Put a valid entry
        let output = dir.path().join("lib.rlib");
        std::fs::write(&output, b"valid rlib content here").unwrap();
        store
            .put(
                "corrupt_key",
                "corrupt_crate",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        assert!(store.contains("corrupt_key"));

        // Simulate corruption: truncate the blob to a different size
        let meta_content =
            std::fs::read_to_string(store.entry_dir("corrupt_key").join("meta.json")).unwrap();
        let meta: EntryMeta = serde_json::from_str(&meta_content).unwrap();
        let blob = store.blob_path(&meta.files[0].hash);
        let mut perms = std::fs::metadata(&blob).unwrap().permissions();
        perms.set_readonly(false);
        std::fs::set_permissions(&blob, perms).unwrap();
        std::fs::write(&blob, b"short").unwrap();

        // get() should detect the size mismatch, evict, and return None
        let result = store.get("corrupt_key").unwrap();
        assert!(
            result.is_none(),
            "expected None for entry with size-corrupted file"
        );
        assert!(
            !store.contains("corrupt_key"),
            "entry should have been evicted"
        );
    }

    #[cfg(unix)]
    #[test]
    fn get_evicts_when_verified_blob_is_unreadable() {
        // Covers get verification hash_file error branch.
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        fs::write(&output, b"readable before chmod").unwrap();
        store
            .put(
                "unreadable_key",
                "unreadable_crate",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        let meta = store.get("unreadable_key").unwrap().unwrap();
        let blob = store.blob_path(&meta.files[0].hash);
        fs::set_permissions(&blob, fs::Permissions::from_mode(0o000)).unwrap();

        let _env_lock = ENV_VAR_TEST_LOCK.lock().unwrap();
        let _verify = EnvVarGuard::set("KACHE_VERIFY_RESTORES", "always");
        let result = store.get("unreadable_key").unwrap();

        assert!(result.is_none(), "unreadable verified blob is evicted");
        assert!(!store.contains("unreadable_key"));
    }

    #[test]
    fn test_store_put_rejects_zero_byte_artifact() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Create a zero-byte file
        let output = dir.path().join("empty.rlib");
        std::fs::write(&output, b"").unwrap();

        let err = store
            .put(
                "zero_key",
                "zero_crate",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "empty.rlib".into())],
                "",
                "",
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("zero-byte"),
            "expected 'zero-byte' error, got: {err}"
        );
        assert!(!store.contains("zero_key"));
    }

    #[test]
    fn test_store_put_accepts_zero_byte_rmeta() {
        // `cargo check` / `cargo clippy --all-targets` compile test and bin units
        // with `--emit=metadata`, and rustc writes an empty `.rmeta` for them.
        // The entry — and the non-empty siblings the old guard took down with it
        // — must still cache (kunobi-ninja/kache#624).
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let rmeta = dir.path().join("libit-15ba26cbaff655a7.rmeta");
        std::fs::write(&rmeta, b"").unwrap();
        let depinfo = dir.path().join("it-15ba26cbaff655a7.d");
        std::fs::write(&depinfo, b"it: tests/it.rs\n").unwrap();

        store
            .put(
                "zero_rmeta_key",
                "it",
                // A `--test` unit: cargo passes no `--crate-type`, so the
                // wrapper records none — the shape observed on a real
                // `cargo check --all-targets`.
                &[],
                &[],
                "",
                "dev",
                &[
                    (rmeta, "libit-15ba26cbaff655a7.rmeta".into()),
                    (depinfo, "it-15ba26cbaff655a7.d".into()),
                ],
                "",
                "",
            )
            .unwrap();

        let meta = store.get("zero_rmeta_key").unwrap().unwrap();
        assert_eq!(meta.files.len(), 2, "sibling outputs survive the empty one");
        let stored_rmeta = meta
            .files
            .iter()
            .find(|f| f.name.ends_with(".rmeta"))
            .expect("rmeta stored");
        assert_eq!(stored_rmeta.size, 0);
        assert_eq!(
            store
                .blob_path(&stored_rmeta.hash)
                .metadata()
                .unwrap()
                .len(),
            0,
            "empty blob materialized in the content store"
        );
        // The emit-coverage gate still sees `metadata` (kunobi-ninja/kache#325),
        // so a `--emit=metadata` invocation can hit this entry.
        assert!(meta.emit_kinds.iter().any(|k| k == "metadata"));
    }

    #[test]
    fn test_store_put_rejects_zero_byte_rmeta_from_a_library_unit() {
        // A `lib` unit HAS metadata to emit, so an empty `.rmeta` there is a
        // truncated write — the exemption for test/bin units must not reopen
        // the guard for libraries (kunobi-ninja/kache#624).
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let rmeta = dir.path().join("libfoo-1234.rmeta");
        std::fs::write(&rmeta, b"").unwrap();

        let err = store
            .put(
                "truncated_lib_rmeta",
                "foo",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(rmeta, "libfoo-1234.rmeta".into())],
                "",
                "",
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("zero-byte"),
            "expected 'zero-byte' error, got: {err}"
        );
        assert!(!store.contains("truncated_lib_rmeta"));
    }

    #[test]
    fn zero_byte_is_valid_output_only_for_metadata_without_a_library_unit() {
        // `--test` unit (no `--crate-type`), and the `--emit=metadata` crate
        // types rustc leaves empty.
        assert!(zero_byte_is_valid_output("libfoo-1234.rmeta", &[]));
        for ct in ["bin", "cdylib", "staticlib"] {
            assert!(
                zero_byte_is_valid_output("libfoo-1234.rmeta", &[ct.into()]),
                "{ct} emits no metadata, so an empty .rmeta is legitimate"
            );
        }
        // These do emit metadata — empty means truncated.
        for ct in ["lib", "rlib", "dylib", "proc-macro", "some-future-type"] {
            assert!(
                !zero_byte_is_valid_output("libfoo-1234.rmeta", &[ct.into()]),
                "{ct} must keep the truncation guard"
            );
        }
        // Everything else empty means a truncated write, not a real output.
        for name in ["libfoo.rlib", "foo.d", "foo.o", "libfoo.so", "foo"] {
            assert!(
                !zero_byte_is_valid_output(name, &[]),
                "{name} must stay rejected when empty"
            );
        }
    }

    #[test]
    fn test_store_import_rejects_size_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Create a fake downloaded entry with mismatched size in metadata
        let entry_dir = config.store_dir().join("mismatch_key");
        std::fs::create_dir_all(&entry_dir).unwrap();

        let meta = EntryMeta {
            cache_key: "mismatch_key".to_string(),
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            crate_name: "mismatch_crate".to_string(),
            crate_types: vec!["lib".to_string()],
            files: vec![CachedFile {
                name: "lib.rlib".to_string(),
                size: 9999, // Wrong size
                // Valid-shaped hash so validation reaches the size check.
                hash: "a".repeat(64),
                executable: false,
            }],
            stdout: String::new(),
            stderr: String::new(),
            features: vec![],
            target: String::new(),
            profile: "dev".to_string(),
            compile_time_ms: 0,
            emit_kinds: Vec::new(),
        };
        let meta_json = serde_json::to_string_pretty(&meta).unwrap();
        std::fs::write(entry_dir.join("meta.json"), meta_json).unwrap();
        std::fs::write(entry_dir.join("lib.rlib"), b"small content").unwrap();

        let err = store.import_downloaded_entry("mismatch_key").unwrap_err();
        assert!(
            err.to_string().contains("size mismatch"),
            "expected 'size mismatch' error, got: {err}"
        );
    }

    /// Build a downloaded entry dir with one artifact and a `meta.json` whose
    /// `CachedFile` is overridden by `mutate`, then try to import it.
    #[cfg(test)]
    fn import_with_poisoned_meta(
        store: &Store,
        config: &Config,
        key: &str,
        content: &[u8],
        mutate: impl FnOnce(&mut CachedFile),
    ) -> anyhow::Result<()> {
        let entry_dir = config.store_dir().join(key);
        std::fs::create_dir_all(&entry_dir).unwrap();
        std::fs::write(entry_dir.join("lib.rlib"), content).unwrap();
        let mut file = CachedFile {
            name: "lib.rlib".to_string(),
            size: content.len() as u64,
            hash: crate::cache_key::hash_file(&entry_dir.join("lib.rlib")).unwrap(),
            executable: false,
        };
        mutate(&mut file);
        let meta = EntryMeta {
            cache_key: key.to_string(),
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            crate_name: "c".to_string(),
            crate_types: vec!["lib".to_string()],
            files: vec![file],
            stdout: String::new(),
            stderr: String::new(),
            features: vec![],
            target: String::new(),
            profile: "dev".to_string(),
            compile_time_ms: 0,
            emit_kinds: Vec::new(),
        };
        std::fs::write(
            entry_dir.join("meta.json"),
            serde_json::to_string_pretty(&meta).unwrap(),
        )
        .unwrap();
        store.import_downloaded_entry(key)
    }

    /// kunobi-ninja/kache#211-A: a same-size object whose bytes don't match the
    /// claimed hash is rejected — size-only validation is insufficient for
    /// untrusted remote content.
    #[test]
    fn import_rejects_content_hash_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        // Claim the hash of *different* same-length bytes.
        let bogus = blake3::hash(b"DIFFERENT!!!!").to_hex().to_string();
        let err =
            import_with_poisoned_meta(&store, &config, "ch_mismatch", b"real_content!", |f| {
                f.hash = bogus;
            })
            .unwrap_err();
        assert!(
            err.to_string().contains("content hash mismatch"),
            "expected content hash mismatch, got: {err}"
        );
        assert!(!store.contains("ch_mismatch"));
    }

    /// kunobi-ninja/kache#211-C: a hash that isn't a 64-char blake3 hex digest
    /// is rejected before it can reach path construction.
    #[test]
    fn import_rejects_malformed_hash() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let err = import_with_poisoned_meta(&store, &config, "bad_hash", b"data", |f| {
            f.hash = "../../etc/passwd".to_string();
        })
        .unwrap_err();
        assert!(
            err.to_string().contains("malformed blob hash"),
            "expected malformed blob hash, got: {err}"
        );
        assert!(!store.contains("bad_hash"));
    }

    /// kunobi-ninja/kache#211-B: an absolute or `..`-bearing artifact name is
    /// rejected — `Path::join` with it would escape the entry/target dir.
    #[test]
    fn import_rejects_unsafe_artifact_name() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        for bad in ["/etc/passwd", "../escape.rlib", "sub/dir.rlib"] {
            let err = import_with_poisoned_meta(&store, &config, "unsafe_name", b"data", |f| {
                f.name = bad.to_string();
            })
            .unwrap_err();
            assert!(
                err.to_string().contains("unsafe artifact name"),
                "name {bad:?} should be rejected, got: {err}"
            );
        }
        assert!(!store.contains("unsafe_name"));
    }

    #[test]
    fn test_store_keys_for_crates_empty() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let result = store.keys_for_crates(&[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_store_keys_for_crates_with_entries() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        std::fs::write(&output, b"content").unwrap();
        store
            .put(
                "k1",
                "serde",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output.clone(), "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        rewrite_source(&output, b"content2");
        store
            .put(
                "k2",
                "tokio",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        let result = store.keys_for_crates(&["serde".to_string()]).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].crate_name, "serde");

        let result = store
            .keys_for_crates(&["serde".to_string(), "tokio".to_string()])
            .unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_store_keys_for_crates_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let result = store.keys_for_crates(&["nonexistent".to_string()]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn keys_for_crates_errors_on_non_text_cache_key_row() {
        // Covers keys_for_crates row decoding error branch.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        store
            .db
            .execute(
                "INSERT INTO entries (cache_key, crate_name, size, committed) \
                 VALUES (x'80', 'badcrate', 1, 1)",
                [],
            )
            .unwrap();

        let err = store
            .keys_for_crates(&["badcrate".to_string()])
            .unwrap_err();

        assert!(
            err.to_string().contains("Invalid column type"),
            "expected SQLite type error, got: {err}"
        );
    }

    #[test]
    fn test_store_put_records_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        std::fs::write(&output, b"my rlib content").unwrap();
        store
            .put(
                "meta_key",
                "mycrate",
                &["lib".into(), "rlib".into()],
                &["std".into(), "derive".into()],
                "x86_64-unknown-linux-gnu",
                "release",
                &[(output, "lib.rlib".into())],
                "stdout text",
                "stderr text",
            )
            .unwrap();

        let meta = store.get("meta_key").unwrap().unwrap();
        assert_eq!(meta.crate_name, "mycrate");
        assert_eq!(meta.crate_types, vec!["lib", "rlib"]);
        assert_eq!(meta.features, vec!["std", "derive"]);
        assert_eq!(meta.target, "x86_64-unknown-linux-gnu");
        assert_eq!(meta.profile, "release");
        assert_eq!(meta.stdout, "stdout text");
        assert_eq!(meta.stderr, "stderr text");
        assert_eq!(meta.files.len(), 1);
        assert!(!meta.files[0].hash.is_empty());
    }

    #[test]
    fn test_store_wait_for_committed_returns_false_when_not_committed() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // No entry committed — should return false immediately (no lock file)
        let result = store.wait_for_committed("nope").unwrap();
        assert!(!result);
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn test_exclude_from_indexing_creates_sentinel() {
        let dir = tempfile::tempdir().unwrap();
        if let Some(handle) = exclude_from_indexing(dir.path()) {
            let _ = handle.join();
        }
        let sentinel = dir.path().join(".metadata_never_index");
        assert!(sentinel.exists());
        assert!(
            sentinel.metadata().unwrap().len() == 0,
            "sentinel should be empty"
        );
        // Idempotent — second call doesn't fail or modify
        if let Some(handle) = exclude_from_indexing(dir.path()) {
            let _ = handle.join();
        }
        assert!(sentinel.exists());
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn test_exclude_from_indexing_sets_tmutil_xattr() {
        let dir = tempfile::tempdir().unwrap();
        // The tmutil child now runs on a detached thread (#588); join the
        // returned handle so the assertion isn't racing it.
        if let Some(handle) = exclude_from_indexing(dir.path()) {
            let _ = handle.join();
        }
        let output = std::process::Command::new("tmutil")
            .args(["isexcluded", &dir.path().display().to_string()])
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains("[Excluded]"),
            "expected [Excluded] in tmutil output, got: {stdout}"
        );

        // Second call must take the xattr fast path: no tmutil spawn at all.
        assert!(
            exclude_from_indexing(dir.path()).is_none(),
            "already-excluded dir must skip the tmutil subprocess"
        );
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn test_exclude_from_indexing_skips_existing_sentinel() {
        let dir = tempfile::tempdir().unwrap();
        let sentinel = dir.path().join(".metadata_never_index");
        // Pre-create sentinel with known content
        fs::write(&sentinel, b"existing").unwrap();
        if let Some(handle) = exclude_from_indexing(dir.path()) {
            let _ = handle.join();
        }
        // Should not overwrite — guard checks exists()
        assert_eq!(fs::read(&sentinel).unwrap(), b"existing");
    }

    #[test]
    fn test_blob_path_sharding() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let hash = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        let path = store.blob_path(hash);
        // Normalise separators: the path is built with `PathBuf::join`, so the
        // shard dirs are `blobs\ab\…` on Windows.
        assert!(
            path.to_string_lossy()
                .replace('\\', "/")
                .contains("blobs/ab/")
        );
        assert!(path.to_string_lossy().ends_with(hash));
    }

    #[test]
    fn test_blobs_table_created() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Table should exist — query it
        let count: i64 = store
            .db
            .query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn test_exclude_from_indexing_nonexistent_dir_silent() {
        let dir = PathBuf::from("/tmp/kache_test_nonexistent_874291");
        assert!(!dir.exists());
        // Should not panic — both operations fail silently
        if let Some(handle) = exclude_from_indexing(&dir) {
            let _ = handle.join();
        }
    }

    #[test]
    fn test_put_creates_blob() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        fs::write(&output, b"rlib content").unwrap();
        store
            .put(
                "k1",
                "mycrate",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // Blob should exist
        let meta_path = store.entry_dir("k1").join("meta.json");
        let content = fs::read_to_string(&meta_path).unwrap();
        let meta: EntryMeta = serde_json::from_str(&content).unwrap();
        let blob = store.blob_path(&meta.files[0].hash);
        assert!(
            blob.exists(),
            "blob file should exist at {}",
            blob.display()
        );

        // Entry dir should only have meta.json (no artifact files)
        let entry_dir = store.entry_dir("k1");
        let mut files: Vec<_> = fs::read_dir(&entry_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        files.sort();
        assert_eq!(
            files,
            vec!["meta.json"],
            "entry dir should only contain meta.json"
        );
    }

    #[test]
    fn test_put_deduplicates_identical_content() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        rewrite_source(&output, b"same content");
        store
            .put(
                "k1",
                "crate_a",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output.clone(), "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // Put again with same content but different cache key
        rewrite_source(&output, b"same content");
        store
            .put(
                "k2",
                "crate_a",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // Both entries should reference the same blob hash
        let m1: EntryMeta = serde_json::from_str(
            &fs::read_to_string(store.entry_dir("k1").join("meta.json")).unwrap(),
        )
        .unwrap();
        let m2: EntryMeta = serde_json::from_str(
            &fs::read_to_string(store.entry_dir("k2").join("meta.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(m1.files[0].hash, m2.files[0].hash);

        // Refcount should be 2
        let refcount: i64 = store
            .db
            .query_row(
                "SELECT refcount FROM blobs WHERE hash = ?1",
                params![m1.files[0].hash],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(refcount, 2);
    }

    #[test]
    fn test_get_verifies_blobs_not_entry_files() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        fs::write(&output, b"content").unwrap();
        store
            .put(
                "k1",
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // Entry dir should NOT have lib.rlib — only meta.json
        assert!(!store.entry_dir("k1").join("lib.rlib").exists());

        // get() should still succeed (resolving via blob store)
        let meta = store.get("k1").unwrap();
        assert!(meta.is_some());
    }

    #[test]
    fn test_get_evicts_when_blob_missing() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        fs::write(&output, b"content").unwrap();
        store
            .put(
                "k1",
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // Read meta to get the hash
        let meta_content = fs::read_to_string(store.entry_dir("k1").join("meta.json")).unwrap();
        let meta: EntryMeta = serde_json::from_str(&meta_content).unwrap();
        let blob = store.blob_path(&meta.files[0].hash);

        // Delete the blob to simulate corruption
        let mut perms = fs::metadata(&blob).unwrap().permissions();
        perms.set_readonly(false);
        fs::set_permissions(&blob, perms).unwrap();
        fs::remove_file(&blob).unwrap();

        // get() should detect missing blob and evict
        let result = store.get("k1").unwrap();
        assert!(result.is_none());
        assert!(!store.contains("k1"));
    }

    #[test]
    fn test_put_blob_is_readonly() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        fs::write(&output, b"content").unwrap();
        store
            .put(
                "k1",
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        let meta: EntryMeta = serde_json::from_str(
            &fs::read_to_string(store.entry_dir("k1").join("meta.json")).unwrap(),
        )
        .unwrap();
        let blob = store.blob_path(&meta.files[0].hash);
        let perms = fs::metadata(&blob).unwrap().permissions();
        assert!(perms.readonly(), "blob should be read-only");
    }

    #[test]
    fn test_remove_entry_decrements_refcount() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        rewrite_source(&output, b"shared content");
        store
            .put(
                "k1",
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output.clone(), "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        rewrite_source(&output, b"shared content");
        store
            .put(
                "k2",
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // Get the hash from meta.json
        let meta_content = fs::read_to_string(store.entry_dir("k1").join("meta.json")).unwrap();
        let meta: EntryMeta = serde_json::from_str(&meta_content).unwrap();
        let hash = meta.files[0].hash.clone();
        let blob = store.blob_path(&hash);

        // Remove first entry — blob should still exist (refcount 1)
        store.remove_entry("k1").unwrap();
        assert!(blob.exists(), "blob should survive when refcount > 0");

        let refcount: i64 = store
            .db
            .query_row(
                "SELECT refcount FROM blobs WHERE hash = ?1",
                params![&hash],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(refcount, 1);

        // Remove second entry — blob should be deleted (refcount 0)
        store.remove_entry("k2").unwrap();
        assert!(!blob.exists(), "blob should be deleted when refcount = 0");

        let count: i64 = store
            .db
            .query_row(
                "SELECT COUNT(*) FROM blobs WHERE hash = ?1",
                params![&hash],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    /// Many independent clients (separate connections, like real wrapper
    /// processes) concurrently cache distinct entries that share identical
    /// content. Because registration is transactional, the shared blob's
    /// refcount must equal the number of entries — no drift, no lost or
    /// duplicated blob — and the per-writer temp names must leave no debris.
    #[test]
    fn test_concurrent_puts_sharing_blob_are_consistent() {
        const N: usize = 8;
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        // Initialise the schema once before the racing opens.
        Store::open(&config).unwrap();

        let content = b"identical artifact content shared across all entries";

        let mut handles = Vec::new();
        for i in 0..N {
            let config = test_config(dir.path());
            let src = dir.path().join(format!("art-{i}.rlib"));
            std::fs::write(&src, content).unwrap();
            handles.push(std::thread::spawn(move || {
                let store = Store::open(&config).unwrap();
                store
                    .put(
                        &format!("key{i}"),
                        "shared",
                        &["lib".into()],
                        &[],
                        "x86_64-unknown-linux-gnu",
                        "dev",
                        &[(src, "libshared.rlib".into())],
                        "",
                        "",
                    )
                    .unwrap();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let store = Store::open(&config).unwrap();
        let hash = store.get("key0").unwrap().unwrap().files[0].hash.clone();

        // Exactly one blob, referenced by every entry.
        assert_eq!(store.blob_stats().unwrap().total_blobs, 1);
        let refcount: i64 = store
            .db
            .query_row(
                "SELECT refcount FROM blobs WHERE hash = ?1",
                params![&hash],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(refcount as usize, N, "refcount must equal the entry count");
        assert!(store.blob_path(&hash).is_file());
        for i in 0..N {
            assert!(
                store.contains(&format!("key{i}")),
                "entry key{i} must be committed"
            );
        }

        // Unique temp names must leave no debris in the shard directory.
        let shard = store.blob_path(&hash).parent().unwrap().to_path_buf();
        let tmp_left = std::fs::read_dir(&shard)
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
            .count();
        assert_eq!(tmp_left, 0, "no leftover .tmp files");

        // Removing all but the last keeps the blob; removing the last reclaims it.
        for i in 0..N - 1 {
            store.remove_entry(&format!("key{i}")).unwrap();
        }
        assert!(
            store.blob_path(&hash).is_file(),
            "blob persists while still referenced"
        );
        store.remove_entry(&format!("key{}", N - 1)).unwrap();
        assert!(
            !store.blob_path(&hash).is_file(),
            "blob reclaimed once the last reference is gone"
        );
        assert_eq!(store.blob_stats().unwrap().total_blobs, 0);
    }

    /// Hammers a single shared blob with concurrent puts and removes from
    /// independent connections. Because blob-file mutations and refcount
    /// mutations both happen under the SQLite write lock, a `put` can never
    /// commit an entry whose blob a concurrent `remove` has unlinked: every
    /// just-put entry must be restorable with its blob present. Once all churn
    /// settles, the blob is fully reclaimed.
    #[test]
    fn test_concurrent_put_remove_never_dangles() {
        const THREADS: usize = 8;
        const ROUNDS: usize = 30;
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        Store::open(&config).unwrap(); // initialise schema before racing opens

        let content = b"hot shared blob churned by concurrent puts and removes";

        // This test proves publication/removal atomicity, not the production
        // five-second fail-fast policy. A two-core hosted Windows runner can
        // keep eight WAL writers queued past that timeout, so give these test
        // connections enough time for every logical operation to commit and
        // reach the invariant checks below.
        let stores: Vec<_> = (0..THREADS)
            .map(|_| {
                let store = Store::open(&config).unwrap();
                store.db.busy_timeout(Duration::from_secs(30)).unwrap();
                store
            })
            .collect();
        let start = std::sync::Arc::new(std::sync::Barrier::new(THREADS));

        let mut handles = Vec::new();
        for (t, store) in stores.into_iter().enumerate() {
            let dir_path = dir.path().to_path_buf();
            let start = std::sync::Arc::clone(&start);
            handles.push(std::thread::spawn(move || {
                start.wait();
                for r in 0..ROUNDS {
                    let key = format!("t{t}r{r}");
                    let src = dir_path.join(format!("src-{t}-{r}.rlib"));
                    std::fs::write(&src, content).unwrap();
                    store
                        .put(
                            &key,
                            "shared",
                            &["lib".into()],
                            &[],
                            "tgt",
                            "dev",
                            &[(src, "lib.rlib".into())],
                            "",
                            "",
                        )
                        .unwrap();

                    // Our reference is committed: the entry must be restorable
                    // and its blob present — never dangling from a concurrent
                    // remove of another entry sharing the same blob.
                    let meta = store
                        .get(&key)
                        .unwrap()
                        .unwrap_or_else(|| panic!("entry {key} vanished right after put"));
                    assert!(
                        store.blob_path(&meta.files[0].hash).is_file(),
                        "blob missing while {key} still references it"
                    );

                    store.remove_entry(&key).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // All entries removed → the shared blob is fully reclaimed.
        let store = Store::open(&config).unwrap();
        assert_eq!(store.blob_stats().unwrap().total_blobs, 0);
    }

    /// kunobi-ninja/kache#670: a remover that deleted no row must not touch
    /// the entry directory — a fresh meta.json there may belong to a
    /// publisher whose registration transaction has not committed yet, and
    /// deleting it strands the publication.
    #[test]
    fn losing_remover_leaves_a_publishers_directory_alone() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let src = dir.path().join("x.rlib");
        std::fs::write(&src, b"mid-publication content").unwrap();
        store
            .put(
                "pub",
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(src, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        // Simulate the publisher's window: meta.json is on disk, the entry
        // row is not committed yet.
        store
            .db
            .execute("DELETE FROM entries WHERE cache_key = 'pub'", [])
            .unwrap();
        store
            .db
            .execute("DELETE FROM entry_blobs WHERE cache_key = 'pub'", [])
            .unwrap();

        store.remove_entry("pub").unwrap();
        assert!(
            store.entry_dir("pub").join("meta.json").exists(),
            "the loser deleted no row and must leave the publisher's meta.json alone"
        );
    }

    /// kunobi-ninja/kache#670: the row a removal deletes may belong to a
    /// NEWER publication than the meta.json it read its hash list from
    /// (`put` writes meta.json before its registration transaction).
    /// Decrementing the old hashes against the new row corrupts refcounts;
    /// the in-transaction meta re-read must detect the republication and
    /// roll the removal back untouched.
    #[test]
    fn removal_rolls_back_when_the_entry_was_republished_mid_flight() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let src_a = dir.path().join("a.rlib");
        std::fs::write(&src_a, b"generation A").unwrap();
        store
            .put(
                "aba",
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(src_a, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        let outcome = store
            .remove_entry_guarded_with_hook("aba", None, || {
                // Republish the same key with different content between the
                // removal's meta read and its transaction.
                let src_b = dir.path().join("b.rlib");
                std::fs::write(&src_b, b"generation B, longer content").unwrap();
                store
                    .put(
                        "aba",
                        "c",
                        &["lib".into()],
                        &[],
                        "",
                        "dev",
                        &[(src_b, "lib.rlib".into())],
                        "",
                        "",
                    )
                    .unwrap();
            })
            .unwrap();

        assert!(
            outcome.is_none(),
            "a removal that lost to a republication must report nothing removed"
        );
        assert!(
            store.contains("aba"),
            "generation B's row must survive the rolled-back removal"
        );
        let meta = store.get("aba").unwrap().expect("B must stay restorable");
        let hash = &meta.files[0].hash;
        assert!(
            store.blob_path(hash).is_file(),
            "generation B's blob must remain on disk"
        );
        let refcount: i64 = store
            .db
            .query_row(
                "SELECT refcount FROM blobs WHERE hash = ?1",
                params![hash],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            refcount, 1,
            "B's refcounts must be untouched by the rollback"
        );
    }

    /// kunobi-ninja/kache#670, residual window: a same-key publisher whose
    /// fresh `meta.json` lands while a removal is between its refcount
    /// decrements and its directory cleanup must not have that meta deleted
    /// out from under its registration — that strands the publisher's
    /// committed row with no artifacts and leaks its refcounts until doctor
    /// or an index rebuild.
    ///
    /// Cleanup now runs in its own locked transaction after the logical
    /// removal commits, guarded by a republication check, and `put`
    /// materializes meta.json inside its registration transaction — so the
    /// publisher is serialized to entirely-before the cleanup (the check then
    /// sees its row and leaves the directory alone) or entirely-after (it
    /// re-creates the directory). The publisher below is released exactly in
    /// the old danger window; on the old structure (unlocked, unchecked
    /// cleanup) it finishes inside the window and its meta.json is destroyed.
    #[test]
    fn republication_during_removal_cleanup_is_never_stranded() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let src_a = dir.path().join("a.rlib");
        std::fs::write(&src_a, b"generation A").unwrap();
        store
            .put(
                "key",
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(src_a, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // The publisher runs on its own connection and is released inside
        // the removal's cleanup window. With cleanup inside the transaction
        // it blocks on the write lock, the seam's bounded wait expires, and
        // the removal finishes first; the publisher then lands cleanly after.
        // With the old post-commit cleanup the lock is already free, the put
        // completes inside the window, and the cleanup destroys its meta.
        //
        // Synchronization is deadline-bounded atomics, not channels: every
        // wait has a hard cap, so a broken removal path that never reaches
        // the seam degrades into assertion failures instead of a hang — which
        // is what lets the mutation lane kill mutants of the removal instead
        // of timing out on them.
        let released = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        // Proves the interleaving actually happened: without it the test can
        // pass vacuously when the publisher thread is scheduled so late that
        // its put simply runs after the whole removal.
        let attempting = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let published = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let wait_for = |flag: &std::sync::atomic::AtomicBool, cap: Duration| {
            let deadline = std::time::Instant::now() + cap;
            while !flag.load(std::sync::atomic::Ordering::Acquire)
                && std::time::Instant::now() < deadline
            {
                std::thread::sleep(Duration::from_millis(5));
            }
        };
        let publisher = {
            let config = config.clone();
            let dir_path = dir.path().to_path_buf();
            let released = std::sync::Arc::clone(&released);
            let attempting = std::sync::Arc::clone(&attempting);
            let published = std::sync::Arc::clone(&published);
            std::thread::spawn(move || {
                let store = Store::open(&config).unwrap();
                store.db.busy_timeout(Duration::from_secs(30)).unwrap();
                let src_b = dir_path.join("b.rlib");
                std::fs::write(&src_b, b"generation B, republished").unwrap();
                // Bounded: if the removal never reaches the seam (a broken
                // removal path), publish anyway so the join terminates and
                // the assertions report the breakage.
                let deadline = std::time::Instant::now() + Duration::from_secs(10);
                while !released.load(std::sync::atomic::Ordering::Acquire)
                    && std::time::Instant::now() < deadline
                {
                    std::thread::sleep(Duration::from_millis(5));
                }
                attempting.store(true, std::sync::atomic::Ordering::Release);
                store
                    .put(
                        "key",
                        "c",
                        &["lib".into()],
                        &[],
                        "",
                        "dev",
                        &[(src_b, "lib.rlib".into())],
                        "",
                        "",
                    )
                    .unwrap();
                published.store(true, std::sync::atomic::Ordering::Release);
            })
        };

        let removed = store
            .remove_entry_guarded_with_hooks(
                "key",
                None,
                || {},
                || {
                    released.store(true, std::sync::atomic::Ordering::Release);
                    // The publisher must have reached its put before cleanup
                    // continues, or the "race" never happened and the test
                    // proves nothing.
                    wait_for(&attempting, Duration::from_secs(5));
                    assert!(
                        attempting.load(std::sync::atomic::Ordering::Acquire),
                        "publisher never reached put; the interleaving was not exercised"
                    );
                    // Give the publisher a real chance to race: on the fixed
                    // structure it blocks on the write lock and this expires;
                    // on the old structure it completes inside the window.
                    wait_for(&published, Duration::from_millis(1500));
                },
            )
            .unwrap();
        publisher.join().unwrap();

        assert!(removed.is_some(), "the removal owned generation A's row");
        assert!(
            store.contains("key"),
            "generation B's row must be committed"
        );
        assert!(
            store.entry_dir("key").join("meta.json").is_file(),
            "generation B's meta.json must survive the racing removal's cleanup"
        );
        let meta = store.get("key").unwrap().expect("B must be restorable");
        let hash = meta.files[0].hash.clone();
        assert!(
            store.blob_path(&hash).is_file(),
            "generation B's blob must be on disk"
        );
        let refcount: i64 = store
            .db
            .query_row(
                "SELECT refcount FROM blobs WHERE hash = ?1",
                params![hash],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(refcount, 1, "B's refcounts must be intact");
    }

    /// kunobi-ninja/kache#510: directory-cleanup tolerance is for the
    /// lost-the-race case ONLY — a cleanup failure while the directory still
    /// exists (permissions, open handles) must surface as an error, not be
    /// swallowed as if the competitor had won.
    #[cfg(unix)]
    #[test]
    fn persistent_directory_cleanup_failure_is_an_error() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let src = dir.path().join("x.rlib");
        std::fs::write(&src, b"content").unwrap();
        store
            .put(
                "stuck",
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(src, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // An unwritable directory with a file inside makes remove_dir_all
        // fail with a persistent, non-race error. Nested one level down:
        // cleanup's readonly-clearing pass covers entry_dir's immediate
        // children, so a top-level readonly dir would simply be repaired.
        let entry_dir = store.entry_dir("stuck");
        let inner = entry_dir.join("legacy").join("inner");
        std::fs::create_dir_all(&inner).unwrap();
        std::fs::write(inner.join("artifact"), b"x").unwrap();
        std::fs::set_permissions(&inner, std::fs::Permissions::from_mode(0o555)).unwrap();

        // Stash the blob hash BEFORE the removal: remove_dir_all deletes
        // children in unspecified order, so meta.json may or may not survive
        // the failed cleanup.
        let stashed_hash = {
            let meta: EntryMeta = serde_json::from_str(
                &std::fs::read_to_string(entry_dir.join("meta.json")).unwrap(),
            )
            .unwrap();
            meta.files[0].hash.clone()
        };

        let err = store.remove_entry("stuck");
        // Restore permissions so the tempdir can be dropped regardless.
        std::fs::set_permissions(&inner, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(
            err.is_err(),
            "a persistent cleanup failure must not be swallowed as a lost race"
        );
        // The failure direction matters (#670): the logical removal commits
        // BEFORE cleanup, so a cleanup failure leaves a deleted row plus
        // partially deleted, unindexed residue — recoverable. Rolling the row
        // back after files were already deleted would manufacture a committed
        // row without artifacts, which is the phantom this function must
        // never produce.
        assert!(
            !store.contains("stuck"),
            "the logical removal must stay committed across a cleanup failure"
        );
        assert!(
            store.blob_path(&stashed_hash).is_file(),
            "blob unlinks run only after directory cleanup succeeds; a failed \
             cleanup leaves the file for the orphan sweep"
        );
    }

    /// kunobi-ninja/kache#510: two removers racing on the SAME entry must not
    /// double-decrement a shared blob's refcount. The victim entry shares its
    /// blob with a survivor; a double decrement would take the refcount 2 → 0
    /// and unlink a blob the survivor still references. Exactly one remover
    /// may report `true`, the loser must report `false` without erroring
    /// (directory cleanup is idempotent), and the survivor stays restorable.
    /// Deliberately holds no `gc.lock`: the function must be safe on its own,
    /// not by caller convention.
    #[test]
    fn two_removers_on_one_entry_never_double_decrement_shared_blob() {
        const ROUNDS: usize = 25;
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        Store::open(&config).unwrap(); // initialise schema before racing opens

        for round in 0..ROUNDS {
            let content = format!("shared blob for round {round}");
            let victim = format!("victim-{round}");
            let survivor = format!("survivor-{round}");

            let store = Store::open(&config).unwrap();
            for key in [&victim, &survivor] {
                let src = dir.path().join(format!("{key}.rlib"));
                std::fs::write(&src, content.as_bytes()).unwrap();
                store
                    .put(
                        key,
                        "c",
                        &["lib".into()],
                        &[],
                        "",
                        "dev",
                        &[(src, "lib.rlib".into())],
                        "",
                        "",
                    )
                    .unwrap();
            }
            let hash = store.get(&survivor).unwrap().unwrap().files[0].hash.clone();

            let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
            let mut handles = Vec::new();
            for _ in 0..2 {
                let config = test_config(dir.path());
                let victim = victim.clone();
                let barrier = barrier.clone();
                handles.push(std::thread::spawn(move || {
                    let store = Store::open(&config).unwrap();
                    barrier.wait();
                    store.remove_entry_guarded(&victim, None)
                }));
            }
            let removed: Vec<bool> = handles
                .into_iter()
                .map(|h| {
                    h.join()
                        .unwrap()
                        .expect("losing remover must not error")
                        .is_some()
                })
                .collect();
            assert_eq!(
                removed.iter().filter(|&&won| won).count(),
                1,
                "exactly one remover releases the entry (round {round}): {removed:?}"
            );

            let refcount: i64 = store
                .db
                .query_row(
                    "SELECT refcount FROM blobs WHERE hash = ?1",
                    params![&hash],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(
                refcount, 1,
                "survivor's shared blob refcount (round {round})"
            );
            assert!(
                store.blob_path(&hash).is_file(),
                "shared blob unlinked out from under the survivor (round {round})"
            );
            assert!(
                store.get(&survivor).unwrap().is_some(),
                "survivor entry must stay restorable (round {round})"
            );
            store.remove_entry(&survivor).unwrap();
        }
    }

    /// kunobi-ninja/kache#608 (over-eviction): on a dedup-heavy store the
    /// logical `SUM(entries.size)` sits far above the physical bytes on disk.
    /// With `max_size` between the two, eviction must NOT fire — the disk is
    /// comfortable. The pre-#608 trigger compared the logical figure and
    /// destroyed rebuild value without reclaiming meaningful space.
    #[test]
    fn evict_does_not_fire_while_physical_size_is_within_budget() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        // Physical: one 200-byte shared blob. Logical: 400 bytes.
        config.max_size = 300;
        let store = Store::open(&config).unwrap();

        for key in ["dup_a", "dup_b"] {
            let src = dir.path().join(format!("{key}.rlib"));
            std::fs::write(&src, vec![b'x'; 200]).unwrap();
            store
                .put(
                    key,
                    "c",
                    &["lib".into()],
                    &[],
                    "",
                    "dev",
                    &[(src, "lib.rlib".into())],
                    "",
                    "",
                )
                .unwrap();
        }
        assert_eq!(store.total_size().unwrap(), 400, "logical double-counts");
        assert_eq!(store.physical_size().unwrap(), 200, "disk holds one copy");

        store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour')",
                [],
            )
            .unwrap();
        let stats = store.evict().unwrap();
        assert_eq!(
            stats.entries_evicted, 0,
            "physical 200 <= max 300 (the trigger): nothing to evict"
        );
        assert!(store.contains("dup_a") && store.contains("dup_b"));
    }

    /// kunobi-ninja/kache#608 (ranking + stop condition): entries whose blobs
    /// are all shared free nothing; the sweep must prefer an entry with a
    /// unique blob and stop once the bytes *actually* freed satisfy the
    /// physical target — not evict the whole shared family because a logical
    /// counter said so.
    #[test]
    fn evict_prefers_and_stops_on_actually_freed_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 500 * 1024; // target 450 KiB; physical 600 KiB
        let store = Store::open(&config).unwrap();

        // Three entries share one 300-byte blob (logical 900, physical 300)…
        for key in ["shared_a", "shared_b", "shared_c"] {
            let src = dir.path().join(format!("{key}.rlib"));
            std::fs::write(&src, vec![b's'; 300 * 1024]).unwrap();
            store
                .put(
                    key,
                    "c",
                    &["lib".into()],
                    &[],
                    "",
                    "dev",
                    &[(src, "lib.rlib".into())],
                    "",
                    "",
                )
                .unwrap();
        }
        // …plus one entry with its own 300-byte blob.
        let src = dir.path().join("unique.rlib");
        std::fs::write(&src, vec![b'u'; 300 * 1024]).unwrap();
        store
            .put(
                "unique",
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(src, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        assert_eq!(store.physical_size().unwrap(), 600 * 1024);
        store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour')",
                [],
            )
            .unwrap();

        let stats = store.evict().unwrap();
        assert_eq!(
            stats.entries_evicted, 1,
            "evicting `unique` frees 300 KiB physical → 300 <= 450 KiB, done"
        );
        assert_eq!(stats.bytes_freed, 300 * 1024);
        assert!(
            !store.contains("unique"),
            "the freeing entry is the one evicted"
        );
        for key in ["shared_a", "shared_b", "shared_c"] {
            assert!(store.contains(key), "{key} frees nothing and must survive");
        }
    }

    /// kunobi-ninja/kache#710: `evict()` must have a real hysteresis band —
    /// fire at the full cap (`max_size`, 100%) and stop at 90% of it. This
    /// store sits between the two edges (950 of max 1000, target 900), where a
    /// sweep that incorrectly triggers at 90% would evict.
    #[test]
    fn evict_noop_within_the_hysteresis_band() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 1000; // target 900; trigger 1000; physical 950
        let store = Store::open(&config).unwrap();

        for i in 0..5 {
            let src = dir.path().join(format!("u{i}.rlib"));
            // Exactly 190 bytes, unique per entry.
            std::fs::write(&src, format!("{i}{}", "x".repeat(189)).as_bytes()).unwrap();
            store
                .put(
                    &format!("u{i}"),
                    "c",
                    &["lib".into()],
                    &[],
                    "",
                    "dev",
                    &[(src, "lib.rlib".into())],
                    "",
                    "",
                )
                .unwrap();
        }
        assert_eq!(store.physical_size().unwrap(), 950);
        store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour')",
                [],
            )
            .unwrap();

        let stats = store.evict().unwrap();
        assert_eq!(
            stats.entries_evicted, 0,
            "950 is inside the band (900 < 950 <= 1000): evict() must not fire"
        );
        assert_eq!(store.physical_size().unwrap(), 950);
    }

    /// Once the store crosses the #710 trigger, eviction stops at the 90%
    /// target rather than at the trigger or after the whole candidate set.
    #[test]
    fn evict_fires_at_the_trigger_and_stops_at_the_target() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 1000; // target 900; trigger 1000
        let store = Store::open(&config).unwrap();

        for i in 0..6 {
            let src = dir.path().join(format!("u{i}.rlib"));
            // Exactly 190 bytes, unique per entry: 6 * 190 = 1140 > 1000.
            std::fs::write(&src, format!("{i}{}", "x".repeat(189)).as_bytes()).unwrap();
            store
                .put(
                    &format!("u{i}"),
                    "c",
                    &["lib".into()],
                    &[],
                    "",
                    "dev",
                    &[(src, "lib.rlib".into())],
                    "",
                    "",
                )
                .unwrap();
        }
        assert_eq!(store.physical_size().unwrap(), 1140);
        store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour')",
                [],
            )
            .unwrap();

        let stats = store.evict().unwrap();
        assert_eq!(
            stats.entries_evicted, 2,
            "1140 > 1000 must trigger, and two 190-byte evictions reach 760 <= 900"
        );
        assert_eq!(store.physical_size().unwrap(), 760);
    }

    /// kunobi-ninja/kache#594: a size-driven sweep records every tombstone
    /// with the value-density shadow's verdict on the same entry, and the
    /// demand stream splits by that verdict. The store here is built so the
    /// two policies disagree: the live policy evicts the LARGEST stale entry
    /// (huge but expensive to rebuild), while the shadow — ranking by
    /// rebuild cost per reclaimable byte — would have kept it and evicted
    /// the small cheap one instead.
    #[test]
    fn size_sweep_records_shadow_verdicts_and_demand_splits() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 450_000; // target 405_000; physical 500_000
        let store = Store::open(&config).unwrap();

        for (key, bytes, fill, compile_ms) in [
            ("huge_expensive", 400_000usize, b'a', 60_000u64),
            ("small_cheap", 100_000usize, b'b', 1u64),
        ] {
            let src = dir.path().join(format!("{key}.rlib"));
            std::fs::write(&src, vec![fill; bytes]).unwrap();
            store
                .put_with_compile_time(
                    key,
                    "c",
                    &["lib".into()],
                    &[],
                    "",
                    "dev",
                    &[(src, "lib.rlib".into())],
                    "",
                    "",
                    compile_ms,
                )
                .unwrap();
        }
        store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour')",
                [],
            )
            .unwrap();

        let stats = store.evict().unwrap();
        assert_eq!(
            stats.entries_evicted, 1,
            "the live policy evicts the largest entry and reaches its target"
        );
        assert!(!store.contains("huge_expensive"));
        assert!(store.contains("small_cheap"));

        // The tombstone carries the shadow's dissent: for the same 95 KB
        // budget the value-density ranking would have taken small_cheap
        // (density ~10 ms/MB) and kept huge_expensive (~150,000 ms/MB).
        let (shadow_policy, shadow_would_evict): (String, i64) = store
            .db
            .query_row(
                "SELECT shadow_policy, shadow_would_evict FROM eviction_tombstones
                 WHERE cache_key = 'huge_expensive'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(shadow_policy, "value-density");
        assert_eq!(shadow_would_evict, 0, "the shadow would have kept it");

        // Demand on the evicted key lands in the shadow-kept cohort — the
        // shadow's save, had it been live.
        assert!(store.get("huge_expensive").unwrap().is_none());
        assert_eq!(
            store.shadow_demand_split().unwrap(),
            ShadowDemandSplit {
                agreed: 0,
                agreed_demanded: 0,
                shadow_kept: 1,
                shadow_kept_demanded: 1,
            }
        );

        // Pin the split query's cohort handling: an agreed row counts, a
        // pre-shadow row (NULL verdict) enters neither cohort, and an
        // unknown-cost row is recorded but excluded from the headline —
        // the density shadow ranks unknown cost as worthless by
        // construction, so counting it would bias the comparison.
        store
            .db
            .execute_batch(
                "INSERT INTO eviction_tombstones
                    (cache_key, policy, compile_time_ms, shadow_policy, shadow_would_evict, demanded_at)
                 VALUES ('agreed_row', 'size-pressure', 500, 'value-density', 1, datetime('now'));
                 INSERT INTO eviction_tombstones (cache_key, policy, compile_time_ms)
                 VALUES ('pre_shadow_row', 'size-pressure', 500);
                 INSERT INTO eviction_tombstones
                    (cache_key, policy, compile_time_ms, shadow_policy, shadow_would_evict)
                 VALUES ('unknown_cost_row', 'size-pressure', 0, 'value-density', 0);",
            )
            .unwrap();
        assert_eq!(
            store.shadow_demand_split().unwrap(),
            ShadowDemandSplit {
                agreed: 1,
                agreed_demanded: 1,
                shadow_kept: 1,
                shadow_kept_demanded: 1,
            }
        );
    }

    /// kunobi-ninja/kache#608 (honest accounting): a sweep over a fully-shared
    /// family reports the physical bytes it freed (once, when the last
    /// reference goes), not the logical sum of the evicted entries.
    #[test]
    fn evict_reports_physical_bytes_freed_not_logical() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 200; // target 180; physical 300 → must evict all three
        let store = Store::open(&config).unwrap();

        for key in ["a", "b", "c"] {
            let src = dir.path().join(format!("{key}.rlib"));
            std::fs::write(&src, vec![b'z'; 300]).unwrap();
            store
                .put(
                    key,
                    "c",
                    &["lib".into()],
                    &[],
                    "",
                    "dev",
                    &[(src, "lib.rlib".into())],
                    "",
                    "",
                )
                .unwrap();
        }
        store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour')",
                [],
            )
            .unwrap();

        let stats = store.evict().unwrap();
        assert_eq!(
            stats.entries_evicted, 3,
            "zero-freeing removals must not stop the sweep early"
        );
        assert_eq!(stats.bytes_freed, 300, "the blob's bytes are freed once");
        assert_eq!(stats.blobs_removed, 1);
        assert_eq!(store.physical_size().unwrap(), 0);
    }

    /// kunobi-ninja/kache#608: pre-#608 stores have no `entry_blobs` rows;
    /// the GC-sweep backfill reconstructs them from meta.json, bounded and
    /// convergent, and candidates go from unknown (rank on logical size) to
    /// exact marginal-reclaimable bytes.
    #[test]
    fn backfill_entry_blobs_reconstructs_marginal_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        for (key, content) in [("shared_a", b'x'), ("shared_b", b'x'), ("solo", b'y')] {
            let src = dir.path().join(format!("{key}.rlib"));
            std::fs::write(&src, vec![content; 100]).unwrap();
            store
                .put(
                    key,
                    "c",
                    &["lib".into()],
                    &[],
                    "",
                    "dev",
                    &[(src, "lib.rlib".into())],
                    "",
                    "",
                )
                .unwrap();
        }
        // Simulate a store written before the table existed.
        store.db.execute("DELETE FROM entry_blobs", []).unwrap();

        let unknowns = store.eviction_candidates().unwrap();
        assert!(
            unknowns.iter().all(|f| f.reclaimable_bytes.is_none()),
            "un-backfilled entries must report unknown, not zero"
        );

        assert_eq!(store.backfill_entry_blobs().unwrap(), 3);
        assert_eq!(store.backfill_entry_blobs().unwrap(), 0, "converges");

        let features = store.eviction_candidates().unwrap();
        let by_key: std::collections::HashMap<&str, &crate::eviction::EntryFeatures> =
            features.iter().map(|f| (f.key.as_str(), f)).collect();
        assert_eq!(by_key["shared_a"].reclaimable_bytes, Some(0));
        assert_eq!(by_key["shared_b"].reclaimable_bytes, Some(0));
        assert_eq!(by_key["solo"].reclaimable_bytes, Some(100));
    }

    #[test]
    fn test_clear_removes_blobs_too() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        fs::write(&output, b"content").unwrap();
        store
            .put(
                "k1",
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        store.clear().unwrap();

        // Blobs dir should be empty or gone
        let blobs_dir = store.blobs_dir();
        if blobs_dir.exists() {
            let has_files = fs::read_dir(&blobs_dir)
                .unwrap()
                .flatten()
                .any(|e| e.path().is_dir());
            assert!(
                !has_files,
                "blobs dir should have no shard subdirs after clear"
            );
        }

        // Blobs table should be empty
        let count: i64 = store
            .db
            .query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_get_lazily_migrates_legacy_entry() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Simulate a legacy entry: artifacts in entry dir, no blobs
        let entry_dir = config.store_dir().join("old_key");
        fs::create_dir_all(&entry_dir).unwrap();
        let content = b"old format artifact";
        fs::write(entry_dir.join("lib.rlib"), content).unwrap();

        let hash = crate::cache_key::hash_file(&entry_dir.join("lib.rlib")).unwrap();
        let meta = EntryMeta {
            cache_key: "old_key".to_string(),
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            crate_name: "old_crate".to_string(),
            crate_types: vec!["lib".to_string()],
            files: vec![CachedFile {
                name: "lib.rlib".to_string(),
                size: content.len() as u64,
                hash: hash.clone(),
                executable: false,
            }],
            stdout: String::new(),
            stderr: String::new(),
            features: vec![],
            target: String::new(),
            profile: "dev".to_string(),
            compile_time_ms: 0,
            emit_kinds: Vec::new(),
        };
        fs::write(
            entry_dir.join("meta.json"),
            serde_json::to_string_pretty(&meta).unwrap(),
        )
        .unwrap();
        store
            .db
            .execute(
                "INSERT INTO entries (cache_key, crate_name, size, committed) VALUES ('old_key', 'old_crate', ?1, 1)",
                params![content.len() as i64],
            )
            .unwrap();

        // get() should transparently migrate the entry
        let result = store.get("old_key").unwrap();
        assert!(result.is_some());

        // Blob should now exist
        let blob = store.blob_path(&hash);
        assert!(
            blob.exists(),
            "get() should have migrated artifact to blob store"
        );

        // Artifact should be gone from entry dir
        assert!(!entry_dir.join("lib.rlib").exists());
    }

    #[test]
    fn get_evicts_when_lazy_legacy_migration_fails() {
        // Covers get lazy-migration error warning branch.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let entry_dir = config.store_dir().join("old_bad_key");
        fs::create_dir_all(&entry_dir).unwrap();
        let artifact = entry_dir.join("lib.rlib");
        fs::write(&artifact, b"old format artifact").unwrap();
        let hash = crate::cache_key::hash_file(&artifact).unwrap();
        let meta = EntryMeta {
            cache_key: "old_bad_key".to_string(),
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            crate_name: "old_bad_crate".to_string(),
            crate_types: vec!["lib".to_string()],
            files: vec![CachedFile {
                name: "lib.rlib".to_string(),
                size: fs::metadata(&artifact).unwrap().len(),
                hash: hash.clone(),
                executable: false,
            }],
            stdout: String::new(),
            stderr: String::new(),
            features: vec![],
            target: String::new(),
            profile: "dev".to_string(),
            compile_time_ms: 0,
            emit_kinds: Vec::new(),
        };
        fs::write(
            entry_dir.join("meta.json"),
            serde_json::to_string_pretty(&meta).unwrap(),
        )
        .unwrap();
        store
            .db
            .execute(
                "INSERT INTO entries (cache_key, crate_name, size, committed) \
                 VALUES ('old_bad_key', 'old_bad_crate', ?1, 1)",
                params![fs::metadata(&artifact).unwrap().len() as i64],
            )
            .unwrap();

        let shard_path = store.blobs_dir().join(&hash[..2]);
        fs::create_dir_all(store.blobs_dir()).unwrap();
        fs::write(&shard_path, b"not a shard directory").unwrap();

        let result = store.get("old_bad_key").unwrap();

        assert!(
            result.is_none(),
            "failed migration falls through to eviction"
        );
        assert!(!store.contains("old_bad_key"));
        assert!(shard_path.is_file(), "unrelated shard conflict remains");
    }

    #[test]
    fn test_migrate_to_blobs_bulk() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let content = b"shared artifact bytes";
        let hash = {
            let tmp = dir.path().join("tmp");
            fs::write(&tmp, content).unwrap();
            crate::cache_key::hash_file(&tmp).unwrap()
        };

        // Create two legacy entries with identical content
        for key in &["old1", "old2"] {
            let entry_dir = config.store_dir().join(key);
            fs::create_dir_all(&entry_dir).unwrap();
            fs::write(entry_dir.join("lib.rlib"), content).unwrap();

            let meta = EntryMeta {
                cache_key: key.to_string(),
                key_schema: crate::cache_key::CACHE_KEY_VERSION,
                crate_name: "shared_crate".to_string(),
                crate_types: vec!["lib".to_string()],
                files: vec![CachedFile {
                    name: "lib.rlib".to_string(),
                    size: content.len() as u64,
                    hash: hash.clone(),
                    executable: false,
                }],
                stdout: String::new(),
                stderr: String::new(),
                features: vec![],
                target: String::new(),
                profile: "dev".to_string(),
                compile_time_ms: 0,
                emit_kinds: Vec::new(),
            };
            fs::write(
                entry_dir.join("meta.json"),
                serde_json::to_string_pretty(&meta).unwrap(),
            )
            .unwrap();
            store
                .db
                .execute(
                    &format!(
                        "INSERT INTO entries (cache_key, crate_name, size, committed) VALUES ('{key}', 'shared_crate', {}, 1)",
                        content.len()
                    ),
                    [],
                )
                .unwrap();
        }

        let stats = store.migrate_to_blobs(|_, _| {}).unwrap();
        assert_eq!(stats.entries_migrated, 2);

        // Refcount should be 2
        let refcount: i64 = store
            .db
            .query_row(
                "SELECT refcount FROM blobs WHERE hash = ?1",
                params![hash],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(refcount, 2);
    }

    #[test]
    fn test_blob_stats() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Empty store
        let stats = store.blob_stats().unwrap();
        assert_eq!(stats.total_blobs, 0);
        assert_eq!(stats.savings, 0);

        // Add two entries with same content
        let output = dir.path().join("lib.rlib");
        rewrite_source(&output, b"shared content!");
        store
            .put(
                "k1",
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output.clone(), "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        rewrite_source(&output, b"shared content!");
        store
            .put(
                "k2",
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        let stats = store.blob_stats().unwrap();
        assert_eq!(stats.total_blobs, 1); // one unique blob
        assert!(stats.total_logical_size > stats.total_blob_size); // dedup savings
        assert!(stats.savings > 0);
    }

    // =========================================================================
    // Comprehensive dedup integration tests
    // =========================================================================

    /// Helper: create a temp file with given content and return its path.
    fn write_temp_file(dir: &Path, name: &str, content: &[u8]) -> PathBuf {
        let path = dir.join(name);
        rewrite_source(&path, content);
        path
    }

    /// (Re)write a source file that an earlier `put` may have turned into a
    /// read-only hardlink of a store blob (non-CoW filesystems): unlink
    /// first, so the write neither fails with EACCES as an unprivileged user
    /// nor reaches the blob through the shared inode.
    fn rewrite_source(path: &Path, content: &[u8]) {
        let _ = fs::remove_file(path);
        fs::write(path, content).unwrap();
    }

    /// Helper: read meta.json for a cache key and return the EntryMeta.
    fn read_meta(store: &Store, cache_key: &str) -> EntryMeta {
        let meta_path = store.entry_dir(cache_key).join("meta.json");
        let content = fs::read_to_string(&meta_path).unwrap();
        serde_json::from_str(&content).unwrap()
    }

    /// Helper: query refcount for a blob hash, returns None if blob doesn't exist in DB.
    fn blob_refcount(store: &Store, hash: &str) -> Option<i64> {
        store
            .db
            .query_row(
                "SELECT refcount FROM blobs WHERE hash = ?1",
                params![hash],
                |row| row.get(0),
            )
            .ok()
    }

    /// Helper: count rows in blobs table.
    fn blob_table_count(store: &Store) -> i64 {
        store
            .db
            .query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get(0))
            .unwrap()
    }

    #[test]
    fn test_full_dedup_lifecycle() {
        // Put two entries with some shared and some unique files.
        // Verify blobs exist and refcounts are correct.
        // Remove one entry — shared blobs still exist (refcount decremented).
        // Remove second entry — all blobs are deleted (refcount 0).
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Shared content between entries 1 and 2
        let shared = write_temp_file(dir.path(), "shared.rlib", b"shared artifact data");
        // Unique content for entry 1
        let unique1 = write_temp_file(dir.path(), "unique1.rlib", b"unique to entry 1");
        // Unique content for entry 2
        let unique2 = write_temp_file(dir.path(), "unique2.rlib", b"unique to entry 2");

        // Put entry 1: shared + unique1
        store
            .put(
                "entry1",
                "crate_a",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[
                    (shared.clone(), "shared.rlib".into()),
                    (unique1, "unique1.rlib".into()),
                ],
                "",
                "",
            )
            .unwrap();

        // Re-create shared file (put() reads from source path, content must exist)
        rewrite_source(&shared, b"shared artifact data");

        // Put entry 2: shared + unique2
        store
            .put(
                "entry2",
                "crate_b",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[
                    (shared, "shared.rlib".into()),
                    (unique2, "unique2.rlib".into()),
                ],
                "",
                "",
            )
            .unwrap();

        // Read metadata to get hashes
        let meta1 = read_meta(&store, "entry1");
        let meta2 = read_meta(&store, "entry2");
        let shared_hash = &meta1
            .files
            .iter()
            .find(|f| f.name == "shared.rlib")
            .unwrap()
            .hash;
        let unique1_hash = &meta1
            .files
            .iter()
            .find(|f| f.name == "unique1.rlib")
            .unwrap()
            .hash;
        let unique2_hash = &meta2
            .files
            .iter()
            .find(|f| f.name == "unique2.rlib")
            .unwrap()
            .hash;

        // Shared blob should have the same hash in both entries
        let shared_hash2 = &meta2
            .files
            .iter()
            .find(|f| f.name == "shared.rlib")
            .unwrap()
            .hash;
        assert_eq!(shared_hash, shared_hash2);

        // Verify refcounts: shared=2, unique1=1, unique2=1
        assert_eq!(blob_refcount(&store, shared_hash), Some(2));
        assert_eq!(blob_refcount(&store, unique1_hash), Some(1));
        assert_eq!(blob_refcount(&store, unique2_hash), Some(1));

        // All blob files should exist on disk
        assert!(store.blob_path(shared_hash).exists());
        assert!(store.blob_path(unique1_hash).exists());
        assert!(store.blob_path(unique2_hash).exists());

        // Remove entry 1 — shared blob should still exist, unique1 blob should be gone
        store.remove_entry("entry1").unwrap();
        assert_eq!(blob_refcount(&store, shared_hash), Some(1));
        assert!(store.blob_path(shared_hash).exists());
        assert!(!store.blob_path(unique1_hash).exists());
        assert_eq!(blob_refcount(&store, unique1_hash), None);

        // Remove entry 2 — everything should be gone
        store.remove_entry("entry2").unwrap();
        assert!(!store.blob_path(shared_hash).exists());
        assert!(!store.blob_path(unique2_hash).exists());
        assert_eq!(blob_refcount(&store, shared_hash), None);
        assert_eq!(blob_refcount(&store, unique2_hash), None);
        assert_eq!(blob_table_count(&store), 0);
    }

    #[test]
    fn gc_lock_is_mutually_exclusive() {
        // kunobi-ninja/kache#326: the cross-process GC lock admits one holder at
        // a time and is re-acquirable after release.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let first = store.try_gc_lock().unwrap();
        assert!(first.is_some(), "first GC lock acquires");
        assert!(
            store.try_gc_lock().unwrap().is_none(),
            "a second GC lock is refused while the first is held"
        );
        drop(first);
        assert!(
            store.try_gc_lock().unwrap().is_some(),
            "the GC lock is re-acquirable after release"
        );
    }

    #[cfg(unix)]
    #[test]
    fn gc_lock_does_not_expire_live_holder_by_mtime() {
        // A live holder must not be considered stale just because the marker
        // file is old; large stores can make GC run for a long time.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let first = store.try_gc_lock().unwrap().expect("first GC lock");
        let lock_path = config.store_dir().join("gc.lock");
        let old = filetime::FileTime::from_system_time(
            std::time::SystemTime::now() - std::time::Duration::from_secs(2 * 3600),
        );
        filetime::set_file_mtime(&lock_path, old).unwrap();

        assert!(
            store.try_gc_lock().unwrap().is_none(),
            "an old marker file must not let a second GC steal a live lock"
        );
        drop(first);
        assert!(store.try_gc_lock().unwrap().is_some());
    }

    #[test]
    fn verify_restores_evicts_a_corrupted_blob() {
        // kunobi-ninja/kache#332: with the opt-in guard on, a blob whose content
        // no longer matches its address (silent corruption) is caught on the hit
        // path and evicted → miss → recompile, instead of poisoning the build.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let f = write_temp_file(dir.path(), "lib.rlib", b"the real artifact bytes");
        store
            .put(
                "vkey",
                "vcrate",
                &["lib".into()],
                &[],
                "aarch64-apple-darwin",
                "release",
                &[(f, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        let meta = store.get("vkey").unwrap().expect("entry present after put");
        let blob = store.blob_path(&meta.files[0].hash);

        // Corrupt the blob in place, keeping the SAME size so the size check
        // passes and only the content (vs its address) differs.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&blob, std::fs::Permissions::from_mode(0o644)).unwrap();
        }
        #[cfg(not(unix))]
        {
            let mut p = std::fs::metadata(&blob).unwrap().permissions();
            p.set_readonly(false);
            std::fs::set_permissions(&blob, p).unwrap();
        }
        std::fs::write(&blob, vec![b'X'; meta.files[0].size as usize]).unwrap();

        let _env_lock = ENV_VAR_TEST_LOCK.lock().unwrap();

        // Guard OFF (default): size matches, content not checked -> still a hit.
        {
            let _verify_off = EnvVarGuard::remove("KACHE_VERIFY_RESTORES");
            assert!(
                store.get("vkey").unwrap().is_some(),
                "without the guard a same-size corrupt blob is not caught"
            );
        }

        // Guard ON: content mismatch -> entry evicted -> miss.
        let result = {
            let _verify_on = EnvVarGuard::set("KACHE_VERIFY_RESTORES", "1");
            store.get("vkey").unwrap()
        };
        assert!(
            result.is_none(),
            "the guard must evict a blob whose content != its address"
        );
    }

    /// kunobi-ninja/kache#332: the env value maps to off|sampled|always, with the
    /// legacy boolean spellings preserved as `Always`.
    #[test]
    fn verify_restores_mode_parses_tristate() {
        assert_eq!(parse_verify_restores(None), VerifyRestores::Off);
        assert_eq!(parse_verify_restores(Some("")), VerifyRestores::Off);
        assert_eq!(parse_verify_restores(Some("0")), VerifyRestores::Off);
        assert_eq!(parse_verify_restores(Some("off")), VerifyRestores::Off);
        assert_eq!(
            parse_verify_restores(Some("sampled")),
            VerifyRestores::Sampled
        );
        assert_eq!(
            parse_verify_restores(Some("SAMPLED")),
            VerifyRestores::Sampled
        );
        assert_eq!(
            parse_verify_restores(Some("always")),
            VerifyRestores::Always
        );
        // Back-compat: the old boolean values still mean "verify every hit".
        assert_eq!(parse_verify_restores(Some("1")), VerifyRestores::Always);
        assert_eq!(parse_verify_restores(Some("true")), VerifyRestores::Always);
    }

    /// kunobi-ninja/kache#332: Off never verifies, Always always does, and
    /// Sampled verifies exactly one in every `VERIFY_SAMPLE_RATE` consecutive
    /// hits (the rolling counter increments by one per call, so any window of
    /// that size contains exactly one multiple — independent of the start).
    #[test]
    fn verify_restores_sampling_cadence() {
        assert!(!should_verify_this_restore(VerifyRestores::Off));
        assert!(should_verify_this_restore(VerifyRestores::Always));

        let window = VERIFY_SAMPLE_RATE as usize;
        let verified = (0..window)
            .filter(|_| should_verify_this_restore(VerifyRestores::Sampled))
            .count();
        assert_eq!(
            verified, 1,
            "exactly one in {window} consecutive sampled hits must verify"
        );
    }

    #[test]
    fn test_put_get_restore_cycle() {
        // Put an entry with multiple files, get it, verify metadata,
        // verify blob files exist and are read-only,
        // verify entry dir only contains meta.json.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let file_a = write_temp_file(dir.path(), "a.rlib", b"rlib artifact content");
        let file_b = write_temp_file(dir.path(), "b.dylib", b"dylib artifact content");
        let file_c = write_temp_file(dir.path(), "c.rmeta", b"rmeta artifact content");

        store
            .put(
                "multi_key",
                "multi_crate",
                &["lib".into(), "dylib".into()],
                &["serde".into(), "tokio".into()],
                "aarch64-apple-darwin",
                "release",
                &[
                    (file_a, "a.rlib".into()),
                    (file_b, "b.dylib".into()),
                    (file_c, "c.rmeta".into()),
                ],
                "some stdout",
                "some stderr",
            )
            .unwrap();

        // Get the entry and verify metadata
        let meta = store.get("multi_key").unwrap().unwrap();
        assert_eq!(meta.crate_name, "multi_crate");
        assert_eq!(meta.crate_types, vec!["lib", "dylib"]);
        assert_eq!(meta.features, vec!["serde", "tokio"]);
        assert_eq!(meta.target, "aarch64-apple-darwin");
        assert_eq!(meta.profile, "release");
        assert_eq!(meta.stdout, "some stdout");
        assert_eq!(meta.stderr, "some stderr");
        assert_eq!(meta.files.len(), 3);

        // Verify blob files exist and are read-only
        for cached_file in &meta.files {
            let blob = store.blob_path(&cached_file.hash);
            assert!(blob.exists(), "blob for {} should exist", cached_file.name);
            let perms = fs::metadata(&blob).unwrap().permissions();
            assert!(
                perms.readonly(),
                "blob for {} should be read-only",
                cached_file.name
            );
        }

        // Verify entry dir only contains meta.json
        let entry_dir = store.entry_dir("multi_key");
        let mut files: Vec<String> = fs::read_dir(&entry_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        files.sort();
        assert_eq!(files, vec!["meta.json"]);
    }

    #[test]
    fn test_clear_removes_all_blobs_and_tables() {
        // Put a few entries, call clear(), verify blobs directory and tables are empty.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Create 3 entries with different content
        for i in 0..3 {
            let file = write_temp_file(
                dir.path(),
                &format!("f{i}.rlib"),
                format!("content {i}").as_bytes(),
            );
            store
                .put(
                    &format!("key{i}"),
                    &format!("crate{i}"),
                    &["lib".into()],
                    &[],
                    "",
                    "dev",
                    &[(file, format!("lib{i}.rlib"))],
                    "",
                    "",
                )
                .unwrap();
        }

        assert_eq!(store.entry_count().unwrap(), 3);
        assert!(blob_table_count(&store) >= 3);

        store.clear().unwrap();

        // Entries table should be empty
        assert_eq!(store.entry_count().unwrap(), 0);

        // Blobs table should be empty
        assert_eq!(blob_table_count(&store), 0);

        // Blobs directory should be empty or removed
        let blobs_dir = store.blobs_dir();
        if blobs_dir.exists() {
            let any_content = fs::read_dir(&blobs_dir).unwrap().flatten().any(|_| true);
            assert!(!any_content, "blobs dir should be empty after clear");
        }
    }

    #[test]
    fn test_migration_of_legacy_entry() {
        // Create a "legacy" entry by manually writing files to an entry dir
        // (meta.json + artifact files, without blob store).
        // Call migrate_entry_to_blobs() directly.
        // Verify artifacts moved to blob store, entry dir only has meta.json.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let entry_dir = config.store_dir().join("legacy_key");
        fs::create_dir_all(&entry_dir).unwrap();

        // Create two legacy artifact files
        let content_a = b"legacy artifact A";
        let content_b = b"legacy artifact B";
        fs::write(entry_dir.join("a.rlib"), content_a).unwrap();
        fs::write(entry_dir.join("b.dylib"), content_b).unwrap();

        let hash_a = crate::cache_key::hash_file(&entry_dir.join("a.rlib")).unwrap();
        let hash_b = crate::cache_key::hash_file(&entry_dir.join("b.dylib")).unwrap();

        let meta = EntryMeta {
            cache_key: "legacy_key".to_string(),
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            crate_name: "legacy_crate".to_string(),
            crate_types: vec!["lib".to_string()],
            files: vec![
                CachedFile {
                    name: "a.rlib".to_string(),
                    size: content_a.len() as u64,
                    hash: hash_a.clone(),
                    executable: false,
                },
                CachedFile {
                    name: "b.dylib".to_string(),
                    size: content_b.len() as u64,
                    hash: hash_b.clone(),
                    executable: false,
                },
            ],
            stdout: String::new(),
            stderr: String::new(),
            features: vec![],
            target: String::new(),
            profile: "dev".to_string(),
            compile_time_ms: 0,
            emit_kinds: Vec::new(),
        };
        fs::write(
            entry_dir.join("meta.json"),
            serde_json::to_string_pretty(&meta).unwrap(),
        )
        .unwrap();

        // Register in DB as committed
        store
            .db
            .execute(
                "INSERT INTO entries (cache_key, crate_name, size, committed) VALUES ('legacy_key', 'legacy_crate', ?1, 1)",
                params![(content_a.len() + content_b.len()) as i64],
            )
            .unwrap();

        // Call migrate_entry_to_blobs directly
        store.migrate_entry_to_blobs(&meta).unwrap();

        // Artifacts should be gone from entry dir
        assert!(
            !entry_dir.join("a.rlib").exists(),
            "a.rlib should be moved to blob store"
        );
        assert!(
            !entry_dir.join("b.dylib").exists(),
            "b.dylib should be moved to blob store"
        );

        // meta.json should remain
        assert!(entry_dir.join("meta.json").exists());

        // Blobs should exist and be read-only
        let blob_a = store.blob_path(&hash_a);
        let blob_b = store.blob_path(&hash_b);
        assert!(blob_a.exists(), "blob for a.rlib should exist");
        assert!(blob_b.exists(), "blob for b.dylib should exist");
        assert!(fs::metadata(&blob_a).unwrap().permissions().readonly());
        assert!(fs::metadata(&blob_b).unwrap().permissions().readonly());

        // Refcounts should be 1
        assert_eq!(blob_refcount(&store, &hash_a), Some(1));
        assert_eq!(blob_refcount(&store, &hash_b), Some(1));

        // Entry dir should only have meta.json
        let files: Vec<String> = fs::read_dir(&entry_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(files, vec!["meta.json"]);
    }

    #[test]
    fn migrate_entry_to_blobs_bumps_refcount_when_insert_loses_race() {
        // Covers migrate_entry_to_blobs INSERT OR IGNORE changes()==0 branch.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let entry_dir = store.entry_dir("legacy_race");
        fs::create_dir_all(&entry_dir).unwrap();
        let artifact = entry_dir.join("lib.rlib");
        fs::write(&artifact, b"legacy race artifact").unwrap();
        let hash = crate::cache_key::hash_file(&artifact).unwrap();
        let size = fs::metadata(&artifact).unwrap().len();
        let meta = EntryMeta {
            cache_key: "legacy_race".to_string(),
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            crate_name: "legacy_crate".to_string(),
            crate_types: vec!["lib".to_string()],
            files: vec![CachedFile {
                name: "lib.rlib".to_string(),
                size,
                hash: hash.clone(),
                executable: false,
            }],
            stdout: String::new(),
            stderr: String::new(),
            features: vec![],
            target: String::new(),
            profile: "dev".to_string(),
            compile_time_ms: 0,
            emit_kinds: Vec::new(),
        };

        store
            .db
            .execute(
                &format!(
                    "CREATE TEMP TRIGGER seed_blob_before_insert \
                     BEFORE INSERT ON blobs \
                     WHEN NEW.hash = '{hash}' \
                     BEGIN \
                       INSERT OR IGNORE INTO blobs (hash, size, refcount) \
                       VALUES (NEW.hash, NEW.size, 41); \
                     END"
                ),
                [],
            )
            .unwrap();

        store.migrate_entry_to_blobs(&meta).unwrap();

        assert_eq!(blob_refcount(&store, &hash), Some(42));
        assert!(store.blob_path(&hash).is_file());
        assert!(!artifact.exists());
    }

    #[test]
    fn test_eviction_with_shared_blobs() {
        // Put 3 entries where entries 1 and 2 share blobs, entry 3 is unique.
        // Remove entry 1 → shared blobs persist with refcount decremented.
        // Remove entry 2 → shared blobs deleted.
        // Entry 3's blobs should be unaffected throughout.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let shared_content = b"shared between 1 and 2";
        let unique3_content = b"unique to entry 3 only";

        // Entry 1: shared blob
        let f = write_temp_file(dir.path(), "shared.rlib", shared_content);
        store
            .put(
                "e1",
                "c1",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(f, "shared.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // Entry 2: same shared blob
        let f = write_temp_file(dir.path(), "shared.rlib", shared_content);
        store
            .put(
                "e2",
                "c2",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(f, "shared.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // Entry 3: unique blob
        let f = write_temp_file(dir.path(), "unique3.rlib", unique3_content);
        store
            .put(
                "e3",
                "c3",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(f, "unique3.rlib".into())],
                "",
                "",
            )
            .unwrap();

        let meta1 = read_meta(&store, "e1");
        let meta3 = read_meta(&store, "e3");
        let shared_hash = &meta1.files[0].hash;
        let unique3_hash = &meta3.files[0].hash;

        assert_eq!(blob_refcount(&store, shared_hash), Some(2));
        assert_eq!(blob_refcount(&store, unique3_hash), Some(1));

        // Remove entry 1 — shared blob persists
        store.remove_entry("e1").unwrap();
        assert_eq!(blob_refcount(&store, shared_hash), Some(1));
        assert!(store.blob_path(shared_hash).exists());
        // Entry 3 unaffected
        assert!(store.blob_path(unique3_hash).exists());
        assert_eq!(blob_refcount(&store, unique3_hash), Some(1));

        // Remove entry 2 — shared blob now deleted
        store.remove_entry("e2").unwrap();
        assert!(!store.blob_path(shared_hash).exists());
        assert_eq!(blob_refcount(&store, shared_hash), None);
        // Entry 3 still unaffected
        assert!(store.blob_path(unique3_hash).exists());
        assert_eq!(blob_refcount(&store, unique3_hash), Some(1));

        // Verify entry 3 can still be retrieved
        let meta = store.get("e3").unwrap();
        assert!(meta.is_some());
    }

    #[test]
    fn test_blob_stats_with_known_overlap() {
        // Put entries with known content overlap.
        // Verify logical vs physical size, savings percentage.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let shared_content = b"AAAA"; // 4 bytes, shared by entries 1 and 2
        let unique_content = b"BBBBBBBB"; // 8 bytes, only in entry 1

        // Entry 1: shared (4 bytes) + unique (8 bytes) = 12 bytes logical
        let f_shared = write_temp_file(dir.path(), "shared.rlib", shared_content);
        let f_unique = write_temp_file(dir.path(), "unique.rlib", unique_content);
        store
            .put(
                "stats1",
                "c1",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[
                    (f_shared, "shared.rlib".into()),
                    (f_unique, "unique.rlib".into()),
                ],
                "",
                "",
            )
            .unwrap();

        // Entry 2: shared (4 bytes) = 4 bytes logical
        let f_shared = write_temp_file(dir.path(), "shared.rlib", shared_content);
        store
            .put(
                "stats2",
                "c2",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(f_shared, "shared.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // Total logical size from entries table = 12 + 4 = 16 bytes
        // Total physical blob size = 4 (shared) + 8 (unique) = 12 bytes
        // Savings = 16 - 12 = 4 bytes
        let stats = store.blob_stats().unwrap();
        assert_eq!(stats.total_blobs, 2, "should have 2 unique blobs");
        assert_eq!(
            stats.total_blob_size, 12,
            "physical size should be 12 bytes"
        );
        assert_eq!(
            stats.total_logical_size, 16,
            "logical size should be 16 bytes"
        );
        assert_eq!(stats.savings, 4, "savings should be 4 bytes");
    }

    /// kunobi-ninja/kache#324: pin an exact `content_hash` for a fixed
    /// multi-file entry. `compute_content_hash` folds `(name, hash, size,
    /// exec-bit)` in a stable serialization; this golden value fails loudly if
    /// that serialization ever drifts (field order, length-prefixing, exec-bit
    /// encoding), which would silently change dedup behavior across versions.
    #[test]
    fn content_hash_golden_pins_serialization() {
        let cf = |name: &str, size: u64, hash: &str, executable: bool| CachedFile {
            name: name.to_string(),
            size,
            hash: hash.to_string(),
            executable,
        };
        // Deliberately unsorted on input — compute_content_hash sorts internally.
        let files = vec![
            cf("foo", 4096, "cccccccccccccccccccccccccccccccc", true),
            cf(
                "libfoo.rlib",
                1024,
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                false,
            ),
            cf(
                "libfoo.rmeta",
                256,
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                false,
            ),
        ];
        assert_eq!(
            compute_content_hash(&files),
            "2dd3b89296eb2d5469d11aa00b312cee8734923698b97890f43ea6a8b9a37585",
        );
    }

    /// kunobi-ninja/kache#325: an entry's covered emit kinds are derived from
    /// its stored filenames, deduped and sorted.
    #[test]
    fn emit_kinds_derived_from_files() {
        let cf = |name: &str| CachedFile {
            name: name.to_string(),
            size: 1,
            hash: "h".to_string(),
            executable: false,
        };
        // A lib `--emit=link` build: rlib + side rmeta + dep-info.
        let kinds = emit_kinds_for_files(&[
            cf("libfoo.rlib"),
            cf("libfoo.rmeta"),
            cf("foo.d"),
            cf("foo.dSYM"), // sidecar → no emit kind, ignored
        ]);
        assert_eq!(kinds, vec!["dep-info", "link", "metadata"]);
    }

    /// kunobi-ninja/kache#325: the lookup gate is superset-tolerant, skips empty
    /// (pre-gate) entries, and rejects genuinely-missing kinds.
    #[test]
    fn covers_requested_emit_semantics() {
        let mk = |kinds: &[&str]| EntryMeta {
            cache_key: "k".into(),
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            crate_name: "c".into(),
            crate_types: vec![],
            files: vec![],
            stdout: String::new(),
            stderr: String::new(),
            features: vec![],
            target: String::new(),
            profile: String::new(),
            compile_time_ms: 0,
            emit_kinds: kinds.iter().map(|s| s.to_string()).collect(),
        };
        let req = |kinds: &[&str]| -> Vec<String> { kinds.iter().map(|s| s.to_string()).collect() };

        // Superset: entry has link+metadata+dep-info, request just link.
        assert!(mk(&["dep-info", "link", "metadata"]).covers_requested_emit(&req(&["link"])));
        // Exact.
        assert!(
            mk(&["dep-info", "metadata"]).covers_requested_emit(&req(&["dep-info", "metadata"]))
        );
        // Missing the requested obj → not covered.
        assert!(!mk(&["link"]).covers_requested_emit(&req(&["link", "obj"])));
        // Pre-gate entry (no recorded kinds) → skip the check.
        assert!(mk(&[]).covers_requested_emit(&req(&["link", "obj"])));
        // A requested kind the gate can't map to a file is ignored, not rejected.
        assert!(mk(&["link"]).covers_requested_emit(&req(&["link", "future-exotic"])));
    }

    /// kunobi-ninja/kache#431: a wasm32 target's link product is a `.wasm`
    /// file. Until it mapped to the `link` emit kind, an entry built for
    /// `--emit=link,dep-info` derived only `["dep-info"]`, so the coverage
    /// gate refused to store it — silently blocking every wasm module,
    /// including substrate's runtime crates (the bench's most expensive
    /// compiles).
    #[test]
    fn wasm_link_output_satisfies_the_emit_coverage_gate() {
        let files = vec![
            CachedFile {
                name: "rococo_runtime.wasm".into(),
                size: 4,
                hash: "h1".into(),
                executable: false,
            },
            CachedFile {
                name: "rococo_runtime.d".into(),
                size: 4,
                hash: "h2".into(),
                executable: false,
            },
        ];
        let kinds = emit_kinds_for_files(&files);
        assert_eq!(
            kinds,
            vec!["dep-info".to_string(), "link".to_string()],
            "a .wasm module is the link product of a wasm32 target"
        );

        let meta = EntryMeta {
            cache_key: "k".into(),
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            crate_name: "rococo_runtime".into(),
            crate_types: vec!["cdylib".into()],
            files,
            stdout: String::new(),
            stderr: String::new(),
            features: vec![],
            target: "wasm32-unknown-unknown".into(),
            profile: "release".into(),
            compile_time_ms: 62_000,
            emit_kinds: kinds,
        };
        assert!(
            meta.covers_requested_emit(&["link".to_string(), "dep-info".to_string()]),
            "the entry must satisfy the --emit it was built for"
        );
    }

    #[test]
    fn test_put_stores_content_hash() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(tmp.path());
        let store = Store::open(&config).unwrap();

        let dir = tmp.path().join("src");
        std::fs::create_dir_all(&dir).unwrap();
        let file1 = dir.join("lib.rlib");
        std::fs::write(&file1, b"artifact-content-1234").unwrap();

        store
            .put(
                "key_ch_1",
                "mycrate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(file1, "lib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        let ch: String = store
            .db
            .query_row(
                "SELECT content_hash FROM entries WHERE cache_key = 'key_ch_1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            ch.len(),
            64,
            "content_hash should be full blake3 hex (64 chars)"
        );
    }

    #[test]
    fn test_import_downloaded_entry_stores_content_hash() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(tmp.path());
        let store = Store::open(&config).unwrap();

        let entry_dir = store.entry_dir("dl_ch_test");
        std::fs::create_dir_all(&entry_dir).unwrap();

        let artifact = entry_dir.join("lib.rlib");
        std::fs::write(&artifact, b"downloaded-artifact-data").unwrap();
        let hash = crate::cache_key::hash_file(&artifact).unwrap();
        let size = std::fs::metadata(&artifact).unwrap().len();

        let meta = EntryMeta {
            cache_key: "dl_ch_test".to_string(),
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            crate_name: "dlcrate".to_string(),
            crate_types: vec!["lib".to_string()],
            files: vec![CachedFile {
                name: "lib.rlib".to_string(),
                size,
                hash,
                executable: false,
            }],
            stdout: String::new(),
            stderr: String::new(),
            features: vec![],
            target: "x86_64-unknown-linux-gnu".to_string(),
            profile: "dev".to_string(),
            compile_time_ms: 0,
            emit_kinds: Vec::new(),
        };
        std::fs::write(
            entry_dir.join("meta.json"),
            serde_json::to_string_pretty(&meta).unwrap(),
        )
        .unwrap();

        store.import_downloaded_entry("dl_ch_test").unwrap();

        let ch: String = store
            .db
            .query_row(
                "SELECT content_hash FROM entries WHERE cache_key = 'dl_ch_test'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ch.len(), 64);
    }

    #[test]
    fn verified_batch_import_is_atomic_and_registers_every_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(tmp.path());
        let store = Store::open(&config).unwrap();
        let keys = [
            blake3::hash(b"packed-batch-a").to_hex().to_string(),
            blake3::hash(b"packed-batch-b").to_hex().to_string(),
        ];

        let mut verified = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            let entry_dir = store.entry_dir(key);
            std::fs::create_dir_all(&entry_dir).unwrap();
            let artifact = entry_dir.join(format!("lib{index}.rlib"));
            let contents = format!("verified packed artifact {index}");
            std::fs::write(&artifact, contents.as_bytes()).unwrap();
            let meta = EntryMeta {
                cache_key: key.clone(),
                key_schema: crate::cache_key::CACHE_KEY_VERSION,
                crate_name: format!("crate{index}"),
                crate_types: vec!["lib".to_string()],
                files: vec![CachedFile {
                    name: format!("lib{index}.rlib"),
                    size: contents.len() as u64,
                    hash: blake3::hash(contents.as_bytes()).to_hex().to_string(),
                    executable: false,
                }],
                stdout: String::new(),
                stderr: String::new(),
                features: vec![],
                target: "x86_64-unknown-linux-gnu".to_string(),
                profile: "dev".to_string(),
                compile_time_ms: 1,
                emit_kinds: Vec::new(),
            };
            std::fs::write(
                entry_dir.join("meta.json"),
                serde_json::to_vec_pretty(&meta).unwrap(),
            )
            .unwrap();
            verified.push(VerifiedRestoredEntry {
                cache_key: key.clone(),
                meta,
            });
        }

        let original_size = verified[1].meta.files[0].size;
        verified[1].meta.files[0].size += 1;
        assert!(store.import_verified_restored_entries(&verified).is_err());
        let rows: i64 = store
            .db
            .query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows, 0, "a failed preflight must register no batch rows");

        verified[1].meta.files[0].size = original_size;
        store
            .db
            .execute(
                "INSERT INTO entries (cache_key, crate_name, crate_type, profile, num_features, size, content_hash, compile_time_ms, key_schema, committed) VALUES (?1, 'stale', 'lib', 'dev', 0, 0, 'stale', 0, ?2, 0)",
                params![keys[0], crate::cache_key::CACHE_KEY_VERSION],
            )
            .unwrap();
        let imported = store.import_verified_restored_entries(&verified).unwrap();
        assert_eq!(imported, 2);
        let rows: i64 = store
            .db
            .query_row(
                "SELECT COUNT(*) FROM entries WHERE committed = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rows, 2);
        for key in keys {
            assert!(store.get(&key).unwrap().is_some());
        }
        let refcounts: Vec<i64> = store
            .db
            .prepare("SELECT refcount FROM blobs ORDER BY hash")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(refcounts, vec![1, 1]);
    }

    fn write_verified_fixture(
        store: &Store,
        key: &str,
        meta_key: &str,
        artifact_name: &str,
        hash_override: Option<String>,
    ) -> VerifiedRestoredEntry {
        let contents = b"verified fixture artifact";
        let entry_dir = store.entry_dir(key);
        let artifact = entry_dir.join(artifact_name);
        std::fs::create_dir_all(artifact.parent().unwrap()).unwrap();
        std::fs::write(&artifact, contents).unwrap();
        let meta = EntryMeta {
            cache_key: meta_key.to_string(),
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            crate_name: "fixture".to_string(),
            crate_types: vec!["lib".to_string()],
            files: vec![CachedFile {
                name: artifact_name.to_string(),
                size: contents.len() as u64,
                hash: hash_override.unwrap_or_else(|| blake3::hash(contents).to_hex().to_string()),
                executable: false,
            }],
            stdout: String::new(),
            stderr: String::new(),
            features: Vec::new(),
            target: "x86_64-unknown-linux-gnu".to_string(),
            profile: "dev".to_string(),
            compile_time_ms: 1,
            emit_kinds: Vec::new(),
        };
        std::fs::write(
            entry_dir.join("meta.json"),
            serde_json::to_vec_pretty(&meta).unwrap(),
        )
        .unwrap();
        VerifiedRestoredEntry {
            cache_key: key.to_string(),
            meta,
        }
    }

    #[test]
    fn verified_batch_import_checks_each_cache_key_binding_independently() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&test_config(tmp.path())).unwrap();
        let invalid = write_verified_fixture(&store, "invalid", "invalid", "lib.rlib", None);
        assert!(store.import_verified_restored_entries(&[invalid]).is_err());

        let key = blake3::hash(b"valid-outer-key").to_hex().to_string();
        let other = blake3::hash(b"different-meta-key").to_hex().to_string();
        let mismatched = write_verified_fixture(&store, &key, &other, "lib.rlib", None);
        assert!(
            store
                .import_verified_restored_entries(&[mismatched])
                .is_err()
        );
    }

    #[test]
    fn verified_batch_import_checks_each_artifact_field_independently() {
        for (label, name, hash_override) in [
            ("unsafe-name", "nested/lib.rlib", None),
            ("invalid-hash", "lib.rlib", Some("g".repeat(64))),
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let store = Store::open(&test_config(tmp.path())).unwrap();
            let key = blake3::hash(label.as_bytes()).to_hex().to_string();
            let entry = write_verified_fixture(&store, &key, &key, name, hash_override);
            assert!(
                store.import_verified_restored_entries(&[entry]).is_err(),
                "{label} must be rejected independently"
            );
        }

        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&test_config(tmp.path())).unwrap();
        let key = blake3::hash(b"duplicate-artifact").to_hex().to_string();
        let mut entry = write_verified_fixture(&store, &key, &key, "lib.rlib", None);
        entry.meta.files.push(entry.meta.files[0].clone());
        std::fs::write(
            store.entry_dir(&key).join("meta.json"),
            serde_json::to_vec_pretty(&entry.meta).unwrap(),
        )
        .unwrap();
        assert!(store.import_verified_restored_entries(&[entry]).is_err());
    }

    #[test]
    fn verified_batch_import_never_rewrites_an_existing_content_addressed_blob() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&test_config(tmp.path())).unwrap();
        let key = blake3::hash(b"existing-immutable-blob")
            .to_hex()
            .to_string();
        let entry = write_verified_fixture(&store, &key, &key, "lib.rlib", None);
        let blob = store.blob_path(&entry.meta.files[0].hash);
        std::fs::create_dir_all(blob.parent().unwrap()).unwrap();
        std::fs::write(&blob, b"pre-existing immutable blob").unwrap();

        assert_eq!(store.import_verified_restored_entries(&[entry]).unwrap(), 1);
        assert_eq!(std::fs::read(blob).unwrap(), b"pre-existing immutable blob");
    }

    #[test]
    fn verified_blob_install_reports_a_vanished_source_before_rename() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&test_config(tmp.path())).unwrap();
        let file = CachedFile {
            name: "lib.rlib".to_string(),
            size: 7,
            hash: blake3::hash(b"missing verified artifact")
                .to_hex()
                .to_string(),
            executable: false,
        };

        let error = store
            .install_verified_blob(&tmp.path().join("missing-entry"), &file)
            .expect_err("a vanished verified source must fail")
            .to_string();
        assert!(
            error.contains("verified restored blob vanished during batch import"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_list_entries_includes_content_hash() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(tmp.path());
        let store = Store::open(&config).unwrap();

        let dir = tmp.path().join("src");
        std::fs::create_dir_all(&dir).unwrap();
        let file1 = dir.join("lib.rlib");
        std::fs::write(&file1, b"list-test-content").unwrap();

        store
            .put(
                "list_ch_1",
                "mycrate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(file1, "lib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        let entries = store.list_entries("name").unwrap();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].content_hash.is_some());
        assert_eq!(entries[0].content_hash.as_ref().unwrap().len(), 64);
    }

    /// kunobi-ninja/kache#709: byte-identical entries share their blob, so
    /// removing the older key destroys history without reclaiming disk.
    #[test]
    fn evict_duplicate_entries_spares_a_pair_sharing_one_blob() {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = test_config(tmp.path());
        config.max_size = 1;
        let store = Store::open(&config).unwrap();

        let dir = tmp.path().join("src");
        std::fs::create_dir_all(&dir).unwrap();

        let file1 = dir.join("lib.rlib");
        std::fs::write(&file1, b"same-content-bytes").unwrap();

        store
            .put(
                "dup_key_1",
                "mycrate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(file1.clone(), "lib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        // Artificially age the first entry's access time (LRU policy)
        store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour') WHERE cache_key = 'dup_key_1'",
                [],
            )
            .unwrap();

        store
            .put(
                "dup_key_2",
                "mycrate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(file1, "lib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        assert_eq!(store.entry_count().unwrap(), 2);

        let stats = store.evict_duplicate_entries().unwrap();
        assert_eq!(stats.entries_evicted, 0);
        assert_eq!(store.entry_count().unwrap(), 2);
        assert!(store.contains("dup_key_1") && store.contains("dup_key_2"));
    }

    #[test]
    fn evict_duplicate_entries_skips_the_scan_under_budget() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(tmp.path());
        let store = Store::open(&config).unwrap();

        let dir = tmp.path().join("src");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("lib.rlib");
        std::fs::write(&file, b"tiny-shared-content").unwrap();
        store
            .put(
                "under_budget_1",
                "mycrate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(file.clone(), "lib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour') \
                 WHERE cache_key = 'under_budget_1'",
                [],
            )
            .unwrap();
        store
            .put(
                "under_budget_2",
                "mycrate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(file, "lib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        let stats = store.evict_duplicate_entries().unwrap();
        assert!(stats.skipped);
        assert_eq!(stats.entries_evicted, 0);
        assert_eq!(store.entry_count().unwrap(), 2);
    }

    #[test]
    fn evict_duplicate_entries_fails_closed_for_unmapped_legacy_victim() {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = test_config(tmp.path());
        config.max_size = 1;
        let store = Store::open(&config).unwrap();

        let file = tmp.path().join("legacy.rlib");
        std::fs::write(&file, b"shared-legacy-content").unwrap();
        for key in ["legacy_old", "legacy_new"] {
            store
                .put(
                    key,
                    "mycrate",
                    &["lib".to_string()],
                    &[],
                    "x86_64-unknown-linux-gnu",
                    "dev",
                    &[(file.clone(), "lib.rlib".to_string())],
                    "",
                    "",
                )
                .unwrap();
        }
        store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour') \
                 WHERE cache_key = 'legacy_old'",
                [],
            )
            .unwrap();
        store
            .db
            .execute("DELETE FROM entry_blobs WHERE cache_key = 'legacy_old'", [])
            .unwrap();

        let stats = store.evict_duplicate_entries().unwrap();
        assert_eq!(stats.entries_evicted, 0);
        assert!(store.contains("legacy_old"));
        assert!(store.contains("legacy_new"));
    }

    #[test]
    fn evict_duplicate_entries_stops_at_the_physical_target() {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = test_config(tmp.path());
        config.max_size = 500; // physical 600; target 450
        let store = Store::open(&config).unwrap();

        for group in 0..3 {
            let old_key = format!("budget_old_{group}");
            let new_key = format!("budget_new_{group}");
            let old_file = tmp.path().join(format!("old-{group}.rlib"));
            let new_file = tmp.path().join(format!("new-{group}.rlib"));
            std::fs::write(&old_file, vec![group as u8 + 1; 100]).unwrap();
            std::fs::write(&new_file, vec![group as u8 + 11; 100]).unwrap();

            store
                .put(
                    &old_key,
                    "mycrate",
                    &["lib".to_string()],
                    &[],
                    "x86_64-unknown-linux-gnu",
                    "dev",
                    &[(old_file, "lib.rlib".to_string())],
                    "",
                    "",
                )
                .unwrap();
            store
                .db
                .execute(
                    "UPDATE entries SET last_accessed = datetime('now', ?1) \
                     WHERE cache_key = ?2",
                    params![format!("-{} hours", 3 - group), old_key],
                )
                .unwrap();
            let group_hash: String = store
                .db
                .query_row(
                    "SELECT content_hash FROM entries WHERE cache_key = ?1",
                    params![old_key],
                    |row| row.get(0),
                )
                .unwrap();
            store
                .put(
                    &new_key,
                    "mycrate",
                    &["lib".to_string()],
                    &[],
                    "x86_64-unknown-linux-gnu",
                    "dev",
                    &[(new_file, "lib.rlib".to_string())],
                    "",
                    "",
                )
                .unwrap();
            store
                .db
                .execute(
                    "UPDATE entries SET content_hash = ?1 WHERE cache_key = ?2",
                    params![group_hash, new_key],
                )
                .unwrap();
        }

        assert_eq!(store.physical_size().unwrap(), 600);
        let stats = store.evict_duplicate_entries().unwrap();
        assert_eq!(stats.entries_evicted, 2);
        assert_eq!(stats.bytes_freed, 200);
        assert_eq!(store.physical_size().unwrap(), 400);
        assert!(!store.contains("budget_old_0"));
        assert!(!store.contains("budget_old_1"));
        assert!(
            store.contains("budget_old_2"),
            "bounded duplicate GC must retain the least-stale eligible victim"
        );
    }

    #[test]
    fn evict_duplicate_entries_skips_victim_with_corrupt_meta() {
        // Covers evict_duplicate_entries remove_entry_guarded error branch.
        let tmp = tempfile::tempdir().unwrap();
        let mut config = test_config(tmp.path());
        config.max_size = 1;
        let store = Store::open(&config).unwrap();

        let dir = tmp.path().join("src");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("lib.rlib");
        rewrite_source(&file, b"same-content-for-corrupt-dedup");
        store
            .put(
                "dup_corrupt_old",
                "mycrate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(file.clone(), "lib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour') \
                 WHERE cache_key = 'dup_corrupt_old'",
                [],
            )
            .unwrap();

        let old_content_hash: String = store
            .db
            .query_row(
                "SELECT content_hash FROM entries WHERE cache_key = 'dup_corrupt_old'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        // Give the newer entry its own blob, then place both keys in the same
        // duplicate group. The older victim now has proven positive marginal
        // bytes, so fail-closed filtering does not make this error-path test
        // vacuous.
        rewrite_source(&file, b"different-content-for-corrupt-dedup");
        store
            .put(
                "dup_corrupt_new",
                "mycrate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(file, "lib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        store
            .db
            .execute(
                "UPDATE entries SET content_hash = ?1 WHERE cache_key = 'dup_corrupt_new'",
                params![old_content_hash],
            )
            .unwrap();
        std::fs::write(
            store.entry_dir("dup_corrupt_old").join("meta.json"),
            b"{not json",
        )
        .unwrap();

        let stats = store.evict_duplicate_entries().unwrap();

        assert_eq!(stats.entries_evicted, 0, "corrupt victim is skipped");
        assert!(store.contains("dup_corrupt_old"));
        assert!(store.contains("dup_corrupt_new"));
    }

    #[test]
    fn test_backfill_content_hashes() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(tmp.path());
        let store = Store::open(&config).unwrap();

        let dir = tmp.path().join("src");
        std::fs::create_dir_all(&dir).unwrap();
        let file1 = dir.join("lib.rlib");
        std::fs::write(&file1, b"backfill-content").unwrap();

        store
            .put(
                "bf_key_1",
                "mycrate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(file1, "lib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        // Simulate a legacy entry by clearing the content_hash
        store
            .db
            .execute(
                "UPDATE entries SET content_hash = NULL WHERE cache_key = 'bf_key_1'",
                [],
            )
            .unwrap();

        let backfilled = store.backfill_content_hashes().unwrap();
        assert_eq!(backfilled, 1);

        let ch: String = store
            .db
            .query_row(
                "SELECT content_hash FROM entries WHERE cache_key = 'bf_key_1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ch.len(), 64);
    }

    #[test]
    fn test_content_hash_column_exists() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(tmp.path());
        let store = Store::open(&config).unwrap();
        let result: Result<Option<String>, _> =
            store
                .db
                .query_row("SELECT content_hash FROM entries LIMIT 1", [], |row| {
                    row.get(0)
                });
        // Query should succeed (column exists), just no rows
        assert!(result.is_ok() || result.unwrap_err().to_string().contains("no rows"));
    }

    #[test]
    fn test_content_hash_full_dedup_lifecycle() {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = test_config(tmp.path());
        config.max_size = 1;
        let store = Store::open(&config).unwrap();

        let dir = tmp.path().join("src");
        std::fs::create_dir_all(&dir).unwrap();

        // Create 3 entries: 2 with identical content, 1 different
        let file_a = dir.join("a.rlib");
        std::fs::write(&file_a, b"shared-content").unwrap();
        let file_b = dir.join("b.rlib");
        std::fs::write(&file_b, b"different-content").unwrap();

        store
            .put(
                "ch_lc_1",
                "mycrate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(file_a.clone(), "a.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        // Age the first entry's access time (LRU policy)
        store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour') WHERE cache_key = 'ch_lc_1'",
                [],
            )
            .unwrap();

        store
            .put(
                "ch_lc_2",
                "mycrate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(file_a, "a.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        store
            .put(
                "ch_lc_3",
                "othercrate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(file_b, "b.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        // Verify content hashes
        let entries = store.list_entries("name").unwrap();
        assert_eq!(entries.len(), 3);

        let ch1 = entries
            .iter()
            .find(|e| e.cache_key == "ch_lc_1")
            .unwrap()
            .content_hash
            .as_ref()
            .unwrap();
        let ch2 = entries
            .iter()
            .find(|e| e.cache_key == "ch_lc_2")
            .unwrap()
            .content_hash
            .as_ref()
            .unwrap();
        let ch3 = entries
            .iter()
            .find(|e| e.cache_key == "ch_lc_3")
            .unwrap()
            .content_hash
            .as_ref()
            .unwrap();
        assert_eq!(ch1, ch2, "identical content should have same hash");
        assert_ne!(ch1, ch3, "different content should have different hash");

        // The shared duplicate frees no bytes, so both keys survive.
        let stats = store.evict_duplicate_entries().unwrap();
        assert_eq!(stats.entries_evicted, 0);
        assert_eq!(store.entry_count().unwrap(), 3);
        assert!(store.contains("ch_lc_1"));
        assert!(store.contains("ch_lc_2"));
        assert!(store.contains("ch_lc_3"));
    }
}
