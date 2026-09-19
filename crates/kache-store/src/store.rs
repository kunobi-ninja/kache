use crate::ArtifactPolicy;
use crate::blob_validation::validate_blob_metadata;
use anyhow::{Context, Result};
pub use kache_format::{CachedFile, EntryMeta};
use rusqlite::{Connection, Error as SqlError, ErrorCode, OptionalExtension, params};
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
pub struct VerifiedRestoredEntry {
    pub cache_key: String,
    pub meta: EntryMeta,
}

impl StorePutResult {
    pub fn is_full_dup(self) -> bool {
        self.output_blobs > 0 && self.duplicate_blobs == self.output_blobs
    }
}

thread_local! {
    /// `[cache] deferred_durability` of the store last opened on this thread;
    /// see [`Store::open`].
    static DEFERRED_DURABILITY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Whether a blob written now must be fsynced before it is published.
fn durable_writes_now() -> bool {
    !DEFERRED_DURABILITY.with(|deferred| deferred.get())
}

/// Mark a blob read-only so accidental writes can't corrupt the shared,
/// content-addressed copy. Best-effort.
fn set_blob_readonly(blob: &Path) {
    let _ = set_blob_readonly_checked(blob);
}

/// Mark a blob read-only, reporting failure. The hardlink ingest path needs
/// the result: there the guard is a correctness requirement (the blob shares
/// an inode with the build's own output), not a courtesy.
/// Flush a published blob to disk. Published blobs are read-only, and
/// Windows needs write access to flush a handle, so there the read-only
/// attribute comes off for the flush and goes straight back on.
fn fsync_published_blob(blob: &Path) -> std::io::Result<()> {
    #[cfg(not(windows))]
    {
        crate::atomic::fsync_file(blob)
    }
    #[cfg(windows)]
    {
        let meta = fs::metadata(blob)?;
        if !meta.permissions().readonly() {
            return crate::atomic::fsync_file(blob);
        }
        let mut writable = meta.permissions();
        writable.set_readonly(false);
        fs::set_permissions(blob, writable)?;
        let flushed = crate::atomic::fsync_file(blob);
        let _ = set_blob_readonly_checked(blob);
        flushed
    }
}

fn set_blob_readonly_checked(blob: &Path) -> std::io::Result<()> {
    let meta = fs::metadata(blob)?;
    let mut perms = meta.permissions();
    perms.set_readonly(true);
    fs::set_permissions(blob, perms)
}

#[cfg(all(test, unix))]
thread_local! {
    /// Test-only ingest override: reproduce, on any filesystem, what a Linux
    /// CoW reflink does to a staged snapshot — independent bytes at the
    /// *umask*, not at the source's mode.
    ///
    /// CI runs on ext4, where `try_reflink` always fails and the `fs::copy`
    /// fallback carries the permission bits over. That is the blind spot #822
    /// shipped through: a mode-losing ingest is simply unobservable there, so
    /// the regression only ever appeared on a developer's btrfs/ZFS box.
    /// Forcing the emulation makes the contract testable everywhere.
    ///
    /// Thread-local rather than a process-wide flag (`link.rs`'s
    /// `WINDOWS_HARDLINK_RESTORE` is the latter, but it is a real feature
    /// switch): `cargo test` runs store tests in parallel, and one test's
    /// emulation must not leak into another's put.
    ///
    /// Unix-only: the tests that construct [`ModeDroppingIngest`] are
    /// `#[cfg(unix)]`, and Windows test builds fail `-D dead-code` otherwise.
    static FORCE_MODE_DROPPING_INGEST: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

/// Enable [`FORCE_MODE_DROPPING_INGEST`] for the duration of the guard.
#[cfg(all(test, unix))]
struct ModeDroppingIngest;

#[cfg(all(test, unix))]
impl ModeDroppingIngest {
    fn enable() -> Self {
        FORCE_MODE_DROPPING_INGEST.with(|forced| forced.set(true));
        Self
    }
}

#[cfg(all(test, unix))]
impl Drop for ModeDroppingIngest {
    fn drop(&mut self) {
        FORCE_MODE_DROPPING_INGEST.with(|forced| forced.set(false));
    }
}

/// Stage `source` at `tmp` the way a Linux CoW reflink would, reporting whether
/// the emulation was active at all. See [`FORCE_MODE_DROPPING_INGEST`].
#[cfg(all(test, unix))]
fn emulate_cow_reflink_ingest(source: &Path, tmp: &Path) -> Result<bool> {
    if !FORCE_MODE_DROPPING_INGEST.with(std::cell::Cell::get) {
        return Ok(false);
    }
    fs::copy(source, tmp)
        .with_context(|| format!("emulating a reflink ingest of {}", source.display()))?;
    // `try_reflink` on Linux opens the destination with `File::create` before
    // the FICLONE ioctl, so the snapshot lands at `0o666 & !umask` whatever the
    // source was. The exact value varies with the umask; the only property that
    // matters here is that it carries no `+x`.
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(tmp, fs::Permissions::from_mode(0o644))
        .with_context(|| format!("resetting the emulated staging mode on {}", tmp.display()))?;
    Ok(true)
}

#[cfg(not(all(test, unix)))]
#[inline(always)]
fn emulate_cow_reflink_ingest(_source: &Path, _tmp: &Path) -> Result<bool> {
    Ok(false)
}

// ── Hardlink-fallback reason seams (#835) ─────────────────────────────────────
//
// Same pattern as [`FORCE_MODE_DROPPING_INGEST`]: thread-local so parallel
// `cargo test` workers do not leak into each other. Production stubs are
// `None`/`false` so the real `link(2)` runs.

#[cfg(test)]
thread_local! {
    /// When set, the ingest `hard_link` attempt fails with this errno instead
    /// of calling `link(2)`, so the reason counter is observable without bind
    /// mounts.
    static INJECT_STORE_HARDLINK_ERROR: std::cell::Cell<Option<std::io::ErrorKind>> =
        const { std::cell::Cell::new(None) };
    /// When set, the ingest skips the `try_reflink` attempt (pretends CoW is
    /// unavailable) so the hardlink path is exercised even on APFS/btrfs.
    static FORCE_STORE_HARDLINK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Enable [`INJECT_STORE_HARDLINK_ERROR`] for the guard's lifetime.
#[cfg(test)]
pub(crate) struct InjectStoreHardlinkError {
    _private: (),
}

#[cfg(test)]
impl InjectStoreHardlinkError {
    pub(crate) fn enable(kind: std::io::ErrorKind) -> Self {
        INJECT_STORE_HARDLINK_ERROR.with(|slot| slot.set(Some(kind)));
        Self { _private: () }
    }
}

#[cfg(test)]
impl Drop for InjectStoreHardlinkError {
    fn drop(&mut self) {
        INJECT_STORE_HARDLINK_ERROR.with(|slot| slot.set(None));
    }
}

/// Enable [`FORCE_STORE_HARDLINK`] for the guard's lifetime: skip reflink so a
/// same-device `.rlib` put must hardlink, even on CoW filesystems.
#[cfg(test)]
pub(crate) struct ForceStoreHardlink {
    _private: (),
}

#[cfg(test)]
impl ForceStoreHardlink {
    pub(crate) fn enable() -> Self {
        FORCE_STORE_HARDLINK.with(|slot| slot.set(true));
        Self { _private: () }
    }
}

#[cfg(test)]
impl Drop for ForceStoreHardlink {
    fn drop(&mut self) {
        FORCE_STORE_HARDLINK.with(|slot| slot.set(false));
    }
}

#[cfg(test)]
fn injected_store_hardlink_error() -> Option<std::io::ErrorKind> {
    INJECT_STORE_HARDLINK_ERROR.with(|slot| slot.get())
}

#[cfg(not(test))]
#[inline(always)]
fn injected_store_hardlink_error() -> Option<std::io::ErrorKind> {
    None
}

fn force_store_hardlink() -> bool {
    #[cfg(test)]
    {
        FORCE_STORE_HARDLINK.with(|slot| slot.get())
    }
    #[cfg(not(test))]
    {
        false
    }
}

fn should_try_store_reflink(force_hardlink: bool) -> bool {
    !force_hardlink
}

fn allow_store_hardlink(allow_hardlink: bool, is_regular_file: bool) -> bool {
    allow_hardlink && is_regular_file
}

/// Attempt the ingest `link(2)`, honouring the test-only error seam. Returns
/// the io error on failure so callers classify it instead of swallowing it.
fn try_store_hard_link(source: &Path, tmp: &Path) -> std::io::Result<()> {
    if let Some(kind) = injected_store_hardlink_error() {
        return Err(std::io::Error::new(
            kind,
            "injected ingest hardlink failure",
        ));
    }
    fs::hard_link(source, tmp)
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

fn hardlink_eligible<P: ArtifactPolicy>(store_name: &str, executable: bool) -> bool {
    if executable {
        return false;
    }
    #[cfg(windows)]
    if !crate::link::windows_hardlink_enabled() {
        return false;
    }
    P::allow_hardlink(store_name)
}

fn source_hardlink_allowed<P: ArtifactPolicy>(
    allow_source_hardlinks: bool,
    store_name: &str,
    executable: bool,
) -> bool {
    allow_source_hardlinks && hardlink_eligible::<P>(store_name, executable)
}

/// How a new blob was staged into the store before publish. Counters are
/// recorded only when this call actually publishes (`atomic_write_and_replace`
/// returns `true`); a concurrent winner already accounted for their ingest,
/// and counting a discarded temp would over-claim zero-copy sharing.
#[derive(Clone, Copy, Debug)]
enum StoreIngest {
    Reflink,
    Hardlink,
    Copy(StoreCopyReason),
}

/// Why an ingest fell back to a copy (#835). `Ineligible` is a policy refusal
/// (kind-ineligible or cc never shares inodes — `allow_hardlink` false, or a
/// symlink source); the rest classify the `link(2)` errno. Recorded alongside
/// `store_copied_bytes` on publish so the report can show *why* zero-copy did
/// not happen. Observability only: the reason never changes what gets linked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StoreCopyReason {
    Ineligible,
    CrossDevice,
    Permission,
    Other,
}

impl StoreCopyReason {
    fn from_io_kind(kind: std::io::ErrorKind) -> Self {
        if kind == std::io::ErrorKind::CrossesDevices {
            StoreCopyReason::CrossDevice
        } else if kind == std::io::ErrorKind::PermissionDenied {
            StoreCopyReason::Permission
        } else {
            StoreCopyReason::Other
        }
    }
}

/// Record the ingest copy-fallback reason alongside `store_copied_bytes`.
/// Pure dispatch so each arm is independently testable (mutant discipline:
// one test per arm, no skip annotations).
fn record_store_copy_reason(reason: StoreCopyReason, bytes: u64) {
    if reason == StoreCopyReason::CrossDevice {
        crate::opcounts::record_store_copy_cross_device(bytes);
    } else if reason == StoreCopyReason::Permission {
        crate::opcounts::record_store_copy_permission(bytes);
    } else if reason == StoreCopyReason::Ineligible {
        crate::opcounts::record_store_copy_ineligible(bytes);
    } else {
        crate::opcounts::record_store_copy_other(bytes);
    }
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
    let durable = durable_writes_now();
    fs::create_dir_all(blob.parent().unwrap()).context("creating blob shard directory")?;
    let bytes = fs::metadata(source).map(|m| m.len()).unwrap_or(0);
    let ingest = std::cell::Cell::new(StoreIngest::Copy(StoreCopyReason::Other));
    let ro_failed = std::cell::Cell::new(false);

    // CoW reflink first; then a hardlink where the artifact kind allows sharing
    // an inode; only then a real copy. The hardlink is refused for a symlink
    // source: hashing followed the link, but `hard_link` would link the symlink
    // itself, and a blob must never be a pointer into mutable external state.
    //
    // Hardlink RO is applied in `after_fsync` (not in the write step): Windows
    // needs a writable handle to flush (#196). On RO failure we demote to a
    // full copy rather than publishing a writable shared inode.
    //
    // The hardlink error is captured, not swallowed with `.is_ok()`: EXDEV
    // across bind mounts, EPERM, and other errnos are classified into
    // `StoreCopyReason` so the report can show *why* zero-copy did not happen.
    // What gets linked is unchanged — a failure still falls back to a copy.
    let published = match crate::atomic::atomic_write_and_replace_deferrable(
        blob,
        true,
        |tmp| {
            // `FORCE_STORE_HARDLINK` (test-only) skips the reflink attempt so a
            // same-device `.rlib` must hardlink even on CoW filesystems.
            let reflink_ok = should_try_store_reflink(force_store_hardlink())
                && crate::link::try_reflink(source, tmp).is_ok();
            if reflink_ok {
                ingest.set(StoreIngest::Reflink);
            } else if allow_store_hardlink(
                allow_hardlink,
                fs::symlink_metadata(source).is_ok_and(|m| m.file_type().is_file()),
            ) {
                match try_store_hard_link(source, tmp) {
                    Ok(()) => {
                        ingest.set(StoreIngest::Hardlink);
                    }
                    Err(io_err) => {
                        let reason = StoreCopyReason::from_io_kind(io_err.kind());
                        {
                            let io_reason = match reason {
                                StoreCopyReason::CrossDevice => {
                                    crate::link::HardlinkIoReason::CrossDevice
                                }
                                StoreCopyReason::Permission => {
                                    crate::link::HardlinkIoReason::Permission
                                }
                                StoreCopyReason::Other | StoreCopyReason::Ineligible => {
                                    crate::link::HardlinkIoReason::Other
                                }
                            };
                            crate::link::warn_hardlink_fallback_once(
                                source, blob, io_reason, &io_err,
                            );
                        }
                        fs::copy(source, tmp).with_context(|| {
                            format!("copying {} to blob store", source.display())
                        })?;
                        ingest.set(StoreIngest::Copy(reason));
                    }
                }
            } else {
                fs::copy(source, tmp)
                    .with_context(|| format!("copying {} to blob store", source.display()))?;
                ingest.set(StoreIngest::Copy(StoreCopyReason::Ineligible));
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
        durable,
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
            StoreIngest::Copy(reason) => {
                crate::opcounts::record_store_copied(bytes);
                record_store_copy_reason(reason, bytes);
            }
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

/// How long a key lock file must sit unused before the sweep may unlink it.
/// Every acquisition rewrites the file, so its mtime is the last claim of the
/// key. Holding a key lock spans one compile and `BUILD_LOCK_TIMEOUT` is ten
/// minutes; an hour, the staging and orphan-blob grace, leaves any claim that
/// is still in flight far behind, and a key untouched that long is not being
/// contended. The sweep also takes the lock before unlinking, so the grace
/// only decides how eagerly idle files go, never whether a holder is safe.
pub const KEY_LOCK_SWEEP_GRACE: Duration = Duration::from_secs(3600);

/// Most key lock files one sweep unlinks. Each costs an open, a lock, two
/// stats and an unlink under `gc.lock`, tens of microseconds on Linux and a
/// few hundred on macOS and Windows, so a full batch stays within seconds.
/// A store with 84k stale locks converges in five sweeps.
pub const KEY_LOCK_SWEEP_CAP: usize = 20_000;

/// What one [`Store::sweep_stale_key_locks`] pass saw and did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct KeyLockSweepStats {
    /// `<key>.lock` names found in `store/`.
    pub seen: usize,
    /// Lock files unlinked.
    pub removed: usize,
}

impl KeyLockSweepStats {
    /// Key lock files left in `store/` after the pass.
    pub fn remaining(&self) -> usize {
        self.seen.saturating_sub(self.removed)
    }
}

/// The cache key a name in `store/` locks, or `None` for anything that is
/// not `<valid key>.lock`: `gc.lock`, `durability.lock`, entry directories.
fn key_of_lock_name(name: &str) -> Option<&str> {
    name.strip_suffix(".lock")
        .filter(|key| kache_format::is_valid_cache_key(key))
}

/// Old enough to unlink? A modification time ahead of `now` reads as young.
fn key_lock_is_stale(
    modified: std::time::SystemTime,
    now: std::time::SystemTime,
    min_age: Duration,
) -> bool {
    now.duration_since(modified).is_ok_and(|age| age >= min_age)
}

/// Unlink one key lock file if it is a regular file, stale, and not held.
/// True when the file is gone. Any failure leaves it for a later sweep; on
/// Windows that includes an unlink refused because another handle is open
/// without delete sharing.
///
/// Staleness is judged twice. The listing's mtime keeps the sweep from ever
/// locking a file in recent use, which a claimant would read as contention.
/// The locked handle's mtime catches a claim that came and went in between.
/// `after_open` runs between the open and the lock so tests can stage both
/// that and a path replaced under the handle.
fn remove_stale_lock_file(
    path: &Path,
    min_age: Duration,
    now: std::time::SystemTime,
    after_open: impl FnOnce(),
) -> bool {
    // A directory or symlink with a lock-shaped name is not ours to remove.
    let listed_stale = fs::symlink_metadata(path)
        .is_ok_and(|meta| meta.is_file() && metadata_is_stale(&meta, now, min_age));
    if !listed_stale {
        return false;
    }
    // No `create`: a name that vanished since the listing stays gone.
    let Ok(file) = fs::OpenOptions::new().read(true).write(true).open(path) else {
        return false;
    };
    after_open();
    if !matches!(StoreLock::try_lock_file(&file), Ok(true)) {
        return false;
    }
    let locked_stale = file
        .metadata()
        .is_ok_and(|meta| metadata_is_stale(&meta, now, min_age));
    // Dropping `file` on any return releases the lock.
    locked_stale && lock_file_is_at_path(&file, path) && fs::remove_file(path).is_ok()
}

fn metadata_is_stale(meta: &fs::Metadata, now: std::time::SystemTime, min_age: Duration) -> bool {
    meta.modified()
        .is_ok_and(|modified| key_lock_is_stale(modified, now, min_age))
}

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

/// What occupies a blob's content-addressed path right after a publish
/// rename onto it failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishDest {
    /// A regular file: a concurrent publisher won. Same digest, same bytes.
    File,
    /// Something that is not a regular file (a directory): no race explains it.
    Obstructed,
    /// Nothing readable: absent, or a Windows delete-pending name.
    Vacant,
}

fn publish_dest_state(blob: &Path) -> PublishDest {
    match fs::metadata(blob) {
        Ok(meta) if meta.is_file() => PublishDest::File,
        Ok(_) => PublishDest::Obstructed,
        Err(_) => PublishDest::Vacant,
    }
}

/// How a publish rename settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishRename {
    /// This call put the blob in place.
    Published,
    /// A concurrent publisher's identical blob is in place.
    LostRace,
    /// Not in place, and no fault found: left to the put's locked phase.
    Deferred,
}

/// Settle one publish rename attempt (#1128).
///
/// A published blob is read-only, and on Windows a rename onto a read-only
/// file fails with ERROR_ACCESS_DENIED, the same code a delete-pending name
/// gives. The error alone cannot tell "a winner is there" from "a removed
/// blob is going away", so the destination decides: a winner settles the
/// publish at once, and only the other states are handed to the retry.
fn publish_attempt_outcome(
    renamed: std::io::Result<()>,
    dest_state: impl FnOnce() -> PublishDest,
) -> std::io::Result<PublishRename> {
    match renamed {
        Ok(()) => Ok(PublishRename::Published),
        Err(_) if dest_state() == PublishDest::File => Ok(PublishRename::LostRace),
        Err(e) => Err(e),
    }
}

/// Settle a publish whose rename kept failing.
///
/// A transient error with the name vacant means the blob was removed after
/// the last attempt found it in the way. That is a race between two healthy
/// operations: the put's locked phase re-materializes the blob where no
/// remover can interleave. Everything else is a real failure.
fn publish_failure_outcome(
    err: std::io::Error,
    transient: bool,
    dest: PublishDest,
) -> Result<PublishRename> {
    match (dest, transient) {
        (PublishDest::File, _) => Ok(PublishRename::LostRace),
        (PublishDest::Vacant, true) => Ok(PublishRename::Deferred),
        _ => Err(err).context("publishing staged blob"),
    }
}

/// Rename a staged blob into place on the shared transient-retry budget,
/// re-reading the destination after every failure. The rename, the probe and
/// the classifier are passed in so each interleaving can be driven in a test;
/// production passes `fs::rename`, [`publish_dest_state`] and
/// `is_transient_rename_error`.
fn publish_rename(
    mut rename: impl FnMut() -> std::io::Result<()>,
    dest_state: impl Fn() -> PublishDest,
    is_transient: impl Fn(&std::io::Error) -> bool,
) -> Result<PublishRename> {
    crate::atomic::retry_transient(
        || publish_attempt_outcome(rename(), &dest_state),
        &is_transient,
    )
    .or_else(|err| {
        let transient = is_transient(&err);
        publish_failure_outcome(err, transient, dest_state())
    })
}

/// Whether `a` and `b` name the same inode (hardlinked). Used after a lost
/// hardlink publish race to decide if the build output still shares the
/// store blob (keep RO) or is an independent file we marked RO by mistake
/// (restore writable).
fn paths_share_inode(a: &Path, b: &Path) -> bool {
    match (kache_fs::file_identity(a), kache_fs::file_identity(b)) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
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

pub fn blob_path_in_store_dir(store_dir: &Path, hash: &str) -> PathBuf {
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
pub enum ProbeOutcome {
    /// Committed, blob-complete entry: safe to restore from this meta.
    Hit(Box<EntryMeta>),
    /// No committed entry for this key (authoritative miss).
    Miss,
    /// Not servable read-only; the wrapper must run today's local path.
    Fallback(&'static str),
}

/// Read-only equivalent of the lookup half of [`ArtifactStore::get`]: same
/// committed-row check, `meta.json` parse, legacy-layout detection, and
/// blob existence/size validation — but with every write side effect
/// (lazy migration, evict-and-miss, hit accounting) replaced by
/// [`ProbeOutcome::Fallback`]. Runs on a read-only connection so parallel
/// probes never contend on the daemon's store mutex (#565).
pub fn probe_entry_readonly(db: &Connection, store_dir: &Path, cache_key: &str) -> ProbeOutcome {
    let committed = db.query_row(
        "SELECT committed, durable FROM entries WHERE cache_key = ?1",
        params![cache_key],
        |row| Ok((row.get::<_, bool>(0)?, row.get::<_, bool>(1)?)),
    );
    match committed {
        Ok((true, true)) => {}
        // Stored without an fsync and not flushed yet: the writing path
        // verifies the bytes before serving it.
        Ok((true, false)) => return ProbeOutcome::Fallback("entry pending durability"),
        Ok((false, _)) => return ProbeOutcome::Miss,
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
        if validate_blob_metadata(&blob, cached_file.size).is_err() {
            return ProbeOutcome::Fallback("blob missing or size mismatch");
        }
    }

    ProbeOutcome::Hit(Box::new(meta))
}

/// Open the index database read-only for probe connections (#565). No schema
/// work, no WAL/synchronous pragma churn — `query_only` hard-refuses any
/// accidental write, and the busy timeout is half the daemon's 50 ms lookup
/// deadline so a contended probe still answers (`Fallback`) inside budget.
pub fn open_index_db_readonly(db_path: &Path) -> Result<Connection> {
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
/// 30-second timeout so readiness never gates on backupd.
///
/// Returns the background thread's handle so tests can join it; production
/// callers drop it (the thread never outlives its bounded wait by more than
/// the child kill).
#[cfg(target_os = "macos")]
pub fn exclude_from_indexing(dir: &Path) -> Option<std::thread::JoinHandle<()>> {
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

/// Statistics returned by GC operations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GcStats {
    pub entries_evicted: usize,
    /// Store-namespace bytes whose blob rows went away. Not filesystem
    /// reclamation — clones in `target/` can keep the blocks.
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
    /// Entries eviction selected but left in place because unlinking their
    /// last-ref blobs would not free disk (hardlink or CoW clone still live
    /// in a worktree). Distinct from [`Self::entries_pinned`].
    #[serde(default)]
    pub entries_unreclaimable: usize,
    /// Best-effort private bytes actually returned by unlinking store names.
    #[serde(default)]
    pub disk_bytes_reclaimed: u64,
    /// Entries eviction selected but failed to remove: an unreadable
    /// `meta.json` (#276), a SQLite error. Counted rather than only logged, so
    /// a sweep that keeps failing shows up in its own stats instead of only
    /// as warning lines nobody reads.
    #[serde(default)]
    pub entries_failed: usize,
    /// The part of [`Self::entries_failed`] that was SQLite write contention
    /// (`SQLITE_BUSY` / `SQLITE_LOCKED`): the sweep lost the write lock to live
    /// builds, as opposed to finding a damaged entry.
    #[serde(default)]
    pub entries_locked: usize,
    /// Failed evictions caused by upgrading a stale WAL read snapshot to a
    /// writer. These fail immediately and cannot be cured by busy_timeout.
    #[serde(default)]
    pub entries_busy_snapshot: usize,
    /// Recently accessed candidates skipped before loading metadata or
    /// starting a SQLite transaction. Included in entries_pinned.
    #[serde(default)]
    pub entries_recent_prefiltered: usize,
    /// Time spent in the eviction writes themselves, each entry's removal
    /// with its busy waits, summed over the run. Next to `entries_locked` it
    /// shows how much of a sweep went to waiting on builds for the index
    /// write lock.
    #[serde(default)]
    pub evict_write_ms: u64,
    /// What the run's housekeeping did. `None` for a run that did none, such
    /// as the eviction after an upload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub housekeeping: Option<HousekeepingStats>,
}

/// Counts from [`Store::sweep_housekeeping`], recorded with the GC run so
/// growth in either structure shows up without a shell on the host.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HousekeepingStats {
    pub key_locks_removed: usize,
    /// Key lock files left in `store/`: live entries, recent claims, and
    /// whatever the per-sweep cap deferred.
    pub key_locks_remaining: usize,
    pub predictions_pruned: usize,
}

/// Whether `err` carries SQLite write contention (`SQLITE_BUSY` or
/// `SQLITE_LOCKED`) anywhere in its cause chain. Bad data and I/O errors are
/// not contention; GC counts the two apart.
pub fn is_sqlite_contention(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<SqlError>(),
            Some(SqlError::SqliteFailure(code, _))
                if matches!(code.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
        )
    })
}

pub fn is_sqlite_busy_snapshot(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<SqlError>(),
            Some(SqlError::SqliteFailure(code, _))
                if code.extended_code == rusqlite::ffi::SQLITE_BUSY_SNAPSHOT
        )
    })
}

fn record_eviction_failure(stats: &mut GcStats, error: &anyhow::Error) {
    stats.entries_failed += 1;
    if is_sqlite_contention(error) {
        stats.entries_locked += 1;
        if is_sqlite_busy_snapshot(error) {
            stats.entries_busy_snapshot += 1;
        }
    }
}

/// Registered blob bytes and blob rows an entry removal released — blobs
/// whose last reference went away, not the entry's logical size
/// (kunobi-ninja/kache#608). Denominated in `blobs` TABLE bytes, the same
/// unit as [`ArtifactStore::physical_size`], so eviction's running budget stays
/// consistent with its trigger; the file unlink itself is best-effort
/// (Windows can defer it), so this is not a guarantee about the disk.
#[derive(Debug, Clone, Copy, Default)]
pub struct RemovalReclaim {
    pub freed_bytes: u64,
    pub blobs_unlinked: usize,
    pub disk_bytes_reclaimed: u64,
}

/// One pass of `remove_entry_guarded_with_hooks`: either a settled outcome,
/// or the instruction to run again because a republication replaced the
/// generation this pass was waiting on.
enum RemovalAttempt {
    Done(Option<RemovalReclaim>),
    Republished,
    /// Last-ref blobs are still cloned outside the store; eviction must not
    /// drop the entry (kunobi-ninja/kache#725).
    Unreclaimable,
}

/// Outcome of removing an entry while holding its compile lock.
#[derive(Debug)]
pub enum GuardedRemoval {
    Reclaimed(RemovalReclaim),
    Skipped,
    Unreclaimable,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TrackedTargetRoot {
    pub path: PathBuf,
    pub workspace_root: PathBuf,
    pub first_seen: i64,
    pub last_seen: i64,
    pub identity: crate::filesystem::PathIdentity,
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

/// Statistics returned by [`ArtifactStore::sweep_orphan_blobs`].
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
pub struct ArtifactStore<P: ArtifactPolicy> {
    policy: std::marker::PhantomData<P>,
    config: Config,
    db: Connection,
    /// Write slice and pause for eviction sweeps: [`EVICTION_WRITE_SLICE`]
    /// and [`EVICTION_WRITE_PAUSE`] outside tests.
    eviction_pacing: (Duration, Duration),
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
pub const EVICTION_IDLE_GRACE: Duration = Duration::from_secs(120);

/// A hit re-stamps `last_accessed` only when the previous stamp is at least
/// this old: well inside [`EVICTION_IDLE_GRACE`], so a restore in flight is
/// still pinned, and rare enough that concurrent hits stop contending for
/// the index's write lock.
pub const HIT_STAMP_INTERVAL: Duration = Duration::from_secs(30);

/// Write-lock time an eviction sweep may spend on removals before it pauses.
///
/// SQLite's write lock is not fair. A build's `put` that finds it taken
/// sleeps in the busy handler, polling at most every 100 ms, while a sweep
/// takes the lock again microseconds after each commit. Without pauses, a
/// build's `put` waited for most of the sweep.
pub const EVICTION_WRITE_SLICE: Duration = Duration::from_millis(50);

/// How long an eviction sweep stands off the write lock after each
/// [`EVICTION_WRITE_SLICE`], or after it lost the lock to another writer.
/// Longer than the busy handler's 100 ms poll interval, so every waiting
/// writer polls at least once while the lock is free.
pub const EVICTION_WRITE_PAUSE: Duration = Duration::from_millis(150);

/// Paces an eviction sweep's writes so build processes waiting on the index
/// write lock get it between slices.
#[derive(Debug)]
struct EvictionWritePacer {
    slice: Duration,
    pause: Duration,
    held: Duration,
}

impl EvictionWritePacer {
    fn new(slice: Duration, pause: Duration) -> Self {
        Self {
            slice,
            pause,
            held: Duration::ZERO,
        }
    }

    /// Record a removal that wrote to the index. Returns the pause to take
    /// once the sweep has spent a full slice writing.
    fn after_write(&mut self, took: Duration) -> Option<Duration> {
        self.held += took;
        if self.held < self.slice {
            return None;
        }
        self.held = Duration::ZERO;
        Some(self.pause)
    }

    /// Another writer holds the lock: stand off for a full pause.
    fn after_contention(&mut self) -> Duration {
        self.held = Duration::ZERO;
        self.pause
    }
}

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
pub const TOMBSTONE_RETENTION_DAYS: u64 = 14;

const BUILD_LOCK_TIMEOUT: Duration = Duration::from_secs(600);
const BUILD_LOCK_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// How long a waiter sleeps before its next `try_lock`: 1 ms, doubling up to
/// [`BUILD_LOCK_POLL_INTERVAL`]. A fixed 100 ms poll cost every waiter half
/// of that on each hand-off; in a six-job cold cell that was 350 s of sleep
/// per cell, more than the compiles being waited for.
pub fn lock_poll_interval(attempt: u32) -> Duration {
    Duration::from_millis(1u64 << attempt.min(7)).min(BUILD_LOCK_POLL_INTERVAL)
}

/// Cross-process advisory lock held through an open file handle.
///
/// Lock files persist after release. Unlinking an advisory lock file can split
/// contenders across the unlinked inode and a newly-created inode, allowing
/// two processes to both believe they hold the same lock. Only
/// [`Store::sweep_stale_key_locks`] unlinks one, and only while holding it;
/// every acquisition then checks that the path still names the file it
/// locked, and starts over when it does not.
pub struct StoreLock {
    file: fs::File,
}

/// Lock guard for a cache key. Dropping it releases the OS lock.
pub type KeyLock = StoreLock;

/// Lock guard for store-wide GC. Dropping it releases the OS lock.
pub type GcLock = StoreLock;

/// How often one acquisition reopens a lock file that was unlinked under it.
/// Each retry needs the sweep to unlink the same path again, and a file this
/// process just created is younger than [`KEY_LOCK_SWEEP_GRACE`], so the
/// second open already settles; the bound only keeps a broken filesystem
/// from spinning.
const LOCK_OPEN_ATTEMPTS: u32 = 4;

/// Does `path` still name the file `handle` refers to?
///
/// No: the path is gone or names another file, so the next contender will
/// lock something else and this handle excludes nobody. A handle with no
/// identity to compare (a platform without one) counts as current; the sweep
/// never unlinks there, see [`lock_file_is_at_path`].
fn lock_is_current(
    handle: std::io::Result<kache_fs::InodeId>,
    at_path: std::io::Result<kache_fs::InodeId>,
) -> bool {
    match (handle, at_path) {
        (Ok(handle), Ok(at_path)) => handle == at_path,
        (Ok(_), Err(_)) => false,
        (Err(_), _) => true,
    }
}

/// The sweep's stricter form: both identities known and equal.
fn lock_file_is_at_path(file: &fs::File, path: &Path) -> bool {
    match (
        kache_fs::handle_identity(file),
        kache_fs::file_identity(path),
    ) {
        (Ok(handle), Ok(at_path)) => handle == at_path,
        _ => false,
    }
}

impl StoreLock {
    fn open(path: &Path) -> Result<fs::File> {
        let parent = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("lock file has no parent: {}", path.display()))?;
        fs::create_dir_all(parent)?;
        Ok(fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?)
    }

    fn finish(mut file: fs::File) -> Result<Self> {
        // Diagnostic only: lock ownership is determined exclusively by the OS.
        use std::io::{Seek, SeekFrom, Write};
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        write!(file, "{}", std::process::id())?;
        Ok(Self { file })
    }

    /// Open, lock, then confirm the locked file is the one at `path`.
    ///
    /// `lock` returns false when the file is held elsewhere. `after_open`
    /// runs between the open and the lock, where an unlink by the sweep does
    /// its damage; production passes a no-op and tests stage the race there.
    fn acquire_current(
        path: &Path,
        mut lock: impl FnMut(&fs::File) -> Result<bool>,
        mut after_open: impl FnMut(),
    ) -> Result<Option<Self>> {
        for _ in 0..LOCK_OPEN_ATTEMPTS {
            let file = Self::open(path)?;
            after_open();
            if !lock(&file)? {
                return Ok(None);
            }
            if lock_is_current(
                kache_fs::handle_identity(&file),
                kache_fs::file_identity(path),
            ) {
                return Ok(Some(Self::finish(file)?));
            }
            // Closing the handle releases the lock on the unlinked file.
        }
        anyhow::bail!(
            "lock file {} was replaced {LOCK_OPEN_ATTEMPTS} times while acquiring it",
            path.display()
        )
    }

    fn try_lock_file(file: &fs::File) -> Result<bool> {
        match file.try_lock() {
            Ok(()) => Ok(true),
            Err(std::fs::TryLockError::WouldBlock) => Ok(false),
            Err(std::fs::TryLockError::Error(e)) => Err(e.into()),
        }
    }

    fn acquire(path: &Path) -> Result<Self> {
        let lock = Self::acquire_current(
            path,
            |file| {
                file.lock()?;
                Ok(true)
            },
            || {},
        )?;
        lock.ok_or_else(|| anyhow::anyhow!("blocking lock on {} reported busy", path.display()))
    }

    pub fn try_acquire(path: &Path) -> Result<Option<Self>> {
        Self::acquire_current(path, Self::try_lock_file, || {})
    }

    fn wait_until_available(path: &Path, timeout: Duration) -> Result<bool> {
        let start = std::time::Instant::now();
        let mut attempt = 0;
        loop {
            if let Some(lock) = Self::try_acquire(path)? {
                drop(lock);
                return Ok(true);
            }
            if start.elapsed() >= timeout {
                return Ok(false);
            }
            std::thread::sleep(
                lock_poll_interval(attempt).min(timeout.saturating_sub(start.elapsed())),
            );
            attempt += 1;
        }
    }
}

impl Drop for StoreLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
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

/// How aggressively a local cache hit re-hashes its blobs against their content
/// address before serving them, to catch silent on-disk corruption / bit rot /
/// a memo collision before it reaches the compiler as a wrong artifact
/// (kunobi-ninja/kache#332).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyRestores {
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
pub fn verify_restores_mode() -> VerifyRestores {
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

fn zero_byte_is_valid_output<P: ArtifactPolicy>(store_name: &str, crate_types: &[String]) -> bool {
    P::allow_empty(store_name, crate_types)
}

/// Length-prefix a field before folding it into a hasher, so adjacent fields
/// cannot be transposed without changing the digest.
fn fold_field(h: &mut blake3::Hasher, bytes: &[u8]) {
    h.update(&(bytes.len() as u64).to_le_bytes());
    h.update(bytes);
}

fn emit_kinds_for_files<P: ArtifactPolicy>(files: &[CachedFile]) -> Vec<String> {
    let mut kinds: Vec<String> = files
        .iter()
        .filter_map(|f| P::emit_kind(&f.name))
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

    // Every statement below is a no-op on a current index, yet each one still
    // opens a write transaction, so every wrapper process queued behind
    // whichever miss was storing (hundreds of milliseconds per hit in a
    // contended cell). The generation stamped after the DDL says the schema
    // is current; bump [`INDEX_SCHEMA_GENERATION`] whenever a statement is
    // added or changed below.
    let generation: i64 = db.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if generation == INDEX_SCHEMA_GENERATION {
        return Ok(());
    }

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
    // Whether the entry's blobs and metadata were fsynced (deferred
    // durability). Rows from before the column were always flushed on put.
    let _ = db.execute_batch("ALTER TABLE entries ADD COLUMN durable INTEGER NOT NULL DEFAULT 1");
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
    // Answers "has this store ever held this unit" in one probe; a cold
    // compile uses it to skip the dep-info pre-pass (see
    // `FileHashCache::has_entry_for_unit`). `unit_id` is Cargo's `-C metadata`
    // hash, recorded after the put by the rustc wrapper; a row without one
    // stands for every unit of its crate name.
    let _ = db.execute_batch("ALTER TABLE entries ADD COLUMN unit_id TEXT NOT NULL DEFAULT ''");
    db.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_entries_crate_name ON entries(crate_name);
         CREATE INDEX IF NOT EXISTS idx_entries_crate_unit ON entries(crate_name, unit_id);",
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

    // Machine-local build-output provenance (kunobi-ninja/kache#725). These
    // absolute paths stay in the local SQLite index and are never exported to
    // remote manifests or artifact metadata.
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS target_roots (
            path           TEXT PRIMARY KEY,
            workspace_root TEXT NOT NULL,
            first_seen     INTEGER NOT NULL DEFAULT (unixepoch()),
            last_seen      INTEGER NOT NULL DEFAULT (unixepoch()),
            device         TEXT NOT NULL,
            inode          TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_target_roots_last_seen
            ON target_roots(last_seen);",
    )?;

    crate::file_hash::ensure_file_hash_cache_schema(db)?;
    db.pragma_update(None, "user_version", INDEX_SCHEMA_GENERATION)?;

    Ok(())
}

/// The `user_version` an index carries once every statement of
/// [`initialize_db`] has run. Bump it with any schema change.
///
/// 3: the boolean env-use memo became the versioned `source_env_dep_uses`.
/// 4: `idx_entries_crate_name`, the crate-presence probe behind deferred
///    discovery (#1117 added the index without bumping the generation, so an
///    index from before it never gained the index and the probe scanned).
/// 5: `entries.unit_id` and `idx_entries_crate_unit`, so the probe can tell
///    two units of one crate name apart (every build script is one name).
const INDEX_SCHEMA_GENERATION: i64 = 5;

/// Raise the refcount of every blob `cache_key` maps to at least the
/// references all mappings hold on it. Run before giving this key's
/// references back.
///
/// A mapping can exist for references nobody counted (an older backfill
/// mapped legacy entries before their migration). Subtracting such a mapping
/// from a count that only covers the other owners would take one of theirs,
/// and the blob would be reclaimed while they still need it. Flooring first
/// can only retain too much, which the blob-index reconcile corrects.
fn floor_blob_refs_at_mappings(
    conn: &rusqlite::Connection,
    cache_key: &str,
) -> rusqlite::Result<usize> {
    conn.execute(
        "UPDATE blobs
         SET refcount = MAX(refcount, (
             SELECT SUM(refs) FROM entry_blobs WHERE hash = blobs.hash
         ))
         WHERE hash IN (
             SELECT hash FROM entry_blobs WHERE cache_key = ?1
         )",
        params![cache_key],
    )
}

/// Give back every blob reference `cache_key`'s current mapping holds, then
/// drop the mapping. Blob rows released to zero are deleted so they do not
/// read as index drift; their files stay for the orphan sweep, or for the
/// caller's own re-reference. The floor below already keeps the subtraction
/// at or above zero; `MAX(0, ..)` stays as a second guard.
///
/// The mapping is the only record of what a generation holds once its
/// meta.json has been replaced, so dropping it without this release strands
/// the refcounts: no entry owns them and no eviction can reach them.
///
/// Call it before taking the new generation's references. A mapping can
/// name a blob that has no row (nothing was ever counted for it); released
/// afterwards, that mapping would consume the reference just taken and
/// delete the new generation's row.
fn release_entry_blob_refs(conn: &rusqlite::Connection, cache_key: &str) -> rusqlite::Result<()> {
    floor_blob_refs_at_mappings(conn, cache_key)?;
    conn.execute(
        "UPDATE blobs
         SET refcount = MAX(0, refcount - COALESCE((
             SELECT refs FROM entry_blobs
             WHERE cache_key = ?1 AND hash = blobs.hash
         ), 0))
         WHERE hash IN (
             SELECT hash FROM entry_blobs WHERE cache_key = ?1
         )",
        params![cache_key],
    )?;
    conn.execute(
        "DELETE FROM blobs
         WHERE refcount <= 0 AND hash IN (
             SELECT hash FROM entry_blobs WHERE cache_key = ?1
         )",
        params![cache_key],
    )?;
    conn.execute(
        "DELETE FROM entry_blobs WHERE cache_key = ?1",
        params![cache_key],
    )?;
    Ok(())
}

/// Whether any file `meta` lists still sits in the entry directory rather
/// than in the blob store.
fn has_unmigrated_artifacts(entry_dir: &Path, meta: &EntryMeta) -> bool {
    meta.files.iter().any(|f| entry_dir.join(&f.name).exists())
}

/// Replace `cache_key`'s rows in `entry_blobs` with one row per unique hash
/// in `files`, `refs` counting per-file references (kunobi-ninja/kache#608).
/// Must run inside the caller's registration transaction so the mapping
/// commits atomically with the entry row and the blob refcounts it mirrors.
///
/// A publisher that may be replacing a generation calls
/// [`release_entry_blob_refs`] first, before it takes its own references.
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

pub fn open_index_db(db_path: &Path) -> Result<Connection> {
    open_index_db_reporting_recovery(db_path).map(|(db, _)| db)
}

/// Like [`open_index_db`], but also reports whether the index had to be
/// recreated from scratch.
///
/// [`ArtifactStore::open`] needs to know: a freshly quarantined index has no rows, while
/// the blobs and every entry's `meta.json` are still on disk, so it can rebuild
/// the rows instead of silently presenting a cold cache (#415). Callers that
/// only need a connection use the wrapper above.
pub fn open_index_db_reporting_recovery(db_path: &Path) -> Result<(Connection, bool)> {
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
/// Returns `(connection, recovered)`. `recovered == true` tells [`ArtifactStore::open`]
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

/// Outcome of [`ArtifactStore::rebuild_index_from_store`].
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

impl<P: ArtifactPolicy> ArtifactStore<P> {
    pub fn open(config: impl Into<Config>) -> Result<Self> {
        let config = config.into();
        fs::create_dir_all(&config.cache_dir)
            .with_context(|| format!("creating cache directory {}", config.cache_dir.display()))?;
        let store_dir = config.store_dir();
        fs::create_dir_all(&store_dir)
            .with_context(|| format!("creating store directory {}", store_dir.display()))?;

        let db_path = config.index_db_path();
        let (db, recovered) = open_index_db_reporting_recovery(&db_path)
            .with_context(|| format!("opening index database {}", db_path.display()))?;

        // The blob ingest helpers are free functions; they read the flush
        // policy of the store last opened on this thread. A thread that never
        // opened a store flushes inline.
        DEFERRED_DURABILITY.with(|deferred| deferred.set(config.deferred_durability));
        let store = Self {
            policy: std::marker::PhantomData,
            config: config.clone(),
            db,
            eviction_pacing: (EVICTION_WRITE_SLICE, EVICTION_WRITE_PAUSE),
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

    /// Persistent-cache lookup for one file's content hash — DB read only, no
    /// blake3. Lets the daemon's `HashFiles` path release the store lock before
    /// the expensive file read (#281). See [`crate::file_hash::FileHashLookup`].
    pub fn file_hash_lookup(&self, path: &Path) -> crate::file_hash::FileHashLookup {
        self.file_hash_cache().lookup_cached(path)
    }

    /// Record a freshly-computed file content hash (the miss arm of
    /// [`Self::file_hash_lookup`]); best-effort.
    pub fn file_hash_record(&self, fingerprint: &crate::file_hash::FileFingerprint, hash: &str) {
        self.file_hash_cache().record_cached(fingerprint, hash);
    }

    /// Associate an already-known content hash with the exact fingerprint the
    /// caller observed it at, avoiding a redundant read when the file becomes a
    /// compiler input (kunobi-ninja/kache#540). Unlike
    /// [`Self::record_known_file_hash`] this never re-stats the path, so a file
    /// overwritten between observation and this call cannot inherit the old
    /// content's hash.
    pub fn record_verified_file_hash(
        &self,
        fingerprint: &crate::file_hash::FileFingerprint,
        hash: &str,
    ) {
        self.file_hash_cache().record_verified(fingerprint, hash);
    }

    /// [`Self::record_verified_file_hash`] for every restored file of one
    /// hit, in one transaction with a short wait: the rows only save a later
    /// hash, so a busy index (a miss's store transaction elsewhere) drops them
    /// instead of stalling the hit.
    pub fn record_verified_file_hashes(
        &self,
        restored: &[(crate::file_hash::FileFingerprint, &str)],
    ) {
        if restored.is_empty() {
            return;
        }
        let _ = self.db.busy_timeout(std::time::Duration::from_millis(100));
        let written = (|| -> rusqlite::Result<()> {
            self.db.execute_batch("BEGIN IMMEDIATE")?;
            for (fingerprint, hash) in restored {
                self.file_hash_cache().record_verified(fingerprint, hash);
            }
            self.db.execute_batch("COMMIT")
        })();
        let _ = self.db.busy_timeout(std::time::Duration::from_millis(5000));
        if let Err(error) = written {
            let _ = self.db.execute_batch("ROLLBACK");
            tracing::debug!("restored file hashes not memoised (index busy): {error}");
        }
    }

    /// Associate a stable file with its already-known content hash, avoiding a
    /// redundant read when it becomes a compiler input. Call this only after
    /// every store-side operation that may change the file's fingerprint and
    /// while the compiler-owned output is stable.
    pub fn record_known_file_hash(&self, path: &Path, hash: &str) {
        if let crate::file_hash::FileHashLookup::NeedsHash(fingerprint) =
            self.file_hash_cache().lookup_cached(path)
        {
            self.file_hash_cache().record_cached(&fingerprint, hash);
        }
    }

    /// Borrow the persistent memo without exposing the index connection.
    pub fn file_hash_cache(&self) -> crate::file_hash::FileHashCache<'_> {
        crate::file_hash::FileHashCache::Borrowed(&self.db)
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
        // An entry stored without an fsync (deferred durability) is verified
        // byte for byte until the flush lands: a crash between the two could
        // have left a blob whose size is right and whose bytes are not.
        let pending_durability = !self.entry_is_durable(cache_key);
        let verify_content =
            pending_durability || should_verify_this_restore(verify_restores_mode());

        // Verify all cached blobs still exist on disk and match expected size
        for cached_file in &meta.files {
            let blob = self.blob_path(&cached_file.hash);
            if let Err(error) = validate_blob_metadata(&blob, cached_file.size) {
                tracing::warn!(
                    "cache entry {} file {} has invalid blob metadata ({error:#}), evicting",
                    cache_key.get(..16).unwrap_or(cache_key),
                    cached_file.name,
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
                match crate::file_hash::hash_file(&blob) {
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

        // Update access time and hit count. The stamp pins the entry against
        // eviction for [`EVICTION_IDLE_GRACE`]; one that is already fresh
        // needs no write, and in a contended cell every hit's write would
        // queue behind the misses' store transactions. Hit counts therefore
        // count at most one hit per entry per [`HIT_STAMP_INTERVAL`].
        let age_seconds: Option<i64> = self
            .db
            .query_row(
                "SELECT strftime('%s', 'now') - strftime('%s', last_accessed) FROM entries \
                 WHERE cache_key = ?1",
                params![cache_key],
                |row| row.get(0),
            )
            .optional()?;
        if age_seconds.is_none_or(|age| age >= HIT_STAMP_INTERVAL.as_secs() as i64) {
            self.db.execute(
                "UPDATE entries SET last_accessed = datetime('now'), hit_count = hit_count + 1 \
                 WHERE cache_key = ?1",
                params![cache_key],
            )?;
        }

        Ok(Some(meta))
    }

    /// Acquire a build lock for a cache key. Returns None if another process holds it.
    pub fn try_lock(&self, cache_key: &str) -> Result<Option<KeyLock>> {
        StoreLock::try_acquire(&self.entry_dir(cache_key).with_extension("lock"))
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
        StoreLock::try_acquire(&self.config.store_dir().join("gc.lock"))
    }

    /// The lock a durability flusher holds while it drains entries stored
    /// without an fsync. A wrapper that finds it held knows a flusher is
    /// already running for this store.
    pub fn try_durability_flush_lock(&self) -> Result<Option<StoreLock>> {
        StoreLock::try_acquire(&self.config.store_dir().join("durability.lock"))
    }

    /// Block until the cross-process GC lock is held.
    ///
    /// Durable upload-intent publication uses the same lock as every
    /// production GC driver: either GC finishes first and publication
    /// revalidates that the payload survived, or the intent becomes durable
    /// before GC snapshots its protected keys.
    pub fn acquire_gc_lock(&self) -> Result<GcLock> {
        StoreLock::acquire(&self.config.store_dir().join("gc.lock"))
    }

    /// Wait for a cache key to become committed (another process is building it).
    pub fn wait_for_committed(&self, cache_key: &str) -> Result<bool> {
        if self.contains(cache_key) {
            return Ok(true);
        }
        self.wait_for_committed_with_timeout(cache_key, BUILD_LOCK_TIMEOUT)
    }

    fn wait_for_committed_with_timeout(&self, cache_key: &str, timeout: Duration) -> Result<bool> {
        let lock_path = self.entry_dir(cache_key).with_extension("lock");
        let _ = StoreLock::wait_until_available(&lock_path, timeout)?;
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
    pub fn put_with_compile_time_independent(
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
            // The mode is read from the compiler's output, never from the
            // staging snapshot taken below. That snapshot is authoritative for
            // *bytes* — hashing it rather than the live output is the whole
            // point of #822 — but it is not authoritative for permissions:
            // `stage_blob_from_source` prefers `try_reflink`, whose Linux
            // implementation creates the temp with `File::create` before the
            // FICLONE ioctl, so a 0o755 binary reads back at the umask on every
            // CoW filesystem (btrfs, XFS-with-reflink, ZFS >= 2.2, bcachefs).
            // `fs::copy` and macOS `clonefile` happen to preserve it, which is
            // why ext4 CI and macOS did not notice #822 undoing #648's restore
            // fix: recorded `executable: false`, a `harness = false` test binary
            // classifies as `Other("rustc:unknown")`, restores via `Hardlink`
            // with no 0o755, and cargo fails the run with "Permission denied
            // (os error 13)".
            //
            // A failed stat is an error rather than a silent `false`, because
            // `false` is precisely the wrong value this guards against — and
            // staging opens the same path one line below regardless.
            let executable = fs::metadata(source_path)
                .map(|meta| is_executable(&meta))
                .with_context(|| format!("stating compiler output for {store_name}"))?;
            let use_source_hardlink =
                source_hardlink_allowed::<P>(allow_source_hardlinks, store_name, executable);

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
            if size == 0 && !zero_byte_is_valid_output::<P>(store_name, crate_types) {
                Self::discard_staged_blob(&staged);
                anyhow::bail!("refusing to cache zero-byte artifact: {}", store_name);
            }
            total_size += size;

            let hash = crate::file_hash::hash_file(&staged)?;
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
        let emit_kinds = emit_kinds_for_files::<P>(&cached_files);

        // Capture the compiler's diagnostics so a cache hit can replay them
        // verbatim — warning gates / `-D warnings` then behave identically on a
        // hit vs a miss (kunobi-ninja/kache#336). Optionally capped against
        // pathological streams; uncapped by default for full fidelity.
        let diag_cap = max_diagnostics_bytes();

        // Write metadata (only meta.json in the entry directory)
        let meta = EntryMeta {
            cache_key: cache_key.to_string(),
            key_schema: kache_format::CACHE_KEY_VERSION,
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
        // A prior generation of this cache_key may still hold blob
        // references, most commonly a stranded row whose removal was
        // refused (#276) and that this put is about to overwrite via
        // INSERT OR REPLACE. Release them in this same transaction, before
        // this generation's increments and whatever the row's `committed`
        // state: a committed-but-stranded row is exactly the shape that
        // funnels back into put. Pre-#608 rows whose mapping the GC
        // backfill hasn't materialized yet still slip through (there is
        // nothing to decrement by); those remain `doctor --repair` /
        // reconcile territory.
        release_entry_blob_refs(&tx, cache_key)?;
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
        let durable = self.durable_writes();
        crate::atomic::atomic_replace_deferrable(&meta_path, meta_json.as_bytes(), durable)
            .context("writing entry metadata")?;
        tx.execute(
            "INSERT OR REPLACE INTO entries (cache_key, crate_name, crate_type, profile, num_features, size, content_hash, compile_time_ms, key_schema, committed, durable) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1, ?10)",
            params![cache_key, crate_name, crate_type_str, profile, num_features, total_size as i64, content_hash, compile_time_ms as i64, kache_format::CACHE_KEY_VERSION, durable],
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
                if P::stable_after_store(&file.name) {
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
            if !kache_format::is_blob_hash(&cached_file.hash) {
                anyhow::bail!(
                    "downloaded entry {short_key}: rejecting file {} — malformed blob hash {:?}",
                    cached_file.name,
                    cached_file.hash,
                );
            }
            // B: a `name` that is absolute or contains `..` escapes the entry dir
            // on join — require a single normal component.
            if !kache_format::is_safe_artifact_name(&cached_file.name) {
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
            let actual = crate::file_hash::hash_file(&file_path).with_context(|| {
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
        // A second download of a committed key (two daemons, a retried
        // prefetch) replaces a generation that already holds references.
        release_entry_blob_refs(&tx, cache_key)?;
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
    /// A failed import must not leave an uncommitted `meta.json` behind: daemon
    /// download waiters use the extracted directory as a wake-up hint, and the
    /// residue could otherwise be mistaken for a published entry. Cleanup is
    /// serialized with Store publishers and preserves any committed generation
    /// that won the race.
    pub fn import_restored_entry(&self, cache_key: &str) -> Result<()> {
        match self.import_downloaded_entry(cache_key) {
            Ok(()) => Ok(()),
            Err(import_error) => match self.discard_uncommitted_restored_entry(cache_key) {
                Ok(()) => Err(import_error),
                Err(cleanup_error) => Err(import_error.context(format!(
                    "also failed to discard uncommitted restored entry: {cleanup_error:#}"
                ))),
            },
        }
    }

    /// Remove extraction residue only when no committed row owns this key.
    ///
    /// An immediate transaction acquires SQLite's cross-process writer lock
    /// before the row check and keeps it through directory removal. A concurrent
    /// publisher therefore either commits first (and is preserved) or publishes
    /// after the stale directory is gone.
    fn discard_uncommitted_restored_entry(&self, cache_key: &str) -> Result<()> {
        self.discard_uncommitted_restored_entry_inner(cache_key, || {}, || {})
    }

    fn discard_uncommitted_restored_entry_inner(
        &self,
        cache_key: &str,
        before_write_lock: impl FnOnce(),
        after_write_lock: impl FnOnce(),
    ) -> Result<()> {
        if !kache_format::is_valid_cache_key(cache_key) {
            anyhow::bail!("refusing to discard invalid restored cache key");
        }

        // The test hooks make both sides of lock acquisition observable
        // without weakening the production lock or relying on scheduler
        // sleeps.
        before_write_lock();
        let tx = rusqlite::Transaction::new_unchecked(
            &self.db,
            rusqlite::TransactionBehavior::Immediate,
        )?;
        after_write_lock();
        let committed: i64 = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM entries WHERE cache_key = ?1 AND committed = 1)",
            params![cache_key],
            |row| row.get(0),
        )?;
        if committed == 0 {
            let entry_dir = self.entry_dir(cache_key);
            match fs::remove_dir_all(&entry_dir) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "removing uncommitted restored entry {}",
                            entry_dir.display()
                        )
                    });
                }
            }
        }
        tx.commit()?;
        Ok(())
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
    pub fn import_verified_restored_entries(
        &self,
        entries: &[VerifiedRestoredEntry],
    ) -> Result<usize> {
        let mut cache_keys = std::collections::HashSet::new();
        for entry in entries {
            if !kache_format::is_valid_cache_key(&entry.cache_key)
                || entry.meta.cache_key != entry.cache_key
            {
                anyhow::bail!("verified restore has an invalid cache-key binding");
            }
            if entry.meta.key_schema != kache_format::CACHE_KEY_VERSION {
                anyhow::bail!(
                    "verified restore {} uses incompatible key schema {}",
                    &entry.cache_key[..16],
                    entry.meta.key_schema
                );
            }
            if !kache_format::is_valid_crate_name(&entry.meta.crate_name) {
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
                if !kache_format::is_safe_artifact_name(&file.name)
                    || !kache_format::is_valid_cache_key(&file.hash)
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
            // No row owned this key, so a mapping still recorded for it is
            // a leftover whose references nothing else will release.
            release_entry_blob_refs(&tx, &entry.cache_key)?;

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
                if !kache_format::is_blob_hash(&file.hash)
                    || !kache_format::is_safe_artifact_name(&file.name)
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

    /// Cheap check for drift between `blobs` and `entry_blobs` alone: plain
    /// reads, no `meta.json` and no write lock. See
    /// [`crate::BlobRefcountDrift`] for what it can and cannot see.
    pub fn blob_refcount_drift(&self) -> Result<crate::BlobRefcountDrift> {
        Ok(crate::blob_refcount_drift(&self.db)?)
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
            if !kache_format::is_valid_cache_key(name) {
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
            if !kache_format::is_blob_hash(&file.hash)
                || !kache_format::is_safe_artifact_name(&file.name)
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
        // As in the batch import: a mapping without an entry row.
        release_entry_blob_refs(&tx, cache_key)?;

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
    pub fn matching_readonly_blob_inode(
        store_dir: &Path,
        output: &Path,
    ) -> Result<Option<PathBuf>> {
        let Ok(initial) = fs::metadata(output) else {
            return Ok(None);
        };
        if !metadata_is_readonly_regular(&initial) {
            return Ok(None);
        }

        let hash = crate::file_hash::hash_file(output)
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
    ///
    /// **The snapshot's bytes are faithful; its mode is not.** Only the hardlink
    /// ingest shares the source's inode, and only `fs::copy` promises to carry
    /// the permission bits over. [`crate::link::try_reflink`] on Linux creates
    /// the destination with `File::create` before the FICLONE ioctl, so a
    /// reflinked snapshot of a 0o755 binary lands at the umask instead — which
    /// is how #822 silently reverted #648. Anything permission-shaped
    /// (`CachedFile::executable`) must be read from the source, never from
    /// here.
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
            // Short-circuit: the real reflink is only attempted when no test
            // emulation claimed the staging slot, and never when the
            // force-hardlink seam (test-only) pretends CoW is unavailable so a
            // same-device `.rlib` must hardlink even on APFS/btrfs.
            let reflink_ok = should_try_store_reflink(force_store_hardlink())
                && (emulate_cow_reflink_ingest(source, tmp)?
                    || crate::link::try_reflink(source, tmp).is_ok());
            let ingest = if reflink_ok {
                StoreIngest::Reflink
            } else if allow_store_hardlink(
                allow_hardlink,
                fs::symlink_metadata(source).is_ok_and(|m| m.file_type().is_file()),
            ) {
                // Refused for symlink sources: hashing followed the link, but a
                // hardlink would link the symlink itself — a pointer into mutable
                // external state, never valid for a blob (same rule as
                // `materialize_blob`). That refusal records as `Ineligible`
                // below; only a real `link(2)` errno records as
                // CrossDevice/Permission/Other.
                match try_store_hard_link(source, tmp) {
                    Ok(()) => StoreIngest::Hardlink,
                    Err(io_err) => {
                        let reason = StoreCopyReason::from_io_kind(io_err.kind());
                        {
                            let io_reason = match reason {
                                StoreCopyReason::CrossDevice => {
                                    crate::link::HardlinkIoReason::CrossDevice
                                }
                                StoreCopyReason::Permission => {
                                    crate::link::HardlinkIoReason::Permission
                                }
                                StoreCopyReason::Other | StoreCopyReason::Ineligible => {
                                    crate::link::HardlinkIoReason::Other
                                }
                            };
                            // Staging links the build output into
                            // `<store>/staging`: EXDEV here means the build
                            // tree and the cache are on different mounts.
                            crate::link::warn_hardlink_fallback_once(
                                source,
                                &self.staging_dir(),
                                io_reason,
                                &io_err,
                            );
                        }
                        fs::copy(source, tmp).with_context(|| {
                            format!("copying {} into store staging", source.display())
                        })?;
                        StoreIngest::Copy(reason)
                    }
                }
            } else {
                fs::copy(source, tmp)
                    .with_context(|| format!("copying {} into store staging", source.display()))?;
                StoreIngest::Copy(StoreCopyReason::Ineligible)
            };
            if self.durable_writes() {
                crate::atomic::fsync_file(tmp).context("flushing staged blob")?;
            }
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
    ///
    /// `Ok(false)` also covers a publish that lost to a concurrent removal
    /// ([`PublishRename::Deferred`]): the blob is then absent, and the put's
    /// locked phase re-materializes it before committing a reference.
    fn publish_staged_blob(
        &self,
        staged: &Path,
        ingest: StoreIngest,
        hash: &str,
        size_bytes: u64,
    ) -> Result<bool> {
        let blob = self.blob_path(hash);
        let outcome = Self::publish_staged_blob_with(
            staged,
            &blob,
            || fs::rename(staged, &blob),
            crate::atomic::is_transient_rename_error,
        )?;
        if outcome != PublishRename::Published {
            return Ok(false);
        }
        if self.durable_writes() {
            let _ = crate::atomic::fsync_dir(blob.parent().unwrap());
        }
        match ingest {
            StoreIngest::Reflink => crate::opcounts::record_store_reflinked(size_bytes),
            StoreIngest::Hardlink => crate::opcounts::record_store_hardlinked(size_bytes),
            StoreIngest::Copy(reason) => {
                crate::opcounts::record_store_copied(size_bytes);
                record_store_copy_reason(reason, size_bytes);
            }
        }
        set_blob_readonly(&blob);
        Ok(true)
    }

    /// The rename half of [`Self::publish_staged_blob`], with the rename and
    /// its transient classifier passed in so a test can stage the Windows
    /// failures. The staged file is gone afterwards unless it became the blob.
    fn publish_staged_blob_with(
        staged: &Path,
        blob: &Path,
        rename: impl FnMut() -> std::io::Result<()>,
        is_transient: impl Fn(&std::io::Error) -> bool,
    ) -> Result<PublishRename> {
        if blob.is_file() {
            Self::discard_staged_blob(staged);
            return Ok(PublishRename::LostRace);
        }
        fs::create_dir_all(blob.parent().unwrap()).context("creating blob shard directory")?;
        // This runs outside the SQLite write lock, so a concurrent remove can
        // unlink the blob, and a concurrent put can publish it, at any point.
        // `publish_rename` waits out the states that clear on their own and
        // keeps the staged file alive between attempts.
        let outcome = publish_rename(rename, || publish_dest_state(blob), is_transient);
        if !matches!(outcome, Ok(PublishRename::Published)) {
            tracing::debug!("{} not published by this put: {outcome:?}", blob.display());
            Self::discard_staged_blob(staged);
        }
        outcome
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
            let actual = crate::file_hash::hash_file(&blob_path)?;
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

    /// Unlink key lock files nobody needs: no entry row for the key, unused
    /// for `min_age`, and not held. At most `cap` per call; the rest wait for
    /// the next sweep. The caller holds `gc.lock`, so one sweeper runs at a
    /// time.
    ///
    /// Each file is unlinked while this process holds its OS lock, and its
    /// age is read from the locked handle, so a claim that slipped in before
    /// the lock is seen. A contender that opened the file before the unlink
    /// finds the path changed once it gets the lock and reopens
    /// ([`StoreLock::acquire_current`]).
    pub fn sweep_stale_key_locks(
        &self,
        min_age: Duration,
        cap: usize,
    ) -> Result<KeyLockSweepStats> {
        self.sweep_stale_key_locks_at(min_age, cap, std::time::SystemTime::now())
    }

    fn sweep_stale_key_locks_at(
        &self,
        min_age: Duration,
        cap: usize,
        now: std::time::SystemTime,
    ) -> Result<KeyLockSweepStats> {
        let mut stats = KeyLockSweepStats::default();
        let Ok(names) = fs::read_dir(self.config.store_dir()) else {
            return Ok(stats);
        };
        let live: std::collections::HashSet<String> = self
            .db
            .prepare("SELECT cache_key FROM entries")?
            .query_map([], |row| row.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        for name in names.flatten() {
            let name = name.file_name();
            let Some(key) = name.to_str().and_then(key_of_lock_name) else {
                continue;
            };
            stats.seen += 1;
            // Past the cap the walk only counts, which costs no stat.
            if stats.removed >= cap || live.contains(key) {
                continue;
            }
            if remove_stale_lock_file(&self.config.store_dir().join(&name), min_age, now, || {}) {
                stats.removed += 1;
            }
        }
        Ok(stats)
    }

    /// The per-key structures nothing else bounds: stale key lock files and
    /// unused input predictions. The caller holds `gc.lock`. A pass that fails
    /// is logged and counts as zero; the next sweep tries again.
    pub fn sweep_housekeeping(&self) -> HousekeepingStats {
        let locks = self
            .sweep_stale_key_locks(KEY_LOCK_SWEEP_GRACE, KEY_LOCK_SWEEP_CAP)
            .unwrap_or_else(|error| {
                tracing::warn!("gc: key lock sweep failed: {error:#}");
                KeyLockSweepStats::default()
            });
        let predictions_pruned = self
            .file_hash_cache()
            .prune_input_predictions()
            .unwrap_or_else(|error| {
                tracing::warn!("gc: input prediction pruning failed: {error}");
                0
            });
        HousekeepingStats {
            key_locks_removed: locks.removed,
            key_locks_remaining: locks.remaining(),
            predictions_pruned,
        }
    }

    /// Cache dir this store was opened with (`blobs/`, `index.db`, `store/`).
    pub fn cache_dir(&self) -> &std::path::Path {
        &self.config.cache_dir
    }

    /// Get the directory for a cache entry.
    /// Whether puts flush on the build path. Off under deferred durability,
    /// where [`Store::flush_durability`] flushes later.
    fn durable_writes(&self) -> bool {
        !self.config.deferred_durability
    }

    /// Whether an entry's blobs and metadata have reached disk. Unknown rows
    /// read as durable: the size check and the verification policy still apply.
    fn entry_is_durable(&self, cache_key: &str) -> bool {
        self.db
            .query_row(
                "SELECT durable FROM entries WHERE cache_key = ?1",
                params![cache_key],
                |row| row.get::<_, bool>(0),
            )
            .unwrap_or(true)
    }

    /// Entries stored without an fsync that no flush has reached yet.
    pub fn pending_durability(&self) -> Result<u64> {
        Ok(self.db.query_row(
            "SELECT count(*) FROM entries WHERE committed = 1 AND durable = 0",
            [],
            |row| row.get::<_, i64>(0),
        )? as u64)
    }

    /// Flush up to `limit` entries stored without an fsync: every blob and
    /// its shard directory, then the entry's `meta.json` and directory, and
    /// mark them durable. An entry whose blob went missing meanwhile is
    /// evicted instead. Returns how many entries were flushed.
    pub fn flush_durability(&self, limit: usize) -> Result<usize> {
        let keys: Vec<String> = {
            let mut stmt = self.db.prepare_cached(
                "SELECT cache_key FROM entries WHERE committed = 1 AND durable = 0
                 ORDER BY created_at LIMIT ?1",
            )?;
            let rows = stmt.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
            rows.collect::<Result<_, _>>()?
        };
        let mut flushed = 0;
        for key in keys {
            if self.flush_entry_durability(&key)? {
                flushed += 1;
            }
        }
        Ok(flushed)
    }

    /// Flush one entry stored without an fsync and mark it durable. `Ok(false)`
    /// when the entry was already durable or had to be evicted.
    pub fn flush_entry_durability(&self, cache_key: &str) -> Result<bool> {
        if self.entry_is_durable(cache_key) {
            return Ok(false);
        }
        let entry_dir = self.entry_dir(cache_key);
        let meta_path = entry_dir.join("meta.json");
        let meta: EntryMeta = match fs::read_to_string(&meta_path)
            .ok()
            .and_then(|json| serde_json::from_str(&json).ok())
        {
            Some(meta) => meta,
            None => {
                tracing::warn!(
                    "cache entry {} has no readable meta.json to flush, evicting",
                    cache_key.get(..16).unwrap_or(cache_key)
                );
                let _ = self.remove_entry(cache_key);
                return Ok(false);
            }
        };
        for file in &meta.files {
            let blob = self.blob_path(&file.hash);
            if let Err(error) = fsync_published_blob(&blob) {
                // A blob that is gone takes the entry with it; anything else
                // (a busy handle, a transient IO error) leaves the entry
                // pending, to be flushed by a later worker or by GC. An
                // unflushed entry is still served, with its bytes verified.
                if error.kind() == std::io::ErrorKind::NotFound {
                    tracing::warn!(
                        "cache entry {} blob {} vanished before it was flushed, evicting",
                        cache_key.get(..16).unwrap_or(cache_key),
                        file.name
                    );
                    let _ = self.remove_entry(cache_key);
                } else {
                    tracing::debug!(
                        "cache entry {} blob {} could not be flushed ({error}); still pending",
                        cache_key.get(..16).unwrap_or(cache_key),
                        file.name
                    );
                }
                return Ok(false);
            }
            if let Some(parent) = blob.parent() {
                let _ = crate::atomic::fsync_dir(parent);
            }
        }
        fsync_published_blob(&meta_path).context("flushing entry metadata")?;
        let _ = crate::atomic::fsync_dir(&entry_dir);
        self.db.execute(
            "UPDATE entries SET durable = 1 WHERE cache_key = ?1",
            params![cache_key],
        )?;
        Ok(true)
    }

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

    /// Record which unit a freshly stored entry came from, by Cargo's
    /// `-C metadata` hash. Every cache key folds that hash in, so a store
    /// with no row for (crate name, unit) holds no key the unit can produce;
    /// the crate-presence probe behind deferred discovery reads it.
    pub fn record_entry_unit(&self, cache_key: &str, unit: &str) -> Result<()> {
        if unit.is_empty() {
            return Ok(());
        }
        self.db.execute(
            "UPDATE entries SET unit_id = ?2 WHERE cache_key = ?1",
            params![cache_key, unit],
        )?;
        Ok(())
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

    /// Remember a Cargo target root without putting absolute paths in cache
    /// entries or remote data. Updates are debounced to keep compiler-wrapper
    /// writes off the hot path.
    pub fn remember_target_root(&self, target: &Path, workspace_root: &Path) -> Result<()> {
        if !crate::filesystem::target_root_is_safe(target, workspace_root) {
            return Ok(());
        }
        let target = std::path::absolute(target)?;
        let workspace_root = std::path::absolute(workspace_root)?;
        let Some(identity) = crate::filesystem::directory_identity(&target) else {
            return Ok(());
        };
        // The upsert below refuses to touch a fresh, unchanged row, but even
        // a refused upsert takes the index's write lock; read first so a
        // warm target directory costs one query per invocation, not a wait
        // behind whichever miss is storing.
        let fresh: Option<bool> = self
            .db
            .query_row(
                "SELECT last_seen > unixepoch() - 300
                    AND workspace_root = ?2 AND device = ?3 AND inode = ?4
                 FROM target_roots WHERE path = ?1",
                params![
                    target.to_string_lossy(),
                    workspace_root.to_string_lossy(),
                    identity.device.to_string(),
                    identity.inode.to_string(),
                ],
                |row| row.get(0),
            )
            .optional()?;
        if fresh == Some(true) {
            return Ok(());
        }
        let changed = self.db.execute(
            "INSERT INTO target_roots
                (path, workspace_root, first_seen, last_seen, device, inode)
             VALUES (?1, ?2, unixepoch(), unixepoch(), ?3, ?4)
             ON CONFLICT(path) DO UPDATE SET
                workspace_root = excluded.workspace_root,
                last_seen = unixepoch(),
                device = excluded.device,
                inode = excluded.inode
             WHERE target_roots.last_seen <= unixepoch() - 300
                OR target_roots.workspace_root != excluded.workspace_root
                OR target_roots.device != excluded.device
                OR target_roots.inode != excluded.inode",
            params![
                target.to_string_lossy(),
                workspace_root.to_string_lossy(),
                identity.device.to_string(),
                identity.inode.to_string(),
            ],
        )?;
        if changed > 0 {
            self.db.execute(
                "DELETE FROM target_roots WHERE last_seen < unixepoch() - 15552000",
                [],
            )?;
            self.db.execute(
                "DELETE FROM target_roots WHERE path IN (
                    SELECT path FROM target_roots
                    ORDER BY last_seen DESC, path ASC
                    LIMIT -1 OFFSET 2048
                )",
                [],
            )?;
        }
        Ok(())
    }

    pub fn tracked_target_roots(&self, stale_hours: u64) -> Result<Vec<TrackedTargetRoot>> {
        let stale_seconds = stale_hours.saturating_mul(3600).min(i64::MAX as u64) as i64;
        let mut stmt = self.db.prepare(
            "SELECT path, workspace_root, first_seen, last_seen, device, inode
             FROM target_roots
             WHERE last_seen <= unixepoch() - ?1
             ORDER BY last_seen ASC, path ASC",
        )?;
        let rows = stmt.query_map(params![stale_seconds], |row| {
            let device: String = row.get(4)?;
            let inode: String = row.get(5)?;
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                device,
                inode,
            ))
        })?;
        let mut targets = Vec::new();
        for row in rows {
            let (path, workspace_root, first_seen, last_seen, device, inode) = row?;
            let (Ok(device), Ok(inode)) = (device.parse::<u64>(), inode.parse::<u64>()) else {
                continue;
            };
            targets.push(TrackedTargetRoot {
                path: PathBuf::from(path),
                workspace_root: PathBuf::from(workspace_root),
                first_seen,
                last_seen,
                identity: crate::filesystem::PathIdentity { device, inode },
            });
        }
        Ok(targets)
    }

    pub fn forget_target_root(&self, path: &Path) -> Result<()> {
        self.db.execute(
            "DELETE FROM target_roots WHERE path = ?1",
            params![path.to_string_lossy()],
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
    pub fn eviction_candidates(&self) -> Result<Vec<crate::eviction::EntryFeatures>> {
        let mut stmt = self.db.prepare(
            "SELECT cache_key, size, hit_count, content_hash, committed,
                    (julianday('now') - julianday(last_accessed)) * 24.0,
                    compile_time_ms,
                    (SELECT COALESCE(SUM(b.size), 0)
                       FROM entry_blobs eb JOIN blobs b ON b.hash = eb.hash
                      WHERE eb.cache_key = entries.cache_key
                        AND eb.refs = b.refcount),
                    EXISTS(SELECT 1 FROM entry_blobs eb2
                            WHERE eb2.cache_key = entries.cache_key),
                    last_accessed >= datetime('now', ?1)
             FROM entries",
        )?;
        let rows = stmt
            .query_map(
                params![format!("-{} seconds", EVICTION_IDLE_GRACE.as_secs())],
                |row| {
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
                        recently_accessed: row.get(9)?,
                    })
                },
            )?
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
            self.config.upload_spool_max_jobs,
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
            if kache_format::is_valid_cache_key(key) {
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
        let mut eviction_writes = std::time::Duration::ZERO;
        let (slice, pause) = self.eviction_pacing;
        let mut pacer = EvictionWritePacer::new(slice, pause);
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
            if features.is_some_and(|f| f.recently_accessed) {
                stats.entries_pinned += 1;
                stats.entries_recent_prefiltered += 1;
                continue;
            }
            let write_started = std::time::Instant::now();
            let removal = self.remove_entry_guarded(key, Some(EVICTION_IDLE_GRACE));
            let took = write_started.elapsed();
            eviction_writes += took;
            match removal {
                Ok(GuardedRemoval::Reclaimed(reclaim)) => {
                    if let Some(pause) = pacer.after_write(took) {
                        std::thread::sleep(pause);
                    }
                    stats.entries_evicted += 1;
                    // Budget on bytes the removal *actually* freed on disk, not
                    // the entry's logical size: evicting an entry whose blobs
                    // are all shared frees nothing, and the sweep must keep
                    // going rather than stop believing it reached the target
                    // (#608).
                    stats.bytes_freed += reclaim.freed_bytes;
                    stats.disk_bytes_reclaimed += reclaim.disk_bytes_reclaimed;
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
                Ok(GuardedRemoval::Skipped) => {
                    stats.entries_pinned += 1;
                    continue;
                }
                Ok(GuardedRemoval::Unreclaimable) => {
                    stats.entries_unreclaimable += 1;
                    continue;
                }
                Err(e) => {
                    // A corrupt entry (unloadable meta.json) refuses removal to
                    // avoid leaking blob refcounts (#276); skip it and keep
                    // evicting the rest rather than aborting the whole sweep.
                    record_eviction_failure(&mut stats, &e);
                    tracing::warn!("gc: skipping eviction of {key}: {e:#}");
                    if is_sqlite_contention(&e) {
                        std::thread::sleep(pacer.after_contention());
                    }
                    continue;
                }
            }
        }
        stats.evict_write_ms = eviction_writes.as_millis() as u64;
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
    /// Bounded to 10,000 entries per call. Measured on
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
                // An entry whose artifacts still sit beside meta.json has
                // references nobody counted yet, and a mapping would claim
                // they were. `migrate_entry_to_blobs` counts them and moves
                // the artifacts out; a later pass maps the entry.
                if still_wanted != 0 && !has_unmigrated_artifacts(&self.entry_dir(key), &meta) {
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
    /// never a false hit). Returns [`GuardedRemoval::Reclaimed`] when this
    /// call removed the entry, with the *physical* bytes and blob files
    /// actually reclaimed — zero when every blob is still referenced by
    /// another entry (#608).
    ///
    /// `None` (the plain `remove_entry` path) always removes — explicit purge /
    /// `doctor` must not be blocked by recency or by external clones.
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
    ) -> Result<GuardedRemoval> {
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
    ) -> Result<GuardedRemoval> {
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
    ) -> Result<GuardedRemoval> {
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
                RemovalAttempt::Done(Some(reclaim)) => {
                    return Ok(GuardedRemoval::Reclaimed(reclaim));
                }
                RemovalAttempt::Done(None) => return Ok(GuardedRemoval::Skipped),
                RemovalAttempt::Unreclaimable => return Ok(GuardedRemoval::Unreclaimable),
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
            Err(e)
                if e.kind() == std::io::ErrorKind::NotFound
                    || crate::atomic::is_transient_rename_error(&e) =>
            {
                // No readable meta.json. A same-key operation may be
                // mid-flight: a publisher materializes meta inside its
                // registration transaction, and a removal's cleanup pass runs
                // in its own locked transaction (#670) — so "meta missing, row
                // present" can be a healthy transient, not only the
                // stranded-entry shape. Bounce off the write lock — the no-op
                // write statement waits (busy_timeout) until any in-flight
                // writer commits or rolls back — then judge the settled state.
                // Both the row and the meta are checked while the lock is
                // still held: after dropping it another writer could move the
                // pairing again and a healthy already-absent state would
                // misreport as #276 corruption.
                //
                // A concurrent remover that already unlinked this meta.json
                // arrives here too. On Unix that read returns NotFound; on
                // Windows the name lingers delete-pending and the read fails
                // with ERROR_ACCESS_DENIED instead, which used to fall through
                // to the unreadable-meta arm and report #276 corruption for two
                // healthy removers. Both shapes mean the same thing — someone
                // else is mid-operation — so both settle on the write lock
                // rather than on a sleep.
                let tx = self.db.unchecked_transaction()?;
                tx.execute("UPDATE entries SET cache_key = cache_key WHERE 1 = 0", [])?;
                let row_exists: i64 = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM entries WHERE cache_key = ?1)",
                    params![cache_key],
                    |row| row.get(0),
                )?;
                if row_exists == 0 {
                    // The concurrent removal won (or the key never existed);
                    // nothing left to remove, and the meta's state cannot
                    // change that — so decide before probing it, which on
                    // Windows may still be delete-pending and unstattable.
                    return Ok(RemovalAttempt::Done(None));
                }
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

        // Eviction only: refuse to drop an entry whose last-ref blobs are
        // still cloned into a worktree (kunobi-ninja/kache#725). Unlinking
        // those names frees no disk and destroys a still-usable hit.
        // Explicit `remove_entry` (purge / doctor) passes `skip_if_idle_lt =
        // None` and still unlinks.
        if skip_if_idle_lt.is_some() && !self.config.gc_evict_shared {
            let mut last_ref_count: std::collections::HashMap<&str, i64> =
                std::collections::HashMap::new();
            for hash in &hashes {
                *last_ref_count.entry(hash.as_str()).or_insert(0) += 1;
            }
            for (hash, held) in last_ref_count {
                let rc: i64 = tx.query_row(
                    "SELECT refcount FROM blobs WHERE hash = ?1",
                    params![hash],
                    |row| row.get(0),
                )?;
                if externally_retained_last_reference(
                    rc,
                    held,
                    crate::filesystem::blob_has_external_retainer(&self.blob_path(hash)),
                ) {
                    return Ok(RemovalAttempt::Unreclaimable);
                }
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
        // A mapping nobody counted must not spend another entry's reference
        // and unlink a blob that entry still serves from.
        floor_blob_refs_at_mappings(&tx, cache_key)?;
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
                let blob = self.blob_path(&hash);
                let disk =
                    crate::filesystem::blob_reclaimable_bytes(&blob).unwrap_or(size.max(0) as u64);
                unlink_blob(&blob);
                reclaim.freed_bytes += size.max(0) as u64;
                reclaim.disk_bytes_reclaimed += disk;
                reclaim.blobs_unlinked += 1;
            }
        }
        cleanup_tx.commit()?;
        Ok(RemovalAttempt::Done(Some(reclaim)))
    }

    /// Test-only: insert a bare committed entry row, for tests that stage a
    /// synthetic `meta.json` and need removal to own the directory (#670
    /// made directory cleanup conditional on owning the row).
    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn insert_entry_row_for_test(&self, cache_key: &str) {
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
    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn set_last_accessed_for_test(&self, cache_key: &str, sql_modifier: &str) {
        self.db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', ?2) WHERE cache_key = ?1",
                params![cache_key, sql_modifier],
            )
            .unwrap();
    }

    /// Clear the entire store.
    ///
    /// Index rows drop first, in one transaction: once it commits no
    /// reader can begin a restore from a purged entry, and the
    /// filesystem wipe then runs against a store the index no longer
    /// references — a crash mid-wipe strands at worst orphan files for
    /// the sweep, never the pre-existing rows dangling over deleted
    /// blobs that the old wipe-then-delete order could leave.
    ///
    /// Publishers don't take `gc.lock`, so a put can still commit a
    /// fresh row while the wipe is deleting the files it just staged.
    /// The second row-deletion pass reduces that to the store's
    /// tolerated shapes: a row committed before the pass is dropped
    /// (its files become sweepable orphans), and one committed after it
    /// at worst lands stranded — the refuse-removal / miss / re-put
    /// path that already recovers it.
    pub fn clear(&self) -> Result<()> {
        let drop_index_rows = || -> Result<()> {
            let tx = self.db.unchecked_transaction()?;
            tx.execute("DELETE FROM entries", [])?;
            tx.execute("DELETE FROM entry_blobs", [])?;
            tx.execute("DELETE FROM blobs", [])?;
            tx.execute("DELETE FROM incremental_dirs", [])?;
            tx.execute("DELETE FROM target_roots", [])?;
            tx.commit()?;
            Ok(())
        };
        drop_index_rows()?;
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
        drop_index_rows()
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
    /// Returns `false`, touching nothing, when no committed row owns the key.
    ///
    /// An artifact beside meta.json is not proof of a legacy entry: a remote
    /// download extracts into the same directory before its import, and a
    /// daemon killed in that window leaves the same shape. Counting
    /// references for it strands them, because no entry, and so no eviction,
    /// ever gives them back.
    ///
    /// A committed entry with a leftover import artifact still gains a
    /// reference it already held. That errs high only, and the daemon's
    /// blob-index reconcile repairs it.
    fn migrate_entry_to_blobs(&self, meta: &EntryMeta) -> Result<bool> {
        let committed: bool = self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM entries WHERE cache_key = ?1 AND committed = 1)",
            params![meta.cache_key],
            |row| row.get(0),
        )?;
        if !committed {
            return Ok(false);
        }
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
        Ok(true)
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
                Ok(true) => stats.entries_migrated += 1,
                Ok(false) | Err(_) => stats.entries_skipped += 1,
            }
        }

        progress(total, total);
        Ok(stats)
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

fn externally_retained_last_reference(rc: i64, held: i64, retained: bool) -> bool {
    rc > 0 && rc <= held && retained
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

    #[test]
    fn lock_polls_back_off_from_a_millisecond_to_the_interval() {
        let naps: Vec<u64> = (0..10)
            .map(|attempt| lock_poll_interval(attempt).as_millis() as u64)
            .collect();
        assert_eq!(naps, vec![1, 2, 4, 8, 16, 32, 64, 100, 100, 100]);
    }

    /// Opening an index runs its DDL once: the second open finds the schema
    /// generation current and skips every statement (each would otherwise
    /// take the write lock), while an index from before the stamp still
    /// migrates.
    #[test]
    fn index_ddl_runs_once_per_schema_generation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.db");
        let db = open_index_db(&path).unwrap();
        let generation: i64 = db
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(generation, INDEX_SCHEMA_GENERATION);
        // A pre-stamp index (generation 0) migrates and gets stamped.
        db.pragma_update(None, "user_version", 0_i64).unwrap();
        db.execute_batch("DROP TABLE target_roots").unwrap();
        drop(db);
        let db = open_index_db(&path).unwrap();
        let generation: i64 = db
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(generation, INDEX_SCHEMA_GENERATION);
        let tables: i64 = db
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'target_roots'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        // A generation-1 index (0.23) lacks the durability column and the two
        // C memo tables; the generation bump is what makes it migrate.
        db.pragma_update(None, "user_version", 1_i64).unwrap();
        db.execute_batch(
            "ALTER TABLE entries DROP COLUMN durable;
             DROP TABLE cc_mapped_hashes;
             DROP TABLE cc_asm_scans;",
        )
        .unwrap();
        drop(db);
        let db = open_index_db(&path).unwrap();
        let durable_column: i64 = db
            .query_row(
                "SELECT count(*) FROM pragma_table_info('entries') WHERE name = 'durable'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(durable_column, 1, "generation 1 gains the durable column");
        let memo_tables: i64 = db
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name IN ('cc_mapped_hashes', 'cc_asm_scans')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(memo_tables, 2, "generation 1 gains the C memo tables");
        assert_eq!(
            tables, 1,
            "the dropped table was recreated by the migration"
        );
    }

    /// An index stamped at generation 3 predates the crate-name index, and
    /// the stamp alone must not keep it from gaining one.
    #[test]
    fn index_from_generation_three_gains_the_crate_name_index() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.db");
        let db = open_index_db(&path).unwrap();
        db.execute_batch("DROP INDEX idx_entries_crate_name;")
            .unwrap();
        db.pragma_update(None, "user_version", 3_i64).unwrap();
        drop(db);

        let db = open_index_db(&path).unwrap();
        let indexes: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'index' AND name = 'idx_entries_crate_name'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(indexes, 1);
        let generation: i64 = db
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(generation, INDEX_SCHEMA_GENERATION);
    }

    /// An index stamped at generation 4 has no unit column; the next open
    /// adds it, its index, and lets the wrapper record units from then on.
    #[test]
    fn index_from_generation_four_gains_the_unit_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.db");
        let db = open_index_db(&path).unwrap();
        db.execute_batch(
            "DROP INDEX idx_entries_crate_unit;
             ALTER TABLE entries DROP COLUMN unit_id;",
        )
        .unwrap();
        db.pragma_update(None, "user_version", 4_i64).unwrap();
        drop(db);

        let db = open_index_db(&path).unwrap();
        let indexes: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'index' AND name = 'idx_entries_crate_unit'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(indexes, 1);
        db.execute(
            "INSERT INTO entries (cache_key, crate_name, unit_id) VALUES ('k', 'c', 'u')",
            [],
        )
        .unwrap();
        let generation: i64 = db
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(generation, INDEX_SCHEMA_GENERATION);
    }

    /// A put never learns its unit; the wrapper records it afterwards, and
    /// an empty unit leaves the row alone.
    #[test]
    fn record_entry_unit_updates_only_that_entry() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path())).unwrap();
        store
            .db
            .execute_batch(
                "INSERT INTO entries (cache_key, crate_name) VALUES ('a', 'x'), ('b', 'x');",
            )
            .unwrap();
        store.record_entry_unit("a", "unit-a").unwrap();
        store.record_entry_unit("b", "").unwrap();
        let units: Vec<(String, String)> = store
            .db
            .prepare("SELECT cache_key, unit_id FROM entries ORDER BY cache_key")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            units,
            vec![
                ("a".to_string(), "unit-a".to_string()),
                ("b".to_string(), String::new())
            ]
        );
        assert!(
            store
                .file_hash_cache()
                .has_entry_for_unit("x", "unit-a")
                .unwrap()
        );
        assert!(
            store
                .file_hash_cache()
                .has_entry_for_unit("x", "unit-z")
                .unwrap(),
            "row b has no unit"
        );
    }

    /// An index stamped before the env-use memo was versioned still carries
    /// the boolean table, whose rows the fixed scanner must never reuse.
    #[test]
    fn index_from_generation_one_moves_to_the_versioned_env_use_memo() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.db");
        let db = open_index_db(&path).unwrap();
        db.execute_batch(
            "DROP TABLE source_env_dep_uses;
             CREATE TABLE source_env_runtime_uses (
                content_hash    TEXT NOT NULL,
                env_var         TEXT NOT NULL,
                has_runtime_use INTEGER NOT NULL,
                updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (content_hash, env_var)
             );",
        )
        .unwrap();
        db.pragma_update(None, "user_version", 1_i64).unwrap();
        drop(db);

        let db = open_index_db(&path).unwrap();
        let tables: Vec<String> = db
            .prepare(
                "SELECT name FROM sqlite_master WHERE type = 'table'
                 AND name IN ('source_env_runtime_uses', 'source_env_dep_uses')",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(tables, vec!["source_env_dep_uses".to_string()]);
    }
    type Store = ArtifactStore<TestPolicy>;
    struct TestPolicy;
    impl ArtifactPolicy for TestPolicy {
        fn allow_hardlink(name: &str) -> bool {
            matches!(
                std::path::Path::new(name)
                    .extension()
                    .and_then(|ext| ext.to_str()),
                Some(
                    "rlib"
                        | "rmeta"
                        | "o"
                        | "obj"
                        | "a"
                        | "lib"
                        | "pdb"
                        | "dwo"
                        | "tar"
                        | "unknown"
                )
            )
        }
        fn allow_empty(name: &str, kinds: &[String]) -> bool {
            name.ends_with(".rmeta")
                && kinds
                    .iter()
                    .all(|kind| matches!(kind.as_str(), "bin" | "cdylib" | "staticlib"))
        }
        fn emit_kind(name: &str) -> Option<&'static str> {
            match std::path::Path::new(name)
                .extension()
                .and_then(|ext| ext.to_str())
                .unwrap_or("")
            {
                "rlib" | "so" | "dylib" | "dll" | "exe" | "a" | "lib" | "wasm" | "" => Some("link"),
                "rmeta" => Some("metadata"),
                "o" | "obj" => Some("obj"),
                "d" | "pp" => Some("dep-info"),
                "s" | "asm" => Some("asm"),
                "ll" => Some("llvm-ir"),
                "bc" => Some("llvm-bc"),
                "mir" => Some("mir"),
                _ => None,
            }
        }
        fn stable_after_store(name: &str) -> bool {
            !name.ends_with(".d") && !name.ends_with(".pp")
        }
    }

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
                hardlink_eligible::<TestPolicy>(name, false),
                gate_open,
                "{name} should be hardlink-eligible on insert (behind the Windows gate)"
            );
        }

        // Mutable kinds (Copy strategy on restore): never eligible.
        assert!(!hardlink_eligible::<TestPolicy>("libfoo.dylib", false));
        assert!(!hardlink_eligible::<TestPolicy>("libfoo.so", false));
        assert!(!hardlink_eligible::<TestPolicy>("foo.exe", false));

        // Insert-only exclusions: `.d` is rewritten in place after `put`
        // (Expand), extensionless names are bin executables by rustc's Unix
        // convention, and an executable mode bit wins over the filename.
        assert!(!hardlink_eligible::<TestPolicy>("serde-abc123.d", false));
        assert!(!hardlink_eligible::<TestPolicy>("my-binary", false));
        assert!(!hardlink_eligible::<TestPolicy>(
            "libserde-abc123.rlib",
            true
        ));
    }

    #[test]
    fn source_hardlink_policy_honors_independent_storage() {
        assert!(!source_hardlink_allowed::<TestPolicy>(
            false, "foo.o", false
        ));
        assert_eq!(
            source_hardlink_allowed::<TestPolicy>(true, "foo.o", false),
            hardlink_eligible::<TestPolicy>("foo.o", false)
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

    /// #648 made restore honour the mode bit recorded at insert time, because a
    /// `[[test]] harness = false` target supplies its own `main` and is compiled
    /// with neither `--test` nor `--crate-type` — nothing in the rustc argv says
    /// "executable", so the recorded bit is the only signal. #822 then began
    /// reading that bit off the *staging snapshot* rather than the compiler's
    /// output, and a reflinked snapshot is created at the umask: on every CoW
    /// filesystem the entry recorded `executable: false`, restore fell back to
    /// `Hardlink`, and cargo failed the run with "Permission denied (os error
    /// 13)".
    ///
    /// The emulation is what makes this reachable on ext4. Without it the copy
    /// fallback preserves the mode, the assertion holds for free, and the test
    /// would only fail on the filesystems CI never runs on.
    #[cfg(unix)]
    #[test]
    fn put_records_the_executable_bit_from_the_compiler_output_not_the_staging_snapshot() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let output = dir.path().join("harness-a1b2c3d4e5f60718");
        fs::write(&output, b"\x7fELF harness=false test binary").unwrap();
        fs::set_permissions(&output, fs::Permissions::from_mode(0o755)).unwrap();

        {
            let _forced = ModeDroppingIngest::enable();
            store
                .put_with_compile_time_independent(
                    "harness-entry",
                    "harness",
                    &[],
                    &[],
                    "x86_64-unknown-linux-gnu",
                    "",
                    &[(output.clone(), "harness-a1b2c3d4e5f60718".to_string())],
                    "",
                    "",
                    1,
                )
                .unwrap();
        }

        let meta = store.get("harness-entry").unwrap().unwrap();
        // Proves the emulation actually dropped the bit, so the assertion below
        // cannot pass because the snapshot happened to keep it.
        let blob_mode = fs::metadata(store.blob_path(&meta.files[0].hash))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(
            blob_mode & 0o111,
            0,
            "emulated reflink ingest should have staged without the bit, got {blob_mode:o}"
        );
        assert!(
            meta.files[0].executable,
            "the recorded mode must come from the compiler's 0o755 output, \
             not from the staging snapshot"
        );
    }

    /// The converse, under the same emulation: reading the mode from the source
    /// must not degenerate into recording every artifact executable.
    #[cfg(unix)]
    #[test]
    fn put_records_no_executable_bit_for_a_non_executable_output() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let output = dir.path().join("libfoo.rlib");
        fs::write(&output, b"rlib bytes").unwrap();
        fs::set_permissions(&output, fs::Permissions::from_mode(0o644)).unwrap();

        {
            let _forced = ModeDroppingIngest::enable();
            store
                .put_with_compile_time_independent(
                    "rlib-entry",
                    "foo",
                    &["lib".to_string()],
                    &[],
                    "x86_64-unknown-linux-gnu",
                    "",
                    &[(output.clone(), "libfoo.rlib".to_string())],
                    "",
                    "",
                    1,
                )
                .unwrap();
        }

        let meta = store.get("rlib-entry").unwrap().unwrap();
        assert!(
            !meta.files[0].executable,
            "a 0o644 compiler output must not be recorded executable"
        );
    }

    /// cargo-mutants replacing `Drop` with `()` left the thread-local on.
    /// The put tests never insert again after the guard, so they could not
    /// see the leak.
    #[cfg(unix)]
    #[test]
    fn mode_dropping_ingest_guard_clears_on_drop() {
        assert!(
            !FORCE_MODE_DROPPING_INGEST.with(std::cell::Cell::get),
            "ingest emulation must start off"
        );
        {
            let _forced = ModeDroppingIngest::enable();
            assert!(
                FORCE_MODE_DROPPING_INGEST.with(std::cell::Cell::get),
                "enable must turn ingest emulation on"
            );
        }
        assert!(
            !FORCE_MODE_DROPPING_INGEST.with(std::cell::Cell::get),
            "Drop must turn ingest emulation off so a later put on this thread is not forced"
        );
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

        let hash = crate::file_hash::hash_file(&source).unwrap();
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

        let hash = crate::file_hash::hash_file(&source).unwrap();
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

        let hash = crate::file_hash::hash_file(&symlink).unwrap();
        let blob = store.blob_path(&hash);
        let meta = fs::symlink_metadata(&blob).unwrap();
        assert!(
            meta.file_type().is_file(),
            "blob must be a regular file, not a symlink"
        );
        assert_eq!(fs::read(&blob).unwrap(), b"real artifact bytes");
    }

    #[test]
    fn should_try_store_reflink_is_the_inverse_of_the_force_flag() {
        assert!(should_try_store_reflink(false));
        assert!(!should_try_store_reflink(true));
    }

    #[test]
    fn allow_store_hardlink_requires_permission_and_a_regular_file() {
        assert!(allow_store_hardlink(true, true));
        assert!(!allow_store_hardlink(false, true));
        assert!(!allow_store_hardlink(true, false));
        assert!(!allow_store_hardlink(false, false));
    }

    #[test]
    fn force_store_hardlink_defaults_off_and_follows_the_guard() {
        assert!(!force_store_hardlink());
        {
            let _guard = ForceStoreHardlink::enable();
            assert!(force_store_hardlink());
        }
        assert!(!force_store_hardlink());
    }

    #[cfg(unix)]
    #[test]
    fn materialize_blob_without_hardlink_permission_does_not_share_inode() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("libx.rlib");
        fs::write(&source, b"ineligible-materialize").unwrap();
        let blob = dir.path().join("blobs").join("aa").join("a".repeat(64));
        let _force = ForceStoreHardlink::enable();
        assert!(materialize_blob(&source, &blob, false).unwrap());
        assert_eq!(
            fs::metadata(&blob).unwrap().nlink(),
            1,
            "allow_hardlink=false must copy, not hardlink"
        );
        assert_eq!(fs::metadata(&source).unwrap().nlink(), 1);
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
        let hash = crate::file_hash::hash_file(&staged_a).unwrap();
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
        assert_eq!(
            stats.scanned, 1,
            "the in-flight file is skipped before it is ever counted"
        );
        assert_eq!(stats.bytes_reclaimed, b"abandoned".len() as u64);
        assert!(!stale.exists());
        assert!(fresh.exists(), "fresh staging file must survive the sweep");

        // Once it ages out, it goes too.
        let stats = store.sweep_stale_staging(Duration::ZERO);
        assert_eq!(stats.removed, 1);
        assert_eq!(stats.scanned, 1);
        assert_eq!(stats.bytes_reclaimed, b"in-flight".len() as u64);
        assert!(!fresh.exists());
    }

    /// Discarding a hardlinked staging temp has to hand the source back
    /// writable. The temp shares the build output's inode, so the read-only
    /// guard the store applies lands on the build's own file too — leaving
    /// it read-only would break the next write to that output.
    #[cfg(unix)]
    #[test]
    fn dropping_a_hardlinked_temp_restores_the_source_writable() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("out.rlib");
        fs::write(&source, b"shared-inode-bytes").unwrap();
        let tmp = dir.path().join("stage.tmp");
        fs::hard_link(&source, &tmp).unwrap();
        set_blob_readonly(&tmp);
        assert_eq!(
            fs::metadata(&source).unwrap().permissions().mode() & 0o200,
            0,
            "precondition: the shared inode is read-only through both names"
        );

        Store::drop_tmp_restore_source(&source, &tmp);

        assert!(!tmp.exists(), "the staging temp must be gone");
        assert_eq!(
            fs::metadata(&source).unwrap().permissions().mode() & 0o200,
            0o200,
            "the build output must be writable again once the temp is gone"
        );
    }

    /// One rename attempt: a winner in place is a lost race whatever the error
    /// says, because Windows reports a read-only winner with the same code as
    /// a delete-pending name (#1128). Anything else goes back to the retry.
    #[test]
    fn publish_attempt_outcome_lets_the_destination_decide() {
        let denied = || Err(std::io::Error::from_raw_os_error(5));
        assert_eq!(
            publish_attempt_outcome(Ok(()), || unreachable!("a clean rename needs no probe"))
                .unwrap(),
            PublishRename::Published
        );
        assert_eq!(
            publish_attempt_outcome(denied(), || PublishDest::File).unwrap(),
            PublishRename::LostRace
        );
        for dest in [PublishDest::Vacant, PublishDest::Obstructed] {
            let err = publish_attempt_outcome(denied(), || dest).unwrap_err();
            assert_eq!(err.raw_os_error(), Some(5), "the rename error is kept");
        }
    }

    /// Every (destination, transient) pair a spent publish can end on.
    #[test]
    fn publish_failure_outcome_covers_every_interleaving() {
        let settle = |transient, dest| {
            publish_failure_outcome(std::io::Error::from_raw_os_error(5), transient, dest)
        };
        for transient in [true, false] {
            assert_eq!(
                settle(transient, PublishDest::File).unwrap(),
                PublishRename::LostRace,
                "a winner that landed after the last attempt still counts"
            );
            assert!(
                settle(transient, PublishDest::Obstructed).is_err(),
                "no race leaves a non-file at a blob path"
            );
        }
        assert_eq!(
            settle(true, PublishDest::Vacant).unwrap(),
            PublishRename::Deferred,
            "removed under the publisher: the locked phase publishes"
        );
        let err = settle(false, PublishDest::Vacant).unwrap_err();
        assert!(format!("{err:#}").contains("publishing staged blob"));
        assert_eq!(
            err.root_cause().to_string(),
            std::io::Error::from_raw_os_error(5).to_string()
        );
    }

    #[test]
    fn publish_dest_state_tells_a_file_from_a_directory_from_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("blob");
        fs::write(&file, b"x").unwrap();
        assert_eq!(publish_dest_state(&file), PublishDest::File);
        assert_eq!(publish_dest_state(dir.path()), PublishDest::Obstructed);
        assert_eq!(
            publish_dest_state(&dir.path().join("absent")),
            PublishDest::Vacant
        );
    }

    /// Drive `publish_rename` with a scripted rename and destination probe.
    /// Rename attempt `n` succeeds when `rename_ok(n)`; otherwise it fails with
    /// ERROR_ACCESS_DENIED and the probe reports `dest(n)`. Returns the outcome
    /// and how many renames ran.
    fn run_publish_rename(
        rename_ok: impl Fn(u32) -> bool,
        dest: impl Fn(u32) -> PublishDest,
        transient: bool,
    ) -> (Result<PublishRename>, u32) {
        let calls = std::cell::Cell::new(0u32);
        let outcome = publish_rename(
            || {
                let n = calls.get();
                calls.set(n + 1);
                if rename_ok(n) {
                    Ok(())
                } else {
                    Err(std::io::Error::from_raw_os_error(5))
                }
            },
            || dest(calls.get() - 1),
            |_| transient,
        );
        (outcome, calls.get())
    }

    #[test]
    fn publish_rename_publishes_on_a_clean_rename() {
        let (outcome, calls) = run_publish_rename(|_| true, |_| PublishDest::Vacant, true);
        assert_eq!(outcome.unwrap(), PublishRename::Published);
        assert_eq!(calls, 1);
    }

    /// The read-only winner: the rename fails with a "transient" code, but the
    /// blob is there. No retry, no sleep.
    #[test]
    fn publish_rename_settles_at_once_on_a_present_winner() {
        let (outcome, calls) = run_publish_rename(|_| false, |_| PublishDest::File, true);
        assert_eq!(outcome.unwrap(), PublishRename::LostRace);
        assert_eq!(calls, 1, "a present winner must not burn the retry budget");
    }

    /// The winner was removed between the failed rename and the probe: the
    /// name is free, so the next attempt publishes.
    #[test]
    fn publish_rename_retries_into_a_name_a_remover_freed() {
        let (outcome, calls) = run_publish_rename(|n| n == 1, |_| PublishDest::Vacant, true);
        assert_eq!(outcome.unwrap(), PublishRename::Published);
        assert_eq!(calls, 2);
    }

    /// A winner that appears while a delete-pending name is waited out.
    #[test]
    fn publish_rename_stops_retrying_once_a_winner_appears() {
        let dest = |n: u32| {
            if n < 2 {
                PublishDest::Vacant
            } else {
                PublishDest::File
            }
        };
        let (outcome, calls) = run_publish_rename(|_| false, dest, true);
        assert_eq!(outcome.unwrap(), PublishRename::LostRace);
        assert_eq!(calls, 3);
    }

    /// #1128: every attempt fails with ERROR_ACCESS_DENIED and the name is
    /// vacant after the last one, because a remover took the winner. That is
    /// not a failed put.
    #[test]
    fn publish_rename_defers_when_the_budget_ends_on_a_vacant_name() {
        let (outcome, calls) = run_publish_rename(|_| false, |_| PublishDest::Vacant, true);
        assert_eq!(outcome.unwrap(), PublishRename::Deferred);
        assert_eq!(calls, crate::atomic::TRANSIENT_ATTEMPTS);
    }

    #[test]
    fn publish_rename_fails_on_a_settled_error_or_an_obstructed_name() {
        let (outcome, calls) = run_publish_rename(|_| false, |_| PublishDest::Vacant, false);
        let err = outcome.unwrap_err();
        assert!(format!("{err:#}").contains("publishing staged blob"));
        assert_eq!(err.root_cause().to_string(), {
            std::io::Error::from_raw_os_error(5).to_string()
        });
        assert_eq!(calls, 1, "a settled error is not retried");

        let (outcome, calls) = run_publish_rename(|_| false, |_| PublishDest::Obstructed, true);
        assert!(outcome.is_err(), "a directory at the blob path is a fault");
        assert_eq!(calls, crate::atomic::TRANSIENT_ATTEMPTS);
    }

    /// The real rename onto a read-only winner, past the early `is_file`
    /// check the way a lost race gets there. Unix replaces the identical
    /// blob; Windows refuses with ERROR_ACCESS_DENIED. Either way one rename
    /// settles it and the winner's bytes stay.
    #[test]
    fn publish_rename_onto_a_read_only_winner_settles_in_one_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let blob = dir.path().join("blob");
        let staged = dir.path().join("staged");
        fs::write(&blob, b"same-bytes").unwrap();
        fs::write(&staged, b"same-bytes").unwrap();
        set_blob_readonly(&blob);

        let calls = std::cell::Cell::new(0u32);
        let outcome = publish_rename(
            || {
                calls.set(calls.get() + 1);
                fs::rename(&staged, &blob)
            },
            || publish_dest_state(&blob),
            crate::atomic::is_transient_rename_error,
        )
        .unwrap();
        assert_ne!(outcome, PublishRename::Deferred);
        assert_eq!(calls.get(), 1);
        assert_eq!(fs::read(&blob).unwrap(), b"same-bytes");
        unlink_blob(&blob);
        unlink_blob(&staged);
    }

    /// The Windows fact #1128 rests on: renaming onto a read-only file is
    /// refused with ERROR_ACCESS_DENIED, the code the retry treats as
    /// transient. If this stops holding, revisit `publish_attempt_outcome`.
    #[cfg(windows)]
    #[test]
    fn rename_onto_a_read_only_file_is_access_denied_on_windows() {
        let dir = tempfile::tempdir().unwrap();
        let blob = dir.path().join("blob");
        let staged = dir.path().join("staged");
        fs::write(&blob, b"winner").unwrap();
        fs::write(&staged, b"loser").unwrap();
        set_blob_readonly(&blob);

        let err = fs::rename(&staged, &blob).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(5));
        assert!(crate::atomic::is_transient_rename_error(&err));
        assert_eq!(fs::read(&blob).unwrap(), b"winner");
        unlink_blob(&blob);
    }

    /// A deferred publish leaves nothing behind and reports no error; the
    /// put's locked phase then puts the blob in place from the source.
    #[test]
    fn deferred_publish_is_repaired_by_the_locked_phase() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let source = dir.path().join("out.rlib");
        fs::write(&source, b"removed-under-the-publisher").unwrap();
        let (staged, _ingest) = store.stage_blob_from_source(&source, false).unwrap();
        let hash = crate::file_hash::hash_file(&staged).unwrap();
        let blob = store.blob_path(&hash);

        let outcome = Store::publish_staged_blob_with(
            &staged,
            &blob,
            || Err(std::io::Error::from_raw_os_error(5)),
            |_| true,
        )
        .unwrap();
        assert_eq!(outcome, PublishRename::Deferred);
        assert!(!staged.exists(), "a deferred publish discards its snapshot");
        assert!(!blob.exists());

        store
            .rematerialize_and_verify(&source, &hash, "out.rlib", false)
            .unwrap();
        assert_eq!(fs::read(&blob).unwrap(), b"removed-under-the-publisher");
    }

    #[test]
    fn failed_publish_discards_its_snapshot_and_keeps_the_error() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let source = dir.path().join("out.rlib");
        fs::write(&source, b"content").unwrap();
        let (staged, _ingest) = store.stage_blob_from_source(&source, false).unwrap();
        let blob = store.blob_path(&"f".repeat(64));

        let err = Store::publish_staged_blob_with(
            &staged,
            &blob,
            || Err(std::io::Error::other("disk on fire")),
            |_| false,
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("disk on fire"));
        assert!(!staged.exists(), "a failed publish discards its snapshot");
    }

    #[test]
    fn successful_publish_keeps_the_staged_bytes_as_the_blob() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let source = dir.path().join("out.rlib");
        fs::write(&source, b"published-bytes").unwrap();
        let (staged, _ingest) = store.stage_blob_from_source(&source, false).unwrap();
        let blob = store.blob_path(&"a".repeat(64));

        let outcome = Store::publish_staged_blob_with(
            &staged,
            &blob,
            || fs::rename(&staged, &blob),
            |_| false,
        )
        .unwrap();
        assert_eq!(outcome, PublishRename::Published);
        assert_eq!(fs::read(&blob).unwrap(), b"published-bytes");
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
        let result = free_staging_path(|n| not_a_dir.join(format!("x-{n}")));

        #[cfg(unix)]
        {
            // ENOTDIR: a real fault must surface as itself, never as an
            // exhausted-name collision, and never be skipped past.
            let err = result.unwrap_err();
            assert_ne!(
                err.kind(),
                std::io::ErrorKind::AlreadyExists,
                "a real fault must not be reported as a name collision: {err}"
            );
        }
        #[cfg(windows)]
        {
            // Windows cannot tell this fault from a free name, so the search
            // hands the candidate back and the ingest is what fails. What
            // must not happen either way is spinning through every attempt.
            let candidate = result.expect("windows reports the parent as absent");
            assert!(candidate.starts_with(&not_a_dir));
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
            !matches!(ingest, StoreIngest::Copy(_)),
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
        let hash = crate::file_hash::hash_file(&source).unwrap();

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
        let hash = crate::file_hash::hash_file(&source).unwrap();

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

    /// A published blob is read-only; flushing one must work anyway, and a
    /// blob that is gone reports `NotFound` so the flusher can tell a lost
    /// entry from a transient error.
    #[test]
    fn a_read_only_blob_flushes_and_a_missing_one_reports_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let blob = dir.path().join("blob");
        fs::write(&blob, b"artifact-bytes").unwrap();
        set_blob_readonly(&blob);
        assert!(
            fs::metadata(&blob).unwrap().permissions().readonly(),
            "the fixture must reproduce a published blob"
        );
        fsync_published_blob(&blob).expect("a published blob flushes");
        assert!(
            fs::metadata(&blob).unwrap().permissions().readonly(),
            "and stays read-only afterwards"
        );
        assert_eq!(
            fsync_published_blob(&dir.path().join("absent"))
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::NotFound
        );
    }

    /// The ingest helpers follow the flush policy of the store last opened
    /// on this thread: deferred stores skip the inline fsync, others keep it.
    #[test]
    fn blob_ingest_follows_the_last_opened_store_policy() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.deferred_durability = true;
        let _deferred = Store::open(&config).unwrap();
        assert!(
            !durable_writes_now(),
            "a deferred store skips the inline fsync"
        );
        config.deferred_durability = false;
        let _strict = Store::open(&config).unwrap();
        assert!(
            durable_writes_now(),
            "a durable store keeps the inline fsync"
        );
    }

    /// Deferred durability: a put leaves the entry pending, a hit on a
    /// pending entry verifies the bytes (a same-size corruption is caught and
    /// evicted), the probe hands a pending entry back to the writing path,
    /// the flush marks it durable, and a durable entry is served on its size
    /// check as before. With the feature off, a put is durable at once.
    #[test]
    fn deferred_durability_verifies_pending_hits_until_the_flush() {
        let _env_lock = crate::test_support::process_state_test_lock();
        let _verify = EnvVarGuard::remove("KACHE_VERIFY_RESTORES");
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.deferred_durability = true;
        let store = Store::open(&config).unwrap();
        // One source per put: a stored source may become a read-only
        // hardlink of its blob, so it cannot be rewritten for the next.
        let puts = std::cell::Cell::new(0u32);
        let put = |bytes: &[u8]| {
            puts.set(puts.get() + 1);
            let output_file = dir.path().join(format!("out-{}.rlib", puts.get()));
            fs::write(&output_file, bytes).unwrap();
            store
                .put(
                    "pending_key",
                    "pending_crate",
                    &["lib".to_string()],
                    &[],
                    "x86_64-unknown-linux-gnu",
                    "dev",
                    &[(output_file.clone(), "libout.rlib".to_string())],
                    "",
                    "",
                )
                .unwrap()
        };
        let corrupt_same_size = |blob: &Path| {
            let mut perms = fs::metadata(blob).unwrap().permissions();
            perms.set_readonly(false);
            fs::set_permissions(blob, perms).unwrap();
            let mut bytes = fs::read(blob).unwrap();
            bytes[0] ^= 0xff;
            fs::write(blob, bytes).unwrap();
        };

        put(b"artifact-bytes-one");
        assert_eq!(store.pending_durability().unwrap(), 1);
        let ro = open_index_db_readonly(&config.index_db_path()).unwrap();
        assert!(matches!(
            probe_entry_readonly(&ro, &config.store_dir(), "pending_key"),
            ProbeOutcome::Fallback("entry pending durability")
        ));
        let meta = store
            .get("pending_key")
            .unwrap()
            .expect("intact pending entry hits");
        let blob = store.blob_path(&meta.files[0].hash);
        corrupt_same_size(&blob);
        assert!(
            store.get("pending_key").unwrap().is_none(),
            "a pending entry whose bytes changed is evicted, not served"
        );
        assert!(!store.contains("pending_key"));

        put(b"artifact-bytes-two");
        assert_eq!(store.pending_durability().unwrap(), 1);
        assert_eq!(store.flush_durability(10).unwrap(), 1);
        assert_eq!(store.pending_durability().unwrap(), 0);
        assert_eq!(
            store.flush_durability(10).unwrap(),
            0,
            "nothing left to flush"
        );
        assert!(!store.flush_entry_durability("pending_key").unwrap());
        assert!(matches!(
            probe_entry_readonly(&ro, &config.store_dir(), "pending_key"),
            ProbeOutcome::Hit(_)
        ));
        let meta = store.get("pending_key").unwrap().unwrap();
        corrupt_same_size(&store.blob_path(&meta.files[0].hash));
        assert!(
            store.get("pending_key").unwrap().is_some(),
            "a durable entry keeps the size-only check the verification policy asks for"
        );

        // A pending entry whose blob vanished is evicted by the flush.
        store.remove_entry("pending_key").unwrap();
        put(b"artifact-bytes-three");
        let meta = store.get("pending_key").unwrap().unwrap();
        let blob = store.blob_path(&meta.files[0].hash);
        let mut perms = fs::metadata(&blob).unwrap().permissions();
        perms.set_readonly(false);
        fs::set_permissions(&blob, perms).unwrap();
        fs::remove_file(&blob).unwrap();
        assert_eq!(store.flush_durability(10).unwrap(), 0);
        assert!(
            !store.contains("pending_key"),
            "evicted rather than marked durable"
        );

        // Feature off: the put is durable inside the compile.
        config.deferred_durability = false;
        let strict = Store::open(&config).unwrap();
        let strict_file = dir.path().join("out-strict.rlib");
        fs::write(&strict_file, b"artifact-bytes-four").unwrap();
        strict
            .put(
                "strict_key",
                "strict_crate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(strict_file.clone(), "libout.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        assert_eq!(strict.pending_durability().unwrap(), 0);
        assert!(strict.try_durability_flush_lock().unwrap().is_some());
    }

    /// The read-only probe (#565) must mirror `Store::get`'s servable/hit
    /// decision without any write side effect: a committed, blob-complete
    /// entry probes `Hit` and leaves `hit_count` untouched; an unknown key is
    /// an authoritative `Miss`; anything needing repair probes `Fallback`.
    #[test]
    fn probe_entry_readonly_hit_miss_fallback() {
        let _env_lock = crate::test_support::process_state_test_lock();
        let _verify = EnvVarGuard::remove("KACHE_VERIFY_RESTORES");
        for damage in ["missing", "short", "long", "directory"] {
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
            let hit_count = || {
                store
                    .db
                    .query_row(
                        "SELECT hit_count FROM entries WHERE cache_key = 'probe_key'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap()
            };

            let meta = match probe_entry_readonly(&ro, &store_dir, "probe_key") {
                ProbeOutcome::Hit(meta) => meta,
                other => panic!("expected hit, got {other:?}"),
            };
            assert_eq!(meta.cache_key, "probe_key");
            assert_eq!(meta.stdout, "out");
            assert_eq!(meta.stderr, "err");
            assert_eq!(meta.files.len(), 1);
            assert_eq!(
                hit_count(),
                0,
                "the probe must leave accounting to the pin writer"
            );

            // A hit re-stamps an entry only once per `HIT_STAMP_INTERVAL`;
            // the put just stamped it, so age it first.
            store.set_last_accessed_for_test("probe_key", "-1 minutes");
            let local = store.get("probe_key").unwrap().unwrap();
            assert_eq!(local.files, meta.files);
            assert_eq!(local.stdout, meta.stdout);
            assert_eq!(local.stderr, meta.stderr);
            assert_eq!(hit_count(), 1, "get must record the hit");
            let _ = store.get("probe_key").unwrap().unwrap();
            assert_eq!(hit_count(), 1, "a fresh stamp is not rewritten");

            assert!(matches!(
                probe_entry_readonly(&ro, &store_dir, "no_such_key"),
                ProbeOutcome::Miss
            ));

            let blob = store.blob_path(&meta.files[0].hash);
            let mut perms = fs::metadata(&blob).unwrap().permissions();
            perms.set_readonly(false);
            fs::set_permissions(&blob, perms).unwrap();
            fs::remove_file(&blob).unwrap();
            match damage {
                "missing" => {}
                "short" => fs::write(&blob, b"short").unwrap(),
                "long" => fs::write(&blob, b"longer than the original artifact").unwrap(),
                "directory" => fs::create_dir(&blob).unwrap(),
                _ => unreachable!(),
            }

            assert!(
                matches!(
                    probe_entry_readonly(&ro, &store_dir, "probe_key"),
                    ProbeOutcome::Fallback("blob missing or size mismatch")
                ),
                "{damage}"
            );
            assert!(
                store.contains("probe_key"),
                "the probe must not evict: {damage}"
            );
            assert_eq!(
                hit_count(),
                1,
                "a fallback must not count as a hit: {damage}"
            );
            assert!(store.get("probe_key").unwrap().is_none(), "{damage}");
            assert!(!store.contains("probe_key"), "get must evict: {damage}");
        }
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
            cache_dir: dir.to_path_buf(),
            max_size: 1024 * 1024,
            gc_evict_shared: false,
            upload_spool_max_jobs: 65_536,
            deferred_durability: false,
        }
    }

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<std::ffi::OsString>,
    }

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
    fn file_hash_records_persist_and_reject_changed_fingerprints() {
        use crate::file_hash::{FileFingerprint, FileHashLookup};

        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let artifact = dir.path().join("artifact.rlib");
        fs::write(&artifact, vec![7; 65_536]).unwrap();
        let fingerprint = FileFingerprint::from_path(&artifact).unwrap();
        assert!(matches!(
            store.file_hash_lookup(&artifact),
            FileHashLookup::NeedsHash(_)
        ));

        store.file_hash_record(&fingerprint, "recorded");
        assert!(
            matches!(store.file_hash_lookup(&artifact), FileHashLookup::Hit(hash) if hash == "recorded")
        );
        store.record_verified_file_hash(&fingerprint, "verified");
        assert!(
            matches!(store.file_hash_lookup(&artifact), FileHashLookup::Hit(hash) if hash == "verified")
        );
        let second = dir.path().join("second.rlib");
        fs::write(&second, vec![9; 65_536]).unwrap();
        let second_fingerprint = FileFingerprint::from_path(&second).unwrap();
        store.record_verified_file_hashes(&[
            (fingerprint.clone(), "batched"),
            (second_fingerprint, "batched-second"),
        ]);
        assert!(
            matches!(store.file_hash_lookup(&artifact), FileHashLookup::Hit(hash) if hash == "batched")
        );
        assert!(
            matches!(store.file_hash_lookup(&second), FileHashLookup::Hit(hash) if hash == "batched-second")
        );
        store.record_verified_file_hashes(&[]);
        drop(store);

        let store = Store::open(&config).unwrap();
        assert!(
            matches!(store.file_hash_lookup(&artifact), FileHashLookup::Hit(hash) if hash == "batched")
        );
        fs::write(&artifact, vec![8; 65_537]).unwrap();
        assert!(matches!(
            store.file_hash_lookup(&artifact),
            FileHashLookup::NeedsHash(_)
        ));
    }

    #[test]
    fn readonly_blob_detection_requires_a_shared_inode() {
        let dir = tempfile::tempdir().unwrap();
        let store_dir = dir.path().join("store");
        let output = dir.path().join("output.o");
        fs::write(&output, b"legacy compiler output").unwrap();
        let writable = fs::metadata(&output).unwrap().permissions();
        assert_eq!(
            Store::matching_readonly_blob_inode(&store_dir, &output).unwrap(),
            None
        );

        let hash = crate::file_hash::hash_file(&output).unwrap();
        let blob = blob_path_in_store_dir(&store_dir, &hash);
        fs::create_dir_all(blob.parent().unwrap()).unwrap();
        fs::hard_link(&output, &blob).unwrap();
        let mut readonly = writable.clone();
        readonly.set_readonly(true);
        fs::set_permissions(&output, readonly).unwrap();
        assert_eq!(
            Store::matching_readonly_blob_inode(&store_dir, &output).unwrap(),
            Some(blob)
        );

        let independent = dir.path().join("independent.o");
        fs::copy(&output, &independent).unwrap();
        assert!(fs::metadata(&independent).unwrap().permissions().readonly());
        assert_eq!(
            Store::matching_readonly_blob_inode(&store_dir, &independent).unwrap(),
            None
        );
        fs::set_permissions(&output, writable.clone()).unwrap();
        fs::set_permissions(&independent, writable).unwrap();
    }

    #[test]
    fn put_records_known_hash_only_for_stable_outputs() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path())).unwrap();
        let artifact = dir.path().join("artifact.rlib");
        std::fs::write(&artifact, vec![b'x'; 64 * 1024]).unwrap();
        let expected = crate::file_hash::hash_file(&artifact).unwrap();

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
        match store.file_hash_lookup(&artifact) {
            crate::file_hash::FileHashLookup::Hit(actual) => assert_eq!(actual, expected),
            _ => panic!("publication should seed the persistent file hash"),
        }

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
            crate::file_hash::FileHashLookup::NeedsHash(_)
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
            crate::file_hash::FileHashLookup::NeedsHash(_)
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
        let _ = std::fs::remove_file(dir.join(format!("out-{seed}.rlib")));
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
    fn rebuild_index_validates_artifact_names_and_hashes_independently() {
        for (name, invalid_hash, accepted) in [
            ("foo.rlib", false, true),
            ("../escape", false, false),
            ("foo.rlib", true, false),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let config = test_config(dir.path());
            let key = {
                let store = Store::open(&config).unwrap();
                put_entry(&store, dir.path(), 30, "foo", b"compiled artifact")
            };
            let meta_path = config.store_dir().join(&key).join("meta.json");
            let mut meta: EntryMeta =
                serde_json::from_slice(&fs::read(&meta_path).unwrap()).unwrap();
            meta.files[0].name = name.into();
            if invalid_hash {
                let original = blob_path_in_store_dir(&config.store_dir(), &meta.files[0].hash);
                meta.files[0].hash = "g".repeat(64);
                let malformed = blob_path_in_store_dir(&config.store_dir(), &meta.files[0].hash);
                fs::create_dir_all(malformed.parent().unwrap()).unwrap();
                // Keep the blob present and correctly sized: only validation
                // of its hash spelling may reject this entry.
                fs::copy(original, malformed).unwrap();
            }
            fs::write(meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();
            fs::remove_file(config.index_db_path()).unwrap();

            let store = Store::open(&config).unwrap();
            let stats = store.rebuild_index_from_store().unwrap();
            assert_eq!(
                (
                    stats.entries_rebuilt,
                    stats.entries_skipped,
                    stats.blobs_registered
                ),
                if accepted { (1, 0, 1) } else { (0, 1, 0) },
                "name={name:?}, invalid_hash={invalid_hash}"
            );
            assert_eq!(store.contains(&key), accepted);
        }
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
                &[(output_file.clone(), "libbig.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        let _ = std::fs::remove_file(&output_file);

        let recent = store.evict().unwrap();
        assert_eq!(recent.entries_recent_prefiltered, 1);
        assert_eq!(recent.entries_pinned, 1);
        assert_eq!(recent.entries_evicted, 0);

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

    /// Put one 200-byte entry into a 100-byte store and age it past the pin
    /// grace, so the next size eviction selects it.
    fn store_with_one_evictable_entry(dir: &Path, key: &str) -> (Store, Config) {
        let mut config = test_config(dir);
        config.max_size = 100;
        let store = Store::open(&config).unwrap();
        let output_file = dir.join("big.rlib");
        std::fs::write(&output_file, vec![0u8; 200]).unwrap();
        store
            .put(
                key,
                "big_crate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(output_file.clone(), "libbig.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        let _ = std::fs::remove_file(&output_file);
        store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour') WHERE cache_key = ?1",
                params![key],
            )
            .unwrap();
        (store, config)
    }

    #[test]
    fn evict_counts_an_entry_it_fails_to_remove() {
        let dir = tempfile::tempdir().unwrap();
        let (store, _config) = store_with_one_evictable_entry(dir.path(), "broken");
        std::fs::write(store.entry_dir("broken").join("meta.json"), b"{not json").unwrap();

        let stats = store.evict().unwrap();

        assert_eq!(stats.entries_evicted, 0);
        assert_eq!(
            stats.entries_failed, 1,
            "the refused removal is counted: {stats:?}"
        );
        assert_eq!(stats.entries_locked, 0, "bad data is not lock contention");
        assert_eq!(store.entry_count().unwrap(), 1);
    }

    #[test]
    fn evict_counts_lock_contention_apart_from_bad_data() {
        let dir = tempfile::tempdir().unwrap();
        let (store, config) = store_with_one_evictable_entry(dir.path(), "busy");
        // A second writer holding the database, as a live build does when it
        // records a hit; the removal waits out the busy timeout and fails.
        let blocker = Connection::open(config.index_db_path()).unwrap();
        blocker.execute_batch("BEGIN EXCLUSIVE").unwrap();

        let stats = store.evict().unwrap();
        blocker.execute_batch("ROLLBACK").unwrap();

        assert_eq!(stats.entries_evicted, 0);
        assert_eq!(stats.entries_failed, 1, "{stats:?}");
        assert_eq!(
            stats.entries_locked, 1,
            "contention is counted as locked: {stats:?}"
        );
        // The removal that lost the lock fails at once instead of waiting out
        // the store's 5 s busy timeout, so this run's eviction write time
        // stays small; the lost-lock count above is the figure to watch.
    }

    #[test]
    fn is_sqlite_contention_matches_busy_and_locked_only() {
        let sqlite = |code: i32| {
            anyhow::Error::from(SqlError::SqliteFailure(
                rusqlite::ffi::Error::new(code),
                None,
            ))
            .context("removing entry")
        };
        assert!(is_sqlite_contention(&sqlite(rusqlite::ffi::SQLITE_BUSY)));
        assert!(is_sqlite_contention(&sqlite(rusqlite::ffi::SQLITE_LOCKED)));
        assert!(is_sqlite_busy_snapshot(&sqlite(
            rusqlite::ffi::SQLITE_BUSY_SNAPSHOT
        )));
        assert!(!is_sqlite_busy_snapshot(&sqlite(
            rusqlite::ffi::SQLITE_BUSY
        )));
        assert!(!is_sqlite_busy_snapshot(&sqlite(
            rusqlite::ffi::SQLITE_LOCKED
        )));
        assert!(!is_sqlite_contention(&sqlite(
            rusqlite::ffi::SQLITE_CORRUPT
        )));
        assert!(!is_sqlite_contention(&anyhow::anyhow!(
            "meta.json unparseable"
        )));

        let mut stats = GcStats::default();
        record_eviction_failure(&mut stats, &sqlite(rusqlite::ffi::SQLITE_BUSY_SNAPSHOT));
        record_eviction_failure(&mut stats, &sqlite(rusqlite::ffi::SQLITE_BUSY));
        record_eviction_failure(&mut stats, &anyhow::anyhow!("meta.json unparseable"));
        assert_eq!(stats.entries_failed, 3);
        assert_eq!(stats.entries_locked, 2);
        assert_eq!(stats.entries_busy_snapshot, 1);
    }

    /// Not `#[cfg(unix)]`: NTFS has hardlinks, `cache.windows_hardlink` and
    /// `cache.shared_hardlink_restores` make them, so the #725 guard has to
    /// hold there too. Gating this test to Unix is how the guard stayed
    /// compiled out on Windows.
    #[test]
    fn evict_leaves_an_entry_whose_blob_is_still_hardlinked_outside() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 100;
        let mut store = Store::open(&config).unwrap();

        let output_file = dir.path().join("big.rlib");
        std::fs::write(&output_file, vec![0u8; 200]).unwrap();
        store
            .put(
                "kept",
                "big_crate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(output_file.clone(), "libbig.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        let _ = std::fs::remove_file(&output_file);

        let meta = store.get("kept").unwrap().unwrap();
        let blob = store.blob_path(&meta.files[0].hash);
        let retainer = dir.path().join("worktree-copy.rlib");
        std::fs::hard_link(&blob, &retainer).unwrap();

        store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour') WHERE cache_key = 'kept'",
                [],
            )
            .unwrap();

        let stats = store.evict().unwrap();
        assert_eq!(stats.entries_evicted, 0, "clone still holds the blocks");
        assert_eq!(
            stats.entries_unreclaimable, 1,
            "the skip must be counted as unreclaimable, not as a pin: {stats:?}"
        );
        assert!(store.contains("kept"), "entry remains restorable");
        assert!(blob.is_file(), "store name remains");

        store.config.gc_evict_shared = true;
        let stats = store.evict().unwrap();
        assert_eq!(stats.entries_evicted, 1);
        assert_eq!(stats.bytes_freed, 200);
        assert_eq!(stats.disk_bytes_reclaimed, 0);
        assert!(!store.contains("kept"));
        assert!(
            retainer.is_file(),
            "compatibility mode drops the store name, not the retained blocks"
        );
    }

    #[test]
    fn shared_entry_retention_requires_the_last_positive_reference() {
        assert!(externally_retained_last_reference(1, 1, true));
        assert!(externally_retained_last_reference(2, 2, true));
        assert!(!externally_retained_last_reference(2, 1, true));
        assert!(!externally_retained_last_reference(0, 1, true));
        assert!(!externally_retained_last_reference(1, 1, false));
    }

    #[cfg(unix)]
    #[test]
    fn evict_leaves_an_entry_whose_blob_is_still_reflinked_outside() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 100;
        let store = Store::open(&config).unwrap();
        let output_file = dir.path().join("big.rlib");
        std::fs::write(&output_file, vec![0u8; 4096]).unwrap();
        store
            .put(
                "kept-reflink",
                "big_crate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(output_file.clone(), "libbig.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        let _ = std::fs::remove_file(&output_file);

        let meta = store.get("kept-reflink").unwrap().unwrap();
        let blob = store.blob_path(&meta.files[0].hash);
        let retainer = dir.path().join("worktree-reflink.rlib");
        if crate::link::try_reflink(&blob, &retainer).is_err() {
            return;
        }
        let sharing = crate::sharing::probe(&blob, 4096);
        if !sharing.shared || sharing.private_bytes != 0 {
            return;
        }
        store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour') WHERE cache_key = 'kept-reflink'",
                [],
            )
            .unwrap();

        let stats = store.evict().unwrap();
        assert_eq!(stats.entries_evicted, 0);
        assert!(stats.entries_unreclaimable > 0);
        assert!(store.contains("kept-reflink"));

        std::fs::remove_file(&retainer).unwrap();
        let stats = store.evict().unwrap();
        assert!(stats.entries_evicted > 0);
        assert!(!store.contains("kept-reflink"));
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
                &[(pending_output.clone(), "libshared.rlib".to_string())],
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
                &[(newer_output.clone(), "libshared.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        fs::remove_file(&pending_output).unwrap();
        fs::remove_file(&newer_output).unwrap();
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
                &[(out.clone(), "libbig.rlib".to_string())],
                "",
                "",
                2500,
            )
            .unwrap();
        fs::remove_file(&out).unwrap();
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
            recently_accessed: false,
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
                &[(output_file.clone(), "libbig.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        std::fs::remove_file(&output_file).unwrap();

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
            matches!(
                store
                    .remove_entry_guarded("rk", Some(EVICTION_IDLE_GRACE))
                    .unwrap(),
                GuardedRemoval::Skipped
            ),
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
    fn target_root_registry_is_local_bounded_provenance_with_identity() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let workspace = dir.path().join("workspace");
        let target = workspace.join("target");
        std::fs::create_dir_all(&target).unwrap();
        std::fs::write(
            target.join("CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55",
        )
        .unwrap();
        std::fs::write(target.join(".rustc_info.json"), "{}").unwrap();

        store.remember_target_root(&target, &workspace).unwrap();
        store.remember_target_root(&target, &workspace).unwrap();
        let roots = store.tracked_target_roots(0).unwrap();
        assert_eq!(roots.len(), 1, "same target is upserted, not duplicated");
        assert_eq!(roots[0].path, std::path::absolute(&target).unwrap());
        assert_eq!(
            roots[0].workspace_root,
            std::path::absolute(&workspace).unwrap()
        );
        assert_eq!(
            crate::filesystem::directory_identity(&target),
            Some(roots[0].identity)
        );

        store.remember_target_root(&workspace, &workspace).unwrap();
        assert_eq!(
            store.tracked_target_roots(0).unwrap().len(),
            1,
            "a source root must never be registered as a cleanup target"
        );

        store.forget_target_root(&target).unwrap();
        assert!(store.tracked_target_roots(0).unwrap().is_empty());
    }

    #[test]
    fn target_root_registry_filters_by_last_seen() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let workspace = dir.path().join("workspace");
        let target = workspace.join("target");
        std::fs::create_dir_all(&target).unwrap();
        std::fs::write(
            target.join("CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55",
        )
        .unwrap();
        std::fs::write(target.join(".rustc_info.json"), "{}").unwrap();
        store.remember_target_root(&target, &workspace).unwrap();

        assert!(store.tracked_target_roots(24).unwrap().is_empty());
        store
            .db
            .execute(
                "UPDATE target_roots SET last_seen = unixepoch() - 90000",
                [],
            )
            .unwrap();
        assert_eq!(store.tracked_target_roots(24).unwrap().len(), 1);
    }

    #[test]
    fn target_registry_prunes_only_after_a_real_upsert() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let workspace = dir.path().join("workspace");
        let target = workspace.join("target");
        std::fs::create_dir_all(&target).unwrap();
        std::fs::write(
            target.join("CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55",
        )
        .unwrap();
        std::fs::write(target.join(".rustc_info.json"), "{}").unwrap();

        let insert_stale = |path: &str| {
            store
                .db
                .execute(
                    "INSERT INTO target_roots
                     (path, workspace_root, first_seen, last_seen, device, inode)
                     VALUES (?1, '/workspace', 0, unixepoch() - 15552001, '1', '1')",
                    params![path],
                )
                .unwrap();
        };
        let contains = |path: &str| -> bool {
            store
                .db
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM target_roots WHERE path = ?1)",
                    params![path],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap()
                != 0
        };

        insert_stale("/stale-before-write");
        store.remember_target_root(&target, &workspace).unwrap();
        assert!(!contains("/stale-before-write"));

        insert_stale("/stale-before-debounced-noop");
        store.remember_target_root(&target, &workspace).unwrap();
        assert!(contains("/stale-before-debounced-noop"));
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
            key_schema: kache_format::CACHE_KEY_VERSION,
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
    fn unlocked_pid_marker_does_not_claim_the_key() {
        // Regression for #821: PID contents are diagnostic only. A PID from a
        // different namespace must not decide whether the key is available.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let lock_path = store.entry_dir("pid-marker").with_extension("lock");
        fs::write(&lock_path, std::process::id().to_string()).unwrap();

        let lock = store.try_lock("pid-marker").unwrap();

        assert!(
            lock.is_some(),
            "an unlocked marker must not cause contention"
        );
    }

    #[test]
    fn advisory_lock_owns_key_even_with_unparseable_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let lock_path = store.entry_dir("foreign-owner").with_extension("lock");
        let mut owner = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .unwrap();
        use std::io::Write;
        write!(owner, "not-a-pid").unwrap();
        owner.lock().unwrap();

        assert!(
            store.try_lock("foreign-owner").unwrap().is_none(),
            "OS lock ownership must override PID metadata"
        );
        owner.unlock().unwrap();
        assert!(store.try_lock("foreign-owner").unwrap().is_some());
    }

    #[test]
    fn concurrent_advisory_lock_acquisition_has_one_winner() {
        const CONTENDERS: usize = 16;
        let dir = tempfile::tempdir().unwrap();

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
            "the advisory lock must admit exactly one live guard"
        );
    }

    #[test]
    fn dropping_key_lock_preserves_stable_lock_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let lock_path = store.entry_dir("stable-path").with_extension("lock");
        let lock = store.try_lock("stable-path").unwrap();

        assert!(lock.is_some());
        drop(lock);
        assert!(
            lock_path.exists(),
            "advisory lock paths must not be unlinked after release"
        );
        assert!(store.try_lock("stable-path").unwrap().is_some());
    }

    #[test]
    fn dropping_store_lock_unlocks_even_with_a_duplicated_handle() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let lock = store
            .try_lock("duplicated-handle")
            .unwrap()
            .expect("owner lock");
        let duplicate = lock.file.try_clone().unwrap();

        drop(lock);

        assert!(
            store.try_lock("duplicated-handle").unwrap().is_some(),
            "explicit unlock must release duplicate descriptors of the lock"
        );
        drop(duplicate);
    }

    #[test]
    fn process_exit_releases_advisory_key_lock() {
        let dir = tempfile::tempdir().unwrap();
        let ready = dir.path().join("lock-ready");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "store::tests::advisory_key_lock_child_fixture",
                "--ignored",
                "--nocapture",
            ])
            .env("KACHE_TEST_ADVISORY_LOCK_ROOT", dir.path())
            .spawn()
            .unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !ready.exists() && std::time::Instant::now() < deadline {
            assert!(
                child.try_wait().unwrap().is_none(),
                "lock fixture exited before acquiring the key"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(ready.exists(), "lock fixture did not become ready");

        let store = Store::open(test_config(dir.path())).unwrap();
        assert!(
            store.try_lock("crash-release").unwrap().is_none(),
            "child must own the key before it exits"
        );
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(
            store.try_lock("crash-release").unwrap().is_some(),
            "the OS must release the key lock when its process exits"
        );
    }

    #[test]
    #[ignore = "subprocess fixture for process_exit_releases_advisory_key_lock"]
    fn advisory_key_lock_child_fixture() {
        let root =
            PathBuf::from(std::env::var_os("KACHE_TEST_ADVISORY_LOCK_ROOT").expect("fixture root"));
        let store = Store::open(test_config(&root)).unwrap();
        let _lock = store
            .try_lock("crash-release")
            .unwrap()
            .expect("fixture key lock");
        fs::write(root.join("lock-ready"), b"ready").unwrap();
        std::thread::sleep(Duration::from_secs(30));
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

    /// The resolution path the #276 comments promise: a refused removal
    /// leaves the row "until a fresh put (INSERT OR REPLACE) overwrites
    /// it". That re-put must also release the stranded generation's blob
    /// references — stacking new increments on top leaks the old hashes
    /// (a refcount no mapping accounts for never reaches zero) and
    /// double-counts hashes shared by both generations.
    #[test]
    fn reput_over_refused_removal_releases_stale_refcounts() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let refcount_sum = |s: &Store| -> i64 {
            s.db.query_row("SELECT COALESCE(SUM(refcount), 0) FROM blobs", [], |r| {
                r.get(0)
            })
            .unwrap()
        };
        // Each generation compiles to a fresh source path: put ingests by
        // hardlink where reflinks are unavailable (ext4), sharing the
        // store blob's read-only inode with the source, so rewriting one
        // path across generations would EACCES on Linux while APFS
        // reflinks mask it (the #822 snapshot-test lesson).
        let put = |s: &Store, generation: &str, content: &[u8]| {
            let output = dir.path().join(format!("lib-{generation}.rlib"));
            std::fs::write(&output, content).unwrap();
            s.put(
                "strand1",
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
        };

        put(&store, "one", b"generation one");
        assert_eq!(refcount_sum(&store), 1);

        // Strand the row: meta.json gone, DB row present. Removal
        // refuses (#276), and lookup's discarded-error path then drives
        // a miss, a recompile, and this re-put over the surviving row.
        std::fs::remove_file(store.entry_dir("strand1").join("meta.json")).unwrap();
        assert!(store.remove_entry("strand1").is_err());
        put(&store, "two", b"generation two");

        assert_eq!(
            refcount_sum(&store),
            1,
            "re-put must release the stranded generation's references"
        );
        let mapped: i64 = store
            .db
            .query_row(
                "SELECT COUNT(*) FROM entry_blobs WHERE cache_key = 'strand1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(mapped, 1, "exactly the new generation's mapping remains");
        assert_eq!(
            store.blob_index_drift().unwrap().total(),
            0,
            "index must match the committed meta with no doctor repair"
        );

        // Same-content re-put: the shared hash must stay at one
        // reference, not accumulate one per generation.
        put(&store, "two-again", b"generation two");
        assert_eq!(refcount_sum(&store), 1, "shared hash must not double-count");
        assert_eq!(store.blob_index_drift().unwrap().total(), 0);
    }

    /// Every blob's refcount equals the references its mappings hold, no
    /// blob row exists without a mapping, and no mapping names a blob that
    /// has no row. Pure SQL, so it also holds a store whose meta.json
    /// files are unreadable to account.
    fn assert_blob_refs_match_mappings(store: &Store) {
        let mut stmt = store
            .db
            .prepare(
                "SELECT b.hash, b.refcount,
                        COALESCE((SELECT SUM(refs) FROM entry_blobs e WHERE e.hash = b.hash), 0)
                 FROM blobs b
                 UNION ALL
                 SELECT e.hash, 0, e.refs FROM entry_blobs e
                 WHERE NOT EXISTS (SELECT 1 FROM blobs b WHERE b.hash = e.hash)",
            )
            .unwrap();
        let drifted: Vec<String> = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })
            .unwrap()
            .map(Result::unwrap)
            .filter(|(_, refcount, mapped)| refcount != mapped || *mapped == 0)
            .map(|(hash, refcount, mapped)| {
                // Too low is the dangerous direction: removing one entry
                // reclaims a blob another still needs.
                let direction = if refcount < mapped {
                    "TOO LOW"
                } else {
                    "too high"
                };
                format!(
                    "{direction}: {} refcount {refcount}, mapped refs {mapped}",
                    &hash[..8.min(hash.len())]
                )
            })
            .collect();
        assert!(drifted.is_empty(), "blob index drift: {drifted:#?}");
    }

    /// Put `key` with one output per `(store_name, content)` pair. Sources
    /// get a fresh path per `generation` (see the #822 note above).
    fn put_outputs(
        store: &Store,
        dir: &Path,
        key: &str,
        generation: &str,
        outputs: &[(&str, &[u8])],
    ) {
        let files: Vec<(PathBuf, String)> = outputs
            .iter()
            .map(|(name, content)| {
                let source = dir.join(format!("{key}-{generation}-{name}"));
                std::fs::write(&source, content).unwrap();
                (source, name.to_string())
            })
            .collect();
        store
            .put(key, "c1", &["lib".into()], &[], "", "dev", &files, "", "")
            .unwrap();
    }

    /// Lay out `key` the way a remote download extracts it: meta.json plus
    /// one artifact per file inside the entry directory, nothing in the DB.
    fn extract_download(store: &Store, key: &str, outputs: &[(&str, &[u8])]) -> EntryMeta {
        let entry_dir = store.entry_dir(key);
        std::fs::create_dir_all(&entry_dir).unwrap();
        let files = outputs
            .iter()
            .map(|(name, content)| {
                std::fs::write(entry_dir.join(name), content).unwrap();
                CachedFile {
                    name: name.to_string(),
                    size: content.len() as u64,
                    hash: blake3::hash(content).to_hex().to_string(),
                    executable: false,
                }
            })
            .collect();
        let meta = EntryMeta {
            cache_key: key.to_string(),
            key_schema: kache_format::CACHE_KEY_VERSION,
            crate_name: "c1".to_string(),
            crate_types: vec!["lib".to_string()],
            files,
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
        meta
    }

    fn content_hash(content: &[u8]) -> String {
        blake3::hash(content).to_hex().to_string()
    }

    #[test]
    fn reput_with_identical_outputs_keeps_each_refcount_at_one() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path())).unwrap();
        let outputs: [(&str, &[u8]); 2] = [("a.rlib", b"blob A"), ("b.rmeta", b"blob B")];

        put_outputs(&store, dir.path(), "reput_same", "one", &outputs);
        put_outputs(&store, dir.path(), "reput_same", "two", &outputs);

        assert_eq!(blob_refcount(&store, &content_hash(b"blob A")), Some(1));
        assert_eq!(blob_refcount(&store, &content_hash(b"blob B")), Some(1));
        assert_blob_refs_match_mappings(&store);
    }

    #[test]
    fn reput_with_different_outputs_releases_only_the_dropped_blob() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path())).unwrap();

        put_outputs(
            &store,
            dir.path(),
            "reput_diff",
            "one",
            &[("a.rlib", b"blob A"), ("b.rmeta", b"blob B")],
        );
        put_outputs(
            &store,
            dir.path(),
            "reput_diff",
            "two",
            &[("a.rlib", b"blob A"), ("b.rmeta", b"blob C")],
        );

        assert_eq!(blob_refcount(&store, &content_hash(b"blob A")), Some(1));
        assert_eq!(blob_refcount(&store, &content_hash(b"blob B")), None);
        assert_eq!(blob_refcount(&store, &content_hash(b"blob C")), Some(1));
        assert_blob_refs_match_mappings(&store);

        // B has no row left, so the orphan sweep may take its file; A and C
        // leave with the entry.
        store.remove_entry("reput_diff").unwrap();
        assert_eq!(blob_table_count(&store), 0);
        assert!(!store.blob_path(&content_hash(b"blob A")).exists());
        assert!(!store.blob_path(&content_hash(b"blob C")).exists());
    }

    #[test]
    fn reput_of_one_key_leaves_a_shared_blob_at_two() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path())).unwrap();
        let shared: (&str, &[u8]) = ("a.rlib", b"shared blob");

        put_outputs(&store, dir.path(), "share_one", "one", &[shared]);
        put_outputs(&store, dir.path(), "share_two", "one", &[shared]);
        put_outputs(&store, dir.path(), "share_one", "two", &[shared]);

        assert_eq!(
            blob_refcount(&store, &content_hash(b"shared blob")),
            Some(2)
        );
        assert_blob_refs_match_mappings(&store);

        store.remove_entry("share_one").unwrap();
        assert_eq!(
            blob_refcount(&store, &content_hash(b"shared blob")),
            Some(1)
        );
        store.remove_entry("share_two").unwrap();
        assert_eq!(blob_table_count(&store), 0);
    }

    /// A second download of a key that is already committed (two daemons, a
    /// retried prefetch) must not add a second set of references.
    #[test]
    fn reimport_over_a_committed_entry_is_refcount_neutral() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path())).unwrap();
        let outputs: [(&str, &[u8]); 2] = [("a.rlib", b"blob A"), ("b.rmeta", b"blob B")];

        extract_download(&store, "reimport", &outputs);
        store.import_downloaded_entry("reimport").unwrap();
        extract_download(&store, "reimport", &outputs);
        store.import_downloaded_entry("reimport").unwrap();

        assert_eq!(blob_refcount(&store, &content_hash(b"blob A")), Some(1));
        assert_eq!(blob_refcount(&store, &content_hash(b"blob B")), Some(1));
        assert_blob_refs_match_mappings(&store);

        store.remove_entry("reimport").unwrap();
        assert_eq!(blob_table_count(&store), 0, "eviction must free both blobs");
    }

    /// The same key can carry different bytes on the remote (a
    /// non-reproducible output). The replaced generation's blob must be
    /// released, the shared one kept at one reference.
    #[test]
    fn reimport_with_different_files_releases_the_replaced_generation() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path())).unwrap();

        extract_download(
            &store,
            "reimport_diff",
            &[("a.rlib", b"blob A"), ("b.rmeta", b"blob B")],
        );
        store.import_downloaded_entry("reimport_diff").unwrap();
        put_outputs(&store, dir.path(), "other", "one", &[("a.rlib", b"blob A")]);
        extract_download(
            &store,
            "reimport_diff",
            &[("a.rlib", b"blob A"), ("b.rmeta", b"blob C")],
        );
        store.import_downloaded_entry("reimport_diff").unwrap();

        assert_eq!(blob_refcount(&store, &content_hash(b"blob A")), Some(2));
        assert_eq!(blob_refcount(&store, &content_hash(b"blob B")), None);
        assert_eq!(blob_refcount(&store, &content_hash(b"blob C")), Some(1));
        assert_blob_refs_match_mappings(&store);
    }

    /// A daemon killed between extraction and import leaves meta.json and
    /// artifacts with no entry row. The startup migration must not read that
    /// as a legacy entry: registering its blobs gives them a refcount that no
    /// entry, and so no eviction, can ever release.
    #[test]
    fn startup_migration_leaves_an_unregistered_extraction_alone() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path())).unwrap();
        extract_download(&store, "abandoned", &[("a.rlib", b"blob A")]);

        let stats = store.migrate_to_blobs(|_, _| {}).unwrap();

        assert_eq!(stats.entries_scanned, 1);
        assert_eq!(stats.entries_migrated, 0);
        assert_eq!(stats.entries_skipped, 1);
        assert_eq!(blob_table_count(&store), 0);
        assert!(store.entry_dir("abandoned").join("a.rlib").is_file());
        assert_blob_refs_match_mappings(&store);

        // The retried download still imports, at one reference.
        store.import_downloaded_entry("abandoned").unwrap();
        assert_eq!(blob_refcount(&store, &content_hash(b"blob A")), Some(1));
        assert_blob_refs_match_mappings(&store);
    }

    /// An uncommitted row is no more an owner than a missing one.
    #[test]
    fn migration_skips_an_uncommitted_entry() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path())).unwrap();
        let meta = extract_download(&store, "uncommitted", &[("a.rlib", b"blob A")]);
        store
            .db
            .execute(
                "INSERT INTO entries (cache_key, crate_name, size, committed)
                 VALUES ('uncommitted', 'c1', 6, 0)",
                [],
            )
            .unwrap();

        assert!(!store.migrate_entry_to_blobs(&meta).unwrap());

        assert_eq!(blob_table_count(&store), 0);
        assert!(store.entry_dir("uncommitted").join("a.rlib").is_file());
    }

    /// Register `key` as a committed legacy entry: artifacts beside
    /// meta.json, an entry row, no blob rows and no mapping.
    fn legacy_entry(store: &Store, key: &str, outputs: &[(&str, &[u8])]) -> EntryMeta {
        let meta = extract_download(store, key, outputs);
        store
            .db
            .execute(
                "INSERT INTO entries (cache_key, crate_name, size, committed)
                 VALUES (?1, 'c1', 1, 1)",
                params![key],
            )
            .unwrap();
        meta
    }

    /// Backfill must not map an entry whose artifacts are still in its
    /// directory: those references were never counted, and a mapping would
    /// claim they were.
    #[test]
    fn backfill_skips_an_entry_with_unmigrated_artifacts() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path())).unwrap();
        let meta = legacy_entry(
            &store,
            "legacy_unmigrated",
            &[("a.rlib", b"blob A"), ("b.rmeta", b"blob B")],
        );
        // One artifact left is enough.
        fs::remove_file(store.entry_dir("legacy_unmigrated").join("a.rlib")).unwrap();

        assert_eq!(store.backfill_entry_blobs().unwrap(), 0);
        let mapped: i64 = store
            .db
            .query_row("SELECT COUNT(*) FROM entry_blobs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(mapped, 0);

        // Migration counts the references; only then does backfill map them.
        assert!(store.migrate_entry_to_blobs(&meta).unwrap());
        assert_eq!(store.backfill_entry_blobs().unwrap(), 1);
        assert_eq!(blob_refcount(&store, &content_hash(b"blob B")), Some(1));
    }

    /// A legacy artifact whose hash a modern entry already owns still needs
    /// its own reference, including a hash listed twice. Backfill waits for
    /// the migration, so the mapping never runs ahead of the count.
    #[test]
    fn migration_counts_a_legacy_hash_another_entry_already_owns() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path())).unwrap();
        put_outputs(&store, dir.path(), "modern", "one", &[("a.rlib", b"twin")]);
        let meta = legacy_entry(
            &store,
            "legacy_shared",
            &[
                ("a.rlib", b"twin"),
                ("b.rlib", b"twin"),
                ("c.rmeta", b"solo"),
            ],
        );
        assert_eq!(store.backfill_entry_blobs().unwrap(), 0);

        assert!(store.migrate_entry_to_blobs(&meta).unwrap());
        assert_eq!(store.backfill_entry_blobs().unwrap(), 1);

        assert_eq!(blob_refcount(&store, &content_hash(b"twin")), Some(3));
        assert_eq!(blob_refcount(&store, &content_hash(b"solo")), Some(1));
        assert_blob_refs_match_mappings(&store);
        store.remove_entry("legacy_shared").unwrap();
        assert!(store.get("modern").unwrap().is_some());
        assert_blob_refs_match_mappings(&store);
    }

    /// A mapping can name a blob that has no row (an older backfill mapped
    /// legacy entries nothing had counted). Releasing it after the new
    /// generation's increment would eat that increment and delete the row,
    /// so every publisher releases first.
    #[test]
    fn republish_over_a_mapping_without_a_blob_row_keeps_the_new_reference() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path())).unwrap();
        let map = |key: &str, content: &[u8]| {
            store
                .db
                .execute(
                    "INSERT INTO entry_blobs (cache_key, hash, refs) VALUES (?1, ?2, 1)",
                    params![key, content_hash(content)],
                )
                .unwrap();
        };

        map("via_import", b"import blob");
        extract_download(&store, "via_import", &[("a.rlib", b"import blob")]);
        store.import_downloaded_entry("via_import").unwrap();
        assert_eq!(
            blob_refcount(&store, &content_hash(b"import blob")),
            Some(1)
        );

        map("via_put", b"put blob");
        put_outputs(
            &store,
            dir.path(),
            "via_put",
            "one",
            &[("a.rlib", b"put blob")],
        );
        assert_eq!(blob_refcount(&store, &content_hash(b"put blob")), Some(1));

        assert_blob_refs_match_mappings(&store);
    }

    /// The batch import and the rebuild claim a key no entry row owned. A
    /// mapping left behind for it holds counted references; they are
    /// released, not dropped with the mapping.
    #[test]
    fn claiming_an_unowned_key_releases_its_leftover_mapping() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path())).unwrap();
        let leftover = |key: &str| {
            store
                .db
                .execute(
                    "INSERT INTO blobs (hash, size, refcount) VALUES (?1, 1, 1)",
                    params![format!("old-{key}")],
                )
                .unwrap();
            store
                .db
                .execute(
                    "INSERT INTO entry_blobs (cache_key, hash, refs) VALUES (?1, ?2, 1)",
                    params![key, format!("old-{key}")],
                )
                .unwrap();
        };

        let batch_key = "b".repeat(64);
        leftover(&batch_key);
        let verified = write_verified_fixture(&store, &batch_key, &batch_key, "lib.rlib", None);
        assert_eq!(
            store.import_verified_restored_entries(&[verified]).unwrap(),
            1
        );
        assert_blob_refs_match_mappings(&store);

        let rebuild_key = "c".repeat(64);
        leftover(&rebuild_key);
        let mut meta = read_meta(&store, &batch_key);
        meta.cache_key = rebuild_key.clone();
        let rebuild_dir = store.entry_dir(&rebuild_key);
        fs::create_dir_all(&rebuild_dir).unwrap();
        fs::write(
            rebuild_dir.join("meta.json"),
            serde_json::to_string(&meta).unwrap(),
        )
        .unwrap();
        assert_eq!(
            store.rebuild_one_entry(&rebuild_key, &rebuild_dir).unwrap(),
            Some(1)
        );
        assert_blob_refs_match_mappings(&store);
        assert_eq!(blob_table_count(&store), 1, "both old-* rows are gone");
    }

    /// Entry `owner` holds the only counted reference to a blob; entry
    /// `uncounted` has a committed row, the same file list and a mapping
    /// whose reference nobody counted (what an older backfill left behind
    /// for an un-migrated legacy entry). Returns the blob's hash.
    fn owner_and_uncounted_mapping(store: &Store, dir: &Path) -> String {
        put_outputs(store, dir, "owner", "one", &[("a.rlib", b"shared blob")]);
        let mut meta = read_meta(store, "owner");
        meta.cache_key = "uncounted".to_string();
        let entry_dir = store.entry_dir("uncounted");
        fs::create_dir_all(&entry_dir).unwrap();
        fs::write(
            entry_dir.join("meta.json"),
            serde_json::to_string(&meta).unwrap(),
        )
        .unwrap();
        store
            .db
            .execute(
                "INSERT INTO entries (cache_key, crate_name, size, committed)
                 VALUES ('uncounted', 'c1', 1, 1)",
                [],
            )
            .unwrap();
        record_entry_blobs(&store.db, "uncounted", &meta.files).unwrap();
        let hash = content_hash(b"shared blob");
        assert_eq!(blob_refcount(store, &hash), Some(1));
        hash
    }

    fn mapped_refs(store: &Store, key: &str, hash: &str) -> Option<i64> {
        store
            .db
            .query_row(
                "SELECT refs FROM entry_blobs WHERE cache_key = ?1 AND hash = ?2",
                params![key, hash],
                |r| r.get(0),
            )
            .ok()
    }

    /// Replacing the uncounted generation through put must not spend the
    /// owner's reference: the blob ends at the owner's one plus the new
    /// generation's own, none of it taken from the owner.
    #[test]
    fn reput_over_an_uncounted_mapping_keeps_the_other_owners_reference() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path())).unwrap();
        let hash = owner_and_uncounted_mapping(&store, dir.path());

        put_outputs(
            &store,
            dir.path(),
            "uncounted",
            "two",
            &[("b.rlib", b"other blob")],
        );

        assert_eq!(
            blob_refcount(&store, &hash),
            Some(1),
            "the owner's reference"
        );
        assert_eq!(mapped_refs(&store, "owner", &hash), Some(1));
        assert_eq!(mapped_refs(&store, "uncounted", &hash), None);
        assert!(store.blob_path(&hash).is_file());
        assert_blob_refs_match_mappings(&store);
    }

    #[test]
    fn reimport_over_an_uncounted_mapping_keeps_the_other_owners_reference() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path())).unwrap();
        let hash = owner_and_uncounted_mapping(&store, dir.path());

        extract_download(&store, "uncounted", &[("b.rlib", b"other blob")]);
        store.import_downloaded_entry("uncounted").unwrap();

        assert_eq!(
            blob_refcount(&store, &hash),
            Some(1),
            "the owner's reference"
        );
        assert_eq!(mapped_refs(&store, "owner", &hash), Some(1));
        assert_eq!(mapped_refs(&store, "uncounted", &hash), None);
        assert_blob_refs_match_mappings(&store);
    }

    /// Evicting the uncounted entry must not reclaim the blob its other
    /// owner still serves from.
    #[test]
    fn removing_an_uncounted_mapping_keeps_the_other_owners_blob() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path())).unwrap();
        let hash = owner_and_uncounted_mapping(&store, dir.path());

        store.remove_entry("uncounted").unwrap();

        assert_eq!(blob_refcount(&store, &hash), Some(1));
        assert!(store.blob_path(&hash).is_file());
        assert!(store.get("owner").unwrap().is_some());
        assert_blob_refs_match_mappings(&store);
    }

    #[test]
    fn floor_raises_only_this_keys_blobs_and_only_up_to_their_mappings() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path())).unwrap();
        store
            .db
            .execute_batch(
                "INSERT INTO blobs (hash, size, refcount) VALUES
                     ('low', 1, 1), ('exact', 1, 5), ('high', 1, 9), ('foreign', 1, 1);
                 INSERT INTO entry_blobs (cache_key, hash, refs) VALUES
                     ('k', 'low', 2), ('other', 'low', 1),
                     ('k', 'exact', 2), ('other', 'exact', 3),
                     ('k', 'high', 1),
                     ('k', 'norow', 1),
                     ('other', 'foreign', 4);",
            )
            .unwrap();

        // Three rows match; 'norow' has none to raise.
        assert_eq!(floor_blob_refs_at_mappings(&store.db, "k").unwrap(), 3);

        assert_eq!(blob_refcount(&store, "low"), Some(3));
        assert_eq!(blob_refcount(&store, "exact"), Some(5));
        assert_eq!(blob_refcount(&store, "high"), Some(9), "never lowered");
        assert_eq!(blob_refcount(&store, "norow"), None);
        assert_eq!(blob_refcount(&store, "foreign"), Some(1), "not this key's");
    }

    #[test]
    fn release_entry_blob_refs_subtracts_this_keys_mapping_only() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path())).unwrap();
        store
            .db
            .execute_batch(
                "INSERT INTO blobs (hash, size, refcount) VALUES
                     ('shared', 1, 5), ('last', 1, 2), ('drifted', 1, 1), ('idle', 1, 0),
                     ('uncounted', 1, 1);
                 INSERT INTO entry_blobs (cache_key, hash, refs) VALUES
                     ('k', 'shared', 2), ('k', 'last', 2), ('k', 'drifted', 3),
                     ('k', 'norow', 1), ('k', 'uncounted', 1),
                     ('other', 'shared', 3), ('other', 'uncounted', 1);",
            )
            .unwrap();

        release_entry_blob_refs(&store.db, "k").unwrap();

        assert_eq!(blob_refcount(&store, "shared"), Some(3));
        assert_eq!(blob_refcount(&store, "last"), None, "released to zero");
        assert_eq!(blob_refcount(&store, "norow"), None, "nothing to release");
        assert_eq!(
            blob_refcount(&store, "uncounted"),
            Some(1),
            "the other owner's reference survives"
        );
        assert_eq!(
            blob_refcount(&store, "drifted"),
            None,
            "clamped, not negative"
        );
        assert_eq!(
            blob_refcount(&store, "idle"),
            Some(0),
            "rows this key never mapped are not its to delete"
        );
        let mapped: Vec<String> = store
            .db
            .prepare("SELECT cache_key FROM entry_blobs")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(mapped, vec!["other".to_string(), "other".to_string()]);
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
                &[(output.clone(), "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        std::fs::remove_file(&output).unwrap();

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
                    &[(output.clone(), format!("{key}.rlib"))],
                    "",
                    "",
                )
                .unwrap();
            std::fs::remove_file(&output).unwrap();
        }

        let prior_schema = kache_format::CACHE_KEY_VERSION.saturating_sub(1);
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
            .evict_stale_key_schemas(kache_format::CACHE_KEY_VERSION)
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
            .evict_stale_key_schemas(kache_format::CACHE_KEY_VERSION)
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
        assert_eq!(current.key_schema, kache_format::CACHE_KEY_VERSION);

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
        let hash = crate::file_hash::hash_file(&entry_dir.join("lib.rlib")).unwrap();
        let prior_schema = kache_format::CACHE_KEY_VERSION.saturating_sub(1);
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
            key_schema: kache_format::CACHE_KEY_VERSION,
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
    fn failed_restore_cleanup_preserves_a_concurrent_committed_generation() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let publisher_store = Store::open(&config).unwrap();
        let cleanup_store = Store::open(&config).unwrap();
        let key = blake3::hash(b"restore-cleanup-publication-race")
            .to_hex()
            .to_string();
        let entry_dir = config.store_dir().join(&key);
        let meta_json = serde_json::to_string_pretty(&EntryMeta {
            cache_key: key.clone(),
            key_schema: kache_format::CACHE_KEY_VERSION,
            crate_name: "published".into(),
            crate_types: vec!["lib".into()],
            files: Vec::new(),
            stdout: String::new(),
            stderr: String::new(),
            features: Vec::new(),
            target: String::new(),
            profile: "dev".into(),
            compile_time_ms: 0,
            emit_kinds: Vec::new(),
        })
        .unwrap();

        // Hold a publisher's SQLite write transaction after materializing its
        // meta.json but before commit. The cleanup announces the exact point at
        // which it is about to request the same writer lock; only then does the
        // publisher commit. This fixes the ordering without scheduler sleeps.
        let (published_tx, published_rx) = std::sync::mpsc::sync_channel(0);
        let (cleanup_attempt_tx, cleanup_attempt_rx) = std::sync::mpsc::sync_channel(0);
        let publisher_key = key.clone();
        let publisher_entry_dir = entry_dir.clone();
        let publisher_meta = meta_json.clone();
        let publisher = std::thread::spawn(move || {
            let tx = publisher_store.db.unchecked_transaction().unwrap();
            tx.execute(
                "INSERT INTO entries (cache_key, crate_name, size, committed) \
                 VALUES (?1, 'published', 0, 1)",
                params![publisher_key],
            )
            .unwrap();
            fs::create_dir_all(&publisher_entry_dir).unwrap();
            fs::write(publisher_entry_dir.join("meta.json"), publisher_meta).unwrap();
            published_tx.send(()).unwrap();
            cleanup_attempt_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("cleanup must attempt the writer lock");
            tx.commit().unwrap();
        });

        published_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("publisher must reach its pre-commit point");
        let cleanup_key = key.clone();
        let cleanup = std::thread::spawn(move || {
            cleanup_store.discard_uncommitted_restored_entry_inner(
                &cleanup_key,
                || {
                    cleanup_attempt_tx.send(()).unwrap();
                },
                || {},
            )
        });

        publisher.join().unwrap();
        cleanup
            .join()
            .unwrap()
            .expect("cleanup should observe and preserve the committed winner");

        let store = Store::open(&config).unwrap();
        assert!(store.contains(&key));
        assert_eq!(
            fs::read_to_string(entry_dir.join("meta.json")).unwrap(),
            meta_json,
            "cleanup must not remove or replace the generation that committed first"
        );
    }

    #[test]
    fn failed_restore_cleanup_holds_the_writer_lock_through_removal() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let cleanup_store = Store::open(&config).unwrap();
        let contender_store = Store::open(&config).unwrap();
        let key = blake3::hash(b"restore-cleanup-first").to_hex().to_string();
        let entry_dir = cleanup_store.entry_dir(&key);
        fs::create_dir_all(&entry_dir).unwrap();
        fs::write(entry_dir.join("meta.json"), b"stale restore").unwrap();

        let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        let cleanup_key = key.clone();
        let cleanup = std::thread::spawn(move || {
            cleanup_store.discard_uncommitted_restored_entry_inner(
                &cleanup_key,
                || {},
                || {
                    locked_tx.send(()).unwrap();
                    release_rx
                        .recv_timeout(Duration::from_secs(5))
                        .expect("test must release the cleanup writer lock");
                },
            )
        });

        locked_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("cleanup must acquire its writer lock");
        contender_store.db.busy_timeout(Duration::ZERO).unwrap();
        let lock_error = match rusqlite::Transaction::new_unchecked(
            &contender_store.db,
            rusqlite::TransactionBehavior::Immediate,
        ) {
            Ok(transaction) => {
                drop(transaction);
                panic!("another publisher acquired SQLite's writer lock during cleanup");
            }
            Err(error) => error,
        };
        assert!(
            matches!(
                lock_error,
                SqlError::SqliteFailure(code, _)
                    if matches!(code.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
            ),
            "unexpected competing-writer result: {lock_error}"
        );

        release_tx.send(()).unwrap();
        cleanup
            .join()
            .unwrap()
            .expect("cleanup should remove the uncommitted residue");
        assert!(!entry_dir.exists());

        let output = dir.path().join("published.rlib");
        fs::write(&output, b"published after cleanup").unwrap();
        contender_store
            .put(
                &key,
                "published",
                &["rlib".into()],
                &[],
                "host",
                "dev",
                &[(output, "published.rlib".into())],
                "",
                "",
            )
            .expect("a publisher must succeed after cleanup releases the lock");
        assert!(contender_store.contains(&key));
        assert!(entry_dir.join("meta.json").is_file());
    }

    #[test]
    fn failed_restore_cleanup_accepts_an_already_missing_directory() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let key = blake3::hash(b"already-missing-restore")
            .to_hex()
            .to_string();

        assert!(!store.entry_dir(&key).exists());
        store
            .discard_uncommitted_restored_entry(&key)
            .expect("an already-absent restore has nothing left to clean up");
    }

    #[test]
    fn failed_restore_cleanup_reports_non_directory_residue() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let key = blake3::hash(b"non-directory-restore-residue")
            .to_hex()
            .to_string();
        let entry_path = store.entry_dir(&key);
        fs::write(&entry_path, b"not a directory").unwrap();

        let error = store
            .discard_uncommitted_restored_entry(&key)
            .expect_err("non-directory residue must not be silently accepted");
        assert!(
            error
                .to_string()
                .contains("removing uncommitted restored entry"),
            "unexpected cleanup error: {error:#}"
        );
        assert!(entry_path.is_file());
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

        let hash = crate::file_hash::hash_file(&entry_dir.join("lib.rlib")).unwrap();
        let meta = EntryMeta {
            cache_key: "dl_key".to_string(),
            key_schema: kache_format::CACHE_KEY_VERSION,
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

        let _env_lock = crate::test_support::process_state_test_lock();
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
        assert!(zero_byte_is_valid_output::<TestPolicy>(
            "libfoo-1234.rmeta",
            &[]
        ));
        for ct in ["bin", "cdylib", "staticlib"] {
            assert!(
                zero_byte_is_valid_output::<TestPolicy>("libfoo-1234.rmeta", &[ct.into()]),
                "{ct} emits no metadata, so an empty .rmeta is legitimate"
            );
        }
        // These do emit metadata — empty means truncated.
        for ct in ["lib", "rlib", "dylib", "proc-macro", "some-future-type"] {
            assert!(
                !zero_byte_is_valid_output::<TestPolicy>("libfoo-1234.rmeta", &[ct.into()]),
                "{ct} must keep the truncation guard"
            );
        }
        // Everything else empty means a truncated write, not a real output.
        for name in ["libfoo.rlib", "foo.d", "foo.o", "libfoo.so", "foo"] {
            assert!(
                !zero_byte_is_valid_output::<TestPolicy>(name, &[]),
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
            key_schema: kache_format::CACHE_KEY_VERSION,
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
            hash: crate::file_hash::hash_file(&entry_dir.join("lib.rlib")).unwrap(),
            executable: false,
        };
        mutate(&mut file);
        let meta = EntryMeta {
            cache_key: key.to_string(),
            key_schema: kache_format::CACHE_KEY_VERSION,
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
    fn wait_for_committed_returns_false_without_an_owner() {
        let dir = tempfile::tempdir().unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "store::tests::wait_for_committed_missing_child_fixture",
                "--ignored",
            ])
            .env("KACHE_TEST_WAIT_ROOT", dir.path())
            .spawn()
            .unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            match child.try_wait().unwrap() {
                Some(status) => {
                    assert!(status.success(), "wait fixture failed: {status}");
                    break;
                }
                None if std::time::Instant::now() >= deadline => {
                    child.kill().unwrap();
                    child.wait().unwrap();
                    panic!("waiting without an owner must return promptly");
                }
                None => std::thread::sleep(Duration::from_millis(10)),
            }
        }
    }

    #[test]
    #[ignore = "subprocess fixture for wait_for_committed_returns_false_without_an_owner"]
    fn wait_for_committed_missing_child_fixture() {
        let root = PathBuf::from(std::env::var_os("KACHE_TEST_WAIT_ROOT").expect("fixture root"));
        let store = Store::open(test_config(&root)).unwrap();
        assert!(!store.wait_for_committed("nope").unwrap());
    }

    #[test]
    fn wait_for_committed_returns_true_for_an_existing_entry() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let output = dir.path().join("already-committed.rlib");
        fs::write(&output, b"committed output").unwrap();
        store
            .put(
                "already-committed",
                "peer",
                &["rlib".to_string()],
                &[],
                "host",
                "dev",
                &[(output, "already-committed.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        assert!(store.wait_for_committed("already-committed").unwrap());
    }

    #[test]
    fn wait_for_committed_observes_advisory_lock_release() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let owner = Store::open(&config).unwrap();
        let cache_key = "peer-commit";
        let owner_lock = owner.try_lock(cache_key).unwrap().expect("owner lock");
        let root = dir.path().to_path_buf();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();

        let waiter = std::thread::spawn(move || {
            let store = Store::open(test_config(&root)).unwrap();
            ready_tx.send(()).unwrap();
            store
                .wait_for_committed_with_timeout(cache_key, Duration::from_secs(5))
                .unwrap()
        });
        ready_rx.recv().unwrap();
        std::thread::sleep(Duration::from_millis(50));

        let output = dir.path().join("peer.rlib");
        fs::write(&output, b"peer output").unwrap();
        owner
            .put(
                cache_key,
                "peer",
                &["rlib".to_string()],
                &[],
                "host",
                "dev",
                &[(output, "peer.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        drop(owner_lock);

        assert!(
            waiter.join().unwrap(),
            "waiter must observe the committed key"
        );
        assert!(
            owner.entry_dir(cache_key).with_extension("lock").exists(),
            "waiting must not depend on deleting the lock file"
        );
    }

    #[test]
    fn wait_for_committed_respects_timeout_while_owned() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let owner = Store::open(&config).unwrap();
        let waiter = Store::open(&config).unwrap();
        let _lock = owner.try_lock("slow-peer").unwrap().expect("owner lock");

        assert!(
            !waiter
                .wait_for_committed_with_timeout("slow-peer", Duration::from_millis(10))
                .unwrap()
        );
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

        // Same Windows-runner caveat as test_concurrent_put_remove_never_dangles:
        // this proves refcount consistency, not the production five-second
        // fail-fast. Eight WAL writers on a two-core hosted Windows runner
        // queue past 5s and surface SQLITE_BUSY (v0.16.1 tag CI).
        let stores: Vec<_> = (0..N)
            .map(|_| {
                let store = Store::open(&config).unwrap();
                store.db.busy_timeout(Duration::from_secs(30)).unwrap();
                store
            })
            .collect();

        let mut handles = Vec::new();
        for (i, store) in stores.into_iter().enumerate() {
            let src = dir.path().join(format!("art-{i}.rlib"));
            std::fs::write(&src, content).unwrap();
            handles.push(std::thread::spawn(move || {
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
        assert_blob_refs_match_mappings(&store);
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
        // keep eight WAL writers queued past that timeout. The full Windows
        // suite has also exhausted 30 seconds under mutation-runner load, so
        // give these test connections enough time for every logical operation
        // to commit and reach the invariant checks below.
        let stores: Vec<_> = (0..THREADS)
            .map(|_| {
                let store = Store::open(&config).unwrap();
                store.db.busy_timeout(Duration::from_secs(60)).unwrap();
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

    /// #1128 without SQLite in the way: publishers race a remover on one
    /// blob path, so the name cycles through present and read-only,
    /// delete-pending and absent far more often than whole puts manage. No
    /// lock orders them, so a publish may end with the blob gone again; what
    /// it must never do is report that race as an error.
    #[test]
    fn publish_racing_an_unlink_never_errors() {
        const PUBLISHERS: usize = 4;
        const ROUNDS: usize = 400;
        let dir = tempfile::tempdir().unwrap();
        // No fsync per publish: the race is in the rename, and the flushes
        // only spread the attempts out.
        let mut config = test_config(dir.path());
        config.deferred_durability = true;
        let store = Store::open(&config).unwrap();

        let content = b"one blob, published and unlinked in a loop";
        let source = dir.path().join("out.rlib");
        fs::write(&source, content).unwrap();
        let hash = crate::file_hash::hash_file(&source).unwrap();
        let blob = store.blob_path(&hash);

        let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let remover = {
            let blob = blob.clone();
            let done = std::sync::Arc::clone(&done);
            std::thread::spawn(move || {
                while !done.load(Ordering::Relaxed) {
                    unlink_blob(&blob);
                    std::thread::yield_now();
                }
            })
        };

        let publishers: Vec<_> = (0..PUBLISHERS)
            .map(|_| {
                let store = Store::open(&config).unwrap();
                let source = source.clone();
                let hash = hash.clone();
                std::thread::spawn(move || {
                    for _ in 0..ROUNDS {
                        let (staged, ingest) =
                            store.stage_blob_from_source(&source, false).unwrap();
                        store
                            .publish_staged_blob(&staged, ingest, &hash, content.len() as u64)
                            .unwrap();
                    }
                })
            })
            .collect();
        for p in publishers {
            p.join().unwrap();
        }
        done.store(true, Ordering::Relaxed);
        remover.join().unwrap();

        // With the remover stopped, the locked phase's repair always lands.
        store
            .rematerialize_and_verify(&source, &hash, "out.rlib", false)
            .unwrap();
        assert_eq!(fs::read(&blob).unwrap(), content);
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
            matches!(outcome, GuardedRemoval::Skipped),
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

        assert!(
            matches!(removed, GuardedRemoval::Reclaimed(_)),
            "the removal owned generation A's row"
        );
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
                    matches!(
                        h.join().unwrap().expect("losing remover must not error"),
                        GuardedRemoval::Reclaimed(_)
                    )
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
                    &[(src.clone(), "lib.rlib".into())],
                    "",
                    "",
                )
                .unwrap();
            let _ = std::fs::remove_file(&src);
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
                    &[(src.clone(), "lib.rlib".into())],
                    "",
                    "",
                )
                .unwrap();
            let _ = std::fs::remove_file(&src);
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
                &[(src.clone(), "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        let _ = std::fs::remove_file(&src);

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
        assert_eq!(stats.disk_bytes_reclaimed, 300 * 1024);
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
                    &[(src.clone(), "lib.rlib".into())],
                    "",
                    "",
                )
                .unwrap();
            let _ = std::fs::remove_file(&src);
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
                    &[(src.clone(), "lib.rlib".into())],
                    "",
                    "",
                    compile_ms,
                )
                .unwrap();
            let _ = std::fs::remove_file(&src);
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
                    &[(src.clone(), "lib.rlib".into())],
                    "",
                    "",
                )
                .unwrap();
            let _ = std::fs::remove_file(&src);
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
        assert_eq!(stats.disk_bytes_reclaimed, 300);
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

        let hash = crate::file_hash::hash_file(&entry_dir.join("lib.rlib")).unwrap();
        let meta = EntryMeta {
            cache_key: "old_key".to_string(),
            key_schema: kache_format::CACHE_KEY_VERSION,
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
        let hash = crate::file_hash::hash_file(&artifact).unwrap();
        let meta = EntryMeta {
            cache_key: "old_bad_key".to_string(),
            key_schema: kache_format::CACHE_KEY_VERSION,
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
            crate::file_hash::hash_file(&tmp).unwrap()
        };

        // Create two legacy entries with identical content
        for key in &["old1", "old2"] {
            let entry_dir = config.store_dir().join(key);
            fs::create_dir_all(&entry_dir).unwrap();
            fs::write(entry_dir.join("lib.rlib"), content).unwrap();

            let meta = EntryMeta {
                cache_key: key.to_string(),
                key_schema: kache_format::CACHE_KEY_VERSION,
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
        assert_eq!(store.backfill_entry_blobs().unwrap(), 2);
        assert_blob_refs_match_mappings(&store);

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
        assert_blob_refs_match_mappings(&store);
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

    const TWO_HOURS: Duration = Duration::from_secs(7200);

    /// A key lock file last claimed `age` ago.
    fn aged_key_lock(config: &Config, key: &str, age: Duration) -> PathBuf {
        let path = config.store_dir().join(format!("{key}.lock"));
        std::fs::create_dir_all(config.store_dir()).unwrap();
        std::fs::write(&path, b"1").unwrap();
        set_age(&path, age);
        path
    }

    fn set_age(path: &Path, age: Duration) {
        let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        file.set_modified(std::time::SystemTime::now() - age)
            .unwrap();
    }

    fn sweep_key_locks(store: &Store, cap: usize) -> KeyLockSweepStats {
        store
            .sweep_stale_key_locks(KEY_LOCK_SWEEP_GRACE, cap)
            .unwrap()
    }

    #[test]
    fn key_lock_sweep_constants_are_pinned() {
        assert_eq!(KEY_LOCK_SWEEP_GRACE, Duration::from_secs(3600));
        assert_eq!(KEY_LOCK_SWEEP_GRACE, STAGING_SWEEP_GRACE);
        assert_eq!(KEY_LOCK_SWEEP_CAP, 20_000);
        assert_eq!(LOCK_OPEN_ATTEMPTS, 4);
        // The CI store that prompted the sweep: 84,496 stale locks.
        assert_eq!(84_496usize.div_ceil(KEY_LOCK_SWEEP_CAP), 5);
    }

    #[test]
    fn key_of_lock_name_accepts_only_a_valid_key_with_the_lock_suffix() {
        let k = key(1);
        assert_eq!(key_of_lock_name(&format!("{k}.lock")), Some(k.as_str()));
        assert_eq!(key_of_lock_name(&k), None);
        assert_eq!(key_of_lock_name("gc.lock"), None);
        assert_eq!(key_of_lock_name("durability.lock"), None);
        assert_eq!(key_of_lock_name(".lock"), None);
        assert_eq!(key_of_lock_name(&format!("{k}.lock.tmp")), None);
        assert_eq!(key_of_lock_name(&format!("{}.lock", &k[1..])), None);
        assert_eq!(
            key_of_lock_name(&format!("{}.lock", k.to_uppercase())),
            None
        );
    }

    #[test]
    fn key_lock_staleness_boundary_is_inclusive_and_future_mtimes_are_young() {
        let now = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let grace = Duration::from_secs(3600);
        assert!(!key_lock_is_stale(
            now - Duration::from_secs(3599),
            now,
            grace
        ));
        assert!(key_lock_is_stale(now - grace, now, grace));
        assert!(key_lock_is_stale(now - TWO_HOURS, now, grace));
        assert!(!key_lock_is_stale(now + TWO_HOURS, now, grace));
    }

    #[test]
    fn key_lock_sweep_stats_report_what_remains() {
        let stats = KeyLockSweepStats {
            seen: 10,
            removed: 3,
        };
        assert_eq!(stats.remaining(), 7);
        let none = KeyLockSweepStats {
            seen: 0,
            removed: 0,
        };
        assert_eq!(none.remaining(), 0);
    }

    #[test]
    fn key_lock_sweep_removes_a_stale_lock_whose_key_has_no_entry() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let stale = aged_key_lock(&config, &key(1), TWO_HOURS);

        assert_eq!(
            sweep_key_locks(&store, KEY_LOCK_SWEEP_CAP),
            KeyLockSweepStats {
                seen: 1,
                removed: 1
            }
        );
        assert!(!stale.exists());
        // The key is claimable again, on a new file.
        let lock = store.try_lock(&key(1)).unwrap();
        assert!(lock.is_some());
        assert!(stale.exists());
    }

    #[test]
    fn key_lock_sweep_removes_the_lock_of_an_evicted_key() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let k = put_entry(&store, dir.path(), 1, "evicted", b"payload");
        drop(store.try_lock(&k).unwrap().expect("claim the key"));
        let lock_path = config.store_dir().join(format!("{k}.lock"));
        set_age(&lock_path, TWO_HOURS);

        // Kept while the entry is live.
        assert_eq!(
            sweep_key_locks(&store, KEY_LOCK_SWEEP_CAP),
            KeyLockSweepStats {
                seen: 1,
                removed: 0
            }
        );
        assert!(lock_path.exists());

        store.remove_entry(&k).unwrap();
        assert_eq!(
            sweep_key_locks(&store, KEY_LOCK_SWEEP_CAP),
            KeyLockSweepStats {
                seen: 1,
                removed: 1
            }
        );
        assert!(!lock_path.exists());
    }

    #[test]
    fn key_lock_sweep_keeps_a_young_lock() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let young = aged_key_lock(&config, &key(1), Duration::from_secs(3000));
        let old = aged_key_lock(&config, &key(2), Duration::from_secs(4200));

        assert_eq!(
            sweep_key_locks(&store, KEY_LOCK_SWEEP_CAP),
            KeyLockSweepStats {
                seen: 2,
                removed: 1
            }
        );
        assert!(young.exists());
        assert!(!old.exists());
    }

    #[test]
    fn key_lock_sweep_never_removes_a_held_lock() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let held = store.try_lock(&key(1)).unwrap().expect("claim the key");
        let path = config.store_dir().join(format!("{}.lock", key(1)));
        let before = kache_fs::file_identity(&path).unwrap();
        // A compile that has been running for two hours.
        set_age(&path, TWO_HOURS);

        // Swept from another thread, as the daemon would from another process.
        let stats = std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let sweeper = Store::open(&config).unwrap();
                    sweep_key_locks(&sweeper, KEY_LOCK_SWEEP_CAP)
                })
                .join()
                .unwrap()
        });
        assert_eq!(
            stats,
            KeyLockSweepStats {
                seen: 1,
                removed: 0
            }
        );
        assert_eq!(kache_fs::file_identity(&path).unwrap(), before);
        assert!(
            store.try_lock(&key(1)).unwrap().is_none(),
            "the holder still excludes every other claimant"
        );

        drop(held);
        set_age(&path, TWO_HOURS);
        assert_eq!(sweep_key_locks(&store, KEY_LOCK_SWEEP_CAP).removed, 1);
    }

    #[test]
    fn key_lock_sweep_respects_the_cap_and_converges() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        for seed in 0..7 {
            aged_key_lock(&config, &key(seed), TWO_HOURS);
        }

        assert_eq!(
            sweep_key_locks(&store, 3),
            KeyLockSweepStats {
                seen: 7,
                removed: 3
            }
        );
        assert_eq!(
            sweep_key_locks(&store, 3),
            KeyLockSweepStats {
                seen: 4,
                removed: 3
            }
        );
        let last = sweep_key_locks(&store, 3);
        assert_eq!(
            last,
            KeyLockSweepStats {
                seen: 1,
                removed: 1
            }
        );
        assert_eq!(last.remaining(), 0);
        assert_eq!(
            sweep_key_locks(&store, 0),
            KeyLockSweepStats::default(),
            "nothing left"
        );
    }

    #[test]
    fn key_lock_sweep_with_a_zero_cap_only_counts() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let stale = aged_key_lock(&config, &key(1), TWO_HOURS);
        assert_eq!(
            sweep_key_locks(&store, 0),
            KeyLockSweepStats {
                seen: 1,
                removed: 0
            }
        );
        assert!(stale.exists());
    }

    #[test]
    fn key_lock_sweep_never_touches_gc_lock() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        drop(store.try_gc_lock().unwrap());
        let path = config.store_dir().join("gc.lock");
        set_age(&path, TWO_HOURS);
        assert_eq!(
            sweep_key_locks(&store, KEY_LOCK_SWEEP_CAP),
            KeyLockSweepStats::default()
        );
        assert!(path.exists());
    }

    #[test]
    fn key_lock_sweep_never_touches_durability_lock() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        drop(store.try_durability_flush_lock().unwrap());
        let path = config.store_dir().join("durability.lock");
        set_age(&path, TWO_HOURS);
        assert_eq!(
            sweep_key_locks(&store, KEY_LOCK_SWEEP_CAP),
            KeyLockSweepStats::default()
        );
        assert!(path.exists());
    }

    #[test]
    fn key_lock_sweep_never_touches_a_directory_named_like_a_key_lock() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let path = config.store_dir().join(format!("{}.lock", key(1)));
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("inside"), b"kept").unwrap();
        assert_eq!(
            sweep_key_locks(&store, KEY_LOCK_SWEEP_CAP),
            KeyLockSweepStats {
                seen: 1,
                removed: 0
            }
        );
        assert!(path.join("inside").exists());
    }

    #[test]
    fn key_lock_sweep_never_touches_an_entry_directory_or_other_files() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let k = put_entry(&store, dir.path(), 1, "kept", b"payload");
        // Not a key: too short, and a stray file beside the locks.
        let short = config.store_dir().join("abc123.lock");
        let stray = config.store_dir().join(format!("{}.lock.bak", key(2)));
        for path in [&short, &stray] {
            std::fs::write(path, b"1").unwrap();
            set_age(path, TWO_HOURS);
        }
        assert_eq!(
            sweep_key_locks(&store, KEY_LOCK_SWEEP_CAP),
            KeyLockSweepStats::default()
        );
        assert!(store.entry_dir(&k).join("meta.json").exists());
        assert!(short.exists());
        assert!(stray.exists());
    }

    #[cfg(unix)]
    #[test]
    fn key_lock_sweep_never_follows_a_symlink_named_like_a_key_lock() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let target = dir.path().join("elsewhere");
        std::fs::write(&target, b"kept").unwrap();
        set_age(&target, TWO_HOURS);
        let link = config.store_dir().join(format!("{}.lock", key(1)));
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert_eq!(sweep_key_locks(&store, KEY_LOCK_SWEEP_CAP).removed, 0);
        // Nor once the link itself is old enough.
        let later = std::time::SystemTime::now() + TWO_HOURS;
        assert!(!remove_stale_lock_file(
            &link,
            KEY_LOCK_SWEEP_GRACE,
            later,
            || {}
        ));
        assert!(link.symlink_metadata().is_ok());
        assert!(target.exists());
    }

    #[test]
    fn key_lock_sweep_on_a_store_with_no_directory_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let _ = std::fs::remove_dir_all(config.store_dir());
        assert_eq!(
            sweep_key_locks(&store, KEY_LOCK_SWEEP_CAP),
            KeyLockSweepStats::default()
        );
    }

    #[test]
    fn housekeeping_sweeps_key_locks_and_prunes_predictions() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        // Nothing to do, as on a fresh store with predictions off.
        assert_eq!(store.sweep_housekeeping(), HousekeepingStats::default());

        let stale = aged_key_lock(&config, &key(1), TWO_HOURS);
        aged_key_lock(&config, &key(2), TWO_HOURS);
        let young = aged_key_lock(&config, &key(3), Duration::ZERO);
        let predictions = store.file_hash_cache();
        for identity in ["unused", "live"] {
            predictions
                .put_input_prediction(identity, 1, None, "payload")
                .unwrap();
        }
        store
            .db
            .execute(
                "UPDATE input_predictions SET last_used = 1 WHERE identity = 'unused'",
                [],
            )
            .unwrap();

        assert_eq!(
            store.sweep_housekeeping(),
            HousekeepingStats {
                key_locks_removed: 2,
                key_locks_remaining: 1,
                predictions_pruned: 1,
            }
        );
        assert!(!stale.exists());
        assert!(young.exists());
        assert!(predictions.get_input_prediction("live").unwrap().is_some());
        assert_eq!(predictions.get_input_prediction("unused").unwrap(), None);
    }

    #[test]
    fn stale_lock_claimed_between_listing_and_lock_is_kept() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let path = aged_key_lock(&config, &key(1), TWO_HOURS);
        let now = std::time::SystemTime::now();
        // A claimant takes and releases the key after the sweep's stat.
        let removed = remove_stale_lock_file(&path, KEY_LOCK_SWEEP_GRACE, now, || {
            drop(StoreLock::try_acquire(&path).unwrap().expect("claim"));
        });
        assert!(!removed);
        assert!(path.exists());
        // Left alone, the same file goes.
        set_age(&path, TWO_HOURS);
        assert!(remove_stale_lock_file(
            &path,
            KEY_LOCK_SWEEP_GRACE,
            now,
            || {}
        ));
        assert!(!path.exists());
    }

    #[test]
    fn stale_lock_replaced_under_the_sweep_keeps_the_new_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let path = aged_key_lock(&config, &key(1), TWO_HOURS);
        let now = std::time::SystemTime::now();
        let removed = remove_stale_lock_file(&path, KEY_LOCK_SWEEP_GRACE, now, || {
            std::fs::remove_file(&path).unwrap();
            std::fs::write(&path, b"2").unwrap();
        });
        assert!(!removed);
        assert_eq!(std::fs::read(&path).unwrap(), b"2");
    }

    #[test]
    fn stale_lock_held_at_lock_time_is_kept() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let path = aged_key_lock(&config, &key(1), TWO_HOURS);
        let now = std::time::SystemTime::now() + TWO_HOURS + TWO_HOURS;
        let mut held = None;
        let removed = remove_stale_lock_file(&path, KEY_LOCK_SWEEP_GRACE, now, || {
            held = StoreLock::try_acquire(&path).unwrap();
        });
        assert!(held.is_some());
        assert!(!removed);
        assert!(path.exists());
    }

    #[test]
    fn lock_is_current_compares_identities_and_trusts_a_handle_without_one() {
        let id = |ino| kache_fs::InodeId { dev: 1, ino };
        let missing = || std::io::Error::from(std::io::ErrorKind::NotFound);
        assert!(lock_is_current(Ok(id(7)), Ok(id(7))));
        assert!(!lock_is_current(Ok(id(7)), Ok(id(8))));
        assert!(!lock_is_current(Ok(id(7)), Err(missing())));
        assert!(lock_is_current(Err(missing()), Ok(id(7))));
        assert!(lock_is_current(Err(missing()), Err(missing())));
    }

    #[test]
    fn lock_file_is_at_path_needs_the_same_file_at_the_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.lock");
        let other = dir.path().join("b.lock");
        std::fs::write(&path, b"1").unwrap();
        std::fs::write(&other, b"1").unwrap();
        let file = std::fs::File::open(&path).unwrap();
        assert!(lock_file_is_at_path(&file, &path));
        assert!(!lock_file_is_at_path(&file, &other));
        assert!(!lock_file_is_at_path(&file, &dir.path().join("missing")));
    }

    /// The race the sweep opens: the path is unlinked and recreated after a
    /// claimant opened it and before the claimant locks it.
    #[test]
    fn acquire_reopens_when_the_lock_file_was_replaced_between_open_and_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key.lock");
        let mut opens = 0;
        let lock = StoreLock::acquire_current(&path, StoreLock::try_lock_file, || {
            opens += 1;
            if opens == 1 {
                std::fs::remove_file(&path).unwrap();
                std::fs::write(&path, b"another claimant").unwrap();
            }
        })
        .unwrap()
        .expect("lock acquired");
        assert_eq!(opens, 2);
        // The lock is on the file the path names now, so it excludes others.
        assert_eq!(
            kache_fs::handle_identity(&lock.file).unwrap(),
            kache_fs::file_identity(&path).unwrap()
        );
        assert!(StoreLock::try_acquire(&path).unwrap().is_none());
        drop(lock);
        // Read after release: Windows refuses reads of a locked range.
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            std::process::id().to_string()
        );
        assert!(StoreLock::try_acquire(&path).unwrap().is_some());
    }

    #[test]
    fn acquire_reopens_when_the_lock_file_was_unlinked_between_open_and_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key.lock");
        let mut opens = 0;
        let lock = StoreLock::acquire_current(&path, StoreLock::try_lock_file, || {
            opens += 1;
            if opens == 1 {
                std::fs::remove_file(&path).unwrap();
            }
        })
        .unwrap()
        .expect("lock acquired");
        assert_eq!(opens, 2);
        assert!(path.exists());
        assert!(StoreLock::try_acquire(&path).unwrap().is_none());
        drop(lock);
    }

    #[test]
    fn acquire_yields_to_the_claimant_holding_the_replacement_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key.lock");
        let mut winner = None;
        let mut opens = 0;
        let lock = StoreLock::acquire_current(&path, StoreLock::try_lock_file, || {
            opens += 1;
            if opens == 1 {
                std::fs::remove_file(&path).unwrap();
                winner = StoreLock::try_acquire(&path).unwrap();
            }
        })
        .unwrap();
        assert!(winner.is_some());
        assert!(lock.is_none(), "only one claimant may hold the key");
        assert_eq!(opens, 2);
    }

    #[test]
    fn acquire_gives_up_after_a_bounded_number_of_replacements() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key.lock");
        let mut opens = 0u32;
        let result = StoreLock::acquire_current(&path, StoreLock::try_lock_file, || {
            opens += 1;
            std::fs::remove_file(&path).unwrap();
        });
        assert!(result.is_err());
        assert_eq!(opens, LOCK_OPEN_ATTEMPTS);
    }

    #[test]
    fn blocking_acquire_checks_the_path_too() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gc.lock");
        let lock = StoreLock::acquire(&path).unwrap();
        assert_eq!(
            kache_fs::handle_identity(&lock.file).unwrap(),
            kache_fs::file_identity(&path).unwrap()
        );
        assert!(StoreLock::try_acquire(&path).unwrap().is_none());
    }

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

        let _env_lock = crate::test_support::process_state_test_lock();

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

        let hash_a = crate::file_hash::hash_file(&entry_dir.join("a.rlib")).unwrap();
        let hash_b = crate::file_hash::hash_file(&entry_dir.join("b.dylib")).unwrap();

        let meta = EntryMeta {
            cache_key: "legacy_key".to_string(),
            key_schema: kache_format::CACHE_KEY_VERSION,
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
        assert!(store.migrate_entry_to_blobs(&meta).unwrap());

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
        assert_eq!(store.backfill_entry_blobs().unwrap(), 1);
        assert_blob_refs_match_mappings(&store);

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
        let hash = crate::file_hash::hash_file(&artifact).unwrap();
        let size = fs::metadata(&artifact).unwrap().len();
        let meta = EntryMeta {
            cache_key: "legacy_race".to_string(),
            key_schema: kache_format::CACHE_KEY_VERSION,
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

        store
            .db
            .execute(
                "INSERT INTO entries (cache_key, crate_name, size, committed)
                 VALUES ('legacy_race', 'legacy_crate', ?1, 1)",
                params![size as i64],
            )
            .unwrap();

        assert!(store.migrate_entry_to_blobs(&meta).unwrap());

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
        assert_blob_refs_match_mappings(&store);
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
        let kinds = emit_kinds_for_files::<TestPolicy>(&[
            cf("libfoo.rlib"),
            cf("libfoo.rmeta"),
            cf("foo.d"),
            cf("foo.dSYM"), // sidecar → no emit kind, ignored
        ]);
        assert_eq!(kinds, vec!["dep-info", "link", "metadata"]);
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
        let kinds = emit_kinds_for_files::<TestPolicy>(&files);
        assert_eq!(
            kinds,
            vec!["dep-info".to_string(), "link".to_string()],
            "a .wasm module is the link product of a wasm32 target"
        );

        let meta = EntryMeta {
            cache_key: "k".into(),
            key_schema: kache_format::CACHE_KEY_VERSION,
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
        let hash = crate::file_hash::hash_file(&artifact).unwrap();
        let size = std::fs::metadata(&artifact).unwrap().len();

        let meta = EntryMeta {
            cache_key: "dl_ch_test".to_string(),
            key_schema: kache_format::CACHE_KEY_VERSION,
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
                key_schema: kache_format::CACHE_KEY_VERSION,
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
                params![keys[0], kache_format::CACHE_KEY_VERSION],
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
            key_schema: kache_format::CACHE_KEY_VERSION,
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
        let store = Store::open(test_config(tmp.path())).unwrap();
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
            let store = Store::open(test_config(tmp.path())).unwrap();
            let key = blake3::hash(label.as_bytes()).to_hex().to_string();
            let entry = write_verified_fixture(&store, &key, &key, name, hash_override);
            assert!(
                store.import_verified_restored_entries(&[entry]).is_err(),
                "{label} must be rejected independently"
            );
        }

        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(tmp.path())).unwrap();
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
        let store = Store::open(test_config(tmp.path())).unwrap();
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
        let store = Store::open(test_config(tmp.path())).unwrap();
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
                    &[(old_file.clone(), "lib.rlib".to_string())],
                    "",
                    "",
                )
                .unwrap();
            std::fs::remove_file(&old_file).unwrap();
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
                    &[(new_file.clone(), "lib.rlib".to_string())],
                    "",
                    "",
                )
                .unwrap();
            std::fs::remove_file(&new_file).unwrap();
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

    #[test]
    fn store_copy_reason_cross_device_maps_from_exdev() {
        assert_eq!(
            StoreCopyReason::from_io_kind(std::io::ErrorKind::CrossesDevices),
            StoreCopyReason::CrossDevice,
        );
    }

    #[test]
    fn store_copy_reason_permission_maps_from_eperm() {
        assert_eq!(
            StoreCopyReason::from_io_kind(std::io::ErrorKind::PermissionDenied),
            StoreCopyReason::Permission,
        );
    }

    #[test]
    fn store_copy_reason_other_maps_from_unexpected_errno() {
        assert_eq!(
            StoreCopyReason::from_io_kind(std::io::ErrorKind::AlreadyExists),
            StoreCopyReason::Other,
        );
    }

    #[test]
    fn record_store_copy_reason_cross_device_increments() {
        let before = crate::opcounts::store_copy_cross_device_bytes();
        record_store_copy_reason(StoreCopyReason::CrossDevice, 11);
        assert!(crate::opcounts::store_copy_cross_device_bytes() >= before + 11);
    }

    #[test]
    fn record_store_copy_reason_permission_increments() {
        let before = crate::opcounts::store_copy_permission_bytes();
        record_store_copy_reason(StoreCopyReason::Permission, 13);
        assert!(crate::opcounts::store_copy_permission_bytes() >= before + 13);
    }

    #[test]
    fn record_store_copy_reason_ineligible_increments() {
        let before = crate::opcounts::store_copy_ineligible_bytes();
        record_store_copy_reason(StoreCopyReason::Ineligible, 17);
        assert!(crate::opcounts::store_copy_ineligible_bytes() >= before + 17);
    }

    #[test]
    fn record_store_copy_reason_other_increments() {
        let before = crate::opcounts::store_copy_other_bytes();
        record_store_copy_reason(StoreCopyReason::Other, 19);
        assert!(crate::opcounts::store_copy_other_bytes() >= before + 19);
    }

    /// Same-device `.rlib` put must hardlink, not copy (#835).
    ///
    /// Forces the hardlink path (skips reflink) so the assertion holds on CoW
    /// filesystems (APFS) as well as ext4: after the put, `store_hardlinked`
    /// grows and the blob has exactly two links (blob + build output).
    #[cfg(unix)]
    #[test]
    fn rlib_put_hardlinks_on_same_device() {
        use std::os::unix::fs::MetadataExt;

        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let source = dir.path().join("libtest.rlib");
        fs::write(&source, b"rlib-bytes-for-hardlink").unwrap();
        let bytes = fs::metadata(&source).unwrap().len();

        let before = crate::opcounts::store_hardlinked_bytes();
        let _force = ForceStoreHardlink::enable();
        store
            .put(
                "hardlink_835_key",
                "hardlink_crate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(source.clone(), "libtest.rlib".to_string())],
                "out",
                "err",
            )
            .unwrap();

        assert!(
            crate::opcounts::store_hardlinked_bytes() >= before + bytes,
            "same-device .rlib put must record hardlinked bytes"
        );
        let hash = crate::file_hash::hash_file(&source).unwrap();
        let blob = store.blob_path(&hash);
        assert_eq!(
            fs::metadata(&blob).unwrap().nlink(),
            2,
            "hardlinked blob must share its inode with the build output"
        );
    }

    #[test]
    fn injected_cross_device_ingest_records_cross_device_reason() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let source = dir.path().join("libx.rlib");
        fs::write(&source, b"cross-device-bytes").unwrap();
        let bytes = fs::metadata(&source).unwrap().len();

        let before = crate::opcounts::store_copy_cross_device_bytes();
        let _force = ForceStoreHardlink::enable();
        let _inject = InjectStoreHardlinkError::enable(std::io::ErrorKind::CrossesDevices);
        let (staged, ingest) = store.stage_blob_from_source(&source, true).unwrap();
        assert!(
            matches!(ingest, StoreIngest::Copy(StoreCopyReason::CrossDevice)),
            "EXDEV injection must stage as Copy(CrossDevice), got {ingest:?}"
        );
        // Publish to record the reason alongside the copy.
        let hash = crate::file_hash::hash_file(&staged).unwrap();
        store
            .publish_staged_blob(&staged, ingest, &hash, bytes)
            .unwrap();
        assert!(
            crate::opcounts::store_copy_cross_device_bytes() >= before + bytes,
            "EXDEV injection must record the cross-device reason"
        );
    }

    #[test]
    fn injected_permission_ingest_records_permission_reason() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let source = dir.path().join("libp.rlib");
        fs::write(&source, b"permission-bytes").unwrap();
        let bytes = fs::metadata(&source).unwrap().len();

        let before = crate::opcounts::store_copy_permission_bytes();
        let _force = ForceStoreHardlink::enable();
        let _inject = InjectStoreHardlinkError::enable(std::io::ErrorKind::PermissionDenied);
        let (staged, ingest) = store.stage_blob_from_source(&source, true).unwrap();
        assert!(
            matches!(ingest, StoreIngest::Copy(StoreCopyReason::Permission)),
            "EPERM injection must stage as Copy(Permission), got {ingest:?}"
        );
        let hash = crate::file_hash::hash_file(&staged).unwrap();
        store
            .publish_staged_blob(&staged, ingest, &hash, bytes)
            .unwrap();
        assert!(
            crate::opcounts::store_copy_permission_bytes() >= before + bytes,
            "EPERM injection must record the permission reason"
        );
    }

    #[test]
    fn injected_other_ingest_records_other_reason() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let source = dir.path().join("libo.rlib");
        fs::write(&source, b"other-bytes").unwrap();
        let bytes = fs::metadata(&source).unwrap().len();

        let before = crate::opcounts::store_copy_other_bytes();
        let _force = ForceStoreHardlink::enable();
        let _inject = InjectStoreHardlinkError::enable(std::io::ErrorKind::AlreadyExists);
        let (staged, ingest) = store.stage_blob_from_source(&source, true).unwrap();
        assert!(
            matches!(ingest, StoreIngest::Copy(StoreCopyReason::Other)),
            "other errno injection must stage as Copy(Other), got {ingest:?}"
        );
        let hash = crate::file_hash::hash_file(&staged).unwrap();
        store
            .publish_staged_blob(&staged, ingest, &hash, bytes)
            .unwrap();
        assert!(
            crate::opcounts::store_copy_other_bytes() >= before + bytes,
            "other errno injection must record the other reason"
        );
    }

    /// The ingest advisory fires for a cross-device fallback and nothing
    /// else, observed through marker files. Sole marker installer in this
    /// binary: no other unit test calls `set_cow_warn_marker`, so the fresh
    /// scratch dir starts empty deterministically.
    #[test]
    fn injected_cross_device_ingest_advises_but_other_errors_do_not() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let base = std::env::temp_dir().join(format!(
            "kache-store-cross-volume-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        crate::link::set_cow_warn_marker(base.join("warn"));
        crate::link::set_storage_layout_advice(true);
        let marker_written = || std::fs::read_dir(&base).unwrap().next().is_some();

        let source = dir.path().join("libv.rlib");
        fs::write(&source, b"cross-volume-bytes").unwrap();

        // Muted layout advice stays silent even on EXDEV.
        crate::link::set_storage_layout_advice(false);
        let _force = ForceStoreHardlink::enable();
        let _inject = InjectStoreHardlinkError::enable(std::io::ErrorKind::CrossesDevices);
        let (_, ingest) = store.stage_blob_from_source(&source, true).unwrap();
        assert!(
            matches!(ingest, StoreIngest::Copy(StoreCopyReason::CrossDevice)),
            "muting advice must not change the copy reason, got {ingest:?}"
        );
        assert!(
            !marker_written(),
            "a muted advisory must not write a marker"
        );
        crate::link::set_storage_layout_advice(true);

        // A non-cross-device failure records its reason but advises nothing.
        let _force = ForceStoreHardlink::enable();
        let _inject = InjectStoreHardlinkError::enable(std::io::ErrorKind::AlreadyExists);
        let (_, ingest) = store.stage_blob_from_source(&source, true).unwrap();
        assert!(
            matches!(ingest, StoreIngest::Copy(StoreCopyReason::Other)),
            "expected Copy(Other), got {ingest:?}"
        );
        assert!(
            !marker_written(),
            "a non-cross-device ingest failure must not advise"
        );

        // EXDEV advises exactly once per session window, through the log
        // sink when the wrapper selected it (#1067).
        let _ = crate::markers::take_emitted();
        crate::link::set_layout_advice_to_log(true);
        let _inject = InjectStoreHardlinkError::enable(std::io::ErrorKind::CrossesDevices);
        let (_, ingest) = store.stage_blob_from_source(&source, true).unwrap();
        crate::link::set_layout_advice_to_log(false);
        let emitted = crate::markers::take_emitted();
        assert_eq!(emitted.len(), 1, "expected one advisory, got {emitted:?}");
        assert_eq!(emitted[0].0, crate::markers::WarnSink::Log);
        assert!(emitted[0].1.contains("EXDEV"), "{emitted:?}");
        assert!(
            matches!(ingest, StoreIngest::Copy(StoreCopyReason::CrossDevice)),
            "expected Copy(CrossDevice), got {ingest:?}"
        );
        assert!(
            marker_written(),
            "a cross-device ingest fallback must advise"
        );
        crate::link::set_storage_layout_advice(true);
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn ineligible_ingest_records_ineligible_reason() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Executables are never hardlink-eligible by policy (#429): the copy
        // records as Ineligible, not as a link failure.
        let source = dir.path().join("out.rlib");
        fs::write(&source, b"ineligible-bytes").unwrap();
        let bytes = fs::metadata(&source).unwrap().len();

        let before = crate::opcounts::store_copy_ineligible_bytes();
        // `allow_hardlink=false` is the cc/policy path: no link attempted.
        // Force past reflink so the policy refusal is exercised even on CoW
        // filesystems where a reflink would otherwise win.
        let _force = ForceStoreHardlink::enable();
        let (staged, ingest) = store.stage_blob_from_source(&source, false).unwrap();
        assert!(
            matches!(ingest, StoreIngest::Copy(StoreCopyReason::Ineligible)),
            "policy refusal must stage as Copy(Ineligible), got {ingest:?}"
        );
        let hash = crate::file_hash::hash_file(&staged).unwrap();
        store
            .publish_staged_blob(&staged, ingest, &hash, bytes)
            .unwrap();
        assert!(
            crate::opcounts::store_copy_ineligible_bytes() >= before + bytes,
            "policy refusal must record the ineligible reason"
        );
    }

    #[test]
    fn eviction_write_pacer_pauses_once_per_full_slice() {
        let slice = Duration::from_millis(50);
        let pause = Duration::from_millis(150);
        let mut pacer = EvictionWritePacer::new(slice, pause);
        assert_eq!(pacer.after_write(Duration::from_millis(30)), None);
        assert_eq!(
            pacer.after_write(Duration::from_millis(20)),
            Some(pause),
            "a slice exactly used up pauses"
        );
        assert_eq!(
            pacer.after_write(Duration::from_millis(49)),
            None,
            "the pause starts a fresh slice"
        );
        assert_eq!(pacer.after_contention(), pause);
        assert_eq!(
            pacer.after_write(Duration::from_millis(49)),
            None,
            "contention starts a fresh slice too"
        );
        assert_eq!(pacer.after_write(Duration::from_millis(1)), Some(pause));
    }

    /// Put `n` small entries that eviction may remove: unique blobs, idle
    /// past the active-pin grace.
    fn put_evictable_entries(store: &Store, dir: &Path, n: usize) {
        for i in 0..n {
            let src = dir.join(format!("evictable-{i}.rlib"));
            std::fs::write(&src, format!("evictable payload {i}").repeat(8)).unwrap();
            store
                .put(
                    &format!("{i:064x}"),
                    "c",
                    &["lib".into()],
                    &[],
                    "",
                    "dev",
                    &[(src.clone(), "lib.rlib".into())],
                    "",
                    "",
                )
                .unwrap();
            let _ = std::fs::remove_file(&src);
        }
        store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour')",
                [],
            )
            .unwrap();
    }

    /// A sweep that finds a build holding the index write lock stands off
    /// before its next removal. It used to retry the next entry at once,
    /// and a build's own writes then competed with a sweep that never let go.
    #[test]
    fn eviction_stands_off_after_losing_the_write_lock() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.deferred_durability = true;
        let store = Store::open(&config).unwrap();
        put_evictable_entries(&store, dir.path(), 3);

        let build = Store::open(&config).unwrap();
        build.db.execute_batch("BEGIN IMMEDIATE").unwrap();

        let mut gc_config = config.clone();
        gc_config.max_size = 1;
        let gc = Store::open(&gc_config).unwrap();
        let started = std::time::Instant::now();
        let stats = gc.evict().unwrap();
        let elapsed = started.elapsed();
        build.db.execute_batch("ROLLBACK").unwrap();

        assert_eq!(stats.entries_locked, 3, "{stats:?}");
        assert!(
            elapsed >= EVICTION_WRITE_PAUSE * 3,
            "one pause per lost write lock, swept in {elapsed:?}"
        );
    }

    /// A sweep with enough removals to use up a write slice pauses between
    /// slices, so builds waiting on the write lock get it. Without the pause
    /// a waiting `put` sat in SQLite's busy handler for most of the sweep.
    ///
    /// The slice is shrunk so a few hundred removals use it up. With the
    /// production slice this needed 1500 entries, and a pacer broken into
    /// pausing after every removal then slept 150 ms 1500 times, past the
    /// mutation lane's timeout.
    #[test]
    fn eviction_pauses_between_write_slices() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.deferred_durability = true;
        let store = Store::open(&config).unwrap();
        put_evictable_entries(&store, dir.path(), 500);

        let mut gc_config = config.clone();
        gc_config.max_size = 1;
        let mut gc = Store::open(&gc_config).unwrap();
        let slice = Duration::from_millis(5);
        let pause = Duration::from_millis(20);
        gc.eviction_pacing = (slice, pause);
        let started = std::time::Instant::now();
        let stats = gc.evict().unwrap();
        let elapsed = started.elapsed();

        assert_eq!(stats.entries_evicted, 500, "{stats:?}");
        let writing = Duration::from_millis(stats.evict_write_ms);
        assert!(
            writing >= slice,
            "fixture too small to use up a slice: {stats:?}"
        );
        assert!(
            elapsed >= writing + pause,
            "{writing:?} of writes must include a pause, swept in {elapsed:?}"
        );
    }

    #[test]
    fn blob_refcount_drift_reads_the_store_without_the_write_lock() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let payload = b"probe shared blob";
        for key in ["probe_a", "probe_b"] {
            let output = dir.path().join(format!("{key}.rlib"));
            fs::write(&output, payload).unwrap();
            store
                .put(
                    key,
                    "probelib",
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
        assert_eq!(
            store.blob_refcount_drift().unwrap(),
            crate::BlobRefcountDrift::default()
        );

        store
            .db
            .execute_batch(
                "UPDATE blobs SET refcount = refcount + 1;
                 INSERT INTO blobs (hash, size, refcount) VALUES ('unowned', 4096, 2);",
            )
            .unwrap();
        let expected = crate::BlobRefcountDrift {
            unowned: 1,
            unowned_bytes: 4096,
            too_high: 1,
            too_high_bytes: payload.len() as u64,
            ..Default::default()
        };
        assert_eq!(store.blob_refcount_drift().unwrap(), expected);

        // A writer holding the lock does not block the probe.
        let writer = Connection::open(config.index_db_path()).unwrap();
        writer.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert_eq!(store.blob_refcount_drift().unwrap(), expected);
        let ro = open_index_db_readonly(&config.index_db_path()).unwrap();
        assert_eq!(crate::blob_refcount_drift(&ro).unwrap(), expected);
    }
}
