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

/// Open the index database read-only. No schema work, no WAL/synchronous
/// pragma churn — `query_only` hard-refuses any accidental write, and the
/// short busy timeout lets a caller skip a figure the index is too busy to
/// answer instead of waiting for it.
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
    /// Candidates an automatic sweep kept because the remote delivered them
    /// within [`IMPORT_PIN`] (#1008). Included in entries_pinned.
    #[serde(default)]
    pub entries_import_pinned: usize,
    /// Time spent in the eviction writes themselves, each entry's removal
    /// with its busy waits, summed over the run. Next to `entries_locked` it
    /// shows how much of a sweep went to waiting on builds for the index
    /// write lock.
    #[serde(default)]
    pub evict_write_ms: u64,
    /// Bytes a size-driven sweep found held by live files outside the store:
    /// last-reference blobs a target directory still clones or hardlinks.
    /// Evicting them frees nothing, so the sweep leaves them out of the bytes
    /// it has to free (kunobi-ninja/kache#1206).
    #[serde(default)]
    pub bytes_held: u64,
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
    /// File hash memo rows deleted as not written for a month (#1206).
    pub file_hashes_pruned: usize,
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

/// How long an automatic sweep keeps an entry the remote delivered, used or
/// not (kunobi-ninja/kache#1008). A CI job imports a warm set, then runs
/// several cargo commands; an upload between them used to evict whatever the
/// job had not touched for [`EVICTION_IDLE_GRACE`], and the next command
/// downloaded it again or missed. Six hours covers the longest GitHub-hosted
/// job, and bounds how long a long-lived daemon keeps imports nobody used.
pub const IMPORT_PIN: Duration = Duration::from_secs(6 * 3600);

/// Who started a sweep. Automatic sweeps (after an upload, on a size hint,
/// the daemon's timer, the detached worker) keep recent imports; a sweep the
/// user asked for does not, so `kache gc` can always get back under budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SweepOrigin {
    Automatic,
    Requested,
}

impl SweepOrigin {
    /// Seconds within which an import is kept, or `None` when imports get
    /// no protection beyond [`EVICTION_IDLE_GRACE`].
    fn import_pin_secs(self) -> Option<i64> {
        match self {
            SweepOrigin::Automatic => Some(IMPORT_PIN.as_secs() as i64),
            SweepOrigin::Requested => None,
        }
    }
}

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
    // When the remote delivered this entry, in unix seconds; NULL for an
    // entry this machine built. Automatic eviction keeps recent imports
    // (kunobi-ninja/kache#1008, see [`IMPORT_PIN`]).
    let _ = db.execute_batch("ALTER TABLE entries ADD COLUMN imported_at INTEGER");
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
/// 6: `entries.imported_at`, so automatic eviction can keep what the remote
///    delivered for the job still running (#1008).
const INDEX_SCHEMA_GENERATION: i64 = 6;

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

    /// An entry's `meta.json` as written, with none of [`Store::get`]'s work:
    /// no blob verification, hit accounting or eviction. For a caller that
    /// just stored the entry and wants its file list.
    pub fn stored_meta(&self, cache_key: &str) -> Option<EntryMeta> {
        let content = fs::read(self.entry_dir(cache_key).join("meta.json")).ok()?;
        serde_json::from_slice(&content).ok()
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
    #[cfg(any(test, feature = "test-support"))]
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
            // Every writer funnels through here, and the index checks over
            // committed metadata read these names back, so the same predicate
            // gates both: a name the verifier would call corrupt metadata must
            // never reach `meta.json` in the first place.
            anyhow::ensure!(
                kache_format::is_safe_stored_artifact_name(store_name),
                "refusing to cache an artifact with an unsafe name: {store_name}"
            );
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
                .map(|meta| crate::filesystem::is_executable(&meta))
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
            "INSERT OR REPLACE INTO entries (cache_key, crate_name, crate_type, profile, num_features, size, content_hash, compile_time_ms, key_schema, committed, imported_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1, unixepoch())",
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
                "INSERT OR IGNORE INTO entries (cache_key, crate_name, crate_type, profile, num_features, size, content_hash, compile_time_ms, key_schema, committed, imported_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, unixepoch())",
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
                    || !kache_format::is_safe_stored_artifact_name(&file.name)
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
                || !kache_format::is_safe_stored_artifact_name(&file.name)
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
    #[cfg(any(test, feature = "test-support"))]
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

    /// The per-key structures nothing else bounds: stale key lock files,
    /// unused input predictions and old file hash rows. The caller holds `gc.lock`. A pass that fails
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
        let file_hashes_pruned =
            self.file_hash_cache()
                .prune_file_hashes()
                .unwrap_or_else(|error| {
                    tracing::warn!("gc: file hash pruning failed: {error}");
                    0
                });
        // After the prune, so the one-time copy is as small as it gets.
        match self.file_hash_cache().rebuild_file_hashes_without_rowid() {
            Ok(true) => tracing::info!("gc: rebuilt file_hashes without rowid"),
            Ok(false) => {}
            Err(error) => tracing::warn!("gc: file hash table rebuild failed: {error}"),
        }
        HousekeepingStats {
            key_locks_removed: locks.removed,
            key_locks_remaining: locks.remaining(),
            predictions_pruned,
            file_hashes_pruned,
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
        self.eviction_candidates_for(SweepOrigin::Requested)
    }

    /// [`Self::eviction_candidates`], with each entry's import protection
    /// judged for a sweep started by `origin`.
    pub fn eviction_candidates_for(
        &self,
        origin: SweepOrigin,
    ) -> Result<Vec<crate::eviction::EntryFeatures>> {
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
                    last_accessed >= datetime('now', ?1),
                    COALESCE(imported_at >= unixepoch() - ?2, 0)
             FROM entries",
        )?;
        let rows = stmt
            .query_map(
                params![
                    format!("-{} seconds", EVICTION_IDLE_GRACE.as_secs()),
                    origin.import_pin_secs()
                ],
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
                        recently_imported: row.get(10)?,
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
        held: &std::collections::HashMap<String, u64>,
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
            if features.is_some_and(|f| f.recently_imported) {
                stats.entries_pinned += 1;
                stats.entries_import_pinned += 1;
                continue;
            }
            // Found held by this sweep's probe, and counted there: its
            // removal would be refused.
            if held.contains_key(key) {
                continue;
            }
            let write_started = std::time::Instant::now();
            let removal = self.remove_entry_guarded(key, Some(EVICTION_IDLE_GRACE));
            let took = write_started.elapsed();
            eviction_writes += took;
            // Every removal that returns Ok held the index write lock, also
            // when it kept the entry (pinned, or still linked into a target
            // directory), so each one counts toward the write slice.
            if removal.is_ok()
                && let Some(pause) = pacer.after_write(took)
            {
                std::thread::sleep(pause);
            }
            match removal {
                Ok(GuardedRemoval::Reclaimed(reclaim)) => {
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
        origin: SweepOrigin,
    ) -> Result<GcStats> {
        let candidates = self.eviction_candidates_for(origin)?;
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
        // Only a size-driven sweep has a byte budget for held bytes to leave.
        let held = if stop_at.is_some() {
            self.held_by_live_files(&candidates)?
        } else {
            std::collections::HashMap::new()
        };
        let bytes_held: u64 = held.values().sum();
        let stop_at = stop_at.map(|(current, target)| (current.saturating_sub(bytes_held), target));
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
        let mut stats = self.apply_eviction(
            &order,
            &by_key,
            policy.name(),
            stop_at,
            shadow.as_ref(),
            &durable_upload_keys,
            &held,
        );
        stats.bytes_held = bytes_held;
        stats.entries_unreclaimable += held.len();
        Ok(stats)
    }

    /// Per entry, the bytes of blobs it holds the last reference to that a
    /// live file outside the store (a clone or hardlink in a target
    /// directory) holds as well. Evicting the entry would unlink those
    /// names and free nothing (kunobi-ninja/kache#725), so a size-driven
    /// sweep counts them apart instead of evicting everything else trying to
    /// get under a budget they keep it over (kunobi-ninja/kache#1206).
    ///
    /// Empty under `gc_evict_shared`, which evicts such entries anyway.
    fn held_by_live_files(
        &self,
        candidates: &[crate::eviction::EntryFeatures],
    ) -> Result<std::collections::HashMap<String, u64>> {
        let mut held = std::collections::HashMap::new();
        if self.config.gc_evict_shared {
            return Ok(held);
        }
        let keys: std::collections::HashSet<&str> =
            candidates.iter().map(|entry| entry.key.as_str()).collect();
        // Read every row first: the probes below touch the filesystem, and a
        // read statement left open across them would pin the WAL snapshot.
        let last_refs = {
            let mut stmt = self.db.prepare(
                "SELECT eb.cache_key, b.hash, b.size, b.refcount, eb.refs
                 FROM entry_blobs eb JOIN blobs b ON b.hash = eb.hash",
            )?;
            stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .into_iter()
            .filter(|(key, _, _, refcount, refs)| {
                keys.contains(key.as_str()) && holds_last_reference(*refcount, *refs)
            })
            .collect::<Vec<_>>()
        };
        for (key, hash, size, _, _) in last_refs {
            if crate::filesystem::blob_has_external_retainer(&self.blob_path(&hash)) {
                *held.entry(key).or_insert(0) += size.max(0) as u64;
            }
        }
        Ok(held)
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
        self.evict_for(SweepOrigin::Requested)
    }

    /// [`Self::evict`] for a sweep started by `origin`.
    pub fn evict_for(&self, origin: SweepOrigin) -> Result<GcStats> {
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
            origin,
        )
    }

    /// Evict entries older than the given duration.
    pub fn evict_older_than(&self, hours: u64) -> Result<GcStats> {
        self.evict_with(
            &crate::eviction::OlderThanPolicy { hours },
            None,
            SweepOrigin::Requested,
        )
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
            &std::collections::HashMap::new(),
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
        self.evict_duplicate_entries_for(SweepOrigin::Requested)
    }

    /// [`Self::evict_duplicate_entries`] for a sweep started by `origin`.
    pub fn evict_duplicate_entries_for(&self, origin: SweepOrigin) -> Result<GcStats> {
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
            origin,
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

        // Eviction only: refuse to drop an entry whose last-ref blobs are
        // still cloned into a worktree (kunobi-ninja/kache#725). Unlinking
        // those names frees no disk and destroys a still-usable hit.
        // Explicit `remove_entry` (purge / doctor) passes `skip_if_idle_lt =
        // None` and still unlinks. The filesystem probe runs before the write
        // lock is taken: the lock does not stop a restore from linking a
        // blob, so probing under it adds no safety and keeps builds waiting.
        let retained_blobs: Vec<(&str, i64)> =
            if skip_if_idle_lt.is_some() && !self.config.gc_evict_shared {
                let mut held_refs: std::collections::HashMap<&str, i64> =
                    std::collections::HashMap::new();
                for hash in &hashes {
                    *held_refs.entry(hash.as_str()).or_insert(0) += 1;
                }
                held_refs
                    .into_iter()
                    .filter(|(hash, _)| {
                        crate::filesystem::blob_has_external_retainer(&self.blob_path(hash))
                    })
                    .collect()
            } else {
                Vec::new()
            };

        // IMMEDIATE takes the write lock before the first read. A DEFERRED
        // transaction would read first and upgrade to a writer at the DELETE,
        // and SQLite fails that upgrade at once with SQLITE_BUSY (or
        // SQLITE_BUSY_SNAPSHOT) without calling the busy handler. A build
        // writing to the index at that moment then made the sweep skip the
        // entry instead of waiting a few milliseconds for it.
        let tx = rusqlite::Transaction::new_unchecked(
            &self.db,
            rusqlite::TransactionBehavior::Immediate,
        )?;

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

        // A blob found retained above blocks the removal only while this
        // entry holds its last references, and that needs the lock.
        for (hash, held) in retained_blobs {
            let rc: i64 = tx.query_row(
                "SELECT refcount FROM blobs WHERE hash = ?1",
                params![hash],
                |row| row.get(0),
            )?;
            if holds_last_reference(rc, held) {
                return Ok(RemovalAttempt::Unreclaimable);
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

    /// Test-only: delete a file that may share a blob's blocks (a `put`
    /// source, a stand-in target) and wait until the sweep's retainer check
    /// no longer sees it holding them.
    ///
    /// A reflinked ingest shares the source's blocks. XFS frees an unlinked
    /// inode's blocks in a background worker, so FIEMAP still reports the
    /// blob as a clone for a moment after the unlink, and a sweep in that
    /// window skips the entry as unreclaimable (kunobi-ninja/kache#1241).
    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn remove_clone_for_test(&self, path: &Path) {
        let blob = self.blob_path(&crate::file_hash::hash_file(path).unwrap());
        fs::remove_file(path).unwrap();
        for _ in 0..2000 {
            if !crate::filesystem::blob_has_external_retainer(&blob) {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!(
            "{} still shares blocks 10s after {} was removed",
            blob.display(),
            path.display()
        );
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

/// Does an entry holding `held` references to a blob hold all of the blob's
/// `rc` remaining ones? An `rc` of zero or less means the index no longer
/// counts the blob at all.
fn holds_last_reference(rc: i64, held: i64) -> bool {
    rc > 0 && rc <= held
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
mod tests;
