//! Cross-process advisory and activity markers.

use std::path::Path;

pub const WARN_SESSION_SECS: u64 = 300;

/// Print `message` to stderr at most once per [`WARN_SESSION_SECS`] window,
/// even across the hundreds of parallel wrapper processes a single build spawns.
///
/// Each wrapper is its own process, so a `static Once` only dedups within one
/// compilation — a build then repeats the same advisory hundreds of times
/// (kunobi-ninja/kache#508). Dedup therefore has to be cross-process: a marker
/// file holding a timestamp, guarded by an flock so two wrappers can't decide
/// to warn simultaneously.
///
/// Best-effort by construction: if the marker can't be created we warn rather
/// than go silent — a duplicated advisory beats a swallowed one.
///
/// Returns whether this call actually emitted the message (for tests).
pub fn warn_once_per_session(marker: &Path, session_secs: u64, message: &str) -> bool {
    if let Ok(metadata) = std::fs::symlink_metadata(marker)
        && metadata.file_type().is_symlink()
    {
        eprintln!("{message}");
        return true;
    }
    if marker_is_fresh(marker, session_secs) {
        return false; // already warned this session
    }
    let Some(lock_file) = open_marker_for_lock(marker) else {
        eprintln!("{message}");
        return true;
    };
    match lock_file.try_lock() {
        Ok(()) => {}
        // Contended: another wrapper is emitting this warning right now.
        Err(std::fs::TryLockError::WouldBlock) => return false,
        // The lock itself is broken — NOT contention (e.g. a filesystem with no
        // working locks). Treating that as "someone else is warning" would
        // silence the advisory forever, so warn best-effort instead.
        Err(std::fs::TryLockError::Error(e)) => {
            tracing::debug!("warn-once marker lock failed ({e}); warning anyway");
            eprintln!("{message}");
            return true;
        }
    }
    // Re-check under the lock — another wrapper may have warned between our
    // first check and acquiring the lock. Read through the handle that OWNS the
    // lock: on Windows the lock is mandatory (`LockFileEx`) and blocks
    // cross-handle reads, so `marker_is_fresh` (which opens its own handle)
    // would always read "stale" here and let a second wrapper warn again — on
    // the very platform this advisory targets. Same reason
    // `write_marker_timestamp` writes through the locked handle (#348).
    finish_warn_once_per_session(&lock_file, session_secs, message)
}

/// Re-check and update a warn-once marker while its lock is held, then release
/// the lock explicitly. Relying on `File` drop is racy with concurrent process
/// spawning: a fork can briefly inherit a duplicate descriptor that keeps the
/// lock alive after this function returns.
pub fn finish_warn_once_per_session(
    lock_file: &std::fs::File,
    session_secs: u64,
    message: &str,
) -> bool {
    let emitted = if marker_file_is_fresh(lock_file, session_secs) {
        false
    } else {
        eprintln!("{message}");
        write_marker_timestamp(lock_file);
        true
    };
    let _ = std::fs::File::unlock(lock_file);
    emitted
}

/// Open a marker file safely for locking and updating. Refuses symlinks and
/// non-regular files up front, and opens with `O_NOFOLLOW` on Unix to prevent
/// symlink attacks and arbitrary file truncation in shared temporary directories.
pub fn open_marker_for_lock(marker: &Path) -> Option<std::fs::File> {
    if let Ok(metadata) = std::fs::symlink_metadata(marker) {
        if !metadata.file_type().is_file() {
            return None;
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            if is_windows_reparse_point(metadata.file_attributes()) {
                return None; // Refuse reparse points explicitly
            }
        }
    }

    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }

    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        // FILE_FLAG_OPEN_REPARSE_POINT (0x00200000) opens the reparse point itself
        // without following it to the target file.
        options.custom_flags(0x0020_0000);
    }

    let file = options.open(marker).ok()?;

    // Post-open verification: ensure the opened file handle itself is a regular file.
    let meta = file.metadata().ok()?;
    if !meta.file_type().is_file() {
        return None;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if is_windows_reparse_point(meta.file_attributes()) {
            return None; // Refuse reparse points explicitly
        }
    }

    Some(file)
}

#[cfg(any(windows, test))]
fn is_windows_reparse_point(attributes: u32) -> bool {
    attributes & 0x400 != 0
}

pub fn marker_is_fresh(marker: &std::path::Path, timeout_secs: u64) -> bool {
    if let Ok(metadata) = std::fs::symlink_metadata(marker)
        && !metadata.file_type().is_file()
    {
        return false;
    }
    let content = match std::fs::read_to_string(marker) {
        Ok(c) => c,
        Err(_) => return false,
    };
    timestamp_is_fresh(&content, timeout_secs)
}

/// Marker freshness read through an ALREADY-OPEN handle. A caller holding the
/// exclusive lock must use this rather than [`marker_is_fresh`]: on Windows the
/// lock is mandatory and blocks reads from any other handle (#348).
pub fn marker_file_is_fresh(mut file: &std::fs::File, timeout_secs: u64) -> bool {
    use std::io::{Read, Seek, SeekFrom};

    let mut content = String::new();
    if file.seek(SeekFrom::Start(0)).is_err() || file.read_to_string(&mut content).is_err() {
        return false;
    }
    timestamp_is_fresh(&content, timeout_secs)
}

/// Is a marker's timestamp within `timeout_secs` of now? Accepts both the
/// legacy bare-epoch format and the v1 session record (`v1 <ts> <id>`), so
/// freshness checks work across marker generations.
pub fn timestamp_is_fresh(content: &str, timeout_secs: u64) -> bool {
    match parse_session_marker(content) {
        Some((ts, _)) => timestamp_is_fresh_at(ts, timeout_secs, now_epoch_secs()),
        None => false, // legacy "1" marker or corrupt — treat as stale
    }
}

/// Write the current Unix epoch to the marker file, reusing the caller's
/// already-locked handle.
///
/// The caller holds an exclusive lock on this file (see `maybe_trigger_prefetch`).
/// On Windows that lock is *mandatory* (`LockFileEx`), so writing through a
/// *separate* handle (e.g. `std::fs::write`) to the locked file fails with a
/// lock violation — the timestamp never lands and every rustc re-detects a new
/// build session, re-firing the prefetch hint. Writing through the same handle
/// that owns the lock is always permitted. (kache #348)
pub fn write_marker_timestamp(mut file: &std::fs::File) {
    use std::io::{Seek, SeekFrom, Write};
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // Truncate any previous (longer) timestamp and rewrite from the start.
    let _ = file.set_len(0);
    let _ = file.seek(SeekFrom::Start(0));
    let _ = file.write_all(now.to_string().as_bytes());
    let _ = file.flush();
}

/// Parse a session marker: `v1 <unix-epoch-secs> <session_id>`, or the legacy
/// bare `<unix-epoch-secs>` (empty session id). Returns `(timestamp, id)`.
pub fn parse_session_marker(content: &str) -> Option<(u64, String)> {
    let content = content.trim();
    if let Some(rest) = content.strip_prefix("v1 ") {
        let mut parts = rest.splitn(2, ' ');
        let ts: u64 = parts.next()?.parse().ok()?;
        let id = parts.next().unwrap_or("").trim().to_string();
        return Some((ts, id));
    }
    content.parse().ok().map(|ts| (ts, String::new()))
}

pub fn now_epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Is `ts` within `timeout_secs` of `now`? Extracted (with an injectable
/// `now`) so freshness is unit-testable without clock races.
pub fn timestamp_is_fresh_at(ts: u64, timeout_secs: u64, now: u64) -> bool {
    now.saturating_sub(ts) < timeout_secs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marker_handle_reads_freshness_from_the_start() {
        use std::io::{Seek, SeekFrom, Write};

        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("session");
        let mut file = open_marker_for_lock(&marker).unwrap();
        write_marker_timestamp(&file);
        file.seek(SeekFrom::End(0)).unwrap();
        assert!(marker_file_is_fresh(&file, 60));

        for content in ["", "invalid timestamp", "0"] {
            file.set_len(0).unwrap();
            file.seek(SeekFrom::Start(0)).unwrap();
            file.write_all(content.as_bytes()).unwrap();
            assert!(!marker_file_is_fresh(&file, 60));
        }
        assert!(open_marker_for_lock(dir.path()).is_none());
        assert!(!marker_is_fresh(dir.path(), 60));
    }

    #[test]
    fn windows_reparse_flag_is_checked_independently_of_other_attributes() {
        assert!(!is_windows_reparse_point(0));
        assert!(!is_windows_reparse_point(0x20));
        assert!(is_windows_reparse_point(0x400));
        assert!(is_windows_reparse_point(0x420));
    }

    /// A build spawns hundreds of wrapper processes, so "warn once" has to hold
    /// ACROSS processes, not just within one (#508). The marker is the only
    /// thing carrying that state — a second wrapper hitting a fresh marker must
    /// stay quiet, and a stale marker must let the advisory through again.
    #[test]
    fn warn_once_per_session_dedups_across_processes_via_the_marker() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("cow-warn");

        assert!(
            warn_once_per_session(&marker, 300, "advisory"),
            "first wrapper in the session must warn"
        );
        assert!(
            !warn_once_per_session(&marker, 300, "advisory"),
            "a later wrapper in the same session must stay quiet"
        );

        // A session window of 0 makes any marker stale — a fresh `cargo` command
        // after a gap warns again rather than staying silent forever.
        assert!(
            warn_once_per_session(&marker, 0, "advisory"),
            "a stale marker must let the advisory through again"
        );
    }

    #[test]
    fn warn_once_per_session_unlocks_even_with_a_duplicated_descriptor() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("cow-warn");
        let lock_file = open_marker_for_lock(&marker).unwrap();
        lock_file.try_lock().unwrap();
        let inherited = lock_file.try_clone().unwrap();

        assert!(finish_warn_once_per_session(&lock_file, 0, "advisory"));
        drop(lock_file);

        let contender = open_marker_for_lock(&marker).unwrap();
        assert!(
            contender.try_lock().is_ok(),
            "the explicit unlock must release the lock even while a duplicated \
             descriptor remains open"
        );
        let _ = contender.unlock();
        drop(inherited);
    }

    #[test]
    #[cfg(unix)]
    fn warn_once_per_session_refuses_symlink() {
        let temp = tempfile::TempDir::new().unwrap();
        let target = temp.path().join("target_file");
        std::fs::write(&target, "some sensitive content").unwrap();

        let marker = temp.path().join("marker_symlink");
        std::os::unix::fs::symlink(&target, &marker).unwrap();

        // If we call warn_once_per_session, it should print the message,
        // but it MUST NOT truncate or modify the target file!
        let warned = warn_once_per_session(&marker, 300, "warning message");
        assert!(warned);

        // Verify target file is untouched.
        let content = std::fs::read_to_string(&target).unwrap();
        assert_eq!(content, "some sensitive content");
    }

    #[test]
    #[cfg(unix)]
    fn open_marker_for_lock_refuses_symlink_target() {
        let temp = tempfile::TempDir::new().unwrap();
        let target = temp.path().join("target_file");
        std::fs::write(&target, "sensitive target content").unwrap();

        let marker = temp.path().join("marker_symlink");
        std::os::unix::fs::symlink(&target, &marker).unwrap();

        // open_marker_for_lock must refuse symlink targets up front
        assert!(open_marker_for_lock(&marker).is_none());

        // Verify target file remains completely untouched
        let content = std::fs::read_to_string(&target).unwrap();
        assert_eq!(content, "sensitive target content");
    }

    #[test]
    fn marker_is_fresh_reads_timestamp_and_window() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join(".build-session");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // A just-written stamp is fresh within the window.
        std::fs::write(&marker, now.to_string()).unwrap();
        assert!(marker_is_fresh(&marker, 60));

        // An old stamp is stale.
        std::fs::write(&marker, (now - 120).to_string()).unwrap();
        assert!(!marker_is_fresh(&marker, 60));

        // Empty, missing, and legacy/non-numeric markers are treated as stale.
        std::fs::write(&marker, "").unwrap();
        assert!(!marker_is_fresh(&marker, 60));
        std::fs::write(&marker, "1-legacy").unwrap();
        assert!(!marker_is_fresh(&marker, 60));
        assert!(!marker_is_fresh(&dir.path().join("nope"), 60));
    }

    /// A marker written slightly in the future can happen under clock skew; the
    /// saturating age calculation should treat it as fresh, not stale.
    #[test]
    fn marker_is_fresh_accepts_future_timestamp_from_clock_skew() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join(".build-session");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        std::fs::write(&marker, (now + 60).to_string()).unwrap();
        assert!(marker_is_fresh(&marker, 300));
    }

    #[test]
    fn session_marker_roundtrip_carries_id_and_freshness() {
        // v1 record: fresh timestamp + id parse back out.
        let now = now_epoch_secs();
        let content = format!("v1 {now} abcd1234efgh5678");
        let (ts, id) = parse_session_marker(&content).expect("v1 record parses");
        assert_eq!(ts, now);
        assert_eq!(id, "abcd1234efgh5678");
        assert!(timestamp_is_fresh(&content, 300));
    }

    #[test]
    fn session_marker_accepts_legacy_bare_timestamp() {
        // Old wrappers wrote a bare epoch; it parses with an empty id, so
        // freshness checks work across marker generations (mixed fleets).
        let now = now_epoch_secs();
        let (ts, id) = parse_session_marker(&now.to_string()).expect("legacy parses");
        assert_eq!(ts, now);
        assert!(id.is_empty());
        // Corrupt / non-numeric stays stale.
        assert!(parse_session_marker("garbage").is_none());
        assert!(parse_session_marker("v1 notanumber id").is_none());
    }

    #[test]
    fn write_marker_timestamp_roundtrips_to_fresh() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join(".build-session");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&marker)
            .unwrap();
        write_marker_timestamp(&file);
        drop(file);
        // The stamp it wrote must read back as fresh and be a parseable epoch.
        let content = std::fs::read_to_string(&marker).unwrap();
        assert!(content.trim().parse::<u64>().is_ok(), "got {content:?}");
        assert!(marker_is_fresh(&marker, 60));
    }

    /// Regression for kache #348: the build-session marker must record a fresh
    /// timestamp even though `maybe_trigger_prefetch` writes it *while still
    /// holding the exclusive lock* on the same file.
    ///
    /// On Windows `File::try_lock` is a *mandatory* `LockFileEx` lock, so a
    /// write through a *second* handle (`std::fs::write`) to the locked file
    /// fails with a lock violation — the timestamp never lands, the marker
    /// stays empty, and every subsequent rustc re-detects a "new build
    /// session" and re-fires the prefetch hint (the 1147-crate spam in the
    /// bug report). On Unix `flock(2)` is advisory and the second write
    /// succeeds, which is why this only reproduces on Windows and was never
    /// caught by the Linux/macOS `cargo test` jobs.
    #[test]
    fn build_session_marker_persists_while_lock_is_held() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join(".build-session");

        // Mirror maybe_trigger_prefetch exactly: open the marker, take the
        // exclusive lock, then persist the freshness timestamp while the lock
        // is still held.
        let lock_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&marker)
            .unwrap();
        assert!(
            lock_file.try_lock().is_ok(),
            "the first lock on a fresh marker must succeed"
        );

        write_marker_timestamp(&lock_file);

        // Release the lock before reading back: on Windows a mandatory lock
        // also blocks cross-handle reads, so `marker_is_fresh` (which opens
        // its own handle) could only observe the write after we unlock.
        let _ = std::fs::File::unlock(&lock_file);
        drop(lock_file);

        assert!(
            marker_is_fresh(&marker, 300),
            "marker must record a fresh timestamp even though the writer held \
             the exclusive lock; otherwise every rustc re-fires the prefetch hint"
        );
    }
}
