use anyhow::{Context, Result};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

static ATOMIC_TMP_NONCE: AtomicU64 = AtomicU64::new(0);

/// fsync a directory so a rename/create into it survives a crash. Without it,
/// a file's contents can be durable while its directory entry is not. No-op on non-Unix:
/// Windows has no directory-handle fsync and `File::open` on a directory fails.
#[cfg(unix)]
fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    fs::File::open(dir)?.sync_all()
}
#[cfg(not(unix))]
fn fsync_dir(_dir: &Path) -> std::io::Result<()> {
    Ok(())
}

/// fsync a file so its bytes are durable on disk.
/// On Windows, FlushFileBuffers requires write access.
pub(crate) fn fsync_file(path: &Path) -> std::io::Result<()> {
    #[cfg(windows)]
    let file = fs::OpenOptions::new().write(true).open(path)?;
    #[cfg(not(windows))]
    let file = fs::File::open(path)?;
    file.sync_all()
}

/// Whether a failed rename is a transient Windows state worth retrying.
/// ERROR_ACCESS_DENIED (5) and ERROR_SHARING_VIOLATION (32) surface when a
/// concurrent put/remove leaves the destination delete-pending or open; both
/// clear on their own. No such transient rename error exists off Windows.
fn is_transient_rename_error(e: &std::io::Error) -> bool {
    #[cfg(windows)]
    {
        matches!(e.raw_os_error(), Some(5) | Some(32))
    }
    #[cfg(not(windows))]
    {
        let _ = e;
        false
    }
}

/// Backoff before the next rename retry: 1, 2, 4, … ms capped at 64 ms
/// (≈0.3 s total across the retry budget), bounding how long a wedged Windows
/// destination can delay a write.
fn rename_backoff_ms(attempt: u32) -> u64 {
    (1u64 << attempt.min(6)).min(64)
}

/// Remove a file, clearing the read-only attribute first to ensure deletion
/// succeeds even on Windows when the file was marked read-only.
fn remove_file_robust(path: &Path) -> std::io::Result<()> {
    if let Ok(meta) = fs::metadata(path) {
        let mut perms = meta.permissions();
        if perms.readonly() {
            perms.set_readonly(false);
            let _ = fs::set_permissions(path, perms);
        }
    }
    fs::remove_file(path)
}

/// Perform an atomic file replacement. This creates a temporary file beside the
/// destination path, executes `write_fn` to populate it, flushes it (`fsync_file`),
/// copies permissions from the original destination (if it exists) to preserve file
/// modes, and atomic renames it with transient error retries on Windows.
///
/// Returns `Ok(true)` if the rename succeeded.
/// If `allow_concurrent_winner` is `true`, a concurrent writer winning the race to create
/// the file is treated as a benign lost race and returns `Ok(false)`. If `allow_concurrent_winner`
/// is `false`, it retries or errors out on conflicts.
pub(crate) fn atomic_write_and_replace<F>(
    dest_path: &Path,
    allow_concurrent_winner: bool,
    write_fn: F,
) -> Result<bool>
where
    F: FnOnce(&Path) -> Result<()>,
{
    let nonce = ATOMIC_TMP_NONCE.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let file_name = dest_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("file");
    let temp_path = dest_path.with_file_name(format!(".{file_name}.{pid}.{nonce}.tmp"));

    let res = (|| -> Result<()> {
        write_fn(&temp_path)?;
        fsync_file(&temp_path).context("flushing temp file")?;
        Ok(())
    })();

    if let Err(e) = res {
        let _ = remove_file_robust(&temp_path);
        return Err(e);
    }

    // Preserve the destination's permissions so atomic replacement does not widen them.
    // Setting permissions is best-effort (e.g. read-only mounts or filesystem limitations).
    if let Ok(meta) = fs::metadata(dest_path) {
        let _ = fs::set_permissions(&temp_path, meta.permissions());
    }

    const MAX_ATTEMPTS: u32 = 10;
    let mut last_err = None;
    for attempt in 0..MAX_ATTEMPTS {
        match fs::rename(&temp_path, dest_path) {
            Ok(()) => {
                if let Some(parent) = dest_path.parent() {
                    let _ = fsync_dir(parent);
                }
                return Ok(true);
            }
            Err(e) => {
                if allow_concurrent_winner && dest_path.is_file() {
                    let _ = remove_file_robust(&temp_path);
                    return Ok(false);
                }
                if dest_path.is_dir() || !is_transient_rename_error(&e) {
                    let _ = remove_file_robust(&temp_path);
                    return Err(e).context("atomic rename");
                }
                last_err = Some(e);
                std::thread::sleep(std::time::Duration::from_millis(rename_backoff_ms(attempt)));
            }
        }
    }

    let _ = remove_file_robust(&temp_path);
    if allow_concurrent_winner && dest_path.is_file() {
        return Ok(false);
    }
    let err = last_err.unwrap_or_else(|| std::io::Error::other("atomic rename failed"));
    Err(anyhow::Error::new(err).context("atomic rename failed after retries"))
}

pub(crate) fn atomic_replace(dest_path: &Path, bytes: &[u8]) -> Result<()> {
    atomic_write_and_replace(dest_path, false, |temp_path| {
        fs::write(temp_path, bytes)?;
        Ok(())
    })?;
    Ok(())
}

pub(crate) fn cleanup_temp_files(
    dir: &Path,
    prefix: &str,
    min_age: std::time::Duration,
) -> Result<()> {
    if let Ok(entries) = fs::read_dir(dir) {
        let now = std::time::SystemTime::now();
        let expected_prefix = format!(".{prefix}.");
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str())
                && name.starts_with(&expected_prefix)
                && name.ends_with(".tmp")
                && let Ok(meta) = entry.metadata()
                && let Ok(modified) = meta.modified()
                && let Ok(age) = now.duration_since(modified)
                && age > min_age
            {
                let _ = remove_file_robust(&path);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rename_backoff_ms_caps_at_64() {
        assert_eq!(rename_backoff_ms(0), 1);
        assert_eq!(rename_backoff_ms(1), 2);
        assert_eq!(rename_backoff_ms(5), 32);
        assert_eq!(rename_backoff_ms(6), 64);
        assert_eq!(rename_backoff_ms(9), 64);
    }

    #[test]
    fn is_transient_rename_error_only_flags_windows_race_codes() {
        use std::io::Error;
        let access_denied = Error::from_raw_os_error(5);
        let sharing_violation = Error::from_raw_os_error(32);
        let other = Error::from_raw_os_error(2);
        #[cfg(windows)]
        {
            assert!(is_transient_rename_error(&access_denied));
            assert!(is_transient_rename_error(&sharing_violation));
            assert!(!is_transient_rename_error(&other));
        }
        #[cfg(not(windows))]
        {
            assert!(!is_transient_rename_error(&access_denied));
            assert!(!is_transient_rename_error(&sharing_violation));
            assert!(!is_transient_rename_error(&other));
        }
    }

    #[test]
    fn test_atomic_replace_writes_content_and_syncs() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("dest.txt");

        atomic_replace(&dest, b"hello world").unwrap();
        assert_eq!(fs::read(&dest).unwrap(), b"hello world");
    }

    #[test]
    #[cfg(unix)]
    fn test_atomic_replace_preserves_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("dest.txt");

        // Write initial file and set permissions to 0o600
        fs::write(&dest, b"initial").unwrap();
        fs::set_permissions(&dest, fs::Permissions::from_mode(0o600)).unwrap();

        // Do atomic replacement
        atomic_replace(&dest, b"new content").unwrap();

        // Verify content and that mode 0o600 was preserved
        assert_eq!(fs::read(&dest).unwrap(), b"new content");
        let meta = fs::metadata(&dest).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o600);
    }

    #[test]
    fn test_cleanup_temp_files_only_removes_matching_stale_temps() {
        let dir = tempfile::tempdir().unwrap();

        // Pattern: .<prefix>.<pid>.<nonce>.tmp
        let temp_live = dir.path().join(".testfile.1234.0.tmp");
        let temp_stale = dir.path().join(".testfile.5678.1.tmp");
        let other_file = dir.path().join(".otherfile.1234.0.tmp");
        let unrelated = dir.path().join("unrelated.txt");

        fs::write(&temp_live, b"live temp").unwrap();
        fs::write(&temp_stale, b"stale temp").unwrap();
        fs::write(&other_file, b"other temp").unwrap();
        fs::write(&unrelated, b"unrelated").unwrap();

        // Adjust mtimes so only temp_stale and other_file are older than 5 minutes
        let stale_time = std::time::SystemTime::now() - std::time::Duration::from_secs(400);
        filetime::set_file_mtime(
            &temp_stale,
            filetime::FileTime::from_system_time(stale_time),
        )
        .unwrap();
        filetime::set_file_mtime(
            &other_file,
            filetime::FileTime::from_system_time(stale_time),
        )
        .unwrap();

        // Run cleanup with prefix "testfile" and min_age 5 minutes (300 secs)
        cleanup_temp_files(dir.path(), "testfile", std::time::Duration::from_secs(300)).unwrap();

        // stale temp must be deleted
        assert!(!temp_stale.exists());
        // live temp must still exist (not old enough)
        assert!(temp_live.exists());
        // other temp must still exist (prefix is different)
        assert!(other_file.exists());
        // unrelated file must still exist
        assert!(unrelated.exists());
    }

    // Regression guard for #196: `fsync_file` must durably flush a freshly
    // written (writable) file. The bug was a read-only handle — fine for Unix
    // `fsync(2)`, but Windows `FlushFileBuffers` rejects it with
    // ERROR_ACCESS_DENIED, so every blob store failed ("flushing blob to disk").
    // Passes on Unix before and after; the real failing→passing signal is on
    // Windows, where the old read-only-handle form errored here.
    #[test]
    fn fsync_file_flushes_a_writable_blob() {
        let dir = tempfile::tempdir().unwrap();
        let blob = dir.path().join("blob.bin");
        fs::write(&blob, b"some bytes").unwrap();
        fsync_file(&blob).expect("fsync of a writable file must succeed on all platforms");
    }

    #[test]
    fn fsync_dir_succeeds_on_a_real_directory() {
        // On Unix this exercises the open+sync_all path; elsewhere it's a no-op.
        let dir = tempfile::tempdir().unwrap();
        fsync_dir(dir.path()).expect("fsync of a directory must not error");
    }
}
