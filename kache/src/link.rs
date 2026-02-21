use anyhow::{Context, Result};
use std::fs;
use std::path::Path;

/// Link strategy for cache restoration.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LinkStrategy {
    /// Hard link (zero-copy, same inode). For rlib/rmeta.
    Hardlink,
    /// Reflink/copy. For bin/dylib/proc-macro (may be mutated post-build).
    Copy,
}

/// Link a cached file to the target output path using the appropriate strategy.
///
/// For hardlinks: creates a hard link from `store_path` to `target_path`.
/// Falls back to reflink, then regular copy if hardlink fails (cross-filesystem).
///
/// For copy strategy: always copies (for mutable outputs like binaries).
pub fn link_to_target(store_path: &Path, target_path: &Path, strategy: LinkStrategy) -> Result<()> {
    // Ensure parent directory exists
    if let Some(parent) = target_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating parent dir for {}", target_path.display()))?;
    }

    // Remove existing file at target (hardlink would fail if it exists)
    if target_path.exists() || target_path.symlink_metadata().is_ok() {
        // Need to make writable first in case it's a read-only hardlink
        if let Ok(meta) = fs::metadata(target_path) {
            let mut perms = meta.permissions();
            perms.set_readonly(false);
            let _ = fs::set_permissions(target_path, perms);
        }
        fs::remove_file(target_path)
            .with_context(|| format!("removing existing file at {}", target_path.display()))?;
    }

    match strategy {
        LinkStrategy::Hardlink => hardlink_with_fallback(store_path, target_path),
        LinkStrategy::Copy => copy_file(store_path, target_path),
    }
}

/// Try hardlink, fall back to reflink (on APFS/btrfs), then regular copy.
fn hardlink_with_fallback(store_path: &Path, target_path: &Path) -> Result<()> {
    // Try hardlink first
    match fs::hard_link(store_path, target_path) {
        Ok(()) => {
            tracing::debug!(
                "hardlinked {} -> {}",
                store_path.display(),
                target_path.display()
            );
            return Ok(());
        }
        Err(e) => {
            tracing::debug!(
                "hardlink failed ({}), trying reflink/copy: {} -> {}",
                e,
                store_path.display(),
                target_path.display()
            );
        }
    }

    // Try reflink (copy-on-write clone) on supported filesystems
    if try_reflink(store_path, target_path).is_ok() {
        tracing::debug!(
            "reflinked {} -> {}",
            store_path.display(),
            target_path.display()
        );
        return Ok(());
    }

    // Fall back to regular copy
    copy_file(store_path, target_path)
}

/// Try a reflink (copy-on-write) clone.
#[cfg(target_os = "macos")]
fn try_reflink(src: &Path, dst: &Path) -> Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let src_c = CString::new(src.as_os_str().as_bytes())?;
    let dst_c = CString::new(dst.as_os_str().as_bytes())?;

    // clonefile(2) on macOS / APFS
    unsafe extern "C" {
        fn clonefile(src: *const libc::c_char, dst: *const libc::c_char, flags: u32)
        -> libc::c_int;
    }

    let ret = unsafe { clonefile(src_c.as_ptr(), dst_c.as_ptr(), 0) };
    if ret == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().into())
    }
}

#[cfg(target_os = "linux")]
fn try_reflink(src: &Path, dst: &Path) -> Result<()> {
    use std::os::unix::io::AsRawFd;

    let src_file = fs::File::open(src)?;
    let dst_file = fs::File::create(dst)?;

    // FICLONE ioctl on Linux (btrfs, XFS with reflink)
    const FICLONE: libc::c_ulong = 0x40049409;

    // Cast needed: ioctl `request` is c_ulong on glibc but c_int on musl
    let ret = unsafe { libc::ioctl(dst_file.as_raw_fd(), FICLONE as _, src_file.as_raw_fd()) };
    if ret == 0 {
        Ok(())
    } else {
        // Clean up the created file on failure
        let _ = fs::remove_file(dst);
        Err(std::io::Error::last_os_error().into())
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn try_reflink(_src: &Path, _dst: &Path) -> Result<()> {
    anyhow::bail!("reflink not supported on this platform")
}

/// Regular file copy.
fn copy_file(src: &Path, dst: &Path) -> Result<()> {
    fs::copy(src, dst)
        .with_context(|| format!("copying {} to {}", src.display(), dst.display()))?;

    // Make the copy writable (the store copy is read-only)
    let meta = fs::metadata(dst)?;
    let mut perms = meta.permissions();
    perms.set_readonly(false);
    fs::set_permissions(dst, perms)?;

    tracing::debug!("copied {} -> {}", src.display(), dst.display());
    Ok(())
}

/// Update the mtime of a file to the current time.
/// For hardlinked files, this also updates the store copy (same inode),
/// which conveniently acts as an LRU access time update.
pub fn touch_mtime(path: &Path) -> Result<()> {
    let now = filetime::FileTime::now();
    filetime::set_file_mtime(path, now)
        .with_context(|| format!("updating mtime of {}", path.display()))?;
    Ok(())
}

/// Rewrite a .d (dep-info) file: replace absolute paths with relative paths for storing,
/// or expand relative paths back to absolute for restoring.
pub fn rewrite_depinfo(depinfo_path: &Path, project_dir: &Path, mode: DepInfoMode) -> Result<()> {
    let content = fs::read_to_string(depinfo_path)
        .with_context(|| format!("reading dep-info file {}", depinfo_path.display()))?;

    let project_prefix = format!("{}/", project_dir.display());
    let rewritten = match mode {
        DepInfoMode::Relativize => content.replace(&project_prefix, "./"),
        DepInfoMode::Expand => content.replace("./", &project_prefix),
    };

    // For hardlinked files, we need to unlink first to avoid modifying the store copy
    if let Ok(meta) = fs::metadata(depinfo_path) {
        // If nlink > 1, it's a hardlink — need to break the link first
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if meta.nlink() > 1 {
                let _ = fs::remove_file(depinfo_path);
            }
        }
    }

    fs::write(depinfo_path, rewritten)?;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub enum DepInfoMode {
    /// Replace absolute project paths with "./" for cross-project cache sharing
    Relativize,
    /// Expand "./" back to absolute project paths after restoring
    Expand,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hardlink() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("source.rlib");
        fs::write(&src, b"rlib content").unwrap();

        let dst = dir.path().join("subdir/output.rlib");
        link_to_target(&src, &dst, LinkStrategy::Hardlink).unwrap();

        assert!(dst.exists());
        assert_eq!(fs::read(&dst).unwrap(), b"rlib content");

        // Verify it's actually a hardlink (same inode)
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let src_ino = fs::metadata(&src).unwrap().ino();
            let dst_ino = fs::metadata(&dst).unwrap().ino();
            assert_eq!(src_ino, dst_ino);
        }
    }

    #[test]
    fn test_copy_strategy() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("source.bin");
        fs::write(&src, b"binary content").unwrap();

        let dst = dir.path().join("output.bin");
        link_to_target(&src, &dst, LinkStrategy::Copy).unwrap();

        assert!(dst.exists());
        assert_eq!(fs::read(&dst).unwrap(), b"binary content");

        // Should NOT be a hardlink
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let src_ino = fs::metadata(&src).unwrap().ino();
            let dst_ino = fs::metadata(&dst).unwrap().ino();
            assert_ne!(src_ino, dst_ino);
        }
    }

    #[test]
    fn test_overwrite_existing() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("source.rlib");
        fs::write(&src, b"new content").unwrap();

        let dst = dir.path().join("output.rlib");
        fs::write(&dst, b"old content").unwrap();

        link_to_target(&src, &dst, LinkStrategy::Hardlink).unwrap();
        assert_eq!(fs::read(&dst).unwrap(), b"new content");
    }

    #[test]
    fn test_touch_mtime() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.rlib");
        fs::write(&file, b"content").unwrap();

        // Set mtime to the past
        let past = filetime::FileTime::from_unix_time(1000000000, 0);
        filetime::set_file_mtime(&file, past).unwrap();

        touch_mtime(&file).unwrap();

        let meta = fs::metadata(&file).unwrap();
        let mtime = filetime::FileTime::from_last_modification_time(&meta);
        assert!(mtime.unix_seconds() > 1000000000);
    }

    #[test]
    fn test_depinfo_rewrite() {
        let dir = tempfile::tempdir().unwrap();
        let depfile = dir.path().join("test.d");
        fs::write(
            &depfile,
            "/home/user/project/target/debug/deps/libserde.rlib: /home/user/project/src/lib.rs",
        )
        .unwrap();

        rewrite_depinfo(
            &depfile,
            Path::new("/home/user/project"),
            DepInfoMode::Relativize,
        )
        .unwrap();

        let content = fs::read_to_string(&depfile).unwrap();
        assert!(content.contains("./target/debug"));
        assert!(content.contains("./src/lib.rs"));
        assert!(!content.contains("/home/user/project/"));

        // Now expand back
        rewrite_depinfo(
            &depfile,
            Path::new("/home/user/project"),
            DepInfoMode::Expand,
        )
        .unwrap();

        let content = fs::read_to_string(&depfile).unwrap();
        assert!(content.contains("/home/user/project/"));
    }
}
