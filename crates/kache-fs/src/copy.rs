//! File copies with an explicit distinction between shared blocks and shared inodes.

use std::{fs, io, path::Path};

#[cfg(target_os = "linux")]
use native_reflink_linux as native_reflink;
#[cfg(target_os = "macos")]
use native_reflink_macos as native_reflink;
#[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
use native_reflink_unsupported as native_reflink;
#[cfg(windows)]
use native_reflink_windows as native_reflink;

/// Clone data into a new destination using the filesystem's copy-on-write API.
/// Existing destinations are never replaced. An unsupported filesystem returns
/// an error so the caller can choose its fallback. This never creates a hardlink.
/// Metadata follows the native clone operation; use `set_writable_permissions`
/// when the destination needs a specified writable mode.
pub fn try_reflink(src: &Path, dst: &Path) -> io::Result<()> {
    native_reflink(src, dst)
}

#[cfg(target_os = "macos")]
fn native_reflink_macos(src: &Path, dst: &Path) -> io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let src_c = CString::new(src.as_os_str().as_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let dst_c = CString::new(dst.as_os_str().as_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

    // clonefile(2) on macOS / APFS
    unsafe extern "C" {
        fn clonefile(src: *const libc::c_char, dst: *const libc::c_char, flags: u32)
        -> libc::c_int;
    }

    let ret = unsafe { clonefile(src_c.as_ptr(), dst_c.as_ptr(), 0) };
    if ret == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(target_os = "linux")]
fn native_reflink_linux(src: &Path, dst: &Path) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;

    let src_file = fs::File::open(src)?;
    let dst_file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dst)?;

    // FICLONE ioctl on Linux (btrfs, XFS with reflink)
    const FICLONE: libc::c_ulong = 0x40049409;

    // Cast needed: ioctl `request` is c_ulong on glibc but c_int on musl
    let ret = unsafe { libc::ioctl(dst_file.as_raw_fd(), FICLONE as _, src_file.as_raw_fd()) };
    if ret == 0 {
        Ok(())
    } else {
        let error = io::Error::last_os_error();
        drop(dst_file);
        let _ = fs::remove_file(dst);
        Err(error)
    }
}

#[cfg(windows)]
fn native_reflink_windows(src: &Path, dst: &Path) -> io::Result<()> {
    use std::io::{Read, Seek, SeekFrom};
    use std::mem::size_of;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::System::IO::DeviceIoControl;
    use windows_sys::Win32::System::Ioctl::{
        DUPLICATE_EXTENTS_DATA, FSCTL_DUPLICATE_EXTENTS_TO_FILE,
    };

    let mut src_file = fs::File::open(src)?;
    let len = src_file.metadata()?.len();

    // Create a new, writable destination without replacing an existing name.
    let mut dst_file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(dst)?;

    let result = (|| -> io::Result<()> {
        if len == 0 {
            return Ok(()); // empty file: dst already created empty, nothing to clone
        }

        // Cluster size of the destination volume; clone ranges must be aligned to
        // it. A file smaller than one cluster has no aligned range at all, so bail
        // before the FSCTL rather than issuing a call that cannot succeed — this is
        // the common case for the small files.
        let cluster = windows_cluster_size(dst)?;
        if len < cluster {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "file smaller than one cluster",
            ));
        }

        let clone_len = (len / cluster) * cluster;

        // Allocate the destination clusters and set EOF to the cloned prefix.
        dst_file.set_len(clone_len)?;

        let src_h = src_file.as_raw_handle();
        let dst_h = dst_file.as_raw_handle();
        // Each FSCTL range must be cluster-aligned and strictly < 4 GiB.
        let max_chunk = (((4u64 << 30) - 1) / cluster) * cluster;
        if max_chunk == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "cluster exceeds clone range",
            ));
        }
        let mut off = 0u64;
        while off < clone_len {
            let chunk = (clone_len - off).min(max_chunk);
            let data = DUPLICATE_EXTENTS_DATA {
                FileHandle: src_h as _,
                SourceFileOffset: off as i64,
                TargetFileOffset: off as i64,
                ByteCount: chunk as i64,
            };
            let mut returned: u32 = 0;
            let ok = unsafe {
                DeviceIoControl(
                    dst_h as _,
                    FSCTL_DUPLICATE_EXTENTS_TO_FILE,
                    &data as *const DUPLICATE_EXTENTS_DATA as *const _,
                    size_of::<DUPLICATE_EXTENTS_DATA>() as u32,
                    std::ptr::null_mut(),
                    0,
                    &mut returned,
                    std::ptr::null_mut(),
                )
            };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
            off += chunk;
        }

        // Byte-copy the sub-cluster tail, if any, then set the exact final length.
        if clone_len < len {
            src_file.seek(SeekFrom::Start(clone_len))?;
            dst_file.seek(SeekFrom::Start(clone_len))?;
            let copied = std::io::copy(&mut (&mut src_file).take(len - clone_len), &mut dst_file)?;
            if copied != len - clone_len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "short tail copy",
                ));
            }
        }
        dst_file.set_len(len)?;
        Ok(())
    })();
    drop(dst_file);
    if result.is_err() {
        let _ = fs::remove_file(dst);
    }
    result
}

/// Allocation (cluster) size of the volume that holds `path`, in bytes.
/// Used to align ReFS block-clone ranges. `path` need not exist; its nearest
/// existing parent volume is resolved.
#[cfg(windows)]
pub fn windows_cluster_size(path: &Path) -> io::Result<u64> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{GetDiskFreeSpaceW, GetVolumePathNameW};

    let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let mut root = [0u16; 260];
    let ok = unsafe { GetVolumePathNameW(wide.as_ptr(), root.as_mut_ptr(), root.len() as u32) };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    let (mut spc, mut bps, mut _free, mut _total): (u32, u32, u32, u32) = (0, 0, 0, 0);
    let ok =
        unsafe { GetDiskFreeSpaceW(root.as_ptr(), &mut spc, &mut bps, &mut _free, &mut _total) };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    let cluster = spc as u64 * bps as u64;
    if cluster == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "zero cluster size",
        ));
    }
    Ok(cluster)
}

#[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
fn native_reflink_unsupported(_src: &Path, _dst: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "reflink not supported on this platform",
    ))
}

/// Copy bytes and set a writable destination mode (0644 or 0755 on Unix).
/// On Windows this clears the destination's read-only attribute.
/// Like `std::fs::copy`, this can overwrite an existing destination. The caller
/// must ensure that doing so cannot modify a file shared through a hardlink.
pub fn copy_writable(src: &Path, dst: &Path, executable: bool) -> io::Result<u64> {
    let bytes = fs::copy(src, dst)?;
    set_writable_permissions(dst, executable)?;
    Ok(bytes)
}

/// Set 0644 or 0755 on Unix; clear the read-only attribute on other platforms.
/// This changes inode metadata and therefore also affects any hardlink aliases.
pub fn set_writable_permissions(path: &Path, executable: bool) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(
            path,
            fs::Permissions::from_mode(if executable { 0o755 } else { 0o644 }),
        )
    }
    #[cfg(not(unix))]
    {
        let _ = executable;
        let mut permissions = fs::metadata(path)?.permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        permissions.set_readonly(false);
        fs::set_permissions(path, permissions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::TempDir;

    #[test]
    fn cloning_never_replaces_an_existing_file_or_source_alias() {
        let dir = TempDir::new("clone-existing");
        let src = dir.write("source", 256 * 1024);
        let dst = dir.path().join("destination");
        fs::write(&dst, b"keep").unwrap();
        assert!(try_reflink(&src, &dst).is_err());
        assert_eq!(fs::read(&dst).unwrap(), b"keep");
        assert!(try_reflink(&src, &src).is_err());
        assert_eq!(fs::metadata(&src).unwrap().len(), 256 * 1024);
        assert!(try_reflink(&dir.path().join("missing"), &dst).is_err());
        assert_eq!(fs::read(&dst).unwrap(), b"keep");
    }

    #[test]
    fn missing_source_leaves_no_destination() {
        let dir = TempDir::new("clone-missing");
        let dst = dir.path().join("destination");
        assert!(try_reflink(&dir.path().join("missing"), &dst).is_err());
        assert!(!dst.exists());
    }

    #[test]
    fn writable_permissions_apply_to_an_existing_readonly_file() {
        let dir = TempDir::new("writable-mode");
        let file = dir.write("file", 32);
        let mut permissions = fs::metadata(&file).unwrap().permissions();
        permissions.set_readonly(true);
        fs::set_permissions(&file, permissions).unwrap();
        set_writable_permissions(&file, true).unwrap();
        assert!(!fs::metadata(&file).unwrap().permissions().readonly());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&file).unwrap().permissions().mode() & 0o777,
                0o755
            );
            set_writable_permissions(&file, false).unwrap();
            assert_eq!(
                fs::metadata(&file).unwrap().permissions().mode() & 0o777,
                0o644
            );
        }
        assert!(set_writable_permissions(&dir.path().join("missing"), false).is_err());
    }

    #[test]
    fn copied_file_can_be_changed_without_changing_source() {
        let dir = TempDir::new("copy-writable");
        let src = dir.write("source", 8192);
        let before = fs::read(&src).unwrap();
        let dst = dir.path().join("copy");
        assert_eq!(copy_writable(&src, &dst, false).unwrap(), 8192);
        fs::write(&dst, b"changed").unwrap();
        assert_eq!(fs::read(&src).unwrap(), before);
        assert!(!fs::metadata(&dst).unwrap().permissions().readonly());
    }

    #[cfg(any(target_os = "macos", target_os = "linux", windows))]
    #[test]
    fn successful_clone_preserves_bytes_and_isolates_later_writes() {
        let dir = TempDir::new("clone-isolation");
        let src = dir.write("source", 256 * 1024 + 123);
        let dst = dir.path().join("clone");
        let original = fs::read(&src).unwrap();
        if let Err(error) = try_reflink(&src, &dst) {
            assert!(!dst.exists(), "failed clone left a destination: {error}");
            return;
        }
        assert_eq!(fs::read(&dst).unwrap(), original);
        set_writable_permissions(&dst, false).unwrap();
        fs::write(&dst, b"changed").unwrap();
        assert_eq!(fs::read(&src).unwrap(), original);
    }
}
