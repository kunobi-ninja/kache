use crate::InodeId;
use std::{fs, io, path::Path};

/// Identity of the object at `path`, following symlinks. Valid for comparing
/// live objects on this host; an inode can be reused after deletion.
pub fn file_identity(path: &Path) -> io::Result<InodeId> {
    let metadata = fs::metadata(path)?;
    metadata_identity(path, &metadata, true).map(|(id, _)| id)
}

/// Identity of a directory. A symlink at the final component is rejected.
pub fn directory_identity(path: &Path) -> io::Result<InodeId> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path is not a directory",
        ));
    }
    metadata_identity(path, &metadata, false).map(|(id, _)| id)
}

pub(crate) fn metadata_identity(
    path: &Path,
    metadata: &fs::Metadata,
    follow: bool,
) -> io::Result<(InodeId, u64)> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let _ = (path, follow);
        Ok((
            InodeId {
                dev: metadata.dev(),
                ino: metadata.ino(),
            },
            metadata.nlink(),
        ))
    }
    #[cfg(windows)]
    {
        let _ = metadata;
        windows_metadata_identity(path, follow)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (path, metadata, follow);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "file identity is unavailable",
        ))
    }
}

#[cfg(windows)]
fn windows_metadata_identity(path: &Path, follow: bool) -> io::Result<(InodeId, u64)> {
    use std::os::windows::{fs::OpenOptionsExt, io::AsRawHandle};
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        GetFileInformationByHandle,
    };
    let flags = FILE_FLAG_BACKUP_SEMANTICS
        | if follow {
            0
        } else {
            FILE_FLAG_OPEN_REPARSE_POINT
        };
    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(flags)
        .open(path)?;
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    // SAFETY: the handle and output buffer are valid for this call.
    let ok = unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, &mut info) };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((
        windows_identity(
            info.dwVolumeSerialNumber,
            info.nFileIndexHigh,
            info.nFileIndexLow,
        ),
        u64::from(info.nNumberOfLinks),
    ))
}

#[cfg(any(windows, test))]
fn windows_identity(volume: u32, high: u32, low: u32) -> InodeId {
    InodeId {
        dev: u64::from(volume),
        ino: (u64::from(high) << 32) | u64::from(low),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::TempDir;

    #[test]
    fn windows_file_index_preserves_both_halves() {
        assert_eq!(
            windows_identity(0x1020_3040, 0x1122_3344, 0x5566_7788),
            InodeId {
                dev: 0x1020_3040,
                ino: 0x1122_3344_5566_7788
            }
        );
    }

    #[test]
    fn hardlinks_share_identity_but_copies_do_not() {
        let dir = TempDir::new("identity");
        let source = dir.write("source", 4096);
        let link = dir.path().join("link");
        let copy = dir.path().join("copy");
        fs::hard_link(&source, &link).unwrap();
        fs::copy(&source, &copy).unwrap();
        assert_eq!(
            file_identity(&source).unwrap(),
            file_identity(&link).unwrap()
        );
        assert_ne!(
            file_identity(&source).unwrap(),
            file_identity(&copy).unwrap()
        );
        assert!(directory_identity(&source).is_err());
        assert!(directory_identity(dir.path()).is_ok());
        assert!(file_identity(&dir.path().join("missing")).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn directory_identity_rejects_a_symlink() {
        let dir = TempDir::new("directory-symlink");
        let link = dir.path().join("alias");
        std::os::unix::fs::symlink(dir.path(), &link).unwrap();
        assert!(directory_identity(&link).is_err());
        assert_eq!(
            file_identity(&link).unwrap(),
            file_identity(dir.path()).unwrap()
        );
    }
}
