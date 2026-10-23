//! Windows file metadata operations for client-owned restores.
use super::{Path, invalid};
use std::{fs, io};

pub(super) fn permissions(mode: u32, file: &fs::File) -> io::Result<fs::Permissions> {
    let mut permissions = file.metadata()?.permissions();
    permissions.set_readonly(mode & 0o200 == 0);
    Ok(permissions)
}

#[allow(clippy::permissions_set_readonly_false)]
pub(super) fn touch(path: &Path) -> io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_OPEN_REPARSE_POINT, FILE_READ_ATTRIBUTES, FILE_WRITE_ATTRIBUTES,
    };
    let sampled = kache_fs::sample_write_clock(path)?;
    let file = fs::OpenOptions::new()
        .access_mode(FILE_READ_ATTRIBUTES | FILE_WRITE_ATTRIBUTES)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(invalid("restored output is not a regular file"));
    }
    let permissions = metadata.permissions();
    let readonly = permissions.readonly();
    if readonly {
        let mut writable = permissions.clone();
        writable.set_readonly(false);
        file.set_permissions(writable)?;
    }
    let result = file.set_modified(sampled);
    if readonly {
        let _ = file.set_permissions(permissions);
    }
    result
}
