//! Apply engine-selected file operations with the original client's authority.

use kache_protocol::execution::{
    NativeString,
    workspace::{Command as Workspace, Reply},
};
#[cfg(unix)]
use std::os::{
    fd::AsRawFd,
    unix::fs::{OpenOptionsExt, PermissionsExt},
};

use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, Seek},
    path::{Path, PathBuf},
};

#[cfg(windows)]
mod windows;
#[cfg(windows)]
use windows::touch;

#[derive(Default)]
pub(super) struct Files {
    next_id: u64,
    stages: HashMap<u64, Stage>,
}

struct Stage {
    file: kache_fs::StagedFile,
    target: PathBuf,
}

impl Files {
    pub(super) fn execute(
        &mut self,
        operation: Workspace,
        mut input: Option<File>,
    ) -> io::Result<Reply> {
        let needs_contents = matches!(
            &operation,
            Workspace::Write { .. } | Workspace::PrepareBytes { .. }
        );
        if needs_contents != input.is_some() {
            return Err(invalid("workspace contents do not match the operation"));
        }
        match operation {
            Workspace::Reflink { source, target } => {
                kache_fs::try_reflink(&path(source)?, &path(target)?)?
            }
            Workspace::Copy {
                source,
                target,
                executable,
            } => {
                kache_fs::copy_writable(&path(source)?, &path(target)?, executable)?;
            }
            Workspace::Hardlink { source, target } => fs::hard_link(path(source)?, path(target)?)?,
            Workspace::Remove { path: value } => remove(&path(value)?)?,
            Workspace::CreateDirectories { path: value } => fs::create_dir_all(path(value)?)?,
            Workspace::Permissions { path: value, mode } => {
                let path = path(value)?;
                let file = File::open(&path)?;
                fs::set_permissions(&path, permissions(mode, &file)?)?
            }
            Workspace::Touch { path: value } => touch(&path(value)?)?,
            Workspace::Write { path: value, mode } => {
                let target = path(value)?;
                let parent = parent(&target)?;
                fs::create_dir_all(parent)?;
                let mut stage = kache_fs::StagedFile::new_in(parent, ".kache-restore-")?;
                let mut input = input
                    .take()
                    .ok_or_else(|| invalid("missing workspace contents"))?;
                io::copy(&mut input, stage.writer())?;
                let permissions = permissions(mode, stage.file_mut())?;
                stage.file_mut().set_permissions(permissions)?;
                stage.replace(&target)?;
            }
            Workspace::PrepareFile { source, target } => {
                let source = path(source)?;
                let target = path(target)?;
                let mut stage = kache_fs::StagedFile::new_in(parent(&target)?, ".kache-restore-")?;
                let reflinked = stage.try_clone_from(&source).is_ok();
                if !reflinked {
                    stage.file_mut().set_len(0)?;
                    stage.file_mut().rewind()?;
                    io::copy(&mut File::open(source)?, stage.writer())?;
                }
                let bytes = stage.file_mut().metadata()?.len();
                return self.keep(stage, target, bytes, reflinked);
            }
            Workspace::PrepareBytes { target } => {
                let target = path(target)?;
                let mut stage = kache_fs::StagedFile::new_in(parent(&target)?, ".kache-restore-")?;
                let bytes = io::copy(
                    &mut input
                        .take()
                        .ok_or_else(|| invalid("missing workspace contents"))?,
                    stage.writer(),
                )?;
                return self.keep(stage, target, bytes, false);
            }
            Workspace::Publish { id, replace } => {
                let stage = self.stages.remove(&id).ok_or_else(unknown)?;
                if replace {
                    stage.file.replace(&stage.target)?;
                } else {
                    stage.file.publish_new(&stage.target)?;
                }
            }
            Workspace::Discard { id } => {
                drop(self.stages.remove(&id).ok_or_else(unknown)?);
            }
        }
        Ok(Reply::Done)
    }

    fn keep(
        &mut self,
        file: kache_fs::StagedFile,
        target: PathBuf,
        bytes: u64,
        reflinked: bool,
    ) -> io::Result<Reply> {
        if self.stages.len() >= 1024 {
            return Err(io::Error::other("too many staged workspace files"));
        }
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or_else(|| io::Error::other("workspace file ID exhausted"))?;
        self.stages.insert(self.next_id, Stage { file, target });
        Ok(Reply::PreparedFile {
            id: self.next_id,
            bytes,
            reflinked,
        })
    }
}

fn path(value: NativeString) -> io::Result<PathBuf> {
    let value = PathBuf::from(value.decode().map_err(invalid)?);
    if !value.is_absolute() {
        return Err(invalid("workspace paths must be absolute"));
    }
    Ok(value)
}

fn parent(path: &Path) -> io::Result<&Path> {
    path.parent()
        .ok_or_else(|| invalid("workspace file has no parent directory"))
}

fn permissions(mode: u32, file: &File) -> io::Result<fs::Permissions> {
    if mode & !0o777 != 0 {
        return Err(invalid("unsupported workspace file mode"));
    }
    #[cfg(unix)]
    {
        let _ = file;
        Ok(fs::Permissions::from_mode(mode))
    }
    #[cfg(windows)]
    {
        windows::permissions(mode, file)
    }
}

fn invalid(message: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.to_string())
}

fn unknown() -> io::Error {
    invalid("unknown staged workspace file")
}

#[cfg(unix)]
pub(super) fn touch(path: &Path) -> io::Result<()> {
    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid("restored output is not a regular file"));
    }
    let times = [
        libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_OMIT,
        },
        libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_NOW,
        },
    ];
    if unsafe { libc::futimens(file.as_raw_fd(), times.as_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[allow(clippy::permissions_set_readonly_false)]
fn remove(path: &Path) -> io::Result<()> {
    #[cfg(windows)]
    if let Ok(metadata) = fs::symlink_metadata(path)
        && metadata.is_file()
        && metadata.permissions().readonly()
    {
        let mut permissions = metadata.permissions();
        permissions.set_readonly(false);
        fs::set_permissions(path, permissions)?;
    }
    fs::remove_file(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    fn native(path: &Path) -> NativeString {
        NativeString::capture(path.as_os_str())
    }
    fn payload(bytes: &[u8]) -> File {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(bytes).unwrap();
        file.rewind().unwrap();
        file
    }

    #[test]
    fn every_platform_stages_publishes_and_discards_transformed_contents() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("output");
        let mut files = Files::default();
        let Reply::PreparedFile {
            id,
            bytes,
            reflinked,
        } = files
            .execute(
                Workspace::PrepareBytes {
                    target: native(&target),
                },
                Some(payload(b"transformed\0\xff")),
            )
            .unwrap()
        else {
            panic!("missing stage")
        };
        assert_eq!(bytes, 13);
        assert!(!reflinked);
        assert!(!target.exists());
        files
            .execute(Workspace::Publish { id, replace: false }, None)
            .unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"transformed\0\xff");
        files
            .execute(
                Workspace::Touch {
                    path: native(&target),
                },
                None,
            )
            .unwrap();
        files
            .execute(
                Workspace::Permissions {
                    path: native(&target),
                    mode: 0o644,
                },
                None,
            )
            .unwrap();
        files
            .execute(
                Workspace::Write {
                    path: native(&target),
                    mode: 0o644,
                },
                Some(payload(b"replacement")),
            )
            .unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"replacement");
        let Reply::PreparedFile { id, .. } = files
            .execute(
                Workspace::PrepareBytes {
                    target: native(&target),
                },
                Some(payload(b"discarded")),
            )
            .unwrap()
        else {
            panic!("missing stage")
        };
        files.execute(Workspace::Discard { id }, None).unwrap();
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
        files
            .execute(
                Workspace::Remove {
                    path: native(&target),
                },
                None,
            )
            .unwrap();
        assert!(!target.exists());
    }

    #[test]
    fn missing_or_unexpected_contents_fail_before_creating_outputs() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("missing/output");
        let mut files = Files::default();
        assert!(
            files
                .execute(
                    Workspace::Write {
                        path: native(&target),
                        mode: 0o644
                    },
                    None
                )
                .is_err()
        );
        assert!(!target.parent().unwrap().exists());
        assert!(
            files
                .execute(
                    Workspace::CreateDirectories {
                        path: native(&target)
                    },
                    Some(payload(b"unexpected"))
                )
                .is_err()
        );
        assert!(!target.exists());
        assert!(
            files
                .execute(
                    Workspace::CreateDirectories {
                        path: NativeString::capture("relative".as_ref())
                    },
                    None
                )
                .is_err()
        );
    }

    #[test]
    fn copying_and_preparing_files_never_modify_the_cached_blob() {
        let root = tempfile::tempdir().unwrap();
        let blob = root.path().join("blob");
        let target = root.path().join("output");
        fs::write(&blob, b"cache bytes").unwrap();
        let mut files = Files::default();
        files
            .execute(
                Workspace::Copy {
                    source: native(&blob),
                    target: native(&target),
                    executable: false,
                },
                None,
            )
            .unwrap();
        fs::write(&target, b"private").unwrap();
        assert_eq!(fs::read(&blob).unwrap(), b"cache bytes");
        let Reply::PreparedFile { id, bytes, .. } = files
            .execute(
                Workspace::PrepareFile {
                    source: native(&blob),
                    target: native(&target),
                },
                None,
            )
            .unwrap()
        else {
            panic!("missing stage")
        };
        assert_eq!(bytes, 11);
        files
            .execute(Workspace::Publish { id, replace: true }, None)
            .unwrap();
        let mut contents = Vec::new();
        File::open(target)
            .unwrap()
            .read_to_end(&mut contents)
            .unwrap();
        assert_eq!(contents, b"cache bytes");
        assert_eq!(fs::read(blob).unwrap(), b"cache bytes");
    }
}
