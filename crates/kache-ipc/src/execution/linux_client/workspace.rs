//! Apply engine-selected file operations with the original client's authority.

use kache_protocol::execution::{
    NativeString,
    unix::{Reply, Workspace},
};
use std::os::{
    fd::{AsFd, AsRawFd},
    unix::{
        fs::{OpenOptionsExt, PermissionsExt},
        net::UnixDatagram,
    },
};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, Seek},
    path::{Path, PathBuf},
};

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
        pipes: &UnixDatagram,
    ) -> io::Result<Reply> {
        // Consume the payload before validating paths or creating a stage.
        // A rejected operation must not leave its bytes for the next request.
        let mut input = if matches!(
            &operation,
            Workspace::Write { .. } | Workspace::PrepareBytes { .. }
        ) {
            Some(contents(pipes)?)
        } else {
            None
        };
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
            Workspace::Remove { path: value } => fs::remove_file(path(value)?)?,
            Workspace::CreateDirectories { path: value } => fs::create_dir_all(path(value)?)?,
            Workspace::Permissions { path: value, mode } => {
                fs::set_permissions(path(value)?, permissions(mode)?)?
            }
            Workspace::Touch { path: value } => touch(&path(value)?)?,
            Workspace::Write { path: value, mode } => {
                let target = path(value)?;
                let parent = parent(&target)?;
                fs::create_dir_all(parent)?;
                let mut stage = kache_fs::StagedFile::new_in(parent, ".kache-restore-")?;
                let mut input = input
                    .take()
                    .ok_or_else(|| super::invalid("missing workspace contents"))?;
                io::copy(&mut input, stage.writer())?;
                stage.file_mut().set_permissions(permissions(mode)?)?;
                stage.replace(&target)?;
            }
            Workspace::PrepareFile { source, target } => {
                let source = path(source)?;
                let target = path(target)?;
                let mut stage = kache_fs::StagedFile::new_in(parent(&target)?, ".kache-restore-")?;
                let reflinked = kache_fs::try_clone_extents_into(&source, stage.file_mut()).is_ok();
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
                        .ok_or_else(|| super::invalid("missing workspace contents"))?,
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

fn contents(pipes: &UnixDatagram) -> io::Result<File> {
    let mut files = crate::descriptors::receive(pipes.as_fd(), &[0; 16], 1)?;
    Ok(File::from(files.remove(0)))
}

fn path(value: NativeString) -> io::Result<PathBuf> {
    let value = PathBuf::from(value.decode().map_err(super::invalid)?);
    if !value.is_absolute() {
        return Err(super::invalid("workspace paths must be absolute"));
    }
    Ok(value)
}

fn parent(path: &Path) -> io::Result<&Path> {
    path.parent()
        .ok_or_else(|| super::invalid("workspace file has no parent directory"))
}

fn permissions(mode: u32) -> io::Result<fs::Permissions> {
    if mode & !0o777 != 0 {
        return Err(super::invalid("unsupported workspace file mode"));
    }
    Ok(fs::Permissions::from_mode(mode))
}

fn unknown() -> io::Error {
    super::invalid("unknown staged workspace file")
}

fn touch(path: &Path) -> io::Result<()> {
    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(super::invalid("restored output is not a regular file"));
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::os::unix::fs::MetadataExt;

    fn native(path: &Path) -> NativeString {
        NativeString::capture(path.as_os_str())
    }

    #[test]
    fn touching_outputs_refuses_symlinks_and_never_blocks_on_a_fifo() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("file");
        let alias = root.path().join("alias");
        fs::write(&target, b"bytes").unwrap();
        std::os::unix::fs::symlink(&target, &alias).unwrap();
        assert_eq!(touch(&alias).unwrap_err().raw_os_error(), Some(libc::ELOOP));
        let fifo = root.path().join("fifo");
        use std::os::unix::ffi::OsStrExt;
        let name = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        let (done, waiting) = std::sync::mpsc::channel();
        std::thread::scope(|threads| {
            let writer_fifo = fifo.clone();
            let rescue = threads.spawn(move || {
                if waiting
                    .recv_timeout(std::time::Duration::from_secs(1))
                    .is_ok()
                {
                    return false;
                }
                // Release a blocking open before asserting, including under mutation.
                let _writer = fs::OpenOptions::new()
                    .write(true)
                    .custom_flags(libc::O_NONBLOCK)
                    .open(&writer_fifo)
                    .unwrap();
                true
            });
            assert!(touch(&fifo).is_err());
            let _ = done.send(());
            assert!(
                !rescue.join().unwrap(),
                "touch blocked while opening a FIFO"
            );
        });
    }

    #[test]
    fn client_copy_keeps_the_blob_independent_and_sets_the_requested_mode() {
        let root = tempfile::tempdir().unwrap();
        let blob = root.path().join("blob");
        let target = root.path().join("target");
        fs::write(&blob, b"cached bytes").unwrap();
        fs::set_permissions(&blob, fs::Permissions::from_mode(0o444)).unwrap();
        let (_, pipes) = UnixDatagram::pair().unwrap();
        let mut files = Files::default();
        for executable in [false, true] {
            files
                .execute(
                    Workspace::Copy {
                        source: native(&blob),
                        target: native(&target),
                        executable,
                    },
                    &pipes,
                )
                .unwrap();
            assert_eq!(fs::read(&target).unwrap(), b"cached bytes");
            assert_ne!(
                fs::metadata(&blob).unwrap().ino(),
                fs::metadata(&target).unwrap().ino()
            );
            assert_eq!(
                fs::metadata(&target).unwrap().mode() & 0o777,
                if executable { 0o755 } else { 0o644 }
            );
            fs::write(&target, b"private edit").unwrap();
            assert_eq!(fs::read(&blob).unwrap(), b"cached bytes");
        }
    }

    #[test]
    fn staged_publication_keeps_race_winners_and_cleans_unpublished_files() {
        let root = tempfile::tempdir().unwrap();
        let blob = root.path().join("blob");
        let target = root.path().join("target");
        fs::write(&blob, b"cached bytes").unwrap();
        let (_, pipes) = UnixDatagram::pair().unwrap();
        let mut files = Files::default();
        let Reply::PreparedFile { id, bytes, .. } = files
            .execute(
                Workspace::PrepareFile {
                    source: native(&blob),
                    target: native(&target),
                },
                &pipes,
            )
            .unwrap()
        else {
            panic!("no stage");
        };
        assert_eq!(bytes, 12);
        assert!(!target.exists());
        fs::write(&target, b"race winner").unwrap();
        assert_eq!(
            files
                .execute(Workspace::Publish { id, replace: false }, &pipes)
                .unwrap_err()
                .kind(),
            io::ErrorKind::AlreadyExists
        );
        assert_eq!(fs::read(&target).unwrap(), b"race winner");
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 2);
        let Reply::PreparedFile { id, .. } = files
            .execute(
                Workspace::PrepareFile {
                    source: native(&blob),
                    target: native(&target),
                },
                &pipes,
            )
            .unwrap()
        else {
            panic!("no stage");
        };
        files
            .execute(Workspace::Publish { id, replace: true }, &pipes)
            .unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"cached bytes");
        files
            .execute(
                Workspace::PrepareFile {
                    source: native(&blob),
                    target: native(&target),
                },
                &pipes,
            )
            .unwrap();
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 3);
        drop(files);
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 2);
    }

    #[test]
    fn transformed_outputs_use_descriptors_instead_of_large_control_frames() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("nested/output");
        let bytes = vec![0xab; 10 << 20];
        let mut contents = tempfile::tempfile().unwrap();
        contents.write_all(&bytes).unwrap();
        contents.rewind().unwrap();
        let (sender, receiver) = UnixDatagram::pair().unwrap();
        crate::descriptors::send(sender.as_fd(), &[0; 16], &[contents.as_fd()]).unwrap();
        let mut files = Files::default();
        files
            .execute(
                Workspace::Write {
                    path: native(&target),
                    mode: 0o644,
                },
                &receiver,
            )
            .unwrap();
        let mut actual = Vec::new();
        File::open(&target)
            .unwrap()
            .read_to_end(&mut actual)
            .unwrap();
        assert_eq!(actual, bytes);
        assert_eq!(fs::read_dir(target.parent().unwrap()).unwrap().count(), 1);
        files
            .execute(
                Workspace::Touch {
                    path: native(&target),
                },
                &receiver,
            )
            .unwrap();
    }

    #[test]
    fn client_operations_reject_relative_paths_and_non_regular_timestamp_targets() {
        let root = tempfile::tempdir().unwrap();
        let (_, pipes) = UnixDatagram::pair().unwrap();
        let mut files = Files::default();
        assert!(
            files
                .execute(
                    Workspace::Remove {
                        path: native(Path::new("relative"))
                    },
                    &pipes
                )
                .is_err()
        );
        assert!(
            files
                .execute(
                    Workspace::Permissions {
                        path: native(root.path()),
                        mode: 0o4777
                    },
                    &pipes
                )
                .is_err()
        );
        assert!(
            files
                .execute(
                    Workspace::Touch {
                        path: native(root.path())
                    },
                    &pipes
                )
                .is_err()
        );
        assert!(
            files
                .execute(
                    Workspace::Publish {
                        id: 88,
                        replace: false
                    },
                    &pipes
                )
                .is_err()
        );
        assert!(
            files
                .execute(Workspace::Discard { id: 88 }, &pipes)
                .is_err()
        );
    }

    #[test]
    fn rejected_writes_discard_their_payload_before_the_next_operation() {
        use std::io::{Seek, Write};
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("output");
        let (sender, receiver) = UnixDatagram::pair().unwrap();
        let mut files = Files::default();
        for (path, bytes, accepted) in [
            (
                PathBuf::from("invalid-relative"),
                b"rejected".as_slice(),
                false,
            ),
            (target.clone(), b"current".as_slice(), true),
        ] {
            let mut payload = tempfile::tempfile().unwrap();
            payload.write_all(bytes).unwrap();
            payload.rewind().unwrap();
            crate::descriptors::send(sender.as_fd(), &[0; 16], &[payload.as_fd()]).unwrap();
            assert_eq!(
                files
                    .execute(
                        Workspace::Write {
                            path: native(&path),
                            mode: 0o644
                        },
                        &receiver
                    )
                    .is_ok(),
                accepted
            );
        }
        assert_eq!(fs::read(target).unwrap(), b"current");
    }
}
