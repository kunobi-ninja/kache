//! Unix descriptor transport for the shared workspace executor.
#[cfg(test)]
use kache_protocol::execution::NativeString;
use kache_protocol::execution::{
    unix::{Reply, Workspace},
    workspace::Reply as WorkspaceReply,
};
#[cfg(test)]
use std::io::Seek;
#[cfg(test)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::os::{fd::AsFd, unix::net::UnixDatagram};
#[cfg(test)]
use std::{
    fs,
    path::{Path, PathBuf},
};
use std::{fs::File, io};

#[derive(Default)]
pub(super) struct Files(super::super::workspace::Files);
impl Files {
    pub(super) fn execute(
        &mut self,
        operation: Workspace,
        pipes: &UnixDatagram,
    ) -> io::Result<Reply> {
        let input = if matches!(
            &operation,
            Workspace::Write { .. } | Workspace::PrepareBytes { .. }
        ) {
            let mut files = crate::descriptors::receive(pipes.as_fd(), &[0; 16], 1)?;
            Some(File::from(files.remove(0)))
        } else {
            None
        };
        self.0.execute(operation, input).map(|reply| match reply {
            WorkspaceReply::Done => Reply::Done,
            WorkspaceReply::PreparedFile {
                id,
                bytes,
                reflinked,
            } => Reply::PreparedFile {
                id,
                bytes,
                reflinked,
            },
        })
    }
}
#[cfg(test)]
fn touch(path: &Path) -> io::Result<()> {
    super::super::workspace::touch(path)
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
        let pending = fs::read_dir(root.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path != &blob && path != &target)
            .collect::<Vec<_>>();
        assert!(!pending.is_empty());
        assert!(pending.iter().all(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with(".kache-")
        }));
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
