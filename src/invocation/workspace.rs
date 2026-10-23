//! Encode restore requests once; native brokers only transfer their payloads.
use super::InvocationEnv;
use kache_protocol::execution::{
    NativeString,
    workspace::{Command, Reply},
};
use std::{
    fs::File,
    io::{self, Seek, Write},
};

pub(super) fn execute(
    env: &InvocationEnv,
    operation: kache_store::workspace::Operation<'_>,
    call: impl FnOnce(Command, Option<&File>) -> io::Result<Reply>,
) -> io::Result<kache_store::workspace::Outcome> {
    use kache_protocol::execution::workspace::Command as Workspace;
    use kache_store::workspace::Operation;
    let path = |path: &std::path::Path| {
        env.resolve(path)
            .map(|path| NativeString::capture(path.as_os_str()))
            .ok_or_else(|| io::Error::other("workspace path cannot be resolved"))
    };
    let mut content = None;
    let command = match operation {
        Operation::Reflink(source, target) => Workspace::Reflink {
            source: path(source)?,
            target: path(target)?,
        },
        Operation::Copy(source, target, executable) => Workspace::Copy {
            source: path(source)?,
            target: path(target)?,
            executable,
        },
        Operation::Hardlink(source, target) => Workspace::Hardlink {
            source: path(source)?,
            target: path(target)?,
        },
        Operation::Remove(value) => Workspace::Remove { path: path(value)? },
        Operation::CreateDirectories(value) => Workspace::CreateDirectories { path: path(value)? },
        Operation::Permissions(value, mode) => Workspace::Permissions {
            path: path(value)?,
            mode: mode & 0o777,
        },
        Operation::Touch(value) => Workspace::Touch { path: path(value)? },
        Operation::Write(value, bytes, mode) => {
            content = Some(content_file(bytes)?);
            Workspace::Write {
                path: path(value)?,
                mode,
            }
        }
        Operation::PrepareFile(source, target) => Workspace::PrepareFile {
            source: path(source)?,
            target: path(target)?,
        },
        Operation::PrepareBytes(target, bytes) => {
            content = Some(content_file(bytes)?);
            Workspace::PrepareBytes {
                target: path(target)?,
            }
        }
        Operation::Publish(id, replace) => Workspace::Publish { id, replace },
        Operation::Discard(id) => Workspace::Discard { id },
    };
    match call(command, content.as_ref())? {
        Reply::Done => Ok(kache_store::workspace::Outcome::Done),
        Reply::PreparedFile {
            id,
            bytes,
            reflinked,
        } => Ok(kache_store::workspace::Outcome::Prepared {
            id,
            bytes,
            reflinked,
        }),
    }
}

pub(super) fn content_file(bytes: &[u8]) -> io::Result<File> {
    #[cfg(target_os = "linux")]
    let mut file = {
        use std::os::fd::FromRawFd;
        let fd = unsafe { libc::memfd_create(c"kache-restore".as_ptr(), libc::MFD_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        unsafe { File::from_raw_fd(fd) }
    };
    #[cfg(not(target_os = "linux"))]
    let mut file = tempfile::tempfile()?;
    file.write_all(bytes)?;
    file.rewind()?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kache_store::workspace::{Operation, Outcome};
    use std::{io::Read, path::Path};

    #[test]
    fn shared_restore_encoding_resolves_paths_transfers_contents_and_keeps_stage_metadata() {
        let root = tempfile::tempdir().unwrap();
        let env = InvocationEnv::from_parts(
            std::iter::empty::<(&str, &str)>(),
            Some(root.path().to_owned()),
            None,
            root.path().to_owned(),
        );
        let target = Path::new("output");
        let source = Path::new("blob");
        let native = |path: &Path| NativeString::capture(root.path().join(path).as_os_str());
        for (operation, expected, bytes) in [
            (
                Operation::Reflink(source, target),
                Command::Reflink {
                    source: native(source),
                    target: native(target),
                },
                None,
            ),
            (
                Operation::Copy(source, target, true),
                Command::Copy {
                    source: native(source),
                    target: native(target),
                    executable: true,
                },
                None,
            ),
            (
                Operation::Hardlink(source, target),
                Command::Hardlink {
                    source: native(source),
                    target: native(target),
                },
                None,
            ),
            (
                Operation::Remove(target),
                Command::Remove {
                    path: native(target),
                },
                None,
            ),
            (
                Operation::CreateDirectories(target),
                Command::CreateDirectories {
                    path: native(target),
                },
                None,
            ),
            (
                Operation::Permissions(target, 0o104644),
                Command::Permissions {
                    path: native(target),
                    mode: 0o644,
                },
                None,
            ),
            (
                Operation::Touch(target),
                Command::Touch {
                    path: native(target),
                },
                None,
            ),
            (
                Operation::Write(target, b"payload", 0o755),
                Command::Write {
                    path: native(target),
                    mode: 0o755,
                },
                Some(b"payload".as_slice()),
            ),
            (
                Operation::PrepareFile(source, target),
                Command::PrepareFile {
                    source: native(source),
                    target: native(target),
                },
                None,
            ),
            (
                Operation::PrepareBytes(target, b"bytes"),
                Command::PrepareBytes {
                    target: native(target),
                },
                Some(b"bytes".as_slice()),
            ),
            (
                Operation::Publish(42, true),
                Command::Publish {
                    id: 42,
                    replace: true,
                },
                None,
            ),
            (Operation::Discard(42), Command::Discard { id: 42 }, None),
        ] {
            let outcome = execute(&env, operation, |request, contents| {
                assert_eq!(request, expected);
                let received = contents.map(|file| {
                    let mut bytes = Vec::new();
                    file.try_clone().unwrap().read_to_end(&mut bytes).unwrap();
                    bytes
                });
                assert_eq!(received.as_deref(), bytes);
                Ok(Reply::PreparedFile {
                    id: 19,
                    bytes: 123,
                    reflinked: true,
                })
            })
            .unwrap();
            assert!(matches!(
                outcome,
                Outcome::Prepared {
                    id: 19,
                    bytes: 123,
                    reflinked: true
                }
            ));
        }
        assert!(matches!(
            execute(&env, Operation::Publish(42, false), |request, _| {
                assert_eq!(
                    request,
                    Command::Publish {
                        id: 42,
                        replace: false
                    }
                );
                Ok(Reply::Done)
            })
            .unwrap(),
            Outcome::Done
        ));
        let error = execute(&env, Operation::Remove(target), |_, _| {
            Err(io::ErrorKind::PermissionDenied.into())
        })
        .err()
        .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    }
}
