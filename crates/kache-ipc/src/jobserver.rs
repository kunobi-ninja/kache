//! Request-owned Unix jobserver descriptors and native environment rewriting.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::process::CommandExt;
use std::process::Command;
use std::sync::Arc;

const VARIABLES: [&str; 3] = ["CARGO_MAKEFLAGS", "MAKEFLAGS", "MFLAGS"];

/// Parent descriptors stay close-on-exec. Commands retain the handles they
/// need; only the post-fork child clears close-on-exec on its own jobserver.
#[derive(Clone, Default)]
pub struct Descriptors(Arc<Vec<OwnedFd>>);

impl Descriptors {
    pub fn new(files: Vec<OwnedFd>) -> io::Result<Self> {
        if files.len() > crate::descriptors::MAX_DESCRIPTORS - 4 {
            return Err(invalid());
        }
        for file in &files {
            let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFD) };
            if flags == -1 || flags & libc::FD_CLOEXEC == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "parent jobserver descriptors must be close-on-exec",
                ));
            }
        }
        Ok(Self(Arc::new(files)))
    }
    pub fn files(&self) -> &[OwnedFd] {
        &self.0
    }
}

thread_local! {
    static CURRENT: RefCell<Descriptors> = RefCell::new(Descriptors::default());
}

pub struct Scope {
    previous: Descriptors,
    _thread: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl Scope {
    pub fn enter(descriptors: Descriptors) -> Self {
        Self {
            previous: CURRENT.with(|slot| slot.replace(descriptors)),
            _thread: std::marker::PhantomData,
        }
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        CURRENT.with(|slot| slot.replace(std::mem::take(&mut self.previous)));
    }
}

/// Configure each new child command once; hold descriptors::spawn_guard()
/// through its spawn. Parent flags and environment variables remain unchanged.
pub fn configure(command: &mut Command) {
    let descriptors = CURRENT.with(|slot| slot.borrow().clone());
    if descriptors.0.is_empty() {
        return;
    }
    unsafe {
        command.pre_exec(move || {
            for descriptor in descriptors.0.iter() {
                let fd = descriptor.as_raw_fd();
                let flags = libc::fcntl(fd, libc::F_GETFD);
                if flags == -1 || libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) != 0 {
                    return Err(io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
}

struct Pair {
    value: std::ops::Range<usize>,
    read: i32,
    write: i32,
}

fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "unsupported jobserver descriptor flags",
    )
}

fn number(bytes: &[u8]) -> io::Result<i32> {
    if bytes.is_empty() {
        return Err(invalid());
    }
    bytes.iter().try_fold(0i32, |number, byte| {
        if !byte.is_ascii_digit() {
            return Err(invalid());
        }
        number
            .checked_mul(10)
            .and_then(|n| n.checked_add(i32::from(byte - b'0')))
            .ok_or_else(invalid)
    })
}

fn pairs(value: &OsStr) -> io::Result<Vec<Pair>> {
    let bytes = value.as_bytes();
    let mut result = Vec::new();
    let mut offset = 0;
    for part in bytes.split_inclusive(|byte| byte.is_ascii_whitespace()) {
        let end = part.len() - usize::from(part.last().is_some_and(u8::is_ascii_whitespace));
        let token = &part[..end];
        for flag in [b"--jobserver-auth=".as_slice(), b"--jobserver-fds="] {
            if token == &flag[..flag.len() - 1] {
                return Err(invalid());
            }
            if let Some(auth) = token.strip_prefix(flag) {
                if auth.starts_with(b"fifo:") {
                    break;
                }
                let comma = auth
                    .iter()
                    .position(|byte| *byte == b',')
                    .ok_or_else(invalid)?;
                result.push(Pair {
                    value: offset + flag.len()..offset + token.len(),
                    read: number(&auth[..comma])?,
                    write: number(&auth[comma + 1..])?,
                });
                break;
            }
        }
        offset += part.len();
    }
    Ok(result)
}

fn relevant(key: &OsStr) -> bool {
    VARIABLES.iter().any(|name| key == OsStr::new(name))
}

/// Numeric endpoints in stable transfer order. FIFO jobservers keep their
/// native flags and need no descriptor transfer.
pub fn original_fds(env: &[(OsString, OsString)]) -> io::Result<Vec<i32>> {
    let mut fds = BTreeSet::new();
    for (_, value) in env.iter().filter(|(key, _)| relevant(key)) {
        for pair in pairs(value)? {
            fds.insert(pair.read);
            fds.insert(pair.write);
        }
    }
    if fds.len() > crate::descriptors::MAX_DESCRIPTORS - 4 {
        return Err(invalid());
    }
    Ok(fds.into_iter().collect())
}

/// Duplicate native client endpoints without borrowing possibly-invalid fds.
pub fn capture(env: &[(OsString, OsString)]) -> io::Result<(Vec<i32>, Descriptors)> {
    let originals = original_fds(env)?;
    let mut files = Vec::with_capacity(originals.len());
    for fd in &originals {
        let owned = unsafe { libc::fcntl(*fd, libc::F_DUPFD_CLOEXEC, 3) };
        if owned == -1 {
            return Err(io::Error::last_os_error());
        }
        files.push(unsafe { OwnedFd::from_raw_fd(owned) });
    }
    Ok((originals, Descriptors::new(files)?))
}

/// Rebind only jobserver numbers. Spaces, unrelated options and non-UTF-8
/// environment bytes retain their original representation.
pub fn rewrite(
    env: &[(OsString, OsString)],
    originals: &[i32],
    received: &Descriptors,
) -> io::Result<Vec<(OsString, OsString)>> {
    if originals != original_fds(env)? || originals.len() != received.0.len() {
        return Err(invalid());
    }
    env.iter()
        .map(|(key, value)| {
            if !relevant(key) {
                return Ok((key.clone(), value.clone()));
            }
            let mut output = Vec::new();
            let mut position = 0;
            for pair in pairs(value)? {
                let new_fd = |fd| {
                    originals
                        .binary_search(&fd)
                        .ok()
                        .map(|index| received.0[index].as_raw_fd())
                        .ok_or_else(invalid)
                };
                output.extend_from_slice(&value.as_bytes()[position..pair.value.start]);
                output.extend_from_slice(
                    format!("{},{}", new_fd(pair.read)?, new_fd(pair.write)?).as_bytes(),
                );
                position = pair.value.end;
            }
            output.extend_from_slice(&value.as_bytes()[position..]);
            Ok((key.clone(), OsString::from_vec(output)))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Seek, Write};
    use std::process::Stdio;

    #[test]
    fn jobserver_scope_restores_the_owned_manifest_after_a_panic() {
        let first = tempfile::tempfile().unwrap();
        let second = tempfile::tempfile().unwrap();
        let expected_first = first.as_raw_fd();
        let expected_second = second.as_raw_fd();
        let descriptors = Descriptors::new(vec![first.into(), second.into()]).unwrap();
        assert_eq!(descriptors.files().len(), 2);
        assert_eq!(descriptors.files()[0].as_raw_fd(), expected_first);
        assert_eq!(descriptors.files()[1].as_raw_fd(), expected_second);
        assert!(CURRENT.with(|slot| slot.borrow().files().is_empty()));
        {
            let _outer = Scope::enter(descriptors);
            let result = std::panic::catch_unwind(|| {
                let _nested = Scope::enter(Descriptors::default());
                assert!(CURRENT.with(|slot| slot.borrow().files().is_empty()));
                panic!("nested invocation failed");
            });
            assert!(result.is_err());
            CURRENT.with(|slot| {
                let current = slot.borrow();
                assert_eq!(current.files().len(), 2);
                assert_eq!(current.files()[0].as_raw_fd(), expected_first);
                assert_eq!(current.files()[1].as_raw_fd(), expected_second);
            });
        }
        assert!(CURRENT.with(|slot| slot.borrow().files().is_empty()));
    }

    #[test]
    fn capture_rejects_a_numeric_endpoint_that_is_not_an_open_descriptor() {
        let error = capture(&[(
            "MAKEFLAGS".into(),
            "--jobserver-auth=2147483647,2147483647".into(),
        )])
        .err()
        .unwrap();
        assert_eq!(error.raw_os_error(), Some(libc::EBADF));
    }

    #[test]
    fn only_jobserver_variables_participate_in_descriptor_transfer() {
        for key in ["CARGO_MAKEFLAGS", "MAKEFLAGS", "MFLAGS"] {
            let env = vec![
                (
                    key.into(),
                    "--jobserver-auth=9,7 --jobserver-fds=7,9".into(),
                ),
                ("UNRELATED".into(), "--jobserver-auth=invalid".into()),
            ];
            assert_eq!(original_fds(&env).unwrap(), [7, 9]);
        }
        for key in ["", "MAKEFLAGS_EXTRA", "CARGO_MAKEFLAGS_", "OTHER"] {
            assert!(
                original_fds(&[(key.into(), "--jobserver-auth=invalid".into())])
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[test]
    fn jobserver_capacity_leaves_room_for_standard_streams_and_terminal() {
        let flags = (0..30)
            .map(|index| format!("--jobserver-auth={},{}", index * 2, index * 2 + 1))
            .collect::<Vec<_>>()
            .join(" ");
        assert_eq!(
            original_fds(&[("MAKEFLAGS".into(), flags.clone().into())]).unwrap(),
            (0..60).collect::<Vec<_>>()
        );
        let overflow = format!("{flags} --jobserver-auth=60,0");
        assert_eq!(
            original_fds(&[("MAKEFLAGS".into(), overflow.into())])
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn rewrite_rejects_an_incomplete_or_misattributed_descriptor_manifest() {
        let env = vec![("MAKEFLAGS".into(), "--jobserver-auth=7,9".into())];
        let short = Descriptors::new(vec![tempfile::tempfile().unwrap().into()]).unwrap();
        assert!(rewrite(&env, &[7, 9], &short).is_err());
        let complete = Descriptors::new(vec![
            tempfile::tempfile().unwrap().into(),
            tempfile::tempfile().unwrap().into(),
        ])
        .unwrap();
        assert!(rewrite(&env, &[8, 9], &complete).is_err());
        assert!(rewrite(&env, &[7, 9], &complete).is_ok());
    }

    #[test]
    fn parent_descriptors_must_be_close_on_exec() {
        let source = tempfile::tempfile().unwrap();
        let _admission = crate::descriptors::spawn_guard();
        let raw = unsafe { libc::dup(source.as_raw_fd()) };
        assert!(raw >= 0);
        let owned = unsafe { OwnedFd::from_raw_fd(raw) };
        assert_eq!(
            Descriptors::new(vec![owned]).err().unwrap().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn native_flag_bytes_and_fifo_auth_are_preserved() {
        let first = tempfile::tempfile().unwrap();
        let second = tempfile::tempfile().unwrap();
        let original =
            OsString::from_vec(b" x=\xff\t--jobserver-auth=7,9  --jobserver-fds=7,9\n".to_vec());
        let env = vec![
            ("CARGO_MAKEFLAGS".into(), original),
            (
                "MAKEFLAGS".into(),
                "--jobserver-auth=fifo:/tmp/native jobserver".into(),
            ),
        ];
        let files = Descriptors::new(vec![first.into(), second.into()]).unwrap();
        let rewritten = rewrite(&env, &[7, 9], &files).unwrap();
        let expected = [
            b" x=\xff\t".as_slice(),
            format!(
                "--jobserver-auth={},{}  --jobserver-fds={},{}\n",
                files.0[0].as_raw_fd(),
                files.0[1].as_raw_fd(),
                files.0[0].as_raw_fd(),
                files.0[1].as_raw_fd()
            )
            .as_bytes(),
        ]
        .concat();
        assert_eq!(rewritten[0].1.as_bytes(), expected);
        assert_eq!(rewritten[1], env[1]);
        assert_eq!(
            original_fds(&[("MAKEFLAGS".into(), "--jobserver-auth=fifo:/tmp/fifo".into())])
                .unwrap(),
            Vec::<i32>::new()
        );
        for bad in [
            "--jobserver-auth",
            "--jobserver-auth=",
            "--jobserver-auth=-1,3",
            "--jobserver-fds=3,4,5",
            "--jobserver-auth=2147483648,3",
        ] {
            assert!(original_fds(&[("MAKEFLAGS".into(), bad.into())]).is_err());
        }
        assert!(rewrite(&env, &[7], &files).is_err());
    }

    #[test]
    #[ignore = "subprocess fixture for native jobserver inheritance"]
    fn inherited_jobserver_fixture() {
        let value = std::env::var_os("CARGO_MAKEFLAGS").unwrap();
        let pair = pairs(&value).unwrap().remove(0);
        let mut token = 0u8;
        assert_eq!(
            unsafe { libc::read(pair.read, (&mut token as *mut u8).cast(), 1) },
            1
        );
        assert_eq!(token, b't');
        assert_eq!(
            unsafe { libc::write(pair.write, (&token as *const u8).cast(), 1) },
            1
        );
    }

    #[test]
    fn child_inherits_only_its_owned_jobserver_after_scope_exit() {
        let mut input = tempfile::tempfile().unwrap();
        input.write_all(b"t").unwrap();
        input.rewind().unwrap();
        let mut output = tempfile::tempfile().unwrap();
        let env = vec![(
            "CARGO_MAKEFLAGS".into(),
            format!(
                "--jobserver-auth={},{}",
                input.as_raw_fd(),
                output.as_raw_fd()
            )
            .into(),
        )];
        let (originals, captured) = capture(&env).unwrap();
        let rewritten = rewrite(&env, &originals, &captured).unwrap();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "jobserver::tests::inherited_jobserver_fixture",
                "--ignored",
                "--nocapture",
            ])
            .env("CARGO_MAKEFLAGS", &rewritten[0].1)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        {
            let _scope = Scope::enter(captured.clone());
            configure(&mut command);
        }
        for file in captured.files() {
            assert_ne!(
                unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
                0
            );
        }
        drop(captured);
        let child = {
            let _admission = crate::descriptors::spawn_guard();
            command.spawn().unwrap()
        };
        let result = child.wait_with_output().unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        output.rewind().unwrap();
        let mut bytes = Vec::new();
        output.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"t");
    }
}
