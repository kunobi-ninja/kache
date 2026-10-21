//! Owned copies of inherited compiler streams.

use std::fs::File;
use std::io;
#[cfg(unix)]
use std::os::fd::FromRawFd;

#[cfg(windows)]
#[path = "stdio_windows.rs"]
mod windows;

pub struct Stdio {
    pub input: File,
    pub output: File,
    pub error: File,
    pub terminal: Option<File>,
}

impl Stdio {
    pub fn capture() -> io::Result<Self> {
        #[cfg(unix)]
        {
            fn duplicate(fd: libc::c_int) -> io::Result<File> {
                // Closed standard descriptors are errors, without creating a
                // borrowed handle for an invalid descriptor.
                let owned = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 3) };
                if owned == -1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(unsafe { File::from_raw_fd(owned) })
            }
            Ok(Self {
                input: duplicate(0)?,
                output: duplicate(1)?,
                error: duplicate(2)?,
                terminal: std::fs::OpenOptions::new()
                    .write(true)
                    .open("/dev/tty")
                    .ok(),
            })
        }
        #[cfg(windows)]
        {
            windows::capture()
        }
        #[cfg(not(any(unix, windows)))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "standard handle capture unavailable",
            ))
        }
    }

    pub fn into_files(self) -> (Vec<File>, bool) {
        let terminal = self.terminal.is_some();
        let mut files = vec![self.input, self.output, self.error];
        files.extend(self.terminal);
        (files, terminal)
    }
}

#[cfg(all(test, unix))]
mod unix_tests {
    use super::*;
    use std::io::{Read, Seek, Write};
    use std::os::fd::AsRawFd;

    #[test]
    #[ignore = "isolated standard descriptors for the Unix stream capture test"]
    fn capture_fixture() {
        if std::env::var("KACHE_TEST_STDIO").unwrap() == "closed" {
            assert_eq!(unsafe { libc::close(0) }, 0);
            assert_eq!(
                Stdio::capture().err().unwrap().raw_os_error(),
                Some(libc::EBADF)
            );
            return;
        }
        let mut captured = Stdio::capture().unwrap();
        for file in [&captured.input, &captured.output, &captured.error] {
            let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFD) };
            assert!(flags >= 0);
            assert_ne!(flags & libc::FD_CLOEXEC, 0);
        }
        let mut input = Vec::new();
        captured.input.read_to_end(&mut input).unwrap();
        assert_eq!(input, b"native-input-\xff");
        captured.output.write_all(b"owned-output-\xfe").unwrap();
        captured.error.write_all(b"owned-error-\xfd").unwrap();
    }

    #[test]
    fn stream_capture_preserves_bytes_and_rejects_closed_standard_descriptors() {
        let root = tempfile::tempdir().unwrap();
        let input = root.path().join("input");
        std::fs::write(&input, b"native-input-\xff").unwrap();
        for mode in ["capture", "closed"] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--ignored", "--exact", "stdio::unix_tests::capture_fixture"])
                .env("KACHE_TEST_STDIO", mode)
                .stdin(File::open(&input).unwrap())
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            if mode == "capture" {
                assert!(
                    output
                        .stdout
                        .windows(b"owned-output-\xfe".len())
                        .any(|bytes| bytes == b"owned-output-\xfe")
                );
                assert!(
                    output
                        .stderr
                        .windows(b"owned-error-\xfd".len())
                        .any(|bytes| bytes == b"owned-error-\xfd")
                );
                assert!(
                    !output
                        .stdout
                        .windows(b"owned-error-\xfd".len())
                        .any(|bytes| bytes == b"owned-error-\xfd")
                );
            }
        }
    }

    #[test]
    fn transfer_manifest_orders_standard_streams_before_an_optional_terminal() {
        for has_terminal in [false, true] {
            let mut originals = (0..4)
                .map(|_| tempfile::tempfile().unwrap())
                .collect::<Vec<_>>();
            let captured = Stdio {
                input: originals[0].try_clone().unwrap(),
                output: originals[1].try_clone().unwrap(),
                error: originals[2].try_clone().unwrap(),
                terminal: has_terminal.then(|| originals[3].try_clone().unwrap()),
            };
            let (mut files, terminal) = captured.into_files();
            assert_eq!(terminal, has_terminal);
            assert_eq!(files.len(), if has_terminal { 4 } else { 3 });
            for (index, file) in files.iter_mut().enumerate() {
                let expected = format!("stream-{index}");
                file.write_all(expected.as_bytes()).unwrap();
                originals[index].rewind().unwrap();
                let mut actual = String::new();
                originals[index].read_to_string(&mut actual).unwrap();
                assert_eq!(actual, expected);
            }
        }
    }
}

#[cfg(all(test, windows))]
mod windows_tests {
    use super::*;
    use std::io::{Read, Write};

    #[test]
    #[ignore = "child process for captured_streams_keep_input_output_and_inheritance_separate"]
    fn capture_fixture() {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Foundation::{GetHandleInformation, HANDLE_FLAG_INHERIT};
        let mut captured = Stdio::capture().unwrap();
        for file in [&captured.input, &captured.output, &captured.error] {
            let mut flags = 0;
            assert_ne!(
                unsafe { GetHandleInformation(file.as_raw_handle(), &mut flags) },
                0
            );
            assert_eq!(flags & HANDLE_FLAG_INHERIT, 0);
        }
        let mut input = String::new();
        captured.input.read_to_string(&mut input).unwrap();
        assert_eq!(input, "compiler input");
        captured.output.write_all(b"owned compiler stdout").unwrap();
        captured.error.write_all(b"owned compiler stderr").unwrap();
    }

    #[test]
    fn captured_streams_keep_input_output_and_inheritance_separate() {
        let root = tempfile::tempdir().unwrap();
        let input = root.path().join("input");
        std::fs::write(&input, "compiler input").unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "stdio::windows_tests::capture_fixture",
            ])
            .stdin(File::open(input).unwrap())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("owned compiler stdout"));
        assert!(String::from_utf8_lossy(&output.stderr).contains("owned compiler stderr"));
        assert!(!String::from_utf8_lossy(&output.stdout).contains("owned compiler stderr"));
    }
}
