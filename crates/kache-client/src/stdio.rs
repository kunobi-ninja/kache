//! Owned copies of inherited compiler streams.

use std::fs::File;
use std::io;
#[cfg(unix)]
use std::os::fd::FromRawFd;

pub struct Stdio {
    pub input: File,
    pub output: File,
    pub error: File,
    pub terminal: Option<File>,
}

impl Stdio {
    #[cfg(unix)]
    pub fn capture() -> io::Result<Self> {
        fn duplicate(fd: libc::c_int) -> io::Result<File> {
            // Closed standard descriptors are errors, without creating a
            // borrowed handle for an invalid descriptor.
            let owned = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 3) };
            if owned < 0 {
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
    pub fn capture() -> io::Result<Self> {
        use std::io::IsTerminal;
        use windows_sys::Win32::System::Console::{
            STD_ERROR_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
        };
        let input = duplicate_standard(STD_INPUT_HANDLE)?;
        let output = duplicate_standard(STD_OUTPUT_HANDLE)?;
        let error = duplicate_standard(STD_ERROR_HANDLE)?;
        // Console handles and Unicode conversion remain with the local
        // engine until the client can proxy them without attaching the daemon.
        if input.is_terminal() || output.is_terminal() || error.is_terminal() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "console handles require local execution",
            ));
        }
        Ok(Self {
            input,
            output,
            error,
            terminal: None,
        })
    }

    pub fn into_files(self) -> (Vec<File>, bool) {
        let terminal = self.terminal.is_some();
        let mut files = vec![self.input, self.output, self.error];
        files.extend(self.terminal);
        (files, terminal)
    }
}

#[cfg(windows)]
fn duplicate_standard(which: u32) -> io::Result<File> {
    use std::os::windows::io::FromRawHandle;
    use windows_sys::Win32::Foundation::{
        DUPLICATE_SAME_ACCESS, DuplicateHandle, INVALID_HANDLE_VALUE,
    };
    use windows_sys::Win32::System::Console::GetStdHandle;
    use windows_sys::Win32::System::Threading::GetCurrentProcess;

    let handle = unsafe { GetStdHandle(which) };
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::other("standard handle unavailable"));
    }
    let process = unsafe { GetCurrentProcess() };
    let mut owned = std::ptr::null_mut();
    if unsafe {
        DuplicateHandle(
            process,
            handle,
            process,
            &mut owned,
            0,
            0,
            DUPLICATE_SAME_ACCESS,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_handle(owned) })
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
