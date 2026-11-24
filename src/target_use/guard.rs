//! Kache 2.3.0 locks the whole of target-use.lock for every command and cleanup.
//! On Darwin and Windows its locks also conflict with overlapping byte ranges.
//! A scoped command shares byte 0 and a cleanup takes byte 1 exclusively.
//! They can coexist while the per-path locks determine whether their targets overlap.

use std::fs::{File, TryLockError};
use std::io;

#[derive(Clone, Copy)]
pub(super) enum Region {
    Whole,
    #[cfg(any(target_os = "macos", windows))]
    Byte(u32),
}

impl Region {
    #[cfg(any(target_os = "macos", windows))]
    pub(super) const COMMAND: Self = Self::Byte(0);
    #[cfg(not(any(target_os = "macos", windows)))]
    pub(super) const COMMAND: Self = Self::Whole;

    #[cfg(any(target_os = "macos", windows))]
    pub(super) const CLEANUP: Self = Self::Byte(1);
    #[cfg(not(any(target_os = "macos", windows)))]
    pub(super) const CLEANUP: Self = Self::Whole;

    pub(super) fn try_shared(self, file: &File) -> Result<(), TryLockError> {
        match self {
            Self::Whole => file.try_lock_shared(),
            #[cfg(any(target_os = "macos", windows))]
            Self::Byte(offset) => {
                platform::apply(file, offset, ByteAction::TryShared).map_err(try_error)
            }
        }
    }

    pub(super) fn shared(self, file: &File) -> io::Result<()> {
        match self {
            Self::Whole => file.lock_shared(),
            #[cfg(any(target_os = "macos", windows))]
            Self::Byte(offset) => platform::apply(file, offset, ByteAction::WaitShared),
        }
    }

    pub(super) fn try_exclusive(self, file: &File) -> Result<(), TryLockError> {
        match self {
            Self::Whole => file.try_lock(),
            #[cfg(any(target_os = "macos", windows))]
            Self::Byte(offset) => {
                platform::apply(file, offset, ByteAction::TryExclusive).map_err(try_error)
            }
        }
    }

    fn unlock(self, file: &File) -> io::Result<()> {
        match self {
            Self::Whole => file.unlock(),
            #[cfg(any(target_os = "macos", windows))]
            Self::Byte(offset) => platform::apply(file, offset, ByteAction::Unlock),
        }
    }
}

#[cfg(any(windows, target_os = "macos", test))]
#[derive(Clone, Copy)]
enum ByteAction {
    TryShared,
    WaitShared,
    TryExclusive,
    Unlock,
}

/// Unlock the same range before closing, including when another process
/// briefly inherited a descriptor across fork.
pub(super) struct Held {
    pub(super) file: File,
    pub(super) region: Region,
}

impl Drop for Held {
    fn drop(&mut self) {
        let _ = self.region.unlock(&self.file);
    }
}

#[cfg(any(target_os = "macos", windows, all(test, target_os = "linux")))]
fn try_error(error: io::Error) -> TryLockError {
    #[cfg(unix)]
    let conflicts = [libc::EAGAIN, libc::EACCES];
    #[cfg(windows)]
    let conflicts = [windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION as i32];
    if error
        .raw_os_error()
        .is_some_and(|code| conflicts.contains(&code))
    {
        TryLockError::WouldBlock
    } else {
        TryLockError::Error(error)
    }
}

#[cfg(all(test, any(target_os = "macos", target_os = "linux", windows)))]
#[test]
fn try_lock_preserves_errors_other_than_contention() {
    let failure = io::Error::new(io::ErrorKind::InvalidData, "not a lock conflict");
    assert!(
        matches!(try_error(failure), TryLockError::Error(e) if e.kind() == io::ErrorKind::InvalidData)
    );
    assert!(
        matches!(try_error(io::Error::from_raw_os_error(12345)), TryLockError::Error(e) if e.raw_os_error() == Some(12345))
    );
    #[cfg(unix)]
    let conflicts = [libc::EAGAIN, libc::EACCES];
    #[cfg(windows)]
    let conflicts = [windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION as i32];
    for code in conflicts {
        assert!(matches!(
            try_error(io::Error::from_raw_os_error(code)),
            TryLockError::WouldBlock
        ));
    }
}

// The two Linux test architectures below use the same five flock fields and
// support F_OFD_SETLK. Linux commands still take the old whole-file guard.
#[cfg(any(
    target_os = "macos",
    all(
        test,
        target_os = "linux",
        target_env = "gnu",
        any(target_arch = "x86_64", target_arch = "aarch64")
    )
))]
mod platform {
    use super::*;
    use std::os::fd::AsRawFd;

    // libc exposes Linux's lock-kind constants as int and Darwin's as short.
    #[cfg(target_os = "macos")]
    type LockKind = libc::c_short;
    #[cfg(target_os = "linux")]
    type LockKind = libc::c_int;

    fn set(file: &File, offset: u32, kind: LockKind, wait: bool) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        let kind = kind as libc::c_short;
        let range = libc::flock {
            l_start: offset.into(),
            l_len: 1,
            l_pid: 0,
            l_type: kind,
            l_whence: libc::SEEK_SET as libc::c_short,
        };
        let command = if wait {
            libc::F_OFD_SETLKW
        } else {
            libc::F_OFD_SETLK
        };
        retry_interrupted(|| {
            // SAFETY: the descriptor is open and range is a valid flock for this call.
            match unsafe { libc::fcntl(file.as_raw_fd(), command, &range) } {
                -1 => Err(io::Error::last_os_error()),
                _ => Ok(()),
            }
        })
    }

    fn retry_interrupted(mut op: impl FnMut() -> io::Result<()>) -> io::Result<()> {
        loop {
            match op() {
                Ok(()) => return Ok(()),
                Err(error) => match error.kind() {
                    io::ErrorKind::Interrupted => continue,
                    _ => return Err(error),
                },
            }
        }
    }

    pub(super) fn apply(file: &File, offset: u32, action: ByteAction) -> io::Result<()> {
        let (kind, wait) = match action {
            ByteAction::TryShared => (libc::F_RDLCK, false),
            ByteAction::WaitShared => (libc::F_RDLCK, true),
            ByteAction::TryExclusive => (libc::F_WRLCK, false),
            ByteAction::Unlock => (libc::F_UNLCK, false),
        };
        set(file, offset, kind, wait)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn file_descriptions_share_read_ranges_and_release_the_exact_byte() {
            let temp = tempfile::NamedTempFile::new().unwrap();
            let first = temp.as_file();
            let second = temp.reopen().unwrap();
            apply(first, 0, ByteAction::TryShared).unwrap();
            apply(&second, 0, ByteAction::TryShared).unwrap();
            assert!(matches!(
                try_error(apply(&second, 0, ByteAction::TryExclusive).unwrap_err()),
                TryLockError::WouldBlock
            ));
            apply(&second, 1, ByteAction::TryExclusive).unwrap();
            assert!(matches!(
                try_error(apply(first, 1, ByteAction::TryShared).unwrap_err()),
                TryLockError::WouldBlock
            ));
            apply(&second, 1, ByteAction::Unlock).unwrap();
            apply(first, 1, ByteAction::WaitShared).unwrap();
            apply(first, 0, ByteAction::Unlock).unwrap();
            apply(&second, 0, ByteAction::TryExclusive).unwrap();
        }

        #[test]
        fn a_lock_retries_an_interrupt_and_returns_the_next_result() {
            let mut results = [
                Err(io::ErrorKind::Interrupted.into()),
                Err(io::ErrorKind::PermissionDenied.into()),
            ]
            .into_iter();
            assert_eq!(
                retry_interrupted(|| results.next().unwrap())
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::PermissionDenied
            );
            assert!(results.next().is_none());
        }
    }
}

#[cfg(any(windows, test))]
impl ByteAction {
    fn windows_flags(self, exclusive: u32, immediate: u32) -> Option<u32> {
        match self {
            Self::TryShared => Some(immediate),
            Self::WaitShared => Some(0),
            Self::TryExclusive => Some(exclusive | immediate),
            Self::Unlock => None,
        }
    }
}

#[cfg(any(windows, test))]
fn windows_result(result: i32) -> io::Result<()> {
    match result {
        0 => Err(io::Error::last_os_error()),
        _ => Ok(()),
    }
}

#[cfg(test)]
#[test]
fn windows_lock_requests_select_flags_and_preserve_kernel_failure() {
    assert_eq!(ByteAction::TryShared.windows_flags(2, 1), Some(1));
    assert_eq!(ByteAction::WaitShared.windows_flags(2, 1), Some(0));
    assert_eq!(ByteAction::TryExclusive.windows_flags(2, 1), Some(3));
    assert_eq!(ByteAction::TryExclusive.windows_flags(3, 1), Some(3));
    assert_eq!(ByteAction::Unlock.windows_flags(2, 1), None);
    assert!(windows_result(0).is_err());
    assert!(windows_result(1).is_ok());
}

#[cfg(windows)]
mod platform {
    use super::*;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY, LockFileEx, UnlockFileEx,
    };
    use windows_sys::Win32::System::IO::{OVERLAPPED, OVERLAPPED_0, OVERLAPPED_0_0};

    pub(super) fn win32_range(file: &File, offset: u32, action: ByteAction) -> io::Result<()> {
        let mut at = OVERLAPPED {
            Internal: 0,
            InternalHigh: 0,
            Anonymous: OVERLAPPED_0 {
                Anonymous: OVERLAPPED_0_0 {
                    Offset: offset,
                    OffsetHigh: 0,
                },
            },
            hEvent: std::ptr::null_mut(),
        };
        // SAFETY: open_lock opens a synchronous handle. The structure identifies
        // one byte and remains valid until the selected kernel call returns.
        let result = unsafe {
            match action.windows_flags(LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY) {
                Some(flags) => LockFileEx(file.as_raw_handle(), flags, 0, 1, 0, &mut at),
                None => UnlockFileEx(file.as_raw_handle(), 0, 1, 0, &mut at),
            }
        };
        windows_result(result)
    }

    pub(super) use win32_range as apply;

    #[cfg(test)]
    #[test]
    fn unlock_releases_only_its_byte_while_both_handles_stay_open() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let owner = temp.as_file();
        let other = temp.reopen().unwrap();
        apply(owner, 0, ByteAction::TryExclusive).unwrap();
        apply(owner, 1, ByteAction::TryExclusive).unwrap();
        assert!(matches!(
            try_error(apply(&other, 1, ByteAction::TryShared).unwrap_err()),
            TryLockError::WouldBlock
        ));
        apply(owner, 1, ByteAction::Unlock).unwrap();
        apply(&other, 1, ByteAction::TryShared).unwrap();
        assert!(matches!(
            try_error(apply(&other, 0, ByteAction::TryShared).unwrap_err()),
            TryLockError::WouldBlock
        ));
        apply(owner, 0, ByteAction::Unlock).unwrap();
        apply(&other, 0, ByteAction::WaitShared).unwrap();
    }
}
