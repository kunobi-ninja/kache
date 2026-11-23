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
            Self::Byte(offset) => platform::lock(file, offset, false, false).map_err(try_error),
        }
    }

    pub(super) fn shared(self, file: &File) -> io::Result<()> {
        match self {
            Self::Whole => file.lock_shared(),
            #[cfg(any(target_os = "macos", windows))]
            Self::Byte(offset) => platform::lock(file, offset, false, true),
        }
    }

    pub(super) fn try_exclusive(self, file: &File) -> Result<(), TryLockError> {
        match self {
            Self::Whole => file.try_lock(),
            #[cfg(any(target_os = "macos", windows))]
            Self::Byte(offset) => platform::lock(file, offset, true, false).map_err(try_error),
        }
    }

    fn unlock(self, file: &File) -> io::Result<()> {
        match self {
            Self::Whole => file.unlock(),
            #[cfg(any(target_os = "macos", windows))]
            Self::Byte(offset) => platform::unlock(file, offset),
        }
    }
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

#[cfg(any(target_os = "macos", windows))]
fn try_error(error: io::Error) -> TryLockError {
    #[cfg(target_os = "macos")]
    let busy = matches!(error.raw_os_error(), Some(libc::EAGAIN | libc::EACCES));
    #[cfg(windows)]
    let busy =
        error.raw_os_error() == Some(windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION as i32);
    if busy {
        TryLockError::WouldBlock
    } else {
        TryLockError::Error(error)
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use super::*;
    use std::os::fd::AsRawFd;

    fn set(file: &File, offset: u32, kind: libc::c_short, wait: bool) -> io::Result<()> {
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
        loop {
            // SAFETY: the descriptor is open and range is a valid flock for this call.
            if unsafe { libc::fcntl(file.as_raw_fd(), command, &range) } != -1 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }

    pub(super) fn lock(file: &File, offset: u32, exclusive: bool, wait: bool) -> io::Result<()> {
        set(
            file,
            offset,
            if exclusive {
                libc::F_WRLCK
            } else {
                libc::F_RDLCK
            },
            wait,
        )
    }

    pub(super) fn unlock(file: &File, offset: u32) -> io::Result<()> {
        set(file, offset, libc::F_UNLCK, false)
    }
}

#[cfg(windows)]
mod platform {
    use super::*;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY, LockFileEx, UnlockFileEx,
    };
    use windows_sys::Win32::System::IO::{OVERLAPPED, OVERLAPPED_0, OVERLAPPED_0_0};

    fn position(offset: u32) -> OVERLAPPED {
        OVERLAPPED {
            Anonymous: OVERLAPPED_0 {
                Anonymous: OVERLAPPED_0_0 {
                    Offset: offset,
                    OffsetHigh: 0,
                },
            },
            ..OVERLAPPED::default()
        }
    }

    pub(super) fn lock(file: &File, offset: u32, exclusive: bool, wait: bool) -> io::Result<()> {
        let mut flags = if exclusive {
            LOCKFILE_EXCLUSIVE_LOCK
        } else {
            0
        };
        if !wait {
            flags |= LOCKFILE_FAIL_IMMEDIATELY;
        }
        // SAFETY: open_lock opens a synchronous handle. OVERLAPPED supplies
        // the offset and remains valid until the call returns.
        let ok = unsafe { LockFileEx(file.as_raw_handle(), flags, 0, 1, 0, &mut position(offset)) };
        if ok == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub(super) fn unlock(file: &File, offset: u32) -> io::Result<()> {
        // SAFETY: the handle is open and the range is the one this guard locked.
        let ok = unsafe { UnlockFileEx(file.as_raw_handle(), 0, 1, 0, &mut position(offset)) };
        if ok == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}
