//! How long a permit waiter sleeps between scans.
//!
//! Admission is still the flock on the slot file: the kernel drops it when
//! the holder exits, and the permit scan is what takes the slots. This
//! module only replaces the 10 ms poll while the slots the waiter needs
//! are already held.
//!
//! macOS sleeps in `kevent` until `NOTE_FUNLOCK` (a `flock` or the `close`
//! of a dying holder) or the slice elapses. Linux publishes a counter when
//! the admitting process drops its permit and waits on that; a killed
//! holder never publishes, so the slice is the backstop. Everywhere else
//! the slice itself is the sleep. The slice stays short of a second so a
//! cancelled wait rechecks [`crate::invocation::process::check`] on the
//! next pass.

use std::path::Path;
use std::time::Duration;

/// Longest a saturated waiter sleeps before scanning again.
pub(super) const SATURATED_SLICE: Duration = Duration::from_millis(200);

/// A macOS watch on the busy slot files. Empty on other systems: nothing
/// there can report an unlock except the Linux counter.
#[cfg(target_os = "macos")]
pub(super) struct Armed {
    watch: MacWatch,
}

/// Named so the wait loop has one type on every OS. Never constructed
/// off macOS; the wait there does not hold a kqueue.
#[cfg(not(target_os = "macos"))]
#[allow(dead_code)]
pub(super) struct Armed;

#[cfg(target_os = "macos")]
#[mutants::skip]
pub(super) fn arm(root: &Path, busy: &[u32]) -> Option<Armed> {
    MacWatch::open(root, busy).map(|watch| Armed { watch })
}

#[cfg(not(target_os = "macos"))]
#[mutants::skip]
pub(super) fn arm(root: &Path, busy: &[u32]) -> Option<Armed> {
    let _ = (root, busy);
    None
}

/// Generation the Linux waiter must see change. Zero where there is no counter.
#[cfg(target_os = "linux")]
pub(super) fn snapshot(root: &Path) -> u32 {
    linux::load(root).unwrap_or(0)
}

#[cfg(not(target_os = "linux"))]
#[mutants::skip]
pub(super) fn snapshot(root: &Path) -> u32 {
    let _ = root;
    0
}

/// Sleep until a watched slot is unlocked, `seen` changes, or `limit` elapses.
#[cfg(target_os = "macos")]
#[mutants::skip]
pub(super) fn park(root: &Path, armed: Option<Armed>, seen: u32, limit: Duration) {
    if let Some(armed) = armed {
        armed.watch.stay(limit);
        return;
    }
    let _ = (root, seen);
    std::thread::sleep(limit);
}

#[cfg(target_os = "linux")]
pub(super) fn park(root: &Path, armed: Option<Armed>, seen: u32, limit: Duration) {
    let _ = armed;
    linux::wait(root, seen, limit);
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
#[mutants::skip]
pub(super) fn park(root: &Path, armed: Option<Armed>, seen: u32, limit: Duration) {
    let _ = (root, armed, seen);
    std::thread::sleep(limit);
}

/// Tell Linux waiters that a permit was dropped. Other systems hear the
/// unlock from the kernel, or notice it on the next slice.
#[cfg(target_os = "linux")]
pub(super) fn notify(root: &Path) {
    linux::bump(root);
}

#[cfg(not(target_os = "linux"))]
#[mutants::skip]
pub(super) fn notify(root: &Path) {
    let _ = root;
}

#[cfg(target_os = "macos")]
use std::os::unix::io::AsRawFd;

#[cfg(target_os = "macos")]
struct MacWatch {
    kq: libc::c_int,
    files: Vec<std::fs::File>,
}

#[cfg(target_os = "macos")]
impl MacWatch {
    #[mutants::skip]
    fn open(root: &Path, busy: &[u32]) -> Option<Self> {
        // SAFETY: `kqueue` returns a new queue or -1. Nothing else owns it
        // until `Drop` closes it.
        let kq = unsafe { libc::kqueue() };
        if kq < 0 {
            return None;
        }
        let mut files = Vec::with_capacity(busy.len());
        let dir = root.join("permits");
        for index in busy {
            if let Ok(file) = std::fs::File::open(dir.join(index.to_string())) {
                files.push(file);
            }
        }
        if files.is_empty() {
            // SAFETY: `kq` was just returned by `kqueue` and is not shared.
            unsafe { libc::close(kq) };
            return None;
        }
        Some(Self { kq, files })
    }

    #[mutants::skip]
    fn stay(&self, limit: Duration) {
        let changes: Vec<libc::kevent> = self
            .files
            .iter()
            .map(|file| libc::kevent {
                ident: file.as_raw_fd() as libc::uintptr_t,
                filter: libc::EVFILT_VNODE,
                flags: libc::EV_ADD | libc::EV_CLEAR,
                fflags: libc::NOTE_FUNLOCK,
                data: 0,
                udata: std::ptr::null_mut(),
            })
            .collect();
        let deadline = std::time::Instant::now() + limit;
        let mut event = libc::kevent {
            ident: 0,
            filter: 0,
            flags: 0,
            fflags: 0,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        while std::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return;
            }
            let timeout = timespec(remaining);
            // SAFETY: `kq` is a live kqueue, `changes` is the changelist for
            // this call only, `event` is a one-slot output, and `timeout`
            // outlives the call. The file descriptors stay open on `self`.
            // `EV_ADD` is sent again after `EINTR`; adding the same watch twice
            // is the same watch.
            let ready = unsafe {
                libc::kevent(
                    self.kq,
                    changes.as_ptr(),
                    changes.len() as libc::c_int,
                    &mut event,
                    1,
                    &timeout,
                )
            };
            if ready >= 0
                || std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted
            {
                return;
            }
        }
    }
}

#[cfg(target_os = "macos")]
impl Drop for MacWatch {
    #[mutants::skip]
    fn drop(&mut self) {
        // SAFETY: `kq` was returned by `kqueue` and is not closed elsewhere.
        // Close the queue before the watch files so this knote is gone
        // before those descriptors are.
        unsafe { libc::close(self.kq) };
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::timespec;
    use std::fs::OpenOptions;
    use std::os::unix::io::AsRawFd;
    use std::path::Path;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    // Linux UAPI. libc does not export these on every target.
    const FUTEX_WAIT: libc::c_long = 0;
    const FUTEX_WAKE: libc::c_long = 1;

    /// Grow a short wake file to four bytes.
    ///
    /// `set_len(4)` on a file that is already four bytes does not change it,
    /// so `len <= 4` is the same check as `len < 4`.
    #[mutants::skip]
    fn extend_to_word(file: &std::fs::File) -> bool {
        let Ok(meta) = file.metadata() else {
            return false;
        };
        if meta.len() < 4 {
            file.set_len(4).is_ok()
        } else {
            true
        }
    }

    /// Protection for the shared word.
    ///
    /// `PROT_READ` and `PROT_WRITE` share no bits, so `^` is the same mask.
    /// That edit does not fail a test; the suite runs out the mutation cap.
    #[mutants::skip]
    fn read_write_prot() -> libc::c_int {
        libc::PROT_READ | libc::PROT_WRITE
    }

    struct WakeWord {
        ptr: *mut libc::c_void,
        len: usize,
    }

    impl WakeWord {
        fn open(root: &Path) -> Option<Self> {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                // The counter has to survive the next open. Truncating it
                // would drop a wake that landed between two waits.
                .truncate(false)
                .open(root.join("wake"))
                .ok()?;
            if !extend_to_word(&file) {
                return None;
            }
            let len = 4usize;
            // SAFETY: the file is at least four bytes, the mapping is
            // `MAP_SHARED` at offset 0, and `Drop` unmaps the same length.
            // `set_len` zero-fills a new file, so the word is initialized.
            let ptr = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    len,
                    read_write_prot(),
                    libc::MAP_SHARED,
                    file.as_raw_fd(),
                    0,
                )
            };
            if ptr == libc::MAP_FAILED {
                return None;
            }
            Some(Self { ptr, len })
        }

        fn addr(&self) -> *mut u32 {
            self.ptr.cast()
        }

        fn atomic(&self) -> &AtomicU32 {
            // SAFETY: `ptr` is page-aligned, non-null, and covers four
            // initialized bytes for as long as `self` is alive.
            unsafe { &*self.addr().cast() }
        }
    }

    impl Drop for WakeWord {
        fn drop(&mut self) {
            // SAFETY: `ptr` and `len` are the pair passed to `mmap`.
            unsafe { libc::munmap(self.ptr, self.len) };
        }
    }

    pub(super) fn load(root: &Path) -> Option<u32> {
        WakeWord::open(root).map(|word| word.atomic().load(Ordering::Relaxed))
    }

    pub(super) fn bump(root: &Path) {
        let Some(word) = WakeWord::open(root) else {
            return;
        };
        // Any non-zero step wakes a waiter that sampled the old word,
        // including one that has not reached the futex yet. The syscall
        // below is what unblocks a waiter already inside it.
        word.atomic().fetch_add(1, Ordering::Relaxed);
        futex(
            word.addr(),
            FUTEX_WAKE,
            libc::c_int::MAX as u32,
            std::ptr::null(),
        );
    }

    pub(super) fn wait(root: &Path, seen: u32, limit: Duration) {
        let Some(word) = WakeWord::open(root) else {
            std::thread::sleep(limit);
            return;
        };
        let timeout = timespec(limit);
        // One wait. A bump that landed after `seen` was sampled makes the
        // kernel return at once. A signal does too; the caller scans and,
        // if the slot is still held, parks again.
        futex(word.addr(), FUTEX_WAIT, seen, &timeout);
    }

    /// `FUTEX_WAIT` / `FUTEX_WAKE` on the shared word. No `FUTEX_PRIVATE`:
    /// the key is the file, so another process's wake reaches this mapping.
    fn futex(addr: *mut u32, op: libc::c_long, val: u32, timeout: *const libc::timespec) {
        // SAFETY: `addr` points at the mapped word for as long as `WakeWord`
        // is alive. `timeout` is null or a relative timeout that outlives
        // the call.
        unsafe {
            libc::syscall(
                libc::SYS_futex,
                addr,
                op,
                val,
                timeout,
                std::ptr::null::<u32>(),
                0u32,
            );
        }
    }
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn timespec(duration: Duration) -> libc::timespec {
    // Padding on some 32-bit targets is private, so this cannot be a literal.
    // SAFETY: `timespec` is a C struct of integers. Zero is a valid value,
    // and both fields are overwritten below.
    let mut out: libc::timespec = unsafe { std::mem::zeroed() };
    out.tv_sec = duration.as_secs() as _;
    out.tv_nsec = duration.subsec_nanos() as _;
    out
}
