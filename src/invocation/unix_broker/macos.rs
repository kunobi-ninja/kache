//! A task-name port pins the compiler's kernel identity across PID reuse.
use std::{io, sync::OnceLock};

type Token = [u32; 8];
type Signal = unsafe extern "C" fn(*mut Token, libc::c_int) -> libc::c_int;

unsafe extern "C" {
    // mach/mach_init.h defines mach_task_self() as this process-local port.
    #[link_name = "mach_task_self_"]
    static TASK_SELF: u32;
    fn task_name_for_pid(task: u32, pid: i32, name: *mut u32) -> i32;
    fn mach_port_deallocate(task: u32, name: u32) -> i32;
}

fn signal_api() -> Option<Signal> {
    static API: OnceLock<Option<Signal>> = OnceLock::new();
    *API.get_or_init(|| unsafe {
        let library = libc::dlopen(c"/usr/lib/libproc.dylib".as_ptr(), libc::RTLD_LAZY);
        if library.is_null() {
            return None;
        }
        let symbol = libc::dlsym(library, c"proc_signal_with_audittoken".as_ptr());
        if symbol.is_null() {
            libc::dlclose(library);
            return None;
        }
        // Retain this library reference for the cached function's lifetime.
        Some(std::mem::transmute::<*mut libc::c_void, Signal>(symbol))
    })
}

pub(super) struct Identity {
    pid: u32,
    task: u32,
    unique: u64,
}

impl Identity {
    #[cfg(test)]
    pub(super) fn stale_for_test(pid: u32) -> Self {
        Self {
            pid,
            task: 0,
            unique: 0,
        }
    }

    pub(super) fn capture(pid: u32) -> io::Result<Self> {
        signal_api().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "audit-token process signalling is unavailable",
            )
        })?;
        let unique = unique_id(pid)?;
        let mut task = 0;
        let status = unsafe { task_name_for_pid(TASK_SELF, pid as i32, &mut task) };
        if status != 0 {
            return Err(io::Error::other(format!(
                "cannot pin compiler task: {status}"
            )));
        }
        let identity = Self { pid, task, unique };
        if unique_id(pid)? != unique {
            return Err(io::Error::other("compiler process identity changed"));
        }
        identity.token()?;
        Ok(identity)
    }

    fn token(&self) -> io::Result<Token> {
        let mut token = [0; 8];
        let mut count = 8;
        // TASK_AUDIT_TOKEN and its natural_t count come from mach/task_info.h.
        let status =
            unsafe { libc::task_info(self.task, 15, token.as_mut_ptr().cast(), &mut count) };
        if status != 0 || count != 8 {
            return Err(io::Error::other("compiler task exited"));
        }
        Ok(token)
    }

    pub(super) fn kill_if_unchanged(&self) {
        let Some(signal) = signal_api() else {
            return;
        };
        // XNU validates the complete audit token while holding the process
        // reference. Refresh after exec, rather than checking a PID then killing it.
        // https://github.com/apple-oss-distributions/xnu/blob/main/bsd/kern/proc_info.c
        for attempt in 0..3 {
            let refreshed;
            let identity = if attempt == 0 {
                self
            } else {
                let Ok(current) = Self::capture(self.pid) else {
                    return;
                };
                if current.unique != self.unique {
                    return;
                }
                refreshed = current;
                &refreshed
            };
            let Ok(mut token) = identity.token() else {
                continue;
            };
            if unsafe { signal(&mut token, libc::SIGSTOP) } != 0 {
                continue;
            }
            // The stopped leader keeps its process group alive during cleanup.
            unsafe {
                libc::kill(-(self.pid as libc::pid_t), libc::SIGKILL);
            }
            unsafe {
                signal(&mut token, libc::SIGKILL);
            }
            return;
        }
    }
}

// proc_info_private.h defines this 56-byte API layout and flavor 17.
// https://github.com/apple-oss-distributions/xnu/blob/main/bsd/sys/proc_info_private.h
#[repr(C)]
struct UniqueInfo {
    uuid: [u8; 16],
    unique: u64,
    parent: u64,
    version: i32,
    original_parent_version: i32,
    reserved: [u64; 2],
}

fn unique_id(pid: u32) -> io::Result<u64> {
    let mut info = std::mem::MaybeUninit::<UniqueInfo>::uninit();
    let size = std::mem::size_of::<UniqueInfo>() as i32;
    let read = unsafe { libc::proc_pidinfo(pid as i32, 17, 0, info.as_mut_ptr().cast(), size) };
    if read != size {
        return Err(io::Error::other("cannot read compiler process identity"));
    }
    Ok(unsafe { info.assume_init() }.unique)
}

impl Drop for Identity {
    fn drop(&mut self) {
        if self.task != 0 {
            unsafe {
                mach_port_deallocate(TASK_SELF, self.task);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::CommandExt;

    #[test]
    fn cancellation_survives_an_exec_after_identity_capture() {
        use std::{
            io::Write,
            process::Stdio,
            time::{Duration, Instant},
        };
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", "read line; exec /bin/sleep 30"])
            .stdin(Stdio::piped())
            .process_group(0)
            .spawn()
            .unwrap();
        let identity = Identity::capture(child.id()).unwrap();
        child.stdin.take().unwrap().write_all(b"go\n").unwrap();
        // Wait for the original task port to disappear at exec.
        let deadline = Instant::now() + Duration::from_secs(2);
        while identity.token().is_ok() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        identity.kill_if_unchanged();
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(!status.success());
                break;
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("exec lost cancellation");
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn disconnected_compiler_cleanup_uses_a_pinned_task_identity() {
        let mut child = std::process::Command::new("/bin/sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .unwrap();
        let identity = Identity::capture(child.id()).unwrap();
        Identity::stale_for_test(child.id()).kill_if_unchanged();
        assert!(child.try_wait().unwrap().is_none());
        identity.kill_if_unchanged();
        assert!(!child.wait().unwrap().success());
        identity.kill_if_unchanged();
    }
}
