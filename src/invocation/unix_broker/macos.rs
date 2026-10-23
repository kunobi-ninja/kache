//! A task-name port pins the compiler's kernel identity across PID reuse.
use std::{io, sync::OnceLock};

type Token = [u32; 8];
type Signal = unsafe extern "C" fn(*mut Token, libc::c_int) -> libc::c_int;

unsafe extern "C" {
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
}

impl Identity {
    #[cfg(test)]
    pub(super) fn stale_for_test(pid: u32) -> Self {
        Self { pid, task: 0 }
    }

    pub(super) fn capture(pid: u32) -> io::Result<Self> {
        signal_api().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "audit-token process signalling is unavailable",
            )
        })?;
        let mut task = 0;
        let status = unsafe { task_name_for_pid(libc::mach_task_self(), pid as i32, &mut task) };
        if status != 0 {
            return Err(io::Error::other(format!(
                "cannot pin compiler task: {status}"
            )));
        }
        let identity = Self { pid, task };
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
        for _ in 0..3 {
            let Ok(mut token) = self.token() else {
                return;
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

impl Drop for Identity {
    fn drop(&mut self) {
        if self.task != 0 {
            unsafe {
                mach_port_deallocate(libc::mach_task_self(), self.task);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::CommandExt;

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
