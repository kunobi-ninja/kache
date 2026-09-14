//! Best-effort preflight for a local sccache server. Unknown policy means retry on failure.
// The Linux mutation lane cannot execute Darwin process APIs. The policy
// decisions live in sandbox.rs and run under Linux mutations; macOS unit and
// subprocess tests exercise this native adapter against an actual sandbox.
use std::ffi::{CStr, CString};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use super::sandbox::{self, Probe};

pub(super) fn denial(name: &str, outputs: &[&Path]) -> Option<String> {
    let policy = Policy::load()?;
    sandbox::check(
        name,
        outputs,
        std::env::var_os("SCCACHE_SERVER_PORT").as_deref(),
        std::env::var_os("SCCACHE_SERVER_UDS").is_some(),
        &policy,
    )
}

impl Probe for Policy {
    fn server_pid(&self, port: u16) -> Option<i32> {
        server_pid(port)
    }
    fn process_path(&self, pid: i32) -> Option<PathBuf> {
        process_path(pid)
    }
    fn sandboxed(&self, pid: i32) -> bool {
        self.sandboxed(pid)
    }
    fn denies_write(&self, pid: i32, path: &Path) -> bool {
        self.denies_write(pid, path)
    }
}

fn server_pid(port: u16) -> Option<i32> {
    // Bound the optional probe, including output volume. No shell, credentials,
    // or compiler arguments are passed to lsof. A timeout leaves recovery to run().
    let output = tempfile::tempfile().ok()?;
    let mut command = Command::new("/usr/sbin/lsof");
    command.args([
        "-nP",
        "-a",
        "-i",
        &format!("TCP@127.0.0.1:{port}"),
        "-sTCP:LISTEN",
        "-Fp",
    ]);
    let mut child = command
        .stdin(Stdio::null())
        .stdout(output.try_clone().ok()?)
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let deadline = Instant::now() + Duration::from_millis(200);
    let success = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status.success(),
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(5)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    };
    if !success {
        return None;
    }
    use std::io::{Read, Seek};
    let mut output = output;
    output.rewind().ok()?;
    let mut text = String::new();
    output.take(4096).read_to_string(&mut text).ok()?;
    sandbox::parse_pid(&text)
}

fn process_path(pid: i32) -> Option<PathBuf> {
    let mut buffer = [0u8; libc::PROC_PIDPATHINFO_MAXSIZE as usize];
    let length =
        unsafe { libc::proc_pidpath(pid, buffer.as_mut_ptr().cast(), buffer.len() as u32) };
    if length <= 0 {
        return None;
    }
    use std::os::unix::ffi::OsStrExt;
    let path = CStr::from_bytes_until_nul(&buffer).ok()?;
    Some(PathBuf::from(std::ffi::OsStr::from_bytes(path.to_bytes())))
}

// This is private macOS SPI. Resolve it at runtime so an unavailable probe
// never prevents compilation or startup of Kache itself.
type SandboxCheck =
    unsafe extern "C" fn(libc::pid_t, *const libc::c_char, libc::c_int, ...) -> libc::c_int;
struct Policy {
    library: *mut libc::c_void,
    check: SandboxCheck,
    no_report: i32,
}
impl Policy {
    fn load() -> Option<Self> {
        unsafe {
            let library = libc::dlopen(c"/usr/lib/libsandbox.dylib".as_ptr(), libc::RTLD_LAZY);
            if library.is_null() {
                return None;
            }
            let check = libc::dlsym(library, c"sandbox_check".as_ptr());
            let no_report = libc::dlsym(library, c"SANDBOX_CHECK_NO_REPORT".as_ptr());
            if check.is_null() || no_report.is_null() {
                libc::dlclose(library);
                return None;
            }
            Some(Self {
                library,
                check: std::mem::transmute::<*mut libc::c_void, SandboxCheck>(check),
                no_report: *no_report.cast::<i32>(),
            })
        }
    }
    fn sandboxed(&self, pid: i32) -> bool {
        unsafe { (self.check)(pid, std::ptr::null(), 0) == 1 }
    }
    fn denies_write(&self, pid: i32, path: &Path) -> bool {
        use std::os::unix::ffi::OsStrExt;
        let Ok(path) = CString::new(path.as_os_str().as_bytes()) else {
            return false;
        };
        // SANDBOX_FILTER_PATH = 1. Check exact return value: -1 is unknown.
        [c"file-write-create", c"file-write-data"]
            .iter()
            .any(|operation| unsafe {
                (self.check)(pid, operation.as_ptr(), 1 | self.no_report, path.as_ptr()) == 1
            })
    }
}
impl Drop for Policy {
    fn drop(&mut self) {
        unsafe {
            libc::dlclose(self.library);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_distinguishes_denied_and_allowed_output() {
        let dir = tempfile::tempdir().unwrap();
        let denied = dir.path().join("denied");
        let allowed = dir.path().join("allowed");
        std::fs::create_dir(&denied).unwrap();
        std::fs::create_dir(&allowed).unwrap();
        let denied = denied.canonicalize().unwrap();
        let profile = format!(
            "(version 1)(allow default)(deny file-write* (subpath \"{}\"))",
            denied.display()
        );
        let mut child = Command::new("/usr/bin/sandbox-exec")
            .args(["-p", &profile, "/bin/sleep", "10"])
            .spawn()
            .unwrap();
        let policy = Policy::load().unwrap();
        // spawn returns before sandbox-exec applies the profile.
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if policy.sandboxed(child.id() as i32)
                && process_path(child.id() as i32)
                    .is_some_and(|path| path.file_name() == Some(std::ffi::OsStr::new("sleep")))
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        let pid = child.id() as i32;
        let denied_result = policy.denies_write(pid, &denied);
        let allowed_result = policy.denies_write(pid, &allowed.canonicalize().unwrap());
        let sandboxed = policy.sandboxed(pid);
        let identity = process_path(pid);
        let _ = child.kill();
        child.wait().unwrap();
        assert!(sandboxed);
        assert!(denied_result);
        assert!(!allowed_result);
        assert_eq!(identity.unwrap().file_name().unwrap(), "sleep");
    }
}
