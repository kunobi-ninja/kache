//! Cross-platform helpers for process management and signal handling.
//!
//! The daemon needs to:
//!   - probe whether a recorded PID is still alive (recovery from crashes)
//!   - politely ask another process to exit, then force-kill if it didn't
//!   - wait for an OS-level shutdown signal to flush state and exit cleanly
//!
//! On Unix these all map to libc primitives (kill(2), signal(2)).
//! On Windows they map to OpenProcess/TerminateProcess and the Windows
//! console-control events surfaced by tokio::signal::windows.

#[cfg(unix)]
pub fn is_process_alive(pid: u32) -> bool {
    // kill(pid, 0) returns 0 if the process exists; EPERM also means it
    // exists but is owned by another user.
    let rc = unsafe { libc::kill(pid as i32, 0) };
    (rc == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM))
        && !is_process_zombie(pid)
}

#[cfg(unix)]
pub fn is_process_zombie(pid: u32) -> bool {
    let pid = pid.to_string();
    let output = std::process::Command::new("ps")
        .args(["-o", "stat=", "-p", pid.as_str()])
        .output();

    match output {
        Ok(output) if output.status.success() => {
            process_stat_indicates_zombie(&String::from_utf8_lossy(&output.stdout))
        }
        _ => false,
    }
}

#[cfg(unix)]
fn process_stat_indicates_zombie(stat: &str) -> bool {
    stat.trim_start().starts_with('Z')
}

#[cfg(windows)]
pub fn is_process_alive(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::{CloseHandle, STILL_ACTIVE};
    use windows_sys::Win32::System::Threading::{
        GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if handle.is_null() {
        return false;
    }
    let mut code: u32 = 0;
    let ok = unsafe { GetExitCodeProcess(handle, &mut code) };
    unsafe { CloseHandle(handle) };
    ok != 0 && code as i32 == STILL_ACTIVE
}

/// Politely request a process to exit. On Unix this sends SIGTERM; on
/// Windows there is no graceful kill-by-PID path, so this forcefully
/// terminates the process (same as `kill_process`). Callers that need
/// graceful shutdown should prefer the daemon's own RPC `Shutdown` request.
pub fn terminate_process(pid: u32) {
    #[cfg(unix)]
    unsafe {
        libc::kill(pid as i32, libc::SIGTERM);
    }
    #[cfg(windows)]
    {
        windows_terminate(pid);
    }
}

/// Forcefully kill a process. SIGKILL on Unix, TerminateProcess on Windows.
pub fn kill_process(pid: u32) {
    #[cfg(unix)]
    unsafe {
        libc::kill(pid as i32, libc::SIGKILL);
    }
    #[cfg(windows)]
    {
        windows_terminate(pid);
    }
}

/// Forcefully kill a process and all its descendants (process group on Unix, process tree on Windows).
pub fn kill_process_group(pid: u32) {
    #[cfg(unix)]
    unsafe {
        libc::kill(-(pid as i32), libc::SIGKILL);
    }
    #[cfg(windows)]
    {
        let _ = std::process::Command::new("taskkill")
            .args(["/F", "/T", "/PID", &pid.to_string()])
            .output();
    }
}

#[cfg(windows)]
fn windows_terminate(pid: u32) {
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{OpenProcess, PROCESS_TERMINATE, TerminateProcess};

    let handle = unsafe { OpenProcess(PROCESS_TERMINATE, 0, pid) };
    if handle.is_null() {
        return;
    }
    unsafe {
        TerminateProcess(handle, 1);
        CloseHandle(handle);
    }
}

/// Current effective UID. Returns `libc::getuid()` on Unix. On Windows
/// there is no equivalent — UIDs are part of macOS launchctl target
/// strings (`gui/{uid}/...`) and that whole code path is macOS-only, so a
/// stub returning 0 keeps the rest of `service.rs` compilable.
#[cfg(unix)]
pub fn current_uid() -> u32 {
    unsafe { libc::getuid() }
}

#[cfg(not(unix))]
pub fn current_uid() -> u32 {
    0
}

/// Resolve when the OS asks the daemon to stop. SIGTERM/SIGINT on Unix,
/// Ctrl+C / console-close on Windows.
pub async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler");
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
        }
    }
    #[cfg(windows)]
    {
        use tokio::signal::windows::{ctrl_break, ctrl_c, ctrl_close, ctrl_shutdown};
        let mut cc = ctrl_c().expect("ctrl_c handler");
        let mut cb = ctrl_break().expect("ctrl_break handler");
        let mut cl = ctrl_close().expect("ctrl_close handler");
        let mut cs = ctrl_shutdown().expect("ctrl_shutdown handler");
        tokio::select! {
            _ = cc.recv() => {}
            _ = cb.recv() => {}
            _ = cl.recv() => {}
            _ = cs.recv() => {}
        }
    }
}

/// Configure a process to be fully detached (in its own process group on Unix,
/// or with detached creation flags on Windows) so that pressing Ctrl-C on the parent
/// does not terminate the child.
pub fn configure_detached_process(cmd: &mut std::process::Command) {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        // CREATE_NEW_PROCESS_GROUP = 0x00000200
        // DETACHED_PROCESS = 0x00000008
        cmd.creation_flags(0x00000200 | 0x00000008);
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = cmd;
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    #[test]
    fn process_stat_zombie_detection_uses_leading_state() {
        assert!(super::process_stat_indicates_zombie("Z"));
        assert!(super::process_stat_indicates_zombie("Z+"));
        assert!(super::process_stat_indicates_zombie("  ZN"));
        assert!(!super::process_stat_indicates_zombie("S"));
        assert!(!super::process_stat_indicates_zombie("Ss"));
        assert!(!super::process_stat_indicates_zombie("R+"));
    }

    #[test]
    fn current_process_is_alive() {
        // The test process itself is, by definition, running.
        assert!(super::is_process_alive(std::process::id()));
    }

    #[cfg(unix)]
    #[test]
    fn current_process_is_not_a_zombie() {
        // A live, running process is in state R/S, never Z.
        assert!(!super::is_process_zombie(std::process::id()));
    }

    #[cfg(unix)]
    #[test]
    fn reaped_child_is_not_alive() {
        // Spawn a child, confirm it's alive, then kill + reap it. After the
        // PID is reaped `kill(pid, 0)` returns ESRCH, so `is_process_alive`
        // must report false.
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn sleep");
        let pid = child.id();
        assert!(super::is_process_alive(pid), "child should be alive");

        child.kill().expect("kill child");
        child.wait().expect("reap child");

        assert!(
            !super::is_process_alive(pid),
            "reaped child should no longer be alive"
        );
    }

    #[cfg(unix)]
    #[test]
    fn current_uid_matches_libc_getuid() {
        let expected = unsafe { libc::getuid() };
        assert_eq!(super::current_uid(), expected);
    }

    #[cfg(not(unix))]
    #[test]
    fn current_uid_is_stub_zero_off_unix() {
        assert_eq!(super::current_uid(), 0);
    }
}
