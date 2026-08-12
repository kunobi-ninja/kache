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

/// Is `pid` safe to hand to `kill(2)` as a single-process target?
///
/// `kill(2)` overloads the pid argument with broadcast modes, and a bogus or
/// sentinel PID that lands in one of them takes down far more than the
/// intended process:
///
///   - `pid == 0`: every process in the *caller's* process group
///   - `pid == -1`: every process the caller has permission to signal, i.e.
///     the user's entire login session
///   - `pid < -1`: the process group `-pid`
///   - `pid == 1`: init/launchd, and negating it yields the `-1` broadcast
///
/// Note that `u32::MAX` casts to `-1`, so a "definitely dead, made-up PID"
/// fixture is exactly the value that wipes the session. Reject anything that
/// is not an ordinary positive PID.
#[cfg(unix)]
fn is_single_process_pid(pid: u32) -> bool {
    pid > 1 && i32::try_from(pid).is_ok()
}

/// Refuse a PID that `kill(2)` would treat as a broadcast selector.
///
/// Production logs and carries on — a corrupt pidfile must not take the daemon
/// down. Tests panic instead: a fixture that reaches here is the exact bug this
/// module exists to prevent, and silently no-oping would let it pass green.
#[cfg(unix)]
fn refuse_unsafe_pid(pid: u32, what: &str) {
    tracing::error!(pid, action = what, "refusing to signal a non-process PID");

    #[cfg(test)]
    panic!(
        "test passed PID {pid} to {what}; kill(2) reads that as a broadcast \
         selector (0 = our process group, -1 = every process the user owns). \
         Use a real spawned child, not a sentinel PID."
    );
}

/// Test-only blast radius limit: a unit test may only signal processes it
/// actually spawned.
///
/// The pid guard above rejects the broadcast selectors, but it cannot tell a
/// legitimate target from someone else's PID — and a test that signals a PID
/// it does not own has no correct outcome, only luck. This walks the target's
/// parent chain and refuses anything the test process does not sit above, so
/// a bad fixture fails the test instead of reaching across the machine.
///
/// Compiled only under `cfg(test)`, so production keeps the bare `kill(2)`.
#[cfg(all(unix, test))]
fn assert_test_owns_process(pid: u32, what: &str) -> bool {
    let me = std::process::id();
    let mut cursor = pid;

    // Depth-limited: a cycle in the reported parent chain must not hang.
    for _ in 0..64 {
        if cursor == me {
            return true;
        }
        if cursor <= 1 {
            break;
        }
        let Ok(out) = std::process::Command::new("ps")
            .args(["-o", "ppid=", "-p", &cursor.to_string()])
            .output()
        else {
            break;
        };
        if !out.status.success() {
            break;
        }
        let Ok(parent) = String::from_utf8_lossy(&out.stdout).trim().parse::<u32>() else {
            break;
        };
        cursor = parent;
    }

    panic!(
        "test tried to {what} PID {pid}, which is not a descendant of the test \
         process ({me}). Tests may only signal processes they spawned; a PID \
         from a fixture, a config file, or a `pgrep` sweep belongs to the \
         developer's machine. (A child orphaned before this check — its \
         intermediate parent exited, so init reparented it — also lands here; \
         keep the process you intend to signal a live descendant.)"
    );
}

#[cfg(unix)]
pub fn is_process_alive(pid: u32) -> bool {
    // Probing with signal 0 is harmless per se, but `kill(-1, 0)` succeeds
    // whenever *any* signalable process exists, so a broadcast PID would
    // report "alive" and send callers straight into the terminate path.
    if !is_single_process_pid(pid) {
        return false;
    }
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
    {
        if !is_single_process_pid(pid) {
            refuse_unsafe_pid(pid, "terminate_process");
            return;
        }
        #[cfg(test)]
        assert_test_owns_process(pid, "SIGTERM");
        unsafe {
            libc::kill(pid as i32, libc::SIGTERM);
        }
    }
    #[cfg(windows)]
    {
        windows_terminate(pid);
    }
}

/// Forcefully kill a process. SIGKILL on Unix, TerminateProcess on Windows.
pub fn kill_process(pid: u32) {
    #[cfg(unix)]
    {
        if !is_single_process_pid(pid) {
            refuse_unsafe_pid(pid, "kill_process");
            return;
        }
        #[cfg(test)]
        assert_test_owns_process(pid, "SIGKILL");
        unsafe {
            libc::kill(pid as i32, libc::SIGKILL);
        }
    }
    #[cfg(windows)]
    {
        windows_terminate(pid);
    }
}

/// Forcefully kill a process and all its descendants (process group on Unix, process tree on Windows).
pub fn kill_process_group(pid: u32) {
    #[cfg(unix)]
    {
        // `-pid` is the group selector, so pid 1 would negate into the `-1`
        // "signal everything" broadcast and pid 0 into the caller's own group.
        if !is_single_process_pid(pid) {
            refuse_unsafe_pid(pid, "kill_process_group");
            return;
        }
        #[cfg(test)]
        assert_test_owns_process(pid, "SIGKILL the process group of");
        unsafe {
            libc::kill(-(pid as i32), libc::SIGKILL);
        }
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

/// Put a child in its own process group so a timeout kill reaches its whole
/// tree, WITHOUT detaching it from the console. A .cmd/.bat wrapper runs via
/// cmd.exe, which needs a console; DETACHED_PROCESS makes it exit 0 with no
/// output, so the probe must not use it.
pub fn configure_process_group(cmd: &mut std::process::Command) {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        // CREATE_NEW_PROCESS_GROUP = 0x00000200
        cmd.creation_flags(0x00000200);
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
    fn broadcast_pids_are_not_single_process_targets() {
        // The kill(2) broadcast selectors. `u32::MAX` is the dangerous one:
        // it casts to -1, which signals the user's entire session.
        for pid in [0, 1, u32::MAX] {
            assert!(
                !super::is_single_process_pid(pid),
                "pid {pid} must never be signalled as a single process"
            );
        }
        assert!(super::is_single_process_pid(std::process::id()));
    }

    #[cfg(unix)]
    #[test]
    fn broadcast_pids_are_never_reported_alive() {
        // `kill(-1, 0)` succeeds whenever anything is signalable, so an
        // unguarded liveness probe would call u32::MAX "alive" and send
        // callers into terminate_process with it.
        for pid in [0, 1, u32::MAX] {
            assert!(
                !super::is_process_alive(pid),
                "pid {pid} must not be reported alive"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_spawned_child_is_owned_and_terminable() {
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn sleep");
        let pid = child.id();

        assert!(super::assert_test_owns_process(pid, "probe"));

        // The ownership check must not get in the way of the legitimate case.
        super::terminate_process(pid);
        let status = child.wait().expect("reap child");
        assert!(!status.success(), "child should have been terminated");
    }

    #[cfg(unix)]
    #[test]
    fn kill_process_actually_kills_a_spawned_child() {
        // SIGKILL is the escalation the daemon recovery path relies on when a
        // polite SIGTERM does not land, so it needs its own coverage: a
        // terminate-only test leaves "kill does nothing" indistinguishable
        // from "kill works".
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn sleep");
        let pid = child.id();

        super::kill_process(pid);
        let status = child.wait().expect("reap child");

        assert!(!status.success(), "child should have been killed");
        assert!(!super::is_process_alive(pid), "child should be gone");
    }

    #[cfg(unix)]
    #[test]
    #[should_panic(expected = "not a descendant of the test process")]
    fn signalling_a_process_the_test_does_not_own_panics() {
        // The test runner that spawned us: a live PID, definitely not ours.
        // `assert_test_owns_process` only inspects the parent chain — it
        // never signals — so this stays harmless even if the check regresses.
        let parent = unsafe { libc::getppid() } as u32;
        super::assert_test_owns_process(parent, "SIGTERM");
    }

    #[cfg(unix)]
    #[test]
    #[should_panic(expected = "broadcast")]
    fn a_broadcast_pid_fails_the_test_that_supplied_it() {
        // `refuse_unsafe_pid` contains no kill call at all, so exercising the
        // rejection path here is safe even if every other guard regresses.
        super::refuse_unsafe_pid(u32::MAX, "terminate_process");
    }

    // Deliberately NOT tested: calling `terminate_process(u32::MAX)` and
    // asserting a bystander process survived. That test passes by doing
    // nothing and fails by killing the developer's entire login session —
    // and this repo runs cargo-mutants, which would build exactly the
    // compromised guard that makes it fire (see kunobi-ninja/kache history
    // for the session-wipe this module now prevents). The guard is a plain
    // early return over `is_single_process_pid`, so pinning the predicate
    // above pins the behaviour without arming a live round.

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
