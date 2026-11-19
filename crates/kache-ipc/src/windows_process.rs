//! Assign compiler children to a job before their first instruction executes.

use std::io;
use std::mem::{size_of, zeroed};
use std::os::windows::io::{AsRawHandle, FromRawHandle, IntoRawHandle, OwnedHandle};
use std::os::windows::process::CommandExt;
use std::process::{Child, Command};
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
    QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
};
use windows_sys::Win32::System::Threading::{CREATE_NEW_PROCESS_GROUP, CREATE_SUSPENDED};

pub struct Job(OwnedHandle);

/// Bind one compiler frontend's native descendants, including discovery tools,
/// to its OS process lifetime. The kernel closes this non-inheritable handle
/// at process exit; closing it earlier would terminate the frontend itself.
pub fn protect_current_process() -> io::Result<()> {
    // Prepare only after an existing daemon has accepted this connection's
    // setup. A cold-start daemon must not inherit the frontend's lifetime job.
    // Repeated setup attempts reuse the same process-owned handle.
    static PROTECTION: std::sync::OnceLock<io::Result<()>> = std::sync::OnceLock::new();
    PROTECTION
        .get_or_init(protect_process)
        .as_ref()
        .copied()
        .map_err(|error| io::Error::new(error.kind(), error.to_string()))
}

fn protect_process() -> io::Result<()> {
    let job = Job::new()?;
    if unsafe {
        AssignProcessToJobObject(
            job.0.as_raw_handle(),
            windows_sys::Win32::System::Threading::GetCurrentProcess(),
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    let _process_owned_handle = job.0.into_raw_handle();
    Ok(())
}

impl Job {
    pub fn from_owned(handle: OwnedHandle) -> io::Result<Self> {
        let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { zeroed() };
        if unsafe {
            QueryInformationJobObject(
                handle.as_raw_handle(),
                JobObjectExtendedLimitInformation,
                (&mut limits as *mut JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
                std::ptr::null_mut(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        if limits.BasicLimitInformation.LimitFlags & JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "compiler job has no lifetime policy",
            ));
        }
        Ok(Self(handle))
    }
    pub fn new() -> io::Result<Self> {
        let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        let job = Self(unsafe { OwnedHandle::from_raw_handle(handle) });
        let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { zeroed() };
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        if unsafe {
            SetInformationJobObject(
                job.0.as_raw_handle(),
                JobObjectExtendedLimitInformation,
                (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(job)
    }

    pub fn kill(&self) -> io::Result<()> {
        if unsafe { TerminateJobObject(self.0.as_raw_handle(), 1) } == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

pub fn spawn(command: &mut Command) -> io::Result<(Child, Job)> {
    let job = Job::new()?;
    let child = spawn_in_jobs(command, &[&job])?;
    Ok((child, job))
}

/// The client lifetime job is never exported. Closing the client kills its
/// native children even while the daemon owns a cancellation-job duplicate.
pub fn spawn_with_lifetime(command: &mut Command) -> io::Result<(Child, Job, Job)> {
    let lifetime = Job::new()?;
    let cancellation = Job::new()?;
    let child = spawn_in_jobs(command, &[&lifetime, &cancellation])?;
    Ok((child, lifetime, cancellation))
}

fn spawn_in_jobs(command: &mut Command, jobs: &[&Job]) -> io::Result<Child> {
    let mut child = command
        .creation_flags(CREATE_NEW_PROCESS_GROUP | CREATE_SUSPENDED)
        .spawn()?;
    let start = || {
        for job in jobs {
            if unsafe { AssignProcessToJobObject(job.0.as_raw_handle(), child.as_raw_handle()) }
                == 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        resume_suspended(&child)
    };
    if let Err(error) = start() {
        // Assignment/resume failures must never leave a suspended process or
        // let a compiler run outside the request's job.
        let _ = child.kill();
        let _ = child.wait();
        return Err(error);
    }
    Ok(child)
}

/// Resume a child spawned with `CREATE_SUSPENDED`, through its own process
/// handle. A suspended process has only its primary thread and cannot create
/// another before this resume, so resuming the process resumes exactly that
/// thread.
///
/// `std::process::Child` does not expose the primary thread on stable Rust.
/// Finding it with a Toolhelp thread snapshot reads every thread on the
/// machine: with a few hundred compilers starting at once on a 128-thread
/// host, each snapshot took seconds and its compiler sat suspended meanwhile,
/// doubling a cold workspace build.
fn resume_suspended(child: &Child) -> io::Result<()> {
    #[link(name = "ntdll")]
    unsafe extern "system" {
        /// Resume every thread of `process`. Exported by ntdll since Windows
        /// XP; the process handle needs `PROCESS_SUSPEND_RESUME`.
        fn NtResumeProcess(process: HANDLE) -> i32;
    }
    // SAFETY: `child` owns a live handle to the process it spawned, which
    // `CreateProcessW` opened with full access, `PROCESS_SUSPEND_RESUME`
    // included; the call reads nothing else.
    let status = unsafe { NtResumeProcess(child.as_raw_handle()) };
    if status < 0 {
        return Err(io::Error::other(format!(
            "resuming the suspended compiler failed: NTSTATUS {status:#010x}"
        )));
    }
    Ok(())
}

impl std::os::windows::io::AsHandle for Job {
    fn as_handle(&self) -> std::os::windows::io::BorrowedHandle<'_> {
        self.0.as_handle()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::time::{Duration, Instant};
    use windows_sys::Win32::Foundation::{WAIT_OBJECT_0, WAIT_TIMEOUT};
    use windows_sys::Win32::System::Threading::{
        OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE, WaitForSingleObject,
    };

    fn ready(path: &Path) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !path.exists() {
            assert!(
                Instant::now() < deadline,
                "native lifetime fixture did not become ready"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn fixture(root: &Path, mode: &str) -> Command {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored",
                "--exact",
                "windows_process::tests::lifetime_fixture",
            ])
            .env("KACHE_LIFETIME_ROOT", root)
            .env("KACHE_LIFETIME_MODE", mode)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        command
    }

    #[test]
    #[ignore = "child process for the Windows frontend-lifetime test"]
    fn lifetime_fixture() {
        let root = std::path::PathBuf::from(std::env::var_os("KACHE_LIFETIME_ROOT").unwrap());
        if std::env::var("KACHE_LIFETIME_MODE").unwrap() == "descendant" {
            std::fs::write(root.join("ready"), b"ready").unwrap();
            std::thread::sleep(Duration::from_secs(30));
            return;
        }
        protect_current_process().unwrap();
        // Discovery helpers use ordinary Command and must still belong to
        // this process's lifetime, without an explicit per-compiler job.
        let descendant = fixture(&root, "descendant").spawn().unwrap();
        std::fs::write(root.join("pid"), descendant.id().to_string()).unwrap();
        ready(&root.join("ready"));
        ready(&root.join("release"));
        std::process::exit(23);
    }

    struct Fixture(Child);

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[test]
    fn frontend_exit_keeps_its_exit_code_and_terminates_indirect_helpers() {
        let root = tempfile::tempdir().unwrap();
        let mut frontend = Fixture(fixture(root.path(), "frontend").spawn().unwrap());
        ready(&root.path().join("pid"));
        ready(&root.path().join("ready"));
        let pid: u32 = std::fs::read_to_string(root.path().join("pid"))
            .unwrap()
            .parse()
            .unwrap();
        let process = unsafe {
            OpenProcess(
                PROCESS_SYNCHRONIZE | PROCESS_QUERY_LIMITED_INFORMATION,
                0,
                pid,
            )
        };
        assert!(!process.is_null());
        let process = unsafe { OwnedHandle::from_raw_handle(process) };
        assert_eq!(
            unsafe { WaitForSingleObject(process.as_raw_handle(), 0) },
            WAIT_TIMEOUT
        );
        std::fs::write(root.path().join("release"), b"go").unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let status = loop {
            if let Some(status) = frontend.0.try_wait().unwrap() {
                break status;
            }
            assert!(Instant::now() < deadline, "native frontend did not exit");
            std::thread::sleep(Duration::from_millis(5));
        };
        assert_eq!(status.code(), Some(23));
        assert_eq!(
            unsafe { WaitForSingleObject(process.as_raw_handle(), 5_000) },
            WAIT_OBJECT_0
        );
    }
}
