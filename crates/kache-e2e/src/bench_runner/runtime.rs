//! A Kache benchmark owns its daemon across all measured phases. The store
//! can live on a shared build volume while IPC and event logs stay private.

use crate::daemon;
use anyhow::{Context, Result, ensure};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tempfile::TempDir;

pub(super) struct Runtime<'a> {
    dir: TempDir,
    kache: &'a Path,
    store: &'a Path,
}

impl<'a> Runtime<'a> {
    pub(super) fn new(kache: &'a Path, store: &'a Path) -> Result<Self> {
        let mut builder = tempfile::Builder::new();
        builder.prefix("kache-bench-");
        #[cfg(unix)]
        let dir = {
            use std::os::unix::fs::PermissionsExt;
            builder.permissions(std::fs::Permissions::from_mode(0o700));
            builder.tempdir_in("/tmp")
        };
        #[cfg(not(unix))]
        let dir = builder.tempdir();
        let dir = dir.context("creating private benchmark runtime")?;
        // A large build must keep the whole phase in its report.
        std::fs::write(
            dir.path().join("kache-config.toml"),
            "[cache]\nevent_log_max_size = \"8GiB\"\n",
        )?;
        Ok(Self { dir, kache, store })
    }

    pub(super) fn path(&self) -> &Path {
        self.dir.path()
    }
}

impl Drop for Runtime<'_> {
    fn drop(&mut self) {
        // A failed build can return before its phase drains. Keep the endpoint
        // reachable if shutdown fails so the daemon can still be stopped.
        if !locks_released(self.dir.path(), Duration::ZERO).unwrap_or(false)
            && let Err(error) = drain(self.kache, self.store, self.dir.path())
        {
            self.dir.disable_cleanup(true);
            eprintln!(
                "[bench] {error}; retaining runtime {}",
                self.dir.path().display()
            );
        }
    }
}

pub(super) fn configure(cmd: &mut Command, store: &Path, runtime: &Path) {
    cmd.env("KACHE_CACHE_DIR", store)
        .env("KACHE_RUNTIME_DIR", runtime)
        .env("KACHE_CONFIG", runtime.join("kache-config.toml"))
        .env_remove("KACHE_SOCKET_PATH");
}

pub(super) fn command(binary: &Path, store: &Path, runtime: &Path) -> Command {
    let mut cmd = Command::new(binary);
    configure(&mut cmd, store, runtime);
    cmd
}

pub(super) fn start(binary: &Path, store: &Path, runtime: &Path) -> bool {
    daemon::start_command(command(binary, store, runtime))
}

pub(super) fn drain(binary: &Path, store: &Path, runtime: &Path) -> Result<()> {
    let _ = command(binary, store, runtime)
        .args(["daemon", "stop"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    ensure!(
        locks_released(runtime, daemon::DRAIN_TIMEOUT)?,
        "daemon did not stop after the build: {}",
        runtime.display()
    );
    Ok(())
}

// The binary under test chooses the name, including a PR-scope suffix if configured.
fn locks_released(runtime: &Path, timeout: Duration) -> Result<bool> {
    let deadline = Instant::now() + timeout;
    for entry in std::fs::read_dir(runtime)? {
        let entry = entry?;
        if entry.file_name().as_encoded_bytes().ends_with(b".run.lock")
            && !daemon::wait_for_lock_path(
                &entry.path(),
                deadline.saturating_duration_since(Instant::now()),
            )
        {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_benchmark_owns_a_runtime_and_children_use_its_settings() {
        let store = tempfile::tempdir().unwrap();
        let kache = Path::new("kache");
        let first = Runtime::new(kache, store.path()).unwrap();
        let second = Runtime::new(kache, store.path()).unwrap();
        assert_ne!(first.path(), second.path());
        assert!(!first.path().starts_with(store.path()));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(first.path())
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }
        let mut cmd = Command::new(kache);
        cmd.env("KACHE_SOCKET_PATH", "wrong")
            .env("KACHE_RUNTIME_DIR", "wrong");
        configure(&mut cmd, store.path(), first.path());
        let env: std::collections::BTreeMap<_, _> = cmd.get_envs().collect();
        let value = |name: &str| env[std::ffi::OsStr::new(name)];
        assert_eq!(value("KACHE_CACHE_DIR"), Some(store.path().as_os_str()));
        assert_eq!(value("KACHE_RUNTIME_DIR"), Some(first.path().as_os_str()));
        assert_eq!(
            value("KACHE_CONFIG"),
            Some(first.path().join("kache-config.toml").as_os_str())
        );
        assert_eq!(value("KACHE_SOCKET_PATH"), None);
        assert!(first.path().join("kache-config.toml").is_file());
        let path = first.path().to_owned();
        drop(first);
        assert!(!path.exists());
    }

    #[test]
    fn draining_waits_for_the_daemons_actual_runtime_lock() {
        let runtime = tempfile::tempdir().unwrap();
        let lock =
            std::fs::File::create(runtime.path().join("daemon-pr-12345678.run.lock")).unwrap();
        lock.lock().unwrap();
        assert!(!locks_released(runtime.path(), Duration::ZERO).unwrap());
        drop(lock);
        assert!(locks_released(runtime.path(), Duration::ZERO).unwrap());
    }
}
