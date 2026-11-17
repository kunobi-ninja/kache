//! The copy of the engine a Windows daemon runs from.
//!
//! Windows cannot replace an executable while a process runs it. A daemon
//! started from the installed `kache-engine.exe` made `cargo install kache`
//! fail with "Access is denied" when it moved the new engine into place, and
//! building the new release through kache is what started that daemon
//! (kunobi-ninja/kache#1949). The daemon runs from a copy instead.
use super::*;

const IMAGE: &str = if cfg!(windows) {
    "kache-engine.exe"
} else {
    "kache-engine"
};
const LOCK: &str = "lock";

/// Where daemon images live: one directory per user, so each engine is
/// copied once however many caches its daemons serve. Pruning removes what
/// else is there, so the directory must be one only Kache writes to.
#[cfg(windows)]
pub(super) fn root() -> Result<PathBuf> {
    Ok(dirs::data_local_dir()
        .context("locating the local application data directory")?
        .join("kache")
        .join("daemon-engines"))
}

/// Exclusive use of an images root from staging until the daemon runs.
/// Pruning happens under it, so no starter deletes an image another starter
/// has placed but not yet started. NTFS refuses to delete a running image.
pub(super) struct Lock {
    _file: std::fs::File,
}

/// When to stop waiting for the images lock: 5 s from `now`, or the start
/// `deadline` if that is sooner. Another cache's start holds the lock only
/// while it copies and spawns, and waiting longer would stall this build.
pub(super) fn lock_deadline(now: Instant, deadline: Instant) -> Instant {
    deadline.min(now + Duration::from_secs(5))
}

/// Take the images lock, or fail at `deadline` so the caller starts the
/// installed engine instead of waiting on another cache's daemon start.
pub(super) fn lock(root: &Path, deadline: Instant) -> Result<Lock> {
    std::fs::create_dir_all(root).with_context(|| format!("creating {}", root.display()))?;
    let path = root.join(LOCK);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .with_context(|| format!("opening {}", path.display()))?;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(Lock { _file: file }),
            Err(std::fs::TryLockError::WouldBlock) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(std::fs::TryLockError::WouldBlock) => {
                anyhow::bail!("{} stayed locked by another daemon start", path.display())
            }
            Err(std::fs::TryLockError::Error(error)) => {
                return Err(error).with_context(|| format!("locking {}", path.display()));
            }
        }
    }
}

/// The image name for an engine: its release, length, modification time and
/// file ID. Two builds of one release can share a length and a timestamp, but
/// not a file; installing or relinking an engine creates a new one.
fn identity(engine: &Path) -> Result<String> {
    let metadata = std::fs::metadata(engine)?;
    let file = kache_fs::file_identity(engine)?;
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0, |time| time.as_nanos());
    Ok(format!(
        "{VERSION}-{:x}-{modified:x}-{:x}-{:x}",
        metadata.len(),
        file.dev,
        file.ino
    ))
}

/// Copy `engine` to `<root>/<identity>/kache-engine.exe` unless that copy
/// exists, and return it. Images no process runs are removed. The caller
/// holds [`lock`].
pub(super) fn stage(engine: &Path, root: &Path, _lock: &Lock) -> Result<PathBuf> {
    let name = identity(engine)?;
    let dir = root.join(&name);
    let image = dir.join(IMAGE);
    if !image.is_file() {
        std::fs::create_dir_all(&dir)?;
        // An interrupted start can leave a copy behind, possibly read-only.
        // A new name never collides with it, and pruning removes it later.
        let partial = dir.join(format!(
            "{IMAGE}.{}-{}.partial",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |time| time.as_nanos())
        ));
        let placed = (|| {
            std::fs::copy(engine, &partial).with_context(|| {
                format!("copying {} to {}", engine.display(), partial.display())
            })?;
            anyhow::ensure!(
                identity(engine)? == name,
                "{} changed while it was copied",
                engine.display()
            );
            std::fs::rename(&partial, &image)
                .with_context(|| format!("placing {}", image.display()))
        })();
        if placed.is_err() {
            let _ = std::fs::remove_file(&partial);
        }
        placed?;
    }
    prune(root, &name);
    Ok(image)
}

/// Remove every image in `root` but `keep`. Windows refuses to delete one a
/// process runs, so a running daemon keeps its image.
fn prune(root: &Path, keep: &str) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        if entry.file_name() == keep || entry.file_name() == LOCK {
            continue;
        }
        let path = entry.path();
        let _ = if path.is_dir() {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
        };
    }
}

/// `kache daemon run` for a staged image. The forwarded-argv envelope names
/// the installed frontend, which the copy cannot find beside itself.
pub(super) fn command(image: &Path, frontend: &Path) -> std::process::Command {
    let mut command = std::process::Command::new(image);
    command
        .arg(kache_client::FORWARDED_ARGV_FLAG)
        .arg(VERSION)
        .arg("0")
        .arg(frontend)
        .arg("-")
        .args(["kache", "daemon", "run"])
        .env(crate::platform::SELF_SPAWN_ENV, "daemon");
    command
}

#[cfg(test)]
mod tests {
    use super::*;

    fn later() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }

    #[test]
    fn staging_copies_an_engine_once_and_removes_older_images() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("images");
        let engine = dir.path().join("kache-engine");
        std::fs::write(&engine, "release one").unwrap();
        let lock = lock(&root, later()).unwrap();
        let one = stage(&engine, &root, &lock).unwrap();
        assert_ne!(one, engine);
        assert_eq!(std::fs::read(&one).unwrap(), b"release one");
        std::fs::write(&one, "staged").unwrap();
        assert_eq!(stage(&engine, &root, &lock).unwrap(), one);
        assert_eq!(std::fs::read(&one).unwrap(), b"staged", "copied again");

        std::fs::write(root.join("left.partial"), "interrupted copy").unwrap();
        std::fs::write(&engine, "release two, longer").unwrap();
        let two = stage(&engine, &root, &lock).unwrap();
        assert_ne!(two.parent(), one.parent());
        assert_eq!(std::fs::read(&two).unwrap(), b"release two, longer");
        let mut left: Vec<_> = std::fs::read_dir(&root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        left.sort();
        let mut expected = vec![two.parent().unwrap().to_owned(), root.join(LOCK)];
        expected.sort();
        assert_eq!(left, expected);
    }

    /// A rebuild of one release can match the old engine's length and
    /// timestamp. Installing it replaces the file, so it gets its own image.
    #[test]
    fn a_replaced_engine_with_the_same_length_and_time_is_copied_again() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("images");
        let engine = dir.path().join("kache-engine");
        std::fs::write(&engine, "build A").unwrap();
        let modified = std::fs::metadata(&engine).unwrap().modified().unwrap();
        let lock = lock(&root, later()).unwrap();
        let a = stage(&engine, &root, &lock).unwrap();

        let next = dir.path().join("next");
        std::fs::write(&next, "build B").unwrap();
        std::fs::File::options()
            .write(true)
            .open(&next)
            .unwrap()
            .set_modified(modified)
            .unwrap();
        let keep_a_file = dir.path().join("a");
        std::fs::hard_link(&engine, &keep_a_file).unwrap();
        std::fs::rename(&next, &engine).unwrap();
        let b = stage(&engine, &root, &lock).unwrap();
        assert_ne!(a, b);
        assert_eq!(std::fs::read(&b).unwrap(), b"build B");
    }

    #[test]
    fn the_images_lock_waits_five_seconds_at_most() {
        let now = Instant::now();
        let five = now + Duration::from_secs(5);
        assert_eq!(lock_deadline(now, now + Duration::from_secs(60)), five);
        assert_eq!(lock_deadline(now, five), five);
        let sooner = now + Duration::from_secs(1);
        assert_eq!(lock_deadline(now, sooner), sooner);
    }

    #[test]
    fn a_lock_released_before_the_deadline_is_taken() {
        let dir = tempfile::tempdir().unwrap();
        let held = lock(dir.path(), later()).unwrap();
        let release = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            drop(held);
        });
        lock(dir.path(), later()).unwrap();
        release.join().unwrap();
    }

    #[test]
    fn a_held_images_lock_fails_at_the_deadline_instead_of_waiting() {
        let dir = tempfile::tempdir().unwrap();
        let held = lock(dir.path(), later()).unwrap();
        let error = lock(dir.path(), Instant::now() + Duration::from_millis(50))
            .err()
            .unwrap();
        assert!(error.to_string().contains("stayed locked"), "{error:#}");
        drop(held);
        lock(dir.path(), Instant::now()).unwrap();
    }

    #[test]
    fn a_staged_daemon_learns_the_installed_frontend() {
        let frontend = std::env::current_dir().unwrap().join("bin").join("kache");
        let command = command(Path::new("image"), &frontend);
        let args: Vec<_> = std::iter::once(command.get_program().to_owned())
            .chain(command.get_args().map(ToOwned::to_owned))
            .collect();
        let entry = kache_client::engine_entry(args, VERSION).unwrap();
        assert_eq!(entry.frontend.unwrap().path, frontend);
        let argv: Vec<String> = entry
            .args
            .iter()
            .map(|arg| arg.to_str().unwrap().to_owned())
            .collect();
        assert_eq!(argv, ["kache", "daemon", "run"]);
        let marker = command
            .get_envs()
            .find(|(name, _)| *name == crate::platform::SELF_SPAWN_ENV)
            .and_then(|(_, value)| value);
        assert!(crate::platform::is_self_spawn(&argv, marker));
    }
}
