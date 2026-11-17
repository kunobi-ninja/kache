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
/// copied once however many caches its daemons serve.
#[cfg(windows)]
pub(super) fn root(socket: &Path) -> PathBuf {
    dirs::data_local_dir()
        .map(|dir| dir.join("kache").join("daemon-engines"))
        .unwrap_or_else(|| socket.with_extension("images"))
}

/// Exclusive use of an images root from staging until the daemon runs.
/// Pruning happens under it, so no starter deletes an image another starter
/// has placed but not yet started. A running image cannot be deleted.
pub(super) struct Lock {
    _file: std::fs::File,
}

pub(super) fn lock(root: &Path) -> Result<Lock> {
    std::fs::create_dir_all(root).with_context(|| format!("creating {}", root.display()))?;
    let path = root.join(LOCK);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .with_context(|| format!("opening {}", path.display()))?;
    file.lock()
        .with_context(|| format!("locking {}", path.display()))?;
    Ok(Lock { _file: file })
}

/// The image name for an engine: its release, length and modification time.
/// A copy keeps the modification time, so this names the copy too.
fn identity(metadata: &std::fs::Metadata) -> String {
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0, |time| time.as_nanos());
    format!("{VERSION}-{:x}-{modified:x}", metadata.len())
}

/// Copy `engine` to `<root>/<identity>/kache-engine.exe` unless that copy
/// exists, and return it. Images no process runs are removed. The caller
/// holds [`lock`].
pub(super) fn stage(engine: &Path, root: &Path, _lock: &Lock) -> Result<PathBuf> {
    let name = identity(&std::fs::metadata(engine)?);
    let image = root.join(&name).join(IMAGE);
    if !image.is_file() {
        std::fs::create_dir_all(root.join(&name))?;
        let partial = image.with_extension("partial");
        let placed = (|| {
            std::fs::copy(engine, &partial).with_context(|| {
                format!("copying {} to {}", engine.display(), partial.display())
            })?;
            anyhow::ensure!(
                identity(&std::fs::metadata(engine)?) == name,
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

    #[test]
    fn staging_copies_an_engine_once_and_removes_older_images() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("images");
        let engine = dir.path().join("kache-engine");
        std::fs::write(&engine, "release one").unwrap();
        let lock = lock(&root).unwrap();
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
