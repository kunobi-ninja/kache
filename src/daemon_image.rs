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

/// Where the images of the daemon at `socket` live. Starting that daemon
/// holds the socket's start lock, so nothing else changes this directory.
pub(super) fn root(socket: &Path) -> PathBuf {
    socket.with_extension("images")
}

/// Copy `engine` to `<root>/<content hash>/kache-engine.exe` and return the
/// copy. Images of earlier releases are removed unless a process still runs
/// them.
///
/// The directory is named after the bytes copied rather than the source, so
/// an install that replaces `engine` during the copy cannot leave an image
/// under another release's name.
pub(super) fn stage(engine: &Path, root: &Path) -> Result<PathBuf> {
    std::fs::create_dir_all(root).with_context(|| format!("creating {}", root.display()))?;
    let partial = root.join(format!("{}.partial", std::process::id()));
    let placed = (|| {
        std::fs::copy(engine, &partial)
            .with_context(|| format!("copying {} to {}", engine.display(), partial.display()))?;
        let digest = kache_store::file_hash::hash_file(&partial)?;
        let name = digest[..32].to_owned();
        let image = root.join(&name).join(IMAGE);
        if !image.is_file() {
            std::fs::create_dir_all(root.join(&name))?;
            std::fs::rename(&partial, &image)
                .with_context(|| format!("placing {}", image.display()))?;
        }
        anyhow::Ok((name, image))
    })();
    let _ = std::fs::remove_file(&partial);
    let (name, image) = placed?;
    prune(root, &name);
    Ok(image)
}

/// Remove every entry of `root` but `keep`. A running image cannot be
/// deleted on Windows, so the daemon still using it keeps it.
fn prune(root: &Path, keep: &str) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        if entry.file_name() == keep {
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
    fn staging_reuses_an_image_and_removes_older_ones() {
        let dir = tempfile::tempdir().unwrap();
        let root = root(&dir.path().join("daemon.sock"));
        let engine = dir.path().join("kache-engine");
        std::fs::write(&engine, "release one").unwrap();
        let one = stage(&engine, &root).unwrap();
        assert_eq!(std::fs::read(&one).unwrap(), b"release one");
        assert_ne!(one, engine);
        assert_eq!(stage(&engine, &root).unwrap(), one);

        std::fs::write(root.join("7.partial"), "interrupted copy").unwrap();
        std::fs::write(&engine, "release two").unwrap();
        let two = stage(&engine, &root).unwrap();
        assert_ne!(two.parent(), one.parent());
        assert_eq!(std::fs::read(&two).unwrap(), b"release two");
        let left: Vec<_> = std::fs::read_dir(&root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert_eq!(left, [two.parent().unwrap()]);
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
