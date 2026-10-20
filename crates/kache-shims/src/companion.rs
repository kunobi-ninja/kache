//! Locate the frontend and engine shipped in one installation.

use std::io;
use std::path::{Path, PathBuf};

#[cfg(windows)]
const ENGINE: &str = "kache-engine.exe";
#[cfg(not(windows))]
const ENGINE: &str = "kache-engine";
#[cfg(windows)]
const FRONTEND: &str = "kache.exe";
#[cfg(not(windows))]
const FRONTEND: &str = "kache";

/// Frontend image captured before entering the engine.
#[derive(Debug, Clone)]
pub struct FrontendImage {
    pub path: PathBuf,
    pub identity: Option<crate::InodeId>,
}

/// The frontend beside a named engine; other executables keep their identity.
/// This also preserves unit-test executables and pinned build-script engines.
pub fn frontend_for(executable: &Path) -> PathBuf {
    if executable.file_name().is_some_and(|name| name == ENGINE) {
        executable.with_file_name(FRONTEND)
    } else {
        executable.to_owned()
    }
}

/// The companion beside an already selected frontend path, retaining stable
/// installer aliases such as Homebrew's opt directory.
pub fn engine_beside(frontend: &Path) -> io::Result<PathBuf> {
    let engine = frontend.with_file_name(ENGINE);
    if crate::is_executable_file(&engine) {
        Ok(engine)
    } else {
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("{} is missing or not executable", engine.display()),
        ))
    }
}

/// Follow a compiler-name shim to its installation before finding the engine.
pub fn engine_for(frontend: &Path) -> io::Result<PathBuf> {
    engine_beside(&std::fs::canonicalize(frontend)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn named_engines_use_their_frontend_and_test_engines_keep_their_path() {
        let root = tempfile::tempdir().unwrap();
        assert_eq!(
            frontend_for(&root.path().join(ENGINE)),
            root.path().join(FRONTEND)
        );
        let test = root.path().join("kache_engine-fixture");
        assert_eq!(frontend_for(&test), test);
        assert!(engine_beside(&root.path().join(FRONTEND)).is_err());
    }

    #[test]
    fn captured_frontend_identity_rejects_a_replaced_installation() {
        let root = tempfile::tempdir().unwrap();
        let engine = root.path().join(ENGINE);
        let frontend = root.path().join(FRONTEND);
        for path in [&engine, &frontend] {
            std::fs::write(path, "image").unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
            }
        }
        let env = crate::Env {
            exe: engine.clone(),
            exe_identity: crate::Fs::identity(&crate::RealFs, &engine),
            ..Default::default()
        };
        let captured = FrontendImage {
            path: frontend.clone(),
            identity: crate::Fs::identity(&crate::RealFs, &frontend),
        };
        let selected = crate::select_frontend(&env, Some(&captured), &crate::RealFs).unwrap();
        assert_eq!(
            selected.path.canonicalize().unwrap(),
            frontend.canonicalize().unwrap()
        );
        let replacement = root.path().join("replacement");
        std::fs::write(&replacement, "new image").unwrap();
        std::fs::rename(replacement, &frontend).unwrap();
        assert!(
            matches!(crate::select_frontend(&env, Some(&captured), &crate::RealFs), Err(crate::Error::Replaced(path)) if path == frontend)
        );
        let captured = FrontendImage {
            path: frontend.clone(),
            identity: crate::Fs::identity(&crate::RealFs, &frontend),
        };
        let replacement = root.path().join("new-engine");
        std::fs::write(&replacement, "new engine").unwrap();
        std::fs::rename(replacement, &engine).unwrap();
        assert!(
            matches!(crate::select_frontend(&env, Some(&captured), &crate::RealFs), Err(crate::Error::Replaced(path)) if path == engine)
        );
    }

    #[cfg(unix)]
    #[test]
    fn compiler_shims_find_the_engine_in_the_resolved_installation() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let root = tempfile::tempdir().unwrap();
        let install = root.path().join("install");
        let farm = root.path().join("farm");
        std::fs::create_dir(&install).unwrap();
        std::fs::create_dir(&farm).unwrap();
        std::fs::write(install.join(FRONTEND), "frontend").unwrap();
        let engine = install.join(ENGINE);
        std::fs::write(&engine, "engine").unwrap();
        std::fs::set_permissions(&engine, std::fs::Permissions::from_mode(0o755)).unwrap();
        symlink(install.join(FRONTEND), farm.join("cc")).unwrap();
        assert_eq!(
            engine_for(&farm.join("cc")).unwrap(),
            engine.canonicalize().unwrap()
        );
    }
}
