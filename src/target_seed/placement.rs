//! Resolve the intermediate directory without changing Cargo's configuration.

use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::Command;

const OVERRIDE: &str = "KACHE_CARGO_SEED_TARGET_DIR";
const BUILD_OVERRIDE: &str = "KACHE_CARGO_SEED_BUILD_DIR";

/// A Cargo front end can see flags that rustc's wrapper cannot. An empty
/// override disables seeding when command-line configuration is ambiguous.
pub(super) fn configure(command: &mut Command, args: &[OsString], isolated_build_dir: bool) {
    command.env_remove(OVERRIDE);
    command.env_remove(BUILD_OVERRIDE);
    if isolated_build_dir {
        command.env(BUILD_OVERRIDE, "{workspace-root}/target");
    }
    match cli_target(args) {
        Ok(Some(path)) => {
            command.env(OVERRIDE, path);
        }
        Ok(None) => {}
        Err(()) => {
            command.env(OVERRIDE, "");
        }
    }
}

fn cli_target(args: &[OsString]) -> Result<Option<&OsStr>, ()> {
    let mut target = None;
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        if arg == "--" {
            break;
        }
        let text = arg.to_str().ok_or(())?;
        if text == "--target-dir" {
            target = Some(
                args.next()
                    .filter(|arg| !arg.is_empty())
                    .ok_or(())?
                    .as_os_str(),
            );
        } else if let Some(path) = text.strip_prefix("--target-dir=") {
            if path.is_empty() {
                return Err(());
            }
            target = Some(OsStr::new(path));
        } else if text == "--config"
            || text.starts_with("--config=")
            || text.starts_with("--manifest-path")
            || text.starts_with("-C")
            || text.starts_with("-Z")
        {
            return Err(());
        }
    }
    Ok(target)
}

pub(super) fn override_target() -> Option<OsString> {
    std::env::var_os(OVERRIDE)
}

pub(super) fn override_build() -> Option<OsString> {
    std::env::var_os(BUILD_OVERRIDE)
}

/// Environment paths are relative to cwd; file paths are relative to the
/// directory above `.cargo`. Unknown templates and includes remain unseeded.
pub(super) fn resolve(
    cwd: &Path,
    workspace: &Path,
    cargo_home: &Path,
    target_env: Option<&OsStr>,
    build_env: Option<&OsStr>,
) -> Option<PathBuf> {
    let mut target = workspace.join("target");
    let mut build = None;
    let mut directories = vec![cargo_home.to_path_buf()];
    directories.extend(
        cwd.ancestors()
            .map(|path| path.join(".cargo"))
            .collect::<Vec<_>>()
            .into_iter()
            .rev(),
    );
    for directory in directories {
        let legacy = directory.join("config");
        let path = if legacy.is_file() {
            legacy
        } else {
            directory.join("config.toml")
        };
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return None,
        };
        let table: toml::Table = text.parse().ok()?;
        if table.contains_key("include") {
            return None;
        }
        let Some(settings) = table.get("build") else {
            continue;
        };
        let settings = settings.as_table()?;
        let base = directory.parent()?;
        if let Some(value) = settings.get("target-dir") {
            target = base.join(value.as_str().filter(|value| !value.is_empty())?);
        }
        if let Some(value) = settings.get("build-dir") {
            build = Some(expand(base, value.as_str()?, workspace, cargo_home)?);
        }
    }
    if let Some(value) = target_env {
        if value.is_empty() {
            return None;
        }
        target = cwd.join(value);
    }
    if let Some(value) = build_env {
        build = Some(expand(cwd, value.to_str()?, workspace, cargo_home)?);
    }
    Some(build.unwrap_or(target))
}

fn expand(base: &Path, value: &str, workspace: &Path, cargo_home: &Path) -> Option<PathBuf> {
    if value.is_empty() {
        return None;
    }
    let value = value
        .replace("{workspace-root}", workspace.to_str()?)
        .replace("{cargo-cache-home}", cargo_home.to_str()?);
    if value.contains(['{', '}']) {
        return None;
    }
    Some(base.join(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &Path, text: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }

    #[test]
    fn resolves_config_precedence_and_each_files_relative_base() {
        let temp = tempfile::tempdir().unwrap();
        let ws = temp.path().join("ws");
        let home = temp.path().join("home/.cargo");
        let cwd = ws.join("member");
        std::fs::create_dir_all(&cwd).unwrap();
        let resolve = || super::resolve(&cwd, &ws, &home, None, None).unwrap();
        assert_eq!(resolve(), ws.join("target"));
        write(&home.join("config.toml"), "[build]\ntarget-dir='global'");
        assert_eq!(resolve(), temp.path().join("home/global"));
        write(
            &ws.join(".cargo/config.toml"),
            "[build]\ntarget-dir='local'",
        );
        assert_eq!(resolve(), ws.join("local"));
        write(&cwd.join(".cargo/config"), "[build]\ntarget-dir='editor'");
        write(
            &cwd.join(".cargo/config.toml"),
            "[build]\ntarget-dir='ignored'",
        );
        assert_eq!(resolve(), cwd.join("editor"));
        assert_eq!(
            super::resolve(&cwd, &ws, &home, Some(OsStr::new("env")), None),
            Some(cwd.join("env"))
        );
        write(
            &ws.join(".cargo/config.toml"),
            "[build]\nbuild-dir='{workspace-root}/intermediate'",
        );
        assert_eq!(resolve(), ws.join("intermediate"));
        assert_eq!(
            super::resolve(&cwd, &ws, &home, None, Some(OsStr::new("build-env"))),
            Some(cwd.join("build-env"))
        );
        assert_eq!(
            super::resolve(
                &cwd,
                &ws,
                &home,
                None,
                Some(OsStr::new("{cargo-cache-home}/build"))
            ),
            Some(home.join("build"))
        );
    }

    #[test]
    fn unsupported_or_invalid_configuration_stays_unseeded() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join(".cargo");
        for text in [
            "not toml [",
            "include=['other.toml']",
            "[build]\nbuild-dir='{workspace-path-hash}'",
            "[build]\nbuild-dir=''",
            "[build]\ntarget-dir=42",
            "[build]\ntarget-dir=''",
            "build=42",
        ] {
            write(&home.join("config.toml"), text);
            assert_eq!(
                resolve(temp.path(), temp.path(), &home, None, None),
                None,
                "{text}"
            );
        }
        std::fs::remove_file(home.join("config.toml")).unwrap();
        assert_eq!(
            resolve(temp.path(), temp.path(), &home, Some(OsStr::new("")), None),
            None
        );
        assert_eq!(
            resolve(temp.path(), temp.path(), &home, None, Some(OsStr::new(""))),
            None
        );
    }

    #[test]
    fn configuration_read_errors_stay_unseeded() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join(".cargo");
        // A directory at the config path fails to read on every platform,
        // including privileged test runners where permission denial does not.
        std::fs::create_dir_all(home.join("config.toml")).unwrap();
        assert_eq!(resolve(temp.path(), temp.path(), &home, None, None), None);
    }

    #[test]
    fn cargo_flags_override_inherited_paths_without_reading_program_arguments() {
        let args = |values: &[&str]| values.iter().map(OsString::from).collect::<Vec<_>>();
        assert_eq!(
            cli_target(&args(&["check", "--target-dir", "one", "--target-dir=two"])),
            Ok(Some(OsStr::new("two")))
        );
        assert_eq!(
            cli_target(&args(&["run", "--", "--target-dir=ignored"])),
            Ok(None)
        );
        for values in [
            vec!["check", "--target-dir"],
            vec!["check", "--target-dir="],
            vec!["check", "--config=build.target-dir='x'"],
            vec!["check", "--config", "x"],
            vec!["check", "--manifest-path", "x"],
            vec!["-Cother", "check"],
            vec!["check", "-Zunstable-options"],
        ] {
            assert_eq!(cli_target(&args(&values)), Err(()), "{values:?}");
        }
        let mut command = Command::new("cargo");
        configure(&mut command, &args(&["check", "--target-dir=editor"]), true);
        assert_eq!(
            command
                .get_envs()
                .find(|(name, _)| *name == BUILD_OVERRIDE)
                .unwrap()
                .1,
            Some(OsStr::new("{workspace-root}/target"))
        );
        assert_eq!(
            command
                .get_envs()
                .find(|(name, _)| *name == OVERRIDE)
                .unwrap()
                .1,
            Some(OsStr::new("editor"))
        );
        configure(
            &mut command,
            &args(&["check", "--config=build.target-dir='x'"]),
            false,
        );
        assert_eq!(
            command
                .get_envs()
                .find(|(name, _)| *name == OVERRIDE)
                .unwrap()
                .1,
            Some(OsStr::new(""))
        );
        configure(&mut command, &args(&["check"]), false);
        assert_eq!(
            command
                .get_envs()
                .find(|(name, _)| *name == BUILD_OVERRIDE)
                .unwrap()
                .1,
            None
        );
        assert_eq!(
            command
                .get_envs()
                .find(|(name, _)| *name == OVERRIDE)
                .unwrap()
                .1,
            None
        );
    }
}
