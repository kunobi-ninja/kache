//! Limit leases to known Cargo build layouts. An unknown command, option,
//! workspace or placement policy keeps the cache-wide lease.

use super::*;
use crate::target_use::Scope;

const DIR_VARIABLES: [&str; 3] = [
    "CARGO_TARGET_DIR",
    "CARGO_BUILD_TARGET_DIR",
    "CARGO_BUILD_BUILD_DIR",
];

/// The shim may invoke Blaze, whose engine places outputs outside Cargo's layout.
pub(super) fn cargo_layout_cwd(cwd: Option<&Path>, blaze: bool) -> Option<&Path> {
    if blaze { None } else { cwd }
}

pub(super) fn of(
    cwd: &Path,
    args: &[OsString],
    env: &crate::invocation::InvocationEnv,
    separate_checks: bool,
) -> Scope {
    if !crate::target_use::SCOPED_LOCKS {
        return Scope::Everything;
    }
    configured_scope(cwd, args, env, separate_checks)
}

fn configured_scope(
    cwd: &Path,
    args: &[OsString],
    env: &crate::invocation::InvocationEnv,
    separate_checks: bool,
) -> Scope {
    if env.vars_os().any(|(name, _)| {
        let Some(name) = name.to_str() else {
            return false;
        };
        let key = name.to_ascii_uppercase();
        key.starts_with("CARGO_UNSTABLE_")
            || key.starts_with("CARGO_CONFIG_")
            || key == "CARGO_INCLUDE"
            || (key.starts_with("CARGO_")
                && key.ends_with("_DIR")
                // Cargo exports the parent package's manifest directory to
                // tests and build scripts that may start their own Cargo.
                && key != "CARGO_MANIFEST_DIR"
                && !DIR_VARIABLES.contains(&key.as_str()))
    }) {
        return Scope::Everything;
    }
    let home = match env.var_os("CARGO_HOME") {
        Some(home) => literal(cwd, Path::new(&home)),
        None => cargo_home(cwd).ok(),
    };
    let variables = DIR_VARIABLES.map(|name| env.var_os(name));
    home.and_then(|home| paths(cwd, args, &variables, &home, separate_checks))
        .map_or(Scope::Everything, Scope::within)
}

fn paths(
    cwd: &Path,
    args: &[OsString],
    variables: &[Option<OsString>],
    cargo_home: &Path,
    separate_checks: bool,
) -> Option<Vec<PathBuf>> {
    cwd.to_str()?;
    cargo_home.to_str()?;
    if args.iter().any(|arg| arg.to_str().is_none()) {
        return None;
    }
    let (_, args) = split_toolchain_selector(args);
    let (command, options) = args.split_first()?;
    match command.to_str()? {
        "build" | "run" | "test" => {}
        "check" if !separate_checks => {}
        _ => return None,
    }
    let mut roots = Vec::new();
    // A logical working directory and its physical path can have different
    // ancestor manifests and Cargo configs.
    for dir in cwd.ancestors() {
        if std::fs::symlink_metadata(dir).ok()?.is_symlink() {
            return None;
        }
    }
    let mut manifest_dir = cwd.to_path_buf();
    let mut args = options.iter();
    while let Some(arg) = args.next() {
        let text = arg.to_str()?;
        if text == "--" {
            break;
        }
        let (option, inline) = text
            .split_once('=')
            .map_or((text, None), |(k, v)| (k, Some(v)));
        match option {
            "--manifest-path" | "--target-dir" => {
                let raw = match inline {
                    Some(value) => value,
                    None => args.next()?.to_str()?,
                };
                if raw.starts_with('-') {
                    return None;
                }
                if option == "--manifest-path" {
                    let path = literal(cwd, Path::new(raw))?;
                    // The ancestor scan below also checks that the manifest is a regular file.
                    if path.file_name()? != "Cargo.toml" || path != path.canonicalize().ok()? {
                        return None;
                    }
                    manifest_dir = path.parent()?.to_owned();
                } else {
                    roots.push(literal(cwd, Path::new(raw))?);
                }
            }
            "--package" | "-p" | "--bin" | "--test" | "--bench" | "--example" | "--features"
            | "-F" | "--jobs" | "-j" | "--color" | "--profile" | "--target" => {
                let value = match inline {
                    Some(value) => value,
                    None => args.next()?.to_str()?,
                };
                if value.is_empty() || value.starts_with('-') {
                    return None;
                }
            }
            "--lib"
            | "--bins"
            | "--tests"
            | "--benches"
            | "--examples"
            | "--all-targets"
            | "--workspace"
            | "--release"
            | "--all-features"
            | "--no-default-features"
            | "--locked"
            | "--offline"
            | "--frozen"
            | "--quiet"
            | "-q"
            | "--verbose"
            | "-v"
            | "-vv"
            | "--no-run"
            | "--no-fail-fast"
                if inline.is_none() => {}
            _ if command == "test" && !text.starts_with('-') => {}
            _ => return None,
        }
    }
    for path in variables.iter().flatten() {
        roots.push(literal(cwd, Path::new(path))?);
    }
    config_dirs(cwd, cargo_home, &mut roots)?;
    // A member can inherit an ancestor workspace. Check each possible default
    // target as well as the workspace itself: target/ can point outside it.
    let mut workspace = None;
    let base = manifest_dir.canonicalize().ok()?;
    for dir in base.ancestors() {
        let manifest = dir.join("Cargo.toml");
        let Some(text) = optional_text(&manifest)? else {
            continue;
        };
        let table: toml::Table = toml::from_str(&text).ok()?;
        if ["package", "project"]
            .iter()
            .any(|key| table.get(*key).and_then(|p| p.get("workspace")).is_some())
        {
            return None;
        }
        roots.push(literal(dir, Path::new("target"))?);
        workspace = Some(dir);
    }
    roots.push(workspace?.to_owned());
    Some(roots)
}

/// Cargo may expand templates or resolve `..` across a symlink. Leave either
/// case to Cargo. Scope::within resolves the existing prefixes of literal paths.
fn literal(base: &Path, value: &Path) -> Option<PathBuf> {
    use std::path::Component;
    let text = value.to_str()?;
    if text.is_empty()
        || text.contains(['{', '}'])
        || value.components().any(|c| c == Component::ParentDir)
    {
        return None;
    }
    let path = base.join(value);
    path.is_absolute().then_some(path)
}

/// Absence is ordinary. An unreadable file, symlink, or malformed file leaves
/// the scope unknown, since Cargo could resolve a different workspace or config.
fn optional_text(path: &Path) -> Option<Option<String>> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.is_file() => Some(Some(std::fs::read_to_string(path).ok()?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Some(None),
        _ => None,
    }
}

fn config_dirs(cwd: &Path, home: &Path, roots: &mut Vec<PathBuf>) -> Option<()> {
    let locals = cwd.ancestors().map(|dir| dir.join(".cargo"));
    for config_dir in std::iter::once(home.to_owned()).chain(locals) {
        let mut config = None;
        for name in ["config", "config.toml"] {
            if let Some(text) = optional_text(&config_dir.join(name))? {
                config = Some(text);
                break;
            }
        }
        let Some(text) = config else { continue };
        let table: toml::Table = toml::from_str(&text).ok()?;
        if table.keys().any(|key| {
            !matches!(
                key.as_str(),
                "build"
                    | "env"
                    | "alias"
                    | "http"
                    | "net"
                    | "term"
                    | "registry"
                    | "registries"
                    | "source"
                    | "target"
                    | "profile"
                    | "credential-alias"
            )
        }) {
            return None;
        }
        if let Some(env) = table.get("env")
            && env
                .as_table()?
                .keys()
                .any(|key| key.to_ascii_uppercase().starts_with("CARGO_"))
        {
            return None;
        }
        if let Some(build) = table.get("build") {
            let build = build.as_table()?;
            if build.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "target-dir"
                        | "build-dir"
                        | "jobs"
                        | "rustflags"
                        | "rustdocflags"
                        | "rustc"
                        | "rustdoc"
                        | "rustc-wrapper"
                        | "rustc-workspace-wrapper"
                        | "incremental"
                        | "target"
                        | "dep-info-basedir"
                        | "pipelining"
                )
            }) {
                return None;
            }
            for key in ["target-dir", "build-dir"] {
                if let Some(value) = build.get(key) {
                    if std::fs::symlink_metadata(&config_dir).ok()?.is_symlink() {
                        return None;
                    }
                    roots.push(literal(config_dir.parent()?, Path::new(value.as_str()?))?);
                }
            }
        }
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    struct Tree {
        _dir: tempfile::TempDir,
        root: PathBuf,
        cwd: PathBuf,
        home: PathBuf,
    }

    impl Tree {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().canonicalize().unwrap();
            let cwd = root.join("repo/member");
            let home = root.join("home/.cargo");
            fs::create_dir_all(&cwd).unwrap();
            fs::create_dir_all(&home).unwrap();
            fs::write(root.join("repo/Cargo.toml"), "[workspace]\n").unwrap();
            fs::write(cwd.join("Cargo.toml"), "[package]\nname='member'\n").unwrap();
            Self {
                _dir: dir,
                root,
                cwd,
                home,
            }
        }

        fn scope(&self, args: &[&str], env: &[Option<OsString>], check: bool) -> Scope {
            let args = args.iter().map(OsString::from).collect::<Vec<_>>();
            paths(&self.cwd, &args, env, &self.home, check).map_or(Scope::Everything, Scope::within)
        }
    }

    #[test]
    fn blaze_has_no_cargo_layout_even_with_a_known_working_directory() {
        let cwd = Some(Path::new("workspace"));
        assert_eq!(cargo_layout_cwd(cwd, false), cwd);
        assert_eq!(cargo_layout_cwd(cwd, true), None);
        assert_eq!(cargo_layout_cwd(None, false), None);
    }

    #[test]
    fn cargo_uses_a_workspace_lease_only_where_scoped_locks_are_supported() {
        let tree = Tree::new();
        let env = crate::invocation::InvocationEnv::from_parts(
            [(OsStr::new("CARGO_HOME"), tree.home.as_os_str())],
            Some(tree.cwd.clone()),
            Some(tree.root.join("home")),
            tree.root.clone(),
        );
        let args = [OsString::from("build")];
        let inferred = configured_scope(&tree.cwd, &args, &env, false);
        assert!(matches!(inferred, Scope::Within(_)));
        let expected = if crate::target_use::SCOPED_LOCKS {
            inferred
        } else {
            Scope::Everything
        };
        assert_eq!(of(&tree.cwd, &args, &env, false), expected);
    }

    #[test]
    fn the_invocation_environment_can_require_a_cache_wide_lease() {
        let tree = Tree::new();
        let args = [OsString::from("build")];
        let parent_package = tree.root.join("parent-package");
        let check = |name: &str, value: &OsStr| {
            let env = crate::invocation::InvocationEnv::from_parts(
                [
                    (OsStr::new("CARGO_HOME"), tree.home.as_os_str()),
                    (OsStr::new("CARGO_MANIFEST_DIR"), parent_package.as_os_str()),
                    (OsStr::new(name), value),
                ],
                Some(tree.cwd.clone()),
                Some(tree.root.join("home")),
                tree.root.clone(),
            );
            configured_scope(&tree.cwd, &args, &env, false)
        };
        let out = tree.root.join("placed");
        for name in DIR_VARIABLES {
            let scope = check(name, out.as_os_str());
            assert!(scope.covers(&out));
            assert!(scope.covers(&tree.cwd));
            assert!(!scope.covers(&parent_package));
            assert!(!scope.covers(&tree.root.join("unrelated")));
        }
        assert!(!check("OTHER_DIR", out.as_os_str()).covers(&out));
        for name in [
            "CARGO_UNSTABLE_FOO",
            "CARGO_CONFIG_INCLUDE",
            "CARGO_INCLUDE",
            "CARGO_BUILD_ARTIFACT_DIR",
        ] {
            assert_eq!(check(name, OsStr::new("x")), Scope::Everything);
        }
        assert_eq!(check("CARGO_TARGET_DIR", OsStr::new("")), Scope::Everything);
    }

    #[test]
    fn common_commands_lease_the_workspace_and_known_output_directories() {
        let tree = Tree::new();
        let outside = tree.root.join("external");
        let home_output = tree.root.join("home/output");
        let config = tree.home.join("config.toml");
        fs::write(config, "[build]\nbuild-dir='output'\n").unwrap();
        let env = [Some(outside.clone().into_os_string())];
        for args in [
            &["+stable", "build", "--locked", "-p", "member", "--release"][..],
            &["test", "name", "--", "a-filter"],
            &["run", "--features=feature", "--bin", "member"],
            &["check", "--manifest-path", "Cargo.toml"],
        ] {
            let scope = tree.scope(args, &env, false);
            assert!(scope.covers(&tree.root.join("repo/target")));
            assert!(scope.covers(&home_output));
            assert!(scope.covers(&outside));
            assert!(!scope.covers(&tree.root.join("unrelated")));
        }
        let scope = tree.scope(
            &["build", "--target-dir", outside.to_str().unwrap()],
            &[],
            false,
        );
        assert!(scope.covers(&outside));
        assert!(!scope.covers(&tree.root.join("other")));
        let arg = format!("--target-dir={}", outside.display());
        assert!(tree.scope(&["build", &arg], &[], false).covers(&outside));
    }

    // Unix returns ENOTDIR for the second lookup; Windows may report the
    // same ERROR_PATH_NOT_FOUND as it does for a missing directory.
    #[cfg(unix)]
    #[test]
    fn an_unusable_cargo_home_is_different_from_an_absent_config() {
        let tree = Tree::new();
        let home = tree.root.join("invalid-home");
        let args = [OsString::from("build")];
        assert!(paths(&tree.cwd, &args, &[], &home, false).is_some());
        fs::write(&home, "not a directory").unwrap();
        assert!(paths(&tree.cwd, &args, &[], &home, false).is_none());
    }

    #[test]
    fn an_existing_nonstandard_manifest_requires_the_global_lease() {
        let tree = Tree::new();
        fs::write(tree.cwd.join("project.toml"), "[workspace]").unwrap();
        assert_eq!(
            tree.scope(&["build", "--manifest-path=project.toml"], &[], false),
            Scope::Everything
        );
    }

    #[test]
    fn unknown_commands_and_placement_keep_the_cache_wide_lease() {
        let tree = Tree::new();
        for args in [
            &["custom-alias"][..],
            &["b"],
            &["clippy"],
            &["install"],
            &["--config", "another", "build"],
            &["build", "--config=include=['extra']"],
            &["build", "-Zunstable-options"],
            &["run", "-C", "elsewhere"],
            &["test", "--unsupported-option"],
            &["build", "unexpected-positional"],
            &["build", "--artifact-dir=elsewhere"],
            &["build", "--release=false"],
            &["build", "--manifest-path=missing/Cargo.toml"],
            &["build", "--manifest-path=elsewhere.toml"],
            &["build", "--target-dir"],
            &["build", "--target-dir=../out"],
            &["build", "--target-dir=out/{workspace-root}/../elsewhere"],
            &["build", "-p", "--target-dir"],
        ] {
            assert_eq!(tree.scope(args, &[], false), Scope::Everything, "{args:?}");
        }
        assert_eq!(tree.scope(&["check"], &[], true), Scope::Everything);
        let config = tree.home.join("config.toml");
        for text in [
            "include=['nested']",
            "[unstable]\nbuild-dir=true",
            "[build]\nartifact-dir='out'",
            "[env]\nCARGO_TARGET_DIR={value='x',force=true}",
            "[env]\ncargo_build_build_dir='elsewhere'",
            "[build]\nbuild-dir='{workspace-root}/build'",
            "[build]\ntarget-dir=2",
            "not a table[",
            "[unrecognized]\nplacement='other'",
        ] {
            fs::write(&config, text).unwrap();
            assert_eq!(
                tree.scope(&["run"], &[], false),
                Scope::Everything,
                "{text}"
            );
        }
        fs::remove_file(config).unwrap();
        for manifest in ["[package]\nworkspace='../../different'", "[missing"] {
            fs::write(tree.cwd.join("Cargo.toml"), manifest).unwrap();
            assert_eq!(tree.scope(&["build"], &[], false), Scope::Everything);
        }
    }

    #[cfg(unix)]
    #[test]
    fn redirected_targets_are_leased_and_broken_links_are_unknown() {
        use std::os::unix::{ffi::OsStrExt, fs::symlink};
        let tree = Tree::new();
        let alias = tree.root.join("member-link");
        symlink(&tree.cwd, &alias).unwrap();
        assert!(paths(&alias, &["build".into()], &[], &tree.home, false).is_none());
        let alias_manifest = alias.join("Cargo.toml");
        assert_eq!(
            tree.scope(
                &["build", "--manifest-path", alias_manifest.to_str().unwrap()],
                &[],
                false
            ),
            Scope::Everything
        );
        let outside = tree.root.join("other");
        fs::create_dir(&outside).unwrap();
        let link = tree.root.join("repo/target");
        symlink(&outside, &link).unwrap();
        let scope = tree.scope(&["test"], &[], false);
        assert!(scope.covers(&outside));
        assert!(!scope.covers(&tree.root.join("elsewhere")));
        fs::remove_dir(&outside).unwrap();
        assert_eq!(tree.scope(&["test"], &[], false), Scope::Everything);
        fs::remove_file(&link).unwrap();
        let bad = OsStr::from_bytes(b"non-utf8-\xff").to_owned();
        assert_eq!(
            tree.scope(&["build"], &[Some(bad.clone())], false),
            Scope::Everything
        );
        let args = [OsString::from("run"), OsString::from("--"), bad];
        assert!(paths(&tree.cwd, &args, &[], &tree.home, false).is_none());
        let config_dir = tree.cwd.join(".cargo");
        symlink(&tree.home, &config_dir).unwrap();
        fs::write(
            tree.home.join("config.toml"),
            "[build]\ntarget-dir='relative'",
        )
        .unwrap();
        assert_eq!(tree.scope(&["build"], &[], false), Scope::Everything);
        fs::remove_file(config_dir).unwrap();
        let manifest = tree.root.join("outside/Cargo.toml");
        fs::create_dir(manifest.parent().unwrap()).unwrap();
        fs::write(&manifest, "[workspace]").unwrap();
        fs::remove_file(tree.cwd.join("Cargo.toml")).unwrap();
        symlink(manifest, tree.cwd.join("Cargo.toml")).unwrap();
        assert_eq!(tree.scope(&["build"], &[], false), Scope::Everything);
    }
}
