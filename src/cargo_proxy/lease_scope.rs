//! Which directories a Cargo command may build in, for its target lease.
//!
//! Cargo puts a workspace's output in `<workspace root>/target` unless a
//! `--target-dir`, a `CARGO_*_DIR` variable or a config `build.target-dir`
//! or `build.build-dir` moves it. The workspace root is the manifest's own
//! directory or an ancestor, so the topmost ancestor with a `Cargo.toml`
//! contains it. Leasing that ancestor and every directory the command moves
//! output to keeps every target the command can touch. Anything this cannot
//! read for certain (`-C`, a `--config` that may move output, a config
//! `include`, a template it does not know) leases everything.

use super::*;
use crate::target_use::Scope;

const DIR_VARIABLES: [&str; 3] = [
    "CARGO_TARGET_DIR",
    "CARGO_BUILD_TARGET_DIR",
    "CARGO_BUILD_BUILD_DIR",
];

/// The lease scope of Cargo run with `args` in `cwd`.
pub(super) fn of(cwd: &Path, args: &[OsString], env: &crate::invocation::InvocationEnv) -> Scope {
    let cargo_home = match env.var_os("CARGO_HOME").filter(|home| !home.is_empty()) {
        Some(home) => Some(cwd.join(home)),
        None => cargo_home(cwd).ok(),
    };
    let variables = DIR_VARIABLES.map(|name| env.var_os(name));
    cargo_home
        .and_then(|home| paths(cwd, args, &variables, &home))
        .map_or(Scope::Everything, Scope::within)
}

fn paths(
    cwd: &Path,
    args: &[OsString],
    variables: &[Option<OsString>],
    cargo_home: &Path,
) -> Option<Vec<PathBuf>> {
    let mut paths = Vec::new();
    let mut base = cwd.to_path_buf();
    let (_, args) = split_toolchain_selector(args);
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        let text = arg.to_string_lossy();
        if text == "--" {
            break;
        }
        if let Some(dir) = value_of(&text, "--target-dir", &mut args) {
            paths.push(cwd.join(dir));
        } else if let Some(manifest) = value_of(&text, "--manifest-path", &mut args) {
            base = cwd.join(manifest).parent()?.to_path_buf();
        } else if let Some(config) = value_of(&text, "--config", &mut args) {
            // A file, or a value that may move output, is more than this reads.
            let config = config.to_string_lossy();
            if !config.contains('=') || config.contains("-dir") {
                return None;
            }
        } else if text.starts_with("-C") {
            return None;
        }
    }
    for value in variables.iter().flatten().filter(|value| !value.is_empty()) {
        paths.extend(templated(cwd, Path::new(value), cargo_home)?);
    }
    for (config, dir) in config_dirs(cwd, cargo_home)? {
        // Cargo reads a config path relative to the directory holding `.cargo`.
        let relative_to = config.parent().and_then(Path::parent).unwrap_or(cwd);
        paths.extend(templated(relative_to, Path::new(&dir), cargo_home)?);
    }
    paths.extend(workspace_roots(&base));
    Some(paths)
}

/// The value of `--name value` or `--name=value`, taking the next argument
/// for the first form.
fn value_of<'a>(
    arg: &str,
    name: &str,
    rest: &mut impl Iterator<Item = &'a OsString>,
) -> Option<OsString> {
    if arg == name {
        return rest.next().cloned();
    }
    arg.strip_prefix(name)?
        .strip_prefix('=')
        .map(OsString::from)
}

/// `path` relative to `base`, cut before the first `{...}` template Cargo
/// fills in: no path for `{workspace-root}`, which lies inside the leased
/// workspace; Cargo's home for `{cargo-cache-home}`; `None` for a template
/// this does not know.
fn templated(base: &Path, path: &Path, cargo_home: &Path) -> Option<Vec<PathBuf>> {
    let mut resolved = PathBuf::new();
    for (index, component) in path.components().enumerate() {
        let text = component.as_os_str().to_string_lossy();
        if !text.contains('{') {
            resolved.push(component);
            continue;
        }
        return match (index, text.as_ref()) {
            (0, "{workspace-root}") => Some(Vec::new()),
            (0, "{cargo-cache-home}") => Some(vec![cargo_home.to_path_buf()]),
            (0, _) => None,
            _ => Some(vec![base.join(resolved)]),
        };
    }
    Some(vec![base.join(resolved)])
}

/// Each config file that sets `build.target-dir` or `build.build-dir`, with
/// the value; `None` when a config cannot be read for certain.
fn config_dirs(cwd: &Path, cargo_home: &Path) -> Option<Vec<(PathBuf, String)>> {
    let mut dirs = Vec::new();
    for (path, _) in cargo_config_candidates(cwd, cargo_home) {
        let text = std::fs::read_to_string(&path).ok()?;
        let table: toml::Table = toml::from_str(&text).ok()?;
        if table.contains_key("include") {
            return None;
        }
        let Some(build) = table.get("build") else {
            continue;
        };
        let build = build.as_table()?;
        for key in ["target-dir", "build-dir"] {
            if let Some(value) = build.get(key) {
                dirs.push((path.clone(), value.as_str()?.to_owned()));
            }
        }
    }
    Some(dirs)
}

/// The topmost ancestor of `base` with a `Cargo.toml`, which contains the
/// workspace root, and the nearest manifest's `package.workspace` when it
/// names one elsewhere. `base` itself when no ancestor has a manifest.
fn workspace_roots(base: &Path) -> Vec<PathBuf> {
    let manifests: Vec<&Path> = base
        .ancestors()
        .filter(|dir| dir.join("Cargo.toml").is_file())
        .collect();
    let mut roots = vec![manifests.last().copied().unwrap_or(base).to_path_buf()];
    if let Some(nearest) = manifests.first() {
        let named = std::fs::read_to_string(nearest.join("Cargo.toml"))
            .ok()
            .and_then(|text| toml::from_str::<toml::Table>(&text).ok())
            .and_then(|manifest| {
                manifest
                    .get("package")?
                    .get("workspace")?
                    .as_str()
                    .map(|workspace| nearest.join(workspace))
            });
        roots.extend(named);
    }
    roots
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Tree {
        _dir: tempfile::TempDir,
        root: PathBuf,
    }

    impl Tree {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().canonicalize().unwrap();
            Self { _dir: dir, root }
        }

        fn write(&self, path: &str, text: &str) {
            let path = self.root.join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, text).unwrap();
        }

        fn at(&self, path: &str) -> PathBuf {
            self.root.join(path)
        }

        fn paths(
            &self,
            cwd: &str,
            args: &[&str],
            variables: &[(usize, &str)],
        ) -> Option<Vec<PathBuf>> {
            let args: Vec<OsString> = args.iter().map(OsString::from).collect();
            let mut values = vec![None, None, None];
            for (index, value) in variables {
                values[*index] = Some(OsString::from(value));
            }
            paths(&self.at(cwd), &args, &values, &self.at("home/.cargo"))
        }
    }

    #[test]
    fn a_workspace_leases_its_topmost_manifest_directory() {
        let tree = Tree::new();
        tree.write("repo/Cargo.toml", "[workspace]\n");
        tree.write("repo/crates/a/Cargo.toml", "[package]\nname = \"a\"\n");
        assert_eq!(
            tree.paths("repo/crates/a", &["build"], &[]),
            Some(vec![tree.at("repo")])
        );
        assert_eq!(
            tree.paths("repo/crates", &["test"], &[]),
            Some(vec![tree.at("repo")])
        );
        // No manifest anywhere: the directory Cargo runs in.
        assert_eq!(
            tree.paths("elsewhere", &["new", "x"], &[]),
            Some(vec![tree.at("elsewhere")])
        );
    }

    #[test]
    fn a_manifest_path_and_a_named_workspace_move_the_root() {
        let tree = Tree::new();
        tree.write(
            "one/Cargo.toml",
            "[package]\nname = \"one\"\nworkspace = \"../ws\"\n",
        );
        tree.write("two/Cargo.toml", "[package]\nname = \"two\"\n");
        assert_eq!(
            tree.paths("", &["build", "--manifest-path", "two/Cargo.toml"], &[]),
            Some(vec![tree.at("two")])
        );
        assert_eq!(
            tree.paths("", &["build", "--manifest-path=one/Cargo.toml"], &[]),
            Some(vec![tree.at("one"), tree.at("one/../ws")])
        );
    }

    #[cfg(unix)]
    #[test]
    fn target_and_build_directories_join_the_lease() {
        let tree = Tree::new();
        tree.write("repo/Cargo.toml", "[package]\nname = \"r\"\n");
        tree.write("repo/.cargo/config.toml", "[build]\ntarget-dir = \"out\"\n");
        tree.write(
            "home/.cargo/config.toml",
            "[build]\nbuild-dir = \"{cargo-cache-home}/build/{workspace-path-hash}\"\n",
        );
        assert_eq!(
            tree.paths(
                "repo",
                &[
                    "+nightly",
                    "build",
                    "--target-dir",
                    "/abs/t",
                    "--target-dir=rel",
                    "--",
                    "--target-dir",
                    "x"
                ],
                &[(0, "env-target"), (2, "{workspace-root}/b")]
            ),
            Some(vec![
                PathBuf::from("/abs/t"),
                tree.at("repo/rel"),
                tree.at("repo/env-target"),
                tree.at("home/.cargo"),
                tree.at("repo/out"),
                tree.at("repo"),
            ])
        );
    }

    #[test]
    fn what_cannot_be_read_for_certain_leases_everything() {
        let tree = Tree::new();
        tree.write("repo/Cargo.toml", "[package]\nname = \"r\"\n");
        for args in [
            &["-C", "elsewhere", "build"][..],
            &["-Celsewhere", "build"],
            &["build", "--config", "extra.toml"],
            &["build", "--config=build.target-dir=\"x\""],
        ] {
            assert_eq!(tree.paths("repo", args, &[]), None, "{args:?}");
        }
        assert!(
            tree.paths("repo", &["build", "--config", "profile.dev.debug=0"], &[])
                .is_some()
        );
        assert_eq!(tree.paths("repo", &["build"], &[(1, "{unknown}/x")]), None);
        tree.write("repo/.cargo/config.toml", "include = [\"other.toml\"]\n");
        assert_eq!(tree.paths("repo", &["build"], &[]), None);
        tree.write("repo/.cargo/config.toml", "[build]\ntarget-dir = 5\n");
        assert_eq!(tree.paths("repo", &["build"], &[]), None);
        tree.write("repo/.cargo/config.toml", "not toml [");
        assert_eq!(tree.paths("repo", &["build"], &[]), None);
    }

    #[cfg(unix)]
    #[test]
    fn templates_cut_the_path_where_cargo_fills_it_in() {
        let base = Path::new("/base");
        let home = Path::new("/home/.cargo");
        let one = |path: &str| Some(vec![PathBuf::from(path)]);
        assert_eq!(templated(base, Path::new("t"), home), one("/base/t"));
        assert_eq!(templated(base, Path::new("/abs/t"), home), one("/abs/t"));
        assert_eq!(
            templated(base, Path::new("out/{workspace-path-hash}/x"), home),
            one("/base/out")
        );
        assert_eq!(
            templated(base, Path::new("{cargo-cache-home}/build"), home),
            one("/home/.cargo")
        );
        assert_eq!(
            templated(base, Path::new("{workspace-root}/t"), home),
            Some(Vec::new())
        );
        assert_eq!(templated(base, Path::new("{other}/t"), home), None);
    }
}
