use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

pub(super) struct Scope {
    pub workspace: Option<HashSet<String>>,
    pub pull: Option<HashSet<String>>,
}

#[derive(Deserialize)]
struct Metadata {
    packages: Vec<Package>,
    workspace_members: HashSet<String>,
    workspace_root: PathBuf,
}

#[derive(Deserialize)]
struct Package {
    id: String,
    targets: Vec<Target>,
}

#[derive(Deserialize)]
struct Target {
    name: String,
    kind: Vec<String>,
}

impl Metadata {
    fn crate_names(&self, workspace_only: bool) -> HashSet<String> {
        self.packages
            .iter()
            .filter(|package| !workspace_only || self.workspace_members.contains(&package.id))
            .flat_map(|package| &package.targets)
            .filter(|target| !target.kind.iter().any(|kind| kind == "custom-build"))
            .map(|target| target.name.replace('-', "_"))
            .collect()
    }
}

fn manifest_in_ancestors(directory: &Path) -> Option<PathBuf> {
    directory
        .ancestors()
        .map(|parent| parent.join("Cargo.toml"))
        .find(|path| path.is_file())
}

fn metadata(manifest: &Path, dependencies: bool) -> Result<Metadata> {
    let mut command = std::process::Command::new("cargo");
    command.args(["metadata", "--format-version", "1", "--locked"]);
    if dependencies {
        command.arg("--all-features");
    } else {
        command.arg("--no-deps");
    }
    let output = command
        .arg("--manifest-path")
        .arg(manifest)
        .output()
        .context("running cargo metadata")?;
    anyhow::ensure!(
        output.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    serde_json::from_slice(&output.stdout).context("parsing cargo metadata")
}

pub(super) fn resolve(manifest_path: Option<&str>, filtered_pull: bool) -> Result<Scope> {
    let cwd = std::env::current_dir().context("finding current directory for sync")?;
    resolve_at(manifest_path, &cwd, filtered_pull, metadata)
}

fn resolve_at(
    manifest_path: Option<&str>,
    cwd: &Path,
    filtered_pull: bool,
    read_metadata: impl Fn(&Path, bool) -> Result<Metadata>,
) -> Result<Scope> {
    let manifest = manifest_path
        .map(PathBuf::from)
        .or_else(|| manifest_in_ancestors(cwd));
    let Some(manifest) = manifest else {
        let pull = if filtered_pull {
            super::parse_cargo_lock_crate_names_from(&cwd.join("Cargo.lock"))
        } else {
            None
        };
        if pull.is_some() {
            eprintln!(
                "warning: Cargo.lock without Cargo.toml cannot resolve renamed library targets; \
                 using normalized package names. Use `kache sync --pull --all` for every remote crate."
            );
        }
        return Ok(Scope {
            workspace: None,
            pull,
        });
    };

    // Resolve the workspace without downloading dependencies. A broken manifest
    // must not turn a workspace-scoped push into an upload of the whole store.
    let workspace = read_metadata(&manifest, false)
        .with_context(|| format!("resolving sync workspace from {}", manifest.display()))?;
    let workspace_names = workspace.crate_names(true);
    let mut pull = if filtered_pull {
        super::parse_cargo_lock_crate_names_from(&workspace.workspace_root.join("Cargo.lock"))
    } else {
        None
    };
    if let Some(names) = &mut pull {
        names.extend(workspace_names.iter().cloned());
        match read_metadata(&manifest, true) {
            Ok(resolved) => names.extend(resolved.crate_names(false)),
            Err(error) => eprintln!(
                "warning: cannot resolve dependency target names: {error:#}\n\
                 warning: using normalized Cargo.lock names and workspace targets; renamed \
                 dependency libraries may be missed. Run `cargo fetch --locked` and retry, \
                 or use `kache sync --pull --all`."
            ),
        }
    }
    Ok(Scope {
        workspace: Some(workspace_names),
        pull,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workspace(root: &Path) -> Metadata {
        Metadata {
            workspace_root: root.to_owned(),
            workspace_members: HashSet::from(["member".into()]),
            packages: vec![Package {
                id: "member".into(),
                targets: vec![Target {
                    name: "custom_member".into(),
                    kind: vec!["lib".into()],
                }],
            }],
        }
    }

    #[test]
    fn failed_dependency_resolution_keeps_normalized_lock_and_workspace_targets() {
        let root = tempfile::tempdir().unwrap();
        let lock = root.path().join("Cargo.lock");
        let contents = "version = 3\n[[package]]\nname = \"dash-package\"\nversion = \"1.0.0\"\n";
        std::fs::write(&lock, contents).unwrap();
        let scope = resolve_at(Some("Cargo.toml"), root.path(), true, |_, dependencies| {
            if dependencies {
                anyhow::bail!("offline source unavailable");
            }
            Ok(workspace(root.path()))
        })
        .unwrap();
        assert_eq!(
            scope.workspace.unwrap(),
            HashSet::from(["custom_member".into()])
        );
        assert_eq!(
            scope.pull.unwrap(),
            HashSet::from(["dash_package".into(), "custom_member".into()])
        );
        assert_eq!(std::fs::read_to_string(lock).unwrap(), contents);
    }

    #[test]
    fn unfiltered_modes_and_missing_lockfiles_never_resolve_dependencies() {
        let root = tempfile::tempdir().unwrap();
        for filtered in [false, true] {
            let scope = resolve_at(
                Some("Cargo.toml"),
                root.path(),
                filtered,
                |_, dependencies| {
                    assert!(
                        !dependencies,
                        "unfiltered pulls and pushes must not fetch sources"
                    );
                    Ok(workspace(root.path()))
                },
            )
            .unwrap();
            assert_eq!(
                scope.workspace.unwrap(),
                HashSet::from(["custom_member".into()])
            );
            assert!(scope.pull.is_none());
        }
        std::fs::write(
            root.path().join("Cargo.lock"),
            "version = 3\npackage = []\n",
        )
        .unwrap();
        let scope = resolve_at(Some("Cargo.toml"), root.path(), false, |_, dependencies| {
            assert!(!dependencies);
            Ok(workspace(root.path()))
        })
        .unwrap();
        assert!(scope.pull.is_none());
        assert!(
            resolve_at(Some("Cargo.toml"), root.path(), true, |_, _| anyhow::bail!(
                "bad manifest"
            ))
            .is_err()
        );
    }

    #[test]
    fn dependency_targets_extend_the_lockfile_filter_without_broadening_push() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("Cargo.lock"),
            "version = 3\n[[package]]\nname = \"optional-package\"\nversion = \"1.0.0\"\n",
        )
        .unwrap();
        let scope = resolve_at(Some("Cargo.toml"), root.path(), true, |_, dependencies| {
            let mut result = workspace(root.path());
            if dependencies {
                result.packages.push(Package {
                    id: "external".into(),
                    targets: vec![Target {
                        name: "renamed_dependency".into(),
                        kind: vec!["lib".into()],
                    }],
                });
            }
            Ok(result)
        })
        .unwrap();
        assert_eq!(
            scope.workspace.unwrap(),
            HashSet::from(["custom_member".into()])
        );
        assert_eq!(
            scope.pull.unwrap(),
            HashSet::from([
                "custom_member".into(),
                "optional_package".into(),
                "renamed_dependency".into()
            ])
        );
    }

    #[test]
    fn standalone_lockfile_filters_only_default_pulls_without_running_cargo() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("Cargo.lock"),
            "version = 3\n[[package]]\nname = \"stand-alone\"\nversion = \"1.0.0\"\n",
        )
        .unwrap();
        for filtered in [false, true] {
            let scope = resolve_at(None, root.path(), filtered, |_, _| {
                panic!("a lockfile without a manifest must not invoke Cargo")
            })
            .unwrap();
            assert!(scope.workspace.is_none());
            assert_eq!(
                scope.pull,
                filtered.then(|| HashSet::from(["stand_alone".into()]))
            );
        }
        std::fs::remove_file(root.path().join("Cargo.lock")).unwrap();
        let scope = resolve_at(None, root.path(), true, |_, _| panic!("no Cargo project")).unwrap();
        assert!(scope.workspace.is_none());
        assert!(scope.pull.is_none());
    }

    #[test]
    fn targets_use_opaque_package_ids_and_exclude_build_scripts() {
        let metadata: Metadata = serde_json::from_value(serde_json::json!({
            "workspace_root": ".",
            "workspace_members": ["workspace-id"],
            "packages": [
                {"id":"workspace-id", "targets":[
                    {"name":"tool-app", "kind":["bin"]},
                    {"name":"custom_library", "kind":["rlib"]},
                    {"name":"build-script-build", "kind":["custom-build"]}
                ]},
                {"id":"dependency-id", "targets":[
                    {"name":"renamed_dependency", "kind":["proc-macro"]},
                    {"name":"custom-build", "kind":["lib"]}
                ]},
                {"id":"other-version-id", "targets":[
                    {"name":"renamed_dependency", "kind":["lib"]}
                ]}
            ]
        }))
        .unwrap();
        assert_eq!(
            metadata.crate_names(true),
            HashSet::from(["tool_app".into(), "custom_library".into()])
        );
        assert_eq!(
            metadata.crate_names(false),
            HashSet::from([
                "tool_app".into(),
                "custom_library".into(),
                "renamed_dependency".into(),
                "custom_build".into(),
            ])
        );
    }

    #[test]
    fn manifest_discovery_uses_the_nearest_ancestor() {
        let root = tempfile::tempdir().unwrap();
        let member = root.path().join("member");
        let nested = member.join("src");
        std::fs::create_dir_all(&nested).unwrap();
        assert!(manifest_in_ancestors(&nested).is_none());
        std::fs::write(root.path().join("Cargo.toml"), "").unwrap();
        assert_eq!(
            manifest_in_ancestors(&nested),
            Some(root.path().join("Cargo.toml"))
        );
        std::fs::write(member.join("Cargo.toml"), "").unwrap();
        assert_eq!(
            manifest_in_ancestors(&nested),
            Some(member.join("Cargo.toml"))
        );
    }
}
