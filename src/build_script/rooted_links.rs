//! OUT_DIR links to a checked location in the current run's source roots.
//!
//! Store a root identifier and a relative suffix, rather than feeding a link
//! target through text replacement. No manifest can introduce a new root.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::{Environment, Manifest, checked_relative};

pub(super) const MANIFEST_VERSION: u32 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum Root {
    OutDir,
    Package,
    RegistrySource,
    GitCheckout,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(super) struct RootedSymlink {
    name: String,
    root: Root,
    /// Empty means the root itself; otherwise only normal path components.
    suffix: String,
}

/// Existing, canonical directories, including the target directory as a
/// denied root. The innermost match wins, so OUT_DIR remains usable below
/// target, while target below the package or Cargo home remains denied.
fn roots(environment: &Environment) -> Vec<(PathBuf, Option<Root>)> {
    let mut roots = vec![
        (environment.out_dir.clone(), Some(Root::OutDir)),
        (environment.manifest_dir.clone(), Some(Root::Package)),
        // The package walk excludes this directory even when this run uses
        // another target directory or an unrecognized Cargo layout.
        (environment.manifest_dir.join("target"), None),
    ];
    roots.extend(
        environment
            .target_roots()
            .map(|path| (path.to_path_buf(), None)),
    );
    for (path, placeholder) in &environment.spellings {
        if *placeholder == "${KACHE_CARGO_HOME}" {
            roots.push((path.join("registry/src"), Some(Root::RegistrySource)));
            roots.push((path.join("git/checkouts"), Some(Root::GitCheckout)));
        }
    }
    let mut roots: Vec<_> = roots
        .into_iter()
        .filter(|(path, _)| path.is_absolute())
        .filter_map(|(path, kind)| {
            let canonical = path.canonicalize().ok()?;
            canonical.is_dir().then_some((canonical, kind))
        })
        .collect();
    // A denied root wins a tie with an allowed root.
    roots
        .sort_by_key(|(path, kind)| (std::cmp::Reverse(path.components().count()), kind.is_some()));
    roots
}

fn classify(environment: &Environment, target: &Path) -> Result<(Root, String)> {
    let target = target
        .canonicalize()
        .context("OUT_DIR symlink target is missing or unreadable")?;
    for (root, kind) in roots(environment) {
        if let Ok(suffix) = target.strip_prefix(&root) {
            let kind = kind.context("OUT_DIR symlink target is in the target directory")?;
            let suffix = suffix
                .to_str()
                .context("OUT_DIR symlink target is not UTF-8")?
                .replace(std::path::MAIN_SEPARATOR, "/");
            checked_suffix(&suffix)?;
            return Ok((kind, suffix));
        }
    }
    anyhow::bail!("OUT_DIR symlink target is outside the permitted roots")
}

fn checked_suffix(suffix: &str) -> Result<&Path> {
    if suffix.is_empty() {
        Ok(Path::new(""))
    } else {
        checked_relative(suffix)
    }
}

pub(super) fn record(
    environment: &Environment,
    path: &Path,
    name: String,
) -> Result<RootedSymlink> {
    checked_relative(&name)?;
    // Resolve the actual link before classifying it. A package link through
    // a source-directory symlink must not hide an unrelated external file.
    let (root, suffix) = classify(environment, path)?;
    Ok(RootedSymlink { name, root, suffix })
}

/// A relative target can traverse another link in OUT_DIR. For a dangling
/// target, resolve its nearest existing ancestor: only a target still inside
/// OUT_DIR may retain the old relative-link behavior.
pub(super) fn relative_target_is_internal(
    environment: &Environment,
    path: &Path,
    target: &Path,
) -> Result<bool> {
    let out_dir = environment.out_dir.canonicalize()?;
    let mut target = path
        .parent()
        .context("OUT_DIR link has no parent")?
        .join(target);
    loop {
        match target.canonicalize() {
            Ok(resolved) => return Ok(resolved.starts_with(&out_dir)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                anyhow::ensure!(target.pop(), "OUT_DIR link target has no existing ancestor");
            }
            Err(error) => return Err(error.into()),
        }
    }
}

pub(super) fn validate_restored_relative(
    environment: &Environment,
    links: &[super::Symlink],
) -> Result<()> {
    for link in links {
        let path = environment.out_dir.join(checked_relative(&link.name)?);
        if !relative_target_is_internal(environment, &path, Path::new(&link.target))? {
            // This can happen in a tampered manifest that combines an
            // internal-looking link with a rooted source-directory link.
            record(environment, &path, link.name.clone())?;
        }
    }
    Ok(())
}

pub(super) fn validate_manifest(manifest: &Manifest) -> Result<()> {
    anyhow::ensure!(
        manifest.rooted_symlinks.is_empty() || manifest.version == MANIFEST_VERSION,
        "rooted build-script links require manifest version 5"
    );
    for link in &manifest.rooted_symlinks {
        checked_relative(&link.name)?;
        checked_suffix(&link.suffix)?;
    }
    Ok(())
}

pub(super) fn restore(environment: &Environment, link: &RootedSymlink) -> Result<()> {
    let name = checked_relative(&link.name)?;
    let suffix = checked_suffix(&link.suffix)?;
    let root = roots(environment)
        .into_iter()
        .find_map(|(path, kind)| (kind == Some(link.root)).then_some(path))
        .context("recorded build-script symlink root is unavailable")?;
    let target = root.join(suffix);
    // Re-resolve both roots and target for this checkout. A valid suffix may
    // now pass through a symlink, or be below a more specific denied root.
    let (kind, resolved_suffix) = classify(environment, &target)?;
    anyhow::ensure!(
        kind == link.root && resolved_suffix == link.suffix,
        "recorded build-script symlink target changed roots"
    );
    let mut directory = environment.out_dir.clone();
    for component in name.parent().into_iter().flat_map(Path::components) {
        directory.push(component);
        anyhow::ensure!(
            std::fs::symlink_metadata(&directory).is_ok_and(|metadata| metadata.is_dir()),
            "recorded rooted symlink is not under a real directory of OUT_DIR"
        );
    }
    let path = environment.out_dir.join(name);
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(target, path)?;
        Ok(())
    }
    #[cfg(not(unix))]
    anyhow::bail!(
        "build-script symlinks are only restored on Unix: {}",
        path.display()
    )
}

#[cfg(all(test, unix))]
mod tests {
    use super::super::TARGET_DIR_PLACEHOLDER;
    use super::*;
    use std::os::unix::fs::symlink;

    fn environment(root: &Path) -> Environment {
        let package = root.join("package");
        let target = package.join("target");
        let out = target.join("debug/build/demo-1/out");
        let cargo = root.join("cargo");
        for path in [
            &out,
            &cargo.join("registry/src"),
            &cargo.join("git/checkouts"),
        ] {
            std::fs::create_dir_all(path).unwrap();
        }
        Environment {
            out_dir: out.clone(),
            manifest_dir: package.clone(),
            mappings: vec![
                (out.clone(), "${KACHE_OUT_DIR}"),
                (target.clone(), TARGET_DIR_PLACEHOLDER),
                (package.clone(), "${KACHE_MANIFEST_DIR}"),
                (cargo.clone(), "${KACHE_CARGO_HOME}"),
            ],
            spellings: vec![
                (out, "${KACHE_OUT_DIR}"),
                (target, TARGET_DIR_PLACEHOLDER),
                (package, "${KACHE_MANIFEST_DIR}"),
                (cargo, "${KACHE_CARGO_HOME}"),
            ],
            base_dirs: Vec::new(),
            dependency_roots: Vec::new(),
            target: None,
            ownership_roots: Vec::new(),
        }
    }

    #[test]
    fn the_innermost_root_selects_sources_and_refuses_target_outputs() {
        let directory = tempfile::tempdir().unwrap();
        let env = environment(directory.path());
        let header = directory
            .path()
            .join("cargo/registry/src/cxx/include/cxx.h");
        std::fs::create_dir_all(header.parent().unwrap()).unwrap();
        std::fs::write(&header, "header").unwrap();
        assert_eq!(
            classify(&env, &header).unwrap(),
            (Root::RegistrySource, "cxx/include/cxx.h".into())
        );
        assert_eq!(
            classify(&env, &env.manifest_dir).unwrap(),
            (Root::Package, String::new())
        );
        std::fs::write(env.out_dir.join("inside"), "generated").unwrap();
        assert_eq!(
            classify(&env, &env.out_dir.join("inside")).unwrap(),
            (Root::OutDir, "inside".into())
        );
        let git = directory.path().join("cargo/git/checkouts/repo/hash");
        std::fs::create_dir_all(&git).unwrap();
        assert_eq!(
            classify(&env, &git).unwrap(),
            (Root::GitCheckout, "repo/hash".into())
        );
        for target in [
            env.manifest_dir.join("target"),
            env.manifest_dir.join("target/debug/build/other-1/out"),
        ] {
            std::fs::create_dir_all(&target).unwrap();
            assert!(classify(&env, &target).is_err(), "{}", target.display());
        }
        assert!(classify(&env, directory.path()).is_err(), "an unknown root");
        assert!(
            classify(&env, &env.manifest_dir.join("missing")).is_err(),
            "a missing target"
        );
        // Cargo home is not itself a source root, and target can also be
        // nested under a registry source directory.
        let mut nested = env.clone();
        let target = directory.path().join("cargo/registry/src/cxx/target");
        std::fs::create_dir_all(&target).unwrap();
        nested
            .spellings
            .push((target.clone(), TARGET_DIR_PLACEHOLDER));
        nested
            .mappings
            .push((target.clone(), TARGET_DIR_PLACEHOLDER));
        assert!(classify(&nested, &target).is_err());
        let owned = directory
            .path()
            .join("cargo/registry/src/cxx/canonical-target");
        std::fs::create_dir_all(&owned).unwrap();
        nested.ownership_roots.push(owned.clone());
        assert!(
            classify(&nested, &owned).is_err(),
            "an ownership-only target view is not a source root"
        );
        assert!(classify(&env, &directory.path().join("cargo")).is_err());
    }

    #[test]
    fn canonical_targets_cannot_hide_an_escape_or_a_more_specific_root() {
        let directory = tempfile::tempdir().unwrap();
        let env = environment(directory.path());
        let external = directory.path().join("external");
        std::fs::write(&external, "not a source").unwrap();
        symlink(&external, env.manifest_dir.join("escape")).unwrap();
        symlink(env.manifest_dir.join("escape"), env.out_dir.join("bad")).unwrap();
        assert!(record(&env, &env.out_dir.join("bad"), "bad".into()).is_err());
        symlink(
            env.manifest_dir.join("target"),
            env.manifest_dir.join("target-alias"),
        )
        .unwrap();
        assert!(classify(&env, &env.manifest_dir.join("target-alias")).is_err());
        let alias = directory.path().join("package-alias");
        symlink(&env.manifest_dir, &alias).unwrap();
        let mut spelled = env.clone();
        spelled.manifest_dir = alias.clone();
        assert_eq!(
            classify(&spelled, &alias).unwrap(),
            (Root::Package, String::new())
        );
    }

    #[test]
    fn the_default_package_target_stays_denied_with_another_or_unknown_layout() {
        let directory = tempfile::tempdir().unwrap();
        for known_layout in [true, false] {
            let mut env = environment(directory.path());
            let active = directory.path().join("active-target");
            env.out_dir = active.join("debug/build/demo-1/out");
            std::fs::create_dir_all(&env.out_dir).unwrap();
            env.spellings
                .retain(|(_, placeholder)| *placeholder != TARGET_DIR_PLACEHOLDER);
            env.mappings
                .retain(|(_, placeholder)| *placeholder != TARGET_DIR_PLACEHOLDER);
            if known_layout {
                env.spellings.push((active.clone(), TARGET_DIR_PLACEHOLDER));
                env.mappings.push((active, TARGET_DIR_PLACEHOLDER));
            }
            let stale = env.manifest_dir.join("target/stale.h");
            std::fs::write(&stale, "stale").unwrap();
            assert!(classify(&env, &stale).is_err());
            assert!(
                restore(
                    &env,
                    &RootedSymlink {
                        name: "stale".into(),
                        root: Root::Package,
                        suffix: "target/stale.h".into(),
                    },
                )
                .is_err()
            );
            std::fs::write(env.out_dir.join("own.h"), "generated").unwrap();
            assert_eq!(
                classify(&env, &env.out_dir.join("own.h")).unwrap(),
                (Root::OutDir, "own.h".into())
            );
        }
    }

    #[test]
    fn restore_rechecks_suffixes_roots_and_the_link_parent() {
        let directory = tempfile::tempdir().unwrap();
        let env = environment(directory.path());
        let link = |name: &str, root: Root, suffix: &str| RootedSymlink {
            name: name.into(),
            root,
            suffix: suffix.into(),
        };
        for suffix in ["../external", "/absolute", "header/../other"] {
            assert!(
                restore(&env, &link("bad", Root::Package, suffix)).is_err(),
                "{suffix}"
            );
        }
        assert!(restore(&env, &link("../bad", Root::Package, "")).is_err());
        assert!(restore(&env, &link("missing", Root::Package, "absent")).is_err());
        let mut absent_root = env.clone();
        absent_root
            .spellings
            .retain(|(_, placeholder)| *placeholder != "${KACHE_CARGO_HOME}");
        assert!(
            restore(
                &absent_root,
                &link("missing-root", Root::RegistrySource, "")
            )
            .is_err()
        );
        let outside = directory.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        symlink(&outside, env.manifest_dir.join("redirect")).unwrap();
        assert!(restore(&env, &link("escape", Root::Package, "redirect")).is_err());
        assert!(restore(&env, &link("target", Root::Package, "target")).is_err());
        symlink(&outside, env.out_dir.join("parent")).unwrap();
        assert!(restore(&env, &link("parent/link", Root::Package, "")).is_err());
        assert!(!outside.join("link").exists());
        assert!(restore(&env, &link("not-created/link", Root::Package, "")).is_err());
        std::fs::create_dir(env.out_dir.join("real")).unwrap();
        restore(&env, &link("real/package", Root::Package, "")).unwrap();
        assert_eq!(
            std::fs::read_link(env.out_dir.join("real/package")).unwrap(),
            env.manifest_dir.canonicalize().unwrap()
        );
    }

    #[test]
    fn rooted_links_share_the_file_and_link_entry_budget_without_following_directories() {
        let directory = tempfile::tempdir().unwrap();
        let env = environment(directory.path());
        std::fs::write(env.out_dir.join("generated.h"), "generated").unwrap();
        symlink("generated.h", env.out_dir.join("relative.h")).unwrap();
        symlink(&env.manifest_dir, env.out_dir.join("package")).unwrap();
        let contents = super::super::collect_out_dir_with_environment(&env, 3).unwrap();
        assert_eq!(contents.files.len(), 1);
        assert_eq!(contents.symlinks.len(), 1);
        assert_eq!(contents.rooted_symlinks.len(), 1);
        assert!(
            contents.directories.is_empty(),
            "the package link was not walked"
        );
        assert!(super::super::collect_out_dir_with_environment(&env, 2).is_err());
        assert!(super::super::collect_out_dir_with_environment(&env, 0).is_err());
    }

    #[test]
    fn relative_links_cannot_hide_behind_a_rooted_directory_even_when_dangling() {
        let directory = tempfile::tempdir().unwrap();
        let env = environment(directory.path());
        symlink(&env.manifest_dir, env.out_dir.join("crate")).unwrap();
        let link = env.out_dir.join("header");
        for target in [
            "crate/target",
            "crate/target/not-created",
            "crate/not-created/nested/header",
        ] {
            symlink(target, &link).unwrap();
            assert!(!relative_target_is_internal(&env, &link, Path::new(target)).unwrap());
            assert!(
                super::super::collect_out_dir_with_environment(&env, 10).is_err(),
                "{target}"
            );
            std::fs::remove_file(&link).unwrap();
        }
        symlink("not-created/nested/header", &link).unwrap();
        assert!(
            relative_target_is_internal(&env, &link, Path::new("not-created/nested/header"))
                .unwrap()
        );
        let contents = super::super::collect_out_dir_with_environment(&env, 2).unwrap();
        assert_eq!(
            contents.symlinks.len(),
            1,
            "ordinary dangling relative links keep their text"
        );
        assert_eq!(contents.rooted_symlinks.len(), 1);
        assert_eq!(contents.symlinks[0].target, "not-created/nested/header");
    }

    #[test]
    fn relative_targets_through_regular_files_are_not_dangling_internal_links() {
        let directory = tempfile::tempdir().unwrap();
        let env = environment(directory.path());
        std::fs::write(env.out_dir.join("regular-file"), "not a directory").unwrap();
        let path = env.out_dir.join("blocked");
        let target = Path::new("regular-file/child");
        symlink(target, &path).unwrap();
        assert!(relative_target_is_internal(&env, &path, target).is_err());
        assert!(super::super::collect_out_dir_with_environment(&env, 2).is_err());
    }

    #[test]
    fn schema_rejects_unknown_roots_and_older_manifest_versions_with_rooted_links() {
        let unknown = r#"{"name":"h","root":"arbitrary","suffix":"h"}"#;
        assert!(serde_json::from_str::<RootedSymlink>(unknown).is_err());
        for version in 1..=5 {
            let plain = format!(
                r#"{{"version":{version},"directories":[],"empty_files":[],"stdout":"","stderr":""}}"#
            );
            let mut manifest: Manifest = serde_json::from_str(&plain).unwrap();
            assert!(manifest.rooted_symlinks.is_empty());
            validate_manifest(&manifest).unwrap();
            manifest.rooted_symlinks.push(RootedSymlink {
                name: "h".into(),
                root: Root::Package,
                suffix: String::new(),
            });
            assert_eq!(validate_manifest(&manifest).is_ok(), version == 5);
        }
        let mut manifest: Manifest = serde_json::from_str(
            r#"{"version":5,"directories":[],"empty_files":[],"stdout":"","stderr":""}"#,
        )
        .unwrap();
        manifest.rooted_symlinks.push(RootedSymlink {
            name: "../bad".into(),
            root: Root::Package,
            suffix: String::new(),
        });
        assert!(validate_manifest(&manifest).is_err());
        manifest.rooted_symlinks[0].name = "good".into();
        manifest.rooted_symlinks[0].suffix = "../bad".into();
        assert!(validate_manifest(&manifest).is_err());
    }
}
