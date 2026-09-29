//! Find Cargo targets in sibling Git worktrees of workspaces kache has built.
//!
//! This runs during quiet daemon maintenance, never in a compiler wrapper.
//! Git supplies candidate worktrees; Cargo markers and a manifest still have
//! to identify a target before it is recorded. Discovery does not claim a
//! build used the target or make it eligible for automatic cleanup.

use crate::store::Store;
use anyhow::Result;
use std::collections::HashSet;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const GIT_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_INVENTORY_BYTES: u64 = 1 << 20;

#[derive(Debug, PartialEq, Eq)]
struct Worktree {
    path: PathBuf,
    bare: bool,
    locked: bool,
    prunable: bool,
}

/// Parse Git's NUL-delimited porcelain records. An unknown attribute does not
/// change the meaning of the attributes used for cleanup decisions.
fn parse_worktrees(bytes: &[u8]) -> Vec<Worktree> {
    let mut trees = Vec::new();
    let mut current: Option<Worktree> = None;
    for field in bytes.split(|byte| *byte == 0) {
        if field.is_empty() {
            if let Some(tree) = current.take() {
                trees.push(tree);
            }
            continue;
        }
        if let Some(path) = field.strip_prefix(b"worktree ") {
            if let Some(tree) = current.take() {
                trees.push(tree);
            }
            #[cfg(unix)]
            let path = {
                use std::os::unix::ffi::OsStringExt;
                Some(PathBuf::from(std::ffi::OsString::from_vec(path.to_vec())))
            };
            #[cfg(not(unix))]
            let path = String::from_utf8(path.to_vec()).ok().map(PathBuf::from);
            current = path.map(|path| Worktree {
                path,
                bare: false,
                locked: false,
                prunable: false,
            });
        } else if let Some(tree) = current.as_mut() {
            tree.bare |= field == b"bare";
            tree.locked |= field == b"locked" || field.starts_with(b"locked ");
            tree.prunable |= field == b"prunable" || field.starts_with(b"prunable ");
        }
    }
    if let Some(tree) = current {
        trees.push(tree);
    }
    trees
}

fn inventory(workspace: &Path) -> Option<Vec<Worktree>> {
    let mut child = Command::new("git")
        .arg("-C")
        .arg(workspace)
        .args(["worktree", "list", "--porcelain", "-z"])
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_COMMON_DIR")
        .env_remove("GIT_INDEX_FILE")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let stdout = child.stdout.take()?;
    let reader = match std::thread::Builder::new()
        .name("kache-git-worktrees".into())
        .spawn(move || {
            let mut bytes = Vec::new();
            stdout
                .take(MAX_INVENTORY_BYTES)
                .read_to_end(&mut bytes)
                .ok()?;
            Some(bytes)
        }) {
        Ok(reader) => reader,
        Err(_) => {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
    };
    let deadline = Instant::now() + GIT_TIMEOUT;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) => {
                if deadline.checked_duration_since(Instant::now()).is_none() {
                    break None;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(_) => break None,
        }
    };
    if status.is_none() {
        let _ = child.kill();
        let _ = child.wait();
    }
    let bytes = reader.join().ok().flatten()?;
    if !status?.success() {
        return None;
    }
    if bytes.len() as u64 == MAX_INVENTORY_BYTES {
        return None;
    }
    Some(
        parse_worktrees(&bytes)
            .into_iter()
            .filter_map(|mut tree| {
                tree.path = tree.path.canonicalize().ok()?;
                Some(tree)
            })
            .collect(),
    )
}

/// Map the source workspace's path relative to its Git worktree into each
/// sibling. A repository may contain more than one Cargo workspace.
fn sibling_workspaces(seed: &Path, trees: &[Worktree]) -> Vec<PathBuf> {
    let Some(source) = trees
        .iter()
        .filter(|tree| seed.starts_with(&tree.path))
        .max_by_key(|tree| tree.path.components().count())
    else {
        return Vec::new();
    };
    let Ok(suffix) = seed.strip_prefix(&source.path) else {
        return Vec::new();
    };
    trees
        .iter()
        .filter(|tree| !tree.bare && !tree.locked && !tree.prunable)
        .map(|tree| tree.path.join(suffix))
        .filter(|workspace| {
            std::fs::symlink_metadata(workspace).is_ok_and(|meta| meta.file_type().is_dir())
                && workspace.join("Cargo.toml").is_file()
        })
        .collect()
}

/// Discover default targets in sibling worktrees. Custom target paths are
/// known only when a wrapper has already recorded them in the target registry.
pub(crate) fn discover(store: &Store) -> Result<usize> {
    let tracked = store.tracked_target_roots(0)?;
    let known: HashSet<_> = tracked.iter().map(|root| root.identity).collect();
    let mut covered = HashSet::new();
    let mut found = 0;
    for root in tracked.iter().filter(|root| !root.discovered) {
        let Ok(seed) = root.workspace_root.canonicalize() else {
            continue;
        };
        if covered.contains(&seed) {
            continue;
        }
        let Some(trees) = inventory(&seed) else {
            continue;
        };
        for workspace in sibling_workspaces(&seed, &trees) {
            covered.insert(workspace.clone());
            let target = workspace.join("target");
            if crate::machine::directory_identity(&target).is_some_and(|id| known.contains(&id)) {
                continue;
            }
            found += usize::from(store.remember_discovered_target_root(&target, &workspace)?);
        }
    }
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn git(repo: &Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn cargo_target(workspace: &Path) -> PathBuf {
        std::fs::create_dir_all(workspace).unwrap();
        std::fs::write(workspace.join("Cargo.toml"), "[workspace]\n").unwrap();
        let target = workspace.join("target");
        std::fs::create_dir_all(target.join("debug")).unwrap();
        std::fs::write(
            target.join("CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55\n",
        )
        .unwrap();
        target
    }

    #[test]
    fn porcelain_parser_keeps_state_with_each_worktree() {
        let trees = parse_worktrees(
            b"worktree /repo\0HEAD abc\0\0worktree /other\0locked offline\0\0worktree /old\0prunable missing\0\0worktree /bare\0bare\0\0",
        );
        assert_eq!(trees.len(), 4);
        assert_eq!(trees[0].path, Path::new("/repo"));
        assert!(!trees[0].locked);
        assert!(trees[1].locked);
        assert!(trees[2].prunable);
        assert!(trees[3].bare);
    }

    #[test]
    fn maps_a_nested_workspace_only_into_available_worktrees() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first");
        let second = dir.path().join("second");
        let locked = dir.path().join("locked");
        let no_manifest = dir.path().join("no-manifest");
        for path in [&first, &second, &locked] {
            std::fs::create_dir_all(path.join("rust")).unwrap();
            std::fs::write(path.join("rust/Cargo.toml"), "").unwrap();
        }
        std::fs::create_dir_all(no_manifest.join("rust")).unwrap();
        let trees = vec![
            Worktree {
                path: first.clone(),
                bare: false,
                locked: false,
                prunable: false,
            },
            Worktree {
                path: second.clone(),
                bare: false,
                locked: false,
                prunable: false,
            },
            Worktree {
                path: locked,
                bare: false,
                locked: true,
                prunable: false,
            },
            Worktree {
                path: no_manifest,
                bare: false,
                locked: false,
                prunable: false,
            },
        ];
        assert_eq!(
            sibling_workspaces(&first.join("rust"), &trees),
            vec![first.join("rust"), second.join("rust")]
        );
    }

    #[test]
    fn daemon_finds_a_sibling_target_without_claiming_a_build_used_it() {
        if Command::new("git")
            .arg("--version")
            .output()
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
        {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        let sibling = dir.path().join("sibling");
        let cache = dir.path().join("cache");
        std::fs::create_dir_all(&repo).unwrap();
        git(&repo, &["init", "-q"]);
        git(
            &repo,
            &[
                "-c",
                "user.name=Kache Test",
                "-c",
                "user.email=kache@example.invalid",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-q",
                "--allow-empty",
                "-m",
                "base",
            ],
        );
        git(
            &repo,
            &[
                "worktree",
                "add",
                "-q",
                "--detach",
                sibling.to_str().unwrap(),
            ],
        );
        let original = cargo_target(&repo.join("rust"));
        let found = cargo_target(&sibling.join("rust"));
        let config = crate::test_support::test_config(cache);
        let store = Store::open(&config).unwrap();
        store
            .remember_target_root(&original, &repo.join("rust"))
            .unwrap();

        assert_eq!(discover(&store).unwrap(), 1);
        assert_eq!(discover(&store).unwrap(), 0);
        let roots = store.tracked_target_roots(0).unwrap();
        assert_eq!(roots.len(), 2);
        let sibling_row = roots
            .iter()
            .find(|root| root.path == found.canonicalize().unwrap())
            .unwrap();
        assert!(sibling_row.discovered);
        assert_eq!(
            sibling_row.workspace_root,
            sibling.join("rust").canonicalize().unwrap()
        );
        assert_eq!(sibling_row.rustc, None);

        let old_identity = sibling_row.identity;
        std::fs::rename(&found, sibling.join("old-target")).unwrap();
        cargo_target(&sibling.join("rust"));
        assert_eq!(discover(&store).unwrap(), 1);
        let replaced = store
            .tracked_target_roots(0)
            .unwrap()
            .into_iter()
            .find(|root| root.path == found.canonicalize().unwrap())
            .unwrap();
        assert!(replaced.discovered);
        assert_ne!(replaced.identity, old_identity);

        store
            .remember_target_root(&found, &sibling.join("rust"))
            .unwrap();
        let row = store
            .tracked_target_roots(0)
            .unwrap()
            .into_iter()
            .find(|root| root.path == found)
            .unwrap();
        assert_eq!(store.tracked_target_roots(0).unwrap().len(), 2);
        assert!(
            !row.discovered,
            "a real build replaces discovery provenance"
        );

        store.forget_target_root(&found).unwrap();
        std::fs::remove_dir_all(&found).unwrap();
        std::fs::create_dir_all(found.join("debug")).unwrap();
        assert_eq!(discover(&store).unwrap(), 0);
        assert_eq!(store.tracked_target_roots(0).unwrap().len(), 1);
    }
}
