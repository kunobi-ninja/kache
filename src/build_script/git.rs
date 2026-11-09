//! Declared inputs inside a Git directory.
//!
//! A script that prints `cargo:rerun-if-changed=.git/HEAD` usually embeds the
//! commit, which lives in a ref file it may not name. Such an input is keyed
//! with the commit HEAD resolves to as well, so a new commit on the same
//! branch cannot replay the old one. In a linked worktree `.git` is a file
//! naming the worktree's Git directory, and the input is read there.

use anyhow::{Context, Result};
use std::path::{Component, Path, PathBuf};

/// A declared input that lies in a Git directory.
#[derive(Debug)]
pub(super) struct GitInput {
    /// Where Git keeps the declared path.
    pub(super) path: PathBuf,
    /// HEAD as written, then the commit it resolves to.
    pub(super) head: String,
    /// The files read to resolve `head`.
    pub(super) read: Vec<PathBuf>,
}

/// The Git directory behind `declared`, when one of its components is a
/// `.git` directory or a `.git` file naming one.
pub(super) fn resolve(declared: &Path) -> Result<Option<GitInput>> {
    let Some((dot_git, rest)) = split_at_dot_git(declared) else {
        return Ok(None);
    };
    let metadata = match std::fs::metadata(&dot_git) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    // The files read to find the Git directory count as inputs too: a `.git`
    // file pointed elsewhere during a run must fail the run's input check.
    let mut read = Vec::new();
    let (git_dir, linked) = if metadata.is_dir() {
        (dot_git, false)
    } else if metadata.is_file() {
        match linked_git_dir(&dot_git)? {
            Some(git_dir) => {
                read.push(dot_git);
                (git_dir, true)
            }
            None => return Ok(None),
        }
    } else {
        return Ok(None);
    };
    let common_dir = common_dir(&git_dir, &mut read)?;
    anyhow::ensure!(
        !common_dir.join("reftable").exists(),
        "{} keeps its refs in a reftable",
        common_dir.display()
    );
    let path = if !linked {
        declared.to_path_buf()
    } else if rest.as_os_str().is_empty() {
        git_dir.clone()
    } else {
        // Per-worktree files (HEAD, index) sit in the worktree's own Git
        // directory; shared ones (branch refs, packed-refs) in the common one.
        let own = git_dir.join(&rest);
        if std::fs::symlink_metadata(&own).is_ok() {
            own
        } else {
            common_dir.join(&rest)
        }
    };
    let head = head_state(&git_dir, &common_dir, &mut read)
        .with_context(|| format!("resolving HEAD in {}", git_dir.display()))?;
    Ok(Some(GitInput { path, head, read }))
}

/// `declared` split at its first `.git` component: the path up to and
/// including it, and what follows.
fn split_at_dot_git(declared: &Path) -> Option<(PathBuf, PathBuf)> {
    let mut components = declared.components();
    let mut prefix = PathBuf::new();
    for component in components.by_ref() {
        prefix.push(component);
        if component == Component::Normal(".git".as_ref()) {
            return Some((prefix, components.as_path().to_path_buf()));
        }
    }
    None
}

/// The directory a `.git` file names with `gitdir: <path>`.
fn linked_git_dir(dot_git: &Path) -> Result<Option<PathBuf>> {
    let contents = std::fs::read_to_string(dot_git)?;
    let Some(named) = contents.strip_prefix("gitdir:") else {
        return Ok(None);
    };
    let named = Path::new(named.trim());
    Ok(Some(if named.is_absolute() {
        named.to_path_buf()
    } else {
        dot_git.parent().unwrap_or(Path::new("")).join(named)
    }))
}

/// Where the refs shared by every worktree live.
fn common_dir(git_dir: &Path, read: &mut Vec<PathBuf>) -> Result<PathBuf> {
    let named_by = git_dir.join("commondir");
    match std::fs::read_to_string(&named_by) {
        Ok(named) => {
            read.push(named_by);
            Ok(git_dir.join(named.trim()))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(git_dir.to_path_buf()),
        Err(error) => Err(error.into()),
    }
}

/// HEAD as written and the commit it names, following symbolic refs through
/// loose ref files and then `packed-refs`. A branch with no commit yet is
/// `unborn`.
fn head_state(git_dir: &Path, common_dir: &Path, read: &mut Vec<PathBuf>) -> Result<String> {
    let head_path = git_dir.join("HEAD");
    let head = std::fs::read_to_string(&head_path)?;
    read.push(head_path);
    let head = head.trim();
    let mut target = head.to_string();
    // Git itself gives up after five levels of symbolic refs.
    for _ in 0..5 {
        let Some(name) = target.strip_prefix("ref:").map(str::trim) else {
            return Ok(format!("{head}\n{target}"));
        };
        let name = Path::new(name);
        anyhow::ensure!(
            name.components()
                .all(|component| matches!(component, Component::Normal(_))),
            "HEAD names an invalid ref {}",
            name.display()
        );
        let loose = [git_dir.join(name), common_dir.join(name)]
            .into_iter()
            .find(|path| path.is_file());
        if let Some(loose) = loose {
            target = std::fs::read_to_string(&loose)?.trim().to_string();
            read.push(loose);
            continue;
        }
        let packed_path = common_dir.join("packed-refs");
        let packed = match std::fs::read_to_string(&packed_path) {
            Ok(packed) => packed,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => return Err(error.into()),
        };
        read.push(packed_path);
        let name = name.to_str().context("ref name is not UTF-8")?;
        return Ok(match packed_commit(&packed, name) {
            Some(commit) => format!("{head}\n{commit}"),
            None => format!("{head}\nunborn"),
        });
    }
    anyhow::bail!("HEAD is a chain of symbolic refs too long to follow")
}

/// The commit `packed-refs` records for `name`.
fn packed_commit<'a>(packed: &'a str, name: &str) -> Option<&'a str> {
    packed
        .lines()
        .filter(|line| !line.starts_with('#') && !line.starts_with('^'))
        .find_map(|line| {
            let (commit, entry) = line.split_once(' ')?;
            (entry.trim() == name).then_some(commit)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &Path, contents: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, contents).unwrap();
    }

    /// A repository at `root/main` with a linked worktree at `root/wt`, the
    /// layout `git worktree add` leaves.
    fn repository(root: &Path) -> (PathBuf, PathBuf) {
        let main = root.join("main");
        let common = main.join(".git");
        write(&common.join("HEAD"), "ref: refs/heads/main\n");
        write(&common.join("refs/heads/main"), "1111\n");
        let own = common.join("worktrees/wt");
        write(&own.join("HEAD"), "ref: refs/heads/other\n");
        write(&own.join("commondir"), "../..\n");
        write(&own.join("index"), "index");
        write(
            &common.join("packed-refs"),
            "# pack-refs with: peeled\n2222 refs/heads/other\n^3333\n",
        );
        let wt = root.join("wt");
        write(&wt.join(".git"), &format!("gitdir: {}\n", own.display()));
        (main, wt)
    }

    #[test]
    fn a_linked_worktree_reads_its_own_git_directory() {
        let dir = tempfile::tempdir().unwrap();
        let (main, wt) = repository(dir.path());
        let own = main.join(".git/worktrees/wt");

        let head = resolve(&wt.join(".git/HEAD")).unwrap().unwrap();
        assert_eq!(head.path, own.join("HEAD"));
        assert_eq!(head.head, "ref: refs/heads/other\n2222");
        assert_eq!(
            resolve(&wt.join(".git/index")).unwrap().unwrap().path,
            own.join("index")
        );
        // Branch refs are shared, so they are read in the common directory.
        assert_eq!(
            resolve(&wt.join(".git/refs/heads/other"))
                .unwrap()
                .unwrap()
                .path,
            own.join("../../refs/heads/other")
        );
        assert_eq!(resolve(&wt.join(".git")).unwrap().unwrap().path, own);
        // Retargeting `.git` or `commondir` during a run must be noticed.
        let read = resolve(&wt.join(".git/HEAD")).unwrap().unwrap().read;
        assert!(read.contains(&wt.join(".git")), "{read:?}");
        assert!(read.contains(&own.join("commondir")), "{read:?}");
    }

    #[test]
    fn head_resolves_to_the_commit_it_names() {
        let dir = tempfile::tempdir().unwrap();
        let (main, _) = repository(dir.path());
        let input = resolve(&main.join(".git/HEAD")).unwrap().unwrap();
        assert_eq!(input.path, main.join(".git/HEAD"));
        assert_eq!(input.head, "ref: refs/heads/main\n1111");

        // A loose ref wins over packed-refs; a missing branch is unborn.
        write(&main.join(".git/refs/heads/main"), "4444\n");
        let input = resolve(&main.join(".git/HEAD")).unwrap().unwrap();
        assert_eq!(input.head, "ref: refs/heads/main\n4444");
        write(&main.join(".git/HEAD"), "ref: refs/heads/none\n");
        let input = resolve(&main.join(".git/HEAD")).unwrap().unwrap();
        assert_eq!(input.head, "ref: refs/heads/none\nunborn");
        write(&main.join(".git/HEAD"), "5555\n");
        let input = resolve(&main.join(".git/HEAD")).unwrap().unwrap();
        assert_eq!(input.head, "5555\n5555");
    }

    #[test]
    fn paths_outside_a_git_directory_are_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        assert!(resolve(&dir.path().join("src/lib.rs")).unwrap().is_none());
        // No repository at all: Cargo and kache both see a missing path.
        assert!(resolve(&dir.path().join(".git/HEAD")).unwrap().is_none());
        write(
            &dir.path().join("reftable/.git/HEAD"),
            "ref: refs/heads/.invalid\n",
        );
        std::fs::create_dir_all(dir.path().join("reftable/.git/reftable")).unwrap();
        assert!(resolve(&dir.path().join("reftable/.git/HEAD")).is_err());
    }
}
