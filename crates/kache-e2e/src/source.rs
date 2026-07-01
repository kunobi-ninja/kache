//! Shared source materialization helpers.
//!
//! The e2e harness materializes a fixture by copying it to a different
//! absolute path. The bench harness materializes a clone source as two git
//! worktrees at different absolute paths. Both are the same cross-path
//! operation at different scales.

use anyhow::{Context, Result, bail};
use std::path::{Path, PathBuf};
use std::process::Command;
#[cfg(unix)]
use std::process::Stdio;
use tempfile::TempDir;

/// A relocated fixture copy. Owns the [`TempDir`] for RAII cleanup but exposes
/// a long-form root path via [`path`](Self::path).
pub struct RelocatedDir {
    _temp: TempDir,
    root: PathBuf,
}

impl RelocatedDir {
    pub fn path(&self) -> &Path {
        &self.root
    }
}

/// Copy `src` into a fresh tempdir for a relocate-style phase to build in.
pub fn prepare_relocated_dir(src: &Path) -> Result<RelocatedDir> {
    let dst = TempDir::new().context("creating relocated tempdir")?;
    copy_dir_recursive(src, dst.path())
        .with_context(|| format!("copying relocated fixture {}", src.display()))?;
    copy_toolchain_pin(src, dst.path());
    let root = if cfg!(windows) {
        std::fs::canonicalize(dst.path())
            .map(|c| crate::portable_path(&c))
            .unwrap_or_else(|_| dst.path().to_path_buf())
    } else {
        dst.path().to_path_buf()
    };
    Ok(RelocatedDir { _temp: dst, root })
}

/// Carry the active Rust toolchain pin into the relocated tree.
pub fn copy_toolchain_pin(src: &Path, dst: &Path) {
    for dir in src.ancestors() {
        for name in ["rust-toolchain.toml", "rust-toolchain"] {
            let pin = dir.join(name);
            if pin.is_file() {
                let _ = std::fs::copy(&pin, dst.join(name));
                return;
            }
        }
    }
}

/// Derive the persistent reference-clone path for a given work dir.
pub fn clone_ref_path(work_dir: &Path) -> PathBuf {
    let parent = work_dir.parent().unwrap_or(Path::new("."));
    let name = work_dir
        .file_name()
        .map(|n| format!("{}-clone-ref", n.to_string_lossy()))
        .unwrap_or_else(|| "bench-clone-ref".to_string());
    parent.join(name)
}

/// Materialize `clone_a` and `clone_b` at `git_ref` as git worktrees off a
/// locally-cached shallow reference clone.
pub fn clone_worktrees(
    repo: &str,
    git_ref: &str,
    clone_ref: &Path,
    clone_a: &Path,
    clone_b: &Path,
) -> Result<()> {
    if clone_ref_at_ref(clone_ref, git_ref)? {
        eprintln!("\n[bench] reusing clone-ref at {git_ref} (no network)");
    } else {
        if clone_ref.exists() {
            eprintln!("\n[bench] clone-ref is at the wrong ref — wiping and re-cloning");
            std::fs::remove_dir_all(clone_ref)
                .with_context(|| format!("removing stale {}", clone_ref.display()))?;
        } else {
            eprintln!("\n[bench] no clone-ref yet — fetching {repo} @ {git_ref} (one-time cost)");
        }
        if let Some(parent) = clone_ref.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        run(Command::new("git")
            .args([
                "-c",
                "core.longpaths=true",
                "clone",
                "--depth",
                "1",
                "--branch",
            ])
            .arg(git_ref)
            .arg(repo)
            .arg(clone_ref))?;
    }

    for d in [clone_a, clone_b] {
        if d.exists() {
            eprintln!(
                "[bench] removing previous worktree {} (can take a few minutes, no progress output)...",
                d.display()
            );
        }
        reset_worktree_path(clone_ref, d)?;
    }
    let _ = Command::new("git")
        .arg("-C")
        .arg(clone_ref)
        .args(["worktree", "prune"])
        .status();

    for d in [clone_a, clone_b] {
        eprintln!("[bench] creating worktree {}", d.display());
        run(Command::new("git")
            .args(["-c", "core.longpaths=true"])
            .arg("-C")
            .arg(clone_ref)
            .args(["worktree", "add", "--detach"])
            .arg(d)
            .arg(git_ref))?;
    }
    Ok(())
}

/// Materialize a single detached worktree at `ref_a` in `clone_a`, with BOTH
/// `ref_a` and `ref_next` fetched into `clone_ref` so a later
/// `git checkout <ref_next>` in `clone_a` succeeds offline.
///
/// Unlike [`clone_worktrees`] (a `--depth 1 --branch` single-branch clone that
/// can reach only one tag/branch), the temporal "daily pull" bench needs two
/// arbitrary commits reachable, so it fetches both by SHA. GitHub honours
/// fetch-by-SHA; the caller MUST pin full 40-char SHAs (short SHAs are not
/// fetchable-by-SHA).
pub fn clone_worktrees_pull(
    repo: &str,
    ref_a: &str,
    ref_next: &str,
    clone_ref: &Path,
    clone_a: &Path,
) -> Result<()> {
    if !clone_ref.join(".git").exists() {
        if let Some(parent) = clone_ref.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        run(Command::new("git").args(["init", "-q"]).arg(clone_ref))?;
        run(Command::new("git")
            .arg("-C")
            .arg(clone_ref)
            .args(["remote", "add", "origin", repo]))?;
    }
    // Fetch both commits shallow. Re-running is cheap once present, but a
    // plain re-fetch is harmless, so keep it simple and always fetch.
    run(Command::new("git")
        .args(["-c", "core.longpaths=true"])
        .arg("-C")
        .arg(clone_ref)
        .args(["fetch", "--depth", "1", "origin", ref_a, ref_next]))?;

    reset_worktree_path(clone_ref, clone_a)?;
    let _ = Command::new("git")
        .arg("-C")
        .arg(clone_ref)
        .args(["worktree", "prune"])
        .status();
    run(Command::new("git")
        .args(["-c", "core.longpaths=true"])
        .arg("-C")
        .arg(clone_ref)
        .args(["worktree", "add", "--detach"])
        .arg(clone_a)
        .arg(ref_a))?;
    Ok(())
}

/// True when `clone_ref` is a valid git repository whose HEAD is the commit
/// named by `git_ref`.
pub fn clone_ref_at_ref(clone_ref: &Path, git_ref: &str) -> Result<bool> {
    if !clone_ref.join(".git").exists() {
        return Ok(false);
    }
    let head = Command::new("git")
        .arg("-C")
        .arg(clone_ref)
        .args(["rev-parse", "HEAD"])
        .output();
    let tagged = Command::new("git")
        .arg("-C")
        .arg(clone_ref)
        .args(["rev-parse"])
        .arg(format!("{git_ref}^{{commit}}"))
        .output();
    Ok(matches!(
        (head, tagged),
        (Ok(h), Ok(t)) if h.status.success() && t.status.success() && h.stdout == t.stdout
    ))
}

/// Best-effort cleanup of a previous worktree at `target`.
pub fn reset_worktree_path(clone_ref: &Path, target: &Path) -> Result<()> {
    if !target.exists() {
        return Ok(());
    }
    let _ = Command::new("git")
        .arg("-C")
        .arg(clone_ref)
        .args(["worktree", "remove", "--force"])
        .arg(target)
        .status();
    if target.exists() {
        std::fs::remove_dir_all(target)
            .with_context(|| format!("removing {}", target.display()))?;
    }
    Ok(())
}

fn run(cmd: &mut Command) -> Result<()> {
    let status = cmd.status().with_context(|| format!("spawning {cmd:?}"))?;
    if !status.success() {
        bail!("command failed ({status}): {cmd:?}");
    }
    Ok(())
}

/// Clone a directory tree via the filesystem's CoW reflink mechanism when
/// supported; fall back to a plain recursive copy.
pub fn snapshot_dir(src: &Path, dst: &Path) -> Result<()> {
    if dst.exists() {
        let _ = std::fs::remove_dir_all(dst);
    }
    #[cfg(unix)]
    {
        let try_cp = |args: &[&str]| -> bool {
            if dst.exists() {
                let _ = std::fs::remove_dir_all(dst);
            }
            Command::new("cp")
                .args(args)
                .arg(src)
                .arg(dst)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map(|s| s.success())
                .unwrap_or(false)
        };
        if try_cp(&["-cR"]) || try_cp(&["-R", "--reflink=auto"]) || try_cp(&["-R"]) {
            return Ok(());
        }
        if dst.exists() {
            let _ = std::fs::remove_dir_all(dst);
        }
    }
    copy_dir_recursive(src, dst)
        .with_context(|| format!("snapshotting {} -> {}", src.display(), dst.display()))
}

/// Recursively copy `src` into `dst` with plain byte copies, preserving
/// symlinks instead of flattening them.
pub fn copy_dir_recursive(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let from = entry.path();
        let to = dst.join(entry.file_name());
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            copy_symlink(&from, &to)?;
        } else if file_type.is_dir() {
            copy_dir_recursive(&from, &to)?;
        } else {
            std::fs::copy(&from, &to)?;
        }
    }
    Ok(())
}

#[cfg(unix)]
fn copy_symlink(from: &Path, to: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(std::fs::read_link(from)?, to)
}

#[cfg(windows)]
fn copy_symlink(from: &Path, to: &Path) -> std::io::Result<()> {
    let target = std::fs::read_link(from)?;
    if std::fs::metadata(from)?.is_dir() {
        std::os::windows::fs::symlink_dir(target, to)
    } else {
        std::os::windows::fs::symlink_file(target, to)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clone_ref_path_is_a_sibling_not_a_child_of_work_dir() {
        assert_eq!(
            clone_ref_path(Path::new("./tmp/bench/firefox")),
            PathBuf::from("./tmp/bench/firefox-clone-ref")
        );
        assert_eq!(
            clone_ref_path(Path::new("/scratch/foo")),
            PathBuf::from("/scratch/foo-clone-ref")
        );
    }

    #[test]
    fn prepare_relocated_dir_copies_fixture_contents() {
        let base = TempDir::new().unwrap();
        let src = base.path().join("src");
        std::fs::create_dir_all(src.join("nested")).unwrap();
        std::fs::write(src.join("top.txt"), b"top").unwrap();
        std::fs::write(src.join("nested").join("deep.txt"), b"deep").unwrap();

        let relocated = prepare_relocated_dir(&src).unwrap();

        assert_eq!(
            std::fs::read(relocated.path().join("top.txt")).unwrap(),
            b"top"
        );
        assert_eq!(
            std::fs::read(relocated.path().join("nested").join("deep.txt")).unwrap(),
            b"deep"
        );
    }

    #[test]
    fn copy_dir_recursive_round_trips_a_tree() {
        let base = std::env::temp_dir().join(format!("kb-copytree-{}", std::process::id()));
        let src = base.join("src");
        let dst = base.join("dst");
        std::fs::create_dir_all(src.join("nested")).unwrap();
        std::fs::write(src.join("top.txt"), b"top").unwrap();
        std::fs::write(src.join("nested").join("deep.txt"), b"deep").unwrap();

        copy_dir_recursive(&src, &dst).unwrap();

        assert_eq!(std::fs::read(dst.join("top.txt")).unwrap(), b"top");
        assert_eq!(
            std::fs::read(dst.join("nested").join("deep.txt")).unwrap(),
            b"deep"
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    #[cfg(unix)]
    #[test]
    fn copy_dir_recursive_preserves_symlinks() {
        let base = std::env::temp_dir().join(format!("kb-copylink-{}", std::process::id()));
        let src = base.join("src");
        let dst = base.join("dst");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("real.txt"), b"real").unwrap();
        std::os::unix::fs::symlink("real.txt", src.join("link.txt")).unwrap();

        copy_dir_recursive(&src, &dst).unwrap();

        let copied = dst.join("link.txt");
        assert!(
            std::fs::symlink_metadata(&copied)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            std::fs::read_link(copied).unwrap(),
            PathBuf::from("real.txt")
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn clone_worktrees_pull_makes_both_commits_checkoutable() {
        use std::process::Command;
        let tmp = tempfile::tempdir().unwrap();
        let upstream = tmp.path().join("upstream");
        std::fs::create_dir_all(&upstream).unwrap();
        let git = |args: &[&str], cwd: &std::path::Path| {
            let ok = Command::new("git")
                .args(args)
                .current_dir(cwd)
                .status()
                .unwrap()
                .success();
            assert!(ok, "git {:?} failed", args);
        };
        git(&["init", "-q"], &upstream);
        git(&["config", "user.email", "t@t"], &upstream);
        git(&["config", "user.name", "t"], &upstream);
        std::fs::write(upstream.join("f"), "a").unwrap();
        git(&["add", "."], &upstream);
        git(&["commit", "-qm", "a"], &upstream);
        let sha_a = rev_parse(&upstream, "HEAD");
        std::fs::write(upstream.join("f"), "b").unwrap();
        git(&["commit", "-aqm", "b"], &upstream);
        let sha_b = rev_parse(&upstream, "HEAD");

        let clone_ref = tmp.path().join("ref");
        let clone_a = tmp.path().join("clone-a");
        clone_worktrees_pull(
            upstream.to_str().unwrap(),
            &sha_a,
            &sha_b,
            &clone_ref,
            &clone_a,
        )
        .unwrap();

        // clone-a starts at ref A ...
        assert_eq!(std::fs::read_to_string(clone_a.join("f")).unwrap(), "a");
        // ... and ref B is fetched, so a same-worktree checkout to it works offline.
        let ok = Command::new("git")
            .args(["-C", clone_a.to_str().unwrap(), "checkout", "--detach", &sha_b])
            .status()
            .unwrap()
            .success();
        assert!(ok);
        assert_eq!(std::fs::read_to_string(clone_a.join("f")).unwrap(), "b");
    }

    fn rev_parse(dir: &std::path::Path, r: &str) -> String {
        let out = std::process::Command::new("git")
            .args(["-C", dir.to_str().unwrap(), "rev-parse", r])
            .output()
            .unwrap();
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    }
}
