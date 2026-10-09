//! The state of a declared build-script input: a file's content, a
//! directory's listing, a symlink's target and referent, or its absence.
//! Large directories are digested once per stamp of their entries' metadata.

use super::{Environment, fold};
use crate::tree_stamp::{memoised_digest, record_digest, tree_stamp};
use anyhow::Result;
use std::path::{Path, PathBuf};

/// A digest of what is at `path`: content for a file, the recursive listing
/// for a directory, the target and referent for a symlink, a marker for
/// nothing. Cargo's own freshness compares the same things.
pub(super) fn input_state(
    path: &Path,
    excluded: &[PathBuf],
    file_hasher: &crate::cache_key::FileHasher<'_>,
    budget: &mut usize,
    symlink_depth: usize,
) -> Result<String> {
    input_state_as(path, excluded, file_hasher, budget, symlink_depth, None)
}

/// [`input_state`], reading text files that spell one of `text`'s roots with
/// the roots as placeholders. A `links` dependency's `OUT_DIR` sits under
/// the same target directory as the dependent's, and a pkg-config file in it
/// names that directory: read raw, every checkout would key the dependent
/// differently. What the dependent writes from such a file is caught by its
/// own outputs' roots.
pub(super) fn input_state_as(
    path: &Path,
    excluded: &[PathBuf],
    file_hasher: &crate::cache_key::FileHasher<'_>,
    budget: &mut usize,
    symlink_depth: usize,
    text: Option<&Environment>,
) -> Result<String> {
    anyhow::ensure!(
        *budget > 0,
        "declared build-script inputs are too many to digest"
    );
    *budget -= 1;
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok("missing".to_string());
        }
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() {
        anyhow::ensure!(
            symlink_depth < 64,
            "declared build-script input is a symlink cycle"
        );
        let target = std::fs::read_link(path)?;
        let resolved = if target.is_absolute() {
            target.clone()
        } else {
            path.parent().unwrap_or(Path::new("")).join(&target)
        };
        let referent = input_state_as(
            &resolved,
            excluded,
            file_hasher,
            budget,
            symlink_depth + 1,
            text,
        )?;
        return Ok(format!("symlink:{}:{referent}", target.to_string_lossy()));
    }
    if metadata.is_file() {
        if let Some(environment) = text
            && let Some(normalized) = read_text(path)?
                .as_deref()
                .and_then(|contents| environment.rewrite_text(contents))
        {
            return Ok(format!("text:{}", blake3::hash(&normalized).to_hex()));
        }
        return Ok(format!("file:{}", file_hasher.hash(path)?));
    }
    if metadata.is_dir() {
        // A declared directory (a vendored C library, the package itself) can
        // hold thousands of files; hashing each through the per-file memo
        // costs a database read per file. One stat walk yields a stamp of
        // every entry's identity, size and times, and the digest of a tree
        // whose stamp is unchanged is the memoised one.
        let stamp = if symlink_depth == 0 {
            tree_stamp(path, excluded, *budget)
        } else {
            None
        };
        if let Some(stamp) = &stamp
            && let Some(digest) = tree_digest_memo(path, text, &stamp.digest)
        {
            return Ok(digest);
        }
        let digest = hash_directory(path, excluded, file_hasher, budget, symlink_depth, text)?;
        // Filesystem timestamps are coarse (a kernel tick on Linux), so a
        // same-size rewrite within the tick of the last write would keep the
        // stamp. A tree touched in the last seconds is hashed again next time
        // rather than memoised; the following run finds it settled.
        if let Some(stamp) = &stamp
            && stamp.settled_at(std::time::SystemTime::now())
        {
            record_tree_digest_memo(path, text, &stamp.digest, &digest);
        }
        return Ok(digest);
    }
    anyhow::bail!(
        "declared build-script input is neither a file nor a directory: {}",
        path.display()
    )
}

fn hash_directory(
    path: &Path,
    excluded: &[PathBuf],
    file_hasher: &crate::cache_key::FileHasher<'_>,
    budget: &mut usize,
    symlink_depth: usize,
    text: Option<&Environment>,
) -> Result<String> {
    let mut entries: Vec<_> = std::fs::read_dir(path)?.collect::<std::io::Result<_>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    let mut hasher = blake3::Hasher::new();
    for entry in entries {
        let child = entry.path();
        if excluded.contains(&child) {
            continue;
        }
        fold(&mut hasher, "name", entry.file_name().as_encoded_bytes());
        fold(
            &mut hasher,
            "state",
            input_state_as(&child, excluded, file_hasher, budget, symlink_depth, text)?.as_bytes(),
        );
    }
    Ok(format!("dir:{}", hasher.finalize().to_hex()))
}

/// Where tree digests are memoised: under the configured cache directory
/// once a run has loaded its configuration, else the environment's or the
/// default one.
pub(super) static TREE_MEMO_DIR: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();

/// A file's bytes when it may be text: `None` once its first block holds a
/// NUL, so object files and archives are not read in full.
fn read_text(path: &Path) -> Result<Option<Vec<u8>>> {
    use std::io::Read as _;
    let mut file = std::fs::File::open(path)?;
    let mut contents = Vec::new();
    (&mut file).take(8192).read_to_end(&mut contents)?;
    if contents.contains(&0) {
        return Ok(None);
    }
    file.read_to_end(&mut contents)?;
    Ok(Some(contents))
}

/// A digest read with roots as placeholders depends on the roots, so it is
/// memoised apart from the raw one and per set of roots.
fn tree_memo_path(path: &Path, text: Option<&Environment>) -> PathBuf {
    let mut hasher = blake3::Hasher::new();
    hasher.update(path.as_os_str().as_encoded_bytes());
    if let Some(environment) = text {
        for (root, placeholder) in environment.roots() {
            fold(
                &mut hasher,
                placeholder,
                root.as_os_str().as_encoded_bytes(),
            );
        }
    }
    let name = hasher.finalize().to_hex();
    TREE_MEMO_DIR
        .get()
        .map(|cache_dir| cache_dir.join("probes"))
        .unwrap_or_else(crate::config::probe_memo_dir)
        .join(format!("tree-{}.txt", &name[..24]))
}

pub(super) fn tree_digest_memo(
    path: &Path,
    text: Option<&Environment>,
    stamp: &str,
) -> Option<String> {
    memoised_digest(&tree_memo_path(path, text), stamp).filter(|digest| digest.starts_with("dir:"))
}

fn record_tree_digest_memo(path: &Path, text: Option<&Environment>, stamp: &str, digest: &str) {
    record_digest(&tree_memo_path(path, text), stamp, digest);
}
