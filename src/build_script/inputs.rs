//! The state of a declared build-script input: a file's content, a
//! directory's listing, a symlink's target and referent, or its absence.
//! Large directories are digested once per stamp of their entries' metadata.
//!
//! The run cache walks a tree leaving out only the paths it names, and
//! fails on anything it cannot read. The rustc key's walks (see
//! [`crate::build_script_inputs`]) also leave out VCS metadata and Cargo's
//! own directories, digest what rustc could not read either instead of
//! failing, and keep their memo apart.

use super::{Environment, fold};
use crate::tree_stamp::{
    StampRules, Stamper, TreeStamp, WalkOutcome, memoised_digest, record_digest, unsearchable,
};
use anyhow::Result;
use std::cell::RefCell;
use std::path::{Path, PathBuf};

/// Names left out of a tree by a walk with [`Walk::skip_metadata`], whether
/// a directory or a file (`.git` is a file in a worktree).
const VCS_METADATA: [&str; 4] = [".git", ".hg", ".jj", ".svn"];

/// A digest ran out of budget.
#[derive(Debug)]
pub(crate) struct TooManyInputs;

impl std::fmt::Display for TooManyInputs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("declared build-script inputs are too many to digest")
    }
}

impl std::error::Error for TooManyInputs {}

/// How a digest walks a tree: what it leaves out, what it makes of an entry
/// it cannot digest, and where it memoises. The default is the run cache's.
#[derive(Clone, Copy, Default)]
pub(crate) struct Walk<'a> {
    /// Paths left out wherever they appear.
    pub(crate) excluded: &'a [PathBuf],
    /// Also leave out VCS metadata and directories holding Cargo's
    /// `CACHEDIR.TAG`.
    pub(crate) skip_metadata: bool,
    /// Also leave out `*.rs` files, and sub-packages: directories below the
    /// walked one that hold a `Cargo.toml`.
    pub(crate) skip_rust_and_packages: bool,
    /// Digest a socket, FIFO or device as `other` instead of failing.
    pub(crate) other_entries: bool,
    /// Digest what rustc, running as the same user, cannot reach either,
    /// instead of failing: a path through a file as `missing`, an entry the
    /// user may not search as `unreadable`, and a symlink to a directory the
    /// walk is inside as a `cycle`. Cargo's own walks recover the same way.
    pub(crate) unreachable_entries: bool,
    /// Fail when a tree's stamp walk runs past the budget, before hashing
    /// any of it.
    pub(crate) stop_when_too_large: bool,
    /// Memoise tree digests in this cache directory, named apart from the
    /// run cache's by this namespace.
    pub(crate) memo: Option<(&'a Path, &'a str)>,
    /// Receives the path and stamp of the tree at the top of the walk.
    pub(crate) stamps: Option<&'a RefCell<Vec<(PathBuf, String)>>>,
}

impl Walk<'_> {
    /// Whether the walk leaves out `child`, the path of `entry`.
    fn skips(&self, child: &Path, entry: &std::fs::DirEntry) -> bool {
        self.excluded
            .iter()
            .any(|excluded| excluded.as_path() == child)
            || self.skip_metadata && is_metadata(child, entry)
            || self.skip_rust_and_packages && is_rust_or_package(child, entry)
    }
}

/// VCS metadata, or a directory Cargo tagged as its own.
fn is_metadata(child: &Path, entry: &std::fs::DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .is_some_and(|name| VCS_METADATA.contains(&name))
        || is_dir(entry) && crate::tree_stamp::is_cargo_build_tag(&child.join("CACHEDIR.TAG"))
}

/// A Rust source, or a directory holding another package.
fn is_rust_or_package(child: &Path, entry: &std::fs::DirEntry) -> bool {
    if is_dir(entry) {
        child.join("Cargo.toml").exists()
    } else {
        entry.file_name().as_encoded_bytes().ends_with(b".rs")
    }
}

/// Whether `entry` is a directory itself, not a link to one.
fn is_dir(entry: &std::fs::DirEntry) -> bool {
    entry.file_type().is_ok_and(|kind| kind.is_dir())
}

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
    let walk = Walk {
        excluded,
        ..Walk::default()
    };
    let within = &mut Within::default();
    walked_state(
        path,
        &walk,
        file_hasher,
        budget,
        symlink_depth,
        text,
        within,
    )
}

/// [`input_state`] under `walk`, for a path at the top of a walk.
pub(crate) fn state_in(
    path: &Path,
    walk: &Walk<'_>,
    file_hasher: &crate::cache_key::FileHasher<'_>,
    budget: &mut usize,
) -> Result<String> {
    let within = &mut Within::default();
    walked_state(path, walk, file_hasher, budget, 0, None, within)
}

/// The directories a walk is inside, outermost first, to tell a symlink that
/// leads back into one of them. Their real paths are looked up only when a
/// symlink needs them.
#[derive(Default)]
struct Within(Vec<(PathBuf, std::cell::OnceCell<Option<PathBuf>>)>);

impl Within {
    /// Whether `referent` is a directory the walk is inside, so following a
    /// symlink to it would walk the same tree again without end.
    fn holds(&self, referent: &Path) -> bool {
        let Ok(referent) = std::fs::canonicalize(referent) else {
            return false;
        };
        self.0.iter().any(|(dir, real)| {
            real.get_or_init(|| std::fs::canonicalize(dir).ok())
                .as_ref()
                == Some(&referent)
        })
    }
}

/// Whether hashing a file failed because the user may not read it.
fn read_denied(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == std::io::ErrorKind::PermissionDenied)
    })
}

/// [`input_state_as`] under `walk`. Each entry costs one from `budget`.
fn walked_state(
    path: &Path,
    walk: &Walk<'_>,
    file_hasher: &crate::cache_key::FileHasher<'_>,
    budget: &mut usize,
    symlink_depth: usize,
    text: Option<&Environment>,
    within: &mut Within,
) -> Result<String> {
    anyhow::ensure!(*budget > 0, TooManyInputs);
    *budget -= 1;
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok("missing".to_string());
        }
        // Cargo counts any path it cannot stat as missing. A component that
        // is a file (`.git` in a worktree) means there is nothing there; a
        // directory on the way the user may not search hides what is.
        Err(error)
            if walk.unreachable_entries && error.kind() == std::io::ErrorKind::NotADirectory =>
        {
            return Ok("missing".to_string());
        }
        Err(error)
            if walk.unreachable_entries && error.kind() == std::io::ErrorKind::PermissionDenied =>
        {
            return Ok("unreadable".to_string());
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
        let referent = if walk.unreachable_entries && within.holds(&resolved) {
            "cycle".to_string()
        } else {
            walked_state(
                &resolved,
                walk,
                file_hasher,
                budget,
                symlink_depth + 1,
                text,
                within,
            )?
        };
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
        return match file_hasher.hash(path) {
            Ok(hash) => Ok(format!("file:{hash}")),
            Err(error) if walk.unreachable_entries && read_denied(&error) => {
                Ok("unreadable".to_string())
            }
            Err(error) => Err(error),
        };
    }
    if metadata.is_dir() {
        // A declared directory (a vendored C library, the package itself) can
        // hold thousands of files; hashing each through the per-file memo
        // costs a database read per file. One stat walk yields a stamp of
        // every entry's identity, size and times, and the digest of a tree
        // whose stamp is unchanged is the memoised one.
        let started = std::time::SystemTime::now();
        let stamp_budget = *budget;
        let stamp = if symlink_depth == 0 {
            match tree_stamp_in(path, walk, stamp_budget) {
                Err(Unstamped::TooLarge) if walk.stop_when_too_large => {
                    return Err(TooManyInputs.into());
                }
                stamp => stamp.ok(),
            }
        } else {
            None
        };
        if let (Some(stamp), Some(stamps)) = (&stamp, walk.stamps) {
            stamps
                .borrow_mut()
                .push((path.to_path_buf(), stamp.digest.clone()));
        }
        if let Some(stamp) = &stamp
            && let Some(digest) = tree_digest_memo_in(path, walk.memo, text, &stamp.digest)
        {
            return Ok(digest);
        }
        // Only the top of the walk reports its stamp.
        let below = Walk {
            stamps: None,
            ..*walk
        };
        let digest = hash_directory(
            path,
            &below,
            file_hasher,
            budget,
            symlink_depth,
            text,
            within,
        )?;
        // Filesystem timestamps are coarse (a kernel tick on Linux), so a
        // same-size rewrite within the tick of the last write would keep the
        // stamp. A tree touched in the last seconds is hashed again next time
        // rather than memoised; the following run finds it settled.
        if let Some(stamp) = &stamp
            && memoisable(path, walk, stamp, stamp_budget, started)
        {
            record_tree_digest_memo(path, walk.memo, text, &stamp.digest, &digest);
        }
        return Ok(digest);
    }
    if walk.other_entries {
        return Ok("other".to_string());
    }
    anyhow::bail!(
        "declared build-script input is neither a file nor a directory: {}",
        path.display()
    )
}

/// Whether the digest of the tree at `path`, read after `stamp` was taken
/// with `budget` to spend, may be memoised under that stamp.
///
/// A walk with its own memo needs the tree to have settled before the walk
/// began (`started`) and to give the same stamp once read, as the tree
/// guard's memo does. A tree that settled only while a long read ran could
/// have been rewritten after the read within the tick of its newest write,
/// keeping its stamp. The run cache keeps its 1.0 rule, settled once read:
/// a 1.0 binary sharing the cache directory writes the same memo files by
/// it.
fn memoisable(
    path: &Path,
    walk: &Walk<'_>,
    stamp: &TreeStamp,
    budget: usize,
    started: std::time::SystemTime,
) -> bool {
    if walk.memo.is_none() {
        return stamp.settled_at(std::time::SystemTime::now());
    }
    stamp.settled_at(started)
        && tree_stamp_in(path, walk, budget).is_ok_and(|again| again.digest == stamp.digest)
}

fn hash_directory(
    path: &Path,
    walk: &Walk<'_>,
    file_hasher: &crate::cache_key::FileHasher<'_>,
    budget: &mut usize,
    symlink_depth: usize,
    text: Option<&Environment>,
    within: &mut Within,
) -> Result<String> {
    let listing = match std::fs::read_dir(path) {
        Ok(listing) => listing,
        Err(error) if walk.unreachable_entries && unsearchable(path, &error) => {
            return Ok("unreadable".to_string());
        }
        Err(error) => return Err(error.into()),
    };
    let mut entries: Vec<_> = listing.collect::<std::io::Result<_>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    within
        .0
        .push((path.to_path_buf(), std::cell::OnceCell::new()));
    let digest = hash_entries(
        &entries,
        walk,
        file_hasher,
        budget,
        symlink_depth,
        text,
        within,
    );
    within.0.pop();
    digest
}

/// The digest of a directory's `entries`, sorted by name.
fn hash_entries(
    entries: &[std::fs::DirEntry],
    walk: &Walk<'_>,
    file_hasher: &crate::cache_key::FileHasher<'_>,
    budget: &mut usize,
    symlink_depth: usize,
    text: Option<&Environment>,
    within: &mut Within,
) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    for entry in entries {
        let child = entry.path();
        if walk.skips(&child, entry) {
            continue;
        }
        let state = walked_state(
            &child,
            walk,
            file_hasher,
            budget,
            symlink_depth,
            text,
            within,
        )?;
        // Like Cargo's list of a package's files, the package digest counts a
        // directory only through what is left in it: adding `tests/` with
        // Rust files alone changes nothing.
        if walk.skip_rust_and_packages && state == empty_directory() {
            continue;
        }
        fold(&mut hasher, "name", entry.file_name().as_encoded_bytes());
        fold(&mut hasher, "state", state.as_bytes());
    }
    Ok(format!("dir:{}", hasher.finalize().to_hex()))
}

/// What a directory with nothing to digest in it digests as.
fn empty_directory() -> String {
    format!("dir:{}", blake3::Hasher::new().finalize().to_hex())
}

/// Why a tree has no stamp.
#[derive(Debug, PartialEq, Eq)]
enum Unstamped {
    /// More entries than the budget.
    TooLarge,
    /// A symlink, or an entry that could not be read.
    Unstampable,
}

#[cfg(all(test, unix))]
pub(super) fn tree_stamp(path: &Path, excluded: &[PathBuf], budget: usize) -> Option<TreeStamp> {
    let walk = Walk {
        excluded,
        ..Walk::default()
    };
    tree_stamp_in(path, &walk, budget).ok()
}

/// The stamp of the tree at `path` as `walk` leaves it out: one stat walk
/// that refuses a symlink, since the digest reads through links.
fn tree_stamp_in(path: &Path, walk: &Walk<'_>, budget: usize) -> Result<TreeStamp, Unstamped> {
    let mut stamper = Stamper::new();
    let mut remaining = budget;
    // What a directory below the top hides from the user, it hides from
    // rustc too; its own metadata is in the stamp already.
    let rules = StampRules {
        unsearchable_dirs: walk.unreachable_entries,
        ..StampRules::default()
    };
    match stamper.walk_skipping(
        path,
        &|child, entry| walk.skips(child, entry),
        rules,
        &mut remaining,
    ) {
        WalkOutcome::Fits => Ok(stamper.finish()),
        WalkOutcome::TooLarge => Err(Unstamped::TooLarge),
        WalkOutcome::Unreadable => Err(Unstamped::Unstampable),
    }
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
/// memoised apart from the raw one and per set of roots. A walk with its own
/// rules memoises apart from the run cache, which must never read a digest
/// those rules produced.
fn tree_memo_path(path: &Path, memo: Option<(&Path, &str)>, text: Option<&Environment>) -> PathBuf {
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
    if let Some((_, namespace)) = memo {
        fold(&mut hasher, "namespace", namespace.as_bytes());
    }
    let name = hasher.finalize().to_hex();
    match memo {
        Some((cache_dir, _)) => cache_dir.join("probes"),
        None => TREE_MEMO_DIR
            .get()
            .map(|cache_dir| cache_dir.join("probes"))
            .unwrap_or_else(crate::config::probe_memo_dir),
    }
    .join(format!("tree-{}.txt", &name[..24]))
}

#[cfg(all(test, unix))]
pub(super) fn tree_digest_memo(
    path: &Path,
    text: Option<&Environment>,
    stamp: &str,
) -> Option<String> {
    tree_digest_memo_in(path, None, text, stamp)
}

fn tree_digest_memo_in(
    path: &Path,
    memo: Option<(&Path, &str)>,
    text: Option<&Environment>,
    stamp: &str,
) -> Option<String> {
    memoised_digest(&tree_memo_path(path, memo, text), stamp)
        .filter(|digest| digest.starts_with("dir:"))
}

fn record_tree_digest_memo(
    path: &Path,
    memo: Option<(&Path, &str)>,
    text: Option<&Environment>,
    stamp: &str,
    digest: &str,
) {
    record_digest(&tree_memo_path(path, memo, text), stamp, digest);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &Path, contents: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, contents).unwrap();
    }

    fn skipped(root: &Path, walk: &Walk<'_>) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(root)
            .unwrap()
            .map(Result::unwrap)
            .filter(|entry| walk.skips(&entry.path(), entry))
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn a_walk_leaves_out_only_what_its_rules_name() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("a.txt"), "a");
        write(&root.join("b.rs"), "b");
        write(&root.join(".git"), "gitdir: elsewhere\n");
        for vcs in [".hg", ".jj", ".svn"] {
            write(&root.join(vcs).join("x"), "x");
        }
        write(
            &root.join("target/CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55\n# created by cargo\n",
        );
        write(
            &root.join("tool-cache/CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55\n# created by a tool\n",
        );
        write(&root.join("sub/Cargo.toml"), "[package]\n");
        write(&root.join("plain/x.rs"), "x");
        write(&root.join("excluded/x"), "x");
        let excluded = [root.join("excluded")];

        let run_cache = Walk {
            excluded: &excluded,
            ..Walk::default()
        };
        assert_eq!(skipped(root, &run_cache), ["excluded"]);
        let metadata = Walk {
            skip_metadata: true,
            ..run_cache
        };
        assert_eq!(
            skipped(root, &metadata),
            [".git", ".hg", ".jj", ".svn", "excluded", "target"]
        );
        let package = Walk {
            skip_rust_and_packages: true,
            ..run_cache
        };
        assert_eq!(skipped(root, &package), ["b.rs", "excluded", "sub"]);
    }

    #[test]
    fn the_package_digest_counts_a_directory_only_through_its_files() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("pkg");
        write(&root.join("a.txt"), "a");
        let hasher = crate::cache_key::FileHasher::new();
        let digest = |walk: &Walk<'_>| state_in(&root, walk, &hasher, &mut 100).unwrap();
        let package = Walk {
            skip_rust_and_packages: true,
            ..Walk::default()
        };
        let (run_cache, packaged) = (digest(&Walk::default()), digest(&package));
        write(&root.join("tests/t.rs"), "");
        std::fs::create_dir(root.join("empty")).unwrap();
        assert_eq!(digest(&package), packaged);
        assert_ne!(
            digest(&Walk::default()),
            run_cache,
            "the run cache counts every directory"
        );
        write(&root.join("tests/data.txt"), "d");
        assert_ne!(
            digest(&package),
            packaged,
            "a file in a new directory counts"
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_special_file_digests_as_other_only_when_the_walk_allows_it() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("tree");
        std::fs::create_dir(&root).unwrap();
        let _socket = std::os::unix::net::UnixListener::bind(root.join("sock")).unwrap();
        let hasher = crate::cache_key::FileHasher::new();
        let mut budget = 10;
        let error = input_state(&root.join("sock"), &[], &hasher, &mut budget, 0).unwrap_err();
        assert!(
            error.to_string().contains("neither a file nor a directory"),
            "{error:#}"
        );
        assert!(input_state(&root, &[], &hasher, &mut budget, 0).is_err());
        let walk = Walk {
            other_entries: true,
            ..Walk::default()
        };
        assert_eq!(
            state_in(&root.join("sock"), &walk, &hasher, &mut budget).unwrap(),
            "other"
        );
        assert!(state_in(&root, &walk, &hasher, &mut budget).is_ok());
    }

    #[test]
    fn a_walk_that_stops_when_too_large_hashes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        for index in 0..5 {
            write(&dir.path().join(format!("{index}.txt")), "x");
        }
        let hashed = |walk: &Walk<'_>| {
            let mut hasher = crate::cache_key::FileHasher::new();
            hasher.arm_too_new_guard(1, 0);
            let mut budget = 4;
            let error = state_in(dir.path(), walk, &hasher, &mut budget).unwrap_err();
            assert!(error.is::<TooManyInputs>(), "{error:#}");
            hasher.take_guarded_inputs().len()
        };
        assert!(hashed(&Walk::default()) > 0, "the run cache hashes first");
        assert_eq!(
            hashed(&Walk {
                stop_when_too_large: true,
                ..Walk::default()
            }),
            0
        );
    }

    #[test]
    fn a_walk_with_its_own_memo_never_shares_the_run_caches() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let root = dir.path().join("tree");
        let named = |namespace| tree_memo_path(&root, Some((&cache, namespace)), None);
        assert_eq!(named("a").parent(), Some(cache.join("probes").as_path()));
        assert_ne!(named("a"), named("b"));
        assert_ne!(
            named("a").file_name(),
            tree_memo_path(&root, None, None).file_name()
        );

        write(&root.join("a.txt"), "a");
        let old = filetime::FileTime::from_unix_time(1_000_000_000, 0);
        filetime::set_file_mtime(root.join("a.txt"), old).unwrap();
        let walk = Walk {
            memo: Some((&cache, "a")),
            ..Walk::default()
        };
        let hasher = crate::cache_key::FileHasher::new();
        let digest = state_in(&root, &walk, &hasher, &mut 10).unwrap();
        let stamp = tree_stamp_in(&root, &walk, 10).unwrap();
        assert_eq!(
            tree_digest_memo_in(&root, walk.memo, None, &stamp.digest),
            Some(digest),
            "a settled tree is memoised in its namespace"
        );
    }

    /// A walk with its own memo records a digest only for a tree that had
    /// settled before the walk began and still has its stamp once read. A
    /// read that ends past the settle window cannot vouch for a same-size
    /// rewrite within the tick of the newest write, which keeps the stamp.
    #[test]
    fn a_walk_with_its_own_memo_records_only_a_tree_that_held_still() {
        use std::time::{Duration, SystemTime};
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let root = dir.path().join("tree");
        let file = root.join("a.txt");
        write(&file, "a");
        let walk = Walk {
            memo: Some((&cache, "a")),
            ..Walk::default()
        };
        let mut hasher = crate::cache_key::FileHasher::new();
        // Armed, the hasher reads each file through the hook.
        hasher.arm_too_new_guard(1, 0);
        let memoised =
            |stamp: &TreeStamp| tree_digest_memo_in(&root, walk.memo, None, &stamp.digest);
        let read = |hook: crate::cache_key::BeforeRead| {
            crate::cache_key::set_before_read(Some(hook));
            let digest = state_in(&root, &walk, &hasher, &mut 10);
            crate::cache_key::set_before_read(None);
            digest.unwrap()
        };

        // Written a little inside the settle window, and held up in the read
        // until the window has passed.
        let written = SystemTime::now() - TreeStamp::SETTLE + Duration::from_millis(400);
        filetime::set_file_mtime(&file, filetime::FileTime::from_system_time(written)).unwrap();
        let newest = std::fs::metadata(&file).unwrap().modified().unwrap();
        let began_inside = std::rc::Rc::new(std::cell::Cell::new(false));
        let seen = began_inside.clone();
        read(Box::new(move |_| {
            let age = SystemTime::now().duration_since(newest).unwrap_or_default();
            seen.set(age < TreeStamp::SETTLE);
            std::thread::sleep(
                (TreeStamp::SETTLE + Duration::from_millis(100)).saturating_sub(age),
            );
        }));
        let stamp = tree_stamp_in(&root, &walk, 10).unwrap();
        if began_inside.get() {
            assert_eq!(memoised(&stamp), None, "it settled only while it was read");
        } else {
            eprintln!("skipped a check: the read began past the settle window");
        }

        // Settled long before, and rewritten while it was read.
        let old = filetime::FileTime::from_unix_time(1_000_000_000, 0);
        filetime::set_file_mtime(&file, old).unwrap();
        let stamp = tree_stamp_in(&root, &walk, 10).unwrap();
        let mut rewrite = true;
        read(Box::new(move |path| {
            if std::mem::take(&mut rewrite) {
                std::fs::write(path, "bb").unwrap();
            }
        }));
        assert_eq!(memoised(&stamp), None, "it moved while it was read");

        filetime::set_file_mtime(&file, old).unwrap();
        let digest = read(Box::new(|_| {}));
        let stamp = tree_stamp_in(&root, &walk, 10).unwrap();
        assert_eq!(memoised(&stamp), Some(digest), "a tree that held still");
    }

    /// Sets the mode of `path` and restores it when dropped, so the scratch
    /// directory can be removed.
    #[cfg(unix)]
    struct Mode(PathBuf);

    #[cfg(unix)]
    impl Mode {
        fn set(path: &Path, mode: u32) -> Self {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
            Self(path.to_path_buf())
        }
    }

    #[cfg(unix)]
    impl Drop for Mode {
        fn drop(&mut self) {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o755));
        }
    }

    /// Whether file modes bind this process. Root reads anything.
    #[cfg(unix)]
    fn modes_bind(dir: &Path) -> bool {
        let probe = dir.join("probe");
        std::fs::create_dir(&probe).unwrap();
        let _mode = Mode::set(&probe, 0o000);
        std::fs::read_dir(&probe).is_err()
    }

    /// A walk that recovers, with its memo kept in `cache`.
    #[cfg(unix)]
    fn recovering(cache: &Path) -> Walk<'_> {
        Walk {
            unreachable_entries: true,
            memo: Some((cache, "test")),
            ..Walk::default()
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_recovering_walk_reads_what_rustc_cannot_reach_either() {
        let dir = tempfile::tempdir().unwrap();
        if !modes_bind(dir.path()) {
            return;
        }
        let cache = dir.path().join("cache");
        let root = dir.path().join("tree");
        write(&root.join("file"), "x");
        write(&root.join("locked/inner.txt"), "inner");
        write(&root.join("sealed.txt"), "sealed");
        let hasher = crate::cache_key::FileHasher::new();
        let walk = recovering(&cache);
        let run_cache = Walk::default();
        let state = |path: &Path, walk: &Walk<'_>| state_in(path, walk, &hasher, &mut 100);

        assert_eq!(state(&root.join("file/child"), &walk).unwrap(), "missing");
        let readable = state(&root, &walk).unwrap();
        {
            let _locked = Mode::set(&root.join("locked"), 0o000);
            let _sealed = Mode::set(&root.join("sealed.txt"), 0o000);
            for (path, why) in [
                (root.join("locked"), "a directory the user may not search"),
                (root.join("locked/inner.txt"), "a path below it"),
                (root.join("sealed.txt"), "a file the user may not read"),
            ] {
                assert_eq!(state(&path, &walk).unwrap(), "unreadable", "{why}");
            }
            assert_ne!(state(&root, &walk).unwrap(), readable);
            for path in [
                root.join("locked"),
                root.join("locked/inner.txt"),
                root.join("sealed.txt"),
            ] {
                assert!(
                    state(&path, &run_cache).is_err(),
                    "the run cache refuses {}",
                    path.display()
                );
            }
        }
        assert!(
            state(&root.join("file/child"), &run_cache).is_err(),
            "the run cache refuses a path through a file"
        );
        let _searchable = Mode::set(&root.join("locked"), 0o100);
        assert!(
            state(&root.join("locked"), &walk).is_err(),
            "rustc can reach what is in a directory it may search but not list"
        );
    }

    #[test]
    fn only_a_permission_error_reads_a_file_as_unreadable() {
        let error = |kind| anyhow::Error::from(std::io::Error::from(kind)).context("hashing");
        assert!(read_denied(&error(std::io::ErrorKind::PermissionDenied)));
        assert!(!read_denied(&error(std::io::ErrorKind::Other)));
        assert!(!read_denied(&anyhow::anyhow!("permission denied")));
    }

    #[cfg(unix)]
    #[test]
    fn a_stamp_passes_over_a_directory_below_its_top_that_no_one_can_search() {
        let dir = tempfile::tempdir().unwrap();
        if !modes_bind(dir.path()) {
            return;
        }
        let cache = dir.path().join("cache");
        let root = dir.path().join("tree");
        write(&root.join("a.txt"), "a");
        std::fs::create_dir(root.join("locked")).unwrap();
        let walk = recovering(&cache);
        {
            let _locked = Mode::set(&root.join("locked"), 0o000);
            assert!(tree_stamp_in(&root, &walk, 100).is_ok());
            assert_eq!(
                tree_stamp_in(&root, &Walk::default(), 100).err(),
                Some(Unstamped::Unstampable),
                "the run cache's walk"
            );
            assert_eq!(
                tree_stamp_in(&root.join("locked"), &walk, 100).err(),
                Some(Unstamped::Unstampable),
                "the top of the walk"
            );
        }
        let _searchable = Mode::set(&root.join("locked"), 0o100);
        assert_eq!(
            tree_stamp_in(&root, &walk, 100).err(),
            Some(Unstamped::Unstampable),
            "a directory that can be searched but not listed"
        );
    }

    /// An empty directory and a locked one would stamp alike, so a locked
    /// top has no stamp and never finds the memo of its empty past.
    #[cfg(unix)]
    #[test]
    fn a_locked_directory_does_not_read_the_memo_of_its_empty_past() {
        let dir = tempfile::tempdir().unwrap();
        if !modes_bind(dir.path()) {
            return;
        }
        let cache = dir.path().join("cache");
        let empty = dir.path().join("empty");
        std::fs::create_dir(&empty).unwrap();
        let walk = recovering(&cache);
        let hasher = crate::cache_key::FileHasher::new();
        let open = state_in(&empty, &walk, &hasher, &mut 10).unwrap();
        assert_eq!(open, empty_directory());
        let _locked = Mode::set(&empty, 0o000);
        assert_eq!(
            state_in(&empty, &walk, &hasher, &mut 10).unwrap(),
            "unreadable"
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_to_a_directory_the_walk_is_in_is_a_cycle() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let tree = dir.path().join("tree");
        let shared = dir.path().join("shared");
        write(&tree.join("a.txt"), "a");
        write(&shared.join("data/d.txt"), "d");
        write(&shared.join("other.txt"), "1");
        std::os::unix::fs::symlink(".", tree.join("self")).unwrap();
        std::os::unix::fs::symlink(shared.join("data"), tree.join("data")).unwrap();
        std::os::unix::fs::symlink("..", shared.join("data/up")).unwrap();
        let hasher = crate::cache_key::FileHasher::new();
        let walk = recovering(&cache);
        let state = |path: &Path| state_in(path, &walk, &hasher, &mut 1000).unwrap();

        let before = state(&tree);
        // `tree/data/up` leads to `shared`, which the walk is not inside, so
        // what is there counts even though `shared/data` is reached twice.
        write(&shared.join("other.txt"), "2");
        assert_ne!(state(&tree), before, "a file reached through two links");
        // A link at the top is followed once: it is not inside its target yet.
        let linked = state(&tree.join("self"));
        write(&tree.join("a.txt"), "b");
        assert_ne!(state(&tree.join("self")), linked);
        assert!(
            state_in(&tree, &Walk::default(), &hasher, &mut 1000).is_err(),
            "the run cache refuses a cycle"
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_beside_a_walked_directory_walks_it_again() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let tree = dir.path().join("tree");
        write(&tree.join("a/f.txt"), "f");
        std::os::unix::fs::symlink("a", tree.join("b")).unwrap();
        let hasher = crate::cache_key::FileHasher::new();
        let mut budget = 100;
        state_in(&tree, &recovering(&cache), &hasher, &mut budget).unwrap();
        // The tree, `a`, `a/f.txt`, `b`, and `a` with `f.txt` again through
        // `b`: a walk is inside `a` only while it walks `a`.
        assert_eq!(100 - budget, 6);
    }
}
