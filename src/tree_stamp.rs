//! Stat-walk stamps of directory trees, and the digests memoised under them.
//!
//! A content digest of a tree reads every file in it. One walk that stats
//! every entry yields a stamp of the tree's names, kinds, sizes and times;
//! while a tree has the stamp a digest was recorded under, that digest still
//! holds and no file needs reading.

use std::path::{Path, PathBuf};

/// A digest of every entry a [`Stamper`] walked: name, kind, size,
/// modification and change time, and the link text of a symlink.
pub(crate) struct TreeStamp {
    pub(crate) digest: String,
    /// The newest modification time seen in the walk.
    newest: std::time::SystemTime,
}

impl TreeStamp {
    /// Coarse filesystem clocks make a stamp taken within this window of its
    /// newest write ambiguous.
    pub(crate) const SETTLE: std::time::Duration = std::time::Duration::from_secs(2);

    pub(crate) fn settled_at(&self, now: std::time::SystemTime) -> bool {
        now.duration_since(self.newest)
            .is_ok_and(|age| age >= Self::SETTLE)
    }
}

/// How a stamp walk treats symlinks and Cargo's build directories.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct StampRules {
    /// Fold a symlink's text instead of refusing the tree. Only for a digest
    /// that does not read through a link either: one that does could change
    /// with the link's target while the stamp stays the same.
    pub(crate) link_text: bool,
    /// Below the root, walk only the tag of a directory Cargo tagged as its
    /// build directory (see [`keep_only_build_tag`]).
    pub(crate) skip_build_dirs: bool,
    /// Stamp the root's own metadata too, so a name created and removed
    /// again directly under it still changes the stamp.
    pub(crate) root_metadata: bool,
    /// Below the root, stamp what cannot be read as unreadable instead of
    /// refusing the tree: a directory that cannot be listed, an entry that
    /// cannot be stat'ed or a link that cannot be read. Only for a digest
    /// that does the same. A process cannot read it either, and once it can,
    /// the walk lists or stats it and the stamp changes.
    pub(crate) unreadable_entries: bool,
    /// Stamp only the entries directly under the root that are not
    /// directories.
    pub(crate) top_files_only: bool,
    /// Below the root, pass over a directory the user may neither list nor
    /// search ([`unsearchable`]): a process the user runs cannot reach what
    /// it holds either, and its own entry is stamped already. Any other
    /// error still refuses the tree.
    pub(crate) unsearchable_dirs: bool,
}

/// How a walk over one root ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WalkOutcome {
    Fits,
    /// More entries than the budget.
    TooLarge,
    /// A directory or entry could not be read and the rules do not stamp it
    /// as unreadable, or a symlink the rules refuse.
    Unreadable,
}

/// One stat walk over one or more roots, folded into one [`TreeStamp`].
pub(crate) struct Stamper {
    hasher: blake3::Hasher,
    newest: std::time::SystemTime,
}

impl Stamper {
    pub(crate) fn new() -> Self {
        Self {
            hasher: blake3::Hasher::new(),
            newest: std::time::SystemTime::UNIX_EPOCH,
        }
    }

    /// Mark the start of the next root, so an entry cannot move from one
    /// root to another with the stamp unchanged.
    pub(crate) fn label(&mut self, label: &[u8]) {
        fold(&mut self.hasher, "root", label);
    }

    /// Fold every entry under `root` but `excluded`, spending one of
    /// `budget` per entry. A symlink is not followed.
    pub(crate) fn walk(
        &mut self,
        root: &Path,
        excluded: &[PathBuf],
        rules: StampRules,
        budget: &mut usize,
    ) -> WalkOutcome {
        self.walk_skipping(
            root,
            &|child, _| excluded.iter().any(|path| path == child),
            rules,
            budget,
        )
    }

    /// [`Self::walk`], leaving out every entry `skips` names by its path and
    /// its listing entry, wherever it appears below `root`.
    pub(crate) fn walk_skipping(
        &mut self,
        root: &Path,
        skips: &dyn Fn(&Path, &std::fs::DirEntry) -> bool,
        rules: StampRules,
        budget: &mut usize,
    ) -> WalkOutcome {
        if rules.root_metadata {
            let Ok(metadata) = std::fs::metadata(root) else {
                return WalkOutcome::Unreadable;
            };
            self.hasher.update(b"top");
            fold_metadata_stamp(&mut self.hasher, &metadata);
            self.saw(&metadata);
        }
        let mut pending = vec![root.to_path_buf()];
        while let Some(directory) = pending.pop() {
            let listed = std::fs::read_dir(&directory)
                .and_then(|entries| entries.collect::<std::io::Result<Vec<_>>>());
            let mut entries = match listed {
                Ok(entries) => entries,
                // Its own entry is stamped; what it holds is not known.
                Err(_) if rules.unreadable_entries && directory != root => {
                    let Ok(relative) = directory.strip_prefix(root) else {
                        return WalkOutcome::Unreadable;
                    };
                    fold(
                        &mut self.hasher,
                        "unlisted",
                        relative.as_os_str().as_encoded_bytes(),
                    );
                    continue;
                }
                Err(error)
                    if rules.unsearchable_dirs
                        && directory != root
                        && unsearchable(&directory, &error) =>
                {
                    continue;
                }
                Err(_) => return WalkOutcome::Unreadable,
            };
            entries.sort_by_key(std::fs::DirEntry::file_name);
            if rules.skip_build_dirs && directory != root {
                keep_only_build_tag(&mut entries);
            }
            for entry in entries {
                let child = entry.path();
                if skips(&child, &entry)
                    || rules.top_files_only && entry.file_type().is_ok_and(|kind| kind.is_dir())
                {
                    continue;
                }
                let Some(left) = budget.checked_sub(1) else {
                    return WalkOutcome::TooLarge;
                };
                *budget = left;
                let Ok(relative) = child.strip_prefix(root) else {
                    return WalkOutcome::Unreadable;
                };
                let read = std::fs::symlink_metadata(&child).and_then(|metadata| {
                    let link = metadata
                        .file_type()
                        .is_symlink()
                        .then(|| std::fs::read_link(&child))
                        .transpose()?;
                    Ok((metadata, link))
                });
                let (metadata, link) = match read {
                    Ok(read) => read,
                    Err(_) if rules.unreadable_entries => {
                        fold(
                            &mut self.hasher,
                            "unreadable",
                            relative.as_os_str().as_encoded_bytes(),
                        );
                        continue;
                    }
                    Err(_) => return WalkOutcome::Unreadable,
                };
                if link.is_some() && !rules.link_text {
                    return WalkOutcome::Unreadable;
                }
                fold(
                    &mut self.hasher,
                    "entry",
                    relative.as_os_str().as_encoded_bytes(),
                );
                match &link {
                    Some(target) => {
                        self.hasher.update(b"lnk");
                        fold(
                            &mut self.hasher,
                            "link",
                            target.as_os_str().as_encoded_bytes(),
                        );
                    }
                    None => {
                        self.hasher
                            .update(if metadata.is_dir() { b"dir" } else { b"fil" });
                    }
                }
                fold_metadata_stamp(&mut self.hasher, &metadata);
                self.saw(&metadata);
                if metadata.is_dir() && !rules.top_files_only {
                    pending.push(child);
                }
            }
        }
        WalkOutcome::Fits
    }

    /// Keep the newest modification time.
    fn saw(&mut self, metadata: &std::fs::Metadata) {
        if let Ok(modified) = metadata.modified()
            && modified > self.newest
        {
            self.newest = modified;
        }
    }

    pub(crate) fn finish(self) -> TreeStamp {
        TreeStamp {
            digest: self.hasher.finalize().to_hex().to_string(),
            newest: self.newest,
        }
    }
}

/// Whether listing `dir` failed because the user may neither list nor
/// search it, so nothing in it can be reached by name either.
pub(crate) fn unsearchable(dir: &Path, error: &std::io::Error) -> bool {
    let denied = |error: &std::io::Error| error.kind() == std::io::ErrorKind::PermissionDenied;
    denied(error) && std::fs::symlink_metadata(dir.join(".")).is_err_and(|error| denied(&error))
}

/// The file Cargo writes into a build directory it creates.
const BUILD_TAG: &str = "CACHEDIR.TAG";

/// Keep only the tag of a directory Cargo tagged as its build directory: the
/// rest is build output. Other tools write `CACHEDIR.TAG` too, so the text
/// is checked, and a directory with another tag keeps every entry.
pub(crate) fn keep_only_build_tag(entries: &mut Vec<std::fs::DirEntry>) {
    let tagged = entries.iter().any(|entry| {
        entry.file_name() == BUILD_TAG
            && entry.file_type().is_ok_and(|kind| kind.is_file())
            && is_cargo_build_tag(&entry.path())
    });
    if tagged {
        entries.retain(|entry| entry.file_name() == BUILD_TAG);
    }
}

/// Did Cargo tag `directory` as its build directory? The same answer
/// [`keep_only_build_tag`] gives from a listing.
pub(crate) fn holds_cargo_build_tag(directory: &Path) -> bool {
    let tag = directory.join(BUILD_TAG);
    std::fs::symlink_metadata(&tag).is_ok_and(|metadata| metadata.is_file())
        && is_cargo_build_tag(&tag)
}

/// Is the `CACHEDIR.TAG` at `path` the one Cargo writes?
pub(crate) fn is_cargo_build_tag(path: &Path) -> bool {
    std::fs::read_to_string(path).is_ok_and(|tag| tag.contains("created by cargo"))
}

fn fold(hasher: &mut blake3::Hasher, label: &str, value: &[u8]) {
    hasher.update(&(label.len() as u64).to_le_bytes());
    hasher.update(label.as_bytes());
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

/// Size and times of one entry, plus the inode where the platform has one.
pub(crate) fn fold_metadata_stamp(hasher: &mut blake3::Hasher, metadata: &std::fs::Metadata) {
    hasher.update(&metadata.len().to_le_bytes());
    for time in [metadata.modified().ok(), metadata.created().ok()] {
        let nanos = time
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(0, |d| d.as_nanos());
        hasher.update(&nanos.to_le_bytes());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        for value in [
            metadata.ctime() as u64,
            metadata.ctime_nsec() as u64,
            metadata.ino(),
        ] {
            hasher.update(&value.to_le_bytes());
        }
    }
}

/// The digest recorded at `memo`, when it was recorded under `stamp`.
pub(crate) fn memoised_digest(memo: &Path, stamp: &str) -> Option<String> {
    let memo = std::fs::read_to_string(memo).ok()?;
    let (recorded_stamp, digest) = memo.trim_end().split_once('\n')?;
    (recorded_stamp == stamp).then(|| digest.to_string())
}

/// Record `digest` at `memo` under `stamp`.
pub(crate) fn record_digest(memo: &Path, stamp: &str, digest: &str) {
    crate::probe_memo::write_atomic(memo, &format!("{stamp}\n{digest}"));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    const CARGO_TAG: &str = "Signature: 8a477f597d28d172789f06886806bc55\n\
         # This file is a cache directory tag created by cargo.\n";

    /// The stamp of one tree under the default rules, which hold no
    /// symlink, as a build script's declared directory must: its digest
    /// reads through links. `None` when the tree is larger than the budget,
    /// unreadable, or holds a symlink.
    fn tree_stamp(path: &Path, excluded: &[PathBuf], budget: usize) -> Option<TreeStamp> {
        let mut stamper = Stamper::new();
        let mut remaining = budget;
        (stamper.walk(path, excluded, StampRules::default(), &mut remaining) == WalkOutcome::Fits)
            .then(|| stamper.finish())
    }

    fn walk(root: &Path, rules: StampRules, budget: usize) -> (WalkOutcome, String, usize) {
        let mut stamper = Stamper::new();
        let mut left = budget;
        let outcome = stamper.walk(root, &[], rules, &mut left);
        (outcome, stamper.finish().digest, budget - left)
    }

    fn write(path: &Path, content: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    #[test]
    fn a_stamp_settles_one_window_after_its_newest_write() {
        let newest = UNIX_EPOCH + Duration::from_secs(1_000);
        let stamp = TreeStamp {
            digest: String::new(),
            newest,
        };
        assert!(stamp.settled_at(newest + TreeStamp::SETTLE));
        assert!(stamp.settled_at(newest + TreeStamp::SETTLE * 3));
        // Not a nanosecond: Windows keeps time in 100 ns ticks, and
        // subtracting less than one leaves the time as it was.
        assert!(!stamp.settled_at(newest + TreeStamp::SETTLE - Duration::from_millis(1)));
        assert!(
            !stamp.settled_at(newest - Duration::from_secs(1)),
            "a clock behind the write"
        );

        let dir = tempfile::tempdir().unwrap();
        write(&dir.path().join("a"), "a");
        let fresh = tree_stamp(dir.path(), &[], 10).unwrap();
        assert!(
            !fresh.settled_at(SystemTime::now()),
            "the walk keeps the newest write it saw"
        );
    }

    #[test]
    fn a_rewrite_changes_the_stamp_and_the_budget_counts_entries() {
        let dir = tempfile::tempdir().unwrap();
        write(&dir.path().join("sub/a"), "a");
        let (outcome, before, spent) = walk(dir.path(), StampRules::default(), 2);
        assert_eq!(
            (outcome, spent),
            (WalkOutcome::Fits, 2),
            "`sub` and `sub/a`"
        );
        assert_eq!(
            walk(dir.path(), StampRules::default(), 1).0,
            WalkOutcome::TooLarge
        );
        assert_eq!(walk(dir.path(), StampRules::default(), 2).1, before);

        let file = dir.path().join("sub/a");
        let old = filetime::FileTime::from_unix_time(1_000_000_000, 0);
        filetime::set_file_mtime(&file, old).unwrap();
        let aged = walk(dir.path(), StampRules::default(), 2).1;
        assert_ne!(aged, before, "a new modification time");
        std::fs::write(&file, "bb").unwrap();
        filetime::set_file_mtime(&file, old).unwrap();
        assert_ne!(
            walk(dir.path(), StampRules::default(), 2).1,
            aged,
            "a rewrite that keeps the modification time"
        );
        assert!(tree_stamp(&dir.path().join("missing"), &[], 2).is_none());
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_is_stamped_by_its_text_or_refuses_the_tree() {
        let dir = tempfile::tempdir().unwrap();
        write(&dir.path().join("a"), "a");
        write(&dir.path().join("b"), "b");
        std::os::unix::fs::symlink("a", dir.path().join("link")).unwrap();
        let text = StampRules {
            link_text: true,
            ..StampRules::default()
        };
        let (outcome, before, _) = walk(dir.path(), text, 10);
        assert_eq!(outcome, WalkOutcome::Fits);
        assert_eq!(
            walk(dir.path(), StampRules::default(), 10).0,
            WalkOutcome::Unreadable,
            "a digest that reads through links cannot be stamped"
        );
        assert!(tree_stamp(dir.path(), &[], 10).is_none());

        std::fs::remove_file(dir.path().join("link")).unwrap();
        std::os::unix::fs::symlink("b", dir.path().join("link")).unwrap();
        assert_ne!(walk(dir.path(), text, 10).1, before, "retargeted");
    }

    /// A name created and removed again directly under the root leaves every
    /// entry as it was, and only the root's own times show it.
    #[test]
    fn a_name_that_came_and_went_under_the_root_moves_its_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("a");
        write(&file, "a");
        let old = filetime::FileTime::from_unix_time(1_000_000_000, 0);
        filetime::set_file_mtime(&file, old).unwrap();
        filetime::set_file_mtime(dir.path(), old).unwrap();
        let root_too = StampRules {
            root_metadata: true,
            ..StampRules::default()
        };
        let stamp = |rules| {
            let mut stamper = Stamper::new();
            let mut left = 10;
            assert_eq!(
                stamper.walk(dir.path(), &[], rules, &mut left),
                WalkOutcome::Fits
            );
            assert_eq!(left, 9, "the root is no entry");
            stamper.finish()
        };
        let entries = stamp(StampRules::default()).digest;
        let with_root = stamp(root_too).digest;

        write(&dir.path().join("transient"), "");
        std::fs::remove_file(dir.path().join("transient")).unwrap();
        let now = SystemTime::now();
        let after = stamp(StampRules::default());
        assert_eq!(after.digest, entries);
        assert!(after.settled_at(now));
        let after = stamp(root_too);
        assert_ne!(after.digest, with_root);
        assert!(!after.settled_at(now), "the root's write is the newest");
    }

    /// A directory that cannot be listed, or an entry that cannot be stat'ed,
    /// refuses the tree unless the rules stamp it as unreadable.
    #[cfg(unix)]
    #[test]
    fn what_cannot_be_read_is_stamped_as_unreadable_or_refuses_the_tree() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        write(&dir.path().join("src/lib.rs"), "");
        write(&dir.path().join("data/PG_VERSION"), "16");
        let data = dir.path().join("data");
        struct Readable<'a>(&'a Path);
        impl Drop for Readable<'_> {
            fn drop(&mut self) {
                let _ = std::fs::set_permissions(self.0, std::fs::Permissions::from_mode(0o755));
            }
        }
        let _restore = Readable(&data);
        let mode = |mode| std::fs::set_permissions(&data, std::fs::Permissions::from_mode(mode));
        mode(0o000).unwrap();
        if std::fs::read_dir(&data).is_ok() {
            eprintln!("skipped: this user reads past permissions");
            return;
        }
        let unreadable = StampRules {
            unreadable_entries: true,
            ..StampRules::default()
        };
        assert_eq!(
            walk(dir.path(), StampRules::default(), 10).0,
            WalkOutcome::Unreadable
        );
        let (outcome, unlisted, spent) = walk(dir.path(), unreadable, 10);
        assert_eq!(
            (outcome, spent),
            (WalkOutcome::Fits, 3),
            "`src`, its file, `data`"
        );

        // Listed, but its entries cannot be stat'ed.
        mode(0o444).unwrap();
        assert_eq!(
            walk(dir.path(), StampRules::default(), 10).0,
            WalkOutcome::Unreadable
        );
        let (outcome, unstated, spent) = walk(dir.path(), unreadable, 10);
        assert_eq!((outcome, spent), (WalkOutcome::Fits, 4));
        assert_ne!(unstated, unlisted);

        mode(0o755).unwrap();
        let (outcome, readable, _) = walk(dir.path(), unreadable, 10);
        assert_eq!(outcome, WalkOutcome::Fits);
        assert_ne!(readable, unstated);
        assert_eq!(
            walk(dir.path(), StampRules::default(), 10).0,
            WalkOutcome::Fits
        );
    }

    /// Only the files and links directly under the root count, and a
    /// directory there counts neither by itself nor by what it holds.
    #[test]
    fn a_top_files_walk_stamps_the_files_under_the_root() {
        let dir = tempfile::tempdir().unwrap();
        write(&dir.path().join(".env"), "A=1");
        write(&dir.path().join("docs/a.md"), "");
        let top = StampRules {
            top_files_only: true,
            ..StampRules::default()
        };
        let (outcome, before, spent) = walk(dir.path(), top, 10);
        assert_eq!((outcome, spent), (WalkOutcome::Fits, 1), "`.env` alone");
        write(&dir.path().join("docs/b.md"), "");
        write(&dir.path().join("pkg/src/lib.rs"), "");
        assert_eq!(walk(dir.path(), top, 10).1, before);
        write(&dir.path().join("Cargo.toml"), "");
        assert_ne!(walk(dir.path(), top, 10).1, before);
    }

    /// Below the root, a directory Cargo tagged counts by its tag alone. The
    /// root itself and a directory with another tool's tag are walked.
    #[test]
    fn a_cargo_build_dir_is_stamped_by_its_tag_alone() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("w");
        write(&root.join("src/lib.rs"), "");
        write(&root.join("old-target/CACHEDIR.TAG"), CARGO_TAG);
        write(&root.join("old-target/debug/a"), "");
        let skip = StampRules {
            skip_build_dirs: true,
            ..StampRules::default()
        };
        let (outcome, before, spent) = walk(&root, skip, 10);
        assert_eq!(outcome, WalkOutcome::Fits);
        assert_eq!(spent, 4, "`src`, `src/lib.rs`, `old-target` and its tag");
        assert_eq!(walk(&root, StampRules::default(), 10).2, 6);

        write(&root.join("old-target/debug/b"), "");
        assert_eq!(walk(&root, skip, 10).1, before, "build output");
        write(&root.join("old-target/CACHEDIR.TAG"), "Signature: x\n");
        assert_eq!(walk(&root, skip, 10).2, 7, "another tool's tag is walked");

        write(&root.join("CACHEDIR.TAG"), CARGO_TAG);
        assert_eq!(walk(&root, skip, 10).2, 8, "the root is always walked");
    }

    #[test]
    fn only_cargos_tag_marks_a_build_dir() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!holds_cargo_build_tag(dir.path()));
        write(&dir.path().join("CACHEDIR.TAG"), "Signature: x\n");
        assert!(!holds_cargo_build_tag(dir.path()));
        write(&dir.path().join("CACHEDIR.TAG"), CARGO_TAG);
        assert!(holds_cargo_build_tag(dir.path()));
        let listed = |path: &Path| {
            let mut entries: Vec<_> = std::fs::read_dir(path)
                .unwrap()
                .collect::<std::io::Result<_>>()
                .unwrap();
            keep_only_build_tag(&mut entries);
            entries.len()
        };
        write(&dir.path().join("x"), "");
        assert_eq!(listed(dir.path()), 1);
        let directory_tag = dir.path().join("dirtag");
        std::fs::create_dir_all(directory_tag.join("CACHEDIR.TAG")).unwrap();
        assert!(!holds_cargo_build_tag(&directory_tag));
        write(&directory_tag.join("y"), "");
        assert_eq!(listed(&directory_tag), 2, "a directory named like the tag");
        #[cfg(unix)]
        {
            let linked = dir.path().join("linked");
            std::fs::create_dir_all(&linked).unwrap();
            std::os::unix::fs::symlink(
                dir.path().join("CACHEDIR.TAG"),
                linked.join("CACHEDIR.TAG"),
            )
            .unwrap();
            write(&linked.join("z"), "");
            assert!(
                !holds_cargo_build_tag(&linked),
                "a link to a tag is not one"
            );
            assert_eq!(listed(&linked), 2);
        }
    }

    #[test]
    fn a_memoised_digest_needs_its_stamp() {
        let dir = tempfile::tempdir().unwrap();
        let memo = dir.path().join("memo/tree");
        assert_eq!(memoised_digest(&memo, "s1"), None);
        record_digest(&memo, "s1", "d1");
        assert_eq!(memoised_digest(&memo, "s1").as_deref(), Some("d1"));
        assert_eq!(memoised_digest(&memo, "s2"), None);
    }
}
