//! Process-held locks that keep target cleanup out of running Cargo commands.
//!
//! A command leases the directories it may use: its workspace, its target and
//! build directories, or a test binary's directory. Cleanup reserves one
//! target at a time, so a long build or a test run that lasts for days keeps
//! its own targets and no one else's. The locks are multi-granularity locks
//! over the path tree: a lease takes a shared `use` lock on each of its paths
//! and a shared `intent` lock on each of their ancestors; a reservation takes
//! an exclusive `use` lock on the target and on each of its ancestors, and an
//! exclusive `intent` lock on the target. A lease and a reservation conflict
//! exactly when one path contains the other. A command whose directories
//! cannot be worked out leases everything through the one cache-wide lock,
//! which every reservation also takes. Platforms where whole-file locks do
//! not interoperate with byte locks keep every lease cache-wide.

mod guard;

use guard::{Held, Region};

use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs::{File, OpenOptions, TryLockError};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// These platforms can protect a scoped command from both new and old cleanup.
pub(crate) const SCOPED_LOCKS: bool = cfg!(any(target_os = "macos", windows));

/// The directories a command may read or write build output in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Scope {
    /// Any target: the command's directories are unknown.
    Everything,
    /// Targets that contain, or lie inside, one of these normalized paths.
    Within(Vec<PathBuf>),
}

impl Scope {
    /// The scope of `paths`, normalized; no paths at all is [`Scope::Everything`].
    pub(crate) fn within(paths: impl IntoIterator<Item = PathBuf>) -> Self {
        let Some(paths) = paths
            .into_iter()
            .map(|path| normalize(&path))
            .collect::<Option<BTreeSet<_>>>()
        else {
            return Self::Everything;
        };
        if paths.is_empty() {
            return Self::Everything;
        }
        // A workspace already covers its target and build directories. Keep
        // redirected directories when normalization locates them elsewhere.
        let roots = paths
            .iter()
            .filter(|path| {
                !path
                    .ancestors()
                    .skip(1)
                    .any(|parent| paths.contains(parent))
            })
            .cloned()
            .collect();
        Self::Within(roots)
    }

    /// Whether removing `target` could take something this scope uses.
    pub(crate) fn covers(&self, target: &Path) -> bool {
        match self {
            Self::Everything => true,
            Self::Within(paths) => {
                let Some(target) = normalize(target) else {
                    return true;
                };
                let target = comparison_path(&target);
                paths
                    .iter()
                    .map(|path| comparison_path(path))
                    .any(|path| path.starts_with(&target) || target.starts_with(path))
            }
        }
    }
}

/// `path` made absolute, with every existing prefix resolved the way the
/// filesystem names it, so a lease and a reservation spell one directory the
/// same: `/tmp` and `/private/tmp` on macOS, or a symlinked checkout.
fn normalize(path: &Path) -> Option<PathBuf> {
    let absolute = std::path::absolute(path).ok()?;
    let mut existing = absolute.as_path();
    let mut missing = Vec::new();
    loop {
        match std::fs::canonicalize(existing) {
            Ok(real) => {
                let path = missing
                    .into_iter()
                    .rev()
                    .fold(real, |path, name| path.join(name));
                // Bytewise keys cannot represent every Unicode equivalence on a
                // case-insensitive volume. A newline cannot round-trip in a holder record.
                // APFS exposes the boot data volume under a second name.
                // realpath preserves both spellings of e.g. /Users/alice.
                #[cfg(target_os = "macos")]
                if comparison_path(&path).starts_with("/system/volumes/data") {
                    return None;
                }
                let name = path.as_os_str().as_encoded_bytes();
                return ((!SCOPED_LOCKS || name.is_ascii()) && !name.contains(&b'\n'))
                    .then_some(path);
            }
            Err(error) => require_not_found(error)?,
        }
        // A dangling symlink already names another path. Wait for a known
        // target under the global guard rather than guessing where it will lead.
        require_not_found(std::fs::symlink_metadata(existing).err()?)?;
        let name = existing.file_name()?;
        #[cfg(windows)]
        if windows_name_may_alias(name) {
            return None;
        }
        missing.push(name.to_owned());
        existing = existing.parent()?;
    }
}

fn require_not_found(error: io::Error) -> Option<()> {
    (error.kind() == io::ErrorKind::NotFound).then_some(())
}

#[cfg(any(windows, test))]
fn windows_name_may_alias(name: &OsStr) -> bool {
    let name = name.as_encoded_bytes();
    name.ends_with(b".") || name.ends_with(b" ") || name.contains(&b'~')
}

fn comparison_path(path: &Path) -> PathBuf {
    if SCOPED_LOCKS {
        // Both systems support case-insensitive volumes. Existing and future
        // paths must hash identically even before their on-disk case is known.
        path.as_os_str().to_ascii_lowercase().into()
    } else {
        path.to_owned()
    }
}

/// Unlock explicitly before closing: a concurrent fork can briefly inherit
/// the descriptors, even though they close on exec. A shared lease also
/// leaves a record naming its command, so cleanup can say what it waits for.
#[derive(Default)]
pub(crate) struct Lease {
    files: Vec<Held>,
    record: Option<PathBuf>,
}

impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(record) = &self.record {
            let _ = std::fs::remove_file(record);
        }
    }
}

/// A running command that holds a shared lease, from its record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Holder {
    pub(crate) process: crate::process_identity::ProcessIdentity,
    /// What the command is, such as `cargo run` or a test binary's name.
    pub(crate) what: String,
    /// When it took the lease, in seconds since the epoch.
    pub(crate) since: u64,
    /// The directories it leased. Records from before scopes lease everything.
    pub(crate) scope: Scope,
}

/// The exclusive lease cleanup holds on one target. While it is held, a file
/// in the cache directory names this process to the commands waiting for it.
/// The file goes before the locks do, so it never takes the next holder's
/// with it.
pub(crate) struct Reservation {
    holder: PathBuf,
    _lease: Lease,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.holder);
    }
}

fn open_lock(path: &Path) -> io::Result<File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
}

/// The lock a command whose directories are unknown shares, and every
/// reservation takes. Older Kache releases share it for every command.
fn everything_lock(cache_dir: &Path) -> PathBuf {
    cache_dir.join("target-use.lock")
}

fn locks_dir(cache_dir: &Path) -> PathBuf {
    cache_dir.join("target-use.d")
}

/// The `use` (`u`) or `intent` (`i`) lock of one normalized path.
fn path_lock(cache_dir: &Path, kind: char, path: &Path) -> PathBuf {
    let hash = blake3::hash(comparison_path(path).as_os_str().as_encoded_bytes()).to_hex();
    locks_dir(cache_dir).join(format!("{kind}-{}.lock", &hash.as_str()[..32]))
}

/// The locks a lease on `scope` shares, in one global order.
fn lease_locks(cache_dir: &Path, paths: &[PathBuf]) -> BTreeSet<PathBuf> {
    paths
        .iter()
        .flat_map(|path| {
            std::iter::once(path_lock(cache_dir, 'u', path)).chain(
                path.ancestors()
                    .skip(1)
                    .map(|ancestor| path_lock(cache_dir, 'i', ancestor)),
            )
        })
        .collect()
}

/// The locks a reservation on the normalized target takes after the legacy guard.
fn reservation_locks(cache_dir: &Path, target: &Path) -> impl Iterator<Item = PathBuf> {
    target
        .ancestors()
        .map(|ancestor| path_lock(cache_dir, 'u', ancestor))
        .chain(std::iter::once(path_lock(cache_dir, 'i', target)))
}

/// Whether `file` is still the file at `path`. A pruned lock file is gone,
/// and a lock taken on it guards nothing.
fn still_named(file: &File, path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        match (file.metadata(), std::fs::metadata(path)) {
            (Ok(held), Ok(named)) => (held.dev(), held.ino()) == (named.dev(), named.ino()),
            _ => false,
        }
    }
    #[cfg(not(unix))]
    {
        // These hosts never unlink lock files.
        let _ = (file, path);
        true
    }
}

// Stop if concurrent pruning repeatedly removes the file we just opened.
const MAX_LOCK_OPENS: usize = 8;

/// Share the lock at `path`, waiting for a cleanup that holds it. `said`
/// keeps the notice to one line per lease.
fn lock_shared(
    path: &Path,
    region: Region,
    cache_dir: &Path,
    notice: &mut dyn Write,
    said: &mut bool,
) -> io::Result<Held> {
    for _ in 0..MAX_LOCK_OPENS {
        let file = open_lock(path)?;
        match region.try_shared(&file) {
            Ok(()) => {}
            // One write, so the line stays whole beside other processes' output.
            Err(TryLockError::WouldBlock) => {
                if !std::mem::replace(said, true) {
                    let _ = notice.write_all(waiting(cache_dir).as_bytes());
                }
                region.shared(&file)?;
            }
            Err(TryLockError::Error(error)) => return Err(error),
        }
        let held = Held { file, region };
        if still_named(&held.file, path) {
            return Ok(held);
        }
    }
    Err(io::Error::other("target-use lock file repeatedly replaced"))
}

/// Take the lock at `path` without waiting: `None` when someone holds it.
fn try_lock_exclusive(path: &Path, region: Region) -> io::Result<Option<Held>> {
    for _ in 0..MAX_LOCK_OPENS {
        let file = open_lock(path)?;
        match region.try_exclusive(&file) {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Ok(None),
            Err(TryLockError::Error(error)) => return Err(error),
        }
        let held = Held { file, region };
        if still_named(&held.file, path) {
            return Ok(Some(held));
        }
    }
    Err(io::Error::other("target-use lock file repeatedly replaced"))
}

fn holder_record(cache_dir: &Path) -> PathBuf {
    cache_dir.join("target-use.holder")
}

/// One file per shared lease, named `<pid>-<nanos>-<start>`: its process,
/// the time it was taken, and when its process started where the platform
/// reports that. Records from before starts were kept end at `<nanos>`.
fn holders_dir(cache_dir: &Path) -> PathBuf {
    cache_dir.join("target-use.holders")
}

/// The record's text: `<secs>\t<what>`, then one leased path per line.
/// Records with no path lines lease everything, as older ones all did.
fn record_holder(cache_dir: &Path, scope: &Scope, what: &str) -> Option<PathBuf> {
    let dir = holders_dir(cache_dir);
    std::fs::create_dir_all(&dir).ok()?;
    let since = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let process = crate::process_identity::ProcessIdentity::current();
    let taken = format!("{}-{}", process.pid(), since.as_nanos());
    let record = dir.join(match process.start() {
        Some(start) => format!("{taken}-{start}"),
        None => taken,
    });
    // A tab cannot appear in the time, so `what` keeps any other characters
    // but the line breaks that separate the paths.
    let mut text = format!("{}\t{}", since.as_secs(), what.replace(['\n', '\r'], " "));
    if let Scope::Within(paths) = scope {
        for path in paths {
            text.push('\n');
            text.push_str(&path.to_string_lossy());
        }
    }
    std::fs::write(&record, text).ok()?;
    Some(record)
}

fn parse_holder(name: &str, text: &str) -> Option<Holder> {
    use crate::process_identity::ProcessIdentity;
    let mut parts = name.split('-');
    let pid = parts.next()?.parse().ok()?;
    let _taken: u128 = parts.next()?.parse().ok()?;
    let process = match parts.next() {
        Some(start) => ProcessIdentity::started_at(pid, start.parse().ok()?),
        None => ProcessIdentity::unrecorded(pid),
    };
    if parts.next().is_some() {
        return None;
    }
    let (since, rest) = text.split_once('\t')?;
    let mut lines = rest.split('\n');
    let what = lines.next()?.trim().to_owned();
    let paths: Vec<PathBuf> = lines
        .filter(|line| !line.is_empty())
        .map(PathBuf::from)
        .collect();
    Some(Holder {
        process,
        what,
        since: since.parse().ok()?,
        scope: if paths.is_empty() {
            Scope::Everything
        } else {
            Scope::Within(paths)
        },
    })
}

/// The commands holding shared leases now, oldest first. A record whose
/// process is gone is removed, even when its number now names another.
pub(crate) fn holders(cache_dir: &Path) -> Vec<Holder> {
    let Ok(entries) = std::fs::read_dir(holders_dir(cache_dir)) else {
        return Vec::new();
    };
    let mut holders = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(holder) = std::fs::read_to_string(entry.path())
            .ok()
            .and_then(|text| parse_holder(&name, &text))
        else {
            continue;
        };
        if holder.process.is_running() {
            holders.push(holder);
        } else {
            let _ = std::fs::remove_file(entry.path());
        }
    }
    holders.sort_by_key(|holder| (holder.since, holder.process.pid()));
    holders
}

/// Who keeps cleanup from deleting `target` now: `None` when nothing does,
/// otherwise the commands whose leases cover it (empty when none left a
/// record, such as another cleanup or an older Kache). The probe takes the
/// locks for an instant and records nothing.
pub(crate) fn blockers(cache_dir: &Path, target: &Path) -> Option<Vec<Holder>> {
    match reserve(cache_dir, target) {
        Ok(Some(_)) => None,
        Ok(None) | Err(_) => Some(holders_covering(cache_dir, target)),
    }
}

/// The commands whose leases cover `target`, oldest first.
pub(crate) fn holders_covering(cache_dir: &Path, target: &Path) -> Vec<Holder> {
    holders(cache_dir)
        .into_iter()
        .filter(|holder| holder.scope.covers(target))
        .collect()
}

/// Hold while Cargo or a target runner may still use build output in any
/// target. See [`shared_in`].
#[cfg(test)]
pub(crate) fn shared(cache_dir: &Path, what: &str, notice: &mut dyn Write) -> io::Result<Lease> {
    shared_in(cache_dir, &Scope::Everything, what, notice)
}

/// Hold while Cargo or a target runner may still use build output in
/// `scope`. While cleanup holds one of its locks, `notice` gets one line
/// saying so before the wait. `what` names the command to a cleanup that has
/// to skip targets for it.
pub(crate) fn shared_in(
    cache_dir: &Path,
    scope: &Scope,
    what: &str,
    notice: &mut dyn Write,
) -> io::Result<Lease> {
    let mut lease = Lease::default();
    let mut said = false;
    let scope = if SCOPED_LOCKS {
        scope
    } else {
        &Scope::Everything
    };
    let guard = match scope {
        Scope::Everything => Region::Whole,
        Scope::Within(_) => Region::COMMAND,
    };
    lease.files.push(lock_shared(
        &everything_lock(cache_dir),
        guard,
        cache_dir,
        notice,
        &mut said,
    )?);
    if let Scope::Within(paths) = scope {
        for lock in lease_locks(cache_dir, paths) {
            lease.files.push(lock_shared(
                &lock,
                Region::Whole,
                cache_dir,
                notice,
                &mut said,
            )?);
        }
    }
    lease.record = record_holder(cache_dir, scope, what);
    Ok(lease)
}

/// [`shared_in`] for a command started with `args`. The line goes to stderr
/// unless a `-q` or `--quiet` before any `--` asked Cargo to be quiet: Cargo
/// then hides its own lock waits, and passes a test binary `--quiet`.
pub(crate) fn shared_for<'a>(
    cache_dir: &Path,
    scope: &Scope,
    what: &str,
    args: impl IntoIterator<Item = &'a OsStr>,
) -> io::Result<Lease> {
    if quiet(args) {
        shared_in(cache_dir, scope, what, &mut io::sink())
    } else {
        shared_in(cache_dir, scope, what, &mut io::stderr())
    }
}

fn quiet<'a>(args: impl IntoIterator<Item = &'a OsStr>) -> bool {
    args.into_iter()
        .take_while(|arg| *arg != "--")
        .any(|arg| arg == "-q" || arg == "--quiet")
}

/// What a command waiting for cleanup says, with the cleanup's process when
/// it left a record.
fn waiting(cache_dir: &Path) -> String {
    let pid = std::fs::read_to_string(holder_record(cache_dir))
        .ok()
        .and_then(|text| text.trim().parse::<u32>().ok());
    match pid {
        Some(pid) => format!("kache: waiting for target cleanup (pid {pid}) to finish\n"),
        None => "kache: waiting for target cleanup to finish\n".to_owned(),
    }
}

/// Every lock a reservation on the normalized `target` needs, or `None`
/// when a lease that covers it holds one.
fn reserve(cache_dir: &Path, target: &Path) -> io::Result<Option<Lease>> {
    let mut lease = Lease::default();
    let target = if SCOPED_LOCKS {
        normalize(target)
    } else {
        None
    };
    let region = if target.is_some() {
        Region::CLEANUP
    } else {
        Region::Whole
    };
    let Some(guard) = try_lock_exclusive(&everything_lock(cache_dir), region)? else {
        return Ok(None);
    };
    lease.files.push(guard);
    if let Some(target) = target {
        for lock in reservation_locks(cache_dir, &target) {
            let Some(file) = try_lock_exclusive(&lock, Region::Whole)? else {
                return Ok(None);
            };
            lease.files.push(file);
        }
    }
    Ok(Some(lease))
}

/// Reserve deleting `target`, or changing build output inside it, without
/// waiting for a running command.
pub(crate) fn try_exclusive(cache_dir: &Path, target: &Path) -> io::Result<Option<Reservation>> {
    let Some(lease) = reserve(cache_dir, target)? else {
        return Ok(None);
    };
    let holder = holder_record(cache_dir);
    let _ = std::fs::write(&holder, std::process::id().to_string());
    Ok(Some(Reservation {
        holder,
        _lease: lease,
    }))
}

/// Remove the per-path lock files nobody holds, so leases of directories
/// that come and go (temporary checkouts) leave nothing behind. A lease that
/// opened one before it went sees the file gone once it locks, and opens the
/// path again.
#[cfg(unix)]
pub(crate) fn prune_locks(cache_dir: &Path) {
    let Ok(entries) = std::fs::read_dir(locks_dir(cache_dir)) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(file) = OpenOptions::new().read(true).write(true).open(&path) else {
            continue;
        };
        if file.try_lock().is_ok() {
            if still_named(&file, &path) {
                let _ = std::fs::remove_file(&path);
            }
            let _ = file.unlock();
        }
    }
}

/// Windows refuses to reopen a file whose deletion is pending, so a lease
/// there could fail rather than retry; its lock files stay.
#[cfg(not(unix))]
pub(crate) fn prune_locks(_: &Path) {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Hands each write to the test as it happens.
    struct Said(std::sync::mpsc::Sender<Vec<u8>>);

    impl Write for Said {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let _ = self.0.send(bytes.to_vec());
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn naming(pid: u32) -> String {
        format!("kache: waiting for target cleanup (pid {pid}) to finish\n")
    }

    /// A cache directory and, beside it, a tree of workspaces.
    struct Fixture {
        _dir: tempfile::TempDir,
        cache: PathBuf,
        root: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let cache = dir.path().join("cache");
            let root = normalize(dir.path()).unwrap().join("work");
            std::fs::create_dir_all(&root).unwrap();
            Self {
                _dir: dir,
                cache,
                root,
            }
        }

        fn at(&self, path: &str) -> PathBuf {
            self.root.join(path)
        }

        fn within(&self, paths: &[&str]) -> Scope {
            Scope::within(paths.iter().map(|path| self.at(path)))
        }

        fn free(&self, target: &str) -> bool {
            try_exclusive(&self.cache, &self.at(target))
                .unwrap()
                .is_some()
        }
    }

    #[test]
    fn a_shared_lease_names_its_command_until_it_ends() {
        let fx = Fixture::new();
        let target = fx.at("ws/target");
        assert_eq!(blockers(&fx.cache, &target), None);
        let run = shared(&fx.cache, "cargo run", &mut io::sink()).unwrap();
        let named = holders(&fx.cache);
        assert_eq!(named.len(), 1);
        assert_eq!(
            (named[0].process.pid(), named[0].what.as_str()),
            (std::process::id(), "cargo run")
        );
        assert_eq!(named[0].scope, Scope::Everything);
        assert_eq!(blockers(&fx.cache, &target), Some(named));
        drop(run);
        assert!(holders(&fx.cache).is_empty());
        assert_eq!(blockers(&fx.cache, &target), None);
        // The probe leaves no record of its own behind.
        assert!(fx.free("ws/target"));
    }

    #[test]
    fn a_lease_keeps_the_targets_it_covers_and_no_others() {
        let fx = Fixture::new();
        let lease = shared_in(
            &fx.cache,
            &fx.within(&["ws"]),
            "cargo test",
            &mut io::sink(),
        )
        .unwrap();
        // Inside the workspace, the workspace itself, and what contains it.
        assert!(!fx.free("ws/target"));
        assert!(!fx.free("ws/target/check"));
        assert!(!fx.free("ws"));
        assert!(!fx.free(""));
        // A sibling workspace, and one whose name only starts the same.
        assert_eq!(fx.free("other/target"), SCOPED_LOCKS);
        assert_eq!(fx.free("ws2/target"), SCOPED_LOCKS);
        drop(lease);
        assert!(fx.free("ws/target"));
    }

    #[test]
    fn a_lease_inside_a_target_keeps_that_target() {
        let fx = Fixture::new();
        let lease = shared_in(
            &fx.cache,
            &fx.within(&["ws/target/debug/deps"]),
            "hub",
            &mut io::sink(),
        )
        .unwrap();
        assert!(!fx.free("ws/target"));
        assert!(!fx.free("ws/target/debug"));
        assert_eq!(fx.free("ws/target/release"), SCOPED_LOCKS);
        assert_eq!(fx.free("ws/crates/target"), SCOPED_LOCKS);
        drop(lease);
    }

    #[test]
    fn a_lease_of_everything_keeps_every_target() {
        let fx = Fixture::new();
        let lease = shared(&fx.cache, "cargo build", &mut io::sink()).unwrap();
        assert!(!fx.free("ws/target"));
        assert!(!fx.free("other/target"));
        drop(lease);
        assert!(fx.free("other/target"));
    }

    #[test]
    fn blockers_name_only_the_commands_that_cover_the_target() {
        let fx = Fixture::new();
        let here = shared_in(
            &fx.cache,
            &fx.within(&["ws"]),
            "cargo test",
            &mut io::sink(),
        )
        .unwrap();
        let there = shared_in(
            &fx.cache,
            &fx.within(&["other"]),
            "cargo build",
            &mut io::sink(),
        )
        .unwrap();
        let named = |target: &str| {
            blockers(&fx.cache, &fx.at(target))
                .map(|holders| holders.into_iter().map(|h| h.what).collect::<BTreeSet<_>>())
        };
        if SCOPED_LOCKS {
            assert_eq!(
                named("ws/target"),
                Some(BTreeSet::from(["cargo test".to_owned()]))
            );
            assert_eq!(
                named("other/target"),
                Some(BTreeSet::from(["cargo build".to_owned()]))
            );
            assert_eq!(named("third/target"), None);
        } else {
            let all = Some(BTreeSet::from([
                "cargo build".to_owned(),
                "cargo test".to_owned(),
            ]));
            assert_eq!(named("ws/target"), all);
            assert_eq!(named("other/target"), all);
            assert_eq!(named("third/target"), all);
        }
        drop((here, there));
        assert_eq!(named("ws/target"), None);
    }

    #[test]
    fn whole_file_locks_from_old_versions_protect_commands_and_cleanup_in_both_directions() {
        let fx = Fixture::new();
        let legacy = open_lock(&everything_lock(&fx.cache)).unwrap();
        let scope = fx.within(&["one"]);
        let modern = shared_in(&fx.cache, &scope, "new cargo", &mut io::sink()).unwrap();
        assert!(matches!(legacy.try_lock(), Err(TryLockError::WouldBlock)));
        // An old Cargo command can run alongside a new one, but it blocks
        // cleanup in *every* workspace, even one outside the new scope.
        legacy.try_lock_shared().unwrap();
        assert!(!fx.free("two"));
        legacy.unlock().unwrap();
        assert_eq!(fx.free("two"), SCOPED_LOCKS);
        drop(modern);

        let cleaning = try_exclusive(&fx.cache, &fx.at("one")).unwrap().unwrap();
        assert!(matches!(
            legacy.try_lock_shared(),
            Err(TryLockError::WouldBlock)
        ));
        assert!(matches!(legacy.try_lock(), Err(TryLockError::WouldBlock)));
        assert!(
            !fx.free("two"),
            "only one cleanup may own the holder record"
        );
        drop(cleaning);

        legacy.try_lock().unwrap();
        let (said, heard) = std::sync::mpsc::channel();
        let cache = fx.cache.clone();
        let command = std::thread::spawn(move || {
            shared_in(&cache, &scope, "new cargo", &mut Said(said)).unwrap()
        });
        assert_eq!(
            heard
                .recv_timeout(Duration::from_secs(10))
                .unwrap()
                .as_slice(),
            b"kache: waiting for target cleanup to finish\n"
        );
        assert!(!command.is_finished());
        legacy.unlock().unwrap();
        let lease = command.join().unwrap();
        assert!(!fx.free("one"));
        drop(lease);
        assert!(fx.free("one"));
    }

    #[test]
    fn a_lock_error_keeps_the_dry_run_and_cleanup_from_promising_removal() {
        let fx = Fixture::new();
        std::fs::write(&fx.cache, b"not a directory").unwrap();
        let target = fx.at("one");
        assert!(try_exclusive(&fx.cache, &target).is_err());
        assert_eq!(blockers(&fx.cache, &target), Some(Vec::new()));
        assert!(shared_in(&fx.cache, &fx.within(&["one"]), "build", &mut io::sink()).is_err());
    }

    #[cfg(not(any(target_os = "macos", windows)))]
    #[test]
    fn the_global_fallback_needs_no_path_lock_files() {
        let fx = Fixture::new();
        let scoped = shared_in(
            &fx.cache,
            &fx.within(&["one"]),
            "cargo build",
            &mut io::sink(),
        )
        .unwrap();
        assert!(!fx.free("two"));
        assert!(!locks_dir(&fx.cache).exists());
        drop(scoped);
        assert!(fx.free("two"));
        assert!(!locks_dir(&fx.cache).exists());
    }

    #[cfg(any(target_os = "macos", windows))]
    #[test]
    fn case_aliases_of_future_paths_overlap_and_name_the_holder() {
        let fx = Fixture::new();
        let scoped = shared_in(
            &fx.cache,
            &fx.within(&["work/Target"]),
            "cargo build",
            &mut io::sink(),
        )
        .unwrap();
        assert_eq!(
            blockers(&fx.cache, &fx.at("WORK/target/deps"))
                .unwrap()
                .len(),
            1
        );
        assert!(fx.free("worker/target"));
        assert_eq!(fx.within(&["work/é"]), Scope::Everything);
        assert!(
            !fx.free("other/é"),
            "an ambiguous cleanup requires the whole guard"
        );
        drop(scoped);
        assert!(fx.free("WORK/target/deps"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn a_boot_data_volume_alias_takes_the_global_guard() {
        let alias = PathBuf::from("/System/Volumes/Data/private/tmp");
        assert_eq!(Scope::within([alias]), Scope::Everything);
    }

    #[cfg(unix)]
    #[test]
    fn a_dangling_symlink_or_unresolved_parent_requires_the_global_guard() {
        let fx = Fixture::new();
        std::os::unix::fs::symlink(fx.at("future"), fx.at("link")).unwrap();
        assert_eq!(fx.within(&["link/target"]), Scope::Everything);
        assert_eq!(fx.within(&["unmade/../outside"]), Scope::Everything);
    }

    #[test]
    fn scopes_cover_paths_that_contain_or_lie_inside_theirs() {
        let fx = Fixture::new();
        assert_eq!(Scope::within(Vec::new()), Scope::Everything);
        assert!(Scope::Everything.covers(&fx.at("anything")));
        let scope = fx.within(&["ws/target/debug", "ws", "ws", "out/target"]);
        let Scope::Within(paths) = &scope else {
            panic!("{scope:?}")
        };
        assert_eq!(paths, &[fx.at("out/target"), fx.at("ws")]);
        assert!(scope.covers(&fx.at("ws/target")));
        assert!(scope.covers(&fx.at("out")));
        assert!(scope.covers(&fx.at("out/target/debug")));
        assert!(!scope.covers(&fx.at("out/other")));
        assert!(!scope.covers(&fx.at("ws2")));
    }

    #[test]
    fn an_unreadable_prefix_is_not_a_missing_directory() {
        assert_eq!(
            require_not_found(io::ErrorKind::PermissionDenied.into()),
            None
        );
        assert_eq!(require_not_found(io::ErrorKind::NotFound.into()), Some(()));
    }

    #[test]
    fn normalized_names_fit_holder_records_and_case_comparison() {
        let fx = Fixture::new();
        assert_eq!(normalize(&fx.at("future\nname")), None);
        assert_eq!(normalize(&fx.at("future")), Some(fx.at("future")));
        assert_eq!(normalize(&fx.at("résultat")).is_some(), !SCOPED_LOCKS);
    }

    #[test]
    fn future_windows_aliases_require_the_global_guard() {
        for name in ["target.", "target ", "TARGE~1"] {
            assert!(windows_name_may_alias(OsStr::new(name)), "{name}");
        }
        for name in ["target", "control.target", "control target"] {
            assert!(!windows_name_may_alias(OsStr::new(name)), "{name}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn paths_are_compared_as_the_filesystem_names_them() {
        let fx = Fixture::new();
        std::fs::create_dir_all(fx.at("real/ws")).unwrap();
        std::os::unix::fs::symlink(fx.at("real"), fx.at("link")).unwrap();
        assert_eq!(
            normalize(&fx.at("link/ws/target")).unwrap(),
            fx.at("real/ws/target")
        );
        assert_eq!(normalize(&fx.at("link/ws")).unwrap(), fx.at("real/ws"));
        let lease = shared_in(
            &fx.cache,
            &fx.within(&["link/ws"]),
            "cargo build",
            &mut io::sink(),
        )
        .unwrap();
        assert!(!fx.free("real/ws/target"));
        drop(lease);
    }

    #[test]
    fn a_record_keeps_the_leased_paths() {
        let fx = Fixture::new();
        let scope = fx.within(&["ws", "out/target"]);
        let lease = shared_in(&fx.cache, &scope, "cargo\nbuild", &mut io::sink()).unwrap();
        let named = holders(&fx.cache);
        assert_eq!(named.len(), 1);
        assert_eq!(named[0].what, "cargo build");
        assert_eq!(
            named[0].scope,
            if SCOPED_LOCKS {
                scope
            } else {
                Scope::Everything
            }
        );
        drop(lease);
    }

    #[test]
    fn a_record_left_by_a_gone_process_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let records = holders_dir(dir.path());
        std::fs::create_dir_all(&records).unwrap();
        // PID 1 never names a lease holder, and garbage parses as nothing.
        std::fs::write(records.join("1-5"), "5\tcargo run").unwrap();
        std::fs::write(records.join("garbage"), "no tab").unwrap();
        assert!(holders(dir.path()).is_empty());
        assert!(!records.join("1-5").exists());
        assert_eq!(
            parse_holder("42-9", "17\tcargo test --all"),
            Some(Holder {
                process: crate::process_identity::ProcessIdentity::unrecorded(42),
                what: "cargo test --all".to_owned(),
                since: 17,
                scope: Scope::Everything,
            })
        );
        assert_eq!(parse_holder("x-9", "17\tcargo"), None);
        for name in ["42", "42-", "42-invalid", "42-9-invalid", "42-9-1-extra"] {
            assert_eq!(parse_holder(name, "17\tcargo"), None, "{name}");
        }
        assert_eq!(parse_holder("42-9", "invalid\tcargo"), None);
        assert_eq!(parse_holder("42-9", "17 no-tab"), None);
        assert_eq!(
            parse_holder("42-9-23", "17\tcargo\targument\n/a/ws\n\n/b"),
            Some(Holder {
                process: crate::process_identity::ProcessIdentity::started_at(42, 23),
                what: "cargo\targument".to_owned(),
                since: 17,
                scope: Scope::Within(vec![PathBuf::from("/a/ws"), PathBuf::from("/b")]),
            })
        );
    }

    /// Linux reuses process numbers, and answers for a thread's ID as for a
    /// process, so a record's number can name a live stranger.
    #[cfg(target_os = "linux")]
    #[test]
    fn reused_numbers_are_pruned_without_discarding_live_legacy_holders() {
        let current = crate::process_identity::ProcessIdentity::current();
        let start = current.start().expect("this process reports its start");
        let (sent_tid, thread_id) = std::sync::mpsc::channel();
        let (stop, stopped) = std::sync::mpsc::channel::<()>();
        let thread = std::thread::spawn(move || {
            // SAFETY: gettid has no arguments and returns this thread's ID.
            sent_tid.send(unsafe { libc::gettid() } as u32).unwrap();
            let _ = stopped.recv();
        });
        let tid = thread_id.recv().unwrap();

        let dir = tempfile::tempdir().unwrap();
        let lease = shared(dir.path(), "cargo build", &mut io::sink()).unwrap();
        let records = holders_dir(dir.path());
        let reused = [
            // A thread of this very process.
            format!("{tid}-1-{start}"),
            // A process that started after the one that wrote the record.
            format!("{}-1-{}", current.pid(), start ^ 1),
        ];
        for name in &reused {
            std::fs::write(records.join(name), "5\tcargo nextest").unwrap();
        }
        let legacy = records.join(format!("{}-2", current.pid()));
        std::fs::write(&legacy, "5\tlegacy cargo").unwrap();
        let named: Vec<String> = holders(dir.path()).into_iter().map(|h| h.what).collect();
        assert_eq!(named, ["legacy cargo", "cargo build"]);
        assert!(legacy.exists(), "an active legacy holder lost its record");
        assert!(try_exclusive(dir.path(), dir.path()).unwrap().is_none());
        for name in &reused {
            assert!(!records.join(name).exists(), "{name} kept");
        }

        drop(lease);
        drop(stop);
        thread.join().unwrap();
        assert!(try_exclusive(dir.path(), dir.path()).unwrap().is_some());
    }

    #[test]
    fn a_running_command_blocks_cleanup_until_it_exits() {
        let fx = Fixture::new();
        let scope = fx.within(&["ws"]);
        let first = shared_in(&fx.cache, &scope, "cargo build", &mut io::sink()).unwrap();
        let second = shared_in(&fx.cache, &scope, "cargo build", &mut io::sink()).unwrap();
        assert!(!fx.free("ws/target"));
        drop(first);
        assert!(!fx.free("ws/target"));
        drop(second);
        assert!(fx.free("ws/target"));
    }

    #[test]
    fn holder_records_name_blockers_but_do_not_hold_the_cleanup_lock() {
        let fx = Fixture::new();
        let records = holders_dir(&fx.cache);
        std::fs::create_dir_all(&records).unwrap();
        std::fs::write(
            records.join(format!("{}-1", std::process::id())),
            "1\told cargo command",
        )
        .unwrap();
        assert_eq!(holders(&fx.cache).len(), 1);
        assert_eq!(blockers(&fx.cache, &fx.at("ws/target")), None);
        assert!(fx.free("ws/target"));
    }

    #[test]
    fn a_command_says_it_waits_for_cleanup_then_runs_once_cleanup_ends() {
        let fx = Fixture::new();
        let cleanup = try_exclusive(&fx.cache, &fx.at("ws/target"))
            .unwrap()
            .unwrap();
        if SCOPED_LOCKS {
            // A scoped command elsewhere can start right away.
            let mut said = Vec::new();
            drop(shared_in(&fx.cache, &fx.within(&["other"]), "cargo build", &mut said).unwrap());
            assert!(said.is_empty(), "{}", String::from_utf8_lossy(&said));
        }

        let (said, heard) = std::sync::mpsc::channel();
        let cache = fx.cache.clone();
        let scope = fx.within(&["ws"]);
        let command = std::thread::spawn(move || {
            shared_in(&cache, &scope, "cargo build", &mut Said(said)).unwrap()
        });
        let line = heard
            .recv_timeout(Duration::from_secs(10))
            .expect("the command never said it was waiting");
        assert_eq!(String::from_utf8(line).unwrap(), naming(std::process::id()));
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            !command.is_finished(),
            "the command started while cleanup held the lock"
        );
        drop(cleanup);
        let lease = command.join().unwrap();
        assert!(!fx.free("ws/target"), "the command runs without its lease");
        drop(lease);
        assert!(heard.try_recv().is_err(), "more than one line");
    }

    #[test]
    fn a_command_takes_a_free_lock_without_a_word() {
        let dir = tempfile::tempdir().unwrap();
        let mut said = Vec::new();
        let first = shared(dir.path(), "cargo build", &mut said).unwrap();
        let second = shared(dir.path(), "cargo build", &mut said).unwrap();
        assert!(said.is_empty(), "{}", String::from_utf8_lossy(&said));
        drop((first, second));
    }

    #[test]
    fn the_wait_names_the_cleanup_process_only_while_it_holds_the_lock() {
        let dir = tempfile::tempdir().unwrap();
        let anonymous = "kache: waiting for target cleanup to finish\n";
        assert_eq!(waiting(dir.path()), anonymous);
        let cleanup = try_exclusive(dir.path(), dir.path()).unwrap().unwrap();
        assert_eq!(waiting(dir.path()), naming(std::process::id()));
        drop(cleanup);
        assert_eq!(waiting(dir.path()), anonymous);
        std::fs::write(holder_record(dir.path()), "not a pid").unwrap();
        assert_eq!(waiting(dir.path()), anonymous);
    }

    #[test]
    fn quiet_cargo_hides_the_wait() {
        let is_quiet = |args: &[&str]| quiet(args.iter().map(OsStr::new));
        assert!(!is_quiet(&["build"]));
        assert!(is_quiet(&["build", "-q"]));
        assert!(is_quiet(&["--quiet", "test"]));
        // What follows `--` belongs to the program Cargo runs.
        assert!(!is_quiet(&["run", "--", "-q"]));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_holder_records_distinguish_starts_and_keep_live_legacy_commands() {
        let dir = tempfile::tempdir().unwrap();
        let _lease = shared(dir.path(), "cargo build", &mut io::sink()).unwrap();
        let current = crate::process_identity::ProcessIdentity::current();
        let start = current.start().expect("macOS reports the holder's start");
        let records = holders_dir(dir.path());
        let stale = records.join(format!("{}-1-{}", current.pid(), start ^ 1));
        let legacy = records.join(format!("{}-2", current.pid()));
        std::fs::write(&stale, "0\tstale cargo").unwrap();
        std::fs::write(&legacy, "0\tlegacy cargo").unwrap();

        let named = holders(dir.path());
        assert_eq!(named.len(), 2);
        assert!(
            named
                .iter()
                .any(|h| h.what == "cargo build" && h.process == current)
        );
        assert!(named.iter().any(|h| h.what == "legacy cargo"));
        assert!(legacy.exists());
        assert!(!stale.exists());
    }

    #[cfg(unix)]
    #[test]
    fn closing_a_lease_releases_an_inherited_descriptor() {
        let fx = Fixture::new();
        let lease = shared_in(
            &fx.cache,
            &fx.within(&["ws"]),
            "cargo build",
            &mut io::sink(),
        )
        .unwrap();
        let inherited: Vec<File> = lease
            .files
            .iter()
            .map(|f| f.file.try_clone().unwrap())
            .collect();
        let legacy = open_lock(&everything_lock(&fx.cache)).unwrap();
        assert!(matches!(legacy.try_lock(), Err(TryLockError::WouldBlock)));
        drop(lease);
        legacy.try_lock().unwrap();
        legacy.unlock().unwrap();
        assert!(fx.free("ws/target"));
        drop(inherited);
    }

    #[cfg(unix)]
    #[test]
    fn pruning_keeps_held_path_locks_and_removes_unused_files() {
        let fx = Fixture::new();
        let held = lease_locks(&fx.cache, &[fx.at("ws")]);
        let mut guards = Vec::new();
        let mut said = false;
        for lock in &held {
            guards.push(
                lock_shared(lock, Region::Whole, &fx.cache, &mut io::sink(), &mut said).unwrap(),
            );
        }
        // Obtain this file before pruning, as a cleanup in another workspace does.
        let unused = path_lock(&fx.cache, 'u', &fx.at("other"));
        drop(try_lock_exclusive(&unused, Region::Whole).unwrap().unwrap());
        prune_locks(&fx.cache);
        assert!(!unused.exists());
        for lock in &held {
            assert!(lock.exists(), "{} pruned while held", lock.display());
        }
        drop(guards);
        prune_locks(&fx.cache);
        assert_eq!(std::fs::read_dir(locks_dir(&fx.cache)).unwrap().count(), 0);
        prune_locks(&fx.cache);
        let absent = fx.at("no-cache");
        prune_locks(&absent);
        assert!(!absent.exists());
    }

    #[test]
    fn path_locks_conflict_on_ancestors_and_descendants_only() {
        let fx = Fixture::new();
        let held = lease_locks(&fx.cache, &[fx.at("a/b"), fx.at("elsewhere/x")]);
        let conflicts = |target: &str| {
            let target = normalize(&fx.at(target)).unwrap();
            reservation_locks(&fx.cache, &target).any(|lock| held.contains(&lock))
        };
        for target in [
            "",
            "a",
            "a/b",
            "a/b/deps",
            "elsewhere",
            "elsewhere/x/release",
        ] {
            assert!(conflicts(target), "{target:?} should be held");
        }
        for target in ["a/another", "a/bb", "elsewhere/y", "c"] {
            assert!(!conflicts(target), "{target:?} should be free");
        }
    }

    /// A lock file pruned between a lease opening it and locking it guards
    /// nothing, so the lease locks the file that replaced it.
    #[cfg(unix)]
    #[test]
    fn a_lock_on_a_pruned_file_is_taken_again() {
        let fx = Fixture::new();
        let lock = fx.cache.join("probe.lock");
        let stale = open_lock(&lock).unwrap();
        assert!(still_named(&stale, &lock));
        std::fs::remove_file(&lock).unwrap();
        assert!(!still_named(&stale, &lock));
        let fresh = open_lock(&lock).unwrap();
        assert!(!still_named(&stale, &lock));
        assert!(still_named(&fresh, &lock));
        let mut said = false;
        let taken =
            lock_shared(&lock, Region::Whole, &fx.cache, &mut io::sink(), &mut said).unwrap();
        assert!(still_named(&taken.file, &lock));
        drop(taken);
        let taken = try_lock_exclusive(&lock, Region::Whole).unwrap().unwrap();
        assert!(still_named(&taken.file, &lock));
        assert!(try_lock_exclusive(&lock, Region::Whole).unwrap().is_none());
    }

    #[cfg(unix)]
    #[test]
    fn a_waiting_command_uses_the_replacement_after_pruning() {
        let fx = Fixture::new();
        let path = fx.cache.join("replace.lock");
        let old = open_lock(&path).unwrap();
        old.lock().unwrap();
        let (said, heard) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let waiter = scope.spawn(|| {
                lock_shared(&path, Region::Whole, &fx.cache, &mut Said(said), &mut false).unwrap()
            });
            heard.recv_timeout(Duration::from_secs(10)).unwrap();
            std::fs::remove_file(&path).unwrap();
            let replacement = open_lock(&path).unwrap();
            old.unlock().unwrap();
            let held = waiter.join().unwrap();
            assert!(matches!(
                replacement.try_lock(),
                Err(TryLockError::WouldBlock)
            ));
            drop(held);
            replacement.try_lock().unwrap();
        });
    }
}
