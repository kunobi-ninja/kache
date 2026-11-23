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
//! which every reservation also takes.

use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs::{File, OpenOptions, TryLockError};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

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
        let paths: BTreeSet<PathBuf> = paths.into_iter().map(|path| normalize(&path)).collect();
        if paths.is_empty() {
            Self::Everything
        } else {
            Self::Within(paths.into_iter().collect())
        }
    }

    /// Whether removing `target` could take something this scope uses.
    pub(crate) fn covers(&self, target: &Path) -> bool {
        match self {
            Self::Everything => true,
            Self::Within(paths) => {
                let target = normalize(target);
                paths
                    .iter()
                    .any(|path| path.starts_with(&target) || target.starts_with(path))
            }
        }
    }
}

/// `path` made absolute, with every existing prefix resolved the way the
/// filesystem names it, so a lease and a reservation spell one directory the
/// same: `/tmp` and `/private/tmp` on macOS, or a symlinked checkout.
pub(crate) fn normalize(path: &Path) -> PathBuf {
    let absolute = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
    let mut existing = absolute.as_path();
    let mut missing = Vec::new();
    loop {
        if let Ok(real) = std::fs::canonicalize(existing) {
            return missing
                .iter()
                .rev()
                .fold(real, |path, name| path.join(name));
        }
        match (existing.parent(), existing.file_name()) {
            (Some(parent), Some(name)) => {
                missing.push(name.to_os_string());
                existing = parent;
            }
            _ => return absolute,
        }
    }
}

/// Unlock explicitly before closing: a concurrent fork can briefly inherit
/// the descriptors, even though they close on exec. A shared lease also
/// leaves a record naming its command, so cleanup can say what it waits for.
#[derive(Default)]
pub(crate) struct Lease {
    files: Vec<File>,
    record: Option<PathBuf>,
}

impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(record) = &self.record {
            let _ = std::fs::remove_file(record);
        }
        for file in &self.files {
            let _ = file.unlock();
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
    let hash = blake3::hash(path.as_os_str().as_encoded_bytes()).to_hex();
    locks_dir(cache_dir).join(format!("{kind}-{}.lock", &hash.as_str()[..32]))
}

/// The locks a lease on `scope` shares, in one global order.
fn lease_locks(cache_dir: &Path, scope: &Scope) -> BTreeSet<PathBuf> {
    match scope {
        Scope::Everything => BTreeSet::from([everything_lock(cache_dir)]),
        Scope::Within(paths) => paths
            .iter()
            .flat_map(|path| {
                std::iter::once(path_lock(cache_dir, 'u', path)).chain(
                    path.ancestors()
                        .skip(1)
                        .map(|ancestor| path_lock(cache_dir, 'i', ancestor)),
                )
            })
            .collect(),
    }
}

/// The locks a reservation on the normalized `target` takes, in one order.
fn reservation_locks(cache_dir: &Path, target: &Path) -> Vec<PathBuf> {
    let mut locks = vec![everything_lock(cache_dir)];
    locks.extend(
        target
            .ancestors()
            .map(|ancestor| path_lock(cache_dir, 'u', ancestor)),
    );
    locks.push(path_lock(cache_dir, 'i', target));
    locks
}

/// Whether `file` is still the file at `path`. A pruned lock file is gone,
/// and a lock taken on it guards nothing.
#[cfg(unix)]
fn still_named(file: &File, path: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    match (file.metadata(), std::fs::metadata(path)) {
        (Ok(held), Ok(named)) => held.dev() == named.dev() && held.ino() == named.ino(),
        _ => false,
    }
}

/// Lock files are never pruned here, so the open file is the named one.
#[cfg(not(unix))]
fn still_named(_: &File, _: &Path) -> bool {
    true
}

/// Share the lock at `path`, waiting for a cleanup that holds it. `said`
/// keeps the notice to one line per lease.
fn lock_shared(
    path: &Path,
    cache_dir: &Path,
    notice: &mut dyn Write,
    said: &mut bool,
) -> io::Result<File> {
    loop {
        let file = open_lock(path)?;
        match file.try_lock_shared() {
            Ok(()) => {}
            // One write, so the line stays whole beside other processes' output.
            Err(TryLockError::WouldBlock) => {
                if !std::mem::replace(said, true) {
                    let _ = notice.write_all(waiting(cache_dir).as_bytes());
                }
                file.lock_shared()?;
            }
            Err(TryLockError::Error(error)) => return Err(error),
        }
        if still_named(&file, path) {
            return Ok(file);
        }
        let _ = file.unlock();
    }
}

/// Take the lock at `path` without waiting: `None` when someone holds it.
fn try_lock_exclusive(path: &Path) -> io::Result<Option<File>> {
    loop {
        let file = open_lock(path)?;
        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Ok(None),
            Err(TryLockError::Error(error)) => return Err(error),
        }
        if still_named(&file, path) {
            return Ok(Some(file));
        }
        let _ = file.unlock();
    }
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
    match reserve(cache_dir, &normalize(target)) {
        Ok(Some(_)) | Err(_) => None,
        Ok(None) => Some(holders_covering(cache_dir, target)),
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
    for lock in lease_locks(cache_dir, scope) {
        lease
            .files
            .push(lock_shared(&lock, cache_dir, notice, &mut said)?);
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
    for lock in reservation_locks(cache_dir, target) {
        match try_lock_exclusive(&lock)? {
            Some(file) => lease.files.push(file),
            None => return Ok(None),
        }
    }
    Ok(Some(lease))
}

/// Reserve deleting `target`, or changing build output inside it, without
/// waiting for a running command.
pub(crate) fn try_exclusive(cache_dir: &Path, target: &Path) -> io::Result<Option<Reservation>> {
    let Some(lease) = reserve(cache_dir, &normalize(target))? else {
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
pub(crate) fn prune_locks(cache_dir: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(locks_dir(cache_dir)) else {
        return 0;
    };
    let mut pruned = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(file) = OpenOptions::new().read(true).write(true).open(&path) else {
            continue;
        };
        if file.try_lock().is_ok() {
            if still_named(&file, &path) && std::fs::remove_file(&path).is_ok() {
                pruned += 1;
            }
            let _ = file.unlock();
        }
    }
    pruned
}

/// Windows refuses to reopen a file whose deletion is pending, so a lease
/// there could fail rather than retry; its lock files stay.
#[cfg(not(unix))]
pub(crate) fn prune_locks(_: &Path) -> usize {
    0
}

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
            let root = normalize(dir.path()).join("work");
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
        assert!(fx.free("other/target"));
        assert!(fx.free("ws2/target"));
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
        assert!(fx.free("ws/target/release"));
        assert!(fx.free("ws/crates/target"));
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
                .map(|holders| holders.into_iter().map(|h| h.what).collect::<Vec<_>>())
        };
        assert_eq!(named("ws/target"), Some(vec!["cargo test".to_owned()]));
        assert_eq!(named("other/target"), Some(vec!["cargo build".to_owned()]));
        assert_eq!(named("third/target"), None);
        drop((here, there));
        assert_eq!(named("ws/target"), None);
    }

    #[test]
    fn scopes_cover_paths_that_contain_or_lie_inside_theirs() {
        let fx = Fixture::new();
        assert_eq!(Scope::within(Vec::new()), Scope::Everything);
        assert!(Scope::Everything.covers(&fx.at("anything")));
        let scope = fx.within(&["ws", "ws", "out/target"]);
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

    #[cfg(unix)]
    #[test]
    fn paths_are_compared_as_the_filesystem_names_them() {
        let fx = Fixture::new();
        std::fs::create_dir_all(fx.at("real/ws")).unwrap();
        std::os::unix::fs::symlink(fx.at("real"), fx.at("link")).unwrap();
        assert_eq!(normalize(&fx.at("link/ws/target")), fx.at("real/ws/target"));
        assert_eq!(normalize(&fx.at("link/ws")), fx.at("real/ws"));
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
        assert_eq!(named[0].scope, scope);
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

    /// A process number is reused once its process exits, and on Linux a
    /// thread's ID answers for a process, so a record's number can name a
    /// live stranger.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn reused_numbers_are_pruned_without_discarding_live_legacy_holders() {
        let current = crate::process_identity::ProcessIdentity::current();
        let start = current.start().expect("this process reports its start");
        let dir = tempfile::tempdir().unwrap();
        let lease = shared(dir.path(), "cargo build", &mut io::sink()).unwrap();
        let records = holders_dir(dir.path());
        #[cfg_attr(not(target_os = "linux"), allow(unused_mut))]
        let mut reused = vec![
            // A process that started after the one that wrote the record.
            format!("{}-1-{}", current.pid(), start ^ 1),
        ];
        #[cfg(target_os = "linux")]
        let thread = {
            let (sent_tid, thread_id) = std::sync::mpsc::channel();
            let (stop, stopped) = std::sync::mpsc::channel::<()>();
            let thread = std::thread::spawn(move || {
                // SAFETY: gettid has no arguments and returns this thread's ID.
                sent_tid.send(unsafe { libc::gettid() } as u32).unwrap();
                let _ = stopped.recv();
            });
            // A thread of this very process.
            reused.push(format!("{}-1-{start}", thread_id.recv().unwrap()));
            (stop, thread)
        };
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
        #[cfg(target_os = "linux")]
        {
            drop(thread.0);
            thread.1.join().unwrap();
        }
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
        // A command elsewhere starts at once and says nothing.
        let mut said = Vec::new();
        drop(shared_in(&fx.cache, &fx.within(&["other"]), "cargo build", &mut said).unwrap());
        assert!(said.is_empty(), "{}", String::from_utf8_lossy(&said));

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
        let inherited: Vec<File> = lease.files.iter().map(|f| f.try_clone().unwrap()).collect();
        drop(lease);
        assert!(fx.free("ws/target"));
        drop(inherited);
    }

    #[cfg(unix)]
    #[test]
    fn pruning_keeps_held_lock_files_and_a_lease_survives_it() {
        let fx = Fixture::new();
        let scope = fx.within(&["ws"]);
        drop(try_exclusive(&fx.cache, &fx.at("other/target")).unwrap());
        let lease = shared_in(&fx.cache, &scope, "cargo build", &mut io::sink()).unwrap();
        let held: BTreeSet<PathBuf> = lease_locks(&fx.cache, &scope);
        assert!(prune_locks(&fx.cache) > 0);
        for lock in &held {
            assert!(lock.exists(), "{} pruned while held", lock.display());
        }
        assert!(!fx.free("ws/target"));
        drop(lease);
        assert!(prune_locks(&fx.cache) >= held.len());
        assert_eq!(std::fs::read_dir(locks_dir(&fx.cache)).unwrap().count(), 0);
        assert_eq!(prune_locks(&fx.cache), 0);
        assert_eq!(prune_locks(&fx.at("no-cache")), 0);
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
        let taken = lock_shared(&lock, &fx.cache, &mut io::sink(), &mut said).unwrap();
        assert!(still_named(&taken, &lock));
        drop(taken);
        let taken = try_lock_exclusive(&lock).unwrap().unwrap();
        assert!(still_named(&taken, &lock));
        assert!(try_lock_exclusive(&lock).unwrap().is_none());
    }
}
