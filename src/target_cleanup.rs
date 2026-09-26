//! Target directories the daemon removes without being asked.
//!
//! Builds through kache record the target directories they use. On a quiet
//! machine, at most once an hour, the daemon removes the ones whose
//! workspace has been deleted (`[cache] auto_clean_orphaned_targets`, on by
//! default) and, when `[cache] auto_clean_idle_targets_days` is set, the ones
//! no build has used for that many days. A directory goes only after the
//! checks `kache clean --tracked --yes` makes: it is still the derived target
//! directory that was recorded, and no running build holds its lock. It is
//! first renamed aside and checked again, so a build that starts meanwhile
//! gets a fresh directory instead of one being deleted under it.

use crate::config::Config;
use crate::maintenance::{Trigger, is_quiet, unix_now_secs};
use crate::store::Store;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// How often a quiet machine is checked.
const INTERVAL: Duration = Duration::from_secs(3600);
const DAY_SECS: u64 = 86_400;
/// How long an orphaned target must also have gone unused, so a worktree
/// that is moved or recreated keeps its target.
const ORPHAN_GRACE_SECS: u64 = DAY_SECS;

/// Holds the Unix time of the last check, so a restarted daemon keeps the
/// hourly pace.
const LAST_CHECK_FILE: &str = "target-cleanup.last";

/// The last check recorded in `cache_dir`, `0` when there is none.
fn last_check(cache_dir: &Path) -> u64 {
    std::fs::read_to_string(cache_dir.join(LAST_CHECK_FILE))
        .ok()
        .and_then(|text| text.trim().parse().ok())
        .unwrap_or(0)
}

/// Why a target directory was removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reason {
    /// Its workspace has been deleted.
    Orphaned,
    /// No build has used it for the configured number of days.
    Idle,
}

impl Reason {
    fn describe(self) -> &'static str {
        match self {
            Reason::Orphaned => "its workspace was deleted",
            Reason::Idle => "no build used it within auto_clean_idle_targets_days",
        }
    }
}

/// Why `config` removes a target, if it does: `orphaned` when its workspace
/// is gone, `idle_secs` since a build last used it.
pub(crate) fn reason(config: &Config, orphaned: bool, idle_secs: i64) -> Option<Reason> {
    let idle = u64::try_from(idle_secs).unwrap_or(0);
    if orphaned && config.auto_clean_orphaned_targets && idle >= ORPHAN_GRACE_SECS {
        return Some(Reason::Orphaned);
    }
    let days = config.auto_clean_idle_targets_days;
    (days > 0 && idle >= days.saturating_mul(DAY_SECS)).then_some(Reason::Idle)
}

/// Whether a periodic check at `now` should run: cleanup is on, an hour has
/// passed since `last`, and the machine is quiet.
pub(crate) fn due(
    config: &Config,
    trigger: Trigger<'_>,
    permits: Option<u32>,
    last: u64,
    now: u64,
) -> bool {
    let enabled = config.auto_clean_orphaned_targets || config.auto_clean_idle_targets_days > 0;
    // A clock set back behind the record does not stop the checks.
    let waited = last == 0 || now < last || now - last >= INTERVAL.as_secs();
    enabled
        && matches!(trigger, Trigger::Periodic(_))
        && waited
        && is_quiet(permits, trigger.idle_for())
}

/// The maintenance step. Logs what it removed.
pub(crate) fn run(config: &Config, trigger: Trigger<'_>) {
    let now = unix_now_secs();
    let permits = crate::scheduler::permits_in_use(&config.cache_dir);
    if !due(config, trigger, permits, last_check(&config.cache_dir), now) {
        return;
    }
    let record = config.cache_dir.join(LAST_CHECK_FILE);
    // The record is what keeps the checks hourly.
    if let Err(error) = crate::atomic::atomic_replace(&record, now.to_string().as_bytes()) {
        tracing::warn!("skipping target directory cleanup: cannot record the time: {error:#}");
        return;
    }
    match sweep(config, now) {
        Ok(removed) => {
            for (path, reason) in removed {
                tracing::info!(
                    "removed target directory {} because {}",
                    path.display(),
                    reason.describe()
                );
            }
        }
        Err(error) => tracing::warn!("target directory cleanup failed: {error:#}"),
    }
}

/// Remove every tracked target directory `config` selects at `now` and
/// return them. A registry row whose directory is gone, moved or no longer
/// a derived target directory is forgotten; a directory a running build
/// holds is kept for the next check.
pub(crate) fn sweep(config: &Config, now: u64) -> anyhow::Result<Vec<(PathBuf, Reason)>> {
    let store = Store::open(config)?;
    let now = i64::try_from(now).unwrap_or(i64::MAX);
    let mut removed = Vec::new();
    for tracked in store.tracked_target_roots(0)? {
        let orphaned = crate::cli::workspace_is_gone(&tracked.workspace_root);
        let idle = now.saturating_sub(tracked.last_seen);
        let Some(reason) = reason(config, orphaned, idle) else {
            continue;
        };
        let intact = crate::machine::target_root_is_safe(&tracked.path, &tracked.workspace_root)
            && crate::machine::directory_identity(&tracked.path) == Some(tracked.identity)
            && !looks_like_a_source_root(&tracked.path);
        if !intact {
            store.forget_target_root(&tracked.path)?;
            continue;
        }
        if crate::cli::target_in_use(&tracked.path) {
            continue;
        }
        match remove(&tracked.path, now) {
            Ok(true) => {
                store.forget_target_root(&tracked.path)?;
                removed.push((tracked.path, reason));
            }
            Ok(false) => {}
            Err(error) => tracing::warn!(
                "could not remove target directory {}: {error}",
                tracked.path.display()
            ),
        }
    }
    Ok(removed)
}

/// A directory holding a manifest or a repository is a source tree, never
/// a target directory, however it is spelled.
fn looks_like_a_source_root(path: &Path) -> bool {
    ["Cargo.toml", ".git"]
        .iter()
        .any(|name| std::fs::symlink_metadata(path.join(name)).is_ok())
}

/// Rename `target` aside, check no build holds it, then delete it. A build
/// that took its lock before the rename is found, and the directory is
/// renamed back; one that starts after the rename creates a new directory.
/// `Ok(false)` when a build holds it.
fn remove(target: &Path, now: i64) -> std::io::Result<bool> {
    let name = target.file_name().unwrap_or_default().to_string_lossy();
    let aside = target.with_file_name(format!(
        ".{name}.kache-removing-{}-{now}",
        std::process::id()
    ));
    std::fs::rename(target, &aside)?;
    if crate::cli::target_in_use(&aside) {
        if std::fs::symlink_metadata(target).is_err() {
            std::fs::rename(&aside, target)?;
        }
        return Ok(false);
    }
    std::fs::remove_dir_all(&aside)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::maintenance::RequestClock;
    use std::path::Path;

    fn config(cache: &Path, orphans: bool, days: u64) -> Config {
        let mut config = crate::test_support::test_config(cache.to_path_buf());
        config.auto_clean_orphaned_targets = orphans;
        config.auto_clean_idle_targets_days = days;
        config
    }

    /// A workspace under `root` with a Cargo target directory kache tracks.
    fn tracked_target(store: &Store, root: &Path, name: &str) -> (PathBuf, PathBuf) {
        let workspace = root.join(name);
        let target = workspace.join("target");
        std::fs::create_dir_all(target.join("debug")).unwrap();
        std::fs::write(
            target.join("CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55\n",
        )
        .unwrap();
        store.remember_target_root(&target, &workspace).unwrap();
        (workspace, target)
    }

    /// Move `target` out of `workspace` and delete the workspace, the way a
    /// removed worktree with a shared `CARGO_TARGET_DIR` leaves it.
    fn orphan(store: &Store, root: &Path, name: &str) -> PathBuf {
        let workspace = root.join(name);
        let target = root.join(format!("{name}-target"));
        std::fs::create_dir_all(target.join("debug")).unwrap();
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(
            target.join("CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55\n",
        )
        .unwrap();
        store.remember_target_root(&target, &workspace).unwrap();
        std::fs::remove_dir(&workspace).unwrap();
        target
    }

    fn tracked(store: &Store) -> Vec<PathBuf> {
        let mut paths: Vec<_> = store
            .tracked_target_roots(0)
            .unwrap()
            .into_iter()
            .map(|t| t.path)
            .collect();
        paths.sort();
        paths
    }

    /// Move every tracked target's last use `secs` into the past.
    fn age(store: &Store, secs: u64) {
        store
            .file_hash_cache()
            .db()
            .execute(
                "UPDATE target_roots SET last_seen = last_seen - ?1",
                [secs as i64],
            )
            .unwrap();
    }

    #[test]
    fn reasons_follow_the_configuration() {
        let dir = tempfile::tempdir().unwrap();
        let day = DAY_SECS as i64;
        let both = config(dir.path(), true, 30);
        // An orphan also waits out a day of disuse.
        assert_eq!(reason(&both, true, day), Some(Reason::Orphaned));
        assert_eq!(reason(&both, true, day - 1), None);
        assert_eq!(reason(&both, true, 0), None);
        assert_eq!(reason(&both, false, 30 * day), Some(Reason::Idle));
        assert_eq!(reason(&both, false, 30 * day - 1), None);
        assert_eq!(reason(&both, false, -1), None);
        let orphans_only = config(dir.path(), true, 0);
        assert_eq!(reason(&orphans_only, false, 10_000 * day), None);
        let idle_only = config(dir.path(), false, 1);
        assert_eq!(reason(&idle_only, true, 0), None);
        assert_eq!(reason(&idle_only, true, day), Some(Reason::Idle));
        assert_eq!(DAY_SECS, 86_400);
        assert_eq!(ORPHAN_GRACE_SECS, 86_400);
        assert_eq!(INTERVAL, Duration::from_secs(3_600));
    }

    #[test]
    fn checks_hourly_when_on_and_quiet() {
        let dir = tempfile::tempdir().unwrap();
        let on = config(dir.path(), true, 0);
        let idle_only = config(dir.path(), false, 5);
        let off = config(dir.path(), false, 0);
        let quiet = RequestClock::idle();
        let busy = RequestClock::new();
        let periodic = Trigger::Periodic(&quiet);
        let now = 10_000;
        assert!(due(&on, periodic, Some(0), 0, now));
        assert!(due(&idle_only, periodic, Some(0), 0, now));
        assert!(!due(&off, periodic, Some(0), 0, now));
        assert!(!due(&on, Trigger::Shutdown, Some(0), 0, now));
        assert!(!due(&on, Trigger::Periodic(&busy), Some(0), 0, now));
        assert!(!due(&on, periodic, Some(1), 0, now));
        assert!(!due(&on, periodic, None, 0, now));
        assert!(!due(&on, periodic, Some(0), now - 3_599, now));
        assert!(due(&on, periodic, Some(0), now - 3_600, now));
        assert!(!due(&on, periodic, Some(0), now, now));
        // The clock went back behind the record.
        assert!(due(&on, periodic, Some(0), now + 1, now));
    }

    #[test]
    fn removes_what_the_configuration_selects() {
        let root = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let both = config(cache.path(), true, 30);
        let store = Store::open(&both).unwrap();
        let (_, fresh) = tracked_target(&store, root.path(), "fresh");
        let gone = orphan(&store, root.path(), "gone");
        let now = unix_now_secs();

        // Just orphaned: kept for a day in case the worktree comes back.
        assert!(sweep(&both, now).unwrap().is_empty());
        let tomorrow = now + DAY_SECS;
        let removed = sweep(&both, tomorrow).unwrap();
        assert_eq!(removed, vec![(gone.clone(), Reason::Orphaned)]);
        assert!(!gone.exists());
        assert!(fresh.exists());
        assert_eq!(tracked(&store), vec![fresh.clone()]);

        // 31 days on, the fresh one is idle.
        let later = now + 31 * DAY_SECS;
        let orphans_only = config(cache.path(), true, 0);
        assert!(sweep(&orphans_only, later).unwrap().is_empty());
        assert_eq!(
            sweep(&both, later).unwrap(),
            vec![(fresh.clone(), Reason::Idle)]
        );
        assert!(!fresh.exists());
        assert!(tracked(&store).is_empty());
        // Nothing is left beside it.
        let left: Vec<_> = std::fs::read_dir(root.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(left, vec![std::ffi::OsString::from("fresh")]);
    }

    #[test]
    fn keeps_directories_that_are_not_safe_to_remove() {
        let root = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let orphans = config(cache.path(), true, 0);
        let store = Store::open(&orphans).unwrap();
        let tag = "Signature: 8a477f597d28d172789f06886806bc55\n";

        // Replaced after it was recorded: forgotten, not removed.
        let replaced = orphan(&store, root.path(), "replaced");
        std::fs::rename(&replaced, root.path().join("moved")).unwrap();
        std::fs::create_dir_all(replaced.join("debug")).unwrap();
        std::fs::write(replaced.join("CACHEDIR.TAG"), tag).unwrap();
        // No longer a Cargo target directory: forgotten, not removed.
        let untagged = orphan(&store, root.path(), "untagged");
        std::fs::remove_file(untagged.join("CACHEDIR.TAG")).unwrap();
        // Holds a manifest or a repository: a source tree.
        let manifest = orphan(&store, root.path(), "manifest");
        std::fs::write(manifest.join("Cargo.toml"), "").unwrap();
        let repo = orphan(&store, root.path(), "repo");
        std::fs::create_dir(repo.join(".git")).unwrap();
        // A running build holds it: kept and still tracked.
        let busy = orphan(&store, root.path(), "busy");
        let lock = std::fs::File::create(busy.join("debug/.cargo-lock")).unwrap();
        lock.lock().unwrap();

        let later = unix_now_secs() + DAY_SECS;
        assert!(sweep(&orphans, later).unwrap().is_empty());
        for kept in [&replaced, &untagged, &manifest, &repo, &busy] {
            assert!(kept.exists(), "{}", kept.display());
        }
        assert_eq!(tracked(&store), vec![busy.clone()]);

        drop(lock);
        assert_eq!(
            sweep(&orphans, later).unwrap(),
            vec![(busy.clone(), Reason::Orphaned)]
        );
    }

    #[test]
    fn a_build_that_holds_the_directory_gets_it_back() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("target");
        std::fs::create_dir_all(target.join("debug")).unwrap();
        let lock = std::fs::File::create(target.join("debug/.cargo-lock")).unwrap();
        lock.lock().unwrap();
        assert!(!remove(&target, 7).unwrap());
        assert!(target.join("debug/.cargo-lock").exists());
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
        drop(lock);
        assert!(remove(&target, 7).unwrap());
        assert!(!target.exists());
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
        assert!(remove(&target, 7).is_err());
    }

    #[test]
    fn the_maintenance_step_removes_orphans_once_an_hour() {
        let root = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let orphans = config(cache.path(), true, 0);
        let store = Store::open(&orphans).unwrap();
        let quiet = RequestClock::idle();
        let gone = orphan(&store, root.path(), "gone");
        age(&store, DAY_SECS);
        run(&orphans, Trigger::Periodic(&quiet));
        assert!(!gone.exists());
        // Checked a moment ago: the next orphan waits for the next hour.
        let next = orphan(&store, root.path(), "next");
        age(&store, DAY_SECS);
        run(&orphans, Trigger::Periodic(&quiet));
        assert!(next.exists());
        let recorded = last_check(cache.path());
        assert!(recorded + 60 >= unix_now_secs(), "{recorded}");
        std::fs::write(cache.path().join(LAST_CHECK_FILE), "garbage").unwrap();
        assert_eq!(last_check(cache.path()), 0);
        run(&orphans, Trigger::Periodic(&quiet));
        assert!(!next.exists());
        assert_eq!(Reason::Orphaned.describe(), "its workspace was deleted");
        assert!(
            Reason::Idle
                .describe()
                .contains("auto_clean_idle_targets_days")
        );
    }

    #[cfg(unix)]
    #[test]
    fn skips_the_check_when_the_time_cannot_be_recorded() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let orphans = config(cache.path(), true, 0);
        let store = Store::open(&orphans).unwrap();
        let gone = orphan(&store, root.path(), "gone");
        age(&store, DAY_SECS);
        // The record's path is taken by a directory.
        std::fs::create_dir(cache.path().join(LAST_CHECK_FILE)).unwrap();
        std::fs::set_permissions(
            cache.path().join(LAST_CHECK_FILE),
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        run(&orphans, Trigger::Periodic(&RequestClock::idle()));
        assert!(gone.exists());
    }
}
