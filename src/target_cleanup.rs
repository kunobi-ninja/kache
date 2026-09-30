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
//!
//! A target directory that stays loses the build units no build used for
//! `[cache] auto_clean_unused_units_days` (30 by default; see
//! [`crate::unit_prune`]).
//!
//! With `[cache] auto_recover_min_free_bytes` set, a volume below that floor
//! first loses the units no build used for a day, from every target on it;
//! only then do whole targets idle for a day go, largest first, until the
//! volume is back above the floor. [`plan`] reports what the next pass would
//! do to one target, from the same checks.

use crate::config::Config;
use crate::maintenance::{Trigger, is_quiet, unix_now_secs};
use crate::store::{Store, TrackedTargetRoot};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

/// How often a quiet machine is checked.
const INTERVAL: Duration = Duration::from_secs(3600);
const DAY_SECS: u64 = 86_400;
/// Under disk pressure, a unit no build used for this long goes before any
/// whole target does.
const PRESSURE_UNIT_WINDOW: Duration = Duration::from_secs(DAY_SECS);
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Reason {
    /// Its workspace has been deleted.
    Orphaned,
    /// No build has used it for the configured number of days.
    Idle,
    /// The volume fell below its configured free-space floor.
    Pressure,
}

impl Reason {
    pub(crate) fn describe(self) -> &'static str {
        match self {
            Reason::Orphaned => "its workspace was deleted",
            Reason::Idle => "no build used it within auto_clean_idle_targets_days",
            Reason::Pressure => "its volume fell below auto_recover_min_free_bytes",
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
    let enabled = config.auto_clean_orphaned_targets
        || config.auto_clean_idle_targets_days > 0
        || config.auto_clean_unused_units_days > 0
        || config.auto_recover_min_free_bytes > 0;
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
        Ok(swept) => {
            for (path, reason) in swept.removed {
                tracing::info!(
                    "removed target directory {} because {}",
                    path.display(),
                    reason.describe()
                );
            }
            for (path, pruned) in swept.pruned {
                tracing::info!(
                    "removed {} unused build units ({}) from {}",
                    pruned.units,
                    bytesize::ByteSize(pruned.bytes),
                    path.display()
                );
            }
            for (path, bytes) in swept.reclaimed {
                tracing::info!(
                    "target recovery returned about {} of free space from {}",
                    bytesize::ByteSize(bytes),
                    path.display()
                );
            }
        }
        Err(error) => tracing::warn!("target directory cleanup failed: {error:#}"),
    }
}

/// What one sweep did.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Swept {
    /// Target directories removed, and why.
    pub(crate) removed: Vec<(PathBuf, Reason)>,
    /// Target directories that stayed and lost unused units.
    pub(crate) pruned: Vec<(PathBuf, crate::unit_prune::Pruned)>,
    /// Observed free-space increase after each pressure-driven prune or removal.
    pub(crate) reclaimed: Vec<(PathBuf, u64)>,
}

/// How long a unit may go unused before it is removed, `None` when
/// unused-unit cleanup is off or the window cannot be expressed.
pub(crate) fn unit_window(days: u64) -> Option<Duration> {
    (days > 0)
        .then(|| days.checked_mul(DAY_SECS))
        .flatten()
        .map(Duration::from_secs)
}

/// Remove every tracked target directory `config` selects at `now`, and the
/// unused units of those that stay. A registry row whose directory is gone,
/// moved or no longer a derived target directory is forgotten; a directory a
/// running build holds is kept for the next check.
pub(crate) fn sweep(config: &Config, now: u64) -> anyhow::Result<Swept> {
    let store = Store::open(config)?;
    crate::worktree_discovery::discover(&store)?;
    let window = unit_window(config.auto_clean_unused_units_days);
    let at = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(now);
    let now = i64::try_from(now).unwrap_or(i64::MAX);
    let mut swept = Swept::default();
    for tracked in store.tracked_target_roots(0)? {
        if tracked.discovered {
            let missing_with_parent =
                !tracked.path.exists() && tracked.path.parent().is_some_and(Path::is_dir);
            let replaced = crate::machine::directory_identity(&tracked.path)
                .is_some_and(|identity| identity != tracked.identity);
            if missing_with_parent || replaced {
                store.forget_target_root(&tracked.path)?;
            }
            continue;
        }
        let orphaned = crate::cli::workspace_is_gone(&tracked.workspace_root);
        let idle = now.saturating_sub(tracked.last_seen);
        let intact = intact(&tracked);
        let Some(reason) = reason(config, orphaned, idle) else {
            if let Some(window) = window.filter(|_| intact) {
                let pruned = crate::unit_prune::prune(&config.cache_dir, &tracked.path, window, at);
                if pruned.units > 0 {
                    swept.pruned.push((tracked.path, pruned));
                }
            }
            continue;
        };
        if !intact {
            store.forget_target_root(&tracked.path)?;
            continue;
        }
        if crate::cli::target_in_use(&tracked.path) {
            continue;
        }
        match remove(&tracked.path, tracked.identity, now, &config.cache_dir) {
            Ok(true) => {
                store.forget_target_root(&tracked.path)?;
                swept.removed.push((tracked.path, reason));
            }
            Ok(false) => {}
            Err(error) => tracing::warn!(
                "could not remove target directory {}: {error}",
                tracked.path.display()
            ),
        }
    }
    prune_under_pressure(config, &store, at, &mut swept)?;
    recover_under_pressure(config, &store, now, &mut swept)?;
    Ok(swept)
}

/// Still the recorded, derived target directory, and not a source tree.
fn intact(tracked: &TrackedTargetRoot) -> bool {
    crate::machine::target_root_is_safe(&tracked.path, &tracked.workspace_root)
        && crate::machine::directory_identity(&tracked.path) == Some(tracked.identity)
        && !looks_like_a_source_root(&tracked.path)
}

/// What the daemon's next quiet pass would do to one tracked target.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub(crate) struct Plan {
    /// Why the whole target would go. Under pressure it goes only if its
    /// volume is still below the floor once unused units are pruned.
    pub(crate) remove: Option<Reason>,
    /// Unused build units that would go while the target stays.
    pub(crate) units: crate::unit_prune::Pruned,
}

/// What [`sweep`] would do to `tracked` at `now`, from the same checks,
/// without changing anything.
pub(crate) fn plan(config: &Config, tracked: &TrackedTargetRoot, now: u64) -> Plan {
    if tracked.discovered {
        return Plan::default();
    }
    let at = SystemTime::UNIX_EPOCH + Duration::from_secs(now);
    let now = i64::try_from(now).unwrap_or(i64::MAX);
    let intact = intact(tracked);
    let orphaned = crate::cli::workspace_is_gone(&tracked.workspace_root);
    let reason = reason(config, orphaned, now.saturating_sub(tracked.last_seen));
    if let Some(reason) = reason.filter(|_| intact && !crate::cli::target_in_use(&tracked.path)) {
        return Plan {
            remove: Some(reason),
            ..Plan::default()
        };
    }
    if !intact {
        return Plan::default();
    }
    let pressured = under_pressure(config, tracked);
    let window = unit_plan_window(
        pressured,
        reason.is_some(),
        unit_window(config.auto_clean_unused_units_days),
    );
    Plan {
        remove: (pressured
            && pressure_eligible(config, tracked, now)
            && crate::cli::target_reclaimable_bytes(&tracked.path) > 0)
            .then_some(Reason::Pressure),
        units: window.map_or_else(Default::default, |window| {
            crate::unit_prune::preview(&config.cache_dir, &tracked.path, window, at)
        }),
    }
}

/// The unit window a pass judges a staying target by: a day under pressure,
/// otherwise the configured one, and none for a target it would have removed
/// but a build held.
fn unit_plan_window(
    pressured: bool,
    selected: bool,
    configured: Option<Duration>,
) -> Option<Duration> {
    if pressured {
        Some(PRESSURE_UNIT_WINDOW)
    } else if selected {
        None
    } else {
        configured
    }
}

/// Whether `tracked` is a recorded target on a local volume below the
/// configured free-space floor.
fn under_pressure(config: &Config, tracked: &TrackedTargetRoot) -> bool {
    let floor = config.auto_recover_min_free_bytes;
    if floor == 0 || tracked.discovered {
        return false;
    }
    let Some(true) = crate::cache_fs::probe(&tracked.path).is_local else {
        return false;
    };
    kache_fs::volume_usage(&tracked.path)
        .is_some_and(|usage| floor <= usage.total && volume_below_floor(usage.free, floor))
}

/// On a volume below the floor, remove the units no build used for a day
/// from each target on it, until the volume is back above the floor.
fn prune_under_pressure(
    config: &Config,
    store: &Store,
    at: SystemTime,
    swept: &mut Swept,
) -> anyhow::Result<()> {
    for tracked in store.tracked_target_roots(0)? {
        if !under_pressure(config, &tracked) || !intact(&tracked) {
            continue;
        }
        let Some(before) = kache_fs::volume_usage(&tracked.path) else {
            continue;
        };
        let pruned =
            crate::unit_prune::prune(&config.cache_dir, &tracked.path, PRESSURE_UNIT_WINDOW, at);
        if pruned.units == 0 {
            continue;
        }
        let freed = kache_fs::volume_usage(&tracked.path)
            .map_or(0, |after| after.free.saturating_sub(before.free));
        swept.reclaimed.push((tracked.path.clone(), freed));
        swept.pruned.push((tracked.path, pruned));
    }
    Ok(())
}

struct PressureCandidate {
    path: PathBuf,
    workspace_root: PathBuf,
    identity: crate::machine::PathIdentity,
    reclaimable: u64,
    idle: i64,
}

/// The daemon's whole-target pressure policy, also used by `kache targets`
/// to preview which paths it may remove on its next quiet pass.
pub(crate) fn pressure_eligible(config: &Config, tracked: &TrackedTargetRoot, now: i64) -> bool {
    now.saturating_sub(tracked.last_seen) >= DAY_SECS as i64
        && under_pressure(config, tracked)
        && intact(tracked)
        && !crate::cli::target_in_use(&tracked.path)
}

fn volume_below_floor(free: u64, floor: u64) -> bool {
    free < floor
}

/// Remove older build targets on volumes below the explicitly configured
/// free-space floor. Every candidate is rescanned and revalidated at removal.
fn recover_under_pressure(
    config: &Config,
    store: &Store,
    now: i64,
    swept: &mut Swept,
) -> anyhow::Result<()> {
    let floor = config.auto_recover_min_free_bytes;
    if floor == 0 {
        return Ok(());
    }
    let mut candidates = Vec::new();
    for tracked in store.tracked_target_roots(0)? {
        if !pressure_eligible(config, &tracked, now) {
            continue;
        }
        let reclaimable = crate::cli::target_reclaimable_bytes(&tracked.path);
        if reclaimable == 0 {
            continue;
        }
        candidates.push(PressureCandidate {
            path: tracked.path,
            workspace_root: tracked.workspace_root,
            identity: tracked.identity,
            reclaimable,
            idle: now.saturating_sub(tracked.last_seen),
        });
    }
    candidates.sort_by_key(|candidate| {
        (
            std::cmp::Reverse(candidate.reclaimable),
            std::cmp::Reverse(candidate.idle),
        )
    });
    let mut stalled_volumes = std::collections::HashSet::new();
    for candidate in candidates {
        if stalled_volumes.contains(&candidate.identity.device) {
            continue;
        }
        let Some(parent) = candidate.path.parent() else {
            continue;
        };
        let Some(before) = kache_fs::volume_usage(parent) else {
            continue;
        };
        if !volume_below_floor(before.free, floor) {
            continue;
        }
        if !crate::machine::target_root_is_safe(&candidate.path, &candidate.workspace_root) {
            continue;
        }
        if looks_like_a_source_root(&candidate.path) {
            continue;
        }
        if crate::cli::target_in_use(&candidate.path) {
            continue;
        }
        match remove(&candidate.path, candidate.identity, now, &config.cache_dir) {
            Ok(true) => {
                store.forget_target_root(&candidate.path)?;
                let freed = kache_fs::volume_usage(parent)
                    .map_or(0, |after| after.free.saturating_sub(before.free));
                record_stalled_volume(&mut stalled_volumes, candidate.identity.device, freed);
                swept.reclaimed.push((candidate.path.clone(), freed));
                swept.removed.push((candidate.path, Reason::Pressure));
            }
            Ok(false) => {}
            Err(error) => tracing::warn!(
                "could not recover target directory {}: {error}",
                candidate.path.display()
            ),
        }
    }
    Ok(())
}

fn record_stalled_volume(stalled: &mut std::collections::HashSet<u64>, device: u64, freed: u64) {
    if freed == 0 {
        stalled.insert(device);
    }
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
/// `Ok(false)` when a build holds it, before or after the rename.
fn remove(
    target: &Path,
    expected: crate::machine::PathIdentity,
    now: i64,
    cache_dir: &Path,
) -> std::io::Result<bool> {
    remove_after_rename(target, expected, now, cache_dir, |_| {})
}

fn remove_after_rename(
    target: &Path,
    expected: crate::machine::PathIdentity,
    now: i64,
    cache_dir: &Path,
    after_rename: impl FnOnce(&Path),
) -> std::io::Result<bool> {
    let Some(_reservation) = crate::target_use::try_exclusive(cache_dir)? else {
        return Ok(false);
    };
    if crate::machine::directory_identity(target) != Some(expected) {
        return Ok(false);
    }
    let name = target.file_name().unwrap_or_default().to_string_lossy();
    let aside = target.with_file_name(format!(
        ".{name}.kache-removing-{}-{now}",
        std::process::id()
    ));
    if let Err(error) = std::fs::rename(target, &aside) {
        return refused(error, crate::cli::target_in_use(target));
    }
    after_rename(&aside);
    let unchanged = crate::machine::directory_identity(&aside) == Some(expected)
        && std::fs::symlink_metadata(&aside).is_ok_and(|meta| meta.file_type().is_dir())
        && !looks_like_a_source_root(&aside);
    if !unchanged || crate::cli::target_in_use(&aside) {
        if std::fs::symlink_metadata(target).is_err() {
            std::fs::rename(&aside, target)?;
        }
        return Ok(false);
    }
    std::fs::remove_dir_all(&aside)?;
    Ok(true)
}

/// A rename that fails while a build holds the directory means it is in
/// use: Windows refuses to rename a directory a build has files open in.
fn refused(error: std::io::Error, held: bool) -> std::io::Result<bool> {
    if held { Ok(false) } else { Err(error) }
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
    fn unit_cleanup_turns_the_check_on() {
        let dir = tempfile::tempdir().unwrap();
        let mut units_only = config(dir.path(), false, 0);
        units_only.auto_clean_unused_units_days = 30;
        let quiet = RequestClock::idle();
        assert!(due(
            &units_only,
            Trigger::Periodic(&quiet),
            Some(0),
            0,
            10_000
        ));
        assert_eq!(unit_window(0), None);
        assert_eq!(unit_window(30), Some(Duration::from_secs(30 * 86_400)));
        assert_eq!(unit_window(u64::MAX), None);
    }

    #[test]
    fn a_target_that_stays_loses_its_unused_units() {
        let root = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let mut units = config(cache.path(), true, 0);
        units.auto_clean_unused_units_days = 30;
        let store = Store::open(&units).unwrap();
        let (_, live) = tracked_target(&store, root.path(), "live");
        let long_ago =
            filetime::FileTime::from_unix_time((unix_now_secs() - 80 * DAY_SECS) as i64, 0);
        let unit = |hash: &str| {
            let dir = live.join("debug/build/dep").join(hash);
            std::fs::create_dir_all(dir.join("fingerprint")).unwrap();
            std::fs::write(dir.join("fingerprint/lib-dep"), "fp").unwrap();
            filetime::set_file_times(dir.join("fingerprint/lib-dep"), long_ago, long_ago).unwrap();
            dir
        };
        let stale = unit("0123456789abcdef");
        let current = unit("fedcba9876543210");
        let now = unix_now_secs();
        // The first check arms the target and removes nothing.
        assert_eq!(
            sweep(&units, now - 40 * DAY_SECS).unwrap(),
            Swept::default()
        );
        // A build used `current` since.
        let read = filetime::FileTime::from_unix_time(now as i64, 0);
        filetime::set_file_atime(current.join("fingerprint/lib-dep"), read).unwrap();
        let swept = sweep(&units, now).unwrap();
        if !crate::unit_prune::reads_visible(root.path()) {
            // Where reads do not show, nothing is ever removed.
            assert_eq!(swept, Swept::default());
            assert!(stale.exists());
            return;
        }
        assert!(swept.removed.is_empty());
        assert_eq!(swept.pruned.len(), 1);
        assert_eq!(swept.pruned[0].0, live);
        assert_eq!(swept.pruned[0].1.units, 1);
        assert!(!stale.exists());
        assert!(
            current.exists() && live.exists(),
            "the target and its used unit stay"
        );
        // Off: nothing is pruned, however long it waits.
        let stale = unit("0123456789abcdef");
        units.auto_clean_unused_units_days = 0;
        assert_eq!(
            sweep(&units, now + 400 * DAY_SECS).unwrap(),
            Swept::default()
        );
        assert!(stale.exists());
        // A target that is no longer the recorded one is not pruned.
        units.auto_clean_unused_units_days = 30;
        std::fs::remove_file(live.join("CACHEDIR.TAG")).unwrap();
        assert!(
            sweep(&units, now + 400 * DAY_SECS)
                .unwrap()
                .pruned
                .is_empty()
        );
        assert!(stale.exists());
    }

    /// A unit under `target`'s debug profile, last touched long ago.
    #[cfg(unix)]
    fn old_unit(target: &Path, hash: &str) -> PathBuf {
        let long_ago =
            filetime::FileTime::from_unix_time((unix_now_secs() - 80 * DAY_SECS) as i64, 0);
        let dir = target.join("debug/build/dep").join(hash);
        std::fs::create_dir_all(dir.join("fingerprint")).unwrap();
        std::fs::write(dir.join("fingerprint/lib-dep"), "fp").unwrap();
        filetime::set_file_times(dir.join("fingerprint/lib-dep"), long_ago, long_ago).unwrap();
        dir
    }

    #[cfg(unix)]
    fn root_of(store: &Store, path: &Path) -> TrackedTargetRoot {
        store
            .tracked_target_roots(0)
            .unwrap()
            .into_iter()
            .find(|tracked| tracked.path == path)
            .unwrap()
    }

    #[cfg(unix)]
    #[test]
    fn pressure_prunes_units_unused_for_a_day_from_a_target_still_in_use() {
        let root = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let mut pressure = config(cache.path(), false, 0);
        let store = Store::open(&pressure).unwrap();
        let (_, live) = tracked_target(&store, root.path(), "live");
        let stale = old_unit(&live, "0123456789abcdef");
        let current = old_unit(&live, "fedcba9876543210");
        let now = unix_now_secs();
        pressure.auto_recover_min_free_bytes = kache_fs::volume_usage(&live).unwrap().total - 1;
        // The first pass arms the target and prunes nothing.
        assert_eq!(
            sweep(&pressure, now - 2 * DAY_SECS).unwrap(),
            Swept::default()
        );
        let read = filetime::FileTime::from_unix_time(now as i64, 0);
        filetime::set_file_atime(current.join("fingerprint/lib-dep"), read).unwrap();
        let planned = plan(&pressure, &root_of(&store, &live), now);
        let swept = sweep(&pressure, now).unwrap();
        if !crate::unit_prune::reads_visible(root.path()) {
            assert_eq!(swept, Swept::default());
            assert!(stale.exists());
            return;
        }
        // A build used the target within the day, so it stays.
        assert!(swept.removed.is_empty(), "{swept:?}");
        assert_eq!(swept.pruned.len(), 1);
        assert_eq!(swept.pruned[0].0, live);
        assert_eq!(swept.pruned[0].1.units, 1);
        assert_eq!(
            planned.units, swept.pruned[0].1,
            "the plan matched the pass"
        );
        assert_eq!(planned.remove, None);
        assert_eq!(swept.reclaimed.len(), 1);
        assert_eq!(swept.reclaimed[0].0, live);
        assert!(!stale.exists());
        assert!(current.exists() && live.exists());

        // Above the floor, nothing is pruned.
        let stale = old_unit(&live, "0123456789abcdef");
        pressure.auto_recover_min_free_bytes = 1;
        assert_eq!(
            sweep(&pressure, now + 2 * DAY_SECS).unwrap(),
            Swept::default()
        );
        assert!(stale.exists());
        // Nor from a directory that is no longer the recorded target.
        pressure.auto_recover_min_free_bytes = kache_fs::volume_usage(&live).unwrap().total - 1;
        let tracked = root_of(&store, &live);
        // `current` was armed again by the last prune and not read since.
        assert_eq!(plan(&pressure, &tracked, now + 2 * DAY_SECS).units.units, 2);
        std::fs::remove_file(live.join("CACHEDIR.TAG")).unwrap();
        assert_eq!(
            plan(&pressure, &tracked, now + 2 * DAY_SECS),
            Plan::default()
        );
        assert!(
            sweep(&pressure, now + 2 * DAY_SECS)
                .unwrap()
                .pruned
                .is_empty()
        );
        assert!(stale.exists());
    }

    #[cfg(unix)]
    #[test]
    fn the_plan_says_what_the_next_pass_does_without_doing_it() {
        let root = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let mut policy = config(cache.path(), false, 3);
        let store = Store::open(&policy).unwrap();
        let (workspace, live) = tracked_target(&store, root.path(), "live");
        std::fs::write(live.join("debug/artifact"), vec![7; 64 * 1024]).unwrap();
        let now = unix_now_secs();
        let tracked = root_of(&store, &live);
        assert_eq!(plan(&policy, &tracked, now), Plan::default());
        assert_eq!(
            plan(&policy, &tracked, now + 3 * DAY_SECS).remove,
            Some(Reason::Idle)
        );
        let lock = std::fs::File::create(live.join("debug/.cargo-lock")).unwrap();
        lock.lock().unwrap();
        assert_eq!(plan(&policy, &tracked, now + 3 * DAY_SECS), Plan::default());
        // A fork elsewhere in the suite can hold a duplicate past the drop.
        lock.unlock().unwrap();
        drop(lock);

        policy.auto_clean_idle_targets_days = 0;
        policy.auto_recover_min_free_bytes = kache_fs::volume_usage(&live).unwrap().total - 1;
        assert_eq!(plan(&policy, &tracked, now).remove, None, "used today");
        assert_eq!(
            plan(&policy, &tracked, now + 2 * DAY_SECS).remove,
            Some(Reason::Pressure)
        );
        std::fs::remove_file(live.join("CACHEDIR.TAG")).unwrap();
        assert_eq!(plan(&policy, &tracked, now + 2 * DAY_SECS), Plan::default());
        std::fs::write(
            live.join("CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55\n",
        )
        .unwrap();

        policy.auto_clean_idle_targets_days = 3;
        std::fs::remove_file(live.join("CACHEDIR.TAG")).unwrap();
        assert_eq!(
            plan(&policy, &tracked, now + 3 * DAY_SECS),
            Plan::default(),
            "not the recorded target"
        );
        std::fs::write(
            live.join("CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55\n",
        )
        .unwrap();
        store.forget_target_root(&live).unwrap();
        store
            .remember_discovered_target_root(&live, &workspace)
            .unwrap();
        let discovered = root_of(&store, &live);
        assert_eq!(
            plan(&policy, &discovered, now + 3 * DAY_SECS),
            Plan::default()
        );
        assert!(live.join("debug/artifact").exists());
    }

    #[test]
    fn a_staying_target_is_judged_by_a_day_under_pressure() {
        let configured = Some(Duration::from_secs(30 * DAY_SECS));
        let day = Some(Duration::from_secs(DAY_SECS));
        assert_eq!(unit_plan_window(true, false, configured), day);
        assert_eq!(unit_plan_window(true, true, None), day);
        assert_eq!(unit_plan_window(false, true, configured), None);
        assert_eq!(unit_plan_window(false, false, configured), configured);
        assert_eq!(unit_plan_window(false, false, None), None);
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
        let mut pressure_only = off.clone();
        pressure_only.auto_recover_min_free_bytes = 1;
        assert!(due(&pressure_only, periodic, Some(0), 0, now));
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
        assert!(sweep(&both, now).unwrap().removed.is_empty());
        let tomorrow = now + DAY_SECS;
        let removed = sweep(&both, tomorrow).unwrap().removed;
        assert_eq!(removed, vec![(gone.clone(), Reason::Orphaned)]);
        assert!(!gone.exists());
        assert!(fresh.exists());
        assert_eq!(tracked(&store), vec![fresh.clone()]);

        // 31 days on, the fresh one is idle.
        let later = now + 31 * DAY_SECS;
        let orphans_only = config(cache.path(), true, 0);
        assert!(sweep(&orphans_only, later).unwrap().removed.is_empty());
        assert_eq!(
            sweep(&both, later).unwrap().removed,
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

    #[cfg(unix)]
    #[test]
    fn git_discovery_does_not_count_as_a_build_for_automatic_cleanup() {
        let root = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let mut idle = config(cache.path(), false, 1);
        let store = Store::open(&idle).unwrap();
        let (workspace, target) = tracked_target(&store, root.path(), "discovered");
        std::fs::write(target.join("debug/artifact"), vec![7; 64 * 1024]).unwrap();
        store.forget_target_root(&target).unwrap();
        store
            .remember_discovered_target_root(&target, &workspace)
            .unwrap();
        let later = unix_now_secs() + 2 * DAY_SECS;
        idle.auto_recover_min_free_bytes = kache_fs::volume_usage(&target).unwrap().total - 1;
        assert!(sweep(&idle, later).unwrap().removed.is_empty());
        assert!(target.exists());
        assert!(store.tracked_target_roots(0).unwrap()[0].discovered);
        std::fs::remove_dir_all(&target).unwrap();
        assert!(sweep(&idle, later).unwrap().removed.is_empty());
        assert!(store.tracked_target_roots(0).unwrap().is_empty());

        let (workspace, replaced) = tracked_target(&store, root.path(), "replaced");
        store.forget_target_root(&replaced).unwrap();
        assert!(
            store
                .remember_discovered_target_root(&replaced, &workspace)
                .unwrap()
        );
        std::fs::rename(&replaced, workspace.join("old-target")).unwrap();
        std::fs::create_dir_all(replaced.join("debug")).unwrap();
        std::fs::write(
            replaced.join("CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55",
        )
        .unwrap();
        assert!(sweep(&idle, later).unwrap().removed.is_empty());
        assert!(store.tracked_target_roots(0).unwrap().is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn pressure_recovery_waits_for_a_build_lock_then_removes_an_idle_target() {
        let root = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let mut pressure = config(cache.path(), false, 0);
        let store = Store::open(&pressure).unwrap();
        let (_, target) = tracked_target(&store, root.path(), "idle");
        std::fs::write(target.join("debug/artifact"), vec![7; 64 * 1024]).unwrap();
        assert!(crate::cli::target_reclaimable_bytes(&target) >= 64 * 1024);
        pressure.auto_recover_min_free_bytes = kache_fs::volume_usage(&target).unwrap().total - 1;
        let later = unix_now_secs() + 2 * DAY_SECS;
        let lock = std::fs::File::create(target.join("debug/.cargo-lock")).unwrap();
        lock.lock().unwrap();
        let tracked = store.tracked_target_roots(0).unwrap().remove(0);
        assert!(!pressure_eligible(&pressure, &tracked, later as i64));
        assert!(sweep(&pressure, later).unwrap().removed.is_empty());
        assert!(target.exists());
        // A fork elsewhere in the suite can hold a duplicate past the drop.
        lock.unlock().unwrap();
        drop(lock);
        for _ in 0..200 {
            if pressure_eligible(&pressure, &tracked, later as i64) {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(pressure_eligible(&pressure, &tracked, later as i64));
        let exact_day = tracked.last_seen + DAY_SECS as i64;
        assert!(!pressure_eligible(&pressure, &tracked, exact_day - 1));
        assert!(pressure_eligible(&pressure, &tracked, exact_day));
        let total = kache_fs::volume_usage(&target).unwrap().total;
        pressure.auto_recover_min_free_bytes = total + 1;
        assert!(!pressure_eligible(&pressure, &tracked, later as i64));
        pressure.auto_recover_min_free_bytes = total;
        assert!(pressure_eligible(&pressure, &tracked, later as i64));
        pressure.auto_recover_min_free_bytes = total - 1;
        let mut swept = Swept::default();
        for _ in 0..200 {
            swept = sweep(&pressure, later).unwrap();
            if !swept.removed.is_empty() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(swept.removed, vec![(target.clone(), Reason::Pressure)]);
        assert!(!target.exists());
    }

    #[test]
    fn pressure_stops_at_the_free_space_floor() {
        assert!(volume_below_floor(99, 100));
        assert!(!volume_below_floor(100, 100));
        assert!(!volume_below_floor(101, 100));
    }

    #[test]
    fn a_volume_stalls_only_when_removal_frees_no_space() {
        let mut stalled = std::collections::HashSet::new();
        record_stalled_volume(&mut stalled, 3, 1);
        assert!(!stalled.contains(&3));
        record_stalled_volume(&mut stalled, 4, 0);
        assert!(stalled.contains(&4));
    }

    #[test]
    fn guarded_remove_keeps_a_source_root() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("target");
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("Cargo.toml"), "[workspace]\n").unwrap();
        let identity = crate::machine::directory_identity(&target).unwrap();
        let cache = tempfile::tempdir().unwrap();
        assert!(!remove(&target, identity, 7, cache.path()).unwrap());
        assert!(target.join("Cargo.toml").exists());
    }

    #[test]
    fn guarded_remove_rechecks_directory_identity_after_rename() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("target");
        let saved = root.path().join("saved");
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("artifact"), b"original").unwrap();
        let identity = crate::machine::directory_identity(&target).unwrap();
        let cache = tempfile::tempdir().unwrap();
        let removed = remove_after_rename(&target, identity, 7, cache.path(), |aside| {
            std::fs::rename(aside, &saved).unwrap();
            std::fs::create_dir(aside).unwrap();
        })
        .unwrap();
        assert!(!removed);
        assert_eq!(std::fs::read(saved.join("artifact")).unwrap(), b"original");
        assert!(target.is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn guarded_remove_keeps_a_symlink() {
        let root = tempfile::tempdir().unwrap();
        let real = root.path().join("real");
        let link = root.path().join("target");
        std::fs::create_dir(&real).unwrap();
        std::os::unix::fs::symlink(&real, &link).unwrap();
        let identity = crate::machine::directory_identity(&real).unwrap();
        let cache = tempfile::tempdir().unwrap();
        assert!(!remove(&link, identity, 7, cache.path()).unwrap());
        assert!(link.symlink_metadata().unwrap().file_type().is_symlink());
        assert!(real.exists());
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
        assert!(sweep(&orphans, later).unwrap().removed.is_empty());
        for kept in [&replaced, &untagged, &manifest, &repo, &busy] {
            assert!(kept.exists(), "{}", kept.display());
        }
        assert_eq!(tracked(&store), vec![busy.clone()]);

        // A fork elsewhere in the suite can hold a duplicate past the drop.
        lock.unlock().unwrap();
        drop(lock);
        // A process another test forks in this instant shares the lock until
        // it execs, so the release can take a moment to show.
        let mut removed = Vec::new();
        for _ in 0..200 {
            removed = sweep(&orphans, later).unwrap().removed;
            if !removed.is_empty() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(removed, vec![(busy.clone(), Reason::Orphaned)]);
    }

    #[test]
    fn a_refused_rename_of_a_held_directory_is_in_use() {
        let denied = || std::io::Error::from(std::io::ErrorKind::PermissionDenied);
        assert!(!refused(denied(), true).unwrap());
        assert_eq!(
            refused(denied(), false).unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied
        );
    }

    #[test]
    fn a_build_that_holds_the_directory_gets_it_back() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("target");
        std::fs::create_dir_all(target.join("debug")).unwrap();
        let lock = std::fs::File::create(target.join("debug/.cargo-lock")).unwrap();
        lock.lock().unwrap();
        let identity = crate::machine::directory_identity(&target).unwrap();
        let cache = tempfile::tempdir().unwrap();
        assert!(!remove(&target, identity, 7, cache.path()).unwrap());
        assert!(target.join("debug/.cargo-lock").exists());
        assert!(target.exists());
        // A fork elsewhere in the suite can hold a duplicate past the drop.
        lock.unlock().unwrap();
        drop(lock);
        assert!(remove(&target, identity, 7, cache.path()).unwrap());
        assert!(!target.exists());
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
        assert!(!remove(&target, identity, 7, cache.path()).unwrap());
    }

    #[test]
    fn a_running_cargo_command_keeps_its_target_until_exit() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("target");
        std::fs::create_dir_all(target.join("debug")).unwrap();
        let identity = crate::machine::directory_identity(&target).unwrap();
        let command = crate::target_use::shared(root.path()).unwrap();
        assert!(!remove(&target, identity, 7, root.path()).unwrap());
        assert!(target.exists());
        drop(command);
        assert!(remove(&target, identity, 7, root.path()).unwrap());
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
