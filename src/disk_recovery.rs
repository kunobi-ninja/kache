//! Background store eviction when the filesystem runs short of space.
//! Wrapper hints and post-upload checks run this without waiting for idle
//! builds. Target cleanup keeps its separate quiet-machine rules.

use crate::config::Config;
use crate::store::Store;
use anyhow::Result;
use kache_fs::VolumeUsage;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

const INTERVAL: Duration = Duration::from_secs(300);
const WRITE_BUDGET: Duration = Duration::from_secs(5);
const LAST_CHECK: &str = "disk-recovery.last";

/// When this process last recovered each store whose timestamp it could not
/// write, by cache dir. On a full disk every write of the timestamp fails;
/// this keeps the process to the same interval.
static UNRECORDED: Mutex<BTreeMap<PathBuf, u64>> = Mutex::new(BTreeMap::new());

fn last_check(cache_dir: &Path) -> Result<u64> {
    match std::fs::read_to_string(cache_dir.join(LAST_CHECK)) {
        Ok(text) => Ok(text.trim().parse()?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
}

fn unrecorded() -> MutexGuard<'static, BTreeMap<PathBuf, u64>> {
    UNRECORDED.lock().unwrap_or_else(PoisonError::into_inner)
}

/// This process's last recovery of `cache_dir` without a timestamp, `0`
/// when there is none.
fn last_unrecorded(cache_dir: &Path) -> u64 {
    unrecorded().get(cache_dir).copied().unwrap_or(0)
}

fn due(last: u64, now: u64) -> bool {
    last == 0 || now < last || now - last >= INTERVAL.as_secs()
}

fn below_floor(floor: u64, usage: Option<VolumeUsage>) -> bool {
    usage.is_some_and(|usage| floor <= usage.total && usage.free < floor)
}

fn keep_recovering(floor: u64, elapsed: Duration, usage: Option<VolumeUsage>) -> bool {
    elapsed < WRITE_BUDGET && below_floor(floor, usage)
}

/// Cheap check shared by wrappers and collectors. An unreadable timestamp
/// or volume skips recovery. The collector checks again under `gc.lock`.
pub(crate) fn wanted(config: &Config) -> bool {
    if !config.auto_gc || config.auto_recover_min_free_bytes == 0 {
        return false;
    }
    let now = crate::maintenance::unix_now_secs();
    last_check(&config.cache_dir).is_ok_and(|last| due(last, now))
        && due(last_unrecorded(&config.cache_dir), now)
        && below_floor(
            config.auto_recover_min_free_bytes,
            kache_fs::volume_usage(&config.cache_dir),
        )
}

/// The daemon and detached worker share the lock and timestamp. No work is
/// done while another collector owns this store. Never propagates a failure
/// to the build that asked for recovery.
pub(crate) fn run(config: &Config) {
    if let Err(error) = recover(config) {
        tracing::warn!(store = %config.cache_dir.display(), "disk recovery failed: {error:#}");
    }
}

fn recover(config: &Config) -> Result<()> {
    recover_with(config, |path, now| {
        crate::atomic::atomic_replace(path, now.to_string().as_bytes())
    })
}

/// [`recover`], writing the timestamp with `record`.
fn recover_with(config: &Config, record: impl FnOnce(&Path, u64) -> Result<()>) -> Result<()> {
    if !wanted(config) {
        return Ok(());
    }
    let store = Store::open(config)?;
    let Some(_lock) = store.try_gc_lock()? else {
        return Ok(());
    };
    if !wanted(config) {
        return Ok(());
    }
    let now = crate::maintenance::unix_now_secs();
    // A full disk refuses the timestamp just when space is needed most:
    // evict anyway, and pace this process from memory instead.
    if let Err(error) = record(&config.cache_dir.join(LAST_CHECK), now) {
        tracing::warn!(
            store = %config.cache_dir.display(),
            "cannot record the disk recovery time, recovering anyway: {error:#}"
        );
        unrecorded().insert(config.cache_dir.clone(), now);
    }
    let started = Instant::now();
    let mut stats = store.evict_for_disk_pressure(|| {
        !keep_recovering(
            config.auto_recover_min_free_bytes,
            started.elapsed(),
            kache_fs::volume_usage(&config.cache_dir),
        )
    })?;
    stats.duration_ms = started.elapsed().as_millis() as u64;
    crate::report::record_gc_run(
        config,
        "disk-recovery",
        crate::store::SweepOrigin::Automatic,
        &stats,
    )?;
    tracing::info!(
        store = %config.cache_dir.display(),
        entries = stats.entries_evicted,
        elapsed_ms = stats.duration_ms,
        "disk recovery finished"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pressure_requires_a_valid_floor_and_a_readable_volume() {
        let usage = Some(VolumeUsage {
            total: 100,
            free: 10,
        });
        assert!(below_floor(11, usage));
        assert!(below_floor(100, usage));
        assert!(!below_floor(10, usage));
        assert!(!below_floor(9, usage));
        assert!(!below_floor(101, usage));
        assert!(!below_floor(
            0,
            Some(VolumeUsage {
                total: 100,
                free: 0
            })
        ));
        assert!(!below_floor(11, None));
    }

    #[test]
    fn repeated_pressure_checks_back_off_and_handle_clock_changes() {
        assert_eq!(INTERVAL, Duration::from_secs(300));
        assert_eq!(WRITE_BUDGET, Duration::from_secs(5));
        assert!(due(0, 1));
        assert!(due(50, 49));
        assert!(!due(50, 50));
        assert!(!due(50, 349));
        assert!(due(50, 350));
        assert!(due(50, 351));
        let pressure = Some(VolumeUsage {
            total: 100,
            free: 10,
        });
        assert!(keep_recovering(11, Duration::from_secs(4), pressure));
        assert!(!keep_recovering(11, Duration::from_secs(5), pressure));
        assert!(!keep_recovering(11, Duration::ZERO, None));
        assert!(!keep_recovering(10, Duration::ZERO, pressure));
    }

    #[test]
    fn missing_timestamp_is_due_but_bad_or_unreadable_records_skip() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(last_check(dir.path()).unwrap(), 0);
        let path = dir.path().join(LAST_CHECK);
        std::fs::write(&path, " 42\n").unwrap();
        assert_eq!(last_check(dir.path()).unwrap(), 42);
        std::fs::write(&path, "bad").unwrap();
        assert!(last_check(dir.path()).is_err());
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(path).unwrap();
        assert!(last_check(dir.path()).is_err());
    }

    #[test]
    fn disabled_and_unreadable_stores_are_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::test_support::test_config(dir.path().join("missing"));
        assert!(!wanted(&config));
        run(&config);
        assert!(!config.cache_dir.exists());
        config.auto_recover_min_free_bytes = 1;
        assert!(!wanted(&config));
        run(&config);
        assert!(!config.cache_dir.exists());
    }

    /// An entry no eviction protection covers: accessed an hour ago, with
    /// no clone of its blob left outside the store.
    #[cfg(unix)]
    fn put_evictable(store: &Store, dir: &Path, key: &str) {
        let source = dir.join(format!("{key}.rlib"));
        std::fs::write(&source, key).unwrap();
        store
            .put(
                key,
                key,
                &["lib".into()],
                &[],
                "host",
                "dev",
                &[(source.clone(), format!("{key}.rlib"))],
                "",
                "",
            )
            .unwrap();
        store.remove_clone_for_test(&source);
        store.set_last_accessed_for_test(key, "-1 hour");
    }

    #[test]
    #[cfg(unix)]
    fn a_full_disk_that_refuses_the_timestamp_still_recovers_once_an_interval() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::test_support::test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        config.auto_recover_min_free_bytes =
            kache_fs::volume_usage(&config.cache_dir).unwrap().total;
        let full = |_: &Path, _: u64| -> Result<()> {
            Err(std::io::Error::from(std::io::ErrorKind::StorageFull).into())
        };
        put_evictable(&store, dir.path(), "old");
        recover_with(&config, full).unwrap();
        assert!(!store.contains("old"), "evicted without a timestamp");
        assert!(!config.cache_dir.join(LAST_CHECK).exists());

        // This process keeps the interval without the file.
        put_evictable(&store, dir.path(), "next");
        assert!(!wanted(&config));
        run(&config);
        assert!(store.contains("next"));

        let interval_ago = crate::maintenance::unix_now_secs() - INTERVAL.as_secs();
        unrecorded().insert(config.cache_dir.clone(), interval_ago);
        run(&config);
        assert!(!store.contains("next"), "the next interval recovers");
        assert!(config.cache_dir.join(LAST_CHECK).exists());
    }
}
