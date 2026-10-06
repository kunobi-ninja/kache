//! Background store eviction when the filesystem runs short of space.
//! Wrapper hints and post-upload checks run this without waiting for idle
//! builds. Target cleanup keeps its separate quiet-machine rules.

use crate::config::Config;
use crate::store::Store;
use anyhow::Result;
use kache_fs::VolumeUsage;
use std::path::Path;
use std::time::{Duration, Instant};

const INTERVAL: Duration = Duration::from_secs(300);
const WRITE_BUDGET: Duration = Duration::from_secs(5);
const LAST_CHECK: &str = "disk-recovery.last";

fn last_check(cache_dir: &Path) -> Result<u64> {
    match std::fs::read_to_string(cache_dir.join(LAST_CHECK)) {
        Ok(text) => Ok(text.trim().parse()?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
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
    last_check(&config.cache_dir).is_ok_and(|last| due(last, crate::maintenance::unix_now_secs()))
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
    crate::atomic::atomic_replace(
        &config.cache_dir.join(LAST_CHECK),
        crate::maintenance::unix_now_secs().to_string().as_bytes(),
    )?;
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
}
