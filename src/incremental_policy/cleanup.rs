//! Reclaim idle managed graphs during the existing target-unit cleanup pass.

use super::*;
use crate::unit_prune::Pruned;
use std::time::Duration;

pub(crate) fn prune_stale(
    cache_dir: &Path,
    target: &Path,
    window: Duration,
    at: SystemTime,
) -> Pruned {
    let Ok(Some(_reservation)) = crate::target_use::try_exclusive(cache_dir) else {
        return Pruned::default();
    };
    sweep(
        target,
        window,
        at,
        true,
        crate::cache_fs::probe(target).is_local,
    )
}

pub(crate) fn preview_stale(target: &Path, window: Duration, at: SystemTime) -> Pruned {
    sweep(
        target,
        window,
        at,
        false,
        crate::cache_fs::probe(target).is_local,
    )
}

fn sweep(
    target: &Path,
    window: Duration,
    at: SystemTime,
    remove: bool,
    local: Option<bool>,
) -> Pruned {
    let mut pruned = Pruned::default();
    if !real_directory(target) || local != Some(true) {
        return pruned;
    }
    // Profiles can outlive their last Cargo fingerprint. Look at both the
    // native and target-triple layouts without descending into graph data.
    for profile in crate::unit_prune::subdirectories(target)
        .into_iter()
        .flat_map(|profile| {
            let nested = crate::unit_prune::subdirectories(&profile);
            std::iter::once(profile).chain(nested)
        })
    {
        let auto_root = profile.join(AUTO_ROOT);
        let root = auto_root.join(POLICY_VERSION);
        if !real_directory(&auto_root) || !real_directory(&root) {
            continue;
        }
        let _profile_locks = if remove {
            let Some(held) = crate::unit_prune::hold(&profile) else {
                continue;
            };
            Some(held)
        } else {
            None
        };
        let Ok(entries) = fs::read_dir(&root) else {
            continue;
        };
        for entry in entries.flatten() {
            let Some(key) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if !valid_hex_digest(&key) || !real_directory(&entry.path()) {
                continue;
            }
            let unit_dir = entry.path();
            let unit = AdaptiveUnit {
                source_file: None,
                original_incremental: profile.join("incremental"),
                policy_guard: Vec::new(),
                unit_key: key,
                state_path: unit_dir.join("state.json"),
                lock_path: unit_dir.join("unit.lock"),
                rustc_dir: unit_dir.join("rustc"),
                unit_dir,
            };
            if !fs::symlink_metadata(&unit.lock_path).is_ok_and(|metadata| metadata.is_file()) {
                continue;
            }
            let _unit_lock = if remove {
                let Some(lock) = unit.lock() else {
                    continue;
                };
                Some(lock)
            } else {
                None
            };
            let Some(bytes) = stale_bytes(&unit, window, at) else {
                continue;
            };
            // Keep the directory and lock inode: a compiler may already have
            // opened that lock. Unlinking it would permit two independent locks.
            if !remove || reset_locked(&unit) {
                pruned.units += 1;
                pruned.bytes += bytes;
            }
        }
    }
    pruned
}

fn stale_bytes(unit: &AdaptiveUnit, window: Duration, at: SystemTime) -> Option<u64> {
    if !real_directory(&unit.rustc_dir) {
        return None;
    }
    let last_used_secs = match unit.load_state() {
        LoadedState::Valid(state) => state.last_used_secs,
        // Immediate leases with no prior learning remove state.json at exit.
        LoadedState::Missing => 0,
        LoadedState::Corrupt | LoadedState::Unavailable => return None,
    };
    // Every lease replaces or removes state.json, refreshing its parent mtime.
    // Also honor the recorded clock when it is ahead of the filesystem clock.
    let modified = fs::symlink_metadata(&unit.unit_dir).ok()?.modified().ok()?;
    let recorded = UNIX_EPOCH.checked_add(Duration::from_secs(last_used_secs))?;
    let last_used = modified.max(recorded);
    if at.duration_since(last_used).ok()? < window {
        return None;
    }
    Some(crate::unit_prune::size(&unit.rustc_dir) + crate::unit_prune::size(&unit.state_path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unverified_filesystem_keeps_graphs_and_creates_no_cargo_locks() {
        for local in [None, Some(false)] {
            let dir = tempfile::tempdir().unwrap();
            let profile = dir.path().join("debug");
            let unit = profile
                .join(AUTO_ROOT)
                .join(POLICY_VERSION)
                .join("a".repeat(64));
            let graph = unit.join("rustc");
            fs::create_dir_all(&graph).unwrap();
            fs::write(graph.join("graph.bin"), b"payload").unwrap();
            fs::write(unit.join("unit.lock"), b"").unwrap();
            filetime::set_file_mtime(&unit, filetime::FileTime::from_unix_time(100, 0)).unwrap();
            let window = Duration::from_secs(10);
            let at = UNIX_EPOCH + Duration::from_secs(110);
            assert_eq!(sweep(dir.path(), window, at, true, local).units, 0);
            assert_eq!(fs::read(graph.join("graph.bin")).unwrap(), b"payload");
            assert!(!profile.join(".cargo-lock").exists());
            assert_eq!(sweep(dir.path(), window, at, true, Some(true)).units, 1);
            assert!(!graph.exists());
        }
    }
}
