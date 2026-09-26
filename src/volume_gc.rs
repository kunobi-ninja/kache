//! GC for `[cache.volumes]` shards (kunobi-ninja/kache#974).
//!
//! A shard is a full store dir (`blobs/`, `index.db`, `staging/`), so every
//! GC driver sweeps it the way it sweeps the main store: with a config whose
//! `cache_dir` is the shard, under the shard's own `gc.lock`, against the
//! shard's own budget and auto-GC backoff. A shard that cannot be swept is
//! logged and skipped; it never fails the main store's GC.

use crate::config::Config;
use anyhow::Result;
use std::path::Path;

/// Total bytes of the filesystem that holds `path`, for a shard's default
/// budget. `None` when the probe fails.
pub(crate) fn filesystem_bytes(path: &Path) -> Option<u64> {
    crate::cache_fs::probe(path).total_bytes
}

/// Whether GC can open the shard at `store` without creating it: its index
/// exists. A missing dir, an unmounted volume's empty mount point, and a
/// shard no build has written to yet have nothing to sweep, and opening
/// them would create the dir.
pub(crate) fn shard_has_index(store: &Path) -> bool {
    store.join("index.db").is_file()
}

/// Configs of the shards GC can sweep, each with its own `cache_dir` and
/// budget. Shards without an index are logged and left out.
pub(crate) fn shard_gc_configs(
    config: &Config,
    filesystem_bytes: impl Fn(&Path) -> Option<u64>,
) -> Vec<Config> {
    let (sweepable, absent): (Vec<_>, Vec<_>) = config
        .volume_stores
        .iter()
        .partition(|shard| shard_has_index(&shard.store));
    for shard in absent {
        tracing::info!(
            volume = %shard.volume,
            store = %shard.store.display(),
            "gc: skipping volume shard with no index (missing or unmounted)"
        );
    }
    sweepable
        .into_iter()
        .map(|shard| config.for_volume_store(shard, &filesystem_bytes))
        .collect()
}

/// Run `sweep` on each shard in turn. A shard whose sweep fails is logged
/// and the others still run. Returns the shards that were swept, with what
/// their sweep returned.
pub(crate) fn run_on_shards<T>(
    shards: Vec<Config>,
    mut sweep: impl FnMut(&Config) -> Result<T>,
) -> Vec<(Config, T)> {
    shards
        .into_iter()
        .filter_map(|shard| match sweep(&shard) {
            Ok(outcome) => Some((shard, outcome)),
            Err(error) => {
                tracing::warn!(
                    store = %shard.cache_dir.display(),
                    "gc: volume shard sweep failed: {error:#}"
                );
                None
            }
        })
        .collect()
}

/// Sweep every sweepable `[cache.volumes]` shard of `config` with `sweep`.
pub(crate) fn run_on_volume_shards<T>(
    config: &Config,
    sweep: impl FnMut(&Config) -> Result<T>,
) -> Vec<(Config, T)> {
    run_on_shards(shard_gc_configs(config, filesystem_bytes), sweep)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::VolumeStore;
    use std::path::PathBuf;

    const GIB: u64 = 1024 * 1024 * 1024;

    fn shard(store: PathBuf, max_size: Option<u64>) -> VolumeStore {
        VolumeStore {
            volume: "/mnt/vol/".into(),
            store,
            max_size,
        }
    }

    #[test]
    fn filesystem_bytes_is_the_cache_fs_probe() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = filesystem_bytes(dir.path());
        assert_eq!(bytes, crate::cache_fs::probe(dir.path()).total_bytes);
        if cfg!(any(target_os = "linux", target_os = "macos")) {
            assert!(bytes.is_some_and(|n| n > 1), "{bytes:?}");
        }
    }

    #[test]
    fn a_shard_needs_an_index_to_be_swept() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!shard_has_index(&dir.path().join("missing")));
        assert!(!shard_has_index(dir.path()), "an empty dir has no index");
        std::fs::create_dir(dir.path().join("index.db")).unwrap();
        assert!(!shard_has_index(dir.path()), "index.db must be a file");
        std::fs::remove_dir(dir.path().join("index.db")).unwrap();
        std::fs::write(dir.path().join("index.db"), b"").unwrap();
        assert!(shard_has_index(dir.path()));
    }

    #[test]
    fn shard_configs_skip_shards_without_an_index_and_create_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::test_support::test_config(dir.path().join("main"));
        let live = dir.path().join("live");
        std::fs::create_dir_all(&live).unwrap();
        std::fs::write(live.join("index.db"), b"").unwrap();
        let missing = dir.path().join("unmounted");
        config.volume_stores = vec![shard(missing.clone(), None), shard(live.clone(), None)];

        let configs = shard_gc_configs(&config, |_| Some(200 * GIB));
        let dirs: Vec<_> = configs.iter().map(|c| c.cache_dir.clone()).collect();
        assert_eq!(dirs, vec![live]);
        assert_eq!(configs[0].max_size, 10 * GIB, "5% of the shard's 200GiB");
        assert!(!missing.exists(), "skipping a shard must not create it");
    }

    #[test]
    fn a_failing_shard_is_skipped_and_the_rest_still_run() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(dir.path().to_path_buf());
        let shards = ["a", "b", "c"].map(|name| {
            let mut shard = config.clone();
            shard.cache_dir = dir.path().join(name);
            shard
        });
        let mut visited = Vec::new();
        let swept = run_on_shards(shards.to_vec(), |shard| {
            let name = shard.cache_dir.file_name().unwrap().to_owned();
            visited.push(name.clone());
            if name == "b" {
                anyhow::bail!("unreadable index");
            }
            Ok(name)
        });
        assert_eq!(visited, ["a", "b", "c"], "a failure must not stop the loop");
        let names: Vec<_> = swept.iter().map(|(_, name)| name.clone()).collect();
        assert_eq!(names, ["a", "c"]);
        assert_eq!(swept[1].0.cache_dir, dir.path().join("c"));
    }

    #[test]
    fn run_on_volume_shards_visits_each_shard_with_an_index() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::test_support::test_config(dir.path().join("main"));
        let live = dir.path().join("live");
        std::fs::create_dir_all(&live).unwrap();
        std::fs::write(live.join("index.db"), b"").unwrap();
        config.volume_stores = vec![
            shard(dir.path().join("gone"), Some(7)),
            shard(live.clone(), Some(7)),
        ];
        let swept = run_on_volume_shards(&config, |shard| Ok(shard.max_size));
        assert_eq!(swept.len(), 1);
        assert_eq!(swept[0].0.cache_dir, live);
        assert_eq!(swept[0].1, 7, "the explicit budget applies to the shard");
    }
}
