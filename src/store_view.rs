//! One inventory for commands that inspect the main store and volume shards.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::daemon::StatsEntry;
use crate::store::{BlobStats, Store};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoreSummary {
    pub path: PathBuf,
    pub bytes: u64,
    pub max_size: u64,
    pub entries: usize,
    pub blob_stats: Option<BlobStats>,
    pub error: Option<String>,
}

#[derive(Default)]
pub(crate) struct StoreView {
    pub total_size: u64,
    pub max_size: u64,
    pub entry_count: usize,
    pub entries: Vec<StatsEntry>,
    pub blob_stats: BlobStats,
    pub stores: Vec<StoreSummary>,
    pub eviction_evidence: Vec<Result<kache_store::EvictionHorizonEvidence, String>>,
}

fn identity(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

/// Include missing shards in diagnostics, but never open (and recreate) them.
/// Several volume mappings may share one store; inspect that directory once.
pub(crate) fn shard_configs(config: &Config) -> Vec<Config> {
    let mut seen = BTreeSet::from([identity(&config.cache_dir)]);
    config
        .volume_stores
        .iter()
        .filter(|shard| seen.insert(identity(&shard.store)))
        .map(|shard| config.for_volume_store(shard, crate::volume_gc::filesystem_bytes))
        .collect()
}

pub(crate) fn read(config: &Config, include_entries: bool, sort: &str) -> Result<StoreView> {
    let main = Store::open(config)?;
    read_with_main(config, &main, include_entries, sort)
}

/// Collect report observations while the existing inventory handles are open.
/// The evidence query itself makes no writes or additional store opens.
pub(crate) fn read_report(config: &Config, as_of: i64, horizon: u64) -> Result<StoreView> {
    let main = Store::open(config)?;
    read_with_evidence(config, &main, false, "name", Some((as_of, horizon)))
}

pub(crate) fn read_with_main(
    config: &Config,
    main: &Store,
    include_entries: bool,
    sort: &str,
) -> Result<StoreView> {
    read_with_evidence(config, main, include_entries, sort, None)
}

fn read_with_evidence(
    config: &Config,
    main: &Store,
    include_entries: bool,
    sort: &str,
    snapshot: Option<(i64, u64)>,
) -> Result<StoreView> {
    let shards = shard_configs(config);
    // Keep the ordinary single-store stats path on COUNT/SUM queries.
    let collect_entries = include_entries || !shards.is_empty();
    let mut view = StoreView::default();
    let mut by_key = BTreeMap::new();
    add_store(
        &mut view,
        &mut by_key,
        config,
        main,
        collect_entries,
        snapshot,
    )?;
    for shard in shards {
        let result = if crate::volume_gc::shard_has_index(&shard.cache_dir) {
            Store::open(&shard).and_then(|store| {
                add_store(
                    &mut view,
                    &mut by_key,
                    &shard,
                    &store,
                    collect_entries,
                    snapshot,
                )
            })
        } else {
            Err(anyhow::anyhow!(
                "no index (not initialized or volume unavailable)"
            ))
        };
        if let Err(error) = result {
            if snapshot.is_some() {
                view.eviction_evidence.push(Err("index unavailable".into()));
            }
            view.stores.push(StoreSummary {
                path: shard.cache_dir,
                max_size: shard.max_size,
                error: Some(error.to_string()),
                ..Default::default()
            });
        }
    }
    if collect_entries {
        view.entry_count = by_key.len();
    }
    if include_entries {
        view.entries = by_key.into_values().collect();
        view.entries.sort_by(|a, b| {
            let order = match sort {
                "size" => b.size.cmp(&a.size),
                "hits" => b.hit_count.cmp(&a.hit_count),
                "age" => a.created_at.cmp(&b.created_at),
                _ => a.crate_name.cmp(&b.crate_name),
            };
            order.then_with(|| a.cache_key.cmp(&b.cache_key))
        });
    }
    Ok(view)
}

fn add_store(
    view: &mut StoreView,
    by_key: &mut BTreeMap<String, StatsEntry>,
    config: &Config,
    store: &Store,
    collect_entries: bool,
    snapshot: Option<(i64, u64)>,
) -> Result<()> {
    // Complete all fallible reads before contributing this store's totals.
    let bytes = store.total_size()?;
    let entries = store.entry_count()?;
    let blobs = store.blob_stats()?;
    let rows = if collect_entries {
        store.list_entries("name")?
    } else {
        Vec::new()
    };
    for row in rows {
        let entry = by_key
            .entry(row.cache_key.clone())
            .or_insert_with(|| StatsEntry {
                cache_key: row.cache_key,
                crate_name: row.crate_name,
                crate_type: row.crate_type,
                profile: row.profile,
                size: row.size,
                hit_count: 0,
                created_at: row.created_at.clone(),
                last_accessed: row.last_accessed.clone(),
                content_hash: row.content_hash,
                store_dirs: Vec::new(),
            });
        entry.hit_count += row.hit_count;
        entry.created_at = std::cmp::min(entry.created_at.clone(), row.created_at);
        entry.last_accessed = std::cmp::max(entry.last_accessed.clone(), row.last_accessed);
        entry.store_dirs.push(config.cache_dir.clone());
    }
    view.total_size += bytes;
    view.max_size += config.max_size;
    view.entry_count += entries;
    view.blob_stats.total_blobs += blobs.total_blobs;
    view.blob_stats.total_blob_size += blobs.total_blob_size;
    view.blob_stats.total_logical_size += blobs.total_logical_size;
    view.blob_stats.savings += blobs.savings;
    if let Some((as_of, horizon)) = snapshot {
        view.eviction_evidence.push(
            store
                .eviction_horizon_evidence(as_of, horizon)
                .map_err(|_| "eviction observations unavailable".into()),
        );
    }
    view.stores.push(StoreSummary {
        path: config.cache_dir.clone(),
        bytes,
        max_size: config.max_size,
        entries,
        blob_stats: Some(blobs),
        error: None,
    });
    Ok(())
}

impl StatsEntry {
    pub(crate) fn meta_path(&self) -> PathBuf {
        self.store_dirs[0]
            .join("store")
            .join(&self.cache_key)
            .join("meta.json")
    }

    pub(crate) fn store_locations(&self) -> String {
        self.store_dirs
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[cfg(test)]
mod tests;
