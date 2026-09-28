use super::*;
use crate::config::VolumeStore;

fn fixture() -> (tempfile::TempDir, Config, Config) {
    let dir = tempfile::tempdir().unwrap();
    let mut main = crate::test_support::test_config(dir.path().join("main"));
    main.max_size = 1000;
    let shard = VolumeStore {
        volume: dir.path().join("build").to_string_lossy().into_owned(),
        store: dir.path().join("shard"),
        max_size: Some(2000),
    };
    let shard_config = main.for_volume_store(&shard, |_| None);
    main.volume_stores = vec![shard];
    (dir, main, shard_config)
}

pub(crate) fn put(store: &Store, root: &Path, name: &str, size: usize) -> String {
    let key = blake3::hash(name.as_bytes()).to_hex().to_string();
    let output = tempfile::NamedTempFile::new_in(root).unwrap();
    std::fs::write(output.path(), vec![size as u8; size]).unwrap();
    store
        .put(
            &key,
            name,
            &["rlib".into()],
            &[],
            "target",
            "debug",
            &[(output.path().to_path_buf(), format!("{name}.rlib"))],
            "",
            "",
        )
        .unwrap();
    key
}

#[test]
fn volume_inventory_counts_keys_once_and_every_stored_copy_in_bytes() {
    let (dir, config, shard_config) = fixture();
    let main = Store::open(&config).unwrap();
    let shard = Store::open(&shard_config).unwrap();
    let common = put(&main, dir.path(), "common", 10);
    put(&main, dir.path(), "main-only", 20);
    put(&shard, dir.path(), "common", 10);
    put(&shard, dir.path(), "shard-only", 30);
    for (include, expected_rows) in [(false, 0), (true, 3)] {
        let view = read_with_main(&config, &main, include, "size").unwrap();
        assert_eq!(view.entry_count, 3);
        assert_eq!(view.entries.len(), expected_rows);
        assert_eq!(view.total_size, 70);
        assert_eq!(view.max_size, 3000);
        assert_eq!(
            view.blob_stats,
            BlobStats {
                total_blobs: 4,
                total_blob_size: 70,
                total_logical_size: 70,
                savings: 0,
            }
        );
        assert_eq!(view.stores.len(), 2);
        assert_eq!(view.stores[1].entries, 2);
        assert_eq!(view.stores[1].bytes, 40);
        assert_eq!(view.stores[1].max_size, 2000);
        assert!(view.stores.iter().all(|s| s.error.is_none()));
    }
    let view = read(&config, true, "size").unwrap();
    assert_eq!(
        view.entries.iter().map(|e| e.size).collect::<Vec<_>>(),
        [30, 20, 10]
    );
    let common_entry = view.entries.iter().find(|e| e.cache_key == common).unwrap();
    assert_eq!(
        common_entry.store_dirs,
        [config.cache_dir.clone(), shard_config.cache_dir.clone()]
    );
    let entry = &view.entries[0];
    assert_eq!(
        entry.meta_path(),
        shard.entry_dir(&entry.cache_key).join("meta.json")
    );
    assert_eq!(
        entry.store_locations(),
        shard_config.cache_dir.display().to_string()
    );
    assert!(entry.meta_path().is_file());
    assert!(!main.entry_dir(&entry.cache_key).exists());
}

#[test]
fn volume_inventory_skips_unavailable_shards_without_creating_them() {
    let (dir, mut config, shard_config) = fixture();
    let main = Store::open(&config).unwrap();
    put(&main, dir.path(), "main", 13);
    // A second mapping to the same directory must not count it twice.
    config.volume_stores.push(config.volume_stores[0].clone());
    config.volume_stores.push(VolumeStore {
        volume: "/other/".into(),
        store: config.cache_dir.clone(),
        max_size: Some(999),
    });
    let view = read(&config, false, "name").unwrap();
    assert_eq!(view.stores.len(), 2);
    assert_eq!(view.total_size, 13);
    assert_eq!(view.entry_count, 1);
    assert_eq!(view.max_size, 1000);
    assert_eq!(view.stores[1].path, shard_config.cache_dir);
    assert_eq!(view.stores[1].max_size, 2000);
    assert!(
        view.stores[1]
            .error
            .as_deref()
            .unwrap()
            .contains("no index")
    );
    assert!(!shard_config.cache_dir.exists());
    assert!(view.entries.is_empty());
    assert_eq!(crate::cli::doctor_shards(&config).len(), 1);
    assert!(!crate::cli::doctor_shards(&config)[0].pass);
    assert!(!shard_config.cache_dir.exists());
}

#[test]
fn volume_inventory_reports_a_failed_shard_and_continues() {
    let (dir, mut config, shard_config) = fixture();
    let main = Store::open(&config).unwrap();
    put(&main, dir.path(), "main", 13);
    std::fs::create_dir_all(&shard_config.cache_dir).unwrap();
    std::fs::write(shard_config.index_db_path(), b"").unwrap();
    // Store::open cannot turn a file into its store directory.
    std::fs::write(shard_config.store_dir(), b"blocked").unwrap();
    let good = dir.path().join("other");
    let mut good_config = shard_config.clone();
    good_config.cache_dir = good.clone();
    put(&Store::open(&good_config).unwrap(), dir.path(), "other", 7);
    config.volume_stores.push(VolumeStore {
        volume: "/other/".into(),
        store: good,
        max_size: Some(3000),
    });
    let view = read(&config, true, "name").unwrap();
    assert_eq!(view.entry_count, 2);
    assert_eq!(view.total_size, 20);
    assert_eq!(view.max_size, 4000);
    assert!(view.stores[1].error.is_some());
    assert_eq!(view.stores[2].bytes, 7);
    assert_eq!(
        crate::cli::doctor_shards(&config)
            .iter()
            .map(|c| c.pass)
            .collect::<Vec<_>>(),
        [false, true]
    );
}

#[test]
fn volume_inventory_single_store_and_sort_orders() {
    let (dir, mut config, _) = fixture();
    config.volume_stores.clear();
    let store = Store::open(&config).unwrap();
    let z = put(&store, dir.path(), "z", 9);
    put(&store, dir.path(), "a", 1);
    store.get(&z).unwrap();
    let db = crate::store::open_index_db(&config.index_db_path()).unwrap();
    db.execute_batch("UPDATE entries SET created_at = CASE crate_name WHEN 'z' THEN '2026-01-01' ELSE '2026-02-01' END").unwrap();
    let view = read(&config, false, "size").unwrap();
    assert_eq!(view.entry_count, 2);
    assert_eq!(view.total_size, 10);
    assert!(view.entries.is_empty());
    let window = crate::since::SinceWindow::DEFAULT;
    let snapshot = crate::cli::snapshot_from_direct_reads(&config, false, "name", window, false);
    let lines = crate::cli::render_stats(&snapshot, &config, window);
    assert_eq!(
        lines
            .iter()
            .filter(|line| line.starts_with("Store:"))
            .count(),
        1
    );
    assert!(
        !lines
            .iter()
            .any(|line| line.starts_with(&format!("Store {}:", config.cache_dir.display())))
    );
    assert_eq!(
        read(&config, true, "name").unwrap().entries[0].crate_name,
        "a"
    );
    assert_eq!(
        read(&config, true, "hits").unwrap().entries[0].crate_name,
        "z"
    );
    let ages = read(&config, true, "age").unwrap().entries;
    assert_eq!(ages[0].crate_name, "z");
    assert!(ages.windows(2).all(|e| e[0].created_at <= e[1].created_at));
    assert_eq!(
        read(&config, true, "size").unwrap().entries[0].crate_name,
        "z"
    );
}

#[test]
fn volume_inventory_combines_hits_dates_and_per_store_dedup_savings() {
    let (dir, config, shard_config) = fixture();
    let main = Store::open(&config).unwrap();
    let shard = Store::open(&shard_config).unwrap();
    let key = put(&main, dir.path(), "common", 10);
    put(&main, dir.path(), "same-bytes", 10);
    put(&shard, dir.path(), "common", 10);
    put(&shard, dir.path(), "same-bytes", 10);
    for (cfg, hits, created, accessed) in [
        (&config, 2, "2026-02-01", "2026-03-01"),
        (&shard_config, 3, "2026-01-01", "2026-04-01"),
    ] {
        let db = crate::store::open_index_db(&cfg.index_db_path()).unwrap();
        db.execute(
            "UPDATE entries SET hit_count=?1, created_at=?2, last_accessed=?3 WHERE cache_key=?4",
            rusqlite::params![hits, created, accessed, key],
        )
        .unwrap();
    }
    let view = read(&config, true, "hits").unwrap();
    assert_eq!(view.blob_stats.savings, 20);
    assert_eq!(view.blob_stats.total_logical_size, 40);
    assert_eq!(view.blob_stats.total_blob_size, 20);
    assert_eq!(view.blob_stats.total_blobs, 2);
    let entry = &view.entries[0];
    assert_eq!(entry.cache_key, key);
    assert_eq!(entry.hit_count, 5);
    assert_eq!(entry.created_at, "2026-01-01");
    assert_eq!(entry.last_accessed, "2026-04-01");
    assert_eq!(
        entry.store_locations(),
        format!(
            "{}, {}",
            config.cache_dir.display(),
            shard_config.cache_dir.display()
        )
    );
}

#[test]
fn volume_inventory_report_does_not_hide_broken_shard_accounting() {
    let (dir, config, shard_config) = fixture();
    let main = Store::open(&config).unwrap();
    put(&main, dir.path(), "main-a", 100);
    put(&main, dir.path(), "main-b", 100);
    let shard = Store::open(&shard_config).unwrap();
    put(&shard, dir.path(), "shard", 10);
    let db = crate::store::open_index_db(&shard_config.index_db_path()).unwrap();
    db.execute("UPDATE blobs SET size = 20", []).unwrap();
    let report =
        crate::report::generate_report(&config, crate::since::SinceWindow::DEFAULT, 10).unwrap();
    assert!(report.storage.blob_bytes < report.storage.logical_bytes);
    assert!(!report.storage.accounting_consistent);
    let checks = crate::cli::doctor_shards(&config);
    assert!(!checks[0].pass);
    assert!(checks[0].detail.contains("accounting needs repair"));
}

#[test]
fn volume_inventory_reports_missing_shards_and_refuses_to_verify_them() {
    let (_dir, config, shard_config) = fixture();
    let report =
        crate::report::generate_report(&config, crate::since::SinceWindow::DEFAULT, 10).unwrap();
    assert!(!report.storage.accounting_consistent);
    assert!(
        report.suggestions.iter().any(|s| s.contains("unavailable")
            && s.contains(&shard_config.cache_dir.display().to_string()))
    );
    assert!(
        crate::cli::verify(&config, false, false)
            .unwrap_err()
            .to_string()
            .contains("cannot verify volume store")
    );
    assert!(!shard_config.cache_dir.exists());
}

#[test]
fn volume_inventory_verify_aggregates_checksums_orphans_and_index_drift() {
    let (dir, config, shard_config) = fixture();
    let shard = Store::open(&shard_config).unwrap();
    let key = put(&shard, dir.path(), "corrupt", 12);
    let meta = shard.stored_meta(&key).unwrap();
    let hash = &meta.files[0].hash;
    let blob = crate::store::blob_path_in_store_dir(&shard_config.store_dir(), hash);
    std::fs::remove_file(&blob).unwrap();
    std::fs::write(&blob, [99; 12]).unwrap();
    let orphan = crate::store::blob_path_in_store_dir(&shard_config.store_dir(), &"0".repeat(64));
    std::fs::create_dir_all(orphan.parent().unwrap()).unwrap();
    std::fs::write(orphan, b"orphan").unwrap();
    let db = crate::store::open_index_db(&shard_config.index_db_path()).unwrap();
    db.execute("UPDATE blobs SET refcount = 77", []).unwrap();
    let indexed = crate::cli::verify(&config, false, false).unwrap();
    assert_eq!(indexed.index_drift, 1);
    assert_eq!(indexed.valid_entries, 1);
    let result = crate::cli::verify(&config, true, false).unwrap();
    assert_eq!(result.total_entries, 1);
    assert_eq!(result.corrupted_entries, 1);
    assert_eq!(result.checksum_failures, 1);
    assert_eq!(result.orphaned_blobs, 1);
    // Corrupt entries cannot serve as the source of truth for index comparison.
    assert_eq!(result.index_drift, 0);
}

#[test]
fn volume_inventory_reaches_direct_stats_daemon_stats_and_reports() {
    let (dir, config, shard_config) = fixture();
    let main = Store::open(&config).unwrap();
    let shard = Store::open(&shard_config).unwrap();
    put(&main, dir.path(), "common", 10);
    put(&shard, dir.path(), "common", 10);
    put(&shard, dir.path(), "shard-only", 30);
    let window = crate::since::SinceWindow::DEFAULT;
    let snapshot = crate::cli::snapshot_from_direct_reads(&config, true, "size", window, false);
    assert_eq!(snapshot.entry_count, 2);
    assert_eq!(snapshot.total_size, 50);
    assert_eq!(snapshot.max_size, 3000);
    assert_eq!(snapshot.entries.len(), 2);
    let lines = crate::cli::render_stats(&snapshot, &config, window);
    assert!(
        lines
            .iter()
            .any(|line| line.contains(&shard_config.cache_dir.display().to_string()))
    );
    let daemon = crate::daemon::Daemon::new(config.clone());
    let req: crate::daemon::StatsRequest = serde_json::from_value(serde_json::json!({
        "include_entries": true, "sort_by": "size", "event_hours": null
    }))
    .unwrap();
    let stats = daemon.handle_stats(&req).stats.unwrap();
    assert_eq!(stats.total_size, 50);
    assert_eq!(stats.entry_count, 2);
    assert_eq!(stats.max_size, 3000);
    assert_eq!(stats.stores, snapshot.stores);
    assert_eq!(stats.entries.unwrap(), snapshot.entries);
    assert_eq!(stats.blob_stats, snapshot.blob_stats);
    let report = crate::report::generate_report(&config, window, 10).unwrap();
    assert_eq!(report.storage.logical_bytes, 50);
    assert_eq!(report.storage.blob_bytes, 50);
    assert_eq!(report.storage.store_entries, 2);
    assert_eq!(report.storage.stores, snapshot.stores);
    assert!(report.storage.accounting_consistent);
    let checks = crate::cli::doctor_shards(&config);
    assert!(checks[0].pass);
    assert!(checks[0].detail.contains("2 entries"));
}

#[test]
fn volume_inventory_verify_finds_corruption_in_a_shard() {
    let (dir, config, shard_config) = fixture();
    let main = Store::open(&config).unwrap();
    let shard = Store::open(&shard_config).unwrap();
    put(&main, dir.path(), "healthy", 17);
    let key = put(&shard, dir.path(), "corrupt", 31);
    let meta = shard.stored_meta(&key).unwrap();
    let hash = &meta.files[0].hash;
    std::fs::remove_file(crate::store::blob_path_in_store_dir(
        &shard_config.store_dir(),
        hash,
    ))
    .unwrap();
    let result = crate::cli::verify(&config, false, false).unwrap();
    assert_eq!(result.total_entries, 2);
    assert_eq!(result.valid_entries, 1);
    assert_eq!(result.corrupted_entries, 1);
    assert_eq!(result.missing_blobs, 1);
    assert_eq!(result.unresolved_integrity_findings(), 1);
    let repaired = crate::cli::verify(&config, false, true).unwrap();
    assert_eq!(repaired.corrupted_removed, 1);
    assert_eq!(repaired.unresolved_integrity_findings(), 0);
    assert_eq!(main.entry_count().unwrap(), 1);
    assert_eq!(shard.entry_count().unwrap(), 0);
}

#[test]
#[cfg(unix)]
fn volume_inventory_deduplicates_symlinked_stores() {
    let (dir, mut config, shard_config) = fixture();
    Store::open(&config).unwrap();
    let shard = Store::open(&shard_config).unwrap();
    put(&shard, dir.path(), "shard", 5);
    let alias = dir.path().join("alias");
    std::os::unix::fs::symlink(&shard_config.cache_dir, &alias).unwrap();
    config.volume_stores.push(VolumeStore {
        volume: "/alias/".into(),
        store: alias,
        max_size: Some(2000),
    });
    let view = read(&config, true, "name").unwrap();
    assert_eq!(view.total_size, 5);
    assert_eq!(view.stores.len(), 2);
    assert_eq!(view.entries[0].store_dirs, [shard_config.cache_dir]);
}
