use super::*;

#[test]
fn compiler_selection_is_exact_and_keeps_unknown_and_shared_outputs() {
    let temp = tempfile::tempdir().unwrap();
    let config = test_config(temp.path());
    let store = Store::open(&config).unwrap();
    let source = temp.path().join("shared.rlib");
    fs::write(&source, b"shared compiler output").unwrap();
    for (n, version) in [
        (1, Some("1.99.0-nightly (aaa 2026-09-01)")),
        (2, Some("1.99.0-nightly (bbb 2026-09-02)")),
        (3, Some("1.9.0")),
        (4, None),
    ] {
        store
            .put_rustc_with_compile_time(
                &format!("{n:064x}"),
                "probe",
                &["rlib".into()],
                &[],
                "",
                "",
                &[(source.clone(), "libprobe.rlib".into())],
                "",
                "",
                10,
                version,
            )
            .unwrap();
    }
    let preview = store.preview_rustc_version("1.99.0-nightly").unwrap();
    assert_eq!(preview.entries, 2);
    assert_eq!(preview.bytes, 44);
    assert_eq!(preview.versions.len(), 2);
    assert_eq!(preview.unknown_entries, 1);
    assert_eq!(store.preview_rustc_version("1.9.0").unwrap().entries, 1);
    assert_eq!(
        store
            .preview_rustc_version("1.99.0-nightly (aaa 2026-09-01)")
            .unwrap()
            .entries,
        1
    );
    assert_eq!(store.preview_rustc_version("1.99").unwrap().entries, 0);
    assert_eq!(
        store.entry_count().unwrap(),
        4,
        "preview never removes entries"
    );
    let recent = store.evict_rustc_version("1.9.0").unwrap();
    assert_eq!(recent.entries_evicted, 0);
    assert_eq!(
        recent.entries_pinned, 1,
        "an active restore keeps its entry"
    );
    store
        .db
        .execute(
            "UPDATE entries SET last_accessed = '2000-01-01 00:00:00'",
            [],
        )
        .unwrap();
    let stats = store
        .evict_rustc_version("1.99.0-nightly (aaa 2026-09-01)")
        .unwrap();
    assert_eq!(stats.entries_evicted, 1);
    assert_eq!(store.entry_count().unwrap(), 3);
    assert!(store.get(&format!("{:064x}", 2)).unwrap().is_some());
    assert!(store.get(&format!("{:064x}", 4)).unwrap().is_some());
    assert_eq!(
        store.physical_size().unwrap(),
        22,
        "other versions retain their shared blob"
    );
}

#[test]
fn compiler_identity_migrates_without_discarding_legacy_entries() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("index.db");
    let db = open_index_db(&path).unwrap();
    db.execute_batch(
        "DROP INDEX idx_entries_rustc; ALTER TABLE entries DROP COLUMN rustc_version;
        INSERT INTO entries(cache_key, crate_name, size, committed) VALUES('legacy', 'kept', 20, 1);
        PRAGMA user_version = 12;",
    )
    .unwrap();
    drop(db);
    let db = open_index_db(&path).unwrap();
    assert_eq!(
        db.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        13
    );
    assert_eq!(
        db.query_row(
            "SELECT COUNT(*) FROM entries WHERE rustc_version IS NULL AND cache_key = 'legacy'",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
}

#[test]
fn imported_compiler_identity_remains_selectable() {
    let temp = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(temp.path())).unwrap();
    let key = "a".repeat(64);
    let dir = store.entry_dir(&key);
    std::fs::create_dir_all(&dir).unwrap();
    let bytes = b"imported Rust library";
    std::fs::write(dir.join("libprobe.rlib"), bytes).unwrap();
    let meta = EntryMeta {
        cache_key: key.clone(),
        key_schema: kache_format::CACHE_KEY_VERSION,
        rustc_version: Some("1.99.1 (abc 2026-10-13)".into()),
        crate_name: "probe".into(),
        crate_types: vec!["rlib".into()],
        files: vec![CachedFile {
            name: "libprobe.rlib".into(),
            size: bytes.len() as u64,
            hash: blake3::hash(bytes).to_hex().to_string(),
            executable: false,
        }],
        stdout: String::new(),
        stderr: String::new(),
        features: vec![],
        target: String::new(),
        profile: String::new(),
        compile_time_ms: 1,
        emit_kinds: vec!["link".into()],
    };
    std::fs::write(dir.join("meta.json"), serde_json::to_vec(&meta).unwrap()).unwrap();
    store.import_downloaded_entry(&key).unwrap();
    assert_eq!(store.preview_rustc_version("1.99.1").unwrap().entries, 1);
    assert_eq!(store.preview_rustc_version("1.98.0").unwrap().entries, 0);
    assert_eq!(
        store.stored_meta(&key).unwrap().rustc_version,
        meta.rustc_version
    );
    assert_eq!(
        store.evict_rustc_version("1.99.1").unwrap().entries_evicted,
        0,
        "recent imports stay protected"
    );
}
