#[test]
fn lock_polls_back_off_from_a_millisecond_to_the_interval() {
    let naps: Vec<u64> = (0..10)
        .map(|attempt| lock_poll_interval(attempt).as_millis() as u64)
        .collect();
    assert_eq!(naps, vec![1, 2, 4, 8, 16, 32, 64, 100, 100, 100]);
}

/// Opening an index runs its DDL once: the second open finds the schema
/// generation current and skips every statement (each would otherwise
/// take the write lock), while an index from before the stamp still
/// migrates.
#[test]
fn index_ddl_runs_once_per_schema_generation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index.db");
    let db = open_index_db(&path).unwrap();
    let generation: i64 = db
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(generation, INDEX_SCHEMA_GENERATION);
    // A pre-stamp index (generation 0) migrates and gets stamped.
    db.pragma_update(None, "user_version", 0_i64).unwrap();
    db.execute_batch("DROP TABLE target_roots").unwrap();
    drop(db);
    let db = open_index_db(&path).unwrap();
    let generation: i64 = db
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(generation, INDEX_SCHEMA_GENERATION);
    let tables: i64 = db
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'target_roots'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    // A generation-1 index (0.23) lacks the durability column and the two
    // C memo tables; the generation bump is what makes it migrate.
    db.pragma_update(None, "user_version", 1_i64).unwrap();
    db.execute_batch(
        "ALTER TABLE entries DROP COLUMN durable;
             DROP TABLE cc_mapped_hashes;
             DROP TABLE cc_asm_scans;",
    )
    .unwrap();
    drop(db);
    let db = open_index_db(&path).unwrap();
    let durable_column: i64 = db
        .query_row(
            "SELECT count(*) FROM pragma_table_info('entries') WHERE name = 'durable'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(durable_column, 1, "generation 1 gains the durable column");
    let memo_tables: i64 = db
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name IN ('cc_mapped_hashes', 'cc_asm_scans')",
                [],
                |row| row.get(0),
            )
            .unwrap();
    assert_eq!(memo_tables, 2, "generation 1 gains the C memo tables");
    assert_eq!(
        tables, 1,
        "the dropped table was recreated by the migration"
    );
}

/// An index stamped at generation 3 predates the crate-name index, and
/// the stamp alone must not keep it from gaining one.
#[test]
fn index_from_generation_three_gains_the_crate_name_index() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index.db");
    let db = open_index_db(&path).unwrap();
    db.execute_batch("DROP INDEX idx_entries_crate_name;")
        .unwrap();
    db.pragma_update(None, "user_version", 3_i64).unwrap();
    drop(db);

    let db = open_index_db(&path).unwrap();
    let indexes: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'index' AND name = 'idx_entries_crate_name'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(indexes, 1);
    let generation: i64 = db
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(generation, INDEX_SCHEMA_GENERATION);
}

/// An index stamped at generation 4 has no unit column; the next open
/// adds it, its index, and lets the wrapper record units from then on.
#[test]
fn index_from_generation_four_gains_the_unit_column() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index.db");
    let db = open_index_db(&path).unwrap();
    db.execute_batch(
        "DROP INDEX idx_entries_crate_unit;
             ALTER TABLE entries DROP COLUMN unit_id;",
    )
    .unwrap();
    db.pragma_update(None, "user_version", 4_i64).unwrap();
    drop(db);

    let db = open_index_db(&path).unwrap();
    let indexes: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'index' AND name = 'idx_entries_crate_unit'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(indexes, 1);
    db.execute(
        "INSERT INTO entries (cache_key, crate_name, unit_id) VALUES ('k', 'c', 'u')",
        [],
    )
    .unwrap();
    let generation: i64 = db
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(generation, INDEX_SCHEMA_GENERATION);
}

/// A put never learns its unit; the wrapper records it afterwards, and
/// an empty unit leaves the row alone.
#[test]
fn record_entry_unit_updates_only_that_entry() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    store
        .db
        .execute_batch("INSERT INTO entries (cache_key, crate_name) VALUES ('a', 'x'), ('b', 'x');")
        .unwrap();
    store.record_entry_unit("a", "unit-a").unwrap();
    store.record_entry_unit("b", "").unwrap();
    let units: Vec<(String, String)> = store
        .db
        .prepare("SELECT cache_key, unit_id FROM entries ORDER BY cache_key")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        units,
        vec![
            ("a".to_string(), "unit-a".to_string()),
            ("b".to_string(), String::new())
        ]
    );
    assert!(
        store
            .file_hash_cache()
            .has_entry_for_unit("x", "unit-a")
            .unwrap()
    );
    assert!(
        store
            .file_hash_cache()
            .has_entry_for_unit("x", "unit-z")
            .unwrap(),
        "row b has no unit"
    );
}

/// An index stamped before the env-use memo was versioned still carries
/// the boolean table, whose rows the fixed scanner must never reuse.
#[test]
fn index_from_generation_one_moves_to_the_versioned_env_use_memo() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index.db");
    let db = open_index_db(&path).unwrap();
    db.execute_batch(
        "DROP TABLE source_env_dep_uses;
             CREATE TABLE source_env_runtime_uses (
                content_hash    TEXT NOT NULL,
                env_var         TEXT NOT NULL,
                has_runtime_use INTEGER NOT NULL,
                updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (content_hash, env_var)
             );",
    )
    .unwrap();
    db.pragma_update(None, "user_version", 1_i64).unwrap();
    drop(db);

    let db = open_index_db(&path).unwrap();
    let tables: Vec<String> = db
        .prepare(
            "SELECT name FROM sqlite_master WHERE type = 'table'
                 AND name IN ('source_env_runtime_uses', 'source_env_dep_uses')",
        )
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(tables, vec!["source_env_dep_uses".to_string()]);
}
type Store = ArtifactStore<TestPolicy>;
struct TestPolicy;
impl ArtifactPolicy for TestPolicy {
    fn allow_hardlink(name: &str) -> bool {
        matches!(
            std::path::Path::new(name)
                .extension()
                .and_then(|ext| ext.to_str()),
            Some("rlib" | "rmeta" | "o" | "obj" | "a" | "lib" | "pdb" | "dwo" | "tar" | "unknown")
        )
    }
    fn allow_empty(name: &str, kinds: &[String]) -> bool {
        name.ends_with(".rmeta")
            && kinds
                .iter()
                .all(|kind| matches!(kind.as_str(), "bin" | "cdylib" | "staticlib"))
    }
    fn emit_kind(name: &str) -> Option<&'static str> {
        match std::path::Path::new(name)
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("")
        {
            "rlib" | "so" | "dylib" | "dll" | "exe" | "a" | "lib" | "wasm" | "" => Some("link"),
            "rmeta" => Some("metadata"),
            "o" | "obj" => Some("obj"),
            "d" | "pp" => Some("dep-info"),
            "s" | "asm" => Some("asm"),
            "ll" => Some("llvm-ir"),
            "bc" => Some("llvm-bc"),
            "mir" => Some("mir"),
            _ => None,
        }
    }
    fn stable_after_store(name: &str) -> bool {
        !name.ends_with(".d") && !name.ends_with(".pp")
    }
}

/// `0` and negatives mean "not recorded", not a measured zero
/// (kunobi-ninja/kache#617). Load-bearing: the `size` and
/// `compile_time_ms` columns default to 0 for rows written before their
/// migrations, and a 0 read as a measurement would rank an un-backfilled
/// entry as free to fetch and worthless to have.
#[test]
fn test_positive_or_none_treats_non_positive_as_unknown() {
    assert_eq!(positive_or_none(0), None, "0 is unknown, not Some(0)");
    assert_eq!(positive_or_none(-1), None, "a negative is unknown");
    assert_eq!(
        positive_or_none(1),
        Some(1),
        "the smallest real value survives"
    );
    assert_eq!(positive_or_none(4200), Some(4200));
    assert_eq!(positive_or_none(i64::MAX), Some(i64::MAX as u64));
}

use super::*;
use crate::eviction::EvictionPolicy as _;

#[test]
fn readonly_regular_metadata_requires_both_properties() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("artifact");
    fs::write(&file, b"artifact").unwrap();

    assert!(!metadata_is_readonly_regular(&fs::metadata(&file).unwrap()));

    let mut permissions = fs::metadata(&file).unwrap().permissions();
    permissions.set_readonly(true);
    fs::set_permissions(&file, permissions).unwrap();
    assert!(metadata_is_readonly_regular(&fs::metadata(&file).unwrap()));

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let readonly_dir = dir.path().join("readonly-dir");
        fs::create_dir(&readonly_dir).unwrap();
        fs::set_permissions(&readonly_dir, fs::Permissions::from_mode(0o555)).unwrap();
        assert!(!metadata_is_readonly_regular(
            &fs::metadata(&readonly_dir).unwrap()
        ));
    }
}

// Regression guard for #324: the local content-dedup hash must distinguish
// entries that the old (bare-hash, 16-hex-truncated) fold could collide —
// otherwise `evict_duplicate_entries` keeps the wrong survivor.
#[test]
fn content_hash_distinguishes_transposition_exec_bit_and_is_order_independent() {
    let cf = |name: &str, hash: &str, executable: bool| CachedFile {
        name: name.to_string(),
        size: 10,
        hash: hash.to_string(),
        executable,
    };

    // Same multiset of blob hashes, but the (name -> hash) mapping is swapped:
    // the old hash-only fold collided these; the new fold must not.
    let a = vec![cf("a.rlib", "H1", false), cf("b.rlib", "H2", false)];
    let swapped = vec![cf("a.rlib", "H2", false), cf("b.rlib", "H1", false)];
    assert_ne!(
        compute_content_hash(&a),
        compute_content_hash(&swapped),
        "a name<->hash transposition must change the content hash"
    );

    // Identical names/hashes/sizes; only which file is executable differs.
    let exec_a = vec![cf("a.rlib", "H1", true), cf("b.rlib", "H2", false)];
    let exec_b = vec![cf("a.rlib", "H1", false), cf("b.rlib", "H2", true)];
    assert_ne!(
        compute_content_hash(&exec_a),
        compute_content_hash(&exec_b),
        "moving the exec-bit to a different file must change the content hash"
    );

    // Deterministic and independent of input order.
    let reordered = vec![cf("b.rlib", "H2", false), cf("a.rlib", "H1", false)];
    assert_eq!(
        compute_content_hash(&a),
        compute_content_hash(&reordered),
        "content hash must not depend on file order"
    );
}

/// Which stored filenames may share an inode with the store blob on
/// insert. Mirrors the restore-side `link_strategy` split, minus the
/// insert-only exclusions documented on `hardlink_eligible`.
#[test]
fn hardlink_eligibility_mirrors_restore_strategy_with_insert_exclusions() {
    // On Windows the gate additionally requires the `windows_hardlink`
    // opt-in, which is off in tests — eligibility is all-false there.
    let gate_open = !cfg!(windows);

    // Immutable kinds the restore side hardlinks: eligible.
    for name in [
        "libserde-abc123.rlib",
        "libserde-abc123.rmeta",
        "foo.rcgu.o",
        "foo.obj",
        "foo.dwo",
    ] {
        assert_eq!(
            hardlink_eligible::<TestPolicy>(name, false),
            gate_open,
            "{name} should be hardlink-eligible on insert (behind the Windows gate)"
        );
    }

    // Mutable kinds (Copy strategy on restore): never eligible.
    assert!(!hardlink_eligible::<TestPolicy>("libfoo.dylib", false));
    assert!(!hardlink_eligible::<TestPolicy>("libfoo.so", false));
    assert!(!hardlink_eligible::<TestPolicy>("foo.exe", false));

    // Insert-only exclusions: `.d` is rewritten in place after `put`
    // (Expand), extensionless names are bin executables by rustc's Unix
    // convention, and an executable mode bit wins over the filename.
    assert!(!hardlink_eligible::<TestPolicy>("serde-abc123.d", false));
    assert!(!hardlink_eligible::<TestPolicy>("my-binary", false));
    assert!(!hardlink_eligible::<TestPolicy>(
        "libserde-abc123.rlib",
        true
    ));
}

#[test]
fn source_hardlink_policy_honors_independent_storage() {
    assert!(!source_hardlink_allowed::<TestPolicy>(
        false, "foo.o", false
    ));
    assert_eq!(
        source_hardlink_allowed::<TestPolicy>(true, "foo.o", false),
        hardlink_eligible::<TestPolicy>("foo.o", false)
    );
}

#[cfg(unix)]
#[test]
fn independent_put_never_hardlinks_or_marks_source_readonly() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let output = dir.path().join("compiler-output.o");
    fs::write(&output, b"independent cc artifact").unwrap();
    fs::set_permissions(&output, fs::Permissions::from_mode(0o660)).unwrap();

    store
        .put_with_compile_time_independent(
            "cc-independent",
            "foo.c",
            &[],
            &[],
            "x86_64-unknown-linux-gnu",
            "",
            &[(output.clone(), "foo.o".to_string())],
            "",
            "",
            1,
        )
        .unwrap();

    let meta = store.get("cc-independent").unwrap().unwrap();
    let blob = store.blob_path(&meta.files[0].hash);
    let output_meta = fs::metadata(&output).unwrap();
    let blob_meta = fs::metadata(&blob).unwrap();
    assert_eq!(output_meta.permissions().mode() & 0o777, 0o660);
    assert!(!output_meta.permissions().readonly());
    assert_ne!(
        (output_meta.dev(), output_meta.ino()),
        (blob_meta.dev(), blob_meta.ino()),
        "independent ingest must never share the compiler output inode"
    );
    assert!(blob_meta.permissions().readonly());
}

/// #648 made restore honour the mode bit recorded at insert time, because a
/// `[[test]] harness = false` target supplies its own `main` and is compiled
/// with neither `--test` nor `--crate-type` — nothing in the rustc argv says
/// "executable", so the recorded bit is the only signal. #822 then began
/// reading that bit off the *staging snapshot* rather than the compiler's
/// output, and a reflinked snapshot is created at the umask: on every CoW
/// filesystem the entry recorded `executable: false`, restore fell back to
/// `Hardlink`, and cargo failed the run with "Permission denied (os error
/// 13)".
///
/// The emulation is what makes this reachable on ext4. Without it the copy
/// fallback preserves the mode, the assertion holds for free, and the test
/// would only fail on the filesystems CI never runs on.
#[cfg(unix)]
#[test]
fn put_records_the_executable_bit_from_the_compiler_output_not_the_staging_snapshot() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let output = dir.path().join("harness-a1b2c3d4e5f60718");
    fs::write(&output, b"\x7fELF harness=false test binary").unwrap();
    fs::set_permissions(&output, fs::Permissions::from_mode(0o755)).unwrap();

    {
        let _forced = ModeDroppingIngest::enable();
        store
            .put_with_compile_time_independent(
                "harness-entry",
                "harness",
                &[],
                &[],
                "x86_64-unknown-linux-gnu",
                "",
                &[(output.clone(), "harness-a1b2c3d4e5f60718".to_string())],
                "",
                "",
                1,
            )
            .unwrap();
    }

    let meta = store.get("harness-entry").unwrap().unwrap();
    // Proves the emulation actually dropped the bit, so the assertion below
    // cannot pass because the snapshot happened to keep it.
    let blob_mode = fs::metadata(store.blob_path(&meta.files[0].hash))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(
        blob_mode & 0o111,
        0,
        "emulated reflink ingest should have staged without the bit, got {blob_mode:o}"
    );
    assert!(
        meta.files[0].executable,
        "the recorded mode must come from the compiler's 0o755 output, \
             not from the staging snapshot"
    );
}

/// The converse, under the same emulation: reading the mode from the source
/// must not degenerate into recording every artifact executable.
#[cfg(unix)]
#[test]
fn put_records_no_executable_bit_for_a_non_executable_output() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let output = dir.path().join("libfoo.rlib");
    fs::write(&output, b"rlib bytes").unwrap();
    fs::set_permissions(&output, fs::Permissions::from_mode(0o644)).unwrap();

    {
        let _forced = ModeDroppingIngest::enable();
        store
            .put_with_compile_time_independent(
                "rlib-entry",
                "foo",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "",
                &[(output.clone(), "libfoo.rlib".to_string())],
                "",
                "",
                1,
            )
            .unwrap();
    }

    let meta = store.get("rlib-entry").unwrap().unwrap();
    assert!(
        !meta.files[0].executable,
        "a 0o644 compiler output must not be recorded executable"
    );
}

/// cargo-mutants replacing `Drop` with `()` left the thread-local on.
/// The put tests never insert again after the guard, so they could not
/// see the leak.
#[cfg(unix)]
#[test]
fn mode_dropping_ingest_guard_clears_on_drop() {
    assert!(
        !FORCE_MODE_DROPPING_INGEST.with(std::cell::Cell::get),
        "ingest emulation must start off"
    );
    {
        let _forced = ModeDroppingIngest::enable();
        assert!(
            FORCE_MODE_DROPPING_INGEST.with(std::cell::Cell::get),
            "enable must turn ingest emulation on"
        );
    }
    assert!(
        !FORCE_MODE_DROPPING_INGEST.with(std::cell::Cell::get),
        "Drop must turn ingest emulation off so a later put on this thread is not forced"
    );
}

/// A mutable-kind blob must never share an inode with the build's output:
/// mutating the output post-put (codesigning, stripping) must not be able
/// to reach the content-addressed blob. Deterministic on every
/// filesystem — reflink yields an independent inode, and the copy
/// fallback trivially does; only a hardlink would fail this.
#[cfg(unix)]
#[test]
fn put_keeps_mutable_kind_blobs_inode_independent_from_the_source() {
    use std::os::unix::fs::MetadataExt;

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let source = dir.path().join("libfoo.dylib");
    fs::write(&source, b"dylib bytes").unwrap();

    store
        .put(
            "key-dylib",
            "foo",
            &["dylib".to_string()],
            &[],
            "host",
            "dev",
            &[(source.clone(), "libfoo.dylib".to_string())],
            "",
            "",
        )
        .unwrap();

    let hash = crate::file_hash::hash_file(&source).unwrap();
    let blob = store.blob_path(&hash);
    assert_ne!(
        fs::metadata(&blob).unwrap().ino(),
        fs::metadata(&source).unwrap().ino(),
        "a mutable-kind blob must not share an inode with the build output"
    );
}

/// Contract for immutable-kind ingest: the blob's content matches, the
/// blob is read-only, and IF the filesystem fell back to a hardlink
/// (no CoW — e.g. ext4 in CI) the build's own output is now the same
/// read-only inode, exactly the state a warm restore leaves behind.
/// Which zero-copy mechanism ran is filesystem-dependent, so the test
/// asserts the contract, not the mechanism.
#[cfg(unix)]
#[test]
fn put_ingests_immutable_kinds_zero_copy_where_the_filesystem_allows() {
    use std::os::unix::fs::MetadataExt;

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let source = dir.path().join("libfoo-abc.rlib");
    fs::write(&source, b"rlib bytes").unwrap();

    store
        .put(
            "key-rlib",
            "foo",
            &["rlib".to_string()],
            &[],
            "host",
            "dev",
            &[(source.clone(), "libfoo-abc.rlib".to_string())],
            "",
            "",
        )
        .unwrap();

    let hash = crate::file_hash::hash_file(&source).unwrap();
    let blob = store.blob_path(&hash);
    assert_eq!(fs::read(&blob).unwrap(), b"rlib bytes");
    assert!(
        fs::metadata(&blob).unwrap().permissions().readonly(),
        "store blob must be read-only"
    );
    if fs::metadata(&blob).unwrap().ino() == fs::metadata(&source).unwrap().ino() {
        // Hardlink fallback ran: the source shares the blob's inode and
        // therefore its read-only mode — the same state a warm restore
        // produces, handled by the pre-compile read-only clean.
        assert!(
            fs::metadata(&source).unwrap().permissions().readonly(),
            "a hardlinked source must carry the blob's read-only mode"
        );
    }
}

/// A symlinked source must never produce a symlink "blob": hashing
/// follows the link, so the blob must hold the target's bytes as a
/// regular file. Reflink and copy both follow the link; only the
/// hardlink fallback could capture the symlink itself, and the
/// eligibility guard refuses it (`symlink_metadata` check).
#[cfg(unix)]
#[test]
fn put_never_stores_a_symlink_as_a_blob() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let target = dir.path().join("real-artifact.rlib");
    fs::write(&target, b"real artifact bytes").unwrap();
    let symlink = dir.path().join("linked.rlib");
    std::os::unix::fs::symlink(&target, &symlink).unwrap();

    store
        .put(
            "key-symlink",
            "foo",
            &["rlib".to_string()],
            &[],
            "host",
            "dev",
            &[(symlink.clone(), "linked.rlib".to_string())],
            "",
            "",
        )
        .unwrap();

    let hash = crate::file_hash::hash_file(&symlink).unwrap();
    let blob = store.blob_path(&hash);
    let meta = fs::symlink_metadata(&blob).unwrap();
    assert!(
        meta.file_type().is_file(),
        "blob must be a regular file, not a symlink"
    );
    assert_eq!(fs::read(&blob).unwrap(), b"real artifact bytes");
}

#[test]
fn should_try_store_reflink_is_the_inverse_of_the_force_flag() {
    assert!(should_try_store_reflink(false));
    assert!(!should_try_store_reflink(true));
}

#[test]
fn allow_store_hardlink_requires_permission_and_a_regular_file() {
    assert!(allow_store_hardlink(true, true));
    assert!(!allow_store_hardlink(false, true));
    assert!(!allow_store_hardlink(true, false));
    assert!(!allow_store_hardlink(false, false));
}

#[test]
fn force_store_hardlink_defaults_off_and_follows_the_guard() {
    assert!(!force_store_hardlink());
    {
        let _guard = ForceStoreHardlink::enable();
        assert!(force_store_hardlink());
    }
    assert!(!force_store_hardlink());
}

#[cfg(unix)]
#[test]
fn materialize_blob_without_hardlink_permission_does_not_share_inode() {
    use std::os::unix::fs::MetadataExt;
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("libx.rlib");
    fs::write(&source, b"ineligible-materialize").unwrap();
    let blob = dir.path().join("blobs").join("aa").join("a".repeat(64));
    let _force = ForceStoreHardlink::enable();
    assert!(materialize_blob(&source, &blob, false).unwrap());
    assert_eq!(
        fs::metadata(&blob).unwrap().nlink(),
        1,
        "allow_hardlink=false must copy, not hardlink"
    );
    assert_eq!(fs::metadata(&source).unwrap().nlink(), 1);
}

#[test]
fn materialize_blob_errors_when_source_cannot_be_copied() {
    // Covers materialize_blob copy-fallback error branch.
    let dir = tempfile::tempdir().unwrap();
    let hash = "a".repeat(64);
    let source = dir.path().join("missing.rlib");
    let blob = dir.path().join("blobs").join("aa").join(&hash);

    let err = materialize_blob(&source, &blob, false).unwrap_err();

    assert!(
        err.to_string().contains("copying"),
        "expected copy context, got: {err:#}"
    );
    assert!(!blob.exists());
}

#[test]
fn materialize_blob_removes_tmp_when_atomic_rename_fails() {
    // Covers materialize_blob atomic-rename failure cleanup branch.
    let dir = tempfile::tempdir().unwrap();
    let hash = "b".repeat(64);
    let source = dir.path().join("source.rlib");
    fs::write(&source, b"blob bytes").unwrap();
    let blob = dir.path().join("blobs").join("bb").join(&hash);
    fs::create_dir_all(&blob).unwrap();

    let err = materialize_blob(&source, &blob, false).unwrap_err();

    assert!(
        err.to_string().contains("atomic rename"),
        "expected rename context, got: {err:#}"
    );
    let tmp_left = fs::read_dir(blob.parent().unwrap())
        .unwrap()
        .flatten()
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"))
        .count();
    assert_eq!(tmp_left, 0, "failed rename must remove its temp file");
    assert!(blob.is_dir(), "the conflicting destination dir remains");
}

/// A failed publish after a provisional hardlink must not leave the
/// build's output read-only: the RO chmod was applied on the shared temp
/// inode before rename, and the temp is discarded on failure. (On CoW
/// filesystems the hardlink path is never taken — source stays writable
/// either way.)
#[test]
fn materialize_blob_failure_does_not_leave_source_readonly() {
    let dir = tempfile::tempdir().unwrap();
    let hash = "c".repeat(64);
    let source = dir.path().join("source.rlib");
    fs::write(&source, b"blob bytes").unwrap();
    // Destination is a directory so rename fails after staging.
    let blob = dir.path().join("blobs").join("cc").join(&hash);
    fs::create_dir_all(&blob).unwrap();

    let err = materialize_blob(&source, &blob, true).unwrap_err();
    assert!(
        err.to_string().contains("atomic rename"),
        "expected rename context, got: {err:#}"
    );
    assert!(
        !fs::metadata(&source).unwrap().permissions().readonly(),
        "failed hardlink ingest must restore a writable build output"
    );
}

// ── stage → hash → publish (review finding #3) ──────────────────────

/// The put path must hash the STAGED snapshot, not the live build
/// output: the bytes published under a digest must be exactly the bytes
/// that were hashed, so a post-build mutator changing the file after the
/// snapshot can never store content X under address H(Y).
///
/// Uses independent (never-hardlink) storage deliberately: on a
/// non-CoW filesystem the hardlink ingest shares the output's inode
/// with the blob, so mutating the output afterwards would both hit the
/// read-only guard and legitimately move the shared blob. Independent
/// storage (reflink/copy) gives the snapshot byte-isolation on every
/// filesystem, which is the property under test.
#[test]
fn put_stores_snapshot_bytes_matching_recorded_digest() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output_file = dir.path().join("out.rlib");
    let original = b"artifact-bytes-v1";
    fs::write(&output_file, original).unwrap();

    store
        .put_with_compile_time_independent(
            "snapshot_key",
            "snapshot_crate",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(output_file.clone(), "libout.rlib".to_string())],
            "",
            "",
            0,
        )
        .unwrap();

    // Simulate a post-put mutator (strip / codesign / wasm tooling):
    // rewrite the build output in place with different content.
    fs::write(
        &output_file,
        b"mutated-after-put-with-a-much-longer-payload",
    )
    .unwrap();

    let meta = store.get("snapshot_key").unwrap().unwrap();
    assert_eq!(meta.files.len(), 1);
    let blob = store.blob_path(&meta.files[0].hash);
    let stored = fs::read(&blob).unwrap();
    assert_eq!(
        stored, original,
        "stored blob must be byte-identical to what was hashed at put time"
    );
    assert_eq!(meta.files[0].size, original.len() as u64);
}

/// A completed put must leave nothing behind in the staging area.
#[test]
fn successful_put_leaves_staging_dir_empty() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output_file = dir.path().join("out.rlib");
    fs::write(&output_file, b"artifact").unwrap();
    store
        .put(
            "staging_clean_key",
            "crate",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(output_file, "libout.rlib".to_string())],
            "",
            "",
        )
        .unwrap();

    let staging = dir.path().join("staging");
    if staging.exists() {
        let leftovers: Vec<_> = fs::read_dir(&staging).unwrap().flatten().collect();
        assert!(leftovers.is_empty(), "staging litter: {leftovers:?}");
    }
}

/// `stored_meta` reads back what a put wrote, and nothing for a key that
/// was never stored.
#[test]
fn stored_meta_reads_the_entry_a_put_wrote() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let output_file = dir.path().join("out.rlib");
    fs::write(&output_file, b"rlib bytes").unwrap();
    store
        .put(
            "stored_key",
            "crate",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(output_file, "libout.rlib".to_string())],
            "",
            "",
        )
        .unwrap();
    let meta = store.stored_meta("stored_key").unwrap();
    assert_eq!(meta.files.len(), 1);
    assert_eq!(meta.files[0].name, "libout.rlib");
    assert!(store.stored_meta("never_stored").is_none());
}

/// A refused zero-byte artifact must clean up its staged snapshot; a
/// crash-refusal that leaked it would otherwise sit until GC.
#[test]
fn zero_byte_refusal_cleans_up_staged_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    // A zero-byte `.rlib` is never valid output for a lib crate.
    let output_file = dir.path().join("empty.rlib");
    fs::write(&output_file, b"").unwrap();

    let result = store.put(
        "zero_key",
        "crate",
        &["lib".to_string()],
        &[],
        "x86_64-unknown-linux-gnu",
        "dev",
        &[(output_file, "libout.rlib".to_string())],
        "",
        "",
    );
    assert!(result.is_err(), "zero-byte rlib must be refused");
    let staging = dir.path().join("staging");
    if staging.exists() {
        let leftovers: Vec<_> = fs::read_dir(&staging).unwrap().flatten().collect();
        assert!(leftovers.is_empty(), "refused put left staging litter");
    }
}

/// Publishing onto an already-present blob discards the staged snapshot
/// and reports `false` — same-digest means same-bytes, so losing the
/// publish race is benign and must not double-count ingest.
#[test]
fn publish_staged_blob_is_idempotent_when_blob_exists() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let source = dir.path().join("out.rlib");
    fs::write(&source, b"identical-content").unwrap();

    let (staged_a, ingest_a) = store.stage_blob_from_source(&source, false).unwrap();
    let hash = crate::file_hash::hash_file(&staged_a).unwrap();
    assert!(
        store
            .publish_staged_blob(&staged_a, ingest_a, &hash, 17)
            .unwrap(),
        "first publish should win"
    );

    let (staged_b, _ingest_b) = store.stage_blob_from_source(&source, false).unwrap();
    assert_ne!(staged_a, staged_b, "each stage gets its own temp");
    assert!(
        !store
            .publish_staged_blob(&staged_b, ingest_a, &hash, 17)
            .unwrap(),
        "second publish of the same digest must be a no-op"
    );

    let staging_leftovers = fs::read_dir(store.staging_dir()).unwrap().flatten().count();
    assert_eq!(staging_leftovers, 0, "discarded stage must not linger");
}

/// Crash-orphaned staging files are reclaimed only once older than the
/// grace period — a concurrent put's fresh snapshot is never touched.
#[test]
fn sweep_stale_staging_respects_min_age() {
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let stale = store.staging_dir().join("stage-old-1.tmp");
    let fresh = store.staging_dir().join("stage-new-1.tmp");
    fs::create_dir_all(store.staging_dir()).unwrap();
    fs::write(&stale, b"abandoned").unwrap();
    fs::write(&fresh, b"in-flight").unwrap();
    let old = filetime::FileTime::from_unix_time(0, 0);
    filetime::set_file_mtime(&stale, old).unwrap();
    filetime::set_file_atime(&stale, old).unwrap();

    let stats = store.sweep_stale_staging(Duration::from_secs(3600));
    assert_eq!(stats.removed, 1, "only the aged-out file is swept");
    assert_eq!(
        stats.scanned, 1,
        "the in-flight file is skipped before it is ever counted"
    );
    assert_eq!(stats.bytes_reclaimed, b"abandoned".len() as u64);
    assert!(!stale.exists());
    assert!(fresh.exists(), "fresh staging file must survive the sweep");

    // Once it ages out, it goes too.
    let stats = store.sweep_stale_staging(Duration::ZERO);
    assert_eq!(stats.removed, 1);
    assert_eq!(stats.scanned, 1);
    assert_eq!(stats.bytes_reclaimed, b"in-flight".len() as u64);
    assert!(!fresh.exists());
}

/// Discarding a hardlinked staging temp has to hand the source back
/// writable. The temp shares the build output's inode, so the read-only
/// guard the store applies lands on the build's own file too — leaving
/// it read-only would break the next write to that output.
#[cfg(unix)]
#[test]
fn dropping_a_hardlinked_temp_restores_the_source_writable() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("out.rlib");
    fs::write(&source, b"shared-inode-bytes").unwrap();
    let tmp = dir.path().join("stage.tmp");
    fs::hard_link(&source, &tmp).unwrap();
    set_blob_readonly(&tmp);
    assert_eq!(
        fs::metadata(&source).unwrap().permissions().mode() & 0o200,
        0,
        "precondition: the shared inode is read-only through both names"
    );

    Store::drop_tmp_restore_source(&source, &tmp);

    assert!(!tmp.exists(), "the staging temp must be gone");
    assert_eq!(
        fs::metadata(&source).unwrap().permissions().mode() & 0o200,
        0o200,
        "the build output must be writable again once the temp is gone"
    );
}

/// One rename attempt: a winner in place is a lost race whatever the error
/// says, because Windows reports a read-only winner with the same code as
/// a delete-pending name (#1128). Anything else goes back to the retry.
#[test]
fn publish_attempt_outcome_lets_the_destination_decide() {
    let denied = || Err(std::io::Error::from_raw_os_error(5));
    assert_eq!(
        publish_attempt_outcome(Ok(()), || unreachable!("a clean rename needs no probe")).unwrap(),
        PublishRename::Published
    );
    assert_eq!(
        publish_attempt_outcome(denied(), || PublishDest::File).unwrap(),
        PublishRename::LostRace
    );
    for dest in [PublishDest::Vacant, PublishDest::Obstructed] {
        let err = publish_attempt_outcome(denied(), || dest).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(5), "the rename error is kept");
    }
}

/// Every (destination, transient) pair a spent publish can end on.
#[test]
fn publish_failure_outcome_covers_every_interleaving() {
    let settle = |transient, dest| {
        publish_failure_outcome(std::io::Error::from_raw_os_error(5), transient, dest)
    };
    for transient in [true, false] {
        assert_eq!(
            settle(transient, PublishDest::File).unwrap(),
            PublishRename::LostRace,
            "a winner that landed after the last attempt still counts"
        );
        assert!(
            settle(transient, PublishDest::Obstructed).is_err(),
            "no race leaves a non-file at a blob path"
        );
    }
    assert_eq!(
        settle(true, PublishDest::Vacant).unwrap(),
        PublishRename::Deferred,
        "removed under the publisher: the locked phase publishes"
    );
    let err = settle(false, PublishDest::Vacant).unwrap_err();
    assert!(format!("{err:#}").contains("publishing staged blob"));
    assert_eq!(
        err.root_cause().to_string(),
        std::io::Error::from_raw_os_error(5).to_string()
    );
}

#[test]
fn publish_dest_state_tells_a_file_from_a_directory_from_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("blob");
    fs::write(&file, b"x").unwrap();
    assert_eq!(publish_dest_state(&file), PublishDest::File);
    assert_eq!(publish_dest_state(dir.path()), PublishDest::Obstructed);
    assert_eq!(
        publish_dest_state(&dir.path().join("absent")),
        PublishDest::Vacant
    );
}

/// Drive `publish_rename` with a scripted rename and destination probe.
/// Rename attempt `n` succeeds when `rename_ok(n)`; otherwise it fails with
/// ERROR_ACCESS_DENIED and the probe reports `dest(n)`. Returns the outcome
/// and how many renames ran.
fn run_publish_rename(
    rename_ok: impl Fn(u32) -> bool,
    dest: impl Fn(u32) -> PublishDest,
    transient: bool,
) -> (Result<PublishRename>, u32) {
    let calls = std::cell::Cell::new(0u32);
    let outcome = publish_rename(
        || {
            let n = calls.get();
            calls.set(n + 1);
            if rename_ok(n) {
                Ok(())
            } else {
                Err(std::io::Error::from_raw_os_error(5))
            }
        },
        || dest(calls.get() - 1),
        |_| transient,
    );
    (outcome, calls.get())
}

#[test]
fn publish_rename_publishes_on_a_clean_rename() {
    let (outcome, calls) = run_publish_rename(|_| true, |_| PublishDest::Vacant, true);
    assert_eq!(outcome.unwrap(), PublishRename::Published);
    assert_eq!(calls, 1);
}

/// The read-only winner: the rename fails with a "transient" code, but the
/// blob is there. No retry, no sleep.
#[test]
fn publish_rename_settles_at_once_on_a_present_winner() {
    let (outcome, calls) = run_publish_rename(|_| false, |_| PublishDest::File, true);
    assert_eq!(outcome.unwrap(), PublishRename::LostRace);
    assert_eq!(calls, 1, "a present winner must not burn the retry budget");
}

/// The winner was removed between the failed rename and the probe: the
/// name is free, so the next attempt publishes.
#[test]
fn publish_rename_retries_into_a_name_a_remover_freed() {
    let (outcome, calls) = run_publish_rename(|n| n == 1, |_| PublishDest::Vacant, true);
    assert_eq!(outcome.unwrap(), PublishRename::Published);
    assert_eq!(calls, 2);
}

/// A winner that appears while a delete-pending name is waited out.
#[test]
fn publish_rename_stops_retrying_once_a_winner_appears() {
    let dest = |n: u32| {
        if n < 2 {
            PublishDest::Vacant
        } else {
            PublishDest::File
        }
    };
    let (outcome, calls) = run_publish_rename(|_| false, dest, true);
    assert_eq!(outcome.unwrap(), PublishRename::LostRace);
    assert_eq!(calls, 3);
}

/// #1128: every attempt fails with ERROR_ACCESS_DENIED and the name is
/// vacant after the last one, because a remover took the winner. That is
/// not a failed put.
#[test]
fn publish_rename_defers_when_the_budget_ends_on_a_vacant_name() {
    let (outcome, calls) = run_publish_rename(|_| false, |_| PublishDest::Vacant, true);
    assert_eq!(outcome.unwrap(), PublishRename::Deferred);
    assert_eq!(calls, crate::atomic::TRANSIENT_ATTEMPTS);
}

#[test]
fn publish_rename_fails_on_a_settled_error_or_an_obstructed_name() {
    let (outcome, calls) = run_publish_rename(|_| false, |_| PublishDest::Vacant, false);
    let err = outcome.unwrap_err();
    assert!(format!("{err:#}").contains("publishing staged blob"));
    assert_eq!(err.root_cause().to_string(), {
        std::io::Error::from_raw_os_error(5).to_string()
    });
    assert_eq!(calls, 1, "a settled error is not retried");

    let (outcome, calls) = run_publish_rename(|_| false, |_| PublishDest::Obstructed, true);
    assert!(outcome.is_err(), "a directory at the blob path is a fault");
    assert_eq!(calls, crate::atomic::TRANSIENT_ATTEMPTS);
}

/// The real rename onto a read-only winner, past the early `is_file`
/// check the way a lost race gets there. Unix replaces the identical
/// blob; Windows refuses with ERROR_ACCESS_DENIED. Either way one rename
/// settles it and the winner's bytes stay.
#[test]
fn publish_rename_onto_a_read_only_winner_settles_in_one_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("blob");
    let staged = dir.path().join("staged");
    fs::write(&blob, b"same-bytes").unwrap();
    fs::write(&staged, b"same-bytes").unwrap();
    set_blob_readonly(&blob);

    let calls = std::cell::Cell::new(0u32);
    let outcome = publish_rename(
        || {
            calls.set(calls.get() + 1);
            fs::rename(&staged, &blob)
        },
        || publish_dest_state(&blob),
        crate::atomic::is_transient_rename_error,
    )
    .unwrap();
    assert_ne!(outcome, PublishRename::Deferred);
    assert_eq!(calls.get(), 1);
    assert_eq!(fs::read(&blob).unwrap(), b"same-bytes");
    unlink_blob(&blob);
    unlink_blob(&staged);
}

/// The Windows fact #1128 rests on: renaming onto a read-only file is
/// refused with ERROR_ACCESS_DENIED, the code the retry treats as
/// transient. If this stops holding, revisit `publish_attempt_outcome`.
#[cfg(windows)]
#[test]
fn rename_onto_a_read_only_file_is_access_denied_on_windows() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("blob");
    let staged = dir.path().join("staged");
    fs::write(&blob, b"winner").unwrap();
    fs::write(&staged, b"loser").unwrap();
    set_blob_readonly(&blob);

    let err = fs::rename(&staged, &blob).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(5));
    assert!(crate::atomic::is_transient_rename_error(&err));
    assert_eq!(fs::read(&blob).unwrap(), b"winner");
    unlink_blob(&blob);
}

/// A deferred publish leaves nothing behind and reports no error; the
/// put's locked phase then puts the blob in place from the source.
#[test]
fn deferred_publish_is_repaired_by_the_locked_phase() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let source = dir.path().join("out.rlib");
    fs::write(&source, b"removed-under-the-publisher").unwrap();
    let (staged, _ingest) = store.stage_blob_from_source(&source, false).unwrap();
    let hash = crate::file_hash::hash_file(&staged).unwrap();
    let blob = store.blob_path(&hash);

    let outcome = Store::publish_staged_blob_with(
        &staged,
        &blob,
        || Err(std::io::Error::from_raw_os_error(5)),
        |_| true,
    )
    .unwrap();
    assert_eq!(outcome, PublishRename::Deferred);
    assert!(!staged.exists(), "a deferred publish discards its snapshot");
    assert!(!blob.exists());

    store
        .rematerialize_and_verify(&source, &hash, "out.rlib", false)
        .unwrap();
    assert_eq!(fs::read(&blob).unwrap(), b"removed-under-the-publisher");
}

#[test]
fn failed_publish_discards_its_snapshot_and_keeps_the_error() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let source = dir.path().join("out.rlib");
    fs::write(&source, b"content").unwrap();
    let (staged, _ingest) = store.stage_blob_from_source(&source, false).unwrap();
    let blob = store.blob_path(&"f".repeat(64));

    let err = Store::publish_staged_blob_with(
        &staged,
        &blob,
        || Err(std::io::Error::other("disk on fire")),
        |_| false,
    )
    .unwrap_err();
    assert!(format!("{err:#}").contains("disk on fire"));
    assert!(!staged.exists(), "a failed publish discards its snapshot");
}

#[test]
fn successful_publish_keeps_the_staged_bytes_as_the_blob() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let source = dir.path().join("out.rlib");
    fs::write(&source, b"published-bytes").unwrap();
    let (staged, _ingest) = store.stage_blob_from_source(&source, false).unwrap();
    let blob = store.blob_path(&"a".repeat(64));

    let outcome =
        Store::publish_staged_blob_with(&staged, &blob, || fs::rename(&staged, &blob), |_| false)
            .unwrap();
    assert_eq!(outcome, PublishRename::Published);
    assert_eq!(fs::read(&blob).unwrap(), b"published-bytes");
}

/// The grace both sweepers share (daemon GC and `doctor --repair`) must
/// outlast an in-flight put. A snapshot another process is still filling
/// is indistinguishable from a crash leftover, and reclaiming it fails
/// that put at publish time — so a fresh snapshot has to survive.
#[test]
fn staging_sweep_grace_spares_an_in_flight_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    fs::create_dir_all(store.staging_dir()).unwrap();
    let in_flight = store.staging_dir().join("stage-1-0.tmp");
    fs::write(&in_flight, b"mid-put").unwrap();

    let stats = store.sweep_stale_staging(STAGING_SWEEP_GRACE);
    assert_eq!(stats.removed, 0, "no sweeper may reclaim a live snapshot");
    assert!(in_flight.exists());
    assert!(
        STAGING_SWEEP_GRACE >= Duration::from_secs(3600),
        "the grace must stay long enough to outlast a slow put"
    );
}

/// The staging name search must SKIP an occupied candidate (a crash
/// leftover) and hand back the next free one — and hand it back
/// uncreated, which is what keeps the zero-copy ingests usable.
#[test]
fn free_staging_path_skips_occupied_names() {
    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("cand-a");
    let second = dir.path().join("cand-b");
    fs::write(&first, b"taken").unwrap();

    // First candidate always collides; the next one is free.
    let names: Vec<PathBuf> = vec![first.clone(), second.clone()];
    let mut calls = 0usize;
    let got = free_staging_path(|_| {
        let p = names[calls].clone();
        calls += 1;
        p
    })
    .unwrap();
    assert_eq!(got, second, "must skip the taken candidate");
    assert!(
        !got.exists(),
        "the chosen path must NOT exist: clonefile(2)/link(2) fail with \
             EEXIST on an existing destination, which would demote every put \
             to a full byte copy"
    );
}

/// A non-collision error must propagate as itself, not be swallowed by
/// the skip branch and reported as an exhausted name search.
#[test]
fn free_staging_path_propagates_real_errors() {
    let dir = tempfile::tempdir().unwrap();
    // A FILE used as the parent path. Unix stats that as ENOTDIR;
    // Windows reports it with the same shape as a free name.
    let not_a_dir = dir.path().join("not-a-dir");
    fs::write(&not_a_dir, b"").unwrap();
    let result = free_staging_path(|n| not_a_dir.join(format!("x-{n}")));

    #[cfg(unix)]
    {
        // ENOTDIR: a real fault must surface as itself, never as an
        // exhausted-name collision, and never be skipped past.
        let err = result.unwrap_err();
        assert_ne!(
            err.kind(),
            std::io::ErrorKind::AlreadyExists,
            "a real fault must not be reported as a name collision: {err}"
        );
    }
    #[cfg(windows)]
    {
        // Windows cannot tell this fault from a free name, so the search
        // hands the candidate back and the ingest is what fails. What
        // must not happen either way is spinning through every attempt.
        let candidate = result.expect("windows reports the parent as absent");
        assert!(candidate.starts_with(&not_a_dir));
    }
}

/// Exhausting every candidate reports a bounded failure rather than
/// spinning: an unbounded search is a hang no test can kill.
#[test]
fn free_staging_path_gives_up_after_bounded_attempts() {
    let dir = tempfile::tempdir().unwrap();
    let taken = dir.path().join("always-taken");
    fs::write(&taken, b"taken").unwrap();

    let mut calls = 0u32;
    let err = free_staging_path(|_| {
        calls += 1;
        taken.clone()
    })
    .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(calls, STAGING_NAME_ATTEMPTS, "search must be bounded");
}

/// Staging must reach the store by reflink or hardlink, never by writing
/// the artifact's bytes a second time.
///
/// The ingest destination has to be a path that does not exist yet:
/// `clonefile(2)` and `link(2)` both fail with `EEXIST` otherwise, so a
/// staging file that is pre-created (to reserve its name, say) turns a
/// metadata-only clone into a full copy of every artifact — still
/// correct, but it doubles put I/O and stops store blobs from sharing
/// blocks with the build output they came from.
#[cfg(unix)]
#[test]
fn staging_ingests_zero_copy_not_a_byte_copy() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let source = dir.path().join("out.rlib");
    fs::write(&source, b"artifact-bytes").unwrap();

    // Same filesystem as the store, so a hardlink is always available
    // even where the filesystem has no reflink support (ext4, tmpfs).
    let (staged, ingest) = store.stage_blob_from_source(&source, true).unwrap();
    assert!(
        !matches!(ingest, StoreIngest::Copy(_)),
        "staging fell back to a byte copy where a reflink or hardlink \
             was available — the ingest destination must not exist yet"
    );
    assert_eq!(fs::read(&staged).unwrap(), b"artifact-bytes");
    Store::drop_tmp_restore_source(&source, &staged);
}

/// A symlinked source must never be hardlinked into the store: hashing
/// follows the link, but a hardlink would publish a pointer to mutable
/// external state. The staged snapshot must be a regular file carrying
/// the target's content.
#[cfg(unix)]
#[test]
fn staging_refuses_to_hardlink_a_symlink_source() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let target = dir.path().join("real.rlib");
    fs::write(&target, b"target-bytes").unwrap();
    let link = dir.path().join("link.rlib");
    std::os::unix::fs::symlink(&target, &link).unwrap();

    // allow_hardlink=true is the interesting case: only the symlink check
    // stands between the link and an inode-sharing blob.
    let (staged, _ingest) = store.stage_blob_from_source(&link, true).unwrap();
    let meta = fs::symlink_metadata(&staged).unwrap();
    assert!(
        meta.is_file(),
        "staged snapshot must be a regular file, never a symlink"
    );
    assert_eq!(fs::read(&staged).unwrap(), b"target-bytes");
    // Whatever ingest was chosen, the store side of the deal is read-only;
    // the symlink TARGET itself must stay owner-writable when the
    // snapshot did not share its inode (copy/reflink).
    if !paths_share_inode(&target, &staged) {
        let mode = fs::metadata(&target).unwrap().permissions().mode();
        assert_eq!(
            mode & 0o200,
            0o200,
            "an isolated snapshot must not flip the symlink target read-only"
        );
    }
}

/// Lost-race semantics of `publish_staged_blob`: a rename failure while
/// the destination already exists as a file is the benign
/// concurrent-winner case and must report `Ok(false)`.
#[test]
fn publish_reports_false_when_rename_fails_on_existing_blob() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let hash = "d".repeat(64);
    let source = dir.path().join("out.rlib");
    fs::write(&source, b"content").unwrap();

    // Publish the winner so the destination exists as a file...
    let (staged_a, ingest_a) = store.stage_blob_from_source(&source, false).unwrap();
    assert!(
        store
            .publish_staged_blob(&staged_a, ingest_a, &hash, 7)
            .unwrap()
    );

    // ...then force the rename to fail: a DIRECTORY cannot be renamed
    // onto an existing regular file. The staged argument being a
    // directory guarantees the error without touching permissions.
    let bogus_staged = store.staging_dir().join("not-a-file");
    fs::create_dir_all(&bogus_staged).unwrap();
    let result = store.publish_staged_blob(&bogus_staged, ingest_a, &hash, 7);
    assert!(!result.unwrap(), "lost race must report Ok(false)");
}

/// A rename failure with NO existing destination is a genuine error, not
/// a lost race, and must propagate.
#[test]
fn publish_errors_when_rename_fails_without_existing_blob() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let hash = "e".repeat(64);
    let source = dir.path().join("out.rlib");
    fs::write(&source, b"content").unwrap();
    let (staged, ingest) = store.stage_blob_from_source(&source, false).unwrap();

    // Put a directory in the way of the destination: renaming a file
    // onto a directory fails even though `blob.is_file()` is false.
    let blob = store.blob_path(&hash);
    fs::create_dir_all(blob.parent().unwrap()).unwrap();
    fs::create_dir_all(&blob).unwrap();

    let result = store.publish_staged_blob(&staged, ingest, &hash, 7);
    assert!(result.is_err(), "genuine rename errors must propagate");
}

/// Phase-2 recovery re-materializes from the LIVE source; a source that
/// still hashes to the recorded digest commits cleanly.
#[test]
fn rematerialize_accepts_untouched_source() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let source = dir.path().join("out.rlib");
    fs::write(&source, b"stable-content").unwrap();
    let hash = crate::file_hash::hash_file(&source).unwrap();

    store
        .rematerialize_and_verify(&source, &hash, "out.rlib", false)
        .unwrap();
    assert_eq!(fs::read(store.blob_path(&hash)).unwrap(), b"stable-content");
}

/// ...but a source mutated after phase 1 must NEVER be stored under the
/// recorded address: the verification must refuse the commit.
#[test]
fn rematerialize_refuses_mutated_source() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let source = dir.path().join("out.rlib");
    fs::write(&source, b"original").unwrap();
    let hash = crate::file_hash::hash_file(&source).unwrap();

    // Simulate a post-build mutator racing between phase 1 and recovery.
    fs::write(&source, b"mutated-after-snapshot").unwrap();

    let err = store
        .rematerialize_and_verify(&source, &hash, "out.rlib", false)
        .unwrap_err();
    assert!(
        err.to_string().contains("refusing to commit"),
        "expected digest-mismatch refusal, got: {err:#}"
    );
}

/// A published blob is read-only; flushing one must work anyway, and a
/// blob that is gone reports `NotFound` so the flusher can tell a lost
/// entry from a transient error.
#[test]
fn a_read_only_blob_flushes_and_a_missing_one_reports_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("blob");
    fs::write(&blob, b"artifact-bytes").unwrap();
    set_blob_readonly(&blob);
    assert!(
        fs::metadata(&blob).unwrap().permissions().readonly(),
        "the fixture must reproduce a published blob"
    );
    fsync_published_blob(&blob).expect("a published blob flushes");
    assert!(
        fs::metadata(&blob).unwrap().permissions().readonly(),
        "and stays read-only afterwards"
    );
    assert_eq!(
        fsync_published_blob(&dir.path().join("absent"))
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::NotFound
    );
}

/// The ingest helpers follow the flush policy of the store last opened
/// on this thread: deferred stores skip the inline fsync, others keep it.
#[test]
fn blob_ingest_follows_the_last_opened_store_policy() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path());
    config.deferred_durability = true;
    let _deferred = Store::open(&config).unwrap();
    assert!(
        !durable_writes_now(),
        "a deferred store skips the inline fsync"
    );
    config.deferred_durability = false;
    let _strict = Store::open(&config).unwrap();
    assert!(
        durable_writes_now(),
        "a durable store keeps the inline fsync"
    );
}

/// Deferred durability: a put leaves the entry pending, a hit on a
/// pending entry verifies the bytes (a same-size corruption is caught and
/// evicted), the flush marks it durable, and a durable entry is served on
/// its size check as before. With the feature off, a put is durable at
/// once.
#[test]
fn deferred_durability_verifies_pending_hits_until_the_flush() {
    let _env_lock = crate::test_support::process_state_test_lock();
    let _verify = EnvVarGuard::remove("KACHE_VERIFY_RESTORES");
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path());
    config.deferred_durability = true;
    let store = Store::open(&config).unwrap();
    // One source per put: a stored source may become a read-only
    // hardlink of its blob, so it cannot be rewritten for the next.
    let puts = std::cell::Cell::new(0u32);
    let put = |bytes: &[u8]| {
        puts.set(puts.get() + 1);
        let output_file = dir.path().join(format!("out-{}.rlib", puts.get()));
        fs::write(&output_file, bytes).unwrap();
        store
            .put(
                "pending_key",
                "pending_crate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(output_file.clone(), "libout.rlib".to_string())],
                "",
                "",
            )
            .unwrap()
    };
    let corrupt_same_size = |blob: &Path| {
        let mut perms = fs::metadata(blob).unwrap().permissions();
        perms.set_readonly(false);
        fs::set_permissions(blob, perms).unwrap();
        let mut bytes = fs::read(blob).unwrap();
        bytes[0] ^= 0xff;
        fs::write(blob, bytes).unwrap();
    };

    put(b"artifact-bytes-one");
    assert_eq!(store.pending_durability().unwrap(), 1);
    let meta = store
        .get("pending_key")
        .unwrap()
        .expect("intact pending entry hits");
    let blob = store.blob_path(&meta.files[0].hash);
    corrupt_same_size(&blob);
    assert!(
        store.get("pending_key").unwrap().is_none(),
        "a pending entry whose bytes changed is evicted, not served"
    );
    assert!(!store.contains("pending_key"));

    put(b"artifact-bytes-two");
    assert_eq!(store.pending_durability().unwrap(), 1);
    assert_eq!(store.flush_durability(10).unwrap(), 1);
    assert_eq!(store.pending_durability().unwrap(), 0);
    assert_eq!(
        store.flush_durability(10).unwrap(),
        0,
        "nothing left to flush"
    );
    assert!(!store.flush_entry_durability("pending_key").unwrap());
    let meta = store.get("pending_key").unwrap().unwrap();
    corrupt_same_size(&store.blob_path(&meta.files[0].hash));
    assert!(
        store.get("pending_key").unwrap().is_some(),
        "a durable entry keeps the size-only check the verification policy asks for"
    );

    // A pending entry whose blob vanished is evicted by the flush.
    store.remove_entry("pending_key").unwrap();
    put(b"artifact-bytes-three");
    let meta = store.get("pending_key").unwrap().unwrap();
    let blob = store.blob_path(&meta.files[0].hash);
    let mut perms = fs::metadata(&blob).unwrap().permissions();
    perms.set_readonly(false);
    fs::set_permissions(&blob, perms).unwrap();
    fs::remove_file(&blob).unwrap();
    assert_eq!(store.flush_durability(10).unwrap(), 0);
    assert!(
        !store.contains("pending_key"),
        "evicted rather than marked durable"
    );

    // Feature off: the put is durable inside the compile.
    config.deferred_durability = false;
    let strict = Store::open(&config).unwrap();
    let strict_file = dir.path().join("out-strict.rlib");
    fs::write(&strict_file, b"artifact-bytes-four").unwrap();
    strict
        .put(
            "strict_key",
            "strict_crate",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(strict_file.clone(), "libout.rlib".to_string())],
            "",
            "",
        )
        .unwrap();
    assert_eq!(strict.pending_durability().unwrap(), 0);
    assert!(strict.try_durability_flush_lock().unwrap().is_some());
}

/// `Store::get` counts a hit once per stamp interval and evicts an entry
/// whose blob is missing or has the wrong size, whatever the damage.
#[test]
fn get_counts_hits_and_evicts_damaged_blobs() {
    let _env_lock = crate::test_support::process_state_test_lock();
    let _verify = EnvVarGuard::remove("KACHE_VERIFY_RESTORES");
    for damage in ["missing", "short", "long", "directory"] {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output_file = dir.path().join("out.rlib");
        fs::write(&output_file, b"artifact-bytes").unwrap();
        store
            .put(
                "probe_key",
                "probe_crate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(output_file, "libout.rlib".to_string())],
                "out",
                "err",
            )
            .unwrap();

        let hit_count = || {
            store
                .db
                .query_row(
                    "SELECT hit_count FROM entries WHERE cache_key = 'probe_key'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap()
        };
        assert_eq!(hit_count(), 0);

        // A hit re-stamps an entry only once per `HIT_STAMP_INTERVAL`;
        // the put just stamped it, so age it first.
        store.set_last_accessed_for_test("probe_key", "-1 minutes");
        let meta = store.get("probe_key").unwrap().unwrap();
        assert_eq!(meta.cache_key, "probe_key");
        assert_eq!(meta.stdout, "out");
        assert_eq!(meta.stderr, "err");
        assert_eq!(meta.files.len(), 1);
        assert_eq!(hit_count(), 1, "get must record the hit");
        let _ = store.get("probe_key").unwrap().unwrap();
        assert_eq!(hit_count(), 1, "a fresh stamp is not rewritten");

        let blob = store.blob_path(&meta.files[0].hash);
        let mut perms = fs::metadata(&blob).unwrap().permissions();
        perms.set_readonly(false);
        fs::set_permissions(&blob, perms).unwrap();
        fs::remove_file(&blob).unwrap();
        match damage {
            "missing" => {}
            "short" => fs::write(&blob, b"short").unwrap(),
            "long" => fs::write(&blob, b"longer than the original artifact").unwrap(),
            "directory" => fs::create_dir(&blob).unwrap(),
            _ => unreachable!(),
        }

        assert!(store.get("probe_key").unwrap().is_none(), "{damage}");
        assert!(!store.contains("probe_key"), "get must evict: {damage}");
    }
}

#[test]
fn probe_connection_refuses_writes() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let _store = Store::open(&config).unwrap();
    let ro = open_index_db_readonly(&config.index_db_path()).unwrap();
    assert!(
        ro.execute("DELETE FROM entries", []).is_err(),
        "read-only probe connection must reject writes"
    );
}

fn test_config(dir: &Path) -> Config {
    Config {
        cache_dir: dir.to_path_buf(),
        max_size: 1024 * 1024,
        gc_evict_shared: false,
        upload_spool_max_jobs: 65_536,
        deferred_durability: false,
    }
}

struct EnvVarGuard {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var_os(key);
        unsafe { std::env::set_var(key, value) };
        Self { key, previous }
    }

    fn remove(key: &'static str) -> Self {
        let previous = std::env::var_os(key);
        unsafe { std::env::remove_var(key) };
        Self { key, previous }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => unsafe { std::env::set_var(self.key, value) },
            None => unsafe { std::env::remove_var(self.key) },
        }
    }
}

/// kunobi-ninja/kache#336: diagnostics are stored in full by default (so a
/// hit replays exactly what a miss emitted), and only truncated — at a char
/// boundary, with a marker — when an explicit cap is set.
#[test]
fn cap_diagnostics_is_lossless_by_default_and_truncates_when_capped() {
    let warnings = "warning: unused variable `x`\nwarning: dead code\n";
    // Uncapped: byte-identical replay.
    assert_eq!(cap_diagnostics(warnings, None), warnings);
    // Cap above length: unchanged.
    assert_eq!(cap_diagnostics(warnings, Some(10_000)), warnings);
    // Cap below length: truncated with a marker, original tail dropped.
    let capped = cap_diagnostics(warnings, Some(20));
    assert!(capped.starts_with("warning: unused vari"));
    assert!(capped.contains("diagnostics truncated"));
    assert!(capped.len() < warnings.len() + 80);
    // Multi-byte safety: never split a char.
    let unicode = "wörning: ".repeat(20);
    let capped = cap_diagnostics(&unicode, Some(5));
    assert!(std::str::from_utf8(capped.as_bytes()).is_ok());
}

#[test]
fn file_hash_records_persist_and_reject_changed_fingerprints() {
    use crate::file_hash::{FileFingerprint, FileHashLookup};

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let artifact = dir.path().join("artifact.rlib");
    fs::write(&artifact, vec![7; 65_536]).unwrap();
    let fingerprint = FileFingerprint::from_path(&artifact).unwrap();
    assert!(matches!(
        store.file_hash_lookup(&artifact),
        FileHashLookup::NeedsHash(_)
    ));

    store.file_hash_record(&fingerprint, "recorded");
    assert!(
        matches!(store.file_hash_lookup(&artifact), FileHashLookup::Hit(hash) if hash == "recorded")
    );
    store.record_verified_file_hash(&fingerprint, "verified");
    assert!(
        matches!(store.file_hash_lookup(&artifact), FileHashLookup::Hit(hash) if hash == "verified")
    );
    let second = dir.path().join("second.rlib");
    fs::write(&second, vec![9; 65_536]).unwrap();
    let second_fingerprint = FileFingerprint::from_path(&second).unwrap();
    store.record_verified_file_hashes(&[
        (fingerprint.clone(), "batched"),
        (second_fingerprint, "batched-second"),
    ]);
    assert!(
        matches!(store.file_hash_lookup(&artifact), FileHashLookup::Hit(hash) if hash == "batched")
    );
    assert!(
        matches!(store.file_hash_lookup(&second), FileHashLookup::Hit(hash) if hash == "batched-second")
    );
    store.record_verified_file_hashes(&[]);
    drop(store);

    let store = Store::open(&config).unwrap();
    assert!(
        matches!(store.file_hash_lookup(&artifact), FileHashLookup::Hit(hash) if hash == "batched")
    );
    fs::write(&artifact, vec![8; 65_537]).unwrap();
    assert!(matches!(
        store.file_hash_lookup(&artifact),
        FileHashLookup::NeedsHash(_)
    ));
}

#[test]
fn readonly_blob_detection_requires_a_shared_inode() {
    let dir = tempfile::tempdir().unwrap();
    let store_dir = dir.path().join("store");
    let output = dir.path().join("output.o");
    fs::write(&output, b"legacy compiler output").unwrap();
    let writable = fs::metadata(&output).unwrap().permissions();
    assert_eq!(
        Store::matching_readonly_blob_inode(&store_dir, &output).unwrap(),
        None
    );

    let hash = crate::file_hash::hash_file(&output).unwrap();
    let blob = blob_path_in_store_dir(&store_dir, &hash);
    fs::create_dir_all(blob.parent().unwrap()).unwrap();
    fs::hard_link(&output, &blob).unwrap();
    let mut readonly = writable.clone();
    readonly.set_readonly(true);
    fs::set_permissions(&output, readonly).unwrap();
    assert_eq!(
        Store::matching_readonly_blob_inode(&store_dir, &output).unwrap(),
        Some(blob)
    );

    let independent = dir.path().join("independent.o");
    fs::copy(&output, &independent).unwrap();
    assert!(fs::metadata(&independent).unwrap().permissions().readonly());
    assert_eq!(
        Store::matching_readonly_blob_inode(&store_dir, &independent).unwrap(),
        None
    );
    fs::set_permissions(&output, writable.clone()).unwrap();
    fs::set_permissions(&independent, writable).unwrap();
}

#[test]
fn put_records_known_hash_only_for_stable_outputs() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    let artifact = dir.path().join("artifact.rlib");
    std::fs::write(&artifact, vec![b'x'; 64 * 1024]).unwrap();
    let expected = crate::file_hash::hash_file(&artifact).unwrap();

    store
        .put(
            "known-hash-stable",
            "artifact",
            &["rlib".to_string()],
            &[],
            "host",
            "dev",
            &[(artifact.clone(), "libartifact.rlib".to_string())],
            "",
            "",
        )
        .unwrap();
    match store.file_hash_lookup(&artifact) {
        crate::file_hash::FileHashLookup::Hit(actual) => assert_eq!(actual, expected),
        _ => panic!("publication should seed the persistent file hash"),
    }

    let dep_info = dir.path().join("artifact.d");
    std::fs::write(&dep_info, vec![b'd'; 64 * 1024]).unwrap();
    store
        .put(
            "known-hash-dep-info",
            "artifact",
            &[],
            &[],
            "host",
            "dev",
            &[(dep_info.clone(), "artifact.d".to_string())],
            "",
            "",
        )
        .unwrap();
    assert!(matches!(
        store.file_hash_lookup(&dep_info),
        crate::file_hash::FileHashLookup::NeedsHash(_)
    ));

    let independent = dir.path().join("independent.o");
    std::fs::write(&independent, vec![b'o'; 64 * 1024]).unwrap();
    store
        .put_with_compile_time_independent(
            "known-hash-independent",
            "artifact.c",
            &[],
            &[],
            "host",
            "dev",
            &[(independent.clone(), "independent.o".to_string())],
            "",
            "",
            1,
        )
        .unwrap();
    assert!(matches!(
        store.file_hash_lookup(&independent),
        crate::file_hash::FileHashLookup::NeedsHash(_)
    ));
}

#[test]
fn test_store_put_and_get() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    // Create a fake output file
    let output_file = dir.path().join("output.rlib");
    std::fs::write(&output_file, b"fake rlib content").unwrap();

    store
        .put(
            "abc123",
            "mylib",
            &["lib".to_string()],
            &["std".to_string()],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(output_file, "libmylib.rlib".to_string())],
            "",
            "",
        )
        .unwrap();

    assert!(store.contains("abc123"));
    let meta = store.get("abc123").unwrap().unwrap();
    assert_eq!(meta.crate_name, "mylib");
    assert_eq!(meta.files.len(), 1);
    assert_eq!(meta.files[0].name, "libmylib.rlib");
}

#[test]
fn sweep_orphan_blobs_removes_unreferenced_files_only() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    // A real entry → its blob is referenced (has a `blobs` row).
    let output_file = dir.path().join("output.rlib");
    std::fs::write(&output_file, b"real rlib content").unwrap();
    store
        .put(
            "abc123",
            "mylib",
            &["lib".to_string()],
            &["std".to_string()],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(output_file, "libmylib.rlib".to_string())],
            "",
            "",
        )
        .unwrap();

    // An orphan blob: a 64-hex file on disk with no `blobs` row, as a
    // crash mid-put would leave behind.
    let orphan_hash = "f".repeat(64);
    let orphan_path = store.blob_path(&orphan_hash);
    std::fs::create_dir_all(orphan_path.parent().unwrap()).unwrap();
    std::fs::write(&orphan_path, b"orphaned bytes").unwrap();
    // A `.tmp` in-progress file must never be touched by the sweep.
    let tmp_path = orphan_path.with_file_name(format!(".{orphan_hash}.123.0.tmp"));
    std::fs::write(&tmp_path, b"in-progress").unwrap();

    // min_age 0 → sweep the freshly-created orphan immediately.
    let stats = store.sweep_orphan_blobs(std::time::Duration::ZERO).unwrap();

    assert_eq!(stats.removed, 1, "only the orphan should be removed");
    // The put blob + the orphan are blob-shaped; the `.tmp` is excluded.
    assert_eq!(stats.scanned, 2);
    assert_eq!(stats.bytes_reclaimed, b"orphaned bytes".len() as u64);
    assert!(!orphan_path.exists(), "orphan blob must be unlinked");
    assert!(tmp_path.exists(), "in-progress .tmp must be left alone");
    // The referenced entry's blob survived: get() still restores it.
    assert!(store.get("abc123").unwrap().is_some());
}

#[test]
fn sweep_orphan_blobs_respects_min_age() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let orphan_hash = "a".repeat(64);
    let orphan_path = store.blob_path(&orphan_hash);
    std::fs::create_dir_all(orphan_path.parent().unwrap()).unwrap();
    std::fs::write(&orphan_path, b"fresh orphan").unwrap();

    // A freshly written orphan is younger than the grace period, so a
    // concurrent put materializing it would be protected: not swept.
    let stats = store
        .sweep_orphan_blobs(std::time::Duration::from_secs(3600))
        .unwrap();
    assert_eq!(stats.removed, 0);
    assert!(orphan_path.exists());
}

#[test]
fn reconcile_blob_index_repairs_refcounts_and_stale_rows_idempotently() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let payload = b"shared authoritative blob";

    for key in ["repair_a", "repair_b"] {
        let output = dir.path().join(format!("{key}.rlib"));
        fs::write(&output, payload).unwrap();
        store
            .put(
                key,
                "repairlib",
                &["lib".to_string()],
                &[],
                "host",
                "dev",
                &[(output, format!("lib{key}.rlib"))],
                "",
                "",
            )
            .unwrap();
    }
    let hash = store.get("repair_a").unwrap().unwrap().files[0]
        .hash
        .clone();
    let stale_hash = "f".repeat(64);
    let stale_path = store.blob_path(&stale_hash);
    fs::create_dir_all(stale_path.parent().unwrap()).unwrap();
    fs::write(&stale_path, b"stale indexed blob").unwrap();

    store
        .db
        .execute(
            "UPDATE blobs SET refcount = 41 WHERE hash = ?1",
            params![hash],
        )
        .unwrap();
    store
        .db
        .execute(
            "UPDATE entry_blobs SET refs = 7 WHERE cache_key = 'repair_a'",
            [],
        )
        .unwrap();
    store
        .db
        .execute(
            "INSERT INTO blobs (hash, size, refcount) VALUES (?1, ?2, 9)",
            params![stale_hash, b"stale indexed blob".len() as i64],
        )
        .unwrap();

    assert_eq!(
        store.blob_index_drift().unwrap(),
        BlobIndexDrift {
            entry_mappings: 1,
            blobs: 2,
        }
    );
    assert_eq!(
        store.reconcile_blob_index().unwrap(),
        BlobIndexDrift {
            entry_mappings: 1,
            blobs: 2,
        }
    );
    assert_eq!(store.blob_index_drift().unwrap(), BlobIndexDrift::default());
    assert_eq!(
        store.reconcile_blob_index().unwrap(),
        BlobIndexDrift::default()
    );

    let refcount: i64 = store
        .db
        .query_row(
            "SELECT refcount FROM blobs WHERE hash = ?1",
            params![hash],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(refcount, 2);
    assert_eq!(
        store
            .db
            .query_row(
                "SELECT COUNT(*) FROM blobs WHERE hash = ?1",
                params![stale_hash],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    let swept = store.sweep_orphan_blobs(Duration::ZERO).unwrap();
    assert_eq!(swept.removed, 1);
    assert!(!stale_path.exists());
}

#[test]
fn reconcile_blob_index_fails_closed_on_unreadable_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let output = dir.path().join("output.rlib");
    fs::write(&output, b"authoritative bytes").unwrap();
    store
        .put(
            "repair_bad_meta",
            "repairlib",
            &["lib".to_string()],
            &[],
            "host",
            "dev",
            &[(output, "librepair.rlib".to_string())],
            "",
            "",
        )
        .unwrap();
    let hash = store.get("repair_bad_meta").unwrap().unwrap().files[0]
        .hash
        .clone();
    store
        .db
        .execute(
            "UPDATE blobs SET refcount = 9 WHERE hash = ?1",
            params![hash],
        )
        .unwrap();
    fs::write(
        store.entry_dir("repair_bad_meta").join("meta.json"),
        b"not json",
    )
    .unwrap();

    let error = store.reconcile_blob_index().unwrap_err().to_string();
    assert!(error.contains("parsing authoritative meta.json"), "{error}");
    let refcount: i64 = store
        .db
        .query_row(
            "SELECT refcount FROM blobs WHERE hash = ?1",
            params![hash],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(refcount, 9, "failed repair must leave the index untouched");
}

#[test]
fn reconcile_blob_index_rejects_each_invalid_metadata_dimension() {
    for invalid_hash in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let output = dir.path().join("output.rlib");
        fs::write(&output, b"valid blob bytes").unwrap();
        store
            .put(
                "repair_invalid_metadata",
                "repairlib",
                &["lib".to_string()],
                &[],
                "host",
                "dev",
                &[(output, "librepair.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        let meta_path = store.entry_dir("repair_invalid_metadata").join("meta.json");
        let mut meta: EntryMeta =
            serde_json::from_str(&fs::read_to_string(&meta_path).unwrap()).unwrap();
        if invalid_hash {
            meta.files[0].hash = "not-a-content-hash".to_string();
        } else {
            meta.files[0].name = "../unsafe.rlib".to_string();
        }
        fs::write(&meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();

        let error = store.reconcile_blob_index().unwrap_err().to_string();
        assert!(error.contains("invalid blob metadata"), "{error}");
    }
}

#[test]
fn blob_index_accepts_build_script_out_dir_artifact_names() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let flat = dir.path().join("asm.s");
    fs::write(&flat, b"asm bytes").unwrap();
    let nested = dir.path().join("asm.o");
    fs::write(&nested, b"object bytes").unwrap();

    // `build_script.rs` records OUT_DIR contents as `out/<relative>`, so a
    // committed build-script entry is *not* drift: the verifier has to
    // accept the names the writer produces. Otherwise every build script
    // makes `doctor --verify` report phantom corruption it cannot repair.
    store
        .put(
            "build_script_entry",
            "build_script_run",
            &["build-script".to_string()],
            &[],
            "host",
            "dev",
            &[
                (flat, "out/asm.s".to_string()),
                (nested, "out/nested/asm.o".to_string()),
            ],
            "",
            "",
        )
        .unwrap();

    assert_eq!(store.blob_index_drift().unwrap().total(), 0);
    assert_eq!(store.reconcile_blob_index().unwrap().total(), 0);
}

#[test]
fn rebuild_index_from_store_adopts_build_script_names_and_refuses_escaping_ones() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let output = dir.path().join("asm.s");
    fs::write(&output, b"asm bytes").unwrap();
    // Rebuild scans entry directories by cache key, so this must be one.
    let key = "b".repeat(64);
    store
        .put(
            &key,
            "build_script_run",
            &["build-script".to_string()],
            &[],
            "host",
            "dev",
            &[(output, "out/asm.s".to_string())],
            "",
            "",
        )
        .unwrap();

    store.db.execute("DELETE FROM entries", []).unwrap();
    let stats = store.rebuild_index_from_store().unwrap();
    assert_eq!(stats.entries_rebuilt, 1, "{stats:?}");
    assert_eq!(stats.entries_skipped, 0, "{stats:?}");
    assert!(store.contains(&key));

    // Rebuild reads the same committed metadata as the index checks, so a
    // name that escapes the entry dir is still refused rather than
    // registered.
    let meta_path = store.entry_dir(&key).join("meta.json");
    let mut meta: EntryMeta =
        serde_json::from_str(&fs::read_to_string(&meta_path).unwrap()).unwrap();
    meta.files[0].name = "../escape.s".to_string();
    fs::write(&meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();

    store.db.execute("DELETE FROM entries", []).unwrap();
    let stats = store.rebuild_index_from_store().unwrap();
    assert_eq!(stats.entries_rebuilt, 0, "{stats:?}");
    assert_eq!(stats.entries_skipped, 1, "{stats:?}");
}

#[test]
fn put_rejects_an_artifact_name_that_escapes_the_entry_dir() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let output = dir.path().join("output.rlib");
    fs::write(&output, b"valid blob bytes").unwrap();

    let error = store
        .put(
            "escaping_name",
            "escapelib",
            &["lib".to_string()],
            &[],
            "host",
            "dev",
            &[(output, "../escape.rlib".to_string())],
            "",
            "",
        )
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("unsafe name"),
        "a traversal must be refused before it reaches meta.json: {error}"
    );
    assert!(!store.contains("escaping_name"));
}

#[test]
fn store_ingest_accounts_new_blob_bytes_by_mechanism() {
    // A new-blob put must record the artifact's bytes against exactly one
    // store-ingest counter — reflink, hardlink, or copy depending on the
    // filesystem and artifact kind. The counters are process-global and
    // monotonic, so a delta of at least the artifact size is a safe
    // assertion under parallel test execution.
    let cache_dir = tempfile::tempdir().unwrap();
    let config = test_config(cache_dir.path());
    let store = Store::open(&config).unwrap();

    // Unique content so this is genuinely a new blob, not a dup of a blob
    // some concurrent test happened to store (which would skip ingest).
    let payload = b"store-ingest-accounting-unique-artifact-bytes-0xC0FFEE".repeat(64);
    let output_file = cache_dir.path().join("output.rlib");
    std::fs::write(&output_file, &payload).unwrap();

    let before = crate::opcounts::store_reflinked_bytes()
        + crate::opcounts::store_hardlinked_bytes()
        + crate::opcounts::store_copied_bytes();
    let put_result = store
        .put(
            "ingest_key",
            "ingestlib",
            &["lib".to_string()],
            &[],
            "host",
            "dev",
            &[(output_file, "libingest.rlib".to_string())],
            "",
            "",
        )
        .unwrap();
    assert_eq!(put_result.new_blobs, 1, "expected a genuinely new blob");

    let after = crate::opcounts::store_reflinked_bytes()
        + crate::opcounts::store_hardlinked_bytes()
        + crate::opcounts::store_copied_bytes();
    assert!(
        after >= before + payload.len() as u64,
        "store ingest must account the new blob's bytes (delta {} < {})",
        after - before,
        payload.len()
    );
}

#[test]
fn test_store_put_reports_full_dup_for_existing_blob() {
    let cache_dir = tempfile::tempdir().unwrap();
    let config = test_config(cache_dir.path());
    let store = Store::open(&config).unwrap();

    let output_file = cache_dir.path().join("output.rlib");
    std::fs::write(&output_file, b"fake rlib content").unwrap();

    let put_result = store
        .put(
            "first_key",
            "mylib",
            &["lib".to_string()],
            &[],
            "host",
            "dev",
            &[(output_file.clone(), "libmylib.rlib".to_string())],
            "",
            "",
        )
        .unwrap();
    assert_eq!(put_result.output_blobs, 1);
    assert_eq!(put_result.duplicate_blobs, 0);
    assert_eq!(put_result.new_blobs, 1);
    assert!(!put_result.is_full_dup());

    let meta = store.get("first_key").unwrap().unwrap();
    let hash = meta.files[0].hash.clone();
    assert!(store.blob_path(&hash).is_file());

    let duplicate_output = cache_dir.path().join("duplicate-output.rlib");
    std::fs::write(&duplicate_output, b"fake rlib content").unwrap();
    let second_put = store
        .put(
            "second_key",
            "mylib",
            &["lib".to_string()],
            &[],
            "host",
            "dev",
            &[(duplicate_output, "libmylib.rlib".to_string())],
            "",
            "",
        )
        .unwrap();
    assert_eq!(second_put.output_blobs, 1);
    assert_eq!(second_put.duplicate_blobs, 1);
    assert_eq!(second_put.new_blobs, 0);
    assert!(second_put.is_full_dup());

    store.remove_entry("first_key").unwrap();
    assert!(store.blob_path(&hash).exists());
    store.remove_entry("second_key").unwrap();
    assert!(!store.blob_path(&hash).exists());
}

#[test]
fn test_retryable_sqlite_open_error_for_missing_parent() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("missing").join("index.db");

    let err = open_index_db(&db_path).unwrap_err();
    let sql_err = err.downcast_ref::<SqlError>().unwrap();

    assert!(is_retryable_sqlite_open_error(sql_err));
}

#[test]
fn is_corruption_error_flags_a_non_sqlite_file() {
    let dir = tempfile::tempdir().unwrap();
    let garbage = dir.path().join("garbage.db");
    fs::write(&garbage, b"definitely not a sqlite database").unwrap();
    let err = try_open_index_db(&garbage).unwrap_err();
    assert!(
        is_corruption_error(&err),
        "a non-sqlite file must classify as corruption: {err}"
    );

    // A transient open failure (missing parent → CannotOpen) is NOT
    // corruption and must not be self-healed.
    let missing = dir.path().join("missing").join("index.db");
    let err = try_open_index_db(&missing).unwrap_err();
    assert!(!is_corruption_error(&err));
}

/// A realistic 64-hex cache key, since the rebuild scan only adopts entry
/// dirs whose name is a well-formed key.
fn key(seed: u8) -> String {
    blake3::hash(&[seed]).to_hex().to_string()
}

/// Put one single-file entry and return its key.
fn put_entry(store: &Store, dir: &Path, seed: u8, crate_name: &str, content: &[u8]) -> String {
    let k = key(seed);
    let src = dir.join(format!("out-{seed}.rlib"));
    std::fs::write(&src, content).unwrap();
    store
        .put(
            &k,
            crate_name,
            &["lib".to_string()],
            &["std".to_string()],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(src, format!("lib{crate_name}.rlib"))],
            "",
            "",
        )
        .unwrap();
    let _ = std::fs::remove_file(dir.join(format!("out-{seed}.rlib")));
    k
}

#[test]
fn rebuild_index_from_store_recovers_entries_after_the_index_is_lost() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let (k1, k2) = {
        let store = Store::open(&config).unwrap();
        let k1 = put_entry(&store, dir.path(), 1, "alpha", b"alpha rlib content");
        let k2 = put_entry(&store, dir.path(), 2, "beta", b"beta rlib content");
        (k1, k2)
    };

    // Lose the index entirely, keeping the store (blobs + meta.json) intact.
    // This is what quarantining a corrupt index leaves behind.
    std::fs::remove_file(config.index_db_path()).unwrap();

    let store = Store::open(&config).unwrap();
    assert_eq!(
        store.entry_count().unwrap(),
        0,
        "a fresh index starts with no rows"
    );

    let stats = store.rebuild_index_from_store().unwrap();
    assert_eq!(
        stats.entries_rebuilt, 2,
        "both entries are adopted: {stats:?}"
    );
    assert_eq!(stats.blobs_registered, 2);

    // The cache is warm again: both keys resolve and restore.
    for k in [&k1, &k2] {
        assert!(store.contains(k), "entry {k} must be usable after rebuild");
        let meta = store.get(k).unwrap().unwrap();
        assert_eq!(meta.files.len(), 1);
    }
    assert_eq!(store.entry_count().unwrap(), 2);
}

#[test]
fn rebuild_index_is_idempotent_and_does_not_inflate_refcounts() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let k = put_entry(&store, dir.path(), 3, "gamma", b"gamma rlib content");

    let hash: String = store
        .db
        .query_row("SELECT hash FROM blobs", [], |r| r.get(0))
        .unwrap();
    let refcount_before: i64 = store
        .db
        .query_row(
            "SELECT refcount FROM blobs WHERE hash = ?1",
            params![hash],
            |r| r.get(0),
        )
        .unwrap();

    // Running against an already-populated index must be a no-op. If it
    // added refcounts, the blob would outlive its last referrer and leak.
    for _ in 0..3 {
        let stats = store.rebuild_index_from_store().unwrap();
        assert_eq!(
            stats.entries_rebuilt, 0,
            "an already-registered entry is not re-adopted"
        );
    }

    let refcount_after: i64 = store
        .db
        .query_row(
            "SELECT refcount FROM blobs WHERE hash = ?1",
            params![hash],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        refcount_after, refcount_before,
        "repeated rebuilds must not inflate refcounts"
    );

    // And removal still reclaims the blob, proving the refcount is truthful.
    store.remove_entry(&k).unwrap();
    let remaining: i64 = store
        .db
        .query_row(
            "SELECT COUNT(*) FROM blobs WHERE hash = ?1",
            params![hash],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        remaining, 0,
        "blob must be reclaimed on removal, not stranded by an inflated refcount"
    );
}

#[test]
fn rebuild_index_skips_entries_whose_blobs_are_missing_or_wrong_size() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let (good, gone, truncated) = {
        let store = Store::open(&config).unwrap();
        let good = put_entry(&store, dir.path(), 4, "good", b"good content here");
        let gone = put_entry(&store, dir.path(), 5, "gone", b"vanishing content");
        let truncated = put_entry(&store, dir.path(), 6, "trunc", b"truncated content");
        (good, gone, truncated)
    };

    // Break two of the three blobs, then lose the index.
    let meta_of = |k: &str| -> EntryMeta {
        let p = config.store_dir().join(k).join("meta.json");
        serde_json::from_str(&std::fs::read_to_string(p).unwrap()).unwrap()
    };
    let gone_hash = meta_of(&gone).files[0].hash.clone();
    let trunc_hash = meta_of(&truncated).files[0].hash.clone();
    let blob_of = |h: &str| blob_path_in_store_dir(&config.store_dir(), h);
    let gone_blob = blob_of(&gone_hash);
    let trunc_blob = blob_of(&trunc_hash);
    // Blobs are stored read-only (`set_blob_readonly`). Windows refuses to
    // delete or write a read-only file, so clear the bit before doing either
    // — on Unix `remove_file` would have succeeded regardless, which is why
    // omitting it passed locally and only failed on Windows CI.
    let make_writable = |p: &Path| {
        let mut perms = std::fs::metadata(p).unwrap().permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        perms.set_readonly(false);
        std::fs::set_permissions(p, perms).unwrap();
    };
    make_writable(&gone_blob);
    make_writable(&trunc_blob);
    std::fs::remove_file(&gone_blob).unwrap();
    std::fs::write(&trunc_blob, b"short").unwrap();
    std::fs::remove_file(config.index_db_path()).unwrap();

    let store = Store::open(&config).unwrap();
    let stats = store.rebuild_index_from_store().unwrap();

    // Only the intact entry is advertised. Registering an entry whose blob is
    // absent or the wrong length would be a false hit: worse than a miss.
    assert_eq!(stats.entries_rebuilt, 1, "only the intact entry: {stats:?}");
    assert_eq!(stats.entries_skipped, 2);
    assert!(store.contains(&good));
    assert!(
        !store.contains(&gone),
        "an entry with a missing blob must not be registered"
    );
    assert!(
        !store.contains(&truncated),
        "an entry with a wrong-sized blob must not be registered"
    );
}

#[test]
fn rebuild_index_validates_artifact_names_and_hashes_independently() {
    for (name, invalid_hash, accepted) in [
        ("foo.rlib", false, true),
        ("../escape", false, false),
        ("foo.rlib", true, false),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let key = {
            let store = Store::open(&config).unwrap();
            put_entry(&store, dir.path(), 30, "foo", b"compiled artifact")
        };
        let meta_path = config.store_dir().join(&key).join("meta.json");
        let mut meta: EntryMeta = serde_json::from_slice(&fs::read(&meta_path).unwrap()).unwrap();
        meta.files[0].name = name.into();
        if invalid_hash {
            let original = blob_path_in_store_dir(&config.store_dir(), &meta.files[0].hash);
            meta.files[0].hash = "g".repeat(64);
            let malformed = blob_path_in_store_dir(&config.store_dir(), &meta.files[0].hash);
            fs::create_dir_all(malformed.parent().unwrap()).unwrap();
            // Keep the blob present and correctly sized: only validation
            // of its hash spelling may reject this entry.
            fs::copy(original, malformed).unwrap();
        }
        fs::write(meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();
        fs::remove_file(config.index_db_path()).unwrap();

        let store = Store::open(&config).unwrap();
        let stats = store.rebuild_index_from_store().unwrap();
        assert_eq!(
            (
                stats.entries_rebuilt,
                stats.entries_skipped,
                stats.blobs_registered
            ),
            if accepted { (1, 0, 1) } else { (0, 1, 0) },
            "name={name:?}, invalid_hash={invalid_hash}"
        );
        assert_eq!(store.contains(&key), accepted);
    }
}

#[test]
fn rebuild_index_ignores_the_blobs_dir_and_foreign_names() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    // Scoped so the connection is closed before index.db is deleted: Windows
    // refuses to remove a file another handle still has open.
    {
        let store = Store::open(&config).unwrap();
        put_entry(&store, dir.path(), 7, "delta", b"delta rlib content");
    }

    // Non-key directories under store/ must be left alone rather than
    // interpreted as entries: `blobs/` is the content-addressed store, and a
    // stray name is not ours (and would be an unvalidated path component).
    std::fs::create_dir_all(config.store_dir().join("not-a-cache-key")).unwrap();
    std::fs::create_dir_all(config.store_dir().join("0123456789")).unwrap();
    std::fs::remove_file(config.index_db_path()).unwrap();

    let store = Store::open(&config).unwrap();
    let stats = store.rebuild_index_from_store().unwrap();
    assert_eq!(
        stats.entries_rebuilt, 1,
        "only the real entry dir is adopted: {stats:?}"
    );
}

#[test]
fn store_open_rebuilds_automatically_after_quarantining_a_corrupt_index() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let k = {
        let store = Store::open(&config).unwrap();
        put_entry(&store, dir.path(), 8, "epsilon", b"epsilon rlib content")
    };

    // Corrupt the index the way #412 did, then just open the store: recovery
    // must both heal the DB *and* bring the cached entry back, rather than
    // silently presenting a cold cache while the artifacts sit on disk.
    std::fs::write(config.index_db_path(), b"not a sqlite database at all").unwrap();
    for ext in ["-wal", "-shm"] {
        let p = index_sidecar_path(&config.index_db_path(), ext);
        let _ = std::fs::remove_file(p);
    }

    let store = Store::open(&config).expect("corrupt index must self-heal");
    assert!(
        store.contains(&k),
        "the entry must be recovered by Store::open, not lost to an empty index"
    );
    assert_eq!(store.entry_count().unwrap(), 1);
}

#[test]
fn open_index_db_self_heals_a_corrupt_index() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("index.db");
    fs::write(
        &db_path,
        b"this is not a sqlite database; it is garbage bytes",
    )
    .unwrap();

    // The corrupt index must NOT brick the command: it is quarantined and a
    // fresh, usable index is recreated in place (#415).
    let db = open_index_db(&db_path).expect("a corrupt index must self-heal, not brick");
    let count: i64 = db
        .query_row("SELECT COUNT(*) FROM entries", [], |r| r.get(0))
        .expect("recreated index must be queryable");
    assert_eq!(count, 0, "the recreated index starts empty");

    assert!(db_path.is_file(), "a fresh index.db is recreated in place");
    let quarantined: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().contains(".corrupt-"))
        .collect();
    assert_eq!(
        quarantined.len(),
        1,
        "the corrupt index is quarantined (kept for forensics), not silently deleted"
    );
}

#[test]
fn quarantine_corrupt_index_moves_wal_and_shm_sidecars() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("index.db");
    fs::write(&db_path, b"corrupt").unwrap();
    fs::write(dir.path().join("index.db-wal"), b"wal").unwrap();
    fs::write(dir.path().join("index.db-shm"), b"shm").unwrap();

    let quarantined = quarantine_corrupt_index(&db_path).unwrap();
    assert!(quarantined.is_file());
    assert!(!db_path.exists(), "the corrupt db is moved aside");
    assert!(
        !dir.path().join("index.db-wal").exists(),
        "the -wal sidecar is moved aside"
    );
    assert!(
        !dir.path().join("index.db-shm").exists(),
        "the -shm sidecar is moved aside"
    );
    assert!(index_sidecar_path(&quarantined, "-wal").exists());
    assert!(index_sidecar_path(&quarantined, "-shm").exists());
}

#[test]
fn recover_corrupt_index_reuses_a_peer_healed_db_without_requarantine() {
    // Models the concurrency race: a peer already healed the index (the DB
    // at db_path is now a valid empty index). recover_corrupt_index must
    // re-check under the lock, find it healthy, and use it WITHOUT
    // quarantining a healthy DB (which would re-empty it and orphan blobs).
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("index.db");

    // A corruption-shaped error to pass in (as the original open would have).
    let garbage = dir.path().join("garbage.db");
    fs::write(&garbage, b"not a sqlite database").unwrap();
    let err = try_open_index_db(&garbage).unwrap_err();

    // The peer's freshly-healed, valid empty index now lives at db_path.
    drop(try_open_index_db(&db_path).unwrap());

    let (db, recovered) = recover_corrupt_index(&db_path, &err).unwrap();
    let count: i64 = db
        .query_row("SELECT COUNT(*) FROM entries", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0);
    assert!(
        !recovered,
        "adopting a peer's healed DB must not claim the rebuild: the peer that \
             quarantined it owns that, and two processes rebuilding at once would \
             double-count blob refcounts"
    );

    let quarantined = fs::read_dir(dir.path())
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().contains(".corrupt-"))
        .count();
    assert_eq!(
        quarantined, 0,
        "a healthy DB on re-check must not be quarantined"
    );
}

#[test]
fn test_store_open_creates_cache_root() {
    let dir = tempfile::tempdir().unwrap();
    let cache_dir = dir.path().join("nested").join("cache");
    let config = test_config(&cache_dir);

    let _store = Store::open(&config).unwrap();

    assert!(cache_dir.is_dir());
    assert!(config.store_dir().is_dir());
    assert!(config.index_db_path().is_file());
}

#[test]
fn test_store_eviction() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path());
    config.max_size = 100; // Very small limit to trigger eviction

    let store = Store::open(&config).unwrap();

    // Put a large-ish entry
    let output_file = dir.path().join("big.rlib");
    std::fs::write(&output_file, vec![0u8; 200]).unwrap();

    store
        .put(
            "key1",
            "big_crate",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(output_file.clone(), "libbig.rlib".to_string())],
            "",
            "",
        )
        .unwrap();
    store.remove_clone_for_test(&output_file);

    let recent = store.evict().unwrap();
    assert_eq!(recent.entries_recent_prefiltered, 1);
    assert_eq!(recent.entries_pinned, 1);
    assert_eq!(recent.entries_evicted, 0);

    // Age the entry past the active-pin grace so size-pressure eviction can
    // claim it (a just-put entry is "recently accessed" and is now pinned
    // against eviction for EVICTION_IDLE_GRACE — kunobi-ninja/kache#326).
    store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour') WHERE cache_key = 'key1'",
                [],
            )
            .unwrap();

    let stats = store.evict().unwrap();
    assert!(stats.entries_evicted > 0);
    assert!(!store.contains("key1"));
}

/// Put one 200-byte entry into a 100-byte store and age it past the pin
/// grace, so the next size eviction selects it.
fn store_with_one_evictable_entry(dir: &Path, key: &str) -> (Store, Config) {
    let mut config = test_config(dir);
    config.max_size = 100;
    let store = Store::open(&config).unwrap();
    let output_file = dir.join("big.rlib");
    std::fs::write(&output_file, vec![0u8; 200]).unwrap();
    store
        .put(
            key,
            "big_crate",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(output_file.clone(), "libbig.rlib".to_string())],
            "",
            "",
        )
        .unwrap();
    store.remove_clone_for_test(&output_file);
    store
        .db
        .execute(
            "UPDATE entries SET last_accessed = datetime('now', '-1 hour') WHERE cache_key = ?1",
            params![key],
        )
        .unwrap();
    (store, config)
}

#[test]
fn evict_counts_an_entry_it_fails_to_remove() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _config) = store_with_one_evictable_entry(dir.path(), "broken");
    std::fs::write(store.entry_dir("broken").join("meta.json"), b"{not json").unwrap();

    let stats = store.evict().unwrap();

    assert_eq!(stats.entries_evicted, 0);
    assert_eq!(
        stats.entries_failed, 1,
        "the refused removal is counted: {stats:?}"
    );
    assert_eq!(stats.entries_locked, 0, "bad data is not lock contention");
    assert_eq!(store.entry_count().unwrap(), 1);
}

#[test]
fn evict_counts_lock_contention_apart_from_bad_data() {
    let dir = tempfile::tempdir().unwrap();
    let (store, config) = store_with_one_evictable_entry(dir.path(), "busy");
    // A second writer holding the database for the whole sweep; the
    // removal waits out the busy timeout and fails. The timeout is cut
    // from 5 s so the test does not sit through it.
    store.db.busy_timeout(Duration::from_millis(50)).unwrap();
    let blocker = Connection::open(config.index_db_path()).unwrap();
    blocker.execute_batch("BEGIN EXCLUSIVE").unwrap();

    let stats = store.evict().unwrap();
    blocker.execute_batch("ROLLBACK").unwrap();

    assert_eq!(stats.entries_evicted, 0);
    assert_eq!(stats.entries_failed, 1, "{stats:?}");
    assert_eq!(
        stats.entries_locked, 1,
        "contention is counted as locked: {stats:?}"
    );
}

/// A build holding the index write lock when a sweep reaches an entry
/// delays that entry's removal; it must not cancel it. The removal used
/// to read before it wrote, and SQLite fails a read-to-write upgrade at
/// once instead of calling the busy handler, so the entry was skipped
/// and the auto-GC worker left the store over budget.
#[test]
fn evict_waits_for_a_competing_writer_and_still_evicts() {
    static REMOVAL_WAITED: std::sync::atomic::AtomicBool =
        std::sync::atomic::AtomicBool::new(false);
    // SQLite calls the busy handler only for a writer waiting to take
    // the lock. A removal that reads first and then upgrades fails at
    // once and never calls it.
    fn wait_for_lock(count: i32) -> bool {
        REMOVAL_WAITED.store(true, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(1));
        count < 5000
    }

    let dir = tempfile::tempdir().unwrap();
    let (store, config) = store_with_one_evictable_entry(dir.path(), "contended");
    store.db.busy_handler(Some(wait_for_lock)).unwrap();
    // The wrapper that spawned the auto-GC worker is still writing its
    // own durability flag when the worker starts evicting.
    let competitor = Connection::open(config.index_db_path()).unwrap();
    competitor.execute_batch("BEGIN IMMEDIATE").unwrap();
    competitor
        .execute("UPDATE entries SET durable = durable", [])
        .unwrap();
    // Commit once the removal is waiting for the lock. A fixed sleep let
    // a stalled test thread reach the removal after the commit, and the
    // test then passed without the removal ever waiting.
    let committer = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !REMOVAL_WAITED.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        competitor.execute_batch("COMMIT").unwrap();
    });

    let stats = store.evict().unwrap();
    committer.join().unwrap();

    assert!(
        REMOVAL_WAITED.load(Ordering::SeqCst),
        "the removal never waited for the lock: {stats:?}"
    );
    assert_eq!(stats.entries_locked, 0, "{stats:?}");
    assert_eq!(stats.entries_evicted, 1, "{stats:?}");
    assert!(!store.contains("contended"));
}

#[test]
fn is_sqlite_contention_matches_busy_and_locked_only() {
    let sqlite = |code: i32| {
        anyhow::Error::from(SqlError::SqliteFailure(
            rusqlite::ffi::Error::new(code),
            None,
        ))
        .context("removing entry")
    };
    assert!(is_sqlite_contention(&sqlite(rusqlite::ffi::SQLITE_BUSY)));
    assert!(is_sqlite_contention(&sqlite(rusqlite::ffi::SQLITE_LOCKED)));
    assert!(is_sqlite_busy_snapshot(&sqlite(
        rusqlite::ffi::SQLITE_BUSY_SNAPSHOT
    )));
    assert!(!is_sqlite_busy_snapshot(&sqlite(
        rusqlite::ffi::SQLITE_BUSY
    )));
    assert!(!is_sqlite_busy_snapshot(&sqlite(
        rusqlite::ffi::SQLITE_LOCKED
    )));
    assert!(!is_sqlite_contention(&sqlite(
        rusqlite::ffi::SQLITE_CORRUPT
    )));
    assert!(!is_sqlite_contention(&anyhow::anyhow!(
        "meta.json unparseable"
    )));

    let mut stats = GcStats::default();
    record_eviction_failure(&mut stats, &sqlite(rusqlite::ffi::SQLITE_BUSY_SNAPSHOT));
    record_eviction_failure(&mut stats, &sqlite(rusqlite::ffi::SQLITE_BUSY));
    record_eviction_failure(&mut stats, &anyhow::anyhow!("meta.json unparseable"));
    assert_eq!(stats.entries_failed, 3);
    assert_eq!(stats.entries_locked, 2);
    assert_eq!(stats.entries_busy_snapshot, 1);
}

/// Not `#[cfg(unix)]`: NTFS has hardlinks, `cache.windows_hardlink` and
/// `cache.shared_hardlink_restores` make them, so the #725 guard has to
/// hold there too. Gating this test to Unix is how the guard stayed
/// compiled out on Windows.
#[test]
fn evict_leaves_an_entry_whose_blob_is_still_hardlinked_outside() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path());
    config.max_size = 100;
    let mut store = Store::open(&config).unwrap();

    let output_file = dir.path().join("big.rlib");
    std::fs::write(&output_file, vec![0u8; 200]).unwrap();
    store
        .put(
            "kept",
            "big_crate",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(output_file.clone(), "libbig.rlib".to_string())],
            "",
            "",
        )
        .unwrap();
    store.remove_clone_for_test(&output_file);

    let meta = store.get("kept").unwrap().unwrap();
    let blob = store.blob_path(&meta.files[0].hash);
    let retainer = dir.path().join("worktree-copy.rlib");
    std::fs::hard_link(&blob, &retainer).unwrap();

    store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour') WHERE cache_key = 'kept'",
                [],
            )
            .unwrap();

    let stats = store.evict().unwrap();
    assert_eq!(stats.entries_evicted, 0, "clone still holds the blocks");
    assert_eq!(
        stats.entries_unreclaimable, 1,
        "the skip must be counted as unreclaimable, not as a pin: {stats:?}"
    );
    assert!(store.contains("kept"), "entry remains restorable");
    assert!(blob.is_file(), "store name remains");

    store.config.gc_evict_shared = true;
    let stats = store.evict().unwrap();
    assert_eq!(stats.entries_evicted, 1);
    assert_eq!(stats.bytes_freed, 200);
    assert_eq!(stats.disk_bytes_reclaimed, 0);
    assert!(!store.contains("kept"));
    assert!(
        retainer.is_file(),
        "compatibility mode drops the store name, not the retained blocks"
    );
}

#[test]
fn shared_entry_retention_requires_the_last_positive_reference() {
    assert!(holds_last_reference(1, 1));
    assert!(holds_last_reference(2, 2));
    assert!(holds_last_reference(1, 2));
    assert!(!holds_last_reference(2, 1));
    assert!(!holds_last_reference(0, 1));
    assert!(!holds_last_reference(-1, 1));
}

#[cfg(unix)]
#[test]
fn evict_leaves_an_entry_whose_blob_is_still_reflinked_outside() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path());
    config.max_size = 100;
    let store = Store::open(&config).unwrap();
    let output_file = dir.path().join("big.rlib");
    std::fs::write(&output_file, vec![0u8; 4096]).unwrap();
    store
        .put(
            "kept-reflink",
            "big_crate",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(output_file.clone(), "libbig.rlib".to_string())],
            "",
            "",
        )
        .unwrap();
    store.remove_clone_for_test(&output_file);

    let meta = store.get("kept-reflink").unwrap().unwrap();
    let blob = store.blob_path(&meta.files[0].hash);
    let retainer = dir.path().join("worktree-reflink.rlib");
    if crate::link::try_reflink(&blob, &retainer).is_err() {
        return;
    }
    let sharing = crate::sharing::probe(&blob, 4096);
    if !sharing.shared || sharing.private_bytes != 0 {
        return;
    }
    store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour') WHERE cache_key = 'kept-reflink'",
                [],
            )
            .unwrap();

    let stats = store.evict().unwrap();
    assert_eq!(stats.entries_evicted, 0);
    assert!(stats.entries_unreclaimable > 0);
    assert!(store.contains("kept-reflink"));

    store.remove_clone_for_test(&retainer);
    let stats = store.evict().unwrap();
    assert!(stats.entries_evicted > 0);
    assert!(!store.contains("kept-reflink"));
}

#[test]
fn durable_upload_intent_pins_payload_across_every_eviction_policy_until_retired() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path());
    config.max_size = 100;
    let store = Store::open(&config).unwrap();
    let pending_key = "a".repeat(64);
    let newer_twin_key = "b".repeat(64);
    let pending_output = dir.path().join("pending.rlib");
    let newer_output = dir.path().join("newer.rlib");
    fs::write(&pending_output, vec![0u8; 200]).unwrap();
    fs::write(&newer_output, vec![1u8; 200]).unwrap();

    store
        .put(
            &pending_key,
            "pending",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(pending_output.clone(), "libshared.rlib".to_string())],
            "",
            "",
        )
        .unwrap();
    store.set_last_accessed_for_test(&pending_key, "-48 hours");
    let duplicate_group = store
        .db
        .query_row(
            "SELECT content_hash FROM entries WHERE cache_key = ?1",
            params![pending_key.as_str()],
            |row| row.get::<_, String>(0),
        )
        .unwrap();
    store
        .put(
            &newer_twin_key,
            "newer",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(newer_output.clone(), "libshared.rlib".to_string())],
            "",
            "",
        )
        .unwrap();
    store.remove_clone_for_test(&pending_output);
    store.remove_clone_for_test(&newer_output);
    // Form a duplicate group while retaining distinct refcount-1 blobs.
    // Healthy identical entries have zero marginal reclaim and are
    // correctly excluded before the durable-upload pin is consulted.
    store
        .db
        .execute(
            "UPDATE entries SET content_hash = ?1 WHERE cache_key = ?2",
            params![duplicate_group, newer_twin_key.as_str()],
        )
        .unwrap();

    let spool_dir = config.upload_spool_dir();
    fs::create_dir_all(&spool_dir).unwrap();
    let intent = spool_dir.join(format!("{pending_key}.json"));
    // Protection is keyed by the durable filename, not JSON parsing. A
    // malformed intent must fail closed and keep its only upload payload.
    fs::write(&intent, b"{malformed").unwrap();

    let size = store.evict().unwrap();
    assert!(size.entries_pinned >= 1);
    assert!(store.contains(&pending_key));

    let age = store.evict_older_than(24).unwrap();
    assert_eq!(age.entries_pinned, 1);
    assert!(store.contains(&pending_key));

    let duplicate = store.evict_duplicate_entries().unwrap();
    assert_eq!(duplicate.entries_pinned, 1);
    assert!(store.contains(&pending_key));

    fs::remove_file(intent).unwrap();
    let retired = store.evict_duplicate_entries().unwrap();
    assert_eq!(retired.entries_evicted, 1);
    assert!(!store.contains(&pending_key));
    assert!(store.contains(&newer_twin_key));
}

#[test]
fn durable_upload_key_enumeration_is_bounded_and_fails_closed_on_read_error() {
    let key = "c".repeat(64);
    let names = [Ok::<_, std::io::Error>(std::ffi::OsString::from(format!(
        "{key}.json"
    )))];
    let keys = Store::durable_upload_keys_from_names(names, 1).unwrap();
    assert_eq!(keys, std::collections::HashSet::from([key]));

    let overflow = Store::durable_upload_keys_from_names(
        [
            Ok::<_, std::io::Error>(std::ffi::OsString::from("junk")),
            Ok::<_, std::io::Error>(std::ffi::OsString::from("more-junk")),
        ],
        1,
    )
    .unwrap_err();
    assert!(format!("{overflow:#}").contains("exceeds 1 jobs"));

    let unreadable = Store::durable_upload_keys_from_names(
        [Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "injected unreadable spool entry",
        ))],
        1,
    )
    .unwrap_err();
    assert!(format!("{unreadable:#}").contains("injected unreadable spool entry"));

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    assert!(
        store.durable_upload_keys().unwrap().is_empty(),
        "a missing spool directory is the one empty-set case"
    );

    fs::write(config.upload_spool_dir(), b"not a directory").unwrap();
    let blocked = store.durable_upload_keys().unwrap_err();
    assert!(
        format!("{blocked:#}").contains("reading"),
        "a non-directory spool path must fail closed: {blocked:#}"
    );
}

/// #594 step 2, end to end: evicting an entry records a tombstone with the
/// features the decision used, and a later lookup for that key marks it as
/// demanded. That demand is the observation a live-store snapshot can never
/// provide, because the entries it evicted are exactly the ones missing.
#[test]
fn eviction_records_a_tombstone_and_a_later_lookup_marks_demand() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path());
    config.max_size = 100; // force size pressure
    let store = Store::open(&config).unwrap();

    let out = dir.path().join("big.rlib");
    fs::write(&out, vec![0u8; 4096]).unwrap();
    store
        .put_with_compile_time(
            "doomed",
            "c",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(out.clone(), "libbig.rlib".to_string())],
            "",
            "",
            2500,
        )
        .unwrap();
    store.remove_clone_for_test(&out);
    // Age it past the active-pin grace so it is actually evictable.
    store
        .db
        .execute(
            "UPDATE entries SET last_accessed = datetime('now', '-2 hours')",
            [],
        )
        .unwrap();

    assert!(
        store.evict().unwrap().entries_evicted > 0,
        "expected eviction"
    );

    let (key, policy, cost, demanded): (String, String, i64, Option<String>) = store
        .db
        .query_row(
            "SELECT cache_key, policy, compile_time_ms, demanded_at FROM eviction_tombstones",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap();
    assert_eq!(key, "doomed");
    assert_eq!(policy, "size-pressure", "records which policy chose it");
    assert_eq!(cost, 2500, "records the rebuild cost that was destroyed");
    assert!(demanded.is_none(), "not demanded yet");
    assert_eq!(store.tombstone_stats().unwrap(), (1, 0));

    // The build asks for it again — exactly the case eviction got wrong.
    assert!(store.get("doomed").unwrap().is_none());
    assert_eq!(store.tombstone_stats().unwrap(), (1, 1));

    // A miss on a key that was never cached must not fabricate a record.
    assert!(store.get("never_existed").unwrap().is_none());
    assert_eq!(store.tombstone_stats().unwrap(), (1, 1));
}

/// Only the first demand is recorded — the question is how long after
/// eviction the key was wanted, so a later repeat must not overwrite it.
#[test]
fn tombstone_demand_records_only_the_first_request() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    store
        .db
        .execute(
            "INSERT INTO eviction_tombstones (cache_key, evicted_at, demanded_at)
                 VALUES ('k', datetime('now','-1 hour'), NULL)",
            [],
        )
        .unwrap();

    store.note_tombstone_demand("k");
    let first: String = store
        .db
        .query_row(
            "SELECT demanded_at FROM eviction_tombstones WHERE cache_key='k'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    store.note_tombstone_demand("k");
    let second: String = store
        .db
        .query_row(
            "SELECT demanded_at FROM eviction_tombstones WHERE cache_key='k'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(first, second, "first demand must not be overwritten");
}

/// The table is bounded: records age out, and a re-eviction of the same key
/// starts a fresh observation rather than colliding on the primary key.
#[test]
fn tombstones_are_pruned_by_age_and_re_eviction_resets_the_record() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    store
        .db
        .execute(
            "INSERT INTO eviction_tombstones (cache_key, evicted_at) VALUES
                   ('old', datetime('now','-30 days')),
                   ('recent', datetime('now','-1 day'))",
            [],
        )
        .unwrap();

    assert_eq!(store.prune_tombstones(14).unwrap(), 1);
    assert_eq!(store.tombstone_stats().unwrap().0, 1, "recent one survives");

    // Re-evicting a key already demanded must clear the demand so the new
    // observation window starts clean.
    store
        .db
        .execute(
            "UPDATE eviction_tombstones SET demanded_at = datetime('now') WHERE cache_key='recent'",
            [],
        )
        .unwrap();
    let features = crate::eviction::EntryFeatures {
        key: "recent".into(),
        size: 1,
        hit_count: 0,
        idle_hours: 5.0,
        content_hash: None,
        committed: true,
        compile_time_ms: 10,
        reclaimable_bytes: None,
        recently_accessed: false,
        recently_imported: false,
    };
    store.record_tombstone(&features, "size-pressure", Some(("value-density", false)));
    assert_eq!(
        store.tombstone_stats().unwrap(),
        (1, 0),
        "re-eviction restarts the observation"
    );
}

/// #594 step 1: rebuild cost must reach the index on write, and reach
/// eviction through `EntryFeatures` — the whole point is that a policy can
/// finally see what it is about to destroy.
#[test]
fn put_records_compile_time_and_eviction_can_see_it() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let out = dir.path().join("out.rlib");
    fs::write(&out, b"artifact").unwrap();
    store
        .put_with_compile_time(
            "costly",
            "c",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(out, "libout.rlib".to_string())],
            "",
            "",
            4321,
        )
        .unwrap();

    let indexed: i64 = store
        .db
        .query_row(
            "SELECT compile_time_ms FROM entries WHERE cache_key = 'costly'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(indexed, 4321, "put must index the rebuild cost");

    let features = store.eviction_candidates().unwrap();
    let entry = features.iter().find(|e| e.key == "costly").unwrap();
    assert_eq!(
        entry.compile_time_ms, 4321,
        "eviction must see rebuild cost (#594)"
    );
}

/// Entries written before the column existed sit at the `0` default; the
/// GC sweep backfills them from `meta.json`, which has always carried the
/// value. Converges: a backfilled row is never re-read.
#[test]
fn backfill_compile_times_recovers_pre_index_entries() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let out = dir.path().join("out.rlib");
    fs::write(&out, b"artifact").unwrap();
    store
        .put_with_compile_time(
            "legacy",
            "c",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(out, "libout.rlib".to_string())],
            "",
            "",
            7777,
        )
        .unwrap();
    // Simulate a row written before the column existed. meta.json still
    // has the real value — that is what makes recovery possible.
    store
        .db
        .execute("UPDATE entries SET compile_time_ms = 0", [])
        .unwrap();

    assert_eq!(store.backfill_compile_times().unwrap(), 1);
    let restored: i64 = store
        .db
        .query_row(
            "SELECT compile_time_ms FROM entries WHERE cache_key = 'legacy'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(restored, 7777);

    // Second pass finds nothing left to do.
    assert_eq!(store.backfill_compile_times().unwrap(), 0);
}

/// The backfill is bounded per sweep so a first GC after upgrade on a large
/// store cannot stall the daemon while it holds the store mutex; successive
/// sweeps converge.
#[test]
fn backfill_compile_times_is_bounded_per_sweep_and_converges() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    // Bound under test, not the production constant: the property is
    // "one sweep stops at the limit and the rest converges", which does
    // not depend on the constant's magnitude.
    const LIMIT: i64 = 3;
    let total = LIMIT + 2;
    for i in 0..total {
        let out = dir.path().join(format!("o{i}.rlib"));
        fs::write(&out, format!("artifact-{i}")).unwrap();
        store
            .put_with_compile_time(
                &format!("k{i}"),
                "c",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(out, format!("libo{i}.rlib"))],
                "",
                "",
                100,
            )
            .unwrap();
    }
    store
        .db
        .execute("UPDATE entries SET compile_time_ms = 0", [])
        .unwrap();

    let first = store.backfill_compile_times_limited(LIMIT).unwrap();
    assert_eq!(
        first, LIMIT as usize,
        "one sweep must not backfill the whole store"
    );
    let second = store.backfill_compile_times_limited(LIMIT).unwrap();
    assert_eq!(second, 2, "the remainder converges on the next sweep");
    assert_eq!(store.backfill_compile_times_limited(LIMIT).unwrap(), 0);
}

/// #595 equivalence guard: the Rust `SizePressurePolicy` ranking must match
/// the SQL `ORDER BY` it replaced, entry for entry. This is the property
/// that makes the refactor a no-op — if someone later changes the scoring
/// formula, this test is what tells them they changed behavior, not just
/// structure. Deliberately uses awkward inputs (zero size, zero idle,
/// equal scores) since those are where the SQL's MAX() clamps mattered.
#[test]
fn size_pressure_policy_matches_the_sql_ordering_it_replaced() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    // (key, size, hit_count, hours idle)
    let seed = [
        ("huge_stale", 600 * 1024 * 1024_i64, 0_i64, 15.0_f64),
        ("small_hot", 14 * 1024, 9, 0.1),
        ("mid", 5 * 1024 * 1024, 2, 48.0),
        ("zero_size", 0, 0, 3.0),
        ("just_touched", 1024 * 1024, 1, 0.0),
        ("ancient_tiny", 512, 0, 5000.0),
        ("twin_a", 2 * 1024 * 1024, 3, 12.0),
        ("twin_b", 2 * 1024 * 1024, 3, 12.0),
    ];
    for (key, size, hits, idle) in seed {
        store
                .db
                .execute(
                    "INSERT INTO entries (cache_key, crate_name, size, hit_count, committed, last_accessed)
                     VALUES (?1, 'c', ?2, ?3, 1, datetime('now', ?4))",
                    params![key, size, hits, format!("-{} seconds", (idle * 3600.0) as i64)],
                )
                .unwrap();
    }

    // The exact query this refactor removed from `evict()`.
    let sql_order: Vec<String> = {
        let mut stmt = store
            .db
            .prepare(
                "SELECT cache_key FROM entries
                     ORDER BY
                       CAST((hit_count + 1) AS REAL)
                       / (MAX((julianday('now') - julianday(last_accessed)) * 24.0, 0.01)
                          * MAX(size / 1048576.0, 0.001))
                       ASC",
            )
            .unwrap();
        stmt.query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };

    let candidates = store.eviction_candidates().unwrap();
    let policy_order = crate::eviction::SizePressurePolicy.select(&candidates);

    // Compare by score rather than raw position: SQLite and Rust may break
    // exact ties (twin_a/twin_b) in either order, and that is not a
    // behavior difference. Any genuine ranking divergence still fails.
    let score_of: std::collections::HashMap<&str, f64> = candidates
        .iter()
        .map(|e| (e.key.as_str(), crate::eviction::size_pressure_score(e)))
        .collect();
    let seq =
        |order: &[String]| -> Vec<f64> { order.iter().map(|k| score_of[k.as_str()]).collect() };
    assert_eq!(
        seq(&sql_order),
        seq(&policy_order),
        "policy ranking diverged from the SQL it replaced\n  sql:    {sql_order:?}\n  policy: {policy_order:?}"
    );
    assert_eq!(policy_order.len(), seed.len(), "every entry must be ranked");
}

/// Age must agree with its former SQL. Duplicate eviction additionally
/// requires proven marginal bytes, so legacy rows without `entry_blobs`
/// deliberately fail closed instead of matching the former SQL.
#[test]
fn older_than_matches_former_sql_while_duplicate_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    for (key, idle_h, hash) in [
        ("stale", 100.0_f64, Some("h1")),
        ("fresh", 1.0, Some("h1")),
        ("boundary", 24.0, None),
        ("lonely", 200.0, Some("h2")),
    ] {
        store
                .db
                .execute(
                    "INSERT INTO entries (cache_key, crate_name, size, committed, content_hash, last_accessed)
                     VALUES (?1, 'c', 100, 1, ?2, datetime('now', ?3))",
                    params![key, hash, format!("-{} seconds", (idle_h * 3600.0) as i64)],
                )
                .unwrap();
    }

    let candidates = store.eviction_candidates().unwrap();

    let sql_old: Vec<String> = {
        let mut stmt = store
            .db
            .prepare(
                "SELECT cache_key FROM entries WHERE last_accessed < datetime('now', '-24 hours')",
            )
            .unwrap();
        stmt.query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    let mut policy_old = crate::eviction::OlderThanPolicy { hours: 24 }.select(&candidates);
    policy_old.sort();

    // Away from the cutoff the two agree exactly. Do not assert boundary
    // membership here: insertion and selection evaluate separate
    // `datetime('now')` calls, so a second rollover can move `boundary`
    // across the old SQL cutoff. Strict cutoff behavior is covered
    // deterministically by `OlderThanPolicy`'s pure unit test.
    let unambiguous = |v: &[String]| -> Vec<String> {
        let mut v: Vec<String> = v.iter().filter(|k| *k != "boundary").cloned().collect();
        v.sort();
        v
    };
    assert_eq!(
        unambiguous(&policy_old),
        unambiguous(&sql_old),
        "older-than selection diverged away from the cutoff boundary"
    );

    let sql_dup: Vec<String> = {
        let mut stmt = store
            .db
            .prepare(
                "SELECT e.cache_key FROM entries e
                     JOIN (SELECT content_hash, MAX(last_accessed) AS newest
                           FROM entries WHERE content_hash IS NOT NULL AND committed = 1
                           GROUP BY content_hash HAVING COUNT(*) > 1) d
                       ON e.content_hash = d.content_hash
                     WHERE e.last_accessed < d.newest AND e.committed = 1",
            )
            .unwrap();
        stmt.query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    let mut policy_dup = crate::eviction::DuplicatePolicy.select(&candidates);
    let mut sql_dup_sorted = sql_dup.clone();
    policy_dup.sort();
    sql_dup_sorted.sort();
    assert_eq!(
        sql_dup_sorted,
        vec!["stale"],
        "former SQL selected the older twin without proving reclaimed bytes"
    );
    assert!(
        policy_dup.is_empty(),
        "unmapped legacy victims must fail closed on unknown marginal bytes"
    );
}

/// kunobi-ninja/kache#326, #182: size-pressure eviction must NOT delete an
/// entry a live build just accessed (it may be mid-restore — the active-pin
/// guard keys off `last_accessed`, which `get` bumps before the wrapper
/// hardlinks the blobs). A recently-accessed entry survives; aging it past
/// the grace window lets it be evicted.
#[test]
fn evict_skips_recently_accessed_entry() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path());
    config.max_size = 100; // tiny limit → over capacity → wants to evict

    let store = Store::open(&config).unwrap();
    let output_file = dir.path().join("big.rlib");
    std::fs::write(&output_file, vec![0u8; 200]).unwrap();
    store
        .put(
            "live_key",
            "live_crate",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(output_file.clone(), "libbig.rlib".to_string())],
            "",
            "",
        )
        .unwrap();
    store.remove_clone_for_test(&output_file);

    // Fresh put → last_accessed = now → within the grace window → pinned.
    let stats = store.evict().unwrap();
    assert_eq!(
        stats.entries_evicted, 0,
        "a recently-accessed entry must be pinned against eviction"
    );
    assert_eq!(
        stats.entries_pinned, 1,
        "and it must be COUNTED as held back — that count is the whole \
             difference between `evicted 0 entries` reading as a broken GC and \
             explaining itself (#509)"
    );
    assert!(store.contains("live_key"));

    // Age it past the grace window → no longer pinned → evictable.
    store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour') WHERE cache_key = 'live_key'",
                [],
            )
            .unwrap();
    let stats = store.evict().unwrap();
    assert!(stats.entries_evicted > 0);
    assert!(!store.contains("live_key"));
}

/// Index `bytes` of artifact as if the remote had just delivered `key`.
fn import_test_entry(store: &Store, key: &str, bytes: usize) {
    let entry_dir = store.entry_dir(key);
    std::fs::create_dir_all(&entry_dir).unwrap();
    let content = vec![b'i'; bytes];
    std::fs::write(entry_dir.join("lib.rlib"), &content).unwrap();
    let meta = EntryMeta {
        cache_key: key.to_string(),
        key_schema: kache_format::CACHE_KEY_VERSION,
        crate_name: format!("{key}_crate"),
        crate_types: vec!["lib".to_string()],
        files: vec![CachedFile {
            name: "lib.rlib".to_string(),
            size: bytes as u64,
            hash: blake3::hash(&content).to_hex().to_string(),
            executable: false,
        }],
        stdout: String::new(),
        stderr: String::new(),
        features: vec![],
        target: "x86_64-unknown-linux-gnu".to_string(),
        profile: "dev".to_string(),
        compile_time_ms: 0,
        emit_kinds: Vec::new(),
    };
    std::fs::write(
        entry_dir.join("meta.json"),
        serde_json::to_vec_pretty(&meta).unwrap(),
    )
    .unwrap();
    store.import_downloaded_entry(key).unwrap();
}

/// Put `bytes` of artifact under `key`, as a local compile would.
fn put_test_entry(store: &Store, dir: &Path, key: &str, bytes: usize) {
    let output = dir.join(format!("{key}.rlib"));
    std::fs::write(&output, vec![b'b'; bytes]).unwrap();
    store
        .put(
            key,
            &format!("{key}_crate"),
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(output.clone(), format!("lib{key}.rlib"))],
            "",
            "",
        )
        .unwrap();
    store.remove_clone_for_test(&output);
}

fn set_idle_past_grace(store: &Store) {
    store
        .db
        .execute(
            "UPDATE entries SET last_accessed = datetime('now', '-1 hour')",
            [],
        )
        .unwrap();
}

/// kunobi-ninja/kache#1008: a CI job imports a warm set, builds, uploads a
/// miss, and the upload's sweep used to evict whatever the job had not
/// touched for two minutes. An automatic sweep now keeps the import and
/// evicts what this machine built instead; a sweep the user asked for
/// still evicts both.
#[test]
fn an_automatic_sweep_keeps_what_the_remote_just_delivered() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path());
    config.max_size = 100;
    let store = Store::open(&config).unwrap();
    import_test_entry(&store, "imported_key", 300);
    put_test_entry(&store, dir.path(), "built_key", 300);
    set_idle_past_grace(&store);

    let automatic = store.evict_for(SweepOrigin::Automatic).unwrap();
    assert!(store.contains("imported_key"), "the job's import survives");
    assert!(!store.contains("built_key"), "the sweep still frees space");
    assert_eq!(automatic.entries_import_pinned, 1);
    assert_eq!(automatic.entries_pinned, 1, "counted as held back too");

    let requested = store.evict().unwrap();
    assert!(!store.contains("imported_key"), "`kache gc` can reclaim it");
    assert_eq!(requested.entries_import_pinned, 0);
}

/// The pin runs out: a long-lived daemon must not keep an import nobody
/// used past [`IMPORT_PIN`].
#[test]
fn an_import_is_kept_only_within_the_import_pin() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path());
    config.max_size = 100;
    let store = Store::open(&config).unwrap();
    import_test_entry(&store, "inside_key", 300);
    import_test_entry(&store, "outside_key", 300);
    set_idle_past_grace(&store);
    // Six hours, as documented: the longest GitHub-hosted job.
    assert_eq!(IMPORT_PIN.as_secs(), 21_600);
    let pin = IMPORT_PIN.as_secs() as i64;
    let age = |key: &str, secs: i64| {
        store
            .db
            .execute(
                "UPDATE entries SET imported_at = unixepoch() - ?1 WHERE cache_key = ?2",
                params![secs, key],
            )
            .unwrap();
    };
    age("inside_key", pin - 60);
    age("outside_key", pin + 60);

    let candidates = store
        .eviction_candidates_for(SweepOrigin::Automatic)
        .unwrap();
    let imported = |key: &str| {
        candidates
            .iter()
            .find(|entry| entry.key == key)
            .unwrap()
            .recently_imported
    };
    assert!(imported("inside_key"));
    assert!(!imported("outside_key"));

    let stats = store.evict_for(SweepOrigin::Automatic).unwrap();
    assert!(store.contains("inside_key"));
    assert!(!store.contains("outside_key"));
    assert_eq!(stats.entries_import_pinned, 1);
}

/// Only the remote's entries carry the stamp, and only an automatic
/// sweep reads it.
#[test]
fn only_imports_are_stamped_and_only_automatic_sweeps_keep_them() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    import_test_entry(&store, "imported_key", 10);
    put_test_entry(&store, dir.path(), "built_key", 10);
    let stamped = |key: &str| -> bool {
        store
            .db
            .query_row(
                "SELECT imported_at IS NOT NULL FROM entries WHERE cache_key = ?1",
                params![key],
                |row| row.get(0),
            )
            .unwrap()
    };
    assert!(stamped("imported_key"));
    assert!(!stamped("built_key"));

    let flags = |origin| -> Vec<(String, bool)> {
        let mut flags: Vec<_> = store
            .eviction_candidates_for(origin)
            .unwrap()
            .into_iter()
            .map(|entry| (entry.key, entry.recently_imported))
            .collect();
        flags.sort();
        flags
    };
    assert_eq!(
        flags(SweepOrigin::Automatic),
        vec![
            ("built_key".to_string(), false),
            ("imported_key".to_string(), true)
        ]
    );
    assert!(
        flags(SweepOrigin::Requested)
            .iter()
            .all(|(_, imported)| !imported)
    );
}

/// kunobi-ninja/kache#326: the recency guard is eviction-only. Explicit
/// `remove_entry` (purge / `doctor`) must remove a just-accessed entry.
#[test]
fn remove_entry_ignores_recency_guard() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let output_file = dir.path().join("x.rlib");
    std::fs::write(&output_file, b"content").unwrap();
    store
        .put(
            "rk",
            "c",
            &["lib".to_string()],
            &[],
            "",
            "dev",
            &[(output_file, "libx.rlib".to_string())],
            "",
            "",
        )
        .unwrap();

    // Just put → recent. The guarded path skips it…
    assert!(
        matches!(
            store
                .remove_entry_guarded("rk", Some(EVICTION_IDLE_GRACE))
                .unwrap(),
            GuardedRemoval::Skipped
        ),
        "guarded removal must skip a recently-accessed entry"
    );
    assert!(store.contains("rk"));

    // …but the unguarded public path removes it regardless of recency.
    store.remove_entry("rk").unwrap();
    assert!(!store.contains("rk"));
}

#[test]
fn test_incremental_dir_registry_deduplicates_and_cleans() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let incremental_dir = dir.path().join("target/debug/incremental");
    std::fs::create_dir_all(&incremental_dir).unwrap();
    std::fs::write(incremental_dir.join("junk"), b"tmp").unwrap();

    store.remember_incremental_dir(&incremental_dir).unwrap();
    store.remember_incremental_dir(&incremental_dir).unwrap();
    store
        .remember_incremental_dir(&dir.path().join("missing/incremental"))
        .unwrap();

    let count_before: i64 = store
        .db
        .query_row("SELECT COUNT(*) FROM incremental_dirs", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(count_before, 2);

    let cleaned = store.clean_registered_incremental_dirs().unwrap();
    assert_eq!(cleaned, 1);
    assert!(!incremental_dir.exists());

    let count_after: i64 = store
        .db
        .query_row("SELECT COUNT(*) FROM incremental_dirs", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(count_after, 0);
}

#[test]
fn target_root_registry_is_local_bounded_provenance_with_identity() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let workspace = dir.path().join("workspace");
    let target = workspace.join("target");
    std::fs::create_dir_all(&target).unwrap();
    std::fs::write(
        target.join("CACHEDIR.TAG"),
        "Signature: 8a477f597d28d172789f06886806bc55",
    )
    .unwrap();
    std::fs::write(target.join(".rustc_info.json"), "{}").unwrap();

    store.remember_target_root(&target, &workspace).unwrap();
    store.remember_target_root(&target, &workspace).unwrap();
    let roots = store.tracked_target_roots(0).unwrap();
    assert_eq!(roots.len(), 1, "same target is upserted, not duplicated");
    assert_eq!(roots[0].path, std::path::absolute(&target).unwrap());
    assert_eq!(
        roots[0].workspace_root,
        std::path::absolute(&workspace).unwrap()
    );
    assert_eq!(
        crate::filesystem::directory_identity(&target),
        Some(roots[0].identity)
    );

    store.remember_target_root(&workspace, &workspace).unwrap();
    assert_eq!(
        store.tracked_target_roots(0).unwrap().len(),
        1,
        "a source root must never be registered as a cleanup target"
    );

    store.forget_target_root(&target).unwrap();
    assert!(store.tracked_target_roots(0).unwrap().is_empty());
}

#[test]
fn target_root_registry_filters_by_last_seen() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let workspace = dir.path().join("workspace");
    let target = workspace.join("target");
    std::fs::create_dir_all(&target).unwrap();
    std::fs::write(
        target.join("CACHEDIR.TAG"),
        "Signature: 8a477f597d28d172789f06886806bc55",
    )
    .unwrap();
    std::fs::write(target.join(".rustc_info.json"), "{}").unwrap();
    store.remember_target_root(&target, &workspace).unwrap();

    assert!(store.tracked_target_roots(24).unwrap().is_empty());
    store
        .db
        .execute(
            "UPDATE target_roots SET last_seen = unixepoch() - 90000",
            [],
        )
        .unwrap();
    assert_eq!(store.tracked_target_roots(24).unwrap().len(), 1);
}

#[test]
fn target_registry_prunes_only_after_a_real_upsert() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let workspace = dir.path().join("workspace");
    let target = workspace.join("target");
    std::fs::create_dir_all(&target).unwrap();
    std::fs::write(
        target.join("CACHEDIR.TAG"),
        "Signature: 8a477f597d28d172789f06886806bc55",
    )
    .unwrap();
    std::fs::write(target.join(".rustc_info.json"), "{}").unwrap();

    let insert_stale = |path: &str| {
        store
            .db
            .execute(
                "INSERT INTO target_roots
                     (path, workspace_root, first_seen, last_seen, device, inode)
                     VALUES (?1, '/workspace', 0, unixepoch() - 15552001, '1', '1')",
                params![path],
            )
            .unwrap();
    };
    let contains = |path: &str| -> bool {
        store
            .db
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM target_roots WHERE path = ?1)",
                params![path],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
            != 0
    };

    insert_stale("/stale-before-write");
    store.remember_target_root(&target, &workspace).unwrap();
    assert!(!contains("/stale-before-write"));

    insert_stale("/stale-before-debounced-noop");
    store.remember_target_root(&target, &workspace).unwrap();
    assert!(contains("/stale-before-debounced-noop"));
}

#[test]
fn clean_registered_incremental_dirs_prunes_a_non_directory_path() {
    // A registered incremental path that now points at a *file* (not a dir)
    // is pruned without being counted as cleaned. Covers the
    // `!path.is_dir()` branch of clean_registered_incremental_dirs.
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let bogus = dir.path().join("not-a-dir");
    std::fs::write(&bogus, b"i am a file").unwrap();
    store.remember_incremental_dir(&bogus).unwrap();

    let cleaned = store.clean_registered_incremental_dirs().unwrap();
    assert_eq!(cleaned, 0, "a non-directory is pruned, not cleaned");
    // The file is left in place (we only remove directories), but its row is gone.
    assert!(bogus.exists(), "the non-directory file is not deleted");
    let remaining: i64 = store
        .db
        .query_row("SELECT COUNT(*) FROM incremental_dirs", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(remaining, 0, "the bogus registration was pruned");
}

#[cfg(unix)]
#[test]
fn clean_registered_incremental_dirs_keeps_row_when_remove_fails() {
    // Covers clean_registered_incremental_dirs remove_dir_all error branch.
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let parent = dir.path().join("readonly-parent");
    let incremental_dir = parent.join("incremental");
    std::fs::create_dir_all(&incremental_dir).unwrap();
    std::fs::write(incremental_dir.join("junk"), b"tmp").unwrap();
    store.remember_incremental_dir(&incremental_dir).unwrap();

    std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o500)).unwrap();
    let cleaned = store.clean_registered_incremental_dirs().unwrap();
    std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o700)).unwrap();

    assert_eq!(cleaned, 0, "failed removals are not counted as cleaned");
    assert!(incremental_dir.exists(), "failed removal leaves the dir");
    let remaining: i64 = store
        .db
        .query_row("SELECT COUNT(*) FROM incremental_dirs", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(remaining, 1, "failed removal keeps the registry row");
}

#[test]
fn test_store_locking() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let lock1 = match store.claim_build("testkey").unwrap() {
        BuildClaim::Acquired(lock) => lock,
        BuildClaim::Committed(_) | BuildClaim::Contended => {
            panic!("first build claim should acquire the key")
        }
    };

    assert!(matches!(
        store.claim_build("testkey").unwrap(),
        BuildClaim::Contended
    ));

    drop(lock1);

    assert!(matches!(
        store.claim_build("testkey").unwrap(),
        BuildClaim::Acquired(_)
    ));
}

#[test]
fn claim_build_rechecks_entry_after_acquiring_lock() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let peer = Store::open(&config).unwrap();
    let waiter = Store::open(&config).unwrap();
    let cache_key = "committed_during_claim_race";

    let peer_lock = match peer.claim_build(cache_key).unwrap() {
        BuildClaim::Acquired(lock) => lock,
        BuildClaim::Committed(_) | BuildClaim::Contended => {
            panic!("peer should acquire the initial build claim")
        }
    };
    assert!(matches!(
        waiter.claim_build(cache_key).unwrap(),
        BuildClaim::Contended
    ));

    let output = dir.path().join("lib.rlib");
    fs::write(&output, b"peer output").unwrap();
    peer.put(
        cache_key,
        "peer",
        &["rlib".to_string()],
        &[],
        "host",
        "dev",
        &[(output, "lib.rlib".to_string())],
        "",
        "",
    )
    .unwrap();
    drop(peer_lock);

    match waiter.claim_build(cache_key).unwrap() {
        BuildClaim::Committed(meta) => assert_eq!(meta.cache_key, cache_key),
        BuildClaim::Acquired(_) => panic!("committed entry must prevent a duplicate compile"),
        BuildClaim::Contended => panic!("peer already released the build lock"),
    }
    assert!(
        waiter.try_lock(cache_key).unwrap().is_some(),
        "serving the committed entry must release the claim"
    );
}

#[test]
fn claim_build_evicts_empty_committed_entry() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let cache_key = "empty_committed_entry";
    let entry_dir = store.entry_dir(cache_key);
    fs::create_dir_all(&entry_dir).unwrap();
    let meta = EntryMeta {
        cache_key: cache_key.to_string(),
        key_schema: kache_format::CACHE_KEY_VERSION,
        crate_name: "empty".to_string(),
        crate_types: vec!["rlib".to_string()],
        files: vec![],
        stdout: String::new(),
        stderr: String::new(),
        features: vec![],
        target: "host".to_string(),
        profile: "dev".to_string(),
        compile_time_ms: 0,
        emit_kinds: Vec::new(),
    };
    fs::write(
        entry_dir.join("meta.json"),
        serde_json::to_string_pretty(&meta).unwrap(),
    )
    .unwrap();
    store
        .db
        .execute(
            "INSERT INTO entries (cache_key, crate_name, size, committed) VALUES (?1, ?2, 0, 1)",
            params![cache_key, "empty"],
        )
        .unwrap();

    match store.claim_build(cache_key).unwrap() {
        BuildClaim::Acquired(_) => {}
        BuildClaim::Committed(_) => panic!("empty entry must not be served"),
        BuildClaim::Contended => panic!("no peer owns the build lock"),
    }
    assert!(store.get(cache_key).unwrap().is_none());
    assert!(!entry_dir.exists());
    assert!(store.try_lock(cache_key).unwrap().is_some());
}

#[test]
fn unlocked_pid_marker_does_not_claim_the_key() {
    // Regression for #821: PID contents are diagnostic only. A PID from a
    // different namespace must not decide whether the key is available.
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let lock_path = store.entry_dir("pid-marker").with_extension("lock");
    fs::write(&lock_path, std::process::id().to_string()).unwrap();

    let lock = store.try_lock("pid-marker").unwrap();

    assert!(
        lock.is_some(),
        "an unlocked marker must not cause contention"
    );
}

#[test]
fn advisory_lock_owns_key_even_with_unparseable_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let lock_path = store.entry_dir("foreign-owner").with_extension("lock");
    let mut owner = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .unwrap();
    use std::io::Write;
    write!(owner, "not-a-pid").unwrap();
    owner.lock().unwrap();

    assert!(
        store.try_lock("foreign-owner").unwrap().is_none(),
        "OS lock ownership must override PID metadata"
    );
    owner.unlock().unwrap();
    assert!(store.try_lock("foreign-owner").unwrap().is_some());
}

#[test]
fn concurrent_advisory_lock_acquisition_has_one_winner() {
    const CONTENDERS: usize = 16;
    let dir = tempfile::tempdir().unwrap();

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(CONTENDERS));
    let mut handles = Vec::new();
    for _ in 0..CONTENDERS {
        let config = test_config(dir.path());
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            let store = Store::open(&config).unwrap();
            barrier.wait();
            store.try_lock("stale-race").unwrap()
        }));
    }

    let guards: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();
    assert_eq!(
        guards.iter().filter(|guard| guard.is_some()).count(),
        1,
        "the advisory lock must admit exactly one live guard"
    );
}

#[test]
fn dropping_key_lock_preserves_stable_lock_file() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let lock_path = store.entry_dir("stable-path").with_extension("lock");
    let lock = store.try_lock("stable-path").unwrap();

    assert!(lock.is_some());
    drop(lock);
    assert!(
        lock_path.exists(),
        "advisory lock paths must not be unlinked after release"
    );
    assert!(store.try_lock("stable-path").unwrap().is_some());
}

#[test]
fn dropping_store_lock_unlocks_even_with_a_duplicated_handle() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let lock = store
        .try_lock("duplicated-handle")
        .unwrap()
        .expect("owner lock");
    let duplicate = lock.file.try_clone().unwrap();

    drop(lock);

    assert!(
        store.try_lock("duplicated-handle").unwrap().is_some(),
        "explicit unlock must release duplicate descriptors of the lock"
    );
    drop(duplicate);
}

#[test]
fn process_exit_releases_advisory_key_lock() {
    let dir = tempfile::tempdir().unwrap();
    let ready = dir.path().join("lock-ready");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "store::tests::advisory_key_lock_child_fixture",
            "--ignored",
            "--nocapture",
        ])
        .env("KACHE_TEST_ADVISORY_LOCK_ROOT", dir.path())
        .spawn()
        .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !ready.exists() && std::time::Instant::now() < deadline {
        assert!(
            child.try_wait().unwrap().is_none(),
            "lock fixture exited before acquiring the key"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(ready.exists(), "lock fixture did not become ready");

    let store = Store::open(test_config(dir.path())).unwrap();
    assert!(
        store.try_lock("crash-release").unwrap().is_none(),
        "child must own the key before it exits"
    );
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(
        store.try_lock("crash-release").unwrap().is_some(),
        "the OS must release the key lock when its process exits"
    );
}

#[test]
#[ignore = "subprocess fixture for process_exit_releases_advisory_key_lock"]
fn advisory_key_lock_child_fixture() {
    let root =
        PathBuf::from(std::env::var_os("KACHE_TEST_ADVISORY_LOCK_ROOT").expect("fixture root"));
    let store = Store::open(test_config(&root)).unwrap();
    let _lock = store
        .try_lock("crash-release")
        .unwrap()
        .expect("fixture key lock");
    fs::write(root.join("lock-ready"), b"ready").unwrap();
    std::thread::sleep(Duration::from_secs(30));
}

#[test]
fn test_store_clear() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output_file = dir.path().join("out.rlib");
    std::fs::write(&output_file, b"content").unwrap();

    store
        .put(
            "k1",
            "c1",
            &["lib".to_string()],
            &[],
            "",
            "dev",
            &[(output_file.clone(), "lib.rlib".to_string())],
            "",
            "",
        )
        .unwrap();

    assert!(store.contains("k1"));
    store.clear().unwrap();
    assert!(!store.contains("k1"));
}

#[test]
fn test_store_entry_dir() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let entry_dir = store.entry_dir("abc123");
    assert!(entry_dir.to_string_lossy().contains("store"));
    assert!(entry_dir.to_string_lossy().contains("abc123"));
}

#[test]
fn test_store_total_size_empty() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    assert_eq!(store.total_size().unwrap(), 0);
}

#[test]
fn test_store_entry_count_empty() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    assert_eq!(store.entry_count().unwrap(), 0);
}

#[test]
fn test_store_entry_count_after_put() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output = dir.path().join("a.rlib");
    std::fs::write(&output, b"data").unwrap();
    store
        .put(
            "k1",
            "c1",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output.clone(), "a.rlib".into())],
            "",
            "",
        )
        .unwrap();

    rewrite_source(&output, b"data2");
    store
        .put(
            "k2",
            "c2",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output, "b.rlib".into())],
            "",
            "",
        )
        .unwrap();

    assert_eq!(store.entry_count().unwrap(), 2);
}

#[test]
fn test_store_contains_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    assert!(!store.contains("nonexistent_key"));
}

#[test]
fn test_store_get_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    assert!(store.get("nonexistent_key").unwrap().is_none());
}

#[test]
fn test_store_remove_entry() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output = dir.path().join("lib.rlib");
    std::fs::write(&output, b"content").unwrap();
    store
        .put(
            "rem1",
            "c1",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output, "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();
    assert!(store.contains("rem1"));

    store.remove_entry("rem1").unwrap();
    assert!(!store.contains("rem1"));
    assert_eq!(store.entry_count().unwrap(), 0);
}

/// #276: removing an entry whose meta.json is unparseable must NOT delete
/// the entry row or silently drop blob refcounts — that orphans the blobs
/// forever (they keep a DB row and evade size-based eviction). It must
/// refuse, leaving the entry and its refcounts intact.
#[test]
fn remove_entry_refuses_on_corrupt_meta_no_refcount_leak() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output = dir.path().join("lib.rlib");
    std::fs::write(&output, b"content").unwrap();
    store
        .put(
            "corrupt1",
            "c1",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output, "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();

    let refcount_sum = |s: &Store| -> i64 {
        s.db.query_row("SELECT COALESCE(SUM(refcount), 0) FROM blobs", [], |r| {
            r.get(0)
        })
        .unwrap()
    };
    let row_present = |s: &Store| -> i64 {
        s.db.query_row(
            "SELECT COUNT(*) FROM entries WHERE cache_key = 'corrupt1'",
            [],
            |r| r.get(0),
        )
        .unwrap()
    };
    assert_eq!(refcount_sum(&store), 1, "one blob at refcount 1 after put");
    assert_eq!(row_present(&store), 1);

    // Corrupt the entry's meta.json so its blob list can't be loaded.
    let meta_path = store.entry_dir("corrupt1").join("meta.json");
    std::fs::write(&meta_path, b"{ not valid json").unwrap();

    assert!(
        store.remove_entry("corrupt1").is_err(),
        "remove_entry must error on unparseable meta.json rather than leak"
    );
    assert_eq!(
        row_present(&store),
        1,
        "corrupt entry row must survive a refused removal"
    );
    assert_eq!(
        refcount_sum(&store),
        1,
        "blob refcounts must be unchanged — no orphan"
    );
}

/// #276: a missing meta.json while the DB row persists is the same hazard.
#[test]
fn remove_entry_refuses_when_meta_missing_but_row_present() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let output = dir.path().join("lib.rlib");
    std::fs::write(&output, b"x").unwrap();
    store
        .put(
            "m1",
            "c1",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output, "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();
    std::fs::remove_file(store.entry_dir("m1").join("meta.json")).unwrap();
    let err = store.remove_entry("m1").unwrap_err();
    // The message identifies WHICH state was diagnosed: the settled
    // missing-meta shape, not the transient-recheck refusal — a removal
    // that misclassifies the settled state would route corrupt entries
    // through the wrong recovery advice.
    assert!(
        format!("{err:#}").contains("meta.json missing but DB row present"),
        "wrong refusal shape: {err:#}"
    );
    let still_there: i64 = store
        .db
        .query_row(
            "SELECT COUNT(*) FROM entries WHERE cache_key = 'm1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(still_there, 1, "entry row must survive a refused removal");
}

/// The resolution path the #276 comments promise: a refused removal
/// leaves the row "until a fresh put (INSERT OR REPLACE) overwrites
/// it". That re-put must also release the stranded generation's blob
/// references — stacking new increments on top leaks the old hashes
/// (a refcount no mapping accounts for never reaches zero) and
/// double-counts hashes shared by both generations.
#[test]
fn reput_over_refused_removal_releases_stale_refcounts() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let refcount_sum = |s: &Store| -> i64 {
        s.db.query_row("SELECT COALESCE(SUM(refcount), 0) FROM blobs", [], |r| {
            r.get(0)
        })
        .unwrap()
    };
    // Each generation compiles to a fresh source path: put ingests by
    // hardlink where reflinks are unavailable (ext4), sharing the
    // store blob's read-only inode with the source, so rewriting one
    // path across generations would EACCES on Linux while APFS
    // reflinks mask it (the #822 snapshot-test lesson).
    let put = |s: &Store, generation: &str, content: &[u8]| {
        let output = dir.path().join(format!("lib-{generation}.rlib"));
        std::fs::write(&output, content).unwrap();
        s.put(
            "strand1",
            "c1",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output, "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();
    };

    put(&store, "one", b"generation one");
    assert_eq!(refcount_sum(&store), 1);

    // Strand the row: meta.json gone, DB row present. Removal
    // refuses (#276), and lookup's discarded-error path then drives
    // a miss, a recompile, and this re-put over the surviving row.
    std::fs::remove_file(store.entry_dir("strand1").join("meta.json")).unwrap();
    assert!(store.remove_entry("strand1").is_err());
    put(&store, "two", b"generation two");

    assert_eq!(
        refcount_sum(&store),
        1,
        "re-put must release the stranded generation's references"
    );
    let mapped: i64 = store
        .db
        .query_row(
            "SELECT COUNT(*) FROM entry_blobs WHERE cache_key = 'strand1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(mapped, 1, "exactly the new generation's mapping remains");
    assert_eq!(
        store.blob_index_drift().unwrap().total(),
        0,
        "index must match the committed meta with no doctor repair"
    );

    // Same-content re-put: the shared hash must stay at one
    // reference, not accumulate one per generation.
    put(&store, "two-again", b"generation two");
    assert_eq!(refcount_sum(&store), 1, "shared hash must not double-count");
    assert_eq!(store.blob_index_drift().unwrap().total(), 0);
}

/// Every blob's refcount equals the references its mappings hold, no
/// blob row exists without a mapping, and no mapping names a blob that
/// has no row. Pure SQL, so it also holds a store whose meta.json
/// files are unreadable to account.
fn assert_blob_refs_match_mappings(store: &Store) {
    let mut stmt = store
        .db
        .prepare(
            "SELECT b.hash, b.refcount,
                        COALESCE((SELECT SUM(refs) FROM entry_blobs e WHERE e.hash = b.hash), 0)
                 FROM blobs b
                 UNION ALL
                 SELECT e.hash, 0, e.refs FROM entry_blobs e
                 WHERE NOT EXISTS (SELECT 1 FROM blobs b WHERE b.hash = e.hash)",
        )
        .unwrap();
    let drifted: Vec<String> = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })
        .unwrap()
        .map(Result::unwrap)
        .filter(|(_, refcount, mapped)| refcount != mapped || *mapped == 0)
        .map(|(hash, refcount, mapped)| {
            // Too low is the dangerous direction: removing one entry
            // reclaims a blob another still needs.
            let direction = if refcount < mapped {
                "TOO LOW"
            } else {
                "too high"
            };
            format!(
                "{direction}: {} refcount {refcount}, mapped refs {mapped}",
                &hash[..8.min(hash.len())]
            )
        })
        .collect();
    assert!(drifted.is_empty(), "blob index drift: {drifted:#?}");
}

/// Put `key` with one output per `(store_name, content)` pair. Sources
/// get a fresh path per `generation` (see the #822 note above).
fn put_outputs(store: &Store, dir: &Path, key: &str, generation: &str, outputs: &[(&str, &[u8])]) {
    let files: Vec<(PathBuf, String)> = outputs
        .iter()
        .map(|(name, content)| {
            let source = dir.join(format!("{key}-{generation}-{name}"));
            std::fs::write(&source, content).unwrap();
            (source, name.to_string())
        })
        .collect();
    store
        .put(key, "c1", &["lib".into()], &[], "", "dev", &files, "", "")
        .unwrap();
}

/// Lay out `key` the way a remote download extracts it: meta.json plus
/// one artifact per file inside the entry directory, nothing in the DB.
fn extract_download(store: &Store, key: &str, outputs: &[(&str, &[u8])]) -> EntryMeta {
    let entry_dir = store.entry_dir(key);
    std::fs::create_dir_all(&entry_dir).unwrap();
    let files = outputs
        .iter()
        .map(|(name, content)| {
            std::fs::write(entry_dir.join(name), content).unwrap();
            CachedFile {
                name: name.to_string(),
                size: content.len() as u64,
                hash: blake3::hash(content).to_hex().to_string(),
                executable: false,
            }
        })
        .collect();
    let meta = EntryMeta {
        cache_key: key.to_string(),
        key_schema: kache_format::CACHE_KEY_VERSION,
        crate_name: "c1".to_string(),
        crate_types: vec!["lib".to_string()],
        files,
        stdout: String::new(),
        stderr: String::new(),
        features: vec![],
        target: String::new(),
        profile: "dev".to_string(),
        compile_time_ms: 0,
        emit_kinds: Vec::new(),
    };
    std::fs::write(
        entry_dir.join("meta.json"),
        serde_json::to_string_pretty(&meta).unwrap(),
    )
    .unwrap();
    meta
}

fn content_hash(content: &[u8]) -> String {
    blake3::hash(content).to_hex().to_string()
}

#[test]
fn reput_with_identical_outputs_keeps_each_refcount_at_one() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    let outputs: [(&str, &[u8]); 2] = [("a.rlib", b"blob A"), ("b.rmeta", b"blob B")];

    put_outputs(&store, dir.path(), "reput_same", "one", &outputs);
    put_outputs(&store, dir.path(), "reput_same", "two", &outputs);

    assert_eq!(blob_refcount(&store, &content_hash(b"blob A")), Some(1));
    assert_eq!(blob_refcount(&store, &content_hash(b"blob B")), Some(1));
    assert_blob_refs_match_mappings(&store);
}

#[test]
fn reput_with_different_outputs_releases_only_the_dropped_blob() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();

    put_outputs(
        &store,
        dir.path(),
        "reput_diff",
        "one",
        &[("a.rlib", b"blob A"), ("b.rmeta", b"blob B")],
    );
    put_outputs(
        &store,
        dir.path(),
        "reput_diff",
        "two",
        &[("a.rlib", b"blob A"), ("b.rmeta", b"blob C")],
    );

    assert_eq!(blob_refcount(&store, &content_hash(b"blob A")), Some(1));
    assert_eq!(blob_refcount(&store, &content_hash(b"blob B")), None);
    assert_eq!(blob_refcount(&store, &content_hash(b"blob C")), Some(1));
    assert_blob_refs_match_mappings(&store);

    // B has no row left, so the orphan sweep may take its file; A and C
    // leave with the entry.
    store.remove_entry("reput_diff").unwrap();
    assert_eq!(blob_table_count(&store), 0);
    assert!(!store.blob_path(&content_hash(b"blob A")).exists());
    assert!(!store.blob_path(&content_hash(b"blob C")).exists());
}

#[test]
fn reput_of_one_key_leaves_a_shared_blob_at_two() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    let shared: (&str, &[u8]) = ("a.rlib", b"shared blob");

    put_outputs(&store, dir.path(), "share_one", "one", &[shared]);
    put_outputs(&store, dir.path(), "share_two", "one", &[shared]);
    put_outputs(&store, dir.path(), "share_one", "two", &[shared]);

    assert_eq!(
        blob_refcount(&store, &content_hash(b"shared blob")),
        Some(2)
    );
    assert_blob_refs_match_mappings(&store);

    store.remove_entry("share_one").unwrap();
    assert_eq!(
        blob_refcount(&store, &content_hash(b"shared blob")),
        Some(1)
    );
    store.remove_entry("share_two").unwrap();
    assert_eq!(blob_table_count(&store), 0);
}

/// A second download of a key that is already committed (two daemons, a
/// retried prefetch) must not add a second set of references.
#[test]
fn reimport_over_a_committed_entry_is_refcount_neutral() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    let outputs: [(&str, &[u8]); 2] = [("a.rlib", b"blob A"), ("b.rmeta", b"blob B")];

    extract_download(&store, "reimport", &outputs);
    store.import_downloaded_entry("reimport").unwrap();
    extract_download(&store, "reimport", &outputs);
    store.import_downloaded_entry("reimport").unwrap();

    assert_eq!(blob_refcount(&store, &content_hash(b"blob A")), Some(1));
    assert_eq!(blob_refcount(&store, &content_hash(b"blob B")), Some(1));
    assert_blob_refs_match_mappings(&store);

    store.remove_entry("reimport").unwrap();
    assert_eq!(blob_table_count(&store), 0, "eviction must free both blobs");
}

/// The same key can carry different bytes on the remote (a
/// non-reproducible output). The replaced generation's blob must be
/// released, the shared one kept at one reference.
#[test]
fn reimport_with_different_files_releases_the_replaced_generation() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();

    extract_download(
        &store,
        "reimport_diff",
        &[("a.rlib", b"blob A"), ("b.rmeta", b"blob B")],
    );
    store.import_downloaded_entry("reimport_diff").unwrap();
    put_outputs(&store, dir.path(), "other", "one", &[("a.rlib", b"blob A")]);
    extract_download(
        &store,
        "reimport_diff",
        &[("a.rlib", b"blob A"), ("b.rmeta", b"blob C")],
    );
    store.import_downloaded_entry("reimport_diff").unwrap();

    assert_eq!(blob_refcount(&store, &content_hash(b"blob A")), Some(2));
    assert_eq!(blob_refcount(&store, &content_hash(b"blob B")), None);
    assert_eq!(blob_refcount(&store, &content_hash(b"blob C")), Some(1));
    assert_blob_refs_match_mappings(&store);
}

/// A daemon killed between extraction and import leaves meta.json and
/// artifacts with no entry row. The startup migration must not read that
/// as a legacy entry: registering its blobs gives them a refcount that no
/// entry, and so no eviction, can ever release.
#[test]
fn startup_migration_leaves_an_unregistered_extraction_alone() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    extract_download(&store, "abandoned", &[("a.rlib", b"blob A")]);

    let stats = store.migrate_to_blobs(|_, _| {}).unwrap();

    assert_eq!(stats.entries_scanned, 1);
    assert_eq!(stats.entries_migrated, 0);
    assert_eq!(stats.entries_skipped, 1);
    assert_eq!(blob_table_count(&store), 0);
    assert!(store.entry_dir("abandoned").join("a.rlib").is_file());
    assert_blob_refs_match_mappings(&store);

    // The retried download still imports, at one reference.
    store.import_downloaded_entry("abandoned").unwrap();
    assert_eq!(blob_refcount(&store, &content_hash(b"blob A")), Some(1));
    assert_blob_refs_match_mappings(&store);
}

/// An uncommitted row is no more an owner than a missing one.
#[test]
fn migration_skips_an_uncommitted_entry() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    let meta = extract_download(&store, "uncommitted", &[("a.rlib", b"blob A")]);
    store
        .db
        .execute(
            "INSERT INTO entries (cache_key, crate_name, size, committed)
                 VALUES ('uncommitted', 'c1', 6, 0)",
            [],
        )
        .unwrap();

    assert!(!store.migrate_entry_to_blobs(&meta).unwrap());

    assert_eq!(blob_table_count(&store), 0);
    assert!(store.entry_dir("uncommitted").join("a.rlib").is_file());
}

/// Register `key` as a committed legacy entry: artifacts beside
/// meta.json, an entry row, no blob rows and no mapping.
fn legacy_entry(store: &Store, key: &str, outputs: &[(&str, &[u8])]) -> EntryMeta {
    let meta = extract_download(store, key, outputs);
    store
        .db
        .execute(
            "INSERT INTO entries (cache_key, crate_name, size, committed)
                 VALUES (?1, 'c1', 1, 1)",
            params![key],
        )
        .unwrap();
    meta
}

/// Backfill must not map an entry whose artifacts are still in its
/// directory: those references were never counted, and a mapping would
/// claim they were.
#[test]
fn backfill_skips_an_entry_with_unmigrated_artifacts() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    let meta = legacy_entry(
        &store,
        "legacy_unmigrated",
        &[("a.rlib", b"blob A"), ("b.rmeta", b"blob B")],
    );
    // One artifact left is enough.
    fs::remove_file(store.entry_dir("legacy_unmigrated").join("a.rlib")).unwrap();

    assert_eq!(store.backfill_entry_blobs().unwrap(), 0);
    let mapped: i64 = store
        .db
        .query_row("SELECT COUNT(*) FROM entry_blobs", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mapped, 0);

    // Migration counts the references; only then does backfill map them.
    assert!(store.migrate_entry_to_blobs(&meta).unwrap());
    assert_eq!(store.backfill_entry_blobs().unwrap(), 1);
    assert_eq!(blob_refcount(&store, &content_hash(b"blob B")), Some(1));
}

/// A legacy artifact whose hash a modern entry already owns still needs
/// its own reference, including a hash listed twice. Backfill waits for
/// the migration, so the mapping never runs ahead of the count.
#[test]
fn migration_counts_a_legacy_hash_another_entry_already_owns() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    put_outputs(&store, dir.path(), "modern", "one", &[("a.rlib", b"twin")]);
    let meta = legacy_entry(
        &store,
        "legacy_shared",
        &[
            ("a.rlib", b"twin"),
            ("b.rlib", b"twin"),
            ("c.rmeta", b"solo"),
        ],
    );
    assert_eq!(store.backfill_entry_blobs().unwrap(), 0);

    assert!(store.migrate_entry_to_blobs(&meta).unwrap());
    assert_eq!(store.backfill_entry_blobs().unwrap(), 1);

    assert_eq!(blob_refcount(&store, &content_hash(b"twin")), Some(3));
    assert_eq!(blob_refcount(&store, &content_hash(b"solo")), Some(1));
    assert_blob_refs_match_mappings(&store);
    store.remove_entry("legacy_shared").unwrap();
    assert!(store.get("modern").unwrap().is_some());
    assert_blob_refs_match_mappings(&store);
}

/// A mapping can name a blob that has no row (an older backfill mapped
/// legacy entries nothing had counted). Releasing it after the new
/// generation's increment would eat that increment and delete the row,
/// so every publisher releases first.
#[test]
fn republish_over_a_mapping_without_a_blob_row_keeps_the_new_reference() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    let map = |key: &str, content: &[u8]| {
        store
            .db
            .execute(
                "INSERT INTO entry_blobs (cache_key, hash, refs) VALUES (?1, ?2, 1)",
                params![key, content_hash(content)],
            )
            .unwrap();
    };

    map("via_import", b"import blob");
    extract_download(&store, "via_import", &[("a.rlib", b"import blob")]);
    store.import_downloaded_entry("via_import").unwrap();
    assert_eq!(
        blob_refcount(&store, &content_hash(b"import blob")),
        Some(1)
    );

    map("via_put", b"put blob");
    put_outputs(
        &store,
        dir.path(),
        "via_put",
        "one",
        &[("a.rlib", b"put blob")],
    );
    assert_eq!(blob_refcount(&store, &content_hash(b"put blob")), Some(1));

    assert_blob_refs_match_mappings(&store);
}

/// The batch import and the rebuild claim a key no entry row owned. A
/// mapping left behind for it holds counted references; they are
/// released, not dropped with the mapping.
#[test]
fn claiming_an_unowned_key_releases_its_leftover_mapping() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    let leftover = |key: &str| {
        store
            .db
            .execute(
                "INSERT INTO blobs (hash, size, refcount) VALUES (?1, 1, 1)",
                params![format!("old-{key}")],
            )
            .unwrap();
        store
            .db
            .execute(
                "INSERT INTO entry_blobs (cache_key, hash, refs) VALUES (?1, ?2, 1)",
                params![key, format!("old-{key}")],
            )
            .unwrap();
    };

    let batch_key = "b".repeat(64);
    leftover(&batch_key);
    let verified = write_verified_fixture(&store, &batch_key, &batch_key, "lib.rlib", None);
    assert_eq!(
        store.import_verified_restored_entries(&[verified]).unwrap(),
        1
    );
    assert_blob_refs_match_mappings(&store);

    let rebuild_key = "c".repeat(64);
    leftover(&rebuild_key);
    let mut meta = read_meta(&store, &batch_key);
    meta.cache_key = rebuild_key.clone();
    let rebuild_dir = store.entry_dir(&rebuild_key);
    fs::create_dir_all(&rebuild_dir).unwrap();
    fs::write(
        rebuild_dir.join("meta.json"),
        serde_json::to_string(&meta).unwrap(),
    )
    .unwrap();
    assert_eq!(
        store.rebuild_one_entry(&rebuild_key, &rebuild_dir).unwrap(),
        Some(1)
    );
    assert_blob_refs_match_mappings(&store);
    assert_eq!(blob_table_count(&store), 1, "both old-* rows are gone");
}

/// Entry `owner` holds the only counted reference to a blob; entry
/// `uncounted` has a committed row, the same file list and a mapping
/// whose reference nobody counted (what an older backfill left behind
/// for an un-migrated legacy entry). Returns the blob's hash.
fn owner_and_uncounted_mapping(store: &Store, dir: &Path) -> String {
    put_outputs(store, dir, "owner", "one", &[("a.rlib", b"shared blob")]);
    let mut meta = read_meta(store, "owner");
    meta.cache_key = "uncounted".to_string();
    let entry_dir = store.entry_dir("uncounted");
    fs::create_dir_all(&entry_dir).unwrap();
    fs::write(
        entry_dir.join("meta.json"),
        serde_json::to_string(&meta).unwrap(),
    )
    .unwrap();
    store
        .db
        .execute(
            "INSERT INTO entries (cache_key, crate_name, size, committed)
                 VALUES ('uncounted', 'c1', 1, 1)",
            [],
        )
        .unwrap();
    record_entry_blobs(&store.db, "uncounted", &meta.files).unwrap();
    let hash = content_hash(b"shared blob");
    assert_eq!(blob_refcount(store, &hash), Some(1));
    hash
}

fn mapped_refs(store: &Store, key: &str, hash: &str) -> Option<i64> {
    store
        .db
        .query_row(
            "SELECT refs FROM entry_blobs WHERE cache_key = ?1 AND hash = ?2",
            params![key, hash],
            |r| r.get(0),
        )
        .ok()
}

/// Replacing the uncounted generation through put must not spend the
/// owner's reference: the blob ends at the owner's one plus the new
/// generation's own, none of it taken from the owner.
#[test]
fn reput_over_an_uncounted_mapping_keeps_the_other_owners_reference() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    let hash = owner_and_uncounted_mapping(&store, dir.path());

    put_outputs(
        &store,
        dir.path(),
        "uncounted",
        "two",
        &[("b.rlib", b"other blob")],
    );

    assert_eq!(
        blob_refcount(&store, &hash),
        Some(1),
        "the owner's reference"
    );
    assert_eq!(mapped_refs(&store, "owner", &hash), Some(1));
    assert_eq!(mapped_refs(&store, "uncounted", &hash), None);
    assert!(store.blob_path(&hash).is_file());
    assert_blob_refs_match_mappings(&store);
}

#[test]
fn reimport_over_an_uncounted_mapping_keeps_the_other_owners_reference() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    let hash = owner_and_uncounted_mapping(&store, dir.path());

    extract_download(&store, "uncounted", &[("b.rlib", b"other blob")]);
    store.import_downloaded_entry("uncounted").unwrap();

    assert_eq!(
        blob_refcount(&store, &hash),
        Some(1),
        "the owner's reference"
    );
    assert_eq!(mapped_refs(&store, "owner", &hash), Some(1));
    assert_eq!(mapped_refs(&store, "uncounted", &hash), None);
    assert_blob_refs_match_mappings(&store);
}

/// Evicting the uncounted entry must not reclaim the blob its other
/// owner still serves from.
#[test]
fn removing_an_uncounted_mapping_keeps_the_other_owners_blob() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    let hash = owner_and_uncounted_mapping(&store, dir.path());

    store.remove_entry("uncounted").unwrap();

    assert_eq!(blob_refcount(&store, &hash), Some(1));
    assert!(store.blob_path(&hash).is_file());
    assert!(store.get("owner").unwrap().is_some());
    assert_blob_refs_match_mappings(&store);
}

#[test]
fn floor_raises_only_this_keys_blobs_and_only_up_to_their_mappings() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    store
        .db
        .execute_batch(
            "INSERT INTO blobs (hash, size, refcount) VALUES
                     ('low', 1, 1), ('exact', 1, 5), ('high', 1, 9), ('foreign', 1, 1);
                 INSERT INTO entry_blobs (cache_key, hash, refs) VALUES
                     ('k', 'low', 2), ('other', 'low', 1),
                     ('k', 'exact', 2), ('other', 'exact', 3),
                     ('k', 'high', 1),
                     ('k', 'norow', 1),
                     ('other', 'foreign', 4);",
        )
        .unwrap();

    // Three rows match; 'norow' has none to raise.
    assert_eq!(floor_blob_refs_at_mappings(&store.db, "k").unwrap(), 3);

    assert_eq!(blob_refcount(&store, "low"), Some(3));
    assert_eq!(blob_refcount(&store, "exact"), Some(5));
    assert_eq!(blob_refcount(&store, "high"), Some(9), "never lowered");
    assert_eq!(blob_refcount(&store, "norow"), None);
    assert_eq!(blob_refcount(&store, "foreign"), Some(1), "not this key's");
}

#[test]
fn release_entry_blob_refs_subtracts_this_keys_mapping_only() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    store
        .db
        .execute_batch(
            "INSERT INTO blobs (hash, size, refcount) VALUES
                     ('shared', 1, 5), ('last', 1, 2), ('drifted', 1, 1), ('idle', 1, 0),
                     ('uncounted', 1, 1);
                 INSERT INTO entry_blobs (cache_key, hash, refs) VALUES
                     ('k', 'shared', 2), ('k', 'last', 2), ('k', 'drifted', 3),
                     ('k', 'norow', 1), ('k', 'uncounted', 1),
                     ('other', 'shared', 3), ('other', 'uncounted', 1);",
        )
        .unwrap();

    release_entry_blob_refs(&store.db, "k").unwrap();

    assert_eq!(blob_refcount(&store, "shared"), Some(3));
    assert_eq!(blob_refcount(&store, "last"), None, "released to zero");
    assert_eq!(blob_refcount(&store, "norow"), None, "nothing to release");
    assert_eq!(
        blob_refcount(&store, "uncounted"),
        Some(1),
        "the other owner's reference survives"
    );
    assert_eq!(
        blob_refcount(&store, "drifted"),
        None,
        "clamped, not negative"
    );
    assert_eq!(
        blob_refcount(&store, "idle"),
        Some(0),
        "rows this key never mapped are not its to delete"
    );
    let mapped: Vec<String> = store
        .db
        .prepare("SELECT cache_key FROM entry_blobs")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(mapped, vec!["other".to_string(), "other".to_string()]);
}

/// An unreadable meta.json (EACCES, not NotFound) must refuse through the
/// unreadable-meta arm — "reading meta.json" — not be misread as missing
/// and routed into the missing-meta bounce, whose diagnostics describe a
/// different state (#276, #670). The distinction is the arm's NotFound
/// guard; this pins it against being widened to every error.
#[cfg(unix)]
#[test]
fn remove_entry_refuses_unreadable_meta_as_a_read_error() {
    use std::os::unix::fs::PermissionsExt;
    if unsafe { libc::geteuid() } == 0 {
        eprintln!("skipping: running as root, mode 000 does not deny access");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let output = dir.path().join("lib.rlib");
    std::fs::write(&output, b"x").unwrap();
    store
        .put(
            "locked",
            "c1",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output, "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();
    let entry_dir = store.entry_dir("locked");
    std::fs::set_permissions(&entry_dir, std::fs::Permissions::from_mode(0o000)).unwrap();
    let err = store.remove_entry("locked").unwrap_err();
    std::fs::set_permissions(&entry_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(
        format!("{err:#}").contains("reading meta.json"),
        "an unreadable meta must refuse as a read error, got: {err:#}"
    );
    assert!(
        store.contains("locked"),
        "entry row must survive a refused removal"
    );
}

/// #211: blob path construction must be panic-safe for a malformed (short)
/// hash that bypasses validation; it must not slice `[..2]` on `len < 2`.
#[test]
fn blob_path_is_panic_safe_for_short_hash() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    // Would panic on `&hash[..2]` before the fix.
    let _ = store.blob_path("a");
    let _ = store.blob_path("");
}

#[test]
fn test_store_remove_entry_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    // Should not error
    store.remove_entry("nonexistent").unwrap();
}

#[test]
fn test_store_list_entries_empty() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let entries = store.list_entries("name").unwrap();
    assert!(entries.is_empty());
}

#[test]
fn test_store_list_entries_sort_by() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let out1 = dir.path().join("a.rlib");
    std::fs::write(&out1, vec![0u8; 100]).unwrap();
    store
        .put(
            "k1",
            "alpha",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(out1, "a.rlib".into())],
            "",
            "",
        )
        .unwrap();

    let out2 = dir.path().join("b.rlib");
    std::fs::write(&out2, vec![0u8; 200]).unwrap();
    store
        .put(
            "k2",
            "beta",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(out2, "b.rlib".into())],
            "",
            "",
        )
        .unwrap();

    // Sort by name
    let entries = store.list_entries("name").unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].crate_name, "alpha");

    // Sort by size
    let entries = store.list_entries("size").unwrap();
    assert_eq!(entries.len(), 2);
    assert!(entries[0].size >= entries[1].size);

    // Sort by hits
    let entries = store.list_entries("hits").unwrap();
    assert_eq!(entries.len(), 2);
}

#[test]
fn list_entries_errors_on_non_integer_size_row() {
    // Covers list_entries row decoding error branch.
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    store
        .db
        .execute(
            "INSERT INTO entries \
                 (cache_key, crate_name, crate_type, profile, size, committed) \
                 VALUES ('bad_size', 'bad', 'lib', 'dev', x'01', 1)",
            [],
        )
        .unwrap();

    let err = store.list_entries("name").unwrap_err();

    assert!(
        err.to_string().contains("Invalid column type"),
        "expected SQLite type error, got: {err}"
    );
}

#[test]
fn test_store_evict_older_than() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output = dir.path().join("lib.rlib");
    std::fs::write(&output, b"content").unwrap();
    store
        .put(
            "k1",
            "c1",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output.clone(), "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();
    store.remove_clone_for_test(&output);

    // Backdate the entry so eviction is deterministic (not timing-dependent)
    store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-48 hours') WHERE cache_key = 'k1'",
                [],
            )
            .unwrap();

    // Evict entries older than 24 hours — our backdated entry qualifies
    let stats = store.evict_older_than(24).unwrap();
    assert_eq!(stats.entries_evicted, 1);
    assert!(!store.contains("k1"));
}

#[test]
fn test_store_evict_older_than_keeps_recent() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output = dir.path().join("lib.rlib");
    std::fs::write(&output, b"content").unwrap();
    store
        .put(
            "k1",
            "c1",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output, "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();

    // Evict entries older than 9999 hours — nothing should be evicted
    let stats = store.evict_older_than(9999).unwrap();
    assert_eq!(stats.entries_evicted, 0);
    assert!(store.contains("k1"));
}

#[test]
fn evict_stale_key_schemas_keeps_only_the_running_schema() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    for (key, content) in [
        ("current", b"current artifact".as_slice()),
        ("old", b"old artifact".as_slice()),
        ("legacy", b"legacy artifact".as_slice()),
    ] {
        let output = dir.path().join(format!("{key}.rlib"));
        std::fs::write(&output, content).unwrap();
        store
            .put(
                key,
                key,
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output.clone(), format!("{key}.rlib"))],
                "",
                "",
            )
            .unwrap();
        store.remove_clone_for_test(&output);
    }

    let prior_schema = kache_format::CACHE_KEY_VERSION.saturating_sub(1);
    store
        .db
        .execute(
            "UPDATE entries
                 SET key_schema = ?1, last_accessed = datetime('now', '-1 day')
                 WHERE cache_key = 'old'",
            params![prior_schema],
        )
        .unwrap();
    store
        .db
        .execute(
            "UPDATE entries
                 SET key_schema = 0, last_accessed = datetime('now', '-1 day')
                 WHERE cache_key = 'legacy'",
            [],
        )
        .unwrap();

    let stats = store
        .evict_stale_key_schemas(kache_format::CACHE_KEY_VERSION)
        .unwrap();
    assert_eq!(stats.entries_evicted, 2);
    assert!(stats.bytes_freed > 0);
    assert_eq!(stats.blobs_removed, 2);
    assert_eq!(stats.entries_pinned, 0);
    assert!(store.contains("current"));
    assert!(!store.contains("old"));
    assert!(!store.contains("legacy"));
    assert_eq!(store.entry_count().unwrap(), 1);

    let second = store
        .evict_stale_key_schemas(kache_format::CACHE_KEY_VERSION)
        .unwrap();
    assert_eq!(second.entries_evicted, 0);
}

#[test]
fn entry_meta_key_schema_defaults_to_unknown_for_legacy_json() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let output = dir.path().join("lib.rlib");
    std::fs::write(&output, b"artifact").unwrap();
    store
        .put(
            "key",
            "crate",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output, "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();

    let content = std::fs::read_to_string(store.entry_dir("key").join("meta.json")).unwrap();
    let current: EntryMeta = serde_json::from_str(&content).unwrap();
    assert_eq!(current.key_schema, kache_format::CACHE_KEY_VERSION);

    let mut legacy: serde_json::Value = serde_json::from_str(&content).unwrap();
    legacy.as_object_mut().unwrap().remove("key_schema");
    let parsed: EntryMeta = serde_json::from_value(legacy).unwrap();
    assert_eq!(parsed.key_schema, 0);
}

#[test]
fn test_store_import_downloaded_entry() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    // Create a fake downloaded entry directory
    let entry_dir = config.store_dir().join("downloaded_key");
    std::fs::create_dir_all(&entry_dir).unwrap();

    let artifact_content = b"fake artifact";
    std::fs::write(entry_dir.join("lib.rlib"), artifact_content).unwrap();
    // Real content hash — the import trust boundary re-hashes and rejects a
    // mismatch (kunobi-ninja/kache#211).
    let hash = crate::file_hash::hash_file(&entry_dir.join("lib.rlib")).unwrap();
    let prior_schema = kache_format::CACHE_KEY_VERSION.saturating_sub(1);
    let meta = EntryMeta {
        cache_key: "downloaded_key".to_string(),
        key_schema: prior_schema,
        crate_name: "downloaded_crate".to_string(),
        crate_types: vec!["lib".to_string()],
        files: vec![CachedFile {
            name: "lib.rlib".to_string(),
            size: artifact_content.len() as u64,
            hash,
            executable: false,
        }],
        stdout: String::new(),
        stderr: String::new(),
        features: vec!["std".to_string()],
        target: "x86_64-unknown-linux-gnu".to_string(),
        profile: "dev".to_string(),
        compile_time_ms: 0,
        emit_kinds: Vec::new(),
    };
    let meta_json = serde_json::to_string_pretty(&meta).unwrap();
    std::fs::write(entry_dir.join("meta.json"), meta_json).unwrap();

    store.import_downloaded_entry("downloaded_key").unwrap();
    assert!(store.contains("downloaded_key"));
    assert_eq!(store.entry_count().unwrap(), 1);
    let indexed_schema: u32 = store
        .db
        .query_row(
            "SELECT key_schema FROM entries WHERE cache_key = 'downloaded_key'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(indexed_schema, prior_schema);
}

#[test]
fn test_store_import_downloaded_entry_missing_file() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    // Create entry directory with meta.json but NO artifact file
    let entry_dir = config.store_dir().join("incomplete_key");
    std::fs::create_dir_all(&entry_dir).unwrap();

    let meta = EntryMeta {
        cache_key: "incomplete_key".to_string(),
        key_schema: kache_format::CACHE_KEY_VERSION,
        crate_name: "incomplete_crate".to_string(),
        crate_types: vec!["lib".to_string()],
        files: vec![CachedFile {
            name: "lib.rlib".to_string(),
            size: 42,
            // Valid-shaped hash so validation reaches the missing-file check.
            hash: "a".repeat(64),
            executable: false,
        }],
        stdout: String::new(),
        stderr: String::new(),
        features: vec![],
        target: String::new(),
        profile: "dev".to_string(),
        compile_time_ms: 0,
        emit_kinds: Vec::new(),
    };
    let meta_json = serde_json::to_string_pretty(&meta).unwrap();
    std::fs::write(entry_dir.join("meta.json"), meta_json).unwrap();
    // Deliberately NOT creating lib.rlib

    let err = store.import_downloaded_entry("incomplete_key").unwrap_err();
    assert!(
        err.to_string().contains("missing file"),
        "expected 'missing file' error, got: {err}"
    );
    assert!(!store.contains("incomplete_key"));
}

#[test]
fn failed_restore_cleanup_preserves_a_concurrent_committed_generation() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let publisher_store = Store::open(&config).unwrap();
    let cleanup_store = Store::open(&config).unwrap();
    let key = blake3::hash(b"restore-cleanup-publication-race")
        .to_hex()
        .to_string();
    let entry_dir = config.store_dir().join(&key);
    let meta_json = serde_json::to_string_pretty(&EntryMeta {
        cache_key: key.clone(),
        key_schema: kache_format::CACHE_KEY_VERSION,
        crate_name: "published".into(),
        crate_types: vec!["lib".into()],
        files: Vec::new(),
        stdout: String::new(),
        stderr: String::new(),
        features: Vec::new(),
        target: String::new(),
        profile: "dev".into(),
        compile_time_ms: 0,
        emit_kinds: Vec::new(),
    })
    .unwrap();

    // Hold a publisher's SQLite write transaction after materializing its
    // meta.json but before commit. The cleanup announces the exact point at
    // which it is about to request the same writer lock; only then does the
    // publisher commit. This fixes the ordering without scheduler sleeps.
    let (published_tx, published_rx) = std::sync::mpsc::sync_channel(0);
    let (cleanup_attempt_tx, cleanup_attempt_rx) = std::sync::mpsc::sync_channel(0);
    let publisher_key = key.clone();
    let publisher_entry_dir = entry_dir.clone();
    let publisher_meta = meta_json.clone();
    let publisher = std::thread::spawn(move || {
        let tx = publisher_store.db.unchecked_transaction().unwrap();
        tx.execute(
            "INSERT INTO entries (cache_key, crate_name, size, committed) \
                 VALUES (?1, 'published', 0, 1)",
            params![publisher_key],
        )
        .unwrap();
        fs::create_dir_all(&publisher_entry_dir).unwrap();
        fs::write(publisher_entry_dir.join("meta.json"), publisher_meta).unwrap();
        published_tx.send(()).unwrap();
        cleanup_attempt_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("cleanup must attempt the writer lock");
        tx.commit().unwrap();
    });

    published_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("publisher must reach its pre-commit point");
    let cleanup_key = key.clone();
    let cleanup = std::thread::spawn(move || {
        cleanup_store.discard_uncommitted_restored_entry_inner(
            &cleanup_key,
            || {
                cleanup_attempt_tx.send(()).unwrap();
            },
            || {},
        )
    });

    publisher.join().unwrap();
    cleanup
        .join()
        .unwrap()
        .expect("cleanup should observe and preserve the committed winner");

    let store = Store::open(&config).unwrap();
    assert!(store.contains(&key));
    assert_eq!(
        fs::read_to_string(entry_dir.join("meta.json")).unwrap(),
        meta_json,
        "cleanup must not remove or replace the generation that committed first"
    );
}

#[test]
fn failed_restore_cleanup_holds_the_writer_lock_through_removal() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let cleanup_store = Store::open(&config).unwrap();
    let contender_store = Store::open(&config).unwrap();
    let key = blake3::hash(b"restore-cleanup-first").to_hex().to_string();
    let entry_dir = cleanup_store.entry_dir(&key);
    fs::create_dir_all(&entry_dir).unwrap();
    fs::write(entry_dir.join("meta.json"), b"stale restore").unwrap();

    let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(0);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
    let cleanup_key = key.clone();
    let cleanup = std::thread::spawn(move || {
        cleanup_store.discard_uncommitted_restored_entry_inner(
            &cleanup_key,
            || {},
            || {
                locked_tx.send(()).unwrap();
                release_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("test must release the cleanup writer lock");
            },
        )
    });

    locked_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("cleanup must acquire its writer lock");
    contender_store.db.busy_timeout(Duration::ZERO).unwrap();
    let lock_error = match rusqlite::Transaction::new_unchecked(
        &contender_store.db,
        rusqlite::TransactionBehavior::Immediate,
    ) {
        Ok(transaction) => {
            drop(transaction);
            panic!("another publisher acquired SQLite's writer lock during cleanup");
        }
        Err(error) => error,
    };
    assert!(
        matches!(
            lock_error,
            SqlError::SqliteFailure(code, _)
                if matches!(code.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
        ),
        "unexpected competing-writer result: {lock_error}"
    );

    release_tx.send(()).unwrap();
    cleanup
        .join()
        .unwrap()
        .expect("cleanup should remove the uncommitted residue");
    assert!(!entry_dir.exists());

    let output = dir.path().join("published.rlib");
    fs::write(&output, b"published after cleanup").unwrap();
    contender_store
        .put(
            &key,
            "published",
            &["rlib".into()],
            &[],
            "host",
            "dev",
            &[(output, "published.rlib".into())],
            "",
            "",
        )
        .expect("a publisher must succeed after cleanup releases the lock");
    assert!(contender_store.contains(&key));
    assert!(entry_dir.join("meta.json").is_file());
}

#[test]
fn failed_restore_cleanup_accepts_an_already_missing_directory() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let key = blake3::hash(b"already-missing-restore")
        .to_hex()
        .to_string();

    assert!(!store.entry_dir(&key).exists());
    store
        .discard_uncommitted_restored_entry(&key)
        .expect("an already-absent restore has nothing left to clean up");
}

#[test]
fn failed_restore_cleanup_reports_non_directory_residue() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let key = blake3::hash(b"non-directory-restore-residue")
        .to_hex()
        .to_string();
    let entry_path = store.entry_dir(&key);
    fs::write(&entry_path, b"not a directory").unwrap();

    let error = store
        .discard_uncommitted_restored_entry(&key)
        .expect_err("non-directory residue must not be silently accepted");
    assert!(
        error
            .to_string()
            .contains("removing uncommitted restored entry"),
        "unexpected cleanup error: {error:#}"
    );
    assert!(entry_path.is_file());
}

#[test]
fn test_import_downloaded_entry_creates_blobs() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    // Simulate a downloaded entry (old tar format: files in entry dir)
    let entry_dir = config.store_dir().join("dl_key");
    fs::create_dir_all(&entry_dir).unwrap();
    fs::write(entry_dir.join("lib.rlib"), b"artifact data").unwrap();

    let hash = crate::file_hash::hash_file(&entry_dir.join("lib.rlib")).unwrap();
    let meta = EntryMeta {
        cache_key: "dl_key".to_string(),
        key_schema: kache_format::CACHE_KEY_VERSION,
        crate_name: "dl_crate".to_string(),
        crate_types: vec!["lib".to_string()],
        files: vec![CachedFile {
            name: "lib.rlib".to_string(),
            size: 13,
            hash: hash.clone(),
            executable: false,
        }],
        stdout: String::new(),
        stderr: String::new(),
        features: vec![],
        target: String::new(),
        profile: "dev".to_string(),
        compile_time_ms: 0,
        emit_kinds: Vec::new(),
    };
    fs::write(
        entry_dir.join("meta.json"),
        serde_json::to_string_pretty(&meta).unwrap(),
    )
    .unwrap();

    store.import_downloaded_entry("dl_key").unwrap();

    // Blob should exist
    let blob = store.blob_path(&hash);
    assert!(
        blob.exists(),
        "blob should be created from downloaded artifact"
    );

    // Entry dir artifact should be gone (only meta.json remains)
    assert!(
        !entry_dir.join("lib.rlib").exists(),
        "artifact should have been moved to blob store"
    );
    assert!(
        entry_dir.join("meta.json").exists(),
        "meta.json should remain"
    );

    // Blob should be read-only
    let perms = fs::metadata(&blob).unwrap().permissions();
    assert!(perms.readonly(), "imported blob should be read-only");

    // Refcount should be 1 in the blobs table
    let refcount: i64 = store
        .db
        .query_row(
            "SELECT refcount FROM blobs WHERE hash = ?1",
            params![&hash],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(refcount, 1);

    // Entry should be committed
    assert!(store.contains("dl_key"));
}

#[test]
fn test_store_get_evicts_entry_with_missing_file() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    // Put a valid entry
    let output = dir.path().join("lib.rlib");
    std::fs::write(&output, b"content").unwrap();
    store
        .put(
            "damaged_key",
            "damaged_crate",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output, "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();
    assert!(store.contains("damaged_key"));

    // Simulate corruption: delete the blob file from the store
    let meta_content =
        std::fs::read_to_string(store.entry_dir("damaged_key").join("meta.json")).unwrap();
    let meta: EntryMeta = serde_json::from_str(&meta_content).unwrap();
    let blob = store.blob_path(&meta.files[0].hash);
    // Make writable so we can delete
    let mut perms = std::fs::metadata(&blob).unwrap().permissions();
    perms.set_readonly(false);
    std::fs::set_permissions(&blob, perms).unwrap();
    std::fs::remove_file(&blob).unwrap();

    // get() should detect the missing file, evict, and return None
    let result = store.get("damaged_key").unwrap();
    assert!(
        result.is_none(),
        "expected None for entry with missing file"
    );
    assert!(
        !store.contains("damaged_key"),
        "entry should have been evicted"
    );
}

#[test]
fn test_store_get_evicts_entry_with_corrupted_file() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    // Put a valid entry
    let output = dir.path().join("lib.rlib");
    std::fs::write(&output, b"valid rlib content here").unwrap();
    store
        .put(
            "corrupt_key",
            "corrupt_crate",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output, "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();
    assert!(store.contains("corrupt_key"));

    // Simulate corruption: truncate the blob to a different size
    let meta_content =
        std::fs::read_to_string(store.entry_dir("corrupt_key").join("meta.json")).unwrap();
    let meta: EntryMeta = serde_json::from_str(&meta_content).unwrap();
    let blob = store.blob_path(&meta.files[0].hash);
    let mut perms = std::fs::metadata(&blob).unwrap().permissions();
    perms.set_readonly(false);
    std::fs::set_permissions(&blob, perms).unwrap();
    std::fs::write(&blob, b"short").unwrap();

    // get() should detect the size mismatch, evict, and return None
    let result = store.get("corrupt_key").unwrap();
    assert!(
        result.is_none(),
        "expected None for entry with size-corrupted file"
    );
    assert!(
        !store.contains("corrupt_key"),
        "entry should have been evicted"
    );
}

#[cfg(unix)]
#[test]
fn get_evicts_when_verified_blob_is_unreadable() {
    // Covers get verification hash_file error branch.
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output = dir.path().join("lib.rlib");
    fs::write(&output, b"readable before chmod").unwrap();
    store
        .put(
            "unreadable_key",
            "unreadable_crate",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output, "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();

    let meta = store.get("unreadable_key").unwrap().unwrap();
    let blob = store.blob_path(&meta.files[0].hash);
    fs::set_permissions(&blob, fs::Permissions::from_mode(0o000)).unwrap();

    let _env_lock = crate::test_support::process_state_test_lock();
    let _verify = EnvVarGuard::set("KACHE_VERIFY_RESTORES", "always");
    let result = store.get("unreadable_key").unwrap();

    assert!(result.is_none(), "unreadable verified blob is evicted");
    assert!(!store.contains("unreadable_key"));
}

#[test]
fn test_store_put_rejects_zero_byte_artifact() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    // Create a zero-byte file
    let output = dir.path().join("empty.rlib");
    std::fs::write(&output, b"").unwrap();

    let err = store
        .put(
            "zero_key",
            "zero_crate",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output, "empty.rlib".into())],
            "",
            "",
        )
        .unwrap_err();
    assert!(
        err.to_string().contains("zero-byte"),
        "expected 'zero-byte' error, got: {err}"
    );
    assert!(!store.contains("zero_key"));
}

#[test]
fn test_store_put_accepts_zero_byte_rmeta() {
    // `cargo check` / `cargo clippy --all-targets` compile test and bin units
    // with `--emit=metadata`, and rustc writes an empty `.rmeta` for them.
    // The entry — and the non-empty siblings the old guard took down with it
    // — must still cache (kunobi-ninja/kache#624).
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let rmeta = dir.path().join("libit-15ba26cbaff655a7.rmeta");
    std::fs::write(&rmeta, b"").unwrap();
    let depinfo = dir.path().join("it-15ba26cbaff655a7.d");
    std::fs::write(&depinfo, b"it: tests/it.rs\n").unwrap();

    store
        .put(
            "zero_rmeta_key",
            "it",
            // A `--test` unit: cargo passes no `--crate-type`, so the
            // wrapper records none — the shape observed on a real
            // `cargo check --all-targets`.
            &[],
            &[],
            "",
            "dev",
            &[
                (rmeta, "libit-15ba26cbaff655a7.rmeta".into()),
                (depinfo, "it-15ba26cbaff655a7.d".into()),
            ],
            "",
            "",
        )
        .unwrap();

    let meta = store.get("zero_rmeta_key").unwrap().unwrap();
    assert_eq!(meta.files.len(), 2, "sibling outputs survive the empty one");
    let stored_rmeta = meta
        .files
        .iter()
        .find(|f| f.name.ends_with(".rmeta"))
        .expect("rmeta stored");
    assert_eq!(stored_rmeta.size, 0);
    assert_eq!(
        store
            .blob_path(&stored_rmeta.hash)
            .metadata()
            .unwrap()
            .len(),
        0,
        "empty blob materialized in the content store"
    );
    // The emit-coverage gate still sees `metadata` (kunobi-ninja/kache#325),
    // so a `--emit=metadata` invocation can hit this entry.
    assert!(meta.emit_kinds.iter().any(|k| k == "metadata"));
}

#[test]
fn test_store_put_rejects_zero_byte_rmeta_from_a_library_unit() {
    // A `lib` unit HAS metadata to emit, so an empty `.rmeta` there is a
    // truncated write — the exemption for test/bin units must not reopen
    // the guard for libraries (kunobi-ninja/kache#624).
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let rmeta = dir.path().join("libfoo-1234.rmeta");
    std::fs::write(&rmeta, b"").unwrap();

    let err = store
        .put(
            "truncated_lib_rmeta",
            "foo",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(rmeta, "libfoo-1234.rmeta".into())],
            "",
            "",
        )
        .unwrap_err();
    assert!(
        err.to_string().contains("zero-byte"),
        "expected 'zero-byte' error, got: {err}"
    );
    assert!(!store.contains("truncated_lib_rmeta"));
}

#[test]
fn zero_byte_is_valid_output_only_for_metadata_without_a_library_unit() {
    // `--test` unit (no `--crate-type`), and the `--emit=metadata` crate
    // types rustc leaves empty.
    assert!(zero_byte_is_valid_output::<TestPolicy>(
        "libfoo-1234.rmeta",
        &[]
    ));
    for ct in ["bin", "cdylib", "staticlib"] {
        assert!(
            zero_byte_is_valid_output::<TestPolicy>("libfoo-1234.rmeta", &[ct.into()]),
            "{ct} emits no metadata, so an empty .rmeta is legitimate"
        );
    }
    // These do emit metadata — empty means truncated.
    for ct in ["lib", "rlib", "dylib", "proc-macro", "some-future-type"] {
        assert!(
            !zero_byte_is_valid_output::<TestPolicy>("libfoo-1234.rmeta", &[ct.into()]),
            "{ct} must keep the truncation guard"
        );
    }
    // Everything else empty means a truncated write, not a real output.
    for name in ["libfoo.rlib", "foo.d", "foo.o", "libfoo.so", "foo"] {
        assert!(
            !zero_byte_is_valid_output::<TestPolicy>(name, &[]),
            "{name} must stay rejected when empty"
        );
    }
}

#[test]
fn test_store_import_rejects_size_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    // Create a fake downloaded entry with mismatched size in metadata
    let entry_dir = config.store_dir().join("mismatch_key");
    std::fs::create_dir_all(&entry_dir).unwrap();

    let meta = EntryMeta {
        cache_key: "mismatch_key".to_string(),
        key_schema: kache_format::CACHE_KEY_VERSION,
        crate_name: "mismatch_crate".to_string(),
        crate_types: vec!["lib".to_string()],
        files: vec![CachedFile {
            name: "lib.rlib".to_string(),
            size: 9999, // Wrong size
            // Valid-shaped hash so validation reaches the size check.
            hash: "a".repeat(64),
            executable: false,
        }],
        stdout: String::new(),
        stderr: String::new(),
        features: vec![],
        target: String::new(),
        profile: "dev".to_string(),
        compile_time_ms: 0,
        emit_kinds: Vec::new(),
    };
    let meta_json = serde_json::to_string_pretty(&meta).unwrap();
    std::fs::write(entry_dir.join("meta.json"), meta_json).unwrap();
    std::fs::write(entry_dir.join("lib.rlib"), b"small content").unwrap();

    let err = store.import_downloaded_entry("mismatch_key").unwrap_err();
    assert!(
        err.to_string().contains("size mismatch"),
        "expected 'size mismatch' error, got: {err}"
    );
}

/// Build a downloaded entry dir with one artifact and a `meta.json` whose
/// `CachedFile` is overridden by `mutate`, then try to import it.
#[cfg(test)]
fn import_with_poisoned_meta(
    store: &Store,
    config: &Config,
    key: &str,
    content: &[u8],
    mutate: impl FnOnce(&mut CachedFile),
) -> anyhow::Result<()> {
    let entry_dir = config.store_dir().join(key);
    std::fs::create_dir_all(&entry_dir).unwrap();
    std::fs::write(entry_dir.join("lib.rlib"), content).unwrap();
    let mut file = CachedFile {
        name: "lib.rlib".to_string(),
        size: content.len() as u64,
        hash: crate::file_hash::hash_file(&entry_dir.join("lib.rlib")).unwrap(),
        executable: false,
    };
    mutate(&mut file);
    let meta = EntryMeta {
        cache_key: key.to_string(),
        key_schema: kache_format::CACHE_KEY_VERSION,
        crate_name: "c".to_string(),
        crate_types: vec!["lib".to_string()],
        files: vec![file],
        stdout: String::new(),
        stderr: String::new(),
        features: vec![],
        target: String::new(),
        profile: "dev".to_string(),
        compile_time_ms: 0,
        emit_kinds: Vec::new(),
    };
    std::fs::write(
        entry_dir.join("meta.json"),
        serde_json::to_string_pretty(&meta).unwrap(),
    )
    .unwrap();
    store.import_downloaded_entry(key)
}

/// kunobi-ninja/kache#211-A: a same-size object whose bytes don't match the
/// claimed hash is rejected — size-only validation is insufficient for
/// untrusted remote content.
#[test]
fn import_rejects_content_hash_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    // Claim the hash of *different* same-length bytes.
    let bogus = blake3::hash(b"DIFFERENT!!!!").to_hex().to_string();
    let err = import_with_poisoned_meta(&store, &config, "ch_mismatch", b"real_content!", |f| {
        f.hash = bogus;
    })
    .unwrap_err();
    assert!(
        err.to_string().contains("content hash mismatch"),
        "expected content hash mismatch, got: {err}"
    );
    assert!(!store.contains("ch_mismatch"));
}

/// kunobi-ninja/kache#211-C: a hash that isn't a 64-char blake3 hex digest
/// is rejected before it can reach path construction.
#[test]
fn import_rejects_malformed_hash() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let err = import_with_poisoned_meta(&store, &config, "bad_hash", b"data", |f| {
        f.hash = "../../etc/passwd".to_string();
    })
    .unwrap_err();
    assert!(
        err.to_string().contains("malformed blob hash"),
        "expected malformed blob hash, got: {err}"
    );
    assert!(!store.contains("bad_hash"));
}

/// kunobi-ninja/kache#211-B: an absolute or `..`-bearing artifact name is
/// rejected — `Path::join` with it would escape the entry/target dir.
#[test]
fn import_rejects_unsafe_artifact_name() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    for bad in ["/etc/passwd", "../escape.rlib", "sub/dir.rlib"] {
        let err = import_with_poisoned_meta(&store, &config, "unsafe_name", b"data", |f| {
            f.name = bad.to_string();
        })
        .unwrap_err();
        assert!(
            err.to_string().contains("unsafe artifact name"),
            "name {bad:?} should be rejected, got: {err}"
        );
    }
    assert!(!store.contains("unsafe_name"));
}

#[test]
fn test_store_keys_for_crates_empty() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let result = store.keys_for_crates(&[]).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_store_keys_for_crates_with_entries() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output = dir.path().join("lib.rlib");
    std::fs::write(&output, b"content").unwrap();
    store
        .put(
            "k1",
            "serde",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output.clone(), "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();

    rewrite_source(&output, b"content2");
    store
        .put(
            "k2",
            "tokio",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output, "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();

    let result = store.keys_for_crates(&["serde".to_string()]).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].crate_name, "serde");

    let result = store
        .keys_for_crates(&["serde".to_string(), "tokio".to_string()])
        .unwrap();
    assert_eq!(result.len(), 2);
}

#[test]
fn test_store_keys_for_crates_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let result = store.keys_for_crates(&["nonexistent".to_string()]).unwrap();
    assert!(result.is_empty());
}

#[test]
fn keys_for_crates_errors_on_non_text_cache_key_row() {
    // Covers keys_for_crates row decoding error branch.
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    store
        .db
        .execute(
            "INSERT INTO entries (cache_key, crate_name, size, committed) \
                 VALUES (x'80', 'badcrate', 1, 1)",
            [],
        )
        .unwrap();

    let err = store
        .keys_for_crates(&["badcrate".to_string()])
        .unwrap_err();

    assert!(
        err.to_string().contains("Invalid column type"),
        "expected SQLite type error, got: {err}"
    );
}

#[test]
fn test_store_put_records_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output = dir.path().join("lib.rlib");
    std::fs::write(&output, b"my rlib content").unwrap();
    store
        .put(
            "meta_key",
            "mycrate",
            &["lib".into(), "rlib".into()],
            &["std".into(), "derive".into()],
            "x86_64-unknown-linux-gnu",
            "release",
            &[(output, "lib.rlib".into())],
            "stdout text",
            "stderr text",
        )
        .unwrap();

    let meta = store.get("meta_key").unwrap().unwrap();
    assert_eq!(meta.crate_name, "mycrate");
    assert_eq!(meta.crate_types, vec!["lib", "rlib"]);
    assert_eq!(meta.features, vec!["std", "derive"]);
    assert_eq!(meta.target, "x86_64-unknown-linux-gnu");
    assert_eq!(meta.profile, "release");
    assert_eq!(meta.stdout, "stdout text");
    assert_eq!(meta.stderr, "stderr text");
    assert_eq!(meta.files.len(), 1);
    assert!(!meta.files[0].hash.is_empty());
}

#[test]
fn wait_for_committed_returns_false_without_an_owner() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "store::tests::wait_for_committed_missing_child_fixture",
            "--ignored",
        ])
        .env("KACHE_TEST_WAIT_ROOT", dir.path())
        .spawn()
        .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match child.try_wait().unwrap() {
            Some(status) => {
                assert!(status.success(), "wait fixture failed: {status}");
                break;
            }
            None if std::time::Instant::now() >= deadline => {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("waiting without an owner must return promptly");
            }
            None => std::thread::sleep(Duration::from_millis(10)),
        }
    }
}

#[test]
#[ignore = "subprocess fixture for wait_for_committed_returns_false_without_an_owner"]
fn wait_for_committed_missing_child_fixture() {
    let root = PathBuf::from(std::env::var_os("KACHE_TEST_WAIT_ROOT").expect("fixture root"));
    let store = Store::open(test_config(&root)).unwrap();
    assert!(!store.wait_for_committed("nope").unwrap());
}

#[test]
fn wait_for_committed_returns_true_for_an_existing_entry() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let output = dir.path().join("already-committed.rlib");
    fs::write(&output, b"committed output").unwrap();
    store
        .put(
            "already-committed",
            "peer",
            &["rlib".to_string()],
            &[],
            "host",
            "dev",
            &[(output, "already-committed.rlib".to_string())],
            "",
            "",
        )
        .unwrap();

    assert!(store.wait_for_committed("already-committed").unwrap());
}

#[test]
fn wait_for_committed_observes_advisory_lock_release() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let owner = Store::open(&config).unwrap();
    let cache_key = "peer-commit";
    let owner_lock = owner.try_lock(cache_key).unwrap().expect("owner lock");
    let root = dir.path().to_path_buf();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();

    let waiter = std::thread::spawn(move || {
        let store = Store::open(test_config(&root)).unwrap();
        ready_tx.send(()).unwrap();
        store
            .wait_for_committed_with_timeout(cache_key, Duration::from_secs(5))
            .unwrap()
    });
    ready_rx.recv().unwrap();
    std::thread::sleep(Duration::from_millis(50));

    let output = dir.path().join("peer.rlib");
    fs::write(&output, b"peer output").unwrap();
    owner
        .put(
            cache_key,
            "peer",
            &["rlib".to_string()],
            &[],
            "host",
            "dev",
            &[(output, "peer.rlib".to_string())],
            "",
            "",
        )
        .unwrap();
    drop(owner_lock);

    assert!(
        waiter.join().unwrap(),
        "waiter must observe the committed key"
    );
    assert!(
        owner.entry_dir(cache_key).with_extension("lock").exists(),
        "waiting must not depend on deleting the lock file"
    );
}

#[test]
fn wait_for_committed_respects_timeout_while_owned() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let owner = Store::open(&config).unwrap();
    let waiter = Store::open(&config).unwrap();
    let _lock = owner.try_lock("slow-peer").unwrap().expect("owner lock");

    assert!(
        !waiter
            .wait_for_committed_with_timeout("slow-peer", Duration::from_millis(10))
            .unwrap()
    );
}

#[test]
#[cfg(target_os = "macos")]
fn test_exclude_from_indexing_creates_sentinel() {
    let dir = tempfile::tempdir().unwrap();
    if let Some(handle) = exclude_from_indexing(dir.path()) {
        let _ = handle.join();
    }
    let sentinel = dir.path().join(".metadata_never_index");
    assert!(sentinel.exists());
    assert!(
        sentinel.metadata().unwrap().len() == 0,
        "sentinel should be empty"
    );
    // Idempotent — second call doesn't fail or modify
    if let Some(handle) = exclude_from_indexing(dir.path()) {
        let _ = handle.join();
    }
    assert!(sentinel.exists());
}

#[test]
#[cfg(target_os = "macos")]
fn test_exclude_from_indexing_sets_tmutil_xattr() {
    let dir = tempfile::tempdir().unwrap();
    // The tmutil child now runs on a detached thread (#588); join the
    // returned handle so the assertion isn't racing it.
    if let Some(handle) = exclude_from_indexing(dir.path()) {
        let _ = handle.join();
    }
    let output = std::process::Command::new("tmutil")
        .args(["isexcluded", &dir.path().display().to_string()])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("[Excluded]"),
        "expected [Excluded] in tmutil output, got: {stdout}"
    );

    // Second call must take the xattr fast path: no tmutil spawn at all.
    assert!(
        exclude_from_indexing(dir.path()).is_none(),
        "already-excluded dir must skip the tmutil subprocess"
    );
}

#[test]
#[cfg(target_os = "macos")]
fn test_exclude_from_indexing_skips_existing_sentinel() {
    let dir = tempfile::tempdir().unwrap();
    let sentinel = dir.path().join(".metadata_never_index");
    // Pre-create sentinel with known content
    fs::write(&sentinel, b"existing").unwrap();
    if let Some(handle) = exclude_from_indexing(dir.path()) {
        let _ = handle.join();
    }
    // Should not overwrite — guard checks exists()
    assert_eq!(fs::read(&sentinel).unwrap(), b"existing");
}

#[test]
fn test_blob_path_sharding() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let hash = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
    let path = store.blob_path(hash);
    // Normalise separators: the path is built with `PathBuf::join`, so the
    // shard dirs are `blobs\ab\…` on Windows.
    assert!(
        path.to_string_lossy()
            .replace('\\', "/")
            .contains("blobs/ab/")
    );
    assert!(path.to_string_lossy().ends_with(hash));
}

#[test]
fn test_blobs_table_created() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    // Table should exist — query it
    let count: i64 = store
        .db
        .query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
#[cfg(target_os = "macos")]
fn test_exclude_from_indexing_nonexistent_dir_silent() {
    let dir = PathBuf::from("/tmp/kache_test_nonexistent_874291");
    assert!(!dir.exists());
    // Should not panic — both operations fail silently
    if let Some(handle) = exclude_from_indexing(&dir) {
        let _ = handle.join();
    }
}

#[test]
fn test_put_creates_blob() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output = dir.path().join("lib.rlib");
    fs::write(&output, b"rlib content").unwrap();
    store
        .put(
            "k1",
            "mycrate",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output, "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();

    // Blob should exist
    let meta_path = store.entry_dir("k1").join("meta.json");
    let content = fs::read_to_string(&meta_path).unwrap();
    let meta: EntryMeta = serde_json::from_str(&content).unwrap();
    let blob = store.blob_path(&meta.files[0].hash);
    assert!(
        blob.exists(),
        "blob file should exist at {}",
        blob.display()
    );

    // Entry dir should only have meta.json (no artifact files)
    let entry_dir = store.entry_dir("k1");
    let mut files: Vec<_> = fs::read_dir(&entry_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    files.sort();
    assert_eq!(
        files,
        vec!["meta.json"],
        "entry dir should only contain meta.json"
    );
}

#[test]
fn test_put_deduplicates_identical_content() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output = dir.path().join("lib.rlib");
    rewrite_source(&output, b"same content");
    store
        .put(
            "k1",
            "crate_a",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output.clone(), "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();

    // Put again with same content but different cache key
    rewrite_source(&output, b"same content");
    store
        .put(
            "k2",
            "crate_a",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output, "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();

    // Both entries should reference the same blob hash
    let m1: EntryMeta =
        serde_json::from_str(&fs::read_to_string(store.entry_dir("k1").join("meta.json")).unwrap())
            .unwrap();
    let m2: EntryMeta =
        serde_json::from_str(&fs::read_to_string(store.entry_dir("k2").join("meta.json")).unwrap())
            .unwrap();
    assert_eq!(m1.files[0].hash, m2.files[0].hash);

    // Refcount should be 2
    let refcount: i64 = store
        .db
        .query_row(
            "SELECT refcount FROM blobs WHERE hash = ?1",
            params![m1.files[0].hash],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(refcount, 2);
}

#[test]
fn test_get_verifies_blobs_not_entry_files() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output = dir.path().join("lib.rlib");
    fs::write(&output, b"content").unwrap();
    store
        .put(
            "k1",
            "c",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output, "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();

    // Entry dir should NOT have lib.rlib — only meta.json
    assert!(!store.entry_dir("k1").join("lib.rlib").exists());

    // get() should still succeed (resolving via blob store)
    let meta = store.get("k1").unwrap();
    assert!(meta.is_some());
}

#[test]
fn test_get_evicts_when_blob_missing() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output = dir.path().join("lib.rlib");
    fs::write(&output, b"content").unwrap();
    store
        .put(
            "k1",
            "c",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output, "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();

    // Read meta to get the hash
    let meta_content = fs::read_to_string(store.entry_dir("k1").join("meta.json")).unwrap();
    let meta: EntryMeta = serde_json::from_str(&meta_content).unwrap();
    let blob = store.blob_path(&meta.files[0].hash);

    // Delete the blob to simulate corruption
    let mut perms = fs::metadata(&blob).unwrap().permissions();
    perms.set_readonly(false);
    fs::set_permissions(&blob, perms).unwrap();
    fs::remove_file(&blob).unwrap();

    // get() should detect missing blob and evict
    let result = store.get("k1").unwrap();
    assert!(result.is_none());
    assert!(!store.contains("k1"));
}

#[test]
fn test_put_blob_is_readonly() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output = dir.path().join("lib.rlib");
    fs::write(&output, b"content").unwrap();
    store
        .put(
            "k1",
            "c",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output, "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();

    let meta: EntryMeta =
        serde_json::from_str(&fs::read_to_string(store.entry_dir("k1").join("meta.json")).unwrap())
            .unwrap();
    let blob = store.blob_path(&meta.files[0].hash);
    let perms = fs::metadata(&blob).unwrap().permissions();
    assert!(perms.readonly(), "blob should be read-only");
}

#[test]
fn test_remove_entry_decrements_refcount() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output = dir.path().join("lib.rlib");
    rewrite_source(&output, b"shared content");
    store
        .put(
            "k1",
            "c",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output.clone(), "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();
    rewrite_source(&output, b"shared content");
    store
        .put(
            "k2",
            "c",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output, "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();

    // Get the hash from meta.json
    let meta_content = fs::read_to_string(store.entry_dir("k1").join("meta.json")).unwrap();
    let meta: EntryMeta = serde_json::from_str(&meta_content).unwrap();
    let hash = meta.files[0].hash.clone();
    let blob = store.blob_path(&hash);

    // Remove first entry — blob should still exist (refcount 1)
    store.remove_entry("k1").unwrap();
    assert!(blob.exists(), "blob should survive when refcount > 0");

    let refcount: i64 = store
        .db
        .query_row(
            "SELECT refcount FROM blobs WHERE hash = ?1",
            params![&hash],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(refcount, 1);

    // Remove second entry — blob should be deleted (refcount 0)
    store.remove_entry("k2").unwrap();
    assert!(!blob.exists(), "blob should be deleted when refcount = 0");

    let count: i64 = store
        .db
        .query_row(
            "SELECT COUNT(*) FROM blobs WHERE hash = ?1",
            params![&hash],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
}

/// Many independent clients (separate connections, like real wrapper
/// processes) concurrently cache distinct entries that share identical
/// content. Because registration is transactional, the shared blob's
/// refcount must equal the number of entries — no drift, no lost or
/// duplicated blob — and the per-writer temp names must leave no debris.
#[test]
fn test_concurrent_puts_sharing_blob_are_consistent() {
    const N: usize = 8;
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    // Initialise the schema once before the racing opens.
    Store::open(&config).unwrap();

    let content = b"identical artifact content shared across all entries";

    // Same Windows-runner caveat as test_concurrent_put_remove_never_dangles:
    // this proves refcount consistency, not the production five-second
    // fail-fast. Eight WAL writers on a two-core hosted Windows runner
    // queue past 5s and surface SQLITE_BUSY (v0.16.1 tag CI).
    let stores: Vec<_> = (0..N)
        .map(|_| {
            let store = Store::open(&config).unwrap();
            store.db.busy_timeout(Duration::from_secs(30)).unwrap();
            store
        })
        .collect();

    let mut handles = Vec::new();
    for (i, store) in stores.into_iter().enumerate() {
        let src = dir.path().join(format!("art-{i}.rlib"));
        std::fs::write(&src, content).unwrap();
        handles.push(std::thread::spawn(move || {
            store
                .put(
                    &format!("key{i}"),
                    "shared",
                    &["lib".into()],
                    &[],
                    "x86_64-unknown-linux-gnu",
                    "dev",
                    &[(src, "libshared.rlib".into())],
                    "",
                    "",
                )
                .unwrap();
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let store = Store::open(&config).unwrap();
    let hash = store.get("key0").unwrap().unwrap().files[0].hash.clone();

    // Exactly one blob, referenced by every entry.
    assert_eq!(store.blob_stats().unwrap().total_blobs, 1);
    let refcount: i64 = store
        .db
        .query_row(
            "SELECT refcount FROM blobs WHERE hash = ?1",
            params![&hash],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(refcount as usize, N, "refcount must equal the entry count");
    assert_blob_refs_match_mappings(&store);
    assert!(store.blob_path(&hash).is_file());
    for i in 0..N {
        assert!(
            store.contains(&format!("key{i}")),
            "entry key{i} must be committed"
        );
    }

    // Unique temp names must leave no debris in the shard directory.
    let shard = store.blob_path(&hash).parent().unwrap().to_path_buf();
    let tmp_left = std::fs::read_dir(&shard)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
        .count();
    assert_eq!(tmp_left, 0, "no leftover .tmp files");

    // Removing all but the last keeps the blob; removing the last reclaims it.
    for i in 0..N - 1 {
        store.remove_entry(&format!("key{i}")).unwrap();
    }
    assert!(
        store.blob_path(&hash).is_file(),
        "blob persists while still referenced"
    );
    store.remove_entry(&format!("key{}", N - 1)).unwrap();
    assert!(
        !store.blob_path(&hash).is_file(),
        "blob reclaimed once the last reference is gone"
    );
    assert_eq!(store.blob_stats().unwrap().total_blobs, 0);
}

/// Hammers a single shared blob with concurrent puts and removes from
/// independent connections. Because blob-file mutations and refcount
/// mutations both happen under the SQLite write lock, a `put` can never
/// commit an entry whose blob a concurrent `remove` has unlinked: every
/// just-put entry must be restorable with its blob present. Once all churn
/// settles, the blob is fully reclaimed.
#[test]
fn test_concurrent_put_remove_never_dangles() {
    const THREADS: usize = 8;
    const ROUNDS: usize = 30;
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    Store::open(&config).unwrap(); // initialise schema before racing opens

    let content = b"hot shared blob churned by concurrent puts and removes";

    // This test proves publication/removal atomicity, not the production
    // five-second fail-fast policy. A two-core hosted Windows runner can
    // keep eight WAL writers queued past that timeout. The full Windows
    // suite has also exhausted 30 seconds under mutation-runner load, so
    // give these test connections enough time for every logical operation
    // to commit and reach the invariant checks below.
    let stores: Vec<_> = (0..THREADS)
        .map(|_| {
            let store = Store::open(&config).unwrap();
            store.db.busy_timeout(Duration::from_secs(60)).unwrap();
            store
        })
        .collect();
    let start = std::sync::Arc::new(std::sync::Barrier::new(THREADS));

    let mut handles = Vec::new();
    for (t, store) in stores.into_iter().enumerate() {
        let dir_path = dir.path().to_path_buf();
        let start = std::sync::Arc::clone(&start);
        handles.push(std::thread::spawn(move || {
            start.wait();
            for r in 0..ROUNDS {
                let key = format!("t{t}r{r}");
                let src = dir_path.join(format!("src-{t}-{r}.rlib"));
                std::fs::write(&src, content).unwrap();
                store
                    .put(
                        &key,
                        "shared",
                        &["lib".into()],
                        &[],
                        "tgt",
                        "dev",
                        &[(src, "lib.rlib".into())],
                        "",
                        "",
                    )
                    .unwrap();

                // Our reference is committed: the entry must be restorable
                // and its blob present — never dangling from a concurrent
                // remove of another entry sharing the same blob.
                let meta = store
                    .get(&key)
                    .unwrap()
                    .unwrap_or_else(|| panic!("entry {key} vanished right after put"));
                assert!(
                    store.blob_path(&meta.files[0].hash).is_file(),
                    "blob missing while {key} still references it"
                );

                store.remove_entry(&key).unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // All entries removed → the shared blob is fully reclaimed.
    let store = Store::open(&config).unwrap();
    assert_eq!(store.blob_stats().unwrap().total_blobs, 0);
}

/// #1128 without SQLite in the way: publishers race a remover on one
/// blob path, so the name cycles through present and read-only,
/// delete-pending and absent far more often than whole puts manage. No
/// lock orders them, so a publish may end with the blob gone again; what
/// it must never do is report that race as an error.
#[test]
fn publish_racing_an_unlink_never_errors() {
    const PUBLISHERS: usize = 4;
    const ROUNDS: usize = 400;
    let dir = tempfile::tempdir().unwrap();
    // No fsync per publish: the race is in the rename, and the flushes
    // only spread the attempts out.
    let mut config = test_config(dir.path());
    config.deferred_durability = true;
    let store = Store::open(&config).unwrap();

    let content = b"one blob, published and unlinked in a loop";
    let source = dir.path().join("out.rlib");
    fs::write(&source, content).unwrap();
    let hash = crate::file_hash::hash_file(&source).unwrap();
    let blob = store.blob_path(&hash);

    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let remover = {
        let blob = blob.clone();
        let done = std::sync::Arc::clone(&done);
        std::thread::spawn(move || {
            while !done.load(Ordering::Relaxed) {
                unlink_blob(&blob);
                std::thread::yield_now();
            }
        })
    };

    let publishers: Vec<_> = (0..PUBLISHERS)
        .map(|_| {
            let store = Store::open(&config).unwrap();
            let source = source.clone();
            let hash = hash.clone();
            std::thread::spawn(move || {
                for _ in 0..ROUNDS {
                    let (staged, ingest) = store.stage_blob_from_source(&source, false).unwrap();
                    store
                        .publish_staged_blob(&staged, ingest, &hash, content.len() as u64)
                        .unwrap();
                }
            })
        })
        .collect();
    for p in publishers {
        p.join().unwrap();
    }
    done.store(true, Ordering::Relaxed);
    remover.join().unwrap();

    // With the remover stopped, the locked phase's repair always lands.
    store
        .rematerialize_and_verify(&source, &hash, "out.rlib", false)
        .unwrap();
    assert_eq!(fs::read(&blob).unwrap(), content);
}

/// kunobi-ninja/kache#670: a remover that deleted no row must not touch
/// the entry directory — a fresh meta.json there may belong to a
/// publisher whose registration transaction has not committed yet, and
/// deleting it strands the publication.
#[test]
fn losing_remover_leaves_a_publishers_directory_alone() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let src = dir.path().join("x.rlib");
    std::fs::write(&src, b"mid-publication content").unwrap();
    store
        .put(
            "pub",
            "c",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(src, "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();
    // Simulate the publisher's window: meta.json is on disk, the entry
    // row is not committed yet.
    store
        .db
        .execute("DELETE FROM entries WHERE cache_key = 'pub'", [])
        .unwrap();
    store
        .db
        .execute("DELETE FROM entry_blobs WHERE cache_key = 'pub'", [])
        .unwrap();

    store.remove_entry("pub").unwrap();
    assert!(
        store.entry_dir("pub").join("meta.json").exists(),
        "the loser deleted no row and must leave the publisher's meta.json alone"
    );
}

/// kunobi-ninja/kache#670: the row a removal deletes may belong to a
/// NEWER publication than the meta.json it read its hash list from
/// (`put` writes meta.json before its registration transaction).
/// Decrementing the old hashes against the new row corrupts refcounts;
/// the in-transaction meta re-read must detect the republication and
/// roll the removal back untouched.
#[test]
fn removal_rolls_back_when_the_entry_was_republished_mid_flight() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let src_a = dir.path().join("a.rlib");
    std::fs::write(&src_a, b"generation A").unwrap();
    store
        .put(
            "aba",
            "c",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(src_a, "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();

    let outcome = store
        .remove_entry_guarded_with_hook("aba", None, || {
            // Republish the same key with different content between the
            // removal's meta read and its transaction.
            let src_b = dir.path().join("b.rlib");
            std::fs::write(&src_b, b"generation B, longer content").unwrap();
            store
                .put(
                    "aba",
                    "c",
                    &["lib".into()],
                    &[],
                    "",
                    "dev",
                    &[(src_b, "lib.rlib".into())],
                    "",
                    "",
                )
                .unwrap();
        })
        .unwrap();

    assert!(
        matches!(outcome, GuardedRemoval::Skipped),
        "a removal that lost to a republication must report nothing removed"
    );
    assert!(
        store.contains("aba"),
        "generation B's row must survive the rolled-back removal"
    );
    let meta = store.get("aba").unwrap().expect("B must stay restorable");
    let hash = &meta.files[0].hash;
    assert!(
        store.blob_path(hash).is_file(),
        "generation B's blob must remain on disk"
    );
    let refcount: i64 = store
        .db
        .query_row(
            "SELECT refcount FROM blobs WHERE hash = ?1",
            params![hash],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        refcount, 1,
        "B's refcounts must be untouched by the rollback"
    );
}

/// kunobi-ninja/kache#670, residual window: a same-key publisher whose
/// fresh `meta.json` lands while a removal is between its refcount
/// decrements and its directory cleanup must not have that meta deleted
/// out from under its registration — that strands the publisher's
/// committed row with no artifacts and leaks its refcounts until doctor
/// or an index rebuild.
///
/// Cleanup now runs in its own locked transaction after the logical
/// removal commits, guarded by a republication check, and `put`
/// materializes meta.json inside its registration transaction — so the
/// publisher is serialized to entirely-before the cleanup (the check then
/// sees its row and leaves the directory alone) or entirely-after (it
/// re-creates the directory). The publisher below is released exactly in
/// the old danger window; on the old structure (unlocked, unchecked
/// cleanup) it finishes inside the window and its meta.json is destroyed.
#[test]
fn republication_during_removal_cleanup_is_never_stranded() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let src_a = dir.path().join("a.rlib");
    std::fs::write(&src_a, b"generation A").unwrap();
    store
        .put(
            "key",
            "c",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(src_a, "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();

    // The publisher runs on its own connection and is released inside
    // the removal's cleanup window. With cleanup inside the transaction
    // it blocks on the write lock, the seam's bounded wait expires, and
    // the removal finishes first; the publisher then lands cleanly after.
    // With the old post-commit cleanup the lock is already free, the put
    // completes inside the window, and the cleanup destroys its meta.
    //
    // Synchronization is deadline-bounded atomics, not channels: every
    // wait has a hard cap, so a broken removal path that never reaches
    // the seam degrades into assertion failures instead of a hang — which
    // is what lets the mutation lane kill mutants of the removal instead
    // of timing out on them.
    let released = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    // Proves the interleaving actually happened: without it the test can
    // pass vacuously when the publisher thread is scheduled so late that
    // its put simply runs after the whole removal.
    let attempting = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let published = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let wait_for = |flag: &std::sync::atomic::AtomicBool, cap: Duration| {
        let deadline = std::time::Instant::now() + cap;
        while !flag.load(std::sync::atomic::Ordering::Acquire)
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(5));
        }
    };
    let publisher = {
        let config = config.clone();
        let dir_path = dir.path().to_path_buf();
        let released = std::sync::Arc::clone(&released);
        let attempting = std::sync::Arc::clone(&attempting);
        let published = std::sync::Arc::clone(&published);
        std::thread::spawn(move || {
            let store = Store::open(&config).unwrap();
            store.db.busy_timeout(Duration::from_secs(30)).unwrap();
            let src_b = dir_path.join("b.rlib");
            std::fs::write(&src_b, b"generation B, republished").unwrap();
            // Bounded: if the removal never reaches the seam (a broken
            // removal path), publish anyway so the join terminates and
            // the assertions report the breakage.
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while !released.load(std::sync::atomic::Ordering::Acquire)
                && std::time::Instant::now() < deadline
            {
                std::thread::sleep(Duration::from_millis(5));
            }
            attempting.store(true, std::sync::atomic::Ordering::Release);
            store
                .put(
                    "key",
                    "c",
                    &["lib".into()],
                    &[],
                    "",
                    "dev",
                    &[(src_b, "lib.rlib".into())],
                    "",
                    "",
                )
                .unwrap();
            published.store(true, std::sync::atomic::Ordering::Release);
        })
    };

    let removed = store
        .remove_entry_guarded_with_hooks(
            "key",
            None,
            || {},
            || {
                released.store(true, std::sync::atomic::Ordering::Release);
                // The publisher must have reached its put before cleanup
                // continues, or the "race" never happened and the test
                // proves nothing.
                wait_for(&attempting, Duration::from_secs(5));
                assert!(
                    attempting.load(std::sync::atomic::Ordering::Acquire),
                    "publisher never reached put; the interleaving was not exercised"
                );
                // Give the publisher a real chance to race: on the fixed
                // structure it blocks on the write lock and this expires;
                // on the old structure it completes inside the window.
                wait_for(&published, Duration::from_millis(1500));
            },
        )
        .unwrap();
    publisher.join().unwrap();

    assert!(
        matches!(removed, GuardedRemoval::Reclaimed(_)),
        "the removal owned generation A's row"
    );
    assert!(
        store.contains("key"),
        "generation B's row must be committed"
    );
    assert!(
        store.entry_dir("key").join("meta.json").is_file(),
        "generation B's meta.json must survive the racing removal's cleanup"
    );
    let meta = store.get("key").unwrap().expect("B must be restorable");
    let hash = meta.files[0].hash.clone();
    assert!(
        store.blob_path(&hash).is_file(),
        "generation B's blob must be on disk"
    );
    let refcount: i64 = store
        .db
        .query_row(
            "SELECT refcount FROM blobs WHERE hash = ?1",
            params![hash],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(refcount, 1, "B's refcounts must be intact");
}

/// kunobi-ninja/kache#510: directory-cleanup tolerance is for the
/// lost-the-race case ONLY — a cleanup failure while the directory still
/// exists (permissions, open handles) must surface as an error, not be
/// swallowed as if the competitor had won.
#[cfg(unix)]
#[test]
fn persistent_directory_cleanup_failure_is_an_error() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let src = dir.path().join("x.rlib");
    std::fs::write(&src, b"content").unwrap();
    store
        .put(
            "stuck",
            "c",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(src, "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();

    // An unwritable directory with a file inside makes remove_dir_all
    // fail with a persistent, non-race error. Nested one level down:
    // cleanup's readonly-clearing pass covers entry_dir's immediate
    // children, so a top-level readonly dir would simply be repaired.
    let entry_dir = store.entry_dir("stuck");
    let inner = entry_dir.join("legacy").join("inner");
    std::fs::create_dir_all(&inner).unwrap();
    std::fs::write(inner.join("artifact"), b"x").unwrap();
    std::fs::set_permissions(&inner, std::fs::Permissions::from_mode(0o555)).unwrap();

    // Stash the blob hash BEFORE the removal: remove_dir_all deletes
    // children in unspecified order, so meta.json may or may not survive
    // the failed cleanup.
    let stashed_hash = {
        let meta: EntryMeta =
            serde_json::from_str(&std::fs::read_to_string(entry_dir.join("meta.json")).unwrap())
                .unwrap();
        meta.files[0].hash.clone()
    };

    let err = store.remove_entry("stuck");
    // Restore permissions so the tempdir can be dropped regardless.
    std::fs::set_permissions(&inner, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(
        err.is_err(),
        "a persistent cleanup failure must not be swallowed as a lost race"
    );
    // The failure direction matters (#670): the logical removal commits
    // BEFORE cleanup, so a cleanup failure leaves a deleted row plus
    // partially deleted, unindexed residue — recoverable. Rolling the row
    // back after files were already deleted would manufacture a committed
    // row without artifacts, which is the phantom this function must
    // never produce.
    assert!(
        !store.contains("stuck"),
        "the logical removal must stay committed across a cleanup failure"
    );
    assert!(
        store.blob_path(&stashed_hash).is_file(),
        "blob unlinks run only after directory cleanup succeeds; a failed \
             cleanup leaves the file for the orphan sweep"
    );
}

/// kunobi-ninja/kache#510: two removers racing on the SAME entry must not
/// double-decrement a shared blob's refcount. The victim entry shares its
/// blob with a survivor; a double decrement would take the refcount 2 → 0
/// and unlink a blob the survivor still references. Exactly one remover
/// may report `true`, the loser must report `false` without erroring
/// (directory cleanup is idempotent), and the survivor stays restorable.
/// Deliberately holds no `gc.lock`: the function must be safe on its own,
/// not by caller convention.
#[test]
fn two_removers_on_one_entry_never_double_decrement_shared_blob() {
    const ROUNDS: usize = 25;
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    Store::open(&config).unwrap(); // initialise schema before racing opens

    for round in 0..ROUNDS {
        let content = format!("shared blob for round {round}");
        let victim = format!("victim-{round}");
        let survivor = format!("survivor-{round}");

        let store = Store::open(&config).unwrap();
        for key in [&victim, &survivor] {
            let src = dir.path().join(format!("{key}.rlib"));
            std::fs::write(&src, content.as_bytes()).unwrap();
            store
                .put(
                    key,
                    "c",
                    &["lib".into()],
                    &[],
                    "",
                    "dev",
                    &[(src, "lib.rlib".into())],
                    "",
                    "",
                )
                .unwrap();
        }
        let hash = store.get(&survivor).unwrap().unwrap().files[0].hash.clone();

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let config = test_config(dir.path());
            let victim = victim.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                let store = Store::open(&config).unwrap();
                barrier.wait();
                store.remove_entry_guarded(&victim, None)
            }));
        }
        let removed: Vec<bool> = handles
            .into_iter()
            .map(|h| {
                matches!(
                    h.join().unwrap().expect("losing remover must not error"),
                    GuardedRemoval::Reclaimed(_)
                )
            })
            .collect();
        assert_eq!(
            removed.iter().filter(|&&won| won).count(),
            1,
            "exactly one remover releases the entry (round {round}): {removed:?}"
        );

        let refcount: i64 = store
            .db
            .query_row(
                "SELECT refcount FROM blobs WHERE hash = ?1",
                params![&hash],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            refcount, 1,
            "survivor's shared blob refcount (round {round})"
        );
        assert!(
            store.blob_path(&hash).is_file(),
            "shared blob unlinked out from under the survivor (round {round})"
        );
        assert!(
            store.get(&survivor).unwrap().is_some(),
            "survivor entry must stay restorable (round {round})"
        );
        store.remove_entry(&survivor).unwrap();
    }
}

/// kunobi-ninja/kache#608 (over-eviction): on a dedup-heavy store the
/// logical `SUM(entries.size)` sits far above the physical bytes on disk.
/// With `max_size` between the two, eviction must NOT fire — the disk is
/// comfortable. The pre-#608 trigger compared the logical figure and
/// destroyed rebuild value without reclaiming meaningful space.
#[test]
fn evict_does_not_fire_while_physical_size_is_within_budget() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path());
    // Physical: one 200-byte shared blob. Logical: 400 bytes.
    config.max_size = 300;
    let store = Store::open(&config).unwrap();

    for key in ["dup_a", "dup_b"] {
        let src = dir.path().join(format!("{key}.rlib"));
        std::fs::write(&src, vec![b'x'; 200]).unwrap();
        store
            .put(
                key,
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(src.clone(), "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        store.remove_clone_for_test(&src);
    }
    assert_eq!(store.total_size().unwrap(), 400, "logical double-counts");
    assert_eq!(store.physical_size().unwrap(), 200, "disk holds one copy");

    store
        .db
        .execute(
            "UPDATE entries SET last_accessed = datetime('now', '-1 hour')",
            [],
        )
        .unwrap();
    let stats = store.evict().unwrap();
    assert_eq!(
        stats.entries_evicted, 0,
        "physical 200 <= max 300 (the trigger): nothing to evict"
    );
    assert!(store.contains("dup_a") && store.contains("dup_b"));
}

/// kunobi-ninja/kache#608 (ranking + stop condition): entries whose blobs
/// are all shared free nothing; the sweep must prefer an entry with a
/// unique blob and stop once the bytes *actually* freed satisfy the
/// physical target — not evict the whole shared family because a logical
/// counter said so.
#[test]
fn evict_prefers_and_stops_on_actually_freed_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path());
    config.max_size = 500 * 1024; // target 450 KiB; physical 600 KiB
    let store = Store::open(&config).unwrap();

    // Three entries share one 300-byte blob (logical 900, physical 300)…
    for key in ["shared_a", "shared_b", "shared_c"] {
        let src = dir.path().join(format!("{key}.rlib"));
        std::fs::write(&src, vec![b's'; 300 * 1024]).unwrap();
        store
            .put(
                key,
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(src.clone(), "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        store.remove_clone_for_test(&src);
    }
    // …plus one entry with its own 300-byte blob.
    let src = dir.path().join("unique.rlib");
    std::fs::write(&src, vec![b'u'; 300 * 1024]).unwrap();
    store
        .put(
            "unique",
            "c",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(src.clone(), "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();
    store.remove_clone_for_test(&src);

    assert_eq!(store.physical_size().unwrap(), 600 * 1024);
    store
        .db
        .execute(
            "UPDATE entries SET last_accessed = datetime('now', '-1 hour')",
            [],
        )
        .unwrap();

    let stats = store.evict().unwrap();
    assert_eq!(
        stats.entries_evicted, 1,
        "evicting `unique` frees 300 KiB physical → 300 <= 450 KiB, done"
    );
    assert_eq!(stats.bytes_freed, 300 * 1024);
    assert_eq!(stats.disk_bytes_reclaimed, 300 * 1024);
    assert!(
        !store.contains("unique"),
        "the freeing entry is the one evicted"
    );
    for key in ["shared_a", "shared_b", "shared_c"] {
        assert!(store.contains(key), "{key} frees nothing and must survive");
    }
}

/// kunobi-ninja/kache#710: `evict()` must have a real hysteresis band —
/// fire at the full cap (`max_size`, 100%) and stop at 90% of it. This
/// store sits between the two edges (950 of max 1000, target 900), where a
/// sweep that incorrectly triggers at 90% would evict.
#[test]
fn evict_noop_within_the_hysteresis_band() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path());
    config.max_size = 1000; // target 900; trigger 1000; physical 950
    let store = Store::open(&config).unwrap();

    for i in 0..5 {
        let src = dir.path().join(format!("u{i}.rlib"));
        // Exactly 190 bytes, unique per entry.
        std::fs::write(&src, format!("{i}{}", "x".repeat(189)).as_bytes()).unwrap();
        store
            .put(
                &format!("u{i}"),
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(src, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
    }
    assert_eq!(store.physical_size().unwrap(), 950);
    store
        .db
        .execute(
            "UPDATE entries SET last_accessed = datetime('now', '-1 hour')",
            [],
        )
        .unwrap();

    let stats = store.evict().unwrap();
    assert_eq!(
        stats.entries_evicted, 0,
        "950 is inside the band (900 < 950 <= 1000): evict() must not fire"
    );
    assert_eq!(store.physical_size().unwrap(), 950);
}

/// Once the store crosses the #710 trigger, eviction stops at the 90%
/// target rather than at the trigger or after the whole candidate set.
#[test]
fn evict_fires_at_the_trigger_and_stops_at_the_target() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path());
    config.max_size = 1000; // target 900; trigger 1000
    let store = Store::open(&config).unwrap();

    for i in 0..6 {
        let src = dir.path().join(format!("u{i}.rlib"));
        // Exactly 190 bytes, unique per entry: 6 * 190 = 1140 > 1000.
        std::fs::write(&src, format!("{i}{}", "x".repeat(189)).as_bytes()).unwrap();
        store
            .put(
                &format!("u{i}"),
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(src.clone(), "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        store.remove_clone_for_test(&src);
    }
    assert_eq!(store.physical_size().unwrap(), 1140);
    store
        .db
        .execute(
            "UPDATE entries SET last_accessed = datetime('now', '-1 hour')",
            [],
        )
        .unwrap();

    let stats = store.evict().unwrap();
    assert_eq!(
        stats.entries_evicted, 2,
        "1140 > 1000 must trigger, and two 190-byte evictions reach 760 <= 900"
    );
    assert_eq!(store.physical_size().unwrap(), 760);
}

/// kunobi-ninja/kache#594: a size-driven sweep records every tombstone
/// with the value-density shadow's verdict on the same entry, and the
/// demand stream splits by that verdict. The store here is built so the
/// two policies disagree: the live policy evicts the LARGEST stale entry
/// (huge but expensive to rebuild), while the shadow — ranking by
/// rebuild cost per reclaimable byte — would have kept it and evicted
/// the small cheap one instead.
#[test]
fn size_sweep_records_shadow_verdicts_and_demand_splits() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path());
    config.max_size = 450_000; // target 405_000; physical 500_000
    let store = Store::open(&config).unwrap();

    for (key, bytes, fill, compile_ms) in [
        ("huge_expensive", 400_000usize, b'a', 60_000u64),
        ("small_cheap", 100_000usize, b'b', 1u64),
    ] {
        let src = dir.path().join(format!("{key}.rlib"));
        std::fs::write(&src, vec![fill; bytes]).unwrap();
        store
            .put_with_compile_time(
                key,
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(src.clone(), "lib.rlib".into())],
                "",
                "",
                compile_ms,
            )
            .unwrap();
        store.remove_clone_for_test(&src);
    }
    store
        .db
        .execute(
            "UPDATE entries SET last_accessed = datetime('now', '-1 hour')",
            [],
        )
        .unwrap();

    let stats = store.evict().unwrap();
    assert_eq!(
        stats.entries_evicted, 1,
        "the live policy evicts the largest entry and reaches its target"
    );
    assert!(!store.contains("huge_expensive"));
    assert!(store.contains("small_cheap"));

    // The tombstone carries the shadow's dissent: for the same 95 KB
    // budget the value-density ranking would have taken small_cheap
    // (density ~10 ms/MB) and kept huge_expensive (~150,000 ms/MB).
    let (shadow_policy, shadow_would_evict): (String, i64) = store
        .db
        .query_row(
            "SELECT shadow_policy, shadow_would_evict FROM eviction_tombstones
                 WHERE cache_key = 'huge_expensive'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(shadow_policy, "value-density");
    assert_eq!(shadow_would_evict, 0, "the shadow would have kept it");

    // Demand on the evicted key lands in the shadow-kept cohort — the
    // shadow's save, had it been live.
    assert!(store.get("huge_expensive").unwrap().is_none());
    assert_eq!(
        store.shadow_demand_split().unwrap(),
        ShadowDemandSplit {
            agreed: 0,
            agreed_demanded: 0,
            shadow_kept: 1,
            shadow_kept_demanded: 1,
        }
    );

    // Pin the split query's cohort handling: an agreed row counts, a
    // pre-shadow row (NULL verdict) enters neither cohort, and an
    // unknown-cost row is recorded but excluded from the headline —
    // the density shadow ranks unknown cost as worthless by
    // construction, so counting it would bias the comparison.
    store
            .db
            .execute_batch(
                "INSERT INTO eviction_tombstones
                    (cache_key, policy, compile_time_ms, shadow_policy, shadow_would_evict, demanded_at)
                 VALUES ('agreed_row', 'size-pressure', 500, 'value-density', 1, datetime('now'));
                 INSERT INTO eviction_tombstones (cache_key, policy, compile_time_ms)
                 VALUES ('pre_shadow_row', 'size-pressure', 500);
                 INSERT INTO eviction_tombstones
                    (cache_key, policy, compile_time_ms, shadow_policy, shadow_would_evict)
                 VALUES ('unknown_cost_row', 'size-pressure', 0, 'value-density', 0);",
            )
            .unwrap();
    assert_eq!(
        store.shadow_demand_split().unwrap(),
        ShadowDemandSplit {
            agreed: 1,
            agreed_demanded: 1,
            shadow_kept: 1,
            shadow_kept_demanded: 1,
        }
    );
}

/// kunobi-ninja/kache#608 (honest accounting): a sweep over a fully-shared
/// family reports the physical bytes it freed (once, when the last
/// reference goes), not the logical sum of the evicted entries.
#[test]
fn evict_reports_physical_bytes_freed_not_logical() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path());
    config.max_size = 200; // target 180; physical 300 → must evict all three
    let store = Store::open(&config).unwrap();

    for key in ["a", "b", "c"] {
        let src = dir.path().join(format!("{key}.rlib"));
        std::fs::write(&src, vec![b'z'; 300]).unwrap();
        store
            .put(
                key,
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(src.clone(), "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        store.remove_clone_for_test(&src);
    }
    store
        .db
        .execute(
            "UPDATE entries SET last_accessed = datetime('now', '-1 hour')",
            [],
        )
        .unwrap();

    let stats = store.evict().unwrap();
    assert_eq!(
        stats.entries_evicted, 3,
        "zero-freeing removals must not stop the sweep early"
    );
    assert_eq!(stats.bytes_freed, 300, "the blob's bytes are freed once");
    assert_eq!(stats.disk_bytes_reclaimed, 300);
    assert_eq!(stats.blobs_removed, 1);
    assert_eq!(store.physical_size().unwrap(), 0);
}

/// kunobi-ninja/kache#608: pre-#608 stores have no `entry_blobs` rows;
/// the GC-sweep backfill reconstructs them from meta.json, bounded and
/// convergent, and candidates go from unknown (rank on logical size) to
/// exact marginal-reclaimable bytes.
#[test]
fn backfill_entry_blobs_reconstructs_marginal_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    for (key, content) in [("shared_a", b'x'), ("shared_b", b'x'), ("solo", b'y')] {
        let src = dir.path().join(format!("{key}.rlib"));
        std::fs::write(&src, vec![content; 100]).unwrap();
        store
            .put(
                key,
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(src, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
    }
    // Simulate a store written before the table existed.
    store.db.execute("DELETE FROM entry_blobs", []).unwrap();

    let unknowns = store.eviction_candidates().unwrap();
    assert!(
        unknowns.iter().all(|f| f.reclaimable_bytes.is_none()),
        "un-backfilled entries must report unknown, not zero"
    );

    assert_eq!(store.backfill_entry_blobs().unwrap(), 3);
    assert_eq!(store.backfill_entry_blobs().unwrap(), 0, "converges");

    let features = store.eviction_candidates().unwrap();
    let by_key: std::collections::HashMap<&str, &crate::eviction::EntryFeatures> =
        features.iter().map(|f| (f.key.as_str(), f)).collect();
    assert_eq!(by_key["shared_a"].reclaimable_bytes, Some(0));
    assert_eq!(by_key["shared_b"].reclaimable_bytes, Some(0));
    assert_eq!(by_key["solo"].reclaimable_bytes, Some(100));
}

#[test]
fn test_clear_removes_blobs_too() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output = dir.path().join("lib.rlib");
    fs::write(&output, b"content").unwrap();
    store
        .put(
            "k1",
            "c",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output, "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();

    store.clear().unwrap();

    // Blobs dir should be empty or gone
    let blobs_dir = store.blobs_dir();
    if blobs_dir.exists() {
        let has_files = fs::read_dir(&blobs_dir)
            .unwrap()
            .flatten()
            .any(|e| e.path().is_dir());
        assert!(
            !has_files,
            "blobs dir should have no shard subdirs after clear"
        );
    }

    // Blobs table should be empty
    let count: i64 = store
        .db
        .query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn test_get_lazily_migrates_legacy_entry() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    // Simulate a legacy entry: artifacts in entry dir, no blobs
    let entry_dir = config.store_dir().join("old_key");
    fs::create_dir_all(&entry_dir).unwrap();
    let content = b"old format artifact";
    fs::write(entry_dir.join("lib.rlib"), content).unwrap();

    let hash = crate::file_hash::hash_file(&entry_dir.join("lib.rlib")).unwrap();
    let meta = EntryMeta {
        cache_key: "old_key".to_string(),
        key_schema: kache_format::CACHE_KEY_VERSION,
        crate_name: "old_crate".to_string(),
        crate_types: vec!["lib".to_string()],
        files: vec![CachedFile {
            name: "lib.rlib".to_string(),
            size: content.len() as u64,
            hash: hash.clone(),
            executable: false,
        }],
        stdout: String::new(),
        stderr: String::new(),
        features: vec![],
        target: String::new(),
        profile: "dev".to_string(),
        compile_time_ms: 0,
        emit_kinds: Vec::new(),
    };
    fs::write(
        entry_dir.join("meta.json"),
        serde_json::to_string_pretty(&meta).unwrap(),
    )
    .unwrap();
    store
            .db
            .execute(
                "INSERT INTO entries (cache_key, crate_name, size, committed) VALUES ('old_key', 'old_crate', ?1, 1)",
                params![content.len() as i64],
            )
            .unwrap();

    // get() should transparently migrate the entry
    let result = store.get("old_key").unwrap();
    assert!(result.is_some());

    // Blob should now exist
    let blob = store.blob_path(&hash);
    assert!(
        blob.exists(),
        "get() should have migrated artifact to blob store"
    );

    // Artifact should be gone from entry dir
    assert!(!entry_dir.join("lib.rlib").exists());
}

#[test]
fn get_evicts_when_lazy_legacy_migration_fails() {
    // Covers get lazy-migration error warning branch.
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let entry_dir = config.store_dir().join("old_bad_key");
    fs::create_dir_all(&entry_dir).unwrap();
    let artifact = entry_dir.join("lib.rlib");
    fs::write(&artifact, b"old format artifact").unwrap();
    let hash = crate::file_hash::hash_file(&artifact).unwrap();
    let meta = EntryMeta {
        cache_key: "old_bad_key".to_string(),
        key_schema: kache_format::CACHE_KEY_VERSION,
        crate_name: "old_bad_crate".to_string(),
        crate_types: vec!["lib".to_string()],
        files: vec![CachedFile {
            name: "lib.rlib".to_string(),
            size: fs::metadata(&artifact).unwrap().len(),
            hash: hash.clone(),
            executable: false,
        }],
        stdout: String::new(),
        stderr: String::new(),
        features: vec![],
        target: String::new(),
        profile: "dev".to_string(),
        compile_time_ms: 0,
        emit_kinds: Vec::new(),
    };
    fs::write(
        entry_dir.join("meta.json"),
        serde_json::to_string_pretty(&meta).unwrap(),
    )
    .unwrap();
    store
        .db
        .execute(
            "INSERT INTO entries (cache_key, crate_name, size, committed) \
                 VALUES ('old_bad_key', 'old_bad_crate', ?1, 1)",
            params![fs::metadata(&artifact).unwrap().len() as i64],
        )
        .unwrap();

    let shard_path = store.blobs_dir().join(&hash[..2]);
    fs::create_dir_all(store.blobs_dir()).unwrap();
    fs::write(&shard_path, b"not a shard directory").unwrap();

    let result = store.get("old_bad_key").unwrap();

    assert!(
        result.is_none(),
        "failed migration falls through to eviction"
    );
    assert!(!store.contains("old_bad_key"));
    assert!(shard_path.is_file(), "unrelated shard conflict remains");
}

#[test]
fn test_migrate_to_blobs_bulk() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let content = b"shared artifact bytes";
    let hash = {
        let tmp = dir.path().join("tmp");
        fs::write(&tmp, content).unwrap();
        crate::file_hash::hash_file(&tmp).unwrap()
    };

    // Create two legacy entries with identical content
    for key in &["old1", "old2"] {
        let entry_dir = config.store_dir().join(key);
        fs::create_dir_all(&entry_dir).unwrap();
        fs::write(entry_dir.join("lib.rlib"), content).unwrap();

        let meta = EntryMeta {
            cache_key: key.to_string(),
            key_schema: kache_format::CACHE_KEY_VERSION,
            crate_name: "shared_crate".to_string(),
            crate_types: vec!["lib".to_string()],
            files: vec![CachedFile {
                name: "lib.rlib".to_string(),
                size: content.len() as u64,
                hash: hash.clone(),
                executable: false,
            }],
            stdout: String::new(),
            stderr: String::new(),
            features: vec![],
            target: String::new(),
            profile: "dev".to_string(),
            compile_time_ms: 0,
            emit_kinds: Vec::new(),
        };
        fs::write(
            entry_dir.join("meta.json"),
            serde_json::to_string_pretty(&meta).unwrap(),
        )
        .unwrap();
        store
                .db
                .execute(
                    &format!(
                        "INSERT INTO entries (cache_key, crate_name, size, committed) VALUES ('{key}', 'shared_crate', {}, 1)",
                        content.len()
                    ),
                    [],
                )
                .unwrap();
    }

    let stats = store.migrate_to_blobs(|_, _| {}).unwrap();
    assert_eq!(stats.entries_migrated, 2);
    assert_eq!(store.backfill_entry_blobs().unwrap(), 2);
    assert_blob_refs_match_mappings(&store);

    // Refcount should be 2
    let refcount: i64 = store
        .db
        .query_row(
            "SELECT refcount FROM blobs WHERE hash = ?1",
            params![hash],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(refcount, 2);
}

#[test]
fn test_blob_stats() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    // Empty store
    let stats = store.blob_stats().unwrap();
    assert_eq!(stats.total_blobs, 0);
    assert_eq!(stats.savings, 0);

    // Add two entries with same content
    let output = dir.path().join("lib.rlib");
    rewrite_source(&output, b"shared content!");
    store
        .put(
            "k1",
            "c",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output.clone(), "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();
    rewrite_source(&output, b"shared content!");
    store
        .put(
            "k2",
            "c",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(output, "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();

    let stats = store.blob_stats().unwrap();
    assert_eq!(stats.total_blobs, 1); // one unique blob
    assert!(stats.total_logical_size > stats.total_blob_size); // dedup savings
    assert!(stats.savings > 0);
}

// =========================================================================
// Comprehensive dedup integration tests
// =========================================================================

/// Helper: create a temp file with given content and return its path.
fn write_temp_file(dir: &Path, name: &str, content: &[u8]) -> PathBuf {
    let path = dir.join(name);
    rewrite_source(&path, content);
    path
}

/// (Re)write a source file that an earlier `put` may have turned into a
/// read-only hardlink of a store blob (non-CoW filesystems): unlink
/// first, so the write neither fails with EACCES as an unprivileged user
/// nor reaches the blob through the shared inode.
fn rewrite_source(path: &Path, content: &[u8]) {
    let _ = fs::remove_file(path);
    fs::write(path, content).unwrap();
}

/// Helper: read meta.json for a cache key and return the EntryMeta.
fn read_meta(store: &Store, cache_key: &str) -> EntryMeta {
    let meta_path = store.entry_dir(cache_key).join("meta.json");
    let content = fs::read_to_string(&meta_path).unwrap();
    serde_json::from_str(&content).unwrap()
}

/// Helper: query refcount for a blob hash, returns None if blob doesn't exist in DB.
fn blob_refcount(store: &Store, hash: &str) -> Option<i64> {
    store
        .db
        .query_row(
            "SELECT refcount FROM blobs WHERE hash = ?1",
            params![hash],
            |row| row.get(0),
        )
        .ok()
}

/// Helper: count rows in blobs table.
fn blob_table_count(store: &Store) -> i64 {
    store
        .db
        .query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get(0))
        .unwrap()
}

#[test]
fn test_full_dedup_lifecycle() {
    // Put two entries with some shared and some unique files.
    // Verify blobs exist and refcounts are correct.
    // Remove one entry — shared blobs still exist (refcount decremented).
    // Remove second entry — all blobs are deleted (refcount 0).
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    // Shared content between entries 1 and 2
    let shared = write_temp_file(dir.path(), "shared.rlib", b"shared artifact data");
    // Unique content for entry 1
    let unique1 = write_temp_file(dir.path(), "unique1.rlib", b"unique to entry 1");
    // Unique content for entry 2
    let unique2 = write_temp_file(dir.path(), "unique2.rlib", b"unique to entry 2");

    // Put entry 1: shared + unique1
    store
        .put(
            "entry1",
            "crate_a",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[
                (shared.clone(), "shared.rlib".into()),
                (unique1, "unique1.rlib".into()),
            ],
            "",
            "",
        )
        .unwrap();

    // Re-create shared file (put() reads from source path, content must exist)
    rewrite_source(&shared, b"shared artifact data");

    // Put entry 2: shared + unique2
    store
        .put(
            "entry2",
            "crate_b",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[
                (shared, "shared.rlib".into()),
                (unique2, "unique2.rlib".into()),
            ],
            "",
            "",
        )
        .unwrap();

    // Read metadata to get hashes
    let meta1 = read_meta(&store, "entry1");
    let meta2 = read_meta(&store, "entry2");
    let shared_hash = &meta1
        .files
        .iter()
        .find(|f| f.name == "shared.rlib")
        .unwrap()
        .hash;
    let unique1_hash = &meta1
        .files
        .iter()
        .find(|f| f.name == "unique1.rlib")
        .unwrap()
        .hash;
    let unique2_hash = &meta2
        .files
        .iter()
        .find(|f| f.name == "unique2.rlib")
        .unwrap()
        .hash;

    // Shared blob should have the same hash in both entries
    let shared_hash2 = &meta2
        .files
        .iter()
        .find(|f| f.name == "shared.rlib")
        .unwrap()
        .hash;
    assert_eq!(shared_hash, shared_hash2);

    // Verify refcounts: shared=2, unique1=1, unique2=1
    assert_eq!(blob_refcount(&store, shared_hash), Some(2));
    assert_blob_refs_match_mappings(&store);
    assert_eq!(blob_refcount(&store, unique1_hash), Some(1));
    assert_eq!(blob_refcount(&store, unique2_hash), Some(1));

    // All blob files should exist on disk
    assert!(store.blob_path(shared_hash).exists());
    assert!(store.blob_path(unique1_hash).exists());
    assert!(store.blob_path(unique2_hash).exists());

    // Remove entry 1 — shared blob should still exist, unique1 blob should be gone
    store.remove_entry("entry1").unwrap();
    assert_eq!(blob_refcount(&store, shared_hash), Some(1));
    assert!(store.blob_path(shared_hash).exists());
    assert!(!store.blob_path(unique1_hash).exists());
    assert_eq!(blob_refcount(&store, unique1_hash), None);

    // Remove entry 2 — everything should be gone
    store.remove_entry("entry2").unwrap();
    assert!(!store.blob_path(shared_hash).exists());
    assert!(!store.blob_path(unique2_hash).exists());
    assert_eq!(blob_refcount(&store, shared_hash), None);
    assert_eq!(blob_refcount(&store, unique2_hash), None);
    assert_eq!(blob_table_count(&store), 0);
}

#[test]
fn gc_lock_is_mutually_exclusive() {
    // kunobi-ninja/kache#326: the cross-process GC lock admits one holder at
    // a time and is re-acquirable after release.
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let first = store.try_gc_lock().unwrap();
    assert!(first.is_some(), "first GC lock acquires");
    assert!(
        store.try_gc_lock().unwrap().is_none(),
        "a second GC lock is refused while the first is held"
    );
    drop(first);
    assert!(
        store.try_gc_lock().unwrap().is_some(),
        "the GC lock is re-acquirable after release"
    );
}

const TWO_HOURS: Duration = Duration::from_secs(7200);

/// A key lock file last claimed `age` ago.
fn aged_key_lock(config: &Config, key: &str, age: Duration) -> PathBuf {
    let path = config.store_dir().join(format!("{key}.lock"));
    std::fs::create_dir_all(config.store_dir()).unwrap();
    std::fs::write(&path, b"1").unwrap();
    set_age(&path, age);
    path
}

fn set_age(path: &Path, age: Duration) {
    let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    file.set_modified(std::time::SystemTime::now() - age)
        .unwrap();
}

fn sweep_key_locks(store: &Store, cap: usize) -> KeyLockSweepStats {
    store
        .sweep_stale_key_locks(KEY_LOCK_SWEEP_GRACE, cap)
        .unwrap()
}

#[test]
fn key_lock_sweep_constants_are_pinned() {
    assert_eq!(KEY_LOCK_SWEEP_GRACE, Duration::from_secs(3600));
    assert_eq!(KEY_LOCK_SWEEP_GRACE, STAGING_SWEEP_GRACE);
    assert_eq!(KEY_LOCK_SWEEP_CAP, 20_000);
    assert_eq!(LOCK_OPEN_ATTEMPTS, 4);
    // The CI store that prompted the sweep: 84,496 stale locks.
    assert_eq!(84_496usize.div_ceil(KEY_LOCK_SWEEP_CAP), 5);
}

#[test]
fn key_of_lock_name_accepts_only_a_valid_key_with_the_lock_suffix() {
    let k = key(1);
    assert_eq!(key_of_lock_name(&format!("{k}.lock")), Some(k.as_str()));
    assert_eq!(key_of_lock_name(&k), None);
    assert_eq!(key_of_lock_name("gc.lock"), None);
    assert_eq!(key_of_lock_name("durability.lock"), None);
    assert_eq!(key_of_lock_name(".lock"), None);
    assert_eq!(key_of_lock_name(&format!("{k}.lock.tmp")), None);
    assert_eq!(key_of_lock_name(&format!("{}.lock", &k[1..])), None);
    assert_eq!(
        key_of_lock_name(&format!("{}.lock", k.to_uppercase())),
        None
    );
}

#[test]
fn key_lock_staleness_boundary_is_inclusive_and_future_mtimes_are_young() {
    let now = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
    let grace = Duration::from_secs(3600);
    assert!(!key_lock_is_stale(
        now - Duration::from_secs(3599),
        now,
        grace
    ));
    assert!(key_lock_is_stale(now - grace, now, grace));
    assert!(key_lock_is_stale(now - TWO_HOURS, now, grace));
    assert!(!key_lock_is_stale(now + TWO_HOURS, now, grace));
}

#[test]
fn key_lock_sweep_stats_report_what_remains() {
    let stats = KeyLockSweepStats {
        seen: 10,
        removed: 3,
    };
    assert_eq!(stats.remaining(), 7);
    let none = KeyLockSweepStats {
        seen: 0,
        removed: 0,
    };
    assert_eq!(none.remaining(), 0);
}

#[test]
fn key_lock_sweep_removes_a_stale_lock_whose_key_has_no_entry() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let stale = aged_key_lock(&config, &key(1), TWO_HOURS);

    assert_eq!(
        sweep_key_locks(&store, KEY_LOCK_SWEEP_CAP),
        KeyLockSweepStats {
            seen: 1,
            removed: 1
        }
    );
    assert!(!stale.exists());
    // The key is claimable again, on a new file.
    let lock = store.try_lock(&key(1)).unwrap();
    assert!(lock.is_some());
    assert!(stale.exists());
}

#[test]
fn key_lock_sweep_removes_the_lock_of_an_evicted_key() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let k = put_entry(&store, dir.path(), 1, "evicted", b"payload");
    drop(store.try_lock(&k).unwrap().expect("claim the key"));
    let lock_path = config.store_dir().join(format!("{k}.lock"));
    set_age(&lock_path, TWO_HOURS);

    // Kept while the entry is live.
    assert_eq!(
        sweep_key_locks(&store, KEY_LOCK_SWEEP_CAP),
        KeyLockSweepStats {
            seen: 1,
            removed: 0
        }
    );
    assert!(lock_path.exists());

    store.remove_entry(&k).unwrap();
    assert_eq!(
        sweep_key_locks(&store, KEY_LOCK_SWEEP_CAP),
        KeyLockSweepStats {
            seen: 1,
            removed: 1
        }
    );
    assert!(!lock_path.exists());
}

#[test]
fn key_lock_sweep_keeps_a_young_lock() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let young = aged_key_lock(&config, &key(1), Duration::from_secs(3000));
    let old = aged_key_lock(&config, &key(2), Duration::from_secs(4200));

    assert_eq!(
        sweep_key_locks(&store, KEY_LOCK_SWEEP_CAP),
        KeyLockSweepStats {
            seen: 2,
            removed: 1
        }
    );
    assert!(young.exists());
    assert!(!old.exists());
}

#[test]
fn key_lock_sweep_never_removes_a_held_lock() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let held = store.try_lock(&key(1)).unwrap().expect("claim the key");
    let path = config.store_dir().join(format!("{}.lock", key(1)));
    let before = kache_fs::file_identity(&path).unwrap();
    // A compile that has been running for two hours.
    set_age(&path, TWO_HOURS);

    // Swept from another thread, as the daemon would from another process.
    let stats = std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let sweeper = Store::open(&config).unwrap();
                sweep_key_locks(&sweeper, KEY_LOCK_SWEEP_CAP)
            })
            .join()
            .unwrap()
    });
    assert_eq!(
        stats,
        KeyLockSweepStats {
            seen: 1,
            removed: 0
        }
    );
    assert_eq!(kache_fs::file_identity(&path).unwrap(), before);
    assert!(
        store.try_lock(&key(1)).unwrap().is_none(),
        "the holder still excludes every other claimant"
    );

    drop(held);
    set_age(&path, TWO_HOURS);
    assert_eq!(sweep_key_locks(&store, KEY_LOCK_SWEEP_CAP).removed, 1);
}

#[test]
fn key_lock_sweep_respects_the_cap_and_converges() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    for seed in 0..7 {
        aged_key_lock(&config, &key(seed), TWO_HOURS);
    }

    assert_eq!(
        sweep_key_locks(&store, 3),
        KeyLockSweepStats {
            seen: 7,
            removed: 3
        }
    );
    assert_eq!(
        sweep_key_locks(&store, 3),
        KeyLockSweepStats {
            seen: 4,
            removed: 3
        }
    );
    let last = sweep_key_locks(&store, 3);
    assert_eq!(
        last,
        KeyLockSweepStats {
            seen: 1,
            removed: 1
        }
    );
    assert_eq!(last.remaining(), 0);
    assert_eq!(
        sweep_key_locks(&store, 0),
        KeyLockSweepStats::default(),
        "nothing left"
    );
}

#[test]
fn key_lock_sweep_with_a_zero_cap_only_counts() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let stale = aged_key_lock(&config, &key(1), TWO_HOURS);
    assert_eq!(
        sweep_key_locks(&store, 0),
        KeyLockSweepStats {
            seen: 1,
            removed: 0
        }
    );
    assert!(stale.exists());
}

#[test]
fn key_lock_sweep_never_touches_gc_lock() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    drop(store.try_gc_lock().unwrap());
    let path = config.store_dir().join("gc.lock");
    set_age(&path, TWO_HOURS);
    assert_eq!(
        sweep_key_locks(&store, KEY_LOCK_SWEEP_CAP),
        KeyLockSweepStats::default()
    );
    assert!(path.exists());
}

#[test]
fn key_lock_sweep_never_touches_durability_lock() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    drop(store.try_durability_flush_lock().unwrap());
    let path = config.store_dir().join("durability.lock");
    set_age(&path, TWO_HOURS);
    assert_eq!(
        sweep_key_locks(&store, KEY_LOCK_SWEEP_CAP),
        KeyLockSweepStats::default()
    );
    assert!(path.exists());
}

#[test]
fn key_lock_sweep_never_touches_a_directory_named_like_a_key_lock() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let path = config.store_dir().join(format!("{}.lock", key(1)));
    std::fs::create_dir_all(&path).unwrap();
    std::fs::write(path.join("inside"), b"kept").unwrap();
    assert_eq!(
        sweep_key_locks(&store, KEY_LOCK_SWEEP_CAP),
        KeyLockSweepStats {
            seen: 1,
            removed: 0
        }
    );
    assert!(path.join("inside").exists());
}

#[test]
fn key_lock_sweep_never_touches_an_entry_directory_or_other_files() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let k = put_entry(&store, dir.path(), 1, "kept", b"payload");
    // Not a key: too short, and a stray file beside the locks.
    let short = config.store_dir().join("abc123.lock");
    let stray = config.store_dir().join(format!("{}.lock.bak", key(2)));
    for path in [&short, &stray] {
        std::fs::write(path, b"1").unwrap();
        set_age(path, TWO_HOURS);
    }
    assert_eq!(
        sweep_key_locks(&store, KEY_LOCK_SWEEP_CAP),
        KeyLockSweepStats::default()
    );
    assert!(store.entry_dir(&k).join("meta.json").exists());
    assert!(short.exists());
    assert!(stray.exists());
}

#[cfg(unix)]
#[test]
fn key_lock_sweep_never_follows_a_symlink_named_like_a_key_lock() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let target = dir.path().join("elsewhere");
    std::fs::write(&target, b"kept").unwrap();
    set_age(&target, TWO_HOURS);
    let link = config.store_dir().join(format!("{}.lock", key(1)));
    std::os::unix::fs::symlink(&target, &link).unwrap();
    assert_eq!(sweep_key_locks(&store, KEY_LOCK_SWEEP_CAP).removed, 0);
    // Nor once the link itself is old enough.
    let later = std::time::SystemTime::now() + TWO_HOURS;
    assert!(!remove_stale_lock_file(
        &link,
        KEY_LOCK_SWEEP_GRACE,
        later,
        || {}
    ));
    assert!(link.symlink_metadata().is_ok());
    assert!(target.exists());
}

#[test]
fn key_lock_sweep_on_a_store_with_no_directory_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let _ = std::fs::remove_dir_all(config.store_dir());
    assert_eq!(
        sweep_key_locks(&store, KEY_LOCK_SWEEP_CAP),
        KeyLockSweepStats::default()
    );
}

#[test]
fn housekeeping_sweeps_key_locks_and_prunes_predictions() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    // Nothing to do, as on a fresh store with predictions off.
    assert_eq!(store.sweep_housekeeping(), HousekeepingStats::default());

    let stale = aged_key_lock(&config, &key(1), TWO_HOURS);
    aged_key_lock(&config, &key(2), TWO_HOURS);
    let young = aged_key_lock(&config, &key(3), Duration::ZERO);
    let predictions = store.file_hash_cache();
    for identity in ["unused", "live"] {
        predictions
            .put_input_prediction(identity, 1, None, "payload")
            .unwrap();
    }
    store
        .db
        .execute(
            "UPDATE input_predictions SET last_used = 1 WHERE identity = 'unused'",
            [],
        )
        .unwrap();
    for (path, written) in [
        ("/old", "2000-01-01 00:00:00"),
        ("/new", "9999-01-01 00:00:00"),
    ] {
        store
            .db
            .execute(
                "INSERT INTO file_hashes (path, size, mtime_ns, hash, updated_at)
                     VALUES (?1, 1, 1, 'h', ?2)",
                rusqlite::params![path, written],
            )
            .unwrap();
    }

    assert_eq!(
        store.sweep_housekeeping(),
        HousekeepingStats {
            key_locks_removed: 2,
            key_locks_remaining: 1,
            predictions_pruned: 1,
            file_hashes_pruned: 1,
        }
    );
    assert!(!stale.exists());
    assert!(young.exists());
    assert!(predictions.get_input_prediction("live").unwrap().is_some());
    assert_eq!(predictions.get_input_prediction("unused").unwrap(), None);
}

/// A store from before #1206 has a rowid `file_hashes`; the sweep
/// rebuilds it after pruning, keeping the rows the prune kept.
#[test]
fn housekeeping_rebuilds_an_older_file_hashes_table_without_rowid() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    store
        .db
        .execute_batch(
            "DROP TABLE file_hashes;
                 CREATE TABLE file_hashes (
                     path TEXT PRIMARY KEY, size INTEGER NOT NULL, mtime_ns INTEGER NOT NULL,
                     ctime_ns INTEGER NOT NULL DEFAULT 0, inode INTEGER NOT NULL DEFAULT 0,
                     hash TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT (datetime('now')));
                 INSERT INTO file_hashes (path, size, mtime_ns, hash, updated_at)
                     VALUES ('/old', 1, 1, 'h', '2000-01-01 00:00:00'),
                            ('/new', 1, 1, 'h', '9999-01-01 00:00:00');",
        )
        .unwrap();
    let table_sql = || -> String {
        store
            .db
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'file_hashes'",
                [],
                |row| row.get(0),
            )
            .unwrap()
    };
    assert!(!table_sql().contains("WITHOUT ROWID"));

    assert_eq!(store.sweep_housekeeping().file_hashes_pruned, 1);
    assert!(table_sql().contains("WITHOUT ROWID"), "{}", table_sql());
    let paths: Vec<String> = store
        .db
        .prepare("SELECT path FROM file_hashes")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(paths, ["/new"]);
}

#[test]
fn stale_lock_claimed_between_listing_and_lock_is_kept() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let path = aged_key_lock(&config, &key(1), TWO_HOURS);
    let now = std::time::SystemTime::now();
    // A claimant takes and releases the key after the sweep's stat.
    let removed = remove_stale_lock_file(&path, KEY_LOCK_SWEEP_GRACE, now, || {
        drop(StoreLock::try_acquire(&path).unwrap().expect("claim"));
    });
    assert!(!removed);
    assert!(path.exists());
    // Left alone, the same file goes.
    set_age(&path, TWO_HOURS);
    assert!(remove_stale_lock_file(
        &path,
        KEY_LOCK_SWEEP_GRACE,
        now,
        || {}
    ));
    assert!(!path.exists());
}

#[test]
fn stale_lock_replaced_under_the_sweep_keeps_the_new_file() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let path = aged_key_lock(&config, &key(1), TWO_HOURS);
    let now = std::time::SystemTime::now();
    let removed = remove_stale_lock_file(&path, KEY_LOCK_SWEEP_GRACE, now, || {
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, b"2").unwrap();
    });
    assert!(!removed);
    assert_eq!(std::fs::read(&path).unwrap(), b"2");
}

#[test]
fn stale_lock_held_at_lock_time_is_kept() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let path = aged_key_lock(&config, &key(1), TWO_HOURS);
    let now = std::time::SystemTime::now() + TWO_HOURS + TWO_HOURS;
    let mut held = None;
    let removed = remove_stale_lock_file(&path, KEY_LOCK_SWEEP_GRACE, now, || {
        held = StoreLock::try_acquire(&path).unwrap();
    });
    assert!(held.is_some());
    assert!(!removed);
    assert!(path.exists());
}

#[test]
fn lock_is_current_compares_identities_and_trusts_a_handle_without_one() {
    let id = |ino| kache_fs::InodeId { dev: 1, ino };
    let missing = || std::io::Error::from(std::io::ErrorKind::NotFound);
    assert!(lock_is_current(Ok(id(7)), Ok(id(7))));
    assert!(!lock_is_current(Ok(id(7)), Ok(id(8))));
    assert!(!lock_is_current(Ok(id(7)), Err(missing())));
    assert!(lock_is_current(Err(missing()), Ok(id(7))));
    assert!(lock_is_current(Err(missing()), Err(missing())));
}

#[test]
fn lock_file_is_at_path_needs_the_same_file_at_the_path() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.lock");
    let other = dir.path().join("b.lock");
    std::fs::write(&path, b"1").unwrap();
    std::fs::write(&other, b"1").unwrap();
    let file = std::fs::File::open(&path).unwrap();
    assert!(lock_file_is_at_path(&file, &path));
    assert!(!lock_file_is_at_path(&file, &other));
    assert!(!lock_file_is_at_path(&file, &dir.path().join("missing")));
}

/// The race the sweep opens: the path is unlinked and recreated after a
/// claimant opened it and before the claimant locks it.
#[test]
fn acquire_reopens_when_the_lock_file_was_replaced_between_open_and_lock() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("key.lock");
    let mut opens = 0;
    let lock = StoreLock::acquire_current(&path, StoreLock::try_lock_file, || {
        opens += 1;
        if opens == 1 {
            std::fs::remove_file(&path).unwrap();
            std::fs::write(&path, b"another claimant").unwrap();
        }
    })
    .unwrap()
    .expect("lock acquired");
    assert_eq!(opens, 2);
    // The lock is on the file the path names now, so it excludes others.
    assert_eq!(
        kache_fs::handle_identity(&lock.file).unwrap(),
        kache_fs::file_identity(&path).unwrap()
    );
    assert!(StoreLock::try_acquire(&path).unwrap().is_none());
    drop(lock);
    // Read after release: Windows refuses reads of a locked range.
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        std::process::id().to_string()
    );
    assert!(StoreLock::try_acquire(&path).unwrap().is_some());
}

#[test]
fn acquire_reopens_when_the_lock_file_was_unlinked_between_open_and_lock() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("key.lock");
    let mut opens = 0;
    let lock = StoreLock::acquire_current(&path, StoreLock::try_lock_file, || {
        opens += 1;
        if opens == 1 {
            std::fs::remove_file(&path).unwrap();
        }
    })
    .unwrap()
    .expect("lock acquired");
    assert_eq!(opens, 2);
    assert!(path.exists());
    assert!(StoreLock::try_acquire(&path).unwrap().is_none());
    drop(lock);
}

#[test]
fn acquire_yields_to_the_claimant_holding_the_replacement_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("key.lock");
    let mut winner = None;
    let mut opens = 0;
    let lock = StoreLock::acquire_current(&path, StoreLock::try_lock_file, || {
        opens += 1;
        if opens == 1 {
            std::fs::remove_file(&path).unwrap();
            winner = StoreLock::try_acquire(&path).unwrap();
        }
    })
    .unwrap();
    assert!(winner.is_some());
    assert!(lock.is_none(), "only one claimant may hold the key");
    assert_eq!(opens, 2);
}

#[test]
fn acquire_gives_up_after_a_bounded_number_of_replacements() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("key.lock");
    let mut opens = 0u32;
    let result = StoreLock::acquire_current(&path, StoreLock::try_lock_file, || {
        opens += 1;
        std::fs::remove_file(&path).unwrap();
    });
    assert!(result.is_err());
    assert_eq!(opens, LOCK_OPEN_ATTEMPTS);
}

#[test]
fn blocking_acquire_checks_the_path_too() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("gc.lock");
    let lock = StoreLock::acquire(&path).unwrap();
    assert_eq!(
        kache_fs::handle_identity(&lock.file).unwrap(),
        kache_fs::file_identity(&path).unwrap()
    );
    assert!(StoreLock::try_acquire(&path).unwrap().is_none());
}

#[test]
fn gc_lock_does_not_expire_live_holder_by_mtime() {
    // A live holder must not be considered stale just because the marker
    // file is old; large stores can make GC run for a long time.
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let first = store.try_gc_lock().unwrap().expect("first GC lock");
    let lock_path = config.store_dir().join("gc.lock");
    let old = filetime::FileTime::from_system_time(
        std::time::SystemTime::now() - std::time::Duration::from_secs(2 * 3600),
    );
    filetime::set_file_mtime(&lock_path, old).unwrap();

    assert!(
        store.try_gc_lock().unwrap().is_none(),
        "an old marker file must not let a second GC steal a live lock"
    );
    drop(first);
    assert!(store.try_gc_lock().unwrap().is_some());
}

#[test]
fn verify_restores_evicts_a_corrupted_blob() {
    // kunobi-ninja/kache#332: with the opt-in guard on, a blob whose content
    // no longer matches its address (silent corruption) is caught on the hit
    // path and evicted → miss → recompile, instead of poisoning the build.
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let f = write_temp_file(dir.path(), "lib.rlib", b"the real artifact bytes");
    store
        .put(
            "vkey",
            "vcrate",
            &["lib".into()],
            &[],
            "aarch64-apple-darwin",
            "release",
            &[(f, "lib.rlib".into())],
            "",
            "",
        )
        .unwrap();

    let meta = store.get("vkey").unwrap().expect("entry present after put");
    let blob = store.blob_path(&meta.files[0].hash);

    // Corrupt the blob in place, keeping the SAME size so the size check
    // passes and only the content (vs its address) differs.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&blob, std::fs::Permissions::from_mode(0o644)).unwrap();
    }
    #[cfg(not(unix))]
    {
        let mut p = std::fs::metadata(&blob).unwrap().permissions();
        p.set_readonly(false);
        std::fs::set_permissions(&blob, p).unwrap();
    }
    std::fs::write(&blob, vec![b'X'; meta.files[0].size as usize]).unwrap();

    let _env_lock = crate::test_support::process_state_test_lock();

    // Guard OFF (default): size matches, content not checked -> still a hit.
    {
        let _verify_off = EnvVarGuard::remove("KACHE_VERIFY_RESTORES");
        assert!(
            store.get("vkey").unwrap().is_some(),
            "without the guard a same-size corrupt blob is not caught"
        );
    }

    // Guard ON: content mismatch -> entry evicted -> miss.
    let result = {
        let _verify_on = EnvVarGuard::set("KACHE_VERIFY_RESTORES", "1");
        store.get("vkey").unwrap()
    };
    assert!(
        result.is_none(),
        "the guard must evict a blob whose content != its address"
    );
}

/// kunobi-ninja/kache#332: the env value maps to off|sampled|always, with the
/// legacy boolean spellings preserved as `Always`.
#[test]
fn verify_restores_mode_parses_tristate() {
    assert_eq!(parse_verify_restores(None), VerifyRestores::Off);
    assert_eq!(parse_verify_restores(Some("")), VerifyRestores::Off);
    assert_eq!(parse_verify_restores(Some("0")), VerifyRestores::Off);
    assert_eq!(parse_verify_restores(Some("off")), VerifyRestores::Off);
    assert_eq!(
        parse_verify_restores(Some("sampled")),
        VerifyRestores::Sampled
    );
    assert_eq!(
        parse_verify_restores(Some("SAMPLED")),
        VerifyRestores::Sampled
    );
    assert_eq!(
        parse_verify_restores(Some("always")),
        VerifyRestores::Always
    );
    // Back-compat: the old boolean values still mean "verify every hit".
    assert_eq!(parse_verify_restores(Some("1")), VerifyRestores::Always);
    assert_eq!(parse_verify_restores(Some("true")), VerifyRestores::Always);
}

/// kunobi-ninja/kache#332: Off never verifies, Always always does, and
/// Sampled verifies exactly one in every `VERIFY_SAMPLE_RATE` consecutive
/// hits (the rolling counter increments by one per call, so any window of
/// that size contains exactly one multiple — independent of the start).
#[test]
fn verify_restores_sampling_cadence() {
    assert!(!should_verify_this_restore(VerifyRestores::Off));
    assert!(should_verify_this_restore(VerifyRestores::Always));

    let window = VERIFY_SAMPLE_RATE as usize;
    let verified = (0..window)
        .filter(|_| should_verify_this_restore(VerifyRestores::Sampled))
        .count();
    assert_eq!(
        verified, 1,
        "exactly one in {window} consecutive sampled hits must verify"
    );
}

#[test]
fn test_put_get_restore_cycle() {
    // Put an entry with multiple files, get it, verify metadata,
    // verify blob files exist and are read-only,
    // verify entry dir only contains meta.json.
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let file_a = write_temp_file(dir.path(), "a.rlib", b"rlib artifact content");
    let file_b = write_temp_file(dir.path(), "b.dylib", b"dylib artifact content");
    let file_c = write_temp_file(dir.path(), "c.rmeta", b"rmeta artifact content");

    store
        .put(
            "multi_key",
            "multi_crate",
            &["lib".into(), "dylib".into()],
            &["serde".into(), "tokio".into()],
            "aarch64-apple-darwin",
            "release",
            &[
                (file_a, "a.rlib".into()),
                (file_b, "b.dylib".into()),
                (file_c, "c.rmeta".into()),
            ],
            "some stdout",
            "some stderr",
        )
        .unwrap();

    // Get the entry and verify metadata
    let meta = store.get("multi_key").unwrap().unwrap();
    assert_eq!(meta.crate_name, "multi_crate");
    assert_eq!(meta.crate_types, vec!["lib", "dylib"]);
    assert_eq!(meta.features, vec!["serde", "tokio"]);
    assert_eq!(meta.target, "aarch64-apple-darwin");
    assert_eq!(meta.profile, "release");
    assert_eq!(meta.stdout, "some stdout");
    assert_eq!(meta.stderr, "some stderr");
    assert_eq!(meta.files.len(), 3);

    // Verify blob files exist and are read-only
    for cached_file in &meta.files {
        let blob = store.blob_path(&cached_file.hash);
        assert!(blob.exists(), "blob for {} should exist", cached_file.name);
        let perms = fs::metadata(&blob).unwrap().permissions();
        assert!(
            perms.readonly(),
            "blob for {} should be read-only",
            cached_file.name
        );
    }

    // Verify entry dir only contains meta.json
    let entry_dir = store.entry_dir("multi_key");
    let mut files: Vec<String> = fs::read_dir(&entry_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    files.sort();
    assert_eq!(files, vec!["meta.json"]);
}

#[test]
fn test_clear_removes_all_blobs_and_tables() {
    // Put a few entries, call clear(), verify blobs directory and tables are empty.
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    // Create 3 entries with different content
    for i in 0..3 {
        let file = write_temp_file(
            dir.path(),
            &format!("f{i}.rlib"),
            format!("content {i}").as_bytes(),
        );
        store
            .put(
                &format!("key{i}"),
                &format!("crate{i}"),
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(file, format!("lib{i}.rlib"))],
                "",
                "",
            )
            .unwrap();
    }

    assert_eq!(store.entry_count().unwrap(), 3);
    assert!(blob_table_count(&store) >= 3);

    store.clear().unwrap();

    // Entries table should be empty
    assert_eq!(store.entry_count().unwrap(), 0);

    // Blobs table should be empty
    assert_eq!(blob_table_count(&store), 0);

    // Blobs directory should be empty or removed
    let blobs_dir = store.blobs_dir();
    if blobs_dir.exists() {
        let any_content = fs::read_dir(&blobs_dir).unwrap().flatten().any(|_| true);
        assert!(!any_content, "blobs dir should be empty after clear");
    }
}

#[test]
fn test_migration_of_legacy_entry() {
    // Create a "legacy" entry by manually writing files to an entry dir
    // (meta.json + artifact files, without blob store).
    // Call migrate_entry_to_blobs() directly.
    // Verify artifacts moved to blob store, entry dir only has meta.json.
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let entry_dir = config.store_dir().join("legacy_key");
    fs::create_dir_all(&entry_dir).unwrap();

    // Create two legacy artifact files
    let content_a = b"legacy artifact A";
    let content_b = b"legacy artifact B";
    fs::write(entry_dir.join("a.rlib"), content_a).unwrap();
    fs::write(entry_dir.join("b.dylib"), content_b).unwrap();

    let hash_a = crate::file_hash::hash_file(&entry_dir.join("a.rlib")).unwrap();
    let hash_b = crate::file_hash::hash_file(&entry_dir.join("b.dylib")).unwrap();

    let meta = EntryMeta {
        cache_key: "legacy_key".to_string(),
        key_schema: kache_format::CACHE_KEY_VERSION,
        crate_name: "legacy_crate".to_string(),
        crate_types: vec!["lib".to_string()],
        files: vec![
            CachedFile {
                name: "a.rlib".to_string(),
                size: content_a.len() as u64,
                hash: hash_a.clone(),
                executable: false,
            },
            CachedFile {
                name: "b.dylib".to_string(),
                size: content_b.len() as u64,
                hash: hash_b.clone(),
                executable: false,
            },
        ],
        stdout: String::new(),
        stderr: String::new(),
        features: vec![],
        target: String::new(),
        profile: "dev".to_string(),
        compile_time_ms: 0,
        emit_kinds: Vec::new(),
    };
    fs::write(
        entry_dir.join("meta.json"),
        serde_json::to_string_pretty(&meta).unwrap(),
    )
    .unwrap();

    // Register in DB as committed
    store
            .db
            .execute(
                "INSERT INTO entries (cache_key, crate_name, size, committed) VALUES ('legacy_key', 'legacy_crate', ?1, 1)",
                params![(content_a.len() + content_b.len()) as i64],
            )
            .unwrap();

    // Call migrate_entry_to_blobs directly
    assert!(store.migrate_entry_to_blobs(&meta).unwrap());

    // Artifacts should be gone from entry dir
    assert!(
        !entry_dir.join("a.rlib").exists(),
        "a.rlib should be moved to blob store"
    );
    assert!(
        !entry_dir.join("b.dylib").exists(),
        "b.dylib should be moved to blob store"
    );

    // meta.json should remain
    assert!(entry_dir.join("meta.json").exists());

    // Blobs should exist and be read-only
    let blob_a = store.blob_path(&hash_a);
    let blob_b = store.blob_path(&hash_b);
    assert!(blob_a.exists(), "blob for a.rlib should exist");
    assert!(blob_b.exists(), "blob for b.dylib should exist");
    assert!(fs::metadata(&blob_a).unwrap().permissions().readonly());
    assert!(fs::metadata(&blob_b).unwrap().permissions().readonly());

    // Refcounts should be 1
    assert_eq!(blob_refcount(&store, &hash_a), Some(1));
    assert_eq!(blob_refcount(&store, &hash_b), Some(1));
    assert_eq!(store.backfill_entry_blobs().unwrap(), 1);
    assert_blob_refs_match_mappings(&store);

    // Entry dir should only have meta.json
    let files: Vec<String> = fs::read_dir(&entry_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert_eq!(files, vec!["meta.json"]);
}

#[test]
fn migrate_entry_to_blobs_bumps_refcount_when_insert_loses_race() {
    // Covers migrate_entry_to_blobs INSERT OR IGNORE changes()==0 branch.
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let entry_dir = store.entry_dir("legacy_race");
    fs::create_dir_all(&entry_dir).unwrap();
    let artifact = entry_dir.join("lib.rlib");
    fs::write(&artifact, b"legacy race artifact").unwrap();
    let hash = crate::file_hash::hash_file(&artifact).unwrap();
    let size = fs::metadata(&artifact).unwrap().len();
    let meta = EntryMeta {
        cache_key: "legacy_race".to_string(),
        key_schema: kache_format::CACHE_KEY_VERSION,
        crate_name: "legacy_crate".to_string(),
        crate_types: vec!["lib".to_string()],
        files: vec![CachedFile {
            name: "lib.rlib".to_string(),
            size,
            hash: hash.clone(),
            executable: false,
        }],
        stdout: String::new(),
        stderr: String::new(),
        features: vec![],
        target: String::new(),
        profile: "dev".to_string(),
        compile_time_ms: 0,
        emit_kinds: Vec::new(),
    };

    store
        .db
        .execute(
            &format!(
                "CREATE TEMP TRIGGER seed_blob_before_insert \
                     BEFORE INSERT ON blobs \
                     WHEN NEW.hash = '{hash}' \
                     BEGIN \
                       INSERT OR IGNORE INTO blobs (hash, size, refcount) \
                       VALUES (NEW.hash, NEW.size, 41); \
                     END"
            ),
            [],
        )
        .unwrap();

    store
        .db
        .execute(
            "INSERT INTO entries (cache_key, crate_name, size, committed)
                 VALUES ('legacy_race', 'legacy_crate', ?1, 1)",
            params![size as i64],
        )
        .unwrap();

    assert!(store.migrate_entry_to_blobs(&meta).unwrap());

    assert_eq!(blob_refcount(&store, &hash), Some(42));
    assert!(store.blob_path(&hash).is_file());
    assert!(!artifact.exists());
}

#[test]
fn test_eviction_with_shared_blobs() {
    // Put 3 entries where entries 1 and 2 share blobs, entry 3 is unique.
    // Remove entry 1 → shared blobs persist with refcount decremented.
    // Remove entry 2 → shared blobs deleted.
    // Entry 3's blobs should be unaffected throughout.
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let shared_content = b"shared between 1 and 2";
    let unique3_content = b"unique to entry 3 only";

    // Entry 1: shared blob
    let f = write_temp_file(dir.path(), "shared.rlib", shared_content);
    store
        .put(
            "e1",
            "c1",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(f, "shared.rlib".into())],
            "",
            "",
        )
        .unwrap();

    // Entry 2: same shared blob
    let f = write_temp_file(dir.path(), "shared.rlib", shared_content);
    store
        .put(
            "e2",
            "c2",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(f, "shared.rlib".into())],
            "",
            "",
        )
        .unwrap();

    // Entry 3: unique blob
    let f = write_temp_file(dir.path(), "unique3.rlib", unique3_content);
    store
        .put(
            "e3",
            "c3",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(f, "unique3.rlib".into())],
            "",
            "",
        )
        .unwrap();

    let meta1 = read_meta(&store, "e1");
    let meta3 = read_meta(&store, "e3");
    let shared_hash = &meta1.files[0].hash;
    let unique3_hash = &meta3.files[0].hash;

    assert_eq!(blob_refcount(&store, shared_hash), Some(2));
    assert_blob_refs_match_mappings(&store);
    assert_eq!(blob_refcount(&store, unique3_hash), Some(1));

    // Remove entry 1 — shared blob persists
    store.remove_entry("e1").unwrap();
    assert_eq!(blob_refcount(&store, shared_hash), Some(1));
    assert!(store.blob_path(shared_hash).exists());
    // Entry 3 unaffected
    assert!(store.blob_path(unique3_hash).exists());
    assert_eq!(blob_refcount(&store, unique3_hash), Some(1));

    // Remove entry 2 — shared blob now deleted
    store.remove_entry("e2").unwrap();
    assert!(!store.blob_path(shared_hash).exists());
    assert_eq!(blob_refcount(&store, shared_hash), None);
    // Entry 3 still unaffected
    assert!(store.blob_path(unique3_hash).exists());
    assert_eq!(blob_refcount(&store, unique3_hash), Some(1));

    // Verify entry 3 can still be retrieved
    let meta = store.get("e3").unwrap();
    assert!(meta.is_some());
}

#[test]
fn test_blob_stats_with_known_overlap() {
    // Put entries with known content overlap.
    // Verify logical vs physical size, savings percentage.
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let shared_content = b"AAAA"; // 4 bytes, shared by entries 1 and 2
    let unique_content = b"BBBBBBBB"; // 8 bytes, only in entry 1

    // Entry 1: shared (4 bytes) + unique (8 bytes) = 12 bytes logical
    let f_shared = write_temp_file(dir.path(), "shared.rlib", shared_content);
    let f_unique = write_temp_file(dir.path(), "unique.rlib", unique_content);
    store
        .put(
            "stats1",
            "c1",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[
                (f_shared, "shared.rlib".into()),
                (f_unique, "unique.rlib".into()),
            ],
            "",
            "",
        )
        .unwrap();

    // Entry 2: shared (4 bytes) = 4 bytes logical
    let f_shared = write_temp_file(dir.path(), "shared.rlib", shared_content);
    store
        .put(
            "stats2",
            "c2",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(f_shared, "shared.rlib".into())],
            "",
            "",
        )
        .unwrap();

    // Total logical size from entries table = 12 + 4 = 16 bytes
    // Total physical blob size = 4 (shared) + 8 (unique) = 12 bytes
    // Savings = 16 - 12 = 4 bytes
    let stats = store.blob_stats().unwrap();
    assert_eq!(stats.total_blobs, 2, "should have 2 unique blobs");
    assert_eq!(
        stats.total_blob_size, 12,
        "physical size should be 12 bytes"
    );
    assert_eq!(
        stats.total_logical_size, 16,
        "logical size should be 16 bytes"
    );
    assert_eq!(stats.savings, 4, "savings should be 4 bytes");
}

/// kunobi-ninja/kache#324: pin an exact `content_hash` for a fixed
/// multi-file entry. `compute_content_hash` folds `(name, hash, size,
/// exec-bit)` in a stable serialization; this golden value fails loudly if
/// that serialization ever drifts (field order, length-prefixing, exec-bit
/// encoding), which would silently change dedup behavior across versions.
#[test]
fn content_hash_golden_pins_serialization() {
    let cf = |name: &str, size: u64, hash: &str, executable: bool| CachedFile {
        name: name.to_string(),
        size,
        hash: hash.to_string(),
        executable,
    };
    // Deliberately unsorted on input — compute_content_hash sorts internally.
    let files = vec![
        cf("foo", 4096, "cccccccccccccccccccccccccccccccc", true),
        cf(
            "libfoo.rlib",
            1024,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            false,
        ),
        cf(
            "libfoo.rmeta",
            256,
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            false,
        ),
    ];
    assert_eq!(
        compute_content_hash(&files),
        "2dd3b89296eb2d5469d11aa00b312cee8734923698b97890f43ea6a8b9a37585",
    );
}

/// kunobi-ninja/kache#325: an entry's covered emit kinds are derived from
/// its stored filenames, deduped and sorted.
#[test]
fn emit_kinds_derived_from_files() {
    let cf = |name: &str| CachedFile {
        name: name.to_string(),
        size: 1,
        hash: "h".to_string(),
        executable: false,
    };
    // A lib `--emit=link` build: rlib + side rmeta + dep-info.
    let kinds = emit_kinds_for_files::<TestPolicy>(&[
        cf("libfoo.rlib"),
        cf("libfoo.rmeta"),
        cf("foo.d"),
        cf("foo.dSYM"), // sidecar → no emit kind, ignored
    ]);
    assert_eq!(kinds, vec!["dep-info", "link", "metadata"]);
}

/// kunobi-ninja/kache#431: a wasm32 target's link product is a `.wasm`
/// file. Until it mapped to the `link` emit kind, an entry built for
/// `--emit=link,dep-info` derived only `["dep-info"]`, so the coverage
/// gate refused to store it — silently blocking every wasm module,
/// including substrate's runtime crates (the bench's most expensive
/// compiles).
#[test]
fn wasm_link_output_satisfies_the_emit_coverage_gate() {
    let files = vec![
        CachedFile {
            name: "rococo_runtime.wasm".into(),
            size: 4,
            hash: "h1".into(),
            executable: false,
        },
        CachedFile {
            name: "rococo_runtime.d".into(),
            size: 4,
            hash: "h2".into(),
            executable: false,
        },
    ];
    let kinds = emit_kinds_for_files::<TestPolicy>(&files);
    assert_eq!(
        kinds,
        vec!["dep-info".to_string(), "link".to_string()],
        "a .wasm module is the link product of a wasm32 target"
    );

    let meta = EntryMeta {
        cache_key: "k".into(),
        key_schema: kache_format::CACHE_KEY_VERSION,
        crate_name: "rococo_runtime".into(),
        crate_types: vec!["cdylib".into()],
        files,
        stdout: String::new(),
        stderr: String::new(),
        features: vec![],
        target: "wasm32-unknown-unknown".into(),
        profile: "release".into(),
        compile_time_ms: 62_000,
        emit_kinds: kinds,
    };
    assert!(
        meta.covers_requested_emit(&["link".to_string(), "dep-info".to_string()]),
        "the entry must satisfy the --emit it was built for"
    );
}

#[test]
fn test_put_stores_content_hash() {
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(tmp.path());
    let store = Store::open(&config).unwrap();

    let dir = tmp.path().join("src");
    std::fs::create_dir_all(&dir).unwrap();
    let file1 = dir.join("lib.rlib");
    std::fs::write(&file1, b"artifact-content-1234").unwrap();

    store
        .put(
            "key_ch_1",
            "mycrate",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(file1, "lib.rlib".to_string())],
            "",
            "",
        )
        .unwrap();

    let ch: String = store
        .db
        .query_row(
            "SELECT content_hash FROM entries WHERE cache_key = 'key_ch_1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        ch.len(),
        64,
        "content_hash should be full blake3 hex (64 chars)"
    );
}

#[test]
fn test_import_downloaded_entry_stores_content_hash() {
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(tmp.path());
    let store = Store::open(&config).unwrap();

    let entry_dir = store.entry_dir("dl_ch_test");
    std::fs::create_dir_all(&entry_dir).unwrap();

    let artifact = entry_dir.join("lib.rlib");
    std::fs::write(&artifact, b"downloaded-artifact-data").unwrap();
    let hash = crate::file_hash::hash_file(&artifact).unwrap();
    let size = std::fs::metadata(&artifact).unwrap().len();

    let meta = EntryMeta {
        cache_key: "dl_ch_test".to_string(),
        key_schema: kache_format::CACHE_KEY_VERSION,
        crate_name: "dlcrate".to_string(),
        crate_types: vec!["lib".to_string()],
        files: vec![CachedFile {
            name: "lib.rlib".to_string(),
            size,
            hash,
            executable: false,
        }],
        stdout: String::new(),
        stderr: String::new(),
        features: vec![],
        target: "x86_64-unknown-linux-gnu".to_string(),
        profile: "dev".to_string(),
        compile_time_ms: 0,
        emit_kinds: Vec::new(),
    };
    std::fs::write(
        entry_dir.join("meta.json"),
        serde_json::to_string_pretty(&meta).unwrap(),
    )
    .unwrap();

    store.import_downloaded_entry("dl_ch_test").unwrap();

    let ch: String = store
        .db
        .query_row(
            "SELECT content_hash FROM entries WHERE cache_key = 'dl_ch_test'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(ch.len(), 64);
}

#[test]
fn verified_batch_import_is_atomic_and_registers_every_entry() {
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(tmp.path());
    let store = Store::open(&config).unwrap();
    let keys = [
        blake3::hash(b"packed-batch-a").to_hex().to_string(),
        blake3::hash(b"packed-batch-b").to_hex().to_string(),
    ];

    let mut verified = Vec::new();
    for (index, key) in keys.iter().enumerate() {
        let entry_dir = store.entry_dir(key);
        std::fs::create_dir_all(&entry_dir).unwrap();
        let artifact = entry_dir.join(format!("lib{index}.rlib"));
        let contents = format!("verified packed artifact {index}");
        std::fs::write(&artifact, contents.as_bytes()).unwrap();
        let meta = EntryMeta {
            cache_key: key.clone(),
            key_schema: kache_format::CACHE_KEY_VERSION,
            crate_name: format!("crate{index}"),
            crate_types: vec!["lib".to_string()],
            files: vec![CachedFile {
                name: format!("lib{index}.rlib"),
                size: contents.len() as u64,
                hash: blake3::hash(contents.as_bytes()).to_hex().to_string(),
                executable: false,
            }],
            stdout: String::new(),
            stderr: String::new(),
            features: vec![],
            target: "x86_64-unknown-linux-gnu".to_string(),
            profile: "dev".to_string(),
            compile_time_ms: 1,
            emit_kinds: Vec::new(),
        };
        std::fs::write(
            entry_dir.join("meta.json"),
            serde_json::to_vec_pretty(&meta).unwrap(),
        )
        .unwrap();
        verified.push(VerifiedRestoredEntry {
            cache_key: key.clone(),
            meta,
        });
    }

    let original_size = verified[1].meta.files[0].size;
    verified[1].meta.files[0].size += 1;
    assert!(store.import_verified_restored_entries(&verified).is_err());
    let rows: i64 = store
        .db
        .query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 0, "a failed preflight must register no batch rows");

    verified[1].meta.files[0].size = original_size;
    store
            .db
            .execute(
                "INSERT INTO entries (cache_key, crate_name, crate_type, profile, num_features, size, content_hash, compile_time_ms, key_schema, committed) VALUES (?1, 'stale', 'lib', 'dev', 0, 0, 'stale', 0, ?2, 0)",
                params![keys[0], kache_format::CACHE_KEY_VERSION],
            )
            .unwrap();
    let imported = store.import_verified_restored_entries(&verified).unwrap();
    assert_eq!(imported, 2);
    let rows: i64 = store
        .db
        .query_row(
            "SELECT COUNT(*) FROM entries WHERE committed = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(rows, 2);
    let stamped: i64 = store
        .db
        .query_row(
            "SELECT COUNT(*) FROM entries WHERE imported_at IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stamped, 2, "a prefetched batch is an import too (#1008)");
    for key in keys {
        assert!(store.get(&key).unwrap().is_some());
    }
    let refcounts: Vec<i64> = store
        .db
        .prepare("SELECT refcount FROM blobs ORDER BY hash")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(refcounts, vec![1, 1]);
}

fn write_verified_fixture(
    store: &Store,
    key: &str,
    meta_key: &str,
    artifact_name: &str,
    hash_override: Option<String>,
) -> VerifiedRestoredEntry {
    let contents = b"verified fixture artifact";
    let entry_dir = store.entry_dir(key);
    let artifact = entry_dir.join(artifact_name);
    std::fs::create_dir_all(artifact.parent().unwrap()).unwrap();
    std::fs::write(&artifact, contents).unwrap();
    let meta = EntryMeta {
        cache_key: meta_key.to_string(),
        key_schema: kache_format::CACHE_KEY_VERSION,
        crate_name: "fixture".to_string(),
        crate_types: vec!["lib".to_string()],
        files: vec![CachedFile {
            name: artifact_name.to_string(),
            size: contents.len() as u64,
            hash: hash_override.unwrap_or_else(|| blake3::hash(contents).to_hex().to_string()),
            executable: false,
        }],
        stdout: String::new(),
        stderr: String::new(),
        features: Vec::new(),
        target: "x86_64-unknown-linux-gnu".to_string(),
        profile: "dev".to_string(),
        compile_time_ms: 1,
        emit_kinds: Vec::new(),
    };
    std::fs::write(
        entry_dir.join("meta.json"),
        serde_json::to_vec_pretty(&meta).unwrap(),
    )
    .unwrap();
    VerifiedRestoredEntry {
        cache_key: key.to_string(),
        meta,
    }
}

#[test]
fn verified_batch_import_checks_each_cache_key_binding_independently() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(tmp.path())).unwrap();
    let invalid = write_verified_fixture(&store, "invalid", "invalid", "lib.rlib", None);
    assert!(store.import_verified_restored_entries(&[invalid]).is_err());

    let key = blake3::hash(b"valid-outer-key").to_hex().to_string();
    let other = blake3::hash(b"different-meta-key").to_hex().to_string();
    let mismatched = write_verified_fixture(&store, &key, &other, "lib.rlib", None);
    assert!(
        store
            .import_verified_restored_entries(&[mismatched])
            .is_err()
    );
}

#[test]
fn verified_batch_import_checks_each_artifact_field_independently() {
    for (label, name, hash_override) in [
        ("unsafe-name", "nested/lib.rlib", None),
        ("invalid-hash", "lib.rlib", Some("g".repeat(64))),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(tmp.path())).unwrap();
        let key = blake3::hash(label.as_bytes()).to_hex().to_string();
        let entry = write_verified_fixture(&store, &key, &key, name, hash_override);
        assert!(
            store.import_verified_restored_entries(&[entry]).is_err(),
            "{label} must be rejected independently"
        );
    }

    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(tmp.path())).unwrap();
    let key = blake3::hash(b"duplicate-artifact").to_hex().to_string();
    let mut entry = write_verified_fixture(&store, &key, &key, "lib.rlib", None);
    entry.meta.files.push(entry.meta.files[0].clone());
    std::fs::write(
        store.entry_dir(&key).join("meta.json"),
        serde_json::to_vec_pretty(&entry.meta).unwrap(),
    )
    .unwrap();
    assert!(store.import_verified_restored_entries(&[entry]).is_err());
}

#[test]
fn verified_batch_import_never_rewrites_an_existing_content_addressed_blob() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(tmp.path())).unwrap();
    let key = blake3::hash(b"existing-immutable-blob")
        .to_hex()
        .to_string();
    let entry = write_verified_fixture(&store, &key, &key, "lib.rlib", None);
    let blob = store.blob_path(&entry.meta.files[0].hash);
    std::fs::create_dir_all(blob.parent().unwrap()).unwrap();
    std::fs::write(&blob, b"pre-existing immutable blob").unwrap();

    assert_eq!(store.import_verified_restored_entries(&[entry]).unwrap(), 1);
    assert_eq!(std::fs::read(blob).unwrap(), b"pre-existing immutable blob");
}

#[test]
fn verified_blob_install_reports_a_vanished_source_before_rename() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(tmp.path())).unwrap();
    let file = CachedFile {
        name: "lib.rlib".to_string(),
        size: 7,
        hash: blake3::hash(b"missing verified artifact")
            .to_hex()
            .to_string(),
        executable: false,
    };

    let error = store
        .install_verified_blob(&tmp.path().join("missing-entry"), &file)
        .expect_err("a vanished verified source must fail")
        .to_string();
    assert!(
        error.contains("verified restored blob vanished during batch import"),
        "unexpected error: {error}"
    );
}

#[test]
fn test_list_entries_includes_content_hash() {
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(tmp.path());
    let store = Store::open(&config).unwrap();

    let dir = tmp.path().join("src");
    std::fs::create_dir_all(&dir).unwrap();
    let file1 = dir.join("lib.rlib");
    std::fs::write(&file1, b"list-test-content").unwrap();

    store
        .put(
            "list_ch_1",
            "mycrate",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(file1, "lib.rlib".to_string())],
            "",
            "",
        )
        .unwrap();

    let entries = store.list_entries("name").unwrap();
    assert_eq!(entries.len(), 1);
    assert!(entries[0].content_hash.is_some());
    assert_eq!(entries[0].content_hash.as_ref().unwrap().len(), 64);
}

/// kunobi-ninja/kache#709: byte-identical entries share their blob, so
/// removing the older key destroys history without reclaiming disk.
#[test]
fn evict_duplicate_entries_spares_a_pair_sharing_one_blob() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = test_config(tmp.path());
    config.max_size = 1;
    let store = Store::open(&config).unwrap();

    let dir = tmp.path().join("src");
    std::fs::create_dir_all(&dir).unwrap();

    let file1 = dir.join("lib.rlib");
    std::fs::write(&file1, b"same-content-bytes").unwrap();

    store
        .put(
            "dup_key_1",
            "mycrate",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(file1.clone(), "lib.rlib".to_string())],
            "",
            "",
        )
        .unwrap();

    // Artificially age the first entry's access time (LRU policy)
    store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour') WHERE cache_key = 'dup_key_1'",
                [],
            )
            .unwrap();

    store
        .put(
            "dup_key_2",
            "mycrate",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(file1, "lib.rlib".to_string())],
            "",
            "",
        )
        .unwrap();

    assert_eq!(store.entry_count().unwrap(), 2);

    let stats = store.evict_duplicate_entries().unwrap();
    assert_eq!(stats.entries_evicted, 0);
    assert_eq!(store.entry_count().unwrap(), 2);
    assert!(store.contains("dup_key_1") && store.contains("dup_key_2"));
}

#[test]
fn evict_duplicate_entries_skips_the_scan_under_budget() {
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(tmp.path());
    let store = Store::open(&config).unwrap();

    let dir = tmp.path().join("src");
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("lib.rlib");
    std::fs::write(&file, b"tiny-shared-content").unwrap();
    store
        .put(
            "under_budget_1",
            "mycrate",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(file.clone(), "lib.rlib".to_string())],
            "",
            "",
        )
        .unwrap();
    store
        .db
        .execute(
            "UPDATE entries SET last_accessed = datetime('now', '-1 hour') \
                 WHERE cache_key = 'under_budget_1'",
            [],
        )
        .unwrap();
    store
        .put(
            "under_budget_2",
            "mycrate",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(file, "lib.rlib".to_string())],
            "",
            "",
        )
        .unwrap();

    let stats = store.evict_duplicate_entries().unwrap();
    assert!(stats.skipped);
    assert_eq!(stats.entries_evicted, 0);
    assert_eq!(store.entry_count().unwrap(), 2);
}

#[test]
fn evict_duplicate_entries_fails_closed_for_unmapped_legacy_victim() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = test_config(tmp.path());
    config.max_size = 1;
    let store = Store::open(&config).unwrap();

    let file = tmp.path().join("legacy.rlib");
    std::fs::write(&file, b"shared-legacy-content").unwrap();
    for key in ["legacy_old", "legacy_new"] {
        store
            .put(
                key,
                "mycrate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(file.clone(), "lib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
    }
    store
        .db
        .execute(
            "UPDATE entries SET last_accessed = datetime('now', '-1 hour') \
                 WHERE cache_key = 'legacy_old'",
            [],
        )
        .unwrap();
    store
        .db
        .execute("DELETE FROM entry_blobs WHERE cache_key = 'legacy_old'", [])
        .unwrap();

    let stats = store.evict_duplicate_entries().unwrap();
    assert_eq!(stats.entries_evicted, 0);
    assert!(store.contains("legacy_old"));
    assert!(store.contains("legacy_new"));
}

#[test]
fn evict_duplicate_entries_stops_at_the_physical_target() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = test_config(tmp.path());
    config.max_size = 500; // physical 600; target 450
    let store = Store::open(&config).unwrap();

    for group in 0..3 {
        let old_key = format!("budget_old_{group}");
        let new_key = format!("budget_new_{group}");
        let old_file = tmp.path().join(format!("old-{group}.rlib"));
        let new_file = tmp.path().join(format!("new-{group}.rlib"));
        std::fs::write(&old_file, vec![group as u8 + 1; 100]).unwrap();
        std::fs::write(&new_file, vec![group as u8 + 11; 100]).unwrap();

        store
            .put(
                &old_key,
                "mycrate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(old_file.clone(), "lib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        store.remove_clone_for_test(&old_file);
        store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', ?1) \
                     WHERE cache_key = ?2",
                params![format!("-{} hours", 3 - group), old_key],
            )
            .unwrap();
        let group_hash: String = store
            .db
            .query_row(
                "SELECT content_hash FROM entries WHERE cache_key = ?1",
                params![old_key],
                |row| row.get(0),
            )
            .unwrap();
        store
            .put(
                &new_key,
                "mycrate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(new_file.clone(), "lib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        store.remove_clone_for_test(&new_file);
        store
            .db
            .execute(
                "UPDATE entries SET content_hash = ?1 WHERE cache_key = ?2",
                params![group_hash, new_key],
            )
            .unwrap();
    }

    assert_eq!(store.physical_size().unwrap(), 600);
    let stats = store.evict_duplicate_entries().unwrap();
    assert_eq!(stats.entries_evicted, 2);
    assert_eq!(stats.bytes_freed, 200);
    assert_eq!(store.physical_size().unwrap(), 400);
    assert!(!store.contains("budget_old_0"));
    assert!(!store.contains("budget_old_1"));
    assert!(
        store.contains("budget_old_2"),
        "bounded duplicate GC must retain the least-stale eligible victim"
    );
}

#[test]
fn evict_duplicate_entries_skips_victim_with_corrupt_meta() {
    // Covers evict_duplicate_entries remove_entry_guarded error branch.
    let tmp = tempfile::tempdir().unwrap();
    let mut config = test_config(tmp.path());
    config.max_size = 1;
    let store = Store::open(&config).unwrap();

    let dir = tmp.path().join("src");
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("lib.rlib");
    rewrite_source(&file, b"same-content-for-corrupt-dedup");
    store
        .put(
            "dup_corrupt_old",
            "mycrate",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(file.clone(), "lib.rlib".to_string())],
            "",
            "",
        )
        .unwrap();
    store
        .db
        .execute(
            "UPDATE entries SET last_accessed = datetime('now', '-1 hour') \
                 WHERE cache_key = 'dup_corrupt_old'",
            [],
        )
        .unwrap();

    let old_content_hash: String = store
        .db
        .query_row(
            "SELECT content_hash FROM entries WHERE cache_key = 'dup_corrupt_old'",
            [],
            |row| row.get(0),
        )
        .unwrap();

    // Give the newer entry its own blob, then place both keys in the same
    // duplicate group. The older victim now has proven positive marginal
    // bytes, so fail-closed filtering does not make this error-path test
    // vacuous.
    rewrite_source(&file, b"different-content-for-corrupt-dedup");
    store
        .put(
            "dup_corrupt_new",
            "mycrate",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(file, "lib.rlib".to_string())],
            "",
            "",
        )
        .unwrap();
    store
        .db
        .execute(
            "UPDATE entries SET content_hash = ?1 WHERE cache_key = 'dup_corrupt_new'",
            params![old_content_hash],
        )
        .unwrap();
    std::fs::write(
        store.entry_dir("dup_corrupt_old").join("meta.json"),
        b"{not json",
    )
    .unwrap();

    let stats = store.evict_duplicate_entries().unwrap();

    assert_eq!(stats.entries_evicted, 0, "corrupt victim is skipped");
    assert!(store.contains("dup_corrupt_old"));
    assert!(store.contains("dup_corrupt_new"));
}

#[test]
fn test_backfill_content_hashes() {
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(tmp.path());
    let store = Store::open(&config).unwrap();

    let dir = tmp.path().join("src");
    std::fs::create_dir_all(&dir).unwrap();
    let file1 = dir.join("lib.rlib");
    std::fs::write(&file1, b"backfill-content").unwrap();

    store
        .put(
            "bf_key_1",
            "mycrate",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(file1, "lib.rlib".to_string())],
            "",
            "",
        )
        .unwrap();

    // Simulate a legacy entry by clearing the content_hash
    store
        .db
        .execute(
            "UPDATE entries SET content_hash = NULL WHERE cache_key = 'bf_key_1'",
            [],
        )
        .unwrap();

    let backfilled = store.backfill_content_hashes().unwrap();
    assert_eq!(backfilled, 1);

    let ch: String = store
        .db
        .query_row(
            "SELECT content_hash FROM entries WHERE cache_key = 'bf_key_1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(ch.len(), 64);
}

#[test]
fn test_content_hash_column_exists() {
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(tmp.path());
    let store = Store::open(&config).unwrap();
    let result: Result<Option<String>, _> =
        store
            .db
            .query_row("SELECT content_hash FROM entries LIMIT 1", [], |row| {
                row.get(0)
            });
    // Query should succeed (column exists), just no rows
    assert!(result.is_ok() || result.unwrap_err().to_string().contains("no rows"));
}

#[test]
fn test_content_hash_full_dedup_lifecycle() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = test_config(tmp.path());
    config.max_size = 1;
    let store = Store::open(&config).unwrap();

    let dir = tmp.path().join("src");
    std::fs::create_dir_all(&dir).unwrap();

    // Create 3 entries: 2 with identical content, 1 different
    let file_a = dir.join("a.rlib");
    std::fs::write(&file_a, b"shared-content").unwrap();
    let file_b = dir.join("b.rlib");
    std::fs::write(&file_b, b"different-content").unwrap();

    store
        .put(
            "ch_lc_1",
            "mycrate",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(file_a.clone(), "a.rlib".to_string())],
            "",
            "",
        )
        .unwrap();

    // Age the first entry's access time (LRU policy)
    store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour') WHERE cache_key = 'ch_lc_1'",
                [],
            )
            .unwrap();

    store
        .put(
            "ch_lc_2",
            "mycrate",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(file_a, "a.rlib".to_string())],
            "",
            "",
        )
        .unwrap();

    store
        .put(
            "ch_lc_3",
            "othercrate",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(file_b, "b.rlib".to_string())],
            "",
            "",
        )
        .unwrap();

    // Verify content hashes
    let entries = store.list_entries("name").unwrap();
    assert_eq!(entries.len(), 3);

    let ch1 = entries
        .iter()
        .find(|e| e.cache_key == "ch_lc_1")
        .unwrap()
        .content_hash
        .as_ref()
        .unwrap();
    let ch2 = entries
        .iter()
        .find(|e| e.cache_key == "ch_lc_2")
        .unwrap()
        .content_hash
        .as_ref()
        .unwrap();
    let ch3 = entries
        .iter()
        .find(|e| e.cache_key == "ch_lc_3")
        .unwrap()
        .content_hash
        .as_ref()
        .unwrap();
    assert_eq!(ch1, ch2, "identical content should have same hash");
    assert_ne!(ch1, ch3, "different content should have different hash");

    // The shared duplicate frees no bytes, so both keys survive.
    let stats = store.evict_duplicate_entries().unwrap();
    assert_eq!(stats.entries_evicted, 0);
    assert_eq!(store.entry_count().unwrap(), 3);
    assert!(store.contains("ch_lc_1"));
    assert!(store.contains("ch_lc_2"));
    assert!(store.contains("ch_lc_3"));
}

#[test]
fn store_copy_reason_cross_device_maps_from_exdev() {
    assert_eq!(
        StoreCopyReason::from_io_kind(std::io::ErrorKind::CrossesDevices),
        StoreCopyReason::CrossDevice,
    );
}

#[test]
fn store_copy_reason_permission_maps_from_eperm() {
    assert_eq!(
        StoreCopyReason::from_io_kind(std::io::ErrorKind::PermissionDenied),
        StoreCopyReason::Permission,
    );
}

#[test]
fn store_copy_reason_other_maps_from_unexpected_errno() {
    assert_eq!(
        StoreCopyReason::from_io_kind(std::io::ErrorKind::AlreadyExists),
        StoreCopyReason::Other,
    );
}

#[test]
fn record_store_copy_reason_cross_device_increments() {
    let before = crate::opcounts::store_copy_cross_device_bytes();
    record_store_copy_reason(StoreCopyReason::CrossDevice, 11);
    assert!(crate::opcounts::store_copy_cross_device_bytes() >= before + 11);
}

#[test]
fn record_store_copy_reason_permission_increments() {
    let before = crate::opcounts::store_copy_permission_bytes();
    record_store_copy_reason(StoreCopyReason::Permission, 13);
    assert!(crate::opcounts::store_copy_permission_bytes() >= before + 13);
}

#[test]
fn record_store_copy_reason_ineligible_increments() {
    let before = crate::opcounts::store_copy_ineligible_bytes();
    record_store_copy_reason(StoreCopyReason::Ineligible, 17);
    assert!(crate::opcounts::store_copy_ineligible_bytes() >= before + 17);
}

#[test]
fn record_store_copy_reason_other_increments() {
    let before = crate::opcounts::store_copy_other_bytes();
    record_store_copy_reason(StoreCopyReason::Other, 19);
    assert!(crate::opcounts::store_copy_other_bytes() >= before + 19);
}

/// Same-device `.rlib` put must hardlink, not copy (#835).
///
/// Forces the hardlink path (skips reflink) so the assertion holds on CoW
/// filesystems (APFS) as well as ext4: after the put, `store_hardlinked`
/// grows and the blob has exactly two links (blob + build output).
#[cfg(unix)]
#[test]
fn rlib_put_hardlinks_on_same_device() {
    use std::os::unix::fs::MetadataExt;

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let source = dir.path().join("libtest.rlib");
    fs::write(&source, b"rlib-bytes-for-hardlink").unwrap();
    let bytes = fs::metadata(&source).unwrap().len();

    let before = crate::opcounts::store_hardlinked_bytes();
    let _force = ForceStoreHardlink::enable();
    store
        .put(
            "hardlink_835_key",
            "hardlink_crate",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(source.clone(), "libtest.rlib".to_string())],
            "out",
            "err",
        )
        .unwrap();

    assert!(
        crate::opcounts::store_hardlinked_bytes() >= before + bytes,
        "same-device .rlib put must record hardlinked bytes"
    );
    let hash = crate::file_hash::hash_file(&source).unwrap();
    let blob = store.blob_path(&hash);
    assert_eq!(
        fs::metadata(&blob).unwrap().nlink(),
        2,
        "hardlinked blob must share its inode with the build output"
    );
}

#[test]
fn injected_cross_device_ingest_records_cross_device_reason() {
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let source = dir.path().join("libx.rlib");
    fs::write(&source, b"cross-device-bytes").unwrap();
    let bytes = fs::metadata(&source).unwrap().len();

    let before = crate::opcounts::store_copy_cross_device_bytes();
    let _force = ForceStoreHardlink::enable();
    let _inject = InjectStoreHardlinkError::enable(std::io::ErrorKind::CrossesDevices);
    let (staged, ingest) = store.stage_blob_from_source(&source, true).unwrap();
    assert!(
        matches!(ingest, StoreIngest::Copy(StoreCopyReason::CrossDevice)),
        "EXDEV injection must stage as Copy(CrossDevice), got {ingest:?}"
    );
    // Publish to record the reason alongside the copy.
    let hash = crate::file_hash::hash_file(&staged).unwrap();
    store
        .publish_staged_blob(&staged, ingest, &hash, bytes)
        .unwrap();
    assert!(
        crate::opcounts::store_copy_cross_device_bytes() >= before + bytes,
        "EXDEV injection must record the cross-device reason"
    );
}

#[test]
fn injected_permission_ingest_records_permission_reason() {
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let source = dir.path().join("libp.rlib");
    fs::write(&source, b"permission-bytes").unwrap();
    let bytes = fs::metadata(&source).unwrap().len();

    let before = crate::opcounts::store_copy_permission_bytes();
    let _force = ForceStoreHardlink::enable();
    let _inject = InjectStoreHardlinkError::enable(std::io::ErrorKind::PermissionDenied);
    let (staged, ingest) = store.stage_blob_from_source(&source, true).unwrap();
    assert!(
        matches!(ingest, StoreIngest::Copy(StoreCopyReason::Permission)),
        "EPERM injection must stage as Copy(Permission), got {ingest:?}"
    );
    let hash = crate::file_hash::hash_file(&staged).unwrap();
    store
        .publish_staged_blob(&staged, ingest, &hash, bytes)
        .unwrap();
    assert!(
        crate::opcounts::store_copy_permission_bytes() >= before + bytes,
        "EPERM injection must record the permission reason"
    );
}

#[test]
fn injected_other_ingest_records_other_reason() {
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let source = dir.path().join("libo.rlib");
    fs::write(&source, b"other-bytes").unwrap();
    let bytes = fs::metadata(&source).unwrap().len();

    let before = crate::opcounts::store_copy_other_bytes();
    let _force = ForceStoreHardlink::enable();
    let _inject = InjectStoreHardlinkError::enable(std::io::ErrorKind::AlreadyExists);
    let (staged, ingest) = store.stage_blob_from_source(&source, true).unwrap();
    assert!(
        matches!(ingest, StoreIngest::Copy(StoreCopyReason::Other)),
        "other errno injection must stage as Copy(Other), got {ingest:?}"
    );
    let hash = crate::file_hash::hash_file(&staged).unwrap();
    store
        .publish_staged_blob(&staged, ingest, &hash, bytes)
        .unwrap();
    assert!(
        crate::opcounts::store_copy_other_bytes() >= before + bytes,
        "other errno injection must record the other reason"
    );
}

/// The ingest advisory fires for a cross-device fallback and nothing
/// else, observed through marker files. Sole marker installer in this
/// binary: no other unit test calls `set_cow_warn_marker`, so the fresh
/// scratch dir starts empty deterministically.
#[test]
fn injected_cross_device_ingest_advises_but_other_errors_do_not() {
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let base = std::env::temp_dir().join(format!(
        "kache-store-cross-volume-test-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    crate::link::set_cow_warn_marker(base.join("warn"));
    crate::link::set_storage_layout_advice(true);
    let marker_written = || std::fs::read_dir(&base).unwrap().next().is_some();

    let source = dir.path().join("libv.rlib");
    fs::write(&source, b"cross-volume-bytes").unwrap();

    // Muted layout advice stays silent even on EXDEV.
    crate::link::set_storage_layout_advice(false);
    let _force = ForceStoreHardlink::enable();
    let _inject = InjectStoreHardlinkError::enable(std::io::ErrorKind::CrossesDevices);
    let (_, ingest) = store.stage_blob_from_source(&source, true).unwrap();
    assert!(
        matches!(ingest, StoreIngest::Copy(StoreCopyReason::CrossDevice)),
        "muting advice must not change the copy reason, got {ingest:?}"
    );
    assert!(
        !marker_written(),
        "a muted advisory must not write a marker"
    );
    crate::link::set_storage_layout_advice(true);

    // A non-cross-device failure records its reason but advises nothing.
    let _force = ForceStoreHardlink::enable();
    let _inject = InjectStoreHardlinkError::enable(std::io::ErrorKind::AlreadyExists);
    let (_, ingest) = store.stage_blob_from_source(&source, true).unwrap();
    assert!(
        matches!(ingest, StoreIngest::Copy(StoreCopyReason::Other)),
        "expected Copy(Other), got {ingest:?}"
    );
    assert!(
        !marker_written(),
        "a non-cross-device ingest failure must not advise"
    );

    // EXDEV advises exactly once per session window, through the log
    // sink when the wrapper selected it (#1067).
    let _ = crate::markers::take_emitted();
    crate::link::set_layout_advice_to_log(true);
    let _inject = InjectStoreHardlinkError::enable(std::io::ErrorKind::CrossesDevices);
    let (_, ingest) = store.stage_blob_from_source(&source, true).unwrap();
    crate::link::set_layout_advice_to_log(false);
    let emitted = crate::markers::take_emitted();
    assert_eq!(emitted.len(), 1, "expected one advisory, got {emitted:?}");
    assert_eq!(emitted[0].0, crate::markers::WarnSink::Log);
    assert!(emitted[0].1.contains("EXDEV"), "{emitted:?}");
    assert!(
        matches!(ingest, StoreIngest::Copy(StoreCopyReason::CrossDevice)),
        "expected Copy(CrossDevice), got {ingest:?}"
    );
    assert!(
        marker_written(),
        "a cross-device ingest fallback must advise"
    );
    crate::link::set_storage_layout_advice(true);
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn ineligible_ingest_records_ineligible_reason() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    // Executables are never hardlink-eligible by policy (#429): the copy
    // records as Ineligible, not as a link failure.
    let source = dir.path().join("out.rlib");
    fs::write(&source, b"ineligible-bytes").unwrap();
    let bytes = fs::metadata(&source).unwrap().len();

    let before = crate::opcounts::store_copy_ineligible_bytes();
    // `allow_hardlink=false` is the cc/policy path: no link attempted.
    // Force past reflink so the policy refusal is exercised even on CoW
    // filesystems where a reflink would otherwise win.
    let _force = ForceStoreHardlink::enable();
    let (staged, ingest) = store.stage_blob_from_source(&source, false).unwrap();
    assert!(
        matches!(ingest, StoreIngest::Copy(StoreCopyReason::Ineligible)),
        "policy refusal must stage as Copy(Ineligible), got {ingest:?}"
    );
    let hash = crate::file_hash::hash_file(&staged).unwrap();
    store
        .publish_staged_blob(&staged, ingest, &hash, bytes)
        .unwrap();
    assert!(
        crate::opcounts::store_copy_ineligible_bytes() >= before + bytes,
        "policy refusal must record the ineligible reason"
    );
}

#[test]
fn eviction_write_pacer_pauses_once_per_full_slice() {
    let slice = Duration::from_millis(50);
    let pause = Duration::from_millis(150);
    let mut pacer = EvictionWritePacer::new(slice, pause);
    assert_eq!(pacer.after_write(Duration::from_millis(30)), None);
    assert_eq!(
        pacer.after_write(Duration::from_millis(20)),
        Some(pause),
        "a slice exactly used up pauses"
    );
    assert_eq!(
        pacer.after_write(Duration::from_millis(49)),
        None,
        "the pause starts a fresh slice"
    );
    assert_eq!(pacer.after_contention(), pause);
    assert_eq!(
        pacer.after_write(Duration::from_millis(49)),
        None,
        "contention starts a fresh slice too"
    );
    assert_eq!(pacer.after_write(Duration::from_millis(1)), Some(pause));
}

/// Put `n` small entries that eviction may remove: unique blobs, idle
/// past the active-pin grace.
fn put_evictable_entries(store: &Store, dir: &Path, n: usize) {
    for i in 0..n {
        let src = dir.join(format!("evictable-{i}.rlib"));
        std::fs::write(&src, format!("evictable payload {i}").repeat(8)).unwrap();
        store
            .put(
                &format!("{i:064x}"),
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(src.clone(), "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        store.remove_clone_for_test(&src);
    }
    store
        .db
        .execute(
            "UPDATE entries SET last_accessed = datetime('now', '-1 hour')",
            [],
        )
        .unwrap();
}

/// A sweep that finds a build holding the index write lock stands off
/// before its next removal. It used to retry the next entry at once,
/// and a build's own writes then competed with a sweep that never let go.
#[test]
fn eviction_stands_off_after_losing_the_write_lock() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path());
    config.deferred_durability = true;
    let store = Store::open(&config).unwrap();
    put_evictable_entries(&store, dir.path(), 3);

    let build = Store::open(&config).unwrap();
    build.db.execute_batch("BEGIN IMMEDIATE").unwrap();

    let mut gc_config = config.clone();
    gc_config.max_size = 1;
    let gc = Store::open(&gc_config).unwrap();
    // Each removal waits out the busy timeout before it gives up. Cut it
    // from 5 s so three lost locks do not take fifteen seconds.
    gc.db.busy_timeout(Duration::from_millis(50)).unwrap();
    let started = std::time::Instant::now();
    let stats = gc.evict().unwrap();
    let elapsed = started.elapsed();
    build.db.execute_batch("ROLLBACK").unwrap();

    assert_eq!(stats.entries_locked, 3, "{stats:?}");
    assert!(
        elapsed >= EVICTION_WRITE_PAUSE * 3,
        "one pause per lost write lock, swept in {elapsed:?}"
    );
}

/// A sweep with enough removals to use up a write slice pauses between
/// slices, so builds waiting on the write lock get it. Without the pause
/// a waiting `put` sat in SQLite's busy handler for most of the sweep.
///
/// The slice is shrunk so a few hundred removals use it up. With the
/// production slice this needed 1500 entries, and a pacer broken into
/// pausing after every removal then slept 150 ms 1500 times, past the
/// mutation lane's timeout.
#[test]
fn eviction_pauses_between_write_slices() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path());
    config.deferred_durability = true;
    let store = Store::open(&config).unwrap();
    put_evictable_entries(&store, dir.path(), 500);

    let mut gc_config = config.clone();
    gc_config.max_size = 1;
    let mut gc = Store::open(&gc_config).unwrap();
    let slice = Duration::from_millis(5);
    let pause = Duration::from_millis(20);
    gc.eviction_pacing = (slice, pause);
    let started = std::time::Instant::now();
    let stats = gc.evict().unwrap();
    let elapsed = started.elapsed();

    assert_eq!(stats.entries_evicted, 500, "{stats:?}");
    let writing = Duration::from_millis(stats.evict_write_ms);
    assert!(
        writing >= slice,
        "fixture too small to use up a slice: {stats:?}"
    );
    assert!(
        elapsed >= writing + pause,
        "{writing:?} of writes must include a pause, swept in {elapsed:?}"
    );
}

/// A size-driven sweep probes for entries still linked into target
/// directories before it removes anything, and skips them without taking
/// the write lock. They used to take it one by one, back to back, to find
/// out the removal had to be refused.
#[test]
fn eviction_skips_entries_it_found_held_without_taking_the_lock() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path());
    config.deferred_durability = true;
    let store = Store::open(&config).unwrap();
    put_evictable_entries(&store, dir.path(), 3);
    // Every blob is still hardlinked into a build's target directory.
    let hashes: Vec<String> = store
        .db
        .prepare("SELECT hash FROM blobs")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(hashes.len(), 3);
    for hash in &hashes {
        let retainer = dir.path().join(format!("retained-{hash}"));
        std::fs::hard_link(store.blob_path(hash), retainer).unwrap();
    }

    let mut gc_config = config.clone();
    gc_config.max_size = 1;
    let mut gc = Store::open(&gc_config).unwrap();
    // A zero slice pauses after every entry that took the lock.
    let pause = Duration::from_secs(5);
    gc.eviction_pacing = (Duration::ZERO, pause);
    let started = std::time::Instant::now();
    let stats = gc.evict().unwrap();
    let elapsed = started.elapsed();

    assert_eq!(stats.entries_unreclaimable, 3, "{stats:?}");
    assert_eq!(stats.evict_write_ms, 0, "{stats:?}");
    assert!(
        elapsed < pause,
        "no entry took the lock, swept in {elapsed:?}"
    );
}

/// Put `key` with a `size`-byte output last used an hour ago, and return
/// its blob's path.
fn put_idle_entry(store: &Store, dir: &Path, key: &str, size: usize) -> PathBuf {
    let output = dir.join(format!("{key}.rlib"));
    // Distinct content per key, so no two entries share a blob.
    let mut bytes = vec![0u8; size];
    bytes[..key.len()].copy_from_slice(key.as_bytes());
    std::fs::write(&output, bytes).unwrap();
    store
        .put(
            key,
            key,
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(output.clone(), format!("lib{key}.rlib"))],
            "",
            "",
        )
        .unwrap();
    store.remove_clone_for_test(&output);
    // Read before aging the entry: a get counts as a use.
    let meta = store.get(key).unwrap().unwrap();
    store
        .db
        .execute(
            "UPDATE entries SET last_accessed = datetime('now', '-1 hour') WHERE cache_key = ?1",
            params![key],
        )
        .unwrap();
    store.blob_path(&meta.files[0].hash)
}

/// Bytes a target directory still holds are not bytes a sweep can free,
/// so they do not count toward what it has to free (#1206). The sweep
/// used to evict every other entry trying to get under a budget those
/// bytes alone kept it over.
#[test]
fn a_size_sweep_leaves_bytes_held_by_target_directories_out_of_its_budget() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path());
    // Trigger at 100 bytes, target 90.
    config.max_size = 100;
    let mut store = Store::open(&config).unwrap();
    let held_blob = put_idle_entry(&store, dir.path(), "held", 200);
    std::fs::hard_link(&held_blob, dir.path().join("target-copy.rlib")).unwrap();
    put_idle_entry(&store, dir.path(), "small", 60);
    assert_eq!(store.physical_size().unwrap(), 260);

    // 260 - 200 held = 60, already under the target.
    let stats = store.evict().unwrap();
    assert_eq!(stats.bytes_held, 200, "{stats:?}");
    assert_eq!(stats.entries_unreclaimable, 1, "{stats:?}");
    assert_eq!(stats.entries_evicted, 0, "{stats:?}");
    assert!(store.contains("held") && store.contains("small"));

    // Over the target even without the held bytes: the free part goes.
    put_idle_entry(&store, dir.path(), "large", 90);
    let stats = store.evict().unwrap();
    assert_eq!(stats.bytes_held, 200, "{stats:?}");
    assert_eq!(stats.entries_evicted, 1, "{stats:?}");
    assert!(store.contains("held"));
    let free_part = store.physical_size().unwrap() - stats.bytes_held;
    assert!(free_part <= 90, "stops at the target: {free_part}");

    // `gc_evict_shared` evicts held entries, so nothing is held back.
    store.config.gc_evict_shared = true;
    let stats = store.evict().unwrap();
    assert_eq!(stats.bytes_held, 0, "{stats:?}");
    assert!(!store.contains("held"));
}

/// A blob two entries share is not either one's last reference, so
/// evicting either would unlink nothing: neither counts as held, even
/// when a target directory holds the blob too.
#[test]
fn a_shared_blob_held_outside_is_no_entrys_held_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path());
    config.max_size = 100;
    let store = Store::open(&config).unwrap();
    for key in ["first", "second"] {
        let output = dir.path().join(format!("{key}.rlib"));
        std::fs::write(&output, vec![7u8; 300]).unwrap();
        store
            .put(
                key,
                key,
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(output.clone(), "libshared.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        store.remove_clone_for_test(&output);
    }
    let meta = store.get("first").unwrap().unwrap();
    std::fs::hard_link(
        store.blob_path(&meta.files[0].hash),
        dir.path().join("target-copy.rlib"),
    )
    .unwrap();
    let candidates = store
        .eviction_candidates_for(SweepOrigin::Requested)
        .unwrap();
    assert!(store.held_by_live_files(&candidates).unwrap().is_empty());
}

/// Only a sweep with a byte budget probes: the others remove what their
/// policy selects whatever it holds.
#[test]
fn only_a_size_sweep_counts_held_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let held_blob = put_idle_entry(&store, dir.path(), "held", 200);
    std::fs::hard_link(&held_blob, dir.path().join("target-copy.rlib")).unwrap();
    let stats = store.evict_older_than(0).unwrap();
    assert_eq!(stats.bytes_held, 0, "{stats:?}");
}

#[test]
fn blob_refcount_drift_reads_the_store_without_the_write_lock() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    let payload = b"probe shared blob";
    for key in ["probe_a", "probe_b"] {
        let output = dir.path().join(format!("{key}.rlib"));
        fs::write(&output, payload).unwrap();
        store
            .put(
                key,
                "probelib",
                &["lib".to_string()],
                &[],
                "host",
                "dev",
                &[(output, format!("lib{key}.rlib"))],
                "",
                "",
            )
            .unwrap();
    }
    assert_eq!(
        store.blob_refcount_drift().unwrap(),
        crate::BlobRefcountDrift::default()
    );

    store
        .db
        .execute_batch(
            "UPDATE blobs SET refcount = refcount + 1;
                 INSERT INTO blobs (hash, size, refcount) VALUES ('unowned', 4096, 2);",
        )
        .unwrap();
    let expected = crate::BlobRefcountDrift {
        unowned: 1,
        unowned_bytes: 4096,
        too_high: 1,
        too_high_bytes: payload.len() as u64,
        ..Default::default()
    };
    assert_eq!(store.blob_refcount_drift().unwrap(), expected);

    // A writer holding the lock does not block the probe.
    let writer = Connection::open(config.index_db_path()).unwrap();
    writer.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert_eq!(store.blob_refcount_drift().unwrap(), expected);
    let ro = open_index_db_readonly(&config.index_db_path()).unwrap();
    assert_eq!(crate::blob_refcount_drift(&ro).unwrap(), expected);
}
