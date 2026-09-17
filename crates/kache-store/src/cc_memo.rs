//! Shared C/C++ memo inputs. Artifact keys and input validation stay compiler-owned.

use crate::file_hash::{FileFingerprint, FileHashCache};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CcPreprocessMemoInput {
    pub name: String,
    #[serde(flatten)]
    pub fingerprint: FileFingerprint,
    pub content: String,
    #[serde(default)]
    pub mapped: String,
}

impl CcPreprocessMemoInput {
    pub fn local_path(&self) -> &str {
        &self.fingerprint.path
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct CcPreprocessMemo {
    pub preprocessed_hash: String,
    pub inputs: Vec<CcPreprocessMemoInput>,
    pub needs_touch: bool,
}

fn has_current_schema(db: &Connection) -> rusqlite::Result<bool> {
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('cc_preprocess_memos') WHERE name = 'input_count')",
        [], |row| row.get(0),
    )
}

pub(crate) fn ensure_schema(db: &Connection) -> rusqlite::Result<()> {
    if has_current_schema(db)? {
        return Ok(());
    }
    // Recheck after acquiring the writer lock: concurrent wrappers may have
    // observed the legacy table before the first migration committed.
    let tx = Transaction::new_unchecked(db, TransactionBehavior::Immediate)?;
    if !has_current_schema(&tx)? {
        tx.execute_batch(
            "DROP TABLE IF EXISTS cc_preprocess_memos;
             CREATE TABLE cc_preprocess_memos (
                id INTEGER PRIMARY KEY,
                memo_key TEXT NOT NULL UNIQUE,
                preprocessed_hash TEXT NOT NULL,
                input_count INTEGER NOT NULL,
                last_used INTEGER NOT NULL DEFAULT (unixepoch())
             );
             CREATE INDEX cc_preprocess_memos_last_used ON cc_preprocess_memos(last_used);
             CREATE TABLE cc_memo_inputs (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                content TEXT NOT NULL,
                mapped TEXT NOT NULL,
                local_path TEXT NOT NULL,
                size INTEGER NOT NULL,
                mtime_ns INTEGER NOT NULL,
                ctime_ns INTEGER NOT NULL,
                inode INTEGER NOT NULL,
                UNIQUE(name, content, mapped)
             );
             CREATE TABLE cc_memo_input_refs (
                memo_id INTEGER NOT NULL REFERENCES cc_preprocess_memos(id),
                input_id INTEGER NOT NULL REFERENCES cc_memo_inputs(id),
                PRIMARY KEY(memo_id, input_id)
             ) WITHOUT ROWID;
             CREATE INDEX cc_memo_input_refs_input ON cc_memo_input_refs(input_id);",
        )?;
    }
    tx.commit()
}

/// The mapped-content memo: what a file's bytes hash to once the prefix maps
/// of one invocation are applied. Keyed by the raw content hash and the map
/// set, never by path, so a header shared by two hundred translation units
/// is read and rewritten once per map set instead of once per unit.
pub(crate) fn ensure_mapped_hash_schema(db: &Connection) -> rusqlite::Result<()> {
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS cc_mapped_hashes (
            content TEXT NOT NULL,
            maps TEXT NOT NULL,
            mapped TEXT NOT NULL,
            PRIMARY KEY(content, maps)
         ) WITHOUT ROWID;
         CREATE TABLE IF NOT EXISTS cc_asm_scans (
            content TEXT PRIMARY KEY,
            construct TEXT NOT NULL
         ) WITHOUT ROWID;",
    )
}

impl FileHashCache<'_> {
    /// Memoised mapped hashes for `contents` under the map set `maps`.
    pub fn get_cc_mapped_hashes(
        &self,
        maps: &str,
        contents: &[&str],
    ) -> rusqlite::Result<std::collections::HashMap<String, String>> {
        let mut found = std::collections::HashMap::new();
        let mut stmt = self.db().prepare_cached(
            "SELECT mapped FROM cc_mapped_hashes WHERE content = ?1 AND maps = ?2",
        )?;
        for content in contents {
            if let Some(mapped) = stmt
                .query_row(params![content, maps], |row| row.get::<_, String>(0))
                .optional()?
            {
                found.insert((*content).to_string(), mapped);
            }
        }
        Ok(found)
    }

    /// Memoised assembler scans for `contents`: the construct found, or an
    /// empty string for a clean file.
    pub fn get_cc_asm_scans(
        &self,
        contents: &[&str],
    ) -> rusqlite::Result<std::collections::HashMap<String, String>> {
        let mut found = std::collections::HashMap::new();
        let mut stmt = self
            .db()
            .prepare_cached("SELECT construct FROM cc_asm_scans WHERE content = ?1")?;
        for content in contents {
            if let Some(construct) = stmt
                .query_row(params![content], |row| row.get::<_, String>(0))
                .optional()?
            {
                found.insert((*content).to_string(), construct);
            }
        }
        Ok(found)
    }

    /// Record assembler scans from this invocation, in one transaction.
    pub fn put_cc_asm_scans(&self, pairs: &[(String, String)]) -> rusqlite::Result<()> {
        if pairs.is_empty() {
            return Ok(());
        }
        let tx = Transaction::new_unchecked(self.db(), TransactionBehavior::Immediate)?;
        {
            let mut put = tx.prepare_cached(
                "INSERT OR IGNORE INTO cc_asm_scans(content, construct) VALUES (?1, ?2)",
            )?;
            for (content, construct) in pairs {
                put.execute(params![content, construct])?;
            }
        }
        tx.commit()
    }

    /// Record mapped hashes computed this invocation, in one transaction.
    pub fn put_cc_mapped_hashes(
        &self,
        maps: &str,
        pairs: &[(String, String)],
    ) -> rusqlite::Result<()> {
        if pairs.is_empty() {
            return Ok(());
        }
        let tx = Transaction::new_unchecked(self.db(), TransactionBehavior::Immediate)?;
        {
            let mut put = tx.prepare_cached(
                "INSERT OR IGNORE INTO cc_mapped_hashes(content, maps, mapped) VALUES (?1, ?2, ?3)",
            )?;
            for (content, mapped) in pairs {
                put.execute(params![content, maps, mapped])?;
            }
        }
        tx.commit()
    }

    /// Compact only during explicit repair. Ordinary opens and GC leave free
    /// SQLite pages available for reuse without blocking builds for VACUUM.
    pub fn compact_sparse_index(&self) -> anyhow::Result<Option<(u64, u64)>> {
        use anyhow::{Context, ensure};
        let db = self.db();
        let pages: u64 = db.pragma_query_value(None, "page_count", |row| {
            row.get::<_, u32>(0).map(u64::from)
        })?;
        let free_pages: u64 = db.pragma_query_value(None, "freelist_count", |row| {
            row.get::<_, u32>(0).map(u64::from)
        })?;
        let page_size: u64 =
            db.pragma_query_value(None, "page_size", |row| row.get::<_, u32>(0).map(u64::from))?;
        let size = pages.saturating_mul(page_size);
        if !should_compact(pages, free_pages, page_size) {
            return Ok(None);
        }
        let path = std::path::Path::new(db.path().context("index has no filesystem path")?);
        let free = kache_fs::volume_usage(path)
            .context("could not check free space for index compaction")?
            .free;
        ensure!(
            free >= size.saturating_mul(2).saturating_add(64 << 20),
            "not enough free space to compact the index; its free pages remain reusable"
        );
        let checkpoint = || -> rusqlite::Result<i64> {
            db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))
        };
        ensure!(
            checkpoint()? == 0,
            "index is busy; retry compaction after builds finish"
        );
        let before = std::fs::metadata(path)?.len();
        db.execute_batch("VACUUM")?;
        ensure!(
            checkpoint()? == 0,
            "index compacted but WAL is busy; retry repair after builds finish"
        );
        Ok(Some((before, std::fs::metadata(path)?.len())))
    }

    pub fn get_cc_preprocess_memo(
        &self,
        memo_key: &str,
    ) -> rusqlite::Result<Option<CcPreprocessMemo>> {
        // One statement provides a single SQLite read snapshot. Reading the
        // digest separately could pair an old digest with a replacement's inputs.
        let mut stmt = self.db().prepare_cached(
            "SELECT m.preprocessed_hash, m.input_count, m.last_used <= unixepoch() - 86400,
                    i.id, i.name, i.content, i.mapped, i.local_path,
                    i.size, i.mtime_ns, i.ctime_ns, i.inode
             FROM cc_preprocess_memos m
             LEFT JOIN cc_memo_input_refs r ON r.memo_id = m.id
             LEFT JOIN cc_memo_inputs i ON i.id = r.input_id
             WHERE m.memo_key = ?1 ORDER BY r.input_id",
        )?;
        let mut rows = stmt.query([memo_key])?;
        let mut result = None;
        let mut expected_count = 0;
        while let Some(row) = rows.next()? {
            if row.get::<_, Option<i64>>(3)?.is_none() {
                return Ok(None);
            }
            if result.is_none() {
                expected_count = row.get::<_, i64>(1)?;
                result = Some(CcPreprocessMemo {
                    preprocessed_hash: row.get(0)?,
                    inputs: Vec::new(),
                    needs_touch: row.get(2)?,
                });
            }
            result.as_mut().unwrap().inputs.push(CcPreprocessMemoInput {
                name: row.get(4)?,
                content: row.get(5)?,
                mapped: row.get(6)?,
                fingerprint: FileFingerprint {
                    path: row.get(7)?,
                    size: row.get(8)?,
                    mtime_ns: row.get(9)?,
                    ctime_ns: row.get(10)?,
                    inode: row.get(11)?,
                },
            });
        }
        // A damaged/incomplete reference set cannot stand in for the full closure.
        Ok(result.filter(|memo| expected_count > 0 && memo.inputs.len() as i64 == expected_count))
    }

    pub fn put_cc_preprocess_memo_inputs(
        &self,
        memo_key: &str,
        preprocessed_hash: &str,
        inputs: &[CcPreprocessMemoInput],
    ) -> rusqlite::Result<()> {
        let tx = Transaction::new_unchecked(self.db(), TransactionBehavior::Immediate)?;
        let id: i64 = tx.query_row(
            "INSERT INTO cc_preprocess_memos(memo_key, preprocessed_hash, input_count)
             VALUES (?1, ?2, 0) ON CONFLICT(memo_key) DO UPDATE SET
             preprocessed_hash = excluded.preprocessed_hash, last_used = unixepoch()
             RETURNING id",
            params![memo_key, preprocessed_hash],
            |row| row.get(0),
        )?;
        tx.execute("DELETE FROM cc_memo_input_refs WHERE memo_id = ?1", [id])?;
        {
            let mut put = tx.prepare_cached(
                "INSERT INTO cc_memo_inputs(name, content, mapped, local_path, size, mtime_ns, ctime_ns, inode)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(name, content, mapped) DO UPDATE SET
                 local_path = excluded.local_path, size = excluded.size,
                 mtime_ns = excluded.mtime_ns, ctime_ns = excluded.ctime_ns, inode = excluded.inode
                 RETURNING id",
            )?;
            let mut reference = tx.prepare_cached(
                "INSERT OR IGNORE INTO cc_memo_input_refs(memo_id, input_id) VALUES (?1, ?2)",
            )?;
            for input in inputs {
                let stamp = &input.fingerprint;
                let input_id: i64 = put.query_row(
                    params![
                        input.name,
                        input.content,
                        input.mapped,
                        stamp.path,
                        stamp.size,
                        stamp.mtime_ns,
                        stamp.ctime_ns,
                        stamp.inode
                    ],
                    |row| row.get(0),
                )?;
                reference.execute(params![id, input_id])?;
            }
        }
        tx.execute(
            "UPDATE cc_preprocess_memos SET input_count =
             (SELECT count(*) FROM cc_memo_input_refs WHERE memo_id = ?1) WHERE id = ?1",
            [id],
        )?;
        tx.commit()
    }

    /// Compatibility for existing compiler validation fixtures; runtime writes
    /// and reads use typed inputs and never persist a JSON payload.
    #[cfg(any(test, feature = "test-support"))]
    pub fn put_cc_preprocess_memo(
        &self,
        memo_key: &str,
        hash: &str,
        json: &str,
    ) -> rusqlite::Result<()> {
        let inputs: Vec<CcPreprocessMemoInput> = serde_json::from_str(json)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        self.put_cc_preprocess_memo_inputs(memo_key, hash, &inputs)
    }

    /// Called after a successful validation and only when the read marked it
    /// due. The SQL guard also bounds updates by competing processes.
    pub fn touch_cc_preprocess_memo(&self, memo_key: &str) -> rusqlite::Result<usize> {
        self.db().execute(
            "UPDATE cc_preprocess_memos SET last_used = unixepoch()
             WHERE memo_key = ?1 AND last_used <= unixepoch() - 86400",
            [memo_key],
        )
    }

    pub fn prune_cc_preprocess_memos(&self) -> rusqlite::Result<(usize, usize)> {
        let tx = Transaction::new_unchecked(self.db(), TransactionBehavior::Immediate)?;
        tx.execute(
            "DELETE FROM cc_memo_input_refs WHERE memo_id IN
             (SELECT id FROM cc_preprocess_memos WHERE last_used < unixepoch() - 2592000)",
            [],
        )?;
        let memos = tx.execute(
            "DELETE FROM cc_preprocess_memos WHERE last_used < unixepoch() - 2592000",
            [],
        )?;
        let inputs = tx.execute(
            "DELETE FROM cc_memo_inputs WHERE NOT EXISTS
             (SELECT 1 FROM cc_memo_input_refs WHERE input_id = cc_memo_inputs.id)",
            [],
        )?;
        tx.commit()?;
        Ok((memos, inputs))
    }
}

fn should_compact(pages: u64, free_pages: u64, page_size: u64) -> bool {
    free_pages.saturating_mul(page_size) >= 64 << 20 && free_pages >= pages.div_ceil(4)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(name: &str, content: &str) -> CcPreprocessMemoInput {
        CcPreprocessMemoInput {
            name: name.into(),
            content: content.into(),
            mapped: format!("mapped-{content}"),
            fingerprint: FileFingerprint {
                path: format!("/checkout/{name}"),
                size: 10,
                mtime_ns: 20,
                ctime_ns: 30,
                inode: 40,
            },
        }
    }

    fn count(cache: &FileHashCache<'_>, table: &str) -> i64 {
        cache
            .db()
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap()
    }

    #[test]
    fn cc_memos_share_inputs_refresh_stamps_and_replace_one_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.db");
        let cache = FileHashCache::open(&path).unwrap();
        let first = input("shared.h", "one");
        cache
            .put_cc_preprocess_memo_inputs("a", "hash-a", std::slice::from_ref(&first))
            .unwrap();
        let mut moved = first.clone();
        moved.fingerprint = FileFingerprint {
            path: "/other/shared.h".into(),
            size: 11,
            mtime_ns: 21,
            ctime_ns: 31,
            inode: 41,
        };
        cache
            .put_cc_preprocess_memo_inputs("b", "hash-b", std::slice::from_ref(&moved))
            .unwrap();
        assert_eq!(count(&cache, "cc_memo_inputs"), 1);
        assert_eq!(count(&cache, "cc_memo_input_refs"), 2);
        let memo = cache.get_cc_preprocess_memo("a").unwrap().unwrap();
        assert_eq!(memo.preprocessed_hash, "hash-a");
        assert_eq!(memo.inputs, vec![moved.clone()]);
        assert!(!memo.needs_touch);
        let second = input("shared.h", "two");
        cache
            .put_cc_preprocess_memo_inputs("a", "hash-c", std::slice::from_ref(&second))
            .unwrap();
        drop(cache);
        let cache = FileHashCache::open(&path).unwrap();
        assert_eq!(
            cache.get_cc_preprocess_memo("a").unwrap().unwrap().inputs,
            vec![second]
        );
        let b = cache.get_cc_preprocess_memo("b").unwrap().unwrap();
        assert_eq!(b.preprocessed_hash, "hash-b");
        assert_eq!(b.inputs, vec![moved]);
        assert_eq!(count(&cache, "cc_memo_inputs"), 2);
        assert!(cache.get_cc_preprocess_memo("absent").unwrap().is_none());
    }

    #[test]
    fn cc_memo_uniqueness_includes_name_and_both_hashes() {
        let dir = tempfile::tempdir().unwrap();
        let cache = FileHashCache::open(&dir.path().join("index.db")).unwrap();
        let base = input("a.h", "raw");
        let mut different_map = base.clone();
        different_map.mapped = "other".into();
        let inputs = vec![
            base.clone(),
            input("b.h", "raw"),
            input("a.h", "changed"),
            different_map,
            base,
        ];
        cache
            .put_cc_preprocess_memo_inputs("unit", "hash", &inputs)
            .unwrap();
        assert_eq!(count(&cache, "cc_memo_inputs"), 4);
        assert_eq!(count(&cache, "cc_memo_input_refs"), 4);
        assert_eq!(
            cache
                .get_cc_preprocess_memo("unit")
                .unwrap()
                .unwrap()
                .inputs
                .len(),
            4
        );
    }

    /// The mapped-hash memo answers by content and map set, the assembler
    /// scan memo by content; both keep what was put across a reopen, and a
    /// duplicate put leaves the first value in place.
    #[test]
    fn mapped_hash_and_asm_scan_memos_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.db");
        let cache = FileHashCache::open(&path).unwrap();
        cache
            .put_cc_mapped_hashes(
                "maps-a",
                &[
                    ("c1".to_string(), "m1".to_string()),
                    ("c2".to_string(), "m2".to_string()),
                ],
            )
            .unwrap();
        cache.put_cc_mapped_hashes("maps-a", &[]).unwrap();
        cache
            .put_cc_asm_scans(&[
                ("c1".to_string(), String::new()),
                ("c3".to_string(), ".incbin".to_string()),
            ])
            .unwrap();
        cache.put_cc_asm_scans(&[]).unwrap();
        drop(cache);
        let cache = FileHashCache::open(&path).unwrap();
        let mapped = cache
            .get_cc_mapped_hashes("maps-a", &["c1", "c2", "c9"])
            .unwrap();
        assert_eq!(mapped.len(), 2);
        assert_eq!(mapped["c1"], "m1");
        assert_eq!(mapped["c2"], "m2");
        assert!(
            cache
                .get_cc_mapped_hashes("maps-b", &["c1"])
                .unwrap()
                .is_empty()
        );
        let scans = cache.get_cc_asm_scans(&["c1", "c3", "c9"]).unwrap();
        assert_eq!(scans.len(), 2);
        assert_eq!(scans["c1"], "");
        assert_eq!(scans["c3"], ".incbin");
        cache
            .put_cc_mapped_hashes("maps-a", &[("c1".to_string(), "changed".to_string())])
            .unwrap();
        cache
            .put_cc_asm_scans(&[("c3".to_string(), String::new())])
            .unwrap();
        assert_eq!(
            cache.get_cc_mapped_hashes("maps-a", &["c1"]).unwrap()["c1"],
            "m1"
        );
        assert_eq!(cache.get_cc_asm_scans(&["c3"]).unwrap()["c3"], ".incbin");
    }

    #[test]
    fn cc_memo_pruning_preserves_shared_live_inputs_and_daily_hits() {
        let dir = tempfile::tempdir().unwrap();
        let cache = FileHashCache::open(&dir.path().join("index.db")).unwrap();
        cache
            .put_cc_preprocess_memo_inputs(
                "old",
                "a",
                &[input("shared.h", "s"), input("orphan.h", "o")],
            )
            .unwrap();
        cache
            .put_cc_preprocess_memo_inputs("live", "b", &[input("shared.h", "s")])
            .unwrap();
        cache
            .db()
            .execute(
                "UPDATE cc_preprocess_memos SET last_used = unixepoch() - 2678400",
                [],
            )
            .unwrap();
        assert!(
            cache
                .get_cc_preprocess_memo("live")
                .unwrap()
                .unwrap()
                .needs_touch
        );
        assert_eq!(cache.touch_cc_preprocess_memo("live").unwrap(), 1);
        assert_eq!(cache.touch_cc_preprocess_memo("live").unwrap(), 0);
        assert!(
            !cache
                .get_cc_preprocess_memo("live")
                .unwrap()
                .unwrap()
                .needs_touch
        );
        assert_eq!(cache.prune_cc_preprocess_memos().unwrap(), (1, 1));
        assert!(cache.get_cc_preprocess_memo("old").unwrap().is_none());
        assert_eq!(
            cache
                .get_cc_preprocess_memo("live")
                .unwrap()
                .unwrap()
                .inputs
                .len(),
            1
        );
        assert_eq!(count(&cache, "cc_memo_input_refs"), 1);
        assert_eq!(cache.prune_cc_preprocess_memos().unwrap(), (0, 0));
    }

    #[test]
    fn cc_memo_missing_references_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let cache = FileHashCache::open(&dir.path().join("index.db")).unwrap();
        cache
            .put_cc_preprocess_memo_inputs("unit", "h", &[input("a.h", "a"), input("b.h", "b")])
            .unwrap();
        cache.db().execute("DELETE FROM cc_memo_input_refs WHERE input_id = (SELECT min(id) FROM cc_memo_inputs)", []).unwrap();
        assert!(cache.get_cc_preprocess_memo("unit").unwrap().is_none());
        cache
            .db()
            .execute_batch("PRAGMA foreign_keys = OFF")
            .unwrap();
        cache
            .db()
            .execute("DELETE FROM cc_memo_inputs", [])
            .unwrap();
        assert!(cache.get_cc_preprocess_memo("unit").unwrap().is_none());
    }

    #[test]
    fn cc_memo_legacy_migration_keeps_other_tables_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.db");
        let db = Connection::open(&path).unwrap();
        db.execute_batch("CREATE TABLE cc_preprocess_memos(memo_key TEXT PRIMARY KEY, preprocessed_hash TEXT, inputs_json TEXT);
            INSERT INTO cc_preprocess_memos VALUES ('old', 'digest', '[{}]');
            CREATE TABLE unrelated(value TEXT); INSERT INTO unrelated VALUES ('keep');").unwrap();
        drop(db);
        let cache = FileHashCache::open(&path).unwrap();
        assert!(cache.get_cc_preprocess_memo("old").unwrap().is_none());
        assert_eq!(
            cache
                .db()
                .query_row("SELECT value FROM unrelated", [], |row| row
                    .get::<_, String>(0))
                .unwrap(),
            "keep"
        );
        cache
            .put_cc_preprocess_memo_inputs("new", "hash", &[input("a.h", "a")])
            .unwrap();
        ensure_schema(cache.db()).unwrap();
        assert!(cache.get_cc_preprocess_memo("new").unwrap().is_some());
        assert_eq!(cache.db().query_row("SELECT count(*) FROM pragma_table_info('cc_preprocess_memos') WHERE name = 'inputs_json'", [], |row| row.get::<_, i64>(0)).unwrap(), 0);
    }

    #[test]
    fn cc_memo_repeated_headers_have_bounded_storage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.db");
        let cache = FileHashCache::open(&path).unwrap();
        let inputs: Vec<_> = (0..100)
            .map(|i| {
                input(
                    &format!("include/{i:04}-{}.h", "x".repeat(100)),
                    &"a".repeat(64),
                )
            })
            .collect();
        let old_payload_bytes = serde_json::to_vec(&inputs).unwrap().len() * 200;
        for i in 0..200 {
            cache
                .put_cc_preprocess_memo_inputs(&format!("{i:064}"), "hash", &inputs)
                .unwrap();
        }
        assert_eq!(count(&cache, "cc_memo_inputs"), 100);
        assert_eq!(count(&cache, "cc_memo_input_refs"), 20_000);
        let pages: u64 = cache
            .db()
            .pragma_query_value(None, "page_count", |r| r.get::<_, u32>(0).map(u64::from))
            .unwrap();
        let size: u64 = cache
            .db()
            .pragma_query_value(None, "page_size", |r| r.get::<_, u32>(0).map(u64::from))
            .unwrap();
        assert!(
            pages * size < old_payload_bytes as u64 / 5,
            "{} bytes vs {} bytes of legacy payloads",
            pages * size,
            old_payload_bytes
        );
    }

    #[test]
    fn index_compaction_requires_both_size_and_fraction() {
        assert!(!should_compact(100_000, 25_000, 1));
        assert!(!should_compact(1_000_000, 20_000, 4096));
        assert!(should_compact(65_536, 16_384, 4096));
        assert!(!should_compact(65_537, 16_384, 4096));
    }

    #[test]
    fn cc_memo_failed_replacement_rolls_back_the_whole_record() {
        let dir = tempfile::tempdir().unwrap();
        let cache = FileHashCache::open(&dir.path().join("index.db")).unwrap();
        cache
            .put_cc_preprocess_memo_inputs("unit", "old", &[input("old.h", "old")])
            .unwrap();
        cache
            .db()
            .execute_batch(
                "CREATE TRIGGER reject_reference BEFORE INSERT ON cc_memo_input_refs
            BEGIN SELECT RAISE(ABORT, 'injected failure'); END;",
            )
            .unwrap();
        assert!(
            cache
                .put_cc_preprocess_memo_inputs("unit", "new", &[input("new.h", "new")])
                .is_err()
        );
        let memo = cache.get_cc_preprocess_memo("unit").unwrap().unwrap();
        assert_eq!(memo.preprocessed_hash, "old");
        assert_eq!(memo.inputs, vec![input("old.h", "old")]);
        assert_eq!(count(&cache, "cc_memo_inputs"), 1);
        assert_eq!(count(&cache, "cc_memo_input_refs"), 1);
    }

    #[test]
    fn cc_memo_concurrent_reads_never_mix_digest_and_inputs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.db");
        let cache = FileHashCache::open(&path).unwrap();
        cache
            .put_cc_preprocess_memo_inputs("unit", "a", &[input("a.h", "a")])
            .unwrap();
        let writer = std::thread::spawn(move || {
            let cache = FileHashCache::open(&path).unwrap();
            for i in 0..300 {
                let value = if i % 2 == 0 { "a" } else { "b" };
                cache
                    .put_cc_preprocess_memo_inputs(
                        "unit",
                        value,
                        &[input(&format!("{value}.h"), value)],
                    )
                    .unwrap();
            }
        });
        for _ in 0..600 {
            let memo = cache.get_cc_preprocess_memo("unit").unwrap().unwrap();
            assert_eq!(memo.inputs.len(), 1);
            assert_eq!(memo.preprocessed_hash, memo.inputs[0].content);
        }
        writer.join().unwrap();
    }

    #[test]
    #[cfg(unix)]
    fn sparse_index_repair_reclaims_legacy_pages_and_preserves_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.db");
        let db = Connection::open(&path).unwrap();
        db.execute_batch(
            "CREATE TABLE cc_preprocess_memos(memo_key TEXT PRIMARY KEY, inputs_json TEXT);
            INSERT INTO cc_preprocess_memos VALUES ('legacy', zeroblob(83886080));",
        )
        .unwrap();
        drop(db);
        let cache = FileHashCache::open(&path).unwrap();
        cache
            .put_cc_preprocess_memo_inputs("live", "digest", &[input("live.h", "live")])
            .unwrap();
        let (before, after) = cache
            .compact_sparse_index()
            .unwrap()
            .expect("large freelist must compact");
        assert!(before >= 80 << 20);
        assert!(after < before / 4);
        assert_eq!(
            cache
                .get_cc_preprocess_memo("live")
                .unwrap()
                .unwrap()
                .preprocessed_hash,
            "digest"
        );
        assert_eq!(cache.compact_sparse_index().unwrap(), None);
    }

    #[test]
    fn a_memo_input_names_its_local_path() {
        let first = input("shared.h", "one");
        assert_eq!(first.local_path(), "/checkout/shared.h");
    }

    #[test]
    fn a_memo_without_inputs_is_not_a_memo() {
        let dir = tempfile::tempdir().unwrap();
        let cache = FileHashCache::open(&dir.path().join("index.db")).unwrap();
        cache
            .put_cc_preprocess_memo_inputs("empty", "hash-e", &[])
            .unwrap();
        assert!(
            cache.get_cc_preprocess_memo("empty").unwrap().is_none(),
            "a preprocess always reads at least the source"
        );
        let json = serde_json::to_string(&[input("a.h", "one")]).unwrap();
        cache
            .put_cc_preprocess_memo("json", "hash-j", &json)
            .unwrap();
        let memo = cache.get_cc_preprocess_memo("json").unwrap().unwrap();
        assert_eq!(memo.preprocessed_hash, "hash-j");
        assert_eq!(memo.inputs, vec![input("a.h", "one")]);
        assert!(
            cache
                .put_cc_preprocess_memo("bad", "hash-b", "not json")
                .is_err()
        );
    }
}
