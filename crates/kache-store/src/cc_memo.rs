//! Shared C/C++ memo inputs. Artifact keys and input validation stay compiler-owned.

use crate::file_hash::{FileFingerprint, FileHashCache, stamp_is_settled};
use rusqlite::{Connection, Transaction, TransactionBehavior, params};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CcPreprocessMemoInput {
    pub name: String,
    /// For a row read back whose stamp was never proven, `size` is -1,
    /// which no file has.
    #[serde(flatten)]
    pub fingerprint: FileFingerprint,
    pub content: String,
    #[serde(default)]
    pub mapped: String,
    /// Wall clock read just before `fingerprint` was taken. 0 when unknown:
    /// an input read back from the index, or one sent by an older wrapper.
    #[serde(default)]
    pub observed_ns: i64,
}

impl CcPreprocessMemoInput {
    pub fn local_path(&self) -> &str {
        &self.fingerprint.path
    }

    /// Whether `fingerprint` had settled when it was taken, so that any later
    /// write moves it (see [`stamp_is_settled`]). Only such a stamp may stand
    /// in for the content on a later lookup.
    pub fn stamp_settled(&self) -> bool {
        stamp_is_settled(&self.fingerprint, self.observed_ns)
    }
}

/// The size an input row records in place of a stamp that had not settled
/// when it was taken. No file has it, so no lookup matches the row by stamp
/// and the content is compared instead. A row whose stamp has no matching
/// proof reads back with it too.
pub const UNPROVEN_SIZE: i64 = -1;

/// The rule the C/C++ memo's rows are recorded under. Lookups serve a
/// mapped hash or an assembler verdict only at this rule or a later one,
/// and trust an input row's stamp only while the row's proof matches it.
/// A release from before these columns writes neither, so nothing it
/// records is served, before an upgrade or after it.
///
/// 1: a stamp is recorded only if it had settled when it was observed,
///    and a mapped hash or a verdict only from the bytes behind its
///    content hash.
pub const CC_MEMO_RULE: i64 = 1;

/// What an input row's `proof` holds for `stamp` under [`CC_MEMO_RULE`].
///
/// An older release records the same bytes under the same name by
/// rewriting the shared row's stamp in place, and leaves `proof` alone. A
/// stamp it wrote, possibly one taken moments after a write, then no longer
/// matches the proof and proves nothing. One it rewrote unchanged still
/// does: this release saw that stamp settle.
fn stamp_proof(stamp: &FileFingerprint) -> i64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&CC_MEMO_RULE.to_le_bytes());
    hasher.update(stamp.path.as_bytes());
    hasher.update(&[0]);
    for value in [stamp.size, stamp.mtime_ns, stamp.ctime_ns, stamp.inode] {
        hasher.update(&value.to_le_bytes());
    }
    let mut proof = [0; 8];
    proof.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
    i64::from_le_bytes(proof)
}

/// The stamp an input row read back with no proof stands for: one that
/// matches no file.
fn unproven(path: String) -> FileFingerprint {
    FileFingerprint {
        path,
        size: UNPROVEN_SIZE,
        mtime_ns: 0,
        ctime_ns: 0,
        inode: 0,
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
                proof INTEGER,
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
            rule INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(content, maps)
         ) WITHOUT ROWID;
         CREATE TABLE IF NOT EXISTS cc_asm_scans (
            content TEXT PRIMARY KEY,
            construct TEXT NOT NULL,
            rule INTEGER NOT NULL DEFAULT 0
         ) WITHOUT ROWID;",
    )
}

/// The columns [`CC_MEMO_RULE`] reads, as `ALTER TABLE` adds them to a
/// table from before them.
const RULE_COLUMNS: [(&str, &str, &str); 3] = [
    ("cc_memo_inputs", "proof", "proof INTEGER"),
    (
        "cc_mapped_hashes",
        "rule",
        "rule INTEGER NOT NULL DEFAULT 0",
    ),
    ("cc_asm_scans", "rule", "rule INTEGER NOT NULL DEFAULT 0"),
];

fn has_column(db: &Connection, table: &str, column: &str) -> rusqlite::Result<bool> {
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info(?1) WHERE name = ?2)",
        params![table, column],
        |row| row.get(0),
    )
}

/// Whether every column [`CC_MEMO_RULE`] reads is present.
pub(crate) fn has_rule_columns(db: &Connection) -> rusqlite::Result<bool> {
    for (table, column, _) in RULE_COLUMNS {
        if !has_column(db, table, column)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Add the columns [`CC_MEMO_RULE`] reads to tables from before them, and
/// [`purge`] the memo once as they arrive. Checked again under the write
/// lock, so the purge runs once however many processes open the index
/// together. Gated on the columns rather than the index generation, which
/// an older release stamps back over a newer one each time it opens the
/// index; no older release drops these columns.
pub(crate) fn ensure_rule(db: &Connection) -> rusqlite::Result<()> {
    if has_rule_columns(db)? {
        return Ok(());
    }
    add_rule_columns(db)
}

/// The part of [`ensure_rule`] that runs under the write lock, once a look
/// without it found a column missing. Another process may have added the
/// columns since and recorded rows under them, so it looks again before it
/// purges.
fn add_rule_columns(db: &Connection) -> rusqlite::Result<()> {
    let tx = Transaction::new_unchecked(db, TransactionBehavior::Immediate)?;
    if !has_rule_columns(&tx)? {
        for (table, column, definition) in RULE_COLUMNS {
            if !has_column(&tx, table, column)? {
                tx.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {definition}"))?;
            }
        }
        purge(&tx)?;
    }
    tx.commit()
}

/// Forget every C/C++ memo, with the mapped hashes and assembler verdicts
/// beside them. [`ensure_rule`] runs this once for rows an older kache
/// recorded: it trusted a stamp however soon after a write it was taken,
/// and it learned a mapped hash or a verdict from a second read without
/// checking that read saw the bytes behind the content hash.
fn purge(db: &Connection) -> rusqlite::Result<()> {
    db.execute_batch(
        "DELETE FROM cc_memo_input_refs;
         DELETE FROM cc_preprocess_memos;
         DELETE FROM cc_memo_inputs;
         DELETE FROM cc_mapped_hashes;
         DELETE FROM cc_asm_scans;",
    )
}

/// Headers looked up per statement. Far below SQLite's bound on bound
/// parameters, and a round number of prepared-statement shapes to cache.
const MEMO_LOOKUP_CHUNK: usize = 256;

impl FileHashCache<'_> {
    /// Memoised mapped hashes for `contents` under the map set `maps`,
    /// recorded under `CC_MEMO_RULE` or a later rule.
    pub fn get_cc_mapped_hashes(
        &self,
        maps: &str,
        contents: &[&str],
    ) -> rusqlite::Result<std::collections::HashMap<String, String>> {
        let mut found = std::collections::HashMap::new();
        // One statement per chunk instead of one per header: a translation
        // unit reads a hundred or more, and the per-statement cost was most
        // of this lookup.
        for chunk in contents.chunks(MEMO_LOOKUP_CHUNK) {
            let placeholders = vec!["?"; chunk.len()].join(",");
            let mut stmt = self.db().prepare_cached(&format!(
                "SELECT content, mapped FROM cc_mapped_hashes
                 WHERE maps = ?1 AND content IN ({placeholders}) AND rule >= {CC_MEMO_RULE}"
            ))?;
            let args = std::iter::once(maps).chain(chunk.iter().copied());
            let rows = stmt.query_map(rusqlite::params_from_iter(args), |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            for row in rows {
                let (content, mapped) = row?;
                found.insert(content, mapped);
            }
        }
        Ok(found)
    }

    /// Memoised assembler scans for `contents`: the construct found, or an
    /// empty string for a clean file. Only verdicts recorded under
    /// `CC_MEMO_RULE` or a later rule count.
    pub fn get_cc_asm_scans(
        &self,
        contents: &[&str],
    ) -> rusqlite::Result<std::collections::HashMap<String, String>> {
        let mut found = std::collections::HashMap::new();
        for chunk in contents.chunks(MEMO_LOOKUP_CHUNK) {
            let placeholders = vec!["?"; chunk.len()].join(",");
            let mut stmt = self.db().prepare_cached(&format!(
                "SELECT content, construct FROM cc_asm_scans
                 WHERE content IN ({placeholders}) AND rule >= {CC_MEMO_RULE}"
            ))?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter().copied()), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?;
            for row in rows {
                let (content, construct) = row?;
                found.insert(content, construct);
            }
        }
        Ok(found)
    }

    /// Record assembler scans from this invocation, in one transaction. A
    /// verdict an older rule recorded is replaced.
    pub fn put_cc_asm_scans(&self, pairs: &[(String, String)]) -> rusqlite::Result<()> {
        if pairs.is_empty() {
            return Ok(());
        }
        let tx = Transaction::new_unchecked(self.db(), TransactionBehavior::Immediate)?;
        {
            let mut put = tx.prepare_cached(&format!(
                "INSERT INTO cc_asm_scans(content, construct, rule) VALUES (?1, ?2, {CC_MEMO_RULE})
                 ON CONFLICT(content) DO UPDATE SET
                 construct = excluded.construct, rule = excluded.rule
                 WHERE cc_asm_scans.rule < excluded.rule"
            ))?;
            for (content, construct) in pairs {
                put.execute(params![content, construct])?;
            }
        }
        tx.commit()
    }

    /// Record mapped hashes computed this invocation, in one transaction. A
    /// mapped hash an older rule recorded is replaced.
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
            let mut put = tx.prepare_cached(&format!(
                "INSERT INTO cc_mapped_hashes(content, maps, mapped, rule)
                 VALUES (?1, ?2, ?3, {CC_MEMO_RULE})
                 ON CONFLICT(content, maps) DO UPDATE SET
                 mapped = excluded.mapped, rule = excluded.rule
                 WHERE cc_mapped_hashes.rule < excluded.rule"
            ))?;
            for (content, mapped) in pairs {
                put.execute(params![content, maps, mapped])?;
            }
        }
        tx.commit()
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
                    i.size, i.mtime_ns, i.ctime_ns, i.inode, i.proof
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
            let stamp = FileFingerprint {
                path: row.get(7)?,
                size: row.get(8)?,
                mtime_ns: row.get(9)?,
                ctime_ns: row.get(10)?,
                inode: row.get(11)?,
            };
            let proven = row.get::<_, Option<i64>>(12)? == Some(stamp_proof(&stamp));
            result.as_mut().unwrap().inputs.push(CcPreprocessMemoInput {
                name: row.get(4)?,
                content: row.get(5)?,
                mapped: row.get(6)?,
                fingerprint: if proven { stamp } else { unproven(stamp.path) },
                observed_ns: 0,
            });
        }
        // A damaged/incomplete reference set cannot stand in for the full closure.
        Ok(result.filter(|memo| expected_count > 0 && memo.inputs.len() as i64 == expected_count))
    }

    /// Record `inputs` as the read set behind `preprocessed_hash`.
    ///
    /// Input rows are shared by every memo that read the same bytes under
    /// the same name. An input whose stamp had settled when it was observed
    /// gives the row its stamp, and the proof that lets a lookup trust it.
    /// One observed sooner never does: a second write in the same timestamp
    /// tick keeps the stamp and changes the bytes, so the row keeps the
    /// stamp it has, or starts with none.
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
                "INSERT INTO cc_memo_inputs(name, content, mapped, local_path, size, mtime_ns, ctime_ns, inode, proof)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(name, content, mapped) DO UPDATE SET
                 local_path = excluded.local_path, size = excluded.size,
                 mtime_ns = excluded.mtime_ns, ctime_ns = excluded.ctime_ns, inode = excluded.inode,
                 proof = excluded.proof
                 RETURNING id",
            )?;
            let mut put_unproven = tx.prepare_cached(
                "INSERT INTO cc_memo_inputs(name, content, mapped, local_path, size, mtime_ns, ctime_ns, inode)
                 VALUES (?1, ?2, ?3, ?4, ?5, 0, 0, 0)
                 ON CONFLICT(name, content, mapped) DO UPDATE SET name = excluded.name
                 RETURNING id",
            )?;
            let mut reference = tx.prepare_cached(
                "INSERT OR IGNORE INTO cc_memo_input_refs(memo_id, input_id) VALUES (?1, ?2)",
            )?;
            for input in inputs {
                let stamp = &input.fingerprint;
                let input_id: i64 = if input.stamp_settled() {
                    put.query_row(
                        params![
                            input.name,
                            input.content,
                            input.mapped,
                            stamp.path,
                            stamp.size,
                            stamp.mtime_ns,
                            stamp.ctime_ns,
                            stamp.inode,
                            stamp_proof(stamp)
                        ],
                        |row| row.get(0),
                    )?
                } else {
                    put_unproven.query_row(
                        params![
                            input.name,
                            input.content,
                            input.mapped,
                            stamp.path,
                            UNPROVEN_SIZE
                        ],
                        |row| row.get(0),
                    )?
                };
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_hash::HASH_SETTLE_NS;

    /// Observed long after any stamp these tests write, so they count as
    /// settled.
    const SETTLED: i64 = 10 * HASH_SETTLE_NS;

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
            observed_ns: SETTLED,
        }
    }

    /// `input` as a lookup reads it back: the index keeps no observation
    /// time.
    fn read_back(mut input: CcPreprocessMemoInput) -> CcPreprocessMemoInput {
        input.observed_ns = 0;
        input
    }

    fn count(cache: &FileHashCache<'_>, table: &str) -> i64 {
        cache
            .db()
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap()
    }

    /// Every process that opens an older index looks for the rule columns
    /// before it takes the write lock, and each that finds one missing goes
    /// on to take it. Only the first may purge: by the time a later one holds
    /// the lock, the columns are there and the rows under them are current.
    #[test]
    fn the_purge_runs_once_however_many_openers_found_a_column_missing() {
        const MEMO_TABLES: [&str; 5] = [
            "cc_preprocess_memos",
            "cc_memo_inputs",
            "cc_memo_input_refs",
            "cc_mapped_hashes",
            "cc_asm_scans",
        ];
        let dir = tempfile::tempdir().unwrap();
        let cache = FileHashCache::open(&dir.path().join("index.db")).unwrap();
        let record = |cache: &FileHashCache<'_>| {
            cache
                .put_cc_preprocess_memo_inputs("memo", "hash", &[input("a.h", "one")])
                .unwrap();
            cache
                .put_cc_mapped_hashes("maps", &[("one".into(), "mapped".into())])
                .unwrap();
            cache
                .put_cc_asm_scans(&[("one".into(), String::new())])
                .unwrap();
        };
        let rows = |cache: &FileHashCache<'_>| MEMO_TABLES.map(|table| count(cache, table));
        record(&cache);
        cache
            .db()
            .execute_batch(
                "ALTER TABLE cc_memo_inputs DROP COLUMN proof;
                 ALTER TABLE cc_mapped_hashes DROP COLUMN rule;
                 ALTER TABLE cc_asm_scans DROP COLUMN rule;",
            )
            .unwrap();
        assert!(!has_rule_columns(cache.db()).unwrap());

        // The first opener to take the lock adds the columns and purges.
        ensure_rule(cache.db()).unwrap();
        assert!(has_rule_columns(cache.db()).unwrap());
        assert_eq!(rows(&cache), [0; 5]);

        // A second opener found a column missing before that, and takes the
        // lock only now, after rows were recorded under the columns.
        record(&cache);
        assert_eq!(rows(&cache), [1; 5]);
        add_rule_columns(cache.db()).unwrap();
        assert_eq!(rows(&cache), [1; 5], "the purge runs once");
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
        assert_eq!(memo.inputs, vec![read_back(moved.clone())]);
        assert!(!memo.needs_touch);
        let second = input("shared.h", "two");
        cache
            .put_cc_preprocess_memo_inputs("a", "hash-c", std::slice::from_ref(&second))
            .unwrap();
        drop(cache);
        let cache = FileHashCache::open(&path).unwrap();
        assert_eq!(
            cache.get_cc_preprocess_memo("a").unwrap().unwrap().inputs,
            vec![read_back(second)]
        );
        let b = cache.get_cc_preprocess_memo("b").unwrap().unwrap();
        assert_eq!(b.preprocessed_hash, "hash-b");
        assert_eq!(b.inputs, vec![read_back(moved)]);
        assert_eq!(count(&cache, "cc_memo_inputs"), 2);
        assert!(cache.get_cc_preprocess_memo("absent").unwrap().is_none());
    }

    /// A stamp taken less than the settle window after its file changed is
    /// never recorded: the memo keeps the input's bytes and no stamp, and a
    /// row another memo proved keeps its own stamp.
    #[test]
    fn an_unsettled_stamp_is_never_recorded_nor_shared() {
        let dir = tempfile::tempdir().unwrap();
        let cache = FileHashCache::open(&dir.path().join("index.db")).unwrap();
        let just_before_settling = |input: &CcPreprocessMemoInput| {
            let changed = input.fingerprint.mtime_ns.max(input.fingerprint.ctime_ns);
            changed + HASH_SETTLE_NS - 1
        };

        let mut fresh = input("fresh.h", "f");
        fresh.observed_ns = just_before_settling(&fresh);
        cache
            .put_cc_preprocess_memo_inputs("fresh", "h", std::slice::from_ref(&fresh))
            .unwrap();
        let recorded = cache.get_cc_preprocess_memo("fresh").unwrap().unwrap();
        let recorded = &recorded.inputs[0];
        assert_eq!(recorded.fingerprint.size, UNPROVEN_SIZE);
        assert_eq!(recorded.fingerprint.path, fresh.fingerprint.path);
        assert_eq!(
            (&recorded.content, &recorded.mapped),
            (&fresh.content, &fresh.mapped)
        );

        let proven = input("shared.h", "s");
        cache
            .put_cc_preprocess_memo_inputs("a", "h", std::slice::from_ref(&proven))
            .unwrap();
        let mut elsewhere = proven.clone();
        elsewhere.fingerprint = FileFingerprint {
            path: "/other/shared.h".into(),
            size: 10,
            mtime_ns: 50,
            ctime_ns: 60,
            inode: 41,
        };
        elsewhere.observed_ns = just_before_settling(&elsewhere);
        cache
            .put_cc_preprocess_memo_inputs("b", "h", std::slice::from_ref(&elsewhere))
            .unwrap();
        assert_eq!(count(&cache, "cc_memo_inputs"), 2, "one row per bytes");
        for key in ["a", "b"] {
            assert_eq!(
                cache.get_cc_preprocess_memo(key).unwrap().unwrap().inputs,
                vec![read_back(proven.clone())],
                "{key}: the unsettled stamp replaced the proven one"
            );
        }

        // One tick later the same observation is proof and takes the row.
        elsewhere.observed_ns += 1;
        cache
            .put_cc_preprocess_memo_inputs("b", "h", std::slice::from_ref(&elsewhere))
            .unwrap();
        assert_eq!(
            cache.get_cc_preprocess_memo("a").unwrap().unwrap().inputs,
            vec![read_back(elsewhere)]
        );
    }

    /// A hand-off from a wrapper that predates observation times carries
    /// none, and its stamps count as unsettled.
    #[test]
    fn an_input_without_an_observation_time_is_unsettled() {
        let mut json = serde_json::to_value(input("a.h", "a")).unwrap();
        json.as_object_mut().unwrap().remove("observed_ns");
        let old: CcPreprocessMemoInput = serde_json::from_value(json).unwrap();
        assert_eq!(old.observed_ns, 0);
        assert!(!old.stamp_settled());
        assert!(input("a.h", "a").stamp_settled());
    }

    /// How 1.0.0 records a memo: it rewrites the stamp of each input row
    /// its bytes share and names no proof.
    fn record_as_older_release(
        cache: &FileHashCache<'_>,
        memo_key: &str,
        inputs: &[CcPreprocessMemoInput],
    ) {
        let db = cache.db();
        let memo: i64 = db
            .query_row(
                "INSERT INTO cc_preprocess_memos(memo_key, preprocessed_hash, input_count)
                 VALUES (?1, 'older', 0) ON CONFLICT(memo_key) DO UPDATE SET
                 preprocessed_hash = excluded.preprocessed_hash, last_used = unixepoch()
                 RETURNING id",
                [memo_key],
                |row| row.get(0),
            )
            .unwrap();
        db.execute("DELETE FROM cc_memo_input_refs WHERE memo_id = ?1", [memo])
            .unwrap();
        for input in inputs {
            let stamp = &input.fingerprint;
            let id: i64 = db
                .query_row(
                    "INSERT INTO cc_memo_inputs(name, content, mapped, local_path, size, mtime_ns, ctime_ns, inode)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                     ON CONFLICT(name, content, mapped) DO UPDATE SET
                     local_path = excluded.local_path, size = excluded.size,
                     mtime_ns = excluded.mtime_ns, ctime_ns = excluded.ctime_ns, inode = excluded.inode
                     RETURNING id",
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
                )
                .unwrap();
            db.execute(
                "INSERT OR IGNORE INTO cc_memo_input_refs(memo_id, input_id) VALUES (?1, ?2)",
                params![memo, id],
            )
            .unwrap();
        }
        db.execute(
            "UPDATE cc_preprocess_memos SET input_count =
             (SELECT count(*) FROM cc_memo_input_refs WHERE memo_id = ?1) WHERE id = ?1",
            [memo],
        )
        .unwrap();
    }

    /// An older release that records the same bytes rewrites the shared
    /// row's stamp in place. A stamp it changed proves nothing until this
    /// release records one again; one it rewrote unchanged still does. A row
    /// only the older release wrote never proves its input.
    #[test]
    fn a_stamp_an_older_release_rewrote_proves_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let cache = FileHashCache::open(&dir.path().join("index.db")).unwrap();
        let stamp_of = |memo_key: &str| {
            cache
                .get_cc_preprocess_memo(memo_key)
                .unwrap()
                .unwrap()
                .inputs[0]
                .fingerprint
                .clone()
        };
        let proven = input("shared.h", "s");
        cache
            .put_cc_preprocess_memo_inputs("a", "h", std::slice::from_ref(&proven))
            .unwrap();
        assert_eq!(stamp_of("a"), proven.fingerprint);

        record_as_older_release(&cache, "older", std::slice::from_ref(&proven));
        assert_eq!(stamp_of("a"), proven.fingerprint, "rewritten unchanged");

        for field in 0..5 {
            cache
                .put_cc_preprocess_memo_inputs("a", "h", std::slice::from_ref(&proven))
                .unwrap();
            assert_eq!(stamp_of("a"), proven.fingerprint, "field {field}");
            let mut rewritten = proven.clone();
            let stamp = &mut rewritten.fingerprint;
            match field {
                0 => stamp.path = "/other/shared.h".into(),
                1 => stamp.size += 1,
                2 => stamp.mtime_ns += 1,
                3 => stamp.ctime_ns += 1,
                _ => stamp.inode += 1,
            }
            record_as_older_release(&cache, "older", std::slice::from_ref(&rewritten));
            assert_eq!(
                stamp_of("a"),
                unproven(rewritten.fingerprint.path.clone()),
                "field {field}"
            );
            cache
                .put_cc_preprocess_memo_inputs("a", "h", std::slice::from_ref(&rewritten))
                .unwrap();
            assert_eq!(stamp_of("a"), rewritten.fingerprint, "field {field}");
        }

        let other = input("other.h", "o");
        record_as_older_release(&cache, "older", std::slice::from_ref(&other));
        assert_eq!(stamp_of("older"), unproven(other.fingerprint.path));
    }

    /// A mapped hash or verdict an older release recorded is never served,
    /// and this release's put replaces it. The older release only ever
    /// inserts, so it leaves what this release recorded alone.
    #[test]
    fn a_mapped_hash_or_verdict_an_older_release_recorded_is_never_served() {
        let dir = tempfile::tempdir().unwrap();
        let cache = FileHashCache::open(&dir.path().join("index.db")).unwrap();
        // The statements 1.0.0 records them with.
        let record_as_older_release = |value: &str| {
            cache
                .db()
                .execute(
                    "INSERT OR IGNORE INTO cc_mapped_hashes(content, maps, mapped)
                     VALUES ('c', 'maps', ?1)",
                    [value],
                )
                .unwrap();
            cache
                .db()
                .execute(
                    "INSERT OR IGNORE INTO cc_asm_scans(content, construct) VALUES ('c', ?1)",
                    [value],
                )
                .unwrap();
        };
        record_as_older_release("older");
        assert!(
            cache
                .get_cc_mapped_hashes("maps", &["c"])
                .unwrap()
                .is_empty()
        );
        assert!(cache.get_cc_asm_scans(&["c"]).unwrap().is_empty());

        cache
            .put_cc_mapped_hashes("maps", &[("c".into(), "current".into())])
            .unwrap();
        cache
            .put_cc_asm_scans(&[("c".into(), String::new())])
            .unwrap();
        record_as_older_release("older again");
        let mapped = cache.get_cc_mapped_hashes("maps", &["c"]).unwrap();
        assert_eq!(mapped.get("c").map(String::as_str), Some("current"));
        let verdicts = cache.get_cc_asm_scans(&["c"]).unwrap();
        assert_eq!(verdicts.get("c").map(String::as_str), Some(""));
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

    /// A lookup larger than one statement's chunk still finds every row,
    /// and an empty lookup issues nothing.
    #[test]
    fn cc_memo_lookups_span_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.db");
        let cache = FileHashCache::open(&path).unwrap();
        let n = MEMO_LOOKUP_CHUNK * 2 + 3;
        let pairs: Vec<(String, String)> = (0..n)
            .map(|i| (format!("content-{i}"), format!("mapped-{i}")))
            .collect();
        cache.put_cc_mapped_hashes("maps", &pairs).unwrap();
        cache.put_cc_asm_scans(&pairs).unwrap();
        let mut wanted: Vec<&str> = pairs.iter().map(|(c, _)| c.as_str()).collect();
        wanted.push("content-missing");
        let mapped = cache.get_cc_mapped_hashes("maps", &wanted).unwrap();
        assert_eq!(mapped.len(), n);
        assert_eq!(
            mapped[&format!("content-{}", n - 1)],
            format!("mapped-{}", n - 1)
        );
        assert_eq!(mapped["content-0"], "mapped-0");
        let scans = cache.get_cc_asm_scans(&wanted).unwrap();
        assert_eq!(scans.len(), n);
        assert_eq!(
            scans[&format!("content-{}", MEMO_LOOKUP_CHUNK)],
            format!("mapped-{}", MEMO_LOOKUP_CHUNK)
        );
        assert!(cache.get_cc_mapped_hashes("maps", &[]).unwrap().is_empty());
        assert!(cache.get_cc_asm_scans(&[]).unwrap().is_empty());
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
        assert_eq!(memo.inputs, vec![read_back(input("old.h", "old"))]);
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
        assert_eq!(memo.inputs, vec![read_back(input("a.h", "one"))]);
        assert!(
            cache
                .put_cc_preprocess_memo("bad", "hash-b", "not json")
                .is_err()
        );
    }
}
