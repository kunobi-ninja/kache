//! A SQL-only probe for disagreement between `blobs` and `entry_blobs`.

use rusqlite::Connection;

/// Disagreement between the two derived blob tables, read without touching
/// `meta.json` and without the write lock.
///
/// This compares `blobs.refcount` with `SUM(entry_blobs.refs)` and nothing
/// else. It is the drift seen on long-lived stores, where a leaked refcount
/// keeps bytes counted against `max_size` that no entry owns. It cannot see
/// both tables being wrong in the same way; only the locked comparison against
/// committed metadata does.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BlobRefcountDrift {
    /// `blobs` rows that no `entry_blobs` row maps to.
    pub unowned: u64,
    pub unowned_bytes: u64,
    /// Mapped blobs whose refcount is above the mapped references.
    pub too_high: u64,
    pub too_high_bytes: u64,
    /// Mapped blobs whose refcount is below the mapped references.
    pub too_low: u64,
    pub too_low_bytes: u64,
    /// Mapped hashes with no `blobs` row. Their size is not recorded.
    pub unindexed: u64,
}

impl BlobRefcountDrift {
    /// Blobs whose refcount disagrees with their mappings, in any direction.
    pub fn mismatched(&self) -> u64 {
        self.unowned
            .saturating_add(self.too_high)
            .saturating_add(self.too_low)
            .saturating_add(self.unindexed)
    }

    pub fn is_clean(&self) -> bool {
        self.mismatched() == 0
    }
}

const BLOB_SIDE: &str = "
    SELECT
        COALESCE(SUM(m.refs IS NULL), 0),
        COALESCE(SUM(CASE WHEN m.refs IS NULL THEN b.size END), 0),
        COALESCE(SUM(b.refcount > m.refs), 0),
        COALESCE(SUM(CASE WHEN b.refcount > m.refs THEN b.size END), 0),
        COALESCE(SUM(b.refcount < m.refs), 0),
        COALESCE(SUM(CASE WHEN b.refcount < m.refs THEN b.size END), 0)
    FROM blobs b
    LEFT JOIN (SELECT hash, SUM(refs) AS refs FROM entry_blobs GROUP BY hash) m
        ON m.hash = b.hash";

const MAPPING_SIDE: &str = "
    SELECT COUNT(DISTINCT hash) FROM entry_blobs
    WHERE hash NOT IN (SELECT hash FROM blobs)";

/// The probe for any connection to the index, including read-only ones.
pub fn blob_refcount_drift(db: &Connection) -> rusqlite::Result<BlobRefcountDrift> {
    let count = |row: &rusqlite::Row<'_>, column: usize| -> rusqlite::Result<u64> {
        row.get::<_, i64>(column).map(|value| value.max(0) as u64)
    };
    let mut drift = db.query_row(BLOB_SIDE, [], |row| {
        Ok(BlobRefcountDrift {
            unowned: count(row, 0)?,
            unowned_bytes: count(row, 1)?,
            too_high: count(row, 2)?,
            too_high_bytes: count(row, 3)?,
            too_low: count(row, 4)?,
            too_low_bytes: count(row, 5)?,
            unindexed: 0,
        })
    })?;
    drift.unindexed = db.query_row(MAPPING_SIDE, [], |row| count(row, 0))?;
    Ok(drift)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE blobs (hash TEXT PRIMARY KEY, size INTEGER NOT NULL, refcount INTEGER NOT NULL);
             CREATE TABLE entry_blobs (cache_key TEXT NOT NULL, hash TEXT NOT NULL, refs INTEGER NOT NULL,
                 PRIMARY KEY (cache_key, hash));
             INSERT INTO blobs VALUES ('exact', 10, 3), ('single', 20, 1);
             INSERT INTO entry_blobs VALUES ('k1', 'exact', 2), ('k2', 'exact', 1), ('k1', 'single', 1);",
        )
        .unwrap();
        db
    }

    #[test]
    fn empty_and_consistent_tables_report_no_drift() {
        let db = index();
        let drift = blob_refcount_drift(&db).unwrap();
        assert_eq!(drift, BlobRefcountDrift::default());
        assert!(drift.is_clean());
        assert_eq!(drift.mismatched(), 0);

        db.execute_batch("DELETE FROM blobs; DELETE FROM entry_blobs;")
            .unwrap();
        assert_eq!(
            blob_refcount_drift(&db).unwrap(),
            BlobRefcountDrift::default()
        );
    }

    #[test]
    fn each_drift_class_is_counted_with_its_bytes() {
        let db = index();
        db.execute_batch(
            "INSERT INTO blobs VALUES ('unowned_a', 100, 1), ('unowned_b', 1000, 0), ('unowned_c', 10000, 7);
             INSERT INTO blobs VALUES ('high_a', 3, 2), ('high_b', 30, 9);
             INSERT INTO entry_blobs VALUES ('k1', 'high_a', 1), ('k1', 'high_b', 4), ('k2', 'high_b', 4);
             INSERT INTO blobs VALUES ('low', 7, 1);
             INSERT INTO entry_blobs VALUES ('k1', 'low', 1), ('k2', 'low', 1);
             INSERT INTO entry_blobs VALUES ('k1', 'gone', 1), ('k2', 'gone', 1), ('k3', 'gone_too', 1);",
        )
        .unwrap();
        let drift = blob_refcount_drift(&db).unwrap();
        assert_eq!(
            drift,
            BlobRefcountDrift {
                unowned: 3,
                unowned_bytes: 11_100,
                too_high: 2,
                too_high_bytes: 33,
                too_low: 1,
                too_low_bytes: 7,
                unindexed: 2,
            }
        );
        assert_eq!(drift.mismatched(), 8);
        assert!(!drift.is_clean());
    }

    #[test]
    fn every_class_alone_makes_the_probe_dirty() {
        let one = |field: fn(&mut BlobRefcountDrift)| {
            let mut drift = BlobRefcountDrift::default();
            field(&mut drift);
            drift
        };
        for drift in [
            one(|d| d.unowned = 1),
            one(|d| d.too_high = 1),
            one(|d| d.too_low = 1),
            one(|d| d.unindexed = 1),
        ] {
            assert_eq!(drift.mismatched(), 1, "{drift:?}");
            assert!(!drift.is_clean(), "{drift:?}");
        }
        // Bytes alone are not a mismatch count.
        assert!(one(|d| d.unowned_bytes = 5).is_clean());
        let all = BlobRefcountDrift {
            unowned: 1,
            too_high: 2,
            too_low: 4,
            unindexed: 8,
            ..Default::default()
        };
        assert_eq!(all.mismatched(), 15);
    }

    #[test]
    fn negative_sums_clamp_to_zero() {
        let db = index();
        db.execute_batch("INSERT INTO blobs VALUES ('negative', -5, 1);")
            .unwrap();
        let drift = blob_refcount_drift(&db).unwrap();
        assert_eq!(drift.unowned, 1);
        assert_eq!(drift.unowned_bytes, 0);
    }
}
