//! Explicit removal of one Rust compiler's cached results.

use super::*;

pub(super) fn ensure_schema(db: &Connection) -> rusqlite::Result<()> {
    let _ = db.execute_batch("ALTER TABLE entries ADD COLUMN rustc_version TEXT");
    db.execute_batch("CREATE INDEX IF NOT EXISTS idx_entries_rustc ON entries(rustc_version)")
}

/// A compiler selection's logical size. Shared output bytes are counted per
/// entry here; the removal report measures what the store actually released.
#[derive(Debug, Default, serde::Serialize)]
pub struct CompilerEntries {
    pub entries: usize,
    pub bytes: u64,
    pub versions: Vec<String>,
    pub unknown_entries: usize,
}

impl<P: crate::ArtifactPolicy> ArtifactStore<P> {
    fn rustc_entries(&self, version: &str) -> Result<Vec<(String, u64, String)>> {
        // A release selects all its builds; a complete identity selects only
        // that build. Use a word boundary, so 1.9.0 cannot select 1.99.0.
        let mut stmt = self.db.prepare(
            "SELECT cache_key, size, rustc_version FROM entries
             WHERE committed = 1 AND (rustc_version = ?1 OR
                substr(rustc_version, 1, instr(rustc_version || ' ', ' ') - 1) = ?1)
             ORDER BY cache_key",
        )?;
        Ok(stmt
            .query_map(params![version], |row| {
                Ok((row.get(0)?, row.get::<_, i64>(1)? as u64, row.get(2)?))
            })?
            .collect::<rusqlite::Result<_>>()?)
    }

    /// Preview one release or exact first-line compiler identity. Entries
    /// without a recorded compiler remain outside every version selection.
    pub fn preview_rustc_version(&self, version: &str) -> Result<CompilerEntries> {
        let selected = self.rustc_entries(version)?;
        let mut versions = std::collections::BTreeSet::new();
        let mut bytes = 0_u64;
        for (_, size, identity) in &selected {
            bytes = bytes.saturating_add(*size);
            versions.insert(identity.clone());
        }
        Ok(CompilerEntries {
            entries: selected.len(),
            bytes,
            versions: versions.into_iter().collect(),
            unknown_entries: self.db.query_row(
                "SELECT COUNT(*) FROM entries WHERE committed = 1 AND rustc_version IS NULL",
                [],
                |row| row.get::<_, i64>(0),
            )? as usize,
        })
    }

    /// Remove matching entries with ordinary eviction's active-use and upload
    /// protections. Refcounts retain blobs still used by another compiler.
    pub fn evict_rustc_version(&self, version: &str) -> Result<GcStats> {
        self.refuse_write("evict a compiler version")?;
        let keys: Vec<_> = self
            .rustc_entries(version)?
            .into_iter()
            .map(|e| e.0)
            .collect();
        let candidates = self.eviction_candidates()?;
        let by_key = candidates
            .iter()
            .map(|entry| (entry.key.as_str(), entry))
            .collect();
        let durable_upload_keys = self.durable_upload_keys()?;
        Ok(self.apply_eviction(
            &keys,
            &by_key,
            "rustc_version",
            None,
            None,
            &durable_upload_keys,
            &std::collections::HashMap::new(),
            &mut || false,
        ))
    }
}
