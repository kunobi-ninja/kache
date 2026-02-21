use anyhow::{Context, Result};
use rusqlite::Connection;
use std::sync::Mutex;

/// Metadata database backed by SQLite in WAL mode.
pub struct Database {
    conn: Mutex<Connection>,
}

impl Database {
    pub fn open(data_dir: &str) -> Result<Self> {
        let path = format!("{data_dir}/kache-server.db");
        let conn = Connection::open(&path).context("opening SQLite database")?;

        // WAL mode for concurrent reads + single writer
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;

        let db = Self {
            conn: Mutex::new(conn),
        };
        db.migrate()?;
        Ok(db)
    }

    fn migrate(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(SCHEMA)?;
        Ok(())
    }

    pub fn with_conn<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T>,
    {
        let conn = self.conn.lock().unwrap();
        f(&conn)
    }

    /// Record that an artifact was uploaded (PUT).
    pub fn record_put(
        &self,
        session_id: Option<&str>,
        cache_key: &str,
        crate_name: &str,
        artifact_size: i64,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();

        if let Some(sid) = session_id {
            conn.execute(
                "INSERT INTO build_events (id, session_id, cache_key, crate_name, result, artifact_size)
                 VALUES (?1, ?2, ?3, ?4, 'put', ?5)",
                rusqlite::params![uuid::Uuid::now_v7().to_string(), sid, cache_key, crate_name, artifact_size],
            )?;
        }

        Ok(())
    }

    /// Record a cache hit (GET that succeeded).
    pub fn record_hit(
        &self,
        session_id: Option<&str>,
        cache_key: &str,
        crate_name: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();

        if let Some(sid) = session_id {
            conn.execute(
                "INSERT INTO build_events (id, session_id, cache_key, crate_name, result)
                 VALUES (?1, ?2, ?3, ?4, 'hit')",
                rusqlite::params![uuid::Uuid::now_v7().to_string(), sid, cache_key, crate_name],
            )?;
        }

        Ok(())
    }

    /// Record a cache miss (GET that returned 404 / NoSuchKey).
    pub fn record_miss(
        &self,
        session_id: Option<&str>,
        cache_key: &str,
        crate_name: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();

        if let Some(sid) = session_id {
            conn.execute(
                "INSERT INTO build_events (id, session_id, cache_key, crate_name, result)
                 VALUES (?1, ?2, ?3, ?4, 'miss')",
                rusqlite::params![uuid::Uuid::now_v7().to_string(), sid, cache_key, crate_name],
            )?;
        }

        Ok(())
    }

    /// Upsert dependency edges from a crate's extern deps.
    pub fn upsert_dep_edges(
        &self,
        repo_id: &str,
        from_crate: &str,
        deps: &[String],
        target_triple: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();

        // Ensure repo exists
        conn.execute(
            "INSERT OR IGNORE INTO repos (id, name) VALUES (?1, ?1)",
            [repo_id],
        )?;

        let mut stmt = conn.prepare_cached(
            "INSERT INTO dep_edges (repo_id, from_crate, to_crate, target_triple, last_seen)
             VALUES (?1, ?2, ?3, ?4, datetime('now'))
             ON CONFLICT(repo_id, from_crate, to_crate, target_triple) DO UPDATE SET last_seen = datetime('now')",
        )?;

        for dep in deps {
            stmt.execute(rusqlite::params![repo_id, from_crate, dep, target_triple])?;
        }

        Ok(())
    }

    /// Record cache key inputs for miss analysis (used in Phase 4).
    #[allow(dead_code)]
    pub fn upsert_cache_key_inputs(
        &self,
        cache_key: &str,
        crate_name: &str,
        target_triple: &str,
        rustc_version: &str,
        source_hash: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();

        conn.execute(
            "INSERT INTO cache_key_inputs (cache_key, crate_name, target_triple, rustc_version, source_hash)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(cache_key) DO UPDATE SET
                crate_name = excluded.crate_name,
                target_triple = excluded.target_triple,
                rustc_version = excluded.rustc_version,
                source_hash = excluded.source_hash",
            rusqlite::params![cache_key, crate_name, target_triple, rustc_version, source_hash],
        )?;

        Ok(())
    }

    /// Create a new build session, returns session_id.
    pub fn create_session(&self, req: &kache_protocol::CreateSessionRequest) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let session_id = uuid::Uuid::now_v7().to_string();

        // Ensure repo exists
        conn.execute(
            "INSERT OR IGNORE INTO repos (id, name) VALUES (?1, ?1)",
            [&req.repo],
        )?;

        let diff_json = serde_json::to_string(&req.git_diff_files)?;

        conn.execute(
            "INSERT INTO build_sessions (id, repo_id, branch, commit_sha, target_triple, profile,
             cargo_lock_hash, runner_id, is_ci, git_diff_files, base_branch)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            rusqlite::params![
                session_id,
                req.repo,
                req.branch,
                req.commit_sha,
                req.target_triple,
                req.profile,
                req.cargo_lock_hash,
                req.runner_id,
                req.is_ci as i32,
                diff_json,
                req.base_branch,
            ],
        )?;

        Ok(session_id)
    }
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS repos (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS build_sessions (
    id               TEXT PRIMARY KEY,
    repo_id          TEXT REFERENCES repos(id),
    branch           TEXT NOT NULL DEFAULT '',
    commit_sha       TEXT NOT NULL DEFAULT '',
    target_triple    TEXT NOT NULL,
    profile          TEXT NOT NULL DEFAULT '',
    cargo_lock_hash  TEXT NOT NULL DEFAULT '',
    runner_id        TEXT NOT NULL DEFAULT '',
    is_ci            INTEGER NOT NULL DEFAULT 0,
    git_diff_files   TEXT NOT NULL DEFAULT '[]',
    base_branch      TEXT NOT NULL DEFAULT '',
    started_at       TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at      TEXT,
    total_compile_ms INTEGER NOT NULL DEFAULT 0,
    total_cache_ms   INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_sessions_repo_branch
    ON build_sessions(repo_id, branch, started_at DESC);

CREATE TABLE IF NOT EXISTS build_events (
    id            TEXT PRIMARY KEY,
    session_id    TEXT NOT NULL REFERENCES build_sessions(id),
    cache_key     TEXT NOT NULL,
    crate_name    TEXT NOT NULL,
    result        TEXT NOT NULL,
    elapsed_ms    INTEGER NOT NULL DEFAULT 0,
    artifact_size INTEGER NOT NULL DEFAULT 0,
    miss_reason   TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_events_session ON build_events(session_id);
CREATE INDEX IF NOT EXISTS idx_events_crate   ON build_events(crate_name);

CREATE TABLE IF NOT EXISTS cache_key_inputs (
    cache_key     TEXT PRIMARY KEY,
    crate_name    TEXT NOT NULL,
    target_triple TEXT NOT NULL,
    rustc_version TEXT NOT NULL,
    source_hash   TEXT NOT NULL DEFAULT '',
    extern_hashes TEXT NOT NULL DEFAULT '{}',
    codegen_opts  TEXT NOT NULL DEFAULT '{}',
    features      TEXT NOT NULL DEFAULT '[]',
    rustflags     TEXT NOT NULL DEFAULT '',
    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_key_inputs_crate
    ON cache_key_inputs(crate_name, target_triple);

CREATE TABLE IF NOT EXISTS dep_edges (
    repo_id       TEXT NOT NULL REFERENCES repos(id),
    from_crate    TEXT NOT NULL,
    to_crate      TEXT NOT NULL,
    target_triple TEXT NOT NULL,
    last_seen     TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY(repo_id, from_crate, to_crate, target_triple)
);

CREATE INDEX IF NOT EXISTS idx_dep_edges_to
    ON dep_edges(repo_id, to_crate);
"#;

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db() -> Database {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        let db = Database {
            conn: Mutex::new(conn),
        };
        db.migrate().unwrap();
        db
    }

    #[test]
    fn test_migrate_idempotent() {
        let db = test_db();
        db.migrate().unwrap(); // second migration should succeed
    }

    #[test]
    fn test_create_session() {
        let db = test_db();
        let req = kache_protocol::CreateSessionRequest {
            repo: "test-repo".into(),
            branch: "main".into(),
            commit_sha: "abc123".into(),
            target_triple: "x86_64-unknown-linux-gnu".into(),
            profile: "release".into(),
            cargo_lock_hash: "lockhash".into(),
            runner_id: "runner-1".into(),
            is_ci: true,
            git_diff_files: vec!["src/main.rs".into()],
            base_branch: "main".into(),
        };
        let id = db.create_session(&req).unwrap();
        assert!(!id.is_empty());
    }

    #[test]
    fn test_record_events() {
        let db = test_db();
        let req = kache_protocol::CreateSessionRequest {
            repo: "test-repo".into(),
            branch: "main".into(),
            commit_sha: "abc123".into(),
            target_triple: "x86_64-unknown-linux-gnu".into(),
            profile: "release".into(),
            cargo_lock_hash: "".into(),
            runner_id: "".into(),
            is_ci: false,
            git_diff_files: vec![],
            base_branch: "".into(),
        };
        let sid = db.create_session(&req).unwrap();

        db.record_put(Some(&sid), "key1", "serde", 1024).unwrap();
        db.record_hit(Some(&sid), "key1", "serde").unwrap();
        db.record_miss(Some(&sid), "key2", "tokio").unwrap();

        let count: i64 = db
            .with_conn(|c| Ok(c.query_row("SELECT COUNT(*) FROM build_events", [], |r| r.get(0))?))
            .unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn test_upsert_dep_edges() {
        let db = test_db();
        let deps = vec!["serde".into(), "tokio".into()];
        db.upsert_dep_edges("repo1", "my_crate", &deps, "x86_64-unknown-linux-gnu")
            .unwrap();
        // Upsert again — should not fail
        db.upsert_dep_edges("repo1", "my_crate", &deps, "x86_64-unknown-linux-gnu")
            .unwrap();

        let count: i64 = db
            .with_conn(|c| Ok(c.query_row("SELECT COUNT(*) FROM dep_edges", [], |r| r.get(0))?))
            .unwrap();
        assert_eq!(count, 2);
    }
}
