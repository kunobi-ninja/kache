use serde::{Deserialize, Serialize};

// ── Artifact metadata sidecar ({key}.meta.json) ────────

#[derive(Debug, Serialize, Deserialize)]
pub struct ArtifactMeta {
    pub cache_key: String,
    pub crate_name: String,
    pub target_triple: String,
    pub profile: String,
    pub rustc_version: String,
    pub source_hash: String,
    /// Extern crate names (dependency edges)
    pub deps: Vec<String>,
    pub compile_time_ms: u64,
    pub artifact_size: u64,
    /// ISO 8601 timestamp
    pub timestamp: String,
}

// ── Session (PUT _kache/session body → response) ────────

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateSessionRequest {
    pub repo: String,
    pub branch: String,
    pub commit_sha: String,
    pub target_triple: String,
    pub profile: String,
    pub cargo_lock_hash: String,
    pub runner_id: String,
    pub is_ci: bool,
    pub git_diff_files: Vec<String>,
    pub base_branch: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateSessionResponse {
    pub session_id: String,
    /// Eager prefetch — server can return recommendations immediately
    pub prefetch: Vec<PrefetchRecommendation>,
}

// ── Prefetch (GET _kache/prefetch/{session_id} response) ─

#[derive(Debug, Serialize, Deserialize)]
pub struct PrefetchRecommendation {
    pub cache_key: String,
    pub crate_name: String,
    pub confidence: f64,
    pub reason: String,
    pub compile_time_ms: u64,
    pub artifact_size: u64,
    pub score: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PrefetchResponse {
    pub recommendations: Vec<PrefetchRecommendation>,
    pub total_download_size: u64,
    pub estimated_time_saved_ms: u64,
}

// ── Miss analysis (GET _kache/miss/{key} response) ──────

#[derive(Debug, Serialize, Deserialize)]
pub struct MissAnalysisResponse {
    pub reason: String,
    pub changed_fields: Vec<String>,
    pub explanation: String,
}

// ── Stats (GET _kache/stats response) ───────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct StatsResponse {
    pub total_builds: u64,
    pub hit_rate: f64,
    pub total_artifacts: u64,
    pub top_crates: Vec<CrateStats>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CrateStats {
    pub name: String,
    pub hits: u64,
    pub misses: u64,
    pub avg_compile_ms: u64,
}
