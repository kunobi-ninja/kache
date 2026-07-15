use crate::transport::prelude::*;
use crate::transport::{ListenerOptions, TokioListener, TokioStream, socket_name};
use anyhow::{Context, Result};
use kache_core::{PrefetchDisposition, PrefetchPlan};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{Notify, RwLock};

use crate::config::Config;
use crate::events;
use crate::store::Store;

const KEY_CACHE_REFRESH_SECS: u64 = 60;
const KEY_CACHE_AUTHORITATIVE_FOR: Duration = Duration::from_secs(KEY_CACHE_REFRESH_SECS * 5);
const REMOTE_CHECK_WARMING_GRACE: Duration = Duration::from_millis(750);
const REMOTE_HEAD_FAILURE_THRESHOLD: u32 = 3;
const REMOTE_HEAD_DEGRADED_FOR: Duration = Duration::from_secs(45);
const DAEMON_START_TIMEOUT: Duration = Duration::from_secs(8);
const DAEMON_START_POLL_INTERVAL: Duration = Duration::from_millis(100);
const DAEMON_COORD_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);

/// How often the daemon re-checks its config file for changes. On a change it
/// schedules a graceful restart so the new config (e.g. `local_max_size`) takes
/// effect — no manual `kache daemon stop`. Cheap (one small-file read); rare to
/// fire, so a coarse interval is fine.
const DAEMON_CONFIG_WATCH_INTERVAL: Duration = Duration::from_secs(15);
const DAEMON_COORD_STALE_AFTER: Duration = Duration::from_secs(15);
const VERSION: &str = crate::VERSION;
const FILE_HASH_MEMORY_CACHE_CAP: usize = 4096;

/// Compute a "build epoch" from the executable's mtime.
/// This changes every time `cargo build` produces a new binary,
/// giving us a cheap way to detect when the daemon is running stale code.
pub fn build_epoch() -> u64 {
    static BUILD_EPOCH: OnceLock<u64> = OnceLock::new();

    *BUILD_EPOCH.get_or_init(|| {
        std::env::current_exe()
            .and_then(std::fs::metadata)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(0)
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum DaemonPhase {
    Starting,
    Ready,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct DaemonCoordState {
    pid: u32,
    build_epoch: u64,
    phase: DaemonPhase,
    updated_at_ms: u64,
}

#[derive(Debug, Clone)]
struct DaemonCoordFile {
    path: PathBuf,
    pid: u32,
    build_epoch: u64,
}

impl DaemonCoordFile {
    fn for_socket(socket_path: &Path) -> Self {
        Self {
            path: daemon_state_path(socket_path),
            pid: std::process::id(),
            build_epoch: build_epoch(),
        }
    }

    fn write_phase(&self, phase: DaemonPhase) -> Result<()> {
        let state = DaemonCoordState {
            pid: self.pid,
            build_epoch: self.build_epoch,
            phase,
            updated_at_ms: now_millis(),
        };
        write_json_atomically(&self.path, &state)
    }
}

struct DaemonCoordGuard {
    path: PathBuf,
}

/// RAII guard that removes the Unix socket file on drop.
/// Ensures the socket is cleaned up even if `server_main` exits early
/// (panic, `?` bail, etc.), preventing a stale socket from blocking
/// future daemon starts while the run lock is already released.
struct SocketCleanupGuard {
    path: PathBuf,
}

struct RemoteHealth {
    head_probe_failures: AtomicU32,
    head_probe_degraded_until_ms: AtomicU64,
    suppressed_head_probes: AtomicU32,
}

impl RemoteHealth {
    fn new() -> Self {
        Self {
            head_probe_failures: AtomicU32::new(0),
            head_probe_degraded_until_ms: AtomicU64::new(0),
            suppressed_head_probes: AtomicU32::new(0),
        }
    }

    fn head_probe_is_degraded(&self) -> bool {
        now_millis() < self.head_probe_degraded_until_ms.load(Ordering::Acquire)
    }

    fn note_head_probe_failure(&self, error: &str) {
        let failures = self.head_probe_failures.fetch_add(1, Ordering::AcqRel) + 1;
        if failures < REMOTE_HEAD_FAILURE_THRESHOLD {
            if failures == 1 {
                tracing::warn!(
                    "remote HEAD probe failed ({failures}/{REMOTE_HEAD_FAILURE_THRESHOLD} before degradation): {error}"
                );
            } else {
                tracing::debug!(
                    "remote HEAD probe failed ({failures}/{REMOTE_HEAD_FAILURE_THRESHOLD} before degradation): {error}"
                );
            }
            return;
        }

        let was_degraded = self.head_probe_is_degraded();
        let degrade_until = now_millis() + REMOTE_HEAD_DEGRADED_FOR.as_millis() as u64;
        self.head_probe_degraded_until_ms
            .store(degrade_until, Ordering::Release);
        self.suppressed_head_probes.store(0, Ordering::Release);

        if !was_degraded {
            tracing::warn!(
                "remote HEAD probes degraded for {}s after {failures} consecutive failure(s); last error: {error}",
                REMOTE_HEAD_DEGRADED_FOR.as_secs()
            );
        } else {
            tracing::debug!("remote HEAD probe failed while degraded: {error}");
        }
    }

    fn note_head_probe_success(&self) {
        let failures = self.head_probe_failures.swap(0, Ordering::AcqRel);
        let degraded_until = self.head_probe_degraded_until_ms.swap(0, Ordering::AcqRel);
        let suppressed = self.suppressed_head_probes.swap(0, Ordering::AcqRel);
        let now = now_millis();

        if failures >= REMOTE_HEAD_FAILURE_THRESHOLD || degraded_until > now || suppressed > 0 {
            tracing::info!(
                "remote HEAD probes recovered after {failures} consecutive failure(s); suppressed {suppressed} probe(s) while degraded"
            );
        }
    }

    fn note_head_probe_suppressed(&self) {
        self.suppressed_head_probes.fetch_add(1, Ordering::Relaxed);
    }
}

impl DaemonCoordGuard {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for DaemonCoordGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

impl Drop for SocketCleanupGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn daemon_state_path(socket_path: &Path) -> PathBuf {
    socket_path.with_extension("state.json")
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn write_json_atomically<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("state file has no parent directory"))?;
    std::fs::create_dir_all(parent)?;

    let file_name = path
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("state file has no file name"))?
        .to_string_lossy();
    let tmp_path = parent.join(format!("{file_name}.{}.tmp", std::process::id()));
    let json = serde_json::to_vec(value)?;
    std::fs::write(&tmp_path, json)?;
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

fn read_daemon_state(socket_path: &Path) -> Option<DaemonCoordState> {
    let path = daemon_state_path(socket_path);
    let bytes = std::fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn daemon_state_is_recent(state: &DaemonCoordState) -> bool {
    let age_ms = now_millis().saturating_sub(state.updated_at_ms);
    age_ms <= DAEMON_COORD_STALE_AFTER.as_millis() as u64
}

fn client_epoch_is_newer(client_epoch: u64, daemon_epoch: u64) -> bool {
    client_epoch > 0 && daemon_epoch > 0 && client_epoch > daemon_epoch
}

use crate::platform::is_process_alive as process_is_alive;

fn wait_for_run_lock_release(socket_path: &Path, timeout: Duration) -> Result<bool> {
    let deadline = Instant::now() + timeout;
    loop {
        if !daemon_run_lock_is_held(socket_path)? {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        std::thread::sleep(DAEMON_START_POLL_INTERVAL);
    }
}

fn terminate_daemon_pid(pid: u32, socket_path: &Path) -> Result<bool> {
    crate::platform::terminate_process(pid);

    if wait_for_run_lock_release(socket_path, Duration::from_secs(1))? {
        return Ok(true);
    }

    crate::platform::kill_process(pid);

    wait_for_run_lock_release(socket_path, Duration::from_secs(1))
}

fn recover_unhealthy_daemon(socket_path: &Path, reason: &str) -> Result<bool> {
    let run_lock_held = daemon_run_lock_is_held(socket_path)?;
    if let Some(state) = read_daemon_state(socket_path) {
        let state_recent = daemon_state_is_recent(&state);
        if run_lock_held && process_is_alive(state.pid) {
            tracing::info!(
                socket = %socket_path.display(),
                pid = state.pid,
                ?state.phase,
                heartbeat_fresh = state_recent,
                reason,
                "terminating unhealthy daemon coordinator"
            );
            if !terminate_daemon_pid(state.pid, socket_path)? {
                tracing::warn!(
                    socket = %socket_path.display(),
                    pid = state.pid,
                    heartbeat_fresh = state_recent,
                    reason,
                    "daemon process did not release run lock during recovery"
                );
                return Ok(false);
            }
        }
    }

    if daemon_run_lock_is_held(socket_path)? {
        tracing::warn!(
            socket = %socket_path.display(),
            reason,
            "daemon run lock still held and no recoverable coordinator state was found"
        );
        return Ok(false);
    }

    let _ = std::fs::remove_file(socket_path);
    let _ = std::fs::remove_file(daemon_state_path(socket_path));
    Ok(true)
}

// ── Protocol types ───────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Request {
    Upload(UploadJob),
    Gc(GcRequest),
    RemoteCheck(RemoteCheckRequest),
    Stats(StatsRequest),
    BatchRemoteCheck(BatchRemoteCheckRequest),
    HashFiles(HashFilesRequest),
    Prefetch(PrefetchRequest),
    BuildStarted(BuildStartedRequest),
    Shutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UploadJob {
    pub key: String,
    pub entry_dir: String,
    #[serde(default)]
    pub crate_name: String,
    /// Client binary mtime — lets the daemon detect when it's running stale code.
    #[serde(default)]
    pub client_epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GcRequest {
    pub max_age_hours: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RemoteCheckRequest {
    pub key: String,
    pub entry_dir: String,
    #[serde(default)]
    pub crate_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StatsRequest {
    pub include_entries: bool,
    pub sort_by: Option<String>,
    pub event_hours: Option<u64>,
    /// Client binary mtime — lets the daemon detect when it's running stale code.
    #[serde(default)]
    pub client_epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BatchRemoteCheckRequest {
    pub checks: Vec<RemoteCheckRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HashFilesRequest {
    pub files: Vec<HashFileRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HashFileRequest {
    pub path: String,
    pub size: i64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    #[serde(default)]
    pub inode: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HashFileResult {
    pub path: String,
    pub size: i64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    #[serde(default)]
    pub inode: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    #[serde(default)]
    pub cache_hit: bool,
    #[serde(default)]
    pub bytes_hashed: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PrefetchRequest {
    /// (cache_key, crate_name) pairs
    pub keys: Vec<(String, String)>,
}

impl PrefetchRequest {
    pub fn from_plan(plan: PrefetchPlan) -> Self {
        Self {
            keys: plan
                .candidates
                .into_iter()
                // The planner is an untrusted boundary (a distinct endpoint
                // from S3). cache_key/crate_name flow into local path joins and
                // S3 object keys, so drop any candidate that isn't a well-formed
                // key + safe crate name before it can become a traversal /
                // prefix-escape primitive. Reject, don't sanitize.
                .filter(|c| {
                    let ok = crate::cache_key::is_valid_cache_key(&c.cache_key)
                        && crate::cache_key::is_valid_crate_name(&c.crate_name);
                    if !ok {
                        tracing::warn!(
                            cache_key = key_prefix(&c.cache_key),
                            cache_key_len = c.cache_key.len(),
                            "prefetch: dropping planner candidate with invalid cache_key/crate_name"
                        );
                    }
                    ok
                })
                .map(|candidate| (candidate.cache_key, candidate.crate_name))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BuildStartedRequest {
    #[serde(default)]
    pub intent: kache_core::BuildIntent,
    /// Client binary mtime — lets the daemon detect when it's running stale code.
    #[serde(default)]
    pub client_epoch: u64,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BatchResponse {
    pub ok: bool,
    pub results: Vec<Response>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StatsResponse {
    pub total_size: u64,
    pub max_size: u64,
    pub entry_count: usize,
    pub entries: Option<Vec<StatsEntry>>,
    pub events: EventStatsResponse,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub build_epoch: u64,
    /// Number of keys queued or in-flight for upload.
    #[serde(default)]
    pub pending_uploads: usize,
    /// Number of keys currently being downloaded from S3.
    #[serde(default)]
    pub active_downloads: usize,
    #[serde(default)]
    pub s3_concurrency_total: usize,
    #[serde(default)]
    pub s3_concurrency_used: usize,
    #[serde(default)]
    pub upload_queue_capacity: usize,
    #[serde(default)]
    pub uploads_completed: u64,
    #[serde(default)]
    pub uploads_failed: u64,
    #[serde(default)]
    pub uploads_skipped: u64,
    #[serde(default)]
    pub downloads_completed: u64,
    #[serde(default)]
    pub downloads_failed: u64,
    #[serde(default)]
    pub bytes_uploaded: u64,
    #[serde(default)]
    pub bytes_downloaded: u64,
    #[serde(default)]
    pub recent_transfers: Vec<TransferEvent>,
    /// Phase-0 prefetch/planning observability (#485). Defaulted so old
    /// clients reading a new daemon (and vice versa) keep working.
    #[serde(default)]
    pub prefetch: PrefetchStatsSnapshot,
}

/// Point-in-time view of [`PrefetchStats`] (+ the cancel latch) carried in
/// [`StatsResponse`]. See the field docs on `PrefetchStats` for semantics.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct PrefetchStatsSnapshot {
    #[serde(default)]
    pub downloads_completed: u64,
    #[serde(default)]
    pub bytes_downloaded: u64,
    #[serde(default)]
    pub keys_used: u64,
    #[serde(default)]
    pub keys_cancelled: u64,
    /// Whether the daemon-lifetime adaptive cancel latch has fired.
    #[serde(default)]
    pub cancelled: bool,
    #[serde(default)]
    pub plans_advisory: u64,
    #[serde(default)]
    pub plans_fallback: u64,
    #[serde(default)]
    pub last_plan_candidates: u64,
    #[serde(default)]
    pub dedup_join_waits: u64,
    #[serde(default)]
    pub dedup_join_wait_ms: u64,
    #[serde(default)]
    pub last_list_duration_ms: u64,
    #[serde(default)]
    pub last_list_key_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StatsEntry {
    pub cache_key: String,
    pub crate_name: String,
    pub crate_type: String,
    pub profile: String,
    pub size: u64,
    pub hit_count: u64,
    pub created_at: String,
    pub last_accessed: String,
    pub content_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventStatsResponse {
    pub local_hits: usize,
    #[serde(default)]
    pub prefetch_hits: usize,
    pub remote_hits: usize,
    #[serde(default)]
    pub dups: usize,
    pub misses: usize,
    pub errors: usize,
    pub total_elapsed_ms: u64,
    #[serde(default)]
    pub hit_elapsed_ms: u64,
    #[serde(default)]
    pub miss_elapsed_ms: u64,
    #[serde(default)]
    pub hit_compile_time_ms: u64,
    #[serde(default)]
    pub miss_compile_time_ms: u64,
    #[serde(default)]
    pub store_output_blobs: u32,
    #[serde(default)]
    pub store_duplicate_blobs: u32,
    #[serde(default)]
    pub store_new_blobs: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct Response {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evicted: Option<usize>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub skipped: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub found: Option<bool>,
    /// True when the artifact was downloaded during manifest/shard prefetch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefetched: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<StatsResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_results: Option<Vec<Response>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash_results: Option<Vec<HashFileResult>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl Response {
    fn ok() -> Self {
        Self {
            ok: true,
            evicted: None,
            skipped: false,
            found: None,
            prefetched: None,
            stats: None,
            batch_results: None,
            hash_results: None,
            error: None,
        }
    }

    fn ok_evicted(n: usize) -> Self {
        Self {
            ok: true,
            evicted: Some(n),
            skipped: false,
            found: None,
            prefetched: None,
            stats: None,
            batch_results: None,
            hash_results: None,
            error: None,
        }
    }

    fn ok_gc_skipped() -> Self {
        Self {
            ok: true,
            evicted: Some(0),
            skipped: true,
            found: None,
            prefetched: None,
            stats: None,
            batch_results: None,
            hash_results: None,
            error: None,
        }
    }

    fn ok_stats(stats: StatsResponse) -> Self {
        Self {
            ok: true,
            evicted: None,
            skipped: false,
            found: None,
            prefetched: None,
            stats: Some(stats),
            batch_results: None,
            hash_results: None,
            error: None,
        }
    }

    fn ok_batch(results: Vec<Response>) -> Self {
        Self {
            ok: true,
            evicted: None,
            skipped: false,
            found: None,
            prefetched: None,
            stats: None,
            batch_results: Some(results),
            hash_results: None,
            error: None,
        }
    }

    fn ok_hash_results(results: Vec<HashFileResult>) -> Self {
        Self {
            ok: true,
            evicted: None,
            skipped: false,
            found: None,
            prefetched: None,
            stats: None,
            batch_results: None,
            hash_results: Some(results),
            error: None,
        }
    }

    fn found(val: bool) -> Self {
        Self {
            ok: true,
            evicted: None,
            skipped: false,
            found: Some(val),
            prefetched: None,
            stats: None,
            batch_results: None,
            hash_results: None,
            error: None,
        }
    }

    fn found_prefetched(val: bool, prefetched: bool) -> Self {
        Self {
            ok: true,
            evicted: None,
            skipped: false,
            found: Some(val),
            prefetched: Some(prefetched),
            stats: None,
            batch_results: None,
            hash_results: None,
            error: None,
        }
    }

    fn err(msg: impl Into<String>) -> Self {
        Self {
            ok: false,
            evicted: None,
            skipped: false,
            found: None,
            prefetched: None,
            stats: None,
            batch_results: None,
            hash_results: None,
            error: Some(msg.into()),
        }
    }
}

// ── Transfer tracking ────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TransferDirection {
    Upload,
    Download,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TransferEvent {
    #[serde(default = "default_transfer_schema")]
    pub schema: u32,
    pub crate_name: String,
    pub direction: TransferDirection,
    #[serde(default)]
    pub format: String,
    #[serde(default)]
    pub cache_key: String,
    #[serde(default)]
    pub object_key: String,
    pub compressed_bytes: u64,
    pub elapsed_ms: u64,
    /// Time spent on S3 GET + body collection only (excludes decompression/disk I/O).
    #[serde(default)]
    pub network_ms: u64,
    /// Time spent waiting for an S3 concurrency permit.
    #[serde(default)]
    pub semaphore_wait_ms: u64,
    /// Time spent on HEAD/existence checks before the transfer.
    #[serde(default)]
    pub head_ms: u64,
    /// Time spent waiting for response headers across all GET requests (ms).
    #[serde(default)]
    pub request_ms: u64,
    /// Time spent reading response bodies across all GET requests (ms).
    #[serde(default)]
    pub body_ms: u64,
    /// Number of GET requests issued for this transfer.
    #[serde(default)]
    pub request_count: u32,
    /// Uncompressed size in bytes (0 for older log entries or failed transfers).
    #[serde(default)]
    pub original_bytes: u64,
    /// Time spent in zstd decompression (ms). 0 for uploads or older entries.
    #[serde(default)]
    pub decompress_ms: u64,
    /// Time spent extracting the downloaded archive to the local store.
    #[serde(default)]
    pub extract_ms: u64,
    /// Time spent on disk I/O (fs::write + permissions + atomic rename), ms.
    #[serde(default)]
    pub disk_io_ms: u64,
    /// Time spent importing downloaded metadata into SQLite.
    #[serde(default)]
    pub import_ms: u64,
    /// Time spent in zstd compression for uploads (ms).
    #[serde(default)]
    pub compression_ms: u64,
    /// Total time for HEAD requests (existence checks) during uploads (ms).
    #[serde(default)]
    pub head_checks_ms: u64,
    /// Number of v2 blobs that were already local and skipped download.
    #[serde(default)]
    pub blobs_skipped: u32,
    /// Total number of v2 blobs for this entry.
    #[serde(default)]
    pub blobs_total: u32,
    pub ok: bool,
    pub timestamp: u64,
}

const fn default_transfer_schema() -> u32 {
    2
}

pub(crate) struct TransferCounters {
    pub uploads_completed: std::sync::atomic::AtomicU64,
    pub uploads_failed: std::sync::atomic::AtomicU64,
    pub uploads_skipped: std::sync::atomic::AtomicU64,
    pub downloads_completed: std::sync::atomic::AtomicU64,
    pub downloads_failed: std::sync::atomic::AtomicU64,
    pub bytes_uploaded: std::sync::atomic::AtomicU64,
    pub bytes_downloaded: std::sync::atomic::AtomicU64,
}

impl TransferCounters {
    fn new() -> Self {
        Self {
            uploads_completed: 0.into(),
            uploads_failed: 0.into(),
            uploads_skipped: 0.into(),
            downloads_completed: 0.into(),
            downloads_failed: 0.into(),
            bytes_uploaded: 0.into(),
            bytes_downloaded: 0.into(),
        }
    }
}

/// Max concurrent speculative prefetch downloads for an S3 permit pool of
/// `s3_concurrency` (#485 Phase 0): total minus a reserve of 1/4 of the pool
/// (at least 1, at most 4), never below 1. Because every prefetch task holds
/// at most one permit and at most this many run at once, at least `reserve`
/// permits stay available to interactive RemoteCheck and uploads — prefetch
/// can slow them but never starve them. A 1-permit pool degrades to no
/// reservation rather than disabling prefetch.
fn prefetch_concurrency_cap(s3_concurrency: u32) -> usize {
    let total = s3_concurrency.max(1) as usize;
    let reserve = (total / 4).clamp(1, 4).min(total.saturating_sub(1));
    (total - reserve).max(1)
}

/// Daemon-lifetime prefetch/planning observability counters (#485 Phase 0).
///
/// Telemetry only — nothing here feeds a decision. The adaptive cancellation
/// keeps its own `prefetch_checks`/`prefetch_hits` pair untouched so behavior
/// is unchanged while the baseline is being measured; these counters exist so
/// `kache stats` can show planner source, plan size, downloaded-vs-used
/// prefetch volume, cancellation, dedup join-waits, and LIST cost — the
/// numbers the prefetch-coordination work is judged against.
pub(crate) struct PrefetchStats {
    /// Downloads completed by the speculative prefetch pipeline (a subset of
    /// `TransferCounters::downloads_completed`, which also counts on-demand).
    pub downloads_completed: std::sync::atomic::AtomicU64,
    /// Compressed bytes downloaded by prefetch (subset of `bytes_downloaded`).
    pub bytes_downloaded: std::sync::atomic::AtomicU64,
    /// Distinct prefetched keys later requested by a wrapper THROUGH the
    /// daemon (RemoteCheck). A LOWER BOUND on real usage: a completed
    /// prefetch is normally consumed via the wrapper's local store path,
    /// which never reaches the daemon (cross-family review, #485). Full
    /// per-build attribution lives in the events log (`kache report`,
    /// PrefetchHit); this counter mainly captures joins on in-flight
    /// prefetch downloads.
    pub keys_used: std::sync::atomic::AtomicU64,
    /// Keys dropped un-downloaded by an adaptive cancellation.
    pub keys_cancelled: std::sync::atomic::AtomicU64,
    /// BuildStarted sessions planned by the advisory service vs locally.
    pub plans_advisory: std::sync::atomic::AtomicU64,
    pub plans_fallback: std::sync::atomic::AtomicU64,
    /// Candidate count of the most recent plan (either source).
    pub last_plan_candidates: std::sync::atomic::AtomicU64,
    /// RemoteCheck handlers that waited on another task's in-flight download
    /// of the same key (the dedup join-wait), and their cumulative wait.
    pub dedup_join_waits: std::sync::atomic::AtomicU64,
    pub dedup_join_wait_ms: std::sync::atomic::AtomicU64,
    /// Most recent key-cache LIST refresh: wall time and key count.
    pub last_list_duration_ms: std::sync::atomic::AtomicU64,
    pub last_list_key_count: std::sync::atomic::AtomicU64,
}

impl PrefetchStats {
    fn new() -> Self {
        Self {
            downloads_completed: 0.into(),
            bytes_downloaded: 0.into(),
            keys_used: 0.into(),
            keys_cancelled: 0.into(),
            plans_advisory: 0.into(),
            plans_fallback: 0.into(),
            last_plan_candidates: 0.into(),
            dedup_join_waits: 0.into(),
            dedup_join_wait_ms: 0.into(),
            last_list_duration_ms: 0.into(),
            last_list_key_count: 0.into(),
        }
    }
}

const RECENT_TRANSFERS_CAP: usize = 50;

// ── S3 Key Cache ─────────────────────────────────────────────────

/// The forward key set and its reverse crate→keys index, held together so they
/// are always swapped/mutated as one unit (kunobi-ninja/kache#213).
#[derive(Default)]
struct S3Index {
    /// Every cache key present in the S3 listing.
    keys: HashSet<String>,
    /// Reverse index: crate_name → [cache_key, ...].
    /// Built from the S3 listing so the daemon can resolve crate names to cache
    /// keys without needing the local SQLite store (critical for cold CI runners).
    by_crate: HashMap<String, Vec<String>>,
}

pub(crate) struct S3KeyCache {
    /// Forward set + reverse index under ONE lock. They were previously two
    /// independent `RwLock`s that `populate` swapped in two steps, so a
    /// concurrent `insert` landing between the swaps could be lost or leave the
    /// two views inconsistent. A single-lock swap of both maps closes that
    /// window (kunobi-ninja/kache#213). `None` until the first populate.
    index: RwLock<Option<S3Index>>,
    populated: AtomicBool,
    last_populated: RwLock<Option<Instant>>,
}

impl S3KeyCache {
    fn new() -> Self {
        Self {
            index: RwLock::new(None),
            populated: AtomicBool::new(false),
            last_populated: RwLock::new(None),
        }
    }

    /// How long since the cache was last populated. Returns `None` if never populated.
    pub async fn age(&self) -> Option<Duration> {
        let guard = self.last_populated.read().await;
        guard.map(|t| t.elapsed())
    }

    /// Check if a key exists. Returns `None` if cache is not yet populated.
    pub async fn check(&self, key: &str) -> Option<bool> {
        if !self.populated.load(Ordering::Acquire) {
            return None;
        }
        let guard = self.index.read().await;
        guard.as_ref().map(|i| i.keys.contains(key))
    }

    /// Look up cache keys for a crate name from the S3 listing.
    /// Returns empty vec if the cache is not yet populated.
    pub async fn keys_for_crate(&self, crate_name: &str) -> Vec<String> {
        if !self.populated.load(Ordering::Acquire) {
            return vec![];
        }
        let guard = self.index.read().await;
        guard
            .as_ref()
            .and_then(|i| i.by_crate.get(crate_name))
            .cloned()
            .unwrap_or_default()
    }

    /// Replace the entire key set (called after list_keys).
    /// Accepts the full cache_key → crate_name mapping from S3 and builds
    /// both a forward set (for `check`) and a reverse index (for `keys_for_crate`).
    ///
    /// The forward set and reverse index are swapped together under a single
    /// write lock, so a concurrent [`insert`](Self::insert) is ordered strictly
    /// before or after this refresh — never interleaved between two separate
    /// swaps (kunobi-ninja/kache#213).
    pub async fn populate(&self, keys: HashMap<String, String>) {
        let mut by_crate: HashMap<String, Vec<String>> = HashMap::new();
        for (cache_key, crate_name) in &keys {
            by_crate
                .entry(crate_name.clone())
                .or_default()
                .push(cache_key.clone());
        }
        let new_index = S3Index {
            keys: keys.into_keys().collect(),
            by_crate,
        };

        let mut guard = self.index.write().await;
        *guard = Some(new_index);
        drop(guard);

        self.populated.store(true, Ordering::Release);
        let mut ts = self.last_populated.write().await;
        *ts = Some(Instant::now());
    }

    /// Insert a single key (called after successful upload).
    ///
    /// Updates the forward set and reverse index under one lock so the two views
    /// stay consistent with each other (kunobi-ninja/kache#213).
    pub async fn insert(&self, key: String, crate_name: Option<&str>) {
        let mut guard = self.index.write().await;
        if let Some(index) = guard.as_mut() {
            index.keys.insert(key.clone());
            if let Some(name) = crate_name {
                index
                    .by_crate
                    .entry(name.to_string())
                    .or_default()
                    .push(key);
            }
        }
    }

    /// Remove a key whose positive turned out stale (a GET returned 404, so
    /// the object is gone from the remote). Forward set and reverse index are
    /// updated under one lock, mirroring [`Self::insert`] (#485 Phase 0).
    pub async fn remove(&self, key: &str) {
        let mut guard = self.index.write().await;
        if let Some(index) = guard.as_mut() {
            index.keys.remove(key);
            for keys in index.by_crate.values_mut() {
                keys.retain(|k| k != key);
            }
        }
    }
}

// ── Daemon (the "lib" — all business logic, no I/O) ─────────────

pub(crate) struct Daemon {
    config: Config,
    store: OnceLock<Mutex<Store>>,
    remote_backend: tokio::sync::OnceCell<Arc<dyn crate::remote_backend::RemoteBackend>>,
    key_cache: Arc<S3KeyCache>,
    remote_health: Arc<RemoteHealth>,
    s3_semaphore: Arc<tokio::sync::Semaphore>,
    upload_tx: Mutex<Option<tokio::sync::mpsc::UnboundedSender<UploadJob>>>,
    upload_queue_closed: AtomicBool,
    /// Keys currently queued or in-flight for upload (dedup guard).
    pending_uploads: Arc<RwLock<HashSet<String>>>,
    /// Keys with an in-flight download, each mapped to the per-key [`Notify`]
    /// that wakes waiters when the leader's [`DownloadingGuard`] drops.
    /// Claiming is an atomic insert-if-absent (see [`claim_download`]).
    downloading: Arc<RwLock<HashMap<String, Arc<Notify>>>>,
    /// Signals when manifest prefetch completes (or is skipped).
    /// `handle_remote_check` waits on this to avoid racing the batch prefetch.
    warming_tx: tokio::sync::watch::Sender<bool>,
    /// Keys downloaded during manifest/shard prefetch. Used to distinguish
    /// PrefetchHit from LocalHit in wrapper event logging.
    prefetched_keys: Arc<RwLock<HashSet<String>>>,
    /// Counters for adaptive prefetch cancellation: number of remote checks
    /// against prefetched keys (checks) vs how many were actually used (hits).
    prefetch_checks: Arc<AtomicU32>,
    prefetch_hits: Arc<AtomicU32>,
    /// Signals remaining prefetch downloads to stop when hit rate is too low.
    prefetch_cancel: tokio::sync::watch::Sender<bool>,
    /// Phase-0 observability counters (#485). Telemetry only.
    prefetch_stats: PrefetchStats,
    /// DAEMON-WIDE cap on concurrent speculative prefetch downloads, sized by
    /// [`prefetch_concurrency_cap`]. Each prefetch task holds one gate permit
    /// for its whole S3-permit tenure, so across ALL coordinators (startup
    /// manifest/shard prefetch overlapping a BuildStarted plan) prefetch can
    /// never occupy more than `cap` of the `s3_concurrency` pool — the reserve
    /// stays available to interactive RemoteCheck. Gate is always acquired
    /// BEFORE the S3 permit and only by prefetch tasks, so no lock-order cycle
    /// with interactive paths exists (cross-family review finding, #485).
    prefetch_gate: Arc<tokio::sync::Semaphore>,
    /// Prefetched keys that a wrapper later requested — the distinct-"used"
    /// side of `PrefetchStats::keys_used`. Separate from `prefetched_keys`
    /// (which must keep every key for PrefetchHit labeling) so counting a use
    /// doesn't disturb labels. Bounded alongside `prefetched_keys`.
    prefetch_used_keys: Arc<RwLock<HashSet<String>>>,
    version: String,
    build_epoch: u64,
    transfer_counters: TransferCounters,
    recent_transfers: std::sync::Mutex<std::collections::VecDeque<TransferEvent>>,
    file_hash_cache: Arc<Mutex<HashMap<FileHashCacheKey, String>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FileHashCacheKey {
    path: String,
    size: i64,
    mtime_ns: i64,
    ctime_ns: i64,
    inode: i64,
}

impl Daemon {
    pub fn new(config: Config) -> Self {
        let permits = config.s3_concurrency.max(1) as usize;
        let (warming_tx, _) = tokio::sync::watch::channel(false);
        let (prefetch_cancel, _) = tokio::sync::watch::channel(false);
        Self {
            store: OnceLock::new(),
            s3_semaphore: Arc::new(tokio::sync::Semaphore::new(permits)),
            remote_backend: tokio::sync::OnceCell::new(),
            key_cache: Arc::new(S3KeyCache::new()),
            remote_health: Arc::new(RemoteHealth::new()),
            upload_tx: Mutex::new(None),
            upload_queue_closed: AtomicBool::new(false),
            pending_uploads: Arc::new(RwLock::new(HashSet::new())),
            downloading: Arc::new(RwLock::new(HashMap::new())),
            warming_tx,
            prefetched_keys: Arc::new(RwLock::new(HashSet::new())),
            prefetch_checks: Arc::new(AtomicU32::new(0)),
            prefetch_hits: Arc::new(AtomicU32::new(0)),
            prefetch_cancel,
            prefetch_stats: PrefetchStats::new(),
            prefetch_gate: Arc::new(tokio::sync::Semaphore::new(prefetch_concurrency_cap(
                config.s3_concurrency,
            ))),
            prefetch_used_keys: Arc::new(RwLock::new(HashSet::new())),
            version: VERSION.to_string(),
            build_epoch: build_epoch(),
            transfer_counters: TransferCounters::new(),
            recent_transfers: std::sync::Mutex::new(std::collections::VecDeque::new()),
            file_hash_cache: Arc::new(Mutex::new(HashMap::new())),
            config,
        }
    }

    fn store_lock(&self) -> Result<&Mutex<Store>> {
        if let Some(store) = self.store.get() {
            return Ok(store);
        }

        let store = Store::open(&self.config)?;
        let _ = self.store.set(Mutex::new(store));

        self.store
            .get()
            .ok_or_else(|| anyhow::anyhow!("daemon store failed to initialize"))
    }

    pub(crate) fn with_store<T>(&self, f: impl FnOnce(&Store) -> Result<T>) -> Result<T> {
        let guard = self
            .store_lock()?
            .lock()
            .map_err(|_| anyhow::anyhow!("daemon store mutex poisoned"))?;
        f(&guard)
    }

    pub(crate) fn entry_dir_for(&self, cache_key: &str) -> PathBuf {
        // Defense-in-depth: every caller must validate untrusted keys before
        // reaching here (see `is_valid_cache_key`), so a malformed key getting
        // this far is a programming error. A 64-char hex key can never contain
        // a path separator or `..`, so the join stays inside the store.
        debug_assert!(
            crate::cache_key::is_valid_cache_key(cache_key),
            "entry_dir_for called with unvalidated cache_key"
        );
        self.config.store_dir().join(cache_key)
    }

    pub(crate) fn remote_config(&self) -> Option<&crate::config::RemoteConfig> {
        self.config.remote.as_ref()
    }

    pub(crate) async fn key_cache_keys_for_crate(&self, crate_name: &str) -> Vec<String> {
        self.key_cache.keys_for_crate(crate_name).await
    }

    /// Wait for the manifest prefetch to complete (or timeout).
    /// Returns immediately if warming already finished or no remote is configured.
    async fn wait_for_warming(&self, timeout: Duration) -> bool {
        let mut rx = self.warming_tx.subscribe();
        if *rx.borrow() {
            return true;
        }
        matches!(
            tokio::time::timeout(timeout, rx.changed()).await,
            Ok(Ok(()))
        ) || *rx.borrow()
    }

    /// Mark warming as complete. Called after manifest prefetch finishes.
    fn signal_warming_complete(&self) {
        self.warming_tx.send_replace(true);
    }

    fn push_transfer_event(&self, event: TransferEvent) {
        // Persist to JSONL — warn on failure but never fail the transfer
        if let Err(e) = events::log_transfer(&self.config.transfer_log_path(), &event) {
            tracing::warn!("failed to log transfer event: {e}");
        }
        if let Ok(mut q) = self.recent_transfers.lock() {
            if q.len() >= RECENT_TRANSFERS_CAP {
                q.pop_front();
            }
            q.push_back(event);
        }
    }

    /// Set the upload buffer sender (called during server setup).
    pub fn set_upload_tx(&self, tx: tokio::sync::mpsc::UnboundedSender<UploadJob>) {
        *self.upload_tx.lock().expect("upload queue mutex poisoned") = Some(tx);
        self.upload_queue_closed.store(false, Ordering::Relaxed);
    }

    fn upload_tx(&self) -> Option<tokio::sync::mpsc::UnboundedSender<UploadJob>> {
        self.upload_tx
            .lock()
            .expect("upload queue mutex poisoned")
            .clone()
    }

    fn close_upload_queue(&self) {
        self.upload_queue_closed.store(true, Ordering::Relaxed);
        self.upload_tx
            .lock()
            .expect("upload queue mutex poisoned")
            .take();
    }

    /// Lazy-init the remote backend (requires remote config).
    pub(crate) async fn get_remote_backend(
        &self,
    ) -> Result<&Arc<dyn crate::remote_backend::RemoteBackend>> {
        self.remote_backend
            .get_or_try_init(|| async {
                let remote = self
                    .config
                    .remote
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("no remote configured"))?;
                crate::remote_backend::create_backend(remote, self.config.s3_pool_idle_secs).await
            })
            .await
    }

    /// Dispatch a parsed request to the appropriate handler (sync-only requests).
    #[cfg(test)]
    pub fn handle_request_sync(&self, req: &Request) -> Response {
        match req {
            Request::Gc(gc) => self.handle_gc(gc),
            Request::Stats(sr) => self.handle_stats(sr),
            Request::HashFiles(req) => self.handle_hash_files(req),
            Request::Upload(_)
            | Request::RemoteCheck(_)
            | Request::BatchRemoteCheck(_)
            | Request::Prefetch(_)
            | Request::BuildStarted(_) => {
                // These require async — caller must use their async handlers
                Response::err(
                    "upload/remote_check/batch/prefetch/build_started must be handled async",
                )
            }
            Request::Shutdown => Response::ok(),
        }
    }

    /// Handle a stats request — reads store and event log.
    pub fn handle_stats(&self, req: &StatsRequest) -> Response {
        let (total_size, entry_count, entries) = match self.with_store(|store| {
            let total_size = store.total_size().unwrap_or(0);
            let entry_count = store.entry_count().unwrap_or(0);
            let entries = if req.include_entries {
                let sort = req.sort_by.as_deref().unwrap_or("size");
                store.list_entries(sort).ok().map(|list| {
                    list.into_iter()
                        .map(|e| StatsEntry {
                            cache_key: e.cache_key,
                            crate_name: e.crate_name,
                            crate_type: e.crate_type,
                            profile: e.profile,
                            size: e.size,
                            hit_count: e.hit_count,
                            created_at: e.created_at,
                            last_accessed: e.last_accessed,
                            content_hash: e.content_hash,
                        })
                        .collect()
                })
            } else {
                None
            };
            Ok((total_size, entry_count, entries))
        }) {
            Ok(values) => values,
            Err(e) => return Response::err(format!("store open failed: {e}")),
        };

        let hours = req.event_hours.unwrap_or(24);
        let since = chrono::Utc::now() - chrono::Duration::hours(hours as i64);
        let event_list =
            events::read_events_since(&self.config.event_log_path(), since).unwrap_or_default();
        let es = events::compute_stats(&event_list);

        let pending_uploads = self
            .pending_uploads
            .try_read()
            .map(|g| g.len())
            .unwrap_or(0);
        let active_downloads = self.downloading.try_read().map(|g| g.len()).unwrap_or(0);

        let tc = &self.transfer_counters;
        let ps = &self.prefetch_stats;
        let s3_total = self.config.s3_concurrency.max(1) as usize;
        let s3_used = s3_total - self.s3_semaphore.available_permits();

        let recent_transfers = self
            .recent_transfers
            .try_lock()
            .map(|q| q.iter().cloned().collect())
            .unwrap_or_default();

        Response::ok_stats(StatsResponse {
            total_size,
            max_size: self.config.max_size,
            entry_count,
            entries,
            events: EventStatsResponse {
                local_hits: es.local_hits,
                prefetch_hits: es.prefetch_hits,
                remote_hits: es.remote_hits,
                dups: es.dups,
                misses: es.misses,
                errors: es.errors,
                total_elapsed_ms: es.total_elapsed_ms,
                hit_elapsed_ms: es.hit_elapsed_ms,
                miss_elapsed_ms: es.miss_elapsed_ms,
                hit_compile_time_ms: es.hit_compile_time_ms,
                miss_compile_time_ms: es.miss_compile_time_ms,
                store_output_blobs: es.store_output_blobs,
                store_duplicate_blobs: es.store_duplicate_blobs,
                store_new_blobs: es.store_new_blobs,
            },
            version: self.version.clone(),
            build_epoch: self.build_epoch,
            pending_uploads,
            active_downloads,
            s3_concurrency_total: s3_total,
            s3_concurrency_used: s3_used,
            upload_queue_capacity: 0,
            uploads_completed: tc.uploads_completed.load(Ordering::Relaxed),
            uploads_failed: tc.uploads_failed.load(Ordering::Relaxed),
            uploads_skipped: tc.uploads_skipped.load(Ordering::Relaxed),
            downloads_completed: tc.downloads_completed.load(Ordering::Relaxed),
            downloads_failed: tc.downloads_failed.load(Ordering::Relaxed),
            bytes_uploaded: tc.bytes_uploaded.load(Ordering::Relaxed),
            bytes_downloaded: tc.bytes_downloaded.load(Ordering::Relaxed),
            recent_transfers,
            prefetch: PrefetchStatsSnapshot {
                downloads_completed: ps.downloads_completed.load(Ordering::Relaxed),
                bytes_downloaded: ps.bytes_downloaded.load(Ordering::Relaxed),
                keys_used: ps.keys_used.load(Ordering::Relaxed),
                keys_cancelled: ps.keys_cancelled.load(Ordering::Relaxed),
                cancelled: *self.prefetch_cancel.borrow(),
                plans_advisory: ps.plans_advisory.load(Ordering::Relaxed),
                plans_fallback: ps.plans_fallback.load(Ordering::Relaxed),
                last_plan_candidates: ps.last_plan_candidates.load(Ordering::Relaxed),
                dedup_join_waits: ps.dedup_join_waits.load(Ordering::Relaxed),
                dedup_join_wait_ms: ps.dedup_join_wait_ms.load(Ordering::Relaxed),
                last_list_duration_ms: ps.last_list_duration_ms.load(Ordering::Relaxed),
                last_list_key_count: ps.last_list_key_count.load(Ordering::Relaxed),
            },
        })
    }

    pub fn handle_hash_files(&self, req: &HashFilesRequest) -> Response {
        let mut results = Vec::with_capacity(req.files.len());

        for file in &req.files {
            let key = FileHashCacheKey {
                path: file.path.clone(),
                size: file.size,
                mtime_ns: file.mtime_ns,
                ctime_ns: file.ctime_ns,
                inode: file.inode,
            };

            if let Ok(cache) = self.file_hash_cache.lock()
                && let Some(hash) = cache.get(&key).cloned()
            {
                results.push(HashFileResult {
                    path: file.path.clone(),
                    size: file.size,
                    mtime_ns: file.mtime_ns,
                    ctime_ns: file.ctime_ns,
                    inode: file.inode,
                    hash: Some(hash),
                    cache_hit: true,
                    bytes_hashed: 0,
                    error: None,
                });
                continue;
            }

            match std::fs::metadata(&file.path) {
                Ok(metadata)
                    if i64::try_from(metadata.len()).unwrap_or(i64::MAX) == file.size
                        && crate::cache_key::metadata_mtime_ns(&metadata) == file.mtime_ns
                        && crate::cache_key::metadata_ctime_ns(&metadata) == file.ctime_ns
                        && crate::cache_key::metadata_inode(&metadata) == file.inode => {}
                Ok(_) => {
                    results.push(HashFileResult {
                        path: file.path.clone(),
                        size: file.size,
                        mtime_ns: file.mtime_ns,
                        ctime_ns: file.ctime_ns,
                        inode: file.inode,
                        hash: None,
                        cache_hit: false,
                        bytes_hashed: 0,
                        error: Some("file metadata changed before hashing".into()),
                    });
                    continue;
                }
                Err(e) => {
                    results.push(HashFileResult {
                        path: file.path.clone(),
                        size: file.size,
                        mtime_ns: file.mtime_ns,
                        ctime_ns: file.ctime_ns,
                        inode: file.inode,
                        hash: None,
                        cache_hit: false,
                        bytes_hashed: 0,
                        error: Some(e.to_string()),
                    });
                    continue;
                }
            }

            // #281: hold the store mutex only for the cheap cache lookup and
            // record; run the blake3 read of the whole file OUTSIDE the lock so
            // it can't stall a concurrent RemoteCheck's `import_restored_entry`.
            let path = Path::new(&file.path);
            let computed: anyhow::Result<(String, bool, u64)> =
                match self.with_store(|store| Ok(store.file_hash_lookup(path))) {
                    Ok(crate::cache_key::FileHashLookup::Hit(hash)) => Ok((hash, true, 0)),
                    Ok(crate::cache_key::FileHashLookup::NeedsHash(fp)) => {
                        crate::cache_key::hash_file(path).map(|hash| {
                            // Brief re-lock just to persist the result.
                            let _ = self.with_store(|store| {
                                store.file_hash_record(&fp, &hash);
                                Ok(())
                            });
                            (hash, false, file.size.max(0) as u64)
                        })
                    }
                    Ok(crate::cache_key::FileHashLookup::Uncacheable) => {
                        crate::cache_key::hash_file(path)
                            .map(|hash| (hash, false, file.size.max(0) as u64))
                    }
                    Err(e) => Err(e),
                };

            match computed {
                Ok((hash, cache_hit, bytes_hashed)) => {
                    if let Ok(mut cache) = self.file_hash_cache.lock() {
                        if cache.len() >= FILE_HASH_MEMORY_CACHE_CAP {
                            cache.clear();
                        }
                        cache.insert(key, hash.clone());
                    }

                    results.push(HashFileResult {
                        path: file.path.clone(),
                        size: file.size,
                        mtime_ns: file.mtime_ns,
                        ctime_ns: file.ctime_ns,
                        inode: file.inode,
                        hash: Some(hash),
                        cache_hit,
                        bytes_hashed,
                        error: None,
                    });
                }
                Err(e) => results.push(HashFileResult {
                    path: file.path.clone(),
                    size: file.size,
                    mtime_ns: file.mtime_ns,
                    ctime_ns: file.ctime_ns,
                    inode: file.inode,
                    hash: None,
                    cache_hit: false,
                    bytes_hashed: 0,
                    error: Some(e.to_string()),
                }),
            }
        }

        Response::ok_hash_results(results)
    }

    /// Handle a GC request — pure logic against the store.
    pub fn handle_gc(&self, req: &GcRequest) -> Response {
        match self.run_gc(req.max_age_hours) {
            Ok(stats) if stats.skipped => Response::ok_gc_skipped(),
            Ok(stats) => Response::ok_evicted(stats.entries_evicted),
            Err(e) => Response::err(format!("gc failed: {e}")),
        }
    }

    /// Handle an upload job. If the upload queue is available, pushes to it (non-blocking).
    /// Otherwise falls back to direct upload (used in tests).
    pub async fn handle_upload(&self, job: &UploadJob) -> Response {
        if self.config.remote_readonly {
            tracing::debug!(
                crate_name = job.crate_name,
                key = key_prefix(&job.key),
                "remote uploads disabled (read-only mode)"
            );
            return Response::ok();
        }

        if self.config.remote.is_none() {
            return Response::err("no remote configured");
        }

        // If upload buffer is set up (server mode), push to it for async processing
        if let Some(tx) = self.upload_tx() {
            // Dedup: skip if this key is already queued or in-flight
            {
                let mut pending = self.pending_uploads.write().await;
                if !pending.insert(job.key.clone()) {
                    return Response::ok(); // already pending
                }
            }
            return match tx.send(job.clone()) {
                Ok(()) => Response::ok(),
                Err(_) => {
                    self.pending_uploads.write().await.remove(&job.key);
                    Response::err("upload queue closed")
                }
            };
        }

        if self.upload_queue_closed.load(Ordering::Relaxed) {
            return Response::err("upload queue closed");
        }

        // Fallback: direct upload (no queue available)
        let Ok(_permit) = self.s3_semaphore.acquire().await else {
            return Response::err("S3 semaphore closed");
        };
        self.do_upload(job).await
    }

    /// Execute an upload directly (used by upload queue workers).
    pub async fn do_upload(&self, job: &UploadJob) -> Response {
        let key_short = key_prefix(&job.key);
        if self.config.remote_readonly {
            tracing::debug!(
                crate_name = job.crate_name,
                key = key_short,
                "skipping upload (read-only mode)"
            );
            return Response::ok();
        }

        let Some(remote) = &self.config.remote else {
            return Response::err("no remote configured");
        };

        let backend = match self.get_remote_backend().await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    crate_name = job.crate_name,
                    key = key_short,
                    "remote backend init failed: {e:#}"
                );
                return Response::err(format!("remote backend init failed: {e:#}"));
            }
        };
        let plan = crate::remote_plan::RemotePlanner::new(&self.config)
            .plan(crate::remote_plan::RemoteWorkload::BackgroundUpload);
        let layout = plan.layout(backend.as_ref(), remote);

        let already_exists = layout
            .exists_entry(&job.key, &job.crate_name)
            .await
            .unwrap_or(false);

        if already_exists {
            self.key_cache
                .insert(job.key.clone(), Some(&job.crate_name))
                .await;
            self.transfer_counters
                .uploads_skipped
                .fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                crate_name = job.crate_name,
                key = key_short,
                "skipping upload — already in S3"
            );
            return Response::ok();
        }

        tracing::debug!(
            crate_name = job.crate_name,
            key = key_short,
            bucket = remote.bucket,
            "starting S3 upload"
        );

        let entry_dir = PathBuf::from(&job.entry_dir);
        let blobs_dir = self.config.store_dir().join("blobs");
        let start = Instant::now();
        match layout
            .upload_entry(
                &job.key,
                &job.crate_name,
                &entry_dir,
                &blobs_dir,
                self.config.compression_level,
            )
            .await
        {
            Ok(ul) => {
                let elapsed_ms = start.elapsed().as_millis() as u64;
                self.transfer_counters
                    .uploads_completed
                    .fetch_add(1, Ordering::Relaxed);
                self.transfer_counters
                    .bytes_uploaded
                    .fetch_add(ul.transfer.compressed_bytes, Ordering::Relaxed);
                self.push_transfer_event(TransferEvent {
                    schema: default_transfer_schema(),
                    crate_name: job.crate_name.clone(),
                    direction: TransferDirection::Upload,
                    format: ul.format.to_string(),
                    cache_key: job.key.clone(),
                    object_key: String::new(),
                    compressed_bytes: ul.transfer.compressed_bytes,
                    elapsed_ms,
                    network_ms: ul.transfer.network_ms,
                    semaphore_wait_ms: 0,
                    head_ms: 0,
                    request_ms: 0,
                    body_ms: 0,
                    request_count: 0,
                    original_bytes: 0,
                    decompress_ms: 0,
                    extract_ms: 0,
                    disk_io_ms: 0,
                    import_ms: 0,
                    compression_ms: ul.transfer.compression_ms,
                    head_checks_ms: ul.transfer.head_checks_ms,
                    blobs_skipped: 0,
                    blobs_total: 0,
                    ok: true,
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                });
                self.key_cache
                    .insert(job.key.clone(), Some(&job.crate_name))
                    .await;
                self.maybe_evict_after_upload();
                Response::ok()
            }
            Err(e) => {
                let elapsed_ms = start.elapsed().as_millis() as u64;
                self.transfer_counters
                    .uploads_failed
                    .fetch_add(1, Ordering::Relaxed);
                self.push_transfer_event(TransferEvent {
                    schema: default_transfer_schema(),
                    crate_name: job.crate_name.clone(),
                    direction: TransferDirection::Upload,
                    format: plan.transfer_format().to_string(),
                    cache_key: job.key.clone(),
                    object_key: String::new(),
                    compressed_bytes: 0,
                    elapsed_ms,
                    network_ms: 0,
                    semaphore_wait_ms: 0,
                    head_ms: 0,
                    request_ms: 0,
                    body_ms: 0,
                    request_count: 0,
                    original_bytes: 0,
                    decompress_ms: 0,
                    extract_ms: 0,
                    disk_io_ms: 0,
                    import_ms: 0,
                    compression_ms: 0,
                    head_checks_ms: 0,
                    blobs_skipped: 0,
                    blobs_total: 0,
                    ok: false,
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                });
                tracing::warn!(
                    crate_name = job.crate_name,
                    key = key_short,
                    elapsed_ms,
                    "upload to S3 failed: {e:#}"
                );
                Response::err(format!("upload failed: {e:#}"))
            }
        }
    }

    /// Handle a remote check: check S3 for a cache key, download if found. Gated by S3 semaphore.
    /// Waits for the manifest prefetch to finish first so batch downloads aren't bypassed.
    pub async fn handle_remote_check(&self, req: &RemoteCheckRequest) -> Response {
        let Some(remote) = &self.config.remote else {
            return Response::err("no remote configured");
        };

        if !self.wait_for_warming(REMOTE_CHECK_WARMING_GRACE).await {
            tracing::debug!(
                "remote check: warming barrier timed out after {}ms, continuing with fallback path",
                REMOTE_CHECK_WARMING_GRACE.as_millis()
            );
        }

        // Adaptive prefetch cancellation: track whether prefetched keys are being used.
        // After enough checks, if hit rate is too low, cancel remaining prefetch downloads.
        {
            let is_prefetched = self.prefetched_keys.read().await.contains(&req.key);
            if is_prefetched {
                self.prefetch_checks.fetch_add(1, Ordering::Relaxed);
                // This is a hit — the wrapper is requesting a key that was prefetched
                self.prefetch_hits.fetch_add(1, Ordering::Relaxed);
                // Phase-0 telemetry: count each prefetched key as "used" once
                // (distinct keys, unlike the per-check counters above).
                if self
                    .prefetch_used_keys
                    .write()
                    .await
                    .insert(req.key.clone())
                {
                    self.prefetch_stats
                        .keys_used
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            let checks = self.prefetch_checks.load(Ordering::Relaxed);
            let hits = self.prefetch_hits.load(Ordering::Relaxed);
            if checks >= 10 && (hits as f64 / checks as f64) < 0.3 {
                // Hit rate below 30% after 10+ checks — cancel remaining prefetch
                let _ = self.prefetch_cancel.send(true);
                tracing::info!(
                    "adaptive prefetch cancel: {hits}/{checks} hit rate, cancelling remaining downloads"
                );
            }
        }

        let cn = &req.crate_name;
        let mut needs_head_probe = false;
        let mut head_ms = 0u64;
        let mut semaphore_wait_ms = 0u64;

        // Check key cache first (no semaphore needed for in-memory lookup)
        match self.key_cache.check(&req.key).await {
            Some(false) => {
                let authoritative = matches!(
                    self.key_cache.age().await,
                    Some(age) if age <= KEY_CACHE_AUTHORITATIVE_FOR
                );
                if authoritative {
                    tracing::debug!("key cache: {} not found (skipping S3)", &req.key);
                    return Response::found(false);
                }
                if self.remote_health.head_probe_is_degraded() {
                    self.remote_health.note_head_probe_suppressed();
                    tracing::debug!(
                        "key cache: {} not found but cache is stale and remote HEAD probes are degraded, treating as miss",
                        &req.key
                    );
                    return Response::found(false);
                }
                tracing::debug!(
                    "key cache: {} not found but cache is stale, falling through to HEAD",
                    &req.key
                );
                needs_head_probe = true;
            }
            Some(true) => {
                tracing::debug!("key cache: {} found, skipping HEAD", &req.key);
                // Skip HEAD, go straight to download
            }
            None => {
                if self.remote_health.head_probe_is_degraded() {
                    self.remote_health.note_head_probe_suppressed();
                    tracing::debug!(
                        "key cache unavailable and remote HEAD probes are degraded, treating {} as a miss",
                        &req.key
                    );
                    return Response::found(false);
                }
                needs_head_probe = true;
            }
        }

        let backend = match self.get_remote_backend().await {
            Ok(b) => b,
            Err(e) => return Response::err(format!("remote backend init failed: {e}")),
        };
        let plan = crate::remote_plan::RemotePlanner::new(&self.config)
            .plan(crate::remote_plan::RemoteWorkload::RestoreCheck);
        let layout = plan.layout(backend.as_ref(), remote);

        if needs_head_probe {
            let semaphore_start = Instant::now();
            let Ok(_permit) = self.s3_semaphore.acquire().await else {
                return Response::err("S3 semaphore closed");
            };
            semaphore_wait_ms += semaphore_start.elapsed().as_millis() as u64;
            let head_start = Instant::now();
            let exists = layout.exists_entry(&req.key, cn).await;
            head_ms += head_start.elapsed().as_millis() as u64;
            match exists {
                Ok(false) => {
                    self.remote_health.note_head_probe_success();
                    return Response::found(false);
                }
                Ok(true) => {
                    self.remote_health.note_head_probe_success();
                    self.key_cache
                        .insert(req.key.clone(), Some(&req.crate_name))
                        .await;
                }
                Err(e) => {
                    // A non-404 error is transient (throttling / 5xx), not a
                    // confirmed miss. Do one bounded retry before falling back,
                    // so a single S3 blip under load isn't recorded as an
                    // authoritative "not in remote" — which would force a full
                    // recompile + re-upload exactly when the remote is
                    // struggling. Still fail safe (found=false) if it persists.
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    let retry_start = Instant::now();
                    let retried = layout.exists_entry(&req.key, cn).await;
                    head_ms += retry_start.elapsed().as_millis() as u64;
                    match retried {
                        Ok(true) => {
                            self.remote_health.note_head_probe_success();
                            self.key_cache
                                .insert(req.key.clone(), Some(&req.crate_name))
                                .await;
                        }
                        Ok(false) => {
                            self.remote_health.note_head_probe_success();
                            return Response::found(false);
                        }
                        Err(e2) => {
                            let error =
                                format!("S3 exists check failed (after retry): {e2}; first: {e}");
                            self.remote_health.note_head_probe_failure(&error);
                            return Response::found(false);
                        }
                    }
                }
            }
        }

        // Download dedup — atomically claim this key. Exactly one task per key
        // is the leader that performs the download; everyone else receives the
        // leader's per-key `Notify` and parks on it until the leader's claim
        // guard drops (success OR failure), instead of polling the map at
        // 100ms for up to 30s. Claiming under one write lock collapses the old
        // read-check-then-write window where two tasks both saw "not
        // downloading" and both downloaded (racing on the destructive
        // entry_dir remove/recreate inside extraction) (#213).
        let mut claimed = true;
        if let Some(mut notify) = claim_download(&self.downloading, &req.key).await {
            tracing::debug!("already downloading {}, waiting for completion", &req.key);
            let join_start = Instant::now();
            let deadline = tokio::time::Instant::now() + DOWNLOAD_JOIN_BUDGET;
            let entry_dir = PathBuf::from(&req.entry_dir);
            claimed = false;
            let found = loop {
                // Missed-wakeup guard: register interest in the Notify BEFORE
                // re-checking the map. `notify_waiters` only wakes futures
                // that are already registered, so a leader whose guard drops
                // between "saw the key present" (the claim above / re-claim
                // below) and "started waiting" would otherwise be missed and
                // this task would stall until the deadline. `enable()`
                // registers the pinned future without awaiting it; the map
                // re-check then tells us whether the leader is already gone
                // (skip the wait entirely).
                let mut timed_out = false;
                let mut adopt: Option<Arc<Notify>> = None;
                {
                    let notified = notify.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    // Generation check, not mere presence (cross-family review
                    // finding): the map entry must be THE SAME Notify we just
                    // registered on. If the old leader failed and broadcast
                    // before we registered, and another task already re-claimed
                    // with a fresh Notify, waiting here on the OLD one would
                    // stall until the deadline even though the new leader may
                    // finish immediately. Adopt the current generation instead
                    // and re-register (below this scope — the pinned future
                    // borrows `notify`).
                    match self.downloading.read().await.get(&req.key).cloned() {
                        Some(cur) if Arc::ptr_eq(&cur, &notify) => {
                            timed_out = tokio::time::timeout_at(deadline, notified).await.is_err();
                        }
                        Some(cur) if tokio::time::Instant::now() < deadline => {
                            adopt = Some(cur);
                        }
                        // Generation changed but the budget is gone: fall
                        // through to the meta.json check + re-claim with the
                        // timeout semantics.
                        Some(_) => timed_out = true,
                        None => {}
                    }
                }
                if let Some(cur) = adopt {
                    notify = cur;
                    continue;
                }
                // Woken (leader's guard dropped), leader already gone, or
                // budget exhausted — if the leader landed the entry, use it.
                if entry_dir.join("meta.json").exists() {
                    break true;
                }
                // The leader failed (or was cancelled). Re-claim atomically:
                // insert-if-absent elects exactly ONE waiter as the new
                // leader. (The old poll-based code re-inserted the key while
                // IGNORING the result, so every waiter that exhausted the poll
                // budget proceeded as an "owner" and double-downloaded — the
                // very race the #213 claim exists to prevent.)
                match claim_download(&self.downloading, &req.key).await {
                    None => {
                        claimed = true;
                        break false;
                    }
                    Some(next) => {
                        if timed_out {
                            // Budget exhausted and another task still holds
                            // the claim (a leader wedged for >30s). Degrade
                            // the way the old poll did: proceed WITHOUT the
                            // claim rather than block a build indefinitely.
                            tracing::warn!(
                                key = key_prefix(&req.key),
                                "download dedup wait exceeded {DOWNLOAD_JOIN_BUDGET:?}; \
                                 proceeding without claim"
                            );
                            break false;
                        }
                        // A different waiter won the re-claim; keep waiting,
                        // now on the NEW leader's Notify.
                        notify = next;
                    }
                }
            };
            // Phase-0 telemetry: how often and how long RemoteCheck blocks
            // behind another task's in-flight download (total elapsed wait,
            // bumped once per waiter).
            self.prefetch_stats
                .dedup_join_waits
                .fetch_add(1, Ordering::Relaxed);
            self.prefetch_stats
                .dedup_join_wait_ms
                .fetch_add(join_start.elapsed().as_millis() as u64, Ordering::Relaxed);
            if found {
                let was_prefetched = self.prefetched_keys.read().await.contains(&req.key);
                return Response::found_prefetched(true, was_prefetched);
            }
        }
        // Leader path: the claim is released on every exit path below (incl.
        // panic) by Drop, which also wakes all waiters. If the join budget
        // expired without winning a re-claim we proceed WITHOUT a claim — no
        // guard is constructed, so we never release a claim we don't hold.
        let _dl_guard =
            claimed.then(|| DownloadingGuard::new(self.downloading.clone(), req.key.clone()));

        // Acquire semaphore for download
        let semaphore_start = Instant::now();
        let Ok(_permit) = self.s3_semaphore.acquire().await else {
            return Response::err("S3 semaphore closed");
        };
        semaphore_wait_ms += semaphore_start.elapsed().as_millis() as u64;

        // Download to local store using the current remote layout.
        let entry_dir = PathBuf::from(&req.entry_dir);
        let blobs_dir = self.config.store_dir().join("blobs");
        let start = Instant::now();
        let download_result = layout
            .download_entry(&req.key, cn, &entry_dir, &blobs_dir)
            .await;

        match download_result {
            Ok(dl) => {
                let elapsed_ms = start.elapsed().as_millis() as u64;
                let import_start = Instant::now();
                let import_ms = if let Err(e) =
                    self.with_store(|store| store.import_restored_entry(&req.key))
                {
                    tracing::warn!("failed to import downloaded entry {}: {e}", &req.key);
                    0
                } else {
                    import_start.elapsed().as_millis() as u64
                };
                self.transfer_counters
                    .downloads_completed
                    .fetch_add(1, Ordering::Relaxed);
                self.transfer_counters
                    .bytes_downloaded
                    .fetch_add(dl.compressed_bytes, Ordering::Relaxed);
                self.push_transfer_event(TransferEvent {
                    schema: default_transfer_schema(),
                    crate_name: cn.to_string(),
                    direction: TransferDirection::Download,
                    format: dl.format.to_string(),
                    cache_key: req.key.clone(),
                    object_key: dl.object_key,
                    compressed_bytes: dl.compressed_bytes,
                    elapsed_ms,
                    network_ms: dl.network_ms,
                    semaphore_wait_ms,
                    head_ms,
                    request_ms: dl.request_ms,
                    body_ms: dl.body_ms,
                    request_count: dl.request_count,
                    original_bytes: dl.original_bytes,
                    decompress_ms: dl.decompress_ms,
                    extract_ms: dl.extract_ms,
                    disk_io_ms: dl.disk_io_ms,
                    import_ms,
                    compression_ms: 0,
                    head_checks_ms: 0,
                    blobs_skipped: dl.blobs_skipped,
                    blobs_total: dl.blobs_total,
                    ok: true,
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                });
                Response::found(true)
            }
            Err(e)
                if e.downcast_ref::<crate::remote_layout::EntryNotFound>()
                    .is_some() =>
            {
                // GET 404 = clean miss (#485 Phase 0). Reached when a
                // key-cache positive was stale (upload evicted/GC'd) or the
                // direct-GET path raced an upload. Correct the cache so the
                // next check doesn't repeat the GET, and report a miss — the
                // wrapper compiles as usual. Not a transfer failure.
                tracing::debug!("remote GET 404 for {} — treating as miss", &req.key);
                self.key_cache.remove(&req.key).await;
                Response::found(false)
            }
            Err(e) => {
                let elapsed_ms = start.elapsed().as_millis() as u64;
                self.transfer_counters
                    .downloads_failed
                    .fetch_add(1, Ordering::Relaxed);
                self.push_transfer_event(TransferEvent {
                    schema: default_transfer_schema(),
                    crate_name: cn.to_string(),
                    direction: TransferDirection::Download,
                    format: plan.transfer_format().to_string(),
                    cache_key: req.key.clone(),
                    object_key: String::new(),
                    compressed_bytes: 0,
                    elapsed_ms,
                    network_ms: 0,
                    semaphore_wait_ms,
                    head_ms,
                    request_ms: 0,
                    body_ms: 0,
                    request_count: 0,
                    original_bytes: 0,
                    decompress_ms: 0,
                    extract_ms: 0,
                    disk_io_ms: 0,
                    import_ms: 0,
                    compression_ms: 0,
                    head_checks_ms: 0,
                    blobs_skipped: 0,
                    blobs_total: 0,
                    ok: false,
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                });
                Response::err(format!("S3 download failed: {e}"))
            }
        }
    }

    /// Handle a batch remote check: check multiple keys against S3 concurrently.
    pub async fn handle_batch_remote_check(
        self: &Arc<Self>,
        req: &BatchRemoteCheckRequest,
    ) -> Response {
        let futures: Vec<_> = req
            .checks
            .iter()
            .map(|check| self.handle_remote_check(check))
            .collect();
        let results = futures::future::join_all(futures).await;
        Response::ok_batch(results)
    }

    /// Handle a prefetch request: fire-and-forget background downloads.
    /// Spawns a single coordinator task that processes keys with bounded concurrency.
    pub async fn handle_prefetch(self: &Arc<Self>, req: &PrefetchRequest) -> Response {
        let Some(remote) = &self.config.remote else {
            return Response::err("no remote configured");
        };

        if self.get_remote_backend().await.is_err() {
            return Response::err("remote backend init failed");
        }

        // Filter to keys that need downloading: (cache_key, crate_name, entry_dir)
        let mut keys_to_fetch: Vec<(String, String, PathBuf)> = Vec::new();
        let downloading_guard = self.downloading.read().await;
        for (key, crate_name) in &req.keys {
            if !crate::cache_key::is_valid_cache_key(key)
                || !crate::cache_key::is_valid_crate_name(crate_name)
            {
                tracing::warn!(
                    key = key_prefix(key),
                    "prefetch: skipping request key with invalid cache_key/crate_name"
                );
                continue;
            }
            let entry_dir = self.entry_dir_for(key);
            if entry_dir.exists() {
                continue;
            }
            if downloading_guard.contains_key(key) {
                continue;
            }
            // Explicit prefetch candidates are treated as authoritative. Negative
            // key-cache knowledge is only used during discovery paths, not to veto
            // planner- or caller-supplied keys here.
            keys_to_fetch.push((key.clone(), crate_name.clone(), entry_dir));
        }
        drop(downloading_guard);

        // If empty keys were sent, fetch all S3 keys missing locally
        if req.keys.is_empty()
            && let Ok(backend) = self.get_remote_backend().await
            && let Ok(s3_keys) = crate::remote_plan::RemotePlanner::new(&self.config)
                .plan(crate::remote_plan::RemoteWorkload::KeyDiscovery)
                .layout(backend.as_ref(), remote)
                .list_keys()
                .await
        {
            for (key, crate_name) in s3_keys {
                if !crate::cache_key::is_valid_cache_key(&key)
                    || !crate::cache_key::is_valid_crate_name(&crate_name)
                {
                    tracing::warn!(
                        key = key_prefix(&key),
                        "prefetch: skipping listing key with invalid cache_key/crate_name"
                    );
                    continue;
                }
                let entry_dir = self.entry_dir_for(&key);
                if !entry_dir.exists() {
                    keys_to_fetch.push((key, crate_name, entry_dir));
                }
            }
        }

        let count = keys_to_fetch.len();
        if count == 0 {
            tracing::info!("prefetch: nothing to fetch");
            return Response::ok();
        }

        // Mark keys as downloading. Skip keys already claimed since the filter
        // above (e.g. by an interactive RemoteCheck): never replace a live
        // claim's Notify, or its waiters would wait on an orphaned handle.
        {
            let mut guard = self.downloading.write().await;
            for (key, _, _) in &keys_to_fetch {
                guard
                    .entry(key.clone())
                    .or_insert_with(|| Arc::new(Notify::new()));
            }
        }

        // Spawn a single coordinator task with bounded concurrency
        let daemon = Arc::clone(self);
        let bucket = remote.bucket.clone();
        let prefix = remote.prefix.clone();
        let remote_endpoint = remote.endpoint.clone();
        let remote_region = remote.region.clone();
        let remote_profile = remote.profile.clone();
        let cancel_rx = self.prefetch_cancel.subscribe();
        tokio::spawn(async move {
            let mut in_flight = futures::stream::FuturesUnordered::new();
            // Speculative prefetch is capped BELOW the S3 permit pool so an
            // interactive RemoteCheck can always acquire a permit without
            // queueing behind a wall of prefetch downloads (#485 Phase 0).
            // A fixed cap (not an available_permits snapshot, which raced
            // whatever happened to be free at spawn time): total minus a
            // reserve of 1/4 of the pool, at least 1, at most 4. With the
            // default 16 permits prefetch uses at most 12, leaving 4 for
            // on-demand traffic; a 1-permit pool degrades to no reservation.
            let max_concurrent = prefetch_concurrency_cap(daemon.config.s3_concurrency);

            let mut keys_iter = keys_to_fetch.into_iter().peekable();
            while let Some((key, crate_name, entry_dir)) = keys_iter.next() {
                // Check for adaptive cancellation
                if *cancel_rx.borrow() {
                    tracing::info!("prefetch: cancelled by adaptive hit-rate check");
                    // Remove this key + all remaining keys from the
                    // downloading map, then wake any waiters parked on them —
                    // AFTER the removals, so a woken waiter re-checking the
                    // map sees the keys gone and its re-claim can win.
                    let mut guard = daemon.downloading.write().await;
                    let mut drained: Vec<Arc<Notify>> = Vec::new();
                    drained.extend(guard.remove(&key));
                    let mut cancelled: u64 = 1;
                    for (k, _, _) in keys_iter {
                        drained.extend(guard.remove(&k));
                        cancelled += 1;
                    }
                    drop(guard);
                    for notify in drained {
                        notify.notify_waiters();
                    }
                    daemon
                        .prefetch_stats
                        .keys_cancelled
                        .fetch_add(cancelled, Ordering::Relaxed);
                    break;
                }

                // If we're at max concurrency, wait for one to complete
                while in_flight.len() >= max_concurrent {
                    use futures::StreamExt;
                    in_flight.next().await;
                }

                let sem = daemon.s3_semaphore.clone();
                let d = daemon.clone();
                let b = bucket.clone();
                let p = prefix.clone();
                let endpoint = remote_endpoint.clone();
                let region = remote_region.clone();
                let profile = remote_profile.clone();
                let download_plan = crate::remote_plan::RemotePlanner::new(&d.config)
                    .plan(crate::remote_plan::RemoteWorkload::Prefetch);
                in_flight.push(tokio::spawn(async move {
                    // Released on every exit path below (including panic) by Drop.
                    let _dl_guard = DownloadingGuard::new(d.downloading.clone(), key.clone());
                    // Daemon-wide speculative gate FIRST, then the shared S3
                    // permit: bounds prefetch across ALL coordinators so the
                    // interactive reserve holds even when startup prefetch
                    // overlaps a BuildStarted plan (#485, cross-family review).
                    let Ok(_gate) = d.prefetch_gate.clone().acquire_owned().await else {
                        tracing::warn!("prefetch: gate closed for {}", key);
                        return;
                    };
                    let semaphore_start = Instant::now();
                    let Ok(_permit) = sem.acquire().await else {
                        tracing::warn!("prefetch: semaphore closed for {}", key);
                        return;
                    };
                    let semaphore_wait_ms = semaphore_start.elapsed().as_millis() as u64;
                    let backend = match d.get_remote_backend().await {
                        Ok(b) => b,
                        Err(_) => {
                            return;
                        }
                    };
                    let blobs_dir = d.config.store_dir().join("blobs");
                    let start = Instant::now();
                    let remote_cfg = crate::config::RemoteConfig {
                        bucket: b,
                        endpoint,
                        region,
                        prefix: p,
                        profile,
                    };
                    let download_result = download_plan
                        .layout(backend.as_ref(), &remote_cfg)
                        .download_entry(&key, &crate_name, &entry_dir, &blobs_dir)
                        .await;

                    match download_result {
                        Ok(dl) => {
                            let elapsed_ms = start.elapsed().as_millis() as u64;
                            let import_start = Instant::now();
                            let import_ms = if let Err(e) =
                                d.with_store(|store| store.import_restored_entry(&key))
                            {
                                tracing::warn!("prefetch import failed for {}: {e}", key);
                                0
                            } else {
                                import_start.elapsed().as_millis() as u64
                            };
                            d.transfer_counters
                                .downloads_completed
                                .fetch_add(1, Ordering::Relaxed);
                            d.transfer_counters
                                .bytes_downloaded
                                .fetch_add(dl.compressed_bytes, Ordering::Relaxed);
                            // Phase-0 telemetry: the prefetch-attributed subset
                            // of the transfer counters above.
                            d.prefetch_stats
                                .downloads_completed
                                .fetch_add(1, Ordering::Relaxed);
                            d.prefetch_stats
                                .bytes_downloaded
                                .fetch_add(dl.compressed_bytes, Ordering::Relaxed);
                            d.push_transfer_event(TransferEvent {
                                schema: default_transfer_schema(),
                                crate_name: crate_name.clone(),
                                direction: TransferDirection::Download,
                                format: dl.format.to_string(),
                                cache_key: key.clone(),
                                object_key: dl.object_key,
                                compressed_bytes: dl.compressed_bytes,
                                elapsed_ms,
                                network_ms: dl.network_ms,
                                semaphore_wait_ms,
                                head_ms: 0,
                                request_ms: dl.request_ms,
                                body_ms: dl.body_ms,
                                request_count: dl.request_count,
                                original_bytes: dl.original_bytes,
                                decompress_ms: dl.decompress_ms,
                                extract_ms: dl.extract_ms,
                                disk_io_ms: dl.disk_io_ms,
                                import_ms,
                                compression_ms: 0,
                                head_checks_ms: 0,
                                blobs_skipped: dl.blobs_skipped,
                                blobs_total: dl.blobs_total,
                                ok: true,
                                timestamp: std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs(),
                            });
                            // Track as prefetched for PrefetchHit attribution.
                            // Bound the set: a long-lived daemon that
                            // prefetches many distinct keys would otherwise
                            // grow it without limit. The attribution memory
                            // is purely cosmetic (PrefetchHit vs LocalHit
                            // event labelling), so clearing on overflow is
                            // harmless.
                            {
                                const MAX_PREFETCHED_KEYS: usize = 50_000;
                                let mut pf = d.prefetched_keys.write().await;
                                if pf.len() >= MAX_PREFETCHED_KEYS {
                                    pf.clear();
                                    // Keep the used-key set consistent with the
                                    // attribution set it mirrors (the counter
                                    // keeps its lifetime total).
                                    d.prefetch_used_keys.write().await.clear();
                                }
                                pf.insert(key.clone());
                            }
                        }
                        Err(e) => {
                            let elapsed_ms = start.elapsed().as_millis() as u64;
                            d.transfer_counters
                                .downloads_failed
                                .fetch_add(1, Ordering::Relaxed);
                            d.push_transfer_event(TransferEvent {
                                schema: default_transfer_schema(),
                                crate_name: crate_name.clone(),
                                direction: TransferDirection::Download,
                                format: download_plan.transfer_format().to_string(),
                                cache_key: key.clone(),
                                object_key: String::new(),
                                compressed_bytes: 0,
                                elapsed_ms,
                                network_ms: 0,
                                semaphore_wait_ms,
                                head_ms: 0,
                                request_ms: 0,
                                body_ms: 0,
                                request_count: 0,
                                original_bytes: 0,
                                decompress_ms: 0,
                                extract_ms: 0,
                                disk_io_ms: 0,
                                import_ms: 0,
                                compression_ms: 0,
                                head_checks_ms: 0,
                                blobs_skipped: 0,
                                blobs_total: 0,
                                ok: false,
                                timestamp: std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs(),
                            });
                            tracing::warn!("prefetch download failed for {}: {e}", key);
                        }
                    }
                }));
            }

            // Drain remaining
            use futures::StreamExt;
            while in_flight.next().await.is_some() {}
            tracing::info!("prefetch: completed {} downloads", count);
        });

        tracing::info!("prefetch: queued {} downloads", count);
        Response::ok()
    }

    /// Handle a build-started hint by asking the advisory remote planner first,
    /// then falling back to the in-process planner that matches the daemon's
    /// current shard/history/key-cache heuristics.
    pub async fn handle_build_started(self: &Arc<Self>, req: &BuildStartedRequest) -> Response {
        let Some(_remote) = &self.config.remote else {
            return Response::err("no remote configured");
        };

        match crate::planner_client::resolve_prefetch_plan(&req.intent).await {
            Ok(Some(plan)) => {
                let plan_id = plan.plan_id.clone();
                let planner = plan.planner.clone();
                match plan.disposition {
                    PrefetchDisposition::Execute if plan.candidates.is_empty() => {
                        tracing::warn!(
                            plan_id = ?plan_id,
                            planner = ?planner,
                            "build-started: planner returned execute with no candidates, falling back to local planning"
                        );
                    }
                    PrefetchDisposition::Execute => {
                        let prefetch_req = PrefetchRequest::from_plan(plan);
                        let resp = self.handle_prefetch(&prefetch_req).await;
                        if resp.ok {
                            self.prefetch_stats
                                .plans_advisory
                                .fetch_add(1, Ordering::Relaxed);
                            self.prefetch_stats
                                .last_plan_candidates
                                .store(prefetch_req.keys.len() as u64, Ordering::Relaxed);
                            tracing::info!(
                                plan_id = ?plan_id,
                                planner = ?planner,
                                candidate_count = prefetch_req.keys.len(),
                                "build-started: using advisory planner plan"
                            );
                            return resp;
                        }
                        tracing::warn!(
                            plan_id = ?plan_id,
                            planner = ?planner,
                            "build-started: planner plan execution failed, falling back to local planning"
                        );
                    }
                    PrefetchDisposition::UseFallback => {
                        tracing::debug!(
                            plan_id = ?plan_id,
                            planner = ?planner,
                            "build-started: planner requested fallback to local planning"
                        );
                    }
                    PrefetchDisposition::DoNothing => {
                        tracing::info!(
                            plan_id = ?plan_id,
                            planner = ?planner,
                            "build-started: planner explicitly requested no prefetch"
                        );
                        return Response::ok();
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(
                    "build-started: planner lookup failed, falling back to local planning: {e}"
                );
            }
        }

        let fallback_plan =
            match crate::fallback_planner::build_prefetch_plan(self, &req.intent).await {
                Ok(plan) => plan,
                Err(e) => return Response::err(format!("fallback planning failed: {e}")),
            };

        if fallback_plan.candidates.is_empty() {
            tracing::debug!(
                "build-started: nothing to prefetch ({} crate names checked)",
                req.intent.crate_names.len()
            );
            return Response::ok();
        }

        tracing::info!(
            "build-started: using fallback planner with {} candidates for {} crates",
            fallback_plan.candidates.len(),
            req.intent.crate_names.len()
        );
        self.prefetch_stats
            .plans_fallback
            .fetch_add(1, Ordering::Relaxed);
        self.prefetch_stats
            .last_plan_candidates
            .store(fallback_plan.candidates.len() as u64, Ordering::Relaxed);

        let prefetch_req = PrefetchRequest::from_plan(fallback_plan);
        self.handle_prefetch(&prefetch_req).await
    }

    /// After a successful upload, check if store exceeds max_size → LRU eviction.
    fn maybe_evict_after_upload(&self) {
        let _ = self.with_store(|store| {
            let _gc_lock = match store.try_gc_lock()? {
                Some(lock) => lock,
                None => {
                    tracing::debug!(
                        "gc.lock held by another GC; skipping upload-triggered eviction"
                    );
                    return Ok(());
                }
            };
            let size = store.total_size()?;
            if size > self.config.max_size {
                tracing::info!(
                    "store size {} > max {}, running LRU eviction",
                    size,
                    self.config.max_size
                );
                let _ = store.evict();
            }
            Ok(())
        });
    }

    /// Core GC logic: evict entries, clean stale tool-version caches, and clean registered incremental dirs.
    /// Returns aggregated GcStats and persists them to `gc_stats.json` in the cache dir.
    pub fn run_gc(&self, max_age_hours: Option<u64>) -> Result<crate::store::GcStats> {
        let start = Instant::now();
        // Cross-process GC mutual exclusion (kunobi-ninja/kache#326): if another
        // GC driver (a manual `kache gc`, a second daemon) holds gc.lock, skip
        // this run rather than double-scan and contend. Held until run_gc returns.
        let _gc_lock = match self.with_store(|store| store.try_gc_lock())? {
            Some(lock) => lock,
            None => {
                tracing::info!("gc.lock held by another GC; skipping this run");
                return Ok(crate::store::GcStats {
                    skipped: true,
                    ..Default::default()
                });
            }
        };
        let (dedup_stats, evict_stats, incremental_cleaned, orphan_stats) =
            self.with_store(|store| {
                // Backfill content_hash for legacy entries
                let backfilled = store.backfill_content_hashes().unwrap_or(0);
                if backfilled > 0 {
                    tracing::info!("backfilled {backfilled} content hashes");
                }

                // Evict duplicate entries (same content, different cache keys)
                let dedup_stats = store.evict_duplicate_entries().unwrap_or_default();
                if dedup_stats.entries_evicted > 0 {
                    tracing::info!("evicted {} duplicate entries", dedup_stats.entries_evicted);
                }

                let evict_stats = if let Some(hours) = max_age_hours {
                    store.evict_older_than(hours)?
                } else {
                    store.evict()?
                };

                let incremental_cleaned = if self.config.clean_incremental {
                    store.clean_registered_incremental_dirs().unwrap_or(0)
                } else {
                    0
                };

                // Reclaim orphaned blob files (crash mid-put, or a meta-less
                // remove_entry that couldn't decrement refcounts). A 1h grace
                // leaves blobs a concurrent build is materializing untouched; they
                // get reclaimed on a later pass once settled.
                let orphan_stats = store
                    .sweep_orphan_blobs(std::time::Duration::from_secs(3600))
                    .unwrap_or_default();
                if orphan_stats.removed > 0 {
                    tracing::info!(
                        "swept {} of {} blobs as orphans ({} reclaimed)",
                        orphan_stats.removed,
                        orphan_stats.scanned,
                        crate::report::format_bytes(orphan_stats.bytes_reclaimed)
                    );
                }

                Ok((dedup_stats, evict_stats, incremental_cleaned, orphan_stats))
            })?;

        // Clean up stale tool-version cache files (rustc-ver-*.txt, linker-ver-*.txt).
        // Each toolchain update leaves behind orphaned files keyed by the old binary mtime.
        Self::clean_tool_version_caches(&self.config.cache_dir);

        if incremental_cleaned > 0 {
            tracing::info!("cleaned {incremental_cleaned} registered incremental dirs");
        }

        // Aggregate stats
        let stats = crate::store::GcStats {
            entries_evicted: dedup_stats.entries_evicted + evict_stats.entries_evicted,
            bytes_freed: dedup_stats.bytes_freed
                + evict_stats.bytes_freed
                + orphan_stats.bytes_reclaimed,
            blobs_removed: dedup_stats.blobs_removed
                + evict_stats.blobs_removed
                + orphan_stats.removed,
            duration_ms: start.elapsed().as_millis() as u64,
            skipped: false,
        };

        tracing::info!(
            "gc complete: {} entries evicted, {} freed, {} blobs removed in {}ms",
            stats.entries_evicted,
            crate::report::format_bytes(stats.bytes_freed),
            stats.blobs_removed,
            stats.duration_ms,
        );

        // Persist GC stats for report consumption
        let gc_stats_path = self.config.cache_dir.join("gc_stats.json");
        let persisted = crate::report::GcStatsPersisted {
            last_run: chrono::Utc::now().to_rfc3339(),
            entries_evicted: stats.entries_evicted,
            bytes_freed: stats.bytes_freed,
            blobs_removed: stats.blobs_removed,
            duration_ms: stats.duration_ms,
        };
        if let Ok(json) = serde_json::to_string_pretty(&persisted) {
            let _ = std::fs::write(&gc_stats_path, json);
        }

        Ok(stats)
    }

    /// Remove tool-version cache files older than 7 days.
    fn clean_tool_version_caches(cache_dir: &Path) {
        let cutoff = std::time::SystemTime::now() - std::time::Duration::from_secs(7 * 24 * 3600);

        let Ok(entries) = std::fs::read_dir(cache_dir) else {
            return;
        };

        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if (name.starts_with("rustc-ver-") || name.starts_with("linker-ver-"))
                && name.ends_with(".txt")
                && let Ok(meta) = entry.metadata()
                && let Ok(modified) = meta.modified()
                && modified < cutoff
            {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
}

// ── Server (thin I/O shell) ──────────────────────────────────────

/// Run the daemon server (foreground, blocking).
pub fn run_server(config: &Config) -> Result<()> {
    // Acquire an exclusive file lock to guarantee only one daemon process runs
    // at a time.  We use a dedicated "daemon.run.lock" (separate from the
    // "daemon.lock" that start_daemon_background uses to serialize *spawning*)
    // so the two never deadlock.
    //
    // The lock is held for the daemon's entire lifetime and is automatically
    // released when this function returns or the process exits/crashes.
    let socket_path = config.socket_path();
    let lock_path = socket_path.with_extension("run.lock");
    std::fs::create_dir_all(socket_path.parent().unwrap())?;

    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .context("opening daemon run lock file")?;

    // Cross-platform exclusive lock: flock(2) on Unix, LockFileEx on Windows.
    if lock_file.try_lock().is_err() {
        tracing::info!("another daemon holds the run lock, exiting");
        return Ok(());
    }

    // Hold lock_file (and thus the lock) for the daemon's entire lifetime.
    let _lock = lock_file;
    let coord = DaemonCoordFile::for_socket(&socket_path);
    coord
        .write_phase(DaemonPhase::Starting)
        .context("writing daemon coordinator state")?;
    let _coord_guard = DaemonCoordGuard::new(coord.path.clone());

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    rt.block_on(server_main(config, coord))
}

async fn server_main(config: &Config, coord: DaemonCoordFile) -> Result<()> {
    let socket_path = config.socket_path();
    std::fs::create_dir_all(socket_path.parent().unwrap())?;

    // Stale socket detection: try connecting — if it succeeds, another daemon is running.
    let probe_name = socket_name(&socket_path)?;
    match TokioStream::connect(probe_name).await {
        Ok(_) => {
            // Exit cleanly (code 0) so launchd/systemd KeepAlive doesn't
            // restart us in an infinite loop when the daemon is already up.
            tracing::info!("another daemon is already running (socket is active), exiting cleanly",);
            return Ok(());
        }
        Err(_) => {
            // No daemon listening — clean up stale socket file if it exists (Unix only).
            let _ = std::fs::remove_file(&socket_path);
        }
    }

    let bind_name = socket_name(&socket_path)?;
    let listener = ListenerOptions::new()
        .name(bind_name)
        .create_tokio()
        .context("binding local IPC socket")?;
    let _socket_guard = SocketCleanupGuard {
        path: socket_path.clone(),
    };
    coord
        .write_phase(DaemonPhase::Ready)
        .context("publishing daemon ready state")?;
    tracing::info!("daemon listening on {}", socket_path.display());

    // Exclude cache dir from Time Machine / Spotlight (once, not per-crate).
    #[cfg(target_os = "macos")]
    crate::store::exclude_from_indexing(&config.cache_dir);

    // Set up two-channel upload pipeline:
    //   handler → unbounded buffer → enqueue task → bounded worker channel → workers → S3
    let (buffer_tx, mut buffer_rx) = tokio::sync::mpsc::unbounded_channel::<UploadJob>();
    let num_workers = (config.s3_concurrency as usize).max(1);
    let (worker_tx, worker_rx) = tokio::sync::mpsc::channel::<UploadJob>(num_workers * 2);
    let worker_rx = Arc::new(tokio::sync::Mutex::new(worker_rx));

    let daemon_inner = Daemon::new(config.clone());
    daemon_inner.set_upload_tx(buffer_tx);
    let daemon = Arc::new(daemon_inner);

    // Enqueue task: drains the unbounded buffer into the bounded worker channel.
    // Backpressure: send().await blocks when workers are full.
    let enqueue_handle = tokio::spawn(async move {
        while let Some(job) = buffer_rx.recv().await {
            if worker_tx.send(job).await.is_err() {
                break;
            }
        }
    });

    // Spawn upload worker tasks
    let mut upload_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    for _ in 0..num_workers {
        let rx = worker_rx.clone();
        let d = daemon.clone();
        upload_handles.push(tokio::spawn(async move {
            while let Some(job) = rx.lock().await.recv().await {
                let Ok(_permit) = d.s3_semaphore.acquire().await else {
                    d.transfer_counters
                        .uploads_failed
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::error!("upload worker: semaphore closed, exiting");
                    break;
                };
                let resp = d.do_upload(&job).await;
                d.pending_uploads.write().await.remove(&job.key);
                if !resp.ok {
                    tracing::warn!(
                        "upload worker: {} failed: {}",
                        job.key,
                        resp.error.as_deref().unwrap_or("unknown")
                    );
                }
            }
        }));
    }
    tracing::info!("started {} upload workers", num_workers);

    // Periodic GC task: run immediately on startup, then every 6 hours
    let gc_daemon = daemon.clone();
    let gc_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(6 * 3600));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            tracing::info!("periodic GC sweep starting");
            // Offload the blocking sweep so it never stalls an async worker —
            // the accept loop and in-flight RemoteCheck stay responsive (#281).
            let gc = gc_daemon.clone();
            match tokio::task::spawn_blocking(move || gc.run_gc(None)).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => tracing::warn!("periodic GC failed: {e}"),
                Err(e) => tracing::warn!("periodic GC task panicked: {e}"),
            }
        }
    });

    // S3 key cache population task (if remote configured)
    let cache_handle = if config.remote.is_some() {
        let cache_daemon = daemon.clone();
        Some(tokio::spawn(async move {
            // Initial population with retry backoff
            let mut delay = std::time::Duration::from_secs(1);
            for attempt in 1..=5 {
                match populate_key_cache(&cache_daemon).await {
                    Ok(count) => {
                        tracing::info!("S3 key cache populated: {count} keys");
                        break;
                    }
                    Err(e) => {
                        tracing::warn!("S3 key cache population attempt {attempt}/5 failed: {e}");
                        if attempt < 5 {
                            tokio::time::sleep(delay).await;
                            delay *= 2;
                        }
                    }
                }
            }

            // Periodic refresh
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(KEY_CACHE_REFRESH_SECS));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            interval.tick().await; // skip immediate tick
            let mut consecutive_refresh_failures = 0u32;
            loop {
                interval.tick().await;
                match populate_key_cache(&cache_daemon).await {
                    Ok(count) => {
                        if consecutive_refresh_failures > 0 {
                            tracing::info!(
                                "S3 key cache refresh recovered after {consecutive_refresh_failures} failed attempt(s)"
                            );
                            consecutive_refresh_failures = 0;
                        }
                        tracing::debug!("S3 key cache refreshed: {count} keys");
                    }
                    Err(e) => {
                        consecutive_refresh_failures += 1;
                        if should_warn_key_cache_refresh_failure(consecutive_refresh_failures) {
                            tracing::warn!(
                                "S3 key cache refresh failed (attempt {consecutive_refresh_failures}): {e}"
                            );
                        } else {
                            tracing::debug!(
                                "S3 key cache refresh failed (attempt {consecutive_refresh_failures}): {e}"
                            );
                        }
                    }
                }
            }
        }))
    } else {
        None
    };

    // Manifest auto-prefetch: download manifest from S3 and prefetch expensive crates.
    // Runs once on startup — subsequent builds update the manifest via `kache save-manifest`.
    // On completion, signals the warming barrier so handle_remote_check can proceed.
    let manifest_handle = if config.remote.is_some() {
        let manifest_daemon = daemon.clone();
        Some(tokio::spawn(async move {
            manifest_prefetch(&manifest_daemon).await;
            manifest_daemon.signal_warming_complete();
        }))
    } else {
        // No remote configured — nothing to warm, unblock immediately
        daemon.signal_warming_complete();
        None
    };

    // Background blob migration: lazily migrate legacy entries on startup
    let migration_config = config.clone();
    tokio::spawn(async move {
        let result = tokio::task::spawn_blocking(move || {
            if let Ok(store) = Store::open(&migration_config) {
                store.migrate_to_blobs(|_, _| {})
            } else {
                Err(anyhow::anyhow!("failed to open store for migration"))
            }
        })
        .await;

        if let Ok(Ok(stats)) = result
            && stats.entries_migrated > 0
        {
            tracing::info!(
                "background migration: migrated {} entries",
                stats.entries_migrated,
            );
        }
    });

    // Shutdown flag: set by Shutdown request or OS signal
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let heartbeat_coord = coord.clone();
    let heartbeat_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(DAEMON_COORD_HEARTBEAT_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(e) = heartbeat_coord.write_phase(DaemonPhase::Ready) {
                tracing::debug!("daemon coordinator heartbeat failed: {e}");
            }
        }
    });

    // Explicit wakeup for the accept loop. A connection handler that sets
    // `shutdown_flag` (a protocol `stop`, or the client-epoch staleness path)
    // pokes this so the loop re-checks the flag immediately instead of waiting
    // out the periodic idle tick — see issue #288.
    let shutdown_notify = Arc::new(Notify::new());

    // Config watchdog: the daemon loads its config once at startup, so an edit
    // to the config file (e.g. `local_max_size`) would otherwise require a
    // manual `kache daemon stop`. Periodically re-fingerprint the active config
    // file; on a change, schedule a graceful restart so the service manager (or
    // the next build's auto-spawn) brings the daemon back up with the new
    // config. This watches only the file the daemon itself resolved — it sends
    // no per-client signal, so it can't thrash across projects.
    let config_fingerprint = crate::config::config_file_fingerprint();
    let config_watch_flag = Arc::clone(&shutdown_flag);
    let config_watch_notify = Arc::clone(&shutdown_notify);
    let config_watch_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(DAEMON_CONFIG_WATCH_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            interval.tick().await;
            if config_watch_flag.load(Ordering::Relaxed) {
                break;
            }
            if crate::config::config_file_fingerprint() != config_fingerprint {
                tracing::info!("config file changed on disk, scheduling restart to reload it");
                config_watch_flag.store(true, Ordering::Relaxed);
                config_watch_notify.notify_one();
                break;
            }
        }
    });

    // Idle watchdog: exit if no connections received for this duration.
    // Prevents zombie daemons from accumulating when the user isn't building.
    // The daemon will be auto-started again on the next build.
    // Configurable via KACHE_DAEMON_IDLE_TIMEOUT or config.toml; 0 = disabled.
    let idle_timeout = if config.daemon_idle_timeout_secs > 0 {
        Some(Duration::from_secs(config.daemon_idle_timeout_secs))
    } else {
        None
    };

    accept_loop(
        &listener,
        &daemon,
        &shutdown_flag,
        &shutdown_notify,
        idle_timeout,
        shutdown_signal(),
    )
    .await;

    gc_handle.abort();
    if let Some(h) = cache_handle {
        h.abort();
    }
    if let Some(h) = manifest_handle {
        h.abort();
    }
    heartbeat_handle.abort();
    config_watch_handle.abort();

    // Graceful shutdown: drop the daemon's sender to close the unbounded buffer,
    // which will cause the enqueue task to exit, closing the worker channel,
    // then wait for upload workers to drain (up to 30s) before aborting.
    daemon.close_upload_queue();
    drop(daemon);
    let _ = enqueue_handle.await;
    let drain_deadline = tokio::time::sleep(Duration::from_secs(30));
    tokio::pin!(drain_deadline);
    for h in &mut upload_handles {
        tokio::select! {
            _ = h => {}
            _ = &mut drain_deadline => {
                tracing::warn!("upload drain timeout, aborting remaining workers");
                break;
            }
        }
    }
    for h in upload_handles {
        h.abort();
    }

    // Socket file is cleaned up by `_socket_guard` (Drop).
    tracing::info!("daemon stopped");
    Ok(())
}

/// Periodic wake interval for the accept loop. The loop is otherwise only woken
/// by an incoming connection, an explicit `shutdown_notify`, or the OS shutdown
/// signal; this tick guarantees the idle-timeout check still runs when the
/// daemon is completely quiet.
const ACCEPT_LOOP_IDLE_TICK: Duration = Duration::from_secs(60);

/// Overall budget a `RemoteCheck` waits behind another task's in-flight
/// download of the same key before degrading to an unclaimed download.
const DOWNLOAD_JOIN_BUDGET: Duration = Duration::from_secs(30);

/// Atomically claim `key` for download in the `downloading` map.
///
/// Under a single write lock: if the key is absent, a fresh [`Notify`] is
/// inserted and `None` is returned — the caller is the LEADER and owns the
/// download (it must release the claim via [`DownloadingGuard`]). If the key
/// is already present, a clone of its `Notify` is returned — the caller is a
/// WAITER and should park on it until the leader's guard drops. Insert-if-
/// absent under one lock is what makes re-claiming after a failed leader
/// race-free: of N waiters retrying concurrently, exactly one sees the key
/// absent and becomes the new leader (#213).
async fn claim_download(
    downloading: &RwLock<HashMap<String, Arc<Notify>>>,
    key: &str,
) -> Option<Arc<Notify>> {
    use std::collections::hash_map::Entry;
    match downloading.write().await.entry(key.to_string()) {
        Entry::Occupied(e) => Some(e.get().clone()),
        Entry::Vacant(v) => {
            v.insert(Arc::new(Notify::new()));
            None
        }
    }
}

/// Releases a download claim when dropped: removes the key from the
/// `downloading` map and wakes every task parked on the key's [`Notify`], so
/// the claim is released on every exit path of a download — an early return,
/// the future being dropped, or a panic deep in the download/extract/import
/// stack (zstd/tar/blake3/sqlite). Without this, a panic between the claim
/// and the trailing remove would leave the key stuck, and every later
/// remote-check for it would block the full [`DOWNLOAD_JOIN_BUDGET`] until
/// the daemon restarts.
///
/// `Drop` cannot await, so removal has two paths: a `try_write` fast path
/// (the map is almost always uncontended at drop time), and a spawned async
/// removal when the lock is contended or the guard drops mid-unwind. On BOTH
/// paths waiters are notified only AFTER the key has been removed from the
/// map, so a woken waiter that re-checks the map is guaranteed to see the
/// key gone and its atomic re-claim can succeed.
struct DownloadingGuard {
    map: Arc<RwLock<HashMap<String, Arc<Notify>>>>,
    key: String,
}

impl DownloadingGuard {
    fn new(map: Arc<RwLock<HashMap<String, Arc<Notify>>>>, key: String) -> Self {
        Self { map, key }
    }
}

impl Drop for DownloadingGuard {
    fn drop(&mut self) {
        let key = std::mem::take(&mut self.key);
        // Fast path: the map is almost always uncontended at drop time.
        if let Ok(mut g) = self.map.try_write() {
            let notify = g.remove(&key);
            drop(g);
            // Notify only after the removal is visible (lock released).
            if let Some(notify) = notify {
                notify.notify_waiters();
            }
            return;
        }
        // Contended (or mid-unwind): hand the async removal to the runtime.
        let map = self.map.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let notify = map.write().await.remove(&key);
                if let Some(notify) = notify {
                    notify.notify_waiters();
                }
            });
        }
    }
}

/// Accept connections until a shutdown is requested.
///
/// Shutdown can arrive three ways: a protocol `stop` (or the client-epoch
/// staleness path) sets `shutdown_flag` from inside a connection handler, the
/// OS sends a termination signal (`shutdown_signal`), or the idle timeout
/// elapses. The flag-based paths run in spawned handler tasks, so the loop only
/// observes the flag at the top of an iteration — it must therefore be woken to
/// re-check it. `shutdown_notify` provides that wakeup: a handler calls
/// `notify_one()` right after setting the flag, and `notify_one` stores a permit
/// if the loop is not currently parked in `select!`, so the wakeup cannot be
/// lost even though the `Notified` future is recreated each iteration. Without
/// it a quiet `stop` would block until the next [`ACCEPT_LOOP_IDLE_TICK`]
/// (issue #288).
async fn accept_loop(
    listener: &TokioListener,
    daemon: &Arc<Daemon>,
    shutdown_flag: &Arc<AtomicBool>,
    shutdown_notify: &Arc<Notify>,
    idle_timeout: Option<Duration>,
    shutdown_signal: impl std::future::Future<Output = ()>,
) {
    tokio::pin!(shutdown_signal);
    let mut last_activity = Instant::now();

    // Bound the number of connection handlers doing work at once. Excess
    // connections park on `acquire_owned` (cheap) instead of all running
    // concurrently, so a burst of local clients can't pile up active handlers.
    const MAX_CONCURRENT_CONNECTIONS: usize = 128;
    let conn_limiter = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_CONNECTIONS));

    loop {
        if shutdown_flag.load(Ordering::Relaxed) {
            tracing::info!("shutdown requested via protocol, draining...");
            break;
        }

        // Check idle timeout
        if let Some(timeout) = idle_timeout
            && last_activity.elapsed() > timeout
        {
            tracing::info!("daemon idle for {:?}, shutting down", timeout);
            break;
        }

        tokio::select! {
            accept = listener.accept() => {
                // interprocess returns `Stream` directly (no peer address tuple)
                match accept {
                    Ok(stream) => {
                        last_activity = Instant::now();
                        let d = daemon.clone();
                        let flag = shutdown_flag.clone();
                        let notify = shutdown_notify.clone();
                        let limiter = conn_limiter.clone();
                        tokio::spawn(async move {
                            let _permit = limiter.acquire_owned().await.ok();
                            if let Err(e) = handle_connection(stream, &d, &flag, &notify).await {
                                // Downcast to check for client-disconnect I/O errors
                                // (broken pipe / connection reset) which are expected
                                // from fire-and-forget clients.
                                if e.downcast_ref::<std::io::Error>()
                                    .is_some_and(is_client_disconnect)
                                {
                                    tracing::debug!("connection handler: client disconnected: {e}");
                                } else {
                                    tracing::warn!("connection handler error: {e}");
                                }
                            }
                        });
                    }
                    Err(e) => {
                        tracing::warn!("accept error: {e}");
                    }
                }
            }
            // Explicit wakeup when a handler set `shutdown_flag`; the empty body
            // just bounces us back to the top-of-loop flag check, which breaks.
            _ = shutdown_notify.notified() => {}
            // Wake periodically to check idle timeout (select won't fire otherwise)
            _ = tokio::time::sleep(ACCEPT_LOOP_IDLE_TICK) => {}
            _ = &mut shutdown_signal => {
                tracing::info!("shutdown signal received, draining...");
                break;
            }
        }
    }
}

/// Populate the key cache by listing every key in the remote.
async fn populate_key_cache(daemon: &Daemon) -> Result<usize> {
    let backend = daemon.get_remote_backend().await?;
    let remote = daemon
        .config
        .remote
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no remote configured"))?;

    let list_start = Instant::now();
    let keys = crate::remote_plan::RemotePlanner::new(&daemon.config)
        .plan(crate::remote_plan::RemoteWorkload::KeyDiscovery)
        .layout(backend.as_ref(), remote)
        .list_keys()
        .await?;
    // Phase-0 telemetry (#485): the LIST cost the coordination service exists
    // to retire. Duration + key count of the most recent refresh.
    daemon
        .prefetch_stats
        .last_list_duration_ms
        .store(list_start.elapsed().as_millis() as u64, Ordering::Relaxed);
    daemon
        .prefetch_stats
        .last_list_key_count
        .store(keys.len() as u64, Ordering::Relaxed);
    let count = keys.len();
    daemon.key_cache.populate(keys).await;
    Ok(count)
}

/// Download the build manifest from S3 and prefetch expensive crates.
/// Runs once on daemon startup — filters by cost-benefit (skip cheap crates).
///
/// If `KACHE_NAMESPACE` is set and Cargo.lock is available, uses shard-based prefetch:
/// computes shard hashes from Cargo.lock deps, downloads matching shards in parallel,
/// and collects cache keys from them. Otherwise falls back to the monolithic build manifest.
async fn manifest_prefetch(daemon: &Arc<Daemon>) {
    let Some(remote) = &daemon.config.remote else {
        return;
    };

    let backend = match daemon.get_remote_backend().await {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!("manifest prefetch: remote backend init failed: {e}");
            return;
        }
    };

    // Try shard-based prefetch first if namespace is available
    if let Ok(namespace) = std::env::var("KACHE_NAMESPACE") {
        let lock_path = std::path::Path::new("Cargo.lock");
        if lock_path.exists() {
            match shard_prefetch(daemon, backend, &remote.prefix, &namespace, lock_path).await {
                Ok(n) => {
                    tracing::info!("shard prefetch: queued {n} keys from shards");
                    return;
                }
                Err(e) => {
                    tracing::warn!(
                        "shard prefetch failed, falling back to monolithic build manifest: {e}"
                    );
                }
            }
        } else {
            tracing::info!(
                "KACHE_NAMESPACE set but no Cargo.lock found, falling back to monolithic build manifest"
            );
        }
    }

    monolithic_manifest_prefetch(daemon, backend.as_ref(), remote).await;
}

/// Shard-based prefetch: compute shard hashes from Cargo.lock, download matching shards
/// from the remote in parallel, collect cache keys.
async fn shard_prefetch(
    daemon: &Arc<Daemon>,
    backend: &Arc<dyn crate::remote_backend::RemoteBackend>,
    prefix: &str,
    namespace: &str,
    lock_path: &std::path::Path,
) -> anyhow::Result<usize> {
    let deps = crate::shards::parse_cargo_lock(lock_path)?;
    shard_prefetch_for_deps(daemon, backend, prefix, namespace, &deps).await
}

async fn shard_prefetch_for_deps(
    daemon: &Arc<Daemon>,
    backend: &Arc<dyn crate::remote_backend::RemoteBackend>,
    prefix: &str,
    namespace: &str,
    deps: &[(String, String)],
) -> anyhow::Result<usize> {
    let shard_set = crate::shards::compute_shards(namespace, deps);

    tracing::info!(
        "shard prefetch: {} deps -> {} shards for namespace '{namespace}'",
        deps.len(),
        shard_set.shards.len()
    );

    // Download all shards in parallel
    let mut handles = Vec::new();
    for (hash, _entries) in &shard_set.shards {
        let b = Arc::clone(backend);
        let p = prefix.to_string();
        let ns = namespace.to_string();
        let h = hash.clone();
        handles.push(tokio::spawn(async move {
            crate::remote::download_shard(b.as_ref(), &p, &ns, &h).await
        }));
    }

    // Collect all cache keys from downloaded shards
    let mut prefetch_keys: Vec<(String, String)> = Vec::new();
    let mut shards_matched = 0usize;
    for handle in handles {
        match handle.await {
            Ok(Ok(Some(shard))) => {
                shards_matched += 1;
                for entry in shard.entries {
                    prefetch_keys.push((entry.cache_key, entry.crate_name));
                }
            }
            Ok(Ok(None)) => {} // shard not found in S3 — new deps, no cached artifacts yet
            Ok(Err(e)) => tracing::warn!("shard download error: {e}"),
            Err(e) => tracing::warn!("shard download task panicked: {e}"),
        }
    }

    tracing::info!(
        "shard prefetch: {shards_matched}/{} shards matched, {} keys to prefetch",
        shard_set.shards.len(),
        prefetch_keys.len()
    );

    if prefetch_keys.is_empty() {
        return Ok(0);
    }

    let count = prefetch_keys.len();
    let req = PrefetchRequest {
        keys: prefetch_keys,
    };
    let resp = daemon.handle_prefetch(&req).await;
    if !resp.ok {
        anyhow::bail!(
            "prefetch failed: {}",
            resp.error.as_deref().unwrap_or("unknown")
        );
    }
    Ok(count)
}

/// Monolithic build-manifest prefetch: download the manifest and filter by compile cost.
async fn monolithic_manifest_prefetch(
    daemon: &Arc<Daemon>,
    backend: &dyn crate::remote_backend::RemoteBackend,
    remote: &crate::config::RemoteConfig,
) {
    let manifest_key =
        std::env::var("KACHE_MANIFEST_KEY").unwrap_or_else(|_| crate::cli::default_manifest_key());

    let min_compile_ms: u64 = std::env::var("KACHE_MIN_COMPILE_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);

    let manifest = match crate::remote::download_manifest(backend, &remote.prefix, &manifest_key)
        .await
    {
        Ok(m) => m,
        Err(e) => {
            tracing::info!("manifest prefetch: no manifest for '{manifest_key}' ({e}), skipping");
            return;
        }
    };

    // Cost-benefit filter: skip crates cheaper to recompile than download
    let mut worth_prefetching: Vec<_> = manifest
        .entries
        .iter()
        .filter(|e| e.compile_time_ms >= min_compile_ms)
        .collect();

    // Most expensive crates first — maximizes value of limited S3 concurrency slots
    worth_prefetching.sort_by_key(|entry| std::cmp::Reverse(entry.compile_time_ms));

    let skipped = manifest.entries.len() - worth_prefetching.len();
    tracing::info!(
        "manifest prefetch: {} entries, prefetching {} (skipped {} cheap crates < {}ms)",
        manifest.entries.len(),
        worth_prefetching.len(),
        skipped,
        min_compile_ms
    );

    if worth_prefetching.is_empty() {
        return;
    }

    let prefetch_keys: Vec<(String, String)> = worth_prefetching
        .iter()
        .map(|e| (e.cache_key.clone(), e.crate_name.clone()))
        .collect();

    let req = PrefetchRequest {
        keys: prefetch_keys,
    };
    let resp = daemon.handle_prefetch(&req).await;
    if !resp.ok {
        tracing::warn!(
            "manifest prefetch failed: {}",
            resp.error.as_deref().unwrap_or("unknown")
        );
    }
}

/// Max bytes for a single request frame (one '\n'-terminated line). Requests
/// are small JSON objects; cap the buffer so a local client that streams bytes
/// without a newline can't drive the per-connection allocation arbitrarily high.
const MAX_REQUEST_FRAME_BYTES: usize = 8 * 1024 * 1024; // 8 MiB

/// Read one '\n'-terminated request frame, bounded to [`MAX_REQUEST_FRAME_BYTES`].
/// Mirrors `AsyncBufReadExt::lines()`: strips a trailing '\n'/'\r\n', returns
/// `Ok(None)` on clean EOF, and yields a final unterminated line — but rejects
/// (with `InvalidData`) a frame that grows past the cap instead of buffering it
/// without limit.
async fn read_bounded_line<R>(reader: &mut R, buf: &mut Vec<u8>) -> std::io::Result<Option<String>>
where
    R: AsyncBufRead + Unpin,
{
    buf.clear();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok((!buf.is_empty()).then(|| decode_request_frame(buf)));
        }
        if let Some(pos) = available.iter().position(|&b| b == b'\n') {
            buf.extend_from_slice(&available[..pos]);
            std::pin::Pin::new(&mut *reader).consume(pos + 1);
            return Ok(Some(decode_request_frame(buf)));
        }
        buf.extend_from_slice(available);
        let consumed = available.len();
        std::pin::Pin::new(&mut *reader).consume(consumed);
        if buf.len() > MAX_REQUEST_FRAME_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "request frame exceeds maximum size",
            ));
        }
    }
}

fn decode_request_frame(buf: &[u8]) -> String {
    let mut s = String::from_utf8_lossy(buf).into_owned();
    if s.ends_with('\r') {
        s.pop();
    }
    s
}

/// Run a blocking daemon handler on tokio's blocking thread pool so its
/// `std::fs` work and `Mutex<Store>` hold never stall an async worker thread —
/// which would otherwise back up the accept loop and every other connection's
/// `RemoteCheck` (#281). A handler panic is mapped to an error response rather
/// than tearing down the connection task.
async fn offload<F>(f: F) -> Response
where
    F: FnOnce() -> Response + Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(resp) => resp,
        Err(e) => Response::err(format!("daemon handler task failed: {e}")),
    }
}

async fn handle_connection(
    stream: TokioStream,
    daemon: &Arc<Daemon>,
    shutdown_flag: &AtomicBool,
    shutdown_notify: &Notify,
) -> Result<()> {
    // Use borrow pattern: &TokioStream implements both AsyncRead and AsyncWrite.
    // Do NOT use stream.split() — interprocess docs warn that "dropping a half
    // does not shut it down", which causes the reader to never see EOF and
    // hangs the server loop (and tarpaulin coverage runs).
    let mut reader = BufReader::new(&stream);
    let mut frame = Vec::new();

    loop {
        let line = match read_bounded_line(&mut reader, &mut frame).await {
            Ok(Some(l)) => l,
            Ok(None) => break,
            Err(e) if is_client_disconnect(&e) => {
                // Fire-and-forget client closed abruptly — not an error.
                tracing::debug!("client disconnected mid-read: {e}");
                break;
            }
            Err(e) => return Err(e.into()),
        };
        let start = Instant::now();
        let parsed = serde_json::from_str::<Request>(&line);

        // Extract client_epoch from fire-and-forget requests for staleness detection.
        let client_epoch = match &parsed {
            Ok(Request::Upload(job)) => job.client_epoch,
            Ok(Request::Stats(req)) => req.client_epoch,
            Ok(Request::BuildStarted(req)) => req.client_epoch,
            _ => 0,
        };

        let resp = match parsed {
            Ok(Request::Upload(ref job)) => {
                tracing::debug!(
                    crate_name = job.crate_name,
                    key = key_prefix(&job.key),
                    "handling upload request"
                );
                daemon.handle_upload(job).await
            }
            Ok(Request::Gc(req)) => {
                // Offload: a GC sweep is seconds of `std::fs` work holding the
                // store mutex — never run it on an async worker (#281).
                let d = Arc::clone(daemon);
                offload(move || d.handle_gc(&req)).await
            }
            Ok(Request::RemoteCheck(req)) => daemon.handle_remote_check(&req).await,
            Ok(Request::Stats(req)) => {
                let d = Arc::clone(daemon);
                offload(move || d.handle_stats(&req)).await
            }
            Ok(Request::BatchRemoteCheck(req)) => daemon.handle_batch_remote_check(&req).await,
            Ok(Request::HashFiles(req)) => {
                // Offload: full-file blake3 hashing is blocking I/O (#281).
                let d = Arc::clone(daemon);
                offload(move || d.handle_hash_files(&req)).await
            }
            Ok(Request::Prefetch(req)) => daemon.handle_prefetch(&req).await,
            Ok(Request::BuildStarted(req)) => daemon.handle_build_started(&req).await,
            Ok(Request::Shutdown) => {
                shutdown_flag.store(true, Ordering::Relaxed);
                // Wake the accept loop so it breaks now rather than on the next
                // periodic tick (issue #288).
                shutdown_notify.notify_one();
                Response::ok()
            }
            Err(e) => {
                tracing::warn!("invalid request from client: {e}");
                Response::err(format!("invalid request: {e}"))
            }
        };
        let elapsed = start.elapsed();

        // If the client binary is newer than this daemon, schedule a graceful restart.
        // The daemon finishes processing in-flight work, then exits so launchd/systemd
        // restarts it with the updated binary.
        if client_epoch_is_newer(client_epoch, daemon.build_epoch)
            && !shutdown_flag.load(Ordering::Relaxed)
        {
            tracing::info!(
                daemon_epoch = daemon.build_epoch,
                client_epoch,
                "client binary is newer than daemon, scheduling restart"
            );
            shutdown_flag.store(true, Ordering::Relaxed);
            // Wake the accept loop so the restart starts now (issue #288).
            shutdown_notify.notify_one();
        }

        if !resp.ok {
            tracing::warn!(
                elapsed_ms = elapsed.as_millis() as u64,
                error = resp.error.as_deref().unwrap_or("unknown"),
                "request failed"
            );
        }

        let mut resp_line = serde_json::to_string(&resp)?;
        resp_line.push('\n');
        if let Err(e) = (&stream).write_all(resp_line.as_bytes()).await {
            // Client closed without reading (fire-and-forget mode) — not an error.
            tracing::debug!("response write failed (client likely closed): {e}");
            break;
        }
    }

    Ok(())
}

/// Returns true for I/O errors that mean the client disconnected, so the
/// daemon can downgrade the log level instead of warning on every occurrence.
fn is_client_disconnect(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
    ) || e.raw_os_error() == Some(32) // EPIPE on macOS may report as ErrorKind::Other
}

/// First (up to) 16 bytes of a key for log display, never panicking on a
/// non-char-boundary. A legitimate wrapper sends 64-char ASCII hex, but a
/// crafted client on the local socket could send arbitrary bytes, and
/// `&key[..16]` would panic mid-multibyte-char and kill the connection task.
fn key_prefix(key: &str) -> &str {
    let mut end = key.len().min(16);
    while end > 0 && !key.is_char_boundary(end) {
        end -= 1;
    }
    &key[..end]
}

fn send_retry_delay(attempt: u32, pid: u32) -> Duration {
    let jitter = (u64::from(pid) * 7) % 50;
    Duration::from_millis(100 * u64::from(attempt) + jitter)
}

fn should_warn_key_cache_refresh_failure(consecutive_refresh_failures: u32) -> bool {
    consecutive_refresh_failures == 1 || consecutive_refresh_failures.is_multiple_of(10)
}

fn rotate_daemon_log_if_large(log_path: &Path) {
    if std::fs::metadata(log_path).is_ok_and(|m| m.len() > 2 * 1024 * 1024) {
        let _ = std::fs::write(log_path, b"--- log rotated ---\n");
    }
}

use crate::platform::wait_for_shutdown as shutdown_signal;

// ── Client ───────────────────────────────────────────────────────

/// Send an upload job to the daemon. Auto-starts daemon if needed.
/// Non-blocking: if daemon can't be reached, logs a warning and returns Ok.
///
/// Uses fire-and-forget: the request is written into the kernel socket buffer
/// and the connection is closed immediately — no waiting for a response.
/// This avoids the read-timeout failures that occur when the daemon's Tokio
/// runtime is saturated during S3 key-cache population at startup.
pub fn send_upload_job(
    config: &Config,
    key: &str,
    entry_dir: &Path,
    crate_name: &str,
) -> Result<()> {
    let socket_path = config.socket_path();

    let req = Request::Upload(UploadJob {
        key: key.to_string(),
        entry_dir: entry_dir.to_string_lossy().into_owned(),
        crate_name: crate_name.to_string(),
        client_epoch: build_epoch(),
    });

    let key_short = key_prefix(key);

    let try_send = |path: &Path| -> Result<()> { send_request_fire_and_forget(path, &req) };

    match try_send(&socket_path) {
        Ok(()) => return Ok(()),
        Err(first_err) => {
            tracing::debug!(
                crate_name,
                key = key_short,
                "initial upload send failed, starting daemon: {first_err:#}",
            );
            // Daemon unreachable — try auto-starting it.
            // Swallow errors: never fail the build over daemon startup issues.
            match start_daemon_background() {
                Ok(true) => {}
                Ok(false) | Err(_) => {
                    tracing::warn!(
                        crate_name,
                        key = key_short,
                        "could not reach or start daemon, skipping upload"
                    );
                    return Ok(());
                }
            }
        }
    }

    // Daemon is (re)started — retry with backoff + jitter.
    // Only the connect() can fail now (daemon not yet listening); writes
    // always succeed once connected because the kernel buffers them.
    for attempt in 1..=3u32 {
        match try_send(&socket_path) {
            Ok(()) => return Ok(()),
            Err(e) => {
                if attempt < 3 {
                    let delay = send_retry_delay(attempt, std::process::id());
                    tracing::debug!(
                        crate_name,
                        key = key_short,
                        attempt,
                        "upload send retry {attempt}/3 failed, backoff {delay:?}: {e:#}",
                    );
                    std::thread::sleep(delay);
                } else {
                    tracing::warn!(
                        crate_name,
                        key = key_short,
                        socket = %socket_path.display(),
                        "upload send failed after {attempt} retries: {e:#}",
                    );
                }
            }
        }
    }
    Ok(()) // Non-blocking: don't fail the build
}

pub struct GcRequestOutcome {
    pub evicted: Option<usize>,
    pub skipped: bool,
}

/// Send a GC request to the daemon. Auto-starts daemon if needed.
pub fn send_gc_request(config: &Config, max_age_hours: Option<u64>) -> Result<GcRequestOutcome> {
    let socket_path = config.socket_path();

    let req = Request::Gc(GcRequest { max_age_hours });

    let try_send = |path: &Path| -> Result<Response> {
        let resp_str = send_request(path, &req)?;
        let resp: Response = serde_json::from_str(&resp_str)?;
        Ok(resp)
    };

    match try_send(&socket_path) {
        Ok(resp) => {
            if resp.ok {
                Ok(GcRequestOutcome {
                    evicted: resp.evicted,
                    skipped: resp.skipped,
                })
            } else {
                anyhow::bail!("daemon GC error: {}", resp.error.unwrap_or_default());
            }
        }
        Err(_) => {
            // Try auto-starting the daemon
            if start_daemon_background()? {
                let resp = try_send(&socket_path)?;
                if resp.ok {
                    Ok(GcRequestOutcome {
                        evicted: resp.evicted,
                        skipped: resp.skipped,
                    })
                } else {
                    anyhow::bail!("daemon GC error: {}", resp.error.unwrap_or_default());
                }
            } else {
                anyhow::bail!("could not reach or start daemon");
            }
        }
    }
}

/// Send a remote check request to the daemon.
/// Returns `Some(true)` if downloaded, `Some(false)` if not in S3, `None` if daemon unreachable.
/// Does NOT auto-start daemon — builds should never break if daemon is down.
/// Result of a remote check: whether the artifact was found and if it came from prefetch.
pub struct RemoteCheckResult {
    pub found: bool,
    pub prefetched: bool,
}

fn remote_check_result_from_response_line(resp_str: &str) -> Option<RemoteCheckResult> {
    match serde_json::from_str::<Response>(resp_str) {
        Ok(resp) if resp.ok => resp.found.map(|found| RemoteCheckResult {
            found,
            prefetched: resp.prefetched.unwrap_or(false),
        }),
        Ok(resp) => {
            tracing::warn!(
                "remote check error: {}",
                resp.error.as_deref().unwrap_or("unknown")
            );
            None
        }
        Err(e) => {
            tracing::warn!("remote check response parse error: {e}");
            None
        }
    }
}

pub fn send_remote_check(
    config: &Config,
    key: &str,
    entry_dir: &Path,
    crate_name: &str,
) -> Option<RemoteCheckResult> {
    let socket_path = config.socket_path();

    // Fast path: if the daemon is not reachable, skip the full request.
    // On Unix this checks if the socket file exists and accepts connections.
    // On Windows (named pipes), this attempts a quick connect probe.
    if !crate::transport::is_reachable(&socket_path) {
        return None;
    }

    let req = Request::RemoteCheck(RemoteCheckRequest {
        key: key.to_string(),
        entry_dir: entry_dir.to_string_lossy().into_owned(),
        crate_name: crate_name.to_string(),
    });

    match send_request_with_timeout(&socket_path, &req, std::time::Duration::from_secs(3)) {
        Ok(resp_str) => remote_check_result_from_response_line(&resp_str),
        Err(e) => {
            tracing::debug!("remote check: daemon unreachable ({e})");
            None
        }
    }
}

pub fn send_hash_files_request(
    socket_path: &Path,
    files: Vec<HashFileRequest>,
) -> Result<Vec<HashFileResult>> {
    if files.is_empty() {
        return Ok(Vec::new());
    }
    if !socket_path.exists() {
        anyhow::bail!("daemon socket does not exist: {}", socket_path.display());
    }

    let req = Request::HashFiles(HashFilesRequest { files });
    let resp_str = send_request_with_timeout(socket_path, &req, std::time::Duration::from_secs(3))?;
    hash_files_results_from_response_line(&resp_str)
}

fn hash_files_results_from_response_line(resp_str: &str) -> Result<Vec<HashFileResult>> {
    let resp: Response = serde_json::from_str(resp_str)?;
    if !resp.ok {
        anyhow::bail!(
            "daemon hash_files error: {}",
            resp.error.unwrap_or_default()
        );
    }
    Ok(resp.hash_results.unwrap_or_default())
}

/// Send a prefetch request to the daemon. Non-blocking — sends the hint and returns.
/// Auto-starts daemon if needed. Uses fire-and-forget (no response wait).
#[allow(dead_code)]
pub fn send_prefetch(config: &Config, keys: &[(String, String)]) -> Result<()> {
    let socket_path = config.socket_path();

    let req = Request::Prefetch(PrefetchRequest {
        keys: keys.to_vec(),
    });

    let try_send = |path: &Path| -> Result<()> { send_request_fire_and_forget(path, &req) };

    match try_send(&socket_path) {
        Ok(()) => return Ok(()),
        Err(_) => match start_daemon_background() {
            Ok(true) => {}
            Ok(false) | Err(_) => {
                tracing::warn!("could not reach or start daemon, skipping prefetch");
                return Ok(());
            }
        },
    }

    for attempt in 1..=3u32 {
        match try_send(&socket_path) {
            Ok(()) => return Ok(()),
            Err(e) => {
                if attempt < 3 {
                    std::thread::sleep(send_retry_delay(attempt, std::process::id()));
                } else {
                    tracing::warn!("prefetch send failed after {attempt} retries: {e}");
                }
            }
        }
    }
    Ok(()) // Non-blocking: don't fail
}

/// Send a build-started hint to the daemon. Non-blocking, fire-and-forget.
///
/// The request carries `client_epoch` (our binary mtime) so the daemon can
/// detect when it's running stale code and self-restart. This replaces the
/// previous stats-request-based version check, avoiding an extra round-trip
/// that was prone to timeouts during daemon startup.
pub fn send_build_started(config: &Config, req: BuildStartedRequest) {
    let socket_path = config.socket_path();
    let crate_count = req.intent.crate_names.len();

    let req = Request::BuildStarted(req);

    match send_request_fire_and_forget(&socket_path, &req) {
        Ok(()) => {
            tracing::debug!("build-started hint sent for {} crates", crate_count);
        }
        Err(e) => {
            tracing::debug!("build-started hint: daemon unreachable ({e}), skipping");
        }
    }
}

/// Send a stats request to the daemon. No auto-start — stats are best-effort.
/// Returns Err if daemon is unreachable.
pub fn send_stats_request(
    config: &Config,
    include_entries: bool,
    sort_by: Option<&str>,
    event_hours: Option<u64>,
) -> Result<StatsResponse> {
    let socket_path = config.socket_path();
    let client_epoch = build_epoch();

    let req = Request::Stats(StatsRequest {
        include_entries,
        sort_by: sort_by.map(String::from),
        event_hours,
        client_epoch,
    });

    let resp_str =
        send_request_with_timeout(&socket_path, &req, std::time::Duration::from_secs(5))?;
    let resp: Response = serde_json::from_str(&resp_str)?;

    let stats = if resp.ok {
        resp.stats
            .ok_or_else(|| anyhow::anyhow!("stats response missing payload"))?
    } else {
        anyhow::bail!("daemon stats error: {}", resp.error.unwrap_or_default())
    };

    if client_epoch_is_newer(client_epoch, stats.build_epoch) {
        tracing::info!(
            daemon_epoch = stats.build_epoch,
            client_epoch,
            "stale daemon detected via stats request, restarting"
        );
        if restart_daemon_for_stale_client(config)?
            && let Ok(fresh_resp_str) =
                send_request_with_timeout(&socket_path, &req, std::time::Duration::from_secs(3))
            && let Ok(fresh_resp) = serde_json::from_str::<Response>(&fresh_resp_str)
            && fresh_resp.ok
            && let Some(fresh_stats) = fresh_resp.stats
        {
            return Ok(fresh_stats);
        }
    }

    Ok(stats)
}

/// Send a shutdown request to the running daemon.
///
/// If the socket is unreachable (stale daemon) but the run lock is still held,
/// falls back to terminating the daemon process via its coordinator PID.
pub fn send_shutdown_request(config: &Config) -> Result<()> {
    let socket_path = config.socket_path();
    match send_request_with_timeout(&socket_path, &Request::Shutdown, Duration::from_secs(5)) {
        Ok(_) => {
            eprintln!("daemon stopped");
            Ok(())
        }
        Err(e) => {
            // Socket unreachable — try to recover via coordinator state.
            if let Some(state) = read_daemon_state(&socket_path)
                && process_is_alive(state.pid)
            {
                tracing::info!(
                    pid = state.pid,
                    "socket unreachable, terminating daemon process"
                );
                crate::platform::terminate_process(state.pid);
                if wait_for_run_lock_release(&socket_path, Duration::from_secs(3))? {
                    let _ = std::fs::remove_file(&socket_path);
                    eprintln!("daemon stopped (terminated stale process)");
                    return Ok(());
                }
                // Graceful termination didn't work, escalate to force kill.
                tracing::warn!(pid = state.pid, "daemon did not stop, force-killing");
                crate::platform::kill_process(state.pid);
                if wait_for_run_lock_release(&socket_path, Duration::from_secs(2))? {
                    let _ = std::fs::remove_file(&socket_path);
                    eprintln!("daemon stopped (killed stale process)");
                    return Ok(());
                }
            }
            Err(e).context("connecting to daemon socket")
        }
    }
}

/// Find PIDs of running `kache daemon run` processes via pgrep.
///
/// Returns only PIDs that are still alive at the moment of the check — stale
/// pgrep output is filtered out with a `kill -0` probe.
pub fn find_daemon_pids() -> Vec<u32> {
    let own_pid = std::process::id();

    #[cfg(unix)]
    {
        let output = match std::process::Command::new("pgrep")
            .args(["-f", "kache daemon run"])
            .output()
        {
            Ok(o) if o.status.success() => o,
            _ => return Vec::new(),
        };
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(|l| l.trim().parse::<u32>().ok())
            .filter(|&pid| pid != own_pid && process_is_alive(pid))
            .collect()
    }

    #[cfg(windows)]
    {
        // tasklist is available on all supported Windows versions.
        // /FI filters by image name, /FO CSV for parseable output, /NH skips header.
        // CSV format: "kache.exe","1234","Console","1","12,345 K"
        let output = match std::process::Command::new("tasklist")
            .args(["/FI", "IMAGENAME eq kache.exe", "/FO", "CSV", "/NH"])
            .output()
        {
            Ok(o) if o.status.success() => o,
            _ => return Vec::new(),
        };
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(|line| {
                let fields: Vec<&str> = line.split(',').collect();
                fields.get(1)?.trim_matches('"').parse::<u32>().ok()
            })
            .filter(|&pid| pid != own_pid && process_is_alive(pid))
            .collect()
    }
}

/// Nuclear recovery: kill any lingering `kache daemon run` processes, then
/// wipe stale coordination files (socket, lock files, state json).
///
/// Used as the fallback when a regular restart can't produce a reachable
/// daemon — typically because a zombie process still holds the run lock or
/// stale lockfiles survived an unclean shutdown.
pub fn force_recover(config: &Config) -> Result<()> {
    let socket_path = config.socket_path();
    let pids = find_daemon_pids();

    if !pids.is_empty() {
        tracing::info!(?pids, "killing lingering kache daemon processes");
        for &pid in &pids {
            crate::platform::terminate_process(pid);
        }
        // Give the graceful terminate a moment to land.
        std::thread::sleep(Duration::from_millis(500));
        for &pid in &pids {
            if process_is_alive(pid) {
                tracing::warn!(pid, "graceful terminate did not land, force-killing");
                crate::platform::kill_process(pid);
            }
        }
        // Allow the OS a moment to reap zombies and release locks.
        std::thread::sleep(Duration::from_millis(200));
    }

    // Remove stale coordination files. Once processes are gone, OS has
    // released their flocks; wiping these files starts the next daemon
    // with a clean slate.
    let _ = std::fs::remove_file(&socket_path);
    let _ = std::fs::remove_file(daemon_state_path(&socket_path));
    let _ = std::fs::remove_file(socket_path.with_extension("lock"));
    let _ = std::fs::remove_file(socket_path.with_extension("run.lock"));

    Ok(())
}

/// Explicit daemon restart for `kache daemon restart` and init recovery.
///
/// Three-tier recovery strategy:
/// 1. Prefer the platform service manager (launchd/systemd) when installed —
///    it owns the daemon lifecycle and `kickstart -k` cleans its own state.
/// 2. If that doesn't yield a reachable daemon, do `force_recover` — kill
///    lingering manually-spawned daemons and wipe stale coordination files
///    (covers the case where a process is alive outside the service manager's
///    knowledge).
/// 3. Finally, spawn a fresh daemon via `start_daemon_background`.
///
/// Returns `Ok(true)` if the daemon is reachable after restart.
pub fn restart(config: &Config) -> Result<bool> {
    let socket_path = config.socket_path();

    // Tier 1: service manager. Only trust it if the resulting daemon actually
    // responds to a real request AND no lingering daemon processes remain.
    // `launchctl kickstart -k` only controls the launchd-spawned process; a
    // manually-spawned zombie can still hold the socket, making the "restart"
    // a no-op that looks like success at the socket layer.
    match crate::service::kickstart() {
        Ok(true) => {
            eprintln!("restarting daemon via service manager...");
            if wait_for_socket_until(&socket_path, None, Duration::from_secs(10))? {
                let responsive = send_stats_request(config, false, None, None).is_ok();
                let pids = find_daemon_pids();
                if responsive && pids.len() <= 1 {
                    eprintln!("daemon restarted");
                    return Ok(true);
                }
                tracing::warn!(
                    responsive,
                    daemon_pids = ?pids,
                    "service kickstart reported success but daemon isn't healthy; attempting nuclear recovery"
                );
            } else {
                tracing::warn!(
                    "service kickstart completed but socket not ready; attempting nuclear recovery"
                );
            }
        }
        Ok(false) => {
            // No service installed — fall through to manual path.
        }
        Err(e) => {
            tracing::warn!("service kickstart failed: {e:#}; attempting nuclear recovery");
        }
    }

    // Tier 2: best-effort graceful shutdown then force cleanup
    let _ = send_shutdown_request(config);
    force_recover(config)?;

    // Tier 3: fresh spawn
    match start_daemon_background()? {
        true => {
            eprintln!("daemon restarted");
            Ok(true)
        }
        false => {
            eprintln!("daemon did not start within timeout");
            Ok(false)
        }
    }
}

/// Best-effort restart for stale-daemon detection from stats polling.
/// This path is intentionally outside build hot paths, so a short bounded wait
/// is acceptable to keep monitor/status output current.
fn restart_daemon_for_stale_client(config: &Config) -> Result<bool> {
    let socket_path = config.socket_path();

    let _ = send_request_with_timeout(&socket_path, &Request::Shutdown, Duration::from_secs(2));

    // Give the old daemon a brief chance to exit before spawning a fresh one.
    for _ in 0..4 {
        if !crate::transport::is_reachable(&socket_path) {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    start_daemon_background()
}

/// Send a request to the daemon, return the response line.
fn send_request(socket_path: &Path, req: &Request) -> Result<String> {
    send_request_with_timeout(socket_path, req, std::time::Duration::from_secs(30))
}

/// Send a request to the daemon with a configurable read timeout.
fn send_request_with_timeout(
    socket_path: &Path,
    req: &Request,
    read_timeout: std::time::Duration,
) -> Result<String> {
    #[cfg(windows)]
    {
        send_request_with_async_timeout(socket_path, req, read_timeout)
    }

    #[cfg(not(windows))]
    {
        send_request_with_socket_timeout(socket_path, req, read_timeout)
    }
}

#[cfg(not(windows))]
fn send_request_with_socket_timeout(
    socket_path: &Path,
    req: &Request,
    read_timeout: std::time::Duration,
) -> Result<String> {
    use crate::transport::SyncStream;
    use interprocess::local_socket::traits::Stream as _;
    use std::io::{BufRead, Write};

    let name = socket_name(socket_path)?;
    let mut stream = SyncStream::connect(name)
        .with_context(|| format!("connecting to daemon socket {}", socket_path.display()))?;

    // Best-effort timeouts: supported on Unix (UDS), not on Windows (named pipes).
    let _ = stream.set_recv_timeout(Some(read_timeout));
    let _ = stream.set_send_timeout(Some(std::time::Duration::from_secs(5)));

    let mut line = serde_json::to_string(req)?;
    line.push('\n');
    stream
        .write_all(line.as_bytes())
        .context("writing request to daemon")?;
    stream.flush().context("flushing request to daemon")?;

    let mut reader = std::io::BufReader::new(&stream);
    let mut resp = String::new();
    reader.read_line(&mut resp).with_context(|| {
        format!(
            "reading response from daemon (timeout {:?}, socket {})",
            read_timeout,
            socket_path.display()
        )
    })?;

    Ok(resp)
}

#[cfg(windows)]
fn send_request_with_async_timeout(
    socket_path: &Path,
    req: &Request,
    read_timeout: std::time::Duration,
) -> Result<String> {
    let mut line = serde_json::to_string(req)?;
    line.push('\n');

    if tokio::runtime::Handle::try_current().is_ok() {
        let socket_path = socket_path.to_path_buf();
        std::thread::spawn(move || {
            send_request_with_async_timeout_blocking(&socket_path, line, read_timeout)
        })
        .join()
        .map_err(|_| anyhow::anyhow!("daemon client timeout thread panicked"))?
    } else {
        send_request_with_async_timeout_blocking(socket_path, line, read_timeout)
    }
}

#[cfg(windows)]
fn send_request_with_async_timeout_blocking(
    socket_path: &Path,
    line: String,
    read_timeout: std::time::Duration,
) -> Result<String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .context("creating daemon client runtime")?;

    runtime.block_on(async {
        tokio::time::timeout(
            read_timeout,
            send_request_with_async_transport(socket_path, line, read_timeout),
        )
        .await
        .with_context(|| {
            format!(
                "daemon request timed out after {:?} (socket {})",
                read_timeout,
                socket_path.display()
            )
        })?
    })
}

#[cfg(windows)]
async fn send_request_with_async_transport(
    socket_path: &Path,
    line: String,
    read_timeout: std::time::Duration,
) -> Result<String> {
    let name = socket_name(socket_path)?;
    let mut stream = TokioStream::connect(name)
        .await
        .with_context(|| format!("connecting to daemon socket {}", socket_path.display()))?;

    stream
        .write_all(line.as_bytes())
        .await
        .context("writing request to daemon")?;
    stream.flush().await.context("flushing request to daemon")?;

    let mut reader = BufReader::new(stream);
    let mut resp = String::new();
    reader.read_line(&mut resp).await.with_context(|| {
        format!(
            "reading response from daemon (timeout {:?}, socket {})",
            read_timeout,
            socket_path.display()
        )
    })?;

    Ok(resp)
}

/// Send a request to the daemon without waiting for a response.
///
/// Used for fire-and-forget operations (upload, prefetch) where the client
/// doesn't need confirmation.  The request is written into the kernel's
/// socket buffer and the connection is closed immediately — the daemon reads
/// and processes it whenever the Tokio runtime gets around to it.
///
/// This avoids the read-timeout failures that occur when the daemon's runtime
/// is saturated (e.g. during S3 key-cache population at startup).
fn send_request_fire_and_forget(socket_path: &Path, req: &Request) -> Result<()> {
    use crate::transport::SyncStream;
    use interprocess::local_socket::traits::Stream as _;
    use std::io::Write;

    let name = socket_name(socket_path)?;
    let mut stream = SyncStream::connect(name)
        .with_context(|| format!("connecting to daemon socket {}", socket_path.display()))?;

    let _ = stream.set_send_timeout(Some(std::time::Duration::from_secs(5)));

    let mut line = serde_json::to_string(req)?;
    line.push('\n');
    stream
        .write_all(line.as_bytes())
        .context("writing request to daemon")?;
    stream.flush().context("flushing request to daemon")?;

    // Don't read a response — just close. The daemon will see EOF on the
    // read half after processing the line and silently skip the response write.
    Ok(())
}

/// Start the daemon in the background and wait for it to be ready.
///
/// Uses a file lock to ensure only one process spawns the daemon when
/// multiple rustc wrapper processes race to auto-start simultaneously.
/// Processes that lose the lock race simply wait for the socket to appear.
///
/// Returns `Ok(true)` if the daemon is accepting connections,
/// `Ok(false)` if the timeout elapsed.
pub fn start_daemon_background() -> Result<bool> {
    let config = Config::load()?;
    let socket_path = config.socket_path();
    let lock_path = socket_path.with_extension("lock");
    let mut recovered_once = false;

    for attempt in 0..2 {
        std::fs::create_dir_all(socket_path.parent().unwrap())?;

        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .context("opening daemon lock file")?;

        let got_lock = lock_file.try_lock().is_ok();

        if !got_lock {
            tracing::debug!("daemon start already in progress, waiting for socket");
            if wait_for_socket(&socket_path, None)? {
                if recovered_once {
                    tracing::info!(
                        socket = %socket_path.display(),
                        "daemon startup recovered after retry"
                    );
                }
                return Ok(true);
            }
            if attempt == 0 {
                tracing::info!(
                    socket = %socket_path.display(),
                    "daemon starter timed out without publishing a ready socket, retrying coordination"
                );
                std::thread::sleep(DAEMON_START_POLL_INTERVAL);
                continue;
            }
            return Ok(false);
        }

        // We hold the lock. Check if daemon is already running.
        if crate::transport::is_reachable(&socket_path) {
            let my_epoch = build_epoch();
            let is_stale = send_request_with_timeout(
                &socket_path,
                &Request::Stats(StatsRequest {
                    include_entries: false,
                    sort_by: None,
                    event_hours: None,
                    client_epoch: my_epoch,
                }),
                Duration::from_secs(2),
            )
            .ok()
            .and_then(|s| serde_json::from_str::<Response>(&s).ok())
            .and_then(|r| r.stats)
            .map(|s| client_epoch_is_newer(my_epoch, s.build_epoch))
            .unwrap_or(false);

            if !is_stale {
                tracing::debug!("daemon already running");
                return Ok(true);
            }

            tracing::info!("stale daemon detected, requesting shutdown before restart");
            let _ =
                send_request_with_timeout(&socket_path, &Request::Shutdown, Duration::from_secs(2));

            if !wait_for_run_lock_release(&socket_path, Duration::from_secs(5))? {
                tracing::info!(
                    socket = %socket_path.display(),
                    "stale daemon did not exit within timeout, attempting bounded recovery"
                );
                if attempt == 0
                    && recover_unhealthy_daemon(
                        &socket_path,
                        "stale daemon did not exit after shutdown request",
                    )?
                {
                    recovered_once = true;
                    continue;
                }
                return Ok(false);
            }
        }

        if daemon_run_lock_is_held(&socket_path)? {
            tracing::debug!(
                socket = %socket_path.display(),
                "daemon run lock already held, waiting for socket"
            );
            if wait_for_socket(&socket_path, None)? {
                return Ok(true);
            }
            if attempt == 0
                && recover_unhealthy_daemon(
                    &socket_path,
                    "daemon run lock held but no ready socket became reachable",
                )?
            {
                recovered_once = true;
                continue;
            }
            return Ok(false);
        }

        let exe = std::env::current_exe().context("getting current executable path")?;
        tracing::info!("auto-starting daemon");

        let log_path = socket_path.with_extension("log");
        rotate_daemon_log_if_large(&log_path);
        let stderr_target = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map(std::process::Stdio::from)
            .unwrap_or_else(|_| std::process::Stdio::null());

        let mut child = std::process::Command::new(exe)
            .args(["daemon", "run"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(stderr_target)
            .spawn()
            .context("spawning daemon process")?;

        let ready = wait_for_socket(&socket_path, Some(&mut child))?;
        if ready {
            if recovered_once {
                tracing::info!(
                    socket = %socket_path.display(),
                    "daemon started successfully after recovery"
                );
            } else {
                tracing::info!("daemon started successfully");
            }
            return Ok(true);
        }
        if attempt == 0
            && recover_unhealthy_daemon(
                &socket_path,
                "daemon starter failed to publish a ready socket before timeout",
            )?
        {
            recovered_once = true;
            continue;
        }
        return Ok(false);
    }

    Ok(false)
}

fn daemon_run_lock_is_held(socket_path: &Path) -> Result<bool> {
    let run_lock_path = socket_path.with_extension("run.lock");
    let run_lock_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&run_lock_path)
        .context("opening daemon run lock probe file")?;

    // Probe: if we can acquire the lock, no daemon is running. Releases
    // immediately on drop (or via explicit unlock below).
    if run_lock_file.try_lock().is_ok() {
        let _ = run_lock_file.unlock();
        Ok(false)
    } else {
        Ok(true)
    }
}

fn wait_for_socket(socket_path: &Path, child: Option<&mut std::process::Child>) -> Result<bool> {
    wait_for_socket_until(socket_path, child, DAEMON_START_TIMEOUT)
}

fn wait_for_socket_until(
    socket_path: &Path,
    mut child: Option<&mut std::process::Child>,
    timeout: Duration,
) -> Result<bool> {
    let deadline = Instant::now() + timeout;

    while Instant::now() < deadline {
        if crate::transport::is_reachable(socket_path) {
            return Ok(true);
        }

        if let Some(child_proc) = child.as_mut()
            && let Some(status) = child_proc
                .try_wait()
                .context("checking daemon process status")?
        {
            if status.success() {
                tracing::debug!(
                    socket = %socket_path.display(),
                    ?status,
                    "daemon starter exited cleanly before socket became ready, continuing to wait"
                );
                child = None;
                continue;
            }
            tracing::warn!(
                socket = %socket_path.display(),
                ?status,
                "daemon exited before socket became ready"
            );
            return Ok(false);
        }

        std::thread::sleep(DAEMON_START_POLL_INTERVAL);
    }

    if crate::transport::is_reachable(socket_path) {
        return Ok(true);
    }

    if let Some(child) = child.as_mut()
        && child
            .try_wait()
            .context("checking daemon process status after timeout")?
            .is_none()
    {
        tracing::debug!(
            socket = %socket_path.display(),
            timeout_ms = timeout.as_millis(),
            "daemon did not start within timeout, terminating starter process"
        );
        let _ = child.kill();
        let _ = child.wait();
    }

    tracing::warn!(
        socket = %socket_path.display(),
        timeout_ms = timeout.as_millis(),
        "daemon did not start within timeout"
    );
    Ok(false)
}

// ── Tests ────────────────────────────────────────────────────────

// Daemon tests run on every platform via the cross-platform `transport` layer
// (Unix domain sockets on Unix, named pipes on Windows). The handful of tests
// that exercise Unix-only semantics directly — socket *files* on disk, POSIX
// process termination via `sh` — are individually `#[cfg(unix)]`-gated below.
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    // Tests use the same cross-platform transport as production. On Unix
    // this resolves to UDS; on Windows (when tests are eventually enabled
    // there) it resolves to named pipes.
    use crate::transport::{ListenerOptions, TokioListener, TokioStream, socket_name};

    /// Bind a daemon-style listener at `path`, taking the cross-platform
    /// transport. Used by every roundtrip test to remove boilerplate.
    fn bind_listener(path: &Path) -> TokioListener {
        let name = socket_name(path).expect("socket name");
        ListenerOptions::new()
            .name(name)
            .create_tokio()
            .expect("create_tokio listener")
    }

    /// Client-side connect mirror of bind_listener.
    async fn connect_stream(path: &Path) -> TokioStream {
        let name = socket_name(path).expect("socket name");
        TokioStream::connect(name).await.expect("connect")
    }

    /// Bind a *synchronous* listener at `path` so `transport::is_reachable`
    /// reports the endpoint as live. Cross-platform (UDS file on Unix, named
    /// pipe on Windows) and, unlike the tokio listener, needs no async runtime
    /// — so it can be created inside a plain `std::thread`.
    fn bind_sync_listener(path: &Path) -> interprocess::local_socket::Listener {
        let name = socket_name(path).expect("socket name");
        ListenerOptions::new()
            .name(name)
            .create_sync()
            .expect("create_sync listener")
    }

    /// Spawn a child that exits immediately with success — a stand-in for a
    /// daemon-starter process that returns before the socket is ready.
    fn spawn_quick_exit_child() -> std::process::Child {
        #[cfg(unix)]
        {
            std::process::Command::new("sh")
                .args(["-c", "exit 0"])
                .spawn()
                .unwrap()
        }
        #[cfg(windows)]
        {
            std::process::Command::new("cmd")
                .args(["/c", "exit", "0"])
                .spawn()
                .unwrap()
        }
    }

    /// Spawn a child that blocks long enough (~30s) to be killed by the code
    /// under test. `sleep` on Unix; on Windows a 30-ping loop, since `timeout`
    /// needs a console and fails under captured stdio.
    fn spawn_blocking_child() -> std::process::Child {
        #[cfg(unix)]
        {
            std::process::Command::new("sh")
                .args(["-c", "sleep 30"])
                .spawn()
                .unwrap()
        }
        #[cfg(windows)]
        {
            std::process::Command::new("cmd")
                .args(["/c", "ping", "-n", "31", "127.0.0.1"])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .unwrap()
        }
    }

    /// Run one client request→response roundtrip against a daemon socket and
    /// return the parsed response.
    ///
    /// This mirrors the production client (`send_request_with_timeout`):
    /// connect, write the request line, read exactly one response line, then
    /// **drop the stream** so the server's read loop sees EOF and
    /// `handle_connection` returns.
    ///
    /// Tests must NOT instead half-close with `AsyncWriteExt::shutdown`: the
    /// `interprocess` tokio stream's `poll_shutdown` does not perform a
    /// `shutdown(SHUT_WR)` on macOS, so the server never sees EOF on its read
    /// half and the test hangs forever waiting on `server.await`. Dropping the
    /// whole stream closes both halves and behaves identically on every
    /// platform — which is also exactly what the real client does.
    async fn client_roundtrip(socket_path: &Path, req: &Request) -> Response {
        let mut stream = connect_stream(socket_path).await;

        let mut line = serde_json::to_string(req).expect("serialize request");
        line.push('\n');
        stream
            .write_all(line.as_bytes())
            .await
            .expect("write request");

        let mut resp_line = String::new();
        {
            let mut reader = BufReader::new(&stream);
            reader
                .read_line(&mut resp_line)
                .await
                .expect("read response");
        }
        drop(stream);

        serde_json::from_str(&resp_line).expect("parse response")
    }

    /// Bind a fresh daemon socket, serve exactly one connection with
    /// `handle_connection`, run a single client roundtrip against it, and
    /// join the server task. Returns the parsed response.
    ///
    /// Every socket integration test funnels through this so the
    /// connect/serve/teardown ordering lives in one place and the macOS EOF
    /// hang (see `client_roundtrip`) cannot be reintroduced piecemeal.
    async fn one_shot_request(daemon: &Arc<Daemon>, socket_path: &Path, req: &Request) -> Response {
        let listener = bind_listener(socket_path);

        let server_daemon = daemon.clone();
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept");
            handle_connection(
                stream,
                &server_daemon,
                &AtomicBool::new(false),
                &Notify::new(),
            )
            .await
            .expect("handle_connection");
        });

        let resp = client_roundtrip(socket_path, req).await;
        server.await.expect("join server task");
        resp
    }

    /// Regression for #288 (handler side): a protocol `stop` must both set
    /// `shutdown_flag` and leave a permit on `shutdown_notify`. The stored
    /// permit is what makes the accept loop's `notified()` arm fire even when
    /// the stop lands while the loop is not parked in `select!` — the
    /// lost-wakeup guarantee the fix depends on. Without the `notify_one()`
    /// call this test hangs on `notified()` and trips the timeout.
    #[tokio::test]
    async fn test_shutdown_request_sets_flag_and_stores_notify_permit() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let listener = bind_listener(&socket_path);
        let daemon = Arc::new(Daemon::new(config));
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_notify = Arc::new(Notify::new());

        let server_daemon = daemon.clone();
        let server_flag = shutdown_flag.clone();
        let server_notify = shutdown_notify.clone();
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept");
            handle_connection(stream, &server_daemon, &server_flag, &server_notify)
                .await
                .expect("handle_connection");
        });

        let resp = client_roundtrip(&socket_path, &Request::Shutdown).await;
        server.await.expect("join server task");

        assert!(resp.ok, "stop request should return ok");
        assert!(
            shutdown_flag.load(Ordering::Relaxed),
            "stop request must set the shutdown flag"
        );
        // A permit must already be stored, so `notified()` resolves immediately.
        tokio::time::timeout(Duration::from_secs(1), shutdown_notify.notified())
            .await
            .expect("stop request must leave a notify permit (issue #288)");
    }

    /// Regression for #288 (loop side): a quiet `stop` must wake the accept loop
    /// immediately rather than leaving it parked until the periodic idle tick.
    /// We drive the real `accept_loop` with the idle timeout disabled and an
    /// OS shutdown signal that never fires, so the *only* thing that can break
    /// the loop within the assertion window is the stop-request wakeup. Before
    /// the fix the loop would stay parked for `ACCEPT_LOOP_IDLE_TICK` (~60s) and
    /// this 5s timeout would elapse.
    #[tokio::test]
    async fn test_accept_loop_breaks_promptly_on_stop_request() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let listener = bind_listener(&socket_path);
        let daemon = Arc::new(Daemon::new(config));
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_notify = Arc::new(Notify::new());

        // Client sends a one-shot `stop` once the loop is up.
        let client_socket = socket_path.clone();
        let client =
            tokio::spawn(async move { client_roundtrip(&client_socket, &Request::Shutdown).await });

        // `accept_loop` borrows the listener, so run it in this task (not a
        // spawned 'static one) under a timeout. `future::pending` stands in for
        // an OS shutdown signal that never arrives.
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            accept_loop(
                &listener,
                &daemon,
                &shutdown_flag,
                &shutdown_notify,
                None,
                std::future::pending::<()>(),
            ),
        )
        .await;

        assert!(
            outcome.is_ok(),
            "accept_loop did not break within 5s of a stop request (issue #288 regression)"
        );
        assert!(
            shutdown_flag.load(Ordering::Relaxed),
            "shutdown flag should be set after the stop request"
        );
        let resp = client.await.expect("join client task");
        assert!(resp.ok, "stop request should return ok");
    }

    #[tokio::test]
    async fn test_send_request_with_timeout_bounds_unresponsive_daemon() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("daemon.sock");
        let listener = bind_listener(&socket_path);

        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept");
            let mut request_line = String::new();
            {
                let mut reader = BufReader::new(&stream);
                reader
                    .read_line(&mut request_line)
                    .await
                    .expect("read request");
            }
            assert!(request_line.contains("\"stats\""));
            tokio::time::sleep(Duration::from_secs(1)).await;
            drop(stream);
        });

        let req = Request::Stats(StatsRequest {
            include_entries: false,
            sort_by: None,
            event_hours: None,
            client_epoch: 0,
        });
        let client_socket_path = socket_path.clone();
        let started = Instant::now();
        let result = tokio::task::spawn_blocking(move || {
            send_request_with_timeout(&client_socket_path, &req, Duration::from_millis(75))
        })
        .await
        .expect("join client task");

        assert!(result.is_err());
        assert!(started.elapsed() < Duration::from_millis(750));
        server.abort();
    }

    fn hold_run_lock_for_test(
        socket_path: &Path,
        hold_for: Duration,
    ) -> std::thread::JoinHandle<()> {
        let run_lock_path = socket_path.with_extension("run.lock");
        let (tx, rx) = mpsc::channel();
        let handle = std::thread::spawn(move || {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&run_lock_path)
                .unwrap();
            file.lock().unwrap();
            tx.send(()).unwrap();
            std::thread::sleep(hold_for);
            let _ = file.unlock();
        });
        rx.recv().unwrap();
        handle
    }

    /// Helper: create a Config pointing at a tempdir.
    fn test_config(dir: &Path) -> Config {
        Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            remote_readonly: false,
            modified_input_guard: false,
            windows_hardlink: false,
            path_only_env_vars: Vec::new(),
            cache_dir: dir.to_path_buf(),
            max_size: 50 * 1024 * 1024, // 50 MiB
            remote: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: false,
            event_log_max_size: 10 * 1024 * 1024,
            event_log_keep_lines: 1000,
            compression_level: 3,
            s3_concurrency: 16,
            daemon_idle_timeout_secs: crate::config::DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: crate::config::DEFAULT_S3_POOL_IDLE_SECS,
        }
    }

    // ── Protocol serde round-trips ───────────────────────────────

    #[test]
    fn test_request_upload_serde() {
        let req = Request::Upload(UploadJob {
            key: "abc123".into(),
            entry_dir: "/tmp/store/abc123".into(),
            crate_name: String::new(),
            client_epoch: 0,
        });
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, parsed);

        // Verify wire format matches protocol spec
        assert!(json.contains("\"upload\""));
        assert!(json.contains("\"key\":\"abc123\""));
    }

    #[test]
    fn test_wait_for_socket_until_observes_late_socket() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("daemon.sock");
        let socket_path_bg = socket_path.clone();

        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(150));
            let listener = bind_sync_listener(&socket_path_bg);
            std::thread::sleep(Duration::from_millis(200));
            drop(listener);
        });

        let ready = wait_for_socket_until(&socket_path, None, Duration::from_secs(1)).unwrap();

        handle.join().unwrap();
        assert!(ready);
    }

    #[test]
    fn test_wait_for_socket_until_times_out_cleanly() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("missing.sock");

        let ready = wait_for_socket_until(&socket_path, None, Duration::from_millis(150)).unwrap();

        assert!(!ready);
    }

    #[test]
    fn test_wait_for_socket_until_ignores_clean_child_exit_if_socket_appears() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("daemon.sock");
        let socket_path_bg = socket_path.clone();

        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(150));
            let listener = bind_sync_listener(&socket_path_bg);
            std::thread::sleep(Duration::from_millis(200));
            drop(listener);
        });

        let mut child = spawn_quick_exit_child();

        let ready =
            wait_for_socket_until(&socket_path, Some(&mut child), Duration::from_secs(1)).unwrap();

        handle.join().unwrap();
        assert!(ready);
    }

    #[test]
    fn test_wait_for_socket_until_kills_stuck_child_after_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("missing.sock");
        let mut child = spawn_blocking_child();

        let ready =
            wait_for_socket_until(&socket_path, Some(&mut child), Duration::from_millis(150))
                .unwrap();

        assert!(!ready);
        let status = child.try_wait().unwrap();
        assert!(status.is_some());
    }

    #[test]
    fn decode_request_frame_strips_trailing_carriage_return() {
        assert_eq!(decode_request_frame(b"{\"x\":1}\r"), "{\"x\":1}");
        assert_eq!(decode_request_frame(b"{\"x\":1}"), "{\"x\":1}");
        assert_eq!(decode_request_frame(b""), "");
    }

    #[test]
    fn is_client_disconnect_matches_disconnect_kinds() {
        use std::io::{Error, ErrorKind};
        assert!(is_client_disconnect(&Error::from(ErrorKind::BrokenPipe)));
        assert!(is_client_disconnect(&Error::from(
            ErrorKind::ConnectionReset
        )));
        assert!(is_client_disconnect(&Error::from_raw_os_error(32))); // EPIPE
        assert!(!is_client_disconnect(&Error::from(ErrorKind::NotFound)));
        assert!(!is_client_disconnect(&Error::from(ErrorKind::TimedOut)));
    }

    #[test]
    fn key_prefix_is_multibyte_safe() {
        // 64-char ASCII hex: first 16 chars.
        let hex = "0123456789abcdef".repeat(4);
        assert_eq!(key_prefix(&hex), "0123456789abcdef");
        // Short keys pass through.
        assert_eq!(key_prefix("short"), "short");
        assert_eq!(key_prefix(""), "");
        // A multibyte char straddling byte 16 must not panic; the prefix backs
        // off to the previous char boundary.
        let s = "アアアアアアアア"; // 8 × 3-byte chars = 24 bytes
        let p = key_prefix(s);
        assert!(s.starts_with(p));
        assert!(p.len() <= 16);
    }

    #[test]
    fn client_epoch_comparison_ignores_zero_and_detects_newer() {
        // Branch: stale-daemon epoch predicate.
        assert!(!client_epoch_is_newer(0, 10));
        assert!(!client_epoch_is_newer(10, 0));
        assert!(!client_epoch_is_newer(10, 10));
        assert!(!client_epoch_is_newer(9, 10));
        assert!(client_epoch_is_newer(11, 10));
    }

    #[test]
    fn send_retry_delay_uses_linear_backoff_and_pid_jitter() {
        // Branch: retry backoff math. delay = 100*attempt + (pid*7)%50.
        // pid 7 -> (49)%50 = 49; pid 8 -> (56)%50 = 6.
        assert_eq!(send_retry_delay(1, 7), Duration::from_millis(100 + 49));
        assert_eq!(send_retry_delay(3, 8), Duration::from_millis(300 + 6));
    }

    #[test]
    fn key_cache_refresh_warning_cadence_is_first_and_every_tenth() {
        // Branch: refresh-failure warning cadence.
        assert!(should_warn_key_cache_refresh_failure(1));
        assert!(!should_warn_key_cache_refresh_failure(2));
        assert!(!should_warn_key_cache_refresh_failure(9));
        assert!(should_warn_key_cache_refresh_failure(10));
        assert!(should_warn_key_cache_refresh_failure(20));
    }

    #[test]
    fn rotate_daemon_log_if_large_truncates_only_oversized_logs() {
        // Branch: daemon startup log rotation size gate.
        let dir = tempfile::tempdir().unwrap();
        let small = dir.path().join("small.log");
        std::fs::write(&small, b"small log").unwrap();
        rotate_daemon_log_if_large(&small);
        assert_eq!(std::fs::read(&small).unwrap(), b"small log");

        let large = dir.path().join("large.log");
        std::fs::write(&large, vec![b'x'; 2 * 1024 * 1024 + 1]).unwrap();
        rotate_daemon_log_if_large(&large);
        assert_eq!(std::fs::read(&large).unwrap(), b"--- log rotated ---\n");
    }

    #[test]
    fn daemon_state_path_uses_state_json_extension() {
        assert_eq!(
            daemon_state_path(Path::new("/tmp/kache/daemon.sock")),
            Path::new("/tmp/kache/daemon.state.json")
        );
    }

    #[test]
    fn daemon_state_is_recent_distinguishes_fresh_from_stale() {
        let fresh = DaemonCoordState {
            pid: 1,
            build_epoch: build_epoch(),
            phase: DaemonPhase::Ready,
            updated_at_ms: now_millis(),
        };
        assert!(daemon_state_is_recent(&fresh));

        let stale = DaemonCoordState {
            pid: 1,
            build_epoch: build_epoch(),
            phase: DaemonPhase::Ready,
            updated_at_ms: now_millis()
                .saturating_sub(DAEMON_COORD_STALE_AFTER.as_millis() as u64 * 2),
        };
        assert!(!daemon_state_is_recent(&stale));
    }

    #[test]
    fn test_daemon_coord_state_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("daemon.sock");
        let coord = DaemonCoordFile::for_socket(&socket_path);

        coord.write_phase(DaemonPhase::Starting).unwrap();
        let state = read_daemon_state(&socket_path).unwrap();
        assert_eq!(state.pid, std::process::id());
        assert_eq!(state.build_epoch, build_epoch());
        assert_eq!(state.phase, DaemonPhase::Starting);
        assert!(daemon_state_is_recent(&state));
    }

    #[test]
    fn test_recover_unhealthy_daemon_cleans_stale_socket_and_state() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("daemon.sock");
        std::fs::write(&socket_path, b"stale").unwrap();

        let state = DaemonCoordState {
            pid: u32::MAX,
            build_epoch: build_epoch(),
            phase: DaemonPhase::Starting,
            updated_at_ms: now_millis(),
        };
        write_json_atomically(&daemon_state_path(&socket_path), &state).unwrap();

        assert!(recover_unhealthy_daemon(&socket_path, "test").unwrap());
        assert!(!socket_path.exists());
        assert!(read_daemon_state(&socket_path).is_none());
    }

    #[test]
    fn test_recover_unhealthy_daemon_terminates_recent_recorded_pid() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("daemon.sock");
        std::fs::write(&socket_path, b"stale").unwrap();
        let run_lock_handle = hold_run_lock_for_test(&socket_path, Duration::from_millis(150));

        let mut child = spawn_blocking_child();

        let state = DaemonCoordState {
            pid: child.id(),
            build_epoch: build_epoch(),
            phase: DaemonPhase::Ready,
            updated_at_ms: now_millis(),
        };
        write_json_atomically(&daemon_state_path(&socket_path), &state).unwrap();

        assert!(recover_unhealthy_daemon(&socket_path, "test").unwrap());
        run_lock_handle.join().unwrap();
        assert_ne!(child.wait().unwrap().code(), Some(0));
        assert!(!socket_path.exists());
        assert!(read_daemon_state(&socket_path).is_none());
    }

    #[test]
    fn test_recover_unhealthy_daemon_terminates_stale_recorded_pid() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("daemon.sock");
        std::fs::write(&socket_path, b"stale").unwrap();
        let run_lock_handle = hold_run_lock_for_test(&socket_path, Duration::from_millis(150));

        let mut child = spawn_blocking_child();

        let state = DaemonCoordState {
            pid: child.id(),
            build_epoch: build_epoch(),
            phase: DaemonPhase::Ready,
            updated_at_ms: now_millis()
                .saturating_sub(DAEMON_COORD_STALE_AFTER.as_millis() as u64 + 1),
        };
        write_json_atomically(&daemon_state_path(&socket_path), &state).unwrap();

        assert!(recover_unhealthy_daemon(&socket_path, "test").unwrap());
        run_lock_handle.join().unwrap();
        assert_ne!(child.wait().unwrap().code(), Some(0));
        assert!(!socket_path.exists());
        assert!(read_daemon_state(&socket_path).is_none());
    }

    #[test]
    fn test_recover_unhealthy_daemon_does_not_kill_pid_without_run_lock() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("daemon.sock");
        std::fs::write(&socket_path, b"stale").unwrap();

        let mut child = spawn_blocking_child();

        let state = DaemonCoordState {
            pid: child.id(),
            build_epoch: build_epoch(),
            phase: DaemonPhase::Ready,
            updated_at_ms: now_millis(),
        };
        write_json_atomically(&daemon_state_path(&socket_path), &state).unwrap();

        assert!(recover_unhealthy_daemon(&socket_path, "test").unwrap());
        assert!(child.try_wait().unwrap().is_none());
        let _ = child.kill();
        let _ = child.wait();
        assert!(!socket_path.exists());
        assert!(read_daemon_state(&socket_path).is_none());
    }

    #[test]
    fn test_recover_unhealthy_daemon_refuses_held_lock_without_state() {
        // Branch: run lock held with no recoverable coordinator state.
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("daemon.sock");
        let run_lock_handle = hold_run_lock_for_test(&socket_path, Duration::from_millis(150));

        assert!(!recover_unhealthy_daemon(&socket_path, "test").unwrap());
        run_lock_handle.join().unwrap();
    }

    #[test]
    fn test_request_gc_serde() {
        let req = Request::Gc(GcRequest {
            max_age_hours: Some(168),
        });
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, parsed);

        assert!(json.contains("\"gc\""));
        assert!(json.contains("\"max_age_hours\":168"));
    }

    #[test]
    fn test_request_gc_null_age_serde() {
        let req = Request::Gc(GcRequest {
            max_age_hours: None,
        });
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, parsed);
        assert!(json.contains("\"max_age_hours\":null"));
    }

    #[test]
    fn test_request_remote_check_serde() {
        let req = Request::RemoteCheck(RemoteCheckRequest {
            key: "abc123".into(),
            entry_dir: "/tmp/store/abc123".into(),
            crate_name: String::new(),
        });
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, parsed);

        assert!(json.contains("\"remote_check\""));
        assert!(json.contains("\"key\":\"abc123\""));
        assert!(json.contains("\"entry_dir\":\"/tmp/store/abc123\""));
    }

    #[test]
    fn test_response_ok_serde() {
        let resp = Response::ok();
        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(json, r#"{"ok":true}"#);
    }

    #[test]
    fn test_response_ok_evicted_serde() {
        let resp = Response::ok_evicted(5);
        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(json, r#"{"ok":true,"evicted":5}"#);
    }

    #[test]
    fn test_response_gc_skipped_serde() {
        let resp = Response::ok_gc_skipped();
        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(json, r#"{"ok":true,"evicted":0,"skipped":true}"#);
    }

    #[test]
    fn test_response_found_true_serde() {
        let resp = Response::found(true);
        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(json, r#"{"ok":true,"found":true}"#);
    }

    #[test]
    fn test_response_found_false_serde() {
        let resp = Response::found(false);
        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(json, r#"{"ok":true,"found":false}"#);
    }

    #[test]
    fn test_response_found_prefetched_serde() {
        // Branch: found+prefetched response constructor.
        let resp = Response::found_prefetched(true, true);
        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(json, r#"{"ok":true,"found":true,"prefetched":true}"#);
    }

    /// A StatsResponse serialized by an OLD daemon (no `prefetch` field) must
    /// deserialize on a new client, and the new nested snapshot round-trips.
    /// Pins the #[serde(default)] compatibility contract for #485 Phase 0.
    #[test]
    fn test_stats_response_prefetch_field_is_backward_compatible() {
        // Old-daemon shape: no `prefetch` key at all.
        let old_json = r#"{"total_size":0,"max_size":0,"entry_count":0,"entries":null,
            "events":{"local_hits":0,"prefetch_hits":0,"remote_hits":0,"dups":0,
            "misses":0,"errors":0,"total_elapsed_ms":0,"hit_elapsed_ms":0,
            "miss_elapsed_ms":0,"hit_compile_time_ms":0,"miss_compile_time_ms":0,
            "store_output_blobs":0,"store_duplicate_blobs":0,"store_new_blobs":0}}"#;
        let parsed: StatsResponse = serde_json::from_str(old_json).unwrap();
        assert_eq!(parsed.prefetch, PrefetchStatsSnapshot::default());

        // New shape round-trips.
        let snap = PrefetchStatsSnapshot {
            downloads_completed: 3,
            bytes_downloaded: 1024,
            keys_used: 2,
            keys_cancelled: 1,
            cancelled: true,
            plans_advisory: 1,
            plans_fallback: 4,
            last_plan_candidates: 17,
            dedup_join_waits: 2,
            dedup_join_wait_ms: 250,
            last_list_duration_ms: 42,
            last_list_key_count: 9001,
        };
        let json = serde_json::to_string(&snap).unwrap();
        let back: PrefetchStatsSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(back, snap);
    }

    #[test]
    fn test_response_err_serde() {
        let resp = Response::err("something broke");
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: Response = serde_json::from_str(&json).unwrap();
        assert!(!parsed.ok);
        assert_eq!(parsed.error.as_deref(), Some("something broke"));
        assert_eq!(parsed.evicted, None);
        assert_eq!(parsed.found, None);
    }

    #[test]
    fn test_invalid_request_json() {
        let result = serde_json::from_str::<Request>(r#"{"bogus": 42}"#);
        assert!(result.is_err());
    }

    // ── S3 Key Cache unit tests ──────────────────────────────────

    #[tokio::test]
    async fn test_key_cache_unpopulated_returns_none() {
        let cache = S3KeyCache::new();
        assert_eq!(cache.check("any_key").await, None);
    }

    #[tokio::test]
    async fn test_key_cache_populate_and_check() {
        let cache = S3KeyCache::new();
        let mut keys = HashMap::new();
        keys.insert("key_a".to_string(), "crate_a".to_string());
        keys.insert("key_b".to_string(), "crate_b".to_string());

        cache.populate(keys).await;

        assert_eq!(cache.check("key_a").await, Some(true));
        assert_eq!(cache.check("key_b").await, Some(true));
        assert_eq!(cache.check("key_c").await, Some(false));

        // Reverse index works
        let crate_a_keys = cache.keys_for_crate("crate_a").await;
        assert_eq!(crate_a_keys, vec!["key_a"]);
        assert!(cache.keys_for_crate("unknown").await.is_empty());
    }

    #[tokio::test]
    async fn test_key_cache_insert_after_populate() {
        let cache = S3KeyCache::new();
        cache.populate(HashMap::new()).await;

        assert_eq!(cache.check("new_key").await, Some(false));
        cache.insert("new_key".to_string(), Some("my_crate")).await;
        assert_eq!(cache.check("new_key").await, Some(true));

        // Reverse index updated
        let keys = cache.keys_for_crate("my_crate").await;
        assert_eq!(keys, vec!["new_key"]);
    }

    #[tokio::test]
    async fn test_key_cache_insert_before_populate_is_noop() {
        let cache = S3KeyCache::new();
        // Insert before populate — the Option is None so insert is a no-op
        cache.insert("key".to_string(), Some("crate")).await;
        assert_eq!(cache.check("key").await, None);
        assert!(cache.keys_for_crate("crate").await.is_empty());
    }

    /// kunobi-ninja/kache#213 (Part B): the forward set and reverse index are
    /// swapped/mutated under one lock, so concurrent refreshes (`populate`) and
    /// `insert`s can never leave a key in one view but not the other. With the
    /// old two-separate-locks design an insert landing between the two swaps
    /// could desync the views; here we hammer both paths and assert the
    /// cross-view invariant always holds.
    #[tokio::test]
    async fn test_key_cache_views_stay_consistent_under_concurrency() {
        use std::sync::Arc;
        let cache = Arc::new(S3KeyCache::new());

        let seed: HashMap<String, String> = (0..50)
            .map(|i| (format!("seed_{i}"), format!("crate_{}", i % 5)))
            .collect();
        cache.populate(seed).await;

        let mut tasks = Vec::new();
        // Refreshers: full re-list (always carries the 50 seed keys + own key).
        for r in 0..8 {
            let c = cache.clone();
            tasks.push(tokio::spawn(async move {
                let mut m: HashMap<String, String> = (0..50)
                    .map(|i| (format!("seed_{i}"), format!("crate_{}", i % 5)))
                    .collect();
                m.insert(format!("refresh_{r}"), "crate_r".to_string());
                c.populate(m).await;
            }));
        }
        // Uploaders: single-key inserts racing with the refreshers.
        for k in 0..8 {
            let c = cache.clone();
            tasks.push(tokio::spawn(async move {
                c.insert(format!("up_{k}"), Some("crate_up")).await;
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }

        // Seed keys are in every refresh snapshot, so they always survive.
        assert_eq!(cache.check("seed_0").await, Some(true));

        // Cross-view invariant: forward set and reverse index hold exactly the
        // same keys. A two-step swap could break this; a single-lock swap can't.
        let guard = cache.index.read().await;
        let idx = guard.as_ref().expect("populated");
        let reverse_total: usize = idx.by_crate.values().map(Vec::len).sum();
        assert_eq!(
            idx.keys.len(),
            reverse_total,
            "forward set and reverse index must agree on key count"
        );
        for keys in idx.by_crate.values() {
            for key in keys {
                assert!(
                    idx.keys.contains(key),
                    "key {key} is in by_crate but missing from the forward set"
                );
            }
        }
    }

    // ── Daemon logic (no sockets) ────────────────────────────────

    #[test]
    fn test_handle_gc_empty_store() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon = Daemon::new(config);

        let resp = daemon.handle_gc(&GcRequest {
            max_age_hours: None,
        });
        assert!(resp.ok);
        assert_eq!(resp.evicted, Some(0));
    }

    #[test]
    fn test_handle_gc_reports_lock_skip() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let _gc_lock = store.try_gc_lock().unwrap().expect("gc lock");
        let daemon = Daemon::new(config);

        let resp = daemon.handle_gc(&GcRequest {
            max_age_hours: None,
        });
        assert!(resp.ok);
        assert!(resp.skipped);
        assert_eq!(resp.evicted, Some(0));
    }

    #[test]
    fn test_handle_gc_with_max_age() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon = Daemon::new(config);

        let resp = daemon.handle_gc(&GcRequest {
            max_age_hours: Some(24),
        });
        assert!(resp.ok);
        assert_eq!(resp.evicted, Some(0));
    }

    #[test]
    fn test_handle_gc_evicts_entries() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());

        // Create a source file outside the store (put() copies it in)
        let src_file = dir.path().join("big.rlib");
        std::fs::write(&src_file, vec![0u8; 200]).unwrap();

        let store = Store::open(&config).unwrap();
        store
            .put(
                "testkey",
                "testcrate",
                &["lib".into()],
                &[],
                "host",
                "dev",
                &[(src_file, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        assert!(store.contains("testkey"));
        assert!(store.total_size().unwrap() >= 200);
        // Age past the active-pin grace so eviction can claim it (a just-put
        // entry is "recently accessed" and pinned — kunobi-ninja/kache#326).
        store.set_last_accessed_for_test("testkey", "-1 hour");
        drop(store);

        // Now set max_size below the entry size so eviction triggers
        config.max_size = 100;

        let daemon = Daemon::new(config);
        let stats = daemon.run_gc(None).unwrap();
        assert!(
            stats.entries_evicted > 0,
            "should have evicted at least 1 entry"
        );
    }

    #[test]
    fn test_upload_triggered_eviction_respects_gc_lock() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 100;

        let src_file = dir.path().join("big.rlib");
        std::fs::write(&src_file, vec![0u8; 200]).unwrap();

        let store = Store::open(&config).unwrap();
        store
            .put(
                "upload_evict_key",
                "testcrate",
                &["lib".into()],
                &[],
                "host",
                "dev",
                &[(src_file, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        // Age past the active-pin grace so eviction can claim it
        // (kunobi-ninja/kache#326).
        store.set_last_accessed_for_test("upload_evict_key", "-1 hour");

        let gc_lock = store.try_gc_lock().unwrap().expect("gc lock");
        let daemon = Daemon::new(config);
        daemon.maybe_evict_after_upload();
        assert!(
            store.contains("upload_evict_key"),
            "upload-triggered eviction must skip while gc.lock is held"
        );

        drop(gc_lock);
        daemon.maybe_evict_after_upload();
        assert!(
            !store.contains("upload_evict_key"),
            "eviction should run once gc.lock is available"
        );
    }

    #[test]
    fn test_handle_request_sync_dispatches_gc() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon = Daemon::new(config);

        let req = Request::Gc(GcRequest {
            max_age_hours: None,
        });
        let resp = daemon.handle_request_sync(&req);
        assert!(resp.ok);
        assert_eq!(resp.evicted, Some(0));
    }

    /// #281: the blocking handlers are dispatched through `offload`, which must
    /// return the handler's own response unchanged.
    #[tokio::test]
    async fn offload_returns_the_handler_response() {
        let resp = offload(Response::ok).await;
        assert!(resp.ok);
    }

    /// #281: a panic inside an offloaded handler must surface as an error
    /// response, not unwind and tear down the connection task.
    #[tokio::test]
    async fn offload_maps_a_handler_panic_to_an_error_response() {
        let resp = offload(|| panic!("handler boom")).await;
        assert!(!resp.ok, "a panicking handler must yield an error response");
        assert!(
            resp.error
                .as_deref()
                .unwrap_or_default()
                .contains("task failed"),
            "error should explain the handler task failed, got {:?}",
            resp.error
        );
    }

    #[test]
    fn test_handle_request_sync_rejects_upload() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon = Daemon::new(config);

        let req = Request::Upload(UploadJob {
            key: "k".into(),
            entry_dir: "/tmp".into(),
            crate_name: String::new(),
            client_epoch: 0,
        });
        let resp = daemon.handle_request_sync(&req);
        assert!(!resp.ok);
        assert!(resp.error.as_deref().unwrap().contains("async"));
    }

    #[test]
    fn test_handle_request_sync_rejects_remote_check() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon = Daemon::new(config);

        let req = Request::RemoteCheck(RemoteCheckRequest {
            key: "k".into(),
            entry_dir: "/tmp".into(),
            crate_name: String::new(),
        });
        let resp = daemon.handle_request_sync(&req);
        assert!(!resp.ok);
        assert!(resp.error.as_deref().unwrap().contains("async"));
    }

    #[tokio::test]
    async fn test_handle_upload_no_remote() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path()); // remote = None
        let daemon = Daemon::new(config);

        let job = UploadJob {
            key: "k".into(),
            entry_dir: "/tmp".into(),
            crate_name: String::new(),
            client_epoch: 0,
        };
        let resp = daemon.handle_upload(&job).await;
        assert!(!resp.ok);
        assert!(
            resp.error
                .as_deref()
                .unwrap()
                .contains("no remote configured")
        );
    }

    #[tokio::test]
    async fn test_handle_upload_remote_readonly() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote_readonly = true;
        let daemon = Daemon::new(config);

        let job = UploadJob {
            key: "k".into(),
            entry_dir: "/tmp".into(),
            crate_name: String::new(),
            client_epoch: 0,
        };
        let resp = daemon.handle_upload(&job).await;
        assert!(resp.ok);
        assert!(resp.error.is_none());

        let resp_do = daemon.do_upload(&job).await;
        assert!(resp_do.ok);
        assert!(resp_do.error.is_none());
    }

    #[tokio::test]
    async fn test_handle_remote_check_no_remote() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path()); // remote = None
        let daemon = Daemon::new(config);

        let req = RemoteCheckRequest {
            key: "k".into(),
            entry_dir: "/tmp".into(),
            crate_name: String::new(),
        };
        let resp = daemon.handle_remote_check(&req).await;
        assert!(!resp.ok);
        assert!(
            resp.error
                .as_deref()
                .unwrap()
                .contains("no remote configured")
        );
    }

    #[test]
    fn test_run_gc_returns_count() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon = Daemon::new(config);

        let stats = daemon.run_gc(None).unwrap();
        assert_eq!(stats.entries_evicted, 0);
    }

    #[test]
    fn test_run_gc_cleans_registered_incremental_dirs_once() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.clean_incremental = true;
        let incremental_dir = dir.path().join("workspace/target/debug/incremental");
        std::fs::create_dir_all(&incremental_dir).unwrap();
        std::fs::write(incremental_dir.join("junk"), b"tmp").unwrap();

        let store = Store::open(&config).unwrap();
        store.remember_incremental_dir(&incremental_dir).unwrap();
        drop(store);

        let daemon = Daemon::new(config.clone());
        let stats = daemon.run_gc(None).unwrap();
        assert_eq!(stats.entries_evicted, 0);
        assert!(!incremental_dir.exists());

        std::fs::create_dir_all(&incremental_dir).unwrap();
        std::fs::write(incremental_dir.join("junk"), b"tmp2").unwrap();

        let stats = daemon.run_gc(None).unwrap();
        assert_eq!(stats.entries_evicted, 0);
        assert!(incremental_dir.exists());
    }

    #[test]
    fn clean_tool_version_caches_removes_only_old_tool_version_txt() {
        // Branch: old rustc/linker version-cache file cleanup.
        let dir = tempfile::tempdir().unwrap();
        let old_rustc = dir.path().join("rustc-ver-old.txt");
        let old_linker = dir.path().join("linker-ver-old.txt");
        let fresh_rustc = dir.path().join("rustc-ver-fresh.txt");
        let old_other = dir.path().join("other-ver-old.txt");
        for path in [&old_rustc, &old_linker, &fresh_rustc, &old_other] {
            std::fs::write(path, b"version").unwrap();
        }

        let old = filetime::FileTime::from_system_time(
            std::time::SystemTime::now() - Duration::from_secs(8 * 24 * 3600),
        );
        for path in [&old_rustc, &old_linker, &old_other] {
            filetime::set_file_mtime(path, old).unwrap();
        }

        Daemon::clean_tool_version_caches(dir.path());

        assert!(!old_rustc.exists());
        assert!(!old_linker.exists());
        assert!(fresh_rustc.exists());
        assert!(old_other.exists());
    }

    // ── Socket integration tests ─────────────────────────────────

    #[tokio::test]
    async fn test_socket_gc_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let daemon = Arc::new(Daemon::new(config));
        let resp = one_shot_request(
            &daemon,
            &socket_path,
            &Request::Gc(GcRequest {
                max_age_hours: None,
            }),
        )
        .await;

        assert!(resp.ok);
        assert_eq!(resp.evicted, Some(0));
    }

    #[tokio::test]
    async fn test_socket_remote_check_no_remote_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path()); // remote = None
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let daemon = Arc::new(Daemon::new(config));
        let resp = one_shot_request(
            &daemon,
            &socket_path,
            &Request::RemoteCheck(RemoteCheckRequest {
                key: "test_key".into(),
                entry_dir: "/tmp/test".into(),
                crate_name: String::new(),
            }),
        )
        .await;

        assert!(!resp.ok);
        assert!(
            resp.error
                .as_deref()
                .unwrap()
                .contains("no remote configured")
        );
    }

    // Unix-only by nature: it asserts that a leftover regular *file* at the
    // socket path is not a connectable socket and can be removed. Windows uses
    // named pipes, which leave no on-disk artifact at the path, so there is no
    // equivalent stale-file scenario to test. (Stale daemon *state* cleanup is
    // covered cross-platform by test_recover_unhealthy_daemon_cleans_*.)
    #[cfg(unix)]
    #[test]
    fn test_stale_socket_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("daemon.sock");

        // Create a file pretending to be a stale socket
        std::fs::write(&socket_path, b"stale").unwrap();
        assert!(socket_path.exists());

        // Attempting to connect as a Unix socket should fail
        let result = std::os::unix::net::UnixStream::connect(&socket_path);
        assert!(result.is_err());

        // After detection, it should be removable (simulating what server_main does)
        std::fs::remove_file(&socket_path).unwrap();
        assert!(!socket_path.exists());
    }

    #[test]
    fn test_send_request_to_nonexistent_socket() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("nonexistent.sock");

        let req = Request::Gc(GcRequest {
            max_age_hours: None,
        });
        let result = send_request(&socket_path, &req);
        assert!(result.is_err());
    }

    #[test]
    fn test_send_remote_check_unreachable_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());

        // No daemon running — should return None gracefully
        let result = send_remote_check(&config, "some_key", Path::new("/tmp/test"), "unknown");
        assert!(result.is_none());
    }

    #[test]
    fn remote_check_response_parser_handles_prefetched_error_and_malformed() {
        // Branch: remote-check response parse success/error/malformed arms.
        let hit = serde_json::to_string(&Response::found_prefetched(true, true)).unwrap();
        let result = remote_check_result_from_response_line(&hit).unwrap();
        assert!(result.found);
        assert!(result.prefetched);

        let plain_hit = serde_json::to_string(&Response::found(true)).unwrap();
        let result = remote_check_result_from_response_line(&plain_hit).unwrap();
        assert!(result.found);
        assert!(!result.prefetched);

        let err = serde_json::to_string(&Response::err("remote down")).unwrap();
        assert!(remote_check_result_from_response_line(&err).is_none());
        assert!(remote_check_result_from_response_line("{not json").is_none());
    }

    #[test]
    fn test_response_constructors() {
        let ok = Response::ok();
        assert!(ok.ok && ok.evicted.is_none() && ok.error.is_none() && ok.found.is_none());
        assert!(ok.batch_results.is_none());

        let evicted = Response::ok_evicted(3);
        assert!(evicted.ok && evicted.evicted == Some(3));

        let found_true = Response::found(true);
        assert!(found_true.ok && found_true.found == Some(true));

        let found_false = Response::found(false);
        assert!(found_false.ok && found_false.found == Some(false));

        let batch = Response::ok_batch(vec![Response::found(true), Response::found(false)]);
        assert!(batch.ok && batch.batch_results.as_ref().unwrap().len() == 2);

        let err = Response::err("oops");
        assert!(!err.ok && err.error.as_deref() == Some("oops"));
    }

    // ── Stats protocol tests ─────────────────────────────────────

    #[test]
    fn test_stats_request_serde() {
        let req = Request::Stats(StatsRequest {
            include_entries: true,
            sort_by: Some("size".into()),
            event_hours: Some(48),
            client_epoch: 0,
        });
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, parsed);

        assert!(json.contains("\"stats\""));
        assert!(json.contains("\"include_entries\":true"));
        assert!(json.contains("\"sort_by\":\"size\""));
        assert!(json.contains("\"event_hours\":48"));
    }

    #[test]
    fn test_stats_response_serde() {
        let stats = StatsResponse {
            total_size: 1024,
            max_size: 4096,
            entry_count: 5,
            entries: None,
            events: EventStatsResponse {
                local_hits: 10,
                prefetch_hits: 0,
                remote_hits: 2,
                dups: 1,
                misses: 3,
                errors: 1,
                total_elapsed_ms: 5000,
                hit_elapsed_ms: 120,
                miss_elapsed_ms: 4880,
                hit_compile_time_ms: 22000,
                miss_compile_time_ms: 9000,
                store_output_blobs: 4,
                store_duplicate_blobs: 1,
                store_new_blobs: 3,
            },
            version: String::new(),
            build_epoch: 0,
            pending_uploads: 0,
            active_downloads: 0,
            s3_concurrency_total: 0,
            s3_concurrency_used: 0,
            upload_queue_capacity: 0,
            uploads_completed: 0,
            uploads_failed: 0,
            uploads_skipped: 0,
            downloads_completed: 0,
            downloads_failed: 0,
            bytes_uploaded: 0,
            bytes_downloaded: 0,
            recent_transfers: Vec::new(),
            prefetch: PrefetchStatsSnapshot::default(),
        };
        let resp = Response::ok_stats(stats.clone());
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: Response = serde_json::from_str(&json).unwrap();
        assert!(parsed.ok);
        let parsed_stats = parsed.stats.unwrap();
        assert_eq!(parsed_stats, stats);
    }

    #[test]
    fn test_stats_response_with_entries() {
        let stats = StatsResponse {
            total_size: 2048,
            max_size: 8192,
            entry_count: 2,
            entries: Some(vec![
                StatsEntry {
                    cache_key: "abc123def456".into(),
                    crate_name: "serde".into(),
                    crate_type: "lib".into(),
                    profile: "release".into(),
                    size: 1024,
                    hit_count: 5,
                    created_at: "2025-01-01 00:00:00".into(),
                    last_accessed: "2025-06-01 12:00:00".into(),
                    content_hash: None,
                },
                StatsEntry {
                    cache_key: "789abc012def".into(),
                    crate_name: "tokio".into(),
                    crate_type: "lib".into(),
                    profile: "dev".into(),
                    size: 1024,
                    hit_count: 3,
                    created_at: "2025-02-01 00:00:00".into(),
                    last_accessed: "2025-05-15 08:00:00".into(),
                    content_hash: None,
                },
            ]),
            events: EventStatsResponse {
                local_hits: 0,
                prefetch_hits: 0,
                remote_hits: 0,
                dups: 0,
                misses: 0,
                errors: 0,
                total_elapsed_ms: 0,
                hit_elapsed_ms: 0,
                miss_elapsed_ms: 0,
                hit_compile_time_ms: 0,
                miss_compile_time_ms: 0,
                store_output_blobs: 0,
                store_duplicate_blobs: 0,
                store_new_blobs: 0,
            },
            version: String::new(),
            build_epoch: 0,
            pending_uploads: 0,
            active_downloads: 0,
            s3_concurrency_total: 0,
            s3_concurrency_used: 0,
            upload_queue_capacity: 0,
            uploads_completed: 0,
            uploads_failed: 0,
            uploads_skipped: 0,
            downloads_completed: 0,
            downloads_failed: 0,
            bytes_uploaded: 0,
            bytes_downloaded: 0,
            recent_transfers: Vec::new(),
            prefetch: PrefetchStatsSnapshot::default(),
        };
        let resp = Response::ok_stats(stats);
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: Response = serde_json::from_str(&json).unwrap();
        let entries = parsed.stats.unwrap().entries.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].crate_name, "serde");
        assert_eq!(entries[1].crate_name, "tokio");
    }

    #[test]
    fn test_handle_stats_empty_store() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon = Daemon::new(config);

        let resp = daemon.handle_stats(&StatsRequest {
            include_entries: true,
            sort_by: None,
            event_hours: Some(24),
            client_epoch: 0,
        });
        assert!(resp.ok);
        let stats = resp.stats.unwrap();
        assert_eq!(stats.total_size, 0);
        assert_eq!(stats.entry_count, 0);
        assert_eq!(stats.max_size, 50 * 1024 * 1024);
        assert_eq!(stats.entries.unwrap().len(), 0);
        assert_eq!(stats.events.local_hits, 0);
        assert_eq!(stats.events.misses, 0);
    }

    #[test]
    fn test_daemon_reuses_store_handle() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon = Daemon::new(config);

        let first = daemon.store_lock().unwrap() as *const _;
        let second = daemon.store_lock().unwrap() as *const _;

        assert_eq!(first, second);
    }

    #[test]
    fn test_handle_hash_files_uses_memory_cache() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon = Daemon::new(config);

        let file = dir.path().join("large.rlib");
        std::fs::write(&file, vec![7u8; 70 * 1024]).unwrap();
        let metadata = std::fs::metadata(&file).unwrap();
        let req = HashFilesRequest {
            files: vec![HashFileRequest {
                path: file.to_string_lossy().into_owned(),
                size: i64::try_from(metadata.len()).unwrap(),
                mtime_ns: crate::cache_key::metadata_mtime_ns(&metadata),
                ctime_ns: crate::cache_key::metadata_ctime_ns(&metadata),
                inode: crate::cache_key::metadata_inode(&metadata),
            }],
        };

        let first = daemon.handle_hash_files(&req);
        assert!(first.ok);
        let first_result = &first.hash_results.as_ref().unwrap()[0];
        assert!(first_result.hash.is_some());
        assert!(!first_result.cache_hit);
        assert!(first_result.bytes_hashed > 0);

        let second = daemon.handle_hash_files(&req);
        assert!(second.ok);
        let second_result = &second.hash_results.as_ref().unwrap()[0];
        assert_eq!(first_result.hash, second_result.hash);
        assert!(second_result.cache_hit);
        assert_eq!(second_result.bytes_hashed, 0);
    }

    /// #281: the lock-narrowed HashFiles path (cache lookup under the store
    /// lock, blake3 outside it, record under the lock) must preserve the
    /// PERSISTENT cache. A second daemon with a fresh in-memory cache but the
    /// same `index.db` gets a hit without re-hashing, and every hash matches
    /// the canonical `hash_file`.
    #[test]
    fn handle_hash_files_persistent_cache_hit_across_daemons() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());

        let file = dir.path().join("big.rlib");
        std::fs::write(&file, vec![3u8; 80 * 1024]).unwrap(); // ≥ 64 KiB → cacheable
        let metadata = std::fs::metadata(&file).unwrap();
        let req = HashFilesRequest {
            files: vec![HashFileRequest {
                path: file.to_string_lossy().into_owned(),
                size: i64::try_from(metadata.len()).unwrap(),
                mtime_ns: crate::cache_key::metadata_mtime_ns(&metadata),
                ctime_ns: crate::cache_key::metadata_ctime_ns(&metadata),
                inode: crate::cache_key::metadata_inode(&metadata),
            }],
        };
        let expected = crate::cache_key::hash_file(&file).unwrap();

        // Daemon A: cold — persistent-cache miss, computes and records.
        let a = Daemon::new(config.clone());
        let ra = a.handle_hash_files(&req);
        let ra = &ra.hash_results.as_ref().unwrap()[0];
        assert_eq!(ra.hash.as_deref(), Some(expected.as_str()));
        assert!(!ra.cache_hit, "first hash is a persistent-cache miss");
        assert!(ra.bytes_hashed > 0);

        // Daemon B: fresh in-memory cache, same store — must hit the PERSISTENT
        // cache via the lock-narrowed lookup rather than re-hashing.
        let b = Daemon::new(config);
        let rb = b.handle_hash_files(&req);
        let rb = &rb.hash_results.as_ref().unwrap()[0];
        assert_eq!(rb.hash.as_deref(), Some(expected.as_str()));
        assert!(rb.cache_hit, "second daemon must hit the persistent cache");
        assert_eq!(rb.bytes_hashed, 0);
    }

    #[test]
    fn handle_hash_files_rejects_changed_metadata_before_hashing() {
        // Branch: stale per-file metadata returns an error result.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon = Daemon::new(config);

        let file = dir.path().join("input.bin");
        std::fs::write(&file, b"stable bytes").unwrap();
        let metadata = std::fs::metadata(&file).unwrap();
        let resp = daemon.handle_hash_files(&HashFilesRequest {
            files: vec![HashFileRequest {
                path: file.to_string_lossy().into_owned(),
                size: i64::try_from(metadata.len()).unwrap() + 1,
                mtime_ns: crate::cache_key::metadata_mtime_ns(&metadata),
                ctime_ns: crate::cache_key::metadata_ctime_ns(&metadata),
                inode: crate::cache_key::metadata_inode(&metadata),
            }],
        });

        assert!(resp.ok);
        let result = &resp.hash_results.as_ref().unwrap()[0];
        assert_eq!(result.hash, None);
        assert_eq!(
            result.error.as_deref(),
            Some("file metadata changed before hashing")
        );
    }

    #[test]
    fn handle_hash_files_reports_hash_read_error() {
        // Branch: hash_file failure becomes a per-file error result.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon = Daemon::new(config);

        let input_dir = dir.path().join("not-a-file");
        std::fs::create_dir(&input_dir).unwrap();
        let metadata = std::fs::metadata(&input_dir).unwrap();
        let resp = daemon.handle_hash_files(&HashFilesRequest {
            files: vec![HashFileRequest {
                path: input_dir.to_string_lossy().into_owned(),
                size: i64::try_from(metadata.len()).unwrap(),
                mtime_ns: crate::cache_key::metadata_mtime_ns(&metadata),
                ctime_ns: crate::cache_key::metadata_ctime_ns(&metadata),
                inode: crate::cache_key::metadata_inode(&metadata),
            }],
        });

        assert!(resp.ok);
        let result = &resp.hash_results.as_ref().unwrap()[0];
        assert_eq!(result.hash, None);
        assert_eq!(result.bytes_hashed, 0);
        assert!(
            result
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("reading"),
            "got {:?}",
            result.error
        );
    }

    #[test]
    fn test_handle_stats_with_store_entries() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());

        // Put an entry in the store
        let src_file = dir.path().join("lib.rlib");
        std::fs::write(&src_file, vec![0u8; 100]).unwrap();

        let store = Store::open(&config).unwrap();
        store
            .put(
                "key1",
                "mycrate",
                &["lib".into()],
                &[],
                "host",
                "dev",
                &[(src_file, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        drop(store);

        let daemon = Daemon::new(config);
        let resp = daemon.handle_stats(&StatsRequest {
            include_entries: true,
            sort_by: Some("size".into()),
            event_hours: Some(24),
            client_epoch: 0,
        });
        assert!(resp.ok);
        let stats = resp.stats.unwrap();
        assert_eq!(stats.entry_count, 1);
        assert!(stats.total_size >= 100);
        let entries = stats.entries.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].crate_name, "mycrate");
    }

    #[test]
    fn test_handle_request_sync_dispatches_stats() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon = Daemon::new(config);

        let req = Request::Stats(StatsRequest {
            include_entries: false,
            sort_by: None,
            event_hours: None,
            client_epoch: 0,
        });
        let resp = daemon.handle_request_sync(&req);
        assert!(resp.ok);
        assert!(resp.stats.is_some());
    }

    #[test]
    fn test_send_stats_request_unreachable() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());

        // No daemon running — should return Err
        let result = send_stats_request(&config, false, None, None);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_socket_stats_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let daemon = Arc::new(Daemon::new(config));
        let resp = one_shot_request(
            &daemon,
            &socket_path,
            &Request::Stats(StatsRequest {
                include_entries: true,
                sort_by: Some("size".into()),
                event_hours: Some(24),
                client_epoch: 0,
            }),
        )
        .await;

        assert!(resp.ok);
        let stats = resp.stats.unwrap();
        assert_eq!(stats.total_size, 0);
        assert_eq!(stats.entry_count, 0);
        assert!(stats.entries.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_socket_hash_files_roundtrip_hashes_a_real_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        // A real file whose request metadata matches the on-disk stat, so the
        // handler proceeds to actually hash it.
        let file_path = dir.path().join("input.bin");
        std::fs::write(&file_path, b"hash me please").unwrap();
        let meta = std::fs::metadata(&file_path).unwrap();
        let req = HashFileRequest {
            path: file_path.to_string_lossy().into_owned(),
            size: meta.len() as i64,
            mtime_ns: crate::cache_key::metadata_mtime_ns(&meta),
            ctime_ns: crate::cache_key::metadata_ctime_ns(&meta),
            inode: crate::cache_key::metadata_inode(&meta),
        };
        let expected = blake3::hash(b"hash me please").to_hex().to_string();

        let daemon = Arc::new(Daemon::new(config));
        let resp = one_shot_request(
            &daemon,
            &socket_path,
            &Request::HashFiles(HashFilesRequest { files: vec![req] }),
        )
        .await;

        assert!(resp.ok, "hash-files request should succeed: {resp:?}");
        let results = resp.hash_results.expect("hash_results present");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].hash.as_deref(), Some(expected.as_str()));
        assert_eq!(results[0].error, None);
    }

    #[tokio::test]
    async fn test_socket_hash_files_missing_file_reports_error_result() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let req = HashFileRequest {
            path: dir
                .path()
                .join("does-not-exist")
                .to_string_lossy()
                .into_owned(),
            size: 10,
            mtime_ns: 0,
            ctime_ns: 0,
            inode: 0,
        };

        let daemon = Arc::new(Daemon::new(config));
        let resp = one_shot_request(
            &daemon,
            &socket_path,
            &Request::HashFiles(HashFilesRequest { files: vec![req] }),
        )
        .await;

        // The batch request itself succeeds; the per-file result carries the error.
        assert!(resp.ok);
        let results = resp.hash_results.expect("hash_results present");
        assert_eq!(results.len(), 1);
        assert!(results[0].hash.is_none());
        assert!(results[0].error.is_some(), "missing file should error");
    }

    #[test]
    fn send_hash_files_request_empty_is_ok_without_socket() {
        // No files -> early Ok(empty), never touching the socket.
        let result = send_hash_files_request(Path::new("/nonexistent/socket"), Vec::new()).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn send_hash_files_request_missing_socket_errors() {
        // A non-empty request against a missing socket bails before connecting.
        let req = HashFileRequest {
            path: "/some/file".into(),
            size: 1,
            mtime_ns: 0,
            ctime_ns: 0,
            inode: 0,
        };
        let err = send_hash_files_request(Path::new("/nonexistent/socket.sock"), vec![req])
            .expect_err("missing socket -> error");
        assert!(
            err.to_string().contains("socket does not exist"),
            "got: {err}"
        );
    }

    #[test]
    fn hash_files_response_parser_handles_results_error_and_malformed() {
        // Branch: hash-files response parse success/error/malformed arms.
        let ok = Response::ok_hash_results(vec![HashFileResult {
            path: "/tmp/a".into(),
            size: 1,
            mtime_ns: 2,
            ctime_ns: 3,
            inode: 4,
            hash: Some("abc".into()),
            cache_hit: false,
            bytes_hashed: 1,
            error: None,
        }]);
        let ok_json = serde_json::to_string(&ok).unwrap();
        assert_eq!(
            hash_files_results_from_response_line(&ok_json)
                .unwrap()
                .len(),
            1
        );

        let err_json = serde_json::to_string(&Response::err("bad hash")).unwrap();
        let err = hash_files_results_from_response_line(&err_json).unwrap_err();
        assert!(err.to_string().contains("daemon hash_files error"));

        let err = hash_files_results_from_response_line("{not json").unwrap_err();
        assert!(err.to_string().contains("key must be a string"));
    }

    // Unix-only: send_hash_files_request guards on `socket_path.exists()`, which
    // is false for a Windows named pipe (no filesystem `.sock` entry), so the
    // round-trip can't run there. The client logic is covered here on Linux/macOS.
    #[cfg(unix)]
    #[tokio::test]
    async fn send_hash_files_request_client_roundtrip() {
        // CLIENT side: send_hash_files_request connects to a live in-process
        // server and parses the hash results (daemon.rs 3397-3406).
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let file_path = dir.path().join("input.bin");
        std::fs::write(&file_path, b"hash me please").unwrap();
        let meta = std::fs::metadata(&file_path).unwrap();
        let req = HashFileRequest {
            path: file_path.to_string_lossy().into_owned(),
            size: meta.len() as i64,
            mtime_ns: crate::cache_key::metadata_mtime_ns(&meta),
            ctime_ns: crate::cache_key::metadata_ctime_ns(&meta),
            inode: crate::cache_key::metadata_inode(&meta),
        };
        let expected = blake3::hash(b"hash me please").to_hex().to_string();

        let listener = bind_listener(&socket_path);
        let daemon = Arc::new(Daemon::new(config.clone()));
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept");
            let _ =
                handle_connection(stream, &daemon, &AtomicBool::new(false), &Notify::new()).await;
        });

        let sp = socket_path.clone();
        let results = tokio::task::spawn_blocking(move || send_hash_files_request(&sp, vec![req]))
            .await
            .unwrap()
            .expect("send_hash_files_request should succeed");
        server.await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].hash.as_deref(), Some(expected.as_str()));
    }

    #[tokio::test]
    async fn test_socket_build_started_roundtrip_without_remote() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let daemon = Arc::new(Daemon::new(config));
        let resp = one_shot_request(
            &daemon,
            &socket_path,
            &Request::BuildStarted(BuildStartedRequest {
                intent: kache_core::BuildIntent {
                    crate_names: vec!["serde".into()],
                    namespace: Some("ns".into()),
                    cargo_lock_deps: vec![],
                },
                client_epoch: 0,
            }),
        )
        .await;

        // No remote configured: the handler declines (ok=false) but the socket
        // dispatch + serialization round-trips cleanly.
        assert!(!resp.ok);
        assert!(resp.error.is_some());
    }

    #[tokio::test]
    async fn test_socket_batch_remote_check_roundtrip_without_remote() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let daemon = Arc::new(Daemon::new(config));
        let resp = one_shot_request(
            &daemon,
            &socket_path,
            &Request::BatchRemoteCheck(BatchRemoteCheckRequest {
                checks: vec![RemoteCheckRequest {
                    key: "k".into(),
                    entry_dir: "/tmp/k".into(),
                    crate_name: String::new(),
                }],
            }),
        )
        .await;

        // With no remote the batch still returns a structured response.
        assert!(resp.batch_results.is_some() || resp.error.is_some());
    }

    /// Put a single one-file cache entry into the store at `config`.
    fn seed_store_entry(config: &Config, cache_key: &str, crate_name: &str, dir: &Path) {
        let store = Store::open(config).unwrap();
        let src = dir.join(format!("{cache_key}-src"));
        std::fs::create_dir_all(&src).unwrap();
        let artifact = src.join("libfoo.rlib");
        std::fs::write(&artifact, b"artifact bytes").unwrap();
        store
            .put(
                cache_key,
                crate_name,
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "debug",
                &[(artifact, "libfoo.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
    }

    #[tokio::test]
    async fn test_socket_stats_roundtrip_with_populated_store() {
        // A populated store exercises the daemon's stats aggregation + entry
        // listing path (vs the empty-store roundtrip above).
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();
        seed_store_entry(&config, "statskey1", "serde", dir.path());

        let daemon = Arc::new(Daemon::new(config));
        let resp = one_shot_request(
            &daemon,
            &socket_path,
            &Request::Stats(StatsRequest {
                include_entries: true,
                sort_by: Some("size".into()),
                event_hours: Some(24),
                client_epoch: 0,
            }),
        )
        .await;

        assert!(resp.ok);
        let stats = resp.stats.unwrap();
        assert_eq!(stats.entry_count, 1);
        assert!(stats.total_size > 0);
        let entries = stats.entries.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].crate_name, "serde");
    }

    #[tokio::test]
    async fn test_send_stats_request_client_roundtrip() {
        // Exercises the CLIENT side: the sync send_stats_request connects to a
        // live in-process server (real handle_connection) and parses the
        // response. Covers send_stats_request + send_request_with_timeout.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();
        seed_store_entry(&config, "ckey1", "serde", dir.path());

        let listener = bind_listener(&socket_path);
        let daemon = Arc::new(Daemon::new(config.clone()));
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept");
            handle_connection(stream, &daemon, &AtomicBool::new(false), &Notify::new())
                .await
                .expect("handle_connection");
        });

        // send_stats_request is a blocking sync client; run it off the runtime.
        let cfg = config.clone();
        let stats = tokio::task::spawn_blocking(move || {
            send_stats_request(&cfg, true, Some("size"), Some(24))
        })
        .await
        .unwrap()
        .expect("send_stats_request should succeed");
        server.await.unwrap();

        assert_eq!(stats.entry_count, 1);
        assert_eq!(stats.entries.unwrap()[0].crate_name, "serde");
    }

    #[tokio::test]
    async fn test_send_gc_request_client_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();
        seed_store_entry(&config, "gcc1", "serde", dir.path());

        let listener = bind_listener(&socket_path);
        let daemon = Arc::new(Daemon::new(config.clone()));
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept");
            handle_connection(stream, &daemon, &AtomicBool::new(false), &Notify::new())
                .await
                .expect("handle_connection");
        });

        let cfg = config.clone();
        let outcome = tokio::task::spawn_blocking(move || send_gc_request(&cfg, Some(0)))
            .await
            .unwrap()
            .expect("send_gc_request should succeed");
        server.await.unwrap();
        assert!(!outcome.skipped);
        assert!(outcome.evicted.is_some());
    }

    #[tokio::test]
    async fn test_send_remote_check_client_roundtrip() {
        // CLIENT side: send_remote_check connects to a live in-process server,
        // sends a RemoteCheck, and parses the response. The daemon's key cache
        // is fresh + authoritative and lacks the key, so it answers a definitive
        // miss with no S3. Covers send_remote_check's Ok(resp)+resp.ok success
        // arm (daemon.rs 3309-3314) through the real socket + handle_connection.
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(crate::config::RemoteConfig {
            bucket: "test".into(),
            endpoint: Some("http://localhost:9000".into()),
            region: "us-east-1".into(),
            prefix: "artifacts".into(),
            profile: None,
        });
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let listener = bind_listener(&socket_path);
        let daemon = Arc::new(Daemon::new(config.clone()));
        daemon.signal_warming_complete();
        let mut keys = HashMap::new();
        keys.insert("c".repeat(64), "othercrate".to_string());
        daemon.key_cache.populate(keys).await;

        // send_remote_check probes is_reachable() (one connect) before sending
        // the real request (a second connect), so the server must accept more
        // than once. Loop and abort once the client is done.
        let server = tokio::spawn(async move {
            loop {
                let stream = listener.accept().await.expect("accept");
                let _ = handle_connection(stream, &daemon, &AtomicBool::new(false), &Notify::new())
                    .await;
            }
        });

        let cfg = config.clone();
        let missing = "d".repeat(64);
        let result = tokio::task::spawn_blocking(move || {
            send_remote_check(&cfg, &missing, Path::new("/tmp/test"), "crate")
        })
        .await
        .unwrap();
        server.abort();

        let result = result.expect("authoritative miss yields a definitive result");
        assert!(
            !result.found,
            "the missing key should round-trip as not found"
        );
    }

    #[tokio::test]
    async fn test_send_remote_check_error_response_yields_none() {
        // The daemon has no remote configured, so handle_remote_check returns an
        // error Response (ok=false). The client send_remote_check sees resp.ok ==
        // false and returns None (covers the error-response arm, daemon.rs
        // 3367-3372).
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path()); // remote = None
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let listener = bind_listener(&socket_path);
        let daemon = Arc::new(Daemon::new(config.clone()));
        daemon.signal_warming_complete();
        // send_remote_check probes is_reachable() before the real request, so the
        // server must accept more than once.
        let server = tokio::spawn(async move {
            loop {
                let stream = listener.accept().await.expect("accept");
                let _ = handle_connection(stream, &daemon, &AtomicBool::new(false), &Notify::new())
                    .await;
            }
        });

        let cfg = config.clone();
        let key = "e".repeat(64);
        let result = tokio::task::spawn_blocking(move || {
            send_remote_check(&cfg, &key, Path::new("/tmp/test"), "crate")
        })
        .await
        .unwrap();
        server.abort();

        assert!(
            result.is_none(),
            "an error response (no remote) must yield None"
        );
    }

    #[tokio::test]
    async fn test_send_shutdown_request_client_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let listener = bind_listener(&socket_path);
        let daemon = Arc::new(Daemon::new(config.clone()));
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept");
            handle_connection(stream, &daemon, &AtomicBool::new(false), &Notify::new())
                .await
                .expect("handle_connection");
        });

        let cfg = config.clone();
        let result = tokio::task::spawn_blocking(move || send_shutdown_request(&cfg))
            .await
            .unwrap();
        server.await.unwrap();
        assert!(result.is_ok(), "shutdown request should round-trip ok");
    }

    #[tokio::test]
    async fn test_socket_gc_roundtrip_evicts_populated_store() {
        // GC with max_age 0h over a populated store exercises the daemon's
        // eviction path and reports the evicted count.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();
        seed_store_entry(&config, "gckey1", "tokio", dir.path());

        let daemon = Arc::new(Daemon::new(config.clone()));
        let resp = one_shot_request(
            &daemon,
            &socket_path,
            &Request::Gc(GcRequest {
                max_age_hours: Some(0),
            }),
        )
        .await;

        // The GC handler ran end-to-end over a populated store (backfill +
        // dedup + age eviction) and reported a structured eviction count.
        assert!(resp.ok, "gc should succeed: {resp:?}");
        assert!(resp.evicted.is_some(), "gc reports an evicted count");
    }

    // ── Daemon remote handlers driven against an injected mock S3 client ─────
    //
    // The daemon's `s3_client` OnceCell can be pre-populated, so a test injects
    // a wire-mock-backed client and exercises the REMOTE handler paths (which
    // otherwise need a live bucket) with zero production change.
    use aws_smithy_http_client::test_util::wire::{ReplayedEvent, WireMockServer};

    fn test_remote_config() -> crate::config::RemoteConfig {
        crate::config::RemoteConfig {
            bucket: "bucket".to_string(),
            endpoint: None,
            region: "us-east-1".to_string(),
            prefix: "prefix".to_string(),
            profile: None,
        }
    }

    async fn mock_s3_client(
        events: Vec<ReplayedEvent>,
    ) -> (
        WireMockServer,
        Arc<dyn crate::remote_backend::RemoteBackend>,
    ) {
        let server = WireMockServer::start(events).await;
        let conf = aws_sdk_s3::config::Builder::new()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "AK", "SK", None, None, "test",
            ))
            .endpoint_url(server.endpoint_url())
            .http_client(server.http_client())
            .force_path_style(true)
            .build();
        let backend = crate::remote_backend::S3Backend::new(
            aws_sdk_s3::Client::from_conf(conf),
            "bucket".to_string(),
        );
        (server, Arc::new(backend))
    }

    #[tokio::test]
    async fn test_socket_remote_check_miss_with_injected_mock_client() {
        // Remote configured + injected mock that 404s the HEAD: handle_remote_check
        // runs its head-probe path and reports found=false (covers get_s3_client,
        // RemoteLayout.exists_entry, and the not-found branch).
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let (server, client) = mock_s3_client(vec![ReplayedEvent::status(404)]).await;
        let daemon = Arc::new(Daemon::new(config));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let resp = one_shot_request(
            &daemon,
            &socket_path,
            &Request::RemoteCheck(RemoteCheckRequest {
                key: "abc123def456".into(),
                entry_dir: dir.path().join("entry").to_string_lossy().into_owned(),
                crate_name: "serde".into(),
            }),
        )
        .await;

        assert!(resp.ok, "remote check should return a response: {resp:?}");
        assert_eq!(resp.found, Some(false), "missing remote key -> found=false");
        server.shutdown();
    }

    #[tokio::test]
    async fn test_socket_prefetch_empty_keys_lists_remote_then_no_op() {
        // Empty prefetch keys + injected mock returning an empty bucket listing:
        // handle_prefetch lists S3, finds nothing missing, and returns ok
        // ("nothing to fetch"). Covers get_s3_client + list_keys + no-op path.
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let empty_list = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
            <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
            <Name>bucket</Name><Prefix>prefix/v3/manifests/</Prefix>\
            <KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>\
            </ListBucketResult>";
        let (server, client) = mock_s3_client(vec![ReplayedEvent::with_body(empty_list)]).await;
        let daemon = Arc::new(Daemon::new(config));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let resp = one_shot_request(
            &daemon,
            &socket_path,
            &Request::Prefetch(PrefetchRequest { keys: Vec::new() }),
        )
        .await;

        assert!(resp.ok, "prefetch over empty remote should be ok: {resp:?}");
        server.shutdown();
    }

    #[tokio::test]
    async fn test_do_upload_skips_when_entry_already_in_remote() {
        // Injected mock HEADs 200 for the manifest -> do_upload sees the entry
        // already exists remotely and returns ok without uploading. Covers
        // get_s3_client + RemoteLayout.exists_entry + the already-exists skip.
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());

        let (server, client) = mock_s3_client(vec![ReplayedEvent::ok()]).await; // 200 HEAD
        let daemon = Arc::new(Daemon::new(config));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let resp = daemon
            .do_upload(&UploadJob {
                key: "abc123def456".into(),
                entry_dir: dir.path().join("entry").to_string_lossy().into_owned(),
                crate_name: "serde".into(),
                client_epoch: 0,
            })
            .await;

        assert!(
            resp.ok,
            "already-present upload should be a no-op ok: {resp:?}"
        );
        server.shutdown();
    }

    #[tokio::test]
    async fn test_do_upload_uploads_when_not_in_remote() {
        // Injected mock 404s the HEAD then 200s the pack + manifest PUTs, so
        // do_upload packs the local entry and uploads it end-to-end. Covers the
        // full upload path: exists_entry(miss) -> upload_entry(pack+manifest) ->
        // transfer-event + key-cache update -> ok.
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        seed_store_entry(&config, "upkey123", "serde", dir.path());
        let entry_dir = config.store_dir().join("upkey123");

        let mut events = vec![ReplayedEvent::status(404)]; // HEAD: not present
        events.extend(std::iter::repeat_with(ReplayedEvent::ok).take(4)); // pack + manifest PUTs
        let (server, client) = mock_s3_client(events).await;
        let daemon = Arc::new(Daemon::new(config));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let resp = daemon
            .do_upload(&UploadJob {
                key: "upkey123".into(),
                entry_dir: entry_dir.to_string_lossy().into_owned(),
                crate_name: "serde".into(),
                client_epoch: 0,
            })
            .await;

        assert!(resp.ok, "upload of a new entry should succeed: {resp:?}");
        server.shutdown();
    }

    #[tokio::test]
    async fn test_do_upload_records_failure_when_put_errors() {
        // Mock 404s the HEAD (not present -> proceed) then 403s the pack PUT, so
        // upload_entry errors and do_upload takes its Err branch: uploads_failed++
        // and a failure TransferEvent, returning Response::err (daemon.rs 1459-1500).
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        seed_store_entry(&config, "upfail1", "serde", dir.path());
        let entry_dir = config.store_dir().join("upfail1");

        let mut events = vec![ReplayedEvent::status(404)]; // HEAD: not present
        events.extend(std::iter::repeat_with(|| ReplayedEvent::status(403)).take(4)); // PUTs denied
        let (server, client) = mock_s3_client(events).await;
        let daemon = Arc::new(Daemon::new(config));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let resp = daemon
            .do_upload(&UploadJob {
                key: "upfail1".into(),
                entry_dir: entry_dir.to_string_lossy().into_owned(),
                crate_name: "serde".into(),
                client_epoch: 0,
            })
            .await;

        assert!(!resp.ok, "a denied upload PUT must fail: {resp:?}");
        assert_eq!(
            daemon
                .transfer_counters
                .uploads_failed
                .load(Ordering::Relaxed),
            1
        );
        server.shutdown();
    }

    #[tokio::test]
    async fn test_handle_build_started_falls_back_to_local_planning() {
        // With a remote configured but no planner endpoint (resolve_prefetch_plan
        // -> Ok(None)) and no local/remote candidates, handle_build_started runs
        // the fallback planner, finds nothing to prefetch, and returns ok.
        // Covers the fallback-planning branch (daemon.rs 2120-2132). namespace is
        // None so no S3 shard query is issued; an injected mock guards the client.
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());

        let (server, client) = mock_s3_client(vec![ReplayedEvent::status(404)]).await;
        let daemon = Arc::new(Daemon::new(config));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let req = BuildStartedRequest {
            intent: kache_core::BuildIntent {
                crate_names: vec!["serde".into(), "tokio".into()],
                namespace: None,
                cargo_lock_deps: vec![],
            },
            client_epoch: 0,
        };
        let resp = daemon.handle_build_started(&req).await;
        assert!(
            resp.ok,
            "fallback with nothing to prefetch should be ok: {resp:?}"
        );
        server.shutdown();
    }

    #[tokio::test]
    async fn test_batch_remote_check_remote_path_with_injected_mock() {
        // Two checks against an injected mock that 404s every HEAD: the batch
        // handler fans out handle_remote_check and returns one found=false per
        // check. Covers handle_batch_remote_check's remote path + join_all.
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());

        // One 404 HEAD per distinct key (over-provisioned; concurrent order-safe
        // since all are 404).
        let events = std::iter::repeat_with(|| ReplayedEvent::status(404))
            .take(6)
            .collect();
        let (server, client) = mock_s3_client(events).await;
        let daemon = Arc::new(Daemon::new(config));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let resp = daemon
            .handle_batch_remote_check(&BatchRemoteCheckRequest {
                checks: vec![
                    RemoteCheckRequest {
                        key: "aaaa1111bbbb2222".into(),
                        entry_dir: dir.path().join("a").to_string_lossy().into_owned(),
                        crate_name: "serde".into(),
                    },
                    RemoteCheckRequest {
                        key: "cccc3333dddd4444".into(),
                        entry_dir: dir.path().join("b").to_string_lossy().into_owned(),
                        crate_name: "tokio".into(),
                    },
                ],
            })
            .await;

        assert!(resp.ok);
        let results = resp.batch_results.expect("batch results present");
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.found == Some(false)));
        server.shutdown();
    }

    #[tokio::test]
    async fn test_remote_check_hit_then_download_failure() {
        // Injected mock: HEAD 200 (entry exists) then a garbage pack body for the
        // GET, so download_entry fails. Covers handle_remote_check's HIT branch +
        // download claim/semaphore + download_entry attempt + the error path.
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());

        let mut events = vec![ReplayedEvent::ok()]; // HEAD 200 -> exists
        // GET pack returns non-zstd bytes -> download_entry errors.
        events.extend(std::iter::repeat_with(|| ReplayedEvent::with_body(b"not a pack")).take(4));
        let (server, client) = mock_s3_client(events).await;
        let daemon = Arc::new(Daemon::new(config));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let resp = daemon
            .handle_remote_check(&RemoteCheckRequest {
                key: "hit01hit02hit03".into(),
                entry_dir: dir.path().join("entry").to_string_lossy().into_owned(),
                crate_name: "serde".into(),
            })
            .await;

        // The entry was present remotely but its download failed -> error.
        assert!(
            !resp.ok,
            "download failure should surface as an error: {resp:?}"
        );
        assert!(resp.error.is_some());
        server.shutdown();
    }

    /// The prefetch cap must always leave head-room in the permit pool for
    /// interactive traffic (#485 Phase 0), across pool sizes.
    #[test]
    fn test_prefetch_concurrency_cap_reserves_interactive_permits() {
        assert_eq!(prefetch_concurrency_cap(16), 12); // default: 4 reserved
        assert_eq!(prefetch_concurrency_cap(8), 6); // 2 reserved
        assert_eq!(prefetch_concurrency_cap(4), 3); // 1 reserved
        assert_eq!(prefetch_concurrency_cap(2), 1); // 1 reserved
        assert_eq!(prefetch_concurrency_cap(1), 1); // degenerate: no reserve
        assert_eq!(prefetch_concurrency_cap(0), 1); // clamped like the pool
        assert_eq!(prefetch_concurrency_cap(64), 60); // reserve capped at 4
        for n in 2..=64u32 {
            assert!(
                prefetch_concurrency_cap(n) < n as usize,
                "pool {n}: prefetch must never be able to hold every permit"
            );
        }
    }

    /// GET 404 = clean miss (#485 Phase 0): a stale key-cache positive sends
    /// the check straight to GET (no HEAD); when the object is gone the
    /// response must be a miss (found=false), NOT an error, and the stale key
    /// must be evicted from the key cache so the next check doesn't repeat it.
    #[tokio::test]
    async fn test_remote_check_known_positive_get_404_is_clean_miss() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());

        // Only event: the GET answers 404 (no HEAD happens — key cache says positive).
        let (server, client) = mock_s3_client(vec![ReplayedEvent::status(404)]).await;
        let daemon = Arc::new(Daemon::new(config));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        // Fresh, authoritative-positive key cache entry for the key.
        let key = "gone01gone02gone03";
        let mut keys = HashMap::new();
        keys.insert(key.to_string(), "serde".to_string());
        daemon.key_cache.populate(keys).await;

        let resp = daemon
            .handle_remote_check(&RemoteCheckRequest {
                key: key.into(),
                entry_dir: dir.path().join("entry").to_string_lossy().into_owned(),
                crate_name: "serde".into(),
            })
            .await;

        assert!(
            resp.ok,
            "GET 404 must be a clean miss, not an error: {resp:?}"
        );
        assert_eq!(resp.found, Some(false));
        // The stale positive was evicted.
        assert_eq!(daemon.key_cache.check(key).await, Some(false));
        // Not counted as a failed transfer.
        assert_eq!(
            daemon
                .transfer_counters
                .downloads_failed
                .load(Ordering::Relaxed),
            0
        );
        server.shutdown();
    }

    /// Build a valid v3 entry pack for `key` from a throwaway store.
    fn build_entry_pack(key: &str, crate_name: &str) -> Vec<u8> {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path());
        let store = Store::open(&cfg).unwrap();
        let src = tmp.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        let artifact = src.join("libfoo.rlib");
        std::fs::write(&artifact, b"real artifact bytes").unwrap();
        store
            .put(
                key,
                crate_name,
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "debug",
                &[(artifact, "libfoo.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        let entry_dir = store.entry_dir(key);
        let meta: crate::store::EntryMeta =
            serde_json::from_slice(&std::fs::read(entry_dir.join("meta.json")).unwrap()).unwrap();
        crate::remote_layout::create_entry_pack_zstd(&entry_dir, &store.blobs_dir(), &meta, 3)
            .unwrap()
    }

    #[tokio::test]
    async fn test_remote_check_hit_downloads_and_imports() {
        // HEAD 200 then a VALID pack GET: handle_remote_check downloads, extracts,
        // and imports the entry, returning found=true. Covers the HIT SUCCESS
        // path (download_entry + import_restored_entry).
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let key = "hitok01hitok02hi";
        let pack = build_entry_pack(key, "serde");
        // The wrapper passes entry_dir = store_dir/key; mirror that so the import
        // finds the extracted entry.
        let entry_dir = config.store_dir().join(key);

        let (server, client) =
            mock_s3_client(vec![ReplayedEvent::ok(), ReplayedEvent::with_body(&pack)]).await;
        let daemon = Arc::new(Daemon::new(config.clone()));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let resp = daemon
            .handle_remote_check(&RemoteCheckRequest {
                key: key.to_string(),
                entry_dir: entry_dir.to_string_lossy().into_owned(),
                crate_name: "serde".to_string(),
            })
            .await;

        assert!(resp.ok, "hit+download should succeed: {resp:?}");
        assert_eq!(resp.found, Some(true));
        assert!(
            config.store_dir().join(key).join("meta.json").exists(),
            "entry should be imported into the local store"
        );
        server.shutdown();
    }

    #[tokio::test]
    async fn test_handle_prefetch_explicit_key_downloads_in_background() {
        // handle_prefetch with an explicit key spawns the background download
        // coordinator. With the injected mock serving a valid pack, the
        // coordinator downloads + imports the entry. Covers the prefetch
        // coordinator + per-key download task (the biggest daemon block).
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        // handle_prefetch validates the key: exactly 64 hex chars.
        let key = "abcdef0123456789".repeat(4);
        let key = key.as_str();
        let pack = build_entry_pack(key, "serde");

        // Over-provision pack responses for the coordinator's GET(s).
        let events = std::iter::repeat_with(|| ReplayedEvent::with_body(&pack))
            .take(4)
            .collect();
        let (server, client) = mock_s3_client(events).await;
        let daemon = Arc::new(Daemon::new(config.clone()));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let resp = daemon
            .handle_prefetch(&PrefetchRequest {
                keys: vec![(key.to_string(), "serde".to_string())],
            })
            .await;
        assert!(resp.ok, "prefetch dispatch should be ok: {resp:?}");

        // The coordinator runs in the background; poll until it imports the entry.
        let entry_meta = config.store_dir().join(key).join("meta.json");
        let mut imported = false;
        for _ in 0..100 {
            if entry_meta.exists() {
                imported = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            imported,
            "background prefetch coordinator should download + import the entry"
        );
        server.shutdown();
    }

    #[tokio::test]
    async fn test_handle_prefetch_records_a_failed_download() {
        // The injected mock serves garbage for the pack GET, so the coordinator's
        // download_entry fails and the per-key task takes its error branch:
        // downloads_failed++ and a failure TransferEvent, with no import.
        // Covers handle_prefetch's download-error path (daemon.rs 2006-2034).
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let key = "abcdef0123456789".repeat(4);
        let key = key.as_str();

        let events = std::iter::repeat_with(|| ReplayedEvent::with_body(b"not a valid pack"))
            .take(6)
            .collect();
        let (server, client) = mock_s3_client(events).await;
        let daemon = Arc::new(Daemon::new(config.clone()));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let resp = daemon
            .handle_prefetch(&PrefetchRequest {
                keys: vec![(key.to_string(), "serde".to_string())],
            })
            .await;
        assert!(
            resp.ok,
            "prefetch dispatch is ok even if downloads fail: {resp:?}"
        );

        // Poll the failure counter; the download runs in the background.
        let mut failed = false;
        for _ in 0..100 {
            if daemon
                .transfer_counters
                .downloads_failed
                .load(Ordering::Relaxed)
                >= 1
            {
                failed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(failed, "a garbage pack must record a failed download");
        // Nothing was imported.
        assert!(!config.store_dir().join(key).join("meta.json").exists());
        server.shutdown();
    }

    fn list_manifests_xml(keys: &[&str]) -> String {
        let contents: String = keys
            .iter()
            .map(|k| format!("<Contents><Key>{k}</Key><Size>10</Size></Contents>"))
            .collect();
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Name>bucket</Name><Prefix>prefix/v3/manifests/</Prefix>\
             <KeyCount>{}</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>\
             {contents}</ListBucketResult>",
            keys.len()
        )
    }

    #[tokio::test]
    async fn test_populate_key_cache_lists_and_populates() {
        // Injected mock returns a 2-key manifest listing -> populate_key_cache
        // lists S3 and seeds the in-memory key cache. Covers the background
        // key-cache population path.
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());

        let xml = list_manifests_xml(&[
            "prefix/v3/manifests/serde/key1aaaa.json",
            "prefix/v3/manifests/tokio/key2bbbb.json",
        ]);
        let (server, client) = mock_s3_client(vec![ReplayedEvent::with_body(xml)]).await;
        let daemon = Daemon::new(config);
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let count = populate_key_cache(&daemon)
            .await
            .expect("populate_key_cache should succeed");
        assert_eq!(count, 2);
        // The cache now answers positively for a listed key.
        assert_eq!(daemon.key_cache.check("key1aaaa").await, Some(true));
        server.shutdown();
    }

    #[tokio::test]
    async fn test_monolithic_manifest_prefetch_downloads_and_filters() {
        // Serve a build manifest whose single entry is below the prefetch cost
        // threshold, so monolithic_manifest_prefetch downloads + parses it, then
        // skips the cheap crate (no prefetch queued). Covers download_manifest +
        // the cost-benefit filter path.
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        let remote = test_remote_config();
        config.remote = Some(remote.clone());

        let manifest = crate::remote::BuildManifest {
            version: 3,
            created: "2025-01-01T00:00:00Z".to_string(),
            manifest_key: crate::cli::default_manifest_key(),
            entries: vec![crate::remote::ManifestEntry {
                cache_key: "cheapkey".to_string(),
                crate_name: "cheap".to_string(),
                compile_time_ms: 10, // below the 1000ms default threshold -> skipped
                artifact_size: 100,
            }],
        };
        let body = serde_json::to_vec(&manifest).unwrap();
        let (server, client) = mock_s3_client(vec![ReplayedEvent::with_body(body)]).await;
        let daemon = Arc::new(Daemon::new(config));

        // Should complete without panicking and without queuing the cheap crate.
        monolithic_manifest_prefetch(&daemon, client.as_ref(), &remote).await;
        server.shutdown();
    }

    #[tokio::test]
    async fn test_monolithic_manifest_prefetch_skips_when_no_manifest() {
        // The mock 404s the manifest GET, so download_manifest errors and
        // monolithic_manifest_prefetch logs + returns early. Covers the
        // "no manifest, skipping" arm (daemon.rs 2957-2960).
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        let remote = test_remote_config();
        config.remote = Some(remote.clone());

        let (server, client) = mock_s3_client(vec![ReplayedEvent::status(404)]).await;
        let daemon = Arc::new(Daemon::new(config));

        monolithic_manifest_prefetch(&daemon, client.as_ref(), &remote).await; // must not panic
        server.shutdown();
    }

    #[tokio::test]
    async fn test_monolithic_manifest_prefetch_dispatches_expensive_entries() {
        // A manifest with an entry above the cost threshold is kept, so the
        // function builds prefetch keys and dispatches handle_prefetch. Covers
        // the worth-prefetching dispatch path (daemon.rs 2986-2994).
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        let remote = test_remote_config();
        config.remote = Some(remote.clone());

        // handle_prefetch validates the key as 64 hex chars.
        let key = "abcdef0123456789".repeat(4);
        let manifest = crate::remote::BuildManifest {
            version: 3,
            created: "2025-01-01T00:00:00Z".to_string(),
            manifest_key: crate::cli::default_manifest_key(),
            entries: vec![crate::remote::ManifestEntry {
                cache_key: key.clone(),
                crate_name: "expensive".to_string(),
                compile_time_ms: 5000, // above the 1000ms default -> kept
                artifact_size: 100,
            }],
        };
        let body = serde_json::to_vec(&manifest).unwrap();
        // Manifest GET, then garbage for the background pack download (which may
        // fail — dispatch is what we're covering, and handle_prefetch returns ok).
        let mut events = vec![ReplayedEvent::with_body(body)];
        events.extend(std::iter::repeat_with(|| ReplayedEvent::with_body(b"nope")).take(6));
        let (server, client) = mock_s3_client(events).await;
        let daemon = Arc::new(Daemon::new(config));

        monolithic_manifest_prefetch(&daemon, client.as_ref(), &remote).await; // dispatches + returns
        server.shutdown();
    }

    #[tokio::test]
    async fn test_shard_prefetch_all_shards_missing_returns_zero() {
        // A Cargo.lock with two deps -> compute_shards -> one download_shard GET
        // per shard. The mock NoSuchKey-404s every shard, so none match and the
        // prefetch queues nothing (Ok(0)). Covers shard computation + parallel
        // shard download + collection (miss path).
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let lock = dir.path().join("Cargo.lock");
        std::fs::write(
            &lock,
            "version = 3\n\n[[package]]\nname = \"serde\"\nversion = \"1.0.0\"\n\n\
             [[package]]\nname = \"tokio\"\nversion = \"1.0.0\"\n",
        )
        .unwrap();

        // One NoSuchKey 404 per shard GET (over-provisioned; concurrent/order-safe).
        let not_found = || ReplayedEvent::HttpResponse {
            status: 404,
            body: bytes::Bytes::from_static(
                b"<?xml version=\"1.0\"?><Error><Code>NoSuchKey</Code></Error>",
            ),
        };
        let events = std::iter::repeat_with(not_found).take(70).collect();
        let (server, client) = mock_s3_client(events).await;
        let daemon = Arc::new(Daemon::new(config));

        let count = shard_prefetch(&daemon, &client, "prefix", "ns", &lock)
            .await
            .expect("shard prefetch should succeed");
        assert_eq!(count, 0, "no shards matched -> nothing queued");
        server.shutdown();
    }

    // ── New protocol types serde tests ────────────────────────────

    #[test]
    fn test_batch_remote_check_request_serde() {
        let req = Request::BatchRemoteCheck(BatchRemoteCheckRequest {
            checks: vec![
                RemoteCheckRequest {
                    key: "key1".into(),
                    entry_dir: "/tmp/key1".into(),
                    crate_name: String::new(),
                },
                RemoteCheckRequest {
                    key: "key2".into(),
                    entry_dir: "/tmp/key2".into(),
                    crate_name: String::new(),
                },
            ],
        });
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, parsed);

        assert!(json.contains("\"batch_remote_check\""));
        assert!(json.contains("\"key1\""));
        assert!(json.contains("\"key2\""));
    }

    #[test]
    fn test_prefetch_request_serde() {
        let req = Request::Prefetch(PrefetchRequest {
            keys: vec![
                ("key_a".into(), "serde".into()),
                ("key_b".into(), "tokio".into()),
            ],
        });
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, parsed);

        assert!(json.contains("\"prefetch\""));
        assert!(json.contains("\"key_a\""));
    }

    #[test]
    fn test_hash_files_request_serde() {
        let req = Request::HashFiles(HashFilesRequest {
            files: vec![HashFileRequest {
                path: "/tmp/libfoo.rlib".into(),
                size: 123,
                mtime_ns: 456,
                ctime_ns: 789,
                inode: 1011,
            }],
        });
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, parsed);
        assert!(json.contains("\"hash_files\""));
    }

    #[test]
    fn test_prefetch_request_empty_keys_serde() {
        let req = Request::Prefetch(PrefetchRequest { keys: vec![] }); // empty vec of (String, String)
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, parsed);
    }

    #[test]
    fn test_prefetch_request_from_plan() {
        let valid_key = "a".repeat(64);
        let plan = PrefetchPlan {
            plan_id: Some("plan-1".into()),
            planner: Some("fallback".into()),
            disposition: PrefetchDisposition::Execute,
            candidates: vec![
                kache_core::PrefetchCandidate {
                    cache_key: valid_key.clone(),
                    crate_name: "serde".into(),
                },
                // Malformed key from an untrusted planner: must be dropped.
                kache_core::PrefetchCandidate {
                    cache_key: "../../../etc/passwd".into(),
                    crate_name: "serde".into(),
                },
                // Valid key but path-escaping crate name: must be dropped.
                kache_core::PrefetchCandidate {
                    cache_key: valid_key.clone(),
                    crate_name: "../evil".into(),
                },
            ],
        };

        let req = PrefetchRequest::from_plan(plan);
        assert_eq!(req.keys, vec![(valid_key, "serde".into())]);
    }

    #[test]
    fn test_batch_response_serde() {
        let batch = BatchResponse {
            ok: true,
            results: vec![Response::found(true), Response::found(false)],
            error: None,
        };
        let json = serde_json::to_string(&batch).unwrap();
        let parsed: BatchResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(batch, parsed);
        assert_eq!(parsed.results.len(), 2);
        assert_eq!(parsed.results[0].found, Some(true));
        assert_eq!(parsed.results[1].found, Some(false));
    }

    // ── Warming barrier tests ─────────────────────────────────────

    #[tokio::test]
    async fn test_wait_for_warming_already_signaled() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon = Daemon::new(config);
        daemon.signal_warming_complete();

        // Should return immediately — no timeout hit
        let start = std::time::Instant::now();
        assert!(daemon.wait_for_warming(Duration::from_millis(100)).await);
        assert!(start.elapsed() < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn test_wait_for_warming_blocks_then_signals() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon = Arc::new(Daemon::new(config));

        let d = daemon.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            d.signal_warming_complete();
        });

        let start = std::time::Instant::now();
        assert!(daemon.wait_for_warming(Duration::from_secs(5)).await);
        let elapsed = start.elapsed();
        // Should have waited ~50ms, not the full 5s timeout
        assert!(elapsed >= Duration::from_millis(30));
        assert!(elapsed < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn test_wait_for_warming_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon = Daemon::new(config);

        // Never signal — should hit timeout
        let start = std::time::Instant::now();
        assert!(!daemon.wait_for_warming(Duration::from_millis(100)).await);
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(90));
        assert!(elapsed < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn test_wait_for_warming_multiple_waiters() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon = Arc::new(Daemon::new(config));

        let d1 = daemon.clone();
        let d2 = daemon.clone();
        let h1 = tokio::spawn(async move { d1.wait_for_warming(Duration::from_secs(5)).await });
        let h2 = tokio::spawn(async move { d2.wait_for_warming(Duration::from_secs(5)).await });

        tokio::time::sleep(Duration::from_millis(50)).await;
        daemon.signal_warming_complete();

        // Both waiters should resolve
        let (r1, r2) = tokio::join!(h1, h2);
        assert!(r1.unwrap());
        assert!(r2.unwrap());
    }

    #[test]
    fn test_remote_health_degrades_after_threshold_and_recovers_on_success() {
        let health = RemoteHealth::new();

        health.note_head_probe_failure("boom-1");
        health.note_head_probe_failure("boom-2");
        assert!(!health.head_probe_is_degraded());

        health.note_head_probe_failure("boom-3");
        assert!(health.head_probe_is_degraded());

        health.note_head_probe_suppressed();
        health.note_head_probe_success();
        assert!(!health.head_probe_is_degraded());
        assert_eq!(health.head_probe_failures.load(Ordering::Acquire), 0);
        assert_eq!(health.suppressed_head_probes.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn test_handle_remote_check_skips_head_when_probe_circuit_is_open() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(crate::config::RemoteConfig {
            bucket: "test".into(),
            endpoint: Some("http://localhost:9000".into()),
            region: "us-east-1".into(),
            prefix: "artifacts".into(),
            profile: None,
        });
        let daemon = Daemon::new(config);
        daemon.signal_warming_complete();

        daemon.remote_health.note_head_probe_failure("boom-1");
        daemon.remote_health.note_head_probe_failure("boom-2");
        daemon.remote_health.note_head_probe_failure("boom-3");

        let req = RemoteCheckRequest {
            key: "k".into(),
            entry_dir: "/tmp/test".into(),
            crate_name: "crate".into(),
        };
        let resp = daemon.handle_remote_check(&req).await;
        assert!(resp.ok);
        assert_eq!(resp.found, Some(false));
        assert_eq!(
            daemon
                .remote_health
                .suppressed_head_probes
                .load(Ordering::Acquire),
            1
        );
    }

    #[tokio::test]
    async fn test_handle_remote_check_authoritative_key_cache_skips_s3() {
        // A freshly-populated key cache that doesn't contain the requested key is
        // authoritative (age <= KEY_CACHE_AUTHORITATIVE_FOR): the daemon answers
        // a definitive miss without ever touching S3. Covers handle_remote_check's
        // Some(false)+authoritative branch. No mock needed — it returns before
        // get_s3_client().
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(crate::config::RemoteConfig {
            bucket: "test".into(),
            endpoint: Some("http://localhost:9000".into()),
            region: "us-east-1".into(),
            prefix: "artifacts".into(),
            profile: None,
        });
        let daemon = Daemon::new(config);
        daemon.signal_warming_complete();

        // Populate with a *different* key so the cache is fresh and authoritative
        // but the requested key is a known absence.
        let present = "a".repeat(64);
        let mut keys = HashMap::new();
        keys.insert(present.clone(), "othercrate".to_string());
        daemon.key_cache.populate(keys).await;

        let missing = "b".repeat(64);
        let req = RemoteCheckRequest {
            key: missing,
            entry_dir: "/tmp/test".into(),
            crate_name: "crate".into(),
        };
        let resp = daemon.handle_remote_check(&req).await;
        assert!(resp.ok);
        assert_eq!(
            resp.found,
            Some(false),
            "fresh key cache should authoritatively report the missing key as not found"
        );
        // The authoritative short-circuit must NOT have suppressed a HEAD probe —
        // it never reached the degraded-health path.
        assert_eq!(
            daemon
                .remote_health
                .suppressed_head_probes
                .load(Ordering::Acquire),
            0
        );
    }

    // ── Prefetch handler tests ────────────────────────────────────

    #[tokio::test]
    async fn test_handle_prefetch_no_remote() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path()); // remote = None
        let daemon = Arc::new(Daemon::new(config));

        let req = PrefetchRequest {
            keys: vec![("k".into(), "mycrate".into())],
        };
        let resp = daemon.handle_prefetch(&req).await;
        assert!(!resp.ok);
        assert!(
            resp.error
                .as_deref()
                .unwrap()
                .contains("no remote configured")
        );
    }

    // ── Upload queue tests ────────────────────────────────────────

    #[tokio::test]
    async fn test_handle_upload_with_queue_returns_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(crate::config::RemoteConfig {
            bucket: "test".into(),
            endpoint: Some("http://localhost:9000".into()),
            region: "us-east-1".into(),
            prefix: "artifacts".into(),
            profile: None,
        });

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<UploadJob>();
        let daemon = Daemon::new(config);
        daemon.set_upload_tx(tx);

        let job = UploadJob {
            key: "test_key".into(),
            entry_dir: "/tmp/test".into(),
            crate_name: String::new(),
            client_epoch: 0,
        };

        // Should return ok immediately (queued, not executed)
        let resp = daemon.handle_upload(&job).await;
        assert!(resp.ok);
        assert!(resp.error.is_none());
    }

    #[tokio::test]
    async fn test_handle_upload_queue_closed() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(crate::config::RemoteConfig {
            bucket: "test".into(),
            endpoint: Some("http://localhost:9000".into()),
            region: "us-east-1".into(),
            prefix: "artifacts".into(),
            profile: None,
        });

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<UploadJob>();
        let daemon = Daemon::new(config);
        daemon.set_upload_tx(tx);

        // Drop receiver to close the channel
        drop(rx);

        let job = UploadJob {
            key: "k1".into(),
            entry_dir: "/tmp/test".into(),
            crate_name: String::new(),
            client_epoch: 0,
        };
        let resp = daemon.handle_upload(&job).await;
        assert!(!resp.ok);
        assert!(resp.error.as_deref().unwrap().contains("queue closed"));
    }

    #[tokio::test]
    async fn test_handle_upload_dedup() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(crate::config::RemoteConfig {
            bucket: "test".into(),
            endpoint: Some("http://localhost:9000".into()),
            region: "us-east-1".into(),
            prefix: "artifacts".into(),
            profile: None,
        });

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<UploadJob>();
        let daemon = Daemon::new(config);
        daemon.set_upload_tx(tx);

        let job = UploadJob {
            key: "same-key".into(),
            entry_dir: "/tmp/test".into(),
            crate_name: String::new(),
            client_epoch: 0,
        };

        // First send succeeds and queues
        let resp1 = daemon.handle_upload(&job).await;
        assert!(resp1.ok);

        // Second send with same key is deduped (returns ok, not queued again)
        let resp2 = daemon.handle_upload(&job).await;
        assert!(resp2.ok);
    }

    #[tokio::test]
    async fn test_close_upload_queue_closes_buffer_with_daemon_clones_alive() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Arc::new(Daemon::new(test_config(dir.path())));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<UploadJob>();
        daemon.set_upload_tx(tx);

        // Upload workers hold Arc<Daemon> clones while they wait on the worker
        // channel. Closing the buffer must not rely on dropping those clones.
        let worker_daemon = daemon.clone();
        daemon.close_upload_queue();

        let recv = tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("upload buffer should close promptly after close_upload_queue");
        assert!(
            recv.is_none(),
            "upload buffer must close even while daemon clones remain alive"
        );
        drop(worker_daemon);
    }

    #[tokio::test]
    async fn test_handle_upload_after_queue_close_rejects_without_direct_upload() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(crate::config::RemoteConfig {
            bucket: "test".into(),
            endpoint: Some("http://localhost:9000".into()),
            region: "us-east-1".into(),
            prefix: "artifacts".into(),
            profile: None,
        });

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<UploadJob>();
        let daemon = Daemon::new(config);
        daemon.set_upload_tx(tx);
        daemon.close_upload_queue();

        let job = UploadJob {
            key: "late-key".into(),
            entry_dir: "/tmp/test".into(),
            crate_name: String::new(),
            client_epoch: 0,
        };
        let resp = daemon.handle_upload(&job).await;
        assert!(!resp.ok);
        assert!(resp.error.as_deref().unwrap().contains("queue closed"));
    }

    // ── Semaphore test ────────────────────────────────────────────

    #[test]
    fn test_semaphore_created_with_config() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.s3_concurrency = 4;

        let daemon = Daemon::new(config);
        assert_eq!(daemon.s3_semaphore.available_permits(), 4);
    }

    #[test]
    fn test_semaphore_min_one_permit() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.s3_concurrency = 0; // edge case

        let daemon = Daemon::new(config);
        assert_eq!(daemon.s3_semaphore.available_permits(), 1);
    }

    // ── Socket integration tests for new types ────────────────────

    #[tokio::test]
    async fn test_socket_prefetch_no_remote_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path()); // remote = None
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let daemon = Arc::new(Daemon::new(config));
        let resp = one_shot_request(
            &daemon,
            &socket_path,
            &Request::Prefetch(PrefetchRequest {
                keys: vec![("key1".into(), "mycrate".into())],
            }),
        )
        .await;

        assert!(!resp.ok);
        assert!(
            resp.error
                .as_deref()
                .unwrap()
                .contains("no remote configured")
        );
    }

    // ── S3KeyCache staleness tests ──────────────────────────────

    #[tokio::test]
    async fn test_key_cache_age_none_before_populate() {
        let cache = S3KeyCache::new();
        assert!(cache.age().await.is_none());
    }

    #[tokio::test]
    async fn test_key_cache_age_some_after_populate() {
        let cache = S3KeyCache::new();
        cache.populate(HashMap::new()).await;
        let age = cache.age().await;
        assert!(age.is_some());
        assert!(age.unwrap() < Duration::from_secs(1));
    }

    // ── BuildStarted protocol tests ─────────────────────────────

    #[test]
    fn test_build_started_request_serde() {
        let req = Request::BuildStarted(BuildStartedRequest {
            intent: kache_core::BuildIntent {
                crate_names: vec!["serde".into(), "tokio".into(), "anyhow".into()],
                namespace: Some("x86_64/hash/release".into()),
                cargo_lock_deps: vec![("serde".into(), "1.0.0".into())],
            },
            client_epoch: 0,
        });
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, parsed);

        assert!(json.contains("\"build_started\""));
        assert!(json.contains("\"serde\""));
        assert!(json.contains("\"tokio\""));
        assert!(json.contains("x86_64/hash/release"));
    }

    #[test]
    fn test_build_started_request_empty_serde() {
        let req = Request::BuildStarted(BuildStartedRequest {
            intent: kache_core::BuildIntent::default(),
            client_epoch: 0,
        });
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, parsed);
    }

    #[tokio::test]
    async fn test_send_build_started_client_roundtrip() {
        // CLIENT side: the fire-and-forget send_build_started reaches a live
        // in-process server and takes its Ok(()) success arm (daemon.rs
        // 3408-3411). No response is read (fire-and-forget), so a single accept
        // suffices.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let listener = bind_listener(&socket_path);
        let daemon = Arc::new(Daemon::new(config.clone()));
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept");
            let _ =
                handle_connection(stream, &daemon, &AtomicBool::new(false), &Notify::new()).await;
        });

        let cfg = config.clone();
        tokio::task::spawn_blocking(move || {
            send_build_started(
                &cfg,
                BuildStartedRequest {
                    intent: kache_core::BuildIntent {
                        crate_names: vec!["serde".into()],
                        ..Default::default()
                    },
                    client_epoch: 0,
                },
            )
        })
        .await
        .unwrap();
        // The server received and handled the hint without error.
        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_send_upload_job_client_roundtrip() {
        // CLIENT side: send_upload_job's first fire-and-forget try_send reaches a
        // live server and returns Ok(()) immediately (daemon.rs 3177-3178),
        // without the start-daemon/retry fallback. The server has an upload queue
        // so handle_upload enqueues cleanly.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let listener = bind_listener(&socket_path);
        let daemon = Arc::new(Daemon::new(config.clone()));
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<UploadJob>();
        daemon.set_upload_tx(tx);
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept");
            let _ =
                handle_connection(stream, &daemon, &AtomicBool::new(false), &Notify::new()).await;
        });

        let cfg = config.clone();
        let result = tokio::task::spawn_blocking(move || {
            send_upload_job(&cfg, &"a".repeat(64), Path::new("/tmp/test"), "serde")
        })
        .await
        .unwrap();
        server.await.unwrap();
        assert!(result.is_ok(), "upload job should send to a live daemon");
    }

    #[tokio::test]
    async fn test_send_prefetch_client_roundtrip() {
        // CLIENT side: send_prefetch's first fire-and-forget try_send reaches a
        // live server and returns Ok(()) immediately (daemon.rs 3369-3370),
        // without falling through to the start-daemon/retry path.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let listener = bind_listener(&socket_path);
        let daemon = Arc::new(Daemon::new(config.clone()));
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept");
            let _ =
                handle_connection(stream, &daemon, &AtomicBool::new(false), &Notify::new()).await;
        });

        let cfg = config.clone();
        let result = tokio::task::spawn_blocking(move || {
            send_prefetch(&cfg, &[("a".repeat(64), "serde".to_string())])
        })
        .await
        .unwrap();
        server.await.unwrap();
        assert!(result.is_ok(), "prefetch hint should send to a live daemon");
    }

    #[tokio::test]
    async fn test_handle_build_started_no_remote() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path()); // remote = None
        let daemon = Arc::new(Daemon::new(config));

        let req = BuildStartedRequest {
            intent: kache_core::BuildIntent {
                crate_names: vec!["mycrate".into()],
                ..Default::default()
            },
            client_epoch: 0,
        };
        let resp = daemon.handle_build_started(&req).await;
        assert!(!resp.ok);
        assert!(
            resp.error
                .as_deref()
                .unwrap()
                .contains("no remote configured")
        );
    }

    #[test]
    fn test_handle_request_sync_rejects_build_started() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon = Daemon::new(config);

        let req = Request::BuildStarted(BuildStartedRequest {
            intent: kache_core::BuildIntent {
                crate_names: vec!["c".into()],
                ..Default::default()
            },
            client_epoch: 0,
        });
        let resp = daemon.handle_request_sync(&req);
        assert!(!resp.ok);
        assert!(resp.error.as_deref().unwrap().contains("async"));
    }

    // ── Download dedup tests ────────────────────────────────────

    #[tokio::test]
    async fn test_downloading_map_starts_empty() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon = Daemon::new(config);
        assert!(daemon.downloading.read().await.is_empty());
    }

    /// Waiter-side wait, mirroring the pattern in `handle_remote_check`:
    /// register interest in the Notify FIRST (`enable`), re-check the map
    /// (skip waiting if the leader is already gone), then await the wakeup.
    async fn park_on_claim(map: &RwLock<HashMap<String, Arc<Notify>>>, notify: &Notify, key: &str) {
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if map.read().await.contains_key(key) {
            let _ = tokio::time::timeout(Duration::from_secs(10), notified).await;
        }
    }

    #[tokio::test]
    async fn downloading_guard_removes_key_via_runtime_when_lock_contended() {
        // Branch: DownloadingGuard contended-drop runtime fallback. The
        // spawned removal must both clear the key and wake waiters parked on
        // the key's Notify (notify runs AFTER the removal).
        let notify = Arc::new(Notify::new());
        let mut keys = HashMap::new();
        keys.insert("cache-key".to_string(), notify.clone());
        let map = Arc::new(RwLock::new(keys));

        let waiter = tokio::spawn({
            let map = map.clone();
            let notify = notify.clone();
            async move {
                park_on_claim(&map, &notify, "cache-key").await;
                // Woken by the async removal task: the key must already be gone.
                !map.read().await.contains_key("cache-key")
            }
        });
        // Let the waiter register with the Notify before the guard drops.
        tokio::time::sleep(Duration::from_millis(20)).await;

        let write_guard = map.write().await;
        let guard = DownloadingGuard::new(map.clone(), "cache-key".to_string());
        drop(guard);
        assert!(write_guard.contains_key("cache-key"));
        drop(write_guard);

        let mut removed = false;
        for _ in 0..20 {
            if !map.read().await.contains_key("cache-key") {
                removed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(removed, "contended drop should eventually remove the key");
        let key_gone_at_wake = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("waiter should be notified by the contended drop path")
            .unwrap();
        assert!(key_gone_at_wake, "wake must happen after the map removal");
    }

    #[tokio::test]
    async fn waiter_wakes_promptly_and_reclaims_when_leader_fails() {
        // A leader claims the key, a waiter parks on the claim's Notify, and
        // the leader's guard drops WITHOUT producing meta.json (failed
        // download). The waiter must wake promptly (not sit out a 30s budget)
        // and win the atomic re-claim.
        let map: Arc<RwLock<HashMap<String, Arc<Notify>>>> = Arc::new(RwLock::new(HashMap::new()));
        assert!(
            claim_download(&map, "k").await.is_none(),
            "first claim is the leader"
        );
        let leader_guard = DownloadingGuard::new(map.clone(), "k".to_string());
        let notify = claim_download(&map, "k")
            .await
            .expect("second claim is a waiter");

        let waiter = tokio::spawn({
            let map = map.clone();
            async move {
                let start = Instant::now();
                park_on_claim(&map, &notify, "k").await;
                let won = claim_download(&map, "k").await.is_none();
                (start.elapsed(), won)
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await; // let the waiter park
        drop(leader_guard); // leader fails: claim released, no meta.json
        let (elapsed, won) = waiter.await.unwrap();
        assert!(won, "waiter should win the re-claim after leader failure");
        assert!(
            elapsed < Duration::from_secs(5),
            "waiter should wake promptly, waited {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn exactly_one_waiter_wins_reclaim_after_leader_failure() {
        // Two waiters park behind the same leader; the leader fails. The
        // atomic insert-if-absent re-claim must elect exactly ONE new leader.
        // (The old poll-based code re-inserted the key IGNORING the result,
        // so both timed-out waiters proceeded as owners and double-downloaded
        // — the destructive-extraction hazard #213 guarded against.)
        let map: Arc<RwLock<HashMap<String, Arc<Notify>>>> = Arc::new(RwLock::new(HashMap::new()));
        assert!(claim_download(&map, "k").await.is_none());
        let leader_guard = DownloadingGuard::new(map.clone(), "k".to_string());
        let n1 = claim_download(&map, "k").await.unwrap();
        let n2 = claim_download(&map, "k").await.unwrap();

        let spawn_waiter = |notify: Arc<Notify>| {
            let map = map.clone();
            tokio::spawn(async move {
                park_on_claim(&map, &notify, "k").await;
                claim_download(&map, "k").await.is_none()
            })
        };
        let w1 = spawn_waiter(n1);
        let w2 = spawn_waiter(n2);

        tokio::time::sleep(Duration::from_millis(50)).await; // let both park
        drop(leader_guard);
        let (r1, r2) = tokio::join!(w1, w2);
        let wins = usize::from(r1.unwrap()) + usize::from(r2.unwrap());
        assert_eq!(wins, 1, "exactly one waiter must win the re-claim");
    }

    #[tokio::test]
    async fn waiter_sees_meta_json_at_wake_on_leader_success() {
        // Leader success path: the leader writes meta.json BEFORE its guard
        // drops. A woken waiter must observe the file (-> found) and not need
        // to re-claim.
        let dir = tempfile::tempdir().unwrap();
        let entry_dir = dir.path().join("entry");
        std::fs::create_dir_all(&entry_dir).unwrap();
        let meta = entry_dir.join("meta.json");

        let map: Arc<RwLock<HashMap<String, Arc<Notify>>>> = Arc::new(RwLock::new(HashMap::new()));
        assert!(claim_download(&map, "k").await.is_none());
        let leader_guard = DownloadingGuard::new(map.clone(), "k".to_string());
        let notify = claim_download(&map, "k").await.unwrap();

        let waiter = tokio::spawn({
            let map = map.clone();
            let meta = meta.clone();
            async move {
                park_on_claim(&map, &notify, "k").await;
                meta.exists()
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await; // let the waiter park
        std::fs::write(&meta, "{}").unwrap(); // leader lands the entry...
        drop(leader_guard); // ...then releases the claim
        let found = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("waiter should wake when the leader's guard drops")
            .unwrap();
        assert!(found, "waiter must observe meta.json at wake");
        assert!(map.read().await.is_empty(), "claim fully released");
    }

    // ── Bounded request-frame reader (#216) ─────────────────────────

    #[tokio::test]
    async fn read_bounded_line_strips_and_handles_eof() {
        let data = b"hello\nwith-cr\r\n\nlast"; // LF, CRLF, empty line, unterminated
        let mut reader = BufReader::new(&data[..]);
        let mut buf = Vec::new();
        let r = |res: std::io::Result<Option<String>>| res.unwrap();
        assert_eq!(
            r(read_bounded_line(&mut reader, &mut buf).await).as_deref(),
            Some("hello")
        );
        assert_eq!(
            r(read_bounded_line(&mut reader, &mut buf).await).as_deref(),
            Some("with-cr")
        );
        assert_eq!(
            r(read_bounded_line(&mut reader, &mut buf).await).as_deref(),
            Some("")
        );
        assert_eq!(
            r(read_bounded_line(&mut reader, &mut buf).await).as_deref(),
            Some("last")
        );
        // Clean EOF.
        assert_eq!(r(read_bounded_line(&mut reader, &mut buf).await), None);
    }

    #[tokio::test]
    async fn read_bounded_line_rejects_oversized_frame() {
        // A frame with no newline, larger than the cap, must be rejected
        // instead of buffered without limit.
        let big = vec![b'x'; MAX_REQUEST_FRAME_BYTES + 4096];
        let mut reader = BufReader::new(&big[..]);
        let mut buf = Vec::new();
        let err = read_bounded_line(&mut reader, &mut buf).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }
}
