use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::RwLock;

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
const DAEMON_COORD_STALE_AFTER: Duration = Duration::from_secs(15);
const VERSION: &str = crate::VERSION;

/// Compute a "build epoch" from the executable's mtime.
/// This changes every time `cargo build` produces a new binary,
/// giving us a cheap way to detect when the daemon is running stale code.
pub fn build_epoch() -> u64 {
    std::env::current_exe()
        .and_then(std::fs::metadata)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0)
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

fn process_is_alive(pid: u32) -> bool {
    let rc = unsafe { libc::kill(pid as i32, 0) };
    rc == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

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
    unsafe {
        libc::kill(pid as i32, libc::SIGTERM);
    }

    if wait_for_run_lock_release(socket_path, Duration::from_secs(1))? {
        return Ok(true);
    }

    unsafe {
        libc::kill(pid as i32, libc::SIGKILL);
    }

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
pub struct PrefetchRequest {
    /// (cache_key, crate_name) pairs
    pub keys: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BuildStartedRequest {
    pub crate_names: Vec<String>,
    /// Shard namespace (target/rustc_hash/profile). If set alongside cargo_lock_deps,
    /// enables shard-based prefetch instead of key-cache lookup.
    #[serde(default)]
    pub namespace: Option<String>,
    /// Sorted (name, version) pairs from Cargo.lock. Enables shard-based prefetch
    /// when the daemon receives a BuildStarted message.
    #[serde(default)]
    pub cargo_lock_deps: Vec<(String, String)>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct Response {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evicted: Option<usize>,
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
    pub error: Option<String>,
}

impl Response {
    fn ok() -> Self {
        Self {
            ok: true,
            evicted: None,
            found: None,
            prefetched: None,
            stats: None,
            batch_results: None,
            error: None,
        }
    }

    fn ok_evicted(n: usize) -> Self {
        Self {
            ok: true,
            evicted: Some(n),
            found: None,
            prefetched: None,
            stats: None,
            batch_results: None,
            error: None,
        }
    }

    fn ok_stats(stats: StatsResponse) -> Self {
        Self {
            ok: true,
            evicted: None,
            found: None,
            prefetched: None,
            stats: Some(stats),
            batch_results: None,
            error: None,
        }
    }

    fn ok_batch(results: Vec<Response>) -> Self {
        Self {
            ok: true,
            evicted: None,
            found: None,
            prefetched: None,
            stats: None,
            batch_results: Some(results),
            error: None,
        }
    }

    fn found(val: bool) -> Self {
        Self {
            ok: true,
            evicted: None,
            found: Some(val),
            prefetched: None,
            stats: None,
            batch_results: None,
            error: None,
        }
    }

    fn found_prefetched(val: bool, prefetched: bool) -> Self {
        Self {
            ok: true,
            evicted: None,
            found: Some(val),
            prefetched: Some(prefetched),
            stats: None,
            batch_results: None,
            error: None,
        }
    }

    fn err(msg: impl Into<String>) -> Self {
        Self {
            ok: false,
            evicted: None,
            found: None,
            prefetched: None,
            stats: None,
            batch_results: None,
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
    pub compressed_bytes: u64,
    pub elapsed_ms: u64,
    /// Time spent on S3 GET + body collection only (excludes decompression/disk I/O).
    #[serde(default)]
    pub network_ms: u64,
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
    /// Time spent on disk I/O (fs::write + permissions + atomic rename), ms.
    #[serde(default)]
    pub disk_io_ms: u64,
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
    1
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

const RECENT_TRANSFERS_CAP: usize = 50;

// ── S3 Key Cache ─────────────────────────────────────────────────

pub(crate) struct S3KeyCache {
    keys: RwLock<Option<HashSet<String>>>,
    /// Reverse index: crate_name → [cache_key, ...].
    /// Built from the S3 listing so the daemon can resolve crate names to cache
    /// keys without needing the local SQLite store (critical for cold CI runners).
    by_crate: RwLock<Option<HashMap<String, Vec<String>>>>,
    populated: AtomicBool,
    last_populated: RwLock<Option<Instant>>,
}

impl S3KeyCache {
    fn new() -> Self {
        Self {
            keys: RwLock::new(None),
            by_crate: RwLock::new(None),
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
        let guard = self.keys.read().await;
        guard.as_ref().map(|set| set.contains(key))
    }

    /// Look up cache keys for a crate name from the S3 listing.
    /// Returns empty vec if the cache is not yet populated.
    pub async fn keys_for_crate(&self, crate_name: &str) -> Vec<String> {
        if !self.populated.load(Ordering::Acquire) {
            return vec![];
        }
        let guard = self.by_crate.read().await;
        guard
            .as_ref()
            .and_then(|m| m.get(crate_name))
            .cloned()
            .unwrap_or_default()
    }

    /// Replace the entire key set (called after list_keys).
    /// Accepts the full cache_key → crate_name mapping from S3 and builds
    /// both a forward set (for `check`) and a reverse index (for `keys_for_crate`).
    pub async fn populate(&self, keys: HashMap<String, String>) {
        let mut by_crate_map: HashMap<String, Vec<String>> = HashMap::new();
        for (cache_key, crate_name) in &keys {
            by_crate_map
                .entry(crate_name.clone())
                .or_default()
                .push(cache_key.clone());
        }

        let key_set: HashSet<String> = keys.into_keys().collect();

        let mut guard = self.keys.write().await;
        *guard = Some(key_set);
        drop(guard);

        let mut crate_guard = self.by_crate.write().await;
        *crate_guard = Some(by_crate_map);
        drop(crate_guard);

        self.populated.store(true, Ordering::Release);
        let mut ts = self.last_populated.write().await;
        *ts = Some(Instant::now());
    }

    /// Insert a single key (called after successful upload).
    pub async fn insert(&self, key: String, crate_name: Option<&str>) {
        let mut guard = self.keys.write().await;
        if let Some(set) = guard.as_mut() {
            set.insert(key.clone());
        }
        drop(guard);

        if let Some(name) = crate_name {
            let mut crate_guard = self.by_crate.write().await;
            if let Some(map) = crate_guard.as_mut() {
                map.entry(name.to_string()).or_default().push(key);
            }
        }
    }
}

// ── Daemon (the "lib" — all business logic, no I/O) ─────────────

pub(crate) struct Daemon {
    config: Config,
    store: OnceLock<Mutex<Store>>,
    s3_client: tokio::sync::OnceCell<aws_sdk_s3::Client>,
    key_cache: Arc<S3KeyCache>,
    remote_health: Arc<RemoteHealth>,
    s3_semaphore: Arc<tokio::sync::Semaphore>,
    upload_tx: Option<tokio::sync::mpsc::UnboundedSender<UploadJob>>,
    /// Keys currently queued or in-flight for upload (dedup guard).
    pending_uploads: Arc<RwLock<HashSet<String>>>,
    downloading: Arc<RwLock<HashSet<String>>>,
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
    version: String,
    build_epoch: u64,
    transfer_counters: TransferCounters,
    recent_transfers: std::sync::Mutex<std::collections::VecDeque<TransferEvent>>,
}

impl Daemon {
    pub fn new(config: Config) -> Self {
        let permits = config.s3_concurrency.max(1) as usize;
        let (warming_tx, _) = tokio::sync::watch::channel(false);
        let (prefetch_cancel, _) = tokio::sync::watch::channel(false);
        Self {
            store: OnceLock::new(),
            s3_semaphore: Arc::new(tokio::sync::Semaphore::new(permits)),
            s3_client: tokio::sync::OnceCell::new(),
            key_cache: Arc::new(S3KeyCache::new()),
            remote_health: Arc::new(RemoteHealth::new()),
            upload_tx: None,
            pending_uploads: Arc::new(RwLock::new(HashSet::new())),
            downloading: Arc::new(RwLock::new(HashSet::new())),
            warming_tx,
            prefetched_keys: Arc::new(RwLock::new(HashSet::new())),
            prefetch_checks: Arc::new(AtomicU32::new(0)),
            prefetch_hits: Arc::new(AtomicU32::new(0)),
            prefetch_cancel,
            version: VERSION.to_string(),
            build_epoch: build_epoch(),
            transfer_counters: TransferCounters::new(),
            recent_transfers: std::sync::Mutex::new(std::collections::VecDeque::new()),
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

    fn with_store<T>(&self, f: impl FnOnce(&Store) -> Result<T>) -> Result<T> {
        let guard = self
            .store_lock()?
            .lock()
            .map_err(|_| anyhow::anyhow!("daemon store mutex poisoned"))?;
        f(&guard)
    }

    fn entry_dir_for(&self, cache_key: &str) -> PathBuf {
        self.config.store_dir().join(cache_key)
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
    pub fn set_upload_tx(&mut self, tx: tokio::sync::mpsc::UnboundedSender<UploadJob>) {
        self.upload_tx = Some(tx);
    }

    /// Lazy-init the S3 client (requires remote config).
    async fn get_s3_client(&self) -> Result<&aws_sdk_s3::Client> {
        self.s3_client
            .get_or_try_init(|| async {
                let remote = self
                    .config
                    .remote
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("no remote configured"))?;
                crate::remote::create_s3_client(remote).await
            })
            .await
    }

    /// Dispatch a parsed request to the appropriate handler (sync-only requests).
    #[cfg(test)]
    pub fn handle_request_sync(&self, req: &Request) -> Response {
        match req {
            Request::Gc(gc) => self.handle_gc(gc),
            Request::Stats(sr) => self.handle_stats(sr),
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
                misses: es.misses,
                errors: es.errors,
                total_elapsed_ms: es.total_elapsed_ms,
                hit_elapsed_ms: es.hit_elapsed_ms,
                miss_elapsed_ms: es.miss_elapsed_ms,
                hit_compile_time_ms: es.hit_compile_time_ms,
                miss_compile_time_ms: es.miss_compile_time_ms,
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
        })
    }

    /// Handle a GC request — pure logic against the store.
    pub fn handle_gc(&self, req: &GcRequest) -> Response {
        match self.run_gc(req.max_age_hours) {
            Ok(stats) => Response::ok_evicted(stats.entries_evicted),
            Err(e) => Response::err(format!("gc failed: {e}")),
        }
    }

    /// Handle an upload job. If the upload queue is available, pushes to it (non-blocking).
    /// Otherwise falls back to direct upload (used in tests).
    pub async fn handle_upload(&self, job: &UploadJob) -> Response {
        if self.config.remote.is_none() {
            return Response::err("no remote configured");
        }

        // If upload buffer is set up (server mode), push to it for async processing
        if let Some(tx) = &self.upload_tx {
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

        // Fallback: direct upload (no queue available)
        let Ok(_permit) = self.s3_semaphore.acquire().await else {
            return Response::err("S3 semaphore closed");
        };
        self.do_upload(job).await
    }

    /// Execute an upload directly (used by upload queue workers).
    pub async fn do_upload(&self, job: &UploadJob) -> Response {
        let key_short = &job.key[..job.key.len().min(16)];
        let Some(remote) = &self.config.remote else {
            return Response::err("no remote configured");
        };

        let client = match self.get_s3_client().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    crate_name = job.crate_name,
                    key = key_short,
                    "S3 client init failed: {e:#}"
                );
                return Response::err(format!("S3 client init failed: {e:#}"));
            }
        };
        let plan = crate::remote_plan::RemotePlanner::new(&self.config)
            .plan(crate::remote_plan::RemoteWorkload::BackgroundUpload);
        let layout = plan.layout(client, remote);

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
                    compressed_bytes: ul.transfer.compressed_bytes,
                    elapsed_ms,
                    network_ms: ul.transfer.network_ms,
                    request_ms: 0,
                    body_ms: 0,
                    request_count: 0,
                    original_bytes: 0,
                    decompress_ms: 0,
                    disk_io_ms: 0,
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
                    compressed_bytes: 0,
                    elapsed_ms,
                    network_ms: 0,
                    request_ms: 0,
                    body_ms: 0,
                    request_count: 0,
                    original_bytes: 0,
                    decompress_ms: 0,
                    disk_io_ms: 0,
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

        let client = match self.get_s3_client().await {
            Ok(c) => c,
            Err(e) => return Response::err(format!("S3 client init failed: {e}")),
        };

        let plan = crate::remote_plan::RemotePlanner::new(&self.config)
            .plan(crate::remote_plan::RemoteWorkload::RestoreCheck);
        let layout = plan.layout(client, remote);

        if needs_head_probe {
            let Ok(_permit) = self.s3_semaphore.acquire().await else {
                return Response::err("S3 semaphore closed");
            };
            match layout.exists_entry(&req.key, cn).await {
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
                    let error = format!("S3 exists check failed: {e}");
                    self.remote_health.note_head_probe_failure(&error);
                    return Response::found(false);
                }
            }
        }

        // Check download dedup — if another task is already downloading this key, wait for it
        {
            let guard = self.downloading.read().await;
            if guard.contains(&req.key) {
                drop(guard);
                tracing::debug!("already downloading {}, waiting for completion", &req.key);
                // Poll until the in-flight download finishes (up to 30s)
                for _ in 0..300 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    if !self.downloading.read().await.contains(&req.key) {
                        break;
                    }
                }
                // Check if the entry is now available on disk
                let entry_dir = PathBuf::from(&req.entry_dir);
                if entry_dir.join("meta.json").exists() {
                    let was_prefetched = self.prefetched_keys.read().await.contains(&req.key);
                    return Response::found_prefetched(true, was_prefetched);
                }
                // Download failed or timed out — fall through to retry ourselves
            }
        }
        self.downloading.write().await.insert(req.key.clone());

        // Acquire semaphore for download
        let Ok(_permit) = self.s3_semaphore.acquire().await else {
            self.downloading.write().await.remove(&req.key);
            return Response::err("S3 semaphore closed");
        };

        // Download to local store using the current remote layout.
        let entry_dir = PathBuf::from(&req.entry_dir);
        let blobs_dir = self.config.store_dir().join("blobs");
        let start = Instant::now();
        let download_result = layout
            .download_entry(&req.key, cn, &entry_dir, &blobs_dir)
            .await;

        let result = match download_result {
            Ok(dl) => {
                let elapsed_ms = start.elapsed().as_millis() as u64;
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
                    compressed_bytes: dl.compressed_bytes,
                    elapsed_ms,
                    network_ms: dl.network_ms,
                    request_ms: dl.request_ms,
                    body_ms: dl.body_ms,
                    request_count: dl.request_count,
                    original_bytes: dl.original_bytes,
                    decompress_ms: dl.decompress_ms,
                    disk_io_ms: dl.disk_io_ms,
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
                // Commit to SQLite so store.get() can find it
                if let Err(e) = self.with_store(|store| store.import_restored_entry(&req.key)) {
                    tracing::warn!("failed to import downloaded entry {}: {e}", &req.key);
                }
                Response::found(true)
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
                    compressed_bytes: 0,
                    elapsed_ms,
                    network_ms: 0,
                    request_ms: 0,
                    body_ms: 0,
                    request_count: 0,
                    original_bytes: 0,
                    decompress_ms: 0,
                    disk_io_ms: 0,
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
        };
        self.downloading.write().await.remove(&req.key);
        result
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

        if self.get_s3_client().await.is_err() {
            return Response::err("S3 client init failed");
        }

        // Filter to keys that need downloading: (cache_key, crate_name, entry_dir)
        let mut keys_to_fetch: Vec<(String, String, PathBuf)> = Vec::new();
        let downloading_guard = self.downloading.read().await;
        for (key, crate_name) in &req.keys {
            let entry_dir = self.entry_dir_for(key);
            if entry_dir.exists() {
                continue;
            }
            if downloading_guard.contains(key) {
                continue;
            }
            if let Some(false) = self.key_cache.check(key).await {
                continue;
            }
            keys_to_fetch.push((key.clone(), crate_name.clone(), entry_dir));
        }
        drop(downloading_guard);

        // If empty keys were sent, fetch all S3 keys missing locally
        if req.keys.is_empty()
            && let Ok(client) = self.get_s3_client().await
            && let Ok(s3_keys) = crate::remote_plan::RemotePlanner::new(&self.config)
                .plan(crate::remote_plan::RemoteWorkload::KeyDiscovery)
                .layout(client, remote)
                .list_keys()
                .await
        {
            for (key, crate_name) in s3_keys {
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

        // Mark keys as downloading
        {
            let mut guard = self.downloading.write().await;
            for (key, _, _) in &keys_to_fetch {
                guard.insert(key.clone());
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
            let max_concurrent = daemon.s3_semaphore.available_permits().max(1);

            let mut keys_iter = keys_to_fetch.into_iter().peekable();
            while let Some((key, crate_name, entry_dir)) = keys_iter.next() {
                // Check for adaptive cancellation
                if *cancel_rx.borrow() {
                    tracing::info!("prefetch: cancelled by adaptive hit-rate check");
                    // Remove this key + all remaining keys from downloading set
                    let mut guard = daemon.downloading.write().await;
                    guard.remove(&key);
                    for (k, _, _) in keys_iter {
                        guard.remove(&k);
                    }
                    drop(guard);
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
                    let Ok(_permit) = sem.acquire().await else {
                        tracing::warn!("prefetch: semaphore closed for {}", key);
                        d.downloading.write().await.remove(&key);
                        return;
                    };
                    let client = match d.get_s3_client().await {
                        Ok(c) => c,
                        Err(_) => {
                            d.downloading.write().await.remove(&key);
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
                        .layout(client, &remote_cfg)
                        .download_entry(&key, &crate_name, &entry_dir, &blobs_dir)
                        .await;

                    match download_result {
                        Ok(dl) => {
                            let elapsed_ms = start.elapsed().as_millis() as u64;
                            d.transfer_counters
                                .downloads_completed
                                .fetch_add(1, Ordering::Relaxed);
                            d.transfer_counters
                                .bytes_downloaded
                                .fetch_add(dl.compressed_bytes, Ordering::Relaxed);
                            d.push_transfer_event(TransferEvent {
                                schema: default_transfer_schema(),
                                crate_name: crate_name.clone(),
                                direction: TransferDirection::Download,
                                format: dl.format.to_string(),
                                compressed_bytes: dl.compressed_bytes,
                                elapsed_ms,
                                network_ms: dl.network_ms,
                                request_ms: dl.request_ms,
                                body_ms: dl.body_ms,
                                request_count: dl.request_count,
                                original_bytes: dl.original_bytes,
                                decompress_ms: dl.decompress_ms,
                                disk_io_ms: dl.disk_io_ms,
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
                            if let Err(e) = d.with_store(|store| store.import_restored_entry(&key))
                            {
                                tracing::warn!("prefetch import failed for {}: {e}", key);
                            }
                            // Track as prefetched for PrefetchHit attribution
                            d.prefetched_keys.write().await.insert(key.clone());
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
                                compressed_bytes: 0,
                                elapsed_ms,
                                network_ms: 0,
                                request_ms: 0,
                                body_ms: 0,
                                request_count: 0,
                                original_bytes: 0,
                                decompress_ms: 0,
                                disk_io_ms: 0,
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
                    d.downloading.write().await.remove(&key);
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

    /// Handle a build-started hint: resolve crate names to cache keys and prefetch
    /// those that exist in S3 but are missing locally.
    ///
    /// Resolution order:
    /// 1. Local SQLite store (has exact key→crate mapping from previous builds)
    /// 2. S3 key cache reverse index (works on cold CI runners with empty local store)
    pub async fn handle_build_started(self: &Arc<Self>, req: &BuildStartedRequest) -> Response {
        let Some(_remote) = &self.config.remote else {
            return Response::err("no remote configured");
        };

        // 1. Try local SQLite first (has exact keys from previous builds)
        let entries = match self.with_store(|store| store.keys_for_crates(&req.crate_names)) {
            Ok(e) => e,
            Err(e) => return Response::err(format!("key lookup failed: {e}")),
        };

        // Track which crate names were resolved via SQLite
        let mut resolved_crates: HashSet<String> = HashSet::new();
        for (_, crate_name, _) in &entries {
            resolved_crates.insert(crate_name.clone());
        }

        // 2. For crates not in local SQLite, fall back to S3 key cache.
        // This is the critical path for cold CI runners where the local store is empty.
        let mut extra_entries = Vec::new();
        let unresolved: Vec<&String> = req
            .crate_names
            .iter()
            .filter(|n| !resolved_crates.contains(*n))
            .collect();

        if !unresolved.is_empty() {
            for name in &unresolved {
                let s3_keys = self.key_cache.keys_for_crate(name).await;
                for key in s3_keys {
                    let entry_dir = self.entry_dir_for(&key);
                    extra_entries.push((key, (*name).clone(), entry_dir));
                }
            }
            if !extra_entries.is_empty() {
                tracing::info!(
                    "build-started: resolved {} extra keys from S3 key cache \
                     ({} crates had no local history)",
                    extra_entries.len(),
                    unresolved.len()
                );
            }
        }

        // Merge both sources and filter to keys missing locally
        let all_entries = entries.into_iter().chain(extra_entries);
        let mut keys_to_prefetch = Vec::new();
        let downloading_guard = self.downloading.read().await;
        for (key, crate_name, entry_dir) in all_entries {
            if entry_dir.exists() {
                continue;
            }
            if downloading_guard.contains(&key) {
                continue;
            }
            match self.key_cache.check(&key).await {
                Some(false) => continue,
                Some(true) | None => keys_to_prefetch.push((key, crate_name)),
            }
        }
        drop(downloading_guard);

        if keys_to_prefetch.is_empty() {
            tracing::debug!(
                "build-started: nothing to prefetch ({} crate names checked)",
                req.crate_names.len()
            );
            return Response::ok();
        }

        tracing::info!(
            "build-started: prefetching {} keys for {} crates",
            keys_to_prefetch.len(),
            req.crate_names.len()
        );

        // Delegate to the existing prefetch machinery
        let prefetch_req = PrefetchRequest {
            keys: keys_to_prefetch,
        };
        self.handle_prefetch(&prefetch_req).await
    }

    /// After a successful upload, check if store exceeds max_size → LRU eviction.
    fn maybe_evict_after_upload(&self) {
        let _ = self.with_store(|store| {
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
        let (dedup_stats, evict_stats, incremental_cleaned) = self.with_store(|store| {
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

            Ok((dedup_stats, evict_stats, incremental_cleaned))
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
            bytes_freed: dedup_stats.bytes_freed + evict_stats.bytes_freed,
            blobs_removed: dedup_stats.blobs_removed + evict_stats.blobs_removed,
            duration_ms: start.elapsed().as_millis() as u64,
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

    use std::os::unix::io::AsRawFd;
    let got_lock =
        unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0;

    if !got_lock {
        tracing::info!("another daemon holds the run lock, exiting");
        return Ok(());
    }

    // Hold lock_file (and thus the flock) for the daemon's entire lifetime.
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

    // Stale socket detection: if file exists, try connecting — if it fails, remove it
    if socket_path.exists() {
        match UnixStream::connect(&socket_path).await {
            Ok(_) => {
                // Exit cleanly (code 0) so launchd/systemd KeepAlive doesn't
                // restart us in an infinite loop when the daemon is already up.
                tracing::info!(
                    "another daemon is already running (socket {} is active), exiting cleanly",
                    socket_path.display()
                );
                return Ok(());
            }
            Err(_) => {
                tracing::info!("removing stale socket {}", socket_path.display());
                std::fs::remove_file(&socket_path)?;
            }
        }
    }

    let listener = UnixListener::bind(&socket_path).context("binding Unix socket")?;
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

    let mut daemon_inner = Daemon::new(config.clone());
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
            if let Err(e) = gc_daemon.run_gc(None) {
                tracing::warn!("periodic GC failed: {e}");
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
                        if consecutive_refresh_failures == 1
                            || consecutive_refresh_failures.is_multiple_of(10)
                        {
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

    // Accept connections until shutdown signal
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    // Idle watchdog: exit if no connections received for this duration.
    // Prevents zombie daemons from accumulating when the user isn't building.
    // The daemon will be auto-started again on the next build.
    // Configurable via KACHE_DAEMON_IDLE_TIMEOUT or config.toml; 0 = disabled.
    let idle_timeout = if config.daemon_idle_timeout_secs > 0 {
        Some(Duration::from_secs(config.daemon_idle_timeout_secs))
    } else {
        None
    };
    let mut last_activity = Instant::now();

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
                match accept {
                    Ok((stream, _)) => {
                        last_activity = Instant::now();
                        let d = daemon.clone();
                        let flag = shutdown_flag.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(stream, &d, &flag).await {
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
                        // Re-check: a handler may have set shutdown_flag while
                        // we were in select! (e.g. client_epoch staleness).
                        if shutdown_flag.load(Ordering::Relaxed) {
                            tracing::info!("shutdown requested via client epoch, draining...");
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::warn!("accept error: {e}");
                    }
                }
            }
            // Wake periodically to check idle timeout (select won't fire otherwise)
            _ = tokio::time::sleep(Duration::from_secs(60)) => {}
            _ = &mut shutdown => {
                tracing::info!("shutdown signal received, draining...");
                break;
            }
        }
    }

    gc_handle.abort();
    if let Some(h) = cache_handle {
        h.abort();
    }
    if let Some(h) = manifest_handle {
        h.abort();
    }
    heartbeat_handle.abort();

    // Graceful shutdown: drop the daemon's sender to close the unbounded buffer,
    // which will cause the enqueue task to exit, closing the worker channel,
    // then wait for upload workers to drain (up to 30s) before aborting.
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

    // Clean up socket file
    let _ = std::fs::remove_file(&socket_path);
    tracing::info!("daemon stopped");
    Ok(())
}

/// Populate the S3 key cache by listing all keys in the bucket.
async fn populate_key_cache(daemon: &Daemon) -> Result<usize> {
    let client = daemon.get_s3_client().await?;
    let remote = daemon
        .config
        .remote
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no remote configured"))?;

    let keys = crate::remote_plan::RemotePlanner::new(&daemon.config)
        .plan(crate::remote_plan::RemoteWorkload::KeyDiscovery)
        .layout(client, remote)
        .list_keys()
        .await?;
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

    let client = match daemon.get_s3_client().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("manifest prefetch: S3 client init failed: {e}");
            return;
        }
    };

    // Try shard-based prefetch first if namespace is available
    if let Ok(namespace) = std::env::var("KACHE_NAMESPACE") {
        let lock_path = std::path::Path::new("Cargo.lock");
        if lock_path.exists() {
            match shard_prefetch(
                daemon,
                client,
                &remote.bucket,
                &remote.prefix,
                &namespace,
                lock_path,
            )
            .await
            {
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

    monolithic_manifest_prefetch(daemon, client, remote).await;
}

/// Shard-based prefetch: compute shard hashes from Cargo.lock, download matching shards
/// from S3 in parallel, collect cache keys.
async fn shard_prefetch(
    daemon: &Arc<Daemon>,
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    namespace: &str,
    lock_path: &std::path::Path,
) -> anyhow::Result<usize> {
    let deps = crate::shards::parse_cargo_lock(lock_path)?;
    let shard_set = crate::shards::compute_shards(namespace, &deps);

    tracing::info!(
        "shard prefetch: {} deps -> {} shards for namespace '{namespace}'",
        deps.len(),
        shard_set.shards.len()
    );

    // Download all shards in parallel
    let mut handles = Vec::new();
    for (hash, _entries) in &shard_set.shards {
        let c = client.clone();
        let b = bucket.to_string();
        let p = prefix.to_string();
        let ns = namespace.to_string();
        let h = hash.clone();
        handles.push(tokio::spawn(async move {
            crate::remote::download_shard(&c, &b, &p, &ns, &h).await
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
    client: &aws_sdk_s3::Client,
    remote: &crate::config::RemoteConfig,
) {
    let manifest_key =
        std::env::var("KACHE_MANIFEST_KEY").unwrap_or_else(|_| crate::cli::default_manifest_key());

    let min_compile_ms: u64 = std::env::var("KACHE_MIN_COMPILE_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);

    let manifest = match crate::remote::download_manifest(
        client,
        &remote.bucket,
        &remote.prefix,
        &manifest_key,
    )
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
    worth_prefetching.sort_by(|a, b| b.compile_time_ms.cmp(&a.compile_time_ms));

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

async fn handle_connection(
    stream: UnixStream,
    daemon: &Arc<Daemon>,
    shutdown_flag: &AtomicBool,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    loop {
        let line = match lines.next_line().await {
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
                    key = &job.key[..job.key.len().min(16)],
                    "handling upload request"
                );
                daemon.handle_upload(job).await
            }
            Ok(Request::Gc(req)) => daemon.handle_gc(&req),
            Ok(Request::RemoteCheck(req)) => daemon.handle_remote_check(&req).await,
            Ok(Request::Stats(req)) => daemon.handle_stats(&req),
            Ok(Request::BatchRemoteCheck(req)) => daemon.handle_batch_remote_check(&req).await,
            Ok(Request::Prefetch(req)) => daemon.handle_prefetch(&req).await,
            Ok(Request::BuildStarted(req)) => daemon.handle_build_started(&req).await,
            Ok(Request::Shutdown) => {
                shutdown_flag.store(true, Ordering::Relaxed);
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
        if client_epoch > 0
            && daemon.build_epoch > 0
            && client_epoch > daemon.build_epoch
            && !shutdown_flag.load(Ordering::Relaxed)
        {
            tracing::info!(
                daemon_epoch = daemon.build_epoch,
                client_epoch,
                "client binary is newer than daemon, scheduling restart"
            );
            shutdown_flag.store(true, Ordering::Relaxed);
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
        if let Err(e) = writer.write_all(resp_line.as_bytes()).await {
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

async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler");

    tokio::select! {
        _ = sigterm.recv() => {}
        _ = sigint.recv() => {}
    }
}

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

    let key_short = if key.len() > 16 { &key[..16] } else { key };

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
                    let jitter = (std::process::id() as u64 * 7) % 50;
                    let delay = Duration::from_millis(100 * u64::from(attempt) + jitter);
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

/// Send a GC request to the daemon. Auto-starts daemon if needed.
pub fn send_gc_request(config: &Config, max_age_hours: Option<u64>) -> Result<Option<usize>> {
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
                Ok(resp.evicted)
            } else {
                anyhow::bail!("daemon GC error: {}", resp.error.unwrap_or_default());
            }
        }
        Err(_) => {
            // Try auto-starting the daemon
            if start_daemon_background()? {
                let resp = try_send(&socket_path)?;
                if resp.ok {
                    Ok(resp.evicted)
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

pub fn send_remote_check(
    config: &Config,
    key: &str,
    entry_dir: &Path,
    crate_name: &str,
) -> Option<RemoteCheckResult> {
    let socket_path = config.socket_path();

    // Fast path: if the socket file doesn't exist, daemon is not running.
    // Skip the connect attempt entirely to avoid wasting time on a miss.
    if !socket_path.exists() {
        return None;
    }

    let req = Request::RemoteCheck(RemoteCheckRequest {
        key: key.to_string(),
        entry_dir: entry_dir.to_string_lossy().into_owned(),
        crate_name: crate_name.to_string(),
    });

    match send_request_with_timeout(&socket_path, &req, std::time::Duration::from_secs(3)) {
        Ok(resp_str) => match serde_json::from_str::<Response>(&resp_str) {
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
        },
        Err(e) => {
            tracing::debug!("remote check: daemon unreachable ({e})");
            None
        }
    }
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
                    let jitter = (std::process::id() as u64 * 7) % 50;
                    std::thread::sleep(Duration::from_millis(100 * u64::from(attempt) + jitter));
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
pub fn send_build_started(config: &Config, crate_names: &[String]) {
    let socket_path = config.socket_path();

    let req = Request::BuildStarted(BuildStartedRequest {
        crate_names: crate_names.to_vec(),
        namespace: None,
        cargo_lock_deps: vec![],
        client_epoch: build_epoch(),
    });

    match send_request_fire_and_forget(&socket_path, &req) {
        Ok(()) => {
            tracing::debug!("build-started hint sent for {} crates", crate_names.len());
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

    if client_epoch > 0 && stats.build_epoch > 0 && client_epoch > stats.build_epoch {
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
pub fn send_shutdown_request(config: &Config) -> Result<()> {
    let socket_path = config.socket_path();
    send_request_with_timeout(&socket_path, &Request::Shutdown, Duration::from_secs(5))?;
    eprintln!("daemon stopped");
    Ok(())
}

/// Best-effort restart for stale-daemon detection from stats polling.
/// This path is intentionally outside build hot paths, so a short bounded wait
/// is acceptable to keep monitor/status output current.
fn restart_daemon_for_stale_client(config: &Config) -> Result<bool> {
    let socket_path = config.socket_path();

    let _ = send_request_with_timeout(&socket_path, &Request::Shutdown, Duration::from_secs(2));

    // Give the old daemon a brief chance to exit before spawning a fresh one.
    for _ in 0..4 {
        if std::os::unix::net::UnixStream::connect(&socket_path).is_err() {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    start_daemon_background()
}

/// Send a request to the daemon via Unix socket, return the response line.
fn send_request(socket_path: &Path, req: &Request) -> Result<String> {
    send_request_with_timeout(socket_path, req, std::time::Duration::from_secs(30))
}

/// Send a request to the daemon via Unix socket with a configurable read timeout.
fn send_request_with_timeout(
    socket_path: &Path,
    req: &Request,
    read_timeout: std::time::Duration,
) -> Result<String> {
    use std::io::{BufRead, Write};
    use std::os::unix::net::UnixStream as StdUnixStream;

    let mut stream = StdUnixStream::connect(socket_path)
        .with_context(|| format!("connecting to daemon socket {}", socket_path.display()))?;

    stream.set_read_timeout(Some(read_timeout))?;
    stream.set_write_timeout(Some(std::time::Duration::from_secs(5)))?;

    let mut line = serde_json::to_string(req)?;
    line.push('\n');
    stream
        .write_all(line.as_bytes())
        .context("writing request to daemon")?;
    stream.flush().context("flushing request to daemon")?;

    // Shutdown the write half to signal we're done sending
    stream
        .shutdown(std::net::Shutdown::Write)
        .context("shutting down write half")?;

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
    use std::io::Write;
    use std::os::unix::net::UnixStream as StdUnixStream;

    let mut stream = StdUnixStream::connect(socket_path)
        .with_context(|| format!("connecting to daemon socket {}", socket_path.display()))?;

    stream.set_write_timeout(Some(std::time::Duration::from_secs(5)))?;

    let mut line = serde_json::to_string(req)?;
    line.push('\n');
    stream
        .write_all(line.as_bytes())
        .context("writing request to daemon")?;
    stream.flush().context("flushing request to daemon")?;

    // Don't read a response — just close.  The daemon will see EOF on the
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

        use std::os::unix::io::AsRawFd;
        let got_lock =
            unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0;

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
        if socket_path.exists() && std::os::unix::net::UnixStream::connect(&socket_path).is_ok() {
            let my_epoch = build_epoch();
            let is_stale = my_epoch > 0
                && send_request_with_timeout(
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
                .map(|s| s.build_epoch > 0 && my_epoch > s.build_epoch)
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
        if std::fs::metadata(&log_path).is_ok_and(|m| m.len() > 2 * 1024 * 1024) {
            let _ = std::fs::write(&log_path, b"--- log rotated ---\n");
        }
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

    use std::os::unix::io::AsRawFd;
    let got_lock =
        unsafe { libc::flock(run_lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0;

    if got_lock {
        unsafe { libc::flock(run_lock_file.as_raw_fd(), libc::LOCK_UN) };
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
        if socket_path.exists() && std::os::unix::net::UnixStream::connect(socket_path).is_ok() {
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

    if socket_path.exists() && std::os::unix::net::UnixStream::connect(socket_path).is_ok() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

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
            use std::os::unix::io::AsRawFd;
            assert_eq!(unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) }, 0);
            tx.send(()).unwrap();
            std::thread::sleep(hold_for);
            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
        });
        rx.recv().unwrap();
        handle
    }

    /// Helper: create a Config pointing at a tempdir.
    fn test_config(dir: &Path) -> Config {
        Config {
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
            let _listener = std::os::unix::net::UnixListener::bind(socket_path_bg).unwrap();
            std::thread::sleep(Duration::from_millis(200));
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
            let _listener = std::os::unix::net::UnixListener::bind(socket_path_bg).unwrap();
            std::thread::sleep(Duration::from_millis(200));
        });

        let mut child = std::process::Command::new("sh")
            .args(["-c", "exit 0"])
            .spawn()
            .unwrap();

        let ready =
            wait_for_socket_until(&socket_path, Some(&mut child), Duration::from_secs(1)).unwrap();

        handle.join().unwrap();
        assert!(ready);
    }

    #[test]
    fn test_wait_for_socket_until_kills_stuck_child_after_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("missing.sock");
        let mut child = std::process::Command::new("sh")
            .args(["-c", "sleep 30"])
            .spawn()
            .unwrap();

        let ready =
            wait_for_socket_until(&socket_path, Some(&mut child), Duration::from_millis(150))
                .unwrap();

        assert!(!ready);
        let status = child.try_wait().unwrap();
        assert!(status.is_some());
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

        let mut child = std::process::Command::new("sh")
            .args(["-c", "sleep 30"])
            .spawn()
            .unwrap();

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

        let mut child = std::process::Command::new("sh")
            .args(["-c", "sleep 30"])
            .spawn()
            .unwrap();

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

        let mut child = std::process::Command::new("sh")
            .args(["-c", "sleep 30"])
            .spawn()
            .unwrap();

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

    // ── Socket integration tests ─────────────────────────────────

    #[tokio::test]
    async fn test_socket_gc_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let daemon = Arc::new(Daemon::new(config));

        // Bind the socket
        let listener = UnixListener::bind(&socket_path).unwrap();

        // Spawn server task
        let server_daemon = daemon.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_connection(stream, &server_daemon, &AtomicBool::new(false))
                .await
                .unwrap();
        });

        // Client: connect and send a GC request
        let mut stream = UnixStream::connect(&socket_path).await.unwrap();

        let req = Request::Gc(GcRequest {
            max_age_hours: None,
        });
        let mut line = serde_json::to_string(&req).unwrap();
        line.push('\n');
        stream.write_all(line.as_bytes()).await.unwrap();
        stream.shutdown().await.unwrap();

        // Read response
        let mut reader = BufReader::new(stream);
        let mut resp_line = String::new();
        reader.read_line(&mut resp_line).await.unwrap();

        let resp: Response = serde_json::from_str(&resp_line).unwrap();
        assert!(resp.ok);
        assert_eq!(resp.evicted, Some(0));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_socket_remote_check_no_remote_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path()); // remote = None
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let daemon = Arc::new(Daemon::new(config));
        let listener = UnixListener::bind(&socket_path).unwrap();

        let server_daemon = daemon.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_connection(stream, &server_daemon, &AtomicBool::new(false))
                .await
                .unwrap();
        });

        let mut stream = UnixStream::connect(&socket_path).await.unwrap();

        let req = Request::RemoteCheck(RemoteCheckRequest {
            key: "test_key".into(),
            entry_dir: "/tmp/test".into(),
            crate_name: String::new(),
        });
        let mut line = serde_json::to_string(&req).unwrap();
        line.push('\n');
        stream.write_all(line.as_bytes()).await.unwrap();
        stream.shutdown().await.unwrap();

        let mut reader = BufReader::new(stream);
        let mut resp_line = String::new();
        reader.read_line(&mut resp_line).await.unwrap();

        let resp: Response = serde_json::from_str(&resp_line).unwrap();
        assert!(!resp.ok);
        assert!(
            resp.error
                .as_deref()
                .unwrap()
                .contains("no remote configured")
        );

        server.await.unwrap();
    }

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
                misses: 3,
                errors: 1,
                total_elapsed_ms: 5000,
                hit_elapsed_ms: 120,
                miss_elapsed_ms: 4880,
                hit_compile_time_ms: 22000,
                miss_compile_time_ms: 9000,
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
                misses: 0,
                errors: 0,
                total_elapsed_ms: 0,
                hit_elapsed_ms: 0,
                miss_elapsed_ms: 0,
                hit_compile_time_ms: 0,
                miss_compile_time_ms: 0,
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
        let listener = UnixListener::bind(&socket_path).unwrap();

        let server_daemon = daemon.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_connection(stream, &server_daemon, &AtomicBool::new(false))
                .await
                .unwrap();
        });

        let mut stream = UnixStream::connect(&socket_path).await.unwrap();

        let req = Request::Stats(StatsRequest {
            include_entries: true,
            sort_by: Some("size".into()),
            event_hours: Some(24),
            client_epoch: 0,
        });
        let mut line = serde_json::to_string(&req).unwrap();
        line.push('\n');
        stream.write_all(line.as_bytes()).await.unwrap();
        stream.shutdown().await.unwrap();

        let mut reader = BufReader::new(stream);
        let mut resp_line = String::new();
        reader.read_line(&mut resp_line).await.unwrap();

        let resp: Response = serde_json::from_str(&resp_line).unwrap();
        assert!(resp.ok);
        let stats = resp.stats.unwrap();
        assert_eq!(stats.total_size, 0);
        assert_eq!(stats.entry_count, 0);
        assert!(stats.entries.unwrap().is_empty());

        server.await.unwrap();
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
    fn test_prefetch_request_empty_keys_serde() {
        let req = Request::Prefetch(PrefetchRequest { keys: vec![] }); // empty vec of (String, String)
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, parsed);
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
        let mut daemon = Daemon::new(config);
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
        let mut daemon = Daemon::new(config);
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
        let mut daemon = Daemon::new(config);
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
        let listener = UnixListener::bind(&socket_path).unwrap();

        let server_daemon = daemon.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_connection(stream, &server_daemon, &AtomicBool::new(false))
                .await
                .unwrap();
        });

        let mut stream = UnixStream::connect(&socket_path).await.unwrap();

        let req = Request::Prefetch(PrefetchRequest {
            keys: vec![("key1".into(), "mycrate".into())],
        });
        let mut line = serde_json::to_string(&req).unwrap();
        line.push('\n');
        stream.write_all(line.as_bytes()).await.unwrap();
        stream.shutdown().await.unwrap();

        let mut reader = BufReader::new(stream);
        let mut resp_line = String::new();
        reader.read_line(&mut resp_line).await.unwrap();

        let resp: Response = serde_json::from_str(&resp_line).unwrap();
        assert!(!resp.ok);
        assert!(
            resp.error
                .as_deref()
                .unwrap()
                .contains("no remote configured")
        );

        server.await.unwrap();
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
            crate_names: vec!["serde".into(), "tokio".into(), "anyhow".into()],
            namespace: None,
            cargo_lock_deps: vec![],
            client_epoch: 0,
        });
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, parsed);

        assert!(json.contains("\"build_started\""));
        assert!(json.contains("\"serde\""));
        assert!(json.contains("\"tokio\""));
    }

    #[test]
    fn test_build_started_request_empty_serde() {
        let req = Request::BuildStarted(BuildStartedRequest {
            crate_names: vec![],
            namespace: None,
            cargo_lock_deps: vec![],
            client_epoch: 0,
        });
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, parsed);
    }

    #[tokio::test]
    async fn test_handle_build_started_no_remote() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path()); // remote = None
        let daemon = Arc::new(Daemon::new(config));

        let req = BuildStartedRequest {
            crate_names: vec!["mycrate".into()],
            namespace: None,
            cargo_lock_deps: vec![],
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
            crate_names: vec!["c".into()],
            namespace: None,
            cargo_lock_deps: vec![],
            client_epoch: 0,
        });
        let resp = daemon.handle_request_sync(&req);
        assert!(!resp.ok);
        assert!(resp.error.as_deref().unwrap().contains("async"));
    }

    // ── Download dedup tests ────────────────────────────────────

    #[tokio::test]
    async fn test_downloading_set_starts_empty() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon = Daemon::new(config);
        assert!(daemon.downloading.read().await.is_empty());
    }
}
