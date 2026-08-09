use crate::transport::prelude::*;
use crate::transport::{ListenerOptions, TokioListener, TokioStream, socket_name};
use anyhow::{Context, Result};
use kache_core::{PrefetchDisposition, PrefetchPlan};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{Notify, RwLock};

use crate::config::{Config, UPLOAD_SPOOL_MAX_JOBS};
use crate::events;
use crate::remote_resilience::{
    KeyedSingleflight, NegativeKeyCache, RemoteBreaker, RemoteDeadline, RemoteErrorClass,
    RemoteOperation, SingleflightClaim, classify_remote_error,
};
use crate::store::Store;

const KEY_CACHE_AUTHORITATIVE_MULTIPLIER: u64 = 5;
// A slower LIST cadence must not let stale negative entries suppress exact
// remote HEAD checks longer than the original 60s × 5 trust window.
const KEY_CACHE_AUTHORITATIVE_MAX_AGE: Duration = Duration::from_secs(300);
const REMOTE_CHECK_WARMING_GRACE: Duration = Duration::from_millis(750);
const REMOTE_CHECK_SINGLEFLIGHT_MAX_KEYS: usize = 4096;
// A synchronous compiler-wrapper demand has historically been best-effort for
// at most three seconds. Keep that hard build-path bound across mixed-version
// clients and daemons; the daemon's remote timeout may tighten it, never
// lengthen it.
const REMOTE_CHECK_LEGACY_BUDGET_MS: u64 = 3_000;
const UPLOAD_SPOOL_MAX_BYTES: u64 = 64 * 1024;
const UPLOAD_RETRY_DELAY: Duration = Duration::from_secs(5);

fn remote_check_budget_ms(configured_secs: u64, client_ms: Option<u64>) -> u64 {
    let configured_ms = if configured_secs == 0 {
        REMOTE_CHECK_LEGACY_BUDGET_MS
    } else {
        configured_secs
            .saturating_mul(1_000)
            .min(REMOTE_CHECK_LEGACY_BUDGET_MS)
    };
    let client_ms = client_ms
        .filter(|milliseconds| *milliseconds != 0)
        .unwrap_or(REMOTE_CHECK_LEGACY_BUDGET_MS)
        .min(REMOTE_CHECK_LEGACY_BUDGET_MS);
    configured_ms.min(client_ms)
}

fn key_cache_miss_is_authoritative(refresh_secs: u64, age: Option<Duration>) -> bool {
    if refresh_secs == 0 {
        return false;
    }
    let refresh_window =
        Duration::from_secs(refresh_secs.saturating_mul(KEY_CACHE_AUTHORITATIVE_MULTIPLIER));
    let authoritative_for = refresh_window.min(KEY_CACHE_AUTHORITATIVE_MAX_AGE);
    matches!(age, Some(age) if age <= authoritative_for)
}

fn speculative_prefetch_disabled(prefetch_enabled: bool) -> bool {
    !prefetch_enabled
}

fn should_start_speculative_prefetch(remote_configured: bool, prefetch_enabled: bool) -> bool {
    remote_configured && prefetch_enabled
}

fn key_cache_periodic_refresh_disabled(refresh_secs: u64) -> bool {
    refresh_secs == 0
}
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
    /// Legacy GC wire command accepted from older clients.
    Gc(GcRequest),
    /// Policy-v2 GC command. Older daemons reject this unknown variant before
    /// mutation, closing the capability-probe/replacement race.
    GcV2(GcRequest),
    RemoteCheck(RemoteCheckRequest),
    Stats(StatsRequest),
    BatchRemoteCheck(BatchRemoteCheckRequest),
    HashFiles(HashFilesRequest),
    LocalLookup(LocalLookupRequest),
    Prefetch(PrefetchRequest),
    BuildStarted(BuildStartedRequest),
    CompileStarted(CompileStartedRequest),
    CompileFinished(CompileFinishedRequest),
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

fn upload_spool_path(config: &Config, key: &str) -> PathBuf {
    config.upload_spool_dir().join(format!("{key}.json"))
}

fn normalize_upload_job(config: &Config, job: &UploadJob) -> Result<UploadJob> {
    if !crate::cache_key::is_valid_cache_key(&job.key) {
        anyhow::bail!("invalid upload cache key");
    }
    if !crate::cache_key::is_valid_crate_name(&job.crate_name) {
        anyhow::bail!("invalid upload crate name");
    }
    Ok(UploadJob {
        key: job.key.clone(),
        entry_dir: config.store_dir().join(&job.key).display().to_string(),
        crate_name: job.crate_name.clone(),
        client_epoch: job.client_epoch,
    })
}

/// Read and validate an already-published intent. Existing intents are never
/// replaced: the first durable publisher wins, and later wrapper/daemon calls
/// reuse its normalized job on every platform.
fn existing_upload_job(config: &Config, key: &str) -> Result<Option<UploadJob>> {
    let path = upload_spool_path(config, key);
    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("reading {}", path.display())),
    };
    if !metadata.file_type().is_file() {
        anyhow::bail!("upload intent is not a regular file: {}", path.display());
    }
    if metadata.len() > UPLOAD_SPOOL_MAX_BYTES {
        anyhow::bail!("upload intent exceeds {UPLOAD_SPOOL_MAX_BYTES} bytes");
    }
    let bytes = std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
    let job: UploadJob = serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing upload intent {}", path.display()))?;
    if job.key != key {
        anyhow::bail!("upload intent key does not match file name");
    }
    let normalized = normalize_upload_job(config, &job)?;
    // A prior publisher may have renamed successfully and then failed its
    // directory fsync. Every idempotent reuse retries that durability step
    // before acknowledging the existing winner.
    let parent = path
        .parent()
        .context("upload intent path has no parent directory")?;
    crate::atomic::fsync_dir(parent).context("flushing existing upload intent directory")?;
    Ok(Some(normalized))
}

/// Atomically publish without replacing an existing winner. The temp contents
/// and destination directory are flushed before success is acknowledged.
fn publish_upload_job_create_only(path: &Path, bytes: &[u8]) -> Result<bool> {
    use std::io::Write as _;

    let parent = path
        .parent()
        .context("upload intent path has no parent directory")?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("creating upload intent temp in {}", parent.display()))?;
    temp.write_all(bytes).context("writing upload intent")?;
    temp.as_file()
        .sync_all()
        .context("flushing upload intent")?;
    match temp.persist_noclobber(path) {
        Ok(_) => {
            crate::atomic::fsync_dir(parent).context("flushing upload intent directory")?;
            Ok(true)
        }
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            drop(error.file);
            // The winner's bytes were flushed before its exclusive publish;
            // flushing the directory here also makes a concurrent winner's
            // directory entry durable before we reuse it.
            crate::atomic::fsync_dir(parent).context("flushing upload intent directory")?;
            Ok(false)
        }
        Err(error) => Err(error.error).context("publishing upload intent"),
    }
}

fn ensure_upload_spool_dir_with<C, S>(dir: &Path, create_dir_all: C, sync_dir: S) -> Result<()>
where
    C: FnOnce(&Path) -> std::io::Result<()>,
    S: FnOnce(&Path) -> std::io::Result<()>,
{
    create_dir_all(dir).with_context(|| format!("creating upload spool {}", dir.display()))?;
    let parent = dir
        .parent()
        .context("upload spool path has no parent directory")?;
    // `create_dir_all` can return before the new `upload-queue` entry is
    // durable. Flush its parent on every caller: if an earlier first-create
    // attempt created the directory but its fsync failed, the next attempt must
    // retry that fsync instead of mistaking `is_dir()` for proof of durability.
    sync_dir(parent).with_context(|| format!("flushing upload spool parent {}", parent.display()))
}

/// Persist an upload intent before acknowledging/sending it. The file name is
/// the already-validated content key, and the entry directory is re-derived
/// from daemon/client config rather than trusting serialized path text.
fn persist_upload_job(config: &Config, job: &UploadJob) -> Result<UploadJob> {
    let normalized = normalize_upload_job(config, job)?;
    let dir = config.upload_spool_dir();
    ensure_upload_spool_dir_with(&dir, std::fs::create_dir_all, crate::atomic::fsync_dir)?;
    if let Some(mut existing) = existing_upload_job(config, &normalized.key)? {
        // The durable first winner stays byte-for-byte unchanged, but the live
        // wire request must carry this caller's epoch so a newer wrapper can
        // still trigger stale-daemon replacement.
        existing.client_epoch = normalized.client_epoch;
        return Ok(existing);
    }

    let store = Store::open(config).context("opening store for upload intent publication")?;
    let _gc_lock = store
        .acquire_gc_lock()
        .context("locking GC for upload intent publication")?;
    // Another publisher may have won while this process waited for GC.
    if let Some(mut existing) = existing_upload_job(config, &normalized.key)? {
        existing.client_epoch = normalized.client_epoch;
        return Ok(existing);
    }
    // This check and the first durable publication are one critical section
    // with every production GC sweep. A GC that won first may have removed the
    // entry; never leave behind an unreplayable intent in that case.
    if !store.contains(&normalized.key) {
        anyhow::bail!("local cache entry missing before upload intent publication");
    }

    let existing_count = std::fs::read_dir(&dir)
        .with_context(|| format!("reading upload spool {}", dir.display()))?
        .take(UPLOAD_SPOOL_MAX_JOBS)
        .try_fold(0usize, |count, entry| -> Result<usize> {
            entry.with_context(|| format!("reading upload spool {}", dir.display()))?;
            Ok(count + 1)
        })?;
    if existing_count == UPLOAD_SPOOL_MAX_JOBS {
        anyhow::bail!("upload spool is full ({UPLOAD_SPOOL_MAX_JOBS} jobs)");
    }
    let bytes = serde_json::to_vec(&normalized).context("serializing upload intent")?;
    if bytes.len() as u64 > UPLOAD_SPOOL_MAX_BYTES {
        anyhow::bail!("upload intent exceeds {UPLOAD_SPOOL_MAX_BYTES} bytes");
    }
    let path = upload_spool_path(config, &normalized.key);
    if publish_upload_job_create_only(&path, &bytes)? {
        Ok(normalized)
    } else {
        let mut existing = existing_upload_job(config, &normalized.key)?
            .context("upload intent winner disappeared")?;
        existing.client_epoch = normalized.client_epoch;
        Ok(existing)
    }
}

fn remove_upload_job(config: &Config, key: &str) -> Result<()> {
    let path = upload_spool_path(config, key);
    match std::fs::remove_file(&path) {
        Ok(()) => {
            if let Some(parent) = path.parent() {
                crate::atomic::fsync_dir(parent).context("flushing upload spool removal")?;
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("removing {}", path.display())),
    }
}

fn load_upload_jobs(config: &Config) -> Result<Vec<UploadJob>> {
    let dir = config.upload_spool_dir();
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error).with_context(|| format!("reading {}", dir.display())),
    };
    let mut jobs = Vec::new();
    for entry in entries.take(UPLOAD_SPOOL_MAX_JOBS) {
        let entry = entry?;
        if !entry.file_type()?.is_file() || entry.metadata()?.len() > UPLOAD_SPOOL_MAX_BYTES {
            continue;
        }
        let Some(file_name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(key) = file_name.strip_suffix(".json") else {
            continue;
        };
        if !crate::cache_key::is_valid_cache_key(key) {
            continue;
        }
        let bytes = std::fs::read(entry.path())?;
        let Ok(job) = serde_json::from_slice::<UploadJob>(&bytes) else {
            tracing::warn!(path = %entry.path().display(), "ignoring malformed upload intent");
            continue;
        };
        if job.key != key || !crate::cache_key::is_valid_crate_name(&job.crate_name) {
            tracing::warn!(path = %entry.path().display(), "ignoring invalid upload intent");
            continue;
        }
        jobs.push(UploadJob {
            entry_dir: config.store_dir().join(key).display().to_string(),
            ..job
        });
    }
    Ok(jobs)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GcRequest {
    /// Legacy wire field retained so old clients and daemons can still
    /// exchange an explicit `--max-age` request during rolling upgrades.
    pub max_age_hours: Option<u64>,
    #[serde(default)]
    pub mode: GcRequestMode,
    /// Effective automatic age policy loaded by the requesting CLI. Old
    /// daemons ignore this unknown field; new daemons no longer substitute
    /// their startup config for a manual request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_max_age_hours: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GcRequestMode {
    /// A pre-mode client. Resolve `Some` as explicit age and `None` using the
    /// daemon's configured automatic policy.
    #[default]
    Legacy,
    Automatic,
    ExplicitAge,
}

#[derive(Debug, Clone, Copy)]
enum GcPolicy {
    Automatic { max_age_hours: u64 },
    ExplicitAge { hours: u64 },
}

impl GcPolicy {
    fn mode(self) -> GcRequestMode {
        match self {
            Self::Automatic { .. } => GcRequestMode::Automatic,
            Self::ExplicitAge { .. } => GcRequestMode::ExplicitAge,
        }
    }
}

impl GcRequest {
    fn automatic(effective_max_age_hours: u64) -> Self {
        Self {
            max_age_hours: None,
            mode: GcRequestMode::Automatic,
            effective_max_age_hours: Some(effective_max_age_hours),
        }
    }

    fn explicit_age(hours: u64) -> Self {
        Self {
            max_age_hours: Some(hours),
            mode: GcRequestMode::ExplicitAge,
            effective_max_age_hours: None,
        }
    }

    #[cfg(test)]
    fn legacy(max_age_hours: Option<u64>) -> Self {
        Self {
            max_age_hours,
            mode: GcRequestMode::Legacy,
            effective_max_age_hours: None,
        }
    }

    fn resolve(&self, daemon_max_age_hours: u64) -> Result<GcPolicy> {
        Ok(match self.mode {
            GcRequestMode::Automatic => GcPolicy::Automatic {
                max_age_hours: self.effective_max_age_hours.ok_or_else(|| {
                    anyhow::anyhow!("automatic GC request is missing effective_max_age_hours")
                })?,
            },
            GcRequestMode::ExplicitAge => GcPolicy::ExplicitAge {
                hours: self.max_age_hours.ok_or_else(|| {
                    anyhow::anyhow!("explicit_age GC request is missing max_age_hours")
                })?,
            },
            GcRequestMode::Legacy => match self.max_age_hours {
                Some(hours) => GcPolicy::ExplicitAge { hours },
                None => GcPolicy::Automatic {
                    max_age_hours: daemon_max_age_hours,
                },
            },
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RemoteCheckRequest {
    pub key: String,
    pub entry_dir: String,
    #[serde(default)]
    pub crate_name: String,
    /// Client-side end-to-end budget. New daemons use the stricter of this,
    /// their own configured budget, and the legacy three-second demand cap, so
    /// config drift cannot make the client time out while the daemon keeps
    /// doing abandoned work. Missing/zero values retain that legacy cap for
    /// compatibility with old clients.
    #[serde(default)]
    pub deadline_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StatsRequest {
    pub include_entries: bool,
    /// Include the bounded recent session-summary tail. False for TUI/health
    /// polling so they do not rescan the append-only summary log every tick.
    #[serde(default)]
    pub include_summaries: bool,
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

/// Daemon-assisted local hit lookup (kunobi-ninja/kache#565).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LocalLookupRequest {
    pub key: String,
    /// Client binary mtime — lets the daemon detect when it's running stale code.
    #[serde(default)]
    pub client_epoch: u64,
}

/// Reply payload for [`Request::LocalLookup`]. `outcome` is a plain string —
/// a client that doesn't recognize the value treats it as `fallback`, so
/// protocol evolution degrades to the fully local path instead of erroring.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LocalLookupReply {
    /// `"hit"` | `"miss"` | `"fallback"`.
    pub outcome: String,
    /// Present on `"hit"`: the entry to restore. Blob paths are derived by the
    /// wrapper from its own `store_dir` (same layout as the daemon's).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<crate::store::EntryMeta>,
    /// Present on `"fallback"`: why the daemon declined (diagnostics only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl LocalLookupReply {
    pub(crate) fn hit(meta: crate::store::EntryMeta) -> Self {
        Self {
            outcome: "hit".to_string(),
            meta: Some(meta),
            reason: None,
        }
    }

    pub(crate) fn miss() -> Self {
        Self {
            outcome: "miss".to_string(),
            meta: None,
            reason: None,
        }
    }

    pub(crate) fn fallback(reason: impl Into<String>) -> Self {
        Self {
            outcome: "fallback".to_string(),
            meta: None,
            reason: Some(reason.into()),
        }
    }
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
    /// Warm the whole remote: LIST every key in the bucket and download the
    /// ones missing locally, in addition to `keys`.
    ///
    /// This has to be asked for explicitly (kunobi-ninja/kache#615). It used
    /// to be what an EMPTY `keys` meant, so any caller that encoded "no
    /// candidates" the obvious way started a download proportional to the
    /// entire bucket. An empty `keys` now means what it says: nothing to do.
    ///
    /// Still unbounded by key count, bytes, or time — see #616.
    #[serde(default)]
    pub warm_all: bool,
}

impl PrefetchRequest {
    pub fn from_plan(plan: PrefetchPlan) -> Self {
        Self {
            warm_all: false,
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
    /// Build session id minted by the wrapper that won the session-marker
    /// lock (kunobi-ninja/kache#583 P0.5). Empty from legacy wrappers.
    #[serde(default)]
    pub session_id: String,
}

/// Register (or update) an in-flight miss compile in the daemon's registry
/// (kunobi-ninja/kache#131). Sent fire-and-forget by the wrapper's heartbeat
/// monitor — at spawn, and again on the first tick once the typical-time
/// median is known. Upserts by `pid`, so the refresh is idempotent. An old
/// daemon rejects the unknown variant with a parse error the client ignores;
/// registration is observability only and must never affect the build.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CompileStartedRequest {
    pub crate_name: String,
    #[serde(default)]
    pub root: String,
    /// PID of the compiler child (registry key; also lets the daemon drop
    /// entries whose process died without a CompileFinished).
    pub pid: u32,
    /// Wall-clock spawn time, ms since epoch — the daemon derives elapsed
    /// from it so a registry entry needs no clock of its own.
    pub started_at_ms: u64,
    /// Median historical compile cost when the wrapper has looked it up
    /// (lazily, on the first heartbeat tick).
    #[serde(default)]
    pub typical_ms: Option<u64>,
    /// Client binary mtime — lets the daemon detect when it's running stale code.
    #[serde(default)]
    pub client_epoch: u64,
}

/// Remove a finished compile from the in-flight registry (fire-and-forget
/// counterpart of [`CompileStartedRequest`]). A wrapper that dies without
/// sending this is covered by liveness pruning on the daemon side.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CompileFinishedRequest {
    pub pid: u32,
    /// Echo of the registration's `started_at_ms` — the daemon removes the
    /// entry only when it matches, so a delayed Finished from a monitor whose
    /// PID the OS already reused cannot delete the NEW compile's entry
    /// (cross-family review finding). `0` (an old client) matches anything.
    #[serde(default)]
    pub started_at_ms: u64,
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
    /// Content-dedup figures for the daemon's store. Defaulted for an old
    /// daemon so a new client can suppress the section instead of mixing in
    /// figures from its own differently configured store.
    #[serde(default)]
    pub blob_stats: Option<crate::store::BlobStats>,
    /// Bounded recent session summaries from the daemon's event directory.
    #[serde(default)]
    pub recent_summaries: Vec<crate::events::BuildSummaryEvent>,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub build_epoch: u64,
    /// GC request semantics supported by this daemon. Version 2 means the
    /// daemon applies age before duplicate/size pressure and reports a policy
    /// breakdown. Missing means an older daemon, so clients must not send a
    /// mutating GC request.
    #[serde(default)]
    pub gc_policy_version: u32,
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
    /// Upload attempts deferred because the remote write breaker was degraded (#327).
    #[serde(default)]
    pub uploads_suppressed: u64,
    #[serde(default)]
    pub downloads_completed: u64,
    #[serde(default)]
    pub downloads_failed: u64,
    /// Restores answered "miss" because the remote breaker was degraded (#327).
    #[serde(default)]
    pub downloads_suppressed: u64,
    /// RemoteChecks that actually reached S3 (HEAD probes + GETs) — the
    /// denominator for `negative_hits` (#564).
    #[serde(default)]
    pub remote_check_roundtrips: u64,
    /// Checks answered from the negative-result cache without S3 (#564).
    #[serde(default)]
    pub negative_hits: u64,
    /// Definitive misses currently remembered by the negative cache (#564).
    #[serde(default)]
    pub negative_entries: u64,
    /// Whether the remote breaker is currently degraded (#327).
    #[serde(default)]
    pub remote_degraded: bool,
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
    /// In-flight miss compiles registered by wrapper heartbeat monitors
    /// (kunobi-ninja/kache#131). Defaulted for old-daemon/new-client mixes.
    #[serde(default)]
    pub in_flight: Vec<InFlightEntry>,
    /// The configuration this daemon actually loaded (kunobi-ninja/kache#689).
    /// Defaulted to `None` so a daemon that predates the field is
    /// distinguishable from one that reported it — the CLI then falls back to
    /// its own config and labels the affected lines as client-derived.
    #[serde(default)]
    pub effective_config: Option<EffectiveConfig>,
}

/// The configuration the daemon loaded at startup, carried in every
/// [`StatsResponse`] (kunobi-ninja/kache#689).
///
/// Daemon-backed CLI reads render these values instead of re-resolving config
/// in the invoking process — whose `KACHE_CONFIG` / `XDG_CONFIG_HOME` /
/// `KACHE_*` env may resolve differently — and name both sides when the two
/// disagree, instead of silently presenting daemon values as if they were the
/// invocation's own.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EffectiveConfig {
    /// `[cache] local_max_size` / `KACHE_MAX_SIZE` as the daemon resolved it.
    pub max_size: u64,
    /// The store directory the daemon's numbers describe.
    pub cache_dir: String,
    /// The config-file path the daemon resolved at startup (the file its
    /// fingerprint watcher tracks). The file may not exist — defaults then
    /// applied — but the path still names where the daemon would read one.
    pub config_path: String,
    /// Fingerprint of the exact path/presence/content snapshot parsed at
    /// daemon startup. This detects same-path edits beyond the rendered field
    /// subset and ties the watcher baseline to what was actually loaded.
    #[serde(default)]
    pub config_fingerprint: Option<String>,
    /// `[cache] prefetch_enabled` / `KACHE_PREFETCH_ENABLED` as resolved.
    pub prefetch_enabled: bool,
    /// Credential-free remote description (for example `s3://bucket/prefix`)
    /// as resolved by the daemon. `None` means no usable remote.
    #[serde(default)]
    pub remote_description: Option<String>,
    /// Whether the daemon started in strict local-only mode.
    #[serde(default)]
    pub local_only: bool,
    /// Why a configured remote was unusable, when configuration degraded to
    /// local-only operation. This is the same user-facing reason the daemon
    /// logs; credentials are never included.
    #[serde(default)]
    pub remote_error: Option<String>,
    /// Remote key-index refresh cadence used by the daemon.
    #[serde(default = "default_effective_remote_key_cache_refresh_secs")]
    pub remote_key_cache_refresh_secs: u64,
    /// The socket endpoint the daemon serves on.
    pub socket_path: String,
    /// Unix millis when the daemon captured this config (process startup),
    /// so a mismatch warning can say how old the in-effect config is.
    #[serde(default)]
    pub started_at_ms: u64,
}

fn default_effective_remote_key_cache_refresh_secs() -> u64 {
    crate::config::DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS
}

impl EffectiveConfig {
    /// Snapshot the reportable view of `config` plus the exact path/content
    /// provenance parsed by [`Config::load_with_provenance`]. The watcher uses
    /// the same fingerprint as its baseline, so an edit between load and
    /// watcher startup is detected on the first poll.
    pub(crate) fn capture(
        config: &Config,
        provenance: &crate::config::ConfigFileProvenance,
    ) -> Self {
        Self {
            max_size: config.max_size,
            cache_dir: config.cache_dir.display().to_string(),
            config_path: provenance.path.display().to_string(),
            config_fingerprint: Some(provenance.fingerprint.clone()),
            prefetch_enabled: config.prefetch_enabled,
            remote_description: config.remote.as_ref().map(|remote| remote.describe()),
            local_only: config.local_only,
            remote_error: config.remote_error.clone(),
            remote_key_cache_refresh_secs: config.remote_key_cache_refresh_secs,
            socket_path: config.socket_path().display().to_string(),
            started_at_ms: now_millis(),
        }
    }
}

/// One in-flight compile as reported to stats consumers (`kache monitor`'s
/// "In flight" panel). Elapsed/ETA are computed at snapshot time from the
/// registry's wall-clock start.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InFlightEntry {
    pub crate_name: String,
    #[serde(default)]
    pub root: String,
    pub pid: u32,
    pub elapsed_s: u64,
    #[serde(default)]
    pub typical_s: Option<u64>,
    #[serde(default)]
    pub eta_s: Option<u64>,
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
    /// Candidates dropped un-downloaded because a plan budget was exhausted
    /// (kunobi-ninja/kache#616). Distinct from `keys_cancelled`, which is the
    /// adaptive hit-rate cancel: this is "the plan was too big / too slow",
    /// that one is "the plan looked wrong".
    #[serde(default)]
    pub keys_over_budget: u64,
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
    #[serde(default)]
    pub list_requests_total: u64,
    #[serde(default)]
    pub list_failures_total: u64,
    #[serde(default)]
    pub list_duration_ms_total: u64,
    #[serde(default)]
    pub list_keys_total: u64,
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

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcPolicyOutcome {
    pub entries_evicted: usize,
    pub bytes_freed: u64,
}

impl From<&crate::store::GcStats> for GcPolicyOutcome {
    fn from(stats: &crate::store::GcStats) -> Self {
        Self {
            entries_evicted: stats.entries_evicted,
            bytes_freed: stats.bytes_freed,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcBreakdown {
    pub mode: GcRequestMode,
    pub duplicate: GcPolicyOutcome,
    pub age: GcPolicyOutcome,
    pub size: GcPolicyOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct Response {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evicted: Option<usize>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub skipped: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gc: Option<GcBreakdown>,
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
    /// Reply payload for `Request::LocalLookup` (kunobi-ninja/kache#565).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_lookup: Option<LocalLookupReply>,
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
            gc: None,
            found: None,
            prefetched: None,
            stats: None,
            batch_results: None,
            hash_results: None,
            local_lookup: None,
            error: None,
        }
    }

    #[cfg(test)]
    fn ok_evicted(n: usize) -> Self {
        Self {
            ok: true,
            evicted: Some(n),
            skipped: false,
            gc: None,
            found: None,
            prefetched: None,
            stats: None,
            batch_results: None,
            hash_results: None,
            local_lookup: None,
            error: None,
        }
    }

    fn ok_gc(total: usize, breakdown: GcBreakdown) -> Self {
        Self {
            evicted: Some(total),
            gc: Some(breakdown),
            ..Self::ok()
        }
    }

    fn ok_gc_skipped(breakdown: GcBreakdown) -> Self {
        Self {
            ok: true,
            evicted: Some(0),
            skipped: true,
            gc: Some(breakdown),
            found: None,
            prefetched: None,
            stats: None,
            batch_results: None,
            hash_results: None,
            local_lookup: None,
            error: None,
        }
    }

    fn ok_stats(stats: StatsResponse) -> Self {
        Self {
            ok: true,
            evicted: None,
            skipped: false,
            gc: None,
            found: None,
            prefetched: None,
            stats: Some(stats),
            batch_results: None,
            hash_results: None,
            local_lookup: None,
            error: None,
        }
    }

    fn ok_batch(results: Vec<Response>) -> Self {
        Self {
            ok: true,
            evicted: None,
            skipped: false,
            gc: None,
            found: None,
            prefetched: None,
            stats: None,
            batch_results: Some(results),
            hash_results: None,
            local_lookup: None,
            error: None,
        }
    }

    fn ok_hash_results(results: Vec<HashFileResult>) -> Self {
        Self {
            ok: true,
            evicted: None,
            skipped: false,
            gc: None,
            found: None,
            prefetched: None,
            stats: None,
            batch_results: None,
            hash_results: Some(results),
            local_lookup: None,
            error: None,
        }
    }

    fn found(val: bool) -> Self {
        Self {
            ok: true,
            evicted: None,
            skipped: false,
            gc: None,
            found: Some(val),
            prefetched: None,
            stats: None,
            batch_results: None,
            hash_results: None,
            local_lookup: None,
            error: None,
        }
    }

    fn found_prefetched(val: bool, prefetched: bool) -> Self {
        Self {
            ok: true,
            evicted: None,
            skipped: false,
            gc: None,
            found: Some(val),
            prefetched: Some(prefetched),
            stats: None,
            batch_results: None,
            hash_results: None,
            local_lookup: None,
            error: None,
        }
    }

    fn ok_local_lookup(reply: LocalLookupReply) -> Self {
        Self {
            local_lookup: Some(reply),
            ..Self::ok()
        }
    }

    fn err(msg: impl Into<String>) -> Self {
        Self {
            ok: false,
            evicted: None,
            skipped: false,
            gc: None,
            found: None,
            prefetched: None,
            stats: None,
            batch_results: None,
            hash_results: None,
            local_lookup: None,
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
    /// Upload attempts deferred without touching S3 because the remote write
    /// breaker was degraded (kunobi-ninja/kache#327). Durable intents remain queued.
    pub uploads_suppressed: std::sync::atomic::AtomicU64,
    pub downloads_completed: std::sync::atomic::AtomicU64,
    pub downloads_failed: std::sync::atomic::AtomicU64,
    /// Restores answered "miss" without touching S3 because the remote
    /// breaker was degraded (kunobi-ninja/kache#327).
    pub downloads_suppressed: std::sync::atomic::AtomicU64,
    /// RemoteCheck requests that actually reached the remote (HEAD probes and
    /// GETs; one transport attempt per admitted operation). The denominator for judging the negative cache
    /// (kunobi-ninja/kache#564).
    pub remote_check_roundtrips: std::sync::atomic::AtomicU64,
    pub bytes_uploaded: std::sync::atomic::AtomicU64,
    pub bytes_downloaded: std::sync::atomic::AtomicU64,
}

impl TransferCounters {
    fn new() -> Self {
        Self {
            uploads_completed: 0.into(),
            uploads_failed: 0.into(),
            uploads_skipped: 0.into(),
            uploads_suppressed: 0.into(),
            downloads_completed: 0.into(),
            downloads_failed: 0.into(),
            downloads_suppressed: 0.into(),
            remote_check_roundtrips: 0.into(),
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
/// Telemetry only — nothing here feeds a decision. Adaptive cancellation is
/// driven by the per-plan [`ActivePlan`] counters (#581); these exist so
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
    /// Keys dropped un-downloaded because a plan budget was exhausted (#616).
    pub keys_over_budget: std::sync::atomic::AtomicU64,
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
    /// Cumulative key-cache LIST telemetry (#583 P0.5). The "last" gauges
    /// above show current behavior; deciding whether LIST replacement (plan
    /// P3) is worth building needs totals — count, failures, total wall time,
    /// total keys returned — and per-session deltas of these.
    pub list_requests_total: std::sync::atomic::AtomicU64,
    pub list_failures_total: std::sync::atomic::AtomicU64,
    pub list_duration_ms_total: std::sync::atomic::AtomicU64,
    pub list_keys_total: std::sync::atomic::AtomicU64,
}

impl PrefetchStats {
    fn new() -> Self {
        Self {
            downloads_completed: 0.into(),
            bytes_downloaded: 0.into(),
            keys_used: 0.into(),
            keys_cancelled: 0.into(),
            keys_over_budget: 0.into(),
            plans_advisory: 0.into(),
            plans_fallback: 0.into(),
            last_plan_candidates: 0.into(),
            dedup_join_waits: 0.into(),
            dedup_join_wait_ms: 0.into(),
            last_list_duration_ms: 0.into(),
            last_list_key_count: 0.into(),
            list_requests_total: 0.into(),
            list_failures_total: 0.into(),
            list_duration_ms_total: 0.into(),
            list_keys_total: 0.into(),
        }
    }
}

const RECENT_TRANSFERS_CAP: usize = 50;

// ── Active prefetch plan (per-session attribution, #583 P0.5) ───────────────

/// Per-plan prefetch bookkeeping. One plan is active at a time (the daemon
/// serves one build session per cache dir); a new BuildStarted supersedes and
/// finalizes the previous plan, and an inactivity sweep finalizes an
/// abandoned one. Fixes #581: the adaptive-cancel counters live HERE, reset
/// per plan, instead of daemon-lifetime atomics whose ratio was 100% by
/// construction.
///
/// KNOWN LIMITS (P0.5 scope, accepted in cross-family review): concurrent
/// builds from different roots share this single slot — their demands are
/// coalesced because RemoteCheck carries no session id yet (a P2a feedback
/// concern); and a superseded plan's still-in-flight downloads record into
/// the superseding plan (brief window, inflates its potential-hit upper
/// bound, i.e. errs toward NOT cancelling — the safe direction).
#[derive(Debug)]
pub(crate) struct ActivePlan {
    pub session_id: String,
    pub plan_id: String,
    /// `advisory` | `fallback`.
    pub plan_source: &'static str,
    pub candidates: HashSet<String>,
    /// Distinct keys demanded via RemoteCheck while this plan was active —
    /// candidate or not. The denominator of the adaptive-cancel ratio.
    pub demanded: HashSet<String>,
    /// Demanded ∩ candidates: the numerator.
    pub demanded_candidates: HashSet<String>,
    /// Prefetch downloads completed under this plan: key → compressed bytes.
    pub downloaded: HashMap<String, u64>,
    /// Demanded ∩ downloaded — daemon-visible use (lower bound; a completed
    /// prefetch consumed via the wrapper's local store path never gets here).
    pub used: HashSet<String>,
    pub cancelled: bool,
    pub started_at_ms: u64,
    pub last_activity_ms: u64,
    /// Cumulative LIST counters at install time, for per-session deltas.
    pub list_requests_at_install: u64,
    pub list_duration_ms_at_install: u64,
}

impl ActivePlan {
    fn new(
        session_id: String,
        plan_id: String,
        plan_source: &'static str,
        candidates: HashSet<String>,
        list_requests_at_install: u64,
        list_duration_ms_at_install: u64,
    ) -> Self {
        let now = epoch_ms();
        Self {
            session_id,
            plan_id,
            plan_source,
            candidates,
            demanded: HashSet::new(),
            demanded_candidates: HashSet::new(),
            downloaded: HashMap::new(),
            used: HashSet::new(),
            cancelled: false,
            started_at_ms: now,
            last_activity_ms: now,
            list_requests_at_install,
            list_duration_ms_at_install,
        }
    }

    /// Record a demanded key; returns true when adaptive cancellation should
    /// fire NOW (single false→true transition of the latch).
    fn record_demand(&mut self, key: &str) -> bool {
        self.last_activity_ms = epoch_ms();
        if self.demanded.insert(key.to_string()) {
            if self.candidates.contains(key) {
                self.demanded_candidates.insert(key.to_string());
            }
            if self.downloaded.contains_key(key) {
                self.used.insert(key.to_string());
            }
        }
        if self.cancelled {
            return false;
        }
        let downloaded_not_demanded = self
            .downloaded
            .keys()
            .filter(|k| !self.demanded.contains(*k))
            .count() as u64;
        if should_cancel_prefetch(
            self.demanded.len() as u64,
            self.demanded_candidates.len() as u64,
            downloaded_not_demanded,
        ) {
            self.cancelled = true;
            return true;
        }
        false
    }

    fn record_download(&mut self, key: &str, compressed_bytes: u64) {
        self.last_activity_ms = epoch_ms();
        self.downloaded.insert(key.to_string(), compressed_bytes);
        if self.demanded.contains(key) {
            self.used.insert(key.to_string());
        }
    }

    fn used_bytes(&self) -> u64 {
        self.used
            .iter()
            .filter_map(|k| self.downloaded.get(k))
            .sum()
    }
}

/// Should adaptive prefetch cancellation fire? (#581)
///
/// `demanded` = distinct keys the build has asked for while the plan is
/// active (candidate or not); `demanded_candidates` = the subset that were
/// plan candidates; `downloaded_not_demanded` = completed prefetch downloads
/// the daemon has NOT seen demanded — these may already have been consumed
/// through the wrapper's local store path without reaching the daemon, so
/// they count as potential hits (conservative upper bound, cross-family
/// review). Cancel only when even the upper-bound hit rate is below 30%
/// after 10+ distinct demands: wasting a plan is cheaper than cancelling a
/// good one on biased evidence.
/// How many of `offered` candidates a key budget of `max_keys` drops
/// (kunobi-ninja/kache#616). `0` disables the budget.
pub(crate) fn prefetch_key_budget_overflow(offered: usize, max_keys: u64) -> usize {
    if max_keys == 0 {
        return 0;
    }
    offered.saturating_sub(max_keys as usize)
}

/// Has this plan spent its byte budget? `0` disables the budget.
///
/// Compared with `>=` so a budget already met stops the next download rather
/// than allowing one more. The budget is soft either way: it gates what may
/// still START, and whatever is in flight is left to finish.
pub(crate) fn prefetch_byte_budget_exhausted(max_bytes: u64, spent: u64) -> bool {
    max_bytes > 0 && spent >= max_bytes
}

pub(crate) fn should_cancel_prefetch(
    demanded: u64,
    demanded_candidates: u64,
    downloaded_not_demanded: u64,
) -> bool {
    if demanded < 10 {
        return false;
    }
    let upper_bound_hits = demanded_candidates + downloaded_not_demanded;
    (upper_bound_hits as f64 / demanded as f64) < 0.3
}

fn epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

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
    /// Incremented for every point insert/remove. A LIST captures this before
    /// I/O and may swap its snapshot only if no newer point knowledge landed
    /// meanwhile; otherwise the stale listing is discarded rather than
    /// erasing a successful upload or resurrecting a stale positive.
    revision: AtomicU64,
}

impl S3KeyCache {
    fn new() -> Self {
        Self {
            index: RwLock::new(None),
            populated: AtomicBool::new(false),
            last_populated: RwLock::new(None),
            revision: AtomicU64::new(0),
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
    fn refresh_revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }

    pub async fn populate(&self, keys: HashMap<String, String>) {
        let revision = self.refresh_revision();
        let _ = self.populate_if_unchanged(keys, revision).await;
    }

    /// Swap a LIST snapshot only if no point update completed since the LIST
    /// began. Returning false is conservative: existing knowledge stays live
    /// and the periodic refresher will try again.
    pub async fn populate_if_unchanged(
        &self,
        keys: HashMap<String, String>,
        start_revision: u64,
    ) -> bool {
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
        if self.revision.load(Ordering::Acquire) != start_revision {
            return false;
        }
        *guard = Some(new_index);
        drop(guard);

        self.populated.store(true, Ordering::Release);
        let mut ts = self.last_populated.write().await;
        *ts = Some(Instant::now());
        true
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
        self.revision.fetch_add(1, Ordering::AcqRel);
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
        self.revision.fetch_add(1, Ordering::AcqRel);
    }
}

// ── Daemon (the "lib" — all business logic, no I/O) ─────────────

pub(crate) struct Daemon {
    config: Config,
    store: OnceLock<Mutex<Store>>,
    /// Daemon-assisted local hits (#565): read-only probe pool + pin writer.
    /// Lazily initialized on the first `LocalLookup`; only success is cached,
    /// so a transient init failure is retried by a later request instead of
    /// disabling the feature for the daemon's lifetime.
    local_hit: OnceLock<crate::daemon_local::LocalHitService>,
    remote_backend: tokio::sync::OnceCell<Arc<dyn crate::remote_backend::RemoteBackend>>,
    key_cache: Arc<S3KeyCache>,
    /// Degradation breaker consulted (and fed) by every remote op: HEAD
    /// probes, restores, uploads, and key-cache LISTs (kunobi-ninja/kache#327).
    remote_breaker: Arc<RemoteBreaker>,
    /// Definitive remote misses remembered for a short TTL so parallel
    /// wrappers don't stampede S3 for the same absent key
    /// (kunobi-ninja/kache#564).
    negative_keys: NegativeKeyCache,
    /// Complete demand-check singleflight, claimed before any negative-cache,
    /// key-cache or HEAD work. This closes the first-miss stampede rather than
    /// deduplicating only the later GET/extraction phase.
    remote_checks: KeyedSingleflight<Response>,
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
    /// Signals remaining prefetch downloads to stop when hit rate is too low.
    /// Reset to `false` on every plan install; the per-plan counters that
    /// drive it live in [`ActivePlan`] (#581).
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
    /// The active per-session prefetch plan (#583 P0.5). Std mutex: every
    /// critical section is a short map/set operation, never held across await.
    active_plan: Arc<std::sync::Mutex<Option<ActivePlan>>>,
    /// In-flight miss compiles keyed by child PID (kunobi-ninja/kache#131).
    /// Upserted by CompileStarted, removed by CompileFinished, and pruned by
    /// liveness/age on both read (stats) and write (register) paths — a
    /// crashed wrapper must not leave a ghost entry forever.
    in_flight_compiles: std::sync::Mutex<HashMap<u32, CompileStartedRequest>>,
    version: String,
    build_epoch: u64,
    /// What this daemon actually loaded, reported in every stats response so
    /// daemon-backed CLI reads can render the daemon's view and name a
    /// CLI/daemon config divergence (kunobi-ninja/kache#689).
    effective_config: EffectiveConfig,
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

#[derive(Debug, Clone)]
struct GcRunReport {
    mode: GcRequestMode,
    duplicate: crate::store::GcStats,
    age: crate::store::GcStats,
    size: crate::store::GcStats,
    total: crate::store::GcStats,
}

impl GcRunReport {
    fn skipped(mode: GcRequestMode) -> Self {
        Self {
            mode,
            duplicate: crate::store::GcStats::default(),
            age: crate::store::GcStats::default(),
            size: crate::store::GcStats::default(),
            total: crate::store::GcStats {
                skipped: true,
                ..Default::default()
            },
        }
    }

    fn breakdown(&self) -> GcBreakdown {
        GcBreakdown {
            mode: self.mode,
            duplicate: GcPolicyOutcome::from(&self.duplicate),
            age: GcPolicyOutcome::from(&self.age),
            size: GcPolicyOutcome::from(&self.size),
        }
    }
}

impl Daemon {
    #[cfg(test)]
    pub fn new(config: Config) -> Self {
        let provenance = crate::config::ConfigFileProvenance::current();
        Self::new_with_provenance(config, &provenance)
    }

    fn new_with_provenance(
        config: Config,
        provenance: &crate::config::ConfigFileProvenance,
    ) -> Self {
        let permits = config.s3_concurrency.max(1) as usize;
        let (warming_tx, _) = tokio::sync::watch::channel(false);
        let (prefetch_cancel, _) = tokio::sync::watch::channel(false);
        Self {
            store: OnceLock::new(),
            local_hit: OnceLock::new(),
            s3_semaphore: Arc::new(tokio::sync::Semaphore::new(permits)),
            remote_backend: tokio::sync::OnceCell::new(),
            key_cache: Arc::new(S3KeyCache::new()),
            remote_breaker: Arc::new(RemoteBreaker::new()),
            negative_keys: NegativeKeyCache::new(config.remote_negative_ttl_secs),
            remote_checks: KeyedSingleflight::new(REMOTE_CHECK_SINGLEFLIGHT_MAX_KEYS),
            upload_tx: Mutex::new(None),
            upload_queue_closed: AtomicBool::new(false),
            pending_uploads: Arc::new(RwLock::new(HashSet::new())),
            downloading: Arc::new(RwLock::new(HashMap::new())),
            warming_tx,
            prefetched_keys: Arc::new(RwLock::new(HashSet::new())),
            prefetch_cancel,
            prefetch_stats: PrefetchStats::new(),
            prefetch_gate: Arc::new(tokio::sync::Semaphore::new(prefetch_concurrency_cap(
                config.s3_concurrency,
            ))),
            prefetch_used_keys: Arc::new(RwLock::new(HashSet::new())),
            active_plan: Arc::new(std::sync::Mutex::new(None)),
            in_flight_compiles: std::sync::Mutex::new(HashMap::new()),
            version: VERSION.to_string(),
            build_epoch: build_epoch(),
            effective_config: EffectiveConfig::capture(&config, provenance),
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

    /// Breaker/deadline/semaphore-aware shard fetch used by the fallback
    /// planner. Keeping it on the daemon prevents planner reads from bypassing
    /// the same controls as demand and startup prefetch.
    pub(crate) async fn download_planner_shard(
        &self,
        namespace: &str,
        shard_hash: &str,
    ) -> Result<Option<crate::remote::Shard>> {
        let remote = self
            .config
            .remote
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no remote configured"))?;
        let deadline = RemoteDeadline::from_secs(self.config.remote_restore_timeout_secs);
        let breaker = self
            .remote_breaker
            .try_acquire(RemoteOperation::ShardGet)
            .ok_or_else(|| anyhow::anyhow!("remote read breaker open"))?;
        let backend = match deadline
            .run("planner backend initialization", self.get_remote_backend())
            .await
        {
            Ok(backend) => backend,
            Err(error) => {
                let class = classify_remote_error(&error);
                breaker.failure(class, &format!("{error:#}"));
                return Err(error);
            }
        };
        let semaphore = match deadline
            .run("planner shard queue", async {
                self.s3_semaphore
                    .acquire()
                    .await
                    .map_err(|_| anyhow::anyhow!("remote semaphore closed"))
            })
            .await
        {
            Ok(permit) => permit,
            Err(error) => {
                let class = classify_remote_error(&error);
                breaker.failure(class, &format!("{error:#}"));
                return Err(error);
            }
        };
        let result = deadline
            .run(
                "planner shard GET",
                crate::remote::download_shard(
                    backend.as_ref(),
                    &remote.prefix,
                    namespace,
                    shard_hash,
                ),
            )
            .await;
        drop(semaphore);
        match &result {
            Ok(_) => breaker.success(),
            Err(error) => {
                let class = classify_remote_error(error);
                breaker.failure(class, &format!("{error:#}"));
            }
        }
        result
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
            Request::Gc(gc) | Request::GcV2(gc) => self.handle_gc(gc),
            Request::Stats(sr) => self.handle_stats(sr),
            Request::HashFiles(req) => self.handle_hash_files(req),
            Request::CompileStarted(req) => self.handle_compile_started(req.clone()),
            Request::CompileFinished(req) => self.handle_compile_finished(req),
            Request::Upload(_)
            | Request::RemoteCheck(_)
            | Request::BatchRemoteCheck(_)
            | Request::LocalLookup(_)
            | Request::Prefetch(_)
            | Request::BuildStarted(_) => {
                // These require async — caller must use their async handlers
                Response::err(
                    "upload/remote_check/batch/local_lookup/prefetch/build_started must be handled async",
                )
            }
            Request::Shutdown => Response::ok(),
        }
    }

    /// Handle a stats request — reads store and event log.
    pub fn handle_stats(&self, req: &StatsRequest) -> Response {
        let (total_size, entry_count, entries, blob_stats) = match self.with_store(|store| {
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
            let blob_stats = store.blob_stats().ok();
            Ok((total_size, entry_count, entries, blob_stats))
        }) {
            Ok(values) => values,
            Err(e) => return Response::err(format!("store open failed: {e}")),
        };

        let hours = req.event_hours.unwrap_or(24);
        let since = chrono::Utc::now() - chrono::Duration::hours(hours as i64);
        let event_list =
            events::read_events_since(&self.config.event_log_path(), since).unwrap_or_default();
        let es = events::compute_stats(&event_list);
        let recent_summaries = if req.include_summaries {
            let mut summaries =
                events::read_summaries(&self.config.summary_log_path()).unwrap_or_default();
            let keep_from = summaries.len().saturating_sub(5);
            summaries.drain(..keep_from);
            summaries
        } else {
            Vec::new()
        };

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

        let in_flight = self.in_flight_snapshot();

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
            blob_stats,
            recent_summaries,
            version: self.version.clone(),
            build_epoch: self.build_epoch,
            gc_policy_version: GC_POLICY_PROTOCOL_VERSION,
            pending_uploads,
            active_downloads,
            s3_concurrency_total: s3_total,
            s3_concurrency_used: s3_used,
            upload_queue_capacity: 0,
            uploads_completed: tc.uploads_completed.load(Ordering::Relaxed),
            uploads_failed: tc.uploads_failed.load(Ordering::Relaxed),
            uploads_skipped: tc.uploads_skipped.load(Ordering::Relaxed),
            uploads_suppressed: tc.uploads_suppressed.load(Ordering::Relaxed),
            downloads_completed: tc.downloads_completed.load(Ordering::Relaxed),
            downloads_failed: tc.downloads_failed.load(Ordering::Relaxed),
            downloads_suppressed: tc.downloads_suppressed.load(Ordering::Relaxed),
            remote_check_roundtrips: tc.remote_check_roundtrips.load(Ordering::Relaxed),
            negative_hits: self.negative_keys.hits(),
            negative_entries: self.negative_keys.len() as u64,
            remote_degraded: self.remote_breaker.is_degraded(),
            bytes_uploaded: tc.bytes_uploaded.load(Ordering::Relaxed),
            bytes_downloaded: tc.bytes_downloaded.load(Ordering::Relaxed),
            recent_transfers,
            prefetch: PrefetchStatsSnapshot {
                downloads_completed: ps.downloads_completed.load(Ordering::Relaxed),
                bytes_downloaded: ps.bytes_downloaded.load(Ordering::Relaxed),
                keys_used: ps.keys_used.load(Ordering::Relaxed),
                keys_cancelled: ps.keys_cancelled.load(Ordering::Relaxed),
                keys_over_budget: ps.keys_over_budget.load(Ordering::Relaxed),
                cancelled: *self.prefetch_cancel.borrow(),
                plans_advisory: ps.plans_advisory.load(Ordering::Relaxed),
                plans_fallback: ps.plans_fallback.load(Ordering::Relaxed),
                last_plan_candidates: ps.last_plan_candidates.load(Ordering::Relaxed),
                dedup_join_waits: ps.dedup_join_waits.load(Ordering::Relaxed),
                dedup_join_wait_ms: ps.dedup_join_wait_ms.load(Ordering::Relaxed),
                last_list_duration_ms: ps.last_list_duration_ms.load(Ordering::Relaxed),
                last_list_key_count: ps.last_list_key_count.load(Ordering::Relaxed),
                list_requests_total: ps.list_requests_total.load(Ordering::Relaxed),
                list_failures_total: ps.list_failures_total.load(Ordering::Relaxed),
                list_duration_ms_total: ps.list_duration_ms_total.load(Ordering::Relaxed),
                list_keys_total: ps.list_keys_total.load(Ordering::Relaxed),
            },
            in_flight,
            effective_config: Some(self.effective_config.clone()),
        })
    }

    /// Upsert an in-flight compile (kunobi-ninja/kache#131). Sync and tiny —
    /// no offload needed. Prunes on the way in so the map can't accumulate
    /// ghosts even if nobody ever asks for stats.
    pub fn handle_compile_started(&self, req: CompileStartedRequest) -> Response {
        if let Ok(mut map) = self.in_flight_compiles.lock() {
            prune_in_flight(&mut map);
            map.insert(req.pid, req);
        }
        Response::ok()
    }

    pub fn handle_compile_finished(&self, req: &CompileFinishedRequest) -> Response {
        if let Ok(mut map) = self.in_flight_compiles.lock()
            && let Some(entry) = map.get(&req.pid)
            && (req.started_at_ms == 0 || entry.started_at_ms == req.started_at_ms)
        {
            map.remove(&req.pid);
        }
        Response::ok()
    }

    /// Snapshot the in-flight registry for stats consumers, computing
    /// elapsed/ETA from wall-clock and pruning dead entries first.
    fn in_flight_snapshot(&self) -> Vec<InFlightEntry> {
        let Ok(mut map) = self.in_flight_compiles.lock() else {
            return Vec::new();
        };
        prune_in_flight(&mut map);
        let now_ms = unix_ms();
        let mut entries: Vec<InFlightEntry> = map
            .values()
            .map(|c| {
                let elapsed_s = now_ms.saturating_sub(c.started_at_ms) / 1000;
                let typical_s = c.typical_ms.map(|ms| ms.div_ceil(1000));
                InFlightEntry {
                    crate_name: c.crate_name.clone(),
                    root: c.root.clone(),
                    pid: c.pid,
                    elapsed_s,
                    typical_s,
                    eta_s: typical_s.map(|t| t.saturating_sub(elapsed_s)),
                }
            })
            .collect();
        // Oldest first — the entry a user is most likely waiting on.
        entries.sort_by_key(|e| std::cmp::Reverse(e.elapsed_s));
        entries
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

    /// Daemon-assisted local hit (kunobi-ninja/kache#565): probe on the
    /// read-only pool, pin via the batched writer, reply within a hard
    /// deadline. Every failure mode maps to a `fallback` reply — the wrapper
    /// then runs today's fully local path — so this endpoint can shed load
    /// but never block or fail a build. Deliberately does NOT touch
    /// `with_store`: probes must not queue behind GC/stats holding the store
    /// mutex. First-request initialization (SQLite opens, thread spawns, and
    /// any `OnceLock` wait behind a peer's in-flight init) runs on the
    /// blocking pool INSIDE the deadline, so a slow cold start degrades to
    /// `fallback` instead of stalling async workers past the client timeout.
    pub async fn handle_local_lookup(self: &Arc<Self>, req: &LocalLookupRequest) -> Response {
        if !crate::cache_key::is_valid_cache_key(&req.key) {
            return Response::err("invalid cache key");
        }
        let reply = tokio::time::timeout(crate::daemon_local::LOCAL_LOOKUP_DEADLINE, async {
            if self.local_hit.get().is_none() {
                // Only a SUCCESSFUL init is cached — a transient failure
                // (store dir racing into existence, disk pressure) must not
                // disable the feature for the daemon's lifetime. If two
                // requests race the init, the losing service is dropped and
                // its worker threads exit as their channel senders drop.
                let daemon = Arc::clone(self);
                let _ = tokio::task::spawn_blocking(move || {
                    match crate::daemon_local::LocalHitService::new(&daemon.config) {
                        Ok(svc) => {
                            let _ = daemon.local_hit.set(svc);
                        }
                        Err(e) => tracing::warn!("local-hit service init failed: {e:#}"),
                    }
                })
                .await;
            }
            match self.local_hit.get() {
                Some(service) => service.lookup(&req.key).await,
                None => LocalLookupReply::fallback("service unavailable"),
            }
        })
        .await
        .unwrap_or_else(|_| LocalLookupReply::fallback("deadline exceeded"));
        Response::ok_local_lookup(reply)
    }

    /// Handle a GC request — pure logic against the store.
    pub fn handle_gc(&self, req: &GcRequest) -> Response {
        let policy = match req.resolve(self.config.gc_max_age_hours) {
            Ok(policy) => policy,
            Err(e) => return Response::err(format!("invalid GC request: {e}")),
        };
        match self.run_gc(policy) {
            Ok(report) if report.total.skipped => Response::ok_gc_skipped(report.breakdown()),
            Ok(report) => Response::ok_gc(report.total.entries_evicted, report.breakdown()),
            Err(e) => Response::err(format!("gc failed: {e}")),
        }
    }

    /// Handle an upload job. If the upload queue is available, pushes to it (non-blocking).
    /// Otherwise falls back to direct upload (used in tests).
    pub async fn handle_upload(&self, job: &UploadJob) -> Response {
        if !crate::cache_key::is_valid_cache_key(&job.key) {
            return Response::err("invalid cache key");
        }
        if !crate::cache_key::is_valid_crate_name(&job.crate_name) {
            return Response::err("invalid crate name");
        }
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
        let normalized_job = match persist_upload_job(&self.config, job) {
            Ok(job) => job,
            Err(error) => {
                return Response::err(format!("persisting upload intent failed: {error:#}"));
            }
        };

        // If upload buffer is set up (server mode), push to it for async processing
        if let Some(tx) = self.upload_tx() {
            // Dedup: skip if this key is already queued or in-flight
            {
                let mut pending = self.pending_uploads.write().await;
                if !pending.insert(job.key.clone()) {
                    return Response::ok(); // already pending
                }
            }
            return match tx.send(normalized_job) {
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

        // Fallback: direct upload (no queue available). `do_upload` owns
        // breaker admission and semaphore acquisition so callers can never
        // hold a permit while waiting for breaker recovery/retry.
        self.do_upload(&normalized_job).await
    }

    /// Execute an upload directly (used by upload queue workers).
    pub async fn do_upload(&self, job: &UploadJob) -> Response {
        let key_short = key_prefix(&job.key);
        if !crate::cache_key::is_valid_cache_key(&job.key) {
            return Response::err("invalid cache key");
        }
        if !crate::cache_key::is_valid_crate_name(&job.crate_name) {
            return Response::err("invalid crate name");
        }
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
        let _write_epoch = self
            .negative_keys
            .begin_write(&job.key)
            .expect("validated upload key must admit a knowledge epoch");
        let deadline = RemoteDeadline::from_secs(self.config.remote_restore_timeout_secs);

        let Some(head_breaker) = self.remote_breaker.try_acquire(RemoteOperation::UploadHead)
        else {
            self.transfer_counters
                .uploads_suppressed
                .fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                crate_name = job.crate_name,
                key = key_short,
                "deferring upload — write breaker is degraded"
            );
            return Response::err("retryable: write breaker open");
        };

        let backend = match deadline
            .run("upload backend initialization", self.get_remote_backend())
            .await
        {
            Ok(b) => b,
            Err(e) => {
                let class = classify_remote_error(&e);
                head_breaker.failure(class, &format!("{e:#}"));
                tracing::warn!(
                    crate_name = job.crate_name,
                    key = key_short,
                    "remote backend init failed: {e:#}"
                );
                return if class.poisons_breaker() {
                    Response::err(format!("retryable: remote backend init failed: {e:#}"))
                } else {
                    Response::err(format!("remote backend init failed: {e:#}"))
                };
            }
        };
        let plan = crate::remote_plan::RemotePlanner::new(&self.config)
            .plan(crate::remote_plan::RemoteWorkload::BackgroundUpload);
        let layout = plan.layout(backend.as_ref(), remote);

        let head_queue_start = Instant::now();
        let head_semaphore = match deadline
            .run("upload HEAD queue", async {
                self.s3_semaphore
                    .acquire()
                    .await
                    .map_err(|_| anyhow::anyhow!("remote semaphore closed"))
            })
            .await
        {
            Ok(permit) => permit,
            Err(error) => {
                let class = classify_remote_error(&error);
                head_breaker.failure(class, &format!("{error:#}"));
                return Response::err("retryable: upload HEAD queue deadline");
            }
        };
        let already_exists = deadline
            .run(
                "upload HEAD",
                layout.exists_entry(&job.key, &job.crate_name),
            )
            .await;
        drop(head_semaphore);
        let _head_queue_ms = head_queue_start.elapsed().as_millis() as u64;
        let already_exists = match already_exists {
            Ok(exists) => exists,
            Err(e) => {
                let class = classify_remote_error(&e);
                head_breaker.failure(
                    class,
                    &format!("upload exists check failed ({class:?}): {e:#}"),
                );
                return if class.poisons_breaker() {
                    Response::err(format!("retryable: upload HEAD failed: {e:#}"))
                } else {
                    Response::err(format!("upload HEAD failed: {e:#}"))
                };
            }
        };
        head_breaker.success();

        if already_exists {
            self.note_key_present(&job.key, &job.crate_name).await;
            if let Err(error) = remove_upload_job(&self.config, &job.key) {
                tracing::warn!("failed to retire completed upload intent: {error:#}");
            }
            self.transfer_counters
                .uploads_skipped
                .fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                crate_name = job.crate_name,
                key = key_short,
                "skipping upload — already in remote"
            );
            return Response::ok();
        }

        tracing::debug!(
            crate_name = job.crate_name,
            key = key_short,
            remote = %remote.describe(),
            "starting remote upload"
        );

        let entry_dir = PathBuf::from(&job.entry_dir);
        let blobs_dir = self.config.store_dir().join("blobs");
        let start = Instant::now();
        let Some(put_breaker) = self.remote_breaker.try_acquire(RemoteOperation::UploadPut) else {
            self.transfer_counters
                .uploads_suppressed
                .fetch_add(1, Ordering::Relaxed);
            return Response::err("retryable: write breaker open before PUT");
        };
        let put_semaphore = match deadline
            .run("upload PUT queue", async {
                self.s3_semaphore
                    .acquire()
                    .await
                    .map_err(|_| anyhow::anyhow!("remote semaphore closed"))
            })
            .await
        {
            Ok(permit) => permit,
            Err(error) => {
                let class = classify_remote_error(&error);
                put_breaker.failure(class, &format!("{error:#}"));
                return Response::err("retryable: upload PUT queue deadline");
            }
        };
        let upload_result = deadline
            .run(
                "upload PUT",
                layout.upload_entry_until(
                    &job.key,
                    &job.crate_name,
                    &entry_dir,
                    &blobs_dir,
                    self.config.compression_level,
                    deadline.at(),
                ),
            )
            .await;
        drop(put_semaphore);
        match upload_result {
            Ok(ul) => {
                put_breaker.success();
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
                // A successful PUT flips the key positive immediately:
                // key-cache insert + negative-cache invalidation (#564).
                self.note_key_present(&job.key, &job.crate_name).await;
                if let Err(error) = remove_upload_job(&self.config, &job.key) {
                    tracing::warn!("failed to retire completed upload intent: {error:#}");
                }
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
                let class = classify_remote_error(&e);
                put_breaker.failure(class, &format!("remote upload failed ({class:?}): {e:#}"));
                tracing::warn!(
                    crate_name = job.crate_name,
                    key = key_short,
                    elapsed_ms,
                    "remote upload failed: {e:#}"
                );
                if class.poisons_breaker() {
                    Response::err(format!("retryable: upload failed: {e:#}"))
                } else {
                    Response::err(format!("upload failed: {e:#}"))
                }
            }
        }
    }

    /// Record that `key` was observed present in the remote: updates the
    /// positive key cache and clears any remembered negative result, so the
    /// two views cannot contradict each other (#564).
    async fn note_key_present(&self, key: &str, crate_name: &str) {
        self.negative_keys.confirm_present(key);
        self.key_cache
            .insert(key.to_string(), Some(crate_name))
            .await;
    }

    /// Handle a remote check: look for a cache key and download it if found.
    /// Waits for the manifest prefetch to finish first so batch downloads aren't bypassed.
    pub async fn handle_remote_check(&self, req: &RemoteCheckRequest) -> Response {
        self.handle_remote_check_started_at(req, Instant::now())
            .await
    }

    async fn handle_remote_check_started_at(
        &self,
        req: &RemoteCheckRequest,
        request_started_at: Instant,
    ) -> Response {
        if !crate::cache_key::is_valid_cache_key(&req.key) {
            return Response::err("invalid cache key");
        }
        if !crate::cache_key::is_valid_crate_name(&req.crate_name) {
            return Response::err("invalid crate name");
        }
        let expected_entry_dir = self.entry_dir_for(&req.key);
        if Path::new(&req.entry_dir) != expected_entry_dir {
            return Response::err("remote-check entry directory does not match daemon store");
        }

        // The same monotonic budget is handed through every stage below and
        // mirrored by the client socket wait. Socket-handler queueing happens
        // before this function, so derive both budgets from the accept-time
        // instant rather than restarting the clock at dispatch. Claiming then
        // also counts singleflight queue time against that original budget.
        let deadline = RemoteDeadline::from_millis_at(
            request_started_at,
            remote_check_budget_ms(self.config.remote_restore_timeout_secs, req.deadline_ms),
        );
        match self.remote_checks.claim(&req.key) {
            SingleflightClaim::Follower(follower) => follower
                .wait(deadline)
                .await
                .unwrap_or_else(|| Response::found(false)),
            SingleflightClaim::AtCapacity => {
                tracing::warn!(
                    key = key_prefix(&req.key),
                    max = REMOTE_CHECK_SINGLEFLIGHT_MAX_KEYS,
                    "remote-check singleflight at capacity; treating as miss"
                );
                Response::found(false)
            }
            SingleflightClaim::Leader(leader) => {
                let response = self.handle_remote_check_leader(req, deadline).await;
                leader.complete(response.clone());
                response
            }
        }
    }

    async fn handle_remote_check_leader(
        &self,
        req: &RemoteCheckRequest,
        deadline: RemoteDeadline,
    ) -> Response {
        let Some(remote) = &self.config.remote else {
            return Response::err("no remote configured");
        };

        let warmed = deadline
            .run("warming barrier", async {
                Ok(self.wait_for_warming(REMOTE_CHECK_WARMING_GRACE).await)
            })
            .await
            .unwrap_or(false);
        if !warmed {
            tracing::debug!(
                "remote check: warming barrier timed out after {}ms, continuing with fallback path",
                REMOTE_CHECK_WARMING_GRACE.as_millis()
            );
        }

        // Adaptive prefetch cancellation (#581, #583 P0.5): per-plan demand
        // tracking. Every distinct demanded key counts (candidate or not) —
        // the old daemon-lifetime counters only incremented on prefetched
        // keys, making the hit ratio 100% by construction so cancellation
        // never fired. The decision itself is `should_cancel_prefetch`,
        // which counts downloaded-but-not-yet-demanded keys as potential
        // hits (they may have been consumed via the wrapper's local store
        // path without reaching the daemon).
        {
            let is_prefetched = self.prefetched_keys.read().await.contains(&req.key);
            if is_prefetched {
                // Phase-0 telemetry: count each prefetched key as "used" once
                // (distinct keys; daemon-visible lower bound).
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
            let fire_cancel = {
                let mut plan = self.active_plan.lock().unwrap_or_else(|p| p.into_inner());
                match plan.as_mut() {
                    Some(p) => p.record_demand(&req.key),
                    None => false,
                }
            };
            if fire_cancel {
                let _ = self.prefetch_cancel.send(true);
                let (demanded, hits) = {
                    let plan = self.active_plan.lock().unwrap_or_else(|p| p.into_inner());
                    plan.as_ref()
                        .map(|p| (p.demanded.len(), p.demanded_candidates.len()))
                        .unwrap_or((0, 0))
                };
                tracing::info!(
                    "adaptive prefetch cancel: {hits}/{demanded} demanded keys were plan candidates, cancelling remaining downloads"
                );
            }
        }

        if deadline.check("demand preparation").is_err() {
            return Response::found(false);
        }

        let cn = &req.crate_name;
        let mut needs_head_probe = false;
        let mut head_ms = 0u64;
        let mut semaphore_wait_ms = 0u64;

        // Negative-result cache (#564): a definitive remote miss recorded
        // within the TTL answers immediately, so parallel wrappers demanding
        // the same absent key don't each pay an S3 round trip. A successful
        // upload of the key clears its entry, so this can only delay
        // visibility of another machine's upload — the same staleness class
        // the key cache's LIST refresh already has.
        if self.negative_keys.check(&req.key) {
            tracing::debug!(
                "negative cache: {} definitively missed recently, skipping remote",
                &req.key
            );
            return Response::found(false);
        }
        let knowledge = self
            .negative_keys
            .begin_observation(&req.key)
            .expect("validated remote-check key must admit a knowledge epoch");

        // Check key cache first (no semaphore needed for in-memory lookup)
        match self.key_cache.check(&req.key).await {
            Some(false) => {
                let authoritative = key_cache_miss_is_authoritative(
                    self.config.remote_key_cache_refresh_secs,
                    self.key_cache.age().await,
                );
                if authoritative {
                    tracing::debug!("key cache: {} not found (skipping remote)", &req.key);
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
                needs_head_probe = true;
            }
        }

        let backend = match deadline
            .run("demand backend initialization", self.get_remote_backend())
            .await
        {
            Ok(b) => b,
            Err(e) => {
                let class = classify_remote_error(&e);
                return if class.poisons_breaker() {
                    Response::found(false)
                } else {
                    Response::err(format!("remote backend init failed: {e}"))
                };
            }
        };
        let plan = crate::remote_plan::RemotePlanner::new(&self.config)
            .plan(crate::remote_plan::RemoteWorkload::RestoreCheck);
        let layout = plan.layout(backend.as_ref(), remote);

        if needs_head_probe {
            let Some(breaker_permit) = self.remote_breaker.try_acquire(RemoteOperation::DemandHead)
            else {
                self.transfer_counters
                    .downloads_suppressed
                    .fetch_add(1, Ordering::Relaxed);
                return Response::found(false);
            };
            let semaphore_start = Instant::now();
            let semaphore_permit = match deadline
                .run("demand HEAD queue", async {
                    self.s3_semaphore
                        .acquire()
                        .await
                        .map_err(|_| anyhow::anyhow!("remote semaphore closed"))
                })
                .await
            {
                Ok(permit) => permit,
                Err(error) => {
                    let class = classify_remote_error(&error);
                    breaker_permit.failure(class, &format!("{error:#}"));
                    return Response::found(false);
                }
            };
            semaphore_wait_ms += semaphore_start.elapsed().as_millis() as u64;
            let head_start = Instant::now();
            // Exactly one retry layer: the daemon issues one transport call.
            // In particular, there is no backoff sleep while the S3 permit is
            // held; a later request can retry after breaker policy admits it.
            let exists = deadline
                .run("demand HEAD", layout.exists_entry(&req.key, cn))
                .await;
            head_ms += head_start.elapsed().as_millis() as u64;
            drop(semaphore_permit);
            self.transfer_counters
                .remote_check_roundtrips
                .fetch_add(1, Ordering::Relaxed);
            match exists {
                Ok(false) => {
                    breaker_permit.success();
                    // A HEAD `false` is S3's definitive 404 answer — exactly
                    // what the negative cache exists to remember (#564).
                    self.negative_keys.record_miss(&knowledge);
                    return Response::found(false);
                }
                Ok(true) => {
                    breaker_permit.success();
                    if self.negative_keys.record_present(&knowledge) {
                        self.key_cache
                            .insert(req.key.clone(), Some(cn.as_str()))
                            .await;
                    }
                }
                Err(e) => {
                    let class = classify_remote_error(&e);
                    let error = format!("remote exists check failed ({class:?}): {e:#}");
                    breaker_permit.failure(class, &error);
                    // Never negative-cache a soft failure: a timeout or 5xx
                    // says nothing about whether the key exists.
                    return Response::found(false);
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
        let mut reclaimed = false;
        if let Some(notify) = claim_download(&self.downloading, &req.key).await {
            tracing::debug!("already downloading {}, waiting for completion", &req.key);
            let join_start = Instant::now();
            let join_budget = tokio::time::Instant::now() + DOWNLOAD_JOIN_BUDGET;
            let join_deadline = deadline
                .at()
                .map(tokio::time::Instant::from_std)
                .map_or(join_budget, |overall| overall.min(join_budget));
            let entry_dir = self.entry_dir_for(&req.key);
            let outcome = join_inflight_download(
                &self.downloading,
                &req.key,
                &entry_dir,
                notify,
                join_deadline,
            )
            .await;
            // Phase-0 telemetry: how often and how long RemoteCheck blocks
            // behind another task's in-flight download (total elapsed wait,
            // bumped once per waiter).
            self.prefetch_stats
                .dedup_join_waits
                .fetch_add(1, Ordering::Relaxed);
            self.prefetch_stats
                .dedup_join_wait_ms
                .fetch_add(join_start.elapsed().as_millis() as u64, Ordering::Relaxed);
            match outcome {
                JoinOutcome::Found => {
                    let was_prefetched = self.prefetched_keys.read().await.contains(&req.key);
                    return Response::found_prefetched(true, was_prefetched);
                }
                JoinOutcome::Reclaimed => reclaimed = true,
                JoinOutcome::GaveUp => {
                    // The join budget expired with a leader still holding the
                    // claim. Post-#613 a live claim means a task is actively
                    // downloading, so becoming a second, unclaimed writer here
                    // would race the leader's destructive extraction over the
                    // same entry_dir — the exact hazard the claim exists to
                    // prevent (#620, #213). Report a miss instead: the wrapper
                    // compiles locally (always safe), and later same-key
                    // demand keeps deduplicating behind the leader. The
                    // wrapper's RemoteCheck read timeout is far below this
                    // budget, so no live request is waiting on this response.
                    return Response::found(false);
                }
            }
        }
        // Leader path: reached only with the claim held — either the first
        // claim above succeeded or this task won the re-claim. The claim is
        // released on every exit path below (incl. panic) by Drop, which also
        // wakes all waiters.
        let _dl_guard = DownloadingGuard::new(self.downloading.clone(), req.key.clone());

        // The previous leader may have landed the entry between our
        // pre-re-claim meta.json check and its claim release. Re-check under
        // the claim we now hold so we don't destructively re-download over
        // the freshly published entry (#620, cross-family review finding —
        // the same re-check-under-claim defence the prefetch path uses).
        if reclaimed && self.entry_dir_for(&req.key).join("meta.json").exists() {
            let was_prefetched = self.prefetched_keys.read().await.contains(&req.key);
            return Response::found_prefetched(true, was_prefetched);
        }

        // Re-check/admit under the claim: after cooldown exactly one demand
        // GET becomes the half-open read probe.
        let Some(breaker_permit) = self.remote_breaker.try_acquire(RemoteOperation::DemandGet)
        else {
            self.transfer_counters
                .downloads_suppressed
                .fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                "remote degraded before downloading {}, treating as miss",
                &req.key
            );
            return Response::found(false);
        };

        // Acquire semaphore for download
        let semaphore_start = Instant::now();
        let semaphore_permit = match deadline
            .run("demand GET queue", async {
                self.s3_semaphore
                    .acquire()
                    .await
                    .map_err(|_| anyhow::anyhow!("remote semaphore closed"))
            })
            .await
        {
            Ok(permit) => permit,
            Err(error) => {
                let class = classify_remote_error(&error);
                breaker_permit.failure(class, &format!("{error:#}"));
                return Response::found(false);
            }
        };
        semaphore_wait_ms += semaphore_start.elapsed().as_millis() as u64;

        // Download to local store using the current remote layout, bounded by
        // the restore deadline (#327): on elapse the future is dropped (which
        // cancels the in-flight request) and the wrapper gets a miss — a
        // recompile is always cheaper than an unbounded wait. A partially
        // extracted entry_dir is safe to abandon: nothing consumes it before
        // `meta.json` lands, and the next download re-extracts from scratch —
        // the same tolerance the design already has for a daemon crash
        // mid-download.
        let entry_dir = self.entry_dir_for(&req.key);
        let blobs_dir = self.config.store_dir().join("blobs");
        let start = Instant::now();
        self.transfer_counters
            .remote_check_roundtrips
            .fetch_add(1, Ordering::Relaxed);
        let download_result = deadline
            .run(
                "demand GET and extraction",
                layout.download_entry_until(&req.key, cn, &entry_dir, &blobs_dir, deadline.at()),
            )
            .await;
        drop(semaphore_permit);

        match download_result {
            Ok(dl) => {
                breaker_permit.success();
                if self.negative_keys.record_present(&knowledge) {
                    self.key_cache
                        .insert(req.key.clone(), Some(cn.as_str()))
                        .await;
                }
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
            Err(e) if classify_remote_error(&e) == RemoteErrorClass::Miss => {
                // GET 404 = clean miss (#485 Phase 0). Reached when a
                // key-cache positive was stale (upload evicted/GC'd) or the
                // direct-GET path raced an upload. Correct the cache so the
                // next check doesn't repeat the GET, and report a miss — the
                // wrapper compiles as usual. Not a transfer failure: the
                // remote answered, so the breaker counts it as a success, and
                // the 404 is definitive, so the negative cache remembers it
                // (#564).
                tracing::debug!("remote GET 404 for {} — treating as miss", &req.key);
                breaker_permit.success();
                if self.negative_keys.record_miss(&knowledge) {
                    self.key_cache.remove(&req.key).await;
                }
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
                // Feed the breaker with the failure class (#327) so a dead or
                // stalling remote degrades and later restores skip S3
                // entirely. A Timeout (transport deadline or the restore
                // deadline above) reports a plain miss: the wrapper's answer
                // is "recompile locally" either way, and an error response
                // would suggest the check itself malfunctioned.
                let class = classify_remote_error(&e);
                breaker_permit
                    .failure(class, &format!("remote download failed ({class:?}): {e:#}"));
                if matches!(
                    class,
                    RemoteErrorClass::Timeout | RemoteErrorClass::Transient
                ) {
                    tracing::warn!(
                        "remote download of {} failed after {elapsed_ms}ms — treating as miss",
                        &req.key
                    );
                    return Response::found(false);
                }
                Response::err(format!("remote download failed: {e}"))
            }
        }
    }

    /// Handle a batch remote check concurrently.
    pub async fn handle_batch_remote_check(
        self: &Arc<Self>,
        req: &BatchRemoteCheckRequest,
    ) -> Response {
        self.handle_batch_remote_check_started_at(req, Instant::now())
            .await
    }

    async fn handle_batch_remote_check_started_at(
        self: &Arc<Self>,
        req: &BatchRemoteCheckRequest,
        request_started_at: Instant,
    ) -> Response {
        let futures: Vec<_> = req
            .checks
            .iter()
            .map(|check| self.handle_remote_check_started_at(check, request_started_at))
            .collect();
        let results = futures::future::join_all(futures).await;
        Response::ok_batch(results)
    }

    /// Handle a prefetch request: fire-and-forget background downloads.
    /// Spawns a single coordinator task that processes keys with bounded concurrency.
    pub async fn handle_prefetch(self: &Arc<Self>, req: &PrefetchRequest) -> Response {
        if !self.config.prefetch_enabled {
            tracing::debug!("prefetch request ignored: speculative prefetch disabled");
            return Response::ok();
        }
        let Some(remote) = &self.config.remote else {
            return Response::err("no remote configured");
        };

        let init_deadline = RemoteDeadline::from_secs(self.config.remote_restore_timeout_secs);
        let backend = match init_deadline
            .run("prefetch backend initialization", self.get_remote_backend())
            .await
        {
            Ok(backend) => backend,
            Err(error) => return Response::err(format!("remote backend init failed: {error:#}")),
        };

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

        // Explicitly requested whole-remote warm (#615). Never inferred from an
        // empty candidate list: that made "nothing to prefetch" mean "download
        // the bucket".
        if req.warm_all {
            let deadline = RemoteDeadline::from_secs(self.config.remote_restore_timeout_secs);
            let s3_keys = if let Some(breaker) = self
                .remote_breaker
                .try_acquire(RemoteOperation::WarmAllList)
            {
                let result = match deadline
                    .run("warm-all LIST queue", async {
                        self.s3_semaphore
                            .acquire()
                            .await
                            .map_err(|_| anyhow::anyhow!("remote semaphore closed"))
                    })
                    .await
                {
                    Ok(semaphore) => {
                        let result = deadline
                            .run(
                                "warm-all LIST",
                                crate::remote_plan::RemotePlanner::new(&self.config)
                                    .plan(crate::remote_plan::RemoteWorkload::KeyDiscovery)
                                    .layout(backend.as_ref(), remote)
                                    .list_keys(),
                            )
                            .await;
                        drop(semaphore);
                        result
                    }
                    Err(error) => Err(error),
                };
                match result {
                    Ok(keys) => {
                        breaker.success();
                        Some(keys)
                    }
                    Err(error) => {
                        let class = classify_remote_error(&error);
                        breaker.failure(class, &format!("{error:#}"));
                        None
                    }
                }
            } else {
                None
            };
            for (key, crate_name) in s3_keys.unwrap_or_default() {
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

        // Key budget (kunobi-ninja/kache#616). Applied AFTER the filters above,
        // so the budget bounds work actually to be done rather than being spent
        // on candidates that are already local or already in flight.
        let offered = keys_to_fetch.len();
        let dropped_over_key_budget =
            prefetch_key_budget_overflow(offered, self.config.prefetch_max_keys);
        if dropped_over_key_budget > 0 {
            keys_to_fetch.truncate(offered - dropped_over_key_budget);
        }

        let count = keys_to_fetch.len();
        if count == 0 {
            tracing::info!("prefetch: nothing to fetch");
            return Response::ok();
        }

        // Never silently truncate: a plan cut short by a budget must not look
        // like a plan that had nothing more to offer (#616).
        if dropped_over_key_budget > 0 {
            self.prefetch_stats
                .keys_over_budget
                .fetch_add(dropped_over_key_budget as u64, Ordering::Relaxed);
            tracing::warn!(
                offered,
                admitted = count,
                dropped = dropped_over_key_budget,
                max_keys = self.config.prefetch_max_keys,
                "prefetch: plan truncated by the key budget"
            );
        }

        // Candidates are deliberately NOT claimed here (kunobi-ninja/kache#613).
        // Claiming the whole plan up front put every candidate in `downloading`
        // before any of them was being downloaded, so a wrapper demanding a key
        // deep in the plan parked on its `Notify` for up to
        // `DOWNLOAD_JOIN_BUDGET` waiting for a leader that had not started —
        // and never reached the point of taking one of the S3 permits the
        // prefetch cap reserves for demand. Each task claims its own key
        // immediately before downloading it instead, so demand never queues
        // behind speculation.

        // Spawn a single coordinator task with bounded concurrency
        let daemon = Arc::clone(self);
        let remote_config = remote.clone();
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

            // Byte and time budgets (#616). Both bound what this plan may still
            // START; work already in flight is left to finish, because
            // cancelling a live download throws away bytes already paid for.
            //
            // The byte budget is therefore SOFT: overshoot is bounded by what
            // was in flight when it tripped, at most `max_concurrent` objects.
            // A hard cap needs counted, cancellable reads in the backend.
            let byte_budget = daemon.config.prefetch_max_bytes;
            let bytes_at_start = daemon
                .prefetch_stats
                .bytes_downloaded
                .load(Ordering::Relaxed);
            let deadline = match daemon.config.prefetch_deadline_secs {
                0 => None,
                secs => Some(Instant::now() + Duration::from_secs(secs)),
            };

            let mut keys_iter = keys_to_fetch.into_iter().peekable();
            while let Some((key, crate_name, entry_dir)) = keys_iter.next() {
                if let Some(deadline) = deadline
                    && Instant::now() >= deadline
                {
                    let dropped = 1 + keys_iter.count() as u64;
                    daemon
                        .prefetch_stats
                        .keys_over_budget
                        .fetch_add(dropped, Ordering::Relaxed);
                    tracing::warn!(
                        dropped,
                        deadline_secs = daemon.config.prefetch_deadline_secs,
                        "prefetch: plan truncated by the time budget"
                    );
                    break;
                }

                {
                    let spent = daemon
                        .prefetch_stats
                        .bytes_downloaded
                        .load(Ordering::Relaxed)
                        .saturating_sub(bytes_at_start);
                    if prefetch_byte_budget_exhausted(byte_budget, spent) {
                        let dropped = 1 + keys_iter.count() as u64;
                        daemon
                            .prefetch_stats
                            .keys_over_budget
                            .fetch_add(dropped, Ordering::Relaxed);
                        tracing::warn!(
                            dropped,
                            spent_bytes = spent,
                            max_bytes = byte_budget,
                            in_flight = in_flight.len(),
                            "prefetch: plan truncated by the byte budget (soft: in-flight \
                             downloads still finish)"
                        );
                        break;
                    }
                }

                // Check for adaptive cancellation
                if *cancel_rx.borrow() {
                    tracing::info!("prefetch: cancelled by adaptive hit-rate check");
                    // Nothing to drain: an un-started candidate holds no claim
                    // (#613), so no waiter can be parked on one. Tasks already
                    // in flight keep their own `DownloadingGuard`, which wakes
                    // their waiters when it drops.
                    let cancelled = 1 + keys_iter.count() as u64;
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
                let remote_cfg = remote_config.clone();
                let remote_backend = backend.clone();
                let download_plan = crate::remote_plan::RemotePlanner::new(&d.config)
                    .plan(crate::remote_plan::RemoteWorkload::Prefetch);
                let plan_deadline = deadline;
                in_flight.push(tokio::spawn(async move {
                    let item_deadline =
                        RemoteDeadline::from_secs(d.config.remote_restore_timeout_secs)
                            .min(RemoteDeadline::from_instant(plan_deadline));
                    // The entry may have landed since planning (an interactive
                    // RemoteCheck, or another coordinator) — re-check before
                    // spending a gate slot on it.
                    if entry_dir.exists() {
                        return;
                    }
                    let knowledge = d
                        .negative_keys
                        .begin_observation(&key)
                        .expect("validated prefetch key must admit a knowledge epoch");
                    let Some(breaker_permit) =
                        d.remote_breaker.try_acquire(RemoteOperation::PrefetchGet)
                    else {
                        d.transfer_counters
                            .downloads_suppressed
                            .fetch_add(1, Ordering::Relaxed);
                        return;
                    };
                    // Daemon-wide speculative gate FIRST, then the shared S3
                    // permit: bounds prefetch across ALL coordinators so the
                    // interactive reserve holds even when startup prefetch
                    // overlaps a BuildStarted plan (#485, cross-family review).
                    let gate = match item_deadline
                        .run("prefetch gate queue", async {
                            d.prefetch_gate
                                .clone()
                                .acquire_owned()
                                .await
                                .map_err(|_| anyhow::anyhow!("prefetch gate closed"))
                        })
                        .await
                    {
                        Ok(permit) => permit,
                        Err(error) => {
                            let class = classify_remote_error(&error);
                            breaker_permit.failure(class, &format!("{error:#}"));
                            return;
                        }
                    };
                    let semaphore_start = Instant::now();
                    let semaphore = match item_deadline
                        .run("prefetch remote queue", async {
                            sem.acquire()
                                .await
                                .map_err(|_| anyhow::anyhow!("remote semaphore closed"))
                        })
                        .await
                    {
                        Ok(permit) => permit,
                        Err(error) => {
                            drop(gate);
                            let class = classify_remote_error(&error);
                            breaker_permit.failure(class, &format!("{error:#}"));
                            return;
                        }
                    };
                    let semaphore_wait_ms = semaphore_start.elapsed().as_millis() as u64;
                    // Claim LAST, once this task is ready to download right
                    // now (#613): the window where a key sits claimed but
                    // idle is what made demand park behind speculation, so it
                    // is kept to the span of the download itself. Someone else
                    // holding the claim means a demand-side download is
                    // already in flight — speculation has nothing to add, so
                    // drop the candidate rather than joining the wait.
                    if claim_download(&d.downloading, &key).await.is_some() {
                        tracing::debug!("prefetch: {} already claimed, skipping", key_prefix(&key));
                        return;
                    }
                    // Released on every exit path below (including panic) by
                    // Drop, which also wakes anyone parked on this key.
                    let _dl_guard = DownloadingGuard::new(d.downloading.clone(), key.clone());
                    // Re-check under the claim: a leader that landed the entry
                    // between the check above and this claim would otherwise be
                    // followed by a destructive re-extraction over a directory
                    // a wrapper may already be hardlinking out of.
                    if entry_dir.exists() {
                        return;
                    }
                    let blobs_dir = d.config.store_dir().join("blobs");
                    let start = Instant::now();
                    let download_result = item_deadline
                        .run(
                            "prefetch GET and extraction",
                            download_plan
                                .layout(remote_backend.as_ref(), &remote_cfg)
                                .download_entry_until(
                                    &key,
                                    &crate_name,
                                    &entry_dir,
                                    &blobs_dir,
                                    item_deadline.at(),
                                ),
                        )
                        .await;
                    drop(semaphore);
                    drop(gate);

                    match download_result {
                        Ok(dl) => {
                            breaker_permit.success();
                            if d.negative_keys.record_present(&knowledge) {
                                d.key_cache
                                    .insert(key.clone(), Some(crate_name.as_str()))
                                    .await;
                            }
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
                            // Per-plan attribution (#583 P0.5): byte-accurate
                            // downloaded set for the session summary.
                            {
                                let mut plan =
                                    d.active_plan.lock().unwrap_or_else(|p| p.into_inner());
                                if let Some(p) = plan.as_mut() {
                                    p.record_download(&key, dl.compressed_bytes);
                                }
                            }
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
                            let class = classify_remote_error(&e);
                            if class == RemoteErrorClass::Miss {
                                breaker_permit.success();
                                if d.negative_keys.record_miss(&knowledge) {
                                    d.key_cache.remove(&key).await;
                                }
                                return;
                            }
                            breaker_permit.failure(
                                class,
                                &format!("prefetch download failed ({class:?}): {e:#}"),
                            );
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
    /// Install a new active plan, finalizing (and summarizing) any previous
    /// one as `superseded`, and reset the adaptive-cancel latch so one bad
    /// build can't poison the next (#581).
    fn install_plan(
        &self,
        session_id: &str,
        plan_id: &str,
        plan_source: &'static str,
        candidates: impl Iterator<Item = String>,
    ) {
        let _ = self.prefetch_cancel.send(false);
        let plan = ActivePlan::new(
            session_id.to_string(),
            plan_id.to_string(),
            plan_source,
            candidates.collect(),
            self.prefetch_stats
                .list_requests_total
                .load(Ordering::Relaxed),
            self.prefetch_stats
                .list_duration_ms_total
                .load(Ordering::Relaxed),
        );
        let prev = {
            let mut slot = self.active_plan.lock().unwrap_or_else(|p| p.into_inner());
            slot.replace(plan)
        };
        if let Some(prev) = prev {
            self.emit_plan_summary(prev, "superseded");
        }
    }

    /// Finalize the active plan if it has been inactive for `inactivity_ms`.
    /// Called from the periodic sweep; cargo gives no positive end-of-build
    /// signal, so inactivity IS the end signal (#583 P0.5).
    pub(crate) fn finalize_inactive_plan(&self, inactivity_ms: u64) {
        let prev = {
            let mut slot = self.active_plan.lock().unwrap_or_else(|p| p.into_inner());
            match slot.as_ref() {
                Some(p) if epoch_ms().saturating_sub(p.last_activity_ms) >= inactivity_ms => {
                    slot.take()
                }
                _ => None,
            }
        };
        if let Some(prev) = prev {
            self.emit_plan_summary(prev, "inactivity");
        }
    }

    /// Append the per-session summary to `summaries.jsonl`. Best-effort:
    /// telemetry must never fail the daemon.
    fn emit_plan_summary(&self, plan: ActivePlan, closure_reason: &str) {
        let used_bytes = plan.used_bytes();
        let downloaded_bytes: u64 = plan.downloaded.values().sum();
        let event = crate::events::BuildSummaryEvent {
            ts: chrono::Utc::now(),
            schema: 1,
            session_id: plan.session_id,
            root: String::new(),
            plan_source: plan.plan_source.to_string(),
            plan_id: plan.plan_id,
            closure_reason: closure_reason.to_string(),
            started_at_ms: plan.started_at_ms,
            last_activity_ms: plan.last_activity_ms,
            candidate_keys: plan.candidates.len() as u64,
            downloaded_keys: plan.downloaded.len() as u64,
            downloaded_bytes,
            used_keys: plan.used.len() as u64,
            used_bytes,
            demanded_keys: plan.demanded.len() as u64,
            demanded_candidate_keys: plan.demanded_candidates.len() as u64,
            cancelled: plan.cancelled,
            list_requests: self
                .prefetch_stats
                .list_requests_total
                .load(Ordering::Relaxed)
                .saturating_sub(plan.list_requests_at_install),
            list_duration_ms: self
                .prefetch_stats
                .list_duration_ms_total
                .load(Ordering::Relaxed)
                .saturating_sub(plan.list_duration_ms_at_install),
        };
        let path = self.config.summary_log_path();
        if let Err(e) = crate::events::log_summary(&path, &event) {
            tracing::debug!("failed to write build summary: {e}");
        }
    }

    pub async fn handle_build_started(self: &Arc<Self>, req: &BuildStartedRequest) -> Response {
        let Some(_remote) = &self.config.remote else {
            return Response::err("no remote configured");
        };
        if speculative_prefetch_disabled(self.config.prefetch_enabled) {
            tracing::debug!("build-started: speculative prefetch disabled");
            return Response::ok();
        }

        // A new session supersedes the previous plan even when THIS build ends
        // up with no plan (DoNothing, empty candidates, planning failure) —
        // otherwise the old plan would keep absorbing the new build's demands,
        // never go inactive, and never finalize (cross-family review finding).
        {
            let prev = {
                let mut slot = self.active_plan.lock().unwrap_or_else(|p| p.into_inner());
                match slot.as_ref() {
                    Some(p) if p.session_id != req.session_id => slot.take(),
                    _ => None,
                }
            };
            if let Some(prev) = prev {
                self.emit_plan_summary(prev, "superseded");
            }
        }

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
                        self.install_plan(
                            &req.session_id,
                            plan_id.as_deref().unwrap_or(""),
                            "advisory",
                            prefetch_req.keys.iter().map(|(k, _)| k.clone()),
                        );
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
        self.install_plan(
            &req.session_id,
            "",
            "fallback",
            prefetch_req.keys.iter().map(|(k, _)| k.clone()),
        );
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
            // Physical bytes, matching `evict()`'s own trigger (#608).
            let size = store.physical_size()?;
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

    /// Core GC logic with an explicit policy and per-policy result accounting.
    fn run_gc(&self, policy: GcPolicy) -> Result<GcRunReport> {
        let start = Instant::now();
        let mode = policy.mode();
        // Cross-process GC mutual exclusion (kunobi-ninja/kache#326): if another
        // GC driver (a manual `kache gc`, a second daemon) holds gc.lock, skip
        // this run rather than double-scan and contend. Held until run_gc returns.
        let _gc_lock = match self.with_store(|store| store.try_gc_lock())? {
            Some(lock) => lock,
            None => {
                tracing::info!("gc.lock held by another GC; skipping this run");
                return Ok(GcRunReport::skipped(mode));
            }
        };
        let (dedup_stats, evict_stats, age_evict_stats, incremental_cleaned, orphan_stats) =
            self.with_store(|store| {
                // Backfill content_hash for legacy entries
                let backfilled = store.backfill_content_hashes().unwrap_or(0);
                if backfilled > 0 {
                    tracing::info!("backfilled {backfilled} content hashes");
                }

                // Backfill rebuild cost for entries written before it was
                // indexed (#594), so a value-aware policy has data to work with.
                let costs = store.backfill_compile_times().unwrap_or(0);
                if costs > 0 {
                    tracing::info!("backfilled {costs} compile times");
                }

                // Backfill entry→blob rows for entries written before the
                // table existed (#608), so eviction can rank on the bytes an
                // entry would actually free.
                let mapped = store.backfill_entry_blobs().unwrap_or(0);
                if mapped > 0 {
                    tracing::info!("backfilled {mapped} entry blob maps");
                }

                // Bound the post-eviction demand log, and report what it says
                // so far: a high demand rate means eviction is discarding
                // entries the build still wants (#594).
                let pruned = store
                    .prune_tombstones(crate::store::TOMBSTONE_RETENTION_DAYS)
                    .unwrap_or(0);
                if let Ok((tracked, demanded)) = store.tombstone_stats()
                    && tracked > 0
                {
                    tracing::info!(
                        tracked,
                        demanded,
                        pruned,
                        demand_rate_pct = demanded * 100 / tracked.max(1),
                        "gc: post-eviction demand"
                    );
                }
                // The #594 policy comparison: demand rate on entries the
                // value-density shadow would have KEPT vs entries it agreed
                // to evict. A markedly higher rate on the kept cohort is the
                // evidence for flipping the live policy; comparable rates
                // are the evidence against.
                if let Ok(split) = store.shadow_demand_split()
                    && split.agreed + split.shadow_kept > 0
                {
                    tracing::info!(
                        shadow_agreed = split.agreed,
                        shadow_agreed_demanded = split.agreed_demanded,
                        shadow_kept = split.shadow_kept,
                        shadow_kept_demanded = split.shadow_kept_demanded,
                        "gc: post-eviction demand by shadow verdict (value-density, #594)"
                    );
                }

                let (dedup_stats, age_evict_stats, evict_stats) = match policy {
                    GcPolicy::ExplicitAge { hours } => (
                        crate::store::GcStats::default(),
                        store.evict_older_than(hours)?,
                        crate::store::GcStats::default(),
                    ),
                    GcPolicy::Automatic { max_age_hours } => {
                        // Expire opt-in stale entries first. Duplicate and size
                        // pressure then observe the reduced physical store and
                        // cannot evict fresh entries for pressure age already
                        // relieved.
                        let age_stats = if max_age_hours > 0 {
                            store.evict_older_than(max_age_hours)?
                        } else {
                            crate::store::GcStats::default()
                        };
                        let duplicate_stats = store.evict_duplicate_entries().unwrap_or_default();
                        let size_stats = store.evict()?;
                        (duplicate_stats, age_stats, size_stats)
                    }
                };
                if dedup_stats.entries_evicted > 0 {
                    tracing::info!("evicted {} duplicate entries", dedup_stats.entries_evicted);
                }
                if age_evict_stats.entries_evicted > 0 {
                    tracing::info!(
                        "evicted {} entries by age policy",
                        age_evict_stats.entries_evicted
                    );
                }

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

                Ok((
                    dedup_stats,
                    evict_stats,
                    age_evict_stats,
                    incremental_cleaned,
                    orphan_stats,
                ))
            })?;

        // Clean up stale tool-version cache files (rustc-ver-*.txt, linker-ver-*.txt).
        // Each toolchain update leaves behind orphaned files keyed by the old binary mtime.
        Self::clean_tool_version_caches(&self.config.cache_dir);

        if incremental_cleaned > 0 {
            tracing::info!("cleaned {incremental_cleaned} registered incremental dirs");
        }

        // Aggregate stats
        let stats = crate::store::GcStats {
            entries_evicted: dedup_stats.entries_evicted
                + evict_stats.entries_evicted
                + age_evict_stats.entries_evicted,
            bytes_freed: dedup_stats.bytes_freed
                + evict_stats.bytes_freed
                + age_evict_stats.bytes_freed
                + orphan_stats.bytes_reclaimed,
            blobs_removed: dedup_stats.blobs_removed
                + evict_stats.blobs_removed
                + age_evict_stats.blobs_removed
                + orphan_stats.removed,
            duration_ms: start.elapsed().as_millis() as u64,
            skipped: false,
            // Policies may select the same protected entry. Count-only stats
            // cannot form an exact union, so report the largest single-sweep
            // count as a non-duplicating lower bound. Explicit age runs have no
            // size sweep and must report their age-policy pins directly.
            entries_pinned: match policy {
                GcPolicy::ExplicitAge { .. } => age_evict_stats.entries_pinned,
                GcPolicy::Automatic { .. } => dedup_stats
                    .entries_pinned
                    .max(age_evict_stats.entries_pinned)
                    .max(evict_stats.entries_pinned),
            },
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

        Ok(GcRunReport {
            mode,
            duplicate: dedup_stats,
            age: age_evict_stats,
            size: evict_stats,
            total: stats,
        })
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
pub fn run_server(config: &Config, provenance: &crate::config::ConfigFileProvenance) -> Result<()> {
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

    rt.block_on(server_main(config, provenance, coord))
}

fn start_manifest_warming(daemon: &Arc<Daemon>) -> Option<tokio::task::JoinHandle<()>> {
    if should_start_speculative_prefetch(
        daemon.config.remote.is_some(),
        daemon.config.prefetch_enabled,
    ) {
        let manifest_daemon = daemon.clone();
        Some(tokio::spawn(async move {
            manifest_prefetch(&manifest_daemon).await;
            manifest_daemon.signal_warming_complete();
        }))
    } else {
        // No warming task will run when no remote exists or speculation is
        // disabled, so release exact remote checks immediately.
        daemon.signal_warming_complete();
        None
    }
}

async fn server_main(
    config: &Config,
    provenance: &crate::config::ConfigFileProvenance,
    coord: DaemonCoordFile,
) -> Result<()> {
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
    // Fire-and-forget (#588): the tmutil half runs on a detached thread with
    // its own timeout, so daemon readiness never gates on backupd.
    let _ = crate::store::exclude_from_indexing(&config.cache_dir);

    // The daemon is the longest-lived writer of the WAL index, so a cache dir on
    // a network or guest-visible mount is worth flagging here too — its log is
    // where a user looks after the fact, and a daemonised setup may never show a
    // wrapper's stderr (kunobi-ninja/kache#415). Log-only: the wrapper owns the
    // stderr advisory and its once-per-session dedup.
    match crate::cache_fs::classify(&crate::cache_fs::probe(&config.cache_dir)) {
        crate::cache_fs::CacheFsVerdict::NotLocal { name } => tracing::warn!(
            cache_dir = %config.cache_dir.display(),
            filesystem = %name,
            "cache directory is not on host-local storage: the WAL index needs working \
             file locking and a single writing machine, and can be corrupted on a shared \
             or network mount. Set KACHE_CACHE_DIR to a local path; to share artifacts \
             between machines use a remote cache instead."
        ),
        verdict => tracing::debug!(
            cache_dir = %config.cache_dir.display(),
            ?verdict,
            "cache filesystem locality check"
        ),
    }

    // Set up two-channel upload pipeline:
    //   handler → unbounded buffer → enqueue task → bounded worker channel → workers → S3
    let (buffer_tx, mut buffer_rx) = tokio::sync::mpsc::unbounded_channel::<UploadJob>();
    let num_workers = (config.s3_concurrency as usize).max(1);
    let (worker_tx, worker_rx) = tokio::sync::mpsc::channel::<UploadJob>(num_workers * 2);
    let worker_rx = Arc::new(tokio::sync::Mutex::new(worker_rx));

    let daemon_inner = Daemon::new_with_provenance(config.clone(), provenance);
    daemon_inner.set_upload_tx(buffer_tx.clone());
    let daemon = Arc::new(daemon_inner);

    match load_upload_jobs(&config) {
        Ok(jobs) => {
            let replay_count = jobs.len();
            for job in jobs {
                if daemon.pending_uploads.write().await.insert(job.key.clone())
                    && buffer_tx.send(job).is_err()
                {
                    tracing::warn!("upload replay buffer closed during startup");
                    break;
                }
            }
            if replay_count > 0 {
                tracing::info!(replay_count, "replayed durable upload intents");
            }
        }
        Err(error) => tracing::warn!("failed to replay durable upload intents: {error:#}"),
    }
    // The daemon-owned sender is the lifecycle handle. Keeping this setup
    // clone alive would prevent graceful shutdown from closing the buffer.
    drop(buffer_tx);

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
                let resp = loop {
                    let response = d.do_upload(&job).await;
                    let retryable = response
                        .error
                        .as_deref()
                        .is_some_and(|error| error.starts_with("retryable:"));
                    if !retryable {
                        break response;
                    }
                    tracing::debug!(
                        key = key_prefix(&job.key),
                        retry_after_secs = UPLOAD_RETRY_DELAY.as_secs(),
                        "durable upload deferred"
                    );
                    // No S3 permit is held here: `do_upload` owns and releases
                    // each permit before returning a retryable outcome.
                    tokio::time::sleep(UPLOAD_RETRY_DELAY).await;
                };
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
    // Session-summary sweep (#583 P0.5): finalize an active prefetch plan
    // once its build session has gone quiet. 60s granularity against a
    // 5-minute inactivity window is plenty; the summary lands in
    // `summaries.jsonl` where `kache report` joins it with per-crate events.
    let sweep_daemon = daemon.clone();
    tokio::spawn(async move {
        const SESSION_INACTIVITY_MS: u64 = 300_000;
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            sweep_daemon.finalize_inactive_plan(SESSION_INACTIVITY_MS);
        }
    });

    let gc_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(6 * 3600));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            tracing::info!("periodic GC sweep starting");
            // Offload the blocking sweep so it never stalls an async worker —
            // the accept loop and in-flight RemoteCheck stay responsive (#281).
            let gc = gc_daemon.clone();
            match tokio::task::spawn_blocking(move || {
                gc.run_gc(GcPolicy::Automatic {
                    max_age_hours: gc.config.gc_max_age_hours,
                })
            })
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => tracing::warn!("periodic GC failed: {e}"),
                Err(e) => tracing::warn!("periodic GC task panicked: {e}"),
            }
        }
    });

    // The remote key cache only serves speculative planning. Exact-key remote
    // checks and uploads do not depend on it, so disabling prefetch also avoids
    // the expensive whole-remote LIST entirely.
    let cache_handle = if should_start_speculative_prefetch(
        config.remote.is_some(),
        config.prefetch_enabled,
    ) {
        let cache_daemon = daemon.clone();
        let refresh_secs = config.remote_key_cache_refresh_secs;
        Some(tokio::spawn(async move {
            // Initial population with retry backoff
            let mut delay = std::time::Duration::from_secs(1);
            for attempt in 1..=5 {
                match populate_key_cache(&cache_daemon).await {
                    Ok(count) => {
                        tracing::info!("remote key cache populated: {count} keys");
                        break;
                    }
                    Err(e) => {
                        tracing::warn!(
                            "remote key cache population attempt {attempt}/5 failed: {e}"
                        );
                        if attempt < 5 {
                            tokio::time::sleep(delay).await;
                            delay *= 2;
                        }
                    }
                }
            }

            if key_cache_periodic_refresh_disabled(refresh_secs) {
                tracing::info!("remote key cache periodic refresh disabled");
                return;
            }

            // Periodic refresh
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(refresh_secs));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            interval.tick().await; // skip immediate tick
            let mut consecutive_refresh_failures = 0u32;
            loop {
                interval.tick().await;
                match populate_key_cache(&cache_daemon).await {
                    Ok(count) => {
                        if consecutive_refresh_failures > 0 {
                            tracing::info!(
                                "remote key cache refresh recovered after {consecutive_refresh_failures} failed attempt(s)"
                            );
                            consecutive_refresh_failures = 0;
                        }
                        tracing::debug!("remote key cache refreshed: {count} keys");
                    }
                    Err(e) => {
                        consecutive_refresh_failures += 1;
                        if should_warn_key_cache_refresh_failure(consecutive_refresh_failures) {
                            tracing::warn!(
                                "remote key cache refresh failed (attempt {consecutive_refresh_failures}): {e}"
                            );
                        } else {
                            tracing::debug!(
                                "remote key cache refresh failed (attempt {consecutive_refresh_failures}): {e}"
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
    // The shared launcher also releases the warming barrier immediately when no
    // remote exists or speculative prefetch is disabled.
    let manifest_handle = start_manifest_warming(&daemon);

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
    let config_provenance = provenance.clone();
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
            if crate::config::config_file_has_changed(&config_provenance) {
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
/// download of the same key before giving up and reporting a remote miss
/// (never a second, unclaimed download — see [`JoinOutcome::GaveUp`]).
const DOWNLOAD_JOIN_BUDGET: Duration = Duration::from_secs(30);

/// Outcome of waiting behind another task's in-flight download of a key.
#[derive(Debug, PartialEq, Eq)]
enum JoinOutcome {
    /// The leader landed the entry (meta.json exists); use it.
    Found,
    /// The leader failed and this task won the atomic re-claim: it is now
    /// the leader and MUST release the claim via [`DownloadingGuard`].
    Reclaimed,
    /// The join budget expired with a leader still holding the claim. The
    /// caller must treat the key as a remote miss — downloading without the
    /// claim would race the live leader's destructive extraction over the
    /// same entry dir (#620).
    GaveUp,
}

/// Park behind an in-flight download of `key` until the leader lands the
/// entry, fails (and this task wins the re-claim), or `deadline` passes with
/// a leader still holding the claim. Never elects a second concurrent writer:
/// the old behavior of proceeding without a claim after the budget let a
/// waiter extract over a directory the wedged leader was still writing, or a
/// wrapper was hardlinking out of (#620).
async fn join_inflight_download(
    downloading: &RwLock<HashMap<String, Arc<Notify>>>,
    key: &str,
    entry_dir: &Path,
    mut notify: Arc<Notify>,
    deadline: tokio::time::Instant,
) -> JoinOutcome {
    loop {
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
            //
            // The read guard MUST be dropped before awaiting the Notify: a
            // match scrutinee's temporaries live through the arms, and
            // holding the read lock across the await deadlocks against
            // DownloadingGuard's drop, which needs the write lock to remove
            // the claim and only notifies waiters after that removal — every
            // waiter would sit out its full deadline instead of waking
            // promptly (#620, cross-family review finding).
            let current = {
                let guard = downloading.read().await;
                guard.get(key).cloned()
            };
            if let Some(cur) = current {
                if Arc::ptr_eq(&cur, &notify) {
                    timed_out = tokio::time::timeout_at(deadline, notified).await.is_err();
                } else if tokio::time::Instant::now() < deadline {
                    adopt = Some(cur);
                } else {
                    // Generation changed but the budget is gone: fall
                    // through to the meta.json check + re-claim with
                    // the timeout semantics.
                    timed_out = true;
                }
            }
        }
        if let Some(cur) = adopt {
            notify = cur;
            continue;
        }
        // Woken (leader's guard dropped), leader already gone, or
        // budget exhausted — if the leader landed the entry, use it.
        if entry_dir.join("meta.json").exists() {
            return JoinOutcome::Found;
        }
        // The leader failed (or was cancelled). Re-claim atomically:
        // insert-if-absent elects exactly ONE waiter as the new
        // leader. (The old poll-based code re-inserted the key while
        // IGNORING the result, so every waiter that exhausted the poll
        // budget proceeded as an "owner" and double-downloaded — the
        // very race the #213 claim exists to prevent.)
        match claim_download(downloading, key).await {
            None => return JoinOutcome::Reclaimed,
            Some(next) => {
                if timed_out {
                    // Budget exhausted and another task still holds the
                    // claim. Give up as a miss rather than become a second
                    // writer (#620).
                    tracing::warn!(
                        key = key_prefix(key),
                        "download dedup wait exceeded {DOWNLOAD_JOIN_BUDGET:?} with the \
                         leader still holding the claim; treating as remote miss"
                    );
                    return JoinOutcome::GaveUp;
                }
                // A different waiter won the re-claim; keep waiting,
                // now on the NEW leader's Notify.
                notify = next;
            }
        }
    }
}

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
                        // Capture the request's monotonic age before it can park
                        // behind the handler limiter. A later dispatch must not
                        // restart a client whose end-to-end budget already ran
                        // out in this queue.
                        let request_started_at = Instant::now();
                        last_activity = request_started_at;
                        let d = daemon.clone();
                        let flag = shutdown_flag.clone();
                        let notify = shutdown_notify.clone();
                        let limiter = conn_limiter.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection_after_queue(
                                stream,
                                &d,
                                &flag,
                                &notify,
                                limiter,
                                request_started_at,
                            )
                            .await
                            {
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
    let remote = daemon
        .config
        .remote
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no remote configured"))?;

    let Some(breaker_permit) = daemon
        .remote_breaker
        .try_acquire(RemoteOperation::ListIndex)
    else {
        anyhow::bail!("remote degraded — key cache refresh suppressed");
    };
    let deadline = RemoteDeadline::from_secs(daemon.config.remote_restore_timeout_secs);
    let backend = match deadline
        .run("index backend initialization", daemon.get_remote_backend())
        .await
    {
        Ok(backend) => backend,
        Err(error) => {
            let class = classify_remote_error(&error);
            breaker_permit.failure(class, &format!("{error:#}"));
            return Err(error);
        }
    };
    let listing_epoch = daemon.negative_keys.listing_epoch();
    let key_cache_revision = daemon.key_cache.refresh_revision();

    let list_start = Instant::now();
    daemon
        .prefetch_stats
        .list_requests_total
        .fetch_add(1, Ordering::Relaxed);
    let semaphore = match deadline
        .run("index LIST queue", async {
            daemon
                .s3_semaphore
                .acquire()
                .await
                .map_err(|_| anyhow::anyhow!("remote semaphore closed"))
        })
        .await
    {
        Ok(permit) => permit,
        Err(error) => {
            let class = classify_remote_error(&error);
            breaker_permit.failure(class, &format!("{error:#}"));
            return Err(error);
        }
    };
    let list_result = deadline
        .run(
            "index LIST",
            crate::remote_plan::RemotePlanner::new(&daemon.config)
                .plan(crate::remote_plan::RemoteWorkload::KeyDiscovery)
                .layout(backend.as_ref(), remote)
                .list_keys(),
        )
        .await;
    drop(semaphore);
    let keys = match list_result {
        Ok(keys) => keys,
        Err(e) => {
            // Failures still cost wall time; count both (#583 P0.5).
            daemon
                .prefetch_stats
                .list_failures_total
                .fetch_add(1, Ordering::Relaxed);
            daemon
                .prefetch_stats
                .list_duration_ms_total
                .fetch_add(list_start.elapsed().as_millis() as u64, Ordering::Relaxed);
            let class = classify_remote_error(&e);
            breaker_permit.failure(
                class,
                &format!("key cache refresh failed ({class:?}): {e:#}"),
            );
            return Err(e);
        }
    };
    // Phase-0 telemetry (#485/#583): the LIST cost the coordination service
    // exists to retire. Last-refresh gauges plus cumulative totals — the
    // totals (and their per-session deltas in the build summary) are what
    // the P3-vs-P4a decision gate reads.
    let list_elapsed_ms = list_start.elapsed().as_millis() as u64;
    daemon
        .prefetch_stats
        .last_list_duration_ms
        .store(list_elapsed_ms, Ordering::Relaxed);
    daemon
        .prefetch_stats
        .last_list_key_count
        .store(keys.len() as u64, Ordering::Relaxed);
    daemon
        .prefetch_stats
        .list_duration_ms_total
        .fetch_add(list_elapsed_ms, Ordering::Relaxed);
    daemon
        .prefetch_stats
        .list_keys_total
        .fetch_add(keys.len() as u64, Ordering::Relaxed);
    breaker_permit.success();
    let count = keys.len();
    // Coherence (#564): a fresh listing proves some remembered misses stale
    // — another machine uploaded them. Drop those before the swap so the
    // negative cache can never contradict newer LIST data.
    daemon.negative_keys.remove_present_in(&keys, listing_epoch);
    if !daemon
        .key_cache
        .populate_if_unchanged(keys, key_cache_revision)
        .await
    {
        tracing::debug!("discarding stale key-cache LIST snapshot after a concurrent point update");
    }
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

    let initialization_deadline =
        RemoteDeadline::from_secs(daemon.config.remote_restore_timeout_secs);
    let backend = match initialization_deadline
        .run(
            "startup prefetch backend initialization",
            daemon.get_remote_backend(),
        )
        .await
    {
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
        let d = Arc::clone(daemon);
        let p = prefix.to_string();
        let ns = namespace.to_string();
        let h = hash.clone();
        handles.push(tokio::spawn(async move {
            let Some(breaker) = d.remote_breaker.try_acquire(RemoteOperation::ShardGet) else {
                return Ok(None);
            };
            let deadline = RemoteDeadline::from_secs(d.config.remote_restore_timeout_secs);
            let semaphore = match deadline
                .run("shard GET queue", async {
                    d.s3_semaphore
                        .acquire()
                        .await
                        .map_err(|_| anyhow::anyhow!("remote semaphore closed"))
                })
                .await
            {
                Ok(permit) => permit,
                Err(error) => {
                    let class = classify_remote_error(&error);
                    breaker.failure(class, &format!("{error:#}"));
                    return Err(error);
                }
            };
            let result = deadline
                .run(
                    "shard GET",
                    crate::remote::download_shard(b.as_ref(), &p, &ns, &h),
                )
                .await;
            drop(semaphore);
            match &result {
                Ok(_) => breaker.success(),
                Err(error) => {
                    let class = classify_remote_error(error);
                    breaker.failure(class, &format!("{error:#}"));
                }
            }
            result
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
        warm_all: false,
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

    let Some(breaker) = daemon
        .remote_breaker
        .try_acquire(RemoteOperation::ManifestGet)
    else {
        tracing::debug!("manifest prefetch suppressed by read breaker");
        return;
    };
    let deadline = RemoteDeadline::from_secs(daemon.config.remote_restore_timeout_secs);
    let semaphore = match deadline
        .run("manifest GET queue", async {
            daemon
                .s3_semaphore
                .acquire()
                .await
                .map_err(|_| anyhow::anyhow!("remote semaphore closed"))
        })
        .await
    {
        Ok(permit) => permit,
        Err(error) => {
            let class = classify_remote_error(&error);
            breaker.failure(class, &format!("{error:#}"));
            tracing::warn!("manifest prefetch queue failed: {error:#}");
            return;
        }
    };
    let manifest_result = deadline
        .run(
            "manifest GET",
            crate::remote::download_manifest(backend, &remote.prefix, &manifest_key),
        )
        .await;
    drop(semaphore);
    let manifest = match manifest_result {
        Ok(manifest) => {
            breaker.success();
            manifest
        }
        Err(e) => {
            let class = classify_remote_error(&e);
            breaker.failure(class, &format!("{e:#}"));
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
        warm_all: false,
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

async fn handle_connection_after_queue(
    stream: TokioStream,
    daemon: &Arc<Daemon>,
    shutdown_flag: &AtomicBool,
    shutdown_notify: &Notify,
    limiter: Arc<tokio::sync::Semaphore>,
    request_started_at: Instant,
) -> Result<()> {
    let _permit = limiter.acquire_owned().await.ok();
    handle_connection_started_at(
        stream,
        daemon,
        shutdown_flag,
        shutdown_notify,
        request_started_at,
    )
    .await
}

#[cfg(test)]
async fn handle_connection(
    stream: TokioStream,
    daemon: &Arc<Daemon>,
    shutdown_flag: &AtomicBool,
    shutdown_notify: &Notify,
) -> Result<()> {
    handle_connection_started_at(
        stream,
        daemon,
        shutdown_flag,
        shutdown_notify,
        Instant::now(),
    )
    .await
}

async fn handle_connection_started_at(
    stream: TokioStream,
    daemon: &Arc<Daemon>,
    shutdown_flag: &AtomicBool,
    shutdown_notify: &Notify,
    request_started_at: Instant,
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
            Ok(Request::LocalLookup(req)) => req.client_epoch,
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
            Ok(Request::Gc(req) | Request::GcV2(req)) => {
                // Offload: a GC sweep is seconds of `std::fs` work holding the
                // store mutex — never run it on an async worker (#281).
                let d = Arc::clone(daemon);
                offload(move || d.handle_gc(&req)).await
            }
            Ok(Request::RemoteCheck(req)) => {
                daemon
                    .handle_remote_check_started_at(&req, request_started_at)
                    .await
            }
            Ok(Request::LocalLookup(req)) => daemon.handle_local_lookup(&req).await,
            Ok(Request::Stats(req)) => {
                let d = Arc::clone(daemon);
                offload(move || d.handle_stats(&req)).await
            }
            Ok(Request::BatchRemoteCheck(req)) => {
                daemon
                    .handle_batch_remote_check_started_at(&req, request_started_at)
                    .await
            }
            Ok(Request::HashFiles(req)) => {
                // Offload: full-file blake3 hashing is blocking I/O (#281).
                let d = Arc::clone(daemon);
                offload(move || d.handle_hash_files(&req)).await
            }
            Ok(Request::Prefetch(req)) => daemon.handle_prefetch(&req).await,
            Ok(Request::BuildStarted(req)) => daemon.handle_build_started(&req).await,
            Ok(Request::CompileStarted(req)) => daemon.handle_compile_started(req),
            Ok(Request::CompileFinished(req)) => daemon.handle_compile_finished(&req),
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
    if config.remote_readonly {
        return Ok(());
    }
    let socket_path = config.socket_path();

    let job = UploadJob {
        key: key.to_string(),
        entry_dir: entry_dir.to_string_lossy().into_owned(),
        crate_name: crate_name.to_string(),
        client_epoch: build_epoch(),
    };
    // Durability precedes the fire-and-forget socket write. If the daemon is
    // absent or restarts after accepting bytes, startup replay still sees the
    // intent and no successful local compile silently loses its upload.
    let durable_job = persist_upload_job(config, &job)?;
    let req = Request::Upload(durable_job);

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
                        "could not reach or start daemon; upload remains queued durably"
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
    pub breakdown: Option<GcBreakdown>,
}

const GC_POLICY_PROTOCOL_VERSION: u32 = 2;

fn require_gc_policy_support(stats: &StatsResponse) -> Result<()> {
    if stats.gc_policy_version < GC_POLICY_PROTOCOL_VERSION {
        anyhow::bail!(
            "connected daemon predates GC policy version {GC_POLICY_PROTOCOL_VERSION}; refusing \
             to send a mutating GC request"
        );
    }
    Ok(())
}

fn require_daemon_started(started: bool) -> Result<()> {
    anyhow::ensure!(started, "could not reach or start daemon");
    Ok(())
}

fn gc_outcome_from_response(resp: Response) -> Result<GcRequestOutcome> {
    if !resp.ok {
        anyhow::bail!("daemon GC error: {}", resp.error.unwrap_or_default());
    }
    if resp.gc.is_none() {
        anyhow::bail!(
            "connected daemon omitted GC policy reporting; refusing to accept ambiguous semantics"
        );
    }
    Ok(GcRequestOutcome {
        evicted: resp.evicted,
        skipped: resp.skipped,
        breakdown: resp.gc,
    })
}

/// Send a GC request to the daemon. Auto-starts daemon if needed.
pub fn send_gc_request(config: &Config, max_age_hours: Option<u64>) -> Result<GcRequestOutcome> {
    let socket_path = config.socket_path();

    // Capability-check before mutation. New clients send v2 fields that an
    // old daemon silently ignores, so discovering incompatibility from the GC
    // response would be too late: the old daemon may already have evicted in
    // duplicate/size-before-age order (or run duplicate GC for --max-age).
    match send_stats_request(config, false, None, None) {
        Ok(stats) => require_gc_policy_support(&stats)?,
        Err(_) => {
            require_daemon_started(start_daemon_background()?)?;
            let stats = send_stats_request(config, false, None, None)
                .context("probing GC policy support after daemon start")?;
            require_gc_policy_support(&stats)?;
        }
    }

    // gc_v2 is itself the atomic compatibility gate: an old daemon cannot
    // deserialize it, even if it replaced the probed daemon between sockets.
    let req = Request::GcV2(match max_age_hours {
        Some(hours) => GcRequest::explicit_age(hours),
        None => GcRequest::automatic(config.gc_max_age_hours),
    });

    let try_send = |path: &Path| -> Result<Response> {
        let resp_str = send_request(path, &req)?;
        let resp: Response = serde_json::from_str(&resp_str)?;
        Ok(resp)
    };

    match try_send(&socket_path) {
        Ok(resp) => gc_outcome_from_response(resp),
        Err(_) => {
            // The daemon may have exited after the capability probe. Any
            // replacement must pass the same pre-mutation check before retry.
            require_daemon_started(start_daemon_background()?)?;
            let stats = send_stats_request(config, false, None, None)
                .context("probing GC policy support before retry")?;
            require_gc_policy_support(&stats)?;
            let resp = try_send(&socket_path)?;
            gc_outcome_from_response(resp)
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

    let client_budget_ms = remote_check_budget_ms(config.remote_restore_timeout_secs, None);
    let req = Request::RemoteCheck(RemoteCheckRequest {
        key: key.to_string(),
        entry_dir: entry_dir.to_string_lossy().into_owned(),
        crate_name: crate_name.to_string(),
        deadline_ms: Some(client_budget_ms),
    });

    // This wait is on rustc's synchronous miss path. Preserve the historical
    // hard three-second ceiling even when talking to an older daemon that
    // ignores `deadline_ms`; configuration may only shorten the wait.
    let client_timeout = Duration::from_millis(client_budget_ms);
    match send_request_with_timeout(&socket_path, &req, client_timeout) {
        Ok(resp_str) => remote_check_result_from_response_line(&resp_str),
        Err(e) => {
            tracing::debug!("remote check: daemon unreachable ({e})");
            None
        }
    }
}

/// Ask the daemon for a local-store hit (kunobi-ninja/kache#565). `None`
/// means "no usable answer" (daemon absent, slow, or too old to know the
/// request) — the caller must run the fully local path. The read timeout is
/// deliberately tight: this sits on the warm-hit critical path, and an
/// overloaded daemon must shed to the local path, never queue the build.
pub fn send_local_lookup(config: &Config, key: &str) -> Option<LocalLookupReply> {
    let socket_path = config.socket_path();
    if !crate::transport::is_reachable(&socket_path) {
        return None;
    }

    let req = Request::LocalLookup(LocalLookupRequest {
        key: key.to_string(),
        client_epoch: build_epoch(),
    });
    let timeout = std::env::var("KACHE_LOCAL_HIT_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .map(std::time::Duration::from_millis)
        .unwrap_or(std::time::Duration::from_millis(250));

    match send_request_with_timeout(&socket_path, &req, timeout) {
        Ok(resp_str) => match serde_json::from_str::<Response>(&resp_str) {
            Ok(resp) if resp.ok => resp.local_lookup,
            // An older daemon answers `ok: false, error: invalid request` for
            // an unknown variant — that's a fallback, not an error.
            _ => None,
        },
        Err(e) => {
            tracing::debug!("local lookup: daemon unreachable ({e})");
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
        warm_all: false,
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

/// Max age before an in-flight compile entry is dropped even if a process
/// with that PID is still alive — PID reuse must not resurrect a ghost.
const IN_FLIGHT_MAX_AGE_MS: u64 = 6 * 60 * 60 * 1000;

/// Ms since the Unix epoch (0 on a pre-epoch clock; entries then age out).
fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Drop registry entries whose process is gone or whose age is absurd. Called
/// on both the register and snapshot paths (kunobi-ninja/kache#131) — a
/// wrapper killed by OOM or ^C never sends CompileFinished.
fn prune_in_flight(map: &mut HashMap<u32, CompileStartedRequest>) {
    let now = unix_ms();
    map.retain(|&pid, c| {
        now.saturating_sub(c.started_at_ms) <= IN_FLIGHT_MAX_AGE_MS && pid_alive(pid)
    });
}

/// Is a process with this PID alive? `kill(pid, 0)` probes without signaling:
/// success or EPERM (alive, not ours) both mean alive; ESRCH means gone.
#[cfg(unix)]
fn pid_alive(pid: u32) -> bool {
    let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
    rc == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// No cheap portable probe off unix — age-based pruning still applies.
#[cfg(not(unix))]
fn pid_alive(_pid: u32) -> bool {
    true
}

/// Register an in-flight compile (kunobi-ninja/kache#131). Fire-and-forget
/// from the wrapper's heartbeat monitor thread. Never auto-starts the daemon —
/// observability is not worth a daemon spawn — and never fails the build.
/// Takes the socket path rather than `&Config` so the monitor thread's context
/// stays a couple of PathBufs.
pub fn send_compile_started(socket_path: &std::path::Path, req: CompileStartedRequest) {
    // Probe before connecting (same pattern as send_remote_check): with no
    // daemon this returns immediately, and a wedged socket can't stall the
    // monitor thread — a lost registration only costs panel visibility, and
    // a lost Finished self-heals via liveness pruning.
    if !crate::transport::is_reachable(socket_path) {
        return;
    }
    let req = Request::CompileStarted(req);
    if let Err(e) = send_request_fire_and_forget(socket_path, &req) {
        tracing::debug!("compile-started: daemon unreachable ({e}), skipping");
    }
}

/// Deregister a finished compile — fire-and-forget counterpart of
/// [`send_compile_started`].
pub fn send_compile_finished(socket_path: &std::path::Path, pid: u32, started_at_ms: u64) {
    if !crate::transport::is_reachable(socket_path) {
        return;
    }
    let req = Request::CompileFinished(CompileFinishedRequest { pid, started_at_ms });
    if let Err(e) = send_request_fire_and_forget(socket_path, &req) {
        tracing::debug!("compile-finished: daemon unreachable ({e}), skipping");
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
    send_stats_request_options(config, include_entries, false, sort_by, event_hours)
}

pub(crate) fn send_stats_request_options(
    config: &Config,
    include_entries: bool,
    include_summaries: bool,
    sort_by: Option<&str>,
    event_hours: Option<u64>,
) -> Result<StatsResponse> {
    let socket_path = config.socket_path();
    let client_epoch = build_epoch();

    let req = Request::Stats(StatsRequest {
        include_entries,
        include_summaries,
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
                    include_summaries: false,
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

        // Spawn with OUR std handles made non-inheritable for the duration
        // (kunobi-ninja/kache#704). Redirecting the daemon's own stdio is not
        // enough on Windows: `CreateProcess` with `bInheritHandles = TRUE` —
        // which Rust's `Command` uses whenever it sets stdio — gives the child
        // EVERY inheritable handle in this process, not only the redirected
        // ones. So a daemon started from a `kache` invocation whose output is
        // being captured would hold a duplicate of the caller's pipe write
        // end, and the caller would wait for an EOF that cannot arrive until
        // the daemon exits — which, with the idle timeout disabled by default
        // (#662), is never. Any tool that captures kache's output hangs:
        // build scripts, CI wrappers, IDE integrations, and the test harness
        // where this was found.
        let mut child = spawn_detached_daemon(&exe, stderr_target)?;

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

/// Spawn `kache daemon run` detached, without leaking this process's
/// inheritable handles to it (kunobi-ninja/kache#704).
fn spawn_detached_daemon(
    exe: &Path,
    stderr_target: std::process::Stdio,
) -> Result<std::process::Child> {
    let mut command = std::process::Command::new(exe);
    command
        .args(["daemon", "run"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(stderr_target);

    // On Windows, clear the inherit flag on our own std handles across the
    // spawn and restore it after. The daemon's stdio is passed explicitly
    // above and is marked inheritable by the standard library itself, so it
    // is unaffected; what this removes is the *incidental* inheritance of the
    // caller's pipes. Restoring matters because later children (rustc)
    // legitimately inherit these handles.
    #[cfg(windows)]
    let spawned = {
        let _guard = NonInheritableStdio::acquire();
        command.spawn()
    };
    #[cfg(not(windows))]
    let spawned = command.spawn();

    spawned.context("spawning daemon process")
}

/// Clears `HANDLE_FLAG_INHERIT` on this process's standard handles and
/// restores it on drop (kunobi-ninja/kache#704). Windows-only.
#[cfg(windows)]
struct NonInheritableStdio {
    restore: Vec<windows_sys::Win32::Foundation::HANDLE>,
}

#[cfg(windows)]
impl NonInheritableStdio {
    fn acquire() -> Self {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Foundation::{HANDLE, HANDLE_FLAG_INHERIT, SetHandleInformation};

        // `RawHandle` and `windows_sys`' `HANDLE` are both `*mut c_void`, so
        // these coerce without a cast — and Windows CI runs clippy with
        // `-D warnings`, where a redundant one is an error.
        let handles: [HANDLE; 3] = [
            std::io::stdin().as_raw_handle(),
            std::io::stdout().as_raw_handle(),
            std::io::stderr().as_raw_handle(),
        ];
        let mut restore = Vec::new();
        for handle in handles {
            // A std handle can be absent in a detached process, reported
            // either as null or as INVALID_HANDLE_VALUE.
            if handle.is_null() || handle == windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE {
                continue;
            }
            // Best-effort: a std handle can legitimately be absent (a detached
            // process) or non-inheritable already. A failure here only means
            // the leak this guards against may still be possible, never that
            // the daemon fails to start.
            let cleared = unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0) };
            if cleared != 0 {
                restore.push(handle);
            }
        }
        Self { restore }
    }
}

#[cfg(windows)]
impl Drop for NonInheritableStdio {
    fn drop(&mut self) {
        use windows_sys::Win32::Foundation::{HANDLE_FLAG_INHERIT, SetHandleInformation};
        for handle in &self.restore {
            unsafe {
                SetHandleInformation(*handle, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT);
            }
        }
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
    use std::fs;
    use std::sync::mpsc;

    #[test]
    fn remote_check_demand_budget_keeps_legacy_cap_and_only_allows_tightening() {
        // Mixed-version/config table: a legacy client omits the wire field, a
        // zero-valued early client must not disable the safety bound, and a new
        // client/daemon may independently tighten but never lengthen it.
        for (case, configured_secs, wire_ms, expected_ms) in [
            ("legacy client / default daemon", 300, None, 3_000),
            ("legacy client / disabled daemon deadline", 0, None, 3_000),
            ("overflowing daemon config", u64::MAX, None, 3_000),
            ("daemon tightens", 2, None, 2_000),
            ("daemon tighter than client", 1, Some(3_000), 1_000),
            ("client tightens", 300, Some(1_500), 1_500),
            ("zero wire value", 300, Some(0), 3_000),
            ("oversized wire value", 300, Some(u64::MAX), 3_000),
        ] {
            assert_eq!(
                remote_check_budget_ms(configured_secs, wire_ms),
                expected_ms,
                "{case}"
            );
        }

        let legacy_json = format!(
            r#"{{"remote_check":{{"key":"{}","entry_dir":"/tmp/entry","crate_name":"serde"}}}}"#,
            "a".repeat(64)
        );
        let Request::RemoteCheck(legacy_request) =
            serde_json::from_str::<Request>(&legacy_json).unwrap()
        else {
            panic!("expected remote-check request");
        };
        assert_eq!(legacy_request.deadline_ms, None);
        assert_eq!(
            remote_check_budget_ms(300, legacy_request.deadline_ms),
            3_000
        );

        let accepted_at = Instant::now();
        let legacy_client_deadline =
            RemoteDeadline::from_millis_at(accepted_at, remote_check_budget_ms(300, None));
        assert_eq!(
            legacy_client_deadline.at(),
            Some(accepted_at + Duration::from_secs(3))
        );
    }

    /// #581: the old counters incremented checks and hits in the same branch,
    /// so the ratio was 100% by construction and cancellation never fired.
    /// The rework counts EVERY distinct demanded key; these pin the decision
    /// function's semantics.
    #[test]
    fn should_cancel_prefetch_fires_on_low_candidate_share() {
        // 12 distinct demands, only 1 was a plan candidate, nothing else
        // downloaded — a plainly bad plan.
        assert!(should_cancel_prefetch(12, 1, 0));
    }

    #[test]
    fn should_cancel_prefetch_holds_below_min_demands() {
        // Never cancel on thin evidence, however bad the ratio looks.
        assert!(!should_cancel_prefetch(9, 0, 0));
    }

    #[test]
    fn should_cancel_prefetch_holds_when_plan_is_good() {
        assert!(!should_cancel_prefetch(20, 15, 0));
    }

    #[test]
    fn should_cancel_prefetch_counts_undmanded_downloads_as_potential_hits() {
        // The local-consumption blind spot: completed prefetches consumed via
        // the wrapper's local store never reach the daemon as demands. They
        // count toward the upper bound, so a plan whose downloads are being
        // silently consumed is NOT cancelled.
        assert!(should_cancel_prefetch(20, 2, 0));
        assert!(!should_cancel_prefetch(20, 2, 8));
    }

    /// Per-plan lifecycle: demand/download bookkeeping and the single-fire
    /// cancel latch.
    #[test]
    fn active_plan_tracks_demand_download_and_use() {
        let mut plan = ActivePlan::new(
            "sess-1".into(),
            "plan-1".into(),
            "fallback",
            ["a", "b"].into_iter().map(String::from).collect(),
            0,
            0,
        );
        // Candidate demanded before download: counted, not yet used.
        assert!(!plan.record_demand("a"));
        assert_eq!(plan.demanded.len(), 1);
        assert_eq!(plan.demanded_candidates.len(), 1);
        assert!(plan.used.is_empty());
        // Download lands after demand → used.
        plan.record_download("a", 100);
        assert!(plan.used.contains("a"));
        // Download-then-demand also counts as used.
        plan.record_download("b", 50);
        assert!(!plan.record_demand("b"));
        assert!(plan.used.contains("b"));
        assert_eq!(plan.used_bytes(), 150);
        // Duplicate demand of the same key doesn't inflate the sets.
        assert!(!plan.record_demand("a"));
        assert_eq!(plan.demanded.len(), 2);
    }

    #[test]
    fn active_plan_cancel_latch_fires_once() {
        let mut plan = ActivePlan::new(
            "sess-2".into(),
            String::new(),
            "advisory",
            ["only-candidate".to_string()].into_iter().collect(),
            0,
            0,
        );
        // Demand 9 non-candidate keys: below the floor, no fire.
        for i in 0..9 {
            assert!(!plan.record_demand(&format!("k{i}")));
        }
        // The 10th distinct non-candidate demand crosses the floor with a
        // 0/10 candidate share → fires exactly once...
        assert!(plan.record_demand("k9"));
        assert!(plan.cancelled);
        // ...and never again for the same plan.
        assert!(!plan.record_demand("k10"));
    }

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
    /// under test. `sleep` on Unix; PowerShell's `Start-Sleep` on Windows,
    /// because `timeout` needs a console and ping request counts do not
    /// guarantee any minimum duration.
    fn spawn_blocking_child() -> std::process::Child {
        #[cfg(unix)]
        let mut child = std::process::Command::new("sh")
            .args(["-c", "sleep 30"])
            .spawn()
            .unwrap();
        #[cfg(windows)]
        let mut child = std::process::Command::new("powershell.exe")
            .args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "Start-Sleep -Seconds 30",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();

        assert!(
            child.try_wait().unwrap().is_none(),
            "blocking test child exited during setup"
        );
        child
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
    /// #131: the in-flight registry upserts by pid, deregisters on finish,
    /// prunes dead/ancient entries, and snapshots with derived elapsed/ETA.
    #[test]
    fn in_flight_registry_upserts_prunes_and_snapshots() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Daemon::new(test_config(dir.path()));
        let now = unix_ms();
        // Use our own (certainly alive) pid so liveness pruning keeps it.
        let pid = std::process::id();

        daemon.handle_compile_started(CompileStartedRequest {
            crate_name: "gkrust".into(),
            root: "/w".into(),
            pid,
            started_at_ms: now.saturating_sub(10_000),
            typical_ms: None,
            client_epoch: 0,
        });
        // Upsert: the first-tick refresh with typical_ms replaces, not duplicates.
        daemon.handle_compile_started(CompileStartedRequest {
            crate_name: "gkrust".into(),
            root: "/w".into(),
            pid,
            started_at_ms: now.saturating_sub(10_000),
            typical_ms: Some(471_000),
            client_epoch: 0,
        });
        // An entry older than the max age is pruned even with a live pid
        // (PID reuse must not resurrect ghosts).
        daemon.handle_compile_started(CompileStartedRequest {
            crate_name: "ghost".into(),
            root: "/w".into(),
            pid: pid.wrapping_add(1),
            started_at_ms: now.saturating_sub(IN_FLIGHT_MAX_AGE_MS + 60_000),
            typical_ms: None,
            client_epoch: 0,
        });

        let snapshot = daemon.in_flight_snapshot();
        assert_eq!(
            snapshot.len(),
            1,
            "ghost pruned, upsert deduped: {snapshot:?}"
        );
        let entry = &snapshot[0];
        assert_eq!(entry.crate_name, "gkrust");
        assert_eq!(entry.pid, pid);
        assert!(entry.elapsed_s >= 10);
        assert_eq!(entry.typical_s, Some(471));
        assert_eq!(entry.eta_s, Some(471u64.saturating_sub(entry.elapsed_s)));

        // A stale Finished with a mismatched start token must NOT remove it.
        daemon.handle_compile_finished(&CompileFinishedRequest {
            pid,
            started_at_ms: 12345,
        });
        assert_eq!(daemon.in_flight_snapshot().len(), 1);
        daemon.handle_compile_finished(&CompileFinishedRequest {
            pid,
            started_at_ms: now.saturating_sub(10_000),
        });
        assert!(daemon.in_flight_snapshot().is_empty());
    }

    /// #131 wire shape: the new variants serialize under snake_case tags an
    /// old daemon will reject as a parse error (fire-and-forget client
    /// ignores), and StatsResponse's `in_flight` defaults for old daemons.
    #[test]
    fn compile_started_wire_tags_and_stats_default() {
        let req = Request::CompileStarted(CompileStartedRequest {
            crate_name: "c".into(),
            root: String::new(),
            pid: 1,
            started_at_ms: 2,
            typical_ms: None,
            client_epoch: 0,
        });
        let wire = serde_json::to_string(&req).unwrap();
        assert!(wire.contains("\"compile_started\""), "{wire}");
        let round: Request = serde_json::from_str(&wire).unwrap();
        assert_eq!(round, req);

        // A StatsResponse serialized by an OLD daemon (no in_flight field)
        // must deserialize with an empty registry view.
        let mut old = serde_json::to_value(StatsResponse {
            total_size: 0,
            max_size: 0,
            entry_count: 0,
            entries: None,
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
            blob_stats: None,
            recent_summaries: Vec::new(),
            version: String::new(),
            build_epoch: 0,
            gc_policy_version: GC_POLICY_PROTOCOL_VERSION,
            pending_uploads: 0,
            active_downloads: 0,
            s3_concurrency_total: 0,
            s3_concurrency_used: 0,
            upload_queue_capacity: 0,
            uploads_completed: 0,
            uploads_failed: 0,
            uploads_skipped: 0,
            uploads_suppressed: 0,
            downloads_completed: 0,
            downloads_failed: 0,
            downloads_suppressed: 0,
            remote_check_roundtrips: 0,
            negative_hits: 0,
            negative_entries: 0,
            remote_degraded: false,
            bytes_uploaded: 0,
            bytes_downloaded: 0,
            recent_transfers: Vec::new(),
            prefetch: PrefetchStatsSnapshot::default(),
            in_flight: vec![InFlightEntry {
                crate_name: "x".into(),
                root: String::new(),
                pid: 1,
                elapsed_s: 1,
                typical_s: None,
                eta_s: None,
            }],
            effective_config: Some(EffectiveConfig {
                max_size: 1,
                cache_dir: "/c".into(),
                config_path: "/c/config.toml".into(),
                config_fingerprint: Some("fingerprint".into()),
                prefetch_enabled: true,
                remote_description: None,
                local_only: false,
                remote_error: None,
                remote_key_cache_refresh_secs: 60,
                socket_path: "/c/daemon.sock".into(),
                started_at_ms: 1,
            }),
        })
        .unwrap();
        {
            let old_obj = old.as_object_mut().unwrap();
            old_obj.remove("in_flight");
            old_obj.remove("blob_stats");
            old_obj.remove("recent_summaries");
            old_obj.remove("gc_policy_version");
        }
        let mut old_effective = old.get("effective_config").unwrap().clone();
        old_effective
            .as_object_mut()
            .unwrap()
            .remove("remote_key_cache_refresh_secs");
        let parsed_effective: EffectiveConfig = serde_json::from_value(old_effective).unwrap();
        assert_eq!(
            parsed_effective.remote_key_cache_refresh_secs,
            crate::config::DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS,
            "an older daemon report must deserialize with the historical cadence"
        );
        // A pre-#689 daemon reports no effective config either; the CLI must
        // see `None` (and fall back to labeled client-config values), not a
        // parse error or a zeroed report.
        old.as_object_mut().unwrap().remove("effective_config");
        let parsed: StatsResponse = serde_json::from_value(old).unwrap();
        assert!(parsed.in_flight.is_empty());
        assert!(parsed.blob_stats.is_none());
        assert!(parsed.recent_summaries.is_empty());
        assert!(parsed.effective_config.is_none());
        assert_eq!(parsed.gc_policy_version, 0);
    }

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
            include_summaries: false,
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
            local_hit_daemon: false,
            windows_hardlink: false,
            auto_gc: true,
            storage_layout_advice: true,
            heartbeat_secs: 30,
            explain_miss: false,
            path_only_env_vars: Vec::new(),
            incremental_crates: Vec::new(),
            key_env_vars: Vec::new(),
            base_dirs: Vec::new(),
            cache_dir: dir.to_path_buf(),
            socket_path_override: None,
            max_size: 50 * 1024 * 1024, // 50 MiB
            remote: None,
            remote_error: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: false,
            preserve_incremental: false,
            adaptive_incremental: true,
            event_log_max_size: 10 * 1024 * 1024,
            event_log_keep_lines: 1000,
            compression_level: 3,
            s3_concurrency: 16,
            prefetch_enabled: crate::config::DEFAULT_PREFETCH_ENABLED,
            remote_key_cache_refresh_secs: crate::config::DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS,
            prefetch_max_keys: crate::config::DEFAULT_PREFETCH_MAX_KEYS,
            prefetch_max_bytes: crate::config::DEFAULT_PREFETCH_MAX_BYTES,
            prefetch_deadline_secs: crate::config::DEFAULT_PREFETCH_DEADLINE_SECS,
            min_store_compile_ms: crate::config::DEFAULT_MIN_STORE_COMPILE_MS,
            gc_max_age_hours: crate::config::DEFAULT_GC_MAX_AGE_HOURS,
            daemon_idle_timeout_secs: crate::config::DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: crate::config::DEFAULT_S3_POOL_IDLE_SECS,
            remote_restore_timeout_secs: crate::config::DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS,
            remote_negative_ttl_secs: crate::config::DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
        }
    }

    fn test_cache_key(label: &str) -> String {
        blake3::hash(label.as_bytes()).to_hex().to_string()
    }

    #[test]
    fn key_cache_authoritative_truth_table() {
        assert!(key_cache_miss_is_authoritative(1, Some(Duration::ZERO)));
        assert!(key_cache_miss_is_authoritative(
            1,
            Some(Duration::from_secs(5))
        ));
        assert!(!key_cache_miss_is_authoritative(
            1,
            Some(Duration::from_secs(6))
        ));
        assert!(key_cache_miss_is_authoritative(
            60,
            Some(Duration::from_secs(300))
        ));
        assert!(!key_cache_miss_is_authoritative(
            60,
            Some(Duration::from_secs(301))
        ));
        assert!(key_cache_miss_is_authoritative(
            900,
            Some(Duration::from_secs(300))
        ));
        assert!(!key_cache_miss_is_authoritative(
            900,
            Some(Duration::from_secs(301))
        ));
        assert!(!key_cache_miss_is_authoritative(0, Some(Duration::ZERO)));
        assert!(!key_cache_miss_is_authoritative(60, None));
    }

    #[test]
    fn speculative_prefetch_decision_truth_table() {
        assert!(speculative_prefetch_disabled(false));
        assert!(!speculative_prefetch_disabled(true));

        assert!(should_start_speculative_prefetch(true, true));
        assert!(!should_start_speculative_prefetch(false, true));
        assert!(!should_start_speculative_prefetch(true, false));
        assert!(!should_start_speculative_prefetch(false, false));
    }

    #[test]
    fn key_cache_periodic_refresh_disabled_truth_table() {
        assert!(key_cache_periodic_refresh_disabled(0));
        assert!(!key_cache_periodic_refresh_disabled(1));
        assert!(!key_cache_periodic_refresh_disabled(60));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_main_binds_socket_and_handles_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.daemon_idle_timeout_secs = 0;
        let socket_path = config.socket_path();
        let coord = DaemonCoordFile::for_socket(&socket_path);
        let server_config = config.clone();
        let provenance = crate::config::config_file_provenance_at(dir.path().join("config.toml"));
        let server =
            tokio::spawn(async move { server_main(&server_config, &provenance, coord).await });

        let ready_socket = socket_path.clone();
        let ready = tokio::task::spawn_blocking(move || {
            wait_for_socket_until(&ready_socket, None, Duration::from_secs(5))
        })
        .await
        .unwrap()
        .unwrap();
        assert!(ready, "server_main must bind its configured socket");

        let shutdown_config = config.clone();
        tokio::task::spawn_blocking(move || send_shutdown_request(&shutdown_config))
            .await
            .unwrap()
            .unwrap();

        let result = tokio::time::timeout(Duration::from_secs(10), server)
            .await
            .expect("server_main should stop after a shutdown request")
            .expect("server_main task should not panic");
        assert!(
            result.is_ok(),
            "server_main should exit cleanly: {result:?}"
        );
        assert!(
            !socket_path.exists(),
            "server_main should remove its socket during shutdown"
        );
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
        let req = Request::Gc(GcRequest::explicit_age(168));
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, parsed);

        assert!(json.contains("\"gc\""));
        assert!(json.contains("\"max_age_hours\":168"));
    }

    #[test]
    fn test_request_gc_null_age_serde() {
        let req = Request::Gc(GcRequest::legacy(None));
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, parsed);
        assert!(json.contains("\"max_age_hours\":null"));

        let old_wire: Request = serde_json::from_str(r#"{"gc":{"max_age_hours":null}}"#).unwrap();
        assert_eq!(old_wire, Request::Gc(GcRequest::legacy(None)));
    }

    #[test]
    fn test_request_gc_automatic_carries_effective_age() {
        let req = Request::Gc(GcRequest::automatic(72));
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"mode\":\"automatic\""));
        assert!(json.contains("\"effective_max_age_hours\":72"));
        assert_eq!(serde_json::from_str::<Request>(&json).unwrap(), req);
    }

    #[test]
    fn gc_v2_is_atomic_compatibility_gate_for_old_daemons() {
        #[allow(dead_code)]
        #[derive(Deserialize)]
        #[serde(rename_all = "snake_case")]
        enum LegacyRequest {
            Gc(GcRequest),
            Stats(StatsRequest),
        }

        let req = Request::GcV2(GcRequest::automatic(72));
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"gc_v2\""));
        assert_eq!(serde_json::from_str::<Request>(&json).unwrap(), req);
        assert!(
            serde_json::from_str::<LegacyRequest>(&json).is_err(),
            "a pre-v2 daemon must reject the request before mutation"
        );
    }

    #[test]
    fn gc_rejects_any_response_without_policy_reporting() {
        let error = match gc_outcome_from_response(Response::ok_evicted(1)) {
            Ok(_) => panic!("legacy aggregate response must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("omitted GC policy reporting"));
    }

    #[test]
    fn daemon_start_requirement_distinguishes_success_and_failure() {
        assert!(require_daemon_started(true).is_ok());
        let error = match require_daemon_started(false) {
            Ok(()) => panic!("a failed daemon start must stop the request"),
            Err(error) => error,
        };
        assert_eq!(error.to_string(), "could not reach or start daemon");
    }

    #[test]
    fn test_request_remote_check_serde() {
        let req = Request::RemoteCheck(RemoteCheckRequest {
            key: "abc123".into(),
            entry_dir: "/tmp/store/abc123".into(),
            crate_name: String::new(),
            deadline_ms: None,
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
        let resp =
            Response::ok_gc_skipped(GcRunReport::skipped(GcRequestMode::Automatic).breakdown());
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: Response = serde_json::from_str(&json).unwrap();
        assert!(parsed.skipped);
        assert_eq!(parsed.evicted, Some(0));
        assert_eq!(parsed.gc.unwrap().mode, GcRequestMode::Automatic);
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
            keys_over_budget: 5,
            cancelled: true,
            plans_advisory: 1,
            plans_fallback: 4,
            last_plan_candidates: 17,
            dedup_join_waits: 2,
            dedup_join_wait_ms: 250,
            last_list_duration_ms: 42,
            last_list_key_count: 9001,
            list_requests_total: 7,
            list_failures_total: 1,
            list_duration_ms_total: 900,
            list_keys_total: 63007,
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

    #[tokio::test]
    async fn stale_list_snapshot_cannot_erase_newer_point_knowledge() {
        let cache = S3KeyCache::new();
        cache.populate(HashMap::new()).await;
        let before_list = cache.refresh_revision();
        let uploaded = test_cache_key("upload-during-list");
        cache.insert(uploaded.clone(), Some("serde")).await;

        assert!(
            !cache
                .populate_if_unchanged(HashMap::new(), before_list)
                .await,
            "a LIST started before the upload must be discarded"
        );
        assert_eq!(cache.check(&uploaded).await, Some(true));
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

        let resp = daemon.handle_gc(&GcRequest::automatic(daemon.config.gc_max_age_hours));
        assert!(resp.ok);
        assert_eq!(resp.evicted, Some(0));
        assert_eq!(resp.gc.as_ref().unwrap().mode, GcRequestMode::Automatic);
    }

    #[test]
    fn test_handle_gc_reports_lock_skip() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();
        let _gc_lock = store.try_gc_lock().unwrap().expect("gc lock");
        let daemon = Daemon::new(config);

        let resp = daemon.handle_gc(&GcRequest::automatic(daemon.config.gc_max_age_hours));
        assert!(resp.ok);
        assert!(resp.skipped);
        assert_eq!(resp.evicted, Some(0));
    }

    #[test]
    fn test_handle_gc_with_max_age() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon = Daemon::new(config);

        let resp = daemon.handle_gc(&GcRequest::explicit_age(24));
        assert!(resp.ok);
        assert_eq!(resp.evicted, Some(0));
        let breakdown = resp.gc.unwrap();
        assert_eq!(breakdown.mode, GcRequestMode::ExplicitAge);
        assert_eq!(breakdown.duplicate.entries_evicted, 0);
        assert_eq!(breakdown.size.entries_evicted, 0);
    }

    #[test]
    fn explicit_age_request_without_hours_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Daemon::new(test_config(dir.path()));
        let resp = daemon.handle_gc(&GcRequest {
            max_age_hours: None,
            mode: GcRequestMode::ExplicitAge,
            effective_max_age_hours: None,
        });
        assert!(!resp.ok);
        assert!(
            resp.error
                .as_deref()
                .unwrap()
                .contains("missing max_age_hours")
        );
    }

    #[test]
    fn automatic_request_without_effective_age_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Daemon::new(test_config(dir.path()));
        let resp = daemon.handle_gc(&GcRequest {
            max_age_hours: None,
            mode: GcRequestMode::Automatic,
            effective_max_age_hours: None,
        });
        assert!(!resp.ok);
        assert!(
            resp.error
                .as_deref()
                .unwrap()
                .contains("missing effective_max_age_hours")
        );
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
        let stats = daemon
            .run_gc(GcPolicy::Automatic {
                max_age_hours: daemon.config.gc_max_age_hours,
            })
            .unwrap();
        assert!(
            stats.total.entries_evicted > 0,
            "should have evicted at least 1 entry"
        );
    }

    /// kunobi-ninja/kache#711: automatic GC applies configured age retention
    /// even while the store is below its size budget.
    #[test]
    fn automatic_gc_applies_configured_max_age_even_under_size_budget() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 1024 * 1024 * 1024;
        config.gc_max_age_hours = 1;

        let src_file = dir.path().join("stale.rlib");
        std::fs::write(&src_file, vec![0u8; 32]).unwrap();
        let store = Store::open(&config).unwrap();
        store
            .put(
                "stale_key",
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
        store.set_last_accessed_for_test("stale_key", "-2 hours");
        drop(store);

        let daemon = Daemon::new(config);
        let stats = daemon
            .run_gc(GcPolicy::Automatic {
                max_age_hours: daemon.config.gc_max_age_hours,
            })
            .unwrap();
        assert_eq!(stats.total.entries_evicted, 1);
        let store = Store::open(&daemon.config).unwrap();
        assert!(!store.contains("stale_key"));
    }

    #[test]
    fn automatic_gc_skips_age_eviction_when_max_age_hours_is_zero() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 1024 * 1024 * 1024;
        config.gc_max_age_hours = 0;

        let src_file = dir.path().join("stale.rlib");
        std::fs::write(&src_file, vec![0u8; 32]).unwrap();
        let store = Store::open(&config).unwrap();
        store
            .put(
                "stale_key",
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
        store.set_last_accessed_for_test("stale_key", "-2 hours");
        drop(store);

        let daemon = Daemon::new(config);
        let stats = daemon
            .run_gc(GcPolicy::Automatic {
                max_age_hours: daemon.config.gc_max_age_hours,
            })
            .unwrap();
        assert_eq!(stats.total.entries_evicted, 0);
        let store = Store::open(&daemon.config).unwrap();
        assert!(store.contains("stale_key"));
    }

    #[test]
    fn manual_automatic_gc_sends_effective_age_and_runs_age_before_size() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 1_000; // physical 1,200; size target 900
        config.gc_max_age_hours = 0; // daemon startup policy differs from request

        let old_file = dir.path().join("old.rlib");
        let fresh_file = dir.path().join("fresh.rlib");
        std::fs::write(&old_file, vec![b'o'; 400]).unwrap();
        std::fs::write(&fresh_file, vec![b'f'; 800]).unwrap();
        let store = Store::open(&config).unwrap();
        store
            .put(
                "old_valuable",
                "testcrate",
                &["lib".into()],
                &[],
                "host",
                "dev",
                &[(old_file, "old.rlib".into())],
                "",
                "",
            )
            .unwrap();
        for _ in 0..1_000 {
            assert!(store.get("old_valuable").unwrap().is_some());
        }
        store
            .put(
                "fresh_cheap",
                "testcrate",
                &["lib".into()],
                &[],
                "host",
                "dev",
                &[(fresh_file, "fresh.rlib".into())],
                "",
                "",
            )
            .unwrap();
        store.set_last_accessed_for_test("old_valuable", "-2 hours");
        store.set_last_accessed_for_test("fresh_cheap", "-2 minutes");
        drop(store);

        let daemon = Daemon::new(config);
        let resp = daemon.handle_gc(&GcRequest::automatic(1));
        assert!(resp.ok);
        assert_eq!(resp.evicted, Some(1));
        let breakdown = resp.gc.expect("new daemon returns policy breakdown");
        assert_eq!(breakdown.mode, GcRequestMode::Automatic);
        assert_eq!(breakdown.age.entries_evicted, 1);
        assert_eq!(breakdown.size.entries_evicted, 0);

        let store = Store::open(&daemon.config).unwrap();
        assert!(!store.contains("old_valuable"));
        assert!(store.contains("fresh_cheap"));
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

        let req = Request::Gc(GcRequest::automatic(daemon.config.gc_max_age_hours));
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
            deadline_ms: None,
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
            key: test_cache_key("no-remote-upload"),
            entry_dir: "/tmp".into(),
            crate_name: "serde".into(),
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
            key: test_cache_key("readonly-upload"),
            entry_dir: "/tmp".into(),
            crate_name: "serde".into(),
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

        let key = test_cache_key("no-remote-check");
        let req = RemoteCheckRequest {
            entry_dir: daemon.entry_dir_for(&key).to_string_lossy().into_owned(),
            key,
            crate_name: "serde".into(),
            deadline_ms: None,
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

        let stats = daemon
            .run_gc(GcPolicy::Automatic {
                max_age_hours: daemon.config.gc_max_age_hours,
            })
            .unwrap();
        assert_eq!(stats.total.entries_evicted, 0);
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
        let stats = daemon
            .run_gc(GcPolicy::Automatic {
                max_age_hours: daemon.config.gc_max_age_hours,
            })
            .unwrap();
        assert_eq!(stats.total.entries_evicted, 0);
        assert!(!incremental_dir.exists());

        std::fs::create_dir_all(&incremental_dir).unwrap();
        std::fs::write(incremental_dir.join("junk"), b"tmp2").unwrap();

        let stats = daemon
            .run_gc(GcPolicy::Automatic {
                max_age_hours: daemon.config.gc_max_age_hours,
            })
            .unwrap();
        assert_eq!(stats.total.entries_evicted, 0);
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
            &Request::Gc(GcRequest::automatic(daemon.config.gc_max_age_hours)),
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

        let key = test_cache_key("socket-no-remote-check");
        let entry_dir = config.store_dir().join(&key).to_string_lossy().into_owned();
        let daemon = Arc::new(Daemon::new(config));
        let resp = one_shot_request(
            &daemon,
            &socket_path,
            &Request::RemoteCheck(RemoteCheckRequest {
                key,
                entry_dir,
                crate_name: "serde".into(),
                deadline_ms: None,
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

        let req = Request::Gc(GcRequest::automatic(0));
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
            include_summaries: true,
            sort_by: Some("size".into()),
            event_hours: Some(48),
            client_epoch: 0,
        });
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, parsed);

        assert!(json.contains("\"stats\""));
        assert!(json.contains("\"include_entries\":true"));
        assert!(json.contains("\"include_summaries\":true"));
        assert!(json.contains("\"sort_by\":\"size\""));
        assert!(json.contains("\"event_hours\":48"));

        let mut old = serde_json::to_value(&req).unwrap();
        old.get_mut("stats")
            .and_then(serde_json::Value::as_object_mut)
            .unwrap()
            .remove("include_summaries");
        let parsed: Request = serde_json::from_value(old).unwrap();
        assert!(matches!(
            parsed,
            Request::Stats(StatsRequest {
                include_summaries: false,
                ..
            })
        ));
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
            blob_stats: None,
            recent_summaries: Vec::new(),
            version: String::new(),
            build_epoch: 0,
            gc_policy_version: GC_POLICY_PROTOCOL_VERSION,
            pending_uploads: 0,
            active_downloads: 0,
            s3_concurrency_total: 0,
            s3_concurrency_used: 0,
            upload_queue_capacity: 0,
            uploads_completed: 0,
            uploads_failed: 0,
            uploads_skipped: 0,
            uploads_suppressed: 0,
            downloads_completed: 0,
            downloads_failed: 0,
            downloads_suppressed: 0,
            remote_check_roundtrips: 0,
            negative_hits: 0,
            negative_entries: 0,
            remote_degraded: false,
            bytes_uploaded: 0,
            bytes_downloaded: 0,
            recent_transfers: Vec::new(),
            prefetch: PrefetchStatsSnapshot::default(),
            in_flight: Vec::new(),
            effective_config: None,
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
            blob_stats: None,
            recent_summaries: Vec::new(),
            version: String::new(),
            build_epoch: 0,
            gc_policy_version: GC_POLICY_PROTOCOL_VERSION,
            pending_uploads: 0,
            active_downloads: 0,
            s3_concurrency_total: 0,
            s3_concurrency_used: 0,
            upload_queue_capacity: 0,
            uploads_completed: 0,
            uploads_failed: 0,
            uploads_skipped: 0,
            uploads_suppressed: 0,
            downloads_completed: 0,
            downloads_failed: 0,
            downloads_suppressed: 0,
            remote_check_roundtrips: 0,
            negative_hits: 0,
            negative_entries: 0,
            remote_degraded: false,
            bytes_uploaded: 0,
            bytes_downloaded: 0,
            recent_transfers: Vec::new(),
            prefetch: PrefetchStatsSnapshot::default(),
            in_flight: Vec::new(),
            effective_config: None,
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
    fn daemon_keeps_the_load_time_config_provenance_after_a_file_edit() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.toml");
        std::fs::write(&config_path, "[cache]\nlocal_max_size = \"10MiB\"\n").unwrap();
        let provenance = crate::config::config_file_provenance_at(config_path.clone());

        let mut config = test_config(dir.path());
        config.max_size = 10 * 1024 * 1024;
        std::fs::write(&config_path, "[cache]\nlocal_max_size = \"20MiB\"\n").unwrap();

        let daemon = Daemon::new_with_provenance(config, &provenance);
        assert_eq!(daemon.effective_config.max_size, 10 * 1024 * 1024);
        assert_eq!(
            daemon.effective_config.config_path,
            config_path.display().to_string()
        );
        assert_eq!(
            daemon.effective_config.config_fingerprint.as_deref(),
            Some(provenance.fingerprint.as_str())
        );
        assert!(
            crate::config::config_file_has_changed(&provenance),
            "the watcher must compare against the parsed snapshot, not a fresh startup baseline"
        );
    }

    #[test]
    fn test_handle_stats_empty_store() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let mut summaries = (0..7)
            .map(|index| {
                format!(
                    "{{\"ts\":\"2026-08-09T00:00:0{index}Z\",\"schema\":1,\"session_id\":\"s{index}\"}}"
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        summaries.push('\n');
        std::fs::write(config.summary_log_path(), summaries).unwrap();
        let daemon = Daemon::new(config);

        let resp = daemon.handle_stats(&StatsRequest {
            include_entries: true,
            include_summaries: false,
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
        assert_eq!(stats.blob_stats.as_ref().unwrap().total_blobs, 0);
        assert!(
            stats.recent_summaries.is_empty(),
            "polling requests must not read summaries"
        );

        // #689: the daemon reports what IT loaded, so a CLI resolving a
        // different config can render daemon truth and name the divergence.
        let eff = stats.effective_config.expect("effective config reported");
        assert_eq!(eff.max_size, 50 * 1024 * 1024);
        assert_eq!(eff.cache_dir, dir.path().display().to_string());
        assert_eq!(
            eff.socket_path,
            dir.path().join("daemon.sock").display().to_string()
        );
        assert!(eff.started_at_ms > 0, "startup capture stamps a time");
        assert!(eff.config_fingerprint.is_some());
        assert!(
            !eff.config_path.is_empty(),
            "resolved config path is always reportable, even when the file is absent"
        );

        let with_summaries = daemon.handle_stats(&StatsRequest {
            include_entries: false,
            include_summaries: true,
            sort_by: None,
            event_hours: Some(24),
            client_epoch: 0,
        });
        let ids = with_summaries
            .stats
            .unwrap()
            .recent_summaries
            .into_iter()
            .map(|summary| summary.session_id)
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            ["s2", "s3", "s4", "s5", "s6"],
            "one-shot stats requests receive the newest bounded summary tail"
        );
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
            include_summaries: false,
            sort_by: Some("size".into()),
            event_hours: Some(24),
            client_epoch: 0,
        });
        assert!(resp.ok);
        let stats = resp.stats.unwrap();
        assert_eq!(stats.entry_count, 1);
        assert!(stats.total_size >= 100);
        assert_eq!(stats.blob_stats.as_ref().unwrap().total_blobs, 1);
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
            include_summaries: false,
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
                include_summaries: false,
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

    /// LocalLookup roundtrip (kunobi-ninja/kache#565): a committed entry
    /// answers `hit` with restorable meta AND a committed pin (the fresh
    /// `last_accessed`/`hit_count` write that guards the wrapper's restore
    /// window against GC); an unknown key answers `miss`. Both entirely
    /// bypass the `with_store` mutex.
    #[tokio::test]
    async fn test_socket_local_lookup_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let store = Store::open(&config).unwrap();
        let output_file = dir.path().join("out.rlib");
        std::fs::write(&output_file, b"artifact-bytes").unwrap();
        store
            .put(
                "0000000000000000000000000000000000000000000000000000000000000001",
                "probe_crate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(output_file, "libout.rlib".to_string())],
                "cached stdout",
                "",
            )
            .unwrap();
        // Age the entry so the pin's `last_accessed` refresh is observable.
        let index_db = crate::store::open_index_db(&config.index_db_path()).unwrap();
        index_db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour')",
                [],
            )
            .unwrap();
        drop(store);

        let daemon = Arc::new(Daemon::new(config));
        let key = "0000000000000000000000000000000000000000000000000000000000000001";
        let resp = one_shot_request(
            &daemon,
            &socket_path,
            &Request::LocalLookup(LocalLookupRequest {
                key: key.to_string(),
                client_epoch: 0,
            }),
        )
        .await;
        assert!(resp.ok);
        let reply = resp.local_lookup.expect("local_lookup payload");
        assert_eq!(reply.outcome, "hit");
        let meta = reply.meta.expect("hit carries meta");
        assert_eq!(meta.cache_key, key);
        assert_eq!(meta.stdout, "cached stdout");
        assert_eq!(meta.files.len(), 1);

        let (hits, recent): (i64, i64) = index_db
            .query_row(
                "SELECT hit_count, last_accessed >= datetime('now', '-60 seconds')
                 FROM entries WHERE cache_key = ?1",
                [key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(hits, 1, "hit must be accounted by the pin writer");
        assert_eq!(recent, 1, "pin must refresh last_accessed before the reply");

        let resp = one_shot_request(
            &daemon,
            &socket_path,
            &Request::LocalLookup(LocalLookupRequest {
                key: "0000000000000000000000000000000000000000000000000000000000000002".to_string(),
                client_epoch: 0,
            }),
        )
        .await;
        assert!(resp.ok);
        assert_eq!(resp.local_lookup.expect("payload").outcome, "miss");
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
                session_id: String::new(),
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

        let key = test_cache_key("socket-batch-no-remote");
        let entry_dir = config.store_dir().join(&key).to_string_lossy().into_owned();
        let daemon = Arc::new(Daemon::new(config));
        let resp = one_shot_request(
            &daemon,
            &socket_path,
            &Request::BatchRemoteCheck(BatchRemoteCheckRequest {
                checks: vec![RemoteCheckRequest {
                    key,
                    entry_dir,
                    crate_name: "serde".into(),
                    deadline_ms: None,
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
                include_summaries: false,
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
            // send_gc_request first performs a non-mutating stats capability
            // probe, then opens a fresh connection for the GC request.
            for _ in 0..2 {
                let stream = listener.accept().await.expect("accept");
                handle_connection(stream, &daemon, &AtomicBool::new(false), &Notify::new())
                    .await
                    .expect("handle_connection");
            }
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
    async fn test_send_gc_request_rejects_old_daemon_before_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        // Build a valid current stats response, then remove the capability to
        // model an old daemon without also making it look stale by epoch.
        let daemon = Daemon::new(config.clone());
        let response = daemon.handle_stats(&StatsRequest {
            include_entries: false,
            include_summaries: false,
            sort_by: None,
            event_hours: None,
            client_epoch: build_epoch(),
        });
        let mut response_value = serde_json::to_value(response).unwrap();
        response_value
            .get_mut("stats")
            .and_then(serde_json::Value::as_object_mut)
            .unwrap()
            .remove("gc_policy_version");
        let mut response_line = serde_json::to_string(&response_value).unwrap();
        response_line.push('\n');

        let listener = bind_listener(&socket_path);
        let server = tokio::spawn(async move {
            let mut stream = listener.accept().await.expect("accept stats probe");
            let mut request_line = String::new();
            {
                let mut reader = BufReader::new(&stream);
                reader
                    .read_line(&mut request_line)
                    .await
                    .expect("read stats probe");
            }
            assert!(matches!(
                serde_json::from_str::<Request>(&request_line).unwrap(),
                Request::Stats(_)
            ));
            stream
                .write_all(response_line.as_bytes())
                .await
                .expect("write old stats response");
            drop(stream);

            // A capability failure must return without opening a second
            // connection and therefore without sending Request::Gc.
            assert!(
                tokio::time::timeout(Duration::from_millis(200), listener.accept())
                    .await
                    .is_err(),
                "client sent a request after the unsupported stats response"
            );
        });

        let cfg = config.clone();
        let error = match tokio::task::spawn_blocking(move || send_gc_request(&cfg, Some(0)))
            .await
            .unwrap()
        {
            Ok(_) => panic!("old daemon must be rejected before GC"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("predates GC policy version"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_send_remote_check_client_roundtrip() {
        // CLIENT side: send_remote_check connects to a live in-process server,
        // sends a RemoteCheck, and parses the response. The daemon's key cache
        // is fresh + authoritative and lacks the key, so it answers a definitive
        // miss without touching the remote. Covers send_remote_check's
        // Ok(resp)+resp.ok success
        // arm (daemon.rs 3309-3314) through the real socket + handle_connection.
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(crate::config::RemoteConfig::test_s3("test", "artifacts"));
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
        let entry_dir = cfg.store_dir().join(&missing);
        let result = tokio::task::spawn_blocking(move || {
            send_remote_check(&cfg, &missing, &entry_dir, "crate")
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
        let entry_dir = cfg.store_dir().join(&key);
        let result =
            tokio::task::spawn_blocking(move || send_remote_check(&cfg, &key, &entry_dir, "crate"))
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
            &Request::Gc(GcRequest::explicit_age(0)),
        )
        .await;

        // The GC handler ran end-to-end over a populated store (backfill +
        // dedup + age eviction) and reported a structured eviction count.
        assert!(resp.ok, "gc should succeed: {resp:?}");
        assert!(resp.evicted.is_some(), "gc reports an evicted count");
    }

    // ── Daemon remote handlers driven against an injected backend ────────────

    fn test_remote_config() -> crate::config::RemoteConfig {
        crate::config::RemoteConfig::test_s3("bucket", "prefix")
    }

    fn test_remote_backend() -> Arc<dyn crate::remote_backend::RemoteBackend> {
        Arc::new(crate::remote_backend::memory_backend())
    }

    fn test_manifest_object_key(cache_key: &str, crate_name: &str) -> String {
        format!("prefix/v3/manifests/{crate_name}/{cache_key}.json")
    }

    fn test_pack_object_key(cache_key: &str, crate_name: &str) -> String {
        format!("prefix/v3/packs/{crate_name}/{cache_key}.tar.zst")
    }

    fn test_build_manifest_object_key() -> String {
        format!(
            "prefix/_manifests/{}.json",
            crate::cli::default_manifest_key()
        )
    }

    async fn put_test_object(
        backend: &Arc<dyn crate::remote_backend::RemoteBackend>,
        key: &str,
        body: &[u8],
    ) {
        backend
            .put(key, body.to_vec(), None)
            .await
            .expect("seed test remote object");
    }

    struct PutFailBackend;

    #[async_trait::async_trait]
    impl crate::remote_backend::RemoteBackend for PutFailBackend {
        async fn head(&self, _key: &str) -> Result<bool> {
            Ok(false)
        }

        async fn get(
            &self,
            _key: &str,
            _max_bytes: Option<u64>,
        ) -> Result<Option<crate::remote_backend::GetObject>> {
            Ok(None)
        }

        async fn put(&self, _key: &str, _body: Vec<u8>, _content_type: Option<&str>) -> Result<()> {
            anyhow::bail!("injected PUT failure")
        }

        async fn list(&self, _prefix: &str) -> Result<Vec<String>> {
            Ok(Vec::new())
        }

        fn describe(&self, key: &str) -> String {
            format!("failure://test/{key}")
        }
    }

    #[tokio::test]
    async fn test_socket_remote_check_miss_with_injected_mock_client() {
        // Remote configured + an empty in-memory backend: handle_remote_check
        // runs its head-probe path and reports found=false.
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let key = test_cache_key("socket-remote-miss");
        let entry_dir = config.store_dir().join(&key).to_string_lossy().into_owned();
        let client = test_remote_backend();
        let daemon = Arc::new(Daemon::new(config.clone()));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let resp = one_shot_request(
            &daemon,
            &socket_path,
            &Request::RemoteCheck(RemoteCheckRequest {
                key,
                entry_dir,
                crate_name: "serde".into(),
                deadline_ms: None,
            }),
        )
        .await;

        assert!(resp.ok, "remote check should return a response: {resp:?}");
        assert_eq!(resp.found, Some(false), "missing remote key -> found=false");
    }

    #[tokio::test]
    async fn test_socket_prefetch_empty_keys_lists_remote_then_no_op() {
        // Empty prefetch keys + an empty backend: handle_prefetch lists the
        // remote, finds nothing missing, and returns ok ("nothing to fetch").
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let client = test_remote_backend();
        let daemon = Arc::new(Daemon::new(config));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let resp = one_shot_request(
            &daemon,
            &socket_path,
            &Request::Prefetch(PrefetchRequest {
                keys: Vec::new(),
                warm_all: false,
            }),
        )
        .await;

        assert!(resp.ok, "prefetch over empty remote should be ok: {resp:?}");
    }

    #[tokio::test]
    async fn test_do_upload_skips_when_entry_already_in_remote() {
        // A seeded manifest makes do_upload see that the entry already exists,
        // so it returns ok without uploading.
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());

        let key = test_cache_key("already-remote-upload");
        let client = test_remote_backend();
        put_test_object(&client, &test_manifest_object_key(&key, "serde"), b"{}").await;
        let daemon = Arc::new(Daemon::new(config));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let resp = daemon
            .do_upload(&UploadJob {
                key,
                entry_dir: dir.path().join("entry").to_string_lossy().into_owned(),
                crate_name: "serde".into(),
                client_epoch: 0,
            })
            .await;

        assert!(
            resp.ok,
            "already-present upload should be a no-op ok: {resp:?}"
        );
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
        config.prefetch_enabled = false;
        let key = test_cache_key("new-upload");
        seed_store_entry(&config, &key, "serde", dir.path());
        let entry_dir = config.store_dir().join(&key);

        let client = test_remote_backend();
        let daemon = Arc::new(Daemon::new(config));
        assert!(
            daemon.remote_backend.set(client.clone()).is_ok(),
            "inject mock backend"
        );

        let resp = daemon
            .do_upload(&UploadJob {
                key: key.clone(),
                entry_dir: entry_dir.to_string_lossy().into_owned(),
                crate_name: "serde".into(),
                client_epoch: 0,
            })
            .await;

        assert!(resp.ok, "upload of a new entry should succeed: {resp:?}");
        assert!(
            client
                .head(&test_pack_object_key(&key, "serde"))
                .await
                .unwrap()
        );
        assert!(
            client
                .head(&test_manifest_object_key(&key, "serde"))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_do_upload_records_failure_when_put_errors() {
        // Mock 404s the HEAD (not present -> proceed) then 403s the pack PUT, so
        // upload_entry errors and do_upload takes its Err branch: uploads_failed++
        // and a failure TransferEvent, returning Response::err (daemon.rs 1459-1500).
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let key = test_cache_key("failed-upload");
        seed_store_entry(&config, &key, "serde", dir.path());
        let entry_dir = config.store_dir().join(&key);

        let client: Arc<dyn crate::remote_backend::RemoteBackend> = Arc::new(PutFailBackend);
        let daemon = Arc::new(Daemon::new(config));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let resp = daemon
            .do_upload(&UploadJob {
                key,
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
    }

    #[tokio::test]
    async fn test_handle_build_started_falls_back_to_local_planning() {
        // With a remote configured but no planner endpoint (resolve_prefetch_plan
        // -> Ok(None)) and no local/remote candidates, handle_build_started runs
        // the fallback planner, finds nothing to prefetch, and returns ok.
        // Covers the fallback-planning branch (daemon.rs 2120-2132). Namespace is
        // None so no remote shard query is issued.
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());

        let client = test_remote_backend();
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
            session_id: String::new(),
        };
        let resp = daemon.handle_build_started(&req).await;
        assert!(
            resp.ok,
            "fallback with nothing to prefetch should be ok: {resp:?}"
        );
    }

    #[tokio::test]
    async fn test_batch_remote_check_remote_path_with_injected_mock() {
        // Two checks against an empty backend: the batch
        // handler fans out handle_remote_check and returns one found=false per
        // check. Covers handle_batch_remote_check's remote path + join_all.
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());

        let key_a = test_cache_key("batch-a");
        let key_b = test_cache_key("batch-b");
        let entry_a = config
            .store_dir()
            .join(&key_a)
            .to_string_lossy()
            .into_owned();
        let entry_b = config
            .store_dir()
            .join(&key_b)
            .to_string_lossy()
            .into_owned();
        let client = test_remote_backend();
        let daemon = Arc::new(Daemon::new(config));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let resp = daemon
            .handle_batch_remote_check(&BatchRemoteCheckRequest {
                checks: vec![
                    RemoteCheckRequest {
                        key: key_a,
                        entry_dir: entry_a,
                        crate_name: "serde".into(),
                        deadline_ms: None,
                    },
                    RemoteCheckRequest {
                        key: key_b,
                        entry_dir: entry_b,
                        crate_name: "tokio".into(),
                        deadline_ms: None,
                    },
                ],
            })
            .await;

        assert!(resp.ok);
        let results = resp.batch_results.expect("batch results present");
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.found == Some(false)));
    }

    #[tokio::test]
    async fn test_remote_check_hit_then_download_failure() {
        // Injected mock: HEAD 200 (entry exists) then a garbage pack body for the
        // GET, so download_entry fails. Covers handle_remote_check's HIT branch +
        // download claim/semaphore + download_entry attempt + the error path.
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());

        let key = test_cache_key("corrupt-download");
        let entry_dir = config.store_dir().join(&key).to_string_lossy().into_owned();
        let client = test_remote_backend();
        put_test_object(&client, &test_manifest_object_key(&key, "serde"), b"{}").await;
        put_test_object(&client, &test_pack_object_key(&key, "serde"), b"not a pack").await;
        let daemon = Arc::new(Daemon::new(config));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let resp = daemon
            .handle_remote_check(&RemoteCheckRequest {
                key,
                entry_dir,
                crate_name: "serde".into(),
                deadline_ms: None,
            })
            .await;

        // The entry was present remotely but its download failed -> error.
        assert!(
            !resp.ok,
            "download failure should surface as an error: {resp:?}"
        );
        assert!(resp.error.is_some());
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

        // The empty backend answers a clean miss (no HEAD happens — key cache
        // says positive).
        let client = test_remote_backend();
        let daemon = Arc::new(Daemon::new(config));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        // Fresh, authoritative-positive key cache entry for the key.
        let key = test_cache_key("gone-positive");
        let mut keys = HashMap::new();
        keys.insert(key.clone(), "serde".to_string());
        daemon.key_cache.populate(keys).await;

        let resp = daemon
            .handle_remote_check(&RemoteCheckRequest {
                key: key.clone(),
                entry_dir: daemon.entry_dir_for(&key).to_string_lossy().into_owned(),
                crate_name: "serde".into(),
                deadline_ms: None,
            })
            .await;

        assert!(
            resp.ok,
            "GET 404 must be a clean miss, not an error: {resp:?}"
        );
        assert_eq!(resp.found, Some(false));
        // The stale positive was evicted.
        assert_eq!(daemon.key_cache.check(&key).await, Some(false));
        // Not counted as a failed transfer.
        assert_eq!(
            daemon
                .transfer_counters
                .downloads_failed
                .load(Ordering::Relaxed),
            0
        );
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
        config.prefetch_enabled = false;
        let key = test_cache_key("successful-download");
        let pack = build_entry_pack(&key, "serde");
        // The wrapper passes entry_dir = store_dir/key; mirror that so the import
        // finds the extracted entry.
        let entry_dir = config.store_dir().join(&key);

        let client = test_remote_backend();
        put_test_object(&client, &test_manifest_object_key(&key, "serde"), b"{}").await;
        put_test_object(&client, &test_pack_object_key(&key, "serde"), &pack).await;
        let daemon = Arc::new(Daemon::new(config.clone()));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let resp = daemon
            .handle_remote_check(&RemoteCheckRequest {
                key: key.clone(),
                entry_dir: entry_dir.to_string_lossy().into_owned(),
                crate_name: "serde".to_string(),
                deadline_ms: None,
            })
            .await;

        assert!(resp.ok, "hit+download should succeed: {resp:?}");
        assert_eq!(resp.found, Some(true));
        assert!(
            config.store_dir().join(&key).join("meta.json").exists(),
            "entry should be imported into the local store"
        );
    }

    #[tokio::test]
    async fn stale_meta_json_does_not_short_circuit_a_first_claim_leader() {
        // The under-claim meta.json re-check (#620) applies ONLY to a waiter
        // that won the re-claim after a failed leader; a first-claim leader
        // that finds a stale pre-existing meta.json on disk must still
        // download and import, or the entry never reaches the local index.
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        config.prefetch_enabled = false;
        let key = test_cache_key("stale-meta");
        let pack = build_entry_pack(&key, "serde");
        let entry_dir = config.store_dir().join(&key);
        std::fs::create_dir_all(&entry_dir).unwrap();
        std::fs::write(entry_dir.join("meta.json"), "{}").unwrap(); // stale, no DB row

        let client = test_remote_backend();
        put_test_object(&client, &test_manifest_object_key(&key, "serde"), b"{}").await;
        put_test_object(&client, &test_pack_object_key(&key, "serde"), &pack).await;
        let daemon = Arc::new(Daemon::new(config.clone()));
        assert!(daemon.remote_backend.set(client).is_ok());

        let resp = daemon
            .handle_remote_check(&RemoteCheckRequest {
                key: key.clone(),
                entry_dir: entry_dir.to_string_lossy().into_owned(),
                crate_name: "serde".to_string(),
                deadline_ms: None,
            })
            .await;

        assert!(resp.ok, "leader download should succeed: {resp:?}");
        assert_eq!(resp.found, Some(true));
        let store = Store::open(&config).unwrap();
        assert!(
            store.contains(&key),
            "the leader must download and import — a stale meta.json is not a hit"
        );
    }

    #[tokio::test]
    async fn test_handle_prefetch_disabled_ignores_explicit_keys() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        config.prefetch_enabled = false;
        let key = "0123456789abcdef".repeat(4);
        let pack = build_entry_pack(&key, "serde");

        let client = test_remote_backend();
        put_test_object(&client, &test_pack_object_key(&key, "serde"), &pack).await;
        let daemon = Arc::new(Daemon::new(config.clone()));
        assert!(daemon.remote_backend.set(client).is_ok());

        let resp = daemon
            .handle_prefetch(&PrefetchRequest {
                keys: vec![(key.clone(), "serde".to_string())],
                warm_all: false,
            })
            .await;

        assert!(resp.ok);
        assert!(!config.store_dir().join(&key).join("meta.json").exists());
        assert_eq!(
            daemon
                .prefetch_stats
                .downloads_completed
                .load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn test_handle_prefetch_explicit_key_downloads_in_background() {
        // handle_prefetch with an explicit key spawns the background download
        // coordinator. With the in-memory backend serving a valid pack, the
        // coordinator downloads + imports the entry. Covers the prefetch
        // coordinator + per-key download task (the biggest daemon block).
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        // handle_prefetch validates the key: exactly 64 hex chars.
        let key = "abcdef0123456789".repeat(4);
        let key = key.as_str();
        let pack = build_entry_pack(key, "serde");

        let client = test_remote_backend();
        put_test_object(&client, &test_pack_object_key(key, "serde"), &pack).await;
        let daemon = Arc::new(Daemon::new(config.clone()));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let resp = daemon
            .handle_prefetch(&PrefetchRequest {
                keys: vec![(key.to_string(), "serde".to_string())],
                warm_all: false,
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
    }

    #[tokio::test]
    async fn test_handle_prefetch_records_a_failed_download() {
        // The in-memory backend serves garbage for the pack GET, so the coordinator's
        // download_entry fails and the per-key task takes its error branch:
        // downloads_failed++ and a failure TransferEvent, with no import.
        // Covers handle_prefetch's download-error path (daemon.rs 2006-2034).
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let key = "abcdef0123456789".repeat(4);
        let key = key.as_str();

        let client = test_remote_backend();
        put_test_object(
            &client,
            &test_pack_object_key(key, "serde"),
            b"not a valid pack",
        )
        .await;
        let daemon = Arc::new(Daemon::new(config.clone()));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let resp = daemon
            .handle_prefetch(&PrefetchRequest {
                keys: vec![(key.to_string(), "serde".to_string())],
                warm_all: false,
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
    }

    #[tokio::test]
    async fn test_populate_key_cache_lists_and_populates() {
        // Injected mock returns a 2-key manifest listing -> populate_key_cache
        // lists S3 and seeds the in-memory key cache. Covers the background
        // key-cache population path.
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());

        let key_a = test_cache_key("listed-key-a");
        let key_b = test_cache_key("listed-key-b");
        let client = test_remote_backend();
        put_test_object(&client, &test_manifest_object_key(&key_a, "serde"), b"{}").await;
        put_test_object(&client, &test_manifest_object_key(&key_b, "tokio"), b"{}").await;
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
        assert_eq!(daemon.key_cache.check(&key_a).await, Some(true));
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
        let client = test_remote_backend();
        put_test_object(&client, &test_build_manifest_object_key(), &body).await;
        let daemon = Arc::new(Daemon::new(config));

        // Should complete without panicking and without queuing the cheap crate.
        monolithic_manifest_prefetch(&daemon, client.as_ref(), &remote).await;
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

        let client = test_remote_backend();
        let daemon = Arc::new(Daemon::new(config));

        monolithic_manifest_prefetch(&daemon, client.as_ref(), &remote).await; // must not panic
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
        let client = test_remote_backend();
        put_test_object(&client, &test_build_manifest_object_key(), &body).await;
        // The background pack download may fail — dispatch is what this covers.
        put_test_object(&client, &test_pack_object_key(&key, "expensive"), b"nope").await;
        let daemon = Arc::new(Daemon::new(config));
        assert!(
            daemon.remote_backend.set(client.clone()).is_ok(),
            "inject mock backend"
        );

        monolithic_manifest_prefetch(&daemon, client.as_ref(), &remote).await; // dispatches + returns
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

        let client = test_remote_backend();
        let daemon = Arc::new(Daemon::new(config));

        let count = shard_prefetch(&daemon, &client, "prefix", "ns", &lock)
            .await
            .expect("shard prefetch should succeed");
        assert_eq!(count, 0, "no shards matched -> nothing queued");
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
                    deadline_ms: None,
                },
                RemoteCheckRequest {
                    key: "key2".into(),
                    entry_dir: "/tmp/key2".into(),
                    crate_name: String::new(),
                    deadline_ms: None,
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
            warm_all: false,
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
        let req = Request::Prefetch(PrefetchRequest {
            keys: vec![],
            warm_all: false,
        });
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
                kache_core::PrefetchCandidate::new(valid_key.clone(), "serde".into()),
                // Malformed key from an untrusted planner: must be dropped.
                kache_core::PrefetchCandidate::new("../../../etc/passwd".into(), "serde".into()),
                // Valid key but path-escaping crate name: must be dropped.
                kache_core::PrefetchCandidate::new(valid_key.clone(), "../evil".into()),
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
    async fn test_prefetch_disabled_remote_releases_warming_barrier() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(crate::config::RemoteConfig::test_s3("test", "artifacts"));
        config.prefetch_enabled = false;
        let daemon = Arc::new(Daemon::new(config));

        assert!(
            start_manifest_warming(&daemon).is_none(),
            "prefetch-disabled startup must not spawn a warming task"
        );
        let start = std::time::Instant::now();
        assert!(daemon.wait_for_warming(Duration::from_millis(100)).await);
        assert!(
            start.elapsed() < Duration::from_millis(500),
            "prefetch-disabled exact checks must not pay the warming grace"
        );
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

    // RemoteBreaker state-transition tests live with the breaker in
    // `remote_resilience`; the tests here cover the daemon paths that
    // consult it.

    #[tokio::test]
    async fn test_handle_remote_check_skips_head_when_probe_circuit_is_open() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(crate::config::RemoteConfig::test_s3("test", "artifacts"));
        let daemon = Daemon::new(config);
        daemon.signal_warming_complete();

        daemon.remote_breaker.note_failure("HEAD", "boom-1");
        daemon.remote_breaker.note_failure("HEAD", "boom-2");
        daemon.remote_breaker.note_failure("HEAD", "boom-3");

        let key = test_cache_key("open-read-breaker");
        let req = RemoteCheckRequest {
            entry_dir: daemon.entry_dir_for(&key).to_string_lossy().into_owned(),
            key,
            crate_name: "crate".into(),
            deadline_ms: None,
        };
        let resp = daemon.handle_remote_check(&req).await;
        assert!(resp.ok);
        assert_eq!(resp.found, Some(false));
        assert_eq!(
            daemon
                .remote_breaker
                .suppressed_ops(crate::remote_resilience::RemoteDirection::Read),
            1
        );
    }

    #[tokio::test]
    async fn test_handle_remote_check_authoritative_key_cache_skips_s3() {
        // A freshly-populated key cache that doesn't contain the requested key is
        // authoritative (age <= KEY_CACHE_AUTHORITATIVE_FOR): the daemon answers
        // a definitive miss without ever touching the remote. Covers
        // handle_remote_check's Some(false)+authoritative branch.
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(crate::config::RemoteConfig::test_s3("test", "artifacts"));
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
            entry_dir: daemon
                .entry_dir_for(&missing)
                .to_string_lossy()
                .into_owned(),
            key: missing,
            crate_name: "crate".into(),
            deadline_ms: None,
        };
        let resp = daemon.handle_remote_check(&req).await;
        assert!(resp.ok);
        assert_eq!(
            resp.found,
            Some(false),
            "fresh key cache should authoritatively report the missing key as not found"
        );
        // The authoritative short-circuit must NOT have suppressed a remote op —
        // it never reached the degraded-breaker path.
        assert_eq!(
            daemon
                .remote_breaker
                .suppressed_ops(crate::remote_resilience::RemoteDirection::Read),
            0
        );
    }

    // ── Remote resilience tests (#327, #564) ──────────────────────

    /// Backend that panics on GET: proves a gated path never reached S3.
    struct PanicOnGetBackend;

    #[async_trait::async_trait]
    impl crate::remote_backend::RemoteBackend for PanicOnGetBackend {
        async fn head(&self, _key: &str) -> Result<bool> {
            Ok(true)
        }

        async fn get(
            &self,
            key: &str,
            _max_bytes: Option<u64>,
        ) -> Result<Option<crate::remote_backend::GetObject>> {
            panic!("GET {key} must not be issued while the remote is degraded");
        }

        async fn put(&self, _key: &str, _body: Vec<u8>, _content_type: Option<&str>) -> Result<()> {
            panic!("PUT must not be issued while the remote is degraded");
        }

        async fn list(&self, _prefix: &str) -> Result<Vec<String>> {
            Ok(Vec::new())
        }

        fn describe(&self, key: &str) -> String {
            format!("panic-on-get://test/{key}")
        }
    }

    /// Backend whose GET stalls forever: the restore-deadline case.
    struct StallingGetBackend;

    #[async_trait::async_trait]
    impl crate::remote_backend::RemoteBackend for StallingGetBackend {
        async fn head(&self, _key: &str) -> Result<bool> {
            Ok(true)
        }

        async fn get(
            &self,
            _key: &str,
            _max_bytes: Option<u64>,
        ) -> Result<Option<crate::remote_backend::GetObject>> {
            std::future::pending::<()>().await;
            unreachable!()
        }

        async fn put(&self, _key: &str, _body: Vec<u8>, _content_type: Option<&str>) -> Result<()> {
            Ok(())
        }

        async fn list(&self, _prefix: &str) -> Result<Vec<String>> {
            Ok(Vec::new())
        }

        fn describe(&self, key: &str) -> String {
            format!("stalling://test/{key}")
        }
    }

    /// Backend whose HEAD fails with the given error class on every call.
    struct FailingHeadBackend {
        timeout: bool,
        calls: std::sync::atomic::AtomicU64,
    }

    #[async_trait::async_trait]
    impl crate::remote_backend::RemoteBackend for FailingHeadBackend {
        async fn head(&self, _key: &str) -> Result<bool> {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if self.timeout {
                Err(anyhow::Error::new(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "connect timed out",
                )))
            } else {
                Err(anyhow::Error::new(
                    opendal::Error::new(opendal::ErrorKind::RateLimited, "503 Service Unavailable")
                        .set_temporary(),
                ))
            }
        }

        async fn get(
            &self,
            _key: &str,
            _max_bytes: Option<u64>,
        ) -> Result<Option<crate::remote_backend::GetObject>> {
            Ok(None)
        }

        async fn put(&self, _key: &str, _body: Vec<u8>, _content_type: Option<&str>) -> Result<()> {
            Ok(())
        }

        async fn list(&self, _prefix: &str) -> Result<Vec<String>> {
            Ok(Vec::new())
        }

        fn describe(&self, key: &str) -> String {
            format!("failing-head://test/{key}")
        }
    }

    fn resilience_test_daemon(
        dir: &Path,
        backend: Arc<dyn crate::remote_backend::RemoteBackend>,
    ) -> Daemon {
        let mut config = test_config(dir);
        config.remote = Some(test_remote_config());
        let daemon = Daemon::new(config);
        daemon.signal_warming_complete();
        assert!(
            daemon.remote_backend.set(backend).is_ok(),
            "inject mock backend"
        );
        daemon
    }

    fn check_request(dir: &Path, key: &str) -> RemoteCheckRequest {
        let key = test_cache_key(key);
        RemoteCheckRequest {
            entry_dir: dir.join("store").join(&key).to_string_lossy().into_owned(),
            key,
            crate_name: "serde".into(),
            deadline_ms: None,
        }
    }

    /// #564: the second check for the same definitively-missing key must be
    /// answered from the negative cache — one S3 round trip, one negative
    /// hit — and a successful upload of the key must clear the entry.
    #[tokio::test]
    async fn test_negative_cache_second_check_skips_s3_and_upload_invalidates() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = resilience_test_daemon(dir.path(), test_remote_backend());
        let req = check_request(dir.path(), "cafe0123deadbeef");

        let resp = daemon.handle_remote_check(&req).await;
        assert_eq!(resp.found, Some(false));
        let roundtrips_after_first = daemon
            .transfer_counters
            .remote_check_roundtrips
            .load(Ordering::Relaxed);
        assert_eq!(roundtrips_after_first, 1, "first check pays one HEAD");
        assert_eq!(daemon.negative_keys.len(), 1, "definitive miss remembered");

        let resp = daemon.handle_remote_check(&req).await;
        assert_eq!(resp.found, Some(false));
        assert_eq!(
            daemon
                .transfer_counters
                .remote_check_roundtrips
                .load(Ordering::Relaxed),
            roundtrips_after_first,
            "second check must not touch S3"
        );
        assert_eq!(daemon.negative_keys.hits(), 1);

        // An upload observing the key present flips it positive immediately.
        // (The key-cache side of `note_key_present` is a no-op until the
        // first LIST populate — S3KeyCache's own tests cover insert — so the
        // invariant asserted here is the #564 one: no stale negative entry.)
        daemon.note_key_present(&req.key, &req.crate_name).await;
        assert_eq!(
            daemon.negative_keys.len(),
            0,
            "upload invalidates the negative entry"
        );
        let resp = daemon.handle_remote_check(&req).await;
        assert_eq!(
            resp.found,
            Some(false),
            "the check after invalidation reaches S3 again instead of the negative cache"
        );
        assert_eq!(
            daemon
                .transfer_counters
                .remote_check_roundtrips
                .load(Ordering::Relaxed),
            2,
            "post-invalidation check pays a fresh round trip"
        );
    }

    /// #327: while the breaker is degraded, a key-cache positive must NOT
    /// reach S3 — the restore reports a miss immediately and rustc
    /// recompiles locally. `PanicOnGetBackend` proves no GET was issued.
    #[tokio::test]
    async fn test_degraded_breaker_gates_the_download_path() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = resilience_test_daemon(dir.path(), Arc::new(PanicOnGetBackend));
        let req = check_request(dir.path(), "cafe0123deadbeef");
        daemon
            .key_cache
            .populate(HashMap::from([(req.key.clone(), "serde".to_string())]))
            .await;

        daemon.remote_breaker.note_failure("GET", "boom-1");
        daemon.remote_breaker.note_failure("GET", "boom-2");
        daemon.remote_breaker.note_failure("GET", "boom-3");
        assert!(daemon.remote_breaker.is_degraded());

        let resp = daemon.handle_remote_check(&req).await;
        assert!(resp.ok);
        assert_eq!(resp.found, Some(false));
        assert_eq!(
            daemon
                .transfer_counters
                .downloads_suppressed
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            daemon.negative_keys.len(),
            0,
            "a suppressed check is not a definitive miss"
        );
    }

    /// #327: a restore that exceeds `remote_restore_timeout_secs` is dropped
    /// and answered as a miss within the deadline, and the timeout feeds the
    /// breaker.
    #[tokio::test]
    async fn test_restore_deadline_returns_miss_instead_of_hanging() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        config.remote_restore_timeout_secs = 1;
        let daemon = Daemon::new(config);
        daemon.signal_warming_complete();
        assert!(
            daemon
                .remote_backend
                .set(Arc::new(StallingGetBackend) as Arc<dyn crate::remote_backend::RemoteBackend>)
                .is_ok()
        );
        let req = check_request(dir.path(), "cafe0123deadbeef");
        daemon
            .key_cache
            .populate(HashMap::from([(req.key.clone(), "serde".to_string())]))
            .await;

        let start = std::time::Instant::now();
        let resp = daemon.handle_remote_check(&req).await;
        let elapsed = start.elapsed();
        assert!(resp.ok);
        assert_eq!(resp.found, Some(false), "deadline elapse answers miss");
        assert!(
            elapsed >= Duration::from_millis(900) && elapsed < Duration::from_secs(5),
            "restore must return at ~the 1s deadline, took {elapsed:?}"
        );
        assert_eq!(
            daemon
                .transfer_counters
                .downloads_failed
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            daemon.negative_keys.len(),
            0,
            "a timeout is never negative-cached"
        );
    }

    #[tokio::test]
    async fn expired_remote_check_queued_by_handler_limiter_never_reaches_backend() {
        let dir = tempfile::tempdir().unwrap();
        let backend = Arc::new(FailingHeadBackend {
            timeout: false,
            calls: 0.into(),
        });
        let daemon = Arc::new(resilience_test_daemon(dir.path(), backend.clone()));
        let socket_path = daemon.config.socket_path();
        let listener = bind_listener(&socket_path);

        // Model a saturated production handler limiter. The accepted request
        // parks before parsing/dispatch, but its monotonic budget has already
        // started at accept time.
        let limiter = Arc::new(tokio::sync::Semaphore::new(1));
        let held_slot = limiter.clone().acquire_owned().await.unwrap();
        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
        let server_daemon = daemon.clone();
        let server_limiter = limiter.clone();
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept");
            let request_started_at = Instant::now();
            accepted_tx.send(()).unwrap();
            handle_connection_after_queue(
                stream,
                &server_daemon,
                &AtomicBool::new(false),
                &Notify::new(),
                server_limiter,
                request_started_at,
            )
            .await
        });

        let mut check = check_request(dir.path(), "expired-handler-queue");
        check.deadline_ms = Some(10);
        let request = Request::RemoteCheck(check);
        let client_socket = socket_path.clone();
        let client = tokio::spawn(async move { client_roundtrip(&client_socket, &request).await });

        accepted_rx.await.expect("server accepted request");
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(held_slot);

        let response = tokio::time::timeout(Duration::from_secs(2), client)
            .await
            .expect("expired queued request must receive a prompt miss")
            .expect("client task");
        assert!(response.ok);
        assert_eq!(response.found, Some(false));
        assert_eq!(
            backend.calls.load(Ordering::Relaxed),
            0,
            "an expired request must not start HEAD after leaving the handler queue"
        );
        server
            .await
            .expect("server task")
            .expect("connection handler");
    }

    /// #327: while degraded, `do_upload` defers the durable job without touching S3.
    #[tokio::test]
    async fn test_do_upload_suppressed_while_degraded() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = resilience_test_daemon(dir.path(), Arc::new(PanicOnGetBackend));

        daemon.remote_breaker.note_failure("PUT", "boom-1");
        daemon.remote_breaker.note_failure("PUT", "boom-2");
        daemon.remote_breaker.note_failure("PUT", "boom-3");

        let job = UploadJob {
            key: test_cache_key("deferred-upload"),
            entry_dir: dir.path().join("entry").to_string_lossy().into_owned(),
            crate_name: "serde".into(),
            client_epoch: 0,
        };
        seed_store_entry(&daemon.config, &job.key, "serde", dir.path());
        let durable_job = persist_upload_job(&daemon.config, &job).unwrap();
        let resp = daemon.do_upload(&durable_job).await;
        assert!(!resp.ok, "a deferred upload must stay retryable: {resp:?}");
        assert!(
            resp.error
                .as_deref()
                .is_some_and(|error| error.starts_with("retryable:")),
            "the worker must retain and retry the durable intent: {resp:?}"
        );
        assert!(upload_spool_path(&daemon.config, &job.key).is_file());
        assert_eq!(
            daemon
                .transfer_counters
                .uploads_suppressed
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            daemon
                .transfer_counters
                .uploads_failed
                .load(Ordering::Relaxed),
            0,
            "no PUT was attempted"
        );
    }

    /// #327/#564: HEAD has exactly one daemon attempt for every soft failure;
    /// neither transient failures nor timeouts are negative-cached.
    #[tokio::test]
    async fn test_head_failure_classes_drive_retries_and_skip_negative_cache() {
        // Transient: one attempt. Retry ownership must not be nested under the
        // daemon's semaphore/deadline/breaker boundary.
        let dir = tempfile::tempdir().unwrap();
        let transient = Arc::new(FailingHeadBackend {
            timeout: false,
            calls: 0.into(),
        });
        let daemon = resilience_test_daemon(dir.path(), transient.clone());
        let resp = daemon
            .handle_remote_check(&check_request(dir.path(), "cafe0123deadbeef"))
            .await;
        assert_eq!(resp.found, Some(false), "fail-safe answer is miss");
        assert_eq!(
            transient.calls.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "the daemon must issue one transport attempt"
        );
        assert_eq!(
            daemon.negative_keys.len(),
            0,
            "soft failures are not misses"
        );

        // Timeout: exactly one attempt, and three such checks degrade the
        // breaker so the fourth never reaches the backend.
        let dir = tempfile::tempdir().unwrap();
        let timeouts = Arc::new(FailingHeadBackend {
            timeout: true,
            calls: 0.into(),
        });
        let daemon = resilience_test_daemon(dir.path(), timeouts.clone());
        for key in ["aaaa000000000001", "aaaa000000000002", "aaaa000000000003"] {
            let resp = daemon
                .handle_remote_check(&check_request(dir.path(), key))
                .await;
            assert_eq!(resp.found, Some(false));
        }
        assert_eq!(
            timeouts.calls.load(std::sync::atomic::Ordering::Relaxed),
            3,
            "a timeout must not be retried at the daemon level"
        );
        assert!(daemon.remote_breaker.is_degraded());
        let resp = daemon
            .handle_remote_check(&check_request(dir.path(), "aaaa000000000004"))
            .await;
        assert_eq!(resp.found, Some(false));
        assert_eq!(
            timeouts.calls.load(std::sync::atomic::Ordering::Relaxed),
            3,
            "a degraded breaker suppresses the probe entirely"
        );
        assert_eq!(daemon.negative_keys.len(), 0);
    }

    // ── Prefetch handler tests ────────────────────────────────────

    #[tokio::test]
    async fn test_handle_prefetch_no_remote() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path()); // remote = None
        let daemon = Arc::new(Daemon::new(config));

        let req = PrefetchRequest {
            keys: vec![("k".into(), "mycrate".into())],
            warm_all: false,
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

    /// The key budget bounds a plan, and the truncation is reported rather than
    /// silent (kunobi-ninja/kache#616).
    ///
    /// The budget applies AFTER the already-local / already-in-flight filters,
    /// so it bounds work actually to be done. Three remote keys, budget of one:
    /// one is admitted and two are counted as dropped over budget.
    #[tokio::test]
    async fn test_prefetch_key_budget_truncates_and_reports() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        config.prefetch_max_keys = 1;
        // Keep the coordinator from racing the assertions: no download can
        // start, so nothing is removed from the plan for any other reason.
        config.s3_concurrency = 2;

        let keys = [
            "1111111111111111".repeat(4),
            "2222222222222222".repeat(4),
            "3333333333333333".repeat(4),
        ];
        let client = test_remote_backend();
        for key in &keys {
            put_test_object(&client, &test_manifest_object_key(key, "serde"), b"{}").await;
            put_test_object(
                &client,
                &test_pack_object_key(key, "serde"),
                &build_entry_pack(key, "serde"),
            )
            .await;
        }

        let daemon = Arc::new(Daemon::new(config.clone()));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );
        let _gate = daemon
            .prefetch_gate
            .clone()
            .acquire_owned()
            .await
            .expect("gate permit");

        let resp = daemon
            .handle_prefetch(&PrefetchRequest {
                keys: keys
                    .iter()
                    .map(|k| (k.clone(), "serde".to_string()))
                    .collect(),
                warm_all: false,
            })
            .await;
        assert!(resp.ok, "prefetch dispatch should be ok: {resp:?}");

        assert_eq!(
            daemon
                .prefetch_stats
                .keys_over_budget
                .load(Ordering::Relaxed),
            2,
            "two of three candidates should be reported as dropped over budget"
        );
    }

    /// The key budget arithmetic, including the `0 = unlimited` sentinel (#616).
    #[test]
    fn test_prefetch_key_budget_overflow() {
        assert_eq!(prefetch_key_budget_overflow(10, 4), 6);
        assert_eq!(prefetch_key_budget_overflow(4, 4), 0, "exactly at budget");
        assert_eq!(prefetch_key_budget_overflow(3, 4), 0, "under budget");
        assert_eq!(prefetch_key_budget_overflow(0, 4), 0, "empty plan");
        assert_eq!(
            prefetch_key_budget_overflow(10_000, 0),
            0,
            "0 disables the key budget"
        );
    }

    /// The byte budget predicate, including the `0 = unlimited` sentinel (#616).
    #[test]
    fn test_prefetch_byte_budget_exhausted() {
        assert!(!prefetch_byte_budget_exhausted(1024, 0));
        assert!(!prefetch_byte_budget_exhausted(1024, 1023));
        assert!(
            prefetch_byte_budget_exhausted(1024, 1024),
            "a budget exactly met stops the next download"
        );
        assert!(prefetch_byte_budget_exhausted(1024, 4096), "overshot");
        assert!(
            !prefetch_byte_budget_exhausted(0, u64::MAX),
            "0 disables the byte budget"
        );
    }

    /// `prefetch_deadline_secs = 0` disables the deadline rather than dropping
    /// the plan immediately (#616).
    #[tokio::test]
    async fn test_prefetch_deadline_stops_the_plan() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        // The coordinator checks the deadline before starting each candidate,
        // so a zero-length budget drops the whole plan on the first iteration.
        config.prefetch_deadline_secs = 0;

        let key = "4444444444444444".repeat(4);
        let client = test_remote_backend();
        put_test_object(&client, &test_manifest_object_key(&key, "serde"), b"{}").await;
        put_test_object(
            &client,
            &test_pack_object_key(&key, "serde"),
            &build_entry_pack(&key, "serde"),
        )
        .await;

        let daemon = Arc::new(Daemon::new(config.clone()));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let resp = daemon
            .handle_prefetch(&PrefetchRequest {
                keys: vec![(key.clone(), "serde".to_string())],
                warm_all: false,
            })
            .await;
        assert!(resp.ok, "prefetch dispatch should be ok: {resp:?}");

        // `0` means "no deadline", so the plan must still run.
        let entry_meta = config.store_dir().join(&key).join("meta.json");
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
            "prefetch_deadline_secs = 0 disables the deadline rather than dropping the plan"
        );
    }

    /// An empty candidate list must mean "nothing to prefetch", never
    /// "download the whole bucket" (kunobi-ninja/kache#615).
    ///
    /// The remote here holds an entry that is missing locally, so the old
    /// empty-list sentinel would have LISTed the bucket and queued it.
    #[tokio::test]
    async fn test_empty_prefetch_request_does_not_warm_the_bucket() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());

        let key = "cccccccccccccccc".repeat(4);
        let client = test_remote_backend();
        // Both objects, so the key IS discoverable by listing — otherwise this
        // test would pass even with the old empty-list-means-everything path.
        put_test_object(&client, &test_manifest_object_key(&key, "serde"), b"{}").await;
        put_test_object(
            &client,
            &test_pack_object_key(&key, "serde"),
            &build_entry_pack(&key, "serde"),
        )
        .await;

        let daemon = Arc::new(Daemon::new(config.clone()));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let resp = daemon
            .handle_prefetch(&PrefetchRequest {
                keys: Vec::new(),
                warm_all: false,
            })
            .await;
        assert!(
            resp.ok,
            "an empty request is a no-op, not an error: {resp:?}"
        );

        // Nothing may be claimed, queued, or imported.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            daemon.downloading.read().await.is_empty(),
            "an empty prefetch request must not claim any key"
        );
        assert!(
            !config.store_dir().join(&key).join("meta.json").exists(),
            "an empty prefetch request must not download anything"
        );
        assert_eq!(
            daemon
                .transfer_counters
                .downloads_completed
                .load(Ordering::Relaxed),
            0
        );
    }

    /// Whole-remote warming still works, but only when asked for (#615).
    #[tokio::test]
    async fn test_warm_all_prefetch_request_downloads_missing_keys() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());

        let key = "dddddddddddddddd".repeat(4);
        let client = test_remote_backend();
        // `list_keys` discovers keys from the manifest objects, not the packs.
        put_test_object(&client, &test_manifest_object_key(&key, "serde"), b"{}").await;
        put_test_object(
            &client,
            &test_pack_object_key(&key, "serde"),
            &build_entry_pack(&key, "serde"),
        )
        .await;

        let daemon = Arc::new(Daemon::new(config.clone()));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let resp = daemon
            .handle_prefetch(&PrefetchRequest {
                keys: Vec::new(),
                warm_all: true,
            })
            .await;
        assert!(resp.ok, "warm_all dispatch should be ok: {resp:?}");

        let entry_meta = config.store_dir().join(&key).join("meta.json");
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
            "warm_all should discover the key by listing and import it"
        );
    }

    /// A demanded key must never queue behind speculation
    /// (kunobi-ninja/kache#613).
    ///
    /// Candidates used to be claimed in `downloading` the moment the plan was
    /// installed, so a `RemoteCheck` for a candidate the coordinator had not
    /// reached yet parked on its `Notify` for up to `DOWNLOAD_JOIN_BUDGET`
    /// (30s) waiting for a leader that did not exist — while the S3 permits
    /// the prefetch cap reserves for demand sat idle.
    ///
    /// The test pins the coordinator: `s3_concurrency = 2` makes the prefetch
    /// gate a single permit, and holding that permit stalls every prefetch
    /// download before it starts. The demanded key is the one the coordinator
    /// has NOT reached.
    #[tokio::test]
    async fn test_demand_does_not_wait_behind_unstarted_prefetch_candidates() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        // Prefetch gate = prefetch_concurrency_cap(2) = 1 permit.
        config.s3_concurrency = 2;

        let stalled_key = "aaaaaaaaaaaaaaaa".repeat(4);
        let demanded_key = "bbbbbbbbbbbbbbbb".repeat(4);

        let client = test_remote_backend();
        for key in [&stalled_key, &demanded_key] {
            // The manifest object is what the demand path's HEAD probe looks for.
            put_test_object(&client, &test_manifest_object_key(key, "serde"), b"{}").await;
            put_test_object(
                &client,
                &test_pack_object_key(key, "serde"),
                &build_entry_pack(key, "serde"),
            )
            .await;
        }

        let daemon = Arc::new(Daemon::new(config.clone()));
        assert!(
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );
        // Skip the startup warming barrier: this test is about the dedup map,
        // not about racing manifest prefetch.
        daemon.signal_warming_complete();

        // Take the only prefetch gate permit, so no prefetch download can
        // begin. The coordinator parks its first task on the gate and never
        // reaches the second candidate.
        let _gate = daemon
            .prefetch_gate
            .clone()
            .acquire_owned()
            .await
            .expect("gate permit");

        let resp = daemon
            .handle_prefetch(&PrefetchRequest {
                keys: vec![
                    (stalled_key.clone(), "serde".to_string()),
                    (demanded_key.clone(), "serde".to_string()),
                ],
                warm_all: false,
            })
            .await;
        assert!(resp.ok, "prefetch dispatch should be ok: {resp:?}");

        // Give the coordinator time to spawn and park on the gate.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // A candidate that has not started downloading holds no claim, so
        // nothing can park on it.
        assert!(
            !daemon.downloading.read().await.contains_key(&demanded_key),
            "an unstarted prefetch candidate must not be claimed in `downloading`"
        );

        // The demanded key must be served now, out of the reserved permits,
        // rather than waiting for the stalled plan to drain. Before the fix
        // this parked for the full 30s join budget and blew the timeout.
        let resp = tokio::time::timeout(
            Duration::from_secs(5),
            daemon.handle_remote_check(&RemoteCheckRequest {
                key: demanded_key.clone(),
                entry_dir: config
                    .store_dir()
                    .join(&demanded_key)
                    .to_string_lossy()
                    .into_owned(),
                crate_name: "serde".into(),
                deadline_ms: None,
            }),
        )
        .await
        .expect("demand must not block behind an unstarted prefetch candidate");

        assert!(resp.ok, "demand download should succeed: {resp:?}");
        assert_eq!(
            resp.found,
            Some(true),
            "the demanded entry should have been downloaded"
        );
    }

    // ── Upload queue tests ────────────────────────────────────────

    #[tokio::test]
    async fn test_handle_upload_with_queue_returns_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(crate::config::RemoteConfig::test_s3("test", "artifacts"));

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<UploadJob>();
        let daemon = Daemon::new(config);
        daemon.set_upload_tx(tx);

        let job = UploadJob {
            key: test_cache_key("queued-upload"),
            entry_dir: "/tmp/test".into(),
            crate_name: "serde".into(),
            client_epoch: 0,
        };
        seed_store_entry(&daemon.config, &job.key, "serde", dir.path());

        // Should return ok immediately (queued, not executed)
        let resp = daemon.handle_upload(&job).await;
        assert!(resp.ok);
        assert!(resp.error.is_none());
        assert!(
            upload_spool_path(&daemon.config, &job.key).is_file(),
            "queue acknowledgement must follow durable persistence"
        );
    }

    #[tokio::test]
    async fn test_handle_upload_queue_closed() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(crate::config::RemoteConfig::test_s3("test", "artifacts"));

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<UploadJob>();
        let daemon = Daemon::new(config);
        daemon.set_upload_tx(tx);

        // Drop receiver to close the channel
        drop(rx);

        let job = UploadJob {
            key: test_cache_key("closed-upload-queue"),
            entry_dir: "/tmp/test".into(),
            crate_name: "serde".into(),
            client_epoch: 0,
        };
        seed_store_entry(&daemon.config, &job.key, "serde", dir.path());
        let resp = daemon.handle_upload(&job).await;
        assert!(!resp.ok);
        assert!(resp.error.as_deref().unwrap().contains("queue closed"));
    }

    #[tokio::test]
    async fn test_handle_upload_dedup() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(crate::config::RemoteConfig::test_s3("test", "artifacts"));

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<UploadJob>();
        let daemon = Daemon::new(config);
        daemon.set_upload_tx(tx);

        let job = UploadJob {
            key: test_cache_key("deduplicated-upload"),
            entry_dir: "/tmp/test".into(),
            crate_name: "serde".into(),
            client_epoch: 0,
        };
        seed_store_entry(&daemon.config, &job.key, "serde", dir.path());

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
        config.remote = Some(crate::config::RemoteConfig::test_s3("test", "artifacts"));

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<UploadJob>();
        let daemon = Daemon::new(config);
        daemon.set_upload_tx(tx);
        daemon.close_upload_queue();

        let job = UploadJob {
            key: test_cache_key("late-upload"),
            entry_dir: "/tmp/test".into(),
            crate_name: "serde".into(),
            client_epoch: 0,
        };
        seed_store_entry(&daemon.config, &job.key, "serde", dir.path());
        let resp = daemon.handle_upload(&job).await;
        assert!(!resp.ok);
        assert!(resp.error.as_deref().unwrap().contains("queue closed"));
    }

    #[test]
    fn upload_spool_directory_sync_follows_creation() {
        let dir = tempfile::tempdir().unwrap();
        let spool = dir.path().join("upload-queue");
        let steps = std::cell::RefCell::new(Vec::new());

        ensure_upload_spool_dir_with(
            &spool,
            |path| {
                steps.borrow_mut().push("create");
                std::fs::create_dir_all(path)
            },
            |parent| {
                assert_eq!(parent, dir.path());
                assert!(spool.is_dir(), "parent sync must follow directory creation");
                steps.borrow_mut().push("sync-parent");
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(steps.borrow().as_slice(), &["create", "sync-parent"]);
    }

    #[test]
    fn upload_spool_directory_sync_failure_is_propagated() {
        let dir = tempfile::tempdir().unwrap();
        let spool = dir.path().join("upload-queue");

        let error = ensure_upload_spool_dir_with(&spool, std::fs::create_dir_all, |_| {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "injected upload-spool parent fsync failure",
            ))
        })
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("injected upload-spool parent fsync failure"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn durable_upload_intent_replays_after_restart_and_normalizes_paths() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let key = test_cache_key("restart-upload");
        let job = UploadJob {
            key: key.clone(),
            entry_dir: "/untrusted/client/path".into(),
            crate_name: "serde".into(),
            client_epoch: 7,
        };
        seed_store_entry(&config, &key, "serde", dir.path());

        let persisted = persist_upload_job(&config, &job).unwrap();
        assert_eq!(
            Path::new(&persisted.entry_dir),
            config.store_dir().join(&key)
        );

        // Loading through a fresh config value models daemon restart: intent
        // state comes solely from the durable spool, never process memory.
        let restarted_config = config.clone();
        let replayed = load_upload_jobs(&restarted_config).unwrap();
        assert_eq!(replayed, vec![persisted]);

        remove_upload_job(&restarted_config, &key).unwrap();
        assert!(load_upload_jobs(&restarted_config).unwrap().is_empty());
    }

    #[test]
    fn duplicate_upload_intent_persistence_reuses_one_valid_create_only_winner() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let key = test_cache_key("double-persist-upload");
        seed_store_entry(&config, &key, "serde", dir.path());
        let first_job = UploadJob {
            key: key.clone(),
            entry_dir: "/wrapper/path".into(),
            crate_name: "serde".into(),
            client_epoch: 7,
        };
        let first = persist_upload_job(&config, &first_job).unwrap();
        let path = upload_spool_path(&config, &key);
        let first_bytes = fs::read(&path).unwrap();

        // Models the daemon persisting the wrapper's already-durable request.
        // Durable bytes keep the first winner, while the live return carries
        // the current caller epoch needed for stale-daemon replacement.
        let second = persist_upload_job(
            &config,
            &UploadJob {
                entry_dir: "/daemon/path".into(),
                client_epoch: 99,
                ..first_job
            },
        )
        .unwrap();
        assert_eq!(second.key, first.key);
        assert_eq!(second.entry_dir, first.entry_dir);
        assert_eq!(second.crate_name, first.crate_name);
        assert_eq!(second.client_epoch, 99);
        assert_eq!(fs::read(&path).unwrap(), first_bytes);
        assert_eq!(fs::read_dir(config.upload_spool_dir()).unwrap().count(), 1);
        assert_eq!(load_upload_jobs(&config).unwrap(), vec![first]);
    }

    #[test]
    fn first_upload_intent_requires_a_committed_local_payload() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let key = test_cache_key("missing-upload-payload");
        let error = persist_upload_job(
            &config,
            &UploadJob {
                key: key.clone(),
                entry_dir: "/missing".into(),
                crate_name: "serde".into(),
                client_epoch: 0,
            },
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("local cache entry missing"));
        assert!(!upload_spool_path(&config, &key).exists());
    }

    #[test]
    fn first_upload_intent_publication_serializes_with_gc_in_both_orders() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let key = test_cache_key("upload-gc-ordering");
        seed_store_entry(&config, &key, "serde", dir.path());
        let store = Store::open(&config).unwrap();
        store.set_last_accessed_for_test(&key, "-48 hours");
        let held_gc = store.acquire_gc_lock().unwrap();
        let job = UploadJob {
            key: key.clone(),
            entry_dir: "/ignored".into(),
            crate_name: "serde".into(),
            client_epoch: 0,
        };
        let path = upload_spool_path(&config, &key);
        let publisher_config = config.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let publisher = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            done_tx
                .send(persist_upload_job(&publisher_config, &job))
                .unwrap();
        });

        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("publisher started");
        match done_rx.recv_timeout(Duration::from_millis(100)) {
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            other => panic!("publisher must wait behind GC, got {other:?}"),
        }
        assert!(
            !path.exists(),
            "GC-first ordering must not publish outside gc.lock"
        );

        drop(held_gc);
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("publisher unblocked")
            .expect("publication succeeds after GC");
        publisher.join().unwrap();
        assert!(path.is_file());

        // Reverse order: once publication wins, a later GC snapshots the
        // intent and pins its deliberately stale payload.
        let _gc_after_publication = store.acquire_gc_lock().unwrap();
        let stats = store.evict_older_than(24).unwrap();
        assert_eq!(stats.entries_pinned, 1);
        assert!(store.contains(&key));
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
                warm_all: false,
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
            session_id: String::new(),
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
            session_id: String::new(),
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
                    session_id: String::new(),
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
        let key = "a".repeat(64);
        seed_store_entry(&config, &key, "serde", dir.path());
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
            send_upload_job(&cfg, &key, Path::new("/tmp/test"), "serde")
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
            session_id: String::new(),
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

    #[tokio::test]
    async fn test_handle_build_started_prefetch_disabled_is_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        config.prefetch_enabled = false;
        let daemon = Arc::new(Daemon::new(config));

        let resp = daemon
            .handle_build_started(&BuildStartedRequest {
                intent: kache_core::BuildIntent {
                    crate_names: vec!["serde".into(), "tokio".into()],
                    ..Default::default()
                },
                client_epoch: 0,
                session_id: "disabled-prefetch".into(),
            })
            .await;

        assert!(resp.ok);
        assert!(daemon.active_plan.lock().unwrap().is_none());
        assert_eq!(
            daemon.prefetch_stats.plans_advisory.load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            daemon.prefetch_stats.plans_fallback.load(Ordering::Relaxed),
            0
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
            session_id: String::new(),
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

    /// A waiter registered on a STALE Notify generation (its leader failed
    /// and another task re-claimed with a fresh Notify before this waiter
    /// re-checked the map) must adopt the current generation and then wake
    /// promptly when THAT leader finishes — not sit out the deadline parked
    /// on a Notify nobody will ever signal (#620 refactor guard; the arm the
    /// diff mutation gate found uncovered).
    #[tokio::test]
    async fn waiter_adopts_the_current_leader_generation() {
        let dir = tempfile::tempdir().unwrap();
        let entry_dir = dir.path().join("entry");
        std::fs::create_dir_all(&entry_dir).unwrap();

        let map: Arc<RwLock<HashMap<String, Arc<Notify>>>> = Arc::new(RwLock::new(HashMap::new()));
        assert!(claim_download(&map, "k").await.is_none());
        let leader_guard = DownloadingGuard::new(map.clone(), "k".to_string());

        // A Notify from a generation that no longer exists in the map.
        let stale = Arc::new(Notify::new());
        let waiter = tokio::spawn({
            let map = map.clone();
            let entry_dir = entry_dir.clone();
            async move {
                let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
                join_inflight_download(&map, "k", &entry_dir, stale, deadline).await
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await; // let the waiter adopt + park
        std::fs::write(entry_dir.join("meta.json"), "{}").unwrap();
        drop(leader_guard);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), waiter)
                .await
                .expect("an adopted leader's completion must wake the waiter promptly")
                .unwrap(),
            JoinOutcome::Found
        );
    }

    /// kunobi-ninja/kache#620: when the budget expires while a leader still
    /// holds the claim (a wedged download), the waiter must give up as a miss
    /// — never proceed as a second, unclaimed writer racing the leader's
    /// destructive extraction. The wedged leader's claim stays in place.
    #[tokio::test]
    async fn waiter_gives_up_as_miss_when_leader_holds_claim_past_budget() {
        let dir = tempfile::tempdir().unwrap();
        let entry_dir = dir.path().join("entry"); // no meta.json ever appears

        let map: Arc<RwLock<HashMap<String, Arc<Notify>>>> = Arc::new(RwLock::new(HashMap::new()));
        assert!(
            claim_download(&map, "k").await.is_none(),
            "first claim is the (wedged) leader"
        );
        // The leader never drops a guard: its download is wedged.
        let notify = claim_download(&map, "k").await.expect("waiter");

        let start = Instant::now();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
        let outcome = join_inflight_download(&map, "k", &entry_dir, notify, deadline).await;
        assert_eq!(outcome, JoinOutcome::GaveUp);
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "give-up must be prompt once the budget expires"
        );
        assert!(
            map.read().await.contains_key("k"),
            "the wedged leader's claim must remain in place — the waiter took nothing over"
        );
    }

    /// The extracted join loop still elects a new leader when the old one
    /// fails, and reports Found when the old one lands the entry (#620
    /// refactor guard).
    #[tokio::test]
    async fn join_inflight_download_reclaims_on_failure_and_finds_on_success() {
        let dir = tempfile::tempdir().unwrap();
        let entry_dir = dir.path().join("entry");
        std::fs::create_dir_all(&entry_dir).unwrap();

        // Failure path: leader's guard drops without meta.json → Reclaimed.
        let map: Arc<RwLock<HashMap<String, Arc<Notify>>>> = Arc::new(RwLock::new(HashMap::new()));
        assert!(claim_download(&map, "k").await.is_none());
        let leader_guard = DownloadingGuard::new(map.clone(), "k".to_string());
        let notify = claim_download(&map, "k").await.unwrap();
        let waiter = tokio::spawn({
            let map = map.clone();
            let entry_dir = entry_dir.clone();
            async move {
                let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
                join_inflight_download(&map, "k", &entry_dir, notify, deadline).await
            }
        });
        tokio::time::sleep(Duration::from_millis(50)).await; // let the waiter park
        drop(leader_guard);
        // Promptness is part of the contract: pre-#620 the loop held the map's
        // read guard across the Notify await, so waiters only proceeded at
        // deadline (10s here) instead of at the leader's guard drop.
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), waiter)
                .await
                .expect("failed leader must wake the waiter promptly")
                .unwrap(),
            JoinOutcome::Reclaimed
        );
        assert!(
            map.read().await.contains_key("k"),
            "Reclaimed means the waiter now holds the claim"
        );
        map.write().await.clear();

        // Success path: leader writes meta.json before releasing → Found.
        assert!(claim_download(&map, "k").await.is_none());
        let leader_guard = DownloadingGuard::new(map.clone(), "k".to_string());
        let notify = claim_download(&map, "k").await.unwrap();
        let waiter = tokio::spawn({
            let map = map.clone();
            let entry_dir = entry_dir.clone();
            async move {
                let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
                join_inflight_download(&map, "k", &entry_dir, notify, deadline).await
            }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        std::fs::write(entry_dir.join("meta.json"), "{}").unwrap();
        drop(leader_guard);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), waiter)
                .await
                .expect("successful leader must wake the waiter promptly")
                .unwrap(),
            JoinOutcome::Found
        );
        assert!(map.read().await.is_empty(), "claim fully released");
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
