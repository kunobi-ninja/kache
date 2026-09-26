use crate::transport::prelude::*;
use crate::transport::{TokioListener, TokioStream, socket_name};
use anyhow::{Context, Result};
use kache_core::timeline::{
    PackedEntryTransfer, PrefetchAccounting, PrefetchOperation, PrefetchOrigin,
};
use kache_core::{PrefetchDisposition, PrefetchPlan};
use kunobi_daemon::Lifecycle;
#[path = "daemon_lifecycle.rs"]
mod lifecycle_client;
#[path = "daemon_control.rs"]
mod lifecycle_control;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{Notify, RwLock};

use crate::cache_remote::V3Prefetch;
use crate::config::{Config, UPLOAD_SPOOL_MAX_JOBS};
use crate::events;
use crate::remote_resilience::{
    BreakerPermit, KeyedSingleflight, NegativeKeyCache, RemoteBreaker, RemoteDeadline,
    RemoteErrorClass, RemoteOperation, SingleflightClaim, classify_remote_error,
};
use crate::store::Store;

#[derive(Debug)]
pub(crate) enum SpeculativeManifestOutcome {
    Completed(Option<crate::remote::BuildManifest>),
    NotAdmitted,
}

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
const UPLOAD_SPOOL_MAX_BYTES: u64 = 65_536;
const UPLOAD_RETRY_DELAY: Duration = Duration::from_secs(5);

fn remote_check_budget_ms(configured_secs: u64, client_ms: Option<u64>) -> NonZeroU64 {
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
    NonZeroU64::new(configured_ms.min(client_ms))
        .expect("the synchronous remote-check budget is always positive")
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

/// Read timeout for a stats round trip. Generous: a busy daemon may be holding
/// the index lock when the request lands.
const STATS_READ_TIMEOUT: Duration = Duration::from_secs(5);
/// Read timeout for the stats refetch after a stale daemon was replaced. The
/// replacement has just bound its socket and has nothing queued, so a slow
/// answer here means something is wrong rather than busy.
const STATS_REFETCH_TIMEOUT: Duration = Duration::from_secs(3);
const DAEMON_COORD_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);

/// How often the daemon re-checks its config file for changes. On a change it
/// schedules a graceful restart so the new config (e.g. `local_max_size`) takes
/// effect — no manual `kache daemon stop`. Cheap (one small-file read); rare to
/// fire, so a coarse interval is fine.
const DAEMON_CONFIG_WATCH_INTERVAL: Duration = Duration::from_secs(15);
const DAEMON_COORD_STALE_AFTER: Duration = Duration::from_secs(15);
const VERSION: &str = crate::VERSION;
const FILE_HASH_MEMORY_CACHE_CAP: usize = 4096;
/// Age a blob file with no `blobs` row must reach before a daemon sweep
/// unlinks it. A put renames its blobs into place before it inserts their
/// rows, and an hour outlasts any put still in flight.
pub(crate) const ORPHAN_BLOB_GRACE: Duration = Duration::from_secs(3600);

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    control_version: Option<u32>,
}

#[derive(Debug, Clone)]
struct DaemonCoordFile {
    control_version: Option<u32>,
    path: PathBuf,
    pid: u32,
    build_epoch: u64,
}

impl DaemonCoordFile {
    fn for_socket(socket_path: &Path) -> Self {
        Self {
            path: daemon_state_path(socket_path),
            control_version: None,
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
            control_version: self.control_version,
        };
        write_json_atomically(&self.path, &state)
    }
}

struct DaemonCoordGuard {
    path: PathBuf,
}

/// Platform adapter for shared, inode-checked socket cleanup.
struct SocketCleanupGuard {
    #[cfg(unix)]
    _guard: kunobi_daemon::local::unix_socket::SocketGuard,
}
impl SocketCleanupGuard {
    fn new(path: &Path) -> std::io::Result<Self> {
        #[cfg(unix)]
        {
            Ok(Self {
                _guard: kunobi_daemon::local::unix_socket::SocketGuard::capture(path)?,
            })
        }
        #[cfg(windows)]
        {
            let _ = path;
            Ok(Self {})
        }
    }
}

impl DaemonCoordGuard {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for DaemonCoordGuard {
    fn drop(&mut self) {
        if let Ok(bytes) = std::fs::read(&self.path)
            && serde_json::from_slice::<DaemonCoordState>(&bytes).is_ok_and(|state| {
                state.pid == std::process::id() && state.build_epoch == build_epoch()
            })
        {
            let _ = kunobi_daemon::RecordSlot::new(&self.path).remove_if_matches(&bytes);
        }
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
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    kunobi_daemon::RecordSlot::new(path).replace(&serde_json::to_vec(value)?)?;
    Ok(())
}

fn read_daemon_state(socket_path: &Path) -> Option<DaemonCoordState> {
    let path = daemon_state_path(socket_path);
    let bytes = std::fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Build epoch of a daemon that holds the run lock but has not bound its socket
/// yet, if one is coming up right now.
///
/// Read-only and instant: it inspects the coordinator state file rather than the
/// socket, which is what makes it usable from `doctor` during the window where a
/// stats request would only report "not reachable" (kunobi-ninja/kache#720).
///
/// A coordinator file outlives an unclean exit, so its mere existence proves
/// nothing, and neither does its PID: PIDs are recycled, and a fresh record whose
/// PID has been reused by an unrelated process would otherwise read as a live
/// starter. The run lock is what actually distinguishes them — a daemon takes it
/// before writing `Starting` and holds it for its whole life, so only a real
/// starter can be holding it. The lock is probed but never created here: `doctor`
/// reports on lock files, and a diagnostic that manufactures one would then flag
/// its own leftovers.
pub fn starting_daemon_epoch(config: &Config) -> Option<u64> {
    let socket_path = config.socket_path();
    let state = read_daemon_state(&socket_path)?;
    if state.phase != DaemonPhase::Starting
        || !daemon_state_is_recent(&state)
        || !process_is_alive(state.pid)
    {
        return None;
    }
    existing_daemon_run_lock_is_held(&socket_path)
        .ok()?
        .then_some(state.build_epoch)
}

fn daemon_state_is_recent(state: &DaemonCoordState) -> bool {
    // A timestamp in the future is not a fresh heartbeat, it is a clock that
    // moved: saturating to an age of zero would read a long-dead record as live
    // until the wall clock caught back up.
    now_millis()
        .checked_sub(state.updated_at_ms)
        .is_some_and(|age_ms| age_ms <= DAEMON_COORD_STALE_AFTER.as_millis() as u64)
}

/// Whether `client_epoch` is a strictly newer build than `daemon_epoch`. A zero
/// on either side means "unknown", never "older".
pub(crate) fn client_epoch_is_newer(client_epoch: u64, daemon_epoch: u64) -> bool {
    client_epoch > 0 && daemon_epoch > 0 && client_epoch > daemon_epoch
}

/// Whether `pid` may still be running (see [`alive_from_state`]).
fn process_is_alive(pid: u32) -> bool {
    alive_from_state(pid, || kunobi_daemon::local::process_state(pid))
}

/// Whether a process the OS reports as `state` may still be running. A state
/// the OS could not establish (access denied on Windows, say) counts as
/// running: both callers wait or report rather than act on it, and waiting
/// is the safe error. PIDs 0 and 1 and values past `i32::MAX` never name a
/// daemon (the first is the kernel's, the second init's, the last read as
/// broadcasts), so a corrupt record naming one is not waited on.
fn alive_from_state(pid: u32, state: impl FnOnce() -> kunobi_daemon::local::ProcessState) -> bool {
    use kunobi_daemon::local::ProcessState;
    if pid <= 1 || i32::try_from(pid).is_err() {
        return false;
    }
    !matches!(state(), ProcessState::Exited)
}

fn wait_for_run_lock_release(socket_path: &Path, timeout: Duration) -> Result<bool> {
    Ok(
        kunobi_daemon::readiness::wait_until(Instant::now() + timeout, |_| {
            daemon_run_lock_is_held(socket_path).map(|held| (!held).then_some(()))
        })?
        .is_some(),
    )
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
    /// A wrapper saw the store over the automatic trigger. The daemon
    /// acknowledges at once and sweeps in the background; hints that arrive
    /// meanwhile coalesce. Older daemons reject the unknown variant, and the
    /// wrapper then spawns its own worker.
    GcHint,
    RemoteCheck(RemoteCheckRequest),
    Stats(StatsRequest),
    Health,
    BatchRemoteCheck(BatchRemoteCheckRequest),
    HashFiles(HashFilesRequest),
    Prefetch(PrefetchRequest),
    BuildStarted(BuildStartedRequest),
    CompileStarted(CompileStartedRequest),
    CompileFinished(CompileFinishedRequest),
    /// A wrapper hands the daemon a finished cc compile to store.
    /// Older handlers lack the atomic receipt; reject their protocol instead
    /// of allowing an ambiguous timeout to publish and log twice.
    #[serde(rename = "publish_cc_v2")]
    PublishCc(Box<crate::daemon_publish::PublishCcRequest>),
    /// A wrapper with no local input prediction row asks for the remote's
    /// (kunobi-ninja/kache#1011). Older daemons reject the unknown variant,
    /// which the wrapper reads as no row.
    PredictionFetch(PredictionFetchRequest),
    /// A wrapper recorded a portable row; the daemon stores it on a writable
    /// remote in the background.
    PredictionPublish(PredictionPublishRequest),
    Shutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PredictionFetchRequest {
    pub identity: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PredictionPublishRequest {
    pub identity: String,
    pub row: crate::prediction_share::SharedPrediction,
}

impl Request {
    /// Whether the request comes from a build. Stats pollers, a TUI and GC
    /// commands do not keep the machine from counting as quiet.
    fn is_build_activity(&self) -> bool {
        !matches!(
            self,
            Request::Health
                | Request::Stats(_)
                | Request::Gc(_)
                | Request::GcV2(_)
                | Request::Shutdown
        )
    }

    /// The client binary's build epoch, for the requests that carry one. A
    /// client newer than the daemon makes it schedule a restart.
    fn client_epoch(&self) -> u64 {
        match self {
            Request::Upload(job) => job.client_epoch,
            Request::Stats(req) => req.client_epoch,
            Request::BuildStarted(req) => req.client_epoch,
            Request::PublishCc(req) => req.client_epoch,
            _ => 0,
        }
    }
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

/// How long the daemon spends fetching one prediction row. A row saves one
/// pre-pass, so waiting longer than a pre-pass takes would be a loss.
const PREDICTION_FETCH_BUDGET: Duration = Duration::from_secs(2);

/// How long a wrapper waits for a prediction row: the daemon's own budget
/// plus the socket round trip.
fn prediction_fetch_wait() -> Duration {
    PREDICTION_FETCH_BUDGET + Duration::from_millis(500)
}

/// Rows go to the remote only when there is one and it is writable: the gate
/// artifact uploads use.
fn publishes_predictions(config: &Config) -> bool {
    config.remote.is_some() && !config.remote_readonly
}

/// Longest identity accepted over the socket: a prefix and a 64-hex hash.
const PREDICTION_IDENTITY_MAX_LEN: usize = 128;

/// Is `identity` one a shared row may answer, and short enough to be one?
fn prediction_identity_is_acceptable(identity: &str) -> bool {
    identity.len() <= PREDICTION_IDENTITY_MAX_LEN
        && (crate::cache_key::is_portable_identity(identity)
            || crate::cache_key::is_shared_target_identity(identity))
}

fn upload_spool_path(config: &Config, key: &str) -> PathBuf {
    config.upload_spool_dir().join(format!("{key}.json"))
}

fn upload_spool_error_is_not_found(error: &std::io::Error) -> bool {
    matches!(error.kind(), std::io::ErrorKind::NotFound)
}

fn upload_spool_error_is_already_exists(error: &std::io::Error) -> bool {
    matches!(error.kind(), std::io::ErrorKind::AlreadyExists)
}

fn upload_intent_size_is_valid(size: u64) -> bool {
    size <= UPLOAD_SPOOL_MAX_BYTES
}

fn upload_spool_has_capacity(existing_count: usize) -> bool {
    existing_count < UPLOAD_SPOOL_MAX_JOBS
}

fn count_upload_spool_entries<I, T>(entries: I) -> Result<usize>
where
    I: IntoIterator<Item = std::io::Result<T>>,
{
    let mut count = 0usize;
    for entry in entries.into_iter().take(UPLOAD_SPOOL_MAX_JOBS) {
        entry.context("reading upload spool entry")?;
        count = count.saturating_add(1);
    }
    Ok(count)
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
        Err(error) => {
            if upload_spool_error_is_not_found(&error) {
                return Ok(None);
            }
            return Err(error).with_context(|| format!("reading {}", path.display()));
        }
    };
    if !metadata.file_type().is_file() {
        anyhow::bail!("upload intent is not a regular file: {}", path.display());
    }
    if !upload_intent_size_is_valid(metadata.len()) {
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
        Err(error) => {
            if upload_spool_error_is_already_exists(&error.error) {
                drop(error.file);
                // The winner's bytes were flushed before its exclusive publish;
                // flushing the directory here also makes a concurrent winner's
                // directory entry durable before we reuse it.
                crate::atomic::fsync_dir(parent).context("flushing upload intent directory")?;
                Ok(false)
            } else {
                Err(error.error).context("publishing upload intent")
            }
        }
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
    ensure_upload_spool_dir_with(
        &dir,
        |path| std::fs::create_dir_all(path),
        crate::atomic::fsync_dir,
    )?;
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

    let entries = std::fs::read_dir(&dir)
        .with_context(|| format!("reading upload spool {}", dir.display()))?;
    let existing_count = count_upload_spool_entries(entries)
        .with_context(|| format!("reading upload spool {}", dir.display()))?;
    if !upload_spool_has_capacity(existing_count) {
        anyhow::bail!("upload spool is full ({UPLOAD_SPOOL_MAX_JOBS} jobs)");
    }
    let bytes = serde_json::to_vec(&normalized).context("serializing upload intent")?;
    if !upload_intent_size_is_valid(bytes.len() as u64) {
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
        Err(error) => {
            if upload_spool_error_is_not_found(&error) {
                Ok(())
            } else {
                Err(error).with_context(|| format!("removing {}", path.display()))
            }
        }
    }
}

fn load_upload_jobs(config: &Config) -> Result<Vec<UploadJob>> {
    let dir = config.upload_spool_dir();
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) => {
            if upload_spool_error_is_not_found(&error) {
                return Ok(Vec::new());
            }
            return Err(error).with_context(|| format!("reading {}", dir.display()));
        }
    };
    let mut jobs = Vec::new();
    for entry in entries.take(UPLOAD_SPOOL_MAX_JOBS) {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        if !upload_intent_size_is_valid(entry.metadata()?.len()) {
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
        if job.key != key {
            tracing::warn!(path = %entry.path().display(), "ignoring invalid upload intent");
            continue;
        }
        if !crate::cache_key::is_valid_crate_name(&job.crate_name) {
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

/// Who started a daemon sweep. It decides only the size pass: a requested
/// `kache gc` always runs it, the timer asks the shared trigger and backoff
/// like every other automatic driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GcDriver {
    Requested,
    Periodic,
}

impl GcDriver {
    /// A sweep the user asked for may evict fresh imports; the timer keeps
    /// them (#1008).
    fn sweep_origin(self) -> crate::store::SweepOrigin {
        match self {
            GcDriver::Requested => crate::store::SweepOrigin::Requested,
            GcDriver::Periodic => crate::store::SweepOrigin::Automatic,
        }
    }
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

/// A GC policy can select the same protected entry in several sweeps. The
/// wire format carries counts rather than keys, so an exact set union is not
/// available here. Report the largest single-sweep count as a non-duplicating
/// lower bound; explicit-age requests have no size sweep and use their sole
/// policy count directly.
fn gc_entries_pinned_lower_bound(
    policy: GcPolicy,
    duplicate: usize,
    age: usize,
    size: usize,
) -> usize {
    match policy {
        GcPolicy::ExplicitAge { .. } => age,
        GcPolicy::Automatic { .. } => duplicate.max(age).max(size),
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
    /// Volume-shard cache dir to import into. Missing or empty keeps the
    /// main store so older clients stay on the historical path; a value
    /// must equal the main cache dir or a configured `[cache.volumes]`
    /// shard. Unknown paths are rejected rather than writing off-tree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_dir: Option<String>,
}

/// Cache dir a RemoteCheck should import into.
///
/// `None` / blank keeps the main store. A path is admitted only when it is
/// the main cache dir or a configured volume shard.
fn remote_check_cache_dir<'a>(
    main_cache_dir: &'a Path,
    volume_stores: &'a [crate::config::VolumeStore],
    shard_dir: Option<&str>,
) -> Result<&'a Path, &'static str> {
    let Some(raw) = shard_dir.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(main_cache_dir);
    };
    let requested = Path::new(raw);
    if requested == main_cache_dir {
        return Ok(main_cache_dir);
    }
    for shard in volume_stores {
        if requested == shard.store.as_path() {
            return Ok(shard.store.as_path());
        }
    }
    Err("remote-check shard_dir is not a configured volume store")
}

fn remote_check_entry_dir(cache_dir: &Path, key: &str) -> PathBuf {
    cache_dir.join("store").join(key)
}

fn remote_check_blobs_dir(cache_dir: &Path) -> PathBuf {
    cache_dir.join("store").join("blobs")
}

fn remote_check_uses_main_store(cache_dir: &Path, main_cache_dir: &Path) -> bool {
    cache_dir == main_cache_dir
}

/// `Some` only when the wrapper opened a volume shard rather than the main
/// store. Older daemons ignore the field.
pub(crate) fn remote_check_shard_dir_arg(
    main_cache_dir: &Path,
    store_cache_dir: &Path,
) -> Option<String> {
    if remote_check_uses_main_store(store_cache_dir, main_cache_dir) {
        None
    } else {
        Some(store_cache_dir.to_string_lossy().into_owned())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StatsRequest {
    pub include_entries: bool,
    /// Include the bounded recent session-summary tail. False for TUI/health
    /// polling so they do not rescan the append-only summary log every tick.
    #[serde(default)]
    pub include_summaries: bool,
    pub sort_by: Option<String>,
    /// Event window in whole hours. Older clients send only this; newer ones
    /// send it rounded up beside `event_secs` so an older daemon still
    /// answers with a superset of the requested window.
    pub event_hours: Option<u64>,
    /// Event window in seconds (kunobi-ninja/kache#897). Wins over
    /// `event_hours` when present, so `--since 15m` is a 15 minute window.
    #[serde(default)]
    pub event_secs: Option<u64>,
    /// Client binary mtime — lets the daemon detect when it's running stale code.
    #[serde(default)]
    pub client_epoch: u64,
}

impl StatsRequest {
    /// The event window this request asks for: `event_secs` from a current
    /// client, else `event_hours` from an older one, else the 24h default.
    pub(crate) fn window(&self) -> crate::since::SinceWindow {
        use crate::since::SinceWindow;
        match (self.event_secs, self.event_hours) {
            (Some(secs), _) => SinceWindow::from_secs(secs),
            (None, Some(hours)) => SinceWindow::from_hours(hours).unwrap_or(SinceWindow::DEFAULT),
            (None, None) => SinceWindow::DEFAULT,
        }
    }
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
    /// Set inside the daemon from the chosen plan, never accepted over IPC.
    #[serde(skip)]
    pub origin: Option<PrefetchOrigin>,
    #[serde(skip)]
    pub candidate_sources: HashMap<String, kache_core::CandidateSource>,
}

impl PrefetchRequest {
    pub fn from_plan(plan: PrefetchPlan) -> Self {
        let mut candidate_sources = HashMap::new();
        let keys = plan
            .candidates
            .into_iter()
            // The planner is an untrusted boundary (a distinct endpoint
            // from S3). cache_key/crate_name flow into local path joins and
            // S3 object keys, so drop any candidate that isn't a well-formed
            // key + safe crate name before it can become a traversal /
            // prefix-escape primitive. Reject, don't sanitize.
            .filter_map(|candidate| {
                if !crate::cache_key::is_valid_cache_key(&candidate.cache_key)
                    || !crate::cache_key::is_valid_crate_name(&candidate.crate_name)
                {
                    tracing::warn!(
                        cache_key = key_prefix(&candidate.cache_key),
                        cache_key_len = candidate.cache_key.len(),
                        "prefetch: dropping planner candidate with invalid cache_key/crate_name"
                    );
                    return None;
                }
                candidate_sources
                    .entry(candidate.cache_key.clone())
                    .or_insert(candidate.source);
                Some((candidate.cache_key, candidate.crate_name))
            })
            .collect();
        Self {
            warm_all: false,
            origin: None,
            candidate_sources,
            keys,
        }
    }
}

#[derive(Debug, Clone)]
struct PackPrefetchContext {
    manifest_key: String,
    namespace: String,
    shard_hashes: Vec<String>,
    selector: String,
}

impl PackPrefetchContext {
    fn from_deps(manifest_key: String, namespace: &str, deps: &[(String, String)]) -> Result<Self> {
        if deps.is_empty() {
            anyhow::bail!("packed-prefetch requires Cargo.lock dependencies");
        }
        let mut shard_hashes = crate::shards::compute_shards(namespace, deps)
            .shards
            .into_iter()
            .map(|(hash, _)| hash)
            .collect::<Vec<_>>();
        shard_hashes.sort();
        let selector = crate::remote_pack::selector_hash(
            &manifest_key,
            namespace,
            &shard_hashes,
            crate::cache_key::CACHE_KEY_VERSION,
        )?;
        Ok(Self {
            manifest_key,
            namespace: namespace.to_string(),
            shard_hashes,
            selector,
        })
    }

    fn from_intent(intent: &kache_core::BuildIntent) -> Option<Self> {
        let namespace = intent.namespace.as_deref()?;
        let manifest_key = crate::identity::manifest_lookup_keys(intent.identity_key.as_deref())
            .into_iter()
            .next()
            .unwrap_or_else(crate::identity::host_target_triple);
        Self::from_deps(manifest_key, namespace, &intent.cargo_lock_deps).ok()
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DaemonHealth {
    pub version: String,
    pub build_epoch: u64,
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
    /// The job/process-lifetime state directory the daemon resolved.
    /// Empty when reported by a daemon that predates runtime-dir support.
    #[serde(default)]
    pub runtime_dir: String,
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
            runtime_dir: config.runtime_dir.display().to_string(),
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
    /// adaptive hit-rate cancellation or daemon shutdown.
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
    /// Remote operations used by packed-prefetch discovery and pack GETs.
    #[serde(default)]
    pub pack_requests_total: u64,
    #[serde(default)]
    pub pack_bytes_downloaded: u64,
    /// Existing object-by-object v3 GETs started by speculative prefetch.
    #[serde(default)]
    pub v3_requests_total: u64,
    #[serde(default)]
    pub v3_bytes_downloaded: u64,
    #[serde(default)]
    pub pack_validation_failures: u64,
    #[serde(default)]
    pub pack_fallback_entries: u64,
    /// Real wall time from plan dispatch through its final import/fallback.
    #[serde(default)]
    pub last_plan_wall_ms: u64,
    #[serde(default)]
    pub plan_wall_ms_total: u64,
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
    #[serde(default)]
    pub entries_pinned: usize,
    #[serde(default)]
    pub disk_bytes_reclaimed: u64,
    #[serde(default)]
    pub entries_unreclaimable: usize,
    #[serde(default)]
    pub entries_failed: usize,
    #[serde(default)]
    pub entries_locked: usize,
    #[serde(default)]
    pub evict_write_ms: u64,
    #[serde(default)]
    pub bytes_held: u64,
}

impl From<&crate::store::GcStats> for GcPolicyOutcome {
    fn from(stats: &crate::store::GcStats) -> Self {
        Self {
            entries_evicted: stats.entries_evicted,
            bytes_freed: stats.bytes_freed,
            disk_bytes_reclaimed: stats.disk_bytes_reclaimed,
            entries_pinned: stats.entries_pinned,
            entries_unreclaimable: stats.entries_unreclaimable,
            entries_failed: stats.entries_failed,
            entries_locked: stats.entries_locked,
            evict_write_ms: stats.evict_write_ms,
            bytes_held: stats.bytes_held,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health: Option<DaemonHealth>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_results: Option<Vec<Response>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash_results: Option<Vec<HashFileResult>>,
    /// Reply payload for `Request::PredictionFetch` (kunobi-ninja/kache#1011).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prediction: Option<crate::prediction_share::SharedPrediction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl Response {
    pub(crate) fn ok() -> Self {
        Self {
            ok: true,
            evicted: None,
            skipped: false,
            gc: None,
            found: None,
            prefetched: None,
            stats: None,
            health: None,
            batch_results: None,
            hash_results: None,
            prediction: None,
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
            health: None,
            batch_results: None,
            hash_results: None,
            prediction: None,
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
            health: None,
            batch_results: None,
            hash_results: None,
            prediction: None,
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
            health: None,
            batch_results: None,
            hash_results: None,
            prediction: None,
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
            health: None,
            batch_results: Some(results),
            hash_results: None,
            prediction: None,
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
            health: None,
            batch_results: None,
            hash_results: Some(results),
            prediction: None,
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
            health: None,
            batch_results: None,
            hash_results: None,
            prediction: None,
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
            health: None,
            batch_results: None,
            hash_results: None,
            prediction: None,
            error: None,
        }
    }

    pub(crate) fn err(msg: impl Into<String>) -> Self {
        Self {
            ok: false,
            evicted: None,
            skipped: false,
            gc: None,
            found: None,
            prefetched: None,
            stats: None,
            health: None,
            batch_results: None,
            hash_results: None,
            prediction: None,
            error: Some(msg.into()),
        }
    }
}

// ── Transfer tracking ────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TransferDirection {
    Upload,
    #[default]
    Download,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct TransferEvent {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accounting: Option<PrefetchAccounting>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefetch: Option<PrefetchOrigin>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub outcome: String,
    pub compressed_bytes: u64,
    /// Wall-clock start of the transfer stage, in Unix epoch milliseconds.
    /// Zero for transfer-event schemas older than v3.
    #[serde(default)]
    pub started_at_unix_ms: u64,
    /// Wall-clock end of the complete transfer, including local import, in Unix
    /// epoch milliseconds. Zero for transfer-event schemas older than v3.
    #[serde(default)]
    pub finished_at_unix_ms: u64,
    /// End-to-end monotonic duration. Download events include SQLite import in
    /// transfer-event schema v3 and later.
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
    /// Backend invocation count; accounting specifies GET or LIST. Older
    /// records report GET counts without completeness metadata.
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
    /// Time spent waiting to acquire the SQLite store lock.
    #[serde(default)]
    pub import_lock_wait_ms: u64,
    /// Time spent executing the SQLite import after acquiring the store lock.
    /// In transfer-event schemas older than v3 this included lock wait.
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
    5
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

const MAX_PREFETCH_RECEIPT_BYTES: usize = 2 * 1024 * 1024;
const MAX_PREFETCH_RECEIPT_ENTRIES: usize = 4096;

#[derive(Default)]
struct PrefetchReceiptQueue {
    events: Vec<TransferEvent>,
    active_receipts: usize,
    retained_bytes: usize,
    entries: usize,
    overflowed: bool,
    writing: bool,
}

impl PrefetchReceiptQueue {
    fn push(&mut self, event: TransferEvent) {
        fn origin_bytes(origin: &PrefetchOrigin) -> usize {
            origin.session_id.capacity() + origin.plan_id.capacity() + origin.source.capacity()
        }
        let mut bytes = std::mem::size_of::<TransferEvent>()
            + event.crate_name.capacity()
            + event.format.capacity()
            + event.cache_key.capacity()
            + event.object_key.capacity()
            + event.outcome.capacity()
            + event.prefetch.as_ref().map_or(0, origin_bytes);
        let mut entries = 0;
        if let Some(accounting) = &event.accounting {
            entries = accounting.entries.len();
            bytes += accounting.entries.capacity() * std::mem::size_of::<PackedEntryTransfer>();
            for entry in &accounting.entries {
                bytes += entry.cache_key.capacity()
                    + entry.crate_name.capacity()
                    + entry.outcome.capacity()
                    + origin_bytes(&entry.prefetch);
            }
        }
        if bytes > MAX_PREFETCH_RECEIPT_BYTES.saturating_sub(self.retained_bytes)
            || entries > MAX_PREFETCH_RECEIPT_ENTRIES.saturating_sub(self.entries)
        {
            self.overflowed = true;
            return;
        }
        self.retained_bytes += bytes;
        self.entries += entries;
        self.events.push(event);
    }

    fn drain(&mut self) -> Vec<TransferEvent> {
        self.retained_bytes = 0;
        self.entries = 0;
        std::mem::take(&mut self.events)
    }
}

/// A panic must release writer admission and make missing coverage visible.
struct PrefetchReceiptWriter {
    queue: Arc<Mutex<PrefetchReceiptQueue>>,
    failed: Arc<AtomicBool>,
    armed: bool,
}

impl Drop for PrefetchReceiptWriter {
    fn drop(&mut self) {
        if self.armed {
            self.failed.store(true, Ordering::Release);
            self.queue.lock().unwrap_or_else(|p| p.into_inner()).writing = false;
        }
    }
}

/// A receipt remains owned across decode/import awaits. Cancellation only
/// queues it in memory; log I/O runs separately on the blocking pool.
struct PrefetchReceipt {
    event: TransferEvent,
    started: Instant,
    queue: Arc<Mutex<PrefetchReceiptQueue>>,
}

impl PrefetchReceipt {
    fn new(
        queue: Arc<Mutex<PrefetchReceiptQueue>>,
        origin: PrefetchOrigin,
        object_key: &str,
        format: &str,
        operation: PrefetchOperation,
    ) -> Self {
        let receipt = Self {
            event: TransferEvent {
                schema: default_transfer_schema(),
                prefetch: Some(origin),
                object_key: object_key.to_owned(),
                crate_name: format.to_owned(),
                format: format.to_owned(),
                outcome: "cancelled".to_owned(),
                started_at_unix_ms: unix_time_ms(),
                accounting: Some(PrefetchAccounting {
                    operation,
                    requests_complete: true,
                    bytes_complete: operation == PrefetchOperation::Get,
                    ..Default::default()
                }),
                ..Default::default()
            },
            started: Instant::now(),
            queue,
        };
        receipt
            .queue
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .active_receipts += 1;
        receipt
    }

    fn accounting(&mut self) -> &mut PrefetchAccounting {
        self.event.accounting.as_mut().expect("receipt accounting")
    }

    fn received(&mut self, object: &crate::remote_backend::GetObject) {
        self.event.compressed_bytes = object.body.len() as u64;
        self.event.request_ms = object.request_ms;
        self.event.body_ms = object.body_ms;
        self.accounting().bytes_complete = true;
    }

    fn finish(&mut self, outcome: &str) {
        self.event.outcome = outcome.to_owned();
        self.event.ok = outcome == "completed";
        self.event.finished_at_unix_ms = unix_time_ms();
        self.event.elapsed_ms = self.started.elapsed().as_millis() as u64;
    }
}

fn finish_open_prefetch_receipt(receipt: &mut PrefetchReceipt, ok: bool) {
    if receipt.event.finished_at_unix_ms == 0 {
        receipt.finish(if ok { "completed" } else { "error" });
    }
}

impl Drop for PrefetchReceipt {
    fn drop(&mut self) {
        if self.event.finished_at_unix_ms == 0 {
            self.event.finished_at_unix_ms = unix_time_ms();
            self.event.elapsed_ms = self.started.elapsed().as_millis() as u64;
        }
        self.event.timestamp = self.event.finished_at_unix_ms / 1_000;
        let mut queue = self.queue.lock().unwrap_or_else(|p| p.into_inner());
        queue.active_receipts -= 1;
        queue.push(std::mem::take(&mut self.event));
    }
}

struct PrefetchBackendObserver<'a> {
    receipt: &'a mut PrefetchReceipt,
    stats: &'a PrefetchStats,
    transfers: &'a TransferCounters,
    network_started: Option<Instant>,
}

impl crate::remote_layout::DownloadObserver for PrefetchBackendObserver<'_> {
    fn started(&mut self, object_key: &str) {
        self.receipt.event.object_key = object_key.to_owned();
        self.receipt.event.request_count = 1;
        self.receipt.accounting().bytes_complete = false;
        self.stats.v3_requests_total.fetch_add(1, Ordering::Relaxed);
    }

    fn received(&mut self, transfer: Option<&crate::remote_backend::GetTransfer>) {
        self.receipt.accounting().bytes_complete = true;
        if let Some(transfer) = transfer {
            self.receipt.event.compressed_bytes = transfer.bytes;
            self.receipt.event.request_ms = transfer.request_ms;
            self.receipt.event.body_ms = transfer.body_ms;
            self.receipt.event.network_ms = transfer.request_ms + transfer.body_ms;
            self.stats
                .v3_bytes_downloaded
                .fetch_add(transfer.bytes, Ordering::Relaxed);
            self.stats
                .bytes_downloaded
                .fetch_add(transfer.bytes, Ordering::Relaxed);
            self.transfers
                .bytes_downloaded
                .fetch_add(transfer.bytes, Ordering::Relaxed);
        }
    }
}

impl crate::remote_layout::ListObserver for PrefetchBackendObserver<'_> {
    fn started(&mut self, prefix: &str) {
        self.network_started = Some(Instant::now());
        self.receipt.event.object_key = prefix.to_owned();
        self.receipt.event.request_count += 1;
        self.receipt.accounting().list_result_count = None;
        self.stats
            .list_requests_total
            .fetch_add(1, Ordering::Relaxed);
    }

    fn completed(&mut self, result_count: usize) {
        self.receipt.accounting().list_result_count = Some(result_count as u64);
        self.receipt.event.network_ms = self
            .network_started
            .expect("LIST start precedes completion")
            .elapsed()
            .as_millis() as u64;
        self.receipt.finish("completed");
    }
}

fn charge_packed_import(event: &mut TransferEvent, original_bytes: u64, extract_ms: u64) {
    event.original_bytes += original_bytes;
    event.extract_ms += extract_ms;
}

struct PackedAttribution<'a> {
    origin: &'a PrefetchOrigin,
    ranks: &'a HashMap<String, u64>,
    sources: &'a HashMap<String, kache_core::CandidateSource>,
}

impl PackedAttribution<'_> {
    fn for_key(&self, key: &str) -> PrefetchOrigin {
        PrefetchOrigin {
            candidate_rank: self.ranks.get(key).copied(),
            candidate_source: self.sources.get(key).copied().unwrap_or_default(),
            ..self.origin.clone()
        }
    }
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
    /// Known candidates dropped before GET by adaptive cancellation or shutdown.
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
    pub pack_requests_total: std::sync::atomic::AtomicU64,
    pub pack_bytes_downloaded: std::sync::atomic::AtomicU64,
    pub v3_requests_total: std::sync::atomic::AtomicU64,
    pub v3_bytes_downloaded: std::sync::atomic::AtomicU64,
    pub pack_validation_failures: std::sync::atomic::AtomicU64,
    pub pack_fallback_entries: std::sync::atomic::AtomicU64,
    pub last_plan_wall_ms: std::sync::atomic::AtomicU64,
    pub plan_wall_ms_total: std::sync::atomic::AtomicU64,
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
            pack_requests_total: 0.into(),
            pack_bytes_downloaded: 0.into(),
            v3_requests_total: 0.into(),
            v3_bytes_downloaded: 0.into(),
            pack_validation_failures: 0.into(),
            pack_fallback_entries: 0.into(),
            last_plan_wall_ms: 0.into(),
            plan_wall_ms_total: 0.into(),
        }
    }
}

const RECENT_TRANSFERS_CAP: usize = 50;

const PREFETCH_CANCELLATION_CAP: usize = 128;

#[derive(Default)]
struct PrefetchCancellations {
    origins: Vec<PrefetchOrigin>,
    overflowed: bool,
}

// Drop never performs file I/O. Shutdown consumes these bounded records only
// after every owned task has exited, before writing the final summary.
struct PrefetchTaskGuard {
    origin: Option<PrefetchOrigin>,
    cancellations: Arc<Mutex<PrefetchCancellations>>,
}

impl PrefetchTaskGuard {
    fn complete(mut self) {
        self.origin = None;
    }
}

impl Drop for PrefetchTaskGuard {
    fn drop(&mut self) {
        if let Some(origin) = self.origin.take() {
            let mut queue = self.cancellations.lock().unwrap_or_else(|p| p.into_inner());
            if queue.origins.len() == PREFETCH_CANCELLATION_CAP {
                queue.overflowed = true;
            } else {
                queue.origins.push(origin);
            }
        }
    }
}

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
    /// `none` while only tracking a session, then `advisory` or `fallback`.
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
    /// Rank-0 identity key for this session, if the wrapper sent one.
    pub identity_key: Option<String>,
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
            identity_key: None,
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

    fn record_download_from(&mut self, origin: &PrefetchOrigin, key: &str, compressed_bytes: u64) {
        if self.session_id == origin.session_id
            && self.plan_id == origin.plan_id
            && self.plan_source == origin.source
        {
            self.record_download(key, compressed_bytes);
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

    #[cfg(test)]
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
            tracing::debug!(
                "discarding stale key-cache LIST snapshot after a concurrent point update"
            );
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
    /// Stores opened for `[cache.volumes]` shards. Main stays in `store`.
    shard_stores: Mutex<HashMap<PathBuf, Arc<Mutex<Store>>>>,
    remote_backend: tokio::sync::OnceCell<Arc<dyn crate::remote_backend::RemoteBackend>>,
    v3_remote: tokio::sync::OnceCell<Arc<crate::cache_remote::V3Remote>>,
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
    /// Finished cc compiles handed off by wrappers, waiting to be stored.
    publish_queue: crate::daemon_publish::PublishQueue,
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
    prefetch_stopping: AtomicBool,
    /// Own both coordinators and their independently scheduled download tasks.
    prefetch_tasks: Mutex<tokio::task::JoinSet<()>>,
    prefetch_cancellations: Arc<Mutex<PrefetchCancellations>>,
    prefetch_receipts: Arc<Mutex<PrefetchReceiptQueue>>,
    prefetch_receipt_writers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    prefetch_receipt_failed: Arc<AtomicBool>,
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
    /// When the last build request arrived. Index compaction waits for a
    /// gap here, because a build made of cache hits holds no compile permit.
    request_clock: Arc<crate::maintenance::RequestClock>,
    /// Set while a hinted sweep is queued or running; further hints coalesce.
    gc_hint_pending: AtomicBool,
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

fn identity_publish_context(
    identity_key: Option<&str>,
    session_id: &str,
    remote_configured: bool,
    remote_readonly: bool,
) -> Option<(String, String)> {
    if !remote_configured || remote_readonly {
        return None;
    }
    let identity_key = identity_key?.trim();
    let session_id = session_id.trim();
    if identity_key.is_empty() || session_id.is_empty() {
        return None;
    }
    Some((identity_key.to_string(), session_id.to_string()))
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
            shard_stores: Mutex::new(HashMap::new()),
            s3_semaphore: Arc::new(tokio::sync::Semaphore::new(permits)),
            remote_backend: tokio::sync::OnceCell::new(),
            v3_remote: tokio::sync::OnceCell::new(),
            key_cache: Arc::new(S3KeyCache::new()),
            remote_breaker: Arc::new(RemoteBreaker::new()),
            negative_keys: NegativeKeyCache::new(config.remote_negative_ttl_secs),
            remote_checks: KeyedSingleflight::new(REMOTE_CHECK_SINGLEFLIGHT_MAX_KEYS),
            upload_tx: Mutex::new(None),
            upload_queue_closed: AtomicBool::new(false),
            pending_uploads: Arc::new(RwLock::new(HashSet::new())),
            publish_queue: crate::daemon_publish::PublishQueue::new(),
            downloading: Arc::new(RwLock::new(HashMap::new())),
            warming_tx,
            prefetched_keys: Arc::new(RwLock::new(HashSet::new())),
            prefetch_cancel,
            prefetch_stopping: AtomicBool::new(false),
            prefetch_tasks: Mutex::new(tokio::task::JoinSet::new()),
            prefetch_cancellations: Arc::new(Mutex::new(PrefetchCancellations::default())),
            prefetch_receipts: Arc::new(Mutex::new(PrefetchReceiptQueue::default())),
            prefetch_receipt_writers: Mutex::new(Vec::new()),
            prefetch_receipt_failed: Arc::new(AtomicBool::new(false)),
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
            request_clock: Arc::new(crate::maintenance::RequestClock::new()),
            gc_hint_pending: AtomicBool::new(false),
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

    fn shard_store_lock(&self, cache_dir: &Path) -> Result<Arc<Mutex<Store>>> {
        {
            let map = self
                .shard_stores
                .lock()
                .map_err(|_| anyhow::anyhow!("daemon shard store map poisoned"))?;
            if let Some(existing) = map.get(cache_dir) {
                return Ok(Arc::clone(existing));
            }
        }
        let mut cfg = self.config.clone();
        cfg.cache_dir = cache_dir.to_path_buf();
        let store = Store::open(&cfg)?;
        let lock = Arc::new(Mutex::new(store));
        let mut map = self
            .shard_stores
            .lock()
            .map_err(|_| anyhow::anyhow!("daemon shard store map poisoned"))?;
        Ok(Arc::clone(
            map.entry(cache_dir.to_path_buf()).or_insert(lock),
        ))
    }

    fn with_import_store<T>(
        &self,
        cache_dir: &Path,
        f: impl FnOnce(&Store) -> Result<T>,
    ) -> Result<T> {
        if remote_check_uses_main_store(cache_dir, &self.config.cache_dir) {
            return self.with_store(f);
        }
        let lock = self.shard_store_lock(cache_dir)?;
        let guard = lock
            .lock()
            .map_err(|_| anyhow::anyhow!("daemon shard store mutex poisoned"))?;
        f(&guard)
    }

    fn with_store_timed<T>(&self, f: impl FnOnce(&Store) -> Result<T>) -> (Result<T>, u64, u64) {
        let store = match self.store_lock() {
            Ok(store) => store,
            Err(error) => return (Err(error), 0, 0),
        };
        let wait_started = Instant::now();
        let guard = match store.lock() {
            Ok(guard) => guard,
            Err(_) => {
                return (
                    Err(anyhow::anyhow!("daemon store mutex poisoned")),
                    wait_started.elapsed().as_millis() as u64,
                    0,
                );
            }
        };
        let lock_wait_ms = wait_started.elapsed().as_millis() as u64;
        let import_started = Instant::now();
        let result = f(&guard);
        let import_ms = import_started.elapsed().as_millis() as u64;
        (result, lock_wait_ms, import_ms)
    }

    fn with_import_store_timed<T>(
        &self,
        cache_dir: &Path,
        f: impl FnOnce(&Store) -> Result<T>,
    ) -> (Result<T>, u64, u64) {
        if remote_check_uses_main_store(cache_dir, &self.config.cache_dir) {
            return self.with_store_timed(f);
        }
        let lock = match self.shard_store_lock(cache_dir) {
            Ok(lock) => lock,
            Err(error) => return (Err(error), 0, 0),
        };
        let wait_started = Instant::now();
        let guard = match lock.lock() {
            Ok(guard) => guard,
            Err(_) => {
                return (
                    Err(anyhow::anyhow!("daemon shard store mutex poisoned")),
                    wait_started.elapsed().as_millis() as u64,
                    0,
                );
            }
        };
        let lock_wait_ms = wait_started.elapsed().as_millis() as u64;
        let import_started = Instant::now();
        let result = f(&guard);
        let import_ms = import_started.elapsed().as_millis() as u64;
        (result, lock_wait_ms, import_ms)
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
        self.config
            .remote
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no remote configured"))?;
        let deadline = RemoteDeadline::from_secs(self.config.remote_restore_timeout_secs);
        let breaker = self
            .remote_breaker
            .try_acquire(RemoteOperation::ShardGet)
            .ok_or_else(|| anyhow::anyhow!("remote read breaker open"))?;
        let v3 = match deadline
            .run("planner backend initialization", self.v3_remote())
            .await
        {
            Ok(v3) => v3,
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
            .run("planner shard GET", v3.get_shard(namespace, shard_hash))
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

    /// Breaker/deadline/semaphore-aware identity-manifest fetch for the
    /// fallback planner. Missing objects are `Ok(None)`.
    pub(crate) async fn download_planner_manifest(
        &self,
        manifest_key: &str,
    ) -> Result<Option<crate::remote::BuildManifest>> {
        if self.config.remote.is_none() {
            return Err(anyhow::anyhow!("no remote configured"));
        }
        let breaker = self
            .remote_breaker
            .try_acquire(RemoteOperation::ManifestGet)
            .ok_or_else(|| anyhow::anyhow!("remote read breaker open"))?;
        self.download_planner_manifest_with_permit(manifest_key, breaker)
            .await
    }

    /// Attempt a speculative identity-manifest fetch without taking the read
    /// breaker's half-open recovery probe. A typed `NotAdmitted` result lets
    /// fallback planning perform the ordinary lookup without conflating it
    /// with a completed remote miss.
    pub(crate) async fn download_planner_manifest_speculative(
        &self,
        manifest_key: &str,
    ) -> Result<SpeculativeManifestOutcome> {
        self.config
            .remote
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no remote configured"))?;
        if !self
            .remote_breaker
            .can_attempt_speculative(RemoteOperation::ManifestGet)
        {
            return Ok(SpeculativeManifestOutcome::NotAdmitted);
        }

        let deadline = RemoteDeadline::from_secs(self.config.remote_restore_timeout_secs);
        let v3 = deadline
            .run("planner backend initialization", self.v3_remote())
            .await?;

        // Identity lookahead is speculative work. Put it behind the same
        // daemon-wide gate as artifact prefetch so the S3 pool's demand
        // reserve remains available even across concurrent BuildStarted hints.
        let gate = deadline
            .run("planner speculative gate", async {
                self.prefetch_gate
                    .acquire()
                    .await
                    .map_err(|_| anyhow::anyhow!("prefetch gate closed"))
            })
            .await?;
        let semaphore = deadline
            .run("planner manifest queue", async {
                self.s3_semaphore
                    .acquire()
                    .await
                    .map_err(|_| anyhow::anyhow!("remote semaphore closed"))
            })
            .await?;

        // Revalidate only after every awaitable admission step. A permit
        // acquired before either queue could outlive an open transition and
        // dispatch stale speculative work against a degraded remote.
        let Some(breaker) = self
            .remote_breaker
            .try_acquire_speculative(RemoteOperation::ManifestGet)
        else {
            drop(semaphore);
            drop(gate);
            return Ok(SpeculativeManifestOutcome::NotAdmitted);
        };
        let result = deadline
            .run("planner manifest GET", v3.get_build_manifest(manifest_key))
            .await;
        drop(semaphore);
        drop(gate);
        match result {
            Ok(manifest) => {
                breaker.success();
                Ok(SpeculativeManifestOutcome::Completed(manifest))
            }
            Err(error) => {
                let class = classify_remote_error(&error);
                breaker.failure(class, &format!("{error:#}"));
                Err(error)
            }
        }
    }

    async fn download_planner_manifest_with_permit(
        &self,
        manifest_key: &str,
        breaker: BreakerPermit,
    ) -> Result<Option<crate::remote::BuildManifest>> {
        self.config
            .remote
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no remote configured"))?;
        let deadline = RemoteDeadline::from_secs(self.config.remote_restore_timeout_secs);
        let v3 = match deadline
            .run("planner backend initialization", self.v3_remote())
            .await
        {
            Ok(v3) => v3,
            Err(error) => {
                let class = classify_remote_error(&error);
                breaker.failure(class, &format!("{error:#}"));
                return Err(error);
            }
        };
        let semaphore = match deadline
            .run("planner manifest queue", async {
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
            .run("planner manifest GET", v3.get_build_manifest(manifest_key))
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

    async fn push_transfer_event(&self, event: TransferEvent) {
        // Persist to JSONL — warn on failure but never fail the transfer.
        // The append takes a cross-process file lock on the sidecar log,
        // so it runs on the blocking pool: every caller is an async
        // upload/download path, and a contended or stalled log must not
        // park an async worker thread (#281).
        let path = self.config.transfer_log_path();
        let event = match tokio::task::spawn_blocking(move || {
            let logged = events::log_transfer(&path, &event);
            (event, logged)
        })
        .await
        {
            Ok((event, logged)) => {
                if let Err(e) = logged {
                    tracing::warn!("failed to log transfer event: {e}");
                }
                event
            }
            Err(e) => {
                tracing::warn!("transfer log task failed: {e}");
                return;
            }
        };
        if let Ok(mut q) = self.recent_transfers.lock() {
            if q.len() >= RECENT_TRANSFERS_CAP {
                q.pop_front();
            }
            q.push_back(event);
        }
    }

    /// Transfer ownership to a tracked writer before any await. Aborting a
    /// coordinator cannot discard a body receipt already queued by its guard.
    fn flush_prefetch_receipts(self: &Arc<Self>) {
        let mut queue = self
            .prefetch_receipts
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if queue.writing || queue.events.is_empty() {
            return;
        }
        queue.writing = true;
        let mut pending = queue.drain();
        if !queue.events.is_empty() {
            // drain() takes the queue. A dummy return would spin the writer.
            queue.writing = false;
            self.prefetch_receipt_failed.store(true, Ordering::Release);
            return;
        }
        let guard = PrefetchReceiptWriter {
            queue: self.prefetch_receipts.clone(),
            failed: self.prefetch_receipt_failed.clone(),
            armed: true,
        };
        // Admission and registration stay synchronous; shutdown first joins
        // every producer, then takes the writer handles.
        drop(queue);
        let daemon = self.clone();
        let writer = tokio::task::spawn_blocking(move || {
            let mut guard = guard;
            let path = daemon.config.transfer_log_path();
            loop {
                for event in pending {
                    if let Err(error) = events::log_transfer(&path, &event) {
                        daemon
                            .prefetch_receipt_failed
                            .store(true, Ordering::Release);
                        tracing::warn!("failed to log prefetch receipt: {error}");
                    }
                    if let Ok(mut recent) = daemon.recent_transfers.lock() {
                        if recent.len() >= RECENT_TRANSFERS_CAP {
                            recent.pop_front();
                        }
                        recent.push_back(event);
                    }
                }
                let mut queue = daemon
                    .prefetch_receipts
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                if queue.events.is_empty() {
                    queue.writing = false;
                    guard.armed = false;
                    break;
                }
                pending = queue.drain();
                if !queue.events.is_empty() {
                    daemon
                        .prefetch_receipt_failed
                        .store(true, Ordering::Release);
                    queue.writing = false;
                    guard.armed = false;
                    break;
                }
            }
        });
        let mut writers = self
            .prefetch_receipt_writers
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        use futures::FutureExt as _;
        writers.retain_mut(|writer| match std::pin::Pin::new(writer).now_or_never() {
            None => true,
            Some(result) => {
                if result.is_err() {
                    self.prefetch_receipt_failed.store(true, Ordering::Release);
                }
                false
            }
        });
        writers.push(writer);
    }

    async fn finish_prefetch_receipts(self: &Arc<Self>) -> bool {
        self.flush_prefetch_receipts();
        let writers = std::mem::take(
            &mut *self
                .prefetch_receipt_writers
                .lock()
                .unwrap_or_else(|p| p.into_inner()),
        );
        let finished = tokio::time::timeout(Duration::from_secs(5), async {
            let mut complete = true;
            for writer in writers {
                complete &= writer.await.is_ok();
            }
            complete
        })
        .await
        .unwrap_or(false);
        let queue = self
            .prefetch_receipts
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        finished
            && !self.prefetch_receipt_failed.load(Ordering::Acquire)
            && !queue.overflowed
            && !queue.writing
            && queue.events.is_empty()
            && queue.active_receipts == 0
    }

    fn background_prefetch_origin(&self) -> PrefetchOrigin {
        let plan = self.active_plan.lock().unwrap_or_else(|p| p.into_inner());
        plan.as_ref().map_or_else(
            || PrefetchOrigin {
                source: "unscoped".into(),
                ..Default::default()
            },
            |plan| PrefetchOrigin {
                session_id: plan.session_id.clone(),
                plan_id: plan.plan_id.clone(),
                source: if plan.plan_id.is_empty() {
                    "unscoped"
                } else {
                    plan.plan_source
                }
                .into(),
                ..Default::default()
            },
        )
    }

    async fn list_warm_all_keys(
        self: &Arc<Self>,
        remote_cache: &dyn crate::cache_remote::CacheRemote,
        origin: PrefetchOrigin,
        deadline: RemoteDeadline,
    ) -> Result<HashMap<String, String>> {
        let mut receipt = PrefetchReceipt::new(
            self.prefetch_receipts.clone(),
            origin,
            "",
            "v3",
            PrefetchOperation::List,
        );
        let result = async {
            let semaphore = deadline
                .run("warm-all LIST queue", async {
                    self.s3_semaphore
                        .acquire()
                        .await
                        .map_err(|_| anyhow::anyhow!("remote semaphore closed"))
                })
                .await?;
            anyhow::ensure!(
                !self.prefetch_stopping.load(Ordering::Acquire),
                "daemon stopping before warm-all LIST"
            );
            receipt.event.semaphore_wait_ms = receipt.started.elapsed().as_millis() as u64;
            let result = deadline
                .run(
                    "warm-all LIST",
                    remote_cache.list_keys_observed(&mut PrefetchBackendObserver {
                        receipt: &mut receipt,
                        stats: &self.prefetch_stats,
                        transfers: &self.transfer_counters,
                        network_started: None,
                    }),
                )
                .await;
            drop(semaphore);
            result
        }
        .await;
        finish_open_prefetch_receipt(&mut receipt, result.is_ok());
        self.prefetch_stats.list_duration_ms_total.fetch_add(
            receipt.started.elapsed().as_millis() as u64,
            Ordering::Relaxed,
        );
        if result.is_err() {
            self.prefetch_stats
                .list_failures_total
                .fetch_add(1, Ordering::Relaxed);
        }
        drop(receipt);
        self.flush_prefetch_receipts();
        result
    }

    /// Set the upload buffer sender (called during server setup).
    pub(crate) fn config(&self) -> &Config {
        &self.config
    }

    pub(crate) fn publish_queue(&self) -> &crate::daemon_publish::PublishQueue {
        &self.publish_queue
    }

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

    /// The configured remote in the v3 layout, built once from the same
    /// backend `get_remote_backend` returns, so test injection still applies.
    pub(crate) async fn v3_remote(&self) -> Result<&Arc<crate::cache_remote::V3Remote>> {
        self.v3_remote
            .get_or_try_init(|| async {
                let backend = Arc::clone(self.get_remote_backend().await?);
                let remote = self
                    .config
                    .remote
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("no remote configured"))?
                    .clone();
                Ok::<_, anyhow::Error>(Arc::new(crate::cache_remote::V3Remote::new(
                    backend, remote,
                )))
            })
            .await
    }

    /// Entry-level view of the configured remote.
    pub(crate) async fn cache_remote(&self) -> Result<Arc<dyn crate::cache_remote::CacheRemote>> {
        Ok(Arc::clone(self.v3_remote().await?) as Arc<dyn crate::cache_remote::CacheRemote>)
    }

    #[cfg(test)]
    pub(crate) fn set_remote_backend_for_test(
        &self,
        backend: Arc<dyn crate::remote_backend::RemoteBackend>,
    ) {
        assert!(
            self.remote_backend.set(backend).is_ok(),
            "test remote backend must be set only once"
        );
    }

    /// Dispatch a parsed request to the appropriate handler (sync-only requests).
    #[cfg(test)]
    pub fn handle_request_sync(&self, req: &Request) -> Response {
        match req {
            Request::Gc(gc) | Request::GcV2(gc) => self.handle_gc(gc),
            Request::GcHint => {
                if self.claim_gc_hint() {
                    self.run_hinted_sweep();
                }
                Response::ok()
            }
            Request::Stats(sr) => self.handle_stats(sr),
            Request::Health => self.handle_health(),
            Request::HashFiles(req) => self.handle_hash_files(req),
            Request::CompileStarted(req) => self.handle_compile_started(req.clone()),
            Request::CompileFinished(req) => self.handle_compile_finished(req),
            Request::Upload(_)
            | Request::RemoteCheck(_)
            | Request::BatchRemoteCheck(_)
            | Request::Prefetch(_)
            | Request::PublishCc(_)
            | Request::PredictionFetch(_)
            | Request::PredictionPublish(_)
            | Request::BuildStarted(_) => {
                // These require async — caller must use their async handlers
                Response::err(
                    "upload/remote_check/batch/prefetch/build_started must be handled async",
                )
            }
            Request::Shutdown => Response::ok(),
        }
    }

    fn handle_health(&self) -> Response {
        Response {
            health: Some(DaemonHealth {
                version: self.version.clone(),
                build_epoch: self.build_epoch,
            }),
            ..Response::ok()
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

        let since = req.window().cutoff(chrono::Utc::now());
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
                pack_requests_total: ps.pack_requests_total.load(Ordering::Relaxed),
                pack_bytes_downloaded: ps.pack_bytes_downloaded.load(Ordering::Relaxed),
                v3_requests_total: ps.v3_requests_total.load(Ordering::Relaxed),
                v3_bytes_downloaded: ps.v3_bytes_downloaded.load(Ordering::Relaxed),
                pack_validation_failures: ps.pack_validation_failures.load(Ordering::Relaxed),
                pack_fallback_entries: ps.pack_fallback_entries.load(Ordering::Relaxed),
                last_plan_wall_ms: ps.last_plan_wall_ms.load(Ordering::Relaxed),
                plan_wall_ms_total: ps.plan_wall_ms_total.load(Ordering::Relaxed),
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

    /// Flush a batch of entries stored without an fsync. Uses its own store
    /// connection, as GC does, so a sweep of slow writes never holds the
    /// mutex a lookup needs.
    fn flush_pending_durability(&self) {
        let flushed = (|| -> Result<usize> {
            let store = Store::open(&self.config)?;
            if store.pending_durability()? == 0 {
                return Ok(0);
            }
            let Some(_lock) = store.try_durability_flush_lock()? else {
                return Ok(0);
            };
            store.flush_durability(crate::cli::DURABILITY_FLUSH_BATCH)
        })();
        match flushed {
            Ok(0) => {}
            Ok(n) => tracing::debug!("flushed {n} entries to disk"),
            Err(error) => tracing::debug!("durability flush failed: {error:#}"),
        }
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

    /// Handle a GC request — pure logic against the store.
    pub fn handle_gc(&self, req: &GcRequest) -> Response {
        let policy = match req.resolve(self.config.gc_max_age_hours) {
            Ok(policy) => policy,
            Err(e) => return Response::err(format!("invalid GC request: {e}")),
        };
        match self.run_gc(policy, GcDriver::Requested) {
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
        // Intent publication blocks on the cross-process GC lock (a
        // concurrent sweep can hold it for seconds) plus store open and
        // `std::fs` work — park it on the blocking pool like the other
        // store-touching handlers (#281) instead of an async worker.
        let persist_config = self.config.clone();
        let persist_job = job.clone();
        let normalized_job = match tokio::task::spawn_blocking(move || {
            persist_upload_job(&persist_config, &persist_job)
        })
        .await
        {
            Ok(Ok(job)) => job,
            Ok(Err(error)) => {
                return Response::err(format!("persisting upload intent failed: {error:#}"));
            }
            Err(error) => {
                return Response::err(format!("persisting upload intent failed: {error}"));
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

        let remote_cache = match deadline
            .run("upload backend initialization", self.cache_remote())
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
                remote_cache.exists_entry(&job.key, &job.crate_name),
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
        let started_at_unix_ms = unix_time_ms();
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
                remote_cache.upload_entry(
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
                let finished_at_unix_ms = unix_time_ms();
                self.transfer_counters
                    .uploads_completed
                    .fetch_add(1, Ordering::Relaxed);
                self.transfer_counters
                    .bytes_uploaded
                    .fetch_add(ul.transfer.compressed_bytes, Ordering::Relaxed);
                self.push_transfer_event(TransferEvent {
                    accounting: None,
                    prefetch: None,
                    outcome: String::new(),
                    schema: default_transfer_schema(),
                    crate_name: job.crate_name.clone(),
                    direction: TransferDirection::Upload,
                    format: ul.format.to_string(),
                    cache_key: job.key.clone(),
                    object_key: String::new(),
                    compressed_bytes: ul.transfer.compressed_bytes,
                    started_at_unix_ms,
                    finished_at_unix_ms,
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
                    import_lock_wait_ms: 0,
                    import_ms: 0,
                    compression_ms: ul.transfer.compression_ms,
                    head_checks_ms: ul.transfer.head_checks_ms,
                    blobs_skipped: 0,
                    blobs_total: 0,
                    ok: true,
                    timestamp: finished_at_unix_ms / 1_000,
                })
                .await;
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
                let finished_at_unix_ms = unix_time_ms();
                self.transfer_counters
                    .uploads_failed
                    .fetch_add(1, Ordering::Relaxed);
                self.push_transfer_event(TransferEvent {
                    accounting: None,
                    prefetch: None,
                    outcome: String::new(),
                    schema: default_transfer_schema(),
                    crate_name: job.crate_name.clone(),
                    direction: TransferDirection::Upload,
                    format: plan.transfer_format().to_string(),
                    cache_key: job.key.clone(),
                    object_key: String::new(),
                    compressed_bytes: 0,
                    started_at_unix_ms,
                    finished_at_unix_ms,
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
                    import_lock_wait_ms: 0,
                    import_ms: 0,
                    compression_ms: 0,
                    head_checks_ms: 0,
                    blobs_skipped: 0,
                    blobs_total: 0,
                    ok: false,
                    timestamp: finished_at_unix_ms / 1_000,
                })
                .await;
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
    #[cfg(test)]
    pub async fn handle_remote_check(&self, req: &RemoteCheckRequest) -> Response {
        self.handle_remote_check_started_at(req, Instant::now())
            .await
    }

    /// The remote's input prediction row for a unit the wrapper has no row
    /// for (kunobi-ninja/kache#1011). No remote, a portable identity the row
    /// may not answer, a missing object and a failed or slow transfer all
    /// answer "no row": the wrapper then runs the pre-pass as before.
    async fn handle_prediction_fetch(&self, req: &PredictionFetchRequest) -> Response {
        let mut response = Response::ok();
        let Some(remote) = self.config.remote.as_ref() else {
            return response;
        };
        if !prediction_identity_is_acceptable(&req.identity) {
            return Response::err("invalid prediction identity");
        }
        let fetch = async {
            let backend = self.get_remote_backend().await?;
            crate::remote_layout::RemoteLayout::new(backend.as_ref(), remote)
                .download_prediction(&req.identity)
                .await
        };
        match tokio::time::timeout(PREDICTION_FETCH_BUDGET, fetch).await {
            Ok(Ok(row)) => response.prediction = row,
            Ok(Err(error)) => tracing::debug!("prediction row fetch failed: {error:#}"),
            Err(_) => tracing::debug!("prediction row fetch timed out"),
        }
        response
    }

    /// Store a portable row on the remote in the background. Acknowledged at
    /// once: the wrapper does not wait for the upload. Skipped with no remote
    /// or a read-only one, the same gate artifact uploads use.
    fn handle_prediction_publish(self: &Arc<Self>, req: PredictionPublishRequest) -> Response {
        if !publishes_predictions(&self.config) {
            return Response::ok();
        }
        if !prediction_identity_is_acceptable(&req.identity) {
            return Response::err("invalid prediction identity");
        }
        let daemon = Arc::clone(self);
        tokio::spawn(async move {
            let Some(remote) = daemon.config.remote.as_ref() else {
                return;
            };
            let upload = async {
                let backend = daemon.get_remote_backend().await?;
                crate::remote_layout::RemoteLayout::new(backend.as_ref(), remote)
                    .upload_prediction(&req.identity, &req.row)
                    .await
            };
            if let Err(error) = upload.await {
                tracing::debug!("prediction row upload failed: {error:#}");
            }
        });
        Response::ok()
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
        let cache_dir = match remote_check_cache_dir(
            &self.config.cache_dir,
            &self.config.volume_stores,
            req.shard_dir.as_deref(),
        ) {
            Ok(dir) => dir,
            Err(msg) => return Response::err(msg),
        };
        let expected_entry_dir = remote_check_entry_dir(cache_dir, &req.key);
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
            remote_check_budget_ms(self.config.remote_restore_timeout_secs, req.deadline_ms).get(),
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
        let cache_dir = match remote_check_cache_dir(
            &self.config.cache_dir,
            &self.config.volume_stores,
            req.shard_dir.as_deref(),
        ) {
            Ok(dir) => dir.to_path_buf(),
            Err(msg) => return Response::err(msg),
        };
        let Some(_) = &self.config.remote else {
            return Response::err("no remote configured");
        };

        let warmed = deadline
            .run("warming barrier", async {
                Ok(self.wait_for_warming(REMOTE_CHECK_WARMING_GRACE).await)
            })
            .await
            .unwrap_or(false);
        tracing::debug!(
            warmed,
            grace_ms = REMOTE_CHECK_WARMING_GRACE.as_millis(),
            "remote check warming barrier completed"
        );

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

        let remote_cache = match deadline
            .run("demand backend initialization", self.cache_remote())
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
            semaphore_wait_ms =
                semaphore_wait_ms.saturating_add(semaphore_start.elapsed().as_millis() as u64);
            let head_start = Instant::now();
            // Exactly one retry layer: the daemon issues one transport call.
            // In particular, there is no backoff sleep while the S3 permit is
            // held; a later request can retry after breaker policy admits it.
            let exists = deadline
                .run("demand HEAD", remote_cache.exists_entry(&req.key, cn))
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
            let join_deadline = download_join_deadline(
                tokio::time::Instant::now(),
                deadline.at().map(tokio::time::Instant::from_std),
            );
            let entry_dir = remote_check_entry_dir(&cache_dir, &req.key);
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
                    // `meta.json` is only a wake-up hint: extraction writes it
                    // before Store publication, and an import failure may leave
                    // residue. Only a committed Store row is a cache hit.
                    let committed = self
                        .with_import_store(&cache_dir, |store| Ok(store.contains(&req.key)))
                        .unwrap_or(false);
                    if committed {
                        let was_prefetched = self.prefetched_keys.read().await.contains(&req.key);
                        return Response::found_prefetched(true, was_prefetched);
                    }
                    return Response::found(false);
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
        if reclaimed
            && self
                .with_import_store(&cache_dir, |store| Ok(store.contains(&req.key)))
                .unwrap_or(false)
        {
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
        semaphore_wait_ms =
            semaphore_wait_ms.saturating_add(semaphore_start.elapsed().as_millis() as u64);

        // Download to local store using the current remote layout, bounded by
        // the restore deadline (#327): on elapse the future is dropped (which
        // cancels the in-flight request) and the wrapper gets a miss — a
        // recompile is always cheaper than an unbounded wait. A partially
        // extracted entry_dir is safe to abandon: nothing consumes it before
        // `meta.json` lands, and the next download re-extracts from scratch —
        // the same tolerance the design already has for a daemon crash
        // mid-download.
        let entry_dir = remote_check_entry_dir(&cache_dir, &req.key);
        let blobs_dir = remote_check_blobs_dir(&cache_dir);
        let started_at_unix_ms = unix_time_ms();
        let start = Instant::now();
        self.transfer_counters
            .remote_check_roundtrips
            .fetch_add(1, Ordering::Relaxed);
        let download_result = deadline
            .run(
                "demand GET and extraction",
                remote_cache.download_entry(&req.key, cn, &entry_dir, &blobs_dir, deadline.at()),
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
                let (import_result, import_lock_wait_ms, import_ms) = self
                    .with_import_store_timed(&cache_dir, |store| {
                        store.import_restored_entry(&req.key)
                    });
                let import_ok = match import_result {
                    Ok(()) => true,
                    Err(e) => {
                        tracing::warn!("failed to import downloaded entry {}: {e:#}", &req.key);
                        false
                    }
                };
                let elapsed_ms = start.elapsed().as_millis() as u64;
                let finished_at_unix_ms = unix_time_ms();
                if import_ok {
                    self.transfer_counters
                        .downloads_completed
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    self.transfer_counters
                        .downloads_failed
                        .fetch_add(1, Ordering::Relaxed);
                }
                // The pack crossed the wire even when local publication failed.
                self.transfer_counters
                    .bytes_downloaded
                    .fetch_add(dl.compressed_bytes, Ordering::Relaxed);
                self.push_transfer_event(TransferEvent {
                    accounting: None,
                    prefetch: None,
                    outcome: String::new(),
                    schema: default_transfer_schema(),
                    crate_name: cn.to_string(),
                    direction: TransferDirection::Download,
                    format: dl.format.to_string(),
                    cache_key: req.key.clone(),
                    object_key: dl.object_key,
                    compressed_bytes: dl.compressed_bytes,
                    started_at_unix_ms,
                    finished_at_unix_ms,
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
                    import_lock_wait_ms,
                    import_ms,
                    compression_ms: 0,
                    head_checks_ms: 0,
                    blobs_skipped: dl.blobs_skipped,
                    blobs_total: dl.blobs_total,
                    ok: import_ok,
                    timestamp: finished_at_unix_ms / 1_000,
                })
                .await;
                Response::found(import_ok)
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
                let finished_at_unix_ms = unix_time_ms();
                self.transfer_counters
                    .downloads_failed
                    .fetch_add(1, Ordering::Relaxed);
                self.push_transfer_event(TransferEvent {
                    accounting: None,
                    prefetch: None,
                    outcome: String::new(),
                    schema: default_transfer_schema(),
                    crate_name: cn.to_string(),
                    direction: TransferDirection::Download,
                    format: plan.transfer_format().to_string(),
                    cache_key: req.key.clone(),
                    object_key: String::new(),
                    compressed_bytes: 0,
                    started_at_unix_ms,
                    finished_at_unix_ms,
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
                    import_lock_wait_ms: 0,
                    import_ms: 0,
                    compression_ms: 0,
                    head_checks_ms: 0,
                    blobs_skipped: 0,
                    blobs_total: 0,
                    ok: false,
                    timestamp: finished_at_unix_ms / 1_000,
                })
                .await;
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
    #[cfg(test)]
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

    async fn packed_prefetch_list(
        &self,
        v3: &crate::cache_remote::V3Remote,
        prefix: &str,
        receipt: &mut PrefetchReceipt,
    ) -> Result<Vec<String>> {
        let breaker = self
            .remote_breaker
            .try_acquire(RemoteOperation::PrefetchGet)
            .ok_or_else(|| anyhow::anyhow!("remote read breaker open"))?;
        let deadline = RemoteDeadline::from_secs(self.config.remote_restore_timeout_secs);
        let gate = deadline
            .run("pack catalog gate", async {
                self.prefetch_gate
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| anyhow::anyhow!("prefetch gate closed"))
            })
            .await?;
        let semaphore = deadline
            .run("pack catalog LIST queue", async {
                self.s3_semaphore
                    .acquire()
                    .await
                    .map_err(|_| anyhow::anyhow!("remote semaphore closed"))
            })
            .await?;
        anyhow::ensure!(
            !self.prefetch_stopping.load(Ordering::Acquire),
            "daemon stopping before packed prefetch request"
        );
        receipt.event.semaphore_wait_ms = receipt.started.elapsed().as_millis() as u64;
        let network_start = Instant::now();
        self.prefetch_stats
            .pack_requests_total
            .fetch_add(1, Ordering::Relaxed);
        let result = deadline
            .run("pack catalog LIST", async {
                receipt.event.request_count = 1;
                v3.list_prefetch_objects(prefix).await
            })
            .await;
        receipt.event.network_ms = network_start.elapsed().as_millis() as u64;
        drop(semaphore);
        drop(gate);
        match result {
            Ok(objects) => {
                receipt.accounting().list_result_count = Some(objects.len() as u64);
                receipt.finish("completed");
                breaker.success();
                Ok(objects)
            }
            Err(error) => {
                receipt.finish("error");
                let class = classify_remote_error(&error);
                breaker.failure(class, &format!("packed-prefetch LIST failed: {error:#}"));
                Err(error)
            }
        }
    }

    async fn packed_prefetch_get(
        &self,
        v3: &crate::cache_remote::V3Remote,
        key: &str,
        max_bytes: u64,
        stage: &'static str,
        receipt: &mut PrefetchReceipt,
    ) -> Result<Option<crate::remote_backend::GetObject>> {
        let breaker = self
            .remote_breaker
            .try_acquire(RemoteOperation::PrefetchGet)
            .ok_or_else(|| anyhow::anyhow!("remote read breaker open"))?;
        let deadline = RemoteDeadline::from_secs(self.config.remote_restore_timeout_secs);
        let gate = deadline
            .run("packed-prefetch gate", async {
                self.prefetch_gate
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| anyhow::anyhow!("prefetch gate closed"))
            })
            .await?;
        let semaphore = deadline
            .run("packed-prefetch GET queue", async {
                self.s3_semaphore
                    .acquire()
                    .await
                    .map_err(|_| anyhow::anyhow!("remote semaphore closed"))
            })
            .await?;
        anyhow::ensure!(
            !self.prefetch_stopping.load(Ordering::Acquire),
            "daemon stopping before packed prefetch request"
        );
        receipt.event.semaphore_wait_ms = receipt.started.elapsed().as_millis() as u64;
        let network_start = Instant::now();
        self.prefetch_stats
            .pack_requests_total
            .fetch_add(1, Ordering::Relaxed);
        let result = deadline
            .run(stage, async {
                receipt.event.request_count = 1;
                receipt.accounting().bytes_complete = false;
                v3.get_prefetch_object(key, max_bytes).await
            })
            .await;
        receipt.event.network_ms = network_start.elapsed().as_millis() as u64;
        drop(semaphore);
        drop(gate);
        match result {
            Ok(object) => {
                breaker.success();
                if let Some(object) = &object {
                    receipt.received(object);
                    self.prefetch_stats
                        .pack_bytes_downloaded
                        .fetch_add(object.body.len() as u64, Ordering::Relaxed);
                }
                if object.is_none() {
                    receipt.accounting().bytes_complete = true;
                    receipt.finish("not_found");
                }
                Ok(object)
            }
            Err(error) => {
                receipt.finish("error");
                let class = classify_remote_error(&error);
                breaker.failure(class, &format!("{stage} failed: {error:#}"));
                Err(error)
            }
        }
    }

    /// Try the manifest-level immutable pack catalog, returning candidate keys
    /// successfully imported. Every other candidate remains eligible for the
    /// existing v3 coordinator below.
    async fn try_packed_prefetch(
        self: &Arc<Self>,
        context: &PackPrefetchContext,
        v3: &Arc<crate::cache_remote::V3Remote>,
        remote: &crate::config::RemoteConfig,
        candidates: &[(String, String, PathBuf)],
        bytes_at_plan_start: u64,
        attribution: &PackedAttribution<'_>,
    ) -> HashSet<String> {
        let wanted = candidates
            .iter()
            .map(|(key, _, _)| key.clone())
            .collect::<HashSet<_>>();
        let mut imported = HashSet::new();
        let catalog_prefix =
            match crate::remote_pack::catalog_prefix(&remote.prefix, &context.selector) {
                Ok(prefix) => prefix,
                Err(error) => {
                    tracing::warn!("packed-prefetch selector rejected: {error:#}");
                    return imported;
                }
            };
        let mut list_receipt = PrefetchReceipt::new(
            self.prefetch_receipts.clone(),
            attribution.origin.clone(),
            &catalog_prefix,
            "pack_catalog",
            PrefetchOperation::List,
        );
        let objects = match self
            .packed_prefetch_list(v3.as_ref(), &catalog_prefix, &mut list_receipt)
            .await
        {
            Ok(objects) => objects,
            Err(error) => {
                list_receipt.finish("error");
                tracing::debug!("packed-prefetch catalog discovery failed: {error:#}");
                self.prefetch_stats
                    .pack_fallback_entries
                    .fetch_add(wanted.len() as u64, Ordering::Relaxed);
                return imported;
            }
        };
        drop(list_receipt);
        let catalog_ref = match crate::remote_pack::latest_catalog_object(
            &remote.prefix,
            &context.selector,
            &objects,
        ) {
            Ok(Some(catalog)) => catalog,
            Ok(None) => {
                self.prefetch_stats
                    .pack_fallback_entries
                    .fetch_add(wanted.len() as u64, Ordering::Relaxed);
                return imported;
            }
            Err(error) => {
                tracing::warn!("packed-prefetch catalog key rejected: {error:#}");
                self.prefetch_stats
                    .pack_validation_failures
                    .fetch_add(1, Ordering::Relaxed);
                self.prefetch_stats
                    .pack_fallback_entries
                    .fetch_add(wanted.len() as u64, Ordering::Relaxed);
                return imported;
            }
        };
        let mut catalog_receipt = PrefetchReceipt::new(
            self.prefetch_receipts.clone(),
            attribution.origin.clone(),
            &catalog_ref.object_key,
            "pack_catalog",
            PrefetchOperation::Get,
        );
        let Some(catalog_object) = self
            .packed_prefetch_get(
                v3.as_ref(),
                &catalog_ref.object_key,
                crate::remote_pack::MAX_CATALOG_BYTES as u64,
                "packed-prefetch catalog GET",
                &mut catalog_receipt,
            )
            .await
            .inspect_err(|_| catalog_receipt.finish("error"))
            .ok()
            .flatten()
        else {
            self.prefetch_stats
                .pack_fallback_entries
                .fetch_add(wanted.len() as u64, Ordering::Relaxed);
            return imported;
        };
        catalog_receipt.event.outcome = "validation_error".to_owned();
        let now_ms = epoch_ms();
        let catalog = match crate::remote_pack::decode_catalog_for_selector(
            &catalog_object.body,
            &catalog_ref.digest,
            &context.selector,
            now_ms,
        ) {
            Ok(catalog)
                if catalog.created_at_ms == catalog_ref.created_at_ms
                    && catalog.manifest_key == context.manifest_key
                    && catalog.namespace == context.namespace
                    && catalog.shard_hashes == context.shard_hashes =>
            {
                catalog
            }
            Ok(_) => {
                tracing::warn!("packed-prefetch catalog context binding mismatch");
                self.prefetch_stats
                    .pack_validation_failures
                    .fetch_add(1, Ordering::Relaxed);
                self.prefetch_stats
                    .pack_fallback_entries
                    .fetch_add(wanted.len() as u64, Ordering::Relaxed);
                return imported;
            }
            Err(error) => {
                tracing::warn!("packed-prefetch catalog validation failed: {error:#}");
                self.prefetch_stats
                    .pack_validation_failures
                    .fetch_add(1, Ordering::Relaxed);
                self.prefetch_stats
                    .pack_fallback_entries
                    .fetch_add(wanted.len() as u64, Ordering::Relaxed);
                return imported;
            }
        };

        // Parsing owns the catalog data. Release its download reservation
        // before scheduling pack GETs against the same memory budget.
        catalog_receipt.finish("completed");
        drop(catalog_receipt);
        drop(catalog_object);
        let selected = catalog
            .packs
            .iter()
            .filter(|pack| {
                pack.entries
                    .iter()
                    .any(|entry| wanted.contains(&entry.cache_key))
            })
            .collect::<Vec<_>>();
        let already_spent = self
            .prefetch_stats
            .bytes_downloaded
            .load(Ordering::Relaxed)
            .saturating_sub(bytes_at_plan_start);
        let mut reserved = 0u64;
        let admitted = selected
            .into_iter()
            .filter(|pack| {
                if self.config.prefetch_max_bytes == 0 {
                    return true;
                }
                let fits = pack.pack_bytes
                    <= self
                        .config
                        .prefetch_max_bytes
                        .saturating_sub(already_spent.saturating_add(reserved));
                if fits {
                    reserved = reserved.saturating_add(pack.pack_bytes);
                }
                fits
            })
            .cloned()
            .collect::<Vec<_>>();
        use futures::StreamExt as _;
        let mut fetched = futures::stream::iter(admitted)
            .map(|pack_ref| async move {
                let pack_key =
                    crate::remote_pack::pack_object_key(&remote.prefix, &pack_ref.digest);
                let key = match pack_key {
                    Ok(key) => key,
                    Err(error) => {
                        tracing::warn!("packed-prefetch pack key rejected: {error:#}");
                        self.prefetch_stats
                            .pack_validation_failures
                            .fetch_add(1, Ordering::Relaxed);
                        return (pack_ref, None);
                    }
                };
                let mut receipt = PrefetchReceipt::new(
                    self.prefetch_receipts.clone(),
                    attribution.origin.clone(),
                    &key,
                    "pack",
                    PrefetchOperation::Get,
                );
                let object = match self
                    .packed_prefetch_get(
                        v3.as_ref(),
                        &key,
                        pack_ref.pack_bytes,
                        "packed-prefetch pack GET",
                        &mut receipt,
                    )
                    .await
                {
                    Ok(Some(object)) => Some((object, receipt)),
                    Ok(None) => None,
                    Err(_) => {
                        receipt.finish("error");
                        None
                    }
                };
                (pack_ref, object)
            })
            .buffer_unordered(prefetch_concurrency_cap(self.config.s3_concurrency));

        let mut verified = Vec::new();
        let mut receipts = Vec::new();
        // Consume each body as it arrives. Retaining completed bodies while
        // waiting for another GET can fill the budget and block that GET.
        while let Some((pack_ref, pack_object)) = fetched.next().await {
            let Some((pack_object, receipt)) = pack_object else {
                continue;
            };
            let pack_index = receipts.len();
            receipts.push(receipt);
            let receipt = &mut receipts[pack_index];
            let decoded = match crate::remote_pack::decode_catalog_pack(
                &pack_object.body,
                &pack_ref,
                crate::remote_pack::DEFAULT_MAX_PACK_BYTES,
            ) {
                Ok(decoded) => decoded,
                Err(error) => {
                    receipt.finish("validation_error");
                    tracing::warn!("packed-prefetch pack validation failed: {error:#}");
                    self.prefetch_stats
                        .pack_validation_failures
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };
            self.transfer_counters
                .downloads_completed
                .fetch_add(1, Ordering::Relaxed);
            self.transfer_counters
                .bytes_downloaded
                .fetch_add(pack_object.body.len() as u64, Ordering::Relaxed);
            self.prefetch_stats
                .bytes_downloaded
                .fetch_add(pack_object.body.len() as u64, Ordering::Relaxed);

            for entry in decoded.entries {
                let key = &entry.descriptor.cache_key;
                let entry_index = receipt.accounting().entries.len();
                receipt.accounting().entries.push(PackedEntryTransfer {
                    cache_key: key.clone(),
                    crate_name: entry.descriptor.crate_name.clone(),
                    compressed_bytes: entry.payload.len() as u64,
                    prefetch: attribution.for_key(key),
                    outcome: "cancelled".to_owned(),
                    ..Default::default()
                });
                let entry_dir = self.entry_dir_for(key);
                if !try_claim_packed_download(&self.downloading, key, &entry_dir).await {
                    receipt.accounting().entries[entry_index].outcome =
                        "already_present_or_inflight".to_owned();
                    continue;
                }
                let guard = DownloadingGuard::new(self.downloading.clone(), key.clone());
                match crate::remote_layout::extract_verified_prefetch_entry(
                    key,
                    &entry.descriptor.crate_name,
                    &entry.descriptor.meta_digest,
                    entry.payload,
                    &entry_dir,
                    None,
                ) {
                    Ok(extracted) => verified.push((extracted, guard, pack_index, entry_index)),
                    Err(error) => {
                        receipt.accounting().entries[entry_index].outcome =
                            "validation_error".to_owned();
                        tracing::warn!(
                            key = key_prefix(key),
                            "packed-prefetch entry validation failed: {error:#}"
                        );
                        self.prefetch_stats
                            .pack_validation_failures
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }

        let batch = verified
            .iter()
            .map(|(entry, _, _, _)| entry.restored.clone())
            .collect::<Vec<_>>();
        if !batch.is_empty() {
            let import_start = Instant::now();
            match self.with_store(|store| store.import_verified_restored_entries(&batch)) {
                Ok(_) => {
                    let completed_at_ms = unix_time_ms();
                    let original_bytes = verified
                        .iter()
                        .map(|(entry, _, _, _)| entry.original_bytes)
                        .sum::<u64>();
                    let extract_ms = verified
                        .iter()
                        .map(|(entry, _, _, _)| entry.extract_ms)
                        .sum::<u64>();
                    // No await before every successfully imported entry has its
                    // exact availability boundary, including other packs in this batch.
                    let import_ms = import_start.elapsed().as_millis() as u64;
                    // One shared batch import: charge its duration once, to
                    // the first imported pack, rather than once per object.
                    receipts[verified[0].2].event.import_ms = import_ms;
                    for (entry, _, pack_index, entry_index) in &verified {
                        let receipt = &mut receipts[*pack_index];
                        charge_packed_import(
                            &mut receipt.event,
                            entry.original_bytes,
                            entry.extract_ms,
                        );
                        let logical = &mut receipt.accounting().entries[*entry_index];
                        logical.finished_at_ms = completed_at_ms;
                        logical.outcome = "completed".to_owned();
                        let key = &entry.restored.cache_key;
                        imported.insert(key.clone());
                        let mut plan = self.active_plan.lock().unwrap_or_else(|p| p.into_inner());
                        if let Some(plan) = plan.as_mut() {
                            plan.record_download_from(
                                &logical.prefetch,
                                key,
                                logical.compressed_bytes,
                            );
                        }
                    }
                    self.prefetch_stats
                        .downloads_completed
                        .fetch_add(batch.len() as u64, Ordering::Relaxed);
                    tracing::info!(
                        entries = batch.len(),
                        original_bytes,
                        extract_ms,
                        import_ms = import_start.elapsed().as_millis() as u64,
                        "packed-prefetch batch imported"
                    );
                }
                Err(error) => {
                    tracing::warn!("packed-prefetch batch import failed: {error:#}");
                    self.prefetch_stats
                        .pack_validation_failures
                        .fetch_add(1, Ordering::Relaxed);
                    for (entry, _, pack_index, entry_index) in &verified {
                        receipts[*pack_index].accounting().entries[*entry_index].outcome =
                            "import_error".to_owned();
                        let _ =
                            std::fs::remove_dir_all(self.entry_dir_for(&entry.restored.cache_key));
                    }
                }
            }
        }
        for receipt in &mut receipts {
            // A body that failed whole-pack validation has no trustworthy entries.
            if receipt.event.outcome == "validation_error" {
                continue;
            }
            let failed = receipt.accounting().entries.iter().any(|entry| {
                entry.outcome == "validation_error" || entry.outcome == "import_error"
            });
            receipt.finish(if failed { "import_error" } else { "completed" });
        }
        drop(receipts);
        for (entry, _, _, _) in &verified {
            if imported.contains(&entry.restored.cache_key) {
                self.note_key_present(&entry.restored.cache_key, &entry.restored.meta.crate_name)
                    .await;
            }
        }
        drop(verified);

        if !imported.is_empty() {
            const MAX_PREFETCHED_KEYS: usize = 50_000;
            let mut prefetched = self.prefetched_keys.write().await;
            if prefetched.len().saturating_add(imported.len()) >= MAX_PREFETCHED_KEYS {
                prefetched.clear();
                self.prefetch_used_keys.write().await.clear();
            }
            prefetched.extend(imported.iter().cloned());
        }
        self.prefetch_stats.pack_fallback_entries.fetch_add(
            wanted.difference(&imported).count() as u64,
            Ordering::Relaxed,
        );
        imported
    }

    /// Own each independently scheduled task; receivers let coordinators
    /// enforce their concurrency cap without owning or detaching child handles.
    fn spawn_prefetch_task(
        &self,
        origin: PrefetchOrigin,
        task: impl Future<Output = ()> + Send + 'static,
    ) -> Option<tokio::sync::oneshot::Receiver<()>> {
        let mut tasks = self
            .prefetch_tasks
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        while tasks.try_join_next().is_some() {}
        if self.prefetch_stopping.load(Ordering::Acquire) {
            self.cancel_prefetch_plan(&origin);
            return None;
        }
        let (completed, receiver) = tokio::sync::oneshot::channel();
        let guard = PrefetchTaskGuard {
            origin: Some(origin),
            cancellations: self.prefetch_cancellations.clone(),
        };
        tasks.spawn(async move {
            task.await;
            guard.complete();
            let _ = completed.send(());
        });
        Some(receiver)
    }

    fn stop_prefetch_admission(&self) {
        self.prefetch_stopping.store(true, Ordering::Release);
    }

    fn record_rejected_prefetch_candidates(&self, remaining: impl Iterator) {
        self.prefetch_stats
            .keys_cancelled
            .fetch_add(1 + remaining.count() as u64, Ordering::Relaxed);
    }

    fn cancel_prefetch_plan(&self, origin: &PrefetchOrigin) {
        let mut slot = self.active_plan.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(plan) = slot.as_mut()
            && plan.session_id == origin.session_id
            && plan.plan_id == origin.plan_id
            && plan.plan_source == origin.source
        {
            plan.cancelled = true;
        }
    }

    /// Stop admission, drain, then abort and join actual tasks before the
    /// summary. Inline extraction and filesystem I/O cannot be preempted by
    /// Tokio; the timeout bounds async waiting, not stalled synchronous I/O.
    async fn finish_prefetch_shutdown(self: &Arc<Self>, timeout: Duration) -> bool {
        self.stop_prefetch_admission();
        let mut tasks = std::mem::take(
            &mut *self
                .prefetch_tasks
                .lock()
                .unwrap_or_else(|p| p.into_inner()),
        );
        let timed_out = tokio::time::timeout(timeout, async {
            while tasks.join_next().await.is_some() {}
        })
        .await
        .is_err();
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}

        // All producers have exited; persist receipts before the summary.
        let receipts_complete = self.finish_prefetch_receipts().await;
        let cancellations = std::mem::take(
            &mut *self
                .prefetch_cancellations
                .lock()
                .unwrap_or_else(|p| p.into_inner()),
        );
        for origin in &cancellations.origins {
            self.cancel_prefetch_plan(origin);
        }
        let plan = self
            .active_plan
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        if let Some(mut plan) = plan {
            let incomplete = !receipts_complete
                || timed_out
                || cancellations.overflowed
                || cancellations.origins.iter().any(|origin| {
                    plan.session_id == origin.session_id
                        && plan.plan_id == origin.plan_id
                        && plan.plan_source == origin.source
                });
            plan.cancelled |= incomplete;
            let daemon = self.clone();
            let reason = if timed_out {
                "shutdown_timeout"
            } else {
                "shutdown"
            };
            let writer = tokio::task::spawn_blocking(move || {
                daemon.emit_plan_summary(plan, reason, incomplete);
            });
            // Timing out does not cancel a blocking fsync. The runtime's
            // existing shutdown timeout remains the last resort for disk I/O.
            if tokio::time::timeout(Duration::from_secs(5), writer)
                .await
                .is_err()
            {
                tracing::warn!("shutdown summary write is still blocked on local I/O");
            }
        }
        timed_out
    }

    /// Handle a prefetch request through an owned background coordinator.
    pub async fn handle_prefetch(self: &Arc<Self>, req: &PrefetchRequest) -> Response {
        self.handle_prefetch_with_context(req, None, Instant::now())
            .await
    }

    async fn handle_prefetch_with_context(
        self: &Arc<Self>,
        req: &PrefetchRequest,
        pack_context: Option<PackPrefetchContext>,
        plan_started_at: Instant,
    ) -> Response {
        if self.prefetch_stopping.load(Ordering::Acquire) {
            if let Some(origin) = &req.origin {
                self.cancel_prefetch_plan(origin);
            }
            return Response::ok();
        }
        if !self.config.prefetch_enabled {
            tracing::debug!("prefetch request ignored: speculative prefetch disabled");
            return Response::ok();
        }
        let Some(remote) = &self.config.remote else {
            return Response::err("no remote configured");
        };

        let origin = req.origin.clone().unwrap_or_else(|| PrefetchOrigin {
            source: "unscoped".to_string(),
            ..PrefetchOrigin::default()
        });
        let init_deadline = RemoteDeadline::from_secs(self.config.remote_restore_timeout_secs);
        let v3_remote = match init_deadline
            .run("prefetch backend initialization", self.v3_remote())
            .await
        {
            Ok(v3_remote) => v3_remote,
            Err(error) => return Response::err(format!("remote backend init failed: {error:#}")),
        };
        let v3_remote = Arc::clone(v3_remote);
        let remote_cache: Arc<dyn crate::cache_remote::CacheRemote> = v3_remote.clone();
        let bytes_at_plan_start = self.prefetch_stats.bytes_downloaded.load(Ordering::Relaxed);

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
                let result = self
                    .list_warm_all_keys(remote_cache.as_ref(), origin.clone(), deadline)
                    .await;
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

        let candidate_sources = req.candidate_sources.clone();
        let ranks: HashMap<String, u64> = req
            .keys
            .iter()
            .enumerate()
            .rev()
            .map(|(rank, (key, _))| (key.clone(), rank as u64))
            .collect();

        // Spawn a single coordinator task with bounded concurrency
        let daemon = Arc::clone(self);
        let remote_config = (*remote).clone();
        let cancel_rx = self.prefetch_cancel.subscribe();
        self.spawn_prefetch_task(origin.clone(), async move {
            if daemon.prefetch_stopping.load(Ordering::Acquire) {
                daemon.cancel_prefetch_plan(&origin);
                daemon
                    .prefetch_stats
                    .keys_cancelled
                    .fetch_add(keys_to_fetch.len() as u64, Ordering::Relaxed);
                return;
            }
            if let Some(context) = pack_context.as_ref() {
                let packed = daemon
                    .try_packed_prefetch(
                        context,
                        &v3_remote,
                        &remote_config,
                        &keys_to_fetch,
                        bytes_at_plan_start,
                        &PackedAttribution {
                            origin: &origin,
                            ranks: &ranks,
                            sources: &candidate_sources,
                        },
                    )
                    .await;
                daemon.flush_prefetch_receipts();
                keys_to_fetch.retain(|(key, _, _)| !packed.contains(key));
            }

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
            let bytes_at_start = bytes_at_plan_start;
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

                // Check adaptive cancellation and shutdown independently:
                // shutdown must not set the adaptive hit-rate latch.
                if daemon.prefetch_stopping.load(Ordering::Acquire) || *cancel_rx.borrow() {
                    daemon.cancel_prefetch_plan(&origin);
                    tracing::info!("prefetch: remaining candidates cancelled");
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
                    daemon.flush_prefetch_receipts();
                }

                let sem = daemon.s3_semaphore.clone();
                let d = daemon.clone();
                let remote_cache = remote_cache.clone();
                let download_plan = crate::remote_plan::RemotePlanner::new(&d.config)
                    .plan(crate::remote_plan::RemoteWorkload::Prefetch);
                let plan_deadline = deadline;
                let mut origin = origin.clone();
                origin.candidate_rank = ranks.get(&key).copied();
                origin.candidate_source = candidate_sources.get(&key).copied().unwrap_or_default();
                let task = daemon.spawn_prefetch_task(origin.clone(), async move {
                    if d.prefetch_stopping.load(Ordering::Acquire) {
                        d.cancel_prefetch_plan(&origin);
                        d.prefetch_stats
                            .keys_cancelled
                            .fetch_add(1, Ordering::Relaxed);
                        return;
                    }
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
                    let mut receipt = PrefetchReceipt::new(
                        d.prefetch_receipts.clone(),
                        origin.clone(),
                        "",
                        download_plan.transfer_format(),
                        PrefetchOperation::Get,
                    );
                    receipt.event.cache_key = key.clone();
                    receipt.event.crate_name = crate_name.clone();
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
                            receipt.finish("error");
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
                            receipt.finish("error");
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
                        receipt.finish("skipped");
                        return;
                    }
                    // Released on every exit path below (including panic) by
                    // Drop, which also wakes anyone parked on this key.
                    let _dl_guard = DownloadingGuard::new(d.downloading.clone(), key.clone());
                    if d.prefetch_stopping.load(Ordering::Acquire) {
                        d.cancel_prefetch_plan(&origin);
                        d.prefetch_stats
                            .keys_cancelled
                            .fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                    // Re-check under the claim: a leader that landed the entry
                    // between the check above and this claim would otherwise be
                    // followed by a destructive re-extraction over a directory
                    // a wrapper may already be hardlinking out of.
                    if entry_dir.exists() {
                        receipt.finish("skipped");
                        return;
                    }
                    let blobs_dir = d.config.store_dir().join("blobs");
                    let started_at_unix_ms = receipt.event.started_at_unix_ms;
                    let start = receipt.started;
                    receipt.event.semaphore_wait_ms = semaphore_wait_ms;
                    let mut observer = PrefetchBackendObserver {
                        receipt: &mut receipt,
                        stats: &d.prefetch_stats,
                        transfers: &d.transfer_counters,
                        network_started: None,
                    };
                    let download_result = item_deadline
                        .run(
                            "prefetch GET and extraction",
                            remote_cache.download_entry_observed(
                                &key,
                                &crate_name,
                                &entry_dir,
                                &blobs_dir,
                                item_deadline.at(),
                                &mut observer,
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
                            let (import_result, import_lock_wait_ms, import_ms) =
                                d.with_store_timed(|store| store.import_restored_entry(&key));
                            let import_ok = match import_result {
                                Ok(()) => true,
                                Err(e) => {
                                    tracing::warn!("prefetch import failed for {}: {e:#}", key);
                                    false
                                }
                            };
                            let elapsed_ms = start.elapsed().as_millis() as u64;
                            let finished_at_unix_ms = unix_time_ms();
                            if import_ok {
                                d.transfer_counters
                                    .downloads_completed
                                    .fetch_add(1, Ordering::Relaxed);
                                d.prefetch_stats
                                    .downloads_completed
                                    .fetch_add(1, Ordering::Relaxed);
                            } else {
                                d.transfer_counters
                                    .downloads_failed
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                            // Per-plan attribution (#583 P0.5): byte-accurate
                            // downloaded set for the session summary.
                            if import_ok {
                                let mut plan =
                                    d.active_plan.lock().unwrap_or_else(|p| p.into_inner());
                                if let Some(p) = plan.as_mut() {
                                    p.record_download_from(&origin, &key, dl.compressed_bytes);
                                }
                            }
                            receipt.event = TransferEvent {
                                accounting: receipt.event.accounting.take(),
                                prefetch: Some(origin.clone()),
                                outcome: if import_ok {
                                    "completed"
                                } else {
                                    "import_error"
                                }
                                .to_string(),
                                schema: default_transfer_schema(),
                                crate_name: crate_name.clone(),
                                direction: TransferDirection::Download,
                                format: dl.format.to_string(),
                                cache_key: key.clone(),
                                object_key: dl.object_key,
                                compressed_bytes: dl.compressed_bytes,
                                started_at_unix_ms,
                                finished_at_unix_ms,
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
                                import_lock_wait_ms,
                                import_ms,
                                compression_ms: 0,
                                head_checks_ms: 0,
                                blobs_skipped: dl.blobs_skipped,
                                blobs_total: dl.blobs_total,
                                ok: import_ok,
                                timestamp: 0,
                            };
                            if !import_ok {
                                return;
                            }
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
                            receipt.finish(if class == RemoteErrorClass::Miss {
                                "not_found"
                            } else {
                                "error"
                            });
                            if class == RemoteErrorClass::Miss {
                                breaker_permit.success();
                                if d.negative_keys.record_miss(&knowledge) {
                                    d.key_cache.remove(&key).await;
                                }
                            } else {
                                tracing::warn!("prefetch download failed for {}: {e}", key);
                                breaker_permit.failure(
                                    class,
                                    &format!("prefetch download failed ({class:?}): {e:#}"),
                                );
                                d.transfer_counters
                                    .downloads_failed
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                });
                if let Some(task) = task {
                    in_flight.push(task);
                } else {
                    daemon.record_rejected_prefetch_candidates(keys_iter);
                    break;
                }
            }

            // Drain remaining
            use futures::StreamExt;
            while in_flight.next().await.is_some() {
                daemon.flush_prefetch_receipts();
            }
            let wall_ms = plan_started_at.elapsed().as_millis() as u64;
            daemon
                .prefetch_stats
                .last_plan_wall_ms
                .store(wall_ms, Ordering::Relaxed);
            daemon
                .prefetch_stats
                .plan_wall_ms_total
                .fetch_add(wall_ms, Ordering::Relaxed);
            tracing::info!(wall_ms, "prefetch: completed {} downloads", count);
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
        identity_key: Option<String>,
    ) {
        if self.prefetch_stopping.load(Ordering::Acquire) {
            return;
        }
        let _ = self.prefetch_cancel.send(false);
        let candidates: HashSet<String> = candidates.collect();
        let mut plan = ActivePlan::new(
            session_id.to_string(),
            plan_id.to_string(),
            plan_source,
            candidates,
            self.prefetch_stats
                .list_requests_total
                .load(Ordering::Relaxed),
            self.prefetch_stats
                .list_duration_ms_total
                .load(Ordering::Relaxed),
        );
        plan.identity_key = identity_key;
        let prev = {
            let mut slot = self.active_plan.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(existing) = slot.as_mut()
                && existing.session_id == session_id
            {
                existing.plan_id = plan.plan_id;
                existing.plan_source = plan.plan_source;
                existing.candidates = plan.candidates;
                existing.cancelled = false;
                existing.last_activity_ms = plan.last_activity_ms;
                if plan.identity_key.is_some() {
                    existing.identity_key = plan.identity_key;
                }
                return;
            }
            slot.replace(plan)
        };
        if let Some(prev) = prev {
            self.emit_plan_summary(prev, "superseded", false);
        }
    }

    /// Start tracking a build even when planning produces no candidates. The
    /// session still owns the exact identity needed to publish its completed
    /// events for the next cold build.
    fn ensure_active_session(&self, req: &BuildStartedRequest) {
        if req.session_id.trim().is_empty() || self.prefetch_stopping.load(Ordering::Acquire) {
            return;
        }
        let mut plan = ActivePlan::new(
            req.session_id.clone(),
            String::new(),
            "none",
            HashSet::new(),
            self.prefetch_stats
                .list_requests_total
                .load(Ordering::Relaxed),
            self.prefetch_stats
                .list_duration_ms_total
                .load(Ordering::Relaxed),
        );
        plan.identity_key = req.intent.identity_key.clone();
        let prev = {
            let mut slot = self.active_plan.lock().unwrap_or_else(|p| p.into_inner());
            match slot.as_mut() {
                Some(existing) if existing.session_id == req.session_id => {
                    if plan.identity_key.is_some() {
                        existing.identity_key = plan.identity_key;
                    }
                    existing.last_activity_ms = epoch_ms();
                    return;
                }
                _ => slot.replace(plan),
            }
        };
        if let Some(prev) = prev {
            self.emit_plan_summary(prev, "superseded", false);
        }
    }

    /// Finalize the active plan if it has been inactive for `inactivity_ms`.
    /// Called from the periodic sweep; cargo gives no positive end-of-build
    /// signal, so inactivity IS the end signal (#583 P0.5).
    pub(crate) fn finalize_inactive_plan(&self, inactivity_ms: u64) {
        if self.prefetch_stopping.load(Ordering::Acquire) {
            return;
        }
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
            self.emit_plan_summary(prev, "inactivity", false);
        }
    }

    /// Normal closure is a conservative snapshot, not a per-origin barrier.
    /// Complete precision qualification requires the drained shutdown summary.
    fn prefetch_accounting_incomplete(&self) -> bool {
        let tasks_pending = {
            let mut tasks = self
                .prefetch_tasks
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            while tasks.try_join_next().is_some() {}
            !tasks.is_empty()
        };
        let receipts_pending = {
            let queue = self
                .prefetch_receipts
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            queue.overflowed
                || queue.writing
                || !queue.events.is_empty()
                || queue.active_receipts > 0
        };
        let cancelled = {
            let cancellations = self
                .prefetch_cancellations
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            cancellations.overflowed || !cancellations.origins.is_empty()
        };
        tasks_pending
            || receipts_pending
            || cancelled
            || self.prefetch_receipt_failed.load(Ordering::Acquire)
    }

    /// Append the per-session summary to `summaries.jsonl`. Best-effort:
    /// telemetry must never fail the daemon.
    fn emit_plan_summary(&self, plan: ActivePlan, closure_reason: &str, incomplete: bool) {
        let incomplete = incomplete || self.prefetch_accounting_incomplete();
        let used_bytes = plan.used_bytes();
        let downloaded_bytes: u64 = plan.downloaded.values().sum();
        let event = crate::events::BuildSummaryEvent {
            ts: chrono::Utc::now(),
            schema: 2,
            incomplete,
            session_id: plan.session_id.clone(),
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
        let shutdown = matches!(closure_reason, "shutdown" | "shutdown_timeout");
        let logged = if shutdown {
            crate::events::log_summary_durable(&path, &event)
        } else {
            crate::events::log_summary(&path, &event)
        };
        if let Err(e) = logged {
            tracing::debug!("failed to write build summary: {e}");
        }
        if shutdown {
            return;
        }
        let _ = self.maybe_publish_identity_manifest(
            plan.identity_key.as_deref(),
            plan.session_id.as_str(),
        );
    }

    /// Best-effort rank-0 publish when a session ends. Failures must not
    /// affect the daemon: the next `save-manifest` still writes the same
    /// events.
    fn maybe_publish_identity_manifest(
        &self,
        identity_key: Option<&str>,
        session_id: &str,
    ) -> bool {
        if self.prefetch_stopping.load(Ordering::Acquire) {
            return false;
        }
        let Some((identity_key, session_id)) = identity_publish_context(
            identity_key,
            session_id,
            self.config.remote.is_some(),
            self.config.remote_readonly,
        ) else {
            return false;
        };
        let config = self.config.clone();
        let publish = move || {
            if let Err(error) =
                crate::cli::save_manifest_auto_for_session(&config, &identity_key, &session_id)
            {
                tracing::debug!("identity manifest auto-publish failed: {error:#}");
            }
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn_blocking(publish);
        } else {
            publish();
        }
        true
    }

    pub async fn handle_build_started(self: &Arc<Self>, req: &BuildStartedRequest) -> Response {
        self.handle_build_started_with_planner(
            req,
            crate::planner_client::resolve_prefetch_plan(&req.intent),
        )
        .await
    }

    async fn handle_build_started_with_planner<F>(
        self: &Arc<Self>,
        req: &BuildStartedRequest,
        planner_lookup: F,
    ) -> Response
    where
        F: Future<Output = Result<Option<PrefetchPlan>>>,
    {
        self.handle_build_started_with_planner_and_prefetch(
            req,
            planner_lookup,
            |daemon, prefetch_req, pack_context, plan_started_at| async move {
                daemon
                    .handle_prefetch_with_context(&prefetch_req, pack_context, plan_started_at)
                    .await
            },
        )
        .await
    }

    async fn handle_build_started_with_planner_and_prefetch<F, E, EFut>(
        self: &Arc<Self>,
        req: &BuildStartedRequest,
        planner_lookup: F,
        execute_prefetch: E,
    ) -> Response
    where
        F: Future<Output = Result<Option<PrefetchPlan>>>,
        E: Fn(Arc<Self>, PrefetchRequest, Option<PackPrefetchContext>, Instant) -> EFut,
        EFut: Future<Output = Response>,
    {
        let plan_started_at = Instant::now();
        let pack_context = PackPrefetchContext::from_intent(&req.intent);
        let Some(_remote) = &self.config.remote else {
            return Response::err("no remote configured");
        };
        self.ensure_active_session(req);
        if speculative_prefetch_disabled(self.config.prefetch_enabled) {
            tracing::debug!("build-started: speculative prefetch disabled");
            return Response::ok();
        }

        // Identity resolution only reads the small manifest metadata. Start it
        // while the advisory planner is in flight, but do not start artifact
        // downloads until the planner disposition is known. Keep the identity
        // future owned so the selected advisory plan can cancel it before
        // taking artifact-prefetch capacity.
        let mut identity_lookup = Some(Box::pin(
            crate::fallback_planner::resolve_identity_candidates_speculative(self, &req.intent),
        ));
        tokio::pin!(planner_lookup);
        let mut early_identity_resolution = None;
        let planner_result = tokio::select! {
            resolution = identity_lookup
                .as_mut()
                .expect("identity lookup is present")
                .as_mut() => {
                early_identity_resolution = Some(resolution);
                planner_lookup.await
            }
            result = &mut planner_lookup => result,
        };
        if early_identity_resolution.is_some() {
            identity_lookup.take();
        }

        match planner_result {
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
                        // The speculative metadata GET may hold both a
                        // prefetch-gate permit and an S3 permit. Cancel it
                        // before artifact prefetch so lookahead cannot reduce
                        // the capacity available to the selected plan.
                        drop(identity_lookup.take());
                        let mut prefetch_req = PrefetchRequest::from_plan(plan);
                        prefetch_req.origin = Some(PrefetchOrigin {
                            session_id: req.session_id.clone(),
                            plan_id: plan_id.clone().unwrap_or_default(),
                            source: "advisory".to_string(),
                            candidate_rank: None,
                            candidate_source: kache_core::CandidateSource::Unknown,
                        });
                        let candidate_count = prefetch_req.keys.len();
                        self.install_plan(
                            &req.session_id,
                            plan_id.as_deref().unwrap_or(""),
                            "advisory",
                            prefetch_req.keys.iter().map(|(k, _)| k.clone()),
                            req.intent.identity_key.clone(),
                        );
                        let resp = execute_prefetch(
                            Arc::clone(self),
                            prefetch_req,
                            pack_context.clone(),
                            plan_started_at,
                        )
                        .await;
                        if resp.ok {
                            self.prefetch_stats
                                .plans_advisory
                                .fetch_add(1, Ordering::Relaxed);
                            self.prefetch_stats
                                .last_plan_candidates
                                .store(candidate_count as u64, Ordering::Relaxed);
                            tracing::info!(
                                plan_id = ?plan_id,
                                planner = ?planner,
                                candidate_count,
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
                        drop(identity_lookup.take());
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

        let resolved_identity = match early_identity_resolution {
            Some(resolution) => resolution,
            None => {
                // Fallback is now selected. Cancel unresolved speculative
                // work and retry its exact keys through ordinary demand
                // admission, including after an advisory execution failure.
                drop(identity_lookup.take());
                crate::fallback_planner::retry_identity_with_ordinary_admission(&req.intent)
            }
        };
        let fallback_plan = match crate::fallback_planner::build_prefetch_plan_with_identity(
            self,
            &req.intent,
            resolved_identity,
        )
        .await
        {
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

        let mut prefetch_req = PrefetchRequest::from_plan(fallback_plan);
        prefetch_req.origin = Some(PrefetchOrigin {
            session_id: req.session_id.clone(),
            plan_id: String::new(),
            source: "fallback".to_string(),
            candidate_rank: None,
            candidate_source: kache_core::CandidateSource::Unknown,
        });
        self.install_plan(
            &req.session_id,
            "",
            "fallback",
            prefetch_req.keys.iter().map(|(k, _)| k.clone()),
            req.intent.identity_key.clone(),
        );
        self.handle_prefetch_with_context(&prefetch_req, pack_context, plan_started_at)
            .await
    }

    /// After a successful upload: sweep if the store is under size pressure.
    fn maybe_evict_after_upload(&self) {
        let _ = self.sweep_under_size_pressure();
    }

    /// Claim the hinted sweep. False while one is queued or running: that
    /// sweep measures the store when it starts, so it covers this hint too.
    fn claim_gc_hint(&self) -> bool {
        !self.gc_hint_pending.swap(true, Ordering::SeqCst)
    }

    /// A wrapper's size-pressure hint: acknowledge now, sweep on the blocking
    /// pool (#281). One sweep, where the wrapper's own worker sweeps twice:
    /// that worker exits and has no later chance at entries a live build
    /// pins, while the daemon sweeps again on the next hint or upload after
    /// the backoff.
    fn handle_gc_hint(self: &Arc<Self>) -> Response {
        if self.claim_gc_hint() {
            let daemon = Arc::clone(self);
            tokio::task::spawn_blocking(move || daemon.run_hinted_sweep());
        }
        Response::ok()
    }

    fn run_hinted_sweep(&self) {
        // Released on unwind too, or one panic would swallow every later hint.
        struct Release<'a>(&'a AtomicBool);
        impl Drop for Release<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::SeqCst);
            }
        }
        let _release = Release(&self.gc_hint_pending);
        if let Err(e) = self.sweep_under_size_pressure() {
            tracing::warn!("hinted GC sweep failed: {e:#}");
        }
    }

    /// The sweep behind the post-upload check and wrapper hints. Skips when
    /// another driver holds `gc.lock`, the store is under the trigger, or the
    /// backoff holds; each costs a lock attempt and one size query.
    fn sweep_under_size_pressure(&self) -> Result<()> {
        let Some((_gc_lock, size)) = self.with_store(|store| {
            let Some(lock) = store.try_gc_lock()? else {
                return Ok(None);
            };
            // The size check is cheap; release the daemon's Store mutex
            // before the long eviction scan and per-entry removals.
            Ok(Some((lock, store.physical_size()?)))
        })?
        else {
            tracing::debug!("gc.lock held by another GC; skipping size-pressure eviction");
            return Ok(());
        };
        if !crate::wrapper::auto_gc_sweep_due(&self.config, size) {
            return Ok(());
        }
        // Under gc.lock like every driver, so the totals cannot race.
        let store = Store::open(&self.config)?;
        let stats = self.automatic_size_pass(&store, size)?;
        if let Err(e) = crate::report::record_gc_run(&self.config, "daemon", &stats) {
            tracing::warn!("recording size-pressure GC run: {e:#}");
        }
        Ok(())
    }

    /// The size pass of an automatic sweep the shared trigger found due.
    /// Records where the store ended, so a sweep that could not clear the
    /// pressure backs off every automatic driver. Caller holds `gc.lock`.
    fn automatic_size_pass(&self, store: &Store, size: u64) -> Result<crate::store::GcStats> {
        tracing::info!(
            "store size {} over the automatic trigger (max {}), running LRU eviction",
            size,
            self.config.max_size
        );
        let started = Instant::now();
        let mut stats = store.evict_for(crate::store::SweepOrigin::Automatic)?;
        stats.duration_ms = started.elapsed().as_millis() as u64;
        if let Ok(after) = store.physical_size() {
            crate::wrapper::record_auto_gc_outcome(&self.config, after, stats.bytes_held);
        }
        Ok(stats)
    }

    /// The size pass of a full sweep. A requested `kache gc` always runs it
    /// and leaves the backoff alone. The timer runs it only when the shared
    /// trigger says a sweep is due; its age and duplicate passes are not
    /// size pressure and stay on schedule.
    fn size_pass(&self, driver: GcDriver, store: &Store) -> Result<crate::store::GcStats> {
        if driver == GcDriver::Requested {
            return store.evict();
        }
        let size = store.physical_size()?;
        if !crate::wrapper::auto_gc_sweep_due(&self.config, size) {
            tracing::info!("periodic GC: size pass not due (under the trigger or backing off)");
            return Ok(crate::store::GcStats::default());
        }
        self.automatic_size_pass(store, size)
    }

    /// Core GC logic with an explicit policy and per-policy result accounting.
    fn run_gc(&self, policy: GcPolicy, driver: GcDriver) -> Result<GcRunReport> {
        let start = Instant::now();
        let mode = policy.mode();
        // Cross-process GC mutual exclusion (kunobi-ninja/kache#326): if another
        // GC driver (a manual `kache gc`, a second daemon) holds gc.lock, skip
        // this run rather than double-scan and contend. Held until run_gc returns.
        // GC may scan and remove thousands of entries. Use its own connection
        // so daemon lookups and uploads can still reach the main Store mutex.
        // SQLite and gc.lock continue to serialize the actual writes.
        let gc_store = Store::open(&self.config)?;
        let _gc_lock = match gc_store.try_gc_lock()? {
            Some(lock) => lock,
            None => {
                tracing::info!("gc.lock held by another GC; skipping this run");
                return Ok(GcRunReport::skipped(mode));
            }
        };
        let (dedup_stats, evict_stats, age_evict_stats, incremental_cleaned, orphan_stats) =
            (|| -> Result<_> {
                let store = &gc_store;
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

                match store.file_hash_cache().prune_cc_preprocess_memos() {
                    Ok((memos, inputs)) => tracing::debug!(memos, inputs, "gc: pruned C/C++ memos"),
                    Err(error) => tracing::warn!("gc: C/C++ memo pruning failed: {error}"),
                }
                let markers = crate::wrapper::prune_session_markers(
                    &self.config,
                    crate::wrapper::SESSION_MARKER_RETENTION,
                    std::time::SystemTime::now(),
                );
                tracing::debug!(markers, "gc: pruned build-session markers");

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
                        let duplicate_stats = store
                            .evict_duplicate_entries_for(driver.sweep_origin())
                            .unwrap_or_default();
                        let size_stats = self.size_pass(driver, store)?;
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
                // remove_entry that couldn't decrement refcounts). The grace
                // leaves blobs a concurrent build is materializing untouched; they
                // get reclaimed on a later pass once settled.
                let orphan_stats = store
                    .sweep_orphan_blobs(ORPHAN_BLOB_GRACE)
                    .unwrap_or_default();
                // Same grace for put-phase staging snapshots abandoned by a
                // crash between staging and publish (review finding #3).
                let staging_stats = store.sweep_stale_staging(crate::store::STAGING_SWEEP_GRACE);
                // Handoff request directories the staging sweep does not
                // enter: a daemon that died with publications queued.
                let handoffs = crate::daemon_publish::sweep_orphaned_handoffs(
                    &self.config,
                    crate::store::STAGING_SWEEP_GRACE,
                );
                if handoffs.removed > 0 {
                    tracing::info!(
                        "swept {} abandoned cc handoffs ({})",
                        handoffs.removed,
                        crate::report::format_bytes(handoffs.bytes_reclaimed)
                    );
                }
                if staging_stats.removed > 0 {
                    tracing::info!(
                        "swept {} stale staging files ({})",
                        staging_stats.removed,
                        crate::report::format_bytes(staging_stats.bytes_reclaimed)
                    );
                }
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
            })()?;

        // Clean up stale tool-version cache files (rustc-ver-*.txt, linker-ver-*.txt).
        // Each toolchain update leaves behind orphaned files keyed by the old binary mtime.
        Self::clean_tool_version_caches(&self.config.cache_dir);

        // Key lock files and input predictions grow with every distinct key
        // and eviction removes neither (#1126). Still under gc.lock.
        let housekeeping = gc_store.sweep_housekeeping();
        tracing::info!(
            key_locks_removed = housekeeping.key_locks_removed,
            key_locks_remaining = housekeeping.key_locks_remaining,
            predictions_pruned = housekeeping.predictions_pruned,
            file_hashes_pruned = housekeeping.file_hashes_pruned,
            "gc: housekeeping"
        );

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
            entries_pinned: gc_entries_pinned_lower_bound(
                policy,
                dedup_stats.entries_pinned,
                age_evict_stats.entries_pinned,
                evict_stats.entries_pinned,
            ),
            entries_unreclaimable: dedup_stats.entries_unreclaimable
                + evict_stats.entries_unreclaimable
                + age_evict_stats.entries_unreclaimable,
            // Only the size pass measures it.
            bytes_held: evict_stats.bytes_held,
            disk_bytes_reclaimed: dedup_stats.disk_bytes_reclaimed
                + evict_stats.disk_bytes_reclaimed
                + age_evict_stats.disk_bytes_reclaimed,
            entries_failed: dedup_stats.entries_failed
                + evict_stats.entries_failed
                + age_evict_stats.entries_failed,
            entries_locked: dedup_stats.entries_locked
                + evict_stats.entries_locked
                + age_evict_stats.entries_locked,
            entries_busy_snapshot: dedup_stats.entries_busy_snapshot
                + evict_stats.entries_busy_snapshot
                + age_evict_stats.entries_busy_snapshot,
            entries_recent_prefiltered: dedup_stats.entries_recent_prefiltered
                + evict_stats.entries_recent_prefiltered
                + age_evict_stats.entries_recent_prefiltered,
            entries_import_pinned: dedup_stats.entries_import_pinned
                + evict_stats.entries_import_pinned
                + age_evict_stats.entries_import_pinned,
            evict_write_ms: dedup_stats.evict_write_ms
                + evict_stats.evict_write_ms
                + age_evict_stats.evict_write_ms,
            housekeeping: Some(housekeeping),
        };

        tracing::info!(
            "gc complete: {} entries evicted, {} freed, {} blobs removed in {}ms",
            stats.entries_evicted,
            crate::report::format_bytes(stats.bytes_freed),
            stats.blobs_removed,
            stats.duration_ms,
        );

        // Persist GC stats for reports and machine telemetry. Still under
        // gc.lock, so the record cannot race another driver.
        if let Err(e) = crate::report::record_gc_run(&self.config, "daemon", &stats) {
            tracing::debug!(
                "gc: could not record {}: {e:#}",
                crate::report::GC_STATS_FILE
            );
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

    let Some(_lock) =
        kunobi_daemon::ProcessLock::try_acquire(&lock_path).context("acquiring daemon run lock")?
    else {
        tracing::info!("another daemon holds the run lock, exiting");
        return Ok(());
    };
    let coord = DaemonCoordFile::for_socket(&socket_path);
    coord
        .write_phase(DaemonPhase::Starting)
        .context("writing daemon coordinator state")?;
    let _coord_guard = DaemonCoordGuard::new(coord.path.clone());

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    run_daemon_runtime(rt, server_main(config, provenance, coord))
}

fn run_daemon_runtime(
    runtime: tokio::runtime::Runtime,
    server: impl std::future::Future<Output = Result<()>>,
) -> Result<()> {
    let result = runtime.block_on(server);
    // The server has already drained handlers and durable uploads. Aborting
    // GC or migration does not cancel its spawn_blocking work: dropping the
    // runtime would wait forever and keep the daemon run lock held. This is
    // the foreground daemon's exit path; the process ends after we return.
    runtime.shutdown_timeout(Duration::from_secs(1));
    result
}

fn start_manifest_warming(daemon: &Arc<Daemon>) -> Option<tokio::task::JoinHandle<()>> {
    if should_start_speculative_prefetch(
        daemon.config.remote.is_some(),
        daemon.config.prefetch_enabled,
    ) {
        let manifest_daemon = daemon.clone();
        let namespace = std::env::var("KACHE_NAMESPACE").ok();
        let lock_path = PathBuf::from("Cargo.lock");
        Some(tokio::spawn(async move {
            manifest_prefetch(&manifest_daemon, namespace.as_deref(), &lock_path).await;
            manifest_daemon.signal_warming_complete();
        }))
    } else {
        // No warming task will run when no remote exists or speculation is
        // disabled, so release exact remote checks immediately.
        daemon.signal_warming_complete();
        None
    }
}

fn upload_result_is_terminal(error: Option<&str>) -> bool {
    !error.is_some_and(|error| error.starts_with("retryable:"))
}

fn daemon_idle_timeout(seconds: u64) -> Option<Duration> {
    std::num::NonZeroU64::new(seconds).map(|seconds| Duration::from_secs(seconds.get()))
}

async fn server_main(
    config: &Config,
    provenance: &crate::config::ConfigFileProvenance,
    mut coord: DaemonCoordFile,
) -> Result<()> {
    let socket_path = config.socket_path();
    std::fs::create_dir_all(socket_path.parent().unwrap())?;

    let Some(listener) = crate::transport::bind_daemon_listener(&socket_path).await? else {
        tracing::info!("another daemon owns the endpoint");
        return Ok(());
    };
    let _socket_guard = SocketCleanupGuard::new(&socket_path)?;

    let lifecycle = Arc::new(Lifecycle::default());
    let mut control = lifecycle_control::serve(config, Arc::clone(&lifecycle)).await?;
    coord.control_version = Some(kunobi_daemon::wire::VERSION);
    coord.write_phase(DaemonPhase::Starting)?;

    let daemon = Arc::new(Daemon::new_with_provenance(config.clone(), provenance));

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

    daemon.set_upload_tx(buffer_tx.clone());

    match load_upload_jobs(config) {
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
            tracing::info!(replay_count, "durable upload replay scan complete");
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
                    if upload_result_is_terminal(response.error.as_deref()) {
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

    // Publication of compiles handed off by cc wrappers: one worker thread
    // with its own store connection, fed by a bounded queue the handler
    // fills only after it holds the key lock (see `daemon_publish`).
    let (publish_tx, publish_rx) = tokio::sync::mpsc::channel::<crate::daemon_publish::PublishJob>(
        crate::daemon_publish::PUBLISH_QUEUE_CAPACITY,
    );
    daemon.publish_queue().set_sender(publish_tx);
    let publish_done = crate::daemon_publish::spawn_publish_worker(daemon.clone(), publish_rx)
        .context("starting the publish worker")?;

    // Periodic GC task: run immediately on startup, then every 6 hours
    let gc_daemon = daemon.clone();
    // Session-summary sweep (#583 P0.5): finalize an active prefetch plan
    // once its build session has gone quiet. 60s granularity against a
    // 5-minute inactivity window is plenty; the summary lands in
    // `summaries.jsonl` where `kache report` joins it with per-crate events.
    let sweep_daemon = daemon.clone();
    let sweep_handle = tokio::spawn(async move {
        const SESSION_INACTIVITY_MS: u64 = 300_000;
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            sweep_daemon.finalize_inactive_plan(SESSION_INACTIVITY_MS);
        }
    });

    // Entries a miss stored without an fsync (`cache.deferred_durability`).
    // Short interval: until an entry is flushed every hit on it re-reads its
    // blobs to verify them, and the wrapper hands the work here precisely so
    // the build does not wait for the disk.
    let durability_daemon = daemon.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let daemon = durability_daemon.clone();
            // Blocking: fsync per blob, off the async workers (#281).
            let _ = tokio::task::spawn_blocking(move || daemon.flush_pending_durability()).await;
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
                gc.run_gc(
                    GcPolicy::Automatic {
                        max_age_hours: gc.config.gc_max_age_hours,
                    },
                    GcDriver::Periodic,
                )
            })
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => tracing::warn!("periodic GC failed: {e}"),
                Err(e) => tracing::warn!("periodic GC task panicked: {e}"),
            }
        }
    });

    let maintenance_handle = config
        .index_auto_compact
        .then(|| crate::maintenance::spawn_periodic(config.clone(), daemon.request_clock.clone()));

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

    // Readiness is published only after application setup can enter the accept loop.
    control.service.mark_ready();
    coord.write_phase(DaemonPhase::Ready)?;
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

    // Config watchdog: the daemon loads its config once at startup, so an edit
    // to the config file (e.g. `local_max_size`) would otherwise require a
    // manual `kache daemon stop`. Periodically re-fingerprint the active config
    // file; on a change, schedule a graceful restart so the service manager (or
    // the next build's auto-spawn) brings the daemon back up with the new
    // config. This watches only the file the daemon itself resolved — it sends
    // no per-client signal, so it can't thrash across projects.
    let config_provenance = provenance.clone();
    let config_watch_lifecycle = Arc::clone(&lifecycle);

    let config_watch_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(DAEMON_CONFIG_WATCH_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = config_watch_lifecycle.draining() => break,
                _ = interval.tick() => {}
            }
            if crate::config::config_file_has_changed(&config_provenance) {
                tracing::info!("config file changed on disk, scheduling restart to reload it");
                config_watch_lifecycle.start_drain();

                break;
            }
        }
    });

    // Idle watchdog: exit if no connections received for this duration.
    // Prevents zombie daemons from accumulating when the user isn't building.
    // The daemon will be auto-started again on the next build.
    // Configurable via KACHE_DAEMON_IDLE_TIMEOUT or config.toml; 0 = disabled.
    let idle_timeout = daemon_idle_timeout(config.daemon_idle_timeout_secs);

    accept_loop(
        &listener,
        &daemon,
        &lifecycle,
        idle_timeout,
        CONNECTION_HANDLER_DRAIN_TIMEOUT,
        shutdown_signal(),
    )
    .await;

    gc_handle.abort();
    if let Some(h) = maintenance_handle {
        h.abort();
    }
    // Stop producers before draining the tasks and taking the final counters.
    let producers = [Some(sweep_handle), cache_handle, manifest_handle];
    for handle in producers.iter().flatten() {
        handle.abort();
    }
    for handle in producers.into_iter().flatten() {
        let _ = handle.await;
    }
    if daemon
        .finish_prefetch_shutdown(Duration::from_secs(5))
        .await
    {
        tracing::warn!("prefetch drain timed out; remaining tasks aborted and joined");
    }
    heartbeat_handle.abort();
    config_watch_handle.abort();

    // Graceful shutdown: drop the daemon's sender to close the unbounded buffer,
    // then give the entire enqueue + worker drain one shared 30s budget. The
    // enqueue task itself can block on a full worker channel during an outage,
    // so awaiting it outside this deadline would make restart unbounded even
    // though every queued job is already durable on disk.
    daemon.close_upload_queue();
    // Accepted hand-offs hold their key locks; give the worker the same
    // budget to drain them. A job it never reaches is a lost store, not a
    // lost build.
    daemon.publish_queue().close();
    drop(daemon);
    if tokio::time::timeout(Duration::from_secs(30), publish_done)
        .await
        .is_err()
    {
        tracing::warn!("publish drain timeout; queued hand-offs were not stored");
    }
    if drain_upload_pipeline(enqueue_handle, upload_handles, Duration::from_secs(30)).await {
        tracing::warn!("upload drain timeout, aborting remaining upload tasks");
    }

    // Handlers and uploads are done, so the index is as idle as this daemon
    // will see it. Quiet rules only: a contended index yields at once, and a
    // large store or live index is skipped, so shutdown stays short.
    if config.index_auto_compact {
        crate::maintenance::run_at_shutdown(config.clone()).await;
    }

    control.finish().await;
    // Socket file is cleaned up by `_socket_guard` (Drop).
    tracing::info!("daemon stopped");
    Ok(())
}

/// Drain the enqueue task and workers under one deadline. Returns true when
/// the deadline fired; all unfinished tasks are aborted because their jobs are
/// already represented by durable spool intents and will replay after restart.
async fn drain_upload_pipeline(
    mut enqueue_handle: tokio::task::JoinHandle<()>,
    mut upload_handles: Vec<tokio::task::JoinHandle<()>>,
    timeout: Duration,
) -> bool {
    let drain_deadline = tokio::time::sleep(timeout);
    tokio::pin!(drain_deadline);
    let mut timed_out = false;

    tokio::select! {
        _ = &mut enqueue_handle => {}
        _ = &mut drain_deadline => {
            timed_out = true;
        }
    }

    if !timed_out {
        for handle in &mut upload_handles {
            tokio::select! {
                _ = handle => {}
                _ = &mut drain_deadline => {
                    timed_out = true;
                    break;
                }
            }
        }
    }

    enqueue_handle.abort();
    for handle in upload_handles {
        handle.abort();
    }
    timed_out
}

/// Periodic wake interval for the accept loop. The loop is otherwise only woken
/// by an incoming connection, a shared drain notification, or the OS shutdown
/// signal; this tick guarantees the idle-timeout check still runs when the
/// daemon is completely quiet.
const ACCEPT_LOOP_IDLE_TICK: Duration = Duration::from_secs(60);

/// Maximum time to let accepted IPC handlers finish their current response
/// during shutdown. A bounded drain preserves in-flight replies (including the
/// shutdown acknowledgement) without letting a silent client hold the daemon
/// open forever.
const CONNECTION_HANDLER_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Overall budget a `RemoteCheck` waits behind another task's in-flight
/// download of the same key before giving up and reporting a remote miss
/// (never a second, unclaimed download — see [`JoinOutcome::GaveUp`]).
const DOWNLOAD_JOIN_BUDGET: Duration = Duration::from_secs(30);

fn download_join_deadline(
    now: tokio::time::Instant,
    overall: Option<tokio::time::Instant>,
) -> tokio::time::Instant {
    let join_budget = now
        .checked_add(DOWNLOAD_JOIN_BUDGET)
        .expect("download join budget fits monotonic time");
    overall.map_or(join_budget, |overall| overall.min(join_budget))
}

/// Outcome of waiting behind another task's in-flight download of a key.
#[derive(Debug, PartialEq, Eq)]
enum JoinOutcome {
    /// The leader left `meta.json`; the caller must verify committed Store state.
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

/// Claim a packed entry only when it is absent on disk and no other download
/// already owns the key. Keeping both rejection cases behind this seam makes
/// the short-circuit contract deterministic to test.
async fn try_claim_packed_download(
    downloading: &RwLock<HashMap<String, Arc<Notify>>>,
    key: &str,
    entry_dir: &Path,
) -> bool {
    if entry_dir.exists() {
        return false;
    }
    claim_download(downloading, key).await.is_none()
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

/// Accept until the shared lifecycle closes admission, an idle budget expires,
/// or the OS requests shutdown. Per-request guards include response delivery.
async fn accept_loop(
    listener: &TokioListener,
    daemon: &Arc<Daemon>,
    lifecycle: &Arc<Lifecycle>,
    idle_timeout: Option<Duration>,
    handler_drain_timeout: Duration,
    shutdown_signal: impl std::future::Future<Output = ()>,
) {
    tokio::pin!(shutdown_signal);
    let mut last_activity = Instant::now();
    let mut handlers = tokio::task::JoinSet::new();

    // Bound the number of connection handlers doing work at once. Excess
    // connections park on `acquire_owned` (cheap) instead of all running
    // concurrently, so a burst of local clients can't pile up active handlers.
    const MAX_CONCURRENT_CONNECTIONS: usize = 128;
    let conn_limiter = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_CONNECTIONS));

    loop {
        if !lifecycle.accepting_calls() {
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
                        let flag = lifecycle.clone();

                        let limiter = conn_limiter.clone();
                        handlers.spawn(async move {
                            if let Err(e) = handle_connection_after_queue(
                                stream,
                                &d,
                                &flag,
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
            // Shared drain notification also wakes an otherwise idle listener.
            _ = lifecycle.draining() => {}
            // Wake periodically to check idle timeout (select won't fire otherwise)
            _ = tokio::time::sleep(ACCEPT_LOOP_IDLE_TICK) => {}
            _ = &mut shutdown_signal => {
                tracing::info!("shutdown signal received, draining...");
                break;
            }
            Some(result) = handlers.join_next(), if !handlers.is_empty() => {
                observe_connection_handler(result);
            }
        }
    }

    // Every accepted connection is owned by this loop. Tell persistent
    // handlers to stop after their current response, then wait for those
    // responses under a deadline. This must happen before `server_main` drops
    // the runtime and starts draining uploads.
    lifecycle.start_drain();
    daemon.stop_prefetch_admission();
    let drain_deadline = tokio::time::Instant::now() + handler_drain_timeout;
    let _ = lifecycle.drain_until(drain_deadline).await;
    if drain_connection_handlers(
        &mut handlers,
        drain_deadline.saturating_duration_since(tokio::time::Instant::now()),
    )
    .await
    {
        tracing::warn!(
            timeout_ms = handler_drain_timeout.as_millis() as u64,
            "connection handler drain timed out; aborted remaining handlers"
        );
    }
}

fn observe_connection_handler(result: std::result::Result<(), tokio::task::JoinError>) -> bool {
    if let Err(error) = result
        && !error.is_cancelled()
    {
        tracing::warn!("connection handler task failed: {error}");
        return true;
    }
    false
}

/// Drain all accepted connection handlers under one deadline. Returns true if
/// unfinished handlers had to be aborted.
async fn drain_connection_handlers(
    handlers: &mut tokio::task::JoinSet<()>,
    timeout: Duration,
) -> bool {
    let drained = tokio::time::timeout(timeout, async {
        while let Some(result) = handlers.join_next().await {
            observe_connection_handler(result);
        }
    })
    .await
    .is_ok();

    if drained {
        return false;
    }

    handlers.abort_all();
    while let Some(result) = handlers.join_next().await {
        observe_connection_handler(result);
    }
    true
}

/// Populate the key cache by listing every key in the remote.
async fn populate_key_cache(daemon: &Arc<Daemon>) -> Result<usize> {
    let mut receipt = PrefetchReceipt::new(
        daemon.prefetch_receipts.clone(),
        daemon.background_prefetch_origin(),
        "",
        "v3",
        PrefetchOperation::List,
    );
    let result = populate_key_cache_observed(daemon, &mut receipt).await;
    finish_open_prefetch_receipt(&mut receipt, result.is_ok());
    drop(receipt);
    daemon.flush_prefetch_receipts();
    result
}

async fn populate_key_cache_observed(
    daemon: &Daemon,
    receipt: &mut PrefetchReceipt,
) -> Result<usize> {
    daemon
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
    let remote_cache = match deadline
        .run("index backend initialization", daemon.cache_remote())
        .await
    {
        Ok(remote_cache) => remote_cache,
        Err(error) => {
            let class = classify_remote_error(&error);
            breaker_permit.failure(class, &format!("{error:#}"));
            return Err(error);
        }
    };
    let listing_epoch = daemon.negative_keys.listing_epoch();
    let key_cache_revision = daemon.key_cache.refresh_revision();

    let list_start = Instant::now();
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
    anyhow::ensure!(
        !daemon.prefetch_stopping.load(Ordering::Acquire),
        "daemon stopping before index LIST"
    );
    receipt.event.semaphore_wait_ms = list_start.elapsed().as_millis() as u64;
    let list_result = deadline
        .run(
            "index LIST",
            remote_cache.list_keys_observed(&mut PrefetchBackendObserver {
                receipt,
                stats: &daemon.prefetch_stats,
                transfers: &daemon.transfer_counters,
                network_started: None,
            }),
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
    let _ = daemon
        .key_cache
        .populate_if_unchanged(keys, key_cache_revision)
        .await;
    Ok(count)
}

/// Download recorded actions, then lockfile shards if those were missing.
///
/// Rank 0 is the identity manifest (lock + target + profile), with the
/// legacy host-triple key as a fallback. Rank 1 is content-addressed
/// shards for a cold first run of this command.
async fn manifest_prefetch(
    daemon: &Arc<Daemon>,
    namespace: Option<&str>,
    lock_path: &Path,
) -> usize {
    let Some(_) = &daemon.config.remote else {
        return 0;
    };

    let initialization_deadline =
        RemoteDeadline::from_secs(daemon.config.remote_restore_timeout_secs);
    let v3 = match initialization_deadline
        .run(
            "startup prefetch backend initialization",
            daemon.v3_remote(),
        )
        .await
    {
        Ok(v3) => v3,
        Err(e) => {
            tracing::warn!("manifest prefetch: remote backend init failed: {e}");
            return 0;
        }
    };

    let identity = crate::identity::profile_from_env().and_then(|profile| {
        crate::identity::identity_key(lock_path, &crate::identity::host_target_triple(), &profile)
    });
    let from_identity = identity_manifest_prefetch(daemon, identity.as_deref()).await;
    if identity_prefetch_satisfied(from_identity) {
        return from_identity;
    }

    if let Some(namespace) = namespace {
        if lock_path.exists() {
            match shard_prefetch(daemon, v3, namespace, lock_path).await {
                Ok(n) => {
                    tracing::info!("shard prefetch: queued {n} keys from shards");
                    return n;
                }
                Err(e) => {
                    tracing::warn!("shard prefetch failed: {e}");
                }
            }
        } else {
            tracing::info!("KACHE_NAMESPACE set but no Cargo.lock found");
        }
    }

    0
}

fn identity_prefetch_satisfied(count: usize) -> bool {
    count > 0
}

/// Shard-based prefetch: compute shard hashes from Cargo.lock, download matching shards
/// from the remote in parallel, collect cache keys.
async fn shard_prefetch(
    daemon: &Arc<Daemon>,
    v3: &Arc<crate::cache_remote::V3Remote>,
    namespace: &str,
    lock_path: &std::path::Path,
) -> anyhow::Result<usize> {
    let deps = crate::shards::parse_cargo_lock(lock_path)?;
    shard_prefetch_for_deps(daemon, v3, namespace, &deps).await
}

async fn shard_prefetch_for_deps(
    daemon: &Arc<Daemon>,
    v3: &Arc<crate::cache_remote::V3Remote>,
    namespace: &str,
    deps: &[(String, String)],
) -> anyhow::Result<usize> {
    let plan_started_at = Instant::now();
    let shard_set = crate::shards::compute_shards(namespace, deps);

    tracing::info!(
        "shard prefetch: {} deps -> {} shards for namespace '{namespace}'",
        deps.len(),
        shard_set.shards.len()
    );

    // Download all shards in parallel
    let mut handles = Vec::new();
    for (hash, _entries) in &shard_set.shards {
        let v = Arc::clone(v3);
        let d = Arc::clone(daemon);
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
            let result = deadline.run("shard GET", v.get_shard(&ns, &h)).await;
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
        origin: None,
        candidate_sources: HashMap::new(),
    };
    let manifest_key = crate::identity::manifest_lookup_keys(None)
        .into_iter()
        .next()
        .unwrap_or_else(crate::identity::host_target_triple);
    let pack_context = PackPrefetchContext::from_deps(manifest_key, namespace, deps).ok();
    let resp = daemon
        .handle_prefetch_with_context(&req, pack_context, plan_started_at)
        .await;
    if !resp.ok {
        anyhow::bail!(
            "prefetch failed: {}",
            resp.error.as_deref().unwrap_or("unknown")
        );
    }
    Ok(count)
}

/// Rank-0 prefetch: identity key, then the legacy host triple.
async fn identity_manifest_prefetch(daemon: &Arc<Daemon>, identity_key: Option<&str>) -> usize {
    let mut manifest = None;
    let mut manifest_key = String::new();
    for key in crate::identity::manifest_lookup_keys(identity_key) {
        match daemon.download_planner_manifest(&key).await {
            Ok(Some(found)) => {
                manifest_key = key;
                manifest = Some(found);
                break;
            }
            Ok(None) => {}
            Err(e) => {
                tracing::debug!("manifest prefetch '{key}': {e:#}");
            }
        }
    }
    let Some(manifest) = manifest else {
        tracing::info!("manifest prefetch: no identity or legacy manifest, skipping");
        return 0;
    };

    identity_manifest_prefetch_from(daemon, manifest_key, manifest).await
}

async fn identity_manifest_prefetch_from(
    daemon: &Arc<Daemon>,
    manifest_key: String,
    manifest: crate::remote::BuildManifest,
) -> usize {
    let min_compile_ms: u64 = std::env::var("KACHE_MIN_COMPILE_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);

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
        "manifest prefetch '{manifest_key}': {} entries, prefetching {} (skipped {} cheap crates < {}ms)",
        manifest.entries.len(),
        worth_prefetching.len(),
        skipped,
        min_compile_ms
    );

    if worth_prefetching.is_empty() {
        return 0;
    }

    let prefetch_keys: Vec<(String, String)> = worth_prefetching
        .iter()
        .map(|e| (e.cache_key.clone(), e.crate_name.clone()))
        .collect();

    let req = PrefetchRequest {
        keys: prefetch_keys,
        warm_all: false,
        origin: None,
        candidate_sources: HashMap::new(),
    };
    let resp = daemon.handle_prefetch(&req).await;
    if !resp.ok {
        tracing::warn!(
            "manifest prefetch failed: {}",
            resp.error.as_deref().unwrap_or("unknown")
        );
        return 0;
    }
    worth_prefetching.len()
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

/// Read the next request only while the daemon is still accepting work.
/// Checking on both sides of the await covers handlers already parked between
/// persistent requests when another connection initiates shutdown.
async fn read_request_before_shutdown(
    lifecycle: &Arc<Lifecycle>,
    read: impl std::future::Future<Output = std::io::Result<Option<String>>>,
) -> std::io::Result<Option<String>> {
    if !lifecycle.accepting_calls() {
        return Ok(None);
    }

    let line = tokio::select! {
        biased;
        _ = lifecycle.draining() => return Ok(None),
        line = read => line?,
    };
    if !lifecycle.accepting_calls() {
        return Ok(None);
    }
    Ok(line)
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
    lifecycle: &Arc<Lifecycle>,
    limiter: Arc<tokio::sync::Semaphore>,
    request_started_at: Instant,
) -> Result<()> {
    // Peer-credential gate before anything else: the check is a cheap
    // syscall, so unauthenticated peers are dropped before they can park on
    // the connection limiter or read a single request frame.
    #[cfg(unix)]
    if let Err(error) = crate::transport::require_self_peer(crate::transport::peer_euid(&stream)) {
        tracing::warn!(%error, "rejected IPC connection from another local user");
        return Ok(());
    }
    let _permit = limiter.acquire_owned().await.ok();
    handle_connection_started_at(stream, daemon, lifecycle, request_started_at).await
}

#[cfg(test)]
pub(crate) async fn handle_connection(
    stream: TokioStream,
    daemon: &Arc<Daemon>,
    lifecycle: &Arc<Lifecycle>,
) -> Result<()> {
    handle_connection_started_at(stream, daemon, lifecycle, Instant::now()).await
}

async fn handle_connection_started_at(
    stream: TokioStream,
    daemon: &Arc<Daemon>,
    lifecycle: &Arc<Lifecycle>,
    request_started_at: Instant,
) -> Result<()> {
    // Use borrow pattern: &TokioStream implements both AsyncRead and AsyncWrite.
    // Do NOT use stream.split() — interprocess docs warn that "dropping a half
    // does not shut it down", which causes the reader to never see EOF and
    // hangs the server loop (and tarpaulin coverage runs).
    let mut reader = BufReader::new(&stream);
    let mut frame = Vec::new();

    loop {
        let line = match read_request_before_shutdown(
            lifecycle,
            read_bounded_line(&mut reader, &mut frame),
        )
        .await
        {
            Ok(Some(l)) => l,
            Ok(None) => break,
            Err(e) if is_client_disconnect(&e) => {
                // Fire-and-forget client closed abruptly — not an error.
                tracing::debug!("client disconnected mid-read: {e}");
                break;
            }
            Err(e) => return Err(e.into()),
        };
        let Some(_request) = lifecycle.begin() else {
            break;
        };
        let start = Instant::now();
        let parsed = serde_json::from_str::<Request>(&line);

        // Extract client_epoch from fire-and-forget requests for staleness detection.
        let client_epoch = parsed.as_ref().map_or(0, Request::client_epoch);

        if parsed.as_ref().is_ok_and(Request::is_build_activity) {
            daemon.request_clock.touch(Instant::now());
        }

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
            Ok(Request::GcHint) => daemon.handle_gc_hint(),
            Ok(Request::RemoteCheck(req)) => {
                daemon
                    .handle_remote_check_started_at(&req, request_started_at)
                    .await
            }
            Ok(Request::Health) => daemon.handle_health(),
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
            Ok(Request::PublishCc(req)) => daemon.handle_publish_cc(*req).await,
            Ok(Request::PredictionFetch(req)) => daemon.handle_prediction_fetch(&req).await,
            Ok(Request::PredictionPublish(req)) => daemon.handle_prediction_publish(req),
            Ok(Request::Shutdown) => {
                lifecycle.start_drain();
                // Wake the accept loop so it breaks now rather than on the next
                // periodic tick (issue #288).

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
        if client_epoch_is_newer(client_epoch, daemon.build_epoch) && lifecycle.accepting_calls() {
            tracing::info!(
                daemon_epoch = daemon.build_epoch,
                client_epoch,
                "client binary is newer than daemon, scheduling restart"
            );
            lifecycle.start_drain();
            // Wake the accept loop so the restart starts now (issue #288).
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

/// How long a wrapper waits for the daemon to acknowledge a GC hint. The
/// daemon answers before it sweeps, so this bounds a saturated daemon, never
/// a sweep.
const GC_HINT_ACK_TIMEOUT: Duration = Duration::from_millis(500);

/// Tell a running daemon the store is under size pressure. True when the
/// daemon took the hint and owns the sweep. False when no daemon listens, it
/// predates the hint, or it did not answer in time; the caller then sweeps
/// itself. Never starts a daemon: [`send_gc_request`] probes stats, starts
/// one and waits out the whole sweep, none of which a compile may pay for.
pub fn send_gc_hint(config: &Config) -> bool {
    gc_hint_accepted(send_request_with_timeout(
        &config.socket_path(),
        &Request::GcHint,
        GC_HINT_ACK_TIMEOUT,
    ))
}

fn gc_hint_accepted(reply: Result<String>) -> bool {
    reply
        .ok()
        .and_then(|line| serde_json::from_str::<Response>(&line).ok())
        .is_some_and(|resp| resp.ok)
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
    shard_dir: Option<&Path>,
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
        deadline_ms: Some(client_budget_ms.get()),
        shard_dir: shard_dir.map(|dir| dir.to_string_lossy().into_owned()),
    });

    // This wait is on rustc's synchronous miss path. Preserve the historical
    // hard three-second ceiling even when talking to an older daemon that
    // ignores `deadline_ms`; configuration may only shorten the wait.
    let client_timeout = Duration::from_millis(client_budget_ms.get());
    match send_request_with_timeout(&socket_path, &req, client_timeout) {
        Ok(resp_str) => remote_check_result_from_response_line(&resp_str),
        Err(e) => {
            tracing::debug!("remote check: daemon unreachable ({e})");
            None
        }
    }
}

/// Ask the daemon for the remote's input prediction row for `identity`
/// (kunobi-ninja/kache#1011). `None` for no row, no remote, no daemon, or a
/// daemon too old to know the request.
pub fn send_prediction_fetch(
    config: &Config,
    identity: &str,
) -> Option<crate::prediction_share::SharedPrediction> {
    config.remote.as_ref()?;
    let socket_path = config.socket_path();
    if !crate::transport::is_reachable(&socket_path) {
        return None;
    }
    let req = Request::PredictionFetch(PredictionFetchRequest {
        identity: identity.to_string(),
    });
    let reply = send_request_with_timeout(&socket_path, &req, prediction_fetch_wait()).ok()?;
    serde_json::from_str::<Response>(&reply)
        .ok()
        .filter(|response| response.ok)?
        .prediction
}

/// Hand a portable row to the daemon to store on the remote. Fire and forget:
/// a lost row costs another machine one pre-pass. Nothing is sent without a
/// writable remote.
pub fn send_prediction_publish(
    config: &Config,
    identity: &str,
    row: crate::prediction_share::SharedPrediction,
) {
    if !publishes_predictions(config) {
        return;
    }
    let req = Request::PredictionPublish(PredictionPublishRequest {
        identity: identity.to_string(),
        row,
    });
    if let Err(error) = send_request_fire_and_forget(&config.socket_path(), &req) {
        tracing::debug!("prediction row publish not sent: {error}");
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
///
/// This runs inside a `retain` over every in-flight compile, so it stays a
/// bare probe rather than [`process_is_alive`]. It refuses PIDs 0 and 1, because
/// `kill(-1, 0)` succeeds whenever anything is signalable and would keep
/// bogus entries alive in the map forever.
#[cfg(unix)]
fn pid_alive(pid: u32) -> bool {
    if pid <= 1 || i32::try_from(pid).is_err() {
        return false;
    }
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

/// Verify readiness without waiting for store locks, scans or maintenance.
/// Legacy daemons use their existing health request; an unsupported reply requires restart.
pub fn send_health_request(config: &Config) -> Result<DaemonHealth> {
    let health = fetch_daemon_health(config)?;
    anyhow::ensure!(
        !client_epoch_is_newer(build_epoch(), health.build_epoch),
        "daemon needs an upgrade"
    );
    Ok(health)
}

fn fetch_daemon_health(config: &Config) -> Result<DaemonHealth> {
    if let Some(health) =
        lifecycle_control::health(config, Instant::now() + Duration::from_secs(2))?
    {
        anyhow::ensure!(health.ready && !health.draining, "daemon is not ready");
        return Ok(DaemonHealth {
            version: health.build,
            build_epoch: health.revision,
        });
    }
    let response = send_request_with_timeout(
        &config.socket_path(),
        &Request::Health,
        Duration::from_secs(2),
    )?;
    parse_daemon_health(&response)
}

fn parse_daemon_health(response: &str) -> Result<DaemonHealth> {
    let response: Response = serde_json::from_str(response)?;
    anyhow::ensure!(response.ok, "daemon rejected readiness check");
    response.health.context("daemon omitted readiness response")
}

/// Send a stats request to the daemon. No auto-start — stats are best-effort.
/// Returns Err if daemon is unreachable.
pub fn send_stats_request(
    config: &Config,
    include_entries: bool,
    sort_by: Option<&str>,
    window: Option<crate::since::SinceWindow>,
) -> Result<StatsResponse> {
    send_stats_request_options(config, include_entries, false, sort_by, window)
}

/// Read the daemon's stats without starting or waiting for a replacement.
///
/// Not the same as side-effect-free, and deliberately not named that way: the
/// request still carries this binary's build epoch, so an older daemon schedules
/// its own graceful shutdown after answering, exactly as it does for any other
/// client. What this variant drops is the *client* side of that handoff —
/// [`send_stats_request`] spawns the replacement and blocks up to
/// [`DAEMON_START_TIMEOUT`] waiting for it to bind its socket.
///
/// `doctor` reads through this variant because a pending upgrade is something to
/// describe, not something to stall on (kunobi-ninja/kache#720).
pub fn send_stats_request_without_restart(
    config: &Config,
    include_entries: bool,
) -> Result<StatsResponse> {
    fetch_stats(
        config,
        include_entries,
        false,
        None,
        None,
        STATS_READ_TIMEOUT,
    )
}

pub(crate) fn send_stats_request_options(
    config: &Config,
    include_entries: bool,
    include_summaries: bool,
    sort_by: Option<&str>,
    window: Option<crate::since::SinceWindow>,
) -> Result<StatsResponse> {
    let client_epoch = build_epoch();
    let stats = fetch_stats(
        config,
        include_entries,
        include_summaries,
        sort_by,
        window,
        STATS_READ_TIMEOUT,
    )?;

    refresh_stale_response(
        stats,
        client_epoch,
        |stats| stats.build_epoch,
        || restart_daemon_for_stale_client(config),
        || {
            fetch_stats(
                config,
                include_entries,
                include_summaries,
                sort_by,
                window,
                STATS_REFETCH_TIMEOUT,
            )
        },
    )
}

fn refresh_stale_response<T>(
    stats: T,
    client_epoch: u64,
    epoch: impl Fn(&T) -> u64,
    restart: impl FnOnce() -> Result<bool>,
    refetch: impl FnOnce() -> Result<T>,
) -> Result<T> {
    if !client_epoch_is_newer(client_epoch, epoch(&stats)) {
        return Ok(stats);
    }
    tracing::info!(
        daemon_epoch = epoch(&stats),
        client_epoch,
        "stale daemon detected, restarting"
    );
    anyhow::ensure!(restart()?, "replacement daemon did not become ready");
    let fresh = refetch().context("reading replacement daemon response")?;
    anyhow::ensure!(
        !client_epoch_is_newer(client_epoch, epoch(&fresh)),
        "replacement daemon is still older than this client"
    );
    Ok(fresh)
}

/// One stats round trip: no auto-start, no restart, no retry.
fn fetch_stats(
    config: &Config,
    include_entries: bool,
    include_summaries: bool,
    sort_by: Option<&str>,
    window: Option<crate::since::SinceWindow>,
    read_timeout: Duration,
) -> Result<StatsResponse> {
    let req = Request::Stats(StatsRequest {
        include_entries,
        include_summaries,
        sort_by: sort_by.map(String::from),
        // Rounded up, not down: a daemon that predates `event_secs` should
        // answer with a superset of a sub-hour window rather than nothing.
        event_hours: window.map(|w| w.secs().div_ceil(3600)),
        event_secs: window.map(crate::since::SinceWindow::secs),
        client_epoch: build_epoch(),
    });

    let resp_str = send_request_with_timeout(&config.socket_path(), &req, read_timeout)?;
    let resp: Response = serde_json::from_str(&resp_str)?;

    if resp.ok {
        resp.stats
            .ok_or_else(|| anyhow::anyhow!("stats response missing payload"))
    } else {
        anyhow::bail!("daemon stats error: {}", resp.error.unwrap_or_default())
    }
}

/// Send a shutdown request to the running daemon.
///
/// Wait for ownership release after a verified drain acknowledgement. A timeout
/// leaves unfinished work running and reports failure.
pub fn send_shutdown_request(config: &Config) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    if lifecycle_control::request(config, kunobi_daemon::wire::operation::DRAIN, deadline)?
        .is_none()
    {
        let response =
            lifecycle_control::legacy_request(&config.socket_path(), &Request::Shutdown, deadline)?;
        let response: Response = serde_json::from_str(&response)?;
        anyhow::ensure!(response.ok, "daemon rejected shutdown");
    }
    anyhow::ensure!(
        wait_for_run_lock_release(&config.socket_path(), Duration::from_secs(35))?,
        "daemon is still draining; ownership has not been released"
    );
    eprintln!("daemon stopped");
    Ok(())
}

/// Drain and replace this cache's daemon through the shared exclusive coordinator.
/// Returns true only after the replacement answers a compatible readiness probe.
pub fn restart(config: &Config) -> Result<bool> {
    lifecycle_client::ensure(config, true)
}

/// Best-effort restart for stale-daemon detection from stats polling.
/// This path is intentionally outside build hot paths, so a short bounded wait
/// is acceptable to keep monitor/status output current.
pub(crate) fn restart_daemon_for_stale_client(config: &Config) -> Result<bool> {
    // Keep service-managed daemons under their manager after an upgrade.
    // A protocol shutdown followed by a direct spawn leaves launchd/systemd
    // stopped after a successful exit and gives the replacement no supervisor.
    restart(config)
}

/// Send a request to the daemon, return the response line.
fn send_request(socket_path: &Path, req: &Request) -> Result<String> {
    send_request_with_timeout(socket_path, req, std::time::Duration::from_secs(30))
}

/// Send a request to the daemon with a configurable read timeout.
pub(crate) fn send_request_with_timeout(
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
    let ready = start_daemon_background_inner()?;
    if !ready {
        tracing::warn!("daemon did not start after recovery");
    }
    Ok(ready)
}

fn start_daemon_background_inner() -> Result<bool> {
    lifecycle_client::ensure(&Config::load()?, false)
}

fn daemon_run_lock_path(socket_path: &Path) -> PathBuf {
    socket_path.with_extension("run.lock")
}

fn daemon_run_lock_is_held(socket_path: &Path) -> Result<bool> {
    kunobi_daemon::ProcessLock::is_held(daemon_run_lock_path(socket_path))
        .context("observing daemon run lock")
}

/// Observe the daemon run lock without creating it: a missing file reads as
/// "not held", so a probe on a host that never ran a daemon leaves nothing
/// behind for `doctor` to report.
pub(crate) fn existing_daemon_run_lock_is_held(socket_path: &Path) -> Result<bool> {
    daemon_run_lock_is_held(socket_path)
}

/// Environment variables that decide the daemon's REMOTE, stripped from an
/// auto-spawned daemon's environment (kunobi-ninja/kache#706).
///
/// A background daemon outlives the build that happened to start it and serves
/// every later build on the machine, so inheriting these makes its remote a
/// lottery decided by whoever won the startup race. In the reported case a
/// monorepo kept `KACHE_S3_*` in per-checkout `.cargo/config.toml`, present in
/// some worktrees and absent in others: 2,330 `no remote configured` failures
/// in six hours, then the remote silently began working after an unrelated
/// restart. With the default `daemon_idle_timeout_secs = 0` one unlucky first
/// start pins the machine to remote-off indefinitely.
///
/// The deeper reason env cannot be authoritative here: the daemon watches its
/// config FILE and restarts when it changes, and there is no equivalent for a
/// parent process's environment. A setting the daemon cannot watch cannot stay
/// correct for the daemon's lifetime.
///
/// Only the auto-spawn path is affected. An operator running `kache daemon
/// run` directly, or a service manager supplying `Environment=`, does not pass
/// through here and keeps env precedence — that placement is deliberate, not a
/// race.
const AMBIENT_REMOTE_ENV_VARS: &[&str] = &[
    "KACHE_S3_BUCKET",
    "KACHE_S3_ENDPOINT",
    "KACHE_S3_REGION",
    "KACHE_S3_PREFIX",
    "KACHE_S3_PROFILE",
    "KACHE_S3_USER_AGENT",
    "KACHE_LOCAL_ONLY",
    "KACHE_REMOTE_READONLY",
];

/// Warn when this build's environment is the ONLY place a remote is
/// configured, because the daemon we are about to start will not use it
/// (kunobi-ninja/kache#706).
///
/// Silence is the failure mode being fixed: before this, such a setup either
/// worked or did not depending on which build won the startup race, with
/// nothing said either way. Deterministically not applying it is only an
/// improvement if the user is told, so this prints the remedy once, at the
/// moment the decision is made.
///
/// Says nothing when the config file already declares a remote (the common
/// case, where env is redundant or an intentional per-build override of a
/// remote the daemon has anyway), so the warning stays rare enough to read.
fn warn_if_remote_is_env_only(config: &Config) -> bool {
    let set: Vec<&str> = AMBIENT_REMOTE_ENV_VARS
        .iter()
        .copied()
        .filter(|name| std::env::var_os(name).is_some())
        .collect();
    if set.is_empty() {
        return false;
    }
    // The merged view. With these variables set, a host remote yields to them
    // in this build, so a remote left here is one the chosen file declares.
    let file_config = Config::load_file_config().unwrap_or_default();
    if file_config
        .cache
        .as_ref()
        .is_some_and(|cache| cache.remote.is_some())
    {
        return false;
    }
    let daemon_remote = daemon_remote_after_env_strip(&crate::config::host_config_status());
    let message = format!(
        "kache: a remote is configured only in this build's environment ({vars}), and the \n         \
         background daemon does not inherit it — {daemon_remote}.\n         \
         A daemon outlives the build that starts it and cannot watch an environment for \n         \
         changes, so an inherited remote would silently depend on which build happened to \n         \
         start it (kunobi-ninja/kache#706).\n         \
         Fix: move the remote into `[cache.remote]` in {path}, or start the daemon \n         \
         yourself with `kache daemon run` from this environment.",
        vars = set.join(", "),
        path = crate::config::resolve_config_path().display(),
    );
    let marker = crate::wrapper::warn_marker_path("daemon-remote-env", &config.cache_dir);
    crate::wrapper::warn_once_per_session(&marker, crate::wrapper::WARN_SESSION_SECS, &message);
    true
}

/// What the daemon falls back to once it drops the build's remote variables:
/// the host config's remote when the host file declares one, else none.
fn daemon_remote_after_env_strip(host: &crate::config::HostConfigStatus) -> String {
    match host {
        crate::config::HostConfigStatus::Present { path, keys }
            if keys.iter().any(|entry| entry.key == "cache.remote") =>
        {
            format!(
                "the daemon will use the remote in the host config {} instead",
                path.display()
            )
        }
        _ => "the daemon will run local-only".to_string(),
    }
}

/// Spawn `kache daemon run` detached, without leaking this process's
/// inheritable handles to it (kunobi-ninja/kache#704), and without leaking the
/// remote configuration of whichever build happened to start it
/// (kunobi-ninja/kache#706).
/// Remove the ambient remote settings from a daemon spawn, so the daemon
/// resolves its remote from its watched config file rather than from whichever
/// build started it. Split out so the stripping is testable without spawning.
fn strip_ambient_remote_env(command: &mut std::process::Command) {
    for name in AMBIENT_REMOTE_ENV_VARS {
        command.env_remove(name);
    }
}

/// `kache daemon run` for the binary at `exe`. A wrapper running behind a
/// compiler shim can start the daemon, and there `exe` can be the shim, so
/// this goes through [`crate::platform::self_command`].
fn daemon_run_command(exe: &Path) -> std::process::Command {
    let mut command = crate::platform::self_command(exe, "daemon");
    command.arg("run");
    command
}

/// Start `kache daemon run` detached from this process, with stderr going to
/// `stderr_target`.
///
/// kunobi-daemon does the detaching. On Unix the daemon leads a new session,
/// so Ctrl-C and the hangup of the terminal a build runs in do not reach it
/// (kunobi-ninja/kache#1205), and it holds no descriptor of ours beyond its
/// standard streams. On Windows it inherits only its three standard handles,
/// so a caller's pipe cannot stay open because of it and keep cargo waiting
/// for end of file (kunobi-ninja/kache#704, #1001), and it leaves the caller's
/// job object when the job allows it. Cargo's job does not: a daemon started
/// under cargo stays in cargo's job and stops when the build is interrupted.
/// `kache daemon install` starts it from a scheduled task instead.
fn spawn_detached_daemon(
    exe: &Path,
    stderr_target: kunobi_daemon::launch::DaemonOutput,
) -> Result<kunobi_daemon::launch::DaemonChild> {
    let mut command = daemon_run_command(exe);
    strip_ambient_remote_env(&mut command);
    let mut daemon = detached_command(&command);
    daemon.stderr(stderr_target);
    let child = daemon.spawn().context("spawning daemon process")?;
    if child.in_callers_job() {
        tracing::debug!(
            "the daemon (PID {}) shares the build's job object and stops if the build is \
             interrupted; `kache daemon install` starts it outside any build",
            child.id()
        );
    }
    Ok(child)
}

/// The daemon command `command` describes, as kunobi-daemon's detached
/// spawn: the same program, arguments and environment changes. Its standard
/// streams start as the null device.
///
/// argv[0] is not carried over: kunobi-daemon cannot set it. That is safe
/// because [`crate::platform::SELF_SPAWN_ENV`] and the `daemon` argument
/// already route the child as a self-spawn before any compiler-shim name is
/// considered, which is how Windows has always worked.
fn detached_command(command: &std::process::Command) -> kunobi_daemon::launch::DaemonCommand {
    let mut daemon = kunobi_daemon::launch::DaemonCommand::new(command.get_program());
    daemon.args(command.get_args());
    for (name, value) in command.get_envs() {
        match value {
            Some(value) => daemon.env(name, value),
            None => daemon.env_remove(name),
        };
    }
    daemon
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
    fn identity_publish_context_requires_both_nonempty_values() {
        assert_eq!(identity_publish_context(None, "session", true, false), None);
        assert_eq!(
            identity_publish_context(Some(""), "session", true, false),
            None
        );
        assert_eq!(
            identity_publish_context(Some("   "), "session", true, false),
            None
        );
        assert_eq!(
            identity_publish_context(Some("identity"), "", true, false),
            None
        );
        assert_eq!(
            identity_publish_context(Some("identity"), "   ", true, false),
            None
        );
        assert_eq!(
            identity_publish_context(Some("identity"), "session", false, false),
            None
        );
        assert_eq!(
            identity_publish_context(Some("identity"), "session", true, true),
            None
        );
        assert_eq!(
            identity_publish_context(Some("  identity  "), "  session  ", true, false),
            Some(("identity".to_string(), "session".to_string()))
        );
    }

    #[test]
    fn identity_prefetch_only_short_circuits_after_queuing_work() {
        assert!(!identity_prefetch_satisfied(0));
        assert!(identity_prefetch_satisfied(1));
    }

    #[test]
    fn automatic_identity_publish_reports_when_work_was_attempted() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let daemon = Daemon::new(config);

        assert!(!daemon.maybe_publish_identity_manifest(None, "session"));
        assert!(daemon.maybe_publish_identity_manifest(Some("id/test"), "session"));
    }

    /// kunobi-ninja/kache#706: an auto-spawned daemon must not inherit the
    /// remote of whichever build started it, or its remote becomes a lottery
    /// decided by the startup race — the reported case logged 2,330
    /// `no remote configured` failures in six hours, then silently started
    /// working after an unrelated restart.
    #[test]
    fn auto_spawned_daemon_does_not_inherit_ambient_remote_env() {
        let mut command = std::process::Command::new("kache");
        strip_ambient_remote_env(&mut command);

        let removed: Vec<&str> = command
            .get_envs()
            .filter(|(_, value)| value.is_none())
            .filter_map(|(name, _)| name.to_str())
            .collect();
        for name in AMBIENT_REMOTE_ENV_VARS {
            assert!(
                removed.contains(name),
                "{name} must be cleared from the daemon spawn"
            );
        }

        // Nothing else is touched: this fix is about the remote being ambient,
        // not about sanitising the daemon's environment in general. Stripping
        // more (PATH, RUSTUP_*, credentials providers) would change unrelated
        // behaviour and break credential discovery for a file-configured S3
        // remote, which still needs the ambient AWS_* / HOME to authenticate.
        assert_eq!(
            removed.len(),
            AMBIENT_REMOTE_ENV_VARS.len(),
            "unexpected extra removals: {removed:?}"
        );
        assert!(
            command.get_envs().all(|(_, value)| value.is_none()),
            "the spawn should only remove variables, never set them"
        );
    }

    /// A wrapper behind a compiler shim can start the daemon with the shim as
    /// its executable. The child must still parse `daemon run` as the CLI.
    #[test]
    fn daemon_spawn_is_marked_as_a_self_spawn() {
        let exe = Path::new("/x/shims/cc");
        let command = daemon_run_command(exe);
        assert_eq!(command.get_program(), exe.as_os_str());
        let args: Vec<&std::ffi::OsStr> = command.get_args().collect();
        assert_eq!(args, ["daemon", "run"]);
        let argv: Vec<String> = std::iter::once("/x/shims/cc")
            .chain(args.iter().filter_map(|arg| arg.to_str()))
            .map(str::to_string)
            .collect();
        let marker = command
            .get_envs()
            .find(|(name, _)| *name == crate::platform::SELF_SPAWN_ENV)
            .and_then(|(_, value)| value);
        assert!(crate::platform::is_self_spawn(&argv, marker));
    }

    /// Env var naming the fixture directory for
    /// `detached_daemon_caller_fixture`; unset, the fixture does nothing.
    #[cfg(unix)]
    const DETACH_FIXTURE_DIR: &str = "KACHE_TEST_DETACH_FIXTURE_DIR";

    /// Plays the build that auto-starts the daemon: it spawns a stand-in
    /// daemon through `spawn_detached_daemon`, records its PID, then waits to
    /// be interrupted. Run only by
    /// `detached_daemon_survives_sigint_to_the_callers_group`.
    #[cfg(unix)]
    #[test]
    #[ignore = "fixture for detached_daemon_survives_sigint_to_the_callers_group"]
    fn detached_daemon_caller_fixture() {
        let Some(dir) = std::env::var_os(DETACH_FIXTURE_DIR).map(PathBuf::from) else {
            return;
        };
        // This process runs this one test, so nothing else reads the env.
        for name in AMBIENT_REMOTE_ENV_VARS {
            unsafe { std::env::set_var(name, "ambient") };
        }
        let log = std::fs::File::create(dir.join("daemon.log")).unwrap();
        let child = spawn_detached_daemon(
            &dir.join("kache"),
            kunobi_daemon::launch::DaemonOutput::File(log),
        )
        .unwrap();
        std::fs::write(dir.join("pid.tmp"), child.id().to_string()).unwrap();
        std::fs::rename(dir.join("pid.tmp"), dir.join("pid")).unwrap();
        std::thread::sleep(Duration::from_secs(60));
    }

    /// kunobi-ninja/kache#1205: Ctrl-C in the terminal signals the whole
    /// foreground process group, and an auto-started daemon used to be part of
    /// it. A caller in its own group starts a stand-in daemon through the real
    /// spawn path; interrupting that group must leave the daemon running, with
    /// the arguments, environment and stderr the daemon is meant to get.
    #[cfg(unix)]
    #[test]
    fn detached_daemon_survives_sigint_to_the_callers_group() {
        use std::os::unix::fs::PermissionsExt;
        use std::os::unix::process::CommandExt;

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let script = root.join("kache");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{0}/args'\nenv > '{0}/env'\n\
                 echo daemon-stderr >&2\ntouch '{0}/ready'\nexec sleep 30\n",
                root.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

        let mut caller = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "daemon::tests::detached_daemon_caller_fixture",
                "--ignored",
                "--nocapture",
            ])
            .env(DETACH_FIXTURE_DIR, root)
            .process_group(0)
            .spawn()
            .unwrap();
        // The caller is a second copy of this debug test binary; on a loaded
        // machine its start alone can take seconds.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while !(root.join("pid").exists() && root.join("ready").exists()) {
            assert!(
                caller.try_wait().unwrap().is_none(),
                "the caller exited before starting the daemon"
            );
            assert!(
                std::time::Instant::now() < deadline,
                "the daemon never started"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        let daemon: i32 = std::fs::read_to_string(root.join("pid"))
            .unwrap()
            .parse()
            .unwrap();
        let caller_group = caller.id() as i32;

        assert_eq!(unsafe { libc::kill(-caller_group, libc::SIGINT) }, 0);
        let status = caller.wait().unwrap();
        // Give a daemon that shared the group time to die, then look.
        std::thread::sleep(Duration::from_millis(300));
        let survived = unsafe { libc::kill(daemon, 0) } == 0;
        let session = unsafe { libc::getsid(daemon) };
        unsafe { libc::kill(daemon, libc::SIGKILL) };

        assert!(!status.success(), "SIGINT never reached the caller's group");
        assert!(survived, "the daemon died with its caller's process group");
        assert_eq!(session, daemon, "the daemon must lead its own session");

        let args = std::fs::read_to_string(root.join("args")).unwrap();
        assert_eq!(args, "daemon\nrun\n");
        let env = std::fs::read_to_string(root.join("env")).unwrap();
        let marker = format!("{}=", crate::platform::SELF_SPAWN_ENV);
        assert!(
            env.lines().any(|line| line.starts_with(&marker)),
            "the daemon must be marked as a self-spawn: {env}"
        );
        for name in AMBIENT_REMOTE_ENV_VARS {
            let prefix = format!("{name}=");
            assert!(
                !env.lines().any(|line| line.starts_with(&prefix)),
                "{name} leaked into the daemon's environment"
            );
        }
        let log = std::fs::read_to_string(root.join("daemon.log")).unwrap();
        assert_eq!(log, "daemon-stderr\n", "stderr must go to the daemon log");
    }

    /// Only an OS that proves the process gone makes it dead; PIDs that can
    /// never name a daemon are dead without asking.
    #[test]
    fn process_liveness_maps_states_conservatively() {
        use kunobi_daemon::local::ProcessState;
        assert!(alive_from_state(2, || ProcessState::Alive));
        assert!(alive_from_state(2, || ProcessState::Unknown));
        assert!(!alive_from_state(2, || ProcessState::Exited));
        assert!(alive_from_state(i32::MAX as u32, || ProcessState::Alive));
        for pid in [0, 1, i32::MAX as u32 + 1, u32::MAX] {
            assert!(
                !alive_from_state(pid, || ProcessState::Alive),
                "PID {pid} must never read as a live daemon"
            );
        }
    }

    #[test]
    fn current_process_is_alive() {
        assert!(process_is_alive(std::process::id()));
    }

    #[cfg(unix)]
    #[test]
    fn reaped_child_is_not_alive() {
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .unwrap();
        let pid = child.id();
        assert!(process_is_alive(pid));
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(!process_is_alive(pid), "a reaped child must read as exited");
    }

    /// Set/remove an env var for one test and restore it on drop. Local to
    /// these tests; the process-global env is serialized by the shared
    /// `config_path_lock`.
    struct EnvVarForTest {
        name: String,
        previous: Option<std::ffi::OsString>,
    }

    impl EnvVarForTest {
        fn set(name: &str, value: &std::ffi::OsStr) -> Self {
            let previous = std::env::var_os(name);
            unsafe { std::env::set_var(name, value) };
            Self {
                name: name.to_string(),
                previous,
            }
        }

        fn remove(name: &str) -> Self {
            let previous = std::env::var_os(name);
            unsafe { std::env::remove_var(name) };
            Self {
                name: name.to_string(),
                previous,
            }
        }
    }

    impl Drop for EnvVarForTest {
        fn drop(&mut self) {
            match self.previous.take() {
                Some(value) => unsafe { std::env::set_var(&self.name, value) },
                None => unsafe { std::env::remove_var(&self.name) },
            }
        }
    }

    /// The env-only warning says what the daemon falls back to: the host
    /// config's remote only when the host file declares one.
    #[test]
    fn env_only_warning_names_the_host_remote_only_when_the_host_declares_one() {
        use crate::config::{HostConfigKey, HostConfigStatus, HostKeySource};
        let path = std::path::PathBuf::from("/etc/kache/config.toml");
        let key = |key: &str| HostConfigKey {
            key: key.to_string(),
            source: HostKeySource::Host,
        };

        let with_remote = HostConfigStatus::Present {
            path: path.clone(),
            keys: vec![key("cache.input_predictions"), key("cache.remote")],
        };
        assert_eq!(
            daemon_remote_after_env_strip(&with_remote),
            "the daemon will use the remote in the host config /etc/kache/config.toml instead"
        );
        let without_remote = HostConfigStatus::Present {
            path: path.clone(),
            keys: vec![key("cache.input_predictions")],
        };
        assert_eq!(
            daemon_remote_after_env_strip(&without_remote),
            "the daemon will run local-only"
        );
        assert_eq!(
            daemon_remote_after_env_strip(&HostConfigStatus::Absent { path }),
            "the daemon will run local-only"
        );
    }

    /// The warning is the entire discoverability half of #706: making the
    /// daemon deterministic turns "sometimes works" into "never works" for an
    /// env-only setup, which is only an improvement if the user is told. A
    /// silently-removed warning would restore exactly the silent
    /// misconfiguration the issue is about, so drive the real entry point.
    #[test]
    fn env_only_remote_warns_but_a_file_configured_remote_does_not() {
        let _lock = crate::config::tests::config_path_lock();
        let dir = tempfile::tempdir().unwrap();

        // Keep the marker out of a shared cache dir so the once-per-session
        // dedup cannot be satisfied by an unrelated run.
        let config = test_config(&dir.path().join("cache"));

        let config_path = dir.path().join("config.toml");
        let restore_config = EnvVarForTest::set("KACHE_CONFIG", config_path.as_os_str());

        // No remote anywhere: nothing to warn about.
        std::fs::write(&config_path, "[cache]\n").unwrap();
        let clear: Vec<_> = AMBIENT_REMOTE_ENV_VARS
            .iter()
            .map(|name| EnvVarForTest::remove(name))
            .collect();
        assert!(!warn_if_remote_is_env_only(&config));

        // Remote ONLY in the environment: the daemon will not use it, so say so.
        let _bucket = EnvVarForTest::set("KACHE_S3_BUCKET", std::ffi::OsStr::new("some-bucket"));
        assert!(
            warn_if_remote_is_env_only(&config),
            "an env-only remote must warn"
        );

        // Same environment, but the file declares a remote: the daemon has one,
        // so the env value is redundant or a deliberate per-build override and
        // the warning would be noise.
        std::fs::write(
            &config_path,
            "[cache.remote]\ntype = \"s3\"\nbucket = \"from-file\"\n",
        )
        .unwrap();
        assert!(
            !warn_if_remote_is_env_only(&config),
            "a file-configured remote must stay quiet"
        );

        // The chosen file has no remote, but the host config does. The build
        // uses the environment's remote over the host's, while the daemon,
        // which drops these variables, would use the host's: still env-only.
        std::fs::write(&config_path, "[cache]\n").unwrap();
        let host_path = dir.path().join("host.toml");
        std::fs::write(
            &host_path,
            "[cache.remote]\ntype = \"s3\"\nbucket = \"from-host\"\n",
        )
        .unwrap();
        let restore_host = crate::config::set_host_config_for_test(&host_path);
        assert!(
            warn_if_remote_is_env_only(&config),
            "an env remote over a host remote must warn"
        );

        drop(restore_host);
        drop(clear);
        drop(restore_config);
    }

    /// The stripped list must stay exactly the set of variables that decide a
    /// remote. A new `KACHE_S3_*` knob added without updating the list would
    /// silently reintroduce the lottery for that setting.
    #[test]
    fn ambient_remote_env_list_covers_every_remote_deciding_var() {
        let documented = [
            "KACHE_S3_BUCKET",
            "KACHE_S3_ENDPOINT",
            "KACHE_S3_REGION",
            "KACHE_S3_PREFIX",
            "KACHE_S3_PROFILE",
            "KACHE_S3_USER_AGENT",
            "KACHE_LOCAL_ONLY",
            "KACHE_REMOTE_READONLY",
        ];
        assert_eq!(
            AMBIENT_REMOTE_ENV_VARS, &documented,
            "remote-deciding env vars changed: update the strip list too (#706)"
        );
    }

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
                remote_check_budget_ms(configured_secs, wire_ms).get(),
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
            remote_check_budget_ms(300, legacy_request.deadline_ms).get(),
            3_000
        );

        let accepted_at = Instant::now();
        let legacy_client_deadline =
            RemoteDeadline::from_millis_at(accepted_at, remote_check_budget_ms(300, None).get());
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

    #[test]
    fn planned_candidates_upgrade_the_tracked_session_in_place() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let summary_path = config.summary_log_path();
        let daemon = Daemon::new(config);
        let req = BuildStartedRequest {
            intent: kache_core::BuildIntent {
                identity_key: Some("id/session".into()),
                ..Default::default()
            },
            client_epoch: 0,
            session_id: "session".into(),
        };
        daemon.ensure_active_session(&req);
        daemon.install_plan(
            "session",
            "plan",
            "fallback",
            ["key".to_string()].into_iter(),
            req.intent.identity_key.clone(),
        );

        let plan = daemon.active_plan.lock().unwrap();
        let plan = plan.as_ref().unwrap();
        assert_eq!(plan.plan_id, "plan");
        assert_eq!(plan.plan_source, "fallback");
        assert_eq!(plan.identity_key.as_deref(), Some("id/session"));
        assert_eq!(plan.candidates, HashSet::from(["key".to_string()]));
        assert!(!summary_path.exists());
    }

    #[test]
    fn active_session_updates_in_place_and_summarizes_on_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let summary_path = config.summary_log_path();
        let daemon = Daemon::new(config);
        let request = |session: &str, identity: &str| BuildStartedRequest {
            intent: kache_core::BuildIntent {
                identity_key: Some(identity.to_string()),
                ..Default::default()
            },
            client_epoch: 0,
            session_id: session.to_string(),
        };

        daemon.ensure_active_session(&request("session-a", "id/a"));
        daemon.ensure_active_session(&request("session-a", "id/a-updated"));
        {
            let plan = daemon.active_plan.lock().unwrap();
            let plan = plan.as_ref().unwrap();
            assert_eq!(plan.session_id, "session-a");
            assert_eq!(plan.identity_key.as_deref(), Some("id/a-updated"));
        }
        assert!(!summary_path.exists());

        daemon.ensure_active_session(&request("session-b", "id/b"));
        {
            let plan = daemon.active_plan.lock().unwrap();
            let plan = plan.as_ref().unwrap();
            assert_eq!(plan.session_id, "session-b");
            assert_eq!(plan.identity_key.as_deref(), Some("id/b"));
        }
        let summaries = crate::events::read_summaries(&summary_path).unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].session_id, "session-a");
        assert_eq!(summaries[0].closure_reason, "superseded");
        assert_eq!(summaries[0].plan_source, "none");
    }

    // Tests use the same cross-platform transport as production. On Unix
    // this resolves to UDS; on Windows (when tests are eventually enabled
    // there) it resolves to named pipes.
    #[cfg(test)]
    use crate::transport::ListenerOptions;
    use crate::transport::{TokioListener, TokioStream, socket_name};

    /// A sibling of `socket_path` that no earlier bind in this process has
    /// used.
    ///
    /// On Unix the endpoint is a filesystem path and rebinding the same name
    /// after the listener is gone is harmless. On Windows `socket_name` hashes
    /// the path into the machine-wide `\\.\pipe\` namespace, and
    /// `CreateNamedPipe` refuses a name any instance of the previous listener
    /// still holds — with `ERROR_ACCESS_DENIED`, not `ERROR_ALREADY_EXISTS`.
    /// Those instances go away when the OS closes the old handles, which is
    /// not ordered against the next bind, so two binds on one name race
    /// (kunobi-ninja/kache#1107). The process-wide counter keeps every bind on
    /// its own name; the caller's temp dir keeps it off the names concurrent
    /// nextest processes use.
    fn fresh_endpoint(socket_path: &Path) -> std::path::PathBuf {
        static BINDS: AtomicU64 = AtomicU64::new(0);
        let n = BINDS.fetch_add(1, Ordering::Relaxed);
        socket_path.with_file_name(format!("daemon-{n}.sock"))
    }

    /// #1107: two binds derived from one socket path must be live at the same
    /// time. Binding both proves it on every platform — a repeated name is
    /// `EADDRINUSE` on Unix and `ERROR_ACCESS_DENIED` on Windows, and either
    /// way `bind_listener` panics here instead of intermittently in whichever
    /// test happened to ask for a second roundtrip.
    #[tokio::test]
    async fn each_bind_gets_an_endpoint_name_of_its_own() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("daemon.sock");
        let first = fresh_endpoint(&socket_path);
        let second = fresh_endpoint(&socket_path);
        assert_ne!(first, second, "a reused name is the collision itself");
        assert_eq!(
            (first.parent(), second.parent()),
            (socket_path.parent(), socket_path.parent()),
            "endpoints stay beside the socket path the caller gave"
        );
        let _first = bind_listener(&first);
        let _second = bind_listener(&second);
    }

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

    /// Send one request, read one response, and deliberately keep the socket
    /// open so shutdown tests can prove the server does not require client EOF.
    async fn client_request_keep_open(
        socket_path: &Path,
        req: &Request,
    ) -> (Response, TokioStream) {
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

        (
            serde_json::from_str(&resp_line).expect("parse response"),
            stream,
        )
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
        let (response, stream) = client_request_keep_open(socket_path, req).await;
        drop(stream);
        response
    }

    /// Bind a fresh daemon socket, serve exactly one connection with
    /// `handle_connection`, run a single client roundtrip against it, and
    /// join the server task. Returns the parsed response.
    ///
    /// Every socket integration test funnels through this so the
    /// connect/serve/teardown ordering lives in one place and the macOS EOF
    /// hang (see `client_roundtrip`) cannot be reintroduced piecemeal.
    async fn one_shot_request(daemon: &Arc<Daemon>, socket_path: &Path, req: &Request) -> Response {
        // One endpoint per call: a test that asks for two roundtrips would
        // otherwise bind the same name twice (#1107). Both the listener and
        // the client below use the derived path, so callers keep passing the
        // socket path their config reports.
        let socket_path = &fresh_endpoint(socket_path);
        let listener = bind_listener(socket_path);

        let server_daemon = daemon.clone();
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept");
            handle_connection(stream, &server_daemon, &Arc::new(Lifecycle::default()))
                .await
                .expect("handle_connection");
        });

        let resp = client_roundtrip(socket_path, req).await;
        server.await.expect("join server task");
        resp
    }

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
                runtime_dir: "/c".into(),
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
        let old_effective_obj = old_effective.as_object_mut().unwrap();
        old_effective_obj.remove("remote_key_cache_refresh_secs");
        old_effective_obj.remove("runtime_dir");
        let parsed_effective: EffectiveConfig = serde_json::from_value(old_effective).unwrap();
        assert_eq!(
            parsed_effective.remote_key_cache_refresh_secs,
            crate::config::DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS,
            "an older daemon report must deserialize with the historical cadence"
        );
        assert!(parsed_effective.runtime_dir.is_empty());
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
        let lifecycle = Arc::new(Lifecycle::default());

        let server_daemon = daemon.clone();
        let server_flag = lifecycle.clone();

        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept");
            handle_connection(stream, &server_daemon, &server_flag)
                .await
                .expect("handle_connection");
        });

        let resp = client_roundtrip(&socket_path, &Request::Shutdown).await;
        server.await.expect("join server task");

        assert!(resp.ok, "stop request should return ok");
        assert!(
            !lifecycle.accepting_calls(),
            "stop request must close admission"
        );
        // A later observer must also see the shared drain transition.
        tokio::time::timeout(Duration::from_secs(1), lifecycle.draining())
            .await
            .expect("stop request must wake a later drain observer");
    }

    struct GatedHeadBackend {
        head_started: Arc<Notify>,
        release_head: Arc<tokio::sync::Semaphore>,
    }

    #[async_trait::async_trait]
    impl crate::remote_backend::RemoteBackend for GatedHeadBackend {
        async fn head(&self, _key: &str) -> Result<bool> {
            self.head_started.notify_one();
            let _release = self
                .release_head
                .acquire()
                .await
                .expect("test gate stays open");
            Ok(false)
        }

        async fn get(
            &self,
            _key: &str,
            _max_bytes: Option<u64>,
        ) -> Result<Option<crate::remote_backend::GetObject>> {
            panic!("a missing HEAD result must not issue GET");
        }

        async fn put(&self, _key: &str, _body: Vec<u8>, _content_type: Option<&str>) -> Result<()> {
            panic!("remote check must not issue PUT");
        }

        async fn list(&self, _prefix: &str) -> Result<Vec<String>> {
            Ok(Vec::new())
        }

        fn describe(&self, key: &str) -> String {
            format!("gated-head://test/{key}")
        }
    }

    /// Once shutdown starts, the accept loop must keep ownership of accepted
    /// requests until their current responses are written. Before the handler
    /// JoinSet, the loop returned as soon as the stop handler notified it,
    /// leaving both responses detached and vulnerable to runtime teardown.
    #[tokio::test]
    async fn accept_loop_drains_in_flight_response_and_shutdown_ack() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let head_started = Arc::new(Notify::new());
        let release_head = Arc::new(tokio::sync::Semaphore::new(0));
        let backend: Arc<dyn crate::remote_backend::RemoteBackend> = Arc::new(GatedHeadBackend {
            head_started: head_started.clone(),
            release_head: release_head.clone(),
        });
        let daemon = Arc::new(Daemon::new(config.clone()));
        daemon.signal_warming_complete();
        assert!(
            daemon.remote_backend.set(backend).is_ok(),
            "inject gated backend"
        );

        let listener = bind_listener(&socket_path);
        let lifecycle = Arc::new(Lifecycle::default());

        let loop_daemon = daemon.clone();
        let loop_flag = lifecycle.clone();

        let mut accept_task = tokio::spawn(async move {
            accept_loop(
                &listener,
                &loop_daemon,
                &loop_flag,
                None,
                Duration::from_secs(2),
                std::future::pending::<()>(),
            )
            .await;
        });

        let key = test_cache_key("shutdown-drain-gated-request");
        let remote_socket = socket_path.clone();
        let remote_client = tokio::spawn(async move {
            client_roundtrip(
                &remote_socket,
                &Request::RemoteCheck(RemoteCheckRequest {
                    entry_dir: config.store_dir().join(&key).to_string_lossy().into_owned(),
                    key,
                    crate_name: "serde".into(),
                    deadline_ms: Some(2_000),
                    shard_dir: None,
                }),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(1), head_started.notified())
            .await
            .expect("remote request reached gated HEAD");

        let shutdown_socket = socket_path.clone();
        let shutdown_client = tokio::spawn(async move {
            client_request_keep_open(&shutdown_socket, &Request::Shutdown).await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while lifecycle.accepting_calls() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shutdown handler set flag");

        let (shutdown_response, shutdown_stream) =
            tokio::time::timeout(Duration::from_secs(1), shutdown_client)
                .await
                .expect("shutdown acknowledgement was written")
                .expect("join shutdown client");
        assert!(shutdown_response.ok, "shutdown request should return ok");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut accept_task)
                .await
                .is_err(),
            "accept loop returned while an accepted response was still blocked"
        );

        assert!(
            daemon.prefetch_stopping.load(Ordering::Acquire),
            "prefetch admission must close before waiting for an accepted response"
        );
        assert!(
            daemon
                .spawn_prefetch_task(PrefetchOrigin::default(), async {
                    panic!("draining daemon admitted speculative work");
                })
                .is_none(),
            "draining daemon must reject new speculative tasks"
        );

        release_head.add_permits(1);
        let remote_response = tokio::time::timeout(Duration::from_secs(1), remote_client)
            .await
            .expect("in-flight response was written after gate release")
            .expect("join remote client");
        assert!(remote_response.ok, "remote request should complete");
        assert_eq!(remote_response.found, Some(false));
        tokio::time::timeout(Duration::from_secs(1), &mut accept_task)
            .await
            .expect("accept loop completed after draining handlers")
            .expect("join accept loop");
        drop(shutdown_stream);
    }

    #[tokio::test]
    async fn connection_handler_drain_aborts_silent_handler_at_deadline() {
        let mut handlers = tokio::task::JoinSet::new();
        handlers.spawn(std::future::pending::<()>());

        assert!(
            drain_connection_handlers(&mut handlers, Duration::from_millis(20)).await,
            "a silent handler must be aborted at the drain deadline"
        );
        assert!(handlers.is_empty(), "aborted handlers must still be joined");
    }

    #[tokio::test]
    async fn connection_handler_observation_distinguishes_panics_from_cancellation() {
        assert!(
            !observe_connection_handler(tokio::spawn(async {}).await),
            "a successful handler is not anomalous"
        );

        let panicked = tokio::spawn(async { panic!("expected handler panic") }).await;
        assert!(
            observe_connection_handler(panicked),
            "a handler panic must remain operationally visible"
        );

        let cancelled = tokio::spawn(std::future::pending::<()>());
        cancelled.abort();
        assert!(
            !observe_connection_handler(cancelled.await),
            "deadline cancellation is expected and must stay quiet"
        );
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
        let lifecycle = Arc::new(Lifecycle::default());

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
                &lifecycle,
                None,
                Duration::from_secs(1),
                std::future::pending::<()>(),
            ),
        )
        .await;

        assert!(
            outcome.is_ok(),
            "accept_loop did not break within 5s of a stop request (issue #288 regression)"
        );
        assert!(
            !lifecycle.accepting_calls(),
            "shutdown flag should be set after the stop request"
        );
        let resp = tokio::time::timeout(Duration::from_secs(5), client)
            .await
            .expect("accept loop never acknowledged shutdown")
            .expect("join client task");
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
            event_secs: None,
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

    pub(super) fn test_config(dir: &Path) -> Config {
        Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            remote_readonly: false,
            modified_input_guard: false,
            input_predictions: false,
            record_sessions: false,
            volume_stores: Vec::new(),
            windows_hardlink: false,
            shared_hardlink_restores: false,
            deferred_discovery: true,
            out_dir_alias: true,
            deferred_durability: false,
            daemon_publish: false,
            project_rules: crate::config::ProjectRules::default(),
            auto_gc: true,
            index_auto_compact: true,
            gc_evict_shared: false,
            storage_layout_advice: true,
            heartbeat_secs: 30,
            explain_miss: false,
            scheduler: true,
            test_lease: None,
            path_only_env_vars: Vec::new(),
            incremental_crates: Vec::new(),
            key_env_vars: Vec::new(),
            base_dirs: Vec::new(),
            cache_dir: dir.to_path_buf(),
            runtime_dir: dir.to_path_buf(),
            socket_path_override: None,
            max_size: 50 * 1024 * 1024, // 50 MiB
            remote: None,
            remote_error: None,
            disabled: false,
            cache_executables: false,
            cache_cc_links: false,
            trust_codegen_backends: false,
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

    fn latest_transfer(daemon: &Daemon) -> TransferEvent {
        daemon
            .recent_transfers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .back()
            .cloned()
            .expect("transfer event")
    }

    fn assert_v3_transfer_timestamps(transfer: &TransferEvent) {
        assert_eq!(transfer.schema, 5);
        assert!(
            transfer.started_at_unix_ms > 1_000_000_000_000,
            "transfer start must be Unix epoch milliseconds: {transfer:?}"
        );
        assert!(
            transfer.finished_at_unix_ms >= transfer.started_at_unix_ms,
            "transfer finish must not precede its start: {transfer:?}"
        );
        assert_eq!(
            transfer.timestamp,
            transfer.finished_at_unix_ms / 1_000,
            "legacy seconds timestamp must match the exact millisecond finish"
        );
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

    #[test]
    fn requests_that_carry_a_client_epoch_report_it() {
        for line in [
            r#"{"upload":{"key":"k","entry_dir":"d","client_epoch":7}}"#,
            r#"{"stats":{"include_entries":false,"sort_by":null,"event_hours":null,"client_epoch":7}}"#,
            r#"{"build_started":{"client_epoch":7}}"#,
        ] {
            let request: Request = serde_json::from_str(line).unwrap();
            assert_eq!(request.client_epoch(), 7, "{line}");
        }
        let publish = Request::PublishCc(Box::new(crate::daemon_publish::PublishCcRequest {
            client_epoch: 7,
            cache_key: "k".to_string(),
            crate_name: "a.c".to_string(),
            target: "x86_64".to_string(),
            files: Vec::new(),
            stdout: String::new(),
            stderr: String::new(),
            compile_time_ms: 0,
            publishes_to_remote: false,
            event: crate::events::BuildEvent::new_for_test("a.c", crate::events::EventResult::Miss),
            memo: None,
        }));
        assert_eq!(publish.client_epoch(), 7);
        assert_eq!(Request::Health.client_epoch(), 0);
    }

    #[test]
    fn only_build_requests_count_as_activity() {
        assert!(!Request::Shutdown.is_build_activity());
        let stats: Request = serde_json::from_str(
            r#"{"stats":{"include_entries":false,"sort_by":null,"event_hours":null}}"#,
        )
        .unwrap();
        assert!(matches!(stats, Request::Stats(_)));
        assert!(!stats.is_build_activity());
        assert!(!Request::Gc(GcRequest::automatic(0)).is_build_activity());
        assert!(!Request::GcV2(GcRequest::automatic(0)).is_build_activity());
        assert!(Request::HashFiles(HashFilesRequest { files: Vec::new() }).is_build_activity());
    }

    #[test]
    fn upload_retry_and_idle_timeout_truth_tables() {
        assert!(upload_result_is_terminal(None));
        assert!(upload_result_is_terminal(Some("local: missing payload")));
        assert!(!upload_result_is_terminal(Some("retryable: remote outage")));

        assert_eq!(daemon_idle_timeout(0), None);
        assert_eq!(daemon_idle_timeout(1), Some(Duration::from_secs(1)));
        assert_eq!(daemon_idle_timeout(60), Some(Duration::from_secs(60)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_main_binds_socket_and_handles_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.daemon_idle_timeout_secs = 0;
        config.prefetch_enabled = false;
        config.remote = Some(crate::config::RemoteConfig {
            prefix: "artifacts".into(),
            backend: crate::config::RemoteBackendConfig::Filesystem(
                crate::config::FilesystemRemoteConfig {
                    root: dir.path().join("remote"),
                    atomic_write_dir: dir.path().join("remote-staging"),
                },
            ),
        });
        let socket_path = config.socket_path();
        let coord = DaemonCoordFile::for_socket(&socket_path);
        let server_config = config.clone();
        let provenance = crate::config::config_file_provenance_at(dir.path().join("config.toml"));
        let server =
            tokio::spawn(async move { server_main(&server_config, &provenance, coord).await });

        let ready_socket = socket_path.clone();
        let ready = tokio::task::spawn_blocking(move || {
            kunobi_daemon::readiness::wait_until(
                Instant::now() + Duration::from_secs(5),
                |deadline| lifecycle_client::current_socket(&ready_socket, deadline),
            )
            .map(|proof| proof.is_some())
        })
        .await
        .unwrap()
        .unwrap();
        assert!(ready, "server_main must bind its configured socket");

        assert_eq!(
            read_daemon_state(&socket_path).unwrap().control_version,
            Some(kunobi_daemon::wire::VERSION),
            "stop must use the advertised binary DRAIN endpoint"
        );
        let response = client_roundtrip(
            &socket_path,
            &Request::BuildStarted(BuildStartedRequest {
                intent: kache_core::BuildIntent::default(),
                client_epoch: 0,
                session_id: "binary-drain-session".into(),
            }),
        )
        .await;
        assert!(response.ok, "build session must be accepted: {response:?}");
        assert!(!config.summary_log_path().exists());

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
        let summaries = events::read_summaries(&config.summary_log_path()).unwrap();
        assert_eq!(
            summaries.len(),
            1,
            "shutdown must finalize the session once"
        );
        assert_eq!(summaries[0].session_id, "binary-drain-session");
        assert_eq!(summaries[0].closure_reason, "shutdown");
        assert!(!summaries[0].incomplete);
        assert!(!summaries[0].cancelled);
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
    fn readiness_rejects_an_accepting_socket_that_does_not_answer() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("daemon.sock");
        let _listener = bind_sync_listener(&socket);
        let start = Instant::now();
        assert!(
            lifecycle_client::current_socket(&socket, start + Duration::from_millis(100))
                .unwrap()
                .is_none()
        );
        assert!(start.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn readiness_requires_a_successful_compatible_health_response() {
        for (response, accepted) in [
            (
                Response {
                    health: Some(DaemonHealth {
                        version: VERSION.into(),
                        build_epoch: build_epoch(),
                    }),
                    ..Response::ok()
                },
                true,
            ),
            (
                Response {
                    health: Some(DaemonHealth {
                        version: "old".into(),
                        build_epoch: 1,
                    }),
                    ..Response::ok()
                },
                false,
            ),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let socket = dir.path().join("daemon.sock");
            let listener = bind_listener(&socket);
            let server = tokio::spawn(async move {
                let peer = listener.accept().await.unwrap();
                let mut request = String::new();
                BufReader::new(&peer).read_line(&mut request).await.unwrap();
                assert!(matches!(
                    serde_json::from_str::<Request>(&request).unwrap(),
                    Request::Health
                ));
                let mut response = serde_json::to_vec(&response).unwrap();
                response.push(b'\n');
                (&peer).write_all(&response).await.unwrap();
            });
            let proof = tokio::task::spawn_blocking(move || {
                lifecycle_client::current_socket(&socket, Instant::now() + Duration::from_secs(5))
            })
            .await
            .unwrap()
            .unwrap();
            tokio::time::timeout(Duration::from_secs(5), server)
                .await
                .expect("readiness probe never completed the health exchange")
                .unwrap();
            assert_eq!(proof.is_some(), accepted);
        }
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
            control_version: None,
        };
        assert!(daemon_state_is_recent(&fresh));

        let stale = DaemonCoordState {
            control_version: None,
            pid: 1,
            build_epoch: build_epoch(),
            phase: DaemonPhase::Ready,
            updated_at_ms: now_millis()
                .saturating_sub(DAEMON_COORD_STALE_AFTER.as_millis() as u64 * 2),
        };
        assert!(!daemon_state_is_recent(&stale));

        // Clock moved backwards: a record stamped in the future is not fresh.
        let future = DaemonCoordState {
            pid: 1,
            build_epoch: build_epoch(),
            phase: DaemonPhase::Ready,
            updated_at_ms: now_millis() + DAEMON_COORD_STALE_AFTER.as_millis() as u64 * 2,
            control_version: None,
        };
        assert!(!daemon_state_is_recent(&future));
    }

    #[test]
    fn coordinator_cleanup_removes_only_this_process_and_build() {
        let root = tempfile::tempdir().unwrap();
        for (same_pid, same_build) in [(true, true), (false, true), (true, false), (false, false)] {
            let mut coord = DaemonCoordFile::for_socket(&root.path().join("daemon.sock"));
            if !same_pid {
                coord.pid = std::process::id() + 1;
            }
            if !same_build {
                coord.build_epoch = build_epoch().wrapping_add(1);
            }
            coord.write_phase(DaemonPhase::Ready).unwrap();
            let bytes = std::fs::read(&coord.path).unwrap();
            drop(DaemonCoordGuard::new(coord.path.clone()));
            if same_pid && same_build {
                assert!(!coord.path.exists(), "own record must be removed");
            } else {
                assert_eq!(
                    std::fs::read(&coord.path).unwrap(),
                    bytes,
                    "replacement record must survive"
                );
            }
        }
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

    #[cfg(unix)]
    #[test]
    fn pid_alive_rejects_broadcast_pids() {
        // This prunes the in-flight compile map. `kill(0, 0)` and `kill(-1, 0)`
        // succeed whenever anything signalable exists, so without the guard
        // entries recorded under a bogus PID would look alive forever.
        assert!(super::pid_alive(std::process::id()));

        assert!(!super::pid_alive(0), "0 is the caller's process group");
        assert!(!super::pid_alive(1), "1 is init/launchd, never a compile");
        assert!(
            !super::pid_alive(u32::MAX),
            "u32::MAX casts to the -1 broadcast"
        );
    }

    #[test]
    fn shutdown_wait_requires_ownership_release() {
        let root = tempfile::tempdir().unwrap();
        let socket = root.path().join("daemon.sock");
        let lock = kunobi_daemon::ProcessLock::try_acquire(daemon_run_lock_path(&socket))
            .unwrap()
            .unwrap();
        assert!(!wait_for_run_lock_release(&socket, Duration::from_millis(30)).unwrap());
        drop(lock);
        assert!(wait_for_run_lock_release(&socket, Duration::from_secs(1)).unwrap());
    }

    /// A missing run lock file means "nobody holds it", which is a different
    /// answer from "the probe failed": callers treat an error as unknown and
    /// stop, so collapsing the two would make a host that never ran a daemon
    /// indistinguishable from one whose lock could not be read.
    #[test]
    fn existing_run_lock_probe_separates_missing_from_unreadable() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("daemon.sock");

        assert!(!existing_daemon_run_lock_is_held(&socket_path).unwrap());
        // Answering must not have created the file it was asked about.
        assert!(!daemon_run_lock_path(&socket_path).exists());

        let lock = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(daemon_run_lock_path(&socket_path))
            .unwrap();

        // Present but free.
        assert!(!existing_daemon_run_lock_is_held(&socket_path).unwrap());

        lock.try_lock().unwrap();
        assert!(existing_daemon_run_lock_is_held(&socket_path).unwrap());
    }

    /// A lock file that cannot be opened must surface as an error, not as
    /// "nobody holds it".
    ///
    /// The probe special-cases exactly one failure — `NotFound`, meaning the
    /// file was never created — and propagates everything else. The test above
    /// covers missing, present-and-free, and held, but never an unreadable
    /// lock, so the `NotFound` guard could be widened to match every error and
    /// nothing would notice: an unreadable lock would then read as free, and
    /// doctor would report a host as clean precisely when it could not tell.
    ///
    /// A directory where the file belongs is the portable way to fail an open
    /// with something that is not `NotFound` (EISDIR on unix, access-denied on
    /// Windows) without depending on running as an unprivileged user, which
    /// chmod-based variants do.
    #[test]
    fn existing_run_lock_probe_propagates_an_unreadable_lock() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("daemon.sock");
        std::fs::create_dir_all(daemon_run_lock_path(&socket_path)).unwrap();

        assert!(
            existing_daemon_run_lock_is_held(&socket_path).is_err(),
            "an unopenable lock file must not be reported as unheld"
        );
    }

    /// A starting record is a waiting hint only while its owner holds the lock.
    #[test]
    fn starting_daemon_epoch_reports_only_a_live_starting_daemon() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();
        let state_path = daemon_state_path(&socket_path);

        // No coordinator file at all.
        assert_eq!(starting_daemon_epoch(&config), None);

        let mut state = DaemonCoordState {
            pid: std::process::id(),
            build_epoch: 4242,
            phase: DaemonPhase::Starting,
            updated_at_ms: now_millis(),
            control_version: None,
        };
        write_json_atomically(&state_path, &state).unwrap();

        // A fresh record naming a live process is exactly what PID reuse looks
        // like. Without the run lock it must not read as a starting daemon — and
        // probing must not create the lock file, which `doctor` would then report
        // as leftover cruft.
        assert_eq!(starting_daemon_epoch(&config), None);
        assert!(!daemon_run_lock_path(&socket_path).exists());

        let run_lock = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(daemon_run_lock_path(&socket_path))
            .unwrap();
        run_lock.try_lock().unwrap();
        assert_eq!(starting_daemon_epoch(&config), Some(4242));

        // Already serving: the socket answers, so this is not the starting window.
        state.phase = DaemonPhase::Ready;
        write_json_atomically(&state_path, &state).unwrap();
        assert_eq!(starting_daemon_epoch(&config), None);

        // Starting, but the heartbeat went cold — a crashed starter, not a live one.
        state.phase = DaemonPhase::Starting;
        state.updated_at_ms =
            now_millis().saturating_sub(DAEMON_COORD_STALE_AFTER.as_millis() as u64 * 2);
        write_json_atomically(&state_path, &state).unwrap();
        assert_eq!(starting_daemon_epoch(&config), None);

        // Fresh record, dead PID: the starter died between writing the file and
        // binding the socket. Use a reaped child rather than a made-up PID so
        // the process really is gone.
        let mut child = std::process::Command::new(if cfg!(windows) { "cmd" } else { "true" })
            .args(if cfg!(windows) {
                vec!["/C", "exit"]
            } else {
                vec![]
            })
            .spawn()
            .unwrap();
        let dead_pid = child.id();
        child.wait().unwrap();

        state.updated_at_ms = now_millis();
        state.pid = dead_pid;
        write_json_atomically(&state_path, &state).unwrap();
        assert_eq!(starting_daemon_epoch(&config), None);
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
            shard_dir: None,
        });
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, parsed);

        assert!(json.contains("\"remote_check\""));
        assert!(json.contains("\"key\":\"abc123\""));
        assert!(json.contains("\"entry_dir\":\"/tmp/store/abc123\""));
        assert!(
            !json.contains("shard_dir"),
            "absent shard_dir must stay off the wire so older daemons can ignore it"
        );
    }

    #[test]
    fn remote_check_serde_defaults_missing_shard_dir() {
        let parsed: Request = serde_json::from_str(
            r#"{"remote_check":{"key":"abc123","entry_dir":"/tmp/store/abc123"}}"#,
        )
        .unwrap();
        match parsed {
            Request::RemoteCheck(req) => assert_eq!(req.shard_dir, None),
            other => panic!("expected remote_check, got {other:?}"),
        }
    }

    #[test]
    fn remote_check_cache_dir_resolves_main_and_configured_shards() {
        let main = Path::new("/cache/main");
        let shard = PathBuf::from("/cache/shard");
        let volumes = [crate::config::VolumeStore {
            volume: "/mnt/vol/".into(),
            store: shard.clone(),
        }];
        assert_eq!(remote_check_cache_dir(main, &volumes, None).unwrap(), main);
        assert_eq!(
            remote_check_cache_dir(main, &volumes, Some("")).unwrap(),
            main
        );
        assert_eq!(
            remote_check_cache_dir(main, &volumes, Some("   ")).unwrap(),
            main
        );
        assert_eq!(
            remote_check_cache_dir(main, &volumes, Some("/cache/main")).unwrap(),
            main
        );
        assert_eq!(
            remote_check_cache_dir(main, &volumes, Some("/cache/shard")).unwrap(),
            shard.as_path()
        );
        assert_eq!(
            remote_check_cache_dir(main, &volumes, Some("/cache/other")).unwrap_err(),
            "remote-check shard_dir is not a configured volume store"
        );
        assert_eq!(remote_check_shard_dir_arg(main, main), None);
        assert_eq!(
            remote_check_shard_dir_arg(main, &shard),
            Some("/cache/shard".into())
        );
        assert!(remote_check_uses_main_store(main, main));
        assert!(!remote_check_uses_main_store(&shard, main));
        assert_eq!(
            remote_check_blobs_dir(main),
            main.join("store").join("blobs")
        );
        assert_eq!(
            remote_check_entry_dir(main, "abc"),
            main.join("store").join("abc")
        );
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
            pack_requests_total: 5,
            pack_bytes_downloaded: 4096,
            v3_requests_total: 2,
            v3_bytes_downloaded: 1024,
            pack_validation_failures: 1,
            pack_fallback_entries: 2,
            last_plan_wall_ms: 123,
            plan_wall_ms_total: 456,
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
        assert_eq!(
            cache.refresh_revision(),
            before_list.wrapping_add(1),
            "a point update must advance the LIST-staleness revision"
        );

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
    fn gc_does_not_hold_the_daemon_store_mutex() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Arc::new(Daemon::new(test_config(dir.path())));
        daemon.with_store(|_| Ok(())).unwrap();
        let main_store = daemon.store.get().unwrap().lock().unwrap();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker_daemon = Arc::clone(&daemon);
        let worker = std::thread::spawn(move || {
            done_tx
                .send(worker_daemon.run_gc(
                    GcPolicy::Automatic { max_age_hours: 0 },
                    GcDriver::Requested,
                ))
                .unwrap();
        });

        let finished = done_rx.recv_timeout(Duration::from_secs(2));
        drop(main_store);
        worker.join().unwrap();
        assert!(
            finished
                .expect("GC must finish while the daemon store is in use")
                .is_ok()
        );
    }

    /// The daemon's sweep is what flushes entries a miss stored without an
    /// fsync: it marks them durable, does nothing once the queue is empty,
    /// and stands aside while another flusher holds the lock.
    #[test]
    fn the_daemon_sweep_flushes_entries_pending_durability() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.deferred_durability = true;
        let store = Store::open(&config).unwrap();
        let output = dir.path().join("out.rlib");
        std::fs::write(&output, b"artifact-bytes").unwrap();
        store
            .put(
                "pending",
                "pending_crate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(output, "libout.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        assert_eq!(store.pending_durability().unwrap(), 1);

        // Another flusher holds the lock: the sweep leaves the entry alone.
        let held = store
            .try_durability_flush_lock()
            .unwrap()
            .expect("flush lock");
        let daemon = Daemon::new(config);
        daemon.flush_pending_durability();
        assert_eq!(
            store.pending_durability().unwrap(),
            1,
            "a held lock means another flusher is draining the queue"
        );
        drop(held);

        daemon.flush_pending_durability();
        assert_eq!(store.pending_durability().unwrap(), 0);
        // Nothing pending: the sweep is a no-op and takes no lock.
        daemon.flush_pending_durability();
        assert_eq!(store.pending_durability().unwrap(), 0);
        assert!(store.try_durability_flush_lock().unwrap().is_some());
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
                &[(src_file.clone(), "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        store.remove_clone_for_test(&src_file);
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
            .run_gc(
                GcPolicy::Automatic {
                    max_age_hours: daemon.config.gc_max_age_hours,
                },
                GcDriver::Requested,
            )
            .unwrap();
        assert!(
            stats.total.entries_evicted > 0,
            "should have evicted at least 1 entry"
        );
    }

    /// kunobi-ninja/kache#1126: the daemon sweep removes stale key locks and
    /// records the housekeeping counts.
    #[test]
    fn automatic_gc_runs_store_housekeeping_and_records_it() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());

        // Two stale key locks with no entry, one claimed just now.
        std::fs::create_dir_all(config.store_dir()).unwrap();
        let lock_path = |seed: u8| {
            config
                .store_dir()
                .join(format!("{}.lock", blake3::hash(&[seed]).to_hex()))
        };
        for seed in [1, 2, 3] {
            std::fs::write(lock_path(seed), b"1").unwrap();
        }
        for seed in [1, 2] {
            std::fs::OpenOptions::new()
                .write(true)
                .open(lock_path(seed))
                .unwrap()
                .set_modified(std::time::SystemTime::now() - std::time::Duration::from_secs(7200))
                .unwrap();
        }

        let daemon = Daemon::new(config.clone());
        let report = daemon
            .run_gc(GcPolicy::Automatic { max_age_hours: 0 }, GcDriver::Periodic)
            .unwrap();
        assert_eq!(
            report.total.housekeeping,
            Some(crate::store::HousekeepingStats {
                key_locks_removed: 2,
                key_locks_remaining: 1,
                predictions_pruned: 0,
                file_hashes_pruned: 0,
            })
        );
        assert!(!lock_path(1).exists());
        assert!(!lock_path(2).exists());
        assert!(lock_path(3).exists());
        let recorded = crate::report::read_gc_stats(&config.cache_dir).expect("run recorded");
        assert_eq!(recorded.key_locks_removed, Some(2));
        assert_eq!(recorded.key_locks_remaining, Some(1));
        assert_eq!(recorded.predictions_pruned, Some(0));
        assert_eq!(recorded.file_hashes_pruned, Some(0));
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
                &[(src_file.clone(), "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        store.remove_clone_for_test(&src_file);
        store.set_last_accessed_for_test("stale_key", "-2 hours");
        drop(store);

        let daemon = Daemon::new(config);
        let stats = daemon
            .run_gc(
                GcPolicy::Automatic {
                    max_age_hours: daemon.config.gc_max_age_hours,
                },
                GcDriver::Requested,
            )
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
            .run_gc(
                GcPolicy::Automatic {
                    max_age_hours: daemon.config.gc_max_age_hours,
                },
                GcDriver::Requested,
            )
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
                &[(old_file.clone(), "old.rlib".into())],
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
                &[(fresh_file.clone(), "fresh.rlib".into())],
                "",
                "",
            )
            .unwrap();
        store.remove_clone_for_test(&old_file);
        store.remove_clone_for_test(&fresh_file);
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
                &[(src_file.clone(), "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        store.remove_clone_for_test(&src_file);
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

    /// Upload-triggered eviction is a GC driver too: without a record, the
    /// evictions it makes (and the ones it fails) never reach gc_stats.json.
    #[test]
    fn upload_triggered_eviction_records_its_run() {
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
                &[(src_file.clone(), "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        store.remove_clone_for_test(&src_file);
        store.set_last_accessed_for_test("upload_evict_key", "-1 hour");

        Daemon::new(config).maybe_evict_after_upload();

        let recorded = crate::report::read_gc_stats(dir.path()).expect("gc_stats.json written");
        assert_eq!(recorded.source, "daemon");
        assert_eq!(recorded.entries_evicted, 1);
    }

    /// Store an idle `size`-byte entry for the upload-eviction tests; with
    /// `retained`, a target directory still hardlinks its blob.
    fn put_upload_evict_entry(
        store: &Store,
        dir: &std::path::Path,
        key: &str,
        size: usize,
        retained: bool,
    ) {
        let src_file = dir.join(format!("{key}.rlib"));
        std::fs::write(&src_file, &key.repeat(size)[..size]).unwrap();
        store
            .put(
                key,
                "testcrate",
                &["lib".into()],
                &[],
                "host",
                "dev",
                &[(src_file.clone(), "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        store.remove_clone_for_test(&src_file);
        // A retained entry is one a build is using: no sweep may evict it
        // yet, so it keeps the store over budget.
        let last_used = if retained { "+0 seconds" } else { "-1 hour" };
        store.set_last_accessed_for_test(key, last_used);
    }

    /// Hardlink `key`'s blob into a stand-in target directory, which then
    /// holds its blocks.
    fn hold_in_target_dir(store: &Store, dir: &std::path::Path, key: &str) {
        let meta = store.get(key).unwrap().unwrap();
        std::fs::hard_link(
            store.blob_path(&meta.files[0].hash),
            dir.join(format!("{key}-target.rlib")),
        )
        .unwrap();
        store.set_last_accessed_for_test(key, "-1 hour");
    }

    /// Bytes target directories hold are not pressure: a hint sweeps, finds
    /// them held, evicts nothing else, and leaves no backoff to wait out
    /// (#1206). It used to evict every other entry and back off.
    #[test]
    fn a_gc_hint_leaves_bytes_held_by_target_directories_alone() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 1000;
        let store = Store::open(&config).unwrap();
        for i in 0..6 {
            let key = format!("held_{i}");
            put_upload_evict_entry(&store, dir.path(), &key, 200, false);
            hold_in_target_dir(&store, dir.path(), &key);
        }
        put_upload_evict_entry(&store, dir.path(), "evictable", 50, false);

        let daemon = Daemon::new(config.clone());
        assert!(daemon.handle_request_sync(&Request::GcHint).ok);
        assert!(store.contains("evictable") && store.contains("held_0"));
        let stats = crate::report::read_gc_stats(dir.path()).unwrap();
        assert_eq!(stats.entries_evicted, 0);
        assert_eq!(
            crate::wrapper::auto_gc_backoff_interval_for_test(dir.path()),
            Some(0),
            "the held bytes are on record, with nothing to back off from"
        );
        assert!(!crate::wrapper::auto_gc_sweep_due(
            &config,
            store.physical_size().unwrap()
        ));
    }

    /// Every upload used to start another full sweep of a store the last
    /// sweep had already failed to bring under budget.
    #[test]
    fn upload_triggered_eviction_waits_out_the_auto_gc_backoff() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 1000;
        let store = Store::open(&config).unwrap();
        for i in 0..6 {
            put_upload_evict_entry(&store, dir.path(), &format!("retained_{i}"), 200, true);
        }
        put_upload_evict_entry(&store, dir.path(), "evictable", 50, false);

        let daemon = Daemon::new(config.clone());
        daemon.maybe_evict_after_upload();
        assert!(!store.contains("evictable"));
        assert!(store.contains("retained_0"));
        assert!(
            dir.path().join("auto-gc-backoff.json").exists(),
            "a sweep that leaves the store over budget records a backoff"
        );

        // Growth inside the slack: nothing a sweep could not already free.
        put_upload_evict_entry(&store, dir.path(), "next", 50, false);
        daemon.maybe_evict_after_upload();
        assert!(
            store.contains("next"),
            "the next upload must not sweep again during the backoff"
        );
    }

    /// The post-upload check used to start at 100% of `max_size` while the
    /// wrapper waited for 110%, so the two alternated on a store in between.
    #[test]
    fn upload_triggered_eviction_starts_one_byte_above_the_shared_trigger() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 1000;
        let store = Store::open(&config).unwrap();
        put_upload_evict_entry(&store, dir.path(), "at_the_trigger", 1100, false);
        assert_eq!(store.physical_size().unwrap(), 1100);
        assert!(!crate::wrapper::auto_gc_sweep_due(&config, 1100));

        let daemon = Daemon::new(config.clone());
        daemon.maybe_evict_after_upload();
        assert!(store.contains("at_the_trigger"));
        assert!(crate::report::read_gc_stats(dir.path()).is_none());

        put_upload_evict_entry(&store, dir.path(), "x", 1, false);
        assert_eq!(store.physical_size().unwrap(), 1101);
        assert!(crate::wrapper::auto_gc_sweep_due(&config, 1101));
        daemon.maybe_evict_after_upload();
        assert!(!store.contains("at_the_trigger"));
        assert!(crate::report::read_gc_stats(dir.path()).is_some());
    }

    #[test]
    fn upload_triggered_eviction_clears_the_backoff_once_the_store_fits() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 1000;
        let store = Store::open(&config).unwrap();
        put_upload_evict_entry(&store, dir.path(), "evictable", 1200, false);
        crate::wrapper::record_auto_gc_outcome(&config, 1200, 0);
        crate::wrapper::expire_auto_gc_backoff_for_test(dir.path());

        Daemon::new(config).maybe_evict_after_upload();
        assert!(!store.contains("evictable"));
        assert_eq!(
            crate::wrapper::auto_gc_backoff_interval_for_test(dir.path()),
            None
        );
    }

    #[test]
    fn gc_hints_coalesce_while_a_sweep_is_pending() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Daemon::new(test_config(dir.path()));
        assert!(daemon.claim_gc_hint());
        assert!(!daemon.claim_gc_hint(), "a pending sweep covers this hint");
        daemon.run_hinted_sweep();
        assert!(
            daemon.claim_gc_hint(),
            "a finished sweep releases the claim"
        );
    }

    #[test]
    fn a_gc_hint_sweeps_under_size_pressure_and_honours_the_backoff() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 1000;
        let store = Store::open(&config).unwrap();
        for i in 0..6 {
            put_upload_evict_entry(&store, dir.path(), &format!("retained_{i}"), 200, true);
        }
        put_upload_evict_entry(&store, dir.path(), "evictable", 50, false);

        let daemon = Daemon::new(config);
        assert!(daemon.handle_request_sync(&Request::GcHint).ok);
        assert!(!store.contains("evictable"));
        assert_eq!(
            crate::report::read_gc_stats(dir.path()).unwrap().source,
            "daemon"
        );
        assert_eq!(
            crate::wrapper::auto_gc_backoff_interval_for_test(dir.path()),
            Some(600),
            "a hinted sweep that leaves the store over budget records the backoff"
        );

        put_upload_evict_entry(&store, dir.path(), "next", 50, false);
        assert!(daemon.handle_request_sync(&Request::GcHint).ok);
        assert!(
            store.contains("next"),
            "a hint during the backoff is a no-op"
        );

        crate::wrapper::expire_auto_gc_backoff_for_test(dir.path());
        assert!(daemon.handle_request_sync(&Request::GcHint).ok);
        assert!(!store.contains("next"));
        assert_eq!(
            crate::wrapper::auto_gc_backoff_interval_for_test(dir.path()),
            Some(1200)
        );
    }

    #[test]
    fn a_gc_hint_does_nothing_at_the_trigger_and_clears_the_backoff_once_the_store_fits() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 1000;
        let store = Store::open(&config).unwrap();
        put_upload_evict_entry(&store, dir.path(), "at_the_trigger", 1100, false);
        let daemon = Daemon::new(config.clone());
        assert!(daemon.handle_request_sync(&Request::GcHint).ok);
        assert!(store.contains("at_the_trigger"));

        put_upload_evict_entry(&store, dir.path(), "x", 1, false);
        crate::wrapper::record_auto_gc_outcome(&config, 1101, 0);
        crate::wrapper::expire_auto_gc_backoff_for_test(dir.path());
        assert!(daemon.handle_request_sync(&Request::GcHint).ok);
        assert!(!store.contains("at_the_trigger"));
        assert_eq!(
            crate::wrapper::auto_gc_backoff_interval_for_test(dir.path()),
            None
        );
    }

    #[test]
    fn gc_hint_wire_name_and_ack_timeout_are_pinned() {
        assert_eq!(
            serde_json::to_string(&Request::GcHint).unwrap(),
            "\"gc_hint\""
        );
        assert!(Request::GcHint.is_build_activity());
        assert_eq!(GC_HINT_ACK_TIMEOUT, Duration::from_millis(500));
    }

    #[test]
    fn a_gc_hint_counts_as_accepted_only_on_an_ok_reply() {
        assert!(gc_hint_accepted(Ok("{\"ok\":true}\n".into())));
        // A daemon from before the hint rejects the unknown request.
        assert!(!gc_hint_accepted(Ok(
            "{\"ok\":false,\"error\":\"invalid request\"}\n".into()
        )));
        assert!(!gc_hint_accepted(Ok("not json".into())));
        assert!(!gc_hint_accepted(Err(anyhow::anyhow!("timed out"))));
    }

    #[test]
    fn send_gc_hint_reports_no_daemon_without_starting_one() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        assert!(!send_gc_hint(&config));
        assert!(!crate::transport::is_reachable(&config.socket_path()));
    }

    /// The daemon acknowledges the hint before it sweeps, then sweeps off the
    /// connection.
    #[tokio::test]
    async fn send_gc_hint_is_acknowledged_and_the_daemon_sweeps() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 1000;
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();
        let store = Store::open(&config).unwrap();
        put_upload_evict_entry(&store, dir.path(), "evictable", 1200, false);

        let listener = bind_listener(&socket_path);
        let daemon = Arc::new(Daemon::new(config.clone()));
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept");
            handle_connection(stream, &daemon, &Arc::new(Lifecycle::default()))
                .await
                .expect("handle_connection");
        });

        let cfg = config.clone();
        let accepted = tokio::task::spawn_blocking(move || send_gc_hint(&cfg))
            .await
            .unwrap();
        assert!(accepted);
        // Bounded: if no hint ever reached the socket, `accept` would wait
        // forever and the test would hang instead of failing.
        tokio::time::timeout(Duration::from_secs(10), server)
            .await
            .expect("the daemon never received the hint")
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        while store.contains("evictable") {
            assert!(Instant::now() < deadline, "the hinted sweep never ran");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Entries for the periodic-sweep tests: a store over budget on retained
    /// bytes, one entry past the age policy and one idle entry a size pass
    /// would evict.
    fn seed_periodic_gc_store(config: &Config, dir: &Path) -> Store {
        let store = Store::open(config).unwrap();
        for i in 0..6 {
            put_upload_evict_entry(&store, dir, &format!("retained_{i}"), 200, true);
        }
        put_upload_evict_entry(&store, dir, "old", 50, false);
        store.set_last_accessed_for_test("old", "-48 hours");
        put_upload_evict_entry(&store, dir, "idle", 50, false);
        store
    }

    /// Age eviction is retention policy, not size pressure: it stays on the
    /// timer while the size pass waits out the shared backoff.
    #[test]
    fn periodic_gc_skips_its_size_pass_under_the_backoff_and_still_expires_by_age() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 1000;
        let store = seed_periodic_gc_store(&config, dir.path());
        crate::wrapper::record_auto_gc_outcome(&config, store.physical_size().unwrap(), 0);

        let daemon = Daemon::new(config);
        let policy = GcPolicy::Automatic { max_age_hours: 24 };
        let report = daemon.run_gc(policy, GcDriver::Periodic).unwrap();
        assert_eq!(report.age.entries_evicted, 1);
        assert_eq!(report.size.entries_evicted, 0);
        assert!(!store.contains("old"));
        assert!(
            store.contains("idle"),
            "the size pass waits out the backoff"
        );

        // `kache gc` ignores the backoff and leaves it as it found it.
        let report = daemon.run_gc(policy, GcDriver::Requested).unwrap();
        assert_eq!(report.size.entries_evicted, 1);
        assert!(!store.contains("idle"));
        assert_eq!(
            crate::wrapper::auto_gc_backoff_interval_for_test(dir.path()),
            Some(600)
        );
    }

    #[test]
    fn periodic_gc_records_where_its_size_pass_left_the_store() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 1000;
        let store = seed_periodic_gc_store(&config, dir.path());

        let daemon = Daemon::new(config.clone());
        let policy = GcPolicy::Automatic { max_age_hours: 0 };
        let report = daemon.run_gc(policy, GcDriver::Periodic).unwrap();
        assert_eq!(report.size.entries_evicted, 2);
        assert_eq!(
            crate::wrapper::auto_gc_backoff_interval_for_test(dir.path()),
            Some(600),
            "the retained entries keep the store over budget"
        );

        crate::wrapper::expire_auto_gc_backoff_for_test(dir.path());
        daemon.run_gc(policy, GcDriver::Periodic).unwrap();
        assert_eq!(
            crate::wrapper::auto_gc_backoff_interval_for_test(dir.path()),
            Some(1200)
        );

        // The builds finish with the retained entries: the next due sweep
        // fits the store.
        for i in 0..6 {
            store.set_last_accessed_for_test(&format!("retained_{i}"), "-1 hour");
        }
        crate::wrapper::expire_auto_gc_backoff_for_test(dir.path());
        daemon.run_gc(policy, GcDriver::Periodic).unwrap();
        assert!(store.physical_size().unwrap() <= 900);
        assert_eq!(
            crate::wrapper::auto_gc_backoff_interval_for_test(dir.path()),
            None
        );
    }

    #[test]
    fn periodic_gc_size_pass_starts_one_byte_above_the_shared_trigger() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 1000;
        let store = Store::open(&config).unwrap();
        put_upload_evict_entry(&store, dir.path(), "at_the_trigger", 1100, false);

        let daemon = Daemon::new(config);
        let policy = GcPolicy::Automatic { max_age_hours: 0 };
        let report = daemon.run_gc(policy, GcDriver::Periodic).unwrap();
        assert_eq!(report.size.entries_evicted, 0);
        assert!(store.contains("at_the_trigger"));

        put_upload_evict_entry(&store, dir.path(), "x", 1, false);
        let report = daemon.run_gc(policy, GcDriver::Periodic).unwrap();
        assert!(report.size.entries_evicted >= 1);
        assert!(!store.contains("at_the_trigger"));
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
            shard_dir: None,
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
    async fn test_handle_remote_check_rejects_invalid_crate_name() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon = Daemon::new(config);
        let invalid_crate = RemoteCheckRequest {
            entry_dir: "/unused".into(),
            key: test_cache_key("invalid-remote-crate"),
            crate_name: "../escape".into(),
            deadline_ms: None,
            shard_dir: None,
        };
        let resp = daemon.handle_remote_check(&invalid_crate).await;
        assert!(!resp.ok);
        assert_eq!(resp.error.as_deref(), Some("invalid crate name"));
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
            shard_dir: None,
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
            .run_gc(
                GcPolicy::Automatic {
                    max_age_hours: daemon.config.gc_max_age_hours,
                },
                GcDriver::Requested,
            )
            .unwrap();
        assert_eq!(stats.total.entries_evicted, 0);
    }

    #[test]
    fn gc_pinned_lower_bound_is_policy_aware() {
        assert_eq!(
            gc_entries_pinned_lower_bound(GcPolicy::ExplicitAge { hours: 24 }, 9, 2, 8),
            2
        );

        for (duplicate, age, size) in [(7, 2, 3), (2, 7, 3), (2, 3, 7)] {
            assert_eq!(
                gc_entries_pinned_lower_bound(
                    GcPolicy::Automatic { max_age_hours: 24 },
                    duplicate,
                    age,
                    size,
                ),
                7
            );
        }
        assert_eq!(
            gc_entries_pinned_lower_bound(GcPolicy::Automatic { max_age_hours: 0 }, 0, 0, 0,),
            0
        );
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
            .run_gc(
                GcPolicy::Automatic {
                    max_age_hours: daemon.config.gc_max_age_hours,
                },
                GcDriver::Requested,
            )
            .unwrap();
        assert_eq!(stats.total.entries_evicted, 0);
        assert!(!incremental_dir.exists());

        std::fs::create_dir_all(&incremental_dir).unwrap();
        std::fs::write(incremental_dir.join("junk"), b"tmp2").unwrap();

        let stats = daemon
            .run_gc(
                GcPolicy::Automatic {
                    max_age_hours: daemon.config.gc_max_age_hours,
                },
                GcDriver::Requested,
            )
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
                shard_dir: None,
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
        let result =
            send_remote_check(&config, "some_key", Path::new("/tmp/test"), "unknown", None);
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
            event_secs: None,
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

    /// #897: a current client sends the window in seconds beside the rounded
    /// hours it sends for older daemons. The daemon must filter on the
    /// seconds, otherwise `--since 15m` silently becomes a 1h window.
    #[test]
    fn handle_stats_filters_on_event_secs_over_event_hours() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let now = chrono::Utc::now();
        let mut recent = events::BuildEvent::new_for_test("serde", events::EventResult::LocalHit);
        recent.ts = now - chrono::Duration::minutes(10);
        let mut old = events::BuildEvent::new_for_test("tokio", events::EventResult::Miss);
        old.ts = now - chrono::Duration::minutes(40);
        events::log_event(&config.event_log_path(), &recent).unwrap();
        events::log_event(&config.event_log_path(), &old).unwrap();
        let daemon = Daemon::new(config);

        let request = |event_hours: Option<u64>, event_secs: Option<u64>| StatsRequest {
            include_entries: false,
            include_summaries: false,
            sort_by: None,
            event_hours,
            event_secs,
            client_epoch: 0,
        };

        let narrow = daemon
            .handle_stats(&request(Some(1), Some(900)))
            .stats
            .unwrap();
        assert_eq!(narrow.events.local_hits, 1);
        assert_eq!(narrow.events.misses, 0, "40 minutes ago is outside 15m");

        let legacy = daemon.handle_stats(&request(Some(1), None)).stats.unwrap();
        assert_eq!(legacy.events.local_hits, 1);
        assert_eq!(
            legacy.events.misses, 1,
            "an older client's hours still apply"
        );

        let default = daemon.handle_stats(&request(None, None)).stats.unwrap();
        assert_eq!(default.events.misses, 1, "no window at all means 24h");
    }

    #[test]
    fn stats_request_window_prefers_secs_then_hours_then_default() {
        use crate::since::SinceWindow;
        let request = |event_hours: Option<u64>, event_secs: Option<u64>| StatsRequest {
            include_entries: false,
            include_summaries: false,
            sort_by: None,
            event_hours,
            event_secs,
            client_epoch: 0,
        };
        assert_eq!(request(Some(24), Some(900)).window().secs(), 900);
        assert_eq!(request(Some(2), None).window().secs(), 7200);
        assert_eq!(request(None, None).window(), SinceWindow::DEFAULT);
        assert_eq!(
            request(Some(u64::MAX), None).window(),
            SinceWindow::DEFAULT,
            "an overflowing hour count falls back rather than wrapping"
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
            event_secs: None,
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
            event_secs: None,
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
    fn handle_hash_files_reports_hash_io_error() {
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
                .contains("hashing"),
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
            event_secs: None,
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
            event_secs: None,
            client_epoch: 0,
        });
        let resp = daemon.handle_request_sync(&req);
        assert!(resp.ok);
        assert!(resp.stats.is_some());
    }

    #[test]
    fn readiness_reply_requires_success_and_identity() {
        for response in [
            "",
            r#"{"ok":false,"health":{"version":"v1","build_epoch":7}}"#,
            r#"{"ok":true}"#,
            r#"{"ok":true,"health":{"version":"v1"}}"#,
        ] {
            assert!(parse_daemon_health(response).is_err(), "{response}");
        }
        assert_eq!(
            parse_daemon_health(r#"{"ok":true,"health":{"version":"v1","build_epoch":7}}"#)
                .unwrap(),
            DaemonHealth {
                version: "v1".into(),
                build_epoch: 7
            }
        );
        assert_eq!(
            serde_json::to_string(&Request::Health).unwrap(),
            r#""health""#
        );
        assert!(!Request::Health.is_build_activity());
    }

    #[tokio::test]
    async fn readiness_roundtrip_does_not_wait_for_the_store() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon = Arc::new(Daemon::new(config.clone()));
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let busy = daemon.clone();
        let maintenance = std::thread::spawn(move || {
            let _guard = busy.store_lock().unwrap().lock().unwrap();
            locked_tx.send(()).unwrap();
            let _ = release_rx.recv();
        });
        locked_rx.recv_timeout(Duration::from_secs(30)).unwrap();
        let listener = bind_listener(&config.socket_path());
        let serving = daemon.clone();
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.unwrap();
            handle_connection(stream, &serving, &Arc::new(Lifecycle::default())).await
        });
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            tokio::task::spawn_blocking(move || send_health_request(&config)),
        )
        .await;
        drop(release_tx);
        maintenance.join().unwrap();
        server.abort();
        let _ = server.await;
        let health = result
            .expect("readiness waited for the store")
            .unwrap()
            .unwrap();
        assert_eq!(health.version, VERSION);
        assert_eq!(health.build_epoch, build_epoch());
        assert_eq!(
            daemon.handle_request_sync(&Request::Health).health,
            Some(health)
        );
    }

    #[test]
    fn daemon_runtime_exits_while_aborted_maintenance_is_blocked() {
        let (release_tx, release_rx) = mpsc::channel();
        let (started_tx, started_rx) = mpsc::channel();
        let (stopped_tx, stopped_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap();
            let result = run_daemon_runtime(runtime, async move {
                let maintenance = tokio::task::spawn_blocking(move || {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                });
                started_rx.recv_timeout(Duration::from_secs(30)).unwrap();
                maintenance.abort();
                Err(anyhow::anyhow!("server result must survive shutdown"))
            });
            stopped_tx.send(result).unwrap();
        });
        // Keep the blocking job parked until shutdown reports completion.
        // Release it even on failure so this regression never hangs the suite.
        let result = stopped_rx.recv_timeout(Duration::from_secs(10));
        release_tx.send(()).unwrap();
        thread.join().unwrap();
        assert_eq!(
            result
                .expect("runtime waited for aborted maintenance")
                .unwrap_err()
                .to_string(),
            "server result must survive shutdown"
        );
        let runtime = tokio::runtime::Runtime::new().unwrap();
        run_daemon_runtime(runtime, async { Ok(()) }).unwrap();
    }

    fn stats_at_epoch(epoch: u64) -> StatsResponse {
        serde_json::from_value(serde_json::json!({
            "total_size": 0, "max_size": 0, "entry_count": 0,
            "entries": null, "build_epoch": epoch,
            "events": { "local_hits": 0, "remote_hits": 0, "misses": 0, "errors": 0,
                        "total_elapsed_ms": 0 }
        }))
        .unwrap()
    }

    #[test]
    fn stats_refresh_preserves_current_or_unknown_epoch_without_restart() {
        for (client, daemon) in [(20, 20), (20, 21), (0, 10), (20, 0)] {
            let stats = stats_at_epoch(daemon);
            assert_eq!(
                refresh_stale_response(
                    stats.clone(),
                    client,
                    |stats| stats.build_epoch,
                    || panic!("current daemon must not restart"),
                    || panic!("current daemon must not refetch"),
                )
                .unwrap(),
                stats
            );
        }
    }

    #[test]
    fn stats_refresh_returns_only_the_replacement_response() {
        let fresh = stats_at_epoch(20);
        let mut restarted = false;
        let result = refresh_stale_response(
            stats_at_epoch(10),
            20,
            |stats| stats.build_epoch,
            || {
                restarted = true;
                Ok(true)
            },
            || Ok(fresh.clone()),
        )
        .unwrap();
        assert!(restarted);
        assert_eq!(result, fresh);
    }

    #[test]
    fn stats_refresh_rejects_failed_restart_without_refetch() {
        for restart in [Ok(false), Err(anyhow::anyhow!("spawn failed"))] {
            let error = refresh_stale_response(
                stats_at_epoch(10),
                20,
                |stats| stats.build_epoch,
                || restart,
                || panic!("failed restart must not refetch"),
            )
            .unwrap_err();
            assert!(matches!(
                error.to_string().as_str(),
                "replacement daemon did not become ready" | "spawn failed"
            ));
        }
    }

    #[test]
    fn stats_refresh_rejects_missing_or_still_stale_replacement() {
        let error = refresh_stale_response(
            stats_at_epoch(10),
            20,
            |stats| stats.build_epoch,
            || Ok(true),
            || Err(anyhow::anyhow!("socket closed")),
        )
        .unwrap_err();
        assert_eq!(
            format!("{error:#}"),
            "reading replacement daemon response: socket closed"
        );
        let error = refresh_stale_response(
            stats_at_epoch(10),
            20,
            |stats| stats.build_epoch,
            || Ok(true),
            || Ok(stats_at_epoch(10)),
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "replacement daemon is still older than this client"
        );
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
                event_secs: None,
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
            let _ = handle_connection(stream, &daemon, &Arc::new(Lifecycle::default())).await;
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
                    identity_key: None,
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
                    shard_dir: None,
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

    fn seed_cc_store_entry(config: &Config, cache_key: &str, crate_name: &str, dir: &Path) {
        let store = Store::open(config).unwrap();
        let src = dir.join(format!("{cache_key}-src"));
        std::fs::create_dir_all(&src).unwrap();
        let artifact = src.join("foo.o");
        std::fs::write(&artifact, b"cc object bytes").unwrap();
        store
            .put_with_compile_time_independent(
                cache_key,
                crate_name,
                &[],
                &[],
                "x86_64-unknown-linux-gnu",
                "",
                &[(artifact, "foo.o".to_string())],
                "",
                "",
                0,
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
                event_secs: None,
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
            handle_connection(stream, &daemon, &Arc::new(Lifecycle::default()))
                .await
                .expect("handle_connection");
        });

        // send_stats_request is a blocking sync client; run it off the runtime.
        let cfg = config.clone();
        let stats = tokio::task::spawn_blocking(move || {
            send_stats_request(
                &cfg,
                true,
                Some("size"),
                Some(crate::since::SinceWindow::DEFAULT),
            )
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
                handle_connection(stream, &daemon, &Arc::new(Lifecycle::default()))
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
            event_secs: None,
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
                let _ = handle_connection(stream, &daemon, &Arc::new(Lifecycle::default())).await;
            }
        });

        let cfg = config.clone();
        let missing = "d".repeat(64);
        let entry_dir = cfg.store_dir().join(&missing);
        let result = tokio::task::spawn_blocking(move || {
            send_remote_check(&cfg, &missing, &entry_dir, "crate", None)
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
                let _ = handle_connection(stream, &daemon, &Arc::new(Lifecycle::default())).await;
            }
        });

        let cfg = config.clone();
        let key = "e".repeat(64);
        let entry_dir = cfg.store_dir().join(&key);
        let result = tokio::task::spawn_blocking(move || {
            send_remote_check(&cfg, &key, &entry_dir, "crate", None)
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
            handle_connection(stream, &daemon, &Arc::new(Lifecycle::default()))
                .await
                .expect("handle_connection");
        });

        let cfg = config.clone();
        let result = tokio::task::spawn_blocking(move || send_shutdown_request(&cfg))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("shutdown client never completed the exchange")
            .unwrap();
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

    const ROW_IDENTITY: &str = "shared-workspace-v1:0123";

    fn test_prediction_row() -> crate::prediction_share::SharedPrediction {
        crate::prediction_share::SharedPrediction::Portable(crate::cache_key::PortablePrediction {
            schema: crate::cache_key::PORTABLE_PREDICTION_SCHEMA,
            sources: vec![crate::cache_key::Portable::Literal("kt/src/lib.rs".into())],
            env_deps: vec![],
            tree: "tree".to_string(),
        })
    }

    /// A daemon on `config` whose remote is the in-memory `backend`.
    fn daemon_on(
        config: Config,
        backend: &Arc<dyn crate::remote_backend::RemoteBackend>,
    ) -> Arc<Daemon> {
        let daemon = Arc::new(Daemon::new(config));
        daemon.set_remote_backend_for_test(Arc::clone(backend));
        daemon
    }

    fn fetch_request(identity: &str) -> PredictionFetchRequest {
        PredictionFetchRequest {
            identity: identity.to_string(),
        }
    }

    #[tokio::test]
    async fn a_prediction_row_published_by_one_daemon_is_fetched_by_another() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let backend = test_remote_backend();
        let writer = daemon_on(config.clone(), &backend);
        let reply = writer.handle_prediction_publish(PredictionPublishRequest {
            identity: ROW_IDENTITY.to_string(),
            row: test_prediction_row(),
        });
        assert!(reply.ok);

        let reader = daemon_on(config, &backend);
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let fetched = reader
                .handle_prediction_fetch(&fetch_request(ROW_IDENTITY))
                .await;
            assert!(fetched.ok);
            if let Some(row) = fetched.prediction {
                assert_eq!(row, test_prediction_row());
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the row never reached the remote"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let other = reader
            .handle_prediction_fetch(&fetch_request("shared-workspace-v1:0124"))
            .await;
        assert!(other.ok);
        assert_eq!(other.prediction, None, "no row for another identity");
    }

    #[tokio::test]
    async fn a_read_only_or_absent_remote_takes_no_prediction_row() {
        let dir = tempfile::tempdir().unwrap();
        let backend = test_remote_backend();
        let mut read_only = test_config(dir.path());
        read_only.remote = Some(test_remote_config());
        read_only.remote_readonly = true;
        let daemon = daemon_on(read_only.clone(), &backend);
        assert!(
            daemon
                .handle_prediction_publish(PredictionPublishRequest {
                    identity: ROW_IDENTITY.to_string(),
                    row: test_prediction_row(),
                })
                .ok
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            backend.list("").await.unwrap().is_empty(),
            "a read-only remote gets nothing"
        );

        let mut absent = test_config(dir.path());
        absent.remote = None;
        let daemon = Arc::new(Daemon::new(absent));
        let reply = daemon
            .handle_prediction_fetch(&fetch_request(ROW_IDENTITY))
            .await;
        assert!(reply.ok);
        assert_eq!(reply.prediction, None);
    }

    #[tokio::test]
    async fn a_malformed_prediction_row_or_identity_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let backend = test_remote_backend();
        let daemon = daemon_on(config, &backend);
        let refused = daemon.handle_prediction_fetch(&fetch_request("0123")).await;
        assert!(
            !refused.ok,
            "only identities a shared row answers are asked for"
        );
        assert!(
            !daemon
                .handle_prediction_publish(PredictionPublishRequest {
                    identity: "0123".to_string(),
                    row: test_prediction_row(),
                })
                .ok
        );

        let key = crate::config::join_remote_key(
            &test_remote_config().prefix,
            &format!(
                "v3/predictions/{}",
                crate::prediction_share::object_name(ROW_IDENTITY)
            ),
        );
        backend
            .put(&key, b"{ not json".to_vec(), Some("application/json"))
            .await
            .unwrap();
        let reply = daemon
            .handle_prediction_fetch(&fetch_request(ROW_IDENTITY))
            .await;
        assert!(reply.ok);
        assert_eq!(reply.prediction, None);
    }

    #[test]
    fn rows_are_published_only_to_a_writable_remote() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = None;
        assert!(!publishes_predictions(&config));
        config.remote = Some(test_remote_config());
        assert!(publishes_predictions(&config));
        config.remote_readonly = true;
        assert!(!publishes_predictions(&config));
    }

    #[test]
    fn a_wrapper_waits_longer_for_a_row_than_the_daemon_spends_fetching_it() {
        assert_eq!(prediction_fetch_wait(), Duration::from_millis(2_500));
        assert_eq!(PREDICTION_FETCH_BUDGET, Duration::from_secs(2));
    }

    #[test]
    fn a_prediction_identity_is_portable_and_bounded() {
        let at_cap = format!(
            "shared-workspace-v1:{}",
            "a".repeat(PREDICTION_IDENTITY_MAX_LEN - "shared-workspace-v1:".len())
        );
        assert!(prediction_identity_is_acceptable(&at_cap));
        assert!(!prediction_identity_is_acceptable(&format!("{at_cap}a")));
        assert!(prediction_identity_is_acceptable("shared-target-v2:0123"));
        assert!(prediction_identity_is_acceptable("shared-out-dir-v1:0123"));
        assert!(!prediction_identity_is_acceptable("0123"));
    }

    #[tokio::test]
    async fn cache_remote_wraps_the_injected_backend() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let backend = test_remote_backend();
        let daemon = Daemon::new(config);
        daemon.set_remote_backend_for_test(Arc::clone(&backend));
        let remote_cache = daemon.cache_remote().await.unwrap();
        backend
            .put(
                "prefix/v3/manifests/foo/k1.json",
                b"{}".to_vec(),
                Some("application/json"),
            )
            .await
            .unwrap();
        assert!(remote_cache.exists_entry("k1", "foo").await.unwrap());
        assert!(Arc::ptr_eq(
            daemon.v3_remote().await.unwrap(),
            daemon.v3_remote().await.unwrap()
        ));
        assert!(Arc::ptr_eq(
            daemon.v3_remote().await.unwrap().backend(),
            &backend
        ));
    }

    struct BlockingIdentityBackend {
        inner: Arc<dyn crate::remote_backend::RemoteBackend>,
        identity_gets: AtomicU64,
        identity_cancellations: AtomicU64,
        artifact_gets: AtomicU64,
        artifact_started_before_identity_cancel: AtomicBool,
        identity_started: Notify,
        block_identity: AtomicBool,
    }

    struct PendingIdentityGuard<'a> {
        cancellations: &'a AtomicU64,
    }

    impl Drop for PendingIdentityGuard<'_> {
        fn drop(&mut self) {
            self.cancellations.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[async_trait::async_trait]
    impl crate::remote_backend::RemoteBackend for BlockingIdentityBackend {
        async fn head(&self, key: &str) -> Result<bool> {
            self.inner.head(key).await
        }

        async fn get(
            &self,
            key: &str,
            max_bytes: Option<u64>,
        ) -> Result<Option<crate::remote_backend::GetObject>> {
            if key.contains("/_manifests/") {
                self.identity_gets.fetch_add(1, Ordering::Relaxed);
                self.identity_started.notify_one();
                if self.block_identity.load(Ordering::Acquire) {
                    let _pending = PendingIdentityGuard {
                        cancellations: &self.identity_cancellations,
                    };
                    std::future::pending::<()>().await;
                }
            } else {
                if self.identity_cancellations.load(Ordering::Acquire) == 0 {
                    self.artifact_started_before_identity_cancel
                        .store(true, Ordering::Release);
                }
                self.artifact_gets.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.get(key, max_bytes).await
        }

        async fn put(&self, key: &str, body: Vec<u8>, content_type: Option<&str>) -> Result<()> {
            self.inner.put(key, body, content_type).await
        }

        async fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.inner.list(prefix).await
        }

        fn describe(&self, key: &str) -> String {
            self.inner.describe(key)
        }
    }

    struct FailingIdentityBackend {
        inner: Arc<dyn crate::remote_backend::RemoteBackend>,
        kind: std::io::ErrorKind,
        identity_keys: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl crate::remote_backend::RemoteBackend for FailingIdentityBackend {
        async fn head(&self, key: &str) -> Result<bool> {
            self.inner.head(key).await
        }

        async fn get(
            &self,
            key: &str,
            max_bytes: Option<u64>,
        ) -> Result<Option<crate::remote_backend::GetObject>> {
            if key.contains("/_manifests/") {
                self.identity_keys.lock().unwrap().push(key.to_string());
                return Err(std::io::Error::new(self.kind, "classified manifest failure").into());
            }
            self.inner.get(key, max_bytes).await
        }

        async fn put(&self, key: &str, body: Vec<u8>, content_type: Option<&str>) -> Result<()> {
            self.inner.put(key, body, content_type).await
        }

        async fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.inner.list(prefix).await
        }

        fn describe(&self, key: &str) -> String {
            self.inner.describe(key)
        }
    }

    async fn wait_for_test_condition(mut condition: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !condition() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("test condition must become true");
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
            crate::identity::host_target_triple()
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

    struct BlockingPackBackend {
        inner: Arc<dyn crate::remote_backend::RemoteBackend>,
        pack_started: Arc<Notify>,
        release_pack: Arc<tokio::sync::Semaphore>,
        v3_get_started: Option<Arc<Notify>>,
    }

    #[async_trait::async_trait]
    impl crate::remote_backend::RemoteBackend for BlockingPackBackend {
        async fn head(&self, key: &str) -> Result<bool> {
            self.inner.head(key).await
        }

        async fn get(
            &self,
            key: &str,
            max_bytes: Option<u64>,
        ) -> Result<Option<crate::remote_backend::GetObject>> {
            if key.contains("/v4/prefetch/packs/") {
                self.pack_started.notify_waiters();
                let _release = self
                    .release_pack
                    .acquire()
                    .await
                    .expect("test release semaphore stays open");
            } else if key.contains("/v3/packs/")
                && let Some(started) = &self.v3_get_started
            {
                started.notify_waiters();
            }
            self.inner.get(key, max_bytes).await
        }

        async fn put(&self, key: &str, body: Vec<u8>, content_type: Option<&str>) -> Result<()> {
            self.inner.put(key, body, content_type).await
        }

        async fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.inner.list(prefix).await
        }

        fn describe(&self, key: &str) -> String {
            self.inner.describe(key)
        }
    }

    struct BlockingV3Backend {
        inner: Arc<dyn crate::remote_backend::RemoteBackend>,
        v3_get_started: tokio::sync::mpsc::UnboundedSender<u64>,
        release_v3_get: Arc<tokio::sync::Semaphore>,
        v3_gets: AtomicU64,
    }

    struct ReorderedPackBackend {
        inner: Arc<dyn crate::remote_backend::RemoteBackend>,
        first_key: String,
        later_downloaded: Notify,
    }

    #[async_trait::async_trait]
    impl crate::remote_backend::RemoteBackend for ReorderedPackBackend {
        async fn head(&self, key: &str) -> Result<bool> {
            self.inner.head(key).await
        }

        async fn get(
            &self,
            key: &str,
            max_bytes: Option<u64>,
        ) -> Result<Option<crate::remote_backend::GetObject>> {
            if key == self.first_key {
                self.later_downloaded.notified().await;
            }
            let object = self.inner.get(key, max_bytes).await?;
            if key != self.first_key && key.contains("/v4/prefetch/packs/") {
                self.later_downloaded.notify_one();
            }
            Ok(object)
        }

        async fn put(&self, key: &str, body: Vec<u8>, content_type: Option<&str>) -> Result<()> {
            self.inner.put(key, body, content_type).await
        }

        async fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.inner.list(prefix).await
        }

        fn describe(&self, key: &str) -> String {
            self.inner.describe(key)
        }
    }

    #[async_trait::async_trait]
    impl crate::remote_backend::RemoteBackend for BlockingV3Backend {
        async fn head(&self, key: &str) -> Result<bool> {
            self.inner.head(key).await
        }

        async fn get(
            &self,
            key: &str,
            max_bytes: Option<u64>,
        ) -> Result<Option<crate::remote_backend::GetObject>> {
            if key.contains("/v3/packs/") {
                let ordinal = self.v3_gets.fetch_add(1, Ordering::SeqCst) + 1;
                let _ = self.v3_get_started.send(ordinal);
                let permit = self
                    .release_v3_get
                    .acquire()
                    .await
                    .map_err(|_| anyhow::anyhow!("v3 GET test gate closed"))?;
                permit.forget();
            }
            self.inner.get(key, max_bytes).await
        }

        async fn put(&self, key: &str, body: Vec<u8>, content_type: Option<&str>) -> Result<()> {
            self.inner.put(key, body, content_type).await
        }

        async fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.inner.list(prefix).await
        }

        fn describe(&self, key: &str) -> String {
            self.inner.describe(key)
        }
    }

    async fn wait_for_download_waiter(daemon: &Daemon, key: &str) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let attached = {
                    let downloading = daemon.downloading.read().await;
                    downloading
                        .get(key)
                        .is_some_and(|notify| Arc::strong_count(notify) > 1)
                };
                if attached {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("same-key request must attach to the active download claim");
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
                shard_dir: None,
            }),
        )
        .await;

        assert!(resp.ok, "remote check should return a response: {resp:?}");
        assert_eq!(resp.found, Some(false), "missing remote key -> found=false");
    }

    #[tokio::test]
    async fn planner_shard_download_returns_the_remote_payload() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let shard = crate::remote::Shard {
            version: 3,
            entries: vec![crate::remote::ShardEntry {
                cache_key: test_cache_key("planner-shard-entry"),
                crate_name: "serde".into(),
                compile_time_ms: Some(1234),
                artifact_size: Some(5678),
            }],
        };
        let client = test_remote_backend();
        put_test_object(
            &client,
            &crate::remote::shard_object_key("prefix", "workspace", "abc"),
            &serde_json::to_vec(&shard).unwrap(),
        )
        .await;
        let daemon = Daemon::new(config);
        assert!(daemon.remote_backend.set(client).is_ok());

        let downloaded = daemon
            .download_planner_shard("workspace", "abc")
            .await
            .expect("planner shard download")
            .expect("seeded shard must be returned");
        assert_eq!(downloaded.version, 3);
        assert_eq!(downloaded.entries, shard.entries);
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
                origin: None,
                candidate_sources: HashMap::new(),
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
    async fn test_do_upload_uploads_when_not_in_remote_records_v3_transfer_timestamps() {
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
        assert_v3_transfer_timestamps(&latest_transfer(&daemon));
    }

    #[tokio::test]
    async fn test_do_upload_failure_records_v3_transfer_timestamps() {
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
        assert_v3_transfer_timestamps(&latest_transfer(&daemon));
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
                identity_key: Some("id/cold-build".into()),
            },
            client_epoch: 0,
            session_id: "cold-session".into(),
        };
        let resp = daemon.handle_build_started(&req).await;
        assert!(
            resp.ok,
            "fallback with nothing to prefetch should be ok: {resp:?}"
        );
        let plan = daemon.active_plan.lock().unwrap();
        let plan = plan.as_ref().expect("cold build session must be tracked");
        assert_eq!(plan.session_id, "cold-session");
        assert_eq!(plan.identity_key.as_deref(), Some("id/cold-build"));
        assert!(plan.candidates.is_empty());
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
                        shard_dir: None,
                    },
                    RemoteCheckRequest {
                        key: key_b,
                        entry_dir: entry_b,
                        crate_name: "tokio".into(),
                        deadline_ms: None,
                        shard_dir: None,
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
    async fn test_remote_check_failure_records_v3_transfer_timestamps() {
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
                shard_dir: None,
            })
            .await;

        // The entry was present remotely but its download failed -> error.
        assert!(
            !resp.ok,
            "download failure should surface as an error: {resp:?}"
        );
        assert!(resp.error.is_some());
        assert_v3_transfer_timestamps(&latest_transfer(&daemon));
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
                shard_dir: None,
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
    fn build_entry_pack_with_meta(key: &str, crate_name: &str) -> (Vec<u8>, String) {
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
        let meta_bytes = std::fs::read(entry_dir.join("meta.json")).unwrap();
        let meta: crate::store::EntryMeta = serde_json::from_slice(&meta_bytes).unwrap();
        let packed =
            crate::remote_layout::create_entry_pack_zstd(&entry_dir, &store.blobs_dir(), &meta, 3)
                .unwrap();
        (packed, blake3::hash(&meta_bytes).to_hex().to_string())
    }

    fn build_entry_pack(key: &str, crate_name: &str) -> Vec<u8> {
        build_entry_pack_with_meta(key, crate_name).0
    }

    #[test]
    fn packed_prefetch_context_is_derived_from_a_complete_build_intent() {
        let intent = kache_core::BuildIntent {
            crate_names: vec!["serde".into()],
            namespace: Some("linux/toolchain/release".into()),
            cargo_lock_deps: vec![("serde".into(), "1.0.0".into())],
            identity_key: None,
        };
        let context = PackPrefetchContext::from_intent(&intent)
            .expect("a namespaced lockfile intent must enable catalog discovery");
        assert_eq!(context.namespace, "linux/toolchain/release");
        assert_eq!(context.shard_hashes.len(), 1);
        assert!(crate::cache_key::is_valid_cache_key(&context.selector));
    }

    async fn seed_packed_catalog(
        backend: &Arc<dyn crate::remote_backend::RemoteBackend>,
        context: &PackPrefetchContext,
        entries: Vec<crate::remote_pack::PackInputEntry>,
        object_override: Option<Vec<u8>>,
    ) -> crate::remote_pack::BuiltPack {
        let built = crate::remote_pack::build_pack(
            "prefix",
            entries,
            crate::remote_pack::DEFAULT_MAX_PACK_BYTES,
        )
        .unwrap();
        put_test_object(
            backend,
            &built.object_key,
            object_override.as_deref().unwrap_or(&built.bytes),
        )
        .await;
        let created_at_ms = epoch_ms();
        let catalog = crate::remote_pack::PackCatalog {
            version: crate::remote_pack::CATALOG_VERSION,
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            manifest_key: context.manifest_key.clone(),
            namespace: context.namespace.clone(),
            selector_hash: context.selector.clone(),
            shard_hashes: context.shard_hashes.clone(),
            created_at_ms,
            expires_at_ms: created_at_ms + 60_000,
            packs: vec![crate::remote_pack::CatalogPackRef {
                digest: built.digest.clone(),
                pack_bytes: built.bytes.len() as u64,
                entries: built
                    .index
                    .entries
                    .iter()
                    .map(|entry| crate::remote_pack::CatalogEntry {
                        cache_key: entry.cache_key.clone(),
                        crate_name: entry.crate_name.clone(),
                        meta_digest: entry.meta_digest.clone(),
                    })
                    .collect(),
            }],
            fallback_entries: Vec::new(),
        };
        let encoded = crate::remote_pack::encode_catalog("prefix", catalog).unwrap();
        put_test_object(backend, &encoded.object_key, &encoded.bytes).await;
        built
    }

    async fn wait_for_store_entry(daemon: &Arc<Daemon>, key: &str) {
        for _ in 0..200 {
            if daemon
                .with_store(|store| Ok(store.get(key)?.is_some()))
                .unwrap_or(false)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for imported entry {}", key_prefix(key));
    }

    fn traversal_entry_payload() -> Vec<u8> {
        let body = b"escape";
        let mut header = [0u8; 512];
        header[..13].copy_from_slice(b"../escape.txt");
        header[100..107].copy_from_slice(b"0000644");
        let size = format!("{:011o}", body.len());
        header[124..135].copy_from_slice(size.as_bytes());
        header[156] = b'0';
        header[148..156].fill(b' ');
        let checksum: u32 = header.iter().map(|byte| u32::from(*byte)).sum();
        header[148..156].copy_from_slice(format!("{checksum:06o}\0 ").as_bytes());
        let mut tar = Vec::new();
        tar.extend_from_slice(&header);
        tar.extend_from_slice(body);
        tar.extend(std::iter::repeat_n(0, 512 - body.len()));
        tar.extend(std::iter::repeat_n(0, 1024));
        zstd::stream::encode_all(std::io::Cursor::new(tar), 3).unwrap()
    }

    #[test]
    fn packed_receipt_queue_bounds_allocated_bytes_and_entries_independently() {
        let mut queue = PrefetchReceiptQueue::default();
        queue.push(TransferEvent {
            object_key: String::with_capacity((2 << 20) - std::mem::size_of::<TransferEvent>()),
            ..Default::default()
        });
        assert_eq!(queue.events.len(), 1, "exact byte boundary is admitted");
        queue.push(TransferEvent::default());
        assert_eq!(queue.events.len(), 1, "additional data exceeds byte bound");
        assert!(queue.overflowed);
        drop(queue.drain());
        queue.push(TransferEvent::default());
        assert_eq!(queue.events.len(), 1, "draining releases retained capacity");
        assert!(
            queue.overflowed,
            "lost coverage remains visible after draining"
        );

        let mut queue = PrefetchReceiptQueue::default();
        queue.push(TransferEvent {
            accounting: Some(PrefetchAccounting {
                entries: vec![PackedEntryTransfer::default(); 4096],
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(queue.events.len(), 1, "exact entry boundary is admitted");
        queue.push(TransferEvent {
            accounting: Some(PrefetchAccounting {
                entries: vec![PackedEntryTransfer::default()],
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(queue.events.len(), 1, "entry bound is independent of bytes");
        assert!(queue.overflowed);
    }

    #[test]
    fn packed_receipt_queue_drain_returns_queued_events_and_clears_the_queue() {
        let mut queue = PrefetchReceiptQueue::default();
        queue.push(TransferEvent {
            object_key: "keep-me".into(),
            ..TransferEvent::default()
        });
        assert!(queue.retained_bytes > 0);
        let drained = queue.drain();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].object_key, "keep-me");
        assert!(queue.events.is_empty());
        assert_eq!(queue.retained_bytes, 0);
        assert_eq!(queue.entries, 0);
        queue.push(TransferEvent {
            object_key: "after-drain".into(),
            ..TransferEvent::default()
        });
        assert_eq!(queue.events.len(), 1);
        assert_eq!(queue.events[0].object_key, "after-drain");
    }

    fn packed_receipt_origin(session: usize, plan: usize, source: usize) -> PrefetchOrigin {
        PrefetchOrigin {
            session_id: String::with_capacity(session),
            plan_id: String::with_capacity(plan),
            source: String::with_capacity(source),
            ..PrefetchOrigin::default()
        }
    }

    fn packed_receipt_retained_bytes(event: &TransferEvent) -> usize {
        fn origin_bytes(origin: &PrefetchOrigin) -> usize {
            origin.session_id.capacity() + origin.plan_id.capacity() + origin.source.capacity()
        }
        let mut bytes = std::mem::size_of::<TransferEvent>()
            + event.crate_name.capacity()
            + event.format.capacity()
            + event.cache_key.capacity()
            + event.object_key.capacity()
            + event.outcome.capacity()
            + event.prefetch.as_ref().map_or(0, origin_bytes);
        if let Some(accounting) = &event.accounting {
            bytes += accounting.entries.capacity() * std::mem::size_of::<PackedEntryTransfer>();
            for entry in &accounting.entries {
                bytes += entry.cache_key.capacity()
                    + entry.crate_name.capacity()
                    + entry.outcome.capacity()
                    + origin_bytes(&entry.prefetch);
            }
        }
        bytes
    }

    #[test]
    fn packed_receipt_queue_counts_origin_and_nested_entry_allocations() {
        let mut entries = Vec::with_capacity(8);
        entries.push(PackedEntryTransfer {
            cache_key: String::with_capacity(8),
            crate_name: String::with_capacity(16),
            outcome: String::with_capacity(32),
            prefetch: packed_receipt_origin(8, 16, 32),
            ..PackedEntryTransfer::default()
        });
        let event = TransferEvent {
            crate_name: String::with_capacity(8),
            format: String::with_capacity(16),
            cache_key: String::with_capacity(32),
            object_key: String::with_capacity(64),
            outcome: String::with_capacity(4),
            prefetch: Some(packed_receipt_origin(8, 16, 32)),
            accounting: Some(PrefetchAccounting {
                entries,
                ..PrefetchAccounting::default()
            }),
            ..TransferEvent::default()
        };
        let expected = packed_receipt_retained_bytes(&event);
        assert!(
            expected > std::mem::size_of::<TransferEvent>(),
            "origin and entry string allocations must contribute: {expected}"
        );
        let mut queue = PrefetchReceiptQueue::default();
        queue.push(event);
        assert_eq!(queue.events.len(), 1);
        assert!(!queue.overflowed);
        assert_eq!(queue.retained_bytes, expected);
        assert_eq!(queue.entries, 1);
    }

    #[test]
    fn packed_receipt_drop_stamps_unfinished_and_keeps_finished_times() {
        let queue = Arc::new(Mutex::new(PrefetchReceiptQueue::default()));
        drop(PrefetchReceipt::new(
            queue.clone(),
            PrefetchOrigin::default(),
            "unfinished",
            "pack",
            PrefetchOperation::Get,
        ));
        let unfinished = queue.lock().unwrap().drain();
        assert_eq!(unfinished.len(), 1);
        assert_v3_transfer_timestamps(&unfinished[0]);
        assert_eq!(unfinished[0].outcome, "cancelled");

        let mut receipt = PrefetchReceipt::new(
            queue.clone(),
            PrefetchOrigin::default(),
            "finished",
            "pack",
            PrefetchOperation::Get,
        );
        receipt.finish("completed");
        receipt.event.finished_at_unix_ms = 1_700_000_000_123;
        drop(receipt);
        let finished = queue.lock().unwrap().drain();
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].finished_at_unix_ms, 1_700_000_000_123);
        assert_eq!(finished[0].timestamp, 1_700_000_000);
        assert_ne!(finished[0].timestamp, 123);
        assert_eq!(finished[0].outcome, "completed");
    }

    #[test]
    fn finish_open_prefetch_receipt_stamps_unfinished_and_keeps_finished() {
        let queue = Arc::new(Mutex::new(PrefetchReceiptQueue::default()));
        let mut open_ok = PrefetchReceipt::new(
            queue.clone(),
            PrefetchOrigin::default(),
            "open-ok",
            "v3",
            PrefetchOperation::List,
        );
        finish_open_prefetch_receipt(&mut open_ok, true);
        assert_eq!(open_ok.event.outcome, "completed");
        assert!(open_ok.event.ok);
        assert_ne!(open_ok.event.finished_at_unix_ms, 0);

        let mut open_err = PrefetchReceipt::new(
            queue.clone(),
            PrefetchOrigin::default(),
            "open-err",
            "v3",
            PrefetchOperation::List,
        );
        finish_open_prefetch_receipt(&mut open_err, false);
        assert_eq!(open_err.event.outcome, "error");
        assert!(!open_err.event.ok);
        assert_ne!(open_err.event.finished_at_unix_ms, 0);

        let mut already = PrefetchReceipt::new(
            queue,
            PrefetchOrigin::default(),
            "already",
            "v3",
            PrefetchOperation::List,
        );
        already.finish("completed");
        already.event.finished_at_unix_ms = 1_700_000_000_123;
        already.event.elapsed_ms = 41;
        finish_open_prefetch_receipt(&mut already, false);
        assert_eq!(already.event.outcome, "completed");
        assert!(already.event.ok);
        assert_eq!(already.event.finished_at_unix_ms, 1_700_000_000_123);
        assert_eq!(already.event.elapsed_ms, 41);
    }

    #[test]
    fn prefetch_backend_observer_received_sums_request_and_body_ms() {
        let queue = Arc::new(Mutex::new(PrefetchReceiptQueue::default()));
        let mut receipt = PrefetchReceipt::new(
            queue,
            PrefetchOrigin::default(),
            "obj",
            "v3",
            PrefetchOperation::Get,
        );
        let stats = PrefetchStats::new();
        let transfers = TransferCounters::new();
        {
            let mut observer = PrefetchBackendObserver {
                receipt: &mut receipt,
                stats: &stats,
                transfers: &transfers,
                network_started: None,
            };
            crate::remote_layout::DownloadObserver::received(
                &mut observer,
                Some(&crate::remote_backend::GetTransfer {
                    bytes: 16,
                    request_ms: 7,
                    body_ms: 5,
                }),
            );
        }
        assert_eq!(receipt.event.compressed_bytes, 16);
        assert_eq!(receipt.event.request_ms, 7);
        assert_eq!(receipt.event.body_ms, 5);
        assert_eq!(receipt.event.network_ms, 12);
        assert!(receipt.accounting().bytes_complete);
        assert_eq!(stats.v3_bytes_downloaded.load(Ordering::Relaxed), 16);
        assert_eq!(stats.bytes_downloaded.load(Ordering::Relaxed), 16);
        assert_eq!(transfers.bytes_downloaded.load(Ordering::Relaxed), 16);
    }

    #[test]
    fn charge_packed_import_accumulates_each_entry() {
        let mut event = TransferEvent::default();
        charge_packed_import(&mut event, 11, 7);
        charge_packed_import(&mut event, 13, 5);
        assert_eq!(event.original_bytes, 24);
        assert_eq!(event.extract_ms, 12);
    }

    #[tokio::test]
    async fn packed_receipts_require_every_writer_to_finish() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Arc::new(Daemon::new(test_config(dir.path())));
        {
            let mut writers = daemon.prefetch_receipt_writers.lock().unwrap();
            writers.push(tokio::task::spawn_blocking(|| {}));
            writers.push(tokio::task::spawn_blocking(|| {
                panic!("injected writer join failure");
            }));
        }
        assert!(!daemon.finish_prefetch_receipts().await);
    }

    #[tokio::test]
    async fn packed_receipt_flush_retains_recent_history_below_the_cap() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Arc::new(Daemon::new(test_config(dir.path())));
        for index in 0..2 {
            drop(PrefetchReceipt::new(
                daemon.prefetch_receipts.clone(),
                PrefetchOrigin::default(),
                &format!("recent-{index}"),
                "pack",
                PrefetchOperation::Get,
            ));
        }
        assert!(daemon.finish_prefetch_receipts().await);
        assert_eq!(daemon.recent_transfers.lock().unwrap().len(), 2);

        for index in 0..51 {
            drop(PrefetchReceipt::new(
                daemon.prefetch_receipts.clone(),
                PrefetchOrigin::default(),
                &format!("capped-{index}"),
                "pack",
                PrefetchOperation::Get,
            ));
        }
        assert!(daemon.finish_prefetch_receipts().await);
        assert_eq!(daemon.recent_transfers.lock().unwrap().len(), 50);
    }

    #[tokio::test]
    async fn packed_shutdown_timeout_marks_incomplete_when_receipts_are_complete() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Arc::new(Daemon::new(test_config(dir.path())));
        daemon.install_plan("session", "plan", "advisory", std::iter::empty(), None);
        daemon
            .spawn_prefetch_task(
                PrefetchOrigin {
                    session_id: "other-session".into(),
                    plan_id: "other-plan".into(),
                    source: "fallback".into(),
                    ..PrefetchOrigin::default()
                },
                std::future::pending(),
            )
            .unwrap();
        assert!(daemon.finish_prefetch_shutdown(Duration::ZERO).await);
        let summaries = events::read_summaries(&daemon.config.summary_log_path()).unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].closure_reason, "shutdown_timeout");
        assert!(summaries[0].incomplete);
        assert!(summaries[0].cancelled);
    }

    #[tokio::test]
    async fn packed_shutdown_incomplete_receipts_mark_incomplete_without_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Arc::new(Daemon::new(test_config(dir.path())));
        daemon.install_plan("session", "plan", "advisory", std::iter::empty(), None);
        daemon
            .prefetch_receipts
            .lock()
            .unwrap()
            .push(TransferEvent {
                object_key: String::with_capacity((2 << 20) + 1),
                ..TransferEvent::default()
            });
        assert!(
            !daemon
                .finish_prefetch_shutdown(Duration::from_secs(1))
                .await
        );
        let summaries = events::read_summaries(&daemon.config.summary_log_path()).unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].closure_reason, "shutdown");
        assert!(summaries[0].incomplete);
        assert!(summaries[0].cancelled);
    }

    #[tokio::test]
    async fn packed_receipts_use_one_writer_and_drain_while_disk_is_blocked() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Arc::new(Daemon::new(test_config(dir.path())));
        std::fs::create_dir_all(&daemon.config.runtime_dir).unwrap();
        let lock_path = daemon.config.runtime_dir.join("transfers.jsonl.lock");
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)
            .unwrap();
        lock.lock().unwrap();
        for index in 0..100 {
            drop(PrefetchReceipt::new(
                daemon.prefetch_receipts.clone(),
                PrefetchOrigin::default(),
                &format!("pack-{index}"),
                "pack",
                PrefetchOperation::Get,
            ));
            daemon.flush_prefetch_receipts();
            if index == 0 {
                daemon.install_plan(
                    "writer-session",
                    "writer-plan",
                    "fallback",
                    std::iter::empty(),
                    None,
                );
                daemon.finalize_inactive_plan(0);
                let summaries = events::read_summaries(&daemon.config.summary_log_path()).unwrap();
                assert_eq!(summaries[0].closure_reason, "inactivity");
                assert!(
                    summaries[0].incomplete,
                    "a blocked writer can still lose records"
                );
            }
        }
        assert_eq!(daemon.prefetch_receipt_writers.lock().unwrap().len(), 1);
        assert_eq!(daemon.prefetch_receipts.lock().unwrap().events.len(), 99);
        lock.unlock().unwrap();
        assert!(daemon.finish_prefetch_receipts().await);
        assert_eq!(
            events::read_transfers(&daemon.config.transfer_log_path())
                .unwrap()
                .len(),
            100
        );
        assert!(daemon.prefetch_receipts.lock().unwrap().events.is_empty());
        assert!(!daemon.prefetch_receipts.lock().unwrap().writing);
    }

    #[tokio::test]
    async fn packed_normal_summary_exposes_overflow_log_failure_and_pending_tasks() {
        for loss in [
            "overflow",
            "log_failure",
            "receipt",
            "task",
            "cancelled_task",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let daemon = Arc::new(Daemon::new(test_config(dir.path())));
            daemon.install_plan(
                "normal-session",
                "normal-plan",
                "fallback",
                std::iter::empty(),
                None,
            );
            match loss {
                "overflow" => daemon
                    .prefetch_receipts
                    .lock()
                    .unwrap()
                    .push(TransferEvent {
                        object_key: String::with_capacity((2 << 20) + 1),
                        ..Default::default()
                    }),
                "log_failure" => {
                    std::fs::create_dir_all(daemon.config.transfer_log_path()).unwrap();
                    drop(PrefetchReceipt::new(
                        daemon.prefetch_receipts.clone(),
                        PrefetchOrigin::default(),
                        "pack-key",
                        "pack",
                        PrefetchOperation::Get,
                    ));
                    assert!(!daemon.finish_prefetch_receipts().await);
                }
                "receipt" => {
                    drop(PrefetchReceipt::new(
                        daemon.prefetch_receipts.clone(),
                        PrefetchOrigin::default(),
                        "queued-record",
                        "pack",
                        PrefetchOperation::Get,
                    ));
                }
                "task" => {
                    daemon
                        .spawn_prefetch_task(PrefetchOrigin::default(), std::future::pending())
                        .unwrap();
                }
                "cancelled_task" => {
                    let receiver = daemon
                        .spawn_prefetch_task(PrefetchOrigin::default(), async {
                            panic!("cancelled before normal closure")
                        })
                        .unwrap();
                    assert!(receiver.await.is_err());
                }
                _ => unreachable!(),
            }
            daemon.finalize_inactive_plan(0);
            let summaries = events::read_summaries(&daemon.config.summary_log_path()).unwrap();
            assert_eq!(summaries.len(), 1);
            assert_eq!(summaries[0].closure_reason, "inactivity");
            assert!(
                summaries[0].incomplete,
                "lost or unfinished coverage: {loss}"
            );
            daemon.finish_prefetch_shutdown(Duration::ZERO).await;
        }
    }

    #[tokio::test]
    async fn packed_receipt_overflow_and_writer_panic_remain_incomplete() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Arc::new(Daemon::new(test_config(dir.path())));
        daemon
            .prefetch_receipts
            .lock()
            .unwrap()
            .push(TransferEvent {
                object_key: String::with_capacity((2 << 20) + 1),
                ..Default::default()
            });
        assert!(!daemon.finish_prefetch_receipts().await);
        assert!(daemon.prefetch_receipts.lock().unwrap().events.is_empty());
        daemon.prefetch_receipts.lock().unwrap().writing = true;
        let guard = PrefetchReceiptWriter {
            queue: daemon.prefetch_receipts.clone(),
            failed: daemon.prefetch_receipt_failed.clone(),
            armed: true,
        };
        assert!(
            tokio::task::spawn_blocking(move || {
                let _guard = guard;
                panic!("injected receipt writer failure");
            })
            .await
            .is_err()
        );
        assert!(!daemon.prefetch_receipts.lock().unwrap().writing);
        assert!(daemon.prefetch_receipt_failed.load(Ordering::Acquire));
        drop(PrefetchReceipt::new(
            daemon.prefetch_receipts.clone(),
            PrefetchOrigin::default(),
            "after-panic",
            "pack",
            PrefetchOperation::Get,
        ));
        assert!(!daemon.finish_prefetch_receipts().await);
        assert_eq!(
            events::read_transfers(&daemon.config.transfer_log_path())
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn packed_receipt_distinguishes_queued_and_inflight_cancellation() {
        for queued in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let mut config = test_config(dir.path());
            config.remote = Some(test_remote_config());
            let daemon = Arc::new(Daemon::new(config));
            let started = Arc::new(Notify::new());
            assert!(
                daemon
                    .remote_backend
                    .set(Arc::new(BlockingPackBackend {
                        inner: test_remote_backend(),
                        pack_started: started.clone(),
                        release_pack: Arc::new(tokio::sync::Semaphore::new(0)),
                        v3_get_started: None,
                    }))
                    .is_ok()
            );
            let v3 = daemon.v3_remote().await.unwrap().clone();
            let permit = if queued {
                Some(
                    daemon
                        .prefetch_gate
                        .clone()
                        .acquire_many_owned(daemon.prefetch_gate.available_permits() as u32)
                        .await
                        .unwrap(),
                )
            } else {
                None
            };
            let backend_started = started.notified();
            tokio::pin!(backend_started);
            backend_started.as_mut().enable();
            let (ready, entered) = tokio::sync::oneshot::channel();
            let worker = daemon.clone();
            let task = tokio::spawn(async move {
                let key = "prefix/v4/prefetch/packs/test";
                let mut receipt = PrefetchReceipt::new(
                    worker.prefetch_receipts.clone(),
                    PrefetchOrigin::default(),
                    key,
                    "pack",
                    PrefetchOperation::Get,
                );
                ready.send(()).unwrap();
                let _ = worker
                    .packed_prefetch_get(&v3, key, 1024, "test GET", &mut receipt)
                    .await;
            });
            entered.await.unwrap();
            if !queued {
                tokio::time::timeout(Duration::from_secs(2), backend_started)
                    .await
                    .unwrap();
            }
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            drop(permit);
            assert!(daemon.finish_prefetch_receipts().await);
            let transfers = events::read_transfers(&daemon.config.transfer_log_path()).unwrap();
            assert_eq!(transfers.len(), 1);
            let event = &transfers[0];
            assert_eq!(event.request_count, u32::from(!queued));
            assert_eq!(event.compressed_bytes, 0);
            assert_eq!(event.outcome, "cancelled");
            assert_v3_transfer_timestamps(event);
            assert_eq!(event.accounting.as_ref().unwrap().bytes_complete, queued);
            assert!(event.accounting.as_ref().unwrap().requests_complete);
        }
    }

    #[tokio::test]
    async fn packed_receipt_log_failure_marks_accounting_incomplete() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Arc::new(Daemon::new(test_config(dir.path())));
        std::fs::create_dir_all(daemon.config.transfer_log_path()).unwrap();
        drop(PrefetchReceipt::new(
            daemon.prefetch_receipts.clone(),
            PrefetchOrigin::default(),
            "pack-key",
            "pack",
            PrefetchOperation::Get,
        ));
        assert!(!daemon.finish_prefetch_receipts().await);
        assert_eq!(daemon.recent_transfers.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn packed_receipt_survives_cancellation_after_body_before_import() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Arc::new(Daemon::new(test_config(dir.path())));
        let mut receipt = PrefetchReceipt::new(
            daemon.prefetch_receipts.clone(),
            PrefetchOrigin::default(),
            "pack-key",
            "pack",
            PrefetchOperation::Get,
        );
        receipt.event.request_count = 1;
        receipt.received(&crate::remote_backend::GetObject {
            body: bytes::Bytes::from_static(b"received before cancellation"),
            request_ms: 7,
            body_ms: 11,
        });
        let (ready, received) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _receipt = receipt;
            ready.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        received.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(daemon.finish_prefetch_receipts().await);
        let transfers = events::read_transfers(&daemon.config.transfer_log_path()).unwrap();
        assert_eq!(transfers.len(), 1);
        let event = &transfers[0];
        assert_eq!(event.compressed_bytes, 28);
        assert_eq!(event.request_count, 1);
        assert_eq!(event.request_ms, 7);
        assert_eq!(event.body_ms, 11);
        assert_eq!(event.outcome, "cancelled");
        assert!(!event.ok);
        assert!(event.accounting.as_ref().unwrap().bytes_complete);
        assert!(event.accounting.as_ref().unwrap().entries.is_empty());
        assert!(daemon.finish_prefetch_receipts().await);
        assert_eq!(
            events::read_transfers(&daemon.config.transfer_log_path())
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn packed_prefetch_discovers_one_pack_and_batch_imports_all_entries() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        config.prefetch_max_bytes = 0;
        let backend = test_remote_backend();
        let daemon = Arc::new(Daemon::new(config));
        assert!(daemon.remote_backend.set(backend.clone()).is_ok());
        let deps = vec![("serde".to_string(), "1.0.0".to_string())];
        let context = PackPrefetchContext::from_deps(
            crate::identity::host_target_triple(),
            "linux/toolchain/release",
            &deps,
        )
        .unwrap();
        let key_a = test_cache_key("packed-batch-a");
        let key_b = test_cache_key("packed-batch-b");
        let (payload_a, meta_a) = build_entry_pack_with_meta(&key_a, "serde");
        let (payload_b, meta_b) = build_entry_pack_with_meta(&key_b, "tokio");
        let payload_bytes = [payload_a.len() as u64, payload_b.len() as u64];
        let built = seed_packed_catalog(
            &backend,
            &context,
            vec![
                crate::remote_pack::PackInputEntry {
                    cache_key: key_a.clone(),
                    crate_name: "serde".into(),
                    meta_digest: meta_a,
                    payload: payload_a,
                },
                crate::remote_pack::PackInputEntry {
                    cache_key: key_b.clone(),
                    crate_name: "tokio".into(),
                    meta_digest: meta_b,
                    payload: payload_b,
                },
            ],
            None,
        )
        .await;

        let sentinel = test_cache_key("existing-prefetched-key");
        daemon
            .prefetched_keys
            .write()
            .await
            .insert(sentinel.clone());

        daemon.install_plan(
            "later-session",
            "later-plan",
            "fallback",
            [key_a.clone(), key_b.clone()].into_iter(),
            None,
        );
        let response = daemon
            .handle_prefetch_with_context(
                &PrefetchRequest {
                    keys: vec![(key_a.clone(), "serde".into())],
                    warm_all: false,
                    origin: Some(PrefetchOrigin {
                        session_id: "packed-session".into(),
                        plan_id: "packed-plan".into(),
                        source: "advisory".into(),
                        ..Default::default()
                    }),
                    candidate_sources: HashMap::from([(
                        key_a.clone(),
                        kache_core::CandidateSource::Manifest,
                    )]),
                },
                Some(context),
                Instant::now(),
            )
            .await;
        assert!(response.ok);
        wait_for_store_entry(&daemon, &key_a).await;
        wait_for_store_entry(&daemon, &key_b).await;
        assert!(
            daemon
                .with_store(|store| Ok(store.get(&key_a)?.is_some()))
                .unwrap()
        );
        assert!(
            daemon
                .with_store(|store| Ok(store.get(&key_b)?.is_some()))
                .unwrap()
        );
        assert_eq!(
            daemon
                .prefetch_stats
                .pack_requests_total
                .load(Ordering::Relaxed),
            3
        );
        assert_eq!(
            daemon
                .prefetch_stats
                .v3_requests_total
                .load(Ordering::Relaxed),
            0
        );
        assert!(
            daemon
                .active_plan
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .downloaded
                .is_empty(),
            "a later plan cannot claim these pack imports"
        );
        assert!(
            !daemon
                .finish_prefetch_shutdown(Duration::from_secs(2))
                .await
        );
        let transfers = events::read_transfers(&daemon.config.transfer_log_path()).unwrap();
        assert_eq!(transfers.len(), 3, "one LIST, catalog GET and pack GET");
        assert_eq!(
            transfers
                .iter()
                .map(|event| event.request_count)
                .sum::<u32>(),
            3
        );
        let list = transfers
            .iter()
            .find(|event| event.accounting.as_ref().unwrap().operation == PrefetchOperation::List)
            .unwrap();
        assert_eq!(list.accounting.as_ref().unwrap().list_result_count, Some(1));
        assert!(!list.accounting.as_ref().unwrap().bytes_complete);
        let pack = transfers
            .iter()
            .find(|event| event.format == "pack")
            .unwrap();
        assert_eq!(pack.compressed_bytes, built.bytes.len() as u64);
        assert_eq!(pack.outcome, "completed");
        assert!(pack.ok);
        assert_v3_transfer_timestamps(pack);
        assert!(pack.original_bytes > 0);
        let accounting = pack.accounting.as_ref().unwrap();
        assert!(accounting.bytes_complete && accounting.requests_complete);
        assert_eq!(accounting.entries.len(), 2);
        for (key, bytes, rank, crate_name) in [
            (&key_a, payload_bytes[0], Some(0), "serde"),
            (&key_b, payload_bytes[1], None, "tokio"),
        ] {
            let entry = accounting
                .entries
                .iter()
                .find(|entry| &entry.cache_key == key)
                .unwrap();
            assert_eq!(entry.compressed_bytes, bytes);
            assert_eq!(entry.crate_name, crate_name);
            assert_eq!(entry.outcome, "completed");
            assert!(entry.finished_at_ms >= pack.started_at_unix_ms);
            assert!(entry.finished_at_ms <= pack.finished_at_unix_ms);
            assert_eq!(entry.prefetch.session_id, "packed-session");
            assert_eq!(entry.prefetch.plan_id, "packed-plan");
            assert_eq!(entry.prefetch.candidate_rank, rank);
            assert_eq!(
                entry.prefetch.candidate_source,
                if rank.is_some() {
                    kache_core::CandidateSource::Manifest
                } else {
                    kache_core::CandidateSource::Unknown
                }
            );
        }
        assert!(
            pack.compressed_bytes > payload_bytes.iter().sum::<u64>(),
            "header bytes remain in physical denominator"
        );
        let prefetched = daemon.prefetched_keys.read().await;
        assert!(prefetched.contains(&sentinel));
        assert!(prefetched.contains(&key_a));
        assert!(prefetched.contains(&key_b));
    }

    #[tokio::test]
    async fn packed_prefetch_drains_bodies_with_room_for_only_one_object() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        let remote = test_remote_config();
        config.remote = Some(remote.clone());
        config.prefetch_max_bytes = 0;
        config.s3_concurrency = 4;
        let backend: Arc<dyn crate::remote_backend::RemoteBackend> = Arc::new(
            crate::remote_backend::memory_backend_with_download_budget(1),
        );
        let daemon = Arc::new(Daemon::new(config));
        let context = PackPrefetchContext::from_deps(
            crate::identity::host_target_triple(),
            "linux/toolchain/release",
            &[("serde".to_string(), "1.0.0".to_string())],
        )
        .unwrap();
        let mut packs = Vec::new();
        let mut candidates = Vec::new();
        for name in ["first", "second", "third"] {
            let key = test_cache_key(name);
            let (payload, meta_digest) = build_entry_pack_with_meta(&key, name);
            let built = crate::remote_pack::build_pack(
                "prefix",
                vec![crate::remote_pack::PackInputEntry {
                    cache_key: key.clone(),
                    crate_name: name.into(),
                    meta_digest: meta_digest.clone(),
                    payload,
                }],
                1 << 20,
            )
            .unwrap();
            put_test_object(&backend, &built.object_key, &built.bytes).await;
            packs.push(crate::remote_pack::CatalogPackRef {
                digest: built.digest,
                pack_bytes: built.bytes.len() as u64,
                entries: vec![crate::remote_pack::CatalogEntry {
                    cache_key: key.clone(),
                    crate_name: name.into(),
                    meta_digest,
                }],
            });
            candidates.push((key.clone(), name.into(), daemon.entry_dir_for(&key)));
        }
        let now = epoch_ms();
        let encoded = crate::remote_pack::encode_catalog(
            "prefix",
            crate::remote_pack::PackCatalog {
                version: crate::remote_pack::CATALOG_VERSION,
                key_schema: crate::cache_key::CACHE_KEY_VERSION,
                manifest_key: context.manifest_key.clone(),
                namespace: context.namespace.clone(),
                selector_hash: context.selector.clone(),
                shard_hashes: context.shard_hashes.clone(),
                created_at_ms: now,
                expires_at_ms: now + 60_000,
                packs,
                fallback_entries: Vec::new(),
            },
        )
        .unwrap();
        put_test_object(&backend, &encoded.object_key, &encoded.bytes).await;
        // A later pack holds the only reservation before the first GET starts.
        // Waiting for input order would retain that body and deadlock the first.
        let backend: Arc<dyn crate::remote_backend::RemoteBackend> =
            Arc::new(ReorderedPackBackend {
                inner: backend,
                first_key: crate::remote_pack::pack_object_key(
                    "prefix",
                    &encoded.catalog.packs[0].digest,
                )
                .unwrap(),
                later_downloaded: Notify::new(),
            });
        daemon.set_remote_backend_for_test(backend);
        let v3 = daemon.v3_remote().await.unwrap();
        let imported = tokio::time::timeout(
            Duration::from_secs(3),
            daemon.try_packed_prefetch(
                &context,
                v3,
                &remote,
                &candidates,
                0,
                &PackedAttribution {
                    origin: &PrefetchOrigin::default(),
                    ranks: &HashMap::new(),
                    sources: &HashMap::new(),
                },
            ),
        )
        .await
        .expect("catalog and pack bodies must be released while the queue is draining");
        assert_eq!(imported.len(), 3);
        for (key, _, _) in candidates {
            assert!(imported.contains(&key));
            assert!(
                daemon
                    .with_store(|store| Ok(store.get(&key)?.is_some()))
                    .unwrap()
            );
        }
        assert_eq!(
            daemon
                .prefetch_stats
                .pack_requests_total
                .load(Ordering::Relaxed),
            5
        );
    }

    #[tokio::test]
    async fn packed_prefetch_response_returns_before_blocked_pack_get() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let inner = test_remote_backend();
        let context = PackPrefetchContext::from_deps(
            crate::identity::host_target_triple(),
            "linux/toolchain/release",
            &[("serde".to_string(), "1.0.0".to_string())],
        )
        .unwrap();
        let key = test_cache_key("nonblocking-prefetch-pack");
        let (payload, meta_digest) = build_entry_pack_with_meta(&key, "serde");
        seed_packed_catalog(
            &inner,
            &context,
            vec![crate::remote_pack::PackInputEntry {
                cache_key: key.clone(),
                crate_name: "serde".into(),
                meta_digest,
                payload,
            }],
            None,
        )
        .await;

        let pack_started = Arc::new(Notify::new());
        let release_pack = Arc::new(tokio::sync::Semaphore::new(0));
        let backend: Arc<dyn crate::remote_backend::RemoteBackend> =
            Arc::new(BlockingPackBackend {
                inner,
                pack_started: pack_started.clone(),
                release_pack: release_pack.clone(),
                v3_get_started: None,
            });
        let daemon = Arc::new(Daemon::new(config));
        assert!(daemon.remote_backend.set(backend).is_ok());

        let started = pack_started.notified();
        tokio::pin!(started);
        started.as_mut().enable();
        let response = tokio::time::timeout(
            Duration::from_secs(10),
            daemon.handle_prefetch_with_context(
                &PrefetchRequest {
                    keys: vec![(key.clone(), "serde".into())],
                    warm_all: false,
                    origin: None,
                    candidate_sources: HashMap::new(),
                },
                Some(context),
                Instant::now(),
            ),
        )
        .await
        .expect("prefetch acknowledgement must not await the pack GET");
        assert!(response.ok);
        tokio::time::timeout(Duration::from_secs(10), started)
            .await
            .expect("background coordinator should start the pack GET");
        assert!(!daemon.entry_dir_for(&key).exists());

        release_pack.add_permits(1);
        wait_for_store_entry(&daemon, &key).await;
    }

    #[tokio::test]
    async fn demand_v3_read_completes_while_a_prefetch_pack_is_blocked() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let inner = test_remote_backend();
        let deps = vec![("serde".to_string(), "1.0.0".to_string())];
        let context = PackPrefetchContext::from_deps(
            crate::identity::host_target_triple(),
            "linux/toolchain/release",
            &deps,
        )
        .unwrap();
        let packed_key = test_cache_key("blocked-prefetch-pack");
        let (packed_payload, packed_meta) = build_entry_pack_with_meta(&packed_key, "serde");
        seed_packed_catalog(
            &inner,
            &context,
            vec![crate::remote_pack::PackInputEntry {
                cache_key: packed_key.clone(),
                crate_name: "serde".into(),
                meta_digest: packed_meta,
                payload: packed_payload,
            }],
            None,
        )
        .await;

        let demand_key = test_cache_key("demand-during-packed-prefetch");
        let demand_payload = build_entry_pack(&demand_key, "tokio");
        put_test_object(
            &inner,
            &test_manifest_object_key(&demand_key, "tokio"),
            b"{}",
        )
        .await;
        put_test_object(
            &inner,
            &test_pack_object_key(&demand_key, "tokio"),
            &demand_payload,
        )
        .await;

        let pack_started = Arc::new(Notify::new());
        let release_pack = Arc::new(tokio::sync::Semaphore::new(0));
        let v3_get_started = Arc::new(Notify::new());
        let backend: Arc<dyn crate::remote_backend::RemoteBackend> =
            Arc::new(BlockingPackBackend {
                inner,
                pack_started: pack_started.clone(),
                release_pack: release_pack.clone(),
                v3_get_started: Some(v3_get_started.clone()),
            });
        let daemon = Arc::new(Daemon::new(config));
        assert!(daemon.remote_backend.set(backend).is_ok());
        daemon.signal_warming_complete();

        let started = pack_started.notified();
        tokio::pin!(started);
        started.as_mut().enable();
        let packed_daemon = daemon.clone();
        let packed = tokio::spawn(async move {
            packed_daemon
                .handle_prefetch_with_context(
                    &PrefetchRequest {
                        keys: vec![(packed_key, "serde".into())],
                        warm_all: false,
                        origin: None,
                        candidate_sources: HashMap::new(),
                    },
                    Some(context),
                    Instant::now(),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(10), started)
            .await
            .expect("pack GET should block in the injected backend");

        let v3_started = v3_get_started.notified();
        tokio::pin!(v3_started);
        v3_started.as_mut().enable();
        let demand_daemon = daemon.clone();
        let demand_entry_dir = daemon
            .entry_dir_for(&demand_key)
            .to_string_lossy()
            .into_owned();
        let demand_task = tokio::spawn(async move {
            demand_daemon
                .handle_remote_check(&RemoteCheckRequest {
                    key: demand_key.clone(),
                    entry_dir: demand_entry_dir,
                    crate_name: "tokio".into(),
                    deadline_ms: None,
                    shard_dir: None,
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(10), v3_started)
            .await
            .expect("demand v3 GET must start while the pack GET remains blocked");
        let demand = tokio::time::timeout(Duration::from_secs(10), demand_task)
            .await
            .expect("demand v3 read must complete while the pack GET remains blocked")
            .expect("demand task must not panic");
        assert_eq!(demand.found, Some(true));

        release_pack.add_permits(1);
        assert!(packed.await.unwrap().ok);
    }

    #[tokio::test]
    async fn corrupt_pack_falls_back_only_to_the_existing_v3_entry() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let backend = test_remote_backend();
        let daemon = Arc::new(Daemon::new(config));
        assert!(daemon.remote_backend.set(backend.clone()).is_ok());
        let deps = vec![("serde".to_string(), "1.0.0".to_string())];
        let context = PackPrefetchContext::from_deps(
            crate::identity::host_target_triple(),
            "linux/toolchain/release",
            &deps,
        )
        .unwrap();
        let key = test_cache_key("packed-corrupt-fallback");
        let (payload, meta_digest) = build_entry_pack_with_meta(&key, "serde");
        seed_packed_catalog(
            &backend,
            &context,
            vec![crate::remote_pack::PackInputEntry {
                cache_key: key.clone(),
                crate_name: "serde".into(),
                meta_digest,
                payload: payload.clone(),
            }],
            Some(b"corrupt immutable pack".to_vec()),
        )
        .await;
        put_test_object(&backend, &test_pack_object_key(&key, "serde"), &payload).await;

        let response = daemon
            .handle_prefetch_with_context(
                &PrefetchRequest {
                    keys: vec![(key.clone(), "serde".into())],
                    warm_all: false,
                    origin: None,
                    candidate_sources: HashMap::new(),
                },
                Some(context),
                Instant::now(),
            )
            .await;
        assert!(response.ok);
        wait_for_store_entry(&daemon, &key).await;
        assert_eq!(
            daemon
                .prefetch_stats
                .v3_requests_total
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            daemon
                .prefetch_stats
                .pack_fallback_entries
                .load(Ordering::Relaxed),
            1
        );
        assert!(
            daemon
                .prefetch_stats
                .pack_validation_failures
                .load(Ordering::Relaxed)
                >= 1
        );
        assert!(
            !daemon
                .finish_prefetch_shutdown(Duration::from_secs(2))
                .await
        );
        let transfers = events::read_transfers(&daemon.config.transfer_log_path()).unwrap();
        let pack = transfers
            .iter()
            .find(|event| event.format == "pack")
            .unwrap();
        assert_eq!(pack.compressed_bytes, 22);
        assert_eq!(pack.outcome, "validation_error");
        assert!(!pack.ok);
        assert!(pack.accounting.as_ref().unwrap().bytes_complete);
        assert!(pack.accounting.as_ref().unwrap().entries.is_empty());
    }

    #[tokio::test]
    async fn catalog_filename_timestamp_mismatch_rejects_context_and_uses_v3() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let backend = test_remote_backend();
        let daemon = Arc::new(Daemon::new(config));
        assert!(daemon.remote_backend.set(backend.clone()).is_ok());
        let context = PackPrefetchContext::from_deps(
            crate::identity::host_target_triple(),
            "linux/toolchain/release",
            &[("serde".to_string(), "1.0.0".to_string())],
        )
        .unwrap();
        let key = test_cache_key("catalog-created-at-mismatch");
        let (payload, meta_digest) = build_entry_pack_with_meta(&key, "serde");
        let built = crate::remote_pack::build_pack(
            "prefix",
            vec![crate::remote_pack::PackInputEntry {
                cache_key: key.clone(),
                crate_name: "serde".into(),
                meta_digest: meta_digest.clone(),
                payload: payload.clone(),
            }],
            crate::remote_pack::DEFAULT_MAX_PACK_BYTES,
        )
        .unwrap();
        put_test_object(&backend, &built.object_key, &built.bytes).await;
        let created_at_ms = epoch_ms();
        let catalog = crate::remote_pack::PackCatalog {
            version: crate::remote_pack::CATALOG_VERSION,
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            manifest_key: context.manifest_key.clone(),
            namespace: context.namespace.clone(),
            selector_hash: context.selector.clone(),
            shard_hashes: context.shard_hashes.clone(),
            created_at_ms,
            expires_at_ms: created_at_ms + 60_000,
            packs: vec![crate::remote_pack::CatalogPackRef {
                digest: built.digest,
                pack_bytes: built.bytes.len() as u64,
                entries: vec![crate::remote_pack::CatalogEntry {
                    cache_key: key.clone(),
                    crate_name: "serde".into(),
                    meta_digest,
                }],
            }],
            fallback_entries: Vec::new(),
        };
        let encoded = crate::remote_pack::encode_catalog("prefix", catalog).unwrap();
        let mismatched_key = crate::remote_pack::catalog_object_key(
            "prefix",
            &context.selector,
            created_at_ms + 1,
            &encoded.digest,
        )
        .unwrap();
        put_test_object(&backend, &mismatched_key, &encoded.bytes).await;
        put_test_object(&backend, &test_pack_object_key(&key, "serde"), &payload).await;

        let response = daemon
            .handle_prefetch_with_context(
                &PrefetchRequest {
                    keys: vec![(key.clone(), "serde".into())],
                    warm_all: false,
                    origin: None,
                    candidate_sources: HashMap::new(),
                },
                Some(context),
                Instant::now(),
            )
            .await;
        assert!(response.ok);
        wait_for_store_entry(&daemon, &key).await;
        assert_eq!(
            daemon
                .prefetch_stats
                .v3_requests_total
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            daemon
                .prefetch_stats
                .pack_fallback_entries
                .load(Ordering::Relaxed),
            1
        );
        assert!(
            daemon
                .prefetch_stats
                .pack_validation_failures
                .load(Ordering::Relaxed)
                >= 1
        );
    }

    #[tokio::test]
    async fn traversal_in_one_pack_entry_preserves_valid_batch_and_falls_back_per_entry() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let backend = test_remote_backend();
        let daemon = Arc::new(Daemon::new(config));
        assert!(daemon.remote_backend.set(backend.clone()).is_ok());
        let deps = vec![("serde".to_string(), "1.0.0".to_string())];
        let context = PackPrefetchContext::from_deps(
            crate::identity::host_target_triple(),
            "linux/toolchain/release",
            &deps,
        )
        .unwrap();
        let good_key = test_cache_key("packed-partial-good");
        let bad_key = test_cache_key("packed-partial-traversal");
        let (good_payload, good_meta) = build_entry_pack_with_meta(&good_key, "serde");
        let (fallback_payload, _) = build_entry_pack_with_meta(&bad_key, "tokio");
        seed_packed_catalog(
            &backend,
            &context,
            vec![
                crate::remote_pack::PackInputEntry {
                    cache_key: good_key.clone(),
                    crate_name: "serde".into(),
                    meta_digest: good_meta,
                    payload: good_payload,
                },
                crate::remote_pack::PackInputEntry {
                    cache_key: bad_key.clone(),
                    crate_name: "tokio".into(),
                    meta_digest: blake3::hash(b"malicious-meta").to_hex().to_string(),
                    payload: traversal_entry_payload(),
                },
            ],
            None,
        )
        .await;
        put_test_object(
            &backend,
            &test_pack_object_key(&bad_key, "tokio"),
            &fallback_payload,
        )
        .await;

        let response = daemon
            .handle_prefetch_with_context(
                &PrefetchRequest {
                    keys: vec![
                        (good_key.clone(), "serde".into()),
                        (bad_key.clone(), "tokio".into()),
                    ],
                    warm_all: false,
                    origin: None,
                    candidate_sources: HashMap::new(),
                },
                Some(context),
                Instant::now(),
            )
            .await;
        assert!(response.ok);
        wait_for_store_entry(&daemon, &bad_key).await;
        assert!(
            daemon
                .with_store(|store| Ok(store.get(&good_key)?.is_some()))
                .unwrap()
        );
        assert!(!dir.path().join("escape.txt").exists());
        assert_eq!(
            daemon
                .prefetch_stats
                .v3_requests_total
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            daemon
                .prefetch_stats
                .pack_fallback_entries
                .load(Ordering::Relaxed),
            1
        );
        assert!(
            !daemon
                .finish_prefetch_shutdown(Duration::from_secs(2))
                .await
        );
        let transfers = events::read_transfers(&daemon.config.transfer_log_path()).unwrap();
        let pack = transfers
            .iter()
            .find(|event| event.format == "pack")
            .unwrap();
        assert_eq!(pack.outcome, "import_error");
        assert!(!pack.ok);
        let accounting = pack.accounting.as_ref().unwrap();
        let good = accounting
            .entries
            .iter()
            .find(|entry| entry.cache_key == good_key)
            .unwrap();
        assert_eq!(good.crate_name, "serde");
        assert_eq!(good.outcome, "completed");
        let bad = accounting
            .entries
            .iter()
            .find(|entry| entry.cache_key == bad_key)
            .unwrap();
        assert_eq!(bad.crate_name, "tokio");
        assert_eq!(bad.outcome, "validation_error");
    }

    #[tokio::test]
    async fn packed_receipt_keeps_cancelled_entries_when_claim_is_blocked() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let remote = config.remote.clone().unwrap();
        let backend = test_remote_backend();
        let daemon = Arc::new(Daemon::new(config));
        assert!(daemon.remote_backend.set(backend.clone()).is_ok());
        let context = PackPrefetchContext::from_deps(
            crate::identity::host_target_triple(),
            "linux/toolchain/release",
            &[("serde".to_string(), "1.0.0".to_string())],
        )
        .unwrap();
        let key = test_cache_key("cancelled-packed-claim");
        let (payload, meta_digest) = build_entry_pack_with_meta(&key, "serde");
        seed_packed_catalog(
            &backend,
            &context,
            vec![crate::remote_pack::PackInputEntry {
                cache_key: key.clone(),
                crate_name: "serde".into(),
                meta_digest,
                payload,
            }],
            None,
        )
        .await;
        let v3 = daemon.v3_remote().await.unwrap().clone();
        let candidates = vec![(key.clone(), "serde".into(), daemon.entry_dir_for(&key))];
        let claim = daemon.downloading.write().await;
        let worker = daemon.clone();
        let task = tokio::spawn(async move {
            let origin = PrefetchOrigin::default();
            let ranks = HashMap::new();
            let sources = HashMap::new();
            worker
                .try_packed_prefetch(
                    &context,
                    &v3,
                    &remote,
                    &candidates,
                    0,
                    &PackedAttribution {
                        origin: &origin,
                        ranks: &ranks,
                        sources: &sources,
                    },
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(3), async {
            while daemon
                .transfer_counters
                .downloads_completed
                .load(Ordering::Relaxed)
                == 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pack body should decode before the claim waits");
        tokio::time::sleep(Duration::from_millis(50)).await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        drop(claim);
        assert!(daemon.finish_prefetch_receipts().await);
        let transfers = events::read_transfers(&daemon.config.transfer_log_path()).unwrap();
        let pack = transfers
            .iter()
            .find(|event| event.format == "pack")
            .unwrap();
        assert_eq!(pack.outcome, "cancelled");
        let entry = pack
            .accounting
            .as_ref()
            .unwrap()
            .entries
            .iter()
            .find(|entry| entry.cache_key == key)
            .unwrap();
        assert_eq!(entry.crate_name, "serde");
        assert_eq!(entry.outcome, "cancelled");
    }

    #[tokio::test]
    async fn packed_import_error_marks_the_pack_failed_without_entry_validation_errors() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        std::fs::create_dir_all(config.store_dir()).unwrap();
        std::fs::write(config.store_dir().join("blobs"), b"not a directory").unwrap();
        let backend = test_remote_backend();
        let daemon = Arc::new(Daemon::new(config));
        assert!(daemon.remote_backend.set(backend.clone()).is_ok());
        let context = PackPrefetchContext::from_deps(
            crate::identity::host_target_triple(),
            "linux/toolchain/release",
            &[("serde".to_string(), "1.0.0".to_string())],
        )
        .unwrap();
        let key = test_cache_key("packed-import-error");
        let (payload, meta_digest) = build_entry_pack_with_meta(&key, "serde");
        seed_packed_catalog(
            &backend,
            &context,
            vec![crate::remote_pack::PackInputEntry {
                cache_key: key.clone(),
                crate_name: "serde".into(),
                meta_digest,
                payload,
            }],
            None,
        )
        .await;
        let response = daemon
            .handle_prefetch_with_context(
                &PrefetchRequest {
                    keys: vec![(key.clone(), "serde".into())],
                    warm_all: false,
                    origin: None,
                    candidate_sources: HashMap::new(),
                },
                Some(context),
                Instant::now(),
            )
            .await;
        assert!(response.ok);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if daemon
                    .prefetch_stats
                    .pack_validation_failures
                    .load(Ordering::Relaxed)
                    >= 1
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("packed import failure should be counted");
        assert!(
            !daemon
                .finish_prefetch_shutdown(Duration::from_secs(2))
                .await
        );
        let transfers = events::read_transfers(&daemon.config.transfer_log_path()).unwrap();
        let pack = transfers
            .iter()
            .find(|event| event.format == "pack")
            .unwrap();
        assert_eq!(pack.outcome, "import_error");
        assert!(!pack.ok);
        let entry = pack
            .accounting
            .as_ref()
            .unwrap()
            .entries
            .iter()
            .find(|entry| entry.cache_key == key)
            .unwrap();
        assert_eq!(entry.crate_name, "serde");
        assert_eq!(entry.outcome, "import_error");
    }

    #[tokio::test]
    async fn test_remote_check_success_records_v3_transfer_timestamps() {
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
                shard_dir: None,
            })
            .await;

        assert!(resp.ok, "hit+download should succeed: {resp:?}");
        assert_eq!(resp.found, Some(true));
        assert!(
            config.store_dir().join(&key).join("meta.json").exists(),
            "entry should be imported into the local store"
        );
        assert_v3_transfer_timestamps(&latest_transfer(&daemon));
    }

    #[tokio::test]
    async fn remote_check_with_configured_shard_dir_imports_into_the_shard() {
        let dir = tempfile::tempdir().unwrap();
        let main = dir.path().join("main");
        let shard = dir.path().join("shard");
        std::fs::create_dir_all(&main).unwrap();
        std::fs::create_dir_all(&shard).unwrap();
        let mut config = test_config(&main);
        config.remote = Some(test_remote_config());
        config.prefetch_enabled = false;
        config.volume_stores = vec![crate::config::VolumeStore {
            volume: "/mnt/vol/".into(),
            store: shard.clone(),
        }];
        let key = test_cache_key("shard-download");
        let pack = build_entry_pack(&key, "serde");
        let entry_dir = shard.join("store").join(&key);

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
                shard_dir: Some(shard.to_string_lossy().into_owned()),
            })
            .await;

        assert!(resp.ok, "hit+download should succeed: {resp:?}");
        assert_eq!(resp.found, Some(true));
        assert!(
            shard.join("store").join(&key).join("meta.json").exists(),
            "entry should land in the requesting shard"
        );
        assert!(
            !main.join("store").join(&key).join("meta.json").exists(),
            "the main store must not receive a shard-targeted import"
        );
    }

    #[tokio::test]
    async fn remote_check_rejects_unconfigured_shard_dir() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon = Daemon::new(config);
        let key = test_cache_key("bad-shard");
        let resp = daemon
            .handle_remote_check(&RemoteCheckRequest {
                key,
                entry_dir: "/unused".into(),
                crate_name: "serde".into(),
                deadline_ms: None,
                shard_dir: Some("/not/a/configured/shard".into()),
            })
            .await;
        assert!(!resp.ok);
        assert_eq!(
            resp.error.as_deref(),
            Some("remote-check shard_dir is not a configured volume store")
        );
    }

    #[tokio::test]
    async fn remote_check_import_failure_is_not_reported_as_hit() {
        // The v3 GET/extraction can succeed while local publication fails. A
        // regular file at `store/blobs` deterministically makes the import's
        // `create_dir_all(store/blobs/<shard>)` fail on every platform, without
        // weakening or corrupting the otherwise-valid remote pack.
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        config.prefetch_enabled = false;
        let key = test_cache_key("download-import-failure");
        let pack = build_entry_pack(&key, "serde");
        let entry_dir = config.store_dir().join(&key);
        std::fs::create_dir_all(config.store_dir()).unwrap();
        std::fs::write(config.store_dir().join("blobs"), b"not a directory").unwrap();

        let client = test_remote_backend();
        put_test_object(&client, &test_manifest_object_key(&key, "serde"), b"{}").await;
        put_test_object(&client, &test_pack_object_key(&key, "serde"), &pack).await;
        let daemon = Arc::new(Daemon::new(config.clone()));
        assert!(daemon.remote_backend.set(client).is_ok());

        let response = daemon
            .handle_remote_check(&RemoteCheckRequest {
                key: key.clone(),
                entry_dir: entry_dir.to_string_lossy().into_owned(),
                crate_name: "serde".into(),
                deadline_ms: None,
                shard_dir: None,
            })
            .await;

        assert!(
            response.ok,
            "a cache fault must degrade to a miss: {response:?}"
        );
        assert_eq!(response.found, Some(false));
        assert_eq!(
            daemon
                .transfer_counters
                .downloads_completed
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            daemon
                .transfer_counters
                .downloads_failed
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            daemon
                .transfer_counters
                .bytes_downloaded
                .load(Ordering::Relaxed),
            pack.len() as u64,
            "a local import failure must not hide bytes already transferred"
        );
        let transfer = latest_transfer(&daemon);
        assert!(!transfer.ok);
        assert_eq!(transfer.compressed_bytes, pack.len() as u64);
        assert!(
            !entry_dir.exists(),
            "failed extraction must not leave meta.json that a waiter can mistake for a hit"
        );
        assert!(!Store::open(&config).unwrap().contains(&key));
    }

    #[tokio::test]
    async fn concurrent_remote_check_waiter_retries_after_leader_import_failure() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        config.prefetch_enabled = false;
        let key = test_cache_key("concurrent-import-failure");
        let pack = build_entry_pack(&key, "serde");
        let entry_dir = config.store_dir().join(&key);
        std::fs::create_dir_all(config.store_dir()).unwrap();
        std::fs::write(config.store_dir().join("blobs"), b"not a directory").unwrap();

        let inner = test_remote_backend();
        put_test_object(&inner, &test_manifest_object_key(&key, "serde"), b"{}").await;
        put_test_object(&inner, &test_pack_object_key(&key, "serde"), &pack).await;
        let (v3_started_tx, mut v3_started_rx) = tokio::sync::mpsc::unbounded_channel();
        let release_v3_get = Arc::new(tokio::sync::Semaphore::new(0));
        let gated = Arc::new(BlockingV3Backend {
            inner,
            v3_get_started: v3_started_tx,
            release_v3_get: release_v3_get.clone(),
            v3_gets: AtomicU64::new(0),
        });
        let backend: Arc<dyn crate::remote_backend::RemoteBackend> = gated.clone();
        let daemon = Arc::new(Daemon::new(config));
        assert!(daemon.remote_backend.set(backend).is_ok());
        daemon.signal_warming_complete();

        let request = RemoteCheckRequest {
            key: key.clone(),
            entry_dir: entry_dir.to_string_lossy().into_owned(),
            crate_name: "serde".into(),
            deadline_ms: None,
            shard_dir: None,
        };
        let leader = {
            let daemon = daemon.clone();
            let request = request.clone();
            tokio::spawn(async move {
                daemon
                    .handle_remote_check_leader(&request, RemoteDeadline::from_secs(10))
                    .await
            })
        };
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), v3_started_rx.recv())
                .await
                .expect("leader must reach its gated v3 GET"),
            Some(1)
        );

        let waiter = {
            let daemon = daemon.clone();
            tokio::spawn(async move {
                daemon
                    .handle_remote_check_leader(&request, RemoteDeadline::from_secs(10))
                    .await
            })
        };
        wait_for_download_waiter(&daemon, &key).await;

        // Let the first valid pack finish. Its local import fails, releases the
        // claim, and wakes the attached waiter. The waiter must re-claim and
        // start a second GET; returning a hit from stale meta would skip it.
        release_v3_get.add_permits(1);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), v3_started_rx.recv())
                .await
                .expect("waiter must retry rather than report the failed leader as a hit"),
            Some(2)
        );
        let leader_response = tokio::time::timeout(Duration::from_secs(5), leader)
            .await
            .expect("leader must finish after its GET is released")
            .expect("leader task must not panic");
        assert_eq!(leader_response.found, Some(false));

        release_v3_get.add_permits(1);
        let waiter_response = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("waiter must finish after its retry is released")
            .expect("waiter task must not panic");
        assert_eq!(waiter_response.found, Some(false));
        assert_eq!(gated.v3_gets.load(Ordering::SeqCst), 2);
        assert_eq!(
            daemon
                .transfer_counters
                .downloads_completed
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            daemon
                .transfer_counters
                .downloads_failed
                .load(Ordering::Relaxed),
            2
        );
    }

    #[tokio::test]
    async fn remote_check_waiter_rejects_uncommitted_meta_after_leader_import_failure() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        config.prefetch_enabled = false;
        let key = test_cache_key("waiter-uncommitted-meta");
        let entry_dir = config.store_dir().join(&key);
        let daemon = Arc::new(Daemon::new(config.clone()));
        let backend: Arc<dyn crate::remote_backend::RemoteBackend> = Arc::new(PanicOnGetBackend);
        assert!(daemon.remote_backend.set(backend).is_ok());
        daemon.signal_warming_complete();

        // Model a leader that has the download claim and lands meta.json, but
        // whose Store import fails before publishing a row. Keeping the residue
        // deliberately exercises the committed-state check independently of
        // best-effort cleanup.
        assert!(claim_download(&daemon.downloading, &key).await.is_none());
        let failed_leader = DownloadingGuard::new(daemon.downloading.clone(), key.clone());
        let request = RemoteCheckRequest {
            key: key.clone(),
            entry_dir: entry_dir.to_string_lossy().into_owned(),
            crate_name: "serde".into(),
            deadline_ms: None,
            shard_dir: None,
        };
        let waiter = {
            let daemon = daemon.clone();
            tokio::spawn(async move {
                daemon
                    .handle_remote_check_leader(&request, RemoteDeadline::from_secs(10))
                    .await
            })
        };
        wait_for_download_waiter(&daemon, &key).await;

        std::fs::create_dir_all(&entry_dir).unwrap();
        std::fs::write(entry_dir.join("meta.json"), b"{}").unwrap();
        let store = Store::open(&config).unwrap();
        assert!(store.import_downloaded_entry(&key).is_err());
        assert!(entry_dir.join("meta.json").exists());
        assert!(!store.contains(&key));
        drop(failed_leader);

        let response = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("failed leader must wake the waiter")
            .expect("waiter task must not panic");
        assert_eq!(
            response.found,
            Some(false),
            "meta.json without a committed Store row is never a hit"
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
                shard_dir: None,
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
                origin: None,
                candidate_sources: HashMap::new(),
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
    async fn test_handle_prefetch_success_records_v3_transfer_timestamps() {
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

        let already_local = test_cache_key("already-local");
        std::fs::create_dir_all(config.store_dir().join(&already_local)).unwrap();
        let resp = daemon
            .handle_prefetch(&PrefetchRequest {
                keys: vec![
                    (already_local, "old".to_string()),
                    (key.to_string(), "serde".to_string()),
                    (key.to_string(), "serde".to_string()),
                ],
                warm_all: false,
                origin: None,
                candidate_sources: HashMap::new(),
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

        let mut transfer = None;
        for _ in 0..100 {
            transfer = daemon
                .recent_transfers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .iter()
                .rev()
                .find(|event| event.cache_key == key && event.outcome == "completed")
                .cloned();
            if transfer.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let transfer = transfer.expect("completed prefetch should record transfer timing");
        assert_v3_transfer_timestamps(&transfer);
        assert_eq!(transfer.outcome, "completed");
        assert_eq!(transfer.prefetch.as_ref().unwrap().source, "unscoped");
        assert_eq!(transfer.prefetch.as_ref().unwrap().candidate_rank, Some(1));
        assert!(
            transfer.elapsed_ms >= transfer.import_lock_wait_ms + transfer.import_ms,
            "end-to-end elapsed must include lock wait and import execution: {transfer:?}"
        );
    }

    #[tokio::test]
    async fn test_handle_prefetch_failure_records_v3_transfer_timestamps() {
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
                origin: None,
                candidate_sources: HashMap::new(),
            })
            .await;
        assert!(
            resp.ok,
            "prefetch dispatch is ok even if downloads fail: {resp:?}"
        );

        // The download runs in the background. downloads_failed is bumped
        // before the TransferEvent is pushed, so waiting on the counter
        // alone races on Windows. Wait for both, same as
        // prefetch_import_failure_is_counted_as_failure.
        let completed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let failed = daemon
                    .transfer_counters
                    .downloads_failed
                    .load(Ordering::Relaxed);
                let has_event = daemon
                    .recent_transfers
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .back()
                    .is_some();
                if failed >= 1 && has_event {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            completed.is_ok(),
            "a garbage pack must record a failed download and a transfer event"
        );
        let transfer = latest_transfer(&daemon);
        assert_v3_transfer_timestamps(&transfer);
        assert_eq!(transfer.outcome, "error");
        assert_eq!(transfer.request_count, 1);
        assert_eq!(transfer.compressed_bytes, 16);
        assert!(transfer.accounting.as_ref().unwrap().bytes_complete);
        assert_eq!(
            daemon
                .prefetch_stats
                .v3_requests_total
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            daemon
                .prefetch_stats
                .v3_bytes_downloaded
                .load(Ordering::Relaxed),
            16
        );
        // Nothing was imported.
        assert!(!config.store_dir().join(key).join("meta.json").exists());
    }

    #[test]
    fn plan_downloads_reject_a_different_origin() {
        let origin = PrefetchOrigin {
            session_id: "session".to_string(),
            plan_id: "plan".to_string(),
            source: "advisory".to_string(),
            ..PrefetchOrigin::default()
        };
        for (session, plan_id, source) in [
            ("other", "plan", "advisory"),
            ("session", "other", "advisory"),
            ("session", "plan", "fallback"),
        ] {
            let mut plan = ActivePlan::new(
                session.to_string(),
                plan_id.to_string(),
                source,
                HashSet::from(["key".to_string()]),
                0,
                0,
            );
            plan.record_download_from(&origin, "key", 42);
            assert!(plan.downloaded.is_empty());
        }
        let mut plan = ActivePlan::new(
            "session".to_string(),
            "plan".to_string(),
            "advisory",
            HashSet::from(["key".to_string()]),
            0,
            0,
        );
        plan.record_download_from(&origin, "key", 42);
        assert_eq!(plan.downloaded, HashMap::from([("key".to_string(), 42)]));
    }

    async fn shutdown_prefetch_fixture(
        key_count: usize,
    ) -> (
        tempfile::TempDir,
        Arc<Daemon>,
        Arc<BlockingV3Backend>,
        tokio::sync::mpsc::UnboundedReceiver<u64>,
        Vec<String>,
        u64,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        config.s3_concurrency = 1;
        let inner = test_remote_backend();
        let keys: Vec<_> = (0..key_count)
            .map(|index| test_cache_key(&format!("shutdown-{index}")))
            .collect();
        let mut first_bytes = 0;
        for (index, key) in keys.iter().enumerate() {
            let pack = build_entry_pack(key, "serde");
            if index == 0 {
                first_bytes = pack.len() as u64;
            }
            put_test_object(&inner, &test_pack_object_key(key, "serde"), &pack).await;
        }
        let (started, received) = tokio::sync::mpsc::unbounded_channel();
        let backend = Arc::new(BlockingV3Backend {
            inner,
            v3_get_started: started,
            release_v3_get: Arc::new(tokio::sync::Semaphore::new(0)),
            v3_gets: AtomicU64::new(0),
        });
        let daemon = Arc::new(Daemon::new(config));
        assert!(daemon.remote_backend.set(backend.clone()).is_ok());
        daemon.install_plan(
            "session",
            "plan",
            "advisory",
            keys.clone().into_iter(),
            Some("identity".into()),
        );
        let origin = PrefetchOrigin {
            session_id: "session".into(),
            plan_id: "plan".into(),
            source: "advisory".into(),
            ..PrefetchOrigin::default()
        };
        assert!(
            daemon
                .handle_prefetch(&PrefetchRequest {
                    keys: keys
                        .iter()
                        .cloned()
                        .map(|key| (key, "serde".into()))
                        .collect(),
                    warm_all: false,
                    origin: Some(origin),
                    candidate_sources: HashMap::new(),
                })
                .await
                .ok
        );
        (dir, daemon, backend, received, keys, first_bytes)
    }

    #[tokio::test]
    async fn shutdown_prefetch_drains_started_child_before_summary_and_rejects_queued_work() {
        let (_dir, daemon, backend, mut started, keys, bytes) = shutdown_prefetch_fixture(2).await;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), started.recv())
                .await
                .unwrap(),
            Some(1)
        );
        daemon
            .active_plan
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .record_demand(&keys[0]);
        daemon.stop_prefetch_admission();
        assert!(
            !*daemon.prefetch_cancel.borrow(),
            "shutdown is not adaptive cancellation"
        );
        let draining = tokio::spawn({
            let daemon = daemon.clone();
            async move {
                daemon
                    .finish_prefetch_shutdown(Duration::from_secs(2))
                    .await
            }
        });
        tokio::task::yield_now().await;
        assert!(!draining.is_finished());
        assert!(
            !daemon.config.summary_log_path().exists(),
            "summary cannot precede an in-flight outcome"
        );
        backend.release_v3_get.add_permits(1);
        assert!(!draining.await.unwrap());
        assert_eq!(
            backend.v3_gets.load(Ordering::SeqCst),
            1,
            "queued candidate must not start"
        );
        assert!(!daemon.entry_dir_for(&keys[1]).exists());
        assert!(daemon.downloading.read().await.is_empty());
        let summaries = events::read_summaries(&daemon.config.summary_log_path()).unwrap();
        assert_eq!(summaries.len(), 1);
        let summary = &summaries[0];
        assert_eq!(summary.schema, 2);
        assert_eq!(summary.closure_reason, "shutdown");
        assert!(summary.cancelled);
        assert!(!summary.incomplete);
        assert_eq!(summary.downloaded_keys, 1);
        assert_eq!(summary.downloaded_bytes, bytes);
        assert_eq!(summary.used_keys, 1);
        assert_eq!(summary.used_bytes, bytes);
        assert_eq!(
            daemon.prefetch_stats.keys_cancelled.load(Ordering::Relaxed),
            1
        );
        assert!(!daemon.maybe_publish_identity_manifest(Some("identity"), "session"));
        assert!(!daemon.finish_prefetch_shutdown(Duration::ZERO).await);
        assert_eq!(
            events::read_summaries(&daemon.config.summary_log_path())
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn shutdown_prefetch_rejects_a_child_waiting_for_the_download_claim() {
        let (_dir, daemon, backend, _started, _keys, _bytes) = shutdown_prefetch_fixture(2).await;
        let claims = daemon.downloading.write().await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while daemon.s3_semaphore.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("child must reach the claim queue after taking its remote permit");
        daemon.stop_prefetch_admission();
        drop(claims);
        // A rejected candidate cannot consume this permit. Letting a wrongly
        // admitted GET finish makes the failure observable without a timeout.
        backend.release_v3_get.add_permits(1);
        assert!(
            !daemon
                .finish_prefetch_shutdown(Duration::from_secs(2))
                .await
        );
        assert_eq!(
            backend.v3_gets.load(Ordering::SeqCst),
            0,
            "a child queued on its claim must not start GET after shutdown"
        );
        assert!(daemon.downloading.read().await.is_empty());
        let summaries = events::read_summaries(&daemon.config.summary_log_path()).unwrap();
        assert_eq!(summaries.len(), 1);
        assert!(summaries[0].cancelled);
        assert!(!summaries[0].incomplete);
        assert_eq!(summaries[0].downloaded_keys, 0);
        assert_eq!(
            daemon.prefetch_stats.keys_cancelled.load(Ordering::Relaxed),
            2
        );
        let transfer = latest_transfer(&daemon);
        assert_eq!(transfer.request_count, 0);
        assert_eq!(transfer.outcome, "cancelled");
        assert!(transfer.accounting.as_ref().unwrap().bytes_complete);
        assert_eq!(
            daemon
                .prefetch_stats
                .v3_requests_total
                .load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn prefetch_receipt_skipped_claim_records_no_backend_get() {
        let (_dir, daemon, backend, mut started, keys, _bytes) = shutdown_prefetch_fixture(2).await;
        daemon
            .downloading
            .write()
            .await
            .insert(keys[0].clone(), Arc::new(Notify::new()));
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), started.recv())
                .await
                .unwrap(),
            Some(1)
        );
        backend.release_v3_get.add_permits(1);
        assert!(
            !daemon
                .finish_prefetch_shutdown(Duration::from_secs(2))
                .await
        );
        let transfers = daemon.recent_transfers.lock().unwrap();
        let skipped = transfers
            .iter()
            .find(|event| event.cache_key == keys[0])
            .expect("claimed candidate receipt");
        assert_eq!(skipped.outcome, "skipped");
        assert!(!skipped.ok);
        assert_eq!(skipped.request_count, 0);
        assert_eq!(skipped.compressed_bytes, 0);
        assert!(skipped.accounting.as_ref().unwrap().bytes_complete);
        assert_eq!(backend.v3_gets.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn shutdown_prefetch_marks_an_unstarted_request_cancelled() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Arc::new(Daemon::new(test_config(dir.path())));
        daemon.install_plan(
            "session",
            "plan",
            "advisory",
            ["key".into()].into_iter(),
            None,
        );
        daemon.stop_prefetch_admission();
        assert!(
            daemon
                .handle_prefetch(&PrefetchRequest {
                    keys: vec![("key".into(), "serde".into())],
                    warm_all: false,
                    origin: Some(PrefetchOrigin {
                        session_id: "session".into(),
                        plan_id: "plan".into(),
                        source: "advisory".into(),
                        ..PrefetchOrigin::default()
                    }),
                    candidate_sources: HashMap::new(),
                })
                .await
                .ok
        );
        assert!(!daemon.finish_prefetch_shutdown(Duration::ZERO).await);
        let summaries = events::read_summaries(&daemon.config.summary_log_path()).unwrap();
        assert_eq!(summaries.len(), 1);
        assert!(summaries[0].cancelled);
        assert!(!summaries[0].incomplete);
        assert_eq!(summaries[0].downloaded_keys, 0);
    }

    #[tokio::test]
    async fn shutdown_prefetch_timeout_drops_and_joins_the_actual_download_child() {
        let (_dir, daemon, backend, mut started, _keys, _bytes) =
            shutdown_prefetch_fixture(2).await;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), started.recv())
                .await
                .unwrap(),
            Some(1)
        );
        assert!(
            tokio::time::timeout(
                Duration::from_secs(2),
                daemon.finish_prefetch_shutdown(Duration::from_millis(5))
            )
            .await
            .unwrap()
        );
        assert!(
            daemon.downloading.read().await.is_empty(),
            "child Drop must release its download claim before the summary"
        );
        backend.release_v3_get.add_permits(1);
        tokio::task::yield_now().await;
        assert_eq!(
            backend.release_v3_get.available_permits(),
            1,
            "no detached child may consume a released gate after shutdown"
        );
        assert_eq!(backend.v3_gets.load(Ordering::SeqCst), 1);
        let transfer = latest_transfer(&daemon);
        assert_eq!(transfer.request_count, 1);
        assert_eq!(transfer.outcome, "cancelled");
        assert_eq!(transfer.compressed_bytes, 0);
        assert!(!transfer.accounting.as_ref().unwrap().bytes_complete);
        assert!(transfer.accounting.as_ref().unwrap().requests_complete);
        let summaries = events::read_summaries(&daemon.config.summary_log_path()).unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].closure_reason, "shutdown_timeout");
        assert!(summaries[0].cancelled);
        assert!(
            summaries[0].incomplete,
            "aborted body bytes/attempts are unknown, not known zero"
        );
        assert_eq!(summaries[0].downloaded_keys, 0);
        assert!(
            daemon
                .prefetch_cancellations
                .lock()
                .unwrap()
                .origins
                .is_empty()
        );
    }

    #[tokio::test]
    async fn prefetch_receipt_keeps_received_body_when_cancelled_before_import() {
        let (_dir, daemon, backend, mut started, keys, bytes) = shutdown_prefetch_fixture(2).await;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), started.recv())
                .await
                .unwrap(),
            Some(1)
        );
        let index = daemon.key_cache.index.write().await;
        backend.release_v3_get.add_permits(1);
        tokio::time::timeout(Duration::from_secs(5), async {
            while daemon
                .prefetch_stats
                .v3_bytes_downloaded
                .load(Ordering::Relaxed)
                == 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            daemon
                .finish_prefetch_shutdown(Duration::from_millis(5))
                .await
        );
        drop(index);
        let transfer = latest_transfer(&daemon);
        assert_eq!(transfer.cache_key, keys[0]);
        assert_eq!(transfer.outcome, "cancelled");
        assert_eq!(transfer.request_count, 1);
        assert_eq!(transfer.compressed_bytes, bytes);
        assert!(transfer.accounting.as_ref().unwrap().bytes_complete);
        assert_eq!(
            daemon
                .prefetch_stats
                .v3_bytes_downloaded
                .load(Ordering::Relaxed),
            bytes
        );
        assert_eq!(
            daemon
                .prefetch_stats
                .bytes_downloaded
                .load(Ordering::Relaxed),
            bytes
        );
        let summaries = events::read_summaries(&daemon.config.summary_log_path()).unwrap();
        assert_eq!(summaries[0].downloaded_keys, 0);
        assert!(summaries[0].incomplete);
    }

    #[tokio::test]
    async fn shutdown_prefetch_finalizes_a_short_session_without_waiting_for_inactivity() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Arc::new(Daemon::new(test_config(dir.path())));
        daemon.ensure_active_session(&BuildStartedRequest {
            intent: kache_core::BuildIntent::default(),
            client_epoch: 0,
            session_id: "short-session".into(),
        });
        daemon.finalize_inactive_plan(300_000);
        assert!(!daemon.config.summary_log_path().exists());
        assert!(!daemon.finish_prefetch_shutdown(Duration::ZERO).await);
        let summaries = events::read_summaries(&daemon.config.summary_log_path()).unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].session_id, "short-session");
        assert_eq!(summaries[0].closure_reason, "shutdown");
        assert!(!summaries[0].incomplete);
        assert!(!summaries[0].cancelled);
        assert!(daemon.active_plan.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn shutdown_prefetch_marks_overflow_incomplete_without_timeout_or_matching_origin() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Arc::new(Daemon::new(test_config(dir.path())));
        daemon.install_plan("session", "plan", "advisory", std::iter::empty(), None);
        // Fill the bounded drop queue with another plan's cancellations. The
        // omitted origins are unknown, so they can still belong to this plan.
        for _ in 0..129 {
            drop(PrefetchTaskGuard {
                origin: Some(PrefetchOrigin {
                    session_id: "other-session".into(),
                    plan_id: "other-plan".into(),
                    source: "other-source".into(),
                    ..PrefetchOrigin::default()
                }),
                cancellations: daemon.prefetch_cancellations.clone(),
            });
        }
        assert!(
            !daemon
                .finish_prefetch_shutdown(Duration::from_secs(1))
                .await
        );
        let summaries = events::read_summaries(&daemon.config.summary_log_path()).unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].closure_reason, "shutdown");
        assert!(summaries[0].incomplete);
        assert!(summaries[0].cancelled);
    }

    #[tokio::test]
    async fn shutdown_prefetch_cancellation_requires_the_complete_plan_origin() {
        for (session, plan, source, matches) in [
            ("session", "plan", "advisory", true),
            ("other-session", "plan", "advisory", false),
            ("session", "other-plan", "advisory", false),
            ("session", "plan", "fallback", false),
            ("other-session", "other-plan", "fallback", false),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let daemon = Arc::new(Daemon::new(test_config(dir.path())));
            daemon.install_plan("session", "plan", "advisory", std::iter::empty(), None);
            let completion = daemon
                .spawn_prefetch_task(
                    PrefetchOrigin {
                        session_id: session.into(),
                        plan_id: plan.into(),
                        source: source.into(),
                        ..PrefetchOrigin::default()
                    },
                    async { panic!("cancel this plan's task") },
                )
                .unwrap();
            assert!(completion.await.is_err());
            assert!(
                !daemon
                    .finish_prefetch_shutdown(Duration::from_secs(1))
                    .await
            );
            let summaries = events::read_summaries(&daemon.config.summary_log_path()).unwrap();
            assert_eq!(summaries.len(), 1);
            let summary = &summaries[0];
            assert_eq!(summary.session_id, "session");
            assert_eq!(summary.plan_id, "plan");
            assert_eq!(summary.plan_source, "advisory");
            assert_eq!(summary.closure_reason, "shutdown");
            assert_eq!(summary.incomplete, matches, "{session}/{plan}/{source}");
            assert_eq!(summary.cancelled, matches, "{session}/{plan}/{source}");
        }
    }

    #[test]
    fn shutdown_prefetch_rejects_new_sessions_and_empty_session_ids() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Daemon::new(test_config(dir.path()));
        for session in ["", " \t "] {
            daemon.ensure_active_session(&BuildStartedRequest {
                intent: kache_core::BuildIntent::default(),
                client_epoch: 0,
                session_id: session.into(),
            });
            assert!(daemon.active_plan.lock().unwrap().is_none());
        }
        daemon.stop_prefetch_admission();
        daemon.ensure_active_session(&BuildStartedRequest {
            intent: kache_core::BuildIntent::default(),
            client_epoch: 0,
            session_id: "too-late".into(),
        });
        assert!(daemon.active_plan.lock().unwrap().is_none());
        assert!(!daemon.config.summary_log_path().exists());
    }

    #[tokio::test]
    async fn shutdown_prefetch_rejected_child_counts_current_and_remaining_candidates() {
        for (remaining_count, expected_total) in [(0, 14), (3, 17)] {
            let dir = tempfile::tempdir().unwrap();
            let daemon = Daemon::new(test_config(dir.path()));
            daemon
                .prefetch_stats
                .keys_cancelled
                .store(13, Ordering::Relaxed);
            daemon.stop_prefetch_admission();
            let polled = Arc::new(AtomicBool::new(false));
            let future_flag = polled.clone();
            let admission = daemon.spawn_prefetch_task(PrefetchOrigin::default(), async move {
                future_flag.store(true, Ordering::Relaxed);
            });
            assert!(admission.is_none());
            let mut remaining =
                (0..remaining_count).map(|i| (format!("key-{i}"), "serde".to_string()));
            daemon.record_rejected_prefetch_candidates(remaining.by_ref());
            assert!(
                remaining.next().is_none(),
                "rejection drains the pending candidates"
            );
            assert_eq!(
                daemon.prefetch_stats.keys_cancelled.load(Ordering::Relaxed),
                expected_total
            );
            assert!(!polled.load(Ordering::Relaxed));
        }
    }

    #[tokio::test]
    async fn shutdown_prefetch_adaptive_cancel_rejects_queued_keys_before_shutdown() {
        let (_dir, daemon, backend, mut started, keys, _bytes) = shutdown_prefetch_fixture(3).await;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), started.recv())
                .await
                .unwrap(),
            Some(1)
        );
        assert!(!daemon.prefetch_stopping.load(Ordering::Acquire));
        daemon.prefetch_cancel.send(true).unwrap();
        // Candidate two passed the guard before waiting for candidate one.
        // Both may finish; candidate three must observe the adaptive latch.
        // A wrongly admitted third GET can finish and expose its request.
        backend.release_v3_get.add_permits(keys.len());
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let pending = {
                    let mut tasks = daemon.prefetch_tasks.lock().unwrap();
                    while tasks.try_join_next().is_some() {}
                    !tasks.is_empty()
                };
                if !pending {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("prefetch coordinator and children must finish");
        assert_eq!(backend.v3_gets.load(Ordering::SeqCst), 2);
        assert_eq!(
            daemon.prefetch_stats.keys_cancelled.load(Ordering::Relaxed),
            1
        );
        assert!(daemon.entry_dir_for(&keys[1]).exists());
        assert!(!daemon.entry_dir_for(&keys[2]).exists());
        assert!(!daemon.prefetch_stopping.load(Ordering::Acquire));
        assert!(
            !daemon
                .finish_prefetch_shutdown(Duration::from_secs(1))
                .await
        );
        let summaries = events::read_summaries(&daemon.config.summary_log_path()).unwrap();
        assert_eq!(summaries.len(), 1);
        assert!(summaries[0].cancelled);
        assert!(!summaries[0].incomplete);
    }

    #[tokio::test]
    async fn shutdown_prefetch_inactivity_emits_once_then_leaves_shutdown_to_drain() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Arc::new(Daemon::new(test_config(dir.path())));
        daemon.install_plan("idle-session", "plan", "advisory", std::iter::empty(), None);
        daemon.finalize_inactive_plan(u64::MAX);
        assert!(!daemon.config.summary_log_path().exists());
        daemon
            .active_plan
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .last_activity_ms = 0;
        daemon.finalize_inactive_plan(1);
        assert!(daemon.active_plan.lock().unwrap().is_none());
        let summaries = events::read_summaries(&daemon.config.summary_log_path()).unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].session_id, "idle-session");
        assert_eq!(summaries[0].closure_reason, "inactivity");
        daemon.finalize_inactive_plan(0);
        daemon.install_plan(
            "last-session",
            "last-plan",
            "none",
            std::iter::empty(),
            None,
        );
        daemon.stop_prefetch_admission();
        daemon.finalize_inactive_plan(0);
        assert!(daemon.active_plan.lock().unwrap().is_some());
        assert_eq!(
            events::read_summaries(&daemon.config.summary_log_path())
                .unwrap()
                .len(),
            1
        );
        assert!(
            !daemon
                .finish_prefetch_shutdown(Duration::from_secs(1))
                .await
        );
        let summaries = events::read_summaries(&daemon.config.summary_log_path()).unwrap();
        assert_eq!(summaries.len(), 2);
        assert_eq!(summaries[1].session_id, "last-session");
        assert_eq!(summaries[1].closure_reason, "shutdown");
    }

    #[tokio::test]
    async fn shutdown_prefetch_registry_reaps_tasks_and_releases_panicked_completion() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Arc::new(Daemon::new(test_config(dir.path())));
        let origin = PrefetchOrigin::default();
        let panic = daemon
            .spawn_prefetch_task(origin.clone(), async { panic!("test child panic") })
            .unwrap();
        assert!(panic.await.is_err());
        for _ in 0..4 {
            daemon
                .spawn_prefetch_task(origin.clone(), async {})
                .unwrap()
                .await
                .unwrap();
        }
        assert_eq!(
            daemon.prefetch_tasks.lock().unwrap().len(),
            1,
            "completed registrations must be reaped on admission"
        );
        daemon.stop_prefetch_admission();
        let polled = Arc::new(AtomicBool::new(false));
        let future_flag = polled.clone();
        assert!(
            daemon
                .spawn_prefetch_task(origin, async move {
                    future_flag.store(true, Ordering::Relaxed);
                })
                .is_none()
        );
        assert!(!daemon.finish_prefetch_shutdown(Duration::ZERO).await);
        assert!(!polled.load(Ordering::Relaxed));
        assert!(daemon.prefetch_tasks.lock().unwrap().is_empty());
    }

    #[test]
    fn shutdown_prefetch_drop_records_are_bounded_and_only_record_cancelled_tasks() {
        let cancellations = Arc::new(Mutex::new(PrefetchCancellations::default()));
        drop(PrefetchTaskGuard {
            origin: None,
            cancellations: cancellations.clone(),
        });
        assert!(cancellations.lock().unwrap().origins.is_empty());
        for _ in 0..129 {
            drop(PrefetchTaskGuard {
                origin: Some(PrefetchOrigin::default()),
                cancellations: cancellations.clone(),
            });
        }
        let queue = cancellations.lock().unwrap();
        assert_eq!(queue.origins.len(), 128);
        assert!(queue.overflowed);
    }

    #[tokio::test]
    async fn prefetch_not_found_keeps_its_origin_and_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let key = test_cache_key("prefetch-not-found");
        let daemon = Arc::new(Daemon::new(config));
        assert!(daemon.remote_backend.set(test_remote_backend()).is_ok());
        let origin = PrefetchOrigin {
            session_id: "session-original".to_string(),
            plan_id: "plan-original".to_string(),
            source: "advisory".to_string(),
            candidate_rank: None,
            candidate_source: kache_core::CandidateSource::Unknown,
        };
        let response = daemon
            .handle_prefetch(&PrefetchRequest {
                keys: vec![(key.clone(), "serde".to_string())],
                warm_all: false,
                origin: Some(origin.clone()),
                candidate_sources: HashMap::from([(
                    key.clone(),
                    kache_core::CandidateSource::Manifest,
                )]),
            })
            .await;
        assert!(response.ok);
        // Superseding the active session must not relabel an already queued task.
        daemon.install_plan(
            "session-next",
            "plan-next",
            "fallback",
            std::iter::once(key.clone()),
            None,
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            while daemon.recent_transfers.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("404 must produce a transfer record");
        let transfer = latest_transfer(&daemon);
        let mut expected = origin;
        expected.candidate_rank = Some(0);
        expected.candidate_source = kache_core::CandidateSource::Manifest;
        assert_eq!(transfer.prefetch, Some(expected));
        assert_eq!(transfer.outcome, "not_found");
        assert!(!transfer.ok);
        assert_eq!(transfer.request_count, 1);
        assert_eq!(transfer.compressed_bytes, 0);
        assert!(transfer.accounting.as_ref().unwrap().bytes_complete);
        assert!(transfer.accounting.as_ref().unwrap().requests_complete);
        assert_eq!(
            daemon
                .transfer_counters
                .downloads_failed
                .load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn prefetch_import_failure_is_counted_as_failure() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let key = test_cache_key("prefetch-import-failure");
        let pack = build_entry_pack(&key, "serde");
        let entry_dir = config.store_dir().join(&key);
        std::fs::create_dir_all(config.store_dir()).unwrap();
        std::fs::write(config.store_dir().join("blobs"), b"not a directory").unwrap();

        let client = test_remote_backend();
        put_test_object(&client, &test_pack_object_key(&key, "serde"), &pack).await;
        let daemon = Arc::new(Daemon::new(config.clone()));
        assert!(daemon.remote_backend.set(client).is_ok());
        daemon.install_plan(
            "test-session",
            "test-plan",
            "test",
            std::iter::once(key.clone()),
            None,
        );

        let response = daemon
            .handle_prefetch(&PrefetchRequest {
                keys: vec![(key.clone(), "serde".into())],
                warm_all: false,
                origin: None,
                candidate_sources: HashMap::new(),
            })
            .await;
        assert!(
            response.ok,
            "prefetch dispatch should remain fire-and-forget"
        );

        let completed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let failed = daemon
                    .transfer_counters
                    .downloads_failed
                    .load(Ordering::Relaxed);
                let has_event = daemon
                    .recent_transfers
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .back()
                    .is_some();
                if failed == 1 && has_event {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            completed.is_ok(),
            "prefetch import failure was not recorded"
        );

        assert_eq!(
            daemon
                .transfer_counters
                .downloads_completed
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            daemon
                .prefetch_stats
                .downloads_completed
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            daemon
                .transfer_counters
                .bytes_downloaded
                .load(Ordering::Relaxed),
            pack.len() as u64,
            "a local import failure must not hide bytes already transferred"
        );
        assert_eq!(
            daemon
                .prefetch_stats
                .bytes_downloaded
                .load(Ordering::Relaxed),
            pack.len() as u64
        );
        let transfer = latest_transfer(&daemon);
        assert!(!transfer.ok);
        assert_eq!(transfer.outcome, "import_error");
        assert_eq!(transfer.compressed_bytes, pack.len() as u64);
        {
            let active_plan = daemon
                .active_plan
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            assert!(
                active_plan
                    .as_ref()
                    .expect("test plan should remain active")
                    .downloaded
                    .is_empty(),
                "a failed local import must not be attributed as a plan download"
            );
        }
        assert!(!daemon.prefetched_keys.read().await.contains(&key));
        assert!(!entry_dir.exists());
        assert!(!Store::open(&config).unwrap().contains(&key));
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
        let daemon = Arc::new(Daemon::new(config));
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
        assert!(daemon.finish_prefetch_receipts().await);
        let transfer = latest_transfer(&daemon);
        assert_eq!(
            transfer.accounting.as_ref().unwrap().operation,
            PrefetchOperation::List
        );
        assert_eq!(
            transfer.accounting.as_ref().unwrap().list_result_count,
            Some(2)
        );
        assert!(!transfer.accounting.as_ref().unwrap().bytes_complete);
        assert_eq!(transfer.request_count, 1);
        assert_eq!(transfer.outcome, "completed");
        assert_eq!(transfer.prefetch.as_ref().unwrap().source, "unscoped");
    }

    #[tokio::test]
    async fn index_list_receipt_snapshots_origin_and_counts_only_backend_admission() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        config.s3_concurrency = 1;
        let daemon = Arc::new(Daemon::new(config));
        assert!(daemon.remote_backend.set(test_remote_backend()).is_ok());
        daemon.install_plan(
            "old-session",
            "old-plan",
            "advisory",
            std::iter::empty(),
            None,
        );
        let permit = daemon.s3_semaphore.acquire().await.unwrap();
        let listing = populate_key_cache(&daemon);
        tokio::pin!(listing);
        assert!(futures::poll!(listing.as_mut()).is_pending());
        assert_eq!(daemon.prefetch_receipts.lock().unwrap().active_receipts, 1);
        assert!(daemon.prefetch_receipts.lock().unwrap().events.is_empty());
        assert!(daemon.prefetch_accounting_incomplete());
        assert!(!daemon.finish_prefetch_receipts().await);
        assert_eq!(
            daemon
                .prefetch_stats
                .list_requests_total
                .load(Ordering::Relaxed),
            0
        );
        daemon.install_plan(
            "new-session",
            "new-plan",
            "fallback",
            std::iter::empty(),
            None,
        );
        assert!(events::read_summaries(&daemon.config.summary_log_path()).unwrap()[0].incomplete);
        drop(permit);
        assert_eq!(listing.await.unwrap(), 0);
        assert!(daemon.finish_prefetch_receipts().await);
        let transfer = latest_transfer(&daemon);
        assert_eq!(
            transfer.prefetch.as_ref().unwrap().session_id,
            "old-session"
        );
        assert_eq!(transfer.prefetch.as_ref().unwrap().plan_id, "old-plan");
        assert_eq!(transfer.prefetch.as_ref().unwrap().source, "advisory");
        assert_eq!(transfer.request_count, 1);
        assert_eq!(
            transfer.accounting.as_ref().unwrap().list_result_count,
            Some(0)
        );
        assert_eq!(
            daemon
                .prefetch_stats
                .list_requests_total
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(daemon.prefetch_receipts.lock().unwrap().active_receipts, 0);
        assert!(!daemon.finish_prefetch_shutdown(Duration::ZERO).await);
        let summaries = events::read_summaries(&daemon.config.summary_log_path()).unwrap();
        assert_eq!(summaries.len(), 2);
        assert_eq!(summaries[1].closure_reason, "shutdown");
        assert!(!summaries[1].incomplete);
    }

    #[tokio::test]
    async fn index_list_receipt_cancelled_in_queue_has_no_physical_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        config.s3_concurrency = 1;
        let daemon = Arc::new(Daemon::new(config));
        assert!(daemon.remote_backend.set(test_remote_backend()).is_ok());
        let permit = daemon.s3_semaphore.acquire().await.unwrap();
        let mut listing = Box::pin(populate_key_cache(&daemon));
        assert!(futures::poll!(listing.as_mut()).is_pending());
        drop(listing);
        drop(permit);
        assert_eq!(daemon.prefetch_receipts.lock().unwrap().active_receipts, 0);
        assert!(daemon.finish_prefetch_receipts().await);
        let transfer = latest_transfer(&daemon);
        assert_eq!(transfer.request_count, 0);
        assert_eq!(transfer.outcome, "cancelled");
        assert_eq!(
            transfer.accounting.as_ref().unwrap().list_result_count,
            None
        );
        assert!(!transfer.accounting.as_ref().unwrap().bytes_complete);
        assert_eq!(
            daemon
                .prefetch_stats
                .list_requests_total
                .load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn index_list_receipt_keeps_backend_completion_when_bookkeeping_is_cancelled() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let backend = test_remote_backend();
        put_test_object(
            &backend,
            &test_manifest_object_key(&test_cache_key("listed"), "serde"),
            b"{}",
        )
        .await;
        let daemon = Arc::new(Daemon::new(config));
        assert!(daemon.remote_backend.set(backend).is_ok());
        let index = daemon.key_cache.index.write().await;
        let producer = tokio::spawn({
            let daemon = daemon.clone();
            async move { populate_key_cache(&daemon).await }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while daemon
                .prefetch_stats
                .list_keys_total
                .load(Ordering::Relaxed)
                == 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let returned_at = unix_time_ms();
        assert_eq!(daemon.prefetch_receipts.lock().unwrap().active_receipts, 1);
        assert!(!producer.is_finished());
        tokio::time::sleep(Duration::from_millis(20)).await;
        producer.abort();
        assert!(producer.await.unwrap_err().is_cancelled());
        drop(index);
        assert!(daemon.finish_prefetch_receipts().await);
        let transfer = latest_transfer(&daemon);
        assert_eq!(transfer.outcome, "completed");
        assert!(transfer.ok);
        assert_eq!(transfer.request_count, 1);
        assert!(transfer.finished_at_unix_ms <= returned_at);
        assert_eq!(
            transfer.accounting.as_ref().unwrap().list_result_count,
            Some(1)
        );
        assert!(!transfer.accounting.as_ref().unwrap().bytes_complete);
    }

    #[tokio::test]
    async fn test_monolithic_manifest_prefetch_downloads_and_filters() {
        // Serve a build manifest whose single entry is below the prefetch cost
        // threshold, so identity_manifest_prefetch downloads + parses it, then
        // skips the cheap crate (no prefetch queued). Covers download_manifest +
        // the cost-benefit filter path.
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        let remote = test_remote_config();
        config.remote = Some(remote.clone());

        let manifest = crate::remote::BuildManifest {
            version: 3,
            created: "2025-01-01T00:00:00Z".to_string(),
            manifest_key: crate::identity::host_target_triple(),
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
        assert!(daemon.remote_backend.set(client).is_ok());

        // Should complete without panicking and without queuing the cheap crate.
        assert_eq!(identity_manifest_prefetch(&daemon, None).await, 0);
    }

    #[tokio::test]
    async fn test_manifest_prefetch_skips_when_no_manifest() {
        // The mock 404s the manifest GET, so identity lookup finds nothing
        // and shard fallback has no lockfile. Covers the
        // "no manifest, skipping" arm (daemon.rs 2957-2960).
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());

        let client = test_remote_backend();
        let daemon = Arc::new(Daemon::new(config));
        assert!(daemon.remote_backend.set(client).is_ok());

        let queued = manifest_prefetch(&daemon, None, &dir.path().join("no-cargo-lock")).await;
        assert_eq!(queued, 0, "a missing manifest must queue no entries");
    }

    #[tokio::test]
    async fn test_manifest_prefetch_dispatches_expensive_entries() {
        // A manifest with an entry above the cost threshold is kept, so the
        // function builds prefetch keys and dispatches handle_prefetch. Covers
        // the worth-prefetching dispatch path (daemon.rs 2986-2994).
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());

        // handle_prefetch validates the key as 64 hex chars.
        let key = "abcdef0123456789".repeat(4);
        let manifest = crate::remote::BuildManifest {
            version: 3,
            created: "2025-01-01T00:00:00Z".to_string(),
            manifest_key: crate::identity::host_target_triple(),
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
            daemon.remote_backend.set(client).is_ok(),
            "inject mock backend"
        );

        let queued = manifest_prefetch(&daemon, None, &dir.path().join("no-cargo-lock")).await;
        assert_eq!(queued, 1, "the expensive manifest entry must be dispatched");
    }

    #[tokio::test]
    async fn test_shard_prefetch_all_shards_missing_returns_zero() {
        // A Cargo.lock with two deps -> compute_shards -> one shard GET per
        // shard. The empty memory backend has no shard objects, so none match
        // and the prefetch queues nothing (Ok(0)). Covers shard computation + parallel
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
        assert!(daemon.remote_backend.set(client).is_ok());
        let v3 = daemon.v3_remote().await.expect("v3 remote");

        let count = shard_prefetch(&daemon, v3, "ns", &lock)
            .await
            .expect("shard prefetch should succeed");
        assert_eq!(count, 0, "no shards matched -> nothing queued");
    }

    /// Seed one local store entry per dep and a remote shard object listing
    /// those entries under `prefix`/`namespace`. Returns the memory backend
    /// and the number of shard entries seeded.
    async fn seed_prefetch_shards(
        config: &Config,
        dir: &Path,
        namespace: &str,
        deps: &[(String, String)],
    ) -> (Arc<dyn crate::remote_backend::RemoteBackend>, usize) {
        let shard_set = crate::shards::compute_shards(namespace, deps);
        assert!(
            shard_set.shards.len() >= 2,
            "test deps must span at least two shards"
        );
        let client = test_remote_backend();
        let mut seeded = 0;
        for (hash, entries) in &shard_set.shards {
            let mut shard = crate::remote::Shard {
                version: 3,
                entries: Vec::new(),
            };
            for (name, version) in entries {
                let key = test_cache_key(&format!("seeded-shard-prefetch-{name}-{version}"));
                seed_store_entry(config, &key, name, dir);
                shard.entries.push(crate::remote::ShardEntry {
                    cache_key: key,
                    crate_name: name.clone(),
                    compile_time_ms: Some(5000),
                    artifact_size: Some(100),
                });
                seeded += 1;
            }
            put_test_object(
                &client,
                &crate::remote::shard_object_key("prefix", namespace, hash),
                &serde_json::to_vec(&shard).unwrap(),
            )
            .await;
        }
        (client, seeded)
    }

    #[tokio::test]
    async fn shard_prefetch_for_deps_returns_seeded_shard_entries() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let deps = vec![
            ("serde".to_string(), "1.0.0".to_string()),
            ("tokio".to_string(), "1.0.0".to_string()),
            ("anyhow".to_string(), "1.0.0".to_string()),
        ];
        let (client, seeded) = seed_prefetch_shards(&config, dir.path(), "workspace", &deps).await;
        assert_eq!(seeded, 3);
        let daemon = Arc::new(Daemon::new(config));
        assert!(daemon.remote_backend.set(client).is_ok());

        let v3 = daemon.v3_remote().await.expect("v3 remote");
        let queued = shard_prefetch_for_deps(&daemon, v3, "workspace", &deps)
            .await
            .expect("seeded shard prefetch");
        assert_eq!(queued, 3, "one queued key per seeded shard entry");
    }

    #[tokio::test]
    async fn shard_prefetch_reads_cargo_lock_and_returns_seeded_shard_entries() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let lock = dir.path().join("Cargo.lock");
        std::fs::write(
            &lock,
            "version = 3\n\n[[package]]\nname = \"serde\"\nversion = \"1.0.0\"\n\n\
             [[package]]\nname = \"tokio\"\nversion = \"1.0.0\"\n\n\
             [[package]]\nname = \"anyhow\"\nversion = \"1.0.0\"\n",
        )
        .unwrap();
        let deps = crate::shards::parse_cargo_lock(&lock).unwrap();
        assert_eq!(deps.len(), 3);
        let (client, seeded) = seed_prefetch_shards(&config, dir.path(), "ns", &deps).await;
        assert_eq!(seeded, 3);
        let daemon = Arc::new(Daemon::new(config));
        assert!(daemon.remote_backend.set(client).is_ok());

        let v3 = daemon.v3_remote().await.expect("v3 remote");
        let count = shard_prefetch(&daemon, v3, "ns", &lock)
            .await
            .expect("seeded shard prefetch from Cargo.lock");
        assert_eq!(count, 3, "one queued key per Cargo.lock package");
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
                    shard_dir: None,
                },
                RemoteCheckRequest {
                    key: "key2".into(),
                    entry_dir: "/tmp/key2".into(),
                    crate_name: String::new(),
                    deadline_ms: None,
                    shard_dir: None,
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
            origin: None,
            candidate_sources: HashMap::new(),
        });
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(req, parsed);

        assert!(json.contains("\"prefetch\""));
        assert!(json.contains("\"key_a\""));
    }

    #[test]
    fn prefetch_origin_cannot_be_supplied_over_ipc() {
        let request: PrefetchRequest = serde_json::from_value(serde_json::json!({
            "keys": [], "origin": {"session_id": "other", "source": "advisory"},
            "candidate_sources": {"key": "manifest"}
        }))
        .unwrap();
        assert!(request.origin.is_none());
        assert!(request.candidate_sources.is_empty());
    }

    #[test]
    fn prefetch_candidate_source_uses_the_first_valid_candidate() {
        let key = "b".repeat(64);
        let invalid_key =
            kache_core::PrefetchCandidate::new("not-a-cache-key".into(), "serde".into());
        let mut invalid = kache_core::PrefetchCandidate::new(key.clone(), "../evil".into());
        invalid.source = kache_core::CandidateSource::Shard;
        let mut first = kache_core::PrefetchCandidate::new(key.clone(), "serde".into());
        first.source = kache_core::CandidateSource::Manifest;
        let mut duplicate = first.clone();
        duplicate.source = kache_core::CandidateSource::History;
        let request = PrefetchRequest::from_plan(PrefetchPlan {
            plan_id: None,
            planner: None,
            disposition: PrefetchDisposition::Execute,
            candidates: vec![invalid_key, invalid, first, duplicate],
        });
        assert_eq!(
            request.keys,
            vec![(key.clone(), "serde".into()), (key.clone(), "serde".into())]
        );
        assert_eq!(
            request.candidate_sources,
            HashMap::from([(key, kache_core::CandidateSource::Manifest)])
        );
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
            origin: None,
            candidate_sources: HashMap::new(),
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
            shard_dir: None,
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
            shard_dir: None,
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
            shard_dir: None,
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
                &Arc::new(Lifecycle::default()),
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
            origin: None,
            candidate_sources: HashMap::new(),
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
                origin: None,
                candidate_sources: HashMap::new(),
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
                origin: None,
                candidate_sources: HashMap::new(),
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
                origin: None,
                candidate_sources: HashMap::new(),
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
                origin: None,
                candidate_sources: HashMap::new(),
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
                origin: None,
                candidate_sources: HashMap::new(),
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
                shard_dir: None,
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
    fn upload_spool_policy_helpers_cover_boundaries_and_error_kinds() {
        let not_found = std::io::Error::new(std::io::ErrorKind::NotFound, "missing");
        let denied = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let exists = std::io::Error::new(std::io::ErrorKind::AlreadyExists, "exists");
        assert!(upload_spool_error_is_not_found(&not_found));
        assert!(!upload_spool_error_is_not_found(&denied));
        assert!(upload_spool_error_is_already_exists(&exists));
        assert!(!upload_spool_error_is_already_exists(&denied));

        assert!(upload_intent_size_is_valid(UPLOAD_SPOOL_MAX_BYTES - 1));
        assert!(upload_intent_size_is_valid(UPLOAD_SPOOL_MAX_BYTES));
        assert!(!upload_intent_size_is_valid(UPLOAD_SPOOL_MAX_BYTES + 1));
        assert!(upload_spool_has_capacity(UPLOAD_SPOOL_MAX_JOBS - 1));
        assert!(!upload_spool_has_capacity(UPLOAD_SPOOL_MAX_JOBS));

        let count =
            count_upload_spool_entries([Ok::<_, std::io::Error>(()), Ok::<_, std::io::Error>(())])
                .unwrap();
        assert_eq!(count, 2);
        let count_error = count_upload_spool_entries([Err::<(), _>(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "injected unreadable entry",
        ))])
        .unwrap_err();
        assert!(format!("{count_error:#}").contains("injected unreadable entry"));
    }

    #[test]
    fn upload_spool_paths_and_normalization_are_config_derived() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let key = test_cache_key("spool-path");
        assert_eq!(config.upload_spool_dir(), dir.path().join("upload-queue"));
        assert_eq!(
            upload_spool_path(&config, &key),
            dir.path().join("upload-queue").join(format!("{key}.json"))
        );

        let normalized = normalize_upload_job(
            &config,
            &UploadJob {
                key: key.clone(),
                entry_dir: "/untrusted/client/path".into(),
                crate_name: "serde".into(),
                client_epoch: 17,
            },
        )
        .unwrap();
        assert_eq!(normalized.key, key);
        assert_eq!(
            Path::new(&normalized.entry_dir),
            config.store_dir().join(&normalized.key)
        );
        assert_eq!(normalized.crate_name, "serde");
        assert_eq!(normalized.client_epoch, 17);
    }

    #[test]
    fn upload_job_normalization_rejects_each_untrusted_component() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let invalid_key = normalize_upload_job(
            &config,
            &UploadJob {
                key: "../escape".into(),
                entry_dir: "/ignored".into(),
                crate_name: "serde".into(),
                client_epoch: 0,
            },
        )
        .unwrap_err();
        assert!(invalid_key.to_string().contains("invalid upload cache key"));

        let invalid_crate = normalize_upload_job(
            &config,
            &UploadJob {
                key: test_cache_key("invalid-crate"),
                entry_dir: "/ignored".into(),
                crate_name: "../serde".into(),
                client_epoch: 0,
            },
        )
        .unwrap_err();
        assert!(
            invalid_crate
                .to_string()
                .contains("invalid upload crate name")
        );
    }

    #[test]
    fn existing_upload_intent_accepts_the_exact_size_limit_only() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let key = test_cache_key("exact-size-intent");
        fs::create_dir_all(config.upload_spool_dir()).unwrap();
        assert!(existing_upload_job(&config, &key).unwrap().is_none());

        let path = upload_spool_path(&config, &key);
        fs::create_dir(&path).unwrap();
        let non_file = existing_upload_job(&config, &key).unwrap_err();
        assert!(non_file.to_string().contains("not a regular file"));
        fs::remove_dir(&path).unwrap();

        let job = UploadJob {
            key: key.clone(),
            entry_dir: "/hostile/serialized/path".into(),
            crate_name: "serde".into(),
            client_epoch: 23,
        };
        let mut exact = serde_json::to_vec(&job).unwrap();
        assert!(exact.len() < UPLOAD_SPOOL_MAX_BYTES as usize);
        exact.resize(UPLOAD_SPOOL_MAX_BYTES as usize, b' ');
        fs::write(&path, &exact).unwrap();

        let loaded = existing_upload_job(&config, &key).unwrap().unwrap();
        assert_eq!(loaded.key, key);
        assert_eq!(
            Path::new(&loaded.entry_dir),
            config.store_dir().join(&loaded.key)
        );

        exact.push(b' ');
        fs::write(&path, exact).unwrap();
        let oversized = existing_upload_job(&config, &key).unwrap_err();
        assert!(oversized.to_string().contains("upload intent exceeds"));
    }

    #[test]
    fn create_only_upload_publication_preserves_the_first_winner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("intent.json");
        assert!(publish_upload_job_create_only(&path, b"first").unwrap());
        assert!(!publish_upload_job_create_only(&path, b"second").unwrap());
        assert_eq!(fs::read(path).unwrap(), b"first");
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

        let error = ensure_upload_spool_dir_with(
            &spool,
            |path| std::fs::create_dir_all(path),
            |_| {
                Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "injected upload-spool parent fsync failure",
                ))
            },
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("injected upload-spool parent fsync failure"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn upload_spool_directory_creation_failure_is_propagated_before_sync() {
        let dir = tempfile::tempdir().unwrap();
        let spool = dir.path().join("upload-queue");
        let error = ensure_upload_spool_dir_with(
            &spool,
            |_| {
                Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "injected upload-spool creation failure",
                ))
            },
            |_| panic!("sync must not run after creation fails"),
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("injected upload-spool creation failure"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn upload_intent_removal_is_idempotent_but_propagates_other_errors() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let key = test_cache_key("remove-upload-intent");
        fs::create_dir_all(config.upload_spool_dir()).unwrap();

        remove_upload_job(&config, &key).expect("a missing intent is already removed");

        let path = upload_spool_path(&config, &key);
        fs::create_dir(&path).unwrap();
        let error = remove_upload_job(&config, &key).unwrap_err();
        assert!(format!("{error:#}").contains("removing"));
        assert!(path.is_dir(), "a failed removal must not hide the obstacle");
    }

    #[test]
    fn upload_intent_loading_distinguishes_missing_from_unreadable_spools() {
        let missing_dir = tempfile::tempdir().unwrap();
        let missing_config = test_config(missing_dir.path());
        assert!(load_upload_jobs(&missing_config).unwrap().is_empty());

        let blocked_dir = tempfile::tempdir().unwrap();
        let blocked_config = test_config(blocked_dir.path());
        fs::write(blocked_config.upload_spool_dir(), b"not a directory").unwrap();
        let error = load_upload_jobs(&blocked_config).unwrap_err();
        assert!(format!("{error:#}").contains("reading"));
    }

    #[test]
    fn upload_intent_loading_filters_each_invalid_shape_and_normalizes_paths() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let spool = config.upload_spool_dir();
        fs::create_dir_all(&spool).unwrap();

        let exact_key = test_cache_key("load-exact-size");
        let exact_job = UploadJob {
            key: exact_key.clone(),
            entry_dir: "/hostile/replayed/path".into(),
            crate_name: "serde".into(),
            client_epoch: 31,
        };
        let mut exact_bytes = serde_json::to_vec(&exact_job).unwrap();
        exact_bytes.resize(UPLOAD_SPOOL_MAX_BYTES as usize, b' ');
        fs::write(upload_spool_path(&config, &exact_key), exact_bytes).unwrap();

        let oversized_key = test_cache_key("load-oversized");
        let oversized_job = UploadJob {
            key: oversized_key.clone(),
            entry_dir: "/ignored".into(),
            crate_name: "serde".into(),
            client_epoch: 0,
        };
        let mut oversized_bytes = serde_json::to_vec(&oversized_job).unwrap();
        oversized_bytes.resize(UPLOAD_SPOOL_MAX_BYTES as usize + 1, b' ');
        fs::write(upload_spool_path(&config, &oversized_key), oversized_bytes).unwrap();

        let directory_key = test_cache_key("load-directory");
        fs::create_dir(upload_spool_path(&config, &directory_key)).unwrap();

        let mismatched_file_key = test_cache_key("load-mismatched-file");
        let mismatched_job = UploadJob {
            key: test_cache_key("load-mismatched-payload"),
            entry_dir: "/ignored".into(),
            crate_name: "serde".into(),
            client_epoch: 0,
        };
        fs::write(
            upload_spool_path(&config, &mismatched_file_key),
            serde_json::to_vec(&mismatched_job).unwrap(),
        )
        .unwrap();

        let invalid_crate_key = test_cache_key("load-invalid-crate");
        let invalid_crate_job = UploadJob {
            key: invalid_crate_key.clone(),
            entry_dir: "/ignored".into(),
            crate_name: "../serde".into(),
            client_epoch: 0,
        };
        fs::write(
            upload_spool_path(&config, &invalid_crate_key),
            serde_json::to_vec(&invalid_crate_job).unwrap(),
        )
        .unwrap();

        let jobs = load_upload_jobs(&config).unwrap();
        assert_eq!(jobs.len(), 1, "only the exact-limit valid job may replay");
        let loaded = &jobs[0];
        assert_eq!(loaded.key, exact_key);
        assert_eq!(loaded.crate_name, "serde");
        assert_eq!(loaded.client_epoch, 31);
        assert_eq!(
            Path::new(&loaded.entry_dir),
            config.store_dir().join(&loaded.key),
            "serialized entry_dir must never be trusted"
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

    #[tokio::test]
    async fn upload_pipeline_drain_deadline_includes_a_blocked_enqueue_task() {
        let job = UploadJob {
            key: test_cache_key("blocked-shutdown-enqueue"),
            entry_dir: "/unused".into(),
            crate_name: "serde".into(),
            client_epoch: 0,
        };
        let (worker_tx, _worker_rx) = tokio::sync::mpsc::channel::<UploadJob>(1);
        worker_tx.send(job.clone()).await.unwrap();
        let (buffer_tx, mut buffer_rx) = tokio::sync::mpsc::unbounded_channel::<UploadJob>();
        buffer_tx.send(job).unwrap();
        drop(buffer_tx);

        let enqueue_handle = tokio::spawn(async move {
            while let Some(job) = buffer_rx.recv().await {
                if worker_tx.send(job).await.is_err() {
                    break;
                }
            }
        });

        let timed_out = tokio::time::timeout(
            Duration::from_secs(1),
            drain_upload_pipeline(enqueue_handle, Vec::new(), Duration::from_millis(10)),
        )
        .await
        .expect("the outer guard must not expire");
        assert!(
            timed_out,
            "a full, non-draining worker channel must consume the shared drain deadline"
        );
    }

    #[tokio::test]
    async fn upload_pipeline_drain_reports_clean_completion() {
        let enqueue = tokio::spawn(async {});
        let workers = vec![tokio::spawn(async {}), tokio::spawn(async {})];
        let timed_out = tokio::time::timeout(
            Duration::from_secs(1),
            drain_upload_pipeline(enqueue, workers, Duration::from_millis(100)),
        )
        .await
        .expect("completed tasks must drain promptly");
        assert!(!timed_out);
    }

    #[tokio::test]
    async fn upload_pipeline_drain_deadline_includes_workers() {
        let enqueue = tokio::spawn(async {});
        let worker = tokio::spawn(std::future::pending::<()>());
        let timed_out = tokio::time::timeout(
            Duration::from_secs(1),
            drain_upload_pipeline(enqueue, vec![worker], Duration::from_millis(10)),
        )
        .await
        .expect("the outer guard must not expire");
        assert!(
            timed_out,
            "a pending worker must consume the shared deadline"
        );
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
                origin: None,
                candidate_sources: HashMap::new(),
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
                identity_key: None,
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
            let _ = handle_connection(stream, &daemon, &Arc::new(Lifecycle::default())).await;
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
            let _ = handle_connection(stream, &daemon, &Arc::new(Lifecycle::default())).await;
        });

        let cfg = config.clone();
        let result = tokio::task::spawn_blocking(move || {
            send_upload_job(&cfg, &key, Path::new("/tmp/test"), "serde")
        })
        .await
        .unwrap();
        assert!(result.is_ok(), "upload job should send to a live daemon");
        assert_eq!(
            load_upload_jobs(&config).unwrap().len(),
            1,
            "the client must durably publish the upload before sending"
        );
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("the upload request must reach the live daemon")
            .unwrap();
    }

    #[tokio::test]
    async fn test_send_upload_job_client_roundtrip_for_cc_object() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let key = test_cache_key("cc-object-upload");
        seed_cc_store_entry(&config, &key, "foo.c", dir.path());
        let socket_path = config.socket_path();
        std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();

        let listener = bind_listener(&socket_path);
        let daemon = Arc::new(Daemon::new(config.clone()));
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<UploadJob>();
        daemon.set_upload_tx(tx);
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept");
            let _ = handle_connection(stream, &daemon, &Arc::new(Lifecycle::default())).await;
        });

        let cfg = config.clone();
        let result = tokio::task::spawn_blocking(move || {
            send_upload_job(&cfg, &key, Path::new("/tmp/test"), "foo.c")
        })
        .await
        .unwrap();
        assert!(
            result.is_ok(),
            "a C object upload job should send to a live daemon: {result:?}"
        );
        let jobs = load_upload_jobs(&config).unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].crate_name, "foo.c");
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("the cc upload request must reach the live daemon")
            .unwrap();
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
        let plan = daemon.active_plan.lock().unwrap();
        let plan = plan
            .as_ref()
            .expect("disabled prefetch must still track the build session");
        assert_eq!(plan.session_id, "disabled-prefetch");
        assert!(plan.candidates.is_empty());
        assert_eq!(
            daemon.prefetch_stats.plans_advisory.load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            daemon.prefetch_stats.plans_fallback.load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn do_nothing_cancels_identity_resolution_started_with_planner_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let backend = Arc::new(BlockingIdentityBackend {
            inner: test_remote_backend(),
            identity_gets: AtomicU64::new(0),
            identity_cancellations: AtomicU64::new(0),
            artifact_gets: AtomicU64::new(0),
            artifact_started_before_identity_cancel: AtomicBool::new(false),
            identity_started: Notify::new(),
            block_identity: AtomicBool::new(true),
        });
        let daemon = Arc::new(Daemon::new(config));
        daemon.set_remote_backend_for_test(backend.clone());

        let planner_backend = backend.clone();
        let planner_lookup = async move {
            tokio::time::timeout(
                Duration::from_secs(1),
                planner_backend.identity_started.notified(),
            )
            .await
            .expect("identity metadata lookup must start before the planner returns");
            Ok(Some(PrefetchPlan {
                plan_id: Some("no-prefetch".into()),
                planner: Some("test".into()),
                disposition: PrefetchDisposition::DoNothing,
                candidates: Vec::new(),
            }))
        };
        let request = BuildStartedRequest {
            intent: kache_core::BuildIntent {
                identity_key: Some("id/test".into()),
                crate_names: vec!["serde".into()],
                ..Default::default()
            },
            client_epoch: 0,
            session_id: "do-nothing".into(),
        };

        let response = tokio::time::timeout(
            Duration::from_secs(2),
            daemon.handle_build_started_with_planner(&request, planner_lookup),
        )
        .await
        .expect("do_nothing must cancel the blocked metadata read");

        assert!(response.ok);
        assert_eq!(backend.identity_gets.load(Ordering::Relaxed), 1);
        assert_eq!(backend.identity_cancellations.load(Ordering::Relaxed), 1);
        assert_eq!(backend.artifact_gets.load(Ordering::Relaxed), 0);
        assert_eq!(
            daemon.prefetch_stats.plans_advisory.load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            daemon.prefetch_stats.plans_fallback.load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn execute_cancels_pending_identity_before_artifact_prefetch_runs() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let backend = Arc::new(BlockingIdentityBackend {
            inner: test_remote_backend(),
            identity_gets: AtomicU64::new(0),
            identity_cancellations: AtomicU64::new(0),
            artifact_gets: AtomicU64::new(0),
            artifact_started_before_identity_cancel: AtomicBool::new(false),
            identity_started: Notify::new(),
            block_identity: AtomicBool::new(true),
        });
        let daemon = Arc::new(Daemon::new(config));
        daemon.set_remote_backend_for_test(backend.clone());

        let planner_backend = backend.clone();
        let planner_lookup = async move {
            tokio::time::timeout(
                Duration::from_secs(1),
                planner_backend.identity_started.notified(),
            )
            .await
            .expect("identity metadata lookup must start before the planner returns");
            Ok(Some(PrefetchPlan {
                plan_id: Some("execute".into()),
                planner: Some("test".into()),
                disposition: PrefetchDisposition::Execute,
                candidates: vec![kache_core::PrefetchCandidate::new(
                    "a".repeat(64),
                    "serde".into(),
                )],
            }))
        };
        let request = BuildStartedRequest {
            intent: kache_core::BuildIntent {
                identity_key: Some("id/test".into()),
                crate_names: vec!["serde".into()],
                ..Default::default()
            },
            client_epoch: 0,
            session_id: "execute".into(),
        };

        let response = daemon
            .handle_build_started_with_planner(&request, planner_lookup)
            .await;
        wait_for_test_condition(|| backend.artifact_gets.load(Ordering::Relaxed) > 0).await;

        assert!(response.ok);
        assert_eq!(backend.identity_gets.load(Ordering::Relaxed), 1);
        assert_eq!(backend.identity_cancellations.load(Ordering::Relaxed), 1);
        assert!(
            backend.artifact_gets.load(Ordering::Relaxed) > 0,
            "the advisory plan must reach the artifact backend"
        );
        assert!(
            !backend
                .artifact_started_before_identity_cancel
                .load(Ordering::Acquire),
            "artifact GET started before the pending identity GET was cancelled"
        );
        assert_eq!(
            daemon.prefetch_stats.plans_advisory.load(Ordering::Relaxed),
            1
        );
        wait_for_test_condition(|| !daemon.recent_transfers.lock().unwrap().is_empty()).await;
        let origin = latest_transfer(&daemon).prefetch.unwrap();
        assert_eq!(origin.session_id, "execute");
        assert_eq!(origin.plan_id, "execute");
        assert_eq!(origin.source, "advisory");
    }

    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::await_holding_lock)]
    async fn execute_failure_retries_cancelled_identity_through_ordinary_fallback() {
        let _lock = crate::config::config_path_lock();
        let _manifest_key_env = EnvVarForTest::remove("KACHE_MANIFEST_KEY");
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let inner = test_remote_backend();
        crate::remote::upload_manifest(
            inner.as_ref(),
            "prefix",
            "id/test",
            &crate::remote::BuildManifest {
                version: 3,
                created: "2026-08-31T00:00:00Z".into(),
                manifest_key: "id/test".into(),
                entries: Vec::new(),
            },
        )
        .await
        .unwrap();
        let backend = Arc::new(BlockingIdentityBackend {
            inner,
            identity_gets: AtomicU64::new(0),
            identity_cancellations: AtomicU64::new(0),
            artifact_gets: AtomicU64::new(0),
            artifact_started_before_identity_cancel: AtomicBool::new(false),
            identity_started: Notify::new(),
            block_identity: AtomicBool::new(true),
        });
        let daemon = Arc::new(Daemon::new(config));
        daemon.set_remote_backend_for_test(backend.clone());
        let planner_backend = backend.clone();
        let planner_lookup = async move {
            tokio::time::timeout(
                Duration::from_secs(1),
                planner_backend.identity_started.notified(),
            )
            .await
            .expect("identity metadata lookup must start before the planner returns");
            Ok(Some(PrefetchPlan {
                plan_id: Some("execute-then-fail".into()),
                planner: Some("test".into()),
                disposition: PrefetchDisposition::Execute,
                candidates: vec![kache_core::PrefetchCandidate::new(
                    "a".repeat(64),
                    "serde".into(),
                )],
            }))
        };
        let request = BuildStartedRequest {
            intent: kache_core::BuildIntent {
                identity_key: Some("id/test".into()),
                crate_names: vec!["serde".into()],
                ..Default::default()
            },
            client_epoch: 0,
            session_id: "execute-failure".into(),
        };
        let executor_backend = backend.clone();

        let response = tokio::time::timeout(
            Duration::from_secs(1),
            daemon.handle_build_started_with_planner_and_prefetch(
                &request,
                planner_lookup,
                move |_daemon, _request, _pack_context, _plan_started_at| {
                    let backend = executor_backend.clone();
                    async move {
                        assert_eq!(
                            backend.identity_cancellations.load(Ordering::Acquire),
                            1,
                            "identity lookahead must be cancelled before advisory execution"
                        );
                        backend.block_identity.store(false, Ordering::Release);
                        Response::err("forced advisory execution failure")
                    }
                },
            ),
        )
        .await
        .expect("fallback after advisory failure must not await cancelled speculation");

        assert!(response.ok);
        assert_eq!(backend.identity_cancellations.load(Ordering::Relaxed), 1);
        assert_eq!(
            backend.identity_gets.load(Ordering::Relaxed),
            2,
            "fallback must retry the cancelled lookup once through ordinary admission"
        );
        assert_eq!(backend.artifact_gets.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn speculative_identity_does_not_consume_half_open_read_probe() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let backend = Arc::new(BlockingIdentityBackend {
            inner: test_remote_backend(),
            identity_gets: AtomicU64::new(0),
            identity_cancellations: AtomicU64::new(0),
            artifact_gets: AtomicU64::new(0),
            artifact_started_before_identity_cancel: AtomicBool::new(false),
            identity_started: Notify::new(),
            block_identity: AtomicBool::new(false),
        });
        let mut daemon = Daemon::new(config);
        daemon.remote_breaker = Arc::new(RemoteBreaker::with_policy(1, Duration::ZERO));
        daemon
            .remote_breaker
            .try_acquire(RemoteOperation::DemandGet)
            .unwrap()
            .failure(RemoteErrorClass::Timeout, "open read direction");
        let daemon = Arc::new(daemon);
        daemon.set_remote_backend_for_test(backend.clone());

        let planner_lookup = async {
            tokio::task::yield_now().await;
            Ok(Some(PrefetchPlan {
                plan_id: Some("no-prefetch".into()),
                planner: Some("test".into()),
                disposition: PrefetchDisposition::DoNothing,
                candidates: Vec::new(),
            }))
        };
        let request = BuildStartedRequest {
            intent: kache_core::BuildIntent {
                identity_key: Some("id/test".into()),
                ..Default::default()
            },
            client_epoch: 0,
            session_id: "half-open-do-nothing".into(),
        };

        let response = daemon
            .handle_build_started_with_planner(&request, planner_lookup)
            .await;

        assert!(response.ok);
        assert_eq!(backend.identity_gets.load(Ordering::Relaxed), 0);
        assert!(
            daemon
                .remote_breaker
                .is_direction_degraded(crate::remote_resilience::RemoteDirection::Read)
        );
        let probe = daemon
            .remote_breaker
            .try_acquire(RemoteOperation::DemandGet)
            .expect("the demand path must retain the half-open probe");
        probe.success();
    }

    #[tokio::test]
    async fn open_breaker_rejects_speculative_identity_before_saturated_gate() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        config.s3_concurrency = 1;
        let backend = Arc::new(BlockingIdentityBackend {
            inner: test_remote_backend(),
            identity_gets: AtomicU64::new(0),
            identity_cancellations: AtomicU64::new(0),
            artifact_gets: AtomicU64::new(0),
            artifact_started_before_identity_cancel: AtomicBool::new(false),
            identity_started: Notify::new(),
            block_identity: AtomicBool::new(false),
        });
        let mut daemon = Daemon::new(config);
        daemon.remote_breaker = Arc::new(RemoteBreaker::with_policy(1, Duration::from_secs(60)));
        daemon
            .remote_breaker
            .try_acquire(RemoteOperation::DemandGet)
            .unwrap()
            .failure(RemoteErrorClass::Timeout, "open read direction");
        let daemon = Arc::new(daemon);
        daemon.set_remote_backend_for_test(backend.clone());
        let held_gate = daemon.prefetch_gate.clone().acquire_owned().await.unwrap();
        assert_eq!(daemon.prefetch_gate.available_permits(), 0);

        let outcome = tokio::time::timeout(
            Duration::from_millis(100),
            daemon.download_planner_manifest_speculative("id/test"),
        )
        .await
        .expect("an open breaker must reject before waiting on the speculative gate")
        .unwrap();

        assert!(matches!(outcome, SpeculativeManifestOutcome::NotAdmitted));
        assert_eq!(backend.identity_gets.load(Ordering::Relaxed), 0);
        assert_eq!(daemon.prefetch_gate.available_permits(), 0);
        drop(held_gate);
    }

    #[tokio::test]
    async fn aborting_build_started_cancels_identity_lookup_and_releases_permit() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        config.s3_concurrency = 1;
        let backend = Arc::new(BlockingIdentityBackend {
            inner: test_remote_backend(),
            identity_gets: AtomicU64::new(0),
            identity_cancellations: AtomicU64::new(0),
            artifact_gets: AtomicU64::new(0),
            artifact_started_before_identity_cancel: AtomicBool::new(false),
            identity_started: Notify::new(),
            block_identity: AtomicBool::new(true),
        });
        let daemon = Arc::new(Daemon::new(config));
        daemon.set_remote_backend_for_test(backend.clone());
        let request = BuildStartedRequest {
            intent: kache_core::BuildIntent {
                identity_key: Some("id/test".into()),
                ..Default::default()
            },
            client_epoch: 0,
            session_id: "cancelled-handler".into(),
        };

        let task_daemon = daemon.clone();
        let task = tokio::spawn(async move {
            task_daemon
                .handle_build_started_with_planner(
                    &request,
                    std::future::pending::<Result<Option<PrefetchPlan>>>(),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), backend.identity_started.notified())
            .await
            .expect("identity lookup must start");
        assert_eq!(daemon.s3_semaphore.available_permits(), 0);

        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(backend.identity_cancellations.load(Ordering::Relaxed), 1);
        assert_eq!(daemon.s3_semaphore.available_permits(), 1);
        assert_eq!(daemon.prefetch_gate.available_permits(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::await_holding_lock)]
    async fn planner_selected_fallback_uses_reserve_while_lookahead_is_saturated() {
        let _lock = crate::config::config_path_lock();
        let _manifest_key_env = EnvVarForTest::remove("KACHE_MANIFEST_KEY");
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        config.s3_concurrency = 4;
        let inner = test_remote_backend();
        let manifest_key = crate::identity::manifest_lookup_keys(Some("id/test"))
            .into_iter()
            .next()
            .expect("identity lookup must have a primary key");
        crate::remote::upload_manifest(
            inner.as_ref(),
            "prefix",
            &manifest_key,
            &crate::remote::BuildManifest {
                version: 3,
                created: "2026-08-31T00:00:00Z".into(),
                manifest_key: manifest_key.clone(),
                entries: Vec::new(),
            },
        )
        .await
        .unwrap();
        let backend = Arc::new(BlockingIdentityBackend {
            inner,
            identity_gets: AtomicU64::new(0),
            identity_cancellations: AtomicU64::new(0),
            artifact_gets: AtomicU64::new(0),
            artifact_started_before_identity_cancel: AtomicBool::new(false),
            identity_started: Notify::new(),
            block_identity: AtomicBool::new(true),
        });
        let daemon = Arc::new(Daemon::new(config));
        daemon.set_remote_backend_for_test(backend.clone());
        let intent = kache_core::BuildIntent {
            identity_key: Some("id/test".into()),
            ..Default::default()
        };

        let mut tasks = Vec::new();
        for _ in 0..4 {
            let daemon = daemon.clone();
            let intent = intent.clone();
            tasks.push(tokio::spawn(async move {
                crate::fallback_planner::resolve_identity_candidates_speculative(&daemon, &intent)
                    .await
            }));
        }
        wait_for_test_condition(|| backend.identity_gets.load(Ordering::Relaxed) == 3).await;

        assert_eq!(daemon.prefetch_gate.available_permits(), 0);
        assert_eq!(daemon.s3_semaphore.available_permits(), 1);
        let demand_permit = daemon
            .s3_semaphore
            .try_acquire()
            .expect("one S3 permit must remain available for demand");
        drop(demand_permit);

        // Existing speculative GETs stay blocked, but a newly admitted
        // ordinary fallback GET may complete through the reserved S3 permit.
        backend.block_identity.store(false, Ordering::Release);
        let response = tokio::time::timeout(
            Duration::from_secs(1),
            daemon.handle_build_started_with_planner(
                &BuildStartedRequest {
                    intent: intent.clone(),
                    client_epoch: 0,
                    session_id: "reserved-fallback".into(),
                },
                async {
                    Ok(Some(PrefetchPlan {
                        plan_id: Some("fallback".into()),
                        planner: Some("test".into()),
                        disposition: PrefetchDisposition::UseFallback,
                        candidates: Vec::new(),
                    }))
                },
            ),
        )
        .await
        .expect("selected fallback must not wait on the saturated speculative gate");
        assert!(response.ok);
        assert_eq!(
            backend.identity_gets.load(Ordering::Relaxed),
            4,
            "fallback must perform one ordinary manifest GET through the reserve"
        );
        assert_eq!(daemon.prefetch_gate.available_permits(), 0);
        assert_eq!(daemon.s3_semaphore.available_permits(), 1);

        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            assert!(task.await.unwrap_err().is_cancelled());
        }
        assert_eq!(backend.identity_cancellations.load(Ordering::Relaxed), 3);
        assert_eq!(daemon.s3_semaphore.available_permits(), 4);
        assert_eq!(daemon.prefetch_gate.available_permits(), 3);
    }

    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::await_holding_lock)]
    async fn fallback_reuses_early_identity_candidates_without_second_manifest_get() {
        let _lock = crate::config::config_path_lock();
        let _manifest_key_env = EnvVarForTest::remove("KACHE_MANIFEST_KEY");
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let inner = test_remote_backend();
        let manifest = crate::remote::BuildManifest {
            version: 3,
            created: "2026-08-31T00:00:00Z".into(),
            manifest_key: "id/test".into(),
            entries: vec![crate::remote::ManifestEntry {
                cache_key: "a".repeat(64),
                crate_name: "serde".into(),
                compile_time_ms: 1200,
                artifact_size: 4096,
            }],
        };
        crate::remote::upload_manifest(inner.as_ref(), "prefix", "id/test", &manifest)
            .await
            .unwrap();
        let backend = Arc::new(BlockingIdentityBackend {
            inner,
            identity_gets: AtomicU64::new(0),
            identity_cancellations: AtomicU64::new(0),
            artifact_gets: AtomicU64::new(0),
            artifact_started_before_identity_cancel: AtomicBool::new(false),
            identity_started: Notify::new(),
            block_identity: AtomicBool::new(false),
        });
        let daemon = Arc::new(Daemon::new(config));
        daemon.set_remote_backend_for_test(backend.clone());
        let intent = kache_core::BuildIntent {
            identity_key: Some("id/test".into()),
            crate_names: vec!["serde".into()],
            ..Default::default()
        };

        let early =
            crate::fallback_planner::resolve_identity_candidates_speculative(&daemon, &intent)
                .await;
        assert!(matches!(
            &early,
            crate::fallback_planner::SpeculativeIdentityOutcome::Resolved(candidates)
                if candidates.len() == 1
        ));
        assert_eq!(backend.identity_gets.load(Ordering::Relaxed), 1);
        let plan =
            crate::fallback_planner::build_prefetch_plan_with_identity(&daemon, &intent, early)
                .await
                .unwrap();

        assert_eq!(backend.identity_gets.load(Ordering::Relaxed), 1);
        assert_eq!(plan.candidates.len(), 1);
        assert_eq!(plan.candidates[0].cache_key, "a".repeat(64));
    }

    #[tokio::test]
    async fn queued_speculative_identity_rechecks_breaker_before_dispatch() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        config.s3_concurrency = 1;
        let backend = Arc::new(BlockingIdentityBackend {
            inner: test_remote_backend(),
            identity_gets: AtomicU64::new(0),
            identity_cancellations: AtomicU64::new(0),
            artifact_gets: AtomicU64::new(0),
            artifact_started_before_identity_cancel: AtomicBool::new(false),
            identity_started: Notify::new(),
            block_identity: AtomicBool::new(false),
        });
        let mut daemon = Daemon::new(config);
        daemon.remote_breaker = Arc::new(RemoteBreaker::with_policy(1, Duration::from_secs(60)));
        let daemon = Arc::new(daemon);
        daemon.set_remote_backend_for_test(backend.clone());
        let held_s3 = daemon.s3_semaphore.clone().acquire_owned().await.unwrap();
        let intent = kache_core::BuildIntent {
            identity_key: Some("id/test".into()),
            ..Default::default()
        };

        let task_daemon = daemon.clone();
        let task = tokio::spawn(async move {
            crate::fallback_planner::resolve_identity_candidates_speculative(&task_daemon, &intent)
                .await
        });
        wait_for_test_condition(|| daemon.prefetch_gate.available_permits() == 0).await;
        assert_eq!(backend.identity_gets.load(Ordering::Relaxed), 0);
        daemon
            .remote_breaker
            .try_acquire(RemoteOperation::DemandGet)
            .unwrap()
            .failure(RemoteErrorClass::Timeout, "open while lookahead is queued");
        drop(held_s3);
        let outcome = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("queued lookahead must finish after the S3 permit is released")
            .unwrap();

        assert!(matches!(
            outcome,
            crate::fallback_planner::SpeculativeIdentityOutcome::NotAdmitted(keys)
                if !keys.is_empty()
        ));
        assert_eq!(backend.identity_gets.load(Ordering::Relaxed), 0);
        assert!(
            daemon
                .remote_breaker
                .is_direction_degraded(crate::remote_resilience::RemoteDirection::Read),
            "queued speculative work must not recover or bypass the open breaker"
        );
        assert_eq!(daemon.s3_semaphore.available_permits(), 1);
        assert_eq!(daemon.prefetch_gate.available_permits(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::await_holding_lock)]
    async fn denied_speculative_identity_is_retried_by_ordinary_fallback_lookup() {
        let _lock = crate::config::config_path_lock();
        let _manifest_key_env = EnvVarForTest::remove("KACHE_MANIFEST_KEY");
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let inner = test_remote_backend();
        let cache_key = "d".repeat(64);
        let manifest = crate::remote::BuildManifest {
            version: 3,
            created: "2026-08-31T00:00:00Z".into(),
            manifest_key: "id/test".into(),
            entries: vec![crate::remote::ManifestEntry {
                cache_key: cache_key.clone(),
                crate_name: "serde".into(),
                compile_time_ms: 1200,
                artifact_size: 4096,
            }],
        };
        crate::remote::upload_manifest(inner.as_ref(), "prefix", "id/test", &manifest)
            .await
            .unwrap();
        let backend = Arc::new(BlockingIdentityBackend {
            inner,
            identity_gets: AtomicU64::new(0),
            identity_cancellations: AtomicU64::new(0),
            artifact_gets: AtomicU64::new(0),
            artifact_started_before_identity_cancel: AtomicBool::new(false),
            identity_started: Notify::new(),
            block_identity: AtomicBool::new(false),
        });
        let mut daemon = Daemon::new(config);
        daemon.remote_breaker = Arc::new(RemoteBreaker::with_policy(1, Duration::ZERO));
        daemon
            .remote_breaker
            .try_acquire(RemoteOperation::DemandGet)
            .unwrap()
            .failure(RemoteErrorClass::Timeout, "open read direction");
        let daemon = Arc::new(daemon);
        daemon.set_remote_backend_for_test(backend.clone());
        let intent = kache_core::BuildIntent {
            identity_key: Some("id/test".into()),
            crate_names: vec!["serde".into()],
            ..Default::default()
        };

        let speculative =
            crate::fallback_planner::resolve_identity_candidates_speculative(&daemon, &intent)
                .await;
        assert!(matches!(
            &speculative,
            crate::fallback_planner::SpeculativeIdentityOutcome::NotAdmitted(keys)
                if !keys.is_empty()
        ));
        assert_eq!(backend.identity_gets.load(Ordering::Relaxed), 0);

        let plan = crate::fallback_planner::build_prefetch_plan_with_identity(
            &daemon,
            &intent,
            speculative,
        )
        .await
        .expect("ordinary fallback lookup must use the half-open probe");

        assert_eq!(backend.identity_gets.load(Ordering::Relaxed), 1);
        assert_eq!(plan.candidates.len(), 1);
        assert_eq!(plan.candidates[0].cache_key, cache_key);
        assert!(
            !daemon
                .remote_breaker
                .is_direction_degraded(crate::remote_resilience::RemoteDirection::Read)
        );
    }

    #[tokio::test]
    async fn speculative_error_class_controls_retry_without_repeating_permanent_failures() {
        let intent = kache_core::BuildIntent {
            identity_key: Some("id/test".into()),
            crate_names: vec!["serde".into()],
            ..Default::default()
        };

        let transient_dir = tempfile::tempdir().unwrap();
        let mut transient_config = test_config(transient_dir.path());
        transient_config.remote = Some(test_remote_config());
        let transient_backend = Arc::new(FailingIdentityBackend {
            inner: test_remote_backend(),
            kind: std::io::ErrorKind::ConnectionReset,
            identity_keys: Mutex::new(Vec::new()),
        });
        let mut transient_daemon = Daemon::new(transient_config);
        transient_daemon.remote_breaker =
            Arc::new(RemoteBreaker::with_policy(1, Duration::from_secs(60)));
        let transient_daemon = Arc::new(transient_daemon);
        transient_daemon.set_remote_backend_for_test(transient_backend.clone());

        let transient = crate::fallback_planner::resolve_identity_candidates_speculative(
            &transient_daemon,
            &intent,
        )
        .await;
        assert!(matches!(
            &transient,
            crate::fallback_planner::SpeculativeIdentityOutcome::NotAdmitted(keys)
                if !keys.is_empty()
        ));
        assert_eq!(transient_backend.identity_keys.lock().unwrap().len(), 1);
        assert!(
            transient_daemon
                .remote_breaker
                .is_direction_degraded(crate::remote_resilience::RemoteDirection::Read)
        );
        let plan = crate::fallback_planner::build_prefetch_plan_with_identity(
            &transient_daemon,
            &intent,
            transient,
        )
        .await
        .unwrap();
        assert!(plan.candidates.is_empty());
        assert_eq!(
            transient_backend.identity_keys.lock().unwrap().len(),
            1,
            "the ordinary retry must be suppressed while threshold=1 keeps the breaker open"
        );

        let permanent_dir = tempfile::tempdir().unwrap();
        let mut permanent_config = test_config(permanent_dir.path());
        permanent_config.remote = Some(test_remote_config());
        let permanent_backend = Arc::new(FailingIdentityBackend {
            inner: test_remote_backend(),
            kind: std::io::ErrorKind::PermissionDenied,
            identity_keys: Mutex::new(Vec::new()),
        });
        let permanent_daemon = Arc::new(Daemon::new(permanent_config));
        permanent_daemon.set_remote_backend_for_test(permanent_backend.clone());

        let permanent = crate::fallback_planner::resolve_identity_candidates_speculative(
            &permanent_daemon,
            &intent,
        )
        .await;
        assert!(matches!(
            &permanent,
            crate::fallback_planner::SpeculativeIdentityOutcome::NonRetryableFailures(failures)
                if !failures.is_empty()
                    && failures.iter().all(|(_, class)| !matches!(
                        class,
                        RemoteErrorClass::Transient | RemoteErrorClass::Timeout
                    ))
        ));
        let attempted_keys = permanent_backend.identity_keys.lock().unwrap().clone();
        assert!(!attempted_keys.is_empty());
        assert_eq!(
            attempted_keys.iter().collect::<HashSet<_>>().len(),
            attempted_keys.len(),
            "each identity alias may be tried once, but none may be repeated"
        );
        assert!(
            !permanent_daemon
                .remote_breaker
                .is_direction_degraded(crate::remote_resilience::RemoteDirection::Read)
        );
        let plan = crate::fallback_planner::build_prefetch_plan_with_identity(
            &permanent_daemon,
            &intent,
            permanent,
        )
        .await
        .unwrap();
        assert!(plan.candidates.is_empty());
        assert_eq!(
            permanent_backend.identity_keys.lock().unwrap().as_slice(),
            attempted_keys.as_slice(),
            "authentication/permanent failures must be authoritative for this plan"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::await_holding_lock)]
    async fn speculative_timeout_is_not_retried_within_build_started() {
        let _lock = crate::config::config_path_lock();
        let _manifest_key = EnvVarForTest::remove("KACHE_MANIFEST_KEY");
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let backend = Arc::new(FailingIdentityBackend {
            inner: test_remote_backend(),
            kind: std::io::ErrorKind::TimedOut,
            identity_keys: Mutex::new(Vec::new()),
        });
        let daemon = Arc::new(Daemon::new(config));
        daemon.set_remote_backend_for_test(backend.clone());
        let intent = kache_core::BuildIntent {
            identity_key: Some("id/test".into()),
            crate_names: vec!["serde".into()],
            ..Default::default()
        };

        let outcome =
            crate::fallback_planner::resolve_identity_candidates_speculative(&daemon, &intent)
                .await;
        assert!(matches!(
            &outcome,
            crate::fallback_planner::SpeculativeIdentityOutcome::NonRetryableFailures(failures)
                if failures.len() == 1 && failures[0].1 == RemoteErrorClass::Timeout
        ));
        assert_eq!(backend.identity_keys.lock().unwrap().len(), 1);

        let plan =
            crate::fallback_planner::build_prefetch_plan_with_identity(&daemon, &intent, outcome)
                .await
                .unwrap();
        assert!(plan.candidates.is_empty());
        assert_eq!(
            backend.identity_keys.lock().unwrap().len(),
            1,
            "a timed-out speculative GET must not start a second manifest deadline"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::await_holding_lock)]
    async fn identity_first_handler_fallback_reuses_manifest_candidates() {
        let _lock = crate::config::config_path_lock();
        let _manifest_key_env = EnvVarForTest::remove("KACHE_MANIFEST_KEY");
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.remote = Some(test_remote_config());
        let inner = test_remote_backend();
        let cache_key = "b".repeat(64);
        let manifest = crate::remote::BuildManifest {
            version: 3,
            created: "2026-08-31T00:00:00Z".into(),
            manifest_key: "id/test".into(),
            entries: vec![crate::remote::ManifestEntry {
                cache_key: cache_key.clone(),
                crate_name: "serde".into(),
                compile_time_ms: 1200,
                artifact_size: 4096,
            }],
        };
        crate::remote::upload_manifest(inner.as_ref(), "prefix", "id/test", &manifest)
            .await
            .unwrap();
        let backend = Arc::new(BlockingIdentityBackend {
            inner,
            identity_gets: AtomicU64::new(0),
            identity_cancellations: AtomicU64::new(0),
            artifact_gets: AtomicU64::new(0),
            artifact_started_before_identity_cancel: AtomicBool::new(false),
            identity_started: Notify::new(),
            block_identity: AtomicBool::new(false),
        });
        let daemon = Arc::new(Daemon::new(config));
        daemon.set_remote_backend_for_test(backend.clone());
        let planner_backend = backend.clone();
        let planner_lookup = async move {
            tokio::time::timeout(
                Duration::from_secs(1),
                planner_backend.identity_started.notified(),
            )
            .await
            .expect("speculative identity lookup must start before fallback is selected");
            Ok(Some(PrefetchPlan {
                plan_id: Some("fallback".into()),
                planner: Some("test".into()),
                disposition: PrefetchDisposition::UseFallback,
                candidates: Vec::new(),
            }))
        };
        let request = BuildStartedRequest {
            intent: kache_core::BuildIntent {
                identity_key: Some("id/test".into()),
                crate_names: vec!["serde".into()],
                ..Default::default()
            },
            client_epoch: 0,
            session_id: "identity-first-fallback".into(),
        };

        let response = daemon
            .handle_build_started_with_planner(&request, planner_lookup)
            .await;

        assert!(response.ok);
        assert_eq!(backend.identity_gets.load(Ordering::Relaxed), 1);
        assert_eq!(
            daemon.prefetch_stats.plans_fallback.load(Ordering::Relaxed),
            1
        );
        wait_for_test_condition(|| !daemon.recent_transfers.lock().unwrap().is_empty()).await;
        let origin = latest_transfer(&daemon).prefetch.unwrap();
        assert_eq!(origin.session_id, "identity-first-fallback");
        assert!(origin.plan_id.is_empty());
        assert_eq!(origin.source, "fallback");
        assert_eq!(
            origin.candidate_source,
            kache_core::CandidateSource::Manifest
        );
        let plan = daemon.active_plan.lock().unwrap();
        let plan = plan.as_ref().expect("fallback plan must be installed");
        assert_eq!(plan.plan_source, "fallback");
        assert_eq!(plan.candidates, HashSet::from([cache_key]));
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

    #[tokio::test]
    async fn packed_download_claim_rejects_disk_and_inflight_entries_independently() {
        let tmp = tempfile::tempdir().unwrap();
        let map = RwLock::new(HashMap::new());

        let existing_key = test_cache_key("packed-existing-on-disk");
        let existing_dir = tmp.path().join("existing");
        std::fs::create_dir_all(&existing_dir).unwrap();
        assert!(
            !try_claim_packed_download(&map, &existing_key, &existing_dir).await,
            "an on-disk entry must not be claimed again"
        );
        assert!(map.read().await.is_empty());

        let inflight_key = test_cache_key("packed-inflight");
        assert!(claim_download(&map, &inflight_key).await.is_none());
        assert!(
            !try_claim_packed_download(&map, &inflight_key, &tmp.path().join("absent")).await,
            "an in-flight entry must not acquire a second claim"
        );

        let fresh_key = test_cache_key("packed-fresh");
        assert!(
            try_claim_packed_download(&map, &fresh_key, &tmp.path().join("fresh")).await,
            "an absent unclaimed entry must become the download leader"
        );
        let claims = map.read().await;
        assert!(claims.contains_key(&inflight_key));
        assert!(claims.contains_key(&fresh_key));
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
        let notify = Arc::new(Notify::new());
        // Branch: DownloadingGuard contended-drop runtime fallback. The
        // spawned removal must both clear the key and wake waiters parked on
        // the key's Notify (notify runs AFTER the removal).

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

    #[test]
    fn download_join_deadline_uses_the_earliest_budget() {
        let now = tokio::time::Instant::now();
        let join_budget = now.checked_add(DOWNLOAD_JOIN_BUDGET).unwrap();
        let sooner = now.checked_add(Duration::from_secs(1)).unwrap();
        let later = now
            .checked_add(DOWNLOAD_JOIN_BUDGET + Duration::from_secs(1))
            .unwrap();

        assert_eq!(download_join_deadline(now, None), join_budget);
        assert_eq!(download_join_deadline(now, Some(later)), join_budget);
        assert_eq!(download_join_deadline(now, Some(sooner)), sooner);
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

    #[tokio::test]
    async fn request_read_refuses_frames_across_both_shutdown_boundaries() {
        let lifecycle = Arc::new(Lifecycle::default());
        lifecycle.start_drain();
        let read_was_polled = AtomicBool::new(false);
        let before_read = read_request_before_shutdown(&lifecycle, async {
            read_was_polled.store(true, Ordering::Relaxed);
            Ok::<_, std::io::Error>(Some("must-not-run".to_string()))
        })
        .await
        .unwrap();
        assert!(before_read.is_none(), "shutdown must skip the next read");
        assert!(
            !read_was_polled.load(Ordering::Relaxed),
            "a queued handler must not poll its request after shutdown"
        );

        let lifecycle = Arc::new(Lifecycle::default());
        let completed_during_shutdown = read_request_before_shutdown(&lifecycle, async {
            // Models another connection initiating shutdown while this handler
            // is parked in its request read.
            lifecycle.start_drain();
            Ok::<_, std::io::Error>(Some("late-frame".to_string()))
        })
        .await
        .unwrap();
        assert!(
            completed_during_shutdown.is_none(),
            "a frame completed after shutdown must not be dispatched"
        );
    }
}
