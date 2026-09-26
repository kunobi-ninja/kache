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
            updated_at_ms: unix_time_ms(),
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
    unix_time_ms()
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
    /// A build of a target directory started: copy its recorded large
    /// executables beside their destinations (see [`crate::prestage`]).
    /// Fire-and-forget; older daemons reject the unknown variant and the
    /// restore copies as before.
    Prestage(PrestageRequest),
    Shutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PrestageRequest {
    pub target_dir: String,
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
        let mut shard_hashes = crate::shards::compute_shards(deps)
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
            started_at_ms: unix_time_ms(),
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

/// Ms since the Unix epoch (0 on a pre-epoch clock).
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
        let now = unix_time_ms();
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
        self.last_activity_ms = unix_time_ms();
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
        self.last_activity_ms = unix_time_ms();
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
            | Request::Prestage(_)
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
        let now_ms = unix_time_ms();
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
        let now_ms = unix_time_ms();
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
                    existing.last_activity_ms = unix_time_ms();
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
                Some(p) if unix_time_ms().saturating_sub(p.last_activity_ms) >= inactivity_ms => {
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

    /// Stage the target directory's recorded executables in the background;
    /// the wrapper that hinted does not wait.
    fn handle_prestage(self: &Arc<Self>, req: &PrestageRequest) -> Response {
        let cache_dir = self.config.cache_dir.clone();
        let store_dir = self.config.store_dir();
        let target_dir = PathBuf::from(&req.target_dir);
        tokio::task::spawn_blocking(move || {
            crate::prestage::stage(&cache_dir, &target_dir, |hash| {
                crate::store::blob_path_in_store_dir(&store_dir, hash)
            });
        });
        Response::ok()
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

    /// The sweep behind the post-upload check and wrapper hints, of the main
    /// store and then each `[cache.volumes]` shard. A shard's failure is
    /// logged and never fails the main store's sweep.
    fn sweep_under_size_pressure(&self) -> Result<()> {
        let main = self.sweep_main_under_size_pressure();
        crate::volume_gc::run_on_volume_shards(&self.config, |shard| {
            self.sweep_shard_under_size_pressure(shard)
        });
        main
    }

    /// One shard's size-pressure sweep, under that shard's `gc.lock`.
    fn sweep_shard_under_size_pressure(&self, shard: &Config) -> Result<()> {
        let store = Store::open(shard)?;
        let Some(_gc_lock) = store.try_gc_lock()? else {
            tracing::debug!(
                "gc.lock of shard {} held by another GC; skipping size-pressure eviction",
                shard.cache_dir.display()
            );
            return Ok(());
        };
        let size = store.physical_size()?;
        self.size_pressure_sweep_if_due(shard, size)
    }

    /// The main store's size-pressure sweep. Skips when another driver holds
    /// `gc.lock`, the store is under the trigger, or the backoff holds; each
    /// costs a lock attempt and one size query.
    fn sweep_main_under_size_pressure(&self) -> Result<()> {
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
        self.size_pressure_sweep_if_due(&self.config, size)
    }

    /// Sweep the store of `config`, now at `size`, if the shared trigger
    /// says a sweep is due. Caller holds that store's `gc.lock`.
    fn size_pressure_sweep_if_due(&self, config: &Config, size: u64) -> Result<()> {
        if !crate::wrapper::auto_gc_sweep_due(config, size) {
            return Ok(());
        }
        // Under gc.lock like every driver, so the totals cannot race.
        let store = Store::open(config)?;
        let stats = self.automatic_size_pass(config, &store, size)?;
        if let Err(e) = crate::report::record_gc_run(config, "daemon", &stats) {
            tracing::warn!("recording size-pressure GC run: {e:#}");
        }
        Ok(())
    }

    /// The size pass of an automatic sweep the shared trigger found due.
    /// Records where the store ended, so a sweep that could not clear the
    /// pressure backs off every automatic driver. Caller holds `gc.lock`.
    fn automatic_size_pass(
        &self,
        config: &Config,
        store: &Store,
        size: u64,
    ) -> Result<crate::store::GcStats> {
        tracing::info!(
            "store {} size {} over the automatic trigger (max {}), running LRU eviction",
            config.cache_dir.display(),
            size,
            config.max_size
        );
        let started = Instant::now();
        let mut stats = store.evict_for(crate::store::SweepOrigin::Automatic)?;
        stats.duration_ms = started.elapsed().as_millis() as u64;
        if let Ok(after) = store.physical_size() {
            crate::wrapper::record_auto_gc_outcome(config, after, stats.bytes_held);
        }
        Ok(stats)
    }

    /// The size pass of a full sweep. A requested `kache gc` always runs it
    /// and leaves the backoff alone. The timer runs it only when the shared
    /// trigger says a sweep is due; its age and duplicate passes are not
    /// size pressure and stay on schedule.
    fn size_pass(
        &self,
        config: &Config,
        driver: GcDriver,
        store: &Store,
    ) -> Result<crate::store::GcStats> {
        if driver == GcDriver::Requested {
            return store.evict();
        }
        let size = store.physical_size()?;
        if !crate::wrapper::auto_gc_sweep_due(config, size) {
            tracing::info!("periodic GC: size pass not due (under the trigger or backing off)");
            return Ok(crate::store::GcStats::default());
        }
        self.automatic_size_pass(config, store, size)
    }

    /// Sweep the main store, then each `[cache.volumes]` shard with its own
    /// budget (kunobi-ninja/kache#974). The report describes the main store.
    /// Each store's `gc.lock` is its own: a shard another GC holds is skipped,
    /// and a busy main store does not stop the shards from being swept.
    fn run_gc(&self, policy: GcPolicy, driver: GcDriver) -> Result<GcRunReport> {
        let report = self.run_gc_store(&self.config, policy, driver)?;
        crate::volume_gc::run_on_volume_shards(&self.config, |shard| {
            self.run_gc_store(shard, policy, driver)
        });
        Ok(report)
    }

    /// Core GC logic for the store of `config` with an explicit policy and
    /// per-policy result accounting. Holds that store's `gc.lock`.
    fn run_gc_store(
        &self,
        config: &Config,
        policy: GcPolicy,
        driver: GcDriver,
    ) -> Result<GcRunReport> {
        let start = Instant::now();
        let mode = policy.mode();
        // Cross-process GC mutual exclusion (kunobi-ninja/kache#326): if another
        // GC driver (a manual `kache gc`, a second daemon) holds gc.lock, skip
        // this run rather than double-scan and contend. Held until run_gc returns.
        // GC may scan and remove thousands of entries. Use its own connection
        // so daemon lookups and uploads can still reach the main Store mutex.
        // SQLite and gc.lock continue to serialize the actual writes.
        let gc_store = Store::open(config)?;
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
                        let size_stats = self.size_pass(config, driver, store)?;
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
                crate::build_script::sweep_hermetic_out_dirs_for_gc(&self.config.cache_dir);

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
        let prestage_pruned =
            crate::prestage::prune(&self.config.cache_dir, std::time::SystemTime::now());
        tracing::info!(
            key_locks_removed = housekeeping.key_locks_removed,
            key_locks_remaining = housekeeping.key_locks_remaining,
            predictions_pruned = housekeeping.predictions_pruned,
            file_hashes_pruned = housekeeping.file_hashes_pruned,
            prestage_pruned,
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
        if let Err(e) = crate::report::record_gc_run(config, "daemon", &stats) {
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
    let shard_set = crate::shards::compute_shards(deps);

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
            Ok(Request::Prestage(req)) => daemon.handle_prestage(&req),
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

/// Hint the daemon to stage `target_dir`'s recorded executables.
/// Non-blocking, fire-and-forget.
pub fn send_prestage(config: &Config, target_dir: &Path) {
    let req = Request::Prestage(PrestageRequest {
        target_dir: target_dir.to_string_lossy().into_owned(),
    });
    if let Err(e) = send_request_fire_and_forget(&config.socket_path(), &req) {
        tracing::debug!("prestage hint: daemon unreachable ({e}), skipping");
    }
}

/// Max age before an in-flight compile entry is dropped even if a process
/// with that PID is still alive — PID reuse must not resurrect a ghost.
const IN_FLIGHT_MAX_AGE_MS: u64 = 6 * 60 * 60 * 1000;

/// Drop registry entries whose process is gone or whose age is absurd. Called
/// on both the register and snapshot paths (kunobi-ninja/kache#131) — a
/// wrapper killed by OOM or ^C never sends CompileFinished.
fn prune_in_flight(map: &mut HashMap<u32, CompileStartedRequest>) {
    let now = unix_time_ms();
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
mod tests;
