//! Data exchanged by Kache clients and the local daemon.
//!
//! These types retain the existing JSON field names and compatibility defaults.
//! They perform no I/O and do not depend on the cache store or daemon runtime.
//! Runtime policy and conversion from live application state stay with callers.

use kache_core::timeline::{PrefetchAccounting, PrefetchOrigin};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub mod command_line;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PrestageRequest {
    pub target_dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SeedTargetRequest {
    pub target_dir: String,
    pub workspace_root: String,
    /// `rustc -vV` from the compiler Cargo is probing.
    pub rustc_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PredictionFetchRequest {
    pub identity: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UploadJob {
    pub key: String,
    pub entry_dir: String,
    #[serde(default)]
    pub crate_name: String,
    /// Client executable mtime. Daemons from 1.0.x and earlier drain when it
    /// is newer than their own; later daemons ignore it.
    #[serde(default)]
    pub client_epoch: u64,
    /// Release requesting an upgrade. Absent from legacy and read-only requests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_version: Option<String>,
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
    /// Client executable mtime, or zero for reads that must not request a
    /// drain. Daemons from 1.0.x and earlier drain when it is newer than their
    /// own; later daemons ignore it.
    #[serde(default)]
    pub client_epoch: u64,
    /// Release requesting an upgrade. Absent from legacy and read-only requests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_version: Option<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BuildStartedRequest {
    #[serde(default)]
    pub intent: kache_core::BuildIntent,
    /// Client executable mtime. Daemons from 1.0.x and earlier drain when it
    /// is newer than their own; later daemons ignore it.
    #[serde(default)]
    pub client_epoch: u64,
    /// Release requesting an upgrade. Absent from legacy and read-only requests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_version: Option<String>,
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
    /// No release drains on this field; clients send zero.
    #[serde(default)]
    pub client_epoch: u64,
    /// Release requesting an upgrade. Absent from legacy and read-only requests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_version: Option<String>,
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
    #[serde(default)]
    pub version: String,
    /// Daemon executable mtime. Clients from 1.0.x and earlier replace a daemon
    /// whose executable is older than theirs; later clients only display it.
    #[serde(default)]
    pub build_epoch: u64,
    /// Whether the daemon skips remote uploads. `None` from older daemons and
    /// from the lifecycle control endpoint, which does not carry it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_readonly: Option<bool>,
}

/// The configuration the daemon loaded at startup, carried in every
/// daemon statistics response (kunobi-ninja/kache#689).
///
/// Daemon-backed CLI reads render these values instead of re-resolving config
/// in the invoking process — whose `KACHE_CONFIG` / `XDG_CONFIG_HOME` /
/// `KACHE_*` env may resolve differently — and name both sides when the two
/// disagree, instead of silently presenting daemon values as if they were the
/// invocation's own.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EffectiveConfig {
    /// Process identity and limits captured after daemon startup raised its limit.
    /// Absent from older daemons.
    #[serde(default)]
    pub process: Option<ProcessContext>,
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
    /// `[cache] remote_key_listing` / `KACHE_REMOTE_KEY_LISTING` as resolved.
    #[serde(default = "default_effective_remote_key_listing")]
    pub remote_key_listing: bool,
    /// Credential-free remote description (for example `s3://bucket/prefix`)
    /// as resolved by the daemon. `None` means no usable remote.
    #[serde(default)]
    pub remote_description: Option<String>,
    /// Whether the daemon started in strict local-only mode.
    #[serde(default)]
    pub local_only: bool,
    /// Whether the daemon skips remote uploads. A daemon keeps the read-only
    /// setting it started with, so a writable build can still reach a
    /// read-only daemon. `None` from daemons that predate the field.
    #[serde(default)]
    pub remote_readonly: Option<bool>,
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
    /// The free-space floor the daemon recovers to. `None` from daemons that
    /// predate the field.
    #[serde(default)]
    pub auto_recover_min_free_bytes: Option<u64>,
    /// Whether the daemon's recovery also removes idle targets. `None` from
    /// daemons that predate the field.
    #[serde(default)]
    pub auto_recover_targets: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProcessContext {
    pub pid: u32,
    pub effective_uid: Option<u32>,
    pub open_file_limits: Option<OpenFileLimits>,
}

/// `None` for a limit means unlimited; a missing whole object means unknown.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpenFileLimits {
    pub soft: Option<u64>,
    pub hard: Option<u64>,
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

/// Point-in-time view of prefetch counters and the cancel latch carried in
/// the daemon statistics response.
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
    #[serde(default)]
    pub store_dirs: Vec<std::path::PathBuf>,
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

/// Transfer-event schema written by this version of the protocol.
pub const TRANSFER_SCHEMA: u32 = 5;

const fn default_transfer_schema() -> u32 {
    TRANSFER_SCHEMA
}

// Compatibility defaults describe old messages, independently of the current
// application's configuration defaults.
fn default_effective_remote_key_listing() -> bool {
    true
}

fn default_effective_remote_key_cache_refresh_secs() -> u64 {
    60
}

mod envelopes;
mod events;
pub mod fallback;
mod prediction;
mod publication;
pub use envelopes::*;
pub use events::*;
pub use kache_format::BlobStats;
pub use prediction::*;
pub use publication::*;

/// Compiler execution handoff, including native strings and commit states.
pub mod execution;
