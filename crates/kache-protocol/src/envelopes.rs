use crate::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StoreSummary {
    pub path: PathBuf,
    pub bytes: u64,
    pub max_size: u64,
    pub entries: usize,
    pub blob_stats: Option<BlobStats>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Request {
    /// Compilation uses its own prepare/commit/completion exchange. Older
    /// daemons reject this command before a client sends Commit.
    ExecuteV1(Box<execution::ExecutionRequest>),
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
    PublishCc(Box<PublishCcRequest>),
    /// A wrapper with no local input prediction row asks for the remote's
    /// (kunobi-ninja/kache#1011). Older daemons reject the unknown variant,
    /// which the wrapper reads as no row.
    PredictionFetch(PredictionFetchRequest),
    /// A wrapper recorded a portable row; the daemon stores it on a writable
    /// remote in the background.
    PredictionPublish(PredictionPublishRequest),
    /// A build of a target directory started: copy its recorded large
    /// executables beside their destinations (see `prestage`).
    /// Fire-and-forget; older daemons reject the unknown variant and the
    /// restore copies as before.
    Prestage(PrestageRequest),
    /// Cargo is probing a target directory it has not built: copy another
    /// checkout's registry units into it before Cargo checks freshness (see
    /// `target_seed`). The wrapper waits for the answer; older
    /// daemons reject the unknown variant and nothing is copied.
    SeedTarget(SeedTargetRequest),
    Shutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PredictionPublishRequest {
    pub identity: String,
    pub row: SharedPrediction,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StatsResponse {
    #[serde(default)]
    pub stores: Vec<StoreSummary>,
    pub total_size: u64,
    pub max_size: u64,
    pub entry_count: usize,
    pub entries: Option<Vec<StatsEntry>>,
    pub events: EventStatsResponse,
    /// Content-dedup figures for the daemon's store. Defaulted for an old
    /// daemon so a new client can suppress the section instead of mixing in
    /// figures from its own differently configured store.
    #[serde(default)]
    pub blob_stats: Option<BlobStats>,
    /// Bounded recent session summaries from the daemon's event directory.
    #[serde(default)]
    pub recent_summaries: Vec<BuildSummaryEvent>,
    #[serde(default)]
    pub version: String,
    /// Daemon executable mtime. Clients from 1.0.x and earlier replace a daemon
    /// whose executable is older than theirs; later clients only display it.
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcBreakdown {
    pub mode: GcRequestMode,
    pub duplicate: GcPolicyOutcome,
    pub age: GcPolicyOutcome,
    pub size: GcPolicyOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Response {
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
    pub prediction: Option<SharedPrediction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Request {
    /// Whether the request comes from a build. Stats pollers, a TUI and GC
    /// commands do not keep the machine from counting as quiet. Execution
    /// proposals count only when the server accepts them.
    pub fn is_build_activity(&self) -> bool {
        !matches!(
            self,
            Request::ExecuteV1(_)
                | Request::Health
                | Request::Stats(_)
                | Request::Gc(_)
                | Request::GcV2(_)
                | Request::Shutdown
        )
    }

    /// Only a declared newer release may request replacement after admission.
    pub fn client_version(&self) -> Option<&str> {
        match self {
            Request::ExecuteV1(req) => Some(&req.client_version),
            Request::Upload(job) => job.client_version.as_deref(),
            Request::Stats(req) => req.client_version.as_deref(),
            Request::BuildStarted(req) => req.client_version.as_deref(),
            Request::CompileStarted(req) => req.client_version.as_deref(),
            Request::PublishCc(req) => req.client_version.as_deref(),
            _ => None,
        }
    }
}

impl Response {
    pub fn ok() -> Self {
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

    #[cfg(feature = "test-support")]
    pub fn ok_evicted(n: usize) -> Self {
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

    pub fn ok_gc(total: usize, breakdown: GcBreakdown) -> Self {
        Self {
            evicted: Some(total),
            gc: Some(breakdown),
            ..Self::ok()
        }
    }

    pub fn ok_gc_skipped(breakdown: GcBreakdown) -> Self {
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

    pub fn ok_stats(stats: StatsResponse) -> Self {
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

    pub fn ok_batch(results: Vec<Response>) -> Self {
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

    pub fn ok_hash_results(results: Vec<HashFileResult>) -> Self {
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

    pub fn found(val: bool) -> Self {
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

    pub fn found_prefetched(val: bool, prefetched: bool) -> Self {
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

    pub fn err(msg: impl Into<String>) -> Self {
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

impl GcRequest {
    pub fn automatic(effective_max_age_hours: u64) -> Self {
        Self {
            max_age_hours: None,
            mode: GcRequestMode::Automatic,
            effective_max_age_hours: Some(effective_max_age_hours),
        }
    }

    pub fn explicit_age(hours: u64) -> Self {
        Self {
            max_age_hours: Some(hours),
            mode: GcRequestMode::ExplicitAge,
            effective_max_age_hours: None,
        }
    }

    #[cfg(feature = "test-support")]
    pub fn legacy(max_age_hours: Option<u64>) -> Self {
        Self {
            max_age_hours,
            mode: GcRequestMode::Legacy,
            effective_max_age_hours: None,
        }
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}
