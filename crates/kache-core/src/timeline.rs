//! Build timeline records: what one build session did, in order, with timings.
//!
//! The client assembles a record from the logs it already writes and sends it
//! to kache-service, which stores it as submitted. Every time is Unix epoch
//! milliseconds. Records carry no filesystem paths and no environment values
//! outside the allowlisted run context.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Version of [`BuildTimeline`]. A server rejects a record whose schema it
/// does not know.
pub const BUILD_TIMELINE_SCHEMA: u32 = 7;

/// One build session: its compiler invocations and the remote transfers that
/// belong to it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct BuildTimeline {
    pub schema: u32,
    /// Stable across re-submissions of the same session from the same run, so
    /// a later, more complete submission replaces the earlier one.
    pub client_record_id: String,
    pub session_id: String,
    #[serde(default)]
    pub kache_version: String,
    /// Earliest unit start.
    pub started_at_ms: u64,
    /// Latest unit finish.
    pub finished_at_ms: u64,
    #[serde(default)]
    pub identity: TimelineIdentity,
    /// Hash of the build root, so sessions from one tree can be grouped without
    /// sending its path.
    #[serde(default)]
    pub root_hash: String,
    #[serde(default)]
    pub context: RunContext,
    #[serde(default)]
    pub log: LogLimits,
    /// Prefetch plan summary for the session, when the daemon had already
    /// closed it when the record was assembled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<TimelineSummary>,
    #[serde(default)]
    pub units: Vec<TimelineUnit>,
    #[serde(default)]
    pub transfers: Vec<TimelineTransfer>,
}

/// What build this was, as far as the client could tell.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct TimelineIdentity {
    /// Truncated content hash of the root's `Cargo.lock`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lock_digest: Option<String>,
    /// Prefetch identity key (`id/{lock}/{target}/{profile}`), only when the
    /// profile was known or the key was set explicitly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_key: Option<String>,
    #[serde(default)]
    pub source: IdentitySource,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum IdentitySource {
    /// Set explicitly by the build environment.
    Explicit,
    /// Derived from the lockfile and a profile named in the environment.
    LockEnv,
    #[default]
    #[serde(other)]
    Absent,
}

/// Where the build ran. Every field is optional; only allowlisted CI variables
/// and explicit labels are sent.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RunContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_attempt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_os: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_arch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_pool: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

/// Log rotation limits in force on the client. Once the event log passes
/// `event_log_max_size` only the last `event_log_keep_lines` lines survive, so
/// a record from a large build may be missing its first units.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct LogLimits {
    #[serde(default)]
    pub event_log_max_size: u64,
    #[serde(default)]
    pub event_log_keep_lines: u64,
}

/// The daemon's per-session prefetch plan summary.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct TimelineSummary {
    /// Some prefetch outcomes were unavailable when the session closed.
    #[serde(default)]
    pub incomplete: bool,
    #[serde(default)]
    pub plan_id: String,
    #[serde(default)]
    pub plan_source: String,
    #[serde(default)]
    pub closure_reason: String,
    #[serde(default)]
    pub started_at_ms: u64,
    #[serde(default)]
    pub last_activity_ms: u64,
    #[serde(default)]
    pub candidate_keys: u64,
    #[serde(default)]
    pub downloaded_keys: u64,
    #[serde(default)]
    pub downloaded_bytes: u64,
    #[serde(default)]
    pub used_keys: u64,
    #[serde(default)]
    pub demanded_keys: u64,
    #[serde(default)]
    pub demanded_candidate_keys: u64,
    #[serde(default)]
    pub cancelled: bool,
    /// Speculative GET keys that a wrapper then consumed as a local or
    /// prefetch hit. Daemon `used_keys` can miss these.
    #[serde(default)]
    pub consumed_prefetch_keys: u64,
    #[serde(default)]
    pub consumed_prefetch_bytes: u64,
    /// Consumed prefetch keys whose import finished at or before first demand.
    #[serde(default)]
    pub useful_prefetch_keys: u64,
    #[serde(default)]
    pub useful_prefetch_bytes: u64,
    /// Sum of wrapper remote-check waits on this session's units.
    #[serde(default)]
    pub remote_wait_ms: u64,
    #[serde(default)]
    pub get_not_found: u64,
    #[serde(default)]
    pub get_errors: u64,
    /// Consumed prefetch keys whose GET had started but not finished at first
    /// demand. The demand waited on the rest of that download, not a new GET.
    #[serde(default)]
    pub in_flight_prefetch_keys: u64,
    #[serde(default)]
    pub in_flight_prefetch_bytes: u64,
    /// Speculative GETs or packed entries whose receipt says `cancelled`.
    #[serde(default)]
    pub get_cancelled: u64,
}

/// One key requested by a wrapper, including unsuccessful predictions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct KeyDemand {
    pub cache_key: String,
    pub first_demand_at_ms: u64,
    /// Wall time blocked on remote-check IPC, including daemon admission and
    /// failed requests. It is not an estimate of time saved by prefetch.
    #[serde(default)]
    pub remote_wait_ms: u64,
}

/// One compiler invocation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct TimelineUnit {
    pub cache_key: String,
    pub crate_name: String,
    /// Wrapper outcome as logged: `local_hit`, `prefetch_hit`, `remote_hit`,
    /// `dup`, `miss`, `error`, `passthrough`, `skipped`. A prefetched entry
    /// consumed from the local store logs `local_hit`.
    pub result: String,
    /// When the build started waiting for this unit.
    pub started_at_ms: u64,
    pub finished_at_ms: u64,
    #[serde(default)]
    pub compile_time_ms: u64,
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub key_ms: u64,
    #[serde(default)]
    pub lookup_ms: u64,
    #[serde(default)]
    pub restore_ms: u64,
    #[serde(default)]
    pub store_ms: u64,
    #[serde(default)]
    pub startup_ms: u64,
    #[serde(default)]
    pub flight_wait_ms: u64,
    #[serde(default)]
    pub permit_wait_ms: u64,
    #[serde(default)]
    pub compiler_runs: u32,
    /// Schema of the wrapper event this unit came from.
    #[serde(default)]
    pub event_schema: u32,
    /// Empty for wrapper events before schema 20, or invocations with no lookup.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub demands: Vec<KeyDemand>,
    /// The session's earliest speculative delivery of this unit's key, joined
    /// from transfer receipts when the record is assembled. A `local_hit`
    /// with `before_demand` timing is a prefetched local hit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefetch: Option<UnitPrefetch>,
}

/// A speculative download that delivered a unit's key in the same session.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct UnitPrefetch {
    /// Plan, source and candidate rank that scheduled the download.
    pub origin: PrefetchOrigin,
    /// When the GET started. A packed entry reports its pack's GET.
    pub started_at_ms: u64,
    /// When the entry finished importing into the local store.
    pub delivered_at_ms: u64,
    pub compressed_bytes: u64,
    pub timing: PrefetchTiming,
}

/// Where a delivery fell relative to the key's first demand in the session.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum PrefetchTiming {
    /// Imported at or before first demand.
    #[default]
    BeforeDemand,
    /// Started before first demand and finished after it.
    InFlight,
    /// Started at or after first demand.
    AfterDemand,
}

impl PrefetchTiming {
    /// Classify one delivery against the key's first demand.
    pub fn classify(started_at_ms: u64, delivered_at_ms: u64, first_demand_at_ms: u64) -> Self {
        if delivered_at_ms <= first_demand_at_ms {
            Self::BeforeDemand
        } else if started_at_ms < first_demand_at_ms {
            Self::InFlight
        } else {
            Self::AfterDemand
        }
    }
}

/// One remote transfer attributed to the session.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct TimelineTransfer {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accounting: Option<PrefetchAccounting>,
    pub cache_key: String,
    #[serde(default)]
    pub crate_name: String,
    pub direction: TransferDirection,
    pub ok: bool,
    #[serde(default)]
    pub compressed_bytes: u64,
    #[serde(default)]
    pub original_bytes: u64,
    pub started_at_ms: u64,
    pub finished_at_ms: u64,
    #[serde(default)]
    pub network_ms: u64,
    #[serde(default)]
    pub semaphore_wait_ms: u64,
    #[serde(default)]
    pub request_count: u32,
    #[serde(default)]
    pub import_ms: u64,
    pub attribution: TransferAttribution,
    /// Immutable origin captured when this candidate was scheduled. Absent
    /// on demand downloads and logs older than transfer schema 4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefetch: Option<PrefetchOrigin>,
    /// Operation/import outcome, including neutral `not_found`, `cancelled`,
    /// and `skipped`; empty for demand downloads, uploads, and old logs.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub outcome: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TransferDirection {
    Upload,
    #[default]
    Download,
}

/// Accounting for one physical backend operation. Nested entries describe
/// payload attribution; their bytes must not be added to the physical total.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PrefetchAccounting {
    pub operation: PrefetchOperation,
    /// True when compressed_bytes covers the complete received body. False
    /// means partial transport bytes are unavailable (including LIST bodies).
    pub bytes_complete: bool,
    /// Counts calls to the backend, not SDK retries or LIST pages.
    pub requests_complete: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub list_result_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<PackedEntryTransfer>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum PrefetchOperation {
    #[default]
    Get,
    List,
}

/// One compressed entry frame within a physical pack body. A successful local
/// import makes the entry available at finished_at_ms; zero means no import.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PackedEntryTransfer {
    pub cache_key: String,
    pub crate_name: String,
    pub compressed_bytes: u64,
    pub finished_at_ms: u64,
    pub outcome: String,
    pub prefetch: PrefetchOrigin,
}

/// The plan that scheduled one speculative download. Kept with the task,
/// so a later build cannot claim a download that was already in flight.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PrefetchOrigin {
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    /// Planner-issued ID when available; empty for fallback and unscoped work.
    pub plan_id: String,
    /// `advisory`, `fallback`, or `unscoped` (startup or direct requests).
    #[serde(default)]
    pub source: String,
    /// Zero-based order in the request list, before local-hit, in-flight,
    /// and budget filtering. Planner requests validate their list first;
    /// direct IPC requests can include rejected keys. None for keys added
    /// by whole-remote warming.
    #[serde(default)]
    pub candidate_rank: Option<u64>,
    #[serde(default)]
    pub candidate_source: crate::CandidateSource,
}

/// How the client tied a transfer to this session.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TransferAttribution {
    /// The download task carries this session's immutable plan origin.
    Session,
    /// Same cache key as one of the session's units.
    Key,
    /// No key match; its time overlaps this session and no other.
    #[default]
    Window,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> BuildTimeline {
        BuildTimeline {
            schema: BUILD_TIMELINE_SCHEMA,
            client_record_id: "0123456789abcdef".into(),
            session_id: "fedcba9876543210".into(),
            kache_version: "0.23.1".into(),
            started_at_ms: 1_000,
            finished_at_ms: 5_000,
            identity: TimelineIdentity {
                lock_digest: Some("aaaabbbbccccdddd".into()),
                identity_key: None,
                source: IdentitySource::Absent,
            },
            root_hash: "1111222233334444".into(),
            context: RunContext {
                repository: Some("org/repo".into()),
                labels: BTreeMap::from([("phase".into(), "cold".into())]),
                ..RunContext::default()
            },
            log: LogLimits {
                event_log_max_size: 10 << 20,
                event_log_keep_lines: 1000,
            },
            summary: None,
            units: vec![TimelineUnit {
                cache_key: "k1".into(),
                crate_name: "serde".into(),
                result: "local_hit".into(),
                demands: vec![KeyDemand {
                    cache_key: "k1".into(),
                    first_demand_at_ms: 1_050,
                    remote_wait_ms: 0,
                }],
                started_at_ms: 1_000,
                finished_at_ms: 1_200,
                prefetch: Some(UnitPrefetch {
                    origin: PrefetchOrigin {
                        session_id: "fedcba9876543210".into(),
                        plan_id: "p1".into(),
                        source: "advisory".into(),
                        candidate_rank: Some(3),
                        ..PrefetchOrigin::default()
                    },
                    started_at_ms: 900,
                    delivered_at_ms: 990,
                    compressed_bytes: 64,
                    timing: PrefetchTiming::BeforeDemand,
                }),
                ..TimelineUnit::default()
            }],
            transfers: vec![TimelineTransfer {
                cache_key: "k1".into(),
                direction: TransferDirection::Download,
                ok: true,
                started_at_ms: 900,
                finished_at_ms: 990,
                attribution: TransferAttribution::Key,
                ..TimelineTransfer::default()
            }],
        }
    }

    #[test]
    fn legacy_unit_has_no_demand_observation() {
        let unit: TimelineUnit = serde_json::from_str(
            r#"{"cache_key":"k","crate_name":"crate","result":"local_hit","started_at_ms":100,"finished_at_ms":200}"#,
        ).unwrap();
        assert!(unit.demands.is_empty());
        assert!(unit.prefetch.is_none());
    }

    #[test]
    fn prefetch_timing_splits_on_first_demand() {
        let classify = PrefetchTiming::classify;
        assert_eq!(classify(10, 99, 100), PrefetchTiming::BeforeDemand);
        assert_eq!(classify(10, 100, 100), PrefetchTiming::BeforeDemand);
        assert_eq!(classify(10, 101, 100), PrefetchTiming::InFlight);
        assert_eq!(classify(99, 150, 100), PrefetchTiming::InFlight);
        assert_eq!(classify(100, 150, 100), PrefetchTiming::AfterDemand);
        assert_eq!(classify(120, 150, 100), PrefetchTiming::AfterDemand);
    }

    #[test]
    fn unit_prefetch_serializes_timing_in_snake_case() {
        let unit = TimelineUnit {
            prefetch: Some(UnitPrefetch {
                timing: PrefetchTiming::InFlight,
                ..UnitPrefetch::default()
            }),
            ..TimelineUnit::default()
        };
        let json = serde_json::to_value(&unit).unwrap();
        assert_eq!(json["prefetch"]["timing"], "in_flight");
        let absent = serde_json::to_value(TimelineUnit::default()).unwrap();
        assert!(absent.get("prefetch").is_none());
    }

    #[test]
    fn record_round_trips_through_json() {
        let record = sample();
        let json = serde_json::to_string(&record).unwrap();
        assert_eq!(
            serde_json::from_str::<BuildTimeline>(&json).unwrap(),
            record
        );
    }

    #[test]
    fn empty_optional_context_is_not_serialized() {
        let json = serde_json::to_value(sample()).unwrap();
        let context = json["context"].as_object().unwrap();
        assert_eq!(
            context.keys().collect::<Vec<_>>(),
            vec!["labels", "repository"]
        );
        assert!(json.get("summary").is_none());
    }

    #[test]
    fn enums_use_snake_case_names() {
        let json = serde_json::to_value(sample()).unwrap();
        assert_eq!(json["identity"]["source"], "absent");
        assert_eq!(json["transfers"][0]["direction"], "download");
        assert_eq!(json["transfers"][0]["attribution"], "key");
        assert_eq!(
            serde_json::to_value(IdentitySource::LockEnv).unwrap(),
            "lock_env"
        );
    }

    #[test]
    fn unknown_identity_source_reads_as_absent() {
        let source: IdentitySource = serde_json::from_str("\"from_the_future\"").unwrap();
        assert_eq!(source, IdentitySource::Absent);
    }

    #[test]
    fn minimal_record_uses_defaults() {
        let record: BuildTimeline = serde_json::from_str(
            r#"{"schema":1,"client_record_id":"r","session_id":"s","started_at_ms":1,"finished_at_ms":2}"#,
        )
        .unwrap();
        assert!(record.units.is_empty());
        assert!(record.transfers.is_empty());
        assert_eq!(record.identity.source, IdentitySource::Absent);
    }
}
