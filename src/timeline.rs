//! Assemble build timeline records from the logs a build already wrote.
//!
//! Everything here is a pure function over log contents plus a snapshot of the
//! environment: the wrapper and daemon are untouched, and `kache telemetry
//! push` is the only caller.
//!
//! Events group by the wrapper's session id. Prefetch transfers use the
//! immutable session stamped by the daemon. Older transfers need a unique
//! key or time-window match; ambiguous transfers are left unattributed.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

use kache_core::timeline::{
    BUILD_TIMELINE_SCHEMA, BuildTimeline, IdentitySource, LogLimits, PrefetchOperation, RunContext,
    TimelineIdentity, TimelineSummary, TimelineTransfer, TimelineUnit, TransferAttribution,
    TransferDirection,
};

use crate::daemon::{TransferDirection as LoggedDirection, TransferEvent};
use crate::events::{BuildEvent, BuildSummaryEvent};

/// How far outside a session's own compiles a transfer may sit and still count
/// as part of it. Prefetch starts before the first compile logs and can still
/// be running after the last one.
pub(crate) const TRANSFER_SLACK_MS: u64 = 60_000;

/// Environment values a record is allowed to carry, read once by the caller.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct EnvSnapshot {
    pub repository: Option<String>,
    pub workflow: Option<String>,
    pub job: Option<String>,
    pub run_id: Option<String>,
    pub run_attempt: Option<String>,
    pub event: Option<String>,
    pub git_ref: Option<String>,
    pub commit: Option<String>,
    pub runner_os: Option<String>,
    pub runner_arch: Option<String>,
    pub runner_pool: Option<String>,
    pub manifest_key: Option<String>,
    pub target: Option<String>,
    pub profile: Option<String>,
}

impl EnvSnapshot {
    /// Read the allowlisted variables. Nothing else in the environment can
    /// reach a record.
    pub(crate) fn from_env() -> Self {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// The allowlist and the value rules, over any source of variables. A
    /// value is trimmed, and a blank one counts as unset: CI exports empty
    /// strings for fields it has no value for.
    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Self {
        let read = |name: &str| {
            lookup(name)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        };
        Self {
            repository: read("GITHUB_REPOSITORY"),
            workflow: read("GITHUB_WORKFLOW"),
            job: read("GITHUB_JOB"),
            run_id: read("GITHUB_RUN_ID"),
            run_attempt: read("GITHUB_RUN_ATTEMPT"),
            event: read("GITHUB_EVENT_NAME"),
            git_ref: read("GITHUB_REF"),
            commit: read("GITHUB_SHA"),
            runner_os: read("RUNNER_OS"),
            runner_arch: read("RUNNER_ARCH"),
            runner_pool: read("KACHE_RUNNER_POOL"),
            manifest_key: read("KACHE_MANIFEST_KEY"),
            target: read("KACHE_TARGET"),
            profile: read("KACHE_PROFILE").or_else(|| read("PROFILE")),
        }
    }

    fn context(&self, labels: BTreeMap<String, String>) -> RunContext {
        RunContext {
            repository: self.repository.clone(),
            workflow: self.workflow.clone(),
            job: self.job.clone(),
            run_id: self.run_id.clone(),
            run_attempt: self.run_attempt.clone(),
            event: self.event.clone(),
            git_ref: self.git_ref.clone(),
            commit: self.commit.clone(),
            runner_os: self.runner_os.clone(),
            runner_arch: self.runner_arch.clone(),
            runner_pool: self.runner_pool.clone(),
            labels,
        }
    }
}

/// Everything a record is built from.
pub(crate) struct TimelineInputs<'a> {
    pub events: &'a [BuildEvent],
    pub transfers: &'a [TransferEvent],
    pub summaries: &'a [BuildSummaryEvent],
    pub env: &'a EnvSnapshot,
    pub labels: BTreeMap<String, String>,
    pub log: LogLimits,
    pub kache_version: String,
}

/// One session's events, before transfers are attached.
#[derive(Debug, Default)]
struct SessionEvents {
    units: Vec<TimelineUnit>,
    roots: HashMap<String, usize>,
    started_at_ms: u64,
    finished_at_ms: u64,
}

impl SessionEvents {
    /// The root most of the session's compiles named. A session belongs to one
    /// build tree; a tie is resolved by name so the choice is deterministic.
    fn root(&self) -> Option<&str> {
        self.roots
            .iter()
            .max_by(|(left_root, left), (right_root, right)| {
                left.cmp(right).then_with(|| right_root.cmp(left_root))
            })
            .map(|(root, _)| root.as_str())
    }
}

/// Build one record per session found in `events`, oldest first.
pub(crate) fn build_timelines(inputs: &TimelineInputs<'_>) -> Vec<BuildTimeline> {
    let sessions = group_sessions(inputs.events);
    let windows: Vec<(String, u64, u64)> = sessions
        .iter()
        .map(|(id, session)| (id.clone(), session.started_at_ms, session.finished_at_ms))
        .collect();
    let summaries = summaries_by_session(inputs.summaries);
    let key_sessions = sessions.iter().fold(
        HashMap::<&str, HashSet<&str>>::new(),
        |mut owners, (id, session)| {
            for unit in &session.units {
                if !unit.cache_key.is_empty() {
                    owners.entry(&unit.cache_key).or_default().insert(id);
                }
            }
            owners
        },
    );
    let transfer_owners: Vec<_> = inputs
        .transfers
        .iter()
        .map(|transfer| transfer_owner(transfer, &windows, &key_sessions))
        .collect();

    let mut records: Vec<BuildTimeline> = sessions
        .into_iter()
        .map(|(session_id, session)| {
            let root = session.root().unwrap_or_default().to_string();
            let identity = identity_for_root(Path::new(&root), inputs.env);
            let transfers = attribute_transfers(&session_id, inputs.transfers, &transfer_owners);
            let context = inputs.env.context(inputs.labels.clone());
            let summary = {
                let base = summaries.get(&session_id).map(summary_projection);
                let has_prefetch = transfers.iter().any(|transfer| {
                    transfer.prefetch.is_some()
                        || transfer
                            .accounting
                            .as_ref()
                            .is_some_and(|accounting| !accounting.entries.is_empty())
                });
                match (base, has_prefetch) {
                    (None, false) => None,
                    (base, _) => Some(overlay_prefetch_join(
                        base.unwrap_or_default(),
                        &session.units,
                        &transfers,
                    )),
                }
            };
            BuildTimeline {
                schema: BUILD_TIMELINE_SCHEMA,
                client_record_id: client_record_id(&session_id, &context, session.started_at_ms),
                session_id: session_id.clone(),
                kache_version: inputs.kache_version.clone(),
                started_at_ms: session.started_at_ms,
                finished_at_ms: session.finished_at_ms,
                identity,
                root_hash: short_hash(&root),
                context,
                log: inputs.log.clone(),
                summary,
                units: session.units,
                transfers,
            }
        })
        .collect();
    records.sort_by_key(|record| (record.started_at_ms, record.session_id.clone()));
    records
}

/// Group compiles by the session the wrapper stamped on them. Events without a
/// session id come from a wrapper too old to stamp one and are skipped.
fn group_sessions(events: &[BuildEvent]) -> BTreeMap<String, SessionEvents> {
    let mut sessions: BTreeMap<String, SessionEvents> = BTreeMap::new();
    for event in events {
        if event.session_id.is_empty() {
            continue;
        }
        let finished_at_ms = event_finished_ms(event);
        let started_at_ms = finished_at_ms.saturating_sub(event.elapsed_ms);
        let session = sessions.entry(event.session_id.clone()).or_default();
        if session.units.is_empty() {
            session.started_at_ms = started_at_ms;
            session.finished_at_ms = finished_at_ms;
        } else {
            session.started_at_ms = session.started_at_ms.min(started_at_ms);
            session.finished_at_ms = session.finished_at_ms.max(finished_at_ms);
        }
        if !event.root.is_empty() {
            *session.roots.entry(event.root.clone()).or_default() += 1;
        }
        session.units.push(TimelineUnit {
            cache_key: event.cache_key.clone(),
            crate_name: event.crate_name.clone(),
            result: event.result.to_string(),
            started_at_ms,
            finished_at_ms,
            compile_time_ms: event.compile_time_ms,
            size: event.size,
            key_ms: event.key_ms,
            lookup_ms: event.lookup_ms,
            restore_ms: event.restore_ms,
            store_ms: event.store_ms,
            startup_ms: event.startup_ms,
            flight_wait_ms: event.flight_wait_ms,
            permit_wait_ms: event.permit_wait_ms,
            compiler_runs: event.compiler_runs,
            event_schema: event.schema,
            demands: event.demands.clone(),
        });
    }
    for session in sessions.values_mut() {
        session
            .units
            .sort_by_key(|unit| (unit.started_at_ms, unit.cache_key.clone()));
    }
    sessions
}

/// The wrapper stamps an event when the invocation ends.
fn event_finished_ms(event: &BuildEvent) -> u64 {
    u64::try_from(event.ts.timestamp_millis()).unwrap_or(0)
}

/// Assign each transfer once. An explicit session is authoritative; older
/// logs need a unique key owner inside the time window, or a unique window.
fn transfer_owner(
    transfer: &TransferEvent,
    windows: &[(String, u64, u64)],
    key_sessions: &HashMap<&str, HashSet<&str>>,
) -> Option<(String, TransferAttribution)> {
    if transfer.started_at_unix_ms == 0 {
        return None;
    }
    if let Some(origin) = &transfer.prefetch
        && !origin.session_id.is_empty()
    {
        return Some((origin.session_id.clone(), TransferAttribution::Session));
    }
    let eligible: Vec<_> = windows
        .iter()
        .filter(|(_, start, finish)| in_window(transfer, *start, *finish))
        .collect();
    let keyed: Vec<_> = eligible
        .iter()
        .filter(|(id, _, _)| {
            key_sessions
                .get(transfer.cache_key.as_str())
                .is_some_and(|owners| owners.contains(id.as_str()))
        })
        .collect();
    if let [owner] = keyed.as_slice() {
        return Some((owner.0.clone(), TransferAttribution::Key));
    }
    if keyed.is_empty()
        && let [owner] = eligible.as_slice()
    {
        return Some((owner.0.clone(), TransferAttribution::Window));
    }
    None
}

fn attribute_transfers(
    session_id: &str,
    transfers: &[TransferEvent],
    owners: &[Option<(String, TransferAttribution)>],
) -> Vec<TimelineTransfer> {
    let mut attributed: Vec<TimelineTransfer> = transfers
        .iter()
        .zip(owners)
        .filter_map(|(transfer, owner)| {
            let (owner_id, attribution) = owner.as_ref()?;
            if owner_id != session_id {
                return None;
            }
            Some(TimelineTransfer {
                accounting: transfer.accounting.clone(),
                cache_key: transfer.cache_key.clone(),
                crate_name: transfer.crate_name.clone(),
                direction: match transfer.direction {
                    LoggedDirection::Upload => TransferDirection::Upload,
                    LoggedDirection::Download => TransferDirection::Download,
                },
                ok: transfer.ok,
                compressed_bytes: transfer.compressed_bytes,
                original_bytes: transfer.original_bytes,
                started_at_ms: transfer.started_at_unix_ms,
                finished_at_ms: transfer.finished_at_unix_ms,
                network_ms: transfer.network_ms,
                semaphore_wait_ms: transfer.semaphore_wait_ms,
                request_count: transfer.request_count,
                import_ms: transfer.import_ms,
                attribution: *attribution,
                prefetch: transfer.prefetch.clone(),
                outcome: transfer.outcome.clone(),
            })
        })
        .collect();
    attributed.sort_by_key(|transfer| (transfer.started_at_ms, transfer.cache_key.clone()));
    attributed
}

/// Whether a transfer overlaps a session window widened by the slack.
fn in_window(transfer: &TransferEvent, started_at_ms: u64, finished_at_ms: u64) -> bool {
    let from = started_at_ms.saturating_sub(TRANSFER_SLACK_MS);
    let to = finished_at_ms.saturating_add(TRANSFER_SLACK_MS);
    transfer.started_at_unix_ms <= to
        && transfer
            .finished_at_unix_ms
            .max(transfer.started_at_unix_ms)
            >= from
}

fn summaries_by_session(summaries: &[BuildSummaryEvent]) -> HashMap<String, &BuildSummaryEvent> {
    let mut latest: HashMap<String, &BuildSummaryEvent> = HashMap::new();
    for summary in summaries {
        if summary.session_id.is_empty() {
            continue;
        }
        latest
            .entry(summary.session_id.clone())
            .and_modify(|held| {
                if summary.last_activity_ms >= held.last_activity_ms {
                    *held = summary;
                }
            })
            .or_insert(summary);
    }
    latest
}

struct KeyConsumption {
    first_demand_at_ms: u64,
    consumed: bool,
}

fn overlay_prefetch_join(
    mut summary: TimelineSummary,
    units: &[TimelineUnit],
    transfers: &[TimelineTransfer],
) -> TimelineSummary {
    let mut keys: HashMap<&str, KeyConsumption> = HashMap::new();
    let mut remote_wait_ms: u64 = 0;
    for unit in units {
        remote_wait_ms = remote_wait_ms.saturating_add(
            unit.demands
                .iter()
                .fold(0, |sum, demand| sum.saturating_add(demand.remote_wait_ms)),
        );
        if unit.cache_key.is_empty() {
            continue;
        }
        let demand_ms = unit
            .demands
            .iter()
            .filter(|demand| demand.cache_key == unit.cache_key)
            .map(|demand| demand.first_demand_at_ms)
            .min()
            .unwrap_or(unit.started_at_ms);
        let consumed = unit.result == "local_hit" || unit.result == "prefetch_hit";
        keys.entry(&unit.cache_key)
            .and_modify(|held| {
                held.first_demand_at_ms = held.first_demand_at_ms.min(demand_ms);
                held.consumed = held.consumed || consumed;
            })
            .or_insert(KeyConsumption {
                first_demand_at_ms: demand_ms,
                consumed,
            });
    }
    summary.remote_wait_ms = remote_wait_ms;

    let mut consumed_keys: u64 = 0;
    let mut consumed_bytes: u64 = 0;
    let mut useful_keys: u64 = 0;
    let mut useful_bytes: u64 = 0;
    let mut get_not_found: u64 = 0;
    let mut get_errors: u64 = 0;
    for transfer in transfers {
        if transfer.direction != TransferDirection::Download {
            continue;
        }
        for (cache_key, bytes, finished_at_ms, outcome) in prefetch_payloads(transfer) {
            let packed = transfer
                .accounting
                .as_ref()
                .is_some_and(|accounting| !accounting.entries.is_empty());
            if outcome == "not_found" {
                get_not_found += 1;
            } else if payload_errored(outcome, packed, transfer.ok) {
                get_errors += 1;
            }
            if !payload_delivered(outcome, packed, transfer.ok) {
                continue;
            }
            // No import time means the entry never landed in the local store.
            if finished_at_ms == 0 {
                continue;
            }
            let Some(consumption) = keys.get(cache_key.as_str()) else {
                continue;
            };
            if !consumption.consumed {
                continue;
            }
            consumed_keys += 1;
            consumed_bytes = consumed_bytes.saturating_add(bytes);
            if finished_at_ms <= consumption.first_demand_at_ms {
                useful_keys += 1;
                useful_bytes = useful_bytes.saturating_add(bytes);
            }
        }
    }
    summary.consumed_prefetch_keys = consumed_keys;
    summary.consumed_prefetch_bytes = consumed_bytes;
    summary.useful_prefetch_keys = useful_keys;
    summary.useful_prefetch_bytes = useful_bytes;
    summary.get_not_found = get_not_found;
    summary.get_errors = get_errors;
    summary
}

/// Whether one payload counts as a failed GET. A packed entry carries its own
/// outcome; a plain GET reports failure on the transfer, and `cancelled` and
/// `skipped` are neutral rather than errors.
fn payload_errored(outcome: &str, packed: bool, transfer_ok: bool) -> bool {
    if outcome == "not_found" {
        return false;
    }
    if packed {
        outcome == "error"
    } else {
        !transfer_ok && outcome != "cancelled" && outcome != "skipped"
    }
}

/// Whether one payload actually put its entry in the local store. A GET that
/// was not found, errored, was cancelled or was skipped delivered nothing,
/// even when the same key later shows up as a local hit that an earlier build
/// stored. Counting those would credit prefetch for work it never did.
fn payload_delivered(outcome: &str, packed: bool, transfer_ok: bool) -> bool {
    !matches!(outcome, "not_found" | "cancelled" | "skipped" | "error")
        && !payload_errored(outcome, packed, transfer_ok)
}

fn prefetch_payloads(transfer: &TimelineTransfer) -> Vec<(String, u64, u64, &str)> {
    if let Some(accounting) = &transfer.accounting {
        if matches!(accounting.operation, PrefetchOperation::List) {
            return Vec::new();
        }
        if !accounting.entries.is_empty() {
            return accounting
                .entries
                .iter()
                .map(|entry| {
                    (
                        entry.cache_key.clone(),
                        entry.compressed_bytes,
                        entry.finished_at_ms,
                        entry.outcome.as_str(),
                    )
                })
                .collect();
        }
    }
    if transfer.prefetch.is_none() {
        return Vec::new();
    }
    vec![(
        transfer.cache_key.clone(),
        transfer.compressed_bytes,
        transfer.finished_at_ms,
        transfer.outcome.as_str(),
    )]
}

fn summary_projection(summary: &&BuildSummaryEvent) -> TimelineSummary {
    TimelineSummary {
        incomplete: summary.incomplete,
        plan_id: summary.plan_id.clone(),
        plan_source: summary.plan_source.clone(),
        closure_reason: summary.closure_reason.clone(),
        started_at_ms: summary.started_at_ms,
        last_activity_ms: summary.last_activity_ms,
        candidate_keys: summary.candidate_keys,
        downloaded_keys: summary.downloaded_keys,
        downloaded_bytes: summary.downloaded_bytes,
        used_keys: summary.used_keys,
        demanded_keys: summary.demanded_keys,
        demanded_candidate_keys: summary.demanded_candidate_keys,
        cancelled: summary.cancelled,
        ..TimelineSummary::default()
    }
}

/// What build this was.
///
/// An explicit manifest key wins, as it does on the prefetch path. Otherwise a
/// full identity key needs a profile from the environment: without one, debug
/// and release would share an identity and their timings would be averaged
/// together. The lockfile digest is recorded either way.
fn identity_for_root(root: &Path, env: &EnvSnapshot) -> TimelineIdentity {
    let lock_digest = lock_digest_for_root(root);
    if let Some(explicit) = env.manifest_key.clone() {
        return TimelineIdentity {
            lock_digest,
            identity_key: Some(explicit),
            source: IdentitySource::Explicit,
        };
    }
    let target = env
        .target
        .clone()
        .unwrap_or_else(crate::identity::host_target_triple);
    let identity_key = env
        .profile
        .as_deref()
        .and_then(|profile| crate::identity::identity_key(&lock_path(root), &target, profile));
    let source = if identity_key.is_some() {
        IdentitySource::LockEnv
    } else {
        IdentitySource::Absent
    };
    TimelineIdentity {
        lock_digest,
        identity_key,
        source,
    }
}

fn lock_path(root: &Path) -> PathBuf {
    root.join("Cargo.lock")
}

fn lock_digest_for_root(root: &Path) -> Option<String> {
    if root.as_os_str().is_empty() {
        return None;
    }
    crate::identity::lockfile_digest(&lock_path(root))
}

/// Identifies one session from one run, so a second push of the same build
/// replaces the first instead of adding a second record.
fn client_record_id(session_id: &str, context: &RunContext, started_at_ms: u64) -> String {
    let mut hasher = blake3::Hasher::new();
    for part in [
        session_id,
        context.run_id.as_deref().unwrap_or_default(),
        context.run_attempt.as_deref().unwrap_or_default(),
        context.job.as_deref().unwrap_or_default(),
    ] {
        hasher.update(part.as_bytes());
        hasher.update(b"\0");
    }
    hasher.update(&started_at_ms.to_le_bytes());
    hasher.finalize().to_hex().as_str()[..16].to_string()
}

/// Truncated hash of a path, so records can be grouped by build tree without
/// carrying anyone's directory layout.
fn short_hash(value: &str) -> String {
    if value.is_empty() {
        return String::new();
    }
    blake3::hash(value.as_bytes()).to_hex().as_str()[..16].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::TransferEvent;
    use crate::events::EventResult;
    use chrono::TimeZone;

    fn event(
        session: &str,
        crate_name: &str,
        key: &str,
        end_ms: i64,
        elapsed_ms: u64,
    ) -> BuildEvent {
        let mut event = BuildEvent::new_for_test(crate_name, EventResult::LocalHit);
        event.session_id = session.to_string();
        event.cache_key = key.to_string();
        event.root = "/w".to_string();
        event.ts = chrono::Utc.timestamp_millis_opt(end_ms).unwrap();
        event.elapsed_ms = elapsed_ms;
        event
    }

    fn transfer(key: &str, started: u64, finished: u64) -> TransferEvent {
        TransferEvent {
            accounting: None,
            prefetch: None,
            outcome: String::new(),
            schema: 3,
            crate_name: "serde".to_string(),
            direction: LoggedDirection::Download,
            format: String::new(),
            cache_key: key.to_string(),
            object_key: String::new(),
            compressed_bytes: 10,
            started_at_unix_ms: started,
            finished_at_unix_ms: finished,
            elapsed_ms: finished.saturating_sub(started),
            network_ms: 1,
            semaphore_wait_ms: 0,
            head_ms: 0,
            request_ms: 0,
            body_ms: 0,
            request_count: 1,
            original_bytes: 20,
            decompress_ms: 0,
            extract_ms: 0,
            disk_io_ms: 0,
            import_lock_wait_ms: 0,
            import_ms: 0,
            compression_ms: 0,
            head_checks_ms: 0,
            blobs_skipped: 0,
            blobs_total: 1,
            ok: true,
            timestamp: finished / 1000,
        }
    }

    fn inputs<'a>(
        events: &'a [BuildEvent],
        transfers: &'a [TransferEvent],
        summaries: &'a [BuildSummaryEvent],
        env: &'a EnvSnapshot,
    ) -> TimelineInputs<'a> {
        TimelineInputs {
            events,
            transfers,
            summaries,
            env,
            labels: BTreeMap::new(),
            log: LogLimits {
                event_log_max_size: 10 << 20,
                event_log_keep_lines: 1000,
            },
            kache_version: "0.23.1".to_string(),
        }
    }

    #[test]
    fn one_record_per_session_with_unit_times_from_the_events() {
        let events = [
            event("s1", "serde", "k1", 5_000, 500),
            event("s2", "syn", "k2", 9_000, 1_000),
            event("s1", "quote", "k3", 6_000, 200),
        ];
        let env = EnvSnapshot::default();
        let records = build_timelines(&inputs(&events, &[], &[], &env));

        assert_eq!(
            records
                .iter()
                .map(|r| r.session_id.as_str())
                .collect::<Vec<_>>(),
            ["s1", "s2"]
        );
        let first = &records[0];
        assert_eq!(first.started_at_ms, 4_500);
        assert_eq!(first.finished_at_ms, 6_000);
        assert_eq!(first.schema, BUILD_TIMELINE_SCHEMA);
        assert_eq!(first.kache_version, "0.23.1");
        assert_eq!(
            first
                .units
                .iter()
                .map(|u| u.crate_name.as_str())
                .collect::<Vec<_>>(),
            ["serde", "quote"]
        );
        assert_eq!(first.units[0].started_at_ms, 4_500);
        assert_eq!(first.units[0].finished_at_ms, 5_000);
        assert_eq!(first.units[0].result, "local_hit");
    }

    #[test]
    fn packed_accounting_survives_timeline_projection_without_duplicating_bytes() {
        let mut pack = transfer("", 1_000, 2_000);
        pack.prefetch = Some(kache_core::timeline::PrefetchOrigin {
            session_id: "s1".into(),
            ..Default::default()
        });
        pack.compressed_bytes = 150;
        pack.accounting = Some(kache_core::timeline::PrefetchAccounting {
            bytes_complete: true,
            requests_complete: true,
            entries: vec![kache_core::timeline::PackedEntryTransfer {
                cache_key: "k1".into(),
                compressed_bytes: 100,
                finished_at_ms: 1_900,
                outcome: "completed".into(),
                ..Default::default()
            }],
            ..Default::default()
        });
        let expected = pack.accounting.clone();
        let records = build_timelines(&inputs(
            &[event("s1", "serde", "k1", 5_000, 100)],
            &[pack],
            &[],
            &EnvSnapshot::default(),
        ));
        assert_eq!(records[0].transfers.len(), 1);
        assert_eq!(records[0].transfers[0].compressed_bytes, 150);
        assert_eq!(records[0].transfers[0].accounting, expected);
    }

    #[test]
    fn a_prefetch_that_lands_before_demand_is_useful() {
        let mut observed = event("s1", "serde", "k1", 5_000, 500);
        observed.demands = vec![kache_core::timeline::KeyDemand {
            cache_key: "k1".to_string(),
            first_demand_at_ms: 4_750,
            remote_wait_ms: 12,
        }];
        let mut downloaded = transfer("k1", 1_000, 2_000);
        downloaded.prefetch = Some(kache_core::timeline::PrefetchOrigin {
            session_id: "s1".into(),
            source: "advisory".into(),
            ..Default::default()
        });
        downloaded.outcome = "completed".into();
        downloaded.compressed_bytes = 80;
        let records = build_timelines(&inputs(
            &[observed],
            &[downloaded],
            &[],
            &EnvSnapshot::default(),
        ));
        let summary = records[0].summary.as_ref().expect("join summary");
        assert_eq!(summary.consumed_prefetch_keys, 1);
        assert_eq!(summary.consumed_prefetch_bytes, 80);
        assert_eq!(summary.useful_prefetch_keys, 1);
        assert_eq!(summary.useful_prefetch_bytes, 80);
        assert_eq!(summary.remote_wait_ms, 12);
        assert_eq!(summary.get_errors, 0);
    }

    #[test]
    fn a_prefetch_that_lands_after_demand_is_consumed_but_not_useful() {
        let mut observed = event("s1", "serde", "k1", 5_000, 500);
        observed.demands = vec![kache_core::timeline::KeyDemand {
            cache_key: "k1".to_string(),
            first_demand_at_ms: 1_500,
            remote_wait_ms: 40,
        }];
        let mut downloaded = transfer("k1", 1_000, 2_000);
        downloaded.prefetch = Some(kache_core::timeline::PrefetchOrigin {
            session_id: "s1".into(),
            source: "advisory".into(),
            ..Default::default()
        });
        downloaded.compressed_bytes = 80;
        let records = build_timelines(&inputs(
            &[observed],
            &[downloaded],
            &[],
            &EnvSnapshot::default(),
        ));
        let summary = records[0].summary.as_ref().expect("join summary");
        assert_eq!(summary.consumed_prefetch_keys, 1);
        assert_eq!(summary.consumed_prefetch_bytes, 80);
        assert_eq!(summary.useful_prefetch_keys, 0);
        assert_eq!(summary.useful_prefetch_bytes, 0);
        assert_eq!(summary.remote_wait_ms, 40);
    }

    #[test]
    fn packed_entry_bytes_are_joined_without_the_parent_get() {
        let mut pack = transfer("", 1_000, 2_000);
        pack.prefetch = Some(kache_core::timeline::PrefetchOrigin {
            session_id: "s1".into(),
            ..Default::default()
        });
        pack.compressed_bytes = 150;
        pack.accounting = Some(kache_core::timeline::PrefetchAccounting {
            bytes_complete: true,
            requests_complete: true,
            entries: vec![kache_core::timeline::PackedEntryTransfer {
                cache_key: "k1".into(),
                compressed_bytes: 100,
                finished_at_ms: 1_900,
                outcome: "completed".into(),
                ..Default::default()
            }],
            ..Default::default()
        });
        let records = build_timelines(&inputs(
            &[event("s1", "serde", "k1", 5_000, 100)],
            &[pack],
            &[],
            &EnvSnapshot::default(),
        ));
        let summary = records[0].summary.as_ref().expect("join summary");
        assert_eq!(summary.consumed_prefetch_keys, 1);
        assert_eq!(summary.consumed_prefetch_bytes, 100);
        assert_eq!(summary.useful_prefetch_keys, 1);
        assert_eq!(summary.useful_prefetch_bytes, 100);
    }

    #[test]
    fn a_prefetch_miss_is_not_consumed() {
        let mut observed = event("s1", "serde", "k1", 5_000, 500);
        observed.result = EventResult::Miss;
        let mut downloaded = transfer("k1", 1_000, 2_000);
        downloaded.prefetch = Some(kache_core::timeline::PrefetchOrigin {
            session_id: "s1".into(),
            source: "advisory".into(),
            ..Default::default()
        });
        downloaded.ok = false;
        downloaded.outcome = "error".into();
        let records = build_timelines(&inputs(
            &[observed],
            &[downloaded],
            &[],
            &EnvSnapshot::default(),
        ));
        let summary = records[0].summary.as_ref().expect("join summary");
        assert_eq!(summary.consumed_prefetch_keys, 0);
        assert_eq!(summary.useful_prefetch_keys, 0);
        assert_eq!(summary.get_errors, 1);
        assert_eq!(summary.get_not_found, 0);
    }

    #[test]
    fn a_unit_that_reports_prefetch_hit_consumes_its_key() {
        let mut observed = event("s1", "serde", "k1", 5_000, 500);
        observed.result = EventResult::PrefetchHit;
        let mut downloaded = transfer("k1", 1_000, 2_000);
        downloaded.prefetch = Some(kache_core::timeline::PrefetchOrigin {
            session_id: "s1".into(),
            source: "advisory".into(),
            ..Default::default()
        });
        downloaded.compressed_bytes = 80;
        let records = build_timelines(&inputs(
            &[observed],
            &[downloaded],
            &[],
            &EnvSnapshot::default(),
        ));
        let summary = records[0].summary.as_ref().expect("join summary");
        assert_eq!(summary.consumed_prefetch_keys, 1);
        assert_eq!(summary.consumed_prefetch_bytes, 80);
    }

    #[test]
    fn one_unit_consuming_a_shared_key_is_enough() {
        let missed = event("s1", "serde", "k1", 5_000, 500);
        let mut missed = missed;
        missed.result = EventResult::Miss;
        let mut hit = event("s1", "serde_derive", "k1", 6_000, 500);
        hit.result = EventResult::LocalHit;
        let mut downloaded = transfer("k1", 1_000, 2_000);
        downloaded.prefetch = Some(kache_core::timeline::PrefetchOrigin {
            session_id: "s1".into(),
            source: "advisory".into(),
            ..Default::default()
        });
        downloaded.compressed_bytes = 80;
        let records = build_timelines(&inputs(
            &[missed, hit],
            &[downloaded],
            &[],
            &EnvSnapshot::default(),
        ));
        let summary = records[0].summary.as_ref().expect("join summary");
        assert_eq!(summary.consumed_prefetch_keys, 1);
    }

    #[test]
    fn a_session_with_no_prefetch_at_all_gets_no_synthetic_summary() {
        let mut demand = transfer("k1", 1_000, 2_000);
        demand.accounting = Some(kache_core::timeline::PrefetchAccounting {
            bytes_complete: true,
            requests_complete: true,
            entries: Vec::new(),
            ..Default::default()
        });
        let records = build_timelines(&inputs(
            &[event("s1", "serde", "k1", 5_000, 500)],
            &[demand],
            &[],
            &EnvSnapshot::default(),
        ));
        // The transfer has to survive attribution, or this would pass for the
        // wrong reason and the empty-entries check would go untested.
        assert_eq!(records[0].transfers.len(), 1);
        assert!(records[0].summary.is_none());
    }

    #[test]
    fn a_packed_entry_that_never_imported_is_not_consumed() {
        let mut pack = transfer("", 1_000, 2_000);
        pack.prefetch = Some(kache_core::timeline::PrefetchOrigin {
            session_id: "s1".into(),
            ..Default::default()
        });
        pack.accounting = Some(kache_core::timeline::PrefetchAccounting {
            bytes_complete: true,
            requests_complete: true,
            entries: vec![kache_core::timeline::PackedEntryTransfer {
                cache_key: "k1".into(),
                compressed_bytes: 100,
                finished_at_ms: 0,
                outcome: "completed".into(),
                ..Default::default()
            }],
            ..Default::default()
        });
        let records = build_timelines(&inputs(
            &[event("s1", "serde", "k1", 5_000, 100)],
            &[pack],
            &[],
            &EnvSnapshot::default(),
        ));
        let summary = records[0].summary.as_ref().expect("join summary");
        assert_eq!(summary.consumed_prefetch_keys, 0);
        assert_eq!(summary.consumed_prefetch_bytes, 0);
        assert_eq!(summary.useful_prefetch_keys, 0);
    }

    #[test]
    fn a_prefetch_that_was_not_found_never_counts_as_delivered() {
        let mut observed = event("s1", "serde", "k1", 5_000, 500);
        observed.demands = vec![kache_core::timeline::KeyDemand {
            cache_key: "k1".to_string(),
            first_demand_at_ms: 4_750,
            remote_wait_ms: 0,
        }];
        let mut missing = transfer("k1", 1_000, 2_000);
        missing.prefetch = Some(kache_core::timeline::PrefetchOrigin {
            session_id: "s1".into(),
            source: "advisory".into(),
            ..Default::default()
        });
        missing.outcome = "not_found".into();
        missing.compressed_bytes = 0;
        let records = build_timelines(&inputs(
            &[observed],
            &[missing],
            &[],
            &EnvSnapshot::default(),
        ));
        let summary = records[0].summary.as_ref().expect("join summary");
        assert_eq!(summary.get_not_found, 1);
        assert_eq!(summary.consumed_prefetch_keys, 0);
        assert_eq!(summary.useful_prefetch_keys, 0);
    }

    #[test]
    fn an_errored_prefetch_never_counts_as_delivered() {
        let mut observed = event("s1", "serde", "k1", 5_000, 500);
        observed.demands = vec![kache_core::timeline::KeyDemand {
            cache_key: "k1".to_string(),
            first_demand_at_ms: 4_750,
            remote_wait_ms: 0,
        }];
        let mut failed = transfer("k1", 1_000, 2_000);
        failed.prefetch = Some(kache_core::timeline::PrefetchOrigin {
            session_id: "s1".into(),
            source: "advisory".into(),
            ..Default::default()
        });
        failed.ok = false;
        failed.outcome = "error".into();
        failed.compressed_bytes = 80;
        let records = build_timelines(&inputs(
            &[observed],
            &[failed],
            &[],
            &EnvSnapshot::default(),
        ));
        let summary = records[0].summary.as_ref().expect("join summary");
        assert_eq!(summary.get_errors, 1);
        assert_eq!(summary.consumed_prefetch_keys, 0);
        assert_eq!(summary.consumed_prefetch_bytes, 0);
        assert_eq!(summary.useful_prefetch_keys, 0);
        assert_eq!(summary.useful_prefetch_bytes, 0);
    }

    #[test]
    fn a_packed_entry_that_errored_never_counts_as_delivered() {
        let mut pack = transfer("", 1_000, 2_000);
        pack.prefetch = Some(kache_core::timeline::PrefetchOrigin {
            session_id: "s1".into(),
            ..Default::default()
        });
        pack.accounting = Some(kache_core::timeline::PrefetchAccounting {
            bytes_complete: true,
            requests_complete: true,
            entries: vec![kache_core::timeline::PackedEntryTransfer {
                cache_key: "k1".into(),
                compressed_bytes: 100,
                finished_at_ms: 1_900,
                outcome: "error".into(),
                ..Default::default()
            }],
            ..Default::default()
        });
        let records = build_timelines(&inputs(
            &[event("s1", "serde", "k1", 5_000, 100)],
            &[pack],
            &[],
            &EnvSnapshot::default(),
        ));
        let summary = records[0].summary.as_ref().expect("join summary");
        assert_eq!(summary.get_errors, 1);
        assert_eq!(summary.consumed_prefetch_keys, 0);
        assert_eq!(summary.useful_prefetch_keys, 0);
    }

    #[test]
    fn daemon_used_keys_stay_the_recorded_lower_bound() {
        let summary = crate::events::BuildSummaryEvent {
            ts: chrono::Utc.timestamp_millis_opt(6_000).unwrap(),
            schema: 1,
            incomplete: false,
            session_id: "s1".into(),
            root: "/w".into(),
            plan_source: "advisory".into(),
            plan_id: "p1".into(),
            closure_reason: "inactivity".into(),
            started_at_ms: 1_000,
            last_activity_ms: 5_000,
            candidate_keys: 1,
            downloaded_keys: 1,
            downloaded_bytes: 80,
            used_keys: 0,
            used_bytes: 0,
            demanded_keys: 0,
            demanded_candidate_keys: 0,
            cancelled: false,
            list_requests: 0,
            list_duration_ms: 0,
        };
        let mut downloaded = transfer("k1", 1_000, 2_000);
        downloaded.prefetch = Some(kache_core::timeline::PrefetchOrigin {
            session_id: "s1".into(),
            source: "advisory".into(),
            ..Default::default()
        });
        let records = build_timelines(&inputs(
            &[event("s1", "serde", "k1", 5_000, 500)],
            &[downloaded],
            std::slice::from_ref(&summary),
            &EnvSnapshot::default(),
        ));
        let joined = records[0].summary.as_ref().expect("join summary");
        assert_eq!(joined.used_keys, 0);
        assert_eq!(joined.consumed_prefetch_keys, 1);
    }

    #[test]
    fn demand_observations_survive_timeline_projection() {
        let mut observed = event("s1", "serde", "final", 5_000, 500);
        observed.demands = vec![
            kache_core::timeline::KeyDemand {
                cache_key: "provisional".to_string(),
                first_demand_at_ms: 4_625,
                remote_wait_ms: 72,
            },
            kache_core::timeline::KeyDemand {
                cache_key: "final".to_string(),
                first_demand_at_ms: 4_750,
                remote_wait_ms: 0,
            },
        ];
        let expected = observed.demands.clone();
        let legacy = event("s1", "syn", "legacy", 6_000, 100);
        let records = build_timelines(&inputs(
            &[observed, legacy],
            &[],
            &[],
            &EnvSnapshot::default(),
        ));
        assert_eq!(records[0].units[0].demands, expected);
        assert!(records[0].units[1].demands.is_empty());
    }

    #[test]
    fn events_without_a_session_are_skipped() {
        let mut orphan = event("", "serde", "k1", 1_000, 10);
        orphan.session_id.clear();
        let records = build_timelines(&inputs(
            &[orphan, event("s1", "syn", "k2", 2_000, 10)],
            &[],
            &[],
            &EnvSnapshot::default(),
        ));
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].session_id, "s1");
    }

    #[test]
    fn a_download_of_a_compiled_key_belongs_to_that_session() {
        // Arrived before the build asked for it, and consumed as a local hit:
        // the case daemon-side counters cannot see.
        let events = [event("s1", "serde", "k1", 5_000, 500)];
        let transfers = [transfer("k1", 1_000, 2_000)];
        let records = build_timelines(&inputs(&events, &transfers, &[], &EnvSnapshot::default()));

        assert_eq!(records[0].transfers.len(), 1);
        let attached = &records[0].transfers[0];
        assert_eq!(attached.attribution, TransferAttribution::Key);
        assert_eq!(attached.finished_at_ms, 2_000);
        assert!(
            attached.finished_at_ms < records[0].units[0].started_at_ms,
            "this is what makes it an on-time prefetch"
        );
    }

    #[test]
    fn a_download_of_an_uncompiled_key_is_attributed_by_time() {
        let events = [event("s1", "serde", "k1", 5_000, 500)];
        let transfers = [transfer("k-unused", 4_000, 4_500)];
        let records = build_timelines(&inputs(&events, &transfers, &[], &EnvSnapshot::default()));
        assert_eq!(
            records[0].transfers[0].attribution,
            TransferAttribution::Window
        );
    }

    #[test]
    fn a_download_two_sessions_could_claim_is_dropped() {
        let events = [
            event("s1", "serde", "k1", 5_000, 500),
            event("s2", "syn", "k2", 5_200, 500),
        ];
        let transfers = [transfer("k-unused", 4_800, 4_900)];
        let records = build_timelines(&inputs(&events, &transfers, &[], &EnvSnapshot::default()));
        assert!(
            records.iter().all(|record| record.transfers.is_empty()),
            "an ambiguous transfer must not be handed to either session"
        );

        // The same transfer, with a key one session compiled, is no longer
        // ambiguous.
        let keyed = [transfer("k1", 4_800, 4_900)];
        let records = build_timelines(&inputs(&events, &keyed, &[], &EnvSnapshot::default()));
        assert_eq!(records[0].transfers.len(), 1);
        assert!(records[1].transfers.is_empty());
    }

    #[test]
    fn explicit_origin_wins_over_shared_keys_and_time_windows() {
        let events = [
            event("s1", "serde", "k1", 500_000, 500),
            event("s2", "serde", "k1", 500_200, 500),
        ];
        let origin = kache_core::timeline::PrefetchOrigin {
            session_id: "s2".to_string(),
            plan_id: "plan-2".to_string(),
            source: "advisory".to_string(),
            candidate_rank: Some(7),
            candidate_source: kache_core::CandidateSource::Shard,
        };
        let mut downloaded = transfer("k1", 1, 2);
        downloaded.prefetch = Some(origin.clone());
        downloaded.outcome = "completed".to_string();
        let records = build_timelines(&inputs(
            &events,
            &[downloaded.clone()],
            &[],
            &EnvSnapshot::default(),
        ));
        assert!(records[0].transfers.is_empty());
        let attached = &records[1].transfers[0];
        assert_eq!(attached.attribution, TransferAttribution::Session);
        assert_eq!(attached.prefetch, Some(origin));
        assert_eq!(attached.outcome, "completed");
        assert_eq!(attached.compressed_bytes, 10);
        assert_eq!(attached.finished_at_ms, 2);
        assert_eq!(records[1].units[0].result, "local_hit");

        // An owner outside the selected logs is not reassigned to a nearby build.
        downloaded.prefetch.as_mut().unwrap().session_id = "missing".to_string();
        let records = build_timelines(&inputs(
            &events,
            &[downloaded],
            &[],
            &EnvSnapshot::default(),
        ));
        assert!(records.iter().all(|record| record.transfers.is_empty()));
    }

    #[test]
    fn empty_origin_session_uses_legacy_matching() {
        let events = [event("s1", "serde", "k1", 5_000, 500)];
        let mut downloaded = transfer("k1", 4_000, 4_100);
        downloaded.prefetch = Some(kache_core::timeline::PrefetchOrigin::default());
        let records = build_timelines(&inputs(
            &events,
            &[downloaded],
            &[],
            &EnvSnapshot::default(),
        ));
        assert_eq!(
            records[0].transfers[0].attribution,
            TransferAttribution::Key
        );
    }

    #[test]
    fn missing_keys_do_not_disambiguate_overlapping_sessions() {
        let events = [
            event("s1", "serde", "", 5_000, 500),
            event("s2", "syn", "k2", 5_100, 500),
        ];
        let transfers = [transfer("", 4_500, 4_600)];
        let records = build_timelines(&inputs(&events, &transfers, &[], &EnvSnapshot::default()));
        assert!(records.iter().all(|record| record.transfers.is_empty()));
    }

    #[test]
    fn same_key_in_two_sessions_does_not_duplicate_a_transfer() {
        let events = [
            event("s1", "serde", "k1", 5_000, 500),
            event("s2", "serde", "k1", 5_200, 500),
        ];
        let transfers = [transfer("k1", 4_000, 4_100)];
        let records = build_timelines(&inputs(&events, &transfers, &[], &EnvSnapshot::default()));
        assert!(records.iter().all(|record| record.transfers.is_empty()));
    }

    #[test]
    fn same_key_transfer_outside_the_session_window_is_not_reused() {
        let events = [event("s1", "serde", "k1", 500_000, 500)];
        let transfers = [transfer("k1", 1_000, 2_000)];
        let records = build_timelines(&inputs(&events, &transfers, &[], &EnvSnapshot::default()));
        assert!(records[0].transfers.is_empty());
    }

    #[test]
    fn transfers_far_outside_the_session_are_dropped() {
        // The session ran at t=500s; these downloads are minutes away from it.
        let events = [event("s1", "serde", "k1", 500_000, 500)];
        let early = [transfer("k-unused", 1_000, 2_000)];
        let records = build_timelines(&inputs(&events, &early, &[], &EnvSnapshot::default()));
        assert!(
            records[0].transfers.is_empty(),
            "a download {}s before the build is not part of it",
            (500_000 - 2_000) / 1000
        );

        let late = [transfer("k-unused", 600_000, 600_100)];
        let records = build_timelines(&inputs(&events, &late, &[], &EnvSnapshot::default()));
        assert!(records[0].transfers.is_empty());

        // Just inside the slack on either side, it is.
        let just_before = [transfer(
            "k-unused",
            499_500 - TRANSFER_SLACK_MS,
            499_500 - TRANSFER_SLACK_MS + 10,
        )];
        let records = build_timelines(&inputs(&events, &just_before, &[], &EnvSnapshot::default()));
        assert_eq!(records[0].transfers.len(), 1);
    }

    #[test]
    fn transfers_from_a_wrapper_without_timestamps_are_skipped() {
        let events = [event("s1", "serde", "k1", 5_000, 500)];
        let mut old = transfer("k1", 0, 0);
        old.schema = 2;
        let records = build_timelines(&inputs(&events, &[old], &[], &EnvSnapshot::default()));
        assert!(records[0].transfers.is_empty());
    }

    #[test]
    fn records_carry_no_paths() {
        let events = [event("s1", "serde", "k1", 5_000, 500)];
        let records = build_timelines(&inputs(&events, &[], &[], &EnvSnapshot::default()));
        let json = serde_json::to_string(&records[0]).unwrap();
        assert!(!json.contains("/w"), "{json}");
        assert_eq!(records[0].root_hash.len(), 16);
    }

    #[test]
    fn the_run_context_only_carries_allowlisted_values() {
        let env = EnvSnapshot {
            repository: Some("org/repo".to_string()),
            job: Some("test".to_string()),
            run_id: Some("42".to_string()),
            ..EnvSnapshot::default()
        };
        let mut record_inputs = inputs(&[], &[], &[], &env);
        record_inputs.labels = BTreeMap::from([("phase".to_string(), "cold".to_string())]);
        let events = [event("s1", "serde", "k1", 5_000, 500)];
        record_inputs.events = &events;

        let record = &build_timelines(&record_inputs)[0];
        assert_eq!(record.context.repository.as_deref(), Some("org/repo"));
        assert_eq!(record.context.labels["phase"], "cold");
        assert_eq!(record.context.workflow, None);
    }

    #[test]
    fn the_record_id_is_stable_for_a_session_and_differs_per_run() {
        let events = [event("s1", "serde", "k1", 5_000, 500)];
        let env = EnvSnapshot {
            run_id: Some("1".to_string()),
            ..EnvSnapshot::default()
        };
        let first = build_timelines(&inputs(&events, &[], &[], &env))[0]
            .client_record_id
            .clone();
        let again = build_timelines(&inputs(&events, &[], &[], &env))[0]
            .client_record_id
            .clone();
        assert_eq!(first, again);
        assert_eq!(first.len(), 16);

        let rerun = EnvSnapshot {
            run_id: Some("2".to_string()),
            ..EnvSnapshot::default()
        };
        assert_ne!(
            build_timelines(&inputs(&events, &[], &[], &rerun))[0].client_record_id,
            first
        );
    }

    #[test]
    fn an_explicit_manifest_key_is_the_identity() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("Cargo.lock"), b"[[package]]\n").unwrap();
        let env = EnvSnapshot {
            manifest_key: Some("id/explicit".to_string()),
            profile: Some("release".to_string()),
            ..EnvSnapshot::default()
        };
        let identity = identity_for_root(dir.path(), &env);
        assert_eq!(identity.identity_key.as_deref(), Some("id/explicit"));
        assert_eq!(identity.source, IdentitySource::Explicit);
        assert!(identity.lock_digest.is_some());
    }

    #[test]
    fn without_a_profile_there_is_a_lock_digest_but_no_identity_key() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("Cargo.lock"), b"[[package]]\n").unwrap();

        let unknown = identity_for_root(dir.path(), &EnvSnapshot::default());
        assert!(unknown.lock_digest.is_some());
        assert_eq!(unknown.identity_key, None);
        assert_eq!(unknown.source, IdentitySource::Absent);

        let env = EnvSnapshot {
            profile: Some("release".to_string()),
            target: Some("x86_64-unknown-linux-gnu".to_string()),
            ..EnvSnapshot::default()
        };
        let known = identity_for_root(dir.path(), &env);
        assert_eq!(
            known.identity_key,
            crate::identity::identity_key(
                &dir.path().join("Cargo.lock"),
                "x86_64-unknown-linux-gnu",
                "release"
            )
        );
        assert_eq!(known.source, IdentitySource::LockEnv);
    }

    #[test]
    fn a_root_without_a_lockfile_has_no_digest() {
        let identity = identity_for_root(Path::new(""), &EnvSnapshot::default());
        assert_eq!(identity.lock_digest, None);
        assert_eq!(identity.source, IdentitySource::Absent);
    }

    #[test]
    fn the_newest_summary_for_the_session_is_attached() {
        let events = [event("s1", "serde", "k1", 5_000, 500)];
        let summary = |last_activity_ms, plan_id: &str| BuildSummaryEvent {
            incomplete: plan_id == "new",
            ts: chrono::Utc.timestamp_millis_opt(1).unwrap(),
            schema: 2,
            session_id: "s1".to_string(),
            root: String::new(),
            plan_source: "advisory".to_string(),
            plan_id: plan_id.to_string(),
            closure_reason: "inactivity".to_string(),
            started_at_ms: 1,
            last_activity_ms,
            candidate_keys: 4,
            downloaded_keys: 3,
            downloaded_bytes: 30,
            used_keys: 1,
            used_bytes: 10,
            demanded_keys: 2,
            demanded_candidate_keys: 1,
            cancelled: false,
            list_requests: 0,
            list_duration_ms: 0,
        };
        let summaries = [summary(10, "old"), summary(20, "new")];
        let records = build_timelines(&inputs(&events, &[], &summaries, &EnvSnapshot::default()));
        let attached = records[0].summary.as_ref().unwrap();
        assert!(attached.incomplete);
        assert_eq!(attached.plan_id, "new");
        assert_eq!(attached.candidate_keys, 4);

        let without = build_timelines(&inputs(&events, &[], &[], &EnvSnapshot::default()));
        assert!(without[0].summary.is_none());
    }

    #[test]
    fn the_sessions_own_root_wins_a_tie_deterministically() {
        let mut session = SessionEvents::default();
        session.roots.insert("/a".to_string(), 2);
        session.roots.insert("/b".to_string(), 2);
        assert_eq!(session.root(), Some("/a"));
        session.roots.insert("/b".to_string(), 3);
        assert_eq!(session.root(), Some("/b"));
        assert_eq!(SessionEvents::default().root(), None);
    }

    #[test]
    fn env_lookup_trims_and_treats_blank_as_unset() {
        let vars: HashMap<&str, &str> = [
            ("GITHUB_REPOSITORY", "  zondax/kache "),
            ("GITHUB_JOB", "   "),
            ("GITHUB_SHA", ""),
            ("KACHE_PROFILE", ""),
            ("PROFILE", "release"),
            ("HOME", "/nope"),
        ]
        .into_iter()
        .collect();
        let env = EnvSnapshot::from_lookup(|name| vars.get(name).map(|v| v.to_string()));

        assert_eq!(env.repository.as_deref(), Some("zondax/kache"));
        assert_eq!(env.job, None, "whitespace-only is unset");
        assert_eq!(env.commit, None, "empty is unset");
        assert_eq!(
            env.profile.as_deref(),
            Some("release"),
            "PROFILE is the fallback"
        );
        assert_eq!(env.workflow, None);
    }

    #[test]
    fn from_env_reads_the_process_environment() {
        // KACHE_RUNNER_POOL is read by nothing else in this binary, so a
        // guarded set cannot disturb another test.
        let _guard = crate::config::tests::set_env_for_test(
            "KACHE_RUNNER_POOL",
            Some(std::ffi::OsStr::new("mutant-pool")),
        );
        assert_eq!(
            EnvSnapshot::from_env().runner_pool.as_deref(),
            Some("mutant-pool")
        );
    }

    #[test]
    fn the_root_named_by_most_compiles_wins() {
        // One compile names /a and three name /b. Insertion order and the
        // tie-break both favor /a, so only counting picks /b.
        let mut events = vec![event("s1", "syn", "k0", 1_000, 100)];
        events[0].root = "/a".to_string();
        for (i, key) in ["k1", "k2", "k3"].iter().enumerate() {
            let mut e = event("s1", "serde", key, 2_000 + i as i64, 100);
            e.root = "/b".to_string();
            events.push(e);
        }
        let env = EnvSnapshot::default();
        let records = build_timelines(&inputs(&events, &[], &[], &env));
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].root_hash, short_hash("/b"));
    }
}
