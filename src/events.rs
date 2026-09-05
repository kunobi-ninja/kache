use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// A single build event logged by the wrapper.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildEvent {
    pub ts: DateTime<Utc>,
    pub crate_name: String,
    /// Build tree/root this compiler invocation belongs to.
    ///
    /// For rustc this is the workspace root derived from Cargo's output
    /// layout. For cc-family compiles this is the best common source/build
    /// root kache can derive, falling back to the current directory.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub root: String,
    #[serde(default)]
    pub version: String,
    pub result: EventResult,
    /// Wall time cargo waited for this wrapper process (ms). From schema 17
    /// it is anchored at process start, so `startup_ms` lies inside it;
    /// older wrappers started the clock at wrapper entry.
    pub elapsed_ms: u64,
    /// Estimated compile cost for this invocation.
    ///
    /// Misses record the compile phase duration before cache-store work.
    /// Hits reuse the cached entry's stored compile cost when known.
    #[serde(default)]
    pub compile_time_ms: u64,
    pub size: u64,
    #[serde(default)]
    pub cache_key: String,
    /// Event schema version: 0 = legacy, 1 = prefetch-aware,
    /// 2 = compile-cost-aware, 3 = op-count-aware, 4 = probe-count-aware,
    /// 5 = passthrough details, 6 = file-hash cache metrics,
    /// 7 = restore-method bytes, 8 = dup outcome + store blob counters,
    /// 9 = event root, 10 = build session id (#583),
    /// 11 = key field hashes + miss key diff (#131),
    /// 12 = per-extern artifact digests (#609),
    /// 13 = store-failure reason (#629),
    /// 14 = compilation-unit identity, own and per-extern (#627),
    /// 15 = same-key lookup rejection reason (#655),
    /// 16 = compile-and-compare verify on hits (`verify_compare`),
    /// 17 = wrapper phase timings: startup, dep-info pre-pass, scheduler wait.
    #[serde(default)]
    pub schema: u32,
    /// Build session this event belongs to (kunobi-ninja/kache#583 P0.5).
    ///
    /// Minted once per build by the wrapper that wins the session-marker
    /// lock (see `wrapper::build_session_id`) and read back by every other
    /// wrapper in the same build, so per-crate events can be joined with the
    /// daemon's per-plan prefetch summary. Empty = legacy wrapper or no
    /// session marker (never fails a build).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub session_id: String,
    /// Cache key computation time (ms).
    #[serde(default)]
    pub key_ms: u64,
    /// File hashes served from the key-computation hash cache.
    #[serde(default)]
    pub key_hash_hits: u64,
    /// File hashes computed by reading file contents during key computation.
    #[serde(default)]
    pub key_hash_misses: u64,
    /// File bytes read and hashed during key computation.
    #[serde(default)]
    pub key_hash_bytes: u64,
    /// Store lookup time — SQLite query + meta read (ms).
    #[serde(default)]
    pub lookup_ms: u64,
    /// Restore from cache time — blob link/copy + mtime + depinfo + codesign (hits only, ms).
    #[serde(default)]
    pub restore_ms: u64,
    /// Store put time — tar + compress + dedup + SQLite (dup/miss only, ms).
    #[serde(default)]
    pub store_ms: u64,
    /// Process start to wrapper entry (ms): argv handling, logging setup and
    /// config load, everything before the first cache decision. Zero when
    /// the process did not go through `main` (unit tests). Schema 17.
    #[serde(default)]
    pub startup_ms: u64,
    /// Time in the `rustc --emit=dep-info` pre-pass spawns (ms). Already part
    /// of `key_ms`; split out because it is a full extra compiler start per
    /// invocation, hit or miss. Zero for cc and passthroughs. Schema 17.
    #[serde(default)]
    pub dep_info_ms: u64,
    /// Dep-info pre-pass spawns for this invocation: 1 on the rustc key path,
    /// 0 on every path that never computes a rustc key. Schema 17.
    #[serde(default)]
    pub dep_info_runs: u32,
    /// Sampled verifications where the predicted input closure disagreed with
    /// the one the dep-info pre-pass discovered. Schema 18.
    ///
    /// This is the number that qualifies input predictions for wider use. The
    /// pre-pass answer always wins, so a mismatch costs nothing at the time —
    /// it is evidence that the soundness argument has a hole, and it is only
    /// counted where `KACHE_VERIFY_INPUT_PREDICTIONS` asked for the
    /// comparison. Always zero with verification off, which is the default.
    #[serde(default)]
    pub prediction_mismatches: u32,
    /// Time blocked joining a machine-wide flight another process already
    /// owned for the same compile (ms). Zero on a hit served at first
    /// lookup; a hit taken on the recheck after the owner finished carries
    /// the wait. Schema 17.
    #[serde(default)]
    pub flight_wait_ms: u64,
    /// Time blocked acquiring scheduler permit slots (ms). Misses only.
    /// Schema 17.
    #[serde(default)]
    pub permit_wait_ms: u64,
    /// Unique output blobs handled by store put (compiled outcomes only).
    #[serde(default)]
    pub store_output_blobs: u32,
    /// Output blobs whose content hash already existed before store put.
    #[serde(default)]
    pub store_duplicate_blobs: u32,
    /// Output blobs whose content hash was new before store put.
    #[serde(default)]
    pub store_new_blobs: u32,
    /// Times kache spawned the underlying compiler for this build.
    /// 0 on a cache hit, 1 on a dup/miss. Deterministic — independent of
    /// machine speed — so the e2e harness can assert on it.
    #[serde(default)]
    pub compiler_runs: u32,
    /// Times kache spawned the preprocessor (`cc -E`) for this build —
    /// once per C/C++ compile to derive the cache key, 0 for rustc.
    #[serde(default)]
    pub preprocessor_runs: u32,
    /// Times kache spawned a compiler probe (`cc --version` / `cc -###`)
    /// for this build. Memoized on disk, so the first compile of a
    /// build records 1 and the rest record 0; a warm probe cache
    /// records 0.
    #[serde(default)]
    pub probe_runs: u32,
    /// Bytes restored from cache by a CoW reflink on a hit — physically
    /// zero-copy and write-isolated, kache's preferred restore path.
    #[serde(default)]
    pub reflinked_bytes: u64,
    /// Bytes restored by a hardlink — zero-copy via a shared inode, the
    /// fallback when the filesystem has no CoW (reflink) support.
    #[serde(default)]
    pub hardlinked_bytes: u64,
    /// Bytes restored by a full physical copy — the last-resort fallback
    /// when neither reflink nor hardlink is available.
    #[serde(default)]
    pub copied_bytes: u64,
    /// Bytes ingested into the store by a CoW reflink on a miss — the blob
    /// shares blocks with the build's own output file, so it costs ~no extra
    /// disk (APFS / btrfs / XFS-with-reflink).
    #[serde(default)]
    pub store_reflinked_bytes: u64,
    /// Bytes ingested into the store by a hardlink on a miss — the blob
    /// shares an inode with the build's own output, zero-copy on filesystems
    /// without CoW (immutable artifact kinds only).
    #[serde(default)]
    pub store_hardlinked_bytes: u64,
    /// Bytes ingested into the store by a full physical copy on a miss — a
    /// genuine second copy, the fallback when neither reflink nor hardlink
    /// is available.
    #[serde(default)]
    pub store_copied_bytes: u64,
    /// Bytes copied into the store after an EXDEV hardlink failure (#835):
    /// `link(2)` refuses across mounts, including two bind mounts of one
    /// filesystem. `serde(default)` so no event-schema bump is needed.
    #[serde(default)]
    pub store_copy_cross_device_bytes: u64,
    /// Bytes copied into the store after an EPERM/EACCES hardlink failure
    /// (#835). `serde(default)`, no schema bump.
    #[serde(default)]
    pub store_copy_permission_bytes: u64,
    /// Bytes copied into the store without attempting a hardlink (#835):
    /// kind-ineligible (executable, dylib, depinfo, extensionless) or a cc
    /// put that never shares inodes. `serde(default)`, no schema bump.
    #[serde(default)]
    pub store_copy_ineligible_bytes: u64,
    /// Bytes copied into the store after any other hardlink errno (#835).
    /// `serde(default)`, no schema bump.
    #[serde(default)]
    pub store_copy_other_bytes: u64,
    /// Bytes restored by copy after an EXDEV hardlink failure (#835).
    /// `serde(default)`, no schema bump.
    #[serde(default)]
    pub restore_copy_cross_device_bytes: u64,
    /// Bytes restored by copy after an EPERM/EACCES hardlink failure (#835).
    /// `serde(default)`, no schema bump.
    #[serde(default)]
    pub restore_copy_permission_bytes: u64,
    /// Bytes restored by copy for the exclusive-carrier rule (#794/#835):
    /// the blob already had a consumer. `serde(default)`, no schema bump.
    #[serde(default)]
    pub restore_copy_exclusive_bytes: u64,
    /// Bytes restored by copy after any other hardlink failure (#835).
    /// `serde(default)`, no schema bump.
    #[serde(default)]
    pub restore_copy_other_bytes: u64,
    /// Why kache passed the invocation through instead of caching it.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub passthrough_reason: String,
    /// Why `Store::put` failed, when the compiler ran and produced outputs that
    /// were then not cached (kunobi-ninja/kache#629).
    ///
    /// The event stays a `Miss` — the compiler really did run, and demoting it
    /// to `Skipped` would drop it out of the hit-rate denominator and out of the
    /// miss table, which is exactly where a repeating miss is visible. This
    /// field is the annotation that separates "cold miss, now cached" from "will
    /// miss again on every build", which the result alone cannot express.
    ///
    /// Empty on every normal outcome, so it costs nothing on the wire.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub store_error: String,
    /// Why an existing entry for this exact key was rejected before the
    /// compiler ran (kunobi-ninja/kache#655).
    ///
    /// This is distinct from `store_error`: the lookup found an entry, but its
    /// artifact set could not satisfy the current invocation. The subsequent
    /// compile and replacement store may succeed, otherwise leaving
    /// `why-miss` to misdiagnose the event as a cold miss or key mismatch.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub lookup_rejection: String,
    /// Compile-and-compare qualification (`KACHE_VERIFY`) on a cache hit.
    ///
    /// Empty when the flag is off. `ok` when the restored artifacts match a
    /// fresh compile. `path-debug: …` for embedded absolute-path / debug-info
    /// differences. `content: …` for remaining byte faults. The hit is still
    /// served (fail-open); this field is the record.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub verify_compare: String,
    /// Whether a configured fallback wrapper handled the passthrough.
    #[serde(default, skip_serializing_if = "is_false")]
    pub fallback: bool,
    /// Exit code from the passthrough command.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Per-group cache-key digests (kunobi-ninja/kache#131): 16-hex prefixes
    /// of each key input group (compiler, args, sources, env_deps, externs,
    /// link, env_cfg, remap, crate), computed as a tee of the exact bytes the
    /// final key hashes. Powers `explain_miss` — diffing two events' maps
    /// names which input group changed. Empty for cc compiles and
    /// passthroughs.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub key_fields: std::collections::BTreeMap<String, String>,
    /// On a miss with `[cache] explain_miss` on: the key groups whose digests
    /// changed vs this crate's last hit in the same build tree.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub key_diff: Vec<String>,
    /// Per-dependency artifact digests folded into the `externs` group
    /// (kunobi-ninja/kache#609), as `crate name -> 16-hex of the dependency's
    /// artifact content hash`, or `(sysroot)` for a dependency whose artifact
    /// isn't readable.
    ///
    /// The group digest in `key_fields` says only that SOME dependency moved.
    /// This map says which one, which is what lets `why-miss` walk an
    /// `extern:` cascade to the crate that actually changed instead of
    /// reporting the same undifferentiated diagnosis for every crate above it.
    ///
    /// Written on both hits and misses (the diff needs a baseline) but only
    /// when `[cache] explain_miss` is on: a crate with 150 dependencies would
    /// otherwise add 150 entries to every event on the hot path. Empty for cc
    /// compiles, passthroughs, and pre-#609 wrappers.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub key_externs: std::collections::BTreeMap<String, String>,
    /// Whether `key_externs` was recorded for this compile, as opposed to being
    /// absent (kunobi-ninja/kache#609).
    ///
    /// An empty map is skipped on the wire, so without this flag a crate with
    /// NO dependencies is indistinguishable from one built before the digests
    /// existed. That difference decides whether the cascade walk can conclude
    /// "its dependencies are stable, so this is the root" or has to stop with
    /// an unresolved endpoint — and a dependency-free leaf is a very common
    /// root. Written only alongside `key_externs`, so it costs nothing on the
    /// default (non-`explain_miss`) path.
    #[serde(default, skip_serializing_if = "is_false")]
    pub key_externs_recorded: bool,
    /// This compile's own compilation-unit identity — cargo's
    /// `-C extra-filename` hash (kunobi-ninja/kache#627).
    ///
    /// `crate_name` cannot carry this: two versions of a package, a host and a
    /// target build of the same crate, and two feature sets of it all share one
    /// name, so pairing a compile with "the previous event of the same crate"
    /// can pair two unrelated units. This is the disambiguator cargo itself uses
    /// to keep those units' artifacts apart in one `deps/` directory.
    ///
    /// Diagnostic only — never folded into a cache key, which would tie the key
    /// to cargo's unit hashing and break cross-machine sharing. Written under
    /// the same `[cache] explain_miss` gate as `key_externs`; empty for cc
    /// compiles, passthroughs, non-cargo rustc invocations, and pre-#627
    /// wrappers.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub unit_id: String,
    /// Producing unit per extern, as `name the consumer used -> the producer's
    /// unit id` (kunobi-ninja/kache#627).
    ///
    /// The companion to `key_externs`: that map says which dependency's artifact
    /// moved, this one says which compilation unit produced it. It is what lets
    /// the cascade walk follow a renamed dependency (`foo_old = { package =
    /// "foo" }` records the alias in the consumer's key but `foo` in the
    /// producer's events) and pick the right one of several same-named units.
    ///
    /// Recovered from the `--extern` artifact filename, so an extern without
    /// that suffix simply has no entry and the walk falls back to name matching.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub extern_units: std::collections::BTreeMap<String, String>,
}

impl BuildEvent {
    /// Served from cache. `compile_time_ms` on a hit is the stored compile
    /// cost, not time this process spent.
    pub fn is_hit(&self) -> bool {
        matches!(
            self.result,
            EventResult::LocalHit | EventResult::PrefetchHit | EventResult::RemoteHit
        )
    }

    /// Wrapper time minus the compile it ran, if it ran one.
    pub fn overhead_ms(&self) -> u64 {
        if self.is_hit() {
            self.elapsed_ms
        } else {
            self.elapsed_ms.saturating_sub(self.compile_time_ms)
        }
    }

    /// Scheduler wait: flight join plus permit acquisition.
    pub fn wait_ms(&self) -> u64 {
        self.flight_wait_ms + self.permit_wait_ms
    }

    /// Overhead a measured phase accounts for. `dep_info_ms` is inside
    /// `key_ms` and is not added again.
    pub fn attributed_ms(&self) -> u64 {
        self.startup_ms
            + self.key_ms
            + self.lookup_ms
            + self.wait_ms()
            + self.restore_ms
            + self.store_ms
    }

    /// Overhead no phase accounts for: argument parsing, store open,
    /// build-lock waits, dep-info staging, process exit. Clamped at zero, so
    /// an event whose phases overlap cannot go negative.
    pub fn unattributed_ms(&self) -> u64 {
        self.overhead_ms().saturating_sub(self.attributed_ms())
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventResult {
    LocalHit,
    /// Local hit on an artifact that was downloaded by the manifest prefetch.
    PrefetchHit,
    RemoteHit,
    /// Entry miss; compiler ran; all output blobs already existed.
    Dup,
    Miss,
    Error,
    Passthrough,
    Skipped,
}

impl std::fmt::Display for EventResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventResult::LocalHit => write!(f, "local_hit"),
            EventResult::PrefetchHit => write!(f, "prefetch_hit"),
            EventResult::RemoteHit => write!(f, "remote_hit"),
            EventResult::Dup => write!(f, "dup"),
            EventResult::Miss => write!(f, "miss"),
            EventResult::Error => write!(f, "error"),
            EventResult::Passthrough => write!(f, "passthrough"),
            EventResult::Skipped => write!(f, "skipped"),
        }
    }
}

/// Per-session prefetch summary, appended to `summaries.jsonl` by the daemon
/// when a build session is finalized (kunobi-ninja/kache#583 P0.5).
///
/// Finalization happens on session inactivity, supersession by a newer
/// session, or daemon shutdown — cargo gives no positive end-of-build signal,
/// so closure is always inferred (`closure_reason` says how). Counts are kept
/// as raw numerators/denominators; consumers derive ratios (key precision =
/// `used_keys / downloaded_keys`, byte precision = `used_bytes /
/// downloaded_bytes`) so mixed-version or partial sessions stay auditable.
///
/// `used_keys`/`used_bytes` are a LOWER BOUND: a completed prefetch is
/// imported into the local store and consumed as a `LocalHit` without
/// contacting the daemon, so daemon-side demand misses it. Join per-crate
/// events by `session_id` + `cache_key` for full attribution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BuildSummaryEvent {
    pub ts: DateTime<Utc>,
    pub schema: u32,
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub root: String,
    /// `advisory` | `fallback` | `none` — which planner produced the plan.
    #[serde(default)]
    pub plan_source: String,
    #[serde(default)]
    pub plan_id: String,
    /// `inactivity` | `superseded` | `shutdown`.
    #[serde(default)]
    pub closure_reason: String,
    #[serde(default)]
    pub started_at_ms: u64,
    #[serde(default)]
    pub last_activity_ms: u64,
    /// Plan candidates offered for prefetch.
    #[serde(default)]
    pub candidate_keys: u64,
    /// Prefetch downloads completed (distinct keys / compressed wire bytes).
    #[serde(default)]
    pub downloaded_keys: u64,
    #[serde(default)]
    pub downloaded_bytes: u64,
    /// Daemon-visible consumption of downloaded keys (lower bound, see above).
    #[serde(default)]
    pub used_keys: u64,
    #[serde(default)]
    pub used_bytes: u64,
    /// Distinct keys demanded via RemoteCheck while the plan was active.
    #[serde(default)]
    pub demanded_keys: u64,
    /// Distinct demanded keys that were plan candidates.
    #[serde(default)]
    pub demanded_candidate_keys: u64,
    /// Whether adaptive cancellation fired for this plan.
    #[serde(default)]
    pub cancelled: bool,
    /// Key-cache LIST refreshes attributed to this session (delta of the
    /// daemon-lifetime counters between plan install and finalization).
    #[serde(default)]
    pub list_requests: u64,
    #[serde(default)]
    pub list_duration_ms: u64,
}

/// A liveness ping for an in-flight compile (kunobi-ninja/kache#131),
/// appended to the same `events.jsonl` stream by the wrapper's monitor thread
/// while a cache-miss compile runs. Gives non-TTY consumers (bench harnesses,
/// dashboards, CI parsers) the same "still compiling X" signal the optional
/// stderr heartbeat gives humans — TTY throttling (mach) can eat stderr, this
/// can't.
///
/// Wire compatibility is load-bearing: pre-heartbeat readers per-line
/// try-parse `BuildEvent` and silently skip lines that fail, and `BuildEvent`
/// tolerates unknown fields — so an `event: "heartbeat"` tag alone would NOT
/// stop an old reader from mis-parsing this line. What does is omission: this
/// struct deliberately carries none of `result`/`elapsed_ms`/`size`, the
/// `BuildEvent` fields with no serde default, so old readers fail the parse
/// and skip the line. Conversely `event` has no default here, so a
/// `BuildEvent` line can never mis-parse as a heartbeat.
///
/// `schema` is the heartbeat record's OWN version lineage (starting at
/// [`HEARTBEAT_SCHEMA`] = 1), independent of `BuildEvent::schema`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatEvent {
    /// Always [`HEARTBEAT_EVENT_TAG`] — the discriminator for jsonl consumers.
    pub event: String,
    pub ts: DateTime<Utc>,
    pub crate_name: String,
    /// Build tree/root, same derivation as [`BuildEvent::root`].
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub root: String,
    /// PID of the compiler child being waited on.
    pub pid: u32,
    /// Seconds since the compiler child was spawned.
    pub elapsed_s: u64,
    /// Median historical compile time for this crate (see
    /// [`typical_compile_ms`]), when history exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typical_s: Option<u64>,
    /// `typical_s - elapsed_s`, floored at zero — omitted with no history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eta_s: Option<u64>,
    #[serde(default)]
    pub schema: u32,
}

/// Discriminator value of [`HeartbeatEvent::event`].
pub const HEARTBEAT_EVENT_TAG: &str = "heartbeat";

/// Current [`HeartbeatEvent::schema`] version.
pub const HEARTBEAT_SCHEMA: u32 = 1;

/// One parsed line of the event log, for consumers that want the full mixed
/// stream (`kache monitor`). Existing [`BuildEvent`]-only readers keep their
/// narrow view and skip heartbeat lines by parse failure.
#[derive(Debug, Clone)]
pub enum EventRecord {
    // Boxed: BuildEvent is ~350 bytes vs the heartbeat's ~140 (clippy
    // large_enum_variant), and records are consumed one at a time.
    Build(Box<BuildEvent>),
    Heartbeat(HeartbeatEvent),
}

/// Parse one event-log line into whichever record type it is. Heartbeats are
/// tried first: a heartbeat line can never parse as a `BuildEvent` (missing
/// required fields), but the reverse must also never happen, which the
/// required `event` tag guarantees.
fn parse_event_line(line: &str) -> Option<EventRecord> {
    if let Ok(hb) = serde_json::from_str::<HeartbeatEvent>(line) {
        if hb.event == HEARTBEAT_EVENT_TAG {
            return Some(EventRecord::Heartbeat(hb));
        }
        return None;
    }
    serde_json::from_str::<BuildEvent>(line)
        .ok()
        .map(|e| EventRecord::Build(Box::new(e)))
}

/// Append one serialized JSON line under the exclusive sidecar lock — the
/// shared tail of [`log_event`] and [`log_heartbeat`], so every writer has the
/// same interleaving guarantee across concurrent wrapper processes.
fn append_log_line(event_log_path: &Path, line: String) -> Result<()> {
    if let Some(parent) = event_log_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let lock = open_log_lock(event_log_path).context("opening event log lock")?;
    lock.lock().context("locking event log")?;

    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(event_log_path)
        .context("opening event log")?;

    let write_result = (|| -> Result<()> {
        // A writer can die after a short write, leaving an unterminated JSON
        // fragment at EOF. Appending the next event straight onto it would
        // merge two records into one unparseable line, losing the VALID event
        // too. Terminate the abandoned fragment as its own (invalid, skipped)
        // record first (#528, cross-family review finding).
        if file.metadata().context("statting event log")?.len() != 0 {
            file.seek(SeekFrom::End(-1))
                .context("seeking to event log tail")?;
            let mut last = [0u8; 1];
            std::io::Read::read_exact(&mut file, &mut last)
                .context("reading event log tail byte")?;
            if last[0] != b'\n' {
                file.write_all(b"\n")
                    .context("terminating abandoned event fragment")?;
            }
        }

        let mut bytes = line.into_bytes();
        bytes.push(b'\n');
        file.write_all(&bytes).context("writing event to log")
    })();
    lock.unlock().context("unlocking event log")?;
    write_result
}

/// Append a build event to the event log file.
/// Uses an exclusive sidecar file lock so concurrent wrapper processes cannot
/// interleave JSON lines.
pub fn log_event(event_log_path: &Path, event: &BuildEvent) -> Result<()> {
    append_log_line(
        event_log_path,
        serde_json::to_string(event).context("serializing event")?,
    )
}

/// Append a heartbeat line to the event log (kunobi-ninja/kache#131).
pub fn log_heartbeat(event_log_path: &Path, event: &HeartbeatEvent) -> Result<()> {
    append_log_line(
        event_log_path,
        serde_json::to_string(event).context("serializing heartbeat")?,
    )
}

/// Number of most-recent samples the typical-time median is computed over.
const TYPICAL_WINDOW: usize = 20;

/// Median compile cost (ms) for `crate_name` from the recent event log — the
/// "typical: 7m51s" / ETA input for heartbeats (kunobi-ninja/kache#131).
///
/// One full log read under a shared lock. Callers invoke this lazily on the
/// FIRST heartbeat tick, never at spawn: only compiles already running longer
/// than one cadence (default 30 s) pay the read, so a cold build with hundreds
/// of fast misses performs no scans at all. Uses the last [`TYPICAL_WINDOW`]
/// events with a recorded compile cost for the crate, dropping samples more
/// than 3σ from the window median (a one-off `-j1` or thermally-throttled
/// build must not wreck the estimate).
pub fn typical_compile_ms(event_log_path: &Path, crate_name: &str, root: &str) -> Option<u64> {
    let events = read_events(event_log_path).ok()?;
    let samples: Vec<u64> = events
        .iter()
        // Only real compiles in the SAME build tree: hits merely repeat the
        // stored cost (biasing the median toward one old measurement), and a
        // same-named crate in another workspace is different code entirely
        // (cross-family review finding).
        .filter(|e| {
            e.crate_name == crate_name
                && e.root == root
                && e.compile_time_ms > 0
                && matches!(e.result, EventResult::Miss | EventResult::Dup)
        })
        .map(|e| e.compile_time_ms)
        .collect();
    let window = &samples[samples.len().saturating_sub(TYPICAL_WINDOW)..];
    let center = median(window)?;
    let n = window.len() as f64;
    let mean = window.iter().sum::<u64>() as f64 / n;
    let sigma = (window
        .iter()
        .map(|&s| {
            let d = s as f64 - mean;
            d * d
        })
        .sum::<f64>()
        / n)
        .sqrt();
    let kept: Vec<u64> = window
        .iter()
        .copied()
        .filter(|&s| sigma == 0.0 || (s as f64 - center as f64).abs() <= 3.0 * sigma)
        .collect();
    median(&kept)
}

/// Median of a non-empty slice (`None` when empty). Even-length slices take
/// the lower-middle element — stability over precision for ETA display.
fn median(samples: &[u64]) -> Option<u64> {
    if samples.is_empty() {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    Some(sorted[(sorted.len() - 1) / 2])
}

/// Read all events from the event log.
pub fn read_events(event_log_path: &Path) -> Result<Vec<BuildEvent>> {
    if !event_log_path.exists() {
        return Ok(Vec::new());
    }

    let lock = open_log_lock(event_log_path).context("opening event log lock")?;
    lock.lock_shared().context("locking event log for read")?;

    let file = File::open(event_log_path).context("opening event log")?;
    let reader = BufReader::new(&file);
    let mut events = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<BuildEvent>(&line) {
            Ok(event) => events.push(event),
            Err(e) => {
                tracing::debug!("skipping invalid event line: {}", e);
            }
        }
    }

    lock.unlock().context("unlocking event log")?;
    Ok(events)
}

/// Read events since a given timestamp.
pub fn read_events_since(event_log_path: &Path, since: DateTime<Utc>) -> Result<Vec<BuildEvent>> {
    let all = read_events(event_log_path)?;
    Ok(all.into_iter().filter(|e| e.ts >= since).collect())
}

#[cfg(windows)]
fn get_file_identity(file: &std::fs::File) -> Option<(u32, u32, u32)> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
    };
    let handle = file.as_raw_handle();
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    let ok = unsafe { GetFileInformationByHandle(handle as _, &mut info) };
    if ok != 0 {
        Some((
            info.dwVolumeSerialNumber,
            info.nFileIndexHigh,
            info.nFileIndexLow,
        ))
    } else {
        None
    }
}

/// Rotation marker sidecar (kunobi-ninja/kache#528): written by
/// [`rotate_log_impl`] under the exclusive sidecar lock, read by
/// [`EventTailer::poll_records`] under the shared lock. A byte position alone
/// cannot distinguish the retained old lines at the head of a rotated file
/// from newly appended ones; the marker records enough for the tailer to map
/// its old cursor onto the new file exactly.
#[derive(Serialize, Deserialize)]
struct RotationMarker {
    /// Monotonic rotation count. The pre-marker state is generation 0.
    generation: u64,
    /// Byte length of the log at the moment it was rotated.
    prev_len: u64,
    /// Byte length of the retained tail the rotated log was replaced with.
    retained_len: u64,
}

fn rotation_marker_path(log_path: &Path) -> PathBuf {
    let mut os = log_path.as_os_str().to_owned();
    os.push(".rotation");
    PathBuf::from(os)
}

fn read_rotation_marker(log_path: &Path) -> Option<RotationMarker> {
    let content = fs::read_to_string(rotation_marker_path(log_path)).ok()?;
    serde_json::from_str(&content).ok()
}

/// Tail the event log, returning new events since the last known position.
pub struct EventTailer {
    path: PathBuf,
    position: u64,
    /// Last rotation-marker generation this tailer reconciled its cursor with.
    generation: u64,
    file: Option<File>,
}

impl EventTailer {
    pub fn new(path: PathBuf) -> Self {
        let file = File::open(&path).ok();
        let position = file
            .as_ref()
            .and_then(|file| file.metadata().ok())
            .map(|m| m.len())
            .unwrap_or(0);
        let generation = read_rotation_marker(&path)
            .map(|m| m.generation)
            .unwrap_or(0);
        EventTailer {
            path,
            position,
            generation,
            file,
        }
    }

    /// Start from the beginning.
    pub fn from_start(path: PathBuf) -> Self {
        let file = File::open(&path).ok();
        let generation = read_rotation_marker(&path)
            .map(|m| m.generation)
            .unwrap_or(0);
        EventTailer {
            path,
            position: 0,
            generation,
            file,
        }
    }

    /// Read new build events since last poll. Note that log rotation is lossy
    /// and may discard events that were never polled. Heartbeat lines are
    /// skipped — use [`EventTailer::poll_records`] for the mixed stream (all
    /// live consumers do; this narrow view is kept for the rotation/truncation
    /// tests and future builds-only consumers).
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn poll(&mut self) -> Result<Vec<BuildEvent>> {
        Ok(self
            .poll_records()?
            .into_iter()
            .filter_map(|r| match r {
                EventRecord::Build(e) => Some(*e),
                EventRecord::Heartbeat(_) => None,
            })
            .collect())
    }

    /// Read all new records since last poll — build events AND heartbeats
    /// (kunobi-ninja/kache#131), for consumers like `kache monitor` that
    /// render in-flight compiles.
    pub fn poll_records(&mut self) -> Result<Vec<EventRecord>> {
        // Nothing was ever logged: don't create the lock sidecar in a
        // directory that doesn't exist yet.
        if let Some(parent) = self.path.parent()
            && !parent.exists()
        {
            return Ok(Vec::new());
        }

        // Hold the shared sidecar lock across the whole rotation-check → read
        // → cursor-update transaction (kunobi-ninja/kache#528): it excludes a
        // writer's in-progress append (a torn final line) and a rotation
        // landing between the rotation check and the read.
        let lock = open_log_lock(&self.path).context("opening event log lock")?;
        lock.lock_shared().context("locking event log for poll")?;
        let res = self.poll_records_locked();
        let _ = lock.unlock();
        res
    }

    fn poll_records_locked(&mut self) -> Result<Vec<EventRecord>> {
        if self.file.is_none() {
            match File::open(&self.path) {
                Ok(file) => self.file = Some(file),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
                Err(e) => return Err(e.into()),
            }
        }

        // Rotation reconciliation, exact path first (#528): a single rotation
        // recorded by the marker maps the old cursor onto the new file — the
        // retained tail we already returned is skipped, retained-but-never-
        // delivered bytes are not.
        let mut handled_rotation = false;
        if let Some(marker) = read_rotation_marker(&self.path) {
            if marker.generation == self.generation + 1 {
                let dropped = marker.prev_len.saturating_sub(marker.retained_len);
                self.position = self
                    .position
                    .saturating_sub(dropped)
                    .min(marker.retained_len);
                self.generation = marker.generation;
                handled_rotation = true;
            } else if marker.generation != self.generation {
                // Missed more than one rotation: anything between the two
                // markers is unrecoverable, so restart from the head. May
                // re-deliver retained lines; never loses new ones.
                self.position = 0;
                self.generation = marker.generation;
                handled_rotation = true;
            }
        }

        // Identity fallback for rotations the marker can't vouch for (legacy
        // rotator, or a crash between the log replace and the marker write).
        let mut rotated = handled_rotation;
        if !handled_rotation && let Some(file) = &self.file {
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if let Ok(m1) = file.metadata()
                    && let Ok(m2) = std::fs::metadata(&self.path)
                    && (m1.dev() != m2.dev() || m1.ino() != m2.ino())
                {
                    rotated = true;
                    self.position = 0;
                }
            }
            #[cfg(windows)]
            {
                if let Some(id1) = get_file_identity(file)
                    && let Ok(f2) = std::fs::File::open(&self.path)
                    && let Some(id2) = get_file_identity(&f2)
                    && id1 != id2
                {
                    rotated = true;
                    self.position = 0;
                }
            }
        }

        if rotated {
            match File::open(&self.path) {
                Ok(file) => self.file = Some(file),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    self.file = None;
                    self.position = 0;
                    return Ok(Vec::new());
                }
                Err(e) => return Err(e.into()),
            }
        }

        let file = self.file.as_mut().unwrap();
        let file_len = file.metadata()?.len();

        if file_len < self.position {
            // File was truncated in place (no marker, same identity):
            // start from the beginning.
            self.position = 0;
        }

        if file_len <= self.position {
            return Ok(Vec::new());
        }

        file.seek(SeekFrom::Start(self.position))?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let mut records = Vec::new();
        let mut consumed = 0usize;
        for chunk in buf.split_inclusive(|&b| b == b'\n') {
            if chunk.last() != Some(&b'\n') {
                // Unterminated tail: a short write the writer never completed.
                // Leave the cursor before it — consuming it here would lose
                // the event permanently if the line is completed later (#528).
                break;
            }
            consumed += chunk.len();
            // trim() drops the terminating newline along with any other
            // whitespace, so the chunk can be parsed as-is.
            let line = String::from_utf8_lossy(chunk);
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match parse_event_line(line) {
                Some(record) => records.push(record),
                None => tracing::debug!("skipping invalid event line: {line}"),
            }
        }

        self.position += consumed as u64;
        Ok(records)
    }
}

/// Rotate the event log if it exceeds the max size.
/// Keeps the last `keep_lines` lines.
fn rotate_log_impl(
    log_path: &Path,
    max_size: u64,
    keep_lines: usize,
    log_label: &str,
) -> Result<()> {
    if !log_path.exists() {
        return Ok(());
    }

    // Lock-free stat gate to keep the hot path cheap when rotation is not needed.
    if let Ok(meta) = fs::metadata(log_path)
        && meta.len() <= max_size
    {
        return Ok(());
    }

    // 1. Acquire the lock before querying metadata or reading
    let lock = open_log_lock(log_path).context("opening log lock")?;
    lock.lock().context("locking log for rotation")?;

    let res = (|| -> Result<()> {
        let meta = fs::metadata(log_path)?;
        if meta.len() <= max_size {
            return Ok(());
        }

        // Clean up stale temp files in the same directory (older than 5 minutes)
        if let Some(parent) = log_path.parent()
            && let Some(file_prefix) = log_path.file_name().and_then(|n| n.to_str())
        {
            let _ = crate::atomic::cleanup_temp_files(
                parent,
                file_prefix,
                std::time::Duration::from_secs(300),
            );
        }

        let content = fs::read_to_string(log_path)?;
        // Only complete (newline-terminated) records participate in rotation,
        // and their raw bytes are preserved verbatim: the tailer's cursor
        // mapping assumes the retained tail is a byte-exact suffix of the
        // first `prev_len` bytes, which `lines()` + rejoin would break for an
        // unterminated final fragment or CRLF content (#528, cross-family
        // review finding). An abandoned fragment is unrecoverable either way
        // and must not perturb the offsets of complete records.
        let complete_len = content.rfind('\n').map_or(0, |i| i + 1);
        let lines: Vec<&str> = content[..complete_len].split_inclusive('\n').collect();
        let keep_from = lines.len().saturating_sub(keep_lines);
        let mut kept: Vec<&str> = lines[keep_from..].to_vec();

        // Size-cap re-check: trim additional lines from the beginning if total
        // size still exceeds max_size (line lengths include their newline).
        let mut total_bytes: u64 = kept.iter().map(|line| line.len() as u64).sum();
        while total_bytes > max_size && kept.len() > 1 {
            let removed = kept.remove(0);
            total_bytes -= removed.len() as u64;
        }

        let output = kept.concat();
        crate::atomic::atomic_replace(log_path, output.as_bytes())
            .context("writing and replacing log file atomically")?;

        // Record the rotation so tailers can map their cursor onto the new
        // file instead of re-delivering the retained tail (#528). Written
        // after the replace: if we crash in between, tailers fall back to
        // the identity check (duplicates at worst, never loss). `prev_len`
        // counts only the complete records, matching the cursor's maximum —
        // the tailer never consumes an unterminated tail.
        let marker = RotationMarker {
            generation: read_rotation_marker(log_path)
                .map(|m| m.generation)
                .unwrap_or(0)
                + 1,
            prev_len: complete_len as u64,
            retained_len: output.len() as u64,
        };
        if let Ok(json) = serde_json::to_string(&marker) {
            let _ = crate::atomic::atomic_replace(&rotation_marker_path(log_path), json.as_bytes());
        }

        tracing::info!(
            "rotated {}: kept {} of {} lines",
            log_label,
            kept.len(),
            lines.len()
        );
        Ok(())
    })();

    let _ = lock.unlock();
    res
}

/// Rotate the event log if it exceeds the max size.
/// Keeps the last `keep_lines` lines.
pub fn rotate_if_needed(event_log_path: &Path, max_size: u64, keep_lines: usize) -> Result<()> {
    rotate_log_impl(event_log_path, max_size, keep_lines, "event log")
}

// ── Summary log ─────────────────────────────────────────────────────────────

/// Append a per-session build summary to the summary log (`summaries.jsonl`).
/// Own file rather than `events.jsonl` so `read_events` never has to skip
/// foreign lines; same locking discipline as the other logs.
pub fn log_summary(summary_log_path: &Path, event: &BuildSummaryEvent) -> Result<()> {
    if let Some(parent) = summary_log_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let lock = open_log_lock(summary_log_path).context("opening summary log lock")?;
    lock.lock().context("locking summary log")?;

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(summary_log_path)
        .context("opening summary log")?;
    let line = serde_json::to_string(event).context("serializing summary event")?;
    let mut bytes = line.into_bytes();
    bytes.push(b'\n');
    file.write_all(&bytes)
        .context("writing summary event to log")?;
    lock.unlock().context("unlocking summary log")?;
    Ok(())
}

/// Read all build summaries from the summary log. Invalid lines are skipped
/// (schema evolution / partial writes must not poison reporting).
pub fn read_summaries(summary_log_path: &Path) -> Result<Vec<BuildSummaryEvent>> {
    if !summary_log_path.exists() {
        return Ok(Vec::new());
    }
    let lock = open_log_lock(summary_log_path).context("opening summary log lock")?;
    lock.lock_shared().context("locking summary log for read")?;

    let file = File::open(summary_log_path).context("opening summary log")?;
    let reader = BufReader::new(&file);
    let mut events = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<BuildSummaryEvent>(&line) {
            Ok(event) => events.push(event),
            Err(e) => {
                tracing::debug!("skipping invalid summary line: {}", e);
            }
        }
    }

    lock.unlock().context("unlocking summary log")?;
    Ok(events)
}

// ── Transfer log ────────────────────────────────────────────────────────────

use crate::daemon::TransferEvent;

/// Append a transfer event to the transfer log file.
pub fn log_transfer(transfer_log_path: &Path, event: &TransferEvent) -> Result<()> {
    if let Some(parent) = transfer_log_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let lock = open_log_lock(transfer_log_path).context("opening transfer log lock")?;
    lock.lock().context("locking transfer log")?;

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(transfer_log_path)
        .context("opening transfer log")?;
    let line = serde_json::to_string(event).context("serializing transfer event")?;
    let mut bytes = line.into_bytes();
    bytes.push(b'\n');
    file.write_all(&bytes)
        .context("writing transfer event to log")?;
    lock.unlock().context("unlocking transfer log")?;
    Ok(())
}

/// Read all transfer events from the transfer log.
pub fn read_transfers(transfer_log_path: &Path) -> Result<Vec<TransferEvent>> {
    if !transfer_log_path.exists() {
        return Ok(Vec::new());
    }
    let lock = open_log_lock(transfer_log_path).context("opening transfer log lock")?;
    lock.lock_shared()
        .context("locking transfer log for read")?;

    let file = File::open(transfer_log_path).context("opening transfer log")?;
    let reader = BufReader::new(&file);
    let mut events = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<TransferEvent>(&line) {
            Ok(event) => events.push(event),
            Err(e) => {
                tracing::debug!("skipping invalid transfer line: {}", e);
            }
        }
    }
    lock.unlock().context("unlocking transfer log")?;
    Ok(events)
}

fn open_log_lock(log_path: &Path) -> Result<File> {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(sidecar_lock_path(log_path))
        .context("opening log lock")
}

fn sidecar_lock_path(log_path: &Path) -> PathBuf {
    let mut path = log_path.as_os_str().to_owned();
    path.push(".lock");
    PathBuf::from(path)
}

/// Read transfer events since a given unix timestamp (seconds).
pub fn read_transfers_since(transfer_log_path: &Path, since_ts: u64) -> Result<Vec<TransferEvent>> {
    let all = read_transfers(transfer_log_path)?;
    Ok(all
        .into_iter()
        .filter(|e| e.timestamp >= since_ts)
        .collect())
}

/// Rotate the transfer log if it exceeds the max size.
/// Keeps the last `keep_lines` lines.
pub fn rotate_transfers_if_needed(
    transfer_log_path: &Path,
    max_size: u64,
    keep_lines: usize,
) -> Result<()> {
    rotate_log_impl(transfer_log_path, max_size, keep_lines, "transfer log")
}

/// Clear the event log.
#[allow(dead_code)]
pub fn clear_events(event_log_path: &Path) -> Result<()> {
    if !event_log_path.exists() {
        return Ok(());
    }
    let lock = open_log_lock(event_log_path).context("opening event log lock")?;
    lock.lock().context("locking event log for clearing")?;
    let res = fs::write(event_log_path, "");
    let _ = lock.unlock();
    res.context("clearing event log")
}

/// Get event statistics.
pub struct EventStats {
    #[allow(dead_code)]
    pub total: usize,
    pub local_hits: usize,
    pub prefetch_hits: usize,
    pub remote_hits: usize,
    pub dups: usize,
    pub misses: usize,
    pub errors: usize,
    pub total_size: u64,
    pub total_elapsed_ms: u64,
    pub hit_elapsed_ms: u64,
    pub miss_elapsed_ms: u64,
    pub hit_compile_time_ms: u64,
    pub miss_compile_time_ms: u64,
    pub total_key_ms: u64,
    pub total_lookup_ms: u64,
    pub total_restore_ms: u64,
    pub total_store_ms: u64,
    /// Process start to wrapper entry, summed over cacheable outcomes.
    pub total_startup_ms: u64,
    /// Dep-info pre-pass time (inside `total_key_ms`) and spawn count.
    pub total_dep_info_ms: u64,
    pub total_dep_info_runs: u64,
    /// Sampled verifications that disagreed with the pre-pass. See
    /// [`BuildEvent::prediction_mismatches`].
    pub total_prediction_mismatches: u64,
    /// Scheduler waits, kept apart so a saturated pool and a shared flight
    /// stay distinguishable.
    pub total_flight_wait_ms: u64,
    pub total_permit_wait_ms: u64,
    /// Wrapper overhead no phase accounts for (see
    /// [`BuildEvent::unattributed_ms`]).
    pub total_unattributed_ms: u64,
    pub store_output_blobs: u32,
    pub store_duplicate_blobs: u32,
    pub store_new_blobs: u32,
    /// Bytes restored from cache by CoW reflink (physically zero-copy).
    pub reflinked_bytes: u64,
    /// Bytes restored by hardlink (zero-copy via a shared inode).
    pub hardlinked_bytes: u64,
    /// Bytes restored by a full physical copy.
    pub copied_bytes: u64,
    /// Bytes ingested into the store by a CoW reflink (shares blocks with the
    /// build's output — not a second physical copy).
    pub store_reflinked_bytes: u64,
    /// Bytes ingested into the store by a hardlink (shares an inode with the
    /// build's output — not a second physical copy).
    pub store_hardlinked_bytes: u64,
    /// Bytes ingested into the store by a full physical copy (a real second copy).
    pub store_copied_bytes: u64,
    /// Copy-fallback reasons (#835), still as bytes so the report can show
    /// *why* zero-copy did not happen. All `serde(default)` on the event, so
    /// old readers stay compatible.
    pub store_copy_cross_device_bytes: u64,
    pub store_copy_permission_bytes: u64,
    pub store_copy_ineligible_bytes: u64,
    pub store_copy_other_bytes: u64,
    pub restore_copy_cross_device_bytes: u64,
    pub restore_copy_permission_bytes: u64,
    pub restore_copy_exclusive_bytes: u64,
    pub restore_copy_other_bytes: u64,
    /// Compiles whose outputs were produced but not cached, because
    /// `Store::put` failed (kunobi-ninja/kache#629). A subset of `misses` —
    /// a failed put leaves the blob counters at zero, which resolves to `Miss`,
    /// never `Dup` — counted separately because an ordinary miss becomes a hit
    /// next build and one of these misses again every time.
    ///
    /// Covers the local store only. A local put that succeeded and a remote
    /// upload that then failed is a different condition (cacheable here, likely
    /// a miss on a fresh CI worker) and is not counted here.
    pub store_failures: usize,
}

pub fn compute_stats(events: &[BuildEvent]) -> EventStats {
    let mut stats = EventStats {
        total: events.len(),
        local_hits: 0,
        prefetch_hits: 0,
        remote_hits: 0,
        dups: 0,
        misses: 0,
        errors: 0,
        total_size: 0,
        total_elapsed_ms: 0,
        hit_elapsed_ms: 0,
        miss_elapsed_ms: 0,
        hit_compile_time_ms: 0,
        miss_compile_time_ms: 0,
        total_key_ms: 0,
        total_lookup_ms: 0,
        total_restore_ms: 0,
        total_store_ms: 0,
        total_startup_ms: 0,
        total_dep_info_ms: 0,
        total_dep_info_runs: 0,
        total_prediction_mismatches: 0,
        total_flight_wait_ms: 0,
        total_permit_wait_ms: 0,
        total_unattributed_ms: 0,
        store_output_blobs: 0,
        store_duplicate_blobs: 0,
        store_new_blobs: 0,
        reflinked_bytes: 0,
        hardlinked_bytes: 0,
        copied_bytes: 0,
        store_reflinked_bytes: 0,
        store_hardlinked_bytes: 0,
        store_copied_bytes: 0,
        store_copy_cross_device_bytes: 0,
        store_copy_permission_bytes: 0,
        store_copy_ineligible_bytes: 0,
        store_copy_other_bytes: 0,
        restore_copy_cross_device_bytes: 0,
        restore_copy_permission_bytes: 0,
        restore_copy_exclusive_bytes: 0,
        restore_copy_other_bytes: 0,
        store_failures: 0,
    };

    for event in events {
        match event.result {
            EventResult::LocalHit => {
                stats.local_hits += 1;
                stats.hit_elapsed_ms += event.elapsed_ms;
                stats.hit_compile_time_ms += event.compile_time_ms;
            }
            EventResult::PrefetchHit => {
                stats.prefetch_hits += 1;
                stats.hit_elapsed_ms += event.elapsed_ms;
                stats.hit_compile_time_ms += event.compile_time_ms;
            }
            EventResult::RemoteHit => {
                stats.remote_hits += 1;
                stats.hit_elapsed_ms += event.elapsed_ms;
                stats.hit_compile_time_ms += event.compile_time_ms;
            }
            EventResult::Dup => {
                stats.dups += 1;
                stats.miss_elapsed_ms += event.elapsed_ms;
                stats.miss_compile_time_ms += if event.compile_time_ms > 0 {
                    event.compile_time_ms
                } else {
                    event.elapsed_ms
                };
            }
            EventResult::Miss => {
                stats.misses += 1;
                stats.miss_elapsed_ms += event.elapsed_ms;
                stats.miss_compile_time_ms += if event.compile_time_ms > 0 {
                    event.compile_time_ms
                } else {
                    event.elapsed_ms
                };
            }
            EventResult::Error => stats.errors += 1,
            EventResult::Passthrough | EventResult::Skipped => continue,
        }
        // Counted on top of the outcome above, not instead of it: the compile
        // is a real miss, and this says it will be one again next build (#629).
        // Gated on a compiled outcome so the counter cannot be inflated by a
        // malformed or future event that carries the field on a hit or an error.
        if matches!(event.result, EventResult::Miss | EventResult::Dup)
            && !event.store_error.is_empty()
        {
            stats.store_failures += 1;
        }
        stats.total_size += event.size;
        stats.total_elapsed_ms += event.elapsed_ms;
        stats.total_key_ms += event.key_ms;
        stats.total_lookup_ms += event.lookup_ms;
        stats.total_restore_ms += event.restore_ms;
        stats.total_store_ms += event.store_ms;
        stats.total_startup_ms += event.startup_ms;
        stats.total_dep_info_ms += event.dep_info_ms;
        stats.total_dep_info_runs += u64::from(event.dep_info_runs);
        stats.total_prediction_mismatches += u64::from(event.prediction_mismatches);
        stats.total_flight_wait_ms += event.flight_wait_ms;
        stats.total_permit_wait_ms += event.permit_wait_ms;
        stats.total_unattributed_ms += event.unattributed_ms();
        stats.store_output_blobs += event.store_output_blobs;
        stats.store_duplicate_blobs += event.store_duplicate_blobs;
        stats.store_new_blobs += event.store_new_blobs;
        stats.reflinked_bytes += event.reflinked_bytes;
        stats.hardlinked_bytes += event.hardlinked_bytes;
        stats.copied_bytes += event.copied_bytes;
        stats.store_reflinked_bytes += event.store_reflinked_bytes;
        stats.store_hardlinked_bytes += event.store_hardlinked_bytes;
        stats.store_copied_bytes += event.store_copied_bytes;
        stats.store_copy_cross_device_bytes += event.store_copy_cross_device_bytes;
        stats.store_copy_permission_bytes += event.store_copy_permission_bytes;
        stats.store_copy_ineligible_bytes += event.store_copy_ineligible_bytes;
        stats.store_copy_other_bytes += event.store_copy_other_bytes;
        stats.restore_copy_cross_device_bytes += event.restore_copy_cross_device_bytes;
        stats.restore_copy_permission_bytes += event.restore_copy_permission_bytes;
        stats.restore_copy_exclusive_bytes += event.restore_copy_exclusive_bytes;
        stats.restore_copy_other_bytes += event.restore_copy_other_bytes;
    }

    stats
}

#[cfg(test)]
impl BuildEvent {
    /// Minimal event for tests in this crate: identity fields set, everything
    /// else zeroed. Callers set the few fields their case is about.
    pub(crate) fn new_for_test(crate_name: &str, result: EventResult) -> BuildEvent {
        BuildEvent::test_event(crate_name, result, 0, 0, 0, "")
    }

    #[cfg(test)]
    fn test_event(
        crate_name: &str,
        result: EventResult,
        elapsed_ms: u64,
        compile_time_ms: u64,
        size: u64,
        cache_key: &str,
    ) -> BuildEvent {
        BuildEvent {
            ts: Utc::now(),
            crate_name: crate_name.to_string(),
            root: "/work/tree".to_string(),
            version: "0.0.0".to_string(),
            session_id: String::new(),
            result,
            elapsed_ms,
            compile_time_ms,
            size,
            cache_key: cache_key.to_string(),
            schema: 8,
            key_ms: 0,
            key_hash_hits: 0,
            key_hash_misses: 0,
            key_hash_bytes: 0,
            lookup_ms: 0,
            restore_ms: 0,
            store_ms: 0,
            startup_ms: 0,
            dep_info_ms: 0,
            dep_info_runs: 0,
            prediction_mismatches: 0,
            flight_wait_ms: 0,
            permit_wait_ms: 0,
            store_output_blobs: 0,
            store_duplicate_blobs: 0,
            store_new_blobs: 0,
            compiler_runs: 0,
            preprocessor_runs: 0,
            probe_runs: 0,
            reflinked_bytes: 0,
            hardlinked_bytes: 0,
            copied_bytes: 0,
            store_reflinked_bytes: 0,
            store_hardlinked_bytes: 0,
            store_copied_bytes: 0,
            store_copy_cross_device_bytes: 0,
            store_copy_permission_bytes: 0,
            store_copy_ineligible_bytes: 0,
            store_copy_other_bytes: 0,
            restore_copy_cross_device_bytes: 0,
            restore_copy_permission_bytes: 0,
            restore_copy_exclusive_bytes: 0,
            restore_copy_other_bytes: 0,
            passthrough_reason: String::new(),
            store_error: String::new(),
            lookup_rejection: String::new(),
            verify_compare: String::new(),
            fallback: false,
            exit_code: None,
            key_fields: Default::default(),
            key_diff: Vec::new(),
            key_externs: Default::default(),
            key_externs_recorded: false,
            unit_id: String::new(),
            extern_units: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_event(
        crate_name: &str,
        result: EventResult,
        elapsed_ms: u64,
        compile_time_ms: u64,
        size: u64,
        cache_key: &str,
    ) -> BuildEvent {
        BuildEvent::test_event(
            crate_name,
            result,
            elapsed_ms,
            compile_time_ms,
            size,
            cache_key,
        )
    }

    #[test]
    fn test_log_and_read_events() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        let event = BuildEvent {
            ts: Utc::now(),
            crate_name: "serde".to_string(),
            root: "/work/tree".to_string(),
            version: "1.0.210".to_string(),
            session_id: String::new(),
            result: EventResult::LocalHit,
            elapsed_ms: 2,
            compile_time_ms: 250,
            size: 3145728,
            cache_key: "abc123".to_string(),
            schema: 8,
            key_ms: 0,
            key_hash_hits: 0,
            key_hash_misses: 0,
            key_hash_bytes: 0,
            lookup_ms: 0,
            restore_ms: 0,
            store_ms: 0,
            startup_ms: 0,
            dep_info_ms: 0,
            dep_info_runs: 0,
            prediction_mismatches: 0,
            flight_wait_ms: 0,
            permit_wait_ms: 0,
            store_output_blobs: 0,
            store_duplicate_blobs: 0,
            store_new_blobs: 0,
            compiler_runs: 0,
            preprocessor_runs: 0,
            probe_runs: 0,
            reflinked_bytes: 0,
            hardlinked_bytes: 0,
            copied_bytes: 0,
            store_reflinked_bytes: 0,
            store_hardlinked_bytes: 0,
            store_copied_bytes: 0,
            store_copy_cross_device_bytes: 0,
            store_copy_permission_bytes: 0,
            store_copy_ineligible_bytes: 0,
            store_copy_other_bytes: 0,
            restore_copy_cross_device_bytes: 0,
            restore_copy_permission_bytes: 0,
            restore_copy_exclusive_bytes: 0,
            restore_copy_other_bytes: 0,
            passthrough_reason: String::new(),
            store_error: String::new(),
            lookup_rejection: String::new(),
            verify_compare: String::new(),
            fallback: false,
            exit_code: None,
            key_fields: Default::default(),
            key_diff: Vec::new(),
            key_externs: Default::default(),
            key_externs_recorded: false,
            unit_id: String::new(),
            extern_units: Default::default(),
        };

        log_event(&log_path, &event).unwrap();
        log_event(&log_path, &event).unwrap();

        let events = read_events(&log_path).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].crate_name, "serde");
        assert_eq!(events[0].result, EventResult::LocalHit);
    }

    /// Phase arithmetic for the report and the trace: overhead excludes a
    /// compile this process ran but keeps a hit's stored compile cost; the
    /// dep-info pass is inside key time; the remainder never goes negative.
    #[test]
    fn phase_arithmetic_attributes_overhead_and_clamps_the_remainder() {
        // Powers of two, so any swapped operator changes the sum.
        let mut miss = BuildEvent::new_for_test("m", EventResult::Miss);
        miss.elapsed_ms = 300;
        miss.compile_time_ms = 100;
        miss.startup_ms = 1;
        miss.key_ms = 2;
        miss.dep_info_ms = 2;
        miss.dep_info_runs = 1;
        miss.lookup_ms = 4;
        miss.flight_wait_ms = 8;
        miss.permit_wait_ms = 16;
        miss.restore_ms = 32;
        miss.store_ms = 64;
        assert!(!miss.is_hit());
        assert_eq!(miss.overhead_ms(), 200);
        assert_eq!(miss.wait_ms(), 24);
        assert_eq!(miss.attributed_ms(), 127);
        assert_eq!(miss.unattributed_ms(), 73);

        for result in [
            EventResult::LocalHit,
            EventResult::PrefetchHit,
            EventResult::RemoteHit,
        ] {
            let mut hit = miss.clone();
            hit.result = result;
            assert!(hit.is_hit(), "{result:?} is served from cache");
            assert_eq!(
                hit.overhead_ms(),
                300,
                "{result:?}: stored compile cost was not spent here"
            );
        }
        let mut hit = miss.clone();
        hit.result = EventResult::PrefetchHit;
        assert_eq!(
            hit.overhead_ms(),
            300,
            "a hit's compile cost was not spent here"
        );
        assert_eq!(hit.unattributed_ms(), 173);

        let mut dup = miss.clone();
        dup.result = EventResult::Dup;
        assert!(!dup.is_hit());
        assert_eq!(dup.overhead_ms(), 200);

        let mut overlapping = miss.clone();
        overlapping.elapsed_ms = 120;
        assert_eq!(overlapping.overhead_ms(), 20);
        assert_eq!(
            overlapping.unattributed_ms(),
            0,
            "phases summing past the overhead clamp at zero"
        );

        let mut short = miss.clone();
        short.elapsed_ms = 50;
        assert_eq!(short.overhead_ms(), 0, "compile longer than elapsed clamps");
    }

    #[test]
    fn compute_stats_totals_the_wrapper_phases_of_cacheable_outcomes() {
        let mut hit = BuildEvent::new_for_test("h", EventResult::LocalHit);
        hit.elapsed_ms = 40;
        hit.startup_ms = 3;
        hit.key_ms = 10;
        hit.dep_info_ms = 6;
        hit.dep_info_runs = 1;
        hit.prediction_mismatches = 2;
        hit.lookup_ms = 2;
        hit.restore_ms = 5;
        // 40 - (3 + 10 + 2 + 5) = 20
        let mut miss = BuildEvent::new_for_test("m", EventResult::Miss);
        miss.elapsed_ms = 500;
        miss.compile_time_ms = 400;
        miss.startup_ms = 4;
        miss.key_ms = 20;
        miss.dep_info_ms = 9;
        miss.dep_info_runs = 1;
        miss.prediction_mismatches = 3;
        miss.lookup_ms = 1;
        miss.flight_wait_ms = 7;
        miss.permit_wait_ms = 11;
        miss.store_ms = 30;
        // 100 - (4 + 20 + 1 + 18 + 30) = 27
        let mut passthrough = BuildEvent::new_for_test("p", EventResult::Passthrough);
        passthrough.elapsed_ms = 1000;
        passthrough.startup_ms = 100;
        passthrough.dep_info_ms = 100;
        passthrough.dep_info_runs = 100;
        passthrough.prediction_mismatches = 100;
        passthrough.flight_wait_ms = 100;
        passthrough.permit_wait_ms = 100;

        let stats = compute_stats(&[hit, miss, passthrough]);
        assert_eq!(stats.total_startup_ms, 7);
        assert_eq!(stats.total_dep_info_ms, 15);
        assert_eq!(stats.total_dep_info_runs, 2);
        // Deliberately different from the run counts above, so a total that
        // sums the wrong field fails. Cacheable outcomes only: the
        // passthrough's 100 must stay out.
        assert_eq!(stats.total_prediction_mismatches, 5);
        assert_eq!(stats.total_flight_wait_ms, 7);
        assert_eq!(stats.total_permit_wait_ms, 11);
        assert_eq!(stats.total_unattributed_ms, 47);
    }

    #[test]
    fn phase_fields_round_trip_and_default_for_older_events() {
        let mut event = BuildEvent::new_for_test("foo", EventResult::Miss);
        event.startup_ms = 5;
        event.dep_info_ms = 6;
        event.dep_info_runs = 1;
        event.flight_wait_ms = 7;
        event.permit_wait_ms = 8;

        let mut value = serde_json::to_value(&event).unwrap();
        assert_eq!(value["startup_ms"], 5);
        assert_eq!(value["dep_info_ms"], 6);
        assert_eq!(value["dep_info_runs"], 1);
        assert_eq!(value["flight_wait_ms"], 7);
        assert_eq!(value["permit_wait_ms"], 8);
        let round_trip: BuildEvent = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(round_trip.startup_ms, 5);
        assert_eq!(round_trip.dep_info_ms, 6);
        assert_eq!(round_trip.dep_info_runs, 1);
        assert_eq!(round_trip.flight_wait_ms, 7);
        assert_eq!(round_trip.permit_wait_ms, 8);

        let object = value.as_object_mut().unwrap();
        for field in [
            "startup_ms",
            "dep_info_ms",
            "dep_info_runs",
            "flight_wait_ms",
            "permit_wait_ms",
        ] {
            object.remove(field);
        }
        let legacy: BuildEvent = serde_json::from_value(value).unwrap();
        assert_eq!(legacy.startup_ms, 0);
        assert_eq!(legacy.dep_info_ms, 0);
        assert_eq!(legacy.dep_info_runs, 0);
        assert_eq!(legacy.flight_wait_ms, 0);
        assert_eq!(legacy.permit_wait_ms, 0);
        assert_eq!(legacy.wait_ms(), 0);
    }

    #[test]
    fn lookup_rejection_round_trips_and_defaults_for_older_events() {
        let mut event = BuildEvent::new_for_test("foo.c", EventResult::Miss);
        event.lookup_rejection =
            "matching entry lacks dep-info required by this invocation".to_string();

        let mut value = serde_json::to_value(&event).unwrap();
        assert_eq!(
            value["lookup_rejection"],
            "matching entry lacks dep-info required by this invocation"
        );
        let round_trip: BuildEvent = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(round_trip.lookup_rejection, event.lookup_rejection);

        value.as_object_mut().unwrap().remove("lookup_rejection");
        let legacy: BuildEvent = serde_json::from_value(value).unwrap();
        assert!(legacy.lookup_rejection.is_empty());
    }

    #[test]
    fn verify_compare_round_trips_and_defaults_for_older_events() {
        let mut event = BuildEvent::new_for_test("foo", EventResult::LocalHit);
        event.verify_compare = "content: libfoo.rlib (byte mismatch)".to_string();

        let mut value = serde_json::to_value(&event).unwrap();
        assert_eq!(
            value["verify_compare"],
            "content: libfoo.rlib (byte mismatch)"
        );
        let round_trip: BuildEvent = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(round_trip.verify_compare, event.verify_compare);

        value.as_object_mut().unwrap().remove("verify_compare");
        let legacy: BuildEvent = serde_json::from_value(value).unwrap();
        assert!(legacy.verify_compare.is_empty());
    }

    fn test_heartbeat(crate_name: &str, elapsed_s: u64) -> HeartbeatEvent {
        HeartbeatEvent {
            event: HEARTBEAT_EVENT_TAG.to_string(),
            ts: Utc::now(),
            crate_name: crate_name.to_string(),
            root: "/work/tree".to_string(),
            pid: 4242,
            elapsed_s,
            typical_s: Some(471),
            eta_s: Some(211),
            schema: HEARTBEAT_SCHEMA,
        }
    }

    /// #131 wire-compat invariant: a heartbeat line must be INVISIBLE to
    /// BuildEvent-only readers (skipped by parse failure, never mis-parsed as
    /// a degenerate BuildEvent) — and a BuildEvent line must never parse as a
    /// heartbeat.
    #[test]
    fn heartbeat_lines_are_invisible_to_build_event_readers() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        log_event(
            &log_path,
            &test_event("gkrust", EventResult::Miss, 471_000, 471_000, 1024, "k1"),
        )
        .unwrap();
        log_heartbeat(&log_path, &test_heartbeat("gkrust", 260)).unwrap();
        log_event(
            &log_path,
            &test_event("serde", EventResult::LocalHit, 2, 250, 64, "k2"),
        )
        .unwrap();

        let builds = read_events(&log_path).unwrap();
        assert_eq!(
            builds.len(),
            2,
            "BuildEvent readers must skip the heartbeat line"
        );

        let hb_line = serde_json::to_string(&test_heartbeat("gkrust", 260)).unwrap();
        assert!(
            serde_json::from_str::<BuildEvent>(&hb_line).is_err(),
            "heartbeat must not deserialize as BuildEvent"
        );
        let build_line =
            serde_json::to_string(&test_event("gkrust", EventResult::Miss, 1, 1, 1, "k")).unwrap();
        assert!(
            serde_json::from_str::<HeartbeatEvent>(&build_line).is_err(),
            "BuildEvent must not deserialize as heartbeat"
        );
    }

    /// #609: `key_externs` is additive. A schema-11 line written before it
    /// existed must still read, with an empty map rather than a parse error
    /// that would drop the whole event from `report` / `why-miss`.
    #[test]
    fn pre_609_events_deserialize_without_extern_digests() {
        let line = r#"{"ts":"2026-07-01T00:00:00Z","crate_name":"serde",
            "result":"miss","elapsed_ms":1,"size":2,"cache_key":"k","schema":11,
            "key_fields":{"externs":"aaaa"}}"#;
        let event: BuildEvent = serde_json::from_str(line).expect("schema 11 must still parse");
        assert_eq!(event.schema, 11);
        assert!(event.key_externs.is_empty());
        assert_eq!(
            event.key_fields.get("externs").map(String::as_str),
            Some("aaaa")
        );
    }

    /// The map is skipped when empty, so events from builds without
    /// `explain_miss` do not grow by an empty object.
    #[test]
    fn empty_extern_digests_are_not_serialized() {
        let event = test_event("serde", EventResult::Miss, 1, 1, 1, "k");
        let line = serde_json::to_string(&event).unwrap();
        assert!(
            !line.contains("key_externs"),
            "empty key_externs must not be written: {line}"
        );
    }

    /// #131: the mixed-stream tailer yields both record kinds in order, while
    /// the legacy `poll` keeps its builds-only view.
    #[test]
    fn tailer_poll_records_yields_heartbeats_and_builds() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");
        let mut tailer = EventTailer::from_start(log_path.clone());

        log_heartbeat(&log_path, &test_heartbeat("gkrust", 30)).unwrap();
        log_event(
            &log_path,
            &test_event("gkrust", EventResult::Miss, 60_000, 60_000, 1024, "k1"),
        )
        .unwrap();

        let records = tailer.poll_records().unwrap();
        assert_eq!(records.len(), 2);
        assert!(matches!(&records[0], EventRecord::Heartbeat(h) if h.elapsed_s == 30));
        assert!(matches!(&records[1], EventRecord::Build(e) if e.result == EventResult::Miss));

        log_heartbeat(&log_path, &test_heartbeat("gkrust", 60)).unwrap();
        assert!(
            tailer.poll().unwrap().is_empty(),
            "builds-only poll must skip a heartbeat-only append"
        );
    }

    /// #131 ETA source: median over the recent window for the crate, ignoring
    /// other crates, zero compile costs, and extreme outliers.
    #[test]
    fn typical_compile_ms_is_a_robust_per_crate_median() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        assert_eq!(
            typical_compile_ms(&log_path, "gkrust", "/work/tree"),
            None,
            "no history → no estimate"
        );

        // Tight cluster + one extreme outlier + noise from other crates and
        // hit events with no compile cost recorded.
        for ms in [100_000u64, 101_000, 99_000, 100_500, 100_200] {
            log_event(
                &log_path,
                &test_event("gkrust", EventResult::Miss, ms, ms, 1024, "k"),
            )
            .unwrap();
        }
        log_event(
            &log_path,
            &test_event("gkrust", EventResult::Miss, 900_000, 900_000, 1024, "k"),
        )
        .unwrap();
        log_event(
            &log_path,
            &test_event("serde", EventResult::Miss, 2_000, 2_000, 64, "k"),
        )
        .unwrap();
        log_event(
            &log_path,
            &test_event("gkrust", EventResult::LocalHit, 5, 0, 1024, "k"),
        )
        .unwrap();

        let typical = typical_compile_ms(&log_path, "gkrust", "/work/tree").unwrap();
        assert!(
            (99_000..=101_000).contains(&typical),
            "median must sit in the cluster and shed the 900s outlier, got {typical}"
        );

        assert_eq!(
            typical_compile_ms(&log_path, "serde", "/work/tree"),
            Some(2_000)
        );
    }

    #[test]
    fn test_event_tailer() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        let mut tailer = EventTailer::from_start(log_path.clone());

        // No file yet
        assert_eq!(tailer.poll().unwrap().len(), 0);

        // Write an event
        let event = test_event("tokio", EventResult::Miss, 5000, 4800, 8388608, "def456");
        log_event(&log_path, &event).unwrap();

        // Should read the new event
        let new_events = tailer.poll().unwrap();
        assert_eq!(new_events.len(), 1);

        // No new events
        assert_eq!(tailer.poll().unwrap().len(), 0);

        // Write another
        log_event(&log_path, &event).unwrap();
        let new_events = tailer.poll().unwrap();
        assert_eq!(new_events.len(), 1);
    }

    #[test]
    fn test_event_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        // Write many events
        for i in 0..100 {
            let event = test_event(
                &format!("crate_{i}"),
                EventResult::LocalHit,
                1,
                25,
                1024,
                &format!("key_{i}"),
            );
            log_event(&log_path, &event).unwrap();
        }

        // Rotate with small max size (10000 bytes), keep 10 lines
        rotate_if_needed(&log_path, 10000, 10).unwrap();

        let events = read_events(&log_path).unwrap();
        assert_eq!(events.len(), 10);
        // Should keep the last 10
        assert_eq!(events[0].crate_name, "crate_90");

        // Now, if we rotate with a very small max size (e.g. 200 bytes), it should trim down to 1 line
        // because 2 lines would exceed 200 bytes.
        rotate_if_needed(&log_path, 200, 10).unwrap();
        let events = read_events(&log_path).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].crate_name, "crate_99");
    }

    #[test]
    fn test_event_result_display() {
        assert_eq!(EventResult::LocalHit.to_string(), "local_hit");
        assert_eq!(EventResult::PrefetchHit.to_string(), "prefetch_hit");
        assert_eq!(EventResult::RemoteHit.to_string(), "remote_hit");
        assert_eq!(EventResult::Dup.to_string(), "dup");
        assert_eq!(EventResult::Miss.to_string(), "miss");
        assert_eq!(EventResult::Error.to_string(), "error");
        assert_eq!(EventResult::Passthrough.to_string(), "passthrough");
        assert_eq!(EventResult::Skipped.to_string(), "skipped");
    }

    #[test]
    fn test_read_events_nonexistent_file() {
        let events = read_events(Path::new("/nonexistent/events.jsonl")).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn test_read_events_with_invalid_lines() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        let event = test_event("valid", EventResult::Miss, 100, 90, 1024, "key");
        log_event(&log_path, &event).unwrap();

        // Append invalid JSON
        use std::io::Write;
        let mut f = OpenOptions::new().append(true).open(&log_path).unwrap();
        writeln!(f, "this is not json").unwrap();
        writeln!(f, "{{}}").unwrap(); // valid JSON but missing fields

        let events = read_events(&log_path).unwrap();
        // Only the first valid event should be parsed
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].crate_name, "valid");
    }

    #[test]
    fn test_read_events_since() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        let mut old_event = test_event("old", EventResult::Miss, 100, 80, 1024, "key1");
        old_event.ts = Utc::now() - chrono::Duration::hours(2);
        let new_event = test_event("new", EventResult::LocalHit, 10, 250, 512, "key2");

        log_event(&log_path, &old_event).unwrap();
        log_event(&log_path, &new_event).unwrap();

        let since = Utc::now() - chrono::Duration::hours(1);
        let events = read_events_since(&log_path, since).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].crate_name, "new");
    }

    #[test]
    fn test_compute_stats() {
        let events = vec![
            test_event("a", EventResult::LocalHit, 10, 300, 100, "k1"),
            test_event("b", EventResult::PrefetchHit, 5, 250, 150, "k1b"),
            test_event("c", EventResult::RemoteHit, 50, 900, 200, "k2"),
            test_event("dup", EventResult::Dup, 700, 650, 400, "kdup"),
            test_event("d", EventResult::Miss, 1000, 950, 500, "k3"),
            test_event("e", EventResult::Error, 5, 0, 0, "k4"),
            test_event("f", EventResult::Skipped, 0, 0, 0, "k5"),
            test_event("g", EventResult::Passthrough, 25, 0, 0, ""),
        ];

        let stats = compute_stats(&events);
        assert_eq!(stats.total, 8);
        assert_eq!(stats.local_hits, 1);
        assert_eq!(stats.prefetch_hits, 1);
        assert_eq!(stats.remote_hits, 1);
        assert_eq!(stats.dups, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.errors, 1);
        assert_eq!(stats.total_size, 1350);
        assert_eq!(stats.total_elapsed_ms, 1770);
        assert_eq!(stats.hit_elapsed_ms, 65);
        assert_eq!(stats.miss_elapsed_ms, 1700);
        assert_eq!(stats.hit_compile_time_ms, 1450);
        assert_eq!(stats.miss_compile_time_ms, 1600);
    }

    #[test]
    fn test_compute_stats_empty() {
        let stats = compute_stats(&[]);
        assert_eq!(stats.total, 0);
        assert_eq!(stats.local_hits, 0);
    }

    #[test]
    fn compute_stats_aggregates_copy_reasons() {
        // Distinct powers of two so any swapped operator or field still
        // changes the sum.
        let mut event = BuildEvent::new_for_test("a", EventResult::Miss);
        event.store_copy_cross_device_bytes = 1;
        event.store_copy_permission_bytes = 2;
        event.store_copy_ineligible_bytes = 4;
        event.store_copy_other_bytes = 8;
        event.restore_copy_cross_device_bytes = 16;
        event.restore_copy_permission_bytes = 32;
        event.restore_copy_exclusive_bytes = 64;
        event.restore_copy_other_bytes = 128;
        let stats = compute_stats(&[event]);
        assert_eq!(stats.store_copy_cross_device_bytes, 1);
        assert_eq!(stats.store_copy_permission_bytes, 2);
        assert_eq!(stats.store_copy_ineligible_bytes, 4);
        assert_eq!(stats.store_copy_other_bytes, 8);
        assert_eq!(stats.restore_copy_cross_device_bytes, 16);
        assert_eq!(stats.restore_copy_permission_bytes, 32);
        assert_eq!(stats.restore_copy_exclusive_bytes, 64);
        assert_eq!(stats.restore_copy_other_bytes, 128);
    }

    #[test]
    fn test_clear_events() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        let event = test_event("test", EventResult::Miss, 100, 80, 1024, "key");
        log_event(&log_path, &event).unwrap();

        assert!(!read_events(&log_path).unwrap().is_empty());
        clear_events(&log_path).unwrap();
        assert!(read_events(&log_path).unwrap().is_empty());
    }

    #[test]
    fn test_clear_events_nonexistent() {
        clear_events(Path::new("/nonexistent/events.jsonl")).unwrap();
    }

    #[test]
    fn test_rotate_skips_small_file() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        let event = test_event("test", EventResult::Miss, 100, 80, 1024, "key");
        log_event(&log_path, &event).unwrap();

        let size_before = fs::metadata(&log_path).unwrap().len();
        // max_size is larger than the file — should not rotate
        rotate_if_needed(&log_path, 1_000_000, 10).unwrap();
        let size_after = fs::metadata(&log_path).unwrap().len();
        assert_eq!(size_before, size_after);
    }

    #[test]
    fn test_rotate_nonexistent() {
        rotate_if_needed(Path::new("/nonexistent/events.jsonl"), 100, 10).unwrap();
    }

    #[test]
    fn test_rotate_transfers_trims_to_keep_lines_when_oversized() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("transfers.jsonl");
        // 100 lines, well over a 100-byte cap.
        let body: String = (0..100).map(|i| format!("line {i}\n")).collect();
        fs::write(&log_path, body).unwrap();

        rotate_transfers_if_needed(&log_path, 100, 10).unwrap();

        let kept = fs::read_to_string(&log_path).unwrap();
        let lines: Vec<&str> = kept.lines().collect();
        assert_eq!(lines.len(), 10, "should keep the last 10 lines");
        assert_eq!(lines[0], "line 90", "keeps the tail");
        assert_eq!(lines[9], "line 99");
    }

    /// The size-cap re-trim inside rotation removes lines from the front
    /// until the retained bytes fit `max_size` — and stops exactly there,
    /// keeping everything that fits. Pins the byte accounting the #528
    /// cursor contract depends on: a drifting counter would either gut the
    /// retained window or leave it over the cap.
    #[test]
    fn rotation_size_cap_trims_to_exactly_what_fits() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        // 100 fixed-width 9-byte lines.
        let body: String = (0..100).map(|i| format!("line {i:03}\n")).collect();
        fs::write(&log_path, body).unwrap();

        // keep_lines retains 50 lines (450 bytes); the 180-byte cap must trim
        // the front down to exactly the last 20.
        rotate_if_needed(&log_path, 180, 50).unwrap();

        let kept = fs::read_to_string(&log_path).unwrap();
        let lines: Vec<&str> = kept.lines().collect();
        assert_eq!(lines.len(), 20, "exactly the lines that fit are retained");
        assert_eq!(lines[0], "line 080");
        assert_eq!(lines[19], "line 099");
    }

    #[test]
    fn test_rotate_transfers_skips_small_and_nonexistent() {
        // Nonexistent file: no-op.
        rotate_transfers_if_needed(Path::new("/nonexistent/transfers.jsonl"), 100, 10).unwrap();
        // Under the cap: left untouched.
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("transfers.jsonl");
        fs::write(&log_path, "a\nb\n").unwrap();
        rotate_transfers_if_needed(&log_path, 1_000_000, 1).unwrap();
        assert_eq!(fs::read_to_string(&log_path).unwrap(), "a\nb\n");
    }

    #[test]
    fn test_event_tailer_handles_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        let event = test_event("test", EventResult::Miss, 100, 80, 1024, "key");

        // Write several events and advance tailer position
        for _ in 0..10 {
            log_event(&log_path, &event).unwrap();
        }
        let mut tailer = EventTailer::from_start(log_path.clone());
        assert_eq!(tailer.poll().unwrap().len(), 10);

        // Truncate (simulate rotation)
        fs::write(&log_path, "").unwrap();
        log_event(&log_path, &event).unwrap();

        // Tailer should detect truncation and reset
        let events = tailer.poll().unwrap();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_read_transfers_missing_file_is_empty() {
        let got = read_transfers(Path::new("/nonexistent/transfers.jsonl")).unwrap();
        assert!(got.is_empty());
    }

    #[test]
    fn test_read_transfers_skips_blank_and_invalid_lines() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("transfers.jsonl");
        // A blank line and a non-JSON line are both skipped, not fatal.
        fs::write(&log, "\n   \nnot json at all\n{ partial: \n").unwrap();
        let got = read_transfers(&log).unwrap();
        assert!(got.is_empty(), "invalid transfer lines are skipped");
    }

    #[test]
    fn test_event_tailer_handles_rename_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        let event = test_event("test", EventResult::Miss, 100, 80, 1024, "key");

        // Write several events
        for _ in 0..5 {
            log_event(&log_path, &event).unwrap();
        }

        // Start tailing from start
        let mut tailer = EventTailer::from_start(log_path.clone());
        assert_eq!(tailer.poll().unwrap().len(), 5);

        rotate_if_needed(&log_path, 1500, 2).unwrap();

        // Write 10 more events to the newly rotated log (new file will have 2 kept + 10 new = 12 events).
        // This ensures the replacement file is larger than the tailer's previous byte offset (5 events),
        // exercising rotation reconciliation instead of the file_len < position fallback.
        for _ in 0..10 {
            log_event(&log_path, &event).unwrap();
        }

        // Exactly-once across rotation (#528): the 2 retained events were
        // already delivered before the rotation and must NOT be re-delivered;
        // only the 10 new events appear.
        let events = tailer.poll().unwrap();
        assert_eq!(events.len(), 10);
    }

    /// kunobi-ninja/kache#528: a tailer that lags behind a rotation must
    /// receive retained-but-never-delivered events exactly once — the marker
    /// maps its cursor onto the rotated file rather than resetting to 0
    /// (duplicates) or to the end (loss).
    #[test]
    fn lagging_tailer_gets_retained_undelivered_events_exactly_once() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        for i in 0..10 {
            let event = test_event(&format!("c{i}"), EventResult::Miss, 100, 80, 1024, "key");
            log_event(&log_path, &event).unwrap();
        }
        let mut tailer = EventTailer::from_start(log_path.clone());
        assert_eq!(tailer.poll().unwrap().len(), 10);

        // Two more events the tailer has NOT polled yet, then a rotation
        // that keeps the last 4 lines: c8, c9, c10, c11.
        for i in 10..12 {
            let event = test_event(&format!("c{i}"), EventResult::Miss, 100, 80, 1024, "key");
            log_event(&log_path, &event).unwrap();
        }
        // Cap chosen to trigger rotation (12 lines exceed half the file) while
        // comfortably fitting the 4 retained lines, so the size-cap re-trim
        // inside rotation does not eat them.
        let max_size = fs::metadata(&log_path).unwrap().len() / 2;
        rotate_if_needed(&log_path, max_size, 4).unwrap();

        let names: Vec<String> = tailer
            .poll()
            .unwrap()
            .into_iter()
            .map(|e| e.crate_name)
            .collect();
        assert_eq!(
            names,
            vec!["c10".to_string(), "c11".to_string()],
            "undelivered retained events arrive exactly once; delivered ones are not repeated"
        );
        assert!(
            tailer.poll().unwrap().is_empty(),
            "cursor lands exactly at the retained end — nothing re-delivered"
        );
    }

    /// kunobi-ninja/kache#528: a writer that dies after a short write leaves
    /// an unterminated fragment; the NEXT event's append must not merge onto
    /// it into one unparseable line (which would lose the valid event too).
    /// The appender terminates the abandoned fragment first.
    #[test]
    fn abandoned_torn_line_does_not_poison_the_next_event() {
        use std::io::Write as _;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        let first = test_event("first", EventResult::Miss, 100, 80, 1024, "key");
        log_event(&log_path, &first).unwrap();

        // Simulate a writer dying after a short write, lock released.
        {
            let mut file = OpenOptions::new().append(true).open(&log_path).unwrap();
            file.write_all(br#"{"crate_name":"abandoned"#).unwrap();
        }

        let next = test_event("next", EventResult::Miss, 100, 80, 1024, "key");
        log_event(&log_path, &next).unwrap();

        let mut tailer = EventTailer::from_start(log_path);
        let names: Vec<String> = tailer
            .poll()
            .unwrap()
            .into_iter()
            .map(|e| e.crate_name)
            .collect();
        assert_eq!(
            names,
            vec!["first".to_string(), "next".to_string()],
            "the abandoned fragment is skipped alone; the valid event survives"
        );
        assert!(tailer.poll().unwrap().is_empty());
    }

    /// kunobi-ninja/kache#528: an unterminated final line (short write) must
    /// not be consumed — the cursor stays before it so the event is delivered
    /// once the newline lands, instead of being silently lost.
    #[test]
    fn torn_final_line_is_not_consumed_until_terminated() {
        use std::io::Write as _;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        let whole = test_event("whole", EventResult::Miss, 100, 80, 1024, "key");
        log_event(&log_path, &whole).unwrap();

        let torn = test_event("torn", EventResult::Miss, 100, 80, 1024, "key");
        let torn_line = serde_json::to_string(&torn).unwrap();
        let (head, tail) = torn_line.split_at(torn_line.len() / 2);
        {
            let mut f = OpenOptions::new().append(true).open(&log_path).unwrap();
            f.write_all(head.as_bytes()).unwrap();
        }

        let mut tailer = EventTailer::from_start(log_path.clone());
        let first = tailer.poll().unwrap();
        assert_eq!(
            first.len(),
            1,
            "only the newline-terminated line is consumed"
        );
        assert_eq!(first[0].crate_name, "whole");

        // Writer completes the line: the event must surface, not vanish.
        {
            let mut f = OpenOptions::new().append(true).open(&log_path).unwrap();
            f.write_all(tail.as_bytes()).unwrap();
            f.write_all(b"\n").unwrap();
        }
        let second = tailer.poll().unwrap();
        assert_eq!(second.len(), 1, "completed line is delivered exactly once");
        assert_eq!(second[0].crate_name, "torn");
        assert!(tailer.poll().unwrap().is_empty());
    }

    #[test]
    fn test_concurrent_log_append_and_rotate() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        // Seed with a first event to ensure the file exists
        let first_event = test_event("0", EventResult::LocalHit, 10, 10, 100, "key0");
        log_event(&log_path, &first_event).unwrap();

        let running = Arc::new(AtomicBool::new(true));
        let appender_done = Arc::new(AtomicBool::new(false));

        // Spawn a background rotator thread
        let log_path_clone = log_path.clone();
        let running_clone = running.clone();
        let rotator = std::thread::spawn(move || {
            // Keep 100 lines, max_size 65536 to trigger rotation.
            // A larger cap reduces scheduler-dependent flakiness while keeping rotation behavior.
            while running_clone.load(Ordering::Relaxed) {
                let _ = rotate_if_needed(&log_path_clone, 65536, 100);
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        });

        // Spawn appender thread
        let log_path_clone2 = log_path.clone();
        let appender_done_clone = appender_done.clone();
        let appender = std::thread::spawn(move || {
            for i in 1..=200 {
                let event = test_event(&i.to_string(), EventResult::LocalHit, 10, 10, 100, "key");
                log_event(&log_path_clone2, &event).expect("log_event failed");
                std::thread::sleep(std::time::Duration::from_millis(3));
            }
            appender_done_clone.store(true, Ordering::Relaxed);
        });

        // Spawn tailer/monitor thread
        let log_path_clone3 = log_path.clone();
        let running_clone3 = running.clone();
        let appender_done_clone2 = appender_done.clone();
        let tailer_thread = std::thread::spawn(move || {
            let mut tailer = EventTailer::from_start(log_path_clone3);
            let mut polled_ids = std::collections::HashSet::new();

            while running_clone3.load(Ordering::Relaxed)
                || !appender_done_clone2.load(Ordering::Relaxed)
            {
                if let Ok(events) = tailer.poll() {
                    for event in events {
                        if let Ok(id) = event.crate_name.parse::<usize>() {
                            polled_ids.insert(id);
                        }
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(2));
            }

            // Final drain
            if let Ok(events) = tailer.poll() {
                for event in events {
                    if let Ok(id) = event.crate_name.parse::<usize>() {
                        polled_ids.insert(id);
                    }
                }
            }
            polled_ids
        });

        appender.join().unwrap();
        running.store(false, Ordering::Relaxed);
        rotator.join().unwrap();
        let polled_ids = tailer_thread.join().unwrap();

        // Verify the final log file contents
        let content = fs::read_to_string(&log_path).unwrap();

        // 1. Assert trailing newline is preserved exactly
        assert!(content.ends_with("\n"), "log must end with a newline");
        assert!(
            !content.ends_with("\n\n"),
            "log must not end with double newlines"
        );

        // 2. Parse surviving events
        let mut ids = Vec::new();
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let event: BuildEvent =
                serde_json::from_str(line).expect("each line must be valid JSON");
            let id: usize = event
                .crate_name
                .parse()
                .expect("crate name must be parsed as id");
            ids.push(id);
        }

        // 3. Verify no gaps in the surviving sequence in the log file
        assert!(!ids.is_empty(), "at least some events must survive");
        assert!(ids.len() > 1, "multiple events must survive");
        let start = ids[0];
        let end = ids[ids.len() - 1];
        let expected: Vec<usize> = (start..=end).collect();
        assert_eq!(
            ids, expected,
            "there must be no missing events or gaps in the surviving log: got {:?}",
            ids
        );

        // 4. The tailer must still be following the log after rotations — i.e. it
        // observed the final event.
        //
        // It may legitimately miss earlier events: the rotator trims to `keep_lines`
        // concurrently, so an event appended and then rotated away before the tailer
        // next polled was never observable. That is rotation working, not event loss —
        // asserting the tailer sees *every* id makes this test a race it can only win
        // when the tailer happens to outrun the rotator (it flaked on Windows CI).
        //
        // What must hold is that rotation never leaves the tailer stranded on the old
        // inode (kunobi-ninja/kache#518): if it did, it would stop seeing new events
        // entirely and never reach the last one. The deterministic inode-swap guard is
        // `test_event_tailer_handles_rename_rotation`; this is its concurrent analogue.
        //
        // In addition, we verify that the events observed by the tailer form a gap-free
        // sequence (excluding any missed events rotated away before the tailer could poll).
        let mut actual_polled: Vec<usize> = polled_ids.into_iter().collect();
        actual_polled.sort();
        assert!(
            !actual_polled.is_empty(),
            "EventTailer must have observed some events"
        );

        let start_id = actual_polled[0];
        let end_id = *actual_polled.last().unwrap();
        let expected_sequence: Vec<usize> = (start_id..=end_id).collect();
        assert_eq!(
            actual_polled, expected_sequence,
            "EventTailer must not miss any events in its observed sequence: got {:?}",
            actual_polled
        );

        assert_eq!(
            end_id,
            200,
            "EventTailer stopped following the log across rotation: never observed the \
             final event (saw {} ids, max {:?})",
            actual_polled.len(),
            actual_polled.last()
        );
    }
}
