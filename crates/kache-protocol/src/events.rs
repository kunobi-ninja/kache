use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Coarse class of a free-form passthrough reason.
///
/// Old logs omit the field. `None` is the same absence: a hit, a miss, or a
/// passthrough whose reason is empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MissReason {
    #[default]
    None,
    NotACompile,
    Unsupported,
    Refused,
    Other,
}

impl MissReason {
    /// Classify the wrapper's free-form reason. `kind|detail`, and the legacy
    /// `refused:` prefix, map onto the typed arms. Anything else is [`MissReason::Other`].
    pub fn classify(reason: &str) -> Self {
        let reason = reason.trim();
        if reason.is_empty() {
            return Self::None;
        }
        let kind = if let Some((kind, _)) = reason.split_once('|') {
            kind.trim()
        } else if reason.starts_with("refused:") {
            "refused"
        } else {
            ""
        };
        match kind {
            "not-a-compile" => Self::NotACompile,
            "unsupported" => Self::Unsupported,
            "refused" => Self::Refused,
            _ => Self::Other,
        }
    }

    fn is_none(&self) -> bool {
        *self == Self::None
    }
}

/// A single build event logged by the wrapper.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// Cargo package name captured from the compiler invocation, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rebuilt_package: Option<String>,
    /// Kache's cache/input key for this successful compilation. This is not
    /// Cargo's on-disk final freshness fingerprint; unkeyed compiles omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rebuilt_fingerprint: Option<String>,
    /// Primary observed compiler output; never an inferred output directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rebuilt_path: Option<String>,
    /// Complete observed compiler output set, excluding Kache's store staging.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rebuilt_paths: Vec<String>,
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
    /// 19 = fallback attempt and recovery details.
    /// 20 = per-key first-demand timestamps and remote-check wait.
    /// 21 = daemon publication and its background store time.
    /// 22 = typed miss reason and the C/C++ object output.
    /// 23 = per-phase restore/store copy fallback counters.
    /// 24 = per-variable hashes of compiler-reported environment inputs.
    /// 25 = reasons for compiled results that were not stored.
    /// 26 = successful compilation outputs and captured package identity.
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
    /// Observed at the first lookup, including locally consumed prefetches.
    /// Recorded in memory, then appended with this event; no extra daemon call.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub demands: Vec<kache_core::timeline::KeyDemand>,
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
    /// Why a successful compile did not publish an entry. Empty in older logs.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub skip_reason: String,
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
    /// The store put ran in the daemon, either after a publication handoff
    /// or inside a delegated compile. The store counters describe that put.
    /// Schema 20.
    #[serde(default, skip_serializing_if = "is_false")]
    pub store_handed_off: bool,
    /// Background publication time; excluded from wrapper phase totals.
    #[serde(default)]
    pub daemon_store_ms: u64,
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
    /// Optional wrapper attempt, including failure or sandbox bypass before a direct compile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_attempt: Option<crate::fallback::Attempt>,
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
    /// Normalized environment-input hashes under `explain_miss`. `None`
    /// means unrecorded; an empty map means no compiler-reported variables.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_env_deps: Option<std::collections::BTreeMap<String, String>>,
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
    /// Typed class of [`Self::passthrough_reason`]. Schema 22.
    #[serde(default, skip_serializing_if = "MissReason::is_none")]
    pub miss_reason: MissReason,
    /// Object file a C or C++ compile named with `-o`. Empty for rustc and
    /// for an invocation that named no object. Schema 22.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub object_output: String,
    /// Package a build-script run belongs to. Empty for compiles. Schema 23.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub package: String,
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
    /// Shutdown could not observe every prefetch outcome. Completed counters
    /// remain lower bounds; missing bytes and attempts must not be read as zero.
    #[serde(default)]
    pub incomplete: bool,
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub root: String,
    /// `advisory` | `fallback` | `none` — which planner produced the plan.
    #[serde(default)]
    pub plan_source: String,
    #[serde(default)]
    pub plan_id: String,
    /// `inactivity` | `superseded` | `shutdown` | `shutdown_timeout`.
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
    /// Whether adaptive cancellation or shutdown stopped work for this plan.
    #[serde(default)]
    pub cancelled: bool,
    /// Key-cache LIST refreshes attributed to this session (delta of the
    /// daemon-lifetime counters between plan install and finalization).
    #[serde(default)]
    pub list_requests: u64,
    #[serde(default)]
    pub list_duration_ms: u64,
}

#[cfg(feature = "test-support")]
impl BuildEvent {
    /// Minimal event for tests in this crate: identity fields set, everything
    /// else zeroed. Callers set the few fields their case is about.
    pub fn new_for_test(crate_name: &str, result: EventResult) -> BuildEvent {
        BuildEvent::test_event(crate_name, result, 0, 0, 0, "")
    }

    pub fn test_event(
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
            demands: Vec::new(),
            result,
            elapsed_ms,
            compile_time_ms,
            size,
            cache_key: cache_key.to_string(),
            rebuilt_package: None,
            rebuilt_fingerprint: None,
            rebuilt_path: None,
            rebuilt_paths: Vec::new(),
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
            skip_reason: String::new(),
            store_error: String::new(),
            store_handed_off: false,
            daemon_store_ms: 0,
            lookup_rejection: String::new(),
            verify_compare: String::new(),
            fallback: false,
            fallback_attempt: None,
            exit_code: None,
            key_fields: Default::default(),
            key_diff: Vec::new(),
            key_externs: Default::default(),
            key_env_deps: None,
            key_externs_recorded: false,
            unit_id: String::new(),
            extern_units: Default::default(),
            miss_reason: MissReason::None,
            object_output: String::new(),
            package: String::new(),
        }
    }
}
