//! Conservative policy for isolated incremental rustc passthroughs.
//!
//! A normal cache hit is always preferred. After a build of a Cargo unit (a
//! hit or a miss) and a later miss in the same target where only
//! source/extern key groups changed, however far apart, the miss may seed a
//! private incremental directory. Successful seeds enable a small, time-bounded run
//! of early passthroughs before Kache probes the cache again. An explicit crate force-list can request the same managed
//! directory without the learning step. Every decision is target-local and
//! protected by a cross-process lock held for the complete compiler invocation.
//!
//! A compile error keeps the private state: rustc publishes a session only
//! after a compile without errors and never changes a published one. A crash,
//! a signal, an unknown exit or an error about rustc's incremental files
//! resets the unit (see [`compile_outcome`]).

use crate::args::RustcArgs;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Sibling of Cargo's `<profile>/incremental` that holds every managed unit.
const AUTO_ROOT: &str = "incremental.kache-auto";
const POLICY_VERSION: &str = "v1";
const STATE_SCHEMA: u32 = 1;
/// How long an active unit may sit idle and still skip the cache: a person
/// between two edits, not a return to the checkout after a break.
const ACTIVE_IDLE_SECS: u64 = 15 * 60;
const MAX_ACTIVE_LEASES: u8 = 8;
const MAX_STATE_BYTES: u64 = 64 * 1024;

/// One standard Cargo unit that is safe to manage automatically.
#[derive(Clone, Debug)]
pub(crate) struct AdaptiveUnit {
    original_incremental: PathBuf,
    policy_guard: Vec<u8>,
    unit_key: String,
    unit_dir: PathBuf,
    state_path: PathBuf,
    lock_path: PathBuf,
    rustc_dir: PathBuf,
}

/// Why the policy granted a compiler lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LeaseKind {
    /// A proven-active unit skipped cache-key work.
    Active,
    /// A second qualifying miss is seeding incremental state.
    Seed,
    /// The wrapper chose an intentional or force-listed passthrough.
    Immediate,
}

/// Exclusive ownership of one unit's private incremental state.
///
/// The lock is deliberately retained until [`Lease::finish`]. Dropping a
/// lease without finishing leaves an `in_flight` marker. Kache cannot tell
/// how an abandoned compile ended, so the next process discards the possibly
/// partial rustc state and falls back to the normal cache path.
#[must_use = "the lease must be finished after the compiler exits"]
pub(crate) struct Lease {
    unit: AdaptiveUnit,
    kind: LeaseKind,
    completion: Completion,
    _lock: UnitLock,
}

struct UnitLock(File);

impl Drop for UnitLock {
    fn drop(&mut self) {
        // Closing our descriptor is insufficient if a concurrent fork still
        // holds a duplicate. Release ownership before that child reaches exec.
        let _ = self.0.unlock();
    }
}

#[derive(Debug)]
enum Completion {
    /// `previous` is the state the seed found, restored if it fails.
    Seed {
        observation: Observation,
        previous: DiskState,
    },
    Active {
        state: DiskState,
    },
    Immediate {
        restore: Option<DiskState>,
    },
}

/// How a compile under a lease ended, for its private rustc state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompileOutcome {
    /// rustc exited 0.
    Success,
    /// rustc exited 1 without blaming its incremental files: compile or lint
    /// errors, a linker error, its own Ctrl-C exit, or a Windows forced
    /// termination. None of these changes the newest finalized session.
    CompileError,
    /// An internal compiler error (101), a signal, any other status, a spawn
    /// or pipe failure, or an error about the incremental files.
    Abnormal,
}

/// `signaled`: a Unix signal ended the compiler; `exit_code` then reads 1.
pub(crate) fn compile_outcome(exit_code: i32, signaled: bool, stderr: &str) -> CompileOutcome {
    match (signaled, exit_code) {
        (false, 0) => CompileOutcome::Success,
        (false, 1) if !reports_incremental_failure(stderr) => CompileOutcome::CompileError,
        _ => CompileOutcome::Abnormal,
    }
}

/// Whether rustc reported that its incremental files failed.
fn reports_incremental_failure(stderr: &str) -> bool {
    stderr.lines().any(blames_incremental_state)
}

/// Cargo asks for JSON, so a diagnostic is judged by its own level and
/// message: quoted source and `--explain` text do not count, and warnings (no
/// hard links here, a session rustc could not delete) do not stop the
/// compile. Any other line counts unless it is a warning.
fn blames_incremental_state(line: &str) -> bool {
    // Most lines name neither marker; only the rest are parsed.
    if !names_incremental_state(line) {
        return false;
    }
    match serde_json::from_str::<serde_json::Value>(line) {
        Ok(json) => {
            let field = |name: &str| json.get(name).and_then(serde_json::Value::as_str);
            field("level") != Some("warning")
                && field("message").is_some_and(names_incremental_state)
        }
        Err(_) => !line.starts_with("warning"),
    }
}

/// rustc's session, lock and file errors say "incremental compilation" or
/// name a path under the policy root.
fn names_incremental_state(text: &str) -> bool {
    text.contains("incremental compilation") || text.contains(AUTO_ROOT)
}

/// Stable and mutation-varying portions of one computed Kache key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct KeyFingerprint {
    cache_key: String,
    stable: String,
    sources_externs: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Observation {
    cache_key: String,
    stable: String,
    sources_externs: String,
    at_secs: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Learning,
    Seed,
    Active,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DiskState {
    schema: u32,
    unit_key: String,
    phase: Phase,
    observation: Option<Observation>,
    active_leases: u8,
    last_used_secs: u64,
    in_flight: bool,
}

enum LoadedState {
    Missing,
    Valid(DiskState),
    /// The file is present and readable but its contents are wrong: not a
    /// regular file, oversized, unparsable, or failing validation. Callers
    /// reset the unit — the state is evidence of corruption and relearning is
    /// the only safe response.
    Corrupt,
    /// The environment refused the read: descriptor exhaustion, permissions,
    /// I/O errors. Says nothing about the state's contents, so callers must
    /// decline the current compile WITHOUT resetting — under transient
    /// pressure (a parallel build hitting EMFILE, #756) destroying learned
    /// state would turn a momentary refusal into a permanent relearn.
    Unavailable,
}

impl AdaptiveUnit {
    /// Recognize the deliberately narrow layout supported by managed
    /// incremental modes.
    ///
    /// Cargo passes `-C incremental` only to path packages (workspace members
    /// and path dependencies) in profiles with incremental on, whether or not
    /// the command selects them. That argument, not `CARGO_PRIMARY_PACKAGE`,
    /// marks a unit someone may be editing.
    /// Eligible invocations must have a stable Cargo unit id and exactly:
    /// `<profile>/deps` (or the unit's own `out` in Cargo's per-unit layout)
    /// plus `<profile>/incremental`, both absolute. A build script is not:
    /// Cargo gives it no extra filename, so it has no unit id.
    pub(crate) fn eligible(args: &RustcArgs, policy_guard: &[u8]) -> Option<Self> {
        if !args.is_primary {
            return None;
        }
        let unit_id = args.unit_id()?;
        if !(8..=64).contains(&unit_id.len())
            || !unit_id.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return None;
        }

        let out_dir = args.out_dir.as_ref()?;
        let original_incremental = args.incremental.as_ref()?;
        if !safe_absolute_path(out_dir) {
            return None;
        }
        let profile = crate::cargo_layout::deps_profile(out_dir)?;
        if original_incremental != &profile.join("incremental") {
            return None;
        }
        if !real_directory(profile) || !real_directory(out_dir) {
            return None;
        }
        if path_exists(original_incremental) && !real_directory(original_incremental) {
            return None;
        }

        let unit_key = unit_key(args, original_incremental, policy_guard);
        let unit_dir = profile.join(AUTO_ROOT).join(POLICY_VERSION).join(&unit_key);
        Some(Self {
            original_incremental: original_incremental.clone(),
            policy_guard: policy_guard.to_vec(),
            state_path: unit_dir.join("state.json"),
            lock_path: unit_dir.join("unit.lock"),
            rustc_dir: unit_dir.join("rustc"),
            unit_key,
            unit_dir,
        })
    }

    /// Try the cheap pre-key path for a previously activated unit.
    pub(crate) fn try_active(&self) -> Option<Lease> {
        self.try_active_at(now_secs())
    }

    /// Try an intentional passthrough without teaching or activating policy.
    pub(crate) fn try_immediate(&self) -> Option<Lease> {
        self.try_immediate_at(now_secs())
    }

    /// On a normal cache miss, try to seed or renew incremental state.
    pub(crate) fn try_seed(
        &self,
        cache_key: &str,
        fields: &BTreeMap<String, String>,
    ) -> Option<Lease> {
        self.try_seed_at(cache_key, fields, now_secs())
    }

    /// Record a build that did not use incremental state: a miss compiled
    /// through the normal path, or a cache hit.
    pub(crate) fn observe_build(&self, cache_key: &str, fields: &BTreeMap<String, String>) -> bool {
        self.observe_build_at(cache_key, fields, now_secs())
    }

    /// Whether policy state exists that a seed on this invocation could use.
    /// Only a state file that is definitely missing reads as none, so an
    /// unreadable one keeps the unit keying first.
    pub(crate) fn has_policy_state(&self) -> bool {
        !definitely_missing(&self.state_path)
    }

    /// A cache hit disproves the need for automatic passthrough. Remove both
    /// policy state and its private rustc state while holding the unit lock.
    pub(crate) fn reset(&self) -> bool {
        if definitely_missing(&self.state_path) && definitely_missing(&self.rustc_dir) {
            return true;
        }
        let lock = match self.lock() {
            Some(lock) => lock,
            None => return false,
        };
        let reset = reset_locked(self);
        drop(lock);
        reset
    }

    fn try_active_at(&self, now: u64) -> Option<Lease> {
        // Do not create policy directories on an ordinary first compile.
        // A racing observer can safely teach this process on the next build.
        if definitely_missing(&self.state_path) {
            return None;
        }
        let lock = self.lock()?;
        let state = match self.load_state() {
            LoadedState::Valid(state) if !state.in_flight => state,
            LoadedState::Missing | LoadedState::Unavailable => return None,
            LoadedState::Valid(_) | LoadedState::Corrupt => {
                reset_locked(self);
                return None;
            }
        };
        if !active_lease_allowed(&state, now, nonempty_real_directory(&self.rustc_dir)) {
            return None;
        }

        let mut busy = state;
        busy.active_leases += 1;
        busy.in_flight = true;
        if !self.store_state(&busy, true) {
            reset_locked(self);
            return None;
        }
        Some(Lease {
            unit: self.clone(),
            kind: LeaseKind::Active,
            completion: Completion::Active {
                state: busy.clone(),
            },
            _lock: lock,
        })
    }

    fn try_immediate_at(&self, _now: u64) -> Option<Lease> {
        let lock = self.lock()?;
        let restore = match self.load_state() {
            LoadedState::Missing => None,
            LoadedState::Valid(state) if !state.in_flight => Some(state),
            // Decline rather than start fresh over a file that may be intact.
            LoadedState::Unavailable => return None,
            LoadedState::Valid(_) | LoadedState::Corrupt => {
                reset_locked(self);
                return None;
            }
        };
        if !ensure_real_directory(&self.rustc_dir) {
            return None;
        }

        let mut busy = restore.clone().unwrap_or_else(|| DiskState {
            schema: STATE_SCHEMA,
            unit_key: self.unit_key.clone(),
            phase: Phase::Learning,
            observation: None,
            active_leases: 0,
            last_used_secs: 0,
            in_flight: false,
        });
        busy.in_flight = true;
        if !self.store_state(&busy, true) {
            reset_locked(self);
            return None;
        }
        Some(Lease {
            unit: self.clone(),
            kind: LeaseKind::Immediate,
            completion: Completion::Immediate { restore },
            _lock: lock,
        })
    }

    fn try_seed_at(
        &self,
        cache_key: &str,
        fields: &BTreeMap<String, String>,
        now: u64,
    ) -> Option<Lease> {
        let fingerprint = key_fingerprint(cache_key, fields)?;
        let lock = self.lock()?;
        let previous = match self.load_state() {
            LoadedState::Valid(state) if !state.in_flight => state,
            LoadedState::Missing | LoadedState::Unavailable => return None,
            LoadedState::Valid(_) | LoadedState::Corrupt => {
                reset_locked(self);
                return None;
            }
        };
        if !matches!(previous.phase, Phase::Learning | Phase::Active) {
            reset_locked(self);
            return None;
        }
        let prior_observation = previous.observation.as_ref()?;
        if !qualifying_pair(prior_observation, &fingerprint) {
            return None;
        }
        if !ensure_real_directory(&self.rustc_dir) {
            return None;
        }

        let observation = fingerprint.at(now);
        let busy = DiskState {
            schema: STATE_SCHEMA,
            unit_key: self.unit_key.clone(),
            phase: Phase::Seed,
            observation: Some(observation.clone()),
            active_leases: 0,
            last_used_secs: now,
            in_flight: true,
        };
        if !self.store_state(&busy, true) {
            reset_locked(self);
            return None;
        }
        Some(Lease {
            unit: self.clone(),
            kind: LeaseKind::Seed,
            completion: Completion::Seed {
                observation,
                previous,
            },
            _lock: lock,
        })
    }

    fn observe_build_at(
        &self,
        cache_key: &str,
        fields: &BTreeMap<String, String>,
        now: u64,
    ) -> bool {
        let Some(fingerprint) = key_fingerprint(cache_key, fields) else {
            return false;
        };
        let Some(lock) = self.lock() else {
            return false;
        };

        // A normal compile did not consume this private state. Clear it so a
        // future seed never starts from state associated with rejected stable
        // fields, a failed process, or corrupt metadata.
        if !reset_locked(self) {
            return false;
        }
        let learning = DiskState {
            schema: STATE_SCHEMA,
            unit_key: self.unit_key.clone(),
            phase: Phase::Learning,
            observation: Some(fingerprint.at(now)),
            active_leases: 0,
            last_used_secs: now,
            in_flight: false,
        };
        let stored = self.store_state(&learning, false);
        drop(lock);
        stored
    }

    fn lock(&self) -> Option<UnitLock> {
        if !self.ensure_layout() {
            return None;
        }
        if unsafe_file(&self.lock_path) {
            return None;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&self.lock_path)
            .ok()?;
        let meta = fs::symlink_metadata(&self.lock_path).ok()?;
        if !meta.is_file() {
            return None;
        }
        match file.try_lock() {
            Ok(()) => Some(UnitLock(file)),
            Err(std::fs::TryLockError::WouldBlock | std::fs::TryLockError::Error(_)) => None,
        }
    }

    fn ensure_layout(&self) -> bool {
        let Some(profile) = self
            .unit_dir
            .parent()
            .and_then(Path::parent)
            .and_then(Path::parent)
        else {
            return false;
        };
        if !real_directory(profile)
            || (path_exists(&self.original_incremental)
                && !real_directory(&self.original_incremental))
        {
            return false;
        }
        let auto_root = profile.join(AUTO_ROOT);
        let version_root = auto_root.join(POLICY_VERSION);
        ensure_real_directory(&auto_root)
            && ensure_real_directory(&version_root)
            && ensure_real_directory(&self.unit_dir)
    }

    fn load_state(&self) -> LoadedState {
        let meta = match fs::symlink_metadata(&self.state_path) {
            Ok(meta) => meta,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return LoadedState::Missing;
            }
            // A metadata error other than NotFound (EMFILE, EACCES, EIO) is
            // the environment failing, not the file being wrong.
            Err(_) => return LoadedState::Unavailable,
        };
        if !meta.is_file() || meta.len() > MAX_STATE_BYTES {
            return LoadedState::Corrupt;
        }
        // Read and parse failures are distinct: a failed read is the
        // environment (folding it into Corrupt is what let a transient EMFILE
        // under a parallel test suite destroy learned state, #756); bytes that
        // do not parse are the file itself being wrong.
        let Ok(bytes) = fs::read(&self.state_path) else {
            return LoadedState::Unavailable;
        };
        let state: DiskState = match serde_json::from_slice(&bytes) {
            Ok(state) => state,
            Err(_) => return LoadedState::Corrupt,
        };
        if !valid_state(&state, &self.unit_key) {
            return LoadedState::Corrupt;
        }
        LoadedState::Valid(state)
    }

    /// Write `state`. Lease writes and the reset marker are `durable`; an
    /// observation is not, since it is written only after [`reset_locked`]
    /// removed the private rustc state.
    fn store_state(&self, state: &DiskState, durable: bool) -> bool {
        if !valid_state(state, &self.unit_key) || unsafe_file(&self.state_path) {
            return false;
        }
        let Ok(bytes) = serde_json::to_vec(state) else {
            return false;
        };
        crate::atomic::atomic_replace_deferrable(&self.state_path, &bytes, durable).is_ok()
    }

    /// The state every lease resets, written before private rustc state is
    /// removed (see [`reset_locked`]).
    fn in_flight_marker(&self) -> DiskState {
        DiskState {
            schema: STATE_SCHEMA,
            unit_key: self.unit_key.clone(),
            phase: Phase::Learning,
            observation: None,
            active_leases: 0,
            last_used_secs: 0,
            in_flight: true,
        }
    }
}

impl Lease {
    pub(crate) fn kind(&self) -> LeaseKind {
        self.kind
    }

    /// Rewrite every accepted rustc incremental spelling to this lease's
    /// private directory. If a caller accidentally supplies different args,
    /// fail closed by stripping incremental flags instead of borrowing state
    /// belonging to another unit.
    pub(crate) fn compiler_args(&self, args: &RustcArgs) -> Vec<String> {
        if args.incremental.as_ref() == Some(&self.unit.original_incremental)
            && unit_key(
                args,
                &self.unit.original_incremental,
                &self.unit.policy_guard,
            ) == self.unit.unit_key
            && let Some(rewritten) = rewrite_incremental(&args.all_args, &self.unit.rustc_dir)
        {
            return rewritten;
        }
        strip_incremental(&args.all_args)
    }

    /// Finish a compiler lease and atomically publish the next policy state.
    /// Returns whether the unit kept its private rustc state.
    pub(crate) fn finish(self, outcome: CompileOutcome) -> bool {
        self.finish_at(outcome, now_secs())
    }

    fn finish_at(self, outcome: CompileOutcome, now: u64) -> bool {
        let keep = match outcome {
            // A success that wrote no incremental files proves nothing reusable.
            CompileOutcome::Success => nonempty_real_directory(&self.unit.rustc_dir),
            // rustc publishes a session only after a compile without errors and
            // never changes a published one. An empty directory is kept too: an
            // active lease still requires files.
            CompileOutcome::CompileError => true,
            CompileOutcome::Abnormal => false,
        };
        if !keep {
            let _ = reset_locked(&self.unit);
            return false;
        }

        let next = match (self.completion, outcome) {
            (Completion::Seed { observation, .. }, CompileOutcome::Success) => Some(DiskState {
                schema: STATE_SCHEMA,
                unit_key: self.unit.unit_key.clone(),
                phase: Phase::Active,
                observation: Some(observation),
                active_leases: 0,
                last_used_secs: now,
                in_flight: false,
            }),
            // A failed seed leaves the unit as it found it; the next miss seeds again.
            (Completion::Seed { previous, .. }, _) => Some(previous),
            (Completion::Active { mut state }, _) => {
                state.in_flight = false;
                state.last_used_secs = now;
                Some(state)
            }
            (Completion::Immediate { restore }, _) => restore,
        };

        match next {
            Some(state) if self.unit.store_state(&state, true) => true,
            None => remove_path_safely(&self.unit.state_path),
            Some(_) => {
                reset_locked(&self.unit);
                false
            }
        }
    }
}

impl KeyFingerprint {
    fn at(&self, at_secs: u64) -> Observation {
        Observation {
            cache_key: self.cache_key.clone(),
            stable: self.stable.clone(),
            sources_externs: self.sources_externs.clone(),
            at_secs,
        }
    }
}

/// Split Kache's grouped key digests into stable and mutation-varying parts.
/// Missing/empty grouped data is not evidence and therefore cannot teach the
/// automatic policy.
pub(crate) fn key_fingerprint(
    cache_key: &str,
    fields: &BTreeMap<String, String>,
) -> Option<KeyFingerprint> {
    if !valid_hex_digest(cache_key) || fields.is_empty() {
        return None;
    }
    let mut stable = blake3::Hasher::new();
    stable.update(b"kache-auto-stable-v1\0");
    let mut dynamic = blake3::Hasher::new();
    dynamic.update(b"kache-auto-sources-externs-v1\0");
    let mut stable_count = 0usize;
    let mut dynamic_count = 0usize;
    for (name, value) in fields {
        let target = if matches!(name.as_str(), "sources" | "externs") {
            dynamic_count += 1;
            &mut dynamic
        } else {
            stable_count += 1;
            &mut stable
        };
        fold(target, name.as_bytes());
        fold(target, value.as_bytes());
    }
    if stable_count == 0 || dynamic_count == 0 {
        return None;
    }
    Some(KeyFingerprint {
        cache_key: cache_key.to_owned(),
        stable: stable.finalize().to_hex().to_string(),
        sources_externs: dynamic.finalize().to_hex().to_string(),
    })
}

/// Two misses of one unit that differ only in its sources or dependencies:
/// someone is editing it. The gap between them does not matter; a person
/// takes minutes between edits.
fn qualifying_pair(previous: &Observation, current: &KeyFingerprint) -> bool {
    previous.cache_key != current.cache_key
        && previous.stable == current.stable
        && previous.sources_externs != current.sources_externs
}

fn recent(then: u64, now: u64, window: u64) -> bool {
    now.checked_sub(then).is_some_and(|age| age <= window)
}

fn active_lease_allowed(state: &DiskState, now: u64, rustc_state_ready: bool) -> bool {
    state.phase == Phase::Active
        && state.observation.is_some()
        && state.active_leases < MAX_ACTIVE_LEASES
        && recent(state.last_used_secs, now, ACTIVE_IDLE_SECS)
        && rustc_state_ready
}

fn valid_state(state: &DiskState, unit_key: &str) -> bool {
    if state.schema != STATE_SCHEMA || state.unit_key != unit_key {
        return false;
    }
    if let Some(observation) = &state.observation
        && (!valid_hex_digest(&observation.cache_key)
            || !valid_hex_digest(&observation.stable)
            || !valid_hex_digest(&observation.sources_externs))
    {
        return false;
    }
    match state.phase {
        Phase::Learning => state.observation.is_some() || state.in_flight,
        Phase::Seed => state.observation.is_some() && state.in_flight,
        Phase::Active => state.observation.is_some() && state.active_leases <= MAX_ACTIVE_LEASES,
    }
}

fn unit_key(args: &RustcArgs, original_incremental: &Path, policy_guard: &[u8]) -> String {
    let mut hasher = blake3::Hasher::new();
    fold(&mut hasher, b"kache-incremental-policy");
    fold(&mut hasher, POLICY_VERSION.as_bytes());
    fold(&mut hasher, policy_guard);
    fold(
        &mut hasher,
        original_incremental.as_os_str().as_encoded_bytes(),
    );
    fold(&mut hasher, args.rustc.as_os_str().as_encoded_bytes());
    fold_compiler_stamp(&mut hasher, &args.rustc);
    match &args.inner_rustc {
        Some(inner) => {
            fold(&mut hasher, b"inner");
            fold(&mut hasher, inner.as_os_str().as_encoded_bytes());
            fold_compiler_stamp(&mut hasher, inner);
        }
        None => fold(&mut hasher, b"no-inner"),
    }
    for argument in strip_incremental_refs(&args.all_args) {
        fold(&mut hasher, argument.as_bytes());
    }
    fold(
        &mut hasher,
        if args.skip_path_remap() {
            b"skip-path-remap"
        } else {
            b"use-path-remap"
        },
    );
    hasher.finalize().to_hex().to_string()
}

fn fold_compiler_stamp(hasher: &mut blake3::Hasher, compiler: &Path) {
    let Ok(metadata) = fs::metadata(compiler) else {
        fold(hasher, b"compiler-metadata-unavailable");
        return;
    };
    fold(hasher, &metadata.len().to_le_bytes());
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    fold(hasher, &modified.to_le_bytes());
}

fn fold(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

fn rewrite_incremental(args: &[String], destination: &Path) -> Option<Vec<String>> {
    let destination = destination.to_str()?;
    let mut rewritten = Vec::with_capacity(args.len());
    let mut found = false;
    let mut arguments = args.iter().peekable();
    while let Some(argument) = arguments.next() {
        if argument.starts_with("-Cincremental=") {
            rewritten.push(format!("-Cincremental={destination}"));
            found = true;
            continue;
        }
        if argument.starts_with("--codegen=incremental=") {
            rewritten.push(format!("--codegen=incremental={destination}"));
            found = true;
            continue;
        }
        if matches!(argument.as_str(), "-C" | "--codegen")
            && arguments
                .peek()
                .is_some_and(|next| next.starts_with("incremental="))
        {
            rewritten.push(argument.clone());
            let _incremental_value = arguments.next();
            rewritten.push(format!("incremental={destination}"));
            found = true;
            continue;
        }
        rewritten.push(argument.clone());
    }
    found.then_some(rewritten)
}

fn strip_incremental(args: &[String]) -> Vec<String> {
    strip_incremental_refs(args).into_iter().cloned().collect()
}

fn strip_incremental_refs(args: &[String]) -> Vec<&String> {
    let mut stripped = Vec::with_capacity(args.len());
    let mut arguments = args.iter().peekable();
    while let Some(argument) = arguments.next() {
        if argument.starts_with("-Cincremental=") || argument.starts_with("--codegen=incremental=")
        {
            continue;
        }
        if matches!(argument.as_str(), "-C" | "--codegen")
            && arguments
                .peek()
                .is_some_and(|next| next.starts_with("incremental="))
        {
            let _incremental_value = arguments.next();
            continue;
        }
        stripped.push(argument);
    }
    stripped
}

fn reset_locked(unit: &AdaptiveUnit) -> bool {
    // Removing private rustc state can fail or stop part-way, for example on
    // Ctrl-C. Mark the unit in flight first, durably, so what survives is a
    // state every lease resets, never an old active one beside a partial
    // directory. The marker stays when the removal fails. Without the marker
    // nothing is removed: a whole directory beside its own state is safe to
    // lease. A state path that is not a regular file needs no marker, because
    // every lease already treats it as corrupt.
    if !definitely_missing(&unit.rustc_dir)
        && !unsafe_file(&unit.state_path)
        && !unit.store_state(&unit.in_flight_marker(), true)
    {
        return false;
    }
    if !remove_path_safely(&unit.rustc_dir) {
        return false;
    }
    remove_path_safely(&unit.state_path)
}

fn remove_path_safely(path: &Path) -> bool {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            let file_type = meta.file_type();
            match (
                file_type.is_symlink() || file_type.is_file(),
                file_type.is_dir(),
            ) {
                (true, _) => fs::remove_file(path).is_ok(),
                (false, true) => fs::remove_dir_all(path).is_ok(),
                (false, false) => false,
            }
        }
        Err(error) => matches!(error.kind(), std::io::ErrorKind::NotFound),
    }
}

/// Create `path` unless it exists. A directory another process created first
/// counts, so concurrent first builds do not lose the unit's lock. A symlink
/// never counts. Whatever the creation returned, only the path decides.
fn ensure_real_directory(path: &Path) -> bool {
    let _ = fs::create_dir(path);
    real_directory(path)
}

fn real_directory(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok_and(|meta| meta.is_dir())
}

fn nonempty_real_directory(path: &Path) -> bool {
    real_directory(path) && fs::read_dir(path).is_ok_and(|mut entries| entries.next().is_some())
}

fn unsafe_file(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok_and(|meta| !meta.is_file())
}

fn path_exists(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok()
}

fn definitely_missing(path: &Path) -> bool {
    fs::symlink_metadata(path).is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
}

fn safe_absolute_path(path: &Path) -> bool {
    path.is_absolute()
        && path.components().all(|component| {
            matches!(
                component,
                Component::Prefix(_) | Component::RootDir | Component::Normal(_)
            )
        })
}

fn valid_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn cache_key(label: &str) -> String {
        blake3::hash(label.as_bytes()).to_hex().to_string()
    }

    fn fields(stable: &str, sources: &str, externs: &str) -> BTreeMap<String, String> {
        BTreeMap::from([
            ("args".to_string(), stable.to_string()),
            ("compiler".to_string(), "compiler".to_string()),
            ("externs".to_string(), externs.to_string()),
            ("sources".to_string(), sources.to_string()),
        ])
    }

    fn fixture() -> (TempDir, RustcArgs, AdaptiveUnit) {
        let temp = tempfile::tempdir().unwrap();
        let profile = temp.path().join("target/debug");
        let deps = profile.join("deps");
        let incremental = profile.join("incremental");
        fs::create_dir_all(&deps).unwrap();
        fs::create_dir(&incremental).unwrap();

        let args = RustcArgs::parse(&[
            "/toolchain/bin/rustc".into(),
            "--crate-name".into(),
            "sample".into(),
            "src/lib.rs".into(),
            "--out-dir".into(),
            deps.to_string_lossy().into_owned(),
            "-C".into(),
            format!("incremental={}", incremental.display()),
            "-Cextra-filename=-1234abcd".into(),
        ])
        .unwrap();
        let unit = AdaptiveUnit::eligible(&args, b"").unwrap();
        (temp, args, unit)
    }

    fn teach(unit: &AdaptiveUnit, at: u64) {
        assert!(unit.observe_build_at(
            &cache_key("first"),
            &fields("stable", "source-a", "extern-a"),
            at,
        ));
    }

    fn read_state(unit: &AdaptiveUnit) -> DiskState {
        serde_json::from_slice(&fs::read(&unit.state_path).unwrap()).unwrap()
    }

    fn write_state(unit: &AdaptiveUnit, state: &DiskState) {
        fs::write(&unit.state_path, serde_json::to_vec(state).unwrap()).unwrap();
    }

    fn activate(unit: &AdaptiveUnit, at: u64) {
        teach(unit, at);
        let lease = unit
            .try_seed_at(
                &cache_key("second"),
                &fields("stable", "source-b", "extern-a"),
                at + 1,
            )
            .unwrap();
        fs::write(lease.unit.rustc_dir.join("dep-graph.bin"), b"seed").unwrap();
        assert!(lease.finish_at(CompileOutcome::Success, at + 2));
    }

    #[test]
    fn unit_lock_release_does_not_wait_for_inherited_descriptors() {
        let (_temp, _args, unit) = fixture();
        let owner = unit.lock().unwrap();
        // A concurrent fork can inherit the same open file description until
        // exec closes it. A duplicate reproduces that lifetime without timing.
        let inherited = owner.0.try_clone().unwrap();
        assert!(unit.lock().is_none(), "the owner still holds the lock");
        drop(owner);
        let next = unit
            .lock()
            .expect("release must not wait for an inherited fd");
        drop(next);
        drop(inherited);
    }

    #[test]
    fn finished_and_abandoned_leases_release_inherited_locks() {
        for finish in [false, true] {
            let (_temp, _args, unit) = fixture();
            let lease = unit.try_immediate_at(100).unwrap();
            let inherited = lease._lock.0.try_clone().unwrap();
            assert!(unit.lock().is_none(), "a live lease remains exclusive");
            if finish {
                fs::write(unit.rustc_dir.join("dep-graph.bin"), b"compiled").unwrap();
                assert!(lease.finish_at(CompileOutcome::Success, 101));
            } else {
                drop(lease);
            }
            let next = unit.lock().expect("the lease must release its lock");
            drop(next);
            drop(inherited);
        }
    }

    #[test]
    fn a_per_unit_out_dir_is_eligible_but_its_build_script_is_not() {
        let temp = tempfile::tempdir().unwrap();
        let profile = temp.path().join("target/debug");
        let out = profile.join("build/sample/1234abcd1234abcd/out");
        let incremental = profile.join("incremental");
        fs::create_dir_all(&out).unwrap();
        fs::create_dir(&incremental).unwrap();
        let argv = |crate_name: &str, extra: &[&str]| {
            let mut argv: Vec<String> = vec![
                "/toolchain/bin/rustc".into(),
                "--crate-name".into(),
                crate_name.into(),
                "src/lib.rs".into(),
                "--out-dir".into(),
                out.to_string_lossy().into_owned(),
                "-C".into(),
                format!("incremental={}", incremental.display()),
            ];
            argv.extend(extra.iter().map(|arg| arg.to_string()));
            RustcArgs::parse(&argv).unwrap()
        };
        let unit =
            AdaptiveUnit::eligible(&argv("sample", &["-Cextra-filename=-1234abcd"]), b"").unwrap();
        assert!(
            unit.unit_dir
                .starts_with(profile.join("incremental.kache-auto")),
            "{}",
            unit.unit_dir.display()
        );
        assert!(
            AdaptiveUnit::eligible(&argv("build_script_build", &[]), b"").is_none(),
            "Cargo 1.100 compiles a build script with no extra filename"
        );
    }

    #[test]
    fn eligibility_is_narrow() {
        let (_temp, mut args, unit) = fixture();
        assert!(unit.unit_dir.ends_with(&unit.unit_key));

        let mut registry_like = args.clone();
        registry_like.incremental = None;
        assert!(
            AdaptiveUnit::eligible(&registry_like, b"").is_none(),
            "a unit Cargo compiles without -C incremental (registry, git) is never managed"
        );

        args.extra_filename = Some("-unstable".into());
        assert!(AdaptiveUnit::eligible(&args, b"").is_none());

        args.extra_filename = Some("-1234abcd".into());
        args.out_dir = args
            .out_dir
            .as_ref()
            .map(|path| path.with_file_name("build"));
        assert!(AdaptiveUnit::eligible(&args, b"").is_none());

        let (_temp, mut args, _unit) = fixture();
        args.incremental = args
            .out_dir
            .as_ref()
            .and_then(|out_dir| out_dir.parent())
            .map(|profile| profile.join("incremental/unit"));
        assert!(
            AdaptiveUnit::eligible(&args, b"").is_none(),
            "only the profile's exact incremental sibling is eligible"
        );

        args.incremental = Some(PathBuf::from("relative/incremental"));
        assert!(
            AdaptiveUnit::eligible(&args, b"").is_none(),
            "relative incremental state must fail closed"
        );
    }

    #[test]
    fn immediate_lease_contention_falls_back() {
        let (_temp, _args, unit) = fixture();
        let first = unit.try_immediate_at(10).unwrap();
        assert!(
            unit.try_immediate_at(10).is_none(),
            "a concurrent compiler must not share the private rustc directory"
        );
        assert!(!first.finish_at(CompileOutcome::Abnormal, 11));
    }

    #[test]
    fn policy_guard_changes_the_private_state_identity() {
        let (_temp, args, _) = fixture();
        let first = AdaptiveUnit::eligible(&args, b"env-a").unwrap();
        let second = AdaptiveUnit::eligible(&args, b"env-b").unwrap();
        assert_ne!(first.unit_key, second.unit_key);
        assert_ne!(first.rustc_dir, second.rustc_dir);
    }

    #[test]
    fn compiler_args_rewrite_all_four_forms() {
        let (_temp, mut args, _unit) = fixture();
        let incremental = args.incremental.as_ref().unwrap().display();
        args.all_args = vec![
            format!("-Cincremental={incremental}"),
            "-C".into(),
            format!("incremental={incremental}"),
            format!("--codegen=incremental={incremental}"),
            "--codegen".into(),
            format!("incremental={incremental}"),
            "--test".into(),
            "-Cextra-filename=-1234abcd".into(),
        ];
        let unit = AdaptiveUnit::eligible(&args, b"").unwrap();
        let lease = unit.try_immediate_at(10).unwrap();
        let destination = lease.unit.rustc_dir.display().to_string();
        let rewritten = lease.compiler_args(&args);
        assert_eq!(rewritten.len(), args.all_args.len());
        assert_eq!(
            rewritten
                .iter()
                .filter(|arg| arg.contains("incremental="))
                .count(),
            4
        );
        assert!(
            rewritten
                .iter()
                .filter(|arg| arg.contains("incremental="))
                .all(|arg| arg.ends_with(&destination))
        );
        fs::write(lease.unit.rustc_dir.join("state"), b"ok").unwrap();
        assert!(lease.finish_at(CompileOutcome::Success, 11));
    }

    #[test]
    fn second_distinct_dynamic_miss_seeds_then_activates() {
        let (_temp, _args, unit) = fixture();
        teach(&unit, 100);
        let lease = unit
            .try_seed_at(
                &cache_key("second"),
                &fields("stable", "source-b", "extern-a"),
                120,
            )
            .unwrap();
        assert_eq!(lease.kind(), LeaseKind::Seed);
        fs::write(lease.unit.rustc_dir.join("query-cache.bin"), b"seed").unwrap();
        assert!(lease.finish_at(CompileOutcome::Success, 121));

        let active = unit.try_active_at(122).unwrap();
        assert_eq!(active.kind(), LeaseKind::Active);
        let busy = read_state(&unit);
        assert_eq!(busy.active_leases, 1);
        assert!(busy.in_flight);
        assert!(active.finish_at(CompileOutcome::Success, 123));
    }

    #[test]
    fn state_size_limit_is_inclusive_and_rejects_oversize_data() {
        let (_temp, _args, unit) = fixture();
        teach(&unit, 100);

        let original = fs::read(&unit.state_path).unwrap();
        let mut bytes = original.clone();
        bytes.resize(2_048, b' ');
        fs::write(&unit.state_path, &bytes).unwrap();
        assert!(matches!(unit.load_state(), LoadedState::Valid(_)));

        let mut bytes = original;
        assert!(bytes.len() < MAX_STATE_BYTES as usize);
        bytes.resize(MAX_STATE_BYTES as usize, b' ');
        fs::write(&unit.state_path, &bytes).unwrap();

        assert!(matches!(unit.load_state(), LoadedState::Valid(_)));

        bytes.push(b' ');
        fs::write(&unit.state_path, bytes).unwrap();
        assert!(matches!(unit.load_state(), LoadedState::Corrupt));
    }

    #[test]
    fn state_io_errors_are_unavailable_not_missing_or_corrupt() {
        let (_temp, _args, mut unit) = fixture();
        unit.state_path = unit.unit_dir.join("invalid\0state");
        assert!(matches!(unit.load_state(), LoadedState::Unavailable));
    }

    /// A state file the environment refuses to read (EMFILE under a parallel
    /// suite, EACCES) is not corruption evidence: the policy must decline the
    /// compile and leave the learned state alone. Folding the two together is
    /// how transient descriptor exhaustion inside the Nix build sandbox reset
    /// a freshly seeded unit and failed the suite (#756).
    #[cfg(unix)]
    #[test]
    fn unreadable_state_declines_without_destroying_learned_state() {
        use std::os::unix::fs::PermissionsExt;
        // Root reads through mode 000, which would void the simulation.
        if unsafe { libc::geteuid() } == 0 {
            eprintln!("skipping: running as root, chmod 000 does not deny reads");
            return;
        }
        let (_temp, _args, unit) = fixture();
        activate(&unit, 100);

        let readable = fs::metadata(&unit.state_path).unwrap().permissions();
        fs::set_permissions(&unit.state_path, fs::Permissions::from_mode(0o000)).unwrap();

        assert!(matches!(unit.load_state(), LoadedState::Unavailable));
        // Every load_state caller must decline without resetting, not just
        // the active path: a deleted Unavailable arm in any of them falls
        // through to the Corrupt arm and destroys the state file.
        assert!(
            unit.try_active_at(103).is_none(),
            "active: decline while the state cannot be read"
        );
        assert!(
            unit.try_seed_at(
                &cache_key("third"),
                &fields("stable", "source-c", "extern-a"),
                103,
            )
            .is_none(),
            "seed: decline while the state cannot be read"
        );
        assert!(
            unit.try_immediate_at(103).is_none(),
            "immediate: decline while the state cannot be read"
        );
        assert!(
            path_exists(&unit.state_path),
            "a refused read must not reset the unit"
        );

        // Pressure gone: the learned state is intact and still activates.
        fs::set_permissions(&unit.state_path, readable).unwrap();
        let lease = unit
            .try_active_at(104)
            .expect("learned state survived the transient refusal");
        assert_eq!(lease.kind(), LeaseKind::Active);
        assert!(lease.finish_at(CompileOutcome::Success, 105));
    }

    #[test]
    fn eligibility_and_layout_require_real_directories() {
        let (_temp, args, _unit) = fixture();
        let out_dir = args.out_dir.as_ref().unwrap();
        fs::remove_dir(out_dir).unwrap();
        fs::write(out_dir, b"not a directory").unwrap();
        assert!(AdaptiveUnit::eligible(&args, b"").is_none());

        let (_temp, _args, unit) = fixture();
        fs::remove_dir(&unit.original_incremental).unwrap();
        fs::write(&unit.original_incremental, b"not a directory").unwrap();
        assert!(!unit.ensure_layout());
    }

    #[test]
    fn stale_active_in_flight_state_cannot_grant_a_lease() {
        let (_temp, _args, unit) = fixture();
        activate(&unit, 10);
        let mut state = read_state(&unit);
        state.in_flight = true;
        write_state(&unit, &state);

        assert!(unit.try_active_at(13).is_none());
    }

    #[test]
    fn stale_learning_in_flight_state_cannot_seed() {
        let (_temp, _args, unit) = fixture();
        teach(&unit, 10);
        let mut state = read_state(&unit);
        state.in_flight = true;
        write_state(&unit, &state);

        assert!(
            unit.try_seed_at(
                &cache_key("second"),
                &fields("stable", "source-b", "extern-a"),
                11,
            )
            .is_none()
        );
    }

    #[test]
    fn immediate_lease_restores_existing_learning_state() {
        let (_temp, _args, unit) = fixture();
        teach(&unit, 10);
        let original = fs::read(&unit.state_path).unwrap();

        let lease = unit.try_immediate_at(11).unwrap();
        fs::write(lease.unit.rustc_dir.join("state"), b"ok").unwrap();
        assert!(lease.finish_at(CompileOutcome::Success, 12));

        assert_eq!(fs::read(&unit.state_path).unwrap(), original);
    }

    #[test]
    fn seed_requires_dynamic_only_change() {
        let (_temp, _args, unit) = fixture();
        teach(&unit, 100);
        assert!(
            unit.try_seed_at(
                &cache_key("same-dynamic"),
                &fields("stable", "source-a", "extern-a"),
                101,
            )
            .is_none()
        );
        assert!(
            unit.try_seed_at(
                &cache_key("stable-changed"),
                &fields("different", "source-b", "extern-a"),
                101,
            )
            .is_none()
        );
    }

    #[test]
    fn a_second_edit_an_hour_later_still_seeds() {
        let (_temp, _args, unit) = fixture();
        teach(&unit, 100);
        let lease = unit
            .try_seed_at(
                &cache_key("second"),
                &fields("stable", "source-b", "extern-a"),
                100 + 3600,
            )
            .unwrap();
        assert_eq!(lease.kind(), LeaseKind::Seed);
    }

    #[test]
    fn active_lease_bounds_and_idle_are_checked() {
        let observation = Observation {
            cache_key: cache_key("active"),
            stable: cache_key("stable"),
            sources_externs: cache_key("dynamic"),
            at_secs: 100,
        };
        let mut state = DiskState {
            schema: STATE_SCHEMA,
            unit_key: cache_key("unit"),
            phase: Phase::Active,
            observation: Some(observation),
            active_leases: MAX_ACTIVE_LEASES - 1,
            last_used_secs: 100,
            in_flight: false,
        };

        assert!(active_lease_allowed(&state, 100 + ACTIVE_IDLE_SECS, true));
        assert!(
            active_lease_allowed(&state, 100 + 10 * 60, true),
            "ten minutes between two edits keeps the unit active"
        );
        state.active_leases = MAX_ACTIVE_LEASES;
        assert!(!active_lease_allowed(&state, 100 + ACTIVE_IDLE_SECS, true));
        state.active_leases = 0;
        assert!(!active_lease_allowed(&state, 101 + ACTIVE_IDLE_SECS, true));
        assert!(!active_lease_allowed(&state, 100, false));
    }

    #[test]
    fn seed_success_without_incremental_files_does_not_activate() {
        let (_temp, _args, unit) = fixture();
        teach(&unit, 10);
        let lease = unit
            .try_seed_at(
                &cache_key("second"),
                &fields("stable", "source-b", "extern-a"),
                11,
            )
            .unwrap();
        assert!(!lease.finish_at(CompileOutcome::Success, 12));
        assert!(unit.try_active_at(13).is_none());
        assert!(!unit.state_path.exists());
    }

    #[test]
    fn compile_outcome_trusts_only_an_ordinary_error() {
        use CompileOutcome::{Abnormal, CompileError, Success};
        // rustc 1.99 `--error-format=json` lines, shortened. The type error
        // quotes the phrase outside its message: in source and explanation.
        const TYPE_ERROR: &str = r#"{"$message_type":"diagnostic","message":"mismatched types","code":{"code":"E0308","explanation":"Expected type did not match the received type, with or without incremental compilation.\n"},"level":"error","spans":[],"children":[],"rendered":"error[E0308]: mismatched types\n --> src/lib.rs:1:26\n  |\n1 | pub fn answer() -> u64 { \"incremental compilation\" }\n"}"#;
        const LOCK_FAILED: &str = r#"{"$message_type":"diagnostic","message":"incremental compilation: could not create session directory lock file: Permission denied (os error 13)","code":null,"level":"error","spans":[],"children":[],"rendered":"error: incremental compilation: could not create session directory lock file: Permission denied (os error 13)\n\n"}"#;
        const LOCK_FAILED_WITH_WARNING_CHILD: &str = r#"{"$message_type":"diagnostic","message":"incremental compilation: could not create session directory lock file: Permission denied (os error 13)","code":null,"level":"error","spans":[],"children":[{"message":"the filesystem for the incremental path at /t/debug/incremental.kache-auto/v1/u/rustc/c does not appear to support locking","code":null,"level":"warning","spans":[],"children":[],"rendered":null}],"rendered":"error: incremental compilation: could not create session directory lock file: Permission denied (os error 13)\n\n"}"#;
        const DEP_GRAPH_MOVE_FAILED: &str = r#"{"$message_type":"diagnostic","message":"failed to move dependency graph from `/t/debug/incremental.kache-auto/v1/u/rustc/c/s-a-working/dep-graph.part.bin` to `/t/debug/incremental.kache-auto/v1/u/rustc/c/s-a-working/dep-graph.bin`: No space left on device (os error 28)","code":null,"level":"error","spans":[],"children":[],"rendered":"error: failed to move dependency graph\n\n"}"#;
        // Printed for every session on FAT32, where every type error follows it.
        const NO_HARD_LINKS: &str = r#"{"$message_type":"diagnostic","message":"hard linking files in the incremental compilation cache failed. copying files instead. consider moving the cache directory to a file system which supports hard linking in session dir `/t/debug/incremental.kache-auto/v1/u/rustc/c/s-a-working`","code":null,"level":"warning","spans":[],"children":[],"rendered":"warning: hard linking files in the incremental compilation cache failed\n\n"}"#;
        let no_hard_links_then_type_error = format!("{NO_HARD_LINKS}\n{TYPE_ERROR}\n");

        for (exit_code, signaled, stderr, expected) in [
            (0, false, "", Success),
            (1, false, TYPE_ERROR, CompileError),
            // rustc's own Ctrl-C exit, or a Windows forced termination.
            (1, false, "", CompileError),
            (1, true, "", Abnormal),
            // An internal compiler error.
            (101, false, "", Abnormal),
            (2, false, "", Abnormal),
            // STATUS_ACCESS_VIOLATION.
            (0xC000_0005_u32 as i32, false, "", Abnormal),
            (1, false, LOCK_FAILED, Abnormal),
            (1, false, LOCK_FAILED_WITH_WARNING_CHILD, Abnormal),
            (1, false, DEP_GRAPH_MOVE_FAILED, Abnormal),
            (
                1,
                false,
                no_hard_links_then_type_error.as_str(),
                CompileError,
            ),
            (
                1,
                false,
                "error: incremental compilation: could not create session directory lock file",
                Abnormal,
            ),
            (
                1,
                false,
                "warning: hard linking files in the incremental compilation cache failed\n\
                 error[E0308]: mismatched types",
                CompileError,
            ),
        ] {
            assert_eq!(
                compile_outcome(exit_code, signaled, stderr),
                expected,
                "exit {exit_code}, signaled {signaled}, stderr {stderr:?}"
            );
        }
    }

    #[test]
    fn active_compile_error_keeps_the_lease_and_rustc_state() {
        let (_temp, _args, unit) = fixture();
        activate(&unit, 10);
        let lease = unit.try_active_at(12).unwrap();
        fs::create_dir(unit.rustc_dir.join("s-working")).unwrap();

        assert!(lease.finish_at(CompileOutcome::CompileError, 13));
        let state = read_state(&unit);
        assert_eq!(state.phase, Phase::Active);
        assert!(!state.in_flight);
        assert_eq!(
            state.active_leases, 1,
            "a failed compile still uses a lease"
        );
        assert_eq!(state.last_used_secs, 13);
        assert!(unit.rustc_dir.join("dep-graph.bin").is_file());
        assert!(unit.try_active_at(14).is_some());
    }

    #[test]
    fn failed_seed_restores_the_state_it_found() {
        for session_started in [false, true] {
            let (_temp, _args, unit) = fixture();
            teach(&unit, 100);
            let learned = fs::read(&unit.state_path).unwrap();
            let lease = unit
                .try_seed_at(
                    &cache_key("broken"),
                    &fields("stable", "source-b", "extern-a"),
                    120,
                )
                .unwrap();
            if session_started {
                fs::create_dir(unit.rustc_dir.join("s-working")).unwrap();
            }

            assert!(lease.finish_at(CompileOutcome::CompileError, 121));
            assert_eq!(fs::read(&unit.state_path).unwrap(), learned);
            assert!(unit.rustc_dir.is_dir());
            assert!(unit.try_active_at(122).is_none());
            let fixed = unit
                .try_seed_at(
                    &cache_key("fixed"),
                    &fields("stable", "source-c", "extern-a"),
                    123,
                )
                .expect("the fix seeds");
            assert_eq!(fixed.kind(), LeaseKind::Seed);
        }
    }

    #[test]
    fn failed_reseed_keeps_the_active_rustc_state() {
        let (_temp, _args, unit) = fixture();
        activate(&unit, 10);
        let mut exhausted = read_state(&unit);
        exhausted.active_leases = MAX_ACTIVE_LEASES;
        write_state(&unit, &exhausted);
        let saved = fs::read(&unit.state_path).unwrap();
        assert!(unit.try_active_at(11).is_none());

        let lease = unit
            .try_seed_at(
                &cache_key("third"),
                &fields("stable", "source-c", "extern-a"),
                20,
            )
            .unwrap();
        assert!(lease.finish_at(CompileOutcome::CompileError, 21));
        assert_eq!(fs::read(&unit.state_path).unwrap(), saved);
        assert!(unit.rustc_dir.join("dep-graph.bin").is_file());
    }

    #[test]
    fn immediate_compile_error_restores_metadata_and_keeps_rustc_state() {
        for learned in [false, true] {
            let (_temp, _args, unit) = fixture();
            if learned {
                teach(&unit, 10);
            }
            let before = fs::read(&unit.state_path).ok();
            let lease = unit.try_immediate_at(11).unwrap();
            fs::write(unit.rustc_dir.join("dep-graph.bin"), b"compiled").unwrap();

            assert!(lease.finish(CompileOutcome::CompileError));
            assert_eq!(fs::read(&unit.state_path).ok(), before);
            assert!(unit.rustc_dir.join("dep-graph.bin").is_file());
        }
    }

    #[test]
    fn abnormal_exit_resets_every_lane() {
        for kind in [LeaseKind::Active, LeaseKind::Seed, LeaseKind::Immediate] {
            let (_temp, _args, unit) = fixture();
            activate(&unit, 10);
            let lease = match kind {
                LeaseKind::Active => unit.try_active_at(12),
                LeaseKind::Seed => unit.try_seed_at(
                    &cache_key("third"),
                    &fields("stable", "source-c", "extern-a"),
                    12,
                ),
                LeaseKind::Immediate => unit.try_immediate_at(12),
            }
            .unwrap();
            assert_eq!(lease.kind(), kind);

            assert!(!lease.finish_at(CompileOutcome::Abnormal, 13));
            assert!(!unit.state_path.exists(), "{kind:?}");
            assert!(!unit.rustc_dir.exists(), "{kind:?}");
        }
    }

    #[test]
    fn cache_hit_reset_removes_state_and_rustc() {
        let (_temp, _args, unit) = fixture();
        activate(&unit, 10);
        assert!(unit.state_path.exists());
        assert!(unit.rustc_dir.exists());
        assert!(unit.reset());
        assert!(!unit.state_path.exists());
        assert!(!unit.rustc_dir.exists());
    }

    #[test]
    fn cache_hit_reset_removes_state_when_rustc_directory_is_missing() {
        let (_temp, _args, unit) = fixture();
        teach(&unit, 10);
        assert!(unit.state_path.exists());
        assert!(!unit.rustc_dir.exists());

        assert!(unit.reset());
        assert!(!unit.state_path.exists());
    }

    #[test]
    fn only_recorded_policy_state_can_seed() {
        let (_temp, _args, unit) = fixture();
        assert!(!unit.has_policy_state());
        assert!(
            unit.try_seed_at(
                &cache_key("second"),
                &fields("stable", "source-b", "extern-a"),
                101,
            )
            .is_none(),
            "without state no seed can run, so the compile need not wait for its key"
        );
        teach(&unit, 100);
        assert!(unit.has_policy_state());
        assert!(unit.reset());
        assert!(!unit.has_policy_state());
    }

    #[test]
    fn invalid_state_is_not_stored() {
        let (_temp, _args, unit) = fixture();
        teach(&unit, 10);
        let original = fs::read(&unit.state_path).unwrap();
        let mut invalid = read_state(&unit);
        invalid.schema += 1;

        assert!(!unit.store_state(&invalid, true));
        assert_eq!(fs::read(&unit.state_path).unwrap(), original);
    }

    #[test]
    fn state_validation_enforces_each_phase_invariant() {
        let (_temp, _args, unit) = fixture();
        teach(&unit, 10);
        let mut state = read_state(&unit);

        state.phase = Phase::Learning;
        state.observation = None;
        state.in_flight = false;
        assert!(!valid_state(&state, &unit.unit_key));
        state.in_flight = true;
        assert!(valid_state(&state, &unit.unit_key));
        state.observation = Some(
            KeyFingerprint {
                cache_key: cache_key("learning"),
                stable: cache_key("stable"),
                sources_externs: cache_key("dynamic"),
            }
            .at(10),
        );
        state.in_flight = false;
        assert!(valid_state(&state, &unit.unit_key));

        state.phase = Phase::Seed;
        assert!(!valid_state(&state, &unit.unit_key));
        state.in_flight = true;
        assert!(valid_state(&state, &unit.unit_key));
        state.observation = None;
        assert!(!valid_state(&state, &unit.unit_key));

        state.phase = Phase::Active;
        state.active_leases = 0;
        assert!(!valid_state(&state, &unit.unit_key));
        state.observation = Some(
            KeyFingerprint {
                cache_key: cache_key("active"),
                stable: cache_key("stable"),
                sources_externs: cache_key("dynamic"),
            }
            .at(10),
        );
        state.active_leases = MAX_ACTIVE_LEASES;
        assert!(valid_state(&state, &unit.unit_key));
        state.active_leases = MAX_ACTIVE_LEASES + 1;
        assert!(!valid_state(&state, &unit.unit_key));
    }

    #[test]
    fn state_validation_rejects_each_invalid_observation_digest() {
        let (_temp, _args, unit) = fixture();
        teach(&unit, 10);
        let state = read_state(&unit);

        for field in ["cache", "stable", "dynamic"] {
            let mut invalid = state.clone();
            let observation = invalid.observation.as_mut().unwrap();
            match field {
                "cache" => observation.cache_key = "not-a-digest".into(),
                "stable" => observation.stable = "not-a-digest".into(),
                "dynamic" => observation.sources_externs = "not-a-digest".into(),
                _ => unreachable!(),
            }
            assert!(!valid_state(&invalid, &unit.unit_key), "accepted {field}");
        }
    }

    #[test]
    fn corrupt_or_interrupted_state_fails_closed() {
        for interrupted in [false, true] {
            let (_temp, _args, unit) = fixture();
            assert!(unit.ensure_layout());
            fs::create_dir(&unit.rustc_dir).unwrap();
            fs::write(unit.rustc_dir.join("partial"), b"bad").unwrap();
            if interrupted {
                let state = DiskState {
                    schema: STATE_SCHEMA,
                    unit_key: unit.unit_key.clone(),
                    phase: Phase::Learning,
                    observation: None,
                    active_leases: 0,
                    last_used_secs: 11,
                    in_flight: true,
                };
                fs::write(&unit.state_path, serde_json::to_vec(&state).unwrap()).unwrap();
            } else {
                fs::write(&unit.state_path, b"{not json").unwrap();
            }

            let lease = if interrupted {
                unit.try_immediate_at(12)
            } else {
                unit.try_active_at(12)
            };
            assert!(lease.is_none());
            // Lock/reset can fail transiently under host resource pressure.
            // Either cleanup completed, or the blocking marker remains;
            // partial rustc state must never survive without that marker.
            assert!(unit.state_path.exists() || !unit.rustc_dir.exists());
        }
    }

    #[cfg(unix)]
    #[test]
    fn failed_rustc_cleanup_keeps_the_fail_closed_marker() {
        use std::os::unix::net::UnixListener;

        let (temp, _args, mut unit) = fixture();
        // Keep the socket path below the small sockaddr_un limit on macOS.
        unit.rustc_dir = temp.path().join("blocked-rustc-state");
        assert!(unit.ensure_layout());
        let lease = unit.try_immediate_at(10).unwrap();
        drop(lease);
        assert!(unit.state_path.exists());

        // A Unix socket is neither a regular file nor a directory, so the
        // conservative remover deliberately refuses it.
        fs::remove_dir(&unit.rustc_dir).unwrap();
        let _socket = UnixListener::bind(&unit.rustc_dir).unwrap();
        assert!(!reset_locked(&unit));
        assert!(unit.state_path.exists());
        assert!(unit.try_immediate_at(11).is_none());
    }

    /// An active unit leaves its edit loop through an observation, which
    /// removes its rustc state first. A removal that fails part-way, or that
    /// Ctrl-C cuts short, must not leave the old active state beside what
    /// remains: the next build would lease a partial session.
    #[cfg(unix)]
    #[test]
    fn a_failed_rustc_removal_never_leaves_a_leasable_state() {
        use std::os::unix::fs::PermissionsExt;
        // Root unlinks through mode 0500, which would void the simulation.
        if unsafe { libc::geteuid() } == 0 {
            eprintln!("skipping: running as root, chmod 0500 does not deny unlinks");
            return;
        }
        let (_temp, _args, unit) = fixture();
        activate(&unit, 10);
        let stuck = unit.rustc_dir.join("s-stuck");
        fs::create_dir(&stuck).unwrap();
        fs::write(stuck.join("work-product.o"), b"object").unwrap();
        fs::set_permissions(&stuck, fs::Permissions::from_mode(0o500)).unwrap();

        let observed = unit.observe_build_at(
            &cache_key("hit"),
            &fields("stable", "source-c", "extern-a"),
            12,
        );
        let leased = unit.try_active_at(13);
        fs::set_permissions(&stuck, fs::Permissions::from_mode(0o700)).unwrap();

        assert!(!observed, "the removal failed, so nothing was observed");
        assert!(
            nonempty_real_directory(&unit.rustc_dir),
            "the simulation must leave part of the rustc state behind"
        );
        assert!(
            leased.is_none(),
            "a partial rustc directory must not be leased"
        );
    }

    /// Without its in-flight marker a reset removes nothing, so a removal cut
    /// short can never leave part of the rustc state beside an active state.
    #[cfg(unix)]
    #[test]
    fn a_reset_that_cannot_mark_the_unit_removes_nothing() {
        use std::os::unix::fs::PermissionsExt;
        // Root writes through mode 0500, which would void the simulation.
        if unsafe { libc::geteuid() } == 0 {
            eprintln!("skipping: running as root, chmod 0500 does not deny writes");
            return;
        }
        let (_temp, _args, unit) = fixture();
        activate(&unit, 10);
        let active = fs::read(&unit.state_path).unwrap();
        // The marker is written through a temporary file beside state.json.
        // rustc/ itself stays writable.
        fs::set_permissions(&unit.unit_dir, fs::Permissions::from_mode(0o500)).unwrap();
        let observed = unit.observe_build_at(
            &cache_key("hit"),
            &fields("stable", "source-c", "extern-a"),
            12,
        );
        fs::set_permissions(&unit.unit_dir, fs::Permissions::from_mode(0o700)).unwrap();

        assert!(!observed, "the marker failed, so nothing was observed");
        assert_eq!(fs::read(&unit.state_path).unwrap(), active);
        assert!(
            unit.rustc_dir.join("dep-graph.bin").is_file(),
            "rustc/ must stay whole beside the state it belongs to"
        );
        let lease = unit
            .try_active_at(13)
            .expect("a whole rustc directory may still be leased");
        assert!(lease.finish_at(CompileOutcome::Success, 14));
    }

    /// Every lease treats a state path that is not a regular file as corrupt,
    /// so a reset removes it, and any rustc state, without a marker.
    #[test]
    fn a_reset_removes_a_state_path_that_is_not_a_file() {
        for rustc_state in [false, true] {
            let (_temp, _args, unit) = fixture();
            assert!(unit.ensure_layout());
            fs::create_dir(&unit.state_path).unwrap();
            if rustc_state {
                fs::create_dir(&unit.rustc_dir).unwrap();
                fs::write(unit.rustc_dir.join("dep-graph.bin"), b"old").unwrap();
            }
            assert!(unit.reset(), "rustc state: {rustc_state}");
            assert!(!path_exists(&unit.state_path), "rustc state: {rustc_state}");
            assert!(!path_exists(&unit.rustc_dir), "rustc state: {rustc_state}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_managed_paths_fail_closed() {
        use std::os::unix::fs::symlink;

        let (_temp, args, unit) = fixture();
        let incremental = args.incremental.as_ref().unwrap();
        fs::remove_dir(incremental).unwrap();
        symlink("elsewhere", incremental).unwrap();
        assert!(AdaptiveUnit::eligible(&args, b"").is_none());

        fs::remove_file(incremental).unwrap();
        fs::create_dir(incremental).unwrap();
        let auto_root = unit.unit_dir.parent().unwrap().parent().unwrap();
        symlink("elsewhere", auto_root).unwrap();
        assert!(unit.try_immediate_at(10).is_none());
    }

    #[test]
    fn invalid_fingerprint_data_cannot_train_policy() {
        let (_temp, _args, unit) = fixture();
        assert!(!unit.observe_build_at("short", &fields("a", "b", "c"), 1));
        assert!(key_fingerprint("short", &fields("a", "b", "c")).is_none());
        assert!(key_fingerprint(&cache_key("ok"), &BTreeMap::new()).is_none());
        assert!(
            key_fingerprint(
                &cache_key("ok"),
                &BTreeMap::from([("sources".to_string(), "only".to_string())]),
            )
            .is_none()
        );
    }

    #[test]
    fn compiler_metadata_contributes_to_unit_identity() {
        let (temp, mut args, unit) = fixture();
        let compiler = temp.path().join("rustc");
        fs::write(&compiler, b"one").unwrap();
        args.rustc = compiler.clone();
        let first = unit_key(&args, &unit.original_incremental, b"");

        fs::write(&compiler, b"different length").unwrap();
        let second = unit_key(&args, &unit.original_incremental, b"");
        assert_ne!(first, second);
    }

    #[test]
    fn stripping_incremental_flags_preserves_every_other_argument() {
        for (input, expected) in [
            (
                &["-Cincremental=first", "--crate-name", "sample"][..],
                &["--crate-name", "sample"][..],
            ),
            (
                &["--codegen=incremental=second", "--crate-name", "sample"][..],
                &["--crate-name", "sample"][..],
            ),
            (
                &["-C", "incremental=third", "--crate-name", "sample"][..],
                &["--crate-name", "sample"][..],
            ),
            (
                &["--codegen", "incremental=fourth", "--crate-name", "sample"][..],
                &["--crate-name", "sample"][..],
            ),
            (
                &["--cfg", "incremental=kept", "sentinel"][..],
                &["--cfg", "incremental=kept", "sentinel"][..],
            ),
            (
                &["-C", "opt-level=2", "sentinel"][..],
                &["-C", "opt-level=2", "sentinel"][..],
            ),
        ] {
            let args: Vec<String> = input.iter().map(|arg| (*arg).to_owned()).collect();
            assert_eq!(strip_incremental(&args), expected, "input: {input:?}");
        }
    }

    #[test]
    fn path_and_digest_guards_reject_each_invalid_shape() {
        let temp = tempfile::tempdir().unwrap();
        let regular = temp.path().join("regular");
        let directory = temp.path().join("directory");
        let missing = temp.path().join("missing");
        fs::write(&regular, b"file").unwrap();
        fs::create_dir(&directory).unwrap();

        assert!(!unsafe_file(&regular));
        assert!(unsafe_file(&directory));
        assert!(!unsafe_file(&missing));
        assert!(definitely_missing(&missing));
        assert!(ensure_real_directory(&missing));
        assert!(real_directory(&missing));
        assert!(!definitely_missing(&missing));
        assert!(
            ensure_real_directory(&missing),
            "a directory that already exists, as when another build created it first, counts"
        );
        assert!(!ensure_real_directory(&regular));

        assert!(safe_absolute_path(&regular));
        assert!(!safe_absolute_path(Path::new("relative/path")));
        assert!(!safe_absolute_path(&temp.path().join("a/../b")));

        assert!(valid_hex_digest(&"a".repeat(64)));
        assert!(!valid_hex_digest("a"));
        assert!(!valid_hex_digest(&format!("{}g", "a".repeat(63))));
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let observed = now_secs();
        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!((before..=after).contains(&observed));

        assert!(remove_path_safely(&regular));
        assert!(remove_path_safely(&directory));
        assert!(!regular.exists());
        assert!(!directory.exists());
        assert!(remove_path_safely(&regular));
        assert!(!remove_path_safely(Path::new("\0")));
    }

    #[cfg(unix)]
    #[test]
    fn managed_symlinks_are_removed_without_following_them() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("target");
        let link = temp.path().join("link");
        fs::write(&target, b"keep").unwrap();
        symlink(&target, &link).unwrap();

        assert!(unsafe_file(&link));
        assert!(remove_path_safely(&link));
        assert!(!link.exists());
        assert_eq!(fs::read(&target).unwrap(), b"keep");
    }
}
