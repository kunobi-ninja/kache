//! Conservative, self-learning policy for incremental rustc passthroughs.
//!
//! A normal cache hit is always preferred. After observing two nearby misses
//! for the same Cargo unit where only source/extern key groups changed, the
//! second compile may seed a private incremental directory. Successful seeds
//! enable a small, time-bounded run of early passthroughs before Kache probes
//! the cache again. Every decision is target-local and protected by a
//! cross-process lock held for the complete compiler invocation.

use crate::args::RustcArgs;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const POLICY_VERSION: &str = "v1";
const STATE_SCHEMA: u32 = 1;
const LEARNING_WINDOW_SECS: u64 = 60;
const ACTIVE_IDLE_SECS: u64 = 30;
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
    /// The wrapper had already chosen an intentional passthrough.
    Immediate,
}

/// Exclusive ownership of one unit's private incremental state.
///
/// The lock is deliberately retained until [`Lease::finish`]. Dropping a
/// lease without finishing leaves an `in_flight` marker; the next process
/// discards the possibly partial rustc state and falls back to the normal
/// cache path.
#[must_use = "the lease must be finished after the compiler exits"]
pub(crate) struct Lease {
    unit: AdaptiveUnit,
    kind: LeaseKind,
    completion: Completion,
    _lock: File,
}

#[derive(Debug)]
enum Completion {
    Seed { observation: Observation },
    Active { state: DiskState },
    Immediate { restore: Option<DiskState> },
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
    Corrupt,
}

impl AdaptiveUnit {
    /// Recognize the deliberately narrow layout supported by auto mode.
    ///
    /// `cargo_primary` should be a snapshot of Cargo's primary-package marker;
    /// passing it in keeps policy tests independent of process-global env.
    /// Eligible invocations must have a stable Cargo unit id and exactly:
    /// `<profile>/deps` plus `<profile>/incremental`, both absolute.
    pub(crate) fn eligible(
        args: &RustcArgs,
        cargo_primary: bool,
        policy_guard: &[u8],
    ) -> Option<Self> {
        if !cargo_primary || !args.is_primary {
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
        if !safe_absolute_path(out_dir) || !safe_absolute_path(original_incremental) {
            return None;
        }
        if out_dir.file_name() != Some(OsStr::new("deps")) {
            return None;
        }
        let profile = out_dir.parent()?;
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
        let unit_dir = profile
            .join("incremental.kache-auto")
            .join(POLICY_VERSION)
            .join(&unit_key);
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

    /// Record a miss that compiled through the normal non-incremental path.
    pub(crate) fn observe_normal_miss(
        &self,
        cache_key: &str,
        fields: &BTreeMap<String, String>,
    ) -> bool {
        self.observe_normal_miss_at(cache_key, fields, now_secs())
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
            LoadedState::Missing => return None,
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
        if !self.store_state(&busy) {
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
        if !self.store_state(&busy) {
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
            LoadedState::Missing => return None,
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
        if !qualifying_pair(prior_observation, &fingerprint, now) {
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
        if !self.store_state(&busy) {
            reset_locked(self);
            return None;
        }
        Some(Lease {
            unit: self.clone(),
            kind: LeaseKind::Seed,
            completion: Completion::Seed { observation },
            _lock: lock,
        })
    }

    fn observe_normal_miss_at(
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
        let stored = self.store_state(&learning);
        drop(lock);
        stored
    }

    fn lock(&self) -> Option<File> {
        if !self.ensure_layout() || unsafe_file(&self.lock_path) {
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
        if meta.file_type().is_symlink() || !meta.is_file() {
            return None;
        }
        match file.try_lock() {
            Ok(()) => Some(file),
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
        let auto_root = profile.join("incremental.kache-auto");
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
            Err(_) => return LoadedState::Corrupt,
        };
        if meta.file_type().is_symlink() || !meta.is_file() || meta.len() > MAX_STATE_BYTES {
            return LoadedState::Corrupt;
        }
        let state: DiskState = match fs::read(&self.state_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        {
            Some(state) => state,
            None => return LoadedState::Corrupt,
        };
        if !valid_state(&state, &self.unit_key) {
            return LoadedState::Corrupt;
        }
        LoadedState::Valid(state)
    }

    fn store_state(&self, state: &DiskState) -> bool {
        if !valid_state(state, &self.unit_key) || unsafe_file(&self.state_path) {
            return false;
        }
        let Ok(bytes) = serde_json::to_vec(state) else {
            return false;
        };
        crate::atomic::atomic_replace(&self.state_path, &bytes).is_ok()
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
    /// Returns whether reusable incremental state remains active/available.
    pub(crate) fn finish(self, success: bool) -> bool {
        self.finish_at(success, now_secs())
    }

    fn finish_at(self, success: bool, now: u64) -> bool {
        if !success || !nonempty_real_directory(&self.unit.rustc_dir) {
            let _ = reset_locked(&self.unit);
            return false;
        }

        let next = match self.completion {
            Completion::Seed { observation } => Some(DiskState {
                schema: STATE_SCHEMA,
                unit_key: self.unit.unit_key.clone(),
                phase: Phase::Active,
                observation: Some(observation),
                active_leases: 0,
                last_used_secs: now,
                in_flight: false,
            }),
            Completion::Active { mut state } => {
                state.in_flight = false;
                state.last_used_secs = now;
                Some(state)
            }
            Completion::Immediate { restore } => restore,
        };

        match next {
            Some(state) if self.unit.store_state(&state) => true,
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

fn qualifying_pair(previous: &Observation, current: &KeyFingerprint, now: u64) -> bool {
    recent(previous.at_secs, now, LEARNING_WINDOW_SECS)
        && previous.cache_key != current.cache_key
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
    let mut index = 0;
    while index < args.len() {
        if args[index].starts_with("-Cincremental=") {
            rewritten.push(format!("-Cincremental={destination}"));
            found = true;
            index += 1;
            continue;
        }
        if args[index].starts_with("--codegen=incremental=") {
            rewritten.push(format!("--codegen=incremental={destination}"));
            found = true;
            index += 1;
            continue;
        }
        if matches!(args[index].as_str(), "-C" | "--codegen")
            && args
                .get(index + 1)
                .is_some_and(|next| next.starts_with("incremental="))
        {
            rewritten.push(args[index].clone());
            rewritten.push(format!("incremental={destination}"));
            found = true;
            index += 2;
            continue;
        }
        rewritten.push(args[index].clone());
        index += 1;
    }
    found.then_some(rewritten)
}

fn strip_incremental(args: &[String]) -> Vec<String> {
    strip_incremental_refs(args).into_iter().cloned().collect()
}

fn strip_incremental_refs(args: &[String]) -> Vec<&String> {
    let mut stripped = Vec::with_capacity(args.len());
    let mut index = 0;
    while index < args.len() {
        if args[index].starts_with("-Cincremental=")
            || args[index].starts_with("--codegen=incremental=")
        {
            index += 1;
            continue;
        }
        if matches!(args[index].as_str(), "-C" | "--codegen")
            && args
                .get(index + 1)
                .is_some_and(|next| next.starts_with("incremental="))
        {
            index += 2;
            continue;
        }
        stripped.push(&args[index]);
        index += 1;
    }
    stripped
}

fn reset_locked(unit: &AdaptiveUnit) -> bool {
    // Keep the in-flight/corrupt state marker when private rustc state could
    // not be removed. Deleting the marker first would make a later immediate
    // lease treat that possibly partial directory as reusable.
    if !remove_path_safely(&unit.rustc_dir) {
        return false;
    }
    remove_path_safely(&unit.state_path)
}

fn remove_path_safely(path: &Path) -> bool {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() || meta.is_file() => {
            fs::remove_file(path).is_ok()
        }
        Ok(meta) if meta.is_dir() => fs::remove_dir_all(path).is_ok(),
        Ok(_) => false,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(_) => false,
    }
}

fn ensure_real_directory(path: &Path) -> bool {
    match fs::symlink_metadata(path) {
        Ok(meta) => meta.is_dir() && !meta.file_type().is_symlink(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path).is_ok() && real_directory(path)
        }
        Err(_) => false,
    }
}

fn real_directory(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok_and(|meta| meta.is_dir() && !meta.file_type().is_symlink())
}

fn nonempty_real_directory(path: &Path) -> bool {
    real_directory(path) && fs::read_dir(path).is_ok_and(|mut entries| entries.next().is_some())
}

fn unsafe_file(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok_and(|meta| meta.file_type().is_symlink() || !meta.is_file())
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
        let unit = AdaptiveUnit::eligible(&args, true, b"").unwrap();
        (temp, args, unit)
    }

    fn teach(unit: &AdaptiveUnit, at: u64) {
        assert!(unit.observe_normal_miss_at(
            &cache_key("first"),
            &fields("stable", "source-a", "extern-a"),
            at,
        ));
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
        assert!(lease.finish_at(true, at + 2));
    }

    #[test]
    fn eligibility_is_narrow_and_cargo_primary() {
        let (_temp, mut args, unit) = fixture();
        assert!(unit.unit_dir.ends_with(&unit.unit_key));
        assert!(AdaptiveUnit::eligible(&args, false, b"").is_none());

        args.extra_filename = Some("-unstable".into());
        assert!(AdaptiveUnit::eligible(&args, true, b"").is_none());

        args.extra_filename = Some("-1234abcd".into());
        args.out_dir = args
            .out_dir
            .as_ref()
            .map(|path| path.with_file_name("build"));
        assert!(AdaptiveUnit::eligible(&args, true, b"").is_none());
    }

    #[test]
    fn policy_guard_changes_the_private_state_identity() {
        let (_temp, args, _) = fixture();
        let first = AdaptiveUnit::eligible(&args, true, b"env-a").unwrap();
        let second = AdaptiveUnit::eligible(&args, true, b"env-b").unwrap();
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
        let unit = AdaptiveUnit::eligible(&args, true, b"").unwrap();
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
        assert!(lease.finish_at(true, 11));
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
        assert!(lease.finish_at(true, 121));

        let active = unit.try_active_at(122).unwrap();
        assert_eq!(active.kind(), LeaseKind::Active);
        assert!(active.finish_at(true, 123));
    }

    #[test]
    fn seed_requires_recent_dynamic_only_change() {
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
        assert!(
            unit.try_seed_at(
                &cache_key("too-late"),
                &fields("stable", "source-b", "extern-a"),
                161,
            )
            .is_none()
        );
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

        assert!(active_lease_allowed(&state, 130, true));
        state.active_leases = MAX_ACTIVE_LEASES;
        assert!(!active_lease_allowed(&state, 130, true));
        state.active_leases = 0;
        assert!(!active_lease_allowed(&state, 131, true));
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
        assert!(!lease.finish_at(true, 12));
        assert!(unit.try_active_at(13).is_none());
        assert!(!unit.state_path.exists());
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

    #[cfg(unix)]
    #[test]
    fn symlinked_managed_paths_fail_closed() {
        use std::os::unix::fs::symlink;

        let (_temp, args, unit) = fixture();
        let incremental = args.incremental.as_ref().unwrap();
        fs::remove_dir(incremental).unwrap();
        symlink("elsewhere", incremental).unwrap();
        assert!(AdaptiveUnit::eligible(&args, true, b"").is_none());

        fs::remove_file(incremental).unwrap();
        fs::create_dir(incremental).unwrap();
        let auto_root = unit.unit_dir.parent().unwrap().parent().unwrap();
        symlink("elsewhere", auto_root).unwrap();
        assert!(unit.try_immediate_at(10).is_none());
    }

    #[test]
    fn invalid_fingerprint_data_cannot_train_policy() {
        let (_temp, _args, unit) = fixture();
        assert!(!unit.observe_normal_miss_at("short", &fields("a", "b", "c"), 1));
        assert!(key_fingerprint(&cache_key("ok"), &BTreeMap::new()).is_none());
        assert!(
            key_fingerprint(
                &cache_key("ok"),
                &BTreeMap::from([("sources".to_string(), "only".to_string())]),
            )
            .is_none()
        );
    }
}
