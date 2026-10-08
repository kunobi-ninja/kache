//! Immutable observations of one build session, separate from planner manifests.
//!
//! Reports describe compiler events, not successful asynchronous artifact uploads.
//! A missing compatibility fact remains unknown; consumers must not infer it from
//! the legacy manifest key or from a long-lived daemon's environment.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::Path;

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

use crate::events::{BuildEvent, EventResult};
use crate::remote_backend::{PutIfAbsentResult, RemoteBackend};

pub const REPORT_SCHEMA: u32 = 1;
pub const MAX_REPORT_BYTES: u64 = 8 << 20;
pub const MAX_REPORT_ENTRIES: usize = 50_000;
pub const MAX_REPORT_KEYS: usize = 1_024;
const REPORT_PREFIX: &str = "_manifests/build-reports/v1";

/// Producer facts. Labels and build shape are caller declarations, not proofs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildIdentity {
    pub repository: Option<String>,
    pub target: Option<String>,
    pub toolchain_hash: Option<String>,
    pub profile: Option<String>,
    pub build_shape: Option<String>,
    pub lock_digest: Option<String>,
}

/// Local-only producer snapshot. The root path never leaves the machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerContext {
    pub session_id: String,
    pub root: String,
    pub namespace: String,
    pub identity: BuildIdentity,
    pub commit: Option<String>,
    pub git_ref: Option<String>,
    /// Unknown ancestry is empty, rather than guessed from the daemon checkout.
    pub parent_commits: Vec<String>,
    pub captured_at_ms: u64,
    pub ci_started_at_ms: Option<u64>,
    #[serde(default)]
    pub facts: Option<ProducerFacts>,
}

/// Local comparison facts. Fingerprints avoid repeated compiler probes and
/// full lockfile hashing on warm hits. A mismatch invalidates compatibility.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerFacts {
    pub namespace: String,
    pub repository: Option<String>,
    pub target: Option<String>,
    pub profile: Option<String>,
    pub build_shape: Option<String>,
    pub commit: Option<String>,
    pub git_ref: Option<String>,
    pub parent_commits: Vec<String>,
    pub ci_started_at_ms: Option<u64>,
    pub compiler_stamp: Option<crate::cache_key::FileFingerprint>,
    pub compiler_selector: Option<String>,
    pub lock_stamp: Option<crate::cache_key::FileFingerprint>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReportEntry {
    pub cache_key: String,
    pub crate_name: String,
    pub result: EventResult,
    pub started_at_ms: u64,
    pub finished_at_ms: u64,
    pub start_offset_ms: u64,
    pub ci_start_offset_ms: Option<u64>,
    pub elapsed_ms: u64,
    pub compile_time_ms: u64,
    pub artifact_size: u64,
    pub event_schema: u32,
    pub demands: Vec<kache_core::timeline::KeyDemand>,
    pub artifact_status: ArtifactStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactStatus {
    Observed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BuildReport {
    pub schema: u32,
    pub session_id: String,
    pub root_hash: String,
    pub namespace: String,
    pub identity: BuildIdentity,
    pub commit: Option<String>,
    pub git_ref: Option<String>,
    pub parent_commits: Vec<String>,
    /// Earliest observed invocation start, not a claim about Cargo/CI start.
    pub started_at_ms: u64,
    pub finished_at_ms: u64,
    pub producer_captured_at_ms: Option<u64>,
    /// Ordered event-log observations. Repeated cache keys are retained.
    pub entries: Vec<ReportEntry>,
}

pub fn toolchain_hash(rustc: &Path) -> Option<String> {
    crate::cache_key::rustc_version_text(rustc)
        .filter(|version| {
            !version.contains('\u{fffd}') && crate::cache_key::rustc_host_triple(version).is_some()
        })
        .map(|version| blake3::hash(version.as_bytes()).to_hex().to_string())
}

fn stable_toolchain_hash(rustc: &Path, facts: &ProducerFacts) -> Option<String> {
    // A session can span multiple Cargo commands. Without both comparison
    // facts, a later compiler change could retain the first invocation's label.
    facts.compiler_stamp.as_ref()?;
    facts.compiler_selector.as_ref()?;
    toolchain_hash(rustc)
}

pub fn lock_digest(lock_path: &Path) -> Option<String> {
    let bytes = std::fs::read(lock_path).ok()?;
    (!bytes.is_empty()).then(|| blake3::hash(&bytes).to_hex().to_string())
}

pub fn root_hash(root: &str) -> String {
    blake3::hash(root.as_bytes()).to_hex().to_string()
}

fn nonempty(lookup: &impl Fn(&str) -> Option<String>, name: &str) -> Option<String> {
    lookup(name)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Ordinary local builds must not gain compiler probes before their fast paths.
pub fn producer_is_declared(lookup: impl Fn(&str) -> Option<String>) -> bool {
    nonempty(&lookup, "KACHE_NAMESPACE").is_some()
        || nonempty(&lookup, "KACHE_REPOSITORY").is_some()
}

pub fn producer_facts(
    args: &crate::args::RustcArgs,
    lock_path: Option<&Path>,
    lookup: impl Fn(&str) -> Option<String>,
) -> ProducerFacts {
    let namespace = nonempty(&lookup, "KACHE_NAMESPACE");
    let compiler = crate::compiler::resolve_program_on_path(&args.rustc.to_string_lossy());
    ProducerFacts {
        namespace: namespace.clone().unwrap_or_else(|| "unscoped".into()),
        repository: nonempty(&lookup, "KACHE_REPOSITORY").or(namespace),
        target: nonempty(&lookup, "KACHE_BUILD_TARGET")
            .or_else(|| nonempty(&lookup, "CARGO_BUILD_TARGET")),
        profile: Some(crate::identity::profile_from_rustc_args(args))
            .filter(|profile| profile != "unknown"),
        build_shape: nonempty(&lookup, "KACHE_BUILD_SHAPE"),
        commit: nonempty(&lookup, "GITHUB_SHA").or_else(|| nonempty(&lookup, "CI_COMMIT_SHA")),
        git_ref: nonempty(&lookup, "GITHUB_REF")
            .or_else(|| nonempty(&lookup, "CI_COMMIT_REF_NAME")),
        parent_commits: nonempty(&lookup, "KACHE_PARENT_COMMITS")
            .map(|value| value.split_whitespace().map(str::to_owned).collect())
            .unwrap_or_default(),
        ci_started_at_ms: nonempty(&lookup, "KACHE_CI_STARTED_AT_MS")
            .and_then(|value| value.parse().ok()),
        compiler_stamp: compiler
            .as_deref()
            .and_then(|path| crate::cache_key::FileFingerprint::from_path(path).ok()),
        compiler_selector: crate::cache_key::rustc_version_fingerprint(&args.rustc),
        lock_stamp: lock_path
            .and_then(|path| crate::cache_key::FileFingerprint::from_path(path).ok()),
    }
}

/// Capture at the producer's first real invocation. Never called in daemon
/// publication. Session target is declared because a host build-script unit
/// cannot identify the target of the surrounding cross build.
pub fn producer_context(
    session_id: &str,
    root: &str,
    args: &crate::args::RustcArgs,
    lock_path: Option<&Path>,
    captured_at_ms: u64,
    lookup: impl Fn(&str) -> Option<String>,
) -> ProducerContext {
    let facts = producer_facts(args, lock_path, lookup);
    ProducerContext {
        session_id: session_id.to_owned(),
        root: root.to_owned(),
        namespace: facts.namespace.clone(),
        identity: BuildIdentity {
            repository: facts.repository.clone(),
            target: facts.target.clone(),
            toolchain_hash: stable_toolchain_hash(&args.rustc, &facts),
            profile: facts.profile.clone(),
            build_shape: facts.build_shape.clone(),
            lock_digest: lock_path.and_then(lock_digest),
        },
        commit: facts.commit.clone(),
        git_ref: facts.git_ref.clone(),
        parent_commits: facts.parent_commits.clone(),
        captured_at_ms,
        ci_started_at_ms: facts.ci_started_at_ms,
        facts: Some(facts),
    }
}

fn context_path(runtime_dir: &Path, session_id: &str, root: &str) -> std::path::PathBuf {
    // Hash local path components too; old clients may use non-hex session ids.
    let session = blake3::hash(session_id.as_bytes()).to_hex();
    runtime_dir
        .join("build-report-contexts")
        .join(format!("{}-{}.json", session, root_hash(root)))
}

pub fn context_exists(runtime_dir: &Path, session_id: &str, root: &str) -> bool {
    context_path(runtime_dir, session_id, root).exists()
}

/// Avoid probing the compiler or hashing the lockfile for every warm invocation.
pub fn capture_context_once(
    runtime_dir: &Path,
    session_id: &str,
    root: &str,
    facts: &ProducerFacts,
    capture: impl FnOnce() -> ProducerContext,
) -> Result<()> {
    let invalid = context_path(runtime_dir, session_id, root).with_extension("invalid");
    if invalid.exists() {
        return Ok(());
    }
    let context = match read_context(runtime_dir, session_id, root)? {
        Some(context) => context,
        None => {
            let context = capture();
            ensure!(
                context.session_id == session_id && context.root == root,
                "producer capture scope mismatch"
            );
            persist_context(runtime_dir, &context)?;
            // Compare the winner when concurrent first invocations raced.
            read_context(runtime_dir, session_id, root)?.context("producer context disappeared")?
        }
    };
    if context.facts.as_ref() != Some(facts) {
        mark_context_invalid(&invalid)?;
    }
    Ok(())
}

fn mark_context_invalid(path: &Path) -> Result<()> {
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Atomic, create-only local snapshot. Later invocations cannot relabel a session.
pub fn persist_context(runtime_dir: &Path, context: &ProducerContext) -> Result<()> {
    let path = context_path(runtime_dir, &context.session_id, &context.root);
    let parent = path.parent().context("report context directory")?;
    std::fs::create_dir_all(parent)?;
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer(&mut file, context)?;
    file.flush()?;
    persist_context_snapshot(file, &path)
}

fn persist_context_snapshot(file: tempfile::NamedTempFile, path: &Path) -> Result<()> {
    match file.persist_noclobber(path) {
        Ok(_) => Ok(()),
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error.error.into()),
    }
}

pub fn read_context(
    runtime_dir: &Path,
    session_id: &str,
    root: &str,
) -> Result<Option<ProducerContext>> {
    let path = context_path(runtime_dir, session_id, root);
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut bytes = Vec::new();
    file.take(MAX_REPORT_BYTES + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= MAX_REPORT_BYTES,
        "report context exceeds size limit"
    );
    let mut context: ProducerContext = serde_json::from_slice(&bytes)?;
    ensure!(
        context.session_id == session_id && context.root == root,
        "report context scope mismatch"
    );
    if context_path(runtime_dir, session_id, root)
        .with_extension("invalid")
        .exists()
    {
        context.identity = BuildIdentity::default();
        context.commit = None;
        context.git_ref = None;
        context.parent_commits.clear();
        context.ci_started_at_ms = None;
    }
    Ok(Some(context))
}

/// Split by session AND root before projecting fields or deduplicating anything.
pub fn collect_reports(
    events: &[BuildEvent],
    session: Option<&str>,
    runtime_dir: &Path,
    namespace: Option<&str>,
) -> Result<Vec<BuildReport>> {
    let mut groups: BTreeMap<(&str, &str), Vec<&BuildEvent>> = BTreeMap::new();
    for event in events {
        if event.session_id.is_empty()
            || event.root.is_empty()
            || session.is_some_and(|session| event.session_id != session)
            || event.crate_name == crate::build_script::CRATE_NAME
            || event.cache_key.is_empty()
            || !matches!(
                event.result,
                EventResult::LocalHit
                    | EventResult::PrefetchHit
                    | EventResult::RemoteHit
                    | EventResult::Dup
                    | EventResult::Miss
            )
        {
            continue;
        }
        groups
            .entry((&event.session_id, &event.root))
            .or_default()
            .push(event);
    }
    groups
        .into_iter()
        .map(|((session_id, root), events)| {
            let context = read_context(runtime_dir, session_id, root)?;
            let started_at_ms = events
                .iter()
                .map(|event| event_start(event))
                .min()
                .context("empty report")?;
            let finished_at_ms = events
                .iter()
                .map(|event| event_finish(event))
                .max()
                .context("empty report")?;
            let ci_start = context
                .as_ref()
                .and_then(|context| context.ci_started_at_ms);
            let report = BuildReport {
                schema: REPORT_SCHEMA,
                session_id: session_id.to_owned(),
                root_hash: root_hash(root),
                namespace: namespace
                    .map(str::to_owned)
                    .or_else(|| context.as_ref().map(|context| context.namespace.clone()))
                    .unwrap_or_else(|| "unscoped".into()),
                identity: context
                    .as_ref()
                    .map(|context| context.identity.clone())
                    .unwrap_or_default(),
                commit: context.as_ref().and_then(|context| context.commit.clone()),
                git_ref: context.as_ref().and_then(|context| context.git_ref.clone()),
                parent_commits: context
                    .as_ref()
                    .map(|context| context.parent_commits.clone())
                    .unwrap_or_default(),
                started_at_ms,
                finished_at_ms,
                producer_captured_at_ms: context.as_ref().map(|context| context.captured_at_ms),
                entries: events
                    .into_iter()
                    .map(|event| {
                        let started = event_start(event);
                        ReportEntry {
                            cache_key: event.cache_key.clone(),
                            crate_name: event.crate_name.clone(),
                            result: event.result,
                            started_at_ms: started,
                            finished_at_ms: event_finish(event),
                            start_offset_ms: started.saturating_sub(started_at_ms),
                            ci_start_offset_ms: ci_start.and_then(|base| started.checked_sub(base)),
                            elapsed_ms: event.elapsed_ms,
                            compile_time_ms: event.compile_time_ms,
                            artifact_size: event.size,
                            event_schema: event.schema,
                            demands: event.demands.clone(),
                            artifact_status: ArtifactStatus::Observed,
                        }
                    })
                    .collect(),
            };
            validate_report(&report)?;
            Ok(report)
        })
        .collect()
}

fn event_finish(event: &BuildEvent) -> u64 {
    u64::try_from(event.ts.timestamp_millis()).unwrap_or_default()
}

fn event_start(event: &BuildEvent) -> u64 {
    event_finish(event).saturating_sub(event.elapsed_ms)
}

fn safe_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 200
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
}

fn validate_namespace(namespace: &str) -> Result<()> {
    ensure!(
        namespace.len() <= 512 && namespace.split('/').all(safe_component),
        "unsafe report namespace"
    );
    Ok(())
}

fn hex64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

pub fn validate_report(report: &BuildReport) -> Result<()> {
    ensure!(
        report.schema == REPORT_SCHEMA,
        "unsupported build report schema"
    );
    validate_namespace(&report.namespace)?;
    ensure!(
        safe_component(&report.session_id),
        "unsafe report session id"
    );
    ensure!(hex64(&report.root_hash), "invalid report root hash");
    ensure!(
        !report.entries.is_empty() && report.entries.len() <= MAX_REPORT_ENTRIES,
        "invalid report entry count"
    );
    ensure!(
        report.started_at_ms <= report.finished_at_ms,
        "invalid report time range"
    );
    for digest in [
        &report.identity.toolchain_hash,
        &report.identity.lock_digest,
    ]
    .into_iter()
    .flatten()
    {
        ensure!(hex64(digest), "invalid report identity digest");
    }
    for entry in &report.entries {
        ensure!(hex64(&entry.cache_key), "invalid report cache key");
        ensure!(
            safe_component(&entry.crate_name)
                && crate::cache_key::is_valid_crate_name(&entry.crate_name),
            "unsafe report crate name"
        );
        ensure!(
            entry.started_at_ms >= report.started_at_ms
                && entry.finished_at_ms <= report.finished_at_ms
                && entry.started_at_ms <= entry.finished_at_ms,
            "entry outside report time range"
        );
        ensure!(
            entry.start_offset_ms == entry.started_at_ms - report.started_at_ms,
            "invalid report entry offset"
        );
        ensure!(
            entry.elapsed_ms == entry.finished_at_ms - entry.started_at_ms,
            "invalid report elapsed time"
        );
        ensure!(
            matches!(
                entry.result,
                EventResult::LocalHit
                    | EventResult::PrefetchHit
                    | EventResult::RemoteHit
                    | EventResult::Dup
                    | EventResult::Miss
            ),
            "uncacheable report entry"
        );
    }
    Ok(())
}

pub fn discovery_prefix(prefix: &str, namespace: &str) -> Result<String> {
    validate_namespace(namespace)?;
    Ok(crate::config::join_remote_key(
        prefix,
        &format!("{REPORT_PREFIX}/{namespace}/"),
    ))
}

pub fn object_key(prefix: &str, report: &BuildReport) -> Result<String> {
    validate_report(report)?;
    Ok(format!(
        "{}{}-{}.json",
        discovery_prefix(prefix, &report.namespace)?,
        report.session_id,
        report.root_hash
    ))
}

pub async fn upload_report(
    backend: &dyn RemoteBackend,
    prefix: &str,
    report: &BuildReport,
) -> Result<()> {
    let key = object_key(prefix, report)?;
    let bytes = serde_json::to_vec(report)?;
    ensure!(
        bytes.len() as u64 <= MAX_REPORT_BYTES,
        "build report exceeds size limit"
    );
    match backend
        .put_if_absent(&key, bytes.clone(), Some("application/json"))
        .await?
    {
        PutIfAbsentResult::Created => Ok(()),
        PutIfAbsentResult::AlreadyExists => {
            let stored = backend
                .get(&key, Some(MAX_REPORT_BYTES))
                .await?
                .context("existing build report disappeared")?;
            ensure!(
                stored.body.as_ref() == bytes,
                "immutable build report conflict: {key}"
            );
            Ok(())
        }
        PutIfAbsentResult::Unsupported => bail!("remote cannot create immutable build reports"),
    }
}

/// Discovery never returns nested namespaces or arbitrary object keys. A large
/// namespace is refused instead of silently selecting an arbitrary partial set.
pub async fn list_reports(
    backend: &dyn RemoteBackend,
    prefix: &str,
    namespace: &str,
) -> Result<Vec<String>> {
    let scope = discovery_prefix(prefix, namespace)?;
    let keys = backend.list(&scope).await?;
    ensure!(
        keys.len() <= MAX_REPORT_KEYS,
        "build report discovery exceeds key limit"
    );
    let mut scoped = Vec::new();
    for key in keys {
        let Some(name) = key.strip_prefix(&scope) else {
            bail!("report listing escaped namespace")
        };
        if valid_report_filename(name) {
            scoped.push(key);
        }
    }
    scoped.sort();
    Ok(scoped)
}

fn valid_report_filename(name: &str) -> bool {
    let Some(stem) = name.strip_suffix(".json") else {
        return false;
    };
    let Some((session, root)) = stem.rsplit_once('-') else {
        return false;
    };
    safe_component(session) && hex64(root)
}

pub async fn download_report(
    backend: &dyn RemoteBackend,
    prefix: &str,
    namespace: &str,
    key: &str,
) -> Result<Option<BuildReport>> {
    let scope = discovery_prefix(prefix, namespace)?;
    ensure!(
        key.strip_prefix(&scope).is_some_and(valid_report_filename),
        "report key outside namespace"
    );
    let Some(object) = backend.get(key, Some(MAX_REPORT_BYTES)).await? else {
        return Ok(None);
    };
    ensure!(
        object.body.len() as u64 <= MAX_REPORT_BYTES,
        "build report exceeds size limit"
    );
    let report: BuildReport = serde_json::from_slice(&object.body)?;
    ensure!(
        object_key(prefix, &report)? == key && report.namespace == namespace,
        "report object scope mismatch"
    );
    Ok(Some(report))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn event(session: &str, root: &str, key: char, finish: i64, elapsed: u64) -> BuildEvent {
        let mut event = BuildEvent::new_for_test("serde", EventResult::Miss);
        event.session_id = session.into();
        event.root = root.into();
        event.cache_key = key.to_string().repeat(64);
        event.ts = Utc.timestamp_millis_opt(finish).unwrap();
        event.elapsed_ms = elapsed;
        event.compile_time_ms = elapsed / 2;
        event.size = 4321;
        event.schema = 25;
        event
    }

    fn report() -> BuildReport {
        let dir = tempfile::tempdir().unwrap();
        collect_reports(
            &[event("one", "/work", 'a', 1200, 200)],
            None,
            dir.path(),
            Some("org/repo"),
        )
        .unwrap()
        .remove(0)
    }

    #[cfg(unix)]
    #[test]
    fn toolchain_identity_requires_full_readable_host_version() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let version = "rustc 1.99.0\nhost: x86_64-unknown-linux-gnu\nrelease: 1.99.0";
        let expected = blake3::hash(version.as_bytes()).to_hex().to_string();
        for (index, (stdout, identity)) in [
            (format!("  {version}\n").into_bytes(), Some(expected)),
            (b"rustc 1.99.0\nrelease: 1.99.0\n".to_vec(), None),
            (
                format!("{version}\ncommit-hash: \u{fffd}\n").into_bytes(),
                None,
            ),
            (
                [version.as_bytes(), b"\ncommit-hash: \xff\n"].concat(),
                None,
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let compiler = dir.path().join(format!("rustc-{index}"));
            std::fs::write(&compiler, "#!/bin/sh\nexec /bin/cat \"${0}.version\"\n").unwrap();
            std::fs::set_permissions(&compiler, std::fs::Permissions::from_mode(0o755)).unwrap();
            std::fs::write(compiler.with_extension("version"), stdout).unwrap();
            assert_eq!(toolchain_hash(&compiler), identity, "version case {index}");
            let facts = ProducerFacts {
                compiler_stamp: Some(
                    crate::cache_key::FileFingerprint::from_path(&compiler).unwrap(),
                ),
                compiler_selector: Some("known-selector".into()),
                ..ProducerFacts::default()
            };
            assert_eq!(stable_toolchain_hash(&compiler, &facts), identity);
            let mut missing_stamp = facts.clone();
            missing_stamp.compiler_stamp = None;
            assert_eq!(stable_toolchain_hash(&compiler, &missing_stamp), None);
            let mut missing_selector = facts;
            missing_selector.compiler_selector = None;
            assert_eq!(stable_toolchain_hash(&compiler, &missing_selector), None);
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_bare_compiler_keeps_replay_identity_unknown() {
        let args = crate::args::RustcArgs::parse(&[
            "rustc".into(),
            "--out-dir".into(),
            "target/debug/deps".into(),
        ])
        .unwrap();
        let context = producer_context("one", "/work", &args, None, 0, |name| {
            (name == "KACHE_NAMESPACE").then(|| "org/repo".into())
        });
        assert_eq!(context.identity.toolchain_hash, None);
        assert_eq!(context.facts.unwrap().compiler_selector, None);
    }

    #[test]
    fn context_size_limit_accepts_exact_cap_and_rejects_trailing_data() {
        let dir = tempfile::tempdir().unwrap();
        let context = ProducerContext {
            session_id: "one".into(),
            root: "/work".into(),
            namespace: "org/repo".into(),
            identity: BuildIdentity::default(),
            commit: None,
            git_ref: None,
            parent_commits: vec![],
            captured_at_ms: 0,
            ci_started_at_ms: None,
            facts: None,
        };
        persist_context(dir.path(), &context).unwrap();
        let path = context_path(dir.path(), "one", "/work");
        let mut bytes = serde_json::to_vec(&context).unwrap();
        bytes.resize(8_388_608, b' ');
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(
            read_context(dir.path(), "one", "/work").unwrap(),
            Some(context)
        );
        bytes.push(b' ');
        std::fs::write(&path, bytes).unwrap();
        assert!(
            read_context(dir.path(), "one", "/work")
                .unwrap_err()
                .to_string()
                .contains("size limit")
        );
    }

    #[test]
    fn racing_context_invalidations_preserve_the_first_marker() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("context.invalid");
        mark_context_invalid(&marker).unwrap();
        assert_eq!(std::fs::read(&marker).unwrap(), b"");
        std::fs::write(&marker, b"already invalid").unwrap();
        // A producer that checked before another invalidated the same session
        // must accept the existing marker without replacing it.
        mark_context_invalid(&marker).unwrap();
        assert_eq!(std::fs::read(&marker).unwrap(), b"already invalid");
        assert!(mark_context_invalid(&dir.path().join("missing/parent/marker")).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn disappearing_snapshot_directory_is_not_an_existing_context() {
        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("contexts");
        std::fs::create_dir(&parent).unwrap();
        let file = tempfile::NamedTempFile::new_in(&parent).unwrap();
        let snapshot = parent.join("session.json");
        // Cache cleanup can remove the runtime directory while a first
        // producer prepares its snapshot. Only a competing snapshot is benign.
        std::fs::rename(&parent, dir.path().join("removed-contexts")).unwrap();
        let error = persist_context_snapshot(file, &snapshot).unwrap_err();
        assert_eq!(
            error.downcast_ref::<std::io::Error>().unwrap().kind(),
            std::io::ErrorKind::NotFound
        );
        assert!(!snapshot.exists());
    }

    #[test]
    fn zero_duration_and_epoch_clamping_preserve_valid_observations() {
        let dir = tempfile::tempdir().unwrap();
        for (finish, elapsed, expected_start, expected_finish) in
            [(0, 0, 0, 0), (-1, 0, 0, 0), (50, 50, 0, 50)]
        {
            let reports = collect_reports(
                &[event("one", "/work", 'a', finish, elapsed)],
                None,
                dir.path(),
                None,
            )
            .unwrap();
            assert_eq!(reports[0].started_at_ms, expected_start);
            assert_eq!(reports[0].finished_at_ms, expected_finish);
            assert_eq!(reports[0].entries[0].start_offset_ms, 0);
            assert_eq!(reports[0].entries[0].ci_start_offset_ms, None);
        }
        let oversized_elapsed = event("one", "/work", 'a', 50, 51);
        assert_eq!(event_start(&oversized_elapsed), 0);
        assert!(collect_reports(&[oversized_elapsed], None, dir.path(), None).is_err());
    }

    #[test]
    fn report_filenames_require_both_safe_session_and_full_root_hash() {
        let hash = "a".repeat(64);
        assert!(valid_report_filename(&format!("session-one-{hash}.json")));
        for session in [
            "",
            ".",
            "..",
            "nested/session",
            "path\\session",
            "space session",
        ] {
            assert!(
                !valid_report_filename(&format!("{session}-{hash}.json")),
                "{session:?}"
            );
        }
        assert!(!valid_report_filename("session.json"));
        assert!(!valid_report_filename(&format!("session-{hash}.json.bak")));
    }

    #[test]
    fn ordered_report_retains_repeats_timings_and_exact_scope() {
        let dir = tempfile::tempdir().unwrap();
        let context = ProducerContext {
            session_id: "one".into(),
            root: "/work".into(),
            namespace: "org/repo".into(),
            identity: BuildIdentity {
                repository: Some("org/repo".into()),
                target: Some("wasm32-unknown-unknown".into()),
                toolchain_hash: Some("c".repeat(64)),
                profile: Some("release".into()),
                build_shape: Some("test-features-a".into()),
                lock_digest: Some("d".repeat(64)),
            },
            commit: Some("producer-commit".into()),
            git_ref: Some("refs/heads/producer".into()),
            parent_commits: vec!["parent-one".into(), "parent-two".into()],
            captured_at_ms: 900,
            ci_started_at_ms: Some(800),
            facts: Some(ProducerFacts::default()),
        };
        persist_context(dir.path(), &context).unwrap();
        let mut foreign = event("one", "/other", 'b', 1300, 10);
        foreign.crate_name = "foreign".into();
        let mut hit = event("one", "/work", 'a', 1600, 10);
        hit.result = EventResult::LocalHit;
        hit.compile_time_ms = 700;
        let mut ignored = event("one", "/work", 'f', 1700, 10);
        ignored.result = EventResult::Error;
        let events = vec![
            event("one", "/work", 'a', 1200, 200),
            foreign,
            hit,
            event("two", "/work", 'e', 1800, 10),
            ignored,
        ];
        let reports = collect_reports(&events, Some("one"), dir.path(), None).unwrap();
        assert_eq!(reports.len(), 2);
        let observed = reports
            .iter()
            .find(|report| report.root_hash == root_hash("/work"))
            .unwrap();
        assert_eq!(observed.identity, context.identity);
        assert_eq!(observed.commit, context.commit);
        assert_eq!(observed.git_ref, context.git_ref);
        assert_eq!(observed.parent_commits, context.parent_commits);
        assert_eq!(observed.namespace, "org/repo");
        assert_eq!(observed.producer_captured_at_ms, Some(900));
        assert_eq!(
            (observed.started_at_ms, observed.finished_at_ms),
            (1000, 1600)
        );
        assert_eq!(observed.entries.len(), 2);
        let first = &observed.entries[0];
        let second = &observed.entries[1];
        assert_eq!(first.cache_key, second.cache_key);
        assert_eq!(
            (
                first.started_at_ms,
                first.finished_at_ms,
                first.start_offset_ms,
                first.ci_start_offset_ms
            ),
            (1000, 1200, 0, Some(200))
        );
        assert_eq!(
            (
                second.started_at_ms,
                second.finished_at_ms,
                second.start_offset_ms,
                second.ci_start_offset_ms
            ),
            (1590, 1600, 590, Some(790))
        );
        assert_eq!(
            (
                first.elapsed_ms,
                first.compile_time_ms,
                first.artifact_size,
                first.event_schema
            ),
            (200, 100, 4321, 25)
        );
        assert_eq!(
            (second.result, second.compile_time_ms),
            (EventResult::LocalHit, 700)
        );
        assert_eq!(first.artifact_status, ArtifactStatus::Observed);
        let unknown = reports
            .iter()
            .find(|report| report.root_hash != root_hash("/work"))
            .unwrap();
        assert_eq!(unknown.identity, BuildIdentity::default());
        assert_eq!(unknown.commit, None);
        assert_eq!(unknown.entries.len(), 1);
        assert!(!serde_json::to_string(observed).unwrap().contains("/work"));
    }

    #[test]
    fn collection_excludes_legacy_uncacheable_and_local_build_script_events() {
        let dir = tempfile::tempdir().unwrap();
        let mut events = vec![
            event("", "/work", 'a', 100, 1),
            event("one", "", 'a', 100, 1),
        ];
        let mut empty_key = event("one", "/work", 'a', 100, 1);
        empty_key.cache_key.clear();
        events.push(empty_key);
        let mut script = event("one", "/work", 'a', 100, 1);
        script.crate_name = crate::build_script::CRATE_NAME.into();
        events.push(script);
        for result in [
            EventResult::Error,
            EventResult::Passthrough,
            EventResult::Skipped,
        ] {
            let mut item = event("one", "/work", 'a', 100, 1);
            item.result = result;
            events.push(item);
        }
        assert!(
            collect_reports(&events, None, dir.path(), None)
                .unwrap()
                .is_empty()
        );
        for result in [
            EventResult::LocalHit,
            EventResult::PrefetchHit,
            EventResult::RemoteHit,
            EventResult::Dup,
            EventResult::Miss,
        ] {
            let mut item = event("one", "/work", 'a', 100, 1);
            item.result = result;
            events.push(item);
        }
        assert_eq!(
            collect_reports(&events, None, dir.path(), None).unwrap()[0]
                .entries
                .len(),
            5
        );
    }

    #[test]
    fn local_context_is_atomic_first_writer_and_checks_scope() {
        let dir = tempfile::tempdir().unwrap();
        let mut context = ProducerContext {
            session_id: "one".into(),
            root: "/work".into(),
            namespace: "producer".into(),
            identity: BuildIdentity::default(),
            commit: Some("first".into()),
            git_ref: None,
            parent_commits: vec![],
            captured_at_ms: 1,
            ci_started_at_ms: None,
            facts: Some(ProducerFacts::default()),
        };
        assert!(read_context(dir.path(), "one", "/work").unwrap().is_none());
        assert!(!context_exists(dir.path(), "one", "/work"));
        persist_context(dir.path(), &context).unwrap();
        assert!(context_exists(dir.path(), "one", "/work"));
        context.commit = Some("later-daemon".into());
        let facts = ProducerFacts::default();
        capture_context_once(dir.path(), "one", "/work", &facts, || {
            panic!("warm invocation probed producer metadata again")
        })
        .unwrap();
        persist_context(dir.path(), &context).unwrap();
        let restored = read_context(dir.path(), "one", "/work").unwrap().unwrap();
        assert_eq!(restored.commit.as_deref(), Some("first"));
        assert!(read_context(dir.path(), "one", "/other").unwrap().is_none());
        assert!(
            capture_context_once(dir.path(), "one", "/other", &facts, || context.clone())
                .unwrap_err()
                .to_string()
                .contains("capture scope mismatch")
        );
        capture_context_once(dir.path(), "two", "/work", &facts, || {
            let mut next = context.clone();
            next.session_id = "two".into();
            next
        })
        .unwrap();
        assert_eq!(
            read_context(dir.path(), "two", "/work")
                .unwrap()
                .unwrap()
                .commit
                .as_deref(),
            Some("later-daemon")
        );
        std::fs::write(
            context_path(dir.path(), "one", "/work"),
            serde_json::to_vec(&context).unwrap(),
        )
        .unwrap();
        context.root = "/foreign".into();
        std::fs::write(
            context_path(dir.path(), "one", "/work"),
            serde_json::to_vec(&context).unwrap(),
        )
        .unwrap();
        assert!(
            read_context(dir.path(), "one", "/work")
                .unwrap_err()
                .to_string()
                .contains("scope mismatch")
        );
    }

    #[test]
    fn producer_context_captures_declared_cross_target_and_full_lock_identity() {
        assert!(!producer_is_declared(|_| None));
        assert!(!producer_is_declared(|_| Some("  ".into())));
        assert!(producer_is_declared(
            |name| (name == "KACHE_NAMESPACE").then(|| "org/repo".into())
        ));
        assert!(producer_is_declared(
            |name| (name == "KACHE_REPOSITORY").then(|| "repo".into())
        ));
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join("Cargo.lock");
        assert_eq!(lock_digest(&lock), None);
        std::fs::write(&lock, []).unwrap();
        assert_eq!(lock_digest(&lock), None);
        std::fs::write(&lock, "producer lock contents").unwrap();
        let args = crate::args::RustcArgs::parse(&[
            dir.path()
                .join("missing-rustc")
                .to_string_lossy()
                .into_owned(),
            "--out-dir".into(),
            dir.path()
                .join("target/debug/deps")
                .to_string_lossy()
                .into_owned(),
        ])
        .unwrap();
        let values = BTreeMap::from([
            ("KACHE_NAMESPACE", "org/repo"),
            ("KACHE_BUILD_TARGET", "wasm32-unknown-unknown"),
            ("CARGO_BUILD_TARGET", "other-target"),
            ("KACHE_BUILD_SHAPE", "declared-shape"),
            ("GITHUB_SHA", "producer-commit"),
            ("GITHUB_REF", "refs/heads/producer"),
            ("KACHE_PARENT_COMMITS", "parent-a parent-b"),
            ("KACHE_CI_STARTED_AT_MS", "1000"),
        ]);
        let captured = producer_context("one", "/work", &args, Some(&lock), 2000, |name| {
            values.get(name).map(|value| (*value).to_owned())
        });
        assert_eq!(captured.identity.repository.as_deref(), Some("org/repo"));
        assert_eq!(
            captured.identity.target.as_deref(),
            Some("wasm32-unknown-unknown")
        );
        assert_eq!(captured.identity.profile.as_deref(), Some("debug"));
        assert_eq!(
            captured.identity.build_shape.as_deref(),
            Some("declared-shape")
        );
        assert_eq!(captured.identity.toolchain_hash, None);
        assert_eq!(
            captured.identity.lock_digest,
            Some(blake3::hash(b"producer lock contents").to_hex().to_string())
        );
        assert_eq!(captured.commit.as_deref(), Some("producer-commit"));
        assert_eq!(captured.git_ref.as_deref(), Some("refs/heads/producer"));
        assert_eq!(captured.parent_commits, ["parent-a", "parent-b"]);
        assert_eq!(captured.ci_started_at_ms, Some(1000));
        assert_eq!(captured.captured_at_ms, 2000);
        let unknown = producer_context("one", "/work", &args, None, 0, |_| None);
        assert_eq!(unknown.namespace, "unscoped");
        assert_eq!(unknown.identity.target, None);
        assert_eq!(unknown.identity.repository, None);
        assert_eq!(unknown.identity.build_shape, None);
        assert_eq!(unknown.identity.lock_digest, None);
        assert_eq!(unknown.commit, None);
        let fallback = producer_context("one", "/work", &args, None, 0, |name| match name {
            "CARGO_BUILD_TARGET" => Some("fallback-target".into()),
            "KACHE_REPOSITORY" => Some("explicit-repo".into()),
            "CI_COMMIT_SHA" => Some("gitlab-commit".into()),
            "CI_COMMIT_REF_NAME" => Some("gitlab-ref".into()),
            "KACHE_CI_STARTED_AT_MS" => Some("invalid".into()),
            _ => None,
        });
        assert_eq!(fallback.identity.target.as_deref(), Some("fallback-target"));
        assert_eq!(
            fallback.identity.repository.as_deref(),
            Some("explicit-repo")
        );
        assert_eq!(fallback.commit.as_deref(), Some("gitlab-commit"));
        assert_eq!(fallback.git_ref.as_deref(), Some("gitlab-ref"));
        assert_eq!(fallback.ci_started_at_ms, None);
    }

    #[test]
    fn identity_drift_in_one_session_becomes_unknown_without_relabeling_events() {
        let dir = tempfile::tempdir().unwrap();
        let original_facts = ProducerFacts {
            namespace: "org/repo".into(),
            repository: Some("repo".into()),
            target: Some("target".into()),
            profile: Some("debug".into()),
            build_shape: Some("shape-a".into()),
            commit: Some("first-commit".into()),
            git_ref: Some("first-ref".into()),
            parent_commits: vec!["first-parent".into()],
            ci_started_at_ms: Some(800),
            ..ProducerFacts::default()
        };
        let original = ProducerContext {
            session_id: "one".into(),
            root: "/work".into(),
            namespace: "org/repo".into(),
            identity: BuildIdentity {
                repository: Some("repo".into()),
                target: Some("target".into()),
                profile: Some("debug".into()),
                build_shape: Some("shape-a".into()),
                toolchain_hash: Some("a".repeat(64)),
                lock_digest: Some("b".repeat(64)),
            },
            commit: Some("first-commit".into()),
            git_ref: Some("first-ref".into()),
            parent_commits: vec!["first-parent".into()],
            captured_at_ms: 900,
            ci_started_at_ms: Some(800),
            facts: Some(original_facts.clone()),
        };
        let stamp = crate::cache_key::FileFingerprint {
            path: "changed".into(),
            size: 1,
            mtime_ns: 1,
            ctime_ns: 2,
            inode: 3,
        };
        let mut variants = Vec::new();
        let mut changed = original_facts.clone();
        changed.profile = Some("release".into());
        variants.push(changed);
        let mut changed = original_facts.clone();
        changed.build_shape = Some("shape-b".into());
        variants.push(changed);
        let mut changed = original_facts.clone();
        changed.target = Some("other-target".into());
        variants.push(changed);
        let mut changed = original_facts.clone();
        changed.repository = Some("other-repo".into());
        variants.push(changed);
        let mut changed = original_facts.clone();
        changed.namespace = "other-namespace".into();
        variants.push(changed);
        let mut changed = original_facts.clone();
        changed.compiler_stamp = Some(stamp.clone());
        variants.push(changed);
        let mut changed = original_facts.clone();
        changed.compiler_selector = Some("other-toolchain".into());
        variants.push(changed);
        let mut changed = original_facts.clone();
        changed.lock_stamp = Some(stamp);
        variants.push(changed);
        let mut changed = original_facts.clone();
        changed.commit = Some("second-commit".into());
        variants.push(changed);
        for changed in variants {
            let runtime = tempfile::tempdir_in(dir.path()).unwrap();
            capture_context_once(runtime.path(), "one", "/work", &original_facts, || {
                original.clone()
            })
            .unwrap();
            let path = context_path(runtime.path(), "one", "/work");
            let immutable_bytes = std::fs::read(&path).unwrap();
            capture_context_once(runtime.path(), "one", "/work", &changed, || {
                panic!("must not probe again")
            })
            .unwrap();
            let mixed = collect_reports(
                &[
                    event("one", "/work", 'a', 1200, 200),
                    event("one", "/work", 'b', 1500, 100),
                ],
                None,
                runtime.path(),
                None,
            )
            .unwrap()
            .remove(0);
            assert_eq!(mixed.identity, BuildIdentity::default());
            assert_eq!(mixed.commit, None);
            assert_eq!(mixed.git_ref, None);
            assert!(mixed.parent_commits.is_empty());
            assert_eq!(mixed.entries.len(), 2);
            assert!(
                mixed
                    .entries
                    .iter()
                    .all(|entry| entry.ci_start_offset_ms.is_none())
            );
            assert_eq!(std::fs::read(&path).unwrap(), immutable_bytes);
            // Invalid remains invalid even if a later command returns to shape A.
            capture_context_once(runtime.path(), "one", "/work", &original_facts, || {
                panic!("already invalid")
            })
            .unwrap();
            assert_eq!(
                read_context(runtime.path(), "one", "/work")
                    .unwrap()
                    .unwrap()
                    .identity,
                BuildIdentity::default()
            );
        }
    }

    #[test]
    fn untrusted_report_validation_rejects_bad_paths_keys_schema_and_timing() {
        let valid = report();
        let at_namespace_cap = format!(
            "{}/{}/{}",
            "a".repeat(200),
            "b".repeat(200),
            "c".repeat(110)
        );
        let mut boundary = valid.clone();
        boundary.namespace = at_namespace_cap.clone();
        validate_report(&boundary).unwrap();
        boundary.namespace.push('c');
        assert!(validate_report(&boundary).is_err());
        boundary.namespace = "a".repeat(201);
        assert!(validate_report(&boundary).is_err());
        boundary.namespace = "é".into();
        assert!(validate_report(&boundary).is_err());
        for namespace in [
            "",
            "../repo",
            "org//repo",
            "/org",
            "org/",
            "org\\repo",
            ".",
            "..",
        ] {
            let mut invalid = valid.clone();
            invalid.namespace = namespace.into();
            assert!(validate_report(&invalid).is_err(), "{namespace:?}");
        }
        for name in [
            "",
            ".",
            "..",
            "../crate",
            "dir/crate",
            "dir\\crate",
            "crate:stream",
            "foo..cpp",
        ] {
            let mut invalid = valid.clone();
            invalid.entries[0].crate_name = name.into();
            assert!(validate_report(&invalid).is_err(), "{name:?}");
        }
        let mut too_long = valid.clone();
        too_long.entries[0].crate_name = "a".repeat(129);
        assert!(validate_report(&too_long).is_err());
        let mut source = valid.clone();
        source.entries[0].crate_name = "foo.cpp".into();
        validate_report(&source).unwrap();
        for key in [
            "a".repeat(63),
            "a".repeat(65),
            "z".repeat(64),
            "A".repeat(64),
        ] {
            let mut invalid = valid.clone();
            invalid.entries[0].cache_key = key;
            assert!(validate_report(&invalid).is_err());
        }
        let mut invalid = valid.clone();
        invalid.schema += 1;
        assert!(validate_report(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.session_id = "../../bad".into();
        assert!(validate_report(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.root_hash.clear();
        assert!(validate_report(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.entries.clear();
        assert!(validate_report(&invalid).is_err());
        let mut at_cap = valid.clone();
        at_cap.entries.resize(50_000, valid.entries[0].clone());
        validate_report(&at_cap).unwrap();
        at_cap.entries.push(valid.entries[0].clone());
        assert!(validate_report(&at_cap).is_err());
        let mut invalid = valid.clone();
        invalid.started_at_ms = invalid.finished_at_ms + 1;
        assert!(validate_report(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.entries[0].start_offset_ms = 1;
        assert!(validate_report(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.entries[0].elapsed_ms += 1;
        assert!(validate_report(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.entries[0].started_at_ms -= 1;
        assert!(validate_report(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.entries[0].finished_at_ms += 1;
        assert!(validate_report(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.entries[0].started_at_ms = invalid.entries[0].finished_at_ms + 1;
        assert!(validate_report(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.entries[0].result = EventResult::Error;
        assert!(validate_report(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.identity.toolchain_hash = Some("bad".into());
        assert!(validate_report(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.identity.lock_digest = Some("bad".into());
        assert!(validate_report(&invalid).is_err());
        assert_eq!(MAX_REPORT_BYTES, 8_388_608);
        assert_eq!(MAX_REPORT_ENTRIES, 50_000);
        assert_eq!(MAX_REPORT_KEYS, 1024);
    }

    #[tokio::test]
    async fn immutable_reports_retry_identically_and_reject_changed_snapshots() {
        let backend = crate::remote_backend::memory_backend();
        let report = report();
        upload_report(&backend, "artifacts", &report).await.unwrap();
        upload_report(&backend, "artifacts", &report).await.unwrap();
        let key = object_key("artifacts", &report).unwrap();
        let before = backend
            .get(&key, None)
            .await
            .unwrap()
            .unwrap()
            .body
            .to_vec();
        let mut changed = report.clone();
        changed.commit = Some("later".into());
        assert!(
            upload_report(&backend, "artifacts", &changed)
                .await
                .unwrap_err()
                .to_string()
                .contains("conflict")
        );
        assert_eq!(
            backend
                .get(&key, None)
                .await
                .unwrap()
                .unwrap()
                .body
                .as_ref(),
            before
        );
        assert_eq!(
            download_report(&backend, "artifacts", "org/repo", &key)
                .await
                .unwrap(),
            Some(report)
        );
        assert!(
            download_report(&backend, "artifacts", "other", &key)
                .await
                .is_err()
        );
        assert!(
            download_report(&backend, "artifacts", "org/repo", &format!("{key}/nested"))
                .await
                .is_err()
        );
    }

    struct UnsupportedBackend {
        inner: crate::remote_backend::OpenDalBackend,
        listing: Vec<String>,
        untrusted_body: Option<Vec<u8>>,
    }

    #[async_trait::async_trait]
    impl RemoteBackend for UnsupportedBackend {
        async fn head(&self, key: &str) -> Result<bool> {
            self.inner.head(key).await
        }
        async fn get(
            &self,
            key: &str,
            max: Option<u64>,
        ) -> Result<Option<crate::remote_backend::GetObject>> {
            if let Some(body) = &self.untrusted_body {
                assert_eq!(max, Some(8_388_608));
                return Ok(Some(crate::remote_backend::GetObject {
                    body: bytes::Bytes::copy_from_slice(body),
                    request_ms: 0,
                    body_ms: 0,
                }));
            }
            self.inner.get(key, max).await
        }
        async fn put(&self, _: &str, _: Vec<u8>, _: Option<&str>) -> Result<()> {
            bail!("immutable reports must never fall back to plain PUT")
        }
        async fn list(&self, _: &str) -> Result<Vec<String>> {
            Ok(self.listing.clone())
        }
        fn describe(&self, key: &str) -> String {
            key.to_owned()
        }
    }

    #[tokio::test]
    async fn immutable_transport_and_discovery_fail_closed_at_boundaries() {
        let observed = report();
        let scope = discovery_prefix("artifacts", "org/repo").unwrap();
        let key = object_key("artifacts", &observed).unwrap();
        let mut backend = UnsupportedBackend {
            inner: crate::remote_backend::memory_backend(),
            listing: vec![key.clone()],
            untrusted_body: None,
        };
        assert!(
            upload_report(&backend, "artifacts", &observed)
                .await
                .unwrap_err()
                .to_string()
                .contains("cannot create immutable")
        );
        assert!(!backend.head(&key).await.unwrap());
        assert_eq!(
            list_reports(&backend, "artifacts", "org/repo")
                .await
                .unwrap()
                .as_slice(),
            std::slice::from_ref(&key)
        );
        backend.listing = vec![key.clone(); 1024];
        assert_eq!(
            list_reports(&backend, "artifacts", "org/repo")
                .await
                .unwrap()
                .len(),
            1024
        );
        backend.listing.push(key.clone());
        assert!(
            list_reports(&backend, "artifacts", "org/repo")
                .await
                .unwrap_err()
                .to_string()
                .contains("key limit")
        );
        backend.listing = vec!["artifacts/outside.json".into()];
        assert!(
            list_reports(&backend, "artifacts", "org/repo")
                .await
                .unwrap_err()
                .to_string()
                .contains("escaped namespace")
        );
        backend.listing = [
            "nested/report.json",
            "one-not-a-root.json",
            "bad.txt",
            "../report.json",
        ]
        .into_iter()
        .map(|name| format!("{scope}{name}"))
        .collect();
        assert!(
            list_reports(&backend, "artifacts", "org/repo")
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            download_report(&backend, "artifacts", "org/repo", &key)
                .await
                .unwrap()
                .is_none()
        );
        let mut wrong_namespace = observed.clone();
        wrong_namespace.namespace = "other".into();
        let mut wrong_session = observed.clone();
        wrong_session.session_id = "other-session".into();
        let mut wrong_root = observed.clone();
        wrong_root.root_hash = "f".repeat(64);
        for mislabelled in [wrong_namespace, wrong_session, wrong_root] {
            backend
                .inner
                .put(&key, serde_json::to_vec(&mislabelled).unwrap(), None)
                .await
                .unwrap();
            assert!(
                download_report(&backend, "artifacts", "org/repo", &key)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("scope mismatch")
            );
        }
        backend
            .inner
            .put(&key, vec![b' '; 8_388_609], None)
            .await
            .unwrap();
        assert!(
            download_report(&backend, "artifacts", "org/repo", &key)
                .await
                .unwrap_err()
                .to_string()
                .contains("too large")
        );
        let mut unbounded = serde_json::to_vec(&observed).unwrap();
        unbounded.resize(8_388_609, b' ');
        backend.untrusted_body = Some(unbounded);
        assert!(
            download_report(&backend, "artifacts", "org/repo", &key)
                .await
                .unwrap_err()
                .to_string()
                .contains("size limit")
        );
        let mut oversized = observed;
        oversized.identity.build_shape = Some("x".repeat(8_388_608));
        assert!(
            upload_report(&backend, "artifacts", &oversized)
                .await
                .unwrap_err()
                .to_string()
                .contains("size limit")
        );
    }

    #[tokio::test]
    async fn filesystem_history_rehydrates_order_and_matrix_variants_without_local_index() {
        use crate::config::{FilesystemRemoteConfig, RemoteBackendConfig, RemoteConfig};
        let remote_dir = tempfile::tempdir().unwrap();
        let runtime = tempfile::tempdir().unwrap();
        let config = RemoteConfig {
            prefix: "artifacts".into(),
            backend: RemoteBackendConfig::Filesystem(FilesystemRemoteConfig {
                root: remote_dir.path().into(),
                atomic_write_dir: remote_dir.path().join(".staging"),
            }),
        };
        let backend = crate::remote_backend::create_backend(&config, 30)
            .await
            .unwrap();
        let first = collect_reports(
            &[
                event("one", "/work", 'a', 1200, 200),
                event("one", "/work", 'b', 1300, 10),
                event("one", "/work", 'a', 1600, 2),
            ],
            None,
            runtime.path(),
            Some("org/repo"),
        )
        .unwrap()
        .remove(0);
        let mut second = first.clone();
        second.session_id = "two".into();
        second.commit = Some("different-commit".into());
        second.identity.profile = Some("release".into());
        second.identity.build_shape = Some("different-shape".into());
        for item in [&first, &second] {
            upload_report(backend.as_ref(), &config.prefix, item)
                .await
                .unwrap();
        }
        let mut index = vec![first.clone(), second.clone()];
        index.clear();
        drop(backend);
        // New backend/client: neither the event log nor an index is consulted.
        let restarted = crate::remote_backend::create_backend(&config, 30)
            .await
            .unwrap();
        for key in list_reports(restarted.as_ref(), &config.prefix, "org/repo")
            .await
            .unwrap()
        {
            index.push(
                download_report(restarted.as_ref(), &config.prefix, "org/repo", &key)
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        assert_eq!(index, vec![first, second]);
        let scoped = discovery_prefix(&config.prefix, "org/repo").unwrap();
        restarted
            .put(
                &format!("{scoped}nested/untrusted.json"),
                b"{}".to_vec(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            list_reports(restarted.as_ref(), &config.prefix, "org/repo")
                .await
                .unwrap()
                .len(),
            2
        );
    }
}
