//! User-declared extra cache-key inputs (issues #220 and #368).
//!
//! kache keys a crate on what rustc *reports* — source files (dep-info),
//! `--extern` artifacts, flags. A growing class of crates also read files
//! at **compile time that rustc never reports**: sqlx's `query!` macro reads
//! `.sqlx/query-*.json`, migration macros read `migrations/`, codegen reads
//! data files. Editing one of those changes the compiled output but no `.rs`
//! rustc lists — so kache's key doesn't move and a stale artifact is restored
//! (a false hit).
//!
//! This module lets a crate *declare* those files in a co-located
//! `<crate-dir>/kache.toml`:
//!
//! ```toml
//! extra_inputs = [".sqlx/**/*.json", "migrations/**/*.sql"]
//! ```
//!
//! A workspace may instead assign workspace-root-relative inputs to explicit
//! provider packages in `<workspace>/.kache.toml`. A propagated provider digest
//! also enters each direct `--extern` consumer key, covering proc macros that
//! read their hidden input only while expanding that consumer.
//!
//! The declared files' content hashes are folded into that crate's key, so a
//! change to them re-keys (a clean miss) instead of serving a stale hit.
//!
//! ## Safety properties
//! - **Opt-in, union-only.** A crate with no `kache.toml` is untouched (one
//!   `stat`, key byte-identical to today — so no `CACHE_KEY_VERSION` bump is
//!   needed). A misconfigured glob can only *add* inputs → an extra miss,
//!   never a wrong artifact.
//! - **Local & explicit.** The file lives inside the crate it applies to and
//!   only ever affects that crate's key — it can never *implicitly* apply to
//!   other projects, and a sibling crate is unaffected. A pattern *may*
//!   deliberately reach outside
//!   the crate (absolute / `..`) when a build genuinely depends on a shared or
//!   machine-specific file; that stays fail-safe but makes the key
//!   host-/layout-specific, so a portability warning fires.
//! - **Relocation-stable, swap-sensitive.** Each file is folded as its
//!   *crate-relative path* + *content hash* (`/`-normalized, sorted). Moving
//!   the worktree or restoring on another machine doesn't change the key
//!   (the path is crate-relative), but swapping two matched files' contents —
//!   where the filename→content binding is load-bearing, e.g. sqlx migration
//!   order — does, because the path travels with the hash.
//! - **Config changes count.** The declared pattern strings are folded too,
//!   so editing `kache.toml` re-keys even when it matches zero files; a
//!   non-empty declaration whose patterns are all rejected still folds (it
//!   never collapses to the unconfigured key).

use crate::cache_key::FileHasher;
use anyhow::{Context, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

/// The co-located per-crate config file. Deliberately distinct from the
/// project config `.kache.toml` so a crate-local file can never shadow the
/// workspace's remote/store settings via the ancestor walk.
const COLOCATED_NAME: &str = "kache.toml";

/// Above this many matched files, an `extra_inputs` glob is almost certainly
/// over-broad (e.g. accidentally spanning `target/`, or an absolute `/**`):
/// folding that many files busts the key on every change and walks a large
/// tree each compile. Warn so it's visible under default verbosity without
/// failing the build — over-folding is fail-safe, just slow.
const OVER_BROAD_FILE_WARN: usize = 1000;

fn should_warn_over_broad_file_count(count: usize) -> bool {
    count > OVER_BROAD_FILE_WARN
}

/// Package selectors must be exact Cargo package names: non-empty and free of
/// surrounding whitespace. Keep the predicate pure so `||` / `&&` mutations are
/// observable from unit tests without standing up a full workspace.
fn workspace_package_selector_is_invalid(selector: &str) -> bool {
    selector.trim() != selector || selector.is_empty()
}

/// Unset `$ENV` references in a pattern stay literal and match nothing; warn so
/// the miss is visible instead of a silent matches-nothing key.
fn should_warn_unset_extra_input_vars(unset_vars: &[String]) -> bool {
    !unset_vars.is_empty()
}

/// Absolute paths and `..` make a co-located pattern host-/layout-specific.
fn pattern_reaches_outside_crate(path: &Path) -> bool {
    path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
}

/// Cargo member globs must stay inside the workspace root.
fn member_pattern_escapes_workspace(path: &Path) -> bool {
    path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
}

/// Library crate names must be non-empty ASCII alphanumeric / `_`.
fn is_valid_rustc_crate_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
}

/// A declaration with more distinct Cargo directory dependencies than this is
/// almost certainly being used as a generated watch list rather than a small
/// set of input globs. Keep the consumer fingerprint bounded.
const MAX_WATCH_PATHS: usize = 256;

/// Whether this crate has a co-located extra-input declaration.
///
/// Adaptive incremental mode cannot cheaply validate those untracked inputs
/// before its early path, so their presence disables and clears adaptation for
/// the unit. I/O errors fail closed as "declared".
#[cfg(test)]
pub(crate) fn declared(source_file: Option<&Path>) -> bool {
    let Some(crate_dir) = source_file.and_then(crate_dir_from_source) else {
        return false;
    };
    match std::fs::symlink_metadata(crate_dir.join(COLOCATED_NAME)) {
        Ok(_) => true,
        Err(error) => error.kind() != std::io::ErrorKind::NotFound,
    }
}

/// Minimal schema for `<crate-dir>/kache.toml`. `deny_unknown_fields` makes a
/// stray `remote`/`local_store`/etc. a loud parse error rather than a
/// silently-honored crate-granularity setting — this file is *only* for
/// extra inputs.
#[derive(serde::Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct ColocatedConfig {
    extra_inputs: Vec<String>,
}

/// One invocation's fully-resolved extra-input declaration.
///
/// The wrapper resolves this once, passes [`Self::digest`] into key
/// computation, then uses [`Self::merge_into_dep_info`] on the consumer-facing
/// dep-info after either compilation or restore. Paths are those of the
/// current consumer worktree; nothing in this value comes from a cached
/// producer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExtraInputsSnapshot {
    config_path: PathBuf,
    /// Additional declarations/manifests that selected the same effective
    /// snapshot. Co-located Phase 1 has none; workspace rules union their
    /// project config and package-mapping manifests here.
    additional_config_paths: Vec<PathBuf>,
    normalized_patterns: Vec<String>,
    /// `None` for an explicit empty declaration: Cargo still watches the
    /// config so later activation is visible, while the cache key remains
    /// byte-identical to an unconfigured crate.
    digest: Option<String>,
    matched_files: Vec<PathBuf>,
    /// Narrow directories Cargo may recursively fingerprint to notice a glob
    /// addition/deletion, or creation of a currently-missing literal input.
    watch_paths: Vec<PathBuf>,
    /// Metadata observed after resolution. This is deliberately outside the
    /// cache-key digest: it detects ABA races (v1 -> transient -> v1, or a
    /// glob member added then removed) before Cargo accepts compiler output.
    observations: Vec<InputObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InputObservation {
    path: PathBuf,
    size: u64,
    mtime_ns: i64,
    ctime_ns: i64,
    inode: i64,
}

fn observe_dependency(path: &Path) -> Result<InputObservation> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("reading extra_inputs metadata for {}", path.display()))?;
    Ok(InputObservation {
        path: path.to_path_buf(),
        size: metadata.len(),
        mtime_ns: crate::cache_key::metadata_mtime_ns(&metadata),
        ctime_ns: crate::cache_key::metadata_ctime_ns(&metadata),
        inode: crate::cache_key::metadata_inode(&metadata),
    })
}

impl ExtraInputsSnapshot {
    /// Resolve a crate's active declaration exactly once.
    ///
    /// `Ok(None)` retains the old no-op cases: non-primary invocation or no
    /// co-located config. An explicit empty declaration returns a config-only
    /// snapshot so Cargo can observe later activation without changing the
    /// cache key. Invalid declarations and unsafe watches fail closed.
    #[cfg(test)]
    pub(crate) fn resolve(
        source_file: Option<&Path>,
        crate_name: &str,
        is_primary: bool,
        file_hasher: &FileHasher<'_>,
    ) -> Result<Option<Self>> {
        resolve_snapshot(source_file, crate_name, is_primary, file_hasher, true)
    }

    /// Resolve the complete Rust/Cargo snapshot: the existing co-located
    /// declaration plus any workspace rule applying to this provider or one of
    /// its direct `--extern` providers.
    pub(crate) fn resolve_for_rustc(
        args: &crate::args::RustcArgs,
        file_hasher: &FileHasher<'_>,
    ) -> Result<Option<Self>> {
        let crate_name = args.crate_name.as_deref().unwrap_or("unknown");
        let colocated = resolve_snapshot(
            args.source_file.as_deref(),
            crate_name,
            args.is_primary,
            file_hasher,
            true,
        )?;
        let workspace = resolve_workspace_snapshot(args, file_hasher)?;
        Ok(combine_snapshots(
            [("colocated", colocated), ("workspace", workspace)]
                .into_iter()
                .filter_map(|(label, snapshot)| {
                    snapshot.map(|snapshot| (label.to_string(), snapshot))
                })
                .collect(),
        ))
    }

    pub(crate) fn digest(&self) -> Option<&str> {
        self.digest.as_deref()
    }

    /// Merge config, matched files, and narrow directory watches into the
    /// first dependency rule of a rustc/Cargo dep-info file.
    ///
    /// Only consumer-facing dep-info should be passed here (after a cache
    /// store/restore boundary), so a restored file never retains a producer's
    /// absolute worktree paths. Current-consumer paths are emitted absolute so
    /// Cargo resolves them correctly even when rustc runs from a workspace root.
    pub(crate) fn merge_into_dep_info(&self, dep_info_path: &Path) -> Result<()> {
        merge_snapshot_into_dep_info(self, dep_info_path)
    }

    /// Pure form used by cache restore: complete and validate the expanded
    /// consumer bytes before any cached artifact is materialized or reported
    /// as a hit.
    pub(crate) fn merge_dep_info_content(&self, content: &str) -> Result<String> {
        merge_snapshot_dep_info_content(self, content)
    }
}

fn combine_snapshots(
    mut contributions: Vec<(String, ExtraInputsSnapshot)>,
) -> Option<ExtraInputsSnapshot> {
    if contributions.is_empty() {
        return None;
    }
    if contributions.len() == 1 {
        return contributions.pop().map(|(_, snapshot)| snapshot);
    }
    contributions.sort_by(|left, right| left.0.cmp(&right.0));

    let mut labeled_digests = Vec::new();
    let mut config_paths = BTreeSet::new();
    let mut normalized_patterns = BTreeSet::new();
    let mut matched_files = BTreeSet::new();
    let mut watch_paths = BTreeSet::new();
    let mut observations = Vec::new();

    for (label, snapshot) in contributions {
        if let Some(digest) = snapshot.digest {
            labeled_digests.push((label, digest));
        }
        config_paths.insert(snapshot.config_path);
        config_paths.extend(snapshot.additional_config_paths);
        normalized_patterns.extend(snapshot.normalized_patterns);
        matched_files.extend(snapshot.matched_files);
        watch_paths.extend(snapshot.watch_paths);
        observations.extend(snapshot.observations);
    }

    observations.sort_by(|left, right| left.path.cmp(&right.path));
    observations.dedup();
    let mut config_paths = config_paths.into_iter();
    let config_path = config_paths
        .next()
        .expect("a snapshot always carries its declaration path");
    let digest = if labeled_digests.is_empty() {
        None
    } else if labeled_digests.len() == 1 {
        labeled_digests.pop().map(|(_, digest)| digest)
    } else {
        let mut hasher = blake3::Hasher::new();
        for (label, digest) in labeled_digests {
            hasher.update(b"extra_inputs_snapshot:");
            hasher.update(label.as_bytes());
            hasher.update(b"=");
            hasher.update(digest.as_bytes());
            hasher.update(b"\x1f");
        }
        Some(hasher.finalize().to_hex().to_string())
    };

    Some(ExtraInputsSnapshot {
        config_path,
        additional_config_paths: config_paths.collect(),
        normalized_patterns: normalized_patterns.into_iter().collect(),
        digest,
        matched_files: matched_files.into_iter().collect(),
        watch_paths: watch_paths.into_iter().collect(),
        observations,
    })
}

#[derive(serde::Deserialize, Default)]
#[serde(default)]
struct WorkspaceConfigEnvelope {
    workspace: Option<WorkspaceExtraInputsConfig>,
}

#[derive(serde::Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct WorkspaceExtraInputsConfig {
    extra_inputs: Vec<WorkspaceExtraInputsRule>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkspaceExtraInputsRule {
    crates: Vec<String>,
    inputs: Vec<String>,
    #[serde(default)]
    propagate_to_dependents: bool,
}

#[derive(serde::Deserialize)]
struct CargoWorkspaceManifest {
    package: Option<CargoPackage>,
    workspace: Option<CargoWorkspace>,
    lib: Option<CargoLib>,
}

#[derive(serde::Deserialize)]
struct CargoPackage {
    name: String,
    autolib: Option<bool>,
}

#[derive(serde::Deserialize, Default)]
#[serde(default)]
struct CargoWorkspace {
    members: Vec<String>,
    exclude: Vec<String>,
}

#[derive(serde::Deserialize, Default)]
#[serde(default)]
struct CargoLib {
    name: Option<String>,
    path: Option<String>,
}

#[derive(Debug, Clone)]
struct WorkspacePackage {
    package_name: String,
    crate_name: Option<String>,
    lib_source_path: Option<PathBuf>,
    manifest_path: PathBuf,
}

struct WorkspaceProviderSpec {
    package: WorkspacePackage,
    rule_indices: Vec<usize>,
}

fn resolve_workspace_snapshot(
    args: &crate::args::RustcArgs,
    file_hasher: &FileHasher<'_>,
) -> Result<Option<ExtraInputsSnapshot>> {
    if !args.is_primary {
        return Ok(None);
    }
    let Some(source_file) = args.source_file.as_deref() else {
        return Ok(None);
    };

    let active_config = crate::config::resolve_config_path();
    if active_config.file_name().and_then(|name| name.to_str()) != Some(".kache.toml") {
        return Ok(None);
    }

    // Parse first so an ordinary config remains valid even when its path is
    // not suitable as a workspace anchor. Once workspace rules are active,
    // however, silently choosing a lexical root would make cache keys depend
    // on whether a `..` crossed a symlink.
    let raw = std::fs::read(&active_config).with_context(|| {
        format!(
            "reading workspace extra_inputs config {}",
            active_config.display()
        )
    })?;
    let text = std::str::from_utf8(&raw).with_context(|| {
        format!(
            "workspace extra_inputs config {} must be valid UTF-8",
            active_config.display()
        )
    })?;
    let envelope: WorkspaceConfigEnvelope = toml::from_str(text).with_context(|| {
        format!(
            "parsing workspace extra_inputs config {}",
            active_config.display()
        )
    })?;
    let Some(workspace_config_body) = envelope.workspace else {
        return Ok(None);
    };
    if workspace_config_body.extra_inputs.is_empty() {
        return Ok(None);
    }

    if active_config
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        anyhow::bail!(
            "workspace extra_inputs config path {} contains `..`; its workspace root is ambiguous across symlinks",
            active_config.display()
        );
    }

    let active_config = std::path::absolute(&active_config).with_context(|| {
        format!(
            "resolving workspace extra_inputs config path {}",
            active_config.display()
        )
    })?;
    let Some(workspace_root) = active_config.parent().map(lexical_normalize) else {
        anyhow::bail!(
            "workspace extra_inputs config {} has no workspace-root parent",
            active_config.display()
        );
    };
    let workspace_config = workspace_root.join(".kache.toml");
    if lexical_normalize(&active_config) != workspace_config {
        anyhow::bail!(
            "workspace extra_inputs config path {} does not unambiguously name {}",
            active_config.display(),
            workspace_config.display()
        );
    }

    let source_file = absolute_source_file(source_file)?;
    let manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .and_then(|path| std::path::absolute(path).ok())
        .map(|path| lexical_normalize(&path));
    if !source_file.starts_with(&workspace_root)
        && !manifest_dir
            .as_deref()
            .is_some_and(|path| path.starts_with(&workspace_root))
    {
        // A user/global `KACHE_CONFIG` may contain the schema, but it must
        // never inject rules while Cargo compiles an unrelated package.
        return Ok(None);
    }

    let metadata = std::fs::symlink_metadata(&workspace_config).with_context(|| {
        format!(
            "reading workspace extra_inputs config metadata {}",
            workspace_config.display()
        )
    })?;
    if metadata.file_type().is_symlink() {
        anyhow::bail!(
            "workspace extra_inputs config {} is a symlink; Cargo cannot notice it being retargeted safely",
            workspace_config.display()
        );
    }

    let workspace_manifest = workspace_root.join("Cargo.toml");
    let Ok(root_manifest) = read_cargo_manifest(&workspace_manifest) else {
        // A global/explicit `.kache.toml` can be an ancestor of a source tree;
        // it is not a workspace-rule anchor unless its sibling manifest is an
        // actual Cargo workspace root.
        return Ok(None);
    };
    if root_manifest.workspace.is_none() {
        return Ok(None);
    }
    let packages = load_workspace_packages(&workspace_root, &workspace_manifest)?;
    let current_package =
        current_workspace_package(&packages, &source_file, manifest_dir.as_deref());

    // Resolve only package/crate identities here. Input globs can be large or
    // fail closed on unsafe filesystem topology, so they are deliberately
    // evaluated later and only for the current provider/direct extern.
    let mut providers_by_package: BTreeMap<String, WorkspaceProviderSpec> = BTreeMap::new();

    for (rule_index, rule) in workspace_config_body.extra_inputs.iter().enumerate() {
        if rule.crates.is_empty() {
            anyhow::bail!(
                "workspace.extra_inputs rule must select at least one package in `crates`"
            );
        }
        let mut selectors = BTreeSet::new();
        for selector in &rule.crates {
            if workspace_package_selector_is_invalid(selector) {
                anyhow::bail!(
                    "workspace.extra_inputs package selector {selector:?} must be a non-empty exact Cargo package name without surrounding whitespace"
                );
            }
            if selector.contains(['*', '?', '[', ']']) {
                anyhow::bail!(
                    "workspace.extra_inputs package selector {selector:?} uses glob syntax; v1 requires exact Cargo package names"
                );
            }
            selectors.insert(selector.clone());
        }

        for selector in selectors {
            let package = packages.get(&selector).ok_or_else(|| {
                anyhow::anyhow!(
                    "workspace.extra_inputs selector {selector:?} does not resolve to an explicit member of {}",
                    workspace_manifest.display()
                )
            })?;
            if rule.propagate_to_dependents && package.crate_name.is_none() {
                anyhow::bail!(
                    "workspace.extra_inputs package {selector:?} sets propagate_to_dependents=true but has no library/proc-macro target to provide through --extern"
                );
            }

            providers_by_package
                .entry(selector.clone())
                .or_insert_with(|| WorkspaceProviderSpec {
                    package: package.clone(),
                    rule_indices: Vec::new(),
                })
                .rule_indices
                .push(rule_index);
        }
    }

    let mut providers_by_crate: BTreeMap<String, Vec<&WorkspaceProviderSpec>> = BTreeMap::new();
    for provider in providers_by_package.values() {
        if provider
            .rule_indices
            .iter()
            .any(|index| workspace_config_body.extra_inputs[*index].propagate_to_dependents)
        {
            let crate_name = provider
                .package
                .crate_name
                .as_ref()
                .expect("propagated provider library was checked above");
            providers_by_crate
                .entry(crate_name.clone())
                .or_default()
                .push(provider);
        }
    }

    let mut relevant: BTreeMap<
        String,
        (
            bool,
            &WorkspaceProviderSpec,
            BTreeMap<PathBuf, InputObservation>,
        ),
    > = BTreeMap::new();
    if let Some(current_package) = current_package
        && let Some(provider) = providers_by_package.get(&current_package.package_name)
    {
        relevant.insert(
            current_package.package_name.clone(),
            (true, provider, BTreeMap::new()),
        );
    }

    for external in &args.externs {
        let Some(identity) = external.path.as_deref().and_then(cargo_artifact_identity) else {
            if providers_by_crate.contains_key(&normalize_crate_name(&external.name)) {
                anyhow::bail!(
                    "workspace.extra_inputs cannot attribute --extern {} without a parseable Cargo artifact path; rebuild the selected provider once and retry",
                    external.name
                );
            }
            continue;
        };
        let Some(candidates) = providers_by_crate.get(&identity.crate_name) else {
            continue;
        };
        let Some((provider, observation)) = resolve_artifact_provider(&identity, candidates)?
        else {
            continue;
        };
        relevant
            .entry(provider.package.package_name.clone())
            .and_modify(|(_, _, observations)| {
                observations.insert(observation.path.clone(), observation.clone());
            })
            .or_insert_with(|| {
                (
                    false,
                    provider,
                    BTreeMap::from([(observation.path.clone(), observation)]),
                )
            });
    }

    if relevant.is_empty() {
        return Ok(None);
    }

    let mut contributions = Vec::new();
    for (package_name, (is_provider, provider, dep_info_observations)) in relevant {
        let mut snapshot = resolve_workspace_provider_snapshot(
            provider,
            is_provider,
            &workspace_config_body.extra_inputs,
            &workspace_root,
            &workspace_config,
            &workspace_manifest,
            file_hasher,
        )?;
        snapshot
            .observations
            .extend(dep_info_observations.into_values());
        snapshot
            .observations
            .sort_by(|left, right| left.path.cmp(&right.path));
        snapshot.observations.dedup();
        let role = if is_provider { "provider" } else { "extern" };
        contributions.push((format!("{role}:{package_name}"), snapshot));
    }

    Ok(combine_snapshots(contributions))
}

fn resolve_workspace_provider_snapshot(
    provider: &WorkspaceProviderSpec,
    include_unpropagated: bool,
    rules: &[WorkspaceExtraInputsRule],
    workspace_root: &Path,
    workspace_config: &Path,
    workspace_manifest: &Path,
    file_hasher: &FileHasher<'_>,
) -> Result<ExtraInputsSnapshot> {
    let package_name = &provider.package.package_name;
    let mut snapshots = BTreeMap::new();

    for rule_index in &provider.rule_indices {
        let rule = &rules[*rule_index];
        if !include_unpropagated && !rule.propagate_to_dependents {
            continue;
        }
        validate_workspace_rule_inputs(rule)?;
        let mut snapshot = resolve_declared_inputs(
            "workspace",
            workspace_root.to_path_buf(),
            workspace_config.to_path_buf(),
            &rule.inputs,
            file_hasher,
            true,
        )?
        .expect("validated workspace rule has at least one input");
        add_workspace_mapping_dependencies(
            &mut snapshot,
            workspace_root,
            workspace_manifest,
            &provider.package.manifest_path,
            file_hasher,
        )?;
        if include_unpropagated && let Some(source) = provider.package.lib_source_path.as_deref() {
            add_workspace_provider_provenance(&mut snapshot, workspace_root, source)?;
        }
        relabel_workspace_snapshot(&mut snapshot, package_name, rule.propagate_to_dependents);
        let identity = snapshot
            .digest
            .clone()
            .expect("workspace rule relabel retains a digest");
        snapshots.entry(identity).or_insert(snapshot);
    }

    combine_snapshots(snapshots.into_iter().collect()).ok_or_else(|| {
        anyhow::anyhow!("workspace package {package_name:?} has no applicable input rule")
    })
}

fn validate_workspace_rule_inputs(rule: &WorkspaceExtraInputsRule) -> Result<()> {
    if rule.inputs.is_empty() {
        anyhow::bail!(
            "workspace.extra_inputs rule must declare at least one workspace-relative path in `inputs`"
        );
    }
    for input in &rule.inputs {
        let input_path = Path::new(input);
        if input_path.is_absolute()
            || input_path
                .components()
                .any(|component| matches!(component, Component::ParentDir))
        {
            anyhow::bail!(
                "workspace.extra_inputs path {input:?} must stay relative to the workspace root and must not contain `..`"
            );
        }
    }
    Ok(())
}

fn relabel_workspace_snapshot(
    snapshot: &mut ExtraInputsSnapshot,
    package_name: &str,
    propagate_to_dependents: bool,
) {
    let inner = snapshot
        .digest
        .as_deref()
        .expect("non-empty workspace rule has a digest");
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"workspace_extra_inputs.v1\0");
    hasher.update(package_name.as_bytes());
    hasher.update(b"\0");
    hasher.update(&[propagate_to_dependents as u8]);
    hasher.update(inner.as_bytes());
    snapshot.digest = Some(hasher.finalize().to_hex().to_string());
}

fn add_workspace_mapping_dependencies(
    snapshot: &mut ExtraInputsSnapshot,
    workspace_root: &Path,
    workspace_manifest: &Path,
    package_manifest: &Path,
    file_hasher: &FileHasher<'_>,
) -> Result<()> {
    for manifest in [workspace_manifest, package_manifest] {
        if let Some(symlink) = first_symlink_below_common(workspace_root, manifest) {
            anyhow::bail!(
                "workspace extra_inputs package mapping {} crosses symlink {}; Cargo cannot track retargeting safely",
                manifest.display(),
                symlink.display()
            );
        }
        file_hasher.hash(manifest).with_context(|| {
            format!(
                "hashing workspace extra_inputs package mapping {}",
                manifest.display()
            )
        })?;
        snapshot
            .additional_config_paths
            .push(manifest.to_path_buf());
        snapshot.observations.push(observe_dependency(manifest)?);
    }
    snapshot.additional_config_paths.sort();
    snapshot.additional_config_paths.dedup();
    snapshot
        .observations
        .sort_by(|left, right| left.path.cmp(&right.path));
    snapshot.observations.dedup();
    Ok(())
}

/// Add an absolute producer-only marker to the provider artifact's dep-info.
/// Direct consumers deliberately do not inherit this path: a later consumer
/// can therefore prove that a same-named Cargo artifact was produced by this
/// selected package rather than by another package exposing the same lib name.
fn add_workspace_provider_provenance(
    snapshot: &mut ExtraInputsSnapshot,
    workspace_root: &Path,
    lib_source_path: &Path,
) -> Result<()> {
    if let Some(symlink) = first_symlink_below_common(workspace_root, lib_source_path) {
        anyhow::bail!(
            "workspace extra_inputs provider source {} crosses symlink {}; artifact provenance cannot track retargeting safely",
            lib_source_path.display(),
            symlink.display()
        );
    }
    snapshot
        .additional_config_paths
        .push(lib_source_path.to_path_buf());
    snapshot
        .observations
        .push(observe_dependency(lib_source_path)?);
    snapshot.additional_config_paths.sort();
    snapshot.additional_config_paths.dedup();
    snapshot
        .observations
        .sort_by(|left, right| left.path.cmp(&right.path));
    snapshot.observations.dedup();
    Ok(())
}

fn normalize_crate_name(name: &str) -> String {
    name.replace('-', "_")
}

struct CargoArtifactIdentity {
    crate_name: String,
    dep_info_path: PathBuf,
}

fn cargo_artifact_identity(path: &Path) -> Option<CargoArtifactIdentity> {
    let stem = path.file_stem()?.to_str()?;
    let (name, hash) = stem.rsplit_once('-')?;
    if hash.len() < 8 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    let has_lib_prefix = path
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| matches!(extension, "rlib" | "rmeta" | "so" | "dylib" | "a"));
    let name = if has_lib_prefix {
        name.strip_prefix("lib").unwrap_or(name)
    } else {
        name
    };
    if name.is_empty() {
        return None;
    }
    let crate_name = normalize_crate_name(name);
    let dep_info_path = path.parent()?.join(format!("{crate_name}-{hash}.d"));
    Some(CargoArtifactIdentity {
        crate_name,
        dep_info_path,
    })
}

fn resolve_artifact_provider<'a>(
    identity: &CargoArtifactIdentity,
    candidates: &[&'a WorkspaceProviderSpec],
) -> Result<Option<(&'a WorkspaceProviderSpec, InputObservation)>> {
    let observation_before = observe_dependency(&identity.dep_info_path).with_context(|| {
        format!(
            "workspace.extra_inputs cannot observe producer dep-info {} for crate {:?}; rebuild the selected provider once and retry",
            identity.dep_info_path.display(),
            identity.crate_name
        )
    })?;
    let raw = std::fs::read_to_string(&identity.dep_info_path).with_context(|| {
        format!(
            "workspace.extra_inputs cannot read producer dep-info {} for crate {:?}; rebuild the selected provider once (for example `cargo clean -p <package> && cargo build`) and retry",
            identity.dep_info_path.display(),
            identity.crate_name
        )
    })?;
    let observation_after = observe_dependency(&identity.dep_info_path)?;
    if observation_before != observation_after {
        anyhow::bail!(
            "workspace.extra_inputs producer dep-info {} changed while artifact provenance was being read; retry the build",
            identity.dep_info_path.display()
        );
    }
    let dependencies = parse_dep_info_dependencies(&raw).with_context(|| {
        format!(
            "workspace.extra_inputs cannot parse producer dep-info {} for crate {:?}; rebuild the selected provider once and retry",
            identity.dep_info_path.display(),
            identity.crate_name
        )
    })?;
    let cwd = std::env::current_dir().context("resolving producer dep-info dependencies")?;
    let dependencies: BTreeSet<PathBuf> = dependencies
        .into_iter()
        .map(|path| {
            if path.is_absolute() {
                lexical_normalize(&path)
            } else {
                anchor_input_path(&cwd, &path)
            }
        })
        .collect();
    let source_matches: Vec<_> = candidates
        .iter()
        .copied()
        .filter(|provider| {
            let source = provider
                .package
                .lib_source_path
                .as_ref()
                .expect("propagated provider has a resolved library source");
            dependencies.contains(source)
        })
        .collect();
    if source_matches.is_empty() {
        // A producer for some other package may validly expose the same lib
        // crate name. Without this selected provider's source marker it is
        // proven unrelated, so leave the consumer's key byte-identical.
        return Ok(None);
    }
    let matches: Vec<_> = source_matches
        .into_iter()
        .filter(|provider| dependencies.contains(&provider.package.manifest_path))
        .collect();

    match matches.as_slice() {
        [provider] => Ok(Some((*provider, observation_before))),
        [] => anyhow::bail!(
            "workspace.extra_inputs found an incomplete producer marker for artifact crate {:?} in {}; rebuild the selected provider once (for example `cargo clean -p <package> && cargo build`) and retry",
            identity.crate_name,
            identity.dep_info_path.display()
        ),
        _ => anyhow::bail!(
            "workspace.extra_inputs artifact provenance for crate {:?} is ambiguous in {}; selected packages must produce distinct Cargo units",
            identity.crate_name,
            identity.dep_info_path.display()
        ),
    }
}

fn load_workspace_packages(
    workspace_root: &Path,
    workspace_manifest: &Path,
) -> Result<BTreeMap<String, WorkspacePackage>> {
    let root_manifest = read_cargo_manifest(workspace_manifest)?;
    let workspace = root_manifest.workspace.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "workspace extra_inputs config {} must sit beside a Cargo.toml containing [workspace]",
            workspace_root.join(".kache.toml").display()
        )
    })?;

    let mut manifests = BTreeSet::new();
    if root_manifest.package.is_some() {
        manifests.insert(workspace_manifest.to_path_buf());
    }
    for member in &workspace.members {
        manifests.extend(expand_workspace_member_pattern(workspace_root, member)?);
    }
    let mut excluded = BTreeSet::new();
    for pattern in &workspace.exclude {
        excluded.extend(expand_workspace_member_pattern(workspace_root, pattern)?);
    }
    manifests.retain(|manifest| !excluded.contains(manifest));

    let mut packages = BTreeMap::new();
    for manifest_path in manifests {
        let manifest = read_cargo_manifest(&manifest_path)?;
        let Some(package) = manifest.package else {
            continue;
        };
        let package_dir = manifest_path
            .parent()
            .expect("Cargo.toml member has a parent directory");
        let has_lib_target = manifest.lib.is_some();
        let (crate_name, lib_source_path) = if has_lib_target {
            let lib = manifest.lib.as_ref().expect("checked above");
            let name = lib
                .name
                .clone()
                .unwrap_or_else(|| normalize_crate_name(&package.name));
            let name = validate_rustc_crate_name(&name, &manifest_path)?;
            let source = lib.path.as_deref().unwrap_or("src/lib.rs");
            (
                Some(name),
                Some(lexical_normalize(&package_dir.join(source))),
            )
        } else if package.autolib != Some(false) && package_dir.join("src/lib.rs").is_file() {
            (
                Some(validate_rustc_crate_name(
                    &normalize_crate_name(&package.name),
                    &manifest_path,
                )?),
                Some(package_dir.join("src/lib.rs")),
            )
        } else {
            (None, None)
        };
        let workspace_package = WorkspacePackage {
            package_name: package.name.clone(),
            crate_name,
            lib_source_path,
            manifest_path,
        };
        if packages
            .insert(package.name.clone(), workspace_package)
            .is_some()
        {
            anyhow::bail!(
                "workspace extra_inputs cannot resolve duplicate package name {:?} unambiguously",
                package.name
            );
        }
    }
    Ok(packages)
}

fn absolute_source_file(source_file: &Path) -> Result<PathBuf> {
    let absolute = if source_file.is_absolute() {
        source_file.to_path_buf()
    } else {
        std::env::current_dir()
            .context("resolving rustc source file from current directory")?
            .join(source_file)
    };
    Ok(lexical_normalize(&absolute))
}

fn current_workspace_package<'a>(
    packages: &'a BTreeMap<String, WorkspacePackage>,
    source_file: &Path,
    cargo_manifest_dir: Option<&Path>,
) -> Option<&'a WorkspacePackage> {
    // Cargo provides CARGO_MANIFEST_DIR even when a custom target path lives
    // outside the package directory. Accept it only if it names one of this
    // workspace's resolved members; an unrelated inherited value is ignored.
    if let Some(manifest_dir) = cargo_manifest_dir {
        let manifest = lexical_normalize(&manifest_dir.join("Cargo.toml"));
        if let Some(package) = packages
            .values()
            .find(|package| package.manifest_path == manifest)
        {
            return Some(package);
        }
    }

    // For ordinary and in-package custom target paths, walk all source
    // ancestors. Do not assume the first Cargo.toml is the current package:
    // generated sources can sit below an unrelated nested manifest.
    let mut ancestor = source_file.parent();
    while let Some(directory) = ancestor {
        let manifest = lexical_normalize(&directory.join("Cargo.toml"));
        if let Some(package) = packages
            .values()
            .find(|package| package.manifest_path == manifest)
        {
            return Some(package);
        }
        ancestor = directory.parent();
    }
    None
}

fn validate_rustc_crate_name(name: &str, manifest: &Path) -> Result<String> {
    if !is_valid_rustc_crate_name(name) {
        anyhow::bail!(
            "workspace extra_inputs cannot map invalid library crate name {name:?} from {}",
            manifest.display()
        );
    }
    Ok(name.to_string())
}

fn read_cargo_manifest(path: &Path) -> Result<CargoWorkspaceManifest> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading workspace member manifest {}", path.display()))?;
    toml::from_str(&raw)
        .with_context(|| format!("parsing workspace member manifest {}", path.display()))
}

fn expand_workspace_member_pattern(workspace_root: &Path, pattern: &str) -> Result<Vec<PathBuf>> {
    let path = Path::new(pattern);
    if member_pattern_escapes_workspace(path) {
        anyhow::bail!(
            "workspace extra_inputs cannot enumerate Cargo member pattern {pattern:?} outside the workspace root"
        );
    }
    if pattern_uses_dynamic_expansion(pattern) {
        anyhow::bail!(
            "workspace extra_inputs cannot enumerate dynamic Cargo member pattern {pattern:?}"
        );
    }

    let manifest_pattern = format!(
        "{}/{}/Cargo.toml",
        glob::Pattern::escape(&workspace_root.to_string_lossy()),
        pattern.trim_end_matches('/')
    );
    let mut manifests = Vec::new();
    let entries = glob::glob(&manifest_pattern)
        .with_context(|| format!("parsing Cargo workspace member pattern {pattern:?}"))?;
    for entry in entries {
        let manifest = entry
            .with_context(|| format!("enumerating Cargo workspace member pattern {pattern:?}"))?;
        if manifest.is_file() {
            manifests.push(lexical_normalize(&manifest));
        }
    }
    manifests.sort();
    manifests.dedup();
    Ok(manifests)
}

/// Compute a digest of a crate's co-located extra inputs, or `None` when the
/// crate declares none (no `kache.toml`, empty list, or non-cacheable
/// invocation). Fold the returned digest into the crate's key via
/// [`crate::cache_key::fold_labeled`].
///
/// `source_file` is the compile's primary source; the crate dir is the
/// nearest ancestor containing a `Cargo.toml`. `file_hasher` is the same
/// memoized hasher the key build holds, so repeated files cost once.
pub(crate) fn digest(
    source_file: Option<&Path>,
    crate_name: &str,
    is_primary: bool,
    file_hasher: &FileHasher<'_>,
) -> Option<String> {
    // Compatibility API for C/C++ and any out-of-tree callers. It deliberately
    // does not enforce Cargo watch-root policy: that policy is Rust/Cargo-only,
    // while the digest bytes must retain their existing semantics. New Rust
    // code should resolve `ExtraInputsSnapshot` and propagate its `Result`.
    match resolve_snapshot(source_file, crate_name, is_primary, file_hasher, false) {
        Ok(snapshot) => snapshot.and_then(|snapshot| snapshot.digest),
        Err(error) => {
            tracing::warn!("[key:{crate_name}] failed to resolve extra_inputs: {error:#}");
            None
        }
    }
}

/// Fold a crate's co-located extra inputs into an already-computed key.
/// A no-op (returns `base` unchanged) when the crate declares none, so it is
/// safe to call unconditionally from every compiler family's `cache_key`.
pub(crate) fn apply_extra_inputs(
    base: String,
    source_file: Option<&Path>,
    crate_name: &str,
    is_primary: bool,
    file_hasher: &FileHasher<'_>,
) -> String {
    match digest(source_file, crate_name, is_primary, file_hasher) {
        Some(d) => crate::cache_key::fold_labeled(base, "extra_inputs", &d),
        None => base,
    }
}

/// Walk up from the primary source file to the nearest directory containing a
/// `Cargo.toml`. Cargo invokes rustc with cwd = the package source dir, so a
/// relative source path is anchored there. Returns `None` outside cargo's
/// layout (bare `rustc`/`cc` with no enclosing crate) — the feature is then a
/// no-op.
fn crate_dir_from_source(source_file: &Path) -> Option<PathBuf> {
    let cwd = std::env::current_dir().ok();
    let absolute = if source_file.is_absolute() {
        source_file.to_path_buf()
    } else {
        cwd?.join(source_file)
    };

    let mut dir = absolute.parent();
    while let Some(d) = dir {
        if d.join("Cargo.toml").is_file() {
            return Some(d.to_path_buf());
        }
        dir = d.parent();
    }
    None
}

#[derive(Debug, Clone)]
struct NormalizedInputPattern {
    glob: String,
    watch: WatchIntent,
}

#[derive(Debug, Clone)]
enum WatchIntent {
    /// A matched literal file catches edits/deletion itself. If currently
    /// missing, Cargo watches its nearest narrow existing parent directory.
    Literal(PathBuf),
    /// Cargo recursively fingerprints this literal root directory so a glob
    /// catches edits, additions, and deletions below it.
    DirectoryRoot(PathBuf),
}

fn io_error_is_not_found(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::NotFound
}

fn symlink_metadata_if_present(path: &Path) -> std::io::Result<Option<std::fs::Metadata>> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if io_error_is_not_found(&error) => Ok(None),
        Err(error) => Err(error),
    }
}

fn read_file_if_present(path: &Path) -> std::io::Result<Option<Vec<u8>>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if io_error_is_not_found(&error) => Ok(None),
        Err(error) => Err(error),
    }
}

fn legacy_digest_mode(strict_watches: bool) -> bool {
    !strict_watches
}

fn strict_input_error<E>(strict_watches: bool, error: E) -> std::result::Result<(), E> {
    if strict_watches { Err(error) } else { Ok(()) }
}

fn resolve_snapshot(
    source_file: Option<&Path>,
    crate_name: &str,
    is_primary: bool,
    file_hasher: &FileHasher<'_>,
    strict_watches: bool,
) -> Result<Option<ExtraInputsSnapshot>> {
    if !is_primary {
        return Ok(None);
    }
    let Some(source_file) = source_file else {
        return Ok(None);
    };
    let Some(crate_dir) = crate_dir_from_source(source_file) else {
        return Ok(None);
    };
    let crate_dir = lexical_normalize(&crate_dir);
    let config_path = crate_dir.join(COLOCATED_NAME);

    if strict_watches {
        match symlink_metadata_if_present(&config_path) {
            Ok(Some(metadata)) if metadata.file_type().is_symlink() => {
                anyhow::bail!(
                    "active extra_inputs config {} is a symlink; Cargo canonicalizes dep-info and \
                     cannot notice that link being retargeted safely",
                    config_path.display()
                );
            }
            Ok(Some(_)) => {}
            Ok(None) => return Ok(None),
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "reading active extra_inputs config metadata {}",
                        config_path.display()
                    )
                });
            }
        }
    }

    let raw = match read_file_if_present(&config_path) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return Ok(None),
        Err(error) if legacy_digest_mode(strict_watches) => {
            // Preserve the legacy digest API's unreadable-config no-op. The
            // strict snapshot API fails closed instead.
            tracing::warn!(
                "[key:{crate_name}] cannot read {}: {error}",
                config_path.display()
            );
            return Ok(None);
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "reading active extra_inputs config {}",
                    config_path.display()
                )
            });
        }
    };
    if strict_watches && windows_path_uses_device_namespace(&crate_dir) {
        anyhow::bail!(
            "active extra_inputs cannot enumerate a crate through a Windows verbatim/device \
             namespace path safely; use the ordinary drive or UNC spelling"
        );
    }
    if strict_watches {
        // The parsed pattern set, rather than formatting/comments, defines the
        // key. Still observe the config through the guarded hasher so a rewrite
        // racing this snapshot suppresses cache publication.
        file_hasher.hash(&config_path).with_context(|| {
            format!(
                "hashing active extra_inputs config {}",
                config_path.display()
            )
        })?;
    }

    let opaque_snapshot = |raw: &[u8]| ExtraInputsSnapshot {
        config_path: config_path.clone(),
        additional_config_paths: Vec::new(),
        normalized_patterns: Vec::new(),
        digest: Some(unparseable_digest(crate_name, &config_path, raw)),
        matched_files: Vec::new(),
        watch_paths: Vec::new(),
        observations: Vec::new(),
    };
    let text = match std::str::from_utf8(&raw) {
        Ok(text) => text,
        Err(_) if legacy_digest_mode(strict_watches) => return Ok(Some(opaque_snapshot(&raw))),
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "active extra_inputs config {} must be valid UTF-8",
                    config_path.display()
                )
            });
        }
    };
    let config: ColocatedConfig = match toml::from_str(text) {
        Ok(config) => config,
        Err(error) if strict_watches => {
            return Err(error).with_context(|| {
                format!(
                    "parsing active extra_inputs config {}",
                    config_path.display()
                )
            });
        }
        Err(error) => {
            tracing::warn!(
                "[key:{crate_name}] {} is invalid ({error}); folding it as an opaque \
                 input so the crate rebuilds until fixed",
                config_path.display()
            );
            return Ok(Some(opaque_snapshot(&raw)));
        }
    };

    resolve_declared_inputs(
        crate_name,
        crate_dir,
        config_path,
        &config.extra_inputs,
        file_hasher,
        strict_watches,
    )
}

/// Fold the declared pattern set and the content hashes of every matched file
/// into a single hex digest. Returns `None` only for the genuinely-empty
/// declaration (`extra_inputs = []`), an explicit opt-out that must stay
/// byte-identical to having no `kache.toml`. A non-empty declaration always
/// folds *something* — even if every pattern is rejected — so it can never
/// collapse back to the no-config key.
fn resolve_declared_inputs(
    crate_name: &str,
    crate_dir: PathBuf,
    config_path: PathBuf,
    patterns: &[String],
    file_hasher: &FileHasher<'_>,
    strict_watches: bool,
) -> Result<Option<ExtraInputsSnapshot>> {
    // An explicit empty list is the opt-out: byte-identical to no `kache.toml`.
    if patterns.is_empty() {
        return if strict_watches {
            let observations = vec![observe_dependency(&config_path)?];
            Ok(Some(ExtraInputsSnapshot {
                config_path,
                additional_config_paths: Vec::new(),
                normalized_patterns: Vec::new(),
                digest: None,
                matched_files: Vec::new(),
                watch_paths: Vec::new(),
                observations,
            }))
        } else {
            Ok(None)
        };
    }

    // Normalize the declared patterns. Out-of-crate patterns (absolute / `..`)
    // are kept (with a portability warning); only a pattern smuggling in the
    // fold separator is skipped.
    let mut by_glob = BTreeMap::new();
    let mut rejected_patterns = Vec::new();
    for pattern in patterns {
        if strict_watches && pattern_uses_dynamic_expansion(pattern) {
            anyhow::bail!(
                "active extra_inputs pattern {pattern:?} uses `$ENV` or `~` expansion; Cargo \
                 cannot notice that expansion changing, so use a stable literal path"
            );
        }
        if strict_watches && windows_pattern_has_ambiguous_root(Path::new(pattern)) {
            anyhow::bail!(
                "active extra_inputs pattern {pattern:?} uses a Windows rooted-without-drive or \
                 drive-relative path; use a fully-qualified absolute path or a crate-relative path"
            );
        }
        if strict_watches && windows_path_uses_device_namespace(Path::new(pattern)) {
            anyhow::bail!(
                "active extra_inputs pattern {pattern:?} uses a Windows verbatim/device namespace \
                 that glob enumeration cannot track safely; use an ordinary drive or UNC path"
            );
        }
        if strict_watches && parent_traversal_follows_glob(pattern) {
            anyhow::bail!(
                "active extra_inputs pattern {pattern:?} traverses `..` after a wildcard; \
                 Cargo cannot derive a bounded watch root that sees new matches"
            );
        }
        if let Some(normalized) = normalize_pattern_info(crate_name, &crate_dir, pattern) {
            by_glob.entry(normalized.glob.clone()).or_insert(normalized);
        } else {
            rejected_patterns.push(pattern);
        }
    }
    if strict_watches && !rejected_patterns.is_empty() {
        anyhow::bail!(
            "active extra_inputs contains {} invalid pattern(s); fix the declaration before Cargo freshness can be completed safely",
            rejected_patterns.len()
        );
    }
    let normalized: Vec<NormalizedInputPattern> = by_glob.into_values().collect();
    let normalized_patterns: Vec<String> = normalized
        .iter()
        .map(|pattern| pattern.glob.clone())
        .collect();

    // The author DECLARED inputs (non-empty list) but every pattern was
    // rejected. Collapsing to `None` here would make the key byte-identical to
    // having no `kache.toml` at all — silently re-opening the exact false hit
    // the feature exists to prevent, while the author believes the file is
    // tracked. Fold the raw declared patterns instead: the key is distinct
    // from no-config and any edit to `kache.toml` re-keys.
    if normalized_patterns.is_empty() {
        tracing::warn!(
            "[key:{crate_name}] every extra_inputs pattern was rejected; folding the raw \
             declaration so the crate stays distinct from an unconfigured one"
        );
        let mut hasher = blake3::Hasher::new();
        let mut raw: Vec<&String> = patterns.iter().collect();
        raw.sort();
        raw.dedup();
        for p in raw {
            hasher.update(b"extra_input_all_rejected:");
            hasher.update(p.as_bytes());
            hasher.update(b"\x1f");
        }
        return Ok(Some(ExtraInputsSnapshot {
            config_path,
            additional_config_paths: Vec::new(),
            normalized_patterns,
            digest: Some(hasher.finalize().to_hex().to_string()),
            matched_files: Vec::new(),
            watch_paths: Vec::new(),
            observations: Vec::new(),
        }));
    }

    // Reject broad recursive directory dependencies before globbing. Cargo
    // fingerprints a directory recursively; injecting the crate root or a
    // filesystem root would turn every build into an unbounded tree walk.
    let watch_paths = match resolve_watch_paths(&crate_dir, &normalized) {
        Ok(paths) => paths,
        Err(error) if !strict_watches => {
            tracing::warn!("[key:{crate_name}] unsafe Cargo extra_inputs watch: {error:#}");
            Vec::new()
        }
        Err(error) => return Err(error),
    };
    if strict_watches {
        for watch in &watch_paths {
            inspect_safe_watch_tree(watch).with_context(|| {
                format!(
                    "checking active extra_inputs watch {} for byte-preserving enumeration",
                    watch.display()
                )
            })?;
        }
    }

    let mut hasher = blake3::Hasher::new();

    // (1) The declared pattern set itself — so editing `kache.toml` re-keys
    // even when it currently matches zero files.
    for pat in &normalized_patterns {
        hasher.update(b"extra_input_pattern:");
        hasher.update(pat.as_bytes());
        hasher.update(b"\x1f");
    }

    // (2) Enumerate the matched files on disk. A per-entry traversal error
    // (e.g. an unreadable subdir) must NOT silently shrink the matched set
    // into a false hit, so failing paths are folded as `glob_error` sentinels
    // — the same fail-safe stance as the per-file `unreadable` sentinel.
    let mut matched: Vec<PathBuf> = Vec::new();
    let mut glob_errors: Vec<String> = Vec::new();
    for normalized in &normalized {
        let pat = &normalized.glob;
        // An absolute pattern is used as-is; a relative one anchors at the
        // crate dir (whose literal bytes are escaped so a `[`/`?` in the path
        // can't be read as a glob metachar — the user's pattern is appended
        // raw).
        let full = if Path::new(pat).is_absolute() {
            pat.clone()
        } else {
            format!(
                "{}/{}",
                glob::Pattern::escape(&crate_dir.to_string_lossy()),
                pat
            )
        };
        // A recursive glob anchored at the filesystem root (`/**`) walks the
        // entire filesystem on every compile — almost never intended, and the
        // walk itself is the cost, so flag it before globbing.
        if walks_filesystem_root(&full) {
            tracing::warn!(
                "[key:{crate_name}] extra_inputs pattern {pat:?} walks from the filesystem \
                 root — this enumerates the entire filesystem on every compile; narrow it"
            );
        }
        let entries = match glob::glob(&full) {
            Ok(entries) => entries,
            Err(error) => {
                let rendered = error.to_string();
                strict_input_error(strict_watches, error).with_context(|| {
                    format!("parsing active extra_inputs glob {pat:?} for {crate_name}")
                })?;
                tracing::warn!("[key:{crate_name}] bad extra_inputs glob {pat:?}: {rendered}");
                continue;
            }
        };
        for entry in entries {
            match entry {
                Ok(p) if p.is_file() => matched.push(p),
                Ok(_) => {}
                Err(error) => {
                    let rel = crate_relative_path(&crate_dir, error.path());
                    let rendered = error.to_string();
                    strict_input_error(strict_watches, error).with_context(|| {
                        format!("enumerating active extra_inputs glob {pat:?} for {crate_name}")
                    })?;
                    tracing::warn!(
                        "[key:{crate_name}] extra_inputs enumeration error at {rel:?}: {rendered}"
                    );
                    glob_errors.push(rel);
                }
            }
        }
    }
    matched.sort();
    matched.dedup();

    // Recheck after globbing. A symlink inserted between the first preflight
    // and enumeration must not let `glob` follow an unbounded/non-UTF-8 tree
    // that was absent from the key's safety check.
    let observed_watch_dirs = if strict_watches {
        let mut directories = BTreeSet::new();
        for watch in &watch_paths {
            directories.extend(inspect_safe_watch_tree(watch).with_context(|| {
                format!(
                    "rechecking active extra_inputs watch {} after enumeration",
                    watch.display()
                )
            })?);
        }
        directories
    } else {
        BTreeSet::new()
    };

    // Empirical breadth guard: catches an over-broad glob regardless of shape
    // (an absolute `/**`, or a relative `**/*` that accidentally spans
    // `target/`). Over-folding is fail-safe, but it busts the key on every
    // change and re-walks a large tree each compile, so surface it.
    if should_warn_over_broad_file_count(matched.len()) {
        tracing::warn!(
            "[key:{crate_name}] extra_inputs matched {} files — likely an over-broad glob; \
             it busts the key on every change and walks a large tree each compile. Narrow it.",
            matched.len()
        );
    }

    // Warm the memoized hasher (daemon-backed) in one batch.
    let paths: Vec<&Path> = matched.iter().map(|p| p.as_path()).collect();
    file_hasher.prefetch(&paths);

    // (3) Fold each readable file as `<crate-relative path>=<content hash>`.
    // The PATH is part of the key, not just the content multiset, so swapping
    // two matched files' contents — or a content-following rename — re-keys.
    // That binding is load-bearing for the inputs this feature targets (sqlx
    // migration order, several `include_str!` sites under one glob): the same
    // bytes at a different filename compile differently. The path is
    // crate-relative with `/` separators, so a worktree move or cross-machine
    // restore is still stable. Unreadable files and enumeration errors fold
    // path-only sentinels under distinct labels that can never alias "absent"
    // or a readable file. All three lists are sorted so the fold order is
    // content-determined, never FS-order dependent.
    let mut readable: Vec<String> = Vec::new();
    let mut unreadable: Vec<String> = Vec::new();
    for path in &matched {
        let rel = crate_relative_path(&crate_dir, path);
        if strict_watches && rel.contains('\x1f') {
            anyhow::bail!(
                "active extra_inputs dependency {} contains the cache-key control separator; \
                 rename it before the declaration can be keyed unambiguously",
                path.display()
            );
        }
        match file_hasher.hash(path) {
            Ok(h) => readable.push(format!("{rel}={h}")),
            Err(error) => {
                let rendered = error.to_string();
                strict_input_error(strict_watches, error).with_context(|| {
                    format!(
                        "hashing active extra_inputs dependency {} for {crate_name}",
                        path.display()
                    )
                })?;
                tracing::warn!("[key:{crate_name}] extra_input unreadable {rel:?}: {rendered}");
                unreadable.push(rel);
            }
        }
    }
    readable.sort();
    unreadable.sort();
    glob_errors.sort();
    glob_errors.dedup();
    for entry in &readable {
        hasher.update(b"extra_input:");
        hasher.update(entry.as_bytes());
        hasher.update(b"\x1f");
    }
    for u in &unreadable {
        hasher.update(b"extra_input_unreadable:");
        hasher.update(u.as_bytes());
        hasher.update(b"\x1f");
    }
    for g in &glob_errors {
        hasher.update(b"extra_input_glob_error:");
        hasher.update(g.as_bytes());
        hasher.update(b"\x1f");
    }

    // The byte total is a debug-only convenience; don't pay a second `stat`
    // per matched file unless DEBUG is actually being recorded.
    if tracing::enabled!(tracing::Level::DEBUG) {
        let total_bytes: u64 = matched
            .iter()
            .filter_map(|p| std::fs::metadata(p).ok().map(|m| m.len()))
            .sum();
        tracing::debug!(
            "[key:{crate_name}] extra_inputs: {} pattern(s), {} file(s), {} unreadable, \
             {} glob-error(s), {} bytes",
            normalized.len(),
            readable.len(),
            unreadable.len(),
            glob_errors.len(),
            total_bytes
        );
    }

    // A single info!-level confirmation so a default-verbosity build shows the
    // feature is live for this crate (and `why-miss` guidance is actionable).
    tracing::info!(
        "[key:{crate_name}] extra_inputs: folded {} file(s) from {} pattern(s)",
        readable.len(),
        normalized.len()
    );

    let dependencies: BTreeSet<PathBuf> = std::iter::once(config_path.clone())
        .chain(matched.iter().cloned())
        .chain(watch_paths.iter().cloned())
        .collect();
    if strict_watches {
        for dependency in &dependencies {
            if let Some(symlink) = first_symlink_below_common(&crate_dir, dependency) {
                anyhow::bail!(
                    "active extra_inputs dependency {} crosses symlink {}; Cargo canonicalizes \
                     dep-info and cannot notice that link being retargeted safely",
                    dependency.display(),
                    symlink.display()
                );
            }
        }
    }
    let observation_paths: BTreeSet<PathBuf> = std::iter::once(config_path.clone())
        .chain(matched.iter().cloned())
        .chain(observed_watch_dirs)
        .collect();
    let observations = if strict_watches {
        observation_paths
            .iter()
            .map(|path| observe_dependency(path))
            .collect::<Result<Vec<_>>>()?
    } else {
        Vec::new()
    };

    Ok(Some(ExtraInputsSnapshot {
        config_path,
        additional_config_paths: Vec::new(),
        normalized_patterns,
        digest: Some(hasher.finalize().to_hex().to_string()),
        matched_files: matched,
        watch_paths,
        observations,
    }))
}

/// A matched file's path as a stable, crate-relative, `/`-separated string for
/// folding into the key. Crate-relative so a worktree move / cross-machine
/// restore doesn't change it; `/`-normalized so the same layout keys
/// identically across platforms. A path that isn't under `crate_dir` (only
/// reachable via a symlink the author placed inside the crate) falls back to
/// its lossy form — it still folds, it just isn't relocation-stable.
fn crate_relative_path(crate_dir: &Path, path: &Path) -> String {
    let rel = path.strip_prefix(crate_dir).unwrap_or(path);
    rel.components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

/// Expand (`$ENV`/`~`) a declared pattern, then reshape directory-style
/// patterns so they actually enumerate. Returns `None` (warn + skip) only for a
/// pattern carrying the fold separator — the one genuinely invalid case.
/// Out-of-crate patterns (absolute / `..`) are *folded*, with a portability
/// warning: reaching outside the crate is the author's explicit, fail-safe
/// choice, but it makes the key host-/layout-specific.
#[cfg(test)]
fn normalize_pattern(crate_name: &str, crate_dir: &Path, pattern: &str) -> Option<String> {
    normalize_pattern_info(crate_name, crate_dir, pattern).map(|pattern| pattern.glob)
}

fn normalize_pattern_info(
    crate_name: &str,
    crate_dir: &Path,
    pattern: &str,
) -> Option<NormalizedInputPattern> {
    let (normalized, unset_vars) = crate::config::expand_exclude_pattern_collecting(pattern);

    // An unset `$VAR` in a pattern is the one failure mode the rest of this
    // module handles loudly but this path used to swallow: the reference stays a
    // literal, matches nothing, and folds a pattern-set-only key that replays
    // regardless of the files the author meant to track. Warn so the missing
    // var is visible instead of presenting as a clean (but wrong) cache hit.
    if should_warn_unset_extra_input_vars(&unset_vars) {
        tracing::warn!(
            "[key:{crate_name}] extra_inputs pattern {pattern:?} references unset env var(s) \
             {unset_vars:?}; they stay literal and match nothing — set the var(s) or remove the \
             reference, otherwise this folds a replayable matches-nothing key"
        );
    }

    // A `\x1f` (the fold separator) in a glob is never legitimate and would let
    // a crafted pattern cross the pattern/hash section boundary in the digest.
    // Reject it rather than fold an ambiguous byte stream.
    if normalized.contains('\x1f') {
        tracing::warn!(
            "[key:{crate_name}] extra_inputs pattern {pattern:?} contains a control \
             separator (\\x1f); skipping"
        );
        return None;
    }

    // A pattern may deliberately reach outside the crate — an absolute path to a
    // machine-specific file, or `..` to a tree shared above the crate. That is
    // the author's explicit local choice and stays fail-safe (folding more
    // inputs can only cost an extra miss, never a wrong artifact); it is not
    // kache's place to forbid a real build dependency. But the key then becomes
    // host-/layout-specific, which reduces cross-machine and cross-worktree
    // cache sharing, so flag it rather than silently degrade portability.
    let as_path = Path::new(&normalized);
    if pattern_reaches_outside_crate(as_path) {
        tracing::warn!(
            "[key:{crate_name}] extra_inputs pattern {pattern:?} reaches outside the crate \
             (absolute or `..`); folding it anyway, but this crate's key is now \
             host-/layout-specific and won't share across machines or worktrees"
        );
    }

    // A bare or trailing-slash directory matches *nothing* under glob, which
    // would silently re-introduce a false hit. Reshape so the canonical
    // examples enumerate: `.sqlx/` and `.sqlx` → `.sqlx/**/*`.
    //
    // When the (de-slashed) pattern names a real on-disk directory it is a
    // LITERAL path, not a user-authored glob, so escape its metachars before
    // appending `/**/*`. Otherwise a directory literally named `data[1]` would
    // be read as a char class, enumerate nothing, and silently drop its files
    // (a false hit). Free-form globs (`.sqlx/**/*.json`) take the `else` arm
    // untouched, so the user's own `*`/`**`/`[…]` keep working.
    //
    // Matching is byte-literal (glob), so a pattern and an on-disk name that
    // differ only by Unicode normalization (NFC vs NFD) won't match. We do NOT
    // force-normalize the pattern: that can only break a match the author's
    // editor already aligned with the on-disk bytes.
    let trimmed = normalized.strip_suffix('/').unwrap_or(&normalized);
    let literal_path = anchor_input_path(crate_dir, Path::new(trimmed));
    let (glob, watch) = if literal_path.is_dir() {
        (
            format!("{}/**/*", glob::Pattern::escape(trimmed)),
            WatchIntent::DirectoryRoot(literal_path),
        )
    } else if normalized.ends_with('/') {
        (
            format!("{normalized}**/*"),
            WatchIntent::DirectoryRoot(literal_path),
        )
    } else if normalized.contains(['*', '?', '[']) {
        let root = literal_glob_root(&normalized);
        (
            normalized,
            WatchIntent::DirectoryRoot(anchor_input_path(crate_dir, &root)),
        )
    } else {
        (normalized, WatchIntent::Literal(literal_path))
    };
    Some(NormalizedInputPattern { glob, watch })
}

fn pattern_uses_dynamic_expansion(pattern: &str) -> bool {
    if pattern == "~" || pattern.starts_with("~/") {
        return true;
    }
    let mut chars = pattern.chars().peekable();
    while let Some(character) = chars.next() {
        if character == '$'
            && chars
                .peek()
                .is_some_and(|next| *next == '{' || *next == '_' || next.is_ascii_alphanumeric())
        {
            return true;
        }
    }
    false
}

#[cfg(any(windows, test))]
fn windows_root_shape_is_ambiguous(is_absolute: bool, has_root: bool, has_prefix: bool) -> bool {
    !is_absolute && (has_root || has_prefix)
}

#[cfg(windows)]
fn windows_pattern_has_ambiguous_root(path: &Path) -> bool {
    windows_root_shape_is_ambiguous(
        path.is_absolute(),
        path.has_root(),
        matches!(path.components().next(), Some(Component::Prefix(_))),
    )
}

#[cfg(not(windows))]
fn windows_pattern_has_ambiguous_root(_path: &Path) -> bool {
    false
}

#[cfg(any(windows, test))]
fn windows_prefix_uses_device_namespace(prefix: std::path::Prefix<'_>) -> bool {
    use std::path::Prefix;
    matches!(
        prefix,
        Prefix::Verbatim(_)
            | Prefix::VerbatimUNC(_, _)
            | Prefix::VerbatimDisk(_)
            | Prefix::DeviceNS(_)
    )
}

#[cfg(windows)]
fn windows_path_uses_device_namespace(path: &Path) -> bool {
    matches!(
        path.components().next(),
        Some(Component::Prefix(prefix)) if windows_prefix_uses_device_namespace(prefix.kind())
    )
}

#[cfg(not(windows))]
fn windows_path_uses_device_namespace(_path: &Path) -> bool {
    false
}

fn parent_traversal_follows_glob(pattern: &str) -> bool {
    let mut saw_glob = false;
    for component in Path::new(pattern).components() {
        match component {
            Component::ParentDir if saw_glob => return true,
            Component::Normal(text) => {
                let text = text.to_string_lossy();
                saw_glob |= text.contains(['*', '?', '[']);
            }
            _ => {}
        }
    }
    false
}

/// Literal directory prefix before the first component carrying glob syntax.
/// A pattern such as `migrations/**/*.sql` yields `migrations`; `**/*.sql`
/// yields an empty relative path, which anchors to the crate root and is
/// rejected before enumeration.
fn literal_glob_root(pattern: &str) -> PathBuf {
    let mut root = PathBuf::new();
    for component in Path::new(pattern).components() {
        let text = component.as_os_str().to_string_lossy();
        if text.contains(['*', '?', '[']) {
            break;
        }
        root.push(component.as_os_str());
    }
    root
}

fn anchor_input_path(crate_dir: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        lexical_normalize(path)
    } else {
        lexical_normalize(&crate_dir.join(path))
    }
}

/// Normalize `.`/`..` without requiring the path to exist or resolving
/// symlinks. Missing literal inputs still need a stable parent watch.
fn lexical_normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() && !normalized.has_root() {
                    normalized.push(component.as_os_str());
                }
            }
            _ => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

/// Find a user-controlled symlink component between the crate and one of its
/// declared dependencies. Cargo canonicalizes dep-info paths, so watching only
/// the symlink target would miss a later retarget of the link itself.
fn first_symlink_below_common(crate_dir: &Path, dependency: &Path) -> Option<PathBuf> {
    let crate_components: Vec<_> = crate_dir.components().collect();
    let dependency_components: Vec<_> = dependency.components().collect();
    let common_len = crate_components
        .iter()
        .zip(&dependency_components)
        .take_while(|(left, right)| left == right)
        .count();
    let mut probe = PathBuf::new();
    for component in &dependency_components[..common_len] {
        probe.push(component.as_os_str());
    }
    for component in &dependency_components[common_len..] {
        probe.push(component.as_os_str());
        if std::fs::symlink_metadata(&probe).is_ok_and(|metadata| metadata.file_type().is_symlink())
        {
            return Some(probe);
        }
    }
    None
}

/// `glob` works through UTF-8 patterns and silently omits non-UTF-8 directory
/// entries on Unix. Preflight each bounded watch tree with `read_dir`, which
/// preserves `OsStr`, and fail closed instead of keying an incomplete set.
fn inspect_safe_watch_tree(root: &Path) -> Result<Vec<PathBuf>> {
    let mut pending = vec![root.to_path_buf()];
    let mut directories = Vec::new();
    while let Some(directory) = pending.pop() {
        directories.push(directory.clone());
        for entry in std::fs::read_dir(&directory)
            .with_context(|| format!("reading directory {}", directory.display()))?
        {
            let entry =
                entry.with_context(|| format!("reading an entry under {}", directory.display()))?;
            if entry.file_name().to_str().is_none() {
                anyhow::bail!(
                    "directory {} contains a non-UTF-8 name that the configured glob cannot enumerate safely",
                    directory.display()
                );
            }
            let file_type = entry
                .file_type()
                .with_context(|| format!("reading file type for {}", entry.path().display()))?;
            if file_type.is_symlink() {
                anyhow::bail!(
                    "directory {} contains symlink {} that Cargo/glob cannot follow with bounded, byte-preserving freshness semantics",
                    directory.display(),
                    entry.path().display()
                );
            }
            if file_type.is_dir() {
                pending.push(entry.path());
            }
        }
    }
    directories.sort();
    directories.dedup();
    Ok(directories)
}

fn nearest_existing_directory(start: &Path) -> Option<PathBuf> {
    let mut candidate = Some(start);
    while let Some(path) = candidate {
        if path.is_dir() {
            return Some(lexical_normalize(path));
        }
        candidate = path.parent();
    }
    None
}

fn is_filesystem_root(path: &Path) -> bool {
    path.has_root() && path.parent().is_none()
}

fn any_broad_watch_condition(conditions: [bool; 4]) -> bool {
    conditions.into_iter().any(|condition| condition)
}

fn resolve_watch_paths(
    crate_dir: &Path,
    patterns: &[NormalizedInputPattern],
) -> Result<Vec<PathBuf>> {
    let crate_dir = lexical_normalize(crate_dir);
    let canonical_crate_dir =
        std::fs::canonicalize(&crate_dir).unwrap_or_else(|_| crate_dir.clone());
    let mut watches = BTreeSet::new();

    for pattern in patterns {
        // Invalid globs retain their existing digest semantics and are skipped
        // during enumeration; they do not justify a broad directory watch.
        if glob::Pattern::new(&pattern.glob).is_err() {
            continue;
        }
        let start = match &pattern.watch {
            WatchIntent::Literal(path) if path.is_file() => continue,
            WatchIntent::Literal(path) => path.parent(),
            WatchIntent::DirectoryRoot(path) => Some(path.as_path()),
        };
        let Some(start) = start else {
            anyhow::bail!(
                "extra_inputs pattern {:?} has no directory that Cargo can watch; \
                 place the input under a narrow existing directory",
                pattern.glob
            );
        };
        let Some(watch) = nearest_existing_directory(start) else {
            anyhow::bail!(
                "extra_inputs pattern {:?} has no existing directory to watch; \
                 create a narrow parent directory first",
                pattern.glob
            );
        };

        // Watching the crate root, an ancestor of it, or the filesystem root
        // makes Cargo recursively fingerprint an unrelated/unbounded tree.
        // Check the resolved directory too: an in-crate symlink to `/` must
        // not bypass the lexical guard and hand Cargo a filesystem-root scan.
        let canonical_watch = std::fs::canonicalize(&watch).unwrap_or_else(|_| watch.clone());
        if any_broad_watch_condition([
            is_filesystem_root(&watch),
            is_filesystem_root(&canonical_watch),
            crate_dir.starts_with(&watch),
            canonical_crate_dir.starts_with(&canonical_watch),
        ]) {
            anyhow::bail!(
                "extra_inputs pattern {:?} would make Cargo recursively watch broad directory {} \
                 (the crate root or an ancestor); add a literal subdirectory to the pattern, or \
                 create a narrow parent directory for a missing literal input",
                pattern.glob,
                watch.display()
            );
        }

        watches.insert(watch);
        if watches.len() > MAX_WATCH_PATHS {
            anyhow::bail!(
                "extra_inputs resolves to more than {MAX_WATCH_PATHS} directory watches; \
                 consolidate patterns under a smaller set of narrow literal roots"
            );
        }
    }

    Ok(watches.into_iter().collect())
}

/// True if a glob's literal prefix (the part before the first glob
/// metacharacter) is the filesystem root, so a following `**` would walk the
/// entire filesystem. Detects the `/**` footgun cheaply, before the slow walk.
fn walks_filesystem_root(glob_pattern: &str) -> bool {
    let literal_end = glob_pattern
        .find(['*', '?', '['])
        .unwrap_or(glob_pattern.len());
    // The directory glob starts walking = the literal prefix up to its last
    // separator. No separator → a bare relative stem, never the FS root.
    let Some(slash) = glob_pattern[..literal_end].rfind('/') else {
        return false;
    };
    let base = Path::new(&glob_pattern[..=slash]);
    // Unix: only absolute paths are rooted. Windows: `Path::is_absolute()` is
    // false for a bare "/" (it expects a drive), but "/" is still the
    // current-drive root and walks a huge tree — treat a leading RootDir as
    // rooted there. Keeping the RootDir arm behind `cfg(windows)` makes the
    // Linux mutation lane observe a single killable absolute check instead of
    // an equivalent `||` / `&&` pair.
    let rooted = path_base_is_rooted(base);
    rooted && base.parent().is_none()
}

fn path_base_is_rooted(base: &Path) -> bool {
    if base.is_absolute() {
        return true;
    }
    #[cfg(windows)]
    {
        matches!(
            base.components().next(),
            Some(std::path::Component::RootDir)
        )
    }
    #[cfg(not(windows))]
    {
        false
    }
}

#[derive(Debug, Clone, Copy)]
struct FirstMakeRule {
    colon: usize,
    insertion: usize,
}

fn merge_snapshot_into_dep_info(
    snapshot: &ExtraInputsSnapshot,
    dep_info_path: &Path,
) -> Result<()> {
    let metadata = std::fs::metadata(dep_info_path).with_context(|| {
        format!(
            "reading required consumer dep-info {} for active extra_inputs declaration",
            dep_info_path.display()
        )
    })?;
    if !metadata.is_file() {
        anyhow::bail!(
            "required consumer dep-info {} is not a regular file; refusing to return with an \
             incomplete Cargo fingerprint for active extra_inputs",
            dep_info_path.display()
        );
    }
    let original = std::fs::read_to_string(dep_info_path).with_context(|| {
        format!(
            "reading required consumer dep-info {} for active extra_inputs declaration",
            dep_info_path.display()
        )
    })?;
    let updated = merge_snapshot_dep_info_content(snapshot, &original).with_context(|| {
        format!(
            "malformed consumer dep-info {} for active extra_inputs declaration",
            dep_info_path.display()
        )
    })?;
    if updated == original {
        return Ok(());
    }
    crate::atomic::atomic_replace(dep_info_path, updated.as_bytes()).with_context(|| {
        format!(
            "atomically updating consumer dep-info {} for active extra_inputs declaration",
            dep_info_path.display()
        )
    })
}

fn merge_snapshot_dep_info_content(
    snapshot: &ExtraInputsSnapshot,
    original: &str,
) -> Result<String> {
    tracing::trace!(
        "merging extra_inputs dep-info: {} normalized pattern(s), {} matched file(s), {} watch path(s)",
        snapshot.normalized_patterns.len(),
        snapshot.matched_files.len(),
        snapshot.watch_paths.len()
    );
    let rule = first_make_dependency_rule(original)?;
    let rule_tail = &original[rule.colon..rule.insertion];
    let dependencies = rule_tail
        .strip_prefix(':')
        .expect("first_make_dependency_rule points at a colon");
    let existing_words = parse_make_words(dependencies).context("parsing Cargo dependency rule")?;
    let compiler_cwd = std::env::current_dir()
        .context("resolving compiler working directory for extra_inputs dep-info")?;
    let existing: BTreeSet<PathBuf> = existing_words
        .iter()
        .map(|word| anchor_input_path(&compiler_cwd, Path::new(word)))
        .collect();

    let mut dependency_paths = BTreeSet::new();
    dependency_paths.insert(lexical_normalize(&snapshot.config_path));
    dependency_paths.extend(
        snapshot
            .additional_config_paths
            .iter()
            .map(|path| lexical_normalize(path)),
    );
    dependency_paths.extend(
        snapshot
            .matched_files
            .iter()
            .map(|path| lexical_normalize(path)),
    );
    dependency_paths.extend(
        snapshot
            .watch_paths
            .iter()
            .map(|path| lexical_normalize(path)),
    );

    let mut additions = Vec::new();
    for path in dependency_paths {
        if existing.contains(&path) {
            continue;
        }
        // Cargo interprets rustc dep-info paths relative to rustc's working
        // directory, which is normally the workspace root rather than the
        // member crate. Absolute consumer paths are unambiguous and are added
        // only after cache storage/restoration, so they never leak producer
        // worktree paths into cached blobs.
        let text = path.to_str().ok_or_else(|| {
            anyhow::anyhow!(
                "extra_inputs dependency {} is not valid UTF-8 and cannot be represented safely \
                 in Cargo dep-info",
                path.display()
            )
        })?;
        additions.push(make_escape_word(text)?);
    }
    additions.sort();
    additions.dedup();
    if additions.is_empty() {
        return Ok(original.to_string());
    }

    let mut updated = String::with_capacity(
        original.len() + additions.iter().map(String::len).sum::<usize>() + additions.len(),
    );
    updated.push_str(&original[..rule.insertion]);
    updated.push(' ');
    updated.push_str(&additions.join(" "));
    updated.push_str(&original[rule.insertion..]);

    Ok(updated)
}

/// Locate the first line Cargo will parse as rustc dep-info. Cargo recognizes
/// the first line containing `: `; drive-letter colons and `#` remain literal.
fn first_make_dependency_rule(input: &str) -> Result<FirstMakeRule> {
    let mut offset = 0usize;
    for line_with_newline in input.split_inclusive('\n') {
        let line = line_with_newline
            .strip_suffix('\n')
            .unwrap_or(line_with_newline);
        let line = line.strip_suffix('\r').unwrap_or(line);
        // Cargo handles rustc's environment records before looking for the
        // dependency rule. An env value may itself contain `: ` and must not
        // be mistaken for the Make target/dependency separator.
        if line.starts_with("# env-dep:") {
            offset += line_with_newline.len();
            continue;
        }
        if let Some(relative_colon) = line.find(": ") {
            let colon = offset + relative_colon;
            let dependency_start = relative_colon + 2;
            let trimmed_dependencies = line[dependency_start..].trim_end_matches([' ', '\t']);
            let insertion = offset + dependency_start + trimmed_dependencies.len();
            return Ok(FirstMakeRule { colon, insertion });
        }
        offset += line_with_newline.len();
    }

    anyhow::bail!("dep-info contains no Make dependency rule")
}

/// Parse dependency words exactly as Cargo 0.98 / Rust 1.97 does: split on
/// whitespace, then join the next token while the current token ends in `\\`.
fn parse_make_words(input: &str) -> Result<Vec<String>> {
    let mut words = Vec::new();
    let mut tokens = input.split_whitespace();
    while let Some(token) = tokens.next() {
        let mut word = token.to_string();
        while word.ends_with('\\') {
            word.pop();
            word.push(' ');
            word.push_str(
                tokens
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("malformed dep-info format, trailing \\"))?,
            );
        }
        words.push(word);
    }
    Ok(words)
}

/// Parse the dependency side of the same rustc dep-info rule Cargo consumes.
///
/// Keep cache-restore validation on the exact grammar used by
/// [`merge_snapshot_dep_info_content`]: Cargo skips leading metadata records
/// and consumes the first line containing `: `.
pub(crate) fn parse_dep_info_dependencies(input: &str) -> Result<Vec<PathBuf>> {
    let rule = first_make_dependency_rule(input)?;
    parse_make_words(&input[rule.colon + 1..rule.insertion])
        .map(|words| words.into_iter().map(PathBuf::from).collect())
}

fn make_escape_word(input: &str) -> Result<String> {
    if input
        .chars()
        .any(|character| character.is_whitespace() && character != ' ')
    {
        anyhow::bail!(
            "extra_inputs dependency contains unsupported whitespace and cannot enter Cargo dep-info"
        );
    }
    if input.ends_with([' ', '\\']) {
        anyhow::bail!(
            "extra_inputs dependency ends in a space or backslash, which Cargo dep-info cannot represent"
        );
    }
    Ok(input.replace(' ', "\\ "))
}

/// Deterministic opaque digest for an unreadable / unparseable `kache.toml`:
/// the build re-keys on any edit and never aliases "no file present".
fn unparseable_digest(crate_name: &str, config_path: &Path, raw: &[u8]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"extra_inputs_unparseable:");
    hasher.update(raw);
    tracing::debug!(
        "[key:{crate_name}] folding {} as opaque (unparseable)",
        config_path.display()
    );
    hasher.finalize().to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal crate dir: `Cargo.toml`, `src/lib.rs`, and any
    /// (relative path, contents) files listed. Returns (tempdir, src_path).
    fn crate_fixture(files: &[(&str, &str)]) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("Cargo.toml"), "[package]\nname = \"x\"\n").unwrap();
        std::fs::create_dir_all(root.join("src")).unwrap();
        let src = root.join("src/lib.rs");
        std::fs::write(&src, "// crate\n").unwrap();
        for (rel, contents) in files {
            let p = root.join(rel);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(p, contents).unwrap();
        }
        (dir, src)
    }

    fn dig(src: &Path) -> Option<String> {
        let fh = FileHasher::new();
        digest(Some(src), "x", true, &fh)
    }

    #[test]
    fn strict_and_legacy_error_policies_are_complementary() {
        let missing = std::io::Error::from(std::io::ErrorKind::NotFound);
        let denied = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
        assert!(io_error_is_not_found(&missing));
        assert!(!io_error_is_not_found(&denied));

        assert!(legacy_digest_mode(false));
        assert!(!legacy_digest_mode(true));
        assert_eq!(strict_input_error(false, "legacy-error"), Ok(()));
        assert_eq!(
            strict_input_error(true, "strict-error"),
            Err("strict-error")
        );
    }

    #[test]
    fn file_presence_helpers_distinguish_missing_paths_from_other_io_errors() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("regular-file");
        std::fs::write(&file, b"contents").unwrap();
        let missing = dir.path().join("missing");
        // Platform filesystems disagree on whether `file/child` is
        // NotFound or NotADirectory. An embedded NUL is rejected before the
        // filesystem lookup everywhere, so it exercises the non-NotFound
        // branch portably.
        let invalid = dir.path().join("invalid\0path");

        assert!(
            symlink_metadata_if_present(&file)
                .unwrap()
                .expect("existing file has metadata")
                .is_file()
        );
        assert!(symlink_metadata_if_present(&missing).unwrap().is_none());
        assert_ne!(
            symlink_metadata_if_present(&invalid).unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );

        assert_eq!(
            read_file_if_present(&file).unwrap(),
            Some(b"contents".to_vec())
        );
        assert!(read_file_if_present(&missing).unwrap().is_none());
        assert_ne!(
            read_file_if_present(&invalid).unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );
    }

    #[test]
    fn missing_config_is_a_strict_snapshot_noop() {
        let (_dir, src) = crate_fixture(&[]);
        assert!(
            ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn config_read_errors_preserve_the_strict_legacy_boundary() {
        let (dir, src) = crate_fixture(&[]);
        std::fs::create_dir(dir.path().join("kache.toml")).unwrap();

        let strict = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new());
        assert!(strict.is_err(), "strict resolution must fail closed");
        assert!(
            resolve_snapshot(Some(&src), "x", true, &FileHasher::new(), false)
                .unwrap()
                .is_none(),
            "the compatibility digest keeps its unreadable-config no-op"
        );
    }

    #[test]
    fn strict_snapshot_rejects_non_utf8_config() {
        let (dir, src) = crate_fixture(&[]);
        std::fs::write(dir.path().join("kache.toml"), b"\xff\xfe extra_inputs").unwrap();
        let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new())
            .expect_err("strict active declarations must be UTF-8");
        assert!(format!("{error:#}").contains("valid UTF-8"), "{error:#}");
    }

    #[test]
    fn strict_snapshot_rejects_invalid_glob_syntax() {
        let (_dir, src) = crate_fixture(&[("kache.toml", "extra_inputs = [\"data/[bad\"]")]);
        let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new())
            .expect_err("strict active declarations must reject invalid globs");
        assert!(
            format!("{error:#}").contains("parsing active extra_inputs glob"),
            "{error:#}"
        );
    }

    #[test]
    fn pattern_safety_helpers_cover_both_sides_of_each_boundary() {
        for pattern in ["~", "~/data", "${DATA}/x", "$_DATA/x", "$DATA/x"] {
            assert!(pattern_uses_dynamic_expansion(pattern), "{pattern:?}");
        }
        for pattern in ["$", "$-", "cash$-value", "path/~"] {
            assert!(!pattern_uses_dynamic_expansion(pattern), "{pattern:?}");
        }

        assert!(!parent_traversal_follows_glob("../shared/*.json"));
        assert!(!parent_traversal_follows_glob("data/../shared/*.json"));
        assert!(parent_traversal_follows_glob("data/*/../shared.json"));

        assert_eq!(
            lexical_normalize(Path::new("./x")).as_os_str(),
            std::ffi::OsStr::new("x")
        );
        assert_eq!(
            lexical_normalize(Path::new("foo/../bar")),
            PathBuf::from("bar")
        );
        assert_eq!(lexical_normalize(Path::new("foo/..")), PathBuf::new());
        assert_eq!(lexical_normalize(Path::new("../x")), PathBuf::from("../x"));

        assert!(is_filesystem_root(Path::new(std::path::MAIN_SEPARATOR_STR)));
        assert!(!is_filesystem_root(Path::new("not-a-root")));
        assert!(!any_broad_watch_condition([false; 4]));
        for index in 0..4 {
            let mut conditions = [false; 4];
            conditions[index] = true;
            assert!(any_broad_watch_condition(conditions));
        }

        for (is_absolute, has_root, has_prefix, expected) in [
            (false, false, false, false),
            (false, false, true, true),
            (false, true, false, true),
            (false, true, true, true),
            (true, false, false, false),
            (true, false, true, false),
            (true, true, false, false),
            (true, true, true, false),
        ] {
            assert_eq!(
                windows_root_shape_is_ambiguous(is_absolute, has_root, has_prefix),
                expected
            );
        }

        use std::path::Prefix;
        let server = std::ffi::OsStr::new("server");
        let share = std::ffi::OsStr::new("share");
        for prefix in [
            Prefix::Verbatim(server),
            Prefix::VerbatimUNC(server, share),
            Prefix::VerbatimDisk(b'C'),
            Prefix::DeviceNS(server),
        ] {
            assert!(windows_prefix_uses_device_namespace(prefix));
        }
        for prefix in [Prefix::Disk(b'C'), Prefix::UNC(server, share)] {
            assert!(!windows_prefix_uses_device_namespace(prefix));
        }
    }

    #[cfg(not(windows))]
    #[test]
    fn windows_only_path_rejections_stay_disabled_on_other_platforms() {
        assert!(!windows_pattern_has_ambiguous_root(Path::new(
            r"C:\relative"
        )));
        assert!(!windows_path_uses_device_namespace(Path::new(
            r"\\?\C:\repo"
        )));
    }

    #[test]
    fn no_colocated_file_is_noop() {
        let (_d, src) = crate_fixture(&[]);
        assert_eq!(dig(&src), None);
        assert!(!declared(Some(&src)));
    }

    #[test]
    fn colocated_file_disables_early_adaptation_even_when_empty() {
        let (_d, src) = crate_fixture(&[("kache.toml", "extra_inputs = []")]);
        assert!(declared(Some(&src)));
    }

    #[test]
    fn non_primary_is_noop() {
        let (_d, src) = crate_fixture(&[
            ("kache.toml", "extra_inputs = [\".sqlx/**/*.json\"]"),
            (".sqlx/q.json", "v1"),
        ]);
        let fh = FileHasher::new();
        assert_eq!(digest(Some(&src), "x", false, &fh), None);
    }

    #[test]
    fn empty_list_is_noop() {
        let (_d, src) = crate_fixture(&[("kache.toml", "extra_inputs = []")]);
        assert_eq!(dig(&src), None);
    }

    #[test]
    fn declared_input_change_rekeys() {
        let (d, src) = crate_fixture(&[
            ("kache.toml", "extra_inputs = [\".sqlx/**/*.json\"]"),
            (".sqlx/q.json", "v1"),
        ]);
        let before = dig(&src).expect("declared input folds a digest");

        // Editing the declared file must move the digest.
        std::fs::write(d.path().join(".sqlx/q.json"), "v2").unwrap();
        let after = dig(&src).expect("still folds after edit");
        assert_ne!(before, after);

        // Re-reading without changes is deterministic.
        assert_eq!(after, dig(&src).unwrap());
    }

    #[test]
    fn zero_match_still_folds_pattern_set() {
        // No matching files, but the declared pattern is folded so editing
        // the pattern set re-keys — and it is distinct from "no file".
        let (_d, src) = crate_fixture(&[("kache.toml", "extra_inputs = [\".sqlx/**/*.json\"]")]);
        let only_pattern = dig(&src).expect("pattern set folds even at zero matches");

        let (_d2, src2) = crate_fixture(&[("kache.toml", "extra_inputs = [\"other/**/*.sql\"]")]);
        let other_pattern = dig(&src2).unwrap();
        assert_ne!(only_pattern, other_pattern);
    }

    #[test]
    fn dir_shaped_patterns_are_equivalent() {
        // `.sqlx/`, `.sqlx`, and `.sqlx/**/*` must enumerate the same set.
        let (d, _src) = crate_fixture(&[(".sqlx/q.json", "v1")]);
        let root = d.path();
        let p1 = normalize_pattern("x", root, ".sqlx/").unwrap();
        let p2 = normalize_pattern("x", root, ".sqlx").unwrap();
        let p3 = normalize_pattern("x", root, ".sqlx/**/*").unwrap();
        assert_eq!(p1, p3);
        assert_eq!(p2, p3);
    }

    #[test]
    fn trailing_slash_on_missing_dir_appends_recursive_glob() {
        // A trailing-slash pattern whose de-slashed form is NOT a real on-disk
        // directory takes the plain `{pattern}**/*` reshape (not the literal-dir
        // escape). Covers normalize_pattern's `else if ends_with('/')` arm.
        let (d, _src) = crate_fixture(&[]);
        let root = d.path();
        let reshaped = normalize_pattern("x", root, "ghostdir/").unwrap();
        assert_eq!(reshaped, "ghostdir/**/*");
    }

    #[test]
    fn unset_env_var_pattern_folds_as_literal() {
        // A pattern referencing an unset $VAR stays literal (matches nothing) and
        // is folded with a warning rather than dropped. Covers the unset-var arm.
        let (d, _src) = crate_fixture(&[]);
        let root = d.path();
        let reshaped =
            normalize_pattern("x", root, "$KACHE_DEFINITELY_UNSET_XYZ/data.json").unwrap();
        // The unexpanded literal survives into the folded pattern.
        assert!(
            reshaped.contains("$KACHE_DEFINITELY_UNSET_XYZ"),
            "unset var should stay literal: {reshaped}"
        );
    }

    #[test]
    fn out_of_crate_patterns_are_folded_not_rejected() {
        // Reaching outside the crate (absolute / `..`) is the author's explicit,
        // fail-safe choice — folded (with a portability warning), not skipped.
        let (d, _src) = crate_fixture(&[]);
        let root = d.path();
        assert!(normalize_pattern("x", root, "../shared/**").is_some());
        assert!(normalize_pattern("x", root, "/etc/**").is_some());
        assert!(normalize_pattern("x", root, ".sqlx/**/*.json").is_some());
        // The one genuinely-invalid case stays rejected: the fold separator,
        // which could cross the pattern/hash section boundary in the digest.
        assert!(normalize_pattern("x", root, "\u{1f}bad").is_none());
    }

    #[test]
    fn absolute_external_input_folds_and_rekeys() {
        // A pattern may point at a file outside the crate (here a sibling
        // tempdir standing in for a machine-specific path). Its content is
        // folded and an edit re-keys — the key is (correctly) host-specific.
        let ext = tempfile::tempdir().unwrap();
        let ext_file = ext.path().join("shared.json");
        std::fs::write(&ext_file, "v1").unwrap();

        // Forward slashes: backslashes are escape sequences in TOML strings (a
        // raw Windows path would be mis-parsed), and Windows path resolution
        // accepts `/` just fine.
        let toml = format!(
            "extra_inputs = [\"{}\"]",
            ext_file.display().to_string().replace('\\', "/")
        );
        let (_d, src) = crate_fixture(&[("kache.toml", toml.as_str())]);
        let before = dig(&src).expect("absolute external input folds");
        std::fs::write(&ext_file, "v2").unwrap();
        let after = dig(&src).expect("still folds after edit");
        assert_ne!(
            before, after,
            "editing an external declared input must re-key"
        );
    }

    #[test]
    fn walks_filesystem_root_detects_root_globs() {
        assert!(walks_filesystem_root("/**"));
        assert!(walks_filesystem_root("/**/*.json"));
        assert!(!walks_filesystem_root("/usr/**"));
        assert!(!walks_filesystem_root("/home/me/proto/**/*.proto"));
        assert!(!walks_filesystem_root("proto/**/*.proto")); // relative, crate-anchored
    }

    #[test]
    fn sibling_crate_without_file_is_unaffected() {
        // One crate declares inputs; a sibling without a kache.toml folds
        // nothing — scoping is per crate.
        let (_d1, src1) = crate_fixture(&[
            ("kache.toml", "extra_inputs = [\".sqlx/**/*.json\"]"),
            (".sqlx/q.json", "v1"),
        ]);
        let (_d2, src2) = crate_fixture(&[(".sqlx/q.json", "v1")]);
        assert!(dig(&src1).is_some());
        assert_eq!(dig(&src2), None);
    }

    #[test]
    fn relocation_is_stable() {
        // Two crates with byte-identical declared inputs at different paths
        // must produce the same digest (content-hash folding, not paths) —
        // this is what survives a worktree move / cross-machine restore.
        let files = &[
            ("kache.toml", "extra_inputs = [\".sqlx/**/*.json\"]"),
            (".sqlx/q.json", "v1"),
        ];
        let (_d1, src1) = crate_fixture(files);
        let (_d2, src2) = crate_fixture(files);
        assert_eq!(dig(&src1), dig(&src2));
        assert!(dig(&src1).is_some());
    }

    fn snap(src: &Path) -> ExtraInputsSnapshot {
        let file_hasher = FileHasher::new();
        ExtraInputsSnapshot::resolve(Some(src), "x", true, &file_hasher)
            .expect("snapshot resolution succeeds")
            .expect("fixture has an active declaration")
    }

    struct EnvVarGuard {
        name: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            // SAFETY: workspace snapshot tests hold config_path_lock for the
            // guard's lifetime, matching config.rs's own environment tests.
            unsafe {
                match self.previous.take() {
                    Some(previous) => std::env::set_var(self.name, previous),
                    None => std::env::remove_var(self.name),
                }
            }
        }
    }

    fn pin_env_path(name: &'static str, path: &Path) -> EnvVarGuard {
        let previous = std::env::var_os(name);
        // SAFETY: caller holds config_path_lock until EnvVarGuard drops.
        unsafe { std::env::set_var(name, path) };
        EnvVarGuard { name, previous }
    }

    fn pin_config(path: &Path) -> EnvVarGuard {
        pin_env_path("KACHE_CONFIG", path)
    }

    fn write_workspace_package(root: &Path, relative: &str, manifest: &str) -> PathBuf {
        let package = root.join(relative);
        std::fs::create_dir_all(package.join("src")).unwrap();
        std::fs::write(package.join("Cargo.toml"), manifest).unwrap();
        let source = package.join("src/lib.rs");
        std::fs::write(&source, "pub fn marker() {}\n").unwrap();
        source
    }

    fn workspace_fixture(member_glob: bool) -> (tempfile::TempDir, PathBuf, PathBuf, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let prefix = if member_glob { "crates/" } else { "" };
        let members = if member_glob {
            "members = [\"crates/*\"]"
        } else {
            "members = [\"macro-provider\", \"consumer\", \"unlisted\"]"
        };
        std::fs::write(
            root.join("Cargo.toml"),
            format!("[workspace]\n{members}\nresolver = \"2\"\n"),
        )
        .unwrap();
        std::fs::create_dir_all(root.join("shared")).unwrap();
        std::fs::write(root.join("shared/value.txt"), "alpha").unwrap();
        std::fs::write(
            root.join(".kache.toml"),
            r#"[[workspace.extra_inputs]]
crates = ["macro-provider"]
inputs = ["shared/value.txt"]
propagate_to_dependents = true
"#,
        )
        .unwrap();
        let provider = write_workspace_package(
            root,
            &format!("{prefix}macro-provider"),
            r#"[package]
name = "macro-provider"
version = "0.1.0"
edition = "2021"

[lib]
proc-macro = true
"#,
        );
        let consumer = write_workspace_package(
            root,
            &format!("{prefix}consumer"),
            r#"[package]
name = "consumer"
version = "0.1.0"
edition = "2021"
"#,
        );
        let unlisted = write_workspace_package(
            root,
            &format!("{prefix}unlisted"),
            r#"[package]
name = "unlisted"
version = "0.1.0"
edition = "2021"
"#,
        );
        (dir, provider, consumer, unlisted)
    }

    fn rustc_args(
        source: &Path,
        crate_name: &str,
        external: Option<&str>,
    ) -> crate::args::RustcArgs {
        let mut raw = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            crate_name.to_string(),
            "--crate-type".to_string(),
            "lib".to_string(),
            "--emit=dep-info,metadata,link".to_string(),
            "--out-dir".to_string(),
            "/external-target/debug/deps".to_string(),
            source.display().to_string(),
        ];
        if let Some(external) = external {
            raw.extend(["--extern".to_string(), external.to_string()]);
        }
        crate::args::RustcArgs::parse(&raw).unwrap()
    }

    fn cargo_extern_with_provider_provenance(
        workspace: &Path,
        package: &str,
        crate_name: &str,
        alias: &str,
    ) -> String {
        let deps = workspace.join("fake-target/debug/deps");
        std::fs::create_dir_all(&deps).unwrap();
        let artifact = deps.join(format!("lib{crate_name}-12345678.rlib"));
        std::fs::write(&artifact, "artifact").unwrap();
        let manifest = workspace.join(package).join("Cargo.toml");
        let source = workspace.join(package).join("src/lib.rs");
        let dep_info = deps.join(format!("{crate_name}-12345678.d"));
        std::fs::write(
            &dep_info,
            format!(
                "{}: {} {}\n",
                make_escape_word(&artifact.to_string_lossy()).unwrap(),
                make_escape_word(&manifest.to_string_lossy()).unwrap(),
                make_escape_word(&source.to_string_lossy()).unwrap(),
            ),
        )
        .unwrap();
        format!("{alias}={}", artifact.display())
    }

    fn lexical_relative_path(from: &Path, to: &Path) -> PathBuf {
        let from: Vec<_> = from.components().collect();
        let to: Vec<_> = to.components().collect();
        let common = from
            .iter()
            .zip(&to)
            .take_while(|(left, right)| left == right)
            .count();
        let mut relative = PathBuf::new();
        for _ in &from[common..] {
            relative.push("..");
        }
        for component in &to[common..] {
            relative.push(component.as_os_str());
        }
        relative
    }

    #[test]
    fn cargo_artifact_identity_requires_an_eight_character_hex_hash() {
        let target = PathBuf::from("target");
        let valid = cargo_artifact_identity(&target.join("libprovider-12345678.rlib"))
            .expect("eight hex characters form a Cargo artifact hash");
        assert_eq!(valid.crate_name, "provider");
        assert_eq!(valid.dep_info_path, target.join("provider-12345678.d"));
        assert!(cargo_artifact_identity(&target.join("libprovider-1234567.rlib")).is_none());
        assert!(cargo_artifact_identity(&target.join("libprovider-1234567g.rlib")).is_none());
    }

    #[test]
    fn workspace_provider_and_aliased_direct_consumer_share_digest_and_dependencies() {
        let _lock = crate::config::config_path_lock();
        let (dir, provider, consumer, unlisted) = workspace_fixture(true);
        let _config = pin_config(&dir.path().join(".kache.toml"));
        let provider_args = rustc_args(&provider, "macro_provider", None);
        let external = cargo_extern_with_provider_provenance(
            dir.path(),
            "crates/macro-provider",
            "macro_provider",
            "provider_alias",
        );
        let consumer_args = rustc_args(&consumer, "consumer", Some(&external));

        let provider_snapshot =
            ExtraInputsSnapshot::resolve_for_rustc(&provider_args, &FileHasher::new())
                .unwrap()
                .expect("selected provider has a snapshot");
        let consumer_snapshot =
            ExtraInputsSnapshot::resolve_for_rustc(&consumer_args, &FileHasher::new())
                .unwrap()
                .expect("direct consumer inherits the provider snapshot");
        assert_eq!(provider_snapshot.digest, consumer_snapshot.digest);
        assert_eq!(
            provider_snapshot.matched_files,
            consumer_snapshot.matched_files
        );
        assert!(
            provider_snapshot
                .additional_config_paths
                .contains(&provider),
            "the provider source must be injected as artifact provenance"
        );
        assert!(
            provider_snapshot
                .observations
                .iter()
                .any(|observation| observation.path == provider)
        );
        assert!(
            !consumer_snapshot
                .additional_config_paths
                .contains(&provider),
            "direct consumers must not inherit the provider-only source marker"
        );
        assert!(
            !consumer_snapshot
                .observations
                .iter()
                .any(|observation| observation.path == provider)
        );
        assert!(
            consumer_snapshot
                .matched_files
                .contains(&dir.path().join("shared/value.txt"))
        );
        assert!(
            consumer_snapshot
                .additional_config_paths
                .contains(&dir.path().join("Cargo.toml"))
        );
        assert!(consumer_snapshot.observations.iter().any(|observation| {
            observation.path == dir.path().join("crates/macro-provider/Cargo.toml")
        }));
        let producer_dep_info = cargo_artifact_identity(
            consumer_args.externs[0]
                .path
                .as_deref()
                .expect("fixture extern has an artifact"),
        )
        .unwrap()
        .dep_info_path;
        assert!(
            consumer_snapshot
                .observations
                .iter()
                .any(|observation| observation.path == producer_dep_info),
            "producer dep-info provenance must be race-revalidated"
        );

        let unlisted_args = rustc_args(&unlisted, "unlisted", None);
        assert!(
            ExtraInputsSnapshot::resolve_for_rustc(&unlisted_args, &FileHasher::new())
                .unwrap()
                .is_none(),
            "an unlisted package must retain the legacy key path"
        );

        let before = consumer_snapshot.digest.unwrap();
        std::fs::write(dir.path().join("shared/value.txt"), "bravo").unwrap();
        let after = ExtraInputsSnapshot::resolve_for_rustc(&consumer_args, &FileHasher::new())
            .unwrap()
            .unwrap()
            .digest
            .unwrap();
        assert_ne!(before, after);
    }

    #[test]
    fn workspace_rule_without_propagation_only_rekeys_provider() {
        let _lock = crate::config::config_path_lock();
        let (dir, provider, consumer, _) = workspace_fixture(false);
        std::fs::write(
            dir.path().join(".kache.toml"),
            "[[workspace.extra_inputs]]\ncrates=['macro-provider']\ninputs=['shared/value.txt']\npropagate_to_dependents=false\n",
        )
        .unwrap();
        let _config = pin_config(&dir.path().join(".kache.toml"));

        assert!(
            ExtraInputsSnapshot::resolve_for_rustc(
                &rustc_args(&provider, "macro_provider", None),
                &FileHasher::new(),
            )
            .unwrap()
            .is_some(),
            "the selected provider always owns its declared digest"
        );
        assert!(
            ExtraInputsSnapshot::resolve_for_rustc(
                &rustc_args(
                    &consumer,
                    "consumer",
                    Some(
                        "macro_provider=/external-target/debug/deps/libmacro_provider-12345678.so",
                    ),
                ),
                &FileHasher::new(),
            )
            .unwrap()
            .is_none(),
            "propagate_to_dependents=false preserves the direct consumer's legacy key"
        );
    }

    #[test]
    fn digestless_colocated_declaration_preserves_workspace_digest_and_watch() {
        let _lock = crate::config::config_path_lock();
        let (dir, provider, _, _) = workspace_fixture(false);
        let _config = pin_config(&dir.path().join(".kache.toml"));
        let args = rustc_args(&provider, "macro_provider", None);

        let workspace_only = resolve_workspace_snapshot(&args, &FileHasher::new())
            .unwrap()
            .expect("provider has a workspace snapshot");
        let colocated = crate_dir_from_source(&provider).unwrap().join("kache.toml");
        std::fs::write(&colocated, "extra_inputs=[]\n").unwrap();
        let combined = ExtraInputsSnapshot::resolve_for_rustc(&args, &FileHasher::new())
            .unwrap()
            .expect("empty co-located declaration still contributes a watch");

        assert_eq!(
            combined.digest, workspace_only.digest,
            "a digestless declaration must be a cache-key no-op"
        );
        let config_paths: Vec<_> = std::iter::once(&combined.config_path)
            .chain(&combined.additional_config_paths)
            .collect();
        assert!(
            config_paths.contains(&&colocated),
            "the empty co-located config must still enter Cargo dep-info: {config_paths:?} vs {colocated:?}"
        );
        assert!(
            combined
                .observations
                .iter()
                .any(|observation| observation.path == colocated),
            "the empty co-located config must still be race-revalidated"
        );
    }

    #[test]
    fn extern_artifact_name_wins_over_a_colliding_dependency_alias() {
        let _lock = crate::config::config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("Cargo.toml"),
            "[workspace]\nmembers=['a', 'b', 'consumer']\nresolver='2'\n",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("shared")).unwrap();
        std::fs::write(dir.path().join("shared/a.txt"), "alpha").unwrap();
        std::fs::write(dir.path().join("shared/b.txt"), "bravo").unwrap();
        let consumer = write_workspace_package(
            dir.path(),
            "consumer",
            "[package]\nname='consumer'\nversion='0.1.0'\n",
        );
        for package in ["a", "b"] {
            write_workspace_package(
                dir.path(),
                package,
                &format!("[package]\nname='{package}'\nversion='0.1.0'\n"),
            );
        }
        std::fs::write(
            dir.path().join(".kache.toml"),
            "[[workspace.extra_inputs]]\ncrates=['a']\ninputs=['shared/a.txt']\npropagate_to_dependents=true\n\n[[workspace.extra_inputs]]\ncrates=['b']\ninputs=['shared/b.txt']\npropagate_to_dependents=true\n",
        )
        .unwrap();
        let _config = pin_config(&dir.path().join(".kache.toml"));

        let external = cargo_extern_with_provider_provenance(dir.path(), "a", "a", "b");
        let snapshot = ExtraInputsSnapshot::resolve_for_rustc(
            &rustc_args(&consumer, "consumer", Some(&external)),
            &FileHasher::new(),
        )
        .unwrap()
        .expect("artifact provenance resolves the aliased provider");
        assert!(
            snapshot
                .matched_files
                .contains(&dir.path().join("shared/a.txt"))
        );
        assert!(
            !snapshot
                .matched_files
                .contains(&dir.path().join("shared/b.txt"))
        );
    }

    #[test]
    fn workspace_snapshot_is_relocation_stable_and_external_config_is_ignored() {
        let _lock = crate::config::config_path_lock();
        let (first, _, first_consumer, _) = workspace_fixture(false);
        let (second, _, second_consumer, _) = workspace_fixture(false);
        let first_external = cargo_extern_with_provider_provenance(
            first.path(),
            "macro-provider",
            "macro_provider",
            "macro_provider",
        );
        let second_external = cargo_extern_with_provider_provenance(
            second.path(),
            "macro-provider",
            "macro_provider",
            "macro_provider",
        );

        let first_digest = {
            let _config = pin_config(&first.path().join(".kache.toml"));
            ExtraInputsSnapshot::resolve_for_rustc(
                &rustc_args(&first_consumer, "consumer", Some(&first_external)),
                &FileHasher::new(),
            )
            .unwrap()
            .unwrap()
            .digest
        };
        let second_digest = {
            let _config = pin_config(&second.path().join(".kache.toml"));
            ExtraInputsSnapshot::resolve_for_rustc(
                &rustc_args(&second_consumer, "consumer", Some(&second_external)),
                &FileHasher::new(),
            )
            .unwrap()
            .unwrap()
            .digest
        };
        assert_eq!(first_digest, second_digest);

        let global = tempfile::tempdir().unwrap();
        std::fs::write(
            global.path().join(".kache.toml"),
            r#"[[workspace.extra_inputs]]
crates = ["macro-provider"]
inputs = ["shared/value.txt"]
"#,
        )
        .unwrap();
        let _global_config = pin_config(&global.path().join(".kache.toml"));
        assert!(
            ExtraInputsSnapshot::resolve_for_rustc(
                &rustc_args(&first_consumer, "consumer", Some(&first_external)),
                &FileHasher::new(),
            )
            .unwrap()
            .is_none(),
            "a KACHE_CONFIG outside the source workspace must not inject rules"
        );
    }

    #[test]
    fn workspace_rules_reject_glob_selectors_unresolved_packages_and_escaping_inputs() {
        let _lock = crate::config::config_path_lock();
        let (dir, provider, _, _) = workspace_fixture(false);
        let _config = pin_config(&dir.path().join(".kache.toml"));
        let args = rustc_args(&provider, "macro_provider", None);

        for (config, expected) in [
            (
                "[[workspace.extra_inputs]]\ncrates=['macro-*']\ninputs=['shared/value.txt']\n",
                "uses glob syntax",
            ),
            (
                "[[workspace.extra_inputs]]\ncrates=['']\ninputs=['shared/value.txt']\n",
                "must be a non-empty exact Cargo package name without surrounding whitespace",
            ),
            (
                "[[workspace.extra_inputs]]\ncrates=[' macro-provider']\ninputs=['shared/value.txt']\n",
                "must be a non-empty exact Cargo package name without surrounding whitespace",
            ),
            (
                "[[workspace.extra_inputs]]\ncrates=['macro-provider ']\ninputs=['shared/value.txt']\n",
                "must be a non-empty exact Cargo package name without surrounding whitespace",
            ),
            (
                "[[workspace.extra_inputs]]\ncrates=['missing']\ninputs=['shared/value.txt']\n",
                "does not resolve to an explicit member",
            ),
            (
                "[[workspace.extra_inputs]]\ncrates=['macro-provider']\ninputs=['../outside']\n",
                "must stay relative to the workspace root",
            ),
        ] {
            std::fs::write(dir.path().join(".kache.toml"), config).unwrap();
            let error = ExtraInputsSnapshot::resolve_for_rustc(&args, &FileHasher::new())
                .expect_err("invalid workspace declaration must fail closed");
            assert!(format!("{error:#}").contains(expected), "{error:#}");
        }
    }

    #[test]
    fn unlisted_crate_does_not_resolve_an_unrelated_provider_input() {
        let _lock = crate::config::config_path_lock();
        let (dir, provider, _, unlisted) = workspace_fixture(false);
        let _config = pin_config(&dir.path().join(".kache.toml"));
        std::fs::write(
            dir.path().join(".kache.toml"),
            "[[workspace.extra_inputs]]\ncrates=['macro-provider']\ninputs=['$KACHE_368_UNSET/value.txt']\n",
        )
        .unwrap();

        assert!(
            ExtraInputsSnapshot::resolve_for_rustc(
                &rustc_args(&unlisted, "unlisted", None),
                &FileHasher::new(),
            )
            .unwrap()
            .is_none(),
            "an unrelated crate must not expand or glob provider inputs"
        );
        let error = ExtraInputsSnapshot::resolve_for_rustc(
            &rustc_args(&provider, "macro_provider", None),
            &FileHasher::new(),
        )
        .expect_err("the selected provider must still fail closed");
        assert!(format!("{error:#}").contains("uses `$ENV`"), "{error:#}");
    }

    #[test]
    fn artifact_provenance_disambiguates_two_selected_packages_with_one_crate_name() {
        let _lock = crate::config::config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("Cargo.toml"),
            "[workspace]\nmembers=['a', 'b', 'consumer']\nresolver='2'\n",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("shared")).unwrap();
        std::fs::write(dir.path().join("shared/a.txt"), "alpha").unwrap();
        std::fs::write(dir.path().join("shared/b.txt"), "bravo").unwrap();
        for package in ["a", "b"] {
            write_workspace_package(
                dir.path(),
                package,
                &format!(
                    "[package]\nname='{package}'\nversion='0.1.0'\n[lib]\nname='shared_provider'\n"
                ),
            );
        }
        let consumer = write_workspace_package(
            dir.path(),
            "consumer",
            "[package]\nname='consumer'\nversion='0.1.0'\n",
        );
        std::fs::write(
            dir.path().join(".kache.toml"),
            "[[workspace.extra_inputs]]\ncrates=['a']\ninputs=['shared/a.txt']\npropagate_to_dependents=true\n\n[[workspace.extra_inputs]]\ncrates=['b']\ninputs=['shared/b.txt']\npropagate_to_dependents=true\n",
        )
        .unwrap();
        let _config = pin_config(&dir.path().join(".kache.toml"));
        let external = cargo_extern_with_provider_provenance(
            dir.path(),
            "a",
            "shared_provider",
            "shared_provider",
        );
        let snapshot = ExtraInputsSnapshot::resolve_for_rustc(
            &rustc_args(&consumer, "consumer", Some(&external)),
            &FileHasher::new(),
        )
        .unwrap()
        .expect("producer marker selects package a");
        assert!(
            snapshot
                .matched_files
                .contains(&dir.path().join("shared/a.txt"))
        );
        assert!(
            !snapshot
                .matched_files
                .contains(&dir.path().join("shared/b.txt"))
        );
    }

    #[test]
    fn same_crate_name_from_unselected_member_leaves_consumer_unaffected() {
        let _lock = crate::config::config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("Cargo.toml"),
            "[workspace]\nmembers=['a', 'b', 'consumer']\nresolver='2'\n",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("shared")).unwrap();
        std::fs::write(dir.path().join("shared/value.txt"), "alpha").unwrap();
        for package in ["a", "b"] {
            write_workspace_package(
                dir.path(),
                package,
                &format!(
                    "[package]\nname='{package}'\nversion='0.1.0'\n[lib]\nname='shared_provider'\n"
                ),
            );
        }
        let consumer = write_workspace_package(
            dir.path(),
            "consumer",
            "[package]\nname='consumer'\nversion='0.1.0'\n",
        );
        std::fs::write(
            dir.path().join(".kache.toml"),
            "[[workspace.extra_inputs]]\ncrates=['a']\ninputs=['shared/value.txt']\npropagate_to_dependents=true\n",
        )
        .unwrap();
        let _config = pin_config(&dir.path().join(".kache.toml"));
        let external = cargo_extern_with_provider_provenance(
            dir.path(),
            "b",
            "shared_provider",
            "shared_provider",
        );
        assert!(
            ExtraInputsSnapshot::resolve_for_rustc(
                &rustc_args(&consumer, "consumer", Some(&external),),
                &FileHasher::new(),
            )
            .unwrap()
            .is_none(),
            "an unselected same-named artifact must not inherit package a's digest"
        );
    }

    #[test]
    fn artifact_provenance_rejects_incomplete_and_missing_producer_dep_info() {
        let (dir, _, _, _) = workspace_fixture(false);
        let workspace_manifest = dir.path().join("Cargo.toml");
        let package = load_workspace_packages(dir.path(), &workspace_manifest)
            .unwrap()
            .remove("macro-provider")
            .unwrap();
        let provider = WorkspaceProviderSpec {
            package,
            rule_indices: vec![0],
        };
        let deps = dir.path().join("fake-target/debug/deps");
        std::fs::create_dir_all(&deps).unwrap();
        let artifact = deps.join("libmacro_provider-12345678.rlib");
        std::fs::write(&artifact, "artifact").unwrap();
        let identity = cargo_artifact_identity(&artifact).unwrap();
        let cwd = std::env::current_dir().unwrap();
        let source = provider.package.lib_source_path.as_ref().unwrap();
        let relative_source = lexical_relative_path(&cwd, source);
        assert_eq!(anchor_input_path(&cwd, &relative_source), *source);
        std::fs::write(
            &identity.dep_info_path,
            format!(
                "{}: {}\n",
                make_escape_word(&artifact.to_string_lossy()).unwrap(),
                make_escape_word(&relative_source.to_string_lossy()).unwrap(),
            ),
        )
        .unwrap();

        let error = resolve_artifact_provider(&identity, &[&provider])
            .err()
            .expect("source without its provider manifest is incomplete provenance");
        assert!(
            format!("{error:#}").contains("incomplete producer marker"),
            "{error:#}"
        );

        std::fs::remove_file(&identity.dep_info_path).unwrap();
        let error = resolve_artifact_provider(&identity, &[&provider])
            .err()
            .expect("missing producer dep-info must fail closed");
        assert!(
            format!("{error:#}").contains("cannot observe producer dep-info"),
            "{error:#}"
        );
    }

    #[test]
    fn explicit_lib_path_without_name_uses_normalized_package_name() {
        let _lock = crate::config::config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("Cargo.toml"),
            "[workspace]\nmembers=['provider-package']\nresolver='2'\n",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("shared")).unwrap();
        std::fs::write(dir.path().join("shared/value.txt"), "alpha").unwrap();
        std::fs::write(
            dir.path().join(".kache.toml"),
            "[[workspace.extra_inputs]]\ncrates=['provider-package']\ninputs=['shared/value.txt']\n",
        )
        .unwrap();
        let package = dir.path().join("provider-package");
        let source = package.join("generated/nested/provider.rs");
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::write(
            package.join("Cargo.toml"),
            "[package]\nname='provider-package'\nversion='0.1.0'\n[lib]\npath='generated/nested/provider.rs'\n",
        )
        .unwrap();
        // A closer, unrelated manifest must not stop member discovery.
        std::fs::write(
            package.join("generated/Cargo.toml"),
            "[package]\nname='nested-nonmember'\nversion='0.1.0'\n",
        )
        .unwrap();
        std::fs::write(&source, "pub fn marker() {}\n").unwrap();
        let _config = pin_config(&dir.path().join(".kache.toml"));

        assert!(
            ExtraInputsSnapshot::resolve_for_rustc(
                &rustc_args(&source, "provider_package", None),
                &FileHasher::new(),
            )
            .unwrap()
            .is_some(),
            "[lib] without name defaults to the normalized package name"
        );
    }

    #[test]
    fn bin_only_package_owns_non_propagated_workspace_digest() {
        let _lock = crate::config::config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("Cargo.toml"),
            "[workspace]\nmembers=['bin-only']\nresolver='2'\n",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("shared")).unwrap();
        std::fs::write(dir.path().join("shared/value.txt"), "alpha").unwrap();
        write_workspace_package(
            dir.path(),
            "bin-only",
            "[package]\nname='bin-only'\nversion='0.1.0'\nautolib=false\n",
        );
        std::fs::write(dir.path().join("bin-only/src/main.rs"), "fn main() {}\n").unwrap();
        let bin_source = dir.path().join("bin-only/src/main.rs");
        std::fs::write(
            dir.path().join(".kache.toml"),
            "[[workspace.extra_inputs]]\ncrates=['bin-only']\ninputs=['shared/value.txt']\n",
        )
        .unwrap();
        let _config = pin_config(&dir.path().join(".kache.toml"));

        let args = rustc_args(&bin_source, "bin_only", None);
        let before = ExtraInputsSnapshot::resolve_for_rustc(&args, &FileHasher::new())
            .unwrap()
            .expect("selected bin target owns its package rule")
            .digest
            .unwrap();
        std::fs::write(dir.path().join("shared/value.txt"), "bravo").unwrap();
        let after = ExtraInputsSnapshot::resolve_for_rustc(&args, &FileHasher::new())
            .unwrap()
            .unwrap()
            .digest
            .unwrap();
        assert_ne!(
            before, after,
            "editing the package input must re-key its bin"
        );
    }

    #[test]
    fn bin_only_package_rejects_propagation_without_library_target() {
        let _lock = crate::config::config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("Cargo.toml"),
            "[workspace]\nmembers=['bin-only']\nresolver='2'\n",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("shared")).unwrap();
        std::fs::write(dir.path().join("shared/value.txt"), "alpha").unwrap();
        write_workspace_package(
            dir.path(),
            "bin-only",
            "[package]\nname='bin-only'\nversion='0.1.0'\nautolib=false\n",
        );
        let bin_source = dir.path().join("bin-only/src/main.rs");
        std::fs::write(&bin_source, "fn main() {}\n").unwrap();
        std::fs::write(
            dir.path().join(".kache.toml"),
            "[[workspace.extra_inputs]]\ncrates=['bin-only']\ninputs=['shared/value.txt']\npropagate_to_dependents=true\n",
        )
        .unwrap();
        let _config = pin_config(&dir.path().join(".kache.toml"));

        let error = ExtraInputsSnapshot::resolve_for_rustc(
            &rustc_args(&bin_source, "bin_only", None),
            &FileHasher::new(),
        )
        .expect_err("bin-only packages cannot provide a direct extern");
        assert!(
            format!("{error:#}")
                .contains("propagate_to_dependents=true but has no library/proc-macro target"),
            "{error:#}"
        );
    }

    #[test]
    fn mixed_package_propagates_only_its_library_target() {
        let _lock = crate::config::config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("Cargo.toml"),
            "[workspace]\nmembers=['mixed-package', 'consumer']\nresolver='2'\n",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("shared")).unwrap();
        std::fs::write(dir.path().join("shared/value.txt"), "alpha").unwrap();
        let lib_source = write_workspace_package(
            dir.path(),
            "mixed-package",
            "[package]\nname='mixed-package'\nversion='0.1.0'\n[[bin]]\nname='custom-tool'\npath='tools/main.rs'\n",
        );
        let bin_source = dir.path().join("mixed-package/tools/main.rs");
        std::fs::create_dir_all(bin_source.parent().unwrap()).unwrap();
        std::fs::write(&bin_source, "fn main() {}\n").unwrap();
        let consumer = write_workspace_package(
            dir.path(),
            "consumer",
            "[package]\nname='consumer'\nversion='0.1.0'\n",
        );
        std::fs::write(
            dir.path().join(".kache.toml"),
            "[[workspace.extra_inputs]]\ncrates=['mixed-package']\ninputs=['shared/value.txt']\npropagate_to_dependents=true\n",
        )
        .unwrap();
        let _config = pin_config(&dir.path().join(".kache.toml"));

        let lib = ExtraInputsSnapshot::resolve_for_rustc(
            &rustc_args(&lib_source, "mixed_package", None),
            &FileHasher::new(),
        )
        .unwrap()
        .unwrap();
        let bin = ExtraInputsSnapshot::resolve_for_rustc(
            &rustc_args(&bin_source, "custom_tool", None),
            &FileHasher::new(),
        )
        .unwrap()
        .unwrap();
        let lib_external = cargo_extern_with_provider_provenance(
            dir.path(),
            "mixed-package",
            "mixed_package",
            "mixed_package",
        );
        let dependent = ExtraInputsSnapshot::resolve_for_rustc(
            &rustc_args(&consumer, "consumer", Some(&lib_external)),
            &FileHasher::new(),
        )
        .unwrap()
        .expect("the package library artifact propagates its digest");

        let deps = dir.path().join("fake-target/debug/deps");
        let bin_artifact = deps.join("custom_tool-87654321");
        std::fs::write(&bin_artifact, "fake bin artifact").unwrap();
        std::fs::write(
            deps.join("custom_tool-87654321.d"),
            format!(
                "{}: {} {}\n",
                make_escape_word(&bin_artifact.to_string_lossy()).unwrap(),
                make_escape_word(
                    &dir.path()
                        .join("mixed-package/Cargo.toml")
                        .to_string_lossy()
                )
                .unwrap(),
                make_escape_word(&bin_source.to_string_lossy()).unwrap(),
            ),
        )
        .unwrap();
        let bin_external = format!("custom_tool={}", bin_artifact.display());

        assert_eq!(lib.digest, bin.digest);
        assert_eq!(lib.digest, dependent.digest);
        assert!(
            ExtraInputsSnapshot::resolve_for_rustc(
                &rustc_args(&consumer, "consumer", Some(&bin_external)),
                &FileHasher::new(),
            )
            .unwrap()
            .is_none(),
            "the selected package's bin artifact must not propagate"
        );
    }

    #[test]
    fn cargo_auto_member_direct_consumer_inherits_provider_snapshot() {
        let _lock = crate::config::config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("Cargo.toml"),
            "[workspace]\nmembers=['host', 'macro-provider']\nresolver='2'\n",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("shared")).unwrap();
        std::fs::write(dir.path().join("shared/value.txt"), "alpha").unwrap();
        std::fs::write(
            dir.path().join(".kache.toml"),
            "[[workspace.extra_inputs]]\ncrates=['macro-provider']\ninputs=['shared/value.txt']\npropagate_to_dependents=true\n",
        )
        .unwrap();
        write_workspace_package(
            dir.path(),
            "macro-provider",
            "[package]\nname='macro-provider'\nversion='0.1.0'\n[lib]\nproc-macro=true\n",
        );
        write_workspace_package(
            dir.path(),
            "host",
            "[package]\nname='host'\nversion='0.1.0'\n[dependencies]\nauto-consumer={path='../auto-consumer'}\n",
        );
        let consumer = write_workspace_package(
            dir.path(),
            "auto-consumer",
            "[package]\nname='auto-consumer'\nversion='0.1.0'\n[dependencies]\nmacro-provider={path='../macro-provider'}\n",
        );

        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let output = std::process::Command::new(cargo)
            .args([
                "metadata",
                "--no-deps",
                "--format-version",
                "1",
                "--manifest-path",
            ])
            .arg(dir.path().join("Cargo.toml"))
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let metadata: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        let consumer_id = metadata["packages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|package| package["name"] == "auto-consumer")
            .map(|package| package["id"].clone())
            .expect("Cargo reports the in-root path dependency package");
        assert!(
            metadata["workspace_members"]
                .as_array()
                .unwrap()
                .contains(&consumer_id),
            "fixture must exercise Cargo's implicit workspace membership"
        );

        let _config = pin_config(&dir.path().join(".kache.toml"));
        let external = cargo_extern_with_provider_provenance(
            dir.path(),
            "macro-provider",
            "macro_provider",
            "macro_provider",
        );
        let snapshot = ExtraInputsSnapshot::resolve_for_rustc(
            &rustc_args(&consumer, "auto_consumer", Some(&external)),
            &FileHasher::new(),
        )
        .unwrap()
        .expect("direct --extern matching must not require the consumer in the manual member map");
        assert!(
            snapshot
                .matched_files
                .contains(&dir.path().join("shared/value.txt"))
        );
    }

    #[test]
    fn workspace_rule_config_path_with_parent_traversal_fails_closed() {
        let _lock = crate::config::config_path_lock();
        let (dir, provider, _, _) = workspace_fixture(false);
        std::fs::create_dir_all(dir.path().join("nested")).unwrap();
        let ambiguous = dir.path().join("nested/../.kache.toml");
        let _config = pin_config(&ambiguous);

        let error = ExtraInputsSnapshot::resolve_for_rustc(
            &rustc_args(&provider, "macro_provider", None),
            &FileHasher::new(),
        )
        .expect_err("workspace rules must not use an ambiguous lexical root");
        assert!(format!("{error:#}").contains("contains `..`"), "{error:#}");
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_workspace_rule_config_fails_closed() {
        use std::os::unix::fs::symlink;

        let _lock = crate::config::config_path_lock();
        let (dir, provider, _, _) = workspace_fixture(false);
        let config = dir.path().join(".kache.toml");
        let target = dir.path().join("workspace-rules.toml");
        std::fs::rename(&config, &target).unwrap();
        symlink(&target, &config).unwrap();
        let _config = pin_config(&config);

        let error = ExtraInputsSnapshot::resolve_for_rustc(
            &rustc_args(&provider, "macro_provider", None),
            &FileHasher::new(),
        )
        .expect_err("workspace root cannot be anchored by a config symlink");
        assert!(format!("{error:#}").contains("is a symlink"), "{error:#}");
    }

    #[test]
    fn snapshot_tracks_config_matches_and_narrow_watch_across_add_delete() {
        let (dir, src) = crate_fixture(&[
            ("kache.toml", "extra_inputs = [\"data/**/*.txt\"]"),
            ("data/a.txt", "a"),
            ("data/nested/b.txt", "b"),
        ]);
        let root = dir.path();

        let initial = snap(&src);
        assert_eq!(initial.config_path, root.join("kache.toml"));
        assert_eq!(
            initial.matched_files,
            vec![root.join("data/a.txt"), root.join("data/nested/b.txt")]
        );
        assert_eq!(initial.watch_paths, vec![root.join("data")]);

        // The literal root stays the watch dependency while the matched-file
        // set follows additions and deletions. Cargo recursively fingerprints
        // that one narrow directory, so either transition makes the unit dirty.
        std::fs::write(root.join("data/added.txt"), "added").unwrap();
        let after_add = snap(&src);
        assert!(
            after_add
                .matched_files
                .contains(&root.join("data/added.txt"))
        );
        assert_eq!(after_add.watch_paths, vec![root.join("data")]);

        std::fs::remove_file(root.join("data/a.txt")).unwrap();
        let after_delete = snap(&src);
        assert!(
            !after_delete
                .matched_files
                .contains(&root.join("data/a.txt"))
        );
        assert_eq!(after_delete.watch_paths, vec![root.join("data")]);
    }

    #[test]
    fn snapshot_observes_nested_watch_directories_against_transient_aba() {
        let (dir, src) = crate_fixture(&[
            ("kache.toml", "extra_inputs = [\"data/**/*.txt\"]"),
            ("data/stable.txt", "v1"),
            ("data/deep/.keep", ""),
        ]);
        let nested = dir.path().join("data/deep");
        let before = snap(&src);
        assert!(
            before
                .observations
                .iter()
                .any(|observation| observation.path == nested),
            "every traversed directory must participate in hit/publication revalidation"
        );

        // The compiler could observe this member while it exists even though
        // the final matched-file set and content digest return to the original
        // state. A changed nested-directory observation must still reject that
        // result instead of publishing it under the old key.
        let transient = nested.join("transient.txt");
        std::fs::write(&transient, "transient").unwrap();
        std::fs::remove_file(&transient).unwrap();
        filetime::set_file_mtime(
            &nested,
            filetime::FileTime::from_unix_time(2_000_000_000, 123),
        )
        .unwrap();

        let after = snap(&src);
        assert_eq!(before.digest, after.digest, "semantic state returned to v1");
        assert_eq!(before.matched_files, after.matched_files);
        assert_ne!(
            before, after,
            "nested directory metadata must expose an add/remove ABA race"
        );
    }

    #[test]
    fn empty_declaration_watches_config_without_changing_the_key() {
        let (dir, src) = crate_fixture(&[("kache.toml", "extra_inputs = []")]);
        assert_eq!(dig(&src), None, "legacy key semantics stay byte-identical");

        let snapshot = snap(&src);
        assert_eq!(snapshot.digest(), None);
        assert_eq!(snapshot.config_path, dir.path().join("kache.toml"));
        assert!(snapshot.matched_files.is_empty());
        assert!(snapshot.watch_paths.is_empty());
    }

    #[test]
    fn active_snapshot_rejects_dynamic_pattern_expansion() {
        let (_dir, src) =
            crate_fixture(&[("kache.toml", "extra_inputs = [\"$HOME/data/**/*.txt\"]")]);
        let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new())
            .expect_err("environment-dependent paths cannot stay Cargo-fresh");
        assert!(
            format!("{error:#}").contains("uses `$ENV` or `~` expansion"),
            "{error:#}"
        );
    }

    #[test]
    fn active_snapshot_rejects_parent_traversal_after_wildcard() {
        let (_dir, src) = crate_fixture(&[(
            "kache.toml",
            "extra_inputs = [\"data/*/../../generated/*.json\"]",
        )]);
        let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new())
            .expect_err("a wildcard must not escape the bounded Cargo watch root");
        assert!(
            format!("{error:#}").contains("traverses `..` after a wildcard"),
            "{error:#}"
        );
    }

    #[cfg(windows)]
    #[test]
    fn active_snapshot_rejects_ambiguous_windows_root_shapes() {
        assert!(windows_pattern_has_ambiguous_root(Path::new(
            r"\shared\data.json"
        )));
        assert!(windows_pattern_has_ambiguous_root(Path::new(
            r"C:shared\data.json"
        )));
        assert!(!windows_pattern_has_ambiguous_root(Path::new(
            r"C:\shared\data.json"
        )));

        for pattern in ["/shared/**/*.json", "C:shared/**/*.json"] {
            let config = format!("extra_inputs = ['{pattern}']");
            let (_dir, src) = crate_fixture(&[("kache.toml", config.as_str())]);
            let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new())
                .expect_err("ambiguous Windows anchoring must fail closed");
            assert!(
                format!("{error:#}").contains("rooted-without-drive or drive-relative"),
                "{error:#}"
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn active_snapshot_rejects_windows_device_namespace_patterns() {
        for pattern in [
            r"\\?\C:\shared\**\*.json",
            r"\\?\UNC\server\share\**\*.json",
        ] {
            let config = format!("extra_inputs = ['{pattern}']");
            let (_dir, src) = crate_fixture(&[("kache.toml", config.as_str())]);
            let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new())
                .expect_err("glob cannot safely enumerate a device namespace");
            assert!(
                format!("{error:#}").contains("verbatim/device namespace"),
                "{error:#}"
            );
        }
        assert!(windows_path_uses_device_namespace(Path::new(
            r"\\?\C:\repo\crate"
        )));
        assert!(!windows_path_uses_device_namespace(Path::new(
            r"C:\repo\crate"
        )));
        assert!(!windows_path_uses_device_namespace(Path::new(
            r"\\server\share\crate"
        )));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn active_glob_rejects_non_utf8_names_in_its_watch_tree() {
        use std::os::unix::ffi::OsStringExt;

        let (dir, src) = crate_fixture(&[("kache.toml", "extra_inputs = [\"data/**/*.txt\"]")]);
        let data = dir.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(
            data.join(std::ffi::OsString::from_vec(vec![0xff])),
            "hidden",
        )
        .unwrap();

        let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new())
            .expect_err("glob must not silently omit non-UTF-8 names");
        assert!(format!("{error:#}").contains("non-UTF-8"), "{error:#}");
    }

    #[test]
    fn missing_literal_watches_existing_narrow_parent() {
        let (dir, src) = crate_fixture(&[
            ("kache.toml", "extra_inputs = [\"inputs/missing.json\"]"),
            ("inputs/.keep", ""),
        ]);
        let snapshot = snap(&src);
        assert!(snapshot.matched_files.is_empty());
        assert_eq!(snapshot.watch_paths, vec![dir.path().join("inputs")]);
    }

    #[test]
    fn broad_crate_root_watch_is_rejected_before_globbing() {
        let (_dir, src) = crate_fixture(&[("kache.toml", "extra_inputs = [\"**/*.json\"]")]);
        let file_hasher = FileHasher::new();
        let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &file_hasher)
            .expect_err("crate-root recursive watch must be rejected");
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("recursively watch broad directory"),
            "{rendered}"
        );
        assert!(rendered.contains("literal subdirectory"), "{rendered}");
    }

    #[cfg(unix)]
    #[test]
    fn filesystem_root_watch_is_rejected_without_enumeration() {
        let (_dir, src) = crate_fixture(&[("kache.toml", "extra_inputs = [\"/**/*.json\"]")]);
        let file_hasher = FileHasher::new();
        let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &file_hasher)
            .expect_err("filesystem-root recursive watch must be rejected");
        assert!(
            format!("{error:#}").contains("recursively watch broad directory"),
            "{error:#}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlink_to_filesystem_root_cannot_bypass_watch_guard() {
        use std::os::unix::fs::symlink;

        let (dir, src) =
            crate_fixture(&[("kache.toml", "extra_inputs = [\"root-link/**/*.json\"]")]);
        symlink("/", dir.path().join("root-link")).unwrap();
        let file_hasher = FileHasher::new();
        let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &file_hasher)
            .expect_err("a symlink to the filesystem root must be rejected before globbing");
        assert!(
            format!("{error:#}").contains("recursively watch broad directory"),
            "{error:#}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn shared_symlink_ancestor_is_not_below_the_crate_dependency_boundary() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        let crate_dir = target.join("crate");
        let dependency = crate_dir.join("data/value.txt");
        std::fs::create_dir_all(dependency.parent().unwrap()).unwrap();
        std::fs::write(&dependency, "v1").unwrap();

        let alias = dir.path().join("alias");
        symlink(&target, &alias).unwrap();
        let aliased_crate = alias.join("crate");
        let aliased_dependency = aliased_crate.join("data/value.txt");
        assert_eq!(
            first_symlink_below_common(&aliased_crate, &aliased_dependency),
            None,
            "only symlinks below the shared crate prefix are unsafe"
        );
    }

    #[test]
    fn watch_path_limit_accepts_the_limit_and_rejects_one_more() {
        let dir = tempfile::tempdir().unwrap();
        let crate_dir = dir.path().join("crate");
        std::fs::create_dir(&crate_dir).unwrap();

        let mut patterns = Vec::with_capacity(MAX_WATCH_PATHS + 1);
        for index in 0..=MAX_WATCH_PATHS {
            let watch = crate_dir.join(format!("watch-{index}"));
            std::fs::create_dir(&watch).unwrap();
            patterns.push(NormalizedInputPattern {
                glob: format!("watch-{index}/**/*"),
                watch: WatchIntent::DirectoryRoot(watch),
            });
        }

        let accepted = resolve_watch_paths(&crate_dir, &patterns[..MAX_WATCH_PATHS]).unwrap();
        assert_eq!(accepted.len(), MAX_WATCH_PATHS);
        let error = resolve_watch_paths(&crate_dir, &patterns)
            .expect_err("one watch beyond the bound must fail closed");
        assert!(
            format!("{error:#}").contains(&format!("more than {MAX_WATCH_PATHS}")),
            "{error:#}"
        );
    }

    #[test]
    fn over_broad_file_warning_starts_after_threshold() {
        assert!(!should_warn_over_broad_file_count(OVER_BROAD_FILE_WARN));
        assert!(should_warn_over_broad_file_count(OVER_BROAD_FILE_WARN + 1));
    }

    #[test]
    fn pure_extra_input_predicates_kill_boolean_mutations() {
        assert!(workspace_package_selector_is_invalid(""));
        assert!(workspace_package_selector_is_invalid(" pkg"));
        assert!(workspace_package_selector_is_invalid("pkg "));
        assert!(!workspace_package_selector_is_invalid("pkg"));

        assert!(!should_warn_unset_extra_input_vars(&[]));
        assert!(should_warn_unset_extra_input_vars(&[String::from("HOME")]));

        assert!(pattern_reaches_outside_crate(Path::new("/abs/path")));
        assert!(pattern_reaches_outside_crate(Path::new("../sibling")));
        assert!(!pattern_reaches_outside_crate(Path::new("relative/path")));

        assert!(member_pattern_escapes_workspace(Path::new("/abs/member")));
        assert!(member_pattern_escapes_workspace(Path::new("../member")));
        assert!(!member_pattern_escapes_workspace(Path::new("crates/*")));

        assert!(!is_valid_rustc_crate_name(""));
        assert!(!is_valid_rustc_crate_name("bad-name"));
        assert!(is_valid_rustc_crate_name("good_name"));
        assert!(is_valid_rustc_crate_name("a1"));
    }

    #[test]
    fn expand_workspace_member_pattern_rejects_escaping_shapes() {
        let root = Path::new("/tmp/workspace");
        let absolute = expand_workspace_member_pattern(root, "/abs/member")
            .expect_err("absolute member patterns escape the workspace");
        assert!(
            format!("{absolute:#}").contains("outside the workspace root"),
            "{absolute:#}"
        );
        let parent = expand_workspace_member_pattern(root, "../member")
            .expect_err("parent member patterns escape the workspace");
        assert!(
            format!("{parent:#}").contains("outside the workspace root"),
            "{parent:#}"
        );
    }

    #[test]
    fn validate_rustc_crate_name_rejects_empty_and_hyphenated_names() {
        let manifest = Path::new("/tmp/Cargo.toml");
        for name in ["", "bad-name", "has space"] {
            let error = validate_rustc_crate_name(name, manifest)
                .expect_err("invalid library crate names must fail closed");
            assert!(
                format!("{error:#}").contains("invalid library crate name"),
                "{error:#}"
            );
        }
        assert_eq!(
            validate_rustc_crate_name("good_name", manifest).unwrap(),
            "good_name"
        );
    }

    #[test]
    fn relabel_workspace_snapshot_domains_by_package_and_propagation() {
        let base = ExtraInputsSnapshot {
            config_path: PathBuf::from("/tmp/.kache.toml"),
            additional_config_paths: Vec::new(),
            normalized_patterns: vec!["shared/value.txt".into()],
            digest: Some("inner-digest".into()),
            matched_files: Vec::new(),
            watch_paths: Vec::new(),
            observations: Vec::new(),
        };
        let mut package_a = base.clone();
        relabel_workspace_snapshot(&mut package_a, "package-a", true);
        let mut package_b = base.clone();
        relabel_workspace_snapshot(&mut package_b, "package-b", true);
        let mut no_propagate = base.clone();
        relabel_workspace_snapshot(&mut no_propagate, "package-a", false);

        assert_ne!(
            package_a.digest, package_b.digest,
            "package identity must enter the workspace digest domain"
        );
        assert_ne!(
            package_a.digest, no_propagate.digest,
            "propagation flag must enter the workspace digest domain"
        );
        assert_ne!(
            package_a.digest, base.digest,
            "relabel must replace the raw content digest"
        );
    }

    #[test]
    fn recursive_glob_skips_directories_when_folding_matched_inputs() {
        // A one-level glob enumerates both files and sibling directories. If the
        // directory match-guard is dropped, the empty directory enters the fold
        // as an unreadable sentinel and the digest diverges from a files-only
        // tree with the same content.
        let with_empty_dir = crate_fixture(&[
            ("kache.toml", "extra_inputs = [\"data/*\"]"),
            ("data/file.txt", "v1"),
            ("data/empty/.keep", ""),
        ]);
        std::fs::remove_file(with_empty_dir.0.path().join("data/empty/.keep")).unwrap();
        std::fs::create_dir_all(with_empty_dir.0.path().join("data/empty")).unwrap();

        let files_only = crate_fixture(&[
            ("kache.toml", "extra_inputs = [\"data/*\"]"),
            ("data/file.txt", "v1"),
        ]);

        assert_eq!(
            dig(&with_empty_dir.1),
            dig(&files_only.1),
            "directories matched by a glob must not affect the digest"
        );
    }

    #[cfg(unix)]
    #[test]
    fn active_snapshot_rejects_symlinked_input_that_cargo_would_canonicalize() {
        use std::os::unix::fs::symlink;

        let (dir, src) = crate_fixture(&[("kache.toml", "extra_inputs = [\"data/value.txt\"]")]);
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        let target = dir.path().join("target-value.txt");
        std::fs::write(&target, "v1").unwrap();
        symlink(&target, dir.path().join("data/value.txt")).unwrap();

        let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new())
            .expect_err("symlink retargeting cannot be represented safely in Cargo dep-info");
        assert!(
            format!("{error:#}").contains("crosses symlink"),
            "{error:#}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn active_snapshot_rejects_symlinked_empty_config() {
        use std::os::unix::fs::symlink;

        let (dir, src) = crate_fixture(&[]);
        let target = dir.path().join("empty-config.toml");
        std::fs::write(&target, "extra_inputs = []\n").unwrap();
        symlink(&target, dir.path().join("kache.toml")).unwrap();

        let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new())
            .expect_err("retargetable empty config must fail closed");
        assert!(
            format!("{error:#}").contains("config") && format!("{error:#}").contains("symlink"),
            "{error:#}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn active_glob_rejects_symlink_nested_under_its_watch_root() {
        use std::os::unix::fs::symlink;

        let (dir, src) = crate_fixture(&[("kache.toml", "extra_inputs = [\"data/**/*.txt\"]")]);
        let external = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        std::fs::write(external.path().join("value.txt"), "v1").unwrap();
        symlink(external.path(), dir.path().join("data/external")).unwrap();

        let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new())
            .expect_err("a glob must not follow a nested symlink outside its bounded watch tree");
        assert!(
            format!("{error:#}").contains("contains symlink"),
            "{error:#}"
        );
    }

    #[test]
    fn active_snapshot_rejects_invalid_config_instead_of_watching_only_config() {
        let (_dir, src) = crate_fixture(&[("kache.toml", "extra_inputs = [")]);
        let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new())
            .expect_err("invalid active config must fail closed");
        assert!(
            format!("{error:#}").contains("parsing active extra_inputs config"),
            "{error:#}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn active_snapshot_rejects_control_separator_in_matched_filename() {
        let (dir, src) = crate_fixture(&[("kache.toml", "extra_inputs = [\"data/**/*\"]")]);
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        std::fs::write(dir.path().join("data/bad\u{1f}name.txt"), "v1").unwrap();

        let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new())
            .expect_err("matched paths must not cross cache-key framing");
        assert!(
            format!("{error:#}").contains("cache-key control separator"),
            "{error:#}"
        );
    }

    #[test]
    fn dep_info_merge_escapes_dedupes_and_preserves_later_rules() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let dep_info = root.join("crate.d");
        let original = concat!(
            "# generated by rustc\n",
            "artifact: src/lib.rs\n",
            "later: keep\\ this\n",
            "# env-dep:VALUE=unchanged\n",
        );
        std::fs::write(&dep_info, original).unwrap();
        let snapshot = ExtraInputsSnapshot {
            config_path: root.join("kache.toml"),
            additional_config_paths: Vec::new(),
            normalized_patterns: vec!["inputs/**/*".to_string()],
            digest: Some("digest".to_string()),
            matched_files: vec![root.join("space #colon:slash\\name.txt")],
            watch_paths: vec![root.join("watched dir")],
            observations: Vec::new(),
        };

        assert_eq!(
            make_escape_word("space #colon:slash\\name.txt").unwrap(),
            "space\\ #colon:slash\\name.txt"
        );
        snapshot.merge_into_dep_info(&dep_info).unwrap();
        let once = std::fs::read_to_string(&dep_info).unwrap();
        assert!(once.contains("space\\ #colon:slash\\name.txt"), "{once}");
        assert!(once.contains("watched\\ dir"), "{once}");
        assert_eq!(once.matches("kache.toml").count(), 1, "{once}");
        assert!(
            once.ends_with("later: keep\\ this\n# env-dep:VALUE=unchanged\n"),
            "later rules/comments changed:\n{once}"
        );

        // Idempotence is the strongest dedupe check: a second merge performs
        // no rewrite and leaves the complete file byte-identical.
        snapshot.merge_into_dep_info(&dep_info).unwrap();
        assert_eq!(once, std::fs::read_to_string(&dep_info).unwrap());
    }

    #[test]
    fn dep_info_merge_fails_closed_on_missing_and_malformed() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let snapshot = ExtraInputsSnapshot {
            config_path: root.join("kache.toml"),
            additional_config_paths: Vec::new(),
            normalized_patterns: vec!["inputs/**/*".to_string()],
            digest: Some("digest".to_string()),
            matched_files: Vec::new(),
            watch_paths: vec![root.join("inputs")],
            observations: Vec::new(),
        };

        let missing = root.join("missing.d");
        let missing_error = snapshot.merge_into_dep_info(&missing).unwrap_err();
        assert!(format!("{missing_error:#}").contains("required consumer dep-info"));

        let malformed = root.join("malformed.d");
        let malformed_bytes = "not a dependency rule\nstill not a dependency rule\n";
        std::fs::write(&malformed, malformed_bytes).unwrap();
        let malformed_error = snapshot.merge_into_dep_info(&malformed).unwrap_err();
        assert!(format!("{malformed_error:#}").contains("malformed consumer dep-info"));
        assert_eq!(
            std::fs::read_to_string(&malformed).unwrap(),
            malformed_bytes
        );
    }

    #[test]
    fn make_escape_round_trips_windows_drive_space_hash_and_backslashes() {
        let windows = r"C:\work tree\generated#1:file.rs";
        let escaped = make_escape_word(windows).unwrap();
        assert_eq!(escaped, r"C:\work\ tree\generated#1:file.rs");
        assert_eq!(parse_make_words(&escaped).unwrap(), vec![windows]);
        assert!(make_escape_word("unix-name-ending-in-backslash\\").is_err());
        assert!(make_escape_word("unix-name-ending-in-space ").is_err());
    }

    #[test]
    fn dep_info_codec_treats_hash_and_windows_drive_colons_as_literals() {
        let input = concat!(
            "# generated metadata stays literal\n",
            r"C:\target\crate.d: C:\work\ tree\#member\src\lib.rs C:\work\data:1.txt",
            "\nsecond: ignored\n",
        );
        let rule = first_make_dependency_rule(input).unwrap();
        assert_eq!(&input[rule.colon..rule.colon + 2], ": ");
        assert_eq!(
            parse_make_words(&input[rule.colon + 2..rule.insertion]).unwrap(),
            vec![r"C:\work tree\#member\src\lib.rs", r"C:\work\data:1.txt"]
        );
    }

    #[test]
    fn first_make_rule_preserves_separator_and_trims_only_trailing_padding() {
        let separator_only = "foo: ";
        assert_eq!(
            first_make_dependency_rule(separator_only)
                .unwrap()
                .insertion,
            separator_only.len(),
            "the separator's required space is not trailing dependency padding"
        );

        let extra_separator_padding = "foo:  ";
        assert_eq!(
            first_make_dependency_rule(extra_separator_padding)
                .unwrap()
                .insertion,
            "foo: ".len()
        );

        let padded_dependencies = "foo: dep \t";
        assert_eq!(
            first_make_dependency_rule(padded_dependencies)
                .unwrap()
                .insertion,
            "foo: dep".len()
        );
    }

    #[test]
    fn dep_info_codec_skips_env_record_containing_colon_space() {
        let input = concat!(
            "# env-dep:CFG=foo: bar\n",
            "artifact: src/lib.rs data/value.txt\n",
        );
        let rule = first_make_dependency_rule(input).unwrap();
        assert_eq!(
            &input[rule.colon - "artifact".len()..rule.colon],
            "artifact"
        );
        assert_eq!(
            parse_dep_info_dependencies(input).unwrap(),
            vec![PathBuf::from("src/lib.rs"), PathBuf::from("data/value.txt")]
        );
    }

    #[test]
    fn relocated_dep_info_uses_unambiguous_consumer_absolute_paths() {
        let files = &[
            ("kache.toml", "extra_inputs = [\"data/**/*.json\"]"),
            ("data/q.json", "v1"),
        ];
        let (dir_a, src_a) = crate_fixture(files);
        let (dir_b, src_b) = crate_fixture(files);
        let snapshot_a = snap(&src_a);
        let snapshot_b = snap(&src_b);
        assert_eq!(snapshot_a.digest, snapshot_b.digest);

        let dep_a = dir_a.path().join("crate.d");
        let dep_b = dir_b.path().join("crate.d");
        std::fs::write(&dep_a, "artifact: src/lib.rs\n").unwrap();
        std::fs::write(&dep_b, "artifact: src/lib.rs\n").unwrap();
        snapshot_a.merge_into_dep_info(&dep_a).unwrap();
        snapshot_b.merge_into_dep_info(&dep_b).unwrap();
        let output_a = std::fs::read_to_string(&dep_a).unwrap();
        let output_b = std::fs::read_to_string(&dep_b).unwrap();
        let dependencies_a = parse_dep_info_dependencies(&output_a).unwrap();
        let dependencies_b = parse_dep_info_dependencies(&output_b).unwrap();

        assert_ne!(output_a, output_b);
        assert!(dependencies_a.contains(&dir_a.path().join("data/q.json")));
        assert!(dependencies_a.contains(&dir_a.path().join("kache.toml")));
        assert!(dependencies_b.contains(&dir_b.path().join("data/q.json")));
        assert!(dependencies_b.contains(&dir_b.path().join("kache.toml")));
        assert!(
            !dependencies_a
                .iter()
                .any(|path| path.starts_with(dir_b.path()))
        );
        assert!(
            !dependencies_b
                .iter()
                .any(|path| path.starts_with(dir_a.path()))
        );
    }

    #[test]
    fn unparseable_file_folds_opaque_and_rekeys_on_edit() {
        let (d, src) = crate_fixture(&[("kache.toml", "this is = not valid toml [[[")]);
        let before = dig(&src).expect("broken config folds opaque, never silently ignored");
        std::fs::write(d.path().join("kache.toml"), "still = broken ]]]").unwrap();
        let after = dig(&src).unwrap();
        assert_ne!(before, after);
    }

    #[test]
    fn stray_key_is_rejected_as_unparseable() {
        // `deny_unknown_fields`: a non-extra_inputs key is a loud parse error,
        // folded opaque rather than silently honored.
        let (_d, src) =
            crate_fixture(&[("kache.toml", "extra_inputs = []\nlocal_store = \"/tmp\"")]);
        assert!(dig(&src).is_some());
    }

    #[test]
    fn content_swap_between_matched_files_rekeys() {
        // CARDINAL-SIN GUARD. Two files matched by one glob; swapping their
        // CONTENTS (same filenames, identical content multiset) must re-key —
        // the filename->content binding is load-bearing (sqlx migration order,
        // several include_str! sites under one glob). A path-blind content
        // multiset would alias these two states and serve a stale artifact.
        let (d, src) = crate_fixture(&[
            ("kache.toml", "extra_inputs = [\"migrations/**/*.sql\"]"),
            ("migrations/0001_init.sql", "CREATE A;"),
            ("migrations/0002_add.sql", "CREATE B;"),
        ]);
        let before = dig(&src).expect("two matched files fold a digest");
        std::fs::write(d.path().join("migrations/0001_init.sql"), "CREATE B;").unwrap();
        std::fs::write(d.path().join("migrations/0002_add.sql"), "CREATE A;").unwrap();
        let after = dig(&src).expect("still folds after swap");
        assert_ne!(
            before, after,
            "content swap between matched files must re-key (false-hit guard)"
        );
    }

    #[test]
    fn metachar_dir_name_still_enumerates() {
        // A directory literally named `gen[1]`, declared as a bare dir, must
        // enumerate its files: the reshape escapes the literal `[`/`]` so glob
        // doesn't read them as a char class and silently fold nothing.
        let (d, src) = crate_fixture(&[
            ("kache.toml", "extra_inputs = [\"gen[1]\"]"),
            ("gen[1]/data.bin", "v1"),
        ]);
        let before = dig(&src).expect("metachar-named dir folds its files");
        std::fs::write(d.path().join("gen[1]/data.bin"), "v2").unwrap();
        let after = dig(&src).expect("still folds");
        assert_ne!(
            before, after,
            "a file inside a metachar-named dir must re-key (false-hit guard)"
        );
    }

    #[test]
    fn all_rejected_patterns_fold_distinct_from_no_config_and_rekey() {
        // A non-empty declaration whose patterns are ALL rejected (the `\x1f`
        // separator is the only rejection now) must NOT collapse to the
        // unconfigured key (None) — that silently re-opens the false hit — and
        // editing the declaration must re-key.
        let (d, src) = crate_fixture(&[("kache.toml", "extra_inputs = [\"\\u001Fa\"]")]);
        let folded = dig(&src).expect("all-rejected declaration still folds, never None");

        let (_n, none_src) = crate_fixture(&[]);
        assert!(
            dig(&none_src).is_none(),
            "no-config baseline is None (opt-out)"
        );

        std::fs::write(d.path().join("kache.toml"), "extra_inputs = [\"\\u001Fb\"]").unwrap();
        let after = dig(&src).expect("still folds after edit");
        assert_ne!(folded, after, "editing a rejected declaration must re-key");
    }

    #[test]
    fn empty_list_stays_distinct_from_all_rejected() {
        // `extra_inputs = []` is the explicit opt-out (None, byte-identical to
        // no file); a non-empty all-rejected list (`\x1f`) folds Some. They
        // must differ.
        let (_e, empty) = crate_fixture(&[("kache.toml", "extra_inputs = []")]);
        let (_r, rejected) = crate_fixture(&[("kache.toml", "extra_inputs = [\"\\u001Fx\"]")]);
        assert_eq!(dig(&empty), None);
        assert!(dig(&rejected).is_some());
    }

    #[test]
    fn control_separator_in_pattern_is_rejected() {
        // A `\x1f` (the fold separator) in a pattern can't be folded
        // unambiguously, so normalize_pattern drops it.
        let (d, _src) = crate_fixture(&[]);
        let root = d.path();
        assert!(normalize_pattern("x", root, "a\u{1f}b").is_none());
        assert!(normalize_pattern("x", root, ".sqlx/**/*.json").is_some());
    }

    #[test]
    fn non_utf8_config_folds_opaque_and_rekeys() {
        // A binary/corrupt kache.toml must fold opaque (never silently ignored
        // as if absent), and any edit must re-key.
        let (d, src) = crate_fixture(&[]);
        std::fs::write(d.path().join("kache.toml"), b"\xff\xfe extra_inputs").unwrap();
        let before = dig(&src).expect("non-utf8 config folds opaque, never None");
        std::fs::write(d.path().join("kache.toml"), b"\xff\xfe extra_input").unwrap();
        let after = dig(&src).expect("still folds");
        assert_ne!(before, after);
    }

    #[test]
    fn invalid_glob_pattern_does_not_abort_other_patterns() {
        // `a[b` survives normalization but is an invalid glob; it must warn +
        // skip without dropping a sibling valid pattern's files.
        let (d, src) = crate_fixture(&[
            (
                "kache.toml",
                "extra_inputs = [\"a[b\", \".sqlx/**/*.json\"]",
            ),
            (".sqlx/q.json", "v1"),
        ]);
        let before = dig(&src).expect("valid pattern still folds despite a bad sibling");
        std::fs::write(d.path().join(".sqlx/q.json"), "v2").unwrap();
        let after = dig(&src).unwrap();
        assert_ne!(before, after, "the valid pattern's file still re-keys");
    }

    #[test]
    fn duplicate_pattern_folds_same_as_single() {
        // pattern-level dedup: a repeated pattern must not change the digest.
        let (_d1, s1) = crate_fixture(&[
            (
                "kache.toml",
                "extra_inputs = [\".sqlx/**/*\", \".sqlx/**/*\"]",
            ),
            (".sqlx/q.json", "v1"),
        ]);
        let (_d2, s2) = crate_fixture(&[
            ("kache.toml", "extra_inputs = [\".sqlx/**/*\"]"),
            (".sqlx/q.json", "v1"),
        ]);
        assert_eq!(dig(&s1), dig(&s2));
    }

    #[test]
    fn overlapping_patterns_are_order_independent() {
        // Two distinct patterns matching the same file: declaration order must
        // not change the digest (sorted pattern set + deduped matched files).
        let files: &[(&str, &str)] = &[
            (
                "kache.toml",
                "extra_inputs = [\".sqlx/**/*\", \".sqlx/q.json\"]",
            ),
            (".sqlx/q.json", "v1"),
        ];
        let files_rev: &[(&str, &str)] = &[
            (
                "kache.toml",
                "extra_inputs = [\".sqlx/q.json\", \".sqlx/**/*\"]",
            ),
            (".sqlx/q.json", "v1"),
        ];
        let (_d1, s1) = crate_fixture(files);
        let (_d2, s2) = crate_fixture(files_rev);
        assert_eq!(dig(&s1), dig(&s2));
    }

    #[test]
    fn cc_style_c_source_folds_extra_inputs() {
        // The cc seam passes a C source path; crate-dir resolution and folding
        // are family-agnostic, so a co-located kache.toml applies to a cc-rs
        // crate (e.g. a generated header) just as to a Rust one.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("Cargo.toml"), "[package]\nname = \"x\"\n").unwrap();
        std::fs::write(root.join("kache.toml"), "extra_inputs = [\"include/*.h\"]").unwrap();
        std::fs::create_dir_all(root.join("include")).unwrap();
        std::fs::write(root.join("include/api.h"), "v1").unwrap();
        let csrc = root.join("src/ffi.c");
        std::fs::create_dir_all(csrc.parent().unwrap()).unwrap();
        std::fs::write(&csrc, "/* c */\n").unwrap();

        let fh = FileHasher::new();
        let before = digest(Some(&csrc), "x", true, &fh).expect("C source folds extra inputs");
        std::fs::write(root.join("include/api.h"), "v2").unwrap();
        let fh2 = FileHasher::new();
        let after = digest(Some(&csrc), "x", true, &fh2).unwrap();
        assert_ne!(
            before, after,
            "editing a declared header must re-key the cc crate"
        );
    }

    #[cfg(unix)]
    #[test]
    fn strict_snapshot_fails_when_a_matched_input_cannot_be_hashed() {
        use std::os::unix::fs::PermissionsExt;

        let (dir, src) = crate_fixture(&[
            ("kache.toml", "extra_inputs = [\"data/**/*\"]"),
            ("data/secret.bin", "v1"),
        ]);
        let input = dir.path().join("data/secret.bin");
        std::fs::set_permissions(&input, std::fs::Permissions::from_mode(0o000)).unwrap();
        if std::fs::read(&input).is_ok() {
            std::fs::set_permissions(&input, std::fs::Permissions::from_mode(0o644)).unwrap();
            return;
        }

        let result = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new());
        std::fs::set_permissions(&input, std::fs::Permissions::from_mode(0o644)).unwrap();
        let error = result.expect_err("strict hashing failures must propagate");
        assert!(
            format!("{error:#}").contains("hashing active extra_inputs dependency"),
            "{error:#}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn unreadable_file_folds_sentinel_distinct_from_absent() {
        use std::os::unix::fs::PermissionsExt;
        let (d, src) = crate_fixture(&[
            ("kache.toml", "extra_inputs = [\"data/**/*\"]"),
            ("data/secret.bin", "v1"),
        ]);
        let readable = dig(&src).expect("folds the readable file");

        let p = d.path().join("data/secret.bin");
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o000)).unwrap();
        // Running as root defeats chmod 000 — skip rather than assert falsely.
        if std::fs::read(&p).is_ok() {
            return;
        }
        let unreadable = dig(&src).expect("unreadable file still folds a sentinel");
        assert_ne!(readable, unreadable, "unreadable must differ from readable");

        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).unwrap();
        std::fs::remove_file(&p).unwrap();
        let absent = dig(&src).expect("zero matches still folds the pattern set");
        assert_ne!(
            unreadable, absent,
            "unreadable must not alias absent (false-hit guard)"
        );
    }
}
