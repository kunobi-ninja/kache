//! Cargo's successful JSON stream identifies units used by an observed command.
//!
//! Receipts are local evidence, never a replacement for Cargo's freshness
//! rules. Changed inputs, unknown layouts and busy roots retain all outputs.
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime};

use crate::config::Config;
use crate::unit_prune::{self, Pruned, Unit};

const SCHEMA: u32 = 1;
const MAX_ENTRIES: usize = 32_768;
const MAX_RECEIPT: u64 = 16 << 20;
const MAX_INPUT_BYTES: u64 = 128 << 20;
const INVALID_STREAM: &[u8] = b"\0kache-invalid-cargo-stream\n";

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(crate) struct Plan {
    pub(crate) status: String,
    pub(crate) units: usize,
    pub(crate) bytes: u64,
    pub(crate) protected: usize,
    pub(crate) unknown: usize,
    pub(crate) command: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Stamp {
    len: u64,
    modified: SystemTime,
    digest: Option<String>,
}

type Snapshot = BTreeMap<PathBuf, Stamp>;
type StagedParts = Vec<(PathBuf, PathBuf)>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObservedUnit {
    fingerprint: PathBuf,
    parts: Vec<PathBuf>,
    snapshot: Snapshot,
    live: bool,
    known: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    schema: u32,
    target: PathBuf,
    workspace: PathBuf,
    device: u64,
    inode: u64,
    recorded: SystemTime,
    command: Vec<String>,
    environment: String,
    inputs: Snapshot,
    units: Vec<ObservedUnit>,
}

/// Prepared before launch; recording failures never change Cargo's result.
pub(crate) struct Capture {
    config: Config,
    workspace: PathBuf,
    target: PathBuf,
    before: Snapshot,
    started: SystemTime,
    command: Vec<String>,
    environment: String,
    human: bool,
    complete: bool,
    observed_bytes: usize,
    invalid: bool,
    artifacts: Vec<serde_json::Value>,
    scripts: Vec<serde_json::Value>,
}

impl Capture {
    pub(crate) fn prepare(command: &Command, _cargo: &Path, config: &Config) -> Option<Self> {
        if !config.target_liveness {
            return None;
        }
        match Self::prepare_inner(command, config) {
            Ok(capture) => Some(capture),
            Err(error) => {
                tracing::debug!(%error, "Cargo liveness capture unavailable");
                None
            }
        }
    }

    fn prepare_inner(command: &Command, config: &Config) -> Result<Self> {
        let args: Vec<String> = command
            .get_args()
            .map(|a| a.to_str().map(str::to_owned))
            .collect::<Option<_>>()
            .context("non-Unicode Cargo arguments")?;
        let subcommand = cargo_subcommand(&args);
        ensure!(
            matches!(subcommand, Some("build" | "check")),
            "unsupported Cargo command"
        );
        ensure!(
            !args.iter().any(|a| a == "--"),
            "compiler argument passthrough"
        );
        let format = argument(&args, "--message-format");
        let human = format.is_none();
        ensure!(
            format.is_none_or(|f| f.split(',').all(|p| matches!(
                p,
                "json"
                    | "json-render-diagnostics"
                    | "json-diagnostic-short"
                    | "json-diagnostic-rendered-ansi"
            ))),
            "unsupported message format"
        );
        let cwd = command
            .get_current_dir()
            .map(Path::to_path_buf)
            .unwrap_or(std::env::current_dir()?);
        let manifest = argument(&args, "--manifest-path").map(|p| absolute(&cwd, Path::new(p)));
        let start = manifest.as_deref().and_then(Path::parent).unwrap_or(&cwd);
        let workspace = workspace_root(start)?.canonicalize()?;
        let env: BTreeMap<_, _> = command.get_envs().collect();
        let target_arg = argument(&args, "--target-dir")
            .map(PathBuf::from)
            .or_else(|| {
                env.get(std::ffi::OsStr::new("CARGO_TARGET_DIR"))
                    .and_then(|v| *v)
                    .map(PathBuf::from)
            })
            .or_else(|| std::env::var_os("CARGO_TARGET_DIR").map(PathBuf::from));
        let target = target_arg
            .map(|p| absolute(&cwd, &p))
            .unwrap_or_else(|| workspace.join("target"));
        let target = if target.exists() {
            ensure!(
                !std::fs::symlink_metadata(&target)?.is_symlink(),
                "symlink target root"
            );
            target.canonicalize()?
        } else {
            let parent = target.parent().context("target parent")?.canonicalize()?;
            parent.join(target.file_name().context("target name")?)
        };
        // A configured split build-dir requires separate receipts. Keep it
        // unknown until every intermediate root can be locked together.
        ensure!(
            std::env::var_os("CARGO_BUILD_BUILD_DIR").is_none(),
            "split build directory"
        );
        let before = source_snapshot(&workspace, &target, &config.cache_dir)?;
        Ok(Self {
            config: config.clone(),
            workspace,
            target,
            before,
            started: SystemTime::now(),
            command: args,
            environment: environment_digest(),
            human,
            complete: false,
            observed_bytes: 0,
            invalid: false,
            artifacts: Vec::new(),
            scripts: Vec::new(),
        })
    }

    pub(crate) fn human_output(&self) -> bool {
        self.human
    }

    /// Recognize Cargo records, retaining unknown output in the caller's stream.
    pub(crate) fn observe(&mut self, line: &[u8]) -> bool {
        if line == INVALID_STREAM {
            self.invalid = true;
            return false;
        }
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(line) else {
            self.invalid = true;
            return false;
        };
        if matches!(
            value["reason"].as_str(),
            Some("compiler-artifact" | "build-script-executed")
        ) {
            self.observed_bytes = self.observed_bytes.saturating_add(line.len());
            if self.observed_bytes as u64 > MAX_RECEIPT {
                self.invalid = true;
                return true;
            }
        }
        match value["reason"].as_str() {
            Some("compiler-artifact") => {
                if self.artifacts.len() >= MAX_ENTRIES {
                    self.invalid = true;
                } else {
                    self.artifacts.push(value);
                }
            }
            Some("build-script-executed") => {
                if self.scripts.len() >= MAX_ENTRIES {
                    self.invalid = true;
                } else {
                    self.scripts.push(value);
                }
            }
            Some("compiler-message") => {}
            Some("build-finished") => {
                self.complete = value["success"].as_bool() == Some(true);
            }
            _ => {
                self.invalid = true;
                return false;
            }
        }
        true
    }

    pub(crate) fn finish(self, success: bool) -> Result<()> {
        if !success || !self.complete || self.invalid || self.artifacts.is_empty() {
            return Ok(());
        }
        ensure!(
            self.before == source_snapshot(&self.workspace, &self.target, &self.config.cache_dir)?,
            "workspace changed during Cargo command"
        );
        let target = checked_root(&self.target, &self.workspace)?;
        let identity = crate::machine::directory_identity(&target).context("target identity")?;
        let profiles = unit_prune::profiles(&target);
        ensure!(!profiles.is_empty(), "no supported Cargo profiles");
        let mut held = Vec::new();
        for profile in &profiles {
            held.push(unit_prune::hold(profile).context("Cargo profile busy while recording")?);
        }
        // Freeze ownership once under Cargo's locks. Exact files use direct
        // lookup; outputs under a unit directory use ancestor lookup.
        let mut inventory = Vec::new();
        let mut file_owners: BTreeMap<PathBuf, BTreeSet<PathBuf>> = BTreeMap::new();
        let mut directory_owners: BTreeMap<PathBuf, BTreeSet<PathBuf>> = BTreeMap::new();
        for profile in &profiles {
            let outputs = unit_prune::outputs(profile);
            for unit in unit_prune::units(profile) {
                ensure!(
                    inventory.len() < MAX_ENTRIES,
                    "too many observed Cargo units"
                );
                let fingerprint = unit.fingerprint();
                let parts: Vec<_> = unit
                    .parts(&outputs)
                    .into_iter()
                    .filter(|p| p.exists())
                    .collect();
                for part in &parts {
                    file_owners
                        .entry(part.clone())
                        .or_default()
                        .insert(fingerprint.clone());
                    if part.is_dir() {
                        directory_owners
                            .entry(part.clone())
                            .or_default()
                            .insert(fingerprint.clone());
                    }
                }
                inventory.push((unit, fingerprint, parts));
            }
        }
        // A JSON artifact outside this root signals target/build-dir overrides
        // we cannot prove. Never publish a partial live graph.
        for artifact in &self.artifacts {
            for file in artifact["filenames"]
                .as_array()
                .context("missing filenames")?
            {
                ensure!(
                    absolute(
                        &self.workspace,
                        Path::new(file.as_str().context("invalid filename")?)
                    )
                    .canonicalize()?
                    .starts_with(&target),
                    "artifact outside the observed target"
                );
            }
        }
        let mut live = BTreeSet::new();
        let mut observed_profiles = BTreeSet::new();
        let mut protected_packages = BTreeSet::new();
        let mut inputs = self.before.clone();
        for artifact in &self.artifacts {
            let manifest = PathBuf::from(
                artifact["manifest_path"]
                    .as_str()
                    .context("missing package manifest")?,
            );
            insert_file(&mut inputs, &manifest, true)?;
            let package = package_name(&manifest)?;
            let files = artifact["filenames"]
                .as_array()
                .context("missing filenames")?;
            let mut mapped = false;
            for file in files {
                let file = absolute(
                    &self.workspace,
                    Path::new(file.as_str().context("invalid filename")?),
                );
                let file = file.canonicalize()?;
                for profile in &profiles {
                    if file.starts_with(profile) {
                        observed_profiles.insert(profile.clone());
                    }
                }
                if let Some(owners) = file_owners.get(&file) {
                    live.extend(owners.iter().cloned());
                    mapped = true;
                }
                for ancestor in file.ancestors() {
                    if let Some(owners) = directory_owners.get(ancestor) {
                        live.extend(owners.iter().cloned());
                        mapped = true;
                    }
                }
            }
            if needs_package_protection(mapped, &artifact["target"]["kind"]) {
                protected_packages.insert(package);
            }
        }
        for script in &self.scripts {
            let out = PathBuf::from(
                script["out_dir"]
                    .as_str()
                    .context("missing script out_dir")?,
            );
            let out = out.canonicalize()?;
            ensure!(out.starts_with(&target), "build script outside target");
            let artifact = self
                .artifacts
                .iter()
                .find(|a| a["package_id"] == script["package_id"])
                .context("build script package was not reported")?;
            let manifest = PathBuf::from(
                artifact["manifest_path"]
                    .as_str()
                    .context("script manifest")?,
            );
            let package = package_name(&manifest)?;
            ensure!(
                crate::cargo_layout::out_dir_unit_name(&out, &package).is_some(),
                "unknown build-script layout"
            );
            protected_packages.insert(package);
            let stdout = crate::cargo_layout::build_script_stdout(&out).context("script stdout")?;
            insert_file(&mut inputs, &stdout, true)?;
            // Cargo can watch the whole package when a build script supplies
            // no rerun directives. Keep a conservative package inventory too.
            walk(
                &mut inputs,
                manifest.parent().context("script package directory")?,
                &[target.clone(), self.config.cache_dir.clone()],
                true,
            )?;
            for line in std::fs::read_to_string(stdout)?.lines() {
                let line = line
                    .strip_prefix("cargo::")
                    .or_else(|| line.strip_prefix("cargo:"));
                if let Some(path) = line.and_then(|l| l.strip_prefix("rerun-if-changed=")) {
                    let path = absolute(
                        manifest.parent().context("script package directory")?,
                        Path::new(path),
                    );
                    walk(&mut inputs, &path, &[], true)?;
                }
            }
        }
        let mut observed = Vec::with_capacity(inventory.len());
        for (unit, fingerprint, mut parts) in inventory {
            // A modern unit owns its fingerprint as a child. Rename the
            // parent once rather than stage overlapping paths.
            retain_owned_parts(&unit, &mut parts);
            let (package, mut known) = known_library(&unit)?;
            let profile = match &unit {
                Unit::Shared { profile, .. } => profile.as_path(),
                Unit::PerUnit { dir } => dir
                    .parent()
                    .and_then(Path::parent)
                    .and_then(Path::parent)
                    .context("modern unit profile")?,
            };
            known &= observed_profiles.contains(profile);
            let is_live = live.contains(&fingerprint) || protected_packages.contains(&package);
            let snapshot = output_snapshot(&parts, &fingerprint)?;
            if is_live {
                add_dep_inputs(&snapshot, &self.workspace, &target, &mut inputs)?;
            }
            observed.push(ObservedUnit {
                fingerprint,
                parts,
                snapshot,
                live: is_live,
                known,
            });
        }
        validate_discovered_inputs(
            &inputs,
            &self.before,
            &self.workspace,
            &target,
            self.started,
        )?;
        let mut receipt = Receipt {
            schema: SCHEMA,
            target,
            workspace: self.workspace,
            device: identity.device,
            inode: identity.inode,
            recorded: SystemTime::now(),
            command: ["kache".to_string(), "cargo".to_string()]
                .into_iter()
                .chain(self.command)
                .collect(),
            environment: self.environment,
            inputs,
            units: observed,
        };
        // Other successfully observed command variants remain protected while
        // their shared source state is unchanged.
        if let Ok(previous) = read_receipt(&self.config, &receipt.target) {
            preserve_previous_live_units(&mut receipt, &previous);
        }
        let record = receipt_path(&self.config, &receipt.target);
        std::fs::create_dir_all(record.parent().context("receipt parent")?)?;
        let bytes = serde_json::to_vec(&receipt)?;
        ensure!(bytes.len() as u64 <= MAX_RECEIPT, "receipt too large");
        crate::atomic::atomic_replace(&record, &bytes)?;
        crate::store::Store::open(&self.config)?
            .remember_target_root(&receipt.target, &receipt.workspace)
    }
}

fn needs_package_protection(mapped: bool, kinds: &serde_json::Value) -> bool {
    !mapped
        || kinds
            .as_array()
            .is_some_and(|k| k.iter().any(|v| v == "custom-build" || v == "bin"))
}

fn preserve_previous_live_units(receipt: &mut Receipt, previous: &Receipt) {
    if previous.inputs != receipt.inputs || previous.environment != receipt.environment {
        return;
    }
    let live: BTreeMap<_, _> = previous
        .units
        .iter()
        .filter(|u| u.live)
        .map(|u| (&u.fingerprint, &u.snapshot))
        .collect();
    for unit in &mut receipt.units {
        unit.live |= live
            .get(&unit.fingerprint)
            .is_some_and(|s| **s == unit.snapshot);
    }
}

fn retain_owned_parts(unit: &Unit, parts: &mut Vec<PathBuf>) {
    parts.retain(|p| !matches!(unit, Unit::PerUnit { dir } if p != dir));
}

/// Inputs first observed after Cargo exits need evidence that they predate
/// compilation. Generated target outputs and the prechecked workspace are
/// handled by their own inventory checks.
fn validate_discovered_inputs(
    inputs: &Snapshot,
    before: &Snapshot,
    workspace: &Path,
    target: &Path,
    started: SystemTime,
) -> Result<()> {
    for (path, stamp) in inputs {
        if !path.starts_with(workspace) && !before.contains_key(path) {
            ensure!(
                path.starts_with(target) || stamp.modified <= started,
                "external input changed during compilation"
            );
        }
    }
    Ok(())
}

fn cargo_subcommand(args: &[String]) -> Option<&str> {
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        if arg == "--config" || arg == "--color" || arg == "--manifest-path" {
            args.next();
        } else if !arg.starts_with('-') && !arg.starts_with('+') {
            return Some(arg.as_str());
        }
    }
    None
}

fn argument<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    args.iter().enumerate().find_map(|(i, a)| {
        a.strip_prefix(&format!("{flag}=")).or_else(|| {
            (a == flag)
                .then(|| args.get(i + 1).map(String::as_str))
                .flatten()
        })
    })
}
fn absolute(cwd: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    }
}

fn workspace_root(start: &Path) -> Result<PathBuf> {
    let mut package = None;
    for dir in start.ancestors() {
        let manifest = dir.join("Cargo.toml");
        if !manifest.is_file() {
            continue;
        }
        let value: toml::Value = toml::from_str(&std::fs::read_to_string(&manifest)?)?;
        if value.get("workspace").is_some() {
            return Ok(dir.to_path_buf());
        }
        if package.is_none() {
            package = Some(dir.to_path_buf());
        }
    }
    package.context("no Cargo workspace")
}

fn package_name(manifest: &Path) -> Result<String> {
    let value: toml::Value = toml::from_str(&std::fs::read_to_string(manifest)?)?;
    value
        .get("package")
        .and_then(|p| p.get("name"))
        .and_then(toml::Value::as_str)
        .map(str::to_string)
        .context("missing package name")
}

fn environment_digest() -> String {
    environment_digest_from(std::env::vars_os())
}

fn environment_digest_from(
    vars: impl IntoIterator<Item = (std::ffi::OsString, std::ffi::OsString)>,
) -> String {
    let mut hasher = blake3::Hasher::new();
    let mut vars: Vec<_> = vars
        .into_iter()
        .filter(|(k, _)| {
            let k = k.to_string_lossy();
            (!k.starts_with("KACHE_") || k == "KACHE_REAL_CARGO")
                && !matches!(
                    k.as_ref(),
                    "PWD" | "OLDPWD" | "SHLVL" | "_" | "TERM" | "COLORTERM"
                )
        })
        .collect();
    vars.sort();
    for (key, value) in vars {
        hasher.update(key.as_encoded_bytes());
        hasher.update(&[0]);
        hasher.update(value.as_encoded_bytes());
        hasher.update(&[0]);
    }
    hasher.finalize().to_hex().to_string()
}

fn insert_file(snapshot: &mut Snapshot, path: &Path, contents: bool) -> Result<()> {
    ensure!(snapshot.len() < MAX_ENTRIES, "input inventory too large");
    let metadata = std::fs::symlink_metadata(path)?;
    ensure!(!metadata.is_symlink(), "symlink in evidence");
    let digest = if contents && metadata.is_file() {
        ensure!(metadata.len() <= MAX_RECEIPT, "evidence file too large");
        Some(blake3::hash(&std::fs::read(path)?).to_hex().to_string())
    } else {
        None
    };
    snapshot.insert(
        path.to_path_buf(),
        Stamp {
            len: metadata.len(),
            modified: metadata.modified()?,
            digest,
        },
    );
    Ok(())
}

fn walk(snapshot: &mut Snapshot, path: &Path, exclude: &[PathBuf], contents: bool) -> Result<()> {
    walk_bounded(snapshot, path, exclude, contents, &mut 0)
}

fn walk_bounded(
    snapshot: &mut Snapshot,
    path: &Path,
    exclude: &[PathBuf],
    contents: bool,
    budget: &mut u64,
) -> Result<()> {
    if exclude.iter().any(|p| path == p) {
        return Ok(());
    }
    let metadata = std::fs::symlink_metadata(path)?;
    ensure!(!metadata.is_symlink(), "symlink in evidence tree");
    if metadata.is_dir() {
        insert_file(snapshot, path, false)?;
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            if entry.file_name() == ".git" {
                continue;
            }
            walk_bounded(snapshot, &entry.path(), exclude, contents, budget)?;
        }
    } else {
        ensure!(metadata.is_file(), "special file in evidence tree");
        if contents {
            *budget = budget
                .checked_add(metadata.len())
                .context("input byte overflow")?;
            ensure!(
                *budget <= MAX_INPUT_BYTES,
                "input inventory exceeds 128 MiB"
            );
        }
        insert_file(snapshot, path, contents)?;
    }
    Ok(())
}

fn source_snapshot(workspace: &Path, target: &Path, cache: &Path) -> Result<Snapshot> {
    let mut snapshot = Snapshot::new();
    walk(
        &mut snapshot,
        workspace,
        &[target.to_path_buf(), cache.to_path_buf()],
        true,
    )?;
    for dir in workspace.ancestors() {
        for name in [".cargo/config", ".cargo/config.toml"] {
            let path = dir.join(name);
            if path.exists() {
                insert_file(&mut snapshot, &path, true)?;
            }
        }
    }
    let cargo_home = std::env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|p| PathBuf::from(p).join(".cargo")));
    if let Some(home) = cargo_home {
        for name in ["config", "config.toml"] {
            let path = home.join(name);
            if path.exists() {
                insert_file(&mut snapshot, &path, true)?;
            }
        }
    }
    let rustup = std::env::var_os("RUSTUP_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|p| PathBuf::from(p).join(".rustup")));
    if let Some(home) = rustup {
        let settings = home.join("settings.toml");
        if settings.exists() {
            insert_file(&mut snapshot, &settings, true)?;
        }
        if let Ok(toolchains) = std::fs::read_dir(home.join("toolchains")) {
            for toolchain in toolchains {
                let toolchain = toolchain?;
                for program in ["rustc", "cargo", "rustc.exe", "cargo.exe"] {
                    let path = toolchain.path().join("bin").join(program);
                    if path.exists() {
                        insert_file(&mut snapshot, &path, false)?;
                    }
                }
            }
        }
    }
    for (variable, default) in [
        ("CARGO", Some("cargo")),
        ("RUSTC", Some("rustc")),
        ("KACHE_REAL_CARGO", None),
    ] {
        let Some(program) = std::env::var(variable)
            .ok()
            .or_else(|| default.map(str::to_owned))
        else {
            continue;
        };
        if let Some(path) = crate::compiler::resolve_program_on_path(&program) {
            insert_file(&mut snapshot, &path.canonicalize()?, false)?;
        }
    }
    Ok(snapshot)
}

fn output_snapshot(parts: &[PathBuf], fingerprint: &Path) -> Result<Snapshot> {
    let mut snapshot = Snapshot::new();
    for part in parts {
        walk(&mut snapshot, part, &[], false)?;
    }
    let mut fp = Snapshot::new();
    walk(&mut fp, fingerprint, &[], true)?;
    snapshot.extend(fp);
    Ok(snapshot)
}

fn known_library(unit: &Unit) -> Result<(String, bool)> {
    let package = match unit {
        Unit::Shared { package, .. } => package.clone(),
        Unit::PerUnit { dir } => dir
            .parent()
            .and_then(Path::file_name)
            .context("unit package")?
            .to_string_lossy()
            .into_owned(),
    };
    let expected = format!("lib-{}.json", package.replace('-', "_"));
    let entries: Vec<_> = std::fs::read_dir(unit.fingerprint())?.collect::<std::io::Result<_>>()?;
    let known = entries.iter().any(|e| e.file_name() == expected.as_str())
        && entries
            .iter()
            .filter(|e| e.path().extension().is_some_and(|x| x == "json"))
            .count()
            == 1;
    Ok((package, known))
}

fn add_dep_inputs(
    outputs: &Snapshot,
    cwd: &Path,
    target: &Path,
    inputs: &mut Snapshot,
) -> Result<()> {
    for file in outputs
        .keys()
        .filter(|p| p.extension().is_some_and(|x| x == "d"))
    {
        let text = std::fs::read_to_string(file)?;
        for path in crate::extra_inputs::parse_dep_info_dependencies(&text)? {
            let path = absolute(cwd, &path);
            if !path.starts_with(target) {
                insert_file(inputs, &path, true)?;
            }
        }
    }
    Ok(())
}

fn checked_root(target: &Path, workspace: &Path) -> Result<PathBuf> {
    ensure!(
        !std::fs::symlink_metadata(target)?.is_symlink(),
        "symlink target root"
    );
    ensure!(
        crate::machine::target_root_is_safe(target, workspace),
        "unsafe target root"
    );
    let canonical = target.canonicalize()?;
    // Parent aliases such as /tmp are harmless, but intermediate symlinks
    // inside the target are rejected by the inventory walkers.
    Ok(canonical)
}

fn receipt_path(config: &Config, target: &Path) -> PathBuf {
    let digest = blake3::hash(target.as_os_str().as_encoded_bytes()).to_hex();
    config
        .cache_dir
        .join("target-liveness")
        .join(format!("{digest}.json"))
}

fn read_receipt(config: &Config, target: &Path) -> Result<Receipt> {
    let path = receipt_path(config, target);
    let meta = match std::fs::symlink_metadata(&path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => bail!("no receipt yet"),
        Err(error) => return Err(error.into()),
    };
    ensure!(!meta.is_symlink(), "symlink receipt");
    ensure!(
        std::fs::metadata(&path)?.len() <= MAX_RECEIPT,
        "receipt too large"
    );
    let receipt: Receipt = serde_json::from_slice(&std::fs::read(path)?)?;
    ensure!(receipt.schema == SCHEMA, "unsupported receipt schema");
    Ok(receipt)
}

fn validated(config: &Config, target: &Path, workspace: &Path) -> Result<Receipt> {
    let target = checked_root(target, workspace)?;
    let receipt = read_receipt(config, &target)?;
    ensure!(
        receipt.target == target && receipt.workspace == workspace.canonicalize()?,
        "receipt root changed"
    );
    let identity = crate::machine::directory_identity(&target).context("missing target")?;
    ensure!(
        identity.device == receipt.device && identity.inode == receipt.inode,
        "target replaced"
    );
    ensure!(
        receipt.environment == environment_digest(),
        "Cargo environment changed"
    );
    let current = source_snapshot(&receipt.workspace, &target, &config.cache_dir)?;
    for (path, stamp) in &receipt.inputs {
        let now = if let Some(stamp) = current.get(path) {
            stamp.clone()
        } else {
            let mut one = Snapshot::new();
            insert_file(&mut one, path, stamp.digest.is_some())?;
            one.remove(path).context("missing input")?
        };
        ensure!(now == *stamp, "Cargo input changed: {}", path.display());
    }
    // Source additions/removals also change the graph or build-script inputs.
    ensure!(
        current
            .iter()
            .all(|(path, stamp)| receipt.inputs.get(path) == Some(stamp)),
        "workspace input set changed"
    );
    let inventory: BTreeMap<_, _> = unit_prune::profiles(&target)
        .iter()
        .flat_map(|profile| {
            let outputs = unit_prune::outputs(profile);
            unit_prune::units(profile).into_iter().map(move |unit| {
                let fingerprint = unit.fingerprint();
                let mut parts: Vec<_> = unit
                    .parts(&outputs)
                    .into_iter()
                    .filter(|p| p.exists())
                    .collect();
                retain_owned_parts(&unit, &mut parts);
                (fingerprint, parts)
            })
        })
        .collect();
    for unit in &receipt.units {
        // Already-cleaned candidates can disappear; a receipt may never
        // nominate arbitrary filesystem paths as unit ownership.
        if let Some(parts) = inventory.get(&unit.fingerprint) {
            ensure!(&unit.parts == parts, "unit ownership changed");
        } else {
            ensure!(
                !unit.live && unit.parts.iter().all(|p| !p.exists()),
                "unknown unit ownership"
            );
        }
        for part in &unit.parts {
            ensure!(
                !part
                    .components()
                    .any(|c| c == std::path::Component::ParentDir),
                "parent traversal in unit path"
            );
            reject_symlink_ancestors(part, &target)?;
        }
        ensure!(
            unit.fingerprint.starts_with(&target),
            "fingerprint escapes target"
        );
        ensure!(
            unit.parts
                .iter()
                .all(|p| p.starts_with(&target) && p != &target),
            "unit escapes target"
        );
        if unit.live {
            ensure!(
                output_snapshot(&unit.parts, &unit.fingerprint)? == unit.snapshot,
                "live output changed"
            );
        }
    }
    Ok(receipt)
}

fn reject_symlink_ancestors(part: &Path, target: &Path) -> Result<()> {
    for parent in part.ancestors().take_while(|p| *p != target) {
        if let Ok(metadata) = std::fs::symlink_metadata(parent) {
            ensure!(!metadata.is_symlink(), "symlink in unit path");
        }
    }
    Ok(())
}

fn eligible(unit: &ObservedUnit, window: Option<Duration>, now: SystemTime) -> bool {
    !unit.live
        && unit.known
        && window.is_none_or(|window| {
            unit.snapshot.values().all(|s| {
                now.duration_since(s.modified)
                    .is_ok_and(|age| age >= window)
            })
        })
}

pub(crate) fn preview(config: &Config, target: &Path, workspace: &Path) -> Plan {
    preview_window(config, target, workspace, None, SystemTime::now())
}

pub(crate) fn preview_window(
    config: &Config,
    target: &Path,
    workspace: &Path,
    window: Option<Duration>,
    now: SystemTime,
) -> Plan {
    let fallback = vec![
        "KACHE_TARGET_LIVENESS=1".into(),
        "kache".into(),
        "cargo".into(),
        "check".into(),
    ];
    let receipt = match validated(config, target, workspace) {
        Ok(receipt) => receipt,
        Err(error) => {
            return Plan {
                status: format!("unavailable: {error}"),
                command: fallback,
                ..Plan::default()
            };
        }
    };
    if crate::cli::target_in_use(target) {
        return Plan {
            status: "busy".into(),
            command: receipt.command,
            ..Plan::default()
        };
    }
    summarize_receipt(&receipt, window, now)
}

fn summarize_receipt(receipt: &Receipt, window: Option<Duration>, now: SystemTime) -> Plan {
    let mut plan = Plan {
        status: "ready".into(),
        command: receipt.command.clone(),
        ..Plan::default()
    };
    for unit in &receipt.units {
        if unit.live {
            plan.protected += 1;
        } else if !unit.known {
            plan.unknown += 1;
        } else if eligible(unit, window, now)
            && output_snapshot(&unit.parts, &unit.fingerprint).is_ok_and(|s| s == unit.snapshot)
        {
            plan.units += 1;
            plan.bytes = plan
                .bytes
                .saturating_add(unit.snapshot.values().map(|s| s.len).sum::<u64>());
        } else {
            plan.unknown += 1;
        }
    }
    plan
}

pub(crate) fn prune(
    config: &Config,
    target: &Path,
    workspace: &Path,
    window: Option<Duration>,
    now: SystemTime,
) -> Result<Pruned> {
    let Some(_reservation) = crate::target_use::try_exclusive(&config.cache_dir)? else {
        return Ok(Pruned::default());
    };
    ensure!(
        matches!(
            crate::cache_fs::classify(&crate::cache_fs::probe(target)),
            crate::cache_fs::CacheFsVerdict::Local
        ),
        "Cargo locks unavailable on this filesystem"
    );
    let receipt = validated(config, target, workspace)?;
    let mut held = Vec::new();
    for profile in unit_prune::profiles(target) {
        let Some(lock) = unit_prune::hold(&profile) else {
            return Ok(Pruned::default());
        };
        held.push(lock);
    }
    // Cargo may have finished between validation and acquiring its locks.
    let receipt_again = validated(config, target, workspace)?;
    ensure!(
        serde_json::to_vec(&receipt)? == serde_json::to_vec(&receipt_again)?,
        "receipt changed"
    );
    let mut pruned = protected_counts(&receipt);
    let mut removed = Vec::new();
    let mut staged_units: Vec<(&ObservedUnit, StagedParts)> = Vec::new();
    for unit in &receipt.units {
        if !eligible(unit, window, now)
            || !output_snapshot(&unit.parts, &unit.fingerprint).is_ok_and(|s| s == unit.snapshot)
        {
            continue;
        }
        let mut staged = Vec::new();
        for (index, part) in unit.parts.iter().enumerate() {
            let aside = part.with_file_name(format!(
                ".kache-unit-{}-{index}-{}",
                std::process::id(),
                blake3::hash(part.as_os_str().as_encoded_bytes()).to_hex()
            ));
            if aside.exists() || std::fs::rename(part, &aside).is_err() {
                rollback(&staged);
                for (_, earlier) in &staged_units {
                    rollback(earlier);
                }
                bail!("cannot stage obsolete unit");
            }
            staged.push((part.clone(), aside));
        }
        staged_units.push((unit, staged));
    }
    // Revalidate the batch once, keeping source and inventory work linear in
    // the number of units rather than repeating a workspace walk per unit.
    let unchanged =
        staged_batch_unchanged(validated(config, target, workspace).is_ok(), &staged_units);
    if !unchanged {
        for (_, staged) in &staged_units {
            rollback(staged);
        }
        bail!("Cargo inputs or staged outputs changed during cleanup");
    }
    if let Err(error) = delete_staged_units(&staged_units, &mut removed, &mut pruned) {
        log_removed(
            config,
            target,
            workspace,
            &removed,
            pruned,
            "cleanup-interrupted",
        )?;
        return Err(error);
    }
    log_removed(
        config,
        target,
        workspace,
        &removed,
        pruned,
        "outside-observed-cargo-live-set",
    )?;
    Ok(pruned)
}

fn staged_batch_unchanged(
    inputs_unchanged: bool,
    staged_units: &[(&ObservedUnit, StagedParts)],
) -> bool {
    inputs_unchanged
        && staged_units
            .iter()
            .all(|(unit, staged)| staged_snapshot(unit, staged).is_ok_and(|s| s == unit.snapshot))
}

fn protected_counts(receipt: &Receipt) -> Pruned {
    Pruned {
        used: receipt.units.iter().filter(|u| u.live).count(),
        ..Pruned::default()
    }
}

fn delete_staged_units(
    staged_units: &[(&ObservedUnit, StagedParts)],
    removed: &mut Vec<String>,
    pruned: &mut Pruned,
) -> Result<()> {
    let mut remaining = staged_units;
    while let Some(((unit, staged), later)) = remaining.split_first() {
        remaining = later;
        if let Err(error) = delete_staged(staged, removed) {
            for (_, later) in remaining {
                rollback(later);
            }
            return Err(error);
        }
        pruned.units += 1;
        pruned.bytes = pruned
            .bytes
            .saturating_add(unit.snapshot.values().map(|s| s.len).sum::<u64>());
    }
    Ok(())
}

fn staged_snapshot(unit: &ObservedUnit, staged: &[(PathBuf, PathBuf)]) -> Result<Snapshot> {
    let fingerprint = staged
        .iter()
        .find_map(|(original, aside)| {
            unit.fingerprint
                .strip_prefix(original)
                .ok()
                .map(|suffix| aside.join(suffix))
        })
        .context("unstaged fingerprint")?;
    let parts: Vec<_> = staged.iter().map(|(_, aside)| aside.clone()).collect();
    let snapshot = output_snapshot(&parts, &fingerprint)?;
    snapshot
        .into_iter()
        .map(|(path, stamp)| {
            let original = staged
                .iter()
                .find_map(|(original, aside)| {
                    path.strip_prefix(aside)
                        .ok()
                        .map(|suffix| original.join(suffix))
                })
                .context("unowned staged path")?;
            Ok((original, stamp))
        })
        .collect()
}

fn log_removed(
    config: &Config,
    target: &Path,
    workspace: &Path,
    removed: &[String],
    pruned: Pruned,
    reason: &str,
) -> Result<()> {
    if removed.is_empty() {
        return Ok(());
    }
    let event = crate::events::CleanupEvent::new(
        target.to_string_lossy().into_owned(),
        workspace.to_string_lossy().into_owned(),
        removed.to_vec(),
        pruned.units as u64,
        pruned.bytes,
        reason.into(),
    );
    crate::events::log_cleanup(&config.event_log_path(), &event)
}

fn delete_staged(staged: &[(PathBuf, PathBuf)], removed: &mut Vec<String>) -> Result<()> {
    for (index, (original, aside)) in staged.iter().enumerate() {
        let deleted = if aside.is_dir() {
            std::fs::remove_dir_all(aside)
        } else {
            std::fs::remove_file(aside)
        };
        if let Err(error) = deleted {
            rollback(&staged[index..]);
            return Err(error.into());
        }
        removed.push(original.to_string_lossy().into_owned());
    }
    Ok(())
}

fn rollback(staged: &[(PathBuf, PathBuf)]) {
    for (original, aside) in staged.iter().rev() {
        if let Err(error) = std::fs::rename(aside, original) {
            tracing::error!(?error, ?original, "restoring staged Cargo unit");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unit(live: bool, known: bool, seconds: u64) -> ObservedUnit {
        ObservedUnit {
            fingerprint: PathBuf::from("/target/debug/.fingerprint/pkg-0123456789abcdef"),
            parts: vec![],
            snapshot: [(
                PathBuf::from("output"),
                Stamp {
                    len: 7,
                    modified: SystemTime::UNIX_EPOCH + Duration::from_secs(seconds),
                    digest: None,
                },
            )]
            .into(),
            live,
            known,
        }
    }

    #[test]
    fn only_proved_unused_units_are_eligible_independent_of_access_time() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(20);
        assert!(eligible(&unit(false, true, 10), None, now));
        assert!(!eligible(&unit(true, true, 10), None, now));
        assert!(!eligible(&unit(false, false, 10), None, now));
        assert!(!eligible(&unit(true, false, 10), None, now));
        assert!(eligible(
            &unit(false, true, 10),
            Some(Duration::from_secs(10)),
            now
        ));
        assert!(!eligible(
            &unit(false, true, 11),
            Some(Duration::from_secs(10)),
            now
        ));
        assert!(!eligible(&unit(false, true, 21), Some(Duration::ZERO), now));
        let mut multiple = unit(false, true, 10);
        multiple.snapshot.insert(
            "newer".into(),
            Stamp {
                len: 1,
                modified: now,
                digest: None,
            },
        );
        assert!(!eligible(&multiple, Some(Duration::from_secs(10)), now));
    }

    #[test]
    fn preview_without_a_receipt_reports_unavailable_and_a_capture_command() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        let target = workspace.join("target");
        // A Cargo profile dir makes it a safe target root, so the preview
        // gets as far as looking for the receipt.
        std::fs::create_dir_all(target.join("debug")).unwrap();
        std::fs::write(workspace.join("Cargo.toml"), "[workspace]\n").unwrap();
        std::fs::write(
            target.join("CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55",
        )
        .unwrap();
        let config = crate::test_support::test_config(dir.path().join("cache"));
        let plan = preview(&config, &target, &workspace);
        assert_eq!(plan.status, "unavailable: no receipt yet", "{plan:?}");
        assert_eq!(
            plan.command,
            ["KACHE_TARGET_LIVENESS=1", "kache", "cargo", "check"]
        );
        assert_eq!(
            (plan.units, plan.bytes, plan.protected, plan.unknown),
            (0, 0, 0, 0)
        );
    }

    #[cfg(unix)]
    #[test]
    fn unit_paths_reject_symlinks_at_the_leaf_and_each_ancestor() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        let actual = target.join("debug/actual");
        std::fs::create_dir_all(&actual).unwrap();
        let file = actual.join("output.rlib");
        std::fs::write(&file, "artifact").unwrap();
        assert!(reject_symlink_ancestors(&file, &target).is_ok());
        assert!(reject_symlink_ancestors(&actual.join("missing"), &target).is_ok());
        let alias = target.join("debug/alias");
        std::os::unix::fs::symlink(&actual, &alias).unwrap();
        for part in [
            alias.clone(),
            alias.join("output.rlib"),
            alias.join("missing"),
        ] {
            assert!(
                reject_symlink_ancestors(&part, &target).is_err(),
                "{}",
                part.display()
            );
        }
        let linked_file = actual.join("linked.rlib");
        std::os::unix::fs::symlink(&file, &linked_file).unwrap();
        assert!(reject_symlink_ancestors(&linked_file, &target).is_err());
    }

    #[test]
    fn newly_observed_external_inputs_must_predate_compilation() {
        let workspace = Path::new("workspace");
        let target = Path::new("build-output");
        let started = SystemTime::UNIX_EPOCH + Duration::from_secs(20);
        let stamp = |seconds| Stamp {
            len: 7,
            modified: SystemTime::UNIX_EPOCH + Duration::from_secs(seconds),
            digest: Some("observed-content".into()),
        };
        let snapshot =
            |path: &str, seconds| Snapshot::from([(PathBuf::from(path), stamp(seconds))]);
        let before = Snapshot::new();
        for seconds in [19, 20] {
            assert!(
                validate_discovered_inputs(
                    &snapshot("external/input.rs", seconds),
                    &before,
                    workspace,
                    target,
                    started
                )
                .is_ok()
            );
        }
        assert!(
            validate_discovered_inputs(
                &snapshot("external/input.rs", 21),
                &before,
                workspace,
                target,
                started
            )
            .is_err()
        );
        assert!(
            validate_discovered_inputs(
                &snapshot("workspace/src/lib.rs", 21),
                &before,
                workspace,
                target,
                started
            )
            .is_ok()
        );
        assert!(
            validate_discovered_inputs(
                &snapshot("build-output/generated.rs", 21),
                &before,
                workspace,
                target,
                started
            )
            .is_ok()
        );
        let prior = snapshot("external/input.rs", 21);
        assert!(validate_discovered_inputs(&prior, &prior, workspace, target, started).is_ok());
    }

    fn receipt_for_test(units: Vec<ObservedUnit>) -> Receipt {
        Receipt {
            schema: SCHEMA,
            target: PathBuf::from("/target"),
            workspace: PathBuf::from("/workspace"),
            device: 1,
            inode: 2,
            recorded: SystemTime::UNIX_EPOCH,
            command: vec![
                "kache".into(),
                "cargo".into(),
                "check".into(),
                "--offline".into(),
            ],
            environment: "stable".into(),
            inputs: Snapshot::new(),
            units,
        }
    }

    #[test]
    fn unmapped_artifacts_and_executable_targets_protect_the_whole_package() {
        assert!(!needs_package_protection(true, &serde_json::json!(["lib"])));
        assert!(needs_package_protection(false, &serde_json::json!(["lib"])));
        assert!(needs_package_protection(true, &serde_json::json!(["bin"])));
        assert!(needs_package_protection(
            true,
            &serde_json::json!(["custom-build"])
        ));
        assert!(!needs_package_protection(true, &serde_json::Value::Null));
    }

    #[test]
    fn prior_live_variants_remain_live_only_with_matching_inputs_environment_and_outputs() {
        let previous = receipt_for_test(vec![unit(true, true, 1)]);
        let mut receipt = receipt_for_test(vec![unit(false, true, 1)]);
        preserve_previous_live_units(&mut receipt, &previous);
        assert!(receipt.units[0].live);
        for changed_inputs in [false, true] {
            let mut receipt = receipt_for_test(vec![unit(false, true, 1)]);
            if changed_inputs {
                receipt.inputs = unit(false, true, 1).snapshot;
            } else {
                receipt.environment = "changed".into();
            }
            preserve_previous_live_units(&mut receipt, &previous);
            assert!(!receipt.units[0].live);
        }
        let mut changed_outputs = receipt_for_test(vec![unit(false, true, 2)]);
        preserve_previous_live_units(&mut changed_outputs, &previous);
        assert!(!changed_outputs.units[0].live);
        let mut current_live = receipt_for_test(vec![unit(true, true, 1)]);
        let previous_unused = receipt_for_test(vec![unit(false, true, 1)]);
        preserve_previous_live_units(&mut current_live, &previous_unused);
        assert!(
            current_live.units[0].live,
            "a previous command cannot clear current liveness"
        );
        let mut different_unit = receipt_for_test(vec![unit(false, true, 1)]);
        different_unit.units[0].fingerprint =
            PathBuf::from("/target/debug/.fingerprint/another-unit");
        preserve_previous_live_units(&mut different_unit, &previous);
        assert!(!different_unit.units[0].live);
    }

    #[test]
    fn environment_snapshot_hashes_cargo_selection_and_build_inputs_but_ignores_display_state() {
        let digest = |vars: &[(&str, &str)]| {
            environment_digest_from(vars.iter().map(|(key, value)| {
                (
                    std::ffi::OsString::from(*key),
                    std::ffi::OsString::from(*value),
                )
            }))
        };
        let stable = digest(&[
            ("PATH", "/toolchain"),
            ("KACHE_REAL_CARGO", "/cargo-a"),
            ("KACHE_LOG", "warn"),
            ("TERM", "xterm"),
        ]);
        assert_eq!(
            stable,
            digest(&[
                ("TERM", "dumb"),
                ("KACHE_LOG", "off"),
                ("KACHE_REAL_CARGO", "/cargo-a"),
                ("PATH", "/toolchain")
            ])
        );
        assert_ne!(
            stable,
            digest(&[("PATH", "/toolchain"), ("KACHE_REAL_CARGO", "/cargo-b")])
        );
        assert_ne!(
            stable,
            digest(&[("PATH", "/new-toolchain"), ("KACHE_REAL_CARGO", "/cargo-a")])
        );
        assert_ne!(
            stable,
            digest(&[
                ("PATH", "/toolchain"),
                ("KACHE_REAL_CARGO", "/cargo-a"),
                ("RUSTFLAGS", "--cfg changed")
            ])
        );
        assert_ne!(digest(&[("AB", "C")]), digest(&[("A", "BC")]));
        assert_ne!(
            digest(&[("A", "B"), ("C", "D")]),
            digest(&[("A", "BC"), ("D", "")])
        );
    }

    #[test]
    fn modern_units_own_the_parent_once_while_shared_units_keep_each_part() {
        let dir = PathBuf::from("/target/debug/build/pkg/0123456789abcdef");
        let mut parts = vec![
            dir.clone(),
            dir.join("fingerprint"),
            dir.join("output.rlib"),
        ];
        retain_owned_parts(&Unit::PerUnit { dir: dir.clone() }, &mut parts);
        assert_eq!(parts, vec![dir]);
        let shared = Unit::Shared {
            profile: PathBuf::from("/target/debug"),
            package: "pkg".into(),
            hash: "0123456789abcdef".into(),
        };
        let mut parts = vec![
            shared.fingerprint(),
            PathBuf::from("/target/debug/deps/libpkg-0123456789abcdef.rlib"),
        ];
        let expected = parts.clone();
        retain_owned_parts(&shared, &mut parts);
        assert_eq!(parts, expected);
    }

    #[test]
    fn receipt_summary_counts_live_unknown_young_changed_and_removable_units() {
        let dir = tempfile::tempdir().unwrap();
        let fingerprint = dir.path().join("fingerprint");
        std::fs::create_dir_all(&fingerprint).unwrap();
        std::fs::write(fingerprint.join("lib-pkg.json"), "{}").unwrap();
        let old = filetime::FileTime::from_unix_time(1, 0);
        filetime::set_file_mtime(fingerprint.join("lib-pkg.json"), old).unwrap();
        filetime::set_file_mtime(&fingerprint, old).unwrap();
        let snapshot = output_snapshot(std::slice::from_ref(&fingerprint), &fingerprint).unwrap();
        let removable = ObservedUnit {
            fingerprint: fingerprint.clone(),
            parts: vec![fingerprint],
            snapshot,
            live: false,
            known: true,
        };
        let young_fingerprint = dir.path().join("young-fingerprint");
        std::fs::create_dir_all(&young_fingerprint).unwrap();
        std::fs::write(young_fingerprint.join("lib-pkg.json"), "{}").unwrap();
        let young = ObservedUnit {
            snapshot: output_snapshot(std::slice::from_ref(&young_fingerprint), &young_fingerprint)
                .unwrap(),
            fingerprint: young_fingerprint.clone(),
            parts: vec![young_fingerprint],
            live: false,
            known: true,
        };
        let changed_fingerprint = dir.path().join("changed-fingerprint");
        std::fs::create_dir_all(&changed_fingerprint).unwrap();
        let changed_file = changed_fingerprint.join("lib-pkg.json");
        std::fs::write(&changed_file, "{}").unwrap();
        filetime::set_file_mtime(&changed_file, old).unwrap();
        filetime::set_file_mtime(&changed_fingerprint, old).unwrap();
        let changed = ObservedUnit {
            snapshot: output_snapshot(
                std::slice::from_ref(&changed_fingerprint),
                &changed_fingerprint,
            )
            .unwrap(),
            fingerprint: changed_fingerprint.clone(),
            parts: vec![changed_fingerprint],
            live: false,
            known: true,
        };
        std::fs::write(changed_file, "changed fingerprint contents").unwrap();
        let now = SystemTime::now();
        let window = Some(Duration::from_secs(20));
        assert!(!eligible(&young, window, now));
        assert_eq!(
            output_snapshot(&young.parts, &young.fingerprint).unwrap(),
            young.snapshot
        );
        assert!(eligible(&changed, window, now));
        assert_ne!(
            output_snapshot(&changed.parts, &changed.fingerprint).unwrap(),
            changed.snapshot
        );
        let receipt = receipt_for_test(vec![
            unit(true, true, 1),
            unit(false, false, 1),
            young,
            changed,
            removable,
        ]);
        let plan = summarize_receipt(&receipt, Some(Duration::from_secs(20)), now);
        assert_eq!(plan.status, "ready");
        assert_eq!(plan.command, ["kache", "cargo", "check", "--offline"]);
        assert_eq!(plan.protected, 1);
        assert_eq!(plan.unknown, 3);
        assert_eq!(plan.units, 1);
        assert!(plan.bytes > 0);
    }

    #[test]
    fn protected_counts_report_live_units_to_disk_pressure_policy() {
        let receipt = receipt_for_test(vec![
            unit(true, true, 1),
            unit(true, false, 1),
            unit(false, true, 1),
        ]);
        let counts = protected_counts(&receipt);
        assert_eq!(counts.used, 2);
        assert_eq!(counts.units, 0);
        assert_eq!(counts.bytes, 0);
    }

    #[test]
    fn interrupted_batch_restores_later_units_and_reports_only_completed_removals() {
        let dir = tempfile::tempdir().unwrap();
        let first = unit(false, true, 1);
        let later = unit(false, true, 1);
        let original = dir.path().join("later-original");
        let aside = dir.path().join("later-aside");
        std::fs::write(&aside, "later unit must survive").unwrap();
        let staged = vec![
            (
                &first,
                vec![(
                    dir.path().join("missing-original"),
                    dir.path().join("missing-aside"),
                )],
            ),
            (&later, vec![(original.clone(), aside.clone())]),
        ];
        let mut removed = Vec::new();
        let mut pruned = Pruned {
            used: 2,
            ..Default::default()
        };
        assert!(delete_staged_units(&staged, &mut removed, &mut pruned).is_err());
        assert!(removed.is_empty());
        assert_eq!(pruned.units, 0);
        assert_eq!(pruned.bytes, 0);
        assert_eq!(pruned.used, 2);
        assert_eq!(
            std::fs::read(&original).unwrap(),
            b"later unit must survive"
        );
        assert!(!aside.exists());
        let completed = dir.path().join("completed-aside");
        std::fs::write(&completed, "completed").unwrap();
        let mut removed = Vec::new();
        let mut pruned = Pruned::default();
        delete_staged_units(
            &[(
                &first,
                vec![(dir.path().join("completed-original"), completed.clone())],
            )],
            &mut removed,
            &mut pruned,
        )
        .unwrap();
        assert!(!completed.exists());
        assert_eq!(pruned.units, 1);
        assert_eq!(pruned.bytes, 7);
        assert_eq!(
            removed,
            [dir.path()
                .join("completed-original")
                .to_string_lossy()
                .into_owned()]
        );
    }

    #[test]
    fn cargo_argument_scope_handles_overrides_and_toolchain_selectors() {
        let args = |a: &[&str]| a.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let a = args(&[
            "+nightly",
            "--config",
            "build.build-dir='target'",
            "check",
            "--target-dir=out",
        ]);
        assert_eq!(cargo_subcommand(&a), Some("check"));
        for flag in ["--config", "--color", "--manifest-path"] {
            assert_eq!(
                cargo_subcommand(&args(&[flag, "value", "check"])),
                Some("check"),
                "{flag}"
            );
        }
        assert_eq!(argument(&a, "--target-dir"), Some("out"));
        assert_eq!(argument(&a, "--manifest-path"), None);
        assert_eq!(
            argument(&args(&["--manifest-path", "Cargo.toml"]), "--manifest-path"),
            Some("Cargo.toml")
        );
        assert_eq!(
            argument(&args(&["--manifest-path"]), "--manifest-path"),
            None
        );
        assert_eq!(cargo_subcommand(&args(&["test", "check"])), Some("test"));
        assert_eq!(cargo_subcommand(&args(&["--config"])), None);
    }

    #[test]
    fn source_inventory_detects_changed_contents_even_with_restored_mtime() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("lib.rs");
        std::fs::write(&file, b"old").unwrap();
        let mut before = Snapshot::new();
        walk(&mut before, dir.path(), &[], true).unwrap();
        let metadata = std::fs::metadata(&file).unwrap();
        std::fs::write(&file, b"new").unwrap();
        filetime::set_file_mtime(
            &file,
            filetime::FileTime::from_last_modification_time(&metadata),
        )
        .unwrap();
        let mut after = Snapshot::new();
        walk(&mut after, dir.path(), &[], true).unwrap();
        assert_ne!(before, after);
        assert_eq!(before[&file].len, 3);
        assert!(before[&file].digest.is_some());
        assert_eq!(before[&file].modified, after[&file].modified);
    }

    #[test]
    fn output_inventory_ignores_access_time_and_detects_output_additions() {
        let dir = tempfile::tempdir().unwrap();
        let fp = dir.path().join("fingerprint");
        std::fs::create_dir(&fp).unwrap();
        let file = dir.path().join("output.rlib");
        std::fs::write(&file, "output").unwrap();
        std::fs::write(fp.join("lib-x.json"), "{}").unwrap();
        let before = output_snapshot(&[dir.path().to_path_buf()], &fp).unwrap();
        filetime::set_file_atime(&file, filetime::FileTime::from_unix_time(1, 0)).unwrap();
        assert_eq!(
            before,
            output_snapshot(&[dir.path().to_path_buf()], &fp).unwrap()
        );
        assert!(before[&file].digest.is_none());
        assert!(before[&fp.join("lib-x.json")].digest.is_some());
        std::fs::write(dir.path().join("new"), "new").unwrap();
        assert_ne!(
            before,
            output_snapshot(&[dir.path().to_path_buf()], &fp).unwrap()
        );
    }

    #[test]
    fn library_ownership_keeps_custom_names_and_mixed_fingerprints_unknown() {
        let dir = tempfile::tempdir().unwrap();
        let unit = Unit::Shared {
            profile: dir.path().to_path_buf(),
            package: "my-pkg".into(),
            hash: "0123456789abcdef".into(),
        };
        let fp = unit.fingerprint();
        std::fs::create_dir_all(&fp).unwrap();
        assert_eq!(known_library(&unit).unwrap(), ("my-pkg".into(), false));
        std::fs::write(fp.join("lib-custom.json"), "{}").unwrap();
        assert!(!known_library(&unit).unwrap().1);
        std::fs::remove_file(fp.join("lib-custom.json")).unwrap();
        std::fs::write(fp.join("lib-my_pkg.json"), "{}").unwrap();
        assert!(known_library(&unit).unwrap().1);
        std::fs::write(fp.join("run-build-script-build-script-build.json"), "{}").unwrap();
        assert!(!known_library(&unit).unwrap().1);
        let modern = Unit::PerUnit {
            dir: dir.path().join("build/my-pkg/0123456789abcdef"),
        };
        std::fs::create_dir_all(modern.fingerprint()).unwrap();
        std::fs::write(modern.fingerprint().join("lib-my_pkg.json"), "{}").unwrap();
        assert_eq!(known_library(&modern).unwrap(), ("my-pkg".into(), true));
    }

    #[test]
    fn workspace_discovery_selects_workspace_ancestor_or_nearest_package() {
        let dir = tempfile::tempdir().unwrap();
        let member = dir.path().join("member");
        std::fs::create_dir_all(member.join("src")).unwrap();
        assert!(workspace_root(&member).is_err());
        std::fs::write(member.join("Cargo.toml"), "[package]\nname='member'\n").unwrap();
        assert_eq!(workspace_root(&member.join("src")).unwrap(), member);
        assert_eq!(package_name(&member.join("Cargo.toml")).unwrap(), "member");
        assert!(package_name(&member.join("missing")).is_err());
        std::fs::write(
            dir.path().join("Cargo.toml"),
            "[workspace]\nmembers=['member']\n",
        )
        .unwrap();
        assert_eq!(workspace_root(&member.join("src")).unwrap(), dir.path());
    }

    #[test]
    fn snapshot_exclusions_and_rollback_preserve_source_paths() {
        let dir = tempfile::tempdir().unwrap();
        let excluded = dir.path().join("target");
        std::fs::create_dir(&excluded).unwrap();
        std::fs::write(excluded.join("output"), "output").unwrap();
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        std::fs::write(dir.path().join(".git/state"), "git").unwrap();
        let original = dir.path().join("original");
        std::fs::write(&original, "keep").unwrap();
        let mut snapshot = Snapshot::new();
        walk(&mut snapshot, dir.path(), &[excluded], true).unwrap();
        assert_eq!(snapshot.len(), 2);
        let aside = dir.path().join("aside");
        std::fs::rename(&original, &aside).unwrap();
        rollback(&[(original.clone(), aside.clone())]);
        assert_eq!(std::fs::read(&original).unwrap(), b"keep");
        assert!(!aside.exists());
    }

    #[cfg(unix)]
    #[test]
    fn symlink_evidence_and_target_roots_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        std::fs::write(&source, "keep").unwrap();
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&source, &link).unwrap();
        assert!(insert_file(&mut Snapshot::new(), &link, true).is_err());
        assert!(walk(&mut Snapshot::new(), dir.path(), &[], true).is_err());
        assert!(checked_root(&link, dir.path()).is_err());
        assert_eq!(std::fs::read(source).unwrap(), b"keep");
    }
    fn capture_for_test(dir: &Path) -> Capture {
        Capture {
            config: crate::test_support::test_config(dir.to_path_buf()),
            workspace: dir.join("uncreated-workspace"),
            target: dir.join("uncreated-target"),
            before: Snapshot::new(),
            started: SystemTime::UNIX_EPOCH + Duration::from_secs(100),
            command: vec!["check".into()],
            environment: "captured-environment".into(),
            human: true,
            complete: false,
            observed_bytes: 0,
            invalid: false,
            artifacts: Vec::new(),
            scripts: Vec::new(),
        }
    }

    #[test]
    fn capture_observes_fresh_and_rebuilt_artifacts_scripts_and_completion() {
        let dir = tempfile::tempdir().unwrap();
        let mut capture = capture_for_test(dir.path());
        assert!(capture.human_output());
        capture.human = false;
        assert!(!capture.human_output());
        let fresh =
            br#"{"reason":"compiler-artifact","fresh":true,"filenames":["/target/liba.rlib"]}"#;
        let rebuilt =
            br#"{"reason":"compiler-artifact","fresh":false,"filenames":["/target/libb.rlib"]}"#;
        let script =
            br#"{"reason":"build-script-executed","package_id":"example","out_dir":"/target/out"}"#;
        assert!(capture.observe(fresh));
        assert!(capture.observe(rebuilt));
        assert!(capture.observe(script));
        assert!(capture.observe(br#"{"reason":"compiler-message","message":{"rendered":null}}"#));
        assert_eq!(capture.artifacts.len(), 2);
        assert_eq!(capture.artifacts[0]["fresh"], true);
        assert_eq!(capture.artifacts[1]["fresh"], false);
        assert_eq!(capture.artifacts[0]["filenames"][0], "/target/liba.rlib");
        assert_eq!(capture.artifacts[1]["filenames"][0], "/target/libb.rlib");
        assert_eq!(capture.scripts.len(), 1);
        assert_eq!(capture.scripts[0]["out_dir"], "/target/out");
        assert_eq!(
            capture.observed_bytes,
            fresh.len() + rebuilt.len() + script.len()
        );
        assert!(!capture.complete);
        assert!(!capture.invalid);
        assert!(capture.observe(br#"{"reason":"build-finished","success":true}"#));
        assert!(capture.complete);
        assert!(capture.observe(br#"{"reason":"build-finished","success":false}"#));
        assert!(!capture.complete);
        assert!(capture.observe(br#"{"reason":"build-finished"}"#));
        assert!(!capture.complete);
        assert!(capture.observe(br#"{"reason":"build-finished","success":"true"}"#));
        assert!(!capture.complete);
        assert!(!capture.invalid);
    }

    #[test]
    fn capture_invalidates_sentinel_unknown_and_malformed_streams_without_claiming_protocol() {
        let dir = tempfile::tempdir().unwrap();
        for line in [
            b"\0kache-invalid-cargo-stream\n".as_slice(),
            br#"{"reason":"future-reason"}"#.as_slice(),
            br#"{"reason":"compiler-artifact""#.as_slice(),
            b"build script stdout\n".as_slice(),
            b"\xff\n".as_slice(),
            b"{}".as_slice(),
        ] {
            let mut capture = capture_for_test(dir.path());
            assert!(!capture.observe(line), "{line:?}");
            assert!(capture.invalid, "{line:?}");
            assert!(capture.artifacts.is_empty());
            assert!(capture.scripts.is_empty());
            assert_eq!(capture.observed_bytes, 0);
            assert!(capture.observe(br#"{"reason":"build-finished","success":true}"#));
            assert!(
                capture.invalid,
                "later success cannot repair an invalid stream"
            );
        }
    }

    #[test]
    fn capture_record_count_limit_accepts_boundary_and_refuses_the_next_record() {
        let dir = tempfile::tempdir().unwrap();
        for script in [false, true] {
            let mut capture = capture_for_test(dir.path());
            let line = if script {
                br#"{"reason":"build-script-executed"}"#.as_slice()
            } else {
                br#"{"reason":"compiler-artifact"}"#.as_slice()
            };
            let records = if script {
                &mut capture.scripts
            } else {
                &mut capture.artifacts
            };
            *records = vec![serde_json::Value::Null; 32_767];
            assert!(capture.observe(line));
            assert!(!capture.invalid);
            let count = if script {
                capture.scripts.len()
            } else {
                capture.artifacts.len()
            };
            assert_eq!(count, 32_768);
            assert!(capture.observe(line));
            assert!(capture.invalid);
            let count = if script {
                capture.scripts.len()
            } else {
                capture.artifacts.len()
            };
            assert_eq!(count, 32_768);
        }
    }

    #[test]
    fn capture_aggregate_byte_limit_is_shared_by_artifact_and_script_records() {
        let dir = tempfile::tempdir().unwrap();
        let artifact = br#"{"reason":"compiler-artifact"}"#;
        let script = br#"{"reason":"build-script-executed"}"#;
        let mut capture = capture_for_test(dir.path());
        capture.observed_bytes = 16_777_216 - artifact.len();
        assert!(capture.observe(artifact));
        assert!(!capture.invalid);
        assert_eq!(capture.observed_bytes, 16_777_216);
        assert_eq!(capture.artifacts.len(), 1);
        assert!(capture.observe(script));
        assert!(capture.invalid);
        assert_eq!(capture.observed_bytes, 16_777_216 + script.len());
        assert!(capture.scripts.is_empty());
        assert_eq!(capture.artifacts.len(), 1);

        let mut capture = capture_for_test(dir.path());
        capture.observed_bytes = 16_777_216 - script.len();
        assert!(capture.observe(script));
        assert!(!capture.invalid);
        assert_eq!(capture.scripts.len(), 1);
        assert!(capture.observe(artifact));
        assert!(capture.invalid);
        assert!(capture.artifacts.is_empty());
        assert_eq!(capture.scripts.len(), 1);

        let mut capture = capture_for_test(dir.path());
        capture.observed_bytes = usize::MAX;
        assert!(capture.observe(artifact));
        assert!(capture.invalid);
        assert_eq!(capture.observed_bytes, usize::MAX);
        assert!(capture.artifacts.is_empty());
    }

    #[test]
    fn capture_finish_never_writes_failed_incomplete_invalid_or_empty_receipts() {
        let dir = tempfile::tempdir().unwrap();
        let existing = dir.path().join("existing-receipt.json");
        std::fs::write(&existing, b"leave unchanged").unwrap();
        for (success, complete, invalid, artifact_present) in [
            (false, true, false, true),
            (true, false, false, true),
            (true, true, true, true),
            (true, true, false, false),
        ] {
            let mut capture = capture_for_test(dir.path());
            capture.complete = complete;
            capture.invalid = invalid;
            if artifact_present {
                capture
                    .artifacts
                    .push(serde_json::json!({"reason":"compiler-artifact"}));
            }
            // Neither workspace nor target exists: reaching any inventory/writing
            // path would fail instead of passing this early-return assertion.
            capture.finish(success).unwrap();
            let entries: Vec<_> = std::fs::read_dir(dir.path())
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect();
            assert_eq!(
                entries,
                vec![std::ffi::OsString::from("existing-receipt.json")]
            );
            assert_eq!(std::fs::read(&existing).unwrap(), b"leave unchanged");
            assert!(!dir.path().join("uncreated-workspace").exists());
            assert!(!dir.path().join("uncreated-target").exists());
            assert!(!dir.path().join("target-liveness").exists());
        }
    }
    #[test]
    fn staged_inventory_revalidates_both_layouts_and_restores_remaining_parts() {
        let dir = tempfile::tempdir().unwrap();
        for modern in [false, true] {
            let root = dir.path().join(if modern { "modern" } else { "legacy" });
            let fingerprint = root.join("fingerprint");
            let output = root.join("output.rlib");
            std::fs::create_dir_all(&fingerprint).unwrap();
            std::fs::write(fingerprint.join("lib-x.json"), "{}").unwrap();
            std::fs::write(&output, "outputs").unwrap();
            let parts = if modern {
                vec![root.clone()]
            } else {
                vec![fingerprint.clone(), output.clone()]
            };
            let snapshot = output_snapshot(&parts, &fingerprint).unwrap();
            let unit = ObservedUnit {
                fingerprint: fingerprint.clone(),
                parts: parts.clone(),
                snapshot: snapshot.clone(),
                live: false,
                known: true,
            };
            let staged: Vec<_> = parts
                .iter()
                .enumerate()
                .map(|(i, original)| {
                    let aside = dir.path().join(format!("aside-{modern}-{i}"));
                    std::fs::rename(original, &aside).unwrap();
                    (original.clone(), aside)
                })
                .collect();
            assert_eq!(staged_snapshot(&unit, &staged).unwrap(), snapshot);
            assert!(staged_batch_unchanged(true, &[(&unit, staged.clone())]));
            assert!(!staged_batch_unchanged(false, &[(&unit, staged.clone())]));
            let staged_output = if modern {
                staged[0].1.join("output.rlib")
            } else {
                staged[1].1.clone()
            };
            std::fs::write(&staged_output, "changed-output").unwrap();
            assert_ne!(staged_snapshot(&unit, &staged).unwrap(), snapshot);
            assert!(!staged_batch_unchanged(true, &[(&unit, staged.clone())]));
            assert!(!staged_batch_unchanged(false, &[(&unit, staged.clone())]));
            rollback(&staged);
            assert_eq!(std::fs::read(&output).unwrap(), b"changed-output");
            assert!(fingerprint.exists());
        }
        // Simulate a failed unlink after a previously completed part.
        let first = dir.path().join("first-aside");
        let last = dir.path().join("last-aside");
        std::fs::write(&first, "first").unwrap();
        std::fs::write(&last, "last").unwrap();
        let first_original = dir.path().join("first");
        let last_original = dir.path().join("last");
        let staged = vec![
            (first_original.clone(), first),
            (
                dir.path().join("missing-original"),
                dir.path().join("missing-aside"),
            ),
            (last_original.clone(), last),
        ];
        let mut removed = Vec::new();
        assert!(delete_staged(&staged, &mut removed).is_err());
        assert_eq!(removed, vec![first_original.to_string_lossy().into_owned()]);
        assert!(!first_original.exists());
        assert_eq!(std::fs::read(last_original).unwrap(), b"last");
    }
}
