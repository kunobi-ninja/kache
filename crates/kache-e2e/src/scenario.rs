//! Shared scenario metadata and selection.
//!
//! Fixtures and clone-scale benchmark scenarios still have different source
//! materialization needs, but they now expose the same identity surface:
//! name, source kind, and tags. Selection is intentionally declarative and
//! source-kind aware, so CI can say "run the gate tier" without knowing which
//! directory layout backs each scenario.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use crate::bench_profile::BenchProfile;
use crate::fixture::Fixture;
use crate::phase::Phase;

/// Where a scenario's source comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SourceKind {
    /// Checked-in fixture source directory.
    Fixture,
    /// External repository materialized as cloned worktrees.
    Clone,
}

impl SourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            SourceKind::Fixture => "fixture",
            SourceKind::Clone => "clone",
        }
    }

    pub fn source_tag(self) -> &'static str {
        match self {
            SourceKind::Fixture => "source:fixture",
            SourceKind::Clone => "source:clone",
        }
    }

    pub fn default_tier_tag(self) -> &'static str {
        match self {
            // Fast checked-in fixtures are the PR correctness gate by default.
            SourceKind::Fixture => "tier:gate",
            // Large cloned repositories are opt-in/nightly by default.
            SourceKind::Clone => "tier:nightly",
        }
    }
}

/// Selector parsed from `--select`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selector {
    raw: String,
}

impl Selector {
    pub fn parse(raw: &str) -> Result<Self> {
        let raw = raw.trim();
        anyhow::ensure!(!raw.is_empty(), "--select cannot be empty");
        Ok(Self {
            raw: raw.to_string(),
        })
    }

    pub fn raw(&self) -> &str {
        &self.raw
    }

    fn matches(&self, name: &str, tags: &BTreeSet<String>, source_kind: SourceKind) -> bool {
        let raw = self.raw.as_str();
        if tags.contains(raw) {
            return true;
        }
        if let Some(needle) = raw.strip_prefix("name:") {
            return name.contains(needle);
        }
        // Convenience aliases for the natural source-kind selector.
        raw == source_kind.as_str()
    }
}

#[derive(Debug, Clone, Default)]
pub struct Selectors {
    items: Vec<Selector>,
}

impl Selectors {
    pub fn parse_many(raw: &[String]) -> Result<Self> {
        let items = raw
            .iter()
            .map(|s| Selector::parse(s).with_context(|| format!("parsing --select `{s}`")))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { items })
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn describe(&self) -> String {
        self.items
            .iter()
            .map(|s| s.raw())
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub fn matches(&self, name: &str, source_kind: SourceKind, explicit_tags: &[String]) -> bool {
        let tags = effective_tags(source_kind, explicit_tags);
        self.items
            .iter()
            .all(|selector| selector.matches(name, &tags, source_kind))
    }

    pub fn matches_scenario(&self, scenario: &Scenario) -> bool {
        self.matches(&scenario.name, scenario.source.kind(), &scenario.tags)
    }

    pub fn matches_metadata(&self, metadata: &ScenarioMetadata) -> bool {
        self.matches(&metadata.name, metadata.source_kind, &metadata.tags)
    }

    /// Needles from any `name:<needle>` selectors. `name:` is a substring
    /// filter, so callers that must resolve to a single scenario (the bench
    /// runner) use these to prefer an exact-name match when the substring is
    /// ambiguous — e.g. `firefox` also catches `bench-firefox-windows`.
    pub fn name_needles(&self) -> Vec<&str> {
        self.items
            .iter()
            .filter_map(|s| s.raw().strip_prefix("name:"))
            .collect()
    }
}

/// Tags used for matching. Source kind is always implicit, and the default
/// tier is implicit unless the TOML declares an explicit `tier:*` tag.
pub fn effective_tags(source_kind: SourceKind, explicit_tags: &[String]) -> BTreeSet<String> {
    let mut tags = BTreeSet::new();
    tags.insert(source_kind.source_tag().to_string());
    if !explicit_tags.iter().any(|tag| tag.starts_with("tier:")) {
        tags.insert(source_kind.default_tier_tag().to_string());
    }
    tags.extend(explicit_tags.iter().cloned());
    tags
}

#[derive(Debug, Clone)]
pub struct ScenarioMetadata {
    pub name: String,
    pub tags: Vec<String>,
    pub source_kind: SourceKind,
    pub dir: PathBuf,
    pub toml_path: PathBuf,
}

#[derive(Debug, Deserialize)]
struct ScenarioMetadataConfig {
    name: String,
    #[serde(default)]
    tags: Vec<String>,
    source: Option<ScenarioSourceConfig>,
}

pub fn discover_metadata(root: &Path) -> Result<Vec<ScenarioMetadata>> {
    let mut scenarios = Vec::new();
    for entry in std::fs::read_dir(root).with_context(|| format!("reading {}", root.display()))? {
        let entry = entry?;
        let dir = entry.path();
        if !dir.is_dir() {
            continue;
        }
        let toml_path = dir.join("scenario.toml");
        if !toml_path.is_file() {
            continue;
        }
        let raw = std::fs::read_to_string(&toml_path)
            .with_context(|| format!("reading {}", toml_path.display()))?;
        let cfg: ScenarioMetadataConfig =
            toml::from_str(&raw).with_context(|| format!("parsing {}", toml_path.display()))?;
        scenarios.push(ScenarioMetadata {
            name: cfg.name,
            tags: cfg.tags,
            source_kind: cfg
                .source
                .map(|source| source.kind())
                .unwrap_or(SourceKind::Fixture),
            dir,
            toml_path,
        });
    }
    scenarios.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(scenarios)
}

/// Common scenario checks surface. The legacy fixture `[assertions]` table
/// remains the blocking correctness contract for fixture TOML; `checks.assert`
/// gives clone-style scenarios a shared blocking verdict surface, while
/// `checks.measure` is advisory only.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ScenarioChecks {
    #[serde(default, rename = "assert")]
    pub assertions: PhaseAssertSpecs,
    #[serde(default)]
    pub measure: PhaseMeasureSpecs,
}

/// Blocking assertion specs by phase.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PhaseAssertSpecs {
    pub cold: Option<ScenarioAssertSpec>,
    pub warm: Option<ScenarioAssertSpec>,
    pub pull: Option<ScenarioAssertSpec>,
    pub noop: Option<ScenarioAssertSpec>,
    pub relocate: Option<ScenarioAssertSpec>,
    #[serde(rename = "relocate-modified")]
    pub relocate_modified: Option<ScenarioAssertSpec>,
    #[serde(rename = "relocate-noop")]
    pub relocate_noop: Option<ScenarioAssertSpec>,
}

impl PhaseAssertSpecs {
    pub fn for_phase(&self, phase: &str) -> Option<&ScenarioAssertSpec> {
        match phase {
            "cold" => self.cold.as_ref(),
            "warm" => self.warm.as_ref(),
            "pull" => self.pull.as_ref(),
            "noop" => self.noop.as_ref(),
            "relocate" => self.relocate.as_ref(),
            "relocate-modified" => self.relocate_modified.as_ref(),
            "relocate-noop" => self.relocate_noop.as_ref(),
            _ => None,
        }
    }
}

/// Blocking thresholds currently used by clone-scale scenarios.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ScenarioAssertSpec {
    pub min_key_stability_pct: Option<f64>,
    pub max_passthrough_pct: Option<f64>,
    pub max_errors: Option<u64>,
}

/// Warn-only measurement specs by phase.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PhaseMeasureSpecs {
    pub cold: Option<MeasureSpec>,
    pub warm: Option<MeasureSpec>,
    pub pull: Option<MeasureSpec>,
    pub noop: Option<MeasureSpec>,
    pub relocate: Option<MeasureSpec>,
    #[serde(rename = "relocate-modified")]
    pub relocate_modified: Option<MeasureSpec>,
    #[serde(rename = "relocate-noop")]
    pub relocate_noop: Option<MeasureSpec>,
}

impl PhaseMeasureSpecs {
    pub fn for_phase(&self, phase: &str) -> Option<&MeasureSpec> {
        match phase {
            "cold" => self.cold.as_ref(),
            "warm" => self.warm.as_ref(),
            "pull" => self.pull.as_ref(),
            "noop" => self.noop.as_ref(),
            "relocate" => self.relocate.as_ref(),
            "relocate-modified" => self.relocate_modified.as_ref(),
            "relocate-noop" => self.relocate_noop.as_ref(),
            _ => None,
        }
    }
}

/// Advisory thresholds. Violations are recorded as warnings in results JSON
/// and never change phase/scenario status.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct MeasureSpec {
    pub max_wall_s: Option<u64>,
    pub min_hit_rate_pct: Option<f64>,
    pub min_speedup: Option<f64>,
}

/// The common scenario shape both fixture and clone TOML files adapt into.
#[derive(Debug, Clone)]
pub struct Scenario {
    pub name: String,
    pub source: ScenarioSource,
    pub tags: Vec<String>,
    pub phases: Vec<Phase>,
    pub env: HashMap<String, String>,
    pub commands: ScenarioCommands,
    pub checks: ScenarioChecks,
}

impl Scenario {
    pub fn from_fixture(fixture: &Fixture) -> Self {
        Self {
            name: fixture.name.clone(),
            source: ScenarioSource::Fixture {
                dir: fixture.dir.clone(),
            },
            tags: fixture.tags.clone(),
            phases: fixture_phases(fixture),
            env: fixture.env.clone(),
            commands: ScenarioCommands {
                setup: Vec::new(),
                build: fixture.commands.build.clone(),
                clean: Some(fixture.commands.clean.clone()),
            },
            checks: fixture.checks.clone(),
        }
    }

    pub fn from_bench_profile(profile: &BenchProfile) -> Self {
        Self {
            name: profile.name.clone(),
            source: ScenarioSource::Clone {
                repo: profile.repo.clone(),
                git_ref: profile.git_ref.clone(),
                objdir: profile.objdir.clone(),
            },
            tags: profile.tags.clone(),
            phases: default_phases(SourceKind::Clone),
            env: profile
                .env
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            commands: ScenarioCommands {
                setup: profile.setup.clone(),
                build: profile.build.clone(),
                clean: None,
            },
            checks: profile.checks.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ScenarioSource {
    Fixture {
        dir: PathBuf,
    },
    Clone {
        repo: String,
        git_ref: String,
        objdir: String,
    },
}

impl ScenarioSource {
    pub fn kind(&self) -> SourceKind {
        match self {
            ScenarioSource::Fixture { .. } => SourceKind::Fixture,
            ScenarioSource::Clone { .. } => SourceKind::Clone,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScenarioCommands {
    pub setup: Vec<String>,
    pub build: String,
    pub clean: Option<String>,
}

/// Deserializable version of the common scenario schema.
#[derive(Debug, Clone, Deserialize)]
pub struct ScenarioConfig {
    pub name: String,
    pub source: ScenarioSourceConfig,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub phases: Vec<Phase>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    pub commands: ScenarioCommandsConfig,
    #[serde(default)]
    pub checks: ScenarioChecks,
}

impl ScenarioConfig {
    pub fn phases_or_default(&self) -> Vec<Phase> {
        if self.phases.is_empty() {
            default_phases(self.source.kind())
        } else {
            self.phases.clone()
        }
    }

    pub fn into_scenario(self) -> Scenario {
        let source = match self.source {
            ScenarioSourceConfig::Fixture { path } => ScenarioSource::Fixture { dir: path },
            ScenarioSourceConfig::Clone {
                repo,
                git_ref,
                objdir,
                ref_next: _,
            } => ScenarioSource::Clone {
                repo,
                git_ref,
                objdir,
            },
        };
        let phases = if self.phases.is_empty() {
            default_phases(source.kind())
        } else {
            self.phases
        };
        Scenario {
            name: self.name,
            source,
            tags: self.tags,
            phases,
            env: self.env,
            commands: ScenarioCommands {
                setup: self.commands.setup,
                build: self.commands.build,
                clean: self.commands.clean,
            },
            checks: self.checks,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ScenarioSourceConfig {
    Fixture {
        path: PathBuf,
    },
    Clone {
        repo: String,
        #[serde(rename = "ref")]
        git_ref: String,
        #[serde(default)]
        ref_next: Option<String>,
        objdir: String,
    },
}

impl ScenarioSourceConfig {
    pub fn kind(&self) -> SourceKind {
        match self {
            ScenarioSourceConfig::Fixture { .. } => SourceKind::Fixture,
            ScenarioSourceConfig::Clone { .. } => SourceKind::Clone,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ScenarioCommandsConfig {
    #[serde(default)]
    pub setup: Vec<String>,
    pub build: String,
    #[serde(default)]
    pub clean: Option<String>,
}

pub fn default_phases(source_kind: SourceKind) -> Vec<Phase> {
    match source_kind {
        SourceKind::Fixture => vec![Phase::Cold, Phase::Warm, Phase::Noop, Phase::Relocate],
        SourceKind::Clone => vec![Phase::Cold, Phase::Warm],
    }
}

fn fixture_phases(fixture: &Fixture) -> Vec<Phase> {
    let mut phases = default_phases(SourceKind::Fixture);
    if fixture.assertions.relocate_noop.is_some() {
        phases.push(Phase::RelocateNoop);
    }
    if fixture.modify.is_some() {
        phases.push(Phase::RelocateModified);
    }
    phases
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_gets_gate_and_source_tags_by_default() {
        let tags = effective_tags(SourceKind::Fixture, &[]);
        assert!(tags.contains("source:fixture"));
        assert!(tags.contains("tier:gate"));
    }

    #[test]
    fn clone_gets_nightly_and_source_tags_by_default() {
        let tags = effective_tags(SourceKind::Clone, &[]);
        assert!(tags.contains("source:clone"));
        assert!(tags.contains("tier:nightly"));
    }

    #[test]
    fn explicit_tier_replaces_default_tier() {
        let fixture_tags = effective_tags(SourceKind::Fixture, &["tier:nightly".to_string()]);
        assert!(fixture_tags.contains("source:fixture"));
        assert!(fixture_tags.contains("tier:nightly"));
        assert!(!fixture_tags.contains("tier:gate"));

        let clone_tags = effective_tags(SourceKind::Clone, &["tier:gate".to_string()]);
        assert!(clone_tags.contains("source:clone"));
        assert!(clone_tags.contains("tier:gate"));
        assert!(!clone_tags.contains("tier:nightly"));
    }

    #[test]
    fn selectors_match_all_requested_tags() {
        let selectors =
            Selectors::parse_many(&["tier:gate".to_string(), "lang:rust".to_string()]).unwrap();
        assert!(selectors.matches(
            "rust-check",
            SourceKind::Fixture,
            &["lang:rust".to_string()]
        ));
        assert!(!selectors.matches("c-hello", SourceKind::Fixture, &["lang:cc".to_string()]));
    }

    #[test]
    fn selectors_support_name_filter_and_source_alias() {
        let selectors =
            Selectors::parse_many(&["fixture".to_string(), "name:hello".to_string()]).unwrap();
        assert!(selectors.matches("c-hello", SourceKind::Fixture, &[]));
        assert!(!selectors.matches("firefox", SourceKind::Clone, &[]));
    }

    #[test]
    fn discovers_scenario_metadata_from_shared_root() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scenarios");
        let scenarios = discover_metadata(&root).unwrap();

        let firefox = scenarios
            .iter()
            .find(|scenario| scenario.name == "bench-firefox")
            .unwrap();
        assert_eq!(firefox.source_kind, SourceKind::Clone);
        assert!(firefox.tags.contains(&"suite:bench".to_string()));

        let c_hello = scenarios
            .iter()
            .find(|scenario| scenario.name == "e2e-c-hello")
            .unwrap();
        assert_eq!(c_hello.source_kind, SourceKind::Fixture);
        assert!(c_hello.tags.contains(&"suite:e2e".to_string()));
    }

    #[test]
    fn parses_unified_fixture_scenario_config() {
        let cfg: ScenarioConfig = toml::from_str(
            r#"
name = "c-hello"
tags = ["lang:cc"]

[source]
kind = "fixture"
path = "source"

[commands]
build = "make"
clean = "make clean"

[checks.measure.warm]
min_hit_rate_pct = 90.0
"#,
        )
        .unwrap();
        assert_eq!(cfg.name, "c-hello");
        assert_eq!(cfg.source.kind(), SourceKind::Fixture);
        assert_eq!(
            cfg.phases_or_default(),
            vec![Phase::Cold, Phase::Warm, Phase::Noop, Phase::Relocate]
        );
        assert_eq!(cfg.commands.clean.as_deref(), Some("make clean"));
        assert_eq!(
            cfg.checks
                .measure
                .warm
                .as_ref()
                .and_then(|m| m.min_hit_rate_pct),
            Some(90.0)
        );

        let scenario = cfg.into_scenario();
        assert_eq!(scenario.name, "c-hello");
        assert_eq!(scenario.source.kind(), SourceKind::Fixture);
        assert_eq!(scenario.commands.clean.as_deref(), Some("make clean"));
    }

    #[test]
    fn fixture_negative_control_exempt_must_be_top_level() {
        let cfg: Fixture = toml::from_str(
            r#"
name = "e2e-exempt"
negative_control_exempt = true

[source]
kind = "fixture"
path = "source"

[commands]
build = "make"
clean = "make clean"
"#,
        )
        .unwrap();
        assert!(cfg.negative_control_exempt);
    }

    #[test]
    fn source_table_rejects_misplaced_fixture_fields() {
        let err = toml::from_str::<Fixture>(
            r#"
name = "e2e-exempt"

[source]
kind = "fixture"
path = "source"
negative_control_exempt = true

[commands]
build = "make"
clean = "make clean"
"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn parses_unified_clone_scenario_config() {
        let cfg: ScenarioConfig = toml::from_str(
            r#"
name = "substrate"
tags = ["lang:rust"]
phases = ["cold", "warm"]

[source]
kind = "clone"
repo = "https://example.com/repo.git"
ref = "v1"
objdir = "target"

[commands]
setup = ["rustup target add wasm32-unknown-unknown"]
build = "cargo build"

[checks.assert.warm]
min_key_stability_pct = 80.0
max_passthrough_pct = 30.0
max_errors = 0
"#,
        )
        .unwrap();
        assert_eq!(cfg.name, "substrate");
        assert_eq!(cfg.source.kind(), SourceKind::Clone);
        assert_eq!(cfg.phases, vec![Phase::Cold, Phase::Warm]);
        assert_eq!(cfg.commands.setup.len(), 1);
        let assert = cfg.checks.assertions.warm.as_ref().unwrap();
        assert_eq!(assert.min_key_stability_pct, Some(80.0));
        assert_eq!(assert.max_passthrough_pct, Some(30.0));
        assert_eq!(assert.max_errors, Some(0));
    }

    #[test]
    fn pull_phase_gates_bind() {
        let checks: ScenarioChecks = toml::from_str(
            r#"
[assert.pull]
max_passthrough_pct = 40.0
max_errors = 0

[measure.pull]
min_hit_rate_pct = 50.0
"#,
        )
        .unwrap();

        let assert = checks
            .assertions
            .for_phase("pull")
            .expect("pull assert gate must bind");
        assert_eq!(assert.max_passthrough_pct, Some(40.0));
        assert_eq!(assert.max_errors, Some(0));

        let measure = checks
            .measure
            .for_phase("pull")
            .expect("pull measure gate must bind");
        assert_eq!(measure.min_hit_rate_pct, Some(50.0));
    }

    #[test]
    fn scenario_source_config_parses_ref_next() {
        let cfg: ScenarioConfig = toml::from_str(
            r#"
name = "bench-firefox-pull"

[source]
kind = "clone"
repo = "https://github.com/mozilla-firefox/firefox.git"
ref = "24e52d9437e3fc676335893e579624458bc34789"
ref_next = "73c9f54454133a705eaceec1a43be995540e3ab4"
objdir = "obj-kache-bench"

[commands]
build = "./mach build"
"#,
        )
        .unwrap();

        match cfg.source {
            ScenarioSourceConfig::Clone { ref ref_next, .. } => {
                assert_eq!(
                    ref_next.as_deref(),
                    Some("73c9f54454133a705eaceec1a43be995540e3ab4")
                );
            }
            _ => panic!("expected a clone source config"),
        }
    }
}
