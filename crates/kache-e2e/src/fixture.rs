//! Fixture metadata: parsed `kache-fixture.toml` per example project.
//!
//! Each fixture declares **what it is** (env, commands, verify, assertions);
//! the harness owns **what the lifecycle is** (cold → warm → noop). This
//! split keeps the toml minimal — adding a new fixture is "drop a directory
//! with a toml" rather than "drop a directory and edit the harness".
//!
//! ## $KACHE expansion
//!
//! Env values may reference `$KACHE`, which is replaced at load time with the
//! absolute path to the kache binary under test. This is the *only* string
//! interpolation the harness performs — it deliberately does NOT support
//! shell expansion of arbitrary variables, because fixtures should be
//! reproducible regardless of the user's environment.

use anyhow::{Context, Result, anyhow};
use indexmap::IndexMap;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// A single example project the harness will drive.
#[derive(Debug, Clone, Deserialize)]
pub struct Fixture {
    /// Human-readable identifier; used in result JSON and CLI output.
    /// Must match the fixture's directory name (the harness checks this).
    pub name: String,

    /// Environment variables exported when running [`Self::commands`].
    /// Values may contain `$KACHE` (replaced with the kache binary path)
    /// — see module docs.
    #[serde(default)]
    pub env: HashMap<String, String>,

    /// Named shell commands. The runner looks up `"build"` and `"clean"`
    /// by convention; fixtures may define additional commands but the
    /// harness will not invoke them.
    pub commands: Commands,

    /// Optional artifact verification (run-the-binary, check stdout).
    /// If absent, the harness only checks build exit codes.
    pub verify: Option<Verify>,

    /// Per-phase assertions. A missing entry means "measure but don't
    /// pass/fail" for that phase.
    #[serde(default)]
    pub assertions: PhaseAssertions,

    /// Optional differential test — artifact files whose bytes must be
    /// identical between the cold build (a real compile) and every
    /// cache-hit phase. See [`DiffSpec`].
    pub diff: Option<DiffSpec>,

    /// Optional invalidation test — a source edit that must force a
    /// recompile in the `relocate-modified` phase. See [`ModifySpec`].
    pub modify: Option<ModifySpec>,

    /// Absolute path to the fixture directory (set at load time, not
    /// in the toml).
    #[serde(skip)]
    pub dir: PathBuf,
}

/// Differential-test spec (`[diff]` in the fixture toml).
///
/// Runtime verification only proves the built program *behaves*
/// right — a subtly wrong `.o` that still prints the expected output
/// would pass. This pins the artifact down at the byte level: the
/// object a cache hit restores must be byte-for-byte identical to the
/// one the real compiler produced on the cold build. Catches
/// store/restore corruption and wrong-key miscaches.
#[derive(Debug, Clone, Deserialize)]
pub struct DiffSpec {
    /// Artifact paths relative to the project root (e.g.
    /// `build/foo.o`). Compared after cold (the baseline) and after
    /// every cache-hit phase.
    pub artifacts: Vec<String>,
}

/// Invalidation-test spec (`[modify]` in the fixture toml).
///
/// Drives the `relocate-modified` phase: relocate the project, apply
/// this one find/replace edit to a source file, rebuild. The build
/// MUST be a cache miss — proving the key stays content-sensitive even
/// across a relocation. Guards the stale-restore bug class (a content
/// change wrongly served from cache).
#[derive(Debug, Clone, Deserialize)]
pub struct ModifySpec {
    /// Source file to edit, relative to the project root.
    pub file: String,
    /// Substring to replace. Must occur in `file`, or the harness
    /// fails the phase — a no-op edit would make the test vacuous.
    pub find: String,
    /// Replacement substring. Must change the preprocessed/compiled
    /// output (not just a comment) so the cache key actually diverges.
    pub replace: String,
}

/// Required shell commands. `build` runs the compiler under kache;
/// `clean` resets the fixture to a pre-build state. Both are run via
/// `sh -c "<value>"` with `cwd = fixture.dir` and `env = fixture.env`.
#[derive(Debug, Clone, Deserialize)]
pub struct Commands {
    pub build: String,
    pub clean: String,
}

/// How to verify the compiled artifact actually works.
///
/// Runs after every successful `build` step. The contract: spawn `run`,
/// wait up to `timeout_s`, assert `expected_exit_code` and that every
/// string in `expected_stdout_contains` appears in stdout.
///
/// Optionally inspects the binary file's bytes (via `strings`) for
/// substrings that must NOT appear — a defense-in-depth check that
/// catches output-byte path leaks. Without this, the harness only
/// sees runtime behavior, so a binary embedding the wrong path
/// passes verify whenever the path happens to still resolve at
/// runtime (e.g. cold/warm/noop populated it). With it, a leak in
/// `--remap-path-prefix` injection (or any future regression that
/// embeds machine-local paths in DWARF / panic strings / track_caller)
/// surfaces structurally.
#[derive(Debug, Clone, Deserialize)]
pub struct Verify {
    /// Shell command (relative paths resolve against fixture dir).
    pub run: String,
    #[serde(default)]
    pub expected_exit_code: i32,
    #[serde(default)]
    pub expected_stdout_contains: Vec<String>,
    #[serde(default = "default_verify_timeout")]
    pub timeout_s: u64,
    /// Optional binary-content inspection. The harness reads the
    /// artifact at [`Verify::run`] (the executable path) via
    /// `strings`, then checks that NONE of these substrings appear.
    /// Use to assert "no machine-local paths leaked into this
    /// binary's debug info" (e.g. `["/Users/", "/home/", "/private/tmp/"]`).
    /// Empty / unset = skip the check entirely.
    #[serde(default)]
    pub forbidden_substrings: Vec<String>,
}

fn default_verify_timeout() -> u64 {
    30
}

/// Per-phase assertion bundle. Absent phases skip assertion checks
/// entirely (the phase still runs and is measured).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PhaseAssertions {
    pub cold: Option<MetricAssertions>,
    pub warm: Option<MetricAssertions>,
    pub noop: Option<NoopAssertions>,
    /// Relocate phase: same source built from a *different* absolute
    /// path with the same cache. Catches the bug class where build
    /// directory / `$HOME` / target paths leak into the cache key —
    /// without this assertion, a path-leak bug is invisible because
    /// every other phase rebuilds at the same path and trivially hits.
    /// Per-fixture opt-in. Reuses [`MetricAssertions`] (same
    /// `min_hits`, `min_hit_rate_pct`, `max_misses` etc.).
    pub relocate: Option<MetricAssertions>,
    /// Relocate-then-modify phase: the project is relocated AND a
    /// source file is edited (see [`ModifySpec`]) before the build.
    /// The contract is the inverse of `relocate` — the build MUST
    /// miss (`min_misses = 1`), proving the cache key reacts to
    /// content changes and does not serve a stale artifact.
    /// Deserialized from `[assertions.relocate-modified]`.
    #[serde(rename = "relocate-modified")]
    pub relocate_modified: Option<MetricAssertions>,
}

/// Assertions applied against `kache report --format json` output.
///
/// Field names map 1:1 to the report's `summary` object. Each field is
/// opt-in (`Option<...>`) — declaring only the constraints that matter
/// for this fixture keeps the toml signal-to-noise high.
#[derive(Debug, Clone, Deserialize)]
pub struct MetricAssertions {
    /// Lower bound on `summary.total_crates` (events seen this phase).
    /// Useful as a coarse "did anything land in the cache" check.
    pub min_entries_after: Option<u64>,
    /// Upper bound on `summary.total_crates`. Skeleton fixtures use
    /// this to assert "still nothing cached" until real caching lands.
    pub max_entries_after: Option<u64>,
    /// Lower bound on `local_hits + prefetch_hits + remote_hits`.
    pub min_hits: Option<u64>,
    /// Lower bound on `summary.misses`. Used by fixtures whose
    /// contract is "must NOT cache-hit on relocate" — e.g.
    /// `out-dir-runtime` where the binary embeds OUT_DIR and a
    /// false hit would silently restore the wrong path.
    pub min_misses: Option<u64>,
    /// Upper bound on `summary.misses`.
    pub max_misses: Option<u64>,
    /// Lower bound on `summary.hit_rate_pct`.
    pub min_hit_rate_pct: Option<f64>,
    /// Per-crate miss-count lower bound. Map: `crate_name` →
    /// minimum miss count for that crate in this phase.
    ///
    /// Aggregate `min_misses` works when the contract is "at least
    /// N total crates miss". This field works when the contract is
    /// "this *specific* crate must miss" — used by `out-dir-runtime`
    /// to enforce that the env!()-as-value crate's key correctly
    /// diverged on relocate, regardless of what other crates in
    /// the build graph (build.rs binary, etc.) did. Without this
    /// tighter assertion, an unrelated miss in the same phase
    /// could mask a false hit on the OUT_DIR-using crate.
    #[serde(default)]
    pub min_misses_per_crate: std::collections::HashMap<String, u64>,
    /// Upper bound on the compiler spawns summed across this phase's
    /// events. `0` is the headline cache assertion — a phase that
    /// fully hits must not spawn the compiler at all. Deterministic
    /// (a count, not a timing), so it is safe to gate CI on.
    pub max_compiler_runs: Option<u32>,
    /// Upper bound on the preprocessor spawns (`cc -E`) summed across
    /// this phase's events. Documents the per-compile C/C++ key
    /// overhead and guards against a regression that runs the
    /// preprocessor more than once per compile.
    pub max_preprocessor_runs: Option<u32>,
    /// Upper bound on compiler-probe spawns (`cc --version` / `cc -###`)
    /// summed across this phase's events. `0` on a warm phase proves
    /// the probe is memoized across builds; `1` on a cold phase proves
    /// it runs once for the build, not once per translation unit.
    pub max_probe_runs: Option<u32>,
}

/// No-op phase assertions. The no-op phase rebuilds without cleaning;
/// the contract is "nothing should recompile". The harness checks this
/// by grepping the build's stdout for [`NoopAssertions::recompile_marker`]
/// (e.g. cargo's `"Compiling"`).
#[derive(Debug, Clone, Deserialize)]
pub struct NoopAssertions {
    /// If `true` and the marker appears in stdout, the assertion fails.
    /// If `false`, the assertion passes regardless (used by skeleton
    /// fixtures where caching isn't implemented yet).
    pub should_not_recompile: bool,
    /// String to search for in build stdout. Required when
    /// `should_not_recompile = true`. Cargo emits `"Compiling"`; CMake
    /// emits `"Building"`; bare make emits nothing useful (so make
    /// fixtures generally can't enforce no-op semantics).
    pub recompile_marker: Option<String>,
}

/// Result of expanding `$KACHE` inside an env value.
///
/// Returned as a borrowed `Cow`-equivalent shape so values without
/// `$KACHE` skip allocation.
fn expand_kache(value: &str, kache_path: &Path) -> String {
    // Bounded substitution: only `$KACHE` is recognized (no `${VAR}`,
    // no `~`, no `$OTHER`). Documented in module docs as intentional.
    value.replace("$KACHE", &kache_path.display().to_string())
}

impl Fixture {
    /// Load a fixture from `<dir>/kache-fixture.toml`, expanding `$KACHE`
    /// in env values against `kache_path`.
    pub fn load(dir: &Path, kache_path: &Path) -> Result<Self> {
        let toml_path = dir.join("kache-fixture.toml");
        let raw = std::fs::read_to_string(&toml_path)
            .with_context(|| format!("reading {}", toml_path.display()))?;
        let mut fixture: Self =
            toml::from_str(&raw).with_context(|| format!("parsing {}", toml_path.display()))?;

        // Sanity: `name` must match directory. Catches copy-paste bugs
        // where a fixture is duplicated and the new name slot wasn't
        // updated; would otherwise silently double-count in results.
        let dir_name = dir
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow!("fixture dir has no usable name: {}", dir.display()))?;
        if fixture.name != dir_name {
            return Err(anyhow!(
                "fixture name `{}` does not match directory `{}`",
                fixture.name,
                dir_name
            ));
        }

        // Expand $KACHE in env values up-front; runners receive a
        // pre-resolved env map and don't need to know about kache_path.
        for value in fixture.env.values_mut() {
            *value = expand_kache(value, kache_path);
        }

        fixture.dir = dir
            .canonicalize()
            .with_context(|| format!("canonicalize {}", dir.display()))?;
        Ok(fixture)
    }
}

/// Discover every fixture under `root` (looking for `*/kache-fixture.toml`).
///
/// Returns fixtures sorted by name for stable result ordering. Directories
/// without a `kache-fixture.toml` are silently skipped — that's intentional,
/// it lets `test-projects/` host both harness-driven and exploratory
/// projects without the latter blowing up the runner.
pub fn discover(root: &Path, kache_path: &Path) -> Result<IndexMap<String, Fixture>> {
    let mut out: Vec<Fixture> = Vec::new();
    let entries = std::fs::read_dir(root).with_context(|| format!("reading {}", root.display()))?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if !path.join("kache-fixture.toml").exists() {
            continue;
        }
        out.push(Fixture::load(&path, kache_path)?);
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));

    let mut map = IndexMap::new();
    for fixture in out {
        map.insert(fixture.name.clone(), fixture);
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expand_kache_substitutes_path() {
        let path = Path::new("/usr/local/bin/kache");
        assert_eq!(expand_kache("$KACHE cc", path), "/usr/local/bin/kache cc");
        assert_eq!(expand_kache("$KACHE", path), "/usr/local/bin/kache");
    }

    #[test]
    fn expand_kache_leaves_other_dollar_refs_alone() {
        // Documents the deliberate restriction: only $KACHE is special.
        // If a fixture wants HOME or PATH, it must declare it explicitly.
        let path = Path::new("/k");
        assert_eq!(expand_kache("$HOME/.cache", path), "$HOME/.cache");
        assert_eq!(expand_kache("${KACHE}", path), "${KACHE}");
    }
}
