//! Fixture metadata: parsed from `scenarios/e2e-*/scenario.toml`.
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

use crate::scenario::{ScenarioChecks, Selectors, SourceKind, discover_metadata};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureSourceConfig {
    pub kind: SourceKind,
    pub path: PathBuf,
}

/// A single example project the harness will drive.
#[derive(Debug, Clone, Deserialize)]
pub struct Fixture {
    /// Human-readable identifier; used in result JSON and CLI output.
    /// Must match the scenario directory name (the harness checks this).
    pub name: String,

    /// Source materialization. For e2e scenarios this points at the checked-in
    /// source tree, normally `source`.
    #[serde(default)]
    pub source: Option<FixtureSourceConfig>,

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

    /// Declarative selection tags, e.g. `lang:rust`, `os:windows`,
    /// `tier:slow`. The source kind (`source:fixture`) is implicit, and
    /// `tier:gate` is added unless the fixture declares another tier.
    #[serde(default)]
    pub tags: Vec<String>,

    /// Shared scenario checks. Fixture `[assertions]` remain the blocking
    /// correctness contract; `[checks.measure.<phase>]` is warn-only.
    #[serde(default)]
    pub checks: ScenarioChecks,

    /// Optional differential test — artifact files whose bytes must be
    /// identical between the cold build (a real compile) and every
    /// cache-hit phase. See [`DiffSpec`].
    pub diff: Option<DiffSpec>,

    /// Optional invalidation test — a source edit that must force a
    /// recompile in the `relocate-modified` phase. See [`ModifySpec`].
    pub modify: Option<ModifySpec>,

    /// Exempt this fixture from the `--negative-control` falsifiability
    /// check. That check reruns every fixture with `KACHE_DISABLED=1`
    /// and asserts the result flips to a failure — proving the
    /// fixture's caching assertions genuinely depend on kache. A
    /// passthrough fixture (e.g. `c-passthrough`) has no such
    /// assertion: disabling kache is indistinguishable from the
    /// refuse-and-passthrough it already exercises, so it legitimately
    /// still passes. Mark those `true` so negative-control expects a
    /// pass, not a flip.
    #[serde(default)]
    pub negative_control_exempt: bool,

    /// Assert that restored dep-info (`.d`) files are path-expanded.
    ///
    /// kache relativizes `.d` files on store (`<target>/...` → `./...`)
    /// and must expand them back on restore. A restored `.d` still
    /// carrying the `./debug/` / `./release/` relativization sentinel
    /// means the restore-side expansion silently failed — the #100 bug
    /// class. This is a *direct* artifact assertion: it inspects the
    /// `.d` content rather than relying on the `should_not_recompile`
    /// proxy, which can pass even when the `.d` is broken. Opt-in — set
    /// `true` for Rust fixtures whose dependencies get `.d`s cached.
    #[serde(default)]
    pub check_depinfo: bool,

    /// External executables that must be on `PATH` for this fixture to
    /// run. If any is missing the harness SKIPS the fixture (status
    /// `"skip"`) rather than failing it — for fixtures that exercise an
    /// optional third-party tool, e.g. `rust-sccache` needs `sccache`
    /// on PATH to test the `KACHE_FALLBACK` delegation. A skipped
    /// fixture is also passed over by `--negative-control`: it was
    /// never evaluated, so there is nothing to falsify.
    #[serde(default)]
    pub requires: Vec<String>,

    /// Operating systems this fixture supports, by `std::env::consts::OS`
    /// value (`"linux"`, `"macos"`, `"windows"`). Empty (default) = all.
    /// When non-empty and the current OS isn't listed, the harness SKIPS the
    /// fixture (like a missing `requires` tool) instead of running it — for
    /// fixtures that only make sense on some platforms, e.g. the cc out-of-tree
    /// convergence scenario which exercises the gnu/clang prefix-map path and
    /// not MSVC `cl` (kunobi-ninja/kache#394).
    #[serde(default)]
    pub os: Vec<String>,

    /// Overrides applied only when the harness runs on Windows (see
    /// [`OsOverride`]). Parsed on every platform but consulted only there.
    #[serde(default)]
    pub windows: Option<OsOverride>,

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

/// Platform-specific overrides, applied when the harness runs on that
/// platform. Each present field replaces its base counterpart; absent fields
/// leave the base untouched. Currently only `[windows]` is consumed — e.g.
/// `rust-c-ffi` must build with the `x86_64-pc-windows-gnu` target there
/// (MinGW C objects can't link into an MSVC Rust binary), which changes the
/// build command and the artifact output paths.
#[derive(Debug, Clone, Deserialize)]
pub struct OsOverride {
    /// Replaces `commands.build`.
    #[serde(default)]
    pub build: Option<String>,
    /// Replaces `verify.run` (only when the fixture has a `[verify]`).
    #[serde(default)]
    pub run: Option<String>,
    /// Replaces `diff.artifacts` (only when the fixture has a `[diff]`).
    #[serde(default)]
    pub artifacts: Option<Vec<String>>,
    /// Merged into `[env]` (override-wins per key). Values ARE token-expanded
    /// (`$KACHE` / `$KACHE_CC` / `$KACHE_CXX` / `$FALLBACK`), because the merge
    /// happens before the expansion pass in [`Fixture::load`]. Used to retarget
    /// a compiler on Windows, e.g. `rust-c-ffi` routes its build.rs C half
    /// through the `$KACHE_CC` / `$KACHE_CXX` shims instead of `$KACHE cc`,
    /// because cc-rs's `check_exe` mangles a multi-token `"<path>.exe cc"` CC
    /// into bare `<path>.exe` on Windows (the `.exe` suffix makes
    /// `set_extension` swallow the ` cc` arg), dropping the subcommand.
    #[serde(default)]
    pub env: HashMap<String, String>,
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
    /// Optional positive counterpart: substrings that MUST appear in the
    /// artifact (via the same `strings` scan). Use to assert that a resolvable
    /// remap target actually landed in the debug info, e.g. `["/kache/.cargo"]`
    /// (kunobi-ninja/kache#485). Empty / unset = skip. Like the forbidden scan
    /// this is a Unix-only lens (skipped where `strings` is unavailable).
    #[serde(default)]
    pub required_substrings: Vec<String>,
    /// As [`Verify::required_substrings`] but only asserted on Linux. For
    /// targets that differ by OS — notably `/proc/self/cwd`, the Linux-only
    /// workspace remap target (other platforms bake `/kache/workspace`).
    #[serde(default)]
    pub required_substrings_linux: Vec<String>,
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
    /// Relocate-noop phase: a SECOND build in the relocated tree,
    /// without cleaning, right after `relocate`. Reuses
    /// [`NoopAssertions`] — the contract is "nothing recompiles".
    ///
    /// Unlike the in-place `noop`, this catches stale dep-info: a
    /// `relocate` cache hit restores each crate's `.d` file, and if
    /// that `.d` carries the producing build's absolute paths, cargo's
    /// freshness `stat()` fails at the relocated path and recompiles
    /// the crate on the next build. The in-place `noop` rebuilds where
    /// the `.d` paths are trivially valid, so only this phase exposes
    /// the bug. Per-fixture opt-in; declaring it is what makes the
    /// phase run at all. Deserialized from `[assertions.relocate-noop]`.
    #[serde(rename = "relocate-noop")]
    pub relocate_noop: Option<NoopAssertions>,
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

/// Expand the harness tokens inside an env value.
///
/// Two tokens are recognized (no `${VAR}`, no `~`, no `$OTHER` — intentional,
/// see module docs): `$KACHE` → the kache binary under test, and `$FALLBACK` →
/// the cross-platform `e2e-fallback` wrapper (used as `KACHE_FALLBACK`). Neither
/// token is a substring of the other, so replace order is irrelevant.
fn expand_tokens(value: &str, kache_path: &Path, fallback_path: &Path) -> String {
    // The `kache-cc` / `kache-cxx` shims are built into the same dir as the
    // real `kache` (cargo puts all bins under `target/<profile>/`), so derive
    // their paths from `kache_path`'s parent. `$KACHE_CC` / `$KACHE_CXX` MUST
    // be replaced before `$KACHE`, else the longer token's `$KACHE` prefix
    // would be substituted first and leave a dangling `_CC`.
    let sibling = |stem: &str| -> String {
        kache_path
            .parent()
            .map(|dir| {
                crate::portable_path(&dir.join(format!("{stem}{}", std::env::consts::EXE_SUFFIX)))
                    .display()
                    .to_string()
            })
            .unwrap_or_default()
    };
    value
        .replace("$KACHE_CXX", &sibling("kache-cxx"))
        .replace("$KACHE_CC", &sibling("kache-cc"))
        .replace("$KACHE", &kache_path.display().to_string())
        .replace("$FALLBACK", &fallback_path.display().to_string())
}

impl Fixture {
    /// Replace base fields with the `[windows]` overrides (build command, the
    /// verify run path, and the diff artifact paths). Each is applied only when
    /// both the override and its target section are present. Clone the override
    /// out first so we don't borrow `self` immutably and mutably at once.
    fn apply_windows_overrides(&mut self) {
        let Some(ov) = self.windows.clone() else {
            return;
        };
        if let Some(build) = ov.build {
            self.commands.build = build;
        }
        if let (Some(run), Some(verify)) = (ov.run, self.verify.as_mut()) {
            verify.run = run;
        }
        if let (Some(artifacts), Some(diff)) = (ov.artifacts, self.diff.as_mut()) {
            diff.artifacts = artifacts;
        }
        // Merge env overrides last (override-wins). `load` runs this BEFORE
        // its token-expansion pass, so these values get `$KACHE`/`$KACHE_CC`/
        // … expanded just like the base env.
        for (key, value) in ov.env {
            self.env.insert(key, value);
        }
    }

    /// Load a fixture from `<dir>/scenario.toml`, expanding `$KACHE` and
    /// `$FALLBACK` in env values against the binaries under test.
    pub fn load(dir: &Path, kache_path: &Path, fallback_path: &Path) -> Result<Self> {
        let toml_path = dir.join("scenario.toml");
        let raw = std::fs::read_to_string(&toml_path)
            .with_context(|| format!("reading {}", toml_path.display()))?;
        let mut fixture: Self =
            toml::from_str(&raw).with_context(|| format!("parsing {}", toml_path.display()))?;

        // Sanity: `name` must match scenario directory. Catches copy-paste bugs
        // where a fixture is duplicated and the new name slot wasn't
        // updated; would otherwise silently double-count in results.
        let scenario_name = dir
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow!("fixture dir has no usable name: {}", dir.display()))?;
        if fixture.name != scenario_name {
            return Err(anyhow!(
                "fixture name `{}` does not match scenario directory `{}`",
                fixture.name,
                scenario_name
            ));
        }
        let source_dir = match &fixture.source {
            Some(source) => {
                if source.kind != SourceKind::Fixture {
                    return Err(anyhow!(
                        "scenario `{}` has source kind `{}`, expected `fixture`",
                        fixture.name,
                        source.kind.as_str()
                    ));
                }
                dir.join(&source.path)
            }
            None => dir.to_path_buf(),
        };

        // Apply platform overrides BEFORE token expansion so the
        // `[windows].env` values (which may use `$KACHE_CC` etc.) are
        // expanded too. Runtime `cfg!` (not `#[cfg]`) so the `windows` field
        // counts as read on every platform — the table is parsed everywhere
        // but only consulted on its target OS.
        if cfg!(windows) {
            fixture.apply_windows_overrides();
        }

        // Expand $KACHE / $FALLBACK / $KACHE_CC / $KACHE_CXX in env values;
        // runners receive a pre-resolved env map and don't need to know about
        // the binary paths.
        for value in fixture.env.values_mut() {
            *value = expand_tokens(value, kache_path, fallback_path);
        }

        // Normalize so the dir is safe as a `cp -R <dir>/.` source (MSYS
        // coreutils) and as a working directory on Windows — see
        // `crate::portable_path`.
        fixture.dir = crate::portable_path(
            &source_dir
                .canonicalize()
                .with_context(|| format!("canonicalize {}", source_dir.display()))?,
        );
        Ok(fixture)
    }
}

/// Discover every fixture scenario under `root` (looking for
/// `*/scenario.toml` with `source.kind = "fixture"`).
///
/// Returns fixtures sorted by name for stable result ordering. Directories
/// without a fixture scenario are silently skipped so clone benchmarks can
/// share the same `scenarios/` root.
pub fn discover(
    root: &Path,
    selectors: &Selectors,
    kache_path: &Path,
    fallback_path: &Path,
) -> Result<IndexMap<String, Fixture>> {
    let mut out: Vec<Fixture> = Vec::new();
    for metadata in discover_metadata(root)? {
        if !selectors.matches_metadata(&metadata) {
            continue;
        }
        if metadata.source_kind != SourceKind::Fixture {
            continue;
        }
        out.push(Fixture::load(&metadata.dir, kache_path, fallback_path)?);
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
    fn expand_tokens_substitutes_paths() {
        let kache = Path::new("/usr/local/bin/kache");
        let fb = Path::new("/tmp/e2e-fallback");
        assert_eq!(
            expand_tokens("$KACHE cc", kache, fb),
            "/usr/local/bin/kache cc"
        );
        assert_eq!(expand_tokens("$KACHE", kache, fb), "/usr/local/bin/kache");
        assert_eq!(expand_tokens("$FALLBACK", kache, fb), "/tmp/e2e-fallback");
    }

    #[test]
    fn expand_tokens_substitutes_compiler_shims_before_kache() {
        // $KACHE_CC / $KACHE_CXX resolve to siblings of $KACHE and MUST be
        // replaced before $KACHE — otherwise "$KACHE_CC" would become
        // "<kache>_CC". Suffix-aware so it holds on Windows too.
        let kache = Path::new("/opt/k/kache");
        let fb = Path::new("/tmp/e2e-fallback");
        let sfx = std::env::consts::EXE_SUFFIX;
        assert_eq!(
            expand_tokens("$KACHE_CC", kache, fb),
            format!("/opt/k/kache-cc{sfx}")
        );
        assert_eq!(
            expand_tokens("$KACHE_CXX", kache, fb),
            format!("/opt/k/kache-cxx{sfx}")
        );
        assert_eq!(expand_tokens("$KACHE", kache, fb), "/opt/k/kache");
    }

    #[test]
    fn expand_tokens_leaves_other_dollar_refs_alone() {
        // Documents the deliberate restriction: only $KACHE / $FALLBACK are
        // special. If a fixture wants HOME or PATH, it must declare it explicitly.
        let k = Path::new("/k");
        let fb = Path::new("/fb");
        assert_eq!(expand_tokens("$HOME/.cache", k, fb), "$HOME/.cache");
        assert_eq!(expand_tokens("${KACHE}", k, fb), "${KACHE}");
    }
}
