//! Bench scenarios: the per-project description the scenario runner loads.
//!
//! The clone benchmark engine owns the *measurement* lifecycle (clone → cold build →
//! warm build → report → verdict); a **scenario** owns everything
//! *project-specific*: which repo to clone, how to wire kache into the
//! build, and how to build. This is the same split the e2e harness uses
//! with [`crate::fixture`] — the harness owns the lifecycle, the TOML
//! owns *what this project is*.
//!
//! See `scenarios/README.md` for the authored reference.

use anyhow::{Context, Result, bail};
use indexmap::IndexMap;
use serde::Deserialize;
use std::path::{Path, PathBuf};

use crate::scenario::{ScenarioChecks, Selectors, SourceKind, discover_metadata};

/// A project the clone benchmark engine can benchmark, parsed from
/// `scenarios/bench-*/scenario.toml`.
#[derive(Debug, Clone, Deserialize)]
pub struct BenchProfile {
    /// Scenario id; must match the scenario directory (`bench-firefox`).
    pub name: String,

    /// Clone source metadata. New scenarios declare this under `[source]`;
    /// legacy profile TOMLs may still use top-level `repo` / `ref` / `objdir`.
    #[serde(default)]
    source: Option<BenchSourceConfig>,

    /// Git URL to clone.
    #[serde(default)]
    pub repo: String,

    /// Tag / branch / commit to pin for reproducibility. Overridable on
    /// the CLI with `--ref`.
    #[serde(default, rename = "ref")]
    pub git_ref: String,

    /// Build output directory, wiped before each phase so every build is
    /// genuinely from-scratch (`obj-bench` for Firefox, `target` for a
    /// Cargo project).
    #[serde(default)]
    pub objdir: String,

    /// Declarative selection tags, e.g. `suite:bench`, `project:firefox`.
    /// The source kind (`source:clone`) is implicit, and `tier:nightly` is
    /// added unless the scenario declares another tier.
    #[serde(default)]
    pub tags: Vec<String>,

    /// Shared scenario checks. `checks.assert` drives the bench verdict;
    /// `checks.measure` is advisory only.
    #[serde(default)]
    pub checks: ScenarioChecks,

    /// Executables that must be on `PATH`; the run is SKIPPED (not
    /// failed) if any is missing — same falsifiability convention as
    /// e2e fixtures' `requires`.
    #[serde(default)]
    pub requires: Vec<String>,

    /// One-time setup shell steps run once after a fresh clone (e.g.
    /// `./mach bootstrap`, `rustup target add`). Each runs via `sh -c`
    /// in the checkout. Skipped on `--skip-clone` / `--retry`, or when
    /// [`Self::setup_marker`] already exists.
    #[serde(default)]
    pub setup: Vec<String>,

    /// Optional path whose existence means "setup already done, skip it"
    /// (e.g. `~/.mozbuild`). `~` expands to the home dir. `--force-setup`
    /// overrides. Without a marker, `setup` runs on every fresh clone.
    #[serde(default)]
    pub setup_marker: Option<String>,

    /// Build command, run via `sh -c` in the checkout with the kache env
    /// injected. Wall-clock of this command is the measured build time.
    pub build: String,

    /// Extra env vars for every build, added on top of the baseline kache
    /// env (`KACHE_CACHE_DIR` / `KACHE_CONFIG` / `RUSTC_WRAPPER` / …).
    #[serde(default)]
    pub env: IndexMap<String, String>,

    /// File injections applied to the (freshly reset) checkout before each
    /// build — how kache gets wired in when env alone is not enough.
    /// Declared in TOML as `[[file]]` (array-of-tables).
    #[serde(default, rename = "file")]
    pub files: Vec<FileInject>,

    /// Absolute path to the scenario file's directory (set at load time,
    /// used to resolve a relative `content_file`). Not in the TOML.
    #[serde(skip)]
    pub dir: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
struct BenchSourceConfig {
    kind: SourceKind,
    repo: String,
    #[serde(rename = "ref")]
    git_ref: String,
    objdir: String,
}

/// One file injection. The `mode` selects how `content` is applied; it
/// defaults to [`FileMode::Write`].
#[derive(Debug, Clone, Deserialize)]
pub struct FileInject {
    /// Path relative to the checkout root.
    pub path: String,
    /// How to apply the payload. Default: `write`.
    #[serde(default)]
    pub mode: FileMode,
    /// The payload. For `write`/`append` it is the literal file text; for
    /// `patch` it is a unified diff. `{kache}` / `{objdir}` are
    /// interpolated. Exactly one of `content` / `content_file` is set.
    #[serde(default)]
    pub content: Option<String>,
    /// Read the payload from a file relative to the scenario dir instead of
    /// inline `content` (handy for large patches).
    #[serde(default)]
    pub content_file: Option<String>,
}

/// How a [`FileInject`] payload is applied to the checkout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileMode {
    /// Create or replace the file. Use only for files the repo does NOT
    /// own (e.g. Firefox ships no `mozconfig`). Idempotent.
    #[default]
    Write,
    /// Append the payload to the end of an existing file. Use to extend a
    /// file the repo OWNS (e.g. its `.cargo/config.toml`) without
    /// clobbering it. Safe because the engine resets the worktree first.
    Append,
    /// `git apply` a unified diff. Escape hatch for editing existing
    /// source — fragile across `ref` bumps, so prefer `write`/`append`.
    Patch,
}

impl BenchProfile {
    /// Discover clone benchmark scenarios under `root`, filtered by tags/name.
    pub fn discover(root: &Path, selectors: &Selectors) -> Result<Vec<Self>> {
        let mut profiles = Vec::new();
        for metadata in discover_metadata(root)? {
            if !selectors.matches_metadata(&metadata) {
                continue;
            }
            if metadata.source_kind != SourceKind::Clone {
                continue;
            }
            profiles.push(Self::load(&metadata.toml_path)?);
        }
        Ok(profiles)
    }

    /// Load and validate a bench scenario from a `.toml` path.
    pub fn load(path: &Path) -> Result<Self> {
        let raw =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let mut profile: Self =
            toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?;

        let expected_name = expected_name_for_path(path);
        if profile.name != expected_name {
            bail!(
                "scenario `name = \"{}\"` does not match scenario directory `{}`",
                profile.name,
                expected_name
            );
        }
        if let Some(source) = profile.source.take() {
            if source.kind != SourceKind::Clone {
                bail!(
                    "bench scenario `{}` has source kind `{}`, expected `clone`",
                    profile.name,
                    source.kind.as_str()
                );
            }
            profile.repo = source.repo;
            profile.git_ref = source.git_ref;
            profile.objdir = source.objdir;
        }
        if profile.repo.is_empty() || profile.git_ref.is_empty() || profile.objdir.is_empty() {
            bail!(
                "bench scenario `{}` needs clone source repo/ref/objdir",
                profile.name
            );
        }
        for f in &profile.files {
            match (&f.content, &f.content_file) {
                (Some(_), Some(_)) => {
                    bail!("file `{}`: set only one of content / content_file", f.path)
                }
                (None, None) => bail!("file `{}`: needs content or content_file", f.path),
                _ => {}
            }
        }
        profile.dir = path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        Ok(profile)
    }

    /// Names of any `requires` executables missing from `PATH`.
    pub fn missing_requirements(&self) -> Vec<String> {
        self.requires
            .iter()
            .filter(|tool| which_on_path(tool).is_none())
            .cloned()
            .collect()
    }

    /// Substitute `{kache}` / `{objdir}` placeholders.
    pub fn interpolate(&self, s: &str, kache: &Path) -> String {
        s.replace("{kache}", &kache.display().to_string())
            .replace("{objdir}", &self.objdir)
    }

    /// The build env as interpolated `(key, value)` pairs (scenario `env`
    /// only — the baseline kache env is added by the engine).
    pub fn build_env(&self, kache: &Path) -> Vec<(String, String)> {
        self.env
            .iter()
            .map(|(k, v)| (k.clone(), self.interpolate(v, kache)))
            .collect()
    }

    /// Interpolated setup commands.
    pub fn setup_commands(&self, kache: &Path) -> Vec<String> {
        self.setup
            .iter()
            .map(|c| self.interpolate(c, kache))
            .collect()
    }

    /// Interpolated build command.
    pub fn build_command(&self, kache: &Path) -> String {
        self.interpolate(&self.build, kache)
    }

    /// Whether setup should be skipped because its marker already exists
    /// (and `force` is not set).
    pub fn setup_satisfied(&self, force: bool) -> bool {
        if force {
            return false;
        }
        match &self.setup_marker {
            Some(m) => expand_home(m).exists(),
            None => false,
        }
    }

    /// Apply every file injection to `checkout` (which must already be
    /// reset to a pristine state). Run before each phase's build.
    pub fn apply_files(&self, checkout: &Path, kache: &Path) -> Result<()> {
        for f in &self.files {
            let raw = match (&f.content, &f.content_file) {
                (Some(c), _) => c.clone(),
                (_, Some(rel)) => std::fs::read_to_string(self.dir.join(rel))
                    .with_context(|| format!("reading content_file {rel}"))?,
                _ => unreachable!("validated in load()"),
            };
            let payload = self.interpolate(&raw, kache);
            let target = checkout.join(&f.path);
            match f.mode {
                FileMode::Write => {
                    if let Some(parent) = target.parent() {
                        std::fs::create_dir_all(parent).ok();
                    }
                    std::fs::write(&target, &payload)
                        .with_context(|| format!("writing {}", target.display()))?;
                }
                FileMode::Append => {
                    use std::io::Write as _;
                    if let Some(parent) = target.parent() {
                        std::fs::create_dir_all(parent).ok();
                    }
                    let mut fh = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&target)
                        .with_context(|| format!("opening {} for append", target.display()))?;
                    fh.write_all(payload.as_bytes())
                        .with_context(|| format!("appending to {}", target.display()))?;
                }
                FileMode::Patch => {
                    use std::io::Write as _;
                    let mut child = std::process::Command::new("git")
                        .arg("-C")
                        .arg(checkout)
                        .args(["apply", "-"])
                        .stdin(std::process::Stdio::piped())
                        .spawn()
                        .context("spawning git apply")?;
                    child
                        .stdin
                        .take()
                        .expect("piped stdin")
                        .write_all(payload.as_bytes())
                        .context("writing diff to git apply")?;
                    let status = child.wait().context("waiting for git apply")?;
                    if !status.success() {
                        bail!(
                            "git apply failed for `{}` — a patch is tied to the target's exact \
                             lines; re-generate it after a `ref` bump",
                            f.path
                        );
                    }
                }
            }
        }
        Ok(())
    }
}

/// Expand a leading `~` to the home directory.
fn expand_home(p: &str) -> PathBuf {
    if let Some(rest) = p.strip_prefix("~/")
        && let Some(home) = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"))
    {
        return PathBuf::from(home).join(rest);
    }
    PathBuf::from(p)
}

fn expected_name_for_path(path: &Path) -> String {
    if path.file_name().and_then(|name| name.to_str()) == Some("scenario.toml") {
        return path
            .parent()
            .and_then(|parent| parent.file_name())
            .and_then(|name| name.to_str())
            .unwrap_or_default()
            .to_string();
    }
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or_default()
        .to_string()
}

/// Minimal `which`: first `PATH` entry containing an executable named
/// `tool`. Enough for the `requires` skip check.
fn which_on_path(tool: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path).find_map(|dir| executable_in_dir(&dir, tool))
}

/// Find an executable named `tool` in `dir`: the exact name, plus — on
/// Windows — the usual executable extensions, so a bare `cargo` matches
/// `cargo.exe` (PATH entries on Windows hold `*.exe`, not extensionless files).
fn executable_in_dir(dir: &Path, tool: &str) -> Option<PathBuf> {
    let exact = dir.join(tool);
    if exact.is_file() {
        return Some(exact);
    }
    #[cfg(windows)]
    for ext in ["exe", "cmd", "bat"] {
        let candidate = dir.join(format!("{tool}.{ext}"));
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executable_in_dir_finds_exact_and_windows_exe() {
        let dir = std::env::temp_dir().join(format!("kb-which-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // Exact name is found on every platform.
        std::fs::write(dir.join("bar"), b"x").unwrap();
        assert_eq!(executable_in_dir(&dir, "bar"), Some(dir.join("bar")));
        // A bare `foo` matches `foo.exe` on Windows (the real bug: `cargo`
        // matching `cargo.exe`); on Unix there's no extension to append.
        std::fs::write(dir.join("foo.exe"), b"x").unwrap();
        let got = executable_in_dir(&dir, "foo");
        #[cfg(windows)]
        assert_eq!(got, Some(dir.join("foo.exe")));
        #[cfg(not(windows))]
        assert_eq!(got, None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn write_profile(dir: &Path, name: &str, body: &str) -> PathBuf {
        let path = dir.join(format!("{name}.toml"));
        std::fs::write(&path, body).unwrap();
        path
    }

    #[test]
    fn loads_minimal_profile_and_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_profile(
            dir.path(),
            "demo",
            r#"
name = "demo"
repo = "https://example.com/demo.git"
ref  = "v1"
objdir = "target"
build = "cargo build"
"#,
        );
        let p = BenchProfile::load(&path).unwrap();
        assert_eq!(p.name, "demo");
        assert_eq!(p.git_ref, "v1");
        assert!(p.requires.is_empty());
        assert!(p.files.is_empty());
        assert_eq!(p.dir, dir.path());
    }

    #[test]
    fn name_must_match_file_stem() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_profile(
            dir.path(),
            "demo",
            r#"
name = "wrong"
repo = "r"
ref  = "v1"
objdir = "target"
build = "b"
"#,
        );
        let err = BenchProfile::load(&path).unwrap_err().to_string();
        assert!(
            err.contains("does not match scenario directory"),
            "got: {err}"
        );
    }

    #[test]
    fn interpolation_substitutes_kache_and_objdir() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_profile(
            dir.path(),
            "demo",
            r#"
name = "demo"
repo = "r"
ref  = "v1"
objdir = "obj-bench"
build = "{kache} build --out {objdir}"
[env]
CC = "{kache} cc"
"#,
        );
        let p = BenchProfile::load(&path).unwrap();
        let kache = Path::new("/usr/local/bin/kache");
        assert_eq!(
            p.build_command(kache),
            "/usr/local/bin/kache build --out obj-bench"
        );
        assert_eq!(
            p.build_env(kache),
            vec![("CC".to_string(), "/usr/local/bin/kache cc".to_string())]
        );
    }

    #[test]
    fn file_mode_defaults_to_write_and_validates_payload() {
        let dir = tempfile::tempdir().unwrap();
        // missing both content and content_file
        let bad = write_profile(
            dir.path(),
            "demo",
            r#"
name = "demo"
repo = "r"
ref  = "v1"
objdir = "target"
build = "b"
[[file]]
path = "mozconfig"
"#,
        );
        assert!(
            BenchProfile::load(&bad)
                .unwrap_err()
                .to_string()
                .contains("needs content")
        );

        let ok = write_profile(
            dir.path(),
            "demo2",
            r#"
name = "demo2"
repo = "r"
ref  = "v1"
objdir = "target"
build = "b"
[[file]]
path = "mozconfig"
content = "x"
"#,
        );
        let p = BenchProfile::load(&ok).unwrap();
        assert_eq!(p.files[0].mode, FileMode::Write);
    }

    #[test]
    fn apply_files_write_and_append() {
        let dir = tempfile::tempdir().unwrap();
        let checkout = tempfile::tempdir().unwrap();
        // pre-existing repo-owned file
        std::fs::write(checkout.path().join("config.toml"), "[existing]\n").unwrap();
        let path = write_profile(
            dir.path(),
            "demo",
            r#"
name = "demo"
repo = "r"
ref  = "v1"
objdir = "target"
build = "b"

[[file]]
path = "mozconfig"
content = "wrapper={kache}\n"

[[file]]
path = "config.toml"
mode = "append"
content = "[build]\nrustc-wrapper = \"{kache}\"\n"
"#,
        );
        let p = BenchProfile::load(&path).unwrap();
        p.apply_files(checkout.path(), Path::new("/k")).unwrap();

        assert_eq!(
            std::fs::read_to_string(checkout.path().join("mozconfig")).unwrap(),
            "wrapper=/k\n"
        );
        // append preserved the repo's own content
        let cfg = std::fs::read_to_string(checkout.path().join("config.toml")).unwrap();
        assert!(cfg.starts_with("[existing]\n"));
        assert!(cfg.contains("rustc-wrapper = \"/k\""));
    }

    #[test]
    fn setup_marker_skips_when_present() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("done");
        std::fs::write(&marker, "").unwrap();
        let path = write_profile(
            dir.path(),
            "demo",
            &format!(
                r#"
name = "demo"
repo = "r"
ref  = "v1"
objdir = "target"
build = "b"
setup = ["echo hi"]
setup_marker = "{}"
"#,
                // Forward slashes: backslashes are escape sequences in TOML
                // strings, so a raw Windows path would fail to parse.
                marker.display().to_string().replace('\\', "/")
            ),
        );
        let p = BenchProfile::load(&path).unwrap();
        assert!(p.setup_satisfied(false), "marker exists → skip");
        assert!(!p.setup_satisfied(true), "force → run anyway");
    }

    #[test]
    fn discover_filters_bench_scenarios_by_tags_and_name() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scenarios");
        let selectors =
            Selectors::parse_many(&["suite:bench".to_string(), "name:firefox".to_string()])
                .unwrap();

        let profiles = BenchProfile::discover(&root, &selectors).unwrap();

        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].name, "bench-firefox");
    }

    fn repo_profile(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("../../scenarios/bench-{name}/scenario.toml"))
    }

    /// The shipped Firefox scenario loads and renders the mozconfig the
    /// old hardcoded `write_mozconfig` produced (parity check for the
    /// migration off the Firefox-specific code path).
    #[test]
    fn shipped_firefox_profile_renders_mozconfig() {
        let p = BenchProfile::load(&repo_profile("firefox")).expect("firefox.toml loads");
        assert_eq!(p.name, "bench-firefox");
        assert_eq!(p.objdir, "obj-kache-bench");
        assert_eq!(p.setup_marker.as_deref(), Some("~/.mozbuild"));
        assert_eq!(p.build_command(Path::new("/k")), "./mach build");

        // The patch-mode entries target a real Firefox checkout (their diffs
        // don't apply to an empty tempdir); the bench run exercises those. Here
        // we only render the content files — i.e. the mozconfig parity check.
        let mut content_only = p.clone();
        content_only.files.retain(|f| f.mode != FileMode::Patch);
        let checkout = tempfile::tempdir().unwrap();
        content_only
            .apply_files(checkout.path(), Path::new("/k"))
            .unwrap();
        let moz = std::fs::read_to_string(checkout.path().join("mozconfig")).unwrap();
        assert!(moz.starts_with("# Generated by kache-scenario"));
        assert!(moz.contains("ac_add_options --with-compiler-wrapper=/k"));
        assert!(moz.contains("mk_add_options \"export RUSTC_WRAPPER=/k\""));
        assert!(moz.contains("mk_add_options \"export CARGO_INCREMENTAL=0\""));
        assert!(moz.contains("mk_add_options MOZ_OBJDIR=@TOPSRCDIR@/obj-kache-bench"));
        assert!(
            !moz.contains("{kache}") && !moz.contains("{objdir}"),
            "placeholders must be interpolated"
        );
        // Cross-clone convergence knobs the scenario adds on top of the
        // base mozconfig.
        assert!(moz.contains("mk_add_options \"export KACHE_CACHE_EXECUTABLES=1\""));
        assert!(moz.contains("mk_add_options \"export KACHE_BASE_DIR=@TOPSRCDIR@\""));
        assert!(moz.contains("mk_add_options \"export MOZ_BUILD_DATE=20260101000000\""));
        assert!(moz.contains("export KACHE_PATH_ONLY_ENV_VARS=BUILDCONFIG_RS,"));
        assert!(moz.ends_with('\n'), "mozconfig is newline-terminated");
    }

    /// The shipped Substrate scenario wires kache via `RUSTC_WRAPPER` only
    /// (no file injection, no CC/CXX — native C deps stay outside kache),
    /// installs the wasm targets + rust-src in setup, and builds the
    /// polkadot node.
    #[test]
    fn shipped_substrate_profile_is_rustc_wrapper_only() {
        let p = BenchProfile::load(&repo_profile("substrate")).expect("substrate.toml loads");
        assert_eq!(p.name, "bench-substrate");
        assert_eq!(p.objdir, "target");
        assert!(p.files.is_empty(), "substrate uses RUSTC_WRAPPER only");
        assert!(
            p.env.is_empty(),
            "no extra env — CARGO_INCREMENTAL is an engine baseline, not a profile var"
        );
        assert!(p.setup.iter().any(|s| s.contains("wasm32-unknown-unknown")));
        assert!(p.setup.iter().any(|s| s.contains("rust-src")));
        assert!(
            p.build_command(Path::new("/k"))
                .contains("cargo build --release -p polkadot")
        );
    }
}
