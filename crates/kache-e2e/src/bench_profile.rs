//! Bench profiles: the per-project description `kache-bench` loads.
//!
//! `kache-bench` owns the *measurement* lifecycle (clone → cold build →
//! warm build → report → verdict); a **profile** owns everything
//! *project-specific*: which repo to clone, how to wire kache into the
//! build, and how to build. This is the same split the e2e harness uses
//! with [`crate::fixture`] — the harness owns the lifecycle, the TOML
//! owns *what this project is*.
//!
//! See `bench-profiles/README.md` for the authored reference.

use anyhow::{Context, Result, bail};
use indexmap::IndexMap;
use serde::Deserialize;
use std::path::{Path, PathBuf};

/// A project `kache-bench` can benchmark, parsed from
/// `bench-profiles/<name>.toml`.
#[derive(Debug, Clone, Deserialize)]
pub struct BenchProfile {
    /// Profile id; must match the file stem (`firefox.toml` → `firefox`).
    pub name: String,

    /// Git URL to clone.
    pub repo: String,

    /// Tag / branch / commit to pin for reproducibility. Overridable on
    /// the CLI with `--ref`.
    #[serde(rename = "ref")]
    pub git_ref: String,

    /// Build output directory, wiped before each phase so every build is
    /// genuinely from-scratch (`obj-bench` for Firefox, `target` for a
    /// Cargo project).
    pub objdir: String,

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

    /// Absolute path to the profile file's directory (set at load time,
    /// used to resolve a relative `patch_file`). Not in the TOML.
    #[serde(skip)]
    pub dir: PathBuf,
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
    /// Read the payload from a file relative to the profile dir instead of
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
    /// Resolve a `--profile` argument to a loaded profile. Accepts either
    /// a path to a `.toml` file or a bare name resolved against
    /// `<profiles_dir>/<name>.toml`.
    pub fn resolve(arg: &str, profiles_dir: &Path) -> Result<Self> {
        let as_path = Path::new(arg);
        let path = if as_path.is_file() {
            as_path.to_path_buf()
        } else {
            profiles_dir.join(format!("{arg}.toml"))
        };
        if !path.is_file() {
            bail!(
                "no bench profile `{arg}` — looked for `{}` and `{}`",
                as_path.display(),
                profiles_dir.join(format!("{arg}.toml")).display()
            );
        }
        Self::load(&path)
    }

    /// Load and validate a profile from a `.toml` path.
    pub fn load(path: &Path) -> Result<Self> {
        let raw =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let mut profile: Self =
            toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?;

        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default();
        if profile.name != stem {
            bail!(
                "profile `name = \"{}\"` does not match file stem `{}`",
                profile.name,
                stem
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

    /// The build env as interpolated `(key, value)` pairs (profile `env`
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

/// Minimal `which`: first `PATH` entry containing an executable named
/// `tool`. Enough for the `requires` skip check.
fn which_on_path(tool: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(tool);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(err.contains("does not match file stem"), "got: {err}");
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
                marker.display()
            ),
        );
        let p = BenchProfile::load(&path).unwrap();
        assert!(p.setup_satisfied(false), "marker exists → skip");
        assert!(!p.setup_satisfied(true), "force → run anyway");
    }

    fn repo_profile(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../bench-profiles")
            .join(format!("{name}.toml"))
    }

    /// The shipped `firefox.toml` loads and renders the mozconfig the
    /// old hardcoded `write_mozconfig` produced (parity check for the
    /// migration off the Firefox-specific code path).
    #[test]
    fn shipped_firefox_profile_renders_mozconfig() {
        let p = BenchProfile::load(&repo_profile("firefox")).expect("firefox.toml loads");
        assert_eq!(p.name, "firefox");
        assert_eq!(p.objdir, "obj-kache-bench");
        assert_eq!(p.setup_marker.as_deref(), Some("~/.mozbuild"));
        assert_eq!(p.build_command(Path::new("/k")), "./mach build");

        let checkout = tempfile::tempdir().unwrap();
        p.apply_files(checkout.path(), Path::new("/k")).unwrap();
        let moz = std::fs::read_to_string(checkout.path().join("mozconfig")).unwrap();
        assert!(moz.starts_with("# Generated by kache-bench"));
        assert!(moz.contains("ac_add_options --with-compiler-wrapper=/k"));
        assert!(moz.contains("mk_add_options \"export RUSTC_WRAPPER=/k\""));
        assert!(moz.contains("mk_add_options \"export CARGO_INCREMENTAL=0\""));
        assert!(moz.contains("mk_add_options MOZ_OBJDIR=@TOPSRCDIR@/obj-kache-bench"));
        assert!(
            !moz.contains("{kache}") && !moz.contains("{objdir}"),
            "placeholders must be interpolated"
        );
        assert!(moz.ends_with("obj-kache-bench\n"));
    }

    /// The shipped `substrate.toml` wires kache via `RUSTC_WRAPPER` only
    /// (no file injection, no CC/CXX — native C deps stay outside kache),
    /// installs the wasm targets + rust-src in setup, and builds the
    /// polkadot node.
    #[test]
    fn shipped_substrate_profile_is_rustc_wrapper_only() {
        let p = BenchProfile::load(&repo_profile("substrate")).expect("substrate.toml loads");
        assert_eq!(p.name, "substrate");
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
