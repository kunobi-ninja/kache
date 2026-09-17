//! Build-script execution cache.
//!
//! Cargo runs a package's build script whenever its target directory has no
//! fresh result for it, which on a CI runner is every build. Compiling the
//! script is already cached like any other unit, but *running* it is not, and
//! a `cc`-driven script pays the `cc` crate's polling overhead per object
//! whether or not the compile behind it was a cache hit. This module caches
//! the run itself.
//!
//! The rustc wrapper, after producing a `build_script_*` binary, moves it aside
//! and installs a launcher at Cargo's expected path. Cargo runs the launcher;
//! it execs kache in build-script mode with the preserved binary beside it.
//! The launcher is a native program (`launcher/build_script.rs`, compiled by
//! kache's build script), not a shell script: a shell drops environment
//! variables whose names are not shell identifiers, and Cargo's `DEP_*`
//! metadata variables can be spelled that way.
//! kache then restores `OUT_DIR` and the script's stdout/stderr from the cache
//! when the run's inputs match a recorded run, and otherwise runs the real
//! script and records the result.
//!
//! The inputs are the ones Cargo itself uses to decide whether a script must
//! rerun: the script binary, the environment Cargo provides, and the
//! `rerun-if-changed` / `rerun-if-env-changed` declarations the script made on
//! its previous run. A script that declares nothing depends on its whole
//! package directory, exactly as in Cargo. Serving a recorded run under those
//! rules is no less correct than Cargo skipping the rerun, which is what it
//! does with a warm target directory. Entries stay in the local store; they
//! are never published to a remote.

use crate::args::RustcArgs;
use crate::compiler_store::StoreHashExt as _;
use crate::config::Config;
use crate::events::EventResult;
use crate::store::{EntryMeta, Store, StorePutResult, link};
use anyhow::{Context, Result};
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Set by the launcher to the path Cargo invoked, which is where the preserved
/// binary lives beside.
pub const SHIM_PATH_ENV: &str = "KACHE_BUILD_SCRIPT_PATH";
/// `KACHE_BUILD_SCRIPT_CACHE=0` leaves build scripts uncached and installs no
/// launcher.
const ENABLE_ENV: &str = "KACHE_BUILD_SCRIPT_CACHE";
const REAL_SUFFIX: &str = ".kache-real";
const ACTION_SUFFIX: &str = ".kache-real.action";
/// Beside the launcher: the preserved script's file name and the pinned
/// kache relative to the profile directory, each NUL-terminated. The launcher
/// reads it instead of listing the directory.
#[cfg_attr(not(unix), allow(dead_code))]
const LAUNCH_RECORD: &str = ".kache-launch";
#[cfg_attr(not(unix), allow(dead_code))]
const SHIM_DIR: &str = ".kache-build-script-shims";
/// The launcher binary for this target, built from `launcher/build_script.rs`.
#[cfg(unix)]
const LAUNCHER: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/build-script-launcher"));
const PREDICTION_SCHEMA: u32 = 1;
const PREDICTION_PREFIX: &str = "build-script:";
const MANIFEST_NAME: &str = "kache-build-script.json";
const OUT_PREFIX: &str = "out/";
pub(crate) const CRATE_NAME: &str = "build_script_run";
/// Above this an `OUT_DIR` is left uncached: a directory this size is a build
/// tree, not a build-script product, and restoring it would not be cheaper
/// than the run.
const MAX_OUT_DIR_BYTES: u64 = 1 << 30;
const MAX_OUT_DIR_FILES: usize = 50_000;
const MAX_INPUT_FILES: usize = 100_000;

pub fn enabled() -> bool {
    !std::env::var_os(ENABLE_ENV).is_some_and(|value| value == "0" || value == "false")
}

pub fn is_shim_invocation() -> bool {
    std::env::var_os(SHIM_PATH_ENV).is_some()
}

/// The binary a build-script compilation produced, when `args` compiled one.
///
/// Cargo compiles `build.rs` as `--crate-name build_script_build --crate-type
/// bin` into `<profile>/build/<pkg>-<hash>/build_script_build-<hash>`, then
/// hardlinks that to `build-script-build` and runs it.
pub fn compiled_build_script(args: &RustcArgs) -> Option<PathBuf> {
    let crate_name = args.crate_name.as_deref()?;
    if !crate_name.starts_with("build_script_") || args.crate_types != ["bin"] {
        return None;
    }
    let out_dir = args.out_dir.as_deref()?;
    if out_dir.parent()?.file_name()? != "build" {
        return None;
    }
    let stem = crate::args::format_crate_output_stem(
        crate_name,
        args.extra_filename.as_deref().unwrap_or(""),
    );
    let path = out_dir.join(stem);
    path.is_file().then_some(path)
}

/// Replace a freshly produced build-script binary with the launcher.
///
/// Best-effort: any failure leaves the real binary where Cargo expects it and
/// the script simply runs uncached.
pub fn install_shim(args: &RustcArgs) {
    if !enabled() || !cfg!(unix) {
        return;
    }
    let Some(executable) = compiled_build_script(args) else {
        return;
    };
    if let Err(error) = install(&executable) {
        tracing::debug!(
            "build-script launcher not installed for {}: {error:#}",
            executable.display()
        );
    }
}

#[cfg(unix)]
fn install(executable: &Path) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::PermissionsExt;

    let metadata = std::fs::metadata(executable)?;
    let modified = metadata.modified()?;
    let real = with_suffix(executable, REAL_SUFFIX);
    let action = with_suffix(executable, ACTION_SUFFIX);
    let record = executable
        .parent()
        .context("build script has no parent directory")?
        .join(LAUNCH_RECORD);
    let binary_hash = kache_store::file_hash::hash_file(executable)?;

    // <profile>/build/<pkg>-<hash>/<exe>: the profile directory is what the
    // launcher can reach relatively when the target directory moves.
    let profile = executable
        .ancestors()
        .nth(3)
        .context("build script is not under a Cargo profile directory")?;
    let kache = std::env::current_exe().context("locating the kache executable")?;
    let pinned = pin_kache(&kache, profile)?;
    let relative = pinned
        .strip_prefix(profile)
        .context("pinned kache is outside the profile directory")?;

    let _ = std::fs::remove_file(&real);
    std::fs::rename(executable, &real).context("preserving the build script")?;
    let installed = (|| -> Result<()> {
        std::fs::write(&action, &binary_hash)?;
        let real_name = real.file_name().context("preserved script has no name")?;
        std::fs::write(
            &record,
            [
                real_name.as_bytes(),
                b"\0",
                relative.as_os_str().as_bytes(),
                b"\0",
            ]
            .concat(),
        )?;
        let temporary = with_suffix(executable, ".kache-launcher");
        std::fs::write(&temporary, LAUNCHER)?;
        std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o755))?;
        // Cargo compares this path's mtime with its inputs when deciding
        // whether the compilation is fresh; keep the binary's.
        std::fs::OpenOptions::new()
            .write(true)
            .open(&temporary)?
            .set_times(std::fs::FileTimes::new().set_modified(modified))?;
        std::fs::rename(&temporary, executable)?;
        Ok(())
    })();
    if let Err(error) = installed {
        let _ = std::fs::remove_file(&action);
        let _ = std::fs::remove_file(&record);
        let _ = std::fs::rename(&real, executable);
        return Err(error);
    }
    Ok(())
}

#[cfg(not(unix))]
fn install(_executable: &Path) -> Result<()> {
    anyhow::bail!("build-script launchers are only installed on Unix")
}

/// One copy of the running kache per profile directory, addressed by the
/// binary's identity so an upgrade pins a new copy and old launchers keep
/// working until the target directory is cleaned.
#[cfg(unix)]
fn pin_kache(kache: &Path, profile: &Path) -> Result<PathBuf> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = std::fs::metadata(kache)?;
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0, |time| time.as_nanos());
    let mut identity = blake3::Hasher::new();
    identity.update(kache.as_os_str().as_encoded_bytes());
    identity.update(&metadata.len().to_le_bytes());
    identity.update(&modified.to_le_bytes());
    let directory = profile
        .join(SHIM_DIR)
        .join(&identity.finalize().to_hex()[..32]);
    let pinned = directory.join("kache");
    if pinned.is_file() {
        return Ok(pinned);
    }
    std::fs::create_dir_all(&directory)?;
    if std::fs::hard_link(kache, &pinned).is_ok() {
        return Ok(pinned);
    }
    let temporary = tempfile::NamedTempFile::new_in(&directory)?;
    std::fs::copy(kache, temporary.path())?;
    std::fs::set_permissions(temporary.path(), std::fs::Permissions::from_mode(0o755))?;
    match temporary.persist_noclobber(&pinned) {
        Ok(_) => {}
        // A concurrent installer won the race with identical bytes.
        Err(error) if pinned.is_file() => drop(error),
        Err(error) => return Err(error.error.into()),
    }
    Ok(pinned)
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

/// The preserved binary for the path Cargo invoked. Cargo runs the un-hashed
/// `build-script-build` hardlink; the wrapper preserved the hashed original.
fn find_real(invoked: &Path) -> Option<PathBuf> {
    let direct = with_suffix(invoked, REAL_SUFFIX);
    if direct.is_file() {
        return Some(direct);
    }
    let parent = invoked.parent()?;
    let mut candidates: Vec<PathBuf> = std::fs::read_dir(parent)
        .ok()?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_file()
                && path
                    .file_name()
                    .and_then(OsStr::to_str)
                    .is_some_and(|name| name.ends_with(REAL_SUFFIX))
        })
        .collect();
    candidates.sort();
    // Exactly one preserved binary lives in a build-script directory; more
    // than one means the layout is not the one this module understands.
    (candidates.len() == 1).then(|| candidates.remove(0))
}

/// Entry point for the launcher: restore or run, then exit like the script.
pub fn run_shim() -> i32 {
    let Some(invoked) = std::env::var_os(SHIM_PATH_ENV).map(PathBuf::from) else {
        eprintln!("kache: build-script launcher invoked without {SHIM_PATH_ENV}");
        return 1;
    };
    let Some(real) = find_real(&invoked) else {
        eprintln!(
            "kache: the preserved build script beside {} is missing; run `cargo clean`",
            invoked.display()
        );
        return 1;
    };
    let argv: Vec<std::ffi::OsString> = std::env::args_os().skip(1).collect();
    let cached = if enabled() {
        match run_cached(&real, &argv) {
            Ok(code) => Some(code),
            Err(error) => {
                tracing::debug!("build-script cache bypassed: {error:#}");
                None
            }
        }
    } else {
        None
    };
    cached.unwrap_or_else(|| run_real(&real, &argv))
}

fn real_command(real: &Path, argv: &[std::ffi::OsString]) -> Command {
    let mut command = Command::new(real);
    command.args(argv);
    command.env_remove(SHIM_PATH_ENV);
    command
}

fn run_real(real: &Path, argv: &[std::ffi::OsString]) -> i32 {
    match real_command(real, argv).status() {
        Ok(status) => status.code().unwrap_or(1),
        Err(error) => {
            eprintln!("kache: failed to run {}: {error}", real.display());
            1
        }
    }
}

struct Run {
    config: Config,
    store: Store,
    binary_hash: String,
    environment: Environment,
    start: std::time::Instant,
}

fn run_cached(real: &Path, argv: &[std::ffi::OsString]) -> Result<i32> {
    let start = std::time::Instant::now();
    let config = Config::load()?;
    let _ = TREE_MEMO_DIR.set(config.cache_dir.clone());
    if config.disabled {
        anyhow::bail!("kache is disabled");
    }
    let environment = Environment::capture()?;
    let store = Store::open(&config)?;
    let binary_hash = stored_binary_hash(real)?;
    let run = Run {
        config,
        store,
        binary_hash,
        environment,
        start,
    };

    let prediction = run.prediction()?;
    if let Some(prediction) = &prediction {
        let key_start = std::time::Instant::now();
        let key = run.action_key(prediction)?;
        let key_ms = key_start.elapsed().as_millis() as u64;
        let lookup_start = std::time::Instant::now();
        let meta = run.store.get(&key)?;
        let lookup_ms = lookup_start.elapsed().as_millis() as u64;
        if let Some(meta) = meta {
            let restore_start = std::time::Instant::now();
            match run.restore(&meta) {
                Ok(size) => {
                    let restore_ms = restore_start.elapsed().as_millis() as u64;
                    run.log(
                        EventResult::LocalHit,
                        &key,
                        key_ms,
                        lookup_ms,
                        restore_ms,
                        0,
                        StorePutResult::default(),
                        size,
                    );
                    return Ok(0);
                }
                Err(error) => {
                    tracing::debug!("build-script result not restored, running it: {error:#}");
                }
            }
        }
    }

    // Only a run that starts from an empty OUT_DIR produces a state worth
    // recording: a rerun over leftovers would snapshot the leftovers too.
    let out_dir_was_empty = directory_is_empty(&run.environment.out_dir);
    let started = std::time::SystemTime::now();
    let compile_start = std::time::Instant::now();
    let output = real_command(real, argv)
        .output()
        .with_context(|| format!("running {}", real.display()))?;
    let compile_ms = compile_start.elapsed().as_millis() as u64;
    replay(&output.stdout, &output.stderr);
    if !output.status.success() {
        return Ok(output.status.code().unwrap_or(1));
    }
    // The script has run and Cargo has its output; nothing below may change
    // the exit status.
    if !out_dir_was_empty {
        tracing::debug!("build script ran over an existing OUT_DIR; not recorded");
        return Ok(0);
    }
    if let Err(error) = run.record(&output.stdout, &output.stderr, compile_ms, started) {
        tracing::debug!("build-script result not recorded: {error:#}");
    }
    Ok(0)
}

fn stored_binary_hash(real: &Path) -> Result<String> {
    let action = with_suffix(
        Path::new(
            real.as_os_str()
                .to_str()
                .and_then(|path| path.strip_suffix(REAL_SUFFIX))
                .context("preserved build script has an unexpected name")?,
        ),
        ACTION_SUFFIX,
    );
    if let Ok(recorded) = std::fs::read_to_string(&action)
        && kache_format::is_blob_hash(recorded.trim())
    {
        return Ok(recorded.trim().to_string());
    }
    kache_store::file_hash::hash_file(real)
}

fn replay(stdout: &[u8], stderr: &[u8]) {
    use std::io::Write;
    let _ = std::io::stdout().write_all(stdout);
    let _ = std::io::stdout().flush();
    let _ = std::io::stderr().write_all(stderr);
    let _ = std::io::stderr().flush();
}

/// What a previous run declared it depends on, so this run can decide from
/// the tree alone whether the recorded result still applies.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Prediction {
    version: u32,
    /// Declared `rerun-if-changed` paths, spelled with the roots mapped.
    inputs: Vec<String>,
    /// Declared `rerun-if-env-changed` names.
    env: Vec<String>,
    /// No declarations: the package directory is the input, as in Cargo.
    default_package: bool,
    /// Nothing under `OUT_DIR` spelled a machine-local root, so the result
    /// can be restored under another `OUT_DIR`.
    portable_out_dir: bool,
}

/// Roots that appear in paths and outputs and differ between checkouts.
#[derive(Debug, Clone)]
struct Environment {
    out_dir: PathBuf,
    manifest_dir: PathBuf,
    /// Longest first, so nested roots map before their parents.
    mappings: Vec<(PathBuf, &'static str)>,
}

impl Environment {
    fn capture() -> Result<Self> {
        let out_dir = PathBuf::from(std::env::var_os("OUT_DIR").context("no OUT_DIR")?);
        let manifest_dir =
            PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").context("no CARGO_MANIFEST_DIR")?);
        anyhow::ensure!(
            out_dir.is_absolute() && manifest_dir.is_absolute(),
            "build-script roots must be absolute"
        );
        let mut mappings = vec![
            (out_dir.clone(), "${KACHE_OUT_DIR}"),
            (manifest_dir.clone(), "${KACHE_MANIFEST_DIR}"),
        ];
        if let Some(target) = target_dir(&out_dir) {
            mappings.push((target, "${KACHE_TARGET_DIR}"));
        }
        let cargo_home = std::env::var_os("CARGO_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cargo")));
        if let Some(cargo_home) = cargo_home {
            mappings.push((cargo_home, "${KACHE_CARGO_HOME}"));
        }
        // A checkout reached through a symlink is spelled both ways by tools
        // that canonicalize; both spellings map to the same placeholder.
        let canonical: Vec<(PathBuf, &'static str)> = mappings
            .iter()
            .filter_map(|(root, placeholder)| {
                let canonical = std::fs::canonicalize(root).ok()?;
                (canonical != *root).then_some((canonical, *placeholder))
            })
            .collect();
        mappings.extend(canonical);
        mappings.sort_by_key(|(root, _)| std::cmp::Reverse(root.as_os_str().len()));
        Ok(Self {
            out_dir,
            manifest_dir,
            mappings,
        })
    }

    fn normalize(&self, text: &[u8]) -> Vec<u8> {
        let mut text = text.to_vec();
        for (root, placeholder) in &self.mappings {
            text = replace_all(
                &text,
                root.as_os_str().as_encoded_bytes(),
                placeholder.as_bytes(),
            );
        }
        text
    }

    fn denormalize(&self, text: &[u8]) -> Vec<u8> {
        let mut text = text.to_vec();
        for (root, placeholder) in &self.mappings {
            text = replace_all(
                &text,
                placeholder.as_bytes(),
                root.as_os_str().as_encoded_bytes(),
            );
        }
        text
    }

    fn normalize_str(&self, text: &str) -> String {
        String::from_utf8_lossy(&self.normalize(text.as_bytes())).into_owned()
    }

    fn denormalize_str(&self, text: &str) -> String {
        String::from_utf8_lossy(&self.denormalize(text.as_bytes())).into_owned()
    }

    /// Whether any file under `OUT_DIR` spells one of the roots.
    fn out_dir_is_portable(&self, files: &[(PathBuf, String)]) -> Result<bool> {
        let needles: Vec<&[u8]> = self
            .mappings
            .iter()
            .map(|(root, _)| root.as_os_str().as_encoded_bytes())
            .collect();
        for (path, _) in files {
            let contents = std::fs::read(path)?;
            if needles
                .iter()
                .any(|needle| find_bytes(&contents, needle).is_some())
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// `<target>/[<triple>/]<profile>/build/<pkg>-<hash>/out` back to `<target>`.
pub(crate) fn target_dir(out_dir: &Path) -> Option<PathBuf> {
    let unit = out_dir.parent()?;
    let build = unit.parent()?;
    if build.file_name()? != "build" {
        return None;
    }
    let profile = build.parent()?;
    let mut target = profile.parent()?;
    if let Some(triple) = std::env::var_os("TARGET")
        && target.file_name() == Some(triple.as_os_str())
        && let Some(parent) = target.parent()
    {
        target = parent;
    }
    Some(target.to_path_buf())
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn replace_all(haystack: &[u8], needle: &[u8], replacement: &[u8]) -> Vec<u8> {
    if needle.is_empty() {
        return haystack.to_vec();
    }
    let mut out = Vec::with_capacity(haystack.len());
    let mut rest = haystack;
    while let Some(index) = find_bytes(rest, needle) {
        let (before, found) = rest.split_at(index);
        let (_, after) = found.split_at(needle.len());
        out.extend_from_slice(before);
        out.extend_from_slice(replacement);
        rest = after;
    }
    out.extend_from_slice(rest);
    out
}

/// The `rerun-if-*` declarations in a script's stdout. `None` when the script
/// asked to run every time, which no finite key can honour.
fn parse_declarations(
    stdout: &str,
    environment: &Environment,
) -> Option<(Vec<String>, Vec<String>, bool)> {
    let mut inputs = std::collections::BTreeSet::new();
    let mut env = std::collections::BTreeSet::new();
    for line in stdout.lines() {
        let Some(directive) = line
            .strip_prefix("cargo::")
            .or_else(|| line.strip_prefix("cargo:"))
        else {
            continue;
        };
        if let Some(path) = directive.strip_prefix("rerun-if-changed=") {
            // Cargo treats an empty path as "always rerun".
            if path.is_empty() {
                return None;
            }
            let resolved = environment.manifest_dir.join(path);
            // An input under OUT_DIR is one this run produced; scripts use
            // that shape to force a rerun every time.
            if resolved.starts_with(&environment.out_dir) {
                return None;
            }
            inputs.insert(environment.normalize_str(&resolved.to_string_lossy()));
        } else if let Some(name) = directive.strip_prefix("rerun-if-env-changed=")
            && !name.is_empty()
        {
            env.insert(name.to_string());
        }
    }
    let default_package = inputs.is_empty() && env.is_empty();
    if default_package {
        inputs.insert("${KACHE_MANIFEST_DIR}".to_string());
    }
    Some((
        inputs.into_iter().collect(),
        env.into_iter().collect(),
        default_package,
    ))
}

/// Cargo-provided environment that shapes a run and is not otherwise keyed.
/// `NUM_JOBS` and the jobserver variables describe the machine, not the run.
fn cargo_environment(environment: &Environment) -> BTreeMap<String, Option<String>> {
    const FIXED: &[&str] = &[
        "CARGO_ENCODED_RUSTFLAGS",
        "CARGO_MANIFEST_DIR",
        "CARGO_MANIFEST_LINKS",
        "CARGO_MANIFEST_PATH",
        "DEBUG",
        "HOST",
        "OPT_LEVEL",
        "PKG_CONFIG_ALLOW_CROSS",
        "PKG_CONFIG_LIBDIR",
        "PKG_CONFIG_PATH",
        "PKG_CONFIG_SYSROOT_DIR",
        "PROFILE",
        "RUSTC",
        "RUSTC_LINKER",
        "RUSTDOC",
        "TARGET",
    ];
    let mut names: std::collections::BTreeSet<String> =
        FIXED.iter().map(|name| (*name).to_string()).collect();
    names.extend(std::env::vars_os().filter_map(|(name, _)| {
        let name = name.into_string().ok()?;
        ["CARGO_CFG_", "CARGO_FEATURE_", "CARGO_PKG_", "DEP_"]
            .iter()
            .any(|prefix| name.starts_with(prefix))
            .then_some(name)
    }));
    names
        .into_iter()
        .map(|name| {
            let value = std::env::var(&name)
                .ok()
                .map(|value| environment.normalize_str(&value));
            (name, value)
        })
        .collect()
}

fn fold(hasher: &mut blake3::Hasher, label: &str, value: &[u8]) {
    hasher.update(&(label.len() as u64).to_le_bytes());
    hasher.update(label.as_bytes());
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

impl Run {
    fn identity(&self) -> String {
        let mut hasher = blake3::Hasher::new();
        fold(&mut hasher, "kind", b"kache-build-script-v1");
        fold(&mut hasher, "binary", self.binary_hash.as_bytes());
        format!("{PREDICTION_PREFIX}{}", hasher.finalize().to_hex())
    }

    fn prediction(&self) -> Result<Option<Prediction>> {
        let Some((schema, json)) = self
            .store
            .file_hash_cache()
            .get_input_prediction(&self.identity())?
        else {
            return Ok(None);
        };
        if schema != PREDICTION_SCHEMA {
            return Ok(None);
        }
        let prediction: Prediction = serde_json::from_str(&json)?;
        Ok((prediction.version == PREDICTION_SCHEMA).then_some(prediction))
    }

    fn record_prediction(&self, prediction: &Prediction) -> Result<()> {
        let json = serde_json::to_string(prediction)?;
        self.store.file_hash_cache().put_input_prediction(
            &self.identity(),
            PREDICTION_SCHEMA,
            Some(CRATE_NAME),
            &json,
        )?;
        Ok(())
    }

    /// The key a recorded run is stored under: everything Cargo would compare
    /// before deciding the script need not rerun, plus the host it ran on.
    fn action_key(&self, prediction: &Prediction) -> Result<String> {
        let mut hasher = blake3::Hasher::new();
        fold(&mut hasher, "kind", b"kache-build-script-action-v1");
        fold(
            &mut hasher,
            "key_version",
            kache_format::CACHE_KEY_VERSION.to_string().as_bytes(),
        );
        fold(&mut hasher, "binary", self.binary_hash.as_bytes());
        fold(&mut hasher, "os", std::env::consts::OS.as_bytes());
        fold(&mut hasher, "arch", std::env::consts::ARCH.as_bytes());
        let file_hasher = self.store.file_hasher();
        let mut budget = MAX_INPUT_FILES;
        for (name, value) in cargo_environment(&self.environment) {
            fold(&mut hasher, "cargo_env_name", name.as_bytes());
            match value {
                Some(value) => fold(&mut hasher, "cargo_env_value", value.as_bytes()),
                None => fold(&mut hasher, "cargo_env_absent", b""),
            }
            // A `links` dependency hands its outputs down as `DEP_*` paths
            // whose spelling is stable across content changes; what is at the
            // path is the real input, as Cargo's rerun of dependents implies.
            if name.starts_with("DEP_")
                && let Some(raw) = std::env::var_os(&name)
            {
                let path = PathBuf::from(raw);
                if path.is_absolute() && path.exists() {
                    let state = input_state(&path, &[], &file_hasher, &mut budget, 0)?;
                    fold(&mut hasher, "cargo_env_path_state", state.as_bytes());
                }
            }
        }
        for name in &prediction.env {
            fold(&mut hasher, "env_name", name.as_bytes());
            match std::env::var_os(name) {
                Some(value) => fold(
                    &mut hasher,
                    "env_value",
                    &self.environment.normalize(value.as_encoded_bytes()),
                ),
                None => fold(&mut hasher, "env_absent", b""),
            }
        }
        for declared in &prediction.inputs {
            let path = PathBuf::from(self.environment.denormalize_str(declared));
            fold(&mut hasher, "input", declared.as_bytes());
            let excluded = if prediction.default_package {
                package_exclusions(&path, &self.environment)
            } else {
                Vec::new()
            };
            let state = input_state(&path, &excluded, &file_hasher, &mut budget, 0)?;
            fold(&mut hasher, "state", state.as_bytes());
        }
        if !prediction.portable_out_dir {
            fold(
                &mut hasher,
                "out_dir",
                self.environment.out_dir.as_os_str().as_encoded_bytes(),
            );
        }
        if let Some(salt) = &self.config.key_salt {
            fold(&mut hasher, "salt", salt.as_bytes());
        }
        Ok(hasher.finalize().to_hex().to_string())
    }

    fn restore(&self, meta: &EntryMeta) -> Result<u64> {
        let manifest = meta
            .files
            .iter()
            .find(|file| file.name == MANIFEST_NAME)
            .context("recorded run has no manifest")?;
        let manifest: Manifest =
            serde_json::from_slice(&std::fs::read(self.store.blob_path(&manifest.hash))?)?;
        anyhow::ensure!(manifest.version == 1, "unsupported build-script manifest");
        let out_dir = &self.environment.out_dir;
        // The recorded run started from an empty OUT_DIR (see `record`), so
        // the restored state is exact only if this one does too.
        clear_directory(out_dir)?;
        for directory in &manifest.directories {
            std::fs::create_dir_all(out_dir.join(checked_relative(directory)?))?;
        }
        let mut prepared = Vec::new();
        let mut size = 0;
        for cached in &meta.files {
            let Some(relative) = cached.name.strip_prefix(OUT_PREFIX) else {
                continue;
            };
            let target = out_dir.join(checked_relative(relative)?);
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let blob = self.store.blob_path(&cached.hash);
            anyhow::ensure!(
                blob.is_file(),
                "blob for {} was evicted before restore",
                cached.name
            );
            prepared.push((
                link::prepare_writable_target_from_file(&blob, &target)?,
                cached.executable,
            ));
            size += cached.size;
        }
        for (artifact, executable) in prepared {
            let target = artifact.target().to_path_buf();
            artifact.publish_replacing()?;
            set_executable(&target, executable)?;
        }
        for empty in &manifest.empty_files {
            let target = out_dir.join(checked_relative(&empty.name)?);
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&target, b"")?;
            set_executable(&target, empty.executable)?;
        }
        replay(
            &self.environment.denormalize(manifest.stdout.as_bytes()),
            &self.environment.denormalize(manifest.stderr.as_bytes()),
        );
        Ok(size)
    }

    fn record(
        &self,
        stdout: &[u8],
        stderr: &[u8],
        compile_ms: u64,
        started: std::time::SystemTime,
    ) -> Result<()> {
        let stdout_text =
            std::str::from_utf8(stdout).context("build-script stdout is not UTF-8")?;
        let Some((inputs, env, default_package)) =
            parse_declarations(stdout_text, &self.environment)
        else {
            tracing::debug!("build script asks to rerun every time; not recorded");
            return Ok(());
        };
        // An input written while the script ran is either an edit racing the
        // build or the script writing into its own inputs; the key computed
        // now would describe neither the bytes the run read nor a run that
        // can be replayed.
        for declared in &inputs {
            let path = PathBuf::from(self.environment.denormalize_str(declared));
            let excluded = if default_package {
                package_exclusions(&path, &self.environment)
            } else {
                Vec::new()
            };
            if modified_since(&path, &excluded, started)? {
                anyhow::bail!("a declared input changed while the script ran");
            }
        }
        let OutDirContents {
            files,
            directories: manifest_dirs,
            empty_files,
        } = collect_out_dir(&self.environment.out_dir)?;
        let portable_out_dir = self.environment.out_dir_is_portable(&files)?;
        let prediction = Prediction {
            version: PREDICTION_SCHEMA,
            inputs,
            env,
            default_package,
            portable_out_dir,
        };
        let key_start = std::time::Instant::now();
        let key = self.action_key(&prediction)?;
        let key_ms = key_start.elapsed().as_millis() as u64;
        let manifest = Manifest {
            version: 1,
            directories: manifest_dirs,
            empty_files,
            stdout: String::from_utf8_lossy(&self.environment.normalize(stdout)).into_owned(),
            stderr: String::from_utf8_lossy(&self.environment.normalize(stderr)).into_owned(),
        };
        let staging = tempfile::Builder::new()
            .prefix("kache-build-script-")
            .tempdir()?;
        let manifest_path = staging.path().join(MANIFEST_NAME);
        std::fs::write(&manifest_path, serde_json::to_vec(&manifest)?)?;
        let mut output_files = files;
        output_files.push((manifest_path, MANIFEST_NAME.to_string()));
        let size: u64 = output_files
            .iter()
            .filter_map(|(path, _)| std::fs::metadata(path).ok())
            .map(|meta| meta.len())
            .sum();
        let store_start = std::time::Instant::now();
        let put = self.store.put_with_compile_time_independent(
            &key,
            CRATE_NAME,
            &["build-script".to_string()],
            &[],
            &std::env::var("TARGET").unwrap_or_default(),
            &std::env::var("PROFILE").unwrap_or_default(),
            &output_files,
            "",
            "",
            compile_ms,
        )?;
        let store_ms = store_start.elapsed().as_millis() as u64;
        self.record_prediction(&prediction)?;
        self.log(EventResult::Miss, &key, key_ms, 0, 0, store_ms, put, size);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn log(
        &self,
        result: EventResult,
        key: &str,
        key_ms: u64,
        lookup_ms: u64,
        restore_ms: u64,
        store_ms: u64,
        put: StorePutResult,
        size: u64,
    ) {
        crate::wrapper::log_build_script_event(
            &self.config,
            &self.environment.manifest_dir.to_string_lossy(),
            CRATE_NAME,
            result,
            self.start.elapsed().as_millis() as u64,
            size,
            key,
            key_ms,
            lookup_ms,
            restore_ms,
            store_ms,
            put,
        );
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Manifest {
    version: u32,
    /// Every directory under `OUT_DIR`, so empty ones come back too.
    directories: Vec<String>,
    empty_files: Vec<EmptyFile>,
    stdout: String,
    stderr: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct EmptyFile {
    name: String,
    executable: bool,
}

/// Reject a recorded name that would escape `OUT_DIR`.
fn checked_relative(name: &str) -> Result<&Path> {
    let path = Path::new(name);
    anyhow::ensure!(
        !name.is_empty()
            && path.is_relative()
            && path
                .components()
                .all(|component| matches!(component, std::path::Component::Normal(_))),
        "recorded build-script output has an unsafe name: {name}"
    );
    Ok(path)
}

fn set_executable(path: &Path, executable: bool) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = if executable { 0o755 } else { 0o644 };
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
    }
    #[cfg(not(unix))]
    let _ = (path, executable);
    Ok(())
}

/// Every regular file under `OUT_DIR` as `(path, "out/<relative>")`, plus the
/// directory list and the zero-byte files the store will not take as blobs.
struct OutDirContents {
    files: Vec<(PathBuf, String)>,
    directories: Vec<String>,
    empty_files: Vec<EmptyFile>,
}

fn collect_out_dir(out_dir: &Path) -> Result<OutDirContents> {
    let mut files = Vec::new();
    let mut directories = Vec::new();
    let mut empty = Vec::new();
    let mut total = 0u64;
    let mut pending = vec![out_dir.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let mut entries: Vec<_> = std::fs::read_dir(&directory)?.collect::<std::io::Result<_>>()?;
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            let path = entry.path();
            let relative = path
                .strip_prefix(out_dir)?
                .to_str()
                .context("OUT_DIR entry name is not UTF-8")?
                .replace(std::path::MAIN_SEPARATOR, "/");
            let metadata = std::fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() {
                anyhow::bail!("OUT_DIR contains a symlink, which is not recorded");
            } else if metadata.is_dir() {
                directories.push(relative);
                pending.push(path);
            } else if metadata.is_file() {
                if metadata.len() == 0 {
                    empty.push(EmptyFile {
                        name: relative,
                        executable: is_executable(&metadata),
                    });
                    continue;
                }
                total += metadata.len();
                anyhow::ensure!(
                    total <= MAX_OUT_DIR_BYTES && files.len() < MAX_OUT_DIR_FILES,
                    "OUT_DIR is too large to record"
                );
                files.push((path, format!("{OUT_PREFIX}{relative}")));
            } else {
                anyhow::bail!("OUT_DIR contains an unsupported entry");
            }
        }
    }
    Ok(OutDirContents {
        files,
        directories,
        empty_files: empty,
    })
}

fn directory_is_empty(directory: &Path) -> bool {
    std::fs::read_dir(directory).is_ok_and(|mut entries| entries.next().is_none())
}

fn clear_directory(directory: &Path) -> Result<()> {
    std::fs::create_dir_all(directory)?;
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        if std::fs::symlink_metadata(&path)?.is_dir() {
            std::fs::remove_dir_all(&path)?;
        } else {
            std::fs::remove_file(&path)?;
        }
    }
    Ok(())
}

/// Whether anything under `path` (a file or a tree) was modified at or after
/// `since`.
fn modified_since(path: &Path, excluded: &[PathBuf], since: std::time::SystemTime) -> Result<bool> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    if metadata.modified()? >= since {
        return Ok(true);
    }
    if metadata.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let child = entry?.path();
            if excluded.contains(&child) {
                continue;
            }
            if modified_since(&child, excluded, since)? {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn is_executable(metadata: &std::fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        false
    }
}

fn package_exclusions(package: &Path, environment: &Environment) -> Vec<PathBuf> {
    let mut excluded = vec![package.join(".git"), package.join("target")];
    for (root, placeholder) in &environment.mappings {
        if *placeholder == "${KACHE_TARGET_DIR}" && root.starts_with(package) {
            excluded.push(root.clone());
        }
    }
    excluded
}

/// A digest of what is at `path`: content for a file, the recursive listing
/// for a directory, the target and referent for a symlink, a marker for
/// nothing. Cargo's own freshness compares the same things.
fn input_state(
    path: &Path,
    excluded: &[PathBuf],
    file_hasher: &crate::cache_key::FileHasher<'_>,
    budget: &mut usize,
    symlink_depth: usize,
) -> Result<String> {
    anyhow::ensure!(
        *budget > 0,
        "declared build-script inputs are too many to digest"
    );
    *budget -= 1;
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok("missing".to_string());
        }
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() {
        anyhow::ensure!(
            symlink_depth < 64,
            "declared build-script input is a symlink cycle"
        );
        let target = std::fs::read_link(path)?;
        let resolved = if target.is_absolute() {
            target.clone()
        } else {
            path.parent().unwrap_or(Path::new("")).join(&target)
        };
        let referent = input_state(&resolved, excluded, file_hasher, budget, symlink_depth + 1)?;
        return Ok(format!("symlink:{}:{referent}", target.to_string_lossy()));
    }
    if metadata.is_file() {
        return Ok(format!("file:{}", file_hasher.hash(path)?));
    }
    if metadata.is_dir() {
        // A declared directory (a vendored C library, the package itself) can
        // hold thousands of files; hashing each through the per-file memo
        // costs a database read per file. One stat walk yields a stamp of
        // every entry's identity, size and times, and the digest of a tree
        // whose stamp is unchanged is the memoised one.
        let stamp = if symlink_depth == 0 {
            tree_stamp(path, excluded, *budget)
        } else {
            None
        };
        if let Some(stamp) = &stamp
            && let Some(digest) = tree_digest_memo(path, &stamp.digest)
        {
            return Ok(digest);
        }
        let digest = hash_directory(path, excluded, file_hasher, budget, symlink_depth)?;
        // Filesystem timestamps are coarse (a kernel tick on Linux), so a
        // same-size rewrite within the tick of the last write would keep the
        // stamp. A tree touched in the last seconds is hashed again next time
        // rather than memoised; the following run finds it settled.
        if let Some(stamp) = &stamp
            && stamp.settled_at(std::time::SystemTime::now())
        {
            record_tree_digest_memo(path, &stamp.digest, &digest);
        }
        return Ok(digest);
    }
    anyhow::bail!(
        "declared build-script input is neither a file nor a directory: {}",
        path.display()
    )
}

fn hash_directory(
    path: &Path,
    excluded: &[PathBuf],
    file_hasher: &crate::cache_key::FileHasher<'_>,
    budget: &mut usize,
    symlink_depth: usize,
) -> Result<String> {
    let mut entries: Vec<_> = std::fs::read_dir(path)?.collect::<std::io::Result<_>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    let mut hasher = blake3::Hasher::new();
    for entry in entries {
        let child = entry.path();
        if excluded.contains(&child) {
            continue;
        }
        fold(&mut hasher, "name", entry.file_name().as_encoded_bytes());
        fold(
            &mut hasher,
            "state",
            input_state(&child, excluded, file_hasher, budget, symlink_depth)?.as_bytes(),
        );
    }
    Ok(format!("dir:{}", hasher.finalize().to_hex()))
}

/// A digest of every entry under `path` by name, kind, size, modification
/// and change time, from one stat walk. Symlinks contribute their link text
/// only, so a tree with a symlink to something outside it is not memoised.
/// `None` when the tree is larger than the budget or holds a symlink.
struct TreeStamp {
    digest: String,
    /// The newest modification time seen in the walk.
    newest: std::time::SystemTime,
}

impl TreeStamp {
    /// Coarse filesystem clocks make a stamp taken within this window of its
    /// newest write ambiguous.
    const SETTLE: std::time::Duration = std::time::Duration::from_secs(2);

    fn settled_at(&self, now: std::time::SystemTime) -> bool {
        now.duration_since(self.newest)
            .is_ok_and(|age| age >= Self::SETTLE)
    }
}

fn tree_stamp(path: &Path, excluded: &[PathBuf], budget: usize) -> Option<TreeStamp> {
    let mut hasher = blake3::Hasher::new();
    let mut newest = std::time::SystemTime::UNIX_EPOCH;
    let mut remaining = budget;
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let mut entries: Vec<_> = std::fs::read_dir(&directory)
            .ok()?
            .collect::<std::io::Result<_>>()
            .ok()?;
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            let child = entry.path();
            if excluded.contains(&child) {
                continue;
            }
            remaining = remaining.checked_sub(1)?;
            let metadata = std::fs::symlink_metadata(&child).ok()?;
            if metadata.file_type().is_symlink() {
                return None;
            }
            fold(
                &mut hasher,
                "entry",
                child
                    .strip_prefix(path)
                    .ok()?
                    .as_os_str()
                    .as_encoded_bytes(),
            );
            hasher.update(if metadata.is_dir() { b"dir" } else { b"fil" });
            fold_metadata_stamp(&mut hasher, &metadata);
            if let Ok(modified) = metadata.modified()
                && modified > newest
            {
                newest = modified;
            }
            if metadata.is_dir() {
                pending.push(child);
            }
        }
    }
    Some(TreeStamp {
        digest: hasher.finalize().to_hex().to_string(),
        newest,
    })
}

/// Where tree digests are memoised: under the configured cache directory
/// once a run has loaded its configuration, else the environment's or the
/// default one.
static TREE_MEMO_DIR: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();

/// Size and times of one entry, plus the inode where the platform has one.
fn fold_metadata_stamp(hasher: &mut blake3::Hasher, metadata: &std::fs::Metadata) {
    hasher.update(&metadata.len().to_le_bytes());
    for time in [metadata.modified().ok(), metadata.created().ok()] {
        let nanos = time
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(0, |d| d.as_nanos());
        hasher.update(&nanos.to_le_bytes());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        for value in [
            metadata.ctime() as u64,
            metadata.ctime_nsec() as u64,
            metadata.ino(),
        ] {
            hasher.update(&value.to_le_bytes());
        }
    }
}

fn tree_memo_path(path: &Path) -> PathBuf {
    let name = blake3::hash(path.as_os_str().as_encoded_bytes()).to_hex();
    TREE_MEMO_DIR
        .get()
        .map(|cache_dir| cache_dir.join("probes"))
        .unwrap_or_else(crate::config::probe_memo_dir)
        .join(format!("tree-{}.txt", &name[..24]))
}

fn tree_digest_memo(path: &Path, stamp: &str) -> Option<String> {
    let memo = std::fs::read_to_string(tree_memo_path(path)).ok()?;
    let (recorded_stamp, digest) = memo.trim_end().split_once('\n')?;
    (recorded_stamp == stamp && digest.starts_with("dir:")).then(|| digest.to_string())
}

fn record_tree_digest_memo(path: &Path, stamp: &str, digest: &str) {
    crate::probe_memo::write_atomic(&tree_memo_path(path), &format!("{stamp}\n{digest}"));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn environment(out_dir: &Path, manifest_dir: &Path) -> Environment {
        let mut mappings = vec![
            (out_dir.to_path_buf(), "${KACHE_OUT_DIR}"),
            (manifest_dir.to_path_buf(), "${KACHE_MANIFEST_DIR}"),
        ];
        mappings.sort_by_key(|(root, _)| std::cmp::Reverse(root.as_os_str().len()));
        Environment {
            out_dir: out_dir.to_path_buf(),
            manifest_dir: manifest_dir.to_path_buf(),
            mappings,
        }
    }

    #[test]
    fn find_bytes_handles_bounds_and_repeated_first_bytes() {
        assert_eq!(find_bytes(b"abcabc", b"bc"), Some(1));
        assert_eq!(find_bytes(b"xxab", b"ab"), Some(2));
        assert_eq!(
            find_bytes(b"aab", b"ab"),
            Some(1),
            "a false first-byte match moves on"
        );
        assert_eq!(
            find_bytes(b"abc", b"abc"),
            Some(0),
            "a needle the size of the haystack"
        );
        assert_eq!(find_bytes(b"ab", b"abc"), None);
        assert_eq!(find_bytes(b"abc", b""), None);
        assert_eq!(find_bytes(b"", b"a"), None);
        assert_eq!(
            replace_all(b"/t/out/x /t/out/y", b"/t/out", b"${O}"),
            b"${O}/x ${O}/y"
        );
        assert_eq!(replace_all(b"keep", b"", b"x"), b"keep");
    }

    #[test]
    fn package_exclusions_cover_vcs_target_and_mapped_target_roots_inside_the_package() {
        let package = Path::new("/src/pkg");
        let environment = Environment {
            out_dir: PathBuf::from("/src/pkg/target/debug/build/pkg-1/out"),
            manifest_dir: package.to_path_buf(),
            mappings: vec![
                (
                    PathBuf::from("/src/pkg/target/debug"),
                    "${KACHE_TARGET_DIR}",
                ),
                (PathBuf::from("/src/pkg"), "${KACHE_MANIFEST_DIR}"),
                (
                    PathBuf::from("/elsewhere/target/debug"),
                    "${KACHE_TARGET_DIR}",
                ),
            ],
        };
        let excluded = package_exclusions(package, &environment);
        assert_eq!(
            excluded,
            vec![
                PathBuf::from("/src/pkg/.git"),
                PathBuf::from("/src/pkg/target"),
                PathBuf::from("/src/pkg/target/debug"),
            ],
            "only target roots under the package are excluded, never the manifest root"
        );
    }

    #[cfg(unix)]
    #[test]
    fn is_executable_reads_any_execute_bit() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f");
        std::fs::write(&file, "x").unwrap();
        for (mode, expected) in [
            (0o644, false),
            (0o600, false),
            (0o755, true),
            (0o700, true),
            (0o010, true),
        ] {
            std::fs::set_permissions(&file, std::fs::Permissions::from_mode(mode)).unwrap();
            assert_eq!(
                is_executable(&std::fs::metadata(&file).unwrap()),
                expected,
                "mode {mode:o}"
            );
        }
    }

    #[test]
    fn target_dir_strips_the_unit_profile_and_triple() {
        let _lock = crate::test_support::process_state_test_lock();
        // SAFETY: the process-state lock serialises environment edits.
        unsafe { std::env::remove_var("TARGET") };
        assert_eq!(
            target_dir(Path::new("/t/debug/build/pkg-1/out")),
            Some(PathBuf::from("/t"))
        );
        assert_eq!(
            target_dir(Path::new(
                "/t/x86_64-unknown-linux-gnu/debug/build/pkg-1/out"
            )),
            Some(PathBuf::from("/t/x86_64-unknown-linux-gnu")),
            "without TARGET the triple directory is not stripped"
        );
        unsafe { std::env::set_var("TARGET", "x86_64-unknown-linux-gnu") };
        assert_eq!(
            target_dir(Path::new(
                "/t/x86_64-unknown-linux-gnu/debug/build/pkg-1/out"
            )),
            Some(PathBuf::from("/t"))
        );
        assert_eq!(
            target_dir(Path::new("/t/debug/build/pkg-1/out")),
            Some(PathBuf::from("/t")),
            "a profile directory that is not the triple is kept"
        );
        unsafe { std::env::remove_var("TARGET") };
        assert_eq!(target_dir(Path::new("/t/debug/other/pkg-1/out")), None);
        assert_eq!(target_dir(Path::new("out")), None);
    }

    #[test]
    fn cargo_environment_lists_fixed_and_prefixed_variables_with_normalized_values() {
        let _lock = crate::test_support::process_state_test_lock();
        let env = environment(Path::new("/t/debug/build/pkg-1/out"), Path::new("/src/pkg"));
        let saved: Vec<(&str, Option<std::ffi::OsString>)> = ["CARGO_PKG_NAME", "PKG_CONFIG_PATH"]
            .into_iter()
            .map(|name| (name, std::env::var_os(name)))
            .collect();
        // SAFETY: the process-state lock serialises environment edits.
        unsafe {
            std::env::set_var("CARGO_PKG_NAME", "kt");
            std::env::set_var("DEP_Z_INCLUDE", "/t/debug/build/z-1/out/include");
            std::env::set_var("CARGO_CFG_UNIX", "");
            std::env::set_var("KACHE_TEST_UNRELATED_VAR", "no");
            std::env::remove_var("PKG_CONFIG_PATH");
        }
        let vars = cargo_environment(&env);
        unsafe {
            std::env::remove_var("DEP_Z_INCLUDE");
            std::env::remove_var("CARGO_CFG_UNIX");
            std::env::remove_var("KACHE_TEST_UNRELATED_VAR");
            for (name, value) in saved {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
        assert_eq!(vars["CARGO_PKG_NAME"].as_deref(), Some("kt"));
        assert_eq!(
            vars["DEP_Z_INCLUDE"].as_deref(),
            Some("/t/debug/build/z-1/out/include"),
            "values outside a mapped root stay verbatim"
        );
        assert_eq!(vars["CARGO_CFG_UNIX"].as_deref(), Some(""));
        assert!(
            vars.contains_key("TARGET"),
            "fixed names are listed even when unset"
        );
        assert_eq!(vars["PKG_CONFIG_PATH"], None);
        assert!(!vars.contains_key("KACHE_TEST_UNRELATED_VAR"));
        assert!(vars.len() >= 16);
    }

    #[test]
    fn the_switch_only_turns_off_on_zero_or_false() {
        let _lock = crate::test_support::process_state_test_lock();
        // SAFETY: the process-state lock serialises environment edits.
        unsafe { std::env::remove_var(ENABLE_ENV) };
        assert!(enabled());
        for (value, expected) in [
            ("0", false),
            ("false", false),
            ("1", true),
            ("yes", true),
            ("", true),
        ] {
            unsafe { std::env::set_var(ENABLE_ENV, value) };
            assert_eq!(enabled(), expected, "{value:?}");
        }
        unsafe { std::env::remove_var(ENABLE_ENV) };
    }

    #[test]
    fn only_a_build_script_bin_in_a_build_unit_directory_is_a_compiled_build_script() {
        let parse = |argv: &[&str]| {
            RustcArgs::parse(&argv.iter().map(|a| (*a).to_string()).collect::<Vec<_>>()).unwrap()
        };
        let dir = tempfile::tempdir().unwrap();
        let unit = dir.path().join("debug").join("build").join("pkg-1");
        std::fs::create_dir_all(&unit).unwrap();
        let unit_str = unit.to_str().unwrap();
        let good = parse(&[
            "rustc",
            "--crate-name",
            "build_script_build",
            "--crate-type",
            "bin",
            "build.rs",
            "--out-dir",
            unit_str,
            "-C",
            "extra-filename=-1",
        ]);
        assert_eq!(
            compiled_build_script(&good),
            None,
            "the binary has to exist before it can be shimmed"
        );
        let binary = unit.join(crate::args::format_crate_output_stem(
            "build_script_build",
            "-1",
        ));
        std::fs::write(&binary, "elf").unwrap();
        assert_eq!(compiled_build_script(&good), Some(binary));
        let deps = dir.path().join("debug").join("deps");
        std::fs::create_dir_all(&deps).unwrap();
        let deps_str = deps.to_str().unwrap();
        for (stem, at) in [("pkg-1", &unit), ("build_script_build-1", &deps)] {
            std::fs::write(at.join(stem), "elf").unwrap();
        }
        for (label, argv) in [
            (
                "not a build script",
                vec![
                    "rustc",
                    "--crate-name",
                    "pkg",
                    "--crate-type",
                    "bin",
                    "src/main.rs",
                    "--out-dir",
                    unit_str,
                    "-C",
                    "extra-filename=-1",
                ],
            ),
            (
                "not a bin",
                vec![
                    "rustc",
                    "--crate-name",
                    "build_script_build",
                    "--crate-type",
                    "lib",
                    "build.rs",
                    "--out-dir",
                    unit_str,
                    "-C",
                    "extra-filename=-1",
                ],
            ),
            (
                "not under build/",
                vec![
                    "rustc",
                    "--crate-name",
                    "build_script_build",
                    "--crate-type",
                    "bin",
                    "build.rs",
                    "--out-dir",
                    deps_str,
                    "-C",
                    "extra-filename=-1",
                ],
            ),
        ] {
            assert!(compiled_build_script(&parse(&argv)).is_none(), "{label}");
        }
    }

    #[test]
    fn an_empty_rerun_if_env_changed_name_is_not_a_declaration() {
        let env = environment(Path::new("/t/build/pkg-1/out"), Path::new("/src/pkg"));
        let (inputs, names, default) =
            parse_declarations("cargo:rerun-if-env-changed=\n", &env).unwrap();
        assert!(names.is_empty());
        assert!(default, "nothing declared means the package is the input");
        assert_eq!(inputs, ["${KACHE_MANIFEST_DIR}"]);

        let (inputs, names, default) =
            parse_declarations("cargo:rerun-if-env-changed=FOO\n", &env).unwrap();
        assert_eq!(names, ["FOO"]);
        assert!(!default, "a declared variable is a declaration");
        assert!(inputs.is_empty());

        let (inputs, names, default) =
            parse_declarations("cargo:rerun-if-changed=build.rs\n", &env).unwrap();
        assert!(names.is_empty());
        assert!(!default, "a declared path is a declaration");
        assert_eq!(
            inputs,
            [format!(
                "${{KACHE_MANIFEST_DIR}}{}build.rs",
                std::path::MAIN_SEPARATOR
            )]
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_symlinked_root_maps_both_spellings_and_a_plain_root_maps_once() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real");
        std::fs::create_dir_all(real.join("out")).unwrap();
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        // SAFETY: the process-state lock serialises environment edits.
        unsafe {
            std::env::set_var("OUT_DIR", link.join("out"));
            std::env::set_var("CARGO_MANIFEST_DIR", &real);
            std::env::remove_var("CARGO_TARGET_DIR");
            std::env::remove_var("CARGO_HOME");
        }
        let environment = Environment::capture();
        unsafe {
            std::env::remove_var("OUT_DIR");
            std::env::remove_var("CARGO_MANIFEST_DIR");
        }
        let environment = environment.unwrap();
        let canonical_out = std::fs::canonicalize(link.join("out")).unwrap();
        let out_roots: Vec<_> = environment
            .mappings
            .iter()
            .filter(|(_, placeholder)| *placeholder == "${KACHE_OUT_DIR}")
            .map(|(root, _)| root.clone())
            .collect();
        assert!(out_roots.contains(&link.join("out")), "{out_roots:?}");
        assert!(out_roots.contains(&canonical_out), "{out_roots:?}");
        let manifest_roots = environment
            .mappings
            .iter()
            .filter(|(_, placeholder)| *placeholder == "${KACHE_MANIFEST_DIR}")
            .count();
        assert_eq!(
            manifest_roots,
            usize::from(std::fs::canonicalize(&real).unwrap() != real) + 1,
            "a root that is already canonical is mapped once"
        );
    }

    #[test]
    fn out_dir_portability_depends_on_spelled_roots() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out");
        std::fs::create_dir(&out).unwrap();
        let env = environment(&out, Path::new("/src/pkg"));
        let plain = out.join("plain.h");
        std::fs::write(&plain, "#define X 1").unwrap();
        assert!(
            env.out_dir_is_portable(&[(plain.clone(), "plain.h".into())])
                .unwrap()
        );
        let spelled = out.join("paths.txt");
        std::fs::write(&spelled, format!("root={}", out.display())).unwrap();
        assert!(
            !env.out_dir_is_portable(&[(spelled, "paths.txt".into())])
                .unwrap()
        );
        let manifest = out.join("manifest.txt");
        std::fs::write(&manifest, "at /src/pkg/src").unwrap();
        assert!(
            !env.out_dir_is_portable(&[(manifest, "manifest.txt".into())])
                .unwrap()
        );
    }

    #[test]
    fn modified_since_distinguishes_absent_from_unreadable() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f");
        std::fs::write(&file, "x").unwrap();
        let later = std::time::SystemTime::now() + std::time::Duration::from_secs(60);
        assert!(!modified_since(&dir.path().join("absent"), &[], later).unwrap());
        // Windows reports a path through a file as not found; Unix as ENOTDIR.
        #[cfg(unix)]
        assert!(
            modified_since(&file.join("child"), &[], later).is_err(),
            "a path through a file is an error, not an absence"
        );
        let hasher = crate::cache_key::FileHasher::new();
        let mut budget = 10;
        #[cfg(unix)]
        assert!(
            input_state(&file.join("child"), &[], &hasher, &mut budget, 0).is_err(),
            "an unreadable declared input is an error, not a missing one"
        );
        assert_eq!(
            input_state(&dir.path().join("absent"), &[], &hasher, &mut budget, 0).unwrap(),
            "missing"
        );
    }

    #[test]
    fn an_out_dir_past_the_byte_cap_is_not_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out");
        std::fs::create_dir(&out).unwrap();
        // Sparse: the cap is judged on the size the metadata reports.
        let big = std::fs::File::create(out.join("big.bin")).unwrap();
        big.set_len(MAX_OUT_DIR_BYTES + 1).unwrap();
        assert!(collect_out_dir(&out).is_err());
        big.set_len(MAX_OUT_DIR_BYTES / 2).unwrap();
        let second = std::fs::File::create(out.join("second.bin")).unwrap();
        second.set_len(MAX_OUT_DIR_BYTES / 2 + 1).unwrap();
        assert!(collect_out_dir(&out).is_err(), "sizes add up across files");
        second.set_len(1).unwrap();
        assert_eq!(collect_out_dir(&out).unwrap().files.len(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn run_real_reports_the_script_status_or_one_when_it_cannot_start() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let script = |name: &str, body: &str| {
            let path = dir.path().join(name);
            std::fs::write(&path, body).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            path
        };
        assert_eq!(run_real(&script("three", "#!/bin/sh\nexit 3\n"), &[]), 3);
        assert_eq!(run_real(&script("zero", "#!/bin/sh\nexit 0\n"), &[]), 0);
        assert_eq!(
            run_real(
                &script("arg", "#!/bin/sh\ntest \"$1\" = arg\n"),
                &["arg".into()]
            ),
            0,
            "arguments reach the script"
        );
        assert_eq!(run_real(&dir.path().join("absent"), &[]), 1);
    }

    #[cfg(unix)]
    #[test]
    fn set_executable_sets_and_clears_the_execute_bits() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f");
        std::fs::write(&file, "x").unwrap();
        set_executable(&file, true).unwrap();
        assert_eq!(
            std::fs::metadata(&file).unwrap().permissions().mode() & 0o777,
            0o755
        );
        set_executable(&file, false).unwrap();
        assert_eq!(
            std::fs::metadata(&file).unwrap().permissions().mode() & 0o777,
            0o644
        );
        assert!(set_executable(&dir.path().join("absent"), true).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn the_launcher_is_not_installed_while_the_switch_is_off() {
        use std::os::unix::fs::PermissionsExt;
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let unit = dir.path().join("debug/build/pkg-1");
        std::fs::create_dir_all(&unit).unwrap();
        let executable = unit.join("build_script_build-1");
        std::fs::write(&executable, "#!/bin/sh\necho real\n").unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        let args = RustcArgs::parse(
            &[
                "rustc",
                "--crate-name",
                "build_script_build",
                "--crate-type",
                "bin",
                "build.rs",
                "--out-dir",
                unit.to_str().unwrap(),
                "-C",
                "extra-filename=-1",
            ]
            .map(String::from),
        )
        .unwrap();
        // SAFETY: the process-state lock serialises environment edits.
        unsafe { std::env::set_var(ENABLE_ENV, "0") };
        install_shim(&args);
        unsafe { std::env::remove_var(ENABLE_ENV) };
        assert_eq!(
            std::fs::read_to_string(&executable).unwrap(),
            "#!/bin/sh\necho real\n",
            "the binary is left alone"
        );
        assert!(!with_suffix(&executable, REAL_SUFFIX).exists());
        install_shim(&args);
        assert!(
            with_suffix(&executable, REAL_SUFFIX).is_file(),
            "the same invocation installs once the switch is back on"
        );
    }

    /// The prediction row is addressed by the script binary: two scripts
    /// never share declarations, and the same binary always finds its own.
    #[test]
    fn a_run_identity_names_the_binary() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(dir.path().join("cache"));
        let run = |binary_hash: &str| Run {
            store: Store::open(&config).unwrap(),
            config: config.clone(),
            binary_hash: binary_hash.to_string(),
            environment: environment(&dir.path().join("out"), &dir.path().join("pkg")),
            start: std::time::Instant::now(),
        };
        let a = run("aaaa");
        let identity = a.identity();
        assert!(identity.starts_with(PREDICTION_PREFIX), "{identity}");
        assert!(identity.len() > PREDICTION_PREFIX.len() + 32);
        assert_eq!(identity, a.identity(), "stable across calls");
        assert_ne!(identity, run("bbbb").identity());
    }

    /// A `links` dependency hands its outputs down as `DEP_*` paths. What is
    /// at an absolute path is part of the key; a relative or missing value
    /// contributes its spelling only.
    #[test]
    fn dep_paths_are_keyed_by_content_only_when_absolute_and_present() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(dir.path().join("cache"));
        let run = Run {
            store: Store::open(&config).unwrap(),
            config: config.clone(),
            binary_hash: "aaaa".to_string(),
            environment: environment(&dir.path().join("out"), &dir.path().join("pkg")),
            start: std::time::Instant::now(),
        };
        let prediction = Prediction {
            version: PREDICTION_SCHEMA,
            inputs: Vec::new(),
            env: Vec::new(),
            default_package: false,
            portable_out_dir: true,
        };
        let absolute = dir.path().join("include.h");
        std::fs::write(&absolute, "one").unwrap();
        std::fs::write(dir.path().join("relative.h"), "one").unwrap();
        std::env::set_current_dir(dir.path()).unwrap();
        // SAFETY: the process-state lock serialises environment edits.
        unsafe { std::env::set_var("DEP_KT_INCLUDE", &absolute) };
        let before = run.action_key(&prediction).unwrap();
        std::fs::write(&absolute, "two").unwrap();
        let after = run.action_key(&prediction).unwrap();
        assert_ne!(
            before, after,
            "the file behind an absolute DEP_ path is keyed"
        );

        unsafe { std::env::set_var("DEP_KT_INCLUDE", "relative.h") };
        let before = run.action_key(&prediction).unwrap();
        std::fs::write(dir.path().join("relative.h"), "two").unwrap();
        let after = run.action_key(&prediction).unwrap();
        assert_eq!(before, after, "a relative value is only a spelling");
        unsafe { std::env::remove_var("DEP_KT_INCLUDE") };
    }

    #[test]
    fn input_digests_are_bounded_and_refuse_symlink_cycles() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("src");
        std::fs::create_dir(&root).unwrap();
        for i in 0..5 {
            std::fs::write(root.join(format!("{i}.c")), "int x;").unwrap();
        }
        let hasher = crate::cache_key::FileHasher::new();
        let mut budget = 3;
        assert!(
            input_state(&root, &[], &hasher, &mut budget, 0).is_err(),
            "six entries exceed a budget of three"
        );
        let mut budget = 100;
        assert!(input_state(&root, &[], &hasher, &mut budget, 0).is_ok());
        assert_eq!(budget, 94, "every entry costs one");
        #[cfg(unix)]
        {
            let a = dir.path().join("a");
            let b = dir.path().join("b");
            std::os::unix::fs::symlink(&b, &a).unwrap();
            std::os::unix::fs::symlink(&a, &b).unwrap();
            let mut budget = 1000;
            let error = input_state(&a, &[], &hasher, &mut budget, 0).unwrap_err();
            assert!(
                error.to_string().contains("symlink cycle"),
                "the cycle is named before the budget runs out: {error:#}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn declarations_follow_cargo_rerun_rules() {
        let env = environment(Path::new("/t/build/pkg-1/out"), Path::new("/src/pkg"));
        let (inputs, names, default) = parse_declarations(
            "cargo:rerun-if-changed=build.rs\ncargo::rerun-if-env-changed=FOO\ncargo:rustc-link-lib=z\n",
            &env,
        )
        .unwrap();
        assert_eq!(inputs, ["${KACHE_MANIFEST_DIR}/build.rs"]);
        assert_eq!(names, ["FOO"]);
        assert!(!default);

        let (inputs, names, default) = parse_declarations("cargo:rustc-cfg=x\n", &env).unwrap();
        assert_eq!(inputs, ["${KACHE_MANIFEST_DIR}"]);
        assert!(names.is_empty());
        assert!(default);

        assert!(parse_declarations("cargo:rerun-if-changed=\n", &env).is_none());
        assert!(
            parse_declarations("cargo:rerun-if-changed=/t/build/pkg-1/out/gen.rs\n", &env)
                .is_none()
        );
    }

    #[test]
    fn output_roots_round_trip_through_placeholders() {
        let env = environment(Path::new("/t/build/pkg-1/out"), Path::new("/t"));
        let text = "cargo:rustc-link-search=native=/t/build/pkg-1/out/build\ncargo:root=/t\n";
        let normalized = env.normalize_str(text);
        assert_eq!(
            normalized,
            "cargo:rustc-link-search=native=${KACHE_OUT_DIR}/build\ncargo:root=${KACHE_MANIFEST_DIR}\n"
        );
        assert_eq!(env.denormalize_str(&normalized), text);
    }

    #[test]
    fn target_dir_is_recovered_from_out_dir() {
        assert_eq!(
            target_dir(Path::new("/w/target/debug/build/pkg-1/out")),
            Some(PathBuf::from("/w/target"))
        );
        assert_eq!(target_dir(Path::new("/w/other/pkg-1/out")), None);
    }

    #[test]
    fn input_state_tracks_content_names_and_absence() {
        let dir = tempfile::tempdir().unwrap();
        let hasher = crate::cache_key::FileHasher::new();
        let mut budget = 100;
        let root = dir.path().join("src");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("a.c"), "int a;").unwrap();
        let first = input_state(&root, &[], &hasher, &mut budget, 0).unwrap();
        std::fs::write(root.join("a.c"), "int b;").unwrap();
        let changed = input_state(&root, &[], &hasher, &mut budget, 0).unwrap();
        assert_ne!(first, changed);
        std::fs::write(root.join("a.c"), "int a;").unwrap();
        assert_eq!(
            first,
            input_state(&root, &[], &hasher, &mut budget, 0).unwrap()
        );
        std::fs::write(root.join("b.c"), "").unwrap();
        assert_ne!(
            first,
            input_state(&root, &[], &hasher, &mut budget, 0).unwrap()
        );
        std::fs::remove_file(root.join("b.c")).unwrap();
        let excluded = vec![root.join("target")];
        std::fs::create_dir(root.join("target")).unwrap();
        std::fs::write(root.join("target/x"), "x").unwrap();
        assert_eq!(
            first,
            input_state(&root, &excluded, &hasher, &mut budget, 0).unwrap()
        );
        assert_eq!(
            input_state(&root.join("missing"), &[], &hasher, &mut budget, 0).unwrap(),
            "missing"
        );
    }

    #[test]
    fn out_dir_collection_separates_empty_files_and_directories() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out");
        std::fs::create_dir_all(out.join("build/sub")).unwrap();
        std::fs::write(out.join("build/a.o"), "obj").unwrap();
        std::fs::write(out.join("empty"), "").unwrap();
        let contents = collect_out_dir(&out).unwrap();
        assert_eq!(
            contents
                .files
                .iter()
                .map(|(_, name)| name.as_str())
                .collect::<Vec<_>>(),
            ["out/build/a.o"]
        );
        assert_eq!(contents.directories, ["build", "build/sub"]);
        assert_eq!(contents.empty_files.len(), 1);
        assert_eq!(contents.empty_files[0].name, "empty");
        assert!(checked_relative("../x").is_err());
        assert!(checked_relative("/x").is_err());
        assert!(checked_relative("a/b").is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn a_tree_digest_is_memoised_by_its_stamp_and_forgets_on_change() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        // SAFETY: the process-state lock serialises environment edits.
        unsafe { std::env::set_var("KACHE_CACHE_DIR", &cache) };
        let root = dir.path().join("lib");
        std::fs::create_dir_all(root.join("src")).unwrap();
        std::fs::write(root.join("src/a.c"), "int a;").unwrap();
        let hasher = crate::cache_key::FileHasher::new();
        let mut budget = 100;

        // Just written: the tree has not settled, so nothing is memoised.
        let stamp = tree_stamp(&root, &[], 100).unwrap();
        let digest = input_state(&root, &[], &hasher, &mut budget, 0).unwrap();
        assert!(tree_digest_memo(&root, &stamp.digest).is_none());
        assert!(!stamp.settled_at(std::time::SystemTime::now()));
        assert!(stamp.settled_at(std::time::SystemTime::now() + TreeStamp::SETTLE));

        // Age the tree past the settle window, then the memo takes.
        let old = filetime::FileTime::from_unix_time(1_000_000_000, 0);
        for entry in [root.join("src/a.c"), root.join("src"), root.clone()] {
            filetime::set_file_mtime(&entry, old).unwrap();
        }
        let settled = tree_stamp(&root, &[], 100).unwrap();
        assert!(settled.settled_at(std::time::SystemTime::now()));
        assert_eq!(
            input_state(&root, &[], &hasher, &mut budget, 0).unwrap(),
            digest
        );
        assert_eq!(
            tree_digest_memo(&root, &settled.digest).as_deref(),
            Some(digest.as_str())
        );

        // A rewrite with the same bytes changes the stamp (mtime), so the
        // tree is hashed again and the digest is unchanged.
        std::fs::write(root.join("src/a.c"), "int a;").unwrap();
        let restamped = tree_stamp(&root, &[], 100).unwrap();
        assert_ne!(restamped.digest, settled.digest);
        assert!(tree_digest_memo(&root, &restamped.digest).is_none());
        assert_eq!(
            input_state(&root, &[], &hasher, &mut budget, 0).unwrap(),
            digest
        );
        // A content change changes the digest.
        std::fs::write(root.join("src/a.c"), "int b;").unwrap();
        assert_ne!(
            input_state(&root, &[], &hasher, &mut budget, 0).unwrap(),
            digest
        );
        // A symlink inside the tree disables the memo.
        std::os::unix::fs::symlink("a.c", root.join("src/link.c")).unwrap();
        assert!(tree_stamp(&root, &[], 100).is_none());
        unsafe { std::env::remove_var("KACHE_CACHE_DIR") };
    }

    #[test]
    fn inputs_written_during_the_run_and_dirty_out_dirs_are_not_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out");
        std::fs::create_dir(&out).unwrap();
        assert!(directory_is_empty(&out));
        std::fs::write(out.join("stale"), "x").unwrap();
        std::fs::create_dir(out.join("sub")).unwrap();
        assert!(!directory_is_empty(&out));
        clear_directory(&out).unwrap();
        assert!(directory_is_empty(&out));

        let src = dir.path().join("src");
        std::fs::create_dir(&src).unwrap();
        std::fs::write(src.join("a.c"), "int a;").unwrap();
        let later = std::time::SystemTime::now() + std::time::Duration::from_secs(60);
        assert!(!modified_since(&src, &[], later).unwrap());
        let earlier = std::time::SystemTime::now() - std::time::Duration::from_secs(60);
        assert!(modified_since(&src, &[], earlier).unwrap());
        assert!(modified_since(&src.join("a.c"), &[], earlier).unwrap());
        assert!(!modified_since(&src.join("missing"), &[], earlier).unwrap());
        let excluded = vec![src.join("a.c")];
        // The directory itself was also modified after `earlier`; only the
        // excluded child is skipped, so this still reports the directory.
        assert!(modified_since(&src, &excluded, earlier).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn launcher_preserves_binary_and_finds_it_from_the_hardlink() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let unit = dir.path().join("debug/build/pkg-1");
        std::fs::create_dir_all(&unit).unwrap();
        let executable = unit.join("build_script_build-1");
        std::fs::write(&executable, "#!/bin/sh\necho real\n").unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        install(&executable).unwrap();
        let real = with_suffix(&executable, REAL_SUFFIX);
        assert_eq!(
            std::fs::read_to_string(&real).unwrap(),
            "#!/bin/sh\necho real\n"
        );
        assert_eq!(std::fs::read(&executable).unwrap(), LAUNCHER);
        let (real_name, pinned) = launch_record(&unit);
        assert_eq!(real_name, "build_script_build-1.kache-real");
        assert!(pinned.starts_with(dir.path().join("debug").join(SHIM_DIR)));
        assert!(pinned.is_file(), "the record names the pinned kache");
        // The launcher runs the preserved script when the pinned kache is gone.
        std::fs::remove_dir_all(dir.path().join("debug").join(SHIM_DIR)).unwrap();
        let output = std::process::Command::new(&executable).output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(String::from_utf8_lossy(&output.stdout), "real\n");
        let hardlink = unit.join("build-script-build");
        std::fs::hard_link(&executable, &hardlink).unwrap();
        assert_eq!(find_real(&hardlink), Some(real.clone()));
        assert_eq!(find_real(&executable), Some(real.clone()));
        assert_eq!(
            stored_binary_hash(&real).unwrap(),
            kache_store::file_hash::hash_file(&real).unwrap()
        );
    }

    /// Cargo spells a `links` dependency's metadata keys as the script printed
    /// them, so `DEP_*` names can hold `:`, `-` or `.`. A shell launcher drops
    /// those under dash; the launcher must hand every name on, to kache and to
    /// the preserved script alike.
    #[cfg(unix)]
    #[test]
    fn the_launcher_passes_every_variable_name_on() {
        const NAME: &str = "DEP_TAURI_CORE:WINDOW__CORE-PLUGIN.PERMISSION_FILES_PATH";
        let dir = tempfile::tempdir().unwrap();
        let unit = dir.path().join("debug/build/pkg-1");
        std::fs::create_dir_all(&unit).unwrap();
        // `env` stands in for both the script and kache: it prints exactly the
        // environment it was started with. Found on PATH, since the Nix build
        // sandbox has no /usr/bin/env. A link, not a copy: macOS kills a
        // copied system binary.
        let env = std::env::split_paths(&std::env::var_os("PATH").unwrap())
            .map(|directory| directory.join("env"))
            .find(|candidate| candidate.is_file())
            .expect("an env binary on PATH");
        let executable = unit.join("build_script_build-1");
        std::os::unix::fs::symlink(&env, &executable).unwrap();
        install(&executable).unwrap();
        let (_, pinned) = launch_record(&unit);
        std::fs::remove_file(&pinned).unwrap();
        std::os::unix::fs::symlink(&env, &pinned).unwrap();
        let hardlink = unit.join("build-script-build");
        std::fs::hard_link(&executable, &hardlink).unwrap();
        let environment = |launcher: &Path, stale: Option<&str>| {
            let mut command = std::process::Command::new(launcher);
            match stale {
                Some(value) => command.env(SHIM_PATH_ENV, value),
                None => command.env_remove(SHIM_PATH_ENV),
            };
            let output = command.env(NAME, "kept").output().unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8(output.stdout).unwrap()
        };

        let to_kache = environment(&hardlink, Some("/stale"));
        assert!(
            to_kache.lines().any(|line| line == format!("{NAME}=kept")),
            "{to_kache}"
        );
        assert!(
            to_kache
                .lines()
                .filter(|line| line.starts_with(SHIM_PATH_ENV))
                .eq([format!("{SHIM_PATH_ENV}={}", hardlink.display()).as_str()]),
            "kache learns only the path Cargo invoked: {to_kache}"
        );

        std::fs::remove_file(&pinned).unwrap();
        let to_script = environment(&hardlink, None);
        assert!(
            to_script.lines().any(|line| line == format!("{NAME}=kept")),
            "{to_script}"
        );
        assert!(
            !to_script.contains(SHIM_PATH_ENV),
            "the preserved script runs as Cargo started it: {to_script}"
        );
    }

    /// The preserved script's name and the pinned kache from `.kache-launch`.
    #[cfg(unix)]
    fn launch_record(unit: &Path) -> (String, PathBuf) {
        let record = std::fs::read_to_string(unit.join(LAUNCH_RECORD)).unwrap();
        let fields: Vec<&str> = record.split('\0').collect();
        assert_eq!(fields.len(), 3, "two NUL-terminated fields: {record:?}");
        let profile = unit.ancestors().nth(2).unwrap();
        (fields[0].to_string(), profile.join(fields[1]))
    }
}
