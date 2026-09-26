//! Hermetic build-script runs: one shared `OUT_DIR` per set of inputs.
//!
//! A cached run is only as portable as the paths the script could see. The
//! regular cache looks for machine-local roots in what a script wrote, but a
//! value the script derives from a root (its length, a hash, an escaped
//! spelling) leaves nothing to find. A hermetic run removes the difference
//! instead: the key covers everything the script sees except `OUT_DIR`, and
//! the script runs with `OUT_DIR` in a directory named by that key. Every
//! target directory whose inputs match sees the same path, so whatever the
//! script derives from it is the same too.
//!
//! The directory is `<cache>/out-dirs/v2/<key>/<OUT_DIR below its target
//! directory>`, so the script finds the same layout above `OUT_DIR` that
//! Cargo gives it. The script runs from a copy of its binary in the same
//! sandbox, with `CARGO_TARGET_DIR` pointing at the sandbox and without
//! `PWD`/`OLDPWD`. A run is sealed read-only and shared only when it
//! succeeded, declared exactly the inputs the key was computed from, left
//! them unchanged, wrote nothing outside `OUT_DIR` and made no symlinks.
//! Otherwise the sandbox is removed and the script runs as usual.
//!
//! Cargo's own `OUT_DIR` then becomes a symlink to the sealed directory.
//! Restoring a run is creating that link. Every path that writes into
//! `OUT_DIR` itself first turns the link back into a directory (see
//! [`super::detach_out_dir`]).
//!
//! Registry and Git packages sit under the Cargo home, so their keys agree in
//! every checkout on the machine. A workspace package's manifest directory
//! is in the key, so it shares only between target directories of one
//! checkout.
//!
//! Every link is recorded beside the sandbox, and [`sweep`] removes a run
//! once no recorded `OUT_DIR` links to it and none has for a while. Off
//! unless `KACHE_BUILD_SCRIPT_HERMETIC=1`.

use super::{
    MAX_INPUT_FILES, Prediction, Run, ZERO_AR_DATE_ENV, fold, input_state, input_state_as,
    modified_since, package_exclusions, parse_declarations, real_command, replay, target_dir,
    zero_ar_date,
};
use crate::compiler_store::StoreHashExt as _;
use crate::events::EventResult;
use crate::store::StorePutResult;
use anyhow::{Context, Result};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

const ENABLE_ENV: &str = "KACHE_BUILD_SCRIPT_HERMETIC";
/// Under the cache directory, beside the shared empty `OUT_DIR`s (`v1`).
const ROOT: &str = "out-dirs/v2";
/// In the sandbox: the record of a sealed run. Written last.
const SEALED: &str = ".kache-sealed";
/// In the sandbox: where the script binary runs from.
const BIN: &str = ".kache-bin";
const RECORD_VERSION: u32 = 1;

pub(super) fn enabled() -> bool {
    std::env::var_os(ENABLE_ENV).is_some_and(|value| value == "1" || value == "true")
}

/// What a sealed run printed. Its paths name the shared `OUT_DIR`.
#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Record {
    version: u32,
    stdout: String,
    stderr: String,
}

/// Where one key's run lives.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Sandbox {
    root: PathBuf,
    out_dir: PathBuf,
}

impl Sandbox {
    fn new(cache_dir: &Path, key: &str, below_target: &Path) -> Self {
        let root = cache_dir.join(ROOT).join(&key[..32]);
        let out_dir = root.join(below_target);
        Self { root, out_dir }
    }

    fn lock_path(&self) -> PathBuf {
        self.root.with_extension("lock")
    }

    /// Beside the sandbox: this key's run could not be shared. Its script
    /// then runs as usual without a second, hermetic attempt first.
    fn refused_path(&self) -> PathBuf {
        self.root.with_extension("refused")
    }
}

/// Beside a sandbox: every `OUT_DIR` ever linked to it, one per line. Its
/// modification time is when the run was last linked.
fn referrers_path(sandbox_root: &Path) -> PathBuf {
    sandbox_root.with_extension("refs")
}

fn open_lock(path: &Path) -> Result<std::fs::File> {
    Ok(std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)?)
}

/// Note that `cargo_out_dir` links to `sandbox`, so a sweep keeps it.
fn record_referrer(sandbox: &Sandbox, cargo_out_dir: &Path) -> Result<()> {
    use std::io::Write as _;
    let mut line = cargo_out_dir.as_os_str().as_encoded_bytes().to_vec();
    line.push(b'\n');
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(referrers_path(&sandbox.root))?
        .write_all(&line)?;
    Ok(())
}

/// The recorded `OUT_DIR`s that still link into `sandbox_root`, once each.
fn live_referrers(sandbox_root: &Path) -> Result<Vec<PathBuf>> {
    let recorded = match std::fs::read(referrers_path(sandbox_root)) {
        Ok(recorded) => recorded,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut live: Vec<PathBuf> = Vec::new();
    for line in recorded
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        // SAFETY: written by `record_referrer` from `as_encoded_bytes`.
        let path = PathBuf::from(unsafe { std::ffi::OsStr::from_encoded_bytes_unchecked(line) });
        if std::fs::read_link(&path).is_ok_and(|target| target.starts_with(sandbox_root))
            && !live.contains(&path)
        {
            live.push(path);
        }
    }
    Ok(live)
}

/// What [`sweep`] did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Sweep {
    pub removed: usize,
    pub kept: usize,
}

/// Remove the shared runs no target directory links to any more and none has
/// linked for `keep_unlinked`, unfinished sandboxes a crashed attempt left,
/// and refusals older than `keep_unlinked`. A run a build holds is skipped.
pub(crate) fn sweep(
    cache_dir: &Path,
    now: std::time::SystemTime,
    keep_unlinked: std::time::Duration,
) -> Result<Sweep> {
    let root = cache_dir.join(ROOT);
    let entries = match std::fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Sweep::default()),
        Err(error) => return Err(error.into()),
    };
    let aged = |path: &Path| {
        std::fs::metadata(path)
            .and_then(|meta| meta.modified())
            .is_ok_and(|modified| now.duration_since(modified).unwrap_or_default() >= keep_unlinked)
    };
    let mut sweep = Sweep::default();
    for entry in entries {
        let path = entry?.path();
        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        if name.ends_with(".refused") {
            if aged(&path) {
                std::fs::remove_file(&path)?;
            }
            continue;
        }
        if name.len() != 32 || !path.is_dir() {
            continue;
        }
        let lock = open_lock(&path.with_extension("lock"))?;
        if lock.try_lock().is_err() {
            sweep.kept += 1;
            continue;
        }
        let sealed = path.join(SEALED).is_file();
        if sealed && !live_referrers(&path)?.is_empty() {
            sweep.kept += 1;
            continue;
        }
        let last_linked = if referrers_path(&path).exists() {
            referrers_path(&path)
        } else {
            path.join(SEALED)
        };
        if sealed && !aged(&last_linked) {
            sweep.kept += 1;
            continue;
        }
        remove_sandbox(&path)?;
        let _ = std::fs::remove_file(referrers_path(&path));
        sweep.removed += 1;
    }
    Ok(sweep)
}

/// Whether this key is not to be run hermetically: it was refused before,
/// or a kache that writes another record sealed it and whoever links it
/// still needs it.
fn not_to_attempt(sandbox: &Sandbox) -> bool {
    sandbox.refused_path().exists() || sandbox.root.join(SEALED).exists()
}

/// Run or restore hermetically. `None` when this run cannot be hermetic
/// (a root user, an `OUT_DIR` outside Cargo's layout) or the attempt could
/// not be sealed: the caller then runs the script as usual.
pub(super) fn run(
    run: &Run,
    real: &Path,
    argv: &[OsString],
    prediction: &Prediction,
) -> Result<Option<i32>> {
    if !crate::out_dir_alias::host_allows(cfg!(unix), crate::out_dir_alias::effective_uid()) {
        return Ok(None);
    }
    let cargo_out_dir = &run.environment.out_dir;
    let Some(target) = target_dir(cargo_out_dir) else {
        return Ok(None);
    };
    let below_target = cargo_out_dir.strip_prefix(&target)?.to_path_buf();
    let key_start = std::time::Instant::now();
    let key = key(run, prediction, &below_target)?;
    let key_ms = key_start.elapsed().as_millis() as u64;
    let sandbox = Sandbox::new(&run.config.cache_dir, &key, &below_target);

    // `out-dirs` also holds the shared empty `OUT_DIR`s, which need it private.
    let shared_roots = run.config.cache_dir.join(ROOT);
    crate::out_dir_alias::create_private_dir_all(shared_roots.parent().context("no out-dirs")?)?;
    std::fs::create_dir_all(&shared_roots)?;
    let lock = open_lock(&sandbox.lock_path())?;
    // Shared, so a sweep cannot remove the run between finding and linking it.
    lock.lock_shared()?;
    if let Some(record) = sealed(&sandbox)? {
        restore(run, &sandbox, &record, &key, key_ms)?;
        return Ok(Some(0));
    }
    if not_to_attempt(&sandbox) {
        return Ok(None);
    }
    lock.unlock()?;
    lock.lock()?;
    // Another build may have sealed or refused it while this one waited.
    if let Some(record) = sealed(&sandbox)? {
        restore(run, &sandbox, &record, &key, key_ms)?;
        return Ok(Some(0));
    }
    if not_to_attempt(&sandbox) {
        return Ok(None);
    }
    let record = match attempt(run, real, argv, prediction, &sandbox, &key, &below_target) {
        Ok(Some(record)) => record,
        Ok(None) => {
            remove_sandbox(&sandbox.root)?;
            std::fs::write(sandbox.refused_path(), b"")?;
            return Ok(None);
        }
        Err(error) => {
            let _ = remove_sandbox(&sandbox.root);
            return Err(error);
        }
    };
    seal(&sandbox, &record)?;
    link_out_dir(cargo_out_dir, &sandbox.out_dir)?;
    record_referrer(&sandbox, cargo_out_dir)?;
    drop(lock);
    replay(
        link_search_in(&record.stdout, &sandbox.out_dir, cargo_out_dir).as_bytes(),
        record.stderr.as_bytes(),
    );
    run.log(
        EventResult::Miss,
        &key,
        key_ms,
        0,
        0,
        0,
        StorePutResult::default(),
        0,
    );
    Ok(Some(0))
}

fn restore(run: &Run, sandbox: &Sandbox, record: &Record, key: &str, key_ms: u64) -> Result<()> {
    let restore_start = std::time::Instant::now();
    link_out_dir(&run.environment.out_dir, &sandbox.out_dir)?;
    record_referrer(sandbox, &run.environment.out_dir)?;
    replay(
        link_search_in(&record.stdout, &sandbox.out_dir, &run.environment.out_dir).as_bytes(),
        record.stderr.as_bytes(),
    );
    run.log(
        EventResult::LocalHit,
        key,
        key_ms,
        0,
        restore_start.elapsed().as_millis() as u64,
        0,
        StorePutResult::default(),
        0,
    );
    Ok(())
}

/// The run's key: the regular action key's inputs, as the script sees them.
/// Nothing is normalized: a root the script can see is in the key, except
/// `OUT_DIR`, which a hermetic run replaces with the same path for every key.
/// Where `OUT_DIR` sits below its target directory is in the key, because
/// the sandbox repeats it.
pub(super) fn key(run: &Run, prediction: &Prediction, below_target: &Path) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    fold(&mut hasher, "kind", b"kache-build-script-hermetic-v1");
    fold(
        &mut hasher,
        "key_version",
        kache_format::CACHE_KEY_VERSION.to_string().as_bytes(),
    );
    fold(&mut hasher, "binary", run.binary_hash.as_bytes());
    fold(&mut hasher, "os", std::env::consts::OS.as_bytes());
    fold(&mut hasher, "arch", std::env::consts::ARCH.as_bytes());
    fold(
        &mut hasher,
        "below_target",
        below_target.as_os_str().as_encoded_bytes(),
    );
    let file_hasher = run.store.file_hasher();
    let mut budget = MAX_INPUT_FILES;
    for name in super::cargo_environment_names() {
        fold(&mut hasher, "cargo_env_name", name.as_bytes());
        let value = std::env::var_os(&name);
        match &value {
            Some(value) => fold(&mut hasher, "cargo_env_value", value.as_encoded_bytes()),
            None => fold(&mut hasher, "cargo_env_absent", b""),
        }
        if name.starts_with("DEP_")
            && let Some(path) = value.map(PathBuf::from)
            && path.is_absolute()
            && path.exists()
        {
            let state = input_state_as(&path, &[], &file_hasher, &mut budget, 0, None)?;
            fold(&mut hasher, "cargo_env_path_state", state.as_bytes());
        }
    }
    for name in &prediction.env {
        fold(&mut hasher, "env_name", name.as_bytes());
        match std::env::var_os(name) {
            Some(value) => fold(&mut hasher, "env_value", value.as_encoded_bytes()),
            None => fold(&mut hasher, "env_absent", b""),
        }
    }
    for declared in &prediction.inputs {
        let path = PathBuf::from(run.environment.denormalize_str(declared));
        fold(&mut hasher, "input", path.as_os_str().as_encoded_bytes());
        let excluded = if prediction.default_package {
            package_exclusions(&path, &run.environment)
        } else {
            Vec::new()
        };
        let state = input_state(&path, &excluded, &file_hasher, &mut budget, 0)?;
        fold(&mut hasher, "state", state.as_bytes());
    }
    if cfg!(target_os = "macos")
        && let Some(value) = zero_ar_date(std::env::var_os(ZERO_AR_DATE_ENV), true)
    {
        fold(&mut hasher, "zero_ar_date", value.as_encoded_bytes());
    }
    if let Some(salt) = &run.config.key_salt {
        fold(&mut hasher, "salt", salt.as_bytes());
    }
    Ok(hasher.finalize().to_hex().to_string())
}

/// Whether `path` lies in a sandbox under `out-dirs/v2` that holds a sealed
/// run. Such a directory is read-only and named by its key.
pub(super) fn in_sealed_out_dir(path: &Path) -> bool {
    path.ancestors().any(|sandbox| {
        sandbox
            .parent()
            .is_some_and(|parent| parent.ends_with(ROOT))
            && sandbox.join(SEALED).is_file()
    })
}

/// The sealed run in `sandbox`, if there is one.
fn sealed(sandbox: &Sandbox) -> Result<Option<Record>> {
    let bytes = match std::fs::read(sandbox.root.join(SEALED)) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let record: Record = serde_json::from_slice(&bytes)?;
    Ok((record.version == RECORD_VERSION && sandbox.out_dir.is_dir()).then_some(record))
}

/// Run the script in `sandbox`. `None` when the result must not be shared.
fn attempt(
    run: &Run,
    real: &Path,
    argv: &[OsString],
    prediction: &Prediction,
    sandbox: &Sandbox,
    expected_key: &str,
    below_target: &Path,
) -> Result<Option<Record>> {
    remove_sandbox(&sandbox.root)?;
    std::fs::create_dir_all(&sandbox.out_dir)?;
    let bin = sandbox.root.join(BIN);
    std::fs::create_dir(&bin)?;
    let script = bin.join(real.file_name().context("build script has no name")?);
    std::fs::copy(real, &script)?;

    let started = std::time::SystemTime::now();
    let target = target_dir(&run.environment.out_dir).context("no target directory")?;
    let mut command = real_command(&script, argv);
    command
        .env("OUT_DIR", &sandbox.out_dir)
        .env("CARGO_TARGET_DIR", &sandbox.root)
        .env_remove("PWD")
        .env_remove("OLDPWD");
    for name in LIBRARY_PATHS {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, without_dirs_under(&value, &target));
        }
    }
    let output = command
        .output()
        .with_context(|| format!("running {}", script.display()))?;
    std::fs::remove_dir_all(&bin)?;
    if !output.status.success() {
        tracing::debug!("hermetic build script failed; running it as usual");
        return Ok(None);
    }
    let Ok(stdout) = String::from_utf8(output.stdout) else {
        return Ok(None);
    };
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    // The key was computed before the run, maybe before waiting for the
    // lock; an input edited meanwhile is older than `started`.
    if key(run, prediction, below_target)? != expected_key {
        tracing::debug!("a declared input changed before the hermetic script ran");
        return Ok(None);
    }

    let mut seen = run.environment.clone();
    seen.out_dir = sandbox.out_dir.clone();
    let declared = parse_declarations(&stdout, &seen);
    if declared
        != Some((
            prediction.inputs.clone(),
            prediction.env.clone(),
            prediction.default_package,
        ))
    {
        tracing::debug!("hermetic build script declared other inputs than its key");
        return Ok(None);
    }
    for declared in &prediction.inputs {
        let path = PathBuf::from(run.environment.denormalize_str(declared));
        let excluded = if prediction.default_package {
            package_exclusions(&path, &run.environment)
        } else {
            Vec::new()
        };
        if modified_since(&path, &excluded, started)? {
            tracing::debug!("a declared input changed while the hermetic script ran");
            return Ok(None);
        }
    }
    if let Some(escape) = escape(sandbox)? {
        tracing::debug!(
            "hermetic build script wrote outside OUT_DIR: {}",
            escape.display()
        );
        return Ok(None);
    }
    Ok(Some(Record {
        version: RECORD_VERSION,
        stdout,
        stderr,
    }))
}

/// The variables Cargo puts the target directory's library dirs in for a
/// build script. The sandbox has none of them, and their spelling differs
/// between target directories.
const LIBRARY_PATHS: &[&str] = &[
    "LD_LIBRARY_PATH",
    "DYLD_LIBRARY_PATH",
    "DYLD_FALLBACK_LIBRARY_PATH",
];

/// `value`, a search path, without its entries under `root`.
fn without_dirs_under(value: &std::ffi::OsStr, root: &Path) -> OsString {
    let kept: Vec<PathBuf> = std::env::split_paths(value)
        .filter(|dir| !dir.starts_with(root))
        .collect();
    std::env::join_paths(kept).unwrap_or_default()
}

/// The first thing in the sandbox that is neither `OUT_DIR` nor a directory
/// on the way to it, or a symlink inside `OUT_DIR`.
fn escape(sandbox: &Sandbox) -> Result<Option<PathBuf>> {
    let mut pending = vec![sandbox.root.clone()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory)? {
            let path = entry?.path();
            let kind = std::fs::symlink_metadata(&path)?.file_type();
            if path.starts_with(&sandbox.out_dir) {
                if kind.is_symlink() {
                    return Ok(Some(path));
                }
                if kind.is_dir() {
                    pending.push(path);
                }
                continue;
            }
            if kind.is_dir() && sandbox.out_dir.starts_with(&path) {
                pending.push(path);
                continue;
            }
            return Ok(Some(path));
        }
    }
    Ok(None)
}

/// Make the run read-only, then record it. The record comes last, so a
/// sandbox without one is an unfinished attempt.
fn seal(sandbox: &Sandbox, record: &Record) -> Result<()> {
    read_only(&sandbox.out_dir)?;
    let temporary = sandbox.root.join(format!("{SEALED}.tmp"));
    std::fs::write(&temporary, serde_json::to_vec(record)?)?;
    std::fs::rename(&temporary, sandbox.root.join(SEALED))?;
    Ok(())
}

fn read_only(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let metadata = std::fs::symlink_metadata(path)?;
        if metadata.is_dir() {
            for entry in std::fs::read_dir(path)? {
                read_only(&entry?.path())?;
            }
        }
        let mode = metadata.permissions().mode();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode & !0o222))?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        anyhow::bail!("hermetic build-script runs are Unix only")
    }
}

/// Remove an unfinished sandbox, read-only parts included.
fn remove_sandbox(root: &Path) -> Result<()> {
    if std::fs::symlink_metadata(root).is_err() {
        return Ok(());
    }
    writable(root)?;
    std::fs::remove_dir_all(root)?;
    Ok(())
}

fn writable(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let metadata = std::fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() {
            return Ok(());
        }
        let mode = metadata.permissions().mode();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode | 0o200))?;
        if metadata.is_dir() {
            for entry in std::fs::read_dir(path)? {
                writable(&entry?.path())?;
            }
        }
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

/// Point Cargo's `OUT_DIR` at the sealed one, replacing whatever is there:
/// its own directory, a file, or an earlier link.
fn link_out_dir(cargo_out_dir: &Path, shared: &Path) -> Result<()> {
    if let Ok(metadata) = std::fs::symlink_metadata(cargo_out_dir) {
        if metadata.is_dir() {
            std::fs::remove_dir_all(cargo_out_dir)?;
        } else {
            std::fs::remove_file(cargo_out_dir)?;
        }
    }
    if let Some(parent) = cargo_out_dir.parent() {
        std::fs::create_dir_all(parent)?;
    }
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(shared, cargo_out_dir)?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = shared;
        anyhow::bail!("hermetic build-script runs are Unix only")
    }
}

/// `stdout` with each `rustc-link-search` path under `shared` spelled under
/// `cargo_out_dir` instead. Cargo puts link-search directories inside the
/// target directory on the library path of `cargo run` and `cargo test`, and
/// the link reaches the same files. Other lines keep the shared path, which
/// is the same in every target directory.
fn link_search_in(stdout: &str, shared: &Path, cargo_out_dir: &Path) -> String {
    let shared = shared.to_string_lossy();
    let cargo = cargo_out_dir.to_string_lossy();
    let mut out = String::with_capacity(stdout.len());
    for line in stdout.split_inclusive('\n') {
        let directive = line
            .strip_prefix("cargo::")
            .or_else(|| line.strip_prefix("cargo:"));
        let search = directive.and_then(|directive| directive.strip_prefix("rustc-link-search="));
        match search {
            Some(value) => {
                let prefix_len = line.len() - value.len();
                let (kind, path) = match value.split_once('=') {
                    Some((kind, path)) if !kind.contains('/') => (&value[..=kind.len()], path),
                    _ => ("", value),
                };
                out.push_str(&line[..prefix_len]);
                out.push_str(kind);
                match path.strip_prefix(shared.as_ref()) {
                    Some(rest)
                        if rest.is_empty() || rest.starts_with('/') || rest.starts_with('\n') =>
                    {
                        out.push_str(&cargo);
                        out.push_str(rest);
                    }
                    _ => out.push_str(path),
                }
            }
            None => out.push_str(line),
        }
    }
    out
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn only_link_search_paths_under_the_shared_out_dir_are_respelled() {
        let shared = Path::new("/cache/out-dirs/v2/k/debug/build/z-1/out");
        let cargo = Path::new("/w/target/debug/build/z-1/out");
        let stdout = "cargo:rustc-link-search=native=/cache/out-dirs/v2/k/debug/build/z-1/out/lib\n\
                      cargo::rustc-link-search=/cache/out-dirs/v2/k/debug/build/z-1/out\n\
                      cargo:rustc-link-search=/cache/out-dirs/v2/k/debug/build/z-1/out/a=b\n\
                      cargo:rustc-link-search=native=/usr/lib\n\
                      cargo:rustc-link-search=native=/cache/out-dirs/v2/k/debug/build/z-1/out2\n\
                      cargo:root=/cache/out-dirs/v2/k/debug/build/z-1/out\n\
                      plain line";
        assert_eq!(
            link_search_in(stdout, shared, cargo),
            "cargo:rustc-link-search=native=/w/target/debug/build/z-1/out/lib\n\
             cargo::rustc-link-search=/w/target/debug/build/z-1/out\n\
             cargo:rustc-link-search=/w/target/debug/build/z-1/out/a=b\n\
             cargo:rustc-link-search=native=/usr/lib\n\
             cargo:rustc-link-search=native=/cache/out-dirs/v2/k/debug/build/z-1/out2\n\
             cargo:root=/cache/out-dirs/v2/k/debug/build/z-1/out\n\
             plain line"
        );
    }

    #[test]
    fn a_sandbox_repeats_the_layout_below_the_target_directory() {
        let sandbox = Sandbox::new(
            Path::new("/cache"),
            &"ab".repeat(32),
            Path::new("debug/build/z/0123456789abcdef/out"),
        );
        let root = PathBuf::from(format!("/cache/out-dirs/v2/{}", "ab".repeat(16)));
        assert_eq!(sandbox.root, root);
        assert_eq!(
            sandbox.out_dir,
            root.join("debug/build/z/0123456789abcdef/out")
        );
        assert_eq!(sandbox.lock_path(), root.with_extension("lock"));
    }

    fn sandbox_in(dir: &Path) -> Sandbox {
        let root = dir.join("k");
        let sandbox = Sandbox {
            out_dir: root.join("debug/build/z-1/out"),
            root,
        };
        std::fs::create_dir_all(&sandbox.out_dir).unwrap();
        sandbox
    }

    #[test]
    fn escape_finds_writes_outside_out_dir_and_symlinks_in_it() {
        let dir = tempfile::tempdir().unwrap();
        let sandbox = sandbox_in(dir.path());
        std::fs::create_dir(sandbox.out_dir.join("lib")).unwrap();
        std::fs::write(sandbox.out_dir.join("lib/libz.a"), b"a").unwrap();
        assert_eq!(escape(&sandbox).unwrap(), None);

        let beside = sandbox.root.join("debug/build/z-1/output");
        std::fs::write(&beside, b"x").unwrap();
        assert_eq!(escape(&sandbox).unwrap(), Some(beside.clone()));
        std::fs::remove_file(&beside).unwrap();

        let up = sandbox.root.join("debug/libz.dylib");
        std::fs::write(&up, b"x").unwrap();
        assert_eq!(escape(&sandbox).unwrap(), Some(up.clone()));
        std::fs::remove_file(&up).unwrap();

        let empty = sandbox.root.join("debug/cxxbridge");
        std::fs::create_dir(&empty).unwrap();
        assert_eq!(
            escape(&sandbox).unwrap(),
            Some(empty.clone()),
            "an empty directory beside the way to OUT_DIR"
        );
        std::fs::remove_dir(&empty).unwrap();

        let link = sandbox.out_dir.join("lib/link");
        std::os::unix::fs::symlink("/etc", &link).unwrap();
        assert_eq!(escape(&sandbox).unwrap(), Some(link));
    }

    #[test]
    fn a_sealed_run_is_read_only_and_found_again() {
        let dir = tempfile::tempdir().unwrap();
        let sandbox = sandbox_in(dir.path());
        std::fs::write(sandbox.out_dir.join("gen.rs"), b"pub const N: u8 = 1;").unwrap();
        assert_eq!(sealed(&sandbox).unwrap(), None);
        let record = Record {
            version: RECORD_VERSION,
            stdout: "cargo:rerun-if-changed=build.rs\n".into(),
            stderr: String::new(),
        };
        seal(&sandbox, &record).unwrap();
        assert_eq!(sealed(&sandbox).unwrap(), Some(record));
        assert!(std::fs::write(sandbox.out_dir.join("gen.rs"), b"x").is_err());
        assert!(std::fs::write(sandbox.out_dir.join("new.rs"), b"x").is_err());

        std::fs::write(
            sandbox.root.join(SEALED),
            br#"{"version":99,"stdout":"","stderr":""}"#,
        )
        .unwrap();
        assert_eq!(sealed(&sandbox).unwrap(), None, "another record version");

        remove_sandbox(&sandbox.root).unwrap();
        assert!(!sandbox.root.exists());
        remove_sandbox(&sandbox.root).unwrap();
    }

    #[test]
    fn linking_replaces_a_directory_or_a_link_and_detaching_undoes_it() {
        let dir = tempfile::tempdir().unwrap();
        let shared = dir.path().join("shared");
        std::fs::create_dir(&shared).unwrap();
        std::fs::write(shared.join("gen.rs"), b"shared").unwrap();
        let cargo = dir.path().join("target/debug/build/z-1/out");
        std::fs::create_dir_all(&cargo).unwrap();
        std::fs::write(cargo.join("stale.rs"), b"stale").unwrap();

        link_out_dir(&cargo, &shared).unwrap();
        assert_eq!(std::fs::read_link(&cargo).unwrap(), shared);
        assert_eq!(std::fs::read(cargo.join("gen.rs")).unwrap(), b"shared");
        link_out_dir(&cargo, &shared).unwrap();
        assert_eq!(std::fs::read_link(&cargo).unwrap(), shared);

        super::super::detach_out_dir(&cargo).unwrap();
        assert!(std::fs::symlink_metadata(&cargo).unwrap().is_dir());
        assert!(super::super::directory_is_empty(&cargo));
        assert_eq!(std::fs::read(shared.join("gen.rs")).unwrap(), b"shared");
        super::super::detach_out_dir(&cargo).unwrap();
        assert!(std::fs::symlink_metadata(&cargo).unwrap().is_dir());

        let missing = dir.path().join("target/debug/build/y-1/out");
        link_out_dir(&missing, &shared).unwrap();
        assert_eq!(std::fs::read_link(&missing).unwrap(), shared);

        let file = dir.path().join("target/debug/build/x-1/out");
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, b"not a directory").unwrap();
        link_out_dir(&file, &shared).unwrap();
        assert_eq!(std::fs::read_link(&file).unwrap(), shared);
    }

    #[test]
    fn only_a_sandbox_with_a_sealed_record_counts() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let sandbox = Sandbox::new(&cache, &"cd".repeat(32), Path::new("debug/build/z-1/out"));
        std::fs::create_dir_all(&sandbox.out_dir).unwrap();
        let archive = sandbox.out_dir.join("libz.a");
        std::fs::write(&archive, b"!<arch>\n").unwrap();
        assert!(!in_sealed_out_dir(&archive), "not sealed yet");
        std::fs::write(sandbox.root.join(SEALED), b"{}").unwrap();
        assert!(in_sealed_out_dir(&archive));
        assert!(in_sealed_out_dir(&sandbox.out_dir));

        let elsewhere = dir.path().join("other/k");
        std::fs::create_dir_all(&elsewhere).unwrap();
        std::fs::write(elsewhere.join(SEALED), b"{}").unwrap();
        assert!(
            !in_sealed_out_dir(&elsewhere.join("libz.a")),
            "a record outside out-dirs/v2"
        );
    }

    #[test]
    fn a_refused_or_foreign_sealed_key_is_not_attempted() {
        let dir = tempfile::tempdir().unwrap();
        let sandbox = Sandbox::new(dir.path(), &"ab".repeat(32), Path::new("out"));
        std::fs::create_dir_all(sandbox.root.parent().unwrap()).unwrap();
        assert!(!not_to_attempt(&sandbox));
        std::fs::write(sandbox.refused_path(), b"").unwrap();
        assert!(not_to_attempt(&sandbox), "refused");
        std::fs::remove_file(sandbox.refused_path()).unwrap();
        std::fs::create_dir_all(&sandbox.root).unwrap();
        std::fs::write(sandbox.root.join(SEALED), b"{}").unwrap();
        assert!(not_to_attempt(&sandbox), "sealed by another record version");
    }

    #[test]
    fn an_unreadable_record_is_an_error_not_an_absence() {
        let dir = tempfile::tempdir().unwrap();
        let sandbox = sandbox_in(dir.path());
        std::fs::create_dir(sandbox.root.join(SEALED)).unwrap();
        assert!(sealed(&sandbox).is_err());
    }

    #[test]
    fn library_paths_lose_only_the_target_directory() {
        let value = std::env::join_paths([
            Path::new("/w/target/debug/deps"),
            Path::new("/usr/lib"),
            Path::new("/w/target/debug"),
            Path::new("/w/target2/lib"),
        ])
        .unwrap();
        assert_eq!(
            without_dirs_under(&value, Path::new("/w/target")),
            std::env::join_paths([Path::new("/usr/lib"), Path::new("/w/target2/lib")]).unwrap()
        );
        assert_eq!(
            without_dirs_under(std::ffi::OsStr::new(""), Path::new("/w/target")),
            OsString::new()
        );
    }

    #[test]
    fn a_refusal_and_a_lock_sit_beside_the_sandbox() {
        let sandbox = Sandbox::new(Path::new("/cache"), &"ab".repeat(32), Path::new("out"));
        assert_eq!(
            sandbox.refused_path(),
            PathBuf::from(format!("/cache/out-dirs/v2/{}.refused", "ab".repeat(16)))
        );
        assert_ne!(sandbox.refused_path(), sandbox.lock_path());
    }

    /// A sealed run with the given 64-digit key, linked from `links` Cargo
    /// `OUT_DIR`s.
    fn sealed_run(cache: &Path, key: &str, links: &[PathBuf]) -> Sandbox {
        let sandbox = Sandbox::new(cache, key, Path::new("debug/build/z-1/out"));
        std::fs::create_dir_all(&sandbox.out_dir).unwrap();
        std::fs::write(sandbox.out_dir.join("gen.rs"), b"x").unwrap();
        seal(
            &sandbox,
            &Record {
                version: RECORD_VERSION,
                stdout: String::new(),
                stderr: String::new(),
            },
        )
        .unwrap();
        for link in links {
            link_out_dir(link, &sandbox.out_dir).unwrap();
            record_referrer(&sandbox, link).unwrap();
        }
        sandbox
    }

    #[test]
    fn a_sweep_keeps_linked_and_recent_runs_and_removes_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let week = std::time::Duration::from_secs(7 * 24 * 3600);
        let later = std::time::SystemTime::now() + week + week;
        let target = dir.path().join("target/debug/build");

        let linked = sealed_run(&cache, &"a1".repeat(32), &[target.join("a-1/out")]);
        let unlinked = sealed_run(&cache, &"b2".repeat(32), &[target.join("b-1/out")]);
        std::fs::remove_file(target.join("b-1/out")).unwrap();
        let relinked = sealed_run(&cache, &"c3".repeat(32), &[target.join("c-1/out")]);
        link_out_dir(&target.join("c-1/out"), &unlinked.out_dir).unwrap();
        record_referrer(&unlinked, &target.join("c-1/out")).unwrap();
        let crashed = Sandbox::new(&cache, &"d4".repeat(32), Path::new("out"));
        std::fs::create_dir_all(&crashed.out_dir).unwrap();
        let busy = sealed_run(&cache, &"e5".repeat(32), &[]);
        let held = open_lock(&busy.lock_path()).unwrap();
        held.lock_shared().unwrap();
        let refused = Sandbox::new(&cache, &"f6".repeat(32), Path::new("out"));
        std::fs::write(refused.refused_path(), b"").unwrap();

        let now = sweep(&cache, std::time::SystemTime::now(), week).unwrap();
        assert!(!crashed.root.exists(), "an unfinished sandbox goes at once");
        assert!(
            unlinked.root.exists(),
            "unlinked for less than the retention"
        );
        assert!(refused.refused_path().exists());
        assert_eq!(
            now,
            Sweep {
                removed: 1,
                kept: 4
            },
            "linked, relinked here, recent, and held"
        );

        let later = sweep(&cache, later, week).unwrap();
        assert!(linked.root.exists(), "a live link keeps it");
        assert!(unlinked.root.exists(), "c-1 links here now");
        assert!(
            !relinked.root.exists(),
            "its only link points elsewhere now"
        );
        assert!(busy.root.exists(), "a build holds it");
        assert!(!refused.refused_path().exists());
        assert!(!referrers_path(&relinked.root).exists());
        assert_eq!(
            later,
            Sweep {
                removed: 1,
                kept: 3
            }
        );
        drop(held);

        assert_eq!(
            sweep(
                &dir.path().join("nothing"),
                std::time::SystemTime::now(),
                week
            )
            .unwrap(),
            Sweep::default()
        );
    }

    #[test]
    fn a_sweep_skips_what_is_not_a_sandbox_and_fails_on_an_unreadable_root() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let root = cache.join(ROOT);
        std::fs::create_dir_all(root.join("scratch")).unwrap();
        let file = root.join("ab".repeat(16));
        std::fs::write(&file, b"not a sandbox").unwrap();
        let later = std::time::SystemTime::now() + HERMETIC_WEEK + HERMETIC_WEEK;
        assert_eq!(
            sweep(&cache, later, HERMETIC_WEEK).unwrap(),
            Sweep::default()
        );
        assert!(root.join("scratch").is_dir());
        assert!(file.is_file());

        let blocked = dir.path().join("blocked");
        std::fs::create_dir_all(blocked.join("out-dirs")).unwrap();
        std::fs::write(blocked.join(ROOT), b"a file where a directory goes").unwrap();
        assert!(sweep(&blocked, later, HERMETIC_WEEK).is_err());
    }

    const HERMETIC_WEEK: std::time::Duration = std::time::Duration::from_secs(7 * 24 * 3600);

    #[test]
    fn unlinked_runs_are_kept_for_a_week() {
        assert_eq!(super::super::HERMETIC_UNLINKED_RETENTION, HERMETIC_WEEK);
    }

    #[test]
    fn live_referrers_are_links_into_the_sandbox_once_each() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let target = dir.path().join("target/debug/build");
        let out = target.join("z-1/out");
        let sandbox = sealed_run(&cache, &"ab".repeat(32), &[out.clone(), out.clone()]);
        let stale = target.join("gone/out");
        record_referrer(&sandbox, &stale).unwrap();
        let elsewhere = target.join("y-1/out");
        std::fs::create_dir_all(target.join("y-1")).unwrap();
        std::os::unix::fs::symlink(dir.path(), &elsewhere).unwrap();
        record_referrer(&sandbox, &elsewhere).unwrap();
        assert_eq!(live_referrers(&sandbox.root).unwrap(), vec![out]);
        assert!(live_referrers(&dir.path().join("none")).unwrap().is_empty());
        let unreadable = dir.path().join("unreadable");
        std::fs::create_dir_all(referrers_path(&unreadable)).unwrap();
        assert!(
            live_referrers(&unreadable).is_err(),
            "an unreadable record is an error, not an empty one"
        );
    }

    #[test]
    fn enabled_only_when_asked() {
        let _lock = crate::test_support::process_state_test_lock();
        let saved = std::env::var_os(ENABLE_ENV);
        let mut seen = Vec::new();
        for value in [None, Some("0"), Some("1"), Some("true"), Some("yes")] {
            // SAFETY: the process-state lock serialises environment edits.
            match value {
                Some(value) => unsafe { std::env::set_var(ENABLE_ENV, value) },
                None => unsafe { std::env::remove_var(ENABLE_ENV) },
            }
            seen.push(enabled());
        }
        match saved {
            Some(value) => unsafe { std::env::set_var(ENABLE_ENV, value) },
            None => unsafe { std::env::remove_var(ENABLE_ENV) },
        }
        assert_eq!(seen, [false, false, true, true, false]);
    }
}
