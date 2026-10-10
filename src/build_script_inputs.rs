//! Inputs a package's own build script declared, keyed for each rustc unit
//! of the package.
//!
//! A proc macro can read files and variables rustc never reports. Tauri's
//! `generate_context!` reads `tauri.conf.json` and `TAURI_CONFIG`, and an
//! asset embedder lists a directory. A package that wants Cargo to notice
//! declares them in its build script with `cargo:rerun-if-changed` and
//! `cargo:rerun-if-env-changed`. When one changes, Cargo reruns the script
//! and recompiles every unit of the package, re-exported macros included.
//! A key that left them out would restore the artifacts built from the old
//! inputs.
//!
//! The declarations come from the stdout Cargo recorded for the run, read as
//! Cargo reads it. A path is resolved against the package directory, as
//! Cargo does, and digested by content; a variable by its value, except one
//! Cargo itself sets on the rustc command, which counts by name only. A
//! script that declares nothing, or declares the empty path, depends on its
//! package: its files other than Rust sources (the compile reports the ones
//! it reads), less sub-packages, target directories and VCS metadata. Every
//! walk also leaves out what a build writes while units compile: Kache's own
//! directories and what Cargo itself writes in its home
//! ([`CARGO_HOME_WRITES`]). The rest of the home counts, its configuration
//! among it.
//!
//! Only workspace and path packages are in scope. Cargo treats paths under
//! its home as immutable, and the variables `cc` and `pkg-config` declare
//! (`CC`, `CFLAGS`, `PKG_CONFIG_PATH`) would split a registry unit's key
//! between machines.

use crate::args::RustcArgs;
use crate::build_script::declarations::cargo_declarations;
use crate::build_script::inputs::{TooManyInputs, Walk, state_in};
use crate::cache_key::{FileFingerprint, FileHasher};
use crate::tree_stamp::fold_metadata_stamp;
use anyhow::{Context, Result};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};

/// Names the digest, so a change in what it folds re-keys every unit.
const DIGEST_TAG: &[u8] = b"kache-build-script-inputs-v1";

/// A record of the run larger than this is refused, not read.
const MAX_STDOUT_BYTES: u64 = 16 << 20;

/// Entries one resolve may digest: the declared paths together, and the
/// package on its own.
pub(crate) const MAX_ENTRIES: usize = crate::cache_key::CRATE_TREE_MAX_ENTRIES;

/// The search path Cargo sets on rustc for proc macros' dynamic libraries.
const DYLIB_PATH: &str = if cfg!(windows) {
    "PATH"
} else if cfg!(target_os = "macos") {
    "DYLD_FALLBACK_LIBRARY_PATH"
} else if cfg!(target_os = "aix") {
    "LIBPATH"
} else {
    "LD_LIBRARY_PATH"
};

/// Variables Cargo sets on the rustc command of a unit.
const SET_BY_CARGO: &[&str] = &[
    "CARGO",
    "CARGO_BIN_NAME",
    "CARGO_CRATE_NAME",
    "CARGO_MANIFEST_DIR",
    "CARGO_MANIFEST_PATH",
    "CARGO_PRIMARY_PACKAGE",
    "CARGO_SBOM_PATH",
    "CARGO_TARGET_TMPDIR",
    "OUT_DIR",
    DYLIB_PATH,
];

/// The run of a unit's own build script, and where Cargo recorded it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Located {
    pub(crate) package: String,
    pub(crate) manifest_dir: PathBuf,
    out_dir: PathBuf,
    stdout: PathBuf,
}

/// The build-script run that keys this unit, for a workspace or path package
/// whose `OUT_DIR` is Cargo's for that package and for the profile rustc
/// writes into. `None` for a registry, Git or vendored package, for a nested
/// Cargo build or a probe that inherited another unit's `OUT_DIR`, and while
/// kache aliases `OUT_DIR`, which it does only for registry units.
pub(crate) fn locate(
    args: &RustcArgs,
    var: &dyn Fn(&str) -> Option<OsString>,
    cwd: Option<&Path>,
    alias_active: bool,
) -> Option<Located> {
    if alias_active {
        return None;
    }
    let out_dir = PathBuf::from(var("OUT_DIR")?);
    let manifest_dir = PathBuf::from(var("CARGO_MANIFEST_DIR")?);
    let package = var("CARGO_PKG_NAME")?.into_string().ok()?;
    if !out_dir.is_absolute() || !manifest_dir.is_absolute() {
        return None;
    }
    if crate::cache_key::is_registry_package(&manifest_dir)
        || cargo_home(var).is_some_and(|home| under_cargo_home(&home, &manifest_dir))
        || cwd.is_some_and(|cwd| crate::cache_key::vendored_source(args, &manifest_dir, cwd))
    {
        return None;
    }
    crate::cargo_layout::out_dir_unit_name(&out_dir, &package)?;
    let rustc_out_dir = args.out_dir.as_deref()?;
    if rustc_out_dir.starts_with(&out_dir)
        || crate::cargo_layout::profile_dir(rustc_out_dir)?
            != crate::cargo_layout::out_dir_profile(&out_dir)?
    {
        return None;
    }
    let stdout = crate::cargo_layout::build_script_stdout(&out_dir)?;
    Some(Located {
        package,
        manifest_dir,
        out_dir,
        stdout,
    })
}

/// Cargo's home, found as Cargo finds it.
pub(crate) fn cargo_home(var: &dyn Fn(&str) -> Option<OsString>) -> Option<PathBuf> {
    let set = |name| var(name).filter(|value| !value.is_empty());
    set("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| set(HOME).map(|home| PathBuf::from(home).join(".cargo")))
}

/// Where Cargo's home defaults to, below the user's home.
const HOME: &str = if cfg!(windows) { "USERPROFILE" } else { "HOME" };

/// Whether `path` is under the registry or the Git sources in `home`, which
/// Cargo takes to be immutable.
fn under_cargo_home(home: &Path, path: &Path) -> bool {
    path.starts_with(home.join("registry")) || path.starts_with(home.join("git"))
}

/// What Cargo itself writes in its home: its downloads, its record of when
/// it last used each one (with the files SQLite keeps beside it while it
/// writes), its locks, and what `cargo install` adds. Anything else there is
/// the user's, such as `config.toml`, which is the project's own when the
/// home is the project's `.cargo`.
const CARGO_HOME_WRITES: [&str; 11] = [
    "registry",
    "git",
    ".global-cache",
    ".global-cache-journal",
    ".global-cache-wal",
    ".global-cache-shm",
    ".package-cache",
    ".package-cache-mutate",
    "bin",
    ".crates.toml",
    ".crates2.json",
];

/// [`CARGO_HOME_WRITES`] in `home`, below the home as spelled and as
/// resolved, so an entry Cargo has not written yet is named both ways too.
pub(crate) fn cargo_home_writes(home: &Path) -> Vec<PathBuf> {
    excluded_roots(None, [home])
        .into_iter()
        .flat_map(|home| CARGO_HOME_WRITES.map(|name| home.join(name)))
        .collect()
}

/// What resolving a unit's declared inputs reads besides the unit.
pub(crate) struct Resolver<'a, 'db> {
    /// Hashes declared files. The fingerprints it guards go into the snapshot.
    pub(crate) file_hasher: &'a FileHasher<'db>,
    pub(crate) var: &'a dyn Fn(&str) -> Option<OsString>,
    /// Holds the tree memo and the marker of a package too large to digest.
    pub(crate) cache_dir: &'a Path,
    /// Left out of every tree, in each spelling: the target directory,
    /// Kache's own directories ([`excluded_roots`]) and what Cargo writes in
    /// its home ([`cargo_home_writes`]).
    pub(crate) excluded: &'a [PathBuf],
    /// Spells a path outside the package and `OUT_DIR`, as the key spells a
    /// source there.
    pub(crate) outside: &'a dyn Fn(&Path) -> Result<Vec<u8>>,
    /// Entries one walk may digest (see [`MAX_ENTRIES`]).
    pub(crate) max_entries: usize,
}

/// What a unit's declared inputs come to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Resolved {
    /// The inputs to fold into the key.
    Folded(Snapshot),
    /// Cargo recorded no stdout for the run, as for a script a `links`
    /// override replaces: nothing to fold.
    Unrecorded,
    /// The script declares nothing and its package cannot be digested, for
    /// the reason given: more entries than a walk may take, or an entry the
    /// walk cannot read.
    PackageUnkeyed(String),
}

/// A unit's declared inputs as they were when resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Snapshot {
    /// Folded into the key as `build_script_inputs`.
    pub(crate) digest: String,
    /// The script declared nothing, so its package was digested.
    pub(crate) package_mode: bool,
    /// Distinct paths and variables in the digest, for the trace.
    pub(crate) paths: usize,
    pub(crate) vars: usize,
    /// Cargo's stdout and `root-output` for the run.
    records: Vec<Option<FileFingerprint>>,
    /// Each tree at the top of a walk: its stamp and its own metadata.
    trees: Vec<(PathBuf, String)>,
    /// Every file hashed, as it was then.
    pub(crate) files: Vec<FileFingerprint>,
}

impl Snapshot {
    /// A snapshot that folds `digest` and records nothing to check.
    #[cfg(test)]
    pub(crate) fn with_digest(digest: &str) -> Self {
        Self {
            digest: digest.to_string(),
            package_mode: false,
            paths: 0,
            vars: 0,
            records: Vec::new(),
            trees: Vec::new(),
            files: Vec::new(),
        }
    }

    /// Whether anything `self` was resolved from moved by the time `now`
    /// was: a different digest, a rewritten record, a tree whose entries
    /// changed, or a file rewritten even with the same bytes.
    pub(crate) fn moved_since(&self, now: &Snapshot) -> bool {
        self.digest != now.digest
            || self.records != now.records
            || self.trees != now.trees
            || self.files.iter().any(|file| {
                FileFingerprint::from_path(Path::new(&file.path))
                    .ok()
                    .as_ref()
                    != Some(file)
            })
    }
}

/// A declared variable as the digest folds it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum EnvState {
    Set(Vec<u8>),
    Unset,
    /// Cargo sets it on the rustc command, so the value rustc sees is not
    /// the one Cargo compared before it reran the script.
    SetByCargo,
}

/// Resolve the inputs `located`'s script declared.
///
/// What rustc could not read either counts as such: a path through a file
/// as missing, an entry the user may not search as unreadable, a symlink
/// back into a directory being walked as a cycle. Fails when a declared
/// path cannot be read otherwise, or the declared paths hold more entries
/// than `max_entries`: the unit must not be keyed without them. A package
/// that cannot be digested degrades to no fold instead
/// ([`Resolved::PackageUnkeyed`]); Cargo would rerun its script on any edit,
/// and kache keys it as it did before.
pub(crate) fn resolve(located: &Located, resolver: &Resolver<'_, '_>) -> Result<Resolved> {
    let root_output = located.stdout.with_file_name("root-output");
    let records = vec![fingerprint(&located.stdout), fingerprint(&root_output)];
    let Some(stdout) = read_record(&located.stdout)? else {
        return Ok(Resolved::Unrecorded);
    };
    let recorded_out_dir =
        read_record(&root_output)?.and_then(|bytes| String::from_utf8(bytes).ok());
    let relocated = recorded_out_dir
        .as_deref()
        .zip(located.out_dir.to_str())
        .filter(|(recorded, current)| recorded != current);
    let declared = cargo_declarations(&stdout, relocated);

    let trees = RefCell::new(Vec::new());
    let walk = Walk {
        excluded: resolver.excluded,
        skip_metadata: true,
        other_entries: true,
        unreachable_entries: true,
        stop_when_too_large: true,
        memo: Some((resolver.cache_dir, "build-script-inputs-v1")),
        stamps: Some(&trees),
        ..Walk::default()
    };
    let roots = Roots::of(located);
    let cargo_home = cargo_home(resolver.var);
    let mut paths: BTreeMap<Vec<u8>, String> = BTreeMap::new();
    let mut budget = resolver.max_entries;
    // Cargo watches the package root for an empty path.
    let mut package_mode = declared.paths.is_empty() && declared.env.is_empty();
    for printed in &declared.paths {
        if printed.is_empty() {
            package_mode = true;
            continue;
        }
        let path = located.manifest_dir.join(printed);
        let state = if cargo_home
            .as_deref()
            .is_some_and(|home| under_cargo_home(home, &path))
        {
            "immutable".to_string()
        } else {
            state_in(&path, &walk, resolver.file_hasher, &mut budget)
                .with_context(|| format!("digesting {}", path.display()))?
        };
        paths.insert(roots.spell(&path, resolver.outside)?, state);
    }
    if package_mode {
        match package_state(located, resolver, &walk) {
            Ok(state) => {
                paths.insert(b"package".to_vec(), state);
            }
            Err(error) if error.is::<TooManyInputs>() => {
                return Ok(Resolved::PackageUnkeyed(format!(
                    "the package holds more than {} entries",
                    resolver.max_entries
                )));
            }
            // Cargo's own walk of a package recovers from what it cannot
            // read, so a script that declares nothing does not cost caching.
            Err(error) => {
                return Ok(Resolved::PackageUnkeyed(format!(
                    "kache could not read the package: {error:#}"
                )));
            }
        }
    }
    let vars: BTreeMap<Vec<u8>, EnvState> = declared
        .env
        .iter()
        .map(|name| {
            (
                crate::cache_key::env_name_key_bytes(OsStr::new(name)),
                env_state(name, resolver.var, located),
            )
        })
        .collect();
    let trees = trees
        .into_inner()
        .into_iter()
        .map(|(path, stamp)| {
            let own = own_stamp(&path);
            (path, format!("{stamp}:{own}"))
        })
        .collect();
    Ok(Resolved::Folded(Snapshot {
        digest: digest(&paths, &vars),
        package_mode,
        paths: paths.len(),
        vars: vars.len(),
        records,
        trees,
        files: resolver
            .file_hasher
            .take_guarded_inputs()
            .into_iter()
            .map(|observed| observed.fingerprint)
            .collect(),
    }))
}

/// The package's files as Cargo watches them for a script that declares
/// nothing, less Rust sources and sub-packages. A package found too large
/// within the last hour is not walked again.
fn package_state(
    located: &Located,
    resolver: &Resolver<'_, '_>,
    walk: &Walk<'_>,
) -> Result<String> {
    let marker = oversized_marker(
        resolver.cache_dir,
        &located.manifest_dir,
        resolver.max_entries,
    );
    let recent = std::fs::metadata(&marker)
        .and_then(|metadata| metadata.modified())
        .is_ok_and(|marked| {
            marked
                .elapsed()
                .map_or(true, |age| age < crate::cache_key::OVERSIZED_TREE_TTL)
        });
    if recent {
        return Err(TooManyInputs.into());
    }
    let walk = Walk {
        skip_rust_and_packages: true,
        memo: Some((resolver.cache_dir, "build-script-package-v1")),
        ..*walk
    };
    let mut budget = resolver.max_entries;
    let state = state_in(
        &located.manifest_dir,
        &walk,
        resolver.file_hasher,
        &mut budget,
    );
    if state
        .as_ref()
        .is_err_and(|error| error.is::<TooManyInputs>())
    {
        crate::probe_memo::write_atomic(&marker, "");
    }
    state
}

/// Marks a package found too large for `max_entries`.
fn oversized_marker(cache_dir: &Path, manifest_dir: &Path, max_entries: usize) -> PathBuf {
    let mut hasher = blake3::Hasher::new();
    crate::build_script::fold(&mut hasher, "kind", b"kache-build-script-package-v1");
    crate::build_script::fold(
        &mut hasher,
        "root",
        manifest_dir.as_os_str().as_encoded_bytes(),
    );
    crate::build_script::fold(&mut hasher, "max", &max_entries.to_le_bytes());
    cache_dir
        .join("probes")
        .join("oversized-trees")
        .join(&hasher.finalize().to_hex()[..32])
}

/// Whether `located`'s script declared any input, by Cargo's record of the
/// run alone. A record that cannot be read counts as declaring some.
pub(crate) fn declares_inputs(located: &Located) -> bool {
    match read_record(&located.stdout) {
        Ok(Some(stdout)) => {
            let declared = cargo_declarations(&stdout, None);
            !declared.paths.is_empty() || !declared.env.is_empty()
        }
        Ok(None) => false,
        Err(_) => true,
    }
}

/// A file Cargo wrote for the run, or `None` when there is none.
fn read_record(path: &Path) -> Result<Option<Vec<u8>>> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("reading {}", path.display())),
    };
    anyhow::ensure!(
        metadata.len() <= MAX_STDOUT_BYTES,
        "{} is larger than {MAX_STDOUT_BYTES} bytes",
        path.display()
    );
    std::fs::read(path)
        .map(Some)
        .with_context(|| format!("reading {}", path.display()))
}

fn fingerprint(path: &Path) -> Option<FileFingerprint> {
    FileFingerprint::from_path(path).ok()
}

/// The metadata of `path` itself: a directory's changes when an entry is
/// added or removed, which its tree stamp, made of the entries, can miss.
fn own_stamp(path: &Path) -> String {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            let mut hasher = blake3::Hasher::new();
            fold_metadata_stamp(&mut hasher, &metadata);
            hasher.finalize().to_hex().to_string()
        }
        Err(_) => "missing".to_string(),
    }
}

/// The spellings of `OUT_DIR` and of the package directory that a declared
/// path may start with.
struct Roots {
    out_dir: Vec<PathBuf>,
    package: Vec<PathBuf>,
}

impl Roots {
    fn of(located: &Located) -> Self {
        let mut out_dir = vec![located.out_dir.clone()];
        // A hermetic run links `OUT_DIR` to a shared directory, and its
        // replayed stdout can name that directory.
        out_dir.extend(std::fs::read_link(&located.out_dir).ok());
        out_dir.extend(std::fs::canonicalize(&located.out_dir).ok());
        let mut package = vec![located.manifest_dir.clone()];
        package.extend(std::fs::canonicalize(&located.manifest_dir).ok());
        Self { out_dir, package }
    }

    /// How the digest names `path`: below `OUT_DIR` or the package, by its
    /// place there, so the name is the same in every checkout; elsewhere,
    /// as the key names a source there.
    fn spell(&self, path: &Path, outside: &dyn Fn(&Path) -> Result<Vec<u8>>) -> Result<Vec<u8>> {
        for (prefix, roots) in [(&b"out:"[..], &self.out_dir), (b"pkg:", &self.package)] {
            if let Some(rest) = roots.iter().find_map(|root| path.strip_prefix(root).ok()) {
                let mut spelling = prefix.to_vec();
                spelling.extend(crate::cache_key::env_os_key_bytes(rest.as_os_str()));
                return Ok(spelling);
            }
        }
        let mut spelling = b"path:".to_vec();
        spelling.extend(outside(path)?);
        Ok(spelling)
    }
}

fn env_state(name: &str, var: &dyn Fn(&str) -> Option<OsString>, located: &Located) -> EnvState {
    if set_by_cargo(name) {
        return EnvState::SetByCargo;
    }
    // No process can hold such a name.
    if name.is_empty() || name.contains(['=', '\0']) {
        return EnvState::Unset;
    }
    match var(name) {
        Some(value) if is_script_out_dir(name, &value, located) => EnvState::SetByCargo,
        Some(value) => EnvState::Set(crate::cache_key::env_os_key_bytes(&value)),
        None => EnvState::Unset,
    }
}

/// Whether Cargo sets `name` on the rustc command of every unit of the
/// package, so rustc's value is Cargo's own.
fn set_by_cargo(name: &str) -> bool {
    let name = if cfg!(windows) {
        name.to_ascii_uppercase()
    } else {
        name.to_string()
    };
    SET_BY_CARGO.contains(&name.as_str())
        || name.starts_with("CARGO_PKG_")
        || name.starts_with("CARGO_BIN_EXE_")
}

/// Whether `name` holds the `OUT_DIR` of one of the package's build scripts,
/// as Cargo sets `<script>_OUT_DIR` for a package with several (an unstable
/// feature): another `OUT_DIR` of the package in the same profile. Any other
/// variable named that way is the user's, and counts by its value.
fn is_script_out_dir(name: &str, value: &OsStr, located: &Located) -> bool {
    let suffixed = if cfg!(windows) {
        name.to_ascii_uppercase().ends_with("_OUT_DIR")
    } else {
        name.ends_with("_OUT_DIR")
    };
    let value = Path::new(value);
    suffixed
        && crate::cargo_layout::out_dir_unit_name(value, &located.package).is_some()
        && crate::cargo_layout::out_dir_profile(value)
            == crate::cargo_layout::out_dir_profile(&located.out_dir)
}

fn digest(paths: &BTreeMap<Vec<u8>, String>, vars: &BTreeMap<Vec<u8>, EnvState>) -> String {
    use crate::build_script::fold;
    let mut hasher = blake3::Hasher::new();
    fold(&mut hasher, "kind", DIGEST_TAG);
    for (spelling, state) in paths {
        fold(&mut hasher, "path", spelling);
        fold(&mut hasher, "state", state.as_bytes());
    }
    for (name, state) in vars {
        fold(&mut hasher, "env_name", name);
        match state {
            EnvState::Set(value) => fold(&mut hasher, "env_value", value),
            EnvState::Unset => fold(&mut hasher, "env_unset", b""),
            EnvState::SetByCargo => fold(&mut hasher, "env_set_by_cargo", b""),
        }
    }
    hasher.finalize().to_hex().to_string()
}

/// What every walk leaves out besides VCS metadata and Cargo's tagged
/// directories: the target directory and `dirs`, raw and canonical.
pub(crate) fn excluded_roots<'p>(
    target_dir: Option<PathBuf>,
    dirs: impl IntoIterator<Item = &'p Path>,
) -> Vec<PathBuf> {
    let mut excluded = Vec::new();
    for root in target_dir
        .into_iter()
        .chain(dirs.into_iter().map(Path::to_path_buf))
    {
        if let Ok(canonical) = std::fs::canonicalize(&root)
            && canonical != root
        {
            excluded.push(canonical);
        }
        excluded.push(root);
    }
    excluded
}

#[cfg(test)]
mod tests;
