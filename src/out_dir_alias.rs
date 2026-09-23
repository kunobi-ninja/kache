//! A shared, read-only `OUT_DIR` for registry units whose `OUT_DIR` is empty.
//!
//! Cargo gives every build-script unit its own `OUT_DIR` under the target
//! directory. A crate that bakes that path into what it builds (a lib whose
//! `env!("OUT_DIR")` is a value, a proc macro with a `rustc-env` debug
//! directory under it) builds different bytes in every checkout, and a proc
//! macro that does it re-keys every crate that uses the macro. When the
//! directory is empty, the path is all the compile reads from it.
//!
//! Such a unit compiles with `OUT_DIR` pointing at
//! `<cache>/out-dirs/v1/d/<pkg>-<hash>/out` instead: the same path in every
//! checkout on this machine. Kache creates it empty with mode 0555 and never
//! writes into it, so no process can put anything there and sharing it shares
//! no state.
//!
//! Proc macros are aliased on their first build. A lib is aliased only once
//! this machine has seen every direct consumer of the unit and all of them
//! were proc macros, because only then does the baked path run nowhere but
//! inside rustc. A consumer that shows up later trips the tripwire: kache
//! deletes the aliased lib so Cargo rebuilds it without the alias.
//!
//! Nothing here is collected: `kache gc` and `kache purge` leave `out-dirs/`
//! alone, since an old artifact could otherwise recreate a deleted alias as a
//! writable directory.

use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use crate::args::{ExternDep, RustcArgs};
use crate::compiler::Compiler;
use crate::compiler::rustc::RustcCompiler;
use crate::config::Config;

/// Directory under the cache dir holding aliases, evidence and deny markers.
const ROOT_NAME: &str = "out-dirs";
/// Layout version under the root.
const LAYOUT: &str = "v1";
/// Extension of the file that marks an aliased lib in its `--out-dir`.
const SIDECAR_EXTENSION: &str = "kache-alias";
/// Largest build-script `output` file the directive check reads.
const MAX_BUILD_OUTPUT_BYTES: u64 = 64 * 1024;
/// Build-script directives that neither export nor link anything.
const ALLOWED_DIRECTIVES: &[&str] = &[
    "rerun-if-changed",
    "rerun-if-env-changed",
    "rustc-cfg",
    "rustc-check-cfg",
    "rustc-env",
    "warning",
];

/// How a unit qualifies for the alias.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Tier {
    /// A proc macro, aliased on its first build.
    ProcMacro,
    /// A lib, aliased once every consumer seen on this machine was a proc macro.
    Lib,
}

/// What a compile that links a candidate lib says about that lib.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConsumerKind {
    /// A proc macro, or a proc macro's test harness.
    ProcMacro,
    /// Anything else: a lib, a bin, a build script, a test.
    Other,
}

impl ConsumerKind {
    /// Evidence file name under `libs/<unit id>/`.
    fn marker(self) -> &'static str {
        match self {
            Self::ProcMacro => "pm",
            Self::Other => "other",
        }
    }
}

/// What this machine has seen link a candidate lib.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LibVerdict {
    /// No consumer yet: the first checkout learns.
    Unseen,
    /// Only proc macros.
    MacrosOnly,
    /// At least one consumer that runs the lib's code outside rustc.
    Other,
}

/// The alias directory as the decision finds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AliasState {
    Absent,
    /// An empty real directory owned by this user with no write bits.
    Usable,
    Unusable,
}

/// Why a unit keeps its own `OUT_DIR`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SkipReason {
    Test,
    CrateType,
    Refused,
    NotRegistry,
    OutDirShape,
    OutDirNotEmpty,
    BuildOutput,
    EnvConflict,
    Forced,
    UnsafeRoot,
    Denied,
    AliasUnusable,
    LibEvidence(LibVerdict),
    Sidecar,
}

impl SkipReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Test => "test harness",
            Self::CrateType => "crate type",
            Self::Refused => "not a cacheable compile",
            Self::NotRegistry => "not a registry package",
            Self::OutDirShape => "OUT_DIR is not Cargo's build/<pkg>-<hash>/out",
            Self::OutDirNotEmpty => "OUT_DIR is not empty",
            Self::BuildOutput => "build script output exports or links",
            Self::EnvConflict => "OUT_DIR appears in another value",
            Self::Forced => "path_only_env_vars forces the var",
            Self::UnsafeRoot => "alias root is not private",
            Self::Denied => "denied",
            Self::AliasUnusable => "alias directory is not an empty read-only directory",
            Self::LibEvidence(LibVerdict::Unseen) => "lib evidence unseen",
            Self::LibEvidence(LibVerdict::MacrosOnly) => "lib evidence macros only",
            Self::LibEvidence(LibVerdict::Other) => "lib evidence other",
            Self::Sidecar => "alias marker could not be written",
        }
    }
}

/// Everything [`decide`] checks about one unit, in the order it checks it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct UnitFacts {
    pub crate_types: Vec<String>,
    pub is_test: bool,
    /// The compile has a refuse reason against its real `OUT_DIR`.
    pub refused: bool,
    pub registry: bool,
    /// `<pkg>-<hash>` of a well-shaped `OUT_DIR`.
    pub unit_dir: Option<String>,
    pub out_dir_empty: bool,
    pub directives_ok: bool,
    pub env_ok: bool,
    pub forced: bool,
    pub root_safe: bool,
    pub denied: bool,
}

/// Can this host alias at all: the setting is on, on Unix, and not as root,
/// whose writes the alias's mode bits would not stop.
pub(crate) fn host_allows(enabled: bool, unix: bool, euid: u32) -> bool {
    enabled && unix && euid != 0
}

/// The tier a unit's crate types put it in, if any.
pub(crate) fn unit_tier(crate_types: &[String]) -> Option<Tier> {
    if crate_types == ["proc-macro"] {
        return Some(Tier::ProcMacro);
    }
    let lib = !crate_types.is_empty()
        && crate_types
            .iter()
            .all(|crate_type| crate_type == "lib" || crate_type == "rlib");
    lib.then_some(Tier::Lib)
}

/// Whether the unit compiles with the alias. Any failed rule keeps today's
/// behaviour; the reason is the first rule that failed.
pub(crate) fn decide(
    facts: &UnitFacts,
    verdict: LibVerdict,
    alias: AliasState,
) -> Result<Tier, SkipReason> {
    if facts.is_test {
        return Err(SkipReason::Test);
    }
    let tier = unit_tier(&facts.crate_types).ok_or(SkipReason::CrateType)?;
    let checks = [
        (!facts.refused, SkipReason::Refused),
        (facts.registry, SkipReason::NotRegistry),
        (facts.unit_dir.is_some(), SkipReason::OutDirShape),
        (facts.out_dir_empty, SkipReason::OutDirNotEmpty),
        (facts.directives_ok, SkipReason::BuildOutput),
        (facts.env_ok, SkipReason::EnvConflict),
        (!facts.forced, SkipReason::Forced),
        (facts.root_safe, SkipReason::UnsafeRoot),
        (!facts.denied, SkipReason::Denied),
        (alias != AliasState::Unusable, SkipReason::AliasUnusable),
    ];
    if let Some((_, reason)) = checks.iter().find(|(passed, _)| !passed) {
        return Err(*reason);
    }
    if tier == Tier::Lib && verdict != LibVerdict::MacrosOnly {
        return Err(SkipReason::LibEvidence(verdict));
    }
    Ok(tier)
}

/// A lib that passed every rule but the evidence one is a candidate: once its
/// key shows it bakes `OUT_DIR`, its consumers are worth recording.
pub(crate) fn is_candidate(decision: Result<Tier, SkipReason>) -> bool {
    matches!(decision, Ok(Tier::Lib) | Err(SkipReason::LibEvidence(_)))
}

/// `<pkg>-<hash>` when `out_dir` is Cargo's `.../build/<pkg>-<16 hex>/out`
/// for package `pkg`.
pub(crate) fn unit_dir_name(out_dir: &Path, pkg: &str) -> Option<String> {
    if !out_dir.is_absolute() || out_dir.file_name()? != "out" {
        return None;
    }
    let unit = out_dir.parent()?;
    if unit.parent()?.file_name()? != "build" {
        return None;
    }
    let name = unit.file_name()?.to_str()?;
    let hash = name.strip_prefix(pkg)?.strip_prefix('-')?;
    let hex = hash.len() == 16 && hash.bytes().all(|byte| byte.is_ascii_hexdigit());
    hex.then(|| name.to_string())
}

/// Whether every `cargo:` / `cargo::` line of a build script's output is one
/// that neither exports to dependents nor links. Plain lines are ignored.
pub(crate) fn directives_allow_alias(output: &str) -> bool {
    output.lines().all(|line| {
        let Some(rest) = line.strip_prefix("cargo:") else {
            return true;
        };
        let directive = rest.strip_prefix(':').unwrap_or(rest);
        let name = directive
            .split_once('=')
            .map_or(directive, |(name, _)| name);
        ALLOWED_DIRECTIVES.contains(&name)
    })
}

/// `<root>/v1/d/<unit dir>/out`.
pub(crate) fn alias_dir(root: &Path, unit_dir: &str) -> PathBuf {
    root.join(LAYOUT).join("d").join(unit_dir).join("out")
}

/// The alias spelling of `value` when it is `real` or under it, by path
/// components, in any of `real`'s spellings.
fn rewritten(value: &OsStr, real: &[&Path], alias: &Path) -> Option<OsString> {
    let relative = real
        .iter()
        .find_map(|spelling| Path::new(value).strip_prefix(spelling).ok())?;
    let target = if relative.as_os_str().is_empty() {
        alias.to_path_buf()
    } else {
        alias.join(relative)
    };
    Some(target.into_os_string())
}

/// Does `bytes` contain any spelling of `real`?
fn mentions(bytes: &[u8], real: &[&Path]) -> bool {
    real.iter().any(|spelling| {
        let needle = spelling.as_os_str().as_encoded_bytes();
        bytes.windows(needle.len()).any(|window| window == needle)
    })
}

/// The env vars to point at `alias`: every value equal to `real` or under it.
/// `None` when any other env value or argv entry contains `real`'s text, since
/// the compile could then see the real path some other way.
pub(crate) fn env_rewrites(
    env: &[(OsString, OsString)],
    argv: &[String],
    real: &[&Path],
    alias: &Path,
) -> Option<Vec<(OsString, OsString)>> {
    let mut rewrites = Vec::new();
    for (name, value) in env {
        match rewritten(value, real, alias) {
            Some(target) => rewrites.push((name.clone(), target)),
            None if mentions(value.as_encoded_bytes(), real) => return None,
            None => {}
        }
    }
    if argv.iter().any(|arg| mentions(arg.as_bytes(), real)) {
        return None;
    }
    Some(rewrites)
}

/// Whether a `crate:VAR` entry in `path_only_env_vars` names this crate's
/// `OUT_DIR` or one of the vars the alias would rewrite.
pub(crate) fn forced_by_user(
    entries: &[String],
    crate_name: &str,
    rewrites: &[(OsString, OsString)],
) -> bool {
    entries.iter().any(|entry| {
        entry.split_once(':').is_some_and(|(krate, var)| {
            krate == crate_name
                && (var == "OUT_DIR" || rewrites.iter().any(|(name, _)| name == var))
        })
    })
}

/// Whether the alias root sits outside every directory of this checkout.
pub(crate) fn root_location_ok(root: &Path, checkout_dirs: &[PathBuf]) -> bool {
    !checkout_dirs.iter().any(|dir| root.starts_with(dir))
}

/// Does this compile link the `proc_macro` crate the way Cargo passes it to a
/// proc macro and its test harness: a bare `--extern proc_macro`?
pub(crate) fn links_proc_macro(externs: &[ExternDep]) -> bool {
    externs
        .iter()
        .any(|dep| dep.name == "proc_macro" && dep.path.is_none())
}

/// What linking a candidate lib from this compile says about the lib.
pub(crate) fn consumer_kind(
    crate_types: &[String],
    is_test: bool,
    bare_proc_macro: bool,
) -> ConsumerKind {
    let proc_macro = crate_types
        .iter()
        .any(|crate_type| crate_type == "proc-macro")
        || (is_test && bare_proc_macro);
    if proc_macro {
        ConsumerKind::ProcMacro
    } else {
        ConsumerKind::Other
    }
}

/// The verdict from which evidence files exist for a lib.
pub(crate) fn lib_verdict(pm: bool, other: bool) -> LibVerdict {
    if other {
        LibVerdict::Other
    } else if pm {
        LibVerdict::MacrosOnly
    } else {
        LibVerdict::Unseen
    }
}

/// Did a compile fail because something could not write?
pub(crate) fn is_permission_failure(stderr: &str) -> bool {
    stderr.contains("Permission denied") || stderr.contains("os error 13")
}

/// Whether a finished compile may be stored: its own alias (if any) is still
/// empty and read-only, and its dep-info lists nothing under the root.
pub(crate) fn store_allowed(alias_clean: bool, lists_root_file: bool) -> bool {
    alias_clean && !lists_root_file
}

/// The `<pkg>-<hash>` of the alias `path` sits in, if it is under one.
fn alias_unit_of<'a>(path: &'a Path, root: &Path) -> Option<&'a str> {
    let relative = path.strip_prefix(root.join(LAYOUT).join("d")).ok()?;
    relative.components().next()?.as_os_str().to_str()
}

/// The package whose alias `stderr` names, for the hint.
fn named_package<'a>(stderr: &'a str, root: &Path) -> Option<&'a str> {
    let prefix = format!("{}/", root.join(LAYOUT).join("d").display());
    let start = stderr.find(&prefix)? + prefix.len();
    let unit = stderr[start..].split('/').next()?;
    unit.rsplit_once('-').map(|(package, _)| package)
}

/// The one-line hint for a compile that failed to write, when an alias root
/// exists on this machine.
pub(crate) fn permission_hint(stderr: &str, root: &Path, root_exists: bool) -> Option<String> {
    (root_exists && is_permission_failure(stderr)).then(|| {
        let package = named_package(stderr, root).unwrap_or("<macro crate>");
        format!(
            "[kache] shared OUT_DIRs are read-only; to let a macro write debug output, run \
             `cargo clean -p {package}` and rebuild with KACHE_OUT_DIR_ALIAS=0"
        )
    })
}

/// `<out-dir>/lib<crate><extra>.kache-alias` for a lib compile.
fn sidecar_path(args: &RustcArgs) -> Option<PathBuf> {
    let stem = args.output_stem()?;
    Some(
        args.out_dir
            .as_ref()?
            .join(format!("lib{stem}.{SIDECAR_EXTENSION}")),
    )
}

/// The rlib, rmeta and sidecar next to an `--extern` artifact.
fn extern_files(path: &Path) -> Option<[PathBuf; 3]> {
    let stem = path.file_stem()?.to_str()?;
    let dir = path.parent()?;
    Some(["rlib", "rmeta", SIDECAR_EXTENSION].map(|ext| dir.join(format!("{stem}.{ext}"))))
}

fn libs_dir(root: &Path) -> PathBuf {
    root.join(LAYOUT).join("libs")
}

fn deny_path(root: &Path, unit_dir: &str) -> PathBuf {
    root.join(LAYOUT).join("deny").join(unit_dir)
}

/// This process's effective user id.
pub(crate) fn effective_uid() -> u32 {
    #[cfg(unix)]
    {
        // SAFETY: geteuid has no arguments, pointers, or preconditions.
        unsafe { libc::geteuid() }
    }
    #[cfg(not(unix))]
    {
        0
    }
}

/// A directory builder that creates with `mode` on Unix.
fn dir_builder(mode: u32, recursive: bool) -> std::fs::DirBuilder {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(recursive);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, mode);
    #[cfg(not(unix))]
    let _ = mode;
    builder
}

/// Create `path` and its missing parents, private to this user.
fn create_private_dir_all(path: &Path) -> std::io::Result<()> {
    dir_builder(0o700, true).create(path)
}

/// Is `path` a real directory owned by `euid` that no one else can write?
fn is_private_dir(path: &Path, euid: u32) -> bool {
    let Ok(meta) = std::fs::symlink_metadata(path) else {
        return false;
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        meta.is_dir() && meta.uid() == euid && meta.mode() & 0o022 == 0
    }
    #[cfg(not(unix))]
    {
        let _ = (meta, euid);
        false
    }
}

/// `<canonical cache_dir>/out-dirs`, creating the cache dir if needed.
fn alias_root(cache_dir: &Path) -> Option<PathBuf> {
    std::fs::create_dir_all(cache_dir).ok()?;
    Some(std::fs::canonicalize(cache_dir).ok()?.join(ROOT_NAME))
}

/// Rule 6: the root exists (created 0700 if missing), is private to this user,
/// and is outside the checkout.
fn root_is_safe(root: &Path, args: &RustcArgs, euid: u32) -> bool {
    let checkout_dirs: Vec<PathBuf> = args
        .path_normalization_root()
        .map(Path::to_path_buf)
        .into_iter()
        .chain(args.target_dir())
        .map(|dir| std::fs::canonicalize(&dir).unwrap_or(dir))
        .collect();
    let _ = create_private_dir_all(root);
    is_private_dir(root, euid) && root_location_ok(root, &checkout_dirs)
}

/// Does `dir` exist with no entries?
fn dir_has_no_entries(dir: &Path) -> bool {
    std::fs::read_dir(dir).is_ok_and(|mut entries| entries.next().is_none())
}

/// Rule 3 against `<build dir>/output`.
fn build_output_allows_alias(build_dir: &Path) -> bool {
    let path = build_dir.join("output");
    let small = std::fs::metadata(&path).is_ok_and(|meta| meta.len() <= MAX_BUILD_OUTPUT_BYTES);
    small
        && std::fs::read(&path)
            .is_ok_and(|bytes| directives_allow_alias(&String::from_utf8_lossy(&bytes)))
}

/// Rule 8, read from the filesystem.
pub(crate) fn alias_state(dir: &Path, euid: u32) -> AliasState {
    let meta = match std::fs::symlink_metadata(dir) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return AliasState::Absent,
        Err(_) => return AliasState::Unusable,
    };
    #[cfg(unix)]
    let usable = {
        use std::os::unix::fs::MetadataExt;
        meta.is_dir() && meta.uid() == euid && meta.mode() & 0o222 == 0 && dir_has_no_entries(dir)
    };
    #[cfg(not(unix))]
    let usable = {
        let _ = (meta, euid);
        false
    };
    if usable {
        AliasState::Usable
    } else {
        AliasState::Unusable
    }
}

/// Create the alias read-only, or accept one that is already usable (another
/// checkout made it). Kache never writes into it.
pub(crate) fn ensure_alias_dir(dir: &Path, euid: u32) -> bool {
    let Some(parent) = dir.parent() else {
        return false;
    };
    if create_private_dir_all(parent).is_err() {
        return false;
    }
    dir_builder(0o555, false).create(dir).is_ok() || alias_state(dir, euid) == AliasState::Usable
}

/// The unit ids registered as candidate libs.
fn listed_candidates(root: &Path) -> BTreeSet<String> {
    std::fs::read_dir(libs_dir(root))
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
        .collect()
}

/// The candidate unit id an `--extern` links, if it is listed.
fn listed_extern<'a>(dep: &'a ExternDep, listed: &BTreeSet<String>) -> Option<(&'a Path, String)> {
    let path = dep.path.as_deref()?;
    let id = crate::args::unit_id_from_artifact_path(path)?;
    listed.contains(&id).then_some((path, id))
}

/// Record this compile as a consumer of every listed lib it links.
pub(crate) fn record_consumers(
    root: &Path,
    listed: &BTreeSet<String>,
    externs: &[ExternDep],
    kind: ConsumerKind,
) {
    for (_, id) in externs.iter().filter_map(|dep| listed_extern(dep, listed)) {
        let _ = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(libs_dir(root).join(id).join(kind.marker()));
    }
}

/// Remove every aliased lib an `other` consumer links, so Cargo rebuilds it
/// unaliased. Returns the extern names it removed.
pub(crate) fn trip(
    listed: &BTreeSet<String>,
    externs: &[ExternDep],
    kind: ConsumerKind,
) -> Vec<String> {
    if kind == ConsumerKind::ProcMacro {
        return Vec::new();
    }
    let mut tripped = Vec::new();
    for dep in externs {
        let Some([rlib, rmeta, sidecar]) = listed_extern(dep, listed)
            .and_then(|(path, _)| extern_files(path))
            .filter(|[_, _, sidecar]| sidecar.exists())
        else {
            continue;
        };
        for file in [rlib, rmeta, sidecar] {
            let _ = std::fs::remove_file(file);
        }
        tripped.push(dep.name.clone());
    }
    tripped
}

/// Evidence and the tripwire, for every rustc run. `Some(exit code)` when the
/// compile must not run.
pub(crate) fn consumer_checks(
    root: &Path,
    externs: &[ExternDep],
    kind: ConsumerKind,
    stderr: &mut dyn Write,
) -> Option<i32> {
    let listed = listed_candidates(root);
    record_consumers(root, &listed, externs, kind);
    let tripped = trip(&listed, externs, kind);
    if tripped.is_empty() {
        return None;
    }
    let _ = writeln!(
        stderr,
        "[kache] {} was built with a shared read-only OUT_DIR that only proc macros may use; \
         removed it so Cargo rebuilds it. Run the build again.",
        tripped.join(", ")
    );
    Some(1)
}

/// Mark `<root>/v1/libs/<unit id>/` so consumers record evidence for it.
pub(crate) fn register_candidate(root: &Path, unit_id: &str) -> std::io::Result<()> {
    create_private_dir_all(&libs_dir(root).join(unit_id))
}

/// Keep the unit off the alias from now on.
pub(crate) fn deny(root: &Path, unit_dir: &str) -> std::io::Result<()> {
    let path = deny_path(root, unit_dir);
    create_private_dir_all(path.parent().unwrap_or(root))?;
    std::fs::write(path, b"")
}

/// Write the sidecar for an aliased lib, or remove a stale one.
pub(crate) fn write_sidecar(path: &Path, aliased: bool) -> std::io::Result<()> {
    if aliased {
        return std::fs::write(path, b"");
    }
    match std::fs::remove_file(path) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

/// Which consumers of a lib this machine has seen.
fn read_verdict(root: &Path, unit_id: &str) -> LibVerdict {
    let dir = libs_dir(root).join(unit_id);
    lib_verdict(
        dir.join(ConsumerKind::ProcMacro.marker()).exists(),
        dir.join(ConsumerKind::Other.marker()).exists(),
    )
}

/// What [`UnitFacts::gather`] found beyond the facts [`decide`] reads.
#[derive(Debug)]
pub(crate) struct Detected {
    pub facts: UnitFacts,
    pub verdict: LibVerdict,
    pub alias: AliasState,
    pub rewrites: Vec<(OsString, OsString)>,
}

impl UnitFacts {
    /// Read the facts for one compile from its args, its environment and the
    /// filesystem. The filesystem is only read for a registry unit with a
    /// well-shaped `OUT_DIR`.
    pub(crate) fn gather(
        args: &RustcArgs,
        env: &[(OsString, OsString)],
        refused: bool,
        path_only_env_vars: &[String],
        root: &Path,
        euid: u32,
    ) -> Detected {
        let var = |name: &str| {
            env.iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.as_os_str())
        };
        let registry = var("CARGO_MANIFEST_DIR")
            .is_some_and(|dir| crate::cache_key::is_registry_package(Path::new(dir)));
        let out_dir = var("OUT_DIR").map(Path::new);
        let unit_dir = out_dir
            .zip(var("CARGO_PKG_NAME").and_then(OsStr::to_str))
            .and_then(|(dir, pkg)| unit_dir_name(dir, pkg));
        // Past this point the facts read the filesystem.
        let checked = unit_dir.as_deref().zip(out_dir).filter(|_| registry);
        let canonical = checked.and_then(|(_, dir)| std::fs::canonicalize(dir).ok());
        let rewrites = checked.and_then(|(name, dir)| {
            let spellings: Vec<&Path> = std::iter::once(dir).chain(canonical.as_deref()).collect();
            env_rewrites(env, &args.all_args, &spellings, &alias_dir(root, name))
        });
        let facts = UnitFacts {
            crate_types: args.crate_types.clone(),
            is_test: args.is_test,
            refused,
            registry,
            out_dir_empty: checked.is_some_and(|(_, dir)| dir_has_no_entries(dir)),
            directives_ok: checked
                .and_then(|(_, dir)| dir.parent())
                .is_some_and(build_output_allows_alias),
            env_ok: rewrites.is_some(),
            forced: rewrites.as_deref().is_some_and(|rewrites| {
                forced_by_user(
                    path_only_env_vars,
                    args.crate_name.as_deref().unwrap_or(""),
                    rewrites,
                )
            }),
            root_safe: checked.is_some_and(|_| root_is_safe(root, args, euid)),
            denied: checked.is_some_and(|(name, _)| deny_path(root, name).exists()),
            unit_dir: unit_dir.clone(),
        };
        Detected {
            verdict: checked
                .and(args.unit_id())
                .map_or(LibVerdict::Unseen, |id| read_verdict(root, &id)),
            alias: checked.map_or(AliasState::Absent, |(name, _)| {
                alias_state(&alias_dir(root, name), euid)
            }),
            rewrites: rewrites.unwrap_or_default(),
            facts,
        }
    }
}

/// The alias this process compiles with, decided once at wrapper start.
#[derive(Debug)]
struct Plan {
    root: PathBuf,
    /// The alias directory and its `<pkg>-<hash>`, when this unit is aliased.
    alias: Option<(PathBuf, String)>,
    /// The unit id to register once the key shows the lib bakes `OUT_DIR`.
    candidate: Option<String>,
}

static PLAN: OnceLock<Plan> = OnceLock::new();

/// Record evidence, run the tripwire, decide, and point `OUT_DIR` and every
/// value under it at the alias. Must run while the wrapper is still single
/// threaded: it changes the process environment. `Some(exit code)` means the
/// compile must not run.
pub(crate) fn apply(config: &Config, wrapper_args: &[String]) -> Option<i32> {
    let _trace = crate::phase_trace::phase("out_dir_alias");
    let euid = effective_uid();
    if !host_allows(config.out_dir_alias, cfg!(unix), euid) {
        return None;
    }
    let compiler = RustcCompiler::new().with_base_dirs(config.base_dirs.clone());
    let args = compiler.parse(wrapper_args).ok()?;
    let root = alias_root(&config.cache_dir)?;
    let kind = consumer_kind(
        &args.crate_types,
        args.is_test,
        links_proc_macro(&args.externs),
    );
    if let Some(code) = consumer_checks(&root, &args.externs, kind, &mut std::io::stderr()) {
        return Some(code);
    }

    let env: Vec<(OsString, OsString)> = std::env::vars_os().collect();
    let refused = !compiler.refuse_reasons(&args).is_empty();
    let detected = UnitFacts::gather(
        &args,
        &env,
        refused,
        &config.path_only_env_vars,
        &root,
        euid,
    );
    let decision = decide(&detected.facts, detected.verdict, detected.alias);
    let unit_dir = detected.facts.unit_dir.clone().unwrap_or_default();
    let sidecar = sidecar_path(&args);
    let aliased = decision.and_then(|tier| {
        let dir = alias_dir(&root, &unit_dir);
        if !ensure_alias_dir(&dir, euid) {
            return Err(SkipReason::AliasUnusable);
        }
        // Pipelined consumers must see the marker before the rmeta exists.
        let marked = sidecar
            .as_deref()
            .map_or(Ok(()), |path| write_sidecar(path, tier == Tier::Lib));
        marked.map(|()| dir).map_err(|_| SkipReason::Sidecar)
    });
    let crate_name = args.crate_name.as_deref().unwrap_or("unknown");
    match &aliased {
        Ok(dir) => {
            tracing::trace!(
                target: "kache::cache_key",
                "[key:{crate_name}] out_dir_alias: aliased to {}",
                dir.display()
            );
            for (name, value) in &detected.rewrites {
                // SAFETY: the wrapper has spawned no thread yet (see the caller).
                unsafe { std::env::set_var(name, value) };
            }
        }
        Err(reason) => {
            tracing::trace!(
                target: "kache::cache_key",
                "[key:{crate_name}] out_dir_alias: skip: {}",
                reason.as_str()
            );
            if let Some(path) = &sidecar {
                let _ = write_sidecar(path, false);
            }
        }
    }
    let _ = PLAN.set(Plan {
        candidate: args.unit_id().filter(|_| is_candidate(decision)),
        alias: aliased.ok().map(|dir| (dir, unit_dir)),
        root,
    });
    None
}

/// The alias this process's `OUT_DIR` points at, if any.
pub(crate) fn active_alias() -> Option<&'static Path> {
    PLAN.get()?.alias.as_ref().map(|(dir, _)| dir.as_path())
}

/// After the key: register a candidate lib whose key keeps an `OUT_DIR`
/// value, so later compiles record who links it.
pub(crate) fn register_after_key(bakes_out_dir: bool) {
    let Some(plan) = PLAN.get() else {
        return;
    };
    if let Some(unit_id) = plan.candidate.as_deref().filter(|_| bakes_out_dir) {
        let _ = register_candidate(&plan.root, unit_id);
    }
}

/// The store gate. Skips the store, and denies the units involved, when this
/// unit's alias is no longer empty and read-only or its dep-info lists a file
/// under the root.
pub(crate) fn store_gate(args: &RustcArgs) -> bool {
    let Some(plan) = PLAN.get() else {
        return true;
    };
    let dirty = plan
        .alias
        .as_ref()
        .filter(|(dir, _)| alias_state(dir, effective_uid()) != AliasState::Usable)
        .map(|(_, name)| name.as_str());
    let sources = args
        .dep_info_path()
        .zip(args.source_file.as_deref())
        .and_then(|(path, source)| crate::cache_key::dep_info_from_emitted(&path, source).ok())
        .map(|dep_info| dep_info.source_files)
        .unwrap_or_default();
    let under_root: Vec<&PathBuf> = sources
        .iter()
        .filter(|source| source.starts_with(&plan.root))
        .collect();
    if store_allowed(dirty.is_none(), !under_root.is_empty()) {
        return true;
    }
    let listed = under_root
        .iter()
        .filter_map(|source| alias_unit_of(source, &plan.root));
    for unit_dir in dirty.into_iter().chain(listed) {
        tracing::warn!(
            "not caching: shared OUT_DIR {unit_dir} changed; it will not be shared again"
        );
        let _ = deny(&plan.root, unit_dir);
    }
    false
}

/// After a failed compile: the hint, and a deny marker for this unit's alias.
pub(crate) fn after_failed_compile(stderr: &str) {
    let Some(plan) = PLAN.get() else {
        return;
    };
    let Some(hint) = permission_hint(stderr, &plan.root, plan.root.exists()) else {
        return;
    };
    eprintln!("{hint}");
    if let Some((_, unit_dir)) = &plan.alias {
        let _ = deny(&plan.root, unit_dir);
    }
}

// The alias only exists on Unix, and these tests spell Unix paths.
#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    fn passing_facts(crate_types: &[&str]) -> UnitFacts {
        UnitFacts {
            crate_types: strings(crate_types),
            is_test: false,
            refused: false,
            registry: true,
            unit_dir: Some("dmac-0123456789abcdef".into()),
            out_dir_empty: true,
            directives_ok: true,
            env_ok: true,
            forced: false,
            root_safe: true,
            denied: false,
        }
    }

    fn os(pairs: &[(&str, &str)]) -> Vec<(OsString, OsString)> {
        pairs
            .iter()
            .map(|(name, value)| (OsString::from(name), OsString::from(value)))
            .collect()
    }

    fn dep(name: &str, path: Option<&Path>) -> ExternDep {
        ExternDep {
            name: name.into(),
            path: path.map(Path::to_path_buf),
        }
    }

    #[test]
    fn host_allows_only_an_enabled_unix_non_root_host() {
        assert!(host_allows(true, true, 501));
        assert!(!host_allows(false, true, 501));
        assert!(!host_allows(true, false, 501));
        assert!(!host_allows(true, true, 0));
    }

    #[test]
    fn unit_tier_follows_the_crate_types() {
        assert_eq!(unit_tier(&strings(&["proc-macro"])), Some(Tier::ProcMacro));
        assert_eq!(unit_tier(&strings(&["lib"])), Some(Tier::Lib));
        assert_eq!(unit_tier(&strings(&["rlib"])), Some(Tier::Lib));
        assert_eq!(unit_tier(&strings(&["lib", "rlib"])), Some(Tier::Lib));
        for crate_types in [
            &[][..],
            &["bin"],
            &["dylib"],
            &["cdylib"],
            &["staticlib"],
            &["lib", "cdylib"],
            &["proc-macro", "lib"],
        ] {
            assert_eq!(unit_tier(&strings(crate_types)), None, "{crate_types:?}");
        }
    }

    #[test]
    fn decide_aliases_a_proc_macro_that_passes_every_rule() {
        for verdict in [
            LibVerdict::Unseen,
            LibVerdict::MacrosOnly,
            LibVerdict::Other,
        ] {
            for alias in [AliasState::Absent, AliasState::Usable] {
                assert_eq!(
                    decide(&passing_facts(&["proc-macro"]), verdict, alias),
                    Ok(Tier::ProcMacro)
                );
            }
        }
    }

    #[test]
    fn decide_aliases_a_lib_only_on_macro_only_evidence() {
        let facts = passing_facts(&["lib"]);
        assert_eq!(
            decide(&facts, LibVerdict::MacrosOnly, AliasState::Absent),
            Ok(Tier::Lib)
        );
        assert_eq!(
            decide(&facts, LibVerdict::MacrosOnly, AliasState::Usable),
            Ok(Tier::Lib)
        );
        for verdict in [LibVerdict::Unseen, LibVerdict::Other] {
            assert_eq!(
                decide(&facts, verdict, AliasState::Absent),
                Err(SkipReason::LibEvidence(verdict))
            );
        }
    }

    #[test]
    fn decide_fails_each_rule_alone() {
        type Flip = fn(&mut UnitFacts);
        let flips: [(Flip, SkipReason); 12] = [
            (|f| f.is_test = true, SkipReason::Test),
            (|f| f.crate_types = strings(&["bin"]), SkipReason::CrateType),
            (|f| f.refused = true, SkipReason::Refused),
            (|f| f.registry = false, SkipReason::NotRegistry),
            (|f| f.unit_dir = None, SkipReason::OutDirShape),
            (|f| f.out_dir_empty = false, SkipReason::OutDirNotEmpty),
            (|f| f.directives_ok = false, SkipReason::BuildOutput),
            (|f| f.env_ok = false, SkipReason::EnvConflict),
            (|f| f.forced = true, SkipReason::Forced),
            (|f| f.root_safe = false, SkipReason::UnsafeRoot),
            (|f| f.denied = true, SkipReason::Denied),
            (
                |f| f.crate_types = strings(&["dylib"]),
                SkipReason::CrateType,
            ),
        ];
        for tier in [&["proc-macro"][..], &["lib"]] {
            for (flip, reason) in flips {
                let mut facts = passing_facts(tier);
                flip(&mut facts);
                assert_eq!(
                    decide(&facts, LibVerdict::MacrosOnly, AliasState::Absent),
                    Err(reason),
                    "{tier:?}"
                );
            }
            assert_eq!(
                decide(
                    &passing_facts(tier),
                    LibVerdict::MacrosOnly,
                    AliasState::Unusable
                ),
                Err(SkipReason::AliasUnusable)
            );
        }
    }

    #[test]
    fn candidates_are_libs_past_every_rule_but_evidence() {
        assert!(is_candidate(Ok(Tier::Lib)));
        assert!(is_candidate(Err(SkipReason::LibEvidence(
            LibVerdict::Unseen
        ))));
        assert!(is_candidate(Err(SkipReason::LibEvidence(
            LibVerdict::Other
        ))));
        assert!(!is_candidate(Ok(Tier::ProcMacro)));
        assert!(!is_candidate(Err(SkipReason::Denied)));
    }

    #[test]
    fn skip_reasons_have_distinct_labels() {
        let reasons = [
            SkipReason::Test,
            SkipReason::CrateType,
            SkipReason::Refused,
            SkipReason::NotRegistry,
            SkipReason::OutDirShape,
            SkipReason::OutDirNotEmpty,
            SkipReason::BuildOutput,
            SkipReason::EnvConflict,
            SkipReason::Forced,
            SkipReason::UnsafeRoot,
            SkipReason::Denied,
            SkipReason::AliasUnusable,
            SkipReason::LibEvidence(LibVerdict::Unseen),
            SkipReason::LibEvidence(LibVerdict::MacrosOnly),
            SkipReason::LibEvidence(LibVerdict::Other),
            SkipReason::Sidecar,
        ];
        let labels: BTreeSet<&str> = reasons.iter().map(|reason| reason.as_str()).collect();
        assert_eq!(labels.len(), reasons.len());
        assert!(
            labels
                .iter()
                .all(|label| !label.is_empty() && *label != "xyzzy")
        );
        assert_eq!(
            SkipReason::LibEvidence(LibVerdict::Other).as_str(),
            "lib evidence other"
        );
    }

    #[test]
    fn unit_dir_name_needs_cargos_build_layout_for_the_package() {
        let ok = Path::new("/t/target/debug/build/dmac-0123456789abcdef/out");
        assert_eq!(
            unit_dir_name(ok, "dmac").as_deref(),
            Some("dmac-0123456789abcdef")
        );
        let dashed = Path::new("/t/target/debug/build/my-mac-0123456789ABCDEF/out");
        assert_eq!(
            unit_dir_name(dashed, "my-mac").as_deref(),
            Some("my-mac-0123456789ABCDEF")
        );
        for (path, pkg) in [
            ("t/target/debug/build/dmac-0123456789abcdef/out", "dmac"),
            ("/t/target/debug/build/dmac-0123456789abcdef/gen", "dmac"),
            ("/t/target/debug/deps/dmac-0123456789abcdef/out", "dmac"),
            ("/t/target/debug/build/dmac-0123456789abcdef/out", "other"),
            ("/t/target/debug/build/dmacx0123456789abcdef/out", "dmac"),
            ("/t/target/debug/build/dmac-0123456789abcde/out", "dmac"),
            ("/t/target/debug/build/dmac-0123456789abcdef0/out", "dmac"),
            ("/t/target/debug/build/dmac-0123456789abcdeg/out", "dmac"),
            ("/out", "dmac"),
        ] {
            assert_eq!(unit_dir_name(Path::new(path), pkg), None, "{path} {pkg}");
        }
    }

    #[test]
    fn directives_allow_only_non_exporting_non_linking_lines() {
        for prefix in ["cargo:", "cargo::"] {
            for directive in ALLOWED_DIRECTIVES {
                let line = format!("{prefix}{directive}=value");
                assert!(directives_allow_alias(&line), "{line}");
            }
            for directive in [
                "metadata=KEY=V",
                "KEY=V",
                "rustc-link-lib=z",
                "rustc-link-search=native=/x",
                "rustc-link-arg=-v",
                "rustc-flags=-l z",
                "rustc-cdylib-link-arg=-v",
                "error=boom",
                "rustc-envx=A=B",
            ] {
                let line = format!("{prefix}{directive}");
                assert!(!directives_allow_alias(&line), "{line}");
            }
        }
        assert!(directives_allow_alias(""));
        assert!(directives_allow_alias("plain output\n  cargo:KEY=V\n"));
        assert!(directives_allow_alias(
            "cargo:rustc-env=A=B\ncargo::rerun-if-changed=build.rs\n"
        ));
        assert!(!directives_allow_alias(
            "cargo:rustc-env=A=B\ncargo:KEY=V\ncargo:warning=w\n"
        ));
    }

    #[test]
    fn alias_dir_is_per_unit_under_the_layout() {
        assert_eq!(
            alias_dir(Path::new("/c/out-dirs"), "dmac-0123456789abcdef"),
            Path::new("/c/out-dirs/v1/d/dmac-0123456789abcdef/out")
        );
    }

    #[test]
    fn env_rewrites_point_values_at_or_under_out_dir_at_the_alias() {
        let real = Path::new("/t/out");
        let alias = Path::new("/c/d/out");
        let rewrites = env_rewrites(
            &os(&[
                ("OUT_DIR", "/t/out"),
                ("DEBUG_OUTPUT_DIR", "/t/out/"),
                ("GEN", "/t/out/sub/gen.rs"),
                ("HOME", "/home/u"),
            ]),
            &strings(&["--crate-name", "dmac", "-L", "/t/deps"]),
            &[real],
            alias,
        )
        .expect("no conflict");
        assert_eq!(
            rewrites,
            os(&[
                ("OUT_DIR", "/c/d/out"),
                ("DEBUG_OUTPUT_DIR", "/c/d/out"),
                ("GEN", "/c/d/out/sub/gen.rs"),
            ])
        );
    }

    #[test]
    fn env_rewrites_match_any_spelling_of_out_dir() {
        let rewrites = env_rewrites(
            &os(&[("OUT_DIR", "/t/out"), ("CANON", "/private/t/out/x")]),
            &[],
            &[Path::new("/t/out"), Path::new("/private/t/out")],
            Path::new("/c/out"),
        )
        .unwrap();
        assert_eq!(
            rewrites,
            os(&[("OUT_DIR", "/c/out"), ("CANON", "/c/out/x")])
        );
    }

    #[test]
    fn env_rewrites_refuse_any_other_mention_of_out_dir() {
        let real = [Path::new("/t/out")];
        let alias = Path::new("/c/out");
        for value in ["/t/out2", "-I/t/out", "/x:/t/out/y"] {
            assert_eq!(
                env_rewrites(
                    &os(&[("OUT_DIR", "/t/out"), ("V", value)]),
                    &[],
                    &real,
                    alias
                ),
                None,
                "{value}"
            );
        }
        assert_eq!(
            env_rewrites(
                &os(&[("OUT_DIR", "/t/out")]),
                &strings(&["-L", "native=/t/out"]),
                &real,
                alias
            ),
            None
        );
        assert_eq!(
            env_rewrites(
                &os(&[("OUT_DIR", "/t/out")]),
                &strings(&["/t/o"]),
                &real,
                alias
            ),
            Some(os(&[("OUT_DIR", "/c/out")]))
        );
    }

    #[test]
    fn env_rewrites_handle_non_utf8_values() {
        use std::os::unix::ffi::OsStringExt;
        let real = [Path::new("/t/out")];
        let under = vec![(
            OsString::from("RAW"),
            OsString::from_vec(b"/t/out/\xff".to_vec()),
        )];
        assert_eq!(
            env_rewrites(&under, &[], &real, Path::new("/c/out")),
            Some(vec![(
                OsString::from("RAW"),
                OsString::from_vec(b"/c/out/\xff".to_vec())
            )])
        );
        let mention = vec![(
            OsString::from("RAW"),
            OsString::from_vec(b"\xff/t/out".to_vec()),
        )];
        assert_eq!(
            env_rewrites(&mention, &[], &real, Path::new("/c/out")),
            None
        );
    }

    #[test]
    fn forced_by_user_matches_this_crates_out_dir_or_a_rewritten_var() {
        let rewrites = os(&[("OUT_DIR", "/c/out"), ("DEBUG_OUTPUT_DIR", "/c/out")]);
        let forced = |entries: &[&str]| forced_by_user(&strings(entries), "dmac", &rewrites);
        assert!(forced(&["dmac:OUT_DIR"]));
        assert!(forced(&["dmac:DEBUG_OUTPUT_DIR"]));
        assert!(forced(&["other:OUT_DIR", "dmac:DEBUG_OUTPUT_DIR"]));
        assert!(!forced(&["other:OUT_DIR"]));
        assert!(!forced(&["dmac:HOME"]));
        assert!(!forced(&["OUT_DIR", "DEBUG_OUTPUT_DIR"]));
        assert!(!forced(&[]));
        assert!(forced_by_user(&strings(&["dmac:OUT_DIR"]), "dmac", &[]));
    }

    #[test]
    fn root_location_must_be_outside_the_checkout() {
        let root = Path::new("/c/out-dirs");
        assert!(root_location_ok(root, &[]));
        assert!(root_location_ok(
            root,
            &[PathBuf::from("/w"), PathBuf::from("/w/target")]
        ));
        assert!(!root_location_ok(root, &[PathBuf::from("/c")]));
        assert!(!root_location_ok(
            root,
            &[PathBuf::from("/w"), PathBuf::from("/c/out-dirs")]
        ));
        assert!(root_location_ok(root, &[PathBuf::from("/c/out")]));
    }

    #[test]
    fn consumer_kind_is_proc_macro_only_for_macros_and_their_harnesses() {
        assert_eq!(
            consumer_kind(&strings(&["proc-macro"]), false, true),
            ConsumerKind::ProcMacro
        );
        assert_eq!(consumer_kind(&[], true, true), ConsumerKind::ProcMacro);
        assert_eq!(consumer_kind(&[], true, false), ConsumerKind::Other);
        assert_eq!(consumer_kind(&[], false, true), ConsumerKind::Other);
        assert_eq!(
            consumer_kind(&strings(&["lib"]), false, false),
            ConsumerKind::Other
        );
        assert_eq!(
            consumer_kind(&strings(&["bin"]), false, false),
            ConsumerKind::Other
        );
        assert_eq!(ConsumerKind::ProcMacro.marker(), "pm");
        assert_eq!(ConsumerKind::Other.marker(), "other");
    }

    #[test]
    fn links_proc_macro_needs_the_bare_extern() {
        let path = Path::new("/t/libproc_macro.rlib");
        assert!(links_proc_macro(&[
            dep("x", Some(path)),
            dep("proc_macro", None)
        ]));
        assert!(!links_proc_macro(&[dep("proc_macro", Some(path))]));
        assert!(!links_proc_macro(&[dep("std", None)]));
        assert!(!links_proc_macro(&[]));
    }

    #[test]
    fn lib_verdict_covers_every_evidence_combination() {
        assert_eq!(lib_verdict(false, false), LibVerdict::Unseen);
        assert_eq!(lib_verdict(true, false), LibVerdict::MacrosOnly);
        assert_eq!(lib_verdict(false, true), LibVerdict::Other);
        assert_eq!(lib_verdict(true, true), LibVerdict::Other);
    }

    #[test]
    fn permission_failures_are_recognized() {
        assert!(is_permission_failure(
            "error: Permission denied (os error 13)"
        ));
        assert!(is_permission_failure("Permission denied"));
        assert!(is_permission_failure("failed: os error 13"));
        assert!(!is_permission_failure("error[E0425]: cannot find value"));
        assert!(!is_permission_failure("os error 2"));
    }

    #[test]
    fn store_allowed_needs_a_clean_alias_and_no_root_file() {
        assert!(store_allowed(true, false));
        assert!(!store_allowed(false, false));
        assert!(!store_allowed(true, true));
        assert!(!store_allowed(false, true));
    }

    #[test]
    fn alias_unit_of_names_the_alias_a_path_is_in() {
        let root = Path::new("/c/out-dirs");
        assert_eq!(
            alias_unit_of(Path::new("/c/out-dirs/v1/d/dmac-0123/out/x.rs"), root),
            Some("dmac-0123")
        );
        assert_eq!(
            alias_unit_of(Path::new("/c/out-dirs/v1/libs/x"), root),
            None
        );
        assert_eq!(alias_unit_of(Path::new("/c/out-dirs/v1/d"), root), None);
    }

    #[test]
    fn permission_hint_names_the_package_whose_alias_failed() {
        let root = Path::new("/c/out-dirs");
        let stderr = "error: failed to write /c/out-dirs/v1/d/sp-api-0123456789abcdef/out/x.rs: \
                      Permission denied (os error 13)";
        let hint = permission_hint(stderr, root, true).unwrap();
        assert!(hint.contains("cargo clean -p sp-api`"), "{hint}");
        assert!(hint.contains("KACHE_OUT_DIR_ALIAS=0"), "{hint}");
        let generic = permission_hint("Permission denied", root, true).unwrap();
        assert!(
            generic.contains("cargo clean -p <macro crate>`"),
            "{generic}"
        );
        assert_eq!(permission_hint(stderr, root, false), None);
        assert_eq!(permission_hint("error: mismatched types", root, true), None);
        assert_eq!(named_package("/c/out-dirs/v1/d/", root), None);
    }

    #[test]
    fn extern_files_sit_next_to_the_extern() {
        assert_eq!(
            extern_files(Path::new("/t/deps/libhelper-0123.rmeta")),
            Some([
                PathBuf::from("/t/deps/libhelper-0123.rlib"),
                PathBuf::from("/t/deps/libhelper-0123.rmeta"),
                PathBuf::from("/t/deps/libhelper-0123.kache-alias"),
            ])
        );
    }

    #[test]
    fn sidecar_path_uses_the_unit_output_stem() {
        let args = RustcCompiler::new()
            .parse(&strings(&[
                "rustc",
                "--crate-name",
                "helper",
                "--crate-type",
                "lib",
                "src/lib.rs",
                "--out-dir",
                "/t/deps",
                "-C",
                "extra-filename=-0123456789abcdef",
            ]))
            .unwrap();
        assert_eq!(
            sidecar_path(&args),
            Some(PathBuf::from(
                "/t/deps/libhelper-0123456789abcdef.kache-alias"
            ))
        );
    }

    mod fs {
        use super::*;
        use std::os::unix::fs::PermissionsExt;

        fn mode(path: &Path) -> u32 {
            std::fs::symlink_metadata(path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777
        }

        #[test]
        fn ensure_alias_dir_creates_it_read_only() {
            let tmp = tempfile::tempdir().unwrap();
            let euid = effective_uid();
            let dir = alias_dir(tmp.path(), "dmac-0123456789abcdef");
            assert!(ensure_alias_dir(&dir, euid));
            assert_eq!(mode(&dir), 0o555);
            assert_eq!(mode(dir.parent().unwrap()) & 0o077, 0);
            assert_eq!(alias_state(&dir, euid), AliasState::Usable);
            // Already there and still usable.
            assert!(ensure_alias_dir(&dir, euid));
            if euid != 0 {
                let error = std::fs::write(dir.join("x"), b"x").unwrap_err();
                assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
            }
        }

        #[test]
        fn ensure_alias_dir_refuses_what_it_did_not_make() {
            let tmp = tempfile::tempdir().unwrap();
            let euid = effective_uid();

            let foreign = alias_dir(tmp.path(), "a-0123456789abcdef");
            assert!(ensure_alias_dir(&foreign, euid));
            assert!(!ensure_alias_dir(&foreign, euid + 1));
            assert_eq!(alias_state(&foreign, euid + 1), AliasState::Unusable);

            let target = tmp.path().join("elsewhere");
            std::fs::create_dir(&target).unwrap();
            std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o555)).unwrap();
            let link = alias_dir(tmp.path(), "b-0123456789abcdef");
            std::fs::create_dir_all(link.parent().unwrap()).unwrap();
            std::os::unix::fs::symlink(&target, &link).unwrap();
            assert!(!ensure_alias_dir(&link, euid));

            let full = alias_dir(tmp.path(), "c-0123456789abcdef");
            std::fs::create_dir_all(&full).unwrap();
            std::fs::write(full.join("x"), b"x").unwrap();
            std::fs::set_permissions(&full, std::fs::Permissions::from_mode(0o555)).unwrap();
            assert!(!ensure_alias_dir(&full, euid));
            std::fs::set_permissions(&full, std::fs::Permissions::from_mode(0o755)).unwrap();

            let writable = alias_dir(tmp.path(), "d-0123456789abcdef");
            std::fs::create_dir_all(&writable).unwrap();
            std::fs::set_permissions(&writable, std::fs::Permissions::from_mode(0o755)).unwrap();
            assert!(!ensure_alias_dir(&writable, euid));

            assert_eq!(
                alias_state(&tmp.path().join("missing"), euid),
                AliasState::Absent
            );
            let file = tmp.path().join("file");
            std::fs::write(&file, b"").unwrap();
            std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o444)).unwrap();
            assert_eq!(alias_state(&file, euid), AliasState::Unusable);
            // Not a missing directory: something else is in the way.
            assert_eq!(alias_state(&file.join("out"), euid), AliasState::Unusable);
            assert!(!ensure_alias_dir(&file.join("d/out"), euid));
            assert!(!ensure_alias_dir(Path::new("/"), euid));
        }

        #[test]
        fn private_dirs_are_owned_and_closed_to_others() {
            let tmp = tempfile::tempdir().unwrap();
            let euid = effective_uid();
            let dir = tmp.path().join("root");
            create_private_dir_all(&dir).unwrap();
            assert_eq!(mode(&dir), 0o700);
            assert!(is_private_dir(&dir, euid));
            assert!(!is_private_dir(&dir, euid + 1));
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o720)).unwrap();
            assert!(!is_private_dir(&dir, euid));
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o702)).unwrap();
            assert!(!is_private_dir(&dir, euid));
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
            assert!(is_private_dir(&dir, euid));
            let link = tmp.path().join("link");
            std::os::unix::fs::symlink(&dir, &link).unwrap();
            assert!(!is_private_dir(&link, euid));
            assert!(!is_private_dir(&tmp.path().join("missing"), euid));
        }

        /// A tempdir `deps/` holding an aliased lib, and a root that lists it.
        fn aliased_lib(tmp: &Path) -> (PathBuf, PathBuf, [PathBuf; 3]) {
            let root = tmp.join("out-dirs");
            register_candidate(&root, "0123456789abcdef").unwrap();
            let deps = tmp.join("target/debug/deps");
            std::fs::create_dir_all(&deps).unwrap();
            let rlib = deps.join("libhelper-0123456789abcdef.rlib");
            let files = extern_files(&rlib).unwrap();
            for file in &files {
                std::fs::write(file, b"x").unwrap();
            }
            (root, rlib, files)
        }

        #[test]
        fn tripwire_removes_an_aliased_lib_an_other_consumer_links() {
            let tmp = tempfile::tempdir().unwrap();
            let (root, rlib, files) = aliased_lib(tmp.path());
            let mut stderr = Vec::new();
            let exit = consumer_checks(
                &root,
                &[dep("helper", Some(&rlib))],
                ConsumerKind::Other,
                &mut stderr,
            );
            assert_eq!(exit, Some(1));
            for file in &files {
                assert!(!file.exists(), "{} survived", file.display());
            }
            assert!(libs_dir(&root).join("0123456789abcdef/other").exists());
            let message = String::from_utf8(stderr).unwrap();
            assert_eq!(message.lines().count(), 1, "{message}");
            assert!(message.contains("helper"), "{message}");
            assert!(message.contains("Run the build again"), "{message}");
        }

        #[test]
        fn a_proc_macro_consumer_only_records_evidence() {
            let tmp = tempfile::tempdir().unwrap();
            let (root, rlib, files) = aliased_lib(tmp.path());
            let mut stderr = Vec::new();
            let exit = consumer_checks(
                &root,
                &[dep("helper", Some(&rlib)), dep("proc_macro", None)],
                ConsumerKind::ProcMacro,
                &mut stderr,
            );
            assert_eq!(exit, None);
            assert!(files.iter().all(|file| file.exists()));
            assert!(stderr.is_empty());
            assert_eq!(
                read_verdict(&root, "0123456789abcdef"),
                LibVerdict::MacrosOnly
            );
        }

        #[test]
        fn an_unaliased_lib_does_not_trip_but_is_recorded() {
            let tmp = tempfile::tempdir().unwrap();
            let (root, rlib, [_, _, sidecar]) = aliased_lib(tmp.path());
            std::fs::remove_file(&sidecar).unwrap();
            let unlisted = tmp
                .path()
                .join("target/debug/deps/libother-fedcba9876543210.rlib");
            std::fs::write(unlisted.with_extension("kache-alias"), b"").unwrap();
            let mut stderr = Vec::new();
            let exit = consumer_checks(
                &root,
                &[dep("helper", Some(&rlib)), dep("other", Some(&unlisted))],
                ConsumerKind::Other,
                &mut stderr,
            );
            assert_eq!(exit, None);
            assert!(rlib.exists());
            assert_eq!(read_verdict(&root, "0123456789abcdef"), LibVerdict::Other);
            assert!(!libs_dir(&root).join("fedcba9876543210").exists());
        }

        #[test]
        fn candidates_deny_markers_and_sidecars_round_trip() {
            let tmp = tempfile::tempdir().unwrap();
            let root = tmp.path().join("out-dirs");
            assert!(listed_candidates(&root).is_empty());
            register_candidate(&root, "aaaa").unwrap();
            register_candidate(&root, "bbbb").unwrap();
            assert_eq!(
                listed_candidates(&root).into_iter().collect::<Vec<_>>(),
                ["aaaa", "bbbb"]
            );
            assert_eq!(read_verdict(&root, "aaaa"), LibVerdict::Unseen);

            deny(&root, "dmac-0123").unwrap();
            assert!(deny_path(&root, "dmac-0123").is_file());

            let sidecar = tmp.path().join("libx-0123.kache-alias");
            write_sidecar(&sidecar, true).unwrap();
            assert!(sidecar.is_file());
            write_sidecar(&sidecar, false).unwrap();
            assert!(!sidecar.exists());
            write_sidecar(&sidecar, false).unwrap();
            assert!(write_sidecar(&tmp.path().join("no/such/dir/x"), true).is_err());
            let blocker = tmp.path().join("dir.kache-alias");
            std::fs::create_dir(&blocker).unwrap();
            assert!(write_sidecar(&blocker, false).is_err());
        }

        #[test]
        fn build_output_and_out_dir_checks_read_the_build_dir() {
            let tmp = tempfile::tempdir().unwrap();
            let build = tmp.path().join("dmac-0123456789abcdef");
            let out = build.join("out");
            std::fs::create_dir_all(&out).unwrap();
            assert!(dir_has_no_entries(&out));
            assert!(!dir_has_no_entries(&tmp.path().join("missing")));
            assert!(!build_output_allows_alias(&build));
            std::fs::write(build.join("output"), "cargo:rustc-env=A=B\n").unwrap();
            assert!(build_output_allows_alias(&build));
            std::fs::write(build.join("output"), "cargo:KEY=V\n").unwrap();
            assert!(!build_output_allows_alias(&build));
            let limit = usize::try_from(MAX_BUILD_OUTPUT_BYTES).unwrap();
            std::fs::write(build.join("output"), "x".repeat(limit)).unwrap();
            assert!(build_output_allows_alias(&build));
            std::fs::write(build.join("output"), "x".repeat(limit + 1)).unwrap();
            assert!(!build_output_allows_alias(&build));
            std::fs::write(out.join("gen.rs"), b"").unwrap();
            assert!(!dir_has_no_entries(&out));
        }

        /// A registry-shaped unit with an empty OUT_DIR, as Cargo lays it out.
        struct Layout {
            _tmp: tempfile::TempDir,
            root: PathBuf,
            out_dir: PathBuf,
            env: Vec<(OsString, OsString)>,
            args: RustcArgs,
        }

        fn layout(crate_type: &str) -> Layout {
            let tmp = tempfile::tempdir().unwrap();
            let base = std::fs::canonicalize(tmp.path()).unwrap();
            let manifest = base.join("registry/src/index/dmac");
            let target = base.join("w/target");
            let build = target.join("debug/build/dmac-0123456789abcdef");
            let out_dir = build.join("out");
            std::fs::create_dir_all(&manifest).unwrap();
            std::fs::create_dir_all(&out_dir).unwrap();
            std::fs::create_dir_all(target.join("debug/deps")).unwrap();
            std::fs::write(build.join("output"), "cargo:rustc-env=DEBUG_OUTPUT_DIR=x\n").unwrap();
            let env = vec![
                (
                    OsString::from("CARGO_MANIFEST_DIR"),
                    manifest.clone().into(),
                ),
                (OsString::from("CARGO_PKG_NAME"), OsString::from("dmac")),
                (OsString::from("OUT_DIR"), out_dir.clone().into()),
                (OsString::from("DEBUG_OUTPUT_DIR"), out_dir.clone().into()),
            ];
            let args = RustcCompiler::new()
                .parse(&strings(&[
                    "rustc",
                    "--crate-name",
                    "dmac",
                    "--crate-type",
                    crate_type,
                    manifest.join("src/lib.rs").to_str().unwrap(),
                    "--out-dir",
                    target.join("debug/deps").to_str().unwrap(),
                    "-C",
                    "extra-filename=-fedcba9876543210",
                ]))
                .unwrap();
            Layout {
                root: base.join("cache/out-dirs"),
                _tmp: tmp,
                out_dir,
                env,
                args,
            }
        }

        fn gather(layout: &Layout, forced: &[&str]) -> Detected {
            UnitFacts::gather(
                &layout.args,
                &layout.env,
                false,
                &strings(forced),
                &layout.root,
                effective_uid(),
            )
        }

        #[test]
        fn gather_reads_a_registry_unit_with_an_empty_out_dir() {
            let layout = layout("proc-macro");
            let detected = gather(&layout, &[]);
            assert_eq!(
                detected.facts,
                UnitFacts {
                    crate_types: strings(&["proc-macro"]),
                    unit_dir: Some("dmac-0123456789abcdef".into()),
                    ..passing_facts(&[])
                }
            );
            let alias = alias_dir(&layout.root, "dmac-0123456789abcdef");
            assert_eq!(
                detected.rewrites,
                vec![
                    (OsString::from("OUT_DIR"), alias.clone().into_os_string()),
                    (OsString::from("DEBUG_OUTPUT_DIR"), alias.into_os_string()),
                ]
            );
            assert_eq!(detected.alias, AliasState::Absent);
            assert_eq!(detected.verdict, LibVerdict::Unseen);
            assert!(is_private_dir(&layout.root, effective_uid()));
        }

        #[test]
        fn gather_reads_evidence_denials_and_the_alias_state() {
            let layout = layout("lib");
            register_candidate(&layout.root, "fedcba9876543210").unwrap();
            std::fs::write(libs_dir(&layout.root).join("fedcba9876543210/pm"), b"").unwrap();
            let alias = alias_dir(&layout.root, "dmac-0123456789abcdef");
            assert!(ensure_alias_dir(&alias, effective_uid()));
            let detected = gather(&layout, &[]);
            assert_eq!(detected.verdict, LibVerdict::MacrosOnly);
            assert_eq!(detected.alias, AliasState::Usable);
            assert!(!detected.facts.denied);
            assert_eq!(
                decide(&detected.facts, detected.verdict, detected.alias),
                Ok(Tier::Lib)
            );

            deny(&layout.root, "dmac-0123456789abcdef").unwrap();
            assert!(gather(&layout, &[]).facts.denied);
            assert!(gather(&layout, &["dmac:DEBUG_OUTPUT_DIR"]).facts.forced);
        }

        #[test]
        fn gather_needs_a_private_root_outside_the_checkout() {
            let mut layout = layout("proc-macro");
            assert!(gather(&layout, &[]).facts.root_safe);
            std::fs::set_permissions(&layout.root, std::fs::Permissions::from_mode(0o777)).unwrap();
            assert!(!gather(&layout, &[]).facts.root_safe);

            // A private root inside the target dir is still refused.
            let target = layout.args.target_dir().unwrap();
            layout.root = target.join("out-dirs");
            assert!(!gather(&layout, &[]).facts.root_safe);
            assert!(is_private_dir(&layout.root, effective_uid()));
        }

        #[test]
        fn gather_rules_the_unit_out_on_what_it_finds() {
            let mut layout = layout("proc-macro");
            std::fs::write(layout.out_dir.join("gen.rs"), b"").unwrap();
            let full = gather(&layout, &[]);
            assert!(!full.facts.out_dir_empty);
            assert!(full.facts.directives_ok);
            std::fs::remove_file(layout.out_dir.join("gen.rs")).unwrap();

            std::fs::write(
                layout.out_dir.parent().unwrap().join("output"),
                "cargo:KEY=V\n",
            )
            .unwrap();
            assert!(!gather(&layout, &[]).facts.directives_ok);

            layout.env.push((
                OsString::from("CFLAGS"),
                format!("-I{}", layout.out_dir.display()).into(),
            ));
            let conflict = gather(&layout, &[]);
            assert!(!conflict.facts.env_ok);
            assert!(conflict.rewrites.is_empty());
        }

        #[test]
        fn gather_reads_nothing_past_the_shape_outside_the_registry() {
            let mut layout = layout("proc-macro");
            layout.env[0].1 = OsString::from("/w/crates/dmac");
            let local = gather(&layout, &[]);
            assert!(!local.facts.registry);
            assert!(local.facts.unit_dir.is_some());
            assert!(!local.facts.out_dir_empty);
            assert!(!local.facts.root_safe);
            assert!(!layout.root.exists());
        }
    }
}
