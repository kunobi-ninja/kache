//! Registry build units copied into a new checkout's target directory before
//! Cargo decides what to build.
//!
//! The first thing Cargo sends a new target directory's `RUSTC_WRAPPER` is
//! its target-info probe (`rustc - --crate-name ___ --print=file-names ...`),
//! before it takes the build lock and before it checks any unit's freshness.
//! Cargo caches the answer in `<target>/.rustc_info.json`, so a target
//! directory without that file is one Cargo has not built yet. The wrapper
//! asks the daemon to seed it and waits up to [`SEED_DEADLINE`]; the daemon
//! copies the units another checkout already built for the same registry and
//! git packages, and Cargo then finds them fresh and skips them.
//!
//! A unit's directory name carries Cargo's hash of everything that decides it
//! (package, version, features, profile, dependencies, compiler). A registry
//! or git package's hash does not depend on where the checkout is, so a unit
//! with the same hash in another target directory is the same unit. The copy
//! keeps what Cargo's freshness check relies on:
//!
//! - Only packages with a `source` in `Cargo.lock` are copied. A path
//!   package's fingerprint trusts its sources' modification times, so a copy
//!   from a checkout with different sources could pass as fresh.
//! - Modification times are kept: Cargo rebuilds a unit whose dependencies'
//!   outputs are newer than its own.
//! - Files are cloned where the filesystem can, else copied. Each unit's
//!   fingerprint is put in place last, so an interrupted copy leaves a unit
//!   Cargo rebuilds rather than one it trusts.
//! - A build script's `OUT_DIR` that a hermetic run left as a link to its
//!   sealed, shared run in this kache's cache is linked to the same run, and
//!   recorded as linking to it so the run is kept while the new target uses
//!   it. Any other symbolic link keeps the unit out.
//! - A build script whose recorded output names the other checkout, other
//!   than its own `OUT_DIR` (which Cargo rewrites), is not copied. Files
//!   under that script's `out/` are copied unchanged, so one that names
//!   the other checkout keeps the unit out too.
//! - The other checkout must have been built by the same `rustc`: the one
//!   kache recorded while building into it, else the one Cargo's rustc info
//!   cache names (Cargo does not write that cache when it cannot fingerprint
//!   `rustc`). No build may hold either profile's lock while units are
//!   copied.
//!
//! Both of Cargo's layouts are handled (see [`crate::cargo_layout`]). Only
//! the host `debug` profile is seeded: the probe does not say which profile
//! the build uses, and `debug` is the one `build`, `check`, `test` and
//! `clippy` share.

mod placement;

use std::collections::{BTreeSet, HashMap};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

#[cfg(unix)]
use crate::build_script::HeldRun;
/// No hermetic run is linked to off Unix.
#[cfg(not(unix))]
type HeldRun = std::convert::Infallible;

/// How long the daemon copies for. Copying stops at this deadline, between
/// files, and Cargo builds whatever was not copied.
pub(crate) const SEED_DEADLINE: Duration = Duration::from_secs(3);

/// How long the probe waits for the daemon's answer: the deadline and time
/// for the file copy in progress when it passed.
pub(crate) fn seed_wait() -> Duration {
    SEED_DEADLINE + Duration::from_secs(2)
}

/// Units copied at once. A copy is mostly metadata calls, which overlap: on
/// btrfs, 1159 units took 2.9 s on one thread, 0.6 s on four and 0.5 s on
/// eight.
const COPY_THREADS: usize = 4;

/// The profile directory seeded.
const PROFILE: &str = "debug";

/// Where a unit is copied before it is renamed into place.
const STAGING: &str = ".kache-seeding-";

/// Largest build-script output file searched for a donor path. Caches such
/// as `CMakeCache.txt` sit well under this. A bigger file is left unread so
/// one object cannot spend the seeding deadline.
const OUT_FILE_SCAN_LIMIT: u64 = 1 << 20;

/// Is `args` (the compiler and its arguments) Cargo's target-info probe?
pub(crate) fn is_target_info_probe(args: &[String]) -> bool {
    let crate_name = args
        .windows(2)
        .any(|pair| pair[0] == "--crate-name" && pair[1] == "___");
    let file_names = args.iter().any(|arg| arg == "--print=file-names")
        || args
            .windows(2)
            .any(|pair| pair[0] == "--print" && pair[1] == "file-names");
    crate_name && file_names
}

/// A target directory Cargo is about to build for the first time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NewTarget {
    pub(crate) target_dir: PathBuf,
    pub(crate) workspace_root: PathBuf,
}

/// The target directory a probe in `cwd` is for, when it is one this can
/// seed: the workspace is the nearest directory up from `cwd` holding
/// `Cargo.toml` and `Cargo.lock`, and the target directory is
/// `target_env` (the resolved intermediate directory) or `<workspace>/target`. `None` when
/// `elsewhere` says configuration moves the target or build directory, and
/// when Cargo has already built there.
pub(crate) fn new_target(
    cwd: &Path,
    target_env: Option<&OsStr>,
    elsewhere: bool,
) -> Option<NewTarget> {
    if elsewhere {
        return None;
    }
    let workspace_root = cwd
        .ancestors()
        .find(|dir| dir.join("Cargo.lock").is_file() && dir.join("Cargo.toml").is_file())?
        .to_path_buf();
    let target_dir = match target_env.filter(|value| !value.is_empty()) {
        Some(value) => cwd.join(value),
        None => workspace_root.join("target"),
    };
    (!built(&target_dir)).then_some(NewTarget {
        target_dir,
        workspace_root,
    })
}

/// Whether Cargo has built in `target_dir`: it cached its compiler probe
/// there or wrote any unit to the seeded profile.
fn built(target_dir: &Path) -> bool {
    [
        target_dir.join(".rustc_info.json"),
        target_dir.join(PROFILE).join(".fingerprint"),
        target_dir.join(PROFILE).join("build"),
    ]
    .iter()
    .any(|path| path.exists())
}

/// The registry and git packages in a lockfile: those with a `source`, less
/// any name a path package also uses.
pub(crate) fn registry_packages(lockfile: &str) -> BTreeSet<String> {
    let Ok(lock) = lockfile.parse::<toml::Table>() else {
        return BTreeSet::new();
    };
    let packages = lock
        .get("package")
        .and_then(toml::Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let mut registry = BTreeSet::new();
    let mut path = BTreeSet::new();
    for package in packages {
        let Some(name) = package.get("name").and_then(toml::Value::as_str) else {
            continue;
        };
        if package.get("source").is_some() {
            registry.insert(name.to_owned());
        } else {
            path.insert(name.to_owned());
        }
    }
    &registry - &path
}

/// A target directory units may be copied from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Donor {
    pub(crate) target_dir: PathBuf,
    pub(crate) workspace_root: PathBuf,
    /// The `rustc -vV` kache recorded building into it, if any.
    pub(crate) rustc: Option<String>,
}

/// Which of Cargo's layouts a profile directory uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Layout {
    /// `build/<pkg>/<hash>/`, one directory per unit (Cargo 1.100 and later).
    PerUnit,
    /// `deps/`, `.fingerprint/<pkg>-<hash>/` and `build/<pkg>-<hash>/`.
    Shared,
}

fn layout(profile: &Path) -> Option<Layout> {
    if profile.join(".fingerprint").is_dir() {
        return Some(Layout::Shared);
    }
    subdirectories(&profile.join("build"))
        .iter()
        .flat_map(|package| subdirectories(package))
        .any(|unit| unit.join("fingerprint").is_dir())
        .then_some(Layout::PerUnit)
}

/// One unit of a donor's profile.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Unit {
    package: String,
    hash: String,
}

/// The donor's units of `packages`.
fn units(profile: &Path, layout: Layout, packages: &BTreeSet<String>) -> Vec<Unit> {
    let mut units = Vec::new();
    match layout {
        Layout::PerUnit => {
            for package in packages {
                for unit in subdirectories(&profile.join("build").join(package)) {
                    let hash = file_name(&unit);
                    if crate::cargo_layout::is_unit_hash(&hash) && unit.join("fingerprint").is_dir()
                    {
                        units.push(Unit {
                            package: package.clone(),
                            hash,
                        });
                    }
                }
            }
        }
        Layout::Shared => {
            for fingerprint in subdirectories(&profile.join(".fingerprint")) {
                let name = file_name(&fingerprint);
                let Some((package, hash)) = name.rsplit_once('-') else {
                    continue;
                };
                if packages.contains(package) && crate::cargo_layout::is_unit_hash(hash) {
                    units.push(Unit {
                        package: package.to_owned(),
                        hash: hash.to_owned(),
                    });
                }
            }
        }
    }
    units
}

/// The unit's directory or fingerprint in a profile: what, once present,
/// makes Cargo consider the unit built.
fn marker(profile: &Path, layout: Layout, unit: &Unit) -> PathBuf {
    match layout {
        Layout::PerUnit => profile.join("build").join(&unit.package).join(&unit.hash),
        Layout::Shared => profile
            .join(".fingerprint")
            .join(format!("{}-{}", unit.package, unit.hash)),
    }
}

/// A path into the donor in the unit's recorded build-script output, other
/// than its own `OUT_DIR`, which Cargo rewrites when it replays the output.
fn names_the_donor(profile: &Path, layout: Layout, unit: &Unit, donor: &Donor) -> bool {
    let run = match layout {
        Layout::PerUnit => profile
            .join("build")
            .join(&unit.package)
            .join(&unit.hash)
            .join("run"),
        Layout::Shared => profile
            .join("build")
            .join(format!("{}-{}", unit.package, unit.hash)),
    };
    let stdout = match layout {
        Layout::PerUnit => run.join("stdout"),
        Layout::Shared => run.join("output"),
    };
    let Ok(output) = std::fs::read(stdout) else {
        return false;
    };
    let output = String::from_utf8_lossy(&output);
    let out_dir = std::fs::read_to_string(run.join("root-output")).unwrap_or_default();
    let rest = match out_dir.trim() {
        "" => output.into_owned(),
        out_dir => output.replace(out_dir, ""),
    };
    [&donor.target_dir, &donor.workspace_root]
        .iter()
        .any(|path| rest.contains(path.to_string_lossy().as_ref()))
}

/// Whether a file under the build script's `out/` names the donor.
///
/// Cargo rewrites `OUT_DIR` only when it replays the script's stdout. A
/// cache written into `out/` (cmake's `CMakeCache.txt` records
/// `OUT_DIR/build`) still contains the other checkout's path, and copying
/// it makes the script fail in the new target. The scan runs only when the
/// script's stdout record exists, so a compiler unit's `out/` is the
/// artifact itself and is still copied. A file larger than
/// [`OUT_FILE_SCAN_LIMIT`] is not read. An unreadable file, or a directory
/// that cannot be listed, keeps the unit out.
fn out_dir_names_the_donor(profile: &Path, layout: Layout, unit: &Unit, donor: &Donor) -> bool {
    let stdout = match layout {
        Layout::PerUnit => profile
            .join("build")
            .join(&unit.package)
            .join(&unit.hash)
            .join("run/stdout"),
        Layout::Shared => profile
            .join("build")
            .join(format!("{}-{}", unit.package, unit.hash))
            .join("output"),
    };
    if !stdout.is_file() {
        return false;
    }
    let needles = donor_needles(donor);
    directory_names_the_donor(&build_script_out_dir(profile, layout, unit), &needles)
}

/// `out/` of a build script: `<unit>/out` since Cargo 1.100, and
/// `build/<pkg>-<hash>/out` before that.
fn build_script_out_dir(profile: &Path, layout: Layout, unit: &Unit) -> PathBuf {
    match layout {
        Layout::PerUnit => profile
            .join("build")
            .join(&unit.package)
            .join(&unit.hash)
            .join("out"),
        Layout::Shared => profile
            .join("build")
            .join(format!("{}-{}", unit.package, unit.hash))
            .join("out"),
    }
}

fn donor_needles(donor: &Donor) -> Vec<String> {
    [&donor.target_dir, &donor.workspace_root]
        .into_iter()
        .map(|path| path.to_string_lossy().into_owned())
        .filter(|text| !text.is_empty())
        .collect()
}

fn directory_names_the_donor(dir: &Path, needles: &[String]) -> bool {
    let listed = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return dir.exists(),
    };
    listed.flatten().any(|entry| {
        let path = entry.path();
        std::fs::symlink_metadata(&path).is_ok_and(|metadata| {
            if metadata.is_dir() {
                directory_names_the_donor(&path, needles)
            } else {
                metadata.is_file() && file_names_the_donor(&path, metadata.len(), needles)
            }
        })
    })
}

fn file_names_the_donor(path: &Path, len: u64, needles: &[String]) -> bool {
    if len > OUT_FILE_SCAN_LIMIT {
        return false;
    }
    let Ok(bytes) = std::fs::read(path) else {
        return true;
    };
    needles
        .iter()
        .any(|needle| crate::build_script::find_bytes(&bytes, needle.as_bytes()).is_some())
}

/// The files in a shared-layout donor's `deps/`, by the unit hash their name
/// carries (`<name>-<hash>` or `<name>-<hash>.<ext>`). `deps/` holds a file
/// or more per unit, so it is listed once per donor, not once per unit.
fn deps_by_hash(deps: &Path) -> HashMap<String, Vec<PathBuf>> {
    let mut index: HashMap<String, Vec<PathBuf>> = HashMap::new();
    for file in entries(deps) {
        let name = file_name(&file);
        for hash in unit_hashes(&name) {
            index.entry(hash.to_owned()).or_default().push(file.clone());
        }
    }
    index
}

/// Every unit hash in a `deps/` file name: 16 hex digits after a `-`,
/// followed by a `.` or the end of the name.
fn unit_hashes(name: &str) -> impl Iterator<Item = &str> {
    name.match_indices('-').filter_map(move |(at, _)| {
        let rest = &name[at + 1..];
        let hash = rest.get(..16)?;
        (crate::cargo_layout::is_unit_hash(hash)
            && matches!(rest.as_bytes().get(16), None | Some(b'.')))
        .then_some(hash)
    })
}

/// Copy one unit from the donor profile `from` to `to`. The unit's marker
/// goes last, by rename. `deps` is [`deps_by_hash`] of the donor's `deps/`
/// for the shared layout.
fn copy_unit(
    from: &Path,
    to: &Path,
    layout: Layout,
    unit: &Unit,
    deps: &HashMap<String, Vec<PathBuf>>,
    cache_dir: &Path,
    deadline: Instant,
) -> std::io::Result<()> {
    match layout {
        Layout::PerUnit => {
            let package = to.join("build").join(&unit.package);
            std::fs::create_dir_all(&package)?;
            place_tree(
                &from.join("build").join(&unit.package).join(&unit.hash),
                &package.join(&unit.hash),
                cache_dir,
                deadline,
            )
        }
        Layout::Shared => {
            let named = format!("{}-{}", unit.package, unit.hash);
            let to_deps = to.join("deps");
            std::fs::create_dir_all(&to_deps)?;
            for file in deps.get(&unit.hash).into_iter().flatten() {
                let name = file_name(file);
                if !to_deps.join(&name).exists() {
                    place_tree(file, &to_deps.join(&name), cache_dir, deadline)?;
                }
            }
            let build = from.join("build").join(&named);
            if build.is_dir() {
                std::fs::create_dir_all(to.join("build"))?;
                place_tree(&build, &to.join("build").join(&named), cache_dir, deadline)?;
            }
            std::fs::create_dir_all(to.join(".fingerprint"))?;
            place_tree(
                &from.join(".fingerprint").join(&named),
                &to.join(".fingerprint").join(&named),
                cache_dir,
                deadline,
            )
        }
    }
}

/// Copy `from` (a file or a directory) beside `to` and rename it there.
fn place_tree(from: &Path, to: &Path, cache_dir: &Path, deadline: Instant) -> std::io::Result<()> {
    let staging = to.with_file_name(format!("{STAGING}{}-{}", file_name(to), std::process::id()));
    let tree = Placing {
        from,
        to,
        cache_dir,
    };
    // Held past the rename, so a sweep keeps each run the tree links to until
    // its recorded `OUT_DIR` does.
    let mut runs = Vec::new();
    let copied = copy_tree(from, &staging, tree, &mut runs, deadline)
        .and_then(|()| std::fs::rename(&staging, to));
    if copied.is_err() {
        let _ = std::fs::remove_dir_all(&staging);
        let _ = std::fs::remove_file(&staging);
    }
    copied
}

/// A tree [`place_tree`] copies: where it comes from, where it is placed,
/// and the cache whose hermetic runs a link in it may name.
#[derive(Clone, Copy)]
#[cfg_attr(not(unix), allow(dead_code))]
struct Placing<'a> {
    from: &'a Path,
    to: &'a Path,
    cache_dir: &'a Path,
}

/// Copy `from`, a file or directory in `tree`, to `to`, keeping every file's
/// modification time. A symbolic link that stays inside the tree is copied
/// as a link, and a build script's `OUT_DIR` linked to its sealed hermetic
/// run is linked to the same run; any other symbolic link fails the copy,
/// and so does reaching `deadline` before a file. The runs linked to are
/// added to `runs`, held.
#[cfg_attr(not(unix), allow(clippy::only_used_in_recursion))]
fn copy_tree(
    from: &Path,
    to: &Path,
    tree: Placing,
    runs: &mut Vec<HeldRun>,
    deadline: Instant,
) -> std::io::Result<()> {
    let metadata = std::fs::symlink_metadata(from)?;
    if metadata.is_dir() {
        std::fs::create_dir(to)?;
        for entry in entries(from) {
            copy_tree(&entry, &to.join(file_name(&entry)), tree, runs, deadline)?;
        }
        return Ok(());
    }
    if Instant::now() >= deadline {
        return Err(std::io::ErrorKind::TimedOut.into());
    }
    #[cfg(unix)]
    if metadata.file_type().is_symlink() {
        let target = std::fs::read_link(from)?;
        let below = from
            .strip_prefix(tree.from)
            .map_err(std::io::Error::other)?;
        if crate::build_script::link_stays_inside(below, &target) {
            return std::os::unix::fs::symlink(target, to);
        }
        let cargo_out_dir = tree.to.join(below);
        if let Some(run) =
            crate::build_script::link_sealed_out_dir(tree.cache_dir, &target, &cargo_out_dir, to)
                .map_err(std::io::Error::other)?
        {
            runs.push(run);
            return Ok(());
        }
    }
    if !metadata.is_file() {
        return Err(std::io::Error::other(format!(
            "{} is not a file or a directory",
            from.display()
        )));
    }
    copy_file(from, to, &metadata)
}

/// Copy the file `from` to `to`, giving it `metadata`'s permissions and
/// modification time. A Linux reflink creates `to` with the default mode, so
/// without the permissions a seeded build script is one Cargo cannot execute.
fn copy_file(from: &Path, to: &Path, metadata: &std::fs::Metadata) -> std::io::Result<()> {
    if kache_store::link::try_reflink(from, to).is_err() {
        std::fs::copy(from, to)?;
    }
    filetime::set_file_mtime(
        to,
        filetime::FileTime::from_last_modification_time(metadata),
    )?;
    std::fs::set_permissions(to, metadata.permissions())
}

/// What a seeding did.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Seeded {
    pub(crate) units: usize,
    pub(crate) donor: Option<PathBuf>,
}

/// Editor and check targets reuse only the same lane in another checkout.
fn seed_lane(directory: &Path) -> &str {
    match directory.file_name().and_then(OsStr::to_str) {
        Some("check") => "check",
        Some("rust-analyzer") => "rust-analyzer",
        _ => "build",
    }
}

/// Copy the registry units `target` needs from the first donor, most recent
/// first, built by the `rustc -vV` in `rustc_version` that has any. A link
/// to a hermetic run is linked again only into `cache_dir`. Stops at
/// `deadline`.
pub(crate) fn seed(
    target: &NewTarget,
    rustc_version: &str,
    donors: &[Donor],
    cache_dir: &Path,
    deadline: Instant,
) -> Seeded {
    let Ok(lockfile) = std::fs::read_to_string(target.workspace_root.join("Cargo.lock")) else {
        return Seeded::default();
    };
    let packages = registry_packages(&lockfile);
    if packages.is_empty() || rustc_version.trim().is_empty() {
        return Seeded::default();
    }
    let to = target.target_dir.join(PROFILE);
    let creates_target = !target.target_dir.exists();
    if std::fs::create_dir_all(&to).is_err() {
        return Seeded::default();
    }
    if creates_target {
        tag_as_cargo_target(&target.target_dir);
    }
    let Some(_ours) = try_lock(&to.join(".cargo-lock")) else {
        return Seeded::default();
    };
    // Another build may have started here since the probe looked.
    if built(&target.target_dir) {
        return Seeded::default();
    }
    for donor in donors {
        if Instant::now() >= deadline {
            break;
        }
        if donor.target_dir == target.target_dir
            || seed_lane(&donor.target_dir) != seed_lane(&target.target_dir)
            || !donor_built_by(donor, rustc_version)
        {
            continue;
        }
        let from = donor.target_dir.join(PROFILE);
        let Some(layout) = layout(&from) else {
            continue;
        };
        let Some(_theirs) = try_lock(&from.join(".cargo-lock")) else {
            continue;
        };
        let deps = match layout {
            Layout::Shared => deps_by_hash(&from.join("deps")),
            Layout::PerUnit => HashMap::new(),
        };
        let units = units(&from, layout, &packages);
        let next = AtomicUsize::new(0);
        let copied = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..COPY_THREADS {
                scope.spawn(|| {
                    while let Some(unit) = units.get(next.fetch_add(1, Ordering::Relaxed)) {
                        if Instant::now() >= deadline {
                            break;
                        }
                        if marker(&to, layout, unit).exists()
                            || names_the_donor(&from, layout, unit, donor)
                            || out_dir_names_the_donor(&from, layout, unit, donor)
                        {
                            continue;
                        }
                        match copy_unit(&from, &to, layout, unit, &deps, cache_dir, deadline) {
                            Ok(()) => {
                                copied.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(error) => {
                                tracing::debug!(
                                    "did not seed {}-{}: {error}",
                                    unit.package,
                                    unit.hash
                                )
                            }
                        }
                    }
                });
            }
        });
        let copied = copied.into_inner();
        if copied > 0 {
            return Seeded {
                units: copied,
                donor: Some(donor.workspace_root.clone()),
            };
        }
    }
    Seeded::default()
}

/// What Cargo writes to `CACHEDIR.TAG` in a target directory it creates.
const CARGO_CACHEDIR_TAG: &str = "Signature: 8a477f597d28d172789f06886806bc55
# This file is a cache directory tag created by cargo.
# For information about cache directory tags see https://bford.info/cachedir/
";

/// Tag a target directory seeding created, as Cargo would have. Cargo tags
/// only a target directory it creates itself, so it never tags this one, and
/// kache neither tracks nor roots events in an untagged target directory.
fn tag_as_cargo_target(target_dir: &Path) {
    let _ = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target_dir.join("CACHEDIR.TAG"))
        .and_then(|mut file| std::io::Write::write_all(&mut file, CARGO_CACHEDIR_TAG.as_bytes()));
}

/// Whether Cargo's cached `rustc -vV` answer in `target_dir` is `version`.
/// Whether `version` built `donor`. What kache recorded while building into
/// it decides; without a record, Cargo's rustc info cache does, which Cargo
/// does not always write (it cannot fingerprint every rustc).
fn donor_built_by(donor: &Donor, version: &str) -> bool {
    match &donor.rustc {
        Some(recorded) => recorded.trim() == version.trim(),
        None => built_by(&donor.target_dir, version),
    }
}

fn built_by(target_dir: &Path, version: &str) -> bool {
    let Ok(text) = std::fs::read_to_string(target_dir.join(".rustc_info.json")) else {
        return false;
    };
    let Ok(info) = serde_json::from_str::<serde_json::Value>(&text) else {
        return false;
    };
    info.get("outputs")
        .and_then(serde_json::Value::as_object)
        .is_some_and(|outputs| {
            outputs.values().any(|output| {
                output.get("stdout").and_then(serde_json::Value::as_str) == Some(version)
            })
        })
}

/// A Cargo lock seeding holds, released when dropped. Closing alone is not
/// enough: a child forked meanwhile keeps a duplicate descriptor until it
/// execs, and Cargo would wait for that.
struct Held(std::fs::File);

impl Drop for Held {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

/// Lock `path` exclusively without waiting, creating it if needed. `None`
/// when another process holds it or it cannot be opened.
fn try_lock(path: &Path) -> Option<Held> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .ok()?;
    file.try_lock().ok()?;
    Some(Held(file))
}

fn entries(dir: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)
        .map(|entries| entries.flatten().map(|entry| entry.path()).collect())
        .unwrap_or_default();
    paths.sort();
    paths
}

fn subdirectories(dir: &Path) -> Vec<PathBuf> {
    entries(dir)
        .into_iter()
        .filter(|path| std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_dir()))
        .collect()
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// Pass Cargo's explicit target selection to its compiler wrappers.
pub(crate) fn configure_cargo_seed(
    command: &mut std::process::Command,
    args: &[std::ffi::OsString],
    isolated_build_dir: bool,
) {
    placement::configure(command, args, isolated_build_dir);
}

/// Whether this rustc invocation should ask for a seed: seeding is on and
/// it is Cargo's target-info probe, which runs before any unit compiles.
fn asks_for_seed(enabled: bool, args: &[String]) -> bool {
    enabled && is_target_info_probe(args)
}

/// The wrapper's side: when `args` is the probe for a target directory Cargo
/// has not built, ask the daemon to seed it and wait for the answer.
///
/// See [`asks_for_seed`] for which invocations qualify.
pub(crate) fn before_probe(config: &crate::config::Config, args: &[String]) {
    if !asks_for_seed(config.seed_new_targets, args) {
        return;
    }
    let Ok(cwd) = std::env::current_dir() else {
        return;
    };
    let workspace = cwd
        .ancestors()
        .find(|dir| dir.join("Cargo.lock").is_file() && dir.join("Cargo.toml").is_file());
    let Some(workspace) = workspace else {
        return;
    };
    let target_env = placement::override_target()
        .or_else(|| std::env::var_os("CARGO_TARGET_DIR"))
        .or_else(|| std::env::var_os("CARGO_BUILD_TARGET_DIR"));
    let build_env =
        placement::override_build().or_else(|| std::env::var_os("CARGO_BUILD_BUILD_DIR"));
    let Some(directory) = placement::resolve(
        &cwd,
        workspace,
        &crate::cli::cargo_home_dir(),
        target_env.as_deref(),
        build_env.as_deref(),
    ) else {
        return;
    };
    let Some(target) = new_target(&cwd, Some(directory.as_os_str()), false) else {
        return;
    };
    let Some(version) = rustc_version(args.first().map(String::as_str)) else {
        return;
    };
    crate::daemon::send_seed_target(config, &target, &version);
}

/// `rustc -vV` from the compiler Cargo is probing.
fn rustc_version(rustc: Option<&str>) -> Option<String> {
    let output = std::process::Command::new(rustc?)
        .arg("-vV")
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    const HASH: &str = "0123456789abcdef";
    const OTHER: &str = "fedcba9876543210";
    const VERSION: &str = "rustc 1.0.0 (test)\nhost: test\n";
    /// A cache directory seeding tests without hermetic runs pass.
    const NO_CACHE: &str = "/nonexistent/kache-cache";

    fn args(line: &str) -> Vec<String> {
        line.split_whitespace().map(str::to_owned).collect()
    }

    fn write(path: &Path, text: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }

    const LOCK: &str = r#"
[[package]]
name = "app"
version = "0.1.0"

[[package]]
name = "dep"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"

[[package]]
name = "gitdep"
version = "0.1.0"
source = "git+https://example.com/gitdep#abc"
"#;

    /// A checkout at `root/name` with `LOCK`, and a donor target built by
    /// `VERSION` in `layout` holding units of `dep` and of `app`.
    fn donor(root: &Path, name: &str, layout: Layout) -> Donor {
        let workspace_root = root.join(name);
        write(
            &workspace_root.join("Cargo.toml"),
            "[package]\nname = \"app\"\n",
        );
        write(&workspace_root.join("Cargo.lock"), LOCK);
        let target_dir = workspace_root.join("target");
        let info = serde_json::json!({"outputs": {"1": {"stdout": VERSION}}});
        write(&target_dir.join(".rustc_info.json"), &info.to_string());
        let profile = target_dir.join(PROFILE);
        for (package, hash) in [("dep", HASH), ("app", OTHER)] {
            match layout {
                Layout::PerUnit => {
                    let unit = profile.join("build").join(package).join(hash);
                    write(&unit.join("fingerprint/lib"), "fingerprint");
                    write(&unit.join("out/lib.rlib"), "rlib");
                }
                Layout::Shared => {
                    let named = format!("{package}-{hash}");
                    write(&profile.join(".fingerprint").join(&named).join("lib"), "fp");
                    write(
                        &profile
                            .join("deps")
                            .join(format!("lib{package}-{hash}.rlib")),
                        "rlib",
                    );
                    write(
                        &profile.join("deps").join(format!("{package}-{hash}.d")),
                        "d",
                    );
                    write(
                        &profile.join("build").join(&named).join("output"),
                        "cargo:rustc-cfg=x\n",
                    );
                }
            }
        }
        Donor {
            target_dir,
            workspace_root,
            rustc: None,
        }
    }

    fn checkout(root: &Path, name: &str) -> NewTarget {
        let workspace_root = root.join(name);
        write(
            &workspace_root.join("Cargo.toml"),
            "[package]\nname = \"app\"\n",
        );
        write(&workspace_root.join("Cargo.lock"), LOCK);
        NewTarget {
            target_dir: workspace_root.join("target"),
            workspace_root,
        }
    }

    fn later() -> Instant {
        Instant::now() + Duration::from_secs(60)
    }

    /// kache's own record of a donor's compiler decides, trimmed; Cargo's
    /// rustc info cache is the fallback when there is no record.
    #[test]
    fn a_donors_recorded_compiler_decides_before_cargos_cache() {
        let dir = tempfile::tempdir().unwrap();
        let mut recorded = donor(dir.path(), "recorded", Layout::Shared);
        std::fs::remove_file(recorded.target_dir.join(".rustc_info.json")).unwrap();
        assert!(!donor_built_by(&recorded, VERSION), "no record, no cache");
        recorded.rustc = Some(VERSION.trim().to_string());
        assert!(donor_built_by(&recorded, VERSION));
        recorded.rustc = Some("rustc 2.0.0".into());
        assert!(!donor_built_by(&recorded, VERSION));

        let cached = donor(dir.path(), "cached", Layout::Shared);
        assert!(
            donor_built_by(&cached, VERSION),
            "Cargo's cache, without a record"
        );
        let mut contradicted = cached.clone();
        contradicted.rustc = Some("rustc 2.0.0".into());
        assert!(
            !donor_built_by(&contradicted, VERSION),
            "a record overrides Cargo's cache"
        );
    }

    #[test]
    fn only_the_target_info_probe_asks_for_a_seed_and_only_when_enabled() {
        let probe = args("rustc - --crate-name ___ --print=file-names --crate-type bin");
        assert!(asks_for_seed(true, &probe));
        assert!(!asks_for_seed(false, &probe));
        assert!(!asks_for_seed(true, &args("rustc -vV")));
    }

    #[test]
    fn a_per_unit_donor_gives_only_hash_named_units_with_a_fingerprint() {
        let dir = tempfile::tempdir().unwrap();
        let profile = dir.path().join(PROFILE);
        let build = profile.join("build").join("dep");
        write(&build.join(HASH).join("fingerprint/lib"), "fp");
        write(&build.join(OTHER).join("out/lib.rlib"), "no fingerprint");
        write(&build.join("not-a-hash/fingerprint/lib"), "fp");
        let packages: BTreeSet<String> = ["dep".to_string()].into();
        let found: Vec<(String, String)> = units(&profile, Layout::PerUnit, &packages)
            .into_iter()
            .map(|unit| (unit.package, unit.hash))
            .collect();
        assert_eq!(found, [("dep".to_string(), HASH.to_string())]);
    }

    #[test]
    fn recognizes_cargos_target_info_probe() {
        let rustc = "rustc - --crate-name ___ --print=file-names --crate-type bin";
        assert!(is_target_info_probe(&args(rustc)));
        assert!(is_target_info_probe(&args(
            "rustc - --crate-name ___ --print file-names"
        )));
        for other in [
            "rustc -vV",
            "rustc - --crate-name app --print=file-names",
            "rustc - --crate-name ___ --print=cfg",
            "rustc - --crate-name ___ --print cfg",
            "rustc - --crate-name ___",
            "rustc --print=file-names ___",
            "rustc - --print file-names --crate-name",
        ] {
            assert!(!is_target_info_probe(&args(other)), "{other}");
        }
    }

    #[test]
    fn finds_the_new_target_directory() {
        let dir = tempfile::tempdir().unwrap();
        let checkout = checkout(dir.path(), "b");
        let member = checkout.workspace_root.join("crates/member");
        std::fs::create_dir_all(&member).unwrap();
        write(&member.join("Cargo.toml"), "");
        assert_eq!(new_target(&member, None, false), Some(checkout.clone()));
        assert_eq!(new_target(&checkout.workspace_root, None, true), None);
        let env = std::ffi::OsString::from("elsewhere/t");
        assert_eq!(
            new_target(&member, Some(&env), false).unwrap().target_dir,
            member.join("elsewhere/t")
        );
        let empty = std::ffi::OsString::new();
        assert_eq!(
            new_target(&member, Some(&empty), false),
            Some(checkout.clone())
        );
        // No lockfile anywhere up: nothing to seed from.
        assert_eq!(new_target(dir.path(), None, false), None);
        // Cargo has built there already.
        for built in [".rustc_info.json", "debug/.fingerprint", "debug/build"] {
            let target = checkout.target_dir.join(built);
            std::fs::create_dir_all(&target).unwrap();
            assert_eq!(new_target(&member, None, false), None, "{built}");
            std::fs::remove_dir_all(&checkout.target_dir).unwrap();
        }
        assert!(new_target(&member, None, false).is_some());
    }

    #[test]
    fn copies_only_packages_with_a_source() {
        let packages = registry_packages(LOCK);
        assert_eq!(
            packages.into_iter().collect::<Vec<_>>(),
            vec!["dep".to_owned(), "gitdep".to_owned()]
        );
        // A path package sharing a name keeps that name out.
        let shadowed = format!("{LOCK}\n[[package]]\nname = \"dep\"\nversion = \"2.0.0\"\n");
        assert_eq!(
            registry_packages(&shadowed).into_iter().collect::<Vec<_>>(),
            vec!["gitdep".to_owned()]
        );
        assert!(registry_packages("not toml [").is_empty());
        assert!(registry_packages("version = 3\n").is_empty());
        assert!(registry_packages("[[package]]\nsource = \"x\"\n").is_empty());
    }

    #[test]
    fn seeds_editor_and_check_targets_only_from_the_matching_lane() {
        let dir = tempfile::tempdir().unwrap();
        for lane in ["check", "rust-analyzer"] {
            for layout in [Layout::Shared, Layout::PerUnit] {
                let label = format!("{lane}-{layout:?}");
                let wrong = donor(dir.path(), &format!("wrong-{label}"), layout);
                let mut matching = donor(dir.path(), &format!("matching-{label}"), layout);
                let old = matching.target_dir.clone();
                matching.target_dir = matching.workspace_root.join("elsewhere").join(lane);
                std::fs::create_dir_all(matching.target_dir.parent().unwrap()).unwrap();
                std::fs::rename(&old, &matching.target_dir).unwrap();
                let mut target = checkout(dir.path(), &format!("new-{label}"));
                target.target_dir = target.target_dir.join(lane);
                let result = seed(
                    &target,
                    VERSION,
                    &[wrong, matching.clone()],
                    Path::new(NO_CACHE),
                    later(),
                );
                assert_eq!(result.donor, Some(matching.workspace_root.clone()));
                assert_eq!(result.units, 1);
                assert!(
                    marker(
                        &target.target_dir.join(PROFILE),
                        layout,
                        &Unit {
                            package: "dep".into(),
                            hash: HASH.into()
                        }
                    )
                    .is_dir()
                );
                assert!(!target.workspace_root.join("target/debug").exists());
                let regular = checkout(dir.path(), &format!("regular-{label}"));
                assert_eq!(
                    seed(&regular, VERSION, &[matching], Path::new(NO_CACHE), later()),
                    Seeded::default()
                );
            }
        }
    }

    #[test]
    fn seeds_both_layouts_and_keeps_modification_times() {
        for layout in [Layout::PerUnit, Layout::Shared] {
            let dir = tempfile::tempdir().unwrap();
            let donor = donor(dir.path(), "a", layout);
            let from = donor.target_dir.join(PROFILE);
            let old = filetime::FileTime::from_unix_time(1_000_000_000, 0);
            let marker_from = marker(&from, layout, &unit("dep", HASH));
            for file in walk(&from) {
                filetime::set_file_mtime(&file, old).unwrap();
            }
            let new = checkout(dir.path(), "b");
            let seeded = seed(
                &new,
                VERSION,
                std::slice::from_ref(&donor),
                Path::new(NO_CACHE),
                later(),
            );
            assert_eq!(
                seeded,
                Seeded {
                    units: 1,
                    donor: Some(donor.workspace_root.clone())
                },
                "{layout:?}"
            );
            let to = new.target_dir.join(PROFILE);
            assert!(
                marker(&to, layout, &unit("dep", HASH)).is_dir(),
                "{layout:?}"
            );
            // The path package is never copied.
            assert!(
                !marker(&to, layout, &unit("app", OTHER)).exists(),
                "{layout:?}"
            );
            let copied: Vec<_> = walk(&to)
                .into_iter()
                .filter(|file| !file.ends_with(".cargo-lock"))
                .collect();
            let expected = match layout {
                Layout::PerUnit => 2,
                Layout::Shared => 4,
            };
            assert_eq!(copied.len(), expected, "{layout:?}: {copied:?}");
            for file in &copied {
                let mtime = filetime::FileTime::from_last_modification_time(
                    &std::fs::metadata(file).unwrap(),
                );
                assert_eq!(mtime, old, "{}", file.display());
                assert!(!file.to_string_lossy().contains(STAGING));
            }
            assert!(marker_from.is_dir(), "the donor keeps its units");
            // A second seeding finds the unit present and copies nothing.
            assert_eq!(
                seed(
                    &new,
                    VERSION,
                    std::slice::from_ref(&donor),
                    Path::new(NO_CACHE),
                    later()
                ),
                Seeded::default()
            );
        }
    }

    fn unit(package: &str, hash: &str) -> Unit {
        Unit {
            package: package.into(),
            hash: hash.into(),
        }
    }

    fn walk(dir: &Path) -> Vec<PathBuf> {
        let mut files = Vec::new();
        for entry in entries(dir) {
            if entry.is_dir() {
                files.extend(walk(&entry));
            } else {
                files.push(entry);
            }
        }
        files
    }

    #[test]
    fn skips_donors_that_cannot_give_this_build_its_units() {
        let dir = tempfile::tempdir().unwrap();
        let new = checkout(dir.path(), "b");
        let wrong_rustc = donor(dir.path(), "a", Layout::Shared);
        // Built by another compiler.
        assert_eq!(
            seed(
                &new,
                "rustc 9.9.9\n",
                std::slice::from_ref(&wrong_rustc),
                Path::new(NO_CACHE),
                later()
            ),
            Seeded::default()
        );
        assert_eq!(
            seed(
                &new,
                "  ",
                std::slice::from_ref(&wrong_rustc),
                Path::new(NO_CACHE),
                later()
            ),
            Seeded::default()
        );
        // A blank version is not a compiler, even where a donor recorded one.
        let blank = donor(dir.path(), "blank", Layout::Shared);
        let info = serde_json::json!({"outputs": {"1": {"stdout": "  "}}});
        write(
            &blank.target_dir.join(".rustc_info.json"),
            &info.to_string(),
        );
        assert_eq!(
            seed(
                &new,
                "  ",
                std::slice::from_ref(&blank),
                Path::new(NO_CACHE),
                later()
            ),
            Seeded::default()
        );
        // A build holds the donor's lock: the next donor gives the units.
        let busy = donor(dir.path(), "busy", Layout::Shared);
        let held = try_lock(&busy.target_dir.join(PROFILE).join(".cargo-lock")).unwrap();
        let free = donor(dir.path(), "free", Layout::PerUnit);
        let seeded = seed(
            &new,
            VERSION,
            &[busy.clone(), free.clone()],
            Path::new(NO_CACHE),
            later(),
        );
        assert_eq!(seeded.donor, Some(free.workspace_root.clone()));
        drop(held);
        // The target itself, and a donor with no units, give nothing.
        let other = checkout(dir.path(), "c");
        let own = Donor {
            target_dir: other.target_dir.clone(),
            workspace_root: other.workspace_root.clone(),
            rustc: None,
        };
        let empty = donor(dir.path(), "empty", Layout::Shared);
        std::fs::remove_dir_all(empty.target_dir.join(PROFILE)).unwrap();
        std::fs::create_dir_all(empty.target_dir.join(PROFILE)).unwrap();
        assert_eq!(
            seed(&other, VERSION, &[own, empty], Path::new(NO_CACHE), later()),
            Seeded::default()
        );
        // Past the deadline nothing is copied.
        let late = checkout(dir.path(), "late");
        assert_eq!(
            seed(
                &late,
                VERSION,
                std::slice::from_ref(&free),
                Path::new(NO_CACHE),
                Instant::now()
            ),
            Seeded::default()
        );
        // A build holding the new target's lock is left alone.
        let locked = checkout(dir.path(), "locked");
        std::fs::create_dir_all(locked.target_dir.join(PROFILE)).unwrap();
        let ours = try_lock(&locked.target_dir.join(PROFILE).join(".cargo-lock")).unwrap();
        assert_eq!(
            seed(
                &locked,
                VERSION,
                std::slice::from_ref(&free),
                Path::new(NO_CACHE),
                later()
            ),
            Seeded::default()
        );
        drop(ours);
        // Without a lockfile, or with only path packages, there is nothing to copy.
        let bare = checkout(dir.path(), "bare");
        std::fs::remove_file(bare.workspace_root.join("Cargo.lock")).unwrap();
        assert_eq!(
            seed(
                &bare,
                VERSION,
                std::slice::from_ref(&free),
                Path::new(NO_CACHE),
                later()
            ),
            Seeded::default()
        );
        write(
            &bare.workspace_root.join("Cargo.lock"),
            "[[package]]\nname = \"app\"\n",
        );
        assert_eq!(
            seed(&bare, VERSION, &[free], Path::new(NO_CACHE), later()),
            Seeded::default()
        );
    }

    fn script_stdout(profile: &Path, layout: Layout) -> PathBuf {
        match layout {
            Layout::PerUnit => profile.join("build/dep").join(HASH).join("run/stdout"),
            Layout::Shared => profile.join("build").join(format!("dep-{HASH}/output")),
        }
    }

    fn script_root_output(profile: &Path, layout: Layout) -> PathBuf {
        script_stdout(profile, layout).with_file_name("root-output")
    }

    fn cmake_cache(profile: &Path, layout: Layout) -> PathBuf {
        build_script_out_dir(profile, layout, &unit("dep", HASH)).join("build/CMakeCache.txt")
    }

    /// Record a build script whose stdout is only its own `OUT_DIR`.
    fn record_out_dir_stdout(profile: &Path, layout: Layout) -> PathBuf {
        let out_dir = build_script_out_dir(profile, layout, &unit("dep", HASH));
        let recorded = out_dir.display().to_string();
        write(
            &script_stdout(profile, layout),
            &format!("cargo:rustc-link-search={recorded}\n"),
        );
        write(&script_root_output(profile, layout), &recorded);
        out_dir
    }

    #[test]
    fn leaves_build_scripts_that_name_the_donor() {
        for layout in [Layout::PerUnit, Layout::Shared] {
            let dir = tempfile::tempdir().unwrap();
            let donor = donor(dir.path(), "a", layout);
            let from = donor.target_dir.join(PROFILE);
            let dep = unit("dep", HASH);
            let run = match layout {
                Layout::PerUnit => from.join("build/dep").join(HASH).join("run"),
                Layout::Shared => from.join("build").join(format!("dep-{HASH}")),
            };
            let (stdout, root) = match layout {
                Layout::PerUnit => ("stdout", "root-output"),
                Layout::Shared => ("output", "root-output"),
            };
            let out_dir = run.join("out");
            let target = donor.target_dir.display().to_string();
            let workspace = donor.workspace_root.display().to_string();
            for (text, root_output, named) in [
                (String::from("cargo:rustc-cfg=x\n"), "", false),
                (
                    format!("cargo:rustc-link-search={}\n", out_dir.display()),
                    "out",
                    false,
                ),
                (
                    format!("cargo:rustc-link-search={target}/native\n"),
                    "out",
                    true,
                ),
                (format!("cargo:rerun-if-changed={workspace}/x\n"), "", true),
            ] {
                write(&run.join(stdout), &text);
                let recorded = if root_output.is_empty() {
                    String::new()
                } else {
                    out_dir.display().to_string()
                };
                write(&run.join(root), &recorded);
                assert_eq!(
                    names_the_donor(&from, layout, &dep, &donor),
                    named,
                    "{layout:?}: {text}"
                );
            }
            std::fs::remove_file(run.join(stdout)).unwrap();
            assert!(!names_the_donor(&from, layout, &dep, &donor));
            // A unit that names the donor is not copied.
            write(
                &run.join(stdout),
                &format!("cargo:rustc-link-search={target}/x\n"),
            );
            let new = checkout(dir.path(), "b");
            assert_eq!(
                seed(
                    &new,
                    VERSION,
                    std::slice::from_ref(&donor),
                    Path::new(NO_CACHE),
                    later()
                ),
                Seeded::default()
            );
        }
    }

    #[test]
    fn an_output_file_that_names_the_donor_is_not_seeded() {
        for layout in [Layout::PerUnit, Layout::Shared] {
            let dir = tempfile::tempdir().unwrap();
            let donor = donor(dir.path(), "a", layout);
            let from = donor.target_dir.join(PROFILE);
            let dep = unit("dep", HASH);
            let out_dir = record_out_dir_stdout(&from, layout);
            // The stdout check strips OUT_DIR. The cache file is not rewritten,
            // and `OUT_DIR/build` still contains the donor target directory.
            write(
                &cmake_cache(&from, layout),
                &format!("CMAKE_CACHEFILE_DIR:INTERNAL={}/build\n", out_dir.display()),
            );
            assert!(
                !names_the_donor(&from, layout, &dep, &donor),
                "{layout:?}: stdout only names OUT_DIR"
            );
            assert!(
                out_dir_names_the_donor(&from, layout, &dep, &donor),
                "{layout:?}"
            );
            let new = checkout(dir.path(), "b");
            assert_eq!(
                seed(
                    &new,
                    VERSION,
                    std::slice::from_ref(&donor),
                    Path::new(NO_CACHE),
                    later()
                ),
                Seeded::default(),
                "{layout:?}"
            );
            assert!(
                !marker(&new.target_dir.join(PROFILE), layout, &dep).exists(),
                "{layout:?}"
            );
        }
    }

    #[test]
    fn an_output_file_that_does_not_name_the_donor_is_seeded() {
        for layout in [Layout::PerUnit, Layout::Shared] {
            let dir = tempfile::tempdir().unwrap();
            let donor = donor(dir.path(), "a", layout);
            let from = donor.target_dir.join(PROFILE);
            let dep = unit("dep", HASH);
            record_out_dir_stdout(&from, layout);
            let cache = "portable-cache\n";
            write(&cmake_cache(&from, layout), cache);
            assert!(
                !out_dir_names_the_donor(&from, layout, &dep, &donor),
                "{layout:?}"
            );
            let new = checkout(dir.path(), "b");
            assert_eq!(
                seed(
                    &new,
                    VERSION,
                    std::slice::from_ref(&donor),
                    Path::new(NO_CACHE),
                    later()
                ),
                Seeded {
                    units: 1,
                    donor: Some(donor.workspace_root.clone()),
                },
                "{layout:?}"
            );
            let copied = cmake_cache(&new.target_dir.join(PROFILE), layout);
            assert_eq!(
                std::fs::read_to_string(&copied).unwrap(),
                cache,
                "{layout:?}"
            );
        }
    }

    #[test]
    fn each_donor_path_is_enough_and_a_long_file_is_not_read() {
        for layout in [Layout::PerUnit, Layout::Shared] {
            let dir = tempfile::tempdir().unwrap();
            let donor = donor(dir.path(), "a", layout);
            let from = donor.target_dir.join(PROFILE);
            let dep = unit("dep", HASH);
            write(&script_stdout(&from, layout), "cargo:rustc-cfg=x\n");
            let cache = cmake_cache(&from, layout);

            let mut named = donor.clone();
            named.target_dir = PathBuf::from("/needle-target");
            named.workspace_root = PathBuf::from("/needle-workspace");
            write(&cache, "see /needle-workspace/src\n");
            assert!(
                out_dir_names_the_donor(&from, layout, &dep, &named),
                "{layout:?} workspace"
            );
            write(&cache, "see /needle-target/debug\n");
            assert!(
                out_dir_names_the_donor(&from, layout, &dep, &named),
                "{layout:?} target"
            );
            write(&cache, "see /neither\n");
            assert!(
                !out_dir_names_the_donor(&from, layout, &dep, &named),
                "{layout:?}"
            );
            // Longer than both needles, and neither needle is in it. A
            // shorter miss makes `windows` empty, so `==` and `!=` agree.
            write(&cache, "xxxx see /neither xxxxxxxxx\n");
            assert!(
                !out_dir_names_the_donor(&from, layout, &dep, &named),
                "{layout:?}: a longer file that names neither path"
            );

            let mut blank = donor.clone();
            blank.target_dir.clear();
            blank.workspace_root.clear();
            write(&cache, "x\n");
            assert!(
                !out_dir_names_the_donor(&from, layout, &dep, &blank),
                "{layout:?} empty"
            );

            let needle = donor.target_dir.to_string_lossy().into_owned();
            let mut exact = vec![b'a'; 1024 * 1024];
            exact[..needle.len()].copy_from_slice(needle.as_bytes());
            std::fs::write(&cache, &exact).unwrap();
            assert!(
                out_dir_names_the_donor(&from, layout, &dep, &donor),
                "{layout:?}: a file at the scan limit is read"
            );
            let mut over = vec![b'a'; 1024 * 1024 + 1];
            over[..needle.len()].copy_from_slice(needle.as_bytes());
            std::fs::write(&cache, &over).unwrap();
            assert!(
                !out_dir_names_the_donor(&from, layout, &dep, &donor),
                "{layout:?}: a longer file is not read"
            );

            std::fs::remove_file(script_stdout(&from, layout)).unwrap();
            write(&cache, &format!("{needle}\n"));
            assert!(
                !out_dir_names_the_donor(&from, layout, &dep, &donor),
                "{layout:?}: no script record, so out/ is the unit's artifact"
            );

            write(&script_stdout(&from, layout), "cargo:rustc-cfg=x\n");
            write(&cache, &format!("{needle}\n"));
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let out = build_script_out_dir(&from, layout, &dep);
                std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o000)).unwrap();
                assert!(
                    std::fs::read(&cache).is_err(),
                    "{layout:?}: mode 000 must be unreadable here"
                );
                assert!(
                    out_dir_names_the_donor(&from, layout, &dep, &donor),
                    "{layout:?}: an unreadable output file is not copied"
                );
                std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o644)).unwrap();

                std::fs::set_permissions(&out, std::fs::Permissions::from_mode(0o000)).unwrap();
                assert!(
                    out_dir_names_the_donor(&from, layout, &dep, &donor),
                    "{layout:?}: an unlisted out/ is not copied"
                );
                std::fs::set_permissions(&out, std::fs::Permissions::from_mode(0o755)).unwrap();

                let outside = dir.path().join("outside.txt");
                write(&outside, &needle);
                write(&cache, "portable-cache\n");
                let link = out.join("linked-cache");
                std::os::unix::fs::symlink(&outside, &link).unwrap();
                assert!(
                    !out_dir_names_the_donor(&from, layout, &dep, &donor),
                    "{layout:?}: a symlink is not followed"
                );
            }
        }
    }

    #[test]
    fn a_unit_that_cannot_be_copied_leaves_nothing_behind() {
        let dir = tempfile::tempdir().unwrap();
        let donor = donor(dir.path(), "a", Layout::PerUnit);
        let unit_dir = donor.target_dir.join(PROFILE).join("build/dep").join(HASH);
        #[cfg(unix)]
        std::os::unix::fs::symlink("/", unit_dir.join("link")).unwrap();
        #[cfg(not(unix))]
        std::fs::remove_dir_all(unit_dir.join("out")).unwrap();
        let new = checkout(dir.path(), "b");
        let seeded = seed(
            &new,
            VERSION,
            std::slice::from_ref(&donor),
            Path::new(NO_CACHE),
            later(),
        );
        #[cfg(unix)]
        {
            assert_eq!(seeded, Seeded::default());
            let package = new.target_dir.join(PROFILE).join("build/dep");
            assert_eq!(entries(&package), Vec::<PathBuf>::new());
        }
        #[cfg(not(unix))]
        let _ = seeded;
    }

    /// rdkafka-sys leaves `librdkafka.so -> librdkafka.so.1` in its
    /// `OUT_DIR`. A link that stays inside the unit is copied as the same
    /// link; one that leaves it, or is the whole unit, fails the copy.
    #[cfg(unix)]
    #[test]
    fn a_link_inside_the_unit_is_copied_as_a_link() {
        let dir = tempfile::tempdir().unwrap();
        let from = dir.path().join("unit");
        write(&from.join("out/src/librdkafka.so.1"), "so");
        std::os::unix::fs::symlink("librdkafka.so.1", from.join("out/src/librdkafka.so")).unwrap();
        write(&from.join("out/include/rdkafka.h"), "h");
        std::os::unix::fs::symlink("../include", from.join("out/src/include")).unwrap();
        let to = dir.path().join("copy");
        place_tree(&from, &to, Path::new(NO_CACHE), later()).unwrap();
        let link = to.join("out/src/librdkafka.so");
        assert_eq!(
            std::fs::read_link(&link).unwrap(),
            Path::new("librdkafka.so.1")
        );
        assert_eq!(std::fs::read_to_string(&link).unwrap(), "so");
        assert_eq!(
            std::fs::read_to_string(to.join("out/src/include/rdkafka.h")).unwrap(),
            "h"
        );

        std::os::unix::fs::symlink("../../../elsewhere", from.join("out/src/up")).unwrap();
        assert!(
            place_tree(
                &from,
                &dir.path().join("escaped"),
                Path::new(NO_CACHE),
                later()
            )
            .is_err()
        );
        assert!(!dir.path().join("escaped").exists());

        let linked_unit = dir.path().join("linked-unit");
        std::os::unix::fs::symlink("unit", &linked_unit).unwrap();
        assert!(
            place_tree(
                &linked_unit,
                &dir.path().join("whole"),
                Path::new(NO_CACHE),
                later()
            )
            .is_err()
        );
    }

    /// A hermetic run leaves `OUT_DIR` as an absolute link to its sealed,
    /// shared run. Seeding with the cache that holds the run links the new
    /// unit's `OUT_DIR` to it and records the link; with any other cache the
    /// unit is left out. Both layouts.
    #[cfg(unix)]
    #[test]
    fn a_link_to_a_sealed_hermetic_run_is_linked_again_and_recorded() {
        use crate::build_script::hermetic_test_support::{linked_to, sealed_run};
        const RUN: &str = "1111222233334444";
        for layout in [Layout::PerUnit, Layout::Shared] {
            let dir = tempfile::tempdir().unwrap();
            let donor = donor(dir.path(), "a", layout);
            let from = donor.target_dir.join(PROFILE);
            let run = match layout {
                Layout::PerUnit => {
                    let run = from.join("build/dep").join(RUN);
                    write(&run.join("fingerprint/run-build-script"), "fp");
                    write(&run.join("run/stdout"), "cargo:rerun-if-changed=build.rs\n");
                    run
                }
                Layout::Shared => {
                    let named = format!("dep-{RUN}");
                    write(&from.join(".fingerprint").join(&named).join("run"), "fp");
                    let run = from.join("build").join(&named);
                    write(&run.join("output"), "cargo:rerun-if-changed=build.rs\n");
                    run
                }
            };
            let below = run.join("out");
            let below = below.strip_prefix(&donor.target_dir).unwrap();
            let cache = dir.path().join("cache");
            let shared = sealed_run(&cache, &"ab".repeat(32), below);
            std::os::unix::fs::symlink(&shared, run.join("out")).unwrap();
            let elsewhere = dir.path().join("other-cache");
            let new = checkout(dir.path(), "b");
            let seeded = seed(
                &new,
                VERSION,
                std::slice::from_ref(&donor),
                &elsewhere,
                later(),
            );
            assert_eq!(seeded.units, 1, "{layout:?}: only the lib unit");
            let to = new.target_dir.join(below);
            assert!(std::fs::symlink_metadata(&to).is_err(), "{layout:?}");
            assert!(
                !marker(&new.target_dir.join(PROFILE), layout, &unit("dep", RUN)).exists(),
                "{layout:?}"
            );

            let new = checkout(dir.path(), "c");
            let seeded = seed(&new, VERSION, std::slice::from_ref(&donor), &cache, later());
            assert_eq!(seeded.units, 2, "{layout:?}: the lib and run units");
            let to = new.target_dir.join(below);
            assert_eq!(std::fs::read_link(&to).unwrap(), shared, "{layout:?}");
            assert_eq!(
                std::fs::read_to_string(to.join("gen.rs")).unwrap(),
                "pub const N: u8 = 1;"
            );
            assert_eq!(linked_to(&shared, below), vec![to.clone()], "{layout:?}");
        }
    }

    #[test]
    fn a_deps_file_is_indexed_by_the_unit_hash_in_its_name() {
        let hash = "0123456789abcdef";
        for name in [
            format!("libfoo_bar-{hash}.rlib"),
            format!("libfoo_bar-{hash}.so.1"),
            format!("foo-bar-{hash}"),
            format!("foo_bar-{hash}.d"),
        ] {
            assert_eq!(unit_hashes(&name).collect::<Vec<_>>(), vec![hash], "{name}");
        }
        for name in [
            format!("foo-{hash}0.rlib"),
            format!("foo-{hash}x"),
            "foo-0123456789abcdeg.rlib".to_owned(),
            format!("foo_{hash}.rlib"),
        ] {
            assert!(unit_hashes(&name).next().is_none(), "{name}");
        }

        let dir = tempfile::tempdir().unwrap();
        for name in [
            format!("libfoo-{hash}.rlib"),
            format!("foo-{hash}.d"),
            "other.d".to_owned(),
        ] {
            write(&dir.path().join(name), "x");
        }
        let index = deps_by_hash(dir.path());
        assert_eq!(index.len(), 1);
        assert_eq!(
            index[hash],
            vec![
                dir.path().join(format!("foo-{hash}.d")),
                dir.path().join(format!("libfoo-{hash}.rlib"))
            ]
        );
    }

    #[test]
    fn a_passed_deadline_stops_a_copy_and_leaves_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let from = dir.path().join("unit");
        write(&from.join("a/file"), "x");
        let to = dir.path().join("copy");
        let error = place_tree(&from, &to, Path::new(NO_CACHE), Instant::now()).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        assert_eq!(entries(dir.path()), vec![from.clone()]);
        place_tree(&from, &to, Path::new(NO_CACHE), later()).unwrap();
        assert_eq!(std::fs::read_to_string(to.join("a/file")).unwrap(), "x");
    }

    /// A target directory seeding creates carries Cargo's tag, so kache
    /// tracks it like one Cargo created, even when nothing was seeded.
    #[test]
    fn a_target_seeding_creates_is_tagged_like_cargos() {
        let dir = tempfile::tempdir().unwrap();
        let new = checkout(dir.path(), "b");
        assert_eq!(
            seed(&new, VERSION, &[], Path::new(NO_CACHE), later()),
            Seeded::default()
        );
        let tag = std::fs::read_to_string(new.target_dir.join("CACHEDIR.TAG")).unwrap();
        assert_eq!(tag, CARGO_CACHEDIR_TAG);
        assert!(crate::machine::target_root_is_safe(
            &new.target_dir,
            &new.workspace_root
        ));
    }

    /// Cargo does not tag a target directory that already exists, and
    /// neither does seeding.
    #[test]
    fn an_existing_untagged_target_stays_untagged() {
        let dir = tempfile::tempdir().unwrap();
        let new = checkout(dir.path(), "b");
        std::fs::create_dir_all(&new.target_dir).unwrap();
        seed(&new, VERSION, &[], Path::new(NO_CACHE), later());
        assert!(new.target_dir.join(PROFILE).is_dir());
        assert!(!new.target_dir.join("CACHEDIR.TAG").exists());
    }

    #[cfg(unix)]
    #[test]
    fn a_copied_file_takes_the_given_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let from = dir.path().join("build-script-build");
        write(&from, "binary");
        std::fs::set_permissions(&from, std::fs::Permissions::from_mode(0o644)).unwrap();
        let recorded = dir.path().join("recorded");
        write(&recorded, "");
        std::fs::set_permissions(&recorded, std::fs::Permissions::from_mode(0o751)).unwrap();
        let to = dir.path().join("copy");
        copy_file(&from, &to, &std::fs::metadata(&recorded).unwrap()).unwrap();
        let mode = std::fs::metadata(&to).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o751);
        assert_eq!(std::fs::read_to_string(&to).unwrap(), "binary");
    }

    #[cfg(windows)]
    #[test]
    fn a_copied_file_can_be_made_readonly_after_setting_its_mtime() {
        let dir = tempfile::tempdir().unwrap();
        let from = dir.path().join("source");
        write(&from, "binary");
        let recorded = dir.path().join("recorded");
        write(&recorded, "");
        let mut permissions = std::fs::metadata(&recorded).unwrap().permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(&recorded, permissions).unwrap();
        let to = dir.path().join("copy");
        copy_file(&from, &to, &std::fs::metadata(&recorded).unwrap()).unwrap();
        assert!(std::fs::metadata(&to).unwrap().permissions().readonly());
        assert_eq!(std::fs::read_to_string(&to).unwrap(), "binary");
    }

    #[test]
    fn a_target_built_while_waiting_for_its_lock_is_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let donor = donor(dir.path(), "a", Layout::Shared);
        let new = checkout(dir.path(), "b");
        write(&new.target_dir.join(".rustc_info.json"), "{}");
        assert_eq!(
            seed(
                &new,
                VERSION,
                std::slice::from_ref(&donor),
                Path::new(NO_CACHE),
                later()
            ),
            Seeded::default()
        );
        assert!(!new.target_dir.join(PROFILE).join(".fingerprint").exists());
    }

    #[test]
    fn reads_the_compiler_a_target_was_built_by() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path();
        assert!(!built_by(target, VERSION));
        write(&target.join(".rustc_info.json"), "not json");
        assert!(!built_by(target, VERSION));
        write(&target.join(".rustc_info.json"), "{\"outputs\": 3}");
        assert!(!built_by(target, VERSION));
        let info =
            serde_json::json!({"outputs": {"a": {"stdout": "other"}, "b": {"stdout": VERSION}}});
        write(&target.join(".rustc_info.json"), &info.to_string());
        assert!(built_by(target, VERSION));
        assert!(!built_by(target, "rustc 2"));
    }

    #[test]
    fn locks_exclusively() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".cargo-lock");
        let first = try_lock(&path).unwrap();
        assert!(try_lock(&path).is_none());
        // Stands in for a child forked while the lock was held.
        let duplicate = first.0.try_clone().unwrap();
        drop(first);
        assert!(try_lock(&path).is_some(), "the lock was not released");
        drop(duplicate);
        assert!(try_lock(&dir.path().join("missing/.cargo-lock")).is_none());
    }

    #[test]
    fn asks_the_probed_compiler_for_its_version() {
        let version = rustc_version(Some("rustc")).expect("rustc on the test PATH");
        assert!(version.starts_with("rustc "), "{version}");
        assert!(version.contains("host: "), "{version}");
        assert_eq!(rustc_version(None), None);
        assert_eq!(rustc_version(Some("/nonexistent/rustc")), None);
        #[cfg(unix)]
        assert_eq!(rustc_version(Some("false")), None);
        assert_eq!(SEED_DEADLINE, Duration::from_secs(3));
        assert_eq!(seed_wait(), Duration::from_secs(5));
    }
}
