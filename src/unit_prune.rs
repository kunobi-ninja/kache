//! Build units a live target directory no longer uses.
//!
//! Cargo keeps every unit it ever built under a profile: a lockfile update, a
//! feature change or a new toolchain moves the build on to new units and
//! leaves the old ones behind. Cargo reads the fingerprint of every unit a
//! build uses, even when nothing needs compiling.
//!
//! A read shows up only if the filesystem records it. Linux's `relatime` and
//! macOS both refresh an access time on a read when it is older than the
//! file's modification time, and otherwise at most daily (Linux) or never
//! (macOS). So the fingerprints are armed: each file's access time is set
//! just below its modification time, and the moment is recorded. Any build
//! that uses the unit afterwards reads the fingerprint and moves the access
//! time past that moment. Cargo judges freshness by modification times only,
//! so arming changes nothing it looks at.
//!
//! [`prune`] arms a target directory the first time it sees it. Once the
//! configured window has passed, it removes every unit whose fingerprint was
//! neither read nor written since, and arms the rest again. It does nothing
//! on a filesystem where reads never move an access time (`noatime`), or on
//! one that is not local, where Cargo does not lock its build directories.
//! When the probe for reads cannot run, as on a full volume, the target
//! keeps its arming and waits for the next pass.
//!
//! Both of Cargo's layouts are handled (see [`crate::cargo_layout`]):
//!
//! - From Cargo 1.100, a unit is `<profile>/build/<pkg>/<hash>/`, with its
//!   fingerprint in `fingerprint/`.
//! - Before, a unit's fingerprint is `<profile>/.fingerprint/<pkg>-<hash>/`,
//!   a build script's directory `<profile>/build/<pkg>-<hash>/`, and its
//!   outputs are named `<crate>-<hash>…` in `deps/` and `examples/`. A file
//!   there goes only when both its hash and its crate name match the unit.
//!
//! Every lock Cargo takes in a profile directory is held while its units are
//! removed, so a build waits rather than lose a unit it just found fresh. The
//! fingerprint goes first, so a removal cut short leaves outputs Cargo
//! rebuilds rather than a fingerprint it trusts. A unit that is needed after
//! all is restored from the cache or compiled again.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

/// The locks Cargo takes in a profile directory: `.cargo-lock` in every
/// version, and the build and artifact locks from 1.100.
const LOCKS: [&str; 3] = [".cargo-lock", ".cargo-build-lock", ".cargo-artifact-lock"];

/// Name of the file [`probe_reads`] reads.
const PROBE: &str = ".kache-atime-probe";

/// Directory under the cache dir holding when each target was armed.
const ARMED_DIR: &str = "unit-prune";

/// What [`prune`] removed from one target directory.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub(crate) struct Pruned {
    pub(crate) units: usize,
    pub(crate) bytes: u64,
    /// Units kept because a build read or wrote them since the last arming.
    #[serde(skip)]
    pub(crate) used: usize,
}

/// Whether reading a file in `dir` whose access time is older than its
/// modification time moves the access time, which is what arming relies on.
/// An error means the probe could not run: a full volume refuses its file,
/// which says nothing about reads.
fn probe_reads(dir: &Path) -> std::io::Result<bool> {
    let probe = dir.join(format!("{PROBE}-{}", std::process::id()));
    let moved = (|| -> std::io::Result<bool> {
        std::fs::write(&probe, b"probe")?;
        let armed = arm_file(&probe)?;
        std::fs::read(&probe)?;
        let accessed = filetime::FileTime::from_last_access_time(&std::fs::metadata(&probe)?);
        Ok(moved_past(accessed, armed))
    })();
    let _ = std::fs::remove_file(&probe);
    moved
}

/// [`probe_reads`], counting a probe that could not run as no.
#[cfg(test)]
pub(crate) fn reads_visible(dir: &Path) -> bool {
    probe_reads(dir).unwrap_or(false)
}

/// Whether `accessed` is later than the `armed` access time: a read moved it.
fn moved_past(accessed: filetime::FileTime, armed: filetime::FileTime) -> bool {
    accessed > armed
}

/// Set `file`'s access time a second below its modification time, and
/// return it.
fn arm_file(file: &Path) -> std::io::Result<filetime::FileTime> {
    let modified = filetime::FileTime::from_last_modification_time(&std::fs::metadata(file)?);
    let armed = filetime::FileTime::from_unix_time(modified.unix_seconds() - 1, 0);
    filetime::set_file_atime(file, armed)?;
    Ok(armed)
}

/// Arm every file of `fingerprint`.
fn arm(fingerprint: &Path) {
    for file in entries(fingerprint) {
        let _ = arm_file(&file);
    }
}

/// Whether any file of `fingerprint` was read or written after `armed`. An
/// unreadable file counts as used.
pub(crate) fn used_since(fingerprint: &Path, armed: SystemTime) -> bool {
    let files = entries(fingerprint);
    if files.is_empty() {
        return std::fs::metadata(fingerprint)
            .and_then(|metadata| metadata.modified())
            .map_or(true, |modified| modified > armed);
    }
    files.iter().any(|file| {
        std::fs::metadata(file).map_or(true, |metadata| {
            [metadata.accessed(), metadata.modified()]
                .into_iter()
                .any(|time| time.map_or(true, |time| time > armed))
        })
    })
}

/// One unit of a profile directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Unit {
    /// `build/<pkg>/<hash>/`, fingerprint in `fingerprint/`.
    PerUnit { dir: PathBuf },
    /// `.fingerprint/<pkg>-<hash>/` and the files named by its hash.
    Shared {
        profile: PathBuf,
        package: String,
        hash: String,
    },
}

impl Unit {
    pub(crate) fn fingerprint(&self) -> PathBuf {
        match self {
            Unit::PerUnit { dir } => dir.join("fingerprint"),
            Unit::Shared {
                profile,
                package,
                hash,
            } => profile
                .join(".fingerprint")
                .join(format!("{package}-{hash}")),
        }
    }

    /// Everything the unit owns, fingerprint first. `outputs` indexes a
    /// shared layout's `deps/` and `examples/` by hash.
    pub(crate) fn parts(&self, outputs: &Outputs) -> Vec<PathBuf> {
        match self {
            Unit::PerUnit { dir } => vec![dir.join("fingerprint"), dir.clone()],
            Unit::Shared {
                profile,
                package,
                hash,
            } => {
                let named = format!("{package}-{hash}");
                let mut parts = vec![
                    profile.join(".fingerprint").join(&named),
                    profile.join("build").join(&named),
                ];
                let crate_name = package.replace('-', "_");
                for (stem, path) in outputs.get(hash).into_iter().flatten() {
                    if stem.strip_prefix("lib").unwrap_or(stem) == crate_name || *stem == crate_name
                    {
                        parts.push(path.clone());
                    }
                }
                parts
            }
        }
    }
}

/// A shared layout's `deps/` and `examples/` entries by the hash in their
/// name, each with the name before the hash.
pub(crate) type Outputs = HashMap<String, Vec<(String, PathBuf)>>;

/// Split `name` at its `-<16 hex>` unit hash, the hash followed by the end
/// or a `.`: `libserde-0123456789abcdef.rlib` is `("libserde", hash)`.
pub(crate) fn hashed_name(name: &str) -> Option<(&str, &str)> {
    name.match_indices('-').find_map(|(at, _)| {
        let (stem, rest) = name.split_at(at);
        let (hash, after) = rest.get(1..)?.split_at_checked(16)?;
        let ends = after.is_empty() || after.starts_with('.');
        (crate::cargo_layout::is_unit_hash(hash) && ends).then_some((stem, hash))
    })
}

pub(crate) fn outputs(profile: &Path) -> Outputs {
    let mut outputs = Outputs::new();
    for dir in ["deps", "examples"] {
        for entry in entries(&profile.join(dir)) {
            let name = file_name(&entry);
            if let Some((stem, hash)) = hashed_name(&name) {
                outputs
                    .entry(hash.to_owned())
                    .or_default()
                    .push((stem.to_owned(), entry.clone()));
            }
        }
    }
    outputs
}

/// The units of one profile directory, in either layout.
pub(crate) fn units(profile: &Path) -> Vec<Unit> {
    let mut units = Vec::new();
    for fingerprint in subdirectories(&profile.join(".fingerprint")) {
        let name = file_name(&fingerprint);
        if let Some(package) = crate::cargo_layout::legacy_unit_package(&name) {
            units.push(Unit::Shared {
                profile: profile.to_path_buf(),
                package: package.to_owned(),
                hash: name[package.len() + 1..].to_owned(),
            });
        }
    }
    for package in subdirectories(&profile.join("build")) {
        for dir in subdirectories(&package) {
            let per_unit = crate::cargo_layout::is_unit_hash(&file_name(&dir))
                && dir.join("fingerprint").is_dir();
            if per_unit {
                units.push(Unit::PerUnit { dir });
            }
        }
    }
    units
}

/// The profile directories of a target directory: `<target>/<profile>` and
/// `<target>/<triple>/<profile>`, those holding any unit.
pub(crate) fn profiles(target_dir: &Path) -> Vec<PathBuf> {
    let mut profiles = Vec::new();
    for dir in subdirectories(target_dir) {
        if !units(&dir).is_empty() {
            profiles.push(dir);
            continue;
        }
        for nested in subdirectories(&dir) {
            if !units(&nested).is_empty() {
                profiles.push(nested);
            }
        }
    }
    profiles
}

/// Since when no build has used `target_dir`, as kache has watched it: the
/// moment [`watch`] or [`prune`] last armed it, when no unit's fingerprint has
/// been read or written since. Cargo reads the fingerprint of every unit a
/// build uses, even one that compiles nothing. `None` when it was never
/// armed, holds no unit, or a unit was used since. Reads only.
pub(crate) fn unused_since(cache_dir: &Path, target_dir: &Path) -> Option<SystemTime> {
    let armed = read_armed(&armed_record(cache_dir, target_dir))?;
    let units: Vec<Unit> = profiles(target_dir)
        .iter()
        .flat_map(|profile| units(profile))
        .collect();
    let unused = !units.is_empty()
        && !units
            .iter()
            .any(|unit| used_since(&unit.fingerprint(), armed));
    unused.then_some(armed)
}

/// Watch `target_dir` so a later [`unused_since`] shows whether a build used
/// it: arm it at `now`, but no earlier than `not_before`, unless it has gone
/// unused since an arming at or after `not_before`. Every profile is armed
/// under Cargo's locks, and the arming is recorded only when none was held.
/// The earlier record goes first ([`forget_armed`]); where it cannot, nothing
/// is armed. Like [`prune`], it needs a local filesystem that shows reads;
/// elsewhere it forgets any earlier watch, so the target never counts as
/// unused.
pub(crate) fn watch(cache_dir: &Path, target_dir: &Path, now: SystemTime, not_before: SystemTime) {
    watch_with(
        cache_dir,
        target_dir,
        now,
        not_before,
        shows_reads,
        write_armed,
    );
}

/// [`watch`], asking `shows_reads` whether arming works in `target_dir` and
/// recording the arming with `write`.
fn watch_with(
    cache_dir: &Path,
    target_dir: &Path,
    now: SystemTime,
    not_before: SystemTime,
    shows_reads: impl FnOnce(&Path) -> std::io::Result<bool>,
    write: impl FnOnce(&Path, SystemTime) -> anyhow::Result<()>,
) {
    let Ok(Some(_reservation)) = crate::target_use::try_exclusive(cache_dir) else {
        return;
    };
    let record = armed_record(cache_dir, target_dir);
    if !can_arm(&record, target_dir, shows_reads) {
        return;
    }
    if unused_since(cache_dir, target_dir).is_some_and(|since| since >= not_before) {
        return;
    }
    let mut held = Vec::new();
    for profile in profiles(target_dir) {
        let Some(locks) = hold(&profile) else {
            return;
        };
        held.push((profile, locks));
    }
    if !forget_armed(&record) {
        return;
    }
    for (profile, _locks) in &held {
        for unit in units(profile) {
            arm(&unit.fingerprint());
        }
    }
    if let Err(error) = write(&record, now.max(not_before)) {
        tracing::debug!(
            "could not record when {} was armed: {error:#}",
            target_dir.display()
        );
    }
}

/// Where the moment `target_dir` was armed is recorded.
fn armed_record(cache_dir: &Path, target_dir: &Path) -> PathBuf {
    let digest = blake3::hash(target_dir.as_os_str().as_encoded_bytes()).to_hex();
    cache_dir.join(ARMED_DIR).join(&digest[..16])
}

fn read_armed(record: &Path) -> Option<SystemTime> {
    let seconds = std::fs::read_to_string(record)
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()?;
    SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(seconds))
}

fn write_armed(record: &Path, at: SystemTime) -> anyhow::Result<()> {
    let seconds = at
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |since| since.as_secs());
    std::fs::create_dir_all(record.parent().unwrap_or(record))?;
    crate::atomic::atomic_replace(record, seconds.to_string().as_bytes())
}

/// Remove `record` before the units it vouches for are armed again. Arming
/// erases the reads that show a unit was used since the recorded moment, so
/// a record that outlived it would count those units as unused. Writing the
/// new record can fail where removing the old one works: a full APFS volume
/// refuses to rename onto an existing file. `false` when it stays, and then
/// nothing may be armed.
fn forget_armed(record: &Path) -> bool {
    match std::fs::remove_file(record) {
        Ok(()) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(error) => {
            tracing::debug!("cannot replace {}: {error}", record.display());
            false
        }
    }
}

/// What to do with a target armed at `armed`, at `now`, for `window`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Step {
    /// Never armed, or armed in the future by a clock set back: arm now.
    Arm,
    /// Armed less than `window` ago: nothing to judge yet.
    Wait,
    /// Remove what was not used since the moment given, then arm again.
    Judge(SystemTime),
}

pub(crate) fn step(armed: Option<SystemTime>, now: SystemTime, window: Duration) -> Step {
    let Some(armed) = armed else {
        return Step::Arm;
    };
    match now.duration_since(armed) {
        Ok(elapsed) if elapsed >= window => Step::Judge(armed),
        Ok(_) => Step::Wait,
        Err(_) => Step::Arm,
    }
}

/// Remove the units of `target_dir` not used within `window`, arming it first
/// if needed; see the module documentation. `cache_dir` keeps when it was
/// armed.
pub(crate) fn prune(
    cache_dir: &Path,
    target_dir: &Path,
    window: Duration,
    now: SystemTime,
) -> Pruned {
    prune_with(cache_dir, target_dir, window, now, shows_reads)
}

/// [`prune`], asking `shows_reads` whether arming works in `target_dir`.
fn prune_with(
    cache_dir: &Path,
    target_dir: &Path,
    window: Duration,
    now: SystemTime,
    shows_reads: impl FnOnce(&Path) -> std::io::Result<bool>,
) -> Pruned {
    let Ok(Some(_reservation)) = crate::target_use::try_exclusive(cache_dir) else {
        return Pruned::default();
    };
    if !can_arm(
        &armed_record(cache_dir, target_dir),
        target_dir,
        shows_reads,
    ) {
        return Pruned::default();
    }
    judge(cache_dir, target_dir, window, now)
}

/// Whether this pass may arm `target_dir`, by `shows_reads`. Where reads do
/// not show, an arming would no longer vouch for anything, so its `record`
/// is forgotten. A probe that could not run, as on a full volume, says
/// nothing about reads: the target waits for the next pass with its record.
fn can_arm(
    record: &Path,
    target_dir: &Path,
    shows_reads: impl FnOnce(&Path) -> std::io::Result<bool>,
) -> bool {
    match shows_reads(target_dir) {
        Ok(true) => true,
        Ok(false) => {
            let _ = std::fs::remove_file(record);
            false
        }
        Err(error) => {
            tracing::debug!(
                "skipping {} this pass: cannot tell whether reads show there: {error}",
                target_dir.display()
            );
            false
        }
    }
}

/// [`can_prune`] for `target_dir`: the filesystem's locality, then the probe.
/// An error means the probe could not run.
fn shows_reads(target_dir: &Path) -> std::io::Result<bool> {
    let local = matches!(
        crate::cache_fs::classify(&crate::cache_fs::probe(target_dir)),
        crate::cache_fs::CacheFsVerdict::Local
    );
    can_prune(local, || probe_reads(target_dir))
}

/// Pruning needs a local filesystem, where Cargo locks its build
/// directories, that also shows reads. `reads` is only asked when local.
fn can_prune(local: bool, reads: impl FnOnce() -> std::io::Result<bool>) -> std::io::Result<bool> {
    Ok(local && reads()?)
}

/// Arm `target_dir`, wait out `window`, then remove what was not used; the
/// part of [`prune`] after the filesystem was found to show reads.
fn judge(cache_dir: &Path, target_dir: &Path, window: Duration, now: SystemTime) -> Pruned {
    judge_with(cache_dir, target_dir, window, now, write_armed)
}

/// [`judge`], recording the arming with `write`.
fn judge_with(
    cache_dir: &Path,
    target_dir: &Path,
    window: Duration,
    now: SystemTime,
    write: impl FnOnce(&Path, SystemTime) -> anyhow::Result<()>,
) -> Pruned {
    let record = armed_record(cache_dir, target_dir);
    let armed = match step(read_armed(&record), now, window) {
        Step::Wait => return Pruned::default(),
        Step::Arm => None,
        Step::Judge(armed) => Some(armed),
    };
    // The old record goes before anything is armed again. A profile a build
    // held was neither judged nor armed, and with an old record that would
    // not go, nothing was armed: recording now would vouch for units never
    // armed, so the next check tries again.
    let (pruned, complete) = sweep(target_dir, armed, || forget_armed(&record));
    if !complete {
        return pruned;
    }
    // On failure no record is left, and the next check arms from scratch.
    if let Err(error) = write(&record, now) {
        tracing::debug!(
            "could not record when {} was armed: {error:#}",
            target_dir.display()
        );
    }
    pruned
}

/// What [`prune`] would remove from `target_dir` at `now` for `window`. Reads
/// only: nothing is removed or armed, and no lock is taken.
pub(crate) fn preview(
    cache_dir: &Path,
    target_dir: &Path,
    window: Duration,
    now: SystemTime,
) -> Pruned {
    let Step::Judge(armed) = step(
        read_armed(&armed_record(cache_dir, target_dir)),
        now,
        window,
    ) else {
        return Pruned::default();
    };
    let mut unused = Pruned::default();
    for profile in profiles(target_dir) {
        let outputs = outputs(&profile);
        for unit in units(&profile) {
            if let Some((_, bytes)) = stale(&unit, &outputs, armed) {
                unused.units += 1;
                unused.bytes += bytes;
            } else {
                unused.used += 1;
            }
        }
    }
    unused
}

/// The parts of `unit` and their bytes, when no build used it since `armed`.
fn stale(unit: &Unit, outputs: &Outputs, armed: SystemTime) -> Option<(Vec<PathBuf>, u64)> {
    if used_since(&unit.fingerprint(), armed) {
        return None;
    }
    let parts = unit.parts(outputs);
    let bytes = parts.iter().map(|part| size(part)).sum();
    Some((parts, bytes))
}

/// Hold every profile before removing or re-arming units. A partial pass must
/// not reset a used unit's access time while keeping the old arming record.
/// `false` when any profile is busy, with every unit left untouched. The
/// unused units go next; then `before_arming` decides whether the rest are
/// armed again, and `false` from it leaves them as they are.
fn sweep(
    target_dir: &Path,
    armed: Option<SystemTime>,
    before_arming: impl FnOnce() -> bool,
) -> (Pruned, bool) {
    let mut pruned = Pruned::default();
    let mut held = Vec::new();
    for profile in profiles(target_dir) {
        let Some(locks) = hold(&profile) else {
            return (pruned, false);
        };
        held.push((profile, locks));
    }
    let mut kept = Vec::new();
    for (profile, _locks) in &held {
        let outputs = outputs(profile);
        for unit in units(profile) {
            if let Some((parts, bytes)) = armed.and_then(|armed| stale(&unit, &outputs, armed)) {
                if remove(&parts).is_ok() {
                    pruned.units += 1;
                    pruned.bytes += bytes;
                }
                continue;
            }
            if armed.is_some() {
                pruned.used += 1;
            }
            kept.push(unit.fingerprint());
        }
    }
    if !before_arming() {
        return (pruned, false);
    }
    for fingerprint in kept {
        arm(&fingerprint);
    }
    (pruned, true)
}

/// Cargo locks this process holds, released when dropped.
pub(crate) struct Held(Vec<std::fs::File>);

impl Drop for Held {
    fn drop(&mut self) {
        // Closing our descriptor is insufficient if a concurrent fork still
        // holds a duplicate. Release ownership before that child reaches exec.
        for file in &self.0 {
            let _ = file.unlock();
        }
    }
}

/// Take every Cargo lock of `profile` without waiting, creating any that is
/// missing. `None` while a build holds one.
pub(crate) fn hold(profile: &Path) -> Option<Held> {
    let mut held = Held(Vec::new());
    for name in LOCKS {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(profile.join(name))
            .ok()?;
        file.try_lock().ok()?;
        held.0.push(file);
    }
    Some(held)
}

/// Remove `parts` in order, stopping at the first failure.
fn remove(parts: &[PathBuf]) -> std::io::Result<()> {
    for part in parts {
        let removed = match std::fs::symlink_metadata(part) {
            Ok(metadata) if metadata.is_dir() => std::fs::remove_dir_all(part),
            Ok(_) => std::fs::remove_file(part),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        };
        removed?;
    }
    Ok(())
}

/// Bytes under `path`, symbolic links not followed.
fn size(path: &Path) -> u64 {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => entries(path).iter().map(|entry| size(entry)).sum(),
        Ok(metadata) => metadata.len(),
        Err(_) => 0,
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    const OLD: &str = "0123456789abcdef";
    const NEW: &str = "fedcba9876543210";
    const DAY: u64 = 86_400;

    fn write(path: &Path, text: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }

    fn ago(days: u64) -> SystemTime {
        SystemTime::now() - Duration::from_secs(days * DAY)
    }

    /// Set every file under `dir` to have been read and written at `at`.
    fn set_times(dir: &Path, at: SystemTime) {
        let time = filetime::FileTime::from_system_time(at);
        for entry in entries(dir) {
            if entry.is_dir() {
                set_times(&entry, at);
            } else {
                filetime::set_file_times(&entry, time, time).unwrap();
            }
        }
    }

    /// A shared-layout profile with units `OLD` and `NEW` of `serde`, and an
    /// output of another crate that happens to carry `OLD`.
    fn shared_profile(profile: &Path) {
        for hash in [OLD, NEW] {
            let name = format!("serde-{hash}");
            write(
                &profile.join(".fingerprint").join(&name).join("lib-serde"),
                "fp",
            );
            write(&profile.join("build").join(&name).join("output"), "");
            write(
                &profile.join("deps").join(format!("libserde-{hash}.rlib")),
                "rlib",
            );
            write(&profile.join("deps").join(format!("serde-{hash}.d")), "d");
        }
        write(&profile.join(format!("deps/libother-{OLD}.rlib")), "keep");
        write(&profile.join("deps/libplain.rlib"), "keep");
    }

    fn per_unit_profile(profile: &Path) {
        for hash in [OLD, NEW] {
            let unit = profile.join("build/serde").join(hash);
            write(&unit.join("fingerprint/lib-serde"), "fp");
            write(&unit.join("out/libserde.rlib"), "rlib-bytes");
        }
    }

    #[test]
    fn a_watched_target_is_unused_until_a_build_reads_or_writes_a_unit() {
        for layout in [shared_profile as fn(&Path), per_unit_profile] {
            let cache = tempfile::tempdir().unwrap();
            let dir = tempfile::tempdir().unwrap();
            let target = dir.path().join("target");
            layout(&target.join("debug"));
            assert_eq!(unused_since(cache.path(), &target), None, "never armed");

            set_times(&target, ago(40));
            let armed = SystemTime::now() - Duration::from_secs(3);
            sweep(&target, None, || true);
            write_armed(&armed_record(cache.path(), &target), armed).unwrap();
            let since = unused_since(cache.path(), &target).unwrap();
            assert!(
                since <= armed && armed.duration_since(since).unwrap() < Duration::from_secs(2)
            );

            // A build that compiles nothing still reads every fingerprint.
            let fingerprint = unit_named(&target.join("debug"), NEW).fingerprint();
            for file in entries(&fingerprint) {
                let now = filetime::FileTime::now();
                filetime::set_file_atime(&file, now).unwrap();
            }
            assert_eq!(
                unused_since(cache.path(), &target),
                None,
                "read since armed"
            );
        }
    }

    #[test]
    fn a_target_without_units_is_never_unused() {
        let cache = tempfile::tempdir().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        write(&target.join("debug/deps/libplain.rlib"), "keep");
        write_armed(&armed_record(cache.path(), &target), ago(40)).unwrap();
        assert_eq!(unused_since(cache.path(), &target), None);
    }

    fn unit_named(profile: &Path, hash: &str) -> Unit {
        units(profile)
            .into_iter()
            .find(|unit| unit.fingerprint().to_string_lossy().contains(hash))
            .unwrap()
    }

    /// Whether reads should move atime on `dir`: on Linux, unless `noatime`.
    #[cfg(target_os = "linux")]
    fn reads_should_show(dir: &Path) -> Option<bool> {
        use std::os::unix::ffi::OsStrExt;
        let path = std::ffi::CString::new(dir.as_os_str().as_bytes()).unwrap();
        // SAFETY: statvfs is plain old data; all-zero bytes are a valid value.
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        // SAFETY: `path` is NUL-terminated and `stat` is a valid out pointer.
        assert_eq!(unsafe { libc::statvfs(path.as_ptr(), &mut stat) }, 0);
        Some(stat.f_flag & libc::ST_NOATIME == 0)
    }

    /// Unknown elsewhere: NTFS and some macOS volumes skip it by setting.
    #[cfg(not(target_os = "linux"))]
    fn reads_should_show(_dir: &Path) -> Option<bool> {
        None
    }

    #[test]
    fn probes_whether_reads_move_an_armed_access_time() {
        let dir = tempfile::tempdir().unwrap();
        let visible = reads_visible(dir.path());
        if let Some(expected) = reads_should_show(dir.path()) {
            assert_eq!(visible, expected);
        }
        assert!(entries(dir.path()).is_empty(), "the probe is removed");
        assert!(!reads_visible(&dir.path().join("missing")));
        assert_eq!(
            probe_reads(&dir.path().join("missing")).unwrap_err().kind(),
            std::io::ErrorKind::NotFound,
            "a probe that cannot run is not a probe that saw no read"
        );
        let armed = filetime::FileTime::from_unix_time(1_000, 0);
        assert!(moved_past(
            filetime::FileTime::from_unix_time(1_001, 0),
            armed
        ));
        assert!(!moved_past(armed, armed));
    }

    #[test]
    fn arms_waits_then_judges() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100 * DAY);
        let window = Duration::from_secs(30 * DAY);
        assert_eq!(step(None, now, window), Step::Arm);
        let armed = now - Duration::from_secs(30 * DAY);
        assert_eq!(step(Some(armed), now, window), Step::Judge(armed));
        let recent = armed + Duration::from_secs(1);
        assert_eq!(step(Some(recent), now, window), Step::Wait);
        let future = now + Duration::from_secs(1);
        assert_eq!(step(Some(future), now, window), Step::Arm);
    }

    #[test]
    fn finds_the_hash_in_an_output_name() {
        assert_eq!(
            hashed_name(&format!("libserde-{OLD}.rlib")),
            Some(("libserde", OLD))
        );
        assert_eq!(hashed_name(&format!("app-{OLD}")), Some(("app", OLD)));
        assert_eq!(
            hashed_name(&format!("proc-macro2-{OLD}.d")),
            Some(("proc-macro2", OLD))
        );
        assert_eq!(
            hashed_name(&format!("app-{OLD}.app.0-cgu.0.rcgu.o")),
            Some(("app", OLD))
        );
        for plain in ["libplain.rlib", "a-b", &format!("x-{OLD}x"), "x-0123"] {
            assert_eq!(hashed_name(plain), None, "{plain}");
        }
    }

    #[test]
    fn a_shared_unit_owns_only_outputs_of_its_crate() {
        let dir = tempfile::tempdir().unwrap();
        let profile = dir.path().join("debug");
        shared_profile(&profile);
        let unit = unit_named(&profile, OLD);
        let name = format!("serde-{OLD}");
        assert_eq!(
            unit.parts(&outputs(&profile)),
            vec![
                profile.join(".fingerprint").join(&name),
                profile.join("build").join(&name),
                profile.join("deps").join(format!("libserde-{OLD}.rlib")),
                profile.join("deps").join(format!("serde-{OLD}.d")),
            ]
        );
        let hyphenated = Unit::Shared {
            profile: profile.clone(),
            package: "proc-macro2".into(),
            hash: NEW.into(),
        };
        write(
            &profile.join(format!("deps/libproc_macro2-{NEW}.rlib")),
            "x",
        );
        assert!(
            hyphenated
                .parts(&outputs(&profile))
                .contains(&profile.join(format!("deps/libproc_macro2-{NEW}.rlib")))
        );
    }

    #[test]
    fn finds_units_in_both_layouts_and_every_profile() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path();
        shared_profile(&target.join("debug"));
        per_unit_profile(&target.join("aarch64-apple-darwin/release"));
        std::fs::create_dir_all(target.join("tmp")).unwrap();
        assert_eq!(
            profiles(target),
            vec![
                target.join("aarch64-apple-darwin/release"),
                target.join("debug")
            ]
        );
        assert!(
            units(&target.join("debug"))
                .iter()
                .all(|unit| matches!(unit, Unit::Shared { .. }))
        );
        assert_eq!(units(&target.join("aarch64-apple-darwin/release")).len(), 2);
        write(&target.join("debug/build/serde/nothex/fingerprint/x"), "");
        write(&target.join("debug/build/serde/aaaaaaaaaaaaaaaa/out/x"), "");
        write(&target.join("debug/.fingerprint/nohash/x"), "");
        assert_eq!(units(&target.join("debug")).len(), 2);
    }

    #[test]
    fn a_read_or_write_after_arming_is_use() {
        let dir = tempfile::tempdir().unwrap();
        let fingerprint = dir.path().join("fp");
        write(&fingerprint.join("a"), "a");
        write(&fingerprint.join("b"), "b");
        set_times(&fingerprint, ago(10));
        let armed = ago(5);
        assert!(!used_since(&fingerprint, armed));
        let read = filetime::FileTime::from_system_time(ago(1));
        filetime::set_file_atime(fingerprint.join("b"), read).unwrap();
        assert!(used_since(&fingerprint, armed));
        set_times(&fingerprint, ago(10));
        filetime::set_file_mtime(fingerprint.join("a"), read).unwrap();
        assert!(used_since(&fingerprint, armed));
        // An empty fingerprint is judged by the directory.
        let empty = dir.path().join("empty");
        std::fs::create_dir(&empty).unwrap();
        let old = filetime::FileTime::from_system_time(ago(10));
        filetime::set_file_mtime(&empty, old).unwrap();
        assert!(!used_since(&empty, armed));
        assert!(used_since(&empty, ago(20)));
        assert!(used_since(&dir.path().join("missing"), armed));
    }

    #[test]
    fn arming_puts_the_access_time_below_the_modification_time() {
        let dir = tempfile::tempdir().unwrap();
        let fingerprint = dir.path().join("fp");
        write(&fingerprint.join("a"), "a");
        set_times(&fingerprint, ago(10));
        arm(&fingerprint);
        let metadata = std::fs::metadata(fingerprint.join("a")).unwrap();
        let modified = filetime::FileTime::from_last_modification_time(&metadata);
        let accessed = filetime::FileTime::from_last_access_time(&metadata);
        assert_eq!(accessed.unix_seconds(), modified.unix_seconds() - 1);
        assert!(!used_since(&fingerprint, ago(5)));
        // Where reads show, a read now moves it past any moment after arming.
        std::fs::read(fingerprint.join("a")).unwrap();
        if reads_visible(dir.path()) {
            assert!(used_since(&fingerprint, ago(1)));
        }
    }

    #[test]
    fn a_time_equal_to_the_arming_is_not_use() {
        let dir = tempfile::tempdir().unwrap();
        let armed = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let at = filetime::FileTime::from_system_time(armed);
        let fingerprint = dir.path().join("fp");
        write(&fingerprint.join("a"), "a");
        filetime::set_file_times(fingerprint.join("a"), at, at).unwrap();
        assert!(!used_since(&fingerprint, armed));
        let empty = dir.path().join("empty");
        std::fs::create_dir(&empty).unwrap();
        filetime::set_file_mtime(&empty, at).unwrap();
        assert!(!used_since(&empty, armed));
    }

    /// Record a build's read of `unit`'s fingerprint now, the way a
    /// filesystem that shows reads would.
    fn read_now(unit: &Unit) {
        let now = filetime::FileTime::now();
        for file in entries(&unit.fingerprint()) {
            filetime::set_file_atime(&file, now).unwrap();
        }
    }

    #[test]
    fn removes_units_nobody_used_since_arming_in_both_layouts() {
        for per_unit in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let cache = dir.path().join("cache");
            let target = dir.path().join("target");
            let profile = target.join("debug");
            if per_unit {
                per_unit_profile(&profile);
            } else {
                shared_profile(&profile);
            }
            set_times(&profile, ago(60));
            let window = Duration::from_secs(30 * DAY);
            // First sight: armed, nothing removed.
            assert_eq!(judge(&cache, &target, window, ago(40)), Pruned::default());
            assert_eq!(units(&profile).len(), 2);
            // Within the window: nothing to judge.
            assert_eq!(judge(&cache, &target, window, ago(20)), Pruned::default());
            // A build used `NEW` after arming.
            read_now(&unit_named(&profile, NEW));
            let old = unit_named(&profile, OLD);
            let parts = old.parts(&outputs(&profile));
            let bytes: u64 = parts.iter().map(|part| size(part)).sum();
            let pruned = judge(&cache, &target, window, SystemTime::now());
            assert_eq!(
                (pruned.units, pruned.bytes),
                (1, bytes),
                "per_unit={per_unit}"
            );
            for part in &parts {
                assert!(!part.exists(), "{}", part.display());
            }
            let left = units(&profile);
            assert_eq!(left, vec![unit_named(&profile, NEW)]);
            if !per_unit {
                assert!(profile.join(format!("deps/libother-{OLD}.rlib")).exists());
                assert!(profile.join("deps/libplain.rlib").exists());
            }
            for lock in LOCKS {
                assert!(profile.join(lock).exists(), "{lock}");
            }
            // Re-armed: judged again only after another window.
            assert_eq!(
                judge(&cache, &target, window, SystemTime::now()),
                Pruned::default()
            );
            let record = armed_record(&cache, &target);
            assert!(read_armed(&record).is_some());
        }
    }

    #[test]
    fn a_preview_reports_what_a_prune_removes_and_changes_nothing() {
        for per_unit in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let cache = dir.path().join("cache");
            let target = dir.path().join("target");
            let profile = target.join("debug");
            if per_unit {
                per_unit_profile(&profile);
            } else {
                shared_profile(&profile);
            }
            set_times(&profile, ago(60));
            let window = Duration::from_secs(30 * DAY);
            let now = SystemTime::now();
            assert_eq!(preview(&cache, &target, window, now), Pruned::default());
            judge(&cache, &target, window, ago(40));
            assert_eq!(preview(&cache, &target, window, ago(20)), Pruned::default());
            let used = unit_named(&profile, NEW);
            read_now(&used);
            let accessed = |unit: &Unit| {
                entries(&unit.fingerprint())
                    .iter()
                    .map(|file| std::fs::metadata(file).unwrap().accessed().unwrap())
                    .collect::<Vec<_>>()
            };
            let before = accessed(&used);
            let record = armed_record(&cache, &target);
            let armed = read_armed(&record);
            let expected = preview(&cache, &target, window, now);
            assert_eq!(expected.units, 1, "per_unit={per_unit}");
            assert!(expected.bytes > 0);
            assert_eq!(units(&profile).len(), 2, "nothing removed");
            assert_eq!(accessed(&used), before, "nothing armed");
            assert_eq!(read_armed(&record), armed, "the record kept");
            assert_eq!(judge(&cache, &target, window, now), expected);
        }
    }

    #[test]
    fn needs_a_local_filesystem_that_shows_reads() {
        assert!(can_prune(true, || Ok(true)).unwrap());
        assert!(!can_prune(true, || Ok(false)).unwrap());
        assert!(!can_prune(false, || Ok(true)).unwrap());
        assert!(!can_prune(false, || panic!("not asked off a local filesystem")).unwrap());
        let full = || Err(std::io::Error::from(std::io::ErrorKind::StorageFull));
        assert_eq!(
            can_prune(true, full).unwrap_err().kind(),
            std::io::ErrorKind::StorageFull
        );
    }

    #[test]
    fn prunes_only_where_the_filesystem_shows_reads() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let window = Duration::from_secs(30 * DAY);
        let missing = dir.path().join("missing");
        assert_eq!(
            prune(&cache, &missing, window, SystemTime::now()),
            Pruned::default()
        );
        assert_eq!(
            read_armed(&armed_record(&cache, &missing)),
            None,
            "a missing target is never armed"
        );
        let target = dir.path().join("target");
        per_unit_profile(&target.join("debug"));
        set_times(&target, ago(60));
        prune(&cache, &target, window, ago(40));
        let armed = read_armed(&armed_record(&cache, &target));
        // Arming happens only where reads show, and only then is it recorded.
        assert_eq!(armed.is_some(), reads_visible(&target));
    }

    #[test]
    fn a_probe_that_cannot_run_keeps_the_arming() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let target = dir.path().join("target");
        let profile = target.join("debug");
        per_unit_profile(&profile);
        set_times(&target, ago(60));
        let record = armed_record(&cache, &target);
        write_armed(&record, ago(40)).unwrap();
        let armed = read_armed(&record);
        let window = Duration::from_secs(30 * DAY);
        let now = SystemTime::now();
        // A full volume refuses the probe's file.
        let full = |_: &Path| -> std::io::Result<bool> {
            Err(std::io::Error::from(std::io::ErrorKind::StorageFull))
        };
        // Judged now, both units would go, and a watch would arm again.
        assert_eq!(
            prune_with(&cache, &target, window, now, full),
            Pruned::default()
        );
        assert_eq!(read_armed(&record), armed, "prune kept the arming");
        assert_eq!(units(&profile).len(), 2, "prune skipped the target");
        watch_with(&cache, &target, now, ago(30), full, write_armed);
        assert_eq!(read_armed(&record), armed, "watch kept the arming");

        // Reads that do not show still forget it.
        let hidden = |_: &Path| -> std::io::Result<bool> { Ok(false) };
        watch_with(&cache, &target, now, ago(30), hidden, write_armed);
        assert_eq!(read_armed(&record), None);
        write_armed(&record, ago(40)).unwrap();
        assert_eq!(
            prune_with(&cache, &target, window, now, hidden),
            Pruned::default()
        );
        assert_eq!(read_armed(&record), None);
        assert_eq!(units(&profile).len(), 2);
    }

    /// Write `record` the way a full APFS volume does: a new file goes in,
    /// but nothing is renamed onto one that exists.
    fn write_where_free(record: &Path, at: SystemTime) -> anyhow::Result<()> {
        if record.exists() {
            return Err(std::io::Error::from(std::io::ErrorKind::StorageFull).into());
        }
        write_armed(record, at)
    }

    fn write_nothing(_: &Path, _: SystemTime) -> anyhow::Result<()> {
        Err(std::io::Error::from(std::io::ErrorKind::StorageFull).into())
    }

    #[test]
    fn an_arming_never_keeps_an_older_record() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let target = dir.path().join("target");
        let profile = target.join("debug");
        per_unit_profile(&profile);
        set_times(&target, ago(60));
        let window = Duration::from_secs(30 * DAY);
        let hour = Duration::from_secs(3600);
        judge(&cache, &target, window, ago(40));
        let used = unit_named(&profile, NEW);
        read_now(&used);

        let now = SystemTime::now();
        let pruned = judge_with(&cache, &target, window, now, write_where_free);
        assert_eq!((pruned.units, pruned.used), (1, 1));
        // `used` was armed again under the new record. Judged an hour on
        // against the old one, it would look unused and go.
        assert_eq!(
            judge(&cache, &target, window, now + hour),
            Pruned::default()
        );
        assert_eq!(units(&profile), vec![used.clone()]);

        // With no record written at all, none is left: the next check arms
        // from scratch.
        read_now(&used);
        let later = now + window;
        let pruned = judge_with(&cache, &target, window, later, write_nothing);
        assert_eq!((pruned.units, pruned.used), (0, 1));
        assert_eq!(read_armed(&armed_record(&cache, &target)), None);
        assert_eq!(
            judge(&cache, &target, window, later + hour),
            Pruned::default()
        );
        assert_eq!(units(&profile), vec![used]);
    }

    #[test]
    fn a_watch_never_keeps_an_older_record() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let target = dir.path().join("target");
        let profile = target.join("debug");
        per_unit_profile(&profile);
        set_times(&target, ago(60));
        let shows = |_: &Path| -> std::io::Result<bool> { Ok(true) };
        watch_with(&cache, &target, ago(10), ago(20), shows, write_armed);
        let first = unused_since(&cache, &target).unwrap();
        let used = unit_named(&profile, NEW);
        read_now(&used);

        let now = SystemTime::now();
        watch_with(&cache, &target, now, ago(20), shows, write_where_free);
        let since = unused_since(&cache, &target).unwrap();
        assert!(since > first, "idle from the new arming, not the old");

        read_now(&used);
        watch_with(&cache, &target, now, ago(20), shows, write_nothing);
        assert_eq!(unused_since(&cache, &target), None, "no record is left");
    }

    #[test]
    fn units_that_stay_are_not_armed_while_the_old_record_stays() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        let profile = target.join("debug");
        per_unit_profile(&profile);
        set_times(&target, ago(60));
        sweep(&target, None, || true);
        let used = unit_named(&profile, NEW);
        read_now(&used);
        let armed = ago(40);
        let (pruned, complete) = sweep(&target, Some(armed), || false);
        assert_eq!((pruned.units, pruned.used, complete), (1, 1, false));
        assert!(
            used_since(&used.fingerprint(), armed),
            "the read still shows"
        );
        assert_eq!(units(&profile), vec![used]);
    }

    /// The record directory turned read-only after an arming was recorded:
    /// the old record can be neither replaced nor removed.
    #[cfg(unix)]
    #[test]
    fn a_record_that_cannot_be_removed_keeps_the_reads_it_judges_by() {
        use std::os::unix::fs::PermissionsExt;
        // SAFETY: plain libc call with no arguments.
        if unsafe { libc::geteuid() } == 0 {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let target = dir.path().join("target");
        let profile = target.join("debug");
        per_unit_profile(&profile);
        set_times(&target, ago(60));
        let window = Duration::from_secs(30 * DAY);
        judge(&cache, &target, window, ago(40));
        let record = armed_record(&cache, &target);
        let armed = read_armed(&record);
        let used = unit_named(&profile, NEW);
        read_now(&used);
        let records = record.parent().unwrap();
        let mode = |mode| std::fs::set_permissions(records, std::fs::Permissions::from_mode(mode));
        mode(0o555).unwrap();
        let now = SystemTime::now();
        let pruned = judge(&cache, &target, window, now);
        let shows = |_: &Path| -> std::io::Result<bool> { Ok(true) };
        watch_with(&cache, &target, now, ago(50), shows, write_armed);
        mode(0o755).unwrap();
        assert_eq!((pruned.units, pruned.used), (1, 1));
        assert_eq!(read_armed(&record), armed);
        assert_eq!(unused_since(&cache, &target), None, "watch armed nothing");

        let pruned = judge(&cache, &target, window, now + Duration::from_secs(3600));
        assert_eq!((pruned.units, pruned.used), (0, 1));
        assert_eq!(units(&profile), vec![used]);
    }

    #[test]
    fn a_profile_a_build_holds_is_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let profile = dir.path().join("debug");
        per_unit_profile(&profile);
        set_times(&profile, ago(10));
        for name in LOCKS {
            let lock = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(profile.join(name))
                .unwrap();
            lock.lock().unwrap();
            let (pruned, complete) = sweep(dir.path(), Some(ago(5)), || true);
            assert_eq!(pruned, Pruned::default(), "{name}");
            assert!(!complete, "a held profile is reported: {name}");
            assert_eq!(units(&profile).len(), 2);
            // A fork elsewhere in the suite can hold a duplicate past the drop.
            lock.unlock().unwrap();
        }
        let (pruned, complete) = sweep(dir.path(), Some(ago(5)), || true);
        assert_eq!((pruned.units, complete), (2, true));
    }

    #[test]
    fn a_busy_profile_preserves_recent_use_in_every_profile() {
        for per_unit in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let cache = dir.path().join("cache");
            let target = dir.path().join("target");
            for name in ["debug", "release"] {
                let profile = target.join(name);
                if per_unit {
                    per_unit_profile(&profile);
                } else {
                    shared_profile(&profile);
                }
            }
            set_times(&target, ago(60));
            let window = Duration::from_secs(30 * DAY);
            judge(&cache, &target, window, ago(40));
            let record = armed_record(&cache, &target);
            let armed = read_armed(&record).unwrap();
            // Whichever profile is visited first must retain its evidence
            // when a later profile is held by Cargo.
            let ordered = profiles(&target);
            let used = unit_named(&ordered[0], NEW);
            read_now(&used);
            let before: Vec<_> = entries(&used.fingerprint())
                .iter()
                .map(|file| std::fs::metadata(file).unwrap().accessed().unwrap())
                .collect();
            let locks = hold(&ordered[1]).unwrap();
            assert_eq!(
                judge(&cache, &target, window, SystemTime::now()),
                Pruned::default()
            );
            assert_eq!(read_armed(&record), Some(armed));
            assert_eq!(
                entries(&used.fingerprint())
                    .iter()
                    .map(|file| std::fs::metadata(file).unwrap().accessed().unwrap())
                    .collect::<Vec<_>>(),
                before,
                "a skipped pass must retain recent use"
            );
            assert_eq!(units(&ordered[0]).len(), 2);
            drop(locks);
            let pruned = judge(&cache, &target, window, SystemTime::now());
            assert_eq!((pruned.units, pruned.used), (3, 1));
            assert_eq!(units(&ordered[0]), vec![used]);
            assert!(units(&ordered[1]).is_empty());
        }
    }

    #[cfg(unix)]
    #[test]
    fn released_locks_are_free_while_a_duplicate_is_open() {
        let dir = tempfile::tempdir().unwrap();
        let held = hold(dir.path()).unwrap();
        // A fork shares the open file description the same way a duplicate does.
        let duplicates: Vec<_> = held.0.iter().map(|f| f.try_clone().unwrap()).collect();
        assert_eq!(duplicates.len(), LOCKS.len());
        assert!(hold(dir.path()).is_none(), "the locks are held");
        drop(held);
        assert!(hold(dir.path()).is_some(), "a duplicate kept a lock held");
        drop(duplicates);
    }

    #[test]
    fn records_when_a_target_was_armed() {
        let dir = tempfile::tempdir().unwrap();
        let record = armed_record(dir.path(), Path::new("/some/target"));
        assert_ne!(record, armed_record(dir.path(), Path::new("/other/target")));
        assert_eq!(read_armed(&record), None);
        let at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        write_armed(&record, at).unwrap();
        assert_eq!(read_armed(&record), Some(at));
        std::fs::write(&record, "garbage").unwrap();
        assert_eq!(read_armed(&record), None);
    }

    #[test]
    fn removal_stops_at_the_first_failure() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a");
        write(&a.join("x"), "x");
        let b = dir.path().join("b");
        write(&b, "bb");
        assert_eq!(size(&a) + size(&b), 3);
        remove(&[a.clone(), dir.path().join("missing"), b.clone()]).unwrap();
        assert!(!a.exists() && !b.exists());
        assert_eq!(size(&dir.path().join("missing")), 0);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            // SAFETY: plain libc call with no arguments.
            let root = unsafe { libc::geteuid() } == 0;
            let locked = dir.path().join("locked");
            write(&locked.join("file"), "x");
            std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o555)).unwrap();
            let after = dir.path().join("after");
            write(&after, "x");
            if !root {
                assert!(remove(&[locked.join("file"), after.clone()]).is_err());
                assert!(after.exists(), "nothing after a failure is removed");
            }
            // A part that cannot even be looked up is a failure, not absent.
            let sealed = dir.path().join("sealed");
            write(&sealed.join("file"), "x");
            std::fs::set_permissions(&sealed, std::fs::Permissions::from_mode(0o000)).unwrap();
            if !root {
                assert!(remove(&[sealed.join("file"), after.clone()]).is_err());
                assert!(after.exists());
            }
            std::fs::set_permissions(&sealed, std::fs::Permissions::from_mode(0o755)).unwrap();
            std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
    }
}
