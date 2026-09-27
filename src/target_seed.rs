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
//! - A build script whose recorded output names the other checkout, other
//!   than its own `OUT_DIR` (which Cargo rewrites), is not copied.
//! - The other checkout must have been built by the same `rustc`, and no
//!   build may hold either profile's lock while units are copied.
//!
//! Both of Cargo's layouts are handled (see [`crate::cargo_layout`]). Only
//! the host `debug` profile is seeded: the probe does not say which profile
//! the build uses, and `debug` is the one `build`, `check`, `test` and
//! `clippy` share.

use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// How long the daemon copies for. Copying stops at this deadline, between
/// files, and Cargo builds whatever was not copied.
pub(crate) const SEED_DEADLINE: Duration = Duration::from_secs(3);

/// How long the probe waits for the daemon's answer: the deadline and time
/// for the file copy in progress when it passed.
pub(crate) fn seed_wait() -> Duration {
    SEED_DEADLINE + Duration::from_secs(2)
}

/// The profile directory seeded.
const PROFILE: &str = "debug";

/// Where a unit is copied before it is renamed into place.
const STAGING: &str = ".kache-seeding-";

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
/// `target_env` (`CARGO_TARGET_DIR`) or `<workspace>/target`. `None` when
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

/// Whether Cargo configuration found from `cwd` sets `build.target-dir` or
/// `build.build-dir`, which this does not follow.
pub(crate) fn configured_elsewhere(cwd: &Path, cargo_home: &Path) -> bool {
    let mut dirs: Vec<PathBuf> = cwd.ancestors().map(|dir| dir.join(".cargo")).collect();
    dirs.push(cargo_home.to_path_buf());
    dirs.iter().any(|dir| {
        ["config", "config.toml"].iter().any(|name| {
            std::fs::read_to_string(dir.join(name))
                .ok()
                .and_then(|text| text.parse::<toml::Table>().ok())
                .and_then(|table| table.get("build").cloned())
                .is_some_and(|build| {
                    build.get("target-dir").is_some() || build.get("build-dir").is_some()
                })
        })
    })
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

/// Copy one unit from the donor profile `from` to `to`. The unit's marker
/// goes last, by rename.
fn copy_unit(
    from: &Path,
    to: &Path,
    layout: Layout,
    unit: &Unit,
    deadline: Instant,
) -> std::io::Result<()> {
    match layout {
        Layout::PerUnit => {
            let package = to.join("build").join(&unit.package);
            std::fs::create_dir_all(&package)?;
            place_tree(
                &from.join("build").join(&unit.package).join(&unit.hash),
                &package.join(&unit.hash),
                deadline,
            )
        }
        Layout::Shared => {
            let named = format!("{}-{}", unit.package, unit.hash);
            let suffix = format!("-{}", unit.hash);
            let deps = to.join("deps");
            std::fs::create_dir_all(&deps)?;
            for file in entries(&from.join("deps")) {
                let name = file_name(&file);
                let ours = name.contains(&format!("{suffix}.")) || name.ends_with(&suffix);
                if ours && !deps.join(&name).exists() {
                    place_tree(&file, &deps.join(&name), deadline)?;
                }
            }
            let build = from.join("build").join(&named);
            if build.is_dir() {
                std::fs::create_dir_all(to.join("build"))?;
                place_tree(&build, &to.join("build").join(&named), deadline)?;
            }
            std::fs::create_dir_all(to.join(".fingerprint"))?;
            place_tree(
                &from.join(".fingerprint").join(&named),
                &to.join(".fingerprint").join(&named),
                deadline,
            )
        }
    }
}

/// Copy `from` (a file or a directory) beside `to` and rename it there.
fn place_tree(from: &Path, to: &Path, deadline: Instant) -> std::io::Result<()> {
    let staging = to.with_file_name(format!("{STAGING}{}-{}", file_name(to), std::process::id()));
    let copied = copy_tree(from, &staging, deadline).and_then(|()| std::fs::rename(&staging, to));
    if copied.is_err() {
        let _ = std::fs::remove_dir_all(&staging);
        let _ = std::fs::remove_file(&staging);
    }
    copied
}

/// Copy a file or directory tree, keeping every file's modification time.
/// A symbolic link fails the copy, and so does reaching `deadline` before a
/// file.
fn copy_tree(from: &Path, to: &Path, deadline: Instant) -> std::io::Result<()> {
    let metadata = std::fs::symlink_metadata(from)?;
    if metadata.is_dir() {
        std::fs::create_dir(to)?;
        for entry in entries(from) {
            copy_tree(&entry, &to.join(file_name(&entry)), deadline)?;
        }
        return Ok(());
    }
    if Instant::now() >= deadline {
        return Err(std::io::ErrorKind::TimedOut.into());
    }
    if !metadata.is_file() {
        return Err(std::io::Error::other(format!(
            "{} is not a file or a directory",
            from.display()
        )));
    }
    if kache_store::link::try_reflink(from, to).is_err() {
        std::fs::copy(from, to)?;
    }
    filetime::set_file_mtime(
        to,
        filetime::FileTime::from_last_modification_time(&metadata),
    )
}

/// What a seeding did.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Seeded {
    pub(crate) units: usize,
    pub(crate) donor: Option<PathBuf>,
}

/// Copy the registry units `target` needs from the first donor, most recent
/// first, built by the `rustc -vV` in `rustc_version` that has any. Stops at
/// `deadline`.
pub(crate) fn seed(
    target: &NewTarget,
    rustc_version: &str,
    donors: &[Donor],
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
    if std::fs::create_dir_all(&to).is_err() {
        return Seeded::default();
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
        if donor.target_dir == target.target_dir || !built_by(&donor.target_dir, rustc_version) {
            continue;
        }
        let from = donor.target_dir.join(PROFILE);
        let Some(layout) = layout(&from) else {
            continue;
        };
        let Some(_theirs) = try_lock(&from.join(".cargo-lock")) else {
            continue;
        };
        let mut copied = 0;
        for unit in units(&from, layout, &packages) {
            if Instant::now() >= deadline {
                break;
            }
            if marker(&to, layout, &unit).exists() || names_the_donor(&from, layout, &unit, donor) {
                continue;
            }
            match copy_unit(&from, &to, layout, &unit, deadline) {
                Ok(()) => copied += 1,
                Err(error) => {
                    tracing::debug!("did not seed {}-{}: {error}", unit.package, unit.hash)
                }
            }
        }
        if copied > 0 {
            return Seeded {
                units: copied,
                donor: Some(donor.workspace_root.clone()),
            };
        }
    }
    Seeded::default()
}

/// Whether Cargo's cached `rustc -vV` answer in `target_dir` is `version`.
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

/// Lock `path` exclusively without waiting, creating it if needed. `None`
/// when another process holds it or it cannot be opened.
fn try_lock(path: &Path) -> Option<std::fs::File> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .ok()?;
    file.try_lock().ok()?;
    Some(file)
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

/// Whether Cargo puts this build's units somewhere other than its target
/// directory: `CARGO_BUILD_BUILD_DIR` is set, or a Cargo config names a
/// target or build directory.
fn builds_elsewhere(build_dir_env: bool, cwd: &Path, cargo_home: &Path) -> bool {
    build_dir_env || configured_elsewhere(cwd, cargo_home)
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
    let target_env =
        std::env::var_os("CARGO_TARGET_DIR").or_else(|| std::env::var_os("CARGO_BUILD_TARGET_DIR"));
    let elsewhere = builds_elsewhere(
        std::env::var_os("CARGO_BUILD_BUILD_DIR").is_some(),
        &cwd,
        &crate::cli::cargo_home_dir(),
    );
    let Some(target) = new_target(&cwd, target_env.as_deref(), elsewhere) else {
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

    #[test]
    fn a_build_dir_from_the_environment_or_a_config_moves_the_build() {
        let dir = tempfile::tempdir().unwrap();
        let cwd = dir.path().join("work");
        let home = dir.path().join("cargo-home");
        std::fs::create_dir_all(&cwd).unwrap();
        assert!(!builds_elsewhere(false, &cwd, &home));
        assert!(builds_elsewhere(true, &cwd, &home), "CARGO_BUILD_BUILD_DIR");
        write(&home.join("config.toml"), "[build]\nbuild-dir = \"/b\"\n");
        assert!(builds_elsewhere(false, &cwd, &home), "a Cargo config");
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
    fn notices_configuration_that_moves_the_target() {
        let dir = tempfile::tempdir().unwrap();
        let cwd = dir.path().join("ws/pkg");
        let home = dir.path().join("home");
        std::fs::create_dir_all(&cwd).unwrap();
        assert!(!configured_elsewhere(&cwd, &home));
        write(&home.join("config.toml"), "[build]\njobs = 2\n");
        assert!(!configured_elsewhere(&cwd, &home));
        write(&home.join("config.toml"), "not toml [");
        assert!(!configured_elsewhere(&cwd, &home));
        write(&home.join("config.toml"), "[build]\nbuild-dir = 'b'\n");
        assert!(configured_elsewhere(&cwd, &home));
        std::fs::remove_file(home.join("config.toml")).unwrap();
        write(
            &dir.path().join("ws/.cargo/config"),
            "[build]\ntarget-dir = 't'\n",
        );
        assert!(configured_elsewhere(&cwd, &home));
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
            let seeded = seed(&new, VERSION, std::slice::from_ref(&donor), later());
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
                seed(&new, VERSION, std::slice::from_ref(&donor), later()),
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
                later()
            ),
            Seeded::default()
        );
        assert_eq!(
            seed(&new, "  ", std::slice::from_ref(&wrong_rustc), later()),
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
            seed(&new, "  ", std::slice::from_ref(&blank), later()),
            Seeded::default()
        );
        // A build holds the donor's lock: the next donor gives the units.
        let busy = donor(dir.path(), "busy", Layout::Shared);
        let held = try_lock(&busy.target_dir.join(PROFILE).join(".cargo-lock")).unwrap();
        let free = donor(dir.path(), "free", Layout::PerUnit);
        let seeded = seed(&new, VERSION, &[busy.clone(), free.clone()], later());
        assert_eq!(seeded.donor, Some(free.workspace_root.clone()));
        drop(held);
        // The target itself, and a donor with no units, give nothing.
        let other = checkout(dir.path(), "c");
        let own = Donor {
            target_dir: other.target_dir.clone(),
            workspace_root: other.workspace_root.clone(),
        };
        let empty = donor(dir.path(), "empty", Layout::Shared);
        std::fs::remove_dir_all(empty.target_dir.join(PROFILE)).unwrap();
        std::fs::create_dir_all(empty.target_dir.join(PROFILE)).unwrap();
        assert_eq!(
            seed(&other, VERSION, &[own, empty], later()),
            Seeded::default()
        );
        // Past the deadline nothing is copied.
        let late = checkout(dir.path(), "late");
        assert_eq!(
            seed(&late, VERSION, std::slice::from_ref(&free), Instant::now()),
            Seeded::default()
        );
        // A build holding the new target's lock is left alone.
        let locked = checkout(dir.path(), "locked");
        std::fs::create_dir_all(locked.target_dir.join(PROFILE)).unwrap();
        let ours = try_lock(&locked.target_dir.join(PROFILE).join(".cargo-lock")).unwrap();
        assert_eq!(
            seed(&locked, VERSION, std::slice::from_ref(&free), later()),
            Seeded::default()
        );
        drop(ours);
        // Without a lockfile, or with only path packages, there is nothing to copy.
        let bare = checkout(dir.path(), "bare");
        std::fs::remove_file(bare.workspace_root.join("Cargo.lock")).unwrap();
        assert_eq!(
            seed(&bare, VERSION, std::slice::from_ref(&free), later()),
            Seeded::default()
        );
        write(
            &bare.workspace_root.join("Cargo.lock"),
            "[[package]]\nname = \"app\"\n",
        );
        assert_eq!(seed(&bare, VERSION, &[free], later()), Seeded::default());
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
                seed(&new, VERSION, std::slice::from_ref(&donor), later()),
                Seeded::default()
            );
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
        let seeded = seed(&new, VERSION, std::slice::from_ref(&donor), later());
        #[cfg(unix)]
        {
            assert_eq!(seeded, Seeded::default());
            let package = new.target_dir.join(PROFILE).join("build/dep");
            assert_eq!(entries(&package), Vec::<PathBuf>::new());
        }
        #[cfg(not(unix))]
        let _ = seeded;
    }

    #[test]
    fn a_passed_deadline_stops_a_copy_and_leaves_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let from = dir.path().join("unit");
        write(&from.join("a/file"), "x");
        let to = dir.path().join("copy");
        let error = place_tree(&from, &to, Instant::now()).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        assert_eq!(entries(dir.path()), vec![from.clone()]);
        place_tree(&from, &to, later()).unwrap();
        assert_eq!(std::fs::read_to_string(to.join("a/file")).unwrap(), "x");
    }

    #[test]
    fn a_target_built_while_waiting_for_its_lock_is_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let donor = donor(dir.path(), "a", Layout::Shared);
        let new = checkout(dir.path(), "b");
        write(&new.target_dir.join(".rustc_info.json"), "{}");
        assert_eq!(
            seed(&new, VERSION, std::slice::from_ref(&donor), later()),
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
        drop(first);
        // A process another test forks in this instant shares the lock until
        // it execs, so the release can take a moment to show.
        let retaken = (0..200).any(|_| {
            try_lock(&path).is_some() || {
                std::thread::sleep(std::time::Duration::from_millis(10));
                false
            }
        });
        assert!(retaken, "the lock was not released");
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
