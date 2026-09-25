//! Where Cargo puts a unit's files, recognized from the paths rustc and a
//! build script are given.
//!
//! Cargo has two layouts under a profile directory (`<target>/debug`,
//! `<target>/<triple>/release`, ...):
//!
//! - Before 1.100, units share directories: libraries, binaries and tests in
//!   `<profile>/deps`, examples in `<profile>/examples`, and each build
//!   script in `<profile>/build/<pkg>-<hash>`, with its `OUT_DIR` in `out/`
//!   and its stdout in `output` beside it.
//! - From 1.100, every unit has its own directory
//!   `<profile>/build/<pkg>/<hash>`. rustc writes into `out/`, a build
//!   script's run keeps its `OUT_DIR` in `out/` and its stdout in
//!   `run/stdout`, and the fingerprint lives in `fingerprint/`.
//!
//! Every function here recognizes both, and returns `None` for a path in
//! neither. [`is_cargo_unit_dir`] says whether any of them applies, so a
//! compile Cargo drove from a directory none of them know can be reported
//! instead of losing caching silently.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};

/// One unit's `out` directory in the per-unit layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Unit<'a> {
    pub profile: &'a Path,
    pub package: &'a str,
    pub hash: &'a str,
}

impl Unit<'_> {
    /// The `<pkg>-<hash>` name the same unit has in the legacy layout.
    pub(crate) fn legacy_name(&self) -> String {
        format!("{}-{}", self.package, self.hash)
    }
}

/// Is `hash` the 16 hex digits Cargo names a unit by?
pub(crate) fn is_unit_hash(hash: &str) -> bool {
    hash.len() == 16 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// The package of a legacy `<pkg>-<16 hex>` unit directory name.
pub(crate) fn legacy_unit_package(name: &str) -> Option<&str> {
    let (package, hash) = name.rsplit_once('-')?;
    (!package.is_empty() && is_unit_hash(hash)).then_some(package)
}

/// `dir` as `<profile>/build/<pkg>/<hash>/out`.
pub(crate) fn per_unit_out_dir(dir: &Path) -> Option<Unit<'_>> {
    if dir.file_name()? != "out" {
        return None;
    }
    let hash_dir = dir.parent()?;
    let package_dir = hash_dir.parent()?;
    let build = package_dir.parent()?;
    if build.file_name()? != "build" {
        return None;
    }
    let hash = hash_dir.file_name()?.to_str()?;
    let package = package_dir.file_name()?.to_str()?;
    (is_unit_hash(hash) && !package.is_empty()).then_some(Unit {
        profile: build.parent()?,
        package,
        hash,
    })
}

/// The directory rustc writes a build script's binary into:
/// `<profile>/build/<unit>` in the legacy layout, or the unit's `out`.
pub(crate) fn is_build_script_dir(dir: &Path) -> bool {
    dir.parent().and_then(Path::file_name) == Some(OsStr::new("build"))
        || per_unit_out_dir(dir).is_some()
}

/// The profile directory above the directory holding a build script's
/// binary, when [`is_build_script_dir`] accepts it.
#[cfg_attr(not(unix), allow(dead_code))]
pub(crate) fn build_script_dir_profile(dir: &Path) -> Option<&Path> {
    if let Some(unit) = per_unit_out_dir(dir) {
        return Some(unit.profile);
    }
    let build = dir.parent()?;
    (build.file_name()? == "build").then(|| build.parent())?
}

/// The profile directory above a build script's `OUT_DIR`:
/// `<profile>/build/<unit>/out` or `<profile>/build/<pkg>/<hash>/out`.
pub(crate) fn out_dir_profile(out_dir: &Path) -> Option<&Path> {
    if let Some(unit) = per_unit_out_dir(out_dir) {
        return Some(unit.profile);
    }
    let build = out_dir.parent()?.parent()?;
    (build.file_name()? == "build").then(|| build.parent())?
}

/// The `<pkg>-<hash>` a build script's `OUT_DIR` belongs to, when it is
/// Cargo's for package `pkg`. The per-unit layout's unit gets the name it
/// would have in the legacy layout.
pub(crate) fn out_dir_unit_name(out_dir: &Path, pkg: &str) -> Option<String> {
    if let Some(unit) = per_unit_out_dir(out_dir) {
        return (unit.package == pkg).then(|| unit.legacy_name());
    }
    if out_dir.file_name()? != "out" {
        return None;
    }
    let unit = out_dir.parent()?;
    if unit.parent()?.file_name()? != "build" {
        return None;
    }
    let name = unit.file_name()?.to_str()?;
    let hash = name.strip_prefix(pkg)?.strip_prefix('-')?;
    is_unit_hash(hash).then(|| name.to_string())
}

/// Where Cargo keeps the stdout of the run that filled `out_dir`.
pub(crate) fn build_script_stdout(out_dir: &Path) -> Option<PathBuf> {
    let unit = out_dir.parent()?;
    Some(if per_unit_out_dir(out_dir).is_some() {
        unit.join("run").join("stdout")
    } else {
        unit.join("output")
    })
}

/// The profile directory of one of Cargo's link output directories, or
/// `None` when `out_dir` is not one.
///
/// Anchored on the directory names Cargo itself uses rather than on the
/// depth below the target directory, because those differ: in the legacy
/// layout a binary and an example land in `<profile>/deps` and
/// `<profile>/examples`, a build script in `<profile>/build/<pkg>-<hash>`.
/// Only `out_dir` and its parent are examined there, so a project that
/// happens to live under a directory called `deps` cannot drag the prefix up
/// to it.
pub(crate) fn profile_dir(out_dir: &Path) -> Option<PathBuf> {
    if let Some(unit) = per_unit_out_dir(out_dir) {
        return Some(unit.profile.to_path_buf());
    }
    let parent = out_dir.parent();
    for cursor in [Some(out_dir), parent].into_iter().flatten() {
        let name = cursor.file_name()?;
        if name == "deps" || name == "examples" || name == "build" {
            return cursor.parent().map(Path::to_path_buf);
        }
    }
    None
}

/// The profile directory of a library, binary or test compile: legacy
/// `<profile>/deps` or a per-unit `out`. Examples and build scripts, which
/// the legacy layout keeps elsewhere, are the caller's to exclude.
pub(crate) fn deps_profile(out_dir: &Path) -> Option<&Path> {
    if let Some(unit) = per_unit_out_dir(out_dir) {
        return Some(unit.profile);
    }
    (out_dir.file_name()? == "deps").then(|| out_dir.parent())?
}

/// The unit `out` directory `dir` sits in under `target`, with the unit's
/// package, when it has one of the shapes
/// `[<triple>/]<profile>/build/<pkg>-<hash>/out` or
/// `[<triple>/]<profile>/build/<pkg>/<hash>/out`.
pub(crate) fn unit_out_dir_under(target: &Path, dir: &Path) -> Option<(PathBuf, String)> {
    let rel = dir.strip_prefix(target).ok()?;
    let components: Vec<&OsStr> = rel.iter().collect();
    let build = components
        .iter()
        .position(|component| *component == "build")
        .filter(|index| (1..=2).contains(index))?;
    let first = components.get(build + 1)?.to_str()?;
    let (package, out) = match legacy_unit_package(first) {
        Some(package) => (package, build + 2),
        None => {
            let hash = components.get(build + 2)?.to_str()?;
            if first.is_empty() || !is_unit_hash(hash) {
                return None;
            }
            (first, build + 3)
        }
    };
    if *components.get(out)? != "out" {
        return None;
    }
    // The directory as spelled, not rejoined: separators must match the
    // argv a caller maps with it.
    let below_out = components.len() - (out + 1);
    let unit_out = dir.ancestors().nth(below_out)?.to_path_buf();
    Some((unit_out, package.to_string()))
}

/// Whether `out_dir` is one of the directories Cargo gives rustc in either
/// layout: a legacy `deps`, `examples`, build-script or `OUT_DIR`
/// directory, or a per-unit `out`.
pub(crate) fn is_cargo_unit_dir(out_dir: &Path) -> bool {
    matches!(
        out_dir.file_name().and_then(OsStr::to_str),
        Some("deps" | "examples")
    ) || is_build_script_dir(out_dir)
        || out_dir_profile(out_dir).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    const HASH: &str = "c83877ccb1f76231";

    fn per_unit(tail: &str) -> PathBuf {
        PathBuf::from(format!("/t/debug/build/{tail}"))
    }

    #[test]
    fn a_unit_hash_is_exactly_sixteen_hex_digits() {
        assert!(is_unit_hash(HASH));
        assert!(is_unit_hash("0123456789ABCDEF"));
        assert!(!is_unit_hash("c83877ccb1f7623"));
        assert!(!is_unit_hash("c83877ccb1f762310"));
        assert!(!is_unit_hash("g83877ccb1f76231"));
    }

    #[test]
    fn a_legacy_unit_name_splits_at_its_last_dash() {
        assert_eq!(
            legacy_unit_package(&format!("aws-lc-sys-{HASH}")),
            Some("aws-lc-sys")
        );
        assert_eq!(legacy_unit_package(&format!("-{HASH}")), None);
        assert_eq!(legacy_unit_package("serde-1"), None);
        assert_eq!(legacy_unit_package(HASH), None);
    }

    #[test]
    fn a_per_unit_out_dir_names_its_profile_package_and_hash() {
        let dir = per_unit(&format!("aws-lc-rs/{HASH}/out"));
        let unit = per_unit_out_dir(&dir).unwrap();
        assert_eq!(unit.profile, Path::new("/t/debug"));
        assert_eq!(unit.package, "aws-lc-rs");
        assert_eq!(unit.hash, HASH);
        assert_eq!(unit.legacy_name(), format!("aws-lc-rs-{HASH}"));

        for (label, path) in [
            ("not out", per_unit(&format!("aws-lc-rs/{HASH}/run"))),
            ("short hash", per_unit("aws-lc-rs/c83877cc/out")),
            ("legacy OUT_DIR", per_unit(&format!("aws-lc-rs-{HASH}/out"))),
            (
                "not under build",
                PathBuf::from(format!("/t/debug/deps/aws-lc-rs/{HASH}/out")),
            ),
            ("too shallow", PathBuf::from(format!("build/{HASH}/out"))),
        ] {
            assert_eq!(per_unit_out_dir(&path), None, "{label}");
        }
    }

    #[test]
    fn a_build_script_dir_is_legacy_under_build_or_a_per_unit_out() {
        let legacy = Path::new("/t/debug/build/pkg-1");
        assert!(is_build_script_dir(legacy));
        assert_eq!(
            build_script_dir_profile(legacy),
            Some(Path::new("/t/debug"))
        );
        let unit = per_unit(&format!("pkg/{HASH}/out"));
        assert!(is_build_script_dir(&unit));
        assert_eq!(build_script_dir_profile(&unit), Some(Path::new("/t/debug")));

        for dir in [
            Path::new("/t/debug/deps"),
            Path::new("/t/debug/build/pkg-1/out"),
            Path::new("/t/debug/build"),
        ] {
            assert!(!is_build_script_dir(dir), "{}", dir.display());
            assert_eq!(build_script_dir_profile(dir), None, "{}", dir.display());
        }
    }

    #[test]
    fn an_out_dir_profile_is_found_in_either_layout() {
        assert_eq!(
            out_dir_profile(Path::new("/t/debug/build/pkg-1/out")),
            Some(Path::new("/t/debug"))
        );
        assert_eq!(
            out_dir_profile(&per_unit(&format!("pkg/{HASH}/out"))),
            Some(Path::new("/t/debug"))
        );
        assert_eq!(out_dir_profile(Path::new("/w/other/pkg-1/out")), None);
        assert_eq!(out_dir_profile(Path::new("/out")), None);
    }

    #[test]
    fn an_out_dir_unit_name_needs_the_package_in_either_layout() {
        let legacy = PathBuf::from(format!("/t/debug/build/my-mac-{HASH}/out"));
        assert_eq!(
            out_dir_unit_name(&legacy, "my-mac").as_deref(),
            Some(format!("my-mac-{HASH}").as_str())
        );
        let unit = per_unit(&format!("my-mac/{HASH}/out"));
        assert_eq!(
            out_dir_unit_name(&unit, "my-mac").as_deref(),
            Some(format!("my-mac-{HASH}").as_str())
        );

        for (label, path, pkg) in [
            ("per-unit, other package", unit.clone(), "other"),
            ("legacy, other package", legacy.clone(), "other"),
            (
                "legacy, not out",
                PathBuf::from(format!("/t/debug/build/my-mac-{HASH}/src")),
                "my-mac",
            ),
            (
                "legacy, not under build",
                PathBuf::from(format!("/t/debug/deps/my-mac-{HASH}/out")),
                "my-mac",
            ),
            (
                "legacy, bad hash",
                PathBuf::from("/t/debug/build/my-mac-1/out"),
                "my-mac",
            ),
            (
                "legacy, no dash",
                PathBuf::from(format!("/t/debug/build/my-mac{HASH}/out")),
                "my-mac",
            ),
        ] {
            assert_eq!(out_dir_unit_name(&path, pkg), None, "{label}");
        }
    }

    #[test]
    fn build_script_stdout_moved_under_run() {
        assert_eq!(
            build_script_stdout(Path::new("/t/debug/build/pkg-1/out")),
            Some(PathBuf::from("/t/debug/build/pkg-1/output"))
        );
        assert_eq!(
            build_script_stdout(&per_unit(&format!("pkg/{HASH}/out"))),
            Some(per_unit(&format!("pkg/{HASH}/run/stdout")))
        );
        assert_eq!(build_script_stdout(Path::new("/")), None);
    }

    #[test]
    fn profile_dir_covers_every_link_output_directory() {
        for dir in [
            "/t/debug/deps",
            "/t/debug/examples",
            "/t/debug/build/pkg-1",
            "/t/debug/deps/nested",
        ] {
            assert_eq!(
                profile_dir(Path::new(dir)),
                Some(PathBuf::from("/t/debug")),
                "{dir}"
            );
        }
        assert_eq!(
            profile_dir(&per_unit(&format!("pkg/{HASH}/out"))),
            Some(PathBuf::from("/t/debug"))
        );
        assert_eq!(profile_dir(Path::new("/t/debug")), None);
        assert_eq!(profile_dir(Path::new("/deps/proj/target/debug")), None);
        assert_eq!(profile_dir(Path::new("/deps")), Some(PathBuf::from("/")));
    }

    #[test]
    fn deps_profile_is_legacy_deps_or_a_per_unit_out() {
        assert_eq!(
            deps_profile(Path::new("/t/debug/deps")),
            Some(Path::new("/t/debug"))
        );
        assert_eq!(
            deps_profile(&per_unit(&format!("pkg/{HASH}/out"))),
            Some(Path::new("/t/debug"))
        );
        assert_eq!(deps_profile(Path::new("/t/debug/examples")), None);
        assert_eq!(deps_profile(Path::new("/t/debug/build/pkg-1")), None);
    }

    #[test]
    fn unit_out_dir_under_finds_either_layout_with_the_package() {
        let target = Path::new("/t");
        for (dir, unit_out) in [
            (
                format!("/t/debug/build/libz-sys-{HASH}/out/include"),
                format!("/t/debug/build/libz-sys-{HASH}/out"),
            ),
            (
                format!("/t/x86_64-unknown-linux-gnu/debug/build/libz-sys-{HASH}/out"),
                format!("/t/x86_64-unknown-linux-gnu/debug/build/libz-sys-{HASH}/out"),
            ),
            (
                format!("/t/debug/build/libz-sys/{HASH}/out/include/zlib"),
                format!("/t/debug/build/libz-sys/{HASH}/out"),
            ),
            (
                format!("/t/x86_64-unknown-linux-gnu/debug/build/libz-sys/{HASH}/out"),
                format!("/t/x86_64-unknown-linux-gnu/debug/build/libz-sys/{HASH}/out"),
            ),
        ] {
            assert_eq!(
                unit_out_dir_under(target, Path::new(&dir)),
                Some((PathBuf::from(unit_out), "libz-sys".to_string())),
                "{dir}"
            );
        }

        for dir in [
            format!("/elsewhere/debug/build/libz-sys-{HASH}/out"),
            format!("/t/build/libz-sys-{HASH}/out"),
            format!("/t/a/b/c/build/libz-sys-{HASH}/out"),
            format!("/t/debug/build/libz-sys-{HASH}/include"),
            format!("/t/debug/build/libz-sys/{HASH}/run"),
            "/t/debug/build/libz-sys/c83877cc/out".to_string(),
            format!("/t/debug/build/libz-sys-{HASH}"),
            format!("/t/debug/build/libz-sys/{HASH}"),
        ] {
            assert_eq!(unit_out_dir_under(target, Path::new(&dir)), None, "{dir}");
        }
    }

    #[test]
    fn every_directory_cargo_gives_rustc_is_a_unit_dir() {
        for dir in [
            "/t/debug/deps".to_string(),
            "/t/debug/examples".to_string(),
            "/t/debug/build/pkg-1".to_string(),
            "/t/debug/build/pkg-1/out".to_string(),
            format!("/t/debug/build/pkg/{HASH}/out"),
        ] {
            assert!(is_cargo_unit_dir(Path::new(&dir)), "{dir}");
        }
        for dir in ["/t/debug", "/tmp/probe", "/t/debug/units/pkg/out"] {
            assert!(!is_cargo_unit_dir(Path::new(dir)), "{dir}");
        }
    }
}
