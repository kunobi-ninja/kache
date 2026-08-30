//! Runtime identity for native rustc links that dep-info does not enumerate.
//!
//! A bin/dylib/cdylib/proc-macro on the host is linked against CRT objects,
//! libc, and (on macOS) an SDK that rustc never lists. Two hosts with the same
//! `cc --version` banner can still produce incompatible binaries. This module
//! resolves those inputs so the cache key can pin them, and fails closed when
//! the essentials cannot be placed — the wrapper then passes through rather
//! than sharing a binary neither host identified.

use anyhow::{Context, Result, bail};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

/// What the driver is asked to place, and which of it a key cannot do without.
///
/// Chosen with `cfg!` rather than `#[cfg]` so both tables compile everywhere;
/// a table only one platform builds is a table only that platform's CI can
/// catch a mistake in.
#[derive(Clone, Copy)]
pub(crate) struct FileProbes {
    /// The object that starts a program. One of these must resolve.
    startup: &'static [&'static str],
    /// Names a libc goes by. One must resolve too.
    libc: &'static [&'static str],
    /// Constructor/destructor objects. Individually optional.
    rest: &'static [&'static str],
}

const LINUX_PROBES: FileProbes = FileProbes {
    startup: &["Scrt1.o", "crt1.o", "rcrt1.o"],
    libc: &["libc.so.6", "libc.so", "libc.a"],
    rest: &[
        "crti.o",
        "crtn.o",
        "crtbegin.o",
        "crtbeginS.o",
        "crtbeginT.o",
        "crtend.o",
        "crtendS.o",
    ],
};

/// Resolve CRT/startup/libc objects through `cc -print-file-name=` and hash
/// each file that comes back as an absolute path.
pub(crate) fn probe_linux_crt_objects(driver: &Path) -> Result<BTreeMap<String, String>> {
    probe_files(LINUX_PROBES, |name| print_file_name(driver, name))
}

/// Resolve each probe, hash what came back, and insist on the ones a key
/// cannot describe a link without.
///
/// A probe that does not resolve is left out, so a host that resolves a
/// different set keys differently. That alone is not enough: two hosts
/// failing the *same* probe would agree on a key without ever pinning what
/// that probe stood for. Startup and libc therefore have to resolve.
pub(crate) fn probe_files(
    probes: FileProbes,
    place: impl Fn(&str) -> Option<PathBuf>,
) -> Result<BTreeMap<String, String>> {
    let mut resolved = BTreeMap::new();
    for name in probes
        .startup
        .iter()
        .chain(probes.libc)
        .chain(probes.rest)
        .copied()
    {
        let Some(path) = place(name) else {
            continue;
        };
        let digest = hash_placed(&path)
            .with_context(|| format!("hashing linker-placed {name} at {}", path.display()))?;
        resolved.insert(name.to_string(), digest);
    }
    for (required, what) in [(probes.startup, "startup object"), (probes.libc, "libc")] {
        if !required.is_empty() && !required.iter().any(|name| resolved.contains_key(*name)) {
            bail!("the linker driver resolved no {what}, so its links cannot be identified");
        }
    }
    Ok(resolved)
}

fn hash_placed(path: &Path) -> Result<String> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("opening {} for hashing", path.display()))?;
    let mut hasher = blake3::Hasher::new();
    hasher
        .update_reader(file)
        .with_context(|| format!("reading {} for hashing", path.display()))?;
    Ok(hasher.finalize().to_hex().to_string())
}

fn print_file_name(driver: &Path, name: &str) -> Option<PathBuf> {
    let output = Command::new(driver)
        .arg(format!("-print-file-name={name}"))
        .env("LC_ALL", "C")
        .env("LANG", "C")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let reported = String::from_utf8_lossy(&output.stdout);
    let reported = reported.trim();
    if reported.is_empty() {
        return None;
    }
    let path = PathBuf::from(reported);
    // The driver echoes the name back when it cannot place it.
    path.is_absolute()
        .then_some(path)
        .filter(|path| path.is_file())
}

/// Identity of the SDK a macOS link builds against.
///
/// Version + build version pin the libraries. The path is not folded: Command
/// Line Tools vs Xcode spell the same SDK differently and would over-key.
/// `SDKROOT`, when set, is the SDK that is queried — reporting the default
/// SDK's version beside another SDK's root would describe an SDK no link used.
#[cfg(target_os = "macos")]
pub(crate) fn sdk_identity_for(root: Option<String>) -> Result<Option<String>> {
    let sdk = root.as_deref().unwrap_or("macosx");
    let version = xcrun(&["--sdk", sdk, "--show-sdk-version"])
        .ok_or_else(|| anyhow::anyhow!("xcrun --show-sdk-version failed"))?;
    let build = xcrun(&["--sdk", sdk, "--show-sdk-build-version"])
        .ok_or_else(|| anyhow::anyhow!("xcrun --show-sdk-build-version failed"))?;
    Ok(Some(format!("{version} ({build})")))
}

#[cfg(not(target_os = "macos"))]
pub(crate) fn sdk_identity_for(_root: Option<String>) -> Result<Option<String>> {
    Ok(None)
}

#[cfg(target_os = "macos")]
fn xcrun(arguments: &[&str]) -> Option<String> {
    let output = Command::new("xcrun").args(arguments).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .filter(|value| !value.is_empty())
}

/// Encode resolved CRT objects as a stable `name=digest` list.
pub(crate) fn encode_crt_objects(objects: &BTreeMap<String, String>) -> String {
    let mut encoded = String::new();
    for (name, digest) in objects {
        if !encoded.is_empty() {
            encoded.push('\n');
        }
        encoded.push_str(name);
        encoded.push('=');
        encoded.push_str(digest);
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    fn placed(dir: &Path, name: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, name).unwrap();
        path
    }

    #[test]
    fn a_link_is_only_identified_once_its_essentials_resolve() {
        let directory = tempfile::tempdir().unwrap();
        let probes = FileProbes {
            startup: &["Scrt1.o", "crt1.o"],
            libc: &["libc.so.6", "libc.a"],
            rest: &["crti.o"],
        };

        let resolved = probe_files(probes, |name| {
            matches!(name, "crt1.o" | "libc.a").then(|| placed(directory.path(), name))
        })
        .unwrap();
        assert_eq!(
            resolved.keys().collect::<Vec<_>>(),
            ["crt1.o", "libc.a"],
            "only what resolved belongs in the key"
        );

        assert!(
            probe_files(probes, |name| {
                (name == "crt1.o").then(|| placed(directory.path(), name))
            })
            .is_err(),
            "no libc should refuse"
        );
        assert!(
            probe_files(probes, |name| {
                (name == "libc.a").then(|| placed(directory.path(), name))
            })
            .is_err(),
            "no startup object should refuse"
        );
        assert!(
            probe_files(probes, |_| None).is_err(),
            "nothing should refuse"
        );

        let glibc = probe_files(probes, |name| {
            matches!(name, "crt1.o" | "libc.so.6").then(|| placed(directory.path(), name))
        })
        .unwrap();
        assert_ne!(glibc, resolved);
    }

    #[test]
    fn a_platform_that_places_nothing_is_still_identified() {
        let probes = FileProbes {
            startup: &[],
            libc: &[],
            rest: &[],
        };
        assert!(probe_files(probes, |_| None).unwrap().is_empty());
    }

    #[test]
    fn encode_crt_objects_is_sorted_and_stable() {
        let mut objects = BTreeMap::new();
        objects.insert("libc.so.6".into(), "bbb".into());
        objects.insert("crt1.o".into(), "aaa".into());
        assert_eq!(encode_crt_objects(&objects), "crt1.o=aaa\nlibc.so.6=bbb");
    }

    #[test]
    fn linux_probes_require_startup_and_libc() {
        assert!(!LINUX_PROBES.startup.is_empty());
        assert!(!LINUX_PROBES.libc.is_empty());
        if cfg!(target_os = "linux") {
            assert_eq!(LINUX_PROBES.startup, ["Scrt1.o", "crt1.o", "rcrt1.o"]);
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn the_probe_describes_this_host() {
        let Ok(driver) = which_cc() else {
            return;
        };
        let Ok(objects) = probe_linux_crt_objects(&driver) else {
            return;
        };
        assert!(
            LINUX_PROBES
                .startup
                .iter()
                .any(|name| objects.contains_key(*name)),
            "resolved CRT must pin a startup object: {objects:?}"
        );
        assert!(
            LINUX_PROBES
                .libc
                .iter()
                .any(|name| objects.contains_key(*name)),
            "resolved CRT must pin a libc: {objects:?}"
        );
        assert!(objects.values().all(|d| d.len() == 64));
    }

    #[cfg(target_os = "linux")]
    fn which_cc() -> Result<PathBuf> {
        let path_var = std::env::var_os("PATH").context("PATH unset")?;
        std::env::split_paths(&path_var)
            .map(|dir| dir.join("cc"))
            .find(|p| p.is_file())
            .context("cc not on PATH")
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn the_sdk_identity_follows_sdkroot() {
        let Ok(Some(_)) = sdk_identity_for(None) else {
            return;
        };
        let overridden = sdk_identity_for(Some("/nonexistent.sdk".into()));
        assert!(
            overridden.is_err(),
            "an unusable SDKROOT must not report the default SDK: {overridden:?}"
        );
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn sdk_identity_is_none_off_macos() {
        assert_eq!(sdk_identity_for(None).unwrap(), None);
        assert_eq!(
            sdk_identity_for(Some(
                "/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk".into()
            ))
            .unwrap(),
            None
        );
    }
}
