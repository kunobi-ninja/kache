//! Runtime identity for native rustc links that dep-info does not enumerate.
//!
//! A bin/dylib/cdylib/proc-macro on the host is linked against CRT objects,
//! libc, and (on macOS) an SDK that rustc never lists. Two hosts with the same
//! `cc --version` banner can still produce incompatible binaries. This module
//! resolves those inputs so the cache key can pin them, and fails closed when
//! the essentials cannot be placed — the wrapper then passes through rather
//! than sharing a binary neither host identified.
//!
//! # Windows MSVC
//!
//! A native `*-windows-msvc` link is keyed by the validated `link.exe` or
//! `lld-link` banner, the `cl.exe` banner, the selected architecture, the
//! MSVC/SDK/UCRT versions, the bytes of every CRT/vcruntime/UCRT library the
//! search path exposes, and the bytes of each `-l` library resolved through
//! `-L`, `/LIBPATH` and `LIB` in the order LINK would use.
//!
//! Everything else fails closed to passthrough: `LINK`/`_LINK_` option
//! variables, a linker other than `link.exe`/`lld-link`, an ambiguous or
//! missing `-l` library, cross-target and windows-gnu links, and any
//! `-C link-arg` that hands LINK a file the identity does not hash. That last
//! group is decided by [`windows_link_argument_has_unmodeled_input`]:
//! `.lib`/`.a`/`.obj`/`.o`/`.res`/`.def`/`.exp`/`.manifest` inputs and
//! file-carrying options such as `/DEF:`, `/MANIFESTINPUT:`,
//! `/MANIFESTFILE:` and `/PDBSTRIPPED:` are only text in the key, so a file
//! rebuilt under an unchanged name would otherwise restore a stale executable.

use anyhow::{Context, Result, bail};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::probe_memo::{self, Material};

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
/// each file that comes back as an absolute path. The memoised form below is
/// what the key uses; this is the reference the tests compare it against.
#[cfg(all(test, unix))]
pub(crate) fn probe_linux_crt_objects(
    driver: &Path,
    hash: impl Fn(&Path) -> Result<String>,
) -> Result<BTreeMap<String, String>> {
    let _trace = crate::phase_trace::phase("native_crt");
    probe_files_with_hash(LINUX_PROBES, |name| print_file_name(driver, name), hash)
}

/// [`probe_linux_crt_objects`] with the driver's placements remembered.
///
/// Placing thirteen names costs thirteen driver spawns per linked output,
/// which on a build-script-heavy warm build is most of what a hit spends.
/// The placements only depend on the driver, the variables that steer its
/// search, and what the searched directories contain, so they are memoised
/// under the first two and validated against the third: every directory the
/// driver searches, and every directory a placement came from, must still
/// carry the modification stamp it had when the memo was written. A file
/// added to or removed from any of them changes that stamp, so a name that
/// would now place differently is probed again. Content is still hashed
/// through `hash` on every call, as before.
pub(crate) fn probe_linux_crt_objects_memoized(
    memo_dir: &Path,
    driver: &Path,
    hash: impl Fn(&Path) -> Result<String>,
) -> Result<BTreeMap<String, String>> {
    let _trace = crate::phase_trace::phase("native_crt");
    let placements = match crt_placement_memo(memo_dir, driver) {
        Some(placed) => placed,
        None => {
            let placed = place_linux_crt_objects(driver);
            record_crt_placement_memo(memo_dir, driver, &placed);
            placed
        }
    };
    probe_files_with_hash(LINUX_PROBES, |name| placements.get(name).cloned(), hash)
}

fn place_linux_crt_objects(driver: &Path) -> BTreeMap<String, PathBuf> {
    LINUX_PROBES
        .startup
        .iter()
        .chain(LINUX_PROBES.libc)
        .chain(LINUX_PROBES.rest)
        .filter_map(|name| Some((name.to_string(), print_file_name(driver, name)?)))
        .collect()
}

#[derive(serde::Serialize, serde::Deserialize)]
struct CrtPlacementMemo {
    /// Name to absolute path, for the names the driver placed.
    placed: BTreeMap<String, PathBuf>,
    /// Directory to `mtime_ns:ctime_ns` for every directory that decides a
    /// placement: the driver's library search directories and the parent of
    /// each placed file.
    stamps: BTreeMap<PathBuf, String>,
}

/// Variables that change where a GCC or Clang driver looks for files.
const CRT_SEARCH_ENV: &[&str] = &["LIBRARY_PATH", "GCC_EXEC_PREFIX", "COMPILER_PATH"];

fn crt_memo_material(driver: &Path) -> Option<(PathBuf, String)> {
    let canonical = std::fs::canonicalize(driver).ok()?;
    let metadata = std::fs::metadata(&canonical).ok()?;
    let mut material = Material::new("linux-crt-placements-v1");
    // Both spellings: a driver that dispatches on its own name (`ccache`,
    // `clang` via `aarch64-linux-gnu-clang`) canonicalizes to one binary and
    // places differently under each name.
    material.push(driver.as_os_str().as_encoded_bytes());
    material.push(canonical.as_os_str().as_encoded_bytes());
    material.push(&metadata.len().to_le_bytes());
    material.push(&crate::cache_key::metadata_mtime_ns(&metadata).to_le_bytes());
    for name in CRT_SEARCH_ENV {
        material.push(name.as_bytes());
        material.push(
            &std::env::var_os(name)
                .map(|value| value.as_encoded_bytes().to_vec())
                .unwrap_or_default(),
        );
    }
    Some((canonical, material.digest()))
}

fn directory_stamp(directory: &Path) -> Option<String> {
    let metadata = std::fs::metadata(directory).ok()?;
    Some(format!(
        "{}:{}",
        crate::cache_key::metadata_mtime_ns(&metadata),
        crate::cache_key::metadata_ctime_ns(&metadata)
    ))
}

fn crt_placement_memo(memo_dir: &Path, driver: &Path) -> Option<BTreeMap<String, PathBuf>> {
    let (_, digest) = crt_memo_material(driver)?;
    let path = probe_memo::memo_path(memo_dir, "crt-placements", "json", &digest);
    let body = probe_memo::read_verified(&path, &digest)?;
    let memo: CrtPlacementMemo = serde_json::from_str(&body).ok()?;
    if memo.stamps.is_empty() {
        return None;
    }
    for (directory, stamp) in &memo.stamps {
        if directory_stamp(directory).as_ref() != Some(stamp) {
            return None;
        }
    }
    memo.placed
        .values()
        .all(|path| path.is_file())
        .then_some(memo.placed)
}

fn record_crt_placement_memo(memo_dir: &Path, driver: &Path, placed: &BTreeMap<String, PathBuf>) {
    let Some((canonical, digest)) = crt_memo_material(driver) else {
        return;
    };
    let mut directories: Vec<PathBuf> = driver_library_search_dirs(&canonical);
    directories.extend(
        placed
            .values()
            .filter_map(|path| path.parent().map(Path::to_path_buf)),
    );
    let mut stamps = BTreeMap::new();
    for directory in directories {
        let Ok(directory) = std::fs::canonicalize(&directory) else {
            continue;
        };
        if let Some(stamp) = directory_stamp(&directory) {
            stamps.insert(directory, stamp);
        }
    }
    if stamps.is_empty() {
        return;
    }
    let memo = CrtPlacementMemo {
        placed: placed.clone(),
        stamps,
    };
    if let Ok(body) = serde_json::to_string(&memo) {
        probe_memo::write_verified(
            &probe_memo::memo_path(memo_dir, "crt-placements", "json", &digest),
            &digest,
            &body,
        );
    }
}

/// The `libraries:` line of `cc -print-search-dirs`, as directories that exist.
fn driver_library_search_dirs(driver: &Path) -> Vec<PathBuf> {
    let output = match Command::new(driver)
        .arg("-print-search-dirs")
        .env("LC_ALL", "C")
        .env("LANG", "C")
        .output()
    {
        Ok(output) => output,
        Err(error) => {
            tracing::debug!(
                driver = %driver.display(),
                %error,
                "could not run the driver for -print-search-dirs"
            );
            return Vec::new();
        }
    };
    if !output.status.success() {
        return Vec::new();
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let Some(line) = text
        .lines()
        .find_map(|line| line.strip_prefix("libraries:"))
    else {
        return Vec::new();
    };
    let line = line.trim().trim_start_matches('=');
    std::env::split_paths(line)
        .filter(|dir| dir.is_absolute() && dir.is_dir())
        .collect()
}

/// Resolve each probe, hash what came back, and insist on the ones a key
/// cannot describe a link without.
///
/// A probe that does not resolve is left out, so a host that resolves a
/// different set keys differently. That alone is not enough: two hosts
/// failing the *same* probe would agree on a key without ever pinning what
/// that probe stood for. Startup and libc therefore have to resolve.
#[cfg(test)]
fn probe_files(
    probes: FileProbes,
    place: impl Fn(&str) -> Option<PathBuf>,
) -> Result<BTreeMap<String, String>> {
    probe_files_with_hash(probes, place, hash_placed)
}

fn probe_files_with_hash(
    probes: FileProbes,
    place: impl Fn(&str) -> Option<PathBuf>,
    hash: impl Fn(&Path) -> Result<String>,
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
        let _trace = crate::phase_trace::phase("native_crt_hash");
        let digest = hash(&path)
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
    let _trace = crate::phase_trace::phase("native_crt_resolve");
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
///
/// Apple's `xcrun` accepts a filesystem path as `--sdk`. xcbuild's `xcrun`
/// only accepts a name such as `macosx`. When `xcrun` rejects a path that is
/// still a real SDK directory, read the same version and build from
/// `SystemVersion.plist` inside that root.
#[cfg(target_os = "macos")]
pub(crate) fn sdk_identity_for(root: Option<String>) -> Result<Option<String>> {
    let sdk = root.as_deref().unwrap_or("macosx");
    if let Some(identity) = xcrun_identity(sdk) {
        return Ok(Some(identity));
    }
    let path = Path::new(sdk);
    if path.is_dir() {
        return identity_from_sdk_root(path).map(Some);
    }
    bail!("xcrun --show-sdk-version failed");
}

#[cfg(target_os = "macos")]
fn xcrun_identity(sdk: &str) -> Option<String> {
    let version = xcrun(&["--sdk", sdk, "--show-sdk-version"])?;
    let build = xcrun(&["--sdk", sdk, "--show-sdk-build-version"])?;
    Some(format!("{version} ({build})"))
}

/// `ProductVersion` / `ProductBuildVersion` as `xcrun --show-sdk-version` and
/// `--show-sdk-build-version` report them for this root.
///
/// Compiled in tests on every OS so the Linux mutation lane can kill mutants
/// in the plist fallback (`#[cfg(any(test, target_os = "macos"))]`).
#[cfg(any(test, target_os = "macos"))]
fn identity_from_sdk_root(root: &Path) -> Result<String> {
    let plist = root.join("System/Library/CoreServices/SystemVersion.plist");
    let body = std::fs::read_to_string(&plist).with_context(|| {
        format!(
            "xcrun rejected --sdk {} and {} is unreadable",
            root.display(),
            plist.display()
        )
    })?;
    let version = plist_string(&body, "ProductVersion").ok_or_else(|| {
        anyhow::anyhow!(
            "{} has no ProductVersion; not a usable macOS SDK",
            plist.display()
        )
    })?;
    let build = plist_string(&body, "ProductBuildVersion").ok_or_else(|| {
        anyhow::anyhow!(
            "{} has no ProductBuildVersion; not a usable macOS SDK",
            plist.display()
        )
    })?;
    Ok(format!("{version} ({build})"))
}

#[cfg(any(test, target_os = "macos"))]
fn plist_string(body: &str, key: &str) -> Option<String> {
    let needle = format!("<key>{key}</key>");
    let rest = body.split(&needle).nth(1)?;
    let rest = rest.trim_start().strip_prefix("<string>")?;
    let (value, _) = rest.split_once("</string>")?;
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
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

/// The tools and SDK selected by a native MSVC link.
///
/// The values are intentionally banners and versions rather than paths. The
/// paths select the tools and libraries on this machine; absolute paths would
/// make a cache key differ for every Visual Studio installation. Library
/// contents are retained as hashes because a version directory can be
/// replaced in place by a servicing update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WindowsMsvcIdentity {
    pub(crate) linker: String,
    pub(crate) compiler: String,
    pub(crate) toolset: String,
    pub(crate) sdk: String,
    pub(crate) ucrt: String,
    pub(crate) architecture: String,
    pub(crate) libraries: BTreeMap<String, String>,
    /// Wrapper script digest and extra flags it injects (Firefox
    /// `cargo-host-linker.bat` via `MOZ_CARGO_WRAP_*`). Empty when rustc
    /// invoked `link.exe` / `lld-link.exe` directly.
    pub(crate) wrapper: Option<(String, Vec<String>)>,
}

impl WindowsMsvcIdentity {
    /// Stable, sorted representation suitable for a length-prefixed key field.
    pub(crate) fn encode(&self) -> String {
        let mut fields = vec![
            format!("architecture={}", self.architecture),
            format!("compiler={}", self.compiler),
            format!("linker={}", self.linker),
            format!("msvc={}", self.toolset),
            format!("sdk={}", self.sdk),
            format!("ucrt={}", self.ucrt),
        ];
        if let Some((digest, extra)) = &self.wrapper {
            fields.push(format!("wrapper={digest}"));
            for flag in extra {
                fields.push(format!("wrapper_flag={flag}"));
            }
        }
        fields.extend(
            self.libraries
                .iter()
                .map(|(name, digest)| format!("lib:{name}={digest}")),
        );
        fields.join("\n")
    }
}

/// A snapshot of the Windows toolchain environment. Keeping this as an input
/// makes discovery tests deterministic on non-Windows hosts and avoids tests
/// accidentally probing the developer's installed toolchain.
#[derive(Debug, Clone, Default)]
pub(crate) struct WindowsProbeEnvironment {
    pub(crate) variables: BTreeMap<String, String>,
    pub(crate) path: Vec<PathBuf>,
    pub(crate) cwd: Option<PathBuf>,
    default_linker: Option<PathBuf>,
    compiler: Option<PathBuf>,
    linker_command_env: Vec<(OsString, OsString)>,
    compiler_command_env: Vec<(OsString, OsString)>,
}

fn collect_unicode_environment(
    variables: impl IntoIterator<Item = (OsString, OsString)>,
) -> Result<BTreeMap<String, String>> {
    variables
        .into_iter()
        .map(|(name, value)| {
            let name = name.into_string().map_err(|_| {
                anyhow::anyhow!("process environment contains a non-Unicode variable name")
            })?;
            let value = value.into_string().map_err(|_| {
                anyhow::anyhow!("process environment variable {name} has a non-Unicode value")
            })?;
            Ok((name, value))
        })
        .collect()
}

impl WindowsProbeEnvironment {
    pub(crate) fn current() -> Result<Self> {
        let variables = collect_unicode_environment(std::env::vars_os())?;
        let path = std::env::var_os("PATH")
            .map(|value| std::env::split_paths(&value).collect())
            .unwrap_or_default();
        let cwd = std::env::current_dir().ok();
        Ok(Self {
            variables,
            path,
            cwd,
            ..Self::default()
        })
    }

    fn var(&self, name: &str) -> Option<&str> {
        self.variables
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
            .filter(|value| !value.trim().is_empty())
    }

    #[cfg(any(windows, test))]
    fn contains_var(&self, name: &str) -> bool {
        self.variables
            .keys()
            .any(|candidate| candidate.eq_ignore_ascii_case(name))
    }

    #[cfg(any(windows, test))]
    fn set_var(&mut self, name: impl Into<String>, value: impl Into<String>) {
        let name = name.into();
        self.variables
            .retain(|candidate, _| !candidate.eq_ignore_ascii_case(&name));
        self.variables.insert(name, value.into());
    }

    /// `find-msvc-tools` may run `cl.exe` without arguments to infer the
    /// developer prompt's target architecture. `CL` and `_CL_` are implicit
    /// compiler arguments, so that probe could compile a caller-provided file.
    /// Refuse only the environment shape which reaches that fallback.
    #[cfg(any(windows, test))]
    fn validate_discovery_probe(&self) -> Result<()> {
        // find-msvc-tools treats even an empty marker as evidence that it is
        // running inside a developer environment.
        let developer_environment =
            self.contains_var("VCINSTALLDIR") || self.contains_var("VSTEL_MSBuildProjectFullPath");
        let target_requires_probe = self.var("VSCMD_ARG_TGT_ARCH").is_none();
        let compiler_inputs = self.var("CL").is_some() || self.var("_CL_").is_some();
        if developer_environment && target_requires_probe && compiler_inputs {
            bail!(
                "cannot discover MSVC safely: CL/_CL_ may be executed while the developer prompt target is unknown"
            );
        }
        Ok(())
    }

    #[cfg(any(windows, test))]
    fn set_var_if_missing(&mut self, name: &str, value: impl Into<String>) {
        if self.var(name).is_none() {
            self.set_var(name, value);
        }
    }

    #[cfg(any(windows, test))]
    fn refresh_path(&mut self) {
        if let Some(path) = self.var("PATH") {
            self.path = std::env::split_paths(path).collect();
        }
    }

    /// Installed-toolchain discovery. Not memoised: which toolset and SDK
    /// discovery selects depends on state only `find-msvc-tools` reads (the
    /// toolset default file, SDK registry roots, per-version eligibility),
    /// so a memo keyed on anything less would serve a stale selection.
    #[cfg(windows)]
    fn augment_from_installed_msvc(&mut self, architecture: &str) -> Result<()> {
        self.validate_discovery_probe()?;
        let linker = find_msvc_tools::find_tool(architecture, "link.exe");
        let compiler = find_msvc_tools::find_tool(architecture, "cl.exe");

        // rustc applies the environment returned for link.exe, including for
        // an explicit MSVC linker. A separately discovered cl.exe environment
        // is only relevant while running our compiler banner probe.
        if let Some(tool) = linker.as_ref() {
            for (name, value) in tool.env() {
                let name = name
                    .to_str()
                    .context("MSVC discovery returned a non-Unicode environment name")?;
                let value = value
                    .to_str()
                    .context("MSVC discovery returned a non-Unicode environment value")?;
                self.set_var(name, value);
            }
            self.refresh_path();
            if let Some(version) = path_version(&tool.path().to_string_lossy(), "MSVC") {
                self.set_var_if_missing("VCToolsVersion", version);
            }
            if let Some(sdk) = find_msvc_tools::find_windows_sdk(architecture) {
                self.set_var_if_missing("WindowsSDKVersion", sdk.sdk_version());
            }
            if let Some((_, version)) = find_msvc_tools::get_ucrt_dir() {
                self.set_var_if_missing("UCRTVersion", version);
            }
        }

        if let Some(tool) = linker {
            self.default_linker = Some(tool.path().to_path_buf());
            self.linker_command_env = tool.env().into_iter().cloned().collect();
        }
        if let Some(tool) = compiler {
            self.compiler = Some(tool.path().to_path_buf());
            self.compiler_command_env = tool.env().into_iter().cloned().collect();
        }
        Ok(())
    }

    #[cfg(not(windows))]
    fn augment_from_installed_msvc(&mut self, _architecture: &str) -> Result<()> {
        Ok(())
    }

    fn command_environment(&self, tool: WindowsTool, path: &Path) -> &[(OsString, OsString)] {
        match tool {
            WindowsTool::Link | WindowsTool::LldLink => &self.linker_command_env,
            WindowsTool::Cl if self.compiler.as_deref() == Some(path) => &self.compiler_command_env,
            WindowsTool::Cl => &[],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WindowsTool {
    Link,
    LldLink,
    Cl,
}

impl WindowsTool {
    fn name(self) -> &'static str {
        match self {
            Self::Link => "link.exe",
            Self::LldLink => "lld-link.exe",
            Self::Cl => "cl.exe",
        }
    }
}

/// Return the canonical MSVC architecture spelling for a rustc target.
pub(crate) fn windows_msvc_architecture(target: &str) -> Option<&'static str> {
    let arch = target.split('-').next()?.to_ascii_lowercase();
    match arch.as_str() {
        "x86_64" | "amd64" => Some("x64"),
        "i686" | "i586" | "i386" | "x86" => Some("x86"),
        "aarch64" | "arm64" => Some("arm64"),
        "thumbv7a" | "arm" => Some("arm"),
        _ => None,
    }
}

/// Whether `target` is a native Windows MSVC target (as opposed to GNU or a
/// different OS). The caller separately checks that it equals rustc's host.
pub(crate) fn is_windows_msvc_target(target: &str) -> bool {
    target
        .split('-')
        .any(|component| component.eq_ignore_ascii_case("windows"))
        && target
            .split('-')
            .any(|component| component.eq_ignore_ascii_case("msvc"))
}

/// Whether a `-C link-arg`/`link-args` value hands the COFF linker an input
/// the native MSVC identity does not hash.
///
/// The key folds the argument *text*, not the bytes of the files that text
/// names. A `.res`, `.def` or `.obj` rebuilt in place under the same OUT_DIR
/// name would then restore a stale executable, so every such argument fails
/// closed and the link passes through. Two shapes are recognised, after the
/// value is split on the commas and whitespace that `-Wl,` and `link-args`
/// use to carry several tokens:
///
/// - a token naming a linker input by extension (`.lib`, `.a`, `.obj`, `.o`,
///   `.res`, `.def`, `.exp`, `.manifest`), bare or as an option value;
/// - a `/OPTION:` or `-OPTION:` (also `=`) whose value is a file the linker
///   reads or writes beside the executable: `/DEF`, `/DEFAULTLIB`,
///   `/WHOLEARCHIVE:lib`, `/STUB`, `/KEYFILE`, `/PGD`, `/NATVIS`,
///   `/SOURCELINK`, `/MANIFESTINPUT`, the CLR `/ASSEMBLY*` inputs, and the
///   side outputs `/MANIFESTFILE`, `/PDBSTRIPPED`, `/PDB`, `/IMPLIB`, `/ILK`.
///
/// Both checks are ASCII case-insensitive because LINK is. `/LIBPATH` is not
/// listed: its directory is modeled by the caller. `/MAP`, `/ORDER:@file`
/// and `@response` files are refused earlier by the generic side-file check.
/// Libraries requested through `-l` never reach this function; they are
/// resolved and hashed by [`hash_windows_selected_libraries`].
pub(crate) fn windows_link_argument_has_unmodeled_input(value: &str) -> bool {
    value
        .split([',', ' ', '\t', '\n', '\r'])
        .map(|token| token.trim_matches('"'))
        .any(|token| link_token_names_input_file(token) || windows_link_token_is_file_option(token))
}

/// A bare or option-value token whose extension marks a linker input file.
pub(crate) fn link_token_names_input_file(token: &str) -> bool {
    ends_with_ignore_ascii_case(token, ".lib")
        || ends_with_ignore_ascii_case(token, ".a")
        || ends_with_ignore_ascii_case(token, ".obj")
        || ends_with_ignore_ascii_case(token, ".o")
        || ends_with_ignore_ascii_case(token, ".res")
        || ends_with_ignore_ascii_case(token, ".def")
        || ends_with_ignore_ascii_case(token, ".exp")
        || ends_with_ignore_ascii_case(token, ".manifest")
}

/// A `/NAME:value` or `-NAME=value` option whose value is a file the identity
/// neither hashes nor captures. A bare `/WHOLEARCHIVE` applies to the inputs
/// rustc already passes and carries no file of its own.
fn windows_link_token_is_file_option(token: &str) -> bool {
    let Some(option) = token.strip_prefix(['/', '-']) else {
        return false;
    };
    let (name, value) = match option.split_once([':', '=']) {
        Some((name, value)) => (name, Some(value)),
        None => (option, None),
    };
    let named = |candidate: &str| name.eq_ignore_ascii_case(candidate);
    named("DEF")
        || named("DEFAULTLIB")
        || (named("WHOLEARCHIVE") && value.is_some())
        || named("STUB")
        || named("KEYFILE")
        || named("PGD")
        || named("NATVIS")
        || named("SOURCELINK")
        || named("MANIFESTINPUT")
        || named("ASSEMBLYMODULE")
        || named("ASSEMBLYRESOURCE")
        || named("ASSEMBLYLINKRESOURCE")
        || named("MANIFESTFILE")
        || named("PDBSTRIPPED")
        || named("PDB")
        || named("IMPLIB")
        || named("ILK")
}

pub(crate) fn ends_with_ignore_ascii_case(token: &str, suffix: &str) -> bool {
    token
        .len()
        .checked_sub(suffix.len())
        .and_then(|start| token.get(start..))
        .is_some_and(|tail| tail.eq_ignore_ascii_case(suffix))
}

/// What a `-C link-arg`/`link-args` value hands a Unix linker driver besides
/// flags: input files named by path, `-L` search directories and `-l`
/// libraries.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct LinkArgInputs {
    /// Linker inputs named by path, in argument order.
    pub files: Vec<PathBuf>,
    /// `-L` search directories, in argument order.
    pub dirs: Vec<PathBuf>,
    /// `-l` libraries as `(name, verbatim)`, in argument order. `-l:file`
    /// names the file itself.
    pub libs: Vec<(String, bool)>,
}

/// Parse one `link-arg` or `link-args` value (`key`) for a Unix link.
///
/// A `link-arg` is one driver argument and `link-args` splits on whitespace;
/// a `-Wl,` argument carries comma-separated linker tokens. A token that names
/// an input file by extension (see [`link_token_names_input_file`]), bare or
/// as the value of a `--option=value`, must be an absolute path, so the key
/// can hash the file the linker reads. The value of an option that names an
/// output or a pattern is skipped (see [`takes_non_input_value`]). `-L<dir>`
/// and `-L <dir>` add a search directory; any other search-path spelling
/// (`-L=`, `--library-path`, a dangling `-L`) is an error rather than a
/// directory we would not search. `-l<name>`, `-l <name>`, `--library=<name>`,
/// `--library <name>` and ld64's `-hidden-l<name>` and its kin (see
/// [`ld64_library_option`]) add a library for the caller to resolve.
pub(crate) fn unix_link_arg_inputs(key: &str, value: &str) -> Result<LinkArgInputs> {
    let arguments: Vec<&str> = if key == "link-args" {
        value.split_whitespace().collect()
    } else {
        vec![value]
    };
    let mut tokens = Vec::new();
    for argument in arguments {
        match argument.strip_prefix("-Wl,") {
            Some(payload) => tokens.extend(payload.split(',')),
            None => tokens.push(argument),
        }
    }
    let mut inputs = LinkArgInputs::default();
    let mut tokens = tokens.into_iter();
    let value_of = |option: &str, next: Option<&str>| {
        next.filter(|next| !next.is_empty())
            .map(str::to_string)
            .with_context(|| format!("`{option}` without a value in linker argument {value:?}"))
    };
    while let Some(token) = tokens.next() {
        if token == "-L" {
            inputs
                .dirs
                .push(PathBuf::from(value_of(token, tokens.next())?));
        } else if token.starts_with("-L=") || token.starts_with("--library-path") {
            bail!("unmodeled library search path {token:?} in linker argument {value:?}");
        } else if let Some(dir) = token.strip_prefix("-L") {
            inputs.dirs.push(PathBuf::from(dir));
        } else if token == "-l" || token == "--library" {
            inputs
                .libs
                .push(library_request(&value_of(token, tokens.next())?));
        } else if let Some(name) = token
            .strip_prefix("--library=")
            .or_else(|| token.strip_prefix("-l"))
            .or_else(|| ld64_library_option(token))
        {
            if name.is_empty() {
                bail!("`{token}` without a library in linker argument {value:?}");
            }
            inputs.libs.push(library_request(name));
        } else if takes_non_input_value(token) {
            if !token.contains('=') {
                tokens.next();
            }
        } else {
            let operand = match token.strip_prefix("--") {
                Some(_) => token.split_once('=').map_or(token, |(_, value)| value),
                None => token,
            };
            if link_token_names_input_file(operand) {
                if !Path::new(operand).is_absolute() {
                    bail!("linker input {operand:?} is not an absolute path");
                }
                inputs.files.push(PathBuf::from(operand));
            }
        }
    }
    Ok(inputs)
}

/// The name an ld64 `-<modifier>-l<name>` option asks for: `man ld` says each
/// of these looks the library up like `-l`, and `-hidden-l` and `-force-l`
/// take only an archive. `None` for any other token, `-rpath-link` included.
/// A token that starts with `-l` is a plain `-l`, as GNU ld reads it.
fn ld64_library_option(token: &str) -> Option<&str> {
    let (modifier, name) = token.strip_prefix('-')?.split_once("-l")?;
    matches!(
        modifier,
        "hidden"
            | "force"
            | "needed"
            | "reexport"
            | "upward"
            | "weak"
            | "assert-weak"
            | "delay"
            | "merge"
    )
    .then_some(name)
}

/// A `-l` name as `(name, verbatim)`: `:file` names the file itself.
fn library_request(name: &str) -> (String, bool) {
    match name.strip_prefix(':') {
        Some(file) => (file.to_string(), true),
        None => (name.to_string(), false),
    }
}

/// Whether `token` is a linker option whose value names an output or a
/// pattern, though it may end like an input: `--exclude-libs libssl.a`,
/// `--out-implib foo.dll.a`, `-object_path_lto lto.o`. Either dash count, with
/// the value after `=` or in the next token.
fn takes_non_input_value(token: &str) -> bool {
    let option = token.split_once('=').map_or(token, |(option, _)| option);
    option.strip_prefix('-').is_some_and(|option| {
        matches!(
            option.trim_start_matches('-'),
            "exclude-libs" | "out-implib" | "object_path_lto"
        )
    })
}

fn windows_tool_from_name(path: &Path) -> Option<WindowsTool> {
    let name = path.file_name()?.to_string_lossy().to_ascii_lowercase();
    let name = name.strip_suffix(".exe").unwrap_or(&name);
    match name {
        "link" => Some(WindowsTool::Link),
        "lld-link" => Some(WindowsTool::LldLink),
        "cl" => Some(WindowsTool::Cl),
        _ => None,
    }
}

fn path_lookup(environment: &WindowsProbeEnvironment, name: &str) -> Option<PathBuf> {
    let wanted = name.to_ascii_lowercase();
    let wanted = wanted.strip_suffix(".exe").unwrap_or(&wanted);
    environment.path.iter().find_map(|directory| {
        let candidate = directory.join(name);
        candidate.is_file().then_some(candidate).or_else(|| {
            // Tests and a few Unix-hosted Windows SDK shims omit `.exe` from
            // their fixture names; Windows itself is case-insensitive.
            std::fs::read_dir(directory)
                .ok()?
                .flatten()
                .find_map(|entry| {
                    let entry_name = entry.file_name().to_string_lossy().to_ascii_lowercase();
                    let entry_name = entry_name.strip_suffix(".exe").unwrap_or(&entry_name);
                    (entry_name == wanted)
                        .then_some(entry.path())
                        .filter(|path| path.is_file())
                })
        })
    })
}

fn is_path_like(path: &Path) -> bool {
    path.to_string_lossy()
        .chars()
        .any(|character| matches!(character, '/' | '\\'))
}

fn architecture_alias(value: &str) -> Option<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "x64" | "amd64" => Some("x64"),
        "x86" | "win32" => Some("x86"),
        "arm64" | "aarch64" => Some("arm64"),
        "arm" => Some("arm"),
        _ => None,
    }
}

fn selected_architecture(
    environment: &WindowsProbeEnvironment,
    target_architecture: &str,
) -> Result<String> {
    let expected = architecture_alias(target_architecture)
        .context("unsupported Windows MSVC target architecture")?;
    for variable in ["VSCMD_ARG_TGT_ARCH", "Platform", "TARGET_ARCH"] {
        if let Some(value) = environment.var(variable) {
            let selected = architecture_alias(value).with_context(|| {
                format!("{variable} contains an unknown MSVC architecture `{value}`")
            })?;
            if selected != expected {
                bail!("MSVC {variable} selects {selected}, but rustc target selects {expected}");
            }
        }
    }
    if let Some(value) = environment.var("VSCMD_ARG_HOST_ARCH") {
        architecture_alias(value).with_context(|| {
            format!("VSCMD_ARG_HOST_ARCH contains an unknown architecture `{value}`")
        })?;
    }
    Ok(expected.to_string())
}

fn selected_compiler(
    environment: &WindowsProbeEnvironment,
    target_architecture: &str,
) -> Result<PathBuf> {
    if let Some(compiler) = environment.compiler.as_ref() {
        return compiler
            .is_file()
            .then(|| compiler.clone())
            .context("installed MSVC discovery returned an unreadable cl.exe");
    }

    // VCToolsInstallDir is more reliable than PATH when a developer has both
    // x86 and x64 VS prompts open. It also gives us an architecture check.
    if let Some(root) = environment.var("VCToolsInstallDir") {
        let host_arch = match environment.var("VSCMD_ARG_HOST_ARCH") {
            Some(value) => architecture_alias(value).with_context(|| {
                format!("VSCMD_ARG_HOST_ARCH contains an unknown architecture `{value}`")
            })?,
            None => target_architecture,
        };
        let root = PathBuf::from(root);
        for candidate in [
            root.join("bin").join(format!("Host{host_arch}")),
            root.join("bin"),
        ] {
            let candidate = candidate.join(target_architecture).join("cl.exe");
            if candidate.is_file() {
                return Ok(candidate);
            }
        }
        bail!(
            "VCToolsInstallDir has no cl.exe for host {host_arch} and target {target_architecture}"
        );
    }

    path_lookup(environment, "cl.exe")
        .context("selected cl.exe is not on PATH and installed MSVC discovery found none")
}

fn validate_tool_banner(tool: WindowsTool, output: &str) -> Result<String> {
    // Keep the version-bearing line. `/Bv` emits paths and other diagnostics
    // alongside the compiler banner; folding the first line would make the
    // key depend on those machine-local details.
    let version_line = output.lines().map(str::trim).find(|line| {
        let line = line.to_ascii_lowercase();
        match tool {
            WindowsTool::Link => {
                line.contains("microsoft")
                    && line.contains("incremental linker")
                    && line.contains("version")
            }
            WindowsTool::LldLink => {
                let mut words = line.split_ascii_whitespace();
                words.next() == Some("lld")
                    && words
                        .next()
                        .is_some_and(|word| word.bytes().any(|byte| byte.is_ascii_digit()))
            }
            WindowsTool::Cl => {
                line.contains("microsoft")
                    && line.contains("c/c++")
                    && line.contains("compiler")
                    && line.contains("version")
            }
        }
    });
    version_line
        .map(str::to_owned)
        .with_context(|| format!("{} returned an unrecognized version banner", tool.name()))
}

fn windows_tool_command(
    tool: WindowsTool,
    path: &Path,
    environment: &[(OsString, OsString)],
) -> Command {
    let argument = match tool {
        WindowsTool::LldLink => "--version",
        WindowsTool::Link => "/?",
        WindowsTool::Cl => "/Bv",
    };
    let mut command = Command::new(path);
    command
        .arg(argument)
        .envs(environment.iter().cloned())
        .env_remove("CL")
        .env_remove("_CL_")
        .env_remove("LINK")
        .env_remove("_LINK_")
        .env("LC_ALL", "C")
        .env("LANG", "C");
    command
}

/// Run a tool for its banner, served from the on-disk memo when the same
/// bytes were already probed under the same command environment.
///
/// The memo identifies the tool by a digest of its contents, not by path or
/// timestamps, so a binary replaced in place, even with its length and mtime
/// preserved, is a different key. The digest is taken again after the probe
/// and the banner is only published when the two agree, so a toolchain
/// swapped mid-probe can not file its banner under the other binary. Only a
/// validated banner is memoised; a tool that fails validation runs again.
///
/// The contract is "the banner is a function of the bytes and the command
/// environment". That holds for Microsoft's `link.exe` and `cl.exe`, which
/// the Visual Studio installer places as plain binaries. It does not hold
/// for a launcher whose target lives in a sidecar file: package managers
/// install `lld-link.exe` exactly that way (a scoop shim keeps its bytes
/// across `scoop update llvm`), so `lld-link` is always probed fresh.
fn run_windows_tool(
    tool: WindowsTool,
    path: &Path,
    environment: &[(OsString, OsString)],
) -> Result<String> {
    run_windows_tool_in(
        Some(&crate::config::default_cache_dir()),
        tool,
        path,
        environment,
    )
}

/// [`run_windows_tool`] with the memo directory chosen by the caller;
/// `None` never memoises.
fn run_windows_tool_in(
    memo_dir: Option<&Path>,
    tool: WindowsTool,
    path: &Path,
    environment: &[(OsString, OsString)],
) -> Result<String> {
    let before = (memo_dir.is_some() && memoises_banner(tool))
        .then(|| probe_memo::file_digest(path))
        .flatten();
    let memo = memo_dir
        .zip(before.as_deref())
        .map(|(dir, digest)| banner_memo(dir, tool, path, digest, environment));
    if let Some(banner) = memo
        .as_ref()
        .and_then(|(memo_path, key)| probe_memo::read_verified(memo_path, key))
        .and_then(|text| validate_tool_banner(tool, &text).ok())
    {
        return Ok(banner);
    }
    let output = windows_tool_command(tool, path, environment)
        .output()
        .with_context(|| format!("running {} at {}", tool.name(), path.display()))?;
    let text = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let banner = validate_tool_banner(tool, &text)?;
    if let Some((memo_path, key)) = &memo
        && probe_memo::file_digest(path) == before
    {
        probe_memo::write_verified(memo_path, key, &text);
    }
    Ok(banner)
}

/// Whether a tool's banner may be served from the memo; see
/// [`run_windows_tool`].
fn memoises_banner(tool: WindowsTool) -> bool {
    matches!(tool, WindowsTool::Link | WindowsTool::Cl)
}

/// `<memo_dir>/msvc-banner-<digest>.txt` plus the full key digest for one
/// tool's bytes under one command environment.
fn banner_memo(
    memo_dir: &Path,
    tool: WindowsTool,
    path: &Path,
    file_digest: &str,
    environment: &[(OsString, OsString)],
) -> (PathBuf, String) {
    let key = banner_memo_key(tool, path, file_digest, environment);
    (
        probe_memo::memo_path(memo_dir, "msvc-banner", "txt", &key),
        key,
    )
}

/// The banner's inputs: which tool, the exact bytes it runs, where they
/// live, the environment the tool runs under, and the two inherited
/// variables that change a banner: `PATH` (which DLLs load) and `VSLANG`
/// (which language the banner prints in).
fn banner_memo_key(
    tool: WindowsTool,
    path: &Path,
    file_digest: &str,
    environment: &[(OsString, OsString)],
) -> String {
    let mut material = Material::new("msvc-banner.v1");
    material
        .push(tool.name().as_bytes())
        .push(file_digest.as_bytes())
        .push(path.as_os_str().as_encoded_bytes());
    for (name, value) in environment {
        material
            .push(name.as_encoded_bytes())
            .push(value.as_encoded_bytes());
    }
    for inherited in ["PATH", "VSLANG"] {
        material.push(
            std::env::var_os(inherited)
                .unwrap_or_default()
                .as_encoded_bytes(),
        );
    }
    material.digest()
}

fn version_from_environment(
    environment: &WindowsProbeEnvironment,
    variables: &[&str],
    label: &str,
) -> Result<String> {
    let mut values = variables
        .iter()
        .filter_map(|name| environment.var(name).map(normalize_windows_version))
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if values.iter().any(|value| !is_windows_version(value)) {
        bail!("MSVC {label} version is malformed: {values:?}");
    }
    values.sort();
    values.dedup();
    match values.as_slice() {
        [value] => Ok(value.clone()),
        [] => bail!("MSVC {label} version is not present in the environment"),
        _ => bail!("MSVC {label} version variables disagree: {values:?}"),
    }
}

fn normalize_windows_version(value: &str) -> String {
    value
        .trim()
        .trim_matches(['\\', '/'])
        .trim_end_matches('.')
        .to_string()
}

fn is_windows_version(value: &str) -> bool {
    value.split('.').count() >= 2
        && value
            .split('.')
            .all(|component| !component.is_empty() && component.bytes().all(|b| b.is_ascii_digit()))
}

fn path_version(path: &str, marker: &str) -> Option<String> {
    let components = path.replace('\\', "/");
    let mut components = components.split('/');
    while let Some(component) = components.next() {
        if component.eq_ignore_ascii_case(marker) {
            let version = components.next()?.trim();
            if !version.is_empty() {
                return Some(version.trim_end_matches('.').to_string());
            }
        }
    }
    None
}

fn version_from_environment_or_path(
    environment: &WindowsProbeEnvironment,
    variables: &[&str],
    path_variable: &str,
    marker: &str,
    label: &str,
) -> Result<String> {
    match version_from_environment(environment, variables, label) {
        Ok(version) => Ok(version),
        Err(error) if error.to_string().contains("not present") => {
            version_from_sdk_root(environment, path_variable)
                .or_else(|| {
                    environment
                        .var(path_variable)
                        .and_then(|path| path_version(path, marker))
                })
                .map(|version| normalize_windows_version(&version))
                .filter(|version| is_windows_version(version))
                .with_context(|| format!("MSVC {label} version is not identifiable"))
        }
        Err(error) => Err(error),
    }
}

fn version_from_sdk_root(
    environment: &WindowsProbeEnvironment,
    root_variable: &str,
) -> Option<String> {
    let root = PathBuf::from(environment.var(root_variable)?);
    let include = root.join("Include");
    let mut versions = std::fs::read_dir(include)
        .ok()?
        .flatten()
        .filter(|entry| entry.path().is_dir())
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().to_string();
            let normalized = normalize_windows_version(&name);
            (!normalized.is_empty()
                && normalized.split('.').count() >= 2
                && normalized.chars().all(|c| c.is_ascii_digit() || c == '.'))
            .then_some(normalized)
        })
        .collect::<Vec<_>>();
    versions.sort();
    versions.dedup();
    (versions.len() == 1).then(|| versions.remove(0))
}

fn windows_library_dirs(
    environment: &WindowsProbeEnvironment,
    architecture: &str,
    sdk_version: &str,
    ucrt_version: &str,
    linker_dirs: &[PathBuf],
) -> Result<Vec<PathBuf>> {
    if linker_dirs.iter().any(|path| !path.is_dir()) {
        bail!("a rustc/linker library search directory is unreadable");
    }
    let explicit = environment
        .var("LIB")
        .into_iter()
        .flat_map(|lib| lib.split(';'))
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    if explicit.iter().any(|path| !path.is_dir()) {
        bail!("LIB contains an unreadable directory for MSVC {architecture}");
    }
    // An unqualified library is resolved from LINK's working directory first,
    // then its ordered /LIBPATH arguments, and finally LIB.
    let cwd = environment
        .cwd
        .clone()
        .context("current directory is unavailable for native link")?;
    if !cwd.is_dir() {
        bail!("current directory is unreadable for native link");
    }
    let mut dirs = vec![cwd];
    dirs.extend(linker_dirs.iter().cloned());
    dirs.extend(explicit);
    if let Some(root) = environment.var("VCToolsInstallDir") {
        dirs.push(PathBuf::from(root).join("lib").join(architecture));
    }
    for (root_variable, version) in [
        ("WindowsSdkDir", sdk_version),
        ("UniversalCRTSdkDir", ucrt_version),
    ] {
        if let Some(root) = environment.var(root_variable) {
            let root = PathBuf::from(root);
            dirs.push(
                root.join("Lib")
                    .join(version)
                    .join("ucrt")
                    .join(architecture),
            );
            dirs.push(root.join("Lib").join(version).join("um").join(architecture));
        }
    }
    dirs.retain(|path| path.is_dir());
    let mut ordered = Vec::with_capacity(dirs.len());
    for directory in dirs {
        if !ordered.contains(&directory) {
            ordered.push(directory);
        }
    }
    let dirs = ordered;
    if dirs.is_empty() {
        bail!("LIB contains no readable directories for MSVC {architecture}");
    }
    Ok(dirs)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WindowsLinkLibraryKind {
    Unspecified,
    Dynamic,
    Static,
    RawDylib,
}

#[derive(Debug, PartialEq, Eq)]
struct WindowsLinkLibraryFilenames {
    rustc_candidates: Vec<String>,
    ambiguous_dynamic_candidate: Option<String>,
    linker_fallback: String,
}

fn windows_link_library_filenames(
    specification: &str,
) -> Result<Option<WindowsLinkLibraryFilenames>> {
    let (kind_and_modifiers, name_and_rename) = specification
        .split_once('=')
        .unwrap_or(("unspecified", specification));
    let (kind, modifiers) = match kind_and_modifiers.split_once(':') {
        Some((_, "")) => {
            bail!("ambiguous native Windows MSVC library modifiers")
        }
        Some((kind, modifiers)) => (kind, Some(modifiers)),
        None => (kind_and_modifiers, None),
    };
    let kind = match kind {
        "unspecified" => WindowsLinkLibraryKind::Unspecified,
        "dylib" => WindowsLinkLibraryKind::Dynamic,
        "static" => WindowsLinkLibraryKind::Static,
        "raw-dylib" => WindowsLinkLibraryKind::RawDylib,
        kind => bail!("unmodeled native Windows MSVC library kind {kind:?}"),
    };

    let (name, rename) = match name_and_rename.split_once(':') {
        Some((name, rename)) if !rename.contains(':') => (name, Some(rename)),
        Some(_) => bail!("ambiguous native Windows MSVC library rename"),
        None => (name_and_rename, None),
    };
    let linked_name = rename.unwrap_or(name);
    if name.is_empty()
        || linked_name.is_empty()
        || [name, linked_name].into_iter().any(|value| {
            value
                .chars()
                .any(|character| matches!(character, '/' | '\\'))
        })
    {
        bail!("ambiguous native Windows MSVC library name {name_and_rename:?}");
    }

    let mut verbatim = None;
    let mut seen_modifiers = Vec::new();
    if let Some(modifiers) = modifiers {
        for modifier in modifiers.split(',') {
            if modifier.is_empty() || seen_modifiers.contains(&modifier) {
                bail!("ambiguous native Windows MSVC library modifiers");
            }
            seen_modifiers.push(modifier);
            match modifier {
                "+verbatim" => {
                    if verbatim.replace(true).is_some() {
                        bail!("ambiguous native Windows MSVC verbatim modifier");
                    }
                }
                "-verbatim" => {
                    if verbatim.replace(false).is_some() {
                        bail!("ambiguous native Windows MSVC verbatim modifier");
                    }
                }
                "+bundle" | "-bundle" | "+whole-archive" | "-whole-archive" | "+as-needed"
                | "-as-needed" => {}
                _ => bail!("unmodeled native Windows MSVC library modifier {modifier:?}"),
            }
        }
    }

    if kind == WindowsLinkLibraryKind::RawDylib {
        return Ok(None);
    }
    if verbatim == Some(true) {
        return Ok(Some(WindowsLinkLibraryFilenames {
            rustc_candidates: vec![linked_name.to_string()],
            ambiguous_dynamic_candidate: None,
            linker_fallback: linked_name.to_string(),
        }));
    }

    let linker_fallback = format!("{linked_name}.lib");
    let (rustc_candidates, ambiguous_dynamic_candidate) = match kind {
        WindowsLinkLibraryKind::Unspecified => (
            vec![linker_fallback.clone(), format!("lib{linked_name}.a")],
            Some(format!("lib{linked_name}.dll.a")),
        ),
        WindowsLinkLibraryKind::Dynamic => (
            vec![
                linker_fallback.clone(),
                format!("lib{linked_name}.dll.a"),
                format!("lib{linked_name}.a"),
            ],
            None,
        ),
        WindowsLinkLibraryKind::Static => (
            vec![linker_fallback.clone(), format!("lib{linked_name}.a")],
            None,
        ),
        WindowsLinkLibraryKind::RawDylib => unreachable!("handled above"),
    };
    Ok(Some(WindowsLinkLibraryFilenames {
        rustc_candidates,
        ambiguous_dynamic_candidate,
        linker_fallback,
    }))
}

fn existing_windows_library(path: &Path) -> Result<bool> {
    match std::fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(true),
        Ok(_) => bail!(
            "native Windows MSVC library {} is not a readable file",
            path.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error)
            .with_context(|| format!("inspecting native Windows library {}", path.display())),
    }
}

fn first_windows_library(directories: &[PathBuf], filenames: &[String]) -> Result<Option<PathBuf>> {
    for directory in directories {
        for filename in filenames {
            let candidate = directory.join(filename);
            if existing_windows_library(&candidate)? {
                return Ok(Some(candidate));
            }
        }
    }
    Ok(None)
}

fn first_rustc_windows_library(
    directories: &[PathBuf],
    filenames: &WindowsLinkLibraryFilenames,
) -> Result<Option<PathBuf>> {
    let Some(primary) = filenames.rustc_candidates.first() else {
        return Ok(None);
    };
    for directory in directories {
        let primary = directory.join(primary);
        if existing_windows_library(&primary)? {
            return Ok(Some(primary));
        }
        if let Some(ambiguous) = &filenames.ambiguous_dynamic_candidate {
            let ambiguous = directory.join(ambiguous);
            if existing_windows_library(&ambiguous)? {
                bail!(
                    "ambiguous unspecified native Windows MSVC library: {} may be selected \
                     only when the inherited library kind is dynamic",
                    ambiguous.display()
                );
            }
        }
        for candidate in filenames.rustc_candidates.iter().skip(1) {
            let candidate = directory.join(candidate);
            if existing_windows_library(&candidate)? {
                return Ok(Some(candidate));
            }
        }
    }
    Ok(None)
}

fn hash_windows_selected_libraries<HashLibrary>(
    rustc_directories: &[PathBuf],
    linker_directories: &[PathBuf],
    specifications: &[String],
    hash_library: HashLibrary,
) -> Result<BTreeMap<String, String>>
where
    HashLibrary: Fn(&Path) -> Result<String>,
{
    let mut selected = BTreeMap::new();
    for (index, specification) in specifications.iter().enumerate() {
        let Some(filenames) = windows_link_library_filenames(specification)? else {
            continue;
        };

        let path = match first_rustc_windows_library(rustc_directories, &filenames)? {
            Some(path) => Some(path),
            None => first_windows_library(
                linker_directories,
                std::slice::from_ref(&filenames.linker_fallback),
            )?,
        }
        .with_context(|| {
            format!(
                "native Windows MSVC library {specification:?} was not found as {}",
                filenames.linker_fallback
            )
        })?;
        let digest = hash_library(&path).with_context(|| {
            format!(
                "hashing selected native Windows MSVC library {}",
                path.display()
            )
        })?;
        selected.insert(format!("link:{index}"), digest);
    }
    Ok(selected)
}

fn hash_windows_runtime_libraries(
    environment: &WindowsProbeEnvironment,
    architecture: &str,
    sdk_version: &str,
    ucrt_version: &str,
    additional_dirs: &[PathBuf],
) -> Result<BTreeMap<String, String>> {
    let directories = windows_library_dirs(
        environment,
        architecture,
        sdk_version,
        ucrt_version,
        additional_dirs,
    )?;
    // One member from each group is selected by the CRT model (/MD vs /MT)
    // and must be present. Hash every candidate that is present so switching
    // debug/static CRT modes cannot accidentally retain one key.
    const MSVC_RUNTIME: &[&str] = &["libcmt.lib", "libcmtd.lib", "msvcrt.lib", "msvcrtd.lib"];
    const VCRUNTIME: &[&str] = &[
        "vcruntime.lib",
        "vcruntimed.lib",
        "libvcruntime.lib",
        "libvcruntimed.lib",
    ];
    const UCRT: &[&str] = &["ucrt.lib", "ucrtd.lib", "libucrt.lib", "libucrtd.lib"];
    let mut libraries = BTreeMap::new();
    for name in MSVC_RUNTIME.iter().chain(VCRUNTIME).chain(UCRT) {
        let paths = directories
            .iter()
            .map(|directory| directory.join(name))
            .filter(|path| path.is_file())
            .collect::<Vec<_>>();
        if let Some(path) = paths.first() {
            let digest = hash_placed(path)
                .with_context(|| format!("hashing MSVC runtime library {}", path.display()))?;
            for duplicate in paths.iter().skip(1) {
                let duplicate_digest = hash_placed(duplicate).with_context(|| {
                    format!(
                        "hashing duplicate MSVC runtime library {}",
                        duplicate.display()
                    )
                })?;
                if duplicate_digest != digest {
                    bail!("MSVC runtime library {name} resolves to conflicting files");
                }
            }
            libraries.insert((*name).to_string(), digest);
        }
    }
    let has_crt = ["libcmt.lib", "libcmtd.lib", "msvcrt.lib", "msvcrtd.lib"]
        .iter()
        .any(|name| libraries.contains_key(*name))
        && VCRUNTIME.iter().any(|name| libraries.contains_key(*name));
    if !has_crt {
        bail!("selected MSVC runtime is incomplete (CRT and vcruntime are required)");
    }
    if !UCRT.iter().any(|name| libraries.contains_key(*name)) {
        bail!("no selected UCRT library was found in LIB");
    }
    Ok(libraries)
}

/// Discover a Windows MSVC identity while honoring rustc/linker search paths
/// that can shadow the environment's default CRT and direct `-l` libraries.
pub(crate) fn probe_windows_msvc_identity_with_library_dirs<HashLibrary>(
    linker: Option<&Path>,
    target_architecture: &str,
    rustc_library_dirs: &[PathBuf],
    linker_library_dirs: &[PathBuf],
    link_libraries: &[String],
    hash_library: HashLibrary,
) -> Result<WindowsMsvcIdentity>
where
    HashLibrary: Fn(&Path) -> Result<String>,
{
    let mut environment =
        WindowsProbeEnvironment::current().context("capturing Windows MSVC environment")?;
    environment.augment_from_installed_msvc(target_architecture)?;
    let mut identity = probe_windows_msvc_identity_with(
        linker,
        target_architecture,
        linker_library_dirs,
        &environment,
        |tool, path| run_windows_tool(tool, path, environment.command_environment(tool, path)),
    )?;
    let directories = windows_library_dirs(
        &environment,
        &identity.architecture,
        &identity.sdk,
        &identity.ucrt,
        linker_library_dirs,
    )?;
    identity.libraries.extend(hash_windows_selected_libraries(
        rustc_library_dirs,
        &directories,
        link_libraries,
        hash_library,
    )?);
    Ok(identity)
}

/// Test seam for [`probe_windows_msvc_identity_with_library_dirs`]. The callback supplies tool
/// output, so all selection, banner validation, version and library rules can
/// run on macOS/Linux without executing Windows binaries.
pub(crate) fn probe_windows_msvc_identity_with<F>(
    linker: Option<&Path>,
    target_architecture: &str,
    additional_library_dirs: &[PathBuf],
    environment: &WindowsProbeEnvironment,
    mut tool_output: F,
) -> Result<WindowsMsvcIdentity>
where
    F: FnMut(WindowsTool, &Path) -> Result<String>,
{
    for variable in ["LINK", "_LINK_"] {
        if environment.var(variable).is_some() {
            bail!("{variable} linker options are unmodeled for native MSVC links");
        }
    }
    let architecture = selected_architecture(environment, target_architecture)?;
    let linker = match linker {
        Some(path) if is_path_like(path) => {
            let cwd = environment
                .cwd
                .as_deref()
                .context("current directory is unavailable for selected linker")?;
            let resolved = cwd.join(path);
            if !resolved.is_file() {
                bail!(
                    "selected linker {} is not readable relative to {}",
                    path.display(),
                    cwd.display()
                );
            }
            resolved
        }
        Some(name) => path_lookup(environment, &name.to_string_lossy())
            .with_context(|| format!("selected linker {} is not on PATH", name.display()))?,
        None => environment
            .default_linker
            .clone()
            .or_else(|| path_lookup(environment, "link.exe"))
            .context("link.exe is not on PATH and installed MSVC discovery found none")?,
    };
    if !linker.is_file() {
        bail!(
            "selected linker {} is not a readable file",
            linker.display()
        );
    }
    let (linker, wrapper) = match windows_tool_from_name(&linker) {
        Some(WindowsTool::Link | WindowsTool::LldLink) => (linker, None),
        Some(WindowsTool::Cl) | None => unwrap_windows_linker_wrapper(&linker, environment)?,
    };
    let linker_tool = match windows_tool_from_name(&linker) {
        Some(tool @ (WindowsTool::Link | WindowsTool::LldLink)) => tool,
        Some(WindowsTool::Cl) | None => {
            bail!("selected Windows linker is neither link.exe nor lld-link.exe")
        }
    };
    let linker_banner = validate_tool_banner(linker_tool, &tool_output(linker_tool, &linker)?)?;

    let compiler = selected_compiler(environment, &architecture)?;
    let compiler_banner =
        validate_tool_banner(WindowsTool::Cl, &tool_output(WindowsTool::Cl, &compiler)?)?;
    let compiler_architecture = compiler_banner
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter_map(architecture_alias)
        .find(|candidate| *candidate == architecture);
    if compiler_architecture.is_none() {
        bail!("cl.exe banner does not identify the selected {architecture} architecture");
    }
    let toolset = version_from_environment_or_path(
        environment,
        &["VCToolsVersion", "VCToolsInstallVersion"],
        "VCToolsInstallDir",
        "MSVC",
        "toolset",
    )?;
    let sdk = version_from_environment_or_path(
        environment,
        &["WindowsSDKVersion", "WindowsSdkVersion", "WindowsSDKVer"],
        "WindowsSdkDir",
        "Include",
        "SDK",
    )?;
    let ucrt = version_from_environment_or_path(
        environment,
        &["UCRTVersion", "UCRT_VER"],
        "UniversalCRTSdkDir",
        "Include",
        "UCRT",
    )?;
    let libraries = hash_windows_runtime_libraries(
        environment,
        &architecture,
        &sdk,
        &ucrt,
        additional_library_dirs,
    )?;
    Ok(WindowsMsvcIdentity {
        linker: linker_banner,
        compiler: compiler_banner,
        toolset,
        sdk,
        ucrt,
        architecture,
        libraries,
        wrapper,
    })
}

/// Effective linker path, plus wrapper digest and extra flags when rustc
/// invoked a script instead of `link.exe` / `lld-link.exe`.
type UnwrappedWindowsLinker = (PathBuf, Option<(String, Vec<String>)>);

/// Set when Firefox's clang is newer than rustc's LLVM. `build/cargo-linker`
/// then swaps rustc's sanitizer runtimes for clang's, which changes the link
/// line in a way the identity does not model.
const MOZ_SANITIZER_RUNTIME_SWAP: &str = "MOZ_CLANG_NEWER_THAN_RUSTC_LLVM";

/// Firefox's `build/cargo-host-linker.bat` runs
/// `%MOZ_CARGO_WRAP_HOST_LD% %* %MOZ_CARGO_WRAP_HOST_LDFLAGS%`, and
/// `build/cargo-linker.bat` runs `%PYTHON3% %~dpn0 %*`: the Python script
/// beside it, `build/cargo-linker`, links with `MOZ_CARGO_WRAP_LD` plus
/// `MOZ_CARGO_WRAP_LDFLAGS`. rustc only sees the `.bat`; the real `link.exe`
/// and extra flags live in those environment variables. Follow them so
/// identity still pins the tool. Where host and target are the same triple,
/// as on Windows, cargo links build scripts and proc-macros with the target
/// linker, so the Python route is the one Firefox's host units take.
fn unwrap_windows_linker_wrapper(
    wrapper: &Path,
    environment: &WindowsProbeEnvironment,
) -> Result<UnwrappedWindowsLinker> {
    let mut bytes = std::fs::read(wrapper).with_context(|| {
        format!(
            "selected Windows linker {} is not readable",
            wrapper.display()
        )
    })?;
    // `%~dpn0` is the script's own path without its extension.
    if String::from_utf8_lossy(&bytes)
        .to_ascii_lowercase()
        .contains("%~dpn0")
    {
        let delegate = wrapper.with_extension("");
        let script = std::fs::read(&delegate).with_context(|| {
            format!(
                "wrapper {} runs {}, which is not readable",
                wrapper.display(),
                delegate.display()
            )
        })?;
        if environment.var(MOZ_SANITIZER_RUNTIME_SWAP).is_some() {
            bail!(
                "{MOZ_SANITIZER_RUNTIME_SWAP} makes {} rewrite link arguments; unmodeled",
                delegate.display()
            );
        }
        bytes.extend_from_slice(b"\0");
        bytes.extend_from_slice(&script);
    }
    let digest = blake3::hash(&bytes).to_hex().to_string();
    let text = String::from_utf8_lossy(&bytes);
    let pairs = [
        ("MOZ_CARGO_WRAP_HOST_LD", "MOZ_CARGO_WRAP_HOST_LDFLAGS"),
        ("MOZ_CARGO_WRAP_LD", "MOZ_CARGO_WRAP_LDFLAGS"),
    ];
    for (ld_var, flags_var) in pairs {
        if !text.to_ascii_uppercase().contains(ld_var) {
            continue;
        }
        let ld = environment.var(ld_var).with_context(|| {
            format!(
                "{ld_var} is unset; wrapper {} cannot name the real linker",
                wrapper.display()
            )
        })?;
        let flags = environment.var(flags_var).unwrap_or("");
        // The scripts split on whitespace (`%VAR%`, `$VAR` unquoted) or with
        // shell quoting (`mozshellutil.split`); only unquoted values split
        // the same way in both.
        if flags.contains(['"', '\'']) {
            bail!("{flags_var} carries quotes; its split is unmodeled");
        }
        let extra = flags
            .split_whitespace()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let named = Path::new(ld);
        let effective = if is_path_like(named) {
            if named.is_absolute() {
                named.to_path_buf()
            } else if let Some(cwd) = &environment.cwd {
                cwd.join(named)
            } else {
                named.to_path_buf()
            }
        } else {
            path_lookup(environment, ld).with_context(|| {
                format!(
                    "{ld_var}={ld} is not on PATH (wrapper {})",
                    wrapper.display()
                )
            })?
        };
        if windows_tool_from_name(&effective)
            .is_none_or(|tool| !matches!(tool, WindowsTool::Link | WindowsTool::LldLink))
        {
            bail!(
                "wrapper {} resolved {ld_var} to {}, which is neither link.exe nor lld-link.exe",
                wrapper.display(),
                effective.display()
            );
        }
        return Ok((effective, Some((digest, extra))));
    }
    bail!("selected Windows linker is neither link.exe nor lld-link.exe")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::process_state_test_lock;

    const LINK_BANNER: &str = "Microsoft (R) Incremental Linker Version 14.44.35207.0";
    const LLD_LINK_BANNER: &str = "LLD 19.1.0 COFF Linker";

    fn placed(dir: &Path, name: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, name).unwrap();
        path
    }

    /// The name a tool reports in diagnostics is the name the parser
    /// recognises on disk; a drifting spelling would make an error message
    /// name a tool `windows_tool_from_name` cannot find.
    #[test]
    fn windows_tool_names_round_trip_through_the_parser() {
        for (tool, name) in [
            (WindowsTool::Link, "link.exe"),
            (WindowsTool::LldLink, "lld-link.exe"),
            (WindowsTool::Cl, "cl.exe"),
        ] {
            assert_eq!(tool.name(), name);
            assert_eq!(windows_tool_from_name(Path::new(tool.name())), Some(tool));
        }
    }

    fn compiler_banner(architecture: &str) -> String {
        format!("Microsoft (R) C/C++ Optimizing Compiler Version 19.44.35207 for {architecture}")
    }

    fn windows_environment(directory: &Path, architecture: &str) -> WindowsProbeEnvironment {
        for name in [
            "link.exe",
            "cl.exe",
            "libcmt.lib",
            "libvcruntime.lib",
            "ucrt.lib",
        ] {
            std::fs::write(directory.join(name), name).unwrap();
        }
        let variables = BTreeMap::from([
            ("Platform".into(), architecture.into()),
            ("VCToolsVersion".into(), "14.44.35207".into()),
            ("WindowsSDKVersion".into(), "10.0.26100.0".into()),
            ("UCRTVersion".into(), "10.0.26100.0".into()),
            ("LIB".into(), directory.to_string_lossy().into_owned()),
        ]);
        WindowsProbeEnvironment {
            variables,
            path: vec![directory.to_path_buf()],
            cwd: Some(directory.to_path_buf()),
            ..WindowsProbeEnvironment::default()
        }
    }

    #[test]
    fn windows_environment_names_are_case_insensitive() {
        let mut environment = WindowsProbeEnvironment {
            variables: BTreeMap::from([
                ("lIb".into(), "first".into()),
                ("vScMd_ArG_TgT_aRcH".into(), "x64".into()),
            ]),
            ..WindowsProbeEnvironment::default()
        };
        assert_eq!(environment.var("LIB"), Some("first"));
        assert_eq!(environment.var("VSCMD_ARG_TGT_ARCH"), Some("x64"));

        environment.set_var("LIB", "second");
        assert_eq!(environment.var("lib"), Some("second"));
        assert_eq!(
            environment
                .variables
                .keys()
                .filter(|name| name.eq_ignore_ascii_case("LIB"))
                .count(),
            1
        );

        environment.set_var_if_missing("lib", "third");
        assert_eq!(environment.var("LIB"), Some("second"));
        environment.set_var_if_missing("INCLUDE", "headers");
        assert_eq!(environment.var("include"), Some("headers"));

        let joined = std::env::join_paths([Path::new("one"), Path::new("two")]).unwrap();
        environment.set_var("pAtH", joined.to_string_lossy());
        environment.refresh_path();
        assert_eq!(
            environment.path,
            [PathBuf::from("one"), PathBuf::from("two")]
        );
    }

    #[test]
    fn windows_discovery_refuses_implicit_compiler_inputs_only_when_it_must_run_cl() {
        for input in ["CL", "_CL_"] {
            for marker_value in ["C:\\Visual Studio\\VC", ""] {
                let environment = WindowsProbeEnvironment {
                    variables: BTreeMap::from([
                        ("VCINSTALLDIR".into(), marker_value.into()),
                        (input.into(), "FILE1.C /O2".into()),
                    ]),
                    ..WindowsProbeEnvironment::default()
                };
                assert!(
                    environment.validate_discovery_probe().is_err(),
                    "{input} can be executed by find-msvc-tools' architecture probe"
                );
            }
        }

        let known_target = WindowsProbeEnvironment {
            variables: BTreeMap::from([
                (
                    "VSTEL_MSBuildProjectFullPath".into(),
                    "project.vcxproj".into(),
                ),
                ("VSCMD_ARG_TGT_ARCH".into(), "x64".into()),
                ("CL".into(), "FILE1.C /O2".into()),
            ]),
            ..WindowsProbeEnvironment::default()
        };
        assert!(known_target.validate_discovery_probe().is_ok());

        let registry_discovery = WindowsProbeEnvironment {
            variables: BTreeMap::from([("CL".into(), "FILE1.C /O2".into())]),
            ..WindowsProbeEnvironment::default()
        };
        assert!(registry_discovery.validate_discovery_probe().is_ok());
    }

    #[test]
    fn windows_current_environment_captures_process_state() {
        let _lock = process_state_test_lock();
        let current = WindowsProbeEnvironment::current().unwrap();
        assert_eq!(current.cwd, std::env::current_dir().ok());
        assert_eq!(
            current.path,
            std::env::var_os("PATH")
                .map(|value| std::env::split_paths(&value).collect::<Vec<_>>())
                .unwrap_or_default()
        );
    }

    #[cfg(unix)]
    #[test]
    fn windows_environment_collection_rejects_non_unicode_without_panicking() {
        use std::os::unix::ffi::OsStringExt;

        let invalid = OsString::from_vec(vec![0xff]);
        let name_error =
            collect_unicode_environment([(invalid.clone(), OsString::from("ordinary value"))])
                .unwrap_err();
        assert!(name_error.to_string().contains("variable name"));

        let value_error =
            collect_unicode_environment([(OsString::from("KACHE_TEST_RAW"), invalid)]).unwrap_err();
        assert!(value_error.to_string().contains("non-Unicode value"));
    }

    #[cfg(unix)]
    #[test]
    fn windows_production_probe_uses_current_environment() {
        let _lock = process_state_test_lock();
        const CHILD: &str = "KACHE_TEST_WINDOWS_PRODUCTION_PROBE_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let identity = probe_windows_msvc_identity_with_library_dirs(
                None,
                "x64",
                &[],
                &[],
                &[],
                hash_placed,
            )
            .expect("production Windows probe should resolve the isolated fixture");
            assert_eq!(identity.linker, LINK_BANNER);
            assert_eq!(identity.compiler, compiler_banner("x64"));
            assert_eq!(identity.toolset, "14.44.35207");
            assert_eq!(identity.sdk, "10.0.26100.0");
            assert_eq!(identity.ucrt, "10.0.26100.0");
            assert_eq!(identity.architecture, "x64");
            assert!(identity.libraries.contains_key("libcmt.lib"));
            assert!(identity.libraries.contains_key("vcruntime.lib"));
            assert!(identity.libraries.contains_key("ucrt.lib"));
            return;
        }

        let directory = tempfile::tempdir().unwrap();
        let script = format!(
            "#!/bin/sh\n\
             if [ \"${{CL+x}}\" = x ] || [ \"${{_CL_+x}}\" = x ] || \\
                [ \"${{LINK+x}}\" = x ] || [ \"${{_LINK_+x}}\" = x ]; then\n\
               echo poisoned-environment\n\
               exit 0\n\
             fi\n\
             case \"$1\" in\n\
               '/?') echo '{LINK_BANNER}' ;;\n\
               '/Bv') echo '{}' ;;\n\
               *) echo unexpected-argument ;;\n\
             esac\n",
            compiler_banner("x64")
        );
        for name in ["link.exe", "cl.exe"] {
            let path = directory.path().join(name);
            kache_fs::testutil::write_executable(&path, &script);
        }
        for (name, contents) in [
            ("libcmt.lib", b"crt".as_slice()),
            ("vcruntime.lib", b"vcruntime".as_slice()),
            ("ucrt.lib", b"ucrt".as_slice()),
        ] {
            std::fs::write(directory.path().join(name), contents).unwrap();
        }

        let output = Command::new(std::env::current_exe().unwrap())
            .arg("windows_production_probe_uses_current_environment")
            .arg("--nocapture")
            .env(CHILD, "1")
            .env("PATH", directory.path())
            .env("LIB", directory.path())
            .env("Platform", "x64")
            .env("VSCMD_ARG_TGT_ARCH", "x64")
            .env("VSCMD_ARG_HOST_ARCH", "x64")
            .env("VCToolsVersion", "14.44.35207")
            .env("WindowsSDKVersion", "10.0.26100.0")
            .env("UCRTVersion", "10.0.26100.0")
            .env("CL", "FILE1.C /O2")
            .env("_CL_", "FILE2.C")
            .env_remove("LINK")
            .env_remove("_LINK_")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child probe failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_installed_msvc_probe_resolves_without_vsdevcmd() {
        let _lock = process_state_test_lock();
        let architecture = windows_msvc_architecture(std::env::consts::ARCH)
            .expect("host architecture must be supported by the MSVC probe");
        let identity = probe_windows_msvc_identity_with_library_dirs(
            None,
            architecture,
            &[],
            &[],
            &[],
            hash_placed,
        )
        .expect("hosted Windows must expose an installed MSVC toolchain");
        assert!(!identity.linker.is_empty());
        assert!(!identity.compiler.is_empty());
        assert!(!identity.toolset.is_empty());
        assert!(!identity.sdk.is_empty());
        assert!(!identity.ucrt.is_empty());
        assert_eq!(identity.architecture, architecture);
        for family in [
            &["libcmt.lib", "libcmtd.lib", "msvcrt.lib", "msvcrtd.lib"][..],
            &[
                "vcruntime.lib",
                "vcruntimed.lib",
                "libvcruntime.lib",
                "libvcruntimed.lib",
            ][..],
            &["ucrt.lib", "ucrtd.lib", "libucrt.lib", "libucrtd.lib"][..],
        ] {
            assert!(
                family
                    .iter()
                    .any(|name| identity.libraries.contains_key(*name)),
                "missing runtime family {family:?}: {:?}",
                identity.libraries.keys().collect::<Vec<_>>()
            );
        }
    }

    /// The placement memo answers from disk while the searched directories
    /// are unchanged, and forgets itself when one of them gains or loses a
    /// file or a placed file disappears.
    #[cfg(unix)]
    #[test]
    fn crt_placement_memo_follows_the_searched_directories() {
        let _lock = process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let lib = dir.path().join("lib");
        std::fs::create_dir(&lib).unwrap();
        for (name, bytes) in [("crt1.o", "start"), ("libc.so", "libc"), ("crti.o", "i")] {
            std::fs::write(lib.join(name), bytes).unwrap();
        }
        let driver = dir.path().join("cc");
        kache_fs::testutil::write_executable(
            &driver,
            format!(
                "#!/bin/sh\ncase \"$1\" in\n  -print-search-dirs) echo \"libraries: ={lib}\" ;;\n  -print-file-name=*) n=\"${{1#-print-file-name=}}\"; if [ -f \"{lib}/$n\" ]; then echo \"{lib}/$n\"; else echo \"$n\"; fi ;;\nesac\n",
                lib = lib.display()
            ),
        );
        let memo_dir = dir.path().join("probes");
        for name in CRT_SEARCH_ENV {
            // SAFETY: the process-state lock serialises environment edits.
            unsafe { std::env::remove_var(name) };
        }

        let direct = probe_linux_crt_objects(&driver, hash_placed).unwrap();
        let memoized = probe_linux_crt_objects_memoized(&memo_dir, &driver, hash_placed).unwrap();
        assert_eq!(memoized, direct);
        assert_eq!(
            memoized.keys().collect::<Vec<_>>(),
            ["crt1.o", "crti.o", "libc.so"]
        );
        let placed = crt_placement_memo(&memo_dir, &driver).expect("memo written and valid");
        assert_eq!(placed["crt1.o"], lib.join("crt1.o"));

        // A file appearing in a searched directory changes what the driver
        // would place; the memo must step aside.
        std::fs::write(lib.join("crtn.o"), "n").unwrap();
        assert!(crt_placement_memo(&memo_dir, &driver).is_none());
        let refreshed = probe_linux_crt_objects_memoized(&memo_dir, &driver, hash_placed).unwrap();
        assert!(refreshed.contains_key("crtn.o"));
        assert!(crt_placement_memo(&memo_dir, &driver).is_some());

        // A placed file that is gone is a miss, and then a failed probe: the
        // essentials must resolve.
        std::fs::remove_file(lib.join("libc.so")).unwrap();
        assert!(crt_placement_memo(&memo_dir, &driver).is_none());
        assert!(probe_linux_crt_objects_memoized(&memo_dir, &driver, hash_placed).is_err());

        // The search variables are part of the identity.
        std::fs::write(lib.join("libc.so"), "libc").unwrap();
        let _ = probe_linux_crt_objects_memoized(&memo_dir, &driver, hash_placed).unwrap();
        assert!(crt_placement_memo(&memo_dir, &driver).is_some());
        unsafe { std::env::set_var("LIBRARY_PATH", "/opt/other") };
        assert!(crt_placement_memo(&memo_dir, &driver).is_none());
        unsafe { std::env::remove_var("LIBRARY_PATH") };
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
    fn crt_hash_cache_preserves_resolution_and_detects_changed_objects() {
        use crate::cache_key::FileHasher;

        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let probes = FileProbes {
            startup: &["startup"],
            libc: &["libc"],
            rest: &["optional"],
        };
        for name in ["startup", "libc"] {
            std::fs::write(root.join(name), vec![1u8; 131072]).unwrap();
        }
        let index = root.join("index.db");
        let run = || {
            let hasher = FileHasher::persistent(&index);
            let result = probe_files_with_hash(
                probes,
                |name| {
                    let path = root.join(name);
                    path.is_file().then_some(path)
                },
                |path| hasher.hash(path),
            );
            // The objects were written moments ago; persist their rows as a
            // later build would, once they have sat longer than the settle
            // window.
            hasher.flush_memo_as_if_settled();
            (result, hasher.stats())
        };
        let (first, cold) = run();
        let first = first.unwrap();
        assert_eq!(cold.bytes_hashed, 262144);
        let (second, warm) = run();
        assert_eq!(second.unwrap(), first);
        assert_eq!(warm.cache_hits, 2);
        assert_eq!(warm.bytes_hashed, 0);

        // A newly resolvable optional object must enter the identity even
        // though every existing object still has a cached content hash.
        std::fs::write(root.join("optional"), b"new CRT object").unwrap();
        let with_optional = run().0.unwrap();
        assert!(with_optional.contains_key("optional"));
        assert_ne!(with_optional, first);

        std::fs::write(root.join("libc"), vec![2u8; 131073]).unwrap();
        let changed = run().0.unwrap();
        assert_ne!(changed["libc"], first["libc"]);
        assert_eq!(changed["libc"], hash_placed(&root.join("libc")).unwrap());
        std::fs::remove_file(root.join("startup")).unwrap();
        assert!(
            run().0.is_err(),
            "cached hashes cannot replace missing startup objects"
        );
    }

    #[test]
    fn crt_hash_failures_are_not_omitted_from_the_identity() {
        let probes = FileProbes {
            startup: &["startup"],
            libc: &[],
            rest: &[],
        };
        let result = probe_files_with_hash(
            probes,
            |_| Some(PathBuf::from("selected-object")),
            |_| anyhow::bail!("unreadable object"),
        );
        assert!(result.is_err());
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
        let _lock = process_state_test_lock();
        let Ok(driver) = which_cc() else {
            return;
        };
        let Ok(objects) = probe_linux_crt_objects(&driver, hash_placed) else {
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
        let _lock = process_state_test_lock();
        let Ok(Some(_)) = sdk_identity_for(None) else {
            return;
        };
        let overridden = sdk_identity_for(Some("/nonexistent.sdk".into()));
        assert!(
            overridden.is_err(),
            "an unusable SDKROOT must not report the default SDK: {overridden:?}"
        );
    }

    const SYSTEM_VERSION_PLIST: &str = "\
<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \
\"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">
<plist version=\"1.0\">
<dict>
	<key>ProductBuildVersion</key>
	<string>23E208</string>
	<key>ProductVersion</key>
	<string>14.4</string>
</dict>
</plist>
";

    fn write_system_version_plist(root: &Path, body: &str) {
        let plist_dir = root.join("System/Library/CoreServices");
        std::fs::create_dir_all(&plist_dir).unwrap();
        std::fs::write(plist_dir.join("SystemVersion.plist"), body).unwrap();
    }

    #[test]
    fn plist_string_reads_xml_string_values_and_rejects_empty() {
        assert_eq!(
            plist_string(SYSTEM_VERSION_PLIST, "ProductVersion").as_deref(),
            Some("14.4")
        );
        assert_eq!(
            plist_string(SYSTEM_VERSION_PLIST, "ProductBuildVersion").as_deref(),
            Some("23E208")
        );
        assert_eq!(plist_string(SYSTEM_VERSION_PLIST, "ProductName"), None);
        assert_eq!(
            plist_string(
                "<key>ProductVersion</key>\n<string>   </string>",
                "ProductVersion"
            ),
            None
        );
    }

    #[test]
    fn identity_from_sdk_root_reads_system_version_plist() {
        let dir = tempfile::tempdir().unwrap();
        write_system_version_plist(dir.path(), SYSTEM_VERSION_PLIST);
        assert_eq!(identity_from_sdk_root(dir.path()).unwrap(), "14.4 (23E208)");

        let empty = tempfile::tempdir().unwrap();
        assert!(identity_from_sdk_root(empty.path()).is_err());

        let missing_version = tempfile::tempdir().unwrap();
        write_system_version_plist(
            missing_version.path(),
            "<key>ProductBuildVersion</key><string>23E208</string>",
        );
        assert!(identity_from_sdk_root(missing_version.path()).is_err());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn sdk_identity_reads_a_filesystem_sdkroot_when_xcrun_rejects_the_path() {
        let dir = tempfile::tempdir().unwrap();
        write_system_version_plist(dir.path(), SYSTEM_VERSION_PLIST);

        let identity = sdk_identity_for(Some(dir.path().display().to_string()))
            .unwrap()
            .expect("a real SDK directory must be identifiable without xcrun --sdk <path>");
        assert_eq!(identity, "14.4 (23E208)");

        let empty = tempfile::tempdir().unwrap();
        assert!(
            sdk_identity_for(Some(empty.path().display().to_string())).is_err(),
            "a directory that is not an SDK must not report the default SDK"
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

    #[test]
    fn windows_target_and_architecture_are_strictly_classified() {
        assert!(is_windows_msvc_target("x86_64-pc-windows-msvc"));
        assert!(is_windows_msvc_target("aarch64-pc-windows-msvc"));
        assert!(!is_windows_msvc_target("x86_64-pc-windows-gnu"));
        assert!(!is_windows_msvc_target("x86_64-unknown-linux-gnu"));
        assert_eq!(
            windows_msvc_architecture("x86_64-pc-windows-msvc"),
            Some("x64")
        );
        assert_eq!(
            windows_msvc_architecture("aarch64-pc-windows-msvc"),
            Some("arm64")
        );
        assert_eq!(
            windows_msvc_architecture("i686-pc-windows-msvc"),
            Some("x86")
        );
        assert_eq!(windows_msvc_architecture("riscv64-pc-windows-msvc"), None);
    }

    #[test]
    fn windows_target_architecture_aliases_and_components_are_exact() {
        for (target, expected) in [
            ("AMD64-pc-windows-msvc", Some("x64")),
            ("i586-pc-windows-msvc", Some("x86")),
            ("i386-pc-windows-msvc", Some("x86")),
            ("x86-pc-windows-msvc", Some("x86")),
            ("ARM64-pc-windows-msvc", Some("arm64")),
            ("thumbv7a-pc-windows-msvc", Some("arm")),
            ("arm-pc-windows-msvc", Some("arm")),
            ("", None),
        ] {
            assert_eq!(windows_msvc_architecture(target), expected, "{target}");
        }
        assert!(is_windows_msvc_target("X86_64-PC-WINDOWS-MSVC"));
        assert!(!is_windows_msvc_target("x86_64-pc-notwindows-msvc"));
        assert!(!is_windows_msvc_target("x86_64-pc-windows-notmsvc"));
        for (alias, expected) in [
            (" AMD64 ", Some("x64")),
            ("win32", Some("x86")),
            ("AARCH64", Some("arm64")),
            ("ARM", Some("arm")),
            ("thumbv7a", None),
        ] {
            assert_eq!(architecture_alias(alias), expected, "{alias}");
        }
    }

    #[test]
    fn windows_path_lookup_accepts_case_and_optional_exe_suffix() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("LLD-LINK"), b"lld").unwrap();
        std::fs::write(directory.path().join("CL"), b"cl").unwrap();
        std::fs::create_dir(directory.path().join("LINK.EXE")).unwrap();
        let environment = WindowsProbeEnvironment {
            path: vec![directory.path().to_path_buf()],
            ..WindowsProbeEnvironment::default()
        };

        assert_eq!(
            path_lookup(&environment, "lld-link.exe")
                .unwrap()
                .file_name()
                .unwrap(),
            "LLD-LINK"
        );
        assert_eq!(
            path_lookup(&environment, "cl.exe")
                .unwrap()
                .file_name()
                .unwrap(),
            "CL"
        );
        assert_eq!(path_lookup(&environment, "link.exe"), None);
        assert_eq!(
            windows_tool_from_name(Path::new("LLD-LINK")),
            Some(WindowsTool::LldLink)
        );
        assert_eq!(
            windows_tool_from_name(Path::new("Cl")),
            Some(WindowsTool::Cl)
        );
        assert_eq!(windows_tool_from_name(Path::new("lld.exe")), None);
        assert!(is_path_like(Path::new(r"tools\link.exe")));
        assert!(is_path_like(Path::new("tools/link.exe")));
        assert!(!is_path_like(Path::new("link.exe")));
    }

    #[test]
    fn windows_selected_architecture_separates_host_and_target() {
        let mut environment = WindowsProbeEnvironment::default();
        environment
            .variables
            .insert("VSCMD_ARG_TGT_ARCH".into(), "aarch64".into());
        environment
            .variables
            .insert("Platform".into(), "ARM64".into());
        environment
            .variables
            .insert("TARGET_ARCH".into(), "arm64".into());
        environment
            .variables
            .insert("VSCMD_ARG_HOST_ARCH".into(), "AMD64".into());
        assert_eq!(
            selected_architecture(&environment, "arm64").unwrap(),
            "arm64"
        );

        for variable in ["VSCMD_ARG_TGT_ARCH", "Platform", "TARGET_ARCH"] {
            let mut mismatch = environment.clone();
            mismatch.variables.insert(variable.into(), "x64".into());
            let error = selected_architecture(&mismatch, "arm64").unwrap_err();
            assert!(error.to_string().contains(variable), "{error:#}");
            assert!(error.to_string().contains("x64"), "{error:#}");
            assert!(error.to_string().contains("arm64"), "{error:#}");
        }

        environment
            .variables
            .insert("VSCMD_ARG_HOST_ARCH".into(), "mystery".into());
        assert!(
            selected_architecture(&environment, "arm64")
                .unwrap_err()
                .to_string()
                .contains("VSCMD_ARG_HOST_ARCH")
        );

        environment.variables.insert("EMPTY".into(), "  ".into());
        assert_eq!(environment.var("EMPTY"), None);
        assert_eq!(environment.var("MISSING"), None);
    }

    #[test]
    fn windows_tool_banners_require_one_complete_version_line() {
        assert_eq!(
            validate_tool_banner(
                WindowsTool::Link,
                &format!("machine-local path\n  {LINK_BANNER}  \nmore diagnostics")
            )
            .unwrap(),
            LINK_BANNER
        );
        assert_eq!(
            validate_tool_banner(WindowsTool::LldLink, LLD_LINK_BANNER).unwrap(),
            LLD_LINK_BANNER
        );
        let cl = compiler_banner("ARM64");
        assert_eq!(validate_tool_banner(WindowsTool::Cl, &cl).unwrap(), cl);

        for (tool, near_miss) in [
            (
                WindowsTool::Link,
                "Microsoft Incremental Linker\nVersion 14.44.35207",
            ),
            (WindowsTool::Link, "Incremental Linker Version 14.44.35207"),
            (WindowsTool::LldLink, "LLDB version 19.1.0"),
            (WindowsTool::LldLink, "LLD COFF Linker"),
            (
                WindowsTool::Cl,
                "Microsoft C/C++ Compiler\nVersion 19.44.35207",
            ),
            (
                WindowsTool::Cl,
                "C/C++ Compiler Version 19.44.35207 for x64",
            ),
        ] {
            let error = validate_tool_banner(tool, near_miss).unwrap_err();
            assert!(
                error.to_string().contains(tool.name()),
                "{tool:?}: {error:#}"
            );
        }
    }

    #[test]
    fn windows_compiler_selection_ignores_cl_options_and_files() {
        let directory = tempfile::tempdir().unwrap();
        let tools = directory.path().join("VC/Tools/MSVC/14.44.35207");
        let compiler = tools.join("bin/Hostx64/arm64/cl.exe");
        std::fs::create_dir_all(compiler.parent().unwrap()).unwrap();
        std::fs::write(&compiler, b"cl").unwrap();
        let mut environment = WindowsProbeEnvironment {
            variables: BTreeMap::from([
                (
                    "VCToolsInstallDir".into(),
                    tools.to_string_lossy().into_owned(),
                ),
                ("VSCMD_ARG_HOST_ARCH".into(), "amd64".into()),
                ("CL".into(), "/O2 /EHsc".into()),
            ]),
            ..WindowsProbeEnvironment::default()
        };
        assert_eq!(selected_compiler(&environment, "arm64").unwrap(), compiler);

        std::fs::write(directory.path().join("custom-cl.exe"), b"cl").unwrap();
        environment.path = vec![directory.path().to_path_buf()];
        environment
            .variables
            .insert("CL".into(), "FILE1.C custom-cl.exe /nologo".into());
        assert_eq!(
            selected_compiler(&environment, "arm64").unwrap(),
            compiler,
            "CL contains compiler options and files, never a tool override"
        );

        let fallback = directory.path().join("cl.exe");
        std::fs::write(&fallback, b"cl").unwrap();
        environment.variables.remove("VCToolsInstallDir");
        assert_eq!(selected_compiler(&environment, "arm64").unwrap(), fallback);
    }

    #[test]
    fn windows_tool_commands_clear_ambient_msvc_option_variables() {
        let command = windows_tool_command(
            WindowsTool::Cl,
            Path::new("cl.exe"),
            &[(OsString::from("LIB"), OsString::from("C:\\sdk\\lib"))],
        );
        let environment = command
            .get_envs()
            .map(|(name, value)| (name.to_string_lossy().into_owned(), value))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            environment.get("LIB").and_then(|value| *value),
            Some("C:\\sdk\\lib".as_ref())
        );
        for name in ["CL", "_CL_", "LINK", "_LINK_"] {
            assert_eq!(environment.get(name), Some(&None), "{name}");
        }
        assert_eq!(
            command.get_args().collect::<Vec<_>>(),
            vec![std::ffi::OsStr::new("/Bv")]
        );
    }

    #[test]
    fn windows_probe_applies_rustcs_link_environment_to_explicit_linkers() {
        let compiler = PathBuf::from("C:\\toolchain\\cl.exe");
        let environment = WindowsProbeEnvironment {
            compiler: Some(compiler.clone()),
            linker_command_env: vec![(OsString::from("LIB"), OsString::from("link-libs"))],
            compiler_command_env: vec![(
                OsString::from("INCLUDE"),
                OsString::from("compiler-includes"),
            )],
            ..WindowsProbeEnvironment::default()
        };
        let link_environment = environment.linker_command_env.as_slice();
        assert_eq!(
            environment.command_environment(WindowsTool::Link, Path::new("custom-link.exe")),
            link_environment
        );
        assert_eq!(
            environment.command_environment(WindowsTool::LldLink, Path::new("custom-lld-link.exe")),
            link_environment
        );
        assert_eq!(
            environment.command_environment(WindowsTool::Cl, &compiler),
            environment.compiler_command_env.as_slice()
        );
        assert!(
            environment
                .command_environment(WindowsTool::Cl, Path::new("other-cl.exe"))
                .is_empty()
        );
    }

    #[test]
    fn windows_versions_reject_malformed_conflicting_and_ambiguous_sources() {
        let mut environment = WindowsProbeEnvironment::default();
        environment
            .variables
            .insert("VERSION_A".into(), "14.44.35207\\".into());
        environment
            .variables
            .insert("VERSION_B".into(), "14.44.35207.".into());
        assert_eq!(
            version_from_environment(&environment, &["VERSION_A", "VERSION_B"], "toolset").unwrap(),
            "14.44.35207"
        );

        environment
            .variables
            .insert("VERSION_B".into(), "14.45.1".into());
        assert!(
            version_from_environment(&environment, &["VERSION_A", "VERSION_B"], "toolset")
                .unwrap_err()
                .to_string()
                .contains("disagree")
        );
        environment
            .variables
            .insert("VERSION_B".into(), "14..45".into());
        assert!(
            version_from_environment(&environment, &["VERSION_B"], "toolset")
                .unwrap_err()
                .to_string()
                .contains("malformed")
        );
        assert!(is_windows_version("10.0"));
        for malformed in ["10", "10..0", ".10", "10.x", "10.0-"] {
            assert!(!is_windows_version(malformed), "{malformed}");
        }
        assert_eq!(
            path_version(r"C:\VS\VC\Tools\MSVC\14.44.35207\", "MSVC"),
            Some("14.44.35207".into())
        );
        assert_eq!(path_version(r"C:\VS\MSVCRT\14.44", "MSVC"), None);

        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("Include/10.0.22000.0")).unwrap();
        // Rejected as versions: no digits, one component, a non-numeric one.
        for stray in ["not-a-version", "10", "1.2.beta"] {
            std::fs::create_dir_all(root.path().join("Include").join(stray)).unwrap();
        }
        let mut sdk_environment = WindowsProbeEnvironment::default();
        sdk_environment.variables.insert(
            "WindowsSdkDir".into(),
            root.path().to_string_lossy().into_owned(),
        );
        assert_eq!(
            version_from_environment_or_path(
                &sdk_environment,
                &["WindowsSDKVersion"],
                "WindowsSdkDir",
                "Include",
                "SDK",
            )
            .unwrap(),
            "10.0.22000.0"
        );
        // A malformed environment value is an error in its own right; only an
        // absent one falls back to the SDK root.
        sdk_environment
            .variables
            .insert("WindowsSDKVersion".into(), "not-a-version".into());
        let malformed = version_from_environment_or_path(
            &sdk_environment,
            &["WindowsSDKVersion"],
            "WindowsSdkDir",
            "Include",
            "SDK",
        )
        .unwrap_err();
        assert!(malformed.to_string().contains("malformed"), "{malformed:#}");
        sdk_environment.variables.remove("WindowsSDKVersion");
        std::fs::create_dir_all(root.path().join("Include/10.0.26100.0")).unwrap();
        assert!(
            version_from_environment_or_path(
                &sdk_environment,
                &["WindowsSDKVersion"],
                "WindowsSdkDir",
                "Include",
                "SDK",
            )
            .is_err(),
            "two installed versions without an environment selection are ambiguous"
        );
    }

    /// A missing library is "absent"; anything else the filesystem says is an
    /// error, never a silent absence. A path through a regular file is the
    /// portable way to get a non-NotFound error on Unix.
    #[cfg(unix)]
    #[test]
    fn existing_windows_library_reports_absence_but_not_other_errors() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("foo.lib");
        std::fs::write(&file, b"lib").unwrap();
        assert!(existing_windows_library(&file).unwrap());
        assert!(!existing_windows_library(&root.path().join("missing.lib")).unwrap());
        assert!(
            existing_windows_library(root.path()).is_err(),
            "a directory is not a library"
        );
        assert!(
            existing_windows_library(&file.join("child.lib")).is_err(),
            "a stat error other than NotFound must not read as absent"
        );
    }

    #[test]
    fn windows_runtime_libraries_require_each_family_and_consistent_duplicates() {
        let root = tempfile::tempdir().unwrap();
        let first = root.path().join("first");
        let second = root.path().join("second");
        let cwd = root.path().join("cwd");
        for directory in [&first, &second, &cwd] {
            std::fs::create_dir_all(directory).unwrap();
        }
        for directory in [&first, &second] {
            for (name, contents) in [
                ("libcmtd.lib", b"crt".as_slice()),
                ("vcruntimed.lib", b"vcrt".as_slice()),
                ("libucrtd.lib", b"ucrt".as_slice()),
            ] {
                std::fs::write(directory.join(name), contents).unwrap();
            }
        }
        let environment = WindowsProbeEnvironment {
            variables: BTreeMap::from([("LIB".into(), second.to_string_lossy().into_owned())]),
            cwd: Some(cwd),
            ..WindowsProbeEnvironment::default()
        };
        let libraries = hash_windows_runtime_libraries(
            &environment,
            "x64",
            "10.0.26100.0",
            "10.0.26100.0",
            std::slice::from_ref(&first),
        )
        .unwrap();
        assert_eq!(
            libraries.keys().map(String::as_str).collect::<Vec<_>>(),
            ["libcmtd.lib", "libucrtd.lib", "vcruntimed.lib"]
        );

        std::fs::write(second.join("vcruntimed.lib"), b"different").unwrap();
        assert!(
            hash_windows_runtime_libraries(
                &environment,
                "x64",
                "10.0.26100.0",
                "10.0.26100.0",
                std::slice::from_ref(&first),
            )
            .unwrap_err()
            .to_string()
            .contains("conflicting files")
        );

        for (names, expected) in [
            (
                &["libcmt.lib", "ucrt.lib"][..],
                "CRT and vcruntime are required",
            ),
            (
                &["vcruntime.lib", "ucrt.lib"][..],
                "CRT and vcruntime are required",
            ),
            (
                &["libcmt.lib", "vcruntime.lib"][..],
                "no selected UCRT library",
            ),
        ] {
            let directory = tempfile::tempdir().unwrap();
            for name in names {
                std::fs::write(directory.path().join(name), name).unwrap();
            }
            let incomplete = WindowsProbeEnvironment {
                variables: BTreeMap::from([(
                    "LIB".into(),
                    directory.path().to_string_lossy().into_owned(),
                )]),
                cwd: Some(directory.path().to_path_buf()),
                ..WindowsProbeEnvironment::default()
            };
            let error = hash_windows_runtime_libraries(
                &incomplete,
                "x64",
                "10.0.26100.0",
                "10.0.26100.0",
                &[],
            )
            .unwrap_err();
            assert!(error.to_string().contains(expected), "{error:#}");
        }
    }

    #[test]
    fn windows_library_directories_validate_each_search_source() {
        let root = tempfile::tempdir().unwrap();
        let additional = root.path().join("z-additional");
        let cwd = root.path().join("m-cwd");
        let lib = root.path().join("a-lib");
        for directory in [&additional, &cwd, &lib] {
            std::fs::create_dir_all(directory).unwrap();
        }
        let environment = WindowsProbeEnvironment {
            variables: BTreeMap::from([("LIB".into(), lib.to_string_lossy().into_owned())]),
            cwd: Some(cwd.clone()),
            ..WindowsProbeEnvironment::default()
        };
        let directories = windows_library_dirs(
            &environment,
            "x64",
            "10.0.26100.0",
            "10.0.26100.0",
            std::slice::from_ref(&additional),
        )
        .unwrap();
        assert_eq!(
            directories,
            vec![cwd.clone(), additional.clone(), lib.clone()]
        );

        let missing = root.path().join("missing");
        assert!(
            windows_library_dirs(
                &environment,
                "x64",
                "10.0.26100.0",
                "10.0.26100.0",
                &[missing],
            )
            .is_err()
        );
        let mut invalid_lib = environment.clone();
        invalid_lib.variables.insert(
            "LIB".into(),
            root.path().join("absent").display().to_string(),
        );
        assert!(
            windows_library_dirs(&invalid_lib, "x64", "10.0.26100.0", "10.0.26100.0", &[],)
                .is_err()
        );
        let mut no_cwd = environment;
        no_cwd.cwd = None;
        assert!(
            windows_library_dirs(&no_cwd, "x64", "10.0.26100.0", "10.0.26100.0", &[],).is_err()
        );
    }

    #[test]
    fn windows_selected_libraries_hash_bytes_in_effective_search_order() {
        let root = tempfile::tempdir().unwrap();
        let first = root.path().join("first");
        let second = root.path().join("second");
        std::fs::create_dir_all(&first).unwrap();
        std::fs::create_dir_all(&second).unwrap();
        let first_library = first.join("foo.lib");
        let second_library = second.join("foo.lib");
        std::fs::write(&first_library, b"first-v1").unwrap();
        std::fs::write(&second_library, b"second-v1").unwrap();
        let specifications = vec!["foo".to_string()];
        let file_hasher = crate::cache_key::FileHasher::new();

        let first_hash = hash_windows_selected_libraries(
            &[first.clone(), second.clone()],
            &[],
            &specifications,
            |path| file_hasher.hash_static_lib(path),
        )
        .unwrap();
        assert_eq!(
            first_hash["link:0"],
            file_hasher.hash_static_lib(&first_library).unwrap()
        );

        std::fs::write(&second_library, b"second-v2").unwrap();
        assert_eq!(
            hash_windows_selected_libraries(
                &[first.clone(), second.clone()],
                &[],
                &specifications,
                |path| file_hasher.hash_static_lib(path),
            )
            .unwrap(),
            first_hash,
            "a shadowed library must not affect the selected identity"
        );
        let reversed = hash_windows_selected_libraries(
            &[second.clone(), first.clone()],
            &[],
            &specifications,
            |path| file_hasher.hash_static_lib(path),
        )
        .unwrap();
        assert_eq!(
            reversed["link:0"],
            file_hasher.hash_static_lib(&second_library).unwrap()
        );
        assert_ne!(
            reversed, first_hash,
            "search order selects a different file"
        );

        std::fs::write(&first_library, b"first-v2").unwrap();
        let changed =
            hash_windows_selected_libraries(&[first, second], &[], &specifications, |path| {
                file_hasher.hash_static_lib(path)
            })
            .unwrap();
        assert_ne!(
            changed, first_hash,
            "changed bytes at the selected path must change the identity"
        );
    }

    #[test]
    fn windows_selected_libraries_follow_rustc_then_link_search_stages() {
        assert_eq!(
            windows_link_library_filenames("foo").unwrap(),
            Some(WindowsLinkLibraryFilenames {
                rustc_candidates: vec!["foo.lib".to_string(), "libfoo.a".to_string(),],
                ambiguous_dynamic_candidate: Some("libfoo.dll.a".to_string()),
                linker_fallback: "foo.lib".to_string(),
            })
        );
        assert_eq!(
            windows_link_library_filenames("static:+verbatim,+whole-archive=source:renamed.lib")
                .unwrap(),
            Some(WindowsLinkLibraryFilenames {
                rustc_candidates: vec!["renamed.lib".to_string()],
                ambiguous_dynamic_candidate: None,
                linker_fallback: "renamed.lib".to_string(),
            })
        );
        assert_eq!(
            windows_link_library_filenames("static=foo").unwrap(),
            Some(WindowsLinkLibraryFilenames {
                rustc_candidates: vec!["foo.lib".to_string(), "libfoo.a".to_string()],
                ambiguous_dynamic_candidate: None,
                linker_fallback: "foo.lib".to_string(),
            })
        );
        assert_eq!(
            windows_link_library_filenames("raw-dylib=foo").unwrap(),
            None
        );
        // `-verbatim` is a real modifier that restores the default lookup.
        assert_eq!(
            windows_link_library_filenames("static:-verbatim=foo").unwrap(),
            windows_link_library_filenames("static=foo").unwrap()
        );

        let root = tempfile::tempdir().unwrap();
        let rustc_first = root.path().join("rustc-first");
        let rustc_second = root.path().join("rustc-second");
        let cwd = root.path().join("cwd");
        let libpath = root.path().join("libpath");
        for directory in [&rustc_first, &rustc_second, &cwd, &libpath] {
            std::fs::create_dir_all(directory).unwrap();
        }
        std::fs::write(rustc_first.join("libfoo.a"), b"first alternate").unwrap();
        std::fs::write(rustc_second.join("foo.lib"), b"later primary").unwrap();
        std::fs::write(cwd.join("foo.lib"), b"cwd fallback").unwrap();
        std::fs::write(libpath.join("foo.lib"), b"libpath fallback").unwrap();
        let file_hasher = crate::cache_key::FileHasher::new();
        let specifications = vec!["static=foo".to_string()];

        let rustc_selected = hash_windows_selected_libraries(
            &[rustc_first, rustc_second],
            &[cwd.clone(), libpath.clone()],
            &specifications,
            |path| file_hasher.hash_static_lib(path),
        )
        .unwrap();
        assert_eq!(
            rustc_selected["link:0"],
            file_hasher
                .hash_static_lib(&root.path().join("rustc-first/libfoo.a"))
                .unwrap()
        );

        let linker_selected = hash_windows_selected_libraries(
            &[],
            &[cwd.clone(), libpath],
            &specifications,
            |path| file_hasher.hash_static_lib(path),
        )
        .unwrap();
        assert_eq!(
            linker_selected["link:0"],
            file_hasher.hash_static_lib(&cwd.join("foo.lib")).unwrap()
        );
    }

    #[test]
    fn windows_selected_libraries_fail_closed_on_ambiguous_or_missing_inputs() {
        for specification in [
            "framework=foo",
            "static:=foo",
            "static=foo:renamed:again",
            "dylib:+unknown=foo",
            "dylib:+verbatim,-verbatim=foo",
            // Each of these fails exactly one of the name checks.
            "static=:renamed",
            "static=foo:",
            "static=dir/foo",
            "static=dir\\foo",
            "static=foo:sub/renamed",
            "static=foo:sub\\renamed",
            // An empty modifier and a repeated one are each rejected on their own.
            "static:+verbatim,=foo",
            "static:+whole-archive,+whole-archive=foo",
        ] {
            assert!(
                windows_link_library_filenames(specification).is_err(),
                "{specification}"
            );
        }

        let root = tempfile::tempdir().unwrap();
        let file_hasher = crate::cache_key::FileHasher::new();

        std::fs::write(root.path().join("libunknown.dll.a"), b"import").unwrap();
        let unspecified = vec!["unknown".to_string()];
        let error = hash_windows_selected_libraries(
            &[root.path().to_path_buf()],
            &[root.path().to_path_buf()],
            &unspecified,
            |path| file_hasher.hash_static_lib(path),
        )
        .unwrap_err();
        assert!(error.to_string().contains("ambiguous"), "{error:#}");

        let missing = vec!["static=missing".to_string()];
        let error =
            hash_windows_selected_libraries(&[], &[root.path().to_path_buf()], &missing, |path| {
                file_hasher.hash_static_lib(path)
            })
            .unwrap_err();
        assert!(error.to_string().contains("was not found"), "{error:#}");

        std::fs::create_dir(root.path().join("directory.lib")).unwrap();
        let unreadable = vec!["static=directory".to_string()];
        let error = hash_windows_selected_libraries(
            &[],
            &[root.path().to_path_buf()],
            &unreadable,
            |path| file_hasher.hash_static_lib(path),
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("not a readable file"),
            "{error:#}"
        );

        std::fs::write(root.path().join("thin.lib"), b"!<thin>\n").unwrap();
        let thin = vec!["static=thin".to_string()];
        let error =
            hash_windows_selected_libraries(&[], &[root.path().to_path_buf()], &thin, |path| {
                file_hasher.hash_static_lib(path)
            })
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("thin static archive"),
            "{error:#}"
        );
    }

    /// Firefox 151's `build/cargo-linker.bat` names no variable itself: it
    /// runs the Python script beside it, which links with
    /// `MOZ_CARGO_WRAP_LD`. On Windows cargo links build scripts and
    /// proc-macros through it too, and every one of them used to be refused.
    #[test]
    fn windows_identity_follows_firefox_python_cargo_linker() {
        let directory = tempfile::tempdir().unwrap();
        let mut environment = windows_environment(directory.path(), "x64");
        let lld = directory.path().join("lld-link.exe");
        std::fs::write(&lld, b"").unwrap();
        let wrapper = directory.path().join("cargo-linker.bat");
        std::fs::write(&wrapper, "@echo off\r\n%PYTHON3% %~dpn0 %*\r\n").unwrap();
        let script = directory.path().join("cargo-linker");
        std::fs::write(
            &script,
            "wrap_ld = os.environ[\"MOZ_CARGO_WRAP_LD\"]\n\
             args.extend(split(os.environ[\"MOZ_CARGO_WRAP_LDFLAGS\"]))\n",
        )
        .unwrap();
        environment.variables.insert(
            "MOZ_CARGO_WRAP_LD".into(),
            lld.to_string_lossy().into_owned(),
        );
        environment
            .variables
            .insert("MOZ_CARGO_WRAP_LDFLAGS".into(), "/DEBUG /OPT:REF".into());
        let probe = |environment: &WindowsProbeEnvironment| {
            probe_windows_msvc_identity_with(
                Some(&wrapper),
                "x64",
                &[],
                environment,
                |tool, path| {
                    if tool == WindowsTool::LldLink {
                        assert_eq!(path, lld);
                    }
                    Ok(match tool {
                        WindowsTool::LldLink => LLD_LINK_BANNER.into(),
                        WindowsTool::Cl => compiler_banner("x64"),
                        WindowsTool::Link => unreachable!(),
                    })
                },
            )
        };

        let identity = probe(&environment).unwrap();
        assert_eq!(identity.linker, LLD_LINK_BANNER);
        let (digest, extra) = identity.wrapper.clone().expect("wrapper identity");
        assert_eq!(extra, ["/DEBUG", "/OPT:REF"]);

        // The script is part of the wrapper's identity.
        std::fs::write(
            &script,
            "wrap_ld = os.environ[\"MOZ_CARGO_WRAP_LD\"] # v2\n",
        )
        .unwrap();
        let (changed, _) = probe(&environment).unwrap().wrapper.unwrap();
        assert_ne!(digest, changed);

        let mut quoted = environment.clone();
        quoted.variables.insert(
            "MOZ_CARGO_WRAP_LDFLAGS".into(),
            "\"/LIBPATH:C:/a b\"".into(),
        );
        assert!(format!("{:#}", probe(&quoted).unwrap_err()).contains("quotes"));

        let mut sanitizer = environment.clone();
        sanitizer
            .variables
            .insert(MOZ_SANITIZER_RUNTIME_SWAP.into(), "1".into());
        assert!(format!("{:#}", probe(&sanitizer).unwrap_err()).contains("unmodeled"));

        std::fs::remove_file(&script).unwrap();
        assert!(format!("{:#}", probe(&environment).unwrap_err()).contains("not readable"));
    }

    #[test]
    fn windows_identity_accepts_absolute_linker_and_rejects_unmodeled_selection() {
        let directory = tempfile::tempdir().unwrap();
        let environment = windows_environment(directory.path(), "x64");
        let link = directory.path().join("link.exe");
        let identity = probe_windows_msvc_identity_with(
            Some(&link),
            "x64",
            &[],
            &environment,
            |tool, path| {
                if tool == WindowsTool::Link {
                    assert_eq!(path, link);
                }
                Ok(match tool {
                    WindowsTool::Link => LINK_BANNER.into(),
                    WindowsTool::Cl => compiler_banner("x64"),
                    WindowsTool::LldLink => unreachable!(),
                })
            },
        )
        .unwrap();
        assert_eq!(identity.linker, LINK_BANNER);

        let relative_cwd = directory.path().join("relative-cwd");
        let relative_link = relative_cwd.join("tools/link.exe");
        std::fs::create_dir_all(relative_link.parent().unwrap()).unwrap();
        std::fs::write(&relative_link, b"link").unwrap();
        let mut relative_environment = environment.clone();
        relative_environment.cwd = Some(relative_cwd);
        let identity = probe_windows_msvc_identity_with(
            Some(Path::new("tools/link.exe")),
            "x64",
            &[],
            &relative_environment,
            |tool, path| {
                if tool == WindowsTool::Link {
                    assert_eq!(path, relative_link);
                }
                Ok(match tool {
                    WindowsTool::Link => LINK_BANNER.into(),
                    WindowsTool::Cl => compiler_banner("x64"),
                    WindowsTool::LldLink => unreachable!(),
                })
            },
        )
        .unwrap();
        assert_eq!(identity.linker, LINK_BANNER);

        assert!(
            probe_windows_msvc_identity_with(
                Some(&directory.path().join("cl.exe")),
                "x64",
                &[],
                &environment,
                |_, _| unreachable!(),
            )
            .unwrap_err()
            .to_string()
            .contains("neither link.exe nor lld-link.exe")
        );

        let wrapper = directory.path().join("cargo-host-linker.bat");
        std::fs::write(
            &wrapper,
            "%MOZ_CARGO_WRAP_HOST_LD% %* %MOZ_CARGO_WRAP_HOST_LDFLAGS%\r\n",
        )
        .unwrap();
        let mut wrapped = environment.clone();
        wrapped
            .variables
            .insert("MOZ_CARGO_WRAP_HOST_LD".into(), "link.exe".into());
        wrapped
            .variables
            .insert("MOZ_CARGO_WRAP_HOST_LDFLAGS".into(), "/DEBUG".into());
        let wrapped_identity =
            probe_windows_msvc_identity_with(Some(&wrapper), "x64", &[], &wrapped, |tool, path| {
                if tool == WindowsTool::Link {
                    assert_eq!(path.file_name().unwrap(), "link.exe");
                }
                Ok(match tool {
                    WindowsTool::Link => LINK_BANNER.into(),
                    WindowsTool::Cl => compiler_banner("x64"),
                    WindowsTool::LldLink => unreachable!(),
                })
            })
            .unwrap();
        assert_eq!(wrapped_identity.linker, LINK_BANNER);
        let (digest, extra) = wrapped_identity.wrapper.as_ref().expect("wrapper identity");
        assert!(!digest.is_empty());
        assert_eq!(extra, &["/DEBUG".to_string()]);
        assert!(wrapped_identity.encode().contains("wrapper="));
        assert!(wrapped_identity.encode().contains("wrapper_flag=/DEBUG"));

        wrapped.variables.insert(
            "MOZ_CARGO_WRAP_HOST_LDFLAGS".into(),
            "/INCREMENTAL:NO".into(),
        );
        let changed_flags =
            probe_windows_msvc_identity_with(Some(&wrapper), "x64", &[], &wrapped, |tool, _| {
                Ok(match tool {
                    WindowsTool::Link => LINK_BANNER.into(),
                    WindowsTool::Cl => compiler_banner("x64"),
                    WindowsTool::LldLink => unreachable!(),
                })
            })
            .unwrap();
        assert_ne!(
            wrapped_identity.encode(),
            changed_flags.encode(),
            "wrapper extra flags must change the identity"
        );
        for variable in ["LINK", "_LINK_"] {
            let mut with_options = environment.clone();
            with_options
                .variables
                .insert(variable.into(), "/DEBUG".into());
            let error = probe_windows_msvc_identity_with(
                Some(&link),
                "x64",
                &[],
                &with_options,
                |_, _| unreachable!(),
            )
            .unwrap_err();
            assert!(error.to_string().contains(variable), "{error:#}");
        }
    }

    #[test]
    fn windows_identity_uses_lld_link_and_matches_arm_exactly() {
        let directory = tempfile::tempdir().unwrap();
        let mut environment = windows_environment(directory.path(), "arm");
        std::fs::write(directory.path().join("LLD-LINK.EXE"), b"lld").unwrap();
        let cwd = directory.path().join("cwd-without-tools");
        std::fs::create_dir(&cwd).unwrap();
        environment.cwd = Some(cwd);
        environment
            .variables
            .insert("VSCMD_ARG_HOST_ARCH".into(), "x64".into());
        let identity = probe_windows_msvc_identity_with(
            Some(Path::new("lld-link.exe")),
            "arm",
            &[],
            &environment,
            |tool, _| {
                Ok(match tool {
                    WindowsTool::LldLink => LLD_LINK_BANNER.into(),
                    WindowsTool::Cl => compiler_banner("ARM"),
                    WindowsTool::Link => unreachable!(),
                })
            },
        )
        .unwrap();
        assert_eq!(identity.architecture, "arm");
        assert_eq!(identity.linker, LLD_LINK_BANNER);

        let error = probe_windows_msvc_identity_with(
            Some(Path::new("lld-link.exe")),
            "arm",
            &[],
            &environment,
            |tool, _| {
                Ok(match tool {
                    WindowsTool::LldLink => LLD_LINK_BANNER.into(),
                    WindowsTool::Cl => compiler_banner("ARM64"),
                    WindowsTool::Link => unreachable!(),
                })
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("selected arm architecture"));

        environment
            .variables
            .insert("Platform".into(), "arm64".into());
        let error = probe_windows_msvc_identity_with(
            Some(Path::new("lld-link.exe")),
            "arm64",
            &[],
            &environment,
            |tool, _| {
                Ok(match tool {
                    WindowsTool::LldLink => LLD_LINK_BANNER.into(),
                    WindowsTool::Cl => compiler_banner("ARM"),
                    WindowsTool::Link => unreachable!(),
                })
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("selected arm64 architecture"));
    }

    #[test]
    fn windows_identity_discovers_tools_versions_and_runtime_hashes() {
        let directory = tempfile::tempdir().unwrap();
        for name in ["link.exe", "cl.exe"] {
            std::fs::write(directory.path().join(name), name).unwrap();
        }
        for name in ["libcmt.lib", "libvcruntime.lib", "ucrt.lib"] {
            std::fs::write(directory.path().join(name), name).unwrap();
        }
        let mut variables = BTreeMap::new();
        variables.insert("Platform".into(), "x64".into());
        variables.insert("VCToolsVersion".into(), "14.44.35207".into());
        variables.insert("WindowsSDKVersion".into(), "10.0.26100.0".into());
        variables.insert("UCRTVersion".into(), "10.0.26100.0".into());
        variables.insert("LIB".into(), directory.path().to_string_lossy().into());
        let environment = WindowsProbeEnvironment {
            variables,
            path: vec![directory.path().to_path_buf()],
            cwd: Some(directory.path().to_path_buf()),
            ..WindowsProbeEnvironment::default()
        };
        let identity =
            probe_windows_msvc_identity_with(None, "x64", &[], &environment, |tool, _| {
                Ok(match tool {
                    WindowsTool::Link => {
                        "Microsoft (R) Incremental Linker Version 14.44.35207.0".into()
                    }
                    WindowsTool::Cl => {
                        "Microsoft (R) C/C++ Optimizing Compiler Version 19.44.35207 for x64".into()
                    }
                    WindowsTool::LldLink => "LLD 19.1.0 COFF Linker".into(),
                })
            })
            .unwrap();
        assert_eq!(identity.architecture, "x64");
        assert_eq!(identity.toolset, "14.44.35207");
        assert_eq!(identity.sdk, "10.0.26100.0");
        assert_eq!(identity.libraries.len(), 3);
        assert!(identity.encode().contains("lib:libvcruntime.lib="));
    }

    #[test]
    fn windows_identity_rejects_wrong_architecture_or_banner() {
        let directory = tempfile::tempdir().unwrap();
        for name in [
            "link.exe",
            "cl.exe",
            "libcmt.lib",
            "vcruntime.lib",
            "ucrt.lib",
        ] {
            std::fs::write(directory.path().join(name), name).unwrap();
        }
        let mut variables = BTreeMap::new();
        variables.insert("Platform".into(), "x86".into());
        variables.insert("VCToolsVersion".into(), "14.44.35207".into());
        variables.insert("WindowsSDKVersion".into(), "10.0.26100.0".into());
        variables.insert("UCRTVersion".into(), "10.0.26100.0".into());
        variables.insert("LIB".into(), directory.path().to_string_lossy().into());
        let environment = WindowsProbeEnvironment {
            variables,
            path: vec![directory.path().to_path_buf()],
            cwd: Some(directory.path().to_path_buf()),
            ..WindowsProbeEnvironment::default()
        };
        let mut mixed_case_link = environment.clone();
        mixed_case_link
            .variables
            .insert("lInK".into(), "/DEBUG".into());
        let error = probe_windows_msvc_identity_with(None, "x64", &[], &mixed_case_link, |_, _| {
            unreachable!("LINK must be rejected before a tool runs")
        })
        .unwrap_err();
        assert!(error.to_string().contains("LINK"));

        let output = |_: WindowsTool, _: &Path| Ok("untrusted tool".to_string());
        assert!(probe_windows_msvc_identity_with(None, "x64", &[], &environment, output).is_err());

        let mut malformed_host = environment.clone();
        malformed_host
            .variables
            .insert("Platform".into(), "x64".into());
        malformed_host
            .variables
            .insert("VSCMD_ARG_HOST_ARCH".into(), "mystery".into());
        let error =
            probe_windows_msvc_identity_with(None, "x64", &[], &malformed_host, |tool, _| {
                Ok(match tool {
                    WindowsTool::Link => {
                        "Microsoft (R) Incremental Linker Version 14.44.35207.0".into()
                    }
                    WindowsTool::Cl => {
                        "Microsoft (R) C/C++ Optimizing Compiler Version 19.44.35207 for x64".into()
                    }
                    WindowsTool::LldLink => unreachable!(),
                })
            })
            .unwrap_err();
        assert!(error.to_string().contains("VSCMD_ARG_HOST_ARCH"));
    }

    #[test]
    fn windows_identity_discovers_sdk_and_ucrt_roots_without_version_environment() {
        let directory = tempfile::tempdir().unwrap();
        let vc = directory.path().join("MSVC").join("14.44.35207");
        let sdk = directory.path().join("Windows Kits").join("10");
        let sdk_version = "10.0.26100.0";
        std::fs::create_dir_all(directory.path().join("tools")).unwrap();
        std::fs::create_dir_all(vc.join("bin/Hostx64/x64")).unwrap();
        std::fs::create_dir_all(vc.join("lib/x64")).unwrap();
        std::fs::create_dir_all(sdk.join("Include").join(sdk_version)).unwrap();
        std::fs::create_dir_all(sdk.join("Lib").join(sdk_version).join("ucrt/x64")).unwrap();
        std::fs::write(directory.path().join("tools/link.exe"), b"link").unwrap();
        std::fs::write(vc.join("bin/Hostx64/x64/cl.exe"), b"cl").unwrap();
        for (path, contents) in [
            (vc.join("lib/x64/libcmt.lib"), b"crt".as_slice()),
            (vc.join("lib/x64/libvcruntime.lib"), b"vcrt".as_slice()),
            (
                sdk.join("Lib").join(sdk_version).join("ucrt/x64/ucrt.lib"),
                b"ucrt".as_slice(),
            ),
        ] {
            std::fs::write(path, contents).unwrap();
        }
        let mut variables = BTreeMap::new();
        variables.insert("Platform".into(), "x64".into());
        variables.insert(
            "VCToolsInstallDir".into(),
            vc.to_string_lossy().into_owned(),
        );
        variables.insert("WindowsSdkDir".into(), sdk.to_string_lossy().into_owned());
        variables.insert(
            "UniversalCRTSdkDir".into(),
            sdk.to_string_lossy().into_owned(),
        );
        let environment = WindowsProbeEnvironment {
            variables,
            path: vec![directory.path().to_path_buf()],
            cwd: Some(directory.path().to_path_buf()),
            ..WindowsProbeEnvironment::default()
        };
        let identity = probe_windows_msvc_identity_with(
            Some(Path::new("tools/link.exe")),
            "x64",
            &[],
            &environment,
            |tool, path| {
                if tool == WindowsTool::Link {
                    assert_eq!(path, directory.path().join("tools/link.exe"));
                }
                Ok(match tool {
                    WindowsTool::Link => {
                        "Microsoft (R) Incremental Linker Version 14.44.35207.0".into()
                    }
                    WindowsTool::Cl => {
                        "Microsoft (R) C/C++ Optimizing Compiler Version 19.44.35207 for x64".into()
                    }
                    WindowsTool::LldLink => unreachable!(),
                })
            },
        )
        .unwrap();
        assert_eq!(identity.toolset, "14.44.35207");
        assert_eq!(identity.sdk, sdk_version);
        assert_eq!(identity.ucrt, sdk_version);
        assert_eq!(identity.libraries.len(), 3);
        assert!(identity.libraries.contains_key("libvcruntime.lib"));
    }

    #[test]
    fn windows_link_arguments_naming_input_files_fail_closed() {
        // One value per extension arm, spelt so that only that arm fires, in
        // both cases LINK accepts. Quoted, comma-joined (`-Wl,`) and
        // whitespace-joined (`link-args`) carriers are split first.
        for argument in [
            "foo.lib",
            "FOO.LIB",
            "libfoo.a",
            "LIBFOO.A",
            "extra.obj",
            "EXTRA.Obj",
            "extra.o",
            "EXTRA.O",
            "app.res",
            "APP.RES",
            "exports.def",
            "EXPORTS.DEF",
            "exports.exp",
            "EXPORTS.EXP",
            "app.manifest",
            "APP.Manifest",
            r#""C:\out dir\app.res""#,
            "-Wl,app.res",
            "/DEBUG app.res",
            "/OPT:REF\tapp.res",
            "/NODEFAULTLIB:libcmt.lib",
        ] {
            assert!(
                windows_link_argument_has_unmodeled_input(argument),
                "{argument}"
            );
        }
    }

    #[test]
    fn windows_link_file_options_fail_closed_under_either_prefix_and_case() {
        // Values avoid every modeled extension so only the option arm fires.
        for argument in [
            "/DEF:exports",
            "-def:exports",
            "/DeF=exports",
            "-Wl,/DEF:exports",
            "/DEFAULTLIB:foo",
            "-defaultlib:foo",
            "/WHOLEARCHIVE:foo",
            "-wholearchive=foo",
            "/STUB:stub.bin",
            "-stub:stub.bin",
            "/KEYFILE:key.snk",
            "-KeyFile:key.snk",
            "/PGD:app.pgd",
            "-pgd:app.pgd",
            "/NATVIS:types.natvis",
            "-natvis:types.natvis",
            "/SOURCELINK:sl.json",
            "-sourcelink:sl.json",
            "/MANIFESTINPUT:extra.xml",
            "-manifestinput:extra.xml",
            "/ASSEMBLYMODULE:m.netmodule",
            "-assemblymodule:m.netmodule",
            "/ASSEMBLYRESOURCE:r.resources",
            "-assemblyresource:r.resources",
            "/ASSEMBLYLINKRESOURCE:r.bin",
            "-assemblylinkresource:r.bin",
            "/MANIFESTFILE:app.xml",
            "-manifestfile:app.xml",
            "/PDBSTRIPPED:public.pdb",
            "-PdbStripped:public.pdb",
            "/PDB:app.pdb",
            "-pdb:app.pdb",
            "/IMPLIB:app.imp",
            "-implib:app.imp",
            "/ILK:app.ilk",
            "-ilk:app.ilk",
            "/DEBUG /DEF:exports",
            r#"/DEF:"C:\out dir\exports""#,
        ] {
            assert!(
                windows_link_argument_has_unmodeled_input(argument),
                "{argument}"
            );
        }
    }

    #[test]
    fn windows_link_arguments_without_unhashed_files_stay_modeled() {
        // Plain options, the modeled `/LIBPATH` directory (its `.lib` name is a
        // directory, not an input), names that are not files, and a bare
        // `/WHOLEARCHIVE` that only re-scopes rustc's own inputs.
        for argument in [
            "",
            "/DEBUG",
            "/DEBUG:FULL",
            "/OPT:REF,ICF",
            "/SUBSYSTEM:WINDOWS,5.02",
            "/STACK:0x800000",
            "/INCLUDE:__foo",
            "/MANIFEST:NO",
            "/MANIFESTUAC:level='asInvoker'",
            "/NODEFAULTLIB",
            "/NODEFAULTLIB:libcmt",
            "/WHOLEARCHIVE",
            "-wholearchive",
            "/DELAYLOAD:foo.dll",
            "/PDBALTPATH:%_PDB%",
            r"/LIBPATH:C:\sdk\lib",
            r"-libpath:C:\vendor.lib\x64",
            "-Wl,/OPT:REF",
            "-fuse-ld=lld",
            "rlib",
        ] {
            assert!(
                !windows_link_argument_has_unmodeled_input(argument),
                "{argument}"
            );
        }

        // A `.lib` requested through `-l` is not a link argument at all: it is
        // resolved against the search directories and its bytes are hashed.
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("foo.lib"), b"!<arch>\nimport").unwrap();
        let hashed = hash_windows_selected_libraries(
            &[root.path().to_path_buf()],
            &[],
            &["static=foo".to_string()],
            hash_placed,
        )
        .unwrap();
        assert_eq!(
            hashed.get("link:0").map(String::as_str),
            Some(hash_placed(&root.path().join("foo.lib")).unwrap().as_str())
        );
    }

    #[cfg(unix)]
    #[test]
    fn unix_link_arg_inputs_find_files_and_search_dirs_in_each_shape() {
        let parse = |key: &str, value: &str| unix_link_arg_inputs(key, value).unwrap();
        let files = |key: &str, value: &str| parse(key, value).files;
        let dirs = |key: &str, value: &str| parse(key, value).dirs;
        let paths = |items: &[&str]| items.iter().map(PathBuf::from).collect::<Vec<_>>();

        assert_eq!(files("link-arg", "/b/libfoo.a"), paths(&["/b/libfoo.a"]));
        assert_eq!(
            files(
                "link-arg",
                "-Wl,--whole-archive,/b/libfoo.a,--no-whole-archive"
            ),
            paths(&["/b/libfoo.a"])
        );
        assert_eq!(
            files("link-args", "-force_load /b/libfoo.a  /b/extra.o"),
            paths(&["/b/libfoo.a", "/b/extra.o"])
        );
        assert_eq!(
            files("link-arg", "-Wl,--just-symbols=/b/syms.o"),
            paths(&["/b/syms.o"])
        );
        assert_eq!(files("link-arg", "/b/FOO.LIB"), paths(&["/b/FOO.LIB"]));
        // Plain flags carry no file.
        for value in ["-fuse-ld=lld", "--gc-sections"] {
            assert_eq!(
                parse("link-arg", value),
                LinkArgInputs::default(),
                "{value}"
            );
        }

        for (key, value) in [
            ("link-arg", "-L/d"),
            ("link-args", "-L /d"),
            ("link-arg", "-Wl,-L,/d"),
            ("link-arg", "-Wl,-L/d"),
        ] {
            assert_eq!(dirs(key, value), paths(&["/d"]), "{key}={value}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn unix_link_arg_inputs_refuse_what_they_cannot_place() {
        for (key, value) in [
            ("link-arg", "libfoo.a"),
            ("link-arg", "-Wl,-force_load,rel/libfoo.a"),
            ("link-arg", "--start-group=libfoo.a"),
            ("link-arg", "-force_load /b/libfoo.a"),
            ("link-arg", "-L"),
            ("link-arg", "-Wl,-L"),
            ("link-arg", "-Wl,-L,"),
            ("link-arg", "-L=/d"),
            ("link-arg", "-Wl,--library-path=/d"),
            ("link-args", "--library-path /d"),
            ("link-arg", "-l"),
            ("link-arg", "-Wl,-l,"),
            ("link-arg", "-Wl,--library"),
            ("link-arg", "-Wl,--library="),
            ("link-arg", "-Wl,-hidden-l"),
            ("link-arg", "-Wl,-needed-l,"),
        ] {
            assert!(
                unix_link_arg_inputs(key, value).is_err(),
                "{key}={value} must fail"
            );
        }
    }

    /// `-l` libraries a link argument names, in each spelling, for the
    /// caller to resolve.
    #[cfg(unix)]
    #[test]
    fn unix_link_arg_inputs_collect_libraries() {
        let libs = |key: &str, value: &str| unix_link_arg_inputs(key, value).unwrap().libs;
        let lib = |name: &str, verbatim| vec![(name.to_string(), verbatim)];
        assert_eq!(libs("link-arg", "-lfoo"), lib("foo", false));
        assert_eq!(libs("link-arg", "-l:libfoo.a"), lib("libfoo.a", true));
        assert_eq!(
            libs("link-arg", "-Wl,--whole-archive,-lfoo,--no-whole-archive"),
            lib("foo", false)
        );
        assert_eq!(libs("link-args", "-l foo"), lib("foo", false));
        assert_eq!(libs("link-arg", "-Wl,-l,:foo.a"), lib("foo.a", true));
        assert_eq!(libs("link-arg", "-Wl,--library=foo"), lib("foo", false));
        assert_eq!(libs("link-arg", "-Wl,--library,foo"), lib("foo", false));
        let inputs = unix_link_arg_inputs("link-args", "-lfoo /b/extra.o").unwrap();
        assert_eq!(inputs.files, [PathBuf::from("/b/extra.o")]);

        // ld64 looks the name after each of these modifiers up like `-l`.
        for modifier in [
            "hidden",
            "force",
            "needed",
            "reexport",
            "upward",
            "weak",
            "assert-weak",
            "delay",
            "merge",
        ] {
            assert_eq!(
                libs("link-arg", &format!("-Wl,-{modifier}-lfoo")),
                lib("foo", false),
                "{modifier}"
            );
        }
        assert_eq!(
            libs("link-args", "-Wl,-hidden-l:libfoo.a"),
            lib("libfoo.a", true)
        );
        assert!(libs("link-arg", "-Wl,-rpath-link,/d").is_empty());
    }

    /// The value of an option that names an output or a pattern is no input,
    /// whatever it ends in, and an input after it still counts.
    #[cfg(unix)]
    #[test]
    fn unix_link_arg_inputs_skip_output_and_pattern_values() {
        let files = |key: &str, value: &str| unix_link_arg_inputs(key, value).unwrap().files;
        for (key, value) in [
            ("link-arg", "-Wl,--exclude-libs,libssl.a"),
            ("link-arg", "-Wl,--exclude-libs=libssl.a:libcrypto.a"),
            ("link-arg", "-Wl,-exclude-libs,libssl.a"),
            ("link-arg", "-Wl,--out-implib,/abs/foo.dll.a"),
            ("link-arg", "-Wl,--out-implib=/abs/foo.dll.a"),
            ("link-arg", "-Wl,-object_path_lto,/abs/lto.o"),
            ("link-args", "-Wl,--exclude-libs libssl.a"),
            ("link-arg", "-Wl,--exclude-libs"),
        ] {
            assert!(files(key, value).is_empty(), "{key}={value}");
        }
        assert_eq!(
            files("link-arg", "-Wl,--exclude-libs=libssl.a,/b/libfoo.a"),
            [PathBuf::from("/b/libfoo.a")]
        );
        assert_eq!(
            files("link-arg", "-Wl,--exclude-libs,libssl.a,/b/libfoo.a"),
            [PathBuf::from("/b/libfoo.a")]
        );
        assert!(!takes_non_input_value("exclude-libs"), "an operand");
        assert!(!takes_non_input_value("--whole-archive"));
    }

    #[test]
    fn banner_memo_key_folds_tool_bytes_path_and_environment() {
        let dir = tempfile::tempdir().unwrap();
        let link = placed(dir.path(), "link.exe");
        let digest = probe_memo::file_digest(&link).unwrap();
        let environment: Vec<(OsString, OsString)> = vec![("LIB".into(), "C:\\libs".into())];

        let base = banner_memo_key(WindowsTool::Link, &link, &digest, &environment);
        assert_eq!(
            base,
            banner_memo_key(WindowsTool::Link, &link, &digest, &environment)
        );
        assert_ne!(
            base,
            banner_memo_key(WindowsTool::LldLink, &link, &digest, &environment)
        );
        assert_ne!(
            base,
            banner_memo_key(WindowsTool::Link, &link, "other-bytes", &environment)
        );
        assert_ne!(
            base,
            banner_memo_key(
                WindowsTool::Link,
                &dir.path().join("x.exe"),
                &digest,
                &environment
            )
        );
        assert_ne!(
            base,
            banner_memo_key(WindowsTool::Link, &link, &digest, &[])
        );
        let split: Vec<(OsString, OsString)> = vec![("LI".into(), "BC:\\libs".into())];
        assert_ne!(
            base,
            banner_memo_key(WindowsTool::Link, &link, &digest, &split),
            "field boundaries are part of the key"
        );

        let memo_dir = dir.path().join("memo");
        let (path, key) = banner_memo(&memo_dir, WindowsTool::Cl, &link, &digest, &[]);
        assert_eq!(key, banner_memo_key(WindowsTool::Cl, &link, &digest, &[]));
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        assert_eq!(name, format!("msvc-banner-{}.txt", &key[..16]));
        assert_eq!(path.parent(), Some(memo_dir.as_path()));
    }

    /// A memoised banner is served without running the tool; only a
    /// validated banner is written; the memo follows the tool's bytes.
    #[cfg(unix)]
    #[test]
    fn run_windows_tool_serves_and_fills_the_banner_memo() {
        let _lock = process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let memo_dir = dir.path().join("memo");
        let run = |tool: WindowsTool, path: &Path, environment: &[(OsString, OsString)]| {
            run_windows_tool_in(Some(&memo_dir), tool, path, environment)
        };
        let write_tool = |name: &str, banner: &str| {
            let path = dir.path().join(name);
            kache_fs::testutil::write_executable(&path, format!("#!/bin/sh\necho '{banner}'\n"));
            path
        };
        let environment: Vec<(OsString, OsString)> = vec![("KACHE_TEST_MEMO".into(), "1".into())];
        let memo_for = |path: &Path| {
            banner_memo(
                &memo_dir,
                WindowsTool::Link,
                path,
                &probe_memo::file_digest(path).unwrap(),
                &environment,
            )
        };

        // A tool whose banner never validates runs every time and leaves no memo.
        let broken = write_tool("broken-link.exe", "not a linker");
        let (broken_memo, _) = memo_for(&broken);
        let _ = std::fs::remove_file(&broken_memo);
        assert!(run(WindowsTool::Link, &broken, &environment).is_err());
        assert!(!broken_memo.exists(), "a failed probe must not be memoised");

        // A valid banner is memoised, and the memo answers while the bytes match.
        let link = write_tool("link.exe", LINK_BANNER);
        let (memo, key) = memo_for(&link);
        let _ = std::fs::remove_file(&memo);
        assert_eq!(
            run(WindowsTool::Link, &link, &environment).unwrap(),
            LINK_BANNER
        );
        assert!(
            memo.exists(),
            "a validated banner is written to {}",
            memo.display()
        );

        let served = "Microsoft (R) Incremental Linker Version 99.0.0.0";
        probe_memo::write_verified(&memo, &key, &format!("{served}\n"));
        assert_eq!(
            run(WindowsTool::Link, &link, &environment).unwrap(),
            served,
            "the memo answers instead of the tool"
        );

        // Replacing the tool's bytes (same length) is a different key: the
        // tool runs, and the old memo is left alone.
        let replaced = "Microsoft (R) Incremental Linker Version 14.44.35208.0";
        assert_eq!(replaced.len(), LINK_BANNER.len());
        write_tool("link.exe", replaced);
        let (memo_after, _) = memo_for(&link);
        assert_ne!(memo_after, memo);
        let _ = std::fs::remove_file(&memo_after);
        assert_eq!(
            run(WindowsTool::Link, &link, &environment).unwrap(),
            replaced
        );
        assert!(memo_after.exists());
        assert_eq!(
            probe_memo::read_verified(&memo, &key).unwrap().trim(),
            served,
            "the previous binary's memo is untouched"
        );

        // lld-link is probed fresh every time: a launcher's bytes do not
        // determine its banner.
        let lld = write_tool("lld-link.exe", LLD_LINK_BANNER);
        let (lld_memo, _) = banner_memo(
            &memo_dir,
            WindowsTool::LldLink,
            &lld,
            &probe_memo::file_digest(&lld).unwrap(),
            &environment,
        );
        let _ = std::fs::remove_file(&lld_memo);
        assert_eq!(
            run(WindowsTool::LldLink, &lld, &environment).unwrap(),
            LLD_LINK_BANNER
        );
        assert!(!lld_memo.exists(), "lld-link banners are never memoised");
        assert!(memoises_banner(WindowsTool::Link));
        assert!(memoises_banner(WindowsTool::Cl));
        assert!(!memoises_banner(WindowsTool::LldLink));

        // A memo that no longer validates is ignored and refilled from the tool.
        let (memo, key) = memo_for(&link);
        probe_memo::write_verified(&memo, &key, "garbage\n");
        assert_eq!(
            run(WindowsTool::Link, &link, &environment).unwrap(),
            replaced
        );
        assert_eq!(
            probe_memo::read_verified(&memo, &key).unwrap().trim(),
            replaced
        );
        // Without a memo directory nothing is read or written.
        let entries = || std::fs::read_dir(&memo_dir).unwrap().count();
        let before = entries();
        assert_eq!(
            run_windows_tool_in(None, WindowsTool::Link, &link, &environment).unwrap(),
            replaced
        );
        assert_eq!(entries(), before);
        let _ = (memo, memo_after);
    }

    #[cfg(unix)]
    #[test]
    fn driver_library_search_dirs_keeps_absolute_directories_that_exist() {
        let dir = tempfile::tempdir().unwrap();
        let lib = dir.path().join("lib");
        std::fs::create_dir(&lib).unwrap();
        let file = dir.path().join("not-a-dir");
        std::fs::write(&file, "").unwrap();
        let driver = dir.path().join("cc");
        kache_fs::testutil::write_executable(
            &driver,
            format!(
                "#!/bin/sh\ncase \"$1\" in\n  -print-search-dirs) echo \"install: /x\"; echo \"libraries: ={lib}:.:{file}:{absent}\" ;;\nesac\n",
                lib = lib.display(),
                file = file.display(),
                absent = dir.path().join("absent").display(),
            ),
        );
        assert_eq!(driver_library_search_dirs(&driver), vec![lib]);
        let silent = dir.path().join("silent");
        kache_fs::testutil::write_executable(&silent, "#!/bin/sh\nexit 0\n");
        assert!(driver_library_search_dirs(&silent).is_empty());
        let failing = dir.path().join("failing");
        kache_fs::testutil::write_executable(&failing, "#!/bin/sh\necho 'libraries: =/'; exit 1\n");
        assert!(driver_library_search_dirs(&failing).is_empty());
        assert!(driver_library_search_dirs(&dir.path().join("absent")).is_empty());
    }
}
