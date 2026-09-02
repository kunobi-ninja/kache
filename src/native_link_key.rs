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
        .any(|token| {
            windows_link_token_names_input_file(token) || windows_link_token_is_file_option(token)
        })
}

/// A bare or option-value token whose extension marks a linker input file.
fn windows_link_token_names_input_file(token: &str) -> bool {
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

fn ends_with_ignore_ascii_case(token: &str, suffix: &str) -> bool {
    token
        .len()
        .checked_sub(suffix.len())
        .and_then(|start| token.get(start..))
        .is_some_and(|tail| tail.eq_ignore_ascii_case(suffix))
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

fn run_windows_tool(
    tool: WindowsTool,
    path: &Path,
    environment: &[(OsString, OsString)],
) -> Result<String> {
    let output = windows_tool_command(tool, path, environment)
        .output()
        .with_context(|| format!("running {} at {}", tool.name(), path.display()))?;
    let text = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    validate_tool_banner(tool, &text)
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
    })
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
        use std::os::unix::fs::PermissionsExt;

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
            std::fs::write(&path, &script).unwrap();
            let mut permissions = std::fs::metadata(&path).unwrap().permissions();
            permissions.set_mode(0o755);
            std::fs::set_permissions(path, permissions).unwrap();
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
        let _lock = process_state_test_lock();
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
        std::fs::create_dir_all(root.path().join("Include/not-a-version")).unwrap();
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
}
