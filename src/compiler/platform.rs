//! Platform abstraction for OS-specific behavior on cached artifacts.
//!
//! Today the only behavior that varies per platform on the cache hot
//! path is **binary loadability**: macOS arm64 requires every executable
//! and dynamic library to carry a valid ad-hoc signature, or `dyld`
//! refuses to load it. Linux and Windows have no such requirement.
//!
//! Before this module, that lived behind `#[cfg(target_os = "macos")]`
//! arms in [`crate::compile`]. The cfg approach has two costs:
//!
//! 1. **Untestable from the wrong host.** Linux developers couldn't
//!    write a unit test that exercised the macOS code path because it
//!    didn't compile. Bugs in the macOS path landed only when a macOS
//!    runner happened to catch them.
//! 2. **Open to duplication.** When the C-family wrapper lands real
//!    caching, restored C/C++ executables need codesigning too. Without
//!    a trait, the cc store path would re-implement the same `cfg` arm
//!    — and the same bug class would be back.
//!
//! The trait isolates *what* needs to happen ("ensure this binary is
//! loadable") from *how* the host accomplishes it. The cc store path
//! gets codesign for free by routing through
//! [`super::PostRestoreAction::Sign`], which dispatches via `Platform`.
//!
//! The second per-platform behavior is **relocatable debug info for
//! cached executables** ([`Platform::package_debug_bundle`],
//! kunobi-ninja/kache#319): a macOS `-g` binary carries `N_OSO`
//! debug-map records pointing at per-build `.o` files, so a restored
//! binary loses source-level debugging elsewhere. At store time the
//! macOS impl hands the wrapper one flat tar of a `.dSYM`. Unpacked
//! debug info still has the `.o` files, so that tar comes from
//! `dsymutil`. Packed debug info, including rustc's default when
//! `-Csplit-debuginfo` was not passed, already has the adjacent bundle.
//! lldb prefers an adjacent UUID-matched `.dSYM` over the debug map, so
//! no binary mutation is needed.
//!
//! ## Future methods
//!
//! New trait methods land when their callers exist (no speculative
//! interface bloat). Concrete cases on the roadmap:
//!
//! - `source_date_epoch() -> Option<u64>` — for the C/C++ preprocessor
//!   cache key. Honored by gcc + clang; neutralizes `__DATE__` /
//!   `__TIME__` macros.
//! - `probe_compiler(path) -> CompilerInfo` — for cc cache keys to
//!   know gcc-vs-clang-vs-MSVC.

use crate::invocation::InvocationEnv;
use anyhow::{Context as _, Result};
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::Command;

/// What [`Platform::ensure_binary_loadable`] established about a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Loadability {
    /// The existing signature was checked and is valid; the file was not
    /// touched. The same bytes will pass the same check on this host again.
    Verified,
    /// Nothing was proven: the host has no check, the tool could not run,
    /// or the file had to be signed.
    Unverified,
}

/// Platform-specific behavior the cache layer needs to apply to
/// restored artifacts. One impl per OS; today only [`MacOsPlatform`]
/// does non-trivial work.
///
/// The trait is `Send + Sync` so a single instance can be constructed
/// at startup and shared across the wrapper's restore loops.
pub trait Platform: Send + Sync {
    /// Short identifier used in tracing output. Stable across versions
    /// so log filters and dashboards can match on it.
    fn name(&self) -> &'static str;

    /// Apply a signature only if the existing one is missing or
    /// invalid, so the OS will load this artifact.
    ///
    /// **Contract**: must be idempotent and must NOT mutate bytes when
    /// an existing signature is already valid. The mutation cost is
    /// real — re-signing changes the file's content hash, which
    /// corrupts the cached blob's identity (kache-fork bug 59866c0).
    /// Each impl is responsible for the verify-then-sign sequence;
    /// callers do not guard.
    ///
    /// **Failure handling**: best-effort. A failed signature attempt
    /// logs a warning and returns `Ok(())`; it must not abort the
    /// wrapper's restore loop. Returning `Err` is reserved for
    /// failures so structural that the next action would also fail
    /// (e.g. the path doesn't exist).
    fn ensure_binary_loadable(&self, path: &Path) -> Result<Loadability>;

    /// Where this host remembers the store blobs whose restored copy
    /// [`Self::ensure_binary_loadable`] found [`Loadability::Verified`], one
    /// empty file per blob hash. A blob is content-addressed, so a restore
    /// that did not rewrite its bytes can skip the check next time. `None`,
    /// the default, where the check costs nothing.
    fn verified_loadable_dir(&self) -> Option<PathBuf> {
        None
    }

    /// Produce a self-contained, relocatable debug-info companion for a
    /// just-linked binary, as ONE flat file inside `staging_dir`
    /// (kunobi-ninja/kache#319).
    ///
    /// `split_debuginfo` is rustc's `-Csplit-debuginfo` value, or `None`
    /// when that flag was not passed. On macOS, `unpacked` still has the
    /// `.o` files the `N_OSO` debug map points at, so the impl runs
    /// `dsymutil`. `packed`, and an absent flag (rustc's macOS default),
    /// already wrote an adjacent `.dSYM` and removed those objects
    /// (kunobi-ninja/kache#1752): the impl packages that bundle and does
    /// not run `dsymutil` again. Returns `Ok(None)` when the platform has
    /// no such companion (Linux embeds DWARF; Windows stores the `.pdb`
    /// as an output of its own), when packed debug info left no bundle,
    /// or when the packaging tool is unavailable or fails. A missing
    /// debug bundle degrades debuggability of the cached binary, never
    /// the build.
    ///
    /// The macOS impl also leaves the `.dSYM` bundle itself next to the
    /// binary (see [`MacOsPlatform`]), so the producing build and a
    /// restoring build converge on the same on-disk shape.
    fn package_debug_bundle(
        &self,
        binary: &Path,
        staging_dir: &Path,
        split_debuginfo: Option<&str>,
    ) -> Result<Option<PathBuf>>;

    /// Whether a restored executable or dynamic library may share its inode
    /// with the store blob when nothing else rewrites it in place. True only
    /// where [`Self::ensure_binary_loadable`] never writes to the file and a
    /// link can keep the blob's read-only mode. The default keeps every
    /// restored loadable a private copy.
    fn may_share_restored_loadables(&self) -> bool {
        false
    }
}

/// Detect the current host platform.
///
/// Returns a boxed trait object so callers don't carry a generic
/// `Platform` parameter through every type. The dispatch cost is
/// negligible relative to the work each method does (codesign shells
/// out; the vtable lookup is in the noise).
#[cfg(test)]
pub fn current() -> Box<dyn Platform> {
    current_for(&InvocationEnv::capture())
}

pub fn current_for(env: &InvocationEnv) -> Box<dyn Platform> {
    #[cfg(not(target_os = "macos"))]
    let _ = env;
    #[cfg(target_os = "macos")]
    {
        Box::new(MacOsPlatform { env: env.clone() })
    }
    #[cfg(target_os = "linux")]
    {
        Box::new(LinuxPlatform)
    }
    #[cfg(target_os = "windows")]
    {
        Box::new(WindowsPlatform)
    }
}

/// macOS implementation. Currently handles ad-hoc codesigning on
/// arm64; other macOS variants (x86_64, future archs) fall through to
/// no-op because their loaders don't enforce the same requirement.
///
/// Off macOS, `current()` doesn't construct it but cross-platform unit
/// tests do, so any host can pin the macOS dispatch shape.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
pub struct MacOsPlatform {
    env: InvocationEnv,
}

#[cfg(test)]
impl Default for MacOsPlatform {
    fn default() -> Self {
        Self {
            env: InvocationEnv::capture(),
        }
    }
}

impl MacOsPlatform {
    #[cfg(target_os = "macos")]
    fn codesign(&self, flags: &[&str], path: &Path) -> std::io::Result<std::process::ExitStatus> {
        crate::invocation::io::status(self.env.command("codesign")?.args(flags).arg(path))
    }
}

impl Platform for MacOsPlatform {
    fn name(&self) -> &'static str {
        "macos"
    }

    #[cfg(target_os = "macos")]
    fn ensure_binary_loadable(&self, path: &Path) -> Result<Loadability> {
        if !signs_on_load(std::env::consts::ARCH, std::env::consts::OS) {
            return Ok(Loadability::Unverified);
        }

        // verify-then-sign: skip mutation when ld64's signature is
        // still valid. `codesign --verify --strict` exits 0 iff a
        // structurally-valid signature is already present.
        let verify = match self.codesign(&["--verify", "--strict"], path) {
            Ok(status) => status,
            Err(err) => {
                tracing::warn!(
                    "unable to run codesign --verify for {}: {err}",
                    path.display()
                );
                return Ok(Loadability::Unverified);
            }
        };

        if verify.success() {
            tracing::debug!(
                "ad-hoc signature already valid for {}, skipping re-sign",
                path.display()
            );
            return Ok(Loadability::Verified);
        }

        tracing::debug!(
            "ad-hoc signature missing or invalid for {}, re-applying",
            path.display()
        );
        let status = match self.codesign(&["--sign", "-", "--force"], path) {
            Ok(status) => status,
            Err(err) => {
                tracing::warn!(
                    "unable to run codesign --sign for {}: {err}",
                    path.display()
                );
                return Ok(Loadability::Unverified);
            }
        };

        if !status.success() {
            tracing::warn!("ad-hoc codesign failed for {}", path.display());
        }
        Ok(Loadability::Unverified)
    }

    #[cfg(not(target_os = "macos"))]
    fn ensure_binary_loadable(&self, _path: &Path) -> Result<Loadability> {
        Ok(Loadability::Unverified)
    }

    /// Kept per kernel release: a check that passed under one macOS need not
    /// pass under the next, so an update starts the memo afresh.
    fn verified_loadable_dir(&self) -> Option<PathBuf> {
        #[cfg(unix)]
        let release = kernel_release;
        #[cfg(not(unix))]
        let release = || None;
        macos_verified_loadable_dir(
            std::env::consts::ARCH,
            std::env::consts::OS,
            release,
            &crate::config::probe_memo_dir_for(&self.env).ok()?,
        )
    }

    fn package_debug_bundle(
        &self,
        binary: &Path,
        staging_dir: &Path,
        split_debuginfo: Option<&str>,
    ) -> Result<Option<PathBuf>> {
        package_macos_debug_bundle(
            &self.env,
            binary,
            staging_dir,
            split_debuginfo,
            std::env::consts::OS,
        )
    }
}

/// What to do at the dsymutil spawn for one macOS binary.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
enum DebugBundleSpawn {
    /// Packed debug info left no bundle, or this output is not Mach-O.
    /// Cache the binary and do not warn.
    Skip,
    /// rustc already wrote the adjacent `.dSYM`. Tar it; do not spawn.
    Reuse,
    /// Objects are still on disk. Run this command, then tar its output.
    Run(Command),
}

/// Package a macOS debug bundle, or `Ok(None)` on any other host.
///
/// `host_os` is [`std::env::consts::OS`] in production. Tests pass `"macos"`
/// so the spawn decision runs on hosts that are not macOS.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn package_macos_debug_bundle(
    env: &InvocationEnv,
    binary: &Path,
    staging_dir: &Path,
    split_debuginfo: Option<&str>,
    host_os: &str,
) -> Result<Option<PathBuf>> {
    // Compiled on every host (same convention as ensure_binary_loadable):
    // the actual `dsymutil` spawn is runtime-gated so a Linux test can
    // construct MacOsPlatform and exercise this method as a no-op.
    if host_os != "macos" {
        return Ok(None);
    }
    let binary_path = env
        .resolve(binary)
        .context("debug binary requires the invocation directory")?;
    let staging_path = env
        .resolve(staging_dir)
        .context("debug staging requires the invocation directory")?;
    let binary = binary_path.as_path();
    let staging_dir = staging_path.as_path();
    let Some(file_name) = binary.file_name().and_then(|n| n.to_str()) else {
        tracing::warn!(
            "not packaging a debug bundle: binary has no usable file name: {}",
            binary.display()
        );
        return Ok(None);
    };

    // The `.dSYM` stays next to the binary, not in the staging dir
    // (kunobi-ninja/kache#319): a restoring build unpacks the bundle to
    // that sibling path, and lldb looks up `<binary>.dSYM` there.
    let bundle_dir = binary.with_file_name(format!("{file_name}.dSYM"));
    let tar_path = staging_dir.join(format!("{file_name}.dsym.tar"));
    match debug_bundle_spawn(binary, &bundle_dir, env, split_debuginfo)? {
        DebugBundleSpawn::Skip => return Ok(None),
        DebugBundleSpawn::Reuse => {}
        DebugBundleSpawn::Run(mut command) => {
            let status = match crate::invocation::io::status(&mut command) {
                Ok(status) => status,
                Err(err) => {
                    // Best-effort per the trait contract: no dsymutil (unusual
                    // but possible without Xcode CLT) → cache the binary
                    // without a bundle rather than fail the store.
                    tracing::warn!("unable to run dsymutil for {}: {err}", binary.display());
                    return Ok(None);
                }
            };
            if !status.success() {
                tracing::warn!("dsymutil failed for {}", binary.display());
                return Ok(None);
            }
        }
    }

    // Tar the bundle into the ONE flat file the store can hold
    // (single-component artifact names, file-level hashing/linking).
    match build_deterministic_tar(&bundle_dir, &tar_path) {
        Ok(()) => Ok(Some(tar_path)),
        Err(err) => {
            tracing::warn!(
                "failed to package debug bundle for {}: {err:#}",
                binary.display()
            );
            let _ = std::fs::remove_file(&tar_path);
            Ok(None)
        }
    }
}

/// Decide whether the spawn below builds a `dsymutil` command.
///
/// `None` and `"packed"` are rustc's packed macOS debug info
/// (kunobi-ninja/kache#1752): rustc already ran dsymutil, wrote the
/// adjacent `.dSYM`, and deleted the `.o` files. Spawning again warns
/// that it cannot open those objects and the new bundle drops compile
/// units. `"unpacked"` keeps the objects, so dsymutil still runs.
/// `split-debuginfo=off` never reaches here.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn debug_bundle_spawn(
    binary: &Path,
    bundle_dir: &Path,
    env: &InvocationEnv,
    split_debuginfo: Option<&str>,
) -> Result<DebugBundleSpawn> {
    if split_debuginfo_already_packed(split_debuginfo) {
        return Ok(if bundle_dir.is_dir() {
            DebugBundleSpawn::Reuse
        } else {
            DebugBundleSpawn::Skip
        });
    }
    match macho_debug_bundle_command_for(binary, bundle_dir, env)? {
        Some(command) => Ok(DebugBundleSpawn::Run(command)),
        None => Ok(DebugBundleSpawn::Skip),
    }
}

/// `packed`, or no `-Csplit-debuginfo` at all. rustc's macOS default for
/// an absent flag is packed.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn split_debuginfo_already_packed(split_debuginfo: Option<&str>) -> bool {
    matches!(split_debuginfo, None | Some("packed"))
}

/// Cross-compilation can produce ELF or PE executables on a macOS host.
/// Inspect only the format marker; dsymutil validates the Mach-O contents.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn macho_debug_bundle_command_for(
    binary: &Path,
    bundle_dir: &Path,
    env: &InvocationEnv,
) -> Result<Option<Command>> {
    let binary = env
        .resolve(binary)
        .context("debug binary requires the invocation directory")?;
    let mut magic = [0; 4];
    if let Err(err) = std::fs::File::open(&binary).and_then(|mut file| file.read_exact(&mut magic))
    {
        tracing::warn!("unable to inspect debug binary {}: {err}", binary.display());
        return Ok(None);
    }
    if !matches!(
        &magic,
        // Thin 32/64-bit Mach-O and universal 32/64-bit Mach-O, in both
        // byte orders. A universal executable can carry multiple slices.
        b"\xce\xfa\xed\xfe"
            | b"\xfe\xed\xfa\xce"
            | b"\xcf\xfa\xed\xfe"
            | b"\xfe\xed\xfa\xcf"
            | b"\xca\xfe\xba\xbe"
            | b"\xbe\xba\xfe\xca"
            | b"\xca\xfe\xba\xbf"
            | b"\xbf\xba\xfe\xca"
    ) {
        return Ok(None);
    }
    debug_bundle_command_for(&binary, bundle_dir, env).map(Some)
}

/// Resolve paths before changing the child's cwd: cached macOS debug links
/// use `-oso_prefix` to make OSO records relative to Cargo's profile
/// directory (see `macos_oso_prefix_root`), so dsymutil must run from that
/// same root. A test binary in `<profile>/deps` records `deps/<obj>.o`,
/// which only resolves from `<profile>` (kunobi-ninja/kache#1161).
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
pub(crate) fn debug_bundle_command_for(
    binary: &Path,
    bundle_dir: &Path,
    env: &InvocationEnv,
) -> Result<Command> {
    let binary = env
        .resolve(binary)
        .context("debug binary requires the invocation directory")?;
    let bundle_dir = env
        .resolve(bundle_dir)
        .context("debug bundle requires the invocation directory")?;
    let output_dir = binary
        .parent()
        .context("debug binary has no parent directory")?;
    let oso_root = cargo_profile_dir(output_dir).unwrap_or_else(|| output_dir.to_path_buf());
    let mut command = env.command("dsymutil")?;
    command
        .current_dir(oso_root)
        .arg(&binary)
        .arg("-o")
        .arg(bundle_dir);
    Ok(command)
}

/// `out_dir`'s ancestor that is Cargo's profile directory, or `None` when
/// this is not one of Cargo's link output directories. See
/// [`crate::cargo_layout::profile_dir`].
pub(crate) fn cargo_profile_dir(out_dir: &Path) -> Option<PathBuf> {
    crate::cargo_layout::profile_dir(out_dir)
}

/// Tar `bundle_dir`'s contents (paths relative to the bundle root, e.g.
/// `Contents/Resources/DWARF/<name>`) into `tar_path`, byte-reproducibly:
/// entries are file-only, sorted by path, with mtime/uid/gid pinned to 0 and
/// fixed modes. Reproducible tar bytes mean two identical dSYMs dedupe to one
/// content-addressed store blob (kunobi-ninja/kache#319).
///
/// Used by macOS rustc ingest and by the C/C++ link store when a compiler
/// already left a `.dSYM` directory next to the binary.
pub(crate) fn build_deterministic_tar(bundle_dir: &Path, tar_path: &Path) -> Result<()> {
    let mut files = Vec::new();
    collect_files_recursively(bundle_dir, bundle_dir, &mut files)?;
    // Sort by the relative path's byte representation so the entry order
    // never depends on readdir order.
    files.sort();

    let out = std::fs::File::create(tar_path)
        .with_context(|| format!("creating {}", tar_path.display()))?;
    let mut builder = tar::Builder::new(out);
    for rel in files {
        let abs = bundle_dir.join(&rel);
        let mut file =
            std::fs::File::open(&abs).with_context(|| format!("opening {}", abs.display()))?;
        let size = file
            .metadata()
            .with_context(|| format!("stat {}", abs.display()))?
            .len();
        let mut header = tar::Header::new_gnu();
        header.set_size(size);
        header.set_mode(0o644);
        header.set_mtime(0);
        header.set_uid(0);
        header.set_gid(0);
        header.set_entry_type(tar::EntryType::Regular);
        builder
            .append_data(&mut header, &rel, &mut file)
            .with_context(|| format!("appending {}", rel.display()))?;
    }
    builder.finish().context("finishing debug bundle tar")?;
    Ok(())
}

/// Collect every regular file under `dir`, as paths relative to `root`.
/// Directories are implied by their files (the unpacker `create_dir_all`s
/// parents), which keeps the archive minimal and the byte layout stable.
fn collect_files_recursively(root: &Path, dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(dir).with_context(|| format!("reading dir {}", dir.display()))? {
        let entry = entry.with_context(|| format!("reading dir entry in {}", dir.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("stat {}", path.display()))?;
        if file_type.is_dir() {
            collect_files_recursively(root, &path, files)?;
        } else if file_type.is_file() {
            let rel = path
                .strip_prefix(root)
                .with_context(|| format!("relativizing {}", path.display()))?
                .to_path_buf();
            files.push(rel);
        }
        // Symlinks are skipped: a dsymutil bundle contains none, and the
        // restore-side unpacker rejects link entries outright (#211-style
        // hardening), so packaging one would only produce a bundle that can
        // never restore.
    }
    Ok(())
}

/// Linux implementation. The kernel doesn't enforce signatures on
/// ELF binaries, so [`Platform::ensure_binary_loadable`] is a no-op.
/// Lives as a concrete struct (not a unit `()`) so it can grow
/// methods independently of the macOS impl when Linux-specific
/// concerns appear.
///
/// Cross-platform unit tests construct `LinuxPlatform` from a macOS host
/// (and vice versa) to exercise the dispatch shape without spawning real
/// `codesign` / `signtool`. On a non-Linux production build no caller
/// constructs it, but having the struct compile keeps the test surface
/// symmetric.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub struct LinuxPlatform;

impl Platform for LinuxPlatform {
    fn name(&self) -> &'static str {
        "linux"
    }

    fn ensure_binary_loadable(&self, _path: &Path) -> Result<Loadability> {
        Ok(Loadability::Unverified)
    }

    fn package_debug_bundle(
        &self,
        _binary: &Path,
        _staging_dir: &Path,
        _split_debuginfo: Option<&str>,
    ) -> Result<Option<PathBuf>> {
        // Linux embeds DWARF in the binary under default `-Cdebuginfo`
        // settings, so a restored executable is already self-contained
        // (kunobi-ninja/kache#319) — nothing to package.
        Ok(None)
    }

    // Nothing here signs a restored file. macOS keeps the default because
    // codesign may rewrite one in place; Windows keeps it because an NTFS
    // hardlink shares the blob's read-only attribute (#429).
    fn may_share_restored_loadables(&self) -> bool {
        true
    }
}

/// Windows implementation. Authenticode signing is not enforced for
/// load-time loading of unsigned PE binaries (only for kernel-mode
/// drivers and SmartScreen), so [`Platform::ensure_binary_loadable`]
/// is a no-op. PE/PDB pairing is checked before storage and before restoring
/// any cached output (`pe_pdb`), including explicitly named executables.
///
/// Constructed off Windows only by tests; see [`LinuxPlatform`].
#[cfg_attr(not(target_os = "windows"), allow(dead_code))]
pub struct WindowsPlatform;

impl Platform for WindowsPlatform {
    fn name(&self) -> &'static str {
        "windows"
    }

    fn ensure_binary_loadable(&self, _path: &Path) -> Result<Loadability> {
        Ok(Loadability::Unverified)
    }

    fn package_debug_bundle(
        &self,
        _binary: &Path,
        _staging_dir: &Path,
        _split_debuginfo: Option<&str>,
    ) -> Result<Option<PathBuf>> {
        // Nothing to package: rustc names the `.pdb` by file name
        // (`/PDBALTPATH:%_PDB%`) and it is stored with the `.exe` as an
        // output of its own; the store checks the pair (`pe_pdb`).
        Ok(None)
    }
}

/// Whether the loader requires a valid signature, so that
/// [`MacOsPlatform::ensure_binary_loadable`] has something to check: arm64
/// macOS only.
fn signs_on_load(arch: &str, os: &str) -> bool {
    arch == "aarch64" && os == "macos"
}

/// The memo directory under `probes` for a macOS host, or `None` where
/// [`MacOsPlatform::ensure_binary_loadable`] checks nothing: every host but
/// arm64 macOS. Unused outside tests on other hosts, like [`MacOsPlatform`].
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn macos_verified_loadable_dir(
    arch: &str,
    os: &str,
    release: impl FnOnce() -> Option<String>,
    probes: &Path,
) -> Option<PathBuf> {
    if !signs_on_load(arch, os) {
        return None;
    }
    let release = release()?;
    Some(
        probes
            .join("verified-loadables")
            .join(format!("darwin-{release}")),
    )
}

/// The running kernel's release (`uname -r`), such as `25.6.0`.
#[cfg(unix)]
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn kernel_release() -> Option<String> {
    // SAFETY: `utsname` is plain C data, valid when zeroed.
    let mut name: libc::utsname = unsafe { std::mem::zeroed() };
    // SAFETY: `uname` writes only into the struct it is handed.
    if unsafe { libc::uname(&mut name) } != 0 {
        return None;
    }
    // SAFETY: on success `release` holds a NUL-terminated string.
    let release = unsafe { std::ffi::CStr::from_ptr(name.release.as_ptr()) };
    release
        .to_str()
        .ok()
        .filter(|release| !release.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Test double: counts calls to each method so dispatch tests can
    /// assert the wrapper's restore loop routes through `Platform`
    /// rather than re-implementing OS-specific behavior in-line.
    pub struct CountingPlatform {
        ensure_binary_loadable_calls: AtomicUsize,
        package_debug_bundle_calls: AtomicUsize,
        verified_dir: Option<PathBuf>,
    }

    impl CountingPlatform {
        pub fn new() -> Self {
            Self {
                ensure_binary_loadable_calls: AtomicUsize::new(0),
                package_debug_bundle_calls: AtomicUsize::new(0),
                verified_dir: None,
            }
        }

        /// A platform whose check always passes, remembered under `dir`.
        pub fn verifying_into(dir: PathBuf) -> Self {
            Self {
                verified_dir: Some(dir),
                ..Self::new()
            }
        }

        pub fn ensure_calls(&self) -> usize {
            self.ensure_binary_loadable_calls.load(Ordering::Relaxed)
        }

        pub fn package_calls(&self) -> usize {
            self.package_debug_bundle_calls.load(Ordering::Relaxed)
        }
    }

    impl Platform for CountingPlatform {
        fn name(&self) -> &'static str {
            "counting"
        }
        fn ensure_binary_loadable(&self, _path: &Path) -> Result<Loadability> {
            self.ensure_binary_loadable_calls
                .fetch_add(1, Ordering::Relaxed);
            Ok(if self.verified_dir.is_some() {
                Loadability::Verified
            } else {
                Loadability::Unverified
            })
        }
        fn verified_loadable_dir(&self) -> Option<PathBuf> {
            self.verified_dir.clone()
        }
        fn package_debug_bundle(
            &self,
            _binary: &Path,
            _staging_dir: &Path,
            _split_debuginfo: Option<&str>,
        ) -> Result<Option<PathBuf>> {
            self.package_debug_bundle_calls
                .fetch_add(1, Ordering::Relaxed);
            Ok(None)
        }
    }

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    #[test]
    fn codesign_requests_use_client_tools_environment_and_streams() {
        use crate::invocation::io::{InvocationIo, InvocationIoScope};
        use std::os::unix::fs::PermissionsExt;

        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|threads| {
            for (directory, name) in [(first.path(), "first"), (second.path(), "second")] {
                let barrier = &barrier;
                threads.spawn(move || {
                    barrier.wait();
                    let root = directory.canonicalize().unwrap();
                    let bin = root.join("tools");
                    std::fs::create_dir_all(&bin).unwrap();
                    let codesign = bin.join("codesign");
                    std::fs::write(&codesign, b"#!/bin/sh\nprintf '%s:%s\\n' \"$CLIENT\" \"$*\" >> calls\nprintf '%s\\n' \"$CLIENT\" >&2\n[ \"$1\" != --verify ] || [ \"$CLIENT\" = first ]\n").unwrap();
                    std::fs::set_permissions(&codesign, std::fs::Permissions::from_mode(0o755)).unwrap();
                    let env = InvocationEnv::from_parts([
                        ("PATH", bin.into_os_string()), ("CLIENT", name.into()),
                        ("KACHE_CACHE_DIR", root.join("cache").into_os_string()),
                    ], Some(root.clone()), None, root.clone());
                    let _io = InvocationIoScope::enter(InvocationIo::from_files(
                        tempfile::tempfile_in(&root).unwrap(),
                        std::fs::File::create(root.join("stdout")).unwrap(),
                        std::fs::File::create(root.join("stderr")).unwrap(), None,
                    ));
                    let platform = current_for(&env);
                    let result = platform.ensure_binary_loadable(Path::new("artifact")).unwrap();
                    let expected = if name == "first" { Loadability::Verified } else { Loadability::Unverified };
                    assert_eq!(result, expected);
                    let calls = std::fs::read_to_string(root.join("calls")).unwrap();
                    let extra = if name == "second" { "second:--sign - --force artifact\n" } else { "" };
                    assert_eq!(calls, format!("{name}:--verify --strict artifact\n{extra}"));
                    assert_eq!(std::fs::read_to_string(root.join("stderr")).unwrap(), if name == "first" { "first\n" } else { "second\nsecond\n" });
                    assert!(platform.verified_loadable_dir().unwrap().starts_with(root.join("cache/probes")));
                });
            }
        });
    }

    fn debug_env(root: &Path) -> InvocationEnv {
        let bin = root.join("tools");
        std::fs::create_dir_all(&bin).unwrap();
        let program = bin.join(if cfg!(windows) {
            "dsymutil.exe"
        } else {
            "dsymutil"
        });
        std::fs::write(&program, b"command fixture").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        InvocationEnv::from_parts(
            [("PATH", bin.into_os_string()), ("PATHEXT", ".EXE".into())],
            Some(root.to_owned()),
            None,
            root.to_owned(),
        )
    }

    #[test]
    fn current_returns_a_platform_named_after_the_host() {
        // Sanity: detection picks the right impl for this build target.
        let platform = current();
        let expected = if cfg!(target_os = "macos") {
            "macos"
        } else if cfg!(target_os = "linux") {
            "linux"
        } else if cfg!(target_os = "windows") {
            "windows"
        } else {
            // The cfg cascade in `current` covers exactly these three.
            // If this branch fires, `current` needs a new arm.
            panic!("unsupported host OS in test")
        };
        assert_eq!(platform.name(), expected);
    }

    #[test]
    fn only_macos_on_arm64_keeps_a_verified_loadable_memo() {
        assert_eq!(LinuxPlatform.verified_loadable_dir(), None);
        assert_eq!(WindowsPlatform.verified_loadable_dir(), None);
        let dir = MacOsPlatform::default().verified_loadable_dir();
        if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
            let dir = dir.expect("arm64 macOS checks signatures, so it keeps the memo");
            let scope = dir.file_name().unwrap().to_str().unwrap();
            assert!(scope.starts_with("darwin-") && scope.len() > "darwin-".len());
            assert_eq!(
                dir.parent().unwrap().file_name().unwrap(),
                "verified-loadables"
            );
        } else {
            assert_eq!(dir, None);
        }
    }

    #[test]
    fn the_memo_is_kept_per_kernel_release_on_arm64_macos_only() {
        let probes = Path::new("/cache/probes");
        let release = || Some("25.6.0".to_string());
        assert_eq!(
            macos_verified_loadable_dir("aarch64", "macos", release, probes),
            Some(probes.join("verified-loadables/darwin-25.6.0")),
        );
        for (arch, os) in [
            ("x86_64", "macos"),
            ("aarch64", "linux"),
            ("x86_64", "linux"),
        ] {
            assert_eq!(
                macos_verified_loadable_dir(arch, os, release, probes),
                None,
                "{arch}/{os} checks nothing, so it keeps no memo"
            );
        }
        assert_eq!(
            macos_verified_loadable_dir("aarch64", "macos", || None, probes),
            None,
            "without a kernel release an update could reuse old passes"
        );
    }

    #[cfg(unix)]
    #[test]
    fn kernel_release_reads_the_running_kernel() {
        let release = kernel_release().expect("uname works on unix");
        assert!(
            release.chars().next().unwrap().is_ascii_digit(),
            "{release}"
        );
    }

    #[test]
    fn linux_ensure_binary_loadable_is_noop_for_any_path() {
        // Documents the contract: Linux impl never errors and never
        // touches the file. Even nonexistent paths are fine because
        // the loader concern doesn't exist on this OS.
        let platform = LinuxPlatform;
        platform
            .ensure_binary_loadable(Path::new("/no/such/file"))
            .unwrap();
    }

    #[test]
    fn windows_ensure_binary_loadable_is_noop_for_any_path() {
        let platform = WindowsPlatform;
        platform
            .ensure_binary_loadable(Path::new("/no/such/file"))
            .unwrap();
    }

    #[test]
    fn macos_ensure_binary_loadable_does_not_propagate_errors() {
        // Two paths exercised by this single test depending on host:
        //
        // - Linux / Windows / x86_64 macOS: the impl bails on the host
        //   check and returns Ok without spawning anything.
        // - macOS arm64: the impl shells out to `codesign --verify`
        //   (which fails on a missing file) and then `codesign --sign`
        //   (which also fails); the contract is that both failures get
        //   logged and the function still returns Ok, so a single
        //   malformed input doesn't tank the wrapper's restore loop.
        let platform = MacOsPlatform::default();
        platform
            .ensure_binary_loadable(Path::new("/no/such/file"))
            .unwrap();
    }

    #[test]
    fn counting_platform_records_ensure_calls() {
        // Sanity for the test double itself; consumers in other tests
        // rely on `ensure_calls()` returning truthful counts.
        let platform = CountingPlatform::new();
        assert_eq!(platform.ensure_calls(), 0);
        platform.ensure_binary_loadable(Path::new("/x")).unwrap();
        platform.ensure_binary_loadable(Path::new("/y")).unwrap();
        assert_eq!(platform.ensure_calls(), 2);
    }

    #[test]
    fn only_linux_lets_restored_loadables_share_the_blob_inode() {
        assert!(LinuxPlatform.may_share_restored_loadables());
        // codesign may rewrite a restored file in place.
        assert!(!MacOsPlatform::default().may_share_restored_loadables());
        // An NTFS link shares the blob's read-only attribute (#429).
        assert!(!WindowsPlatform.may_share_restored_loadables());
    }

    // ── package_debug_bundle (kunobi-ninja/kache#319) ────────────────

    #[test]
    fn linux_package_debug_bundle_is_none_for_any_path() {
        // Linux DWARF is embedded — no companion to produce, ever.
        let platform = LinuxPlatform;
        let dir = tempfile::tempdir().unwrap();
        assert!(
            platform
                .package_debug_bundle(Path::new("/no/such/binary"), dir.path(), None)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn windows_package_debug_bundle_is_none_for_any_path() {
        let platform = WindowsPlatform;
        let dir = tempfile::tempdir().unwrap();
        assert!(
            platform
                .package_debug_bundle(Path::new("/no/such/binary"), dir.path(), None)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn macos_package_debug_bundle_does_not_propagate_errors() {
        // Off-macOS hosts: the runtime gate short-circuits to Ok(None)
        // without spawning anything. On a real macOS host: inspecting
        // the missing file fails, which the best-effort contract
        // turns into a logged warning + Ok(None) — a failed bundle must
        // never fail the store path.
        let platform = MacOsPlatform::default();
        let dir = tempfile::tempdir().unwrap();
        assert!(
            platform
                .package_debug_bundle(Path::new("/no/such/binary"), dir.path(), None)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn debug_bundle_command_is_prepared_for_every_macho_format() {
        let dir = tempfile::tempdir().unwrap();
        let binary = dir.path().join("demo");
        let bundle = dir.path().join("demo.dSYM");
        for magic in [
            b"\xce\xfa\xed\xfe",
            b"\xfe\xed\xfa\xce",
            b"\xcf\xfa\xed\xfe",
            b"\xfe\xed\xfa\xcf",
            b"\xca\xfe\xba\xbe",
            b"\xbe\xba\xfe\xca",
            b"\xca\xfe\xba\xbf",
            b"\xbf\xba\xfe\xca",
        ] {
            std::fs::write(&binary, magic).unwrap();
            let command = macho_debug_bundle_command_for(&binary, &bundle, &debug_env(dir.path()))
                .unwrap()
                .expect("Mach-O outputs need dsymutil");
            assert_eq!(
                Path::new(command.get_program()).file_stem().unwrap(),
                "dsymutil"
            );
        }
    }

    #[test]
    fn non_macho_outputs_never_prepare_a_dsymutil_command() {
        let dir = tempfile::tempdir().unwrap();
        let binary = dir.path().join("demo");
        let bundle = dir.path().join("demo.dSYM");
        // Exercise the same command-preparation seam as MacOsPlatform on
        // every host, without a fake dsymutil or a process-wide PATH edit.
        for bytes in [
            b"\x7fELF\x02\x01\x01\0".as_slice(),
            b"MZ\x90\0PE\0\0",
            b"\0asm\x01\0\0\0",
            b"#!/bin/sh\n",
            b"",
            b"\xcf\xfa\xed",
        ] {
            std::fs::write(&binary, bytes).unwrap();
            assert!(
                macho_debug_bundle_command_for(&binary, &bundle, &debug_env(dir.path()))
                    .unwrap()
                    .is_none(),
                "non-Mach-O output must not invoke dsymutil: {bytes:?}"
            );
        }
        assert!(!bundle.exists());
        std::fs::remove_file(&binary).unwrap();
        assert!(
            macho_debug_bundle_command_for(&binary, &bundle, &debug_env(dir.path()))
                .unwrap()
                .is_none(),
            "a missing output must keep debug packaging best-effort"
        );
    }

    /// Packed debug info, and an absent `-Csplit-debuginfo` (rustc's macOS
    /// default), must not build a dsymutil command when rustc already wrote
    /// the adjacent bundle. Unpacked still does: the objects are on disk.
    #[test]
    fn dsymutil_packed_or_absent_does_not_build_a_command_when_the_bundle_exists() {
        let dir = tempfile::tempdir().unwrap();
        let binary = dir.path().join("build_script_build");
        std::fs::write(&binary, b"\xcf\xfa\xed\xfe").unwrap();
        let bundle = dir.path().join("build_script_build.dSYM");
        std::fs::create_dir_all(bundle.join("Contents")).unwrap();
        std::fs::write(bundle.join("Contents/Info.plist"), b"plist").unwrap();
        let env = debug_env(dir.path());

        for split in [None, Some("packed")] {
            let plan = debug_bundle_spawn(&binary, &bundle, &env, split).unwrap();
            assert!(
                matches!(plan, DebugBundleSpawn::Reuse),
                "{split:?} must package the existing bundle and not build a dsymutil command"
            );
        }

        match debug_bundle_spawn(&binary, &bundle, &env, Some("unpacked")).unwrap() {
            DebugBundleSpawn::Run(command) => {
                assert_eq!(
                    Path::new(command.get_program()).file_stem().unwrap(),
                    "dsymutil",
                    "unpacked still builds a dsymutil command when a bundle is already present"
                );
            }
            DebugBundleSpawn::Skip | DebugBundleSpawn::Reuse => {
                panic!("unpacked still builds a dsymutil command when a bundle is already present");
            }
        }
    }

    /// No adjacent bundle and a packed or absent flag: do not point dsymutil
    /// at objects rustc already deleted.
    #[test]
    fn dsymutil_packed_or_absent_does_not_build_a_command_without_a_bundle() {
        let dir = tempfile::tempdir().unwrap();
        let binary = dir.path().join("build_script_build");
        std::fs::write(&binary, b"\xcf\xfa\xed\xfe").unwrap();
        let bundle = dir.path().join("build_script_build.dSYM");
        let env = debug_env(dir.path());
        assert!(!bundle.exists());

        for split in [None, Some("packed")] {
            let plan = debug_bundle_spawn(&binary, &bundle, &env, split).unwrap();
            assert!(
                matches!(plan, DebugBundleSpawn::Skip),
                "{split:?} must cache the binary without building a dsymutil command"
            );
        }

        assert!(
            matches!(
                debug_bundle_spawn(&binary, &bundle, &env, Some("unpacked")).unwrap(),
                DebugBundleSpawn::Run(_)
            ),
            "unpacked still builds a dsymutil command when no bundle exists yet"
        );
    }

    /// The spawn site packages rustc's bundle instead of running dsymutil.
    /// The fixture `dsymutil` exits 1, so a spawn returns `Ok(None)`.
    #[test]
    fn dsymutil_packed_bundle_is_packaged_without_spawning() {
        let dir = tempfile::tempdir().unwrap();
        let binary = dir.path().join("demo");
        std::fs::write(&binary, b"\xcf\xfa\xed\xfe").unwrap();
        let bundle = dir.path().join("demo.dSYM");
        std::fs::create_dir_all(bundle.join("Contents")).unwrap();
        std::fs::write(bundle.join("Contents/Info.plist"), b"plist").unwrap();
        let env = debug_env(dir.path());
        let dsymutil = dir.path().join("tools").join(if cfg!(windows) {
            "dsymutil.exe"
        } else {
            "dsymutil"
        });
        std::fs::write(&dsymutil, b"#!/bin/sh\nexit 1\n").unwrap();
        let staging = tempfile::tempdir().unwrap();

        for split in [None, Some("packed")] {
            let tar = package_macos_debug_bundle(&env, &binary, staging.path(), split, "macos")
                .unwrap()
                .unwrap_or_else(|| panic!("{split:?} must package the existing bundle"));
            assert_eq!(tar.file_name().unwrap(), "demo.dsym.tar");
            let bytes = std::fs::read(&tar).unwrap();
            let mut archive = tar::Archive::new(std::io::Cursor::new(bytes));
            let names: Vec<String> = archive
                .entries()
                .unwrap()
                .map(|entry| {
                    entry
                        .unwrap()
                        .path()
                        .unwrap()
                        .to_string_lossy()
                        .into_owned()
                })
                .collect();
            assert!(
                names.iter().any(|name| name == "Contents/Info.plist"),
                "{split:?} tar must contain the bundle rustc wrote, got {names:?}"
            );
        }

        assert!(
            package_macos_debug_bundle(&env, &binary, staging.path(), Some("unpacked"), "macos")
                .unwrap()
                .is_none(),
            "unpacked still spawns dsymutil; this fixture cannot run, so no bundle is packaged"
        );
        assert!(
            package_macos_debug_bundle(&env, &binary, staging.path(), None, "linux")
                .unwrap()
                .is_none(),
            "only a macOS host packages a debug bundle"
        );

        let via_platform = MacOsPlatform { env: env.clone() }
            .package_debug_bundle(&binary, staging.path(), None)
            .unwrap();
        assert_eq!(via_platform.is_some(), std::env::consts::OS == "macos");
    }

    #[test]
    fn counting_platform_records_package_calls() {
        let platform = CountingPlatform::new();
        assert_eq!(platform.package_calls(), 0);
        let dir = tempfile::tempdir().unwrap();
        assert!(
            platform
                .package_debug_bundle(Path::new("/x"), dir.path(), None)
                .unwrap()
                .is_none()
        );
        assert_eq!(platform.package_calls(), 1);
    }

    /// The tar builder is pure and runs on every host: a synthetic bundle
    /// tree must produce a complete, sorted, byte-deterministic archive —
    /// determinism is what lets two identical dSYMs dedupe to one
    /// content-addressed blob (kunobi-ninja/kache#319). Linux-runnable so
    /// the ubuntu mutation lane can kill mutants in the packaging path.
    #[test]
    fn deterministic_tar_captures_the_whole_tree_reproducibly() {
        let dir = tempfile::tempdir().unwrap();
        let bundle = dir.path().join("fake.dSYM");
        std::fs::create_dir_all(bundle.join("Contents/Resources/DWARF")).unwrap();
        std::fs::write(bundle.join("Contents/Info.plist"), b"plist").unwrap();
        std::fs::write(bundle.join("Contents/Resources/DWARF/fake"), b"dwarf").unwrap();

        let tar_a = dir.path().join("a.tar");
        let tar_b = dir.path().join("b.tar");
        build_deterministic_tar(&bundle, &tar_a).unwrap();
        build_deterministic_tar(&bundle, &tar_b).unwrap();

        let bytes_a = std::fs::read(&tar_a).unwrap();
        assert_eq!(
            bytes_a,
            std::fs::read(&tar_b).unwrap(),
            "two packagings of the same bundle must be byte-identical"
        );

        let mut archive = tar::Archive::new(std::io::Cursor::new(bytes_a));
        let entries: Vec<String> = archive
            .entries()
            .unwrap()
            .map(|e| e.unwrap().path().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            entries,
            vec![
                "Contents/Info.plist".to_string(),
                "Contents/Resources/DWARF/fake".to_string(),
            ],
            "every file, relative to the bundle root, in sorted order"
        );
    }

    #[test]
    fn debug_bundle_command_resolves_relative_arguments_before_changing_directory() {
        let root = tempfile::tempdir().unwrap();
        let env = debug_env(root.path());
        let binary = Path::new("target/debug/deps/demo");
        let bundle = Path::new("target/debug/deps/demo.dSYM");
        let command = debug_bundle_command_for(binary, bundle, &env).unwrap();
        let cwd = root.path();
        assert_eq!(
            command.get_current_dir(),
            Some(cwd.join("target/debug").as_path()),
            "dsymutil runs from the profile dir the OSO prefix stripped"
        );
        let args = command.get_args().collect::<Vec<_>>();
        assert_eq!(args.len(), 3);
        // Windows absolute() normalizes separators; compare paths, not argv bytes.
        assert_eq!(Path::new(args[0]), cwd.join(binary));
        assert_eq!(args[1], "-o");
        assert_eq!(Path::new(args[2]), cwd.join(bundle));
    }

    #[test]
    fn macos_debug_bundle_contains_symbols_with_output_relative_oso_paths() {
        if std::env::consts::OS != "macos" {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let dir = dir.path().canonicalize().unwrap();
        let Some(binary) = compile_debug_c_binary(&dir) else {
            return;
        };
        // Relink with the same OSO prefix kache injects for Rust executables.
        let prefix = format!("-Wl,-oso_prefix,{}/", dir.display());
        if relink_c_binary(&dir.join("hello.o"), &prefix, &binary).is_none() {
            return;
        }
        let staging = tempfile::tempdir().unwrap();
        MacOsPlatform::default()
            .package_debug_bundle(&binary, staging.path(), Some("unpacked"))
            .unwrap()
            .unwrap();
        let dwarf = dir.join("hello-bin.dSYM/Contents/Resources/DWARF/hello-bin");
        let dump = Command::new("dwarfdump")
            .arg("--debug-info")
            .arg(dwarf)
            .output()
            .unwrap();
        assert!(dump.status.success());
        let info = String::from_utf8_lossy(&dump.stdout);
        assert!(
            info.contains("hello.c"),
            "bundle must contain the source compile unit: {info}"
        );
        assert!(
            info.contains("DW_TAG_subprogram"),
            "bundle must contain function debug info: {info}"
        );
    }

    #[test]
    fn debug_bundle_command_runs_outside_cargo_layout_from_the_output_dir() {
        let root = tempfile::tempdir().unwrap();
        let env = debug_env(root.path());
        let command =
            debug_bundle_command_for(Path::new("out/demo"), Path::new("out/demo.dSYM"), &env)
                .unwrap();
        let cwd = root.path();
        assert_eq!(command.get_current_dir(), Some(cwd.join("out").as_path()));
    }

    /// kunobi-ninja/kache#1161: a Cargo test binary lives in `<profile>/deps`
    /// and is linked with `-oso_prefix <profile>/`, so its debug map says
    /// `deps/<obj>.o`. dsymutil must find those objects without warnings.
    #[test]
    fn macos_debug_bundle_resolves_profile_relative_oso_paths_for_deps_binaries() {
        if std::env::consts::OS != "macos" {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let profile = dir.path().canonicalize().unwrap().join("debug");
        let deps = profile.join("deps");
        std::fs::create_dir_all(&deps).unwrap();
        let Some(binary) = compile_debug_c_binary(&deps) else {
            return;
        };
        let prefix = format!("-Wl,-oso_prefix,{}/", profile.display());
        if relink_c_binary(&deps.join("hello.o"), &prefix, &binary).is_none() {
            return;
        }
        let bundle = deps.join("hello-bin.dSYM");
        let output = debug_bundle_command_for(&binary, &bundle, &InvocationEnv::capture())
            .unwrap()
            .output()
            .unwrap();
        assert!(output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            !stderr.contains("unable to open object file"),
            "dsymutil must resolve deps/ OSO paths: {stderr}"
        );
        let dump = Command::new("dwarfdump")
            .arg("--debug-info")
            .arg(bundle.join("Contents/Resources/DWARF/hello-bin"))
            .output()
            .unwrap();
        assert!(String::from_utf8_lossy(&dump.stdout).contains("DW_TAG_subprogram"));
    }

    /// Compile a tiny real `-g` binary with the host's C compiler (fast:
    /// three lines of C) so the macOS leg exercises real `dsymutil` output.
    /// Returns None when the host can't run this leg: not macOS, or the
    /// compiler would not start or failed (see [`host_tool_unavailable`]).
    fn compile_debug_c_binary(dir: &Path) -> Option<std::path::PathBuf> {
        let cc = host_c_compiler()?;
        let source = dir.join("hello.c");
        std::fs::write(
            &source,
            "#include <stdio.h>\nint main(void) { printf(\"hi\\n\"); return 0; }\n",
        )
        .unwrap();
        // Compile and link SEPARATELY so the `.o` the binary's N_OSO debug
        // map points at persists in `dir` — the exact shape rustc's default
        // macOS `-g` output has, and what dsymutil consumes. A one-step
        // `cc -g` deletes its temp `.o`, leaving dsymutil nothing to bake.
        let object = dir.join("hello.o");
        let binary = dir.join("hello-bin");
        let compiled = run_host_tool(|| {
            let mut command = cc.command();
            command
                .args(["-g", "-c"])
                .arg(&source)
                .arg("-o")
                .arg(&object);
            command
        });
        if let Err(error) = compiled {
            return host_tool_unavailable("compiling hello.c", &error);
        }
        relink_c_binary(&object, "", &binary)
    }

    /// Link `object` into `binary` with the host's C compiler, adding
    /// `extra` (such as an `-oso_prefix`) when it is not empty.
    fn relink_c_binary(object: &Path, extra: &str, binary: &Path) -> Option<std::path::PathBuf> {
        let cc = host_c_compiler()?;
        let linked = run_host_tool(|| {
            let mut command = cc.command();
            command.arg(object);
            if !extra.is_empty() {
                command.arg(extra);
            }
            command.arg("-o").arg(binary);
            command
        });
        match linked {
            Ok(_) => Some(binary.to_path_buf()),
            Err(error) => host_tool_unavailable("linking hello-bin", &error),
        }
    }

    /// The host's real C compiler and the SDK it compiles against, found
    /// once per test process through `xcrun`. Resolving them that way keeps
    /// a `cc` shim on `PATH` (a kache install, mise) out of these tests, and
    /// asks `xcrun` once instead of on every compile
    /// (kunobi-ninja/kache#1208). The toolchain's `clang` does not know the
    /// SDK on its own (`/usr/bin/cc` is what supplies it), so every compile
    /// passes `-isysroot`. None off macOS.
    fn host_c_compiler() -> Option<&'static HostCompiler> {
        static COMPILER: std::sync::OnceLock<Option<HostCompiler>> = std::sync::OnceLock::new();
        COMPILER
            .get_or_init(|| {
                if std::env::consts::OS != "macos" {
                    return None;
                }
                let clang = xcrun_path(&["--find", "clang"])?;
                let sdk = xcrun_path(&["--show-sdk-path"])?;
                Some(HostCompiler { clang, sdk })
            })
            .as_ref()
    }

    /// A compiler [`host_c_compiler`] found.
    struct HostCompiler {
        clang: std::path::PathBuf,
        sdk: std::path::PathBuf,
    }

    impl HostCompiler {
        /// `clang -isysroot <sdk>`, ready for arguments.
        fn command(&self) -> Command {
            let mut command = Command::new(&self.clang);
            command.arg("-isysroot").arg(&self.sdk);
            command
        }
    }

    /// The path `xcrun <args>` prints, when it names something that exists.
    fn xcrun_path(args: &[&str]) -> Option<std::path::PathBuf> {
        let what = format!("xcrun {}", args.join(" "));
        match run_host_tool(|| {
            let mut command = Command::new("xcrun");
            command.args(args);
            command
        }) {
            Ok(output) => {
                let path = std::path::PathBuf::from(String::from_utf8_lossy(&output.stdout).trim());
                if path.exists() {
                    Some(path)
                } else {
                    host_tool_unavailable(&what, &format!("{} does not exist", path.display()))
                }
            }
            Err(error) => host_tool_unavailable(&what, &error),
        }
    }

    /// How many times a host tool is started before giving up. A full
    /// parallel `cargo test` run on macOS sometimes fails to start or run
    /// the toolchain once, then succeeds (kunobi-ninja/kache#1208).
    const HOST_TOOL_ATTEMPTS: u32 = 3;

    /// Run the command `make` builds until it succeeds or
    /// [`HOST_TOOL_ATTEMPTS`] runs out. The error names the last failure,
    /// with the tool's stderr.
    fn run_host_tool(mut make: impl FnMut() -> Command) -> Result<std::process::Output, String> {
        let mut last = String::new();
        for attempt in 0..HOST_TOOL_ATTEMPTS {
            if attempt > 0 {
                std::thread::sleep(std::time::Duration::from_millis(250 << attempt));
            }
            match make().output() {
                Ok(output) if output.status.success() => return Ok(output),
                Ok(output) => {
                    last = format!(
                        "{}: {}",
                        output.status,
                        String::from_utf8_lossy(&output.stderr).trim()
                    )
                }
                Err(error) => last = error.to_string(),
            }
        }
        Err(last)
    }

    /// A host tool these tests need did not work. Locally that skips the
    /// test; in CI (`CI` set) it fails, so the macOS lane keeps real
    /// coverage instead of passing on a broken toolchain.
    fn host_tool_unavailable<T>(what: &str, error: &str) -> Option<T> {
        if std::env::var_os("CI").is_some() {
            panic!("{what} failed after {HOST_TOOL_ATTEMPTS} attempts: {error}");
        }
        eprintln!("skipping: {what} failed after {HOST_TOOL_ATTEMPTS} attempts: {error}");
        None
    }

    #[test]
    fn macos_package_debug_bundle_produces_tar_with_dwarf_and_adjacent_bundle() {
        // Runtime-gated real-tool leg: skipped (compile_debug_c_binary →
        // None) everywhere but a macOS host with a working `cc`.
        let dir = tempfile::tempdir().unwrap();
        let Some(binary) = compile_debug_c_binary(dir.path()) else {
            return;
        };
        let staging = tempfile::tempdir().unwrap();

        let tar_path = MacOsPlatform::default()
            .package_debug_bundle(&binary, staging.path(), Some("unpacked"))
            .unwrap()
            .expect("macOS host with dsymutil must produce a bundle tar");
        assert_eq!(
            tar_path.file_name().unwrap().to_str().unwrap(),
            "hello-bin.dsym.tar"
        );

        // The bundle itself stays next to the binary (cold/warm parity —
        // see MacOsPlatform::package_debug_bundle).
        let bundle = dir.path().join("hello-bin.dSYM");
        assert!(
            bundle.join("Contents/Resources/DWARF/hello-bin").is_file(),
            "dSYM bundle must remain adjacent to the binary"
        );

        // The tar holds the bundle contents relative to the bundle root —
        // the layout `unpack_debug_bundle` re-creates on restore.
        let mut archive = tar::Archive::new(std::fs::File::open(&tar_path).unwrap());
        let names: Vec<String> = archive
            .entries()
            .unwrap()
            .map(|e| e.unwrap().path().unwrap().to_string_lossy().into_owned())
            .collect();
        assert!(
            names
                .iter()
                .any(|n| n == "Contents/Resources/DWARF/hello-bin"),
            "tar must contain the DWARF payload, got: {names:?}"
        );
    }

    #[test]
    fn macos_package_debug_bundle_tar_bytes_are_reproducible() {
        // Two packagings of the same binary must produce identical tar
        // bytes so identical dSYMs dedupe to one content-addressed blob.
        let dir = tempfile::tempdir().unwrap();
        let Some(binary) = compile_debug_c_binary(dir.path()) else {
            return;
        };
        let staging_a = tempfile::tempdir().unwrap();
        let staging_b = tempfile::tempdir().unwrap();

        let tar_a = MacOsPlatform::default()
            .package_debug_bundle(&binary, staging_a.path(), Some("unpacked"))
            .unwrap()
            .expect("first packaging must succeed");
        let tar_b = MacOsPlatform::default()
            .package_debug_bundle(&binary, staging_b.path(), Some("unpacked"))
            .unwrap()
            .expect("second packaging must succeed");

        assert_eq!(
            std::fs::read(&tar_a).unwrap(),
            std::fs::read(&tar_b).unwrap(),
            "debug bundle tar bytes must be reproducible for store dedup"
        );
    }
}
