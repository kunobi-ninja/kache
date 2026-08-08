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
//! binary loses source-level debugging elsewhere. At store time —
//! while the `.o`s still exist — the macOS impl bakes a self-contained
//! `.dSYM` via `dsymutil` and hands the wrapper a single flat tar of
//! it for the cache entry. lldb prefers an adjacent UUID-matched
//! `.dSYM` over the debug map, so no binary mutation is needed.
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

use anyhow::{Context as _, Result};
use std::path::{Path, PathBuf};
use std::process::Command;

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
    fn ensure_binary_loadable(&self, path: &Path) -> Result<()>;

    /// Produce a self-contained, relocatable debug-info companion for a
    /// just-linked binary, as ONE flat file inside `staging_dir`
    /// (kunobi-ninja/kache#319).
    ///
    /// Called on the store path while the binary's build-local debug
    /// inputs (macOS: the `.o` files its `N_OSO` debug map points at)
    /// still exist. Returns `Ok(None)` when the platform has no such
    /// companion (Linux embeds DWARF; Windows `.pdb` is untouched for
    /// now) or when the packaging tool is unavailable/fails — the
    /// best-effort failure contract of this trait: a missing debug
    /// bundle degrades debuggability of the cached binary, never the
    /// build.
    ///
    /// The macOS impl also leaves the `.dSYM` bundle itself next to the
    /// binary (see [`MacOsPlatform`]), so the producing build and a
    /// restoring build converge on the same on-disk shape.
    fn package_debug_bundle(&self, binary: &Path, staging_dir: &Path) -> Result<Option<PathBuf>>;
}

/// Detect the current host platform.
///
/// Returns a boxed trait object so callers don't carry a generic
/// `Platform` parameter through every type. The dispatch cost is
/// negligible relative to the work each method does (codesign shells
/// out; the vtable lookup is in the noise).
pub fn current() -> Box<dyn Platform> {
    #[cfg(target_os = "macos")]
    {
        Box::new(MacOsPlatform)
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
/// `#[allow(dead_code)]` for the same reason as [`LinuxPlatform`] —
/// on a Linux or Windows build, `current()` doesn't construct it but
/// cross-platform unit tests do, and the symmetric availability lets
/// any future test pin the macOS dispatch shape from any host.
#[allow(dead_code)]
pub struct MacOsPlatform;

impl Platform for MacOsPlatform {
    fn name(&self) -> &'static str {
        "macos"
    }

    fn ensure_binary_loadable(&self, path: &Path) -> Result<()> {
        // Compiled in on every host so unit tests can construct
        // MacOsPlatform from Linux. The actual `codesign` invocation
        // is gated below — a Linux test that calls into this method
        // gets Ok(()) because the host check fails, no `codesign`
        // process is spawned.
        if std::env::consts::ARCH != "aarch64" {
            return Ok(());
        }
        if std::env::consts::OS != "macos" {
            return Ok(());
        }

        // verify-then-sign: skip mutation when ld64's signature is
        // still valid. `codesign --verify --strict` exits 0 iff a
        // structurally-valid signature is already present.
        let verify = match Command::new("codesign")
            .args(["--verify", "--strict"])
            .arg(path)
            .status()
        {
            Ok(status) => status,
            Err(err) => {
                tracing::warn!(
                    "unable to run codesign --verify for {}: {err}",
                    path.display()
                );
                return Ok(());
            }
        };

        if verify.success() {
            tracing::debug!(
                "ad-hoc signature already valid for {}, skipping re-sign",
                path.display()
            );
            return Ok(());
        }

        tracing::debug!(
            "ad-hoc signature missing or invalid for {}, re-applying",
            path.display()
        );
        let status = match Command::new("codesign")
            .args(["--sign", "-", "--force"])
            .arg(path)
            .status()
        {
            Ok(status) => status,
            Err(err) => {
                tracing::warn!(
                    "unable to run codesign --sign for {}: {err}",
                    path.display()
                );
                return Ok(());
            }
        };

        if !status.success() {
            tracing::warn!("ad-hoc codesign failed for {}", path.display());
        }
        Ok(())
    }

    fn package_debug_bundle(&self, binary: &Path, staging_dir: &Path) -> Result<Option<PathBuf>> {
        // Compiled on every host (same convention as ensure_binary_loadable):
        // the actual `dsymutil` spawn is runtime-gated so a Linux test can
        // construct MacOsPlatform and exercise this method as a no-op.
        if std::env::consts::OS != "macos" {
            return Ok(None);
        }
        let Some(file_name) = binary.file_name().and_then(|n| n.to_str()) else {
            tracing::warn!(
                "not packaging a debug bundle: binary has no usable file name: {}",
                binary.display()
            );
            return Ok(None);
        };

        // Bake the `.dSYM` NEXT TO the binary, not in the staging dir. Two
        // reasons (kunobi-ninja/kache#319):
        //   1. cold/warm parity — a restoring build unpacks the bundle next
        //      to the binary, so the producing build should end up with the
        //      same on-disk shape (and gets a usable dSYM for its own lldb
        //      sessions out of the link work it already paid for);
        //   2. lldb's adjacent-bundle lookup is by `<binary>.dSYM` sibling
        //      path, which is exactly this location.
        let bundle_dir = binary.with_file_name(format!("{file_name}.dSYM"));
        let status = match Command::new("dsymutil")
            .arg(binary)
            .arg("-o")
            .arg(&bundle_dir)
            .status()
        {
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

        // Tar the bundle into the ONE flat file the store can hold
        // (single-component artifact names, file-level hashing/linking).
        let tar_path = staging_dir.join(format!("{file_name}.dsym.tar"));
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
}

/// Tar `bundle_dir`'s contents (paths relative to the bundle root, e.g.
/// `Contents/Resources/DWARF/<name>`) into `tar_path`, byte-reproducibly:
/// entries are file-only, sorted by path, with mtime/uid/gid pinned to 0 and
/// fixed modes. Reproducible tar bytes mean two identical dSYMs dedupe to one
/// content-addressed store blob (kunobi-ninja/kache#319).
// Reachable only through `MacOsPlatform::package_debug_bundle`; on other
// hosts nothing outside the tests constructs `MacOsPlatform`, so the
// compile-everywhere/test-anywhere convention needs the helpers exempted
// from dead-code there.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn build_deterministic_tar(bundle_dir: &Path, tar_path: &Path) -> Result<()> {
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
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
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
/// `#[allow(dead_code)]` because cross-platform unit tests construct
/// `LinuxPlatform` from a macOS host (and vice versa) to exercise the
/// dispatch shape without spawning real `codesign` / `signtool`. On a
/// non-Linux production build, no caller constructs it — but having
/// the struct compile keeps the test surface symmetric.
#[allow(dead_code)]
pub struct LinuxPlatform;

impl Platform for LinuxPlatform {
    fn name(&self) -> &'static str {
        "linux"
    }

    fn ensure_binary_loadable(&self, _path: &Path) -> Result<()> {
        Ok(())
    }

    fn package_debug_bundle(&self, _binary: &Path, _staging_dir: &Path) -> Result<Option<PathBuf>> {
        // Linux embeds DWARF in the binary under default `-Cdebuginfo`
        // settings, so a restored executable is already self-contained
        // (kunobi-ninja/kache#319) — nothing to package.
        Ok(None)
    }
}

/// Windows implementation. Authenticode signing is not enforced for
/// load-time loading of unsigned PE binaries (only for kernel-mode
/// drivers and SmartScreen), so [`Platform::ensure_binary_loadable`]
/// is a no-op. When PE/PDB-specific handling lands, it goes here.
///
/// See [`LinuxPlatform`] for the `#[allow(dead_code)]` rationale.
#[allow(dead_code)]
pub struct WindowsPlatform;

impl Platform for WindowsPlatform {
    fn name(&self) -> &'static str {
        "windows"
    }

    fn ensure_binary_loadable(&self, _path: &Path) -> Result<()> {
        Ok(())
    }

    fn package_debug_bundle(&self, _binary: &Path, _staging_dir: &Path) -> Result<Option<PathBuf>> {
        // The PE/PDB analogue of kunobi-ninja/kache#319 (an `.exe`
        // references its `.pdb` by recorded path) is a separate
        // investigation; until it lands there is nothing to package.
        Ok(None)
    }
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
    }

    impl CountingPlatform {
        pub fn new() -> Self {
            Self {
                ensure_binary_loadable_calls: AtomicUsize::new(0),
                package_debug_bundle_calls: AtomicUsize::new(0),
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
        fn ensure_binary_loadable(&self, _path: &Path) -> Result<()> {
            self.ensure_binary_loadable_calls
                .fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        fn package_debug_bundle(
            &self,
            _binary: &Path,
            _staging_dir: &Path,
        ) -> Result<Option<PathBuf>> {
            self.package_debug_bundle_calls
                .fetch_add(1, Ordering::Relaxed);
            Ok(None)
        }
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
        let platform = MacOsPlatform;
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

    // ── package_debug_bundle (kunobi-ninja/kache#319) ────────────────

    #[test]
    fn linux_package_debug_bundle_is_none_for_any_path() {
        // Linux DWARF is embedded — no companion to produce, ever.
        let platform = LinuxPlatform;
        let dir = tempfile::tempdir().unwrap();
        assert!(
            platform
                .package_debug_bundle(Path::new("/no/such/binary"), dir.path())
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
                .package_debug_bundle(Path::new("/no/such/binary"), dir.path())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn macos_package_debug_bundle_does_not_propagate_errors() {
        // Off-macOS hosts: the runtime gate short-circuits to Ok(None)
        // without spawning anything. On a real macOS host: `dsymutil`
        // fails on the missing file, which the best-effort contract
        // turns into a logged warning + Ok(None) — a failed bundle must
        // never fail the store path.
        let platform = MacOsPlatform;
        let dir = tempfile::tempdir().unwrap();
        assert!(
            platform
                .package_debug_bundle(Path::new("/no/such/binary"), dir.path())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn counting_platform_records_package_calls() {
        let platform = CountingPlatform::new();
        assert_eq!(platform.package_calls(), 0);
        let dir = tempfile::tempdir().unwrap();
        assert!(
            platform
                .package_debug_bundle(Path::new("/x"), dir.path())
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

    /// Compile a tiny real `-g` binary with the system `cc` (fast: three
    /// lines of C) so the macOS leg exercises real `dsymutil` output.
    /// Returns None when the host can't run this leg (non-macOS, no cc).
    fn compile_debug_c_binary(dir: &Path) -> Option<std::path::PathBuf> {
        if std::env::consts::OS != "macos" {
            return None;
        }
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
        let compile = Command::new("cc")
            .args(["-g", "-c"])
            .arg(&source)
            .arg("-o")
            .arg(&object)
            .status()
            .ok()?;
        if !compile.success() {
            return None;
        }
        let link = Command::new("cc")
            .arg(&object)
            .arg("-o")
            .arg(&binary)
            .status()
            .ok()?;
        link.success().then_some(binary)
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

        let tar_path = MacOsPlatform
            .package_debug_bundle(&binary, staging.path())
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

        let tar_a = MacOsPlatform
            .package_debug_bundle(&binary, staging_a.path())
            .unwrap()
            .expect("first packaging must succeed");
        let tar_b = MacOsPlatform
            .package_debug_bundle(&binary, staging_b.path())
            .unwrap()
            .expect("second packaging must succeed");

        assert_eq!(
            std::fs::read(&tar_a).unwrap(),
            std::fs::read(&tar_b).unwrap(),
            "debug bundle tar bytes must be reproducible for store dedup"
        );
    }
}
