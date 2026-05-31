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
//! ## Future methods
//!
//! New trait methods land when their callers exist (no speculative
//! interface bloat). Concrete cases on the roadmap:
//!
//! - `source_date_epoch() -> Option<u64>` — for the C/C++ preprocessor
//!   cache key. Honored by gcc + clang; neutralizes `__DATE__` /
//!   `__TIME__` macros.
//! - `rewrite_debug_paths(path)` — to fix Mach-O OSO records on macOS
//!   (kache-fork bug e3f64ec). Likely lands alongside the
//!   `PathNormalizer` work where the compile-time vs post-restore
//!   choice becomes well-defined.
//! - `probe_compiler(path) -> CompilerInfo` — for cc cache keys to
//!   know gcc-vs-clang-vs-MSVC.

use anyhow::Result;
use std::path::Path;
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
    }

    impl CountingPlatform {
        pub fn new() -> Self {
            Self {
                ensure_binary_loadable_calls: AtomicUsize::new(0),
            }
        }

        pub fn ensure_calls(&self) -> usize {
            self.ensure_binary_loadable_calls.load(Ordering::Relaxed)
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
}
