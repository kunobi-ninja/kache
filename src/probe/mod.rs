//! Compiler probe memoization.
//!
//! A "probe" is the act of asking a compiler about itself — today,
//! running `<cc> --version` to capture its version-stamped identity
//! line for the cache key. A probe's result depends only on the
//! compiler *binary*, so it is identical for every translation unit in
//! a build.
//!
//! kache runs as a fresh process per compile line (`CC=kache cc ...`),
//! so without memoization a 2000-file build would fork `cc --version`
//! 2000 times for 2000 identical answers. This module turns that into
//! one probe per build: the first process to need a compiler's config
//! runs the probe and writes a content-addressed record under the
//! cache dir; every later process reads that record instead.
//!
//! ## Why a file, not a daemon round-trip
//!
//! The record is a small JSON file. After the first write the kernel
//! page cache holds it in RAM, so every subsequent read is a
//! RAM-speed `read()` with no IPC and no dependency on the daemon
//! being alive. A regular file *is* the shared-memory area across the
//! build's processes — the kernel deduplicates it.
//!
//! ## Correctness
//!
//! A record is bound to the exact compiler binary via a `stat`
//! fingerprint (path + size + mtime, plus ctime + inode on Unix). Any
//! compiler change — an upgrade, or even a mtime-preserving `cp -p`
//! swap, which still bumps ctime — changes the key, so a stale record
//! is simply never looked up. [`ResolvedConfig::schema_version`] guards
//! against a record written by a different kache version being
//! mis-read.
//!
//! A probe-cache fault is never a compile fault: if the cache cannot be
//! keyed, read, or written, [`probe`] just runs the probe directly.
//!
//! ## Plugin seam
//!
//! [`Prober`] is the extension point. [`CcProber`] handles the
//! C-family compilers today; a `RustcProber`, or compiler-specific
//! probers that also capture the resolved `cc -###` invocation, slot
//! in behind the same trait without touching callers.

mod cache;
mod resolve;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::process::Command;

/// Schema version of a [`ResolvedConfig`] record. Bump whenever the
/// struct's shape or the probe logic changes in a way that would make
/// an old on-disk record wrong: a mismatch turns the record into a
/// cache miss (re-probe), never a wrong hit.
pub const PROBE_SCHEMA_VERSION: u32 = 4;

/// The memoized result of probing a compiler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedConfig {
    /// Schema of this record — see [`PROBE_SCHEMA_VERSION`].
    pub schema_version: u32,
    /// Id of the [`Prober`] that produced this record (`"cc"`). Lets
    /// one cache dir hold records from multiple probers safely.
    pub prober: String,
    /// `file_name` of the compiler executable, e.g. `clang`.
    pub compiler_name: String,
    /// First line of `<cc> --version` — the version-stamped identity
    /// string. gcc, clang and Apple clang each emit a distinct line.
    pub version_line: String,
    /// Codegen-semantic tokens of the resolved `cc -###` invocation —
    /// the driver's fully-expanded `-cc1` line with host-local paths
    /// sentinelled (see [`resolve`]). `None` when `-###` produced no
    /// resolvable compile line.
    pub resolved_tokens: Option<Vec<String>>,
}

/// What to probe.
pub struct ProbeRequest<'a> {
    /// The compiler as named on the command line: `cc`, `clang-17`, or
    /// a path like `/usr/bin/gcc`.
    pub compiler: &'a str,
    /// Full compile arguments. `cc -###` is run with these so the
    /// driver resolves exactly what the real compile would.
    pub args: &'a [String],
    /// The configuration-identifying subset of `args` — per-TU noise
    /// (source files, `-o`, dep-file flags) removed. The probe cache
    /// is keyed on this, so every TU of a build that shares a flag set
    /// shares one resolved-invocation record.
    pub key_args: &'a [String],
    /// Per-TU path strings (this invocation's source, output, dep-file
    /// paths) to blank out of the resolved tokens. Because the record is
    /// SHARED across the build's TUs (keyed by `key_args`), a per-TU path
    /// left in the tokens would make the record TU-specific — and under
    /// `make -j` the TUs race over whose paths the first-probing TU stored,
    /// corrupting other TUs' cache keys. Blanking them keeps the record
    /// invariant. Empty for callers that have no per-TU paths to hide.
    pub per_tu_paths: &'a [String],
    /// Whether the resolved-invocation path sentinel should recognise
    /// absolute Windows paths (drive / UNC). True for gnu/clang (their
    /// objects are remapped via `-ffile-prefix-map`, so blanking host
    /// paths in the key is portable); **false for clang-cl**, whose
    /// objects keep raw native paths, so its key stays path-literal /
    /// machine-local (#299/#312). POSIX `/…` is always sentinelled.
    pub windows_aware: bool,
}

/// A compiler-family-specific probe strategy — the plugin seam.
pub trait Prober {
    /// Short, stable identifier, stored in the record and mixed into
    /// the cache key so different probers never collide.
    fn id(&self) -> &'static str;

    /// Run the probe. This forks the compiler; [`probe`] calls it at
    /// most once per compiler binary per build.
    fn probe(&self, req: &ProbeRequest<'_>) -> Result<ResolvedConfig>;
}

/// Prober for the C-family compilers (`cc`, `gcc`, `clang`, …).
pub struct CcProber;

impl Prober for CcProber {
    fn id(&self) -> &'static str {
        "cc"
    }

    fn probe(&self, req: &ProbeRequest<'_>) -> Result<ResolvedConfig> {
        // Compiler identity — `cc --version`.
        let output = Command::new(req.compiler)
            .arg("--version")
            .output()
            .with_context(|| format!("running `{} --version`", req.compiler))?;
        if !output.status.success() {
            anyhow::bail!("`{} --version` exited {}", req.compiler, output.status);
        }
        let version_line = String::from_utf8_lossy(&output.stdout)
            .lines()
            .next()
            .unwrap_or("unknown")
            .to_string();
        let compiler_name = Path::new(req.compiler)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(req.compiler)
            .to_string();

        Ok(ResolvedConfig {
            schema_version: PROBE_SCHEMA_VERSION,
            prober: self.id().to_string(),
            compiler_name,
            version_line,
            resolved_tokens: resolve_invocation(
                req.compiler,
                req.args,
                req.windows_aware,
                req.per_tu_paths,
            ),
        })
    }
}

/// Run `cc -### <args>` and reduce the resolved `-cc1` invocation to
/// its codegen-semantic token list.
///
/// `-###` prints the fully-resolved command lines to stderr without
/// compiling. Returns `None` on any failure — a missing compiler, a
/// non-zero exit (bad flags), or output with no `-cc1` line. The probe
/// degrades to "no resolved invocation"; it never turns a `-###`
/// hiccup into a hard error.
fn resolve_invocation(
    compiler: &str,
    args: &[String],
    windows_aware: bool,
    per_tu_paths: &[String],
) -> Option<Vec<String>> {
    let output = Command::new(compiler)
        .arg("-###")
        .args(args)
        .output()
        .ok()?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    resolve::resolved_semantic_tokens(&stderr, windows_aware, per_tu_paths)
}

/// Probe a compiler, memoized through an on-disk cache under
/// `cache_dir`.
///
/// The first call for a given compiler binary runs `prober` and writes
/// a content-addressed record; later calls — this process or any
/// other — read the record. Resilient: if the cache cannot be keyed or
/// read the probe simply runs directly. A probe-cache fault never
/// fails a compile.
pub fn probe(
    cache_dir: &Path,
    prober: &dyn Prober,
    req: &ProbeRequest<'_>,
) -> Result<ResolvedConfig> {
    let key = cache::probe_key(prober.id(), req);

    if let Some(key) = &key
        && let Some(hit) = cache::load(cache_dir, key)
    {
        return Ok(hit);
    }

    // Miss, or the probe could not be keyed: run the real probe.
    crate::opcounts::record_probe_run();
    let config = prober.probe(req)?;

    if let Some(key) = &key {
        cache::store(cache_dir, key, &config);
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::{NamedTempFile, TempDir};

    /// A `Prober` that records how many times it actually ran — lets a
    /// test prove memoization without forking a real compiler.
    #[derive(Default)]
    struct CountingProber {
        runs: AtomicUsize,
    }

    impl Prober for CountingProber {
        fn id(&self) -> &'static str {
            "test"
        }

        fn probe(&self, _req: &ProbeRequest<'_>) -> Result<ResolvedConfig> {
            self.runs.fetch_add(1, Ordering::SeqCst);
            Ok(ResolvedConfig {
                schema_version: PROBE_SCHEMA_VERSION,
                prober: "test".to_string(),
                compiler_name: "fake".to_string(),
                version_line: "fake 1.0".to_string(),
                resolved_tokens: None,
            })
        }
    }

    /// A `ProbeRequest` for a compiler with no extra arguments.
    fn req(compiler: &str) -> ProbeRequest<'_> {
        ProbeRequest {
            compiler,
            args: &[],
            key_args: &[],
            per_tu_paths: &[],
            windows_aware: true,
        }
    }

    #[test]
    fn probe_runs_prober_once_then_serves_from_cache() {
        let cache = TempDir::new().unwrap();
        // A real, stat-able file stands in for the compiler binary —
        // the CountingProber never actually execs it.
        let compiler = NamedTempFile::new().unwrap();
        let prober = CountingProber::default();
        let req = req(compiler.path().to_str().unwrap());

        let first = probe(cache.path(), &prober, &req).unwrap();
        let second = probe(cache.path(), &prober, &req).unwrap();

        assert_eq!(first, second, "memoized result must match the original");
        assert_eq!(
            prober.runs.load(Ordering::SeqCst),
            1,
            "second probe must be served from the on-disk cache"
        );
    }

    #[test]
    fn probe_falls_back_to_running_when_compiler_is_unresolvable() {
        // A path that doesn't exist cannot be keyed, so every call
        // re-probes — but each call still succeeds. Correctness is
        // never sacrificed for memoization.
        let cache = TempDir::new().unwrap();
        let prober = CountingProber::default();
        let req = req("/nonexistent/kache-probe-test-cc");

        let _ = probe(cache.path(), &prober, &req).unwrap();
        let _ = probe(cache.path(), &prober, &req).unwrap();

        assert_eq!(
            prober.runs.load(Ordering::SeqCst),
            2,
            "an unkeyable probe is not memoized — both calls run"
        );
    }

    #[test]
    fn cc_prober_has_stable_id() {
        assert_eq!(CcProber.id(), "cc");
    }

    #[test]
    fn cc_prober_reads_a_real_compiler_version() {
        // Forks `cc --version`. Every dev box and CI runner that builds
        // kache has a C compiler; if `cc` is somehow absent, skip
        // rather than fail.
        let Ok(config) = CcProber.probe(&req("cc")) else {
            return;
        };
        assert!(
            !config.version_line.is_empty(),
            "version line should be populated"
        );
        assert_eq!(config.prober, "cc");
        assert_eq!(config.schema_version, PROBE_SCHEMA_VERSION);
    }

    #[test]
    fn cc_prober_resolves_the_invocation_with_flags() {
        // Forks `cc -### -O2 -x c -c <file>`. On clang this resolves a
        // `-cc1` line; on gcc the resolved-line shape differs and
        // `resolved_tokens` is `None` until the gcc prober lands — so
        // the token assertion only runs when resolution succeeded.
        let src = NamedTempFile::new().unwrap();
        let args: Vec<String> = ["-O2", "-x", "c", "-c", src.path().to_str().unwrap()]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let request = ProbeRequest {
            compiler: "cc",
            args: &args,
            key_args: &args,
            per_tu_paths: &[],
            windows_aware: true,
        };
        let Ok(config) = CcProber.probe(&request) else {
            return;
        };
        if let Some(tokens) = config.resolved_tokens {
            assert!(
                tokens.iter().any(|t| t == "-O2"),
                "resolved `-cc1` tokens should carry -O2: {tokens:?}"
            );
        }
    }
}
