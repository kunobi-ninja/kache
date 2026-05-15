//! Path normalization for cache keys.
//!
//! Purpose: stop machine-local absolute paths from leaking into cache
//! keys. Without this, a build at `/path/A/foo` and a build at
//! `/path/B/foo` produce different cache keys for byte-identical
//! sources — cache hits don't transfer across machines or worktrees,
//! and even relocating a project on the same machine misses
//! everything.
//!
//! ## Why a struct, not just `replace_cwd`
//!
//! Before this module, the cache_key code shipped a one-off
//! `normalize_flags` that did `value.replace(cwd, ".")`. Two failure
//! modes the struct fixes:
//!
//! 1. **Symlink-induced miss** (the e422e55 class). On macOS, `/tmp`
//!    is a symlink to `/private/tmp`. CWD reports `/tmp/build` but
//!    cargo emits `OUT_DIR=/private/tmp/build/...`. Literal string
//!    replace finds no match → path leaks into the key. Canonicalizing
//!    the prefix once at construction time fixes this without paying
//!    a `canonicalize()` per-replacement.
//!
//! 2. **Single-prefix horizon**. CWD is one of several path prefixes
//!    a cache key wants stripped. The struct holds an ordered list of
//!    `(prefix, sentinel)` rules so adding a new one is a one-line
//!    edit, not a new ad-hoc helper.
//!
//! ## Cross-machine contract
//!
//! For shared / remote caches (CI runners, multiple devs hitting the
//! same S3 bucket), key portability requires every machine-local path
//! to map to the same sentinel. Today's rule set covers:
//!
//! | Prefix              | Sentinel        | Why it varies per host |
//! |---------------------|-----------------|------------------------|
//! | workspace root      | `<WORKSPACE>`   | per-checkout location  |
//! | `$CARGO_TARGET_DIR` | `<TARGET>`      | per-user / per-job dir |
//! | `$CARGO_HOME`       | `<CARGO_HOME>`  | usually `~/.cargo`     |
//! | `$RUSTUP_HOME`      | `<RUSTUP_HOME>` | usually `~/.rustup`    |
//! | `$HOME`             | `<HOME>`        | per-user               |
//! | system tempdir      | `<TMPDIR>`      | macOS uses `/var/folders/<random>/T` per user |
//!
//! What this **doesn't** cover (separate concerns):
//!
//! - **Output-byte portability**: rustc binaries embed source paths
//!   in DWARF / PDB / `#[track_caller]` callsites. A cached `.rlib`
//!   restored on a different machine carries the cacher's paths. This
//!   PR's `--remap-path-prefix` injection mitigates source paths but
//!   not build.rs-generated paths inside `OUT_DIR`.
//! - **Toolchain divergence**: different rustc / linker / SDK versions
//!   produce different output for the same source. Already captured
//!   by `rustc_version` + `linker_id` in the cache key — non-shareable
//!   by design when these differ.
//! - **Sentinel collision**: if a real path string contained
//!   `<WORKSPACE>` literally, replacement would corrupt it. POSIX
//!   paths don't use `<`/`>`; Windows uses them only inside `\\?\`
//!   device-path forms which aren't cache-key inputs. Acceptable risk.
//!
//! ## Sentinel strategy
//!
//! Each prefix maps to a stable token. Sentinels are angle-bracketed
//! so they can't appear in real path strings. Order matters: longer /
//! more-specific prefixes match first so `<WORKSPACE>` shadows
//! `<HOME>` for users whose workspace lives inside their home dir.
//!
//! ## Leak detection
//!
//! After applying all rules, [`PathNormalizer::normalize`] scans the
//! result for substrings that still look like absolute paths
//! (`/Users/`, `/home/`, `/private/`, drive letters). When found,
//! emits a `tracing::warn!` naming the suspicious value. This is a
//! defense-in-depth check — the relocate e2e phase exists to surface
//! these by failing the build, but the warning gives a faster signal
//! during ad-hoc dev runs without needing to run the full harness.

use std::path::{Path, PathBuf};

/// One canonicalized prefix + its replacement sentinel.
#[derive(Debug, Clone)]
struct Rule {
    prefix: String,
    sentinel: &'static str,
}

/// Replaces machine-local path prefixes with stable sentinels so
/// cache keys are reproducible across hosts and worktrees.
///
/// Construction is best-effort: prefixes that fail to canonicalize
/// (because the env var isn't set, or the path doesn't exist) are
/// silently dropped. This is intentional — a missing
/// `$CARGO_TARGET_DIR` shouldn't make every cache key generation
/// fail; the worst case is the rule doesn't apply for that build.
#[derive(Debug, Clone)]
pub struct PathNormalizer {
    rules: Vec<Rule>,
}

impl PathNormalizer {
    /// Build a normalizer from the current process environment plus
    /// an optional explicit workspace root.
    ///
    /// Rule order is most-specific first, so when prefixes nest
    /// (workspace inside `$HOME`, target dir inside workspace) the
    /// inner prefix wins:
    ///
    /// 1. `workspace_root` (if provided) → `<WORKSPACE>`
    /// 2. `$CARGO_TARGET_DIR` → `<TARGET>`
    /// 3. `$CARGO_HOME` (or default `~/.cargo`) → `<CARGO_HOME>`
    /// 4. `$RUSTUP_HOME` (or default `~/.rustup`) → `<RUSTUP_HOME>`
    /// 5. `$HOME` → `<HOME>`
    /// 6. system tempdir (`std::env::temp_dir()`) → `<TMPDIR>`
    ///
    /// Tempdir lands last because it can be unrelated to user paths
    /// (`/tmp` on Linux, `/var/folders/.../T` on macOS) — it's
    /// purely for relocate-style scenarios where build artifacts
    /// flow through tempdirs.
    pub fn from_env(workspace_root: Option<&Path>) -> Self {
        let mut rules = Vec::new();

        if let Some(ws) = workspace_root.and_then(canonical_string) {
            rules.push(Rule {
                prefix: ws,
                sentinel: "<WORKSPACE>",
            });
        }

        if let Some(td) =
            std::env::var_os("CARGO_TARGET_DIR").and_then(|p| canonical_string(Path::new(&p)))
        {
            rules.push(Rule {
                prefix: td,
                sentinel: "<TARGET>",
            });
        }

        // CARGO_HOME defaults to $HOME/.cargo if not set; honor that
        // so users with the default layout still get the substitution.
        let cargo_home = std::env::var_os("CARGO_HOME")
            .map(PathBuf::from)
            .or_else(|| dirs::home_dir().map(|h| h.join(".cargo")));
        if let Some(ch) = cargo_home.and_then(|p| canonical_string(&p)) {
            rules.push(Rule {
                prefix: ch,
                sentinel: "<CARGO_HOME>",
            });
        }

        // Same default-fallback story for RUSTUP_HOME. rustup paths
        // appear in -L flags and toolchain refs — without this rule,
        // Dev A's `~alice/.rustup` and Dev B's `~bob/.rustup` would
        // produce different keys for byte-identical sources.
        let rustup_home = std::env::var_os("RUSTUP_HOME")
            .map(PathBuf::from)
            .or_else(|| dirs::home_dir().map(|h| h.join(".rustup")));
        if let Some(rh) = rustup_home.and_then(|p| canonical_string(&p)) {
            rules.push(Rule {
                prefix: rh,
                sentinel: "<RUSTUP_HOME>",
            });
        }

        if let Some(home) = dirs::home_dir().and_then(|p| canonical_string(&p)) {
            rules.push(Rule {
                prefix: home,
                sentinel: "<HOME>",
            });
        }

        // System tempdir — varies wildly across hosts (per-user random
        // segment on macOS like `/var/folders/1z/.../T`). Build
        // artifacts from build.rs scripts often pass through tempdirs
        // for intermediate output.
        if let Some(td) = canonical_string(&std::env::temp_dir()) {
            rules.push(Rule {
                prefix: td,
                sentinel: "<TMPDIR>",
            });
        }

        // De-dupe: if (e.g.) CARGO_HOME == workspace_root, the second
        // entry would never fire. Also keep order stable so the
        // first-listed sentinel wins for identical prefixes.
        rules.dedup_by(|a, b| a.prefix == b.prefix);

        Self { rules }
    }

    /// A normalizer with no rules — leaves every input unchanged.
    /// Used in tests and as a sentinel "no normalization configured"
    /// state in code paths where the env-derived constructor isn't
    /// applicable.
    ///
    /// `#[allow(dead_code)]` because the only consumer today is the
    /// test module; production code always builds via `from_env`.
    /// Kept as part of the public surface so future callers (e.g.
    /// integration tests of cache_key with deterministic inputs)
    /// don't have to construct `PathNormalizer` via `from_env` and
    /// pull in the host's actual env state.
    #[allow(dead_code)]
    pub fn empty() -> Self {
        Self { rules: Vec::new() }
    }

    /// Replace every known prefix in `s` with its sentinel.
    ///
    /// Operates on raw strings (not [`Path`]) because cache-key
    /// inputs are typically values from shell commands and env vars
    /// that may contain paths embedded in larger strings (e.g.
    /// `-L /home/x/lib -L /home/x/build/deps`). Each rule does a
    /// literal substring replace; rules apply in declaration order
    /// so the first match for a given byte range wins.
    pub fn normalize<S: AsRef<str>>(&self, s: S) -> String {
        let mut out = s.as_ref().to_string();
        for rule in &self.rules {
            if rule.prefix.is_empty() {
                continue;
            }
            out = out.replace(&rule.prefix, rule.sentinel);
        }
        warn_if_path_leaked(&out);
        out
    }
}

/// Defense-in-depth: scan a normalized string for substrings that
/// still look like machine-local absolute paths and emit a warning.
///
/// The relocate e2e phase already catches these by failing the build
/// when keys diverge across paths, but this gives a faster signal
/// during ad-hoc dev runs (a `KACHE_LOG=warn` user sees the leak the
/// first time they hit it instead of waiting for the full harness).
///
/// Only matches well-known absolute-path prefixes — avoids false
/// positives on legitimate strings that happen to contain `/`. The
/// list is intentionally conservative: missing a leak is preferable
/// to spamming the log on innocuous values.
fn warn_if_path_leaked(s: &str) {
    const SUSPICIOUS_PREFIXES: &[&str] = &[
        "/Users/",       // macOS home
        "/home/",        // Linux home
        "/private/tmp/", // macOS canonicalized tempdir
        "/private/var/", // macOS canonicalized var
        "/var/folders/", // macOS per-user tempdir
        "C:\\Users\\",   // Windows home
    ];
    for prefix in SUSPICIOUS_PREFIXES {
        if let Some(idx) = s.find(prefix) {
            // Show a small window around the leak to make it easier
            // to identify (env var name etc.) without dumping the
            // whole value, which could be huge.
            let start = idx.saturating_sub(40);
            let end = (idx + prefix.len() + 40).min(s.len());
            tracing::warn!(
                "PathNormalizer: residual absolute path detected (prefix `{}`) in normalized value: ...{}...",
                prefix,
                &s[start..end]
            );
            return;
        }
    }
}

/// Canonicalize `path` to a string for prefix matching, or return
/// `None` if the path doesn't exist / can't be resolved. Empty
/// strings are also rejected because `String::replace("", x)` would
/// match between every byte and produce nonsense.
fn canonical_string(path: &Path) -> Option<String> {
    let canon = path.canonicalize().ok()?;
    let s = canon.to_string_lossy().into_owned();
    if s.is_empty() { None } else { Some(s) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn empty_normalizer_is_identity() {
        let n = PathNormalizer::empty();
        assert_eq!(n.normalize("/anything/at/all"), "/anything/at/all");
        assert_eq!(n.normalize(""), "");
    }

    #[test]
    fn canonicalizes_workspace_prefix_and_replaces_with_sentinel() {
        // Real on-disk dir so `canonicalize` succeeds.
        let dir = TempDir::new().unwrap();
        let n = PathNormalizer::from_env(Some(dir.path()));
        let canonical = dir.path().canonicalize().unwrap();
        let input = format!("{}/src/main.rs", canonical.display());
        assert!(
            n.normalize(&input).contains("<WORKSPACE>"),
            "got {} for input {input}",
            n.normalize(&input)
        );
    }

    #[test]
    fn workspace_rule_strips_macos_private_tmp_symlink() {
        // The exact bug class this struct exists to prevent: macOS
        // `/tmp` → `/private/tmp` symlink. A literal-replace
        // implementation that didn't canonicalize would miss when
        // the env var contains the canonicalized form but CWD
        // reports the symlink form (or vice versa).
        if !cfg!(target_os = "macos") {
            return;
        }
        // Construct a real dir under /tmp (the symlink form). The
        // normalizer canonicalizes to /private/tmp internally, so an
        // OUT_DIR-style string using the canonical form should match.
        let symlink_root = Path::new("/tmp");
        let unique = format!("kache-pn-test-{}", std::process::id());
        let real_dir = symlink_root.join(&unique);
        let _ = fs::create_dir_all(&real_dir);
        let n = PathNormalizer::from_env(Some(&real_dir));
        let private_form = format!("/private/tmp/{unique}/target/release/build/foo/out");
        let normalized = n.normalize(&private_form);
        let _ = fs::remove_dir_all(&real_dir);
        assert!(
            normalized.starts_with("<WORKSPACE>"),
            "expected <WORKSPACE> sentinel, got {normalized:?}"
        );
    }

    #[test]
    fn home_rule_normalizes_paths_inside_home() {
        let n = PathNormalizer::from_env(None);
        if let Some(home) = dirs::home_dir().and_then(|p| p.canonicalize().ok()) {
            let input = format!("{}/some/thing", home.display());
            // Either <HOME> or <CARGO_HOME> may match (cargo home is
            // inside home by default). Either is a valid normalization.
            let out = n.normalize(&input);
            assert!(
                out.starts_with('<'),
                "expected a sentinel prefix, got {out:?}"
            );
        }
    }

    #[test]
    fn empty_prefix_does_not_corrupt_input() {
        // Defense-in-depth: an empty prefix would make
        // `String::replace("", x)` match at every byte boundary,
        // producing garbage. The constructor rejects empty prefixes,
        // and `normalize` skips them as belt-and-braces.
        let n = PathNormalizer {
            rules: vec![Rule {
                prefix: String::new(),
                sentinel: "<NEVER>",
            }],
        };
        assert_eq!(n.normalize("hello world"), "hello world");
    }

    #[test]
    fn unmatched_input_passes_through_unchanged() {
        let n = PathNormalizer::from_env(None);
        let input = "this/path/does/not/match/anything/local";
        assert_eq!(n.normalize(input), input);
    }

    #[test]
    fn from_env_includes_rustup_home_rule_when_dir_exists() {
        // Default RUSTUP_HOME is `~/.rustup`; the dir typically exists
        // in any test environment with rustup installed (which is the
        // common case for kache contributors). Skip the assertion if
        // the dir really doesn't exist — better to skip than spam CI
        // with environment-dependent flakes.
        let rustup_dir = dirs::home_dir().map(|h| h.join(".rustup"));
        let Some(dir) = rustup_dir else {
            return;
        };
        if !dir.exists() {
            return;
        }
        let n = PathNormalizer::from_env(None);
        let canonical = dir.canonicalize().unwrap();
        let input = format!("{}/toolchains/stable/bin/rustc", canonical.display());
        let out = n.normalize(&input);
        assert!(
            out.contains("<RUSTUP_HOME>") || out.contains("<HOME>"),
            "expected rustup or home sentinel in {out:?}"
        );
    }

    #[test]
    fn from_env_includes_tempdir_rule() {
        let n = PathNormalizer::from_env(None);
        let temp = std::env::temp_dir().canonicalize().unwrap();
        let input = format!("{}/some-build-artifact", temp.display());
        let out = n.normalize(&input);
        // On macOS the tempdir is `/var/folders/.../T` which is also
        // inside the canonical /private/var path; either sentinel is
        // a valid normalization.
        assert!(
            out.contains("<TMPDIR>") || out.contains("<HOME>") || out.starts_with('<'),
            "expected a sentinel for tempdir-based path, got {out:?}"
        );
    }

    #[test]
    fn leak_detector_does_not_panic_on_unsentineled_paths() {
        // The detector logs but doesn't error — failure mode is
        // observability, not correctness. This test pins that
        // behavior so a future change doesn't accidentally make
        // leak detection panic.
        let n = PathNormalizer::empty();
        // Empty normalizer leaves input untouched; if the input
        // contains a suspicious path, the detector logs.
        let _ = n.normalize("/Users/alice/leaked/path");
        let _ = n.normalize("/home/bob/leaked/path");
        let _ = n.normalize("C:\\Users\\charlie\\leaked");
        // No assertions on log output (we don't capture tracing in
        // unit tests); the assertion is "did not panic".
    }
}
