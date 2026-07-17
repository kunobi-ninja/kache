use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::compiler::{ArtifactSet, classify_by_filename};

/// Result of running rustc.
pub struct CompileResult {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    /// Full artifact set produced by this compilation.
    pub artifacts: ArtifactSet,
}

/// Run rustc with the given arguments, capturing all outputs.
///
/// `path_normalizer` provides the rule set for `--remap-path-prefix`
/// injection — same rules used to normalize the cache key, applied
/// at the rustc invocation layer so the resulting binary's debug
/// info uses stable sentinels instead of machine-local paths.
/// Cross-machine cache hits then serve binaries whose DWARF / PDB
/// references map cleanly to a recipient's local source via
/// `lldb`/`gdb`'s `set substitute-path` (or equivalent).
pub fn run_rustc(
    rustc: &Path,
    inner_rustc: Option<&Path>,
    args: &[String],
    output_path: Option<&Path>,
    out_dir: Option<&Path>,
    crate_name: Option<&str>,
    extra_filename: Option<&str>,
    emit: &[String],
    skip_remap: bool,
    path_normalizer: &crate::path_normalizer::PathNormalizer,
) -> Result<CompileResult> {
    // Pre-clean output paths: remove any read-only hardlinks left by a previous
    // kache cache hit. Without this, rustc cannot overwrite the 0444 hardlinked
    // files and fails with "output file is not writeable".
    pre_clean_outputs(output_path, out_dir, crate_name, extra_filename, emit);

    crate::opcounts::record_compiler_run();
    let mut cmd = Command::new(rustc);

    // Double-wrapper (RUSTC_WRAPPER + RUSTC_WORKSPACE_WRAPPER): the workspace
    // wrapper (e.g. clippy-driver) expects the actual rustc path as its first arg.
    if let Some(inner) = inner_rustc {
        cmd.arg(inner);
    }

    // Multi-prefix path remapping for reproducible builds across
    // different machines / worktrees / CI runners. Replaces the
    // earlier single-prefix `--remap-path-prefix=$CWD=.` injection,
    // which had two structural failure modes (same as the old
    // `normalize_flags`):
    //   - single prefix: source paths under $CARGO_HOME, $RUSTUP_HOME,
    //     etc. weren't covered, so DWARF embedded e.g.
    //     `/Users/alice/.cargo/registry/...` even when CWD was the
    //     workspace.
    //   - CWD-only: macOS `/tmp` ↔ `/private/tmp` symlink + the
    //     transitive-dep-cwd issue (cargo cd's into each dep's
    //     source dir before invoking rustc) made CWD the wrong
    //     anchor for transitive deps.
    //
    // Skipped under coverage instrumentation — tools like tarpaulin
    // and llvm-cov need original source paths in profraw data to
    // map coverage hits back to source files.
    if !skip_remap {
        for arg in path_normalizer.remap_args() {
            cmd.arg(arg);
        }
    }

    // Disable incremental compilation — kache's artifact cache subsumes it, and
    // incremental is prone to APFS-related corruption on macOS (dep-graph move failures).
    // Strip `-C incremental=...` from args since CARGO_INCREMENTAL=0 is too late
    // (cargo already passed the flag before the wrapper runs).
    // Handles: `-Cincremental=<path>` and `-C` `incremental=<path>` (two-arg form).
    let filtered_args = strip_incremental_flags(args);
    if filtered_args.len() < args.len() {
        tracing::info!(
            "[kache] stripped incremental flags for {} ({} args removed)",
            crate_name.unwrap_or("unknown"),
            args.len() - filtered_args.len()
        );
    }
    cmd.args(&filtered_args);

    tracing::debug!("running: {} {}", rustc.display(), args.join(" "));

    let output = cmd
        .output()
        .with_context(|| format!("executing {}", rustc.display()))?;

    let exit_code = output.status.code().unwrap_or(1);
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    // Detect incremental-related failures and log diagnostics
    if exit_code != 0
        && (stderr.contains("failed to move dependency graph")
            || stderr.contains("failed to create query cache")
            || stderr.contains("incremental"))
    {
        tracing::warn!(
            "[kache] incremental compilation failure detected for {} — \
             this is an APFS bug in git worktrees. \
             Run `cargo clean` in the affected project to recover.",
            crate_name.unwrap_or("unknown")
        );
    }

    // Discover output files. rustc's own `artifact` JSON notifications
    // are authoritative — when cargo invokes rustc it passes
    // `--error-format=json --json=artifacts`, so rustc reports every
    // file it writes by exact path. Candidate path probing is the fallback
    // for invocations that don't emit those messages (a bare `rustc`
    // without `--json=artifacts`), where filename guessing is the best
    // available signal.
    let artifacts = if exit_code == 0 {
        let from_json = resolve_artifacts(&parse_rustc_artifacts(&stderr));
        if from_json.is_empty() {
            tracing::debug!(
                "[kache] no rustc artifact notifications for {}; falling back to candidate path discovery",
                crate_name.unwrap_or("unknown")
            );
            ArtifactSet::from_output_files(
                discover_output_files(output_path, out_dir, crate_name, extra_filename, emit)?,
                classify_by_filename,
            )
        } else {
            tracing::debug!(
                "[kache] discovered {} output file(s) for {} from rustc artifact notifications",
                from_json.len(),
                crate_name.unwrap_or("unknown")
            );
            ArtifactSet::from_output_files(from_json, classify_by_filename)
        }
    } else {
        ArtifactSet::empty()
    };

    Ok(CompileResult {
        exit_code,
        stdout,
        stderr,
        artifacts,
    })
}

/// Strip `-C incremental=...` flags from rustc arguments.
///
/// Cargo passes `-C incremental=<path>` to rustc before RUSTC_WRAPPER runs,
/// so setting `CARGO_INCREMENTAL=0` on the child process is too late.
/// We must remove the flags from the argument list directly.
///
/// Handles both forms:
/// - `-Cincremental=<path>` (joined)
/// - `-C` `incremental=<path>` (two-arg)
pub fn strip_incremental_flags(args: &[String]) -> Vec<&String> {
    let mut filtered: Vec<&String> = Vec::with_capacity(args.len());
    let mut i = 0;
    while i < args.len() {
        if args[i].starts_with("-Cincremental=") {
            i += 1;
            continue;
        }
        if args[i] == "-C"
            && args
                .get(i + 1)
                .is_some_and(|next| next.starts_with("incremental="))
        {
            i += 2;
            continue;
        }
        filtered.push(&args[i]);
        i += 1;
    }
    filtered
}

/// Parse rustc's `artifact` JSON notifications out of a captured stderr
/// stream.
///
/// When cargo invokes rustc it passes `--error-format=json
/// --json=artifacts`; rustc then prints one line per file it writes:
///
/// ```json
/// {"$message_type":"artifact","artifact":"target/debug/deps/libfoo-9a.rlib","emit":"link"}
/// ```
///
/// That is rustc's own authoritative statement of the produced file
/// set — no filename guessing, no directory globbing that can over- or
/// under-capture. Lines that are not artifact messages (diagnostics,
/// non-JSON text) are skipped. Returns the paths exactly as rustc
/// reported them (relative to rustc's cwd, or absolute); an empty Vec
/// means the stream carried no artifact messages, and the caller falls
/// back to [`discover_output_files`].
fn parse_rustc_artifacts(stderr: &str) -> Vec<PathBuf> {
    let mut artifacts = Vec::new();
    for line in stderr.lines() {
        let line = line.trim();
        // Cheap reject before the JSON parse: every artifact message is
        // a JSON object, and most stderr lines are not.
        if !line.starts_with('{') {
            continue;
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if value.get("$message_type").and_then(|v| v.as_str()) != Some("artifact") {
            continue;
        }
        if let Some(path) = value.get("artifact").and_then(|v| v.as_str()) {
            artifacts.push(PathBuf::from(path));
        }
    }
    artifacts
}

/// Resolve rustc-reported artifact paths into the `(absolute_path,
/// store_filename)` pairs the cache layer stores.
///
/// rustc reports artifact paths relative to its own cwd; kache never
/// sets a cwd on the rustc child, so that cwd is the wrapper's cwd.
/// Absolute paths are kept as-is. A reported artifact missing from disk
/// is dropped with a warning rather than aborting — kache then stores a
/// smaller set (a conservative miss next time), never a corrupt entry.
fn resolve_artifacts(artifacts: &[PathBuf]) -> Vec<(PathBuf, String)> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let mut files = Vec::new();
    for artifact in artifacts {
        let abs = if artifact.is_absolute() {
            artifact.clone()
        } else {
            cwd.join(artifact)
        };
        let Some(name) = abs.file_name().map(|n| n.to_string_lossy().to_string()) else {
            continue;
        };
        if !abs.exists() {
            tracing::warn!(
                "[kache] rustc reported artifact missing on disk, skipping: {}",
                abs.display()
            );
            continue;
        }
        files.push((abs, name));
    }
    files
}

/// Product suffixes for rustc/cargo artifacts (`{base}.{suffix}`).
///
/// Shared by `--out-dir` candidates and `-o` stem siblings. Includes compound
/// Windows import-lib names (`dll.lib` / `dll.exp` / `dll.a`) so a primary
/// `foo.dll` still probes `foo.dll.lib`. Over-probe is intentional: missing a
/// restored 0444 hardlink causes EACCES on the next rustc write.
///
/// Not covered: per-CGU names under `--emit=obj|llvm-ir|asm|llvm-bc` with
/// `codegen-units > 1` (`{crate}{extra}.{crate}.{hash}-cgu.N.rcgu.o`, …).
/// Those are unbounded — see [`emit_may_produce_per_cgu_files`].
const ARTIFACT_SUFFIXES: &[&str] = &[
    "rlib", "rmeta", "d", "so", "dylib", "dll", "wasm", "a", "lib", "dll.lib", "dll.exp", "dll.a",
    "exe", "pdb", "dwo", "dwp", "o", "obj", "ll", "bc", "mir", "s", "asm",
];

/// Emit kinds that produce unbounded per-CGU filenames when `codegen-units > 1`.
/// A closed candidate list cannot name them; fall back to a prefix `read_dir`.
const PER_CGU_EMIT_KINDS: &[&str] = &["obj", "llvm-ir", "asm", "llvm-bc"];

/// True when `--emit` includes kinds that can produce per-CGU artifact names
/// (`foo-abc.foo.<hash>-cgu.0.rcgu.o`, …). Cargo's usual `link,metadata,dep-info`
/// does not — so the hot path stays probe-only.
fn emit_may_produce_per_cgu_files(emit: &[String]) -> bool {
    emit.iter().any(|e| {
        let kind = e.split('=').next().unwrap_or(e.as_str());
        PER_CGU_EMIT_KINDS.contains(&kind)
    })
}

/// Invoke `f` for each `--out-dir` candidate basename (no intermediate `Vec`).
fn for_each_out_dir_candidate(crate_name: &str, extra: &str, mut f: impl FnMut(&str)) {
    let bare = crate::args::format_crate_output_stem(crate_name, extra);
    let mut buf = String::with_capacity(bare.len() + 8 + 16);
    for suffix in ARTIFACT_SUFFIXES {
        buf.clear();
        buf.push_str("lib");
        buf.push_str(&bare);
        buf.push('.');
        buf.push_str(suffix);
        f(&buf);

        buf.clear();
        buf.push_str(&bare);
        buf.push('.');
        buf.push_str(suffix);
        f(&buf);
    }
    f(&bare);
}

/// Probe known same-stem sibling paths next to a `-o` primary output.
fn for_each_stem_sibling(parent: &Path, stem: &str, primary: &Path, mut f: impl FnMut(&Path)) {
    let mut buf = String::with_capacity(stem.len() + 16);
    for suffix in ARTIFACT_SUFFIXES {
        buf.clear();
        buf.push_str(stem);
        buf.push('.');
        buf.push_str(suffix);
        let path = parent.join(&buf);
        if path != *primary {
            f(&path);
        }
    }
}

fn crate_depinfo_path(parent: &Path, crate_name: &str, extra: &str) -> PathBuf {
    parent.join(format!(
        "{}.d",
        crate::args::format_crate_output_stem(crate_name, extra)
    ))
}

fn path_file_name(path: &Path) -> Option<String> {
    path.file_name().map(|n| n.to_string_lossy().into_owned())
}

/// Whether `name` is a cargo/rustc product for `crate_name` + `extra`.
///
/// Matches the exact stem and any `{stem}.…` prefix (covers per-CGU files,
/// custom dep-info basenames, etc. — the old full-scan filter).
fn matches_crate_output_prefix(name: &str, crate_name: &str, extra: &str) -> bool {
    let bare = crate::args::format_crate_output_stem(crate_name, extra);
    let lib = format!("lib{bare}");
    name == bare
        || name == lib
        || name.starts_with(&format!("{bare}."))
        || name.starts_with(&format!("{lib}."))
}

/// Prefix `read_dir` of `dir` for products of `crate_name` + `extra`.
///
/// Only used when [`emit_may_produce_per_cgu_files`] — per-CGU filenames are
/// unbounded (`…-cgu.N.rcgu.o`), so they cannot be probed. Cargo's default
/// emit sets never take this path.
fn for_each_prefix_match_in_dir(
    dir: &Path,
    crate_name: &str,
    extra: &str,
    mut f: impl FnMut(&Path),
) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if matches_crate_output_prefix(name, crate_name, extra) {
            f(&path);
        }
    }
}

/// Fallback discovery when rustc did not emit JSON artifact notifications.
///
/// Probes a fixed candidate set on the cargo hot path. When `--emit` includes
/// per-CGU kinds, falls back to a prefix `read_dir` (unbounded names).
fn discover_output_files(
    output_path: Option<&Path>,
    out_dir: Option<&Path>,
    crate_name: Option<&str>,
    extra_filename: Option<&str>,
    emit: &[String],
) -> Result<Vec<(PathBuf, String)>> {
    let mut files = Vec::new();
    let extra = extra_filename.unwrap_or("");
    let prefix_scan = emit_may_produce_per_cgu_files(emit);

    if let Some(output) = output_path {
        if output.is_file()
            && let Some(filename) = path_file_name(output)
        {
            files.push((output.to_path_buf(), filename));
        }

        if let (Some(parent), Some(stem)) = (output.parent(), output.file_stem()) {
            let stem_str = stem.to_string_lossy();
            for_each_stem_sibling(parent, &stem_str, output, |path| {
                if path.is_file()
                    && let Some(name) = path_file_name(path)
                {
                    files.push((path.to_path_buf(), name));
                }
            });
        }

        if let (Some(parent), Some(name)) = (output.parent(), crate_name) {
            let d_file = crate_depinfo_path(parent, name, extra);
            if d_file.is_file()
                && !files.iter().any(|(p, _)| p == &d_file)
                && let Some(filename) = path_file_name(&d_file)
            {
                files.push((d_file, filename));
            }
            if prefix_scan {
                for_each_prefix_match_in_dir(parent, name, extra, |path| {
                    if path.is_file()
                        && !files.iter().any(|(p, _)| p == path)
                        && let Some(filename) = path_file_name(path)
                    {
                        files.push((path.to_path_buf(), filename));
                    }
                });
            }
        }
    } else if let (Some(dir), Some(name)) = (out_dir, crate_name) {
        if prefix_scan {
            for_each_prefix_match_in_dir(dir, name, extra, |path| {
                if path.is_file()
                    && let Some(filename) = path_file_name(path)
                {
                    files.push((path.to_path_buf(), filename));
                }
            });
        } else {
            for_each_out_dir_candidate(name, extra, |fname| {
                let path = dir.join(fname);
                if path.is_file() {
                    files.push((path, fname.to_string()));
                }
            });
        }
    }

    Ok(files)
}

/// Remove read-only hardlinks at known output paths so rustc can overwrite them.
///
/// Cargo's usual emit sets probe a fixed candidate list (no out-dir `read_dir`).
/// When `--emit` includes per-CGU kinds (`obj` / `llvm-ir` / `asm` / `llvm-bc`),
/// falls back to a prefix directory scan — those filenames are unbounded.
/// Also used by direct-exec passthrough (`KACHE_DISABLED`) — see `main.rs`.
pub(crate) fn pre_clean_outputs(
    output_path: Option<&Path>,
    out_dir: Option<&Path>,
    crate_name: Option<&str>,
    extra_filename: Option<&str>,
    emit: &[String],
) {
    let extra = extra_filename.unwrap_or("");
    let prefix_scan = emit_may_produce_per_cgu_files(emit);

    if let Some(output) = output_path {
        remove_if_readonly(output);

        if let (Some(parent), Some(stem)) = (output.parent(), output.file_stem()) {
            let stem_str = stem.to_string_lossy();
            for_each_stem_sibling(parent, &stem_str, output, remove_if_readonly);
        }

        if let (Some(parent), Some(name)) = (output.parent(), crate_name) {
            remove_if_readonly(&crate_depinfo_path(parent, name, extra));
            if prefix_scan {
                for_each_prefix_match_in_dir(parent, name, extra, remove_if_readonly);
            }
        }
    } else if let (Some(dir), Some(name)) = (out_dir, crate_name) {
        if prefix_scan {
            for_each_prefix_match_in_dir(dir, name, extra, remove_if_readonly);
        } else {
            for_each_out_dir_candidate(name, extra, |fname| {
                remove_if_readonly(&dir.join(fname));
            });
        }
    } else {
        // Incomplete identity: refuse a mass sweep of out-dir (rio-build#51 / kache#242).
        tracing::debug!(
            "pre_clean_outputs: nothing to identify outputs by \
             (out_dir={out_dir:?}, crate_name={crate_name:?}); skipping read-only pre-clean — \
             a warm restore in this directory may block this compile"
        );
    }
}

/// Remove a file if it exists and is read-only (likely a kache hardlink).
fn remove_if_readonly(path: &Path) {
    if let Ok(meta) = std::fs::metadata(path)
        && meta.permissions().readonly()
    {
        #[cfg(windows)]
        {
            let mut perms = meta.permissions();
            perms.set_readonly(false);
            let _ = std::fs::set_permissions(path, perms);
        }
        let _ = std::fs::remove_file(path);
    }
}

// NOTE: ad-hoc codesign logic used to live here behind
// `#[cfg(target_os = "macos")]`. It now lives on
// `crate::compiler::platform::MacOsPlatform::ensure_binary_loadable`,
// reachable from any caller (including the future cc store path) via
// the `Platform` trait. Restore-time dispatch flows through
// `PostRestoreAction::Sign(SigningPurpose::OsLoading)`.

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    // Used only by the `#[cfg(unix)]` hardlink test below, which relies on
    // Unix mode bits; the portable read-only tests use `make_readonly`.
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    /// Mark a file read-only on any platform. Unix mode bits (`from_mode`)
    /// aren't available on Windows, and `remove_if_readonly` keys off the
    /// portable `permissions().readonly()` flag anyway — so this also lets
    /// these tests exercise the Windows read-only branch of the cleaner.
    fn make_readonly(path: &std::path::Path) {
        let mut perms = fs::metadata(path).unwrap().permissions();
        perms.set_readonly(true);
        fs::set_permissions(path, perms).unwrap();
    }

    #[test]
    fn test_pre_clean_removes_readonly_output() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("libfoo-abc123.rlib");

        // Simulate a kache hardlink: create a read-only file
        fs::write(&output, b"cached content").unwrap();
        make_readonly(&output);
        assert!(fs::metadata(&output).unwrap().permissions().readonly());

        pre_clean_outputs(Some(&output), None, None, None, &[]);

        assert!(!output.exists(), "read-only file should have been removed");
    }

    #[test]
    fn test_pre_clean_removes_readonly_siblings() {
        let dir = tempfile::tempdir().unwrap();
        let rlib = dir.path().join("libfoo-abc123.rlib");
        let rmeta = dir.path().join("libfoo-abc123.rmeta");
        let dep = dir.path().join("foo-abc123.d");

        for path in [&rlib, &rmeta, &dep] {
            fs::write(path, b"cached").unwrap();
            make_readonly(path);
        }

        pre_clean_outputs(Some(&rlib), None, Some("foo"), Some("-abc123"), &[]);

        assert!(!rlib.exists());
        assert!(!rmeta.exists());
        assert!(!dep.exists());
    }

    #[test]
    fn test_pre_clean_skips_writable_files() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("libfoo-abc123.rlib");

        // Create a normal writable file (not a kache hardlink)
        fs::write(&output, b"fresh content").unwrap();
        assert!(!fs::metadata(&output).unwrap().permissions().readonly());

        pre_clean_outputs(Some(&output), None, None, None, &[]);

        assert!(output.exists(), "writable file should NOT be removed");
    }

    #[test]
    fn test_pre_clean_out_dir_mode() {
        let dir = tempfile::tempdir().unwrap();
        let rlib = dir.path().join("libmycrate-def456.rlib");
        let rmeta = dir.path().join("libmycrate-def456.rmeta");
        let unrelated = dir.path().join("libother-xyz.rlib");

        for path in [&rlib, &rmeta, &unrelated] {
            fs::write(path, b"cached").unwrap();
            make_readonly(path);
        }

        pre_clean_outputs(
            None,
            Some(dir.path()),
            Some("mycrate"),
            Some("-def456"),
            &[],
        );

        assert!(!rlib.exists());
        assert!(!rmeta.exists());
        assert!(
            unrelated.exists(),
            "unrelated crate files should not be removed"
        );
    }

    #[test]
    fn test_pre_clean_without_identity_is_a_safe_noop() {
        // Unusual spawner: no output path and no crate_name to identify
        // outputs by. pre_clean must not touch anything (and must not panic) —
        // it logs at debug instead of sweeping the directory.
        let dir = tempfile::tempdir().unwrap();
        let readonly = dir.path().join("libfoo-abc123.rlib");
        fs::write(&readonly, b"cached").unwrap();
        make_readonly(&readonly);

        pre_clean_outputs(None, Some(dir.path()), None, None, &[]);
        pre_clean_outputs(None, None, None, None, &[]);

        assert!(
            readonly.exists(),
            "with no crate identity, pre_clean must not remove files"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_pre_clean_removes_hardlink_without_mutating_store_blob() {
        let dir = tempfile::tempdir().unwrap();
        let blob = dir.path().join("blob.rlib");
        let output = dir.path().join("libfoo-abc123.rlib");

        fs::write(&blob, b"cached content").unwrap();
        fs::set_permissions(&blob, fs::Permissions::from_mode(0o444)).unwrap();
        fs::hard_link(&blob, &output).unwrap();

        pre_clean_outputs(Some(&output), None, None, None, &[]);

        assert!(
            !output.exists(),
            "restored hardlink should have been removed"
        );
        assert!(blob.exists(), "store blob should remain");
        assert!(
            fs::metadata(&blob).unwrap().permissions().readonly(),
            "removing the output must not make the shared blob writable"
        );
    }

    #[test]
    fn test_strip_incremental_joined_form() {
        let args: Vec<String> = vec![
            "--crate-name".into(),
            "foo".into(),
            "-Cincremental=/tmp/incr".into(),
            "-Copt-level=3".into(),
        ];
        let filtered = strip_incremental_flags(&args);
        assert_eq!(filtered.len(), 3);
        assert!(!filtered.iter().any(|a| a.contains("incremental")));
    }

    #[test]
    fn test_strip_incremental_two_arg_form() {
        let args: Vec<String> = vec![
            "--crate-name".into(),
            "foo".into(),
            "-C".into(),
            "incremental=/tmp/incr".into(),
            "-C".into(),
            "opt-level=3".into(),
        ];
        let filtered = strip_incremental_flags(&args);
        assert_eq!(filtered.len(), 4); // crate-name, foo, -C, opt-level=3
        assert!(!filtered.iter().any(|a| a.contains("incremental")));
    }

    #[test]
    fn test_strip_incremental_preserves_other_flags() {
        let args: Vec<String> = vec![
            "-C".into(),
            "opt-level=3".into(),
            "-C".into(),
            "metadata=abc".into(),
        ];
        let filtered = strip_incremental_flags(&args);
        assert_eq!(filtered.len(), args.len());
    }

    #[test]
    fn test_strip_incremental_empty_args() {
        let args: Vec<String> = vec![];
        let filtered = strip_incremental_flags(&args);
        assert!(filtered.is_empty());
    }

    #[test]
    fn test_strip_incremental_multiple() {
        let args: Vec<String> = vec![
            "-Cincremental=/a".into(),
            "-C".into(),
            "incremental=/b".into(),
            "src/lib.rs".into(),
        ];
        let filtered = strip_incremental_flags(&args);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0], "src/lib.rs");
    }

    #[test]
    fn test_strip_incremental_c_without_incremental() {
        let args: Vec<String> = vec!["-C".into(), "debuginfo=2".into()];
        let filtered = strip_incremental_flags(&args);
        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn test_remove_if_readonly_nonexistent_file() {
        remove_if_readonly(Path::new("/nonexistent/path"));
        // Should not panic
    }

    #[test]
    fn test_discover_output_files_missing_dir() {
        let result = discover_output_files(
            Some(Path::new("/nonexistent/output.rlib")),
            None,
            None,
            None,
            &[],
        )
        .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_discover_output_files_out_dir_mode() {
        let dir = tempfile::tempdir().unwrap();
        // Plant a mix of lib / dep-info / bin / dylib products plus noise.
        let planted = [
            "libfoo-abc.rlib",
            "libfoo-abc.rmeta",
            "foo-abc.d",
            "libfoo-abc.dylib",
            "foo-abc", // unix bin
        ];
        for name in planted {
            fs::write(dir.path().join(name), b"content").unwrap();
        }
        fs::write(dir.path().join("libbar-xyz.rlib"), b"content").unwrap();

        let files =
            discover_output_files(None, Some(dir.path()), Some("foo"), Some("-abc"), &[]).unwrap();
        let mut names: Vec<&str> = files.iter().map(|(_, n)| n.as_str()).collect();
        names.sort_unstable();
        let mut expected = planted.to_vec();
        expected.sort_unstable();
        assert_eq!(names, expected, "exact discover set for planted candidates");
    }

    // ── authoritative discovery via rustc artifact notifications ──

    #[test]
    fn parse_rustc_artifacts_extracts_paths_and_skips_noise() {
        // A realistic stderr stream: a diagnostic JSON message, two
        // artifact messages, a human-readable summary line, and a
        // non-JSON line. Only the two artifact paths must come back,
        // in order.
        let stream = concat!(
            r#"{"$message_type":"diagnostic","message":"unused","level":"warning"}"#,
            "\n",
            r#"{"$message_type":"artifact","artifact":"target/debug/deps/libfoo-9a.rmeta","emit":"metadata"}"#,
            "\n",
            "warning: 1 warning emitted\n",
            r#"{"$message_type":"artifact","artifact":"target/debug/deps/libfoo-9a.rlib","emit":"link"}"#,
            "\n",
            "not json at all\n",
        );
        assert_eq!(
            parse_rustc_artifacts(stream),
            vec![
                PathBuf::from("target/debug/deps/libfoo-9a.rmeta"),
                PathBuf::from("target/debug/deps/libfoo-9a.rlib"),
            ]
        );
    }

    #[test]
    fn parse_rustc_artifacts_empty_when_no_artifact_messages() {
        // A bare `rustc` invocation without `--json=artifacts`:
        // human-readable diagnostics only, no artifact notifications.
        // The caller must then fall back to candidate path discovery.
        let stream = "warning: unused variable: `x`\nerror: aborting due to 1 error\n";
        assert!(parse_rustc_artifacts(stream).is_empty());
    }

    #[test]
    fn parse_rustc_artifacts_empty_input() {
        assert!(parse_rustc_artifacts("").is_empty());
    }

    #[test]
    fn resolve_artifacts_keeps_existing_and_drops_missing() {
        let dir = tempfile::tempdir().unwrap();
        let present = dir.path().join("libfoo-9a.rlib");
        fs::write(&present, b"rlib bytes").unwrap();
        let missing = dir.path().join("ghost-9a.rmeta");

        let resolved = resolve_artifacts(&[present.clone(), missing]);

        // A reported-but-missing artifact is dropped, not fatal: kache
        // stores the smaller set rather than a corrupt entry.
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].0, present);
        assert_eq!(resolved[0].1, "libfoo-9a.rlib");
    }

    #[test]
    fn test_discover_output_files_o_mode_finds_siblings_and_depinfo() {
        // -o mode: the primary output plus same-stem siblings, plus the
        // crate-name dep-info pattern, are all discovered.
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("libfoo.rlib");
        fs::write(&output, b"rlib").unwrap();
        fs::write(dir.path().join("libfoo.rmeta"), b"rmeta").unwrap();
        fs::write(dir.path().join("libfoo.d"), b"deps").unwrap();
        // crate-name dep-info pattern: {crate_name}{extra}.d
        fs::write(dir.path().join("foo-abc123.d"), b"deps2").unwrap();
        // An unrelated file with a different stem is ignored.
        fs::write(dir.path().join("other.rlib"), b"x").unwrap();

        let files =
            discover_output_files(Some(&output), None, Some("foo"), Some("-abc123"), &[]).unwrap();
        let mut names: Vec<&str> = files.iter().map(|(_, n)| n.as_str()).collect();
        names.sort_unstable();
        assert_eq!(
            names,
            vec!["foo-abc123.d", "libfoo.d", "libfoo.rlib", "libfoo.rmeta"]
        );
    }

    fn collect_out_dir_candidates(crate_name: &str, extra: &str) -> Vec<String> {
        let mut names = Vec::with_capacity(ARTIFACT_SUFFIXES.len() * 2 + 1);
        for_each_out_dir_candidate(crate_name, extra, |n| names.push(n.to_string()));
        names
    }

    #[test]
    fn out_dir_candidate_filenames_exact_ordered_set() {
        // Hardcoded full ordered set: dropping/reordering a product suffix
        // (or the lib/bare dual emission) must fail this equality.
        let names = collect_out_dir_candidates("serde", "-9f2a1b");
        let expected: &[&str] = &[
            "libserde-9f2a1b.rlib",
            "serde-9f2a1b.rlib",
            "libserde-9f2a1b.rmeta",
            "serde-9f2a1b.rmeta",
            "libserde-9f2a1b.d",
            "serde-9f2a1b.d",
            "libserde-9f2a1b.so",
            "serde-9f2a1b.so",
            "libserde-9f2a1b.dylib",
            "serde-9f2a1b.dylib",
            "libserde-9f2a1b.dll",
            "serde-9f2a1b.dll",
            "libserde-9f2a1b.wasm",
            "serde-9f2a1b.wasm",
            "libserde-9f2a1b.a",
            "serde-9f2a1b.a",
            "libserde-9f2a1b.lib",
            "serde-9f2a1b.lib",
            "libserde-9f2a1b.dll.lib",
            "serde-9f2a1b.dll.lib",
            "libserde-9f2a1b.dll.exp",
            "serde-9f2a1b.dll.exp",
            "libserde-9f2a1b.dll.a",
            "serde-9f2a1b.dll.a",
            "libserde-9f2a1b.exe",
            "serde-9f2a1b.exe",
            "libserde-9f2a1b.pdb",
            "serde-9f2a1b.pdb",
            "libserde-9f2a1b.dwo",
            "serde-9f2a1b.dwo",
            "libserde-9f2a1b.dwp",
            "serde-9f2a1b.dwp",
            "libserde-9f2a1b.o",
            "serde-9f2a1b.o",
            "libserde-9f2a1b.obj",
            "serde-9f2a1b.obj",
            "libserde-9f2a1b.ll",
            "serde-9f2a1b.ll",
            "libserde-9f2a1b.bc",
            "serde-9f2a1b.bc",
            "libserde-9f2a1b.mir",
            "serde-9f2a1b.mir",
            "libserde-9f2a1b.s",
            "serde-9f2a1b.s",
            "libserde-9f2a1b.asm",
            "serde-9f2a1b.asm",
            "serde-9f2a1b",
        ];
        assert_eq!(names.as_slice(), expected);

        let bare = collect_out_dir_candidates("app", "");
        assert_eq!(bare.first().map(String::as_str), Some("libapp.rlib"));
        assert!(bare.iter().any(|n| n == "app.d"));
        assert_eq!(bare.last().map(String::as_str), Some("app"));
        assert_eq!(bare.len(), expected.len());
    }

    #[test]
    fn pre_clean_out_dir_removes_all_common_readonly_candidates() {
        let dir = tempfile::tempdir().unwrap();
        let targets = [
            "libfoo-abc.rlib",
            "libfoo-abc.rmeta",
            "foo-abc.d",
            "libfoo-abc.so",
            "libfoo-abc.dylib",
            "libfoo-abc.dll",
            "foo-abc.dll",
            "foo-abc.exe",
            "foo-abc", // bin
            "libfoo-abc.a",
            // emit products + Windows dll sidecars
            "foo-abc.ll",
            "foo-abc.dll.lib",
            "foo-abc.dll.exp",
            "foo-abc.dll.a",
            "libfoo-abc.dll.a",
        ];
        for name in targets {
            let path = dir.path().join(name);
            fs::write(&path, b"cached").unwrap();
            make_readonly(&path);
        }
        // Unrelated crate must survive.
        let other = dir.path().join("libbar-xyz.rlib");
        fs::write(&other, b"cached").unwrap();
        make_readonly(&other);

        pre_clean_outputs(None, Some(dir.path()), Some("foo"), Some("-abc"), &[]);

        for name in targets {
            assert!(
                !dir.path().join(name).exists(),
                "expected pre_clean to remove readonly {name}"
            );
        }
        assert!(other.exists(), "unrelated crate must not be touched");
    }

    #[test]
    fn pre_clean_windows_dll_sidecars_o_mode() {
        // Primary `foo-abc.dll` has file_stem `foo-abc`; compound suffixes
        // must still hit `foo-abc.dll.lib` / `.dll.exp` / `.dll.a`.
        let dir = tempfile::tempdir().unwrap();
        let dll = dir.path().join("foo-abc.dll");
        let sidecars = [
            "foo-abc.dll.lib",
            "foo-abc.dll.exp",
            "foo-abc.dll.a",
            "foo-abc.pdb",
        ];
        fs::write(&dll, b"dll").unwrap();
        make_readonly(&dll);
        for name in sidecars {
            let path = dir.path().join(name);
            fs::write(&path, b"cached").unwrap();
            make_readonly(&path);
        }

        pre_clean_outputs(Some(&dll), None, Some("foo"), Some("-abc"), &[]);

        assert!(!dll.exists());
        for name in sidecars {
            assert!(
                !dir.path().join(name).exists(),
                "o-mode must remove dll sidecar {name}"
            );
        }
    }

    #[test]
    fn production_read_dir_only_in_prefix_scan_helper() {
        // `read_dir` is allowed only inside `for_each_prefix_match_in_dir`
        // (gated by per-CGU emit). Normalize CRLF first — Windows checkouts
        // can rewrite line endings, which would make a bare `\n#[cfg(test)]`
        // split miss and fail-open over the whole file (including this test).
        let src = include_str!("compile.rs").replace("\r\n", "\n");
        let marker = "\n#[cfg(test)]\nmod tests {";
        assert!(
            src.contains(marker),
            "expected tests module marker for production/test split"
        );
        let production = src.split(marker).next().expect("tests module");
        let mut code = String::new();
        for line in production.lines() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("//") {
                continue;
            }
            code.push_str(line);
            code.push('\n');
        }
        let helper = "fn for_each_prefix_match_in_dir";
        let start = code
            .find(helper)
            .expect("prefix-scan helper must exist in production code");
        let after = &code[start + helper.len()..];
        let end_rel = after
            .find("\nfn ")
            .or_else(|| after.find("\npub(crate) fn "))
            .unwrap_or(after.len());
        let helper_body = &after[..end_rel];
        let outside = format!("{}{}", &code[..start], &after[end_rel..]);
        assert!(
            helper_body.contains("read_dir"),
            "prefix-scan helper must call read_dir"
        );
        assert!(
            !outside.contains("read_dir"),
            "read_dir must only appear in for_each_prefix_match_in_dir"
        );
    }

    #[test]
    fn pre_clean_removes_per_cgu_object_when_emit_obj() {
        // Regression (PR review): with --emit=obj and codegen-units > 1, rustc
        // writes `{crate}{extra}.{crate}.{hash}-cgu.N.rcgu.o`. A closed suffix
        // list cannot name these; prefix scan (gated on emit) must still clean.
        let dir = tempfile::tempdir().unwrap();
        let cgu = dir.path().join("foo-abc.foo.deadbeef12345678-cgu.0.rcgu.o");
        let noise = dir.path().join("libbar-xyz.rlib");
        fs::write(&cgu, b"cached").unwrap();
        make_readonly(&cgu);
        fs::write(&noise, b"cached").unwrap();
        make_readonly(&noise);

        let emit = ["obj".to_string()];
        pre_clean_outputs(None, Some(dir.path()), Some("foo"), Some("-abc"), &emit);

        assert!(!cgu.exists(), "per-CGU object must be pre-cleaned");
        assert!(noise.exists(), "unrelated crate must not be touched");
    }

    #[test]
    fn pre_clean_probe_path_leaves_per_cgu_when_emit_is_link_only() {
        // Cargo-typical emit must not take the prefix-scan path.
        let dir = tempfile::tempdir().unwrap();
        let cgu = dir.path().join("foo-abc.foo.deadbeef12345678-cgu.0.rcgu.o");
        fs::write(&cgu, b"cached").unwrap();
        make_readonly(&cgu);

        let emit = [
            "link".to_string(),
            "metadata".to_string(),
            "dep-info".to_string(),
        ];
        pre_clean_outputs(None, Some(dir.path()), Some("foo"), Some("-abc"), &emit);

        assert!(
            cgu.exists(),
            "probe-only path must not readdir; per-CGU files stay (cargo never makes them)"
        );
    }

    #[test]
    fn pre_clean_and_discover_only_touch_candidates_amid_noise() {
        // Behavioral: noise files that are not candidates must survive, and
        // discover must return exactly the planted candidate set.
        let dir = tempfile::tempdir().unwrap();
        for i in 0..64 {
            fs::write(dir.path().join(format!("libnoise-{i:03}.rlib")), b"n").unwrap();
        }

        let rlib = dir.path().join("libhot-deadbeef.rlib");
        let rmeta = dir.path().join("libhot-deadbeef.rmeta");
        let dep = dir.path().join("hot-deadbeef.d");
        for path in [&rlib, &rmeta, &dep] {
            fs::write(path, b"cached").unwrap();
            make_readonly(path);
        }

        pre_clean_outputs(None, Some(dir.path()), Some("hot"), Some("-deadbeef"), &[]);

        assert!(!rlib.exists());
        assert!(!rmeta.exists());
        assert!(!dep.exists());
        assert!(dir.path().join("libnoise-000.rlib").exists());

        for path in [&rlib, &rmeta, &dep] {
            fs::write(path, b"fresh").unwrap();
        }
        let found =
            discover_output_files(None, Some(dir.path()), Some("hot"), Some("-deadbeef"), &[])
                .unwrap();
        let mut names: Vec<&str> = found.iter().map(|(_, n)| n.as_str()).collect();
        names.sort_unstable();
        assert_eq!(
            names,
            vec![
                "hot-deadbeef.d",
                "libhot-deadbeef.rlib",
                "libhot-deadbeef.rmeta"
            ]
        );
    }

    #[test]
    fn pre_clean_o_mode_probes_stem_siblings_without_scan() {
        let dir = tempfile::tempdir().unwrap();
        for i in 0..64 {
            fs::write(dir.path().join(format!("zzz-noise-{i}")), b"n").unwrap();
        }
        let rlib = dir.path().join("libfoo-abc.rlib");
        let rmeta = dir.path().join("libfoo-abc.rmeta");
        let dep = dir.path().join("foo-abc.d");
        for path in [&rlib, &rmeta, &dep] {
            fs::write(path, b"cached").unwrap();
            make_readonly(path);
        }

        pre_clean_outputs(Some(&rlib), None, Some("foo"), Some("-abc"), &[]);

        assert!(!rlib.exists());
        assert!(!rmeta.exists());
        assert!(!dep.exists());
        assert!(dir.path().join("zzz-noise-0").exists());
    }

    #[test]
    fn discover_out_dir_without_crate_name_finds_nothing() {
        // Incomplete identity: out-dir alone must not invent deletions or
        // discoveries (no delete-everything, no silent sweep).
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("libfoo-abc.rlib"), b"x").unwrap();

        let found = discover_output_files(None, Some(dir.path()), None, Some("-abc"), &[]).unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn pre_clean_and_discover_with_none_extra_filename() {
        let dir = tempfile::tempdir().unwrap();
        let rlib = dir.path().join("libapp.rlib");
        let dep = dir.path().join("app.d");
        for path in [&rlib, &dep] {
            fs::write(path, b"cached").unwrap();
            make_readonly(path);
        }

        pre_clean_outputs(None, Some(dir.path()), Some("app"), None, &[]);
        assert!(!rlib.exists());
        assert!(!dep.exists());

        fs::write(&rlib, b"fresh").unwrap();
        fs::write(&dep, b"fresh").unwrap();
        let found = discover_output_files(None, Some(dir.path()), Some("app"), None, &[]).unwrap();
        let mut names: Vec<&str> = found.iter().map(|(_, n)| n.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, vec!["app.d", "libapp.rlib"]);
    }

    #[test]
    fn pre_clean_skips_writable_out_dir_candidates() {
        let dir = tempfile::tempdir().unwrap();
        let rlib = dir.path().join("libfoo-abc.rlib");
        fs::write(&rlib, b"fresh").unwrap();
        // writable — not a kache hardlink
        assert!(!fs::metadata(&rlib).unwrap().permissions().readonly());

        pre_clean_outputs(None, Some(dir.path()), Some("foo"), Some("-abc"), &[]);
        assert!(rlib.exists(), "writable candidates must not be removed");
    }
}
