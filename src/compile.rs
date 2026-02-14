use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Result of running rustc.
pub struct CompileResult {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    /// All output files produced by this compilation.
    /// Each entry is (absolute_path, filename_for_store).
    pub output_files: Vec<(PathBuf, String)>,
}

/// Run rustc with the given arguments, capturing all outputs.
pub fn run_rustc(
    rustc: &Path,
    args: &[String],
    output_path: Option<&Path>,
    out_dir: Option<&Path>,
    crate_name: Option<&str>,
    extra_filename: Option<&str>,
) -> Result<CompileResult> {
    // Pre-clean output paths: remove any read-only hardlinks left by a previous
    // kache cache hit. Without this, rustc cannot overwrite the 0444 hardlinked
    // files and fails with "output file is not writeable".
    pre_clean_outputs(output_path, out_dir, crate_name, extra_filename);

    let mut cmd = Command::new(rustc);

    // Add path remapping for reproducible builds across different project directories.
    // This makes debug info path-independent, enabling cross-user cache sharing.
    if let Ok(pwd) = std::env::current_dir() {
        cmd.arg(format!("--remap-path-prefix={}=.", pwd.display()));
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

    // Discover output files
    let output_files = if exit_code == 0 {
        discover_output_files(output_path, out_dir, crate_name, extra_filename)?
    } else {
        Vec::new()
    };

    Ok(CompileResult {
        exit_code,
        stdout,
        stderr,
        output_files,
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

/// Discover all output files from a compilation.
///
/// Rustc can produce multiple output files:
/// - `.rlib` (Rust library)
/// - `.rmeta` (metadata only)
/// - `.d` (dependency info)
/// - `.o` (object file)
/// - binary (no extension on Unix)
/// - `.dylib` / `.so` / `.dll` (dynamic library)
///
/// We find them via two paths:
/// 1. `-o path`: look at the output file and siblings with the same stem
/// 2. `--out-dir dir`: scan the directory for files matching `{crate_name}{extra_filename}.*`
fn discover_output_files(
    output_path: Option<&Path>,
    out_dir: Option<&Path>,
    crate_name: Option<&str>,
    extra_filename: Option<&str>,
) -> Result<Vec<(PathBuf, String)>> {
    let mut files = Vec::new();

    if let Some(output) = output_path {
        // -o mode: discover primary output and siblings with same stem
        if output.exists() {
            let filename = output
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();
            files.push((output.to_path_buf(), filename));
        }

        if let Some(parent) = output.parent()
            && let Some(stem) = output.file_stem()
        {
            let stem_str = stem.to_string_lossy();
            let output_filename = output
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();

            if let Ok(entries) = std::fs::read_dir(parent) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    let name = path
                        .file_name()
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_default();

                    if name == output_filename {
                        continue;
                    }

                    if name.starts_with(&*stem_str) {
                        files.push((path, name));
                    }
                }
            }
        }

        // Also check for dep-info files with the crate name pattern
        if let (Some(parent), Some(name), Some(extra)) =
            (output.parent(), crate_name, extra_filename)
        {
            let d_file = parent.join(format!("{name}{extra}.d"));
            if d_file.exists() && !files.iter().any(|(p, _)| p == &d_file) {
                let filename = d_file
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default();
                files.push((d_file, filename));
            }
        }
    } else if let (Some(dir), Some(name)) = (out_dir, crate_name) {
        // --out-dir mode: scan directory for files matching the crate
        // Cargo uses patterns like: lib{name}{extra}.rlib, {name}{extra}.d
        let extra = extra_filename.unwrap_or("");
        let prefixes = [format!("lib{name}{extra}"), format!("{name}{extra}")];

        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if !path.is_file() {
                    continue;
                }
                let fname = path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default();

                let matches = prefixes
                    .iter()
                    .any(|prefix| fname == *prefix || fname.starts_with(&format!("{prefix}.")));

                if matches {
                    files.push((path, fname));
                }
            }
        }
    }

    Ok(files)
}

/// Remove read-only files at output paths before rustc writes to them.
///
/// When kache restores a cache hit, it hardlinks store files (0444) into the
/// target directory. If a subsequent build is a cache miss for the same crate,
/// rustc tries to overwrite these paths but fails because the hardlinked files
/// are read-only. This function removes them so rustc can create fresh files.
fn pre_clean_outputs(
    output_path: Option<&Path>,
    out_dir: Option<&Path>,
    crate_name: Option<&str>,
    extra_filename: Option<&str>,
) {
    if let Some(output) = output_path {
        remove_if_readonly(output);

        // Also clean sibling files with the same stem (e.g., .rmeta alongside .rlib)
        if let (Some(parent), Some(stem)) = (output.parent(), output.file_stem()) {
            let stem_str = stem.to_string_lossy();
            if let Ok(entries) = std::fs::read_dir(parent) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path == *output {
                        continue;
                    }
                    if let Some(name) = path.file_name()
                        && name.to_string_lossy().starts_with(&*stem_str)
                    {
                        remove_if_readonly(&path);
                    }
                }
            }
        }

        // Check for dep-info files with crate name pattern
        if let (Some(parent), Some(name), Some(extra)) =
            (output.parent(), crate_name, extra_filename)
        {
            remove_if_readonly(&parent.join(format!("{name}{extra}.d")));
        }
    } else if let (Some(dir), Some(name)) = (out_dir, crate_name) {
        let extra = extra_filename.unwrap_or("");
        let prefixes = [format!("lib{name}{extra}"), format!("{name}{extra}")];

        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(fname) = path.file_name() {
                    let fname = fname.to_string_lossy();
                    if prefixes
                        .iter()
                        .any(|prefix| *fname == *prefix || fname.starts_with(&format!("{prefix}.")))
                    {
                        remove_if_readonly(&path);
                    }
                }
            }
        }
    }
}

/// Remove a file if it exists and is read-only (likely a kache hardlink).
fn remove_if_readonly(path: &Path) {
    if let Ok(meta) = std::fs::metadata(path)
        && meta.permissions().readonly()
    {
        let mut perms = meta.permissions();
        perms.set_readonly(false);
        let _ = std::fs::set_permissions(path, perms);
        let _ = std::fs::remove_file(path);
    }
}

/// Sign a binary with an ad-hoc signature on macOS Apple Silicon.
/// This is required for binaries restored from cache on arm64 macOS.
#[cfg(target_os = "macos")]
pub fn codesign_adhoc(path: &Path) -> Result<()> {
    // Only needed on arm64
    if std::env::consts::ARCH != "aarch64" {
        return Ok(());
    }

    let status = Command::new("codesign")
        .args(["--sign", "-", "--force"])
        .arg(path)
        .status()
        .context("running codesign")?;

    if !status.success() {
        tracing::warn!("ad-hoc codesign failed for {}", path.display());
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
pub fn codesign_adhoc(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn test_pre_clean_removes_readonly_output() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("libfoo-abc123.rlib");

        // Simulate a kache hardlink: create a read-only file
        fs::write(&output, b"cached content").unwrap();
        fs::set_permissions(&output, fs::Permissions::from_mode(0o444)).unwrap();
        assert!(fs::metadata(&output).unwrap().permissions().readonly());

        pre_clean_outputs(Some(&output), None, None, None);

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
            fs::set_permissions(path, fs::Permissions::from_mode(0o444)).unwrap();
        }

        pre_clean_outputs(Some(&rlib), None, Some("foo"), Some("-abc123"));

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

        pre_clean_outputs(Some(&output), None, None, None);

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
            fs::set_permissions(path, fs::Permissions::from_mode(0o444)).unwrap();
        }

        pre_clean_outputs(None, Some(dir.path()), Some("mycrate"), Some("-def456"));

        assert!(!rlib.exists());
        assert!(!rmeta.exists());
        assert!(
            unrelated.exists(),
            "unrelated crate files should not be removed"
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
        )
        .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_discover_output_files_out_dir_mode() {
        let dir = tempfile::tempdir().unwrap();
        let rlib = dir.path().join("libfoo-abc.rlib");
        let rmeta = dir.path().join("libfoo-abc.rmeta");
        let dep = dir.path().join("foo-abc.d");
        let unrelated = dir.path().join("libbar-xyz.rlib");

        for path in [&rlib, &rmeta, &dep, &unrelated] {
            fs::write(path, b"content").unwrap();
        }

        let files =
            discover_output_files(None, Some(dir.path()), Some("foo"), Some("-abc")).unwrap();
        let names: Vec<&str> = files.iter().map(|(_, n)| n.as_str()).collect();
        assert!(names.contains(&"libfoo-abc.rlib"));
        assert!(names.contains(&"libfoo-abc.rmeta"));
        assert!(names.contains(&"foo-abc.d"));
        assert!(!names.contains(&"libbar-xyz.rlib"));
    }
}
