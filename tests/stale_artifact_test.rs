use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// Finds the kache binary in the test output directory.
fn kache_binary() -> PathBuf {
    let mut path = std::env::current_exe().unwrap();
    path.pop(); // remove test binary name
    path.pop(); // remove deps/
    path.push("kache");
    path
}

/// Runs `cargo build` to ensure kache is built.
fn build_kache() {
    let status = std::process::Command::new("cargo")
        .args(["build"])
        .status()
        .expect("failed to build kache");
    assert!(status.success(), "kache build failed");
}

/// Recursively copies a directory.
fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let ty = entry.file_type().unwrap();
        let dest_path = dst.join(entry.file_name());
        if ty.is_dir() {
            copy_dir(&entry.path(), &dest_path);
        } else {
            std::fs::copy(entry.path(), &dest_path).unwrap();
        }
    }
}

/// Copies `test-projects/stale-check/` to a temp dir and returns it.
fn copy_fixture() -> TempDir {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("test-projects/stale-check");
    let tmp = TempDir::new().unwrap();
    copy_dir(&fixture, tmp.path());
    tmp
}

/// Builds with kache as RUSTC_WRAPPER and runs the binary, returns stdout trimmed.
fn build_and_run(
    project: &Path,
    cache_dir: &Path,
    target_dir: &Path,
    extra_env: &[(&str, &str)],
    extra_cargo_args: &[&str],
) -> String {
    let mut cmd = std::process::Command::new("cargo");
    cmd.args(["build"])
        .current_dir(project)
        .env("RUSTC_WRAPPER", kache_binary())
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("CARGO_TARGET_DIR", target_dir)
        .env("CARGO_INCREMENTAL", "0")
        .env("KACHE_LOG", "kache=debug");
    for &(k, v) in extra_env {
        cmd.env(k, v);
    }
    for arg in extra_cargo_args {
        cmd.arg(arg);
    }

    let output = cmd.output().expect("failed to run cargo build");
    assert!(
        output.status.success(),
        "cargo build failed.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    // Determine binary path based on profile
    let binary = if extra_cargo_args.contains(&"--release") {
        target_dir.join("release/stale-check")
    } else {
        target_dir.join("debug/stale-check")
    };

    let run_output = std::process::Command::new(&binary)
        .output()
        .unwrap_or_else(|e| panic!("failed to run binary {}: {e}", binary.display()));

    String::from_utf8(run_output.stdout)
        .unwrap()
        .trim()
        .to_string()
}

/// Counts directories in `{cache_dir}/store/`.
fn store_entry_count(cache_dir: &Path) -> usize {
    let store_dir = cache_dir.join("store");
    if !store_dir.exists() {
        return 0;
    }
    std::fs::read_dir(&store_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .count()
}

#[test]
fn stale_cache_hit_baseline() {
    build_kache();
    let project = copy_fixture();
    let cache_dir = TempDir::new().unwrap();
    let target_dir = TempDir::new().unwrap();

    // First build -- populates cache
    let out1 = build_and_run(project.path(), cache_dir.path(), target_dir.path(), &[], &[]);
    assert_eq!(out1, "v1.helper-v1.default.debug.plain");

    let entries_after_first = store_entry_count(cache_dir.path());
    println!("Store entries after first build: {entries_after_first}");

    // Clean target (force cargo to re-invoke rustc through kache)
    let _ = std::process::Command::new("cargo")
        .args(["clean"])
        .current_dir(project.path())
        .env("CARGO_TARGET_DIR", target_dir.path())
        .status();

    // Second build -- should hit cache, same output
    let out2 = build_and_run(project.path(), cache_dir.path(), target_dir.path(), &[], &[]);
    assert_eq!(out2, "v1.helper-v1.default.debug.plain");
    assert_eq!(out1, out2, "cache hit should produce identical output");
}
