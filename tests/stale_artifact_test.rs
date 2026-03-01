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

#[test]
fn stale_invalidates_on_crate_root_edit() {
    build_kache();
    let project = copy_fixture();
    let cache_dir = TempDir::new().unwrap();
    let target_dir = TempDir::new().unwrap();

    // First build
    let out1 = build_and_run(project.path(), cache_dir.path(), target_dir.path(), &[], &[]);
    assert_eq!(out1, "v1.helper-v1.default.debug.plain");

    // Mutate: change value() return in lib.rs
    let lib_rs = project.path().join("src/lib.rs");
    let content = std::fs::read_to_string(&lib_rs).unwrap();
    std::fs::write(&lib_rs, content.replace("\"v1\"", "\"v2\"")).unwrap();

    // Clean and rebuild
    let _ = std::process::Command::new("cargo")
        .args(["clean"])
        .current_dir(project.path())
        .env("CARGO_TARGET_DIR", target_dir.path())
        .status();

    let out2 = build_and_run(project.path(), cache_dir.path(), target_dir.path(), &[], &[]);
    assert_eq!(out2, "v2.helper-v1.default.debug.plain");
    assert_ne!(out1, out2, "editing lib.rs must invalidate cache");
}

#[test]
fn stale_invalidates_on_module_edit() {
    build_kache();
    let project = copy_fixture();
    let cache_dir = TempDir::new().unwrap();
    let target_dir = TempDir::new().unwrap();

    let out1 = build_and_run(project.path(), cache_dir.path(), target_dir.path(), &[], &[]);
    assert_eq!(out1, "v1.helper-v1.default.debug.plain");

    // Mutate: change helper_value() return in helper.rs
    let helper_rs = project.path().join("src/helper.rs");
    let content = std::fs::read_to_string(&helper_rs).unwrap();
    std::fs::write(&helper_rs, content.replace("\"helper-v1\"", "\"helper-v2\"")).unwrap();

    let _ = std::process::Command::new("cargo")
        .args(["clean"])
        .current_dir(project.path())
        .env("CARGO_TARGET_DIR", target_dir.path())
        .status();

    let out2 = build_and_run(project.path(), cache_dir.path(), target_dir.path(), &[], &[]);
    assert_eq!(out2, "v1.helper-v2.default.debug.plain");
    assert_ne!(out1, out2, "editing a module file must invalidate cache");
}

#[test]
fn stale_invalidates_on_new_module() {
    build_kache();
    let project = copy_fixture();
    let cache_dir = TempDir::new().unwrap();
    let target_dir = TempDir::new().unwrap();

    let out1 = build_and_run(project.path(), cache_dir.path(), target_dir.path(), &[], &[]);
    assert_eq!(out1, "v1.helper-v1.default.debug.plain");

    // Mutate: add extra.rs module, change value() to call it
    std::fs::write(
        project.path().join("src/extra.rs"),
        "pub fn extra_value() -> &'static str { \"v2\" }\n",
    )
    .unwrap();

    let lib_rs = project.path().join("src/lib.rs");
    let content = std::fs::read_to_string(&lib_rs).unwrap();
    let content = content.replace(
        "mod helper;",
        "mod helper;\nmod extra;",
    );
    let content = content.replace(
        "\"v1\"",
        "extra::extra_value()",
    );
    std::fs::write(&lib_rs, content).unwrap();

    let _ = std::process::Command::new("cargo")
        .args(["clean"])
        .current_dir(project.path())
        .env("CARGO_TARGET_DIR", target_dir.path())
        .status();

    let out2 = build_and_run(project.path(), cache_dir.path(), target_dir.path(), &[], &[]);
    assert_eq!(out2, "v2.helper-v1.default.debug.plain");
    assert_ne!(out1, out2, "adding a new module must invalidate cache");
}

#[test]
fn stale_invalidates_on_feature_change() {
    build_kache();
    let project = copy_fixture();
    let cache_dir = TempDir::new().unwrap();
    let target_dir = TempDir::new().unwrap();

    // Build without features
    let out1 = build_and_run(project.path(), cache_dir.path(), target_dir.path(), &[], &[]);
    assert_eq!(out1, "v1.helper-v1.default.debug.plain");

    // Clean and rebuild WITH --features fancy
    let _ = std::process::Command::new("cargo")
        .args(["clean"])
        .current_dir(project.path())
        .env("CARGO_TARGET_DIR", target_dir.path())
        .status();

    let out2 = build_and_run(
        project.path(),
        cache_dir.path(),
        target_dir.path(),
        &[],
        &["--features", "fancy"],
    );
    assert_eq!(out2, "v1.helper-v1.default.debug.fancy");
    assert_ne!(out1, out2, "feature flag change must invalidate cache");
}

#[test]
fn stale_invalidates_on_rustflags_change() {
    build_kache();
    let project = copy_fixture();
    let cache_dir = TempDir::new().unwrap();
    let target_dir = TempDir::new().unwrap();

    // Build with default flags
    let out1 = build_and_run(project.path(), cache_dir.path(), target_dir.path(), &[], &[]);
    assert_eq!(out1, "v1.helper-v1.default.debug.plain");
    let count1 = store_entry_count(cache_dir.path());

    // Clean and rebuild with different RUSTFLAGS
    let _ = std::process::Command::new("cargo")
        .args(["clean"])
        .current_dir(project.path())
        .env("CARGO_TARGET_DIR", target_dir.path())
        .status();

    let out2 = build_and_run(
        project.path(),
        cache_dir.path(),
        target_dir.path(),
        &[("RUSTFLAGS", "-C opt-level=1")],
        &[],
    );
    let count2 = store_entry_count(cache_dir.path());

    // Output may or may not differ, but store MUST have more entries (different cache key)
    assert!(
        count2 > count1,
        "RUSTFLAGS change must produce new cache entry: before={count1}, after={count2}"
    );
    // Build still succeeds and produces valid output
    assert!(out2.contains("v1"), "output should still contain base value");
}

#[test]
fn stale_invalidates_on_profile_change() {
    build_kache();
    let project = copy_fixture();
    let cache_dir = TempDir::new().unwrap();
    let target_dir = TempDir::new().unwrap();

    // Build debug
    let out1 = build_and_run(project.path(), cache_dir.path(), target_dir.path(), &[], &[]);
    assert_eq!(out1, "v1.helper-v1.default.debug.plain");

    // Build release (no need to clean — different output dir)
    let out2 = build_and_run(
        project.path(),
        cache_dir.path(),
        target_dir.path(),
        &[],
        &["--release"],
    );
    assert_eq!(out2, "v1.helper-v1.default.release.plain");
    assert_ne!(out1, out2, "release build must not serve debug cached artifact");
}

#[test]
fn stale_recovers_from_corrupted_artifact() {
    build_kache();
    let project = copy_fixture();
    let cache_dir = TempDir::new().unwrap();
    let target_dir = TempDir::new().unwrap();

    // First build — populates cache
    let out1 = build_and_run(project.path(), cache_dir.path(), target_dir.path(), &[], &[]);
    assert_eq!(out1, "v1.helper-v1.default.debug.plain");

    // Corrupt: delete an rlib from the store
    let store_dir = cache_dir.path().join("store");
    let mut deleted = false;
    for entry in std::fs::read_dir(&store_dir).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            for file in std::fs::read_dir(entry.path()).unwrap() {
                let file = file.unwrap();
                let name = file.file_name();
                if name.to_string_lossy().ends_with(".rlib") {
                    // rlib files in the store are read-only; make writable before deleting
                    let mut perms = std::fs::metadata(file.path()).unwrap().permissions();
                    #[allow(clippy::permissions_set_readonly_false)]
                    perms.set_readonly(false);
                    std::fs::set_permissions(file.path(), perms).unwrap();
                    std::fs::remove_file(file.path()).unwrap();
                    deleted = true;
                    break;
                }
            }
            if deleted {
                break;
            }
        }
    }
    assert!(deleted, "should have found and deleted an rlib in the store");

    // Clean and rebuild — kache should detect the missing file and recompile
    let _ = std::process::Command::new("cargo")
        .args(["clean"])
        .current_dir(project.path())
        .env("CARGO_TARGET_DIR", target_dir.path())
        .status();

    let out2 = build_and_run(project.path(), cache_dir.path(), target_dir.path(), &[], &[]);
    assert_eq!(out2, "v1.helper-v1.default.debug.plain", "must recover from corrupted cache");
}

#[test]
fn stale_concurrent_builds_safe() {
    build_kache();
    let project = copy_fixture();
    let cache_dir = TempDir::new().unwrap();
    let target1 = TempDir::new().unwrap();
    let target2 = TempDir::new().unwrap();

    // Spawn two builds in parallel sharing the same cache dir
    let kache = kache_binary();
    let mut child1 = std::process::Command::new("cargo")
        .args(["build"])
        .current_dir(project.path())
        .env("RUSTC_WRAPPER", &kache)
        .env("KACHE_CACHE_DIR", cache_dir.path())
        .env("CARGO_TARGET_DIR", target1.path())
        .env("CARGO_INCREMENTAL", "0")
        .spawn()
        .expect("failed to spawn build 1");

    let mut child2 = std::process::Command::new("cargo")
        .args(["build"])
        .current_dir(project.path())
        .env("RUSTC_WRAPPER", &kache)
        .env("KACHE_CACHE_DIR", cache_dir.path())
        .env("CARGO_TARGET_DIR", target2.path())
        .env("CARGO_INCREMENTAL", "0")
        .spawn()
        .expect("failed to spawn build 2");

    let status1 = child1.wait().unwrap();
    let status2 = child2.wait().unwrap();

    assert!(status1.success(), "concurrent build 1 should succeed");
    assert!(status2.success(), "concurrent build 2 should succeed");

    // Both binaries should produce correct output
    let run = |target: &Path| -> String {
        let binary = target.join("debug/stale-check");
        let output = std::process::Command::new(&binary).output().unwrap();
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    };

    let out1 = run(target1.path());
    let out2 = run(target2.path());

    assert_eq!(out1, "v1.helper-v1.default.debug.plain");
    assert_eq!(out2, "v1.helper-v1.default.debug.plain");
    assert_eq!(out1, out2, "concurrent builds must produce identical output");
}

#[test]
fn stale_invalidates_on_env_change() {
    build_kache();
    let project = copy_fixture();
    let cache_dir = TempDir::new().unwrap();
    let target_dir = TempDir::new().unwrap();

    // Build without env var
    let out1 = build_and_run(project.path(), cache_dir.path(), target_dir.path(), &[], &[]);
    assert_eq!(out1, "v1.helper-v1.default.debug.plain");

    // Clean and rebuild WITH env var set
    let _ = std::process::Command::new("cargo")
        .args(["clean"])
        .current_dir(project.path())
        .env("CARGO_TARGET_DIR", target_dir.path())
        .status();

    let out2 = build_and_run(
        project.path(),
        cache_dir.path(),
        target_dir.path(),
        &[("KACHE_TEST_VALUE", "custom")],
        &[],
    );
    assert_eq!(out2, "v1.helper-v1.custom.debug.plain");
    assert_ne!(out1, out2, "env var change must invalidate cache");
}

#[test]
fn stale_invalidates_on_coverage_flag() {
    build_kache();
    let project = copy_fixture();
    let cache_dir = TempDir::new().unwrap();
    let target_dir = TempDir::new().unwrap();

    // Build without coverage
    let _out1 = build_and_run(project.path(), cache_dir.path(), target_dir.path(), &[], &[]);
    let count1 = store_entry_count(cache_dir.path());

    // Clean and rebuild with coverage instrumentation
    let _ = std::process::Command::new("cargo")
        .args(["clean"])
        .current_dir(project.path())
        .env("CARGO_TARGET_DIR", target_dir.path())
        .status();

    let _out2 = build_and_run(
        project.path(),
        cache_dir.path(),
        target_dir.path(),
        &[("RUSTFLAGS", "-Cinstrument-coverage")],
        &[],
    );
    let count2 = store_entry_count(cache_dir.path());

    assert!(
        count2 > count1,
        "coverage flag must produce new cache entry: before={count1}, after={count2}"
    );
}
