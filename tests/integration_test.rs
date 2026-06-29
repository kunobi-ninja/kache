use assert_cmd::Command;
use std::path::{Path, PathBuf};
use std::sync::{Once, OnceLock};
use tempfile::TempDir;

fn kache_binary() -> PathBuf {
    if let Some(path) = KACHE_BIN.get() {
        return path.clone();
    }
    let mut path = std::env::current_exe().unwrap();
    path.pop(); // remove test binary name
    path.pop(); // remove deps/
    path.push("kache");
    path
}

static KACHE_BIN: OnceLock<PathBuf> = OnceLock::new();

fn build_kache() {
    static BUILD: Once = Once::new();
    BUILD.call_once(|| {
        // Bootstrap the binary under test without any configured wrapper. The wrapper
        // behavior is exercised below by setting RUSTC_WRAPPER to this fresh binary.
        let target_dir =
            std::env::temp_dir().join(format!("kache-integration-target-{}", std::process::id()));
        let status = std::process::Command::new("cargo")
            .args([
                "build",
                "--bin",
                "kache",
                "--target-dir",
                target_dir.to_str().unwrap(),
                "--config",
                "build.rustc-wrapper=\"\"",
            ])
            .env_remove("RUSTC_WRAPPER")
            .env_remove("CARGO_BUILD_RUSTC_WRAPPER")
            .status()
            .expect("failed to build kache");
        assert!(status.success(), "kache build failed");

        let mut bin = target_dir.join("debug").join("kache");
        if cfg!(windows) {
            bin.set_extension("exe");
        }
        KACHE_BIN.set(bin).ok();
    });
}

fn isolated_config_path(cache_dir: &Path) -> PathBuf {
    // Prevent tests from inheriting the developer's real ~/.config/kache/config.toml.
    cache_dir.join("config.toml")
}

fn run_kache_cc(project: &Path, cache_dir: &Path, args: &[&str]) {
    run_kache_cc_from(project, cache_dir, args);
}

fn run_kache_cc_from(cwd: &Path, cache_dir: &Path, args: &[&str]) {
    let output = std::process::Command::new(kache_binary())
        .args(args)
        .current_dir(cwd)
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", isolated_config_path(cache_dir))
        .env("KACHE_LOG", "kache=debug")
        .output()
        .expect("failed to run kache cc");

    assert!(
        output.status.success(),
        "kache cc failed.\nargs: {args:?}\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn run_cargo_build_with_kache(project: &Path, cache_dir: &Path, target_dir: &Path) {
    let output = std::process::Command::new("cargo")
        .args(["build", "--lib"])
        .current_dir(project)
        .env("RUSTC_WRAPPER", kache_binary())
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", isolated_config_path(cache_dir))
        .env("CARGO_TARGET_DIR", target_dir)
        .env("CARGO_INCREMENTAL", "0")
        .env("KACHE_LOG", "kache=debug")
        .output()
        .expect("failed to run cargo build with kache");

    assert!(
        output.status.success(),
        "cargo build failed.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn run_cargo_test_with_kache(project: &Path, cache_dir: &Path, target_dir: &Path, package: &str) {
    let output = std::process::Command::new("cargo")
        .args(["test", "-q", "-p", package])
        .current_dir(project)
        .env("RUSTC_WRAPPER", kache_binary())
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", isolated_config_path(cache_dir))
        .env("CARGO_TARGET_DIR", target_dir)
        .env("CARGO_INCREMENTAL", "0")
        .env("KACHE_LOG", "kache=debug")
        .output()
        .expect("failed to run cargo test with kache");

    assert!(
        output.status.success(),
        "cargo test failed.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn kache_report(cache_dir: &Path) -> serde_json::Value {
    let output = std::process::Command::new(kache_binary())
        .args(["report", "--format", "json", "--since", "1h"])
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", isolated_config_path(cache_dir))
        .output()
        .expect("failed to run kache report");

    assert!(
        output.status.success(),
        "kache report failed.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    serde_json::from_slice(&output.stdout).expect("report should be valid json")
}

fn kache_perfetto_report(cache_dir: &Path, root: &Path) -> serde_json::Value {
    let output = std::process::Command::new(kache_binary())
        .arg("report")
        .args(["--format", "perfetto", "--since", "1h", "--root"])
        .arg(root)
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", isolated_config_path(cache_dir))
        .output()
        .expect("failed to run kache perfetto report");

    assert!(
        output.status.success(),
        "kache perfetto report failed.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    serde_json::from_slice(&output.stdout).expect("perfetto report should be valid json")
}

fn assert_cc_report_counts(report: &serde_json::Value, expected_misses: u64, expected_hits: u64) {
    let summary = &report["summary"];
    assert_eq!(summary["misses"].as_u64(), Some(expected_misses));
    assert_eq!(summary["local_hits"].as_u64(), Some(expected_hits));
}

fn assert_last_cc_event(report: &serde_json::Value, result: &str, compiler_runs: u64) {
    let events = report["all_events"]
        .as_array()
        .expect("report should include all_events");
    let last = events
        .last()
        .expect("report should include at least one event");
    assert_eq!(last["crate_name"].as_str(), Some("foo.c"));
    assert_eq!(last["result"].as_str(), Some(result));
    assert_eq!(last["compiler_runs"].as_u64(), Some(compiler_runs));
}

fn find_depinfo_containing(root: &Path, needle: &str) -> Option<(PathBuf, String)> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("d") {
                continue;
            }
            let Ok(content) = std::fs::read_to_string(&path) else {
                continue;
            };
            if content.contains(needle) {
                return Some((path, content));
            }
        }
    }
    None
}

fn write_manifest_dir_workspace(root: &Path) {
    std::fs::create_dir_all(root.join("helper/src")).unwrap();
    std::fs::create_dir_all(root.join("consumer/src")).unwrap();
    std::fs::create_dir_all(root.join("consumer/tests")).unwrap();

    std::fs::write(
        root.join("Cargo.toml"),
        r#"[workspace]
members = ["helper", "consumer"]
resolver = "3"
"#,
    )
    .unwrap();
    std::fs::write(
        root.join("helper/Cargo.toml"),
        r#"[package]
name = "helper"
version = "0.1.0"
edition = "2024"

[lib]
path = "src/lib.rs"
"#,
    )
    .unwrap();
    std::fs::write(
        root.join("helper/src/lib.rs"),
        r#"#[inline(never)]
pub fn manifest_dir() -> &'static str {
    env!("CARGO_MANIFEST_DIR")
}
"#,
    )
    .unwrap();
    std::fs::write(
        root.join("consumer/Cargo.toml"),
        r#"[package]
name = "consumer"
version = "0.1.0"
edition = "2024"

[dependencies]
helper = { path = "../helper" }
"#,
    )
    .unwrap();
    std::fs::write(
        root.join("consumer/src/lib.rs"),
        r#"pub fn helper_manifest_dir() -> &'static str {
    helper::manifest_dir()
}
"#,
    )
    .unwrap();
    std::fs::write(
        root.join("consumer/tests/manifest.rs"),
        r#"use std::path::Path;

#[test]
fn helper_manifest_dir_matches_this_checkout() {
    let embedded = Path::new(consumer::helper_manifest_dir()).canonicalize().unwrap();
    let expected = std::env::current_dir()
        .unwrap()
        .parent()
        .unwrap()
        .join("helper")
        .canonicalize()
        .unwrap();
    assert_eq!(embedded, expected);
}
"#,
    )
    .unwrap();
}

#[test]
fn test_cli_version_matches_package_version() {
    build_kache();
    assert_ne!(
        env!("CARGO_PKG_VERSION"),
        "0.0.0",
        "release builds must not use the placeholder package version"
    );

    Command::new(kache_binary())
        .arg("--version")
        .assert()
        .success()
        .stdout(predicates::str::contains(format!(
            "kache {}",
            env!("CARGO_PKG_VERSION")
        )));
}

#[test]
fn test_cli_help() {
    build_kache();
    Command::new(kache_binary())
        .arg("--help")
        .assert()
        .success()
        .stdout(predicates::str::contains("kache"));
}

#[test]
fn test_cli_list_empty() {
    build_kache();
    let cache_dir = TempDir::new().unwrap();

    Command::new(kache_binary())
        .arg("list")
        .env("KACHE_CACHE_DIR", cache_dir.path())
        .env("KACHE_CONFIG", isolated_config_path(cache_dir.path()))
        .assert()
        .success()
        .stdout(predicates::str::contains("No cached entries"));
}

#[test]
fn test_cli_purge_empty() {
    build_kache();
    let cache_dir = TempDir::new().unwrap();

    Command::new(kache_binary())
        .arg("purge")
        .env("KACHE_CACHE_DIR", cache_dir.path())
        .env("KACHE_CONFIG", isolated_config_path(cache_dir.path()))
        .assert()
        .success()
        .stdout(predicates::str::contains("Cleared"));
}

#[test]
fn test_disabled_passthrough() {
    build_kache();

    let test_project = Path::new(env!("CARGO_MANIFEST_DIR")).join("test-projects/hello-world");
    let cache_dir = TempDir::new().unwrap();
    let target_dir = TempDir::new().unwrap();

    // Build with kache disabled — should work just like normal cargo
    let status = std::process::Command::new("cargo")
        .args(["build"])
        .current_dir(&test_project)
        .env("RUSTC_WRAPPER", kache_binary())
        .env("KACHE_DISABLED", "1")
        .env("KACHE_CACHE_DIR", cache_dir.path())
        .env("KACHE_CONFIG", isolated_config_path(cache_dir.path()))
        .env("CARGO_TARGET_DIR", target_dir.path())
        .status()
        .expect("failed to run cargo build with kache disabled");

    assert!(
        status.success(),
        "cargo build with KACHE_DISABLED should succeed"
    );
}

#[test]
fn test_wrapper_hello_world() {
    build_kache();

    let test_project = Path::new(env!("CARGO_MANIFEST_DIR")).join("test-projects/hello-world");
    let cache_dir = TempDir::new().unwrap();
    let target_dir = TempDir::new().unwrap();

    // First build (should be all cache misses)
    let status = std::process::Command::new("cargo")
        .args(["build"])
        .current_dir(&test_project)
        .env("RUSTC_WRAPPER", kache_binary())
        .env("KACHE_CACHE_DIR", cache_dir.path())
        .env("KACHE_CONFIG", isolated_config_path(cache_dir.path()))
        .env("KACHE_EVENT_ROOT", &test_project)
        .env("CARGO_TARGET_DIR", target_dir.path())
        .env("KACHE_LOG", "kache=debug")
        .status()
        .expect("failed to run cargo build with kache");

    assert!(status.success(), "first build with kache should succeed");

    // Verify the binary was produced (`.exe` on Windows).
    assert!(
        target_dir
            .path()
            .join(format!("debug/hello-world{}", std::env::consts::EXE_SUFFIX))
            .exists(),
        "binary should be produced"
    );

    // Check that the store has entries
    let store_dir = cache_dir.path().join("store");
    if store_dir.exists() {
        let entries: Vec<_> = std::fs::read_dir(&store_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        // Should have at least one cached entry (the hello-world lib)
        println!("Store entries after first build: {}", entries.len());
    }

    // Clean and rebuild (should be cache hits)
    let _ = std::process::Command::new("cargo")
        .args(["clean"])
        .current_dir(&test_project)
        .env("CARGO_TARGET_DIR", target_dir.path())
        .status();

    let status = std::process::Command::new("cargo")
        .args(["build"])
        .current_dir(&test_project)
        .env("RUSTC_WRAPPER", kache_binary())
        .env("KACHE_CACHE_DIR", cache_dir.path())
        .env("KACHE_CONFIG", isolated_config_path(cache_dir.path()))
        .env("KACHE_EVENT_ROOT", &test_project)
        .env("CARGO_TARGET_DIR", target_dir.path())
        .env("KACHE_LOG", "kache=debug")
        .status()
        .expect("failed to run second cargo build with kache");

    assert!(status.success(), "second build (cache hit) should succeed");

    let trace = kache_perfetto_report(cache_dir.path(), &test_project);
    let trace_obj = trace.as_object().expect("trace report should be an object");
    assert_eq!(trace_obj.len(), 2);
    assert_eq!(trace["displayTimeUnit"].as_str(), Some("ms"));
    let trace_events = trace["traceEvents"]
        .as_array()
        .expect("trace report should include traceEvents");
    assert!(!trace_events.is_empty());
    // The trace leads with metadata events (ph "M": process_name / thread_name)
    // that name the kache process and worker lanes (#456); the compile slices
    // are ph "X". Assert on the first slice, not blindly on index 0.
    let first_slice = trace_events
        .iter()
        .find(|e| e["ph"].as_str() == Some("X"))
        .expect("trace report should include at least one X slice");
    assert_eq!(
        first_slice["args"]["root"].as_str(),
        Some(
            test_project
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .as_ref()
        )
    );
}

#[test]
fn test_manifest_dir_env_dep_does_not_restore_stale_rlib_across_worktrees() {
    build_kache();

    let root = TempDir::new().unwrap();
    let workspace_a = root.path().join("checkout-a");
    let workspace_b = root.path().join("checkout-b");
    write_manifest_dir_workspace(&workspace_a);
    write_manifest_dir_workspace(&workspace_b);

    let cache_dir = TempDir::new().unwrap();
    let target_a = TempDir::new().unwrap();
    let target_b = TempDir::new().unwrap();

    run_cargo_test_with_kache(&workspace_a, cache_dir.path(), target_a.path(), "consumer");
    let events_after_a = kache_report(cache_dir.path())["all_events"]
        .as_array()
        .expect("report should include all_events")
        .len();

    run_cargo_test_with_kache(&workspace_b, cache_dir.path(), target_b.path(), "consumer");
    let report = kache_report(cache_dir.path());
    let all_events = report["all_events"]
        .as_array()
        .expect("report should include all_events");
    let checkout_b_events = &all_events[events_after_a..];

    assert!(
        checkout_b_events
            .iter()
            .any(|event| event["crate_name"].as_str() == Some("helper")
                && event["result"].as_str() == Some("miss")),
        "helper embeds CARGO_MANIFEST_DIR and must miss in checkout B, not restore checkout A's rlib: {checkout_b_events:?}"
    );
}

#[test]
fn test_rust_depinfo_restore_preserves_include_str_parent_relative_path() {
    build_kache();

    let project = TempDir::new().unwrap();
    let cache_dir = TempDir::new().unwrap();
    let target_dir = TempDir::new().unwrap();
    std::fs::create_dir_all(project.path().join("src")).unwrap();
    std::fs::write(
        project.path().join("Cargo.toml"),
        r#"[package]
name = "kache-depinfo-repro"
version = "0.1.0"
edition = "2024"

[lib]
path = "src/lib.rs"

# Standalone workspace root so cargo never walks up into an ancestor workspace
# (e.g. when the system temp dir lives under one — common on Windows, where
# %TEMP% is under the user profile).
[workspace]
"#,
    )
    .unwrap();
    std::fs::write(
        project.path().join("README.md"),
        "included by the crate root\n",
    )
    .unwrap();
    std::fs::write(
        project.path().join("src/lib.rs"),
        r#"#![doc = include_str!("../README.md")]

pub fn value() -> u8 {
    1
}
"#,
    )
    .unwrap();

    run_cargo_build_with_kache(project.path(), cache_dir.path(), target_dir.path());
    std::fs::remove_dir_all(target_dir.path()).unwrap();
    run_cargo_build_with_kache(project.path(), cache_dir.path(), target_dir.path());

    // rustc joins the package-relative dir with the OS separator but keeps the
    // `include_str!` literal's own slashes verbatim, so the recorded path is
    // "src/../README.md" on Unix and "src\../README.md" on Windows.
    let sep = std::path::MAIN_SEPARATOR;
    let parent_rel = format!("src{sep}../README.md");
    let (depinfo_path, depinfo) = find_depinfo_containing(target_dir.path(), &parent_rel)
        .expect("restored target dir should contain rustc's parent-relative README.md dep-info");
    assert!(
        depinfo.contains(&parent_rel),
        "restored dep-info should preserve rustc's parent-relative include_str path in {}:\n{}",
        depinfo_path.display(),
        depinfo
    );
    assert!(
        !depinfo.contains(&format!("src{sep}./")),
        "restore must not inject the target dir into a parent-relative source path in {}:\n{}",
        depinfo_path.display(),
        depinfo
    );
    assert!(
        // Match either separator so a leaked sentinel can't slip through on
        // Windows (`__kache_root__\`).
        !depinfo.contains("__kache_root__"),
        "restored-facing dep-info must not expose kache sentinels in {}:\n{}",
        depinfo_path.display(),
        depinfo
    );

    let report = kache_report(cache_dir.path());
    assert!(
        report["summary"]["local_hits"].as_u64().unwrap_or(0) >= 1,
        "second build should restore at least one artifact from the local cache: {report}"
    );
}

#[test]
fn test_cc_depinfo_sidecar_restores_on_hit_and_new_mf_path() {
    build_kache();

    let project = TempDir::new().unwrap();
    let cache_dir = TempDir::new().unwrap();
    std::fs::create_dir_all(project.path().join("src")).unwrap();
    std::fs::create_dir_all(project.path().join("build")).unwrap();
    std::fs::write(
        project.path().join("src/bar.h"),
        "#define BAR_GREETING \"hello depinfo\"\n",
    )
    .unwrap();
    std::fs::write(
        project.path().join("src/foo.c"),
        "#include \"bar.h\"\nconst char *greeting(void) { return BAR_GREETING; }\n",
    )
    .unwrap();

    let base_args = [
        "cc",
        "-O0",
        "-g0",
        "-MMD",
        "-MP",
        "-Isrc",
        "-c",
        "src/foo.c",
        "-o",
        "build/foo.o",
    ];

    run_kache_cc(project.path(), cache_dir.path(), &base_args);
    let cold_depinfo = std::fs::read_to_string(project.path().join("build/foo.d")).unwrap();
    assert!(project.path().join("build/foo.o").exists());
    assert!(cold_depinfo.contains("build/foo.o"));
    assert!(cold_depinfo.contains("src/foo.c"));
    assert!(cold_depinfo.contains("src/bar.h"));
    assert!(!cold_depinfo.contains("./foo.o"));
    let report = kache_report(cache_dir.path());
    assert_cc_report_counts(&report, 1, 0);
    assert_last_cc_event(&report, "miss", 1);

    std::fs::remove_dir_all(project.path().join("build")).unwrap();
    run_kache_cc(project.path(), cache_dir.path(), &base_args);
    let warm_depinfo = std::fs::read_to_string(project.path().join("build/foo.d")).unwrap();
    assert!(project.path().join("build/foo.o").exists());
    assert_eq!(warm_depinfo, cold_depinfo);
    let report = kache_report(cache_dir.path());
    assert_cc_report_counts(&report, 1, 1);
    assert_last_cc_event(&report, "local_hit", 0);

    std::fs::remove_dir_all(project.path().join("build")).unwrap();
    let mf_args = [
        "cc",
        "-O0",
        "-g0",
        "-MMD",
        "-MP",
        "-MF",
        "deps/custom.d",
        "-Isrc",
        "-c",
        "src/foo.c",
        "-o",
        "build/foo.o",
    ];
    run_kache_cc(project.path(), cache_dir.path(), &mf_args);
    let mf_depinfo = std::fs::read_to_string(project.path().join("deps/custom.d")).unwrap();
    assert!(project.path().join("build/foo.o").exists());
    assert!(!project.path().join("build/foo.d").exists());
    assert_eq!(mf_depinfo, cold_depinfo);
    let report = kache_report(cache_dir.path());
    assert_cc_report_counts(&report, 1, 2);
    assert_last_cc_event(&report, "local_hit", 0);

    let pp_cache_dir = TempDir::new().unwrap();
    let _ = std::fs::remove_dir_all(project.path().join("build"));
    let _ = std::fs::remove_dir_all(project.path().join("deps"));
    std::fs::create_dir_all(project.path().join("build")).unwrap();
    std::fs::create_dir_all(project.path().join("deps")).unwrap();
    let pp_args = [
        "cc",
        "-O0",
        "-g0",
        "-MMD",
        "-MP",
        "-MF",
        "deps/custom.pp",
        "-Isrc",
        "-c",
        "src/foo.c",
        "-o",
        "build/foo.o",
    ];
    run_kache_cc(project.path(), pp_cache_dir.path(), &pp_args);
    let cold_pp_depinfo = std::fs::read_to_string(project.path().join("deps/custom.pp")).unwrap();
    assert!(project.path().join("build/foo.o").exists());
    assert!(cold_pp_depinfo.contains("build/foo.o"));
    assert!(cold_pp_depinfo.contains("src/foo.c"));
    assert!(cold_pp_depinfo.contains("src/bar.h"));
    let report = kache_report(pp_cache_dir.path());
    assert_cc_report_counts(&report, 1, 0);
    assert_last_cc_event(&report, "miss", 1);

    std::fs::remove_dir_all(project.path().join("build")).unwrap();
    std::fs::remove_dir_all(project.path().join("deps")).unwrap();
    run_kache_cc(project.path(), pp_cache_dir.path(), &pp_args);
    let warm_pp_depinfo = std::fs::read_to_string(project.path().join("deps/custom.pp")).unwrap();
    assert!(project.path().join("build/foo.o").exists());
    assert_eq!(warm_pp_depinfo, cold_pp_depinfo);
    let report = kache_report(pp_cache_dir.path());
    assert_cc_report_counts(&report, 1, 1);
    assert_last_cc_event(&report, "local_hit", 0);
}

#[test]
fn test_cc_depinfo_restore_preserves_parent_relative_deps() {
    build_kache();

    let project = TempDir::new().unwrap();
    let cache_dir = TempDir::new().unwrap();
    let source_dir = project.path().join("src");
    let object_dir = project.path().join("obj/a/b/c");
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::create_dir_all(object_dir.join(".deps")).unwrap();
    std::fs::write(source_dir.join("bar.h"), "#define VALUE 42\n").unwrap();
    std::fs::write(
        source_dir.join("foo.c"),
        "#include \"bar.h\"\nint answer(void) { return VALUE; }\n",
    )
    .unwrap();

    let args = [
        "cc",
        "-O0",
        "-g0",
        "-MMD",
        "-MP",
        "-MF",
        ".deps/foo.o.pp",
        "-I../../../../src",
        "-c",
        "../../../../src/foo.c",
        "-o",
        "foo.o",
    ];

    run_kache_cc_from(&object_dir, cache_dir.path(), &args);
    let cold_depinfo = std::fs::read_to_string(object_dir.join(".deps/foo.o.pp")).unwrap();
    assert!(object_dir.join("foo.o").exists());
    assert!(
        cold_depinfo.contains("../../../../src/foo.c"),
        "cold depfile should preserve compiler parent-relative source path: {cold_depinfo}"
    );
    assert!(
        cold_depinfo.contains("../../../../src/bar.h"),
        "cold depfile should preserve compiler parent-relative header path: {cold_depinfo}"
    );
    assert!(
        !cold_depinfo.contains("__kache_root__/"),
        "restored-facing depfiles must not expose kache sentinels: {cold_depinfo}"
    );

    std::fs::remove_file(object_dir.join("foo.o")).unwrap();
    std::fs::remove_dir_all(object_dir.join(".deps")).unwrap();
    std::fs::create_dir_all(object_dir.join(".deps")).unwrap();

    run_kache_cc_from(&object_dir, cache_dir.path(), &args);
    let warm_depinfo = std::fs::read_to_string(object_dir.join(".deps/foo.o.pp")).unwrap();
    assert!(object_dir.join("foo.o").exists());
    assert_eq!(
        warm_depinfo, cold_depinfo,
        "cache-hit restore must reproduce parent-relative depfiles byte-for-byte"
    );
    assert!(
        !warm_depinfo.contains(&object_dir.to_string_lossy().to_string()),
        "restore must not inject the object dir into parent-relative paths: {warm_depinfo}"
    );

    let report = kache_report(cache_dir.path());
    assert_cc_report_counts(&report, 1, 1);
}
