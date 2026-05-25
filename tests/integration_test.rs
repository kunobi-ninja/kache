use assert_cmd::Command;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

fn kache_binary() -> PathBuf {
    let mut path = std::env::current_exe().unwrap();
    path.pop(); // remove test binary name
    path.pop(); // remove deps/
    path.push("kache");
    path
}

fn build_kache() {
    // Bootstrap the binary under test without any configured wrapper. The wrapper
    // behavior is exercised below by setting RUSTC_WRAPPER to this fresh binary.
    let status = std::process::Command::new("cargo")
        .args(["build", "--config", "build.rustc-wrapper=\"\""])
        .env_remove("RUSTC_WRAPPER")
        .env_remove("CARGO_BUILD_RUSTC_WRAPPER")
        .status()
        .expect("failed to build kache");
    assert!(status.success(), "kache build failed");
}

fn isolated_config_path(cache_dir: &Path) -> PathBuf {
    // Prevent tests from inheriting the developer's real ~/.config/kache/config.toml.
    cache_dir.join("config.toml")
}

fn run_kache_cc(project: &Path, cache_dir: &Path, args: &[&str]) {
    let output = std::process::Command::new(kache_binary())
        .args(args)
        .current_dir(project)
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
        .env("CARGO_TARGET_DIR", target_dir.path())
        .env("KACHE_LOG", "kache=debug")
        .status()
        .expect("failed to run cargo build with kache");

    assert!(status.success(), "first build with kache should succeed");

    // Verify the binary was produced
    assert!(
        target_dir.path().join("debug/hello-world").exists(),
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
        .env("CARGO_TARGET_DIR", target_dir.path())
        .env("KACHE_LOG", "kache=debug")
        .status()
        .expect("failed to run second cargo build with kache");

    assert!(status.success(), "second build (cache hit) should succeed");
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
}
