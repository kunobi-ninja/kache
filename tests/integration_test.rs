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
    let status = std::process::Command::new("cargo")
        .args(["build"])
        .status()
        .expect("failed to build kache");
    assert!(status.success(), "kache build failed");
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
        .env("CARGO_TARGET_DIR", target_dir.path())
        .env("KACHE_LOG", "kache=debug")
        .status()
        .expect("failed to run second cargo build with kache");

    assert!(status.success(), "second build (cache hit) should succeed");
}
