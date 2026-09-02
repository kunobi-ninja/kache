//! Cross-worktree regression for #760.
//!
//! A proc macro can expand to an absolute `include_bytes!` path elsewhere in
//! the workspace. Rustc reports that path in dep-info, so a relocated Kache hit
//! must rebase it to the consuming worktree. Otherwise Cargo watches the donor
//! and can keep a stale rlib `Fresh` after the consumer's asset changes.

use serde_json::Value;
use std::path::Path;
use std::process::{Command, Output};
use std::time::Duration;
use tempfile::TempDir;

mod common;
use common::{build_kache, hermetic_command, isolated_config_path, kache_binary};

fn write_package(root: &Path, package: &str, manifest: &str, source: &str, name: &str) {
    let package = root.join(package);
    std::fs::create_dir_all(package.join("src")).unwrap();
    std::fs::write(package.join("Cargo.toml"), manifest).unwrap();
    std::fs::write(package.join("src").join(name), source).unwrap();
}

fn write_workspace(root: &Path) {
    std::fs::write(
        root.join("Cargo.toml"),
        r#"[workspace]
members = ["embed-macro", "embedded", "app"]
resolver = "2"
"#,
    )
    .unwrap();

    write_package(
        root,
        "embed-macro",
        r#"[package]
name = "embed-macro"
version = "0.1.0"
edition = "2021"

[lib]
proc-macro = true
"#,
        r#"use proc_macro::TokenStream;

#[proc_macro]
pub fn embed(_input: TokenStream) -> TokenStream {
    let root = std::env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir");
    let path = std::path::PathBuf::from(root)
        .parent()
        .expect("workspace root")
        .join("shared/asset.txt")
        .canonicalize()
        .expect("workspace asset");
    format!("include_bytes!({:?})", path.to_string_lossy())
        .parse()
        .expect("include_bytes expansion")
}
"#,
        "lib.rs",
    );

    write_package(
        root,
        "embedded",
        r#"[package]
name = "embedded"
version = "0.1.0"
edition = "2021"

[dependencies]
embed-macro = { path = "../embed-macro" }
"#,
        r#"pub fn value() -> &'static str {
    std::str::from_utf8(embed_macro::embed!()).expect("UTF-8 fixture")
}
"#,
        "lib.rs",
    );
    std::fs::create_dir_all(root.join("shared")).unwrap();
    std::fs::write(root.join("shared/asset.txt"), "alpha").unwrap();

    write_package(
        root,
        "app",
        r#"[package]
name = "app"
version = "0.1.0"
edition = "2021"

[dependencies]
embedded = { path = "../embedded" }
"#,
        "fn main() { print!(\"{}\", embedded::value()); }\n",
        "main.rs",
    );
}

fn cargo_build(workspace: &Path, target: &Path, cache: &Path) -> Output {
    hermetic_command("cargo", cache, Some(&isolated_config_path(cache)))
        .args(["build", "--offline", "--workspace", "--verbose"])
        .current_dir(workspace)
        .env("RUSTC_WRAPPER", kache_binary())
        .env("CARGO_TARGET_DIR", target)
        .env("CARGO_INCREMENTAL", "0")
        .env("CARGO_TERM_COLOR", "never")
        .env("KACHE_BASE_DIR", workspace)
        .env("KACHE_LOG", "off")
        .env_remove("RUSTC_WORKSPACE_WRAPPER")
        .env_remove("KACHE_DISABLED")
        .output()
        .expect("run Cargo through Kache")
}

fn assert_build_succeeded(output: &Output) {
    assert!(
        output.status.success(),
        "Cargo build failed.\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn run_app(target: &Path) -> String {
    let mut binary = target.join("debug/app");
    if cfg!(windows) {
        binary.set_extension("exe");
    }
    let output = Command::new(&binary).output().unwrap();
    assert!(output.status.success(), "{} failed", binary.display());
    String::from_utf8(output.stdout).unwrap()
}

fn crate_events(cache: &Path, crate_name: &str) -> Vec<Value> {
    std::fs::read_to_string(cache.join("events.jsonl"))
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|event| event["crate_name"] == crate_name)
        .collect()
}

#[test]
fn restored_depinfo_tracks_the_consuming_worktree() {
    build_kache();
    let root = TempDir::new().unwrap();
    let workspace_a = root.path().join("worktree-a");
    let workspace_b = root.path().join("worktree-b");
    let target_a = workspace_a.join("target");
    let target_b = workspace_b.join("target");
    let cache = root.path().join("cache");
    std::fs::create_dir_all(&workspace_a).unwrap();
    std::fs::create_dir_all(&workspace_b).unwrap();
    write_workspace(&workspace_a);
    write_workspace(&workspace_b);

    let donor = cargo_build(&workspace_b, &target_b, &cache);
    assert_build_succeeded(&donor);
    assert_eq!(run_app(&target_b), "alpha");

    let relocated = cargo_build(&workspace_a, &target_a, &cache);
    assert_build_succeeded(&relocated);
    assert_eq!(run_app(&target_a), "alpha");
    let relocated_events = crate_events(&cache, "embedded");
    assert_eq!(relocated_events.len(), 2, "events: {relocated_events:#?}");
    assert_eq!(relocated_events[1]["result"], "local_hit");

    let unchanged = cargo_build(&workspace_a, &target_a, &cache);
    assert_build_succeeded(&unchanged);
    assert_eq!(run_app(&target_a), "alpha");
    assert_eq!(
        crate_events(&cache, "embedded").len(),
        2,
        "consumer-rooted dep-info should let Cargo keep an unchanged unit Fresh"
    );

    // Ensure Cargo's mtime freshness check can observe the edit even on
    // filesystems with coarse timestamp granularity.
    std::thread::sleep(Duration::from_millis(1100));
    std::fs::write(workspace_a.join("shared/asset.txt"), "bravo").unwrap();

    let changed = cargo_build(&workspace_a, &target_a, &cache);
    assert_build_succeeded(&changed);
    assert_eq!(
        run_app(&target_a),
        "bravo",
        "Cargo watched the donor worktree and retained a stale embedded rlib"
    );

    let changed_events = crate_events(&cache, "embedded");
    assert_eq!(changed_events.len(), 3, "events: {changed_events:#?}");
    assert_ne!(changed_events[2]["result"], "local_hit");
    assert_ne!(
        changed_events[2]["cache_key"], relocated_events[1]["cache_key"],
        "changing the included asset must re-key the consumer"
    );
}
