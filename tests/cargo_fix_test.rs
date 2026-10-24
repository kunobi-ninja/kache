//! Cached metadata must remain writable when Cargo's fix driver bypasses kache.

#[allow(dead_code)]
mod common;

use std::fs;

#[test]
fn cached_check_outputs_allow_cargo_and_clippy_fix() {
    let kache = common::kache_binary();
    for (fix_command, source, fixed) in [
        (
            "fix",
            "pub fn value() -> u32 { let mut value = 1; value }\n",
            "let value = 1",
        ),
        (
            "clippy",
            "#![warn(clippy::needless_return)]\npub fn value() -> u32 { return 1; }\n",
            "{ 1}",
        ),
    ] {
        let dir = tempfile::tempdir_in(common::scratch_dir()).unwrap();
        let root = dir.path().canonicalize().unwrap();
        let project = root.join("project");
        let target = root.join("target");
        let cache = root.join("cache");
        let cargo_home = root.join("cargo-home");
        fs::create_dir_all(project.join("src")).unwrap();
        fs::create_dir_all(&cargo_home).unwrap();
        fs::write(project.join("Cargo.toml"),
            "[package]\nname = \"fix_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n").unwrap();
        let _daemon = common::DaemonGuard::new(&kache, &cache);
        let run = |args: &[&str]| {
            let output = common::hermetic_command(
                env!("CARGO"),
                &cache,
                Some(&common::isolated_config_path(&cache)),
            )
            .args(args)
            .arg("--offline")
            .current_dir(&project)
            .env("CARGO_HOME", &cargo_home)
            .env("CARGO_TARGET_DIR", &target)
            .env("CARGO_INCREMENTAL", "0")
            .env("RUSTC_WRAPPER", &kache)
            .env_remove("CARGO_BUILD_BUILD_DIR")
            .env_remove("RUSTC_WORKSPACE_WRAPPER")
            .env_remove("CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER")
            .env_remove("RUSTFLAGS")
            .env_remove("CARGO_ENCODED_RUSTFLAGS")
            .env_remove("CARGO_BUILD_RUSTFLAGS")
            .output()
            .unwrap();
            assert!(
                output.status.success(),
                "{args:?} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        };
        for warm in [false, true] {
            fs::write(project.join("src/lib.rs"), source).unwrap();
            if warm {
                fs::remove_dir_all(&target).unwrap();
            }
            common::settle_writes(&[&project]);
            run(&["check"]);
            let events: Vec<serde_json::Value> = fs::read_to_string(cache.join("events.jsonl"))
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .filter(|event: &serde_json::Value| event["crate_name"] == "fix_fixture")
                .collect();
            assert_eq!(
                events.last().unwrap()["result"],
                if warm { "local_hit" } else { "miss" },
                "{events:#?}"
            );
            if fix_command == "clippy" {
                run(&["clippy", "--fix", "--allow-dirty", "--allow-no-vcs"]);
            } else {
                run(&["fix", "--allow-dirty", "--allow-no-vcs"]);
            }
            let edited = fs::read_to_string(project.join("src/lib.rs")).unwrap();
            assert_ne!(edited, source);
            assert!(edited.contains(fixed), "unexpected source edit: {edited}");
        }
    }
}
