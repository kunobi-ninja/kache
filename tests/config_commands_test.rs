//! Scriptable configuration edits through the shipped frontend and engine.

use std::path::Path;
use std::process::{Command, Output};

fn run(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_kache"))
        .args(args)
        .current_dir(root)
        .env("HOME", root.join("home"))
        .env("XDG_CONFIG_HOME", root.join("user"))
        .env("KACHE_CONFIG", root.join("selected.toml"))
        .env("KACHE_HOST_CONFIG", "")
        .env("KACHE_CACHE_DIR", root.join("must-not-create-cache"))
        .env("KACHE_RUNTIME_DIR", root.join("runtime"))
        .env("KACHE_LOG", "off")
        .env_remove("KACHE_ENGINE")
        .env_remove("KACHE_ENGINE_PATH")
        .output()
        .unwrap()
}

fn success(root: &Path, args: &[&str]) -> String {
    let out = run(root, args);
    assert!(
        out.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

#[test]
fn config_commands_edit_only_the_requested_file_without_starting_the_daemon() {
    let root = tempfile::tempdir().unwrap();
    let root = root.path();
    assert_eq!(
        success(root, &["config", "path"]).trim(),
        root.join("selected.toml").to_str().unwrap()
    );
    assert!(!root.join("selected.toml").exists());
    success(root, &["config", "set", "cache.local_only", "true"]);
    assert_eq!(
        success(root, &["config", "get", "cache.local_only"]),
        "true\n"
    );
    success(
        root,
        &["config", "set", "--project", "cache.local_only", "false"],
    );
    success(
        root,
        &["config", "set", "--global", "cache.local_only", "false"],
    );
    success(
        root,
        &[
            "config",
            "set",
            "--file",
            "custom.toml",
            "cache.local_only",
            "false",
        ],
    );
    assert!(root.join(".kache.toml").exists());
    assert!(root.join("user/kache/config.toml").exists());
    assert!(root.join("custom.toml").exists());
    assert_eq!(
        success(root, &["config", "get", "cache.local_only"]),
        "true\n"
    );
    assert_eq!(
        success(root, &["config", "get", "--project", "cache.local_only"]),
        "false\n"
    );
    let output = success(root, &["config", "list", "--json"]);
    let json: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(json["success"], true);
    assert_eq!(json["command"], "config");
    assert_eq!(json["values"]["cache"]["local_only"], true);
    success(root, &["config", "unset", "cache.local_only"]);
    let out = run(root, &["config", "get", "cache.local_only", "--json"]);
    assert!(!out.status.success());
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(json["success"], false);
    assert!(!root.join("runtime").exists());
}

#[test]
fn project_scope_finds_the_nearest_existing_project_file() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join(".kache.toml"),
        "[cache]\nlocal_only = true\n",
    )
    .unwrap();
    let nested = root.path().join("src/nested");
    std::fs::create_dir_all(&nested).unwrap();
    assert_eq!(
        success(&nested, &["config", "get", "--project", "cache.local_only"]),
        "true\n"
    );
    assert!(
        !run(&nested, &["config", "path", "--global", "--project"])
            .status
            .success()
    );
}

#[test]
fn get_and_list_report_file_values_without_applying_environment_overrides() {
    let root = tempfile::tempdir().unwrap();
    success(root.path(), &["config", "set", "cache.local_only", "false"]);
    let output = Command::new(env!("CARGO_BIN_EXE_kache"))
        .args(["config", "get", "cache.local_only"])
        .env("KACHE_CONFIG", root.path().join("selected.toml"))
        .env("KACHE_LOCAL_ONLY", "1")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(output.stdout, b"false\n");
    assert_eq!(
        success(root.path(), &["config", "list"]),
        "cache.local_only = false\n"
    );
}

#[test]
fn every_config_subcommand_emits_one_json_document() {
    let root = tempfile::tempdir().unwrap();
    for args in [
        vec!["config", "path", "--json"],
        vec!["config", "list", "--json"],
        vec!["config", "set", "cache.local_only", "true", "--json"],
        vec!["config", "get", "cache.local_only", "--json"],
        vec!["config", "unset", "cache.local_only", "--json"],
    ] {
        let text = success(root.path(), &args);
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(json["schema_version"], 1);
        assert_eq!(json["success"], true);
        assert_eq!(
            json["path"],
            root.path().join("selected.toml").to_str().unwrap()
        );
        match args[1] {
            "get" => assert_eq!(json["value"], true),
            "set" | "unset" => assert_eq!(json["action"], args[1]),
            "list" => assert!(json["values"].is_object()),
            _ => {}
        }
    }
}

#[test]
fn config_list_reports_a_file_read_error_instead_of_empty_values() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("selected.toml")).unwrap();
    let out = run(root.path(), &["config", "list", "--json"]);
    assert!(!out.status.success());
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(json["success"], false);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("reading")
    );
}

#[test]
fn malformed_keys_report_a_key_error_and_legacy_keys_remain_accessible() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("selected.toml"),
        "[cache]\nfuture-setting0 = 7\n",
    )
    .unwrap();
    assert_eq!(
        success(root.path(), &["config", "get", "cache.future-setting0"]),
        "7\n"
    );
    for key in [
        "",
        "cache..local_only",
        "cache.é",
        "cache.[local_only]",
        "cache.local_only\n",
    ] {
        let out = run(root.path(), &["config", "get", key, "--json"]);
        assert!(!out.status.success());
        let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        assert!(
            json["error"]["message"]
                .as_str()
                .unwrap()
                .contains("dotted key")
        );
    }
    success(root.path(), &["config", "unset", "cache.future-setting0"]);
    assert!(
        !std::fs::read_to_string(root.path().join("selected.toml"))
            .unwrap()
            .contains("future-setting0")
    );
}
