//! A fresh client reconstructs session history from the filesystem remote alone.

use serde_json::{Value, json};
use std::path::Path;

#[allow(dead_code)]
mod common;

fn event(session: &str, root: &str, key: char, finish: &str, elapsed: u64) -> Value {
    json!({"ts":finish, "crate_name":"serde", "root":root, "session_id":session,
        "result":"miss", "elapsed_ms":elapsed, "compile_time_ms":elapsed / 2,
        "size":1234, "cache_key":key.to_string().repeat(64), "schema":25})
}

fn publish(binary: &Path, config: &Path, cache: &Path, workspace: &Path) {
    let output = common::hermetic_command(binary, cache, Some(config))
        .current_dir(workspace)
        .args([
            "save-manifest",
            "--manifest-key",
            "same-build-identity",
            "--namespace",
            "org/repo",
        ])
        .env_remove("GITHUB_ACTIONS")
        .env_remove("GITLAB_CI")
        .env_remove("CI")
        .env_remove("KACHE_NAMESPACE")
        .env_remove("KACHE_DISABLED")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn save_manifest_retains_session_history_and_rehydrates_without_local_state() {
    let scratch = tempfile::tempdir().unwrap();
    let cache = scratch.path().join("client");
    let remote = scratch.path().join("remote");
    let workspace = scratch.path().join("workspace");
    std::fs::create_dir_all(&cache).unwrap();
    std::fs::create_dir_all(&workspace).unwrap();
    let config = scratch.path().join("config.toml");
    let path = |path: &Path| toml::Value::String(path.to_string_lossy().into_owned()).to_string();
    std::fs::write(&config, format!("[cache]\nignore_env=true\nlocal_store={}\nruntime_dir={}\n[cache.remote]\ntype=\"filesystem\"\npath={}\nprefix=\"artifacts\"\n", path(&cache), path(&cache), path(&remote))).unwrap();
    let root = workspace.to_string_lossy();
    let first = vec![
        event("session-one", &root, 'a', "2026-01-01T00:00:01.200Z", 200),
        event("session-one", &root, 'b', "2026-01-01T00:00:01.300Z", 10),
        event("session-one", &root, 'a', "2026-01-01T00:00:01.600Z", 2),
    ];
    let log = |events: &[Value]| {
        events
            .iter()
            .map(|event| format!("{event}\n"))
            .collect::<String>()
    };
    std::fs::write(cache.join("events.jsonl"), log(&first)).unwrap();
    let binary = std::env::var_os("KACHE_REPORT_TEST_BINARY")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| Path::new(env!("CARGO_BIN_EXE_kache")).to_path_buf());
    publish(&binary, &config, &cache, &workspace);
    let report_dir = remote.join("artifacts/_manifests/build-reports/v1/org/repo");
    assert!(
        report_dir.is_dir(),
        "save-manifest did not publish durable session reports"
    );
    let first_path = std::fs::read_dir(&report_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let first_bytes = std::fs::read(&first_path).unwrap();
    let first_report: Value = serde_json::from_slice(&first_bytes).unwrap();
    assert_eq!(first_report["schema"], 1);
    assert_eq!(first_report["session_id"], "session-one");
    assert_eq!(first_report["entries"].as_array().unwrap().len(), 3);
    assert_eq!(
        first_report["entries"][0]["cache_key"],
        first_report["entries"][2]["cache_key"]
    );
    assert_eq!(first_report["entries"][2]["start_offset_ms"], 598);
    assert_eq!(first_report["entries"][0]["artifact_status"], "observed");
    assert!(first_report["identity"]["toolchain_hash"].is_null());

    let mut second_log = first;
    second_log.push(event(
        "session-two",
        &root,
        'c',
        "2026-01-01T00:00:02.500Z",
        500,
    ));
    std::fs::write(cache.join("events.jsonl"), log(&second_log)).unwrap();
    publish(&binary, &config, &cache, &workspace);
    assert_eq!(std::fs::read(&first_path).unwrap(), first_bytes);
    let legacy: Value = serde_json::from_slice(
        &std::fs::read(remote.join("artifacts/_manifests/same-build-identity.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(legacy["entries"].as_array().unwrap().len(), 3);

    // Delete the producer event log, SQLite/cache state and all local metadata.
    std::fs::remove_dir_all(&cache).unwrap();
    let mut rebuilt = std::fs::read_dir(&report_dir)
        .unwrap()
        .map(|entry| {
            serde_json::from_slice::<Value>(&std::fs::read(entry.unwrap().path()).unwrap()).unwrap()
        })
        .collect::<Vec<_>>();
    rebuilt.sort_by_key(|report| report["session_id"].as_str().unwrap().to_string());
    assert_eq!(rebuilt.len(), 2);
    assert_eq!(rebuilt[0], first_report);
    assert_eq!(rebuilt[1]["session_id"], "session-two");
    assert_eq!(rebuilt[1]["entries"][0]["cache_key"], "c".repeat(64));
}
