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

#[test]
fn compiler_queries_do_not_poison_offline_producer_identity() {
    let scratch = tempfile::tempdir().unwrap();
    let cache = scratch.path().join("client");
    let remote = scratch.path().join("remote");
    let workspace = scratch.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let out = workspace.join("target/debug/deps");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::write(
        workspace.join("Cargo.toml"),
        "[package]\nname=\"fixture\"\nversion=\"0.1.0\"\n",
    )
    .unwrap();
    std::fs::write(workspace.join("Cargo.lock"), "fixture lock contents").unwrap();
    std::fs::write(workspace.join("lib.rs"), "pub fn value() -> u32 { 42 }\n").unwrap();
    let config = scratch.path().join("config.toml");
    let path = |path: &Path| toml::Value::String(path.to_string_lossy().into_owned()).to_string();
    std::fs::write(&config, format!("[cache]\nignore_env=true\nlocal_only=true\nlocal_store={}\nruntime_dir={}\nprefetch_enabled=false\n[cache.remote]\ntype=\"filesystem\"\npath={}\nprefix=\"artifacts\"\n", path(&cache), path(&cache), path(&remote))).unwrap();
    let binary = Path::new(env!("CARGO_BIN_EXE_kache"));
    let command = || {
        let mut command = common::hermetic_command(binary, &cache, Some(&config));
        command
            .current_dir(&workspace)
            .env("KACHE_NAMESPACE", "org/repo")
            .env("KACHE_REPOSITORY", "org/repo")
            .env("KACHE_BUILD_SHAPE", "declared-fixture-shape")
            .env("KACHE_BUILD_TARGET", "declared-target")
            .env_remove("KACHE_DISABLED")
            .env_remove("GITHUB_ACTIONS")
            .env_remove("GITLAB_CI")
            .env_remove("CI");
        command
    };
    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let probe = command().arg(&rustc).arg("--print=cfg").output().unwrap();
    assert!(
        probe.status.success(),
        "{}",
        String::from_utf8_lossy(&probe.stderr)
    );
    assert!(
        !cache.join("build-report-contexts").exists(),
        "query froze unknown producer facts"
    );
    let compile = command()
        .arg(&rustc)
        .args(["--crate-name", "fixture", "--crate-type=lib", "--out-dir"])
        .arg(&out)
        .arg("lib.rs")
        .output()
        .unwrap();
    assert!(
        compile.status.success(),
        "{}",
        String::from_utf8_lossy(&compile.stderr)
    );
    let contexts = std::fs::read_dir(cache.join("build-report-contexts"))
        .unwrap()
        .map(|entry| {
            serde_json::from_slice::<Value>(&std::fs::read(entry.unwrap().path()).unwrap()).unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(contexts.len(), 1);
    assert_eq!(contexts[0]["identity"]["profile"], "debug");
    assert_eq!(contexts[0]["identity"]["target"], "declared-target");
    assert_eq!(
        contexts[0]["identity"]["build_shape"],
        "declared-fixture-shape"
    );
    assert_eq!(contexts[0]["identity"]["repository"], "org/repo");
    assert_eq!(
        contexts[0]["identity"]["lock_digest"],
        blake3::hash(b"fixture lock contents").to_hex().to_string()
    );
    assert_eq!(
        contexts[0]["identity"]["toolchain_hash"]
            .as_str()
            .unwrap()
            .len(),
        64
    );
    assert!(
        contexts[0]["facts"]["compiler_selector"]
            .as_str()
            .unwrap()
            .starts_with("rustc-ver-")
    );
    assert!(
        contexts[0]["facts"]["compiler_stamp"]["size"]
            .as_i64()
            .unwrap()
            > 0
    );
    assert_eq!(contexts[0]["facts"]["lock_stamp"]["size"], 21);

    // The five-minute marker intentionally coalesces adjacent commands. A
    // different declaration in that same session must make replay ineligible.
    let release = workspace.join("target/release/deps");
    std::fs::create_dir_all(&release).unwrap();
    let changed = command()
        .env("KACHE_BUILD_SHAPE", "different-shape")
        .env_remove("KACHE_NAMESPACE")
        .env_remove("KACHE_REPOSITORY")
        .arg(&rustc)
        .args([
            "--crate-name",
            "fixture_two",
            "--crate-type=lib",
            "--out-dir",
        ])
        .arg(&release)
        .arg("lib.rs")
        .output()
        .unwrap();
    assert!(
        changed.status.success(),
        "{}",
        String::from_utf8_lossy(&changed.stderr)
    );
    let invalid = std::fs::read_dir(cache.join("build-report-contexts"))
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|extension| extension == "invalid")
        })
        .count();
    assert_eq!(
        invalid, 1,
        "changed producer facts retained stale compatibility"
    );
    let config_text = std::fs::read_to_string(&config).unwrap();
    std::fs::write(&config, config_text.replace("local_only=true\n", "")).unwrap();
    let published = common::hermetic_command(binary, &cache, Some(&config))
        .current_dir(&workspace)
        .args(["save-manifest", "--manifest-key", "mixed-session"])
        .env_remove("KACHE_NAMESPACE")
        .env_remove("GITHUB_ACTIONS")
        .env_remove("GITLAB_CI")
        .env_remove("CI")
        .output()
        .unwrap();
    assert!(
        published.status.success(),
        "{}",
        String::from_utf8_lossy(&published.stderr)
    );
    let reports = std::fs::read_dir(remote.join("artifacts/_manifests/build-reports/v1/org/repo"))
        .unwrap()
        .map(|entry| {
            serde_json::from_slice::<Value>(&std::fs::read(entry.unwrap().path()).unwrap()).unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0]["entries"].as_array().unwrap().len(), 2);
    assert!(
        reports[0]["identity"]
            .as_object()
            .unwrap()
            .values()
            .all(Value::is_null)
    );
    assert!(reports[0]["commit"].is_null());
}
