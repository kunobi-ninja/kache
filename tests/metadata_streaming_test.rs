//! Metadata and diagnostics must survive both streaming misses and verified hits.

use serde_json::Value;
use std::fs;
use std::path::Path;

mod common;
use common::{build_kache, hermetic_command, isolated_config_path, kache_binary};

fn compile(project: &Path, cache: &Path, verify: bool, expected_result: &str) -> Vec<Value> {
    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let output = hermetic_command(kache_binary(), cache, Some(&isolated_config_path(cache)))
        .arg(rustc)
        .args([
            "--crate-name",
            "metadata_fixture",
            "--crate-type",
            "lib",
            "--edition=2021",
            "--emit=dep-info,metadata,link",
            "--error-format=json",
            "--json=artifacts",
            "--out-dir",
        ])
        .arg(project.join("out"))
        .arg(project.join("lib.rs"))
        .current_dir(project)
        .env("KACHE_VERIFY", if verify { "1" } else { "0" })
        .env("KACHE_LOG", "kache=info")
        .output()
        .unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(output.status.success(), "{stderr}");
    if verify {
        assert!(
            stderr.contains("KACHE_VERIFY:") && stderr.contains("a fresh compile"),
            "{stderr}"
        );
    }
    let events = fs::read_to_string(cache.join("events.jsonl")).unwrap();
    let event: Value = serde_json::from_str(events.lines().last().unwrap()).unwrap();
    assert_eq!(event["result"], expected_result);
    stderr
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

#[test]
fn miss_and_verified_hit_emit_metadata_once_and_preserve_large_diagnostics() {
    build_kache();
    let project = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    fs::create_dir(project.path().join("out")).unwrap();
    fs::write(
        isolated_config_path(cache.path()),
        "[cache]\nlocal_only = true\nscheduler = false\n",
    )
    .unwrap();
    let mut source = String::from("pub fn answer() -> u32 { 42 }\n");
    for index in 0..256 {
        source.push_str(&format!("fn unused_{index}() {{}}\n"));
    }
    fs::write(project.path().join("lib.rs"), source).unwrap();

    let miss = compile(project.path(), cache.path(), false, "miss");
    let metadata: Vec<_> = miss
        .iter()
        .filter(|message| message["emit"] == "metadata")
        .collect();
    assert_eq!(metadata.len(), 1);
    assert_eq!(
        metadata[0]["artifact"],
        project
            .path()
            .join("out")
            .join("libmetadata_fixture.rmeta")
            .to_string_lossy()
            .as_ref()
    );
    let diagnostics: Vec<_> = miss
        .iter()
        .filter(|message| message["$message_type"] == "diagnostic")
        .collect();
    assert!(diagnostics.len() >= 256);
    assert!(serde_json::to_vec(&diagnostics).unwrap().len() > 128 * 1024);

    for verify in [false, true] {
        fs::remove_dir_all(project.path().join("out")).unwrap();
        fs::create_dir(project.path().join("out")).unwrap();
        let hit = compile(project.path(), cache.path(), verify, "local_hit");
        let hit_metadata: Vec<_> = hit
            .iter()
            .filter(|message| message["emit"] == "metadata")
            .collect();
        assert_eq!(
            hit_metadata, metadata,
            "verify metadata must not leak staging paths"
        );
        let hit_diagnostics: Vec<_> = hit
            .iter()
            .filter(|message| message["$message_type"] == "diagnostic")
            .collect();
        assert_eq!(hit_diagnostics, diagnostics);
    }
}

#[test]
fn changing_diagnostic_format_preserves_requested_output_and_reuse() {
    build_kache();
    let fixture = tempfile::tempdir_in(common::scratch_dir()).unwrap();
    let project = fixture.path();
    let cache = project.join("cache");
    let home = project.join("home");
    let out = project.join("out");
    fs::create_dir(&cache).unwrap();
    fs::create_dir(&home).unwrap();
    fs::create_dir(&out).unwrap();
    let config = isolated_config_path(&cache);
    fs::write(
        &config,
        "[cache]\nlocal_only = true\nscheduler = false\ndeferred_discovery = false\nauto_gc = false\ndaemon_publish = false\n",
    )
    .unwrap();
    fs::write(
        project.join("lib.rs"),
        "pub fn answer() -> u32 { let unused = 1; 42 }\n",
    )
    .unwrap();
    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let rustup_home = std::env::var_os("RUSTUP_HOME")
        .unwrap_or_else(|| dirs::home_dir().unwrap().join(".rustup").into_os_string());
    let run = |wrapped: bool, format: &str| {
        let mut command = if wrapped {
            let mut command = hermetic_command(kache_binary(), &cache, Some(&config));
            command.arg(&rustc);
            command
        } else {
            hermetic_command(&rustc, &cache, Some(&config))
        };
        let output = command
            .args([
                "--crate-name",
                "format_fixture",
                "--crate-type=rlib",
                "--emit=dep-info,metadata,link",
                "--out-dir",
            ])
            .arg(&out)
            .arg(project.join("lib.rs"))
            .arg(format!("--error-format={format}"))
            .env("HOME", &home)
            .env("RUSTUP_HOME", &rustup_home)
            .env("KACHE_LOG", "off")
            .current_dir(project)
            .output()
            .unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(output.status.success(), "{stderr}");
        stderr
    };
    let human = run(true, "human");
    assert!(human.starts_with("warning: unused variable:"), "{human}");
    for _ in 0..2 {
        fs::remove_dir_all(&out).unwrap();
        fs::create_dir(&out).unwrap();
        let json = run(true, "json");
        let messages: Vec<Value> = json
            .lines()
            .map(|line| {
                serde_json::from_str(line).expect("JSON request replayed human diagnostics")
            })
            .collect();
        assert!(
            messages
                .iter()
                .any(|message| message["code"]["code"] == "unused_variables")
        );
    }
    fs::remove_dir_all(&out).unwrap();
    fs::create_dir(&out).unwrap();
    let direct = run(false, "json");
    let direct_messages: Vec<Value> = direct
        .lines()
        .map(|line| serde_json::from_str(line).expect("direct rustc emits JSON"))
        .collect();
    assert!(
        direct_messages
            .iter()
            .any(|message| message["code"]["code"] == "unused_variables")
    );
    let events: Vec<Value> = fs::read_to_string(cache.join("events.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0]["result"], "miss");
    assert!(matches!(events[1]["result"].as_str(), Some("miss" | "dup")));
    assert_eq!(events[2]["result"], "local_hit");
    assert_ne!(events[0]["cache_key"], events[1]["cache_key"]);
    assert_eq!(events[1]["cache_key"], events[2]["cache_key"]);
}
