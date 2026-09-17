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
