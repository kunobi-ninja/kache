//! Real rustdoc output must follow external item documentation edits.

use serde_json::Value;
use std::fs;

mod common;
use common::{build_kache, hermetic_command, isolated_config_path, kache_binary};

#[test]
fn outer_doc_include_edits_refresh_cached_rustdoc_and_keep_warm_reuse() {
    build_kache();
    let temp_root = if cfg!(target_os = "macos") {
        "/private/tmp".into()
    } else {
        std::env::temp_dir()
    };
    let fixture = tempfile::tempdir_in(temp_root).unwrap();
    let project = fixture.path();
    let cache = project.join("cache");
    let home = project.join("home");
    let out = project.join("out");
    let parts = project.join("parts");
    for directory in [&cache, &home] {
        fs::create_dir(directory).unwrap();
    }
    let config = isolated_config_path(&cache);
    fs::write(
        &config,
        "[cache]\nlocal_only = true\nscheduler = false\nauto_gc = false\ndaemon_publish = false\n",
    )
    .unwrap();
    let source = "#[doc = include_str!(\"README.md\")]\npub fn thing() {}\n";
    fs::write(project.join("lib.rs"), source).unwrap();
    let readme = project.join("README.md");
    fs::write(&readme, "OLD_MARKER\n").unwrap();
    let rustdoc = std::env::var_os("RUSTDOC").unwrap_or_else(|| "rustdoc".into());
    let rustup_home = std::env::var_os("RUSTUP_HOME")
        .unwrap_or_else(|| dirs::home_dir().unwrap().join(".rustup").into_os_string());
    let run = |wrapped: bool| {
        let mut command = if wrapped {
            let mut command = hermetic_command(kache_binary(), &cache, Some(&config));
            command.arg(&rustdoc);
            command
        } else {
            hermetic_command(&rustdoc, &cache, Some(&config))
        };
        let output = command
            .args([
                "lib.rs",
                "--crate-name=doc_fixture",
                "--crate-type=rlib",
                "--edition=2021",
                "-Zunstable-options",
                "--merge=none",
                "--parts-out-dir",
            ])
            .arg(&parts)
            .arg("-o")
            .arg(&out)
            .env("HOME", &home)
            .env("RUSTUP_HOME", &rustup_home)
            .env("RUSTC_BOOTSTRAP", "1")
            .env("KACHE_LOG", "off")
            .current_dir(project)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        fs::read_to_string(out.join("doc_fixture/fn.thing.html")).unwrap()
    };
    let clear = || {
        fs::remove_dir_all(&out).unwrap();
        fs::remove_dir_all(&parts).unwrap();
    };
    assert!(run(true).contains("OLD_MARKER"));
    clear();
    assert!(run(true).contains("OLD_MARKER"));
    clear();
    fs::write(&readme, "NEW_MARKER\n").unwrap();
    let edited = run(true);
    assert!(
        edited.contains("NEW_MARKER"),
        "cached rustdoc omitted the outer doc include edit"
    );
    assert!(!edited.contains("OLD_MARKER"));
    clear();
    assert!(run(true).contains("NEW_MARKER"));
    clear();
    let direct = run(false);
    assert!(direct.contains("NEW_MARKER"));
    assert!(!direct.contains("OLD_MARKER"));
    let events: Vec<Value> = fs::read_to_string(cache.join("events.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(events.len(), 4);
    assert_eq!(events[0]["result"], "miss");
    assert_eq!(events[1]["result"], "local_hit");
    assert_eq!(events[2]["result"], "miss");
    assert_eq!(events[3]["result"], "local_hit");
    assert_eq!(events[0]["cache_key"], events[1]["cache_key"]);
    assert_ne!(events[1]["cache_key"], events[2]["cache_key"]);
    assert_eq!(events[2]["cache_key"], events[3]["cache_key"]);
}
