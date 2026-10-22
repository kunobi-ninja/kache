//! Real rustdoc output must follow edits below non-mod.rs modules.

use serde_json::Value;
use std::fs;

mod common;
use common::{build_kache, hermetic_command, isolated_config_path, kache_binary};

#[test]
fn nested_module_edits_refresh_cached_rustdoc_and_keep_warm_reuse() {
    assert_doc_edit_refreshes("pub mod foo;\n", "foo.rs", "foo/child.rs");
}

#[test]
fn explicit_path_module_child_edits_refresh_docs_and_keep_warm_reuse() {
    assert_doc_edit_refreshes(
        "#[path = \"renamed.rs\"] pub mod foo;\n",
        "renamed.rs",
        "child.rs",
    );
}

fn assert_doc_edit_refreshes(root_source: &str, module_file: &str, child_file: &str) {
    build_kache();
    let fixture = tempfile::tempdir_in(common::scratch_dir()).unwrap();
    let project = fixture.path();
    let cache = project.join("cache");
    let _daemon = common::DaemonGuard::new(kache_binary(), &cache);
    let home = project.join("home");
    let out = project.join("out");
    let parts = project.join("parts");
    for directory in [&cache, &home, &project.join("foo")] {
        fs::create_dir(directory).unwrap();
    }
    let config = isolated_config_path(&cache);
    fs::write(
        &config,
        "[cache]\nlocal_only = true\nscheduler = false\nauto_gc = false\ndaemon_publish = false\n",
    )
    .unwrap();
    fs::write(project.join("lib.rs"), root_source).unwrap();
    fs::write(project.join(module_file), "pub mod child;\n").unwrap();
    let child = project.join(child_file);
    fs::write(&child, "/// OLD_MARKER\npub fn thing() {}\n").unwrap();
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
        fs::read_to_string(out.join("doc_fixture/foo/child/fn.thing.html")).unwrap()
    };
    let clear = || {
        fs::remove_dir_all(&out).unwrap();
        fs::remove_dir_all(&parts).unwrap();
    };
    let cold = run(true);
    assert!(cold.contains("OLD_MARKER"));
    clear();
    assert!(run(true).contains("OLD_MARKER"));
    clear();
    fs::write(&child, "/// NEW_MARKER\npub fn thing() {}\n").unwrap();
    let edited = run(true);
    assert!(
        edited.contains("NEW_MARKER"),
        "cached rustdoc omitted the nested module edit"
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
