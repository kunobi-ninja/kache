//! Rust diagnostics name real files, whether fresh or replayed from the cache.

use serde_json::Value;
use std::fs;
use std::path::Path;
use std::process::Output;

#[allow(dead_code)]
mod common;
use common::{build_kache, hermetic_command, isolated_config_path, kache_binary};

struct Fixture<'a> {
    cache: &'a Path,
    cargo_home: &'a Path,
}

fn compile(fixture: &Fixture, dep: &Path, wrapped: bool, json: bool) -> Output {
    let cache = fixture.cache;
    let out = dep.join("out");
    if out.exists() {
        fs::remove_dir_all(&out).unwrap();
    }
    fs::create_dir(&out).unwrap();
    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let config = isolated_config_path(cache);
    let mut command = if wrapped {
        let mut command = hermetic_command(kache_binary(), cache, Some(&config));
        command.arg(&rustc);
        command
    } else {
        hermetic_command(&rustc, cache, Some(&config))
    };
    command
        .args([
            "--crate-name",
            dep.file_name().unwrap().to_str().unwrap(),
            "--crate-type=rlib",
            "--emit=dep-info,metadata,link",
            "--out-dir",
        ])
        .arg(out)
        // Cargo passes registry and external path sources by absolute path.
        .arg(dep.join("src").join("lib.rs"))
        .env("CARGO_HOME", fixture.cargo_home)
        .env("KACHE_LOG", "off")
        .current_dir(dep);
    if json {
        command.args(["--error-format=json", "--json=artifacts"]);
    } else {
        command.arg("--color=never");
    }
    command.output().unwrap()
}

fn diagnostics(output: &Output) -> Vec<Value> {
    String::from_utf8_lossy(&output.stderr)
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|record| record["$message_type"] == "diagnostic")
        .collect()
}

fn assert_real_paths(records: &[Value]) {
    let located: Vec<&Value> = records
        .iter()
        .filter(|record| record["spans"][0]["file_name"].is_string())
        .collect();
    assert!(!located.is_empty(), "{records:?}");
    for record in located {
        let file = record["spans"][0]["file_name"].as_str().unwrap();
        assert!(Path::new(file).is_file(), "{record}");
        let rendered = record["rendered"].as_str().unwrap();
        assert!(rendered.contains(file), "{record}");
    }
}

/// Warnings show real files on a miss and on a hit, and so do errors in
/// JSON and human format.
fn check(fixture: &Fixture, dep: &Path) {
    let name = dep.file_name().unwrap().to_str().unwrap();
    fs::create_dir_all(dep.join("src")).unwrap();
    let lib = dep.join("src").join("lib.rs");

    fs::write(&lib, "pub fn answer() -> u32 { let unused = 1; 42 }\n").unwrap();
    let direct = compile(fixture, dep, false, true);
    assert!(direct.status.success(), "{direct:?}");
    let mut replays = Vec::new();
    for _ in 0..2 {
        let wrapped = compile(fixture, dep, true, true);
        assert!(wrapped.status.success(), "{wrapped:?}");
        assert_real_paths(&diagnostics(&wrapped));
        if cfg!(unix) {
            assert_eq!(diagnostics(&wrapped), diagnostics(&direct));
        }
        replays.push(diagnostics(&wrapped));
    }
    assert_eq!(replays[0], replays[1]);
    let events: Vec<Value> = fs::read_to_string(fixture.cache.join("events.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|event| event["crate_name"] == name)
        .collect();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["result"], "miss");
    assert_eq!(events[1]["result"], "local_hit");

    fs::write(&lib, "pub fn answer() -> u32 { \"42\" }\n").unwrap();
    let failed = compile(fixture, dep, true, true);
    assert!(!failed.status.success(), "{failed:?}");
    assert_real_paths(&diagnostics(&failed));
    let human = compile(fixture, dep, true, false);
    assert!(!human.status.success(), "{human:?}");
    let human = String::from_utf8_lossy(&human.stderr);
    let location = human
        .lines()
        .find_map(|line| line.trim_start().strip_prefix("--> "))
        .unwrap();
    // Strip `:line:col`; a Windows path has its own colon.
    let file = location.rsplitn(3, ':').nth(2).unwrap();
    assert!(Path::new(file).is_file(), "{human}");
}

#[test]
fn dependency_diagnostics_name_real_files_fresh_and_cached() {
    build_kache();
    let fixture = tempfile::tempdir_in(common::scratch_dir()).unwrap();
    let root = fixture.path();
    let cache = root.join("cache");
    fs::create_dir_all(&cache).unwrap();
    fs::write(
        isolated_config_path(&cache),
        "[cache]\nlocal_only = true\nscheduler = false\ndeferred_discovery = false\nauto_gc = false\ndaemon_publish = false\n",
    )
    .unwrap();
    let _daemon = common::DaemonGuard::new(kache_binary(), &cache);
    let cargo_home = root.join("cargo");
    let fixture = Fixture {
        cache: &cache,
        cargo_home: &cargo_home,
    };
    // A registry crate maps to the Cargo home target.
    check(
        &fixture,
        &cargo_home
            .join("registry")
            .join("src")
            .join("index.crates.io-0")
            .join("registry_dep"),
    );
    // A path dependency maps to its own working directory, which a file
    // must be a source of the crate to resolve against.
    check(&fixture, &root.join("path_dep"));
}
