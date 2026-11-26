//! A unit whose lints are capped at `allow` shares one entry across diagnostic
//! presentations and lint levels; every other unit keeps them in its key.

use serde_json::Value;
use std::fs;
use std::path::Path;
use std::process::Output;

#[allow(dead_code)]
mod common;
use common::{build_kache, hermetic_command, isolated_config_path, kache_binary};

fn write_config(cache: &Path) {
    fs::create_dir_all(cache).unwrap();
    fs::write(
        isolated_config_path(cache),
        "[cache]\nlocal_only = true\nscheduler = false\ndeferred_discovery = false\nauto_gc = false\ndaemon_publish = false\n",
    )
    .unwrap();
}

fn events(cache: &Path, name: &str) -> Vec<Value> {
    fs::read_to_string(cache.join("events.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|event| event["crate_name"] == name)
        .collect()
}

fn compile(project: &Path, cache: &Path, wrapped: bool, flags: &[&str]) -> Output {
    let out = project.join("out");
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
            "capped_fixture",
            "--crate-type=rlib",
            "--emit=dep-info,metadata,link",
            "--out-dir",
        ])
        .arg(out)
        .arg(project.join("lib.rs"))
        .args(flags)
        .env("KACHE_LOG", "off")
        // Windows runners lack copy-on-write; the advice would join stderr.
        .env("KACHE_STORAGE_LAYOUT_ADVICE", "0")
        .current_dir(project)
        .output()
        .unwrap()
}

/// Cargo's flags for a terminal build of a dependency.
const HUMAN: &[&str] = &[
    "--error-format=json",
    "--json=diagnostic-rendered-ansi,artifacts,future-incompat",
    "--diagnostic-width=100",
];
/// Cargo's flags under `--message-format short`.
const SHORT: &[&str] = &[
    "--error-format=json",
    "--json=diagnostic-rendered-ansi,artifacts,future-incompat,diagnostic-short",
];

/// stderr lines in order of their text: a miss forwards the metadata
/// notification early.
fn lines(output: &Output) -> Vec<String> {
    let mut lines: Vec<String> = String::from_utf8_lossy(&output.stderr)
        .lines()
        .map(str::to_owned)
        .collect();
    lines.sort();
    lines
}

fn with(base: &[&'static str], extra: &[&'static str]) -> Vec<&'static str> {
    let mut flags = base.to_vec();
    flags.extend_from_slice(extra);
    flags
}

#[test]
fn capped_unit_hits_across_message_formats_and_lint_levels() {
    build_kache();
    let fixture = tempfile::tempdir_in(common::scratch_dir()).unwrap();
    let project = fixture.path();
    let cache = project.join("cache");
    write_config(&cache);
    let _daemon = common::DaemonGuard::new(kache_binary(), &cache);
    fs::write(
        project.join("lib.rs"),
        "pub fn answer() -> u32 { let unused = 1; 42 }\n",
    )
    .unwrap();

    let runs = [
        with(HUMAN, &["--cap-lints", "allow", "-Awarnings"]),
        with(SHORT, &["--cap-lints=allow"]),
        with(SHORT, &["--cap-lints=allow", "-D", "warnings"]),
        with(HUMAN, &["-W", "unused", "--cap-lints=allow"]),
    ];
    for flags in &runs {
        let direct = compile(project, &cache, false, flags);
        assert!(direct.status.success(), "{flags:?}: {direct:?}");
        let cached = compile(project, &cache, true, flags);
        assert!(cached.status.success(), "{flags:?}: {cached:?}");
        // Only artifact notifications, the same lines a direct compile prints.
        assert_eq!(lines(&cached), lines(&direct), "{flags:?}");
        assert!(
            String::from_utf8_lossy(&cached.stderr)
                .lines()
                .all(|line| line.contains("\"$message_type\":\"artifact\"")),
            "{cached:?}"
        );
    }
    let recorded = events(&cache, "capped_fixture");
    assert_eq!(recorded.len(), runs.len());
    assert_eq!(recorded[0]["result"], "miss");
    for event in &recorded[1..] {
        assert_eq!(event["result"], "local_hit", "{recorded:?}");
        assert_eq!(event["cache_key"], recorded[0]["cache_key"]);
    }

    // Text output is another emission class: no artifact notifications to replay.
    for flags in [
        &["--cap-lints=allow", "--color=never"][..],
        &["--error-format=short", "--cap-lints=allow", "-Dwarnings"][..],
    ] {
        let text = compile(project, &cache, true, flags);
        assert!(text.status.success(), "{text:?}");
        assert!(text.stderr.is_empty(), "{text:?}");
    }
    let recorded = events(&cache, "capped_fixture");
    let text = &recorded[recorded.len() - 2..];
    assert_eq!(text[0]["result"], "miss");
    assert_eq!(text[1]["result"], "local_hit");
    assert_eq!(text[0]["cache_key"], text[1]["cache_key"]);
    assert_ne!(text[0]["cache_key"], recorded[0]["cache_key"]);

    // Without the cap, `-D warnings` fails on the unused variable: never a hit.
    for flags in [with(SHORT, &[]), with(SHORT, &["-D", "warnings"])] {
        let direct = compile(project, &cache, false, &flags);
        let cached = compile(project, &cache, true, &flags);
        assert_eq!(
            cached.status.success(),
            direct.status.success(),
            "{flags:?}"
        );
    }
    let denied = compile(project, &cache, true, &with(SHORT, &["-D", "warnings"]));
    assert!(!denied.status.success(), "{denied:?}");
}

#[test]
fn capped_unit_keeps_argument_errors_and_forced_warnings_keyed() {
    build_kache();
    let fixture = tempfile::tempdir_in(common::scratch_dir()).unwrap();
    let project = fixture.path();
    let cache = project.join("cache");
    write_config(&cache);
    let _daemon = common::DaemonGuard::new(kache_binary(), &cache);
    fs::write(
        project.join("lib.rs"),
        "pub fn answer() -> u32 { let unused = 1; 42 }\n",
    )
    .unwrap();
    let warm = with(SHORT, &["--cap-lints=allow"]);
    let output = compile(project, &cache, true, &warm);
    assert!(output.status.success(), "{output:?}");

    // rustc rejects these; a warm entry must not answer for them.
    for rejected in [
        &["--color=never"][..],
        &["--color=sometimes"][..],
        &["--error-format=pretty"][..],
        &["--diagnostic-width=wide"][..],
    ] {
        let flags = with(&warm, rejected);
        let direct = compile(project, &cache, false, &flags);
        assert!(!direct.status.success(), "{flags:?}: {direct:?}");
        let cached = compile(project, &cache, true, &flags);
        assert!(!cached.status.success(), "{flags:?}: {cached:?}");
    }

    // `--force-warn` escapes the cap: its warning is rendered and replayed.
    let forced = with(&warm, &["--force-warn=unused_variables"]);
    let direct = compile(project, &cache, false, &forced);
    assert!(direct.status.success(), "{direct:?}");
    assert!(String::from_utf8_lossy(&direct.stderr).contains("unused variable"));
    for _ in 0..2 {
        let cached = compile(project, &cache, true, &forced);
        assert!(cached.status.success(), "{cached:?}");
        assert!(String::from_utf8_lossy(&cached.stderr).contains("unused variable"));
    }
    let recorded = events(&cache, "capped_fixture");
    let forced_runs = &recorded[recorded.len() - 2..];
    assert_eq!(forced_runs[1]["result"], "local_hit");
    assert_ne!(forced_runs[0]["cache_key"], recorded[0]["cache_key"]);
}
