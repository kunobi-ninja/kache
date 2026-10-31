//! Build-script timestamp inputs must not turn a cache hit into stale code.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::Command;

mod common;
use common::{DaemonGuard, build_kache, hermetic_command, isolated_config_path, kache_binary};

const OLD: i64 = 1_700_000_000;

fn timestamp(path: &Path, seconds: i64) {
    filetime::set_file_mtime(path, filetime::FileTime::from_unix_time(seconds, 0)).unwrap();
}

fn fixture(root: &Path, directory: bool) {
    std::fs::create_dir_all(root.join("src")).unwrap();
    std::fs::create_dir_all(root.join("data")).unwrap();
    std::fs::write(
        root.join("Cargo.toml"),
        "[package]\nname='mtime_probe'\nversion='0.1.0'\nedition='2021'\n[workspace]\n",
    )
    .unwrap();
    std::fs::write(root.join("data/input.txt"), "identical bytes\n").unwrap();
    timestamp(&root.join("data/input.txt"), OLD);
    timestamp(&root.join("data"), OLD);
    let declared = if directory { "data" } else { "data/input.txt" };
    std::fs::write(
        root.join("build.rs"),
        format!(
            r#"fn main() {{
    println!("cargo:rerun-if-changed={declared}");
    println!("cargo:rerun-if-env-changed=REPORT_MTIME");
    let modified = std::fs::metadata("data/input.txt").unwrap().modified().unwrap();
    let seconds = modified.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    let value = if std::env::var("REPORT_MTIME").unwrap() == "1" {{ seconds.to_string() }} else {{ "constant".into() }};
    println!("cargo:rustc-env=INPUT_VALUE={{value}}");
}}
"#
        ),
    )
    .unwrap();
    std::fs::write(
        root.join("src/main.rs"),
        "fn main() { println!(\"{}\", env!(\"INPUT_VALUE\")); }\n",
    )
    .unwrap();
}

fn configure(cache: &Path) -> PathBuf {
    std::fs::create_dir_all(cache).unwrap();
    let config = isolated_config_path(cache);
    std::fs::write(
        &config,
        "[cache]\nlocal_only=true\ndaemon_publish=false\ninput_predictions=false\n",
    )
    .unwrap();
    config
}

fn build(
    source: &Path,
    target: &Path,
    cache: &Path,
    hermetic: bool,
    report_mtime: bool,
    expected: &str,
) -> Vec<serde_json::Value> {
    let events = cache.join("events.jsonl");
    let before = std::fs::read_to_string(&events).unwrap_or_default().len();
    let output = hermetic_command("cargo", cache, Some(&isolated_config_path(cache)))
        .args(["build", "--offline", "--quiet"])
        .current_dir(source)
        .env("RUSTC_WRAPPER", kache_binary())
        .env("CARGO_TARGET_DIR", target)
        .env("CARGO_INCREMENTAL", "0")
        .env("RUSTFLAGS", "-C debuginfo=0")
        .env("KACHE_BASE_DIR", source)
        .env("KACHE_SEED_NEW_TARGETS", "0")
        .env("KACHE_BUILD_SCRIPT_CACHE", "1")
        .env(
            "KACHE_BUILD_SCRIPT_HERMETIC",
            if hermetic { "1" } else { "0" },
        )
        .env("REPORT_MTIME", if report_mtime { "1" } else { "0" })
        .env_remove("CARGO_BUILD_BUILD_DIR")
        .env_remove("CARGO_BUILD_TARGET_DIR")
        .env_remove("RUSTC_WORKSPACE_WRAPPER")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let program = Command::new(target.join("debug/mtime_probe"))
        .output()
        .unwrap();
    assert!(
        program.status.success(),
        "{}",
        String::from_utf8_lossy(&program.stderr)
    );
    assert_eq!(
        String::from_utf8(program.stdout).unwrap().trim(),
        expected,
        "stale build-script output"
    );
    let text = std::fs::read_to_string(events).unwrap();
    text[before..]
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn script(events: &[serde_json::Value]) -> &serde_json::Value {
    events
        .iter()
        .find(|event| event["crate_name"] == "build_script_run")
        .expect("a build-script event")
}

fn changed_timestamp(hermetic: bool, directory: bool) {
    build_kache();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let source = root.join("source");
    fixture(&source, directory);
    let cache = root.join("cache");
    configure(&cache);
    let _daemon = DaemonGuard::new(kache_binary(), &cache);
    let old = OLD.to_string();
    build(&source, &root.join("t0"), &cache, hermetic, true, &old);
    // Hermetic mode needs one run with known declarations to seal its output.
    if hermetic {
        build(&source, &root.join("t1"), &cache, true, true, &old);
    }
    let warm = build(&source, &root.join("t2"), &cache, hermetic, true, &old);
    assert_eq!(script(&warm)["result"], "local_hit");
    let old_key = script(&warm)["cache_key"].clone();

    timestamp(&source.join("data/input.txt"), OLD + 60);
    let new = (OLD + 60).to_string();
    let changed = build(&source, &root.join("t3"), &cache, hermetic, true, &new);
    assert_eq!(script(&changed)["result"], "miss");
    let new_key = script(&changed)["cache_key"].clone();
    assert_ne!(old_key, new_key);
    let again = build(&source, &root.join("t4"), &cache, hermetic, true, &new);
    assert_eq!(script(&again)["result"], "local_hit");
    assert_eq!(script(&again)["cache_key"], new_key);

    timestamp(&source.join("data/input.txt"), OLD);
    let reverted = build(&source, &root.join("t5"), &cache, hermetic, true, &old);
    assert_eq!(script(&reverted)["result"], "local_hit");
    assert_eq!(script(&reverted)["cache_key"], old_key);
}

#[test]
fn declared_file_timestamp_changes_do_not_replay_stale_outputs() {
    changed_timestamp(false, false);
}

#[test]
fn declared_directory_tracks_its_child_timestamps() {
    changed_timestamp(false, true);
}

#[test]
fn hermetic_runs_track_declared_input_timestamps() {
    changed_timestamp(true, false);
}

#[test]
fn a_script_rerun_with_unchanged_outputs_keeps_the_rust_hit() {
    build_kache();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let source = root.join("source");
    fixture(&source, false);
    let cache = root.join("cache");
    configure(&cache);
    let _daemon = DaemonGuard::new(kache_binary(), &cache);
    build(&source, &root.join("t0"), &cache, false, false, "constant");
    timestamp(&source.join("data/input.txt"), OLD + 60);
    let events = build(&source, &root.join("t1"), &cache, false, false, "constant");
    assert_eq!(script(&events)["result"], "miss");
    assert!(
        events
            .iter()
            .any(|event| event["crate_name"] == "mtime_probe" && event["result"] == "local_hit"),
        "{events:?}"
    );
}

#[test]
fn matching_timestamps_still_share_script_runs_across_checkouts() {
    build_kache();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let cache = root.join("cache");
    configure(&cache);
    let _daemon = DaemonGuard::new(kache_binary(), &cache);
    let a = root.join("a");
    let b = root.join("b");
    fixture(&a, true);
    fixture(&b, true);
    let expected = OLD.to_string();
    build(&a, &root.join("ta"), &cache, false, true, &expected);
    let events = build(&b, &root.join("tb"), &cache, false, true, &expected);
    assert_eq!(script(&events)["result"], "local_hit");
}

#[test]
fn restoring_a_header_does_not_hide_its_change_from_incremental_native_builds() {
    build_kache();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let source = root.join("source");
    let write = |name: &str, text: &str| {
        let path = source.join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    };
    write(
        "Cargo.toml",
        "[package]\nname='mtime_probe'\nversion='0.1.0'\nedition='2021'\n[dependencies]\nmtime_provider={path='provider'}\n[workspace]\nmembers=['provider']\n",
    );
    write(
        "provider/Cargo.toml",
        "[package]\nname='mtime_provider'\nversion='0.1.0'\nedition='2021'\nlinks='mtime_provider'\n",
    );
    write("provider/src/lib.rs", "");
    write(
        "provider/build.rs",
        r##"fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=HEADER_VALUE");
    let out = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
    std::fs::write(out.join("value.h"), format!("#define HEADER_VALUE {}\n", std::env::var("HEADER_VALUE").unwrap())).unwrap();
    println!("cargo:include={}", out.display());
}
"##,
    );
    write(
        "src/native.c",
        "#include \"value.h\"\nint probe(void) { return HEADER_VALUE; }\n",
    );
    write(
        "src/main.rs",
        "extern \"C\" { fn probe() -> i32; }\nfn main() { println!(\"{}\", unsafe { probe() }); }\n",
    );
    write(
        "build.rs",
        r#"fn main() {
    println!("cargo:rerun-if-changed=src/native.c");
    println!("cargo:rerun-if-env-changed=NATIVE_VARIANT");
    let out = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
    let include = std::path::PathBuf::from(std::env::var_os("DEP_MTIME_PROVIDER_INCLUDE").unwrap());
    let object = out.join("native.o");
    let archive = out.join("libprobe.a");
    // Make and CMake compare header timestamps with existing native objects.
    let dirty = !object.exists() || std::fs::metadata(include.join("value.h")).unwrap().modified().unwrap() > std::fs::metadata(&object).unwrap().modified().unwrap();
    if dirty {
        assert!(std::process::Command::new("cc").args(["-c", "src/native.c", "-I"]).arg(&include).arg("-o").arg(&object).status().unwrap().success());
        assert!(std::process::Command::new("ar").arg("crs").arg(&archive).arg(&object).status().unwrap().success());
    }
    println!("cargo:rustc-link-search=native={}", out.display());
    println!("cargo:rustc-link-lib=static=probe");
}
"#,
    );
    let cache = root.join("cache");
    configure(&cache);
    let _daemon = DaemonGuard::new(kache_binary(), &cache);
    let target = root.join("target");
    for (variant, value) in [("first", "1"), ("second", "2"), ("restore-first", "1")] {
        let output = hermetic_command("cargo", &cache, Some(&isolated_config_path(&cache)))
            .args(["build", "--offline", "--quiet"])
            .current_dir(&source)
            .env("RUSTC_WRAPPER", kache_binary())
            .env("CARGO_TARGET_DIR", &target)
            .env("CARGO_INCREMENTAL", "0")
            .env("RUSTFLAGS", "-C debuginfo=0")
            .env_remove("CARGO_ENCODED_RUSTFLAGS")
            .env_remove("RUSTC_WORKSPACE_WRAPPER")
            .env_remove("CARGO_BUILD_BUILD_DIR")
            .env_remove("CARGO_BUILD_TARGET_DIR")
            .env("KACHE_SEED_NEW_TARGETS", "0")
            .env("KACHE_BUILD_SCRIPT_CACHE", "1")
            .env("KACHE_BUILD_SCRIPT_HERMETIC", "0")
            .env("HEADER_VALUE", value)
            .env("NATIVE_VARIANT", variant)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{variant}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let output = Command::new(target.join("debug/mtime_probe"))
            .output()
            .unwrap();
        assert!(output.status.success());
        assert_eq!(
            String::from_utf8(output.stdout).unwrap().trim(),
            value,
            "{variant}: stale native object"
        );
    }
}

#[test]
fn implicit_package_reuse_survives_directory_and_cargo_marker_changes() {
    build_kache();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let source = root.join("source");
    fixture(&source, false);
    std::fs::write(
        source.join("build.rs"),
        "fn main() { println!(\"cargo:rustc-env=INPUT_VALUE=constant\"); }\n",
    )
    .unwrap();
    let cache = root.join("cache");
    configure(&cache);
    let _daemon = DaemonGuard::new(kache_binary(), &cache);
    build(&source, &root.join("t0"), &cache, false, false, "constant");
    std::fs::create_dir(source.join("target")).unwrap();
    std::fs::write(source.join("target/ignored"), "ignored").unwrap();
    std::fs::write(source.join(".cargo-ok"), "{}").unwrap();
    std::fs::write(source.join(".cargo-checksum.json"), "{}").unwrap();
    let events = build(&source, &root.join("t1"), &cache, false, false, "constant");
    assert_eq!(script(&events)["result"], "local_hit");
    std::fs::remove_dir_all(source.join("target")).unwrap();
    let events = build(&source, &root.join("t2"), &cache, false, false, "constant");
    assert_eq!(script(&events)["result"], "local_hit");
    timestamp(&source.join("data/input.txt"), OLD + 60);
    let events = build(&source, &root.join("t3"), &cache, false, false, "constant");
    assert_eq!(script(&events)["result"], "miss");
}
