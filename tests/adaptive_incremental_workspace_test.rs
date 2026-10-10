//! Adaptive incremental mode for workspace crates the Cargo command does not
//! select.

use filetime::FileTime;
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

// Only the env plumbing is shared; this file drives the Cargo-built binary,
// so the bootstrap helpers in `common` stay unused here.
#[allow(dead_code)]
mod common;
use common::{hermetic_command, settle_writes};

fn kache_binary() -> &'static str {
    env!("CARGO_BIN_EXE_kache")
}

/// Events Kache recorded for `crate_name`.
fn crate_events(cache_dir: &Path, crate_name: &str) -> Vec<Value> {
    fs::read_to_string(cache_dir.join("events.jsonl"))
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|event| event["crate_name"] == crate_name)
        .collect()
}

fn assert_passthrough(event: &Value, reason: &str) {
    assert_eq!(event["result"], "passthrough", "event: {event:#}");
    assert!(
        event["passthrough_reason"]
            .as_str()
            .is_some_and(|value| value.contains(reason)),
        "expected passthrough reason containing {reason:?}; event: {event:#}",
    );
}

/// A virtual workspace whose binary `adaptive-app` prints what `adaptive-lib`
/// returns through `adaptive-mid`.
fn create_workspace(project: &Path) {
    for member in ["lib", "mid", "app"] {
        fs::create_dir_all(project.join(member).join("src")).unwrap();
    }
    let package = |name: &str, dependency: Option<(&str, &str)>| {
        let mut manifest =
            format!("[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2024\"\n");
        if let Some((dependency, path)) = dependency {
            manifest.push_str(&format!(
                "\n[dependencies]\n{dependency} = {{ path = \"{path}\" }}\n"
            ));
        }
        manifest
    };
    fs::write(
        project.join("Cargo.toml"),
        "[workspace]\nmembers = [\"lib\", \"mid\", \"app\"]\nresolver = \"2\"\n",
    )
    .unwrap();
    fs::write(
        project.join("lib/Cargo.toml"),
        package("adaptive-lib", None),
    )
    .unwrap();
    fs::write(
        project.join("mid/Cargo.toml"),
        package("adaptive-mid", Some(("adaptive-lib", "../lib"))),
    )
    .unwrap();
    fs::write(
        project.join("mid/src/lib.rs"),
        "pub fn answer() -> u64 { adaptive_lib::answer() }\n",
    )
    .unwrap();
    fs::write(
        project.join("app/Cargo.toml"),
        package("adaptive-app", Some(("adaptive-mid", "../mid"))),
    )
    .unwrap();
    fs::write(
        project.join("app/src/main.rs"),
        "fn main() { print!(\"{}\", adaptive_mid::answer()); }\n",
    )
    .unwrap();
}

/// `dir` spelled as Cargo spells the package directories in it. Cargo
/// resolves its working directory, and macOS reaches the temporary directory
/// through the `/var` link, so a target directory under the unresolved
/// spelling would not lie inside the workspace Kache finds, and the unit's
/// records would get no workspace guard. Windows keeps the spelling it was
/// given.
fn checkout_path(dir: &Path) -> PathBuf {
    if cfg!(windows) {
        dir.to_path_buf()
    } else {
        dir.canonicalize().unwrap()
    }
}

/// Write `answer` into the library and run `cargo build -p adaptive-app`,
/// which selects neither the library nor the middle crate. Runs the app to
/// prove it linked this build's library, then returns this build's single
/// event for the library and for the middle crate.
fn build_unselected(
    project: &Path,
    cache_dir: &Path,
    target_dir: &Path,
    source_mtime: Option<i64>,
    answer: u64,
) -> (Value, Value) {
    let source = project.join("lib/src/lib.rs");
    fs::write(&source, format!("pub fn answer() -> u64 {{ {answer} }}\n")).unwrap();
    if let Some(mtime) = source_mtime {
        filetime::set_file_mtime(&source, FileTime::from_unix_time(mtime, 0)).unwrap();
    }
    // A compile that runs before its key is not stored when a source was
    // written just before the build started, either.
    settle_writes(&[project]);
    let before_lib = crate_events(cache_dir, "adaptive_lib").len();
    let before_mid = crate_events(cache_dir, "adaptive_mid").len();
    let mut command = hermetic_command(
        "cargo",
        cache_dir,
        Some(&project.join("missing-kache.toml")),
    );
    command
        .args(["build", "--offline", "--quiet", "-p", "adaptive-app"])
        .current_dir(project)
        .env("RUSTC_WRAPPER", kache_binary())
        .env("CARGO_TARGET_DIR", target_dir)
        .env("CARGO_INCREMENTAL", "1")
        .env("KACHE_LOCAL_ONLY", "1");
    for name in [
        // Cargo sets it for selected units only; an inherited value would
        // reach every unit and hide what this test is about.
        "CARGO_PRIMARY_PACKAGE",
        "RUSTC_WORKSPACE_WRAPPER",
        "KACHE_ADAPTIVE_INCREMENTAL",
        "KACHE_CLEAN_INCREMENTAL",
        "KACHE_DEFERRED_DISCOVERY",
        "KACHE_DISABLED",
        "KACHE_FALLBACK",
        "KACHE_INCREMENTAL_CRATES",
        "KACHE_MIN_STORE_COMPILE_MS",
        "KACHE_PRESERVE_INCREMENTAL",
        "KACHE_READONLY_STORE",
        "KACHE_SCHEDULER",
    ] {
        command.env_remove(name);
    }
    for (name, _) in std::env::vars_os() {
        if name
            .to_str()
            .is_some_and(|name| name.starts_with("KACHE_S3_") || name.starts_with("AWS_"))
        {
            command.env_remove(name);
        }
    }
    let output = command
        .output()
        .expect("failed to build the workspace fixture");
    assert!(
        output.status.success(),
        "workspace build failed for variant {answer}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stderr),
    );
    let app = target_dir.join(format!(
        "debug/adaptive-app{}",
        std::env::consts::EXE_SUFFIX
    ));
    let run = Command::new(&app)
        .output()
        .expect("failed to run the workspace binary");
    assert_eq!(String::from_utf8_lossy(&run.stdout), answer.to_string());

    let one_new = |crate_name: &str, before: usize| {
        let events = crate_events(cache_dir, crate_name);
        assert_eq!(events.len(), before + 1, "{crate_name} events: {events:#?}");
        events.into_iter().nth(before).unwrap()
    };
    (
        one_new("adaptive_lib", before_lib),
        one_new("adaptive_mid", before_mid),
    )
}

/// A library the command does not select is a unit someone edits, like a
/// selected one: `cargo build -p adaptive-app` seeds it after an edit and then
/// compiles it on the active lane. So does the unselected crate between it
/// and the app, whose only change is the library it links.
#[test]
fn unselected_workspace_crates_seed_then_stay_active() {
    let checkout = tempfile::tempdir().unwrap();
    let project = checkout_path(checkout.path());
    let cache = tempfile::tempdir().unwrap();
    let target = project.join("target");
    create_workspace(&project);
    let future = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        + 3_600;
    let build = |mtime: Option<i64>, answer| {
        build_unselected(&project, cache.path(), &target, mtime, answer)
    };

    // Builds without policy state keep real mtimes: such a unit compiles
    // before its key, and a source stamped after the build started would be
    // neither stored nor learned from.
    let (lib, mid) = build(None, 1);
    assert_eq!(lib["result"], "miss", "event: {lib:#}");
    assert_eq!(
        lib["dep_info_runs"], 0,
        "a unit without policy state compiles before its key: {lib:#}"
    );
    assert_eq!(mid["result"], "miss", "event: {mid:#}");

    // Future, increasing mtimes make Cargo rebuild without sleeping.
    let (lib, mid) = build(Some(future), 2);
    assert_passthrough(&lib, "adaptive seed");
    assert_passthrough(&mid, "adaptive seed");

    let (lib, mid) = build(Some(future + 1), 3);
    assert_passthrough(&lib, "adaptive active");
    assert_eq!(lib["key_ms"], 0, "event: {lib:#}");
    assert_passthrough(&mid, "adaptive active");

    // With the policy state gone, the unit cannot seed. Its record from the
    // first build no longer applies, because the workspace changed since, so
    // the unit compiles before its key, as on its first build, instead of
    // paying the dep-info pre-pass first.
    fs::remove_dir_all(target.join("debug/incremental.kache-auto")).unwrap();
    let (lib, _mid) = build(None, 4);
    assert_eq!(lib["result"], "miss", "event: {lib:#}");
    assert_eq!(
        lib["dep_info_runs"], 0,
        "a unit whose policy state is gone compiles before its key: {lib:#}"
    );
}
