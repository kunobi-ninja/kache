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

/// Keep a remote configured in the developer's shell out of the fixture.
fn remove_remote_env(command: &mut Command) {
    for (name, _) in std::env::vars_os() {
        if name
            .to_str()
            .is_some_and(|name| name.starts_with("KACHE_S3_") || name.starts_with("AWS_"))
        {
            command.env_remove(name);
        }
    }
}

/// Remove `crate_name`'s cache entries, as an eviction would.
fn purge_crate(project: &Path, cache_dir: &Path, crate_name: &str) {
    let mut command = hermetic_command(
        kache_binary(),
        cache_dir,
        Some(&project.join("missing-kache.toml")),
    );
    command.args(["clean", "--crate", crate_name, "--yes"]);
    remove_remote_env(&mut command);
    let output = command.output().expect("failed to run kache clean");
    assert!(
        output.status.success(),
        "kache clean --crate {crate_name} failed\nstderr:\n{}",
        String::from_utf8_lossy(&output.stderr),
    );
}

/// `cargo build -p <package>` in `project` through Kache, local only, with
/// every setting that changes adaptive mode, deferral or what is stored left
/// at its default.
fn build_command(project: &Path, cache_dir: &Path, target_dir: &Path, package: &str) -> Command {
    let mut command = hermetic_command(
        "cargo",
        cache_dir,
        Some(&project.join("missing-kache.toml")),
    );
    command
        .args(["build", "--offline", "--quiet", "-p", package])
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
        "KACHE_MODIFIED_INPUT_GUARD",
        "KACHE_PRESERVE_INCREMENTAL",
        "KACHE_READONLY_STORE",
        "KACHE_SCHEDULER",
    ] {
        command.env_remove(name);
    }
    remove_remote_env(&mut command);
    command
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
    let output = build_command(project, cache_dir, target_dir, "adaptive-app")
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

    // That build taught the unit again and recorded its closure. The same
    // source leaves the workspace as that record saw it, so the record
    // predicts the key the unit was last built under, which cannot seed, and
    // its entry is gone. No lane needs the key first any more, so the unit
    // compiles while the key is re-derived.
    purge_crate(&project, cache.path(), "adaptive_lib");
    let (lib, _mid) = build(None, 4);
    assert_eq!(lib["result"], "miss", "event: {lib:#}");
    assert_eq!(
        lib["dep_info_runs"], 0,
        "a missed prediction that cannot seed compiles while re-deriving: {lib:#}"
    );
}

/// A rustc that writes `$RACE_NEXT` over `$RACE_SOURCE` once, just before
/// the compile of `$RACE_CRATE` reads it: a save that lands after Kache took
/// the unit's key. The dep-info pre-pass passes `--emit dep-info` as two
/// arguments and is left alone.
#[cfg(unix)]
const RACE_RUSTC: &str = r#"#!/bin/sh
case " $* " in
  *" --crate-name $RACE_CRATE "*)
    case " $* " in
      *" --emit=dep-info,"*)
        if [ -f "$RACE_NEXT" ]; then
          cat "$RACE_NEXT" > "$RACE_SOURCE"
          rm -f "$RACE_NEXT"
        fi
        ;;
    esac
    ;;
esac
exec "$REAL_RUSTC" "$@"
"#;

/// The rustc the shim hands each compile to.
#[cfg(unix)]
fn real_rustc() -> PathBuf {
    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let output = Command::new(rustc)
        .args(["--print", "sysroot"])
        .output()
        .expect("failed to ask rustc for its sysroot");
    assert!(output.status.success(), "rustc --print sysroot failed");
    PathBuf::from(String::from_utf8(output.stdout).unwrap().trim()).join("bin/rustc")
}

/// A workspace whose binary `race-app` depends on `race-lib`, built through
/// [`RACE_RUSTC`].
#[cfg(unix)]
struct RaceWorkspace {
    _checkout: tempfile::TempDir,
    cache: tempfile::TempDir,
    tools: tempfile::TempDir,
    project: PathBuf,
    /// Set for every build, after the defaults.
    env: Vec<(&'static str, &'static str)>,
}

#[cfg(unix)]
impl RaceWorkspace {
    fn new(lib: &str, main: &str) -> Self {
        use std::os::unix::fs::PermissionsExt;
        let checkout = tempfile::tempdir().unwrap();
        let project = checkout_path(checkout.path());
        for member in ["lib", "app"] {
            fs::create_dir_all(project.join(member).join("src")).unwrap();
        }
        let package = "version = \"0.1.0\"\nedition = \"2024\"\n";
        fs::write(
            project.join("Cargo.toml"),
            "[workspace]\nmembers = [\"lib\", \"app\"]\nresolver = \"2\"\n",
        )
        .unwrap();
        fs::write(
            project.join("lib/Cargo.toml"),
            format!("[package]\nname = \"race-lib\"\n{package}"),
        )
        .unwrap();
        fs::write(
            project.join("app/Cargo.toml"),
            format!(
                "[package]\nname = \"race-app\"\n{package}\n[dependencies]\n\
                 race-lib = {{ path = \"../lib\" }}\n"
            ),
        )
        .unwrap();
        fs::write(project.join("lib/src/lib.rs"), lib).unwrap();
        fs::write(project.join("app/src/main.rs"), main).unwrap();
        fs::write(project.join("README.md"), "race\n").unwrap();
        let tools = tempfile::tempdir().unwrap();
        let shim = tools.path().join("rustc");
        fs::write(&shim, RACE_RUSTC).unwrap();
        fs::set_permissions(&shim, fs::Permissions::from_mode(0o755)).unwrap();
        Self {
            _checkout: checkout,
            cache: tempfile::tempdir().unwrap(),
            tools,
            project,
            env: Vec::new(),
        }
    }

    fn with_env(mut self, name: &'static str, value: &'static str) -> Self {
        self.env.push((name, value));
        self
    }

    /// Run `cargo build -p race-app` with `RACE_FLAG=flag`, the app's
    /// output and this build's event for `crate_name`. With `save`,
    /// `source`, a path in the project, becomes `save` just before the
    /// compile of `crate_name` reads it.
    fn build(
        &self,
        flag: &str,
        crate_name: &str,
        source: &str,
        save: Option<&str>,
    ) -> (String, Value) {
        let next = self.tools.path().join("next");
        if let Some(content) = save {
            fs::write(&next, content).unwrap();
        }
        settle_writes(&[&self.project]);
        let target = self.project.join("target");
        let before = crate_events(self.cache.path(), crate_name).len();
        let mut command = build_command(&self.project, self.cache.path(), &target, "race-app");
        command
            .env("RUSTC", self.tools.path().join("rustc"))
            .env("REAL_RUSTC", real_rustc())
            .env("RACE_CRATE", crate_name)
            .env("RACE_SOURCE", self.project.join(source))
            .env("RACE_NEXT", &next)
            .env("RACE_FLAG", flag)
            .env("KACHE_CACHE_EXECUTABLES", "1")
            .envs(self.env.iter().copied());
        let output = command.output().expect("failed to build the race fixture");
        assert!(
            output.status.success(),
            "race fixture build failed\nstderr:\n{}",
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(!next.exists(), "the compile of {crate_name} never ran");
        let run = Command::new(target.join("debug/race-app"))
            .output()
            .expect("failed to run the race fixture");
        let events = crate_events(self.cache.path(), crate_name);
        assert_eq!(events.len(), before + 1, "{crate_name} events: {events:#?}");
        (
            String::from_utf8_lossy(&run.stdout).into_owned(),
            events.into_iter().nth(before).unwrap(),
        )
    }
}

/// A unit with policy state took its key from the pre-pass, before the
/// compile, and a save landed after that key. The compile must not be stored
/// under it.
#[cfg(unix)]
fn assert_unstored_after_a_save(event: &Value) {
    assert_eq!(
        event["dep_info_runs"], 1,
        "the key came from the pre-pass: {event:#}"
    );
    assert_eq!(event["result"], "skipped", "event: {event:#}");
    assert_eq!(event["skip_reason"], "inputs-changed", "event: {event:#}");
}

/// A library with policy state keys first so that a miss can seed. When no
/// seed can start, it compiles before keying again: a save that lands after
/// its first key leaves the result unstored, so reverting the save cannot
/// restore what rustc built from the saved source.
///
/// In the second build the flag changes the key outside the sources, so no
/// seed can start, and the README edit leaves no record that applies, so the
/// key comes from the pre-pass.
#[cfg(unix)]
#[test]
fn a_save_while_a_library_with_policy_state_compiles_is_not_stored() {
    let lib = |answer: u64| {
        format!(
            "pub fn answer() -> u64 {{ {answer} }}\n\
             pub fn flag() -> &'static str {{ option_env!(\"RACE_FLAG\").unwrap_or(\"none\") }}\n"
        )
    };
    let fixture = RaceWorkspace::new(
        &lib(1),
        "fn main() { print!(\"{}\", race_lib::answer()); }\n",
    );
    let build = |flag, save: Option<&str>| fixture.build(flag, "race_lib", "lib/src/lib.rs", save);

    let (printed, lib_event) = build("a", None);
    assert_eq!(printed, "1");
    assert_eq!(lib_event["result"], "miss", "event: {lib_event:#}");

    fs::write(fixture.project.join("README.md"), "race, edited\n").unwrap();
    let (printed, lib_event) = build("b", Some(&lib(2)));
    assert_eq!(printed, "2", "rustc read the saved source");
    assert_unstored_after_a_save(&lib_event);

    fs::write(fixture.project.join("lib/src/lib.rs"), lib(1)).unwrap();
    let (printed, lib_event) = build("b", None);
    assert_eq!(lib_event["result"], "miss", "event: {lib_event:#}");
    assert_eq!(printed, "1", "the revert compiled");
}

/// The same for a binary. Its compile links, so it cannot be stopped on a
/// hit, but it still compiles before keying again.
#[cfg(unix)]
#[test]
fn a_save_while_a_binary_with_policy_state_compiles_is_not_stored() {
    let main = |word: &str| {
        format!(
            "fn main() {{ print!(\"{word} {{}}\", option_env!(\"RACE_FLAG\").unwrap_or(\"none\")); }}\n"
        )
    };
    let fixture = RaceWorkspace::new("pub fn answer() -> u64 { 1 }\n", &main("old"));
    let build = |flag, save: Option<&str>| fixture.build(flag, "race_app", "app/src/main.rs", save);

    let (printed, app_event) = build("a", None);
    assert_eq!(printed, "old a");
    assert_eq!(app_event["result"], "miss", "event: {app_event:#}");

    fs::write(fixture.project.join("README.md"), "race, edited\n").unwrap();
    let (printed, app_event) = build("b", Some(&main("new")));
    assert_eq!(printed, "new b", "rustc read the saved source");
    assert_unstored_after_a_save(&app_event);

    fs::write(fixture.project.join("app/src/main.rs"), main("old")).unwrap();
    let (printed, app_event) = build("b", None);
    assert_eq!(app_event["result"], "miss", "event: {app_event:#}");
    assert_eq!(printed, "old b", "the revert compiled");
}

/// A binary whose predicted key missed, as after an eviction, re-derives
/// that key with the pre-pass first, because its compile cannot be stopped
/// on a hit. When the key comes out the same and no seed can start, since
/// the unit was last built under it, the binary still compiles before keying
/// again.
#[cfg(unix)]
#[test]
fn a_save_while_a_binary_re_derives_its_key_is_not_stored() {
    let main = |word: &str| format!("fn main() {{ print!(\"{word}\"); }}\n");
    let fixture = RaceWorkspace::new("pub fn answer() -> u64 { 1 }\n", &main("old"));
    let build = |save: Option<&str>| fixture.build("a", "race_app", "app/src/main.rs", save);

    let (printed, app_event) = build(None);
    assert_eq!(printed, "old");
    assert_eq!(app_event["result"], "miss", "event: {app_event:#}");

    // The same bytes again: Cargo rebuilds, the first build's record still
    // applies, and it predicts the key whose entry is gone.
    fs::write(fixture.project.join("app/src/main.rs"), main("old")).unwrap();
    purge_crate(&fixture.project, fixture.cache.path(), "race_app");
    let (printed, app_event) = build(Some(&main("new")));
    assert_eq!(printed, "new", "rustc read the saved source");
    assert_unstored_after_a_save(&app_event);

    fs::write(fixture.project.join("app/src/main.rs"), main("old")).unwrap();
    let (printed, app_event) = build(None);
    assert_eq!(app_event["result"], "miss", "event: {app_event:#}");
    assert_eq!(printed, "old", "the revert compiled");
}

/// With deferred discovery off, every unit keys before it compiles. The
/// modified-input guard then refuses the store when a keyed input moved
/// after the key read it, though its stamp was old at that point, so no
/// wall-clock flag tripped.
#[cfg(unix)]
#[test]
fn the_modified_input_guard_refuses_a_save_after_a_key_taken_first() {
    let lib = |answer: u64| {
        format!(
            "pub fn answer() -> u64 {{ {answer} }}\n\
             pub fn flag() -> &'static str {{ option_env!(\"RACE_FLAG\").unwrap_or(\"none\") }}\n"
        )
    };
    let fixture = RaceWorkspace::new(
        &lib(1),
        "fn main() { print!(\"{}\", race_lib::answer()); }\n",
    )
    .with_env("KACHE_DEFERRED_DISCOVERY", "0")
    .with_env("KACHE_ADAPTIVE_INCREMENTAL", "0")
    .with_env("KACHE_MODIFIED_INPUT_GUARD", "1");
    let build = |flag, save: Option<&str>| fixture.build(flag, "race_lib", "lib/src/lib.rs", save);

    let (printed, lib_event) = build("a", None);
    assert_eq!(printed, "1");
    assert_eq!(lib_event["result"], "miss", "event: {lib_event:#}");

    // The flag makes Cargo rebuild the library and keeps its record out.
    let (printed, lib_event) = build("b", Some(&lib(2)));
    assert_eq!(printed, "2", "rustc read the saved source");
    assert_unstored_after_a_save(&lib_event);

    fs::write(fixture.project.join("lib/src/lib.rs"), lib(1)).unwrap();
    let (printed, lib_event) = build("b", None);
    assert_eq!(lib_event["result"], "miss", "event: {lib_event:#}");
    assert_eq!(printed, "1", "the revert compiled");
}

/// A rustc that, before each compile of `$FLIGHT_CRATE` (not the dep-info
/// pre-pass), appends to `$FLIGHT_LOG` what `$FLIGHT_PROBE` prints for
/// `$FLIGHT_DIR`. Every other unit of the fixture depends on that crate, so
/// no other wrapper runs then.
#[cfg(unix)]
const FLIGHT_RUSTC: &str = r#"#!/bin/sh
case " $* " in
  *" --crate-name $FLIGHT_CRATE "*)
    case " $* " in
      *" --emit=dep-info,"*) "$FLIGHT_PROBE" "$FLIGHT_DIR" >> "$FLIGHT_LOG" ;;
    esac
    ;;
esac
exec "$REAL_RUSTC" "$@"
"#;

/// Prints how many files in the directory it is given another process holds
/// locked, as a peer joining one of those discovery flights finds them.
#[cfg(unix)]
const FLIGHT_PROBE: &str = r#"fn main() {
    let dir = std::env::args().nth(1).expect("a directory");
    let held = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|entry| {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(entry.path())
                .is_ok_and(|file| file.try_lock().is_err())
        })
        .count();
    println!("{held}");
}
"#;

/// A peer in another checkout that finds no record for a unit waits on the
/// unit's discovery flight for the entry and the record its holder leaves.
/// A seed leaves neither, so it releases the flight before it compiles,
/// while a compile whose result is stored keeps holding it.
#[cfg(unix)]
#[test]
fn a_seed_releases_the_discovery_flight_before_it_compiles() {
    use std::os::unix::fs::PermissionsExt;
    let checkout = tempfile::tempdir().unwrap();
    let project = checkout_path(checkout.path());
    let cache = tempfile::tempdir().unwrap();
    let tools = tempfile::tempdir().unwrap();
    let target = project.join("target");
    create_workspace(&project);
    let probe = tools.path().join("probe");
    let probe_source = tools.path().join("probe.rs");
    fs::write(&probe_source, FLIGHT_PROBE).unwrap();
    let compiled = Command::new(real_rustc())
        .args(["--edition", "2021", "-o"])
        .arg(&probe)
        .arg(&probe_source)
        .output()
        .expect("failed to compile the flight probe");
    assert!(
        compiled.status.success(),
        "{}",
        String::from_utf8_lossy(&compiled.stderr)
    );
    let shim = tools.path().join("rustc");
    fs::write(&shim, FLIGHT_RUSTC).unwrap();
    fs::set_permissions(&shim, fs::Permissions::from_mode(0o755)).unwrap();
    let log = tools.path().join("held");
    let future = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        + 3_600;
    // This build's event for the library, and how many flights another
    // process held when the library's compile started.
    let build = |mtime: Option<i64>, answer: u64, env: &[(&str, &str)]| {
        let source = project.join("lib/src/lib.rs");
        fs::write(&source, format!("pub fn answer() -> u64 {{ {answer} }}\n")).unwrap();
        if let Some(mtime) = mtime {
            filetime::set_file_mtime(&source, FileTime::from_unix_time(mtime, 0)).unwrap();
        }
        settle_writes(&[&project]);
        let before = crate_events(cache.path(), "adaptive_lib").len();
        let probed = fs::read_to_string(&log).unwrap_or_default().lines().count();
        let output = build_command(&project, cache.path(), &target, "adaptive-app")
            .env("RUSTC", &shim)
            .env("REAL_RUSTC", real_rustc())
            .env("FLIGHT_CRATE", "adaptive_lib")
            .env("FLIGHT_PROBE", &probe)
            .env("FLIGHT_DIR", cache.path().join("scheduler/discovery"))
            .env("FLIGHT_LOG", &log)
            .envs(env.iter().copied())
            .output()
            .expect("failed to build the workspace fixture");
        assert!(
            output.status.success(),
            "workspace build failed for variant {answer}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stderr),
        );
        let events = crate_events(cache.path(), "adaptive_lib");
        assert_eq!(events.len(), before + 1, "library events: {events:#?}");
        let held: Vec<String> = fs::read_to_string(&log)
            .unwrap()
            .lines()
            .skip(probed)
            .map(String::from)
            .collect();
        assert_eq!(held.len(), 1, "one compile of the library: {held:?}");
        (events[before].clone(), held[0].clone())
    };

    // No policy state: the library holds the flight it found no record
    // under while it compiles before its key, and stores the result.
    let (lib, held) = build(None, 1, &[]);
    assert_eq!(lib["result"], "miss", "event: {lib:#}");
    assert_eq!(lib["dep_info_runs"], 0, "event: {lib:#}");
    assert_eq!(held, "1", "a compile before the key holds the flight");

    // The edit voids the record, so the key comes from the pre-pass under
    // the flight, and then the library seeds.
    let (lib, held) = build(Some(future), 2, &[]);
    assert_passthrough(&lib, "adaptive seed");
    assert_eq!(lib["dep_info_runs"], 1, "event: {lib:#}");
    assert_eq!(held, "0", "the seed released the flight");

    // With no lane to seed and no compile before the key, the library
    // compiles after both lookups miss, and stores the result.
    let (lib, held) = build(
        Some(future + 1),
        3,
        &[
            ("KACHE_ADAPTIVE_INCREMENTAL", "0"),
            ("KACHE_DEFERRED_DISCOVERY", "0"),
        ],
    );
    assert_eq!(lib["result"], "miss", "event: {lib:#}");
    assert_eq!(lib["dep_info_runs"], 1, "event: {lib:#}");
    assert_eq!(held, "1", "a keyed miss holds the flight");
}
