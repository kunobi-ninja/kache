//! End-to-end regression test for exact C/C++ passthrough arguments.
//!
//! A refused invocation must reach the selected compiler byte-for-byte at the
//! argument boundary: no cache-only prefix-map flags may be injected. The
//! `compose_cc_args` unit tests separately cover ordering on cacheable misses.
//!
//! Unix-only: it relies on a shell-script stand-in for the compiler. The
//! splice logic itself is also covered by the `compose_cc_args` unit
//! tests in `src/compiler/cc.rs`.

#![cfg(unix)]

use std::fs;
use std::process::Command;

#[allow(dead_code)]
mod common;

fn kache_binary() -> &'static str {
    env!("CARGO_BIN_EXE_kache")
}

struct FixtureDir(tempfile::TempDir);

impl FixtureDir {
    fn new() -> Self {
        Self(tempfile::tempdir().unwrap())
    }
    fn path(&self) -> &std::path::Path {
        self.0.path()
    }
    fn stop_daemon(&self) {
        let cache = self.path().join("cache");
        let mut stop = common::hermetic_command(
            kache_binary(),
            &cache,
            Some(&self.path().join("kache.toml")),
        );
        common::stop_daemon(stop.args(["daemon", "stop"]), &cache);
    }
}

impl Drop for FixtureDir {
    fn drop(&mut self) {
        self.stop_daemon();
    }
}

#[test]
fn refused_double_dash_invocation_is_exact_passthrough() {
    let dir = FixtureDir::new();

    // Fake compiler: dump every argument it receives, one per line, then
    // succeed. A real clang-cl would *fail* on this argv if a flag landed
    // after `--`; we assert the ordering directly instead of depending on
    // a real clang-cl being installed.
    // Named `cc` (no extension) so kache's wrapper recognizer accepts it
    // as a C compiler and a GNU dialect is inferred — the dialect that
    // injects `-ffile-prefix-map` today (clang-cl injection is gated by
    // #285).
    let argv_dump = dir.path().join("argv.txt");
    let fake = dir.path().join("cc");
    kache_fs::testutil::write_executable(
        &fake,
        format!(
            "#!/bin/sh\n: > '{dump}'\nfor a in \"$@\"; do printf '%s\\n' \"$a\" >> '{dump}'; done\nexit 0\n",
            dump = argv_dump.display()
        ),
    );

    // A real source keeps the invocation representative of cc-rs.
    let source = dir.path().join("windows.c");
    fs::write(&source, b"int main(void){return 0;}\n").unwrap();

    let cache_dir = dir.path().join("cache");
    let config = dir.path().join("kache.toml");

    // Mirror the cc-rs clang-cl shape: flags, an object output, then the
    // source behind a `--` separator. A GNU-dialect compiler name keeps
    // prefix-map injection enabled (clang-cl injection is gated by #285),
    // so this guards the argv ordering on the path that injects today.
    let output = Command::new(kache_binary())
        .args([
            fake.to_str().unwrap(),
            "-c",
            "-o",
            "windows.o",
            "--",
            source.to_str().unwrap(),
        ])
        .current_dir(dir.path())
        .env("KACHE_CACHE_DIR", &cache_dir)
        .env("KACHE_CONFIG", &config)
        .env("KACHE_BASE_DIR", dir.path())
        .env("KACHE_LOG", "kache=debug")
        .output()
        .expect("failed to run kache as a cc wrapper");

    assert!(
        output.status.success(),
        "kache cc passthrough should succeed; status={:?}\nstderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let recorded = fs::read_to_string(&argv_dump).expect("fake compiler did not record argv");
    let args: Vec<&str> = recorded.lines().collect();

    assert_eq!(
        args,
        ["-c", "-o", "windows.o", "--", source.to_str().unwrap()],
        "refused passthrough must preserve the original argv without cache-only flags"
    );
}

/// Compile-first misses retain their initial store and parsed policy through
/// publication. Exercise the read-set memo, then invalidate it with an edit.
#[test]
fn deferred_cc_reuses_setup_and_still_invalidates_changed_headers() {
    let dir = FixtureDir::new();
    let root = dir.path().canonicalize().unwrap();
    let cache = root.join("cache");
    let config = root.join("kache.toml");
    fs::write(&config, "").unwrap();
    fs::write(
        root.join("unit.c"),
        "#include \"value.h\"\nint value(void) { return VALUE; }\n",
    )
    .unwrap();
    fs::write(root.join("value.h"), "#define VALUE 42\n").unwrap();
    fs::write(
        root.join("main.c"),
        "int value(void); int main(void) { return value(); }\n",
    )
    .unwrap();

    for (phase, expected, hit) in [
        ("cold", 42, false),
        ("warm", 42, true),
        ("edited", 17, false),
    ] {
        if phase == "edited" {
            fs::write(root.join("value.h"), "#define VALUE 17\n").unwrap();
        }
        let trace = root.join(phase);
        let _ = fs::remove_file(root.join("unit.o"));
        let output = cacheable_cc_command(&root)
            .args(["cc", "-c", "unit.c", "-o", "unit.o"])
            .env("KACHE_PHASE_TRACE_DIR", &trace)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{phase}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let events = fs::read_to_string(cache.join("events.jsonl")).unwrap();
        let event: serde_json::Value =
            serde_json::from_str(events.lines().last().unwrap()).unwrap();
        assert_eq!(event["result"] == "local_hit", hit, "{phase}: {event}");
        assert_eq!(event["compiler_runs"], u32::from(!hit), "{phase}: {event}");
        assert!(
            event
                .get("store_error")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .is_empty(),
            "{phase}: {event}"
        );
        let spans: Vec<serde_json::Value> = fs::read_dir(&trace)
            .unwrap()
            .flat_map(|entry| {
                let value: serde_json::Value =
                    serde_json::from_slice(&fs::read(entry.unwrap().path()).unwrap()).unwrap();
                value["traceEvents"].as_array().unwrap().clone()
            })
            .collect();
        for name in ["cc_parse", "store_open"] {
            assert_eq!(
                spans.iter().filter(|span| span["name"] == name).count(),
                1,
                "{phase}: {name}"
            );
        }
        let status = common::hermetic_command(kache_binary(), &cache, Some(&config))
            .args(["daemon", "status", "--json"])
            .output()
            .unwrap();
        assert!(status.status.success(), "{status:?}");
        let status: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
        assert_eq!(status["daemon_running"], true, "{status}");
        let state =
            std::path::Path::new(status["socket"].as_str().unwrap()).with_extension("state.json");
        let state: serde_json::Value = serde_json::from_slice(&fs::read(state).unwrap()).unwrap();
        let daemon_pid = state["pid"].as_u64().unwrap();
        let pipeline_pid = spans
            .iter()
            .find(|span| span["name"] == "cc_parse")
            .unwrap()["pid"]
            .as_u64()
            .unwrap();
        eprintln!("{phase}: pipeline pid={pipeline_pid}, daemon pid={daemon_pid}");
        // Sandboxed clients keep compilation local. They may still hand off
        // publication, while daemon-owned compilation must publish directly.
        if pipeline_pid == daemon_pid {
            assert!(
                !spans.iter().any(|span| matches!(
                    span["name"].as_str(),
                    Some("handoff_snapshot" | "handoff_event")
                )),
                "execution inside the daemon must not hand publication back to itself"
            );
            assert!(!cache.join("store/staging/handoff").exists());
        }
        if phase == "cold" {
            assert!(
                spans.iter().any(|span| span["name"] == "cc_capture"),
                "the test must exercise compile-first discovery"
            );
            assert_eq!(event["preprocessor_runs"], 0);
        }
        assert!(
            Command::new("cc")
                .args(["main.c", "unit.o", "-o", "check-value"])
                .current_dir(&root)
                .status()
                .unwrap()
                .success()
        );
        assert_eq!(
            Command::new(root.join("check-value"))
                .status()
                .unwrap()
                .code(),
            Some(expected)
        );
    }
}

#[test]
fn exchanged_header_contents_compile_current_output_and_keep_checkout_hits() {
    for discovery in ["0", "1"] {
        let dir = FixtureDir::new();
        let root = dir.path().canonicalize().unwrap();
        let cache = root.join("cache");
        fs::create_dir(root.join("home")).unwrap();
        let header = |value| format!("#undef VALUE\n#define VALUE {value}\n");
        for copy in ["a", "b"] {
            let clone = root.join(copy);
            fs::create_dir(&clone).unwrap();
            fs::write(clone.join("kache.toml"), "").unwrap();
            fs::write(clone.join("a.h"), header(11)).unwrap();
            fs::write(clone.join("b.h"), header(23)).unwrap();
            fs::write(
                clone.join("unit.c"),
                "#include \"a.h\"\n#include \"b.h\"\nint main(void) { return VALUE; }\n",
            )
            .unwrap();
        }
        for (phase, copy, expected, result) in [
            ("cold", "a", 23, "miss"),
            ("warm", "a", 23, "local_hit"),
            ("relocated", "b", 23, "local_hit"),
            ("exchanged", "a", 11, "miss"),
            ("exchanged-warm", "a", 11, "local_hit"),
            ("restored", "a", 23, "local_hit"),
        ] {
            let clone = root.join(copy);
            if phase == "exchanged" || phase == "restored" {
                let first = if phase == "exchanged" { 23 } else { 11 };
                fs::write(clone.join("a.h"), header(first)).unwrap();
                fs::write(clone.join("b.h"), header(expected)).unwrap();
            }
            let _ = fs::remove_file(clone.join("unit.o"));
            let compiled = cacheable_cc_command(&clone)
                .args(["cc", "-c", "unit.c", "-o", "unit.o"])
                .env("HOME", root.join("home"))
                .env("KACHE_CACHE_DIR", &cache)
                .env("KACHE_RUNTIME_DIR", &cache)
                .env("KACHE_DEFERRED_DISCOVERY", discovery)
                .env("KACHE_DAEMON_PUBLISH", "0")
                .env("KACHE_MIN_STORE_COMPILE_MS", "0")
                .env_remove("RUSTC_WRAPPER")
                .output()
                .unwrap();
            assert!(compiled.status.success(), "{phase}: {compiled:?}");
            let linked = common::hermetic_command("cc", &cache, None)
                .current_dir(&clone)
                .env("HOME", root.join("home"))
                .args(["unit.o", "-o", "check-value"])
                .output()
                .unwrap();
            assert!(linked.status.success(), "{phase}: {linked:?}");
            assert_eq!(
                Command::new(clone.join("check-value"))
                    .status()
                    .unwrap()
                    .code(),
                Some(expected),
                "{phase}: exchanging header contents must compile their current meaning \
                 (discovery={discovery})"
            );
            let events = fs::read_to_string(cache.join("events.jsonl")).unwrap();
            let event: serde_json::Value =
                serde_json::from_str(events.lines().last().unwrap()).unwrap();
            assert_eq!(event["result"], result, "{phase}: {event}");
        }
    }
}

#[test]
fn changed_link_archives_use_current_code_and_explicit_files_keep_cache_hits() {
    for style in [
        "wl-rsp",
        "named",
        "forwarded",
        "xlinker",
        "response",
        "xl-rsp",
        "explicit",
        #[cfg(target_os = "linux")]
        "explicit-search-path",
        "default",
    ] {
        let dir = FixtureDir::new();
        let root = dir.path().canonicalize().unwrap();
        fs::create_dir(root.join("home")).unwrap();
        fs::write(root.join("kache.toml"), "").unwrap();
        let main = "int value(void); int main(void) { return value(); }\n";
        fs::write(root.join("main.c"), main).unwrap();
        let direct_cc = |args: &[&str]| {
            common::hermetic_command("cc", &root.join("cache"), None)
                .env("HOME", root.join("home"))
                .current_dir(&root)
                .args(args)
                .output()
                .unwrap()
        };
        let compile = direct_cc(&["-c", "main.c", "-o", "main.o"]);
        assert!(compile.status.success(), "{compile:?}");
        fs::write(root.join("link.rsp"), "main.o -L. \"-lvalue\" -o app\n").unwrap();
        fs::write(root.join("l.rsp"), "-lvalue\n").unwrap();
        let options: &[&str] = match style {
            "wl-rsp" => &["main.o", "-L.", "-Wl,@l.rsp", "-o", "app"],
            "xl-rsp" => &["main.o", "-L.", "-Xlinker", "@l.rsp", "-o", "app"],
            "forwarded" => &["main.o", "-L.", "-Wl,-lvalue", "-o", "app"],
            "xlinker" => &["main.o", "-L.", "-Xlinker", "-lvalue", "-o", "app"],
            "response" => &["@link.rsp", "-o", "app"],
            "explicit" => &["main.o", "libvalue.a", "-o", "app"],
            "explicit-search-path" => {
                &["main.o", "-Wl,--library-path=.", "libvalue.a", "-o", "app"]
            }
            _ => &["main.o", "-L.", "-lvalue", "-o", "app"],
        };
        let mut results = Vec::new();
        for (phase, expected) in [
            ("cold", 11),
            ("warm", 11),
            ("changed", 23),
            ("changed-warm", 23),
        ] {
            if phase == "cold" || phase == "changed" {
                let code = format!("int value(void) {{ return {expected}; }}\n");
                fs::write(root.join("value.c"), code).unwrap();
                let compile = direct_cc(&["-c", "value.c", "-o", "value.o"]);
                assert!(compile.status.success(), "{compile:?}");
                let _ = fs::remove_file(root.join("libvalue.a"));
                let archive = common::hermetic_command("ar", &root.join("cache"), None)
                    .env("HOME", root.join("home"))
                    .current_dir(&root)
                    .args(["rcs", "libvalue.a", "value.o"])
                    .output()
                    .unwrap();
                assert!(archive.status.success(), "{archive:?}");
            }
            let _ = fs::remove_file(root.join("app"));
            let linked = cacheable_cc_command(&root)
                .arg("cc")
                .args(options)
                .env("HOME", root.join("home"))
                .env(
                    "KACHE_CACHE_CC_LINKS",
                    if style == "default" { "0" } else { "1" },
                )
                .env("KACHE_DAEMON_PUBLISH", "0")
                .env("KACHE_MIN_STORE_COMPILE_MS", "0")
                .env_remove("RUSTC_WRAPPER")
                .output()
                .unwrap();
            assert!(linked.status.success(), "{style}/{phase}: {linked:?}");
            assert_eq!(
                Command::new(root.join("app")).status().unwrap().code(),
                Some(expected),
                "{style}/{phase}: a changed archive must run its current code"
            );
            results.push(last_cc_event(&root)["result"].clone());
        }
        let cacheable = matches!(style, "explicit" | "explicit-search-path");
        let expected_results = if cacheable {
            vec!["miss", "local_hit", "miss", "local_hit"]
        } else {
            vec!["passthrough"; 4]
        };
        assert_eq!(
            results, expected_results,
            "{style}: explicit archives retain reuse and untracked selections pass through"
        );
        if !cacheable && root.join("cache/index.db").is_file() {
            assert_eq!(
                index_rows(&root, "entries"),
                0,
                "{style}: untracked or default links must not publish cache entries"
            );
        }
    }
}

#[test]
fn cc_header_availability_creation_uses_current_compiler_output() {
    for discovery in ["0", "1"] {
        for in_header in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().canonicalize().unwrap();
            fs::write(root.join("kache.toml"), "").unwrap();
            fs::create_dir(root.join("home")).unwrap();
            let availability = "#if __has_include(\"optional.h\")\n\
                                #include \"optional.h\"\n\
                                #else\n#define VALUE 11\n#endif\n";
            let prefix = if in_header {
                fs::write(
                    root.join("availability.h"),
                    availability.replace("__has_include", "__has_inc\\\r\nlude"),
                )
                .unwrap();
                "#include \"availability.h\"\n"
            } else {
                availability
            };
            fs::write(
                root.join("unit.c"),
                format!("{prefix}int main(void) {{ return VALUE; }}\n"),
            )
            .unwrap();
            for (phase, expected) in [
                ("cold", 11),
                ("warm", 11),
                ("created", 23),
                ("created-warm", 23),
                ("modified", 31),
                ("modified-warm", 31),
                ("removed", 11),
                ("recreated", 47),
            ] {
                match phase {
                    "created" | "modified" | "recreated" => {
                        fs::write(
                            root.join("optional.h"),
                            format!("#define VALUE {expected}\n"),
                        )
                        .unwrap();
                    }
                    "removed" => fs::remove_file(root.join("optional.h")).unwrap(),
                    _ => {}
                }
                let _ = fs::remove_file(root.join("unit.o"));
                let compiled = cacheable_cc_command(&root)
                    .args(["/usr/bin/cc", "-c", "unit.c", "-o", "unit.o"])
                    .env("HOME", root.join("home"))
                    .env("KACHE_DEFERRED_DISCOVERY", discovery)
                    .env("KACHE_DAEMON_PUBLISH", "0")
                    .env("KACHE_MIN_STORE_COMPILE_MS", "0")
                    .output()
                    .unwrap();
                assert!(
                    compiled.status.success(),
                    "{phase}: {}",
                    String::from_utf8_lossy(&compiled.stderr)
                );
                let linked = common::hermetic_command(
                    "/usr/bin/cc",
                    &root.join("cache"),
                    Some(&root.join("kache.toml")),
                )
                .current_dir(&root)
                .args(["unit.o", "-o", "check-value"])
                .output()
                .unwrap();
                assert!(linked.status.success(), "{phase}: {linked:?}");
                assert_eq!(
                    Command::new(root.join("check-value"))
                        .status()
                        .unwrap()
                        .code(),
                    Some(expected),
                    "{phase}: __has_include must observe newly created headers \
                     (discovery={discovery}, in_header={in_header})"
                );
            }
        }
    }
}

/// Compile `unit.c` in `root` through kache with `args` after the source,
/// then link and run it. Returns the cache key the compile used and the
/// program's exit code.
fn compile_and_run_unit(root: &std::path::Path, discovery: &str, args: &[&str]) -> (String, i32) {
    let _ = fs::remove_file(root.join("unit.o"));
    let compiled = cacheable_cc_command(root)
        .args(["/usr/bin/cc", "-c", "unit.c", "-o", "unit.o"])
        .args(args)
        .env("HOME", root.join("home"))
        .env("KACHE_DEFERRED_DISCOVERY", discovery)
        .env("KACHE_DAEMON_PUBLISH", "0")
        .env("KACHE_MIN_STORE_COMPILE_MS", "0")
        .output()
        .unwrap();
    assert!(
        compiled.status.success(),
        "{}",
        String::from_utf8_lossy(&compiled.stderr)
    );
    let events = fs::read_to_string(root.join("cache/events.jsonl")).unwrap();
    let event: serde_json::Value = serde_json::from_str(events.lines().last().unwrap()).unwrap();
    let linked = Command::new("/usr/bin/cc")
        .current_dir(root)
        .args(["unit.o", "-o", "check-value"])
        .status()
        .unwrap();
    assert!(linked.success());
    let value = Command::new(root.join("check-value"))
        .status()
        .unwrap()
        .code()
        .unwrap();
    (event["cache_key"].as_str().unwrap().to_string(), value)
}

/// Exercise the optional header lifecycle in both discovery modes. A newly
/// discovered header must join the memo read set so later edits invalidate it.
fn assert_probe_follows_optional_header(root: &std::path::Path, args: &[&str], absent: i32) {
    for discovery in ["0", "1"] {
        let _ = fs::remove_file(root.join("inc/optional.h"));
        let _ = fs::remove_dir_all(root.join("cache"));
        let (cold, value) = compile_and_run_unit(root, discovery, args);
        assert!(
            !cold.is_empty(),
            "discovery={discovery}: the unit must have a key"
        );
        assert_eq!(value, absent, "discovery={discovery}: cold");
        let (warm, value) = compile_and_run_unit(root, discovery, args);
        assert_eq!(warm, cold, "discovery={discovery}: the key must hold");
        assert_eq!(value, absent, "discovery={discovery}: warm");
        fs::write(root.join("inc/optional.h"), "#define VALUE 23\n").unwrap();
        let (created, value) = compile_and_run_unit(root, discovery, args);
        assert_ne!(
            created, cold,
            "discovery={discovery}: the probed header appearing must change the key"
        );
        assert_eq!(value, 23, "discovery={discovery}: created");
        fs::write(root.join("inc/optional.h"), "#define VALUE 31\n").unwrap();
        let (modified, value) = compile_and_run_unit(root, discovery, args);
        assert_ne!(
            modified, created,
            "the newly read header must enter the memo"
        );
        assert_eq!(value, 31);
        let (warm_modified, value) = compile_and_run_unit(root, discovery, args);
        assert_eq!(warm_modified, modified);
        assert_eq!(value, 31);
        assert_eq!(last_cc_event(root)["result"], "local_hit");
        fs::remove_file(root.join("inc/optional.h")).unwrap();
        let (_, value) = compile_and_run_unit(root, discovery, args);
        assert_eq!(
            value, absent,
            "removing the header must restore the fallback"
        );
        fs::write(root.join("inc/optional.h"), "#define VALUE 47\n").unwrap();
        let (recreated, value) = compile_and_run_unit(root, discovery, args);
        assert_ne!(recreated, created);
        assert_eq!(value, 47);
    }
}

/// A unit probes for a header that is absent. The header then appears in a
/// user `-I` directory. The key must change and the object must use it.
#[test]
fn cc_header_probe_keys_a_header_appearing_in_a_user_include_dir() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    fs::write(root.join("kache.toml"), "").unwrap();
    fs::create_dir(root.join("home")).unwrap();
    fs::create_dir(root.join("inc")).unwrap();
    fs::write(
        root.join("unit.c"),
        "#if __has_include(<optional.h>)\n#include <optional.h>\n\
         #else\n#define VALUE 11\n#endif\n\
         int main(void) { return VALUE; }\n",
    )
    .unwrap();
    for args in [["-Iinc"], ["-isysteminc"], ["-idirafterinc"]] {
        assert_probe_follows_optional_header(&root, &args, 11);
    }
}

#[test]
fn absolute_angle_header_probe_follows_its_optional_header() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    fs::write(root.join("kache.toml"), "").unwrap();
    fs::create_dir(root.join("home")).unwrap();
    fs::create_dir(root.join("inc")).unwrap();
    let optional = root.join("inc/optional.h");
    fs::write(
        root.join("unit.c"),
        format!(
            "#if __has_include(<{0}>)\n#include <{0}>\n\
             #else\n#define VALUE 11\n#endif\n\
             int main(void) {{ return VALUE; }}\n",
            optional.display()
        ),
    )
    .unwrap();
    assert_probe_follows_optional_header(&root, &[], 11);
}

/// A header outside every user directory, as a system header is, probes
/// for a header and carries the macOS SDK's fallback definition of the
/// operator. The unit keeps a key, and the key follows the probe when the
/// header appears in a user `-I` directory.
#[test]
fn cc_header_probe_outside_user_include_dirs_keys_the_unit() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    fs::write(root.join("kache.toml"), "").unwrap();
    fs::create_dir(root.join("home")).unwrap();
    fs::create_dir(root.join("inc")).unwrap();
    fs::create_dir(root.join("system")).unwrap();
    fs::write(
        root.join("system/probing.h"),
        "#ifndef __has_include\n#define __has_include(x) 0\n#endif\n\
         #if defined(__has_include) && __has_include(<optional.h>)\n\
         #include <optional.h>\n#else\n#define VALUE 5\n#endif\n",
    )
    .unwrap();
    fs::write(
        root.join("unit.c"),
        "#include <probing.h>\nint main(void) { return VALUE; }\n",
    )
    .unwrap();
    assert_probe_follows_optional_header(&root, &["-isystem", "system", "-Iinc"], 5);
}

#[test]
fn quoted_header_probe_outside_user_dirs_follows_its_local_header() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    fs::write(root.join("kache.toml"), "").unwrap();
    fs::create_dir(root.join("home")).unwrap();
    fs::create_dir(root.join("inc")).unwrap();
    fs::create_dir(root.join("elsewhere")).unwrap();
    fs::write(root.join("elsewhere/probe.h"),
        "#if __has_include(\"optional.h\")\n#include \"optional.h\"\n#else\n#define VALUE 11\n#endif\n").unwrap();
    fs::write(
        root.join("unit.c"),
        "#include \"elsewhere/probe.h\"\nint main(void) { return VALUE; }\n",
    )
    .unwrap();
    for discovery in ["0", "1"] {
        let _ = fs::remove_file(root.join("elsewhere/optional.h"));
        let _ = fs::remove_dir_all(root.join("cache"));
        assert_eq!(compile_and_run_unit(&root, discovery, &[]).1, 11);
        assert_eq!(compile_and_run_unit(&root, discovery, &[]).1, 11);
        for value in [23, 31] {
            fs::write(
                root.join("elsewhere/optional.h"),
                format!("#define VALUE {value}\n"),
            )
            .unwrap();
            assert_eq!(compile_and_run_unit(&root, discovery, &[]).1, value);
        }
        fs::remove_file(root.join("elsewhere/optional.h")).unwrap();
        assert_eq!(compile_and_run_unit(&root, discovery, &[]).1, 11);
    }
}

#[test]
fn a_header_created_after_preprocessing_cannot_publish_an_incomplete_read_set() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    fs::write(root.join("kache.toml"), "").unwrap();
    fs::create_dir(root.join("home")).unwrap();
    fs::create_dir(root.join("inc")).unwrap();
    fs::write(root.join("unit.c"),
        "#if __has_include(<optional.h>)\n#include <optional.h>\n#else\n#define VALUE 11\n#endif\nint main(void) { return VALUE; }\n").unwrap();
    let compiler = root.join("cc");
    kache_fs::testutil::write_executable(
        &compiler,
        concat!(
            "#!/bin/sh\n",
            "preprocess=0; unit=0\n",
            "for arg in \"$@\"; do case \"$arg\" in -E) preprocess=1;; unit.c) unit=1;; esac; done\n",
            "/usr/bin/cc \"$@\"\nstatus=$?\n",
            "if [ \"$status:$preprocess:$unit\" = '0:1:1' ]; then\n",
            "  printf '#define VALUE 23\n' > inc/optional.h\n",
            "  : > injected\nfi\nexit \"$status\"\n",
        ),
    );
    let compiled = cacheable_cc_command(&root)
        .args([
            compiler.to_str().unwrap(),
            "-c",
            "unit.c",
            "-Iinc",
            "-o",
            "unit.o",
        ])
        .env("HOME", root.join("home"))
        .env("KACHE_DEFERRED_DISCOVERY", "0")
        .env("KACHE_DAEMON_PUBLISH", "0")
        .env("KACHE_MIN_STORE_COMPILE_MS", "0")
        .output()
        .unwrap();
    assert!(compiled.status.success(), "{compiled:?}");
    assert!(
        root.join("injected").is_file(),
        "the header must appear after the unit's preprocess"
    );
    assert_eq!(index_rows(&root, "cc_preprocess_memos"), 0);
    assert_eq!(index_rows(&root, "entries"), 0);
}

fn cacheable_cc_command(root: &std::path::Path) -> Command {
    common::settle_writes(&[root]);
    let mut command = Command::new(kache_binary());
    command
        .current_dir(root)
        .env("KACHE_CACHE_DIR", root.join("cache"))
        .env("KACHE_RUNTIME_DIR", root.join("cache"))
        .env("KACHE_CONFIG", root.join("kache.toml"))
        .env("KACHE_HOST_CONFIG", "")
        .env("KACHE_BASE_DIR", root)
        .env("KACHE_LOCAL_ONLY", "1")
        .env("KACHE_DEFERRED_DISCOVERY", "1")
        .env("KACHE_DAEMON_PUBLISH", "1")
        .env_remove("OUT_DIR")
        .env_remove("KACHE_ACTIVE")
        .env_remove("KACHE_SOCKET_PATH");
    command
}

#[test]
fn cc_admission_reports_a_skip_reason_and_recompiles_until_admitted() {
    let dir = FixtureDir::new();
    let root = dir.path().canonicalize().unwrap();
    fs::write(root.join("kache.toml"), "").unwrap();
    fs::write(root.join("unit.c"), "int value(void) { return 42; }\n").unwrap();

    for (threshold, result, reason) in [
        ("18446744073709551615", "skipped", "admission-threshold"),
        ("18446744073709551615", "skipped", "admission-threshold"),
        ("0", "miss", ""),
        ("0", "local_hit", ""),
    ] {
        let _ = fs::remove_file(root.join("unit.o"));
        let output = cacheable_cc_command(&root)
            .args(["cc", "-c", "unit.c", "-o", "unit.o"])
            .env("KACHE_MIN_STORE_COMPILE_MS", threshold)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{result}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(root.join("unit.o").is_file());
        let events = fs::read_to_string(root.join("cache/events.jsonl")).unwrap();
        let event: serde_json::Value =
            serde_json::from_str(events.lines().last().unwrap()).unwrap();
        assert_eq!(event["result"], result, "{event}");
        assert_eq!(
            event
                .get("skip_reason")
                .and_then(|value| value.as_str())
                .unwrap_or(""),
            reason,
            "{event}"
        );
        assert_eq!(event["compiler_runs"], u32::from(result != "local_hit"));
    }
}

#[test]
fn deferred_cc_does_not_publish_inputs_changed_during_the_compile() {
    let dir = FixtureDir::new();
    let root = dir.path().canonicalize().unwrap();
    fs::write(root.join("kache.toml"), "").unwrap();
    fs::write(
        root.join("unit.c"),
        "#include \"value.h\"\nint value(void) { return VALUE; }\n",
    )
    .unwrap();
    fs::write(root.join("value.h"), "#define VALUE 42\n").unwrap();
    fs::write(
        root.join("main.c"),
        "int value(void); int main(void) { return value(); }\n",
    )
    .unwrap();
    let compiler = root.join("cc");
    kache_fs::testutil::write_executable(
        &compiler,
        r#"#!/bin/sh
for argument in "$@"; do
    case "$argument" in -###|--version|-E) exec cc "$@" ;; esac
done
cc "$@"
status=$?
if [ "$status" = 0 ]; then
    case " $* " in *" unit.c "*) printf '#define VALUE 17\n' > value.h ;; esac
fi
exit "$status"
"#,
    );
    let output = cacheable_cc_command(&root)
        .arg(&compiler)
        .args(["-c", "unit.c", "-o", "unit.o"])
        .env("KACHE_LOG", "kache=debug")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read_to_string(root.join("value.h")).unwrap(),
        "#define VALUE 17\n"
    );
    assert!(
        Command::new("cc")
            .args(["main.c", "unit.o", "-o", "check-value"])
            .current_dir(&root)
            .status()
            .unwrap()
            .success()
    );
    assert_eq!(
        Command::new(root.join("check-value"))
            .status()
            .unwrap()
            .code(),
        Some(42)
    );
    let db = rusqlite::Connection::open(root.join("cache/index.db")).unwrap();
    for table in ["entries", "cc_preprocess_memos"] {
        let count: i64 = db
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "a changed input must not populate {table}");
    }
}

/// A key taken before the compile hashed `value.h`, and the header was
/// saved before the compiler read it. The object holds the new value under
/// the old header's key, so neither the entry nor the memo may be stored.
#[test]
fn key_first_cc_does_not_store_an_input_changed_before_the_compile_read_it() {
    let dir = FixtureDir::new();
    let root = dir.path().canonicalize().unwrap();
    fs::write(root.join("kache.toml"), "").unwrap();
    fs::write(
        root.join("unit.c"),
        "#include \"value.h\"\nint value(void) { return VALUE; }\n",
    )
    .unwrap();
    fs::write(root.join("value.h"), "#define VALUE 42\n").unwrap();
    fs::write(
        root.join("main.c"),
        "int value(void); int main(void) { return value(); }\n",
    )
    .unwrap();
    let compiler = root.join("cc");
    kache_fs::testutil::write_executable(
        &compiler,
        r#"#!/bin/sh
for argument in "$@"; do
    case "$argument" in -###|--version|-E) exec cc "$@" ;; esac
done
case " $* " in *" unit.c "*) printf '#define VALUE 17\n' > value.h ;; esac
exec cc "$@"
"#,
    );
    let output = cacheable_cc_command(&root)
        .arg(&compiler)
        .args(["-c", "unit.c", "-o", "unit.o"])
        .env("KACHE_DEFERRED_DISCOVERY", "0")
        .env("KACHE_LOG", "kache=debug")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(last_cc_event(&root)["preprocessor_runs"], 1, "keyed first");
    assert!(
        Command::new("cc")
            .args(["main.c", "unit.o", "-o", "check-value"])
            .current_dir(&root)
            .status()
            .unwrap()
            .success()
    );
    assert_eq!(
        Command::new(root.join("check-value"))
            .status()
            .unwrap()
            .code(),
        Some(17),
        "the compile read the saved header"
    );
    let db = rusqlite::Connection::open(root.join("cache/index.db")).unwrap();
    for table in ["entries", "cc_preprocess_memos"] {
        let count: i64 = db
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "a changed input must not populate {table}");
    }
}

/// A key taken before the compile hashes the files the preprocessor
/// reported. A header saved between that read and the key's own changes
/// which files the compile reads, while every file the key hashed stays as
/// the key saw it. Only the header's stamp, from after the build started,
/// shows the save. Stored, the object would come back after an edit to the
/// header it now includes.
#[test]
fn key_first_cc_does_not_store_a_header_saved_while_it_was_keyed() {
    let dir = FixtureDir::new();
    let root = dir.path().canonicalize().unwrap();
    fs::write(root.join("kache.toml"), "").unwrap();
    fs::write(
        root.join("unit.c"),
        "#include \"value.h\"\nint value(void) { return VALUE; }\n",
    )
    .unwrap();
    fs::write(root.join("value.h"), "#include \"a.h\"\n").unwrap();
    fs::write(root.join("a.h"), "#define VALUE 1\n").unwrap();
    fs::write(root.join("b.h"), "#define VALUE 2\n").unwrap();
    fs::write(
        root.join("main.c"),
        "int value(void); int main(void) { return value(); }\n",
    )
    .unwrap();
    let compiler = root.join("cc");
    kache_fs::testutil::write_executable(
        &compiler,
        r#"#!/bin/sh
for argument in "$@"; do
    case "$argument" in
        -###|--version) exec cc "$@" ;;
        -E) cc "$@"; status=$?; printf '#include "b.h"\n' > value.h; exit "$status" ;;
    esac
done
exec cc "$@"
"#,
    );

    for (phase, expected) in [("saved while keyed", 2), ("included header edited", 3)] {
        if expected == 3 {
            fs::write(root.join("b.h"), "#define VALUE 3\n").unwrap();
        }
        let _ = fs::remove_file(root.join("unit.o"));
        let output = cacheable_cc_command(&root)
            .arg(&compiler)
            .args(["-c", "unit.c", "-o", "unit.o"])
            .env("KACHE_DEFERRED_DISCOVERY", "0")
            .env("KACHE_LOG", "kache=debug")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{phase}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            last_cc_event(&root)["preprocessor_runs"],
            1,
            "{phase}: keyed first"
        );
        assert!(
            Command::new("cc")
                .args(["main.c", "unit.o", "-o", "check-value"])
                .current_dir(&root)
                .status()
                .unwrap()
                .success()
        );
        assert_eq!(
            Command::new(root.join("check-value"))
                .status()
                .unwrap()
                .code(),
            Some(expected),
            "{phase}"
        );
        for table in ["entries", "cc_preprocess_memos"] {
            assert_eq!(index_rows(&root, table), 0, "{phase}: {table}");
        }
    }
}

/// A header stamped an hour ahead, as a skewed file server or a
/// future-dated checkout leaves it, was not written during the build. A
/// key taken before the compile stores the result, and the next run hits.
#[test]
fn key_first_cc_caches_a_header_stamped_ahead_of_the_clock() {
    let dir = FixtureDir::new();
    let root = dir.path().canonicalize().unwrap();
    fs::write(root.join("kache.toml"), "").unwrap();
    fs::write(
        root.join("unit.c"),
        "#include \"value.h\"\nint value(void) { return VALUE; }\n",
    )
    .unwrap();
    fs::write(root.join("value.h"), "#define VALUE 42\n").unwrap();
    let ahead = std::time::SystemTime::now() + std::time::Duration::from_secs(3600);
    filetime::set_file_mtime(
        root.join("value.h"),
        filetime::FileTime::from_system_time(ahead),
    )
    .unwrap();

    for expected in ["miss", "local_hit"] {
        let _ = fs::remove_file(root.join("unit.o"));
        let output = cacheable_cc_command(&root)
            .args(["cc", "-c", "unit.c", "-o", "unit.o"])
            .env("KACHE_DEFERRED_DISCOVERY", "0")
            .env("KACHE_LOG", "kache=debug")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{expected}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(root.join("unit.o").is_file(), "{expected}");
        let event = last_cc_event(&root);
        assert_eq!(event["result"], expected, "{event}");
    }
}

fn index_rows(root: &std::path::Path, table: &str) -> i64 {
    let index = rusqlite::Connection::open_with_flags(
        root.join("cache/index.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    // Passthrough may open the hash memo without initializing entry tables.
    let exists: bool = index
        .query_row(
            "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
            [table],
            |row| row.get(0),
        )
        .unwrap();
    if !exists {
        return 0;
    }
    index
        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
}

fn last_cc_event(root: &std::path::Path) -> serde_json::Value {
    let events = fs::read_to_string(root.join("cache/events.jsonl")).unwrap();
    serde_json::from_str(events.lines().last().unwrap()).unwrap()
}

fn cc_memo_count(root: &std::path::Path) -> i64 {
    rusqlite::Connection::open(root.join("cache/index.db"))
        .unwrap()
        .query_row("SELECT COUNT(*) FROM cc_preprocess_memos", [], |row| {
            row.get(0)
        })
        .unwrap()
}

/// A compile-first miss that finds its entry already committed leaves that
/// entry alone but still records its memo, so the next run hits
/// (kunobi-ninja/kache#1390).
#[test]
fn deferred_cc_records_its_memo_when_the_entry_already_exists() {
    let dir = FixtureDir::new();
    let root = dir.path().canonicalize().unwrap();
    fs::write(root.join("kache.toml"), "").unwrap();
    fs::write(
        root.join("unit.c"),
        "int square(int value) { return value * value; }\n",
    )
    .unwrap();
    let compile = |deferred: &str| {
        let output = cacheable_cc_command(&root)
            .args(["cc", "-c", "unit.c", "-o", "unit.o"])
            .env("KACHE_DEFERRED_DISCOVERY", deferred)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        last_cc_event(&root)
    };

    let seeded = compile("0");
    assert_eq!(seeded["result"], "miss", "{seeded}");
    // A memo pruned or retired while its entry stays.
    rusqlite::Connection::open(root.join("cache/index.db"))
        .unwrap()
        .execute_batch("DELETE FROM cc_memo_input_refs; DELETE FROM cc_preprocess_memos;")
        .unwrap();

    let deferred = compile("1");
    assert_eq!(deferred["result"], "miss", "{deferred}");
    assert_eq!(deferred["compiler_runs"], 1, "{deferred}");
    assert_eq!(deferred["cache_key"], seeded["cache_key"]);
    assert_eq!(
        cc_memo_count(&root),
        1,
        "the deferred compile records its memo"
    );

    let next = compile("1");
    assert_eq!(next["result"], "local_hit", "{next}");
    assert_eq!(next["compiler_runs"], 0, "{next}");
}

/// Two byte-identical sources are still two translation units: each object
/// spells its own path, so they must not share an entry.
#[test]
fn identical_sources_at_different_paths_get_their_own_entries() {
    let dir = FixtureDir::new();
    let root = dir.path().canonicalize().unwrap();
    fs::write(root.join("kache.toml"), "").unwrap();
    let unit = "const char *unit_file(void) { return __FILE__; }\n";
    for copy in ["a", "b"] {
        fs::create_dir(root.join(copy)).unwrap();
        fs::write(root.join(copy).join("unit.c"), unit).unwrap();
    }
    fs::write(
        root.join("main.c"),
        "#include <stdio.h>\nconst char *unit_file(void);\nint main(void) { puts(unit_file()); return 0; }\n",
    )
    .unwrap();

    for deferred in ["0", "1"] {
        dir.stop_daemon();
        let _ = fs::remove_dir_all(root.join("cache"));
        let mut keys = Vec::new();
        for copy in ["a", "b", "b"] {
            let output = cacheable_cc_command(&root)
                .args(["cc", "-c", &format!("{copy}/unit.c"), "-o", "unit.o"])
                .env("KACHE_DEFERRED_DISCOVERY", deferred)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            keys.push(last_cc_event(&root)["cache_key"].clone());
        }
        assert_ne!(
            keys[0], keys[1],
            "deferred={deferred}: the copies share a key"
        );
        assert_eq!(keys[1], keys[2], "deferred={deferred}: b's key is stable");
        assert!(
            Command::new("cc")
                .args(["main.c", "unit.o", "-o", "which-file"])
                .current_dir(&root)
                .status()
                .unwrap()
                .success()
        );
        let printed = Command::new(root.join("which-file")).output().unwrap();
        assert_eq!(
            String::from_utf8_lossy(&printed.stdout).trim(),
            "b/unit.c",
            "deferred={deferred}"
        );
    }
}
