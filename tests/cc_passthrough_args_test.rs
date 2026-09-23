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

fn kache_binary() -> &'static str {
    env!("CARGO_BIN_EXE_kache")
}

#[test]
fn refused_double_dash_invocation_is_exact_passthrough() {
    let dir = tempfile::tempdir().unwrap();

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
    let dir = tempfile::tempdir().unwrap();
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
        assert!(
            !spans.iter().any(|span| matches!(
                span["name"].as_str(),
                Some("handoff_snapshot" | "handoff_event")
            )),
            "a missing daemon must not add handoff staging work"
        );
        assert!(!cache.join("store/staging/handoff").exists());
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

fn cacheable_cc_command(root: &std::path::Path) -> Command {
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
fn deferred_cc_does_not_publish_inputs_changed_during_the_compile() {
    let dir = tempfile::tempdir().unwrap();
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
    case "$argument" in -###|--version|-E) exec /usr/bin/cc "$@" ;; esac
done
/usr/bin/cc "$@"
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
