//! Terminal width can be shared only when rustc cannot emit lint warnings.

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
    compile_with_log(project, cache, wrapped, flags, "off")
}

fn compile_with_log(
    project: &Path,
    cache: &Path,
    wrapped: bool,
    flags: &[&str],
    log: &str,
) -> Output {
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
            "width_fixture",
            "--crate-type=rlib",
            "--emit=dep-info,metadata,link",
            "--out-dir",
        ])
        .arg(out)
        .arg(project.join("lib.rs"))
        .args(flags)
        .env("KACHE_LOG", log)
        .current_dir(project);
    if !flags
        .iter()
        .any(|flag| *flag == "--json" || flag.starts_with("--json="))
    {
        command.arg("--color=never");
    }
    command.output().unwrap()
}

fn diagnostics(output: &Output, project: &Path) -> String {
    normalize_diagnostic_paths(
        std::str::from_utf8(&output.stderr).unwrap(),
        project.to_str().unwrap(),
    )
}

fn normalize_diagnostic_paths(stderr: &str, project: &str) -> String {
    // Compare rendering apart from Kache's existing stable workspace paths.
    let json_project = serde_json::to_string(project).unwrap();
    stderr
        .replace(&json_project[1..json_project.len() - 1], "<PROJECT>")
        .replace(project, "<PROJECT>")
        .replace("/proc/self/cwd", "<PROJECT>")
        .replace("/kache/workspace", "<PROJECT>")
}

#[test]
fn windows_project_paths_preserve_rendered_diagnostic_comparisons() {
    let project = r"D:\a\kache\target\scratch";
    let record = |path: &str, line: &str| {
        serde_json::json!({
            "file_name": path,
            "rendered": format!("warning: unused variable\n --> {path}:1:30\n1 | {line}\n"),
        })
        .to_string()
    };
    let full_line = "let unused = \"a long value\";";
    let direct = record(&format!(r"{project}\lib.rs"), full_line);
    let wrapped = record(r"/kache/workspace\lib.rs", full_line);
    let expected = record(r"<PROJECT>\lib.rs", full_line);
    assert!(direct.contains(r"D:\\a\\kache"), "{direct}");
    assert_eq!(normalize_diagnostic_paths(&direct, project), expected);
    assert_eq!(normalize_diagnostic_paths(&wrapped, project), expected);
    assert_eq!(
        normalize_diagnostic_paths(&format!(r"warning at {project}\lib.rs"), project),
        r"warning at <PROJECT>\lib.rs"
    );
    let narrow = record(&format!(r"{project}\lib.rs"), "...unused...");
    assert_ne!(
        normalize_diagnostic_paths(&direct, project),
        normalize_diagnostic_paths(&narrow, project),
        "normalizing paths must preserve width-dependent rendered text"
    );
}

fn json_records(output: &Output) -> Vec<String> {
    let mut records: Vec<_> = String::from_utf8_lossy(&output.stderr)
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap().to_string())
        .collect();
    // Rustc can report concurrently emitted artifacts in different orders.
    records.sort();
    records
}

#[test]
fn warm_capped_cache_does_not_hide_argument_errors_or_warning_widths() {
    build_kache();
    let fixture = tempfile::tempdir_in(common::scratch_dir()).unwrap();
    let project = fixture.path();
    let cache = project.join("cache");
    write_config(&cache);
    let _daemon = common::DaemonGuard::new(kache_binary(), &cache);
    fs::write(
        project.join("lib.rs"),
        format!(
            "pub fn answer() -> u32 {{ let unused = \"{}\"; 42 }}\n",
            "x".repeat(180)
        ),
    )
    .unwrap();

    for flags in [
        vec!["--cap-lints=allow"],
        vec!["--cap-lints=allow", "--diagnostic-width=40"],
        vec!["--cap-lints", "allow", "--diagnostic-width", "120"],
    ] {
        let output = compile(project, &cache, true, &flags);
        assert!(output.status.success(), "{output:?}");
        assert!(output.stderr.is_empty(), "{output:?}");
    }
    let shared = events(&cache, "width_fixture");
    assert_eq!(shared.len(), 3);
    assert_eq!(shared[0]["result"], "miss");
    assert_eq!(shared[1]["result"], "local_hit");
    // Cap-lint spelling remains keyed; width spelling does not.
    assert_eq!(shared[0]["cache_key"], shared[1]["cache_key"]);

    for malformed in [
        vec!["--diagnostic-width=garbage"],
        vec!["--diagnostic-width="],
        vec!["--diagnostic-width"],
        vec!["--diagnostic-width=40", "--diagnostic-width=120"],
    ] {
        let mut flags = vec!["--cap-lints=allow"];
        flags.extend_from_slice(&malformed);
        let output = compile(project, &cache, true, &flags);
        assert!(!output.status.success(), "{flags:?}: {output:?}");
        assert!(String::from_utf8_lossy(&output.stderr).contains("diagnostic-width"));
    }

    for lint_flags in [
        vec![],
        vec!["--cap-lints=allow", "--force-warn=unused_variables"],
        vec!["--cap-lints=warn", "--cap-lints=allow"],
    ] {
        let mut rendered = Vec::new();
        for width in ["--diagnostic-width=40", "--diagnostic-width=120"] {
            let mut flags = lint_flags.clone();
            flags.push(width);
            let direct = compile(project, &cache, false, &flags);
            assert!(direct.status.success(), "{direct:?}");
            assert!(String::from_utf8_lossy(&direct.stderr).contains("unused variable"));
            for _ in 0..2 {
                let cached = compile(project, &cache, true, &flags);
                assert!(cached.status.success(), "{cached:?}");
                assert_eq!(
                    diagnostics(&cached, project),
                    diagnostics(&direct, project),
                    "{flags:?}"
                );
            }
            rendered.push(direct.stderr);
        }
        assert_ne!(
            rendered[0], rendered[1],
            "fixture must exercise width rendering"
        );
        let recorded = events(&cache, "width_fixture");
        let pair = &recorded[recorded.len() - 4..];
        assert_eq!(pair[1]["result"], "local_hit");
        assert_eq!(pair[3]["result"], "local_hit");
        assert_ne!(pair[0]["cache_key"], pair[2]["cache_key"]);
    }
}

#[test]
fn warm_nightly_cache_replays_silent_unused_externs_across_widths() {
    build_kache();
    let fixture = tempfile::tempdir_in(common::scratch_dir()).unwrap();
    let project = fixture.path();
    let cache = project.join("cache");
    write_config(&cache);
    let _daemon = common::DaemonGuard::new(kache_binary(), &cache);
    fs::write(project.join("dep.rs"), "pub fn unused_dependency() {}\n").unwrap();
    fs::write(
        project.join("lib.rs"),
        format!(
            "pub fn answer() -> u32 {{ let unused = \"{}\"; 42 }}\n",
            "x".repeat(180)
        ),
    )
    .unwrap();
    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let dependency = project.join("libwidth_dep.rlib");
    let output = hermetic_command(&rustc, &cache, None)
        .args(["--crate-name=width_dep", "--crate-type=rlib"])
        .arg(project.join("dep.rs"))
        .arg("-o")
        .arg(&dependency)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let extern_flag = format!("width_dep={}", dependency.display());
    for cap in [vec!["--cap-lints=allow"], vec!["--cap-lints", "allow"]] {
        for forced in [
            vec!["--force-warn=unused_crate_dependencies"],
            vec!["--force-warn", "unused_crate_dependencies"],
            vec!["--force-warn=unused-crate-dependencies"],
            vec!["--force-warn", "unused-crate-dependencies"],
        ] {
            let mut flags = vec![
                "--error-format=json",
                "--json=artifacts,future-incompat,unused-externs-silent",
                "--extern",
                &extern_flag,
            ];
            flags.extend_from_slice(&cap);
            flags.extend_from_slice(&forced);
            let direct = compile(project, &cache, false, &flags);
            assert!(direct.status.success(), "{direct:?}");
            let record = String::from_utf8_lossy(&direct.stderr)
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .find(|record| record["$message_type"] == "unused_extern")
                .unwrap();
            assert!(record.get("rendered").is_none(), "{record}");
            for width in [
                vec![],
                vec!["--diagnostic-width=80"],
                vec!["--diagnostic-width", "120"],
            ] {
                let mut terminal = flags.clone();
                terminal.extend_from_slice(&width);
                let cached = compile(project, &cache, true, &terminal);
                assert!(cached.status.success(), "{terminal:?}: {cached:?}");
                assert_eq!(cached.stdout, direct.stdout, "{terminal:?}");
                assert_eq!(json_records(&cached), json_records(&direct), "{terminal:?}");
            }
            let recorded = events(&cache, "width_fixture");
            let runs = &recorded[recorded.len() - 3..];
            assert!(matches!(runs[0]["result"].as_str(), Some("miss" | "dup")));
            for hit in &runs[1..] {
                assert_eq!(hit["result"], "local_hit");
                assert_eq!(hit["cache_key"], runs[0]["cache_key"]);
            }
        }
    }

    let mut rendered = Vec::new();
    for width in ["--diagnostic-width=40", "--diagnostic-width=120"] {
        let flags = [
            "--cap-lints=allow",
            "--force-warn=unused_variables",
            "--error-format=json",
            "--json=unused-externs-silent",
            width,
        ];
        let direct = compile(project, &cache, false, &flags);
        assert!(direct.status.success(), "{direct:?}");
        assert!(String::from_utf8_lossy(&direct.stderr).contains("unused variable"));
        for _ in 0..2 {
            let cached = compile(project, &cache, true, &flags);
            assert!(cached.status.success(), "{cached:?}");
            assert_eq!(diagnostics(&cached, project), diagnostics(&direct, project));
        }
        rendered.push(direct.stderr);
    }
    assert_ne!(rendered[0], rendered[1]);
    let recorded = events(&cache, "width_fixture");
    let runs = &recorded[recorded.len() - 4..];
    assert_eq!(runs[1]["result"], "local_hit");
    assert_eq!(runs[3]["result"], "local_hit");
    assert_ne!(runs[0]["cache_key"], runs[2]["cache_key"]);
}

#[test]
fn nightly_future_incompat_reports_are_recompiled_with_the_requested_width() {
    build_kache();
    let fixture = tempfile::tempdir_in(common::scratch_dir()).unwrap();
    let project = fixture.path();
    let cache = project.join("cache");
    write_config(&cache);
    let _daemon = common::DaemonGuard::new(kache_binary(), &cache);
    fs::write(
        project.join("lib.rs"),
        "macro_rules! foo { () => { true; } }\npub fn bar() -> bool { foo!() }\n",
    )
    .unwrap();
    let mut rendered = Vec::new();
    for width in [
        None,
        Some("--diagnostic-width=20"),
        Some("--diagnostic-width=120"),
    ] {
        let mut flags = vec![
            "--cap-lints=allow",
            "--force-warn=unused_crate_dependencies",
            "--error-format=json",
            "--json=future-incompat,unused-externs-silent",
        ];
        flags.extend(width);
        let direct = compile(project, &cache, false, &flags);
        assert!(direct.status.success(), "{direct:?}");
        assert!(String::from_utf8_lossy(&direct.stderr).contains("future_incompat_report"));
        for _ in 0..2 {
            let cached = compile_with_log(project, &cache, true, &flags, "kache=warn");
            assert!(cached.status.success(), "{cached:?}");
            assert_eq!(diagnostics(&cached, project), diagnostics(&direct, project));
        }
        rendered.push(direct.stderr);
    }
    assert_ne!(rendered[1], rendered[2]);
    let recorded = events(&cache, "width_fixture");
    assert_eq!(recorded.len(), 6);
    for event in &recorded {
        assert_eq!(event["result"], "skipped");
        assert_eq!(
            event["skip_reason"],
            "compiler diagnostics require their original terminal width"
        );
        assert_eq!(event["cache_key"], recorded[0]["cache_key"]);
    }
}

#[test]
fn legacy_width_sensitive_metadata_is_a_policy_miss_without_restore_warnings() {
    build_kache();
    let fixture = tempfile::tempdir_in(common::scratch_dir()).unwrap();
    let project = fixture.path();
    let cache = project.join("cache");
    write_config(&cache);
    let _daemon = common::DaemonGuard::new(kache_binary(), &cache);
    fs::write(project.join("lib.rs"), "pub fn answer() -> u32 { 42 }\n").unwrap();
    let base = [
        "--cap-lints=allow",
        "--force-warn=unused_crate_dependencies",
        "--error-format=json",
        "--json=future-incompat,unused-externs-silent",
    ];
    let warm = compile(project, &cache, true, &base);
    assert!(warm.status.success(), "{warm:?}");
    let recorded = events(&cache, "width_fixture");
    assert_eq!(recorded.len(), 1);
    let key = recorded[0]["cache_key"].as_str().unwrap();
    let meta_path = cache.join("store").join(key).join("meta.json");
    let mut meta: Value = serde_json::from_slice(&fs::read(&meta_path).unwrap()).unwrap();
    meta.as_object_mut().unwrap().remove("key_schema");
    meta["stderr"] = Value::String(r#"{"$message_type":"future_incompat","future_incompat_report":[{"diagnostic":{"rendered":"obsolete wrapped warning"}}]}"#.to_string());
    fs::write(&meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();
    let mut terminal = base.to_vec();
    terminal.push("--diagnostic-width=120");
    let direct = compile(project, &cache, false, &terminal);
    assert!(direct.status.success(), "{direct:?}");
    let cached = compile_with_log(project, &cache, true, &terminal, "kache=warn");
    assert!(cached.status.success(), "{cached:?}");
    assert_eq!(cached.stdout, direct.stdout);
    assert_eq!(cached.stderr, direct.stderr);
    let recorded = events(&cache, "width_fixture");
    assert_eq!(recorded.len(), 2);
    assert_eq!(recorded[1]["result"], "skipped");
    assert!(
        !recorded[1]["skip_reason"]
            .as_str()
            .unwrap()
            .contains("restore")
    );
    assert!(
        recorded[1]["passthrough_reason"]
            .as_str()
            .unwrap_or("")
            .is_empty()
    );
}

#[cfg(unix)]
fn terminal_output(mut command: std::process::Command, columns: u16) -> Output {
    use std::io::Read;
    use std::os::fd::FromRawFd;
    use std::process::Stdio;

    let mut master = -1;
    let mut slave = -1;
    let mut size = libc::winsize {
        ws_row: 24,
        ws_col: columns,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: openpty initializes both descriptors on success; each is owned once.
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &raw mut size,
            )
        },
        0
    );
    let mut master = unsafe { fs::File::from_raw_fd(master) };
    let slave = unsafe { fs::File::from_raw_fd(slave) };
    for file in [&master, &slave] {
        use std::os::fd::AsRawFd;
        // SAFETY: the descriptor is owned and remains open for this call.
        assert_eq!(
            unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) },
            0
        );
    }
    command.stdout(Stdio::piped()).stderr(slave);
    let child = command.spawn().unwrap();
    // Command retains its Stdio handles after spawn; close the slave before reading EOF.
    drop(command);
    let reader = std::thread::spawn(move || {
        let mut stderr = Vec::new();
        if let Err(error) = master.read_to_end(&mut stderr) {
            // Linux returns EIO when the final slave descriptor closes.
            assert_eq!(error.raw_os_error(), Some(libc::EIO));
        }
        stderr
    });
    let mut output = child.wait_with_output().unwrap();
    output.stderr = reader.join().unwrap();
    output
}

#[cfg(unix)]
#[test]
fn cargo_dependency_hits_between_terminal_widths_and_pipes() {
    use std::os::unix::fs::PermissionsExt;

    build_kache();
    let fixture = tempfile::tempdir_in(common::scratch_dir()).unwrap();
    let project = fixture.path();
    let cache = project.join("cache");
    write_config(&cache);
    let _daemon = common::DaemonGuard::new(kache_binary(), &cache);
    for dir in ["src", ".cargo", "vendor/capped-dep/src"] {
        fs::create_dir_all(project.join(dir)).unwrap();
    }
    for (path, contents) in [
        (
            "Cargo.toml",
            "[package]\nname='width-consumer'\nversion='0.1.0'\nedition='2021'\n[workspace]\n[dependencies]\ncapped-dep='1.0.0'\n",
        ),
        (
            "src/lib.rs",
            "pub fn answer() -> u32 { capped_dep::answer() }\n",
        ),
        (
            ".cargo/config.toml",
            "[source.crates-io]\nreplace-with='vendored'\n[source.vendored]\ndirectory='vendor'\n",
        ),
        (
            "vendor/capped-dep/Cargo.toml",
            "[package]\nname='capped-dep'\nversion='1.0.0'\nedition='2021'\n",
        ),
        (
            "vendor/capped-dep/src/lib.rs",
            "fn unused() {}\npub fn answer() -> u32 { 42 }\n",
        ),
        (
            "vendor/capped-dep/.cargo-checksum.json",
            "{\"files\":{},\"package\":null}",
        ),
    ] {
        fs::write(project.join(path), contents).unwrap();
    }
    let wrapper = project.join("capture-wrapper.sh");
    fs::write(&wrapper, "#!/bin/sh\ncase \" $* \" in *\" --crate-name capped_dep \"*) printf '%s\\n' \"$@\" > \"$WIDTH_ARGV_LOG\";; esac\nexec \"$KACHE_UNDER_TEST\" \"$@\"\n").unwrap();
    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();
    let target = project.join("target");
    let log = project.join("rustc-argv.txt");

    for (index, columns) in [Some(80), None, Some(120), Some(80), None]
        .into_iter()
        .enumerate()
    {
        if target.exists() {
            fs::remove_dir_all(&target).unwrap();
        }
        let mut command = hermetic_command("cargo", &cache, Some(&isolated_config_path(&cache)));
        command
            .args(["build", "--offline", "--color", "never"])
            .env("RUSTC_WRAPPER", &wrapper)
            .env("KACHE_UNDER_TEST", kache_binary())
            .env("WIDTH_ARGV_LOG", &log)
            .env("CARGO_TARGET_DIR", &target)
            .env("CARGO_INCREMENTAL", "0")
            .env("CARGO_TERM_PROGRESS_WHEN", "never")
            .env("KACHE_ADAPTIVE_INCREMENTAL", "0")
            .env("KACHE_LOG", "off")
            .env_remove("RUSTFLAGS")
            .env_remove("CARGO_ENCODED_RUSTFLAGS")
            .current_dir(project);
        let output = match columns {
            Some(columns) => terminal_output(command, columns),
            None => command.output().unwrap(),
        };
        assert!(output.status.success(), "{output:?}");
        let argv = fs::read_to_string(&log).unwrap();
        assert!(argv.contains("--cap-lints\nallow\n"), "{argv}");
        match columns {
            Some(columns) => assert!(
                argv.contains(&format!("--diagnostic-width={columns}\n")),
                "{argv}"
            ),
            None => assert!(!argv.contains("--diagnostic-width"), "{argv}"),
        }
        let recorded = events(&cache, "capped_dep");
        assert_eq!(recorded.len(), index + 1);
        assert_eq!(
            recorded[index]["result"],
            if index == 0 { "miss" } else { "local_hit" },
            "{recorded:?}"
        );
        assert_eq!(recorded[0]["cache_key"], recorded[index]["cache_key"]);
    }
}
