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
            "--color=never",
            "--out-dir",
        ])
        .arg(out)
        .arg(project.join("lib.rs"))
        .args(flags)
        .env("KACHE_LOG", "off")
        .current_dir(project)
        .output()
        .unwrap()
}

fn diagnostics(output: &Output, project: &Path) -> String {
    // Compare rendering apart from Kache's existing stable workspace paths.
    String::from_utf8(output.stderr.clone())
        .unwrap()
        .replace(project.to_str().unwrap(), "<PROJECT>")
        .replace("/proc/self/cwd", "<PROJECT>")
        .replace("/kache/workspace", "<PROJECT>")
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
