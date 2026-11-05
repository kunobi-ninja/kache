//! Native clang-cl launchers must cache and delegate without a local engine.
#![cfg(any(unix, windows))]

#[allow(dead_code)]
mod common;

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn real_clang() -> Option<PathBuf> {
    let name = if cfg!(windows) {
        "clang-cl.exe"
    } else {
        "clang"
    };
    let mut directories = Vec::new();
    // /usr/bin/clang dispatches by argv[0]; its clang-cl symlink would ask
    // Xcode for a tool named clang-cl instead of starting the Clang driver.
    #[cfg(target_os = "macos")]
    if let Ok(output) = Command::new("/usr/bin/xcrun")
        .args(["--find", "clang"])
        .output()
        && output.status.success()
        && let Ok(path) = String::from_utf8(output.stdout)
        && let Some(parent) = Path::new(path.trim()).parent()
    {
        directories.push(parent.to_owned());
    }
    directories.extend(
        std::env::var_os("PATH")
            .map(|path| std::env::split_paths(&path).collect::<Vec<_>>())
            .unwrap_or_default(),
    );
    if cfg!(windows) {
        if let Some(program_files) = std::env::var_os("ProgramFiles") {
            directories.push(PathBuf::from(program_files).join("LLVM/bin"));
        }
    } else {
        directories.push(PathBuf::from("/usr/bin"));
    }
    directories.into_iter().find_map(|directory| {
        let candidate = directory.join(name);
        if !kache_shims::is_executable_file(&candidate) {
            return None;
        }
        let real = candidate.canonicalize().ok()?;
        (!kache_shims::farm::is_kache_binary(&real)).then_some(real)
    })
}

fn command(program: impl AsRef<OsStr>, root: &Path, path: &OsStr) -> Command {
    let mut command = Command::new(program);
    command.env_clear();
    for name in ["SYSTEMROOT", "WINDIR", "COMSPEC"] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    command
        .current_dir(root)
        .env("PATH", path)
        .env("HOME", root)
        .env("USERPROFILE", root)
        .env("TMPDIR", root)
        .env("TEMP", root)
        .env("TMP", root)
        .env("KACHE_CONFIG", root.join("config.toml"))
        .env("KACHE_HOST_CONFIG", "")
        .env("KACHE_CACHE_DIR", root.join("cache"))
        .env("KACHE_RUNTIME_DIR", root.parent().unwrap().join("run"));
    command
}

fn success(command: &mut Command) -> Output {
    let output = command.output().unwrap();
    assert!(output.status.success(), "{command:?}: {output:?}");
    output
}

struct FixtureDaemon<'a> {
    admin: &'a Path,
    root: &'a Path,
    path: &'a OsStr,
    runtime: &'a Path,
}

impl Drop for FixtureDaemon<'_> {
    fn drop(&mut self) {
        let mut stop = command(self.admin, self.root, self.path);
        common::stop_daemon(stop.args(["daemon", "stop"]), self.runtime);
    }
}

fn entries(root: &Path) -> Vec<(String, i64)> {
    let db = rusqlite::Connection::open_with_flags(
        root.join("cache/index.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    db.prepare("SELECT cache_key, hit_count FROM entries WHERE committed = 1 ORDER BY cache_key")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn age_entry(root: &Path, key: &str) {
    // Store::get throttles hit stamps; age the row instead of sleeping for it.
    let db = rusqlite::Connection::open(root.join("cache/index.db")).unwrap();
    db.busy_timeout(std::time::Duration::from_secs(5)).unwrap();
    assert_eq!(
        db.execute(
            "UPDATE entries SET last_accessed = datetime('now', '-1 hour') WHERE cache_key = ?1",
            [key],
        )
        .unwrap(),
        1
    );
}

#[test]
fn native_clang_cl_restores_objects_and_runs_through_a_warm_daemon() {
    let Some(compiler) = real_clang() else {
        assert!(std::env::var_os("CI").is_none(), "CI must provide Clang");
        eprintln!("skipping native clang-cl cache test: Clang unavailable");
        return;
    };
    let scratch = tempfile::tempdir().unwrap();
    let root = scratch.path().join("native cache with spaces 日本語");
    let install = root.join("install");
    let frontend_only = root.join("frontend-only");
    let toolchain = root.join("toolchain");
    for directory in [&install, &frontend_only, &toolchain] {
        std::fs::create_dir_all(directory).unwrap();
    }
    std::fs::write(
        root.join("config.toml"),
        "[cache]\ntrust_daemon_execution=true\nlocal_only=true\nnative_runtime_warmup=false\nauto_gc=false\nindex_auto_compact=false\nscheduler=false\ndaemon_publish=false\nprefetch_enabled=false\nmin_store_compile_ms=0\n",
    )
    .unwrap();
    let name = format!("kache-clang-cl{}", std::env::consts::EXE_SUFFIX);
    let launcher = install.join(&name);
    let daemon_launcher = frontend_only.join(&name);
    for target in [&launcher, &daemon_launcher] {
        std::fs::copy(env!("CARGO_BIN_EXE_kache-clang-cl"), target).unwrap();
    }
    std::fs::copy(
        env!("CARGO_BIN_EXE_kache-engine"),
        install.join(format!("kache-engine{}", std::env::consts::EXE_SUFFIX)),
    )
    .unwrap();
    std::fs::copy(
        env!("CARGO_BIN_EXE_kache"),
        install.join(format!("kache{}", std::env::consts::EXE_SUFFIX)),
    )
    .unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(&compiler, toolchain.join("clang-cl")).unwrap();
    let path = std::env::join_paths([toolchain.as_path(), compiler.parent().unwrap()]).unwrap();
    // Keep the socket below Unix pathname limits while installation paths
    // still exercise spaces and Unicode.
    let runtime = scratch.path().join("run");
    let admin = install.join(format!("kache{}", std::env::consts::EXE_SUFFIX));
    let daemon = FixtureDaemon {
        admin: &admin,
        root: &root,
        path: &path,
        runtime: &runtime,
    };
    let source = root.join("native-cache.c");
    let object = root.join("native-cache.obj");
    std::fs::write(&source, "int native_cache_answer(void) { return 42; }\n").unwrap();
    common::settle_writes(&[&root]);
    let compile = |program: &Path| {
        let mut command = command(program, &root, &path);
        command.args([
            "--driver-mode=cl",
            "/nologo",
            "/c",
            "native-cache.c",
            "/Fonative-cache.obj",
        ]);
        command
    };
    success(command(&admin, &root, &path).args(["daemon", "start"]));
    let status = success(command(&admin, &root, &path).args(["daemon", "status", "--json"]));
    let status: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(status["daemon_running"], true, "{status}");
    eprintln!("native clang-cl daemon status: {status}");
    success(&mut compile(&launcher));
    let expected = std::fs::read(&object).unwrap();
    assert!(!expected.is_empty());
    let cold = entries(&root);
    assert_eq!(cold.len(), 1, "{cold:?}");
    assert_eq!(cold[0].1, 0, "cold compilation must store a new entry");
    let key = &cold[0].0;
    for (program, hits) in [(&launcher, 1_i64), (&daemon_launcher, 2)] {
        std::fs::remove_file(&object).unwrap();
        age_entry(&root, key);
        if program == &daemon_launcher {
            assert!(kache_shims::engine_for(program).is_err());
        }
        success(&mut compile(program));
        assert_eq!(std::fs::read(&object).unwrap(), expected);
        assert_eq!(entries(&root), [(key.clone(), hits)]);
    }
    // A new miss must also execute through the frontend with no engine beside it.
    std::fs::write(&source, "int native_cache_answer(void) { return 43; }\n").unwrap();
    std::fs::remove_file(&object).unwrap();
    common::settle_writes(&[&source]);
    success(&mut compile(&daemon_launcher));
    assert_ne!(std::fs::read(&object).unwrap(), expected);
    assert_eq!(entries(&root).len(), 2);
    drop(daemon);
    common::wait_for_daemon_exit(&runtime, std::time::Duration::from_secs(5)).unwrap();
    let events = std::fs::read_to_string(runtime.join("events.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .filter(|event| event.get("result").is_some())
        .collect::<Vec<_>>();
    assert_eq!(
        events
            .iter()
            .map(|event| event["result"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["miss", "local_hit", "local_hit", "miss"],
        "{events:#?}"
    );
    assert_eq!(events[0]["cache_key"], *key);
    assert_ne!(events[3]["cache_key"], *key);
}
