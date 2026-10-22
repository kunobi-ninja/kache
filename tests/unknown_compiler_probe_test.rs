#![cfg(unix)]

use std::fs;
#[allow(dead_code)]
mod common;
use std::time::{Duration, Instant};

fn kache_binary() -> &'static str {
    env!("CARGO_BIN_EXE_kache")
}

fn process_is_running(pid: i32) -> bool {
    #[cfg(target_os = "linux")]
    if fs::read_to_string(format!("/proc/{pid}/status")).is_ok_and(|status| {
        status
            .lines()
            .any(|line| line.starts_with("State:") && line.split_whitespace().nth(1) == Some("Z"))
    }) {
        return false;
    }
    unsafe { libc::kill(pid, 0) == 0 }
}

#[test]
fn probe_recovers_when_wrapper_fork_bombs() {
    let dir = tempfile::tempdir().unwrap();
    let _daemon = common::DaemonGuard::new(kache_binary(), dir.path().join("cache"));
    let wrapper = dir.path().join("my-compiler");

    kache_fs::testutil::write_executable(
        &wrapper,
        format!("#!/bin/sh\nexec {} \"$0\" \"$@\"\n", kache_binary()),
    );

    let start = Instant::now();
    let _ = common::hermetic_command(
        kache_binary(),
        &dir.path().join("cache"),
        Some(&dir.path().join("missing-config.toml")),
    )
    .arg(&wrapper)
    .arg("-c")
    .arg("foo.c")
    .env("KACHE_CACHE_DIR", dir.path().join("cache"))
    .output()
    .expect("failed to run kache");

    assert!(
        start.elapsed().as_secs() < 10,
        "probe should not hang on fork bomb"
    );
}

#[test]
fn probe_recovers_when_wrapper_emits_8kb_then_hangs() {
    let dir = tempfile::tempdir().unwrap();
    let _daemon = common::DaemonGuard::new(kache_binary(), dir.path().join("cache"));
    let wrapper = dir.path().join("my-compiler-hang");

    kache_fs::testutil::write_executable(
        &wrapper,
        "#!/bin/sh\n\
        head -c 9000 /dev/zero\n\
        sleep 60\n",
    );

    let start = Instant::now();
    let _ = common::hermetic_command(
        kache_binary(),
        &dir.path().join("cache"),
        Some(&dir.path().join("missing-config.toml")),
    )
    .arg(&wrapper)
    .arg("-c")
    .arg("foo.c")
    .env("KACHE_CACHE_DIR", dir.path().join("cache"))
    .output()
    .expect("failed to run kache");

    assert!(
        start.elapsed().as_secs() < 15,
        "probe must kill hanging wrapper after reading 8KB"
    );
}

#[test]
fn probe_recovers_when_wrapper_leaves_descendant_on_stdout() {
    let dir = tempfile::tempdir().unwrap();
    let _daemon = common::DaemonGuard::new(kache_binary(), dir.path().join("cache"));
    let wrapper = dir.path().join("my-compiler-descendant");
    let pid_file = dir.path().join("descendant.pid");

    kache_fs::testutil::write_executable(
        &wrapper,
        format!(
            "#!/bin/sh\n\
            sleep 60 &\n\
            echo $! > \"{}\"\n\
            exit 0\n",
            pid_file.display()
        ),
    );

    let start = Instant::now();
    let _ = common::hermetic_command(
        kache_binary(),
        &dir.path().join("cache"),
        Some(&dir.path().join("missing-config.toml")),
    )
    .arg(&wrapper)
    .arg("-c")
    .arg("foo.c")
    .env("KACHE_CACHE_DIR", dir.path().join("cache"))
    .output()
    .expect("failed to run kache");

    assert!(
        start.elapsed().as_secs() < 15,
        "probe must kill descendants holding stdout"
    );

    if let Ok(pid_str) = fs::read_to_string(&pid_file)
        && let Ok(pid) = pid_str.trim().parse::<i32>()
    {
        // A container's init may retain a killed descendant as a zombie.
        // It cannot run or hold stdout open, so only a live process fails.
        let deadline = Instant::now() + Duration::from_secs(2);
        while process_is_running(pid) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let still_alive = process_is_running(pid);
        assert!(
            !still_alive,
            "descendant process {} should have been killed",
            pid
        );
    }
}
