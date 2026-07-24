#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;
use std::time::Instant;

fn kache_binary() -> &'static str {
    env!("CARGO_BIN_EXE_kache")
}

#[test]
fn probe_recovers_when_wrapper_fork_bombs() {
    let dir = tempfile::tempdir().unwrap();
    let wrapper = dir.path().join("my-compiler");

    fs::write(
        &wrapper,
        format!("#!/bin/sh\nexec {} \"$0\" \"$@\"\n", kache_binary()),
    )
    .unwrap();
    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();

    let start = Instant::now();
    let _ = Command::new(kache_binary())
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
    let wrapper = dir.path().join("my-compiler-hang");

    fs::write(
        &wrapper,
        "#!/bin/sh\n\
        head -c 9000 /dev/zero\n\
        sleep 60\n",
    )
    .unwrap();
    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();

    let start = Instant::now();
    let _ = Command::new(kache_binary())
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
    let wrapper = dir.path().join("my-compiler-descendant");
    let pid_file = dir.path().join("descendant.pid");

    fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\n\
            sleep 60 &\n\
            echo $! > \"{}\"\n\
            exit 0\n",
            pid_file.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();

    let start = Instant::now();
    let _ = Command::new(kache_binary())
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
        let still_alive = unsafe { libc::kill(pid, 0) == 0 };
        assert!(
            !still_alive,
            "descendant process {} should have been killed",
            pid
        );
    }
}
