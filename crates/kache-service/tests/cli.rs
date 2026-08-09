use std::{
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

#[test]
fn version_flag_runs_the_service_entrypoint() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_kache-service"))
        .arg("--version")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("run kache-service --version");
    let deadline = Instant::now() + Duration::from_secs(30);

    loop {
        if child.try_wait().expect("poll kache-service").is_some() {
            break;
        }
        if Instant::now() >= deadline {
            child.kill().expect("kill unresponsive kache-service");
            child.wait().expect("reap unresponsive kache-service");
            panic!("kache-service --version did not exit within 30 seconds");
        }
        thread::sleep(Duration::from_millis(10));
    }

    let output = child
        .wait_with_output()
        .expect("collect kache-service output");

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!("kache-service {}\n", kache_service::VERSION)
    );
}
