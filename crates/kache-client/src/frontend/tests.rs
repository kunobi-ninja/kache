use super::*;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::os::unix::net::UnixListener;
use std::os::unix::process::CommandExt;
use std::process::{Child, Stdio};
use std::time::{Duration, Instant};

const FIXTURE: &str = "frontend::boundary_tests::frontend_process_fixture";

struct ChildGuard {
    child: Child,
    finished: bool,
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[test]
#[ignore = "subprocess fixture for the public frontend boundary"]
fn frontend_process_fixture() {
    std::process::exit(run("fixture-version").unwrap());
}

fn run_case(arg0: OsString, flag: Option<&str>, expected_connections: usize) {
    let root = tempfile::tempdir_in("/tmp").unwrap();
    let directory = root.path().canonicalize().unwrap();
    let frontend = directory.join("kache");
    std::fs::copy(std::env::current_exe().unwrap(), &frontend).unwrap();
    let engine = directory.join("kache-engine");
    std::fs::write(
        &engine,
        "#!/bin/sh\nprintf '%s\\0' \"$@\" > \"$FORWARDED_FILE\"\nprintf 'engine-stdout-\\377'\nprintf 'engine-stderr-\\376' >&2\nexit 29\n",
    )
    .unwrap();
    std::fs::set_permissions(&engine, std::fs::Permissions::from_mode(0o755)).unwrap();
    let config = directory.join("config.toml");
    std::fs::write(&config, "").unwrap();
    let socket = directory.join("control");
    let listener = UnixListener::bind(&socket).unwrap();
    listener.set_nonblocking(true).unwrap();
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600)).unwrap();
    let forwarded = directory.join("forwarded");
    let stdout = directory.join("stdout");
    let stderr = directory.join("stderr");
    let args = ["--exact", FIXTURE, "--ignored", "--nocapture"];
    let original_args = std::iter::once(arg0.clone())
        .chain(args.map(OsString::from))
        .collect::<Vec<_>>();
    let compiler = arg0 == "cc";
    assert_eq!(engine_only(&original_args), !compiler);
    let mut command = Command::new(&frontend);
    command
        .arg0(&arg0)
        .args(args)
        .env_clear()
        .env("PATH", env!("PATH"))
        .env("HOME", &directory)
        .env("TMPDIR", &directory)
        .env("KACHE_CONFIG", &config)
        .env("KACHE_HOST_CONFIG", "")
        .env("KACHE_CACHE_DIR", directory.join("cache"))
        .env("KACHE_RUNTIME_DIR", directory.join("runtime"))
        .env("KACHE_SOCKET_PATH", &socket)
        .env("FORWARDED_FILE", &forwarded)
        .current_dir(&directory)
        .stdin(Stdio::null())
        .stdout(std::fs::File::create(&stdout).unwrap())
        .stderr(std::fs::File::create(&stderr).unwrap());
    if let Some(flag) = flag {
        command.env(flag, "1");
    }
    let mut child = ChildGuard {
        child: command.spawn().unwrap(),
        finished: false,
    };
    let mut connections = 0;
    let deadline = Instant::now() + Duration::from_secs(15);
    let status = loop {
        loop {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    connections += 1;
                    stream
                        .set_write_timeout(Some(Duration::from_secs(1)))
                        .unwrap();
                    let mut decline =
                        serde_json::to_vec(&kache_protocol::execution::ExecutionReply::Declined {
                            reason: "frontend boundary fixture".to_owned(),
                        })
                        .unwrap();
                    decline.push(b'\n');
                    let _ = stream.write_all(&decline);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => panic!("frontend listener failed: {error}"),
            }
        }
        if let Some(status) = child.child.try_wait().unwrap() {
            child.finished = true;
            break status;
        }
        assert!(Instant::now() < deadline, "frontend did not finish");
        std::thread::sleep(Duration::from_millis(2));
    };
    let output = std::fs::read(stdout).unwrap();
    let error = std::fs::read(stderr).unwrap();
    assert_eq!(status.code(), Some(29), "{output:?} {error:?}");
    assert_eq!(connections, expected_connections);
    assert!(output.ends_with(b"engine-stdout-\xff"), "{output:?}");
    assert_eq!(error, b"engine-stderr-\xfe");
    let bytes = std::fs::read(forwarded).unwrap();
    let fields = bytes
        .strip_suffix(b"\0")
        .unwrap()
        .split(|byte| *byte == 0)
        .collect::<Vec<_>>();
    assert_eq!(fields.len(), 5 + original_args.len());
    assert_eq!(fields[0], crate::FORWARDED_ARGV_FLAG.as_bytes());
    assert_eq!(fields[1], b"fixture-version");
    std::str::from_utf8(fields[2])
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert_eq!(fields[3], frontend.as_os_str().as_bytes());
    let metadata = std::fs::metadata(&frontend).unwrap();
    assert_eq!(
        fields[4],
        format!("{}:{}", metadata.dev(), metadata.ino()).as_bytes()
    );
    for (actual, expected) in fields[5..].iter().zip(original_args) {
        assert_eq!(*actual, expected.as_bytes());
    }
}

#[test]
fn cli_forwarding_preserves_native_argv_streams_version_and_image_identity() {
    run_case("client-ñ\nname".into(), None, 0);
}

#[test]
fn compiler_requests_try_the_daemon_before_forwarding_a_declined_request() {
    run_case("cc".into(), None, 1);
}

#[test]
fn family_probe_forwarding_never_contacts_the_daemon() {
    run_case("cc".into(), Some("KACHE_FAMILY_PROBE_ACTIVE"), 0);
}

#[test]
fn build_script_forwarding_never_contacts_the_daemon() {
    run_case("cc".into(), Some("KACHE_BUILD_SCRIPT_PATH"), 0);
}
