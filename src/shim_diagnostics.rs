//! Bounded checks of the Rust tools behind installed shims, outside builds.

use crate::compiler::shim;
use crate::invocation::{InvocationEnv, process::CommandExt};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_OUTPUT: u64 = 65_536;

pub(crate) struct Diagnostic {
    pub(crate) label: &'static str,
    pub(crate) pass: bool,
    pub(crate) detail: String,
    pub(crate) fix: Option<String>,
}

/// Test the PATH init will install without modifying this process's environment.
#[cfg(any(test, unix))]
pub(crate) fn projected_env(env: &InvocationEnv, shims: &Path) -> anyhow::Result<InvocationEnv> {
    let path = env.var_os("PATH").unwrap_or_default();
    let path = std::env::join_paths(
        std::iter::once(shims.to_path_buf()).chain(std::env::split_paths(&path)),
    )?;
    let mut projected = env.clone();
    projected.set("PATH", path);
    Ok(projected)
}

fn shim_on_path(name: &str, env: &InvocationEnv, executable: Option<&Path>) -> bool {
    let path = env.var_os("PATH").unwrap_or_default();
    let real = executable.and_then(|path| path.canonicalize().ok());
    std::env::split_paths(&path)
        .filter_map(|dir| env.resolve(&dir))
        .any(|dir| {
            let program = dir.join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
            program.is_file()
                && (kache_shims::farm::has_marker(&dir)
                    || kache_shims::farm::resolves_to_kache(&program, real.as_deref(), &|p| {
                        p.canonicalize().ok()
                    }))
        })
}

pub(crate) fn inspect(env: &InvocationEnv, configured_rustdoc: bool) -> Vec<Diagnostic> {
    let executable = crate::platform::frontend_executable().ok();
    inspect_for(env, configured_rustdoc, executable.as_deref())
}

fn inspect_for(
    env: &InvocationEnv,
    configured_rustdoc: bool,
    executable: Option<&Path>,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    for (name, label) in [
        ("cargo", "Cargo shim chain"),
        ("rustdoc", "rustdoc shim chain"),
    ] {
        if !(configured_rustdoc && name == "rustdoc") && !shim_on_path(name, env, executable) {
            continue;
        }
        let result = match shim::real_tool_for(name, env, executable) {
            // Installing C/C++ shims does not require a Rust toolchain.
            Ok(None) => continue,
            Ok(Some(program)) => env
                .command(&program)
                .map_err(|error| error.to_string())
                .and_then(|mut command| {
                    command.arg("--version");
                    run_probe(&mut command, name, env.temp_dir(), PROBE_TIMEOUT)
                })
                .map(|()| program.clone())
                .map_err(|reason| format!("{}: {reason}", program.display())),
            Err(error) => Err(error),
        };
        let (pass, detail) = match result {
            Ok(program) => (true, format!("{} --version succeeded", program.display())),
            Err(error) => (false, error),
        };
        diagnostics.push(Diagnostic {
            label,
            pass,
            detail,
            fix: (!pass).then(|| {
                format!(
                    "Activate Rust in your version manager, or set {} to the real {name} executable (a rustup proxy is supported). Then rerun kache doctor.",
                    shim::real_tool_override(name).expect("Rust shim tool"),
                )
            }),
        });
    }
    diagnostics
}

#[cfg(unix)]
pub(crate) fn validate_init(shims: &Path) -> anyhow::Result<()> {
    let env = projected_env(&InvocationEnv::capture(), shims)?;
    let failures = inspect(&env, false)
        .into_iter()
        .filter(|check| !check.pass)
        .map(|check| {
            format!(
                "{}: {}. {}",
                check.label,
                check.detail,
                check.fix.unwrap_or_default()
            )
        })
        .collect::<Vec<_>>();
    anyhow::ensure!(
        failures.is_empty(),
        "Rust shim setup needs attention:\n{}",
        failures.join("\n")
    );
    Ok(())
}

fn run_probe(
    command: &mut Command,
    name: &str,
    temp: &Path,
    timeout: Duration,
) -> Result<(), String> {
    let mut output = tempfile::tempfile_in(temp).map_err(|error| error.to_string())?;
    command
        // A fallback into Kache stops at its first re-entry, before config or leases.
        .env(
            crate::KACHE_ACTIVE_ENV,
            crate::MAX_WRAPPERS_ABOVE.to_string(),
        )
        .env(
            crate::cargo_proxy::CARGO_SHIM_DEPTH_ENV,
            crate::cargo_proxy::MAX_CARGO_SHIMS_ABOVE.to_string(),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::from(
            output.try_clone().map_err(|error| error.to_string())?,
        ))
        .stderr(Stdio::from(
            output.try_clone().map_err(|error| error.to_string())?,
        ));
    let mut child = command
        .invocation_spawn_in_group()
        .map_err(|error| error.to_string())?;
    let deadline = Instant::now() + timeout;
    let status = loop {
        if output.metadata().map_err(|error| error.to_string())?.len() > MAX_OUTPUT {
            return Err("version probe exceeded 64 KiB of output".into());
        }
        if let Some(status) = child.try_wait_probe().map_err(|error| error.to_string())? {
            break status;
        }
        if Instant::now() >= deadline {
            return Err("version probe timed out".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    read_probe_output(&mut output, name, status.success())
}

fn read_probe_output(output: &mut std::fs::File, name: &str, success: bool) -> Result<(), String> {
    output
        .seek(SeekFrom::Start(0))
        .map_err(|error| error.to_string())?;
    let mut bytes = Vec::new();
    output
        .take(MAX_OUTPUT + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    classify(name, success, &bytes)
}

fn classify(name: &str, success: bool, bytes: &[u8]) -> Result<(), String> {
    if bytes.len() as u64 > MAX_OUTPUT {
        return Err("version probe exceeded 64 KiB of output".into());
    }
    let text = String::from_utf8_lossy(bytes);
    if text.contains("started kache's Cargo shim again")
        || (text.contains("kache wrappers") && text.contains("loop"))
    {
        return Err("the selected program forwards back to a Kache shim".into());
    }
    if !success {
        return Err("version probe exited unsuccessfully".into());
    }
    if !text.lines().any(|line| {
        line.strip_prefix(name)
            .is_some_and(|rest| rest.starts_with(' ') && !rest.trim().is_empty())
    }) {
        return Err(format!("version probe did not identify {name}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{File, OpenOptions};

    fn fixture(mode: &str) -> Command {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .args([
                "--exact",
                "shim_diagnostics::tests::tool_fixture",
                "--ignored",
                "--nocapture",
            ])
            .env("KACHE_SHIM_TEST_MODE", mode);
        command
    }

    // First-party subprocess fixture. It never reads or changes the parent's
    // environment; the probe supplies the counters to this child only.
    #[test]
    #[ignore = "subprocess fixture for bounded shim diagnostics"]
    fn tool_fixture() {
        assert_eq!(std::env::var("KACHE_ACTIVE").unwrap(), "8");
        assert_eq!(std::env::var("KACHE_CARGO_SHIM_DEPTH").unwrap(), "8");
        let mode = std::env::var("KACHE_SHIM_TEST_MODE").unwrap();
        match mode.as_str() {
            "ok" => println!("cargo 1.99.0\nrustdoc 1.99.0"),
            "proxy" => {
                assert!(fixture("ok").status().unwrap().success());
            }
            "loop" => {
                let error =
                    crate::check_wrapper_depth(8, Some("version-manager/rustdoc")).unwrap_err();
                eprintln!("{error}");
                std::process::exit(2);
            }
            "oversized" => {
                use std::io::Write;
                std::io::stdout().write_all(&[b'x'; 65_537]).unwrap();
                std::thread::sleep(Duration::from_secs(30));
            }
            "hang" => std::thread::sleep(Duration::from_secs(30)),
            "lock-child" => {
                let lock = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(std::env::var_os("KACHE_SHIM_TEST_LOCK").unwrap())
                    .unwrap();
                lock.lock().unwrap();
                std::fs::write(std::env::var_os("KACHE_SHIM_TEST_READY").unwrap(), "ready")
                    .unwrap();
                std::thread::sleep(Duration::from_secs(30));
            }
            "leave-child" | "hang-with-child" => {
                #[expect(
                    clippy::zombie_processes,
                    reason = "The fixture leaves its child for the probe's process-tree cleanup after the leader exits."
                )]
                let mut child = fixture("lock-child").spawn().unwrap();
                let ready = std::env::var_os("KACHE_SHIM_TEST_READY").unwrap();
                let deadline = Instant::now() + Duration::from_secs(10);
                while !Path::new(&ready).exists() {
                    assert!(Instant::now() < deadline, "descendant did not start");
                    std::thread::sleep(Duration::from_millis(5));
                }
                if mode == "hang-with-child" {
                    let _ = child.wait();
                } else {
                    // The probe must stop this child even after its leader exits.
                    println!("cargo 1.99.0");
                    std::process::exit(0);
                }
            }
            _ => panic!("unexpected fixture mode"),
        }
    }

    #[test]
    fn classification_requires_the_requested_tool_and_success() {
        assert!(classify("cargo", true, b"cargo 1.99.0\n").is_ok());
        assert!(classify("rustdoc", true, b"proxy notice\nrustdoc 1.99.0\n").is_ok());
        for bytes in [
            b"cargo\n".as_slice(),
            b"cargo \n",
            b"cargo-wrapper 1\n",
            b"rustdoc 1\n",
            b"\xff",
        ] {
            assert_eq!(
                classify("cargo", true, bytes).unwrap_err(),
                "version probe did not identify cargo"
            );
        }
        assert_eq!(
            classify("cargo", false, b"cargo 1\n").unwrap_err(),
            "version probe exited unsuccessfully"
        );
        assert!(
            classify("cargo", false, b"started kache's Cargo shim again")
                .unwrap_err()
                .contains("forwards back")
        );
        assert!(
            classify("rustdoc", false, b"kache wrappers form a loop")
                .unwrap_err()
                .contains("forwards back")
        );
        assert!(
            !classify("cargo", false, b"kache wrappers")
                .unwrap_err()
                .contains("forwards back")
        );
        assert!(
            !classify("cargo", false, b"loop")
                .unwrap_err()
                .contains("forwards back")
        );
        let mut boundary = vec![b'x'; 65_536];
        boundary[..8].copy_from_slice(b"cargo 1\n");
        assert!(classify("cargo", true, &boundary).is_ok());
        boundary.push(b'x');
        assert!(
            classify("cargo", true, &boundary)
                .unwrap_err()
                .contains("64 KiB")
        );
    }

    #[test]
    fn bounded_capture_keeps_a_version_at_the_last_allowed_byte() {
        use std::io::Write;
        let mut output = tempfile::tempfile().unwrap();
        output.write_all(&[b'x'; 65_528]).unwrap();
        output.write_all(b"\ncargo 1").unwrap();
        assert!(read_probe_output(&mut output, "cargo", true).is_ok());
        output.seek(SeekFrom::End(0)).unwrap();
        output.write_all(b"x").unwrap();
        assert!(
            read_probe_output(&mut output, "cargo", true)
                .unwrap_err()
                .contains("64 KiB")
        );
    }

    #[test]
    fn probes_accept_real_or_forwarding_tools_and_stop_bad_chains() {
        let root = tempfile::tempdir().unwrap();
        for (mode, name) in [("ok", "cargo"), ("ok", "rustdoc"), ("proxy", "cargo")] {
            run_probe(
                &mut fixture(mode),
                name,
                root.path(),
                Duration::from_secs(10),
            )
            .unwrap();
        }
        assert!(
            run_probe(
                &mut fixture("loop"),
                "rustdoc",
                root.path(),
                Duration::from_secs(10)
            )
            .unwrap_err()
            .contains("forwards back")
        );
        assert!(
            run_probe(
                &mut fixture("hang"),
                "cargo",
                root.path(),
                Duration::from_millis(100)
            )
            .unwrap_err()
            .contains("timed out")
        );
        assert!(
            run_probe(
                &mut fixture("oversized"),
                "cargo",
                root.path(),
                Duration::from_secs(10)
            )
            .unwrap_err()
            .contains("64 KiB")
        );
    }

    #[test]
    fn probe_wait_requires_process_tree_ownership() {
        let mut command = fixture("ok");
        command
            .env(crate::KACHE_ACTIVE_ENV, "8")
            .env(crate::cargo_proxy::CARGO_SHIM_DEPTH_ENV, "8")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut child = command.invocation_spawn().unwrap();
        assert!(
            child
                .try_wait_probe()
                .unwrap_err()
                .to_string()
                .contains("owned process group or job")
        );
        assert!(child.wait().unwrap().success());
    }

    #[test]
    fn probes_release_descendants_on_success_and_timeout() {
        for mode in ["leave-child", "hang-with-child"] {
            let root = tempfile::tempdir().unwrap();
            let lock_path = root.path().join("child.lock");
            let ready = root.path().join("child.ready");
            File::create(&lock_path).unwrap();
            let mut command = fixture(mode);
            command
                .env("KACHE_SHIM_TEST_LOCK", &lock_path)
                .env("KACHE_SHIM_TEST_READY", &ready);
            let result = run_probe(&mut command, "cargo", root.path(), Duration::from_secs(3));
            assert!(ready.exists(), "descendant never acquired the lock");
            if mode == "leave-child" {
                result.unwrap();
            } else {
                assert!(result.unwrap_err().contains("timed out"));
            }
            // An independent file description can acquire only once the child
            // has stopped. Bound scheduling slack after the kill request.
            let lock = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&lock_path)
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(3);
            while lock.try_lock().is_err() {
                assert!(Instant::now() < deadline, "probe left its descendant alive");
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    }

    #[test]
    fn projected_path_preserves_snapshot_and_finds_a_farm_behind_a_manager() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path().canonicalize().unwrap();
        let shims = root.join("shims");
        std::fs::create_dir(&shims).unwrap();
        kache_shims::farm::write_marker(&shims).unwrap();
        std::fs::write(
            shims.join(format!("cargo{}", std::env::consts::EXE_SUFFIX)),
            "shim",
        )
        .unwrap();
        let manager = root.join("manager");
        let path = std::env::join_paths([manager.clone(), shims.clone()]).unwrap();
        let mut env = InvocationEnv::from_parts(
            [("PATH", path.clone()), ("KEEP", "yes".into())],
            Some(root.clone()),
            Some(root.clone()),
            root.clone(),
        );
        if cfg!(not(windows)) {
            env.set("path", "case-sensitive-value");
        }
        assert!(shim_on_path("cargo", &env, None));
        assert!(!shim_on_path("rustdoc", &env, None));
        // With no Rust tool behind the farm, C/C++ setup remains valid.
        assert!(inspect_for(&env, false, None).is_empty());
        let broken = InvocationEnv::from_parts(
            [
                ("PATH", path.clone()),
                (
                    "KACHE_REAL_CARGO",
                    root.join("absent-cargo").into_os_string(),
                ),
                (
                    "KACHE_REAL_RUSTDOC",
                    root.join("absent-rustdoc").into_os_string(),
                ),
            ],
            Some(root.clone()),
            None,
            root.clone(),
        );
        let checks = inspect_for(&broken, true, None);
        assert_eq!(checks.len(), 2);
        assert!(
            checks
                .iter()
                .all(|check| !check.pass && check.detail.contains("cannot resolve"))
        );
        assert!(checks[0].fix.as_ref().unwrap().contains("KACHE_REAL_CARGO"));
        assert!(
            checks[1]
                .fix
                .as_ref()
                .unwrap()
                .contains("KACHE_REAL_RUSTDOC")
        );
        let projected = projected_env(&env, &root.join("custom shims")).unwrap();
        assert_eq!(env.var_os("PATH").unwrap(), path);
        assert_eq!(projected.var("KEEP").unwrap(), "yes");
        #[cfg(not(windows))]
        assert_eq!(projected.var("path").unwrap(), "case-sensitive-value");
        #[cfg(windows)]
        assert_eq!(projected.var_os("path"), projected.var_os("PATH"));
        assert_eq!(
            std::env::split_paths(&projected.var_os("PATH").unwrap()).collect::<Vec<_>>(),
            [root.join("custom shims"), manager, shims]
        );
        assert_eq!(projected.cwd(), env.cwd());
        assert_eq!(projected.home_dir(), env.home_dir());
        assert_eq!(projected.temp_dir(), env.temp_dir());
    }
}
