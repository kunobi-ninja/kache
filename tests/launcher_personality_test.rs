//! Link and run the launcher against the selected toolchain's real core.

#![cfg(unix)]

use std::ffi::OsString;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::process::Command;

#[path = "../launcher/compile.rs"]
mod launcher;

#[test]
fn the_launcher_links_with_current_core_and_runs_the_preserved_script() {
    let fixture = tempfile::tempdir().unwrap();
    let out = fixture.path().join("launcher");
    build_launcher(&out);

    // A successful link is the symbol oracle. Executing the fallback also
    // proves that the generated launcher remains usable, rather than merely
    // carrying a chosen string somewhere in its image.
    let preserved = fixture.path().join("preserved.kache-real");
    std::fs::write(&preserved, "#!/bin/sh\nexit 23\n").unwrap();
    std::fs::set_permissions(&preserved, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(
        fixture.path().join(".kache-launch"),
        b"preserved.kache-real\0\0./\0",
    )
    .unwrap();
    let output = Command::new(&out).env_clear().output().unwrap();
    assert_eq!(output.status.code(), Some(23), "{output:?}");
}

fn build_launcher(out: &Path) {
    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let version = Command::new(&rustc).arg("-vV").output().unwrap();
    assert!(version.status.success(), "{version:?}");
    let version = String::from_utf8(version.stdout).unwrap();
    let host = version
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .unwrap();
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("launcher/build_script.rs");
    let mut flags: Vec<OsString> = Vec::new();
    if let Some(linker) = std::env::var_os("RUSTC_LINKER") {
        let mut flag = OsString::from("linker=");
        flag.push(linker);
        flags.extend(["-C".into(), flag]);
    }
    if cfg!(all(target_os = "linux", target_env = "gnu")) {
        flags.extend(["-C".into(), "link-arg=-lc".into()]);
    }
    let output = launcher::build(&rustc, host, &source, out, &flags).unwrap();
    assert!(output.status.success(), "{output:?}");
}

#[test]
fn doctor_repairs_an_owned_client_pin_without_its_engine_and_preserves_the_script() {
    let fixture = tempfile::tempdir().unwrap();
    let target = fixture.path().join("target");
    let profile = target.join("debug");
    let unit = "fixture-0123456789abcdef";
    let directory = profile.join("build").join(unit);
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::create_dir_all(profile.join(".fingerprint").join(unit)).unwrap();
    std::fs::write(
        target.join("CACHEDIR.TAG"),
        "Signature: 8a477f597d28d172789f06886806bc55\n",
    )
    .unwrap();
    let launcher = directory.join("build_script_build");
    build_launcher(&launcher);
    let preserved = directory.join("build_script_build.kache-real");
    let script = b"#!/bin/sh\nprintf 'preserved:%s\\n' \"$1\"\nexit 23\n";
    std::fs::write(&preserved, script).unwrap();
    std::fs::set_permissions(&preserved, std::fs::Permissions::from_mode(0o700)).unwrap();
    let real_id = std::fs::metadata(&preserved).unwrap().ino();
    std::fs::write(
        directory.join("build_script_build.kache-real.action"),
        blake3::hash(script).to_hex().as_bytes(),
    )
    .unwrap();
    let old_relative = ".kache-build-script-shims/11111111111111111111111111111111/kache";
    let old_pin = profile.join(old_relative);
    std::fs::create_dir_all(old_pin.parent().unwrap()).unwrap();
    std::fs::copy(env!("CARGO_BIN_EXE_kache"), &old_pin).unwrap();
    let old_id = std::fs::metadata(&old_pin).unwrap().ino();
    let old_hash = kache_store::file_hash::hash_file(&old_pin).unwrap();
    assert!(!old_pin.with_file_name("kache-engine").exists());
    let record = directory.join(".kache-launch");
    std::fs::write(
        &record,
        [
            b"build_script_build.kache-real\0".as_slice(),
            old_relative.as_bytes(),
            b"\0",
        ]
        .concat(),
    )
    .unwrap();
    let cache = fixture.path().join("cache");
    let command = |program: &Path| {
        let mut command = Command::new(program);
        command
            .env_clear()
            .env("HOME", fixture.path())
            .env("KACHE_CACHE_DIR", &cache)
            .env("KACHE_RUNTIME_DIR", &cache)
            .env("KACHE_CONFIG", cache.join("config.toml"))
            .env("KACHE_HOST_CONFIG", "")
            .env("KACHE_BUILD_SCRIPT_CACHE", "0")
            .env("KACHE_LOG", "off");
        let mut command = assert_cmd::Command::from(command);
        command.timeout(std::time::Duration::from_secs(30));
        command
    };
    command(&launcher)
        .arg("argument")
        .assert()
        .failure()
        .stderr(predicates::str::contains(
            "locating kache-engine; reinstall both Kache binaries together",
        ));
    let before = std::fs::read(&record).unwrap();
    command(Path::new(env!("CARGO_BIN_EXE_kache")))
        .args(["doctor", "--json", "--repair-build-scripts"])
        .arg(&target)
        .assert()
        .failure()
        .stderr(predicates::str::contains("reports diagnostics only"));
    assert_eq!(std::fs::read(&record).unwrap(), before);
    command(Path::new(env!("CARGO_BIN_EXE_kache")))
        .args(["doctor", "--repair-build-scripts"])
        .arg(&target)
        .assert()
        .success()
        .stdout(predicates::str::contains(
            "Refreshed 1 build-script launchers",
        ));
    let repaired = std::fs::read(&record).unwrap();
    let pin = repaired.split(|&byte| byte == 0).nth(1).unwrap();
    let pin = profile.join(std::ffi::OsStr::from_bytes(pin));
    assert_ne!(pin, old_pin);
    assert_eq!(
        kache_store::file_hash::hash_file(&pin).unwrap(),
        kache_store::file_hash::hash_file(Path::new(env!("CARGO_BIN_EXE_kache-engine"))).unwrap()
    );
    assert!(!pin.with_file_name("kache-engine").exists());
    command(&launcher)
        .arg("argument")
        .assert()
        .code(23)
        .stdout("preserved:argument\n");
    assert_eq!(std::fs::read(&preserved).unwrap(), script);
    assert_eq!(std::fs::metadata(&preserved).unwrap().ino(), real_id);
    assert_eq!(std::fs::metadata(&old_pin).unwrap().ino(), old_id);
    assert_eq!(
        kache_store::file_hash::hash_file(&old_pin).unwrap(),
        old_hash
    );
    command(Path::new(env!("CARGO_BIN_EXE_kache")))
        .args(["doctor", "--repair-build-scripts"])
        .arg(&target)
        .assert()
        .success()
        .stdout(predicates::str::contains(
            "Refreshed 0 build-script launchers",
        ));
    assert_eq!(std::fs::read(&record).unwrap(), repaired);
}
