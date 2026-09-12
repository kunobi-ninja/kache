//! The self-hosted CI Macs that run this suite carry a real
//! `/etc/kache/config.toml`. A kache a test spawns must see only the config
//! the test writes, so the shared harness turns the host layer off, the
//! repository's cargo `[env]` turns it off for every child that inherits the
//! test process's environment, and a test opts back in by naming a host file
//! on its own command.

#[allow(dead_code)]
mod common;

use common::{hermetic_command, kache_binary};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, MutexGuard};

/// The tests below read or change this process's `KACHE_HOST_CONFIG`.
static ENV_LOCK: Mutex<()> = Mutex::new(());

/// Holds [`ENV_LOCK`] and puts `KACHE_HOST_CONFIG` back when dropped, so a
/// failing assertion cannot leak a changed value into the next test.
struct HostConfigEnv {
    previous: Option<OsString>,
    _lock: MutexGuard<'static, ()>,
}

impl HostConfigEnv {
    fn lock() -> Self {
        let lock = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
        Self {
            previous: std::env::var_os("KACHE_HOST_CONFIG"),
            _lock: lock,
        }
    }

    fn set(&self, value: &Path) {
        // SAFETY: every test here holds ENV_LOCK while it touches the
        // variable, and nothing else in this binary reads it concurrently.
        unsafe { std::env::set_var("KACHE_HOST_CONFIG", value) };
    }
}

impl Drop for HostConfigEnv {
    fn drop(&mut self) {
        // SAFETY: as in `set`; ENV_LOCK is still held while this runs.
        unsafe {
            match self.previous.take() {
                Some(value) => std::env::set_var("KACHE_HOST_CONFIG", value),
                None => std::env::remove_var("KACHE_HOST_CONFIG"),
            }
        }
    }
}

fn write_host_file(dir: &Path) -> PathBuf {
    let path = dir.join("host.toml");
    std::fs::write(&path, "[cache]\ninput_predictions = true\n").unwrap();
    path
}

/// The detail line of `kache doctor`'s "Host config" check.
fn host_config_detail(mut command: Command) -> String {
    let output = command
        .args(["--json", "doctor"])
        .output()
        .expect("run kache doctor");
    // Doctor may report unrelated failing checks on a given machine; only its
    // JSON document matters here.
    let doc: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "kache --json doctor: {error}\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    // The machine-readable envelope flattens the body next to `command`.
    let checks = doc["checks"]
        .as_array()
        .unwrap_or_else(|| panic!("doctor lists no checks: {doc}"));
    checks
        .iter()
        .find(|check| check["label"] == "Host config")
        .unwrap_or_else(|| panic!("doctor has no Host config check: {doc}"))["detail"]
        .as_str()
        .expect("the check has a detail line")
        .to_string()
}

/// A machine-wide `KACHE_HOST_CONFIG` naming a real file stands in for the
/// default `/etc` path, which a test cannot write: a child the harness builds
/// must still see no host layer.
#[test]
fn a_harness_spawned_kache_ignores_the_host_file() {
    let env = HostConfigEnv::lock();
    let dir = tempfile::tempdir().unwrap();
    let cache = dir.path().join("cache");
    let host = write_host_file(dir.path());

    env.set(&host);
    let detail = host_config_detail(hermetic_command(
        kache_binary(),
        &cache,
        Some(&cache.join("config.toml")),
    ));

    assert_eq!(detail, "off (KACHE_HOST_CONFIG is empty)");
}

/// Most integration tests build their own `Command` rather than going through
/// the harness. The cargo `[env]` in `.cargo/config.toml` turns the layer off
/// for the test process, so those children inherit it off too.
#[test]
fn a_directly_spawned_kache_inherits_the_host_layer_off() {
    let _env = HostConfigEnv::lock();
    assert_eq!(
        std::env::var("KACHE_HOST_CONFIG").as_deref(),
        Ok(""),
        "run through cargo test or nextest, which apply .cargo/config.toml [env]"
    );
    let dir = tempfile::tempdir().unwrap();
    let cache = dir.path().join("cache");

    let mut command = Command::new(kache_binary());
    command
        .env("KACHE_CACHE_DIR", &cache)
        .env("KACHE_CONFIG", cache.join("config.toml"));
    assert_eq!(
        host_config_detail(command),
        "off (KACHE_HOST_CONFIG is empty)"
    );
}

#[test]
fn a_test_can_opt_into_a_host_file() {
    let _env = HostConfigEnv::lock();
    let dir = tempfile::tempdir().unwrap();
    let cache = dir.path().join("cache");
    let host = write_host_file(dir.path());

    let mut command = hermetic_command(kache_binary(), &cache, Some(&cache.join("config.toml")));
    command.env("KACHE_HOST_CONFIG", &host);
    let detail = host_config_detail(command);

    assert!(
        detail.starts_with(&host.display().to_string()),
        "the host file is in effect: {detail}"
    );
    assert!(detail.contains("cache.input_predictions"), "{detail}");
}
