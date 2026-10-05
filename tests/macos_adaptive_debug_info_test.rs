//! A test binary that Kache relinks on its adaptive incremental lanes keeps a debug map that backtraces can read.

#![cfg(target_os = "macos")]

use filetime::FileTime;
use serde_json::Value;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

#[allow(dead_code)]
mod common;
use common::{hermetic_command, kache_binary, stop_daemon};

/// A package whose integration test, `it`, fails with a backtrace through a library function.
/// It builds through Kache with its own target directory and cache.
struct Fixture {
    kache: PathBuf,
    package: PathBuf,
    /// Canonical, as in a checkout.
    /// macOS reaches the temporary directory through the `/var` symlink, and rustc gives the linker canonical archive paths,
    /// so a non-canonical target directory would leave the library's records outside the injected prefix.
    target: PathBuf,
    cache: TempDir,
    /// Future, increasing source mtimes make Cargo rebuild every variant without sleeping.
    first_mtime: i64,
    _dirs: [TempDir; 2],
}

/// One build's event for `it`, and the test binary it produced.
struct Build {
    event: Value,
    executable: PathBuf,
}

/// What `RUST_BACKTRACE=1` printed for one test binary.
struct Backtrace {
    /// `boom`'s frame is followed by a `src/lib.rs` location.
    library: bool,
    /// The test function's frame is followed by a `tests/it.rs` location.
    test: bool,
    output: String,
}

impl Fixture {
    fn new() -> Self {
        let package_dir = TempDir::new().unwrap();
        let target_dir = TempDir::new().unwrap();
        let package = package_dir.path().canonicalize().unwrap();
        let target = target_dir.path().canonicalize().unwrap();
        fs::create_dir(package.join("src")).unwrap();
        fs::create_dir(package.join("tests")).unwrap();
        fs::write(
            package.join("Cargo.toml"),
            "[package]\nname = \"adaptive_debug_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\n[workspace]\n",
        )
        .unwrap();
        fs::write(
            package.join("tests/it.rs"),
            "#[test]\nfn fails_with_a_backtrace() {\n    adaptive_debug_fixture::boom(0);\n}\n",
        )
        .unwrap();
        Fixture {
            kache: kache_binary(),
            package,
            target,
            cache: TempDir::new().unwrap(),
            first_mtime: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
                + 3_600,
            _dirs: [package_dir, target_dir],
        }
    }

    fn config(&self) -> PathBuf {
        self.package.join("missing-kache.toml")
    }

    /// Builds the variant whose library constant is `salt`.
    /// Returns the one new event Kache recorded for `it`, and the test binary from Cargo's messages.
    fn build(&self, salt: u32) -> Build {
        let source = self.package.join("src/lib.rs");
        fs::write(
            &source,
            format!(
                "pub const SALT: u32 = {salt};\n\npub fn boom(n: u32) -> u32 {{\n    if n == 0 {{\n        panic!(\"boom {{}}\", SALT);\n    }}\n    n\n}}\n"
            ),
        )
        .unwrap();
        filetime::set_file_mtime(
            &source,
            FileTime::from_unix_time(self.first_mtime + i64::from(salt), 0),
        )
        .unwrap();

        let before = self.events().len();
        let output = hermetic_command(env!("CARGO"), self.cache.path(), Some(&self.config()))
            .args(["test", "--offline", "--no-run", "--message-format=json"])
            .current_dir(&self.package)
            .env("RUSTC_WRAPPER", &self.kache)
            .env("CARGO_TARGET_DIR", &self.target)
            .env("CARGO_INCREMENTAL", "1")
            .env("KACHE_ADAPTIVE_INCREMENTAL", "1")
            .env("KACHE_LOG", "kache=debug")
            .env_remove("RUSTC_WORKSPACE_WRAPPER")
            .env_remove("KACHE_CLEAN_INCREMENTAL")
            .env_remove("KACHE_DISABLED")
            .env_remove("KACHE_PRESERVE_INCREMENTAL")
            .env_remove("KACHE_FALLBACK")
            .env_remove("KACHE_CACHE_EXECUTABLES")
            .env_remove("RUSTFLAGS")
            .env_remove("CARGO_ENCODED_RUSTFLAGS")
            .env_remove("CARGO_BUILD_RUSTFLAGS")
            .output()
            .expect("failed to run cargo on the fixture");
        assert!(
            output.status.success(),
            "fixture build {salt} failed\nstderr:\n{}",
            String::from_utf8_lossy(&output.stderr),
        );

        let stdout = String::from_utf8_lossy(&output.stdout);
        let executable = stdout
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|message| {
                message["reason"] == "compiler-artifact" && message["target"]["name"] == "it"
            })
            .and_then(|message| message["executable"].as_str().map(PathBuf::from))
            .unwrap_or_else(|| panic!("build {salt} reported no test binary for `it`:\n{stdout}"));

        let events = self.events();
        assert_eq!(
            events.len(),
            before + 1,
            "build {salt} should compile `it` exactly once; events:\n{events:#?}",
        );
        Build {
            event: events.into_iter().nth(before).unwrap(),
            executable,
        }
    }

    /// Kache's events for `it`, the integration test's crate.
    fn events(&self) -> Vec<Value> {
        fs::read_to_string(self.cache.path().join("events.jsonl"))
            .unwrap_or_default()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|event| event["crate_name"] == "it")
            .collect()
    }

    /// Runs the test binary from the package directory, as `cargo test` does.
    fn backtrace(&self, build: &Build) -> Backtrace {
        let run = Command::new(&build.executable)
            .current_dir(&self.package)
            .env("RUST_BACKTRACE", "1")
            .output()
            .expect("failed to run the fixture's test binary");
        // The test fails by design.
        // Libtest captures the panic hook's output and prints it again on stdout.
        let output = format!(
            "{}{}",
            String::from_utf8_lossy(&run.stdout),
            String::from_utf8_lossy(&run.stderr)
        );
        Backtrace {
            library: frame_has_location(&output, "adaptive_debug_fixture::boom", "src/lib.rs:"),
            test: frame_has_location(&output, "it::fails_with_a_backtrace", "tests/it.rs:"),
            output,
        }
    }
}

impl Drop for Fixture {
    /// Stops the daemon a build started, if any, before its cache goes away.
    fn drop(&mut self) {
        let mut stop = hermetic_command(&self.kache, self.cache.path(), Some(&self.config()));
        stop.args(["daemon", "stop"]);
        stop_daemon(&mut stop, self.cache.path());
    }
}

/// Whether the frame named `function` is followed by a source location in `file`.
/// The frame line must end with the name, because a later `call_once` frame also names the test function,
/// and its location is in `core`.
/// Without debug information the next line is the next frame.
fn frame_has_location(backtrace: &str, function: &str, file: &str) -> bool {
    let frame = format!(": {function}");
    let lines: Vec<&str> = backtrace.lines().collect();
    lines.windows(2).any(|pair| {
        pair[0].ends_with(&frame)
            && pair[1].trim_start().starts_with("at ")
            && pair[1].contains(file)
    })
}

fn assert_passthrough(event: &Value, reason: &str) {
    assert_eq!(event["result"], "passthrough", "event: {event:#}");
    assert!(
        event["passthrough_reason"]
            .as_str()
            .is_some_and(|value| value.contains(reason)),
        "expected passthrough reason containing {reason:?}; event: {event:#}",
    );
}

/// The crate's own frames keep their file and line when the adaptive seed and active lanes relink the test binary.
/// A relative `OSO` record resolves against the reader's working directory,
/// and the test binary runs from its package directory, not from the profile directory the records would be relative to.
/// Those lanes store no `.dSYM` to fall back on, so a relative record leaves the frames without a location.
#[test]
fn adaptive_test_binaries_keep_a_readable_debug_map() {
    let fixture = Fixture::new();

    let first = fixture.build(1);
    assert_eq!(first.event["result"], "miss", "event: {:#}", first.event);

    // Each build replaces the previous test binary, so run it before the next build.
    let seed = fixture.build(2);
    assert_passthrough(&seed.event, "adaptive seed");
    let seed_backtrace = fixture.backtrace(&seed);

    let active = fixture.build(3);
    assert_passthrough(&active.event, "adaptive active");
    let active_backtrace = fixture.backtrace(&active);

    let unlocated: Vec<String> = [
        ("adaptive seed", &seed_backtrace),
        ("adaptive active", &active_backtrace),
    ]
    .into_iter()
    .filter(|(_, backtrace)| !(backtrace.library && backtrace.test))
    .map(|(lane, backtrace)| {
        format!(
            "{lane}: library frame located {}, test frame located {}\n{}",
            backtrace.library, backtrace.test, backtrace.output
        )
    })
    .collect();
    assert!(
        unlocated.is_empty(),
        "the crate's frames lost their source locations:\n{}",
        unlocated.join("\n")
    );
}
