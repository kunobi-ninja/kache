//! `kache-e2e` — drive every fixture through the lifecycle and emit
//! a single results document.
//!
//! Usage:
//!   cargo run -p kache-e2e -- \
//!     --kache ./target/release/kache \
//!     --fixtures ./test-projects \
//!     --out ./tmp/e2e/results.json
//!
//! Exit code: `0` if every fixture passed, `1` if any failed.

use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;
use std::process::Command;

use kache_e2e::fixture;
use kache_e2e::result::{FixtureResult, RunResults};
use kache_e2e::runner;

#[derive(Debug, Parser)]
#[command(about = "End-to-end test harness for kache.")]
struct Args {
    /// Path to the kache binary under test.
    #[arg(long, default_value = "./target/release/kache")]
    kache: PathBuf,

    /// Directory containing per-fixture subdirectories with
    /// `kache-fixture.toml`.
    #[arg(long, default_value = "./test-projects")]
    fixtures: PathBuf,

    /// Where to write the results JSON. Parent dir is created if missing.
    /// Default lives under the repo's `tmp/` convention (gitignored).
    #[arg(long, default_value = "./tmp/e2e/results.json")]
    out: PathBuf,

    /// Only run fixtures whose name matches this filter (substring, not
    /// regex). Useful for `cargo run -p kache-e2e -- --only c-hello`
    /// during local iteration.
    #[arg(long)]
    only: Option<String>,

    /// Falsifiability check. Reruns every fixture with kache disabled
    /// (`KACHE_DISABLED=1`) and asserts each result FLIPS to a failure
    /// — a fixture whose caching assertions still pass with kache off
    /// has a vacuous test. Fixtures marked `negative_control_exempt`
    /// (passthrough fixtures) must still pass. Exits 0 iff every
    /// fixture behaved as expected.
    #[arg(long)]
    negative_control: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let kache_path = args
        .kache
        .canonicalize()
        .with_context(|| format!("kache binary not found at {}", args.kache.display()))?;

    let kache_version_out = Command::new(&kache_path)
        .arg("--version")
        .output()
        .context("running `kache --version`")?;
    let kache_version = String::from_utf8_lossy(&kache_version_out.stdout)
        .trim()
        .to_string();

    eprintln!("=== kache e2e harness ===");
    eprintln!("Binary:  {}", kache_path.display());
    eprintln!("Version: {kache_version}");

    let fixtures_dir = args
        .fixtures
        .canonicalize()
        .with_context(|| format!("fixtures dir not found at {}", args.fixtures.display()))?;
    let mut fixtures = fixture::discover(&fixtures_dir, &kache_path)?;

    if let Some(filter) = &args.only {
        fixtures.retain(|name, _| name.contains(filter.as_str()));
    }
    if fixtures.is_empty() {
        anyhow::bail!(
            "no fixtures discovered under {}{}",
            fixtures_dir.display(),
            args.only
                .as_ref()
                .map(|f| format!(" (after --only `{f}` filter)"))
                .unwrap_or_default()
        );
    }
    eprintln!(
        "Fixtures: {}",
        fixtures.keys().cloned().collect::<Vec<_>>().join(", ")
    );
    if args.negative_control {
        // SAFETY: set once here, before the sequential fixture loop —
        // the harness is single-threaded at this point. Disables kache
        // for every fixture build so we can assert the suite genuinely
        // depends on kache (see the `--negative-control` doc).
        unsafe { std::env::set_var("KACHE_DISABLED", "1") };
        eprintln!("Mode:     negative-control (KACHE_DISABLED=1 — fixtures must flip to fail)");
    }
    eprintln!();

    let mut fixture_results = Vec::new();
    let mut any_failed = false;
    for fixture in fixtures.values() {
        // Skip a fixture whose required external tooling is absent.
        // `"skip"` counts as neither a pass nor a failure — in both
        // normal and negative-control mode the fixture is passed over
        // entirely, so a missing optional tool never reds the suite.
        let missing = missing_tools(&fixture.requires);
        if !missing.is_empty() {
            eprintln!(
                "=> {}: skipped — {} not on PATH",
                fixture.name,
                missing.join(", ")
            );
            eprintln!();
            fixture_results.push(FixtureResult {
                name: fixture.name.clone(),
                status: "skip".to_string(),
                phases: Vec::new(),
            });
            continue;
        }
        let result = runner::run_fixture(fixture, &kache_path)?;
        let passed = result.status == "pass";
        if args.negative_control {
            // Falsifiability verdict: with kache disabled a non-exempt
            // fixture MUST fail (its caching assertions cannot be met);
            // an exempt passthrough fixture must still pass. Either
            // mismatch means the fixture's test does not actually
            // exercise kache.
            let want_pass = fixture.negative_control_exempt;
            let ok = passed == want_pass;
            if !ok {
                any_failed = true;
            }
            let verdict = if ok {
                "falsifiable"
            } else if want_pass {
                "BROKEN — exempt fixture should still pass"
            } else {
                "VACUOUS — test passes even with kache disabled"
            };
            eprintln!(
                "=> {}: kache-disabled run {} [{}]",
                result.name,
                if passed { "PASSED" } else { "failed" },
                verdict,
            );
        } else {
            if !passed {
                any_failed = true;
            }
            eprintln!("=> {}: {}", result.name, result.status);
        }
        eprintln!();
        fixture_results.push(result);
    }

    let results = RunResults {
        kache_binary: kache_path.display().to_string(),
        kache_version,
        platform: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
        fixtures: fixture_results,
    };

    if let Some(parent) = args.out.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let json = serde_json::to_string_pretty(&results)?;
    std::fs::write(&args.out, &json).with_context(|| format!("writing {}", args.out.display()))?;
    eprintln!("Results written to {}", args.out.display());

    if any_failed {
        eprintln!(
            "{}",
            if args.negative_control {
                "FAIL: negative-control — a fixture's test is vacuous (see above)"
            } else {
                "FAIL: one or more fixtures failed"
            }
        );
        std::process::exit(1);
    }
    eprintln!(
        "{}",
        if args.negative_control {
            "PASS: negative-control — every fixture is falsifiable"
        } else {
            "PASS: all fixtures green"
        }
    );
    Ok(())
}

/// The subset of `tools` not found as a regular file on any `PATH`
/// entry. Drives a fixture's `requires` skip: an empty result means
/// every required tool is available.
fn missing_tools(tools: &[String]) -> Vec<String> {
    let path = std::env::var_os("PATH").unwrap_or_default();
    tools
        .iter()
        .filter(|tool| !std::env::split_paths(&path).any(|dir| dir.join(tool).is_file()))
        .cloned()
        .collect()
}
