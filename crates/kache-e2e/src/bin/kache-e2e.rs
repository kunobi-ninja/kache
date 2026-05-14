//! `kache-e2e` — drive every fixture through the lifecycle and emit
//! a single results document.
//!
//! Usage:
//!   cargo run -p kache-e2e -- \
//!     --kache ./target/release/kache \
//!     --fixtures ./test-projects \
//!     --out ./e2e-results/results.json
//!
//! Exit code: `0` if every fixture passed, `1` if any failed.

use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;
use std::process::Command;

use kache_e2e::fixture;
use kache_e2e::result::RunResults;
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
    #[arg(long, default_value = "./e2e-results/results.json")]
    out: PathBuf,

    /// Only run fixtures whose name matches this filter (substring, not
    /// regex). Useful for `cargo run -p kache-e2e -- --only c-hello`
    /// during local iteration.
    #[arg(long)]
    only: Option<String>,
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
    eprintln!();

    let mut fixture_results = Vec::new();
    let mut any_failed = false;
    for fixture in fixtures.values() {
        let result = runner::run_fixture(fixture, &kache_path)?;
        if result.status != "pass" {
            any_failed = true;
        }
        eprintln!("=> {}: {}", result.name, result.status);
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
        eprintln!("FAIL: one or more fixtures failed");
        std::process::exit(1);
    }
    eprintln!("PASS: all fixtures green");
    Ok(())
}
