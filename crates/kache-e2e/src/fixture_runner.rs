//! Fixture scenario runner entrypoint.

use anyhow::{Context, Result};
use std::path::PathBuf;
use std::process::Command;

use crate::fixture;
use crate::portable_path;
use crate::result::{FixtureResult, RunResults};
use crate::runner;
use crate::scenario::Selectors;

#[derive(Debug, Clone)]
pub struct FixtureRunConfig {
    pub kache: PathBuf,
    pub scenarios: PathBuf,
    pub out: PathBuf,
    pub only: Option<String>,
    pub select: Vec<String>,
    pub negative_control: bool,
}

pub fn run(config: FixtureRunConfig) -> Result<()> {
    let kache_path = portable_path(
        &config
            .kache
            .canonicalize()
            .with_context(|| format!("kache binary not found at {}", config.kache.display()))?,
    );

    let kache_version_out = Command::new(&kache_path)
        .arg("--version")
        .output()
        .context("running `kache --version`")?;
    let kache_version = String::from_utf8_lossy(&kache_version_out.stdout)
        .trim()
        .to_string();

    eprintln!("=== kache fixture scenarios ===");
    eprintln!("Binary:  {}", kache_path.display());
    eprintln!("Version: {kache_version}");

    let scenarios_dir =
        portable_path(&config.scenarios.canonicalize().with_context(|| {
            format!("scenarios dir not found at {}", config.scenarios.display())
        })?);

    let fallback_path = {
        let exe = std::env::current_exe().context("locating the scenario runner binary")?;
        let dir = exe
            .parent()
            .context("scenario runner binary has no parent directory")?;
        portable_path(&dir.join(format!("e2e-fallback{}", std::env::consts::EXE_SUFFIX)))
    };

    let selectors = Selectors::parse_many(&config.select)?;
    let mut fixtures = fixture::discover(&scenarios_dir, &selectors, &kache_path, &fallback_path)?;

    if let Some(filter) = &config.only {
        fixtures.retain(|name, _| name.contains(filter.as_str()));
    }
    if fixtures.is_empty() {
        anyhow::bail!(
            "no fixture scenarios discovered under {}{}{}",
            scenarios_dir.display(),
            config
                .only
                .as_ref()
                .map(|f| format!(" (after --only `{f}` filter)"))
                .unwrap_or_default(),
            format!(" (after --select {})", selectors.describe())
        );
    }
    eprintln!(
        "Fixtures: {}",
        fixtures.keys().cloned().collect::<Vec<_>>().join(", ")
    );
    eprintln!("Select:   {}", selectors.describe());
    if config.negative_control {
        // SAFETY: set once here, before the sequential fixture loop. The
        // harness is single-threaded at this point.
        unsafe { std::env::set_var("KACHE_DISABLED", "1") };
        eprintln!("Mode:     negative-control (KACHE_DISABLED=1 — fixtures must flip to fail)");
    }
    eprintln!();

    let mut fixture_results = Vec::new();
    let mut any_failed = false;
    for fixture in fixtures.values() {
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
        if config.negative_control {
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

    if let Some(parent) = config.out.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let json = serde_json::to_string_pretty(&results)?;
    std::fs::write(&config.out, &json)
        .with_context(|| format!("writing {}", config.out.display()))?;
    eprintln!("Results written to {}", config.out.display());

    if any_failed {
        eprintln!(
            "{}",
            if config.negative_control {
                "FAIL: negative-control — a fixture's test is vacuous (see above)"
            } else {
                "FAIL: one or more fixtures failed"
            }
        );
        std::process::exit(1);
    }
    eprintln!(
        "{}",
        if config.negative_control {
            "PASS: negative-control — every fixture is falsifiable"
        } else {
            "PASS: all fixtures green"
        }
    );
    Ok(())
}

fn missing_tools(tools: &[String]) -> Vec<String> {
    let path = std::env::var_os("PATH").unwrap_or_default();
    tools
        .iter()
        .filter(|tool| !std::env::split_paths(&path).any(|dir| dir.join(tool).is_file()))
        .cloned()
        .collect()
}
