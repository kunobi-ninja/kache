//! Unified scenario runner CLI.

use anyhow::{Result, bail};
use clap::Parser;
use std::path::PathBuf;

use crate::bench_runner::{self, BenchRunConfig};
use crate::fixture_runner::{self, FixtureRunConfig};
use crate::scenario::{Selectors, SourceKind, discover_metadata};

const SCENARIOS_DIR: &str = "./scenarios";

#[derive(Debug, Parser)]
#[command(about = "Run kache scenarios selected by tags/name/source kind.")]
struct Args {
    /// Path to the kache binary under test.
    #[arg(long, default_value = "./target/release/kache")]
    kache: PathBuf,

    /// Directory containing scenario subdirectories with `scenario.toml`.
    #[arg(long = "scenarios", default_value = SCENARIOS_DIR)]
    scenarios: PathBuf,

    /// Select scenarios by tag, source kind, or `name:<substring>`.
    /// Multiple selectors are ANDed.
    #[arg(long = "select")]
    select: Vec<String>,

    /// Scenario name filter. Shorthand for `--select name:<profile>`.
    #[arg(long)]
    profile: Option<String>,

    /// E2E result JSON path for selected fixture scenarios.
    #[arg(long, default_value = "./tmp/e2e/results.json")]
    out: PathBuf,

    /// Legacy fixture-only name substring filter.
    #[arg(long)]
    only: Option<String>,

    /// Fixture falsifiability mode. Valid only when selected scenarios are
    /// fixtures.
    #[arg(long)]
    negative_control: bool,

    /// Override a selected clone scenario's pinned `source.ref`.
    #[arg(long = "ref")]
    git_ref: Option<String>,

    /// Benchmark work dir for a selected clone scenario.
    #[arg(long)]
    work_dir: Option<PathBuf>,

    /// Reuse clone worktrees already present under the benchmark work dir.
    #[arg(long)]
    skip_clone: bool,

    /// Re-run clone scenario setup even if its setup marker exists.
    #[arg(long)]
    force_setup: bool,

    /// Re-measure only the warm benchmark phase from a prior cold snapshot.
    #[arg(long)]
    retry: bool,

    /// Enable cache-key trace logging for clone benchmark scenarios.
    #[arg(long)]
    trace_keys: bool,
}

pub fn main() -> Result<()> {
    let args = Args::parse();
    let mut select = args.select.clone();
    if let Some(profile) = &args.profile {
        select.push(format!("name:{profile}"));
    }
    if select.is_empty() {
        bail!(
            "no scenario selectors provided; pass --select suite:e2e, --select suite:bench, or --profile <name>"
        );
    }

    let selectors = Selectors::parse_many(&select)?;
    let metadata = discover_metadata(&args.scenarios)?;
    let selected = metadata
        .iter()
        .filter(|scenario| selectors.matches_metadata(scenario))
        .collect::<Vec<_>>();
    if selected.is_empty() {
        bail!(
            "no scenarios selected under {} (after --select {})",
            args.scenarios.display(),
            selectors.describe()
        );
    }

    let fixture_selected = selected
        .iter()
        .any(|scenario| scenario.source_kind == SourceKind::Fixture);
    let clone_selected = selected
        .iter()
        .any(|scenario| scenario.source_kind == SourceKind::Clone);

    if args.negative_control && clone_selected {
        bail!("--negative-control is only valid for fixture scenarios");
    }
    let bench_flags = args.git_ref.is_some()
        || args.work_dir.is_some()
        || args.skip_clone
        || args.force_setup
        || args.retry
        || args.trace_keys;
    if bench_flags && !clone_selected {
        bail!("benchmark options were provided, but no clone scenario was selected");
    }

    if fixture_selected {
        fixture_runner::run(FixtureRunConfig {
            kache: args.kache.clone(),
            scenarios: args.scenarios.clone(),
            out: args.out.clone(),
            only: args.only.clone(),
            select: select.clone(),
            negative_control: args.negative_control,
        })?;
    }

    if clone_selected {
        bench_runner::run_bench(BenchRunConfig {
            kache: args.kache,
            scenarios: args.scenarios,
            select,
            git_ref: args.git_ref,
            work_dir: args.work_dir,
            skip_clone: args.skip_clone,
            force_setup: args.force_setup,
            retry: args.retry,
            trace_keys: args.trace_keys,
        })?;
    }

    Ok(())
}
