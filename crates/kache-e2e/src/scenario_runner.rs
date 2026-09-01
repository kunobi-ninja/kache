//! Unified scenario runner CLI.

use anyhow::{Result, bail};
use clap::Parser;
use std::path::PathBuf;

use crate::bench_runner::{self, BenchRunConfig, CacheBackend};
use crate::fixture_runner::{self, FixtureRunConfig};
use crate::scenario::{ScenarioMetadata, Selectors, SourceKind, discover_metadata};

const SCENARIOS_DIR: &str = "./scenarios";

#[derive(Debug, Parser)]
#[command(about = "Run kache scenarios selected by tags/name/source kind.")]
struct Args {
    /// Path to the kache binary under test.
    #[arg(long, default_value = "./target/release/kache")]
    kache: PathBuf,

    /// Path to the sccache binary when running `--cache-backend sccache`.
    #[arg(long, default_value = "sccache")]
    sccache: PathBuf,

    /// Compiler-cache backend for clone benchmark scenarios.
    #[arg(long, value_enum, default_value_t = CacheBackend::Kache)]
    cache_backend: CacheBackend,

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

    /// List selected scenarios instead of running them.
    #[arg(long)]
    list: bool,

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

    /// Fail after writing results if a supported fixture is missing a required tool.
    #[arg(long)]
    deny_missing_tools: bool,

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

    /// Add a same-worktree warm rebuild between the cold and cross-clone
    /// phases (clone-a again, objdir wiped, store left warm). Off by default:
    /// it costs a third full build, which only the PR perf gate wants.
    #[arg(long)]
    warm_same_tree: bool,
}

impl Args {
    /// Was any clone-benchmark-only option supplied?
    ///
    /// These options mean nothing to a fixture scenario, so passing one without
    /// selecting a clone scenario is a mistake worth reporting rather than
    /// silently ignoring — the caller would otherwise believe a flag took
    /// effect when it did not. Every benchmark option belongs on this list;
    /// forgetting to add a new one is how a flag becomes silently inert.
    fn has_bench_options(&self) -> bool {
        self.git_ref.is_some()
            || self.work_dir.is_some()
            || self.skip_clone
            || self.force_setup
            || self.retry
            || self.trace_keys
            || self.warm_same_tree
            || self.cache_backend != CacheBackend::Kache
    }
}

pub fn main() -> Result<()> {
    let args = Args::parse();
    let mut select = args.select.clone();
    if let Some(profile) = &args.profile {
        select.push(format!("name:{profile}"));
    }
    if select.is_empty() && !args.list {
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
    if args.list {
        print_scenario_list(&selected, args.cache_backend);
        return Ok(());
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
    if args.deny_missing_tools && clone_selected {
        bail!("--deny-missing-tools is only valid for fixture scenarios");
    }
    let bench_flags = args.has_bench_options();
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
            deny_missing_tools: args.deny_missing_tools,
        })?;
    }

    if clone_selected {
        bench_runner::run_bench(BenchRunConfig {
            kache: args.kache,
            sccache: args.sccache,
            cache_backend: args.cache_backend,
            scenarios: args.scenarios,
            select,
            git_ref: args.git_ref,
            work_dir: args.work_dir,
            skip_clone: args.skip_clone,
            force_setup: args.force_setup,
            retry: args.retry,
            trace_keys: args.trace_keys,
            warm_same_tree: args.warm_same_tree,
        })?;
    }

    Ok(())
}

fn print_scenario_list(selected: &[&ScenarioMetadata], cache_backend: CacheBackend) {
    let benchmark_only = selected
        .iter()
        .all(|scenario| scenario.tags.iter().any(|tag| tag == "suite:bench"));
    if benchmark_only {
        println!("{} benchmark profiles:", cache_backend_label(cache_backend));
    } else {
        println!("matching profiles:");
    }
    for scenario in selected {
        println!("  {}", profile_hint(&scenario.name, cache_backend));
    }
    if benchmark_only {
        println!("use: {}", benchmark_recipe_hint(cache_backend));
    }
}

fn cache_backend_label(cache_backend: CacheBackend) -> &'static str {
    match cache_backend {
        CacheBackend::Kache => "kache",
        CacheBackend::Sccache => "sccache",
    }
}

fn benchmark_recipe_hint(cache_backend: CacheBackend) -> &'static str {
    match cache_backend {
        CacheBackend::Kache => "just bench <profile>",
        CacheBackend::Sccache => "just bench-sccache <profile>",
    }
}

fn profile_hint(name: &str, cache_backend: CacheBackend) -> String {
    let mut profile = name.strip_prefix("bench-").unwrap_or(name).to_string();
    if cache_backend == CacheBackend::Sccache
        && let Some(stripped) = profile.strip_suffix("-sccache")
    {
        profile = stripped.to_string();
    }
    profile
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every clone-benchmark option, on its own, must be recognised as one.
    /// The check is a long disjunction, and a term that stops contributing
    /// makes its flag silently inert against a fixture selection instead of
    /// producing the "no clone scenario was selected" error.
    #[test]
    fn every_benchmark_option_is_recognised_on_its_own() {
        let plain = Args::parse_from(["kache-scenario", "--select", "suite:e2e"]);
        assert!(
            !plain.has_bench_options(),
            "a selection with no benchmark option must not look like one"
        );

        for option in [
            vec!["--ref", "797e8a9"],
            vec!["--work-dir", "tmp/bench/x"],
            vec!["--skip-clone"],
            vec!["--force-setup"],
            vec!["--retry"],
            vec!["--trace-keys"],
            vec!["--warm-same-tree"],
            vec!["--cache-backend", "sccache"],
        ] {
            let mut argv = vec!["kache-scenario", "--select", "suite:e2e"];
            argv.extend(option.iter().copied());
            let args = Args::parse_from(&argv);
            assert!(
                args.has_bench_options(),
                "{option:?} must count as a benchmark option"
            );
        }
    }

    #[test]
    fn profile_hint_matches_bench_recipe_arguments() {
        assert_eq!(
            profile_hint("bench-firefox", CacheBackend::Kache),
            "firefox"
        );
        assert_eq!(
            profile_hint("bench-substrate", CacheBackend::Kache),
            "substrate"
        );
        assert_eq!(
            profile_hint("bench-firefox-sccache", CacheBackend::Sccache),
            "firefox"
        );
    }
}
