//! Explicit, bounded replay of a compatible session's observed cache keys.

mod hooks;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Serialize;
use tokio::io::AsyncWrite;

use crate::build_reports::{BuildIdentity, BuildReport};
use crate::cache_remote::CacheRemote;
use crate::config::Config;
use crate::remote_backend::{GetObject, GetTransfer, RemoteBackend};
use crate::store::Store;

#[derive(Debug, clap::Args)]
pub(crate) struct Options {
    /// Repository namespace used when the producer saved its session report.
    #[arg(long)]
    namespace: Option<String>,
    /// Repository label; defaults to KACHE_REPOSITORY, then the namespace.
    #[arg(long)]
    repository: Option<String>,
    /// Producer-declared build configuration label (KACHE_BUILD_SHAPE).
    #[arg(long)]
    build_shape: Option<String>,
    /// Compilation target; defaults to this compiler's host.
    #[arg(long)]
    target: Option<String>,
    /// Cargo output profile directory (debug, release, or a custom profile).
    #[arg(long, default_value = "debug")]
    profile: String,
    /// Compiler whose version must match the producer.
    #[arg(long)]
    rustc: Option<PathBuf>,
    /// Print the selected keys without downloading them.
    #[arg(long)]
    dry_run: bool,
    /// Install post-checkout and post-merge hooks with these replay options.
    #[arg(long, conflicts_with_all = ["uninstall_hooks", "dry_run"])]
    install_hooks: bool,
    /// Remove only hooks installed by this command.
    #[arg(long, conflicts_with = "dry_run")]
    uninstall_hooks: bool,
}

#[derive(Debug, Default, Serialize)]
struct ReplaySummary {
    session_id: String,
    restored: usize,
    local: usize,
    busy: usize,
    failed: usize,
    omitted: usize,
    downloaded_bytes: u64,
    budget_exhausted: bool,
}

impl ReplaySummary {
    fn record_local(&mut self) {
        self.local += 1;
    }

    fn record_busy(&mut self) {
        self.busy += 1;
    }
}

fn option_or_env(option: &Option<String>, name: &str) -> Option<String> {
    option
        .clone()
        .or_else(|| std::env::var(name).ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(crate) fn run(config: &Config, options: &Options) -> Result<()> {
    if options.uninstall_hooks {
        return hooks::uninstall();
    }
    let namespace = option_or_env(&options.namespace, "KACHE_NAMESPACE");
    let shape = option_or_env(&options.build_shape, "KACHE_BUILD_SHAPE");
    let (Some(namespace), Some(shape)) = (namespace, shape) else {
        eprintln!(
            "No warm set selected: supply --namespace and --build-shape, or their KACHE environment variables."
        );
        return Ok(());
    };
    let repository =
        option_or_env(&options.repository, "KACHE_REPOSITORY").unwrap_or_else(|| namespace.clone());
    let rustc = options
        .rustc
        .clone()
        .or_else(|| std::env::var_os("RUSTC").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("rustc"));
    let toolchain = crate::warm_set_toolchain::probe(&rustc)?;
    let target = options.target.clone().unwrap_or(toolchain.host);
    if options.install_hooks {
        return hooks::install(
            &namespace,
            &repository,
            &shape,
            &target,
            &options.profile,
            &rustc,
        );
    }
    let Some(remote) = config.remote.as_ref() else {
        eprintln!("No warm set selected: no remote cache is configured.");
        return Ok(());
    };
    let Some(lock_digest) = crate::build_reports::lock_digest(Path::new("Cargo.lock")) else {
        eprintln!("No warm set selected: Cargo.lock is missing or empty.");
        return Ok(());
    };
    let expected = BuildIdentity {
        repository: Some(repository),
        target: Some(target),
        toolchain_hash: Some(toolchain.hash),
        profile: Some(options.profile.clone()),
        build_shape: Some(shape),
        lock_digest: Some(lock_digest),
    };
    let seconds = config.prefetch_deadline_secs;
    if seconds == 0 {
        bail!("explicit prefetch requires a finite KACHE_PREFETCH_DEADLINE_SECS");
    }
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(seconds))
        .context("prefetch deadline is too large")?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let backend = crate::remote_resilience::RemoteDeadline::from_instant(Some(deadline))
            .run("warm-set backend setup", crate::remote_backend::create_backend_for(
                remote, config.pull_request_prefix.as_deref(), config.s3_pool_idle_secs,
            )).await?;
        let report = select_report(backend.as_ref(), &remote.prefix, &namespace, &expected, deadline).await?;
        let Some(report) = report else {
            eprintln!("No warm set selected: no report has matching repository, lockfile, target, toolchain, profile and declared build shape.");
            return Ok(());
        };
        if options.dry_run {
            for entry in ordered_entries(&report) {
                println!("{} {}", entry.cache_key, entry.crate_name);
            }
            return Ok(());
        }
        if config.prefetch_max_keys == 0 || config.prefetch_max_bytes == 0 {
            bail!("explicit prefetch requires finite nonzero key and byte budgets");
        }
        let budgeted = Arc::new(BudgetedBackend::new(backend, config.prefetch_max_bytes));
        let client = crate::cache_remote::V3Remote::new(budgeted.clone(), remote.clone());
        let summary = replay(&client, config, &report, config.prefetch_max_keys, deadline, &budgeted.remaining).await?;
        println!("Prefetched {} entries; {} local, {} busy, {} failed, {} omitted; {} compressed bytes.",
            summary.restored, summary.local, summary.busy, summary.failed, summary.omitted, summary.downloaded_bytes);
        if summary.budget_exhausted {
            eprintln!("Prefetch stopped at its key, byte or wall-time budget.");
        }
        if summary.failed > 0 {
            bail!("{} warm-set entries could not be restored; existing entries were retained", summary.failed);
        }
        Ok(())
    })
}

fn compatible(actual: &BuildIdentity, expected: &BuildIdentity) -> bool {
    [
        &expected.repository,
        &expected.target,
        &expected.toolchain_hash,
        &expected.profile,
        &expected.build_shape,
        &expected.lock_digest,
    ]
    .into_iter()
    .all(|field| field.is_some())
        && actual == expected
}

async fn select_report(
    backend: &dyn RemoteBackend,
    prefix: &str,
    namespace: &str,
    expected: &BuildIdentity,
    deadline: Instant,
) -> Result<Option<BuildReport>> {
    let timeout = crate::remote_resilience::RemoteDeadline::from_instant(Some(deadline));
    timeout.check("warm-set discovery")?;
    let keys = timeout
        .run(
            "warm-set discovery",
            crate::build_reports::list_reports(backend, prefix, namespace),
        )
        .await?;
    let mut selected: Option<BuildReport> = None;
    for key in keys {
        timeout.check("warm-set report")?;
        let report = timeout
            .run(
                "warm-set report",
                crate::build_reports::download_report(backend, prefix, namespace, &key),
            )
            .await;
        match report {
            Ok(Some(report)) if compatible(&report.identity, expected) => {
                selected = Some(match selected {
                    None => report,
                    Some(prior) => std::cmp::max_by(prior, report, |a, b| {
                        (a.finished_at_ms, &a.session_id, &a.root_hash).cmp(&(
                            b.finished_at_ms,
                            &b.session_id,
                            &b.root_hash,
                        ))
                    }),
                });
            }
            Ok(_) => {}
            Err(error) => {
                if Instant::now() >= deadline {
                    return Err(error);
                }
                eprintln!("Skipping unreadable warm-set report: {error:#}");
            }
        }
    }
    Ok(selected)
}

fn ordered_entries(report: &BuildReport) -> Vec<&crate::build_reports::ReportEntry> {
    let mut seen = HashSet::new();
    report
        .entries
        .iter()
        .filter(|entry| seen.insert(&entry.cache_key))
        .collect()
}

/// Download one entry at a time in recorded order. The configured concurrency
/// ceiling is therefore respected even when it is one. Extract into an owned
/// staging directory so a racing publisher's entry cannot be replaced by GET.
async fn replay(
    client: &dyn CacheRemote,
    config: &Config,
    report: &BuildReport,
    max_keys: u64,
    deadline: Instant,
    remaining: &AtomicU64,
) -> Result<ReplaySummary> {
    crate::build_reports::validate_report(report)?;
    let store = Store::open(config)?;
    let entries = ordered_entries(report);
    let mut summary = ReplaySummary {
        session_id: report.session_id.clone(),
        ..Default::default()
    };
    let mut started = 0;
    for (index, entry) in entries.iter().enumerate() {
        if store.contains(&entry.cache_key) {
            summary.record_local();
            continue;
        }
        if started >= max_keys
            || remaining.load(Ordering::Relaxed) == 0
            || Instant::now() >= deadline
        {
            summary.omitted = entries.len() - index;
            summary.budget_exhausted = true;
            break;
        }
        let Some(_lock) = store.try_lock(&entry.cache_key)? else {
            summary.record_busy();
            continue;
        };
        // A compiler may have committed while this command acquired the lock.
        if store.contains(&entry.cache_key) {
            summary.record_local();
            continue;
        }
        let destination = store.entry_dir(&entry.cache_key);
        if destination.exists() {
            summary.record_busy();
            continue;
        }
        let staging = tempfile::Builder::new()
            .prefix("warm-set-")
            .tempdir_in(config.store_dir())?;
        let incoming = staging.path().join("entry");
        started += 1;
        let result = client
            .download_entry(
                &entry.cache_key,
                &entry.crate_name,
                &incoming,
                &config.store_dir().join("blobs"),
                Some(deadline),
            )
            .await;
        match result {
            Ok(download) => {
                summary.downloaded_bytes = summary
                    .downloaded_bytes
                    .saturating_add(download.compressed_bytes);
                if destination.exists() {
                    summary.record_busy();
                    continue;
                }
                // An existing committed generation is a nonempty directory;
                // rename cannot replace it, including a publication after the
                // recheck above. Never remove the destination on this path.
                match std::fs::rename(&incoming, &destination) {
                    Ok(()) => match store.import_restored_entry(&entry.cache_key) {
                        Ok(()) => summary.restored += 1,
                        Err(error) => {
                            summary.failed += 1;
                            eprintln!("Cannot import {}: {error:#}", entry.cache_key);
                        }
                    },
                    Err(_) if destination.exists() => summary.record_busy(),
                    Err(error) => {
                        summary.failed += 1;
                        eprintln!("Cannot publish {}: {error}", entry.cache_key);
                    }
                }
            }
            Err(error) => {
                summary.failed += 1;
                eprintln!("Cannot restore {}: {error:#}", entry.cache_key);
            }
        }
    }
    Ok(summary)
}

/// The restore pipeline's transport cap is narrowed to the remaining total.
/// Failed bodies conservatively consume the admitted cap because the backend
/// cannot report a partial transfer's byte count.
struct BudgetedBackend {
    inner: Arc<dyn RemoteBackend>,
    remaining: AtomicU64,
}

impl BudgetedBackend {
    fn new(inner: Arc<dyn RemoteBackend>, bytes: u64) -> Self {
        Self {
            inner,
            remaining: AtomicU64::new(bytes),
        }
    }

    fn take(&self, max_bytes: Option<u64>) -> Result<u64> {
        let mut available = self.remaining.load(Ordering::Relaxed);
        loop {
            if available == 0 {
                bail!("prefetch byte budget exhausted");
            }
            let admitted = available.min(max_bytes.unwrap_or(available));
            if admitted == 0 {
                bail!("remote body cap is zero");
            }
            match self.remaining.compare_exchange_weak(
                available,
                available - admitted,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(admitted),
                Err(current) => available = current,
            }
        }
    }

    fn refund(&self, admitted: u64, used: u64) -> Result<()> {
        if used > admitted {
            bail!("remote backend exceeded its body limit");
        }
        self.remaining.fetch_add(admitted - used, Ordering::Relaxed);
        Ok(())
    }
}

#[async_trait]
impl RemoteBackend for BudgetedBackend {
    async fn head(&self, key: &str) -> Result<bool> {
        self.inner.head(key).await
    }
    async fn get(&self, key: &str, max_bytes: Option<u64>) -> Result<Option<GetObject>> {
        let admitted = self.take(max_bytes)?;
        let object = self.inner.get(key, Some(admitted)).await?;
        self.refund(
            admitted,
            object.as_ref().map_or(0, |object| object.body.len() as u64),
        )?;
        Ok(object)
    }
    async fn get_into(
        &self,
        key: &str,
        max_bytes: Option<u64>,
        destination: &mut (dyn AsyncWrite + Unpin + Send),
    ) -> Result<Option<GetTransfer>> {
        let admitted = self.take(max_bytes)?;
        let object = self
            .inner
            .get_into(key, Some(admitted), destination)
            .await?;
        self.refund(admitted, object.as_ref().map_or(0, |object| object.bytes))?;
        Ok(object)
    }
    async fn put(&self, _key: &str, _body: Vec<u8>, _content_type: Option<&str>) -> Result<()> {
        bail!("prefetch is read-only")
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix).await
    }
    fn describe(&self, key: &str) -> String {
        self.inner.describe(key)
    }
}

#[cfg(test)]
mod tests;
