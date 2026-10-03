//! Machine-readable CLI output, and the store-vs-disk split that output uses.
//!
//! Interactive surfaces (`kache monitor`, `kache config`, the `clean` selector)
//! stay human. Setup, reports, diagnostics and disk-management commands use
//! one versioned document on stdout, including argument and command failures.

use anyhow::Result;
pub use kache_store::filesystem::*;
use serde::Serialize;
use std::path::Path;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

static JSON_MODE: AtomicBool = AtomicBool::new(false);
static COMMAND: OnceLock<String> = OnceLock::new();

pub(crate) fn configure(json: bool, command: String) {
    JSON_MODE.store(json, Ordering::Relaxed);
    let _ = COMMAND.set(command);
}

pub(crate) fn command() -> &'static str {
    COMMAND.get().map(String::as_str).unwrap_or("cli")
}

pub(crate) fn is_json() -> bool {
    JSON_MODE.load(Ordering::Relaxed)
}

/// JSON document version. Bump on breaking field/meaning changes; additive
/// fields do not bump it.
pub const SCHEMA_VERSION: u32 = 1;

/// How complete the clone probe is on this platform.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClonedCoverage {
    /// macOS getattrlist private/shared sizes are trustworthy.
    Full,
    /// Linux FIEMAP works on some filesystems and not others.
    Partial,
    /// No read-side clone query (Windows ReFS clone is write-only).
    Unknown,
}

/// Bytes the store names vs bytes the filesystem would actually give back.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DiskView {
    pub store_bytes: u64,
    pub store_limit_bytes: u64,
    pub disk_private_bytes: u64,
    pub cloned_into_targets_bytes: u64,
    /// Bytes only filesystem snapshots hold. Evictable: they are freed when
    /// the snapshots are deleted, not by cleaning build outputs.
    pub snapshot_retained_bytes: u64,
    pub cloned_coverage: ClonedCoverage,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct NextAction {
    pub argv: Vec<String>,
    pub why: String,
}

#[derive(Debug, Serialize)]
struct JsonDoc<'a, T: Serialize> {
    schema_version: u32,
    command: &'a str,
    success: bool,
    #[serde(flatten)]
    body: T,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    next: Vec<NextAction>,
}

/// Write one JSON document to stdout.
pub fn emit<T: Serialize>(command: &str, body: T, next: Vec<NextAction>) -> Result<()> {
    let doc = JsonDoc {
        schema_version: SCHEMA_VERSION,
        command,
        success: true,
        body,
        next,
    };
    serde_json::to_writer_pretty(std::io::stdout(), &doc)?;
    println!();
    Ok(())
}

/// Failures use the same stdout document contract as successful commands.
pub(crate) fn emit_error(command: &str, code: &str, error: &anyhow::Error) -> Result<()> {
    #[derive(Serialize)]
    struct Failure<'a> {
        code: &'a str,
        message: String,
        causes: Vec<String>,
    }
    #[derive(Serialize)]
    struct Body<'a> {
        error: Failure<'a>,
    }
    let doc = JsonDoc {
        schema_version: SCHEMA_VERSION,
        command,
        success: false,
        body: Body {
            error: Failure {
                code,
                message: error.to_string(),
                causes: error.chain().skip(1).map(ToString::to_string).collect(),
            },
        },
        next: Vec::new(),
    };
    serde_json::to_writer_pretty(std::io::stdout(), &doc)?;
    println!();
    Ok(())
}

/// The command tree lets scripts discover syntax without parsing help prose.
fn help_document(mut command: clap::Command) -> impl Serialize {
    #[derive(Serialize)]
    struct Argument {
        name: String,
        long: Option<String>,
        short: Option<char>,
        required: bool,
        description: Option<String>,
        values: Vec<String>,
    }
    #[derive(Serialize)]
    struct Command {
        name: String,
        description: Option<String>,
        usage: String,
        arguments: Vec<Argument>,
        subcommands: Vec<Command>,
    }
    fn describe(command: &mut clap::Command) -> Command {
        let usage = command.render_usage().to_string();
        Command {
            name: command.get_name().to_owned(),
            description: command.get_about().map(ToString::to_string),
            usage,
            arguments: command
                .get_arguments()
                .filter(|arg| !arg.is_hide_set())
                .map(|arg| Argument {
                    name: arg.get_id().to_string(),
                    long: arg.get_long().map(str::to_owned),
                    short: arg.get_short(),
                    required: arg.is_required_set(),
                    description: arg.get_help().map(ToString::to_string),
                    values: arg
                        .get_possible_values()
                        .into_iter()
                        .filter(|value| !value.is_hide_set())
                        .map(|value| value.get_name().to_owned())
                        .collect(),
                })
                .collect(),
            subcommands: command
                .get_subcommands_mut()
                .filter(|command| !command.is_hide_set())
                .map(describe)
                .collect(),
        }
    }
    describe(&mut command)
}

pub(crate) fn emit_help(command: clap::Command) -> Result<()> {
    #[derive(Serialize)]
    struct Body<T> {
        cli: T,
    }
    emit(
        "help",
        Body {
            cli: help_document(command),
        },
        Vec::new(),
    )
}

pub fn require_tty(is_tty: bool, command: &str, alternative: &str) -> Result<()> {
    if is_tty {
        Ok(())
    } else {
        anyhow::bail!(
            "`kache {command}` needs a terminal. For scripts and agents, use {alternative}."
        );
    }
}

pub fn cloned_coverage() -> ClonedCoverage {
    if cfg!(target_os = "macos") {
        ClonedCoverage::Full
    } else if cfg!(target_os = "linux") {
        ClonedCoverage::Partial
    } else {
        ClonedCoverage::Unknown
    }
}

/// Walk `store_dir/blobs` and split apparent blob bytes into private vs cloned.
pub fn disk_view(store_dir: &Path, store_bytes: u64, store_limit_bytes: u64) -> DiskView {
    let probed = probe_store_blobs(store_dir);
    disk_view_from_probe(store_bytes, store_limit_bytes, probed)
}

fn disk_view_from_probe(store_bytes: u64, store_limit_bytes: u64, probed: ProbeTotals) -> DiskView {
    // Prefer the indexed store size when the walk and the index disagree:
    // the index is what `max_size` bounds. Scale the probe split to it when
    // the walk found anything.
    let scale = |bytes: u64| {
        if probed.apparent_bytes == 0 {
            0
        } else {
            ((bytes as u128 * store_bytes as u128) / probed.apparent_bytes as u128) as u64
        }
    };
    let cloned = scale(probed.cloned_bytes);
    let snapshot = scale(probed.snapshot_bytes);
    DiskView {
        store_bytes,
        store_limit_bytes,
        disk_private_bytes: store_bytes.saturating_sub(cloned).saturating_sub(snapshot),
        cloned_into_targets_bytes: cloned,
        snapshot_retained_bytes: snapshot,
        cloned_coverage: cloned_coverage(),
    }
}

#[derive(Debug, Default)]
struct ProbeTotals {
    apparent_bytes: u64,
    cloned_bytes: u64,
    snapshot_bytes: u64,
}

fn probe_store_blobs(store_dir: &Path) -> ProbeTotals {
    let mut totals = ProbeTotals::default();
    let blobs_dir = store_dir.join("blobs");
    let Ok(shards) = std::fs::read_dir(&blobs_dir) else {
        return totals;
    };
    for shard in shards.flatten() {
        let Ok(file_type) = shard.file_type() else {
            continue;
        };
        if !file_type.is_dir() {
            continue;
        }
        let Ok(blobs) = std::fs::read_dir(shard.path()) else {
            continue;
        };
        for blob in blobs.flatten() {
            let Some(retainer) = retainer_from_meta(&blob.path()) else {
                continue;
            };
            totals.apparent_bytes = totals.apparent_bytes.saturating_add(retainer.size);
            if retainer.cloned {
                totals.cloned_bytes = totals.cloned_bytes.saturating_add(retainer.size);
            } else {
                let cloned = retainer
                    .size
                    .saturating_sub(retainer.private_bytes)
                    .saturating_sub(retainer.snapshot_bytes);
                totals.cloned_bytes = totals.cloned_bytes.saturating_add(cloned);
                totals.snapshot_bytes = totals
                    .snapshot_bytes
                    .saturating_add(retainer.snapshot_bytes);
            }
        }
    }
    totals
}

pub fn next_for_clones(disk: &DiskView) -> Vec<NextAction> {
    let mut next = Vec::new();
    if disk.cloned_into_targets_bytes > 0 {
        next.extend(clean_tracked_targets_action());
    }
    next.extend(snapshot_action(disk));
    next
}

/// Snapshot-held bytes are freed by deleting snapshots, not build outputs.
fn snapshot_action(disk: &DiskView) -> Option<NextAction> {
    (disk.snapshot_retained_bytes > 0).then(|| NextAction {
        argv: vec!["tmutil".into(), "listlocalsnapshots".into(), "/".into()],
        why: "filesystem snapshots still hold blocks of evicted or evictable blobs; \
              they are freed when the snapshots are thinned or deleted"
            .into(),
    })
}

fn clean_tracked_targets_action() -> Vec<NextAction> {
    vec![NextAction {
        argv: vec![
            "kache".into(),
            "clean".into(),
            "--tracked".into(),
            "--stale".into(),
            "14d".into(),
            "--dry-run".into(),
        ],
        why: "tracked build outputs still hold blocks; cleaning stale target directories is what frees disk".into(),
    }]
}

pub fn next_after_gc(
    disk: &DiskView,
    unreclaimable: usize,
    disk_reclaimed: u64,
    store_removed: u64,
) -> Vec<NextAction> {
    let leftover = store_removed.saturating_sub(disk_reclaimed);
    let mut next = if unreclaimable > 0 || leftover > 0 || disk.cloned_into_targets_bytes > 0 {
        clean_tracked_targets_action()
    } else {
        Vec::new()
    };
    next.extend(snapshot_action(disk));
    next
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disk_view_on_empty_store_is_all_private() {
        let dir = tempfile::tempdir().unwrap();
        let view = disk_view(dir.path(), 0, 1024);
        assert_eq!(view.store_bytes, 0);
        assert_eq!(view.disk_private_bytes, 0);
        assert_eq!(view.cloned_into_targets_bytes, 0);
        assert_eq!(view.snapshot_retained_bytes, 0);
        assert_eq!(view.store_limit_bytes, 1024);
    }

    #[test]
    fn disk_view_scales_the_probe_split_to_the_indexed_store_size() {
        let view = disk_view_from_probe(
            1_000,
            2_000,
            ProbeTotals {
                apparent_bytes: 400,
                cloned_bytes: 100,
                snapshot_bytes: 40,
            },
        );
        assert_eq!(view.disk_private_bytes, 650);
        assert_eq!(view.cloned_into_targets_bytes, 250);
        assert_eq!(view.snapshot_retained_bytes, 100);
        assert_eq!(view.store_bytes, 1_000);
        assert_eq!(view.store_limit_bytes, 2_000);
    }

    #[test]
    fn blob_probe_ignores_files_outside_shard_directories() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = dir.path().join("blobs");
        std::fs::create_dir_all(&blobs).unwrap();
        std::fs::write(blobs.join("not-a-shard"), vec![0u8; 99]).unwrap();
        let shard = blobs.join("aa");
        std::fs::create_dir_all(&shard).unwrap();
        std::fs::write(shard.join("blob"), vec![0u8; 7]).unwrap();

        let totals = probe_store_blobs(dir.path());
        assert_eq!(totals.apparent_bytes, 7);
    }

    #[test]
    fn terminal_requirement_names_the_command_and_alternative() {
        assert!(require_tty(true, "config", "the alternative").is_ok());
        let error = require_tty(false, "config", "the alternative")
            .unwrap_err()
            .to_string();
        assert!(error.contains("kache config"), "{error}");
        assert!(error.contains("the alternative"), "{error}");
    }

    #[test]
    fn help_document_omits_hidden_arguments_commands_and_values() {
        use clap::builder::{PossibleValue, PossibleValuesParser};
        let command = clap::Command::new("fixture")
            .disable_help_flag(true)
            .disable_help_subcommand(true)
            .arg(
                clap::Arg::new("mode")
                    .long("mode")
                    .short('m')
                    .help("Choose a mode")
                    .required(true)
                    .value_parser(PossibleValuesParser::new([
                        PossibleValue::new("visible"),
                        PossibleValue::new("secret").hide(true),
                    ])),
            )
            .arg(clap::Arg::new("internal").long("internal").hide(true))
            .subcommand(clap::Command::new("shown").about("Visible command"))
            .subcommand(clap::Command::new("hidden").hide(true));
        let doc = serde_json::to_value(help_document(command)).unwrap();
        assert_eq!(doc["name"], "fixture");
        assert!(doc["usage"].as_str().unwrap().contains("--mode"));
        assert_eq!(doc["arguments"].as_array().unwrap().len(), 1);
        let arg = &doc["arguments"][0];
        assert_eq!(arg["name"], "mode");
        assert_eq!(arg["long"], "mode");
        assert_eq!(arg["short"], "m");
        assert_eq!(arg["required"], true);
        assert_eq!(arg["description"], "Choose a mode");
        assert_eq!(arg["values"], serde_json::json!(["visible"]));
        assert_eq!(doc["subcommands"].as_array().unwrap().len(), 1);
        assert_eq!(doc["subcommands"][0]["name"], "shown");
        assert_eq!(doc["subcommands"][0]["description"], "Visible command");
    }

    fn empty_view() -> DiskView {
        DiskView {
            store_bytes: 0,
            store_limit_bytes: 0,
            disk_private_bytes: 0,
            cloned_into_targets_bytes: 0,
            snapshot_retained_bytes: 0,
            cloned_coverage: ClonedCoverage::Unknown,
        }
    }

    #[test]
    fn next_for_clones_points_at_whatever_holds_the_blocks() {
        let empty = empty_view();
        assert!(next_for_clones(&empty).is_empty());
        let cloned = DiskView {
            cloned_into_targets_bytes: 1,
            ..empty_view()
        };
        let next = next_for_clones(&cloned);
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].argv[1], "clean");
        let snapshot = DiskView {
            snapshot_retained_bytes: 1,
            ..empty_view()
        };
        let next = next_for_clones(&snapshot);
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].argv[0], "tmutil");
        let both = DiskView {
            cloned_into_targets_bytes: 1,
            snapshot_retained_bytes: 1,
            ..empty_view()
        };
        assert_eq!(next_for_clones(&both).len(), 2);
    }

    #[test]
    fn next_after_gc_covers_each_retention_signal_boundary() {
        let empty = empty_view();
        assert!(next_after_gc(&empty, 0, 0, 0).is_empty());
        assert!(!next_after_gc(&empty, 1, 0, 0).is_empty());
        assert!(!next_after_gc(&empty, 0, 0, 1).is_empty());
        assert!(next_after_gc(&empty, 0, 1, 1).is_empty());

        let cloned = DiskView {
            cloned_into_targets_bytes: 1,
            ..empty
        };
        assert!(!next_after_gc(&cloned, 0, 0, 0).is_empty());

        let snapshot = DiskView {
            snapshot_retained_bytes: 1,
            ..empty
        };
        let next = next_after_gc(&snapshot, 0, 0, 0);
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].argv[0], "tmutil");
    }
}
