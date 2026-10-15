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
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<Failure<'a>>,
    #[serde(flatten)]
    body: T,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    next: Vec<NextAction>,
}

#[derive(Debug, Serialize)]
struct Failure<'a> {
    code: &'a str,
    message: String,
    causes: Vec<String>,
}

/// Write one JSON document to stdout.
pub fn emit<T: Serialize>(command: &str, body: T, next: Vec<NextAction>) -> Result<()> {
    emit_outcome(command, None, body, next)
}

/// [`emit`] for a command that may have done only part of its work. With
/// `failure`, `success` is false and `error` carries it as `command_failed`;
/// the body says what was done.
pub fn emit_outcome<T: Serialize>(
    command: &str,
    failure: Option<&str>,
    body: T,
    next: Vec<NextAction>,
) -> Result<()> {
    write_outcome(&mut std::io::stdout().lock(), command, failure, body, next)
}

fn write_outcome<T: Serialize>(
    out: &mut impl std::io::Write,
    command: &str,
    failure: Option<&str>,
    body: T,
    next: Vec<NextAction>,
) -> Result<()> {
    let doc = JsonDoc {
        schema_version: SCHEMA_VERSION,
        command,
        success: failure.is_none(),
        error: failure.map(|message| Failure {
            code: "command_failed",
            message: message.to_string(),
            causes: Vec::new(),
        }),
        body,
        next,
    };
    still_failed(write_document(out, &doc), failure)
}

/// The result of writing a command's document, when `failure` says the
/// command failed. A closed stdout ends a command quietly only when it
/// succeeded; a failed one still fails, with its own message.
pub(crate) fn still_failed(written: Result<()>, failure: Option<&str>) -> Result<()> {
    match (written, failure) {
        (Err(error), Some(message)) if failure_exit(&error).is_none() => {
            Err(Reported(message.to_string()).into())
        }
        (written, _) => written,
    }
}

/// Failures use the same stdout document contract as successful commands.
pub(crate) fn emit_error(command: &str, code: &str, error: &anyhow::Error) -> Result<()> {
    #[derive(Serialize)]
    struct NoBody {}
    let doc = JsonDoc {
        schema_version: SCHEMA_VERSION,
        command,
        success: false,
        error: Some(Failure {
            code,
            message: error.to_string(),
            causes: error.chain().skip(1).map(ToString::to_string).collect(),
        }),
        body: NoBody {},
        next: Vec::new(),
    };
    write_document(&mut std::io::stdout().lock(), &doc)
}

/// Pretty JSON and a newline, as one stdout document.
fn write_document(out: &mut impl std::io::Write, doc: &impl Serialize) -> Result<()> {
    serde_json::to_writer_pretty(&mut *out, doc)
        .map_err(std::io::Error::from)
        .and_then(|()| writeln!(out))
        .and_then(|()| out.flush())
        .map_err(stdout_error)
}

/// The reader of stdout went away, as with `kache --json --help | head`.
/// Nothing more can be shown, and the reader already has what it wanted.
#[derive(Debug)]
pub(crate) struct StdoutClosed;

impl std::fmt::Display for StdoutClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("stdout was closed")
    }
}

impl std::error::Error for StdoutClosed {}

/// A failed write to stdout, with a closed pipe told apart from other errors.
pub(crate) fn stdout_error(error: std::io::Error) -> anyhow::Error {
    if error.kind() == std::io::ErrorKind::BrokenPipe {
        anyhow::Error::new(StdoutClosed)
    } else {
        anyhow::Error::new(error).context("writing to stdout")
    }
}

/// An argument error found after clap parsed the command line: a value clap
/// cannot check, or a combination of flags. It exits 2 with
/// `invalid_arguments`, like the errors clap reports itself.
#[derive(Debug)]
pub(crate) struct InvalidArguments(pub String);

impl std::fmt::Display for InvalidArguments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for InvalidArguments {}

pub(crate) fn invalid_arguments(message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(InvalidArguments(message.into()))
}

/// How a failed command ends: its JSON error code and exit status. `None`
/// when stdout was closed, which ends the command quietly with status 0.
pub(crate) fn failure_exit(error: &anyhow::Error) -> Option<(&'static str, i32)> {
    if error.chain().any(|cause| cause.is::<StdoutClosed>()) {
        return None;
    }
    if error.chain().any(|cause| cause.is::<InvalidArguments>()) {
        return Some(("invalid_arguments", 2));
    }
    Some(("command_failed", 1))
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

/// A failure the command already described in its JSON document. The
/// process still exits non-zero, without printing a second document.
#[derive(Debug)]
pub(crate) struct Reported(pub String);

impl std::fmt::Display for Reported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Reported {}

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

    /// A writer that fails every write with one error kind.
    struct Failing(std::io::ErrorKind);

    impl std::io::Write for Failing {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(self.0.into())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Err(self.0.into())
        }
    }

    #[test]
    fn a_document_is_pretty_json_and_one_newline() {
        let mut out = Vec::new();
        write_document(&mut out, &serde_json::json!({ "a": 1 })).unwrap();
        assert_eq!(String::from_utf8(out).unwrap(), "{\n  \"a\": 1\n}\n");
    }

    /// `kache clean --json --yes | head` with a failed removal still exits 1.
    #[test]
    fn a_closed_stdout_does_not_hide_a_failed_command() {
        let closed = || Err(stdout_error(std::io::ErrorKind::BrokenPipe.into()));
        let failed =
            still_failed(closed(), Some("could not remove 1 target directory")).unwrap_err();
        assert!(failed.is::<Reported>(), "{failed:#}");
        assert_eq!(failed.to_string(), "could not remove 1 target directory");
        assert_eq!(failure_exit(&failed), Some(("command_failed", 1)));

        assert_eq!(
            failure_exit(&still_failed(closed(), None).unwrap_err()),
            None
        );

        let pipe = || Failing(std::io::ErrorKind::BrokenPipe);
        let failed = write_outcome(
            &mut pipe(),
            "clean",
            Some("could not remove"),
            serde_json::json!({}),
            Vec::new(),
        )
        .unwrap_err();
        assert!(failed.is::<Reported>(), "{failed:#}");
        let succeeded = write_outcome(
            &mut pipe(),
            "clean",
            None,
            serde_json::json!({}),
            Vec::new(),
        )
        .unwrap_err();
        assert_eq!(failure_exit(&succeeded), None);
        let mut out = Vec::new();
        write_outcome(
            &mut out,
            "clean",
            Some("could not remove"),
            serde_json::json!({}),
            Vec::new(),
        )
        .unwrap();
        let doc: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(doc["success"], false);
        assert_eq!(doc["error"]["code"], "command_failed");
        assert!(still_failed(Ok(()), Some("failed")).is_ok());
        let full = Err(stdout_error(std::io::ErrorKind::StorageFull.into()));
        let full = still_failed(full, Some("failed")).unwrap_err();
        assert!(!full.is::<Reported>(), "{full:#}");
    }

    #[test]
    fn a_closed_stdout_ends_quietly_and_other_write_errors_fail() {
        let closed = write_document(
            &mut Failing(std::io::ErrorKind::BrokenPipe),
            &serde_json::json!({}),
        )
        .unwrap_err();
        assert_eq!(failure_exit(&closed), None);
        assert_eq!(failure_exit(&closed.context("emitting")), None);

        let full = write_document(
            &mut Failing(std::io::ErrorKind::StorageFull),
            &serde_json::json!({}),
        )
        .unwrap_err();
        assert_eq!(failure_exit(&full), Some(("command_failed", 1)));
        assert!(
            format!("{full:#}").contains("writing to stdout"),
            "{full:#}"
        );
    }

    #[test]
    fn argument_errors_exit_two_and_others_one() {
        let invalid = invalid_arguments("bad flag");
        assert_eq!(invalid.to_string(), "bad flag");
        assert_eq!(failure_exit(&invalid), Some(("invalid_arguments", 2)));
        assert_eq!(
            failure_exit(&invalid.context("parsing")),
            Some(("invalid_arguments", 2))
        );
        assert_eq!(
            failure_exit(&anyhow::anyhow!("disk on fire")),
            Some(("command_failed", 1))
        );
        assert_eq!(StdoutClosed.to_string(), "stdout was closed");
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
