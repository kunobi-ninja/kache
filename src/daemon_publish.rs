//! Publication of compiler outputs handed off by a wrapper.
//!
//! A C compile through a build script runs nearly serially: cc-rs gets the
//! jobserver tokens rustc leaves over, and it cannot start the next file
//! until the wrapper for the previous one exits. Everything the wrapper does
//! after the object is written (staging, the store put, the durability
//! hand-off, the event log) is therefore wall-clock time on the build's
//! critical path. This module lets the wrapper hand that work to the daemon
//! and exit as soon as the daemon has accepted it.
//!
//! The contract:
//! - The wrapper snapshots its outputs into files it owns under the store's
//!   staging directory, drops its own key lock, and sends
//!   [`PublishCcRequest`]. The daemon takes the key lock itself before it
//!   answers, so a peer computing the same key sees the build as contended
//!   and waits for the commit, as it would for a wrapper that was still
//!   storing.
//! - The daemon answers `ok` only once the job is queued behind that lock.
//!   Any other answer, a full queue, an old daemon that does not know the
//!   request, or no answer within the wrapper's timeout, means the wrapper
//!   publishes on its own, as it did before this module existed.
//! - The daemon owns the handoff files from acceptance on and removes them
//!   once the put has copied them. A daemon that dies with jobs queued loses
//!   those stores, never a build: the compile has already produced its
//!   outputs and returned its exit code.
//! - The compiler never runs in the daemon.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::daemon::{Daemon, Response, UploadJob};
use crate::events::{BuildEvent, EventResult};
use crate::store::{BuildClaim, KeyLock, Store};

/// Jobs waiting for the worker. A build produces at most a few per second and
/// each takes a few milliseconds, so the queue only fills when the daemon is
/// starved; the wrapper then publishes itself.
pub(crate) const PUBLISH_QUEUE_CAPACITY: usize = 256;

/// How long a wrapper waits for the daemon to accept a hand-off. The daemon's
/// side of it is a file lock and a channel send; anything slower means the
/// daemon is busy and the wrapper is better off doing the work.
pub(crate) const PUBLISH_HANDOFF_TIMEOUT: Duration = Duration::from_millis(25);

/// Name of the directory under the store's staging area that holds files a
/// wrapper handed to the daemon.
pub(crate) const HANDOFF_DIR: &str = "handoff";

/// One output the wrapper snapshotted for the daemon.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct HandoffFile {
    /// Absolute path of the snapshot, under the store's handoff directory.
    pub path: String,
    /// Name the file takes in the entry.
    pub store_name: String,
}

/// A wrapper's request that the daemon store a cc compile it has finished.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct PublishCcRequest {
    /// Client binary mtime — lets the daemon detect when it's running stale code.
    #[serde(default)]
    pub client_epoch: u64,
    pub cache_key: String,
    pub crate_name: String,
    pub target: String,
    pub files: Vec<HandoffFile>,
    pub stdout: String,
    pub stderr: String,
    pub compile_time_ms: u64,
    /// Whether the entry may be uploaded to a writable remote.
    pub publishes_to_remote: bool,
    /// The build event the wrapper would have logged, with the store fields
    /// still empty. The daemon fills them in and writes it.
    pub event: BuildEvent,
    /// The read-set memo of a deferred compile, recorded here instead of in
    /// the wrapper: a hundred-row write transaction the next compile of the
    /// build script would otherwise wait for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memo: Option<CcMemoHandoff>,
}

/// A preprocess memo as the wrapper captured it: the fingerprints and
/// content hashes of everything the compile read, under the key of its
/// arguments.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct CcMemoHandoff {
    pub memo_key: String,
    pub preprocessed_hash: String,
    pub inputs: Vec<crate::cache_key::CcPreprocessMemoInput>,
}

/// A job the handler accepted: the request plus the key lock it holds.
pub(crate) struct PublishJob {
    request: PublishCcRequest,
    _lock: KeyLock,
}

/// The daemon's publish queue: a bounded channel drained by one blocking
/// worker with its own store connection.
pub(crate) struct PublishQueue {
    tx: std::sync::Mutex<Option<tokio::sync::mpsc::Sender<PublishJob>>>,
}

impl PublishQueue {
    pub(crate) fn new() -> Self {
        Self {
            tx: std::sync::Mutex::new(None),
        }
    }

    pub(crate) fn set_sender(&self, tx: tokio::sync::mpsc::Sender<PublishJob>) {
        if let Ok(mut slot) = self.tx.lock() {
            *slot = Some(tx);
        }
    }

    fn sender(&self) -> Option<tokio::sync::mpsc::Sender<PublishJob>> {
        self.tx.lock().ok().and_then(|slot| slot.clone())
    }

    /// Drop the sender so the worker drains what is queued and exits.
    pub(crate) fn close(&self) {
        if let Ok(mut slot) = self.tx.lock() {
            slot.take();
        }
    }
}

/// Where a wrapper snapshots outputs it hands to the daemon.
pub(crate) fn handoff_dir(config: &Config) -> PathBuf {
    config.store_dir().join("staging").join(HANDOFF_DIR)
}

/// Whether `path` is one the daemon may ingest: an absolute path under this
/// store's handoff directory, with no `..` to walk out of it.
fn handoff_path_is_owned(config: &Config, path: &Path) -> bool {
    path.is_absolute()
        && path.starts_with(handoff_dir(config))
        && !path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
}

/// Why a hand-off was refused, in the response's error text. The wrapper only
/// needs to know it was refused; the text is for `kache daemon` logs.
fn refused(reason: &str) -> Response {
    Response::err(format!("publish refused: {reason}"))
}

impl Daemon {
    /// Accept a wrapper's hand-off if the key can be claimed and the queue has
    /// room. Claiming happens before the answer: from the wrapper's return
    /// onwards a peer must see the key as contended, not absent.
    pub(crate) async fn handle_publish_cc(self: &Arc<Self>, request: PublishCcRequest) -> Response {
        if !crate::cache_key::is_valid_cache_key(&request.cache_key) {
            return refused("invalid cache key");
        }
        if request.files.is_empty() {
            return refused("no files");
        }
        let config = self.config().clone();
        if let Some(file) = request
            .files
            .iter()
            .find(|file| !handoff_path_is_owned(&config, Path::new(&file.path)))
        {
            return refused(&format!("{} is outside the handoff directory", file.path));
        }
        let Some(tx) = self.publish_queue().sender() else {
            return refused("queue closed");
        };
        if tx.capacity() == 0 {
            return refused("queue full");
        }
        let key = request.cache_key.clone();
        let daemon = Arc::clone(self);
        let claim =
            tokio::task::spawn_blocking(move || daemon.with_store(|store| store.claim_build(&key)))
                .await;
        let lock = match claim {
            Ok(Ok(BuildClaim::Acquired(lock))) => lock,
            Ok(Ok(BuildClaim::Committed(_))) => return refused("already committed"),
            Ok(Ok(BuildClaim::Contended)) => return refused("key contended"),
            Ok(Err(error)) => return refused(&format!("claim failed: {error:#}")),
            Err(error) => return refused(&format!("claim task failed: {error}")),
        };
        // Own separate links before acknowledging. A client may time out
        // after acceptance and remove every path it sent us.
        let claimed = tokio::task::spawn_blocking(move || {
            let files = request
                .files
                .iter()
                .map(|file| (PathBuf::from(&file.path), file.store_name.clone()))
                .collect::<Vec<_>>();
            let owned = snapshot_for_handoff(&config, &files)?;
            remove_handoff_files(&request.files);
            let mut request = request;
            request.files = owned;
            Ok::<_, anyhow::Error>(PublishJob {
                request,
                _lock: lock,
            })
        })
        .await;
        let job = match claimed {
            Ok(Ok(job)) => job,
            Ok(Err(error)) => return refused(&format!("snapshot failed: {error:#}")),
            Err(error) => return refused(&format!("snapshot task failed: {error}")),
        };
        match tx.try_send(job) {
            Ok(()) => Response::ok(),
            Err(tokio::sync::mpsc::error::TrySendError::Full(job)) => {
                discard_handoff_files(&job.request);
                refused("queue full")
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(job)) => {
                discard_handoff_files(&job.request);
                refused("queue closed")
            }
        }
    }
}

/// Run the publish worker until the queue closes and drains. Blocking: it is
/// meant for `spawn_blocking`, and it opens its own store connection so the
/// daemon's shared store mutex never waits on a put.
pub(crate) fn run_publish_worker(
    daemon: Arc<Daemon>,
    mut rx: tokio::sync::mpsc::Receiver<PublishJob>,
) {
    let config = daemon.config().clone();
    let store = match Store::open(&config) {
        Ok(store) => store,
        Err(error) => {
            tracing::warn!("publish worker could not open the store: {error:#}; hand-offs refused");
            daemon.publish_queue().close();
            // Drain so accepted jobs release their locks and files.
            while let Some(job) = rx.blocking_recv() {
                discard_handoff_files(&job.request);
            }
            return;
        }
    };
    while let Some(job) = rx.blocking_recv() {
        publish_one(&daemon, &config, &store, job);
    }
}

/// Store one handed-off compile, log its event, and release what the job
/// held. Every failure is recorded on the event; none of them can reach a
/// build.
fn publish_one(daemon: &Arc<Daemon>, config: &Config, store: &Store, job: PublishJob) {
    let PublishJob { request, _lock } = job;
    let store_started = std::time::Instant::now();
    let files: Vec<(PathBuf, String)> = request
        .files
        .iter()
        .map(|file| (PathBuf::from(&file.path), file.store_name.clone()))
        .collect();
    let bytes_before = StoreBytes::snapshot();
    let mut event = request.event.clone();
    let put = store.put_with_compile_time_independent(
        &request.cache_key,
        &request.crate_name,
        &[],
        &[],
        &request.target,
        "",
        &files,
        &request.stdout,
        &request.stderr,
        request.compile_time_ms,
    );
    match put {
        Ok(put) => {
            event.result = if put.is_full_dup() {
                EventResult::Dup
            } else {
                EventResult::Miss
            };
            event.store_output_blobs = put.output_blobs;
            event.store_duplicate_blobs = put.duplicate_blobs;
            event.store_new_blobs = put.new_blobs;
            bytes_before.charge(&mut event);
            if let Some(memo) = &request.memo
                && let Err(error) = store.file_hash_cache().put_cc_preprocess_memo_inputs(
                    &memo.memo_key,
                    &memo.preprocessed_hash,
                    &memo.inputs,
                )
            {
                tracing::debug!("daemon could not record the cc memo: {error}");
            }
            crate::wrapper::maybe_spawn_auto_gc(config, store);
            if request.publishes_to_remote && config.remote.is_some() {
                enqueue_upload(daemon, config, &request);
            }
        }
        Err(error) => {
            event.store_error = crate::wrapper::store_error_for_event(&error);
            tracing::warn!(
                "daemon could not store the cc entry for {}: {}",
                request.crate_name,
                event.store_error
            );
        }
    }
    // The wrapper measured everything up to the hand-off; add what the put
    // cost here, so the phase totals still add up to the wrapper's overhead.
    event.store_ms = event
        .store_ms
        .saturating_add(store_started.elapsed().as_millis() as u64);
    event.store_handed_off = true;
    // The put has its own copy (staged then published); the snapshots are
    // done. Drop the lock only after the files are gone: a peer that takes
    // the key next must find a committed entry, and nothing of ours left.
    discard_handoff_files(&request);
    crate::wrapper::write_event(config, &event);
    drop(_lock);
}

fn discard_handoff_files(request: &PublishCcRequest) {
    remove_handoff_files(&request.files);
}

/// Queue the entry for upload the way the wrapper would have: a durable
/// intent first, then the daemon's own upload pipeline.
fn enqueue_upload(daemon: &Arc<Daemon>, config: &Config, request: &PublishCcRequest) {
    if config.remote_readonly {
        return;
    }
    let job = UploadJob {
        key: request.cache_key.clone(),
        entry_dir: config
            .store_dir()
            .join(&request.cache_key)
            .to_string_lossy()
            .into_owned(),
        crate_name: request.crate_name.clone(),
        client_epoch: crate::daemon::build_epoch(),
    };
    let daemon = Arc::clone(daemon);
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        let response = handle.block_on(daemon.handle_upload(&job));
        if !response.ok {
            tracing::debug!(
                "upload of {} not queued: {}",
                request.crate_name,
                response.error.as_deref().unwrap_or("unknown")
            );
        }
    }
}

/// The process-global store byte counters, read before a put so the event
/// can carry this put's share. One worker runs puts one at a time, so the
/// difference is exactly this job's.
struct StoreBytes {
    reflinked: u64,
    hardlinked: u64,
    copied: u64,
    cross_device: u64,
    permission: u64,
    ineligible: u64,
    other: u64,
}

impl StoreBytes {
    fn snapshot() -> Self {
        Self {
            reflinked: crate::opcounts::store_reflinked_bytes(),
            hardlinked: crate::opcounts::store_hardlinked_bytes(),
            copied: crate::opcounts::store_copied_bytes(),
            cross_device: crate::opcounts::store_copy_cross_device_bytes(),
            permission: crate::opcounts::store_copy_permission_bytes(),
            ineligible: crate::opcounts::store_copy_ineligible_bytes(),
            other: crate::opcounts::store_copy_other_bytes(),
        }
    }

    fn charge(&self, event: &mut BuildEvent) {
        let now = Self::snapshot();
        event.store_reflinked_bytes = now.reflinked.saturating_sub(self.reflinked);
        event.store_hardlinked_bytes = now.hardlinked.saturating_sub(self.hardlinked);
        event.store_copied_bytes = now.copied.saturating_sub(self.copied);
        event.store_copy_cross_device_bytes = now.cross_device.saturating_sub(self.cross_device);
        event.store_copy_permission_bytes = now.permission.saturating_sub(self.permission);
        event.store_copy_ineligible_bytes = now.ineligible.saturating_sub(self.ineligible);
        event.store_copy_other_bytes = now.other.saturating_sub(self.other);
    }
}

// ── Wrapper side ─────────────────────────────────────────────────

/// Outcome of offering a compile to the daemon.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Handoff {
    /// The daemon holds the key and will publish; the wrapper is done.
    Accepted,
    /// The wrapper publishes itself; `reason` is for the debug log.
    Declined(String),
}

/// Offer `request` to the daemon. Bounded by [`PUBLISH_HANDOFF_TIMEOUT`]:
/// a daemon that is slow to answer is one the wrapper should not wait for.
pub(crate) fn hand_off_cc_publish(config: &Config, request: &PublishCcRequest) -> Handoff {
    let _trace = crate::phase_trace::phase("store_handoff");
    let socket = config.socket_path();
    #[cfg(unix)]
    if !socket.exists() {
        return Handoff::Declined("no daemon socket".to_string());
    }
    match send_handoff(&socket, request, PUBLISH_HANDOFF_TIMEOUT) {
        Ok(line) => match serde_json::from_str::<Response>(&line) {
            Ok(response) if response.ok => Handoff::Accepted,
            Ok(response) => Handoff::Declined(
                response
                    .error
                    .unwrap_or_else(|| "daemon declined".to_string()),
            ),
            Err(error) => Handoff::Declined(format!("unreadable daemon reply: {error}")),
        },
        Err(error) => Handoff::Declined(format!("{error:#}")),
    }
}

/// Bound connection, writes, and reads by the same deadline. The general
/// daemon client only bounds reads and can spend seconds sending a memo to
/// a stalled daemon.
#[cfg(unix)]
fn send_handoff(socket: &Path, request: &PublishCcRequest, budget: Duration) -> Result<String> {
    use interprocess::local_socket::{ConnectOptions, traits::Stream as _};
    use std::io::{Read, Write};
    let deadline = std::time::Instant::now() + budget;
    let remaining = || -> std::io::Result<Duration> {
        deadline
            .checked_duration_since(std::time::Instant::now())
            .filter(|duration| !duration.is_zero())
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::TimedOut, "cc handoff deadline"))
    };
    let mut line = serde_json::to_vec(&crate::daemon::Request::PublishCc(Box::new(
        request.clone(),
    )))?;
    line.push(b'\n');
    let mut stream = ConnectOptions::new()
        .name(crate::transport::socket_name(socket)?)
        .wait_mode(interprocess::ConnectWaitMode::Timeout(remaining()?))
        .connect_sync()?;
    let mut pending = line.as_slice();
    while !pending.is_empty() {
        stream.set_send_timeout(Some(remaining()?))?;
        let written = stream.write(pending)?;
        anyhow::ensure!(written != 0, "daemon closed during handoff write");
        pending = &pending[written..];
    }
    let mut response = Vec::new();
    loop {
        stream.set_recv_timeout(Some(remaining()?))?;
        let mut buffer = [0; 1024];
        let read = stream.read(&mut buffer)?;
        anyhow::ensure!(read != 0, "daemon closed before handoff reply");
        response.extend_from_slice(&buffer[..read]);
        anyhow::ensure!(response.len() <= 8192, "oversized handoff reply");
        if response.contains(&b'\n') {
            return Ok(String::from_utf8(response)?);
        }
    }
}

#[cfg(windows)]
fn send_handoff(socket: &Path, request: &PublishCcRequest, budget: Duration) -> Result<String> {
    crate::daemon::send_request_with_timeout(
        socket,
        &crate::daemon::Request::PublishCc(Box::new(request.clone())),
        budget,
    )
}

/// Snapshot `files` (each `(source, store_name)`) into the handoff directory
/// so they outlive the wrapper. Returns the daemon-visible list; on any
/// failure the snapshots made so far are removed and the error is returned,
/// leaving the wrapper to publish from its own staging as before.
pub(crate) fn snapshot_for_handoff(
    config: &Config,
    files: &[(PathBuf, String)],
) -> Result<Vec<HandoffFile>> {
    let _trace = crate::phase_trace::phase("handoff_snapshot");
    let dir = handoff_dir(config);
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("creating handoff directory {}", dir.display()))?;
    // Each request has its own directory. PID-based names can collide with
    // a previous request still queued in the daemon, or with a reused PID.
    let batch = tempfile::Builder::new().prefix("cc-").tempdir_in(&dir)?;
    let mut snapshots = Vec::with_capacity(files.len());
    for (index, (source, store_name)) in files.iter().enumerate() {
        let target = batch
            .path()
            .join(format!("{index}-{}", handoff_file_name(store_name)));
        // Inputs are private staging files, never compiler-owned outputs.
        if std::fs::hard_link(source, &target).is_err() {
            std::fs::copy(source, &target)
                .with_context(|| format!("snapshotting {} for the daemon", source.display()))?;
        }
        snapshots.push(HandoffFile {
            path: target.to_string_lossy().into_owned(),
            store_name: store_name.clone(),
        });
    }
    let _ = batch.keep();
    Ok(snapshots)
}

/// A store name flattened to one path component.
fn handoff_file_name(store_name: &str) -> String {
    store_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

pub(crate) fn remove_handoff_files(files: &[HandoffFile]) {
    for file in files {
        let _ = std::fs::remove_file(&file.path);
        if let Some(parent) = Path::new(&file.path).parent() {
            // Only empty request directories are removed; other snapshots
            // in the batch keep the directory alive until their turn.
            let _ = std::fs::remove_dir(parent);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handoff_paths_must_sit_under_the_store_handoff_dir() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(dir.path().join("cache"));
        let inside = handoff_dir(&config).join("1-0-foo.o");
        assert!(handoff_path_is_owned(&config, &inside));
        assert!(!handoff_path_is_owned(&config, Path::new("foo.o")));
        assert!(!handoff_path_is_owned(
            &config,
            &dir.path().join("elsewhere.o")
        ));
        let escaping = handoff_dir(&config).join("..").join("..").join("index.db");
        assert!(!handoff_path_is_owned(&config, &escaping));
    }

    #[test]
    fn snapshots_land_in_the_handoff_dir_and_are_removed_together_on_failure() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(dir.path().join("cache"));
        let good = dir.path().join("a.o");
        std::fs::write(&good, b"object").unwrap();
        let snapshots =
            snapshot_for_handoff(&config, &[(good.clone(), "a.o".to_string())]).unwrap();
        assert_eq!(snapshots.len(), 1);
        let path = Path::new(&snapshots[0].path);
        assert!(handoff_path_is_owned(&config, path));
        assert_eq!(std::fs::read(path).unwrap(), b"object");
        assert!(
            path.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .ends_with("0-a.o")
        );

        let missing = dir.path().join("missing.o");
        let error = snapshot_for_handoff(
            &config,
            &[
                (good.clone(), "a.o".to_string()),
                (missing, "sub/dir/b.o".to_string()),
            ],
        )
        .unwrap_err();
        assert!(error.to_string().contains("missing.o"), "{error:#}");
        let leftovers: Vec<_> = std::fs::read_dir(handoff_dir(&config))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .filter(|name| name != path.parent().unwrap().file_name().unwrap())
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
        assert_eq!(std::fs::read(path).unwrap(), b"object");
        remove_handoff_files(&snapshots);
        assert!(!path.exists());
    }

    #[test]
    fn store_names_flatten_to_one_component() {
        assert_eq!(handoff_file_name("foo.o"), "foo.o");
        assert_eq!(handoff_file_name("sub/dir/foo.d"), "sub_dir_foo.d");
        assert_eq!(handoff_file_name("../x"), ".._x");
    }

    fn handoff_request(config: &Config, key: &str, dir: &Path) -> PublishCcRequest {
        let object = dir.join("a.o");
        std::fs::write(&object, b"object bytes").unwrap();
        let files = snapshot_for_handoff(config, &[(object, "a.o".to_string())]).unwrap();
        PublishCcRequest {
            client_epoch: 0,
            cache_key: key.to_string(),
            crate_name: "a.c".to_string(),
            target: "x86_64".to_string(),
            files,
            stdout: String::new(),
            stderr: "warning: w\n".to_string(),
            compile_time_ms: 7,
            publishes_to_remote: false,
            event: crate::events::BuildEvent::new_for_test("a.c", EventResult::Miss),
            memo: None,
        }
    }

    fn key(label: &str) -> String {
        blake3::hash(label.as_bytes()).to_hex().to_string()
    }

    /// The worker stores what the handler accepted: the entry is committed
    /// under the key, the event carries the daemon's store fields, and the
    /// snapshots are gone.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_accepted_handoff_is_stored_logged_and_cleaned_up() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(dir.path().join("cache"));
        let daemon = Arc::new(Daemon::new(config.clone()));
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        daemon.publish_queue().set_sender(tx);
        let worker = {
            let d = Arc::clone(&daemon);
            tokio::task::spawn_blocking(move || run_publish_worker(d, rx))
        };
        let mut request = handoff_request(&config, &key("accepted"), dir.path());
        let header = dir.path().join("a.h");
        std::fs::write(&header, b"#define A 1\n").unwrap();
        let memo = CcMemoHandoff {
            memo_key: "m".repeat(64),
            preprocessed_hash: "p".repeat(64),
            inputs: vec![crate::cache_key::CcPreprocessMemoInput {
                name: "a.h".to_string(),
                fingerprint: crate::cache_key::FileFingerprint::from_path(&header).unwrap(),
                content: "c".repeat(64),
                mapped: "d".repeat(64),
            }],
        };
        request.memo = Some(memo.clone());
        let snapshot = PathBuf::from(&request.files[0].path);
        let response = daemon.handle_publish_cc(request.clone()).await;
        assert!(response.ok, "{:?}", response.error);
        daemon.publish_queue().close();
        worker.await.unwrap();

        let store = Store::open(&config).unwrap();
        let meta = store
            .get(&request.cache_key)
            .unwrap()
            .expect("entry committed");
        assert_eq!(meta.files.len(), 1);
        assert_eq!(meta.files[0].name, "a.o");
        assert_eq!(meta.stderr, "warning: w\n");
        let recorded = store
            .file_hash_cache()
            .get_cc_preprocess_memo(&memo.memo_key)
            .unwrap()
            .expect("the memo is recorded with the entry");
        assert_eq!(recorded.preprocessed_hash, memo.preprocessed_hash);
        assert_eq!(recorded.inputs, memo.inputs);
        assert!(
            !snapshot.exists(),
            "the daemon removes the snapshot after the put"
        );
        let events = std::fs::read_to_string(config.event_log_path()).unwrap();
        let event: BuildEvent = serde_json::from_str(events.lines().last().unwrap()).unwrap();
        assert!(event.store_handed_off);
        assert_eq!(event.result, EventResult::Miss);
        assert_eq!(event.store_new_blobs, 1);
        assert!(event.store_error.is_empty());
    }

    /// Refusals never take the key: a full queue, a foreign path, a key a
    /// peer holds, and an invalid key all leave the wrapper to publish.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refusals_leave_the_key_free_and_the_snapshots_alone() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(dir.path().join("cache"));
        let daemon = Arc::new(Daemon::new(config.clone()));
        let store = Store::open(&config).unwrap();

        // No sender yet: closed.
        let request = handoff_request(&config, &key("closed"), dir.path());
        let response = daemon.handle_publish_cc(request.clone()).await;
        assert!(!response.ok);
        assert!(response.error.unwrap().contains("queue closed"));
        assert!(Path::new(&request.files[0].path).exists());
        assert!(matches!(
            store.claim_build(&request.cache_key).unwrap(),
            BuildClaim::Acquired(_)
        ));

        // A queue of one: the second hand-off finds it full.
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        daemon.publish_queue().set_sender(tx);
        let first = handoff_request(&config, &key("first"), dir.path());
        assert!(daemon.handle_publish_cc(first.clone()).await.ok);
        let second = handoff_request(&config, &key("second"), dir.path());
        let response = daemon.handle_publish_cc(second.clone()).await;
        assert!(!response.ok);
        assert!(response.error.unwrap().contains("queue full"));
        assert!(matches!(
            store.claim_build(&second.cache_key).unwrap(),
            BuildClaim::Acquired(_)
        ));
        // The accepted one holds its key until the worker is done with it.
        assert!(matches!(
            store.claim_build(&first.cache_key).unwrap(),
            BuildClaim::Contended
        ));
        let job = rx.recv().await.unwrap();
        drop(job);
        assert!(matches!(
            store.claim_build(&first.cache_key).unwrap(),
            BuildClaim::Acquired(_)
        ));

        // A key a peer holds.
        let held = handoff_request(&config, &key("held"), dir.path());
        let _peer = match store.claim_build(&held.cache_key).unwrap() {
            BuildClaim::Acquired(lock) => lock,
            _ => panic!("fresh key"),
        };
        let response = daemon.handle_publish_cc(held).await;
        assert!(!response.ok);
        assert!(response.error.unwrap().contains("contended"));

        // A file outside the handoff directory.
        let mut foreign = handoff_request(&config, &key("foreign"), dir.path());
        foreign.files[0].path = dir.path().join("a.o").to_string_lossy().into_owned();
        let response = daemon.handle_publish_cc(foreign).await;
        assert!(!response.ok);
        assert!(response.error.unwrap().contains("outside"));

        let mut invalid = handoff_request(&config, &key("invalid"), dir.path());
        invalid.cache_key = "nope".to_string();
        assert!(!daemon.handle_publish_cc(invalid).await.ok);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn client_cleanup_after_a_lost_reply_cannot_remove_queued_outputs() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(dir.path().join("cache"));
        let daemon = Arc::new(Daemon::new(config.clone()));
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        daemon.publish_queue().set_sender(tx);
        let request = handoff_request(&config, &key("lost-reply"), dir.path());
        assert!(daemon.handle_publish_cc(request.clone()).await.ok);
        // The reply is lost. The client times out, removes its snapshots,
        // and a subsequent invocation stages different bytes under its PID.
        remove_handoff_files(&request.files);
        let replacement = dir.path().join("replacement.o");
        std::fs::write(&replacement, b"different object").unwrap();
        let later = snapshot_for_handoff(&config, &[(replacement, "a.o".into())]).unwrap();
        let job = rx.recv().await.unwrap();
        assert_ne!(job.request.files, request.files);
        assert_eq!(
            std::fs::read(&job.request.files[0].path).unwrap(),
            b"object bytes"
        );
        let store = Store::open(&config).unwrap();
        publish_one(&daemon, &config, &store, job);
        assert!(store.get(&request.cache_key).unwrap().is_some());
        assert_eq!(std::fs::read(&later[0].path).unwrap(), b"different object");
        remove_handoff_files(&later);
        assert_eq!(std::fs::read_dir(handoff_dir(&config)).unwrap().count(), 0);
    }

    #[cfg(unix)]
    #[test]
    fn handoff_budget_covers_a_daemon_that_never_reads_the_request() {
        use std::os::unix::net::UnixListener;
        let dir = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(dir.path().join("cache"));
        let socket = dir.path().join("stalled.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            done_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        });
        let mut request = handoff_request(&config, &key("stalled"), dir.path());
        request.stderr = "x".repeat(1024 * 1024);
        let start = std::time::Instant::now();
        assert!(send_handoff(&socket, &request, Duration::from_millis(500)).is_err());
        let elapsed = start.elapsed();
        done_tx.send(()).unwrap();
        server.join().unwrap();
        assert!(elapsed < Duration::from_secs(2), "handoff took {elapsed:?}");
    }

    #[cfg(unix)]
    #[test]
    fn handoff_waits_for_a_complete_reply_and_rejects_eof() {
        use std::io::{BufRead, Write};
        use std::os::unix::net::UnixListener;
        let dir = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(dir.path().join("cache"));
        for reply in [b"{\"ok\":true}\n".as_slice(), b""] {
            let socket = dir.path().join("reply.sock");
            let listener = UnixListener::bind(&socket).unwrap();
            let server = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = String::new();
                std::io::BufReader::new(&stream)
                    .read_line(&mut request)
                    .unwrap();
                assert!(request.contains("publish_cc"));
                stream.write_all(reply).unwrap();
            });
            let request = handoff_request(&config, &key("reply"), dir.path());
            let result = send_handoff(&socket, &request, Duration::from_secs(1));
            if reply.is_empty() {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("before handoff reply")
                );
            } else {
                assert_eq!(result.unwrap(), "{\"ok\":true}\n");
            }
            server.join().unwrap();
            std::fs::remove_file(socket).unwrap();
        }
    }

    #[test]
    fn handing_off_without_a_socket_declines_at_once() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(dir.path().join("cache"));
        let request = PublishCcRequest {
            client_epoch: 0,
            cache_key: "k".repeat(64),
            crate_name: "a.c".to_string(),
            target: String::new(),
            files: Vec::new(),
            stdout: String::new(),
            stderr: String::new(),
            compile_time_ms: 1,
            publishes_to_remote: false,
            event: crate::events::BuildEvent::new_for_test("a.c", crate::events::EventResult::Miss),
            memo: None,
        };
        let started = std::time::Instant::now();
        let outcome = hand_off_cc_publish(&config, &request);
        assert!(matches!(outcome, Handoff::Declined(reason) if reason.contains("socket")));
        assert!(started.elapsed() < Duration::from_millis(100));
    }
}
