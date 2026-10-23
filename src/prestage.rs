//! Executables the daemon copies into a target directory ahead of their
//! restore.
//!
//! A restored executable is a private copy: a post-build `strip` rewrites a
//! file with several links in place, so sharing the store blob's inode would
//! let it corrupt the blob. Copying a large binary is, though, the last thing
//! a warm build waits for: hk's 186 MB debug binary takes 0.2 s after every
//! other unit has finished. The daemon therefore makes that copy while the
//! rest of the build runs.
//!
//! - When a large executable is stored or restored, the wrapper records its
//!   destination and blob under the cache's `prestage` directory, one
//!   directory per target directory and one record per artifact. A later
//!   build of the same artifact under a new metadata hash replaces the
//!   record.
//! - The first wrapper of a later build of that target directory hints the
//!   daemon, and no wrapper again for [`HINT_INTERVAL`]. For each record
//!   whose destination is missing, the daemon materializes the blob next to
//!   it the way a restore would, checks the bytes against the blob's hash,
//!   then writes the copy's fingerprint.
//! - A target directory with no records of its own, such as a new checkout's,
//!   borrows those of the target directory of the same project that recorded
//!   last, at the same paths under its own root. The project is the
//!   `Cargo.toml` beside the target directory, so two checkouts of one
//!   repository match and unrelated projects never do. A guess that turns out
//!   wrong costs a copy in the daemon and a removal at the restore.
//! - The restore renames a staged copy into place when the copy still has the
//!   fingerprint the daemon wrote. Anything else is removed and the blob is
//!   copied as before.
//! - A successful wrapper retires the unused prediction for its artifact,
//!   including a prediction under an older Cargo metadata hash. Publication
//!   and retirement share a short per-artifact lock; copying and hashing do
//!   not hold it. A completed output also stops an already queued daemon pass
//!   from publishing that prediction after the wrapper returns. A new hint
//!   starts another generation, so other metadata variants can still stage.
//!
//! What lands at the destination is the blob's bytes in a file of its own,
//! exactly what the restore's copy would have produced. The fingerprint is
//! the check the file-hash memo already relies on (kunobi-ninja/kache#540):
//! any write to the staged file changes its size, times or inode.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use kache_store::link;

use crate::cache_key::FileFingerprint;

/// Executables smaller than this are cheaper to copy than to stage.
pub(crate) const MIN_BYTES: u64 = 16 << 20;

/// How often a target directory's builds hint the daemon.
const HINT_INTERVAL: Duration = Duration::from_secs(30);

/// A staged copy nobody took within this long is removed by the next staging
/// pass in its directory.
const STALE_AFTER: Duration = Duration::from_secs(60 * 60);

/// A record no build has rewritten for this long is dropped by the daemon's
/// GC; the next store or restore of the artifact records it again.
const RECORD_TTL: Duration = Duration::from_secs(30 * 24 * 60 * 60);

const STAGED_PREFIX: &str = ".kache-prestage-";
const FINGERPRINT_SUFFIX: &str = ".fp";
const HINT_MARKER: &str = ".hinted";
const STAGING_LOCK: &str = ".staging.lock";
const ACTIVITY_PREFIX: &str = ".activity-";
/// `latest-<project>` names the target directory of the project that
/// recorded last.
const LATEST_PREFIX: &str = "latest-";

/// A large executable a build of some target directory produced.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Record {
    dest: PathBuf,
    /// `dest` under its target directory, for another target to borrow.
    rel: PathBuf,
    blob: String,
    size: u64,
}

/// What the daemon wrote about a staged copy.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Staged {
    blob: String,
    fingerprint: FileFingerprint,
}

#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum Activity {
    Pending {
        record: Record,
        generation: Option<FileFingerprint>,
    },
    Completed {
        dest: PathBuf,
        generation: Option<FileFingerprint>,
    },
}

/// Only publication and retirement hold this lock. In particular, a wrapper
/// never waits for the daemon's potentially large copy or hash.
fn with_activity<T>(
    cache_dir: &Path,
    target_dir: &Path,
    rel: &Path,
    action: impl FnOnce(&Path) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    let path = records_dir(cache_dir, target_dir).join(format!(
        "{ACTIVITY_PREFIX}{}",
        short_hash(artifact_key(rel).as_bytes())
    ));
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path.with_extension("lock"))?;
    lock.lock()?;
    let result = action(&path);
    // A fork can inherit this descriptor until exec. Release explicitly.
    let _ = lock.unlock();
    result
}

fn activity_at(path: &Path) -> anyhow::Result<Option<Activity>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn belongs_to_artifact(target_dir: &Path, dest: &Path, rel: &Path) -> bool {
    dest.strip_prefix(target_dir).is_ok_and(|candidate| {
        candidate
            .components()
            .all(|part| matches!(part, std::path::Component::Normal(_)))
            && artifact_key(candidate) == artifact_key(rel)
    })
}

fn discard_prediction(record: &Record) {
    if let Some(staged) = staged_path(&record.dest, &record.blob) {
        let _ = std::fs::remove_file(&staged);
        let _ = std::fs::remove_file(fingerprint_path(&staged));
    }
}

/// Retire this artifact's prediction after a successful restore or compile.
/// Point lookups only: neither other units nor the target directory are scanned.
/// Missing outputs leave staging available for a later build.
pub(crate) fn complete(
    cache_dir: &Path,
    target_dir: &Path,
    dest: &Path,
    generation: &Option<FileFingerprint>,
) {
    let Ok(rel) = dest.strip_prefix(target_dir) else {
        return;
    };
    if !dest.is_file() || !records_dir(cache_dir, target_dir).is_dir() {
        return;
    }
    let result = with_activity(cache_dir, target_dir, rel, |path| {
        let previous = activity_at(path)?;
        write_atomic(
            path,
            &serde_json::to_vec(&Activity::Completed {
                dest: dest.to_path_buf(),
                generation: generation.clone(),
            })?,
        )?;
        if let Some(Activity::Pending { record, .. }) = previous
            && belongs_to_artifact(target_dir, &record.dest, rel)
        {
            discard_prediction(&record);
        }
        Ok(())
    });
    if let Err(error) = result {
        tracing::debug!(
            "prestage retirement for {} failed: {error:#}",
            dest.display()
        );
    }
}

fn begin_stage(
    cache_dir: &Path,
    target_dir: &Path,
    record: &Record,
    generation: &Option<FileFingerprint>,
) -> anyhow::Result<bool> {
    with_activity(cache_dir, target_dir, &record.rel, |path| {
        if hint_generation(cache_dir, target_dir) != *generation {
            return Ok(false);
        }
        match activity_at(path)? {
            Some(Activity::Completed {
                dest,
                generation: completed,
            }) if completed == *generation
                && belongs_to_artifact(target_dir, &dest, &record.rel)
                && dest.is_file() =>
            {
                return Ok(false);
            }
            Some(Activity::Pending {
                record: previous, ..
            }) if previous != *record
                && belongs_to_artifact(target_dir, &previous.dest, &record.rel) =>
            {
                discard_prediction(&previous);
            }
            _ => {}
        }
        write_atomic(
            path,
            &serde_json::to_vec(&Activity::Pending {
                record: record.clone(),
                generation: generation.clone(),
            })?,
        )?;
        Ok(true)
    })
}

fn short_hash(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex()[..16].to_string()
}

fn prestage_dir(cache_dir: &Path) -> PathBuf {
    cache_dir.join("prestage")
}

/// The records of one target directory.
fn records_dir(cache_dir: &Path, target_dir: &Path) -> PathBuf {
    prestage_dir(cache_dir).join(short_hash(target_dir.as_os_str().as_encoded_bytes()))
}

pub(crate) fn has_predictions(cache_dir: &Path, target_dir: &Path) -> bool {
    records_dir(cache_dir, target_dir).is_dir()
}

/// Atomic hint replacement gives each staging pass a bounded lifetime.
pub(crate) fn hint_generation(cache_dir: &Path, target_dir: &Path) -> Option<FileFingerprint> {
    FileFingerprint::from_path(&records_dir(cache_dir, target_dir).join(HINT_MARKER)).ok()
}

/// The project a target directory belongs to: a hash of the `Cargo.toml`
/// beside it. `None` for a target directory placed elsewhere, which then
/// only ever uses its own records.
fn project(target_dir: &Path) -> Option<String> {
    let manifest = std::fs::read(target_dir.parent()?.join("Cargo.toml")).ok()?;
    Some(short_hash(&manifest))
}

fn latest_path(cache_dir: &Path, project: &str) -> PathBuf {
    prestage_dir(cache_dir).join(format!("{LATEST_PREFIX}{project}"))
}

/// The artifact a destination holds, without Cargo's metadata hash:
/// `debug/deps/hk-0123456789abcdef` and its successor under a new hash are
/// the same artifact.
fn artifact_key(rel: &Path) -> String {
    let rel = rel.to_string_lossy();
    let name_start = rel.rfind(['/', '\\']).map_or(0, |slash| slash + 1);
    let (stem, extension) = match rel.rfind('.') {
        Some(dot) if dot > name_start => rel.split_at(dot),
        _ => (rel.as_ref(), ""),
    };
    let unhashed = stem
        .rsplit_once('-')
        .filter(|(_, hash)| hash.len() == 16 && hash.bytes().all(|b| b.is_ascii_hexdigit()))
        .map_or(stem, |(name, _)| name);
    format!("{unhashed}{extension}")
}

/// Where the daemon stages `blob` for `dest`: beside it, so the restore's
/// rename stays on one filesystem.
pub(crate) fn staged_path(dest: &Path, blob: &str) -> Option<PathBuf> {
    let name = dest.file_name()?.to_string_lossy();
    let blob = blob.get(..16)?;
    Some(dest.with_file_name(format!("{STAGED_PREFIX}{blob}-{name}")))
}

fn fingerprint_path(staged: &Path) -> PathBuf {
    let mut name = staged.as_os_str().to_owned();
    name.push(FINGERPRINT_SUFFIX);
    PathBuf::from(name)
}

/// Write `contents` to `path` through a temporary file in the same directory.
fn write_atomic(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or(Path::new("."));
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    std::io::Write::write_all(&mut file, contents)?;
    file.persist(path).map_err(|error| error.error)?;
    Ok(())
}

/// When a file last changed hands: its status-change time on Unix, which a
/// clone or copy that keeps the source's mtime still resets.
fn changed_at(metadata: &std::fs::Metadata) -> Option<SystemTime> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let secs = u64::try_from(metadata.ctime()).ok()?;
        let nanos = u32::try_from(metadata.ctime_nsec()).unwrap_or(0);
        Some(SystemTime::UNIX_EPOCH + Duration::new(secs, nanos))
    }
    #[cfg(not(unix))]
    {
        metadata.modified().ok()
    }
}

fn older_than(metadata: &std::fs::Metadata, age: Duration, now: SystemTime) -> bool {
    changed_at(metadata).is_some_and(|at| elapsed_beyond(at, age, now))
}

/// Whether more than `age` passed between `at` and `now`.
fn elapsed_beyond(at: SystemTime, age: Duration, now: SystemTime) -> bool {
    now.duration_since(at).is_ok_and(|elapsed| elapsed > age)
}

/// Stage `blob_path`'s bytes, named `blob`, for `dest`, as the daemon would.
#[cfg(test)]
pub(crate) fn stage_for_test(dest: &Path, blob_path: &Path, blob: &str) {
    let record = Record {
        dest: dest.to_path_buf(),
        rel: PathBuf::from(dest.file_name().unwrap()),
        blob: blob.to_string(),
        size: MIN_BYTES,
    };
    stage_one(&record, blob_path).unwrap();
}

/// Every destination recorded under `cache_dir`.
#[cfg(test)]
pub(crate) fn recorded_dests(cache_dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(prestage_dir(cache_dir)) else {
        return Vec::new();
    };
    entries
        .flatten()
        .flat_map(|entry| read_records(&entry.path()))
        .map(|(_, record)| record.dest)
        .collect()
}

/// Remember that a build of `target_dir` put `blob` at `dest`. Best-effort:
/// a record that cannot be written only means the next build copies.
pub(crate) fn remember(cache_dir: &Path, target_dir: &Path, dest: &Path, blob: &str, size: u64) {
    if size < MIN_BYTES || !target_dir.is_absolute() {
        return;
    }
    let Ok(rel) = dest.strip_prefix(target_dir) else {
        return;
    };
    let record = Record {
        dest: dest.to_path_buf(),
        rel: rel.to_path_buf(),
        blob: blob.to_string(),
        size,
    };
    // Paths that are not UTF-8 cannot go into a record; such a target just
    // copies as before.
    let Ok(bytes) = serde_json::to_vec(&record) else {
        return;
    };
    let directory = records_dir(cache_dir, target_dir);
    let path = directory.join(short_hash(artifact_key(rel).as_bytes()));
    if std::fs::read(&path).is_ok_and(|existing| existing == bytes) {
        return;
    }
    let target = target_dir.as_os_str().as_encoded_bytes();
    let written = std::fs::create_dir_all(&directory)
        .and_then(|()| write_atomic(&path, &bytes))
        .and_then(|()| {
            let Some(project) = project(target_dir) else {
                return Ok(());
            };
            let latest = latest_path(cache_dir, &project);
            if std::fs::read(&latest).is_ok_and(|current| current == target) {
                return Ok(());
            }
            write_atomic(&latest, target)
        });
    if let Err(error) = written {
        tracing::debug!(
            "prestage record for {} not written: {error}",
            dest.display()
        );
    }
}

/// The target directory of `target_dir`'s project that recorded last, when
/// it is not `target_dir`.
fn lender(cache_dir: &Path, target_dir: &Path) -> Option<PathBuf> {
    let bytes = std::fs::read(latest_path(cache_dir, &project(target_dir)?)).ok()?;
    let latest = PathBuf::from(String::from_utf8(bytes).ok()?);
    (latest != target_dir).then_some(latest)
}

fn read_records(directory: &Path) -> Vec<(PathBuf, Record)> {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter(|entry| !entry.file_name().to_string_lossy().starts_with('.'))
        .filter_map(|entry| {
            let bytes = std::fs::read(entry.path()).ok()?;
            let record = serde_json::from_slice::<Record>(&bytes).ok()?;
            Some((entry.path(), record))
        })
        .collect()
}

/// What to stage for `target_dir`: its own records, or else those of the
/// project's target directory that recorded last, moved under `target_dir`.
fn records_for(cache_dir: &Path, target_dir: &Path) -> Vec<Record> {
    let own: Vec<Record> = read_records(&records_dir(cache_dir, target_dir))
        .into_iter()
        .map(|(_, record)| record)
        .collect();
    if !own.is_empty() {
        return own;
    }
    let Some(other) = lender(cache_dir, target_dir) else {
        return Vec::new();
    };
    read_records(&records_dir(cache_dir, &other))
        .into_iter()
        .map(|(_, record)| Record {
            dest: target_dir.join(&record.rel),
            ..record
        })
        .collect()
}

/// Whether a build of `target_dir` should hint the daemon now: something is
/// recorded for the target, or for its project elsewhere, and no hint went
/// out in the last [`HINT_INTERVAL`]. Marks the hint as sent when it answers
/// yes.
fn hint_due(cache_dir: &Path, target_dir: &Path, now: SystemTime) -> bool {
    if !target_dir.is_absolute() {
        return false;
    }
    let directory = records_dir(cache_dir, target_dir);
    if !directory.is_dir()
        && (lender(cache_dir, target_dir).is_none() || std::fs::create_dir_all(&directory).is_err())
    {
        return false;
    }
    let marker = directory.join(HINT_MARKER);
    // A marker newer than `now` (clock skew, or written after `now` was
    // taken) counts as recent.
    let recent = std::fs::metadata(&marker)
        .and_then(|metadata| metadata.modified())
        .is_ok_and(|modified| {
            now.duration_since(modified)
                .map_or(true, |age| age < HINT_INTERVAL)
        });
    if recent {
        return false;
    }
    write_atomic(&marker, b"").is_ok()
}

/// Ask the daemon to stage what `target_dir` will restore, when due. Two
/// `stat`s on the common path; a target directory with no records also
/// reads its project's `Cargo.toml`.
pub(crate) fn maybe_hint(config: &crate::config::Config, target_dir: &Path) {
    if hint_due(&config.cache_dir, target_dir, SystemTime::now()) {
        crate::daemon::send_prestage(config, target_dir);
    }
}

/// Put the copy the daemon staged for `blob` at `dest`, if there is one and
/// nothing has touched it since the daemon wrote it. `false` means the caller
/// copies the blob itself; a staged copy that fails the check is removed.
pub(crate) fn take(dest: &Path, blob: &str) -> bool {
    let Some(staged) = staged_path(dest, blob) else {
        return false;
    };
    let fingerprint = fingerprint_path(&staged);
    let Ok(bytes) = std::fs::read(&fingerprint) else {
        return false;
    };
    let current = FileFingerprint::from_path(&staged).ok();
    let intact = serde_json::from_slice::<Staged>(&bytes)
        .ok()
        .is_some_and(|recorded| recorded.blob == blob && Some(recorded.fingerprint) == current);
    if intact && std::fs::rename(&staged, dest).is_ok() {
        let _ = std::fs::remove_file(&fingerprint);
        if let Ok(metadata) = std::fs::metadata(dest) {
            kache_store::opcounts::record_copied(metadata.len());
        }
        return true;
    }
    let _ = std::fs::remove_file(&staged);
    let _ = std::fs::remove_file(&fingerprint);
    false
}

/// Stage every executable recorded for `target_dir` (see [`records_for`])
/// whose destination is missing. `blob_path` finds a blob in the store; a
/// record whose blob is gone is skipped. Runs in the daemon, off any
/// wrapper's path; a second pass for the same target while one runs does
/// nothing.
pub(crate) fn stage(cache_dir: &Path, target_dir: &Path, blob_path: impl Fn(&str) -> PathBuf) {
    match stage_pass(cache_dir, target_dir, blob_path) {
        Ok(results) => {
            for (dest, result) in results {
                if let Err(error) = result {
                    tracing::debug!("not staging {}: {error:#}", dest.display());
                }
            }
        }
        Err(error) => tracing::debug!("no staging pass for {}: {error}", target_dir.display()),
    }
}

/// One staging pass, and what happened to each record. `Err` when the pass
/// did not run: the lock could not be opened, or another pass holds it.
fn stage_pass(
    cache_dir: &Path,
    target_dir: &Path,
    blob_path: impl Fn(&str) -> PathBuf,
) -> std::io::Result<Vec<(PathBuf, anyhow::Result<()>)>> {
    let directory = records_dir(cache_dir, target_dir);
    std::fs::create_dir_all(&directory)?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(directory.join(STAGING_LOCK))?;
    lock.try_lock().map_err(std::io::Error::from)?;
    let generation = hint_generation(cache_dir, target_dir);
    let results = records_for(cache_dir, target_dir)
        .into_iter()
        .map(|record| {
            let result = stage_record(
                cache_dir,
                target_dir,
                &record,
                &blob_path(&record.blob),
                &generation,
            );
            (record.dest, result)
        })
        .collect();
    // Unlock rather than only drop: a child forked while the file is open
    // shares its lock until the child execs, and would make the next pass
    // skip.
    let _ = lock.unlock();
    Ok(results)
}

fn stage_record(
    cache_dir: &Path,
    target_dir: &Path,
    record: &Record,
    blob: &Path,
    generation: &Option<FileFingerprint>,
) -> anyhow::Result<()> {
    if !begin_stage(cache_dir, target_dir, record, generation)? {
        return Ok(());
    }
    if let Some(prepared) = prepare_stage(record, blob)? {
        publish_stage(cache_dir, target_dir, record, prepared, generation)?;
    }
    Ok(())
}

fn publish_stage(
    cache_dir: &Path,
    target_dir: &Path,
    record: &Record,
    prepared: PreparedStage,
    generation: &Option<FileFingerprint>,
) -> anyhow::Result<()> {
    with_activity(cache_dir, target_dir, &record.rel, |path| {
        if hint_generation(cache_dir, target_dir) == *generation
            && activity_at(path)?
                == Some(Activity::Pending {
                    record: record.clone(),
                    generation: generation.clone(),
                })
            && !record.dest.exists()
        {
            prepared.publish(record)?;
        }
        Ok(())
    })
}

#[cfg(test)]
fn stage_one(record: &Record, blob: &Path) -> anyhow::Result<()> {
    if let Some(prepared) = prepare_stage(record, blob)? {
        prepared.publish(record)?;
    }
    Ok(())
}

struct PreparedStage {
    temporary: tempfile::TempPath,
    staged: PathBuf,
}

impl PreparedStage {
    fn publish(self, record: &Record) -> anyhow::Result<()> {
        self.temporary.persist(&self.staged)?;
        let staged_file = Staged {
            blob: record.blob.clone(),
            fingerprint: FileFingerprint::from_path(&self.staged)?,
        };
        write_atomic(
            &fingerprint_path(&self.staged),
            &serde_json::to_vec(&staged_file)?,
        )?;
        Ok(())
    }
}

fn prepare_stage(record: &Record, blob: &Path) -> anyhow::Result<Option<PreparedStage>> {
    let Some(directory) = record.dest.parent() else {
        return Ok(None);
    };
    sweep_stale(directory, SystemTime::now());
    if !record.dest.is_absolute() || record.dest.exists() || !directory.is_dir() || !blob.is_file()
    {
        return Ok(None);
    }
    let staged = staged_path(&record.dest, &record.blob)
        .ok_or_else(|| anyhow::anyhow!("no staging name"))?;
    let fingerprint = fingerprint_path(&staged);
    if fingerprint.is_file() {
        if staged.is_file() {
            return Ok(None);
        }
        std::fs::remove_file(&fingerprint)?;
    }
    // A fresh name the restore never looks at, materialized the way the
    // restore would: clone or copy, writable, executable.
    let temporary = tempfile::Builder::new()
        .prefix(STAGED_PREFIX)
        .tempfile_in(directory)?
        .into_temp_path();
    link::link_to_target(blob, &temporary, link::LinkStrategy::Copy)?;
    let hash = kache_store::file_hash::hash_file(&temporary)?;
    anyhow::ensure!(
        hash == record.blob,
        "blob {} reads back as {hash}",
        record.blob
    );
    Ok(Some(PreparedStage { temporary, staged }))
}

/// Remove staged copies and fingerprints in `directory` that nobody took
/// within [`STALE_AFTER`].
fn sweep_stale(directory: &Path, now: SystemTime) {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        if !entry
            .file_name()
            .to_string_lossy()
            .starts_with(STAGED_PREFIX)
        {
            continue;
        }
        if entry
            .metadata()
            .is_ok_and(|metadata| older_than(&metadata, STALE_AFTER, now))
        {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

/// Drop records no build rewrote within [`RECORD_TTL`] or whose target
/// directory is gone, and `latest` pointers to missing target directories.
/// Called from the daemon's GC; returns how many files it removed.
pub(crate) fn prune(cache_dir: &Path, now: SystemTime) -> usize {
    let Ok(entries) = std::fs::read_dir(prestage_dir(cache_dir)) else {
        return 0;
    };
    let mut removed = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with(LATEST_PREFIX)
        {
            let gone = std::fs::read(&path)
                .ok()
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .is_none_or(|target| !Path::new(&target).is_dir());
            if gone && std::fs::remove_file(&path).is_ok() {
                removed += 1;
            }
            continue;
        }
        if !path.is_dir() {
            continue;
        }
        for (record_path, record) in read_records(&path) {
            let dest = record.dest.to_string_lossy();
            let target_gone = dest
                .strip_suffix(&*record.rel.to_string_lossy())
                .is_none_or(|target| !Path::new(target).is_dir());
            let expired = std::fs::metadata(&record_path)
                .is_ok_and(|metadata| older_than(&metadata, RECORD_TTL, now));
            if (target_gone || expired) && std::fs::remove_file(&record_path).is_ok() {
                removed += 1;
            }
        }
        // A target borrowing predictions can have no records of its own.
        // Expire its handshake data under the same publication lock.
        if let Ok(states) = std::fs::read_dir(&path) {
            for state in states.flatten().filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(ACTIVITY_PREFIX)
                    && entry.path().extension().is_none()
            }) {
                let Ok(lock) = std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .write(true)
                    .open(state.path().with_extension("lock"))
                else {
                    continue;
                };
                if lock.try_lock().is_err() {
                    continue;
                }
                let gone = activity_at(&state.path())
                    .ok()
                    .flatten()
                    .is_none_or(|activity| {
                        let dest = match &activity {
                            Activity::Pending { record, .. } => &record.dest,
                            Activity::Completed { dest, .. } => dest,
                        };
                        !dest.ancestors().any(|ancestor| {
                            records_dir(cache_dir, ancestor) == path && ancestor.is_dir()
                        })
                    });
                let expired = std::fs::metadata(state.path())
                    .is_ok_and(|metadata| older_than(&metadata, RECORD_TTL, now));
                if (gone || expired) && std::fs::remove_file(state.path()).is_ok() {
                    removed += 1;
                }
                let _ = lock.unlock();
            }
        }
        if read_records(&path).is_empty() {
            // Never unlink coordination files: a wrapper may have opened a
            // lock but not written its first state yet. Recreating the same
            // lock path would give two owners different locked inodes.
            let _ = std::fs::remove_dir(&path);
        }
    }
    removed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blob(dir: &Path, content: &[u8]) -> (PathBuf, String) {
        let hash = blake3::hash(content).to_hex().to_string();
        let path = dir.join("blobs").join(&hash);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, content).unwrap();
        (path, hash)
    }

    /// `<dir>/<name>/target`, beside a `Cargo.toml` naming `project`.
    fn checkout(dir: &Path, name: &str, project: &str) -> PathBuf {
        let target = dir.join(name).join("target");
        std::fs::create_dir_all(target.join("debug/deps")).unwrap();
        std::fs::write(
            dir.join(name).join("Cargo.toml"),
            format!("[package]\nname = \"{project}\"\n"),
        )
        .unwrap();
        target
    }

    fn target(dir: &Path, name: &str) -> PathBuf {
        checkout(dir, name, "app")
    }

    #[test]
    fn missing_activity_is_absent_but_other_read_errors_propagate() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(activity_at(&dir.path().join("missing")).unwrap(), None);
        assert!(
            activity_at(dir.path()).is_err(),
            "a directory read error must not become absent activity"
        );
    }

    /// A recorded executable is staged beside its missing destination and
    /// the restore takes it: the destination ends up with the blob's bytes
    /// in a file of its own, and nothing is left behind.
    #[test]
    fn a_staged_copy_is_taken_by_the_restore() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let target = target(dir.path(), "a");
        let (blob_path, hash) = blob(dir.path(), b"an executable");
        let dest = target.join("debug/deps/app-0123456789abcdef");

        remember(&cache, &target, &dest, &hash, MIN_BYTES);
        stage(&cache, &target, |_| blob_path.clone());
        let staged = staged_path(&dest, &hash).unwrap();
        assert!(staged.is_file() && fingerprint_path(&staged).is_file());
        assert!(!dest.exists());

        assert!(take(&dest, &hash));
        assert_eq!(std::fs::read(&dest).unwrap(), b"an executable");
        assert!(!staged.exists() && !fingerprint_path(&staged).exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(
                std::fs::metadata(&dest).unwrap().nlink(),
                1,
                "a private copy"
            );
            assert_eq!(std::fs::metadata(&dest).unwrap().mode() & 0o111, 0o111);
        }
        assert!(!take(&dest, &hash), "taken once");
    }

    /// Stage the one recorded executable, failing with the pass's own error
    /// rather than leaving a silently unstaged copy for a later assertion.
    fn stage_ok(cache: &Path, target: &Path, blob_path: &Path) {
        let results = stage_pass(cache, target, |_| blob_path.to_path_buf()).unwrap();
        assert_eq!(results.len(), 1);
        for (dest, result) in results {
            result.unwrap_or_else(|error| panic!("staging {}: {error:#}", dest.display()));
        }
    }

    #[test]
    fn completion_retires_each_publication_phase_and_allows_a_later_build() {
        for phase in 0..5 {
            let dir = tempfile::tempdir().unwrap();
            let cache = dir.path().join("cache");
            let target = target(dir.path(), "a");
            let (blob_path, hash) = blob(dir.path(), b"predicted executable");
            let predicted = target.join("debug/deps/app-aaaaaaaaaaaaaaaa");
            let completed = target.join("debug/deps/app-bbbbbbbbbbbbbbbb");
            remember(&cache, &target, &predicted, &hash, MIN_BYTES);
            let record = records_for(&cache, &target).pop().unwrap();
            let staged = staged_path(&predicted, &hash).unwrap();
            let finish = || {
                std::fs::write(&completed, b"compiled executable").unwrap();
                complete(&cache, &target, &completed, &None);
            };
            match phase {
                0 => {
                    // The daemon has queued the record, but has not begun it.
                    finish();
                    stage_ok(&cache, &target, &blob_path);
                }
                1 => {
                    assert!(begin_stage(&cache, &target, &record, &None).unwrap());
                    finish();
                    let prepared = prepare_stage(&record, &blob_path).unwrap().unwrap();
                    publish_stage(&cache, &target, &record, prepared, &None).unwrap();
                }
                2 => {
                    assert!(begin_stage(&cache, &target, &record, &None).unwrap());
                    let prepared = prepare_stage(&record, &blob_path).unwrap().unwrap();
                    finish();
                    publish_stage(&cache, &target, &record, prepared, &None).unwrap();
                }
                3 => {
                    assert!(begin_stage(&cache, &target, &record, &None).unwrap());
                    let prepared = prepare_stage(&record, &blob_path).unwrap().unwrap();
                    // An interrupted writer can leave the data without .fp.
                    prepared.temporary.persist(&staged).unwrap();
                    assert!(!fingerprint_path(&staged).exists());
                    finish();
                }
                4 => {
                    stage_ok(&cache, &target, &blob_path);
                    assert!(staged.is_file() && fingerprint_path(&staged).is_file());
                    finish();
                }
                _ => unreachable!(),
            }
            assert!(
                !staged.exists() && !fingerprint_path(&staged).exists(),
                "phase {phase}"
            );
            assert_eq!(std::fs::read(&completed).unwrap(), b"compiled executable");
            let leftovers: Vec<_> = std::fs::read_dir(predicted.parent().unwrap())
                .unwrap()
                .flatten()
                .filter(|entry| {
                    entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with(STAGED_PREFIX)
                })
                .collect();
            assert!(
                leftovers.is_empty(),
                "discarded copies must drop their temporary file"
            );

            // Cargo removes the output before a later rebuild. Completion
            // must not permanently suppress this artifact's staging.
            std::fs::remove_file(&completed).unwrap();
            stage_ok(&cache, &target, &blob_path);
            assert!(
                take(&predicted, &hash),
                "phase {phase}: next build can stage"
            );
            assert_eq!(std::fs::read(&predicted).unwrap(), b"predicted executable");
        }
    }

    #[test]
    fn publication_requires_its_own_pending_record_and_a_missing_destination() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let target = target(dir.path(), "a");
        let (first_blob, first_hash) = blob(dir.path(), b"first prediction");
        let (second_blob, second_hash) = blob(dir.path(), b"second prediction");
        let first_dest = target.join("debug/deps/app-aaaaaaaaaaaaaaaa");
        let second_dest = target.join("debug/deps/app-bbbbbbbbbbbbbbbb");
        remember(&cache, &target, &first_dest, &first_hash, MIN_BYTES);
        let first = records_for(&cache, &target).pop().unwrap();
        assert!(begin_stage(&cache, &target, &first, &None).unwrap());
        let first_copy = prepare_stage(&first, &first_blob).unwrap().unwrap();
        remember(&cache, &target, &second_dest, &second_hash, MIN_BYTES);
        let second = records_for(&cache, &target).pop().unwrap();
        assert!(begin_stage(&cache, &target, &second, &None).unwrap());
        let second_copy = prepare_stage(&second, &second_blob).unwrap().unwrap();
        publish_stage(&cache, &target, &first, first_copy, &None).unwrap();
        assert!(!staged_path(&first_dest, &first_hash).unwrap().exists());
        publish_stage(&cache, &target, &second, second_copy, &None).unwrap();
        assert!(take(&second_dest, &second_hash));

        // Even without a wrapper acknowledgement, never publish over a
        // destination that appeared while the daemon copied.
        assert!(begin_stage(&cache, &target, &first, &None).unwrap());
        let copy = prepare_stage(&first, &first_blob).unwrap().unwrap();
        std::fs::write(&first_dest, b"compiler won").unwrap();
        publish_stage(&cache, &target, &first, copy, &None).unwrap();
        assert!(!staged_path(&first_dest, &first_hash).unwrap().exists());
        assert_eq!(std::fs::read(&first_dest).unwrap(), b"compiler won");
    }

    #[test]
    fn beginning_a_stage_preserves_foreign_and_identical_valid_predictions() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let target = target(dir.path(), "a");
        let (own_blob, own_hash) = blob(dir.path(), b"own prediction");
        let (foreign_blob, foreign_hash) = blob(dir.path(), b"foreign prediction");
        let own_dest = target.join("debug/deps/app-aaaaaaaaaaaaaaaa");
        remember(&cache, &target, &own_dest, &own_hash, MIN_BYTES);
        let own = records_for(&cache, &target).pop().unwrap();
        let foreign = Record {
            dest: target.join("debug/deps/other-aaaaaaaaaaaaaaaa"),
            rel: own.rel.clone(),
            blob: foreign_hash,
            size: MIN_BYTES,
        };
        stage_one(&foreign, &foreign_blob).unwrap();
        with_activity(&cache, &target, &own.rel, |path| {
            write_atomic(
                path,
                &serde_json::to_vec(&Activity::Pending {
                    record: foreign.clone(),
                    generation: None,
                })?,
            )?;
            Ok(())
        })
        .unwrap();
        std::fs::remove_file(&foreign_blob).unwrap();
        assert!(begin_stage(&cache, &target, &own, &None).unwrap());
        assert!(
            take(&foreign.dest, &foreign.blob),
            "beginning our artifact must not discard another artifact's prediction"
        );
        assert_eq!(std::fs::read(&foreign.dest).unwrap(), b"foreign prediction");

        stage_record(&cache, &target, &own, &own_blob, &None).unwrap();
        let staged = staged_path(&own.dest, &own.blob).unwrap();
        let fingerprint = std::fs::read(fingerprint_path(&staged)).unwrap();
        std::fs::remove_file(&own_blob).unwrap();
        // A duplicate request must keep the published prediction. With its
        // blob gone, a needless discard cannot be hidden by making a new copy.
        stage_record(&cache, &target, &own, &own_blob, &None).unwrap();
        assert_eq!(
            std::fs::read(fingerprint_path(&staged)).unwrap(),
            fingerprint
        );
        assert!(
            take(&own.dest, &own.blob),
            "an identical Pending must keep the already-valid staged copy"
        );
        assert_eq!(std::fs::read(&own.dest).unwrap(), b"own prediction");
    }

    #[test]
    fn a_different_own_record_discards_a_published_prediction_without_a_new_blob() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let target = target(dir.path(), "a");
        let (old_blob, old_hash) = blob(dir.path(), b"old prediction");
        let old_dest = target.join("debug/deps/app-aaaaaaaaaaaaaaaa");
        remember(&cache, &target, &old_dest, &old_hash, MIN_BYTES);
        let old = records_for(&cache, &target).pop().unwrap();
        stage_record(&cache, &target, &old, &old_blob, &None).unwrap();
        let staged = staged_path(&old_dest, &old_hash).unwrap();
        assert!(staged.is_file() && fingerprint_path(&staged).is_file());

        let (new_blob, new_hash) = blob(dir.path(), b"new prediction");
        let new_dest = target.join("debug/deps/app-bbbbbbbbbbbbbbbb");
        remember(&cache, &target, &new_dest, &new_hash, MIN_BYTES);
        let new = records_for(&cache, &target).pop().unwrap();
        std::fs::remove_file(&new_blob).unwrap();
        stage_record(&cache, &target, &new, &new_blob, &None).unwrap();
        assert!(
            !staged.exists() && !fingerprint_path(&staged).exists(),
            "a different own Pending retires the published old prediction even without a new blob"
        );
        assert!(!staged_path(&new_dest, &new_hash).unwrap().exists());
        assert!(!old_dest.exists() && !new_dest.exists());
    }

    #[test]
    fn a_new_hint_stages_another_metadata_variant_while_the_completed_output_remains() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let target = target(dir.path(), "a");
        let (blob_path, hash) = blob(dir.path(), b"predicted executable");
        let predicted = target.join("debug/deps/app-aaaaaaaaaaaaaaaa");
        let completed = target.join("debug/deps/app-bbbbbbbbbbbbbbbb");
        remember(&cache, &target, &predicted, &hash, MIN_BYTES);
        assert!(hint_due(&cache, &target, SystemTime::now()));
        let first = hint_generation(&cache, &target);
        assert!(first.is_some());
        stage_ok(&cache, &target, &blob_path);
        std::fs::write(&completed, b"completed variant").unwrap();
        complete(&cache, &target, &completed, &first);
        let staged = staged_path(&predicted, &hash).unwrap();
        assert!(!staged.exists() && !fingerprint_path(&staged).exists());
        stage_ok(&cache, &target, &blob_path);
        assert!(
            !staged.exists(),
            "the completed generation cannot publish late"
        );

        assert!(hint_due(
            &cache,
            &target,
            SystemTime::now() + HINT_INTERVAL + Duration::from_secs(1)
        ));
        assert_ne!(hint_generation(&cache, &target), first);
        stage_ok(&cache, &target, &blob_path);
        assert!(staged.is_file() && fingerprint_path(&staged).is_file());
        assert_eq!(std::fs::read(&completed).unwrap(), b"completed variant");
        assert!(take(&predicted, &hash));
    }

    #[test]
    fn an_old_copy_cannot_publish_over_a_new_pending_generation_of_the_same_record() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let target = target(dir.path(), "a");
        let (blob_path, hash) = blob(dir.path(), b"predicted executable");
        let predicted = target.join("debug/deps/app-aaaaaaaaaaaaaaaa");
        let completed = target.join("debug/deps/app-bbbbbbbbbbbbbbbb");
        remember(&cache, &target, &predicted, &hash, MIN_BYTES);
        assert!(hint_due(&cache, &target, SystemTime::now()));
        let first = hint_generation(&cache, &target);
        let record = records_for(&cache, &target).pop().unwrap();
        assert!(begin_stage(&cache, &target, &record, &first).unwrap());
        let old_copy = prepare_stage(&record, &blob_path).unwrap().unwrap();
        std::fs::write(&completed, b"completed variant").unwrap();
        complete(&cache, &target, &completed, &first);
        assert!(hint_due(
            &cache,
            &target,
            SystemTime::now() + HINT_INTERVAL + Duration::from_secs(1)
        ));
        let second = hint_generation(&cache, &target);
        assert_ne!(first, second);
        assert!(begin_stage(&cache, &target, &record, &second).unwrap());
        let new_copy = prepare_stage(&record, &blob_path).unwrap().unwrap();
        publish_stage(&cache, &target, &record, old_copy, &first).unwrap();
        let staged = staged_path(&predicted, &hash).unwrap();
        assert!(!staged.exists() && !fingerprint_path(&staged).exists());
        publish_stage(&cache, &target, &record, new_copy, &second).unwrap();
        assert!(take(&predicted, &hash));
        assert_eq!(std::fs::read(&completed).unwrap(), b"completed variant");
    }

    #[test]
    fn an_obsolete_pass_cannot_begin_after_a_new_generations_completion() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let target = target(dir.path(), "a");
        let (blob_path, hash) = blob(dir.path(), b"predicted executable");
        let predicted = target.join("debug/deps/app-aaaaaaaaaaaaaaaa");
        let completed = target.join("debug/deps/app-bbbbbbbbbbbbbbbb");
        remember(&cache, &target, &predicted, &hash, MIN_BYTES);
        assert!(hint_due(&cache, &target, SystemTime::now()));
        let first = hint_generation(&cache, &target);
        let record = records_for(&cache, &target).pop().unwrap();
        assert!(begin_stage(&cache, &target, &record, &first).unwrap());
        let old_copy = prepare_stage(&record, &blob_path).unwrap().unwrap();
        assert!(hint_due(
            &cache,
            &target,
            SystemTime::now() + HINT_INTERVAL + Duration::from_secs(1)
        ));
        let second = hint_generation(&cache, &target);
        assert_ne!(first, second);
        // A new hint alone fences an old copy, even before a new Pending or
        // Completed record replaces the otherwise identical pending state.
        publish_stage(&cache, &target, &record, old_copy, &first).unwrap();
        let staged = staged_path(&predicted, &hash).unwrap();
        assert!(!staged.exists() && !fingerprint_path(&staged).exists());

        std::fs::write(&completed, b"completed variant").unwrap();
        complete(&cache, &target, &completed, &second);
        // Another g1 record was queued before begin_stage. It must not erase
        // the g2 completion and then publish after that wrapper has returned.
        assert!(!begin_stage(&cache, &target, &record, &first).unwrap());
        assert!(
            !begin_stage(&cache, &target, &record, &second).unwrap(),
            "the newer completion remains indexed"
        );
        stage_ok(&cache, &target, &blob_path);
        assert!(!staged.exists() && !fingerprint_path(&staged).exists());
        assert_eq!(std::fs::read(&completed).unwrap(), b"completed variant");
    }

    #[test]
    fn retirement_checks_index_ownership_and_requires_a_real_completed_output() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let target = target(dir.path(), "a");
        let (blob_path, hash) = blob(dir.path(), b"prediction");
        let predicted = target.join("debug/deps/app-aaaaaaaaaaaaaaaa");
        let completed = target.join("debug/deps/app-bbbbbbbbbbbbbbbb");
        remember(&cache, &target, &predicted, &hash, MIN_BYTES);
        stage_ok(&cache, &target, &blob_path);
        let staged = staged_path(&predicted, &hash).unwrap();
        complete(&cache, &target, &completed, &None);
        assert!(staged.is_file() && fingerprint_path(&staged).is_file());

        let rel = completed.strip_prefix(&target).unwrap();
        for foreign_dest in [
            target.join("debug/deps/other-aaaaaaaaaaaaaaaa"),
            dir.path().join("app-aaaaaaaaaaaaaaaa"),
        ] {
            let foreign = Record {
                dest: foreign_dest,
                rel: rel.to_path_buf(),
                blob: hash.clone(),
                size: MIN_BYTES,
            };
            stage_one(&foreign, &blob_path).unwrap();
            let foreign_staged = staged_path(&foreign.dest, &hash).unwrap();
            with_activity(&cache, &target, rel, |path| {
                write_atomic(
                    path,
                    &serde_json::to_vec(&Activity::Pending {
                        record: foreign.clone(),
                        generation: None,
                    })?,
                )?;
                Ok(())
            })
            .unwrap();
            std::fs::write(&completed, b"completed").unwrap();
            complete(&cache, &target, &completed, &None);
            assert!(foreign_staged.is_file() && fingerprint_path(&foreign_staged).is_file());
        }
        assert!(
            staged.is_file(),
            "an unrelated indexed prediction cannot name this staged file"
        );
    }

    #[test]
    fn pruning_preserves_borrowed_inflight_and_completed_activity_until_expiry() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let donor = target(dir.path(), "donor");
        let recipient = target(dir.path(), "recipient");
        let (blob_path, hash) = blob(dir.path(), b"prediction");
        remember(
            &cache,
            &donor,
            &donor.join("debug/deps/app-aaaaaaaaaaaaaaaa"),
            &hash,
            MIN_BYTES,
        );
        std::fs::create_dir_all(records_dir(&cache, &recipient)).unwrap();
        let record = records_for(&cache, &recipient).pop().unwrap();
        assert!(begin_stage(&cache, &recipient, &record, &None).unwrap());
        let prepared = prepare_stage(&record, &blob_path).unwrap().unwrap();
        assert!(read_records(&records_dir(&cache, &recipient)).is_empty());
        prune(&cache, SystemTime::now());
        publish_stage(&cache, &recipient, &record, prepared, &None).unwrap();
        let staged = staged_path(&record.dest, &hash).unwrap();
        assert!(staged.is_file());
        let completed = recipient.join("debug/deps/app-bbbbbbbbbbbbbbbb");
        std::fs::write(&completed, b"compiled").unwrap();
        complete(&cache, &recipient, &completed, &None);
        prune(&cache, SystemTime::now());
        assert!(records_dir(&cache, &recipient).is_dir());
        assert!(!begin_stage(&cache, &recipient, &record, &None).unwrap());
        assert!(!staged.exists() && !fingerprint_path(&staged).exists());
        prune(
            &cache,
            SystemTime::now() + RECORD_TTL + Duration::from_secs(1),
        );
        let recipient_records = records_dir(&cache, &recipient);
        assert!(
            recipient_records.is_dir(),
            "coordination inodes remain stable"
        );
        assert!(
            std::fs::read_dir(recipient_records)
                .unwrap()
                .flatten()
                .all(|entry| entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "lock"))
        );
    }

    #[test]
    fn pruning_counts_an_expired_activity_once_and_keeps_its_lock() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let target = target(dir.path(), "a");
        let dest = target.join("debug/deps/app-aaaaaaaaaaaaaaaa");
        std::fs::create_dir_all(records_dir(&cache, &target)).unwrap();
        std::fs::write(&dest, b"completed").unwrap();
        complete(&cache, &target, &dest, &None);
        let path = with_activity(
            &cache,
            &target,
            dest.strip_prefix(&target).unwrap(),
            |path| Ok(path.to_path_buf()),
        )
        .unwrap();
        assert!(path.is_file());
        let at = changed_at(&std::fs::metadata(&path).unwrap()).unwrap();
        assert_eq!(
            prune(&cache, at),
            0,
            "the completed activity is not expired"
        );
        let expired = at + RECORD_TTL + Duration::from_secs(1);
        assert_eq!(
            prune(&cache, expired),
            1,
            "only the expired activity was removed"
        );
        assert!(!path.exists());
        assert!(
            path.with_extension("lock").is_file(),
            "the coordination inode remains stable"
        );
        assert_eq!(prune(&cache, expired), 0, "a second sweep removes nothing");
    }

    #[test]
    fn pruning_removes_a_fresh_activity_after_its_target_disappears() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let target = target(dir.path(), "a");
        let dest = target.join("debug/deps/app-aaaaaaaaaaaaaaaa");
        std::fs::create_dir_all(records_dir(&cache, &target)).unwrap();
        std::fs::write(&dest, b"completed").unwrap();
        complete(&cache, &target, &dest, &None);
        let path = with_activity(
            &cache,
            &target,
            dest.strip_prefix(&target).unwrap(),
            |path| Ok(path.to_path_buf()),
        )
        .unwrap();
        assert!(path.is_file());
        let at = changed_at(&std::fs::metadata(&path).unwrap()).unwrap();
        assert_eq!(prune(&cache, at), 0, "the target and fresh activity remain");
        std::fs::remove_dir_all(&target).unwrap();
        assert_eq!(
            prune(&cache, at),
            1,
            "the target disappeared without aging the activity"
        );
        assert!(!path.exists());
        assert!(path.with_extension("lock").is_file());
        assert_eq!(prune(&cache, at), 0);
    }

    #[test]
    #[ignore = "child fixture for activity_lock_releases_before_its_owner_process_exits"]
    fn activity_lock_probe_child() {
        let root = PathBuf::from(std::env::var_os("KACHE_TEST_PRESTAGE_ACTIVITY_ROOT").unwrap());
        let path = std::env::var_os("KACHE_TEST_PRESTAGE_ACTIVITY_LOCK").unwrap();
        let lock = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        assert!(
            matches!(lock.try_lock(), Err(std::fs::TryLockError::WouldBlock)),
            "with_activity must hold the lock while its closure runs"
        );
        std::fs::write(root.join("blocked"), b"blocked").unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while !root.join("release").is_file() {
            assert!(
                std::time::Instant::now() < deadline,
                "parent did not return from with_activity"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        lock.try_lock()
            .expect("the same child must acquire after with_activity returns");
        std::fs::write(root.join("acquired"), b"acquired").unwrap();
    }

    #[test]
    fn activity_lock_releases_before_its_owner_process_exits() {
        struct ProbeChild(std::process::Child);
        impl Drop for ProbeChild {
            fn drop(&mut self) {
                if !matches!(self.0.try_wait(), Ok(Some(_))) {
                    let _ = self.0.kill();
                }
                let _ = self.0.wait();
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let target = target(dir.path(), "a");
        std::fs::create_dir_all(records_dir(&cache, &target)).unwrap();
        let log_path = dir.path().join("activity-probe.log");
        let log = std::fs::File::create(&log_path).unwrap();
        let child_log = || std::fs::read_to_string(&log_path).unwrap_or_default();
        let mut ran = false;
        let mut child = None;
        with_activity(
            &cache,
            &target,
            Path::new("debug/deps/app-0123456789abcdef"),
            |path| {
                ran = true;
                child = Some(ProbeChild(
                    std::process::Command::new(std::env::current_exe()?)
                        .args([
                            "--exact",
                            "prestage::tests::activity_lock_probe_child",
                            "--ignored",
                            "--nocapture",
                            "--test-threads=1",
                        ])
                        .env("KACHE_TEST_PRESTAGE_ACTIVITY_ROOT", dir.path())
                        .env(
                            "KACHE_TEST_PRESTAGE_ACTIVITY_LOCK",
                            path.with_extension("lock"),
                        )
                        .current_dir(dir.path())
                        .stdout(log.try_clone()?)
                        .stderr(log.try_clone()?)
                        .spawn()?,
                ));
                let probe = child.as_mut().unwrap();
                let deadline = std::time::Instant::now() + Duration::from_secs(30);
                while !dir.path().join("blocked").is_file() {
                    let status = probe.0.try_wait()?;
                    assert!(
                        status.is_none(),
                        "child exited before its blocked probe: {status:?}\n{}",
                        child_log()
                    );
                    assert!(
                        std::time::Instant::now() < deadline,
                        "child did not probe the held lock\n{}",
                        child_log()
                    );
                    std::thread::sleep(Duration::from_millis(10));
                }
                assert_eq!(std::fs::read(dir.path().join("blocked"))?, b"blocked");
                Ok(())
            },
        )
        .unwrap();
        assert!(ran, "with_activity must execute the closure");
        // This signal is written only after the actual owner's File has
        // dropped; the parent process is still alive and checking the child.
        std::fs::write(dir.path().join("release"), b"release").unwrap();
        let child = child.as_mut().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                assert!(
                    status.success(),
                    "child failed its released probe: {status}\n{}",
                    child_log()
                );
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "child did not finish its released probe\n{}",
                child_log()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            std::fs::read(dir.path().join("acquired")).unwrap(),
            b"acquired"
        );
    }

    #[test]
    fn pruning_cannot_replace_a_lock_before_its_first_activity_write() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let donor = target(dir.path(), "donor");
        let recipient = target(dir.path(), "recipient");
        let (blob_path, hash) = blob(dir.path(), b"prediction");
        remember(
            &cache,
            &donor,
            &donor.join("debug/deps/app-aaaaaaaaaaaaaaaa"),
            &hash,
            MIN_BYTES,
        );
        std::fs::create_dir_all(records_dir(&cache, &recipient)).unwrap();
        let record = records_for(&cache, &recipient).pop().unwrap();
        with_activity(&cache, &recipient, &record.rel, |path| {
            assert!(!path.exists());
            // The coordination file exists, but no activity data does yet.
            prune(&cache, SystemTime::now());
            let contender = std::fs::OpenOptions::new()
                .write(true)
                .open(path.with_extension("lock"))?;
            assert!(
                contender.try_lock().is_err(),
                "GC must leave the same locked inode"
            );
            write_atomic(
                path,
                &serde_json::to_vec(&Activity::Pending {
                    record: record.clone(),
                    generation: None,
                })?,
            )?;
            // GC also must not expire a state while its publisher owns it.
            prune(
                &cache,
                SystemTime::now() + RECORD_TTL + Duration::from_secs(1),
            );
            assert!(path.is_file());
            Ok(())
        })
        .unwrap();
        let prepared = prepare_stage(&record, &blob_path).unwrap().unwrap();
        publish_stage(&cache, &recipient, &record, prepared, &None).unwrap();
        let staged = staged_path(&record.dest, &hash).unwrap();
        assert!(staged.is_file() && fingerprint_path(&staged).is_file());
        let completed = recipient.join("debug/deps/app-bbbbbbbbbbbbbbbb");
        std::fs::write(&completed, b"completed").unwrap();
        complete(&cache, &recipient, &completed, &None);
        assert!(!staged.exists() && !fingerprint_path(&staged).exists());
    }

    /// A staged copy written to after staging, or staged for another blob,
    /// is never used, and the failed check removes it.
    #[test]
    fn a_touched_or_foreign_staged_copy_is_refused_and_removed() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let target = target(dir.path(), "a");
        let (blob_path, hash) = blob(dir.path(), b"an executable");
        let dest = target.join("debug/deps/app-0123456789abcdef");
        remember(&cache, &target, &dest, &hash, MIN_BYTES);

        stage_ok(&cache, &target, &blob_path);
        let staged = staged_path(&dest, &hash).unwrap();
        std::fs::write(&staged, b"an executable, stripped").unwrap();
        assert!(!take(&dest, &hash));
        assert!(!dest.exists() && !staged.exists());
        assert!(!fingerprint_path(&staged).exists());

        stage_ok(&cache, &target, &blob_path);
        let other = "f".repeat(64);
        assert!(
            !take(&dest, &other),
            "a copy of another blob is not this one"
        );
        assert!(staged.exists(), "and the copy for this blob stays");
        assert!(take(&dest, &hash));
    }

    /// A blob whose bytes do not hash to its name is never staged, and a
    /// fingerprint whose copy is gone does not block a new copy.
    #[test]
    fn staging_checks_the_bytes_and_replaces_an_orphan_fingerprint() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let target = target(dir.path(), "a");
        let (blob_path, hash) = blob(dir.path(), b"an executable");
        let dest = target.join("debug/deps/app-0123456789abcdef");
        remember(&cache, &target, &dest, &hash, MIN_BYTES);

        std::fs::write(&blob_path, b"torn bytes!!!").unwrap();
        let results = stage_pass(&cache, &target, |_| blob_path.clone()).unwrap();
        assert!(
            matches!(&results[..], [(_, Err(error))] if error.to_string().contains("reads back as")),
            "{results:?}"
        );
        let staged = staged_path(&dest, &hash).unwrap();
        assert!(!staged.exists() && !fingerprint_path(&staged).exists());
        let leftovers = std::fs::read_dir(dest.parent().unwrap()).unwrap().count();
        assert_eq!(leftovers, 0, "the temporary copy is removed too");

        std::fs::write(&blob_path, b"an executable").unwrap();
        std::fs::write(fingerprint_path(&staged), b"{}").unwrap();
        stage_ok(&cache, &target, &blob_path);
        assert!(take(&dest, &hash));
    }

    /// Small executables and relative targets are not recorded, an unchanged
    /// record is not rewritten, a new metadata hash replaces the artifact's
    /// record, and a present destination is not staged.
    #[test]
    fn records_are_one_per_artifact() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let target = target(dir.path(), "a");
        let (blob_path, hash) = blob(dir.path(), b"an executable");
        let dest = target.join("debug/deps/app-0123456789abcdef");

        remember(&cache, &target, &dest, &hash, MIN_BYTES - 1);
        assert!(!records_dir(&cache, &target).exists());
        let relative = Path::new("relative");
        remember(&cache, relative, &relative.join("x"), &hash, MIN_BYTES);
        assert!(!prestage_dir(&cache).exists());

        remember(&cache, &target, &dest, &hash, MIN_BYTES);
        let records = || read_records(&records_dir(&cache, &target));
        let (path, _) = records().pop().unwrap();
        let written = std::fs::metadata(&path).unwrap().modified().unwrap();
        std::thread::sleep(Duration::from_millis(20));
        remember(&cache, &target, &dest, &hash, MIN_BYTES);
        assert_eq!(
            std::fs::metadata(&path).unwrap().modified().unwrap(),
            written
        );

        let rebuilt = target.join("debug/deps/app-fedcba9876543210");
        remember(&cache, &target, &rebuilt, &hash, MIN_BYTES);
        let records = records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].1.dest, rebuilt);

        std::fs::write(&rebuilt, b"already built").unwrap();
        stage(&cache, &target, |_| blob_path.clone());
        assert!(!staged_path(&rebuilt, &hash).unwrap().exists());
    }

    #[test]
    fn durations_are_pinned() {
        assert_eq!(MIN_BYTES, 16 * 1024 * 1024);
        assert_eq!(HINT_INTERVAL, Duration::from_secs(30));
        assert_eq!(STALE_AFTER, Duration::from_secs(3600));
        assert_eq!(RECORD_TTL, Duration::from_secs(2_592_000));
    }

    #[test]
    fn elapsed_beyond_is_strict() {
        let at = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        let age = Duration::from_secs(10);
        assert!(!elapsed_beyond(at, age, at + age));
        assert!(elapsed_beyond(at, age, at + age + Duration::from_micros(1)));
        assert!(
            !elapsed_beyond(at, age, at - Duration::from_secs(1)),
            "future"
        );
    }

    /// A missing blob stages nothing and is not an error: GC may have taken
    /// it since the record was written.
    #[test]
    fn a_missing_blob_is_skipped_quietly() {
        let dir = tempfile::tempdir().unwrap();
        let deps = target(dir.path(), "a").join("debug/deps");
        let dest = deps.join("app-0123456789abcdef");
        let record = Record {
            dest: dest.clone(),
            rel: PathBuf::from("debug/deps/app-0123456789abcdef"),
            blob: "a".repeat(64),
            size: MIN_BYTES,
        };
        assert!(stage_one(&record, &dir.path().join("gone")).is_ok());
        assert_eq!(std::fs::read_dir(&deps).unwrap().count(), 0);
    }

    /// The project's `latest` pointer follows the target directory that
    /// recorded last, so a third checkout borrows from the newest.
    #[test]
    fn the_latest_pointer_follows_the_newest_recording() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let first = target(dir.path(), "a");
        let second = target(dir.path(), "b");
        let third = target(dir.path(), "c");
        let rel = Path::new("debug/deps/app-0123456789abcdef");
        let hash = "a".repeat(64);
        remember(&cache, &first, &first.join(rel), &hash, MIN_BYTES);
        assert_eq!(lender(&cache, &third), Some(first.clone()));
        remember(&cache, &second, &second.join(rel), &hash, MIN_BYTES);
        assert_eq!(lender(&cache, &third), Some(second.clone()));
        assert_eq!(lender(&cache, &second), None, "not its own lender");
    }

    #[test]
    fn artifact_keys_drop_the_metadata_hash_only() {
        for (rel, key) in [
            ("debug/deps/hk-0123456789abcdef", "debug/deps/hk"),
            ("debug/deps/hk-0123456789abcdef.exe", "debug/deps/hk.exe"),
            ("debug/deps/my-tool-0123456789abcdef", "debug/deps/my-tool"),
            ("debug/deps/my-tool", "debug/deps/my-tool"),
            ("debug/deps/hk-0123", "debug/deps/hk-0123"),
            ("debug/v1.2/hk-0123456789abcdef", "debug/v1.2/hk"),
            ("debug/deps/.tool-0123456789abcdef", "debug/deps/.tool"),
            (".tool-0123456789abcdef", ".tool"),
        ] {
            assert_eq!(artifact_key(Path::new(rel)), key, "{rel}");
        }
    }

    /// A hint goes out when something is recorded, then not again within the
    /// interval.
    #[test]
    fn a_hint_is_due_once_per_interval_and_only_with_records() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let target = target(dir.path(), "a");
        let now = SystemTime::now();
        assert!(!hint_due(&cache, &target, now), "nothing recorded");
        remember(
            &cache,
            &target,
            &target.join("app"),
            &"a".repeat(64),
            MIN_BYTES,
        );
        assert!(hint_due(&cache, &target, now));
        assert!(!hint_due(&cache, &target, now), "just sent");
        assert!(hint_due(
            &cache,
            &target,
            now + HINT_INTERVAL + Duration::from_secs(1)
        ));
    }

    /// A hint is recent for strictly less than the interval: at exactly the
    /// interval the next one is due.
    #[test]
    fn a_hint_is_due_again_at_exactly_the_interval() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let target = target(dir.path(), "a");
        remember(
            &cache,
            &target,
            &target.join("app"),
            &"a".repeat(64),
            MIN_BYTES,
        );
        assert!(hint_due(&cache, &target, SystemTime::now()));
        let sent = std::fs::metadata(records_dir(&cache, &target).join(HINT_MARKER))
            .unwrap()
            .modified()
            .unwrap();
        assert!(!hint_due(
            &cache,
            &target,
            sent + HINT_INTERVAL - Duration::from_micros(1)
        ));
        assert!(hint_due(&cache, &target, sent + HINT_INTERVAL));
    }

    /// `maybe_hint` marks the hint as sent, daemon or not.
    #[test]
    fn maybe_hint_marks_its_hint() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(dir.path().join("cache"));
        let target = target(dir.path(), "a");
        remember(
            &config.cache_dir,
            &target,
            &target.join("app"),
            &"a".repeat(64),
            MIN_BYTES,
        );
        let marker = records_dir(&config.cache_dir, &target).join(HINT_MARKER);
        assert!(!marker.exists());
        maybe_hint(&config, &target);
        assert!(marker.exists());
    }

    /// A new target directory borrows the records of its project's target
    /// directory that recorded last, under its own root, and stops borrowing
    /// once it has its own. Another project, or a target directory with no
    /// `Cargo.toml` beside it, borrows nothing.
    #[test]
    fn a_new_target_borrows_from_its_own_project_only() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let first = target(dir.path(), "a");
        let second = target(dir.path(), "b");
        let unrelated = checkout(dir.path(), "c", "other");
        let elsewhere = dir.path().join("shared-target");
        std::fs::create_dir_all(elsewhere.join("debug/deps")).unwrap();
        let (blob_path, hash) = blob(dir.path(), b"an executable");
        let rel = Path::new("debug/deps/app-0123456789abcdef");
        let now = SystemTime::now();

        assert!(!has_predictions(&cache, &first));
        assert!(!has_predictions(&cache, &second));
        assert!(!hint_due(&cache, &second, now), "nothing to borrow yet");
        remember(&cache, &first, &first.join(rel), &hash, MIN_BYTES);
        assert!(
            has_predictions(&cache, &first),
            "a real own record is eligible"
        );
        assert!(
            !has_predictions(&cache, &second),
            "an unhinted borrower is not yet eligible"
        );
        assert!(hint_due(&cache, &second, now), "the second borrows");
        assert!(
            has_predictions(&cache, &second),
            "a borrowed hint is eligible"
        );
        for other in [&unrelated, &elsewhere] {
            assert!(!hint_due(&cache, other, now));
            assert!(!has_predictions(&cache, other));
            stage(&cache, other, |_| blob_path.clone());
            assert!(!staged_path(&other.join(rel), &hash).unwrap().exists());
        }

        stage(&cache, &second, |_| blob_path.clone());
        assert!(take(&second.join(rel), &hash));
        assert_eq!(std::fs::read(second.join(rel)).unwrap(), b"an executable");
        assert!(!first.join(rel).exists(), "the lender is left alone");

        // Once the second target records for itself, its own records win.
        let (other_path, other) = blob(dir.path(), b"another executable");
        let own = second.join("debug/deps/tool-0123456789abcdef");
        remember(&cache, &second, &own, &other, MIN_BYTES);
        std::fs::remove_file(second.join(rel)).unwrap();
        stage(&cache, &second, |blob| {
            if blob == other {
                other_path.clone()
            } else {
                blob_path.clone()
            }
        });
        assert!(staged_path(&own, &other).unwrap().is_file());
        assert!(!staged_path(&second.join(rel), &hash).unwrap().exists());
    }

    /// Staged copies left for an hour are removed by the next pass.
    #[test]
    fn stale_staged_copies_are_swept() {
        let dir = tempfile::tempdir().unwrap();
        let old = dir.path().join(format!("{STAGED_PREFIX}abc-app"));
        let kept = dir.path().join("app");
        std::fs::write(&old, b"x").unwrap();
        std::fs::write(&kept, b"x").unwrap();
        sweep_stale(dir.path(), SystemTime::now());
        assert!(old.exists(), "fresh");
        sweep_stale(
            dir.path(),
            SystemTime::now() + STALE_AFTER + Duration::from_secs(1),
        );
        assert!(!old.exists() && kept.exists());
    }

    /// GC drops records of deleted target directories, records nobody
    /// rewrote for the TTL, and pointers to deleted targets.
    #[test]
    fn prune_drops_gone_and_expired_records() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let kept = target(dir.path(), "a");
        let gone = checkout(dir.path(), "b", "other");
        let kept_project = project(&kept).unwrap();
        let gone_project = project(&gone).unwrap();
        let hash = "a".repeat(64);
        let rel = Path::new("debug/deps/app-0123456789abcdef");
        remember(&cache, &kept, &kept.join(rel), &hash, MIN_BYTES);
        remember(&cache, &gone, &gone.join(rel), &hash, MIN_BYTES);
        std::fs::remove_dir_all(gone.parent().unwrap()).unwrap();

        let now = SystemTime::now();
        assert_eq!(
            prune(&cache, now),
            2,
            "the gone target's record and pointer"
        );
        assert!(!records_dir(&cache, &gone).exists());
        assert!(!latest_path(&cache, &gone_project).exists());
        assert_eq!(read_records(&records_dir(&cache, &kept)).len(), 1);

        assert_eq!(prune(&cache, now + RECORD_TTL + Duration::from_secs(1)), 1);
        assert!(!records_dir(&cache, &kept).exists());
        assert!(
            latest_path(&cache, &kept_project).exists(),
            "the target is still there"
        );
    }
}
