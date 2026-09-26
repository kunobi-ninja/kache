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
    std::fs::write(&marker, b"").is_ok()
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
    let directory = records_dir(cache_dir, target_dir);
    let Ok(lock) = std::fs::create_dir_all(&directory).and_then(|()| {
        std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(directory.join(STAGING_LOCK))
    }) else {
        return;
    };
    if lock.try_lock().is_err() {
        return;
    }
    for record in records_for(cache_dir, target_dir) {
        if let Err(error) = stage_one(&record, &blob_path(&record.blob)) {
            tracing::debug!("not staging {}: {error:#}", record.dest.display());
        }
    }
}

fn stage_one(record: &Record, blob: &Path) -> anyhow::Result<()> {
    let Some(directory) = record.dest.parent() else {
        return Ok(());
    };
    sweep_stale(directory, SystemTime::now());
    if !record.dest.is_absolute() || record.dest.exists() || !directory.is_dir() || !blob.is_file()
    {
        return Ok(());
    }
    let staged = staged_path(&record.dest, &record.blob)
        .ok_or_else(|| anyhow::anyhow!("no staging name"))?;
    let fingerprint = fingerprint_path(&staged);
    if fingerprint.is_file() {
        if staged.is_file() {
            return Ok(());
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
    temporary.persist(&staged)?;
    let staged_file = Staged {
        blob: record.blob.clone(),
        fingerprint: FileFingerprint::from_path(&staged)?,
    };
    write_atomic(&fingerprint, &serde_json::to_vec(&staged_file)?)?;
    Ok(())
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
        if read_records(&path).is_empty() {
            let _ = std::fs::remove_dir_all(&path);
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

        stage(&cache, &target, |_| blob_path.clone());
        let staged = staged_path(&dest, &hash).unwrap();
        std::fs::write(&staged, b"an executable, stripped").unwrap();
        assert!(!take(&dest, &hash));
        assert!(!dest.exists() && !staged.exists());
        assert!(!fingerprint_path(&staged).exists());

        stage(&cache, &target, |_| blob_path.clone());
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
        stage(&cache, &target, |_| blob_path.clone());
        let staged = staged_path(&dest, &hash).unwrap();
        assert!(!staged.exists() && !fingerprint_path(&staged).exists());
        let leftovers = std::fs::read_dir(dest.parent().unwrap()).unwrap().count();
        assert_eq!(leftovers, 0, "the temporary copy is removed too");

        std::fs::write(&blob_path, b"an executable").unwrap();
        std::fs::write(fingerprint_path(&staged), b"{}").unwrap();
        stage(&cache, &target, |_| blob_path.clone());
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
        assert!(elapsed_beyond(at, age, at + age + Duration::from_nanos(1)));
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

        assert!(!hint_due(&cache, &second, now), "nothing to borrow yet");
        remember(&cache, &first, &first.join(rel), &hash, MIN_BYTES);
        assert!(hint_due(&cache, &second, now), "the second borrows");
        for other in [&unrelated, &elsewhere] {
            assert!(!hint_due(&cache, other, now));
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
