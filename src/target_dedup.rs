//! Target files that are already stored blobs.
//!
//! Kache remaps paths into the artifacts it compiles, and the store keeps
//! those bytes. This pass replaces a target file with a clone of the blob
//! when the file is a copy of that artifact: another worktree, or a target
//! restored from the same blobs. The path Cargo opens does not move, and the
//! file keeps its mode and modification time.
//!
//! Comparison is a pipeline. A file whose length matches no blob is finished
//! after a `stat`. A file that differs in its first or last block is finished
//! after those two reads. Only a survivor is hashed, and only once: the hash
//! is stored against the file's stamp (path, size, modification time, change
//! time, inode). The next look at an unchanged stamp reads nothing.
//!
//! `kache targets share` reports unless `--apply` is set. With `[cache]
//! auto_share_target_files` on (the default), the daemon runs one short slice
//! of the same pass on a quiet machine. A wrapper request or a held compile
//! permit ends the slice between files. `incremental/` and `.fingerprint/`
//! are not entered. A hardlink is never used.

use crate::config::Config;
use crate::maintenance::{Trigger, is_quiet};
use anyhow::{Context, Result};
use kache_store::file_hash::{FileFingerprint, hash_file};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Files smaller than this are not worth a clone. This is also the smallest
/// file the hash memo will remember.
pub(crate) const MIN_BYTES: u64 = 64 * 1024;

/// Bytes compared at each end before a full hash.
const BLOCK: usize = 4096;

/// How many files one idle slice will stat. Content reads are capped apart
/// from this.
const EXAMINE_PER_SLICE: usize = 512;

/// First-and-last comparisons in one idle slice.
const ENDS_PER_SLICE: usize = 16;

/// Full hashes in one idle slice. A match still reads the whole file; one
/// of those is enough work while the machine is idle.
const HASH_PER_SLICE: usize = 1;

/// Stop the cheap part of a slice after this long, even if files remain.
const SLICE_BUDGET: Duration = Duration::from_millis(200);

/// A length shared by more blobs than this is not used. Keeping a partial
/// list would reject a file that matched a blob we dropped.
const MAX_BLOBS_PER_SIZE: usize = 8;

/// Reject rows. A full hash is stored in the file-hash memo instead, because
/// that hash is the real content.
const REJECT_SIZE: i64 = 1;
const REJECT_ENDS: i64 = 2;
const REJECT_DEVICE: i64 = 3;

/// An hour after a walk finishes, before the daemon starts again at the
/// first file. Stamps cover the files already decided.
const CYCLE_PAUSE: Duration = Duration::from_secs(3600);

const DB_FILE: &str = "target-dedup.db";

/// A directory component a scan does not enter.
pub(crate) fn skipped_dir(name: &std::ffi::OsStr) -> bool {
    name == "incremental" || name == ".fingerprint"
}

/// Extensions whose bytes are compiler outputs rather than files Cargo rewrites
/// in place on every unit.
pub(crate) fn artifact_extension(ext: Option<&str>) -> bool {
    matches!(
        ext,
        Some("rlib" | "rmeta" | "so" | "dylib" | "a" | "o" | "dll" | "lib" | "exe" | "pdb")
    )
}

/// Whether `relative` (from the target root) is a file this pass may clone.
pub(crate) fn candidate(relative: &Path, len: u64) -> bool {
    len >= MIN_BYTES
        && !relative
            .components()
            .any(|component| matches!(component, Component::Normal(name) if skipped_dir(name)))
        && artifact_extension(relative.extension().and_then(|ext| ext.to_str()))
}

/// Both ends have to match. Either side alone is a different file.
pub(crate) fn ends_match(
    file_head: &[u8],
    file_tail: &[u8],
    blob_head: &[u8],
    blob_tail: &[u8],
) -> bool {
    file_head == blob_head && file_tail == blob_tail
}

/// A full hash is the last stage. It runs only after the length and both ends
/// have matched a blob.
#[cfg(test)]
fn needs_full_hash(size_known: bool, ends_matched: bool) -> bool {
    size_known && ends_matched
}

/// A clone is allowed only for a proven match on the same volume, and only
/// while nothing is using the target.
pub(crate) fn may_replace(matched: bool, same_device: bool, busy: bool) -> bool {
    matched && same_device && !busy
}

/// Profile directory whose `.cargo-lock` covers `file` inside `target`.
pub(crate) fn profile_dir(target: &Path, file: &Path) -> Option<PathBuf> {
    let parent = file.parent()?;
    let profile = if parent.file_name() == Some(std::ffi::OsStr::new("deps")) {
        parent.parent()?
    } else {
        parent
    };
    profile.starts_with(target).then(|| profile.to_path_buf())
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Ends {
    head: [u8; BLOCK],
    tail: [u8; BLOCK],
}

struct Blob {
    hash: String,
    path: PathBuf,
    /// Volume identity. `None` when the platform did not report one.
    device: Option<u64>,
}

enum SizeClass {
    Few(Vec<Blob>),
    /// More blobs share this length than [`MAX_BLOBS_PER_SIZE`]. Those files
    /// are left for a later change of the index, not rejected.
    Many,
}

struct BlobIndex {
    by_size: HashMap<u64, SizeClass>,
}

impl BlobIndex {
    fn get(&self, size: u64) -> Option<&SizeClass> {
        self.by_size.get(&size)
    }
}

struct Limits {
    examine: usize,
    ends: usize,
    hashes: usize,
    budget: Duration,
}

struct Counters {
    examined: usize,
    ends: usize,
    hashes: usize,
    started: Instant,
}

/// What one pass found. A pass stops when its limits are spent or the walk
/// reaches the end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Report {
    pub targets: usize,
    pub examined: usize,
    pub remembered: usize,
    pub size_rejected: usize,
    pub ends_rejected: usize,
    pub cross_device: usize,
    pub hashed: usize,
    pub matches: usize,
    pub matched_bytes: u64,
    pub already_shared: usize,
    pub replaced: usize,
    pub failed: usize,
    pub skipped_busy: usize,
    pub exhausted: bool,
}

impl Report {
    fn new(targets: usize) -> Self {
        Self {
            targets,
            examined: 0,
            remembered: 0,
            size_rejected: 0,
            ends_rejected: 0,
            cross_device: 0,
            hashed: 0,
            matches: 0,
            matched_bytes: 0,
            already_shared: 0,
            replaced: 0,
            failed: 0,
            skipped_busy: 0,
            exhausted: false,
        }
    }
}

/// Scan tracked targets, or `paths` when those are given. Rewrites only when
/// `apply` is set.
pub fn run(config: &Config, paths: &[PathBuf], apply: bool, json: bool) -> Result<()> {
    let roots = resolve_roots(config, paths)?;
    let mut totals = Report::new(roots.len());
    if roots.is_empty() {
        for line in present(&totals, apply, json)? {
            println!("{line}");
        }
        return Ok(());
    }
    let store_dir = config.store_dir();
    let store = crate::store::Store::open(config)?;
    let index = load_index(&store, &store_dir)?;
    let mut db = PipelineDb::open(&config.cache_dir)?;
    let limits = Limits {
        examine: usize::MAX,
        ends: usize::MAX,
        hashes: usize::MAX,
        budget: Duration::from_secs(86_400),
    };
    // The command walks until the cursor comes back around. Each pass is
    // still the same stages; the loop only continues a cursor.
    loop {
        let quiet = true;
        let report = one_pass(
            &store,
            &store_dir,
            &index,
            &mut db,
            &roots,
            apply,
            &limits,
            &mut |_| quiet,
        )?;
        fold(&mut totals, &report);
        if report.exhausted {
            break;
        }
    }
    for line in present(&totals, apply, json)? {
        println!("{line}");
    }
    Ok(())
}

/// One idle slice over the tracked targets, when `auto_share_target_files`
/// is on and the machine has been quiet.
pub(crate) fn idle_slice(config: &Config, trigger: Trigger<'_>) {
    if !config.auto_share_target_files {
        return;
    }
    let Trigger::Periodic(clock) = trigger else {
        return;
    };
    if !quiet_now(config, clock) {
        return;
    }
    let Ok(store) = crate::store::Store::open(config) else {
        return;
    };
    let Ok(roots) = tracked_roots(&store) else {
        return;
    };
    if roots.is_empty() {
        return;
    }
    let Ok(mut db) = PipelineDb::open(&config.cache_dir) else {
        return;
    };
    if db.cycle_paused(unix_now()) {
        return;
    }
    let store_dir = config.store_dir();
    let Ok(index) = load_index(&store, &store_dir) else {
        return;
    };
    let limits = Limits {
        examine: EXAMINE_PER_SLICE,
        ends: ENDS_PER_SLICE,
        hashes: HASH_PER_SLICE,
        budget: SLICE_BUDGET,
    };
    let result = one_pass(
        &store,
        &store_dir,
        &index,
        &mut db,
        &roots,
        true,
        &limits,
        &mut |_| quiet_now(config, clock),
    );
    match result {
        Ok(report) => {
            if report.exhausted {
                db.mark_cycle(unix_now());
            }
            if let Some(note) = share_note(report.replaced, report.matched_bytes) {
                tracing::info!("{note}");
            }
        }
        Err(error) => tracing::warn!("target dedup slice failed: {error:#}"),
    }
}

fn quiet_now(config: &Config, clock: &crate::maintenance::RequestClock) -> bool {
    let permits = crate::scheduler::permits_in_use(&config.cache_dir);
    is_quiet(permits, clock.idle_for())
}

fn fold(totals: &mut Report, report: &Report) {
    totals.examined = totals.examined.saturating_add(report.examined);
    totals.remembered = totals.remembered.saturating_add(report.remembered);
    totals.size_rejected = totals.size_rejected.saturating_add(report.size_rejected);
    totals.ends_rejected = totals.ends_rejected.saturating_add(report.ends_rejected);
    totals.cross_device = totals.cross_device.saturating_add(report.cross_device);
    totals.hashed = totals.hashed.saturating_add(report.hashed);
    totals.matches = totals.matches.saturating_add(report.matches);
    totals.matched_bytes = totals.matched_bytes.saturating_add(report.matched_bytes);
    totals.already_shared = totals.already_shared.saturating_add(report.already_shared);
    totals.replaced = totals.replaced.saturating_add(report.replaced);
    totals.failed = totals.failed.saturating_add(report.failed);
    totals.skipped_busy = totals.skipped_busy.saturating_add(report.skipped_busy);
    totals.exhausted = report.exhausted;
}

/// One line of the text report. `None` when nothing was rewritten.
fn share_note(replaced: usize, matched_bytes: u64) -> Option<String> {
    if replaced == 0 {
        return None;
    }
    Some(format!(
        "cloned {replaced} stored artifacts into old target directories (about {} now shared)",
        bytesize::ByteSize(matched_bytes),
    ))
}

fn present(report: &Report, apply: bool, json: bool) -> Result<Vec<String>> {
    if json {
        #[derive(serde::Serialize)]
        struct Body {
            targets: usize,
            examined: usize,
            remembered: usize,
            size_rejected: usize,
            ends_rejected: usize,
            cross_device: usize,
            hashed: usize,
            matches: usize,
            matched_bytes: u64,
            already_shared: usize,
            replaced: usize,
            failed: usize,
            skipped_busy: usize,
            applied: bool,
        }
        crate::machine::emit(
            "targets-share",
            Body {
                targets: report.targets,
                examined: report.examined,
                remembered: report.remembered,
                size_rejected: report.size_rejected,
                ends_rejected: report.ends_rejected,
                cross_device: report.cross_device,
                hashed: report.hashed,
                matches: report.matches,
                matched_bytes: report.matched_bytes,
                already_shared: report.already_shared,
                replaced: report.replaced,
                failed: report.failed,
                skipped_busy: report.skipped_busy,
                applied: apply,
            },
            Vec::new(),
        )?;
        return Ok(Vec::new());
    }
    Ok(render(report, apply))
}

fn render(report: &Report, apply: bool) -> Vec<String> {
    let mut lines = vec![
        format!(
            "{} target director{}, {} files examined",
            report.targets,
            if report.targets == 1 { "y" } else { "ies" },
            report.examined,
        ),
        format!(
            "remembered: {}, length misses: {}, end misses: {}, other volume: {}",
            report.remembered, report.size_rejected, report.ends_rejected, report.cross_device,
        ),
        format!(
            "hashed: {}, matching the store: {} ({})",
            report.hashed,
            report.matches,
            bytesize::ByteSize(report.matched_bytes),
        ),
        format!(
            "already sharing blocks: {}, busy targets skipped: {}",
            report.already_shared, report.skipped_busy,
        ),
    ];
    if report.replaced > 0 || report.failed > 0 {
        lines.push(format!(
            "replaced {} files, {} failed",
            report.replaced, report.failed,
        ));
    } else if report.matched_bytes > 0 && !apply {
        lines.push("nothing rewritten; pass --apply to clone these files".to_string());
    }
    lines
}

fn resolve_roots(config: &Config, paths: &[PathBuf]) -> Result<Vec<PathBuf>> {
    if !paths.is_empty() {
        let mut roots = Vec::new();
        for path in paths {
            let meta =
                fs::metadata(path).with_context(|| format!("cannot read {}", path.display()))?;
            if !meta.is_dir() {
                anyhow::bail!("{} is not a directory", path.display());
            }
            roots.push(std::path::absolute(path).unwrap_or_else(|_| path.clone()));
        }
        roots.sort();
        return Ok(roots);
    }
    let store = crate::store::Store::open(config)?;
    crate::worktree_discovery::discover(&store)?;
    tracked_roots(&store)
}

fn tracked_roots(store: &crate::store::Store) -> Result<Vec<PathBuf>> {
    let mut roots = Vec::new();
    for tracked in store.tracked_target_roots(0)? {
        if tracked.path.is_dir() {
            roots.push(tracked.path);
        }
    }
    roots.sort();
    roots.dedup();
    Ok(roots)
}

#[allow(clippy::too_many_arguments)]
fn one_pass(
    store: &crate::store::Store,
    store_dir: &Path,
    index: &BlobIndex,
    db: &mut PipelineDb,
    roots: &[PathBuf],
    apply: bool,
    limits: &Limits,
    keep_going: &mut dyn FnMut(&Path) -> bool,
) -> Result<Report> {
    let mut report = Report::new(roots.len());
    let mut counters = Counters {
        examined: 0,
        ends: 0,
        hashes: 0,
        started: Instant::now(),
    };
    let mut ends_cache: HashMap<String, Ends> = HashMap::new();
    let mut held: HashMap<PathBuf, File> = HashMap::new();
    let cursor = db.cursor();
    let mut last_done = cursor.clone();
    let mut progressed = false;
    let mut exhausted = true;
    for root in roots {
        if !cursor_before_root(&cursor, root) {
            continue;
        }
        if !keep_going(root) {
            exhausted = false;
            break;
        }
        if apply && another_build_holds(root, &held) {
            report.skipped_busy = report.skipped_busy.saturating_add(1);
            // A busy target is not a finished walk. Marking the cycle here
            // would wait an hour before these files were considered again.
            exhausted = false;
            break;
        }
        let mut files = Vec::new();
        let room = limits.examine.saturating_sub(counters.examined);
        let full = !collect_candidates(root, &cursor, room, &mut files);
        for path in files {
            if !keep_going(&path) {
                db.set_cursor(&last_done);
                report.exhausted = false;
                return Ok(report);
            }
            if counters.examined >= limits.examine || counters.started.elapsed() >= limits.budget {
                db.set_cursor(&last_done);
                report.exhausted = false;
                return Ok(report);
            }
            match consider(
                store,
                store_dir,
                index,
                db,
                root,
                &path,
                apply,
                limits,
                &mut counters,
                &mut ends_cache,
                &mut held,
                &mut report,
            )? {
                Consider::Done => {
                    last_done = path.to_string_lossy().into_owned();
                    progressed = true;
                }
                Consider::Hold => {
                    // This file needs a hash and the slice has spent its one.
                    // Leave the cursor where it is so the next slice hashes it.
                    db.set_cursor(&last_done);
                    report.exhausted = false;
                    return Ok(report);
                }
            }
        }
        if full {
            exhausted = false;
            break;
        }
    }
    if exhausted {
        db.set_cursor("");
    } else if progressed {
        db.set_cursor(&last_done);
    }
    report.exhausted = exhausted;
    Ok(report)
}

enum Consider {
    Done,
    /// Do not advance the cursor past this file.
    Hold,
}

#[allow(clippy::too_many_arguments)]
fn consider(
    store: &crate::store::Store,
    store_dir: &Path,
    index: &BlobIndex,
    db: &mut PipelineDb,
    root: &Path,
    path: &Path,
    apply: bool,
    limits: &Limits,
    counters: &mut Counters,
    ends_cache: &mut HashMap<String, Ends>,
    held: &mut HashMap<PathBuf, File>,
    report: &mut Report,
) -> Result<Consider> {
    let meta = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(_) => return Ok(Consider::Done),
    };
    if !meta.is_file() {
        return Ok(Consider::Done);
    }
    let relative = path.strip_prefix(root).unwrap_or(path);
    if !candidate(relative, meta.len()) {
        return Ok(Consider::Done);
    }
    counters.examined = counters.examined.saturating_add(1);
    report.examined = report.examined.saturating_add(1);
    let Ok(stamp) = FileFingerprint::from_path(path) else {
        return Ok(Consider::Done);
    };
    if let Some(hash) = store.file_hash_cache().get(&stamp).ok().flatten() {
        report.remembered = report.remembered.saturating_add(1);
        if blob_file(store_dir, &hash).is_file() {
            account_match(
                store,
                store_dir,
                path,
                &hash,
                meta.len(),
                apply,
                root,
                held,
                report,
            );
        }
        return Ok(Consider::Done);
    }
    if db.rejected(&stamp) {
        report.remembered = report.remembered.saturating_add(1);
        return Ok(Consider::Done);
    }
    let Some(class) = index.get(meta.len()) else {
        db.reject(&stamp, REJECT_SIZE);
        report.size_rejected = report.size_rejected.saturating_add(1);
        return Ok(Consider::Done);
    };
    let SizeClass::Few(blobs) = class else {
        return Ok(Consider::Done);
    };
    let device = device_of(path, &meta);
    if blobs.iter().all(|blob| !same_volume(blob.device, device)) {
        db.reject(&stamp, REJECT_DEVICE);
        report.cross_device = report.cross_device.saturating_add(1);
        return Ok(Consider::Done);
    }
    if counters.ends >= limits.ends {
        return Ok(Consider::Hold);
    }
    let file_ends = match read_ends(path) {
        Ok(ends) => ends,
        Err(error) => {
            tracing::debug!("skipping {}: {error}", path.display());
            report.failed = report.failed.saturating_add(1);
            return Ok(Consider::Done);
        }
    };
    counters.ends = counters.ends.saturating_add(1);
    let mut matched = false;
    let mut compared = false;
    for blob in blobs {
        if !same_volume(blob.device, device) {
            continue;
        }
        let blob_ends = match cached_ends(ends_cache, blob) {
            Ok(ends) => ends,
            Err(error) => {
                tracing::debug!("skipping blob {}: {error}", blob.path.display());
                continue;
            }
        };
        compared = true;
        if ends_match(
            &file_ends.head,
            &file_ends.tail,
            &blob_ends.head,
            &blob_ends.tail,
        ) {
            matched = true;
            break;
        }
    }
    if !matched {
        // An unreadable blob is not a difference. Leave the stamp unset so
        // the next wake can try it again.
        if compared {
            db.reject(&stamp, REJECT_ENDS);
            report.ends_rejected = report.ends_rejected.saturating_add(1);
        } else {
            report.failed = report.failed.saturating_add(1);
        }
        return Ok(Consider::Done);
    }
    if counters.hashes >= limits.hashes {
        return Ok(Consider::Hold);
    }
    let hash = match hash_file(path) {
        Ok(hash) => hash,
        Err(error) => {
            tracing::debug!("could not hash {}: {error}", path.display());
            report.failed = report.failed.saturating_add(1);
            return Ok(Consider::Done);
        }
    };
    counters.hashes = counters.hashes.saturating_add(1);
    report.hashed = report.hashed.saturating_add(1);
    // The hash is the blob's name. The ends only decided this file was worth
    // reading; a different same-sized blob can still be the one that matches.
    store.record_verified_file_hash(&stamp, &hash);
    if blob_file(store_dir, &hash).is_file() {
        account_match(
            store,
            store_dir,
            path,
            &hash,
            meta.len(),
            apply,
            root,
            held,
            report,
        );
    }
    Ok(Consider::Done)
}

#[allow(clippy::too_many_arguments)]
fn account_match(
    store: &crate::store::Store,
    store_dir: &Path,
    path: &Path,
    hash: &str,
    len: u64,
    apply: bool,
    root: &Path,
    held: &mut HashMap<PathBuf, File>,
    report: &mut Report,
) {
    let blob = blob_file(store_dir, hash);
    let sharing = crate::sharing::probe(path, len);
    if sharing.private_bytes == 0 {
        report.already_shared = report.already_shared.saturating_add(1);
        report.matches = report.matches.saturating_add(1);
        return;
    }
    report.matches = report.matches.saturating_add(1);
    report.matched_bytes = report.matched_bytes.saturating_add(sharing.private_bytes);
    if !apply {
        return;
    }
    let same_device = same_volume(device_of_path(&blob), device_of_path(path));
    // Locks this pass already holds are not a build. `target_in_use` would
    // see them and refuse every file after the first.
    let busy = another_build_holds(root, held);
    if !may_replace(true, same_device, busy) {
        if busy {
            report.skipped_busy = report.skipped_busy.saturating_add(1);
        } else {
            report.cross_device = report.cross_device.saturating_add(1);
        }
        return;
    }
    let Some(profile) = profile_dir(root, path) else {
        report.failed = report.failed.saturating_add(1);
        return;
    };
    match lock_profile(&profile, held) {
        Ok(true) => {}
        Ok(false) => {
            report.skipped_busy = report.skipped_busy.saturating_add(1);
            return;
        }
        Err(error) => {
            tracing::debug!("could not lock {}: {error}", profile.display());
            report.failed = report.failed.saturating_add(1);
            return;
        }
    }
    match replace_with(&blob, path, &mut |from, to| kache_fs::try_reflink(from, to)) {
        Ok(()) => {
            report.replaced = report.replaced.saturating_add(1);
            // The clone is a new inode. Remember that stamp, or the next
            // wake hashes the file it just rewrote.
            if let Ok(stamp) = FileFingerprint::from_path(path) {
                store.record_verified_file_hash(&stamp, hash);
            }
        }
        Err(error) => {
            tracing::debug!("leaving {} in place: {error}", path.display());
            report.failed = report.failed.saturating_add(1);
        }
    }
}

fn cached_ends<'a>(cache: &'a mut HashMap<String, Ends>, blob: &Blob) -> io::Result<&'a Ends> {
    if !cache.contains_key(&blob.hash) {
        let ends = read_ends(&blob.path)?;
        cache.insert(blob.hash.clone(), ends);
    }
    Ok(cache.get(&blob.hash).expect("ends just inserted"))
}

fn blob_file(store_dir: &Path, hash: &str) -> PathBuf {
    kache_store::blob_path_in_store_dir(store_dir, hash)
}

/// The store's blobs large enough to share, by length, from the index. A
/// blob the index lists but the disk lost fails its end read later and is
/// retried, never taken as a difference.
fn load_index(store: &crate::store::Store, store_dir: &Path) -> Result<BlobIndex> {
    let mut by_size: HashMap<u64, SizeClass> = HashMap::new();
    let blobs_path = store_dir.join("blobs");
    let Ok(blobs_dir) = fs::metadata(&blobs_path) else {
        return Ok(BlobIndex { by_size });
    };
    let device = device_of(&blobs_path, &blobs_dir);
    for (hash, size) in store.blobs_at_least(MIN_BYTES)? {
        let path = blob_file(store_dir, &hash);
        push_blob(&mut by_size, size, Blob { hash, path, device });
    }
    Ok(BlobIndex { by_size })
}

fn push_blob(by_size: &mut HashMap<u64, SizeClass>, size: u64, blob: Blob) {
    let class = by_size
        .entry(size)
        .or_insert_with(|| SizeClass::Few(Vec::new()));
    match class {
        SizeClass::Many => {}
        SizeClass::Few(list) if list.len() < MAX_BLOBS_PER_SIZE => list.push(blob),
        SizeClass::Few(_) => *class = SizeClass::Many,
    }
}

/// Whether a Cargo process holds a lock under `root` that this pass did not
/// take. Depth matches [`crate::cli::target_in_use`].
fn another_build_holds(root: &Path, held: &HashMap<PathBuf, File>) -> bool {
    fn walk(dir: &Path, depth: usize, held: &HashMap<PathBuf, File>) -> bool {
        let Ok(entries) = fs::read_dir(dir) else {
            return false;
        };
        for entry in entries.flatten() {
            let Ok(kind) = entry.file_type() else {
                continue;
            };
            let path = entry.path();
            if kind.is_file() && entry.file_name() == ".cargo-lock" {
                if path
                    .parent()
                    .is_some_and(|profile| held.contains_key(profile))
                {
                    continue;
                }
                let blocked = File::open(&path).is_ok_and(|file| {
                    matches!(file.try_lock(), Err(std::fs::TryLockError::WouldBlock))
                });
                if blocked {
                    return true;
                }
            } else if kind.is_dir() && depth > 0 && walk(&path, depth - 1, held) {
                return true;
            }
        }
        false
    }
    walk(root, 3, held)
}

fn read_ends(path: &Path) -> io::Result<Ends> {
    let len = fs::metadata(path)?.len();
    if len < BLOCK as u64 {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "file is shorter than one block",
        ));
    }
    let mut file = File::open(path)?;
    let mut head = [0u8; BLOCK];
    let mut tail = [0u8; BLOCK];
    file.read_exact(&mut head)?;
    file.seek(SeekFrom::End(-(BLOCK as i64)))?;
    file.read_exact(&mut tail)?;
    Ok(Ends { head, tail })
}

/// Volume that holds this file.
///
/// Unix uses `st_dev` from `meta`. Windows cannot: `volume_serial_number` is
/// still the unstable `windows_by_handle` feature on the 1.98 toolchain, so
/// the serial comes from `GetFileInformationByHandle` on `path`. A missing
/// serial is `None`, not `0`. Unknown files used to compare equal, and a
/// cross-drive copy was hashed and then recorded as a failed clone.
fn device_of(path: &Path, meta: &fs::Metadata) -> Option<u64> {
    #[cfg(unix)]
    {
        let _ = path;
        use std::os::unix::fs::MetadataExt;
        Some(meta.dev())
    }
    #[cfg(windows)]
    {
        let _ = meta;
        windows_volume_serial(path)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (path, meta);
        None
    }
}

/// Volume serial of `path`, including a directory. `None` when the file cannot
/// be opened or the query fails.
///
/// The Win32 call is not in the Linux mutation binary. Skipping it here keeps
/// a replaced body from being reported as a survivor of tests that never
/// compiled it.
#[cfg(windows)]
#[mutants::skip]
fn windows_volume_serial(path: &Path) -> Option<u64> {
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, FILE_FLAG_BACKUP_SEMANTICS, GetFileInformationByHandle,
    };

    let file = OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
        .ok()?;
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    // SAFETY: `file` is open and `info` is a valid output buffer for this call.
    let ok = unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, &mut info) };
    (ok != 0).then_some(u64::from(info.dwVolumeSerialNumber))
}

fn device_of_path(path: &Path) -> Option<u64> {
    fs::metadata(path)
        .ok()
        .and_then(|meta| device_of(path, &meta))
}

/// Both identities known and equal. `None` does not match `None`.
fn same_volume(left: Option<u64>, right: Option<u64>) -> bool {
    matches!((left, right), (Some(left), Some(right)) if left == right)
}

/// `true` when `root` can still hold a file after `cursor`.
fn cursor_before_root(cursor: &str, root: &Path) -> bool {
    if cursor.is_empty() {
        return true;
    }
    let root = root.to_string_lossy();
    path_still_ahead(root.as_ref(), cursor)
}

fn dir_may_contain(dir: &Path, cursor: &str) -> bool {
    if cursor.is_empty() {
        return true;
    }
    let dir = dir.to_string_lossy();
    path_still_ahead(dir.as_ref(), cursor)
}

/// `cursor` is inside `dir`, or a path that sorts at or before `dir`.
///
/// Equality has to reach the comparison. `path_holds` is already true when
/// the strings are equal, and a strict `<` beside that check is the same
/// either way.
fn path_still_ahead(dir: &str, cursor: &str) -> bool {
    if cursor != dir && path_holds(dir, cursor) {
        return true;
    }
    cursor <= dir
}

fn path_holds(dir: &str, cursor: &str) -> bool {
    let Some(rest) = cursor.strip_prefix(dir) else {
        return false;
    };
    rest.is_empty()
        || rest
            .as_bytes()
            .first()
            .is_some_and(|&byte| path_separator(byte))
}

fn path_separator(byte: u8) -> bool {
    // `\` separates paths only where `Path` does. Both checks are compiled
    // everywhere: a Windows-only `==` never changes the Linux mutation binary.
    matches!(byte, b'/') || cfg!(windows) && matches!(byte, b'\\')
}

/// Collect up to `limit` candidate files strictly after `cursor`. `true` means
/// the tree ended before the limit.
fn collect_candidates(root: &Path, cursor: &str, limit: usize, out: &mut Vec<PathBuf>) -> bool {
    if limit == 0 {
        return false;
    }
    walk_candidates(root, root, cursor, limit, out)
}

fn walk_candidates(
    root: &Path,
    dir: &Path,
    cursor: &str,
    limit: usize,
    out: &mut Vec<PathBuf>,
) -> bool {
    if out.len() >= limit {
        return false;
    }
    let Ok(entries) = fs::read_dir(dir) else {
        return true;
    };
    let mut entries: Vec<_> = entries.flatten().collect();
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        if out.len() >= limit {
            return false;
        }
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_symlink() {
            continue;
        }
        let path = entry.path();
        if file_type.is_dir() {
            if skipped_dir(&entry.file_name()) {
                continue;
            }
            if !dir_may_contain(&path, cursor) {
                continue;
            }
            if !walk_candidates(root, &path, cursor, limit, out) {
                return false;
            }
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        let relative = path.strip_prefix(root).unwrap_or(&path);
        if !candidate(relative, meta.len()) {
            continue;
        }
        let key = path.to_string_lossy();
        if !cursor.is_empty() && key.as_ref() <= cursor {
            continue;
        }
        out.push(path);
    }
    true
}

struct PipelineDb {
    conn: Connection,
}

impl PipelineDb {
    fn open(cache_dir: &Path) -> Result<Self> {
        fs::create_dir_all(cache_dir)?;
        let conn = Connection::open(cache_dir.join(DB_FILE))
            .with_context(|| format!("opening {}", cache_dir.join(DB_FILE).display()))?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS reject (
                path TEXT NOT NULL,
                size INTEGER NOT NULL,
                mtime_ns INTEGER NOT NULL,
                ctime_ns INTEGER NOT NULL,
                inode INTEGER NOT NULL,
                stage INTEGER NOT NULL,
                PRIMARY KEY (path, size, mtime_ns, ctime_ns, inode)
            );
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );",
        )?;
        Ok(Self { conn })
    }

    fn cursor(&self) -> String {
        self.meta("cursor").unwrap_or_default()
    }

    fn set_cursor(&self, path: &str) {
        self.set_meta("cursor", path);
    }

    fn rejected(&self, stamp: &FileFingerprint) -> bool {
        self.conn
            .query_row(
                "SELECT 1 FROM reject
                 WHERE path = ?1 AND size = ?2 AND mtime_ns = ?3 AND ctime_ns = ?4 AND inode = ?5",
                params![
                    stamp.path,
                    stamp.size,
                    stamp.mtime_ns,
                    stamp.ctime_ns,
                    stamp.inode
                ],
                |_| Ok(()),
            )
            .optional()
            .ok()
            .flatten()
            .is_some()
    }

    fn reject(&self, stamp: &FileFingerprint, stage: i64) {
        let _ = self.conn.execute(
            "INSERT OR REPLACE INTO reject
                (path, size, mtime_ns, ctime_ns, inode, stage)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                stamp.path,
                stamp.size,
                stamp.mtime_ns,
                stamp.ctime_ns,
                stamp.inode,
                stage
            ],
        );
    }

    fn cycle_paused(&self, now: u64) -> bool {
        self.meta("cycle_finished_at")
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|finished| now.saturating_sub(finished) < CYCLE_PAUSE.as_secs())
    }

    fn mark_cycle(&self, now: u64) {
        self.set_meta("cycle_finished_at", &now.to_string());
    }

    fn meta(&self, key: &str) -> Option<String> {
        self.conn
            .query_row("SELECT value FROM meta WHERE key = ?1", [key], |row| {
                row.get(0)
            })
            .optional()
            .ok()
            .flatten()
    }

    fn set_meta(&self, key: &str, value: &str) {
        let _ = self.conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?1, ?2)",
            params![key, value],
        );
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
}

/// Lock `profile/.cargo-lock` for the rest of this pass. `false` means a
/// build already holds it.
fn lock_profile(profile: &Path, held: &mut HashMap<PathBuf, File>) -> io::Result<bool> {
    if held.contains_key(profile) {
        return Ok(true);
    }
    let lock_path = profile.join(".cargo-lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)?;
    match file.try_lock() {
        Ok(()) => {
            held.insert(profile.to_path_buf(), file);
            Ok(true)
        }
        Err(std::fs::TryLockError::WouldBlock) => Ok(false),
        Err(std::fs::TryLockError::Error(error)) => Err(error),
    }
}

/// Replace `dest` with a clone of `source` placed by `place`. The destination
/// keeps its mode and modification time. A clone whose ends differ from the
/// source leaves `dest` unchanged.
pub(crate) fn replace_with(
    source: &Path,
    dest: &Path,
    place: &mut dyn FnMut(&Path, &Path) -> io::Result<()>,
) -> io::Result<()> {
    let dest_meta = fs::symlink_metadata(dest)?;
    if !dest_meta.is_file() {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "destination is not a regular file",
        ));
    }
    let source_meta = fs::symlink_metadata(source)?;
    if !source_meta.is_file() || source_meta.len() != dest_meta.len() {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "source no longer matches the destination",
        ));
    }
    let source_ends = read_ends(source)?;
    let dest_ends = read_ends(dest)?;
    if !ends_match(
        &dest_ends.head,
        &dest_ends.tail,
        &source_ends.head,
        &source_ends.tail,
    ) {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "content changed before the clone",
        ));
    }
    let parent = dest
        .parent()
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "destination has no parent"))?;
    let temporary = temporary_path(parent, dest)?;
    if let Err(error) = place(source, &temporary) {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    let cloned_ok = fs::symlink_metadata(&temporary)
        .is_ok_and(|meta| meta.is_file() && meta.len() == dest_meta.len())
        && read_ends(&temporary).is_ok_and(|ends| {
            ends_match(&ends.head, &ends.tail, &source_ends.head, &source_ends.tail)
        });
    if !cloned_ok {
        let _ = fs::remove_file(&temporary);
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "clone does not match the source",
        ));
    }
    let modified = filetime::FileTime::from_last_modification_time(&dest_meta);
    let accessed = filetime::FileTime::from_last_access_time(&dest_meta);
    commit_or_abandon(
        &temporary,
        fs::set_permissions(&temporary, dest_meta.permissions()),
    )?;
    commit_or_abandon(
        &temporary,
        filetime::set_file_times(&temporary, accessed, modified),
    )?;
    commit_or_abandon(&temporary, fs::rename(&temporary, dest))?;
    Ok(())
}

/// Run `step`. On failure, remove `temporary` and return the error.
fn commit_or_abandon(temporary: &Path, step: io::Result<()>) -> io::Result<()> {
    if let Err(error) = step {
        let _ = fs::remove_file(temporary);
        return Err(error);
    }
    Ok(())
}

fn temporary_path(parent: &Path, dest: &Path) -> io::Result<PathBuf> {
    let name = dest
        .file_name()
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "destination has no file name"))?;
    let name = name.to_string_lossy();
    for suffix in 0..100 {
        let path = if suffix == 0 {
            parent.join(format!(".{name}.kache-dedup"))
        } else {
            parent.join(format!(".{name}.kache-dedup-{suffix}"))
        };
        if !path.exists() {
            return Ok(path);
        }
    }
    Err(io::Error::new(
        ErrorKind::AlreadyExists,
        "could not choose a temporary clone path",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration as StdDuration;

    fn bytes(len: usize, fill: u8) -> Vec<u8> {
        vec![fill; len]
    }

    /// A stored blob: its file, and its row in the store's index.
    fn write_blob(config: &Config, body: &[u8]) -> String {
        let hash = blake3::hash(body).to_hex().to_string();
        let path = blob_file(&config.store_dir(), &hash);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, body).unwrap();
        crate::store::Store::open(config)
            .unwrap()
            .file_hash_cache()
            .db()
            .execute(
                "INSERT OR REPLACE INTO blobs (hash, size, refcount) VALUES (?1, ?2, 1)",
                params![hash, body.len() as i64],
            )
            .unwrap();
        hash
    }

    fn index(config: &Config) -> BlobIndex {
        load_index(
            &crate::store::Store::open(config).unwrap(),
            &config.store_dir(),
        )
        .unwrap()
    }

    fn track(config: &Config, workspace: &Path) -> PathBuf {
        let target = workspace.join("target");
        fs::create_dir_all(target.join("debug")).unwrap();
        fs::write(
            target.join("CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55\n",
        )
        .unwrap();
        let store = crate::store::Store::open(config).unwrap();
        store.remember_target_root(&target, workspace).unwrap();
        let recorded = tracked_roots(&store).unwrap();
        assert!(
            recorded.iter().any(|path| path == &target),
            "target was not recorded: {recorded:?}"
        );
        target
    }

    fn target_file(root: &Path, name: &str, body: &[u8]) -> PathBuf {
        // `join("debug/deps")` keeps the slash on Windows, while the walk
        // records the backslash `read_dir` returns. The hash is keyed by that
        // string, so the lookup has to use the same components.
        let deps = root.join("debug").join("deps");
        fs::create_dir_all(&deps).unwrap();
        fs::write(
            root.join("CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55\n",
        )
        .unwrap();
        let path = deps.join(name);
        fs::write(&path, body).unwrap();
        path
    }

    #[test]
    fn the_report_offers_apply_only_for_bytes_it_would_free() {
        let mut report = Report::new(1);
        report.matches = 2;
        report.already_shared = 2;
        let shared = render(&report, false).join("\n");
        assert!(!shared.contains("--apply"), "{shared}");
        report.already_shared = 1;
        report.matched_bytes = 4096;
        let private = render(&report, false).join("\n");
        assert!(
            private.contains("pass --apply to clone these files"),
            "{private}"
        );
        assert!(!render(&report, true).join("\n").contains("--apply"));
        report.replaced = 1;
        let applied = render(&report, true).join("\n");
        assert!(applied.contains("replaced 1 files, 0 failed"), "{applied}");
    }

    #[test]
    fn extension_list_accepts_each_artifact_and_rejects_dep_info() {
        for ext in [
            "rlib", "rmeta", "so", "dylib", "a", "o", "dll", "lib", "exe", "pdb",
        ] {
            assert!(artifact_extension(Some(ext)), "{ext}");
        }
        assert!(!artifact_extension(Some("d")));
        assert!(!artifact_extension(Some("rlib-extra")));
        assert!(!artifact_extension(None));
    }

    #[test]
    fn skipped_directories_are_the_mutable_trees_only() {
        assert!(skipped_dir(std::ffi::OsStr::new("incremental")));
        assert!(skipped_dir(std::ffi::OsStr::new(".fingerprint")));
        assert!(!skipped_dir(std::ffi::OsStr::new("deps")));
    }

    #[test]
    fn candidate_requires_size_path_and_extension() {
        let artifact = Path::new("debug/deps/libfoo.rlib");
        assert!(candidate(artifact, MIN_BYTES));
        assert!(!candidate(artifact, MIN_BYTES - 1));
        assert!(!candidate(
            Path::new("debug/incremental/libfoo.rlib"),
            MIN_BYTES
        ));
        assert!(!candidate(
            Path::new("debug/.fingerprint/libfoo.rlib"),
            MIN_BYTES
        ));
        assert!(candidate(
            Path::new("debug/deps/incremental.rlib"),
            MIN_BYTES
        ));
        assert!(!candidate(Path::new("debug/deps/libfoo.d"), MIN_BYTES));
    }

    #[test]
    fn ends_match_requires_both_sides() {
        let head = [1u8; 4];
        let tail = [2u8; 4];
        let other = [3u8; 4];
        assert!(ends_match(&head, &tail, &head, &tail));
        assert!(!ends_match(&other, &tail, &head, &tail));
        assert!(!ends_match(&head, &other, &head, &tail));
        assert!(!ends_match(&other, &other, &head, &tail));
    }

    #[test]
    fn full_hash_waits_for_a_known_length_and_matching_ends() {
        assert!(!needs_full_hash(false, true));
        assert!(!needs_full_hash(true, false));
        assert!(needs_full_hash(true, true));
        assert!(!needs_full_hash(false, false));
    }

    #[test]
    fn replace_requires_a_match_on_the_same_volume_while_idle() {
        assert!(!may_replace(false, true, false));
        assert!(!may_replace(true, false, false));
        assert!(!may_replace(true, true, true));
        assert!(may_replace(true, true, false));
    }

    #[test]
    fn profile_dir_is_the_directory_cargo_locks() {
        let target = Path::new("/work/target");
        assert_eq!(
            profile_dir(target, Path::new("/work/target/debug/deps/libfoo.rlib")).unwrap(),
            PathBuf::from("/work/target/debug")
        );
        assert_eq!(
            profile_dir(
                target,
                Path::new("/work/target/aarch64-apple-darwin/release/deps/libfoo.rlib")
            )
            .unwrap(),
            PathBuf::from("/work/target/aarch64-apple-darwin/release")
        );
        assert!(profile_dir(target, Path::new("/elsewhere/debug/deps/libfoo.rlib")).is_none());
    }

    #[test]
    fn a_length_past_the_cap_is_not_a_partial_list() {
        let mut by_size = HashMap::new();
        for index in 0..=MAX_BLOBS_PER_SIZE {
            push_blob(
                &mut by_size,
                80_000,
                Blob {
                    hash: format!("hash-{index}"),
                    path: PathBuf::from(format!("blob-{index}")),
                    device: Some(1),
                },
            );
        }
        assert!(matches!(by_size.get(&80_000), Some(SizeClass::Many)));
        push_blob(
            &mut by_size,
            90_000,
            Blob {
                hash: "only".into(),
                path: PathBuf::from("only"),
                device: Some(1),
            },
        );
        assert!(matches!(
            by_size.get(&90_000),
            Some(SizeClass::Few(list)) if list.len() == 1
        ));
    }

    #[cfg(unix)]
    #[test]
    fn replace_keeps_destination_mtime_and_mode_and_aborts_on_a_bad_clone() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.rlib");
        let dest = dir.path().join("dest.rlib");
        let body = bytes(8192, 7);
        fs::write(&source, &body).unwrap();
        fs::write(&dest, &body).unwrap();
        let kept = SystemTime::UNIX_EPOCH + StdDuration::from_secs(1_700_000_111);
        filetime::set_file_mtime(&dest, filetime::FileTime::from_system_time(kept)).unwrap();
        fs::set_permissions(&source, fs::Permissions::from_mode(0o755)).unwrap();
        fs::set_permissions(&dest, fs::Permissions::from_mode(0o644)).unwrap();

        let failed = replace_with(&source, &dest, &mut |_from, to| {
            fs::write(to, bytes(8192, 9))?;
            Ok(())
        });
        assert!(failed.is_err());
        assert_eq!(fs::read(&dest).unwrap(), body);
        assert!(!dir.path().join(".dest.rlib.kache-dedup").exists());

        replace_with(&source, &dest, &mut |from, to| {
            fs::copy(from, to).map(|_| ())
        })
        .unwrap();
        assert_eq!(fs::read(&dest).unwrap(), body);
        let meta = fs::symlink_metadata(&dest).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o644);
        assert_eq!(
            filetime::FileTime::from_last_modification_time(&meta),
            filetime::FileTime::from_system_time(kept)
        );
    }

    #[cfg(unix)]
    #[test]
    fn replace_leaves_the_destination_when_the_place_fails() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.rlib");
        let dest = dir.path().join("dest.rlib");
        let body = bytes(8192, 4);
        fs::write(&source, &body).unwrap();
        fs::write(&dest, &body).unwrap();
        let error = replace_with(&source, &dest, &mut |_from, _to| {
            Err(io::Error::new(ErrorKind::Unsupported, "no clone"))
        });
        assert_eq!(error.unwrap_err().kind(), ErrorKind::Unsupported);
        assert_eq!(fs::read(&dest).unwrap(), body);
    }

    #[test]
    fn pipeline_hashes_only_a_survivor_and_remembers_the_rest() {
        let cache = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(cache.path().to_path_buf());
        let target = track(&config, workspace.path());
        let match_body = bytes(MIN_BYTES as usize, 7);
        write_blob(&config, &match_body);
        let matched = target_file(&target, "libmatch.rlib", &match_body);
        let mut different_head = match_body.clone();
        different_head[0] = 9;
        target_file(&target, "libends.rlib", &different_head);
        target_file(&target, "libsize.rlib", &bytes(MIN_BYTES as usize + 1, 3));
        fs::create_dir_all(target.join("debug/incremental")).unwrap();
        fs::write(target.join("debug/incremental/libskip.rlib"), &match_body).unwrap();

        let first = run_quiet(&config);
        assert_eq!(first.hashed, 1, "only the matching file is hashed");
        assert_eq!(first.size_rejected, 1);
        assert_eq!(first.ends_rejected, 1);
        assert_eq!(first.matches, 1);
        assert!(first.matched_bytes > 0);
        assert_eq!(first.remembered, 0);

        let second = run_quiet(&config);
        assert_eq!(second.hashed, 0, "an unchanged stamp is not hashed again");
        assert_eq!(second.size_rejected, 0);
        assert_eq!(second.ends_rejected, 0);
        assert_eq!(second.remembered, 3);
        assert_eq!(second.matches, 1);
        let _ = matched;
    }

    #[test]
    fn same_ends_and_a_different_middle_are_hashed_once() {
        let cache = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(cache.path().to_path_buf());
        let target = track(&config, workspace.path());
        let mut body = bytes(MIN_BYTES as usize, 7);
        write_blob(&config, &body);
        body[MIN_BYTES as usize / 2] = 9;
        target_file(&target, "libmiddle.rlib", &body);

        let first = run_quiet(&config);
        assert_eq!(first.ends_rejected, 0);
        assert_eq!(first.hashed, 1);
        assert_eq!(first.matches, 0);
        let second = run_quiet(&config);
        assert_eq!(second.hashed, 0);
        assert_eq!(second.remembered, 1);
        assert_eq!(second.matches, 0);
    }

    #[test]
    fn an_unreadable_blob_is_not_remembered_as_a_difference() {
        let cache = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(cache.path().to_path_buf());
        let target = track(&config, workspace.path());
        let body = bytes(MIN_BYTES as usize, 2);
        let hash = write_blob(&config, &body);
        target_file(&target, "libgone.rlib", &body);
        let index = index(&config);
        fs::remove_file(blob_file(&config.store_dir(), &hash)).unwrap();
        let store = crate::store::Store::open(&config).unwrap();

        let once = |cursor_db: &mut PipelineDb| {
            let roots = tracked_roots(&store).unwrap();
            one_pass(
                &store,
                &config.store_dir(),
                &index,
                cursor_db,
                &roots,
                false,
                &Limits {
                    examine: usize::MAX,
                    ends: usize::MAX,
                    hashes: usize::MAX,
                    budget: Duration::from_secs(60),
                },
                &mut |_| true,
            )
            .unwrap()
        };
        let mut db = PipelineDb::open(&config.cache_dir).unwrap();
        let first = once(&mut db);
        assert_eq!(first.ends_rejected, 0);
        assert_eq!(first.hashed, 0);
        assert!(
            first.failed >= 1,
            "a missing blob is a retry, not a verdict"
        );
        let second = once(&mut db);
        assert!(second.failed >= 1, "the file is visited again");
        assert_eq!(second.remembered, 0, "the miss was not stored");
        assert_eq!(second.ends_rejected, 0);
        assert_eq!(second.hashed, 0);
    }

    #[test]
    fn apply_clones_a_stored_blob_and_keeps_the_mtime() {
        let cache = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(cache.path().to_path_buf());
        let target = track(&config, workspace.path());
        let body = bytes(MIN_BYTES as usize, 5);
        write_blob(&config, &body);
        let file = target_file(&target, "libkeep.rlib", &body);
        let second = target_file(&target, "libkeepb.rlib", &body);
        let kept = SystemTime::UNIX_EPOCH + StdDuration::from_secs(1_700_000_222);
        filetime::set_file_mtime(&file, filetime::FileTime::from_system_time(kept)).unwrap();
        filetime::set_file_mtime(&second, filetime::FileTime::from_system_time(kept)).unwrap();

        let report = run_apply(&config);
        assert_eq!(fs::read(&file).unwrap(), body);
        assert_eq!(fs::read(&second).unwrap(), body);
        assert_eq!(report.skipped_busy, 0);
        assert_eq!(report.replaced + report.failed, 2);
        if report.replaced == 2 {
            for path in [&file, &second] {
                let meta = fs::symlink_metadata(path).unwrap();
                assert_eq!(
                    filetime::FileTime::from_last_modification_time(&meta),
                    filetime::FileTime::from_system_time(kept)
                );
            }
            let again = run_quiet(&config);
            assert_eq!(again.hashed, 0, "the clone's new stamp is remembered");
        } else {
            assert_eq!(
                report.failed, 2,
                "a volume that cannot clone leaves the copies"
            );
        }
    }

    #[test]
    fn path_holds_stops_at_a_separator_and_accepts_the_directory_itself() {
        assert!(path_holds("/work/target", "/work/target"));
        assert!(path_holds(
            "/work/target",
            "/work/target/debug/deps/lib.rlib"
        ));
        assert!(!path_holds("/work/target", "/work/target2/debug"));
        assert!(!path_holds("/work/target", "/elsewhere/target/debug"));
        assert!(!path_holds("/work/target/debug", "/work/target"));
        #[cfg(windows)]
        {
            assert!(path_holds(
                r"C:\work\target",
                r"C:\work\target\debug\deps\lib.rlib"
            ));
            assert!(!path_holds(r"C:\work\target", r"C:\work\target2\debug"));
        }
        #[cfg(not(windows))]
        assert!(!path_holds("work", "work\\debug"));
    }

    #[test]
    fn a_busy_target_does_not_finish_the_walk() {
        let cache = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(cache.path().to_path_buf());
        let target = track(&config, workspace.path());
        let body = bytes(MIN_BYTES as usize, 3);
        write_blob(&config, &body);
        target_file(&target, "libbusy.rlib", &body);
        let lock_path = target.join("debug").join(".cargo-lock");
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .unwrap();
        lock.try_lock().unwrap();

        let store = crate::store::Store::open(&config).unwrap();
        let roots = tracked_roots(&store).unwrap();
        let found = index(&config);
        let mut db = PipelineDb::open(&config.cache_dir).unwrap();
        let report = one_pass(
            &store,
            &config.store_dir(),
            &found,
            &mut db,
            &roots,
            true,
            &Limits {
                examine: usize::MAX,
                ends: usize::MAX,
                hashes: usize::MAX,
                budget: Duration::from_secs(60),
            },
            &mut |_| true,
        )
        .unwrap();
        assert_eq!(report.skipped_busy, 1);
        assert!(
            !report.exhausted,
            "a held cargo lock is retried, not finished"
        );
        assert_eq!(report.examined, 0);
        assert_eq!(report.hashed, 0);
        assert_eq!(db.cursor(), "");
        drop(lock);
    }

    #[test]
    fn an_idle_slice_runs_only_when_on_and_the_machine_is_quiet() {
        let cache = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let mut config = crate::test_support::test_config(cache.path().to_path_buf());
        let target = track(&config, workspace.path());
        let body = bytes(MIN_BYTES as usize, 4);
        write_blob(&config, &body);
        let file = target_file(&target, "libidle.rlib", &body);
        let idle = crate::maintenance::RequestClock::idle();

        idle_slice(&config, Trigger::Periodic(&idle));
        assert!(!cache.path().join(DB_FILE).exists(), "off");

        config.auto_share_target_files = true;
        idle_slice(
            &config,
            Trigger::Periodic(&crate::maintenance::RequestClock::new()),
        );
        assert!(
            !cache.path().join(DB_FILE).exists(),
            "a fresh request keeps the slice from starting"
        );
        idle_slice(&config, Trigger::Shutdown);
        assert!(!cache.path().join(DB_FILE).exists(), "not at shutdown");

        idle_slice(&config, Trigger::Periodic(&idle));
        assert!(cache.path().join(DB_FILE).exists());
        let store = crate::store::Store::open(&config).unwrap();
        assert!(matches!(
            store.file_hash_lookup(&file),
            kache_store::file_hash::FileHashLookup::Hit(_)
        ));
        assert_eq!(fs::read(&file).unwrap(), body);
    }

    #[test]
    fn the_index_lists_large_blobs_by_length_from_the_store() {
        let cache = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(cache.path().to_path_buf());
        assert!(index(&config).by_size.is_empty(), "no store yet");
        let large = bytes(MIN_BYTES as usize, 1);
        let hash = write_blob(&config, &large);
        write_blob(&config, &bytes(MIN_BYTES as usize - 1, 2));
        let found = index(&config);
        assert_eq!(found.by_size.len(), 1);
        let Some(SizeClass::Few(blobs)) = found.get(MIN_BYTES) else {
            panic!("the large blob is indexed by its length");
        };
        assert_eq!(blobs.len(), 1);
        assert_eq!(blobs[0].hash, hash);
        assert_eq!(blobs[0].path, blob_file(&config.store_dir(), &hash));
        assert_eq!(
            blobs[0].device,
            device_of(&blobs[0].path, &fs::metadata(&blobs[0].path).unwrap())
        );
    }

    #[test]
    fn same_volume_needs_two_known_identities() {
        assert!(same_volume(Some(7), Some(7)));
        assert!(!same_volume(Some(7), Some(8)));
        assert!(!same_volume(None, None));
        assert!(!same_volume(Some(7), None));
        assert!(!same_volume(None, Some(7)));
    }

    #[test]
    fn a_different_volume_is_cross_device_before_the_hash() {
        let cache = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(cache.path().to_path_buf());
        let target = track(&config, workspace.path());
        let body = bytes(MIN_BYTES as usize, 3);
        let hash = write_blob(&config, &body);
        let file = target_file(&target, "libcross.rlib", &body);
        let real = device_of(&file, &fs::metadata(&file).unwrap());
        let other = real.map(|value| value.wrapping_add(1)).or(Some(1));
        let mut by_size = HashMap::new();
        push_blob(
            &mut by_size,
            MIN_BYTES,
            Blob {
                hash: hash.clone(),
                path: blob_file(&config.store_dir(), &hash),
                device: other,
            },
        );
        let index = BlobIndex { by_size };
        let store = crate::store::Store::open(&config).unwrap();
        let mut db = PipelineDb::open(&config.cache_dir).unwrap();
        let report = one_pass(
            &store,
            &config.store_dir(),
            &index,
            &mut db,
            std::slice::from_ref(&target),
            true,
            &Limits {
                examine: EXAMINE_PER_SLICE,
                ends: ENDS_PER_SLICE,
                hashes: HASH_PER_SLICE,
                budget: Duration::from_secs(60),
            },
            &mut |_| true,
        )
        .unwrap();
        assert_eq!(report.cross_device, 1, "{report:?}");
        assert_eq!(report.hashed, 0, "{report:?}");
        assert!(
            !matches!(
                store.file_hash_lookup(&file),
                kache_store::file_hash::FileHashLookup::Hit(_)
            ),
            "a different volume is decided before the file is hashed"
        );
    }

    fn blob_at(config: &Config, hash: &str, device: u64) -> Blob {
        Blob {
            hash: hash.to_string(),
            path: blob_file(&config.store_dir(), hash),
            device: Some(device),
        }
    }

    /// Run one pass against an index built by the caller, not the store.
    fn report_against(config: &Config, target: &PathBuf, blobs: Vec<Blob>) -> Report {
        let mut by_size = HashMap::new();
        for blob in blobs {
            push_blob(&mut by_size, MIN_BYTES, blob);
        }
        let store = crate::store::Store::open(config).unwrap();
        let mut db = PipelineDb::open(&config.cache_dir).unwrap();
        one_pass(
            &store,
            &config.store_dir(),
            &BlobIndex { by_size },
            &mut db,
            std::slice::from_ref(target),
            false,
            &Limits {
                examine: EXAMINE_PER_SLICE,
                ends: ENDS_PER_SLICE,
                hashes: HASH_PER_SLICE,
                budget: Duration::from_secs(60),
            },
            &mut |_| true,
        )
        .unwrap()
    }

    /// One blob on this volume still reaches the hash when another blob of
    /// the same length is on a different volume. A single foreign blob must
    /// not shelve the file.
    #[test]
    fn a_same_volume_blob_is_hashed_beside_a_foreign_one() {
        let cache = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(cache.path().to_path_buf());
        let target = track(&config, workspace.path());
        let body = bytes(MIN_BYTES as usize, 3);
        let other_body = bytes(MIN_BYTES as usize, 9);
        let file = target_file(&target, "libmix.rlib", &body);
        let Some(real) = device_of(&file, &fs::metadata(&file).unwrap()) else {
            return;
        };
        let same = write_blob(&config, &body);
        let different = write_blob(&config, &other_body);
        let report = report_against(
            &config,
            &target,
            vec![
                blob_at(&config, &same, real),
                blob_at(&config, &different, real.wrapping_add(1)),
            ],
        );
        assert_eq!(report.hashed, 1, "{report:?}");
        assert_eq!(report.cross_device, 0, "{report:?}");
        let store = crate::store::Store::open(&config).unwrap();
        assert!(matches!(
            store.file_hash_lookup(&file),
            kache_store::file_hash::FileHashLookup::Hit(_)
        ));
    }

    /// The foreign blob can have the same ends. It is not a reason to hash,
    /// and it is not a match: only the blob on this volume is compared.
    #[test]
    fn a_foreign_blob_with_the_same_ends_is_not_hashed() {
        let cache = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(cache.path().to_path_buf());
        let target = track(&config, workspace.path());
        let body = bytes(MIN_BYTES as usize, 3);
        let other_body = bytes(MIN_BYTES as usize, 9);
        let file = target_file(&target, "libforeign.rlib", &body);
        let Some(real) = device_of(&file, &fs::metadata(&file).unwrap()) else {
            return;
        };
        let same_bytes = write_blob(&config, &body);
        let different_bytes = write_blob(&config, &other_body);
        let report = report_against(
            &config,
            &target,
            vec![
                blob_at(&config, &different_bytes, real),
                blob_at(&config, &same_bytes, real.wrapping_add(1)),
            ],
        );
        assert_eq!(report.hashed, 0, "{report:?}");
        assert_eq!(report.cross_device, 0, "{report:?}");
        assert_eq!(report.ends_rejected, 1, "{report:?}");
        let store = crate::store::Store::open(&config).unwrap();
        assert!(
            !matches!(
                store.file_hash_lookup(&file),
                kache_store::file_hash::FileHashLookup::Hit(_)
            ),
            "ends on another volume are not this file's match"
        );
    }

    #[test]
    fn commit_or_abandon_removes_the_temporary_only_when_the_step_fails() {
        let dir = tempfile::tempdir().unwrap();
        let temporary = dir.path().join(".dest.rlib.kache-dedup");
        fs::write(&temporary, b"partial").unwrap();
        let error = commit_or_abandon(
            &temporary,
            Err(io::Error::new(ErrorKind::PermissionDenied, "mode")),
        );
        assert_eq!(error.unwrap_err().kind(), ErrorKind::PermissionDenied);
        assert!(!temporary.exists());

        fs::write(&temporary, b"kept").unwrap();
        commit_or_abandon(&temporary, Ok(())).unwrap();
        assert_eq!(fs::read(&temporary).unwrap(), b"kept");
    }

    #[test]
    fn a_slice_hashes_one_survivor_and_leaves_the_next() {
        let cache = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(cache.path().to_path_buf());
        let target = track(&config, workspace.path());
        let first_body = bytes(MIN_BYTES as usize, 6);
        let second_body = bytes(MIN_BYTES as usize, 8);
        write_blob(&config, &first_body);
        write_blob(&config, &second_body);
        let first = target_file(&target, "liba.rlib", &first_body);
        let second = target_file(&target, "libb.rlib", &second_body);
        let known = |lookup: kache_store::file_hash::FileHashLookup| {
            matches!(lookup, kache_store::file_hash::FileHashLookup::Hit(_))
        };
        let slice = |db: &mut PipelineDb| {
            let store = crate::store::Store::open(&config).unwrap();
            let roots = tracked_roots(&store).unwrap();
            let index = index(&config);
            // The time budget is not part of this assertion: a busy machine
            // can spend it before the first hash, and free space moves while
            // the rest of the suite runs.
            one_pass(
                &store,
                &config.store_dir(),
                &index,
                db,
                &roots,
                true,
                &Limits {
                    examine: EXAMINE_PER_SLICE,
                    ends: ENDS_PER_SLICE,
                    hashes: HASH_PER_SLICE,
                    budget: Duration::from_secs(60),
                },
                &mut |_| true,
            )
            .unwrap()
        };

        let mut db = PipelineDb::open(&config.cache_dir).unwrap();
        assert_eq!(slice(&mut db).hashed, 1);
        let store = crate::store::Store::open(&config).unwrap();
        assert!(known(store.file_hash_lookup(&first)));
        assert!(!known(store.file_hash_lookup(&second)));

        assert_eq!(slice(&mut db).hashed, 1);
        let store = crate::store::Store::open(&config).unwrap();
        assert!(known(store.file_hash_lookup(&second)));
    }

    fn run_quiet(config: &Config) -> Report {
        let roots = tracked_roots(&crate::store::Store::open(config).unwrap()).unwrap();
        let index = index(config);
        let mut db = PipelineDb::open(&config.cache_dir).unwrap();
        let store = crate::store::Store::open(config).unwrap();
        let mut report = Report::new(roots.len());
        loop {
            let pass = one_pass(
                &store,
                &config.store_dir(),
                &index,
                &mut db,
                &roots,
                false,
                &Limits {
                    examine: usize::MAX,
                    ends: usize::MAX,
                    hashes: usize::MAX,
                    budget: Duration::from_secs(60),
                },
                &mut |_| true,
            )
            .unwrap();
            let done = pass.exhausted;
            fold(&mut report, &pass);
            if done {
                break;
            }
        }
        report
    }

    fn run_apply(config: &Config) -> Report {
        let roots = tracked_roots(&crate::store::Store::open(config).unwrap()).unwrap();
        let index = index(config);
        let mut db = PipelineDb::open(&config.cache_dir).unwrap();
        let store = crate::store::Store::open(config).unwrap();
        one_pass(
            &store,
            &config.store_dir(),
            &index,
            &mut db,
            &roots,
            true,
            &Limits {
                examine: usize::MAX,
                ends: usize::MAX,
                hashes: usize::MAX,
                budget: Duration::from_secs(60),
            },
            &mut |_| true,
        )
        .unwrap()
    }

    #[test]
    fn share_note_names_only_a_rewrite() {
        assert_eq!(share_note(0, 4096), None);
        let note = share_note(2, 4096).unwrap();
        assert!(note.starts_with("cloned 2 stored artifacts"), "{note}");
        assert!(note.contains("shared"), "{note}");
    }

    #[test]
    fn present_returns_the_text_report() {
        let mut report = Report::new(2);
        report.examined = 4;
        report.failed = 1;
        let text = present(&report, true, false).unwrap().join("\n");
        assert!(
            text.contains("2 target directories, 4 files examined"),
            "{text}"
        );
        assert!(text.contains("replaced 0 files, 1 failed"), "{text}");
        report.failed = 0;
        let quiet = present(&report, true, false).unwrap().join("\n");
        assert!(!quiet.contains("replaced"), "{quiet}");
    }

    #[test]
    fn resolve_roots_keeps_the_directory_it_was_given() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(dir.path().join("cache"));
        let root = dir.path().join("target");
        fs::create_dir_all(&root).unwrap();
        let resolved = resolve_roots(&config, std::slice::from_ref(&root)).unwrap();
        assert_eq!(resolved, vec![std::path::absolute(&root).unwrap()]);
        let file = root.join("lib.rlib");
        fs::write(&file, b"x").unwrap();
        let err = resolve_roots(&config, &[file]).unwrap_err().to_string();
        assert!(err.contains("not a directory"), "{err}");
    }

    #[test]
    fn share_command_opens_the_pipeline_for_a_target() {
        let cache = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(cache.path().to_path_buf());
        let target = track(&config, workspace.path());
        run(&config, &[target], false, false).unwrap();
        assert!(cache.path().join(DB_FILE).is_file());
    }

    #[test]
    fn separators_are_slash_and_the_platform_separator() {
        assert!(path_separator(b'/'));
        assert!(!path_separator(b'a'));
        if std::path::MAIN_SEPARATOR == '\\' {
            assert!(path_separator(b'\\'));
        } else {
            assert!(!path_separator(b'\\'));
        }
    }

    #[test]
    fn the_cursor_keeps_a_directory_that_can_still_hold_a_later_file() {
        let root = Path::new("/work/z");
        assert!(cursor_before_root("", root));
        assert!(cursor_before_root("/work/z", root));
        assert!(cursor_before_root("/work/a", root));
        assert!(cursor_before_root("/work/z/debug/lib.rlib", root));
        assert!(!cursor_before_root("/work/zz", root));

        let dir = Path::new("/work/z/debug");
        assert!(dir_may_contain(dir, ""));
        assert!(dir_may_contain(dir, "/work/z/debug"));
        assert!(dir_may_contain(dir, "/work/a"));
        assert!(dir_may_contain(dir, "/work/z/debug/lib.rlib"));
        assert!(!dir_may_contain(dir, "/work/z/debug2"));
    }

    #[test]
    fn candidates_after_the_cursor_stop_when_the_tree_ends() {
        let root = tempfile::tempdir().unwrap();
        // One directory under the root, so the recursive return reaches the caller.
        let deps = root.path().join("deps");
        fs::create_dir_all(&deps).unwrap();
        let body = bytes(MIN_BYTES as usize, 1);
        let first = deps.join("a.rlib");
        let second = deps.join("b.rlib");
        fs::write(&first, &body).unwrap();
        fs::write(&second, &body).unwrap();

        let mut found = Vec::new();
        assert!(collect_candidates(root.path(), "", 10, &mut found));
        assert_eq!(found.len(), 2, "{found:?}");

        let cursor = first.to_string_lossy().into_owned();
        let mut later = Vec::new();
        assert!(collect_candidates(root.path(), &cursor, 10, &mut later));
        assert_eq!(later.len(), 1, "{later:?}");
        assert_eq!(later[0], second);
    }

    #[test]
    fn a_short_file_is_rejected_before_its_ends_are_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("short.rlib");
        fs::write(&path, [1, 2, 3]).unwrap();
        let Err(err) = read_ends(&path) else {
            panic!("a short file has no block ends");
        };
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert!(err.to_string().contains("shorter"), "{err}");
    }

    #[test]
    fn temporary_clone_paths_skip_names_already_taken() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("lib.rlib");
        let first = temporary_path(dir.path(), &dest).unwrap();
        assert_eq!(
            first.file_name().unwrap(),
            std::ffi::OsStr::new(".lib.rlib.kache-dedup")
        );
        fs::write(&first, b"x").unwrap();
        let second = temporary_path(dir.path(), &dest).unwrap();
        assert_eq!(
            second.file_name().unwrap(),
            std::ffi::OsStr::new(".lib.rlib.kache-dedup-1")
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_clone_with_the_right_ends_and_the_wrong_length_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.rlib");
        let dest = dir.path().join("dest.rlib");
        let body = bytes(8192, 7);
        fs::write(&source, &body).unwrap();
        fs::write(&dest, &body).unwrap();
        let err = replace_with(&source, &dest, &mut |_from, to| {
            let mut wrong = bytes(4096, 7);
            wrong.extend(bytes(8192, 1));
            wrong.extend(bytes(4096, 7));
            fs::write(to, wrong)?;
            Ok(())
        });
        assert!(err.is_err(), "a longer clone is not the source");
        assert_eq!(fs::read(&dest).unwrap(), body);
    }

    #[test]
    fn the_pipeline_remembers_the_cursor_and_pauses_after_a_cycle() {
        let dir = tempfile::tempdir().unwrap();
        let db = PipelineDb::open(dir.path()).unwrap();
        assert_eq!(db.cursor(), "");
        assert_eq!(db.meta("missing"), None);
        db.set_meta("k", "v");
        assert_eq!(db.meta("k").as_deref(), Some("v"));
        db.set_cursor("target/debug/lib.rlib");
        assert_eq!(db.cursor(), "target/debug/lib.rlib");

        let now = 1_700_000_000;
        assert!(!db.cycle_paused(now));
        db.mark_cycle(now);
        assert_eq!(db.meta("cycle_finished_at").as_deref(), Some("1700000000"));
        assert!(db.cycle_paused(now));
        assert!(db.cycle_paused(now + CYCLE_PAUSE.as_secs() - 1));
        assert!(!db.cycle_paused(now + CYCLE_PAUSE.as_secs()));
        assert!(unix_now() > 1_700_000_000);
    }

    #[test]
    fn a_file_device_is_its_volume_not_a_constant() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lib.rlib");
        fs::write(&path, b"abc").unwrap();
        let meta = fs::metadata(&path).unwrap();
        let device = device_of(&path, &meta);
        assert_eq!(device_of_path(&path), device);
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(device, Some(meta.dev()));
            assert!(meta.dev() > 1, "dev {}", meta.dev());
        }
        assert_ne!(device, Some(0));
        assert_ne!(device, Some(1));
    }

    #[test]
    fn lock_profile_records_the_lock_it_takes() {
        let dir = tempfile::tempdir().unwrap();
        let profile = dir.path().join("debug");
        fs::create_dir(&profile).unwrap();
        let mut held = HashMap::new();
        assert!(lock_profile(&profile, &mut held).unwrap());
        assert!(held.contains_key(&profile));
        assert!(profile.join(".cargo-lock").is_file());
    }

    fn lock_held(path: &Path) -> File {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .unwrap();
        file.try_lock().unwrap();
        file
    }

    #[test]
    fn a_locked_file_that_is_not_the_cargo_lock_does_not_block_the_walk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lib.rlib");
        let _lock = lock_held(&path);
        assert!(!another_build_holds(dir.path(), &HashMap::new()));
    }

    #[test]
    fn a_cargo_lock_past_the_walk_depth_does_not_block() {
        let dir = tempfile::tempdir().unwrap();
        let mut deep = dir.path().to_path_buf();
        for name in ["a", "b", "c", "d", "e"] {
            deep.push(name);
        }
        fs::create_dir_all(&deep).unwrap();
        let _lock = lock_held(&deep.join(".cargo-lock"));
        assert!(!another_build_holds(dir.path(), &HashMap::new()));
    }
}
