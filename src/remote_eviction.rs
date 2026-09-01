//! Size budget and eviction for a filesystem remote (kunobi-ninja/kache#774).
//!
//! A filesystem remote had no eviction path at all: `kache gc` bounded the
//! local store, an S3 remote could be bounded with a bucket lifecycle rule,
//! and a shared folder grew monotonically. The report that opened #774 measured
//! 319 GiB in one day.
//!
//! Selection reuses [`crate::eviction`] rather than growing a second ranking
//! vocabulary. Each evictable unit is described as an
//! [`EntryFeatures`](crate::eviction::EntryFeatures) and ranked by
//! [`SizePressurePolicy`](crate::eviction::SizePressurePolicy), and the sweep
//! fires and stops on
//! [`over_eviction_trigger`](crate::eviction::over_eviction_trigger) /
//! [`eviction_target`](crate::eviction::eviction_target), so the remote
//! inherits the local store's hysteresis band instead of thrashing on its cap.
//!
//! ## What the filesystem can and cannot tell us
//!
//! The local store records `last_accessed` in SQLite on every hit. A remote has
//! no database, so recency has to come from `stat`, and every mount option
//! degrades it differently:
//!
//! - **`strictatime`** — `atime` is true read recency. Nobody mounts a build
//!   cache this way; it is a write per read.
//! - **`relatime`** (the Linux default, and what #774 measured on) — `atime`
//!   advances at most once per 24h. Read recency is therefore reliable at day
//!   granularity and meaningless below it. That is enough here: the ranking only
//!   has to separate a hot set re-read every build from a tail nothing has
//!   touched in weeks.
//! - **`noatime` / `nodiratime`** — `atime` never advances past the write, so
//!   read recency is simply not observable. The sweep degrades to write recency
//!   and says so, rather than pretending a frozen `atime` is a read.
//!
//! [`observed_access`] takes `max(atime, mtime)`, which is right under all
//! three: a read pushes `atime` past `mtime` where the mount allows it, and
//! collapses to write time where it does not. [`RemoteScan::atime_advanced`]
//! records whether *any* object showed a read newer than its write, which is how
//! `kache gc --remote` knows to warn that it is ranking by write time — #774's
//! acceptance criterion that `noatime` degrade to a documented fallback rather
//! than silently evicting the hot set.
//!
//! The issue also proposes a sidecar touch-file so `noatime` mounts regain read
//! recency. That needs the restore path to write on every hit and is out of
//! scope here; the advisory names the limitation instead.
//!
//! ## Pack sharing, and why the unit of eviction differs per layout
//!
//! - **v3** (`v3/manifests/{crate}/{key}.json` + `v3/packs/{crate}/{key}.tar.zst`)
//!   is **not** shared. Both keys are derived from the same `(crate, cache_key)`
//!   pair by `remote_layout::v3_manifest_key` / `v3_pack_key`, so exactly one
//!   manifest can ever name a given pack. The pair is one eviction unit.
//! - **v4 prefetch** (`v4/prefetch/packs/{dd}/{digest}.kpack`) **is** shared. A
//!   pack is content-addressed by digest and referenced by digest from
//!   `v4/prefetch/catalogs/{selector}/...`, so successive catalog generations
//!   for one selector — and different selectors that happened to build the same
//!   pack — all point at one object. The unit of eviction is the *catalog*, and
//!   a pack is unlinked only when the last catalog referencing it goes.
//!   [`RemoteGroup::digests`] against [`SharedPack::refs`] is that reference
//!   count, and it feeds `reclaimable_bytes` exactly as blob refcounts do
//!   locally (kunobi-ninja/kache#608): a catalog whose packs are all shared
//!   frees only its own few kilobytes and ranks accordingly.
//!
//! A dangling reference must degrade to a miss, never to an error that fails a
//! build, and both layouts already do that. A manifest whose pack is gone makes
//! `RemoteLayout::download_entry` return `EntryNotFound`, which callers downcast
//! to a clean miss (#485 Phase 0). A catalog whose pack is gone makes the
//! daemon's `try_packed_prefetch` see `None` for that pack and fall through to
//! the v3 coordinator. Deletion order still avoids creating them: a v3 unit
//! drops its manifest *before* its pack, so an interrupted sweep leaves an
//! unreferenced pack — invisible to readers, reclaimed by the next run — rather
//! than the manifest-without-pack state #774 asks us never to leave behind.
//!
//! Build manifests and shards under `_manifests/` count toward the measured size
//! but are never evicted: they are kilobyte-scale JSON that every prefetch plan
//! starts from, so removing them costs hit rate and reclaims nothing worth
//! having.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::config::FilesystemRemoteConfig;
use crate::eviction::{EntryFeatures, EvictionPolicy, SizePressurePolicy, eviction_target};

/// How recently an object must have been read or written for the sweep to treat
/// it as pinned by a live build and skip it.
///
/// The local store's `EVICTION_IDLE_GRACE` is two minutes because a local
/// restore is a hardlink or reflink and takes milliseconds. A remote restore is
/// a full pack GET across a shared filesystem, so this is tied to
/// [`DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS`](crate::config::DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS)
/// instead: a download still running past the daemon's own operation deadline
/// has already been abandoned, so there is nothing left to protect.
///
/// This is the cheap half of the mid-download guard. The load-bearing half is
/// POSIX unlink semantics — a reader that has already opened the pack keeps
/// reading the unlinked inode to completion — and, on Windows, that deleting a
/// file with an open handle fails outright and the object is skipped. The grace
/// closes the remaining window, where a peer has listed a manifest but not yet
/// issued the GET.
pub(crate) const REMOTE_EVICTION_IDLE_GRACE: Duration =
    Duration::from_secs(crate::config::DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS);

/// How long an unreferenced object must sit untouched before the sweep calls it
/// an orphan.
///
/// An upload writes the pack and *then* the manifest, so a pack whose manifest
/// has not landed yet is indistinguishable from one whose publisher crashed.
/// Same reasoning and same value as
/// [`STAGING_SWEEP_GRACE`](crate::store::STAGING_SWEEP_GRACE): the grace has to
/// outlast any plausible in-flight put.
pub(crate) const REMOTE_ORPHAN_GRACE: Duration = crate::store::STAGING_SWEEP_GRACE;

/// Advisory lock file, at the remote root beside `.kache-tmp`.
///
/// Outside the prefix on purpose, so it is never mistaken for a cache object by
/// the scan or by `RemoteLayout::list_keys`.
pub(crate) const REMOTE_GC_LOCK_FILE: &str = ".kache-remote-gc.lock";

/// Why an object is dead weight regardless of the size budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OrphanReason {
    /// A v3 pack no manifest names. Unreachable: both `exists_entry` and
    /// `list_keys` go through the manifest tree.
    PackWithoutManifest,
    /// A v3 manifest whose pack is gone. Serving it can only produce
    /// `EntryNotFound`, so it is a miss that costs a round trip.
    ManifestWithoutPack,
    /// A v4 prefetch pack (or its metadata sidecar) that no surviving catalog
    /// references.
    UnreferencedPrefetchPack,
}

impl OrphanReason {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::PackWithoutManifest => "pack without manifest",
            Self::ManifestWithoutPack => "manifest without pack",
            Self::UnreferencedPrefetchPack => "unreferenced prefetch pack",
        }
    }
}

/// One object the sweep may unlink on its own, independent of the budget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteOrphan {
    pub path: PathBuf,
    pub bytes: u64,
    pub reason: OrphanReason,
}

/// A shared v4 prefetch pack and how many catalogs still name it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct SharedPack {
    /// The `.kpack` and its `pack-meta` sidecar, both keyed by this digest.
    pub objects: Vec<(PathBuf, u64)>,
    pub bytes: u64,
    pub refs: usize,
}

/// One evictable unit: a v3 manifest+pack pair, or a v4 catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteGroup {
    /// Stable identifier for ranking and for `--dry-run` output.
    pub id: String,
    /// Objects unlinked together, in an order that leaves the remote consistent
    /// after any prefix of it.
    pub objects: Vec<PathBuf>,
    /// Bytes those objects occupy.
    pub bytes: u64,
    /// Shared v4 pack digests this group references. Empty for v3.
    pub digests: Vec<String>,
    /// Time since the newest observed access across the group's objects.
    pub idle: Duration,
}

impl RemoteGroup {
    /// Bytes evicting this group would actually free right now: its own objects
    /// plus every shared pack it holds the last reference to.
    ///
    /// The remote counterpart of `EntryFeatures::reclaimable_bytes`
    /// (kunobi-ninja/kache#608).
    fn reclaimable(&self, shared: &BTreeMap<String, SharedPack>) -> u64 {
        let unique: u64 = self
            .digests
            .iter()
            .filter_map(|digest| shared.get(digest))
            .filter(|pack| pack.refs <= 1)
            .map(|pack| pack.bytes)
            .sum();
        self.bytes.saturating_add(unique)
    }

    fn features(&self, shared: &BTreeMap<String, SharedPack>) -> EntryFeatures {
        EntryFeatures {
            key: self.id.clone(),
            size: i64::try_from(self.bytes).unwrap_or(i64::MAX),
            // A remote has no hit counter. Holding it at zero for every group
            // reduces `size_pressure_score` to `1 / (idle * reclaimable_mb)`,
            // which is exactly the least-recently-read-first ranking #774 asks
            // for, with the same marginal-bytes tiebreak the local sweep uses.
            hit_count: 0,
            idle_hours: self.idle.as_secs_f64() / 3600.0,
            content_hash: None,
            committed: true,
            // No rebuild-cost signal crosses the remote boundary: the v3
            // manifest does not carry `compile_time_ms` (see #594).
            compile_time_ms: 0,
            reclaimable_bytes: Some(i64::try_from(self.reclaimable(shared)).unwrap_or(i64::MAX)),
        }
    }
}

/// Everything one pass over the remote observed.
#[derive(Debug, Clone, Default)]
pub(crate) struct RemoteScan {
    /// Every regular file under the prefix, including objects the sweep will
    /// never evict. The budget is a statement about folder size, so it has to
    /// count what is actually there.
    pub total_bytes: u64,
    pub object_count: usize,
    pub groups: Vec<RemoteGroup>,
    pub orphans: Vec<RemoteOrphan>,
    pub shared: BTreeMap<String, SharedPack>,
    /// Bytes under the prefix that belong to no evictable unit.
    pub unclassified_bytes: u64,
    /// Whether any object showed an access time later than its write time. When
    /// false the mount is not recording reads (`noatime`), or nothing has been
    /// read yet, and the ranking is really by write time.
    pub atime_advanced: bool,
}

/// A victim the plan selected, with the objects the sweep will unlink for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedVictim {
    pub id: String,
    /// The group's own objects, then any shared pack whose last reference it
    /// held.
    pub objects: Vec<PathBuf>,
    pub freed: u64,
}

/// What a sweep would do, computed without touching the filesystem.
#[derive(Debug, Clone, Default)]
pub(crate) struct RemotePlan {
    pub total_bytes: u64,
    pub object_count: usize,
    /// `None` when the remote is explicitly unbounded.
    pub budget: Option<u64>,
    /// Low edge of the hysteresis band the sweep evicts down to.
    pub target: Option<u64>,
    pub orphans: Vec<RemoteOrphan>,
    pub victims: Vec<PlannedVictim>,
    /// Groups the ranking selected but the recency grace protected.
    pub pinned: usize,
    /// Measured size once the plan is applied.
    pub projected_bytes: u64,
    pub atime_advanced: bool,
}

impl RemotePlan {
    pub(crate) fn bytes_freed(&self) -> u64 {
        let orphaned: u64 = self.orphans.iter().map(|o| o.bytes).sum();
        let evicted: u64 = self.victims.iter().map(|v| v.freed).sum();
        orphaned.saturating_add(evicted)
    }

    /// Whether the plan leaves the remote still over its budget. Reported
    /// rather than retried: the residue is groups the grace pinned or bytes no
    /// evictable unit owns, and a second pass would not shift either.
    pub(crate) fn still_over_budget(&self) -> bool {
        self.budget
            .is_some_and(|budget| self.projected_bytes > budget)
    }
}

/// What a sweep actually did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RemoteGcStats {
    pub groups_evicted: usize,
    pub orphans_removed: usize,
    pub objects_removed: usize,
    pub bytes_freed: u64,
    /// Objects the plan named that could not be unlinked — a peer deleted them
    /// first, or, on Windows, a reader still holds the handle open. Both are
    /// benign: the next sweep sees the state that is actually there.
    pub delete_failures: usize,
}

/// Time since `observed`, clamped at zero so clock skew between the machines
/// sharing a remote cannot report a negative idle.
fn idle_since(now: SystemTime, observed: SystemTime) -> Duration {
    now.duration_since(observed).unwrap_or(Duration::ZERO)
}

/// The most recent thing we can prove happened to a file.
///
/// `max(atime, mtime)`. See the module docs: read recency where the mount
/// records reads, write recency where it does not, never older than the write.
fn observed_access(
    accessed: Option<SystemTime>,
    modified: Option<SystemTime>,
) -> Option<SystemTime> {
    match (accessed, modified) {
        (Some(a), Some(m)) => Some(a.max(m)),
        (Some(a), None) => Some(a),
        (None, Some(m)) => Some(m),
        (None, None) => None,
    }
}

/// Whether this file's access time is evidence that the mount records reads.
fn atime_is_advanced(accessed: Option<SystemTime>, modified: Option<SystemTime>) -> bool {
    matches!((accessed, modified), (Some(a), Some(m)) if a > m)
}

/// A v4 catalog's contribution to the reference count.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CatalogRead {
    /// Not a catalog at all.
    None,
    /// The pack digests this catalog references.
    Digests(Vec<String>),
    /// A catalog that could not be read or parsed. It may hold the only
    /// reference to a pack, so the pass fails closed and orphans nothing in the
    /// v4 tree.
    Unreadable,
}

/// One file the scan found, before classification.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ScannedFile {
    /// Path relative to the prefix root, with `/` separators.
    key: String,
    path: PathBuf,
    bytes: u64,
    idle: Duration,
    catalog: CatalogRead,
}

/// How a key maps onto the remote layout.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Classified {
    V3Manifest { unit: String },
    V3Pack { unit: String },
    V4Catalog,
    V4PrefetchPack { digest: String },
    V4PrefetchPackMeta { digest: String },
    Other,
}

/// Classify a prefix-relative key. Pure, so every layout branch is testable
/// without materializing a remote.
fn classify(key: &str) -> Classified {
    if let Some(rest) = key.strip_prefix("v3/manifests/")
        && let Some((crate_name, cache_key)) = rest.rsplit_once('/')
        && let Some(cache_key) = cache_key.strip_suffix(".json")
        && !crate_name.contains('/')
    {
        return Classified::V3Manifest {
            unit: format!("{crate_name}/{cache_key}"),
        };
    }
    if let Some(rest) = key.strip_prefix("v3/packs/")
        && let Some((crate_name, cache_key)) = rest.rsplit_once('/')
        && let Some(cache_key) = cache_key.strip_suffix(".tar.zst")
        && !crate_name.contains('/')
    {
        return Classified::V3Pack {
            unit: format!("{crate_name}/{cache_key}"),
        };
    }
    if key.starts_with("v4/prefetch/catalogs/") && key.ends_with(".json") {
        return Classified::V4Catalog;
    }
    if let Some(rest) = key.strip_prefix("v4/prefetch/packs/")
        && let Some((shard, file)) = rest.rsplit_once('/')
        && let Some(digest) = file.strip_suffix(".kpack")
        && !shard.contains('/')
    {
        return Classified::V4PrefetchPack {
            digest: digest.to_string(),
        };
    }
    if let Some(rest) = key.strip_prefix("v4/prefetch/pack-meta/")
        && let Some(digest) = rest.strip_suffix(".json")
        && !digest.contains('/')
    {
        return Classified::V4PrefetchPackMeta {
            digest: digest.to_string(),
        };
    }
    Classified::Other
}

/// Just enough of a catalog to count references.
///
/// Deliberately not `remote_pack::PackCatalog`: that type is
/// `deny_unknown_fields` and is validated against a selector and an expiry,
/// neither of which a sweep may apply. An expired catalog still pins its packs
/// until it is itself evicted, and a catalog written by a newer kache with an
/// extra field must not read as unparseable and suppress the whole v4 sweep.
#[derive(Debug, Deserialize)]
struct CatalogRefs {
    packs: Vec<CatalogPackRef>,
}

#[derive(Debug, Deserialize)]
struct CatalogPackRef {
    digest: String,
}

fn catalog_digests(body: &[u8]) -> Result<Vec<String>> {
    let catalog: CatalogRefs =
        serde_json::from_slice(body).context("parsing prefetch catalog references")?;
    Ok(catalog.packs.into_iter().map(|pack| pack.digest).collect())
}

/// Walk `dir` collecting every regular file, skipping `excluded` subtrees.
///
/// Per-entry errors are skipped rather than propagated: a shared remote written
/// by two uids routinely has a directory one of them cannot read, and a sweep
/// that refuses to run at all is worse than one that bounds what it can see.
/// Symlinks are never followed and never counted — following one would let a
/// symlink planted in the shared folder aim the sweep's unlinks outside the
/// remote, the same escalation `verify_write_containment` guards the write path
/// against.
fn walk_files(dir: &Path, excluded: &[PathBuf], out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if excluded.contains(&path) {
            continue;
        }
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_dir() {
            walk_files(&path, excluded, out);
        } else if file_type.is_file() {
            out.push(path);
        }
    }
}

/// Measure a filesystem remote and classify everything under its prefix.
pub(crate) fn scan(
    remote: &FilesystemRemoteConfig,
    prefix: &str,
    now: SystemTime,
) -> Result<RemoteScan> {
    let prefix_root = if prefix.is_empty() {
        remote.root.clone()
    } else {
        remote.root.join(prefix)
    };
    let excluded = vec![
        remote.atomic_write_dir.clone(),
        remote.root.join(REMOTE_GC_LOCK_FILE),
    ];

    let mut paths = Vec::new();
    walk_files(&prefix_root, &excluded, &mut paths);

    let mut files = Vec::with_capacity(paths.len());
    let mut total_bytes = 0u64;
    let mut object_count = 0usize;
    let mut atime_advanced = false;

    for path in paths {
        // `symlink_metadata` so a symlink is measured as the link, never as its
        // target; `walk_files` already refused to emit one.
        let Ok(meta) = path.symlink_metadata() else {
            continue;
        };
        let bytes = meta.len();
        total_bytes = total_bytes.saturating_add(bytes);
        object_count += 1;

        let accessed = meta.accessed().ok();
        let modified = meta.modified().ok();
        atime_advanced |= atime_is_advanced(accessed, modified);
        let idle = observed_access(accessed, modified)
            .map(|observed| idle_since(now, observed))
            // No timestamp at all: treat it as just touched, so the sweep never
            // evicts something it could not date.
            .unwrap_or(Duration::ZERO);

        // A key that is not UTF-8 belongs to no eviction unit. Its bytes stay in
        // `total_bytes` — the budget is about folder size — but nothing can
        // select it, so the sweep leaves it alone.
        let Some(key) = relative_key(&prefix_root, &path) else {
            continue;
        };
        let catalog = if classify(&key) == Classified::V4Catalog {
            match std::fs::read(&path)
                .context("reading prefetch catalog")
                .and_then(|body| catalog_digests(&body))
            {
                Ok(digests) => CatalogRead::Digests(digests),
                Err(error) => {
                    tracing::warn!(
                        path = %path.display(),
                        "unreadable prefetch catalog ({error:#}); \
                         not orphaning any prefetch pack this pass"
                    );
                    CatalogRead::Unreadable
                }
            }
        } else {
            CatalogRead::None
        };
        files.push(ScannedFile {
            key,
            path,
            bytes,
            idle,
            catalog,
        });
    }

    let mut scan = assemble(files);
    scan.total_bytes = total_bytes;
    scan.object_count = object_count;
    scan.atime_advanced = atime_advanced;
    Ok(scan)
}

/// Prefix-relative key with `/` separators, or `None` when the path is not
/// under the prefix or is not representable as UTF-8 (the transport rejects
/// such keys on write, so nothing kache published can hit this).
fn relative_key(prefix_root: &Path, path: &Path) -> Option<String> {
    let relative = path.strip_prefix(prefix_root).ok()?;
    let mut parts = Vec::new();
    for component in relative.components() {
        parts.push(component.as_os_str().to_str()?);
    }
    Some(parts.join("/"))
}

/// Group classified files into eviction units.
///
/// Pure over its input, so the pairing, reference counting, and orphan rules are
/// testable without a filesystem. Leaves `total_bytes`, `object_count`, and
/// `atime_advanced` for [`scan`] to fill: those describe every file under the
/// prefix, including ones no unit claims.
fn assemble(files: Vec<ScannedFile>) -> RemoteScan {
    #[derive(Default)]
    struct V3Unit {
        manifest: Option<ScannedFile>,
        pack: Option<ScannedFile>,
    }

    let mut scan = RemoteScan::default();
    let mut v3: BTreeMap<String, V3Unit> = BTreeMap::new();
    let mut catalogs: Vec<(ScannedFile, Vec<String>)> = Vec::new();
    let mut unreadable_catalog = false;

    for file in files {
        match classify(&file.key) {
            Classified::V3Manifest { unit } => {
                v3.entry(unit).or_default().manifest = Some(file);
            }
            Classified::V3Pack { unit } => {
                v3.entry(unit).or_default().pack = Some(file);
            }
            Classified::V4Catalog => match &file.catalog {
                CatalogRead::Digests(digests) => {
                    let digests = digests.clone();
                    catalogs.push((file, digests));
                }
                // An unreadable catalog is not an eviction unit and its bytes
                // are not reclaimable, so it counts as unclassified.
                CatalogRead::Unreadable | CatalogRead::None => {
                    unreadable_catalog = true;
                    scan.unclassified_bytes = scan.unclassified_bytes.saturating_add(file.bytes);
                }
            },
            Classified::V4PrefetchPack { digest } | Classified::V4PrefetchPackMeta { digest } => {
                let shared = scan.shared.entry(digest).or_default();
                shared.bytes = shared.bytes.saturating_add(file.bytes);
                shared.objects.push((file.path, file.bytes));
            }
            Classified::Other => {
                scan.unclassified_bytes = scan.unclassified_bytes.saturating_add(file.bytes);
            }
        }
    }

    for (unit, pair) in v3 {
        match (pair.manifest, pair.pack) {
            (Some(manifest), Some(pack)) => scan.groups.push(RemoteGroup {
                id: format!("v3:{unit}"),
                // Manifest first: an interrupted sweep must never leave a
                // manifest without its pack (#774).
                objects: vec![manifest.path, pack.path],
                bytes: manifest.bytes.saturating_add(pack.bytes),
                digests: Vec::new(),
                // As idle as its freshest half: reading the pack keeps the
                // entry alive even though the manifest is only listed.
                idle: manifest.idle.min(pack.idle),
            }),
            (Some(half), None) => push_orphan(&mut scan, half, OrphanReason::ManifestWithoutPack),
            (None, Some(half)) => push_orphan(&mut scan, half, OrphanReason::PackWithoutManifest),
            (None, None) => unreachable!("a v3 unit exists only because a file created it"),
        }
    }

    for (catalog, digests) in catalogs {
        for digest in &digests {
            if let Some(shared) = scan.shared.get_mut(digest) {
                shared.refs += 1;
            }
        }
        scan.groups.push(RemoteGroup {
            id: format!("v4:{}", catalog.key),
            objects: vec![catalog.path],
            bytes: catalog.bytes,
            digests,
            idle: catalog.idle,
        });
    }

    if !unreadable_catalog {
        let unreferenced: Vec<RemoteOrphan> = scan
            .shared
            .values()
            .filter(|pack| pack.refs == 0)
            .flat_map(|pack| pack.objects.iter())
            .map(|(path, bytes)| RemoteOrphan {
                path: path.clone(),
                bytes: *bytes,
                reason: OrphanReason::UnreferencedPrefetchPack,
            })
            .collect();
        scan.orphans.extend(unreferenced);
    }

    scan
}

/// Record `file` as an orphan, unless it is young enough to belong to a publish
/// still in flight — in which case its bytes just count toward the total.
fn push_orphan(scan: &mut RemoteScan, file: ScannedFile, reason: OrphanReason) {
    if file.idle < REMOTE_ORPHAN_GRACE {
        scan.unclassified_bytes = scan.unclassified_bytes.saturating_add(file.bytes);
        return;
    }
    scan.orphans.push(RemoteOrphan {
        path: file.path,
        bytes: file.bytes,
        reason,
    });
}

/// Decide what to evict. Pure: no filesystem access, so the ranking, the budget
/// arithmetic, the recency pin, and shared-pack reference counting are all
/// directly testable.
pub(crate) fn plan(scan: &RemoteScan, budget: Option<u64>) -> RemotePlan {
    let orphans = scan.orphans.clone();
    let orphaned: u64 = orphans.iter().map(|o| o.bytes).sum();
    let mut remaining = scan.total_bytes.saturating_sub(orphaned);

    let mut plan = RemotePlan {
        total_bytes: scan.total_bytes,
        object_count: scan.object_count,
        budget,
        target: budget.map(eviction_target),
        orphans,
        victims: Vec::new(),
        pinned: 0,
        projected_bytes: remaining,
        atime_advanced: scan.atime_advanced,
    };

    // Removing orphans is repair, not budget enforcement, so it happens even on
    // an unbounded remote. Ranked eviction needs a cap to aim at.
    let (Some(budget), Some(target)) = (budget, plan.target) else {
        return plan;
    };
    if !crate::eviction::over_eviction_trigger(remaining, budget) {
        return plan;
    }

    let by_id: BTreeMap<&str, &RemoteGroup> =
        scan.groups.iter().map(|g| (g.id.as_str(), g)).collect();
    let features: Vec<EntryFeatures> = scan
        .groups
        .iter()
        .map(|group| group.features(&scan.shared))
        .collect();

    // Live reference counts, decremented as catalogs are selected, so the last
    // catalog naming a shared pack is the one that actually frees it.
    let mut refs: BTreeMap<&str, usize> = scan
        .shared
        .iter()
        .map(|(digest, pack)| (digest.as_str(), pack.refs))
        .collect();

    for id in SizePressurePolicy.select(&features) {
        if remaining <= target {
            break;
        }
        let Some(group) = by_id.get(id.as_str()) else {
            continue;
        };
        if group.idle < REMOTE_EVICTION_IDLE_GRACE {
            plan.pinned += 1;
            continue;
        }

        let mut objects = group.objects.clone();
        let mut freed = group.bytes;
        for digest in &group.digests {
            let Some(count) = refs.get_mut(digest.as_str()) else {
                continue;
            };
            *count = count.saturating_sub(1);
            if *count == 0
                && let Some(pack) = scan.shared.get(digest)
            {
                objects.extend(pack.objects.iter().map(|(path, _)| path.clone()));
                freed = freed.saturating_add(pack.bytes);
            }
        }

        remaining = remaining.saturating_sub(freed);
        plan.victims.push(PlannedVictim {
            id: group.id.clone(),
            objects,
            freed,
        });
    }

    plan.projected_bytes = remaining;
    plan
}

/// Unlink everything the plan named.
///
/// A failed unlink is counted and stepped over, never propagated. On POSIX the
/// realistic cause is a peer that already removed the object; on Windows it is a
/// reader holding the handle open, which is precisely the object we must not
/// remove. Either way the next sweep re-measures.
pub(crate) fn apply(plan: &RemotePlan) -> RemoteGcStats {
    let mut stats = RemoteGcStats::default();
    for orphan in &plan.orphans {
        if unlink(&orphan.path) {
            stats.orphans_removed += 1;
            stats.objects_removed += 1;
            stats.bytes_freed = stats.bytes_freed.saturating_add(orphan.bytes);
        } else {
            stats.delete_failures += 1;
        }
    }
    for victim in &plan.victims {
        let mut removed_any = false;
        for path in &victim.objects {
            if unlink(path) {
                stats.objects_removed += 1;
                removed_any = true;
            } else {
                stats.delete_failures += 1;
            }
        }
        if removed_any {
            stats.groups_evicted += 1;
            stats.bytes_freed = stats.bytes_freed.saturating_add(victim.freed);
        }
    }
    stats
}

/// `true` when the object is gone afterwards, including when a peer had already
/// removed it.
fn unlink(path: &Path) -> bool {
    match std::fs::remove_file(path) {
        Ok(()) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(error) => {
            tracing::debug!(path = %path.display(), "remote GC could not unlink: {error}");
            false
        }
    }
}

/// The cross-machine advisory lock for a remote sweep, or `None` when a peer
/// holds it.
///
/// The same mechanism as `Store::try_gc_lock` — [`StoreLock`](crate::store::StoreLock),
/// `flock(2)` on Unix and `LockFileEx` on Windows, released by the OS when the
/// holder exits — just anchored in the shared folder instead of the local store.
/// Two machines collecting at once is the default case for a filesystem remote,
/// not an edge case.
///
/// **Known limitation.** `flock(2)` only spans machines where the filesystem
/// implements it: it does on a local disk and on a correctly configured NFSv4 or
/// SMB mount, and silently does not on NFSv3 without `lockd` or on some FUSE
/// mounts. `cache_fs` already states that a shared remote needs working
/// POSIX/`LockFileEx` locking between its participants. Where the lock is a
/// no-op, two concurrent sweeps each plan against a snapshot the other is
/// deleting from; the failure mode is over-eviction and wasted unlinks — both
/// rank the same cold tail, so together they take more of it than either
/// intended — never a torn object and never a manifest left without its pack,
/// because every unlink is of one whole immutable object and the order within a
/// unit does not depend on the two sweeps agreeing.
pub(crate) fn try_lock(remote: &FilesystemRemoteConfig) -> Result<Option<crate::store::StoreLock>> {
    crate::store::StoreLock::try_acquire(&remote.root.join(REMOTE_GC_LOCK_FILE))
}

/// Volume size behind the remote root, for the derived budget.
pub(crate) fn volume_bytes(remote: &FilesystemRemoteConfig) -> Option<u64> {
    crate::cache_fs::probe(&remote.root).total_bytes
}

/// The remote's effective cap in bytes, or `None` when it is unbounded.
pub(crate) fn resolve_budget(remote: &FilesystemRemoteConfig) -> Option<u64> {
    remote.budget.resolve(volume_bytes(remote))
}

/// How the budget was arrived at, for `kache gc --remote` to print.
pub(crate) fn describe_budget(remote: &FilesystemRemoteConfig) -> String {
    crate::config::describe_remote_budget(remote.budget, volume_bytes(remote))
}

/// The warning a sweep prints when read recency is not observable, or `None`
/// when it is.
pub(crate) fn atime_advisory(scan: &RemoteScan) -> Option<&'static str> {
    (!scan.atime_advanced && scan.object_count > 0).then_some(
        "no object shows a read newer than its write, so this remote is probably mounted \
         noatime and eviction is ranking by WRITE time, not read time. Remount relatime to \
         rank by read recency, or set an explicit max_size you are comfortable with.",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RemoteBudget;

    const HOUR: Duration = Duration::from_secs(3600);

    fn file(key: &str, bytes: u64, idle: Duration) -> ScannedFile {
        ScannedFile {
            key: key.to_string(),
            path: PathBuf::from("/remote").join(key),
            bytes,
            idle,
            catalog: CatalogRead::None,
        }
    }

    fn catalog(key: &str, bytes: u64, idle: Duration, digests: &[&str]) -> ScannedFile {
        ScannedFile {
            catalog: CatalogRead::Digests(digests.iter().map(|d| d.to_string()).collect()),
            ..file(key, bytes, idle)
        }
    }

    fn v3_unit(crate_name: &str, cache_key: &str, bytes: u64, idle: Duration) -> Vec<ScannedFile> {
        vec![
            file(
                &format!("v3/manifests/{crate_name}/{cache_key}.json"),
                256,
                idle,
            ),
            file(
                &format!("v3/packs/{crate_name}/{cache_key}.tar.zst"),
                bytes,
                idle,
            ),
        ]
    }

    /// Mirror what [`scan`] fills in around [`assemble`], so tests can work on
    /// synthetic files.
    fn scan_of(files: Vec<ScannedFile>) -> RemoteScan {
        let total_bytes = files.iter().map(|f| f.bytes).sum();
        let object_count = files.len();
        RemoteScan {
            total_bytes,
            object_count,
            ..assemble(files)
        }
    }

    fn test_remote(root: &Path, budget: RemoteBudget) -> FilesystemRemoteConfig {
        FilesystemRemoteConfig {
            root: root.to_path_buf(),
            atomic_write_dir: root.join(".kache-tmp"),
            budget,
        }
    }

    // ── layout classification ───────────────────────────────────────────────

    #[test]
    fn classify_maps_each_layout_tree_to_its_eviction_unit() {
        assert_eq!(
            classify("v3/manifests/serde/abc123.json"),
            Classified::V3Manifest {
                unit: "serde/abc123".to_string()
            }
        );
        assert_eq!(
            classify("v3/packs/serde/abc123.tar.zst"),
            Classified::V3Pack {
                unit: "serde/abc123".to_string()
            }
        );
        assert_eq!(
            classify("v4/prefetch/catalogs/aa/0001-bb.json"),
            Classified::V4Catalog
        );
        assert_eq!(
            classify("v4/prefetch/packs/ab/abcdef.kpack"),
            Classified::V4PrefetchPack {
                digest: "abcdef".to_string()
            }
        );
        assert_eq!(
            classify("v4/prefetch/pack-meta/abcdef.json"),
            Classified::V4PrefetchPackMeta {
                digest: "abcdef".to_string()
            }
        );
    }

    /// Anything the layout does not produce is counted but never evicted —
    /// including build manifests and shards, which every prefetch plan starts
    /// from.
    #[test]
    fn classify_leaves_unknown_and_malformed_keys_alone() {
        for key in [
            "_manifests/v3/ns/shards/deadbeef.json",
            "_manifests/build-key.json",
            "v3/packs/serde/abc123.tar",
            "v3/manifests/abc123.json",
            "v3/manifests/a/b/c.json",
            "v3/packs/a/b/c.tar.zst",
            "v4/prefetch/packs/abcdef.kpack",
            "v4/prefetch/packs/a/b/c.kpack",
            "v4/prefetch/pack-meta/ab/cdef.json",
            "v4/prefetch/catalogs/aa/0001-bb.tmp",
            REMOTE_GC_LOCK_FILE,
            "",
        ] {
            assert_eq!(classify(key), Classified::Other, "{key}");
        }
    }

    /// The manifest and the pack of one entry land in a single group, with the
    /// manifest unlinked first (#774: never leave a manifest without its pack).
    #[test]
    fn a_v3_entry_is_one_group_that_drops_its_manifest_first() {
        let scan = scan_of(v3_unit("serde", "abc", 10_000, 100 * HOUR));
        assert_eq!(scan.groups.len(), 1);
        let group = &scan.groups[0];
        assert_eq!(group.id, "v3:serde/abc");
        assert_eq!(group.bytes, 10_256);
        assert!(group.digests.is_empty());
        assert_eq!(
            group.objects,
            vec![
                PathBuf::from("/remote/v3/manifests/serde/abc.json"),
                PathBuf::from("/remote/v3/packs/serde/abc.tar.zst"),
            ]
        );
        assert!(scan.orphans.is_empty());
    }

    /// A group is only as idle as its *most recently* touched object: reading
    /// the pack keeps the entry alive even when the manifest is never re-read.
    #[test]
    fn a_v3_group_takes_the_freshest_of_its_objects() {
        let mut files = v3_unit("serde", "abc", 10_000, 500 * HOUR);
        files[1].idle = 2 * HOUR;
        assert_eq!(scan_of(files).groups[0].idle, 2 * HOUR);

        let mut reversed = v3_unit("serde", "abc", 10_000, 500 * HOUR);
        reversed[0].idle = 3 * HOUR;
        assert_eq!(scan_of(reversed).groups[0].idle, 3 * HOUR);
    }

    // ── orphan repair ───────────────────────────────────────────────────────

    #[test]
    fn a_stale_half_of_a_v3_entry_is_an_orphan_in_either_direction() {
        let manifest_only = scan_of(vec![file("v3/manifests/serde/a.json", 200, 100 * HOUR)]);
        assert_eq!(manifest_only.orphans.len(), 1);
        assert_eq!(
            manifest_only.orphans[0].reason,
            OrphanReason::ManifestWithoutPack
        );
        assert_eq!(manifest_only.orphans[0].bytes, 200);

        let pack_only = scan_of(vec![file("v3/packs/serde/a.tar.zst", 900, 100 * HOUR)]);
        assert_eq!(pack_only.orphans.len(), 1);
        assert_eq!(
            pack_only.orphans[0].reason,
            OrphanReason::PackWithoutManifest
        );
        assert_eq!(pack_only.orphans[0].bytes, 900);
        assert!(pack_only.groups.is_empty());
    }

    /// An upload writes the pack and only then the manifest, so a fresh
    /// unpaired pack belongs to a build in flight and must survive.
    #[test]
    fn a_fresh_unpaired_pack_is_spared_by_the_publish_grace() {
        let fresh = scan_of(vec![file(
            "v3/packs/serde/a.tar.zst",
            900,
            REMOTE_ORPHAN_GRACE - Duration::from_secs(1),
        )]);
        assert!(
            fresh.orphans.is_empty(),
            "an in-flight publish must not be swept"
        );
        assert_eq!(fresh.unclassified_bytes, 900);

        let aged = scan_of(vec![file(
            "v3/packs/serde/a.tar.zst",
            900,
            REMOTE_ORPHAN_GRACE,
        )]);
        assert_eq!(
            aged.orphans.len(),
            1,
            "at the grace boundary it is an orphan"
        );
        assert_eq!(aged.unclassified_bytes, 0);
    }

    #[test]
    fn orphan_reasons_are_labelled_distinctly() {
        let labels = [
            OrphanReason::PackWithoutManifest.label(),
            OrphanReason::ManifestWithoutPack.label(),
            OrphanReason::UnreferencedPrefetchPack.label(),
        ];
        assert_eq!(
            labels
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            3
        );
    }

    /// Pins the translation into the local store's vocabulary field by field:
    /// the ranking is `SizePressurePolicy`'s, so anything set wrong here quietly
    /// changes what a remote sweep prefers.
    #[test]
    fn features_describe_a_group_in_the_local_eviction_vocabulary() {
        let mut shared = BTreeMap::new();
        shared.insert(
            "sole".to_string(),
            SharedPack {
                objects: vec![(PathBuf::from("/remote/sole.kpack"), 4_096)],
                bytes: 4_096,
                refs: 1,
            },
        );
        let group = RemoteGroup {
            id: "v4:cat".into(),
            objects: vec![PathBuf::from("/remote/cat.json")],
            bytes: 1_024,
            digests: vec!["sole".into()],
            idle: 2 * HOUR,
        };

        let features = group.features(&shared);
        assert_eq!(features.key, "v4:cat");
        assert_eq!(features.size, 1_024);
        assert_eq!(features.hit_count, 0, "a remote has no hit counter");
        assert_eq!(features.idle_hours, 2.0, "idle is expressed in hours");
        assert_eq!(features.compile_time_ms, 0);
        assert!(features.committed);
        assert_eq!(features.content_hash, None);
        assert_eq!(features.reclaimable_bytes, Some(5_120));
    }

    // ── the budget ──────────────────────────────────────────────────────────

    #[test]
    fn still_over_budget_is_strict_and_silent_without_a_budget() {
        let at_the_line = RemotePlan {
            budget: Some(100),
            projected_bytes: 100,
            ..RemotePlan::default()
        };
        assert!(
            !at_the_line.still_over_budget(),
            "exactly at the cap is fine"
        );
        assert!(
            RemotePlan {
                projected_bytes: 101,
                ..at_the_line.clone()
            }
            .still_over_budget()
        );
        assert!(
            !RemotePlan {
                budget: None,
                ..at_the_line
            }
            .still_over_budget()
        );
    }

    /// The walk stops the moment it reaches the target, not one group later.
    #[test]
    fn a_sweep_stops_exactly_when_it_reaches_the_target() {
        // Two groups, 110 + 90 bytes (plus 256 of manifest each). Budget 100
        // targets 90, and evicting only the larger group lands on it exactly.
        let mut files = v3_unit("a", "big", 110, 900 * HOUR);
        files.extend(v3_unit("a", "small", 90, 800 * HOUR));
        // Trim the manifests so the arithmetic is the two numbers above.
        for f in &mut files {
            if f.key.contains("manifests") {
                f.bytes = 0;
            }
        }
        let scan = scan_of(files);
        assert_eq!(scan.total_bytes, 200);

        let outcome = plan(&scan, Some(100));
        assert_eq!(outcome.target, Some(90));
        assert_eq!(
            outcome.victims.len(),
            1,
            "landing exactly on the target must end the walk"
        );
        assert_eq!(outcome.projected_bytes, 90);
    }

    #[test]
    fn an_unbounded_remote_still_repairs_orphans_but_evicts_nothing() {
        let mut files = v3_unit("serde", "hot", 10_000, 1000 * HOUR);
        files.push(file("v3/packs/serde/dead.tar.zst", 5_000, 1000 * HOUR));
        let scan = scan_of(files);

        let outcome = plan(&scan, None);
        assert!(
            outcome.victims.is_empty(),
            "no cap means no ranked eviction"
        );
        assert_eq!(outcome.target, None);
        assert_eq!(outcome.orphans.len(), 1);
        assert_eq!(outcome.bytes_freed(), 5_000);
        assert_eq!(outcome.projected_bytes, 10_256);
        assert!(!outcome.still_over_budget());
    }

    #[test]
    fn a_remote_under_its_budget_evicts_nothing() {
        let scan = scan_of(v3_unit("serde", "a", 10_000, 1000 * HOUR));
        let outcome = plan(&scan, Some(1_000_000));
        assert!(outcome.victims.is_empty());
        assert_eq!(outcome.projected_bytes, scan.total_bytes);
        assert!(!outcome.still_over_budget());
    }

    /// The sweep fires above the cap and stops at the 90% target, inheriting the
    /// local store's hysteresis band rather than parking on the line.
    #[test]
    fn a_sweep_stops_at_the_hysteresis_target_not_the_cap() {
        let mut files = Vec::new();
        for i in 0..10u32 {
            files.extend(v3_unit(
                "serde",
                &format!("k{i}"),
                100_000,
                HOUR * (1000 - i),
            ));
        }
        let scan = scan_of(files);
        let budget = 700_000;
        let outcome = plan(&scan, Some(budget));

        assert_eq!(outcome.target, Some(eviction_target(budget)));
        assert!(outcome.projected_bytes <= eviction_target(budget));
        assert!(outcome.projected_bytes <= budget);
        // It stops as soon as it is under: dropping the last victim would have
        // left it above the target.
        let last = outcome.victims.last().expect("at least one victim");
        assert!(outcome.projected_bytes + last.freed > eviction_target(budget));
    }

    /// Least-recently-read first. Every group is the same size, so idle time is
    /// the only thing separating them.
    #[test]
    fn ranking_evicts_the_least_recently_read_first() {
        let mut files = Vec::new();
        files.extend(v3_unit("a", "coldest", 100_000, 900 * HOUR));
        files.extend(v3_unit("a", "warm", 100_000, 100 * HOUR));
        files.extend(v3_unit("a", "hottest", 100_000, 30 * HOUR));
        let scan = scan_of(files);

        let outcome = plan(&scan, Some(200_000));
        let ids: Vec<&str> = outcome.victims.iter().map(|v| v.id.as_str()).collect();
        assert_eq!(ids, vec!["v3:a/coldest", "v3:a/warm"]);
    }

    /// Between two equally cold groups the bigger one goes first: a
    /// size-pressure sweep exists to free bytes.
    #[test]
    fn ranking_prefers_the_bigger_of_two_equally_cold_groups() {
        let mut files = Vec::new();
        files.extend(v3_unit("a", "small", 10_000, 500 * HOUR));
        files.extend(v3_unit("a", "big", 900_000, 500 * HOUR));
        let scan = scan_of(files);

        let outcome = plan(&scan, Some(500_000));
        assert_eq!(
            outcome.victims.first().map(|v| v.id.as_str()),
            Some("v3:a/big")
        );
    }

    /// A build may have listed a manifest and not yet issued the GET, so the
    /// grace pins recently touched groups even when they rank worst.
    #[test]
    fn the_recency_grace_pins_a_group_a_live_build_may_be_restoring() {
        let mut files = v3_unit("a", "inflight", 1_000_000, Duration::ZERO);
        files.extend(v3_unit("a", "cold", 1_000, 900 * HOUR));
        let scan = scan_of(files);

        let outcome = plan(&scan, Some(1_000));
        assert_eq!(outcome.pinned, 1);
        assert!(
            outcome.victims.iter().all(|v| v.id != "v3:a/inflight"),
            "a group inside the grace window must never be selected"
        );
        assert!(
            outcome.still_over_budget(),
            "the residue must be reported, not hidden"
        );
    }

    #[test]
    fn the_recency_grace_boundary_is_the_remote_restore_deadline() {
        let just_inside = scan_of(v3_unit(
            "a",
            "x",
            1_000_000,
            REMOTE_EVICTION_IDLE_GRACE - Duration::from_secs(1),
        ));
        let inside = plan(&just_inside, Some(1));
        assert_eq!(inside.pinned, 1);
        assert!(inside.victims.is_empty());

        let just_outside = scan_of(v3_unit("a", "x", 1_000_000, REMOTE_EVICTION_IDLE_GRACE));
        let outside = plan(&just_outside, Some(1));
        assert_eq!(outside.pinned, 0);
        assert_eq!(outside.victims.len(), 1);
    }

    // ── shared prefetch packs ───────────────────────────────────────────────

    /// The case that makes reference counting necessary: two catalogs name one
    /// content-addressed `.kpack`, so evicting the first catalog must leave the
    /// pack alone and evicting the second must take it.
    #[test]
    fn a_prefetch_pack_shared_by_two_catalogs_survives_the_first_eviction() {
        let shared_pack = PathBuf::from("/remote/v4/prefetch/packs/ab/abcd.kpack");
        let scan = scan_of(vec![
            file("v4/prefetch/packs/ab/abcd.kpack", 500_000, 900 * HOUR),
            catalog(
                "v4/prefetch/catalogs/s1/0001-aa.json",
                1_000,
                900 * HOUR,
                &["abcd"],
            ),
            catalog(
                "v4/prefetch/catalogs/s2/0002-bb.json",
                1_000,
                800 * HOUR,
                &["abcd"],
            ),
        ]);
        assert_eq!(scan.shared["abcd"].refs, 2);

        let outcome = plan(&scan, Some(1_000));
        assert_eq!(outcome.victims.len(), 2);
        assert_eq!(
            outcome.victims[0].id,
            "v4:v4/prefetch/catalogs/s1/0001-aa.json"
        );
        assert_eq!(
            outcome.victims[0].objects,
            vec![PathBuf::from(
                "/remote/v4/prefetch/catalogs/s1/0001-aa.json"
            )],
            "the shared pack must survive the first catalog that referenced it"
        );
        assert_eq!(outcome.victims[0].freed, 1_000);
        assert!(
            outcome.victims[1].objects.contains(&shared_pack),
            "the last catalog referencing a pack takes it with it"
        );
        assert_eq!(outcome.victims[1].freed, 501_000);
        assert_eq!(outcome.projected_bytes, 0);
    }

    /// Ranking sees the same thing: a catalog whose packs are all still shared
    /// frees only its own bytes, so an equally cold catalog holding a sole
    /// reference outranks it (kunobi-ninja/kache#608's marginal-bytes rule).
    #[test]
    fn a_catalog_holding_a_sole_reference_outranks_one_sharing_its_packs() {
        let scan = scan_of(vec![
            file("v4/prefetch/packs/aa/sole.kpack", 400_000, 900 * HOUR),
            file("v4/prefetch/packs/bb/many.kpack", 400_000, 900 * HOUR),
            catalog(
                "v4/prefetch/catalogs/s1/0001-aa.json",
                1_000,
                500 * HOUR,
                &["sole"],
            ),
            catalog(
                "v4/prefetch/catalogs/s2/0002-bb.json",
                1_000,
                500 * HOUR,
                &["many"],
            ),
            catalog(
                "v4/prefetch/catalogs/s3/0003-cc.json",
                1_000,
                10 * HOUR,
                &["many"],
            ),
        ]);

        let sole = scan
            .groups
            .iter()
            .find(|g| g.id.contains("0001"))
            .expect("sole-reference catalog");
        let many = scan
            .groups
            .iter()
            .find(|g| g.id.contains("0002"))
            .expect("shared-reference catalog");
        assert_eq!(sole.reclaimable(&scan.shared), 401_000);
        assert_eq!(
            many.reclaimable(&scan.shared),
            1_000,
            "a catalog whose packs are still shared frees only itself"
        );

        let order =
            SizePressurePolicy.select(&[many.features(&scan.shared), sole.features(&scan.shared)]);
        assert_eq!(order, vec![sole.id.clone(), many.id.clone()]);
    }

    #[test]
    fn a_catalog_registers_a_reference_and_its_pack_is_not_an_orphan() {
        let scan = scan_of(vec![
            file("v4/prefetch/packs/ab/abcd.kpack", 500_000, 900 * HOUR),
            file("v4/prefetch/pack-meta/abcd.json", 100, 900 * HOUR),
            catalog(
                "v4/prefetch/catalogs/sel/0001-cc.json",
                700,
                900 * HOUR,
                &["abcd"],
            ),
        ]);
        assert_eq!(scan.shared["abcd"].refs, 1);
        assert_eq!(scan.shared["abcd"].bytes, 500_100);
        assert_eq!(scan.shared["abcd"].objects.len(), 2);
        assert!(
            scan.orphans.is_empty(),
            "a referenced prefetch pack is not an orphan"
        );
        assert_eq!(scan.groups.len(), 1);
        assert_eq!(scan.groups[0].digests, vec!["abcd".to_string()]);
    }

    /// Evicting the sole catalog takes the pack *and* its metadata sidecar.
    #[test]
    fn evicting_the_last_catalog_takes_the_pack_and_its_sidecar() {
        let scan = scan_of(vec![
            file("v4/prefetch/packs/ab/abcd.kpack", 500_000, 900 * HOUR),
            file("v4/prefetch/pack-meta/abcd.json", 100, 900 * HOUR),
            catalog(
                "v4/prefetch/catalogs/sel/0001-cc.json",
                700,
                900 * HOUR,
                &["abcd"],
            ),
        ]);
        let outcome = plan(&scan, Some(1_000));
        assert_eq!(outcome.victims.len(), 1);
        assert_eq!(outcome.victims[0].objects.len(), 3);
        assert_eq!(outcome.victims[0].freed, 500_800);
    }

    #[test]
    fn an_unreferenced_prefetch_pack_is_an_orphan() {
        let scan = scan_of(vec![
            file("v4/prefetch/packs/ab/abcd.kpack", 500_000, 900 * HOUR),
            file("v4/prefetch/pack-meta/abcd.json", 100, 900 * HOUR),
        ]);
        assert_eq!(scan.orphans.len(), 2);
        assert!(
            scan.orphans
                .iter()
                .all(|o| o.reason == OrphanReason::UnreferencedPrefetchPack)
        );
        assert_eq!(scan.orphans.iter().map(|o| o.bytes).sum::<u64>(), 500_100);
    }

    /// Fail closed: a catalog we cannot parse may hold the only reference to a
    /// pack, so no prefetch pack is orphaned on a pass that saw one.
    #[test]
    fn an_unreadable_catalog_suppresses_prefetch_orphaning() {
        let mut torn = file("v4/prefetch/catalogs/sel/0001-cc.json", 11, 900 * HOUR);
        torn.catalog = CatalogRead::Unreadable;
        let scan = scan_of(vec![
            file("v4/prefetch/packs/ab/abcd.kpack", 500_000, 900 * HOUR),
            torn,
        ]);

        assert!(
            scan.orphans.is_empty(),
            "an unparseable catalog must not let its packs look unreferenced"
        );
        assert!(scan.groups.is_empty());
        assert_eq!(scan.unclassified_bytes, 11);
    }

    #[test]
    fn catalog_digests_reads_pack_references_and_rejects_garbage() {
        assert_eq!(
            catalog_digests(br#"{"packs":[{"digest":"aa"},{"digest":"bb"}]}"#).unwrap(),
            vec!["aa".to_string(), "bb".to_string()]
        );
        // Unknown fields are tolerated so a newer kache's catalog does not read
        // as torn and suppress the sweep.
        assert_eq!(
            catalog_digests(br#"{"version":9,"packs":[{"digest":"aa","pack_bytes":7}]}"#).unwrap(),
            vec!["aa".to_string()]
        );
        assert!(catalog_digests(b"not json").is_err());
        assert!(catalog_digests(b"{}").is_err());
    }

    // ── timestamps ──────────────────────────────────────────────────────────

    #[test]
    fn observed_access_is_the_later_of_read_and_write() {
        let early = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let late = SystemTime::UNIX_EPOCH + Duration::from_secs(900);
        // relatime/strictatime: the read is newer, so read recency wins.
        assert_eq!(observed_access(Some(late), Some(early)), Some(late));
        // noatime: atime is frozen at or before the write, so write time wins.
        assert_eq!(observed_access(Some(early), Some(late)), Some(late));
        assert_eq!(observed_access(None, Some(late)), Some(late));
        assert_eq!(observed_access(Some(late), None), Some(late));
        assert_eq!(observed_access(None, None), None);
    }

    #[test]
    fn atime_is_advanced_only_when_a_read_outran_the_write() {
        let early = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let late = SystemTime::UNIX_EPOCH + Duration::from_secs(900);
        assert!(atime_is_advanced(Some(late), Some(early)));
        assert!(!atime_is_advanced(Some(early), Some(late)));
        assert!(!atime_is_advanced(Some(early), Some(early)));
        assert!(!atime_is_advanced(None, Some(early)));
        assert!(!atime_is_advanced(Some(late), None));
        assert!(!atime_is_advanced(None, None));
    }

    #[test]
    fn idle_never_goes_negative_under_clock_skew() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        assert_eq!(
            idle_since(now, SystemTime::UNIX_EPOCH + Duration::from_secs(500)),
            Duration::ZERO
        );
        assert_eq!(
            idle_since(now, SystemTime::UNIX_EPOCH + Duration::from_secs(40)),
            Duration::from_secs(60)
        );
    }

    /// The `noatime` advisory fires exactly when no object in a non-empty
    /// remote shows a read newer than its write.
    #[test]
    fn the_noatime_advisory_tracks_whether_any_read_was_recorded() {
        let mut scan = RemoteScan {
            object_count: 4,
            ..RemoteScan::default()
        };
        assert!(atime_advisory(&scan).is_some_and(|text| text.contains("noatime")));
        scan.atime_advanced = true;
        assert!(atime_advisory(&scan).is_none());

        assert!(
            atime_advisory(&RemoteScan::default()).is_none(),
            "an empty remote proves nothing about the mount"
        );
    }

    // ── end to end against a real directory ─────────────────────────────────

    fn write_object(root: &Path, rel: &str, bytes: usize) -> PathBuf {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, vec![7u8; bytes]).unwrap();
        path
    }

    /// Build a remote on disk, scan it, and check the measured size, the
    /// staging exclusion, and that planning deletes nothing.
    #[test]
    fn scanning_a_real_remote_measures_it_and_skips_staging() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut remote = test_remote(root, RemoteBudget::Bytes(1));
        // Staging is allowed to live inside the prefix as long as it is outside
        // the v3 object tree, so put it there: that is the placement the
        // exclusion list actually has to defend against.
        remote.atomic_write_dir = root.join("artifacts/.staging");

        write_object(root, "artifacts/v3/manifests/serde/aaa.json", 100);
        let pack = write_object(root, "artifacts/v3/packs/serde/aaa.tar.zst", 4_000);
        write_object(root, "artifacts/_manifests/v3/ns/shards/dd.json", 60);
        write_object(root, "artifacts/.staging/inflight.tmp", 9_999);

        let scan = scan(&remote, "artifacts", SystemTime::now()).unwrap();
        assert_eq!(scan.object_count, 3, "staging is not a remote object");
        assert_eq!(scan.total_bytes, 4_160);
        assert_eq!(
            scan.unclassified_bytes, 60,
            "_manifests counts toward the budget but is never evicted"
        );
        assert_eq!(scan.groups.len(), 1);
        assert_eq!(scan.groups[0].id, "v3:serde/aaa");

        // Just-written files are inside the recency grace, so nothing is picked.
        let dry = plan(&scan, Some(1));
        assert_eq!(dry.pinned, 1);
        assert!(dry.victims.is_empty());
        assert!(pack.exists(), "planning must never touch the filesystem");
    }

    /// The scan reports whether the mount recorded a read. Timestamps are set
    /// explicitly because the host's own mount options must not decide the
    /// answer.
    #[test]
    fn scanning_detects_whether_the_mount_records_reads() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let objects = [
            write_object(root, "artifacts/v3/manifests/serde/a.json", 10),
            write_object(root, "artifacts/v3/packs/serde/a.tar.zst", 10),
        ];
        let written = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000_000);
        let remote = test_remote(root, RemoteBudget::Default);

        // noatime: the access time never outran the write.
        let stamp = filetime::FileTime::from_system_time(written);
        for path in &objects {
            filetime::set_file_times(path, stamp, stamp).unwrap();
        }
        let frozen = scan(&remote, "artifacts", SystemTime::now()).unwrap();
        assert!(!frozen.atime_advanced);
        assert!(atime_advisory(&frozen).is_some());

        // relatime: a read pushed the access time past the write.
        let read_at = filetime::FileTime::from_system_time(written + 100 * HOUR);
        for path in &objects {
            filetime::set_file_times(path, read_at, stamp).unwrap();
        }
        let observed = scan(&remote, "artifacts", SystemTime::now()).unwrap();
        assert!(observed.atime_advanced);
        assert!(atime_advisory(&observed).is_none());
        // And the group dates from the read, not the write.
        assert_eq!(observed.groups.len(), 1);
        assert!(
            observed.groups[0].idle + 99 * HOUR < frozen.groups[0].idle,
            "a recorded read must make the group look fresher"
        );
    }

    /// A real catalog on disk is parsed during the scan, so its packs are
    /// reference-counted rather than orphaned.
    #[test]
    fn scanning_reads_catalogs_off_disk_to_count_references() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write_object(root, "artifacts/v4/prefetch/packs/ab/abcd.kpack", 2_000);
        let catalog_path = root.join("artifacts/v4/prefetch/catalogs/sel/0001-cc.json");
        std::fs::create_dir_all(catalog_path.parent().unwrap()).unwrap();
        std::fs::write(&catalog_path, br#"{"packs":[{"digest":"abcd"}]}"#).unwrap();

        let counted = scan(
            &test_remote(root, RemoteBudget::Default),
            "artifacts",
            SystemTime::now(),
        )
        .unwrap();
        assert_eq!(counted.shared["abcd"].refs, 1);
        assert!(counted.orphans.is_empty());

        // A torn catalog on the same pass suppresses v4 orphaning entirely.
        std::fs::write(&catalog_path, b"{ half-writ").unwrap();
        let torn = scan(
            &test_remote(root, RemoteBudget::Default),
            "artifacts",
            SystemTime::now(),
        )
        .unwrap();
        assert_eq!(torn.shared["abcd"].refs, 0);
        assert!(
            torn.orphans.is_empty(),
            "a catalog we cannot read must fail closed"
        );
    }

    /// Applying a plan unlinks exactly the planned objects and leaves
    /// everything else in place.
    #[test]
    fn applying_a_plan_removes_only_the_planned_objects() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let manifest = root.join("m.json");
        let pack = root.join("p.tar.zst");
        let keep = root.join("keep.json");
        for path in [&manifest, &pack, &keep] {
            std::fs::write(path, b"x").unwrap();
        }
        let plan = RemotePlan {
            victims: vec![PlannedVictim {
                id: "v3:a/b".into(),
                objects: vec![manifest.clone(), pack.clone()],
                freed: 2,
            }],
            orphans: vec![RemoteOrphan {
                path: root.join("gone-already.json"),
                bytes: 5,
                reason: OrphanReason::PackWithoutManifest,
            }],
            ..RemotePlan::default()
        };

        let stats = apply(&plan);
        assert!(!manifest.exists() && !pack.exists());
        assert!(keep.exists());
        assert_eq!(stats.groups_evicted, 1);
        assert_eq!(stats.orphans_removed, 1);
        assert_eq!(stats.objects_removed, 3);
        assert_eq!(stats.bytes_freed, 7);
        assert_eq!(
            stats.delete_failures, 0,
            "an object a peer already removed is not a failure"
        );
    }

    /// An object that cannot be unlinked — the stand-in for a Windows reader
    /// holding the handle open — is counted and stepped over, never propagated.
    #[test]
    fn an_undeletable_object_is_counted_and_does_not_abort_the_sweep() {
        let dir = tempfile::tempdir().unwrap();
        let stuck = dir.path().join("stuck");
        std::fs::create_dir(&stuck).unwrap();
        assert!(!unlink(&stuck));
        assert!(unlink(&dir.path().join("absent")));

        // Both loops have to survive it: a stuck orphan and a stuck victim.
        let stats = apply(&RemotePlan {
            orphans: vec![RemoteOrphan {
                path: stuck.clone(),
                bytes: 7,
                reason: OrphanReason::PackWithoutManifest,
            }],
            victims: vec![PlannedVictim {
                id: "v3:a/b".into(),
                objects: vec![stuck.clone()],
                freed: 99,
            }],
            ..RemotePlan::default()
        });
        assert_eq!(stats.delete_failures, 2);
        assert_eq!(stats.orphans_removed, 0);
        assert_eq!(stats.objects_removed, 0);
        assert_eq!(stats.groups_evicted, 0);
        assert_eq!(stats.bytes_freed, 0);
        assert!(stuck.exists());
    }

    #[test]
    fn a_symlink_under_the_prefix_is_never_walked_or_counted() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let outside = root.join("outside.bin");
        std::fs::write(&outside, vec![1u8; 5_000]).unwrap();
        std::fs::create_dir_all(root.join("artifacts/v3/packs/serde")).unwrap();
        let link = root.join("artifacts/v3/packs/serde/a.tar.zst");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside, &link).unwrap();
        #[cfg(windows)]
        std::os::windows::fs::symlink_file(&outside, &link).unwrap();

        let scan = scan(
            &test_remote(root, RemoteBudget::Default),
            "artifacts",
            SystemTime::now(),
        )
        .unwrap();
        assert_eq!(scan.object_count, 0, "a symlink is not a remote object");
        assert_eq!(scan.total_bytes, 0);
        assert!(outside.exists());
    }

    #[test]
    fn an_empty_prefix_scans_the_remote_root_and_still_skips_the_lock() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write_object(root, "v3/packs/serde/a.tar.zst", 300);
        write_object(root, REMOTE_GC_LOCK_FILE, 4);

        let scan = scan(
            &test_remote(root, RemoteBudget::Default),
            "",
            SystemTime::now(),
        )
        .unwrap();
        assert_eq!(scan.total_bytes, 300);
        assert_eq!(scan.object_count, 1);
    }

    #[test]
    fn relative_key_rejects_paths_outside_the_prefix() {
        assert_eq!(
            relative_key(
                Path::new("/remote/artifacts"),
                Path::new("/remote/artifacts/v3/a.json")
            ),
            Some("v3/a.json".to_string())
        );
        assert_eq!(
            relative_key(
                Path::new("/remote/artifacts"),
                Path::new("/elsewhere/a.json")
            ),
            None
        );
    }

    // ── the concurrency guard ───────────────────────────────────────────────

    /// Two collectors on one shared folder: the second must decline rather than
    /// plan against a snapshot the first is deleting from.
    #[test]
    fn only_one_remote_sweep_holds_the_advisory_lock() {
        let dir = tempfile::tempdir().unwrap();
        let remote = test_remote(dir.path(), RemoteBudget::Default);

        let first = try_lock(&remote).unwrap().expect("the first sweep wins");
        assert!(
            try_lock(&remote).unwrap().is_none(),
            "a second concurrent sweep must be turned away"
        );
        drop(first);
        assert!(
            try_lock(&remote).unwrap().is_some(),
            "the lock is released when the sweep ends"
        );
        assert!(
            dir.path().join(REMOTE_GC_LOCK_FILE).exists(),
            "the lock file persists so contenders share one inode"
        );
    }

    // ── budget resolution ───────────────────────────────────────────────────

    #[test]
    fn an_explicit_budget_wins_over_the_derived_default() {
        let dir = tempfile::tempdir().unwrap();
        let remote = |budget| test_remote(dir.path(), budget);
        assert_eq!(
            resolve_budget(&remote(RemoteBudget::Bytes(80 << 30))),
            Some(80 << 30)
        );
        assert_eq!(resolve_budget(&remote(RemoteBudget::Unbounded)), None);
        assert_eq!(
            resolve_budget(&remote(RemoteBudget::Default)),
            Some(crate::config::remote_disk_share_budget(volume_bytes(
                &remote(RemoteBudget::Default)
            )))
        );
        assert!(
            volume_bytes(&remote(RemoteBudget::Default)).is_some_and(|n| n > 1_000_000),
            "a real directory must yield a volume size"
        );

        assert!(describe_budget(&remote(RemoteBudget::Unbounded)).contains("none"));
        assert!(describe_budget(&remote(RemoteBudget::Bytes(1 << 30))).contains("configured"));
        assert!(describe_budget(&remote(RemoteBudget::Default)).contains('%'));
    }

    #[test]
    fn plan_totals_add_up_across_orphans_and_victims() {
        let mut files = v3_unit("a", "cold", 100_000, 900 * HOUR);
        files.push(file("v3/packs/a/dead.tar.zst", 7_000, 900 * HOUR));
        let scan = scan_of(files);
        let outcome = plan(&scan, Some(1_000));

        assert_eq!(outcome.orphans.len(), 1);
        assert_eq!(outcome.victims.len(), 1);
        assert_eq!(outcome.bytes_freed(), 7_000 + 100_256);
        assert_eq!(outcome.projected_bytes, 0);
        assert_eq!(outcome.total_bytes, 107_256);
        assert_eq!(outcome.object_count, 3);
        assert!(!outcome.still_over_budget());
    }
}
