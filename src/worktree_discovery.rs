//! Find Cargo targets in the Git worktrees of repositories kache knows.
//!
//! This runs during quiet daemon maintenance, at most hourly, never in a
//! compiler wrapper or a command. Every tracked target's workspace
//! leads to its repository, and from there to the other repositories in the
//! same folders. Git lists each repository's worktrees and, in each, the
//! directories it ignores; every ignored `target/` Cargo has tagged is
//! recorded. Discovery does not claim a build used the target or make it
//! eligible for automatic cleanup.

use crate::config::Config;
use crate::maintenance::{Trigger, is_quiet, unix_now_secs};
use crate::store::Store;
use anyhow::Result;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const GIT_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_INVENTORY_BYTES: u64 = 1 << 20;
/// How long Git may take to list one worktree's ignored directories.
const LISTING_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_LISTING_BYTES: u64 = 4 << 20;
/// Repositories one discovery pass may ask Git about.
const DISCOVERY_STEPS: usize = 2_000;
/// How long one discovery pass may spend.
const DISCOVERY_TIME: Duration = Duration::from_secs(20);
/// How long what a pass found counts as current.
const FRESH_FOR: Duration = Duration::from_secs(3600);
/// Holds the Unix time discovery last ran, so a restarted daemon keeps the
/// hourly pace. A new install or an upgrade has none, and discovers at its
/// first quiet check.
const LAST_RUN_FILE: &str = "target-discovery.last";

/// When this process last ran discovery, for a volume too full to keep
/// [`LAST_RUN_FILE`]: `0` before the first run.
static LAST_RUN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Whether a run last at `last` (Unix seconds, `0` for never) is stale at
/// `now`. A clock set back behind it does not stop discovery.
fn stale(last: u64, now: u64) -> bool {
    last == 0 || now < last || now - last >= FRESH_FOR.as_secs()
}

/// Whether to discover now: a periodic check (never the shutdown pass) on a
/// quiet machine, when both the recorded and the remembered last run are
/// stale.
fn due(periodic: bool, quiet: bool, recorded: u64, remembered: u64, now: u64) -> bool {
    periodic && quiet && stale(recorded, now) && stale(remembered, now)
}

/// The maintenance step: on a quiet machine, discover when the last run is
/// an hour old or there is none, and record when. `true` when it ran, so the
/// pass leaves its other walks for the next check.
pub(crate) fn run(config: &Config, trigger: Trigger<'_>) -> bool {
    let now = unix_now_secs();
    let record = config.cache_dir.join(LAST_RUN_FILE);
    let recorded = std::fs::read_to_string(&record)
        .ok()
        .and_then(|text| text.trim().parse().ok())
        .unwrap_or(0);
    let remembered = LAST_RUN.load(std::sync::atomic::Ordering::Relaxed);
    let periodic = matches!(trigger, Trigger::Periodic(_));
    let quiet = is_quiet(
        crate::scheduler::permits_in_use(&config.cache_dir),
        trigger.idle_for(),
    );
    if !due(periodic, quiet, recorded, remembered, now) {
        return false;
    }
    // Recorded before the pass, so a pass that fails waits an hour too.
    LAST_RUN.store(now, std::sync::atomic::Ordering::Relaxed);
    if let Err(error) = crate::atomic::atomic_replace(&record, now.to_string().as_bytes()) {
        tracing::warn!("cannot record the target discovery time: {error:#}");
    }
    match Store::open(config).and_then(|store| discover(&store, DISCOVERY_TIME, now)) {
        Ok(0) => {}
        Ok(found) => tracing::info!("found {found} target directories in Git worktrees"),
        Err(error) => tracing::warn!("target discovery failed: {error:#}"),
    }
    true
}

#[derive(Debug, PartialEq, Eq)]
struct Worktree {
    path: PathBuf,
    bare: bool,
    locked: bool,
    prunable: bool,
}

/// Parse Git's NUL-delimited porcelain records. An unknown attribute does not
/// change the meaning of the attributes used for cleanup decisions.
fn parse_worktrees(bytes: &[u8]) -> Vec<Worktree> {
    let mut trees = Vec::new();
    let mut current: Option<Worktree> = None;
    for field in bytes.split(|byte| *byte == 0) {
        if field.is_empty() {
            if let Some(tree) = current.take() {
                trees.push(tree);
            }
            continue;
        }
        if let Some(path) = field.strip_prefix(b"worktree ") {
            if let Some(tree) = current.take() {
                trees.push(tree);
            }
            #[cfg(unix)]
            let path = {
                use std::os::unix::ffi::OsStringExt;
                Some(PathBuf::from(std::ffi::OsString::from_vec(path.to_vec())))
            };
            #[cfg(not(unix))]
            let path = String::from_utf8(path.to_vec()).ok().map(PathBuf::from);
            current = path.map(|path| Worktree {
                path,
                bare: false,
                locked: false,
                prunable: false,
            });
        } else if let Some(tree) = current.as_mut() {
            tree.bare |= field == b"bare";
            tree.locked |= field == b"locked" || field.starts_with(b"locked ");
            tree.prunable |= field == b"prunable" || field.starts_with(b"prunable ");
        }
    }
    if let Some(tree) = current {
        trees.push(tree);
    }
    trees
}

/// `git -C dir <args>`'s output, or `None` if Git fails, outruns `timeout`
/// or `deadline`, or writes `max_bytes` or more. The repository's own
/// filesystem monitor is switched off: discovery reads repositories no build
/// ran in, and a configured `core.fsmonitor` is a program Git would run.
fn git_output(
    dir: &Path,
    args: &[&str],
    timeout: Duration,
    deadline: Instant,
    max_bytes: u64,
) -> Option<Vec<u8>> {
    let deadline = git_deadline(Instant::now(), timeout, deadline)?;
    let mut child = crate::platform::host_spawn(
        Command::new("git")
            .args(["-c", "core.fsmonitor=false", "-C"])
            .arg(dir)
            .args(args)
            .env("GIT_OPTIONAL_LOCKS", "0")
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_COMMON_DIR")
            .env_remove("GIT_INDEX_FILE")
            .stdout(Stdio::piped())
            .stderr(Stdio::null()),
    )
    .ok()?;
    let stdout = child.stdout.take()?;
    let reader = match std::thread::Builder::new()
        .name("kache-git-discovery".into())
        .spawn(move || {
            let mut bytes = Vec::new();
            stdout.take(max_bytes).read_to_end(&mut bytes).ok()?;
            Some(bytes)
        }) {
        Ok(reader) => reader,
        Err(_) => {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
    };
    let status = loop {
        if Instant::now() >= deadline {
            break None;
        }
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(_) => break None,
        }
    };
    if status.is_none() {
        let _ = child.kill();
        let _ = child.wait();
    }
    let bytes = reader.join().ok().flatten()?;
    if !status?.success() || bytes.len() as u64 == max_bytes {
        return None;
    }
    Some(bytes)
}

fn git_deadline(started: Instant, timeout: Duration, deadline: Instant) -> Option<Instant> {
    let deadline = deadline.min(started.checked_add(timeout)?);
    (deadline > started).then_some(deadline)
}

fn inventory(workspace: &Path, deadline: Instant) -> Option<Vec<Worktree>> {
    let bytes = git_output(
        workspace,
        &["worktree", "list", "--porcelain", "-z"],
        GIT_TIMEOUT,
        deadline,
        MAX_INVENTORY_BYTES,
    )?;
    Some(
        parse_worktrees(&bytes)
            .into_iter()
            .filter_map(|mut tree| {
                tree.path = tree.path.canonicalize().ok()?;
                Some(tree)
            })
            .collect(),
    )
}

/// Where `seed` sits in its own worktree, relative to that worktree's root:
/// the nearest of its ancestors among `roots`.
fn seed_suffix(seed: &Path, roots: &HashSet<&Path>) -> Option<PathBuf> {
    let root = seed.ancestors().find(|dir| roots.contains(dir))?;
    seed.strip_prefix(root).ok().map(Path::to_path_buf)
}

/// The roots of `trees`, for [`seed_suffix`].
fn roots_of(trees: &[Worktree]) -> HashSet<&Path> {
    trees.iter().map(|tree| tree.path.as_path()).collect()
}

/// The workspaces at `suffix` in each of `trees` that has one.
fn workspaces_at<'a>(
    suffix: &'a Path,
    trees: &'a [&'a Worktree],
) -> impl Iterator<Item = PathBuf> + 'a {
    trees
        .iter()
        // `join("")` would add a trailing separator to the worktree's root.
        .map(move |tree| {
            if suffix.as_os_str().is_empty() {
                tree.path.clone()
            } else {
                tree.path.join(suffix)
            }
        })
        .filter(|workspace| {
            std::fs::symlink_metadata(workspace).is_ok_and(|meta| meta.file_type().is_dir())
                && workspace.join("Cargo.toml").is_file()
        })
}

/// Whether discovery may read `tree`: available to Git, and not shielded.
fn readable(tree: &Worktree, homes: &[PathBuf]) -> bool {
    !tree.bare && !tree.locked && !tree.prunable && !shielded(&tree.path, homes)
}

/// Map the source workspace's path relative to its Git worktree into each
/// sibling. A repository may contain more than one Cargo workspace.
#[cfg(test)]
fn sibling_workspaces(seed: &Path, trees: &[Worktree]) -> Vec<PathBuf> {
    let available: Vec<_> = trees.iter().filter(|tree| readable(tree, &[])).collect();
    seed_suffix(seed, &roots_of(trees))
        .map(|suffix| workspaces_at(&suffix, &available).collect())
        .unwrap_or_default()
}

/// The `target/` directories among a worktree's ignored ones, as Git lists
/// them with `ls-files --others --ignored --directory`: paths relative to
/// the worktree, NUL-separated, a directory with a trailing slash. Git does
/// not descend into an ignored directory, so a target inside another one
/// (`node_modules/…/target`, `target/package/…/target`) is not listed.
fn ignored_target_dirs(worktree: &Path, listing: &[u8]) -> Vec<PathBuf> {
    listing
        .split(|byte| *byte == 0)
        .filter_map(|entry| entry.strip_suffix(b"/"))
        .filter(|dir| *dir == b"target" || dir.ends_with(b"/target"))
        .filter_map(|dir| std::str::from_utf8(dir).ok())
        .map(|dir| worktree.join(dir))
        .collect()
}

/// Cargo's targets in `worktree`, as (target, workspace): each ignored
/// `target/` Cargo tagged. The workspace is the directory holding it, even
/// once its Cargo.toml is gone: a removed crate leaves its output behind.
fn targets_in_worktree(worktree: &Path, deadline: Instant) -> Vec<(PathBuf, PathBuf)> {
    let Some(listing) = git_output(
        worktree,
        &[
            "ls-files",
            "-z",
            "--others",
            "--ignored",
            "--exclude-standard",
            "--directory",
        ],
        LISTING_TIMEOUT,
        deadline,
        MAX_LISTING_BYTES,
    ) else {
        return Vec::new();
    };
    ignored_target_dirs(worktree, &listing)
        .into_iter()
        .filter(|target| crate::machine::cargo_tagged(target))
        .filter_map(|target| Some((target.clone(), target.parent()?.to_path_buf())))
        .collect()
}

/// Places discovery never reads: the filesystem root, the home folder
/// itself (a dotfiles repository there would be listed whole), and folders
/// macOS asks the user about before an app reads them. Discovery neither
/// lists them, nor asks Git about them, nor crawls them. `homes` is the home
/// folder as spelled and as resolved, so a symlinked home is still matched.
fn shielded(dir: &Path, homes: &[PathBuf]) -> bool {
    const PROTECTED: [&str; 9] = [
        "Desktop",
        "Documents",
        "Downloads",
        "Library",
        "Pictures",
        "Music",
        "Movies",
        "Applications",
        "Public",
    ];
    dir.parent().is_none()
        || homes.iter().any(|home| {
            dir == home
                || PROTECTED
                    .iter()
                    .any(|name| dir.starts_with(home.join(name)))
        })
}

/// Git repositories directly in `dir`: children holding a `.git` directory
/// or worktree file, read one entry at a time until `deadline`.
fn repositories_in(dir: PathBuf, deadline: Instant) -> impl Iterator<Item = PathBuf> {
    std::fs::read_dir(&dir)
        .into_iter()
        .flatten()
        .take_while(move |_| deadline.checked_duration_since(Instant::now()).is_some())
        .flatten()
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .map(|entry| entry.path())
        .filter(|child| std::fs::symlink_metadata(child.join(".git")).is_ok())
}

/// The Git directory every worktree of the repository holding `path`
/// shares, read from the filesystem without running Git: the nearest
/// `.git` directory, or the one a worktree's `.git` file points to through
/// its `commondir`. Two paths in one repository give the same answer.
fn repository_of(path: &Path) -> Option<PathBuf> {
    let dot_git = path
        .ancestors()
        .map(|dir| dir.join(".git"))
        .find(|dot_git| std::fs::symlink_metadata(dot_git).is_ok())?;
    if dot_git.is_dir() {
        return dot_git.canonicalize().ok();
    }
    let text = std::fs::read_to_string(&dot_git).ok()?;
    let git_dir = Path::new(text.strip_prefix("gitdir:")?.trim());
    let git_dir = dot_git.parent()?.join(git_dir);
    let common = std::fs::read_to_string(git_dir.join("commondir"))
        .map_or_else(|_| git_dir.clone(), |common| git_dir.join(common.trim()));
    common.canonicalize().ok()
}

/// `items` turned by `turn` places: each pass starts at a different one, so
/// a pass that runs out of time does not leave the same ones out every time.
fn rotated<T>(mut items: Vec<T>, turn: u64) -> Vec<T> {
    if !items.is_empty() {
        let by = (turn % items.len() as u64) as usize;
        items.rotate_left(by);
    }
    items
}

/// The turn for [`rotated`] at `now` (Unix seconds): the hour, as discovery
/// runs at most hourly.
fn turn_at(now: u64) -> u64 {
    now / FRESH_FOR.as_secs()
}

/// The home folder from its variable, as spelled and as resolved; none when
/// it is unset or empty.
fn homes_from(home: Option<std::ffi::OsString>) -> Vec<PathBuf> {
    let Some(home) = home.filter(|home| !home.is_empty()).map(PathBuf::from) else {
        return Vec::new();
    };
    let resolved = home.canonicalize().ok();
    let mut homes = vec![home];
    homes.extend(resolved.filter(|resolved| *resolved != homes[0]));
    homes
}

/// What Git says about one repository: its worktrees, and the targets in
/// them, as (target, workspace).
struct Survey {
    trees: Vec<Worktree>,
    targets: Vec<(PathBuf, PathBuf)>,
}

/// Run `task` on each item, several at once (each mostly waits on Git and
/// the filesystem), and return the results in the items' order.
fn in_parallel<T: Sync, R: Send>(items: &[T], task: impl Fn(&T) -> R + Sync) -> Vec<R> {
    let workers = std::thread::available_parallelism()
        .map_or(4, std::num::NonZeroUsize::get)
        .clamp(1, 8);
    let next = std::sync::atomic::AtomicUsize::new(0);
    let done = std::sync::Mutex::new(Vec::with_capacity(items.len()));
    std::thread::scope(|scope| {
        for _ in 0..workers.min(items.len()) {
            scope.spawn(|| {
                loop {
                    let index = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let Some(item) = items.get(index) else {
                        break;
                    };
                    let result = task(item);
                    if let Ok(mut done) = done.lock() {
                        done.push((index, result));
                    }
                }
            });
        }
    });
    let mut done = done
        .into_inner()
        .unwrap_or_else(|poison| poison.into_inner());
    done.sort_by_key(|(index, _)| *index);
    done.into_iter().map(|(_, result)| result).collect()
}

/// Survey each repository, given as a start inside it and the seed
/// workspaces in it: list its worktrees, then, across every readable
/// worktree at once, the tagged targets Git ignores. Each seed's place in
/// its worktree is also looked at in every sibling, once per place, which
/// finds a target there that Git does not ignore. Starts no Git after
/// `deadline`.
fn survey_all(
    repositories: &[(PathBuf, Vec<PathBuf>)],
    deadline: Instant,
    homes: &[PathBuf],
    turn: u64,
) -> Vec<Survey> {
    // Asked in a turned order, so a pass that runs out of time leaves out
    // different repositories each time; answers go back in the given order.
    let order = rotated((0..repositories.len()).collect(), turn);
    let mut inventories: Vec<Option<Vec<Worktree>>> = std::iter::repeat_with(|| None)
        .take(repositories.len())
        .collect();
    let asked = in_parallel(&order, |index| inventory(&repositories[*index].0, deadline));
    for (index, trees) in order.into_iter().zip(asked) {
        inventories[index] = trees;
    }
    let worktrees: Vec<PathBuf> = inventories
        .iter()
        .flatten()
        .flatten()
        .filter(|tree| readable(tree, homes))
        .map(|tree| tree.path.clone())
        .collect();
    let worktrees = rotated(worktrees, turn);
    let listed = in_parallel(&worktrees, |worktree| {
        targets_in_worktree(worktree, deadline)
    });
    let mut listed: HashMap<_, _> = worktrees.into_iter().zip(listed).collect();
    // In the order given, so builds' workspaces are recorded first.
    repositories
        .iter()
        .zip(inventories)
        .filter_map(|((_, seeds), trees)| {
            let trees = trees?;
            let available: Vec<_> = trees.iter().filter(|tree| readable(tree, homes)).collect();
            let roots = roots_of(&trees);
            let suffixes: BTreeSet<_> = seeds
                .iter()
                .filter_map(|seed| seed_suffix(seed, &roots))
                .collect();
            let mut targets: Vec<_> = suffixes
                .iter()
                .flat_map(|suffix| workspaces_at(suffix, &available))
                .map(|workspace| (workspace.join("target"), workspace))
                .collect();
            for tree in &trees {
                targets.extend(listed.remove(&tree.path).unwrap_or_default());
            }
            Some(Survey { trees, targets })
        })
        .collect()
}

/// Group `starts` by the repository each is in, keeping the first start of
/// each and every seed, leaving out repositories in `seen` and starts that
/// are shielded. Stops taking new repositories at `limit` or `deadline`.
fn by_repository(
    starts: impl IntoIterator<Item = (PathBuf, Option<PathBuf>)>,
    seen: &mut HashSet<PathBuf>,
    limit: usize,
    deadline: Instant,
    homes: &[PathBuf],
) -> Vec<(PathBuf, Vec<PathBuf>)> {
    let mut order = Vec::new();
    let mut grouped: HashMap<PathBuf, (PathBuf, Vec<PathBuf>)> = HashMap::new();
    for (start, seed) in starts {
        if Instant::now() >= deadline {
            break;
        }
        if shielded(&start, homes) {
            continue;
        }
        let Some(repository) = repository_of(&start) else {
            continue;
        };
        if seen.contains(&repository) {
            continue;
        }
        if !grouped.contains_key(&repository) {
            if order.len() >= limit {
                break;
            }
            order.push(repository.clone());
            grouped.insert(repository.clone(), (start, Vec::new()));
        }
        if let Some(entry) = grouped.get_mut(&repository) {
            entry.1.extend(seed);
        }
    }
    order
        .into_iter()
        .filter_map(|repository| {
            seen.insert(repository.clone());
            grouped.remove(&repository)
        })
        .collect()
}

/// Discover targets in the Git worktrees of every repository a tracked
/// target belongs to, then of the other repositories in the folders those
/// worktrees sit in: the same workspace in each sibling worktree, and every
/// tagged `target/` Git ignores in each worktree, within `time`. A custom
/// target path outside its worktree is known only when a wrapper recorded it.
pub(crate) fn discover(store: &Store, time: Duration, now: u64) -> Result<usize> {
    let homes = homes_from(std::env::var_os(if cfg!(windows) {
        "USERPROFILE"
    } else {
        "HOME"
    }));
    discover_within(store, Instant::now() + time, &homes, turn_at(now))
}

fn discover_within(
    store: &Store,
    deadline: Instant,
    homes: &[PathBuf],
    turn: u64,
) -> Result<usize> {
    let tracked = store.tracked_target_roots(0)?;
    let mut known: HashSet<_> = tracked.iter().map(|root| root.identity).collect();
    // Every tracked target leads to its repository, a target found through
    // Git too, so a repository stays reachable after its builds' rows go.
    // Turned, so a limit leaves out different ones each pass. A seed is
    // checked against the shield before it is resolved: resolving reads it.
    let seeds = rotated(tracked.iter().collect(), turn)
        .into_iter()
        .filter(|root| !shielded(&root.workspace_root, homes))
        .filter_map(|root| root.workspace_root.canonicalize().ok())
        .map(|seed| (seed.clone(), Some(seed)));
    let mut seen = HashSet::new();
    let known_repositories = by_repository(seeds, &mut seen, DISCOVERY_STEPS, deadline, homes);
    let mut surveys = survey_all(&known_repositories, deadline, homes, turn);
    // One hop out: the repositories beside the worktrees of the known ones.
    // The listing is lazy, so a spent limit stops it at once.
    let neighbourhood: BTreeSet<_> = surveys
        .iter()
        .flat_map(|survey| &survey.trees)
        .filter_map(|tree| tree.path.parent().map(Path::to_path_buf))
        .filter(|dir| !shielded(dir, homes))
        .collect();
    let beside = rotated(neighbourhood.into_iter().collect(), turn)
        .into_iter()
        .flat_map(|dir| repositories_in(dir, deadline))
        .map(|repository| (repository, None));
    let limit = DISCOVERY_STEPS.saturating_sub(seen.len());
    let neighbours = by_repository(beside, &mut seen, limit, deadline, homes);
    surveys.extend(survey_all(&neighbours, deadline, homes, turn));

    let mut covered = HashSet::new();
    let mut found = 0;
    // What the surveys found is recorded whatever the time: a few inserts.
    for (target, workspace) in surveys.into_iter().flat_map(|survey| survey.targets) {
        if !covered.insert(workspace.clone()) {
            continue;
        }
        if let Some(id) = crate::machine::directory_identity(&target)
            && !known.insert(id)
        {
            continue;
        }
        found += usize::from(store.remember_discovered_target_root(&target, &workspace)?);
    }
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn git(repo: &Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn cargo_target(workspace: &Path) -> PathBuf {
        std::fs::create_dir_all(workspace).unwrap();
        std::fs::write(workspace.join("Cargo.toml"), "[workspace]\n").unwrap();
        let target = workspace.join("target");
        std::fs::create_dir_all(target.join("debug")).unwrap();
        std::fs::write(
            target.join("CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55\n",
        )
        .unwrap();
        target
    }

    #[test]
    fn only_target_directories_count_among_ignored_ones() {
        let root = Path::new("/w");
        let listing = b"target/\0extensions/kgw/target/\0node_modules/\0.env\0crates/removed/target/\0dist/targets/\0build/my-target/\0";
        assert_eq!(
            ignored_target_dirs(root, listing),
            vec![
                root.join("target"),
                root.join("extensions/kgw/target"),
                root.join("crates/removed/target"),
            ]
        );
    }

    #[test]
    fn discovery_never_reads_home_or_protected_folders() {
        let home = PathBuf::from("/Users/dev");
        let homes = std::slice::from_ref(&home);
        assert!(shielded(Path::new("/"), homes));
        assert!(shielded(&home, homes));
        assert!(shielded(&home.join("Documents"), homes));
        assert!(shielded(&home.join("Desktop/projects"), homes));
        assert!(!shielded(&home.join("code"), homes));
        assert!(!shielded(Path::new("/private/tmp"), homes));
        assert!(!shielded(&home.join("code"), &[]));
        // A home reached through a symlink is matched as spelled and as
        // resolved.
        let both = [home.clone(), PathBuf::from("/Volumes/Homes/dev")];
        assert!(shielded(Path::new("/Volumes/Homes/dev/Documents/x"), &both));
        assert!(shielded(Path::new("/Volumes/Homes/dev"), &both));
        assert!(!shielded(
            Path::new("/Volumes/Homes/dev/Documents/x"),
            homes
        ));
    }

    #[test]
    fn porcelain_parser_keeps_state_with_each_worktree() {
        let trees = parse_worktrees(
            b"worktree /repo\0HEAD abc\0\0worktree /other\0locked offline\0\0worktree /old\0prunable missing\0\0worktree /bare\0bare\0\0",
        );
        assert_eq!(trees.len(), 4);
        assert_eq!(trees[0].path, Path::new("/repo"));
        assert!(!trees[0].locked);
        assert!(trees[1].locked);
        assert!(trees[2].prunable);
        assert!(trees[3].bare);
    }

    #[test]
    fn a_workspace_at_a_worktree_root_maps_without_a_trailing_separator() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first");
        let second = dir.path().join("second");
        for path in [&first, &second] {
            std::fs::create_dir_all(path).unwrap();
            std::fs::write(path.join("Cargo.toml"), "").unwrap();
        }
        let tree = |path: &Path| Worktree {
            path: path.to_path_buf(),
            bare: false,
            locked: false,
            prunable: false,
        };
        let mapped = sibling_workspaces(&first, &[tree(&first), tree(&second)]);
        assert_eq!(mapped, vec![first.clone(), second.clone()]);
        assert!(
            mapped
                .iter()
                .all(|path| !path.to_string_lossy().ends_with(std::path::MAIN_SEPARATOR))
        );
    }

    #[test]
    fn maps_a_nested_workspace_only_into_available_worktrees() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first");
        let second = dir.path().join("second");
        let locked = dir.path().join("locked");
        let no_manifest = dir.path().join("no-manifest");
        for path in [&first, &second, &locked] {
            std::fs::create_dir_all(path.join("rust")).unwrap();
            std::fs::write(path.join("rust/Cargo.toml"), "").unwrap();
        }
        std::fs::create_dir_all(no_manifest.join("rust")).unwrap();
        let trees = vec![
            Worktree {
                path: first.clone(),
                bare: false,
                locked: false,
                prunable: false,
            },
            Worktree {
                path: second.clone(),
                bare: false,
                locked: false,
                prunable: false,
            },
            Worktree {
                path: locked,
                bare: false,
                locked: true,
                prunable: false,
            },
            Worktree {
                path: no_manifest,
                bare: false,
                locked: false,
                prunable: false,
            },
        ];
        assert_eq!(
            sibling_workspaces(&first.join("rust"), &trees),
            vec![first.join("rust"), second.join("rust")]
        );
    }

    fn commit_and_add_worktree(repo: &Path, worktree: &Path) {
        git(repo, &["init", "-q"]);
        std::fs::write(repo.join(".gitignore"), "target/\n").unwrap();
        git(repo, &["add", ".gitignore"]);
        git(
            repo,
            &[
                "-c",
                "user.name=Kache Test",
                "-c",
                "user.email=kache@example.invalid",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-q",
                "--allow-empty",
                "-m",
                "base",
            ],
        );
        git(
            repo,
            &[
                "worktree",
                "add",
                "-q",
                "--detach",
                worktree.to_str().unwrap(),
            ],
        );
    }

    #[test]
    fn a_repository_and_its_worktrees_share_one_key() {
        if Command::new("git")
            .arg("--version")
            .output()
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
        {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        let worktree = dir.path().join("worktree");
        std::fs::create_dir_all(repo.join("deep/inside")).unwrap();
        commit_and_add_worktree(&repo, &worktree);
        let key = repository_of(&repo).unwrap();
        assert_eq!(key, repo.join(".git").canonicalize().unwrap());
        assert_eq!(repository_of(&repo.join("deep/inside")), Some(key.clone()));
        assert_eq!(repository_of(&worktree), Some(key));
        assert_eq!(repository_of(Path::new("/")), None);
    }

    #[test]
    fn discovery_runs_hourly_on_a_quiet_machine_and_at_first_after_start() {
        let hour = FRESH_FOR.as_secs();
        let now = 10 * hour;
        assert!(
            due(true, true, 0, 0, now),
            "never ran: an install or upgrade"
        );
        assert!(due(true, true, now - hour, 0, now));
        assert!(
            !due(true, true, now - hour + 1, 0, now),
            "ran within the hour"
        );
        assert!(
            !due(true, true, 0, now - 60, now),
            "this process ran it, unrecorded"
        );
        assert!(!due(true, false, 0, 0, now), "a build is running");
        assert!(!due(false, true, 0, 0, now), "the shutdown pass");
        assert!(stale(now + 60, now), "a clock set back does not stop it");
        assert!(!stale(now, now), "it just ran");
    }

    #[test]
    fn the_daemon_step_runs_when_due_and_records_it() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(dir.path().join("cache"));
        std::fs::create_dir_all(&config.cache_dir).unwrap();
        let record = config.cache_dir.join(LAST_RUN_FILE);
        let idle = crate::maintenance::RequestClock::idle();
        let busy = crate::maintenance::RequestClock::new();

        assert!(!run(&config, Trigger::Shutdown), "never at shutdown");
        assert!(
            !run(&config, Trigger::Periodic(&busy)),
            "not during a build"
        );
        assert!(!record.exists());
        LAST_RUN.store(0, std::sync::atomic::Ordering::Relaxed);
        assert!(run(&config, Trigger::Periodic(&idle)));
        let when: u64 = std::fs::read_to_string(&record)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert!(!stale(when, unix_now_secs()));
        assert!(
            !run(&config, Trigger::Periodic(&idle)),
            "ran within the hour"
        );
    }

    #[test]
    fn turns_follow_the_hour_and_home_is_read_both_ways() {
        let hour = FRESH_FOR.as_secs();
        assert_eq!(turn_at(0), 0);
        assert_eq!(turn_at(hour - 1), 0);
        assert_eq!(turn_at(5 * hour + 7), 5);
        assert!(homes_from(None).is_empty());
        assert!(homes_from(Some("".into())).is_empty());
        let dir = tempfile::tempdir().unwrap();
        let homes = homes_from(Some(dir.path().as_os_str().to_owned()));
        assert_eq!(homes[0], dir.path());
        let resolved = dir.path().canonicalize().unwrap();
        assert!(homes.contains(&resolved));
        assert_eq!(homes.len(), if resolved == dir.path() { 1 } else { 2 });
    }

    #[test]
    fn git_deadline_requires_time_and_keeps_both_limits() {
        let started = Instant::now();
        let one_second = started + Duration::from_secs(1);
        let two_seconds = started + Duration::from_secs(2);
        assert_eq!(git_deadline(started, Duration::ZERO, two_seconds), None);
        assert_eq!(git_deadline(started, Duration::from_secs(1), started), None);
        assert_eq!(
            git_deadline(two_seconds, Duration::from_secs(1), one_second),
            None
        );
        assert_eq!(
            git_deadline(started, Duration::from_secs(1), two_seconds),
            Some(one_second)
        );
        assert_eq!(
            git_deadline(started, Duration::from_secs(2), one_second),
            Some(one_second)
        );
    }

    #[test]
    fn git_output_refuses_a_failure_and_output_at_its_cap() {
        if Command::new("git")
            .arg("--version")
            .output()
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
        {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let later = Instant::now() + GIT_TIMEOUT;
        // Not a repository: Git fails, whatever it printed.
        assert_eq!(
            git_output(dir.path(), &["ls-files"], GIT_TIMEOUT, later, 1 << 20),
            None
        );
        let version = git_output(dir.path(), &["--version"], GIT_TIMEOUT, later, 1 << 20).unwrap();
        assert!(version.starts_with(b"git version"));
        let cap = version.len() as u64;
        assert_eq!(
            git_output(dir.path(), &["--version"], GIT_TIMEOUT, later, cap),
            None
        );
        assert_eq!(
            git_output(dir.path(), &["--version"], Duration::ZERO, later, 1 << 20),
            None
        );
        assert_eq!(
            git_output(
                dir.path(),
                &["--version"],
                GIT_TIMEOUT,
                Instant::now().checked_sub(Duration::from_secs(1)).unwrap(),
                1 << 20
            ),
            None
        );
    }

    #[test]
    fn each_pass_starts_at_a_different_place() {
        assert_eq!(rotated(vec![0, 1, 2], 0), vec![0, 1, 2]);
        assert_eq!(rotated(vec![0, 1, 2], 1), vec![1, 2, 0]);
        assert_eq!(rotated(vec![0, 1, 2], 5), vec![2, 0, 1]);
        assert!(rotated(Vec::<u8>::new(), 7).is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn listing_a_worktree_never_runs_its_filesystem_monitor() {
        if Command::new("git")
            .arg("--version")
            .output()
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
        {
            return;
        }
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        commit_and_add_worktree(&repo, &dir.path().join("worktree"));
        let target = cargo_target(&repo.join("app"));
        let ran = dir.path().join("monitor-ran");
        let hook = dir.path().join("monitor");
        std::fs::write(&hook, format!("#!/bin/sh\ntouch '{}'\n", ran.display())).unwrap();
        std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o755)).unwrap();
        git(&repo, &["config", "core.fsmonitor", hook.to_str().unwrap()]);
        // Git would run it for a listing like discovery's.
        let _ = Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args([
                "ls-files",
                "--others",
                "--ignored",
                "--exclude-standard",
                "--directory",
            ])
            .output();
        if !ran.exists() {
            return;
        }
        std::fs::remove_file(&ran).unwrap();

        let found = targets_in_worktree(
            &repo.canonicalize().unwrap(),
            Instant::now() + LISTING_TIMEOUT,
        );
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(found[0].0, target.canonicalize().unwrap());
        assert!(!ran.exists(), "discovery ran the repository's monitor");
    }

    #[test]
    fn discovery_leaves_home_and_protected_folders_alone() {
        if Command::new("git")
            .arg("--version")
            .output()
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
        {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        // Git gets the home as spelled; the shield gets it both ways, as
        // `discover` passes it (`/var` is `/private/var` on macOS, and Windows
        // resolves to a `\\?\` path Git cannot create a worktree under).
        let home = dir.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        let homes = [home.clone(), home.canonicalize().unwrap()];
        // A dotfiles repository at home, with a worktree in Documents.
        commit_and_add_worktree(&home, &home.join("Documents/notes"));
        let built = cargo_target(&home.join("tools/rust"));
        cargo_target(&home.join("tools/other"));
        cargo_target(&home.join("Documents/notes/app"));
        let config = crate::test_support::test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        store
            .remember_target_root(&built, &home.join("tools/rust"))
            .unwrap();

        let pass = |deadline| discover_within(&store, deadline, &homes, 0).unwrap();
        assert_eq!(pass(Instant::now() + DISCOVERY_TIME), 0);
        assert_eq!(store.tracked_target_roots(0).unwrap().len(), 1);
    }

    #[test]
    fn a_spent_pass_starts_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        std::fs::create_dir_all(repo.join(".git")).unwrap();
        let mut seen = HashSet::new();
        let starts = || vec![(repo.clone(), None)];
        assert!(
            by_repository(
                starts(),
                &mut seen,
                0,
                Instant::now() + LISTING_TIMEOUT,
                &[]
            )
            .is_empty()
        );
        assert!(by_repository(starts(), &mut seen, 5, Instant::now(), &[]).is_empty());
        assert!(seen.is_empty());
        assert_eq!(
            by_repository(
                starts(),
                &mut seen,
                5,
                Instant::now() + LISTING_TIMEOUT,
                &[]
            )
            .len(),
            1
        );
    }

    #[test]
    fn discovery_crawls_every_worktree_of_a_known_repository() {
        if Command::new("git")
            .arg("--version")
            .output()
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
        {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        let sibling = dir.path().join("sibling");
        std::fs::create_dir_all(&repo).unwrap();
        commit_and_add_worktree(&repo, &sibling);
        let built = cargo_target(&repo.join("rust"));
        // Workspaces no build through kache used, at other paths than the
        // one it built: in the same worktree and in a sibling.
        let tool = cargo_target(&repo.join("tools/gen"));
        let extension = cargo_target(&sibling.join("extensions/kgw"));
        // A repository beside a known worktree is crawled too, one hop out.
        let neighbour = dir.path().join("neighbour");
        std::fs::create_dir_all(&neighbour).unwrap();
        commit_and_add_worktree(&neighbour, &dir.path().join("elsewhere/neighbour-wt"));
        let beside = cargo_target(&neighbour.join("app"));
        // One further away is not.
        let unrelated = dir.path().join("elsewhere/deeper/unrelated");
        std::fs::create_dir_all(&unrelated).unwrap();
        commit_and_add_worktree(
            &unrelated,
            &dir.path().join("elsewhere/deeper/unrelated-wt"),
        );
        cargo_target(&unrelated.join("rust"));

        let config = crate::test_support::test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        store
            .remember_target_root(&built, &repo.join("rust"))
            .unwrap();
        let pass = || discover_within(&store, Instant::now() + DISCOVERY_TIME, &[], 0);
        assert_eq!(pass().unwrap(), 3);
        assert_eq!(pass().unwrap(), 0, "already known");
        let mut discovered: Vec<_> = store
            .tracked_target_roots(0)
            .unwrap()
            .into_iter()
            .filter(|root| root.discovered)
            .map(|root| root.path)
            .collect();
        discovered.sort();
        let mut expected = vec![
            tool.canonicalize().unwrap(),
            extension.canonicalize().unwrap(),
            beside.canonicalize().unwrap(),
        ];
        expected.sort();
        assert_eq!(discovered, expected);
    }

    #[test]
    fn daemon_finds_a_sibling_target_without_claiming_a_build_used_it() {
        if Command::new("git")
            .arg("--version")
            .output()
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
        {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        let sibling = dir.path().join("sibling");
        let cache = dir.path().join("cache");
        std::fs::create_dir_all(&repo).unwrap();
        git(&repo, &["init", "-q"]);
        git(
            &repo,
            &[
                "-c",
                "user.name=Kache Test",
                "-c",
                "user.email=kache@example.invalid",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-q",
                "--allow-empty",
                "-m",
                "base",
            ],
        );
        git(
            &repo,
            &[
                "worktree",
                "add",
                "-q",
                "--detach",
                sibling.to_str().unwrap(),
            ],
        );
        let original = cargo_target(&repo.join("rust"));
        let found = cargo_target(&sibling.join("rust"));
        let config = crate::test_support::test_config(cache);
        let store = Store::open(&config).unwrap();
        store
            .remember_target_root(&original, &repo.join("rust"))
            .unwrap();

        assert_eq!(discover(&store, DISCOVERY_TIME, 0).unwrap(), 1);
        assert_eq!(discover(&store, DISCOVERY_TIME, 0).unwrap(), 0);
        let roots = store.tracked_target_roots(0).unwrap();
        assert_eq!(roots.len(), 2);
        let sibling_row = roots
            .iter()
            .find(|root| root.path == found.canonicalize().unwrap())
            .unwrap();
        assert!(sibling_row.discovered);
        assert_eq!(
            sibling_row.workspace_root,
            sibling.join("rust").canonicalize().unwrap()
        );
        assert_eq!(sibling_row.rustc, None);

        let old_identity = sibling_row.identity;
        std::fs::rename(&found, sibling.join("old-target")).unwrap();
        cargo_target(&sibling.join("rust"));
        assert_eq!(discover(&store, DISCOVERY_TIME, 0).unwrap(), 1);
        let replaced = store
            .tracked_target_roots(0)
            .unwrap()
            .into_iter()
            .find(|root| root.path == found.canonicalize().unwrap())
            .unwrap();
        assert!(replaced.discovered);
        assert_ne!(replaced.identity, old_identity);

        store
            .remember_target_root(&found, &sibling.join("rust"))
            .unwrap();
        let row = store
            .tracked_target_roots(0)
            .unwrap()
            .into_iter()
            .find(|root| root.path == found)
            .unwrap();
        assert_eq!(store.tracked_target_roots(0).unwrap().len(), 2);
        assert!(
            !row.discovered,
            "a real build replaces discovery provenance"
        );

        store.forget_target_root(&found).unwrap();
        std::fs::remove_dir_all(&found).unwrap();
        std::fs::create_dir_all(found.join("debug")).unwrap();
        assert_eq!(discover(&store, DISCOVERY_TIME, 0).unwrap(), 0);
        assert_eq!(store.tracked_target_roots(0).unwrap().len(), 1);
    }
}
