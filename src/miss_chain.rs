//! Walking an `extern:` cascade back to the crate that actually changed
//! (kunobi-ninja/kache#609).
//!
//! When one crate's artifact content moves, `extern:<name>=<hash>` re-keys
//! every crate above it. `why-miss` then reports the same thing for all of
//! them: "same config -- likely source code, dependency, or rustc version
//! change". True for the crate that changed, useless for the forty that are
//! only downstream of it, and the reporter in #580 had to work down the chain
//! by hand to find that `aws_lc_sys` was the root.
//!
//! The per-dependency digests recorded on each event (`key_externs`) make the
//! walk mechanical: diff a crate's compile against the previous recorded state
//! of the same crate to see WHICH dependencies moved, then repeat for each of
//! them until reaching crates whose divergence is not itself dependency-driven.
//!
//! # Why every branch is walked
//!
//! In a real cascade the crate being asked about usually has SEVERAL changed
//! dependencies — in #580, `rig_core` sits above `aws_lc_rs`, `aws_sdk_*`,
//! `async_nats` and more, all re-keyed by the same leaf. Following one of them
//! and calling it "the root" would be an arbitrary choice dressed up as a
//! diagnosis. Bailing out whenever more than one moved would instead give up
//! exactly in the case this exists to explain. So every branch is walked, and
//! the roots are ranked by how many branches converge on them: the crate that
//! shows up under most of the changed dependencies is the one to look at.
//!
//! # Event selection
//!
//! The walk is driven by POSITION in the oldest-first event slice, never by
//! timestamp. Each hop searches strictly before the parent's event, so the
//! selected events are causally ordered and the walk cannot pick a later,
//! unrelated compile of a dependency. Timestamps are unfit for this: two events
//! can share one, and a dependency can be recompiled between the moment a
//! parent hashes its artifact and the moment the parent's own event is logged.
//!
//! # Unit identity
//!
//! `crate_name` is not a compilation-unit identity: two versions of a package,
//! a host and a target build of the same crate, and two feature sets of it all
//! collapse onto one name, and Cargo's `package = "..."` renaming makes the
//! consumer's name for a dependency differ from the producer's own
//! (kunobi-ninja/kache#627). Pairing by name therefore risks comparing
//! unrelated units, or dead-ending on an alias no event carries.
//!
//! So the walk pairs by unit id wherever the events have one: cargo's
//! `-C extra-filename` hash, recorded on the producing event (`unit_id`) and
//! recovered by the consumer from its `--extern` artifact filename
//! (`extern_units`). That is the disambiguator cargo itself uses to keep those
//! units' artifacts apart in one `deps/` directory, and it is visible from both
//! sides, so the join holds regardless of the name the consumer used.
//!
//! Name matching stays as the fallback for events carrying no unit id — a
//! non-cargo rustc invocation, a sysroot crate, a pre-#627 wrapper. There the
//! old ambiguity remains, and the walk still prefers an unresolved endpoint
//! over a confident wrong one wherever the history does not support a
//! conclusion.
//!
//! Everything here is a pure function over already-read events: no store, no
//! filesystem, no clock. `cli` renders the result.

use crate::events::{BuildEvent, EventResult};
use std::collections::{BTreeMap, HashSet};

/// Hops to follow down one branch before giving up.
const MAX_DEPTH: usize = 12;

/// Total crates examined across all branches, so a wide graph cannot turn a
/// diagnostic into a long walk.
const MAX_NODES: usize = 64;

/// A dependency whose artifact digest differs between a crate's compile and the
/// previous recorded state of that crate.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangedDep {
    /// The name the CONSUMER used, which Cargo's `package = "..."` renaming can
    /// make different from the producing crate's own name.
    pub name: String,
    /// Digest in the baseline event. `None` when the dependency is new.
    pub from: Option<String>,
    /// Digest at the compile being explained. `None` when it went away.
    pub to: Option<String>,
    /// The producing unit's id, when the consumer's event recorded one
    /// (kunobi-ninja/kache#627). This, not `name`, is what selects the
    /// dependency's own events.
    pub unit: Option<String>,
}

/// One step down a branch: `crate_name` diverged because `via` moved.
#[derive(Debug, Clone, PartialEq)]
pub struct Hop {
    pub crate_name: String,
    /// Unit id of the crate at this hop, when its event recorded one. Carried
    /// so cycle detection can compare units rather than names; renderers use
    /// `crate_name`.
    pub unit: Option<String>,
    pub via: ChangedDep,
}

/// Identity a branch is tracked by: the unit id when known, else the name.
///
/// Prefixed so a unit id can never collide with a crate that happens to be
/// named the same as some hash.
fn node_key(name: &str, unit: Option<&str>) -> String {
    match unit {
        Some(unit) => format!("unit:{unit}"),
        None => format!("name:{name}"),
    }
}

/// Why the crate at the end of a branch diverged.
#[derive(Debug, Clone, PartialEq)]
pub enum RootKind {
    /// Key input groups that moved, `externs` excluded. The only variant that
    /// asserts a cause; everything else is an explicit dead end.
    Groups(Vec<String>),
    /// Dependencies compared clean and no traced group moved: the difference
    /// sits in a post-hoc fold (key salt, extra inputs).
    NothingRecorded,
    /// The dependency moved but has no compile recorded in this event window.
    NoMissRecorded,
    /// Nothing earlier to diff this crate against.
    NoBaseline,
    /// Reached, but its own dependency history is not comparable, so it cannot
    /// be shown to be the end of the cascade. Distinct from "dependencies were
    /// compared and were stable" — conflating the two invents a root.
    NoDiffableHistory,
    /// The branch was still descending when a limit was hit.
    LimitReached,
}

impl RootKind {
    /// Whether this endpoint actually explains anything. Renderers must not
    /// present an unresolved endpoint as the cause.
    pub fn is_resolved(&self) -> bool {
        matches!(self, RootKind::Groups(_) | RootKind::NothingRecorded)
    }
}

/// Passthrough (uncached) compiles attributed to a root crate, grouped by
/// reason. This is the actionable half: a passthrough there means the crate's
/// artifact varies per checkout, and the reason names the flag to model.
#[derive(Debug, Clone, PartialEq)]
pub struct PassthroughGroup {
    pub reason: String,
    pub count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Root {
    pub crate_name: String,
    /// Unit id of the event this endpoint resolved to, when that event recorded
    /// one.
    ///
    /// Taken from the SELECTED producer rather than from what the consumer
    /// asked for, so `crate_name` and `unit` always describe the same event and
    /// convergence counting keys on the node actually analyzed. Renderers show
    /// `crate_name`.
    pub unit: Option<String>,
    pub kind: RootKind,
    pub passthroughs: Vec<PassthroughGroup>,
    /// Distinct branches from the starting crate that ended here. A root many
    /// branches converge on is the likely cause of the whole cascade.
    pub branches: usize,
    /// Shortest path of hops that reached this root.
    pub path: Vec<Hop>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Chain {
    /// Endpoints, most-converged first, then shallowest.
    pub roots: Vec<Root>,
    /// Dependencies that moved directly under the crate being explained.
    pub direct: Vec<ChangedDep>,
    /// Set when exploration stopped early rather than exhausting the graph.
    pub truncated: Option<&'static str>,
}

impl Chain {
    /// Whether any endpoint actually explains the cascade.
    pub fn has_resolved_root(&self) -> bool {
        self.roots.iter().any(|r| r.kind.is_resolved())
    }
}

/// Walk the cascade below the compile at `miss_index`.
///
/// `miss_index` indexes `events` — an oldest-first slice. `None` means there is
/// nothing to report: no per-dependency digests were recorded (the wrapper
/// writes them only under `[cache] explain_miss`, or the events predate #609),
/// or this crate's dependencies did not move, in which case the existing
/// diagnosis already says everything known.
pub fn analyze(events: &[BuildEvent], miss_index: usize) -> Option<Chain> {
    let miss = events.get(miss_index)?;
    if !miss.key_externs_recorded || miss.root.is_empty() {
        return None;
    }
    let direct = changed_deps_at(events, miss_index)?;
    if direct.is_empty() {
        return None;
    }

    // Breadth-first over (crate, event position), so the first path reaching a
    // crate is the shortest one. `seen` and `reached` are keyed by unit where
    // the events carry one, so two same-named units count as two nodes rather
    // than silently folding into one (#627); revisiting a node on another
    // branch adds no information beyond the convergence count, tracked
    // separately.
    let mut queue: Vec<(usize, Vec<Hop>)> = vec![(miss_index, Vec::new())];
    let mut seen: HashSet<String> = HashSet::from([node_key(&miss.crate_name, unit_of(miss))]);
    // Branches that reached each node, counted independently of `roots`: a node
    // can be reached again while it is still queued, long before it becomes an
    // endpoint.
    let mut reached: BTreeMap<String, usize> = BTreeMap::new();
    let mut roots: Vec<Root> = Vec::new();
    let mut nodes = 0usize;
    let mut truncated = None;

    while let Some((index, path)) = queue.pop() {
        let changed = match changed_deps_at(events, index) {
            Some(changed) => changed,
            // Guarded by the caller for the first node; deeper nodes are
            // checked before being enqueued.
            None => continue,
        };

        for dep in changed {
            if nodes >= MAX_NODES {
                truncated = Some("too many changed dependencies to follow");
                break;
            }
            nodes += 1;

            let mut next_path = path.clone();
            next_path.push(Hop {
                crate_name: events[index].crate_name.clone(),
                unit: unit_of(&events[index]).map(str::to_string),
                via: dep.clone(),
            });

            // A dependency that points back at a crate already on this path is
            // a loop, whether or not it has an event of its own to resolve to.
            // Checking the REQUESTED identity here catches the case where it
            // has none — the walk would otherwise report the crate being
            // explained as an unresolved endpoint of its own cascade.
            if loops_back(&path, miss, &node_key(&dep.name, dep.unit.as_deref())) {
                truncated = Some("cycle in recorded dependency digests");
                continue;
            }

            // Resolve the producer, then track the branch by the identity of the
            // event actually selected — not by the identity the consumer asked
            // for. The two differ whenever a dep carrying a unit id resolves to
            // a legacy event that has none, and every downstream structure
            // (`reached`, `seen`, the cycle check, each root) has to agree on
            // one key, or the walk reports a converged root as reached by a
            // single branch, explores one event twice, and misses loops. Hops
            // carry their own event's identity, so comparing against the
            // selected producer's keeps that check apples-to-apples.
            let Some(dep_index) = producer_index(events, &dep, &events[index].root, index) else {
                // Nothing was selected, so the requested identity is all there
                // is to count this dead end under.
                *reached
                    .entry(node_key(&dep.name, dep.unit.as_deref()))
                    .or_default() += 1;
                roots.push(unresolved(
                    dep.name,
                    dep.unit,
                    RootKind::NoMissRecorded,
                    next_path,
                ));
                continue;
            };
            let producer = &events[dep_index];
            // Report the producing crate's OWN name from here down. Under
            // Cargo's `package = "..."` renaming the consumer's alias names no
            // crate the user can go look at (#627).
            let dep_name = producer.crate_name.clone();
            let dep_unit = unit_of(producer).map(str::to_string);
            let dep_key = node_key(&dep_name, dep_unit.as_deref());
            *reached.entry(dep_key.clone()).or_default() += 1;

            // And again on the resolved identity: name matching in a mixed
            // window can land on an event already visited under a unit id.
            if loops_back(&path, miss, &dep_key) {
                truncated = Some("cycle in recorded dependency digests");
                continue;
            }

            if !seen.insert(dep_key) {
                // Reached by another branch already; the convergence is
                // recorded above and there is nothing new to explore.
                continue;
            }

            match changed_deps_at(events, dep_index) {
                // Its own dependencies moved: keep descending, unless this
                // branch has run out of depth.
                Some(next) if !next.is_empty() => {
                    if next_path.len() >= MAX_DEPTH {
                        truncated = Some("chain longer than the walk limit");
                        roots.push(unresolved(
                            dep_name,
                            dep_unit,
                            RootKind::LimitReached,
                            next_path,
                        ));
                        continue;
                    }
                    queue.push((dep_index, next_path));
                }
                // Compared cleanly and stable: this is a genuine endpoint.
                // `classify_at` derives the same identity from the same event.
                Some(_) => roots.push(classify_at(events, dep_index, next_path)),
                // Not comparable. NOT the same as stable — saying so would
                // invent a root out of missing data.
                None => {
                    let mut root =
                        unresolved(dep_name, dep_unit, RootKind::NoDiffableHistory, next_path);
                    root.passthroughs = passthroughs_for(events, &root.crate_name, dep_index);
                    roots.push(root);
                }
            }
        }
        if truncated == Some("too many changed dependencies to follow") {
            break;
        }
    }

    for root in &mut roots {
        root.branches = reached
            .get(&node_key(&root.crate_name, root.unit.as_deref()))
            .copied()
            .unwrap_or(1);
    }
    // Most-converged first, then shallowest, then by name so output is stable.
    roots.sort_by(|a, b| {
        b.branches
            .cmp(&a.branches)
            .then_with(|| a.path.len().cmp(&b.path.len()))
            .then_with(|| a.crate_name.cmp(&b.crate_name))
    });

    Some(Chain {
        roots,
        direct,
        truncated,
    })
}

/// Whether `key` names a node already on this path, or the crate being
/// explained.
///
/// Cargo forbids real dependency cycles, so a loop here means the recorded
/// digests are inconsistent: with unit ids present, two compiles disagreeing
/// about what produced what; without them, the older failure of two units
/// sharing a crate name being paired as one. Either way the branch stops.
fn loops_back(path: &[Hop], miss: &BuildEvent, key: &str) -> bool {
    path.iter()
        .any(|hop| node_key(&hop.crate_name, hop.unit.as_deref()) == key)
        || key == node_key(&miss.crate_name, unit_of(miss))
}

fn unresolved(crate_name: String, unit: Option<String>, kind: RootKind, path: Vec<Hop>) -> Root {
    Root {
        crate_name,
        unit,
        kind,
        passthroughs: Vec::new(),
        branches: 1,
        path,
    }
}

/// Dependencies whose digests differ between the compile at `index` and the
/// previous recorded state of the same crate in the same build tree.
///
/// `None` means the history is not diffable (no baseline, or either side
/// recorded no digests). That is deliberately distinct from `Some(vec![])`,
/// which means the comparison succeeded and the dependencies are identical —
/// only the latter supports concluding that a crate ends the cascade.
fn changed_deps_at(events: &[BuildEvent], index: usize) -> Option<Vec<ChangedDep>> {
    let compiled = events.get(index)?;
    if !compiled.key_externs_recorded {
        return None;
    }
    let baseline = last_baseline_index(events, compiled, &compiled.root, index)?;
    Some(diff_externs(
        &events[baseline].key_externs,
        &compiled.key_externs,
        &compiled.extern_units,
    ))
}

/// Symmetric diff of two dependency-digest maps, sorted by name.
///
/// `units` comes from the LATER of the two events: it describes the dependency
/// set as of the compile being explained, which is the one the walk descends
/// into. A dependency that only exists in the baseline has no unit recorded and
/// falls back to name matching, which is all that state supports anyway.
fn diff_externs(
    before: &BTreeMap<String, String>,
    after: &BTreeMap<String, String>,
    units: &BTreeMap<String, String>,
) -> Vec<ChangedDep> {
    let mut out = Vec::new();
    for (name, to) in after {
        match before.get(name) {
            Some(from) if from == to => {}
            from => out.push(ChangedDep {
                name: name.clone(),
                from: from.cloned(),
                to: Some(to.clone()),
                unit: units.get(name).cloned(),
            }),
        }
    }
    // Dependencies that disappeared also moved the key.
    for (name, from) in before {
        if !after.contains_key(name) {
            out.push(ChangedDep {
                name: name.clone(),
                from: Some(from.clone()),
                to: None,
                unit: units.get(name).cloned(),
            });
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// Explain a crate's own divergence, dependencies aside.
fn classify_at(events: &[BuildEvent], index: usize, path: Vec<Hop>) -> Root {
    let compiled = &events[index];
    let passthroughs = passthroughs_for(events, &compiled.crate_name, index);
    let Some(baseline) = last_baseline_index(events, compiled, &compiled.root, index) else {
        return Root {
            crate_name: compiled.crate_name.clone(),
            unit: unit_of(compiled).map(str::to_string),
            kind: RootKind::NoBaseline,
            passthroughs,
            branches: 1,
            path,
        };
    };

    // Diff against the SAME baseline the dependency comparison used. The
    // wrapper's own `key_diff` is computed against that crate's last hit, which
    // can be an older event than this baseline, so preferring it here would mix
    // two different comparisons.
    let mut groups = changed_groups(&events[baseline].key_fields, &compiled.key_fields);
    if groups.is_empty() && !compiled.key_diff.is_empty() {
        // No group digests recorded to diff (pre-#131 events): fall back to
        // whatever the wrapper concluded at the time.
        groups = compiled.key_diff.clone();
    }
    groups.retain(|g| g != "externs");
    groups.sort();
    groups.dedup();

    let kind = if groups.is_empty() {
        RootKind::NothingRecorded
    } else {
        RootKind::Groups(groups)
    };
    Root {
        crate_name: compiled.crate_name.clone(),
        unit: unit_of(compiled).map(str::to_string),
        kind,
        passthroughs,
        branches: 1,
        path,
    }
}

fn changed_groups(
    before: &BTreeMap<String, String>,
    after: &BTreeMap<String, String>,
) -> Vec<String> {
    let mut out: Vec<String> = after
        .iter()
        .filter(|(group, digest)| before.get(*group) != Some(digest))
        .map(|(group, _)| group.clone())
        .collect();
    out.extend(before.keys().filter(|g| !after.contains_key(*g)).cloned());
    out
}

/// Passthrough compiles plausibly belonging to `crate_name`, grouped by reason.
///
/// Attribution is a heuristic and deliberately a loose one. A cc invocation
/// driven by a build script logs the source file as its `crate_name`
/// (`bcm.c`), so it cannot be joined to the Rust crate by name; what it does
/// carry is a `root` derived from the build-script cwd, which for a registry
/// dependency contains the package directory (`.../aws-lc-sys-0.43.0`).
/// Matching on that names the right crate for the case this exists to
/// diagnose, and when it is wrong it over-reports rather than pointing
/// somewhere else. `cli` labels the line as inferred.
///
/// Bounded to events before the compile being explained, so an unrelated later
/// build cannot be folded in.
fn passthroughs_for(
    events: &[BuildEvent],
    crate_name: &str,
    before: usize,
) -> Vec<PassthroughGroup> {
    let needle = crate_name.replace('_', "-");
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for event in &events[..before.min(events.len())] {
        if event.result != EventResult::Passthrough || event.root.is_empty() {
            continue;
        }
        if !package_dir_matches(&event.root, &needle) {
            continue;
        }
        let reason = if event.passthrough_reason.is_empty() {
            "(no reason recorded)".to_string()
        } else {
            event.passthrough_reason.clone()
        };
        *counts.entry(reason).or_default() += 1;
    }
    let mut out: Vec<PassthroughGroup> = counts
        .into_iter()
        .map(|(reason, count)| PassthroughGroup { reason, count })
        .collect();
    out.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.reason.cmp(&b.reason)));
    out.truncate(4);
    out
}

/// Whether any path component of `root` is the package directory for `needle` —
/// exactly `needle`, or `needle-<semver>`.
///
/// Split on both separators rather than using `Path::components`, so a Windows
/// path analyzed on Unix (or the reverse) still resolves. Component-wise and
/// version-shaped on purpose: a plain `contains` would let `aws-lc` match
/// `aws-lc-sys-0.43.0`, and a bare "starts with a digit" test would let
/// `foo-2-helper-0.1.0` match `foo`.
fn package_dir_matches(root: &str, needle: &str) -> bool {
    root.split(['/', '\\']).any(|component| {
        component == needle
            || component
                .strip_prefix(needle)
                .and_then(|rest| rest.strip_prefix('-'))
                .is_some_and(looks_like_semver)
    })
}

/// `MAJOR.MINOR.PATCH`, with an optional `-pre` / `+build` tail — the shape
/// Cargo puts in a registry package directory.
fn looks_like_semver(value: &str) -> bool {
    let core = value.split(['-', '+']).next().unwrap_or_default();
    let parts: Vec<&str> = core.split('.').collect();
    parts.len() == 3
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
}

/// This compile's unit id, or `None` when it recorded none.
fn unit_of(event: &BuildEvent) -> Option<&str> {
    (!event.unit_id.is_empty()).then_some(event.unit_id.as_str())
}

/// Whether `event` is the unit `dep` names.
///
/// Exact when both sides carry a unit id. When the dependency has one and the
/// candidate does not, the candidate predates #627, so fall back to the name —
/// otherwise the walk would go blind across the upgrade. An event with a
/// DIFFERENT unit id is never accepted on the name: that is precisely the
/// wrong-pairing this exists to stop.
fn is_producer(event: &BuildEvent, dep: &ChangedDep) -> bool {
    match (dep.unit.as_deref(), unit_of(event)) {
        (Some(want), Some(have)) => want == have,
        (Some(_), None) => event.crate_name == dep.name,
        (None, _) => event.crate_name == dep.name,
    }
}

/// Last compile (`Miss`/`Dup`) of the unit `dep` names, strictly before
/// `before`.
///
/// Selection prefers an exact unit match anywhere in the window over a
/// name-only match, so one legacy event cannot shadow the right unit's own
/// history just by being closer.
fn producer_index(
    events: &[BuildEvent],
    dep: &ChangedDep,
    root: &str,
    before: usize,
) -> Option<usize> {
    let window = &events[..before.min(events.len())];
    let compiled = |e: &BuildEvent| matches!(e.result, EventResult::Miss | EventResult::Dup);

    if let Some(want) = dep.unit.as_deref() {
        let exact = window
            .iter()
            .rposition(|e| unit_of(e) == Some(want) && same_root(e, root) && compiled(e));
        if exact.is_some() {
            return exact;
        }
    }
    window
        .iter()
        .rposition(|e| is_producer(e, dep) && same_root(e, root) && compiled(e))
}

/// The previous event that recorded dependency digests for this crate.
///
/// Any keyed outcome counts, not just a hit: after two consecutive misses,
/// diffing the second against the last HIT reports everything that changed
/// across both, which over-reports the dependencies responsible for the second
/// one. A dependency can equally reach its new artifact through a remote or
/// prefetch hit, so those count too.
/// Matched by unit id when the compile has one, so the previous state of THIS
/// unit is compared rather than that of a same-named sibling — a duplicate
/// package version, or the host build of a crate also built for the target
/// (#627). Events with no unit id (pre-#627, non-cargo) still match by name, so
/// a mixed window keeps working.
fn last_baseline_index(
    events: &[BuildEvent],
    compiled: &BuildEvent,
    root: &str,
    before: usize,
) -> Option<usize> {
    let keyed = |e: &BuildEvent| {
        same_root(e, root)
            && e.key_externs_recorded
            && matches!(
                e.result,
                EventResult::LocalHit
                    | EventResult::PrefetchHit
                    | EventResult::RemoteHit
                    | EventResult::Miss
                    | EventResult::Dup
            )
    };
    let window = &events[..before.min(events.len())];

    if let Some(unit) = unit_of(compiled) {
        if let Some(exact) = window
            .iter()
            .rposition(|e| unit_of(e) == Some(unit) && keyed(e))
        {
            return Some(exact);
        }
        // No event for this unit carries an id: only legacy events are left to
        // compare against, and those can only be matched by name.
        return window.iter().rposition(|e| {
            e.unit_id.is_empty() && e.crate_name == compiled.crate_name && keyed(e)
        });
    }
    window
        .iter()
        .rposition(|e| e.crate_name == compiled.crate_name && keyed(e))
}

/// Exact build-tree match.
///
/// A missing root is NOT a wildcard. Treating it as one lets a legacy or
/// foreign event stand in as the baseline for a crate of the same name in a
/// different workspace, which produces a confident and wrong cascade.
fn same_root(event: &BuildEvent, root: &str) -> bool {
    !root.is_empty() && event.root == root
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc};

    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + secs, 0).unwrap()
    }

    fn event(
        crate_name: &str,
        result: EventResult,
        at: i64,
        externs: &[(&str, &str)],
    ) -> BuildEvent {
        let mut e = BuildEvent::new_for_test(crate_name, result);
        e.ts = ts(at);
        e.root = "/w".to_string();
        e.key_externs = externs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        e.key_externs_recorded = true;
        e
    }

    /// An event from a build where the digests were not recorded at all —
    /// `explain_miss` off, or a pre-#609 wrapper.
    fn unrecorded(crate_name: &str, result: EventResult, at: i64) -> BuildEvent {
        let mut e = event(crate_name, result, at, &[]);
        e.key_externs_recorded = false;
        e
    }

    fn with_fields(mut e: BuildEvent, fields: &[(&str, &str)]) -> BuildEvent {
        e.key_fields = fields
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        e
    }

    fn analyze_last(events: &[BuildEvent]) -> Option<Chain> {
        analyze(events, events.len() - 1)
    }

    /// This event's own compilation unit (cargo's `-C extra-filename`).
    fn with_unit(mut e: BuildEvent, unit: &str) -> BuildEvent {
        e.unit_id = unit.to_string();
        e
    }

    /// The producing unit behind each of this event's externs, as the consumer
    /// recovered it from the `--extern` artifact path.
    fn with_extern_units(mut e: BuildEvent, units: &[(&str, &str)]) -> BuildEvent {
        e.extern_units = units
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        e
    }

    /// Cargo's `package = "..."` renaming: the consumer's key records the alias
    /// (`foo_old`), the producer's events record its real name (`foo`). Matching
    /// by name dead-ends on the alias; matching by unit reaches the producer and
    /// reports the name the user can actually go look at (#627).
    #[test]
    fn follows_a_renamed_dependency_to_the_crate_that_produced_it() {
        let events = vec![
            with_extern_units(
                event("app", EventResult::LocalHit, 0, &[("foo_old", "aaaa")]),
                &[("foo_old", "ufoo")],
            ),
            with_unit(
                with_fields(
                    event("foo", EventResult::LocalHit, 1, &[]),
                    &[("sources", "1111")],
                ),
                "ufoo",
            ),
            with_unit(
                with_fields(
                    event("foo", EventResult::Miss, 10, &[]),
                    &[("sources", "2222")],
                ),
                "ufoo",
            ),
            with_extern_units(
                event("app", EventResult::Miss, 11, &[("foo_old", "bbbb")]),
                &[("foo_old", "ufoo")],
            ),
        ];

        let chain = analyze_last(&events).expect("cascade should be reported");
        assert_eq!(chain.roots.len(), 1);
        let root = &chain.roots[0];
        assert_eq!(
            root.crate_name, "foo",
            "the producing crate's own name, not the consumer's alias"
        );
        assert_eq!(root.kind, RootKind::Groups(vec!["sources".to_string()]));
        // The alias still names the edge, since that is what the consumer's
        // manifest says.
        assert_eq!(chain.direct[0].name, "foo_old");

        // Strip the unit ids and the same events reproduce the pre-#627
        // behaviour: no crate is named `foo_old`, so the walk dead-ends on the
        // alias. This is what the fix buys.
        let by_name: Vec<BuildEvent> = events
            .iter()
            .cloned()
            .map(|mut e| {
                e.unit_id.clear();
                e.extern_units.clear();
                e
            })
            .collect();
        let chain = analyze_last(&by_name).expect("cascade should still be reported");
        assert_eq!(chain.roots[0].crate_name, "foo_old");
        assert_eq!(chain.roots[0].kind, RootKind::NoMissRecorded);
    }

    /// Two versions of one package in the same graph. The consumer depends on
    /// the second, so the walk must diff THAT unit's history — matching by name
    /// would pick whichever `foo` event came last and report a change that
    /// belongs to the other version (#627).
    #[test]
    fn picks_the_right_unit_when_two_versions_share_a_crate_name() {
        let events = vec![
            with_extern_units(
                event("app", EventResult::LocalHit, 0, &[("foo", "aaaa")]),
                &[("foo", "foo_v2")],
            ),
            // v2: stable dependencies, its own sources moved.
            with_unit(
                with_fields(
                    event("foo", EventResult::LocalHit, 1, &[("libc", "cccc")]),
                    &[("sources", "1111")],
                ),
                "foo_v2",
            ),
            with_unit(
                with_fields(
                    event("foo", EventResult::Miss, 10, &[("libc", "cccc")]),
                    &[("sources", "2222")],
                ),
                "foo_v2",
            ),
            // v1 compiles later and is the nearest `foo` event by name, with a
            // completely different dependency set. A name-keyed walk would diff
            // against this one.
            with_unit(
                with_fields(
                    event("foo", EventResult::Miss, 11, &[("bitflags", "zzzz")]),
                    &[("args", "9999")],
                ),
                "foo_v1",
            ),
            with_extern_units(
                event("app", EventResult::Miss, 12, &[("foo", "bbbb")]),
                &[("foo", "foo_v2")],
            ),
        ];

        let chain = analyze_last(&events).expect("cascade should be reported");
        assert_eq!(chain.roots.len(), 1);
        let root = &chain.roots[0];
        assert_eq!(root.crate_name, "foo");
        assert_eq!(root.unit.as_deref(), Some("foo_v2"));
        assert_eq!(
            root.kind,
            RootKind::Groups(vec!["sources".to_string()]),
            "v2's own inputs moved; v1's `args` change belongs to a different unit"
        );

        // Without unit ids the walk picks v1 — the nearest `foo` by name — then
        // diffs it against v2's event, so v1's unrelated dependency set reads as
        // "everything changed" and the walk descends into crates the miss has
        // nothing to do with. The pre-#627 failure, pinned.
        let by_name: Vec<BuildEvent> = events
            .iter()
            .cloned()
            .map(|mut e| {
                e.unit_id.clear();
                e.extern_units.clear();
                e
            })
            .collect();
        let chain = analyze_last(&by_name).expect("cascade should still be reported");
        let mut names: Vec<&str> = chain.roots.iter().map(|r| r.crate_name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(
            names,
            vec!["bitflags", "libc"],
            "name matching walks into v1's dependencies: {:?}",
            chain.roots
        );
        assert!(
            !chain.has_resolved_root(),
            "and explains nothing, having compared two different units"
        );
    }

    /// A host build and a target build of one crate, interleaved. Each unit's
    /// baseline must be its own previous compile: pairing across the two makes
    /// stable dependencies look like they were added and removed.
    #[test]
    fn baselines_a_unit_against_itself_not_a_same_named_sibling() {
        let events = vec![
            with_unit(
                event("shared", EventResult::LocalHit, 0, &[("libc", "aaaa")]),
                "host",
            ),
            with_unit(
                event("shared", EventResult::LocalHit, 1, &[("libc", "aaaa")]),
                "target",
            ),
            // The host unit recompiles with everything unchanged. Diffed against
            // the target unit's event it would look identical here, so give the
            // target unit a different dependency set to make a wrong pairing
            // visible.
            with_unit(
                event("shared", EventResult::Miss, 2, &[("libc", "aaaa")]),
                "host",
            ),
        ];

        // Same unit, same digests: comparison succeeded and nothing moved.
        assert_eq!(changed_deps_at(&events, 2), Some(vec![]));

        let mut cross = events.clone();
        cross[1].key_externs = [("winapi".to_string(), "bbbb".to_string())]
            .into_iter()
            .collect();
        assert_eq!(
            changed_deps_at(&cross, 2),
            Some(vec![]),
            "the target unit's different dependency set must not leak into the host unit's diff"
        );
    }

    /// Convergence counting has to use the identity the BRANCH was tracked by.
    /// Here two branches reach one dependency that the consumers identify by
    /// unit, but whose own events are legacy and carry no id: keying the root
    /// off the producing event instead would report 1 branch for a root that 2
    /// converge on, which is the ranking signal `why-miss` sorts by (#627).
    #[test]
    fn counts_converging_branches_when_the_producer_is_a_legacy_event() {
        let via_foo = |name: &str, unit: &str, at: i64, digest: &str, result| {
            with_extern_units(
                with_unit(event(name, result, at, &[("foo", digest)]), unit),
                &[("foo", "ufoo")],
            )
        };
        let events = vec![
            with_extern_units(
                event(
                    "app",
                    EventResult::LocalHit,
                    0,
                    &[("b1", "aa"), ("b2", "aa")],
                ),
                &[("b1", "ub1"), ("b2", "ub2")],
            ),
            via_foo("b1", "ub1", 1, "x1", EventResult::LocalHit),
            via_foo("b2", "ub2", 2, "x1", EventResult::LocalHit),
            // The shared dependency predates #627: no unit id of its own.
            with_fields(
                event("foo", EventResult::LocalHit, 3, &[]),
                &[("sources", "1111")],
            ),
            with_fields(
                event("foo", EventResult::Miss, 4, &[]),
                &[("sources", "2222")],
            ),
            via_foo("b1", "ub1", 5, "x2", EventResult::Miss),
            via_foo("b2", "ub2", 6, "x2", EventResult::Miss),
            with_extern_units(
                event("app", EventResult::Miss, 7, &[("b1", "bb"), ("b2", "bb")]),
                &[("b1", "ub1"), ("b2", "ub2")],
            ),
        ];

        let chain = analyze_last(&events).expect("cascade should be reported");
        let foo = chain
            .roots
            .iter()
            .find(|r| r.crate_name == "foo")
            .expect("the shared dependency should be a root");
        assert_eq!(foo.kind, RootKind::Groups(vec!["sources".to_string()]));
        assert_eq!(
            foo.branches, 2,
            "both b1 and b2 converge on it: {:?}",
            chain.roots
        );
    }

    /// Two consumers ask for two DIFFERENT units that both fall back, by name,
    /// to the same legacy producer. Tracking the branch by what was asked for
    /// would explore that one event twice and report two roots for it; tracking
    /// it by the event actually selected collapses them into one node, which is
    /// what the walk analyzed (#627).
    #[test]
    fn two_requested_units_resolving_to_one_legacy_event_are_one_node() {
        let events = vec![
            with_extern_units(
                event(
                    "app",
                    EventResult::LocalHit,
                    0,
                    &[("b1", "aa"), ("b2", "aa")],
                ),
                &[("b1", "ub1"), ("b2", "ub2")],
            ),
            with_extern_units(
                with_unit(
                    event("b1", EventResult::LocalHit, 1, &[("foo", "x1")]),
                    "ub1",
                ),
                // b1 and b2 disagree about which unit of `foo` they used, and
                // neither id exists in the window.
                &[("foo", "ufoo_a")],
            ),
            with_extern_units(
                with_unit(
                    event("b2", EventResult::LocalHit, 2, &[("foo", "x1")]),
                    "ub2",
                ),
                &[("foo", "ufoo_b")],
            ),
            with_fields(
                event("foo", EventResult::LocalHit, 3, &[]),
                &[("sources", "1111")],
            ),
            with_fields(
                event("foo", EventResult::Miss, 4, &[]),
                &[("sources", "2222")],
            ),
            with_extern_units(
                with_unit(event("b1", EventResult::Miss, 5, &[("foo", "x2")]), "ub1"),
                &[("foo", "ufoo_a")],
            ),
            with_extern_units(
                with_unit(event("b2", EventResult::Miss, 6, &[("foo", "x2")]), "ub2"),
                &[("foo", "ufoo_b")],
            ),
            with_extern_units(
                event("app", EventResult::Miss, 7, &[("b1", "bb"), ("b2", "bb")]),
                &[("b1", "ub1"), ("b2", "ub2")],
            ),
        ];

        let chain = analyze_last(&events).expect("cascade should be reported");
        let foo: Vec<&Root> = chain
            .roots
            .iter()
            .filter(|r| r.crate_name == "foo")
            .collect();
        assert_eq!(foo.len(), 1, "one event, one node: {:?}", chain.roots);
        assert_eq!(foo[0].branches, 2, "both consumers converge on it");
    }

    /// Mixed windows happen across an upgrade: the compile carries a unit id,
    /// the only earlier event for it does not. Falling back to the name keeps
    /// the walk working rather than going blind.
    #[test]
    fn falls_back_to_the_name_when_only_legacy_events_are_available() {
        let events = vec![
            event("foo", EventResult::LocalHit, 0, &[("libc", "aaaa")]),
            with_unit(
                event("foo", EventResult::Miss, 10, &[("libc", "bbbb")]),
                "ufoo",
            ),
        ];

        assert_eq!(
            changed_deps_at(&events, 1),
            Some(vec![ChangedDep {
                name: "libc".to_string(),
                from: Some("aaaa".to_string()),
                to: Some("bbbb".to_string()),
                unit: None,
            }])
        );
    }

    /// The #580 shape: a leaf `-sys` crate's artifact moves and re-keys two
    /// crates above it. The walk must name the leaf, not the crate asked about.
    #[test]
    fn walks_a_cascade_to_the_leaf_that_changed() {
        let events = vec![
            event(
                "rig_core",
                EventResult::LocalHit,
                0,
                &[("aws_lc_rs", "aaaa")],
            ),
            event(
                "aws_lc_rs",
                EventResult::LocalHit,
                1,
                &[("aws_lc_sys", "bbbb")],
            ),
            with_fields(
                event("aws_lc_sys", EventResult::LocalHit, 2, &[("libc", "cccc")]),
                &[("sources", "1111")],
            ),
            with_fields(
                event("aws_lc_sys", EventResult::Miss, 10, &[("libc", "cccc")]),
                &[("sources", "2222")],
            ),
            event(
                "aws_lc_rs",
                EventResult::Miss,
                11,
                &[("aws_lc_sys", "dddd")],
            ),
            event("rig_core", EventResult::Miss, 12, &[("aws_lc_rs", "eeee")]),
        ];

        let chain = analyze_last(&events).expect("cascade should be reported");
        assert_eq!(chain.roots.len(), 1);
        let root = &chain.roots[0];
        assert_eq!(root.crate_name, "aws_lc_sys");
        assert_eq!(root.kind, RootKind::Groups(vec!["sources".to_string()]));
        assert_eq!(
            root.path
                .iter()
                .map(|h| (h.crate_name.as_str(), h.via.name.as_str()))
                .collect::<Vec<_>>(),
            vec![("rig_core", "aws_lc_rs"), ("aws_lc_rs", "aws_lc_sys")]
        );
        assert!(chain.truncated.is_none());
        assert!(chain.has_resolved_root());
    }

    /// Several dependencies moving is the NORMAL case in a cascade (#580's
    /// `rig_core` sits above a whole re-keyed subtree). Every branch must be
    /// walked, and the root they converge on ranked first — following one
    /// branch and calling it "the root" would be an arbitrary choice.
    #[test]
    fn ranks_the_root_that_most_branches_converge_on() {
        let events = vec![
            event(
                "top",
                EventResult::LocalHit,
                0,
                &[("a", "1111"), ("b", "2222")],
            ),
            event("a", EventResult::LocalHit, 1, &[("leaf", "5555")]),
            event("b", EventResult::LocalHit, 2, &[("leaf", "5555")]),
            with_fields(
                event("leaf", EventResult::LocalHit, 3, &[("libc", "9999")]),
                &[("sources", "1111")],
            ),
            with_fields(
                event("leaf", EventResult::Miss, 10, &[("libc", "9999")]),
                &[("sources", "2222")],
            ),
            event("a", EventResult::Miss, 11, &[("leaf", "6666")]),
            event("b", EventResult::Miss, 12, &[("leaf", "6666")]),
            event(
                "top",
                EventResult::Miss,
                13,
                &[("a", "3333"), ("b", "4444")],
            ),
        ];

        let chain = analyze_last(&events).unwrap();
        assert_eq!(chain.direct.len(), 2, "both direct dependencies moved");
        let leaf = &chain.roots[0];
        assert_eq!(leaf.crate_name, "leaf");
        assert_eq!(leaf.branches, 2, "reached via both a and b");
        assert_eq!(leaf.kind, RootKind::Groups(vec!["sources".to_string()]));
    }

    /// A dependency whose own history is not comparable must NOT be reported as
    /// a root: missing data is not evidence that its dependencies were stable.
    #[test]
    fn undiffable_history_is_not_reported_as_a_root() {
        let events = vec![
            // b's baseline recorded no digests (explain_miss was off then).
            unrecorded("b", EventResult::LocalHit, 0),
            with_fields(
                event("b", EventResult::Miss, 1, &[("c", "1111")]),
                &[("sources", "2222")],
            ),
            event("a", EventResult::LocalHit, 2, &[("b", "aaaa")]),
            event("a", EventResult::Miss, 3, &[("b", "bbbb")]),
        ];
        let chain = analyze_last(&events).unwrap();
        assert_eq!(chain.roots.len(), 1);
        assert_eq!(chain.roots[0].crate_name, "b");
        assert_eq!(chain.roots[0].kind, RootKind::NoDiffableHistory);
        assert!(
            !chain.has_resolved_root(),
            "an undiffable endpoint must not read as an explanation"
        );
    }

    /// After two consecutive misses the baseline is the previous MISS, not the
    /// older hit — otherwise the diff reports everything that moved across
    /// both compiles and can follow the wrong branch.
    #[test]
    fn baseline_is_the_previous_recorded_state_not_the_last_hit() {
        let events = vec![
            event(
                "b",
                EventResult::LocalHit,
                0,
                &[("c", "1111"), ("d", "2222")],
            ),
            // c moved here.
            event("b", EventResult::Miss, 1, &[("c", "9999"), ("d", "2222")]),
            // only d moved here; c is unchanged since the previous miss.
            event("b", EventResult::Miss, 2, &[("c", "9999"), ("d", "8888")]),
        ];
        let chain = analyze_last(&events).unwrap();
        assert_eq!(
            chain
                .direct
                .iter()
                .map(|d| d.name.as_str())
                .collect::<Vec<_>>(),
            vec!["d"],
            "diffing against the last hit would wrongly also report c"
        );
    }

    /// Walking must descend strictly backwards through the log, so a later,
    /// causally unrelated compile of a dependency is never selected.
    #[test]
    fn dependency_lookup_cannot_select_a_later_compile() {
        let events = vec![
            with_fields(
                event("c", EventResult::LocalHit, 0, &[("d", "1111")]),
                &[("sources", "aaaa")],
            ),
            event("b", EventResult::LocalHit, 1, &[("c", "5555")]),
            event("a", EventResult::LocalHit, 2, &[("b", "7777")]),
            // The compile of c that b actually consumed: driven by d.
            with_fields(
                event("c", EventResult::Miss, 10, &[("d", "2222")]),
                &[("sources", "aaaa")],
            ),
            event("b", EventResult::Miss, 11, &[("c", "6666")]),
            // A LATER, independent change to c, after b was already built.
            with_fields(
                event("c", EventResult::Miss, 12, &[("d", "2222")]),
                &[("sources", "bbbb")],
            ),
            event("a", EventResult::Miss, 13, &[("b", "8888")]),
        ];
        let chain = analyze_last(&events).unwrap();
        // c's dependencies moved at the event b consumed, so the walk descends
        // past c to d rather than stopping at c's later source change.
        assert_eq!(chain.roots[0].crate_name, "d");
        assert_eq!(chain.roots[0].kind, RootKind::NoMissRecorded);
    }

    /// A crate with no dependency movement is not part of a cascade.
    #[test]
    fn reports_nothing_when_dependencies_are_stable() {
        let events = vec![
            event("foo", EventResult::LocalHit, 0, &[("bar", "aaaa")]),
            event("foo", EventResult::Miss, 1, &[("bar", "aaaa")]),
        ];
        assert!(analyze_last(&events).is_none());
    }

    /// Without recorded digests there is nothing to walk, and the caller must
    /// fall back to the existing diagnosis rather than print an empty chain.
    #[test]
    fn reports_nothing_without_recorded_digests() {
        let events = vec![
            unrecorded("foo", EventResult::LocalHit, 0),
            unrecorded("foo", EventResult::Miss, 1),
        ];
        assert!(analyze_last(&events).is_none());
    }

    /// A rootless legacy event must not stand in as a baseline for a crate of
    /// the same name in a real build tree.
    #[test]
    fn rootless_events_are_not_wildcards() {
        let mut legacy = event("app", EventResult::LocalHit, 0, &[("dep", "1111")]);
        legacy.root = String::new();
        let events = vec![
            legacy,
            event("app", EventResult::Miss, 1, &[("dep", "2222")]),
        ];
        assert!(
            analyze_last(&events).is_none(),
            "an unrelated rootless event must not seed a cascade"
        );
    }

    /// Digests that point in a loop must terminate the walk and be reported as
    /// unexplained. Cargo forbids real dependency cycles, so a loop here means
    /// two compilation units were paired by a shared crate name — which is
    /// exactly when a confident answer would be wrong.
    #[test]
    fn terminates_on_a_cycle_in_recorded_digests() {
        let events = vec![
            event("a", EventResult::LocalHit, 0, &[("b", "1111")]),
            event("b", EventResult::LocalHit, 1, &[("a", "3333")]),
            event("b", EventResult::Miss, 10, &[("a", "4444")]),
            event("a", EventResult::Miss, 11, &[("b", "2222")]),
        ];
        let chain = analyze_last(&events).unwrap();
        assert_eq!(
            chain.truncated,
            Some("cycle in recorded dependency digests")
        );
        assert!(
            !chain.has_resolved_root(),
            "a cycle must not yield a confident root: {:?}",
            chain.roots
        );
        assert!(chain.roots.iter().all(|r| r.path.len() <= MAX_DEPTH));
    }

    /// A chain exactly MAX_DEPTH long ends in a real root, not a truncation.
    #[test]
    fn a_chain_at_the_depth_limit_still_resolves() {
        let mut events = Vec::new();
        let names: Vec<String> = (0..=MAX_DEPTH).map(|i| format!("c{i}")).collect();
        let last = names.len() - 1;
        // Every crate keeps one dependency, including the deepest: an empty
        // digest map cannot be told apart from an unrecorded one, so a
        // dependency-free crate would end the branch unresolved for reasons
        // unrelated to the depth limit under test.
        let deps_of = |i: usize, digest: &'static str| -> Vec<(String, &'static str)> {
            match names.get(i + 1) {
                Some(next) => vec![(next.clone(), digest)],
                None => vec![("libc".to_string(), "stable")],
            }
        };
        // Baselines, shallowest first.
        for (i, name) in names.iter().enumerate() {
            let deps = deps_of(i, "old");
            let deps: Vec<(&str, &str)> = deps.iter().map(|(n, d)| (n.as_str(), *d)).collect();
            events.push(with_fields(
                event(name, EventResult::LocalHit, i as i64, &deps),
                &[("sources", "1111")],
            ));
        }
        // Misses, deepest first, so each parent's dependency compile precedes it.
        for (i, name) in names.iter().enumerate().rev() {
            let deps = deps_of(i, "new");
            let deps: Vec<(&str, &str)> = deps.iter().map(|(n, d)| (n.as_str(), *d)).collect();
            events.push(with_fields(
                event(
                    name,
                    EventResult::Miss,
                    100 + (names.len() - i) as i64,
                    &deps,
                ),
                &[("sources", if i == last { "2222" } else { "1111" })],
            ));
        }
        let start = events
            .iter()
            .rposition(|e| e.crate_name == "c0" && e.result == EventResult::Miss)
            .unwrap();

        let chain = analyze(&events, start).unwrap();
        assert_eq!(chain.roots[0].crate_name, format!("c{MAX_DEPTH}"));
        assert!(
            chain.roots[0].kind.is_resolved(),
            "a root exactly at the limit must still resolve: {:?}",
            chain.roots[0].kind
        );
        assert!(chain.truncated.is_none());
    }

    #[test]
    fn diff_externs_reports_added_and_removed() {
        let before: BTreeMap<String, String> = [
            ("keep".to_string(), "1".to_string()),
            ("gone".to_string(), "2".to_string()),
        ]
        .into_iter()
        .collect();
        let after: BTreeMap<String, String> = [
            ("keep".to_string(), "1".to_string()),
            ("new".to_string(), "3".to_string()),
        ]
        .into_iter()
        .collect();
        // The unit map covers only the current dependency set, so the removed
        // one carries no unit and the added one does.
        let units: BTreeMap<String, String> = [("new".to_string(), "unit3".to_string())]
            .into_iter()
            .collect();
        assert_eq!(
            diff_externs(&before, &after, &units),
            vec![
                ChangedDep {
                    name: "gone".to_string(),
                    from: Some("2".to_string()),
                    to: None,
                    unit: None,
                },
                ChangedDep {
                    name: "new".to_string(),
                    from: None,
                    to: Some("3".to_string()),
                    unit: Some("unit3".to_string()),
                },
            ]
        );
    }

    /// The passthrough join is by package directory, so a shorter crate name
    /// must not absorb a longer one's uncached compiles, and a directory that
    /// merely starts with the name must not read as a version.
    #[test]
    fn package_dir_match_is_component_and_version_aware() {
        assert!(package_dir_matches(
            "/home/u/.cargo/registry/src/idx/aws-lc-sys-0.43.0",
            "aws-lc-sys"
        ));
        assert!(package_dir_matches("/src/aws-lc-sys", "aws-lc-sys"));
        assert!(package_dir_matches(
            "/registry/src/idx/serde-1.0.0-alpha.1",
            "serde"
        ));
        // `aws-lc` must not swallow `aws-lc-sys`.
        assert!(!package_dir_matches(
            "/home/u/.cargo/registry/src/idx/aws-lc-sys-0.43.0",
            "aws-lc"
        ));
        // A substring that isn't a whole component doesn't count.
        assert!(!package_dir_matches(
            "/src/my-aws-lc-sys-fork",
            "aws-lc-sys"
        ));
        // "starts with a digit" is not enough to be a version.
        assert!(!package_dir_matches("/src/foo-2-helper-0.1.0", "foo"));
        // Windows separators resolve even when parsed on Unix.
        assert!(package_dir_matches(
            r"C:\Users\u\.cargo\registry\src\idx\aws-lc-sys-0.43.0",
            "aws-lc-sys"
        ));
    }

    /// The root crate's uncached cc TUs are the actionable half of the answer.
    #[test]
    fn attributes_passthroughs_to_the_root_package_dir() {
        let mut pt = BuildEvent::new_for_test("bcm.c", EventResult::Passthrough);
        pt.ts = ts(5);
        pt.root = "/home/u/.cargo/registry/src/idx/aws-lc-sys-0.43.0".to_string();
        pt.passthrough_reason = "cc unsupported flag(s): --include=... not yet".to_string();

        let mut unrelated = pt.clone();
        unrelated.root = "/home/u/.cargo/registry/src/idx/ring-0.17.8".to_string();

        let events = vec![
            with_fields(
                event("aws_lc_sys", EventResult::LocalHit, 0, &[("libc", "cccc")]),
                &[("sources", "1111")],
            ),
            pt.clone(),
            pt,
            unrelated,
            with_fields(
                event("aws_lc_sys", EventResult::Miss, 10, &[("libc", "cccc")]),
                &[("sources", "2222")],
            ),
        ];
        let root = classify_at(&events, events.len() - 1, Vec::new());
        assert_eq!(root.passthroughs.len(), 1);
        assert_eq!(root.passthroughs[0].count, 2);
        assert!(root.passthroughs[0].reason.contains("--include="));
    }

    /// Passthroughs logged after the compile being explained belong to a later
    /// build and must not be folded in.
    #[test]
    fn passthrough_attribution_is_bounded_to_earlier_events() {
        let mut pt = BuildEvent::new_for_test("bcm.c", EventResult::Passthrough);
        pt.root = "/registry/src/idx/aws-lc-sys-0.43.0".to_string();
        pt.passthrough_reason = "later build".to_string();
        let events = vec![
            event("aws_lc_sys", EventResult::Miss, 0, &[("libc", "cccc")]),
            pt,
        ];
        assert!(passthroughs_for(&events, "aws_lc_sys", 1).is_empty());
    }
}
