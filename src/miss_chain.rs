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
//! walk mechanical: diff a crate's miss against its own last hit to see WHICH
//! dependency moved, then repeat for that dependency until reaching one whose
//! divergence is not itself dependency-driven. That crate is the root.
//!
//! Everything here is a pure function over already-read events: no store, no
//! filesystem, no clock. `cli` renders the result.

use crate::events::{BuildEvent, EventResult};
use chrono::{DateTime, Utc};
use std::collections::{BTreeMap, HashSet};

/// Hops to follow before giving up. A cascade deeper than this is possible in
/// principle; the walk stops so a pathological graph can't spin, and says so.
const MAX_DEPTH: usize = 12;

/// Changed dependencies listed per hop before eliding the rest.
const MAX_LISTED: usize = 4;

/// A dependency whose artifact digest differs between a crate's last hit and
/// its subsequent miss.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangedDep {
    pub name: String,
    /// Digest at the last hit. `None` when the dependency is new since then.
    pub from: Option<String>,
    /// Digest at the miss. `None` when the dependency went away.
    pub to: Option<String>,
}

/// One step down the cascade: `crate_name` missed because `via` moved.
#[derive(Debug, Clone, PartialEq)]
pub struct Hop {
    pub crate_name: String,
    pub via: ChangedDep,
    /// Other dependencies that also moved at this hop. The walk follows one;
    /// this keeps the rest from being silently dropped.
    pub siblings: Vec<String>,
}

/// Why the crate at the end of the chain diverged.
#[derive(Debug, Clone, PartialEq)]
pub enum RootKind {
    /// Key input groups that moved, `externs` excluded — this is the crate
    /// whose own inputs changed.
    Groups(Vec<String>),
    /// Its dependencies are unchanged and so are its recorded groups. Usually
    /// means the divergence sits in a post-hoc fold (key salt, extra inputs).
    NothingRecorded,
    /// The dependency moved but has no miss recorded in the event window, so
    /// the walk cannot go further. Not a dead end worth hiding: it still names
    /// the crate to look at.
    NoMissRecorded,
    /// No prior hit to diff this crate against.
    NoBaseline,
}

/// Passthrough (uncached) compiles attributed to the root crate, grouped by
/// reason. This is what turns "aws_lc_sys's sources changed" into "aws_lc_sys
/// contains uncached cc TUs, and here is the flag that blocked them".
#[derive(Debug, Clone, PartialEq)]
pub struct PassthroughGroup {
    pub reason: String,
    pub count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Root {
    pub crate_name: String,
    pub kind: RootKind,
    pub passthroughs: Vec<PassthroughGroup>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Chain {
    pub hops: Vec<Hop>,
    pub root: Root,
    /// Set when the walk stopped early rather than reaching a root.
    pub truncated: Option<&'static str>,
}

/// Walk the cascade starting from `crate_name`'s miss.
///
/// `None` means there is nothing to report: no per-dependency digests were
/// recorded (the wrapper writes them only under `[cache] explain_miss`, or the
/// events predate #609), or this crate's dependencies did not move — in which
/// case the existing diagnosis already says everything known.
pub fn analyze(events: &[BuildEvent], miss: &BuildEvent) -> Option<Chain> {
    if miss.key_externs.is_empty() {
        return None;
    }
    let changed = changed_deps(events, &miss.crate_name, &miss.root, miss.ts)?;
    if changed.is_empty() {
        return None;
    }

    let mut hops = Vec::new();
    let mut visited: HashSet<String> = HashSet::new();
    visited.insert(miss.crate_name.clone());

    let mut current = miss.crate_name.clone();
    let mut current_changed = changed;
    let mut truncated = None;

    let root = loop {
        // Follow a dependency that actually has a compiled event to inspect;
        // a dependency that only ever hit cannot be the thing that moved.
        let followed = current_changed
            .iter()
            .find(|dep| {
                !visited.contains(&dep.name)
                    && last_compiled(events, &dep.name, &miss.root, miss.ts).is_some()
            })
            .or_else(|| current_changed.iter().find(|d| !visited.contains(&d.name)))
            .cloned();

        let Some(dep) = followed else {
            // Every changed dependency is already on the path: a cycle in the
            // recorded data, not a real dependency cycle. Stop and report the
            // crate we are on.
            truncated = Some("cycle in recorded dependency digests");
            break classify(events, &current, &miss.root, miss.ts);
        };

        let siblings = current_changed
            .iter()
            .filter(|d| d.name != dep.name)
            .map(|d| d.name.clone())
            .take(MAX_LISTED)
            .collect();
        hops.push(Hop {
            crate_name: current.clone(),
            via: dep.clone(),
            siblings,
        });

        if hops.len() >= MAX_DEPTH {
            truncated = Some("chain longer than the walk limit");
            break classify(events, &dep.name, &miss.root, miss.ts);
        }

        visited.insert(dep.name.clone());

        // Does this dependency's own miss point further down?
        match changed_deps(events, &dep.name, &miss.root, miss.ts) {
            Some(next) if !next.is_empty() => {
                current = dep.name;
                current_changed = next;
            }
            // Its dependencies are stable (or undiffable): it is the root.
            _ => break classify(events, &dep.name, &miss.root, miss.ts),
        }
    };

    Some(Chain {
        hops,
        root,
        truncated,
    })
}

/// Dependencies whose digests differ between `crate_name`'s last compile at or
/// before `at` and the last hit preceding it.
///
/// `None` when there is no diffable pair (no compile, no prior hit, or either
/// side recorded no digests) — distinct from `Some(vec![])`, which means the
/// pair exists and the dependencies are identical.
fn changed_deps(
    events: &[BuildEvent],
    crate_name: &str,
    root: &str,
    at: DateTime<Utc>,
) -> Option<Vec<ChangedDep>> {
    let compiled = last_compiled(events, crate_name, root, at)?;
    if compiled.key_externs.is_empty() {
        return None;
    }
    let hit = last_hit_before(events, crate_name, root, compiled.ts)?;
    if hit.key_externs.is_empty() {
        return None;
    }
    Some(diff_externs(&hit.key_externs, &compiled.key_externs))
}

/// Symmetric diff of two dependency-digest maps, sorted by name.
fn diff_externs(
    before: &BTreeMap<String, String>,
    after: &BTreeMap<String, String>,
) -> Vec<ChangedDep> {
    let mut out = Vec::new();
    for (name, to) in after {
        match before.get(name) {
            Some(from) if from == to => {}
            from => out.push(ChangedDep {
                name: name.clone(),
                from: from.cloned(),
                to: Some(to.clone()),
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
            });
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// Explain a crate's own divergence, dependencies aside.
fn classify(events: &[BuildEvent], crate_name: &str, root: &str, at: DateTime<Utc>) -> Root {
    let passthroughs = passthroughs_for(events, crate_name, root, at);
    let Some(compiled) = last_compiled(events, crate_name, root, at) else {
        return Root {
            crate_name: crate_name.to_string(),
            kind: RootKind::NoMissRecorded,
            passthroughs,
        };
    };
    let Some(hit) = last_hit_before(events, crate_name, root, compiled.ts) else {
        return Root {
            crate_name: crate_name.to_string(),
            kind: RootKind::NoBaseline,
            passthroughs,
        };
    };

    // Prefer the diff the wrapper already computed; fall back to diffing the
    // group digests here when `explain_miss` was off at the time.
    let mut groups: Vec<String> = if compiled.key_diff.is_empty() {
        changed_groups(&hit.key_fields, &compiled.key_fields)
    } else {
        compiled.key_diff.clone()
    };
    groups.retain(|g| g != "externs");
    groups.sort();
    groups.dedup();

    let kind = if groups.is_empty() {
        RootKind::NothingRecorded
    } else {
        RootKind::Groups(groups)
    };
    Root {
        crate_name: crate_name.to_string(),
        kind,
        passthroughs,
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
/// dependency contains the package directory (`.../aws-lc-sys-0.43.0/...`).
/// Matching on that names the right crate for the case this exists to
/// diagnose, and when it is wrong it over-reports rather than pointing
/// somewhere else. `cli` labels the line as inferred.
fn passthroughs_for(
    events: &[BuildEvent],
    crate_name: &str,
    root: &str,
    at: DateTime<Utc>,
) -> Vec<PassthroughGroup> {
    let needle = crate_name.replace('_', "-");
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for event in events {
        if event.result != EventResult::Passthrough || event.ts > at {
            continue;
        }
        // Skip the build tree's own root: every event shares it, so matching
        // on it would attribute the whole build to whichever crate is asked
        // about.
        if event.root == root || event.root.is_empty() {
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
    out.truncate(MAX_LISTED);
    out
}

/// Whether any path component of `root` is the package directory for
/// `needle` — exactly `needle`, or `needle-<version>`.
///
/// Component-wise and version-aware on purpose: a plain `contains` would let
/// `aws-lc` match `aws-lc-sys-0.43.0` and attribute one crate's passthroughs
/// to another.
fn package_dir_matches(root: &str, needle: &str) -> bool {
    std::path::Path::new(root)
        .components()
        .filter_map(|c| c.as_os_str().to_str())
        .any(|component| {
            component == needle
                || component
                    .strip_prefix(needle)
                    .and_then(|rest| rest.strip_prefix('-'))
                    .is_some_and(|version| version.starts_with(|c: char| c.is_ascii_digit()))
        })
}

fn last_compiled<'a>(
    events: &'a [BuildEvent],
    crate_name: &str,
    root: &str,
    at: DateTime<Utc>,
) -> Option<&'a BuildEvent> {
    events.iter().rev().find(|e| {
        e.crate_name == crate_name
            && same_root(e, root)
            && e.ts <= at
            && matches!(e.result, EventResult::Miss | EventResult::Dup)
    })
}

fn last_hit_before<'a>(
    events: &'a [BuildEvent],
    crate_name: &str,
    root: &str,
    before: DateTime<Utc>,
) -> Option<&'a BuildEvent> {
    events.iter().rev().find(|e| {
        e.crate_name == crate_name
            && same_root(e, root)
            && e.ts < before
            && matches!(
                e.result,
                EventResult::LocalHit | EventResult::PrefetchHit | EventResult::RemoteHit
            )
    })
}

/// Scope to one build tree, tolerating legacy events that recorded no root
/// (matching those would be better than dropping the whole diagnosis).
fn same_root(event: &BuildEvent, root: &str) -> bool {
    root.is_empty() || event.root.is_empty() || event.root == root
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

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
        e
    }

    fn with_fields(mut e: BuildEvent, fields: &[(&str, &str)]) -> BuildEvent {
        e.key_fields = fields
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        e
    }

    /// The #580 shape: a leaf `-sys` crate's artifact moves and re-keys two
    /// crates above it. The walk must name the leaf, not the crate asked about.
    #[test]
    fn walks_a_cascade_to_the_leaf_that_changed() {
        let events = vec![
            // Baseline hits.
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
            // The leaf recompiles with different sources; the cascade follows.
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
        let miss = events.last().unwrap();

        let chain = analyze(&events, miss).expect("cascade should be reported");
        assert_eq!(
            chain
                .hops
                .iter()
                .map(|h| (h.crate_name.as_str(), h.via.name.as_str()))
                .collect::<Vec<_>>(),
            vec![("rig_core", "aws_lc_rs"), ("aws_lc_rs", "aws_lc_sys")]
        );
        assert_eq!(chain.root.crate_name, "aws_lc_sys");
        assert_eq!(
            chain.root.kind,
            RootKind::Groups(vec!["sources".to_string()])
        );
        assert!(chain.truncated.is_none());
    }

    /// A crate whose own dependencies are unchanged is not part of a cascade,
    /// and must not be dressed up as one.
    #[test]
    fn reports_nothing_when_dependencies_are_stable() {
        let events = vec![
            event("foo", EventResult::LocalHit, 0, &[("bar", "aaaa")]),
            event("foo", EventResult::Miss, 1, &[("bar", "aaaa")]),
        ];
        assert!(analyze(&events, events.last().unwrap()).is_none());
    }

    /// Without recorded digests there is nothing to walk, and the caller must
    /// fall back to the existing diagnosis rather than print an empty chain.
    #[test]
    fn reports_nothing_without_recorded_digests() {
        let events = vec![
            event("foo", EventResult::LocalHit, 0, &[]),
            event("foo", EventResult::Miss, 1, &[]),
        ];
        assert!(analyze(&events, events.last().unwrap()).is_none());
    }

    /// A dependency that moved but never missed in the window is still worth
    /// naming; the walk just cannot go past it.
    #[test]
    fn stops_at_a_dependency_with_no_recorded_miss() {
        let events = vec![
            event("foo", EventResult::LocalHit, 0, &[("bar", "aaaa")]),
            event("foo", EventResult::Miss, 1, &[("bar", "bbbb")]),
        ];
        let chain = analyze(&events, events.last().unwrap()).unwrap();
        assert_eq!(chain.root.crate_name, "bar");
        assert_eq!(chain.root.kind, RootKind::NoMissRecorded);
    }

    /// Digests that point in a loop must terminate the walk, not spin it.
    #[test]
    fn terminates_on_a_cycle_in_recorded_digests() {
        let events = vec![
            event("a", EventResult::LocalHit, 0, &[("b", "1111")]),
            event("b", EventResult::LocalHit, 1, &[("a", "3333")]),
            event("b", EventResult::Miss, 10, &[("a", "4444")]),
            event("a", EventResult::Miss, 11, &[("b", "2222")]),
        ];
        let chain = analyze(&events, events.last().unwrap()).unwrap();
        assert_eq!(
            chain.truncated,
            Some("cycle in recorded dependency digests")
        );
        assert!(chain.hops.len() <= MAX_DEPTH);
    }

    /// Several dependencies moving at once must not silently drop all but one.
    #[test]
    fn records_siblings_when_several_dependencies_moved() {
        let events = vec![
            event(
                "foo",
                EventResult::LocalHit,
                0,
                &[("bar", "aaaa"), ("baz", "cccc")],
            ),
            event(
                "foo",
                EventResult::Miss,
                1,
                &[("bar", "bbbb"), ("baz", "dddd")],
            ),
        ];
        let chain = analyze(&events, events.last().unwrap()).unwrap();
        assert_eq!(chain.hops.len(), 1);
        assert_eq!(chain.hops[0].siblings, vec!["baz".to_string()]);
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
        let diff = diff_externs(&before, &after);
        assert_eq!(
            diff,
            vec![
                ChangedDep {
                    name: "gone".to_string(),
                    from: Some("2".to_string()),
                    to: None
                },
                ChangedDep {
                    name: "new".to_string(),
                    from: None,
                    to: Some("3".to_string())
                },
            ]
        );
    }

    /// The passthrough join is by package directory, so a shorter crate name
    /// must not absorb a longer one's uncached compiles.
    #[test]
    fn package_dir_match_is_component_and_version_aware() {
        assert!(package_dir_matches(
            "/home/u/.cargo/registry/src/idx/aws-lc-sys-0.43.0",
            "aws-lc-sys"
        ));
        assert!(package_dir_matches("/src/aws-lc-sys", "aws-lc-sys"));
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
    }

    /// The root crate's uncached cc TUs are the actionable half of the answer.
    #[test]
    fn attributes_passthroughs_to_the_root_package_dir() {
        let mut pt = BuildEvent::new_for_test("bcm.c", EventResult::Passthrough);
        pt.ts = ts(5);
        pt.root = "/home/u/.cargo/registry/src/idx/aws-lc-sys-0.43.0".to_string();
        pt.passthrough_reason = "cc unsupported flag(s): --include=... — not yet".to_string();

        let mut unrelated = pt.clone();
        unrelated.root = "/home/u/.cargo/registry/src/idx/ring-0.17.8".to_string();

        let events = vec![
            event("aws_lc_sys", EventResult::LocalHit, 0, &[("libc", "cccc")]),
            pt.clone(),
            pt,
            unrelated,
            event("aws_lc_sys", EventResult::Miss, 10, &[("libc", "cccc")]),
        ];
        let root = classify(&events, "aws_lc_sys", "/w", ts(10));
        assert_eq!(root.passthroughs.len(), 1);
        assert_eq!(root.passthroughs[0].count, 2);
        assert!(root.passthroughs[0].reason.contains("--include="));
    }
}
