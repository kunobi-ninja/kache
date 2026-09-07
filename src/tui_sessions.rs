//! Build sessions and miss causes for the monitor.
//!
//! The event log is one flat stream across every workspace and every
//! `cargo build` on the machine. A person watching it wants two things the
//! stream does not give directly: "which build is this row from" and "why did
//! that build miss". This module answers both as pure functions over the
//! events the monitor already holds, so `tui` only renders.
//!
//! # Sessions
//!
//! A session is one build: every event sharing a `session_id` (minted per
//! build by the wrapper, kunobi-ninja/kache#583). Events without one, from
//! older wrappers or non-cargo invocations, are grouped the way `kache report`
//! groups them: by build root, split wherever the root sat idle for at least
//! the wrapper's own session timeout.
//!
//! # Causes
//!
//! A miss row says what happened; the cause says why, using what the wrapper
//! recorded on the event. Precedence follows `kache why-miss`: a failed store
//! or a rejected lookup outranks everything, because either is a caching
//! failure to act on. Below that, a recorded dependency cascade names the
//! crate that actually changed (`miss_chain`), so forty downstream misses read
//! as one line. Then the crate's own changed key groups, then "no earlier
//! compile in the loaded history", and last "unexplained", which is what the
//! monitor says when the wrapper recorded nothing it could reason from.
//!
//! Everything here describes the loaded history, never more: with `--since`
//! the history starts at the cutoff, and the wording says so.

use crate::events::{BuildEvent, EventResult};
use crate::miss_chain;
use chrono::{DateTime, Utc};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

/// How long after its last event a session with nothing in flight still
/// counts as live. Long enough to bridge cargo's own gaps between compiles,
/// short enough that a finished build stops claiming the top row.
pub(crate) const LIVE_GRACE: Duration = Duration::from_secs(30);

/// The most misses one session's cause analysis walks. The cascade walk is
/// per miss and scans history, so a 3,000-crate cold build must not stall
/// the draw loop; the analysis says when it was capped.
pub(crate) const MAX_ANALYZED_MISSES: usize = 400;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionState {
    Live,
    Finished,
}

impl SessionState {
    pub(crate) fn label(self) -> &'static str {
        match self {
            SessionState::Live => "running",
            SessionState::Finished => "done",
        }
    }
}

/// Per-session counts and timings. Everything a row or a cost strip shows.
/// Sums saturate: a corrupt event with an absurd duration must not take the
/// monitor down, and a saturated total is visibly wrong rather than wrapped.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Tally {
    pub hits: u64,
    pub misses: u64,
    pub dups: u64,
    /// Passthroughs that were real compiles kache declined to cache.
    pub passthroughs: u64,
    /// Passthroughs that were queries (`--print`, `-vV`, `cc -###`), not
    /// compiles. Counted apart so they never read as a caching failure.
    pub probes: u64,
    pub skipped: u64,
    pub errors: u64,
    /// Compile time the hits did not have to spend (the entry's recorded
    /// compile cost).
    pub saved_ms: u64,
    /// Compile time spent in misses and dups.
    pub miss_ms: u64,
    /// Time kache itself spent around the compiler on lookups: the whole of
    /// a hit, and everything but the compile of a miss or dup. Passthroughs
    /// carry no compile timing, so they are left out rather than counted as
    /// pure overhead.
    pub overhead_ms: u64,
    /// Summed invocation time of those same lookups, the denominator for the
    /// overhead share. Invocations overlap under `-j`, so this is not the
    /// build's wall time.
    pub lookup_elapsed_ms: u64,
    pub reflinked_bytes: u64,
    pub hardlinked_bytes: u64,
    pub copied_bytes: u64,
}

/// Outcomes that consulted the cache: the only ones a hit rate, an overhead
/// share, or a chronic-miss count may be built from.
fn is_lookup(event: &BuildEvent) -> bool {
    matches!(
        event.result,
        EventResult::LocalHit
            | EventResult::PrefetchHit
            | EventResult::RemoteHit
            | EventResult::Miss
            | EventResult::Dup
    )
}

fn is_miss(event: &BuildEvent) -> bool {
    matches!(event.result, EventResult::Miss | EventResult::Dup)
}

impl Tally {
    pub(crate) fn add(&mut self, event: &BuildEvent) {
        match event.result {
            EventResult::LocalHit | EventResult::PrefetchHit | EventResult::RemoteHit => {
                self.hits = self.hits.saturating_add(1);
                self.saved_ms = self.saved_ms.saturating_add(event.compile_time_ms);
            }
            EventResult::Miss => {
                self.misses = self.misses.saturating_add(1);
                self.miss_ms = self.miss_ms.saturating_add(event.compile_time_ms);
            }
            EventResult::Dup => {
                self.dups = self.dups.saturating_add(1);
                self.miss_ms = self.miss_ms.saturating_add(event.compile_time_ms);
            }
            EventResult::Passthrough if is_probe(event) => {
                self.probes = self.probes.saturating_add(1);
            }
            EventResult::Passthrough => {
                self.passthroughs = self.passthroughs.saturating_add(1);
            }
            EventResult::Skipped => self.skipped = self.skipped.saturating_add(1),
            EventResult::Error => self.errors = self.errors.saturating_add(1),
        }
        if is_lookup(event) {
            self.overhead_ms = self.overhead_ms.saturating_add(event.overhead_ms());
            self.lookup_elapsed_ms = self.lookup_elapsed_ms.saturating_add(event.elapsed_ms);
        }
        self.reflinked_bytes = self.reflinked_bytes.saturating_add(event.reflinked_bytes);
        self.hardlinked_bytes = self.hardlinked_bytes.saturating_add(event.hardlinked_bytes);
        self.copied_bytes = self.copied_bytes.saturating_add(event.copied_bytes);
    }

    /// Misses and dups together: compiles that entered the cache and lost.
    pub(crate) fn compiled(&self) -> u64 {
        self.misses.saturating_add(self.dups)
    }

    /// Lookups that hit, over lookups alone. Passthroughs and probes never
    /// consulted the cache, so they are not in the denominator; folding them
    /// in would report a number no cache could move.
    pub(crate) fn hit_rate(&self) -> Option<f64> {
        let lookups = self.hits.saturating_add(self.compiled());
        (lookups > 0).then(|| self.hits as f64 * 100.0 / lookups as f64)
    }

    pub(crate) fn restored_bytes(&self) -> u64 {
        self.reflinked_bytes
            .saturating_add(self.hardlinked_bytes)
            .saturating_add(self.copied_bytes)
    }

    /// Share of restored bytes that were physically copied, as a percentage.
    /// Anything much above zero on a CoW filesystem is worth a look.
    pub(crate) fn copy_share(&self) -> Option<f64> {
        let total = self.restored_bytes();
        (total > 0).then(|| self.copied_bytes as f64 * 100.0 / total as f64)
    }

    /// kache's own time as a share of the lookups' summed invocation time,
    /// as a percentage.
    pub(crate) fn overhead_share(&self) -> Option<f64> {
        (self.lookup_elapsed_ms > 0)
            .then(|| self.overhead_ms as f64 * 100.0 / self.lookup_elapsed_ms as f64)
    }
}

/// One build, as far as the events show it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Session {
    /// Stable identity across redraws: `id:<session_id>` for a recorded
    /// session, `inferred:<root>:<ordinal>` for one grouped by idle gap. The
    /// prefixes keep the two from ever colliding.
    pub key: String,
    pub root: String,
    /// Grouped by idle gap rather than by a recorded id.
    pub inferred: bool,
    pub started: DateTime<Utc>,
    pub last_activity: DateTime<Utc>,
    /// Indices into the monitor's event list, in log order.
    pub events: Vec<usize>,
    pub tally: Tally,
    pub state: SessionState,
}

impl Session {
    /// The last path component of the root, which is what a reader
    /// recognizes; the whole root when there is no component to take.
    pub(crate) fn workspace_name(&self) -> &str {
        if self.root.is_empty() {
            return "(unknown root)";
        }
        std::path::Path::new(&self.root)
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .filter(|name| !name.is_empty())
            .unwrap_or(&self.root)
    }

    /// The recorded session id behind the key.
    #[cfg(test)]
    pub(crate) fn recorded_id(&self) -> Option<&str> {
        self.key.strip_prefix("id:")
    }

    /// The newest event index, the tie-breaker for "newest first".
    fn last_index(&self) -> usize {
        self.events.last().copied().unwrap_or(0)
    }
}

fn is_probe(event: &BuildEvent) -> bool {
    matches!(event.result, EventResult::Passthrough)
        && event
            .passthrough_reason
            .split('|')
            .next()
            .unwrap_or("")
            .trim()
            == "not-a-compile"
}

fn event_start(event: &BuildEvent) -> DateTime<Utc> {
    let elapsed = i64::try_from(event.elapsed_ms).unwrap_or(i64::MAX);
    event
        .ts
        .checked_sub_signed(chrono::Duration::milliseconds(elapsed))
        .unwrap_or(event.ts)
}

/// Group `events` into sessions, newest first with running builds on top.
/// [`group`] and [`refresh_state`] in one call; the monitor calls the two
/// apart so it can skip the regrouping on ticks where the log did not grow.
#[cfg(test)]
pub(crate) fn group_sessions(
    events: &[BuildEvent],
    now: DateTime<Utc>,
    live_roots: &HashSet<String>,
    idle_gap: Duration,
) -> Vec<Session> {
    let mut sessions = group(events, idle_gap);
    refresh_state(&mut sessions, now, live_roots);
    sessions
}

/// Group `events` into sessions, in order of first appearance and with no
/// state yet. Depends on the events alone, so the caller can keep the result
/// while the log does not grow and only [`refresh_state`] it each tick.
///
/// `idle_gap` splits inferred sessions; pass the wrapper's own session
/// timeout so the monitor and `kache report` agree on what one build is.
pub(crate) fn group(events: &[BuildEvent], idle_gap: Duration) -> Vec<Session> {
    let mut sessions: Vec<Session> = Vec::new();
    let mut by_id: HashMap<&str, usize> = HashMap::new();
    let mut inferred_by_root: HashMap<&str, usize> = HashMap::new();
    let idle_gap = chrono::Duration::from_std(idle_gap).unwrap_or(chrono::Duration::MAX);

    for (index, event) in events.iter().enumerate() {
        let slot = if !event.session_id.is_empty() {
            *by_id.entry(event.session_id.as_str()).or_insert_with(|| {
                sessions.push(Session {
                    key: format!("id:{}", event.session_id),
                    root: event.root.clone(),
                    inferred: false,
                    started: event_start(event),
                    last_activity: event.ts,
                    events: Vec::new(),
                    tally: Tally::default(),
                    state: SessionState::Finished,
                });
                sessions.len() - 1
            })
        } else {
            let current = inferred_by_root.get(event.root.as_str()).copied();
            match current {
                // Same rule as `kache report`: a gap of at least the timeout
                // is a new build. A backwards timestamp (clock step) joins
                // the current session rather than opening a phantom one.
                Some(slot)
                    if event.ts.signed_duration_since(sessions[slot].last_activity) < idle_gap =>
                {
                    slot
                }
                _ => {
                    // The key carries the session's ordinal, so two
                    // idle-split sessions of one root never collide, and it
                    // stays stable while the log only grows.
                    let ordinal = sessions.len();
                    sessions.push(Session {
                        key: format!("inferred:{}:{ordinal}", event.root),
                        root: event.root.clone(),
                        inferred: true,
                        started: event_start(event),
                        last_activity: event.ts,
                        events: Vec::new(),
                        tally: Tally::default(),
                        state: SessionState::Finished,
                    });
                    inferred_by_root.insert(event.root.as_str(), sessions.len() - 1);
                    sessions.len() - 1
                }
            }
        };
        let session = &mut sessions[slot];
        session.events.push(index);
        session.tally.add(event);
        session.started = session.started.min(event_start(event));
        session.last_activity = session.last_activity.max(event.ts);
        // A recorded id can outlive a root change only in a broken log; the
        // first root wins and later ones are ignored rather than flapping.
        if session.root.is_empty() {
            session.root = event.root.clone();
        }
    }
    sessions
}

/// Decide which sessions are live and sort them: running builds first, then
/// newest activity first, ties broken by log position.
///
/// `live_roots` are the build roots with a compile in flight right now (the
/// daemon registry or tailed heartbeats). A root names a tree, not a build,
/// so it can only keep the root's newest session live; it never revives an
/// older build of the same tree. Only time between the last event and `now`
/// counts as recent: an event stamped in the future is a clock problem, not
/// a running build.
pub(crate) fn refresh_state(
    sessions: &mut [Session],
    now: DateTime<Utc>,
    live_roots: &HashSet<String>,
) {
    // Owned keys: the map outlives the shared borrow it is built from.
    let mut newest_by_root: HashMap<String, usize> = HashMap::new();
    for session in sessions.iter() {
        if session.root.is_empty() {
            continue;
        }
        let last = session.last_index();
        newest_by_root
            .entry(session.root.clone())
            .and_modify(|newest| *newest = (*newest).max(last))
            .or_insert(last);
    }
    let grace = chrono::Duration::from_std(LIVE_GRACE).unwrap_or(chrono::Duration::MAX);
    for session in sessions.iter_mut() {
        let age = now.signed_duration_since(session.last_activity);
        let recent = age >= chrono::Duration::zero() && age < grace;
        let compiling = live_roots.contains(&session.root)
            && newest_by_root.get(&session.root) == Some(&session.last_index());
        session.state = if recent || compiling {
            SessionState::Live
        } else {
            SessionState::Finished
        };
    }
    // The build somebody is watching is the reason they opened the monitor.
    sessions.sort_by(|a, b| {
        (a.state != SessionState::Live)
            .cmp(&(b.state != SessionState::Live))
            .then_with(|| b.last_activity.cmp(&a.last_activity))
            .then_with(|| b.last_index().cmp(&a.last_index()))
            .then_with(|| a.key.cmp(&b.key))
    });
}

/// Why a miss missed, in `why-miss` precedence.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Cause {
    /// `Store::put` failed after the compile (kunobi-ninja/kache#629). The
    /// output was built and not cached.
    StoreFailed(String),
    /// An entry for this exact key existed but could not serve the
    /// invocation (kunobi-ninja/kache#655).
    LookupRejected(String),
    /// Downstream of dependencies whose artifacts changed; `root` names the
    /// crates at the bottom of the cascade, resolved ones first
    /// (kunobi-ninja/kache#609). `complete` is false when the walk was cut
    /// short or left a branch unresolved, so the names are contributors,
    /// not the whole story.
    Downstream { root: String, complete: bool },
    /// This crate's own key inputs changed; the groups that moved.
    OwnInputs(Vec<String>),
    /// No earlier compile of this crate in this build tree is in the loaded
    /// history. A cold cache or the `--since` cutoff; the events cannot tell.
    NoHistory,
    /// Earlier compiles exist but the wrapper recorded nothing that
    /// explains the change. Recording is off, the event predates it, or the
    /// root is unknown.
    Unexplained,
}

impl Cause {
    /// One line a reader can act on.
    pub(crate) fn describe(&self) -> String {
        match self {
            Cause::StoreFailed(reason) => format!("compiled but not cached: {reason}"),
            Cause::LookupRejected(reason) => format!("entry rejected at lookup: {reason}"),
            Cause::Downstream {
                root,
                complete: true,
            } => format!("downstream of {root}"),
            Cause::Downstream {
                root,
                complete: false,
            } => format!("downstream of {root} (and more; cascade not fully resolved)"),
            Cause::OwnInputs(groups) => format!("own inputs changed: {}", groups.join(", ")),
            Cause::NoHistory => "no earlier compile in the loaded history".to_string(),
            Cause::Unexplained => "unexplained (no key diff recorded)".to_string(),
        }
    }

    /// Whether the cause is a caching failure rather than a changed input:
    /// something to fix, not something that changed.
    pub(crate) fn is_failure(&self) -> bool {
        matches!(self, Cause::StoreFailed(_) | Cause::LookupRejected(_))
    }
}

/// Misses sharing one cause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CauseGroup {
    pub cause: Cause,
    pub count: usize,
    /// Compile time these misses cost.
    pub compile_ms: u64,
    /// Up to three crate names, in log order, so the group is recognizable.
    pub examples: Vec<String>,
}

/// Passthroughs sharing one reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PassthroughGroup {
    pub kind: String,
    pub reason: String,
    pub count: usize,
    pub probe: bool,
}

/// A crate that keeps missing across builds of the same tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Chronic {
    pub crate_name: String,
    /// Builds in which it missed, over builds in which it was looked up.
    pub missed: usize,
    pub seen: usize,
    /// Its latest lookup in the loaded history ended in a store failure.
    pub last_store_failed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Analysis {
    pub causes: Vec<CauseGroup>,
    pub passthroughs: Vec<PassthroughGroup>,
    pub chronic: Vec<Chronic>,
    pub misses_total: usize,
    pub misses_analyzed: usize,
    /// Some miss in the session (analyzed or not) carried dependency
    /// digests, so cascade roots were possible. When false and there were
    /// misses, the reader should be told what recording is missing.
    pub cascade_recorded: bool,
}

/// Explain the selected session's misses and passthroughs.
///
/// `events` is the monitor's whole list, in log order, because the cascade
/// walk and the history test both look at events before the session.
/// `sessions` is every session, for the chronic-miss count.
pub(crate) fn analyze_session(
    events: &[BuildEvent],
    session: &Session,
    sessions: &[Session],
) -> Analysis {
    let misses: Vec<usize> = session
        .events
        .iter()
        .copied()
        .filter(|&i| is_miss(&events[i]))
        .collect();
    let misses_total = misses.len();
    let cascade_recorded = misses.iter().any(|&i| events[i].key_externs_recorded);
    // Newest misses are the ones a reader is looking at; when capped, keep
    // those and drop the oldest.
    let analyzed: Vec<usize> = misses
        .iter()
        .copied()
        .skip(misses_total.saturating_sub(MAX_ANALYZED_MISSES))
        .collect();

    // The history test asks "was this crate compiled in this tree before
    // index i". One pass over the log answers it for every miss at once
    // instead of rescanning the prefix per miss.
    let mut first_compile: HashMap<(&str, &str), usize> = HashMap::new();
    for (index, event) in events.iter().enumerate() {
        let compiled = is_lookup(event)
            || (matches!(event.result, EventResult::Passthrough) && !is_probe(event));
        if compiled && !event.root.is_empty() {
            first_compile
                .entry((event.root.as_str(), event.crate_name.as_str()))
                .or_insert(index);
        }
    }

    let mut groups: BTreeMap<Cause, CauseGroup> = BTreeMap::new();
    for &index in &analyzed {
        let event = &events[index];
        let cause = cause_of(events, index, &first_compile);
        let group = groups.entry(cause.clone()).or_insert_with(|| CauseGroup {
            cause,
            count: 0,
            compile_ms: 0,
            examples: Vec::new(),
        });
        group.count += 1;
        group.compile_ms = group.compile_ms.saturating_add(event.compile_time_ms);
        if group.examples.len() < 3 && !group.examples.contains(&event.crate_name) {
            group.examples.push(event.crate_name.clone());
        }
    }
    let mut causes: Vec<CauseGroup> = groups.into_values().collect();
    causes.sort_by(|a, b| {
        b.count
            .cmp(&a.count)
            .then_with(|| b.compile_ms.cmp(&a.compile_ms))
            .then_with(|| a.cause.cmp(&b.cause))
    });

    let mut passes: BTreeMap<(String, String), PassthroughGroup> = BTreeMap::new();
    for &index in &session.events {
        let event = &events[index];
        if !matches!(event.result, EventResult::Passthrough) {
            continue;
        }
        let (kind, reason) = passthrough_parts(&event.passthrough_reason);
        let group = passes
            .entry((kind.to_string(), reason.to_string()))
            .or_insert_with(|| PassthroughGroup {
                kind: kind.to_string(),
                reason: reason.to_string(),
                count: 0,
                probe: is_probe(event),
            });
        group.count += 1;
    }
    let mut passthroughs: Vec<PassthroughGroup> = passes.into_values().collect();
    // Real compiles before probes, then most frequent first: the reason worth
    // acting on is the common one that was a compile.
    passthroughs.sort_by(|a, b| {
        a.probe
            .cmp(&b.probe)
            .then_with(|| b.count.cmp(&a.count))
            .then_with(|| a.reason.cmp(&b.reason))
    });

    Analysis {
        causes,
        passthroughs,
        chronic: chronic_misses(events, sessions, &session.root),
        misses_total,
        misses_analyzed: analyzed.len(),
        cascade_recorded,
    }
}

/// The `kind|detail` split kache writes into passthrough reasons, with the
/// legacy `refused: ...` form mapped to an empty kind.
pub(crate) fn passthrough_parts(reason: &str) -> (&str, &str) {
    let reason = reason.trim();
    if reason.is_empty() {
        return ("", "unknown");
    }
    match reason.split_once('|') {
        Some((kind, detail)) => (kind.trim(), detail.trim()),
        None => ("", reason.strip_prefix("refused: ").unwrap_or(reason)),
    }
}

fn cause_of(
    events: &[BuildEvent],
    index: usize,
    first_compile: &HashMap<(&str, &str), usize>,
) -> Cause {
    let event = &events[index];
    if !event.store_error.is_empty() {
        return Cause::StoreFailed(event.store_error.clone());
    }
    if !event.lookup_rejection.is_empty() {
        return Cause::LookupRejected(event.lookup_rejection.clone());
    }
    if let Some(chain) = miss_chain::analyze(events, index)
        && !chain.roots.is_empty()
    {
        // The walk ranks roots by how many branches converge on them. Every
        // root is named, resolved ones first, so a miss below two changed
        // crates does not read as below one; and the line admits when that
        // is not the whole story: a branch unresolved, or the walk cut short.
        let mut names: Vec<&str> = Vec::new();
        for root in chain
            .roots
            .iter()
            .filter(|root| root.kind.is_resolved())
            .chain(chain.roots.iter().filter(|root| !root.kind.is_resolved()))
        {
            if !names.contains(&root.crate_name.as_str()) {
                names.push(root.crate_name.as_str());
            }
        }
        // The walk itself caps the roots it returns; the renderer clips the
        // line, so every name is kept.
        let root = names.join(", ");
        let complete =
            chain.truncated.is_none() && chain.roots.iter().all(|root| root.kind.is_resolved());
        return Cause::Downstream { root, complete };
    }
    if !event.key_diff.is_empty() {
        let mut groups = event.key_diff.clone();
        groups.sort();
        groups.dedup();
        return Cause::OwnInputs(groups);
    }
    if event.root.is_empty() {
        // "Here" has no identity; two unknown-root events with one crate
        // name may be unrelated workspaces.
        return Cause::Unexplained;
    }
    let seen_before = first_compile
        .get(&(event.root.as_str(), event.crate_name.as_str()))
        .is_some_and(|&first| first < index);
    if seen_before {
        Cause::Unexplained
    } else {
        Cause::NoHistory
    }
}

/// Crates in `root` that missed in at least three of its builds, or in two
/// with a store failure on the latest lookup, most often first. Only
/// lookups count as a build "seeing" a crate: a build that only ran a query
/// never tried the cache. Capped so the panel stays a list, not a log.
fn chronic_misses(events: &[BuildEvent], sessions: &[Session], root: &str) -> Vec<Chronic> {
    if root.is_empty() {
        return Vec::new();
    }
    struct Seen {
        seen: usize,
        missed: usize,
    }
    let mut by_crate: BTreeMap<&str, Seen> = BTreeMap::new();
    for session in sessions {
        if session.root != root {
            continue;
        }
        let mut looked_up: HashSet<&str> = HashSet::new();
        let mut missed: HashSet<&str> = HashSet::new();
        for &index in &session.events {
            let event = &events[index];
            if !is_lookup(event) {
                continue;
            }
            looked_up.insert(event.crate_name.as_str());
            if is_miss(event) {
                missed.insert(event.crate_name.as_str());
            }
        }
        for name in looked_up {
            let entry = by_crate.entry(name).or_insert(Seen { seen: 0, missed: 0 });
            entry.seen += 1;
            if missed.contains(name) {
                entry.missed += 1;
            }
        }
    }
    // The newest lookup per crate, by log position: sessions are sorted
    // running-first, which is not chronological, so walk the log instead
    // and let the last write win.
    let mut latest_failed: HashMap<&str, bool> = HashMap::new();
    for event in events {
        if event.root == root && is_lookup(event) {
            latest_failed.insert(
                event.crate_name.as_str(),
                is_miss(event) && !event.store_error.is_empty(),
            );
        }
    }
    let mut chronic: Vec<Chronic> = by_crate
        .into_iter()
        .map(|(name, seen)| Chronic {
            crate_name: name.to_string(),
            missed: seen.missed,
            seen: seen.seen,
            last_store_failed: latest_failed.get(name).copied().unwrap_or(false),
        })
        .filter(|c| c.missed >= 3 || (c.missed >= 2 && c.last_store_failed))
        .collect();
    chronic.sort_by(|a, b| {
        b.last_store_failed
            .cmp(&a.last_store_failed)
            .then_with(|| b.missed.cmp(&a.missed))
            .then_with(|| a.crate_name.cmp(&b.crate_name))
    });
    chronic.truncate(8);
    chronic
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
        root: &str,
        session: &str,
    ) -> BuildEvent {
        let mut e = BuildEvent::new_for_test(crate_name, result);
        e.ts = ts(at);
        e.root = root.to_string();
        e.session_id = session.to_string();
        e.elapsed_ms = 100;
        e.compile_time_ms = match result {
            EventResult::Miss | EventResult::Dup => 1_000,
            EventResult::LocalHit | EventResult::PrefetchHit | EventResult::RemoteHit => 5_000,
            _ => 0,
        };
        e
    }

    fn no_live() -> HashSet<String> {
        HashSet::new()
    }

    fn gap() -> Duration {
        Duration::from_secs(300)
    }

    #[test]
    fn state_labels_are_the_words_on_screen() {
        assert_eq!(SessionState::Live.label(), "running");
        assert_eq!(SessionState::Finished.label(), "done");
    }

    #[test]
    fn recorded_ids_group_and_running_sessions_sort_first() {
        let events = vec![
            event("a", EventResult::Miss, 0, "/w/one", "s1"),
            event("b", EventResult::LocalHit, 1, "/w/one", "s1"),
            event("c", EventResult::Miss, 500, "/w/two", "s2"),
            event("d", EventResult::Passthrough, 501, "/w/two", "s2"),
        ];
        // s1 is old and idle; s2 finished 100s ago but its root is compiling.
        let live: HashSet<String> = ["/w/two".to_string()].into_iter().collect();
        let sessions = group_sessions(&events, ts(601), &live, gap());
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].key, "id:s2");
        assert_eq!(sessions[0].recorded_id(), Some("s2"));
        assert_eq!(sessions[0].state, SessionState::Live);
        assert_eq!(sessions[0].workspace_name(), "two");
        assert_eq!(sessions[0].tally.passthroughs, 1);
        assert_eq!(sessions[1].key, "id:s1");
        assert_eq!(sessions[1].state, SessionState::Finished);
        assert_eq!(sessions[1].tally.hits, 1);
        assert_eq!(sessions[1].tally.misses, 1);
        assert_eq!(sessions[1].tally.saved_ms, 5_000);
        assert_eq!(sessions[1].tally.miss_ms, 1_000);
        assert_eq!(sessions[1].tally.hit_rate(), Some(50.0));
        assert_eq!(sessions[1].events, vec![0, 1]);
    }

    /// A root names a tree, not a build: activity there keeps only the
    /// tree's newest session live, never an older build of the same tree.
    #[test]
    fn a_live_root_revives_only_its_newest_session() {
        let events = vec![
            event("a", EventResult::Miss, 0, "/w", "old"),
            event("b", EventResult::Miss, 5_000, "/w", "new"),
        ];
        let live: HashSet<String> = ["/w".to_string()].into_iter().collect();
        let sessions = group_sessions(&events, ts(10_000), &live, gap());
        assert_eq!(sessions[0].key, "id:new");
        assert_eq!(sessions[0].state, SessionState::Live);
        assert_eq!(sessions[1].key, "id:old");
        assert_eq!(sessions[1].state, SessionState::Finished);
    }

    #[test]
    fn recent_activity_alone_keeps_a_session_live_but_the_future_does_not() {
        let events = vec![event("a", EventResult::Miss, 0, "/w", "s1")];
        let sessions = group_sessions(&events, ts(10), &no_live(), gap());
        assert_eq!(sessions[0].state, SessionState::Live);
        let sessions = group_sessions(&events, ts(30), &no_live(), gap());
        assert_eq!(
            sessions[0].state,
            SessionState::Finished,
            "the grace is exclusive"
        );
        // Stamped 10s in the future: a clock problem, not a running build.
        let sessions = group_sessions(&events, ts(-10), &no_live(), gap());
        assert_eq!(sessions[0].state, SessionState::Finished);
    }

    #[test]
    fn events_without_ids_split_by_idle_gap_per_root() {
        let events = vec![
            event("a", EventResult::Miss, 0, "/w", ""),
            event("b", EventResult::Miss, 100, "/w", ""),
            event("x", EventResult::Miss, 150, "/other", ""),
            // 299s after b: still the same build.
            event("b2", EventResult::Miss, 399, "/w", ""),
            // 300s after b2: a new build of /w (the gap is "at least").
            event("c", EventResult::LocalHit, 699, "/w", ""),
            event("d", EventResult::LocalHit, 700, "/w", ""),
        ];
        let sessions = group_sessions(&events, ts(10_000), &no_live(), gap());
        assert_eq!(sessions.len(), 3);
        assert!(sessions.iter().all(|s| s.inferred));
        assert!(sessions.iter().all(|s| s.recorded_id().is_none()));
        let keys: Vec<&str> = sessions.iter().map(|s| s.key.as_str()).collect();
        assert_eq!(
            keys.len(),
            keys.iter().collect::<HashSet<_>>().len(),
            "keys are unique"
        );
        assert_eq!(sessions[0].events, vec![4, 5], "newest first");
        assert_eq!(
            sessions[0].started,
            ts(699) - chrono::Duration::milliseconds(100)
        );
        assert_eq!(sessions[1].events, vec![0, 1, 3], "last active at 399s");
        assert_eq!(sessions[2].events, vec![2], "last active at 150s");
    }

    #[test]
    fn keys_are_stable_as_the_log_grows_and_ties_follow_log_order() {
        let mut events = vec![
            event("a", EventResult::Miss, 0, "/w", ""),
            event("x", EventResult::Miss, 1, "/other", ""),
        ];
        let before: Vec<String> = group_sessions(&events, ts(2), &no_live(), gap())
            .into_iter()
            .map(|s| s.key)
            .collect();
        events.push(event("b", EventResult::Miss, 2, "/w", ""));
        events.push(event("c", EventResult::Miss, 900, "/w", ""));
        let after: Vec<String> = group_sessions(&events, ts(901), &no_live(), gap())
            .into_iter()
            .map(|s| s.key)
            .collect();
        for key in &before {
            assert!(after.contains(key), "{key} vanished: {after:?}");
        }
        assert_eq!(after.len(), 3);

        // Same timestamp, two sessions: the one that logged last is newest.
        let events = vec![
            event("a", EventResult::Miss, 5, "/w", "zzz"),
            event("b", EventResult::Miss, 5, "/w", "aaa"),
        ];
        let sessions = group_sessions(&events, ts(10_000), &no_live(), gap());
        assert_eq!(sessions[0].key, "id:aaa");
    }

    /// Regrouping only when the log grew, and refreshing state every tick,
    /// gives the same answer as grouping from scratch.
    #[test]
    fn refresh_state_matches_a_fresh_grouping() {
        let events = vec![
            event("a", EventResult::Miss, 0, "/w", "s1"),
            event("b", EventResult::Miss, 100, "/v", "s2"),
        ];
        let mut cached = group(&events, gap());
        refresh_state(&mut cached, ts(101), &no_live());
        assert_eq!(cached, group_sessions(&events, ts(101), &no_live(), gap()));
        // Later, s1's root is compiling: it moves back to the top.
        let live: HashSet<String> = ["/w".to_string()].into_iter().collect();
        refresh_state(&mut cached, ts(5_000), &live);
        assert_eq!(cached, group_sessions(&events, ts(5_000), &live, gap()));
        assert_eq!(cached[0].key, "id:s1");
    }

    #[test]
    fn tally_separates_probes_rates_only_lookups_and_saturates() {
        let mut t = Tally::default();
        let mut probe = event("rustc", EventResult::Passthrough, 0, "/w", "s");
        probe.passthrough_reason = "not-a-compile|--print cfg".to_string();
        let mut pass = event("cc", EventResult::Passthrough, 0, "/w", "s");
        pass.passthrough_reason = "unsupported|cc flag -march".to_string();
        t.add(&probe);
        t.add(&pass);
        assert_eq!((t.probes, t.passthroughs), (1, 1));
        assert_eq!(t.hit_rate(), None, "no lookups, no rate");
        assert_eq!(t.overhead_ms, 0, "passthroughs are not overhead");
        assert_eq!(t.overhead_share(), None);
        let mut hit = event("a", EventResult::LocalHit, 0, "/w", "s");
        hit.reflinked_bytes = 900;
        hit.copied_bytes = 100;
        t.add(&hit);
        t.add(&event("b", EventResult::Dup, 0, "/w", "s"));
        assert_eq!(t.hit_rate(), Some(50.0));
        assert_eq!(t.copy_share(), Some(10.0));
        assert_eq!(t.restored_bytes(), 1000);
        // A hit's whole elapsed time is overhead; the dup's 100ms elapsed
        // minus 1000ms compile clamps to zero.
        assert_eq!(t.overhead_ms, 100);
        assert_eq!(t.lookup_elapsed_ms, 200);
        assert_eq!(t.overhead_share(), Some(50.0));

        let mut huge = event("z", EventResult::LocalHit, 0, "/w", "s");
        huge.compile_time_ms = u64::MAX;
        t.add(&huge);
        t.add(&huge);
        assert_eq!(t.saved_ms, u64::MAX, "saturated, not wrapped");
        assert_eq!(
            Tally::default().copy_share(),
            None,
            "nothing restored, no share"
        );
    }

    fn analyze_one(events: &[BuildEvent]) -> Analysis {
        let sessions = group_sessions(events, ts(1_000_000), &no_live(), gap());
        let selected = sessions.iter().find(|s| s.key == "id:now").unwrap();
        analyze_session(events, selected, &sessions)
    }

    #[test]
    fn causes_follow_why_miss_precedence() {
        let mut store_failed = event("sf", EventResult::Miss, 100, "/w", "now");
        store_failed.store_error = "disk full".to_string();
        store_failed.key_diff = vec!["sources".to_string()];
        let mut rejected = event("rj", EventResult::Miss, 101, "/w", "now");
        rejected.lookup_rejection = "artifact set incomplete".to_string();
        let mut own = event("own", EventResult::Miss, 102, "/w", "now");
        own.key_diff = vec![
            "sources".to_string(),
            "args".to_string(),
            "sources".to_string(),
        ];
        let mut unknown_root = event("nowhere", EventResult::Miss, 106, "", "now");
        unknown_root.root.clear();
        let events = vec![
            event("seen", EventResult::LocalHit, 0, "/w", "before"),
            // A real passthrough compile counts as history; a probe does not.
            {
                let mut e = event("passed", EventResult::Passthrough, 1, "/w", "before");
                e.passthrough_reason = "unsupported|flag".to_string();
                e
            },
            {
                let mut e = event("probed", EventResult::Passthrough, 2, "/w", "before");
                e.passthrough_reason = "not-a-compile|--print".to_string();
                e
            },
            store_failed,
            rejected,
            own,
            event("first", EventResult::Miss, 103, "/w", "now"),
            event("seen", EventResult::Miss, 104, "/w", "now"),
            event("passed", EventResult::Miss, 105, "/w", "now"),
            event("probed", EventResult::Miss, 105, "/w", "now"),
            unknown_root,
        ];
        let analysis = analyze_one(&events);
        let by_cause: BTreeMap<&Cause, usize> = analysis
            .causes
            .iter()
            .map(|g| (&g.cause, g.count))
            .collect();
        assert_eq!(by_cause[&Cause::StoreFailed("disk full".into())], 1);
        assert_eq!(
            by_cause[&Cause::LookupRejected("artifact set incomplete".into())],
            1
        );
        assert_eq!(
            by_cause[&Cause::OwnInputs(vec!["args".into(), "sources".into()])],
            1
        );
        let examples = |cause: &Cause| -> Vec<String> {
            analysis
                .causes
                .iter()
                .find(|g| &g.cause == cause)
                .map(|g| g.examples.clone())
                .unwrap_or_default()
        };
        assert_eq!(by_cause[&Cause::NoHistory], 2, "{by_cause:?}");
        assert_eq!(
            examples(&Cause::NoHistory),
            vec!["first", "probed"],
            "a probe is not a prior compile"
        );
        assert_eq!(by_cause[&Cause::Unexplained], 3, "{by_cause:?}");
        assert_eq!(
            examples(&Cause::Unexplained),
            vec!["seen", "passed", "nowhere"],
            "a real passthrough compile is; an unknown root is never a first build"
        );
        assert_eq!(analysis.misses_total, 8);
        assert!(!analysis.cascade_recorded);
        assert!(Cause::StoreFailed(String::new()).is_failure());
        assert!(!Cause::NoHistory.is_failure());
        assert_eq!(
            Cause::NoHistory.describe(),
            "no earlier compile in the loaded history"
        );
    }

    fn with_externs(mut e: BuildEvent, externs: &[(&str, &str)]) -> BuildEvent {
        e.key_externs = externs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        e.key_externs_recorded = true;
        e
    }

    fn with_fields(mut e: BuildEvent, fields: &[(&str, &str)]) -> BuildEvent {
        e.key_fields = fields
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        e
    }

    /// Forty crates downstream of one changed leaf collapse to one cause
    /// naming the leaf, and the leaf itself is its own cause.
    #[test]
    fn a_cascade_collapses_to_its_root() {
        let mut events = vec![with_fields(
            with_externs(event("leaf", EventResult::LocalHit, 0, "/w", "before"), &[]),
            &[("sources", "1111")],
        )];
        for i in 0..40 {
            events.push(with_externs(
                event(&format!("app{i}"), EventResult::LocalHit, 1, "/w", "before"),
                &[("leaf", "aaaa")],
            ));
        }
        let mut leaf_miss = with_fields(
            with_externs(event("leaf", EventResult::Miss, 100, "/w", "now"), &[]),
            &[("sources", "2222")],
        );
        leaf_miss.key_diff = vec!["sources".to_string()];
        events.push(leaf_miss);
        for i in 0..40 {
            events.push(with_externs(
                event(&format!("app{i}"), EventResult::Miss, 101, "/w", "now"),
                &[("leaf", "bbbb")],
            ));
        }
        let analysis = analyze_one(&events);
        assert!(analysis.cascade_recorded);
        assert_eq!(analysis.misses_total, 41);
        assert_eq!(analysis.causes.len(), 2, "{:?}", analysis.causes);
        assert_eq!(
            analysis.causes[0].cause,
            Cause::Downstream {
                root: "leaf".into(),
                complete: true
            }
        );
        assert_eq!(analysis.causes[0].count, 40);
        assert_eq!(analysis.causes[0].examples, vec!["app0", "app1", "app2"]);
        assert_eq!(
            analysis.causes[1].cause,
            Cause::OwnInputs(vec!["sources".into()])
        );
        assert_eq!(analysis.causes[0].cause.describe(), "downstream of leaf");
    }

    /// A miss below two changed leaves names both, not the first one the
    /// walk happened to rank higher.
    #[test]
    fn a_cascade_with_two_roots_names_both() {
        let leaf = |name: &str, result, at, session, sources: &str| {
            with_fields(
                with_externs(event(name, result, at, "/w", session), &[]),
                &[("sources", sources)],
            )
        };
        let events = vec![
            leaf("alpha", EventResult::LocalHit, 0, "before", "a1"),
            leaf("beta", EventResult::LocalHit, 1, "before", "b1"),
            with_externs(
                event("app", EventResult::LocalHit, 2, "/w", "before"),
                &[("alpha", "aaaa"), ("beta", "bbbb")],
            ),
            leaf("alpha", EventResult::Miss, 100, "now", "a2"),
            leaf("beta", EventResult::Miss, 101, "now", "b2"),
            with_externs(
                event("app", EventResult::Miss, 102, "/w", "now"),
                &[("alpha", "aaa2"), ("beta", "bbb2")],
            ),
        ];
        let analysis = analyze_one(&events);
        let downstream = analysis
            .causes
            .iter()
            .find(|g| matches!(g.cause, Cause::Downstream { .. }))
            .unwrap_or_else(|| panic!("{:?}", analysis.causes));
        assert_eq!(
            downstream.cause,
            Cause::Downstream {
                root: "alpha, beta".into(),
                complete: true
            }
        );
    }

    /// A cascade whose walk dead-ends says so rather than naming the one
    /// endpoint it reached as the whole explanation.
    #[test]
    fn an_unresolved_cascade_is_not_presented_as_complete() {
        // `dep` changed, but has no compile of its own in the window: the
        // walk reaches it and cannot explain it.
        let events = vec![
            with_externs(
                event("app", EventResult::LocalHit, 0, "/w", "before"),
                &[("dep", "aaaa")],
            ),
            with_externs(
                event("app", EventResult::Miss, 100, "/w", "now"),
                &[("dep", "bbbb")],
            ),
        ];
        let analysis = analyze_one(&events);
        assert_eq!(analysis.causes.len(), 1);
        match &analysis.causes[0].cause {
            Cause::Downstream { root, complete } => {
                assert_eq!(root, "dep");
                assert!(!complete);
            }
            other => panic!("{other:?}"),
        }
        assert!(
            analysis.causes[0]
                .cause
                .describe()
                .contains("not fully resolved")
        );
    }

    /// The recording flag describes the whole session, not the analyzed
    /// sample, so a capped analysis never tells a user to switch on what
    /// they already have.
    #[test]
    fn cascade_recorded_looks_past_the_cap() {
        let mut events = vec![with_externs(
            event("first", EventResult::Miss, 0, "/w", "now"),
            &[],
        )];
        for i in 1..=MAX_ANALYZED_MISSES {
            events.push(event(
                &format!("c{i}"),
                EventResult::Miss,
                i as i64,
                "/w",
                "now",
            ));
        }
        let analysis = analyze_one(&events);
        assert_eq!(analysis.misses_analyzed, MAX_ANALYZED_MISSES);
        assert_eq!(analysis.misses_total, MAX_ANALYZED_MISSES + 1);
        assert!(analysis.cascade_recorded);
    }

    #[test]
    fn passthroughs_rank_compiles_before_probes() {
        let mut probe = event("rustc", EventResult::Passthrough, 100, "/w", "now");
        probe.passthrough_reason = "not-a-compile|--print cfg".to_string();
        let mut cc = event("cc", EventResult::Passthrough, 101, "/w", "now");
        cc.passthrough_reason = "unsupported|cc flag -march".to_string();
        let mut legacy = event("old", EventResult::Passthrough, 102, "/w", "now");
        legacy.passthrough_reason = "refused: linker invocation".to_string();
        let events = vec![probe.clone(), probe.clone(), probe, cc, legacy];
        let analysis = analyze_one(&events);
        let listed: Vec<(&str, &str, usize, bool)> = analysis
            .passthroughs
            .iter()
            .map(|g| (g.kind.as_str(), g.reason.as_str(), g.count, g.probe))
            .collect();
        assert_eq!(
            listed,
            vec![
                ("unsupported", "cc flag -march", 1, false),
                ("", "linker invocation", 1, false),
                ("not-a-compile", "--print cfg", 3, true),
            ]
        );
        assert_eq!(passthrough_parts(""), ("", "unknown"));
    }

    /// Chronic misses are per tree, count only builds that looked the crate
    /// up, and take "latest" from log order, which session order is not.
    #[test]
    fn chronic_misses_need_repeats_across_builds_of_one_tree() {
        let mut events = Vec::new();
        for (n, session) in ["s1", "s2", "s3", "s4"].iter().enumerate() {
            let at = n as i64 * 1000;
            events.push(event("flaky", EventResult::Miss, at, "/w", session));
            events.push(event("stable", EventResult::LocalHit, at, "/w", session));
            let mut broken = event("broken", EventResult::Miss, at, "/w", session);
            if n >= 2 {
                broken.store_error = "read-only store".to_string();
            }
            events.push(broken);
            if n == 0 {
                events.push(event("once", EventResult::Miss, at, "/w", session));
            }
            // Missed twice with a failure, then recovered: not chronic.
            let mut healed = event("healed", EventResult::Miss, at, "/w", session);
            if n < 2 {
                healed.store_error = "disk full".to_string();
            } else {
                healed.result = EventResult::LocalHit;
                healed.compile_time_ms = 5_000;
            }
            events.push(healed);
            // Looked up only by probes in later builds: those builds never
            // tried the cache for it, so they do not dilute its count.
            let mut queried = event("queried", EventResult::Miss, at, "/w", session);
            if n >= 3 {
                queried.result = EventResult::Passthrough;
                queried.passthrough_reason = "not-a-compile|--print".to_string();
            }
            events.push(queried);
        }
        // Missed twice, the latest with a store failure: chronic on that
        // ground alone. Missed once with a failure: not yet.
        for session in ["s3", "s4"] {
            let mut twice = event("twice", EventResult::Miss, 8_000, "/w", session);
            twice.store_error = "disk full".to_string();
            events.push(twice);
        }
        let mut once_failed = event("once_failed", EventResult::Miss, 8_500, "/w", "s4");
        once_failed.store_error = "disk full".to_string();
        events.push(once_failed);
        // A later query for a failed crate is not a lookup: it must not
        // clear the failure flag.
        let mut queried_broken = event("broken", EventResult::Passthrough, 8_700, "/w", "s4");
        queried_broken.passthrough_reason = "not-a-compile|--print".to_string();
        events.push(queried_broken);
        // The same crate missing in three other trees is their problem.
        for (n, root) in ["/a", "/b", "/c"].iter().enumerate() {
            events.push(event(
                "elsewhere",
                EventResult::Miss,
                9_000 + n as i64,
                root,
                "x",
            ));
        }
        let sessions = group_sessions(&events, ts(1_000_000), &no_live(), gap());
        let selected = sessions.iter().find(|s| s.key == "id:s4").unwrap();
        let analysis = analyze_session(&events, selected, &sessions);
        let names: Vec<(&str, usize, usize, bool)> = analysis
            .chronic
            .iter()
            .map(|c| (c.crate_name.as_str(), c.missed, c.seen, c.last_store_failed))
            .collect();
        assert_eq!(
            names,
            vec![
                ("broken", 4, 4, true),
                ("twice", 2, 2, true),
                ("flaky", 4, 4, false),
                ("queried", 3, 3, false),
            ],
            "once, once_failed, stable, healed, and elsewhere are not chronic"
        );
        // A session with no root has nothing to compare against.
        let mut rootless = event("r", EventResult::Miss, 1, "", "nr");
        rootless.root.clear();
        let events = vec![rootless];
        let sessions = group_sessions(&events, ts(1_000_000), &no_live(), gap());
        assert!(
            analyze_session(&events, &sessions[0], &sessions)
                .chronic
                .is_empty()
        );
    }

    #[test]
    fn analysis_caps_the_walk_and_says_so() {
        let mut events = Vec::new();
        for i in 0..(MAX_ANALYZED_MISSES + 10) {
            events.push(event(
                &format!("c{i}"),
                EventResult::Miss,
                i as i64,
                "/w",
                "now",
            ));
        }
        let analysis = analyze_one(&events);
        assert_eq!(analysis.misses_total, MAX_ANALYZED_MISSES + 10);
        assert_eq!(analysis.misses_analyzed, MAX_ANALYZED_MISSES);
    }
}
