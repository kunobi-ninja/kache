//! `kache diff`: compare miss counts of the two newest sessions that share a root.

use crate::config::Config;
use crate::events::{self, BuildEvent};
use crate::tui_sessions::{self, Session};
use anyhow::Result;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

/// What comparing the two newest sessions found.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DiffOutcome {
    /// Fewer than two sessions share a root.
    NeedTwo,
    Compared(DiffBody),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DiffBody {
    pub root: String,
    pub earlier_misses: usize,
    pub later_misses: usize,
    pub earlier_unexplained_pct: u64,
    pub later_unexplained_pct: u64,
    pub excess: bool,
    pub unknown: bool,
}

impl DiffBody {
    pub(crate) fn alarmed(&self) -> bool {
        self.excess || self.unknown
    }

    pub(crate) fn text(&self) -> String {
        let mut lines = vec![
            format!("root {}", self.root),
            format!("misses {} -> {}", self.earlier_misses, self.later_misses),
            format!(
                "unexplained {}% -> {}%",
                self.earlier_unexplained_pct, self.later_unexplained_pct
            ),
        ];
        lines.extend(alarm_lines(self));
        lines.join("\n")
    }
}

fn alarm_lines(body: &DiffBody) -> Vec<String> {
    let mut lines = Vec::new();
    if body.excess {
        lines.push(format!(
            "EXCESS later misses {} earlier {}",
            body.later_misses, body.earlier_misses
        ));
    }
    if body.unknown {
        lines.push(format!(
            "UNKNOWN later unexplained {}% earlier {}%",
            body.later_unexplained_pct, body.earlier_unexplained_pct
        ));
    }
    lines
}

pub(crate) fn compare(events: &[BuildEvent], root: Option<&str>) -> DiffOutcome {
    let sessions = tui_sessions::group(
        events,
        Duration::from_secs(crate::wrapper::BUILD_SESSION_SECS),
    );
    let Some((earlier, later)) = newest_pair(&sessions, root) else {
        return DiffOutcome::NeedTwo;
    };
    let (earlier_misses, earlier_pct) = share(events, earlier);
    let (later_misses, later_pct) = share(events, later);
    let excess = later_misses > earlier_misses;
    let unknown = later_pct > earlier_pct;
    DiffOutcome::Compared(DiffBody {
        root: later.root.clone(),
        earlier_misses,
        later_misses,
        earlier_unexplained_pct: earlier_pct,
        later_unexplained_pct: later_pct,
        excess,
        unknown,
    })
}

fn share(events: &[BuildEvent], session: &Session) -> (usize, u64) {
    let misses = tui_sessions::miss_count(events, &session.events);
    let unexplained = tui_sessions::unexplained_misses(events, &session.events);
    (misses, percent(unexplained, misses))
}

fn percent(part: usize, whole: usize) -> u64 {
    if whole == 0 {
        0
    } else {
        (part as u64).saturating_mul(100) / (whole as u64)
    }
}

fn newest_pair<'a>(
    sessions: &'a [Session],
    root: Option<&str>,
) -> Option<(&'a Session, &'a Session)> {
    let mut by_root: BTreeMap<&str, Vec<&Session>> = BTreeMap::new();
    for session in sessions {
        if root_selected(session, root) {
            by_root
                .entry(session.root.as_str())
                .or_default()
                .push(session);
        }
    }
    by_root
        .into_values()
        .filter(|group| group.len() >= 2)
        .map(|mut group| {
            group.sort_by(|left, right| {
                right.last_activity.cmp(&left.last_activity).then_with(|| {
                    right
                        .events
                        .last()
                        .copied()
                        .unwrap_or(0)
                        .cmp(&left.events.last().copied().unwrap_or(0))
                })
            });
            (group[1], group[0])
        })
        .max_by_key(|(_, later)| {
            (
                later.last_activity,
                later.events.last().copied().unwrap_or(0),
            )
        })
}

fn root_selected(session: &Session, root: Option<&str>) -> bool {
    match root {
        None => true,
        Some(root) => session.root == root,
    }
}

const NEED_TWO: &str = "need two sessions to compare";

/// Print the comparison. `Ok(true)` means the later session has more misses
/// or a larger unexplained share; the caller exits 1. Fewer than two
/// sessions prints a line and returns `Ok(false)`.
pub fn run(config: &Config, root: Option<&Path>, json: bool) -> Result<bool> {
    let events = events::read_events(&config.event_log_path())?;
    let root = root.map(crate::report::normalize_filter_root);
    match compare(&events, root.as_deref()) {
        DiffOutcome::NeedTwo => {
            if json {
                #[derive(serde::Serialize)]
                struct Body {
                    message: &'static str,
                }
                crate::machine::emit("diff", Body { message: NEED_TWO }, Vec::new())?;
            } else {
                println!("{NEED_TWO}");
            }
            Ok(false)
        }
        DiffOutcome::Compared(body) => {
            let alarmed = body.alarmed();
            if json {
                #[derive(serde::Serialize)]
                struct Body {
                    root: String,
                    earlier_misses: usize,
                    later_misses: usize,
                    earlier_unexplained_pct: u64,
                    later_unexplained_pct: u64,
                    excess: bool,
                    unknown: bool,
                    lines: Vec<String>,
                }
                let lines = alarm_lines(&body);
                crate::machine::emit(
                    "diff",
                    Body {
                        root: body.root.clone(),
                        earlier_misses: body.earlier_misses,
                        later_misses: body.later_misses,
                        earlier_unexplained_pct: body.earlier_unexplained_pct,
                        later_unexplained_pct: body.later_unexplained_pct,
                        excess: body.excess,
                        unknown: body.unknown,
                        lines,
                    },
                    Vec::new(),
                )?;
            } else {
                println!("{}", body.text());
            }
            Ok(alarmed)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::EventResult;
    use chrono::{DateTime, Utc};

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000 + secs, 0).unwrap()
    }

    fn ev(
        crate_name: &str,
        result: EventResult,
        root: &str,
        session: &str,
        secs: i64,
    ) -> BuildEvent {
        let mut event = BuildEvent::new_for_test(crate_name, result);
        event.root = root.to_string();
        event.session_id = session.to_string();
        event.ts = at(secs);
        event
    }

    fn compared(events: &[BuildEvent], root: Option<&str>) -> DiffBody {
        match compare(events, root) {
            DiffOutcome::Compared(body) => body,
            DiffOutcome::NeedTwo => panic!("expected two sessions"),
        }
    }

    #[test]
    fn one_session_needs_another() {
        let events = vec![ev("a", EventResult::Miss, "/w", "s1", 1)];
        assert_eq!(compare(&events, None), DiffOutcome::NeedTwo);
        assert_eq!(compare(&events, Some("/other")), DiffOutcome::NeedTwo);
    }

    #[test]
    fn matching_sessions_are_quiet() {
        // Different crates, so neither miss is a repeat. Equal counts and a
        // zero unexplained share on both sides stay quiet.
        let events = vec![
            ev("a", EventResult::Miss, "/w", "s1", 1),
            ev("b", EventResult::Miss, "/w", "s2", 2),
        ];
        let body = compared(&events, None);
        assert!(!body.excess);
        assert!(!body.unknown);
        assert!(!body.alarmed());
        assert_eq!(body.earlier_misses, 1);
        assert_eq!(body.later_misses, 1);
        assert_eq!(body.earlier_unexplained_pct, 0);
        assert_eq!(body.later_unexplained_pct, 0);
        assert!(!body.text().contains("EXCESS"));
        assert!(!body.text().contains("UNKNOWN"));
    }

    #[test]
    fn a_later_session_with_more_misses_is_excess() {
        let events = vec![
            ev("a", EventResult::Miss, "/w", "s1", 1),
            ev("a", EventResult::Miss, "/w", "s2", 2),
            ev("b", EventResult::Miss, "/w", "s2", 3),
        ];
        let body = compared(&events, Some("/w"));
        assert!(body.excess);
        assert!(body.alarmed());
        assert!(body.text().contains("EXCESS later misses 2 earlier 1"));
        assert_eq!(body.earlier_unexplained_pct, 0);
        assert_eq!(body.later_unexplained_pct, 50);
    }

    #[test]
    fn a_higher_unexplained_share_is_unknown_without_more_misses() {
        // s1: first compile of `a` (no history) and a repeat of nothing else.
        // s2: the same crate misses again, which is unexplained, and a fresh
        // crate, which is not. Two misses each. Later share is 1/2 = 50,
        // earlier share is 0.
        let events = vec![
            ev("a", EventResult::Miss, "/w", "s1", 1),
            ev("b", EventResult::Miss, "/w", "s1", 2),
            ev("a", EventResult::Miss, "/w", "s2", 3),
            ev("c", EventResult::Miss, "/w", "s2", 4),
        ];
        let body = compared(&events, None);
        assert!(!body.excess);
        assert!(body.unknown);
        assert!(body.alarmed());
        assert_eq!(body.later_misses, body.earlier_misses);
        assert_eq!(body.earlier_unexplained_pct, 0);
        assert_eq!(body.later_unexplained_pct, 50);
        assert!(
            body.text()
                .contains("UNKNOWN later unexplained 50% earlier 0%")
        );
        assert!(!body.text().contains("EXCESS"));
    }

    #[test]
    fn an_empty_miss_set_is_zero_percent() {
        // The later miss is a crate this log has never compiled, so the
        // extra miss is excess and the unexplained share stays 0.
        let events = vec![
            ev("a", EventResult::LocalHit, "/w", "s1", 1),
            ev("b", EventResult::Miss, "/w", "s2", 2),
        ];
        let body = compared(&events, None);
        assert_eq!(body.earlier_misses, 0);
        assert_eq!(body.earlier_unexplained_pct, 0);
        assert_eq!(body.later_unexplained_pct, 0);
        assert!(body.excess);
        assert!(!body.unknown);
        assert!(body.alarmed());
    }

    #[test]
    fn one_unexplained_miss_in_four_is_25_percent() {
        assert_eq!(percent(1, 4), 25);
        assert_eq!(percent(2, 4), 50);
        assert_eq!(percent(0, 0), 0);
        assert_eq!(percent(3, 0), 0);
    }

    #[test]
    fn the_root_flag_ignores_the_other_tree() {
        let events = vec![
            ev("a", EventResult::Miss, "/w", "s1", 1),
            ev("a", EventResult::Miss, "/w", "s2", 2),
            ev("a", EventResult::Miss, "/other", "t1", 3),
            ev("b", EventResult::Miss, "/other", "t1", 4),
            ev("c", EventResult::Miss, "/other", "t1", 5),
            ev("a", EventResult::Miss, "/other", "t2", 9),
        ];
        let w = compared(&events, Some("/w"));
        assert_eq!(w.root, "/w");
        assert_eq!(w.later_misses, 1);
        let other = compared(&events, Some("/other"));
        assert_eq!(other.later_misses, 1);
        assert_eq!(other.earlier_misses, 3);
        assert!(!other.excess);
    }

    #[test]
    fn without_a_root_the_newest_pair_wins() {
        let events = vec![
            ev("a", EventResult::Miss, "/old", "o1", 1),
            ev("a", EventResult::Miss, "/old", "o2", 2),
            ev("a", EventResult::Miss, "/new", "n1", 3),
            ev("a", EventResult::Miss, "/new", "n2", 9),
        ];
        let body = compared(&events, None);
        assert_eq!(body.root, "/new");
    }

    #[test]
    fn three_sessions_compare_the_two_newest() {
        let events = vec![
            ev("a", EventResult::Miss, "/w", "s1", 1),
            ev("b", EventResult::Miss, "/w", "s1", 2),
            ev("c", EventResult::Miss, "/w", "s2", 3),
            ev("d", EventResult::Miss, "/w", "s3", 4),
        ];
        let body = compared(&events, None);
        assert_eq!(body.earlier_misses, 1);
        assert_eq!(body.later_misses, 1);
        assert!(!body.excess);
    }

    fn logged(events: &[BuildEvent]) -> (tempfile::TempDir, crate::config::Config) {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(dir.path().to_path_buf());
        for event in events {
            events::log_event(&config.event_log_path(), event).unwrap();
        }
        (dir, config)
    }

    #[test]
    fn run_reports_an_alarm_when_the_later_session_misses_more() {
        let (_dir, config) = logged(&[
            ev("a", EventResult::Miss, "/w", "s1", 1),
            ev("b", EventResult::Miss, "/w", "s2", 2),
            ev("c", EventResult::Miss, "/w", "s2", 3),
        ]);
        assert!(run(&config, None, false).unwrap());
    }

    #[test]
    fn run_is_quiet_when_the_sessions_match() {
        let (_dir, config) = logged(&[
            ev("a", EventResult::Miss, "/w", "s1", 1),
            ev("b", EventResult::Miss, "/w", "s2", 2),
        ]);
        assert!(!run(&config, None, false).unwrap());
    }
}
