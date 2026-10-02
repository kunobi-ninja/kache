//! `kache explain` with no crate: why the latest build missed, cause by
//! cause, costliest first, with what would remove each cause.

use crate::config::Config;
use crate::events::{self, BuildEvent};
use crate::term::{self, Align};
use crate::tui_sessions::{self, Analysis, Cause, Session};
use anyhow::Result;
use std::path::Path;
use std::time::Duration;

/// The latest build of a tree, and the one before it when there is one.
pub(crate) struct Builds<'a> {
    pub latest: &'a Session,
    pub earlier: Option<&'a Session>,
}

/// The newest session, limited to `root` when given, and the newest earlier
/// session of the same root.
pub(crate) fn latest_builds<'a>(sessions: &'a [Session], root: Option<&str>) -> Option<Builds<'a>> {
    let selected = |session: &&Session| root.is_none_or(|root| session.root == root);
    let latest = sessions.iter().filter(selected).max_by_key(|session| {
        (
            session.last_activity,
            session.events.last().copied().unwrap_or(0),
        )
    })?;
    let earlier = sessions
        .iter()
        .filter(|session| session.root == latest.root && session.key != latest.key)
        .filter(|session| session.last_activity <= latest.last_activity)
        .max_by_key(|session| {
            (
                session.last_activity,
                session.events.last().copied().unwrap_or(0),
            )
        });
    Some(Builds { latest, earlier })
}

/// What would remove a cause, in one line.
pub(crate) fn fix(cause: &Cause, max_size: u64) -> String {
    match cause {
        Cause::StoreFailed(_) => {
            "the result could not be written; check free space and run `kache doctor`".to_string()
        }
        Cause::LookupRejected(_) => {
            "an entry with this key was refused; `kache doctor --verify` checks the store"
                .to_string()
        }
        Cause::Evicted => format!(
            "the entry was evicted or never reached this machine; a cache larger than {} keeps more",
            term::bytes(max_size)
        ),
        Cause::Downstream { root, .. } => format!(
            "every crate built on {root} recompiled after it changed; an edit low in the dependency graph rebuilds the most"
        ),
        Cause::OwnInputs(groups) => {
            let changed: Vec<&str> = groups.iter().map(|group| input_words(group)).collect();
            format!(
                "{}; `kache explain <crate>` names the exact input",
                changed.join(", ")
            )
        }
        Cause::NoHistory => {
            "first build of these crates in this tree; the next build can reuse them".to_string()
        }
        Cause::Unexplained => {
            "nothing recorded why; `[cache] explain_miss = true` records it from the next build"
                .to_string()
        }
    }
}

/// A cache-key input group in words.
fn input_words(group: &str) -> &str {
    match group {
        "sources" => "its source files changed",
        "args" => "compiler flags, profile, features or target changed",
        "env_deps" => "an environment variable it reads changed",
        "env_cfg" => "an environment variable in its build configuration changed",
        "externs" => "a dependency it links against changed",
        "link" => "its linker or link inputs changed",
        "remap" => "its path remapping changed",
        "crate" => "its name, edition or crate type changed",
        other => other,
    }
}

/// The `kache explain` report for the latest build. Pure.
pub(crate) fn render(
    builds: &Builds,
    analysis: &Analysis,
    events: &[BuildEvent],
    max_size: u64,
) -> Vec<String> {
    let latest = builds.latest;
    let mut lines = vec![format!(
        "kache explain · latest build of {}",
        term::home_path(Path::new(&latest.root))
    )];
    lines.push(String::new());

    let misses = analysis.misses_total;
    let unexplained = tui_sessions::unexplained_misses(events, &latest.events);
    let mut rows = vec![(
        "Misses",
        term::count(misses as u64),
        builds.earlier.map_or_else(
            || "no earlier build of this tree".to_string(),
            |earlier| {
                format!(
                    "{} in the build before",
                    term::count(tui_sessions::miss_count(events, &earlier.events) as u64)
                )
            },
        ),
    )];
    if misses > 0 {
        rows.push((
            "Unexplained",
            term::percent(percent(unexplained, misses)),
            String::new(),
        ));
    }
    lines.extend(term::sections(&[rows]));

    let mut causes = analysis.causes.clone();
    causes.sort_by(|a, b| {
        b.compile_ms
            .cmp(&a.compile_ms)
            .then_with(|| b.count.cmp(&a.count))
            .then_with(|| a.cause.cmp(&b.cause))
    });
    if !causes.is_empty() {
        let body: Vec<Vec<String>> = causes
            .iter()
            .map(|group| {
                vec![
                    term::duration_ms(group.compile_ms),
                    term::count(group.count as u64),
                    group.cause.describe(),
                    group.examples.join(", "),
                ]
            })
            .collect();
        lines.push(String::new());
        lines.push("Why it missed, costliest first".to_string());
        lines.extend(term::table(
            &["COMPILE", "COUNT", "WHY", "CRATES"],
            &[Align::Right, Align::Right, Align::Left, Align::Left],
            &body,
        ));
        lines.push(String::new());
        lines.push("What would help".to_string());
        let mut seen = Vec::new();
        for group in &causes {
            let fix = fix(&group.cause, max_size);
            if !seen.contains(&fix) {
                lines.push(format!("  - {}: {fix}", group.cause.describe()));
                seen.push(fix);
            }
        }
    }
    if analysis.misses_analyzed < analysis.misses_total {
        lines.push(format!(
            "  The newest {} of {} misses are shown.",
            term::count(analysis.misses_analyzed as u64),
            term::count(analysis.misses_total as u64)
        ));
    }

    if !analysis.chronic.is_empty() {
        lines.push(String::new());
        lines.push("Keeps missing".to_string());
        for chronic in &analysis.chronic {
            lines.push(format!(
                "  {}   missed in {} of {} builds{}",
                chronic.crate_name,
                chronic.missed,
                chronic.seen,
                if chronic.last_store_failed {
                    ", and its last result failed to store"
                } else {
                    ""
                }
            ));
        }
    }

    let (probes, real): (Vec<_>, Vec<_>) =
        analysis.passthroughs.iter().partition(|group| group.probe);
    if !real.is_empty() || !probes.is_empty() {
        let mut body: Vec<Vec<String>> = real
            .iter()
            .map(|group| {
                vec![
                    term::count(group.count as u64),
                    if group.kind.is_empty() {
                        group.reason.clone()
                    } else {
                        format!("{}: {}", group.kind, group.reason)
                    },
                ]
            })
            .collect();
        let probe_count: usize = probes.iter().map(|group| group.count).sum();
        if probe_count > 0 {
            body.push(vec![
                term::count(probe_count as u64),
                "compiler queries; nothing to cache, expected".to_string(),
            ]);
        }
        lines.push(String::new());
        lines.push("Not cached".to_string());
        lines.extend(term::table(
            &["COUNT", "WHY"],
            &[Align::Right, Align::Left],
            &body,
        ));
    }

    lines.push(String::new());
    let costliest = causes
        .first()
        .and_then(|group| group.examples.first())
        .map(|name| format!("`kache explain {name}` for the costliest miss; "))
        .unwrap_or_default();
    lines.push(format!(
        "{costliest}the build's timeline: `kache stats --format trace -o trace.json`"
    ));
    lines
}

fn percent(part: usize, whole: usize) -> f64 {
    if whole == 0 {
        0.0
    } else {
        part as f64 / whole as f64 * 100.0
    }
}

/// `kache explain` with no crate. Always succeeds when the log reads; a
/// script that wants a failing exit when misses grow uses `kache diff`.
pub fn run(config: &Config, root: Option<&Path>, json: bool) -> Result<()> {
    let events = events::read_events(&config.event_log_path())?;
    let root = root.map(crate::report::normalize_filter_root);
    let sessions = tui_sessions::group(
        &events,
        Duration::from_secs(crate::wrapper::BUILD_SESSION_SECS),
    );
    let Some(builds) = latest_builds(&sessions, root.as_deref()) else {
        if json {
            #[derive(serde::Serialize)]
            struct Body {
                message: &'static str,
            }
            return crate::machine::emit("explain", Body { message: NO_BUILD }, Vec::new());
        }
        println!("{NO_BUILD}");
        return Ok(());
    };
    let analysis = tui_sessions::analyze_session(&events, builds.latest, &sessions);
    if json {
        let body = json_body(&builds, &analysis, &events, config.max_size);
        return crate::machine::emit("explain", body, Vec::new());
    }
    for line in render(&builds, &analysis, &events, config.max_size) {
        println!("{line}");
    }
    Ok(())
}

const NO_BUILD: &str = "no recorded build to explain";

/// One cause in `kache --json explain`.
#[derive(Debug, serde::Serialize)]
pub(crate) struct CauseBody {
    pub why: String,
    pub fix: String,
    pub failure: bool,
    pub count: usize,
    pub compile_ms: u64,
    pub crates: Vec<String>,
}

/// One reason a compilation was not cached.
#[derive(Debug, serde::Serialize)]
pub(crate) struct NotCached {
    pub kind: String,
    pub reason: String,
    pub count: usize,
    pub expected: bool,
}

/// A crate that keeps missing across builds of the tree.
#[derive(Debug, serde::Serialize)]
pub(crate) struct KeepsMissing {
    pub crate_name: String,
    pub missed: usize,
    pub seen: usize,
}

/// The body of `kache --json explain` with no crate.
#[derive(Debug, serde::Serialize)]
pub(crate) struct Body {
    pub root: String,
    pub misses: usize,
    pub earlier_misses: Option<usize>,
    pub unexplained: usize,
    pub causes: Vec<CauseBody>,
    pub not_cached: Vec<NotCached>,
    pub keeps_missing: Vec<KeepsMissing>,
}

/// The JSON report for the latest build, causes costliest first. Pure.
pub(crate) fn json_body(
    builds: &Builds,
    analysis: &Analysis,
    events: &[BuildEvent],
    max_size: u64,
) -> Body {
    let mut causes = analysis.causes.clone();
    causes.sort_by(|a, b| {
        b.compile_ms
            .cmp(&a.compile_ms)
            .then_with(|| a.cause.cmp(&b.cause))
    });
    Body {
        root: builds.latest.root.clone(),
        misses: analysis.misses_total,
        earlier_misses: builds
            .earlier
            .map(|earlier| tui_sessions::miss_count(events, &earlier.events)),
        unexplained: tui_sessions::unexplained_misses(events, &builds.latest.events),
        causes: causes
            .iter()
            .map(|group| CauseBody {
                why: group.cause.describe(),
                fix: fix(&group.cause, max_size),
                failure: group.cause.is_failure(),
                count: group.count,
                compile_ms: group.compile_ms,
                crates: group.examples.clone(),
            })
            .collect(),
        not_cached: analysis
            .passthroughs
            .iter()
            .map(|group| NotCached {
                kind: group.kind.clone(),
                reason: group.reason.clone(),
                count: group.count,
                expected: group.probe,
            })
            .collect(),
        keeps_missing: analysis
            .chronic
            .iter()
            .map(|chronic| KeepsMissing {
                crate_name: chronic.crate_name.clone(),
                missed: chronic.missed,
                seen: chronic.seen,
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::EventResult;

    fn ev(name: &str, result: EventResult, session: &str, secs: i64, key: &str) -> BuildEvent {
        let mut event = BuildEvent::new_for_test(name, result);
        event.root = "/w".to_string();
        event.session_id = session.to_string();
        event.ts = chrono::DateTime::from_timestamp(1_700_000_000 + secs, 0).unwrap();
        event.cache_key = key.to_string();
        event.compile_time_ms = 1_000;
        event
    }

    fn sessions(events: &[BuildEvent]) -> Vec<Session> {
        tui_sessions::group(events, Duration::from_secs(300))
    }

    #[test]
    fn the_latest_build_and_the_one_before_come_from_one_tree() {
        let mut other = ev("z", EventResult::Miss, "o1", 50, "zk");
        other.root = "/other".to_string();
        let events = vec![
            ev("a", EventResult::Miss, "s1", 1, "k1"),
            ev("a", EventResult::LocalHit, "s2", 20, "k1"),
            other,
        ];
        let all = sessions(&events);
        let builds = latest_builds(&all, None).unwrap();
        assert_eq!(builds.latest.root, "/other", "newest anywhere");
        assert!(builds.earlier.is_none(), "no earlier build of /other");
        let builds = latest_builds(&all, Some("/w")).unwrap();
        assert_eq!(builds.latest.key, "id:s2");
        assert_eq!(builds.earlier.unwrap().key, "id:s1");
        assert!(latest_builds(&all, Some("/none")).is_none());
        assert!(latest_builds(&[], None).is_none());
    }

    #[test]
    fn every_cause_says_what_would_help() {
        let max = 5 << 30;
        assert!(fix(&Cause::StoreFailed("x".into()), max).contains("kache doctor"));
        assert!(fix(&Cause::LookupRejected("x".into()), max).contains("--verify"));
        assert!(fix(&Cause::Evicted, max).contains("larger than 5.0 GiB"));
        assert!(
            fix(
                &Cause::Downstream {
                    root: "serde".into(),
                    complete: true
                },
                max
            )
            .starts_with("every crate built on serde")
        );
        assert_eq!(
            fix(
                &Cause::OwnInputs(vec!["sources".into(), "env_deps".into()]),
                max
            ),
            "its source files changed, an environment variable it reads changed; `kache explain <crate>` names the exact input"
        );
        assert!(fix(&Cause::NoHistory, max).starts_with("first build"));
        assert!(fix(&Cause::Unexplained, max).contains("explain_miss"));
        for (group, words) in [
            ("args", "flags"),
            ("env_cfg", "build configuration"),
            ("externs", "dependency"),
            ("link", "linker"),
            ("remap", "remapping"),
            ("crate", "edition"),
        ] {
            assert!(input_words(group).contains(words), "{group}");
        }
        assert_eq!(input_words("outcome_lints"), "outcome_lints");
    }

    #[test]
    fn the_report_ranks_causes_by_compile_time_and_names_the_next_step() {
        let mut changed = ev("cheap", EventResult::Miss, "s2", 21, "c2");
        changed.key_diff = vec!["sources".to_string()];
        changed.compile_time_ms = 100;
        let mut evicted = ev("costly", EventResult::Miss, "s2", 22, "k1");
        evicted.compile_time_ms = 9_000;
        let mut probe = ev("probe", EventResult::Passthrough, "s2", 23, "");
        probe.passthrough_reason = "not-a-compile|--print cfg".to_string();
        let mut link = ev("app", EventResult::Passthrough, "s2", 24, "");
        link.passthrough_reason = "unsupported|cc link mode".to_string();
        let events = vec![
            ev("costly", EventResult::LocalHit, "s1", 1, "k1"),
            ev("cheap", EventResult::Miss, "s1", 2, "c1"),
            changed,
            evicted,
            probe,
            link,
        ];
        let all = sessions(&events);
        let builds = latest_builds(&all, None).unwrap();
        let analysis = tui_sessions::analyze_session(&events, builds.latest, &all);
        let lines = render(&builds, &analysis, &events, 5 << 30);
        let text = lines.join("\n");
        assert_eq!(lines[0], "kache explain · latest build of /w");
        assert!(
            text.contains("  Misses        2   1 in the build before"),
            "{text}"
        );
        assert!(text.contains("  Unexplained   0.0%"), "{text}");
        let costly = lines.iter().position(|l| l.contains("costly")).unwrap();
        let cheap = lines.iter().position(|l| l.contains("cheap")).unwrap();
        assert!(costly < cheap, "costliest first: {text}");
        assert!(lines[costly].contains("exact key"), "{text}");
        assert!(text.contains("What would help"), "{text}");
        assert!(text.contains("larger than 5.0 GiB"), "{text}");
        assert!(text.contains("unsupported: cc link mode"), "{text}");
        assert!(
            text.contains("compiler queries; nothing to cache, expected"),
            "{text}"
        );
        assert_eq!(
            lines.last().unwrap(),
            "`kache explain costly` for the costliest miss; the build's timeline: `kache stats --format trace -o trace.json`"
        );
    }

    #[test]
    fn a_build_with_no_misses_and_no_history_says_so() {
        let events = vec![ev("a", EventResult::LocalHit, "s1", 1, "k1")];
        let all = sessions(&events);
        let builds = latest_builds(&all, None).unwrap();
        let analysis = tui_sessions::analyze_session(&events, builds.latest, &all);
        let text = render(&builds, &analysis, &events, 1 << 30).join("\n");
        assert!(text.contains("no earlier build of this tree"), "{text}");
        assert!(!text.contains("Unexplained"), "{text}");
        assert!(!text.contains("Why it missed"), "{text}");
        assert!(text.ends_with("the build's timeline: `kache stats --format trace -o trace.json`"));
        assert_eq!(percent(1, 4), 25.0);
        assert_eq!(percent(1, 0), 0.0);
    }

    fn report(events: &[BuildEvent]) -> (Vec<String>, Body) {
        let all = sessions(events);
        let builds = latest_builds(&all, None).unwrap();
        let analysis = tui_sessions::analyze_session(events, builds.latest, &all);
        (
            render(&builds, &analysis, events, 1 << 30),
            json_body(&builds, &analysis, events, 1 << 30),
        )
    }

    #[test]
    fn the_json_body_carries_the_same_report() {
        let mut evicted = ev("costly", EventResult::Miss, "s2", 22, "k1");
        evicted.compile_time_ms = 9_000;
        let mut cheap = ev("cheap", EventResult::Miss, "s2", 21, "c2");
        cheap.key_diff = vec!["sources".to_string()];
        let mut probe = ev("probe", EventResult::Passthrough, "s2", 23, "");
        probe.passthrough_reason = "not-a-compile|--print cfg".to_string();
        let events = vec![
            ev("costly", EventResult::LocalHit, "s1", 1, "k1"),
            ev("cheap", EventResult::Miss, "s1", 2, "c1"),
            cheap,
            evicted,
            probe,
        ];
        let (_, body) = report(&events);
        assert_eq!(body.root, "/w");
        assert_eq!(body.misses, 2);
        assert_eq!(body.earlier_misses, Some(1));
        assert_eq!(body.causes[0].crates, ["costly"], "costliest first");
        assert!(body.causes[0].failure);
        assert!(body.causes[0].fix.contains("1.0 GiB"));
        assert_eq!(body.causes[1].compile_ms, 1_000);
        assert!(!body.causes[1].failure);
        assert_eq!(body.not_cached.len(), 1);
        assert!(body.not_cached[0].expected);
        assert!(body.keeps_missing.is_empty());
    }

    #[test]
    fn not_cached_lists_real_reasons_and_probes_apart() {
        let mut probe = ev("probe", EventResult::Passthrough, "s1", 1, "");
        probe.passthrough_reason = "not-a-compile|--print cfg".to_string();
        let (only_probes, _) = report(&[probe]);
        let text = only_probes.join("\n");
        assert!(text.contains("Not cached"), "{text}");
        assert!(
            text.contains("compiler queries; nothing to cache, expected"),
            "{text}"
        );

        let mut link = ev("app", EventResult::Passthrough, "s1", 1, "");
        link.passthrough_reason = "unsupported|cc link mode".to_string();
        let (only_real, _) = report(&[link]);
        let text = only_real.join("\n");
        assert!(text.contains("unsupported: cc link mode"), "{text}");
        assert!(!text.contains("compiler queries"), "{text}");

        let (neither, _) = report(&[ev("a", EventResult::LocalHit, "s1", 1, "k")]);
        assert!(!neither.join("\n").contains("Not cached"));
    }

    #[test]
    fn a_capped_analysis_says_how_many_misses_it_shows() {
        let events = vec![ev("a", EventResult::Miss, "s1", 1, "k")];
        let all = sessions(&events);
        let builds = latest_builds(&all, None).unwrap();
        let mut analysis = tui_sessions::analyze_session(&events, builds.latest, &all);
        let full = render(&builds, &analysis, &events, 1 << 30).join("\n");
        assert!(!full.contains("misses are shown"), "{full}");
        assert!(!full.contains("Keeps missing"), "{full}");
        analysis.chronic.push(tui_sessions::Chronic {
            crate_name: "flaky".to_string(),
            missed: 3,
            seen: 4,
            last_store_failed: true,
        });
        let chronic = render(&builds, &analysis, &events, 1 << 30).join("\n");
        assert!(
            chronic.contains("Keeps missing\n  flaky   missed in 3 of 4 builds, and its last result failed to store"),
            "{chronic}"
        );
        analysis.misses_total = 3;
        let capped = render(&builds, &analysis, &events, 1 << 30).join("\n");
        assert!(
            capped.contains("The newest 1 of 3 misses are shown."),
            "{capped}"
        );
    }

    #[test]
    fn an_unreadable_log_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(dir.path().join("cache"));
        std::fs::create_dir_all(config.event_log_path()).unwrap();
        assert!(run(&config, None, false).is_err());
        assert!(run(&config, None, true).is_err());
    }
}
