use super::Check;
use crate::events::{BuildEvent, EventResult};
use std::path::PathBuf;

pub(super) const FILE_HASH_MEMO: &str = "File-hash memo";
pub(super) const KEY_TIME: &str = "Key time";

/// A median key at or above this is slow: keys normally take a few
/// milliseconds, and compiling a small crate can take less than this.
const SLOW_KEY_MEDIAN_MS: u64 = 250;
/// Fewer keyed units than this say little about a build.
const MIN_KEYED_UNITS: usize = 20;

/// How long the latest build in the event log spent computing cache keys.
pub(super) fn key_time_check(event_log: &std::path::Path) -> Option<Check> {
    let events = crate::events::read_events(event_log).ok()?;
    let sessions = crate::tui_sessions::group(
        &events,
        std::time::Duration::from_secs(crate::wrapper::BUILD_SESSION_SECS),
    );
    let builds = crate::explain::latest_builds(&sessions, None)?;
    let latest: Vec<_> = builds.latest.events.iter().map(|&i| &events[i]).collect();
    key_time_result(&latest)
}

fn key_time_result(events: &[&BuildEvent]) -> Option<Check> {
    let keyed: Vec<_> = events
        .iter()
        .filter(|event| {
            matches!(
                event.result,
                EventResult::Miss
                    | EventResult::Dup
                    | EventResult::LocalHit
                    | EventResult::PrefetchHit
                    | EventResult::RemoteHit
            )
        })
        .collect();
    if keyed.len() < MIN_KEYED_UNITS {
        return None;
    }
    let mut key_ms: Vec<_> = keyed.iter().map(|event| event.key_ms).collect();
    key_ms.sort_unstable();
    // The lower median: half the units keyed at least this slowly.
    let median = key_ms[(key_ms.len() - 1) / 2];
    let files: u64 = keyed.iter().map(|event| event.key_hash_misses).sum();
    let mib = keyed.iter().map(|event| event.key_hash_bytes).sum::<u64>() / (1024 * 1024);
    let detail = format!(
        "median {median} ms over {} units in the latest build; keys read {files} files ({mib} MiB)",
        keyed.len()
    );
    let pass = median < SLOW_KEY_MEDIAN_MS;
    Some(Check {
        label: KEY_TIME,
        pass,
        detail,
        fix: (!pass).then(|| {
            "set KACHE_PHASE_TRACE_DIR for one build to see which key phase is slow; \
             on Windows, `kache doctor` also checks the file-hash memo"
                .into()
        }),
    })
}

/// Whether unchanged key inputs under each root can be served from the
/// file-hash memo. On Windows that needs NTFS file identity; elsewhere every
/// root passes, so the check is only shown on Windows.
pub(super) fn memo_volume_check(roots: &[(&str, PathBuf)]) -> Check {
    let problems: Vec<_> = roots
        .iter()
        .filter(|(_, path)| path.exists())
        .filter_map(|(name, path)| {
            kache_store::file_hash::memo_identity_problem(path)
                .map(|problem| (*name, path.as_path(), problem))
        })
        .collect();
    memo_volume_result(&problems)
}

fn memo_volume_result(problems: &[(&str, &std::path::Path, String)]) -> Check {
    if problems.is_empty() {
        return Check {
            label: FILE_HASH_MEMO,
            pass: true,
            detail: "key inputs are on NTFS; an unchanged file is hashed once".into(),
            fix: None,
        };
    }
    let detail = problems
        .iter()
        .map(|(name, path, problem)| format!("{name} {}: {problem}", crate::term::home_path(path)))
        .collect::<Vec<_>>()
        .join("; ");
    Check {
        label: FILE_HASH_MEMO,
        pass: false,
        detail,
        fix: Some(
            "every cache key reads these files again, which antivirus scanning makes slow; \
             keep them on NTFS (ReFS, including Dev Drive, FAT and exFAT are not memoised)"
                .into(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn keyed(result: EventResult, key_ms: u64) -> BuildEvent {
        let mut event = BuildEvent::new_for_test("unit", result);
        event.key_ms = key_ms;
        event.key_hash_misses = 3;
        event.key_hash_bytes = 1024 * 1024;
        event
    }

    #[test]
    fn a_slow_median_key_fails() {
        // The shape reported in #1855: misses keying for about a second.
        let mut events: Vec<_> = (0..19).map(|_| keyed(EventResult::Miss, 1048)).collect();
        events.push(keyed(EventResult::LocalHit, 9));
        events.push(keyed(EventResult::Passthrough, 0));
        let refs: Vec<_> = events.iter().collect();
        let check = key_time_result(&refs).unwrap();
        assert!(!check.pass);
        assert_eq!(
            check.detail,
            "median 1048 ms over 20 units in the latest build; keys read 60 files (20 MiB)"
        );
        assert!(check.fix.unwrap().contains("KACHE_PHASE_TRACE_DIR"));
    }

    #[test]
    fn the_lower_median_is_reported() {
        let events: Vec<_> = (0..20).map(|i| keyed(EventResult::Miss, i * 10)).collect();
        let check = key_time_result(&events.iter().collect::<Vec<_>>()).unwrap();
        assert!(
            check.detail.starts_with("median 90 ms over 20 units"),
            "{}",
            check.detail
        );
    }

    #[test]
    fn key_time_reads_the_latest_build_from_the_event_log() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("events.jsonl");
        let lines: Vec<_> = (0..MIN_KEYED_UNITS)
            .map(|_| serde_json::to_string(&keyed(EventResult::Miss, 1048)).unwrap())
            .collect();
        std::fs::write(&log, lines.join("\n") + "\n").unwrap();
        let check = key_time_check(&log).unwrap();
        assert!(!check.pass, "{}", check.detail);
        assert!(key_time_check(&dir.path().join("missing.jsonl")).is_none());
    }

    #[test]
    fn key_time_threshold_and_sample_size() {
        let build = |key_ms: u64, units: usize| -> Option<Check> {
            let events: Vec<_> = (0..units)
                .map(|_| keyed(EventResult::Miss, key_ms))
                .collect();
            key_time_result(&events.iter().collect::<Vec<_>>())
        };
        assert!(build(SLOW_KEY_MEDIAN_MS - 1, MIN_KEYED_UNITS).unwrap().pass);
        assert!(!build(SLOW_KEY_MEDIAN_MS, MIN_KEYED_UNITS).unwrap().pass);
        assert!(build(SLOW_KEY_MEDIAN_MS, MIN_KEYED_UNITS - 1).is_none());

        // Ten fast and ten slow keys: half the build is not slow enough.
        let mut events: Vec<_> = (0..10).map(|_| keyed(EventResult::Miss, 100)).collect();
        events.extend((0..10).map(|_| keyed(EventResult::Dup, SLOW_KEY_MEDIAN_MS)));
        assert!(
            key_time_result(&events.iter().collect::<Vec<_>>())
                .unwrap()
                .pass
        );
    }

    #[test]
    fn each_root_without_file_identity_is_named() {
        let pass = memo_volume_result(&[]);
        assert!(pass.pass);

        let check = memo_volume_result(&[
            (
                "workspace",
                Path::new("D:/src/app"),
                "ReFS gives no file identity".into(),
            ),
            (
                "target directory",
                Path::new("E:/target"),
                "FAT32 gives no file identity".into(),
            ),
        ]);
        assert!(!check.pass);
        assert!(
            check.detail.contains("workspace D:/src/app: ReFS"),
            "{}",
            check.detail
        );
        assert!(
            check.detail.contains("target directory E:/target: FAT32"),
            "{}",
            check.detail
        );
        assert!(check.fix.unwrap().contains("Dev Drive"));
    }

    #[test]
    fn missing_roots_are_skipped() {
        let check = memo_volume_check(&[("target directory", PathBuf::from("/no/such/kache/dir"))]);
        assert!(check.pass);
    }
}
