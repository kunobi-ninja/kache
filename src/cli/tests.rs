use super::*;
use std::fs;

#[test]
fn shutdown_summary_status_distinguishes_partial_totals_from_cancellation() {
    assert_eq!(summary_status_suffix(false, false), "");
    assert_eq!(summary_status_suffix(true, false), ", CANCELLED");
    assert_eq!(
        summary_status_suffix(false, true),
        ", INCOMPLETE (partial totals)"
    );
    assert_eq!(
        summary_status_suffix(true, true),
        ", CANCELLED, INCOMPLETE (partial totals)"
    );
}

// ── Build timeline push ─────────────────────────────────────────────────

fn timeline_record(session_id: &str, started_at_ms: u64) -> kache_core::timeline::BuildTimeline {
    kache_core::timeline::BuildTimeline {
        schema: kache_core::timeline::BUILD_TIMELINE_SCHEMA,
        client_record_id: format!("r-{session_id}"),
        session_id: session_id.to_string(),
        started_at_ms,
        ..Default::default()
    }
}

#[test]
fn the_default_selection_is_the_last_build() {
    let records = vec![timeline_record("old", 1), timeline_record("new", 2)];
    let selected = select_timelines(records.clone(), &TimelineSelection::Latest);
    assert_eq!(
        selected
            .iter()
            .map(|r| r.session_id.as_str())
            .collect::<Vec<_>>(),
        ["new"]
    );

    assert_eq!(
        select_timelines(records.clone(), &TimelineSelection::All).len(),
        2
    );
    assert_eq!(
        select_timelines(records, &TimelineSelection::Session("old".to_string()))
            .iter()
            .map(|r| r.session_id.as_str())
            .collect::<Vec<_>>(),
        ["old"]
    );
}

#[test]
fn selecting_from_no_records_sends_nothing() {
    for selection in [
        TimelineSelection::Latest,
        TimelineSelection::All,
        TimelineSelection::Session("s".to_string()),
    ] {
        assert!(select_timelines(Vec::new(), &selection).is_empty());
    }
    assert!(
        select_timelines(
            vec![timeline_record("s1", 1)],
            &TimelineSelection::Session("other".to_string())
        )
        .is_empty()
    );
}

#[test]
fn labels_are_key_value_pairs() {
    let parsed = parse_labels(&[
        "phase=cold".to_string(),
        " scenario = firefox ".to_string(),
        "empty=".to_string(),
    ])
    .unwrap();
    assert_eq!(parsed["phase"], "cold");
    assert_eq!(parsed["scenario"], "firefox");
    assert_eq!(parsed["empty"], "");

    for bad in ["nope", "=value"] {
        assert!(parse_labels(&[bad.to_string()]).is_err(), "{bad}");
    }
}

#[test]
fn an_unstamped_log_says_the_wrapper_is_too_old() {
    let versions = std::collections::BTreeSet::from(["0.23.1".to_string()]);
    let message = nothing_to_send(12, &versions);
    assert!(message.contains("12 compile(s)"), "{message}");
    assert!(message.contains("0.23.1"), "{message}");
    assert!(message.contains("Upgrade kache"), "{message}");

    assert_eq!(
        nothing_to_send(0, &Default::default()),
        "no builds in the event log"
    );
    assert!(
        nothing_to_send(3, &Default::default()).contains("unknown version"),
        "a log with no version still explains itself"
    );
}

#[test]
fn a_push_without_logs_reports_that_and_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    telemetry_push(&config, &TimelineSelection::Latest, &[], true)
        .expect("an empty log directory is not an error");
}

// ── List pager resolution ───────────────────────────────────────────────

#[test]
fn detail_fields_render_only_nonempty_values() {
    let mut lines = Vec::new();
    push_nonempty_detail(&mut lines, "  Type:     ", "");
    assert!(lines.is_empty());

    push_nonempty_detail(&mut lines, "  Type:     ", "lib");
    push_nonempty_detail(&mut lines, "  Features: ", "serde, std");
    assert_eq!(
        lines,
        vec![
            "  Type:     lib".to_string(),
            "  Features: serde, std".to_string(),
        ]
    );
}

struct FailOnWrite {
    fail_on_call: usize,
    calls: usize,
}

impl std::io::Write for FailOnWrite {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.calls += 1;
        if self.calls == self.fail_on_call {
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "pager exited",
            ))
        } else {
            Ok(buffer.len())
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn pager_line_writer_stops_on_content_or_newline_errors() {
    let lines = vec!["first".to_string(), "second".to_string()];
    let mut output = Vec::new();
    assert!(write_pager_lines(&mut output, &lines));
    assert_eq!(output, b"first\nsecond\n");

    let mut content_error = FailOnWrite {
        fail_on_call: 1,
        calls: 0,
    };
    assert!(!write_pager_lines(&mut content_error, &lines));
    assert_eq!(content_error.calls, 1);

    let mut newline_error = FailOnWrite {
        fail_on_call: 2,
        calls: 0,
    };
    assert!(!write_pager_lines(&mut newline_error, &lines));
    assert_eq!(newline_error.calls, 2);
}

#[test]
fn pager_resolution_obeys_tty_disable_and_environment_precedence() {
    assert_eq!(
        resolve_pager_argv(false, true, Some("most -R"), Some("less -S"), false),
        Some(vec!["most".to_string(), "-R".to_string()])
    );
    assert_eq!(
        resolve_pager_argv(false, true, None, Some("less -S"), false),
        Some(vec!["less".to_string(), "-S".to_string()])
    );
    assert_eq!(
        resolve_pager_argv(true, true, Some("less"), None, false),
        None
    );
    assert_eq!(
        resolve_pager_argv(false, false, Some("less"), None, false),
        None
    );

    // A present but empty KACHE_PAGER wins over PAGER and disables paging.
    assert_eq!(
        resolve_pager_argv(false, true, Some(""), Some("less"), false),
        None
    );
    assert_eq!(
        resolve_pager_argv(false, true, Some("cat"), Some("less"), false),
        None
    );
    assert_eq!(
        resolve_pager_argv(false, true, None, Some("cat"), false),
        None
    );
}

#[test]
fn pager_resolution_uses_platform_defaults() {
    assert_eq!(
        resolve_pager_argv(false, true, None, None, false),
        Some(vec!["less".to_string(), "-FRX".to_string()])
    );
    assert_eq!(
        resolve_pager_argv(false, true, None, None, true),
        Some(vec!["more.com".to_string()])
    );
}

#[test]
fn pager_resolution_groups_quotes_without_shell_evaluation() {
    assert_eq!(
        resolve_pager_argv(
            false,
            true,
            Some(r#""C:\Program Files\Git\usr\bin\less.exe" -FRX"#),
            None,
            true
        ),
        Some(vec![
            r"C:\Program Files\Git\usr\bin\less.exe".to_string(),
            "-FRX".to_string()
        ])
    );
    assert_eq!(
        resolve_pager_argv(
            false,
            true,
            Some("less --prompt='literal ; $HOME | text'"),
            None,
            false
        ),
        Some(vec![
            "less".to_string(),
            "--prompt=literal ; $HOME | text".to_string()
        ])
    );
    assert_eq!(
        resolve_pager_argv(false, true, Some("less 'unterminated"), None, false),
        None
    );
}

// ── Doctor check dispositions (kunobi-ninja/kache#443, #626) ──────────

/// Which failing checks are informational: the full truth table for
/// daemon-optional and probe-no-compiler downgrades.
/// `kache stats` names the host file only while it is merged in.
#[test]
fn stats_names_the_host_config_only_when_in_effect() {
    use crate::config::HostConfigStatus;
    let path = std::path::PathBuf::from("/etc/kache/config.toml");
    assert_eq!(
        host_config_in_effect(&HostConfigStatus::Present {
            path: path.clone(),
            keys: Vec::new(),
        })
        .as_deref(),
        Some("/etc/kache/config.toml")
    );
    assert_eq!(host_config_in_effect(&HostConfigStatus::Disabled), None);
    assert_eq!(
        host_config_in_effect(&HostConfigStatus::Absent { path: path.clone() }),
        None
    );
    assert_eq!(
        host_config_in_effect(&HostConfigStatus::Invalid {
            path,
            error: "parsing host config".to_string(),
        }),
        None
    );
}

/// Each host-config state reads the way `kache doctor` should say it: an
/// unusable file is the only failure, and an overridden key names what
/// wins over it.
#[test]
fn doctor_host_config_check_wording() {
    use crate::config::{HostConfigKey, HostConfigStatus, HostKeySource};
    let path = std::path::PathBuf::from("/etc/kache/config.toml");

    assert!(doctor_host_config_check(&HostConfigStatus::Disabled).0);

    let (pass, detail, fix) =
        doctor_host_config_check(&HostConfigStatus::Absent { path: path.clone() });
    assert!(pass);
    assert!(
        detail.contains("none at /etc/kache/config.toml"),
        "{detail}"
    );
    assert!(fix.is_none());

    let (pass, detail, fix) = doctor_host_config_check(&HostConfigStatus::Invalid {
        path: path.clone(),
        error: "parsing host config".to_string(),
    });
    assert!(!pass);
    assert!(detail.contains("is ignored"), "{detail}");
    assert!(fix.unwrap().contains("fix or remove"));

    let (pass, detail, _) = doctor_host_config_check(&HostConfigStatus::Present {
        path,
        keys: vec![
            HostConfigKey {
                key: "cache.input_predictions".to_string(),
                source: HostKeySource::Host,
            },
            HostConfigKey {
                key: "cache.local_max_size".to_string(),
                source: HostKeySource::ChosenFile("/job/kache-action.toml".into()),
            },
            HostConfigKey {
                key: "cache.local_only".to_string(),
                source: HostKeySource::Env("KACHE_LOCAL_ONLY"),
            },
        ],
    });
    assert!(pass);
    assert!(detail.contains("cache.input_predictions, "), "{detail}");
    assert!(
        detail.contains("cache.local_max_size (overridden by /job/kache-action.toml)"),
        "{detail}"
    );
    assert!(
        detail.contains("cache.local_only (overridden by KACHE_LOCAL_ONLY)"),
        "{detail}"
    );
}

#[test]
fn doctor_check_optionality_truth_table() {
    // Daemon labels downgrade exactly when the daemon is optional.
    assert!(doctor_check_is_optional("Daemon version", true, false));
    assert!(!doctor_check_is_optional("Daemon version", false, false));
    // The compiler probe downgrades exactly when there is no cc at all.
    assert!(doctor_check_is_optional("Compiler probe", false, true));
    assert!(!doctor_check_is_optional("Compiler probe", false, false));
    // probe_no_compiler must not leak onto other labels, nor
    // daemon_optional onto the probe.
    assert!(!doctor_check_is_optional("Binary", true, true));
    assert!(!doctor_check_is_optional("Daemon version", false, true));
    assert!(!doctor_check_is_optional("Compiler probe", true, false));
    // Non-daemon, non-probe labels are never optional, except C/C++
    // shims: PATH masquerade is opt-in and rust-only setups must not
    // fail doctor for skipping it.
    assert!(!doctor_check_is_optional("Remote", true, true));
    assert!(doctor_check_is_optional("C/C++ shims", false, false));
    assert!(doctor_check_is_optional("C/C++ shims", true, true));
}

#[test]
fn daemon_service_is_satisfied_by_install_or_healthy_on_demand_daemon() {
    assert_eq!(daemon_service_check(true, false), (true, None));
    assert_eq!(daemon_service_check(false, true), (true, None));
    assert_eq!(
        daemon_service_check(false, false),
        (false, Some("kache daemon install"))
    );
}

/// The daemon footnote prints only for a FAILING daemon check under an
/// optional daemon — never for passing daemon checks, failing non-daemon
/// checks, or a required daemon.
#[test]
fn daemon_footnote_only_for_downgraded_daemon_failures() {
    assert!(daemon_footnote_needed(
        true,
        &[("Daemon service", false), ("Binary", true)]
    ));
    assert!(!daemon_footnote_needed(false, &[("Daemon service", false)]));
    assert!(!daemon_footnote_needed(
        true,
        &[("Daemon service", true), ("Binary", true)]
    ));
    assert!(!daemon_footnote_needed(
        true,
        &[("Compiler probe", false), ("Binary", false)]
    ));
    assert!(!daemon_footnote_needed(true, &[]));
}

// ── Daemon version reporting (kunobi-ninja/kache#720) ──────────────────

/// The upgrade window: a daemon from before the upgrade is still answering.
/// It must read as an upgrade left to finish, not as a version conflict, and
/// the hint must point at the flag that actually restarts it.
#[test]
fn daemon_version_check_names_the_pending_upgrade() {
    let (pass, detail, fix) = daemon_version_check(
        Some(("0.13.0", 100)),
        None,
        "0.14.0",
        200,
        "/run/kache/daemon.log",
    );
    assert!(!pass);
    assert!(detail.contains("predates"), "{detail}");
    assert!(
        detail.contains("0.13.0") && detail.contains("0.14.0"),
        "{detail}"
    );
    assert!(detail.contains("shutting down"), "{detail}");
    assert!(fix.unwrap().contains("doctor --fix"));
}

/// Epochs are executable mtimes and `0` means unreadable, so several
/// mismatches are genuinely unordered. Guessing a culprit there sends someone
/// to reinstall a working kache, so every one of them must decline to.
#[test]
fn daemon_version_check_does_not_invent_an_order_it_cannot_determine() {
    for (daemon, my_version, my_epoch, case) in [
        (("0.13.0", 0), "0.14.0", 200, "daemon epoch unreadable"),
        (("0.13.0", 200), "0.14.0", 0, "binary epoch unreadable"),
        (("0.13.0", 0), "0.14.0", 0, "neither epoch readable"),
        (
            ("0.13.0", 200),
            "0.14.0",
            200,
            "one build, two version strings",
        ),
    ] {
        let (pass, detail, fix) = daemon_version_check(
            Some(daemon),
            None,
            my_version,
            my_epoch,
            "/run/kache/daemon.log",
        );
        assert!(!pass, "{case}: {detail}");
        assert!(detail.contains("cannot be determined"), "{case}: {detail}");
        let fix = fix.unwrap();
        assert!(
            !fix.contains("this binary is the stale one"),
            "{case}: {fix}"
        );
    }

    // Equal version strings and unreadable epochs are not evidence of the
    // same build either — that pair must not pass.
    let (pass, detail, _) = daemon_version_check(
        Some(("0.14.0", 0)),
        None,
        "0.14.0",
        0,
        "/run/kache/daemon.log",
    );
    assert!(!pass, "{detail}");
}

/// The other direction — an old binary against a newer daemon — must not
/// advise restarting the daemon, which would downgrade it.
#[test]
fn daemon_version_check_blames_the_binary_when_the_daemon_is_newer() {
    let (pass, detail, fix) = daemon_version_check(
        Some(("0.14.0", 200)),
        None,
        "0.13.0",
        100,
        "/run/kache/daemon.log",
    );
    assert!(!pass);
    assert!(detail.contains("newer than binary"), "{detail}");
    let fix = fix.unwrap();
    assert!(fix.contains("this binary is the stale one"), "{fix}");
    assert!(!fix.contains("daemon start"), "{fix}");
}

/// Matching build: the only passing state, and it stays terse.
#[test]
fn daemon_version_check_passes_on_identical_build() {
    let (pass, detail, fix) = daemon_version_check(
        Some(("0.14.0", 200)),
        None,
        "0.14.0",
        200,
        "/run/kache/daemon.log",
    );
    assert!(pass);
    assert_eq!(detail, "v0.14.0 (epoch 200)");
    assert!(fix.is_none());
}

/// Same version string, different build — a locally rebuilt daemon is stale
/// even though the version reads identical.
#[test]
fn daemon_version_check_catches_same_version_different_build() {
    let (pass, detail, _) = daemon_version_check(
        Some(("0.14.0", 100)),
        None,
        "0.14.0",
        200,
        "/run/kache/daemon.log",
    );
    assert!(!pass, "{detail}");
    assert!(detail.contains("predates"), "{detail}");
}

/// The window that made a routine upgrade look like a broken install: no
/// daemon answers yet because the replacement is still binding its socket.
/// Reporting "not reachable → start the daemon" there is actively wrong, and
/// so is counting a healthy transient against the install — the coordinator
/// file says the right build is coming up, which is what this check asks.
#[test]
fn daemon_version_check_reports_a_daemon_that_is_still_starting() {
    let (pass, detail, fix) =
        daemon_version_check(None, Some(200), "0.14.0", 200, "/run/kache/daemon.log");
    assert!(pass, "{detail}");
    assert!(detail.contains("starting"), "{detail}");
    assert!(fix.is_none());
    // Coordinator state has no version string, so none may be asserted here.
    assert!(!detail.contains("v0.14.0"), "{detail}");

    // A starting daemon of some other build gets named as such rather than
    // silently claimed to be this one.
    let (pass, detail, _) =
        daemon_version_check(None, Some(100), "0.14.0", 200, "/run/kache/daemon.log");
    assert!(!pass, "{detail}");
    assert!(detail.contains("epoch 100"), "{detail}");
    assert!(detail.contains("0.14.0"), "{detail}");

    // An unreadable epoch on both sides is not a match, so it must not pass
    // through the equality arm.
    let (pass, detail, _) =
        daemon_version_check(None, Some(0), "0.14.0", 0, "/run/kache/daemon.log");
    assert!(!pass, "{detail}");
}

/// The behaviour this whole change exists for: a plain `doctor` run reports
/// a stale daemon, it does not replace it and pay the startup wait. Only
/// `--fix` opts into that.
#[test]
fn only_fix_restarts_a_stale_daemon() {
    assert!(should_restart_stale_daemon(true, true));

    assert!(!should_restart_stale_daemon(false, true), "plain doctor");
    assert!(!should_restart_stale_daemon(true, false), "nothing stale");
    assert!(!should_restart_stale_daemon(false, false), "neither");
}

/// A restart that did not finish must never read as one that is still
/// finishing — that is what turns a failed `--fix` into a silent no-op in the
/// reader's head.
#[test]
fn stale_restart_note_never_claims_a_replacement_that_may_not_exist() {
    assert!(stale_restart_note(&Ok(true)).is_none());

    let timed_out = stale_restart_note(&Ok(false)).unwrap();
    assert!(timed_out.contains("did not bind"), "{timed_out}");
    assert!(!timed_out.contains("background"), "{timed_out}");

    let failed = stale_restart_note(&Err(anyhow::anyhow!("spawn refused"))).unwrap();
    assert!(failed.contains("restart failed"), "{failed}");
    assert!(failed.contains("spawn refused"), "{failed}");
}

/// Nothing answering and nothing coming up keeps the original wording.
#[test]
fn daemon_version_check_reports_an_absent_daemon() {
    let (pass, detail, fix) =
        daemon_version_check(None, None, "0.14.0", 200, "/run/kache/daemon.log");
    assert!(!pass);
    assert_eq!(detail, "daemon not reachable");
    let fix = fix.unwrap();
    assert!(fix.contains("kache daemon start"), "{fix}");
    assert!(fix.contains("/run/kache/daemon.log"), "{fix}");
}

/// A daemon that answered wins over the coordinator file: a leftover
/// `Starting` record must not relabel a reachable daemon as starting.
#[test]
fn daemon_version_check_prefers_the_daemon_that_answered() {
    let (pass, detail, _) = daemon_version_check(
        Some(("0.14.0", 200)),
        Some(100),
        "0.14.0",
        200,
        "/run/kache/daemon.log",
    );
    assert!(pass, "{detail}");
    assert_eq!(detail, "v0.14.0 (epoch 200)");
}

// ── Eviction reporting (kunobi-ninja/kache#509) ────────────────────────

fn gc_stats(evicted: usize, pinned: usize, bytes: u64) -> crate::store::GcStats {
    crate::store::GcStats {
        entries_evicted: evicted,
        bytes_freed: bytes,
        entries_pinned: pinned,
        ..Default::default()
    }
}

#[test]
fn cloned_targets_summary_only_appears_for_retained_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let mut disk = crate::machine::disk_view(dir.path(), 0, 1024);
    assert!(cloned_targets_line(&disk).is_none());

    disk.disk_private_bytes = 3;
    disk.cloned_into_targets_bytes = 7;
    let line = cloned_targets_line(&disk).expect("cloned blocks need a summary");
    assert!(line.contains("3 B"), "{line}");
    assert!(line.contains("7 B cloned"), "{line}");
    assert!(!line.contains("snapshots"), "{line}");

    disk.snapshot_retained_bytes = 5;
    let line = cloned_targets_line(&disk).expect("both retainers are named");
    assert!(line.contains("7 B cloned"), "{line}");
    assert!(
        line.contains("5 B held only by filesystem snapshots"),
        "{line}"
    );

    disk.cloned_into_targets_bytes = 0;
    let line = cloned_targets_line(&disk).expect("snapshot blocks need a summary");
    assert!(!line.contains("cloned"), "{line}");
    assert!(line.contains("5 B held only"), "{line}");
}

#[test]
fn auto_gc_worker_retries_after_pins_or_a_lost_lock() {
    let swept_clean = crate::store::GcStats::default();
    assert!(!auto_gc_retry_wanted(&swept_clean));
    assert!(auto_gc_retry_wanted(&skipped_gc_stats()));
    let pinned = crate::store::GcStats {
        entries_pinned: 1,
        ..crate::store::GcStats::default()
    };
    assert!(auto_gc_retry_wanted(&pinned));
}

#[test]
fn gc_machine_output_boundaries_are_explicit() {
    assert!(human_gc_output(false));
    assert!(!human_gc_output(true));

    let stats = skipped_gc_stats();
    assert!(stats.skipped);
    assert_eq!(stats.entries_evicted, 0);
    assert_eq!(DEFAULT_TRACKED_STALE_HOURS, 14 * 24);

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("target");
    std::fs::create_dir(&path).unwrap();
    assert!(!path_was_removed(&path));
    std::fs::remove_dir(&path).unwrap();
    assert!(path_was_removed(&path));
    assert!(human_clean_output(false));
    assert!(!human_clean_output(true));
    assert!(!doctor_has_issues(0));
    assert!(doctor_has_issues(1));
}

#[test]
fn json_list_crate_filter_is_exact() {
    assert!(crate_name_matches("serde", "serde"));
    assert!(!crate_name_matches("serde", "serde_json"));
}

#[test]
fn daemon_gc_breakdown_sums_every_policy_field() {
    fn policy(n: u64) -> crate::daemon::GcPolicyOutcome {
        crate::daemon::GcPolicyOutcome {
            entries_evicted: n as usize,
            bytes_freed: n * 10,
            entries_pinned: (n * 100) as usize,
            disk_bytes_reclaimed: n * 1_000,
            entries_unreclaimable: (n * 10_000) as usize,
            entries_failed: (n * 100_000) as usize,
            entries_locked: (n * 1_000_000) as usize,
            evict_write_ms: n * 10_000_000,
            bytes_held: n * 100_000_000,
        }
    }
    let report = crate::daemon::GcBreakdown {
        mode: crate::daemon::GcRequestMode::Automatic,
        age: policy(1),
        duplicate: policy(2),
        size: policy(3),
    };
    let total = gc_stats_from_breakdown(&report);
    assert_eq!(total.entries_evicted, 6);
    assert_eq!(total.bytes_freed, 60);
    assert_eq!(total.entries_pinned, 600);
    assert_eq!(total.disk_bytes_reclaimed, 6_000);
    assert_eq!(total.entries_unreclaimable, 60_000);
    assert_eq!(total.entries_failed, 600_000);
    assert_eq!(total.entries_locked, 6_000_000);
    assert_eq!(total.evict_write_ms, 60_000_000);
    assert_eq!(
        total.bytes_held, 300_000_000,
        "the largest probe, not a sum"
    );

    let mut accumulated = crate::store::GcStats {
        entries_evicted: 1,
        bytes_freed: 2,
        entries_pinned: 3,
        blobs_removed: 4,
        duration_ms: 7,
        entries_unreclaimable: 5,
        disk_bytes_reclaimed: 6,
        skipped: false,
        entries_failed: 8,
        entries_locked: 9,
        entries_busy_snapshot: 0,
        entries_recent_prefiltered: 0,
        entries_import_pinned: 12,
        evict_write_ms: 11,
        bytes_held: 13,
        // Set once by the driver, never summed over policies.
        housekeeping: None,
    };
    let part = crate::store::GcStats {
        entries_evicted: 10,
        bytes_freed: 20,
        entries_pinned: 30,
        blobs_removed: 40,
        duration_ms: 70,
        entries_unreclaimable: 50,
        disk_bytes_reclaimed: 60,
        skipped: true,
        entries_failed: 80,
        entries_locked: 90,
        entries_busy_snapshot: 0,
        entries_recent_prefiltered: 0,
        entries_import_pinned: 120,
        evict_write_ms: 110,
        bytes_held: 7,
        // Set once by the driver, never summed over policies.
        housekeeping: None,
    };
    add_gc_stats(&mut accumulated, &part);
    assert_eq!(accumulated.bytes_held, 13, "the larger probe, not a sum");
    assert_eq!(accumulated.entries_evicted, 11);
    assert_eq!(accumulated.bytes_freed, 22);
    assert_eq!(accumulated.entries_pinned, 33);
    assert_eq!(accumulated.blobs_removed, 44);
    assert_eq!(accumulated.duration_ms, 77);
    assert_eq!(accumulated.entries_unreclaimable, 55);
    assert_eq!(accumulated.disk_bytes_reclaimed, 66);
    assert_eq!(accumulated.entries_failed, 88);
    assert_eq!(accumulated.entries_locked, 99);
    assert_eq!(accumulated.evict_write_ms, 121);
    assert_eq!(accumulated.entries_import_pinned, 132);
    assert!(accumulated.skipped);
}

/// The auto-GC worker used to throw its outcome away, so a machine where
/// only the worker ever ran GC had no `gc_stats.json` at all.
#[test]
fn local_gc_records_its_run_with_the_driver_that_ran_it() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().to_path_buf(), None);

    run_gc_local(&config, GcMode::Background).unwrap();
    let stats = crate::report::read_gc_stats(&config.cache_dir).expect("worker run recorded");
    assert_eq!(stats.source, "auto");

    run_gc_local(&config, GcMode::Cli).unwrap();
    let stats = crate::report::read_gc_stats(&config.cache_dir).unwrap();
    assert_eq!(stats.source, "manual");
}

/// A sandbox a crashed hermetic attempt left is gone after any GC run.
#[test]
fn local_gc_sweeps_hermetic_build_script_runs() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().to_path_buf(), None);
    let leftover = config.cache_dir.join("out-dirs/v2").join("ab".repeat(16));
    std::fs::create_dir_all(leftover.join("debug/build/z-1/out")).unwrap();
    run_gc_local(&config, GcMode::Background).unwrap();
    assert!(!leftover.exists());
}

/// `kache purge` removes every shared build-script run but the ones a
/// target directory still links to.
#[cfg(unix)]
#[test]
fn purge_keeps_only_linked_build_script_runs() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    let v2 = config.cache_dir.join("out-dirs/v2");
    let run = |key: &str| {
        let root = v2.join(key.repeat(16));
        std::fs::create_dir_all(root.join("out")).unwrap();
        std::fs::write(root.join(".kache-sealed"), b"{}").unwrap();
        root
    };
    let (linked, unlinked) = (run("ab"), run("cd"));
    let link = dir.path().join("target-out");
    std::os::unix::fs::symlink(linked.join("out"), &link).unwrap();
    let mut line = link.as_os_str().as_encoded_bytes().to_vec();
    line.push(b'\n');
    std::fs::write(linked.with_extension("refs"), line).unwrap();

    purge(&config, None).unwrap();
    assert!(linked.exists(), "a target directory links to it");
    assert!(!unlinked.exists());
}

/// Every driver records through record_gc_run; the local one shows the
/// history is wired in and stays off until record_sessions asks for it.
#[test]
fn local_gc_appends_to_the_gc_history_only_when_record_sessions_is_on() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = save_manifest_config(dir.path().to_path_buf(), None);

    run_gc_local(&config, GcMode::Cli).unwrap();
    assert!(!config.cache_dir.join("telemetry").exists());

    config.record_sessions = true;
    run_gc_local(&config, GcMode::Background).unwrap();
    let log = std::fs::read_to_string(crate::report::gc_runs_log_path(&config.cache_dir)).unwrap();
    let records: Vec<crate::report::GcRunRecord> = log
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(records.len(), 1, "{log}");
    assert_eq!(records[0].source, "auto");
}

/// kunobi-ninja/kache#1126: the local sweep removes stale key locks and
/// records the housekeeping counts.
#[test]
fn local_gc_runs_store_housekeeping_and_records_it() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().to_path_buf(), None);

    // Two stale key locks with no entry, one claimed just now.
    std::fs::create_dir_all(config.store_dir()).unwrap();
    let lock_path = |seed: u8| {
        config
            .store_dir()
            .join(format!("{}.lock", blake3::hash(&[seed]).to_hex()))
    };
    for seed in [1, 2, 3] {
        std::fs::write(lock_path(seed), b"1").unwrap();
    }
    for seed in [1, 2] {
        std::fs::OpenOptions::new()
            .write(true)
            .open(lock_path(seed))
            .unwrap()
            .set_modified(std::time::SystemTime::now() - std::time::Duration::from_secs(7200))
            .unwrap();
    }

    let stats = run_gc_local(&config, GcMode::Background).unwrap();
    assert_eq!(
        stats.housekeeping,
        Some(crate::store::HousekeepingStats {
            key_locks_removed: 2,
            key_locks_remaining: 1,
            predictions_pruned: 0,
            file_hashes_pruned: 0,
        })
    );
    assert!(!lock_path(1).exists());
    assert!(!lock_path(2).exists());
    assert!(lock_path(3).exists());
    let recorded = crate::report::read_gc_stats(&config.cache_dir).expect("run recorded");
    assert_eq!(recorded.key_locks_removed, Some(2));
    assert_eq!(recorded.key_locks_remaining, Some(1));
    assert_eq!(recorded.predictions_pruned, Some(0));
    assert_eq!(recorded.file_hashes_pruned, Some(0));
}

#[test]
fn stale_schema_gc_records_its_run() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().to_path_buf(), None);

    gc(&config, None, true, true).unwrap();

    let stats = crate::report::read_gc_stats(&config.cache_dir).expect("stale-schema run recorded");
    assert_eq!(stats.source, "manual");
}

/// `kache gc --max-age` with no reachable daemon evicts locally; that run
/// counts like any other.
#[test]
fn age_gc_without_a_daemon_records_its_run() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().to_path_buf(), None);
    let store = Store::open(&config).unwrap();
    let _gc_lock = store.try_gc_lock().unwrap().expect("gc lock");

    evict_older_than_recorded(&store, &config, 24).unwrap();

    let stats = crate::report::read_gc_stats(&config.cache_dir).expect("age run recorded");
    assert_eq!(stats.source, "manual");
}

/// An idle `size`-byte entry the store holds alone.
fn put_idle_entry(store: &Store, dir: &std::path::Path, key: &str, size: usize) {
    let src = dir.join(format!("{key}.o"));
    std::fs::write(&src, &key.as_bytes().repeat(size)[..size]).unwrap();
    store
        .put(
            key,
            "test-crate",
            &[],
            &[],
            "host",
            "dev",
            &[(src.clone(), format!("{key}.o"))],
            "",
            "",
        )
        .unwrap();
    store.remove_clone_for_test(&src);
    store.set_last_accessed_for_test(key, "-1 hour");
}

/// A main config with one `[cache.volumes]` shard of `budget` bytes whose
/// store holds two idle 600-byte entries, and one mapped shard that does
/// not exist.
fn config_with_an_over_budget_shard(dir: &std::path::Path, budget: u64) -> (Config, Config, Store) {
    let mut config = save_manifest_config(dir.join("main"), None);
    let shard = crate::config::VolumeStore {
        volume: "/mnt/shard/".into(),
        store: dir.join("shard"),
        max_size: Some(budget),
    };
    let shard_config = config.for_volume_store(&shard, |_| None);
    config.volume_stores = vec![
        crate::config::VolumeStore {
            volume: "/mnt/gone/".into(),
            store: dir.join("unmounted"),
            max_size: None,
        },
        shard,
    ];
    let store = Store::open(&shard_config).unwrap();
    put_idle_entry(&store, dir, "shard_a", 600);
    put_idle_entry(&store, dir, "shard_b", 600);
    (config, shard_config, store)
}

/// kunobi-ninja/kache#974: `kache gc` without a daemon sweeps each shard
/// against its own budget, skips a missing one, and records the shard's
/// run in the shard.
#[test]
fn local_gc_sweeps_each_shard_against_its_own_budget() {
    let dir = tempfile::tempdir().unwrap();
    let (config, shard_config, shard) = config_with_an_over_budget_shard(dir.path(), 1000);
    let main = Store::open(&config).unwrap();
    put_idle_entry(&main, dir.path(), "main_entry", 1200);

    let stats = run_gc_local_with_shards(&config, GcMode::Cli).unwrap();
    assert_eq!(stats.entries_evicted, 0, "the stats are the main store's");
    assert!(main.contains("main_entry"));
    assert_eq!(shard.physical_size().unwrap(), 600);
    let recorded = crate::report::read_gc_stats(&shard_config.cache_dir).unwrap();
    assert_eq!(
        (recorded.source.as_str(), recorded.entries_evicted),
        ("manual", 1)
    );
    assert!(!dir.path().join("unmounted").exists());

    let dir = tempfile::tempdir().unwrap();
    let (config, _, shard) = config_with_an_over_budget_shard(dir.path(), 1_000_000);
    run_gc_local_with_shards(&config, GcMode::Background).unwrap();
    assert_eq!(
        shard.physical_size().unwrap(),
        1200,
        "under budget: left alone"
    );
}

/// Each store has its own gc.lock: a busy main store does not stop the
/// shards from being swept, and a busy shard is left to its holder.
#[test]
fn local_gc_takes_each_stores_lock_on_its_own() {
    let dir = tempfile::tempdir().unwrap();
    let (config, _, shard) = config_with_an_over_budget_shard(dir.path(), 1000);
    let main = Store::open(&config).unwrap();
    let main_lock = main.try_gc_lock().unwrap().expect("gc lock");
    let shard_lock = shard.try_gc_lock().unwrap().expect("gc lock");

    let stats = run_gc_local_with_shards(&config, GcMode::Background).unwrap();
    assert!(stats.skipped);
    assert_eq!(
        shard.physical_size().unwrap(),
        1200,
        "the shard's lock is held"
    );

    drop(shard_lock);
    run_gc_local_with_shards(&config, GcMode::Background).unwrap();
    assert_eq!(
        shard.physical_size().unwrap(),
        600,
        "only the main lock is held"
    );
    drop(main_lock);
}

#[test]
fn only_a_printing_sweep_names_the_shard_first() {
    let shard = std::path::Path::new("/mnt/shard");
    assert_eq!(
        shard_sweep_heading(GcMode::Cli, shard).as_deref(),
        Some("Volume shard /mnt/shard:")
    );
    assert_eq!(shard_sweep_heading(GcMode::Background, shard), None);
}

/// The auto-GC worker a build spawns with no daemon sweeps shards too,
/// each against its own trigger.
#[test]
fn the_auto_gc_worker_sweeps_an_over_budget_shard() {
    let dir = tempfile::tempdir().unwrap();
    let (config, shard_config, shard) = config_with_an_over_budget_shard(dir.path(), 1000);

    run_auto_gc_workers(&config, std::time::Duration::ZERO);
    assert_eq!(shard.physical_size().unwrap(), 600);
    let recorded = crate::report::read_gc_stats(&shard_config.cache_dir).unwrap();
    assert_eq!(recorded.source, "auto");
    assert!(!dir.path().join("unmounted").exists());
}

#[test]
fn age_gc_without_a_daemon_reaches_shards_whose_lock_is_free() {
    let dir = tempfile::tempdir().unwrap();
    let (config, shard_config, shard) = config_with_an_over_budget_shard(dir.path(), 1_000_000);
    shard.set_last_accessed_for_test("shard_a", "-48 hours");

    let held = shard.try_gc_lock().unwrap().expect("gc lock");
    let stats = evict_shards_older_than(&config, 24);
    assert_eq!(stats.len(), 1);
    assert!(stats[0].skipped);
    assert!(shard.contains("shard_a"));
    drop(held);

    let stats = evict_shards_older_than(&config, 24);
    assert_eq!(stats[0].entries_evicted, 1);
    assert!(!shard.contains("shard_a") && shard.contains("shard_b"));
    let recorded = crate::report::read_gc_stats(&shard_config.cache_dir).unwrap();
    assert_eq!(recorded.source, "manual");
}

#[test]
fn the_gc_summary_has_a_line_per_shard_it_swept() {
    let dir = tempfile::tempdir().unwrap();
    let (config, shard_config, shard) = config_with_an_over_budget_shard(dir.path(), 1000);
    assert_eq!(
        shard_store_lines(&config),
        vec![format!(
            "Volume shard {}: {} / {} (2 entries)",
            shard_config.cache_dir.display(),
            ByteSize(shard.total_size().unwrap()),
            ByteSize(1000),
        )]
    );
}

#[test]
fn machine_snapshot_reads_a_store_without_creating_one() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().to_path_buf(), None);

    let empty = machine_snapshot(&config);
    assert!(empty.index_bytes.is_none());
    assert!(empty.wal_bytes.is_none());
    assert!(empty.rowid_high_water.is_empty());
    assert!(empty.gc.is_none());
    assert!(
        !config.index_db_path().exists(),
        "a snapshot must never create the index"
    );

    let store = Store::open(&config).unwrap();
    let artifact = dir.path().join("physical-size.rlib");
    std::fs::write(&artifact, b"payload").unwrap();
    store
        .put(
            "physical-size",
            "physical_size",
            &["lib".to_string()],
            &[],
            "",
            "dev",
            &[(artifact, "libphysical_size.rlib".to_string())],
            "",
            "",
        )
        .unwrap();
    drop(store);
    crate::report::record_gc_run(&config, "auto", &crate::store::GcStats::default()).unwrap();
    let snap = machine_snapshot(&config);
    let db_len = std::fs::metadata(config.index_db_path()).unwrap().len();
    let mut wal_path = config.index_db_path().into_os_string();
    wal_path.push("-wal");
    let wal_len = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
    assert_eq!(snap.wal_bytes, Some(wal_len));
    assert_eq!(snap.index_bytes, Some(db_len + wal_len));
    assert_eq!(snap.store_physical_bytes, Some(7));
    assert_eq!(snap.index_free_bytes, Some(0), "a fresh index has no holes");
    assert_eq!(
        snap.blob_drift,
        Some(crate::store::BlobRefcountDrift::default())
    );
    let db = rusqlite::Connection::open(config.index_db_path()).unwrap();
    db.execute_batch(
        "UPDATE blobs SET refcount = 5;
             INSERT INTO blobs (hash, size, refcount) VALUES ('unowned', 4096, 1);",
    )
    .unwrap();
    drop(db);
    assert_eq!(
        machine_snapshot(&config).blob_drift,
        Some(crate::store::BlobRefcountDrift {
            unowned: 1,
            unowned_bytes: 4096,
            too_high: 1,
            too_high_bytes: 7,
            ..Default::default()
        })
    );
    let tables: Vec<_> = snap
        .rowid_high_water
        .iter()
        .map(|(table, _)| *table)
        .collect();
    assert!(
        tables.contains(&"entries") && tables.contains(&"blobs"),
        "{tables:?}"
    );
    assert_eq!(snap.gc.expect("gc_stats.json read").source, "auto");
}

/// The last read-write connection to a WAL database checkpoints on close,
/// copying the WAL into `index.db`: a write, on a 27 GiB file, that a
/// snapshot must never make. A read-only connection leaves both files as
/// it found them.
#[test]
fn machine_snapshot_never_checkpoints_the_wal() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().to_path_buf(), None);
    drop(Store::open(&config).unwrap());
    let db_path = config.index_db_path();
    let mut wal_path = db_path.clone().into_os_string();
    wal_path.push("-wal");
    {
        // A writer that exits without checkpointing, as a killed build does.
        let writer = rusqlite::Connection::open(&db_path).unwrap();
        writer
            .set_db_config(
                rusqlite::config::DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
                true,
            )
            .unwrap();
        writer
            .execute_batch(
                "PRAGMA wal_autocheckpoint = 0;
                     CREATE TABLE probe (x INTEGER);
                     INSERT INTO probe VALUES (1);",
            )
            .unwrap();
    }
    let db_before = std::fs::read(&db_path).unwrap();
    let wal_before = std::fs::metadata(&wal_path).unwrap().len();
    assert!(wal_before > 0, "the setup leaves frames in the WAL");

    machine_snapshot(&config);

    assert_eq!(
        std::fs::read(&db_path).unwrap(),
        db_before,
        "index.db must not be written"
    );
    assert_eq!(
        std::fs::metadata(&wal_path).map(|m| m.len()).ok(),
        Some(wal_before),
        "the WAL must be left in place"
    );
}

/// The GC line grows a suffix only when there is something to report:
/// zero failures and zero write time add nothing, one of each adds both.
/// A record without a driver predates `source`, when only the daemon
/// wrote one.
#[test]
fn stats_gc_line_suffixes_start_at_one() {
    let gc_line = |failed: usize, locked: usize, write_ms: u64, source: &str| {
        machine_lines(&crate::otel::MachineSnapshot {
            gc: Some(crate::report::GcStatsPersisted {
                last_run: "2026-09-12T12:11:05+00:00".to_string(),
                source: source.to_string(),
                entries_evicted: 3,
                entries_failed: failed,
                entries_locked: locked,
                evict_write_ms: write_ms,
                ..Default::default()
            }),
            ..Default::default()
        })
    };
    assert_eq!(
        gc_line(0, 0, 0, "manual"),
        vec!["GC:        last run 2026-09-12T12:11:05+00:00 (manual): 3 evicted".to_string()]
    );
    assert_eq!(
            gc_line(1, 1, 1, ""),
            vec![
                "GC:        last run 2026-09-12T12:11:05+00:00 (daemon): 3 evicted, 1 failed (1 lost the index write lock), 1 ms in index writes"
                    .to_string()
            ]
        );
    let index_only = machine_lines(&crate::otel::MachineSnapshot {
        index_bytes: Some(4096),
        ..Default::default()
    });
    assert_eq!(index_only, vec![format!("Index:     {}", ByteSize(4096))]);
}

#[test]
fn stats_lines_show_the_index_and_a_gc_that_keeps_losing_the_lock() {
    let machine = crate::otel::MachineSnapshot {
        store_physical_bytes: None,
        index_bytes: Some(29_074_419_712),
        wal_bytes: Some(1_073_741_824),
        index_free_bytes: None,
        blob_drift: None,
        rowid_high_water: vec![
            ("entries", 2),
            ("file_hashes", 13_286_285),
            ("cc_preprocess_memos", 874_517),
            ("eviction_tombstones", 5_425_819),
            ("blobs", 5),
        ],
        gc: Some(crate::report::GcStatsPersisted {
            last_run: "2026-09-12T12:11:05+00:00".to_string(),
            source: "auto".to_string(),
            entries_failed: 40,
            entries_locked: 40,
            evict_write_ms: 4200,
            ..Default::default()
        }),
    };
    let lines = machine_lines(&machine);
    assert!(lines[0].starts_with("Index:"), "{lines:?}");
    assert!(
        lines[0].contains(&format!(
            "{}, WAL {} (rowid high-water:",
            ByteSize(29_074_419_712),
            ByteSize(1_073_741_824)
        )),
        "{lines:?}"
    );
    assert!(
        lines[0].contains("(rowid high-water: file_hashes 13286285"),
        "{lines:?}"
    );
    assert!(
        !lines[0].contains("blobs"),
        "only the three largest tables: {lines:?}"
    );
    assert!(lines[1].contains("(auto)"), "{lines:?}");
    assert!(
        lines[1].contains("40 lost the index write lock"),
        "{lines:?}"
    );
    assert!(lines[1].ends_with(", 4200 ms in index writes"), "{lines:?}");
    assert_eq!(lines.len(), 2, "{lines:?}");
    assert!(machine_lines(&crate::otel::MachineSnapshot::default()).is_empty());
}

/// Dropping a table leaves its pages on the freelist until a compaction.
#[test]
fn index_free_bytes_counts_freelist_pages() {
    let db = rusqlite::Connection::open_in_memory().unwrap();
    db.execute_batch("PRAGMA page_size = 4096; CREATE TABLE keep (v BLOB);")
        .unwrap();
    assert_eq!(index_free_bytes(&db), Some(0));

    db.execute_batch(
        "CREATE TABLE junk (v BLOB);
             INSERT INTO junk VALUES (zeroblob(65536));
             DROP TABLE junk;",
    )
    .unwrap();
    let pages: i64 = db
        .query_row("PRAGMA freelist_count", [], |row| row.get(0))
        .unwrap();
    assert!(
        pages >= 16,
        "a 64 KiB blob spans at least 16 pages: {pages}"
    );
    assert_eq!(index_free_bytes(&db), Some(pages as u64 * 4096));

    db.execute_batch("VACUUM").unwrap();
    assert_eq!(index_free_bytes(&db), Some(0));
}

#[test]
fn free_page_bytes_multiplies_and_rejects_nonsense() {
    assert_eq!(free_page_bytes(3, 4096), Some(12_288));
    assert_eq!(free_page_bytes(0, 4096), Some(0));
    assert_eq!(free_page_bytes(-1, 4096), None);
    assert_eq!(free_page_bytes(1, -4096), None);
    assert_eq!(free_page_bytes(i64::MAX, 4096), None);
}

/// Why the figure is not called rows: `INSERT OR REPLACE` on a TEXT key,
/// the way `file_hashes` and the memo tables are written, deletes the old
/// row and inserts the new one at the next rowid.
#[test]
fn rowid_high_water_counts_replacements_not_rows() {
    let db = rusqlite::Connection::open_in_memory().unwrap();
    db.execute_batch("CREATE TABLE file_hashes (path TEXT PRIMARY KEY, hash TEXT)")
        .unwrap();
    for hash in ["a", "b", "c"] {
        db.execute(
            "INSERT OR REPLACE INTO file_hashes VALUES ('src/lib.rs', ?1)",
            [hash],
        )
        .unwrap();
    }
    let high_water: i64 = db
        .query_row(&rowid_high_water_sql("file_hashes"), [], |row| row.get(0))
        .unwrap();
    let rows: i64 = db
        .query_row("SELECT COUNT(*) FROM file_hashes", [], |row| row.get(0))
        .unwrap();
    assert_eq!((high_water, rows), (3, 1));
}

/// The rowid high-water mark on a 27 GiB index must not read the table.
/// The query seeks to the last row (`Last`) and stops, so its step count does
/// not grow with the table; a scan steps once per row, and `COUNT(*)` is a
/// single `Count` step that still reads every page.
#[test]
fn index_row_figures_seek_instead_of_scanning() {
    let db = rusqlite::Connection::open_in_memory().unwrap();
    db.execute_batch(
        "CREATE TABLE memos (key TEXT PRIMARY KEY, body BLOB);
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 5000)
             INSERT INTO memos SELECT 'k' || i, zeroblob(64) FROM n;",
    )
    .unwrap();

    let mut stmt = db.prepare(&rowid_high_water_sql("memos")).unwrap();
    let top: i64 = stmt.query_row([], |row| row.get(0)).unwrap();
    assert_eq!(top, 5000);
    let steps = stmt.get_status(rusqlite::StatementStatus::VmStep);
    assert!(steps < 100, "{steps} VM steps for one high-water mark");

    let mut explain = db
        .prepare(&format!("EXPLAIN {}", rowid_high_water_sql("memos")))
        .unwrap();
    let opcodes: Vec<String> = explain
        .query_map([], |row| row.get(1))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert!(opcodes.iter().any(|op| op == "Last"), "{opcodes:?}");
    assert!(!opcodes.iter().any(|op| op == "Count"), "{opcodes:?}");
}

/// `kache stats` must not stall behind the index. The store's own busy
/// timeout is 5 s per statement; the snapshot gives up after 25 ms and
/// drops the figures it could not read.
#[test]
fn machine_snapshot_skips_row_figures_on_a_locked_index() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().to_path_buf(), None);
    drop(Store::open(&config).unwrap());
    let holder = rusqlite::Connection::open(config.index_db_path()).unwrap();
    holder
        .execute_batch(
            "PRAGMA locking_mode = EXCLUSIVE; BEGIN EXCLUSIVE; \
                 DELETE FROM entries WHERE 0;",
        )
        .unwrap();

    let started = std::time::Instant::now();
    let snap = machine_snapshot(&config);
    let waited = started.elapsed();

    assert!(snap.index_bytes.is_some(), "file sizes need no lock");
    assert!(
        snap.rowid_high_water.is_empty(),
        "{:?}",
        snap.rowid_high_water
    );
    assert!(
        waited < std::time::Duration::from_secs(4),
        "waited {waited:?} on a locked index"
    );
    holder.execute_batch("COMMIT").unwrap();
}

fn report_run(config: &Config, out: &std::path::Path, record: bool) {
    report(
        config,
        "json",
        SinceWindow::DEFAULT,
        crate::report::ReportFilter {
            root: Some(std::path::PathBuf::from("/ci/runner-3/_work/secret-repo")),
            last_build: false,
        },
        Some(out.join("report.json")),
        10,
        record,
    )
    .unwrap();
}

/// Every file under `dir` with its contents.
fn tree_contents(dir: &std::path::Path) -> Vec<(std::path::PathBuf, Vec<u8>)> {
    let mut files = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(&next).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else {
                let body = std::fs::read(&path).unwrap();
                files.push((path, body));
            }
        }
    }
    files.sort();
    files
}

/// `record_sessions` (env or config) makes a plain `kache report` record
/// exactly as `--record` would, so a host can opt in once for every job.
#[test]
fn report_records_without_the_flag_when_record_sessions_is_on() {
    let cache = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let mut config = save_manifest_config(cache.path().to_path_buf(), None);
    config.record_sessions = true;

    report_run(&config, out.path(), false);

    let log = std::fs::read_to_string(session_log_path(&config)).unwrap();
    assert_eq!(log.lines().count(), 1, "{log}");
    let record: crate::report::SessionRecord =
        serde_json::from_str(log.lines().next().unwrap()).unwrap();
    assert_eq!(record.schema, crate::report::SESSION_RECORD_SCHEMA);
}

/// Recording is opt-in: a plain `kache report` leaves the cache dir
/// exactly as it found it.
#[test]
fn report_without_record_leaves_the_cache_dir_unchanged() {
    let cache = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let config = save_manifest_config(cache.path().to_path_buf(), None);
    drop(Store::open(&config).unwrap());
    let before = tree_contents(cache.path());

    report_run(&config, out.path(), false);

    assert!(out.path().join("report.json").is_file());
    assert!(!cache.path().join("telemetry").exists());
    assert_eq!(tree_contents(cache.path()), before);
}

/// The report reads events from the runtime dir, which CI deletes with the
/// job, so a recorded session has to land in the cache dir, without the
/// path.
#[test]
fn report_record_appends_one_line_to_the_cache_dir_not_the_runtime_dir() {
    let cache = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let mut config = save_manifest_config(cache.path().to_path_buf(), None);
    config.runtime_dir = runtime.path().to_path_buf();
    let line_count = || {
        std::fs::read_to_string(session_log_path(&config))
            .unwrap()
            .lines()
            .count()
    };

    report_run(&config, out.path(), true);
    assert_eq!(line_count(), 1, "one line per recorded session");
    report_run(&config, out.path(), true);
    assert_eq!(line_count(), 2, "a second record appends, never rewrites");

    assert!(!runtime.path().join("telemetry").exists());
    let log = std::fs::read_to_string(session_log_path(&config)).unwrap();
    let lines: Vec<_> = log.lines().collect();
    assert!(
        !log.contains("secret-repo"),
        "the root is hashed, never written"
    );
    let record: crate::report::SessionRecord = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(record.schema, crate::report::SESSION_RECORD_SCHEMA);
    assert_eq!(record.root_hash.as_deref().map(str::len), Some(16));
    assert_eq!(record.summary.total_crates, 0);
    assert_eq!(record.machine.store_max, config.max_size);
}

/// Recording is a side effect. A cache dir it cannot write (a read-only
/// mount, another owner) costs the session line, never the report.
#[test]
fn report_record_that_cannot_write_still_delivers_the_report() {
    let cache = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let config = save_manifest_config(cache.path().to_path_buf(), None);
    // A file where the telemetry dir goes fails create_dir_all the way a
    // read-only mount does, and root cannot write past it.
    std::fs::write(cache.path().join("telemetry"), b"").unwrap();

    let result = report(
        &config,
        "json",
        SinceWindow::DEFAULT,
        crate::report::ReportFilter {
            root: None,
            last_build: false,
        },
        Some(out.path().join("report.json")),
        10,
        true,
    );

    assert!(result.is_ok(), "{result:?}");
    assert!(out.path().join("report.json").is_file());
}

#[test]
fn evicting_nothing_because_everything_is_pinned_explains_itself() {
    // The #509 report: `evicted 0 entries` printed next to a store at 912%.
    // The number is right and the message is useless, so the user concludes
    // GC is broken. The output must name the grace and say what to do.
    let msg = describe_eviction(&gc_stats(0, 24, 0), true);
    assert!(
        msg.contains("24"),
        "must say how many were held back: {msg}"
    );
    assert!(
        msg.contains("120s"),
        "must name the grace period so the wait is bounded and knowable: {msg}"
    );
    assert!(
        msg.contains("Re-run"),
        "must tell the user what to do next: {msg}"
    );
    assert!(
        msg.contains("durable remote upload"),
        "the shared pin counter must describe upload-backed entries too: {msg}"
    );
}

#[test]
fn evicting_nothing_while_over_limit_still_explains_the_grace() {
    // Nothing pinned and nothing evicted, but still over budget: the user
    // needs to know the idle rule exists, or "0" reads as a broken GC.
    let msg = describe_eviction(&gc_stats(0, 0, 0), true);
    assert!(msg.contains("over its limit"), "{msg}");
    assert!(msg.contains("120s"), "{msg}");
}

#[test]
fn an_empty_store_does_not_imply_something_is_wrong() {
    // Under budget with nothing to do is the normal case and must not
    // inherit the alarming phrasing of the over-limit one.
    let msg = describe_eviction(&gc_stats(0, 0, 0), false);
    assert!(msg.contains("nothing to evict"), "{msg}");
    assert!(
        !msg.contains("over its limit"),
        "a healthy store must not be told it is over budget: {msg}"
    );
}

#[test]
fn a_successful_eviction_reports_bytes_and_still_flags_pinned_entries() {
    let mut stats = gc_stats(12, 3, 5 * 1024 * 1024);
    stats.disk_bytes_reclaimed = 4 * 1024 * 1024;
    stats.entries_unreclaimable = 2;
    let msg = describe_eviction(&stats, false);
    assert!(msg.contains("12 entries"), "{msg}");
    assert!(msg.contains("MiB") || msg.contains("MB"), "{msg}");
    assert!(
        msg.contains('3'),
        "entries left behind matter even on a successful sweep — they are \
            why the store may still be over budget: {msg}"
    );
    assert!(msg.contains("became free on disk"), "{msg}");
    assert!(msg.contains("is still held by clones"), "{msg}");
    assert!(msg.contains("2 entries left in place"), "{msg}");
}

#[test]
fn fully_retained_eviction_has_a_cleanup_path() {
    let mut stats = gc_stats(0, 0, 0);
    stats.entries_unreclaimable = 1;
    let msg = describe_eviction(&stats, false);
    assert!(msg.contains("1 entry cloned"), "{msg}");
    assert!(msg.contains("clean --tracked"), "{msg}");

    stats.bytes_held = 3 * 1024 * 1024 * 1024;
    let msg = describe_eviction(&stats, false);
    assert!(msg.contains("1 entry (3.0 GiB) cloned"), "{msg}");
}

#[test]
fn a_successful_eviction_sizes_the_entries_clones_hold() {
    let mut stats = gc_stats(1, 0, 1024);
    stats.entries_unreclaimable = 2;
    stats.bytes_held = 2048;
    let msg = describe_eviction(&stats, false);
    assert!(msg.contains("2 entries (2.0 KiB) left in place"), "{msg}");
}

#[test]
fn eviction_messages_are_singular_for_one_entry() {
    let mut stats = gc_stats(1, 0, 1024);
    stats.disk_bytes_reclaimed = 1024;
    let msg = describe_eviction(&stats, false);
    assert!(msg.contains("1 entry"), "{msg}");
    assert!(!msg.contains("1 entries"), "{msg}");
    assert!(!msg.contains("is still held by clones"), "{msg}");
    assert!(!msg.contains("more 0 entries"), "{msg}");
    assert!(!msg.contains("0 entries left in place"), "{msg}");
}

#[test]
fn a_clean_sweep_says_nothing_about_entries_left_behind() {
    // Nothing pinned: the grace-period paragraph would be noise at best,
    // and at worst reads as "some entries are stuck" on a sweep that
    // emptied everything it was asked to.
    let msg = describe_eviction(&gc_stats(7, 0, 4096), false);
    assert!(msg.contains("7 entries"), "{msg}");
    assert!(
        !msg.contains("in use within the last"),
        "no entries were held back, so the grace note must not appear: {msg}"
    );
}

#[test]
fn over_limit_is_decided_by_a_strict_comparison_against_the_budget() {
    assert!(store_over_limit(Some(1025), 1024));
    assert!(
        !store_over_limit(Some(1024), 1024),
        "exactly at budget is within it: a store that fits must not be told \
             it is over"
    );
    assert!(!store_over_limit(Some(512), 1024));
    assert!(
        !store_over_limit(None, 1024),
        "an unreadable size must not be reported as over budget — that sends \
             the user chasing an eviction problem they may not have"
    );
}

/// Hardlink identity is tracked separately from extent sharing so callers
/// can distinguish an external survivor from two names inside one target/.
#[cfg(unix)]
#[test]
fn a_hardlinked_artifact_records_its_link_group() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("blob.bin");
    let linked = dir.path().join("target-copy.bin");
    fs::write(&blob, vec![0u8; 4096]).unwrap();
    fs::hard_link(&blob, &linked).unwrap();

    let meta = fs::metadata(&linked).unwrap();
    let observation = observe_storage(&linked, &meta);
    let hardlink = observation.hardlink.expect("nlink > 1 records a group");
    assert_eq!(hardlink.total_links, 2);
    let mut reclaim = ReclaimEstimator::default();
    reclaim.record(meta.len(), observation);
    assert_eq!(reclaim.estimated_reclaimable_bytes(), 0);
    assert!(reclaim.hardlink_has_external_ref(hardlink.id));

    let plain = dir.path().join("plain.bin");
    fs::write(&plain, vec![0u8; 4096]).unwrap();
    let plain_meta = fs::metadata(&plain).unwrap();
    let plain_observation = observe_storage(&plain, &plain_meta);
    assert!(plain_observation.hardlink.is_none());
    assert!(!plain_observation.sharing.shared);
}

fn zero_event_stats() -> daemon::EventStatsResponse {
    daemon::EventStatsResponse {
        local_hits: 0,
        prefetch_hits: 0,
        remote_hits: 0,
        dups: 0,
        misses: 0,
        errors: 0,
        total_elapsed_ms: 0,
        hit_elapsed_ms: 0,
        miss_elapsed_ms: 0,
        hit_compile_time_ms: 0,
        miss_compile_time_ms: 0,
        store_output_blobs: 0,
        store_duplicate_blobs: 0,
        store_new_blobs: 0,
    }
}

#[test]
fn test_daemon_needed_when_remote_configured() {
    assert!(daemon_needed(true, false));
}

#[test]
fn test_daemon_needed_when_planner_configured() {
    assert!(daemon_needed(false, true));
}

#[test]
fn test_daemon_needed_when_both_configured() {
    assert!(daemon_needed(true, true));
}

#[test]
fn test_daemon_not_needed_when_local_only_or_unconfigured() {
    assert!(!daemon_needed(false, false));
}

#[test]
fn test_optional_failing_check_is_not_an_issue() {
    // A downgraded (optional) daemon check that failed must not count.
    assert!(!is_doctor_issue(false, true));
}

#[test]
fn test_genuine_failing_check_is_an_issue() {
    assert!(is_doctor_issue(false, false));
}

#[test]
fn test_passing_check_is_never_an_issue() {
    assert!(!is_doctor_issue(true, false));
    assert!(!is_doctor_issue(true, true));
}

#[test]
fn test_count_hit_rate_zero_total_is_zero() {
    assert_eq!(count_hit_rate(&zero_event_stats()), 0.0);
}

#[test]
fn test_count_hit_rate_counts_all_hit_kinds() {
    let es = daemon::EventStatsResponse {
        local_hits: 3,
        prefetch_hits: 2,
        remote_hits: 1,
        dups: 0,
        misses: 4,
        ..zero_event_stats()
    };
    // (3+2+1) hits / (6+4) total = 60%
    assert!((count_hit_rate(&es) - 60.0).abs() < 1e-9);
}

#[test]
fn test_count_hit_rate_all_hits_is_hundred() {
    let es = daemon::EventStatsResponse {
        local_hits: 5,
        ..zero_event_stats()
    };
    assert!((count_hit_rate(&es) - 100.0).abs() < 1e-9);
}

#[test]
fn test_compile_weighted_hit_rate_none_when_no_compile_time() {
    assert_eq!(compile_weighted_hit_rate(&zero_event_stats()), None);
}

#[test]
fn test_compile_weighted_hit_rate_weights_by_time() {
    let es = daemon::EventStatsResponse {
        hit_compile_time_ms: 750,
        miss_compile_time_ms: 250,
        ..zero_event_stats()
    };
    let r = compile_weighted_hit_rate(&es).unwrap();
    assert!((r - 75.0).abs() < 1e-9);
}

#[test]
fn test_key_short_truncates_long_keys() {
    assert_eq!(key_short("0123456789abcdefghij"), "0123456789ab");
    assert_eq!(key_short("short"), "short");
    // Exactly 12 chars: not truncated (len > 12 is the cutoff).
    assert_eq!(key_short("123456789012"), "123456789012");
}

#[test]
fn test_format_duration_ms_buckets() {
    assert_eq!(format_duration_ms(0), "~0ms");
    assert_eq!(format_duration_ms(500), "~500ms");
    assert_eq!(format_duration_ms(1_000), "~1s");
    assert_eq!(format_duration_ms(59_000), "~59s");
    assert_eq!(format_duration_ms(60_000), "~1min");
    assert_eq!(format_duration_ms(3_600_000), "~1.0h");
    assert_eq!(format_duration_ms(7_200_000), "~2.0h");
}

#[test]
fn test_format_relative_time_invalid_passes_through() {
    assert_eq!(format_relative_time("not a date"), "not a date");
}

#[test]
fn test_format_relative_time_buckets() {
    let now = chrono::Utc::now();
    let fmt = |dt: chrono::DateTime<chrono::Utc>| {
        format_relative_time(&dt.format("%Y-%m-%d %H:%M:%S").to_string())
    };
    assert_eq!(fmt(now - chrono::Duration::seconds(10)), "just now");
    assert_eq!(fmt(now - chrono::Duration::minutes(5)), "5m ago");
    assert_eq!(fmt(now - chrono::Duration::hours(3)), "3h ago");
    assert_eq!(fmt(now - chrono::Duration::days(2)), "2d ago");
    // A future timestamp clamps to "just now" (secs.max(0)).
    assert_eq!(fmt(now + chrono::Duration::hours(1)), "just now");
}

#[test]
fn test_is_binary_artifact_extensions() {
    // Non-binary artifacts
    assert!(!is_binary_artifact(std::path::Path::new("libfoo.d")));
    assert!(!is_binary_artifact(std::path::Path::new("libfoo.rmeta")));
    assert!(!is_binary_artifact(std::path::Path::new("libfoo.rlib")));

    // Binary artifacts
    assert!(is_binary_artifact(std::path::Path::new("myapp")));
    assert!(is_binary_artifact(std::path::Path::new("libfoo.dylib")));
    assert!(is_binary_artifact(std::path::Path::new("libfoo.so")));
    assert!(is_binary_artifact(std::path::Path::new("myapp.exe")));
    assert!(is_binary_artifact(std::path::Path::new("mylib.dll")));

    // Unknown extension defaults to non-binary
    assert!(!is_binary_artifact(std::path::Path::new("file.txt")));
}

#[test]
fn test_detect_profiles_empty() {
    let dir = tempfile::tempdir().unwrap();
    let profiles = detect_profiles(dir.path());
    assert!(profiles.is_empty());
}

#[test]
fn test_detect_profiles_with_dirs() {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir(dir.path().join("debug")).unwrap();
    fs::create_dir(dir.path().join("release")).unwrap();

    let profiles = detect_profiles(dir.path());
    assert!(profiles.contains(&"debug".to_string()));
    assert!(profiles.contains(&"release".to_string()));
    assert!(!profiles.contains(&"profiling".to_string()));
}

#[test]
fn test_detect_profiles_all() {
    let dir = tempfile::tempdir().unwrap();
    for name in &["debug", "release", "profiling", "coverage"] {
        fs::create_dir(dir.path().join(name)).unwrap();
    }

    let profiles = detect_profiles(dir.path());
    assert_eq!(profiles.len(), 4);
}

#[test]
fn profiles_are_found_by_what_cargo_wrote_into_them() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path();
    fs::create_dir_all(target.join("release")).unwrap();
    fs::create_dir_all(target.join("ci/.fingerprint")).unwrap();
    fs::create_dir_all(target.join("x86_64-unknown-linux-gnu/release/deps")).unwrap();
    fs::create_dir_all(target.join("aarch64-apple-darwin/debug")).unwrap();
    fs::create_dir_all(target.join("doc/some_crate")).unwrap();
    fs::create_dir_all(target.join("tmp")).unwrap();

    assert_eq!(
        detect_profiles(target),
        [
            "release",
            "aarch64-apple-darwin/debug",
            "ci",
            "x86_64-unknown-linux-gnu/release",
        ]
    );

    fs::write(
        target.join("x86_64-unknown-linux-gnu/release/deps/libfoo.rlib"),
        vec![0u8; 4096],
    )
    .unwrap();
    let (stats, _) = compute_project_stats(target);
    assert!(
        stats.total_bytes >= 4096,
        "cross-compiled output is counted"
    );
}

#[test]
fn targets_inside_worktree_containers_are_found() {
    let dir = tempfile::tempdir().unwrap();
    for workspace in [
        ".worktrees/feature",
        ".claude/worktrees/agent-1",
        ".cache/other",
    ] {
        let root = dir.path().join(workspace);
        fs::create_dir_all(root.join("target/debug")).unwrap();
        fs::write(root.join("Cargo.toml"), "[package]\nname = \"x\"\n").unwrap();
        fs::write(root.join("target/debug/out.rlib"), vec![0u8; 1024]).unwrap();
    }
    let mut results = Vec::new();
    find_target_dirs(dir.path(), &mut results);
    let mut found: Vec<_> = results
        .iter()
        .map(|entry| entry.path.strip_prefix(dir.path()).unwrap().to_path_buf())
        .collect();
    found.sort();
    assert_eq!(
        found,
        [
            std::path::PathBuf::from(".claude/worktrees/agent-1/target"),
            std::path::PathBuf::from(".worktrees/feature/target"),
        ]
    );
}

#[test]
fn test_sccache_program_detection_accepts_paths() {
    assert!(is_sccache_program("sccache"));
    assert!(is_sccache_program("/opt/homebrew/bin/sccache"));
    assert!(is_sccache_program("sccache.exe"));
    assert!(!is_sccache_program("kache"));
    assert!(!is_sccache_program("sccache-wrapper"));
}

#[test]
fn test_sccache_rc_detection_ignores_fallback_setting() {
    assert!(!active_sccache_migration_line("# RUSTC_WRAPPER=sccache"));
    assert!(!active_sccache_migration_line(
        "export KACHE_FALLBACK=sccache"
    ));
    assert!(active_sccache_migration_line(
        "export RUSTC_WRAPPER=sccache"
    ));
    assert!(active_sccache_migration_line("rustc-wrapper = \"sccache\""));
}

#[test]
fn test_fallback_is_sccache() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = save_manifest_config(dir.path().to_path_buf(), None);

    // No config / no fallback -> false.
    assert!(!fallback_is_sccache(None));
    assert!(!fallback_is_sccache(Some(&cfg)));

    // Fallback set to an sccache binary (incl. a full path) -> true.
    cfg.fallback = Some("sccache".to_string());
    assert!(fallback_is_sccache(Some(&cfg)));
    cfg.fallback = Some("/usr/local/bin/sccache".to_string());
    assert!(fallback_is_sccache(Some(&cfg)));

    // A non-sccache fallback -> false.
    cfg.fallback = Some("/usr/bin/gcc".to_string());
    assert!(!fallback_is_sccache(Some(&cfg)));
}

#[test]
fn test_dir_size_empty() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(dir_size(dir.path()), 0);
}

#[test]
fn test_dir_size_with_files() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("a.txt"), vec![0u8; 100]).unwrap();
    fs::write(dir.path().join("b.txt"), vec![0u8; 200]).unwrap();

    let size = dir_size(dir.path());
    assert!(size >= 300, "expected >= 300, got {}", size);
}

#[test]
fn test_dir_size_recursive() {
    let dir = tempfile::tempdir().unwrap();
    let sub = dir.path().join("sub");
    fs::create_dir(&sub).unwrap();
    fs::write(sub.join("file.txt"), vec![0u8; 50]).unwrap();

    let size = dir_size(dir.path());
    assert!(size >= 50);
}

#[test]
fn test_dir_size_nonexistent() {
    assert_eq!(dir_size(std::path::Path::new("/nonexistent/path")), 0);
}

#[test]
fn test_find_target_dirs_empty() {
    let dir = tempfile::tempdir().unwrap();
    let mut results = Vec::new();
    find_target_dirs(dir.path(), &mut results);
    assert!(results.is_empty());
}

#[test]
fn test_find_target_dirs_with_cargo_project() {
    let dir = tempfile::tempdir().unwrap();
    let project = dir.path().join("myproject");
    fs::create_dir(&project).unwrap();
    fs::write(project.join("Cargo.toml"), "[package]\nname = \"test\"").unwrap();

    let target = project.join("target");
    fs::create_dir(&target).unwrap();
    let debug = target.join("debug");
    fs::create_dir(&debug).unwrap();
    fs::write(debug.join("test.rlib"), vec![0u8; 100]).unwrap();

    let mut results = Vec::new();
    find_target_dirs(dir.path(), &mut results);
    assert_eq!(results.len(), 1);
    assert!(results[0].size >= 100);
    assert!(results[0].profiles.contains(&"debug".to_string()));
}

#[test]
fn find_target_dirs_ignores_an_empty_cargo_target() {
    let dir = tempfile::tempdir().unwrap();
    let project = dir.path().join("empty-project");
    fs::create_dir_all(project.join("target")).unwrap();
    fs::write(project.join("Cargo.toml"), "[package]\nname = \"empty\"").unwrap();

    let mut results = Vec::new();
    find_target_dirs(dir.path(), &mut results);

    assert!(
        results.is_empty(),
        "an empty target has no reclaimable bytes and must not be offered for deletion"
    );
}

#[test]
fn test_find_target_dirs_skips_hidden() {
    let dir = tempfile::tempdir().unwrap();
    let hidden = dir.path().join(".hidden");
    fs::create_dir(&hidden).unwrap();
    fs::write(hidden.join("Cargo.toml"), "[package]").unwrap();
    fs::create_dir(hidden.join("target")).unwrap();

    let mut results = Vec::new();
    find_target_dirs(dir.path(), &mut results);
    assert!(results.is_empty());
}

#[test]
fn test_find_target_dirs_skips_node_modules() {
    let dir = tempfile::tempdir().unwrap();
    let nm = dir.path().join("node_modules");
    fs::create_dir(&nm).unwrap();
    fs::write(nm.join("Cargo.toml"), "[package]").unwrap();
    fs::create_dir(nm.join("target")).unwrap();

    let mut results = Vec::new();
    find_target_dirs(dir.path(), &mut results);
    assert!(results.is_empty());
}

#[test]
fn test_compute_project_stats_empty_dir() {
    let dir = tempfile::tempdir().unwrap();
    let (stats, breakdown) = compute_project_stats(dir.path());
    assert_eq!(stats.total_bytes, 0);
    assert_eq!(stats.cached_bytes, 0);
    assert_eq!(stats.estimated_reclaimable_bytes, 0);
    assert_eq!(breakdown.incremental, 0);
}

#[test]
fn reclaim_estimator_uses_private_bytes_for_partial_reflinks() {
    let mut reclaim = ReclaimEstimator::default();
    reclaim.record(
        10_000,
        StorageObservation {
            sharing: crate::sharing::Sharing {
                shared: true,
                private_bytes: 4_000,
                snapshot_bytes: 0,
            },
            hardlink: None,
        },
    );

    assert_eq!(
        reclaim.estimated_reclaimable_bytes(),
        4_000,
        "a partially shared reflink reclaims its private extents, not zero"
    );
}

#[cfg(unix)]
#[test]
fn reclaim_estimator_clamps_private_bytes_to_allocated_storage() {
    assert_eq!(clamp_private_bytes(1 << 30, 1 << 30, Some(0)), 0);
    assert_eq!(clamp_private_bytes(10_000, 8_000, Some(4_096)), 4_096);
    assert_eq!(clamp_private_bytes(10_000, 4_000, Some(8_192)), 4_000);
}

#[test]
fn reclaim_estimator_collapses_internal_hardlink_groups() {
    let id = FileIdentity {
        device: 7,
        inode: 11,
    };
    let observation = StorageObservation {
        sharing: crate::sharing::Sharing::unknown_for(100),
        hardlink: Some(HardlinkObservation { id, total_links: 2 }),
    };
    let mut stats = ProjectStats::default();
    let mut breakdown = CategoryBreakdown::default();
    let mut reclaim = ReclaimEstimator::default();
    let mut candidates = Vec::new();

    for _ in 0..2 {
        record_scanned_file(
            &mut stats,
            &mut breakdown,
            &mut reclaim,
            &mut candidates,
            100,
            ProjectBucket::Deps,
            true,
            observation,
        );
    }
    finalize_cache_candidates(&mut stats, &mut breakdown, &reclaim, &candidates);

    assert_eq!(
        reclaim.estimated_reclaimable_bytes(),
        100,
        "two names for one inode reclaim that inode only once"
    );
    assert_eq!(
        stats.cached_bytes, 0,
        "hardlinks wholly inside target/ are not evidence of a store link"
    );
    assert_eq!(stats.local_bytes, 200, "both apparent paths stay local");
}

#[test]
fn cache_candidates_accept_each_independent_store_sharing_signal() {
    let external_id = FileIdentity {
        device: 17,
        inode: 23,
    };
    let mut reclaim = ReclaimEstimator::default();
    reclaim.record(
        200,
        StorageObservation {
            sharing: crate::sharing::Sharing::unknown_for(200),
            hardlink: Some(HardlinkObservation {
                id: external_id,
                total_links: 2,
            }),
        },
    );

    let candidates = [
        CacheCandidate {
            size: 100,
            bucket: ProjectBucket::Deps,
            reflink_shared: true,
            hardlink_id: None,
        },
        CacheCandidate {
            size: 200,
            bucket: ProjectBucket::Deps,
            reflink_shared: false,
            hardlink_id: Some(external_id),
        },
    ];
    let mut stats = ProjectStats::default();
    let mut breakdown = CategoryBreakdown::default();
    finalize_cache_candidates(&mut stats, &mut breakdown, &reclaim, &candidates);

    assert_eq!(stats.cached_bytes, 300);
    assert_eq!(stats.local_bytes, 0);
    assert_eq!(breakdown.deps_local, 0);
}

#[test]
fn reclaim_estimator_counts_binary_reflink_private_bytes() {
    let mut stats = ProjectStats::default();
    let mut breakdown = CategoryBreakdown::default();
    let mut reclaim = ReclaimEstimator::default();
    let mut candidates = Vec::new();

    record_scanned_file(
        &mut stats,
        &mut breakdown,
        &mut reclaim,
        &mut candidates,
        100,
        ProjectBucket::Binaries,
        false,
        StorageObservation {
            sharing: crate::sharing::Sharing {
                shared: true,
                private_bytes: 25,
                snapshot_bytes: 0,
            },
            hardlink: None,
        },
    );
    finalize_cache_candidates(&mut stats, &mut breakdown, &reclaim, &candidates);

    assert_eq!(reclaim.estimated_reclaimable_bytes(), 25);
    assert_eq!(stats.cached_bytes, 0, "binary bucketing stays unchanged");
    assert_eq!(breakdown.binaries, 100);
}

#[test]
fn windows_identity_parts_preserve_volume_and_both_file_index_halves() {
    let identity = windows_file_identity_from_parts(0x1020_3040, 0x1122_3344, 0x5566_7788);
    assert_eq!(identity.device, 0x1020_3040);
    assert_eq!(identity.inode, 0x1122_3344_5566_7788);
    assert_ne!(
        identity,
        windows_file_identity_from_parts(0x1020_3040, 0x1122_3344, 0x5566_7789),
        "the low DWORD remains part of the stable identity"
    );
}

#[test]
fn windows_storage_observation_covers_identity_and_link_count_boundaries() {
    let id = FileIdentity {
        device: 5,
        inode: 8,
    };

    let single = windows_storage_observation(4096, Some((id, 1)));
    assert_eq!(single.sharing.private_bytes, 4096);
    assert!(
        single.hardlink.is_none(),
        "one link is not a hardlink group"
    );

    let linked = windows_storage_observation(4096, Some((id, 2)));
    assert_eq!(
        linked.hardlink,
        Some(HardlinkObservation { id, total_links: 2 })
    );

    let unavailable = windows_storage_observation(4096, None);
    assert_eq!(unavailable.sharing.private_bytes, 0);
    assert!(!unavailable.sharing.shared);
    assert!(unavailable.hardlink.is_none());
}

#[test]
fn unsupported_storage_observation_treats_the_whole_file_as_private() {
    let observation = unsupported_storage_observation(4096);
    assert_eq!(observation.sharing.private_bytes, 4096);
    assert!(!observation.sharing.shared);
    assert!(observation.hardlink.is_none());
}

#[cfg(windows)]
#[test]
fn windows_hardlinks_share_one_reclaimable_file() {
    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("first.rlib");
    let second = dir.path().join("second.rlib");
    fs::write(&first, vec![0u8; 4096]).unwrap();
    fs::hard_link(&first, &second).unwrap();

    let first_meta = fs::metadata(&first).unwrap();
    let second_meta = fs::metadata(&second).unwrap();
    let (first_identity, first_links) =
        query_windows_file_identity(&first).expect("first hardlink has a Windows identity");
    let (second_identity, second_links) =
        query_windows_file_identity(&second).expect("second hardlink has a Windows identity");
    assert_eq!(first_identity, second_identity);
    assert_eq!((first_links, second_links), (2, 2));

    let distinct = dir.path().join("distinct.rlib");
    fs::write(&distinct, vec![0u8; 4096]).unwrap();
    let (distinct_identity, distinct_links) =
        query_windows_file_identity(&distinct).expect("a plain file has a Windows identity");
    assert_ne!(first_identity, distinct_identity);
    assert_eq!(distinct_links, 1);
    assert!(
        query_windows_file_identity(&dir.path().join("missing.rlib")).is_none(),
        "a failed open must not invent an identity"
    );

    let first_observation = observe_storage(&first, &first_meta);
    let second_observation = observe_storage(&second, &second_meta);
    assert_eq!(first_observation.hardlink, second_observation.hardlink);
    let distinct_observation = observe_storage(&distinct, &fs::metadata(&distinct).unwrap());
    assert!(distinct_observation.hardlink.is_none());

    let mut reclaim = ReclaimEstimator::default();
    reclaim.record(first_meta.len(), first_observation);
    reclaim.record(second_meta.len(), second_observation);
    assert_eq!(reclaim.estimated_reclaimable_bytes(), 4096);
}

#[test]
fn test_compute_project_stats_with_profiles() {
    let dir = tempfile::tempdir().unwrap();
    let debug = dir.path().join("debug");
    fs::create_dir(&debug).unwrap();

    // incremental dir
    let incr = debug.join("incremental");
    fs::create_dir(&incr).unwrap();
    fs::write(incr.join("data"), vec![0u8; 100]).unwrap();

    // .fingerprint dir
    let fp = debug.join(".fingerprint");
    fs::create_dir(&fp).unwrap();
    fs::write(fp.join("hash"), vec![0u8; 50]).unwrap();

    // build dir
    let build = debug.join("build");
    fs::create_dir(&build).unwrap();
    fs::write(build.join("script"), vec![0u8; 30]).unwrap();

    // deps dir
    let deps = debug.join("deps");
    fs::create_dir(&deps).unwrap();
    fs::write(deps.join("libfoo.rlib"), vec![0u8; 200]).unwrap();

    let (stats, breakdown) = compute_project_stats(dir.path());
    assert!(stats.total_bytes > 0);
    assert!(breakdown.incremental >= 100);
    assert!(breakdown.fingerprints >= 50);
    assert!(breakdown.build_scripts >= 30);
}

#[cfg(unix)]
#[test]
fn compute_project_stats_classifies_an_externally_hardlinked_rlib_as_cached() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("target");
    let debug = target.join("debug");
    fs::create_dir_all(&debug).unwrap();

    let retained_blob = dir.path().join("store-blob.rlib");
    fs::write(&retained_blob, vec![0u8; 4096]).unwrap();
    fs::hard_link(&retained_blob, debug.join("libcached.rlib")).unwrap();

    let (stats, breakdown) = compute_project_stats(&target);
    assert_eq!(stats.total_bytes, 4096);
    assert_eq!(stats.cached_bytes, 4096);
    assert_eq!(stats.local_bytes, 0);
    assert_eq!(breakdown.other, 0);
}

#[cfg(unix)]
#[test]
fn compute_project_stats_does_not_follow_profile_symlinks() {
    let dir = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    fs::write(outside.path().join("outside.rlib"), vec![0u8; 4096]).unwrap();
    std::os::unix::fs::symlink(outside.path(), dir.path().join("debug")).unwrap();

    let (stats, _) = compute_project_stats(dir.path());
    assert_eq!(stats.total_bytes, 0);
    assert_eq!(stats.estimated_reclaimable_bytes, 0);
    assert!(detect_profiles(dir.path()).is_empty());
}

#[test]
fn compute_project_stats_reads_cargos_per_unit_build_layout() {
    let dir = tempfile::tempdir().unwrap();
    let build = dir.path().join("debug/build");
    let write = |path: &str, len: usize| {
        let path = build.join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, vec![0u8; len]).unwrap();
    };
    write("foo/0123456789abcdef/fingerprint/lib-foo", 5);
    write("foo/0123456789abcdef/out/libfoo-0123456789abcdef.rlib", 7);
    write("foo/1111111111111111/out/build_script_build", 11);
    write("foo/2222222222222222/out/gen.rs", 13);
    write("foo/2222222222222222/run/stdout", 17);
    write("bar-0123456789abcdef/output", 19);
    write("loose", 23);
    write("foo/0123456789abcdef/extra", 29);
    write("foo/not-a-unit/out/libfoo.rlib", 31);
    write("foo/fedcba9876543210", 37);

    let (stats, breakdown) = compute_project_stats(dir.path());
    assert_eq!(stats.total_bytes, 95 + 29 + 31 + 37);
    assert_eq!(breakdown.fingerprints, 5);
    assert_eq!(breakdown.deps_local, 7, "a compiled unit's out is deps");
    assert_eq!(
        breakdown.build_scripts,
        11 + 13 + 17 + 19 + 23 + 29 + 31 + 37,
        "a build script's binary, a run, the legacy layout, and anything \
             unrecognized, as the whole directory used to"
    );
    assert_eq!(breakdown.other, 0);
}

#[test]
fn test_compute_project_stats_classifies_remaining_buckets() {
    // Profile files/directories -> binaries, deps-local, and other buckets.
    let dir = tempfile::tempdir().unwrap();
    let debug = dir.path().join("debug");
    let deps_nested = debug.join("deps").join("nested");
    let other_dir = debug.join("examples");
    fs::create_dir_all(&deps_nested).unwrap();
    fs::create_dir_all(&other_dir).unwrap();
    fs::write(debug.join("runner"), vec![0u8; 11]).unwrap();
    fs::write(deps_nested.join("libdep.rmeta"), vec![0u8; 13]).unwrap();
    fs::write(other_dir.join("note.txt"), vec![0u8; 17]).unwrap();
    fs::write(dir.path().join("CACHEDIR.TAG"), vec![0u8; 19]).unwrap();

    let (stats, breakdown) = compute_project_stats(dir.path());
    assert_eq!(stats.total_bytes, 60);
    assert!(breakdown.binaries >= 11, "got {}", breakdown.binaries);
    assert!(breakdown.deps_local >= 13, "got {}", breakdown.deps_local);
    assert!(breakdown.other >= 36, "got {}", breakdown.other);
    assert_eq!(
        stats.local_files, 4,
        "nested local files are counted individually"
    );
}

#[test]
fn test_parse_cargo_lock_crate_names_nonexistent() {
    // When Cargo.lock doesn't exist in cwd, should return None
    // We can't guarantee cwd lacks Cargo.lock, so just test the function doesn't panic
    let _ = parse_cargo_lock_crate_names();
}

#[test]
fn test_parse_cargo_lock_crate_names_from_valid_missing_and_bad_files() {
    // Cargo.lock parser -> valid names, missing file, and malformed TOML.
    let dir = tempfile::tempdir().unwrap();
    let lock = dir.path().join("Cargo.lock");
    std::fs::write(
        &lock,
        "version = 3\n\n[[package]]\nname = \"serde\"\nversion = \"1.0.0\"\n\n\
             [[package]]\nname = \"tokio\"\nversion = \"1.0.0\"\n",
    )
    .unwrap();

    let names = parse_cargo_lock_crate_names_from(&lock).unwrap();
    assert!(names.contains("serde"));
    assert!(names.contains("tokio"));
    assert_eq!(
        parse_cargo_lock_crate_names_from(&dir.path().join("missing.lock")),
        None
    );
    std::fs::write(&lock, "not valid toml [[[[").unwrap();
    assert_eq!(parse_cargo_lock_crate_names_from(&lock), None);
}

#[test]
fn test_is_macos_protected() {
    // On non-macOS the stub always returns false — verify that invariant
    // and skip the positive-match assertions.
    if !cfg!(target_os = "macos") {
        assert!(!is_macos_protected(std::path::Path::new("/System/Library")));
        assert!(!is_macos_protected(std::path::Path::new("/tmp/build")));
        return;
    }

    // System paths
    assert!(is_macos_protected(std::path::Path::new("/System/Library")));
    assert!(is_macos_protected(std::path::Path::new(
        "/Library/Preferences"
    )));
    assert!(is_macos_protected(std::path::Path::new(
        "/Applications/Xcode.app"
    )));
    assert!(is_macos_protected(std::path::Path::new(
        "/Volumes/External"
    )));
    assert!(is_macos_protected(std::path::Path::new("/private/var")));
    assert!(is_macos_protected(std::path::Path::new("/Network/Servers")));

    // Home TCC dirs (if home is available)
    if let Some(home) = dirs::home_dir() {
        assert!(is_macos_protected(&home.join("Desktop")));
        assert!(is_macos_protected(&home.join("Documents")));
        assert!(is_macos_protected(&home.join("Downloads")));
        assert!(is_macos_protected(&home.join("Library")));
        assert!(is_macos_protected(&home.join("Pictures")));
        assert!(is_macos_protected(&home.join("Music")));
        assert!(is_macos_protected(&home.join("Movies")));
        assert!(is_macos_protected(&home.join("Applications")));
        assert!(is_macos_protected(&home.join("Public")));
        // Nested paths under protected dirs are also caught
        assert!(is_macos_protected(&home.join("Documents/subfolder")));

        // Developer directories are NOT protected
        assert!(!is_macos_protected(&home.join("projects")));
        assert!(!is_macos_protected(&home.join("src")));
        assert!(!is_macos_protected(&home.join("work")));
        assert!(!is_macos_protected(&home.join(".config")));
    }

    // Arbitrary dev paths are not protected
    assert!(!is_macos_protected(std::path::Path::new("/tmp/build")));
    assert!(!is_macos_protected(std::path::Path::new("/Users/dev/code")));
}

#[test]
fn test_category_breakdown_default() {
    let b = CategoryBreakdown::default();
    assert_eq!(b.incremental, 0);
    assert_eq!(b.build_scripts, 0);
    assert_eq!(b.fingerprints, 0);
    assert_eq!(b.binaries, 0);
    assert_eq!(b.deps_local, 0);
    assert_eq!(b.other, 0);
}

#[test]
fn test_cargo_wrapper_edit_create() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    let plan = plan_cargo_wrapper_edit(&path).unwrap();
    assert!(matches!(plan, CargoWrapperPlan::Create));
    let new = apply_cargo_wrapper_edit("", &plan);
    assert_eq!(new, "[build]\nrustc-wrapper = \"kache\"\n");
}

#[test]
fn test_cargo_wrapper_edit_replace() {
    let existing = "[build]\nrustc-wrapper = \"sccache\"\n";
    let plan = CargoWrapperPlan::Replace("sccache".into());
    let new = apply_cargo_wrapper_edit(existing, &plan);
    assert_eq!(new, "[build]\nrustc-wrapper = \"kache\"\n");
}

#[test]
fn test_cargo_wrapper_edit_replace_quote_styles_and_miss() {
    // Replace plan -> single-quoted, compact, and no-match branches.
    let single = apply_cargo_wrapper_edit(
        "[build]\nrustc-wrapper = 'sccache'\n",
        &CargoWrapperPlan::Replace("sccache".into()),
    );
    assert_eq!(single, "[build]\nrustc-wrapper = \"kache\"\n");

    let compact = apply_cargo_wrapper_edit(
        "[build]\nrustc-wrapper=\"sccache\"\n",
        &CargoWrapperPlan::Replace("sccache".into()),
    );
    assert_eq!(compact, "[build]\nrustc-wrapper = \"kache\"\n");

    let unchanged = apply_cargo_wrapper_edit(
        "[build]\nrustc-wrapper = \"other\"\n",
        &CargoWrapperPlan::Replace("sccache".into()),
    );
    assert_eq!(unchanged, "[build]\nrustc-wrapper = \"other\"\n");
}

#[test]
fn test_cargo_wrapper_edit_add_under_build() {
    let existing = "[build]\njobs = 4\n";
    let plan = CargoWrapperPlan::AddUnderBuild;
    let new = apply_cargo_wrapper_edit(existing, &plan);
    assert!(new.contains("rustc-wrapper = \"kache\""));
    assert!(new.contains("jobs = 4"));
}

#[test]
fn test_cargo_wrapper_edit_append_section() {
    let existing = "[net]\nretry = 3\n";
    let plan = CargoWrapperPlan::AppendSection;
    let new = apply_cargo_wrapper_edit(existing, &plan);
    assert!(new.contains("[net]"));
    assert!(new.trim_end().ends_with("rustc-wrapper = \"kache\""));
}

#[test]
fn test_cargo_wrapper_edit_already_set() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(&path, "[build]\nrustc-wrapper = \"kache\"\n").unwrap();
    let plan = plan_cargo_wrapper_edit(&path).unwrap();
    assert!(matches!(plan, CargoWrapperPlan::AlreadySet));
}

// The planner's Replace / AddUnderBuild / AppendSection arms are reached by
// reading a real config file (the apply tests above build those plans by
// hand). Drive each shape through the file-reading path, then apply the
// resulting plan to confirm the round-trip lands kache as the wrapper.
#[test]
fn test_plan_cargo_wrapper_edit_replace_from_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(&path, "[build]\nrustc-wrapper = \"sccache\"\n").unwrap();
    let plan = plan_cargo_wrapper_edit(&path).unwrap();
    assert_eq!(plan, CargoWrapperPlan::Replace("sccache".into()));
    let new = apply_cargo_wrapper_edit(&std::fs::read_to_string(&path).unwrap(), &plan);
    assert_eq!(new, "[build]\nrustc-wrapper = \"kache\"\n");
}

#[test]
fn test_plan_cargo_wrapper_edit_add_under_build_from_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(&path, "[build]\njobs = 8\n").unwrap();
    let plan = plan_cargo_wrapper_edit(&path).unwrap();
    assert_eq!(plan, CargoWrapperPlan::AddUnderBuild);
    let new = apply_cargo_wrapper_edit(&std::fs::read_to_string(&path).unwrap(), &plan);
    assert!(new.contains("jobs = 8"));
    assert!(new.contains("rustc-wrapper = \"kache\""));
}

#[test]
fn test_plan_cargo_wrapper_edit_append_section_from_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(&path, "[net]\nretry = 2\n").unwrap();
    let plan = plan_cargo_wrapper_edit(&path).unwrap();
    assert_eq!(plan, CargoWrapperPlan::AppendSection);
    let new = apply_cargo_wrapper_edit(&std::fs::read_to_string(&path).unwrap(), &plan);
    assert!(new.contains("[net]"));
    assert!(new.contains("[build]"));
    assert!(new.contains("rustc-wrapper = \"kache\""));
}

#[test]
fn test_plan_cargo_wrapper_edit_rejects_malformed_toml() {
    // A file that isn't valid TOML surfaces the parse-error context arm.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(&path, "this = = not valid toml\n").unwrap();
    let err = plan_cargo_wrapper_edit(&path).unwrap_err();
    assert!(
        err.to_string().contains("parsing"),
        "expected a parse-context error, got: {err}"
    );
}

#[test]
fn test_get_workspace_crate_names_lists_members() {
    // A two-member workspace; `cargo metadata --no-deps` should report both.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::write(
        root.join("Cargo.toml"),
        "[workspace]\nmembers = [\"a\", \"b\"]\nresolver = \"2\"\n",
    )
    .unwrap();
    for m in ["a", "b"] {
        std::fs::create_dir_all(root.join(m).join("src")).unwrap();
        std::fs::write(
            root.join(m).join("Cargo.toml"),
            format!("[package]\nname = \"{m}\"\nversion = \"0.1.0\"\nedition = \"2021\"\n"),
        )
        .unwrap();
        std::fs::write(root.join(m).join("src/lib.rs"), "").unwrap();
    }

    let names = get_workspace_crate_names(root.join("Cargo.toml").to_str().unwrap()).unwrap();
    assert!(names.contains(&"a".to_string()), "got {names:?}");
    assert!(names.contains(&"b".to_string()), "got {names:?}");
}

#[test]
fn test_get_workspace_crate_names_errors_on_bad_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let bad = dir.path().join("Cargo.toml");
    std::fs::write(&bad, "this is not valid toml [[[").unwrap();
    assert!(get_workspace_crate_names(bad.to_str().unwrap()).is_err());
}

#[test]
fn test_workspace_filter_bad_manifest_is_empty_set() {
    // Explicit bad manifest path -> warning branch with empty filter.
    let dir = tempfile::tempdir().unwrap();
    let bad = dir.path().join("Cargo.toml");
    std::fs::write(&bad, "not toml [[[[").unwrap();

    let filter = workspace_filter(Some(bad.to_str().unwrap())).unwrap();
    assert!(filter.is_empty());
}

// ── backend-neutral remote tests ──────────────────────────────────────────

#[derive(Default)]
struct BackendCalls {
    gets: Vec<String>,
    puts: Vec<String>,
    lists: Vec<String>,
}

struct TestBackend {
    inner: crate::remote_backend::OpenDalBackend,
    calls: std::sync::Mutex<BackendCalls>,
    fail_put: bool,
}

impl TestBackend {
    fn memory() -> Arc<Self> {
        Arc::new(Self {
            inner: crate::remote_backend::memory_backend(),
            calls: std::sync::Mutex::new(BackendCalls::default()),
            fail_put: false,
        })
    }

    fn failing_put() -> Arc<Self> {
        Arc::new(Self {
            inner: crate::remote_backend::memory_backend(),
            calls: std::sync::Mutex::new(BackendCalls::default()),
            fail_put: true,
        })
    }

    async fn seed(&self, key: &str, body: impl Into<Vec<u8>>) {
        crate::remote_backend::RemoteBackend::put(&self.inner, key, body.into(), None)
            .await
            .expect("seed remote object");
    }

    fn get_calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().gets.clone()
    }

    fn put_calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().puts.clone()
    }

    fn list_calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().lists.clone()
    }
}

fn as_remote_backend(backend: &Arc<TestBackend>) -> Arc<dyn crate::remote_backend::RemoteBackend> {
    backend.clone()
}

fn as_cache_remote(
    backend: Arc<dyn crate::remote_backend::RemoteBackend>,
    remote: &crate::config::RemoteConfig,
) -> crate::cache_remote::V3Remote {
    crate::cache_remote::V3Remote::new(backend, remote.clone())
}

#[async_trait::async_trait]
impl crate::remote_backend::RemoteBackend for TestBackend {
    async fn head(&self, key: &str) -> Result<bool> {
        crate::remote_backend::RemoteBackend::head(&self.inner, key).await
    }

    async fn get(
        &self,
        key: &str,
        max_bytes: Option<u64>,
    ) -> Result<Option<crate::remote_backend::GetObject>> {
        self.calls.lock().unwrap().gets.push(key.to_string());
        crate::remote_backend::RemoteBackend::get(&self.inner, key, max_bytes).await
    }

    async fn put(&self, key: &str, body: Vec<u8>, content_type: Option<&str>) -> Result<()> {
        self.calls.lock().unwrap().puts.push(key.to_string());
        if self.fail_put {
            anyhow::bail!("injected PUT failure for {key}");
        }
        crate::remote_backend::RemoteBackend::put(&self.inner, key, body, content_type).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.calls.lock().unwrap().lists.push(prefix.to_string());
        crate::remote_backend::RemoteBackend::list(&self.inner, prefix).await
    }

    fn describe(&self, key: &str) -> String {
        crate::remote_backend::RemoteBackend::describe(&self.inner, key)
    }
}

#[tokio::test]
async fn upload_shards_uploads_one_shard_per_nonempty_bucket() {
    // Two deps that both have build events -> they land in (likely) two
    // shards; assert the upload count equals the shards that had entries.
    let dir = tempfile::tempdir().unwrap();
    let lock = dir.path().join("Cargo.lock");
    std::fs::write(
        &lock,
        "version = 3\n\n[[package]]\nname = \"serde\"\nversion = \"1.0.0\"\n\n\
             [[package]]\nname = \"tokio\"\nversion = \"1.0.0\"\n",
    )
    .unwrap();

    let entries = vec![
        crate::remote::ManifestEntry {
            cache_key: "k-serde".to_string(),
            crate_name: "serde".to_string(),
            compile_time_ms: 1,
            artifact_size: 1,
        },
        crate::remote::ManifestEntry {
            cache_key: "k-tokio".to_string(),
            crate_name: "tokio".to_string(),
            compile_time_ms: 1,
            artifact_size: 1,
        },
    ];

    // Compute how many shards actually carry entries, so the test is robust
    // to the bucket assignment.
    let deps = crate::shards::parse_cargo_lock(&lock).unwrap();
    let shard_set = crate::shards::compute_shards(&deps);
    let expected = shard_set.shards.len();

    let backend = TestBackend::memory();
    let client = as_remote_backend(&backend);
    let remote = test_remote_cfg();
    let remote_cache: Arc<crate::cache_remote::V3Remote> =
        Arc::new(crate::cache_remote::V3Remote::new(client, remote));

    let uploaded = upload_shards(&remote_cache, "ns", &lock, &entries, None)
        .await
        .expect("upload_shards should succeed");
    assert_eq!(uploaded, expected);
    let puts = backend.put_calls();
    assert_eq!(puts.len(), expected);
    assert!(
        puts.iter()
            .all(|key| key.starts_with("prefix/_manifests/v3/ns/shards/"))
    );
}

#[tokio::test]
async fn upload_shards_skips_when_no_entries_match() {
    // Deps present but no matching build events -> no shards uploaded, so
    // no S3 requests are made.
    let dir = tempfile::tempdir().unwrap();
    let lock = dir.path().join("Cargo.lock");
    std::fs::write(
        &lock,
        "version = 3\n\n[[package]]\nname = \"serde\"\nversion = \"1.0.0\"\n",
    )
    .unwrap();

    let backend = TestBackend::memory();
    let client = as_remote_backend(&backend);
    let remote = test_remote_cfg();
    let remote_cache: Arc<crate::cache_remote::V3Remote> =
        Arc::new(crate::cache_remote::V3Remote::new(client, remote));
    let uploaded = upload_shards(&remote_cache, "ns", &lock, &[], None)
        .await
        .expect("should succeed with nothing to upload");
    assert_eq!(uploaded, 0);
    assert!(backend.put_calls().is_empty());
}

#[tokio::test]
async fn upload_shards_errors_on_malformed_lockfile() {
    // Bad Cargo.lock -> parse error before any shard upload.
    let dir = tempfile::tempdir().unwrap();
    let lock = dir.path().join("Cargo.lock");
    std::fs::write(&lock, "not valid toml [[[[").unwrap();
    let backend = TestBackend::memory();
    let client = as_remote_backend(&backend);
    let remote = test_remote_cfg();
    let remote_cache: Arc<crate::cache_remote::V3Remote> =
        Arc::new(crate::cache_remote::V3Remote::new(client, remote));

    let err = upload_shards(&remote_cache, "ns", &lock, &[], None)
        .await
        .expect_err("bad lockfile should error");
    assert!(
        err.to_string().contains("TOML") || err.to_string().contains("parse"),
        "got {err}"
    );
    assert!(backend.put_calls().is_empty());
}

fn save_manifest_config(
    cache_dir: std::path::PathBuf,
    remote: Option<crate::config::RemoteConfig>,
) -> Config {
    use crate::config::{
        DEFAULT_DAEMON_IDLE_TIMEOUT_SECS, DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
        DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS, DEFAULT_S3_POOL_IDLE_SECS,
    };
    Config {
        remote_error: None,
        socket_path_override: None,
        fallback: None,
        key_salt: None,
        cc_extra_allowlist_flags: Vec::new(),
        local_only: false,
        remote_readonly: false,
        modified_input_guard: false,
        input_predictions: false,
        record_sessions: false,
        volume_stores: Vec::new(),
        windows_hardlink: false,
        shared_hardlink_restores: false,
        deferred_discovery: true,
        out_dir_alias: true,
        deferred_durability: false,
        daemon_publish: false,
        project_rules: crate::config::ProjectRules::default(),
        auto_gc: true,
        index_auto_compact: true,
        auto_clean_orphaned_targets: false,
        auto_clean_idle_targets_days: 0,
        gc_evict_shared: false,
        storage_layout_advice: true,
        heartbeat_secs: 30,
        explain_miss: false,
        scheduler: true,
        test_lease: None,
        path_only_env_vars: Vec::new(),
        incremental_crates: Vec::new(),
        key_env_vars: Vec::new(),
        base_dirs: Vec::new(),
        runtime_dir: cache_dir.clone(),
        cache_dir,
        max_size: 1024 * 1024,
        remote,
        disabled: false,
        cache_executables: false,
        cache_cc_links: false,
        trust_codegen_backends: false,
        clean_incremental: true,
        preserve_incremental: false,
        adaptive_incremental: true,
        event_log_max_size: 1024 * 1024,
        event_log_keep_lines: 1000,
        compression_level: 3,
        s3_concurrency: 16,
        prefetch_enabled: crate::config::DEFAULT_PREFETCH_ENABLED,
        remote_key_cache_refresh_secs: crate::config::DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS,
        prefetch_max_keys: crate::config::DEFAULT_PREFETCH_MAX_KEYS,
        prefetch_max_bytes: crate::config::DEFAULT_PREFETCH_MAX_BYTES,
        prefetch_deadline_secs: crate::config::DEFAULT_PREFETCH_DEADLINE_SECS,
        min_store_compile_ms: crate::config::DEFAULT_MIN_STORE_COMPILE_MS,
        gc_max_age_hours: crate::config::DEFAULT_GC_MAX_AGE_HOURS,
        daemon_idle_timeout_secs: DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
        s3_pool_idle_secs: DEFAULT_S3_POOL_IDLE_SECS,
        remote_restore_timeout_secs: DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS,
        remote_negative_ttl_secs: DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
    }
}

fn put_entry(config: &Config, key: &str, crate_name: &str, dir: &std::path::Path) {
    let store = Store::open(config).unwrap();
    let src = dir.join(format!("{key}.rlib"));
    std::fs::write(&src, format!("artifact bytes for {key}")).unwrap();
    store
        .put(
            key,
            crate_name,
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "debug",
            &[(src, format!("{key}.rlib"))],
            "",
            "",
        )
        .unwrap();
}

fn overwrite_entry_meta(
    config: &Config,
    key: &str,
    crate_name: &str,
    mut meta: crate::store::EntryMeta,
) {
    meta.cache_key = key.to_string();
    meta.crate_name = crate_name.to_string();
    std::fs::write(
        config.store_dir().join(key).join("meta.json"),
        serde_json::to_vec(&meta).unwrap(),
    )
    .unwrap();
}

#[test]
fn why_miss_no_events_prints_tip() {
    // No events for the crate -> the "build it first" tip path.
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    why_miss(&config, "ghost", false).expect("why_miss with no events should succeed");
}

#[test]
fn why_miss_shows_stored_metadata_for_a_missed_key() {
    // A Miss event whose cache_key has a stored entry on disk drives the
    // miss-metadata display (target/profile) + the stored-entries listing.
    // Covers why_miss's metadata branch (cli.rs ~471-490+).
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    // Seed a store entry for "serde" so meta.json (with target/profile) exists.
    put_entry(&config, "serdemisskey", "serde", dir.path());
    // Log a Miss event for that crate + key.
    crate::events::log_event(
        &config.event_log_path(),
        &build_event(
            "serde",
            crate::events::EventResult::Miss,
            1234,
            1300,
            4096,
            "serdemisskey",
        ),
    )
    .unwrap();

    why_miss(&config, "serde", false).expect("why_miss should succeed for a missed key");
}

#[test]
fn why_miss_leads_with_a_store_failure_when_there_was_one() {
    // The store-failure banner replaces guesswork about the key: nothing was
    // stored, so no later build could have matched (kunobi-ninja/kache#629).
    let mut miss = build_event(
        "lint_crate",
        crate::events::EventResult::Miss,
        1000,
        900,
        4096,
        "somekey",
    );
    assert!(
        store_failure_banner(&miss).is_none(),
        "a normal miss gets no banner"
    );

    miss.store_error = "creating blob shard directory: Permission denied (os error 13)".into();
    let banner = store_failure_banner(&miss).expect("failed store should produce a banner");
    assert!(banner.contains("NOT CACHED"));
    assert!(banner.contains("Permission denied (os error 13)"));
    // The stored-entry analysis owns the `Diagnosis:` label; a second one
    // here would read as a competing conclusion.
    assert!(!banner.contains("Diagnosis:"), "banner: {banner}");
}

#[test]
fn why_miss_prioritizes_same_key_lookup_rejection() {
    let mut miss = build_event(
        "foo.c",
        crate::events::EventResult::Miss,
        10,
        20,
        30,
        "same-key",
    );
    assert!(lookup_rejection_banner(&miss, true).is_none());

    miss.lookup_rejection = "matching entry lacks dep-info required by this invocation".to_string();
    let banner = lookup_rejection_banner(&miss, true).unwrap();
    assert!(banner.contains("matching key was found but rejected"));
    assert!(banner.contains("lacks dep-info required"));
    assert!(banner.contains("currently present under the same key"));
    assert!(!banner.contains("key mismatch"), "banner: {banner}");
}

#[test]
fn why_miss_legacy_same_key_repeat_does_not_claim_key_mismatch() {
    let mut miss = build_event(
        "foo.c",
        crate::events::EventResult::Miss,
        10,
        20,
        30,
        "same-key",
    );
    miss.schema = 14;

    assert!(legacy_repeated_same_key_banner(&miss, false).is_none());
    let banner = legacy_repeated_same_key_banner(&miss, true).unwrap();
    assert!(banner.contains("repeated miss for the same cache key"));
    assert!(banner.contains("older event did not record"));
    assert!(banner.contains("not caused by a cache-key change"));
    assert!(
        !banner.contains("Diagnosis: key mismatch"),
        "banner: {banner}"
    );

    miss.schema = 15;
    assert!(legacy_repeated_same_key_banner(&miss, true).is_none());
}

#[test]
fn why_miss_all_hits_reports_no_misses() {
    // Events exist but none are Miss/Dup -> the "all events are hits" path.
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    crate::events::log_event(
        &config.event_log_path(),
        &build_event(
            "tokio",
            crate::events::EventResult::LocalHit,
            0,
            5,
            4096,
            "tokiohitkey",
        ),
    )
    .unwrap();

    why_miss(&config, "tokio", false).expect("why_miss with only hits should succeed");
}

#[test]
fn why_miss_reports_many_stored_diffs() {
    // Stored miss + many other entries -> diff printer cap branch.
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    put_entry(&config, "misskeymanydiffs", "serde", dir.path());
    overwrite_entry_meta(
        &config,
        "misskeymanydiffs",
        "serde",
        diff_meta("wasm32", "release", &[], &["lib"]),
    );
    for i in 0..3 {
        let key = format!("otherkeymanydiffs{i}");
        let feature = format!("feat{i}");
        put_entry(&config, &key, "serde", dir.path());
        overwrite_entry_meta(
            &config,
            &key,
            "serde",
            diff_meta(
                &format!("target{i}"),
                &format!("profile{i}"),
                &[&feature],
                &["bin"],
            ),
        );
    }
    crate::events::log_event(
        &config.event_log_path(),
        &build_event(
            "serde",
            crate::events::EventResult::Miss,
            123,
            130,
            4096,
            "misskeymanydiffs",
        ),
    )
    .unwrap();

    why_miss(&config, "serde", false).expect("why_miss should print capped diffs");
}

#[test]
fn why_miss_diff_messages_cap_feature_and_type_diffs() {
    // Diff helper -> empty miss features, crate-type diffs, and output cap.
    let miss = diff_meta("wasm32", "release", &[], &["lib"]);
    let others: Vec<_> = (0..3)
        .map(|i| {
            (
                format!("other{i}"),
                diff_meta("x86_64", "debug", &[&format!("feat{i}")], &["bin"]),
            )
        })
        .collect();
    let (messages, extra) = why_miss_diff_messages(
        &miss,
        others.iter().map(|(key, meta)| (key.as_str(), meta)),
        5,
    );

    assert_eq!(messages.len(), 5);
    assert!(extra > 0, "expected capped messages, got {messages:?}");
    assert!(messages.iter().any(|m| m.contains("different target")));
    assert!(messages.iter().any(|m| m.contains("different profile")));
    assert!(
        messages.iter().any(|m| m.contains("[(none)] vs [feat0]")),
        "got {messages:?}"
    );
    assert!(messages.iter().any(|m| m.contains("different crate types")));
}

#[test]
fn why_miss_diff_messages_dedupes_same_config_and_empty_other_features() {
    // Diff helper -> empty other features and same-config de-duplication.
    let miss = diff_meta("x86_64", "debug", &["feat"], &["lib"]);
    let others = [
        (
            "empty-features",
            diff_meta("x86_64", "debug", &[], &["lib"]),
        ),
        ("same-a", diff_meta("x86_64", "debug", &["feat"], &["lib"])),
        ("same-b", diff_meta("x86_64", "debug", &["feat"], &["lib"])),
    ];
    let (messages, extra) =
        why_miss_diff_messages(&miss, others.iter().map(|(key, meta)| (*key, meta)), 5);

    assert_eq!(extra, 0);
    assert!(
        messages.iter().any(|m| m.contains("[feat] vs [(none)]")),
        "got {messages:?}"
    );
    assert_eq!(
        messages
            .iter()
            .filter(|m| m.contains("likely source code"))
            .count(),
        1
    );
}

#[test]
fn checkout_comparison_lines_separate_path_only_from_input_changes() {
    use crate::miss_chain::{ChangedDep, CheckoutComparison, CheckoutVerdict};
    let dep = |name: &str| ChangedDep {
        name: name.to_string(),
        from: Some("a".to_string()),
        to: Some("b".to_string()),
        unit: None,
    };
    let compared = |verdict, groups: &[&str], dependencies: Vec<ChangedDep>| {
        checkout_comparison_lines(&CheckoutComparison {
            root: "/b".to_string(),
            baseline_root: "/a".to_string(),
            groups: groups.iter().map(|g| g.to_string()).collect(),
            dependencies,
            same_key: verdict == CheckoutVerdict::PathOnly,
            verdict,
        })
    };

    let lines = compared(CheckoutVerdict::PathOnly, &[], vec![]);
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("the key is not why this missed"));

    let lines = compared(CheckoutVerdict::OwnInputs, &["env_deps"], vec![]);
    assert_eq!(lines[0], "own inputs differ: env_deps");
    assert_eq!(lines.len(), 2, "no dependency or remap line: {lines:?}");
    assert!(lines[1].contains("real input difference"));

    let lines = compared(
        CheckoutVerdict::OwnInputs,
        &["args", "remap"],
        vec![dep("mid"), dep("util")],
    );
    assert_eq!(lines[0], "own inputs differ: args, remap");
    assert_eq!(lines[1], "dependencies differ: mid, util");
    assert!(lines[2].contains("real input difference"), "{lines:?}");
    assert!(lines[3].starts_with("(remap:"), "{lines:?}");

    // Only `remap` differs: the normalization note does not apply.
    let lines = compared(CheckoutVerdict::OwnInputs, &["remap"], vec![]);
    assert_eq!(lines.len(), 2, "{lines:?}");
    assert!(lines[1].starts_with("(remap:"), "{lines:?}");

    let lines = compared(CheckoutVerdict::Dependencies, &[], vec![dep("mid")]);
    assert_eq!(
        lines,
        vec!["own inputs match after path normalization; dependencies differ: mid"]
    );

    let lines = compared(CheckoutVerdict::Untraced, &[], vec![]);
    assert!(lines[0].contains("key salt or extra inputs"));
}

#[test]
fn telemetry_push_refuses_a_malformed_label_before_reading_logs() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    let err = telemetry_push(
        &config,
        &TimelineSelection::Latest,
        &["no-equals-sign".to_string()],
        true,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("not key=value"),
        "the label parse error must reach the caller: {err}"
    );
}

#[test]
fn collect_timelines_reports_only_stamped_wrapper_versions() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    std::fs::create_dir_all(&config.runtime_dir).unwrap();
    // Neither event carries a session id, so no record is built and the
    // version list is what `nothing_to_send` will name. An unstamped
    // version must not appear as an empty entry.
    let mut unstamped =
        crate::events::BuildEvent::new_for_test("serde", crate::events::EventResult::LocalHit);
    unstamped.version = String::new();
    let mut stamped =
        crate::events::BuildEvent::new_for_test("syn", crate::events::EventResult::LocalHit);
    stamped.version = "0.23.0".to_string();
    let log = config.event_log_path();
    crate::events::log_event(&log, &unstamped).unwrap();
    crate::events::log_event(&log, &stamped).unwrap();

    let (records, seen, versions) = collect_timelines(
        &config,
        &TimelineSelection::All,
        std::collections::BTreeMap::new(),
    )
    .unwrap();
    assert!(records.is_empty());
    assert_eq!(seen, 2);
    assert_eq!(
        versions.into_iter().collect::<Vec<_>>(),
        vec!["0.23.0".to_string()]
    );
}

#[test]
fn telemetry_write_emits_cache_scope_not_bench() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    let out = dir.path().join("otlp");
    telemetry_write(&config, &out, Some("bench-firefox"), Some("warm")).expect("telemetry write");
    let body = std::fs::read_to_string(out.join("metrics.otlp.json")).unwrap();
    assert!(
        body.contains("\"kache.cache\""),
        "cache OTLP must use the kache.cache scope"
    );
    assert!(
        body.contains("\"kache.cache.scenario\""),
        "bench dumps must name the scenario"
    );
    assert!(
        body.contains("bench-firefox"),
        "scenario value must match kache.bench.project"
    );
    assert!(
        body.contains("\"kache.cache.phase\""),
        "bench dumps must name the phase"
    );
    assert!(
        body.contains("warm"),
        "phase value must match the bench phase"
    );
    assert!(
        !body.contains("kache.bench."),
        "cache dump must not mix bench gauges"
    );
    assert!(
        body.contains("AGGREGATION_TEMPORALITY_CUMULATIVE"),
        "daemon counters must ride as cumulative sums, not gauges"
    );
    assert_eq!(
        std::fs::read_to_string(out.join("schema_version"))
            .unwrap()
            .trim(),
        "1"
    );
}

#[test]
fn purge_skips_corrupt_filtered_entry() {
    // purge(crate) -> corrupt meta removal error is skipped, not fatal.
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    put_entry(&config, "badpurgekey", "bad", dir.path());
    std::fs::write(
        config.store_dir().join("badpurgekey").join("meta.json"),
        b"{ not valid json",
    )
    .unwrap();

    purge(&config, Some("bad")).expect("purge skips corrupt entries");
    assert!(
        config
            .store_dir()
            .join("badpurgekey")
            .join("meta.json")
            .exists(),
        "corrupt entry remains accounted-for after skipped purge"
    );
}

/// Purge is a bulk mutation and must serialize behind the same
/// cross-process lock every GC driver takes; unlocked, a purge can
/// interleave with a live sweep and each observes the other's
/// half-removed state.
#[test]
fn purge_waits_for_the_gc_lock() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    put_entry(&config, "purgelockkey", "locked", dir.path());

    let store = crate::store::Store::open(&config).unwrap();
    let gc_lock = store.try_gc_lock().unwrap().expect("uncontended lock");

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let cfg = config.clone();
    let worker = std::thread::spawn(move || {
        let result = purge(&cfg, None);
        let _ = done_tx.send(());
        result
    });

    // While the "sweep" holds gc.lock the purge must not complete.
    // (A scheduling hiccup can only delay the mutant's completion
    // signal, never produce a false failure for the real code.)
    assert!(
        done_rx
            .recv_timeout(std::time::Duration::from_millis(300))
            .is_err(),
        "purge completed while the GC lock was held"
    );
    drop(gc_lock);
    done_rx
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("purge proceeds once the GC lock is released");
    worker.join().unwrap().expect("purge succeeds");
    let store = crate::store::Store::open(&config).unwrap();
    assert_eq!(store.entry_count().unwrap(), 0, "store cleared");
}

#[test]
fn verify_reports_valid_entries_on_a_clean_store() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    put_entry(&config, "validkey1", "serde", dir.path());

    // A clean store verifies with checksums and no repair needed.
    verify(&config, true, false).expect("verify of a clean store should succeed");
}

#[test]
#[allow(clippy::field_reassign_with_default)] // incremental snapshot setup reads clearer
fn render_stats_rich_snapshot_covers_all_lines() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = save_manifest_config(dir.path().join("cache"), Some(test_remote_cfg()));
    config.remote.as_mut().unwrap().prefix = "artifacts".to_string();

    let mut snap = StatsSnapshot::default();
    snap.total_size = 5000;
    snap.max_size = 10000;
    snap.entry_count = 3;
    snap.daemon_connected = true;
    snap.daemon_version = "9.9.9".to_string();
    snap.daemon_build_epoch = crate::daemon::build_epoch(); // matches -> no mismatch
    snap.event_stats.local_hits = 8;
    snap.event_stats.misses = 2;
    snap.event_stats.total_elapsed_ms = 1000;
    snap.event_stats.miss_elapsed_ms = 300;
    snap.event_stats.hit_compile_time_ms = 5000;
    snap.event_stats.miss_compile_time_ms = 2000;

    snap.blob_stats = Some(crate::store::BlobStats {
        total_blobs: 4,
        total_blob_size: 2048,
        total_logical_size: 4096,
        savings: 2048,
    });
    let out = render_stats(&snap, &config, SinceWindow::DEFAULT).join("\n");
    assert!(out.contains("Store:"));
    assert!(out.contains("Dedup:      4 unique blobs"));
    assert!(out.contains("Hit rate:"));
    assert!(out.contains("Weighted:"));
    assert!(out.contains("Miss share:"));
    assert!(out.contains("Time saved:"));
    assert!(out.contains("Daemon:     v9.9.9"));
    assert!(
        !out.contains("MISMATCH"),
        "matching epoch -> no mismatch tag"
    );
    assert!(out.contains("Remote:     s3://"));
}

#[test]
#[allow(clippy::field_reassign_with_default)]
fn render_stats_resilience_requires_both_sources_and_any_single_signal() {
    let dir = tempfile::tempdir().unwrap();
    let remote_config =
        save_manifest_config(dir.path().join("remote-cache"), Some(test_remote_cfg()));
    let local_config = save_manifest_config(dir.path().join("local-cache"), None);

    let mut quiet = StatsSnapshot::default();
    quiet.daemon_connected = true;
    assert!(
        render_stats(&quiet, &remote_config, SinceWindow::DEFAULT)
            .iter()
            .all(|line| !line.starts_with("Resilience:"))
    );

    let signals = [
        (
            "round trips",
            StatsSnapshot {
                remote_check_roundtrips: 1,
                ..Default::default()
            },
        ),
        (
            "negative hits",
            StatsSnapshot {
                negative_hits: 1,
                ..Default::default()
            },
        ),
        (
            "download suppression",
            StatsSnapshot {
                downloads_suppressed: 1,
                ..Default::default()
            },
        ),
        (
            "upload suppression",
            StatsSnapshot {
                uploads_suppressed: 1,
                ..Default::default()
            },
        ),
        (
            "degraded state",
            StatsSnapshot {
                remote_degraded: true,
                ..Default::default()
            },
        ),
    ];
    for (signal, mut snap) in signals {
        snap.daemon_connected = true;
        assert!(
            render_stats(&snap, &remote_config, SinceWindow::DEFAULT)
                .iter()
                .any(|line| line.starts_with("Resilience:")),
            "{signal} must independently render the resilience section"
        );
    }

    let disconnected = StatsSnapshot {
        negative_hits: 1,
        ..Default::default()
    };
    assert!(
        render_stats(&disconnected, &remote_config, SinceWindow::DEFAULT)
            .iter()
            .all(|line| !line.starts_with("Resilience:"))
    );
    let connected_without_remote = StatsSnapshot {
        daemon_connected: true,
        negative_hits: 1,
        ..Default::default()
    };
    assert!(
        render_stats(
            &connected_without_remote,
            &local_config,
            SinceWindow::DEFAULT
        )
        .iter()
        .all(|line| !line.starts_with("Resilience:"))
    );
}

/// The #485 Phase-0 prefetch section renders when the daemon reports
/// activity and stays absent for a quiet/offline daemon, so local-only
/// `kache stats` output is unchanged.
#[test]
#[allow(clippy::field_reassign_with_default)]
fn render_stats_prefetch_section_gated_on_activity() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), Some(test_remote_cfg()));
    // Quiet daemon: no prefetch lines at all.
    let mut quiet = StatsSnapshot::default();
    quiet.daemon_connected = true;
    let out = render_stats(&quiet, &config, SinceWindow::DEFAULT).join("\n");
    assert!(!out.contains("Prefetch:"));
    assert!(!out.contains("Planning:"));

    // Active daemon: all lines present with the right arithmetic.
    let mut snap = StatsSnapshot::default();
    snap.daemon_connected = true;
    snap.prefetch = crate::daemon::PrefetchStatsSnapshot {
        downloads_completed: 4,
        bytes_downloaded: 2048,
        keys_used: 2,
        keys_cancelled: 3,
        keys_over_budget: 0,
        cancelled: true,
        plans_advisory: 1,
        plans_fallback: 2,
        last_plan_candidates: 7,
        dedup_join_waits: 5,
        dedup_join_wait_ms: 1234,
        last_list_duration_ms: 88,
        last_list_key_count: 250_000,
        list_requests_total: 0,
        list_failures_total: 0,
        list_duration_ms_total: 0,
        list_keys_total: 0,
        pack_requests_total: 3,
        pack_bytes_downloaded: 4096,
        v3_requests_total: 1,
        v3_bytes_downloaded: 1024,
        pack_validation_failures: 1,
        pack_fallback_entries: 1,
        last_plan_wall_ms: 250,
        plan_wall_ms_total: 500,
    };
    let mut eff = effective_config_like(&config);
    eff.remote_key_cache_refresh_secs = 7;
    snap.daemon_effective_config = Some(eff);
    let out = render_stats(&snap, &config, SinceWindow::DEFAULT).join("\n");
    assert!(out.contains("Prefetch:   4 downloads"));
    assert!(out.contains("2 used (50%)"));
    assert!(out.contains("CANCELLED"));
    assert!(out.contains("Planning:   1 advisory / 2 fallback plans (last: 7 candidates)"));
    assert!(out.contains("Transport:  pack 3 requests"));
    assert!(out.contains("v3 1 requests"));
    assert!(out.contains("Plan wall:  250 ms last / 500 ms total"));
    assert!(out.contains("Key LIST:   250000 keys in 88 ms (refreshes every 7s)"));
    assert!(!out.contains("daemon did not report its cadence"));
    assert!(out.contains("Join-wait:  5 waits, 1234 ms total"));

    let mut initial_only = config.clone();
    initial_only.remote_key_cache_refresh_secs = 0;
    let mut eff = effective_config_like(&initial_only);
    eff.remote_key_cache_refresh_secs = 0;
    snap.daemon_effective_config = Some(eff);
    let out = render_stats(&snap, &initial_only, SinceWindow::DEFAULT).join("\n");
    assert!(out.contains(
        "Key LIST:   250000 keys in 88 ms (one initial population; periodic refresh disabled)"
    ));

    let mut disabled = config.clone();
    disabled.prefetch_enabled = false;
    let out = render_stats(&quiet, &disabled, SinceWindow::DEFAULT).join("\n");
    assert!(out.contains("Prefetch:   disabled (exact remote lookup and uploads remain enabled)"));
    assert!(!out.contains("Planning:"));
    assert!(!out.contains("Key LIST:"));

    snap.prefetch.last_list_key_count = 0;
    let out = render_stats(&snap, &config, SinceWindow::DEFAULT).join("\n");
    assert!(out.contains("Prefetch:   4 downloads"));
    assert!(
        !out.contains("Key LIST:"),
        "zero listed keys must not render a LIST status line: {out}"
    );
    snap.prefetch.last_list_key_count = 250_000;

    // Transport activity is the sum of two independent counters.  Neither
    // a zero total nor a zero wall clock sample should render a line.
    snap.prefetch.pack_requests_total = 0;
    snap.prefetch.v3_requests_total = 0;
    snap.prefetch.last_plan_wall_ms = 0;
    let out = render_stats(&snap, &config, SinceWindow::DEFAULT).join("\n");
    assert!(!out.contains("Transport:"));
    assert!(!out.contains("Plan wall:"));

    snap.prefetch.pack_requests_total = 1;
    let out = render_stats(&snap, &config, SinceWindow::DEFAULT).join("\n");
    assert!(out.contains("Transport:  pack 1 requests"));
    snap.prefetch.pack_requests_total = 0;
    snap.prefetch.v3_requests_total = 1;
    let out = render_stats(&snap, &config, SinceWindow::DEFAULT).join("\n");
    assert!(out.contains("v3 1 requests"));
}

/// A daemon-shaped [`crate::daemon::EffectiveConfig`] mirroring `config`,
/// with a distinct config path so tests can tell daemon-side rendering
/// from client-side rendering.
fn effective_config_like(config: &Config) -> crate::daemon::EffectiveConfig {
    crate::daemon::EffectiveConfig {
        max_size: config.max_size,
        cache_dir: config.cache_dir.display().to_string(),
        runtime_dir: config.runtime_dir.display().to_string(),
        config_path: "/daemon-home/.config/kache/config.toml".to_string(),
        config_fingerprint: Some("daemon-fingerprint".to_string()),
        prefetch_enabled: config.prefetch_enabled,
        remote_description: config.remote.as_ref().map(|remote| remote.describe()),
        local_only: config.local_only,
        remote_error: config.remote_error.clone(),
        remote_key_cache_refresh_secs: config.remote_key_cache_refresh_secs,
        socket_path: config.socket_path().display().to_string(),
        started_at_ms: 1_700_000_000_000,
    }
}

/// #652/#689: the prefetch policy line renders the DAEMON's effective
/// policy. A client config saying "disabled" must not produce a disabled
/// line while the daemon reports prefetch enabled — and the daemon's
/// config path is surfaced on the Daemon line.
#[test]
#[allow(clippy::field_reassign_with_default)]
fn render_stats_prefetch_policy_prefers_daemon_effective() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = save_manifest_config(dir.path().join("cache"), Some(test_remote_cfg()));
    config.prefetch_enabled = false; // client says disabled…

    let mut snap = StatsSnapshot::default();
    snap.daemon_connected = true;
    let mut eff = effective_config_like(&config);
    eff.prefetch_enabled = true; // …but the daemon is still planning
    snap.daemon_effective_config = Some(eff);

    let out = render_stats(&snap, &config, SinceWindow::DEFAULT).join("\n");
    assert!(
        !out.contains("Prefetch:   disabled"),
        "must not claim disabled while the daemon reports enabled: {out}"
    );
    assert!(out.contains("config /daemon-home/.config/kache/config.toml"));

    // And the inverse: the daemon reports disabled, so the line shows it
    // even though this process's config says enabled — unlabeled, because
    // it is a daemon fact.
    config.prefetch_enabled = true;
    let mut eff = effective_config_like(&config);
    eff.prefetch_enabled = false;
    snap.daemon_effective_config = Some(eff);
    let out = render_stats(&snap, &config, SinceWindow::DEFAULT).join("\n");
    assert!(out.contains("Prefetch:   disabled (exact remote lookup and uploads remain enabled)"));
    assert!(!out.contains("client config"));
}

/// Old-daemon fallback (#689): a daemon that predates effective-config
/// reporting leaves `daemon_effective_config` empty, so the policy line
/// falls back to this process's config — labeled as such, because it can
/// disagree with what the daemon is actually doing.
#[test]
#[allow(clippy::field_reassign_with_default)]
fn render_stats_prefetch_policy_labels_client_fallback_for_old_daemon() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = save_manifest_config(dir.path().join("cache"), Some(test_remote_cfg()));
    config.prefetch_enabled = false;

    let mut snap = StatsSnapshot::default();
    snap.daemon_connected = true; // reachable, but reported no config

    let out = render_stats(&snap, &config, SinceWindow::DEFAULT).join("\n");
    assert!(out.contains(
        "Prefetch:   disabled (exact remote lookup and uploads remain enabled) \
             [client config — daemon did not report its policy]"
    ));
    assert!(
        !out.contains(", config "),
        "no daemon config path to show without a report: {out}"
    );
}

/// #689: each rendered field that differs between the daemon's effective
/// config and this process's resolved config produces one warning naming
/// both values and both sources; agreement produces none.
#[test]
fn config_mismatch_warnings_name_both_sides_per_field() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    let same = crate::config::ConfigFileProvenance {
        path: "/daemon-home/.config/kache/config.toml".into(),
        fingerprint: "daemon-fingerprint".to_string(),
    };
    let other_path = crate::config::ConfigFileProvenance {
        path: "/cli-home/kache-repro.toml".into(),
        fingerprint: "client-fingerprint".to_string(),
    };

    // Full agreement is silent.
    let eff = effective_config_like(&config);
    assert!(config_mismatch_warnings(&config, &same, &eff).is_empty());

    // LIST cadence is irrelevant when neither side has a remote. A
    // stale/default cadence alone must not produce a mismatch warning.
    let mut cadence_only = eff.clone();
    cadence_only.remote_key_cache_refresh_secs += 1;
    assert!(config_mismatch_warnings(&config, &same, &cadence_only).is_empty());

    // Different config provenance is visible even while selected values
    // happen to match: edits to one file will not reach the other process.
    let warnings = config_mismatch_warnings(&config, &other_path, &eff);
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].contains("daemon loaded config"), "{warnings:?}");
    assert!(warnings[0].contains("values may diverge"), "{warnings:?}");

    // The path can stay fixed while its contents change between process
    // starts. Exact loaded fingerprints keep that mismatch visible.
    let changed = crate::config::ConfigFileProvenance {
        path: same.path.clone(),
        fingerprint: "changed-fingerprint".to_string(),
    };
    let warnings = config_mismatch_warnings(&config, &changed, &eff);
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].contains("different snapshots"), "{warnings:?}");

    // An intermediate/older daemon that reports effective values but no
    // fingerprint must not create a false mismatch warning.
    let mut old_eff = eff.clone();
    old_eff.config_fingerprint = None;
    assert!(config_mismatch_warnings(&config, &same, &old_eff).is_empty());

    // Runtime placement is daemon-owned too, but an older daemon that
    // cannot report it must not create a false mismatch.
    let mut runtime_eff = effective_config_like(&config);
    runtime_eff.runtime_dir = "/somewhere/runtime".to_string();
    let warnings = config_mismatch_warnings(&config, &same, &runtime_eff);
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].contains("runtime_dir=/somewhere/runtime"));
    runtime_eff.runtime_dir.clear();
    assert!(config_mismatch_warnings(&config, &same, &runtime_eff).is_empty());

    // Store cap differs.
    let mut eff = effective_config_like(&config);
    eff.max_size = config.max_size * 2;
    let warnings = config_mismatch_warnings(&config, &same, &eff);
    assert_eq!(warnings.len(), 1);
    assert!(
        warnings[0].contains("local_max_size=2.0 MiB"),
        "{warnings:?}"
    );
    assert!(warnings[0].contains("says 1.0 MiB"), "{warnings:?}");
    assert!(
        warnings[0].contains(
            "daemon (started 2023-11-14 22:13 UTC, config /daemon-home/.config/kache/config.toml)"
        ),
        "{warnings:?}"
    );
    assert!(
        warnings[0].contains("this process's config (/daemon-home/.config/kache/config.toml)"),
        "{warnings:?}"
    );
    assert!(
        warnings[0].contains("the daemon's value is in effect"),
        "{warnings:?}"
    );

    // Every rendered field differs -> one warning per field, and the
    // store divergence calls out that the numbers describe another store.
    let mut eff = effective_config_like(&config);
    eff.max_size += 1;
    eff.cache_dir = "/somewhere/else".to_string();
    eff.prefetch_enabled = !config.prefetch_enabled;
    eff.remote_description = Some("s3://daemon-bucket/artifacts".to_string());
    eff.remote_key_cache_refresh_secs += 1;
    eff.started_at_ms = 0; // old field default must not claim 1970
    let warnings = config_mismatch_warnings(&config, &same, &eff);
    assert_eq!(warnings.len(), 5);
    assert!(
        warnings[1].contains("local_store=/somewhere/else"),
        "{warnings:?}"
    );
    assert!(
        warnings[1].contains("the daemon's numbers describe ITS store"),
        "{warnings:?}"
    );
    assert!(
        warnings[2].contains("prefetch_enabled=false"),
        "{warnings:?}"
    );
    assert!(warnings[3].contains("remote=s3://daemon-bucket/artifacts"));
    assert!(warnings[4].contains("remote_key_cache_refresh_secs=61"));
    assert!(warnings[0].contains("started unknown time"), "{warnings:?}");
}

/// #689: remote state and LIST cadence are daemon-owned. Both mismatch
/// directions render the daemon's truth, never the invoking shell's.
#[test]
#[allow(clippy::field_reassign_with_default)]
fn render_stats_remote_state_prefers_daemon_effective() {
    let dir = tempfile::tempdir().unwrap();
    let client_remote =
        save_manifest_config(dir.path().join("client-remote"), Some(test_remote_cfg()));
    let mut snap = StatsSnapshot::default();
    snap.daemon_connected = true;
    let mut eff = effective_config_like(&client_remote);
    eff.remote_description = None;
    eff.remote_key_cache_refresh_secs = 7;
    snap.daemon_effective_config = Some(eff);
    let out = render_stats(&snap, &client_remote, SinceWindow::DEFAULT).join("\n");
    assert!(out.contains("Remote:     not configured"), "{out}");
    assert!(!out.contains("Remote:     s3://"), "{out}");

    let client_local = save_manifest_config(dir.path().join("client-local"), None);
    let mut eff = effective_config_like(&client_local);
    eff.remote_description = Some("s3://daemon-bucket/artifacts".to_string());
    snap.daemon_effective_config = Some(eff);
    let out = render_stats(&snap, &client_local, SinceWindow::DEFAULT).join("\n");
    assert!(
        out.contains("Remote:     s3://daemon-bucket/artifacts"),
        "{out}"
    );
    assert!(!out.contains("client config"), "{out}");
}

#[test]
#[allow(clippy::field_reassign_with_default)] // incremental snapshot setup reads clearer
fn render_stats_daemon_mismatch_and_local_only() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = save_manifest_config(dir.path().join("cache"), None);
    config.local_only = true;

    let mut snap = StatsSnapshot::default();
    snap.daemon_connected = true;
    snap.daemon_version = "1.0.0".to_string();
    snap.daemon_build_epoch = crate::daemon::build_epoch().wrapping_add(1); // mismatch
    let out = render_stats(&snap, &config, SinceWindow::DEFAULT).join("\n");
    assert!(out.contains("MISMATCH — auto-restart pending"));
    assert!(out.contains("local-only mode"));
}

#[test]
fn render_stats_offline_and_not_configured() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    let snap = StatsSnapshot::default(); // daemon_connected=false
    let out = render_stats(&snap, &config, SinceWindow::DEFAULT).join("\n");
    assert!(out.contains("Daemon:     offline"));
    assert!(out.contains("Remote:     not configured"));
    // No blobs -> no Dedup line.
    assert!(!out.contains("Dedup:"));
}

#[test]
fn render_stats_handles_zero_limits_and_zero_logical_dedup() {
    // Zero max/logical sizes -> percentage branches stay finite.
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    let snap = StatsSnapshot {
        total_size: 500,
        max_size: 0,
        blob_stats: Some(crate::store::BlobStats {
            total_blobs: 2,
            total_blob_size: 500,
            total_logical_size: 0,
            savings: 0,
        }),
        ..Default::default()
    };

    let window = SinceWindow::parse("15m").unwrap();
    let out = render_stats(&snap, &config, window).join("\n");
    assert!(out.contains("Store:      500 B / 0 B (0 entries, 0%)"));
    assert!(out.contains("Dedup:      2 unique blobs, 500 B physical, 0.0% savings"));
    // #897: the label names the requested window, not a hardcoded 24h.
    assert!(
        out.contains("Time saved: n/a (estimated compile work avoided, last 15m)"),
        "{out}"
    );

    let no_blobs = StatsSnapshot {
        blob_stats: Some(crate::store::BlobStats {
            total_blobs: 0,
            total_blob_size: 0,
            total_logical_size: 0,
            savings: 0,
        }),
        ..Default::default()
    };
    let out = render_stats(&no_blobs, &config, window).join("\n");
    assert!(
        !out.contains("Dedup:"),
        "an empty blob snapshot must not render a dedup line: {out}"
    );
}

#[test]
fn snapshot_from_direct_reads_returns_only_the_newest_five_summaries() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    let summary_path = config.summary_log_path();
    std::fs::create_dir_all(summary_path.parent().unwrap()).unwrap();
    let mut summaries = (0..7)
        .map(|index| {
            format!(
                "{{\"ts\":\"2026-08-09T00:00:0{index}Z\",\"schema\":1,\"session_id\":\"s{index}\"}}"
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    summaries.push('\n');
    std::fs::write(summary_path, summaries).unwrap();

    let snap = snapshot_from_direct_reads(&config, false, "size", SinceWindow::DEFAULT, true);
    let ids = snap
        .recent_summaries
        .iter()
        .map(|summary| summary.session_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(ids, ["s2", "s3", "s4", "s5", "s6"]);
}

#[test]
fn snapshot_from_direct_reads_reflects_store_and_events() {
    // No daemon: the snapshot is built from direct store + event-log reads.
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    put_entry(&config, "serdekey", "serde", dir.path());
    // Log a couple of events so event_stats is populated.
    crate::events::log_event(
        &config.event_log_path(),
        &build_event(
            "serde",
            crate::events::EventResult::LocalHit,
            0,
            5,
            4096,
            "serdekey",
        ),
    )
    .unwrap();
    crate::events::log_event(
        &config.event_log_path(),
        &build_event(
            "tokio",
            crate::events::EventResult::Miss,
            900,
            950,
            8192,
            "tk",
        ),
    )
    .unwrap();

    let snap = snapshot_from_direct_reads(&config, true, "name", SinceWindow::DEFAULT, false);
    assert!(!snap.daemon_connected, "direct reads report no daemon");
    assert_eq!(snap.entry_count, 1);
    assert_eq!(snap.entries.len(), 1);
    assert_eq!(snap.entries[0].crate_name, "serde");
    assert_eq!(snap.event_stats.local_hits, 1);
    assert_eq!(snap.event_stats.misses, 1);
    assert_eq!(snap.max_size, config.max_size);
}

/// #897: `--since` must narrow the counters, not just the label. A 15m
/// window excludes the three-hour-old miss that a 24h window includes.
#[test]
fn snapshot_from_direct_reads_honors_a_sub_hour_window() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    let now = chrono::Utc::now();
    let mut recent = build_event(
        "serde",
        crate::events::EventResult::LocalHit,
        0,
        5,
        4096,
        "a",
    );
    recent.ts = now - chrono::Duration::minutes(10);
    let mut old = build_event(
        "tokio",
        crate::events::EventResult::Miss,
        900,
        950,
        8192,
        "b",
    );
    old.ts = now - chrono::Duration::hours(3);
    crate::events::log_event(&config.event_log_path(), &recent).unwrap();
    crate::events::log_event(&config.event_log_path(), &old).unwrap();

    let narrow = SinceWindow::parse("15m").unwrap();
    let snap = snapshot_from_direct_reads(&config, false, "size", narrow, false);
    assert_eq!(snap.event_stats.local_hits, 1);
    assert_eq!(snap.event_stats.misses, 0, "a 3h-old miss is outside 15m");

    let wide = SinceWindow::parse("24h").unwrap();
    let snap = snapshot_from_direct_reads(&config, false, "size", wide, false);
    assert_eq!(snap.event_stats.local_hits, 1);
    assert_eq!(snap.event_stats.misses, 1);
}

#[test]
fn snapshot_from_direct_reads_without_entries_skips_listing() {
    // include_entries=false -> entries list is empty even with a populated store.
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    put_entry(&config, "k1", "serde", dir.path());
    let snap = snapshot_from_direct_reads(&config, false, "size", SinceWindow::DEFAULT, false);
    assert!(
        snap.entries.is_empty(),
        "entries omitted when not requested"
    );
    assert_eq!(snap.entry_count, 1, "count still reflects the store");
}

#[test]
fn sync_without_remote_errors() {
    // The sync entry point bails before building a runtime/client when no
    // remote is configured. Covers sync()'s no-remote guard.
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    let err = sync(&config, None, false, false, false, false, false, false)
        .expect_err("sync without a remote must error");
    assert!(
        err.to_string().contains("No remote configured"),
        "got: {err}"
    );
}

#[test]
fn repair_tip_and_reconciliation_message_cover_every_boundary() {
    assert!(!should_print_repair_tip(0, 0, 0, false));
    assert!(should_print_repair_tip(1, 0, 0, false));
    assert!(should_print_repair_tip(0, 1, 0, false));
    assert!(should_print_repair_tip(0, 0, 1, false));
    assert!(!should_print_repair_tip(1, 1, 1, true));

    assert_eq!(
        reconciled_index_message(crate::store::BlobIndexDrift::default()),
        None
    );
    assert_eq!(
        reconciled_index_message(crate::store::BlobIndexDrift {
            entry_mappings: 1,
            blobs: 2,
        })
        .as_deref(),
        Some("Repairing: reconciled 1 entry mappings and 2 blob rows.")
    );
}

#[test]
fn verify_detects_missing_blob_and_missing_meta_then_repairs() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    let store = Store::open(&config).unwrap();

    // Entry A: blob deleted -> "missing blob" path.
    put_entry(&config, "missingblobkey", "aaa", dir.path());
    let meta_a = store.get("missingblobkey").unwrap().unwrap();
    let blob_a = store.blob_path(&meta_a.files[0].hash);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(
            blob_a.parent().unwrap(),
            std::fs::Permissions::from_mode(0o755),
        );
    }
    let _ = std::fs::remove_file(&blob_a);

    // Entry B: meta.json deleted -> "missing meta" path.
    put_entry(&config, "missingmetakey", "bbb", dir.path());
    std::fs::remove_file(config.store_dir().join("missingmetakey").join("meta.json")).unwrap();

    // Entry C: meta.json corrupted -> "invalid meta" path.
    put_entry(&config, "badmetakey", "ccc", dir.path());
    std::fs::write(
        config.store_dir().join("badmetakey").join("meta.json"),
        b"{ not valid json",
    )
    .unwrap();

    // Entry D: a clean valid entry.
    put_entry(&config, "validkey", "ddd", dir.path());

    // repair=true attempts to remove every corrupted entry; the call must
    // succeed regardless of whether each removal is permitted.
    verify(&config, false, true).expect("verify --repair should succeed");

    // The valid entry survives. The missing-blob entry has a *parseable*
    // meta, so repair can (and does) remove it. The missing-meta and
    // corrupt-meta entries are deliberately NOT removed (#276: refusing to
    // orphan blob refcounts), so we don't assert their removal — running
    // verify again must still succeed over the remaining corrupt entries.
    let store2 = Store::open(&config).unwrap();
    assert!(store2.get("validkey").unwrap().is_some());
    assert!(store2.get("missingblobkey").unwrap().is_none());
    verify(&config, false, false).expect("a second verify pass should succeed");
}

#[test]
fn verify_reports_and_repairs_blob_index_drift() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    put_entry(&config, "driftkey", "drift", dir.path());
    let store = Store::open(&config).unwrap();
    let hash = store.get("driftkey").unwrap().unwrap().files[0]
        .hash
        .clone();
    drop(store);

    let db = crate::store::open_index_db(&config.index_db_path()).unwrap();
    db.execute(
        "UPDATE blobs SET refcount = 77 WHERE hash = ?1",
        rusqlite::params![hash],
    )
    .unwrap();
    drop(db);

    let drifted = verify(&config, false, false).unwrap();
    assert_eq!(drifted.index_drift, 1, "{drifted:?}");
    assert_eq!(drifted.unresolved_integrity_findings(), 1, "{drifted:?}");

    let repaired = verify(&config, false, true).unwrap();
    assert_eq!(repaired.index_drift, 0, "{repaired:?}");
    assert_eq!(repaired.unresolved_integrity_findings(), 0, "{repaired:?}");
    let clean = verify(&config, false, false).unwrap();
    assert_eq!(clean.index_drift, 0, "{clean:?}");
    assert_eq!(clean.unresolved_integrity_findings(), 0, "{clean:?}");
}

#[test]
fn verify_accepts_build_script_out_dir_artifact_names() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    let store = Store::open(&config).unwrap();
    let output = dir.path().join("asm.s");
    std::fs::write(&output, b"asm bytes").unwrap();

    // What a build-script run commits: OUT_DIR contents under `out/`
    // (`build_script.rs`). `doctor --verify` must read this as valid
    // metadata — flagging it reports corruption no repair can remove.
    store
        .put(
            "buildscriptkey",
            "build_script_run",
            &["build-script".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "debug",
            &[(output, "out/asm.s".to_string())],
            "",
            "",
        )
        .unwrap();
    drop(store);

    let outcome = verify(&config, false, false).expect("verify must succeed");
    assert_eq!(outcome.corrupted_entries, 0, "{outcome:?}");
    assert_eq!(outcome.index_drift, 0, "{outcome:?}");
    assert_eq!(outcome.unresolved_integrity_findings(), 0, "{outcome:?}");
}

#[test]
fn verify_detects_checksum_mismatch_with_checksums_enabled() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    let store = Store::open(&config).unwrap();
    put_entry(&config, "corruptblobkey", "eee", dir.path());

    // Corrupt the blob in place, same size so only the checksum differs.
    let meta = store.get("corruptblobkey").unwrap().unwrap();
    let blob = store.blob_path(&meta.files[0].hash);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&blob, std::fs::Permissions::from_mode(0o644)).unwrap();
    }
    #[cfg(not(unix))]
    {
        let mut p = std::fs::metadata(&blob).unwrap().permissions();
        p.set_readonly(false);
        std::fs::set_permissions(&blob, p).unwrap();
    }
    std::fs::write(&blob, vec![b'X'; meta.files[0].size as usize]).unwrap();

    // checksums=true detects the content mismatch and reports it as an
    // unresolved integrity finding (kunobi-ninja/kache#176).
    let outcome = verify(&config, true, false).expect("verify with checksums should succeed");
    assert_eq!(outcome.checksum_failures, 1, "{outcome:?}");
    assert_eq!(outcome.corrupted_entries, 1, "{outcome:?}");
    assert_eq!(
        outcome.unresolved_integrity_findings(),
        1,
        "without --repair the finding stands: {outcome:?}"
    );

    // --repair removes the entry, so nothing is left unresolved and a CI
    // run gated on this exits zero.
    let repaired = verify(&config, true, true).expect("verify --repair should succeed");
    assert_eq!(repaired.corrupted_removed, repaired.corrupted_entries);
    assert_eq!(repaired.unresolved_integrity_findings(), 0, "{repaired:?}");

    // And the store is clean afterwards.
    let clean = verify(&config, true, false).expect("verify after repair should succeed");
    assert_eq!(clean.corrupted_entries, 0, "{clean:?}");
    assert_eq!(clean.unresolved_integrity_findings(), 0, "{clean:?}");
}

/// kunobi-ninja/kache#176: the scrub hashes each unique blob ONCE, however
/// many entries share it — the dedup that makes the store cheap must not
/// make verification quadratic. Two entries sharing one corrupt blob
/// produce ONE checksum failure while marking BOTH entries corrupt.
#[test]
fn verify_scrubs_each_unique_blob_once_and_marks_every_referencing_entry() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    let store = Store::open(&config).unwrap();

    // Two entries, identical content → one shared blob. Each put gets
    // its OWN source path: on Linux the store hardlinks the source into
    // the read-only blob store, so reusing one path would make the
    // second write fail with EACCES (macOS reflinks, and would not).
    for key in ["sharedkey_a", "sharedkey_b"] {
        let src = dir.path().join(format!("{key}.rlib"));
        std::fs::write(&src, b"shared artifact bytes").unwrap();
        store
            .put(
                key,
                "shared",
                &["lib".to_string()],
                &[],
                "",
                "dev",
                &[(src, "lib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
    }
    let meta = store.get("sharedkey_a").unwrap().unwrap();
    let blob = store.blob_path(&meta.files[0].hash);
    assert_eq!(
        store.get("sharedkey_b").unwrap().unwrap().files[0].hash,
        meta.files[0].hash,
        "the two entries must share one blob for this test to mean anything"
    );

    let mut perms = std::fs::metadata(&blob).unwrap().permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        perms.set_mode(0o644);
    }
    #[cfg(not(unix))]
    perms.set_readonly(false);
    std::fs::set_permissions(&blob, perms).unwrap();
    std::fs::write(&blob, vec![b'X'; meta.files[0].size as usize]).unwrap();

    // Without --checksums the scrub does not run, so silent corruption
    // is invisible: that is exactly what the flag buys, and asserting it
    // pins the flag's meaning.
    let unchecked = verify(&config, false, false).expect("verify should succeed");
    assert_eq!(
        unchecked.checksum_failures, 0,
        "checksums=false must not hash anything: {unchecked:?}"
    );
    assert_eq!(
        unchecked.unresolved_integrity_findings(),
        0,
        "same-size corruption is undetectable without --checksums: {unchecked:?}"
    );

    let outcome = verify(&config, true, false).expect("verify should succeed");
    assert_eq!(
        outcome.checksum_failures, 1,
        "one shared blob is one failure, not one per referencing entry: {outcome:?}"
    );
    assert_eq!(
        outcome.corrupted_entries, 2,
        "but both entries referencing it are corrupt: {outcome:?}"
    );
    assert_eq!(outcome.unresolved_integrity_findings(), 2, "{outcome:?}");
}

/// A healthy store reports nothing unresolved, so a CI gate on
/// `doctor --verify` stays green (kunobi-ninja/kache#176).
#[test]
fn verify_reports_no_findings_for_a_clean_store() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    put_entry(&config, "cleanstorekey", "ggg", dir.path());

    let outcome = verify(&config, true, false).expect("verify should succeed");
    assert_eq!(outcome.corrupted_entries, 0, "{outcome:?}");
    assert_eq!(outcome.checksum_failures, 0, "{outcome:?}");
    assert_eq!(outcome.missing_blobs, 0, "{outcome:?}");
    assert_eq!(outcome.unresolved_integrity_findings(), 0, "{outcome:?}");
    assert!(outcome.valid_entries >= 1, "{outcome:?}");
}

#[test]
fn verify_detects_blob_size_mismatch() {
    // Blob metadata length mismatch -> entry is marked corrupt.
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().join("cache"), None);
    let store = Store::open(&config).unwrap();
    put_entry(&config, "sizemismatchkey", "fff", dir.path());
    let meta = store.get("sizemismatchkey").unwrap().unwrap();
    let blob = store.blob_path(&meta.files[0].hash);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&blob, std::fs::Permissions::from_mode(0o644)).unwrap();
    }
    #[cfg(not(unix))]
    {
        let mut p = std::fs::metadata(&blob).unwrap().permissions();
        p.set_readonly(false);
        std::fs::set_permissions(&blob, p).unwrap();
    }
    std::fs::write(&blob, vec![b'Y'; meta.files[0].size as usize + 1]).unwrap();

    verify(&config, false, false).expect("verify with size mismatch should succeed");
}

#[test]
fn save_manifest_without_remote_errors() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().to_path_buf(), None);
    let err = save_manifest(&config, None, None).expect_err("no remote -> error");
    assert!(
        err.to_string().contains("No remote configured"),
        "got {err}"
    );
}

#[test]
fn automatic_manifest_save_without_remote_errors() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().to_path_buf(), None);
    let err =
        save_manifest_auto_for_session(&config, "key", "session").expect_err("no remote -> error");
    assert!(
        err.to_string().contains("No remote configured"),
        "got {err}"
    );
}

#[test]
fn only_the_primary_manifest_key_publishes_shards() {
    assert_eq!(shard_namespace_for_publish_key(0, Some("ns")), Some("ns"));
    assert_eq!(shard_namespace_for_publish_key(1, Some("ns")), None);
    assert_eq!(shard_namespace_for_publish_key(0, None), None);
}

#[test]
fn save_manifest_with_no_events_returns_ok_before_touching_remote() {
    // A remote is configured, but the event log is empty, so save_manifest
    // returns Ok early ("No build events found") without creating a remote
    // client or making any network call.
    let dir = tempfile::tempdir().unwrap();
    let remote = crate::config::RemoteConfig::test_s3("b", "p");
    let config = save_manifest_config(dir.path().to_path_buf(), Some(remote));
    // No event log written -> read_events yields empty -> early Ok.
    save_manifest(&config, Some("mykey"), None).expect("empty events -> Ok");
}

fn build_event(
    crate_name: &str,
    result: crate::events::EventResult,
    compile_time_ms: u64,
    elapsed_ms: u64,
    size: u64,
    cache_key: &str,
) -> crate::events::BuildEvent {
    crate::events::BuildEvent {
        ts: chrono::Utc::now(),
        session_id: String::new(),
        demands: Vec::new(),
        crate_name: crate_name.to_string(),
        version: "0.1.0".to_string(),
        result,
        elapsed_ms,
        compile_time_ms,
        size,
        cache_key: cache_key.to_string(),
        schema: 8,
        key_ms: 0,
        key_hash_hits: 0,
        key_hash_misses: 0,
        key_hash_bytes: 0,
        lookup_ms: 0,
        restore_ms: 0,
        store_ms: 0,
        startup_ms: 0,
        dep_info_ms: 0,
        dep_info_runs: 0,
        prediction_mismatches: 0,
        flight_wait_ms: 0,
        permit_wait_ms: 0,
        store_output_blobs: 0,
        store_duplicate_blobs: 0,
        store_new_blobs: 0,
        compiler_runs: 0,
        preprocessor_runs: 0,
        probe_runs: 0,
        reflinked_bytes: 0,
        hardlinked_bytes: 0,
        copied_bytes: 0,
        store_reflinked_bytes: 0,
        store_hardlinked_bytes: 0,
        store_copied_bytes: 0,
        store_copy_cross_device_bytes: 0,
        store_copy_permission_bytes: 0,
        store_copy_ineligible_bytes: 0,
        store_copy_other_bytes: 0,
        restore_copy_cross_device_bytes: 0,
        restore_copy_permission_bytes: 0,
        restore_copy_exclusive_bytes: 0,
        restore_copy_other_bytes: 0,
        root: String::new(),
        passthrough_reason: String::new(),
        store_error: String::new(),
        store_handed_off: false,
        daemon_store_ms: 0,
        lookup_rejection: String::new(),
        verify_compare: String::new(),
        fallback: false,
        fallback_attempt: None,
        exit_code: None,
        key_fields: Default::default(),
        key_diff: Vec::new(),
        key_externs: Default::default(),
        key_externs_recorded: false,
        unit_id: String::new(),
        extern_units: Default::default(),
    }
}

fn diff_meta(
    target: &str,
    profile: &str,
    features: &[&str],
    crate_types: &[&str],
) -> crate::store::EntryMeta {
    crate::store::EntryMeta {
        cache_key: "k".to_string(),
        key_schema: crate::cache_key::CACHE_KEY_VERSION,
        crate_name: "c".to_string(),
        crate_types: crate_types.iter().map(|v| (*v).to_string()).collect(),
        files: Vec::new(),
        stdout: String::new(),
        stderr: String::new(),
        features: features.iter().map(|v| (*v).to_string()).collect(),
        target: target.to_string(),
        profile: profile.to_string(),
        compile_time_ms: 0,
        emit_kinds: Vec::new(),
    }
}

#[test]
fn manifest_commit_prefers_github_then_gitlab_and_skips_daemon_sessions() {
    let env = |vars: &'static [(&'static str, &'static str)]| {
        move |name: &str| {
            vars.iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| value.to_string())
        }
    };
    let both = env(&[("GITHUB_SHA", " gh "), ("CI_COMMIT_SHA", "gl")]);
    assert_eq!(manifest_commit(None, both).as_deref(), Some("gh"));
    assert_eq!(manifest_commit(Some("session"), both), None);
    let blank_github = env(&[("GITHUB_SHA", "  "), ("CI_COMMIT_SHA", "gl")]);
    assert_eq!(manifest_commit(None, blank_github).as_deref(), Some("gl"));
    assert_eq!(manifest_commit(None, env(&[])), None);
}

#[test]
fn manifest_entries_from_events_dedups_and_filters() {
    use crate::events::EventResult;
    let events = vec![
        // Same key twice: the larger compile time wins.
        build_event("serde", EventResult::Miss, 100, 0, 10, "k-serde"),
        build_event("serde", EventResult::LocalHit, 900, 0, 10, "k-serde"),
        // A distinct cacheable entry.
        build_event("tokio", EventResult::Dup, 50, 0, 20, "k-tokio"),
        // Ignored: empty cache_key.
        build_event("nokey", EventResult::Miss, 5, 0, 0, ""),
        // Ignored: non-cacheable outcomes.
        build_event("passth", EventResult::Passthrough, 5, 0, 0, "k-p"),
        build_event("skip", EventResult::Skipped, 5, 0, 0, "k-s"),
    ];

    let mut entries = manifest_entries_from_events(&events, None);
    entries.sort_by(|a, b| a.crate_name.cmp(&b.crate_name));

    assert_eq!(entries.len(), 2, "only the two cacheable keys survive");
    let serde = entries.iter().find(|e| e.crate_name == "serde").unwrap();
    assert_eq!(serde.compile_time_ms, 900, "larger compile time wins");
    assert!(entries.iter().any(|e| e.crate_name == "tokio"));
}

#[test]
fn manifest_entry_ties_keep_the_first_observation() {
    use crate::events::EventResult;
    let events = vec![
        build_event("first", EventResult::Miss, 100, 0, 10, "same-key"),
        build_event("second", EventResult::Miss, 100, 0, 20, "same-key"),
    ];

    let entries = manifest_entries_from_events(&events, None);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].crate_name, "first");
    assert_eq!(entries[0].artifact_size, 10);
}

#[test]
fn automatic_manifest_entries_are_scoped_to_the_build_session() {
    use crate::events::EventResult;
    let mut current = build_event("current", EventResult::Miss, 100, 0, 10, "current-key");
    current.session_id = "current-session".into();
    let mut other = build_event("other", EventResult::Miss, 200, 0, 20, "other-key");
    other.session_id = "other-session".into();

    let entries = manifest_entries_from_events(&[other, current], Some("current-session"));
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].crate_name, "current");
    assert_eq!(entries[0].cache_key, "current-key");
}

#[test]
fn manifest_entries_from_events_falls_back_to_elapsed_when_no_compile_time() {
    use crate::events::EventResult;
    // compile_time_ms == 0 -> the entry's compile_time_ms uses elapsed_ms.
    let events = vec![build_event("x", EventResult::Miss, 0, 77, 1, "k")];
    let entries = manifest_entries_from_events(&events, None);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].compile_time_ms, 77);
}

#[test]
fn manifest_entries_from_events_accepts_remote_and_prefetch_hits() {
    use crate::events::EventResult;
    // Prefetch/remote hits are cacheable manifest inputs.
    let events = vec![
        build_event(
            "prefetch",
            EventResult::PrefetchHit,
            12,
            3,
            10,
            "k-prefetch",
        ),
        build_event("remote", EventResult::RemoteHit, 34, 5, 20, "k-remote"),
        build_event("error", EventResult::Error, 99, 99, 99, "k-error"),
    ];

    let mut entries = manifest_entries_from_events(&events, None);
    entries.sort_by(|a, b| a.crate_name.cmp(&b.crate_name));
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].crate_name, "prefetch");
    assert_eq!(entries[0].artifact_size, 10);
    assert_eq!(entries[1].crate_name, "remote");
    assert_eq!(entries[1].compile_time_ms, 34);
}

fn test_remote_cfg() -> crate::config::RemoteConfig {
    crate::config::RemoteConfig::test_s3("bucket", "prefix")
}

#[tokio::test]
async fn upload_manifest_and_shards_uploads_manifest_only_without_namespace() {
    // No namespace -> exactly one object (the monolithic manifest).
    let backend = TestBackend::memory();
    let client = as_remote_backend(&backend);
    let remote = test_remote_cfg();
    let remote_cache: Arc<crate::cache_remote::V3Remote> =
        Arc::new(crate::cache_remote::V3Remote::new(client, remote));
    let entries = vec![crate::remote::ManifestEntry {
        cache_key: "k".to_string(),
        crate_name: "c".to_string(),
        compile_time_ms: 1,
        artifact_size: 1,
    }];
    upload_manifest_and_shards(
        &remote_cache,
        "mykey",
        None,
        std::path::Path::new("/nonexistent/Cargo.lock"),
        entries,
        None,
    )
    .await
    .expect("manifest-only upload should succeed");
    assert_eq!(backend.put_calls(), vec!["prefix/_manifests/mykey.json"]);
}

#[tokio::test]
async fn upload_manifest_and_shards_skips_shards_when_lock_missing() {
    // Namespace given but Cargo.lock absent -> still only the manifest.
    let backend = TestBackend::memory();
    let client = as_remote_backend(&backend);
    let remote = test_remote_cfg();
    let remote_cache: Arc<crate::cache_remote::V3Remote> =
        Arc::new(crate::cache_remote::V3Remote::new(client, remote));
    let entries = vec![crate::remote::ManifestEntry {
        cache_key: "k".to_string(),
        crate_name: "c".to_string(),
        compile_time_ms: 1,
        artifact_size: 1,
    }];
    upload_manifest_and_shards(
        &remote_cache,
        "mykey",
        Some("ns"),
        std::path::Path::new("/nonexistent/Cargo.lock"),
        entries,
        None,
    )
    .await
    .expect("upload should succeed, shards skipped");
    assert_eq!(backend.put_calls(), vec!["prefix/_manifests/mykey.json"]);
}

#[tokio::test]
async fn upload_manifest_and_shards_uploads_shards_when_lock_present() {
    // Namespace + a real Cargo.lock with deps that match the entries -> the
    // manifest PUT plus one PUT per non-empty shard.
    let dir = tempfile::tempdir().unwrap();
    let lock = dir.path().join("Cargo.lock");
    std::fs::write(
        &lock,
        "version = 3\n\n[[package]]\nname = \"serde\"\nversion = \"1.0.0\"\n",
    )
    .unwrap();
    let entries = vec![crate::remote::ManifestEntry {
        cache_key: "k-serde".to_string(),
        crate_name: "serde".to_string(),
        compile_time_ms: 1,
        artifact_size: 1,
    }];
    let deps = crate::shards::parse_cargo_lock(&lock).unwrap();
    let expected_shards = crate::shards::compute_shards(&deps).shards.len();

    let backend = TestBackend::memory();
    let client = as_remote_backend(&backend);
    let remote = test_remote_cfg();
    let remote_cache: Arc<crate::cache_remote::V3Remote> =
        Arc::new(crate::cache_remote::V3Remote::new(client, remote));

    upload_manifest_and_shards(&remote_cache, "mykey", Some("ns"), &lock, entries, None)
        .await
        .expect("upload with shards should succeed");
    let puts = backend.put_calls();
    assert_eq!(puts.len(), expected_shards + 1);
    assert!(puts.contains(&"prefix/_manifests/mykey.json".to_string()));
    assert_eq!(
        puts.iter()
            .filter(|key| key.starts_with("prefix/_manifests/v3/ns/shards/"))
            .count(),
        expected_shards
    );
}

fn sync_test_cache_key(seed: &str) -> String {
    blake3::hash(seed.as_bytes()).to_hex().to_string()
}

#[tokio::test]
async fn sync_with_client_dry_run_empty_remote_reports_nothing() {
    // Empty remote + empty local store: the diff is empty and sync reports
    // "Nothing to sync" after one list call.
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
    let store = Store::open(&config).unwrap();
    let remote = test_remote_cfg();

    let backend = TestBackend::memory();
    sync_with_client(
        &as_cache_remote(as_remote_backend(&backend), &remote),
        &config,
        &store,
        None,
        false,
        false,
        true,
        false,
        None,
        false,
        false,
    )
    .await
    .expect("dry-run sync over empty remote should succeed");
    assert_eq!(backend.list_calls(), vec!["prefix/v3/manifests/"]);
}

#[tokio::test]
async fn sync_with_client_workspace_pull_scopes_listing_to_workspace_members() {
    // `--workspace` must scope the pull listing to workspace members and
    // ignore the Cargo.lock dep set.
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
    let store = Store::open(&config).unwrap();
    let remote = test_remote_cfg();

    let workspace: std::collections::HashSet<String> = ["wsfoo".to_string()].into_iter().collect();
    let lock: std::collections::HashSet<String> = ["dep_a".to_string(), "dep_b".to_string()]
        .into_iter()
        .collect();

    let backend = TestBackend::memory();
    sync_with_client(
        &as_cache_remote(as_remote_backend(&backend), &remote),
        &config,
        &store,
        Some(&workspace), // workspace_crates
        true,             // pull_only
        false,            // push_only
        true,             // dry_run
        false,            // pull_all
        Some(&lock),      // lock_crates — must be ignored under --workspace
        true,             // pull_workspace
        false,            // allow_partial
    )
    .await
    .expect("workspace-scoped pull should list only the workspace member(s)");
    assert_eq!(backend.list_calls(), vec!["prefix/v3/manifests/wsfoo/"]);
}

#[tokio::test]
async fn sync_with_client_workspace_pull_errors_when_no_workspace_resolved() {
    // `--workspace` with an unresolved (None) or empty workspace set must
    // error, NOT silently fall back to a full-remote scan.
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
    let store = Store::open(&config).unwrap();
    let remote = test_remote_cfg();

    let empty: std::collections::HashSet<String> = std::collections::HashSet::new();
    let cases: [Option<&std::collections::HashSet<String>>; 2] = [None, Some(&empty)];

    for workspace_crates in cases {
        let backend = TestBackend::memory();
        let err = sync_with_client(
            &as_cache_remote(as_remote_backend(&backend), &remote),
            &config,
            &store,
            workspace_crates, // None or empty → cannot scope to workspace
            true,             // pull_only
            false,            // push_only
            true,             // dry_run
            false,            // pull_all
            None,             // lock_crates (always None under --workspace)
            true,             // pull_workspace
            false,            // allow_partial
        )
        .await
        .expect_err("--workspace with no resolved members must error, not scan the bucket");
        assert!(
            err.to_string().contains("no workspace members resolved"),
            "unexpected error: {err}"
        );
        assert!(
            backend.list_calls().is_empty(),
            "guard must run before listing the remote"
        );
    }
}

#[tokio::test]
async fn sync_with_client_push_uploads_local_only_entry() {
    // A populated local store + an empty remote: push-only sync uploads the
    // local entry end-to-end (real pack creation + manifest) through the
    // backend. Exercises the push loop and upload_entry, not just planning.
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
    let store = Store::open(&config).unwrap();

    // Materialize one cache entry with a single artifact file.
    let src_dir = dir.path().join("src");
    std::fs::create_dir_all(&src_dir).unwrap();
    let artifact = src_dir.join("libfoo.rlib");
    std::fs::write(&artifact, b"artifact bytes").unwrap();
    store
        .put(
            "pushkey123",
            "foo",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "debug",
            &[(artifact, "libfoo.rlib".to_string())],
            "",
            "",
        )
        .unwrap();

    let remote = test_remote_cfg();
    let backend = TestBackend::memory();

    sync_with_client(
        &as_cache_remote(as_remote_backend(&backend), &remote),
        &config,
        &store,
        None,
        false,
        true,
        false,
        false,
        None,
        false,
        false,
    )
    .await
    .expect("push sync should succeed");
    let puts = backend.put_calls();
    assert_eq!(puts.len(), 2);
    assert!(puts.contains(&"prefix/v3/packs/foo/pushkey123.tar.zst".to_string()));
    assert!(puts.contains(&"prefix/v3/manifests/foo/pushkey123.json".to_string()));
}

#[tokio::test]
async fn sync_with_client_push_throttles_with_low_concurrency() {
    // Two local entries with concurrency=1 force the push loop's
    // max-concurrency wait branch (the second upload waits for the first).
    let dir = tempfile::tempdir().unwrap();
    let mut config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
    config.s3_concurrency = 1;
    let store = Store::open(&config).unwrap();

    let src_dir = dir.path().join("src");
    std::fs::create_dir_all(&src_dir).unwrap();
    for (key, cn) in [("pusha1", "aaa"), ("pushb2", "bbb")] {
        let artifact = src_dir.join(format!("{cn}.rlib"));
        std::fs::write(&artifact, format!("{cn} bytes")).unwrap();
        store
            .put(
                key,
                cn,
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "debug",
                &[(artifact, format!("{cn}.rlib"))],
                "",
                "",
            )
            .unwrap();
    }

    let remote = test_remote_cfg();
    let backend = TestBackend::memory();

    sync_with_client(
        &as_cache_remote(as_remote_backend(&backend), &remote),
        &config,
        &store,
        None,
        false,
        true,
        false,
        false,
        None,
        false,
        false,
    )
    .await
    .expect("throttled push sync should succeed");
    assert_eq!(backend.put_calls().len(), 4);
}

#[tokio::test]
async fn sync_with_client_dry_run_plans_pull_for_remote_only_key() {
    // The remote lists a manifest for a key absent from the local store, so
    // the dry-run plan schedules a pull and returns without transferring.
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
    let store = Store::open(&config).unwrap();
    let remote = test_remote_cfg();
    let key = sync_test_cache_key("dry-run-remote-only");

    let backend = TestBackend::memory();
    backend
        .seed(
            &format!("prefix/v3/manifests/serde/{key}.json"),
            b"{}".to_vec(),
        )
        .await;
    sync_with_client(
        &as_cache_remote(as_remote_backend(&backend), &remote),
        &config,
        &store,
        None,
        false,
        false,
        true,
        false,
        None,
        false,
        false,
    )
    .await
    .expect("dry-run sync planning a pull should succeed");
    assert!(
        backend.get_calls().is_empty(),
        "dry-run must not download the pack"
    );
}

#[tokio::test]
async fn sync_with_client_pull_loop_records_failure_and_returns_err_by_default() {
    // A remote-only key drives a real (non-dry-run) pull. The served pack is
    // garbage, so download_entry errors — the pull loop must record the
    // failure and return Err by default.
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
    let store = Store::open(&config).unwrap();
    let remote = test_remote_cfg();
    let key = sync_test_cache_key("failed-pull");

    let backend = TestBackend::memory();
    backend
        .seed(
            &format!("prefix/v3/manifests/serde/{key}.json"),
            b"{}".to_vec(),
        )
        .await;
    backend
        .seed(
            &format!("prefix/v3/packs/serde/{key}.tar.zst"),
            b"not a valid pack".to_vec(),
        )
        .await;

    let err = sync_with_client(
        &as_cache_remote(as_remote_backend(&backend), &remote),
        &config,
        &store,
        None,
        true,
        false,
        false,
        false,
        None,
        false,
        false,
    )
    .await
    .expect_err("pull sync should return Err when a download fails by default");
    assert!(
        err.to_string()
            .contains("1 transfer(s) or import(s) failed")
    );
    assert_eq!(
        backend.get_calls(),
        vec![format!("prefix/v3/packs/serde/{key}.tar.zst")]
    );
}

#[tokio::test]
async fn sync_with_client_pull_allows_partial_when_flag_set() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
    let store = Store::open(&config).unwrap();
    let remote = test_remote_cfg();
    let key = sync_test_cache_key("failed-pull-partial");

    let backend = TestBackend::memory();
    backend
        .seed(
            &format!("prefix/v3/manifests/serde/{key}.json"),
            b"{}".to_vec(),
        )
        .await;
    backend
        .seed(
            &format!("prefix/v3/packs/serde/{key}.tar.zst"),
            b"not a valid pack".to_vec(),
        )
        .await;

    sync_with_client(
        &as_cache_remote(as_remote_backend(&backend), &remote),
        &config,
        &store,
        None,
        true,
        false,
        false,
        false,
        None,
        false,
        true,
    )
    .await
    .expect("pull sync with allow_partial should complete Ok despite failed download");
    assert_eq!(
        backend.get_calls(),
        vec![format!("prefix/v3/packs/serde/{key}.tar.zst")]
    );
}

#[tokio::test]
async fn sync_with_client_push_reports_failed_uploads_and_returns_err_by_default() {
    // A local-only entry is scheduled for push, but the backend rejects
    // uploads, so upload_entry errors and the loop records a failure.
    // Returns Err by default.
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
    let store = Store::open(&config).unwrap();
    let remote = test_remote_cfg();

    let src_dir = dir.path().join("src");
    std::fs::create_dir_all(&src_dir).unwrap();
    let artifact = src_dir.join("foo.rlib");
    std::fs::write(&artifact, b"foo bytes").unwrap();
    store
        .put(
            "pushfail1aaaa",
            "foo",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "debug",
            &[(artifact, "foo.rlib".to_string())],
            "",
            "",
        )
        .unwrap();

    let backend = TestBackend::failing_put();

    let err = sync_with_client(
        &as_cache_remote(as_remote_backend(&backend), &remote),
        &config,
        &store,
        None,
        false,
        true,
        false,
        false,
        None,
        false,
        false,
    )
    .await
    .expect_err("push sync should return Err when an upload fails by default");
    assert!(
        err.to_string()
            .contains("1 transfer(s) or import(s) failed")
    );
    assert_eq!(
        backend.put_calls(),
        vec!["prefix/v3/packs/foo/pushfail1aaaa.tar.zst"]
    );
}

#[tokio::test]
async fn sync_with_client_push_allows_partial_when_flag_set() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
    let store = Store::open(&config).unwrap();
    let remote = test_remote_cfg();

    let src_dir = dir.path().join("src");
    std::fs::create_dir_all(&src_dir).unwrap();
    let artifact = src_dir.join("foo.rlib");
    std::fs::write(&artifact, b"foo bytes").unwrap();
    store
        .put(
            "pushfail1aaaa",
            "foo",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "debug",
            &[(artifact, "foo.rlib".to_string())],
            "",
            "",
        )
        .unwrap();

    let backend = TestBackend::failing_put();

    sync_with_client(
        &as_cache_remote(as_remote_backend(&backend), &remote),
        &config,
        &store,
        None,
        false,
        true,
        false,
        false,
        None,
        false,
        true,
    )
    .await
    .expect("push sync with allow_partial should complete Ok even when an upload fails");
    assert_eq!(
        backend.put_calls(),
        vec!["prefix/v3/packs/foo/pushfail1aaaa.tar.zst"]
    );
}

#[tokio::test]
async fn sync_with_client_pull_throttles_with_low_concurrency() {
    // Two remote-only keys with concurrency=1 force the pull loop's
    // max-concurrency wait branch (the second download waits for the first
    // to drain a slot). Packs are garbage so each download fails fast, but
    // the throttle path is still exercised; with allow_partial, the sync completes Ok.
    let dir = tempfile::tempdir().unwrap();
    let mut config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
    config.s3_concurrency = 1;
    let store = Store::open(&config).unwrap();
    let remote = test_remote_cfg();

    let backend = TestBackend::memory();
    for (crate_name, key) in [
        ("aaa", sync_test_cache_key("throttled-pull-a")),
        ("bbb", sync_test_cache_key("throttled-pull-b")),
    ] {
        backend
            .seed(
                &format!("prefix/v3/manifests/{crate_name}/{key}.json"),
                b"{}".to_vec(),
            )
            .await;
        backend
            .seed(
                &format!("prefix/v3/packs/{crate_name}/{key}.tar.zst"),
                b"not a pack".to_vec(),
            )
            .await;
    }

    sync_with_client(
        &as_cache_remote(as_remote_backend(&backend), &remote),
        &config,
        &store,
        None,
        true,
        false,
        false,
        false,
        None,
        false,
        true,
    )
    .await
    .expect("throttled pull sync should complete Ok with allow_partial");
    assert_eq!(backend.get_calls().len(), 2);
}

/// Build a valid v3 entry pack (tar.zst) for `key`/`crate_name` from a
/// throwaway store, so tests can serve it as a GET body to drive the
/// download-success path.
fn build_entry_pack(key: &str, crate_name: &str) -> Vec<u8> {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = save_manifest_config(tmp.path().to_path_buf(), None);
    let store = Store::open(&cfg).unwrap();
    let src = tmp.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    let artifact = src.join("libfoo.rlib");
    std::fs::write(&artifact, b"real artifact bytes").unwrap();
    store
        .put(
            key,
            crate_name,
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "debug",
            &[(artifact, "libfoo.rlib".to_string())],
            "",
            "",
        )
        .unwrap();
    let entry_dir = store.entry_dir(key);
    let meta: crate::store::EntryMeta =
        serde_json::from_slice(&std::fs::read(entry_dir.join("meta.json")).unwrap()).unwrap();
    crate::remote_layout::create_entry_pack_zstd(&entry_dir, &store.blobs_dir(), &meta, 3).unwrap()
}

#[tokio::test]
async fn sync_with_client_pull_downloads_and_imports_entry() {
    // Remote lists a key absent locally; the GET returns a VALID pack, so
    // the pull downloads, extracts, and imports it into the local store.
    // Covers the pull SUCCESS path (download_entry + import), not just the
    // error path.
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
    let store = Store::open(&config).unwrap();
    let remote = test_remote_cfg();

    let key = sync_test_cache_key("successful-pull");
    let pack = build_entry_pack(&key, "serde");

    let backend = TestBackend::memory();
    backend
        .seed(
            &format!("prefix/v3/manifests/serde/{key}.json"),
            b"{}".to_vec(),
        )
        .await;
    backend
        .seed(&format!("prefix/v3/packs/serde/{key}.tar.zst"), pack)
        .await;

    sync_with_client(
        &as_cache_remote(as_remote_backend(&backend), &remote),
        &config,
        &store,
        None,
        true,
        false,
        false,
        false,
        None,
        false,
        false,
    )
    .await
    .expect("pull sync should succeed");

    // The entry was imported into the local store.
    assert!(
        config.store_dir().join(&key).join("meta.json").exists(),
        "pulled entry should be materialized in the local store"
    );
    assert_eq!(
        backend.get_calls(),
        vec![format!("prefix/v3/packs/serde/{key}.tar.zst")]
    );
}

struct DisappearingBackend {
    inner: Arc<TestBackend>,
    on_put_delete: std::path::PathBuf,
}

#[async_trait::async_trait]
impl crate::remote_backend::RemoteBackend for DisappearingBackend {
    async fn head(&self, key: &str) -> Result<bool> {
        self.inner.as_ref().head(key).await
    }
    async fn get(
        &self,
        key: &str,
        max_bytes: Option<u64>,
    ) -> Result<Option<crate::remote_backend::GetObject>> {
        self.inner.as_ref().get(key, max_bytes).await
    }
    async fn put(&self, key: &str, body: Vec<u8>, content_type: Option<&str>) -> Result<()> {
        if self.on_put_delete.exists() {
            let _ = std::fs::remove_dir_all(&self.on_put_delete);
        }
        self.inner.as_ref().put(key, body, content_type).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.as_ref().list(prefix).await
    }
    fn describe(&self, key: &str) -> String {
        self.inner.as_ref().describe(key)
    }
}

#[tokio::test]
async fn sync_with_client_push_fails_when_local_entry_disappears() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
    config.s3_concurrency = 1;
    let store = Store::open(&config).unwrap();
    let remote = test_remote_cfg();

    let src_dir = dir.path().join("src");
    std::fs::create_dir_all(&src_dir).unwrap();

    // Materialize two entries ("aaa" sorts before "zzz")
    for (k, cn) in [("keep_first", "aaa"), ("disappear_second", "zzz")] {
        let artifact = src_dir.join(format!("{cn}.rlib"));
        std::fs::write(&artifact, format!("{cn} bytes")).unwrap();
        store
            .put(
                k,
                cn,
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "debug",
                &[(artifact, format!("{cn}.rlib"))],
                "",
                "",
            )
            .unwrap();
    }

    let disappear_dir = config.store_dir().join("disappear_second");
    let backend: Arc<dyn crate::remote_backend::RemoteBackend> = Arc::new(DisappearingBackend {
        inner: TestBackend::memory(),
        on_put_delete: disappear_dir,
    });

    let err = sync_with_client(
        &as_cache_remote(backend, &remote),
        &config,
        &store,
        None,
        false,
        true,
        false,
        false,
        None,
        false,
        false,
    )
    .await
    .expect_err("disappeared local entry must cause non-zero exit by default");
    assert!(
        err.to_string()
            .contains("1 transfer(s) or import(s) failed")
    );
}

/// Build a tar.zst pack containing an invalid meta.json to test import failure.
fn build_invalid_meta_pack() -> Vec<u8> {
    let mut tar_builder = tar::Builder::new(Vec::new());
    let meta_bytes = b"{\"not\": \"valid meta\"}";
    let mut header = tar::Header::new_gnu();
    header.set_path("meta.json").unwrap();
    header.set_size(meta_bytes.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar_builder.append(&header, &meta_bytes[..]).unwrap();
    let tar_data = tar_builder.into_inner().unwrap();
    zstd::encode_all(&tar_data[..], 3).unwrap()
}

#[tokio::test]
async fn sync_with_client_pull_fails_when_import_fails_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
    let store = Store::open(&config).unwrap();
    let remote = test_remote_cfg();

    let key = sync_test_cache_key("invalid-meta-pull");
    let pack = build_invalid_meta_pack();

    let backend = TestBackend::memory();
    backend
        .seed(
            &format!("prefix/v3/manifests/foo/{key}.json"),
            b"{}".to_vec(),
        )
        .await;
    backend
        .seed(&format!("prefix/v3/packs/foo/{key}.tar.zst"), pack)
        .await;

    let err = sync_with_client(
        &as_cache_remote(as_remote_backend(&backend), &remote),
        &config,
        &store,
        None,
        true,
        false,
        false,
        false,
        None,
        false,
        false,
    )
    .await
    .expect_err("pull sync should return Err when import fails by default");
    assert!(
        err.to_string()
            .contains("1 transfer(s) or import(s) failed")
    );
}

#[tokio::test]
async fn sync_with_client_pull_allows_partial_when_import_fails() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
    let store = Store::open(&config).unwrap();
    let remote = test_remote_cfg();

    let key = sync_test_cache_key("invalid-meta-pull-allow");
    let pack = build_invalid_meta_pack();

    let backend = TestBackend::memory();
    backend
        .seed(
            &format!("prefix/v3/manifests/foo/{key}.json"),
            b"{}".to_vec(),
        )
        .await;
    backend
        .seed(&format!("prefix/v3/packs/foo/{key}.tar.zst"), pack)
        .await;

    sync_with_client(
        &as_cache_remote(as_remote_backend(&backend), &remote),
        &config,
        &store,
        None,
        true,
        false,
        false,
        false,
        None,
        false,
        true,
    )
    .await
    .expect("pull sync with allow_partial should return Ok even when import fails");
}

#[tokio::test]
async fn sync_with_client_mixed_success_and_failure_completes_all_and_fails_default() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
    let store = Store::open(&config).unwrap();
    let remote = test_remote_cfg();

    let key_good = sync_test_cache_key("mixed-good");
    let key_bad = sync_test_cache_key("mixed-bad");
    let pack_good = build_entry_pack(&key_good, "serde");

    let backend = TestBackend::memory();
    backend
        .seed(
            &format!("prefix/v3/manifests/serde/{key_good}.json"),
            b"{}".to_vec(),
        )
        .await;
    backend
        .seed(
            &format!("prefix/v3/packs/serde/{key_good}.tar.zst"),
            pack_good,
        )
        .await;

    backend
        .seed(
            &format!("prefix/v3/manifests/tokio/{key_bad}.json"),
            b"{}".to_vec(),
        )
        .await;
    backend
        .seed(
            &format!("prefix/v3/packs/tokio/{key_bad}.tar.zst"),
            b"corrupted bytes".to_vec(),
        )
        .await;

    // Default behavior: completes all transfers, but returns Err
    let err = sync_with_client(
        &as_cache_remote(as_remote_backend(&backend), &remote),
        &config,
        &store,
        None,
        true,
        false,
        false,
        false,
        None,
        false,
        false,
    )
    .await
    .expect_err("mixed sync should complete all transfers and return Err by default");
    assert!(
        err.to_string()
            .contains("1 transfer(s) or import(s) failed")
    );

    // Both GET calls were executed (no fail-fast abort)
    assert_eq!(backend.get_calls().len(), 2);
    // The good pack was successfully imported
    assert!(
        config
            .store_dir()
            .join(&key_good)
            .join("meta.json")
            .exists(),
        "good entry should be materialized"
    );
}

#[tokio::test]
async fn sync_with_client_mixed_success_and_failure_with_allow_partial_returns_ok() {
    let dir = tempfile::tempdir().unwrap();
    let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
    let store = Store::open(&config).unwrap();
    let remote = test_remote_cfg();

    let key_good = sync_test_cache_key("mixed-good-2");
    let key_bad = sync_test_cache_key("mixed-bad-2");
    let pack_good = build_entry_pack(&key_good, "serde");

    let backend = TestBackend::memory();
    backend
        .seed(
            &format!("prefix/v3/manifests/serde/{key_good}.json"),
            b"{}".to_vec(),
        )
        .await;
    backend
        .seed(
            &format!("prefix/v3/packs/serde/{key_good}.tar.zst"),
            pack_good,
        )
        .await;

    backend
        .seed(
            &format!("prefix/v3/manifests/tokio/{key_bad}.json"),
            b"{}".to_vec(),
        )
        .await;
    backend
        .seed(
            &format!("prefix/v3/packs/tokio/{key_bad}.tar.zst"),
            b"corrupted bytes".to_vec(),
        )
        .await;

    // With allow_partial: completes all transfers and returns Ok
    sync_with_client(
        &as_cache_remote(as_remote_backend(&backend), &remote),
        &config,
        &store,
        None,
        true,
        false,
        false,
        false,
        None,
        false,
        true,
    )
    .await
    .expect("mixed sync with allow_partial should complete Ok");

    assert_eq!(backend.get_calls().len(), 2);
    assert!(
        config
            .store_dir()
            .join(&key_good)
            .join("meta.json")
            .exists(),
        "good entry should be materialized"
    );
}

#[test]
fn draw_clean_renders_target_table() {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    let targets = vec![
        TargetEntry {
            path: std::path::PathBuf::from("/work/proj-a/target"),
            size: 5_000_000,
            cached_bytes: 3_000_000,
            estimated_reclaimable_bytes: 2_000_000,
            scan_identity: None,
            profiles: vec!["debug".to_string(), "release".to_string()],
            breakdown: CategoryBreakdown::default(),
            stale: false,
        },
        TargetEntry {
            path: std::path::PathBuf::from("/work/proj-b/target"),
            size: 2_000_000,
            cached_bytes: 0,
            estimated_reclaimable_bytes: 2_000_000,
            scan_identity: None,
            profiles: vec![],
            breakdown: CategoryBreakdown::default(),
            stale: false,
        },
    ];
    // First row selected (the one carrying cached bytes), cursor on it.
    let selected = vec![true, false];

    let mut terminal = Terminal::new(TestBackend::new(120, 40)).unwrap();
    terminal
        .draw(|frame| draw_clean(frame, &targets, &selected, 0, std::path::Path::new("/work")))
        .expect("clean selector draw should succeed");
    let buffer = terminal.backend().buffer().clone();
    let rendered: String = buffer.content().iter().map(|c| c.symbol()).collect();
    assert!(rendered.contains("kache clean"), "header should render");
    assert!(
        rendered.contains("proj-a") && rendered.contains("proj-b"),
        "both target rows should render"
    );
    // The selected row's checkbox is set.
    assert!(rendered.contains("[x]"), "selected row shows a checked box");
    // Selection uses the separate scan-time reclaim estimate, not cached bytes.
    assert!(
        rendered.contains("Selected: 1 (est. 1.9 MiB)"),
        "selected total subtracts cached bytes, got: {rendered}"
    );
    assert!(
        !rendered.contains("Selected: 1 (est. 4.8 MiB)"),
        "selected total must not report the raw size"
    );
    // The header's dir totals stay raw (4.8 + 1.9 MiB, 2.9 MiB cached).
    assert!(
        rendered.contains("2 dirs (6.7 MiB total, 2.9 MiB cached)"),
        "header totals remain raw sizes"
    );
}

#[test]
fn render_clean_dry_run_formats_plural_profiles_and_fallback_paths() {
    // Dry-run formatter -> plural/singular, profile tags, and strip fallback.
    let root = std::path::Path::new("/work");
    let single = vec![TargetEntry {
        path: std::path::PathBuf::from("/work/proj/target"),
        size: 1024,
        cached_bytes: 512,
        estimated_reclaimable_bytes: 512,
        scan_identity: None,
        profiles: vec!["debug".to_string()],
        breakdown: CategoryBreakdown::default(),
        stale: false,
    }];
    let none = std::collections::HashSet::new();
    let single_out = render_clean_dry_run(&single, root, &none).join("\n");
    assert!(single_out.contains("Found 1 target/ directory"));
    assert!(single_out.contains("proj/target"));
    assert!(single_out.contains("[debug]"));
    assert!(single_out.contains("Dry run: estimated to free 512 B"));
    assert!(single_out.contains("(512 B of apparent size is shared, sparse, or duplicate)"));

    let many = vec![
        TargetEntry {
            path: std::path::PathBuf::from("/work/proj-a/target"),
            size: 10,
            cached_bytes: 0,
            estimated_reclaimable_bytes: 10,
            scan_identity: None,
            profiles: Vec::new(),
            breakdown: CategoryBreakdown::default(),
            stale: false,
        },
        TargetEntry {
            path: std::path::PathBuf::from("/outside/proj-b/target"),
            size: 20,
            cached_bytes: 5,
            estimated_reclaimable_bytes: 15,
            scan_identity: None,
            profiles: vec!["release".to_string()],
            breakdown: CategoryBreakdown::default(),
            stale: false,
        },
    ];
    let many_out = render_clean_dry_run(&many, root, &none).join("\n");
    assert!(!many_out.contains("worktree deleted"));
    let orphaned = [std::path::PathBuf::from("/outside/proj-b/target")].into();
    let marked = render_clean_dry_run(&many, root, &orphaned).join("\n");
    assert_eq!(marked.matches("(worktree deleted)").count(), 1, "{marked}");
    assert!(many_out.contains("Found 2 target/ directories"));
    assert!(many_out.contains("/outside/proj-b/target"));
    assert!(many_out.contains("Dry run: estimated to free 25 B"));
    assert!(many_out.contains("(5 B of apparent size is shared, sparse, or duplicate)"));
    assert!(!many_out.contains("estimated to free 30 B"));
}

#[test]
fn clean_handle_key_navigation_and_selection() {
    use crossterm::event::KeyCode;
    let mut selected = vec![false, false, false];
    let mut cursor = 0usize;
    let len = 3;

    // Down moves the cursor; clamped at the end.
    assert_eq!(
        clean_handle_key(KeyCode::Down, &mut selected, &mut cursor, len),
        CleanStep::Continue
    );
    assert_eq!(cursor, 1);
    // Up moves back; saturates at 0.
    clean_handle_key(KeyCode::Up, &mut selected, &mut cursor, len);
    assert_eq!(cursor, 0);
    clean_handle_key(KeyCode::Up, &mut selected, &mut cursor, len);
    assert_eq!(cursor, 0, "up saturates at 0");

    // Space toggles the current row and advances.
    clean_handle_key(KeyCode::Char(' '), &mut selected, &mut cursor, len);
    assert!(selected[0]);
    assert_eq!(cursor, 1);

    // Select-all / select-none.
    clean_handle_key(KeyCode::Char('a'), &mut selected, &mut cursor, len);
    assert!(selected.iter().all(|s| *s));
    clean_handle_key(KeyCode::Char('n'), &mut selected, &mut cursor, len);
    assert!(selected.iter().all(|s| !*s));
}

#[test]
fn clean_handle_key_handles_boundaries() {
    use crossterm::event::KeyCode;
    // Empty/edge state -> no panic and cursor stays bounded.
    let mut empty = Vec::new();
    let mut empty_cursor = 0usize;
    assert_eq!(
        clean_handle_key(KeyCode::Char(' '), &mut empty, &mut empty_cursor, 0),
        CleanStep::Continue
    );
    assert_eq!(empty_cursor, 0);

    let mut selected = vec![false, false];
    let mut cursor = 1usize;
    clean_handle_key(KeyCode::Down, &mut selected, &mut cursor, 2);
    assert_eq!(cursor, 1, "down clamps at last row");
    clean_handle_key(KeyCode::Char(' '), &mut selected, &mut cursor, 2);
    assert!(selected[1]);
    assert_eq!(cursor, 1, "space on last row does not advance");

    cursor = 10;
    clean_handle_key(KeyCode::Char(' '), &mut selected, &mut cursor, 2);
    assert_eq!(cursor, 10, "out-of-range cursor is ignored");
}

#[test]
fn remove_targets_deletes_all_and_reports_estimates() {
    // Two real target/ dirs under a root; --yes removes every one and sums
    // the scan-time estimate/gap only for successful removals.
    let root = tempfile::tempdir().unwrap();
    let a = root.path().join("proj-a/target");
    let b = root.path().join("proj-b/target");
    std::fs::create_dir_all(&a).unwrap();
    std::fs::create_dir_all(&b).unwrap();

    let to_remove = vec![
        RemovalTarget {
            path: a.clone(),
            scanned_identity: directory_identity(&a),
            estimated_reclaimable: 60,
            apparent_gap: 40,
        },
        RemovalTarget {
            path: b.clone(),
            scanned_identity: directory_identity(&b),
            estimated_reclaimable: 200,
            apparent_gap: 0,
        },
    ];
    let (removed, estimated_reclaimed, apparent_gap) =
        remove_targets(&to_remove, root.path(), false, &mut Vec::new());

    assert_eq!(removed, 2, "both target/ dirs removed");
    assert_eq!(estimated_reclaimed, 260);
    assert_eq!(apparent_gap, 40);
    assert!(!a.exists() && !b.exists(), "directories are gone from disk");
}

#[test]
fn remove_targets_skips_failures_without_aborting() {
    // A missing path fails to remove; a real one after it still succeeds and
    // only the removed dir's bytes are counted.
    let root = tempfile::tempdir().unwrap();
    let missing = root.path().join("gone/target");
    let real = root.path().join("proj/target");
    std::fs::create_dir_all(&real).unwrap();

    let to_remove = vec![
        RemovalTarget {
            path: missing,
            scanned_identity: None,
            estimated_reclaimable: 90,
            apparent_gap: 10,
        },
        RemovalTarget {
            path: real.clone(),
            scanned_identity: directory_identity(&real),
            estimated_reclaimable: 150,
            apparent_gap: 50,
        },
    ];
    let (removed, estimated_reclaimed, apparent_gap) =
        remove_targets(&to_remove, root.path(), false, &mut Vec::new());

    assert_eq!(removed, 1, "only the existing dir counts as removed");
    assert_eq!(
        estimated_reclaimed, 150,
        "failed dir's estimate is not counted"
    );
    assert_eq!(apparent_gap, 50, "failed dir's gap is not counted");
    assert!(!real.exists(), "the reachable dir was still removed");
}

#[test]
fn a_target_whose_build_lock_is_held_is_in_use() {
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("target");
    let debug = target.join("debug");
    let cross = target.join("x86_64-unknown-linux-gnu/release");
    let too_deep = target.join("a/b/c/d");
    for dir in [&debug, &cross, &too_deep] {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join(".cargo-lock"), b"").unwrap();
        // Build output beside the lock is not a lock.
        std::fs::write(dir.join("libfoo.rlib"), b"rlib").unwrap();
    }
    let mut found = cargo_lock_files(&target, 3);
    found.sort();
    let mut expected = vec![debug.join(".cargo-lock"), cross.join(".cargo-lock")];
    expected.sort();
    assert_eq!(found, expected, "profiles and triple profiles, not deeper");
    assert!(!target_in_use(&target));

    let held = std::fs::File::open(cross.join(".cargo-lock")).unwrap();
    held.lock().unwrap();
    assert!(target_in_use(&target));
    // Unlock rather than only drop: a child another test forks while the
    // file is open shares its lock until the child execs.
    held.unlock().unwrap();
    drop(held);
    assert!(!target_in_use(&target));
}

#[test]
fn remove_targets_leaves_a_target_a_build_is_writing() {
    let root = tempfile::tempdir().unwrap();
    let busy = root.path().join("busy/target");
    let idle = root.path().join("idle/target");
    for target in [&busy, &idle] {
        std::fs::create_dir_all(target.join("debug")).unwrap();
        std::fs::write(target.join("debug/.cargo-lock"), b"").unwrap();
    }
    let held = std::fs::File::open(busy.join("debug/.cargo-lock")).unwrap();
    held.lock().unwrap();

    let to_remove = [&busy, &idle].map(|path| RemovalTarget {
        path: path.clone(),
        scanned_identity: directory_identity(path),
        estimated_reclaimable: 10,
        apparent_gap: 0,
    });
    let mut skipped = Vec::new();
    let (removed, reclaimed, _) = remove_targets(&to_remove, root.path(), true, &mut skipped);
    assert_eq!((removed, reclaimed), (1, 10));
    assert!(busy.exists() && !idle.exists());
    assert_eq!(skipped.len(), 1);
    assert_eq!(skipped[0].path, busy.display().to_string());
    assert_eq!(skipped[0].reason, TARGET_IN_USE);
}

#[test]
fn idle_times_use_the_largest_whole_unit() {
    assert_eq!(format_idle(59), "59s");
    assert_eq!(format_idle(60), "1m");
    assert_eq!(format_idle(3_599), "59m");
    assert_eq!(format_idle(3_600), "1h");
    assert_eq!(format_idle(86_399), "23h");
    assert_eq!(format_idle(86_400), "1d");
}

const CARGO_CACHEDIR_TAG: &str = "Signature: 8a477f597d28d172789f06886806bc55";

fn row(workspace: &str, state: TargetState, reclaimable: u64) -> TargetRow {
    TargetRow {
        path: format!("{workspace}/target"),
        workspace: workspace.to_string(),
        state,
        idle_seconds: 3 * 86_400,
        profiles: vec!["debug".to_string()],
        apparent_bytes: reclaimable * 2,
        reclaimable_bytes: reclaimable,
        cached_bytes: reclaimable,
    }
}

#[test]
fn json_targets_suggest_the_orphan_clean_only_when_a_worktree_is_gone() {
    let live = [row("/wt/a", TargetState::Live, 1)];
    assert!(targets_next_actions(&live).is_empty());
    let one = [
        row("/wt/a", TargetState::Live, 1),
        row("/wt/b", TargetState::WorktreeDeleted, 1),
    ];
    let next = targets_next_actions(&one);
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].argv, ["kache", "clean", "--orphans", "--yes"]);
    assert_eq!(next[0].why, "1 target(s) belong to deleted worktrees");
}

#[test]
fn the_targets_table_totals_and_points_at_deleted_worktrees() {
    let one = render_targets(&[row("/wt/a", TargetState::Live, 1024)]).join("\n");
    assert!(
        one.starts_with("1 tracked target directory: 2.0 KiB on disk, 1.0 KiB freeable"),
        "{one}"
    );
    assert!(one.contains("3d  /wt/a"), "{one}");
    assert!(!one.contains("deleted"), "{one}");

    let rows = [
        row("/wt/a", TargetState::Live, 1024),
        row("/wt/b", TargetState::WorktreeDeleted, 2048),
    ];
    let many = render_targets(&rows).join("\n");
    assert!(
        many.starts_with("2 tracked target directories: 6.0 KiB on disk, 3.0 KiB freeable"),
        "{many}"
    );
    assert_eq!(many.matches("(worktree deleted)").count(), 1, "{many}");
    assert!(
        many.contains("Remove the 1 deleted worktree's targets: kache clean --orphans --yes"),
        "{many}"
    );
    let two = render_targets(&[
        row("/wt/b", TargetState::WorktreeDeleted, 1),
        row("/wt/c", TargetState::WorktreeDeleted, 1),
    ])
    .join("\n");
    assert!(
        two.contains("Remove the 2 deleted worktrees' targets"),
        "{two}"
    );
}

#[test]
fn target_rows_report_each_worktree_and_sort_by_what_frees_most() {
    let dir = tempfile::tempdir().unwrap();
    let config = crate::test_support::test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let mut targets = Vec::new();
    for (name, bytes) in [("small", 4096usize), ("large", 64 * 1024)] {
        let workspace = dir.path().join("wt").join(name);
        let target = dir.path().join("targets").join(name);
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(target.join("debug/deps")).unwrap();
        std::fs::write(target.join("debug/deps/libx.rlib"), vec![1u8; bytes]).unwrap();
        std::fs::write(target.join("CACHEDIR.TAG"), CARGO_CACHEDIR_TAG).unwrap();
        store.remember_target_root(&target, &workspace).unwrap();
        targets.push((workspace, target));
    }
    std::fs::remove_dir_all(&targets[1].0).unwrap();
    std::fs::create_dir_all(dir.path().join("targets/gone")).unwrap();
    std::fs::write(
        dir.path().join("targets/gone/CACHEDIR.TAG"),
        CARGO_CACHEDIR_TAG,
    )
    .unwrap();
    store
        .remember_target_root(&dir.path().join("targets/gone"), &targets[0].0)
        .unwrap();
    std::fs::remove_dir_all(dir.path().join("targets/gone")).unwrap();

    let now = kache_store::markers::now_epoch_secs() as i64 + 120;
    let rows = target_rows(&config, now).unwrap();
    assert_eq!(rows.len(), 2, "a vanished target is not listed: {rows:?}");
    assert!(rows[0].path.ends_with("large") && rows[1].path.ends_with("small"));
    assert_eq!(rows[0].state, TargetState::WorktreeDeleted);
    assert_eq!(rows[1].state, TargetState::Live);
    assert!(rows[0].reclaimable_bytes >= 64 * 1024, "{rows:?}");
    assert!(
        rows.iter()
            .all(|row| (120..=125).contains(&row.idle_seconds)),
        "{rows:?}"
    );
    assert_eq!(rows[1].profiles, ["debug"]);
}

#[test]
fn a_workspace_is_gone_only_when_its_parent_is_still_there() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("wt");
    std::fs::create_dir(&workspace).unwrap();
    assert!(!workspace_is_gone(&workspace));
    std::fs::remove_dir(&workspace).unwrap();
    assert!(workspace_is_gone(&workspace));
    // An unmounted volume looks like a missing parent, not a deletion.
    assert!(!workspace_is_gone(&root.path().join("offline/wt")));
}

#[test]
fn tracked_selection_takes_orphans_at_any_age() {
    let stale = TrackedSelection::StaleOrOrphaned(1);
    assert!(!stale.includes(false, 3599));
    assert!(stale.includes(false, 3600));
    assert!(stale.includes(true, 0));
    assert!(!TrackedSelection::Orphaned.includes(false, i64::MAX));
    assert!(TrackedSelection::Orphaned.includes(true, 0));
}

#[test]
fn a_freshly_built_target_is_offered_once_its_worktree_is_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let config = crate::test_support::test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let workspace = dir.path().join("worktrees/feature");
    let target = dir.path().join("targets/feature");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(target.join("debug")).unwrap();
    std::fs::write(
        target.join("CACHEDIR.TAG"),
        "Signature: 8a477f597d28d172789f06886806bc55",
    )
    .unwrap();
    store.remember_target_root(&target, &workspace).unwrap();
    let target = std::path::absolute(&target).unwrap();

    let (live, _, _) =
        tracked_target_entries(&config, TrackedSelection::StaleOrOrphaned(24)).unwrap();
    assert!(live.is_empty(), "seen just now and the worktree is there");

    std::fs::remove_dir_all(&workspace).unwrap();
    for selection in [
        TrackedSelection::StaleOrOrphaned(24),
        TrackedSelection::Orphaned,
    ] {
        let (targets, _, orphans) = tracked_target_entries(&config, selection).unwrap();
        assert_eq!(
            targets.iter().map(|t| t.path.clone()).collect::<Vec<_>>(),
            std::slice::from_ref(&target)
        );
        assert!(orphans.contains(&target));
    }
}

#[test]
fn remove_targets_refuses_a_directory_replaced_after_scan() {
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("proj/target");
    let moved = root.path().join("proj/scanned-target");
    std::fs::create_dir_all(&target).unwrap();
    let scanned_identity = directory_identity(&target);
    std::fs::rename(&target, &moved).unwrap();
    std::fs::create_dir(&target).unwrap();

    let to_remove = vec![RemovalTarget {
        path: target.clone(),
        scanned_identity,
        estimated_reclaimable: 100,
        apparent_gap: 0,
    }];
    let (removed, estimated_reclaimed, apparent_gap) =
        remove_targets(&to_remove, root.path(), false, &mut Vec::new());

    assert_eq!((removed, estimated_reclaimed, apparent_gap), (0, 0, 0));
    assert!(
        target.exists(),
        "replacement directory must be left untouched"
    );
    assert!(
        moved.exists(),
        "the scanned directory was moved, not removed"
    );
}

#[test]
fn clean_handle_key_cancel_and_confirm() {
    use crossterm::event::KeyCode;
    let mut selected = vec![true];
    let mut cursor = 0usize;
    assert_eq!(
        clean_handle_key(KeyCode::Char('q'), &mut selected, &mut cursor, 1),
        CleanStep::Cancel
    );
    assert_eq!(
        clean_handle_key(KeyCode::Esc, &mut selected, &mut cursor, 1),
        CleanStep::Cancel
    );
    assert_eq!(
        clean_handle_key(KeyCode::Enter, &mut selected, &mut cursor, 1),
        CleanStep::Confirm
    );
    // An unhandled key is a no-op Continue.
    assert_eq!(
        clean_handle_key(KeyCode::Char('z'), &mut selected, &mut cursor, 1),
        CleanStep::Continue
    );
}

#[test]
fn clean_event_applies_press_but_ignores_release() {
    use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
    let key_event = |kind| {
        Event::Key(KeyEvent::new_with_kind(
            KeyCode::Char('q'),
            KeyModifiers::NONE,
            kind,
        ))
    };
    let mut selected = vec![false];
    let mut cursor = 0;

    assert_eq!(
        clean_handle_event(
            key_event(KeyEventKind::Release),
            &mut selected,
            &mut cursor,
            1,
        ),
        CleanStep::Continue,
        "key release must not repeat the action"
    );
    assert_eq!(
        clean_handle_event(
            key_event(KeyEventKind::Repeat),
            &mut selected,
            &mut cursor,
            1,
        ),
        CleanStep::Continue,
        "key repeat must not repeat the action"
    );
    assert_eq!(
        clean_handle_event(
            key_event(KeyEventKind::Press),
            &mut selected,
            &mut cursor,
            1,
        ),
        CleanStep::Cancel,
        "key press must apply the action"
    );
}

#[tokio::test]
async fn sync_with_client_push_skipped_when_remote_readonly() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
    config.remote_readonly = true;
    let store = Store::open(&config).unwrap();

    // Materialize one cache entry with a single artifact file.
    let src_dir = dir.path().join("src");
    std::fs::create_dir_all(&src_dir).unwrap();
    let artifact = src_dir.join("libfoo.rlib");
    std::fs::write(&artifact, b"artifact bytes").unwrap();
    store
        .put(
            "pushkey123",
            "foo",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "debug",
            &[(artifact, "libfoo.rlib".to_string())],
            "",
            "",
        )
        .unwrap();

    let remote = test_remote_cfg();
    // Since remote_readonly is true, the plan lists the remote keys but
    // does not push.
    let backend = TestBackend::memory();

    sync_with_client(
        &as_cache_remote(as_remote_backend(&backend), &remote),
        &config,
        &store,
        None,
        false,
        true,
        false,
        false,
        None,
        false,
        false,
    )
    .await
    .expect("push sync should succeed (by skipping pushes)");
    assert!(backend.put_calls().is_empty());
}

#[tokio::test]
async fn save_manifest_skipped_when_remote_readonly() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
    config.remote_readonly = true;

    // Create an event so save_manifest wouldn't normally skip due to empty events.
    let event_log = config.event_log_path();
    std::fs::create_dir_all(event_log.parent().unwrap()).unwrap();
    let event = serde_json::json!({
        "ts": chrono::Utc::now().to_rfc3339(),
        "crate_name": "foo",
        "result": "Miss",
        "elapsed_ms": 100,
        "compile_time_ms": 100,
        "size": 10,
        "cache_key": "key123"
    });
    let mut file = std::fs::File::create(&event_log).unwrap();
    use std::io::Write;
    writeln!(file, "{event}").unwrap();

    // Calling save_manifest should return Ok immediately without creating
    // a remote client or making any calls.
    save_manifest(&config, Some("mykey"), None)
        .expect("save_manifest should succeed by doing nothing");
}
