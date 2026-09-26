use super::*;

/// A warm phase with tolerated wrapped-compile failures (build-script
/// feature probes that rustc rejected — kache `EventResult::Error`) must
/// NOT degrade the run: those are not kache cache faults. Regression guard
/// for kunobi-ninja/kache (SurrealDB bench surfaced one such probe).
#[test]
fn run_archive_keeps_otlp_sidecars() {
    assert!(is_run_artifact("metrics.otlp.json"));
    assert!(is_run_artifact("schema_version"));
    assert!(is_run_artifact("bench-firefox.json"));
    assert!(!is_run_artifact("kache.log"));
}

#[test]
fn write_cache_otlp_or_warn_runs_telemetry_write() {
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("invoked.txt");
    let kache = fake_kache_that_records(dir.path(), &marker, 0);
    assert!(write_otlp_until_spawned(
        &kache,
        dir.path(),
        &marker,
        "bench-firefox",
        "warm"
    ));
    let invoked = std::fs::read_to_string(&marker).unwrap();
    assert!(
        invoked.contains("telemetry"),
        "expected telemetry subcommand, got {invoked:?}"
    );
    assert!(
        invoked.contains("write"),
        "expected write subcommand, got {invoked:?}"
    );
    assert!(
        invoked.contains("bench-firefox") && invoked.contains("warm"),
        "expected scenario and phase, got {invoked:?}"
    );
}

#[test]
fn write_cache_otlp_or_warn_is_false_when_kache_exits_nonzero() {
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("invoked.txt");
    let kache = fake_kache_that_records(dir.path(), &marker, 1);
    assert!(!write_otlp_until_spawned(
        &kache,
        dir.path(),
        &marker,
        "bench-firefox",
        "cold"
    ));
    assert!(marker.is_file(), "the stand-in never ran");
}

#[test]
fn write_cache_otlp_or_warn_is_false_when_kache_is_missing() {
    let dir = tempfile::tempdir().unwrap();
    assert!(!write_cache_otlp_or_warn(
        &dir.path().join("no-such-kache"),
        dir.path(),
        &dir.path().join("kache.toml"),
        &dir.path().join("cache-otlp-pull"),
        "bench-firefox",
        "pull",
    ));
}

/// The denominator is the cold phase's pre-pass count, because that is
/// one per rustc unit and nothing else in the result is. Using the crate
/// count would have reported hk as "890 of 890 skipped" when 315 units ran
/// the pre-pass and none skipped: cc compiles inflate the crate count and
/// never run one.
#[test]
fn the_prediction_line_counts_rustc_units_not_all_compiles() {
    let warm = |runs: u64, mismatches: u64| PhaseTimes {
        dep_info_runs: runs,
        prediction_mismatches: mismatches,
        ..PhaseTimes::default()
    };

    let used = prediction_summary_line(&warm(28, 0), 306).expect("predictions were used");
    assert!(used.contains("278 of 306 rustc units"), "{used}");
    assert!(used.contains("0 sampled check(s) disagreed"), "{used}");

    let disagreed = prediction_summary_line(&warm(28, 4), 306).unwrap();
    assert!(
        disagreed.contains("4 sampled check(s) disagreed"),
        "{disagreed}"
    );

    assert_eq!(
        prediction_summary_line(&warm(315, 0), 315),
        None,
        "every unit ran the pre-pass, so predictions did nothing here"
    );
    assert_eq!(
        prediction_summary_line(&warm(0, 0), 0),
        None,
        "an arm with no rustc units has nothing to report"
    );
    assert_eq!(
        prediction_summary_line(&warm(400, 0), 306),
        None,
        "more warm spawns than cold is not a skip count; say nothing"
    );
}

/// Every field here is a key under `timing` in `kache report --format
/// json`. Nothing else couples the two, and the previous version of this
/// test built its fixture from the code's own assumption — it fed a
/// `{"summary": {...}}` object and passed while production read the wrong
/// object and got zero for everything.
///
/// So the oracle is the published schema, which is the actual contract.
#[test]
fn phase_times_read_the_fields_the_schema_publishes_under_timing() {
    let schema: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../docs/commands/report.schema.json"),
        )
        .expect("the published report schema"),
    )
    .expect("the schema is JSON");

    let declared = |object: &str, field: &str| {
        schema["properties"][object]["properties"]
            .get(field)
            .is_some()
    };

    // The names `from_raw` reads, listed once so the assertion below and
    // production cannot disagree about the set.
    let fields = [
        "total_startup_ms",
        "total_key_ms",
        "total_dep_info_ms",
        "dep_info_runs",
        "prediction_mismatches",
        "total_lookup_ms",
        "total_wait_ms",
        "total_restore_ms",
        "total_store_ms",
        "total_unattributed_ms",
    ];
    for field in fields {
        assert!(
            declared("timing", field),
            "{field} is not published under `timing`; from_raw reads it there"
        );
        assert!(
            !declared("summary", field),
            "{field} also exists under `summary` — the two objects must not \
                 both carry it, or reading the wrong one stays invisible"
        );
    }

    // And the values arrive, keyed off that object.
    let raw = serde_json::json!({
        "timing": {
            "total_startup_ms": 11, "total_key_ms": 22, "total_dep_info_ms": 33,
            "dep_info_runs": 44, "prediction_mismatches": 55, "total_lookup_ms": 66,
            "total_wait_ms": 77, "total_restore_ms": 88, "total_store_ms": 99,
            "total_unattributed_ms": 111,
        },
        "summary": { "total_key_ms": 999 },
    });
    let phases = PhaseTimes::from_raw(&raw);
    assert_eq!(phases.startup_ms, 11);
    assert_eq!(phases.key_ms, 22, "a value under `summary` must not win");
    assert_eq!(phases.dep_info_ms, 33);
    assert_eq!(phases.dep_info_runs, 44);
    assert_eq!(phases.prediction_mismatches, 55);
    assert_eq!(phases.lookup_ms, 66);
    assert_eq!(phases.wait_ms, 77);
    assert_eq!(phases.restore_ms, 88);
    assert_eq!(phases.store_ms, 99);
    assert_eq!(phases.unattributed_ms, 111);

    // An external backend, or a report from before these fields existed.
    assert_eq!(
        PhaseTimes::from_raw(&serde_json::json!({"timing": {}})),
        PhaseTimes::default()
    );
    assert_eq!(
        PhaseTimes::from_raw(&serde_json::json!({})),
        PhaseTimes::default()
    );
}

/// Call `write_cache_otlp_or_warn` until the stand-in it spawns actually
/// ran, and return what the call reported.
///
/// Linux refuses to exec a file any process still holds open for writing
/// (ETXTBSY). This suite runs in parallel and spawns processes, so a child
/// can inherit the writable fd of a stand-in another test just wrote, and
/// the exec fails for as long as that child lives — microseconds, and
/// nothing to do with the behaviour under test. macOS does not enforce
/// this at all, which is how a test that fails on Linux and passes here
/// shipped: nothing runs the e2e crate's tests on Linux except the
/// mutation lane's baseline, and only when a change touches this crate.
///
/// The retry is bounded, so a stand-in that genuinely never runs still
/// fails the assertion that follows rather than hanging.
fn write_otlp_until_spawned(
    kache: &Path,
    dir: &Path,
    marker: &Path,
    scenario: &str,
    phase: &str,
) -> bool {
    let mut reported = false;
    for _ in 0..50 {
        reported = write_cache_otlp_or_warn(
            kache,
            dir,
            &dir.join("kache.toml"),
            &dir.join(format!("cache-otlp-{phase}")),
            scenario,
            phase,
        );
        if marker.is_file() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    reported
}

fn fake_kache_that_records(dir: &Path, marker: &Path, exit: i32) -> PathBuf {
    #[cfg(unix)]
    {
        let path = dir.join("fake-kache");
        kache_fs::testutil::write_executable(
            &path,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$0\" \"$@\" > '{}'\nexit {exit}\n",
                marker.display()
            ),
        );
        path
    }
    #[cfg(windows)]
    {
        let path = dir.join("fake-kache.cmd");
        std::fs::write(
            &path,
            format!(
                "@echo off\r\necho %* > \"{}\"\r\nexit /b {exit}\r\n",
                marker.display()
            ),
        )
        .unwrap();
        path
    }
}

#[test]
fn write_otlp_or_warn_writes_artifact_pair() {
    let dir = tempfile::tempdir().unwrap();
    write_otlp_or_warn(
        dir.path(),
        crate::bench_otlp::OtlpRun {
            project: "bench-test".into(),
            git_ref: "main".into(),
            cache_tool: "kache",
            cache_tool_version: "kache 0.19.0".into(),
            time_unix_nano: "1".into(),
            verdict_ok: true,
            speedup: None,
            cache_size_bytes: 0,
            key_stability_pct: None,
            disk_measured_bytes: None,
            disk_footprint_bytes: 0,
            phases: Vec::new(),
        },
    );
    assert!(dir.path().join(crate::bench_otlp::METRICS_FILE).is_file());
    assert!(
        dir.path()
            .join(crate::bench_otlp::SCHEMA_VERSION_FILE)
            .is_file()
    );
}

#[test]
fn wrapped_compile_failures_do_not_degrade_the_run() {
    let stability = KeyStability {
        stable_pct: Some(96.9),
        stable: 560,
        compared: 578,
    };
    let warm = PhaseMetrics {
        // 5 probes the compiler rejected — these used to flip the verdict.
        errors: 5,
        event_log: EventLogStats {
            total: 800,
            cached: 700,
            passed_through: 100,
            ..Default::default()
        },
        ..Default::default()
    };
    let spec = ScenarioAssertSpec {
        min_key_stability_pct: Some(50.0),
        max_passthrough_pct: Some(40.0),
        max_errors: Some(0),
        ..Default::default()
    };

    let verdict = Verdict::evaluate(&stability, &warm, Some(&spec));

    assert!(
        verdict.ok,
        "tolerated probe failures must not degrade the run: {:?}",
        verdict.issues
    );
    let errs = verdict
        .checks
        .iter()
        .find(|c| c.name == "max_errors")
        .expect("max_errors check is still surfaced");
    assert!(
        errs.passed,
        "max_errors must pass on tolerated probe failures"
    );
    assert!(
        errs.actual.contains('5'),
        "the probe-failure count is surfaced for visibility: {}",
        errs.actual
    );
}

#[test]
fn resolve_binary_prefers_existing_then_exe() {
    let dir = std::env::temp_dir().join(format!("kb-resolvebin-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("foo.exe"), b"x").unwrap();
    assert_eq!(resolve_binary(&dir.join("foo")), dir.join("foo.exe"));
    std::fs::write(dir.join("bar"), b"x").unwrap();
    assert_eq!(resolve_binary(&dir.join("bar")), dir.join("bar"));
    assert_eq!(resolve_binary(&dir.join("nope")), dir.join("nope"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn posix_sh_resolves_a_shell() {
    let sh = posix_sh().expect("a POSIX sh should be resolvable on the test host");
    let name = sh.file_name().unwrap().to_string_lossy().to_lowercase();
    assert!(name.starts_with("sh"), "unexpected shell: {}", sh.display());
}

#[test]
fn de_verbatim_strips_windows_extended_prefix() {
    assert_eq!(
        de_verbatim(PathBuf::from("/tmp/x")),
        PathBuf::from("/tmp/x")
    );
    #[cfg(windows)]
    {
        assert_eq!(
            de_verbatim(PathBuf::from(r"\\?\C:\a\b")),
            PathBuf::from(r"C:\a\b")
        );
        assert_eq!(
            de_verbatim(PathBuf::from(r"\\?\UNC\srv\share")),
            PathBuf::from(r"\\srv\share")
        );
    }
}

#[test]
fn parse_key_line_extracts_crate_and_payload_from_default_tracing_format() {
    let line = "2026-05-24T16:45:23.123Z TRACE kache::cache_key: \
                    [key:gkrust] env_dep:CARGO_MANIFEST_DIR=/abs/path";
    let parsed = parse_key_line(line);
    assert_eq!(
        parsed,
        Some((
            "gkrust".to_string(),
            "env_dep:CARGO_MANIFEST_DIR=/abs/path".to_string()
        ))
    );
}

#[test]
fn parse_key_line_returns_none_for_non_key_lines() {
    for line in [
        "",
        "2026-05-24T16:45:23.123Z INFO kache: starting build",
        "WARN kache::path_normalizer: residual absolute path detected",
        "[key:gkrust]",  // payload empty
        "[key:gkrust] ", // payload whitespace only
    ] {
        assert!(
            parse_key_line(line).is_none(),
            "line should not parse: {line:?}"
        );
    }
}

#[test]
fn normalize_payload_preserves_keyed_source_identity() {
    // Source path-to-content association is a real key input (#760), so
    // diagnostics must not discard it as display-only noise.
    assert_eq!(
        normalize_payload("source:/Users/a/clone-a/foo.rs=254dfb8084fc2e3f"),
        "source:/Users/a/clone-a/foo.rs=254dfb8084fc2e3f"
    );
    assert_ne!(
        normalize_payload("source:/Users/a/clone-a/foo.rs=254dfb8084fc2e3f"),
        normalize_payload("source:/Users/b/clone-b/foo.rs=254dfb8084fc2e3f"),
    );
    // Non-source payloads pass through untouched.
    assert_eq!(
        normalize_payload("env_dep:CARGO_MANIFEST_DIR=/p"),
        "env_dep:CARGO_MANIFEST_DIR=/p"
    );
    assert_eq!(normalize_payload("final=abcdef"), "final=abcdef");
}

#[test]
fn normalize_payload_strips_display_only_link_lib_path() {
    // `link_lib_content:static=NAME=HASH (PATH)` is display-only: the
    // hasher consumes ONLY the content hash (cache_key.rs hashes
    // `link_lib_content:` + content_hash). The lib name and absolute
    // `.a` path are context, so two clones with identical content but
    // different build paths must normalize to the same payload — else
    // the key-diff over-reports them as cross-clone divergences (#470).
    // The path is stripped; the lib name + content hash are kept (so the
    // aggregate still buckets under `link_lib_content:static`, and the
    // name↔content association survives — cross-family review caught that
    // reducing to hash-only would break `field_of`).
    assert_eq!(
        normalize_payload(
            "link_lib_content:static=ring_core_0_17_14_=218693c3a2c8618a \
                 (/x/clone-a/target/release/build/ring-abc/out/libring_core_0_17_14_.a)"
        ),
        "link_lib_content:static=ring_core_0_17_14_=218693c3a2c8618a"
    );
    // Same content, different clone path → identical normalized payload
    // (this is the false-divergence the issue is about).
    assert_eq!(
        normalize_payload(
            "link_lib_content:static=ring_core_0_17_14_=218693c3a2c8618a (/clone-a/out/libring.a)"
        ),
        normalize_payload(
            "link_lib_content:static=ring_core_0_17_14_=218693c3a2c8618a (/clone-b/out/libring.a)"
        ),
    );
    // Genuinely different content (quickjs) must STAY divergent.
    assert_ne!(
        normalize_payload(
            "link_lib_content:static=quickjs=834656493be21f86 (/clone-a/out/libquickjs.a)"
        ),
        normalize_payload(
            "link_lib_content:static=quickjs=3dc0f4dbcc36e69f (/clone-b/out/libquickjs.a)"
        ),
    );
    // A path containing '=' or ' (' must not corrupt the kept name/hash.
    assert_eq!(
        normalize_payload("link_lib_content:static=foo=deadbeef (/odd=dir (x)/out/libfoo.a)"),
        "link_lib_content:static=foo=deadbeef"
    );
    // No trailing path (defensive) passes through unchanged.
    assert_eq!(
        normalize_payload("link_lib_content:static=foo=deadbeef"),
        "link_lib_content:static=foo=deadbeef"
    );
    // Regression guard (cross-family review): the normalized payload must
    // still bucket under the `link_lib_content:static` field, not per-hash.
    assert_eq!(
        field_of(&normalize_payload(
            "link_lib_content:static=quickjs=834656493be21f86 (/clone-a/out/libquickjs.a)"
        )),
        "link_lib_content:static"
    );
}

#[test]
fn field_of_buckets_payloads_by_prefix() {
    assert_eq!(
        field_of("env_dep:CARGO_MANIFEST_DIR=/x"),
        "env_dep:CARGO_MANIFEST_DIR"
    );
    assert_eq!(field_of("codegen:opt-level=3"), "codegen:opt-level");
    assert_eq!(field_of("RUSTFLAGS=-C debuginfo=2"), "RUSTFLAGS");
    assert_eq!(field_of("final=abcdef"), "final");
    assert_eq!(field_of("feature:foo"), "feature:foo"); // no '=' → whole payload
}

#[test]
fn clone_ref_path_is_a_sibling_not_a_child_of_work_dir() {
    // The reference must live OUTSIDE work_dir so that the natural
    // `rm -rf <work_dir>` wipe (what someone reaches for to reset
    // the bench) doesn't accidentally torch the network clone.
    assert_eq!(
        source::clone_ref_path(Path::new("./tmp/bench")),
        PathBuf::from("./tmp/bench-clone-ref")
    );
    assert_eq!(
        source::clone_ref_path(Path::new("/scratch/foo")),
        PathBuf::from("/scratch/foo-clone-ref")
    );
    // Sanity: the reference path is not nested inside work_dir.
    let work = Path::new("./tmp/bench");
    let r = source::clone_ref_path(work);
    assert!(
        !r.starts_with(work),
        "clone-ref must not live under work_dir, got {r:?}"
    );
}

#[test]
fn default_work_dir_is_per_scenario_under_tmp_bench() {
    assert_eq!(
        default_work_dir("substrate"),
        PathBuf::from("./tmp/bench/substrate")
    );
    assert_eq!(
        default_work_dir("firefox"),
        PathBuf::from("./tmp/bench/firefox")
    );
    // clone-ref stays a sibling WITHIN ./tmp/bench (not under work_dir), so
    // `rm -rf ./tmp/bench/<scenario>` spares the clone and `rm -rf tmp/bench`
    // wipes every scenario at once.
    let wd = default_work_dir("substrate");
    assert_eq!(
        source::clone_ref_path(&wd),
        PathBuf::from("./tmp/bench/substrate-clone-ref")
    );
    assert!(!source::clone_ref_path(&wd).starts_with(&wd));
}

#[test]
fn work_dir_lock_is_exclusive() {
    let dir = std::env::temp_dir().join(format!("kache-bench-locktest-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    // Keep the first guard BOUND for the duration of the second attempt —
    // if it dropped first, the lock would release and the second succeed
    // (false pass).
    let first = acquire_work_dir_lock(&dir).expect("first lock should acquire");
    let second = acquire_work_dir_lock(&dir);
    assert!(
        second.is_err(),
        "second lock on the same work_dir must be refused"
    );
    drop(first);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn compute_key_divergence_buckets_diffs_by_field() {
    let mut cold: HashMap<String, Vec<String>> = HashMap::new();
    let mut warm: HashMap<String, Vec<String>> = HashMap::new();
    cold.insert(
        "gkrust".into(),
        vec![
            "env_dep:CARGO_MANIFEST_DIR=/clone-a/path".into(),
            "codegen:opt-level=3".into(),
            "final=AAAA".into(),
        ],
    );
    warm.insert(
        "gkrust".into(),
        vec![
            "env_dep:CARGO_MANIFEST_DIR=/clone-b/path".into(),
            "codegen:opt-level=3".into(),
            "final=BBBB".into(),
        ],
    );
    // Stable cross-clone: same key both times → must not appear in diff.
    cold.insert("stable".into(), vec!["final=XXXX".into()]);
    warm.insert("stable".into(), vec!["final=XXXX".into()]);

    let diff = compute_key_divergence(&cold, &warm);
    assert_eq!(diff.diverging_crates, 1, "stable crate must not appear");
    // Two fields diverge for gkrust: env_dep:CARGO_MANIFEST_DIR and final.
    assert_eq!(diff.aggregate_by_field.len(), 2);
    let fields: Vec<&str> = diff
        .aggregate_by_field
        .iter()
        .map(|a| a.field.as_str())
        .collect();
    assert!(fields.contains(&"env_dep:CARGO_MANIFEST_DIR"));
    assert!(fields.contains(&"final"));
}

#[test]
fn compute_key_divergence_drops_tu_matching_artifacts() {
    let mut cold: HashMap<String, Vec<String>> = HashMap::new();
    let mut warm: HashMap<String, Vec<String>> = HashMap::new();

    cold.insert(
        "Unified_cpp_x0.cpp".into(),
        vec![
            "resolved_token=foo.cpp".into(),
            "resolved_token=.deps/foo.o.pp".into(),
            "final=AAAA".into(),
        ],
    );
    warm.insert(
        "Unified_cpp_x0.cpp".into(),
        vec![
            "resolved_token=bar.cpp".into(),
            "resolved_token=.deps/bar.o.pp".into(),
            "final=BBBB".into(),
        ],
    );
    cold.insert(
        "real_cc.cpp".into(),
        vec![
            "resolved_token=<BASE_DIR>/a/foo.h".into(),
            "final=CCCC".into(),
        ],
    );
    warm.insert(
        "real_cc.cpp".into(),
        vec![
            "resolved_token=<BASE_DIR>/b/foo.h".into(),
            "final=DDDD".into(),
        ],
    );

    let diff = compute_key_divergence(&cold, &warm);

    assert_eq!(diff.filtered_tu_artifacts, 1);
    assert_eq!(diff.diverging_crates, 1);
    assert_eq!(diff.by_crate[0].crate_name, "real_cc.cpp");
    // The filtered artifact is excluded from the headline count but kept
    // as an auditable sample (with its divergent tokens) — never silently
    // dropped, so a real leak can't hide behind the filter.
    assert_eq!(diff.filtered_tu_samples.len(), 1);
    assert_eq!(diff.filtered_tu_samples[0].crate_name, "Unified_cpp_x0.cpp");
    let toks: Vec<&str> = diff.filtered_tu_samples[0]
        .only_in_cold
        .iter()
        .chain(diff.filtered_tu_samples[0].only_in_warm.iter())
        .map(String::as_str)
        .collect();
    assert!(
        toks.iter()
            .any(|t| t.contains("foo.cpp") || t.contains("bar.cpp")),
        "filtered sample must retain the divergent tokens: {toks:?}"
    );
}

fn phase_metrics_for_verdict(errors: u64) -> PhaseMetrics {
    PhaseMetrics {
        wall_s: 1,
        wall_ms: 1_000,
        total_crates: 10,
        hits: 6,
        dups: 0,
        misses: 4,
        errors,
        hit_rate_pct: 60.0,
        weighted_hit_rate_pct: Some(60.0),
        time_saved_s: 0,
        top_misses: Vec::new(),
        event_log: EventLogStats {
            total: 10,
            cached: 7,
            passed_through: 3,
            probed: 0,
            errored: 0,
            top_passthrough: Vec::new(),
            passthrough_reasons: Vec::new(),
            hit_bytes: 0,
            miss_bytes: 0,
            store_errors: 0,
            lookup_rejections: 0,
            fallbacks: 0,
        },
        leak_warnings: 0,
        storage: StorageInfo {
            reflinked_bytes: 0,
            hardlinked_bytes: 0,
            copied_bytes: 0,
            restored_bytes: 0,
            zero_copy_pct: 0.0,
            store_reflinked_bytes: 0,
            store_hardlinked_bytes: 0,
            store_copied_bytes: 0,
            store_blobs: 0,
            logical_bytes: 0,
            blob_bytes: 0,
            dedup_saved_bytes: 0,
        },
        phases: PhaseTimes::default(),
        prepare: None,
        invalid_reasons: Vec::new(),
        load: crate::bench_host::PhaseLoad::default(),
    }
}

/// Stability exactly at the floor passes, just under it fails, and an
/// unknown percentage is not checked at all.
#[test]
fn key_stability_check_passes_at_the_floor_and_skips_the_unknown() {
    let spec = ScenarioAssertSpec {
        min_key_stability_pct: Some(80.0),
        ..Default::default()
    };
    let warm = PhaseMetrics::default();
    let stability = |stable_pct| KeyStability {
        stable_pct,
        stable: 0,
        compared: 0,
    };

    let at_floor = Verdict::evaluate(&stability(Some(80.0)), &warm, Some(&spec));
    assert!(at_floor.ok, "{:?}", at_floor.issues);
    assert_eq!(at_floor.checks[0].actual, "80.0");

    let under = Verdict::evaluate(&stability(Some(79.9)), &warm, Some(&spec));
    assert!(!under.ok);
    assert!(under.issues[0].contains("79.9%"), "{:?}", under.issues);

    let unknown = Verdict::evaluate(&stability(None), &warm, Some(&spec));
    assert!(unknown.ok);
    assert!(unknown.checks.is_empty());
}

#[test]
fn verdict_has_no_hidden_assertions_without_configured_checks() {
    let stability = KeyStability {
        stable_pct: Some(0.0),
        stable: 0,
        compared: 4,
    };
    let warm = phase_metrics_for_verdict(99);

    let verdict = Verdict::evaluate(&stability, &warm, None);

    assert!(verdict.ok);
    assert!(verdict.checks.is_empty());
    assert!(verdict.issues.is_empty());
}

#[test]
fn verdict_uses_only_configured_assert_thresholds() {
    let stability = KeyStability {
        stable_pct: Some(0.0),
        stable: 0,
        compared: 4,
    };
    let warm = phase_metrics_for_verdict(1);
    let spec = ScenarioAssertSpec {
        min_key_stability_pct: None,
        max_passthrough_pct: None,
        max_errors: Some(0),
        ..Default::default()
    };

    let verdict = Verdict::evaluate(&stability, &warm, Some(&spec));

    // Only max_errors is configured, so it's the only check produced. It
    // records the wrapped-compile failure count but never degrades the run —
    // those are tolerated build-script probes, not kache cache faults.
    assert!(verdict.ok);
    assert_eq!(verdict.checks.len(), 1);
    assert_eq!(verdict.checks[0].name, "max_errors");
    assert!(verdict.checks[0].passed);
}

#[test]
fn verdict_uses_configured_assert_thresholds() {
    let stability = KeyStability {
        stable_pct: Some(75.0),
        stable: 3,
        compared: 4,
    };
    let warm = phase_metrics_for_verdict(0);
    let spec = ScenarioAssertSpec {
        min_key_stability_pct: Some(80.0),
        max_passthrough_pct: Some(20.0),
        max_errors: Some(0),
        ..Default::default()
    };

    let verdict = Verdict::evaluate(&stability, &warm, Some(&spec));

    assert!(!verdict.ok);
    let failed: Vec<&str> = verdict
        .checks
        .iter()
        .filter(|check| !check.passed)
        .map(|check| check.name)
        .collect();
    assert_eq!(failed, vec!["min_key_stability_pct", "max_passthrough_pct"]);
}

/// The passthrough gate reads "at most `max_passthrough_pct`", so a rate
/// exactly at the limit passes. 2 of 8 is 25% with no rounding, unlike the
/// shared fixture's 3 of 10, whose `f64` rate lands just above 30.
#[test]
fn verdict_passthrough_exactly_at_the_limit_passes() {
    let stability = KeyStability {
        stable_pct: Some(100.0),
        stable: 4,
        compared: 4,
    };
    let mut warm = phase_metrics_for_verdict(0);
    warm.event_log.total = 8;
    warm.event_log.passed_through = 2;
    let spec = ScenarioAssertSpec {
        max_passthrough_pct: Some(25.0),
        ..Default::default()
    };

    let verdict = Verdict::evaluate(&stability, &warm, Some(&spec));

    let check = verdict
        .checks
        .iter()
        .find(|check| check.name == "max_passthrough_pct")
        .expect("passthrough check is configured");
    assert!(check.passed, "25% at a 25% limit, got {}", check.actual);
}

/// The PR perf gate's core safety property: a phase that recompiled the
/// world must be rejected, not reported as a fast build. Zero hits and zero
/// restored bytes are the two ways "this measured nothing" shows up.
#[test]
fn verdict_rejects_a_phase_that_consumed_no_cache() {
    let spec = ScenarioAssertSpec {
        min_hits: Some(1),
        min_restored_bytes: Some(1024),
        ..Default::default()
    };
    let measured_nothing = PhaseMetrics::default();

    // A default `KeyStability` is how the same-tree phase is evaluated:
    // stability is cross-clone only, so it must stay out of the verdict.
    let verdict = Verdict::evaluate(&KeyStability::default(), &measured_nothing, Some(&spec));

    assert!(!verdict.ok, "a zero-hit phase must not pass");
    assert_eq!(
        failed_check_names(&verdict),
        vec!["min_hits", "min_restored_bytes"]
    );
    assert!(
        verdict
            .checks
            .iter()
            .all(|check| check.name != "min_key_stability_pct"),
        "stability must be skipped when nothing was compared"
    );
    // Both failures must be explained. The issue list is what the summary
    // prints and what a reviewer reads; a verdict that fails without saying
    // why is barely more useful than one that passes wrongly.
    assert_eq!(verdict.issues.len(), 2, "{:?}", verdict.issues);
    assert!(
        verdict.issues[0].contains("cache hits"),
        "{:?}",
        verdict.issues
    );
    assert!(
        verdict.issues[1].contains("restored from the store"),
        "{:?}",
        verdict.issues
    );
}

/// A phase that hit the cache and pulled real bytes out of it passes both
/// validity checks — at the exact floor and above it. The floors are
/// inclusive: `min_hits = N` means N hits is enough.
#[test]
fn verdict_accepts_a_phase_that_hit_and_restored() {
    let spec = ScenarioAssertSpec {
        min_hits: Some(412),
        min_restored_bytes: Some(1024),
        ..Default::default()
    };
    // Exactly at both floors — the boundary an off-by-one moves.
    let at_the_floor = PhaseMetrics {
        hits: 412,
        storage: StorageInfo {
            restored_bytes: 1024,
            ..Default::default()
        },
        ..Default::default()
    };
    let comfortably_over = PhaseMetrics {
        hits: 4_120,
        storage: StorageInfo {
            restored_bytes: 64 * 1024 * 1024,
            ..Default::default()
        },
        ..Default::default()
    };

    for measured in [at_the_floor, comfortably_over] {
        let verdict = Verdict::evaluate(&KeyStability::default(), &measured, Some(&spec));

        assert!(verdict.ok, "{:?}", verdict.issues);
        assert_eq!(verdict.checks.len(), 2);
        assert!(verdict.checks.iter().all(|check| check.passed));
        // A passing check must not manufacture an issue. The issue list is
        // the run's "why this is not a measurement" evidence; noise in it
        // makes a degraded run indistinguishable from a healthy one.
        assert!(verdict.issues.is_empty(), "{:?}", verdict.issues);
    }
}

/// One short of either floor fails. Together with the at-the-floor case
/// above this pins both comparisons to `>=` rather than `>`.
#[test]
fn verdict_rejects_a_phase_one_short_of_either_floor() {
    let spec = ScenarioAssertSpec {
        min_hits: Some(412),
        min_restored_bytes: Some(1024),
        ..Default::default()
    };

    let one_hit_short = PhaseMetrics {
        hits: 411,
        storage: StorageInfo {
            restored_bytes: 1024,
            ..Default::default()
        },
        ..Default::default()
    };
    let verdict = Verdict::evaluate(&KeyStability::default(), &one_hit_short, Some(&spec));
    assert!(!verdict.ok);
    assert_eq!(failed_check_names(&verdict), vec!["min_hits"]);
    assert_eq!(verdict.issues.len(), 1, "{:?}", verdict.issues);

    let one_byte_short = PhaseMetrics {
        hits: 412,
        storage: StorageInfo {
            restored_bytes: 1023,
            ..Default::default()
        },
        ..Default::default()
    };
    let verdict = Verdict::evaluate(&KeyStability::default(), &one_byte_short, Some(&spec));
    assert!(!verdict.ok);
    assert_eq!(failed_check_names(&verdict), vec!["min_restored_bytes"]);
    assert_eq!(verdict.issues.len(), 1, "{:?}", verdict.issues);
}

/// Hits without bytes is the subtler failure: kache answered every query,
/// but nothing landed in the build tree, so the wall-clock is not a restore.
#[test]
fn verdict_rejects_hits_that_restored_nothing() {
    let spec = ScenarioAssertSpec {
        min_hits: Some(1),
        min_restored_bytes: Some(1024),
        ..Default::default()
    };
    let hollow = PhaseMetrics {
        hits: 412,
        ..Default::default()
    };

    let verdict = Verdict::evaluate(&KeyStability::default(), &hollow, Some(&spec));

    assert!(!verdict.ok);
    assert_eq!(failed_check_names(&verdict), vec!["min_restored_bytes"]);
    // Exactly one issue: the hits check passed, so it contributes nothing.
    assert_eq!(verdict.issues.len(), 1, "{:?}", verdict.issues);
    assert!(
        verdict.issues[0].contains("restored from the store"),
        "{:?}",
        verdict.issues
    );
}

fn failed_check_names(verdict: &Verdict) -> Vec<&'static str> {
    verdict
        .checks
        .iter()
        .filter(|check| !check.passed)
        .map(|check| check.name)
        .collect()
}

fn verdict_with(ok: bool) -> Verdict {
    Verdict {
        ok,
        issues: if ok {
            Vec::new()
        } else {
            vec!["something was wrong".to_string()]
        },
        checks: Vec::new(),
    }
}

/// The exit-code decision, as a truth table. Both verdicts have to hold,
/// and a scenario that never ran the same-tree phase has nothing to add.
/// This is the last gate between a broken run and a green PR check, so
/// every combination is pinned rather than sampled.
#[test]
fn a_run_is_degraded_when_either_verdict_says_so() {
    let ok = verdict_with(true);
    let bad = verdict_with(false);

    assert!(
        !run_is_degraded(&ok, None),
        "a passing run without the same-tree phase is not degraded"
    );
    assert!(
        !run_is_degraded(&ok, Some(&ok)),
        "both verdicts passing is not degraded"
    );
    assert!(
        run_is_degraded(&ok, Some(&bad)),
        "the same-tree verdict alone must be able to fail the run"
    );
    assert!(
        run_is_degraded(&bad, Some(&ok)),
        "the cross-clone verdict alone must be able to fail the run"
    );
    assert!(run_is_degraded(&bad, None));
    assert!(run_is_degraded(&bad, Some(&bad)));
}

/// The free-space delta is only meaningful for a plain full run. Both
/// opt-outs are independent, so all four combinations are pinned.
/// Pools of 1500 (objdirs 600 + 400, store 500) with 500 hardlinked (cold
/// ingest 200, warm ingest 100, warm restore 200): 1000 predicted.
fn footprint_fixture() -> (StorageInfo, StorageInfo) {
    let cold = StorageInfo {
        store_hardlinked_bytes: 200,
        ..Default::default()
    };
    let warm = StorageInfo {
        store_hardlinked_bytes: 100,
        hardlinked_bytes: 200,
        blob_bytes: 500,
        // A reflink is a second inode, which the footprint counts too.
        reflinked_bytes: 300,
        store_reflinked_bytes: 300,
        ..Default::default()
    };
    (cold, warm)
}

#[test]
fn a_footprint_within_a_fifth_of_the_prediction_does_not_warn() {
    let (cold, warm) = footprint_fixture();
    for footprint in [800, 1000, 1200] {
        assert_eq!(
            footprint_warning(600, 400, &cold, &warm, footprint),
            None,
            "{footprint}"
        );
    }
}

#[test]
fn a_footprint_past_a_fifth_of_the_prediction_warns() {
    let (cold, warm) = footprint_fixture();
    for footprint in [799, 1201] {
        let warning = footprint_warning(600, 400, &cold, &warm, footprint)
            .unwrap_or_else(|| panic!("no warning at {footprint}"));
        assert!(
            warning.contains("storage byte accounting may be off"),
            "{warning}"
        );
    }
}

#[test]
fn nothing_predicted_never_warns() {
    let empty = StorageInfo::default();
    assert_eq!(footprint_warning(0, 0, &empty, &empty, 5), None);
}

#[test]
fn only_a_plain_full_run_measures_its_disk_footprint() {
    assert!(measures_disk_footprint(false, false));
    // --retry restores cold from a snapshot, so the delta misses that pool.
    assert!(!measures_disk_footprint(true, false));
    // --warm-same-tree refills clone-a's objdir twice, so the delta no
    // longer maps onto the three-pool model.
    assert!(!measures_disk_footprint(false, true));
    assert!(!measures_disk_footprint(true, true));
}

/// The warm-vs-cold speedup, taken on milliseconds, including the zero
/// boundary.
#[test]
fn phase_speedup_divides_cold_by_the_phase_and_guards_zero() {
    // A three-times-faster warm build.
    assert_eq!(phase_speedup(300_000, 100_000), 3.0);
    // Not a ratio a multiply or a remainder would produce.
    assert_eq!(phase_speedup(90_000, 60_000), 1.5);
    // Slower than cold is reported as-is; it is the validity gates' job to
    // reject that, not this helper's to hide it.
    assert_eq!(phase_speedup(60_000, 120_000), 0.5);
    // The milliseconds matter: 100 s over 13.9 s is 7.19x. Truncated to
    // whole seconds the same builds would read 100 / 13 = 7.69x.
    assert_eq!(phase_speedup(100_000, 13_900), 7.19);
    // One millisecond is the smallest measurable phase and must still
    // divide rather than fall into the zero guard.
    assert_eq!(phase_speedup(42, 1), 42.0);
    // Zero: dividing would yield infinity, which no report should carry.
    assert_eq!(phase_speedup(300, 0), 0.0);
    assert_eq!(phase_speedup(0, 0), 0.0);
}

/// Resetting the log between phases is what keeps each phase's report to
/// its own build. A reset that silently did nothing would fold the previous
/// phase's events into the next one's counters — and those counters are
/// what the validity gates read.
#[test]
fn reset_event_log_removes_the_log_and_tolerates_its_absence() {
    let dir = tempfile::tempdir().unwrap();
    let event_log = dir.path().join("events.jsonl");
    std::fs::write(&event_log, b"{\"kind\":\"hit\"}\n").unwrap();

    reset_event_log(&event_log).expect("removing an existing log succeeds");
    assert!(
        !event_log.exists(),
        "the event log must be gone after a reset"
    );

    // Called again — and before the first phase, when no log exists yet.
    reset_event_log(&event_log).expect("a missing log is not an error");
    assert!(!event_log.exists());
}

#[cfg(unix)]
#[test]
fn sccache_same_tree_resets_artifacts_and_does_not_seed_cross_checkout() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let work = root.join("work");
    let a = root.join("a");
    let b = root.join("b");
    for path in [&work, &a, &b] {
        std::fs::create_dir_all(path.join("target")).unwrap();
        std::fs::write(path.join("target/stale"), "stale").unwrap();
    }
    let cache = work.join("cache");
    let tool = root.join("sccache");
    kache_fs::testutil::write_executable(
        &tool,
        r#"#!/bin/sh
case "$1" in
  --show-stats)
    hits=$(cat "$SCCACHE_DIR/hits")
    printf '{"stats":{"cache_hits":{"counts":{"Rust":%s}}},"basedirs":["%s"],"cache_location":"%s"}' "$hits" "$SCCACHE_BASEDIRS" "$SCCACHE_DIR"
    ;;
esac
"#,
    );
    let scenario_dir = root.join("bench-fixture");
    std::fs::create_dir_all(&scenario_dir).unwrap();
    let scenario = scenario_dir.join("scenario.toml");
    std::fs::write(
        &scenario,
        r#"
name = "bench-fixture"
tags = ["suite:bench", "backend:sccache"]
build = '''
set -eu
test ! -e target/stale
mkdir -p target
touch target/stale
if [ ! -f "$SCCACHE_DIR/seed" ]; then
    touch "$SCCACHE_DIR/seed"
    echo 0 > "$SCCACHE_DIR/hits"
else
    if [ "$(basename "$PWD")" = a ]; then
        touch "$SCCACHE_DIR/same-tree-only"
    else
        test ! -e "$SCCACHE_DIR/same-tree-only"
    fi
    echo 4 > "$SCCACHE_DIR/hits"
fi
'''
[source]
kind = "clone"
repo = "unused"
ref = "fixed"
objdir = "target"
"#,
    )
    .unwrap();
    let profile = BenchProfile::load(&scenario).unwrap();
    for retry in [false, true] {
        run_sccache_bench(
            &profile,
            &tool,
            &cache,
            &a,
            &b,
            &work,
            retry,
            true,
            Path::new("sh"),
            None,
            "target",
            "test",
            &work.join("archive"),
            Some("fixture 1"),
        )
        .unwrap();
        let result: serde_json::Value = read_json(&work.join("bench-fixture.json")).unwrap();
        assert_eq!(result["cold"]["cache_hits"], 0);
        assert_eq!(result["warm_same_tree"]["cache_hits"], 4);
        assert_eq!(result["warm"]["cache_hits"], 4);
        assert!(work.join("report-warm-same-tree.sccache.json").is_file());
        let otlp = std::fs::read_to_string(work.join("metrics.otlp.json")).unwrap();
        assert!(otlp.contains("warm-same-tree"));
    }
    // A command that does no restoration cannot publish a successful warm phase.
    kache_fs::testutil::write_executable(
        &tool,
        "#!/bin/sh\ncase \"$1\" in --show-stats) printf '{\"stats\":{},\"basedirs\":[\"%s\"]}' \"$SCCACHE_BASEDIRS\";; esac\n",
    );
    let error = measure_sccache_phase(
        &profile,
        &tool,
        &cache,
        &a,
        &work,
        "warm-same-tree",
        Path::new("sh"),
    )
    .unwrap_err();
    assert!(error.to_string().contains("restored nothing"));
}

fn bench_result_fixture() -> BenchResult {
    let phase = |wall_ms: u64, hits: u64| PhaseMetrics {
        wall_s: whole_seconds(wall_ms),
        wall_ms,
        hits,
        ..Default::default()
    };
    BenchResult {
        project: "bench-hk".to_string(),
        git_ref: "797e8a9".to_string(),
        platform: "linux-x86_64".to_string(),
        run_id: "20260901T000000Z-kache-1".to_string(),
        artifact_dir: "tmp/perf-gate/head/runs/x".to_string(),
        cache_tool_version: Some("kache 0.16.1".to_string()),
        host: crate::bench_host::HostInfo::default(),
        cold: phase(300_000, 0),
        warm_same_tree: Some(phase(100_400, 412)),
        warm: phase(120_900, 400),
        speedup: 2.5,
        warm_same_tree_speedup: Some(3.0),
        cache_size_mb: 1024.0,
        cold_objdir_bytes: 1 << 30,
        warm_objdir_bytes: 1 << 30,
        disk_measured_bytes: None,
        disk_footprint_bytes: 1 << 31,
        key_stability: KeyStability::default(),
        warm_leak_samples: Vec::new(),
        verdict: verdict_with(true),
        warm_same_tree_verdict: Some(verdict_with(true)),
        measure_warnings: Vec::new(),
        key_diff_top: None,
        reports: Vec::new(),
    }
}

/// The summary is the only place a human sees the run's numbers, and the
/// perf gate compares exactly the three wall-clocks at the top of it. All
/// three must be reported, each labelled for the phase it came from.
#[test]
fn summary_reports_all_three_phase_wall_clocks() {
    let text = summary_text(&bench_result_fixture());

    assert!(
        text.contains("cold build : 5m 00.0s"),
        "cold wall-clock missing:\n{text}"
    );
    assert!(
        text.contains("same-tree  : 1m 40.4s"),
        "same-tree wall-clock missing:\n{text}"
    );
    assert!(
        text.contains("warm build : 2m 00.9s"),
        "cross-clone wall-clock missing:\n{text}"
    );
    assert!(
        text.contains("3.00x vs cold"),
        "the same-tree speedup is what says the cache helped:\n{text}"
    );
    assert!(
        text.contains("VERDICT: ok"),
        "the verdict must be stated:\n{text}"
    );
    assert!(
        text.contains("VERDICT (same-tree): ok"),
        "the same-tree verdict must be stated separately:\n{text}"
    );
    assert!(
        text.contains("/tmp/run"),
        "the artifact directory must be pointed at:\n{text}"
    );
}

/// A run without the same-tree phase — every nightly scenario — must not
/// grow same-tree lines it has no numbers for.
#[test]
fn summary_omits_the_same_tree_block_when_the_phase_did_not_run() {
    let mut result = bench_result_fixture();
    result.warm_same_tree = None;
    result.warm_same_tree_speedup = None;
    result.warm_same_tree_verdict = None;

    let text = summary_text(&result);

    assert!(text.contains("cold build : 5m 00.0s"), "{text}");
    assert!(text.contains("warm build : 2m 00.9s"), "{text}");
    assert!(!text.contains("same-tree"), "{text}");
}

/// A degraded same-tree phase must say so, and say why. This is the line a
/// reviewer reads when the perf gate refuses to report a delta.
#[test]
fn summary_explains_a_degraded_same_tree_phase() {
    let mut result = bench_result_fixture();
    result.warm_same_tree_verdict = Some(Verdict {
        ok: false,
        issues: vec!["0 cache hits (need >= 100)".to_string()],
        checks: Vec::new(),
    });

    let text = summary_text(&result);

    assert!(text.contains("VERDICT (same-tree): DEGRADED"), "{text}");
    assert!(text.contains("0 cache hits (need >= 100)"), "{text}");
}

/// The cross-clone verdict has three distinct wordings, and the difference
/// between two of them is the whole point: "no blocking assertions
/// configured" means the scenario declared no gate, while "validly
/// exercised kache" means a gate ran and passed. A reader who cannot tell
/// those apart cannot tell an unguarded scenario from a guarded one.
#[test]
fn summary_distinguishes_an_unguarded_run_from_a_guarded_one() {
    let passed_check = AssertionCheck {
        name: "min_hits",
        expected: ">= 100".to_string(),
        actual: "412".to_string(),
        passed: true,
    };

    let mut unguarded = bench_result_fixture();
    unguarded.verdict = Verdict {
        ok: true,
        issues: Vec::new(),
        checks: Vec::new(),
    };
    let text = summary_text(&unguarded);
    assert!(
        text.contains("VERDICT: ok — no blocking assertions configured."),
        "{text}"
    );

    let mut guarded = bench_result_fixture();
    guarded.verdict = Verdict {
        ok: true,
        issues: Vec::new(),
        checks: vec![passed_check],
    };
    let text = summary_text(&guarded);
    assert!(
        text.contains("VERDICT: ok — the run validly exercised kache."),
        "{text}"
    );

    let mut degraded = bench_result_fixture();
    degraded.verdict = Verdict {
        ok: false,
        issues: vec!["cross-clone key stability 3.1%".to_string()],
        checks: Vec::new(),
    };
    let text = summary_text(&degraded);
    assert!(text.contains("VERDICT: DEGRADED RUN"), "{text}");
    assert!(text.contains("cross-clone key stability 3.1%"), "{text}");
}

/// The two optional diagnostic sections appear only when they have content.
/// An empty "costliest misses" or "measure warnings" heading reads as a
/// finding that is not there.
#[test]
fn summary_shows_optional_sections_only_when_they_have_content() {
    let quiet = bench_result_fixture();
    let text = summary_text(&quiet);
    assert!(!text.contains("costliest warm misses"), "{text}");
    assert!(!text.contains("MEASURE WARNINGS"), "{text}");

    let mut noisy = bench_result_fixture();
    noisy.warm.top_misses = vec![MissEntry {
        crate_name: "libgit2-sys".to_string(),
        compile_time_s: 125,
        compile_time_ms: 125_400,
    }];
    noisy.measure_warnings = vec!["warm hit rate 12.0% below threshold 50.0%".to_string()];
    let text = summary_text(&noisy);
    assert!(text.contains("costliest warm misses"), "{text}");
    // A miss's compile time is shown from its milliseconds, as minutes and
    // zero-padded seconds with tenths. 125 s rather than 71 s because
    // 71 - 60 == 71 % 60, which would let a wrong operator print the same.
    assert!(text.contains("2m 05.4s  libgit2-sys"), "{text}");
    assert!(text.contains("MEASURE WARNINGS"), "{text}");
    assert!(text.contains("warm hit rate 12.0%"), "{text}");
}

fn summary_text(result: &BenchResult) -> String {
    let mut out = Vec::new();
    write_summary(&mut out, result, Path::new("/tmp/run")).unwrap();
    String::from_utf8(out).expect("the summary is UTF-8")
}

#[test]
fn pull_bench_result_serializes_with_pull_phase() {
    let r = PullBenchResult {
        project: "bench-firefox-pull".into(),
        git_ref: "aaaa".into(),
        ref_next: "bbbb".into(),
        platform: "linux-x86_64".into(),
        run_id: "run-1".into(),
        artifact_dir: "/tmp/run-1".into(),
        cache_tool_version: Some("kache 0.8.0".into()),
        host: crate::bench_host::HostInfo::default(),
        cold: PhaseMetrics::default(),
        pull: PhaseMetrics::default(),
        cache_size_mb: 1.0,
        cold_objdir_bytes: 10,
        pull_objdir_bytes: 20,
        disk_footprint_bytes: 25,
        verdict: Verdict {
            ok: true,
            issues: vec![],
            checks: vec![],
        },
        measure_warnings: vec![],
        reports: vec!["report-pull.md".into()],
    };
    let j = serde_json::to_value(&r).unwrap();
    assert_eq!(j["ref_next"], "bbbb");
    assert!(j.get("pull").is_some());
    assert!(
        j.get("warm").is_none(),
        "pull result must not carry a warm phase"
    );
}

/// `wall_s` is the truncated whole-second view of `wall_ms`, the same
/// rounding `Duration::as_secs` applied when the engine recorded seconds
/// only, so the nightly JSON and the kartero payload keep their values.
/// The mbx report maps onto the phase metrics field by field; the hit
/// rate is hits over hits plus misses, bypasses are summed by reason,
/// and the avoided compiler time is reduced to whole seconds.
#[test]
fn mbx_phase_metrics_reduce_the_stats_report() {
    let raw = serde_json::json!({
        "version": 4,
        "lookups": 300,
        "hits": 240,
        "misses": 60,
        "unconsulted": 12,
        "compiler_invocations_avoided": 240,
        "estimated_compiler_duration_avoided_ns": 95_500_000_000u64,
        "bypasses": { "incremental": 5, "native-link": 2 },
        "restored_output_files": 700,
        "restored_output_bytes": 400_000_000u64,
        "reflinked_output_bytes": 390_000_000u64,
        "copied_output_bytes": 10_000_000u64,
        "stored_bytes": 123u64
    });
    let m = MbxPhaseMetrics::from_report(&raw, 19_400);
    assert_eq!((m.wall_s, m.wall_ms), (19, 19_400));
    assert_eq!(m.report_version, 4);
    assert_eq!(
        (m.lookups, m.hits, m.misses, m.unconsulted),
        (300, 240, 60, 12)
    );
    assert_eq!(m.hit_rate_pct, 80.0);
    assert_eq!(m.bypassed, 7);
    assert_eq!(m.bypasses["native-link"], 2);
    assert_eq!(m.compiler_invocations_avoided, 240);
    assert_eq!(m.time_saved_s, 95, "nanoseconds round down to seconds");
    assert_eq!(m.restored_output_files, 700);
    assert_eq!(m.restored_output_bytes, 400_000_000);
    assert_eq!(m.reflinked_output_bytes, 390_000_000);
    assert_eq!(m.copied_output_bytes, 10_000_000);
    assert_eq!(m.stored_bytes, 123);

    let empty = MbxPhaseMetrics::from_report(&serde_json::json!({}), 0);
    assert_eq!(
        empty.hit_rate_pct, 0.0,
        "no lookups is not a division by zero"
    );
    assert!(empty.bypasses.is_empty());
}

/// The shim is what `mbx setup` installs: it names the mbx binary from
/// the target file next to it, sets shim mode, and execs. Rewriting it
/// with a new binary path replaces the target.
#[test]
fn mbx_cargo_shim_points_at_the_given_binary() {
    let dir = tempfile::tempdir().unwrap();
    let shim_dir = install_mbx_cargo_shim(dir.path(), Path::new("/opt/first/mbx")).unwrap();
    assert_eq!(shim_dir, dir.path().join("mbx-shim"));
    let shim = std::fs::read_to_string(shim_dir.join("cargo")).unwrap();
    assert!(shim.starts_with("#!/bin/sh\n"));
    assert!(shim.contains("MBX_CARGO_SHIM_MODE=1"));
    assert!(shim.contains("MBX_CARGO_SHIM_PATH=\"$shim_dir/cargo\""));
    assert!(shim.trim_end().ends_with("exec \"$mbx_executable\" \"$@\""));
    assert_eq!(
        std::fs::read_to_string(shim_dir.join("mbx-target")).unwrap(),
        "/opt/first/mbx\n"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(shim_dir.join("cargo"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o111, 0o111, "the shim must be executable");
    }

    install_mbx_cargo_shim(dir.path(), Path::new("/opt/second/mbx")).unwrap();
    assert_eq!(
        std::fs::read_to_string(shim_dir.join("mbx-target")).unwrap(),
        "/opt/second/mbx\n"
    );
}

#[test]
fn mbx_shim_directory_goes_first_on_path() {
    let joined = prepend_to_path(Path::new("/tmp/mbx-shim")).unwrap();
    let first = std::env::split_paths(&joined).next().unwrap();
    assert_eq!(first, Path::new("/tmp/mbx-shim"));
    assert!(
        std::env::split_paths(&joined).count() >= 2 || std::env::var_os("PATH").is_none(),
        "the existing PATH must follow"
    );
}

/// Every backend's label is the name its recipe, scenario tag and OTLP
/// `cache_tool` attribute use; a drifting one silently mislabels a run.
#[test]
fn cache_backend_labels_are_the_names_the_arms_are_selected_by() {
    assert_eq!(CacheBackend::Kache.label(), "kache");
    assert_eq!(CacheBackend::Sccache.label(), "sccache");
    assert_eq!(CacheBackend::Mbx.label(), "mbx");
    assert!(!CacheBackend::Kache.is_external());
    assert!(CacheBackend::Sccache.is_external());
    assert!(CacheBackend::Mbx.is_external());
}

#[test]
fn disk_delta_is_the_drop_in_free_space_or_nothing() {
    assert_eq!(disk_delta(Some(900), Some(300)), Some(600));
    assert_eq!(
        disk_delta(Some(300), Some(900)),
        None,
        "free space that grew is not a measurement of this run"
    );
    assert_eq!(disk_delta(Some(300), Some(300)), Some(0));
    assert_eq!(disk_delta(None, Some(300)), None);
    assert_eq!(disk_delta(Some(300), None), None);
}

#[test]
fn bytes_to_mib_divides_and_rounds() {
    assert_eq!(bytes_to_mib(1024 * 1024), 1.0);
    assert_eq!(bytes_to_mib(3 * 1024 * 1024 / 2), 1.5);
    assert_eq!(bytes_to_mib(0), 0.0);
    // Not a value a multiply or a kilobyte divisor would produce.
    assert_eq!(bytes_to_mib(157_286_400), 150.0);
}

/// `--retry` reuses the previous cold phase, so it must refuse when
/// either artifact is missing and accept when both are present.
#[test]
fn retry_load_mbx_cold_requires_both_artifacts() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    let cache = dir.path().join("cache");
    std::fs::create_dir_all(&work).unwrap();

    let missing_both = retry_load_mbx_cold("bench-hk-mbx", &cache, &work).unwrap_err();
    assert!(
        missing_both.to_string().contains("cache-after-cold"),
        "{missing_both:#}"
    );

    let snapshot = work.join("cache-after-cold");
    std::fs::create_dir_all(&snapshot).unwrap();
    std::fs::write(snapshot.join("blob"), b"cold store").unwrap();
    let missing_json = retry_load_mbx_cold("bench-hk-mbx", &cache, &work).unwrap_err();
    assert!(
        missing_json.to_string().contains("bench-hk-mbx.json"),
        "{missing_json:#}"
    );

    std::fs::write(
        work.join("bench-hk-mbx.json"),
        serde_json::json!({
            "cold": { "wall_s": 94, "wall_ms": 94_200, "lookups": 300, "hits": 0,
                      "misses": 300, "unconsulted": 0, "bypassed": 0,
                      "compiler_invocations_avoided": 0, "time_saved_s": 0,
                      "hit_rate_pct": 0.0, "restored_output_files": 0,
                      "restored_output_bytes": 0, "reflinked_output_bytes": 0,
                      "copied_output_bytes": 0, "stored_bytes": 0 }
        })
        .to_string(),
    )
    .unwrap();
    let cold = retry_load_mbx_cold("bench-hk-mbx", &cache, &work).unwrap();
    assert_eq!((cold.wall_s, cold.misses), (94, 300));
    assert!(
        cache.join("blob").exists(),
        "the snapshot must be restored into the cache dir"
    );
}

#[test]
fn mbx_report_path_is_per_phase() {
    assert_eq!(
        mbx_report_path(Path::new("/w"), "warm"),
        Path::new("/w/report-warm.mbx.json")
    );
    assert_eq!(
        mbx_trace_path(Path::new("/w"), "warm"),
        Path::new("/w/trace-warm.json")
    );
    assert_eq!(
        mbx_sessions_dir(Path::new("/cache")),
        Path::new("/cache/sessions/v1")
    );
}

#[test]
fn list_mbx_session_files_skips_locks_and_missing_dirs() {
    let dir = tempfile::tempdir().unwrap();
    let cache = dir.path().join("cache");
    assert!(list_mbx_session_files(&cache).unwrap().is_empty());

    let sessions = mbx_sessions_dir(&cache);
    std::fs::create_dir_all(&sessions).unwrap();
    std::fs::write(sessions.join("1-1-aaaa.lock"), b"lock").unwrap();
    std::fs::create_dir(sessions.join("2-1-bbbb.jsonl")).unwrap();
    std::fs::write(sessions.join("3-1-cccc.jsonl"), b"session\n").unwrap();
    let files = list_mbx_session_files(&cache).unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(
        files.iter().next().unwrap().file_name().unwrap(),
        "3-1-cccc.jsonl"
    );
}

#[test]
fn list_mbx_session_files_errors_when_sessions_is_not_a_directory() {
    let dir = tempfile::tempdir().unwrap();
    let cache = dir.path().join("cache");
    std::fs::create_dir_all(cache.join("sessions")).unwrap();
    std::fs::write(mbx_sessions_dir(&cache), b"not a directory").unwrap();
    let err = list_mbx_session_files(&cache).unwrap_err().to_string();
    assert!(
        err.contains("sessions"),
        "a non-directory sessions path must fail, got {err}"
    );
}

#[test]
fn mbx_session_order_uses_the_leading_timestamp_not_the_name() {
    // Unpadded names: string order would pick `20.jsonl` over `100.jsonl`.
    assert!(mbx_session_order(Path::new("100.jsonl")) > mbx_session_order(Path::new("20.jsonl")));
    assert!(
        mbx_session_order(Path::new("100-1-bbbb.jsonl"))
            > mbx_session_order(Path::new("99-9-aaaa.jsonl"))
    );
}

#[test]
fn write_mbx_trace_skips_empty_stdout() {
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("trace-warm.json");
    write_mbx_trace(&dest, b"").unwrap();
    assert!(!dest.exists(), "empty stdout must not create a trace file");
    write_mbx_trace(&dest, b"{\"traceEvents\":[]}").unwrap();
    assert_eq!(
        std::fs::read_to_string(&dest).unwrap(),
        "{\"traceEvents\":[]}"
    );
}

#[test]
fn mbx_trace_artifacts_lists_only_files_that_exist() {
    let dir = tempfile::tempdir().unwrap();
    assert!(mbx_trace_artifacts(dir.path(), &["cold", "warm"]).is_empty());
    std::fs::write(dir.path().join("trace-warm.json"), b"{}").unwrap();
    assert_eq!(
        mbx_trace_artifacts(dir.path(), &["cold", "warm"]),
        vec!["trace-warm.json"]
    );
}

#[cfg(unix)]
fn unix_script(dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    kache_fs::testutil::write_executable(&path, body);
    path
}

#[cfg(unix)]
fn fake_mbx_trace(dir: &Path) -> PathBuf {
    unix_script(
        dir,
        "fake-mbx",
        r#"#!/bin/sh
if [ "$1" = cache ] && [ "$2" = trace ]; then
  session=$3
  if [ ! -f "$session" ]; then
    echo "missing session" >&2
    exit 2
  fi
  printf '{"traceEvents":[{"name":"%s"}],"displayTimeUnit":"ms"}\n' "$(basename "$session")"
  exit 0
fi
exit 0
"#,
    )
}

#[cfg(unix)]
#[test]
fn export_mbx_session_trace_writes_stdout_and_rejects_failure() {
    let dir = tempfile::tempdir().unwrap();
    let session = dir.path().join("1-1-aaaa.jsonl");
    std::fs::write(&session, b"{}\n").unwrap();
    let dest = dir.path().join("trace-cold.json");
    export_mbx_session_trace(&fake_mbx_trace(dir.path()), &session, &dest).unwrap();
    let body = std::fs::read_to_string(&dest).unwrap();
    assert!(body.contains("1-1-aaaa.jsonl"), "{body}");

    let empty = dir.path().join("trace-empty.json");
    export_mbx_session_trace(Path::new("/usr/bin/true"), &session, &empty).unwrap();
    assert!(
        !empty.exists(),
        "a successful trace with empty stdout must not write a file"
    );

    let failing = unix_script(dir.path(), "fail-mbx", "#!/bin/sh\necho boom >&2\nexit 7\n");
    let err = export_mbx_session_trace(&failing, &session, &dir.path().join("nope.json"))
        .unwrap_err()
        .to_string();
    assert!(err.contains("exit 7") || err.contains("7"), "{err}");
    assert!(err.contains("boom"), "{err}");
    assert!(!dir.path().join("nope.json").exists());
}

#[cfg(unix)]
#[test]
fn export_new_mbx_session_trace_picks_the_session_this_phase_created() {
    let dir = tempfile::tempdir().unwrap();
    let cache = dir.path().join("cache");
    let sessions = mbx_sessions_dir(&cache);
    std::fs::create_dir_all(&sessions).unwrap();
    let old = sessions.join("900-1-old.jsonl");
    let new = sessions.join("100-1-new.jsonl");
    std::fs::write(&old, b"old\n").unwrap();
    std::fs::write(&new, b"new\n").unwrap();
    let mut before = BTreeSet::new();
    before.insert(old);
    export_new_mbx_session_trace(
        &fake_mbx_trace(dir.path()),
        &cache,
        dir.path(),
        "warm",
        &before,
    )
    .unwrap();
    let body = std::fs::read_to_string(dir.path().join("trace-warm.json")).unwrap();
    assert!(body.contains("100-1-new.jsonl"), "{body}");
    assert!(!body.contains("900-1-old.jsonl"), "{body}");

    // Two new sessions: BTreeSet path order would pick `100-` first;
    // the later timestamp must win.
    std::fs::write(sessions.join("200-1-later.jsonl"), b"later\n").unwrap();
    export_new_mbx_session_trace(
        &fake_mbx_trace(dir.path()),
        &cache,
        dir.path(),
        "warm",
        &before,
    )
    .unwrap();
    let body = std::fs::read_to_string(dir.path().join("trace-warm.json")).unwrap();
    assert!(body.contains("200-1-later.jsonl"), "{body}");
}

#[cfg(unix)]
#[test]
fn export_new_mbx_session_trace_is_a_noop_without_a_new_session() {
    let dir = tempfile::tempdir().unwrap();
    let cache = dir.path().join("cache");
    let sessions = mbx_sessions_dir(&cache);
    std::fs::create_dir_all(&sessions).unwrap();
    let old = sessions.join("1-1-old.jsonl");
    std::fs::write(&old, b"old\n").unwrap();
    let mut before = BTreeSet::new();
    before.insert(old);
    export_new_mbx_session_trace(
        &fake_mbx_trace(dir.path()),
        &cache,
        dir.path(),
        "warm",
        &before,
    )
    .unwrap();
    assert!(!dir.path().join("trace-warm.json").exists());
}

#[cfg(unix)]
#[test]
fn mbx_cold_phase_exports_the_session_as_a_chrome_trace() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let mut profile = prepare_fixture(&work, None);
    profile.build = r#"
            mkdir -p "$MBX_CACHE_DIR/sessions/v1"
            printf '%s\n' '{"hits":1}' > "$MBX_STATS_REPORT"
            printf '%s\n' '{}' > "$MBX_CACHE_DIR/sessions/v1/100-1-cold.jsonl"
        "#
    .into();
    let clone = work.join("clone");
    std::fs::create_dir_all(&clone).unwrap();
    let cache = work.join("cache");
    run_mbx_cold_phase(
        &profile,
        &fake_mbx_trace(&work),
        &cache,
        &clone,
        &work,
        &posix_sh().unwrap(),
    )
    .unwrap();
    let body = std::fs::read_to_string(work.join("trace-cold.json")).unwrap();
    assert!(body.contains("100-1-cold.jsonl"), "{body}");
    assert!(work.join("report-cold.mbx.json").is_file());
}

/// Each threshold fires alone, and the message carries the number.
#[test]
fn external_measure_warnings_fire_per_threshold() {
    let spec = MeasureSpec {
        max_wall_s: Some(20),
        min_hit_rate_pct: Some(90.0),
        min_speedup: Some(3.0),
        known_passthrough: Vec::new(),
    };
    assert!(external_measure_warnings(20, 90.0, 3.0, Some(&spec)).is_empty());
    let wall = external_measure_warnings(21, 90.0, 3.0, Some(&spec));
    assert_eq!(wall.len(), 1);
    assert!(wall[0].contains("21s") && wall[0].contains("20s"));
    let rate = external_measure_warnings(20, 89.9, 3.0, Some(&spec));
    assert_eq!(rate.len(), 1);
    assert!(rate[0].contains("89.9%"));
    let speed = external_measure_warnings(20, 90.0, 2.99, Some(&spec));
    assert_eq!(speed.len(), 1);
    assert!(speed[0].contains("2.99x"));
    assert_eq!(
        external_measure_warnings(99, 0.0, 0.0, Some(&spec)).len(),
        3
    );
    assert!(external_measure_warnings(99, 0.0, 0.0, None).is_empty());
}

/// The mbx OTLP phase carries the report's own time-saved estimate and
/// its lookups as the total; kache-only gauges stay absent.
#[test]
fn otlp_mbx_phase_maps_lookups_and_time_saved() {
    let m = MbxPhaseMetrics::from_report(
        &serde_json::json!({"lookups": 10, "hits": 8, "misses": 2,
                "estimated_compiler_duration_avoided_ns": 3_000_000_000u64}),
        4_000,
    );
    let phase = otlp_mbx_phase("warm", &m, 77);
    assert_eq!(
        (phase.name, phase.wall_ms, phase.objdir_bytes),
        ("warm", 4_000, 77)
    );
    assert_eq!((phase.hits, phase.misses, phase.total), (8, 2, Some(10)));
    assert_eq!(phase.time_saved_s, Some(3));
    assert_eq!(phase.hit_rate_pct, 80.0);
    assert!(
        phase.dups.is_none() && phase.errors.is_none() && phase.weighted_hit_rate_pct.is_none()
    );
}

#[test]
fn whole_seconds_truncates_like_as_secs() {
    assert_eq!(whole_seconds(0), 0);
    assert_eq!(whole_seconds(999), 0);
    assert_eq!(whole_seconds(1_000), 1);
    // The first live gate run's warm build: 14.9s is 14s, not 15s.
    assert_eq!(whole_seconds(14_900), 14);
    assert_eq!(
        whole_seconds(14_900),
        Duration::from_millis(14_900).as_secs()
    );
    assert_eq!(whole_seconds(112_400), 112);
}

#[test]
fn elapsed_ms_reports_milliseconds_and_saturates() {
    assert_eq!(elapsed_ms(Duration::from_millis(0)), 0);
    assert_eq!(elapsed_ms(Duration::from_millis(1_499)), 1_499);
    assert_eq!(elapsed_ms(Duration::from_micros(1_499_999)), 1_499);
    assert_eq!(elapsed_ms(Duration::MAX), u64::MAX);
}

/// One decimal, truncated: the tenths must agree with the whole seconds
/// `wall_s` carries, so 14.96s reads as 14.9s beside a `wall_s` of 14.
#[test]
fn fmt_wall_clock_prints_minutes_seconds_and_truncated_tenths() {
    assert_eq!(fmt_wall_clock(0), "0m 00.0s");
    assert_eq!(fmt_wall_clock(14_600), "0m 14.6s");
    assert_eq!(fmt_wall_clock(14_960), "0m 14.9s");
    assert_eq!(fmt_wall_clock(100_400), "1m 40.4s");
    assert_eq!(fmt_wall_clock(300_000), "5m 00.0s");
    assert_eq!(fmt_wall_clock(3_661_050), "61m 01.0s");
}

/// The wall clock enters the metrics once, in milliseconds; the whole
/// seconds are derived, never recorded separately.
#[test]
fn from_report_records_milliseconds_and_derives_whole_seconds() {
    let raw = serde_json::json!({
        "schema_version": 1,
        "summary": {
            "hit_rate_pct": 100.0,
            "total_crates": 783,
            "local_hits": 783,
            "prefetch_hits": 0,
            "remote_hits": 0,
            "dups": 0,
            "misses": 0,
            "errors": 0,
            "weighted_hit_rate_pct": 100.0,
            "time_saved_ms": 95_500
        },
        "top_misses": []
    });
    let report: report::KacheReport =
        serde_json::from_value(raw.clone()).expect("a minimal report parses");

    let metrics = PhaseMetrics::from_report(&report, &raw, 14_600, EventLogStats::default(), 0);

    assert_eq!(metrics.wall_ms, 14_600);
    assert_eq!(metrics.wall_s, 14);
    assert_eq!(metrics.hits, 783);
    assert_eq!(metrics.time_saved_s, 95);
}

#[test]
fn sccache_metrics_record_milliseconds_and_derive_whole_seconds() {
    let raw = serde_json::json!({
        "stats": {
            "compile_requests": 10,
            "cache_hits": { "counts": { "Rust": 8 } },
            "cache_misses": { "counts": { "Rust": 2 } }
        }
    });

    let metrics = SccachePhaseMetrics::from_raw(&raw, 95_400);

    assert_eq!(metrics.wall_ms, 95_400);
    assert_eq!(metrics.wall_s, 95);
    assert_eq!(metrics.cache_hits, 8);
    assert_eq!(metrics.hit_rate_pct, 80.0);
}

/// The Firefox scenario always reports a few cache errors, because
/// configure compiles programs that are meant to fail. Those must not fail
/// the phase, while a store that cannot be read or written still does.
#[test]
fn sccache_cache_errors_are_budgeted_but_store_errors_are_not() {
    let metrics = |errors: u64, read: u64, write: u64| {
        SccachePhaseMetrics::from_raw(
            &serde_json::json!({
                "stats": {
                    "requests_executed": 5102,
                    "cache_hits": { "counts": { "Rust": 4272 } },
                    "cache_errors": { "counts": { "c [clang]": errors } },
                    "cache_read_errors": read,
                    "cache_write_errors": write,
                }
            }),
            1_000,
        )
    };

    // What the nightly actually reported: 6 configure probes out of 5102.
    let healthy = metrics(6, 0, 0);
    assert_eq!(healthy.cache_errors, 6);
    assert_eq!(sccache_cache_error_complaint(&healthy), None);

    // 1% of 5102 is 51.02, so 51 is the last value inside the budget.
    assert_eq!(sccache_cache_error_complaint(&metrics(51, 0, 0)), None);
    let over = sccache_cache_error_complaint(&metrics(52, 0, 0)).unwrap();
    assert!(
        over.contains("52 cache errors over 5102 requests"),
        "{over}"
    );
    assert!(over.contains("1% budget"), "{over}");

    // Storage failures have no budget, and each is named on its own.
    let read = sccache_cache_error_complaint(&metrics(0, 1, 0)).unwrap();
    assert!(read.contains("1 cache read errors"), "{read}");
    let write = sccache_cache_error_complaint(&metrics(0, 0, 1)).unwrap();
    assert!(write.contains("1 cache write errors"), "{write}");
}

/// The result JSON is the perf gate's input: both wall clocks must be
/// there, under these names, for every phase the gate reads.
#[test]
fn result_json_carries_both_wall_clocks_for_every_phase() {
    let j = serde_json::to_value(bench_result_fixture()).unwrap();

    assert_eq!(j["cold"]["wall_ms"], 300_000);
    assert_eq!(j["cold"]["wall_s"], 300);
    assert_eq!(j["warm_same_tree"]["wall_ms"], 100_400);
    assert_eq!(j["warm_same_tree"]["wall_s"], 100);
    assert_eq!(j["warm"]["wall_ms"], 120_900);
    assert_eq!(j["warm"]["wall_s"], 120);
}

/// `--retry` reuses cold from the previous run's JSON. A snapshot written
/// before the engine timed milliseconds has only `wall_s`; the milliseconds
/// are backfilled from it rather than read as zero, and a snapshot that has
/// them keeps its own value.
#[test]
fn a_saved_phase_without_wall_ms_is_backfilled_from_its_seconds() {
    let mut saved = serde_json::to_value(PhaseMetrics {
        wall_s: 14,
        wall_ms: 14_600,
        hits: 783,
        ..Default::default()
    })
    .unwrap();

    let current: PhaseMetrics = load_saved_phase(saved.clone()).unwrap();
    assert_eq!(current.wall_ms, 14_600, "a recorded value is kept as is");
    assert_eq!(current.wall_s, 14);

    saved.as_object_mut().unwrap().remove("wall_ms");
    let older: PhaseMetrics = load_saved_phase(saved).unwrap();
    assert_eq!(older.wall_ms, 14_000, "backfilled from wall_s, as a floor");
    assert_eq!(older.wall_s, 14);
    assert_eq!(older.hits, 783);
}

/// `--retry` restores cold from the previous run: the cache snapshot goes
/// back into place and cold's metrics come from the result JSON. The JSON
/// that matters is one an engine timing whole seconds wrote, since that
/// is what the first retry after the millisecond wall clock finds on disk.
#[test]
fn retry_load_cold_restores_the_snapshot_and_an_older_results_wall_clock() {
    let dir = tempfile::tempdir().unwrap();
    let work_dir = dir.path().join("work");
    let cache_dir = dir.path().join("cache");
    let kache = dir.path().join("no-such-kache");

    let missing = retry_load_cold("pr-cargo", &kache, &cache_dir, &work_dir)
        .expect_err("nothing to retry from")
        .to_string();
    assert!(
        missing.contains("--retry: required artifact missing"),
        "{missing}"
    );

    let snapshot = work_dir.join("cache-after-cold");
    std::fs::create_dir_all(&snapshot).unwrap();
    std::fs::write(snapshot.join("index.db"), b"cold store").unwrap();
    std::fs::write(
        work_dir.join("report-cold.json"),
        r#"{"summary": {"total_crates": 783}}"#,
    )
    .unwrap();
    // Cold as a whole-second engine wrote it: `wall_s` only.
    let mut previous_cold = serde_json::to_value(PhaseMetrics {
        wall_s: 112,
        wall_ms: 0,
        total_crates: 783,
        ..Default::default()
    })
    .unwrap();
    previous_cold.as_object_mut().unwrap().remove("wall_ms");
    std::fs::write(
        work_dir.join("pr-cargo.json"),
        serde_json::json!({ "cold": previous_cold }).to_string(),
    )
    .unwrap();

    let (cold, cold_raw) = retry_load_cold("pr-cargo", &kache, &cache_dir, &work_dir)
        .expect("a complete set of artifacts loads");

    assert_eq!(cold.wall_s, 112);
    assert_eq!(
        cold.wall_ms, 112_000,
        "backfilled from the seconds the older engine recorded"
    );
    assert_eq!(cold.total_crates, 783);
    assert_eq!(cold_raw["summary"]["total_crates"], 783);
    assert_eq!(
        std::fs::read(cache_dir.join("index.db")).unwrap(),
        b"cold store",
        "the cold-state cache is restored into the cache dir"
    );
}

/// `build` is what the gate times. It must report the build command's own
/// wall clock in milliseconds, wipe the objdir before the timer starts,
/// and leave the logs the failure path points at.
#[cfg(unix)]
#[test]
fn build_times_the_build_command_in_milliseconds() {
    let dir = tempfile::tempdir().unwrap();
    let profile_path = dir.path().join("sleeper.toml");
    std::fs::write(
        &profile_path,
        r#"
name = "sleeper"
repo = "https://example.com/sleeper.git"
ref = "v1"
objdir = "target"
build = "sleep 0.2"
"#,
    )
    .unwrap();
    let profile = BenchProfile::load(&profile_path).unwrap();
    let clone = dir.path().join("clone");
    let stale = clone.join("target").join("stale.o");
    std::fs::create_dir_all(stale.parent().unwrap()).unwrap();
    std::fs::write(&stale, b"left by a previous phase").unwrap();
    let work_dir = dir.path().join("work");
    std::fs::create_dir_all(&work_dir).unwrap();
    let sh = posix_sh().unwrap();

    let run = build(
        &profile,
        &clone,
        "cold",
        &dir.path().join("cache"),
        &dir.path().join("kache.toml"),
        &dir.path().join("kache"),
        &work_dir,
        CacheBackend::Kache,
        false,
        &sh,
    )
    .unwrap();
    let wall_ms = run.wall_ms;

    assert!(
        (200..60_000).contains(&wall_ms),
        "a 200ms sleep must time as at least 200ms, got {wall_ms}ms"
    );
    assert!(!stale.exists(), "the objdir is wiped before the build");
    assert!(work_dir.join("build-cold.log").exists());
    assert!(work_dir.join("wrapper-cold.log").exists());
    // Both supported unix hosts expose load averages; the build samples
    // them on each side of the timer.
    assert!(
        run.load.loadavg_start.is_some() && run.load.loadavg_end.is_some(),
        "{:?}",
        run.load
    );
}

fn prepare_fixture(dir: &Path, prepare: Option<&str>) -> BenchProfile {
    let path = dir.join("prep.toml");
    let prepare = prepare
        .map(|c| format!("prepare = {}\n", toml::Value::String(c.to_string())))
        .unwrap_or_default();
    std::fs::write(
        &path,
        format!(
            r#"
name = "prep"
repo = "https://example.com/prep.git"
ref = "v1"
objdir = "target"
build = "true"
{prepare}
[env]
PREP_MARKER = "{{kache}}"
"#
        ),
    )
    .unwrap();
    BenchProfile::load(&path).unwrap()
}

/// `prepare` runs in the clone with the scenario env, without the cache
/// wrapper (whatever the ambient environment sets), and reports its own
/// time so it never has to hide inside a phase's wall clock.
#[cfg(unix)]
#[test]
fn prepare_runs_in_the_clone_with_the_scenario_env_and_no_wrapper() {
    let dir = tempfile::tempdir().unwrap();
    let command = r#"sleep 0.2; echo "$PREP_MARKER ${RUSTC_WRAPPER:-unset} ${RUSTC_WORKSPACE_WRAPPER:-unset}" > prepared.txt"#;
    let profile = prepare_fixture(dir.path(), Some(command));
    let clone = dir.path().join("clone");
    let work_dir = dir.path().join("work");
    std::fs::create_dir_all(&clone).unwrap();
    std::fs::create_dir_all(&work_dir).unwrap();

    let got = prepare_clone(
        &profile,
        &clone,
        "cold",
        Path::new("/k"),
        &work_dir,
        &posix_sh().unwrap(),
    )
    .unwrap()
    .expect("a declared prepare runs");

    assert_eq!(got.command, command);
    assert!(
        (200..60_000).contains(&got.wall_ms),
        "a 200ms sleep must time as at least 200ms, got {}ms",
        got.wall_ms
    );
    assert_eq!(
        std::fs::read_to_string(clone.join("prepared.txt")).unwrap(),
        "/k unset unset\n"
    );
    assert!(work_dir.join("prepare-cold.log").exists());
}

#[test]
fn prepare_is_a_no_op_when_the_scenario_declares_none() {
    let dir = tempfile::tempdir().unwrap();
    let profile = prepare_fixture(dir.path(), None);
    let got = prepare_clone(
        &profile,
        dir.path(),
        "cold",
        Path::new("/k"),
        dir.path(),
        &posix_sh().unwrap(),
    )
    .unwrap();
    assert_eq!(got, None);
    assert!(!dir.path().join("prepare-cold.log").exists());
}

#[cfg(unix)]
#[test]
fn mbx_pull_reports_both_revisions_and_rejects_empty_restores() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let repo = root.join("repo");
    std::fs::create_dir(&repo).unwrap();
    run(Command::new("git").arg("init").arg("-q").arg(&repo)).unwrap();
    let mut refs = Vec::new();
    for revision in ["parent", "child"] {
        std::fs::write(repo.join("revision"), revision).unwrap();
        run(Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args(["add", "revision"]))
        .unwrap();
        run(Command::new("git").arg("-C").arg(&repo).args([
            "-c",
            "user.name=Bench Test",
            "-c",
            "user.email=bench@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "-c",
            "core.hooksPath=/dev/null",
            "commit",
            "-qm",
            revision,
        ]))
        .unwrap();
        let output = Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args(["rev-parse", "HEAD"])
            .output()
            .unwrap();
        assert!(output.status.success());
        refs.push(String::from_utf8(output.stdout).unwrap().trim().to_owned());
    }
    let scenarios = root.join("scenarios");
    let scenario = scenarios.join("bench-fixture");
    std::fs::create_dir_all(&scenario).unwrap();
    for hits in [0, 1] {
        std::fs::write(
            scenario.join("scenario.toml"),
            format!(
                r#"
name = "bench-fixture"
tags = ["suite:bench", "backend:mbx"]
build = '''
test ! -e "$CARGO_TARGET_DIR/built" || exit 9
mkdir -p "$CARGO_TARGET_DIR"
cp revision "$CARGO_TARGET_DIR/built"
cat revision
printf '{{"hits": {hits}}}' > "$MBX_STATS_REPORT"
'''
[source]
kind = "clone"
repo = "{}"
ref = "{}"
ref_next = "{}"
objdir = "target"
"#,
                repo.display(),
                refs[0],
                refs[1]
            ),
        )
        .unwrap();
        let work = root.join(format!("run-{hits}"));
        let result = run_bench(BenchRunConfig {
            kache: "/usr/bin/true".into(),
            sccache: "/usr/bin/true".into(),
            mbx: "/usr/bin/true".into(),
            cache_backend: CacheBackend::Mbx,
            scenarios: scenarios.clone(),
            select: vec!["suite:bench".into()],
            git_ref: None,
            work_dir: Some(work.clone()),
            skip_clone: false,
            force_setup: false,
            retry: false,
            trace_keys: false,
            warm_same_tree: false,
        });
        if hits == 0 {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("mbx pull restored nothing")
            );
        } else {
            result.unwrap();
        }
        assert!(
            std::fs::read_to_string(work.join("build-cold.log"))
                .unwrap()
                .contains("parent")
        );
        assert!(
            std::fs::read_to_string(work.join("build-pull.log"))
                .unwrap()
                .contains("child")
        );
        let report: serde_json::Value = read_json(&work.join("bench-fixture.json")).unwrap();
        assert_eq!(report["verdict"]["ok"], hits == 1);
        assert_eq!(report["pull"]["hits"], hits);
        let otlp: serde_json::Value = read_json(&work.join("metrics.otlp.json")).unwrap();
        let metrics = otlp["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
            .as_array()
            .unwrap();
        let verdict = metrics
            .iter()
            .find(|m| m["name"] == "kache.bench.verdict.ok")
            .unwrap();
        assert_eq!(verdict["gauge"]["dataPoints"][0]["asInt"], hits.to_string());
        let archive = Path::new(report["artifact_dir"].as_str().unwrap());
        assert!(archive.join("report-pull.mbx.json").is_file());
    }
}

#[cfg(unix)]
#[test]
fn mbx_same_tree_build_owns_and_wipes_the_real_target() {
    let temp = tempfile::tempdir().unwrap();
    let work = temp.path().canonicalize().unwrap();
    let mut profile = prepare_fixture(&work, None);
    profile.build = r#"test -n "$CARGO_TARGET_DIR" || exit 9
            test ! -e "$CARGO_TARGET_DIR/old" || exit 8
            mkdir -p "$CARGO_TARGET_DIR"
            touch "$CARGO_TARGET_DIR/old"
            printf '%s' "$CARGO_TARGET_DIR" > used-target"#
        .into();
    for phase in ["cold", "warm-same-tree"] {
        build(
            &profile,
            &work,
            phase,
            &work.join("cache"),
            &work.join("config"),
            Path::new("/unused/mbx"),
            &work,
            CacheBackend::Mbx,
            false,
            &posix_sh().unwrap(),
        )
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(work.join("used-target")).unwrap(),
            work.join(&profile.objdir).display().to_string()
        );
    }
}

/// The `warm` phase rebuilds in a second, independently checked-out
/// worktree, so mbx is told to share compilations across checkouts. The
/// setting changes mbx's action keys, so the cold phase that fills the
/// store, and the same-tree phase between them, must set it too.
#[cfg(unix)]
#[test]
fn mbx_every_phase_enables_share_workspace_root_env() {
    let temp = tempfile::tempdir().unwrap();
    let work = temp.path().canonicalize().unwrap();
    let mut profile = prepare_fixture(&work, None);
    profile.build =
        r#"printf '%s' "${MBX_SHARE_WORKSPACE_ROOT:-unset}" > share-workspace-root"#.into();
    for phase in ["cold", "warm-same-tree", "warm"] {
        build(
            &profile,
            &work,
            phase,
            &work.join("cache"),
            &work.join("config"),
            Path::new("/unused/mbx"),
            &work,
            CacheBackend::Mbx,
            false,
            &posix_sh().unwrap(),
        )
        .unwrap();
        let recorded = std::fs::read_to_string(work.join("share-workspace-root")).unwrap();
        assert_eq!(recorded, "1", "phase {phase}");
    }
}

#[test]
fn a_failed_prepare_fails_the_run_and_names_its_log() {
    let dir = tempfile::tempdir().unwrap();
    let profile = prepare_fixture(dir.path(), Some("exit 3"));
    let err = prepare_clone(
        &profile,
        dir.path(),
        "warm",
        Path::new("/k"),
        dir.path(),
        &posix_sh().unwrap(),
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("[warm] prepare failed"), "{err}");
    assert!(err.contains("prepare-warm.log"), "{err}");
}

#[test]
fn prepare_logs_are_archived_with_the_run() {
    assert!(is_run_artifact("prepare-cold.log"));
    assert!(is_run_artifact("prepare-warm.log"));
    assert!(is_run_artifact("key-diff.json"));
    assert!(is_run_artifact("trace-cold.json"));
    assert!(is_run_artifact("trace-warm.json"));
    assert!(!is_run_artifact("prepared.txt"));
}

/// A phase that ran no prepare step serializes exactly as before, and a
/// saved phase from before the field existed still loads.
#[test]
fn phase_prepare_is_omitted_when_absent_and_recorded_when_present() {
    let plain = serde_json::to_value(PhaseMetrics::default()).unwrap();
    assert!(plain.get("prepare").is_none(), "{plain}");
    let loaded: PhaseMetrics = serde_json::from_value(plain).unwrap();
    assert_eq!(loaded.prepare, None);

    let prepared = PhaseMetrics {
        prepare: Some(PrepareMetrics {
            command: "cargo fetch --locked".to_string(),
            wall_ms: 41_200,
        }),
        ..Default::default()
    };
    let j = serde_json::to_value(&prepared).unwrap();
    assert_eq!(j["prepare"]["command"], "cargo fetch --locked");
    assert_eq!(j["prepare"]["wall_ms"], 41_200);
}

fn reason(reason: &str, count: u64, elapsed_ms: u64) -> ReasonCount {
    ReasonCount {
        action: "reject".into(),
        reason: reason.into(),
        count,
        elapsed_ms,
    }
}

#[test]
fn a_reason_label_drops_paths_and_keeps_flags() {
    let cases = [
        (
            "uncacheable|cc include candidate /home/runner/obj/dist/pprio.h is unreadable",
            "uncacheable|cc include candidate <path> is unreadable",
        ),
        (
            "unsupported|cc unsupported flag(s): -I/usr/include -mavx",
            "unsupported|cc unsupported flag(s): -I<path> -mavx",
        ),
        ("x --sysroot=/opt/sdk/usr y", "x --sysroot=<path> y"),
        ("x path=/a/b y", "x path=<path> y"),
        ("x a,/a/b y", "x a,<path> y"),
        ("x '/a/b' \"/c/d\" y", "x '<path> \"<path> y"),
        ("x (./obj/a.o) y", "x (<path> y"),
        ("x ../src/a.c ~/b.c y", "x <path> <path> y"),
        ("x .\\obj\\a.o ..\\b.o y", "x <path> <path> y"),
        ("x C:\\Users\\me\\a.h C:/b/c y", "x <path> <path> y"),
        ("x \\\\?\\C:\\a y", "x <path> y"),
        // An MSVC option, a list of extensions and a relative name are not
        // paths worth a placeholder.
        (
            "x /DEF, (.lib/.a/.obj) obj/a.o y",
            "x /DEF, (.lib/.a/.obj) obj/a.o y",
        ),
        (
            "unsupported|rustc build-script probe — not yet",
            "unsupported|rustc build-script probe — not yet",
        ),
    ];
    for (reason, label) in cases {
        assert_eq!(reason_label(reason), label, "{reason}");
    }
}

#[test]
fn a_long_reason_label_is_cut_at_the_limit() {
    let label = reason_label(&"a".repeat(REASON_LABEL_MAX_CHARS + 5));
    assert_eq!(label.chars().count(), REASON_LABEL_MAX_CHARS + 1);
    assert!(label.ends_with('…'));
    let exact = "b".repeat(REASON_LABEL_MAX_CHARS);
    assert_eq!(reason_label(&exact), exact);
}

#[test]
fn passthrough_reasons_merge_by_label_and_rank_by_time() {
    let reasons = [
        reason("unsupported|cc -E to stdout", 161, 3_000),
        reason("uncacheable|cc include /a/b/x.h unreadable", 25, 90_000),
        reason("uncacheable|cc include /a/b/y.h unreadable", 1, 18_000),
        reason("unsupported|cc link mode", 188, 40_000),
    ];
    assert_eq!(
        passthrough_reason_series(&reasons, 2),
        vec![
            (
                "uncacheable|cc include <path> unreadable".to_string(),
                26,
                108_000
            ),
            ("unsupported|cc link mode".to_string(), 188, 40_000),
        ]
    );
    // Equal time: the one that passed through more compiles comes first.
    let tied = [reason("a|one", 1, 500), reason("b|five", 5, 500)];
    assert_eq!(passthrough_reason_series(&tied, 1)[0].0, "b|five");
}

#[test]
fn only_real_compiles_outside_the_known_list_are_new() {
    let series = vec![
        (
            "not-a-compile|query / probe (--print, -vV)".to_string(),
            264,
            8_000,
        ),
        (
            "unsupported|rustc build-script probe — not yet".to_string(),
            12,
            900,
        ),
        (
            "unsupported|cc unsupported flag(s): -funroll-loops — not yet".to_string(),
            1,
            4_000,
        ),
    ];
    let known = vec!["unsupported|rustc build-script probe".to_string()];
    let new: Vec<&str> = unknown_passthrough_reasons(&series, &known)
        .into_iter()
        .map(|(label, _, _)| label.as_str())
        .collect();
    assert_eq!(
        new,
        vec!["unsupported|cc unsupported flag(s): -funroll-loops — not yet"]
    );
}

#[test]
fn a_passthrough_reason_outside_the_known_list_warns() {
    let warm = PhaseMetrics {
        event_log: EventLogStats {
            passthrough_reasons: vec![
                reason("unsupported|rustc build-script probe — not yet", 12, 900),
                reason(
                    "unsupported|cc unsupported flag(s): -funroll-loops — not yet",
                    1,
                    4_300,
                ),
            ],
            ..Default::default()
        },
        ..Default::default()
    };
    let known = MeasureSpec {
        known_passthrough: vec!["unsupported|rustc build-script probe".into()],
        ..Default::default()
    };
    assert_eq!(
            bench_measure_warnings(&warm, 0.0, Some(&known)),
            vec![
                "new passthrough reason (1 compiles, 4.3s): unsupported|cc unsupported flag(s): -funroll-loops — not yet"
                    .to_string()
            ]
        );
    // No list, no check.
    assert!(bench_measure_warnings(&warm, 0.0, Some(&MeasureSpec::default())).is_empty());
}

#[test]
fn the_event_log_sums_count_and_time_per_reason() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("events.jsonl");
    let events = [
        r#"{"result":"passthrough","passthrough_reason":"unsupported|cc link mode","elapsed_ms":1200}"#,
        r#"{"result":"passthrough","passthrough_reason":"unsupported|cc link mode","elapsed_ms":800}"#,
        r#"{"result":"passthrough","passthrough_reason":"not-a-compile|query / probe","elapsed_ms":30}"#,
        r#"{"result":"local_hit","size":10}"#,
    ];
    std::fs::write(&log, events.join("\n")).unwrap();
    let stats = read_event_log(&log);
    assert_eq!(stats.passthrough_reasons.len(), 2);
    assert_eq!(stats.top_passthrough.len(), 2);
    let first = &stats.passthrough_reasons[0];
    assert_eq!(first.reason, "unsupported|cc link mode");
    assert_eq!((first.count, first.elapsed_ms), (2, 2_000));
    assert_eq!(stats.passthrough_reasons[1].elapsed_ms, 30);
}

#[test]
fn the_payload_carries_the_costliest_passthrough_reasons() {
    let reasons: Vec<ReasonCount> = (0..20u64)
        .map(|i| reason(&format!("unsupported|flag -f{i}"), 1, i * 100))
        .collect();
    let metrics = PhaseMetrics {
        event_log: EventLogStats {
            passthrough_reasons: reasons,
            ..Default::default()
        },
        ..Default::default()
    };
    let phase = otlp_phase("warm", &metrics, 0);
    assert_eq!(phase.passthrough_reasons.len(), PASSTHROUGH_REASONS_EMITTED);
    assert_eq!(
        phase.passthrough_reasons[0],
        ("unsupported|flag -f19".to_string(), 1, 1_900)
    );
}

/// The two refusal categories must reach the payload as two numbers, from
/// the two counters that mean different things: `probed` is a query that
/// was never a compilation, `passed_through` a real compile kache does not
/// model yet. Collapsing them into one loses the only part anyone can act
/// on, and does so without any test on the emitter noticing -- the emitter
/// is handed whatever this function built.
#[test]
fn the_refusal_categories_come_from_their_own_counters() {
    let metrics = PhaseMetrics {
        event_log: EventLogStats {
            probed: 272,
            passed_through: 12,
            ..Default::default()
        },
        ..Default::default()
    };
    let phase = otlp_phase("warm", &metrics, 0);
    assert_eq!(
        phase.passthrough,
        vec![("not-a-compile", 272), ("unsupported", 12)],
        "the categories must not be summed or swapped"
    );
}

/// The same-tree warm is a phase of the same gauges, present only when it
/// ran, between cold and the cross-clone warm, and measured on the cold
/// clone's objdir.
#[test]
fn otlp_phases_carry_the_same_tree_warm_only_when_it_ran() {
    let phase = |wall_ms| PhaseMetrics {
        wall_ms,
        ..Default::default()
    };
    let (cold, same_tree, warm) = (phase(100_400), phase(20_000), phase(30_900));

    let without = otlp_phases(&cold, None, &warm, 7, 9);
    assert_eq!(
        without
            .iter()
            .map(|phase| (phase.name, phase.wall_ms, phase.objdir_bytes))
            .collect::<Vec<_>>(),
        vec![("cold", 100_400, 7), ("warm", 30_900, 9)]
    );

    let with = otlp_phases(&cold, Some(&same_tree), &warm, 7, 9);
    assert_eq!(
        with.iter()
            .map(|phase| (phase.name, phase.wall_ms, phase.objdir_bytes))
            .collect::<Vec<_>>(),
        vec![
            ("cold", 100_400, 7),
            ("warm-same-tree", 20_000, 7),
            ("warm", 30_900, 9)
        ]
    );
}

/// An unknown weighted rate stays unknown on its way to the payload, and a
/// sub-second miss keeps its milliseconds instead of truncating to zero.
#[test]
fn from_report_keeps_unknown_rates_and_sub_second_misses() {
    let raw = serde_json::json!({
        "schema_version": 1,
        "summary": {
            "hit_rate_pct": 50.0,
            "total_crates": 2,
            "local_hits": 1,
            "prefetch_hits": 0,
            "remote_hits": 0,
            "dups": 0,
            "misses": 1,
            "errors": 0,
            "weighted_hit_rate_pct": null,
            "time_saved_ms": 0
        },
        "top_misses": [
            { "crate_name": "tiny", "compile_time_ms": 400 },
            { "crate_name": "big", "compile_time_ms": 97_250 }
        ]
    });
    let report: report::KacheReport =
        serde_json::from_value(raw.clone()).expect("a minimal report parses");

    let metrics = PhaseMetrics::from_report(&report, &raw, 1, EventLogStats::default(), 0);

    assert_eq!(metrics.weighted_hit_rate_pct, None);
    assert_eq!(
        metrics
            .top_misses
            .iter()
            .map(|m| (m.crate_name.as_str(), m.compile_time_s, m.compile_time_ms))
            .collect::<Vec<_>>(),
        vec![("tiny", 0, 400), ("big", 97, 97_250)]
    );
    let otlp = otlp_phase("warm", &metrics, 0);
    assert_eq!(otlp.weighted_hit_rate_pct, None);
    assert_eq!(
        otlp.top_misses,
        vec![("tiny".to_string(), 0.4), ("big".to_string(), 97.25)]
    );
}

#[test]
fn a_known_weighted_rate_is_rounded_and_carried() {
    let raw = serde_json::json!({
        "schema_version": 1,
        "summary": {
            "hit_rate_pct": 50.0, "total_crates": 2, "local_hits": 1,
            "prefetch_hits": 0, "remote_hits": 0, "misses": 1,
            "weighted_hit_rate_pct": 97.46
        }
    });
    let report: report::KacheReport = serde_json::from_value(raw.clone()).unwrap();
    let metrics = PhaseMetrics::from_report(&report, &raw, 1, EventLogStats::default(), 0);
    assert_eq!(metrics.weighted_hit_rate_pct, Some(97.5));
    assert_eq!(
        otlp_phase("warm", &metrics, 0).weighted_hit_rate_pct,
        Some(97.5)
    );
}

/// `--retry` against an older result: a miss recorded in whole seconds is
/// backfilled as a floor, one that has milliseconds keeps them, and fields
/// added since then default instead of failing the load.
#[test]
fn a_saved_miss_without_milliseconds_is_backfilled_from_its_seconds() {
    let mut saved = serde_json::to_value(PhaseMetrics {
        top_misses: vec![
            MissEntry {
                crate_name: "old".into(),
                compile_time_s: 97,
                compile_time_ms: 97_250,
            },
            MissEntry {
                crate_name: "new".into(),
                compile_time_s: 0,
                compile_time_ms: 400,
            },
        ],
        ..Default::default()
    })
    .unwrap();
    saved["top_misses"][0]
        .as_object_mut()
        .unwrap()
        .remove("compile_time_ms");
    for key in ["invalid_reasons", "load"] {
        saved.as_object_mut().unwrap().remove(key);
    }
    for key in ["store_errors", "lookup_rejections", "fallbacks"] {
        saved["event_log"].as_object_mut().unwrap().remove(key);
    }

    let loaded: PhaseMetrics = load_saved_phase(saved).unwrap();

    assert_eq!(loaded.top_misses[0].compile_time_ms, 97_000);
    assert_eq!(loaded.top_misses[1].compile_time_ms, 400);
    assert!(loaded.invalid_reasons.is_empty());
    assert_eq!(loaded.event_log.store_errors, 0);
}

/// With nothing compared there is no ratio. Reporting 0% would read as
/// "every key leaked", the opposite of "nothing to compare".
#[test]
fn key_stability_is_unknown_until_a_crate_was_cached_in_both_clones() {
    let events = |pairs: &[(&str, &str)]| {
        serde_json::json!({
            "all_events": pairs
                .iter()
                .map(|(name, key)| serde_json::json!({ "crate_name": name, "cache_key": key }))
                .collect::<Vec<_>>()
        })
    };

    let disjoint = key_stability(&events(&[("a", "k1")]), &events(&[("b", "k2")]));
    assert_eq!((disjoint.stable_pct, disjoint.compared), (None, 0));

    let one = key_stability(&events(&[("a", "k1")]), &events(&[("a", "k1")]));
    assert_eq!(
        (one.stable_pct, one.stable, one.compared),
        (Some(100.0), 1, 1)
    );

    let half = key_stability(
        &events(&[("a", "k1"), ("b", "k2"), ("c", "k3")]),
        &events(&[("a", "k1"), ("b", "moved"), ("d", "k4")]),
    );
    assert_eq!(
        (half.stable_pct, half.stable, half.compared),
        (Some(50.0), 1, 2)
    );
}

/// The wrapper marks a failed store, a rejected lookup and a fallback on
/// the event itself; each is counted once per event that carries it, and
/// an empty annotation is not one.
#[test]
fn the_event_log_counts_the_annotations_that_invalidate_a_phase() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("events.jsonl");
    std::fs::write(
        &log,
        concat!(
            r#"{"result":"miss","store_error":"disk full"}"#,
            "\n",
            r#"{"result":"miss","store_error":""}"#,
            "\n",
            r#"{"result":"local_hit"}"#,
            "\n",
            r#"{"result":"miss","lookup_rejection":"artifact set incomplete"}"#,
            "\n",
            r#"{"result":"passthrough","passthrough_reason":"unsupported|x","fallback":true}"#,
            "\n",
            r#"{"result":"passthrough","passthrough_reason":"unsupported|y","fallback":false}"#,
            "\n",
            r#"{"result":"miss","store_error":"read-only","lookup_rejection":"stale"}"#,
            "\n",
        ),
    )
    .unwrap();

    let stats = read_event_log(&log);

    assert_eq!(stats.total, 7);
    assert_eq!(
        (stats.store_errors, stats.lookup_rejections, stats.fallbacks),
        (2, 2, 1)
    );
}

#[test]
fn invalid_reasons_name_each_nonzero_count_and_a_missing_daemon() {
    let clean = EventLogStats::default();
    assert!(invalid_reasons(&clean, true).is_empty());
    assert_eq!(invalid_reasons(&clean, false), vec!["daemon_not_running"]);

    let noisy = EventLogStats {
        store_errors: 3,
        lookup_rejections: 1,
        fallbacks: 2,
        ..Default::default()
    };
    assert_eq!(
        invalid_reasons(&noisy, true),
        vec![
            "store_errors:3",
            "lookup_rejections:1",
            "fallback_passthroughs:2"
        ]
    );

    let one = EventLogStats {
        fallbacks: 1,
        ..Default::default()
    };
    assert_eq!(
        invalid_reasons(&one, false),
        vec!["fallback_passthroughs:1", "daemon_not_running"]
    );
}

#[test]
fn build_context_sets_the_load_and_the_invalid_reasons() {
    let load = crate::bench_host::PhaseLoad {
        cpu_pressure_some_us: Some(42),
        ..Default::default()
    };
    let metrics = PhaseMetrics {
        event_log: EventLogStats {
            store_errors: 1,
            ..Default::default()
        },
        ..Default::default()
    }
    .with_build_context(load.clone(), false);

    assert_eq!(metrics.load, load);
    assert_eq!(
        metrics.invalid_reasons,
        vec!["store_errors:1", "daemon_not_running"]
    );
}

/// Probes are not compiles. Counting them in the denominator made a run
/// that declined a quarter of its real compiles look like it declined a
/// tenth, and pass a ceiling it should have failed.
#[test]
fn the_passthrough_rate_divides_by_real_compiles_only() {
    let el = EventLogStats {
        total: 100,
        probed: 60,
        passed_through: 10,
        ..Default::default()
    };
    assert_eq!(compile_count(&el), 40);
    assert_eq!(passthrough_pct(&el), Some(25.0));

    let probes_only = EventLogStats {
        total: 5,
        probed: 5,
        ..Default::default()
    };
    assert_eq!(passthrough_pct(&probes_only), None);

    let single = EventLogStats {
        total: 1,
        passed_through: 1,
        ..Default::default()
    };
    assert_eq!(passthrough_pct(&single), Some(100.0));

    let warm = PhaseMetrics {
        event_log: el,
        ..Default::default()
    };
    let spec = ScenarioAssertSpec {
        max_passthrough_pct: Some(20.0),
        ..Default::default()
    };
    let verdict = Verdict::evaluate(&KeyStability::default(), &warm, Some(&spec));
    assert!(!verdict.ok);
    assert_eq!(verdict.checks[0].actual, "25.0");
    assert!(
        verdict.issues[0].contains("(10 of 40)"),
        "{:?}",
        verdict.issues
    );
}

#[test]
fn fmt_pct_shows_one_decimal_or_not_available() {
    assert_eq!(fmt_pct(Some(96.94)), "96.9%");
    assert_eq!(fmt_pct(None), "n/a");
}

#[test]
fn invalid_reason_lines_skip_absent_and_clean_phases() {
    let flagged = PhaseMetrics {
        invalid_reasons: vec![
            "fallback_passthroughs:4".into(),
            "daemon_not_running".into(),
        ],
        ..Default::default()
    };
    let clean = PhaseMetrics::default();
    assert_eq!(
        invalid_reason_lines(&[
            ("cold", Some(&clean)),
            ("warm-same-tree", None),
            ("pull", Some(&flagged)),
        ]),
        vec![
            "  invalid (pull) : fallback_passthroughs:4, daemon_not_running   (reported, not blocking)"
        ]
    );
}

#[test]
fn summary_marks_unknown_rates_and_lists_invalid_reasons() {
    let mut result = bench_result_fixture();
    let text = summary_text(&result);
    assert!(text.contains("hit rate (n/a weighted)"), "{text}");
    assert!(
        text.contains("key stability : n/a   (0 of 0 crates"),
        "{text}"
    );
    assert!(!text.contains("invalid ("), "{text}");

    result.warm.weighted_hit_rate_pct = Some(97.46);
    result.key_stability = KeyStability {
        stable_pct: Some(96.9),
        stable: 560,
        compared: 578,
    };
    result.warm.invalid_reasons = vec!["store_errors:2".into()];
    let text = summary_text(&result);
    assert!(text.contains("hit rate (97.5% weighted)"), "{text}");
    assert!(
        text.contains("key stability : 96.9%   (560 of 578 crates"),
        "{text}"
    );
    assert!(
        text.contains("invalid (warm) : store_errors:2   (reported, not blocking)"),
        "{text}"
    );
    assert!(!text.contains("invalid (cold)"), "{text}");
}

/// Run the stand-in until it actually executed; see
/// `write_otlp_until_spawned` for why a spawn can transiently fail.
fn daemon_start_until_spawned(kache: &Path, dir: &Path, marker: &Path) -> bool {
    let mut started = false;
    for _ in 0..50 {
        started = daemon::start(kache, dir, &dir.join("kache.toml"));
        if marker.is_file() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    started
}

/// `daemon_not_running` rests on this result, so both answers matter.
#[test]
fn daemon_start_reports_whether_the_start_succeeded() {
    let up = tempfile::tempdir().unwrap();
    let marker = up.path().join("ran");
    let kache = fake_kache_that_records(up.path(), &marker, 0);
    assert!(daemon_start_until_spawned(&kache, up.path(), &marker));
    let args = std::fs::read_to_string(&marker).unwrap();
    assert!(args.contains("daemon") && args.contains("start"), "{args}");

    let down = tempfile::tempdir().unwrap();
    let marker = down.path().join("ran");
    let kache = fake_kache_that_records(down.path(), &marker, 1);
    assert!(!daemon_start_until_spawned(&kache, down.path(), &marker));
    assert!(marker.is_file(), "the failing stand-in must have run");

    assert!(!daemon::start(
        &down.path().join("no-such-kache"),
        down.path(),
        &down.path().join("kache.toml")
    ));
}
