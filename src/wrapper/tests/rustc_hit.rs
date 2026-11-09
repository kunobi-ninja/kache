use super::*;
use std::time::{Duration, Instant};

const CACHE_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

fn shared_key() -> rustc_diagnostics::EntryKey {
    rustc_diagnostics::EntryKey::shared(CACHE_KEY)
}

/// A key record whose group digests an event can be checked for.
fn key_record() -> KeyEventRecord {
    let mut record = KeyEventRecord::default();
    record.absorb(&crate::cache_key::KeyOutputs {
        fields: Some(std::collections::BTreeMap::from([(
            "args".to_string(),
            "aaaa".to_string(),
        )])),
        ..Default::default()
    });
    record
}

fn key_with(dep_info: crate::cache_key::DepInfo) -> crate::cache_key::KeyOutputs {
    crate::cache_key::KeyOutputs {
        dep_info: Some(dep_info),
        ..Default::default()
    }
}

struct Fixture {
    dir: tempfile::TempDir,
    config: Config,
    store: Store,
    compiler: RustcCompiler,
    args: RustcArgs,
    meta: EntryMeta,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path().join("cache"));
        config.input_predictions = true;
        let store = Store::open(&config).unwrap();
        let out_dir = dir.path().join("target/debug/deps");
        let incremental = dir.path().join("incremental");
        std::fs::create_dir(&incremental).unwrap();
        std::fs::write(incremental.join("state"), b"incremental state").unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, "pub fn f() {}\n").unwrap();
        let args = rustc_args(&[
            "rustc",
            source.to_str().unwrap(),
            "--crate-name",
            "foo",
            "--emit",
            "metadata",
            "--out-dir",
            out_dir.to_str().unwrap(),
            "-C",
            &format!("incremental={}", incremental.display()),
        ]);
        let files = [
            ("libfoo.rmeta", b"metadata".as_slice()),
            ("second.rmeta", b"second".as_slice()),
        ]
        .into_iter()
        .map(|(name, content)| {
            let hash = blake3::hash(content).to_hex().to_string();
            create_blob(&store, &hash, content);
            let mut file = cached_file(name, &hash);
            file.size = content.len() as u64;
            file
        })
        .collect();
        let mut meta = entry_meta(CACHE_KEY, files, &["metadata"]);
        meta.stdout = "HIT-STDOUT\n".into();
        meta.stderr = "HIT-STDERR\n".into();
        Self {
            dir,
            config,
            store,
            compiler: RustcCompiler::new(),
            args,
            meta,
        }
    }

    fn context(&self) -> RustcHitContext<'_> {
        RustcHitContext {
            config: &self.config,
            compiler: &self.compiler,
            memo_store: &self.store,
            args: &self.args,
            crate_name: "foo",
            event_root: "/consumer",
            start: Instant::now() - Duration::from_millis(1500),
            extra_inputs: None,
        }
    }

    fn closure(&self) -> crate::cache_key::DepInfo {
        crate::cache_key::DepInfo {
            source_files: vec![self.dir.path().join("lib.rs")],
            env_deps: Vec::new(),
        }
    }

    fn key(&self) -> crate::cache_key::KeyOutputs {
        key_with(self.closure())
    }

    fn publish(&self) {
        self.publish_under(CACHE_KEY);
    }

    fn publish_under(&self, key: &str) {
        let entry_dir = self.store.entry_dir(key);
        std::fs::create_dir_all(&entry_dir).unwrap();
        std::fs::write(
            entry_dir.join("meta.json"),
            serde_json::to_vec(&self.meta).unwrap(),
        )
        .unwrap();
        self.store.insert_entry_row_for_test(key);
    }
}

fn shared_width_fixture() -> Fixture {
    let mut fixture = Fixture::new();
    let mut argv = vec![fixture.args.rustc.display().to_string()];
    argv.extend_from_slice(&fixture.args.all_args);
    argv.extend([
        "--error-format=json".to_string(),
        "--json=artifacts,future-incompat".to_string(),
        "--diagnostic-width=80".to_string(),
    ]);
    fixture.args = RustcArgs::parse(&argv).unwrap();
    fixture
}

fn width_key() -> rustc_diagnostics::EntryKey {
    rustc_diagnostics::lookup_keys(&shared_width_fixture().args, CACHE_KEY)
        .pop()
        .filter(|key| key.width == Some(80))
        .unwrap()
}

#[test]
fn shared_width_entry_eligibility_requires_files_and_compatible_diagnostics() {
    let mut fixture = shared_width_fixture();
    let usable = |args: &RustcArgs, key: &rustc_diagnostics::EntryKey, meta: &EntryMeta| {
        rustc_diagnostics::entry_usable(args, key, meta)
    };
    let ordinary = Fixture::new();
    // Rendered output: only the width's own key, or an invocation without a
    // width, may replay it.
    assert!(!usable(&fixture.args, &shared_key(), &fixture.meta));
    assert!(usable(&fixture.args, &width_key(), &fixture.meta));
    assert!(usable(&ordinary.args, &shared_key(), &fixture.meta));
    fixture.meta.stdout.clear();
    fixture.meta.stderr.clear();
    assert!(usable(&fixture.args, &shared_key(), &fixture.meta));
    fixture.meta.files.clear();
    for (args, key) in [
        (&fixture.args, shared_key()),
        (&fixture.args, width_key()),
        (&ordinary.args, shared_key()),
    ] {
        assert!(!usable(args, &key, &fixture.meta));
    }
}

#[test]
fn shared_width_policy_miss_reacquires_the_scheduler_flight_after_contention() {
    let _lock = crate::test_support::process_state_test_lock();
    // The file-only predicate reproduces the old route: it hands back an
    // unusable hit and drops the permit before the compiler runs.
    for file_only in [true, false] {
        let fixture = shared_width_fixture();
        fixture.publish();
        let cache = fixture.config.cache_dir.clone();
        let mut holder = spawn_flight_holder(&cache, "foo", CACHE_KEY);
        wait_flight_ready(&cache, &mut holder);
        let config = fixture.config.clone();
        let args = fixture.args.clone();
        let waiter = std::thread::spawn(move || {
            let store = Store::open(&config).unwrap();
            admit_scheduler_miss(
                &config,
                &store,
                CACHE_KEY,
                FlightIdentity::rustc("foo", &["lib".to_string()], false),
                "foo",
                false,
                |meta| {
                    if file_only {
                        cache_entry_has_files(meta)
                    } else {
                        rustc_diagnostics::entry_usable(&args, &shared_key(), meta)
                    }
                },
            )
        });
        std::thread::sleep(Duration::from_millis(200));
        std::fs::write(cache.join("go"), b"go").unwrap();
        let (guard, hit) = waiter.join().unwrap();
        assert_eq!(hit.is_some(), file_only);
        assert_eq!(guard.is_empty(), file_only);
        assert!(holder.wait().unwrap().success());
    }
}

#[test]
fn shared_width_hits_reject_legacy_diagnostics_before_restoring_any_output() {
    let _lock = crate::test_support::process_state_test_lock();
    let mut fixture = shared_width_fixture();
    let out_dir = fixture.args.out_dir.as_ref().unwrap();
    std::fs::create_dir_all(out_dir).unwrap();
    let output = out_dir.join("libfoo.rmeta");
    std::fs::write(&output, b"consumer output").unwrap();
    let mut legacy = serde_json::to_value(&fixture.meta).unwrap();
    legacy.as_object_mut().unwrap().remove("key_schema");
    fixture.meta = serde_json::from_value(legacy).unwrap();
    assert_eq!(fixture.meta.key_schema, 0);
    for (stdout, stderr) in [
        (
            "",
            r#"{"$message_type":"future_incompat","future_incompat_report":[{"diagnostic":{"rendered":"warning at an old width"}}]}"#,
        ),
        ("unexpected compiler output", ""),
        ("", "{invalid JSON"),
        ("", r#"{"$message_type":"unknown"}"#),
    ] {
        fixture.meta.stdout = stdout.to_string();
        fixture.meta.stderr = stderr.to_string();
        assert!(
            !fixture
                .context()
                .accepts_diagnostics(&shared_key(), &fixture.meta)
        );
        for result in [
            EventResult::LocalHit,
            EventResult::RemoteHit,
            EventResult::PrefetchHit,
        ] {
            let error = fixture
                .context()
                .restore_and_finish(
                    &fixture.store,
                    &fixture.meta,
                    result,
                    &shared_key(),
                    0,
                    FileHashStats::default(),
                    0,
                    Some(&fixture.store),
                    &fixture.key(),
                    &key_record(),
                )
                .unwrap_err();
            assert!(
                error.to_string().contains("original terminal width"),
                "{error:#}"
            );
            assert_eq!(std::fs::read(&output).unwrap(), b"consumer output");
            assert!(!out_dir.join("second.rmeta").exists());
            assert!(!fixture.config.event_log_path().exists());
            assert!(fixture.args.incremental.as_ref().unwrap().is_dir());
            let identity = crate::cache_key::rustc_prediction_identity(&fixture.args).unwrap();
            assert!(
                fixture
                    .store
                    .file_hasher()
                    .input_prediction(&identity)
                    .is_none()
            );
        }
    }
}

#[test]
fn shared_width_concurrent_winner_is_validated_before_restore() {
    let _lock = crate::test_support::process_state_test_lock();
    let mut fixture = shared_width_fixture();
    fixture.meta.stdout.clear();
    fixture.meta.stderr = r#"{"$message_type":"diagnostic","rendered":"old warning"}"#.to_string();
    fixture.publish();
    let BuildClaim::Committed(meta) = fixture.store.claim_build(CACHE_KEY).unwrap() else {
        panic!("published concurrent entry must be visible");
    };
    assert!(!fixture.context().accepts_diagnostics(&shared_key(), &meta));
    let error = fixture
        .context()
        .restore_and_finish(
            &fixture.store,
            &meta,
            EventResult::LocalHit,
            &shared_key(),
            0,
            FileHashStats::default(),
            0,
            None,
            &fixture.key(),
            &key_record(),
        )
        .unwrap_err();
    assert!(error.to_string().contains("original terminal width"));
    assert!(!fixture.args.out_dir.as_ref().unwrap().exists());
    assert!(!fixture.config.event_log_path().exists());
}

#[test]
fn width_claim_keeps_the_lock_past_a_rendered_shared_entry() {
    let _lock = crate::test_support::process_state_test_lock();
    let mut fixture = shared_width_fixture();
    fixture.meta.stdout.clear();
    fixture.meta.stderr =
        r#"{"$message_type":"diagnostic","rendered":"piped warning"}"#.to_string();
    fixture.publish();
    // A piped compile's rendered entry cannot serve this width, so this
    // invocation compiles and must own the lock to store its own entry.
    let RustcClaim::Acquired(lock) =
        claim_rustc_build(&fixture.store, &fixture.args, CACHE_KEY).unwrap()
    else {
        panic!("a width must claim past a rendered shared entry");
    };
    drop(lock);
    let piped = Fixture::new();
    let RustcClaim::Committed(key, _) =
        claim_rustc_build(&fixture.store, &piped.args, CACHE_KEY).unwrap()
    else {
        panic!("a piped compile must use the piped entry");
    };
    assert_eq!(key, shared_key());

    fixture.publish_under(&width_key().key);
    let RustcClaim::Committed(key, _) =
        claim_rustc_build(&fixture.store, &fixture.args, CACHE_KEY).unwrap()
    else {
        panic!("a peer's entry for this width must be found");
    };
    assert_eq!(key, width_key());
    let (key, _) = rustc_diagnostics::usable_entry(&fixture.store, &fixture.args, CACHE_KEY)
        .unwrap()
        .unwrap();
    assert_eq!(key, width_key());
}

#[test]
fn width_waiter_waits_past_a_rendered_shared_entry_for_its_own() {
    let _lock = crate::test_support::process_state_test_lock();
    let mut fixture = shared_width_fixture();
    fixture.meta.stdout.clear();
    fixture.meta.stderr =
        r#"{"$message_type":"diagnostic","rendered":"piped warning"}"#.to_string();
    fixture.publish();
    let held = fixture.store.try_lock(CACHE_KEY).unwrap().unwrap();
    let config = fixture.config.clone();
    let width = width_key().key;
    let meta = fixture.meta.clone();
    let peer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        let store = Store::open(&config).unwrap();
        let entry_dir = store.entry_dir(&width);
        std::fs::create_dir_all(&entry_dir).unwrap();
        std::fs::write(
            entry_dir.join("meta.json"),
            serde_json::to_vec(&meta).unwrap(),
        )
        .unwrap();
        store.insert_entry_row_for_test(&width);
        drop(held);
    });
    let (key, _) = wait_for_rustc_peer(&fixture.store, &fixture.args, CACHE_KEY)
        .expect("the peer's entry for this width");
    assert_eq!(key, width_key());
    peer.join().unwrap();
}

#[test]
fn a_waiter_whose_peer_stored_another_width_claims_the_build() {
    let _lock = crate::test_support::process_state_test_lock();
    let fixture = shared_width_fixture();
    let held = fixture.store.try_lock(CACHE_KEY).unwrap().unwrap();
    let config = fixture.config.clone();
    let other_width = rustc_diagnostics::EntryKey::for_width(CACHE_KEY, 200).key;
    let meta = fixture.meta.clone();
    let peer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        let store = Store::open(&config).unwrap();
        let entry_dir = store.entry_dir(&other_width);
        std::fs::create_dir_all(&entry_dir).unwrap();
        std::fs::write(
            entry_dir.join("meta.json"),
            serde_json::to_vec(&meta).unwrap(),
        )
        .unwrap();
        store.insert_entry_row_for_test(&other_width);
        drop(held);
    });
    // Nothing this width can use: it compiles, and must hold the lock to
    // store its own entry instead of compiling uncached.
    let (lock, hit) = wait_then_claim_rustc(&fixture.store, &fixture.args, CACHE_KEY);
    assert!(hit.is_none());
    assert!(lock.is_some(), "the waiter must claim the build");
    peer.join().unwrap();
}

#[test]
fn shared_width_safe_legacy_entry_still_restores() {
    let _lock = crate::test_support::process_state_test_lock();
    let mut fixture = shared_width_fixture();
    fixture.meta.key_schema = 0;
    fixture.meta.stdout.clear();
    fixture.meta.stderr = r#"{"$message_type":"unused_extern","lint_level":"force-warn","unused_extern_names":["dep"]}"#.to_string();
    assert!(
        fixture
            .context()
            .accepts_diagnostics(&shared_key(), &fixture.meta)
    );
    fixture
        .context()
        .restore_and_finish(
            &fixture.store,
            &fixture.meta,
            EventResult::LocalHit,
            &shared_key(),
            0,
            FileHashStats::default(),
            0,
            None,
            &fixture.key(),
            &key_record(),
        )
        .unwrap();
    assert_eq!(
        std::fs::read(fixture.args.out_dir.as_ref().unwrap().join("libfoo.rmeta")).unwrap(),
        b"metadata",
    );
    assert_eq!(
        events::read_events(&fixture.config.event_log_path())
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn shared_width_remote_and_prefetch_unsafe_entries_are_policy_misses() {
    let _lock = crate::test_support::process_state_test_lock();
    for prefetched in [false, true] {
        let mut fixture = shared_width_fixture();
        fixture.config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
        fixture.meta.stdout.clear();
        fixture.meta.stderr =
            r#"{"$message_type":"diagnostic","rendered":"old wrapping"}"#.to_string();
        fixture.publish();
        let daemon = RemoteCheckReplyDaemon::with_reply(
            fixture.config.socket_path(),
            serde_json::json!({"ok": true, "found": true, "prefetched": prefetched}),
        );
        assert!(
            try_rustc_remote_hit(
                &fixture.context(),
                &fixture.store,
                CACHE_KEY,
                0,
                FileHashStats::default(),
                0,
                true,
                &fixture.key(),
                &key_record(),
            )
            .is_none()
        );
        // The shared key, then this width's key, which holds nothing.
        assert_eq!(daemon.request_count(), 2);
        assert!(!fixture.args.out_dir.as_ref().unwrap().exists());
        assert!(!fixture.config.event_log_path().exists());
        assert!(fixture.args.incremental.as_ref().unwrap().exists());
    }
}

// Run in a subprocess so assertions can observe the real compiler streams.
#[test]
#[ignore]
fn completion_child() {
    let mode = std::env::var("KACHE_TEST_RUSTC_HIT").unwrap();
    let fixture = Fixture::new();
    let fails = mode == "failure";
    if fails {
        std::fs::remove_file(fixture.store.blob_path(&fixture.meta.files[1].hash)).unwrap();
    }
    let result = match mode.as_str() {
        "remote" => EventResult::RemoteHit,
        "prefetch" => EventResult::PrefetchHit,
        _ => EventResult::LocalHit,
    };
    let restored = fixture.context().restore_and_finish(
        &fixture.store,
        &fixture.meta,
        result,
        &shared_key(),
        41,
        FileHashStats {
            cache_hits: 3,
            cache_misses: 5,
            bytes_hashed: 73,
        },
        29,
        Some(&fixture.store),
        &fixture.key(),
        &key_record(),
    );
    let identity = crate::cache_key::rustc_prediction_identity(&fixture.args).unwrap();
    let prediction = fixture.store.file_hasher().input_prediction(&identity);
    if fails {
        assert!(restored.is_err());
        assert!(
            !fixture.config.event_log_path().exists(),
            "a failed restore must not report a hit"
        );
        assert!(fixture.args.incremental.as_ref().unwrap().is_dir());
        assert!(prediction.is_none());
        return;
    }
    restored.unwrap();
    assert_eq!(
        std::fs::read(fixture.args.out_dir.as_ref().unwrap().join("libfoo.rmeta")).unwrap(),
        b"metadata"
    );
    assert_eq!(
        std::fs::read(fixture.args.out_dir.as_ref().unwrap().join("second.rmeta")).unwrap(),
        b"second"
    );
    assert!(!fixture.args.incremental.as_ref().unwrap().exists());
    assert!(prediction.is_some());
    let events = events::read_events(&fixture.config.event_log_path()).unwrap();
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.result, result);
    assert_eq!(event.root, "/consumer");
    assert_eq!(event.crate_name, "foo");
    assert_eq!(event.cache_key, CACHE_KEY);
    assert_eq!(event.size, 14);
    assert_eq!(event.compile_time_ms, 7);
    assert!(event.elapsed_ms >= 1500);
    assert_eq!(event.key_ms, 41);
    assert_eq!(
        event.key_fields.get("args").map(String::as_str),
        Some("aaaa")
    );
    assert_eq!(event.key_hash_hits, 3);
    assert_eq!(event.key_hash_misses, 5);
    assert_eq!(event.key_hash_bytes, 73);
    assert_eq!(event.lookup_ms, 29);
    assert_eq!(event.store_ms, 0);
}

#[test]
fn completion_replays_diagnostics_and_progress_only_after_success() {
    let _lock = crate::test_support::process_state_test_lock();
    for (mode, label) in [
        ("local", "local hit"),
        ("remote", "remote hit"),
        ("prefetch", "prefetch hit"),
        ("failure", ""),
    ] {
        let output = crate::test_support::without_terminal(&mut std::process::Command::new(
            std::env::current_exe().unwrap(),
        ))
        .args([
            "--exact",
            "wrapper::tests::rustc_hit::completion_child",
            "--ignored",
            "--nocapture",
        ])
        .env("KACHE_TEST_RUSTC_HIT", mode)
        .env("KACHE_PROGRESS", "1")
        .env_remove("CARGO_MANIFEST_DIR")
        .output()
        .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{mode}: {stdout}\n{stderr}");
        let expected = usize::from(mode != "failure");
        assert_eq!(
            stdout.matches("HIT-STDOUT\n").count(),
            expected,
            "{mode}: {stdout}"
        );
        assert_eq!(
            stderr.matches("HIT-STDERR\n").count(),
            expected,
            "{mode}: {stderr}"
        );
        assert_eq!(
            stderr.contains("[kache] foo:"),
            mode != "failure",
            "{mode}: {stderr}"
        );
        if mode != "failure" {
            assert!(
                stderr.contains(&format!("[kache] foo: {label}")),
                "{mode}: {stderr}"
            );
        }
    }
}

#[test]
fn predictions_belong_to_the_key_store_and_are_not_rewritten_when_declined() {
    let _lock = crate::test_support::process_state_test_lock();
    let _manifest = TestEnvGuard::remove("CARGO_MANIFEST_DIR");
    let fixture = Fixture::new();
    let fallback = Store::open(&test_config(fixture.dir.path().join("fallback"))).unwrap();
    for file in &fixture.meta.files {
        let bytes = std::fs::read(fixture.store.blob_path(&file.hash)).unwrap();
        create_blob(&fallback, &file.hash, &bytes);
        std::fs::remove_file(fixture.store.blob_path(&file.hash)).unwrap();
    }
    let closure = fixture.closure();
    fixture
        .context()
        .restore_and_finish(
            &fallback,
            &fixture.meta,
            EventResult::LocalHit,
            &shared_key(),
            0,
            FileHashStats::default(),
            0,
            Some(&fixture.store),
            &fixture.key(),
            &KeyEventRecord::default(),
        )
        .unwrap();
    let identity = crate::cache_key::rustc_prediction_identity(&fixture.args).unwrap();
    assert_eq!(
        fixture
            .store
            .file_hasher()
            .input_prediction(&identity)
            .unwrap()
            .sources,
        closure.source_files
    );
    assert!(fallback.file_hasher().input_prediction(&identity).is_none());

    fixture
        .context()
        .restore_and_finish(
            &fallback,
            &fixture.meta,
            EventResult::LocalHit,
            &shared_key(),
            0,
            FileHashStats::default(),
            0,
            None,
            &key_with(crate::cache_key::DepInfo {
                source_files: vec!["do-not-record.rs".into()],
                env_deps: Vec::new(),
            }),
            &KeyEventRecord::default(),
        )
        .unwrap();
    assert_eq!(
        fixture
            .store
            .file_hasher()
            .input_prediction(&identity)
            .unwrap()
            .sources,
        closure.source_files
    );
}

#[test]
fn remote_hits_report_provenance_and_record_fresh_predictions() {
    let _lock = crate::test_support::process_state_test_lock();
    let _manifest = TestEnvGuard::remove("CARGO_MANIFEST_DIR");
    for (prefetched, found, record_closure) in [
        (false, true, true),
        (true, true, true),
        (false, false, true),
        (true, false, true),
        (false, true, false),
    ] {
        let mut fixture = Fixture::new();
        fixture.config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
        fixture.publish();
        let daemon = RemoteCheckReplyDaemon::with_reply(
            fixture.config.socket_path(),
            serde_json::json!({"ok": true, "found": found, "prefetched": prefetched}),
        );
        try_rustc_remote_hit(
            &fixture.context(),
            &fixture.store,
            CACHE_KEY,
            41,
            FileHashStats::default(),
            29,
            record_closure,
            &fixture.key(),
            &key_record(),
        )
        .expect("a committed entry is usable even if the daemon reports found:false")
        .unwrap();
        assert_eq!(daemon.request_count(), 1);
        let events = events::read_events(&fixture.config.event_log_path()).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].key_fields.get("args").map(String::as_str),
            Some("aaaa")
        );
        assert_eq!(
            events[0].result,
            if !found {
                EventResult::LocalHit
            } else if prefetched {
                EventResult::PrefetchHit
            } else {
                EventResult::RemoteHit
            }
        );
        let identity = crate::cache_key::rustc_prediction_identity(&fixture.args).unwrap();
        let prediction = fixture.store.file_hasher().input_prediction(&identity);
        assert_eq!(prediction.is_some(), record_closure);
        if let Some(prediction) = prediction {
            assert_eq!(prediction.sources, fixture.closure().source_files);
        }
    }
}

#[test]
fn remote_misses_and_failed_restores_do_not_complete_hits() {
    let _lock = crate::test_support::process_state_test_lock();
    let _manifest = TestEnvGuard::remove("CARGO_MANIFEST_DIR");
    for mode in ["disabled", "no-reply", "no-entry", "restore-error"] {
        let mut fixture = Fixture::new();
        if mode != "disabled" {
            fixture.config.remote =
                Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
        }
        if mode == "restore-error" {
            fixture.meta.emit_kinds = vec!["link".into()];
        }
        if mode != "no-entry" {
            fixture.publish();
        }
        let daemon = (mode != "no-reply")
            .then(|| RemoteCheckReplyDaemon::spawn(fixture.config.socket_path(), true));
        let restored = try_rustc_remote_hit(
            &fixture.context(),
            &fixture.store,
            CACHE_KEY,
            0,
            FileHashStats::default(),
            0,
            true,
            &fixture.key(),
            &KeyEventRecord::default(),
        );
        if mode == "restore-error" {
            assert!(restored.unwrap().is_err());
        } else {
            assert!(restored.is_none(), "{mode}");
        }
        if let Some(daemon) = daemon {
            assert_eq!(daemon.request_count(), usize::from(mode != "disabled"));
        }
        assert!(!fixture.config.event_log_path().exists(), "{mode}");
        assert!(
            fixture.args.incremental.as_ref().unwrap().is_dir(),
            "{mode}"
        );
    }
}

#[test]
fn readonly_hit_records_restored_output_hashes_in_the_primary_store() {
    let _lock = crate::test_support::process_state_test_lock();
    let fixture = Fixture::new();
    let content = vec![42; 65536];
    let hash = blake3::hash(&content).to_hex().to_string();
    create_blob(&fixture.store, &hash, &content);
    let mut meta = fixture.meta.clone();
    meta.files[0].hash = hash.clone();
    meta.files[0].size = content.len() as u64;
    let readonly = Store::open_read_only(&fixture.config).unwrap();
    let primary = Store::open(&test_config(fixture.dir.path().join("primary"))).unwrap();
    let mut hit = fixture.context();
    hit.memo_store = &primary;
    hit.restore_and_finish(
        &readonly,
        &meta,
        EventResult::LocalHit,
        &shared_key(),
        0,
        FileHashStats::default(),
        0,
        None,
        &fixture.key(),
        &key_record(),
    )
    .unwrap();
    let output = fixture.args.out_dir.as_ref().unwrap().join("libfoo.rmeta");
    assert!(matches!(primary.file_hash_lookup(&output),
        crate::cache_key::FileHashLookup::Hit(found) if found == hash));
    assert!(matches!(
        fixture.store.file_hash_lookup(&output),
        crate::cache_key::FileHashLookup::NeedsHash(_)
    ));
}

#[test]
fn incomplete_or_mismatched_executable_pairs_never_publish_a_hit() {
    let _lock = crate::test_support::process_state_test_lock();
    let invocation = crate::invocation::InvocationEnv::capture();
    for result in [
        EventResult::LocalHit,
        EventResult::RemoteHit,
        EventResult::PrefetchHit,
    ] {
        for (name, mismatch) in [("app.exe", false), ("app", false), ("app.custom", true)] {
            let mut fixture = Fixture::new();
            let out_dir = fixture.dir.path().join("target/debug/deps");
            std::fs::create_dir_all(&out_dir).unwrap();
            let output = out_dir.join(name);
            fixture.args = RustcArgs::parse_for(
                &s(&[
                    "rustc",
                    fixture.dir.path().join("lib.rs").to_str().unwrap(),
                    "--crate-type=bin",
                    "--crate-name=app",
                    "--emit=link",
                    "-o",
                    output.to_str().unwrap(),
                ]),
                &invocation,
            )
            .unwrap();
            fixture.compiler = RustcCompiler::new_for(&invocation);
            let (exe, mut pdb) = crate::pe_pdb::fixture_pair();
            let exe_hash = blake3::hash(&exe).to_hex().to_string();
            create_blob(&fixture.store, &exe_hash, &exe);
            let mut exe_file = cached_file(name, &exe_hash);
            exe_file.size = exe.len() as u64;
            fixture.meta.files = vec![exe_file];
            if mismatch {
                pdb[1536 + 12] ^= 1; // PDB information GUID, not an unrelated padding byte.
                let hash = blake3::hash(&pdb).to_hex().to_string();
                create_blob(&fixture.store, &hash, &pdb);
                let mut file = cached_file("app.pdb", &hash);
                file.size = pdb.len() as u64;
                fixture.meta.files.push(file);
            }
            fixture.meta.emit_kinds = vec!["link".into()];
            fixture.publish();
            std::fs::write(&output, b"existing executable").unwrap();
            let destination_pdb = out_dir.join("app.pdb");
            std::fs::write(&destination_pdb, b"existing database").unwrap();
            let error = fixture
                .context()
                .restore_and_finish(
                    &fixture.store,
                    &fixture.meta,
                    result,
                    &shared_key(),
                    0,
                    FileHashStats::default(),
                    0,
                    Some(&fixture.store),
                    &fixture.key(),
                    &key_record(),
                )
                .unwrap_err();
            assert!(
                format!("{error:#}").contains(if mismatch {
                    "GUID or age differ"
                } else {
                    "not among the outputs"
                }),
                "{error:#}"
            );
            assert_eq!(std::fs::read(&output).unwrap(), b"existing executable");
            assert_eq!(
                std::fs::read(&destination_pdb).unwrap(),
                b"existing database"
            );
            assert!(
                !fixture.config.event_log_path().exists(),
                "{result:?}/{name}: a failed pair was reported as a hit"
            );
            assert!(
                fixture.store.get(CACHE_KEY).unwrap().is_none(),
                "the writable corrupt entry must be evicted"
            );
        }
    }
}

#[test]
fn unidentifiable_executable_entries_evict_only_the_writable_source() {
    let _lock = crate::test_support::process_state_test_lock();
    let invocation = crate::invocation::InvocationEnv::capture();
    for result in [
        EventResult::LocalHit,
        EventResult::RemoteHit,
        EventResult::PrefetchHit,
    ] {
        for readonly in [false, true] {
            for (ambiguous, explicit) in [(false, false), (false, true), (true, false)] {
                let mut fixture = Fixture::new();
                let out_dir = fixture.dir.path().join("target/debug/deps");
                let mut argv = s(&[
                    "rustc",
                    fixture.dir.path().join("lib.rs").to_str().unwrap(),
                    "--crate-type=bin",
                    "--crate-name=app",
                    "--emit=link",
                ]);
                argv.extend(if explicit {
                    vec![
                        "-o".into(),
                        out_dir.join("app.custom").display().to_string(),
                    ]
                } else {
                    vec!["--out-dir".into(), out_dir.display().to_string()]
                });
                fixture.args = RustcArgs::parse_for(&argv, &invocation).unwrap();
                fixture.compiler = RustcCompiler::new_for(&invocation);
                let (exe, pdb) = crate::pe_pdb::fixture_pair();
                let mut outputs = vec![("app.pdb", pdb.as_slice())];
                if ambiguous {
                    outputs.extend([("app.exe", exe.as_slice()), ("other.exe", exe.as_slice())]);
                }
                fixture.meta.files = outputs
                    .into_iter()
                    .map(|(name, bytes)| {
                        let hash = blake3::hash(bytes).to_hex().to_string();
                        create_blob(&fixture.store, &hash, bytes);
                        let mut file = cached_file(name, &hash);
                        file.size = bytes.len() as u64;
                        file.executable = name.ends_with(".exe");
                        file
                    })
                    .collect();
                fixture.meta.crate_types = vec!["bin".into()];
                fixture.meta.emit_kinds = vec!["link".into()];
                fixture.publish();
                let metadata = fixture.store.entry_dir(CACHE_KEY).join("meta.json");
                let before = std::fs::read(&metadata).unwrap();
                let readonly_store =
                    readonly.then(|| Store::open_read_only(&fixture.config).unwrap());
                let source = readonly_store.as_ref().unwrap_or(&fixture.store);
                assert_eq!(source.is_read_only(), readonly);
                let error = fixture
                    .context()
                    .restore_and_finish(
                        source,
                        &fixture.meta,
                        result,
                        &shared_key(),
                        0,
                        FileHashStats::default(),
                        0,
                        Some(&fixture.store),
                        &fixture.key(),
                        &key_record(),
                    )
                    .unwrap_err();
                assert!(
                    format!("{error:#}").contains(if ambiguous {
                        "ambiguous linked executable outputs"
                    } else if explicit {
                        "executable output app.custom is missing"
                    } else {
                        "cannot identify the linked executable output"
                    }),
                    "{error:#}"
                );
                assert!(
                    !out_dir.exists(),
                    "no destination may be created before executable admission"
                );
                assert!(
                    !fixture.config.event_log_path().exists(),
                    "{result:?}/{readonly}/{ambiguous}/{explicit}: failed admission reported a hit"
                );
                assert_eq!(fixture.store.get(CACHE_KEY).unwrap().is_some(), readonly);
                if readonly {
                    assert_eq!(std::fs::read(&metadata).unwrap(), before);
                } else {
                    assert!(!metadata.exists());
                }
            }
        }
    }
}
