use super::*;
use std::time::{Duration, Instant};

const CACHE_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

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

    fn publish(&self) {
        let entry_dir = self.store.entry_dir(CACHE_KEY);
        std::fs::create_dir_all(&entry_dir).unwrap();
        std::fs::write(
            entry_dir.join("meta.json"),
            serde_json::to_vec(&self.meta).unwrap(),
        )
        .unwrap();
        self.store.insert_entry_row_for_test(CACHE_KEY);
    }
}

// Run in a subprocess so assertions can observe the real compiler streams.
#[test]
#[ignore]
fn completion_child() {
    let mode = std::env::var("KACHE_TEST_RUSTC_HIT").unwrap();
    let fixture = Fixture::new();
    let is_daemon = mode == "daemon";
    let fails = mode == "failure";
    if fails {
        std::fs::remove_file(fixture.store.blob_path(&fixture.meta.files[1].hash)).unwrap();
    }
    let result = match mode.as_str() {
        "remote" => EventResult::RemoteHit,
        "prefetch" => EventResult::PrefetchHit,
        _ => EventResult::LocalHit,
    };
    crate::cache_key::stash_last_dep_info_for_test(fixture.closure());
    let source = if is_daemon {
        BlobSource::StoreDir(fixture.config.store_dir())
    } else {
        BlobSource::Store(&fixture.store)
    };
    let restored = fixture.context().restore_and_finish(
        source,
        &fixture.meta,
        result,
        CACHE_KEY,
        41,
        FileHashStats {
            cache_hits: 3,
            cache_misses: 5,
            bytes_hashed: 73,
        },
        29,
        (!is_daemon).then_some(&fixture.store),
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
        assert!(crate::cache_key::take_last_dep_info().is_some());
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
    assert_eq!(prediction.is_some(), !is_daemon);
    assert!(crate::cache_key::take_last_dep_info().is_none());
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
        ("daemon", "local hit"),
        ("failure", ""),
    ] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
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
    crate::cache_key::stash_last_dep_info_for_test(closure.clone());
    fixture
        .context()
        .restore_and_finish(
            BlobSource::Store(&fallback),
            &fixture.meta,
            EventResult::LocalHit,
            CACHE_KEY,
            0,
            FileHashStats::default(),
            0,
            Some(&fixture.store),
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

    crate::cache_key::stash_last_dep_info_for_test(crate::cache_key::DepInfo {
        source_files: vec!["do-not-record.rs".into()],
        env_deps: Vec::new(),
    });
    fixture
        .context()
        .restore_and_finish(
            BlobSource::Store(&fallback),
            &fixture.meta,
            EventResult::LocalHit,
            CACHE_KEY,
            0,
            FileHashStats::default(),
            0,
            None,
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
    assert!(crate::cache_key::take_last_dep_info().is_none());
}

#[test]
fn remote_hits_preserve_labels_and_record_fresh_predictions() {
    let _lock = crate::test_support::process_state_test_lock();
    let _manifest = TestEnvGuard::remove("CARGO_MANIFEST_DIR");
    for (prefetched, found, record_closure) in [
        (false, true, true),
        (true, true, true),
        (false, false, true),
        (false, true, false),
    ] {
        let mut fixture = Fixture::new();
        fixture.config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
        fixture.publish();
        let daemon = RemoteCheckReplyDaemon::with_reply(
            fixture.config.socket_path(),
            serde_json::json!({"ok": true, "found": found, "prefetched": prefetched}),
        );
        crate::cache_key::stash_last_dep_info_for_test(fixture.closure());
        try_rustc_remote_hit(
            &fixture.context(),
            &fixture.store,
            CACHE_KEY,
            41,
            FileHashStats::default(),
            29,
            record_closure,
        )
        .expect("a committed entry is usable even if the daemon reports found:false")
        .unwrap();
        assert_eq!(daemon.request_count(), 1);
        let events = events::read_events(&fixture.config.event_log_path()).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].result,
            if prefetched {
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
        assert!(crate::cache_key::take_last_dep_info().is_none());
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
fn daemon_hits_need_no_writable_index() {
    let _lock = crate::test_support::process_state_test_lock();
    let _manifest = TestEnvGuard::remove("CARGO_MANIFEST_DIR");
    let Fixture {
        dir: _dir,
        config,
        store,
        compiler,
        args,
        meta,
    } = Fixture::new();
    drop(store);
    std::fs::remove_file(config.index_db_path()).unwrap();
    std::fs::create_dir(config.index_db_path()).unwrap();
    let _daemon = RemoteCheckReplyDaemon::with_reply(
        config.socket_path(),
        serde_json::json!({"ok": true, "local_lookup": crate::daemon::LocalLookupReply::hit(meta)}),
    );
    let hit = RustcHitContext {
        config: &config,
        compiler: &compiler,
        args: &args,
        crate_name: "foo",
        event_root: "/consumer",
        start: Instant::now(),
        extra_inputs: None,
    };
    assert_eq!(
        try_daemon_local_hit(&hit, CACHE_KEY, 0, FileHashStats::default()),
        Some(0)
    );
    assert!(config.index_db_path().is_dir());
    assert_eq!(
        events::read_events(&config.event_log_path()).unwrap().len(),
        1
    );
}

#[test]
fn daemon_declines_invalid_replies_and_failed_restores() {
    let _lock = crate::test_support::process_state_test_lock();
    let _manifest = TestEnvGuard::remove("CARGO_MANIFEST_DIR");
    for mode in ["miss", "no-meta", "empty", "wrong-key", "missing-blob"] {
        let fixture = Fixture::new();
        let mut reply = crate::daemon::LocalLookupReply::hit(fixture.meta.clone());
        match mode {
            "miss" => reply.outcome = "miss".into(),
            "no-meta" => reply.meta = None,
            "empty" => reply.meta.as_mut().unwrap().files.clear(),
            "wrong-key" => reply.meta.as_mut().unwrap().cache_key = "other-key".into(),
            "missing-blob" => {
                std::fs::remove_file(fixture.store.blob_path(&fixture.meta.files[0].hash)).unwrap()
            }
            _ => unreachable!(),
        }
        let _daemon = RemoteCheckReplyDaemon::with_reply(
            fixture.config.socket_path(),
            serde_json::json!({"ok": true, "local_lookup": reply}),
        );
        assert_eq!(
            try_daemon_local_hit(&fixture.context(), CACHE_KEY, 0, FileHashStats::default()),
            None,
            "{mode}"
        );
        assert!(!fixture.config.event_log_path().exists(), "{mode}");
        assert!(
            fixture.args.incremental.as_ref().unwrap().is_dir(),
            "{mode}"
        );
    }
}
