use super::*;
use std::time::{Duration, Instant};

#[test]
fn report_preserves_phase_metrics() {
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let mut first = cached_file("first.o", "a");
    first.size = 13;
    let mut second = cached_file("second.o", "b");
    second.size = 29;
    let meta = entry_meta("stored-key", vec![first, second], &[]);
    HitCompletion {
        event_root: "/consumer",
        crate_name: "consumer.c",
        result: EventResult::PrefetchHit,
        cache_key: "requested-key",
        start: Instant::now() - Duration::from_millis(1500),
        key_ms: 41,
        key_hash_stats: FileHashStats {
            cache_hits: 3,
            cache_misses: 5,
            bytes_hashed: 73,
        },
        lookup_ms: 23,
        restore_ms: 19,
    }
    .report(&config, &meta);
    let events = events::read_events(&config.event_log_path()).unwrap();
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.root, "/consumer");
    assert_eq!(event.crate_name, "consumer.c");
    assert_eq!(event.result, EventResult::PrefetchHit);
    assert_eq!(event.cache_key, "requested-key");
    assert_eq!(event.size, 42);
    assert_eq!(event.compile_time_ms, 7);
    assert!(event.elapsed_ms >= 1500);
    assert_eq!(event.key_ms, 41);
    assert_eq!(event.key_hash_hits, 3);
    assert_eq!(event.key_hash_misses, 5);
    assert_eq!(event.key_hash_bytes, 73);
    assert_eq!(event.lookup_ms, 23);
    assert_eq!(event.restore_ms, 19);
    assert_eq!(event.store_ms, 0);
}

// Capture the real reporting streams without redirecting other tests' output.
#[cfg(unix)]
#[test]
#[ignore]
fn remote_completion_child() {
    use std::os::unix::fs::PermissionsExt;

    let family = std::env::var("KACHE_TEST_HIT_FAMILY").unwrap();
    let mode = std::env::var("KACHE_TEST_HIT_MODE").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().join("cache"));
    config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
    let store = Store::open(&config).unwrap();
    let key = blake3::hash(b"completion-entry").to_hex().to_string();
    seed_cc_object_entry(&store, &key, dir.path());
    let mut meta = store.get(&key).unwrap().unwrap();
    meta.stdout = "CACHED-STDOUT\n".into();
    meta.stderr = "CACHED-STDERR\n".into();
    std::fs::write(
        store.entry_dir(&key).join("meta.json"),
        serde_json::to_vec(&meta).unwrap(),
    )
    .unwrap();
    let compiler_path = dir
        .path()
        .join(if family == "cc" { "clang" } else { "nvcc" });
    let shell = crate::compiler::resolve_program_on_path("sh").unwrap();
    std::fs::write(
        &compiler_path,
        format!(
            "#!{}\nprintf 'FALLBACK-STDOUT\\n'\nprintf 'FALLBACK-STDERR\\n' >&2\nexit 7\n",
            shell.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&compiler_path, std::fs::Permissions::from_mode(0o755)).unwrap();
    let output = dir.path().join(if mode == "failure" {
        "missing-parent/restored.o"
    } else {
        "restored.o"
    });
    let argv = s(&[
        compiler_path.to_str().unwrap(),
        "-c",
        if family == "cc" { "foo.c" } else { "foo.cu" },
        "-o",
        output.to_str().unwrap(),
    ]);
    let _daemon = RemoteCheckReplyDaemon::with_reply(
        config.socket_path(),
        serde_json::json!({"ok": true, "found": true, "prefetched": mode == "prefetch"}),
    );
    let start = Instant::now() - Duration::from_millis(1500);
    let exit = if family == "cc" {
        let compiler = CcCompiler::new();
        let parsed = compiler.parse(&argv).unwrap();
        cc_try_remote_hit(
            &config,
            &store,
            &compiler,
            &parsed,
            &FileHasher::new(),
            &key,
            "consumer",
            "/consumer",
            start,
            41,
            29,
        )
    } else {
        let parsed = NvccCompiler::with_extra_allowlist_flags(Vec::new())
            .parse(&argv)
            .unwrap();
        nvcc_try_remote_hit(
            &config,
            &store,
            &parsed,
            &key,
            "consumer",
            "/consumer",
            start,
            41,
            29,
        )
    }
    .unwrap();
    let events = events::read_events(&config.event_log_path()).unwrap();
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.root, "/consumer");
    assert_eq!(event.crate_name, "consumer");
    if mode == "failure" {
        assert_eq!(exit, Some(7));
        assert_eq!(event.result, EventResult::Passthrough);
        assert!(event.passthrough_reason.contains("restore failed"));
        assert!(!output.exists());
    } else {
        assert_eq!(exit, Some(0));
        assert_eq!(std::fs::read(output).unwrap(), b"object bytes");
        assert_eq!(
            event.result,
            if mode == "prefetch" {
                EventResult::PrefetchHit
            } else {
                EventResult::RemoteHit
            }
        );
        assert_eq!(event.cache_key, key);
        assert_eq!(event.size, 12);
        assert_eq!(event.compile_time_ms, 12);
        assert!(event.elapsed_ms >= 1500);
        assert_eq!(event.key_ms, 41);
        assert_eq!(event.lookup_ms, 29);
        assert_eq!(event.key_hash_hits, 0);
        assert_eq!(event.key_hash_misses, 0);
        assert_eq!(event.key_hash_bytes, 0);
        assert_eq!(event.store_ms, 0);
    }
}

#[cfg(unix)]
#[test]
fn cc_and_nvcc_complete_remote_hits_once_and_fall_back_without_false_hits() {
    let _lock = crate::test_support::process_state_test_lock();
    for family in ["cc", "nvcc"] {
        for mode in ["remote", "prefetch", "failure"] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "wrapper::tests::hit::remote_completion_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env("KACHE_TEST_HIT_FAMILY", family)
                .env("KACHE_TEST_HIT_MODE", mode)
                .env("KACHE_PROGRESS", "1")
                .env_remove("CARGO_MANIFEST_DIR")
                .output()
                .unwrap();
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                output.status.success(),
                "{family}/{mode}: {stdout}\n{stderr}"
            );
            let hit = usize::from(mode != "failure");
            assert_eq!(stdout.matches("CACHED-STDOUT\n").count(), hit, "{stdout}");
            assert_eq!(stderr.matches("CACHED-STDERR\n").count(), hit, "{stderr}");
            assert_eq!(stderr.matches("[kache] consumer:").count(), hit, "{stderr}");
            let fallback = usize::from(mode == "failure");
            assert_eq!(
                stdout.matches("FALLBACK-STDOUT\n").count(),
                fallback,
                "{stdout}"
            );
            assert_eq!(
                stderr.matches("FALLBACK-STDERR\n").count(),
                fallback,
                "{stderr}"
            );
        }
    }
}
