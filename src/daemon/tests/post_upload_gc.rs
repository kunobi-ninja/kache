use super::*;

#[test]
fn a_post_upload_gc_sweep_allows_health_requests_while_a_writer_is_busy() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path());
    config.max_size = 100;
    config.remote = Some(test_remote_config());
    let key = test_cache_key("gc-responsiveness-upload");
    seed_store_entry(&config, &key, "serde", dir.path());
    let store = Store::open(&config).unwrap();
    put_upload_evict_entry(&store, dir.path(), "gc_victim", 200, false);
    let daemon = daemon_on(config.clone(), &test_remote_backend());
    daemon.with_store(|_| Ok(())).unwrap();
    let gc_lock_path = config.store_dir().join("gc.lock");
    assert!(!gc_lock_path.exists(), "the fixture has not run GC");
    let writer = rusqlite::Connection::open(config.index_db_path()).unwrap();
    writer.execute_batch("BEGIN IMMEDIATE").unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let worker_daemon = daemon.clone();
    let worker = runtime.spawn(async move {
        worker_daemon
            .do_upload(&UploadJob {
                entry_dir: config.store_dir().join(&key).to_string_lossy().into_owned(),
                key,
                crate_name: "serde".into(),
                client_epoch: 0,
                client_version: None,
            })
            .await
    });
    // Observe gc.lock by reading it, never by try-locking it: winning the lock
    // would make the sweep skip. On Unix the lock is advisory, so the read
    // returns the holder's PID. On Windows it is mandatory (LockFileEx), so a
    // read through another handle fails with ERROR_LOCK_VIOLATION (33) while
    // GC holds it (#348).
    let sweep_holds_gc_lock = || match fs::read_to_string(&gc_lock_path) {
        Ok(pid) => !pid.trim().is_empty(),
        Err(e) => cfg!(windows) && e.raw_os_error() == Some(33),
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while !sweep_holds_gc_lock() {
        if Instant::now() >= deadline {
            writer.execute_batch("ROLLBACK").unwrap();
            panic!("the upload must enter its post-upload GC sweep");
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let (health_tx, health_rx) = mpsc::channel();
    let health_daemon = daemon.clone();
    runtime.spawn(async move {
        let _ = health_tx.send(health_daemon.handle_health());
    });
    let health = health_rx.recv_timeout(Duration::from_secs(10));
    // The sweep took gc.lock above and needs the held writer to evict, so a
    // free lock means the sweep returned before health ran.
    let sweep_still_running = store.try_gc_lock().unwrap().is_none();
    // Release the real writer before any assertion, including on failure.
    writer.execute_batch("ROLLBACK").unwrap();
    let health = health.expect("a health request must complete while the writer is held");
    assert!(health.ok && health.health.is_some());
    assert!(
        sweep_still_running,
        "health must answer while the post-upload sweep still holds gc.lock"
    );
    let response = runtime.block_on(worker).unwrap();
    assert!(response.ok, "upload must succeed: {response:?}");
    let deadline = Instant::now() + Duration::from_secs(5);
    while crate::report::read_gc_stats(dir.path()).is_none() {
        assert!(
            Instant::now() < deadline,
            "the sweep must record its result"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        !store.contains("gc_victim"),
        "the sweep must evict its victim"
    );
}
