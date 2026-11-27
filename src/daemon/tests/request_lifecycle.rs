//! Status, liveness and hand-off requests stay bounded while compiler work
//! holds the daemon's handler slots or its shared store.

use super::*;

fn stats_request() -> StatsRequest {
    StatsRequest {
        include_entries: false,
        include_summaries: false,
        sort_by: None,
        event_hours: None,
        event_secs: None,
        client_epoch: 0,
        client_version: None,
    }
}

/// `kache stats` must answer while another holder keeps the shared store, as
/// a GC sweep or a wedged import would.
#[test]
fn stats_answer_while_the_shared_store_is_held() {
    let dir = tempfile::tempdir().unwrap();
    let daemon = Arc::new(Daemon::new(test_config(dir.path())));
    let (held, release) = (mpsc::channel(), mpsc::channel::<()>());
    let holder = {
        let daemon = Arc::clone(&daemon);
        let (held, release) = (held.0, release.1);
        std::thread::spawn(move || {
            let _guard = daemon.store_lock().unwrap().lock().unwrap();
            held.send(()).unwrap();
            let _ = release.recv_timeout(Duration::from_secs(30));
        })
    };
    held.1.recv().unwrap();

    assert!(
        daemon
            .with_store_or_private(Duration::ZERO, |store| Ok(store.is_read_only()))
            .unwrap(),
        "status must not open another writer while the shared store is held"
    );
    let started = Instant::now();
    let response = daemon.handle_stats(&stats_request());
    let elapsed = started.elapsed();
    release.0.send(()).unwrap();
    holder.join().unwrap();

    assert!(response.ok, "{response:?}");
    assert!(response.stats.is_some());
    assert!(
        elapsed >= STATS_STORE_WAIT,
        "stats gave the shared store up after {elapsed:?}"
    );
    assert!(
        elapsed < STATS_READ_TIMEOUT,
        "stats waited {elapsed:?} behind the store holder"
    );
}

/// A free shared store is used directly: no second connection is opened.
#[test]
fn a_free_shared_store_serves_status_reads_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let daemon = Daemon::new(test_config(dir.path()));
    let started = Instant::now();
    let shared = daemon
        .with_store_or_private(Duration::from_secs(30), |store| {
            Ok(std::ptr::from_ref(store) as usize)
        })
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(5));
    let held = daemon.store_lock().unwrap().lock().unwrap();
    assert_eq!(shared, std::ptr::from_ref(&*held) as usize);
    drop(held);
}

#[test]
fn a_poisoned_shared_store_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let daemon = Arc::new(Daemon::new(test_config(dir.path())));
    let poisoner = Arc::clone(&daemon);
    let _ = std::thread::spawn(move || {
        let _guard = poisoner.store_lock().unwrap().lock().unwrap();
        panic!("poison the shared store");
    })
    .join();
    let error = daemon
        .with_store_or_private(Duration::from_millis(50), |_| Ok(()))
        .unwrap_err();
    assert!(error.to_string().contains("poisoned"), "{error:#}");
}

#[test]
fn only_status_and_lifecycle_requests_skip_the_handler_limit() {
    for request in [
        Request::Health,
        Request::Shutdown,
        Request::GcHint,
        Request::Stats(stats_request()),
        Request::ExecutionStatus {
            client_pid: 1,
            client_version: VERSION.into(),
            trusted_pipeline: false,
        },
    ] {
        assert!(bypasses_handler_limit(&request), "{request:?}");
    }
    assert!(!bypasses_handler_limit(&Request::RemoteCheck(
        RemoteCheckRequest {
            key: test_cache_key("limited"),
            entry_dir: String::new(),
            crate_name: "serde".into(),
            deadline_ms: None,
            shard_dir: None,
        }
    )));
}

/// Health answers on a connection while every handler slot is taken by
/// blocked compiler pipelines; any other request still waits for a slot.
#[tokio::test]
async fn health_answers_while_every_handler_slot_is_taken() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let daemon = Arc::new(Daemon::new(config.clone()));
    let socket_path = fresh_endpoint(&config.socket_path());
    std::fs::create_dir_all(socket_path.parent().unwrap()).unwrap();
    let listener = bind_listener(&socket_path);
    let exhausted = Arc::new(tokio::sync::Semaphore::new(0));
    let lifecycle = Arc::new(Lifecycle::default());

    let server = {
        let daemon = Arc::clone(&daemon);
        let lifecycle = Arc::clone(&lifecycle);
        let limiter = Arc::clone(&exhausted);
        tokio::spawn(async move {
            let mut handlers = tokio::task::JoinSet::new();
            for _ in 0..2 {
                let stream = listener.accept().await.expect("accept");
                let daemon = Arc::clone(&daemon);
                let lifecycle = Arc::clone(&lifecycle);
                let limiter = Arc::clone(&limiter);
                handlers.spawn(async move {
                    handle_connection_started_at(
                        stream,
                        &daemon,
                        &lifecycle,
                        Some(limiter),
                        Instant::now(),
                    )
                    .await
                });
            }
            handlers.join_all().await
        })
    };
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        client_roundtrip(&socket_path, &Request::Health),
    )
    .await
    .expect("health must not wait for a handler slot");
    assert!(response.ok, "{response:?}");

    let gated_socket = socket_path.clone();
    let gated = tokio::spawn(async move {
        client_roundtrip(
            &gated_socket,
            &Request::RemoteCheck(RemoteCheckRequest {
                key: test_cache_key("gated"),
                entry_dir: String::new(),
                crate_name: "serde".into(),
                deadline_ms: None,
                shard_dir: None,
            }),
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!gated.is_finished(), "a remote check must wait for a slot");
    exhausted.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), gated)
        .await
        .expect("a released slot serves the waiting request")
        .unwrap();
    for result in server.await.unwrap() {
        result.unwrap();
    }
}
