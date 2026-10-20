use kache_protocol::{
    CompileFinishedRequest, CompileStartedRequest, DaemonHealth, EffectiveConfig, HashFilesRequest,
    PrefetchRequest, RemoteCheckRequest, TransferEvent, UploadJob,
};
use serde_json::{from_value, json, to_value};

#[test]
fn legacy_compile_notifications_keep_their_defaults() {
    let started: CompileStartedRequest = from_value(json!({
        "crate_name": "example", "pid": 42, "started_at_ms": 123
    }))
    .unwrap();
    assert_eq!(
        to_value(started).unwrap(),
        json!({
            "crate_name": "example", "root": "", "pid": 42,
            "started_at_ms": 123, "typical_ms": null, "client_epoch": 0
        })
    );
    let finished: CompileFinishedRequest = from_value(json!({"pid": 42})).unwrap();
    assert_eq!(finished.started_at_ms, 0);
    let current = CompileFinishedRequest {
        pid: 42,
        started_at_ms: 123,
    };
    assert_eq!(
        to_value(current).unwrap(),
        json!({"pid": 42, "started_at_ms": 123})
    );
}

#[test]
fn old_clients_do_not_request_an_upgrade_or_select_another_store() {
    let upload: UploadJob = from_value(json!({"key": "key", "entry_dir": "/cache/key"})).unwrap();
    assert_eq!(upload.client_version, None);
    assert_eq!(upload.client_epoch, 0);
    assert_eq!(upload.crate_name, "");
    let check: RemoteCheckRequest = from_value(json!({
        "key": "key", "entry_dir": "/cache/key", "future_option": true
    }))
    .unwrap();
    assert_eq!(
        to_value(check).unwrap(),
        json!({"key": "key", "entry_dir": "/cache/key", "crate_name": "", "deadline_ms": null})
    );
}

#[test]
fn hash_requests_preserve_fingerprints_and_accept_pre_inode_clients() {
    let request: HashFilesRequest = from_value(json!({"files": [
        {"path": "/old", "size": 9, "mtime_ns": 10, "ctime_ns": 11},
        {"path": "/new", "size": 12, "mtime_ns": 13, "ctime_ns": 14, "inode": 15}
    ]}))
    .unwrap();
    assert_eq!(
        to_value(request).unwrap(),
        json!({"files": [
            {"path": "/old", "size": 9, "mtime_ns": 10, "ctime_ns": 11, "inode": 0},
            {"path": "/new", "size": 12, "mtime_ns": 13, "ctime_ns": 14, "inode": 15}
        ]})
    );
}

#[test]
fn clients_cannot_inject_daemon_prefetch_attribution() {
    let request: PrefetchRequest = from_value(json!({
        "keys": [], "origin": {"session_id": "forged"},
        "candidate_sources": {"key": "manifest"}
    }))
    .unwrap();
    assert!(!request.warm_all);
    assert!(request.origin.is_none());
    assert!(request.candidate_sources.is_empty());
    assert_eq!(
        to_value(request).unwrap(),
        json!({"keys": [], "warm_all": false})
    );
}

#[test]
fn legacy_health_and_configuration_remain_distinguishable() {
    let health: DaemonHealth = from_value(json!({})).unwrap();
    assert_eq!(health.remote_readonly, None);
    assert_eq!(health.version, "");
    let config: EffectiveConfig = from_value(json!({
        "max_size": 1024, "cache_dir": "/cache", "runtime_dir": "/run",
        "config_path": "/config", "prefetch_enabled": true,
        "remote_description": null, "local_only": true,
        "socket_path": "/run/daemon.sock", "started_at_ms": 123
    }))
    .unwrap();
    assert!(config.remote_key_listing);
    assert_eq!(config.remote_key_cache_refresh_secs, 60);
    assert_eq!(config.remote_readonly, None);
    assert_eq!(config.config_fingerprint, None);
}

#[test]
fn old_transfer_records_keep_their_schema_and_direction() {
    let transfer: TransferEvent = from_value(json!({
        "direction": "upload", "crate_name": "example",
        "compressed_bytes": 42, "elapsed_ms": 7, "ok": true, "timestamp": 123
    }))
    .unwrap();
    assert_eq!(transfer.schema, 5);
    assert!(transfer.accounting.is_none());
    assert!(transfer.prefetch.is_none());
}

#[test]
fn complete_envelopes_keep_legacy_gc_and_prediction_shapes() {
    use kache_protocol::{GcRequestMode, Request, SharedPrediction};
    let request: Request = from_value(json!({"gc": {"max_age_hours": 12}})).unwrap();
    let Request::Gc(gc) = &request else {
        panic!("expected legacy gc")
    };
    assert_eq!(gc.mode, GcRequestMode::Legacy);
    assert_eq!(gc.effective_max_age_hours, None);
    assert!(!request.is_build_activity());
    assert_eq!(
        to_value(request).unwrap(),
        json!({
            "gc": {"max_age_hours": 12, "mode": "legacy"}
        })
    );
    let wire = json!({"prediction_publish": {"identity": "unit", "row": {"plain": {
        "schema": 1, "sources": [{"Registry": "/pkg/lib.rs"}],
        "env_deps": [["UNSET", null]], "tree": null
    }}}});
    let request: Request = from_value(wire.clone()).unwrap();
    let Request::PredictionPublish(ref prediction) = request else {
        panic!("expected prediction")
    };
    assert!(matches!(prediction.row, SharedPrediction::Plain(_)));
    assert_eq!(to_value(request).unwrap(), wire);
}

#[test]
fn complete_responses_accept_older_daemons_and_omit_absent_fields() {
    use kache_protocol::Response;
    let response: Response = from_value(json!({"ok": true, "found": true})).unwrap();
    assert!(!response.skipped);
    assert!(response.gc.is_none());
    assert!(response.health.is_none());
    assert_eq!(
        to_value(response).unwrap(),
        json!({"ok": true, "found": true})
    );
    let wire = json!({"ok": true, "prediction": {"portable": {
        "schema": 1, "sources": [{"OutDir": "/generated.rs"}],
        "env_deps": [], "tree": "digest"
    }}});
    assert_eq!(
        to_value(from_value::<Response>(wire.clone()).unwrap()).unwrap(),
        wire
    );
}

#[test]
fn publication_memos_keep_flattened_fingerprints_and_old_defaults() {
    use kache_protocol::CcPreprocessMemoInput;
    let input: CcPreprocessMemoInput = from_value(json!({
        "name": "header.h", "path": "/src/header.h", "size": 5,
        "mtime_ns": 7, "ctime_ns": 9, "inode": 11, "content": "digest"
    }))
    .unwrap();
    assert_eq!(input.observed_ns, 0);
    assert_eq!(input.mapped, "");
    assert_eq!(
        to_value(input).unwrap(),
        json!({
            "name": "header.h", "path": "/src/header.h", "size": 5,
            "mtime_ns": 7, "ctime_ns": 9, "inode": 11, "content": "digest",
            "mapped": "", "observed_ns": 0
        })
    );
}
