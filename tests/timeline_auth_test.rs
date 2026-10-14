//! Automatic planner credentials must also authenticate the real timeline post-step.

#[allow(dead_code)]
mod common;

use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine as _;
use serde_json::{Value, json};

#[derive(Clone)]
struct Service {
    base: String,
    token: String,
    expected: Option<String>,
    calls: Arc<Mutex<Vec<Value>>>,
}

/// The variables a child needs to reach the network after `env_clear`:
/// Winsock fails without `SystemRoot`, and the Nix sandbox's only trust store
/// is `SSL_CERT_FILE`.
fn platform_env() -> impl Iterator<Item = (&'static str, std::ffi::OsString)> {
    ["SystemRoot", "windir", "SSL_CERT_FILE", "SSL_CERT_DIR"]
        .into_iter()
        .filter_map(|key| std::env::var_os(key).map(|value| (key, value)))
}

async fn actions_token(
    State(service): State<Service>,
    Query(query): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fixture-runtime");
    assert_eq!(query.get("api-version").map(String::as_str), Some("2.0"));
    assert_eq!(query.get("audience"), Some(&service.base));
    service.calls.lock().unwrap().push(json!({"method": "GET"}));
    Json(json!({"value": service.token}))
}

async fn timeline(
    State(service): State<Service>,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, Json<Value>) {
    assert_eq!(headers["content-encoding"], "zstd");
    assert_eq!(headers["content-type"], "application/json");
    let record: Value = serde_json::from_slice(&zstd::decode_all(body.as_ref()).unwrap()).unwrap();
    assert_eq!(record["session_id"], "fixture-session");
    let bearer = headers
        .get("authorization")
        .map(|h| h.to_str().unwrap().to_owned());
    service
        .calls
        .lock()
        .unwrap()
        .push(json!({"method": "POST", "authorization": bearer}));
    if service.expected.is_some() && bearer == service.expected {
        (StatusCode::OK, Json(json!({"stored": "inserted"})))
    } else {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "fixture requires authentication"})),
        )
    }
}

#[tokio::test]
async fn telemetry_push_uses_automatic_auth_and_preserves_explicit_and_missing_controls() {
    // Controls run before the regression, so a baseline red proves that its
    // fixture can already upload and can distinguish missing credentials.
    for mode in ["explicit", "missing", "actions"] {
        let root = tempfile::tempdir().unwrap();
        let cache = root.path().join("cache");
        let home = root.path().join("home");
        std::fs::create_dir_all(&cache).unwrap();
        std::fs::create_dir_all(&home).unwrap();
        let config = root.path().join("config.toml");
        std::fs::write(&config, "[cache]\nauto_gc = false\n").unwrap();
        let event = json!({"ts":"2026-10-11T01:00:00Z", "crate_name":"fixture",
            "result":"miss", "elapsed_ms":10, "size":1, "cache_key":"fixture-key",
            "schema":26, "session_id":"fixture-session", "root":root.path()});
        std::fs::write(cache.join("events.jsonl"), format!("{event}\n")).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let token = format!(
            "eyJhbGciOiJSUzI1NiJ9.{}.fixture-signature",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(r#"{"exp":4102444800}"#)
        );
        let expected = match mode {
            "explicit" => Some("Bearer fixture-explicit".to_owned()),
            "actions" => Some(format!("Bearer {token}")),
            _ => None,
        };
        let calls = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .route("/token", get(actions_token))
            .route("/v1/build-timelines", post(timeline))
            .with_state(Service {
                base: base.clone(),
                token,
                expected: expected.clone(),
                calls: calls.clone(),
            });
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut command =
            common::hermetic_command(env!("CARGO_BIN_EXE_kache"), &cache, Some(&config));
        command.env_clear();
        command
            .envs(platform_env())
            .env("HOME", &home)
            .env("XDG_CONFIG_HOME", home.join("config"))
            .env("KUNOBI_CONFIG_DIR", home.join("kunobi"))
            .env("KACHE_HOST_CONFIG", "")
            .env("KACHE_CONFIG", &config)
            .env("KACHE_CACHE_DIR", &cache)
            .env("KACHE_RUNTIME_DIR", &cache)
            .env("KACHE_PLANNER_ENDPOINT", format!("{base}/v1/prefetch-plan"))
            .args(["telemetry", "push", "--all"]);
        if mode != "missing" {
            command
                .env(
                    "ACTIONS_ID_TOKEN_REQUEST_URL",
                    format!("{base}/token?api-version=2.0"),
                )
                .env("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "fixture-runtime");
        }
        if mode == "explicit" {
            command.env("KACHE_PLANNER_TOKEN", "fixture-explicit");
        }
        let output = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            tokio::process::Command::from(command)
                .kill_on_drop(true)
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        server.abort();
        let calls = calls.lock().unwrap();
        let posts: Vec<_> = calls.iter().filter(|c| c["method"] == "POST").collect();
        assert!(
            output.status.success(),
            "{mode}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            posts.len(),
            1,
            "{mode}: {calls:?}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            posts[0]["authorization"],
            json!(expected),
            "available Actions credential must authenticate the production timeline upload"
        );
        let gets = calls.iter().filter(|c| c["method"] == "GET").count();
        assert_eq!(gets, usize::from(mode == "actions"), "{mode}: {calls:?}");
        let stdout = String::from_utf8_lossy(&output.stdout);
        if mode == "missing" {
            assert!(!stdout.contains("sent session"), "{stdout}");
            assert!(String::from_utf8_lossy(&output.stderr).contains("401 Unauthorized"));
        } else {
            assert!(stdout.contains("sent session fixture-session"), "{stdout}");
        }
    }
}
