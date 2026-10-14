//! Send build timeline records to kache-service.
//!
//! Records are advisory: a failure here costs a record, never a build. The
//! caller is a CI post-step, so every error is reported and swallowed there.

use std::time::Duration;

use anyhow::{Context, Result};
use kache_core::timeline::BuildTimeline;

use crate::config::PlannerConfig;

const BUILD_TIMELINES_PATH: &str = "/v1/build-timelines";
/// Records are far larger than a plan request, so the planner's sub-second
/// timeout does not fit. This is the floor for an upload.
pub(crate) const MIN_PUSH_TIMEOUT: Duration = Duration::from_secs(30);
/// zstd level. 3 is the default trade: a 1.2 MB record becomes about 126 KB
/// in a few milliseconds.
const COMPRESSION_LEVEL: i32 = 3;

/// What the service did with a record.
#[derive(Debug, Clone, serde::Deserialize, PartialEq, Eq)]
pub(crate) struct PushResponse {
    pub stored: String,
}

/// Where a submission goes, given the configured planner endpoint.
pub(crate) fn build_timelines_url(endpoint: &str) -> String {
    let trimmed = endpoint.trim_end_matches('/');
    if trimmed.ends_with(BUILD_TIMELINES_PATH) {
        return trimmed.to_string();
    }
    let base = trimmed
        .strip_suffix("/v1/prefetch-plan")
        .or_else(|| trimmed.strip_suffix("/v2/prefetch-plan"))
        .unwrap_or(trimmed);
    format!("{base}{BUILD_TIMELINES_PATH}")
}

/// An upload waits far longer than a plan request does.
pub(crate) fn push_timeout(config: &PlannerConfig) -> Duration {
    Duration::from_millis(config.timeout_ms).max(MIN_PUSH_TIMEOUT)
}

/// The body as sent: JSON, zstd-compressed.
pub(crate) fn encode_record(record: &BuildTimeline) -> Result<Vec<u8>> {
    let json = serde_json::to_vec(record).context("serializing the build timeline")?;
    zstd::encode_all(json.as_slice(), COMPRESSION_LEVEL).context("compressing the build timeline")
}

pub(crate) async fn push_timeline(
    config: &PlannerConfig,
    record: &BuildTimeline,
) -> Result<PushResponse> {
    crate::planner_client::ensure_crypto_provider();
    let client = reqwest::Client::builder()
        .timeout(push_timeout(config))
        .build()
        .context("building the timeline client")?;

    let deadline = tokio::time::Instant::now() + push_timeout(config);
    let bearer = crate::planner_auth::bearer(config, deadline).await;
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        anyhow::bail!("timeline timeout spent before the request could be sent");
    }
    let mut request = client
        .post(build_timelines_url(&config.endpoint))
        .timeout(remaining)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header(reqwest::header::CONTENT_ENCODING, "zstd")
        .body(encode_record(record)?);
    if let Some(token) = bearer {
        request = request.bearer_auth(token);
    }

    let response = request.send().await.context("sending the build timeline")?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!(
            "service answered {status}: {}",
            service_error(&body, &status)
        );
    }
    serde_json::from_str::<PushResponse>(&body)
        .with_context(|| format!("decoding the service answer: {body}"))
}

/// The service explains a refusal in an `error` field, and a size refusal
/// names the setting to raise. Show that rather than the raw JSON.
fn service_error(body: &str, status: &reqwest::StatusCode) -> String {
    #[derive(serde::Deserialize)]
    struct ErrorResponse {
        error: String,
    }
    serde_json::from_str::<ErrorResponse>(body)
        .map(|parsed| parsed.error)
        .unwrap_or_else(|_| {
            let trimmed = body.trim();
            if trimmed.is_empty() {
                status.canonical_reason().unwrap_or("no detail").to_string()
            } else {
                trimmed.to_string()
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use kache_core::timeline::{BUILD_TIMELINE_SCHEMA, BuildTimeline};

    fn planner_config(endpoint: &str, timeout_ms: u64) -> PlannerConfig {
        PlannerConfig {
            endpoint: endpoint.to_string(),
            timeout_ms,
            token: None,
        }
    }

    fn record() -> BuildTimeline {
        BuildTimeline {
            schema: BUILD_TIMELINE_SCHEMA,
            client_record_id: "r1".to_string(),
            session_id: "s1".to_string(),
            ..BuildTimeline::default()
        }
    }

    #[test]
    fn the_url_is_derived_from_the_planner_endpoint() {
        for endpoint in [
            "https://kache.example",
            "https://kache.example/",
            "https://kache.example/v1/prefetch-plan",
            "https://kache.example/v2/prefetch-plan",
            "https://kache.example/v1/build-timelines",
        ] {
            assert_eq!(
                build_timelines_url(endpoint),
                "https://kache.example/v1/build-timelines",
                "{endpoint}"
            );
        }
    }

    #[test]
    fn a_path_prefix_in_the_endpoint_is_kept() {
        assert_eq!(
            build_timelines_url("https://example.test/kache/v2/prefetch-plan"),
            "https://example.test/kache/v1/build-timelines"
        );
    }

    #[test]
    fn an_upload_waits_longer_than_a_plan_request() {
        assert_eq!(push_timeout(&planner_config("x", 750)), MIN_PUSH_TIMEOUT);
        assert_eq!(
            push_timeout(&planner_config("x", 90_000)),
            Duration::from_secs(90)
        );
    }

    #[test]
    fn the_body_is_zstd_of_the_record_json() {
        let record = record();
        let body = encode_record(&record).unwrap();
        let decoded = zstd::decode_all(body.as_slice()).unwrap();
        assert_eq!(
            serde_json::from_slice::<BuildTimeline>(&decoded).unwrap(),
            record
        );
        assert_ne!(body, decoded, "the body must be compressed, not raw JSON");
    }

    #[test]
    fn a_refusal_is_reported_with_the_services_own_explanation() {
        let status = reqwest::StatusCode::PAYLOAD_TOO_LARGE;
        assert_eq!(
            service_error(
                r#"{"error":"decompressed record is over this service's 10 byte limit; raise --timeline-max-decoded-bytes (KACHE_TIMELINE_MAX_DECODED_BYTES)"}"#,
                &status
            ),
            "decompressed record is over this service's 10 byte limit; raise --timeline-max-decoded-bytes (KACHE_TIMELINE_MAX_DECODED_BYTES)"
        );
        assert_eq!(service_error("plain text", &status), "plain text");
        assert_eq!(service_error("  ", &status), "Payload Too Large");
    }

    /// The variables a child needs to reach the network after `env_clear`:
    /// Winsock fails without `SystemRoot`, and the Nix sandbox's only trust
    /// store is `SSL_CERT_FILE`.
    fn platform_env() -> impl Iterator<Item = (&'static str, std::ffi::OsString)> {
        ["SystemRoot", "windir", "SSL_CERT_FILE", "SSL_CERT_DIR"]
            .into_iter()
            .filter_map(|key| std::env::var_os(key).map(|value| (key, value)))
    }

    async fn run_deadline_fixture(mode: &str) {
        let root = tempfile::tempdir().unwrap();
        let output = tokio::time::timeout(
            Duration::from_secs(10),
            tokio::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "timeline_client::tests::timeline_deadline_child_fixture",
                    "--ignored",
                    "--nocapture",
                ])
                .env_clear()
                .envs(platform_env())
                .env("HOME", root.path())
                .env("USERPROFILE", root.path())
                .env("XDG_CONFIG_HOME", root.path())
                .env("KUNOBI_CONFIG_DIR", root.path())
                .env("TIMELINE_DEADLINE_CASE", mode)
                .kill_on_drop(true)
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            output.status.success(),
            "{mode}: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[tokio::test]
    async fn exhausted_auth_does_not_dispatch_a_timeline() {
        run_deadline_fixture("exhausted").await;
    }

    #[tokio::test]
    async fn auth_and_timeline_share_one_timeout() {
        run_deadline_fixture("delayed").await;
    }

    #[tokio::test(start_paused = true)]
    #[ignore = "isolated subprocess fixture for timeline authentication deadlines"]
    async fn timeline_deadline_child_fixture() {
        use axum::{
            Json, Router,
            routing::{get, post},
        };
        use std::sync::Arc;
        use tokio::sync::Notify;

        // Keep a runnable task while real loopback IO progresses: paused
        // Tokio time must advance only at the explicit budget boundaries.
        let awake = tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        });
        let token_started = Arc::new(Notify::new());
        let release_token = Arc::new(Notify::new());
        let post_started = Arc::new(Notify::new());
        let started = token_started.clone();
        let release = release_token.clone();
        let posted = post_started.clone();
        let app = Router::new()
            .route("/token", get(move || {
                let started = started.clone();
                let release = release.clone();
                async move {
                    started.notify_one();
                    release.notified().await;
                    Json(serde_json::json!({"value": "eyJhbGciOiJSUzI1NiJ9.eyJleHAiOjQxMDI0NDQ4MDB9.fixture-signature"}))
                }
            }))
            .route("/v1/build-timelines", post(move || {
                let posted = posted.clone();
                async move {
                    posted.notify_one();
                    std::future::pending::<()>().await;
                    Json(serde_json::json!({"stored": "inserted"}))
                }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        // Only this exact ignored fixture runs in the child process. Neither
        // its synthetic credentials nor its paused clock are shared with tests.
        unsafe {
            std::env::set_var("ACTIONS_ID_TOKEN_REQUEST_URL", format!("{base}/token"));
            std::env::set_var("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "fixture-runtime");
        }
        let config = planner_config(&base, 750);
        let value = record();
        let push = push_timeline(&config, &value);
        tokio::pin!(push);
        tokio::select! {
            answer = &mut push => panic!("upload ended before auth started: {answer:?}"),
            () = post_started.notified() => panic!("the timeline was posted before auth started"),
            () = token_started.notified() => {}
        }
        let mode = std::env::var("TIMELINE_DEADLINE_CASE").unwrap();
        if mode == "delayed" {
            tokio::time::advance(Duration::from_secs(8)).await;
            release_token.notify_one();
            tokio::select! {
                answer = &mut push => panic!("upload ended before POST: {answer:?}"),
                () = post_started.notified() => {}
            }
            tokio::time::advance(Duration::from_secs(23)).await;
        } else {
            assert_eq!(mode, "exhausted");
            tokio::time::advance(Duration::from_secs(31)).await;
        }
        let error = match futures::poll!(push.as_mut()) {
            std::task::Poll::Ready(Err(error)) => error,
            answer => panic!("auth and upload must end within one 30-second budget: {answer:?}"),
        };
        if mode == "exhausted" {
            assert_eq!(
                error.to_string(),
                "timeline timeout spent before the request could be sent"
            );
            let posted = post_started.notified();
            tokio::pin!(posted);
            assert!(futures::poll!(posted.as_mut()).is_pending());
        } else {
            assert!(
                error.chain().any(|cause| cause
                    .downcast_ref::<reqwest::Error>()
                    .is_some_and(reqwest::Error::is_timeout)),
                "{error:#}"
            );
        }
        server.abort();
        awake.abort();
    }

    #[tokio::test]
    async fn a_record_is_posted_compressed_with_the_bearer_token() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = vec![0_u8; 64 * 1024];
            let read = socket.read(&mut buffer).await.unwrap();
            let request = String::from_utf8_lossy(&buffer[..read]).to_string();
            let response = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 22\r\n\r\n{\"stored\":\"inserted\"}\n";
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.flush().await.unwrap();
            request
        });

        let mut config = planner_config(&format!("http://{addr}"), 750);
        config.token = Some("secret".to_string());
        let answer = push_timeline(&config, &record()).await.unwrap();
        assert_eq!(answer.stored, "inserted");

        let request = server.await.unwrap();
        assert!(
            request.starts_with("POST /v1/build-timelines HTTP/1.1"),
            "{request}"
        );
        assert!(
            request.to_lowercase().contains("content-encoding: zstd"),
            "{request}"
        );
        assert!(
            request.contains("authorization: Bearer secret")
                || request.contains("Authorization: Bearer secret"),
            "{request}"
        );
    }

    #[tokio::test]
    async fn a_refused_record_fails_with_the_services_message() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = vec![0_u8; 64 * 1024];
            let _ = socket.read(&mut buffer).await.unwrap();
            let body = "{\"error\":\"raise --timeline-max-decoded-bytes\"}";
            let response = format!(
                "HTTP/1.1 413 Payload Too Large\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.flush().await.unwrap();
        });

        // This fixture isolates the service's refusal, without discovery traffic.
        let mut config = planner_config(&format!("http://{addr}"), 750);
        config.token = Some("fixture-explicit".to_string());
        let error = push_timeline(&config, &record()).await.unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("413"), "{message}");
        assert!(
            message.contains("--timeline-max-decoded-bytes"),
            "{message}"
        );
    }
}
