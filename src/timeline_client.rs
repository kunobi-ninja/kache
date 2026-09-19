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

    let mut request = client
        .post(build_timelines_url(&config.endpoint))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header(reqwest::header::CONTENT_ENCODING, "zstd")
        .body(encode_record(record)?);
    if let Some(token) = config.token.as_deref() {
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

        let error = push_timeline(&planner_config(&format!("http://{addr}"), 750), &record())
            .await
            .unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("413"), "{message}");
        assert!(
            message.contains("--timeline-max-decoded-bytes"),
            "{message}"
        );
    }
}
