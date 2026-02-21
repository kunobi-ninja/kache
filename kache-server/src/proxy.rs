use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};

use crate::db::Database;

// ── Shared state ────────────────────────────────────────────────

#[derive(Clone)]
pub struct AppState {
    db: Arc<Database>,
    /// reqwest client for forwarding to upstream S3
    upstream: reqwest::Client,
    /// e.g. "https://s3.zondax.ch"
    upstream_url: String,
    /// e.g. "kache"
    bucket: String,
}

impl AppState {
    pub fn new(db: Database, upstream_url: &str, bucket: &str) -> Self {
        Self {
            db: Arc::new(db),
            upstream: reqwest::Client::new(),
            upstream_url: upstream_url.trim_end_matches('/').to_string(),
            bucket: bucket.to_string(),
        }
    }
}

// ── Router ──────────────────────────────────────────────────────

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        // Virtual objects — _kache/ namespace
        .route(
            "/{bucket}/_kache/session",
            axum::routing::put(virtual_create_session),
        )
        .route(
            "/{bucket}/_kache/prefetch/{session_id}",
            get(virtual_prefetch),
        )
        .route("/{bucket}/_kache/miss/{cache_key}", get(virtual_miss))
        .route("/{bucket}/_kache/stats", get(virtual_stats))
        // Also match with prefix: /{bucket}/{prefix}/_kache/...
        .route(
            "/{bucket}/{prefix}/_kache/session",
            axum::routing::put(virtual_create_session),
        )
        .route(
            "/{bucket}/{prefix}/_kache/prefetch/{session_id}",
            get(virtual_prefetch),
        )
        .route(
            "/{bucket}/{prefix}/_kache/miss/{cache_key}",
            get(virtual_miss),
        )
        .route("/{bucket}/{prefix}/_kache/stats", get(virtual_stats))
        // Catch-all: forward everything else to upstream S3
        .fallback(proxy_to_upstream)
        .with_state(state)
}

// ── Health ──────────────────────────────────────────────────────

async fn health() -> &'static str {
    "ok"
}

// ── S3 proxy (catch-all) ────────────────────────────────────────

async fn proxy_to_upstream(State(state): State<AppState>, req: Request) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let query = req
        .uri()
        .query()
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    let headers = req.headers().clone();

    // Extract kache metadata headers before forwarding
    let session_id = header_str(&headers, "x-kache-session");
    let deps = header_csv(&headers, "x-kache-deps");
    let target = header_str(&headers, "x-kache-target");

    // Build upstream URL: endpoint + original path + query
    let upstream_url = format!("{}{path}{query}", state.upstream_url);

    // Forward the request
    let mut upstream_req = state.upstream.request(method.clone(), &upstream_url);

    // Forward relevant headers (content-type, content-length, range, auth, etc.)
    for (name, value) in &headers {
        let n = name.as_str();
        // Skip hop-by-hop and host headers
        if matches!(n, "host" | "connection" | "transfer-encoding") {
            continue;
        }
        upstream_req = upstream_req.header(name, value);
    }

    // Forward body (collect into memory — kache artifacts are typically a few MB)
    let body_bytes = match axum::body::to_bytes(req.into_body(), 512 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("failed to read body: {e}")).into_response();
        }
    };
    if !body_bytes.is_empty() {
        upstream_req = upstream_req.body(body_bytes);
    }

    let upstream_resp = match upstream_req.send().await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("upstream request failed: {e}");
            return (StatusCode::BAD_GATEWAY, format!("upstream error: {e}")).into_response();
        }
    };

    let status = upstream_resp.status();

    // Record metadata (best-effort, don't block the response)
    record_metadata(
        &state,
        method.as_str(),
        &path,
        status,
        session_id.as_deref(),
        &deps,
        target.as_deref(),
    );

    // Stream the upstream response back to the client
    let mut response_builder = Response::builder().status(status);
    for (name, value) in upstream_resp.headers() {
        response_builder = response_builder.header(name, value);
    }
    let body = Body::from_stream(upstream_resp.bytes_stream());
    response_builder.body(body).unwrap_or_else(|e| {
        tracing::error!("failed to build response: {e}");
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
    })
}

/// Record metadata from a proxied S3 request (best-effort).
fn record_metadata(
    state: &AppState,
    method: &str,
    path: &str,
    status: StatusCode,
    session_id: Option<&str>,
    deps: &[String],
    target: Option<&str>,
) {
    let Some((crate_name, cache_key)) = parse_artifact_key(path) else {
        return;
    };

    match method {
        "PUT" => {
            if let Err(e) = state.db.record_put(session_id, cache_key, crate_name, 0) {
                tracing::warn!("failed to record PUT: {e}");
            }

            if !deps.is_empty() {
                let target_triple = target.unwrap_or("unknown");
                if let Err(e) =
                    state
                        .db
                        .upsert_dep_edges(&state.bucket, crate_name, deps, target_triple)
                {
                    tracing::warn!("failed to record dep edges: {e}");
                }
            }

            tracing::info!(crate_name, cache_key, "PUT recorded");
        }
        "GET" => {
            if status.is_success() {
                if let Err(e) = state.db.record_hit(session_id, cache_key, crate_name) {
                    tracing::warn!("failed to record hit: {e}");
                }
                tracing::debug!(crate_name, cache_key, "cache HIT");
            } else {
                if let Err(e) = state.db.record_miss(session_id, cache_key, crate_name) {
                    tracing::warn!("failed to record miss: {e}");
                }
                tracing::debug!(crate_name, cache_key, "cache MISS");
            }
        }
        _ => {}
    }
}

// ── Virtual object handlers ─────────────────────────────────────

async fn virtual_create_session(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> Response {
    let session_req: kache_protocol::CreateSessionRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("invalid JSON: {e}")).into_response(),
    };

    let session_id = match state.db.create_session(&session_req) {
        Ok(id) => id,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to create session: {e}"),
            )
                .into_response();
        }
    };

    tracing::info!(
        session_id,
        repo = session_req.repo,
        branch = session_req.branch,
        "session created"
    );

    let resp = kache_protocol::CreateSessionResponse {
        session_id,
        prefetch: vec![],
    };
    Json(resp).into_response()
}

async fn virtual_prefetch() -> Json<kache_protocol::PrefetchResponse> {
    // Phase 3: smart prefetch algorithm
    Json(kache_protocol::PrefetchResponse {
        recommendations: vec![],
        total_download_size: 0,
        estimated_time_saved_ms: 0,
    })
}

async fn virtual_miss() -> Json<kache_protocol::MissAnalysisResponse> {
    // Phase 4: miss diff engine
    Json(kache_protocol::MissAnalysisResponse {
        reason: "not yet implemented".into(),
        changed_fields: vec![],
        explanation: "miss analysis will be available in a future release".into(),
    })
}

async fn virtual_stats(State(state): State<AppState>) -> Json<kache_protocol::StatsResponse> {
    let resp =
        crate::stats::get_stats(&state.db).unwrap_or_else(|_| kache_protocol::StatsResponse {
            total_builds: 0,
            hit_rate: 0.0,
            total_artifacts: 0,
            top_crates: vec![],
        });
    Json(resp)
}

// ── Helpers ─────────────────────────────────────────────────────

/// Parse S3 path `/{bucket}/{prefix}/{crate_name}/{cache_key}.tar.zst`
/// or `/{bucket}/{crate_name}/{cache_key}.tar.zst`
/// Returns `(crate_name, cache_key)`.
fn parse_artifact_key(path: &str) -> Option<(&str, &str)> {
    let without_ext = path.strip_suffix(".tar.zst")?;
    let slash = without_ext.rfind('/')?;
    let cache_key = &without_ext[slash + 1..];
    let rest = &without_ext[..slash];
    let crate_name_start = rest.rfind('/').map(|i| i + 1).unwrap_or(0);
    let crate_name = &rest[crate_name_start..];
    if crate_name.is_empty() || cache_key.is_empty() {
        return None;
    }
    Some((crate_name, cache_key))
}

fn header_str(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

fn header_csv(headers: &HeaderMap, name: &str) -> Vec<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').map(|d| d.trim().to_string()).collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_artifact_key() {
        assert_eq!(
            parse_artifact_key("/kache/artifacts/serde/abc123.tar.zst"),
            Some(("serde", "abc123"))
        );
        assert_eq!(
            parse_artifact_key("/kache/prefix/sub/tokio/def456.tar.zst"),
            Some(("tokio", "def456"))
        );
        assert_eq!(
            parse_artifact_key("/kache/serde/abc123.tar.zst"),
            Some(("serde", "abc123"))
        );
        assert_eq!(parse_artifact_key("/kache/not-an-artifact.json"), None);
        assert_eq!(parse_artifact_key(""), None);
    }

    #[test]
    fn test_header_csv() {
        let mut headers = HeaderMap::new();
        headers.insert("x-kache-deps", "serde, tokio, anyhow".parse().unwrap());
        let deps = header_csv(&headers, "x-kache-deps");
        assert_eq!(deps, vec!["serde", "tokio", "anyhow"]);
    }

    #[test]
    fn test_header_csv_empty() {
        let headers = HeaderMap::new();
        let deps = header_csv(&headers, "x-kache-deps");
        assert!(deps.is_empty());
    }
}
