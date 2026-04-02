use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    routing::{get, post},
};
use kache_core::{BuildIntent, PrefetchDisposition, PrefetchPlan};
use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    time::{SystemTime, UNIX_EPOCH},
};

pub const VERSION: &str = {
    const RAW: &str = match option_env!("KACHE_VERSION") {
        Some(v) => v,
        None => env!("CARGO_PKG_VERSION"),
    };
    let bytes = RAW.as_bytes();
    if bytes.len() > 1 && bytes[0] == b'v' {
        // SAFETY: removing a leading ASCII 'v' preserves UTF-8 validity.
        unsafe { core::str::from_utf8_unchecked(bytes.split_at(1).1) }
    } else {
        RAW
    }
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannerConfig {
    pub bind: SocketAddr,
    pub token: Option<String>,
    pub planner_name: String,
}

#[derive(Debug, Clone)]
struct AppState {
    token: Option<String>,
    planner_name: String,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct HealthResponse {
    status: String,
    planner: String,
    version: String,
}

pub fn app(config: PlannerConfig) -> Router {
    let state = AppState {
        token: normalize_optional(config.token),
        planner_name: normalize_name(config.planner_name),
    };

    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(healthz))
        .route("/v1/prefetch-plan", post(prefetch_plan))
        .route("/v2/prefetch-plan", post(prefetch_plan))
        .with_state(state)
}

pub async fn serve(config: PlannerConfig) -> Result<()> {
    let bind = config.bind;
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("binding planner listener on {bind}"))?;
    let local_addr = listener
        .local_addr()
        .context("reading planner local address")?;

    tracing::info!(bind = %local_addr, planner = %config.planner_name, "planner listening");

    axum::serve(listener, app(config))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("running planner server")
}

fn normalize_optional(value: Option<String>) -> Option<String> {
    value
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn normalize_name(value: String) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        "planner".to_string()
    } else {
        trimmed.to_string()
    }
}

async fn healthz(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        planner: state.planner_name,
        version: VERSION.to_string(),
    })
}

async fn prefetch_plan(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(intent): Json<BuildIntent>,
) -> Result<Json<PrefetchPlan>, StatusCode> {
    authorize(&headers, state.token.as_deref())?;

    tracing::info!(
        planner = %state.planner_name,
        crate_count = intent.crate_names.len(),
        lock_dep_count = intent.cargo_lock_deps.len(),
        has_namespace = intent.namespace.is_some(),
        "planner request: delegating to local fallback"
    );

    Ok(Json(PrefetchPlan {
        plan_id: Some(next_plan_id()),
        planner: Some(state.planner_name),
        disposition: PrefetchDisposition::UseFallback,
        candidates: vec![],
    }))
}

fn authorize(headers: &HeaderMap, expected_token: Option<&str>) -> Result<(), StatusCode> {
    let Some(expected_token) = expected_token else {
        return Ok(());
    };

    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));

    match token {
        Some(token) if token == expected_token => Ok(()),
        _ => Err(StatusCode::UNAUTHORIZED),
    }
}

fn next_plan_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("fallback-{millis}")
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("installing ctrl+c handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("installing terminate handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http_body_util::BodyExt;
    use tower::util::ServiceExt;

    fn test_app(token: Option<&str>) -> Router {
        app(PlannerConfig {
            bind: "127.0.0.1:8080".parse().unwrap(),
            token: token.map(str::to_string),
            planner_name: "planner".to_string(),
        })
    }

    #[tokio::test]
    async fn health_endpoint_returns_service_metadata() {
        let response = test_app(None)
            .oneshot(
                axum::http::Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: HealthResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            parsed,
            HealthResponse {
                status: "ok".to_string(),
                planner: "planner".to_string(),
                version: VERSION.to_string(),
            }
        );
    }

    #[tokio::test]
    async fn prefetch_plan_requires_bearer_token_when_configured() {
        let response = test_app(Some("secret-token"))
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v2/prefetch-plan")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&BuildIntent::default()).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn prefetch_plan_returns_use_fallback_when_authorized() {
        let response = test_app(Some("secret-token"))
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v2/prefetch-plan")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, "Bearer secret-token")
                    .body(Body::from(
                        serde_json::to_vec(&BuildIntent {
                            crate_names: vec!["serde".to_string()],
                            namespace: Some("linux/hash/debug".to_string()),
                            cargo_lock_deps: vec![("serde".to_string(), "1.0.0".to_string())],
                        })
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let plan: PrefetchPlan = serde_json::from_slice(&body).unwrap();
        assert_eq!(plan.disposition, PrefetchDisposition::UseFallback);
        assert!(plan.candidates.is_empty());
        assert_eq!(plan.planner.as_deref(), Some("planner"));
        assert!(
            plan.plan_id
                .as_deref()
                .is_some_and(|id| id.starts_with("fallback-"))
        );
    }
}
