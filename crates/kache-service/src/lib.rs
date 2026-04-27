use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use kache_core::{
    BuildIntent, PlannerDataSource, PrefetchDisposition, PrefetchPlan, build_prefetch_plan,
};
use kunobi_auth::{
    AuthError, AuthIdentity,
    server::{AuthnProvider, OptionalAuth},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{RwLock, watch};

mod state;

pub use state::{DEFAULT_DB_PATH, NamespaceState, PlannerStateFile, SurrealPlannerRepository};

type SharedPlannerDataSource = Arc<dyn PlannerDataSource + Send + Sync>;

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
    pub db_path: PathBuf,
    pub seed_state_file: Option<PathBuf>,
    pub ha: HaConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HaConfig {
    pub enabled: bool,
    pub namespace: Option<String>,
    pub lease_name: String,
}

impl Default for HaConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            namespace: None,
            lease_name: "kache-service".to_string(),
        }
    }
}

#[derive(Clone)]
struct AppState {
    token: Option<String>,
    planner_name: String,
    repository: Arc<RwLock<Option<SharedPlannerDataSource>>>,
    ready: Arc<AtomicBool>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct HealthResponse {
    status: String,
    planner: String,
    version: String,
}

pub async fn app(config: PlannerConfig) -> Result<Router> {
    let repository = load_repository(&config).await?;
    Ok(app_with_repository(config, repository))
}

fn app_with_repository(
    config: PlannerConfig,
    repository: Option<SharedPlannerDataSource>,
) -> Router {
    let state = AppState {
        token: normalize_optional(config.token),
        planner_name: normalize_name(config.planner_name),
        repository: Arc::new(RwLock::new(repository)),
        ready: Arc::new(AtomicBool::new(true)),
    };

    router(state)
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/v1/prefetch-plan", post(prefetch_plan))
        .route("/v2/prefetch-plan", post(prefetch_plan))
        .with_state(state)
}

pub async fn serve(config: PlannerConfig) -> Result<()> {
    let bind = config.bind;
    let planner_name = normalize_name(config.planner_name.clone());
    let state = AppState {
        token: normalize_optional(config.token.clone()),
        planner_name: planner_name.clone(),
        repository: Arc::new(RwLock::new(None)),
        ready: Arc::new(AtomicBool::new(false)),
    };
    let app = router(state.clone());
    let (ha_done_tx, ha_done_rx) = watch::channel(false);

    if config.ha.enabled {
        spawn_leader_task(config.clone(), state, ha_done_tx);
    } else {
        let repository = load_repository(&config).await?;
        *state.repository.write().await = repository;
        state.ready.store(true, Ordering::Release);
    }

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("binding planner listener on {bind}"))?;
    let local_addr = listener
        .local_addr()
        .context("reading planner local address")?;

    tracing::info!(bind = %local_addr, planner = %planner_name, "planner listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(ha_done_rx))
        .await
        .context("running planner server")
}

fn spawn_leader_task(config: PlannerConfig, state: AppState, ha_done_tx: watch::Sender<bool>) {
    tokio::spawn(async move {
        if let Err(error) = run_leader(config, state).await {
            tracing::error!(%error, "HA leader task failed");
        }
        let _ = ha_done_tx.send(true);
    });
}

async fn run_leader(config: PlannerConfig, state: AppState) -> Result<()> {
    let namespace = ha_namespace(&config.ha)?;
    let lease_name = normalize_name(config.ha.lease_name.clone());
    let client = kube::Client::try_default()
        .await
        .context("creating Kubernetes client for HA leader election")?;
    let leader =
        kunobi_ha::leader::LeaderElection::builder(client, namespace.clone(), lease_name.clone())
            .build();

    tracing::info!(namespace = %namespace, lease = %lease_name, "waiting for kache planner leadership");
    let mut guard = leader
        .acquire()
        .await
        .context("acquiring kache planner leadership")?;
    tracing::info!(namespace = %namespace, lease = %lease_name, "acquired kache planner leadership");

    let repository = load_repository(&config).await?;
    *state.repository.write().await = repository;
    state.ready.store(true, Ordering::Release);

    guard.lost().await;
    state.ready.store(false, Ordering::Release);
    *state.repository.write().await = None;
    tracing::warn!(namespace = %namespace, lease = %lease_name, "lost kache planner leadership");
    Ok(())
}

fn ha_namespace(config: &HaConfig) -> Result<String> {
    normalize_optional(config.namespace.clone())
        .or_else(|| normalize_optional(std::env::var("POD_NAMESPACE").ok()))
        .or_else(|| read_service_account_namespace().ok())
        .context(
            "HA leader election requires KACHE_HA_NAMESPACE or a mounted service account namespace",
        )
}

fn read_service_account_namespace() -> Result<String> {
    std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
        .context("reading service account namespace")
        .map(|s| s.trim().to_string())
        .and_then(|s| {
            if s.is_empty() {
                anyhow::bail!("service account namespace is empty");
            }
            Ok(s)
        })
}

async fn load_repository(config: &PlannerConfig) -> Result<Option<SharedPlannerDataSource>> {
    let repository = SurrealPlannerRepository::open(&config.db_path).await?;
    if let Some(seed_state_file) = config.seed_state_file.as_deref() {
        repository.seed_from_state_file(seed_state_file).await?;
    }
    Ok(Some(Arc::new(repository)))
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
        planner: state.planner_name.clone(),
        version: VERSION.to_string(),
    })
}

async fn readyz(State(state): State<AppState>) -> Result<Json<HealthResponse>, StatusCode> {
    if !state.ready.load(Ordering::Acquire) {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    Ok(Json(HealthResponse {
        status: "ok".to_string(),
        planner: state.planner_name.clone(),
        version: VERSION.to_string(),
    }))
}

impl AuthnProvider for AppState {
    async fn authenticate(&self, token: &str) -> Result<AuthIdentity, AuthError> {
        match self.token.as_deref() {
            Some(expected) if token == expected => Ok(AuthIdentity {
                provider: "kache".to_string(),
                identity: "planner-client".to_string(),
                method: "token".to_string(),
                claims: HashMap::new(),
            }),
            Some(_) => Err(AuthError::Unauthorized("invalid bearer token".to_string())),
            None => Ok(AuthIdentity {
                provider: "kache".to_string(),
                identity: "anonymous".to_string(),
                method: "none".to_string(),
                claims: HashMap::new(),
            }),
        }
    }
}

async fn prefetch_plan(
    State(state): State<AppState>,
    OptionalAuth(identity): OptionalAuth,
    Json(intent): Json<BuildIntent>,
) -> Result<Json<PrefetchPlan>, StatusCode> {
    if state.token.is_some() && identity.is_none() {
        return Err(StatusCode::UNAUTHORIZED);
    }

    if !state.ready.load(Ordering::Acquire) {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    let repository = state.repository.read().await;
    let Some(repository) = repository.as_ref() else {
        tracing::info!(
            planner = %state.planner_name,
            crate_count = intent.crate_names.len(),
            lock_dep_count = intent.cargo_lock_deps.len(),
            has_namespace = intent.namespace.is_some(),
            "planner request: no service-side state configured, requesting fallback"
        );
        return Ok(Json(fallback_plan(&state.planner_name)));
    };

    let mut plan =
        match build_prefetch_plan(repository.as_ref(), &intent, &state.planner_name).await {
            Ok(plan) => plan,
            Err(error) => {
                tracing::warn!(
                    planner = %state.planner_name,
                    %error,
                    "planner request: planning failed, requesting fallback"
                );
                return Ok(Json(fallback_plan(&state.planner_name)));
            }
        };

    if plan.candidates.is_empty() {
        tracing::info!(
            planner = %state.planner_name,
            crate_count = intent.crate_names.len(),
            lock_dep_count = intent.cargo_lock_deps.len(),
            has_namespace = intent.namespace.is_some(),
            "planner request: no candidates resolved from service state, requesting fallback"
        );
        return Ok(Json(fallback_plan(&state.planner_name)));
    }

    tracing::info!(
        planner = %state.planner_name,
        crate_count = intent.crate_names.len(),
        lock_dep_count = intent.cargo_lock_deps.len(),
        has_namespace = intent.namespace.is_some(),
        candidate_count = plan.candidates.len(),
        "planner request: returning execute plan from service state"
    );

    plan.plan_id.get_or_insert_with(next_plan_id);
    Ok(Json(plan))
}

fn next_plan_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("plan-{millis}")
}

fn fallback_plan(planner_name: &str) -> PrefetchPlan {
    PrefetchPlan {
        plan_id: Some(next_plan_id()),
        planner: Some(planner_name.to_string()),
        disposition: PrefetchDisposition::UseFallback,
        candidates: vec![],
    }
}

async fn shutdown_signal(mut ha_done_rx: watch::Receiver<bool>) {
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
        _ = async {
            while ha_done_rx.changed().await.is_ok() {
                if *ha_done_rx.borrow() {
                    break;
                }
            }
        } => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::header;
    use http_body_util::BodyExt;
    use kache_core::PrefetchCandidate;
    use std::collections::HashMap;
    use tower::util::ServiceExt;

    fn test_app(token: Option<&str>, repository: Option<SharedPlannerDataSource>) -> Router {
        app_with_repository(
            PlannerConfig {
                bind: "127.0.0.1:8080".parse().unwrap(),
                token: token.map(str::to_string),
                planner_name: "planner".to_string(),
                db_path: PathBuf::from(DEFAULT_DB_PATH),
                seed_state_file: None,
                ha: HaConfig::default(),
            },
            repository,
        )
    }

    #[tokio::test]
    async fn health_endpoint_returns_service_metadata() {
        let response = test_app(None, None)
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
        let response = test_app(Some("secret-token"), None)
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
        let response = test_app(Some("secret-token"), None)
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
                .is_some_and(|id| id.starts_with("plan-"))
        );
    }

    #[tokio::test]
    async fn prefetch_plan_returns_execute_when_repository_has_candidates() {
        let dir = tempfile::tempdir().unwrap();
        let repository = SurrealPlannerRepository::open(&dir.path().join("planner.db"))
            .await
            .unwrap();
        repository
            .seed_from_state(PlannerStateFile {
                namespaces: HashMap::new(),
                history: HashMap::from([(
                    "serde".to_string(),
                    vec![PrefetchCandidate {
                        cache_key: "serde-key".to_string(),
                        crate_name: "serde".to_string(),
                    }],
                )]),
                key_cache: HashMap::new(),
            })
            .await
            .unwrap();

        let response = test_app(None, Some(Arc::new(repository)))
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v2/prefetch-plan")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&BuildIntent {
                            crate_names: vec!["serde".to_string()],
                            namespace: None,
                            cargo_lock_deps: vec![],
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
        assert_eq!(plan.disposition, PrefetchDisposition::Execute);
        assert_eq!(plan.candidates.len(), 1);
        assert_eq!(plan.candidates[0].cache_key, "serde-key");
        assert_eq!(plan.planner.as_deref(), Some("planner"));
        assert!(
            plan.plan_id
                .as_deref()
                .is_some_and(|id| id.starts_with("plan-"))
        );
    }

    #[tokio::test]
    async fn prefetch_plan_returns_use_fallback_when_repository_has_no_candidates() {
        let dir = tempfile::tempdir().unwrap();
        let repository = SurrealPlannerRepository::open(&dir.path().join("planner.db"))
            .await
            .unwrap();

        let response = test_app(None, Some(Arc::new(repository)))
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v2/prefetch-plan")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&BuildIntent {
                            crate_names: vec!["serde".to_string()],
                            namespace: None,
                            cargo_lock_deps: vec![],
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
    }
}
