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
    future::Future,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{RwLock, watch};

mod metrics;

mod state;

pub use state::{DEFAULT_DB_PATH, NamespaceState, PlannerStateFile, SurrealPlannerRepository};

type SharedPlannerDataSource = Arc<dyn PlannerDataSource + Send + Sync>;

const SERVICE_ACCOUNT_NAMESPACE_PATH: &str =
    "/var/run/secrets/kubernetes.io/serviceaccount/namespace";

const fn strip_version_prefix(raw: &str) -> &str {
    let bytes = raw.as_bytes();
    if bytes.len() > 1 && bytes[0] == b'v' {
        // SAFETY: removing a leading ASCII 'v' preserves UTF-8 validity.
        unsafe { core::str::from_utf8_unchecked(bytes.split_at(1).1) }
    } else {
        raw
    }
}

pub const VERSION: &str = {
    const RAW: &str = match option_env!("KACHE_VERSION") {
        Some(v) => v,
        None => env!("CARGO_PKG_VERSION"),
    };
    strip_version_prefix(RAW)
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
        .route("/metrics", get(metrics_endpoint))
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
        let leader = run_leader(config.clone(), state, |namespace, lease_name| async move {
            let client = kube::Client::try_default()
                .await
                .context("creating Kubernetes client for HA leader election")?;
            let leader =
                kunobi_ha::leader::LeaderElection::builder(client, namespace, lease_name).build();
            let mut guard = leader
                .acquire()
                .await
                .context("acquiring kache planner leadership")?;

            Ok::<_, anyhow::Error>(async move {
                guard.lost().await;
            })
        });
        spawn_leader_future(leader, ha_done_tx);
    } else {
        let repository = load_repository(&config).await?;
        *state.repository.write().await = repository;
        state.ready.store(true, Ordering::Release);
        metrics::metrics().set_ready(true);
    }

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("binding planner listener on {bind}"))?;
    let local_addr = listener
        .local_addr()
        .context("reading planner local address")?;

    tracing::info!(bind = %local_addr, planner = %planner_name, "planner listening");

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

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(ctrl_c, terminate, ha_done_rx))
        .await
        .context("running planner server")
}

fn spawn_leader_future(
    leader: impl Future<Output = Result<()>> + Send + 'static,
    ha_done_tx: watch::Sender<bool>,
) {
    tokio::spawn(async move {
        if let Err(error) = leader.await {
            tracing::error!(%error, "HA leader task failed");
        }
        let _ = ha_done_tx.send(true);
    });
}

async fn run_leader<Acquire, AcquireFuture, LeadershipLost>(
    config: PlannerConfig,
    state: AppState,
    acquire: Acquire,
) -> Result<()>
where
    Acquire: FnOnce(String, String) -> AcquireFuture,
    AcquireFuture: Future<Output = Result<LeadershipLost>>,
    LeadershipLost: Future<Output = ()>,
{
    let namespace = ha_namespace(&config.ha)?;
    let lease_name = normalize_name(config.ha.lease_name.clone());

    tracing::info!(namespace = %namespace, lease = %lease_name, "waiting for kache planner leadership");
    let leadership_lost = acquire(namespace.clone(), lease_name.clone()).await?;
    tracing::info!(namespace = %namespace, lease = %lease_name, "acquired kache planner leadership");

    let repository = load_repository(&config).await?;
    *state.repository.write().await = repository;
    state.ready.store(true, Ordering::Release);
    metrics::metrics().set_ready(true);

    leadership_lost.await;
    state.ready.store(false, Ordering::Release);
    metrics::metrics().set_ready(false);
    *state.repository.write().await = None;
    tracing::warn!(namespace = %namespace, lease = %lease_name, "lost kache planner leadership");
    Ok(())
}

fn ha_namespace(config: &HaConfig) -> Result<String> {
    normalize_optional(config.namespace.clone())
        .or_else(|| normalize_optional(std::env::var("POD_NAMESPACE").ok()))
        .or_else(|| read_service_account_namespace(Path::new(SERVICE_ACCOUNT_NAMESPACE_PATH)).ok())
        .context(
            "HA leader election requires KACHE_HA_NAMESPACE or a mounted service account namespace",
        )
}

fn read_service_account_namespace(path: &Path) -> Result<String> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("reading service account namespace from {}", path.display()))?;
    parse_service_account_namespace(&contents)
}

fn parse_service_account_namespace(contents: &str) -> Result<String> {
    let namespace = contents.trim();
    if namespace.is_empty() {
        anyhow::bail!("service account namespace is empty");
    }
    Ok(namespace.to_string())
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

/// Prometheus scrape endpoint.
///
/// Deliberately unauthenticated, like /healthz and /readyz: the cluster's
/// SigNoz collector scrapes by pod annotation and carries no bearer token, and
/// the series here describe the planner's own behaviour, not cache contents.
async fn metrics_endpoint() -> ([(axum::http::header::HeaderName, &'static str); 1], String) {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        metrics::metrics().render(),
    )
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
        // Constant-time comparison: a plain `==` short-circuits on the
        // first differing byte, letting a network peer binary-search the
        // token one byte at a time from response timing. Only the length
        // check can leak, which a random bearer token doesn't hinge on.
        use subtle::ConstantTimeEq;
        match self.token.as_deref() {
            Some(expected) if bool::from(token.as_bytes().ct_eq(expected.as_bytes())) => {
                Ok(AuthIdentity {
                    provider: "kache".to_string(),
                    identity: "planner-client".to_string(),
                    method: "token".to_string(),
                    claims: HashMap::new(),
                })
            }
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
    let started = std::time::Instant::now();
    // Every exit below records exactly one outcome. Keeping the helper local
    // means a new early return that forgets to call it shows up as a request
    // that vanished, rather than as silence.
    let record = |outcome: metrics::Outcome| {
        metrics::metrics().record_request(outcome, started.elapsed().as_secs_f64());
    };

    if state.token.is_some() && identity.is_none() {
        record(metrics::Outcome::Unauthorized);
        return Err(StatusCode::UNAUTHORIZED);
    }

    if !state.ready.load(Ordering::Acquire) {
        record(metrics::Outcome::NotReady);
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
        record(metrics::Outcome::FallbackNoState);
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
                record(metrics::Outcome::FallbackPlanningError);
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
        record(metrics::Outcome::FallbackNoCandidates);
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

    metrics::metrics().record_plan_candidates(plan.candidates.len());
    record(metrics::Outcome::Execute);

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

async fn shutdown_signal(
    ctrl_c: impl Future<Output = ()>,
    terminate: impl Future<Output = ()>,
    mut ha_done_rx: watch::Receiver<bool>,
) {
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

    fn test_config(db_path: PathBuf) -> PlannerConfig {
        PlannerConfig {
            bind: "127.0.0.1:8080".parse().unwrap(),
            token: None,
            planner_name: "planner".to_string(),
            db_path,
            seed_state_file: None,
            ha: HaConfig::default(),
        }
    }

    fn test_app(token: Option<&str>, repository: Option<SharedPlannerDataSource>) -> Router {
        let mut config = test_config(PathBuf::from(DEFAULT_DB_PATH));
        config.token = token.map(str::to_string);
        app_with_repository(config, repository)
    }

    fn test_app_with_readiness(ready: bool) -> Router {
        router(AppState {
            token: None,
            planner_name: "planner".to_string(),
            repository: Arc::new(RwLock::new(None)),
            ready: Arc::new(AtomicBool::new(ready)),
        })
    }

    #[test]
    fn version_prefix_is_normalized_independently_of_build_metadata() {
        assert_eq!(strip_version_prefix("v1.2.3"), "1.2.3");
        assert_eq!(strip_version_prefix("1.2.3"), "1.2.3");
        assert_eq!(strip_version_prefix("v"), "v");
        assert_eq!(strip_version_prefix(""), "");
    }

    #[tokio::test]
    async fn app_propagates_repository_open_errors() {
        let dir = tempfile::tempdir().unwrap();
        let blocking_file = dir.path().join("not-a-directory");
        std::fs::write(&blocking_file, b"block child creation").unwrap();

        let result = app(test_config(blocking_file.join("planner.db"))).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn serve_reports_listener_bind_failures() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path().join("planner.db"));
        config.bind = listener.local_addr().unwrap();

        let error = tokio::time::timeout(std::time::Duration::from_secs(5), serve(config))
            .await
            .expect("serve did not report the occupied listener address")
            .unwrap_err();
        assert!(format!("{error:#}").contains("binding planner listener"));
    }

    #[test]
    fn configured_ha_namespace_is_trimmed() {
        let config = HaConfig {
            enabled: true,
            namespace: Some(" team-a ".to_string()),
            lease_name: "planner".to_string(),
        };
        assert_eq!(ha_namespace(&config).unwrap(), "team-a");
    }

    #[test]
    fn service_account_namespace_must_be_non_empty() {
        assert_eq!(
            parse_service_account_namespace(" team-a\n").unwrap(),
            "team-a"
        );
        assert!(parse_service_account_namespace(" \n\t").is_err());
    }

    #[test]
    fn service_account_namespace_is_read_from_the_requested_path() {
        let dir = tempfile::tempdir().unwrap();
        let namespace_path = dir.path().join("namespace");
        std::fs::write(&namespace_path, " team-a\n").unwrap();

        assert_eq!(
            read_service_account_namespace(&namespace_path).unwrap(),
            "team-a"
        );

        std::fs::write(&namespace_path, " \n").unwrap();
        assert!(read_service_account_namespace(&namespace_path).is_err());

        let missing = dir.path().join("missing");
        let error = read_service_account_namespace(&missing).unwrap_err();
        assert!(format!("{error:#}").contains(&missing.display().to_string()));
    }

    #[tokio::test]
    async fn leader_future_always_signals_completion() {
        let (done_tx, mut done_rx) = watch::channel(false);
        spawn_leader_future(async { anyhow::bail!("leader failed") }, done_tx);

        tokio::time::timeout(std::time::Duration::from_secs(5), done_rx.changed())
            .await
            .expect("leader task did not finish")
            .expect("leader task dropped signal");
        assert!(*done_rx.borrow());
    }

    #[tokio::test]
    async fn leader_lifecycle_publishes_and_clears_repository_state() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path().join("planner.db"));
        config.ha = HaConfig {
            enabled: true,
            namespace: Some(" team-a ".to_string()),
            lease_name: " lease-a ".to_string(),
        };

        let state = AppState {
            token: None,
            planner_name: "planner".to_string(),
            repository: Arc::new(RwLock::new(None)),
            ready: Arc::new(AtomicBool::new(false)),
        };
        let observed_state = state.clone();
        let acquisition = Arc::new(std::sync::Mutex::new(None));
        let acquisition_from_task = Arc::clone(&acquisition);
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();

        let leader = tokio::spawn(run_leader(config, state, move |namespace, lease_name| {
            *acquisition_from_task.lock().unwrap() = Some((namespace, lease_name));
            async move {
                Ok(async move {
                    let _ = release_rx.await;
                })
            }
        }));

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while acquisition.lock().unwrap().is_none() {
                assert!(!leader.is_finished(), "leader exited before acquisition");
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("leader acquisition was not observed");
        assert_eq!(
            acquisition.lock().unwrap().as_ref(),
            Some(&("team-a".to_string(), "lease-a".to_string()))
        );

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !observed_state.ready.load(Ordering::Acquire) {
                assert!(!leader.is_finished(), "leader exited before becoming ready");
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("leader did not publish ready state");
        assert!(observed_state.ready.load(Ordering::Acquire));
        assert!(observed_state.repository.read().await.is_some());

        release_tx.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), leader)
            .await
            .expect("leader did not stop after leadership loss")
            .unwrap()
            .unwrap();
        assert!(!observed_state.ready.load(Ordering::Acquire));
        assert!(observed_state.repository.read().await.is_none());
    }

    #[tokio::test]
    async fn shutdown_waits_for_ha_completion() {
        let (done_tx, done_rx) = watch::channel(false);
        let mut shutdown = tokio::spawn(shutdown_signal(
            std::future::pending(),
            std::future::pending(),
            done_rx,
        ));

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut shutdown)
                .await
                .is_err(),
            "shutdown returned before any signal"
        );

        done_tx.send(true).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), shutdown)
            .await
            .expect("shutdown did not observe HA completion")
            .unwrap();
    }

    #[tokio::test]
    async fn shutdown_accepts_each_signal_source() {
        let (_done_tx, done_rx) = watch::channel(false);
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            shutdown_signal(std::future::ready(()), std::future::pending(), done_rx),
        )
        .await
        .expect("shutdown did not observe ctrl-c");

        let (_done_tx, done_rx) = watch::channel(false);
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            shutdown_signal(std::future::pending(), std::future::ready(()), done_rx),
        )
        .await
        .expect("shutdown did not observe termination");
    }

    #[tokio::test]
    async fn shutdown_completes_when_the_ha_sender_drops() {
        let (done_tx, done_rx) = watch::channel(false);
        drop(done_tx);

        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            shutdown_signal(std::future::pending(), std::future::pending(), done_rx),
        )
        .await
        .expect("shutdown did not observe the closed HA channel");
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

    /// The scrape endpoint has to be reachable without a token.
    ///
    /// The metrics module's own tests prove the registry renders; they say
    /// nothing about whether the route is wired, so a missing `.route()` or a
    /// token check creeping onto /metrics would leave the series correct and
    /// the scrape silently 404/401 — metrics that exist but nobody collects.
    #[tokio::test]
    async fn metrics_endpoint_serves_prometheus_text_without_auth() {
        let response = test_app(Some("secret-token"), None)
            .oneshot(
                axum::http::Request::builder()
                    .method("GET")
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::OK,
            "a configured bearer token must not gate the scrape endpoint"
        );
        assert_eq!(
            response
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("text/plain; version=0.0.4"),
            "the collector needs the Prometheus text content type"
        );

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(
            body.contains("kache_planner_requests_total"),
            "scrape body carried no planner series: {body}"
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
    async fn prefetch_plan_rejects_wrong_bearer_token() {
        let response = test_app(Some("secret-token"), None)
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v2/prefetch-plan")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, "Bearer wrong-token")
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
    async fn readiness_endpoint_rejects_requests_until_ready() {
        let response = test_app_with_readiness(false)
            .oneshot(
                axum::http::Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
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
                            identity_key: None,
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
                    vec![PrefetchCandidate::new(
                        "serde-key".to_string(),
                        "serde".to_string(),
                    )],
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
                            identity_key: None,
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
                            identity_key: None,
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
