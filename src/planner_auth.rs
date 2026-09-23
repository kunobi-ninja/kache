//! The bearer a planner request carries, resolved without ever prompting.
//!
//! First match wins:
//! 1. an explicit token (`KACHE_PLANNER_TOKEN` / `cache.planner.token`);
//! 2. the job's workload token (kunobi-auth's `client::workload`): the
//!    GitHub Actions ID token, a GitLab CI ID token, or a Kubernetes
//!    service-account token, minted for the planner's own base URL so no
//!    other endpoint can replay it (a GitHub job needs `id-token: write`);
//! 3. the Kunobi session `kache login` stored for this planner;
//! 4. nothing.
//!
//! Automatic credentials (2 and 3) only go to a planner reached over HTTPS
//! (or loopback), and a Kunobi session only to a planner the user trusted
//! with `kache login`: a planner URL picked up from a project's config must
//! not be able to collect anyone's credentials.
//!
//! The planner is advisory, so every failure here means "no bearer": the
//! request goes out anyway and a 401 lands in the local-planning fallback.

use crate::config::PlannerConfig;
use anyhow::{Context, Result, bail};
use kunobi_auth::client::workload::{self, WorkloadSources};
use kunobi_auth::client::{ServiceConfig, StoredToken, TofuStore, TokenStorage, TokenStore};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// How long a planner's advertised login is reused. The daemon is long-lived;
/// a planner's auth configuration rarely moves. Trust (the pin) is checked on
/// every use, so a fresh `kache login` takes effect at once.
const DISCOVERY_TTL: Duration = Duration::from_secs(600);
/// How long "this planner offers no login" is reused.
const NO_LOGIN_TTL: Duration = Duration::from_secs(60);
/// How long a session refresh may wait for the session lock, and how long the
/// refresh itself may take: a stalled IdP must not hold the lock forever.
const REFRESH_BOUND: Duration = Duration::from_secs(30);

/// When a planner was asked for its login, and what it answered (`None`: it
/// offers none).
type Discovered = (Instant, Option<ServiceConfig>);
static DISCOVERY: Mutex<Option<HashMap<String, Discovered>>> = Mutex::new(None);
/// At most one refresh in flight per process. A build that finds one running
/// sends no bearer rather than queueing behind it; later builds pick up the
/// refreshed session.
static REFRESH: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The bearer for a planner request, resolved before `deadline`. The caller
/// shares that deadline with the request itself.
pub async fn bearer(config: &PlannerConfig, deadline: tokio::time::Instant) -> Option<String> {
    if let Some(token) = &config.token {
        return Some(token.clone());
    }
    crate::planner_client::ensure_crypto_provider();
    let sources = WorkloadSources::from_env();
    match tokio::time::timeout_at(deadline, resolve(config, &sources)).await {
        Ok(token) => token,
        Err(_) => {
            tracing::debug!("planner auth: resolving a bearer ran out of time; sending none");
            None
        }
    }
}

/// `sources` are the workload tokens this process can present.
async fn resolve(config: &PlannerConfig, sources: &WorkloadSources) -> Option<String> {
    let base = planner_base(&config.endpoint);
    if !workload::may_receive(&base) {
        tracing::debug!("planner auth: {base} is not HTTPS; sending no automatic credential");
        return None;
    }
    // Always the planner's own URL as the audience: one taken from config
    // would let a project point `endpoint` elsewhere and collect a token
    // minted for a real planner.
    match workload::token_from(sources, &base, &base).await {
        Ok(Some(token)) => return Some(token.token),
        Ok(None) => {}
        Err(error) => tracing::debug!("planner auth: workload token: {error:#}"),
    }
    match kunobi_session_token(&base).await {
        Ok(token) => token,
        Err(error) => {
            tracing::debug!("planner auth: Kunobi session ({error:#}); run `kache login`");
            None
        }
    }
}

/// Kunobi sessions keyed by issuer **and** client id. kunobi-auth's shared
/// store keys by issuer alone, and other Kunobi tools (kobe) log in to the
/// same issuer with their own client: without the client id they would
/// overwrite each other's sessions and refresh with the wrong client.
pub(crate) struct ScopedTokenStore {
    inner: Box<dyn TokenStorage>,
    client_id: String,
}

impl ScopedTokenStore {
    pub(crate) fn new(client_id: &str) -> Result<Self> {
        Ok(Self::over(Box::new(TokenStore::new()?), client_id))
    }

    fn over(inner: Box<dyn TokenStorage>, client_id: &str) -> Self {
        Self {
            inner,
            client_id: client_id.to_string(),
        }
    }

    fn key(&self, issuer: &str) -> String {
        format!("{issuer}#client={}", self.client_id)
    }
}

impl TokenStorage for ScopedTokenStore {
    fn load(&self, issuer: &str) -> Result<Option<StoredToken>> {
        Ok(self.inner.load(&self.key(issuer))?.map(|mut token| {
            token.issuer = issuer.to_string();
            token
        }))
    }

    fn save(&self, token: &StoredToken) -> Result<()> {
        let mut scoped = token.clone();
        scoped.issuer = self.key(&token.issuer);
        self.inner.save(&scoped)
    }

    fn remove(&self, issuer: &str) -> Result<()> {
        self.inner.remove(&self.key(issuer))
    }
}

/// The session `kache login` stored for the planner at `base`, refreshed when
/// expired. `Ok(None)` when the planner offers no login, this machine never
/// trusted it, or nobody logged in.
pub(crate) async fn kunobi_session_token(base: &str) -> Result<Option<String>> {
    let Some(service) = trusted_login(base, &TofuStore::new()?).await else {
        return Ok(None);
    };
    let store = ScopedTokenStore::new(&service.client_id)?;
    session_token(service, Box::new(store), default_lock_dir()?, oidc_refresh).await
}

/// Exchange a refresh token at the issuer, bounded by [`REFRESH_BOUND`].
async fn oidc_refresh(service: ServiceConfig, refresh_token: String) -> Result<StoredToken> {
    tokio::time::timeout(
        REFRESH_BOUND,
        kunobi_auth::client::oidc::refresh(
            &service.issuer,
            &service.client_id,
            &service.redirect_uri,
            &refresh_token,
        ),
    )
    .await
    .context("the Kunobi session refresh timed out")?
    .context("refreshing the Kunobi session")
}

/// The stored session's ID token, refreshing it with `refresh` when expired.
async fn session_token<R, F>(
    service: ServiceConfig,
    store: Box<dyn TokenStorage>,
    lock_dir: std::path::PathBuf,
    refresh: R,
) -> Result<Option<String>>
where
    R: FnOnce(ServiceConfig, String) -> F + Send + 'static,
    F: std::future::Future<Output = Result<StoredToken>> + Send,
{
    match store.load(&service.issuer)? {
        None => return Ok(None),
        Some(stored) if !stored.is_expired() => return Ok(Some(stored.id_token)),
        Some(_) => {}
    }
    let Ok(in_flight) = REFRESH.try_lock() else {
        tracing::debug!("planner auth: a session refresh is already running");
        return Ok(None);
    };
    // Refresh in its own task so it completes and persists the rotated refresh
    // token even when this build stops waiting for it.
    tokio::spawn(async move {
        let _in_flight = in_flight;
        refresh_session(service, store, lock_dir, refresh).await
    })
    .await
    .context("the session refresh task failed")?
}

async fn refresh_session<R, F>(
    service: ServiceConfig,
    store: Box<dyn TokenStorage>,
    lock_dir: std::path::PathBuf,
    refresh: R,
) -> Result<Option<String>>
where
    R: FnOnce(ServiceConfig, String) -> F,
    F: std::future::Future<Output = Result<StoredToken>>,
{
    let _session = session_lock_in(
        lock_dir,
        &service.issuer,
        &service.client_id,
        REFRESH_BOUND,
        || {},
    )
    .await?;
    // Re-check under the lock: another process may have refreshed meanwhile.
    let Some(stored) = store.load(&service.issuer)? else {
        return Ok(None);
    };
    if !stored.is_expired() {
        return Ok(Some(stored.id_token));
    }
    let Some(refresh_token) = stored.refresh_token.clone() else {
        bail!("the Kunobi session expired and has no refresh token");
    };
    let mut refreshed = refresh(service, refresh_token).await?;
    refreshed.extra = stored.extra;
    let token = refreshed.id_token.clone();
    store.save(&refreshed)?;
    Ok(Some(token))
}

/// The planner's login, if this machine trusted it with `kache login`.
/// Discovery is unpinned and then checked fail-closed against the pin on
/// every call, so the daemon never establishes trust on its own and a new
/// login is honoured immediately.
async fn trusted_login(base: &str, pins: &TofuStore) -> Option<ServiceConfig> {
    let service = advertised_login(base).await?;
    let pinned = pins.verify_or_reject(
        &service.endpoint,
        &service.issuer,
        service.audience.as_deref().unwrap_or(""),
    );
    match pinned {
        Ok(()) => Some(service),
        Err(error) => {
            tracing::debug!(
                "planner auth: {base} is not trusted for login ({error:#}); run `kache login`"
            );
            None
        }
    }
}

/// What `/.well-known/kunobi-auth` says, cached per planner.
async fn advertised_login(base: &str) -> Option<ServiceConfig> {
    let cached = DISCOVERY.lock().ok().and_then(|cache| {
        let (at, service) = cache.as_ref()?.get(base)?;
        let ttl = if service.is_some() {
            DISCOVERY_TTL
        } else {
            NO_LOGIN_TTL
        };
        (at.elapsed() < ttl).then(|| service.clone())
    });
    if let Some(service) = cached {
        return service;
    }
    let service = match kunobi_auth::client::discover_unpinned(base).await {
        Ok(service) => Some(service),
        Err(error) => {
            tracing::debug!("planner auth: no login offered by {base}: {error:#}");
            None
        }
    };
    if let Ok(mut cache) = DISCOVERY.lock() {
        cache
            .get_or_insert_with(HashMap::new)
            .insert(base.to_string(), (Instant::now(), service.clone()));
    }
    service
}

/// Cross-process lock for one planner session (issuer + client id). Refresh,
/// `kache login` and `kache logout` take it, so concurrent daemons and CLIs
/// never spend the same rotating refresh token twice, and a refresh cannot
/// overwrite a new login or bring back a removed session. Held until the
/// returned file is dropped.
pub(crate) async fn session_lock(
    issuer: &str,
    client_id: &str,
    wait: Duration,
) -> Result<std::fs::File> {
    session_lock_in(default_lock_dir()?, issuer, client_id, wait, || {}).await
}

fn default_lock_dir() -> Result<std::path::PathBuf> {
    Ok(dirs::config_dir()
        .context("no config directory for the session lock")?
        .join("kunobi")
        .join("locks"))
}

/// [`session_lock`] in `dir`; `on_wait` runs each time the lock is found
/// held and the caller keeps waiting.
async fn session_lock_in(
    dir: std::path::PathBuf,
    issuer: &str,
    client_id: &str,
    wait: Duration,
    on_wait: impl Fn() + Send + 'static,
) -> Result<std::fs::File> {
    let key = blake3::hash(format!("kache\0{issuer}\0{client_id}").as_bytes()).to_hex();
    let path = dir.join(format!("kache-session-{}.lock", &key[..16]));
    tokio::task::spawn_blocking(move || -> Result<std::fs::File> {
        std::fs::create_dir_all(&dir)?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .with_context(|| format!("opening {}", path.display()))?;
        let started = Instant::now();
        loop {
            match file.try_lock() {
                Ok(()) => return Ok(file),
                Err(std::fs::TryLockError::WouldBlock) if started.elapsed() < wait => {
                    on_wait();
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(std::fs::TryLockError::WouldBlock) => {
                    bail!("another kache process is using the planner session")
                }
                Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
            }
        }
    })
    .await
    .context("the session lock task failed")?
}

/// The planner's base URL: discovery lives at its root, while the configured
/// endpoint may carry the `/v1/prefetch-plan` path.
pub(crate) fn planner_base(endpoint: &str) -> String {
    let trimmed = endpoint.trim_end_matches('/');
    ["/v1/prefetch-plan", "/v2/prefetch-plan"]
        .iter()
        .find_map(|path| trimmed.strip_suffix(path))
        .unwrap_or(trimmed)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn jwt_with_exp(exp: u64) -> String {
        let encode = |v: serde_json::Value| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(v.to_string())
        };
        format!(
            "{}.{}.sig",
            encode(serde_json::json!({"alg": "RS256"})),
            encode(serde_json::json!({"exp": exp, "aud": "kache"}))
        )
    }

    /// A stub Actions token endpoint. Returns its URL (with the query the
    /// runtime's URL already carries) and a request counter.
    async fn stub_github(token: String, audience: &str) -> (String, Arc<AtomicUsize>) {
        let expected = format!("get /?api-version=2.0&{} ", {
            let mut q = reqwest::Url::parse("http://x/").unwrap();
            q.query_pairs_mut().append_pair("audience", audience);
            q.query().unwrap().to_lowercase()
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = hits.clone();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                counter.fetch_add(1, Ordering::SeqCst);
                let mut buf = [0u8; 4096];
                let n = socket.read(&mut buf).await.unwrap();
                let request = String::from_utf8_lossy(&buf[..n]).to_lowercase();
                assert!(request.starts_with(&expected), "{request}");
                assert!(
                    request.contains("authorization: bearer runtime-token"),
                    "{request}"
                );
                let body = serde_json::json!({ "value": token }).to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (format!("http://{addr}/?api-version=2.0"), hits)
    }

    fn in_an_hour() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600
    }

    fn soon() -> tokio::time::Instant {
        tokio::time::Instant::now() + Duration::from_millis(500)
    }

    #[tokio::test]
    async fn explicit_token_wins() {
        let config = PlannerConfig {
            endpoint: "http://127.0.0.1:9".to_string(),
            timeout_ms: 50,
            token: Some("static".to_string()),
        };
        assert_eq!(bearer(&config, soon()).await.as_deref(), Some("static"));
    }

    #[tokio::test]
    async fn a_plain_http_planner_gets_no_automatic_credential() {
        let config = PlannerConfig {
            endpoint: "http://planner.example.com".to_string(),
            timeout_ms: 50,
            token: None,
        };
        assert_eq!(bearer(&config, soon()).await, None);
    }

    #[test]
    fn planner_base_strips_the_prefetch_plan_path() {
        assert_eq!(planner_base("https://p.example/"), "https://p.example");
        assert_eq!(
            planner_base("https://p.example/v1/prefetch-plan"),
            "https://p.example"
        );
        assert_eq!(
            planner_base("https://p.example/v2/prefetch-plan/"),
            "https://p.example"
        );
    }

    #[tokio::test]
    async fn a_planner_without_login_yields_no_session() {
        crate::planner_client::ensure_crypto_provider();
        // Nothing listens here: discovery fails, which means "no login". The
        // pin store is a temporary one, so this holds in sandboxes (Nix) where
        // HOME is not writable.
        let directory = tempfile::tempdir().unwrap();
        assert!(
            trusted_login("http://127.0.0.1:9", &pins(&directory))
                .await
                .is_none()
        );
    }

    #[test]
    fn session_locks_live_under_the_kunobi_config_directory() {
        let dir = default_lock_dir().unwrap();
        assert!(dir.ends_with("kunobi/locks"), "{}", dir.display());
        assert!(dir.is_absolute(), "{}", dir.display());
    }

    /// In-memory stand-in for kunobi-auth's file store, keyed like it is.
    #[derive(Default, Clone)]
    struct MemoryStore(Arc<Mutex<HashMap<String, StoredToken>>>);

    impl TokenStorage for MemoryStore {
        fn load(&self, issuer: &str) -> Result<Option<StoredToken>> {
            Ok(self.0.lock().unwrap().get(issuer).cloned())
        }
        fn save(&self, token: &StoredToken) -> Result<()> {
            self.0
                .lock()
                .unwrap()
                .insert(token.issuer.clone(), token.clone());
            Ok(())
        }
        fn remove(&self, issuer: &str) -> Result<()> {
            self.0.lock().unwrap().remove(issuer);
            Ok(())
        }
    }

    #[test]
    fn sessions_for_one_issuer_are_kept_apart_per_client() {
        let shared = MemoryStore::default();
        let kache = ScopedTokenStore::over(Box::new(shared.clone()), "kache-cli");
        let kobe = ScopedTokenStore::over(Box::new(shared.clone()), "kobe-cli");
        let issuer = "https://clerk.example";
        kache
            .save(&StoredToken::new(
                "kache-id".into(),
                None,
                None,
                issuer.into(),
            ))
            .unwrap();
        kobe.save(&StoredToken::new(
            "kobe-id".into(),
            None,
            None,
            issuer.into(),
        ))
        .unwrap();

        let loaded = kache.load(issuer).unwrap().unwrap();
        assert_eq!(loaded.id_token, "kache-id");
        assert_eq!(loaded.issuer, issuer, "the caller sees the real issuer");
        assert_eq!(kobe.load(issuer).unwrap().unwrap().id_token, "kobe-id");
        // Neither lands on the unscoped key another tool would read.
        assert!(shared.load(issuer).unwrap().is_none());

        kache.remove(issuer).unwrap();
        assert!(kache.load(issuer).unwrap().is_none());
        assert!(kobe.load(issuer).unwrap().is_some());
    }

    async fn lock_in(
        dir: &tempfile::TempDir,
        client_id: &str,
        wait: Duration,
    ) -> Result<std::fs::File> {
        session_lock_in(
            dir.path().to_path_buf(),
            "https://idp.example",
            client_id,
            wait,
            || {},
        )
        .await
    }

    /// Runs `test` on a runtime that does not wait for blocking threads at
    /// shutdown, so a lock that polls forever fails the test instead of
    /// hanging it.
    fn on_short_lived_runtime<T>(test: impl std::future::Future<Output = T>) -> T {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let outcome = runtime.block_on(test);
        runtime.shutdown_timeout(Duration::from_millis(100));
        outcome
    }

    #[test]
    fn the_session_lock_excludes_a_second_holder_until_released() {
        let dir = tempfile::tempdir().unwrap();
        on_short_lived_runtime(async {
            let held = lock_in(&dir, "kache-cli", Duration::from_secs(1))
                .await
                .unwrap();
            let second = tokio::time::timeout(
                Duration::from_secs(5),
                lock_in(&dir, "kache-cli", Duration::from_millis(100)),
            )
            .await
            .expect("a bounded wait must end");
            assert!(second.is_err());
            // A different client for the same issuer is a different session.
            drop(
                lock_in(&dir, "kobe-cli", Duration::from_millis(100))
                    .await
                    .unwrap(),
            );
            drop(held);
            drop(
                lock_in(&dir, "kache-cli", Duration::from_millis(100))
                    .await
                    .unwrap(),
            );
        });
    }

    #[tokio::test]
    async fn resolve_sends_the_actions_token_to_a_loopback_planner_bound_to_its_url() {
        crate::planner_client::ensure_crypto_provider();
        let base = "http://127.0.0.1:1";
        let token = jwt_with_exp(in_an_hour());
        let (url, hits) = stub_github(token.clone(), base).await;
        let config = PlannerConfig {
            endpoint: format!("{base}/v1/prefetch-plan"),
            timeout_ms: 1000,
            token: None,
        };
        let sources = WorkloadSources {
            github: Some((url.clone(), "runtime-token".to_string())),
            ..Default::default()
        };
        assert_eq!(resolve(&config, &sources).await, Some(token));
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        // The same runtime never mints a token for a plain-HTTP remote planner.
        let remote = PlannerConfig {
            endpoint: "http://planner.example.com".to_string(),
            ..config
        };
        assert_eq!(resolve(&remote, &sources).await, None);
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    /// A planner that advertises its login; returns its base URL and a
    /// counter of discovery requests.
    async fn stub_planner(issuer: &str) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = hits.clone();
        let body = serde_json::json!({"issuer": issuer, "clientId": "kache-cli"}).to_string();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                counter.fetch_add(1, Ordering::SeqCst);
                let mut buf = [0u8; 4096];
                let _ = socket.read(&mut buf).await.unwrap();
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (format!("http://{addr}"), hits)
    }

    fn pins(directory: &tempfile::TempDir) -> TofuStore {
        TofuStore::with_path(directory.path().join("known.json"))
    }

    #[tokio::test]
    async fn only_a_planner_pinned_by_kache_login_offers_its_session() {
        crate::planner_client::ensure_crypto_provider();
        let (base, hits) = stub_planner("https://clerk.example").await;
        let directory = tempfile::tempdir().unwrap();
        let pins = pins(&directory);
        assert!(
            trusted_login(&base, &pins).await.is_none(),
            "never logged in"
        );

        pins.trust(&base, "https://clerk.example", "").unwrap();
        let service = trusted_login(&base, &pins).await.unwrap();
        assert_eq!(service.issuer, "https://clerk.example");
        assert_eq!(service.client_id, "kache-cli");

        // Pinned to another issuer: refused.
        pins.trust(&base, "https://other.example", "").unwrap();
        assert!(trusted_login(&base, &pins).await.is_none());
        // All three answers came from one cached discovery request.
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    /// `session_token` shares the process-wide refresh slot; run those tests
    /// one at a time.
    static SESSION_TESTS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn service() -> ServiceConfig {
        // A per-process issuer keeps the session lock file to this test run.
        let issuer = format!("https://session-test-{}.example", std::process::id());
        ServiceConfig::new("https://planner.example", &issuer, "kache-cli")
    }

    fn stored(
        service: &ServiceConfig,
        id: &str,
        expires_in: i64,
        refresh: Option<&str>,
    ) -> StoredToken {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        StoredToken::new(
            id.into(),
            refresh.map(str::to_string),
            Some(now + expires_in),
            service.issuer.clone(),
        )
    }

    async fn no_refresh(_: ServiceConfig, _: String) -> Result<StoredToken> {
        panic!("a fresh session must not be refreshed")
    }

    #[tokio::test]
    async fn a_fresh_session_is_used_as_is() {
        let _serial = SESSION_TESTS.lock().await;
        let lock_dir = tempfile::tempdir().unwrap();
        let service = service();
        let store = MemoryStore::default();
        store
            .save(&stored(&service, "fresh", 3600, Some("rt")))
            .unwrap();
        let token = session_token(
            service,
            Box::new(store),
            lock_dir.path().to_path_buf(),
            no_refresh,
        )
        .await
        .unwrap();
        assert_eq!(token.as_deref(), Some("fresh"));
    }

    #[tokio::test]
    async fn an_expired_session_is_refreshed_and_saved() {
        let _serial = SESSION_TESTS.lock().await;
        let lock_dir = tempfile::tempdir().unwrap();
        let service = service();
        let store = MemoryStore::default();
        store
            .save(&stored(&service, "old", -10, Some("rt-1")))
            .unwrap();
        let issued = stored(&service, "new", 3600, Some("rt-2"));
        let refresh = move |_: ServiceConfig, refresh_token: String| async move {
            assert_eq!(refresh_token, "rt-1");
            Ok(issued)
        };
        let token = session_token(
            service.clone(),
            Box::new(store.clone()),
            lock_dir.path().to_path_buf(),
            refresh,
        )
        .await
        .unwrap();
        assert_eq!(token.as_deref(), Some("new"));
        let saved = store.load(&service.issuer).unwrap().unwrap();
        assert_eq!(saved.id_token, "new");
        assert_eq!(saved.refresh_token.as_deref(), Some("rt-2"));
    }

    #[tokio::test]
    async fn an_expired_session_without_a_refresh_token_is_not_sent() {
        let _serial = SESSION_TESTS.lock().await;
        let lock_dir = tempfile::tempdir().unwrap();
        let service = service();
        let store = MemoryStore::default();
        store.save(&stored(&service, "old", -10, None)).unwrap();
        assert!(
            session_token(
                service,
                Box::new(store),
                lock_dir.path().to_path_buf(),
                no_refresh
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn a_refresh_that_finds_the_session_already_renewed_keeps_it() {
        let _serial = SESSION_TESTS.lock().await;
        let lock_dir = tempfile::tempdir().unwrap();
        let service = service();
        let store = MemoryStore::default();
        store
            .save(&stored(&service, "renewed", 3600, Some("rt")))
            .unwrap();
        let token = refresh_session(
            service,
            Box::new(store),
            lock_dir.path().to_path_buf(),
            no_refresh,
        )
        .await
        .unwrap();
        assert_eq!(token.as_deref(), Some("renewed"));
    }

    #[tokio::test]
    async fn the_session_lock_waits_for_the_holder_to_release() {
        let dir = tempfile::tempdir().unwrap();
        let held = lock_in(&dir, "kache-cli", Duration::from_secs(1))
            .await
            .unwrap();
        // Release only once the waiter has seen the lock held, so the test
        // exercises the waiting branch rather than a lucky late start.
        let (contended, seen) = std::sync::mpsc::channel::<()>();
        let waiter = session_lock_in(
            dir.path().to_path_buf(),
            "https://idp.example",
            "kache-cli",
            Duration::from_secs(3),
            move || {
                let _ = contended.send(());
            },
        );
        let waiter = tokio::spawn(waiter);
        tokio::task::spawn_blocking(move || seen.recv_timeout(Duration::from_secs(3)))
            .await
            .unwrap()
            .expect("the waiter must find the lock held and wait");
        drop(held);
        let waited = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("the waiter must not hang")
            .unwrap();
        assert!(waited.is_ok(), "{waited:?}");
    }

    #[test]
    fn the_session_lock_gives_up_after_its_wait() {
        let dir = tempfile::tempdir().unwrap();
        let second = on_short_lived_runtime(async {
            let _held = lock_in(&dir, "kache-cli", Duration::from_secs(1))
                .await
                .unwrap();
            tokio::time::timeout(
                Duration::from_secs(5),
                lock_in(&dir, "kache-cli", Duration::from_millis(100)),
            )
            .await
        })
        .expect("a bounded wait must end");
        assert!(second.is_err());
    }

    #[tokio::test]
    async fn a_fresh_session_needs_no_refresh_slot() {
        let _serial = SESSION_TESTS.lock().await;
        let lock_dir = tempfile::tempdir().unwrap();
        // Another build holds the refresh slot: a fresh session is still sent.
        let _busy = REFRESH.try_lock().unwrap();
        let service = service();
        let store = MemoryStore::default();
        store
            .save(&stored(&service, "fresh", 3600, Some("rt")))
            .unwrap();
        let token = session_token(
            service,
            Box::new(store),
            lock_dir.path().to_path_buf(),
            no_refresh,
        )
        .await
        .unwrap();
        assert_eq!(token.as_deref(), Some("fresh"));
    }

    /// End to end through the real per-user stores. Linux only: there the
    /// token store (`dirs::config_dir()`, from `XDG_CONFIG_HOME`) and the pin
    /// store (`dirs::home_dir()`, from `HOME`) both follow the environment, so
    /// they land in a temporary directory instead of the developer's home.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_logged_in_planner_gets_the_stored_session() {
        let _process = crate::test_support::process_state_test_lock();
        crate::planner_client::ensure_crypto_provider();
        let home = tempfile::tempdir().unwrap();
        let _env = EnvGuard::set(&[
            (
                "XDG_CONFIG_HOME",
                home.path().join(".config").into_os_string(),
            ),
            ("HOME", home.path().as_os_str().to_owned()),
        ]);
        let (base, _) = stub_planner("https://clerk.example").await;
        TofuStore::new()
            .unwrap()
            .trust(&base, "https://clerk.example", "")
            .unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        ScopedTokenStore::new("kache-cli")
            .unwrap()
            .save(&StoredToken::new(
                "session-id".into(),
                None,
                Some(now + 3600),
                "https://clerk.example".into(),
            ))
            .unwrap();
        let token = kunobi_session_token(&base).await;
        assert_eq!(token.unwrap().as_deref(), Some("session-id"));
    }

    /// Sets environment variables and restores them on drop, even when the
    /// test panics. Callers hold `process_state_test_lock`.
    #[cfg(target_os = "linux")]
    struct EnvGuard(Vec<(&'static str, Option<std::ffi::OsString>)>);

    #[cfg(target_os = "linux")]
    impl EnvGuard {
        fn set(vars: &[(&'static str, std::ffi::OsString)]) -> Self {
            let saved = vars
                .iter()
                .map(|(key, value)| {
                    let previous = std::env::var_os(key);
                    unsafe { std::env::set_var(key, value) };
                    (*key, previous)
                })
                .collect();
            Self(saved)
        }
    }

    #[cfg(target_os = "linux")]
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, value) in self.0.drain(..) {
                unsafe {
                    match value {
                        Some(value) => std::env::set_var(key, value),
                        None => std::env::remove_var(key),
                    }
                }
            }
        }
    }
}
