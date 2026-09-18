//! The bearer a planner request carries, resolved without ever prompting.
//!
//! First match wins:
//! 1. an explicit token (`KACHE_PLANNER_TOKEN` / `cache.planner.token`);
//! 2. inside GitHub Actions, the job's OIDC ID token for
//!    `cache.planner.github_audience` (the job needs `id-token: write`);
//! 3. the Kunobi session `kache login` stored for the planner's issuer,
//!    refreshed with its refresh token when it has expired;
//! 4. nothing.
//!
//! The planner is advisory, so every failure here means "no bearer": the
//! request goes out anyway and a 401 lands in the local-planning fallback.

use crate::config::PlannerConfig;
use anyhow::{Context, Result, bail};
use base64::Engine as _;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const GITHUB_REQUEST_URL: &str = "ACTIONS_ID_TOKEN_REQUEST_URL";
const GITHUB_REQUEST_TOKEN: &str = "ACTIONS_ID_TOKEN_REQUEST_TOKEN";
/// Refresh a token this long before it expires, so a request in flight does
/// not carry one that lapses on arrival.
const EXPIRY_MARGIN: Duration = Duration::from_secs(60);
/// How long a discovery result (including "this planner has no login") is
/// reused. The daemon is long-lived; the planner's auth config rarely moves.
const DISCOVERY_TTL: Duration = Duration::from_secs(600);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CachedToken {
    pub token: String,
    pub expires_at: SystemTime,
}

impl CachedToken {
    fn is_fresh(&self) -> bool {
        SystemTime::now() + EXPIRY_MARGIN < self.expires_at
    }
}

static GITHUB_TOKENS: Mutex<Option<HashMap<String, CachedToken>>> = Mutex::new(None);
/// When a planner was asked for its login, and what it answered (`None`: it
/// offers none).
type Discovered = (Instant, Option<kunobi_auth::client::ServiceConfig>);
static DISCOVERY: Mutex<Option<HashMap<String, Discovered>>> = Mutex::new(None);

pub async fn bearer(config: &PlannerConfig) -> Option<String> {
    if let Some(token) = &config.token {
        return Some(token.clone());
    }
    crate::planner_client::ensure_crypto_provider();
    let budget = Duration::from_millis(config.timeout_ms);
    match tokio::time::timeout(budget, resolve(config)).await {
        Ok(token) => token,
        Err(_) => {
            tracing::debug!("planner auth: resolving a bearer timed out; sending none");
            None
        }
    }
}

async fn resolve(config: &PlannerConfig) -> Option<String> {
    if let (Ok(url), Ok(request_token)) = (
        std::env::var(GITHUB_REQUEST_URL),
        std::env::var(GITHUB_REQUEST_TOKEN),
    ) {
        match cached_github_token(&url, &request_token, &config.github_audience).await {
            Ok(token) => return Some(token),
            Err(error) => tracing::debug!("planner auth: GitHub Actions ID token: {error:#}"),
        }
    }
    match kunobi_session_token(&config.endpoint).await {
        Ok(token) => token,
        Err(error) => {
            tracing::debug!("planner auth: Kunobi session ({error:#}); run `kache login`");
            None
        }
    }
}

async fn cached_github_token(url: &str, request_token: &str, audience: &str) -> Result<String> {
    let cached = GITHUB_TOKENS
        .lock()
        .ok()
        .and_then(|cache| cache.as_ref()?.get(audience).cloned())
        .filter(CachedToken::is_fresh);
    if let Some(cached) = cached {
        return Ok(cached.token);
    }
    let fresh = github_actions_token(url, request_token, audience).await?;
    if let Ok(mut cache) = GITHUB_TOKENS.lock() {
        cache
            .get_or_insert_with(HashMap::new)
            .insert(audience.to_string(), fresh.clone());
    }
    Ok(fresh.token)
}

/// Ask the Actions runtime for this job's OIDC ID token.
pub(crate) async fn github_actions_token(
    request_url: &str,
    request_token: &str,
    audience: &str,
) -> Result<CachedToken> {
    #[derive(serde::Deserialize)]
    struct Response {
        value: String,
    }
    let mut url = reqwest::Url::parse(request_url).context("parsing the Actions ID token URL")?;
    url.query_pairs_mut().append_pair("audience", audience);
    let response: Response = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?
        .get(url)
        .bearer_auth(request_token)
        .send()
        .await
        .context("requesting the GitHub Actions ID token")?
        .error_for_status()
        .context(
            "GitHub Actions refused the ID token request (does the job have `id-token: write`?)",
        )?
        .json()
        .await
        .context("decoding the GitHub Actions ID token response")?;
    let expires_at = jwt_expiry(&response.value)?;
    Ok(CachedToken {
        token: response.value,
        expires_at,
    })
}

/// The `exp` of a JWT, read without verifying it: the token is ours to send,
/// not to trust, and only its lifetime matters here.
fn jwt_expiry(jwt: &str) -> Result<SystemTime> {
    let payload = jwt.split('.').nth(1).context("ID token is not a JWT")?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .context("ID token payload is not base64url")?;
    let claims: serde_json::Value =
        serde_json::from_slice(&bytes).context("ID token payload is not JSON")?;
    let exp = claims
        .get("exp")
        .and_then(serde_json::Value::as_u64)
        .context("ID token has no numeric `exp`")?;
    Ok(UNIX_EPOCH + Duration::from_secs(exp))
}

/// The stored `kache login` session for `endpoint`'s issuer, refreshed when
/// expired. `Ok(None)` when the planner offers no login or nobody logged in.
pub(crate) async fn kunobi_session_token(endpoint: &str) -> Result<Option<String>> {
    let Some(service) = discovered(endpoint).await? else {
        return Ok(None);
    };
    let store = kunobi_auth::client::TokenStore::new()?;
    let Some(stored) = store.load(&service.issuer)? else {
        return Ok(None);
    };
    if !stored.is_expired() {
        return Ok(Some(stored.id_token));
    }
    let Some(refresh_token) = stored.refresh_token.as_deref() else {
        bail!("the Kunobi session expired and has no refresh token");
    };
    let mut refreshed = kunobi_auth::client::oidc::refresh(
        &service.issuer,
        &service.client_id,
        &service.redirect_uri,
        refresh_token,
    )
    .await
    .context("refreshing the Kunobi session")?;
    refreshed.extra = stored.extra;
    let token = refreshed.id_token.clone();
    store.save(&refreshed)?;
    Ok(Some(token))
}

/// Pinned discovery of the planner's login, cached for [`DISCOVERY_TTL`]. A
/// planner without `/.well-known/kunobi-auth` caches as `None`, so builds
/// against it do not pay for a discovery request each time.
async fn discovered(endpoint: &str) -> Result<Option<kunobi_auth::client::ServiceConfig>> {
    let key = planner_base(endpoint);
    let cached = DISCOVERY.lock().ok().and_then(|cache| {
        let (at, service) = cache.as_ref()?.get(&key)?;
        (at.elapsed() < DISCOVERY_TTL).then(|| service.clone())
    });
    if let Some(service) = cached {
        return Ok(service);
    }
    let service = match kunobi_auth::client::discover(&key).await {
        Ok(service) => Some(service),
        Err(error) if error.to_string().contains("TOFU:") => {
            bail!("{error:#}; if the change is expected run `kache login --retrust`")
        }
        Err(error) => {
            tracing::debug!("planner auth: no login offered by {key}: {error:#}");
            None
        }
    };
    if let Ok(mut cache) = DISCOVERY.lock() {
        cache
            .get_or_insert_with(HashMap::new)
            .insert(key, (Instant::now(), service.clone()));
    }
    Ok(service)
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
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
    async fn stub_github(token: String) -> (String, Arc<AtomicUsize>) {
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
                assert!(
                    request.starts_with("get /?api-version=2.0&audience=kache "),
                    "{request}"
                );
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

    #[tokio::test]
    async fn explicit_token_wins() {
        let config = PlannerConfig {
            endpoint: "http://127.0.0.1:9".to_string(),
            timeout_ms: 50,
            token: Some("static".to_string()),
            github_audience: "kache".to_string(),
        };
        assert_eq!(bearer(&config).await.as_deref(), Some("static"));
    }

    #[tokio::test]
    async fn github_actions_token_requests_the_audience_and_reads_its_expiry() {
        crate::planner_client::ensure_crypto_provider();
        let exp = in_an_hour();
        let (url, _) = stub_github(jwt_with_exp(exp)).await;
        let token = github_actions_token(&url, "runtime-token", "kache")
            .await
            .unwrap();
        assert_eq!(token.token, jwt_with_exp(exp));
        assert_eq!(token.expires_at, UNIX_EPOCH + Duration::from_secs(exp));
    }

    #[tokio::test]
    async fn github_token_is_cached_until_near_expiry() {
        crate::planner_client::ensure_crypto_provider();
        let (url, hits) = stub_github(jwt_with_exp(in_an_hour())).await;
        // A dedicated audience keeps this test's cache entry to itself.
        let audience = "kache";
        if let Ok(mut cache) = GITHUB_TOKENS.lock() {
            cache.get_or_insert_with(HashMap::new).remove(audience);
        }
        let first = cached_github_token(&url, "runtime-token", audience)
            .await
            .unwrap();
        let second = cached_github_token(&url, "runtime-token", audience)
            .await
            .unwrap();
        assert_eq!(first, second);
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_token_near_expiry_is_not_fresh() {
        let soon = CachedToken {
            token: "t".into(),
            expires_at: SystemTime::now() + Duration::from_secs(30),
        };
        assert!(!soon.is_fresh());
    }

    #[test]
    fn jwt_expiry_rejects_a_non_jwt() {
        assert!(jwt_expiry("not-a-jwt").is_err());
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
        // Nothing listens here: discovery fails, which means "no login".
        let token = kunobi_session_token("http://127.0.0.1:9").await.unwrap();
        assert_eq!(token, None);
    }
}
