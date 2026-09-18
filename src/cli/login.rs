//! `kache login` / `kache logout`: a Kunobi session for the planner.
//!
//! The planner advertises its issuer and client id at
//! `/.well-known/kunobi-auth`. Login runs the browser (PKCE) or device flow
//! against that issuer and stores the session in kunobi-auth's per-issuer
//! token store, where the daemon picks it up (see `planner_auth`).

use anyhow::{Context, Result};
use kunobi_auth::client::{
    AuthClient, ServiceConfig, StoredToken, TofuResult, TofuStore, TokenStorage,
    oidc::DeviceFlowPrompt,
};

const DEVICE_SCOPE: &str = "openid profile email offline_access";

pub fn login(device: bool, retrust: bool) -> Result<()> {
    let endpoint = planner_endpoint()?;
    runtime()?.block_on(async {
        crate::planner_client::ensure_crypto_provider();
        println!("Discovering the planner's login at {endpoint}...");
        let service = discover_for_login(&endpoint, retrust, &TofuStore::new()?).await?;
        // Log in without holding any lock (the user may take a while), into
        // a throwaway store; then save under the session lock, so a daemon
        // refresh cannot overwrite the new session with one from an old token.
        let client = AuthClient::with_storage(service.clone(), Box::new(Discard));
        let session = if device {
            client
                .device_login(DEVICE_SCOPE, |prompt| {
                    let _ = write_device_prompt(&mut std::io::stderr(), prompt);
                })
                .await?
        } else {
            println!("Opening the browser to log in...");
            client.login().await?
        };
        let _session = session_lock(&service).await?;
        crate::planner_auth::ScopedTokenStore::new(&service.client_id)?.save(&session)?;
        println!("Logged in to the planner at {endpoint}.");
        Ok(())
    })
}

pub fn logout() -> Result<()> {
    let endpoint = planner_endpoint()?;
    runtime()?.block_on(async {
        crate::planner_client::ensure_crypto_provider();
        let service = kunobi_auth::client::discover(&endpoint).await?;
        let _session = session_lock(&service).await?;
        let issuer = service.issuer.clone();
        let store = crate::planner_auth::ScopedTokenStore::new(&service.client_id)?;
        // Revoking at the IdP is best-effort and bounded; forgetting the
        // session locally always happens, so a slow IdP cannot leave it usable.
        let revoked = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            scoped_client(service)?.logout_async(),
        )
        .await;
        store.remove(&issuer)?;
        match revoked {
            Ok(Ok(())) => println!(
                "Logged out of the planner at {endpoint}: the local session is gone and its revocation was requested at the IdP (best effort)."
            ),
            Ok(Err(error)) => eprintln!(
                "Logged out of the planner at {endpoint}; revoking the session at the IdP failed: {error:#}"
            ),
            Err(_) => eprintln!(
                "Logged out of the planner at {endpoint}; revoking the session at the IdP timed out"
            ),
        }
        Ok(())
    })
}

/// Keeps nothing: `kache login` saves the session it gets back itself, under
/// the session lock.
struct Discard;

impl TokenStorage for Discard {
    fn load(&self, _issuer: &str) -> Result<Option<StoredToken>> {
        Ok(None)
    }
    fn save(&self, _token: &StoredToken) -> Result<()> {
        Ok(())
    }
    fn remove(&self, _issuer: &str) -> Result<()> {
        Ok(())
    }
}

/// Sessions are stored per issuer and client id, so logging in here never
/// replaces another Kunobi tool's session for the same issuer.
fn scoped_client(service: ServiceConfig) -> Result<AuthClient> {
    let store = crate::planner_auth::ScopedTokenStore::new(&service.client_id)?;
    Ok(AuthClient::with_storage(service, Box::new(store)))
}

async fn session_lock(service: &ServiceConfig) -> Result<std::fs::File> {
    crate::planner_auth::session_lock(
        &service.issuer,
        &service.client_id,
        std::time::Duration::from_secs(60),
    )
    .await
    .context("waiting for the daemon to finish refreshing the planner session")
}

fn planner_endpoint() -> Result<String> {
    let config = crate::config::Config::load_planner_config().context(
        "no planner is configured; set KACHE_PLANNER_ENDPOINT or `cache.planner.endpoint` first",
    )?;
    Ok(crate::planner_auth::planner_base(&config.endpoint))
}

fn runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("starting the login runtime")
}

async fn discover_for_login(
    endpoint: &str,
    retrust: bool,
    pins: &TofuStore,
) -> Result<ServiceConfig> {
    if !retrust {
        return kunobi_auth::client::discover_with_store(endpoint, pins)
            .await
            .map_err(|error| {
                if error.to_string().contains("TOFU:") {
                    error.context(
                        "the planner's login configuration changed since it was first trusted; \
                         if that is expected, run `kache login --retrust`",
                    )
                } else {
                    error
                }
            });
    }
    let service = kunobi_auth::client::discover_unpinned(endpoint).await?;
    if let Some(change) = retrust_pin(pins, &service)? {
        eprintln!("{change}");
    }
    Ok(service)
}

/// Pin the planner's current issuer and audience, which the user asked for
/// with `--retrust`. Returns a description of the change when the pin moved.
fn retrust_pin(store: &TofuStore, service: &ServiceConfig) -> Result<Option<String>> {
    let endpoint = &service.endpoint;
    let issuer = &service.issuer;
    let audience = service.audience.as_deref().unwrap_or("");
    let change = match store.verify(endpoint, issuer, audience)? {
        TofuResult::Trusted => return Ok(None),
        TofuResult::FirstConnect { .. } => None,
        TofuResult::IssuerChanged {
            previous, current, ..
        } => Some(format!(
            "Re-pinned the login issuer for {endpoint}: {previous:?} -> {current:?}"
        )),
        TofuResult::AudienceChanged {
            previous, current, ..
        } => Some(format!(
            "Re-pinned the login audience for {endpoint}: {previous:?} -> {current:?}"
        )),
        other => anyhow::bail!("unexpected trust result for {endpoint}: {other:?}"),
    };
    store.trust(endpoint, issuer, audience)?;
    Ok(change)
}

fn write_device_prompt(
    out: &mut impl std::io::Write,
    prompt: &DeviceFlowPrompt,
) -> std::io::Result<()> {
    writeln!(out)?;
    match &prompt.verification_uri_complete {
        Some(complete) => {
            writeln!(out, "  Open this URL in any browser:\n    {complete}\n")?;
            writeln!(
                out,
                "  Or visit {} and enter code: {}",
                prompt.verification_uri, prompt.user_code
            )?;
        }
        None => {
            writeln!(
                out,
                "  Open this URL in any browser:\n    {}\n",
                prompt.verification_uri
            )?;
            writeln!(out, "  Then enter code: {}", prompt.user_code)?;
        }
    }
    writeln!(
        out,
        "\n  The code expires in {} seconds. Waiting...\n",
        prompt.expires_in.as_secs()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service(issuer: &str) -> ServiceConfig {
        ServiceConfig::new("https://planner.example", issuer, "kache-cli")
    }

    fn store(directory: &tempfile::TempDir) -> TofuStore {
        TofuStore::with_path(directory.path().join("known.json"))
    }

    #[test]
    fn retrust_moves_a_changed_issuer() {
        let directory = tempfile::tempdir().unwrap();
        let store = store(&directory);
        store
            .trust("https://planner.example", "https://old.example", "")
            .unwrap();
        assert!(
            store
                .check_and_pin("https://planner.example", "https://idp.example", "")
                .is_err(),
            "without --retrust the pinned discovery refuses the new issuer"
        );

        let change = retrust_pin(&store, &service("https://idp.example"))
            .unwrap()
            .unwrap();
        assert!(
            change.contains("\"https://old.example\" -> \"https://idp.example\""),
            "{change}"
        );
        store
            .check_and_pin("https://planner.example", "https://idp.example", "")
            .unwrap();
    }

    #[test]
    fn retrust_moves_a_changed_audience() {
        let directory = tempfile::tempdir().unwrap();
        let store = store(&directory);
        store
            .trust(
                "https://planner.example",
                "https://idp.example",
                "kache-cli",
            )
            .unwrap();
        let change = retrust_pin(&store, &service("https://idp.example"))
            .unwrap()
            .unwrap();
        assert!(change.contains("audience"), "{change}");
    }

    #[test]
    fn retrust_is_silent_when_the_pin_matches_or_is_new() {
        let directory = tempfile::tempdir().unwrap();
        let store = store(&directory);
        assert_eq!(
            retrust_pin(&store, &service("https://idp.example")).unwrap(),
            None
        );
        assert_eq!(
            retrust_pin(&store, &service("https://idp.example")).unwrap(),
            None
        );
    }

    fn prompt(complete: Option<&str>) -> DeviceFlowPrompt {
        DeviceFlowPrompt {
            verification_uri: "https://idp.example/device".into(),
            verification_uri_complete: complete.map(str::to_string),
            user_code: "ABCD-EFGH".into(),
            expires_in: std::time::Duration::from_secs(599),
        }
    }

    fn rendered(prompt: &DeviceFlowPrompt) -> String {
        let mut out = Vec::new();
        write_device_prompt(&mut out, prompt).unwrap();
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn the_device_prompt_shows_where_to_go_and_the_code() {
        let with_link = rendered(&prompt(Some("https://idp.example/device?code=ABCD-EFGH")));
        assert!(
            with_link.contains("https://idp.example/device?code=ABCD-EFGH"),
            "{with_link}"
        );
        assert!(with_link.contains("ABCD-EFGH"), "{with_link}");
        assert!(with_link.contains("599 seconds"), "{with_link}");
        let plain = rendered(&prompt(None));
        assert!(plain.contains("https://idp.example/device"), "{plain}");
        assert!(plain.contains("Then enter code: ABCD-EFGH"), "{plain}");
    }

    /// A planner that advertises a login at `/.well-known/kunobi-auth`.
    async fn stub_planner(issuer: &'static str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body = serde_json::json!({"issuer": issuer, "clientId": "kache-cli"}).to_string();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
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
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn a_changed_issuer_needs_retrust() {
        crate::planner_client::ensure_crypto_provider();
        let planner = stub_planner("https://clerk.example").await;
        let directory = tempfile::tempdir().unwrap();
        let pins = store(&directory);
        pins.trust(&planner, "https://old.example", "").unwrap();

        let refused = discover_for_login(&planner, false, &pins)
            .await
            .unwrap_err();
        assert!(format!("{refused:#}").contains("--retrust"), "{refused:#}");

        let service = discover_for_login(&planner, true, &pins).await.unwrap();
        assert_eq!(service.issuer, "https://clerk.example");
        // Re-pinned: a plain login now succeeds.
        discover_for_login(&planner, false, &pins).await.unwrap();
    }

    /// Point kache at an empty config and set (or clear) the planner endpoint
    /// for the duration of a test.
    struct PlannerEnv {
        _lock: crate::test_support::ProcessStateTestGuard,
        _dir: tempfile::TempDir,
        saved: Vec<(&'static str, Option<std::ffi::OsString>)>,
    }

    impl PlannerEnv {
        fn new(endpoint: Option<&str>) -> Self {
            let lock = crate::test_support::process_state_test_lock();
            let dir = tempfile::tempdir().unwrap();
            let config = dir.path().join("config.toml");
            let mut saved = Vec::new();
            for (key, value) in [
                ("KACHE_CONFIG", Some(config.as_os_str().to_owned())),
                (
                    "KACHE_PLANNER_ENDPOINT",
                    endpoint.map(std::ffi::OsString::from),
                ),
            ] {
                saved.push((key, std::env::var_os(key)));
                unsafe {
                    match value {
                        Some(value) => std::env::set_var(key, value),
                        None => std::env::remove_var(key),
                    }
                }
            }
            Self {
                _lock: lock,
                _dir: dir,
                saved,
            }
        }
    }

    impl Drop for PlannerEnv {
        fn drop(&mut self) {
            for (key, value) in self.saved.drain(..) {
                unsafe {
                    match value {
                        Some(value) => std::env::set_var(key, value),
                        None => std::env::remove_var(key),
                    }
                }
            }
        }
    }

    #[test]
    fn login_and_logout_need_a_planner() {
        let _env = PlannerEnv::new(None);
        for result in [login(false, false), logout()] {
            let error = result.unwrap_err();
            assert!(
                format!("{error:#}").contains("no planner is configured"),
                "{error:#}"
            );
        }
    }

    #[test]
    fn the_login_endpoint_is_the_planner_base_url() {
        let _env = PlannerEnv::new(Some("https://planner.example/v1/prefetch-plan"));
        assert_eq!(planner_endpoint().unwrap(), "https://planner.example");
    }
}
