//! `kache login` / `kache logout`: a Kunobi session for the planner.
//!
//! The planner advertises its issuer and client id at
//! `/.well-known/kunobi-auth`. Login runs the browser (PKCE) or device flow
//! against that issuer and stores the session in kunobi-auth's per-issuer
//! token store, where the daemon picks it up (see `planner_auth`).

use anyhow::{Context, Result};
use kunobi_auth::client::{
    AuthClient, ServiceConfig, TofuResult, TofuStore, oidc::DeviceFlowPrompt,
};

const DEVICE_SCOPE: &str = "openid profile email offline_access";

pub fn login(device: bool, retrust: bool) -> Result<()> {
    let endpoint = planner_endpoint()?;
    runtime()?.block_on(async {
        crate::planner_client::ensure_crypto_provider();
        println!("Discovering the planner's login at {endpoint}...");
        let service = discover_for_login(&endpoint, retrust).await?;
        let client = scoped_client(service)?;
        if device {
            client
                .device_login(DEVICE_SCOPE, print_device_prompt)
                .await?;
        } else {
            println!("Opening the browser to log in...");
            client.login().await?;
        }
        println!("Logged in to the planner at {endpoint}.");
        Ok(())
    })
}

pub fn logout() -> Result<()> {
    let endpoint = planner_endpoint()?;
    runtime()?.block_on(async {
        crate::planner_client::ensure_crypto_provider();
        let service = kunobi_auth::client::discover(&endpoint).await?;
        scoped_client(service)?.logout_async().await?;
        println!("Logged out of the planner at {endpoint} (session revoked at the IdP).");
        Ok(())
    })
}

/// Sessions are stored per issuer and client id, so logging in here never
/// replaces another Kunobi tool's session for the same issuer.
fn scoped_client(service: ServiceConfig) -> Result<AuthClient> {
    let store = crate::planner_auth::ScopedTokenStore::new(&service.client_id)?;
    Ok(AuthClient::with_storage(service, Box::new(store)))
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

async fn discover_for_login(endpoint: &str, retrust: bool) -> Result<ServiceConfig> {
    if !retrust {
        return kunobi_auth::client::discover(endpoint)
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
    if let Some(change) = retrust_pin(&TofuStore::new()?, &service)? {
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

fn print_device_prompt(prompt: &DeviceFlowPrompt) {
    eprintln!();
    match &prompt.verification_uri_complete {
        Some(complete) => {
            eprintln!("  Open this URL in any browser:\n    {complete}\n");
            eprintln!(
                "  Or visit {} and enter code: {}",
                prompt.verification_uri, prompt.user_code
            );
        }
        None => {
            eprintln!(
                "  Open this URL in any browser:\n    {}\n",
                prompt.verification_uri
            );
            eprintln!("  Then enter code: {}", prompt.user_code);
        }
    }
    eprintln!(
        "\n  The code expires in {} seconds. Waiting...\n",
        prompt.expires_in.as_secs()
    );
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
}
