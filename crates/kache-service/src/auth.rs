//! Planner authentication: which bearers the service accepts.
//!
//! Three optional providers, each enabled by its own settings:
//! - a static shared token (`--token`), for self-hosters;
//! - Kunobi (Clerk) ID tokens from `kache login`, bound to the CLI client id;
//! - GitHub Actions ID tokens, bound to an audience and trusted only for the
//!   listed repository owners.
//!
//! With none configured the planner stays open, as it always has.

use anyhow::{Result, bail};
use kunobi_auth::{
    AuthError, AuthIdentity, KunobiAuthDiscovery,
    server::{AuthBuilder, AuthnProvider, ConfiguredAuth, JwtAuthConfig},
};

pub const GITHUB_ISSUER: &str = "https://token.actions.githubusercontent.com";
pub const TOKEN_PROVIDER: &str = "kache";
pub const KUNOBI_PROVIDER: &str = "kunobi";
pub const GITHUB_PROVIDER: &str = "github-actions";

/// Both issuers sign with RS256.
const PRODUCTION_ALGORITHMS: &[&str] = &["RS256"];

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuthSettings {
    pub token: Option<String>,
    pub oidc_issuer: Option<String>,
    pub oidc_client_id: Option<String>,
    pub github_audience: Option<String>,
    pub github_owners: Vec<String>,
}

fn clean(value: Option<String>) -> Option<String> {
    value
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

impl AuthSettings {
    /// Trim values, drop empty ones, and reject combinations that would
    /// silently weaken or disable a provider.
    pub fn validated(self) -> Result<Self> {
        let settings = AuthSettings {
            token: clean(self.token),
            oidc_issuer: clean(self.oidc_issuer).map(|i| i.trim_end_matches('/').to_string()),
            oidc_client_id: clean(self.oidc_client_id),
            github_audience: clean(self.github_audience),
            github_owners: self
                .github_owners
                .into_iter()
                .map(|owner| owner.trim().to_string())
                .filter(|owner| !owner.is_empty())
                .collect(),
        };
        if settings.oidc_issuer.is_some() != settings.oidc_client_id.is_some() {
            bail!("--oidc-issuer and --oidc-client-id must be set together");
        }
        if settings.github_audience.is_some() && settings.github_owners.is_empty() {
            bail!(
                "--github-oidc-audience needs at least one --github-owner; \
                 an empty allow-list would trust every GitHub repository"
            );
        }
        Ok(settings)
    }

    pub fn is_enabled(&self) -> bool {
        self.token.is_some() || self.oidc_issuer.is_some() || self.github_audience.is_some()
    }

    /// What `/.well-known/kunobi-auth` advertises, when people can log in.
    pub fn discovery(&self) -> Option<KunobiAuthDiscovery> {
        Some(KunobiAuthDiscovery {
            issuer: self.oidc_issuer.clone()?,
            client_id: self.oidc_client_id.clone()?,
            // The CLI forwards an advertised audience to the IdP. An ID token's
            // `aud` is already the client id, and Clerk rejects a requested
            // audience it has not whitelisted, so advertise none.
            audience: None,
        })
    }
}

/// The configured providers plus the GitHub owner allow-list.
#[derive(Clone)]
pub struct PlannerAuth {
    inner: ConfiguredAuth,
    github_owners: Vec<String>,
}

impl PlannerAuth {
    /// `None` when no provider is configured (the planner stays open).
    pub fn from_settings(settings: &AuthSettings) -> Option<Self> {
        Self::build(settings, GITHUB_ISSUER, PRODUCTION_ALGORITHMS)
    }

    fn build(settings: &AuthSettings, github_issuer: &str, algorithms: &[&str]) -> Option<Self> {
        if !settings.is_enabled() {
            return None;
        }
        let algorithms: Vec<String> = algorithms.iter().map(|a| a.to_string()).collect();
        let mut builder = AuthBuilder::new();
        if let Some(token) = &settings.token {
            builder = builder.static_token(TOKEN_PROVIDER, token.clone(), "planner-client");
        }
        if let (Some(issuer), Some(client_id)) = (&settings.oidc_issuer, &settings.oidc_client_id) {
            builder = builder.jwt(
                JwtAuthConfig::oidc(
                    KUNOBI_PROVIDER,
                    issuer.clone(),
                    format!("{issuer}/.well-known/jwks.json"),
                    vec![client_id.clone()],
                )
                .algorithms(algorithms.clone()),
            );
        }
        if let Some(audience) = &settings.github_audience {
            let issuer = github_issuer.trim_end_matches('/');
            builder = builder.jwt(
                JwtAuthConfig::oidc(
                    GITHUB_PROVIDER,
                    issuer,
                    format!("{issuer}/.well-known/jwks"),
                    vec![audience.clone()],
                )
                .algorithms(algorithms),
            );
        }
        Some(PlannerAuth {
            inner: builder.build(),
            github_owners: settings.github_owners.clone(),
        })
    }

    pub async fn authenticate(&self, token: &str) -> Result<AuthIdentity, AuthError> {
        let identity = self.inner.authenticate(token).await?;
        if identity.provider == GITHUB_PROVIDER {
            let owner = identity
                .claims
                .get("repository_owner")
                .and_then(|value| value.as_str());
            let allowed = owner.is_some_and(|owner| {
                self.github_owners
                    .iter()
                    .any(|allowed| allowed.eq_ignore_ascii_case(owner))
            });
            if !allowed {
                return Err(AuthError::Unauthorized(
                    "GitHub repository owner is not allowed".to_string(),
                ));
            }
        }
        Ok(identity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, routing::get};
    use jsonwebtoken::{EncodingKey, Header};
    use serde_json::{Value, json};
    use std::time::{SystemTime, UNIX_EPOCH};

    // A throwaway P-256 key (kunobi-auth's own JWT test fixture).
    const KID: &str = "kache-test-key";
    const PRIV_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgjCZ3enwwbi1sTMaE
CIAe12xZratKWzRoekhOUBIDCZChRANCAAQitjpgInyqDv9dQ4D0FZ4SiZX+KaqP
4uS/qxtTQoPfLryamFKS8SYa/uu0hcS+ASwxyTxsMBNuMpdBBC+mLBOO
-----END PRIVATE KEY-----
";
    const X: &str = "IrY6YCJ8qg7_XUOA9BWeEomV_imqj-Lkv6sbU0KD3y4";
    const Y: &str = "vJqYUpLxJhr-67SFxL4BLDHJPGwwE24yl0EEL6YsE44";

    /// Serves the test JWKS at both paths the providers use; returns the
    /// base URL, which doubles as the issuer.
    async fn test_idp() -> String {
        let jwks = || async {
            Json(json!({"keys": [{
                "kty": "EC", "crv": "P-256", "kid": KID, "alg": "ES256", "use": "sig",
                "x": X, "y": Y,
            }]}))
        };
        let app = Router::new()
            .route("/.well-known/jwks.json", get(jwks))
            .route("/.well-known/jwks", get(jwks));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    fn sign(claims: Value) -> String {
        kunobi_auth::ensure_crypto_provider();
        let mut header = Header::new(jsonwebtoken::Algorithm::ES256);
        header.kid = Some(KID.to_string());
        let key = EncodingKey::from_ec_pem(PRIV_PEM.as_bytes()).unwrap();
        jsonwebtoken::encode(&header, &claims, &key).unwrap()
    }

    fn exp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 600
    }

    fn settings(issuer: &str) -> AuthSettings {
        AuthSettings {
            token: Some("shared".into()),
            oidc_issuer: Some(issuer.to_string()),
            oidc_client_id: Some("kache-cli".into()),
            github_audience: Some("kache".into()),
            github_owners: vec!["Zondax".into(), "kunobi-ninja".into()],
        }
    }

    fn auth(settings: &AuthSettings, github_issuer: &str) -> PlannerAuth {
        PlannerAuth::build(settings, github_issuer, &["ES256"]).unwrap()
    }

    #[test]
    fn validated_rejects_issuer_without_client_id() {
        let err = AuthSettings {
            oidc_issuer: Some("https://idp".into()),
            ..Default::default()
        }
        .validated()
        .unwrap_err();
        assert!(err.to_string().contains("set together"), "{err}");
    }

    #[test]
    fn validated_rejects_client_id_without_issuer() {
        let err = AuthSettings {
            oidc_client_id: Some("cli".into()),
            ..Default::default()
        }
        .validated()
        .unwrap_err();
        assert!(err.to_string().contains("set together"), "{err}");
    }

    #[test]
    fn validated_rejects_github_audience_without_owners() {
        let err = AuthSettings {
            github_audience: Some("kache".into()),
            github_owners: vec!["  ".into()],
            ..Default::default()
        }
        .validated()
        .unwrap_err();
        assert!(err.to_string().contains("--github-owner"), "{err}");
    }

    #[test]
    fn validated_trims_and_drops_empty_values() {
        let settings = AuthSettings {
            token: Some("  ".into()),
            oidc_issuer: Some(" https://idp/ ".into()),
            oidc_client_id: Some(" cli ".into()),
            github_audience: None,
            github_owners: vec![" Zondax ".into(), "".into()],
        }
        .validated()
        .unwrap();
        assert_eq!(settings.token, None);
        assert_eq!(settings.oidc_issuer.as_deref(), Some("https://idp"));
        assert_eq!(settings.oidc_client_id.as_deref(), Some("cli"));
        assert_eq!(settings.github_owners, vec!["Zondax".to_string()]);
        assert!(!AuthSettings::default().is_enabled());
        assert!(PlannerAuth::from_settings(&AuthSettings::default()).is_none());
    }

    #[test]
    fn discovery_needs_a_client_id_and_advertises_no_audience() {
        assert!(AuthSettings::default().discovery().is_none());
        let doc = settings("https://idp").discovery().unwrap();
        assert_eq!(doc.issuer, "https://idp");
        assert_eq!(doc.client_id, "kache-cli");
        assert_eq!(doc.audience, None);
    }

    #[tokio::test]
    async fn static_token_still_authenticates() {
        let issuer = test_idp().await;
        let identity = auth(&settings(&issuer), &issuer)
            .authenticate("shared")
            .await
            .unwrap();
        assert_eq!(identity.identity, "planner-client");
    }

    #[tokio::test]
    async fn kunobi_token_with_client_id_audience_is_accepted() {
        let issuer = test_idp().await;
        let token = sign(json!({"iss": issuer, "aud": "kache-cli", "sub": "user_1", "exp": exp()}));
        let identity = auth(&settings(&issuer), "http://unused.invalid")
            .authenticate(&token)
            .await
            .unwrap();
        assert_eq!(identity.provider, KUNOBI_PROVIDER);
        assert_eq!(identity.identity, "user_1");
    }

    #[tokio::test]
    async fn kunobi_token_with_other_audience_is_rejected() {
        let issuer = test_idp().await;
        let token = sign(json!({"iss": issuer, "aud": "kobe-cli", "sub": "user_1", "exp": exp()}));
        assert!(
            auth(&settings(&issuer), "http://unused.invalid")
                .authenticate(&token)
                .await
                .is_err()
        );
    }

    fn github_claims(issuer: &str, owner: Option<&str>) -> Value {
        let mut claims = json!({
            "iss": issuer, "aud": "kache", "sub": "repo:Zondax/x:ref:refs/heads/main",
            "exp": exp(),
        });
        if let Some(owner) = owner {
            claims["repository_owner"] = json!(owner);
        }
        claims
    }

    #[tokio::test]
    async fn github_token_from_allowed_owner_is_accepted() {
        let issuer = test_idp().await;
        let token = sign(github_claims(&issuer, Some("zondax")));
        let identity = auth(&settings("http://unused.invalid"), &issuer)
            .authenticate(&token)
            .await
            .unwrap();
        assert_eq!(identity.provider, GITHUB_PROVIDER);
    }

    #[tokio::test]
    async fn github_token_from_other_owner_is_rejected() {
        let issuer = test_idp().await;
        let token = sign(github_claims(&issuer, Some("evil")));
        assert!(
            auth(&settings("http://unused.invalid"), &issuer)
                .authenticate(&token)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn github_token_without_owner_claim_is_rejected() {
        let issuer = test_idp().await;
        let token = sign(github_claims(&issuer, None));
        assert!(
            auth(&settings("http://unused.invalid"), &issuer)
                .authenticate(&token)
                .await
                .is_err()
        );
    }
}
