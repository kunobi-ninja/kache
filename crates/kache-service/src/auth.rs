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

    // A throwaway RSA-2048 key, for the RS256 path Clerk and GitHub use.
    const RSA_KID: &str = "kache-test-rsa";
    const RSA_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDgrkZGAG57hb1e
Xjbcfe+87WReOoq9dirBzKFl2IUcbhcQ+HCmk8dHj9mcBGBLf0MhPpcE/Sh62j/f
/i9vHIB+DoMAJc2M4FA6fNptgcsN+aq6uQyZdDgYeG3HvLGs7skxeCcEXlxzmUVG
RO9GoQ2zmdjsg+gMMpyH5idAXeYZRLdpCZhImXIoa6VH/n3sJ/4x4JuBBY6I/NMG
zYTCnUGPJ7LTWOZj+jxfcGM2QdLP4SPJh/ctiCnDYAvmJxcFkp/Bh3Y1Ms3p04SP
5SbFs83mOgCHgpUyG9QFlt5UA8dVQSWne/EguYFvxyAPkjAUhU8F5D0aXtU2j+Pm
yTRWHxhrAgMBAAECggEAS3Hpsf7aGos9m+8J//8f3fJe9g81YEpKMDpc5dyPUg0b
nLy34w+TZpHEshF8Hk3VBlhEkM3LZnlb4oSxe993vBx2W7nV7Fy0Q/eBxyzCuOYh
sMKLmwm9/mWOA3h2twmVsJFWlK503/JTIzXpnO9esPTTtRPT+yiRjEa53nDJX6kk
xGJg7aN7eG7SPtM5ZonqMdMWYMd/TNUpYJdTbByUjWcRaMW2ak7yR3lVxIVpxW0v
NqpFQtQ2SxdWK9HZYTAdOvm8xRPjArxrno2ulB+jvBIvKAUclMNMQHNcMQoCRSXO
gngAqqByrCA7ZrH4CCF/xeyKPYw50eBUKikzyowrCQKBgQD3B/x7/NtqLwzSwBx+
1zDPOFUA4yJPwycBjzCFwlIK/CF+whogE1euM3kooU+VUyx37mtQjAWSqWvTGIcn
/eTeJ3nA1FCNNNJT9BrwMr+IGH0kfRdrxcYg2nid25weDil3XmVFJhYgS11sAYtF
dny6CexJCihoJ/BL3oBUfhDuaQKBgQDo1o2/5rGliYoopcwWx+rBvqCI+hp0Q1Hu
7Lg2D34AKVfwQsnWm01ypi+Nes+PHIZ1N8G5Fdtt8kXG/B8OVu8WdQU/nthCURiX
WFU2puP3B2K5l921xqGF61ViapgkyFzNRD/GcUT7CIqhSTGPZYU5XT1NGExBuSkP
SBfCv0KdswKBgQDL2ItAxSdKOAkc3+qjwG9Gj/WVBdkKeL93SfGAbxnEdsotD37/
ePahiVgxbut9DWVkkhl8Hg97NtMDHvpoxdpWd7zAonLWEwB8xrJ0A2yJoauisJbj
GBCmi6F8ofvx0T3mgr6OkR8xBv/QYHXMnDnQSt/wZbFyQYLJGJJNJrpSAQKBgHw6
1aC5Hrma++saIx1Yh0neQtsyzmCP9qxLAzJc67Z8WJ4plHV7oUP1pVOQXiQWbSsn
l6YAIna0GETGCm/lNGXABA/g2bPwfvVpPPYO73zDJQqvG608ELxaRtFe3FRrzw/6
RSjFEK/767OIUPgYi4+Czw1OVImQmHa92WegEpP/AoGAcA/+gNO17Byg1HYcw7PM
CKsAI1KGZ5NmzVlXyS5ZJ2kacSNYU/mS6/luQ4hUUcHZWhcF6rFe9fgDRM6MUTv/
J6xTV3yXyjfPzsyg25edgkVLvHhvPy+/uwjddn15qhr8qlRF4OUnFfghTnOBynog
3wVBRqOg4tMbRcesdYgYwWw=
-----END PRIVATE KEY-----
";
    const RSA_N: &str = "4K5GRgBue4W9Xl423H3vvO1kXjqKvXYqwcyhZdiFHG4XEPhwppPHR4_ZnARgS39DIT6XBP0oeto_3_4vbxyAfg6DACXNjOBQOnzabYHLDfmqurkMmXQ4GHhtx7yxrO7JMXgnBF5cc5lFRkTvRqENs5nY7IPoDDKch-YnQF3mGUS3aQmYSJlyKGulR_597Cf-MeCbgQWOiPzTBs2Ewp1Bjyey01jmY_o8X3BjNkHSz-EjyYf3LYgpw2AL5icXBZKfwYd2NTLN6dOEj-UmxbPN5joAh4KVMhvUBZbeVAPHVUElp3vxILmBb8cgD5IwFIVPBeQ9Gl7VNo_j5sk0Vh8Yaw";

    /// Serves the test JWKS at both paths the providers use; returns the
    /// base URL, which doubles as the issuer.
    async fn test_idp() -> String {
        let jwks = || async {
            Json(json!({"keys": [{
                "kty": "EC", "crv": "P-256", "kid": KID, "alg": "ES256", "use": "sig",
                "x": X, "y": Y,
            }, {
                "kty": "RSA", "kid": RSA_KID, "alg": "RS256", "use": "sig",
                "n": RSA_N, "e": "AQAB",
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

    fn sign_rs256(claims: Value) -> String {
        kunobi_auth::ensure_crypto_provider();
        let mut header = Header::new(jsonwebtoken::Algorithm::RS256);
        header.kid = Some(RSA_KID.to_string());
        let key = EncodingKey::from_rsa_pem(RSA_PEM.as_bytes()).unwrap();
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
    fn each_provider_alone_enables_auth() {
        let token = AuthSettings {
            token: Some("t".into()),
            ..Default::default()
        };
        let oidc = AuthSettings {
            oidc_issuer: Some("https://idp".into()),
            oidc_client_id: Some("cli".into()),
            ..Default::default()
        };
        let github = AuthSettings {
            github_audience: Some("kache".into()),
            github_owners: vec!["Zondax".into()],
            ..Default::default()
        };
        for settings in [token, oidc, github] {
            assert!(settings.is_enabled(), "{settings:?}");
        }
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

    /// The production algorithm list: what Clerk and GitHub actually sign.
    fn production(settings: &AuthSettings, github_issuer: &str) -> PlannerAuth {
        PlannerAuth::build(settings, github_issuer, PRODUCTION_ALGORITHMS).unwrap()
    }

    #[tokio::test]
    async fn rs256_kunobi_and_github_tokens_pass_the_production_algorithms() {
        let issuer = test_idp().await;
        let auth = production(&settings(&issuer), &issuer);
        let kunobi =
            sign_rs256(json!({"iss": issuer, "aud": "kache-cli", "sub": "user_1", "exp": exp()}));
        assert_eq!(
            auth.authenticate(&kunobi).await.unwrap().provider,
            KUNOBI_PROVIDER
        );
        let github = sign_rs256(github_claims(&issuer, Some("kunobi-ninja")));
        // Same issuer serves both providers here; the audience picks GitHub.
        assert_eq!(
            auth.authenticate(&github).await.unwrap().provider,
            GITHUB_PROVIDER
        );
    }

    #[tokio::test]
    async fn a_token_signed_with_another_algorithm_is_rejected_in_production() {
        let issuer = test_idp().await;
        let token = sign(json!({"iss": issuer, "aud": "kache-cli", "sub": "user_1", "exp": exp()}));
        assert!(
            production(&settings(&issuer), "http://unused.invalid")
                .authenticate(&token)
                .await
                .is_err()
        );
    }
}
