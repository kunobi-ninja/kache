# Planner Kunobi OAuth + GitHub OIDC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Authenticate kache planner calls with Kunobi (Clerk) logins for people and GitHub Actions OIDC for CI, then deploy it on zur1 behind the VPN ingress.

**Architecture:** kache-service swaps its hand-rolled static-token check for a kunobi-auth `ConfiguredAuth` (static token + Clerk JWT + GitHub JWT) plus an owner allow-list, and serves `/.well-known/kunobi-auth`. The kache client gains `kache login/logout` and resolves a planner bearer (explicit token → GitHub Actions ID token → stored Kunobi session), never interactively. kunobi-auth first gets a feature so its client can run on ring TLS.

**Tech Stack:** Rust 2024, axum 0.8, reqwest 0.13 (`rustls-no-provider` + ring), kunobi-auth 0.11 (`server`, `client`, `browser-login`, `rust_crypto`), jsonwebtoken 11 + rsa (tests), Helm, Flux.

**Spec:** `notes/design/2026-09-18-planner-kunobi-oauth-design.md`

## Global Constraints

- Clerk issuer: `https://clerk.kunobi.com`. GitHub issuer: `https://token.actions.githubusercontent.com`.
- Discovery never advertises an audience (`KunobiAuthDiscovery { issuer, client_id, audience: None }`).
- GitHub tokens are accepted only when `repository_owner` is in the configured owner list; an audience without owners is a startup error.
- The kache CLI package must not depend on `aws-lc-sys` (`cargo tree -p kache -i aws-lc-sys` must report no match).
- Planner auth never prompts, opens a browser, or fails a build; any auth failure means "no bearer" and the existing local-planning fallback.
- `/healthz`, `/readyz`, `/metrics` stay unauthenticated.
- Env names: `KACHE_PLANNER_OIDC_ISSUER`, `KACHE_PLANNER_OIDC_CLIENT_ID`, `KACHE_PLANNER_GITHUB_AUDIENCE`, `KACHE_PLANNER_GITHUB_OWNERS` (comma list); client `KACHE_PLANNER_GITHUB_AUDIENCE` / `cache.planner.github_audience` (default `kache`).
- Work from worktrees under `~/Documents/Work/Zondax/Git/worktrees`; run `cargo fmt`, `cargo clippy -- -D warnings` and the touched crates' tests before every PR.

## File Structure

| Repo | File | Responsibility |
|---|---|---|
| kunobi-auth | `Cargo.toml` | `aws-lc-tls` feature; reqwest on `rustls-no-provider` |
| kache | `crates/kache-service/src/auth.rs` (new) | `AuthSettings`, validation, `PlannerAuth` (ConfiguredAuth + owner authz), discovery metadata |
| kache | `crates/kache-service/src/lib.rs` | `PlannerConfig.auth`, `AppState` uses `PlannerAuth`, discovery route |
| kache | `crates/kache-service/src/main.rs` | new flags → `AuthSettings` |
| kache | `packaging/charts/kache-service/{values.yaml,templates/deployment.yaml}` | `auth.oidc`, `auth.githubOidc` → env |
| kache | `src/planner_auth.rs` (new) | bearer resolution: GitHub Actions ID token, stored Kunobi session, in-memory cache |
| kache | `src/planner_client.rs` | uses `planner_auth::bearer` when no explicit token |
| kache | `src/config.rs` | `PlannerConfig.github_audience`, file/env plumbing |
| kache | `src/cli/login.rs` (new), `src/cli.rs`, `src/main.rs` | `kache login [--device] [--retrust]`, `kache logout` |
| kache | `docs/remote-service.mdx`, `docs/getting-started/configuration.mdx` | operator + user docs |
| tenant-int-pro | `by-cluster/zur1-worker1/kache/service/helmrelease.yaml` | auth values + ingress |

---

### Task 1: kunobi-auth — selectable TLS provider

**Files:**
- Modify: `kunobi-auth/Cargo.toml` (reqwest dep, `[features]`)
- Modify: `kunobi-auth/README.md` (feature table row)

**Interfaces:**
- Produces: feature `aws-lc-tls` (in `default`). With `default-features = false` and no `aws-lc-tls`, reqwest is built with `rustls-no-provider`; the consumer installs a rustls `CryptoProvider`.

- [ ] **Step 1: Worktree** — `git -C ~/…/products/kunobi/kunobi-auth worktree add ~/…/worktrees/kunobi-auth-tls -b feat/selectable-tls origin/main`
- [ ] **Step 2: Failing check** — from the worktree:

```bash
cargo tree --no-default-features --features client,browser-login,rust_crypto -i aws-lc-sys
```
Expected today: aws-lc-sys **is** found (the thing we are removing).

- [ ] **Step 3: Change `Cargo.toml`**

```toml
reqwest = { version = "0.13", features = ["json", "form", "rustls-no-provider"], default-features = false }

[features]
default = ["client", "browser-login", "server", "rust_crypto", "aws-lc-tls"]
# TLS for outbound HTTP (discovery, JWKS, token endpoint). Default-on so
# existing consumers keep reqwest's bundled aws-lc-rs provider. Disable
# default features to bring your own rustls CryptoProvider (e.g. ring) and
# install it before the first request.
aws-lc-tls = ["reqwest/rustls"]
```

- [ ] **Step 4: Verify both shapes**

```bash
cargo tree --no-default-features --features client,browser-login,rust_crypto -i aws-lc-sys   # expect: no match
cargo test                                                                                  # default features, all pass
cargo clippy --all-targets -- -D warnings && cargo fmt --check
```

- [ ] **Step 5: README** — add a row to the features table: `aws-lc-tls | yes | reqwest's rustls with the aws-lc-rs provider; disable to supply your own provider (e.g. ring)`.
- [ ] **Step 6: Commit + PR** — `feat: make the reqwest TLS provider selectable (aws-lc-tls feature)`; wait for CI green; squash.

### Task 2: kunobi-auth — release 0.11.2

- [ ] **Step 1:** Follow the repo's release pattern (`chore(release): v0.11.1 (#83)`): bump `version` in `Cargo.toml` to `0.11.2`, update `Cargo.lock`, CHANGELOG entry "aws-lc-tls feature", PR `chore(release): v0.11.2`, squash.
- [ ] **Step 2:** Tag/release per the repo's `publish-crates.yaml` trigger; confirm `https://crates.io/api/v1/crates/kunobi-auth/0.11.2` returns 200.

### Task 3: kache-service — `auth.rs` (settings, validation, owner authz)

**Files:**
- Create: `crates/kache-service/src/auth.rs`
- Modify: `crates/kache-service/src/lib.rs` (`mod auth; pub use auth::AuthSettings;`)
- Modify: `crates/kache-service/Cargo.toml` — `kunobi-auth = { version = "0.11.2", default-features = false, features = ["server", "rust_crypto", "aws-lc-tls"] }`; dev-deps `jsonwebtoken = { version = "11", default-features = false, features = ["rust_crypto", "use_pem"] }`, `rsa = "0.9"`, `base64 = "0.22"`, `rand = "0.8"`.

**Interfaces:**
- Produces:

```rust
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuthSettings {
    pub token: Option<String>,
    pub oidc_issuer: Option<String>,
    pub oidc_client_id: Option<String>,
    pub github_audience: Option<String>,
    pub github_owners: Vec<String>,
}
impl AuthSettings {
    /// Normalized (trimmed, empty → None) and validated.
    pub fn validated(self) -> anyhow::Result<Self>;
    pub fn is_enabled(&self) -> bool;
    pub fn discovery(&self) -> Option<kunobi_auth::KunobiAuthDiscovery>;
}
#[derive(Clone)]
pub struct PlannerAuth { /* ConfiguredAuth + owners */ }
impl PlannerAuth {
    pub fn from_settings(settings: &AuthSettings) -> Option<PlannerAuth>; // None when !is_enabled
    pub fn with_github_issuer(settings: &AuthSettings, github_issuer: &str) -> Option<PlannerAuth>; // tests
    pub async fn authenticate(&self, token: &str) -> Result<AuthIdentity, AuthError>;
}
pub const GITHUB_ISSUER: &str = "https://token.actions.githubusercontent.com";
pub const KUNOBI_PROVIDER: &str = "kunobi";
pub const GITHUB_PROVIDER: &str = "github-actions";
pub const TOKEN_PROVIDER: &str = "kache";
```

- [ ] **Step 1: Write failing tests** in `auth.rs` `#[cfg(test)] mod tests`. Test harness: generate one RSA key (`rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048)`), serve `{"keys":[{"kty":"RSA","kid":"k1","alg":"RS256","use":"sig","n":<b64url n>,"e":<b64url e>}]}` from an axum listener on `127.0.0.1:0` at `/.well-known/jwks.json`, use that listener's URL as the issuer (and as the GitHub issuer via `with_github_issuer`), and sign with `jsonwebtoken::encode(&Header{alg: RS256, kid: Some("k1")}, &claims, &EncodingKey::from_rsa_pem(pem))`. Tests:
  - `validated_rejects_issuer_without_client_id` / `…client_id_without_issuer`
  - `validated_rejects_github_audience_without_owners`
  - `validated_trims_and_drops_empty_values`
  - `discovery_is_none_without_client_id` and `discovery_has_no_audience`
  - `static_token_still_authenticates` (identity `planner-client`)
  - `kunobi_token_with_client_id_audience_is_accepted`
  - `kunobi_token_with_other_audience_is_rejected`
  - `github_token_from_allowed_owner_is_accepted` (claims `repository_owner: "Zondax"`, `aud: "kache"`)
  - `github_token_from_other_owner_is_rejected` (`repository_owner: "evil"`)
  - `github_token_without_owner_claim_is_rejected`
- [ ] **Step 2:** `cargo test -p kache-service --lib auth::` → FAIL (module missing).
- [ ] **Step 3: Implement**

```rust
use anyhow::{Result, bail};
use kunobi_auth::{
    AuthError, AuthIdentity, KunobiAuthDiscovery,
    server::{AuthBuilder, AuthnProvider, ConfiguredAuth},
};

fn clean(v: Option<String>) -> Option<String> {
    v.map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

impl AuthSettings {
    pub fn validated(self) -> Result<Self> {
        let s = AuthSettings {
            token: clean(self.token),
            oidc_issuer: clean(self.oidc_issuer).map(|i| i.trim_end_matches('/').to_string()),
            oidc_client_id: clean(self.oidc_client_id),
            github_audience: clean(self.github_audience),
            github_owners: self.github_owners.into_iter()
                .map(|o| o.trim().to_string()).filter(|o| !o.is_empty()).collect(),
        };
        if s.oidc_issuer.is_some() != s.oidc_client_id.is_some() {
            bail!("--oidc-issuer and --oidc-client-id must be set together");
        }
        if s.github_audience.is_some() && s.github_owners.is_empty() {
            bail!("--github-oidc-audience needs at least one --github-owner; an empty allow-list would trust every repository");
        }
        Ok(s)
    }
    pub fn is_enabled(&self) -> bool {
        self.token.is_some() || self.oidc_issuer.is_some() || self.github_audience.is_some()
    }
    pub fn discovery(&self) -> Option<KunobiAuthDiscovery> {
        Some(KunobiAuthDiscovery {
            issuer: self.oidc_issuer.clone()?,
            client_id: self.oidc_client_id.clone()?,
            // An ID token's `aud` is already the client id; Clerk rejects a
            // requested audience it has not whitelisted (kobe#326).
            audience: None,
        })
    }
}

impl PlannerAuth {
    pub fn from_settings(s: &AuthSettings) -> Option<Self> { Self::with_github_issuer(s, GITHUB_ISSUER) }
    pub fn with_github_issuer(s: &AuthSettings, github_issuer: &str) -> Option<Self> {
        if !s.is_enabled() { return None; }
        let mut b = AuthBuilder::new();
        if let Some(t) = &s.token { b = b.static_token(TOKEN_PROVIDER, t.clone(), "planner-client"); }
        if let (Some(iss), Some(cid)) = (&s.oidc_issuer, &s.oidc_client_id) {
            b = b.oidc(KUNOBI_PROVIDER, iss.clone(), format!("{iss}/.well-known/jwks.json"), vec![cid.clone()]);
        }
        if let Some(aud) = &s.github_audience {
            let iss = github_issuer.trim_end_matches('/');
            b = b.oidc(GITHUB_PROVIDER, iss.to_string(), format!("{iss}/.well-known/jwks"), vec![aud.clone()]);
        }
        Some(PlannerAuth { inner: b.build(), github_owners: s.github_owners.clone() })
    }
    pub async fn authenticate(&self, token: &str) -> Result<AuthIdentity, AuthError> {
        let identity = self.inner.authenticate(token).await?;
        if identity.provider == GITHUB_PROVIDER {
            let owner = identity.claims.get("repository_owner").and_then(|v| v.as_str());
            if !owner.is_some_and(|o| self.github_owners.iter().any(|a| a.eq_ignore_ascii_case(o))) {
                return Err(AuthError::Unauthorized("repository owner not allowed".into()));
            }
        }
        Ok(identity)
    }
}
```
(GitHub publishes its JWKS at `/.well-known/jwks`; the test JWKS server serves both paths.)
- [ ] **Step 4:** `cargo test -p kache-service --lib auth::` → PASS; `cargo clippy -p kache-service --all-targets -- -D warnings`.
- [ ] **Step 5: Commit** — `feat(service): Kunobi and GitHub OIDC planner auth settings`.

### Task 4: kache-service — wire flags, state and discovery

**Files:**
- Modify: `crates/kache-service/src/main.rs` (Cli flags, `planner_config`)
- Modify: `crates/kache-service/src/lib.rs` (`PlannerConfig`, `AppState`, `router`, `serve`, `impl AuthnProvider`, `prefetch_plan`)

**Interfaces:**
- Consumes: `AuthSettings`, `PlannerAuth` (Task 3).
- Produces: `PlannerConfig { bind, auth: AuthSettings, planner_name, db_path, seed_state_file, ha }` (the `token` field moves into `auth.token`).

- [ ] **Step 1: Failing tests**
  - main.rs `cli_fields_map_to_planner_config`: add `--oidc-issuer https://clerk.example --oidc-client-id cli --github-oidc-audience kache --github-owner Zondax --github-owner kunobi-ninja`; expect `auth: AuthSettings{ token: Some("secret"), oidc_issuer: Some(..), oidc_client_id: Some("cli"), github_audience: Some("kache"), github_owners: vec!["Zondax","kunobi-ninja"] }`.
  - main.rs `github_owners_env_is_a_comma_list` (`KACHE_PLANNER_GITHUB_OWNERS=Zondax,kunobi-ninja` via `value_delimiter = ','`).
  - lib.rs `well_known_kunobi_auth_is_served_with_a_client_id` (GET returns `{"issuer":…,"clientId":"cli"}` and no `audience` key) and `well_known_kunobi_auth_is_404_without_one`.
  - Existing tests `prefetch_plan_requires_bearer_token_when_configured`, `…rejects_wrong_bearer_token`, `…accepts…` keep passing via `config.auth.token`.
- [ ] **Step 2:** `cargo test -p kache-service` → FAIL (fields missing).
- [ ] **Step 3: Implement**
  - Cli:
    ```rust
    #[arg(long, env = "KACHE_PLANNER_OIDC_ISSUER")] oidc_issuer: Option<String>,
    #[arg(long, env = "KACHE_PLANNER_OIDC_CLIENT_ID")] oidc_client_id: Option<String>,
    #[arg(long = "github-oidc-audience", env = "KACHE_PLANNER_GITHUB_AUDIENCE")] github_audience: Option<String>,
    #[arg(long = "github-owner", env = "KACHE_PLANNER_GITHUB_OWNERS", value_delimiter = ',')] github_owners: Vec<String>,
    ```
  - `main`: `let config = planner_config(Cli::parse()); config.auth = config.auth.validated()?;` before `serve`.
  - `AppState { auth: Option<PlannerAuth>, … }` built with `PlannerAuth::from_settings(&config.auth)`; `impl AuthnProvider for AppState` becomes: `Some(auth) => auth.authenticate(token).await`, `None => Ok(anonymous identity)` (keep the existing anonymous shape).
  - `prefetch_plan`: `if state.auth.is_some() && identity.is_none() { 401 }` (replaces `state.token.is_some()`).
  - `router`: after `.with_state(state)`, `if let Some(meta) = config.auth.discovery() { router = router.merge(kunobi_auth::server::kunobi_auth_discovery_router(meta)); }` (pass the discovery into `router` alongside state).
  - Remove the now-unused `subtle` dependency if nothing else uses it.
- [ ] **Step 4:** `cargo test -p kache-service`, clippy, fmt → PASS.
- [ ] **Step 5: Commit** — `feat(service): accept Kunobi and GitHub OIDC bearers; serve kunobi-auth discovery`.

### Task 5: Helm chart + operator docs

**Files:**
- Modify: `packaging/charts/kache-service/values.yaml` (`auth.oidc`, `auth.githubOidc`)
- Modify: `packaging/charts/kache-service/templates/deployment.yaml` (env)
- Modify: `packaging/charts/kache-service/templates/NOTES.txt`, `docs/remote-service.mdx`

- [ ] **Step 1: Values**
```yaml
auth:
  existingSecret: ""
  existingSecretKey: token
  # People log in with `kache login` against this issuer; the client id is
  # the accepted `aud` and is advertised at /.well-known/kunobi-auth.
  oidc:
    issuer: ""
    clientId: ""
  # GitHub Actions ID tokens with this audience, from these repository owners.
  githubOidc:
    audience: ""
    owners: []
```
- [ ] **Step 2: Env in deployment.yaml** (inside the planner container `env:`)
```yaml
{{- with .Values.auth.oidc }}
{{- if .issuer }}
- name: KACHE_PLANNER_OIDC_ISSUER
  value: {{ .issuer | quote }}
- name: KACHE_PLANNER_OIDC_CLIENT_ID
  value: {{ required "auth.oidc.clientId is required with auth.oidc.issuer" .clientId | quote }}
{{- end }}
{{- end }}
{{- with .Values.auth.githubOidc }}
{{- if .audience }}
- name: KACHE_PLANNER_GITHUB_AUDIENCE
  value: {{ .audience | quote }}
- name: KACHE_PLANNER_GITHUB_OWNERS
  value: {{ required "auth.githubOidc.owners is required with auth.githubOidc.audience" (join "," .owners) | quote }}
{{- end }}
{{- end }}
```
- [ ] **Step 3: Verify**
```bash
helm template t packaging/charts/kache-service | rg KACHE_PLANNER_ || true          # none by default
helm template t packaging/charts/kache-service --set auth.oidc.issuer=https://clerk.kunobi.com --set auth.oidc.clientId=x \
  --set auth.githubOidc.audience=kache --set 'auth.githubOidc.owners={Zondax,kunobi-ninja}' | rg -A1 'KACHE_PLANNER_(OIDC|GITHUB)'
helm template t packaging/charts/kache-service --set auth.githubOidc.audience=kache 2>&1 | rg 'owners is required'
helm lint packaging/charts/kache-service
```
- [ ] **Step 4: Docs** — `remote-service.mdx`: new "Authentication" section (static token, Kunobi login, GitHub Actions OIDC incl. `permissions: id-token: write`, discovery endpoint); remove "authentication … may still change" caveat only if it refers to this. NOTES.txt: mention OIDC when enabled.
- [ ] **Step 5: Commit** — `feat(chart): kache-service OIDC and GitHub OIDC values`.

### Task 6: kache client — dependency, config, bearer resolution

**Files:**
- Modify: `Cargo.toml` — `kunobi-auth = { version = "0.11.2", default-features = false, features = ["client", "browser-login", "rust_crypto"] }`
- Modify: `src/config.rs` — `PlannerConfig.github_audience: String`, `PlannerFileConfig.github_audience`, env `KACHE_PLANNER_GITHUB_AUDIENCE` (add to `PLANNER_ENV_VARS`), default `DEFAULT_PLANNER_GITHUB_AUDIENCE = "kache"`
- Create: `src/planner_auth.rs`; Modify: `src/main.rs` (`mod planner_auth;`)

**Interfaces:**
- Produces:
```rust
/// Bearer for a planner request, never interactive. `None` = send no Authorization.
pub async fn bearer(config: &PlannerConfig) -> Option<String>;
// test seams
pub(crate) async fn github_actions_token(request_url: &str, request_token: &str, audience: &str) -> anyhow::Result<CachedToken>;
pub(crate) async fn kunobi_session_token(endpoint: &str) -> anyhow::Result<Option<String>>;
pub(crate) struct CachedToken { pub token: String, pub expires_at: std::time::SystemTime }
```

- [ ] **Step 1: Failing tests** (`planner_auth.rs` + config.rs)
  - config: `test_load_planner_config_github_audience_default_and_override` (default `kache`; env override; file override).
  - `explicit_token_wins` — `bearer` returns the configured token without touching env.
  - `github_actions_token_requests_audience` — stub server (tokio `TcpListener`) asserts `GET /?api-version=…&audience=kache` with `Authorization: Bearer <request_token>`, returns `{"value":"<jwt with exp>"}`; result token + `expires_at` from the JWT `exp` (decode payload with base64, no signature check).
  - `github_token_is_cached_until_near_expiry` — second call within validity doesn't hit the server (counter).
  - `no_bearer_when_nothing_is_available` — no env, empty token store dir → `None`.
- [ ] **Step 2:** `cargo test -p kache planner_auth` → FAIL.
- [ ] **Step 3: Implement**
  - GitHub source: if `ACTIONS_ID_TOKEN_REQUEST_URL` and `ACTIONS_ID_TOKEN_REQUEST_TOKEN` are set, GET `"{url}&audience={audience}"` with bearer request token, parse `{"value": String}`, read `exp` from the JWT payload, cache in a `static CACHE: std::sync::Mutex<Option<CachedToken>>` keyed by audience until `exp - 60s`.
  - Kunobi source: `kunobi_auth::client::discover(endpoint)` (pinned; cache the `ServiceConfig` in a `OnceCell`-style static per endpoint for the daemon lifetime), then `TokenStore::new()?.load(&issuer)?`; if not expired → `id_token`; if expired with refresh token → `kunobi_auth::client::oidc::refresh(&issuer, &client_id, &redirect_uri, rt)`, preserve `extra`, `store.save`, return new id token; otherwise `None`.
  - `bearer`: explicit → GitHub → Kunobi → None; each source wrapped so errors are `tracing::debug!` and map to `None`. Whole `bearer` bounded by `tokio::time::timeout(Duration::from_millis(config.timeout_ms), …)`.
- [ ] **Step 4:** tests PASS; `cargo tree -p kache -i aws-lc-sys` → "did not match any packages"; clippy; fmt.
- [ ] **Step 5: Commit** — `feat(planner): resolve a bearer from GitHub Actions OIDC or a kache login session`.

### Task 7: kache client — planner requests use the resolver

**Files:** Modify `src/planner_client.rs`

- [ ] **Step 1: Failing test** — `planner_request_uses_resolved_bearer_when_no_token_is_configured`: set `ACTIONS_ID_TOKEN_REQUEST_*` to a stub, planner stub asserts `Authorization: Bearer <that token>` (reuse `spawn_response_server(body, Some(token), "HTTP/1.1 200 OK")`).
- [ ] **Step 2:** FAIL.
- [ ] **Step 3:** replace
```rust
if let Some(token) = config.token.as_deref() { request = request.bearer_auth(token); }
```
with
```rust
if let Some(token) = crate::planner_auth::bearer(config).await { request = request.bearer_auth(token); }
```
- [ ] **Step 4:** `cargo test -p kache planner_client` PASS (existing explicit-token tests unchanged).
- [ ] **Step 5: Commit** — `feat(planner): send the resolved bearer on prefetch-plan requests`.

### Task 8: `kache login` / `kache logout`

**Files:** Create `src/cli/login.rs`; Modify `src/cli.rs` (`pub mod login;`), `src/main.rs` (`Commands::Login { device: bool, retrust: bool }`, `Commands::Logout`, dispatch), `docs/getting-started/configuration.mdx` (planner auth section)

- [ ] **Step 1: Failing tests** in `login.rs`: `retrust_pin` behaviour copied from kobe (`kobectl/src/commands/login.rs`): ssh/other-issuer pin moves to the new issuer; audience change re-pins; first contact/unchanged silent (same three tests, TofuStore in a tempdir). Plus `login_requires_a_planner_endpoint` (no planner config → error mentioning `KACHE_PLANNER_ENDPOINT`).
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement** — mirror kobe's `login`/`logout`:
```rust
pub fn login(device: bool, retrust: bool) -> Result<()> {
    let config = crate::config::Config::load_planner_config()
        .context("no planner configured; set KACHE_PLANNER_ENDPOINT or cache.planner.endpoint")?;
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    rt.block_on(async {
        crate::planner_client::ensure_crypto_provider();
        let service = if retrust {
            let s = kunobi_auth::client::discover_unpinned(&config.endpoint).await?;
            if let Some(change) = retrust_pin(&kunobi_auth::client::TofuStore::new()?, &s)? { eprintln!("{change}"); }
            s
        } else {
            kunobi_auth::client::discover(&config.endpoint).await
                .map_err(|e| if e.to_string().contains("TOFU:") { e.context("the planner's auth configuration changed; if expected run `kache login --retrust`") } else { e })?
        };
        let client = kunobi_auth::client::AuthClient::new(service)?;
        if device { client.device_login("openid profile email offline_access", print_device_prompt).await?; }
        else { client.login().await?; }
        println!("Logged in to {}", config.endpoint);
        anyhow::Ok(())
    })
}
pub fn logout() -> Result<()> { /* discover → AuthClient::new → logout_async */ }
```
  (make `ensure_crypto_provider` `pub(crate)`.)
- [ ] **Step 4:** tests PASS; `cargo run -- login --help` shows both flags; clippy; fmt.
- [ ] **Step 5: Docs** — configuration.mdx: "Planner authentication": `kache login`, GitHub Actions (`permissions: id-token: write`, `KACHE_PLANNER_GITHUB_AUDIENCE`), token precedence.
- [ ] **Step 6: Commit** — `feat(cli): kache login and logout for the planner`.

### Task 9: kache PR, review, release

- [ ] **Step 1:** Push `feat/planner-kunobi-oauth`, open PR against `main` with the spec summary and test evidence.
- [ ] **Step 2:** Codex review (polycode `codex-exec.sh task`, read-only, VERIFY); address findings; re-review until APPROVE.
- [ ] **Step 3:** CI green → squash-merge.
- [ ] **Step 4:** Release with the `kunobi:create-rust-release` skill (`kache`, next minor `0.25.0`): bump PR, squash, `just release`, monitor Service Image + Package Publish; confirm `zondax/kache:v0.25.0` exists.

### Task 10: Deploy on zur1 and verify

**Files:** Modify `tenant-int-pro/by-cluster/zur1-worker1/kache/service/helmrelease.yaml`

- [ ] **Step 1: Clerk** — user creates the `kache-cli` OAuth app (public, PKCE, redirect `http://localhost:8329/callback`, scopes `openid profile email offline_access`, device grant) and provides its client id.
- [ ] **Step 2: Values**
```yaml
    auth:
      oidc:
        issuer: https://clerk.kunobi.com
        clientId: "<kache-cli client id>"
      githubOidc:
        audience: kache
        owners: [kunobi-ninja, Zondax]
    ingress:
      enabled: true
      className: ${clusterIngressClass}
      host: kache.${clusterDomain}
      annotations:
        cert-manager.io/cluster-issuer: ${acmeIssuer}
      tls:
        enabled: true
```
  and bump the image fallback `${PLANNER_TAG:=v0.25.0}`.
- [ ] **Step 3:** `kubectl kustomize`, prettier, server-side dry-run of the rendered chart; PR; merge after review.
- [ ] **Step 4: Verify**
```bash
curl -s https://kache.zur1-worker1.int-pro.zondax.io/.well-known/kunobi-auth     # {"issuer":"https://clerk.kunobi.com","clientId":"…"} no audience
curl -s -o /dev/null -w '%{http_code}' -X POST https://kache.…/v1/prefetch-plan -d '{}'   # 401
KACHE_PLANNER_ENDPOINT=https://kache.… kache login && <build> ; planner metrics show authorized requests
```
