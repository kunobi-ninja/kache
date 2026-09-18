# Planner auth: Kunobi (Clerk) login and GitHub Actions OIDC

Status: approved design, 2026-09-18

## Goal

Put the kache prefetch planner (`kache-service`) behind real identity instead
of one optional shared token, so it can be exposed on the zur1 VPN ingress:

- developers run `kache login` once and their builds authenticate with their
  Kunobi (Clerk) account, refreshed silently;
- GitHub Actions jobs authenticate with the job's GitHub OIDC token, trusted
  only for repositories owned by `kunobi-ninja` or `Zondax`;
- the existing static `--token` keeps working for self-hosters.

The planner is advisory: a build that cannot authenticate falls back to local
planning exactly as it does today when the planner is unreachable. Auth must
never block, prompt, or open a browser on the build path.

## Non-goals

- Per-user authorization or quotas (any authenticated caller may plan).
- Protecting `/healthz`, `/readyz`, `/metrics` (they stay unauthenticated).
- Auth for the cache remote (S3 etc.); only the planner is in scope.
- A generic, file-configured provider/rule engine (YAGNI; two fixed providers).

## Components

### 1. kunobi-auth: selectable TLS provider (prerequisite)

`kunobi-auth` hard-enables `reqwest/rustls`, which in reqwest 0.13 pulls
`aws-lc-rs`. The kache CLI is deliberately free of `aws-lc-sys` (ring only).

Change: depend on reqwest with `rustls-no-provider`, and add a default-on
feature `aws-lc-tls = ["reqwest/rustls"]`. Default builds are unchanged;
consumers that disable default features must install a rustls crypto provider
before the first request (kache already does, in `ensure_crypto_provider`).
Release as a patch (0.11.2).

### 2. kache-service: providers, authorization, discovery

New CLI flags (each with an env var):

| Flag | Env | Meaning |
|---|---|---|
| `--oidc-issuer` | `KACHE_PLANNER_OIDC_ISSUER` | Human OIDC issuer (Clerk: `https://clerk.kunobi.com`) |
| `--oidc-client-id` | `KACHE_PLANNER_OIDC_CLIENT_ID` | Public CLI client id; accepted `aud`, advertised for login |
| `--github-oidc-audience` | `KACHE_PLANNER_GITHUB_AUDIENCE` | Accepted `aud` for GitHub Actions tokens (e.g. `kache`) |
| `--github-owner` (repeatable) / | `KACHE_PLANNER_GITHUB_OWNERS` (comma list) | Allowed `repository_owner` values |
| `--token` (existing) | `KACHE_PLANNER_TOKEN` | Static shared token |

Wiring: build a `kunobi_auth::server::ConfiguredAuth` with
- a static-token provider when `--token` is set (identity `planner-client`);
- JWT provider `kunobi` (issuer, JWKS `{issuer}/.well-known/jwks.json`,
  audience `[client_id]`, identity claim `sub`) when issuer + client id are set;
- JWT provider `github-actions` (issuer `https://token.actions.githubusercontent.com`,
  audience `[github_audience]`, identity claim `sub`) when the audience is set.

`AppState` holds `Option<ConfiguredAuth>` plus the owner allow-list, and its
`AuthnProvider` impl delegates to `ConfiguredAuth`, then enforces: an identity
from `github-actions` must carry a `repository_owner` claim in the allow-list,
otherwise `Unauthorized`. "Auth required" means any provider is configured;
with none, behaviour is unchanged (anonymous).

Startup validation: `--oidc-issuer` without `--oidc-client-id` (or vice versa)
is an error; `--github-oidc-audience` without at least one owner is an error
(an empty allow-list would trust every public repository).

Discovery: when the Clerk provider is configured, mount
`kunobi_auth_discovery_router` at `/.well-known/kunobi-auth` with
`{issuer, clientId}` and **no audience** (an ID token's `aud` is already the
client id; Clerk rejects a requested audience that is not whitelisted — the
lesson from kobe#326).

### 3. kache client: login, token sources, planner bearer

kache takes `kunobi-auth` with `default-features = false`,
features `client`, `browser-login`, `rust_crypto` (ring TLS via its own
provider).

Commands:
- `kache login [--device] [--retrust]` — resolves the planner endpoint from
  config, discovers `/.well-known/kunobi-auth` (pinned; `--retrust` re-pins a
  changed issuer), runs browser PKCE or device login, stores the session in
  kunobi-auth's per-issuer token store.
- `kache logout` — revokes and deletes the stored session.

Bearer resolution for each planner request, first match wins:
1. explicit token (`KACHE_PLANNER_TOKEN` / `cache.planner.token`) — unchanged;
2. GitHub Actions: when `ACTIONS_ID_TOKEN_REQUEST_URL` and
   `ACTIONS_ID_TOKEN_REQUEST_TOKEN` are present, request an ID token with
   audience `cache.planner.github_audience` (default `kache`), cached in memory
   until shortly before `exp`;
3. stored Kunobi session for the endpoint's issuer, refreshed with the refresh
   token when expired — never interactive;
4. none.

A failure in any source is logged at debug and yields "no bearer"; the request
proceeds and a 401 lands in the existing local-planning fallback. Discovery
results and the resolved bearer are cached in the daemon so the 750 ms planner
budget is not spent on discovery for every build.

### 4. Helm chart

`values.yaml` gains:

```yaml
auth:
  existingSecret: ""        # unchanged (static token)
  existingSecretKey: token
  oidc:
    issuer: ""
    clientId: ""
  githubOidc:
    audience: ""
    owners: []
```

mapped to the env vars above. The existing optional Ingress is unchanged.

### 5. Deployment (tenant-int-pro, zur1-worker1)

- New Clerk OAuth app `kache-cli`: public client, PKCE, redirect
  `http://localhost:8329/callback`, scopes `openid profile email
  offline_access`, device grant on.
- HelmRelease values: `auth.oidc` (Clerk, kache-cli client id),
  `auth.githubOidc` (`audience: kache`, owners `kunobi-ninja`, `Zondax`),
  `ingress.enabled` on `kache.${clusterDomain}` (VPN-only, TLS via the
  cluster issuer), image ≥ the release carrying this work.

## Error handling

| Situation | Behaviour |
|---|---|
| No/invalid bearer, auth configured | 401 on `/v1/prefetch-plan`; client falls back to local planning |
| GitHub token from a non-allowed owner | 401 |
| JWKS unreachable | 500 (kunobi-auth maps transport faults to Internal); client falls back |
| Stored session expired, refresh fails | no bearer; debug log hints `kache login` |
| Discovery pin mismatch on `kache login` | error pointing at `--retrust` |

## Testing

- kunobi-auth: `cargo tree` shows no `aws-lc-sys` with default features off;
  default build unchanged; existing tests pass.
- kache-service unit tests: static token still works; Clerk JWT accepted;
  wrong audience rejected; GitHub token accepted for an allowed owner and
  rejected otherwise; startup validation errors; discovery served only with a
  client id and without audience. JWTs signed by a test RSA key served from an
  in-test JWKS endpoint.
- kache client unit tests: bearer precedence; GitHub token request/caching
  against a stub server; stored-session refresh path; no bearer on failure.
- `cargo tree -p kache -i aws-lc-sys` stays empty (guard in CI if cheap).
- End-to-end on zur1 after deploy: `kache login`, a build receives a plan
  (200) and `/metrics` shows authorized requests; an unauthenticated request
  gets 401.
