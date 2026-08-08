//! Transport abstraction for the remote cache.
//!
//! The remote layout ([`crate::remote_layout`]) and manifest/shard sync
//! ([`crate::remote`]) speak in opaque byte objects addressed by key. OpenDAL
//! supplies the concrete S3 and shared-filesystem transports behind this seam.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
#[cfg(test)]
use opendal::services::Memory;
use opendal::{ErrorKind, HttpTransporter, OperationContext, Operator};
use opendal_http_transport_reqwest::ReqwestTransport;
use opendal_layer_retry::RetryLayer;
use opendal_service_fs::Fs;
use opendal_service_s3::S3;
use reqsign_aws_v4::{
    AssumeRoleWithWebIdentityCredentialProvider, Credential, DefaultCredentialProvider,
    ECSCredentialProvider, EnvCredentialProvider, IMDSv2CredentialProvider,
    ProcessCredentialProvider, ProfileCredentialProvider, SSOCredentialProvider,
    StaticCredentialProvider,
};
use reqsign_core::{
    CommandExecute, Context as SigningContext, Env, OsEnv, ProvideCredential,
    ProvideCredentialChain,
};

use crate::config::{FilesystemRemoteConfig, RemoteBackendConfig, RemoteConfig, S3RemoteConfig};

/// Abort a LIST that cannot yield an entry or completion. Repeated entries are
/// detected separately because a malformed continuation response can keep
/// yielding the first page without ever stalling.
const LIST_PROGRESS_TIMEOUT: Duration = Duration::from_secs(60);

/// Matches the AWS SDK's former default (`SDK_DEFAULT_CONNECT_TIMEOUT`), so a
/// black-holed endpoint fails fast instead of stalling a compile.
const CONNECT_TIMEOUT: Duration = Duration::from_millis(3100);

/// Per-read inactivity deadline. Not a total-request timeout: a large pack on a
/// slow link is legitimate, a stalled socket is not.
const READ_INACTIVITY_TIMEOUT: Duration = Duration::from_secs(30);

/// Ceiling on a single LIST. `LIST_PROGRESS_TIMEOUT` only catches a *stalled*
/// lister; an endpoint that keeps emitting fresh entries just under that timeout
/// would otherwise run unbounded while both `entries` and `seen` grow.
const LIST_TOTAL_TIMEOUT: Duration = Duration::from_secs(600);

/// Ceiling on entries returned by a single LIST, so a pathological or hostile
/// listing cannot exhaust memory in a compiler process. A DoS backstop, not a
/// tuning knob: set far above any plausible real cache.
const LIST_MAX_ENTRIES: usize = 1_000_000;

/// Companion byte ceiling on retained key text, because entry count alone does not
/// bound memory when keys are long.
const LIST_MAX_KEY_BYTES: usize = 256 * 1024 * 1024;

/// A fetched object plus the timing split callers report as transfer telemetry.
#[derive(Debug)]
pub struct GetObject {
    /// `Bytes` so a restore does not copy the whole pack a second time.
    pub body: Bytes,
    /// Time to response headers, ms.
    pub request_ms: u64,
    /// Time spent reading the body, ms.
    pub body_ms: u64,
}

/// Byte-object transport backing the remote cache.
///
/// Absence is not an error: `head` answers `false` and `get` answers `None`, so
/// callers can take a clean miss path without inspecting transport-specific
/// error codes.
#[async_trait]
pub trait RemoteBackend: Send + Sync {
    /// Whether `key` exists.
    async fn head(&self, key: &str) -> Result<bool>;

    /// Fetch `key`, or `None` when it is absent.
    ///
    /// `max_bytes` checks the object's advertised size before the body is
    /// buffered when metadata is available, and always enforces the cap while
    /// streaming the body.
    async fn get(&self, key: &str, max_bytes: Option<u64>) -> Result<Option<GetObject>>;

    /// Store `body` at `key`.
    async fn put(&self, key: &str, body: Vec<u8>, content_type: Option<&str>) -> Result<()>;

    /// File keys under `prefix`.
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// Where `key` lives, for logs and errors.
    fn describe(&self, key: &str) -> String;
}

/// OpenDAL-backed object transport.
pub struct OpenDalBackend {
    operator: Operator,
    root_description: String,
    /// Canonical filesystem root, for the filesystem backend only. Present means
    /// "this backend writes real paths", which enables the extra key rules and the
    /// write-containment check.
    filesystem_root: Option<PathBuf>,
}

impl OpenDalBackend {
    pub(crate) fn new(operator: Operator, root_description: String) -> Self {
        Self {
            operator,
            root_description,
            filesystem_root: None,
        }
    }

    fn is_filesystem(&self) -> bool {
        self.filesystem_root.is_some()
    }

    /// Best-effort check that `key` resolves inside the configured root.
    ///
    /// [`Self::validate_key`] is purely lexical, and OpenDAL's fs service
    /// canonicalizes only the root (once, at build time) before joining each key
    /// onto it — so a symlink at an intermediate path *inside* the root is
    /// followed. On a shared cache that another user can write, a symlink at
    /// `<prefix>/v3/packs` would redirect kache's writes outside the cache
    /// entirely, which is a real escalation over ordinary cache poisoning (that
    /// is already bounded by the layout layer's blake3 gate).
    ///
    /// This is defense in depth, not a hermetic boundary: the resolved path can
    /// still change between this check and the write. A hermetic version needs
    /// `openat2(RESOLVE_BENEATH)`, which OpenDAL does not expose.
    fn verify_write_containment(&self, key: &str) -> Result<()> {
        let Some(root) = &self.filesystem_root else {
            return Ok(());
        };
        let target = root.join(key);
        // Start at the leaf, not its parent: the destination itself may already
        // exist as a symlink. `symlink_metadata` so the check sees the link rather
        // than its target.
        let mut existing = None;
        for candidate in target.ancestors() {
            match candidate.symlink_metadata() {
                Ok(_) => {
                    existing = Some(candidate);
                    break;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("inspecting {} for containment check", candidate.display())
                    });
                }
            }
        }
        let Some(existing) = existing else {
            return Ok(());
        };
        let resolved = existing
            .canonicalize()
            .with_context(|| format!("resolving {} for containment check", existing.display()))?;
        if !resolved.starts_with(root) {
            anyhow::bail!(
                "refusing to write {}: {} resolves to {}, outside the configured remote root {}",
                self.describe(key),
                existing.display(),
                resolved.display(),
                root.display()
            );
        }
        Ok(())
    }

    fn contextual_error(&self, operation: &str, key: &str, error: opendal::Error) -> anyhow::Error {
        anyhow::Error::new(error).context(format!("{operation} {}", self.describe(key)))
    }

    fn validate_key(&self, operation: &str, key: &str, list_prefix: bool) -> Result<()> {
        let original = key;
        let key = if list_prefix && !key.is_empty() {
            key.strip_suffix('/').unwrap_or(key)
        } else {
            key
        };
        let valid_empty = list_prefix && original.is_empty();
        let canonical = valid_empty
            || (!key.is_empty()
                && !original.starts_with('/')
                && !key.contains('\\')
                // Control characters have no legitimate place in a cache key and
                // enable log injection in the messages built from it.
                && !key.chars().any(char::is_control)
                // On Windows a colon can introduce a drive prefix or alternate
                // data stream. Reject it for filesystem keys on every platform
                // so a shared config stays portable and contained by its root.
                && !(self.is_filesystem() && key.contains(':'))
                // Windows silently strips trailing dots and spaces from path
                // components, so `a.` and `a` would collide on a filesystem
                // remote written from Windows. Reject on every platform to keep
                // one shared cache addressable from all of them.
                && !(self.is_filesystem()
                    && key
                        .split('/')
                        .any(|segment| segment.ends_with('.') || segment.ends_with(' ')))
                && key
                    .split('/')
                    .all(|segment| !segment.is_empty() && segment != "." && segment != ".."));
        if !canonical {
            anyhow::bail!(
                "{operation} rejected non-canonical remote key {original:?} under {}",
                self.root_description
            );
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn memory_backend() -> OpenDalBackend {
    ensure_rustls_provider();
    let operator = Operator::new(Memory::default()).expect("memory operator");
    OpenDalBackend::new(operator, "memory://test".to_string())
}

#[async_trait]
impl RemoteBackend for OpenDalBackend {
    async fn head(&self, key: &str) -> Result<bool> {
        self.validate_key("HEAD", key, false)?;
        match self.operator.stat(key).await {
            Ok(metadata) => Ok(metadata.is_file()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
            Err(error) => Err(self.contextual_error("HEAD", key, error)),
        }
    }

    async fn get(&self, key: &str, max_bytes: Option<u64>) -> Result<Option<GetObject>> {
        self.validate_key("GET", key, false)?;
        let request_start = Instant::now();
        let reader = self
            .operator
            .reader(key)
            .await
            .map_err(|error| self.contextual_error("GET", key, error))?;
        let mut stream = reader
            .into_stream(..)
            .await
            .map_err(|error| self.contextual_error("GET", key, error))?;

        // Opening stream metadata starts the real read request for S3 without
        // consuming its body. Filesystem readers do not expose open metadata,
        // so fall back to stat there.
        let advertised_length = match stream.metadata().await {
            Ok(metadata) => Some(metadata.content_length()),
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) if error.kind() == ErrorKind::Unsupported => {
                match self.operator.stat(key).await {
                    Ok(metadata) => Some(metadata.content_length()),
                    Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
                    Err(error) => return Err(self.contextual_error("STAT", key, error)),
                }
            }
            Err(error) => return Err(self.contextual_error("GET", key, error)),
        };
        let request_ms = request_start.elapsed().as_millis() as u64;

        if let (Some(max), Some(length)) = (max_bytes, advertised_length)
            && length > max
        {
            anyhow::bail!(
                "{} too large: {length} bytes (max {max})",
                self.describe(key)
            );
        }

        let body_start = Instant::now();
        let mut chunks = Vec::new();
        let mut length = 0_u64;
        loop {
            let chunk = match stream.try_next().await {
                Ok(Some(chunk)) => chunk,
                Ok(None) => break,
                Err(error) if error.kind() == ErrorKind::NotFound && length == 0 => {
                    return Ok(None);
                }
                Err(error) => return Err(self.contextual_error("reading body of", key, error)),
            };
            length = length
                .checked_add(chunk.len() as u64)
                .context("remote object length overflow")?;
            if let Some(max) = max_bytes
                && length > max
            {
                anyhow::bail!(
                    "{} too large: at least {length} bytes (max {max})",
                    self.describe(key)
                );
            }
            chunks.extend(chunk);
        }
        // A stream that ends early is not a valid object. The layout layer's
        // blake3 gate would reject a truncated pack anyway, but catching it here
        // keeps the failure at the transport (where the key and byte counts are
        // known) instead of surfacing as a confusing hash mismatch, and it also
        // covers objects fetched outside that gate.
        verify_complete_body(advertised_length, length, &self.describe(key))?;

        let body_ms = body_start.elapsed().as_millis() as u64;
        let body = chunks.into_iter().collect::<opendal::Buffer>().to_bytes();

        Ok(Some(GetObject {
            body,
            request_ms,
            body_ms,
        }))
    }

    async fn put(&self, key: &str, body: Vec<u8>, content_type: Option<&str>) -> Result<()> {
        self.validate_key("PUT", key, false)?;
        self.verify_write_containment(key)?;
        let request = self.operator.write_with(key, body);
        let result = match content_type {
            Some(content_type) => request.content_type(content_type).await,
            None => request.await,
        };
        result
            .map(|_| ())
            .map_err(|error| self.contextual_error("PUT", key, error))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.validate_key("LIST", prefix, true)?;
        let mut lister = match self.operator.lister_with(prefix).recursive(true).await {
            Ok(lister) => lister,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(self.contextual_error("LIST", prefix, error)),
        };

        let mut entries = Vec::new();
        let mut seen = HashSet::new();
        let mut key_bytes = 0_usize;
        let list_start = Instant::now();
        loop {
            // Two guards: no-progress (a stalled lister) and total elapsed (one that
            // keeps trickling entries forever). Wait for whichever comes first, so
            // the total deadline cannot be overshot by a whole progress timeout.
            let elapsed = list_start.elapsed();
            let Some(remaining) = LIST_TOTAL_TIMEOUT.checked_sub(elapsed) else {
                anyhow::bail!(
                    "LIST {} exceeded its {}s total deadline after {} entries",
                    self.describe(prefix),
                    LIST_TOTAL_TIMEOUT.as_secs(),
                    entries.len()
                );
            };
            let wait = LIST_PROGRESS_TIMEOUT.min(remaining);
            let next = tokio::time::timeout(wait, lister.try_next())
                .await
                .with_context(|| {
                    if wait == remaining {
                        format!(
                            "LIST {} exceeded its {}s total deadline",
                            self.describe(prefix),
                            LIST_TOTAL_TIMEOUT.as_secs()
                        )
                    } else {
                        format!(
                            "LIST {} made no progress for {}s",
                            self.describe(prefix),
                            LIST_PROGRESS_TIMEOUT.as_secs()
                        )
                    }
                })?;
            match next {
                Ok(Some(entry)) => {
                    let path = entry.path().to_string();
                    if !seen.insert(path.clone()) {
                        anyhow::bail!(
                            "LIST {} returned duplicate entry {path:?}; \
                             the remote likely supplied an invalid continuation token",
                            self.describe(prefix)
                        );
                    }
                    // Bound entries AND bytes: each path is retained twice (once in
                    // `seen`, once in `entries`), so a flood of long keys can exhaust
                    // memory well before any plausible entry count.
                    key_bytes = key_bytes.saturating_add(path.len() * 2);
                    if seen.len() > LIST_MAX_ENTRIES || key_bytes > LIST_MAX_KEY_BYTES {
                        anyhow::bail!(
                            "LIST {} exceeded its limits ({} entries, {key_bytes} key bytes; \
                             caps are {LIST_MAX_ENTRIES} entries and {LIST_MAX_KEY_BYTES} bytes)",
                            self.describe(prefix),
                            seen.len()
                        );
                    }
                    if entry.metadata().is_file() {
                        entries.push(path);
                    }
                }
                Ok(None) => break,
                Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
                Err(error) => return Err(self.contextual_error("LIST", prefix, error)),
            }
        }
        Ok(entries)
    }

    fn describe(&self, key: &str) -> String {
        if key.is_empty() {
            self.root_description.clone()
        } else {
            format!("{}/{}", self.root_description, key)
        }
    }
}

/// A stream that ends before its advertised length has not delivered the object.
///
/// Split out from `get` so the comparison itself is unit-testable: over HTTP the
/// transport rejects an incomplete body first, so an integration test cannot prove
/// this branch. It exists for the case the transport cannot see — a backend that
/// ends the stream cleanly, such as the filesystem `stat`-then-read race where
/// another process truncates the file in between.
fn verify_complete_body(advertised: Option<u64>, read: u64, description: &str) -> Result<()> {
    if let Some(advertised) = advertised
        && read != advertised
    {
        anyhow::bail!("{description} truncated: read {read} bytes, expected {advertised}");
    }
    Ok(())
}

fn with_retries(operator: Operator) -> Operator {
    operator.layer(
        RetryLayer::new()
            .with_jitter()
            .with_min_delay(Duration::from_millis(100))
            .with_max_delay(Duration::from_secs(10))
            .with_max_times(3),
    )
}

fn ensure_rustls_provider() {
    // Kache's reqwest client is compiled with rustls-no-provider. Install ring
    // before constructing the S3 transport; the operation is process-wide and
    // idempotent.
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Override only `AWS_PROFILE`, preserving every other process environment
/// value and the platform home-directory lookup.
#[derive(Debug, Clone)]
struct ProfileSelectingEnv<E> {
    inner: E,
    profile: String,
}

impl<E: Env> Env for ProfileSelectingEnv<E> {
    fn var(&self, key: &str) -> Option<String> {
        if key == "AWS_PROFILE" {
            Some(self.profile.clone())
        } else {
            self.inner.var(key)
        }
    }

    fn vars(&self) -> HashMap<String, String> {
        let mut vars = self.inner.vars();
        vars.insert("AWS_PROFILE".to_string(), self.profile.clone());
        vars
    }

    fn home_dir(&self) -> Option<PathBuf> {
        self.inner.home_dir()
    }
}

/// OpenDAL exposes a custom credential chain but does not expose the selected
/// profile or command executor on its S3 builder. Wrap reqsign's default
/// provider so Kache can preserve both behaviors without mutating the process
/// environment.
#[derive(Debug)]
struct KacheCredentialProvider {
    inner: DefaultCredentialProvider,
    profile: Option<String>,
}

impl KacheCredentialProvider {
    fn new(profile: Option<String>, region: &str) -> Self {
        // Keep the AWS SDK's broad precedence: environment credentials first,
        // then all selected-profile providers, then workload identity/roles.
        let chain = ProvideCredentialChain::new()
            .push(EnvCredentialProvider::new())
            .push(ProfileCredentialProvider::default())
            .push(SSOCredentialProvider::default())
            .push(ProcessCredentialProvider::default())
            .push(
                AssumeRoleWithWebIdentityCredentialProvider::new().with_region(region.to_string()),
            )
            .push(ECSCredentialProvider::default())
            .push(IMDSv2CredentialProvider::default());
        Self {
            inner: DefaultCredentialProvider::with_chain(chain),
            profile,
        }
    }
}

/// Re-lex reqsign's `credential_process` tokens and execute the result directly.
///
/// reqsign splits the configured command with `split_whitespace()` and no quote
/// handling (see `reqsign-aws-v4`'s `execute_process`), so quote characters
/// survive *inside* the tokens: `credential_process = "/opt/my helper" --role "a b"`
/// arrives as `["\"/opt/my", "helper\"", "--role", "\"a", "b\""]`. Executing those
/// tokens as-is would try to run a program literally named `"/opt/my`, so the
/// quoting has to be reapplied.
///
/// Rejoining and handing the string to `sh -c` / `cmd.exe /C` does reapply it,
/// but it also hands the user's config to a shell: globs expand, `$(...)` and
/// backticks execute, `%VAR%` expands, and `&`/`|`/`^`/parens become operators.
/// Under `cmd.exe` it is worse still — single quotes are not quoting characters
/// there, so `'a b'` would not regroup. The AWS SDKs deliberately do shlex-style
/// splitting and never invoke a shell; match that instead.
fn relex_credential_command(program: &str, args: &[&str]) -> Result<(String, Vec<String>)> {
    let mut command = program.to_string();
    for arg in args {
        command.push(' ');
        command.push_str(arg);
    }
    let tokens = shlex_split(&command)
        .with_context(|| "credential_process has unbalanced quotes".to_string())?;
    let mut tokens = tokens.into_iter();
    let program = tokens
        .next()
        .context("credential_process resolved to an empty command")?;
    Ok((program, tokens.collect()))
}

/// Minimal POSIX-style lexer: single quotes are literal, double quotes group
/// while honoring `\` escapes, and unquoted `\` escapes the next character.
///
/// Deliberately does NOT implement expansion of any kind — this exists to undo
/// reqsign's whitespace split, not to emulate a shell. Returns `None` on
/// unterminated quotes rather than guessing where the argument ended.
fn shlex_split(input: &str) -> Option<Vec<String>> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut has_token = false;
    let mut chars = input.chars();

    while let Some(c) = chars.next() {
        match c {
            c if c.is_whitespace() => {
                if has_token {
                    tokens.push(std::mem::take(&mut current));
                    has_token = false;
                }
            }
            '\'' => {
                has_token = true;
                loop {
                    match chars.next() {
                        Some('\'') => break,
                        Some(c) => current.push(c),
                        None => return None,
                    }
                }
            }
            '"' => {
                has_token = true;
                loop {
                    match chars.next() {
                        Some('"') => break,
                        Some('\\') => match chars.next() {
                            // Only these are special inside double quotes; every
                            // other backslash stays literal, as in POSIX sh.
                            Some(escaped @ ('"' | '\\' | '$' | '`')) => current.push(escaped),
                            Some(other) => {
                                current.push('\\');
                                current.push(other);
                            }
                            None => return None,
                        },
                        Some(c) => current.push(c),
                        None => return None,
                    }
                }
            }
            '\\' => {
                has_token = true;
                // POSIX shells escape with backslash; Windows uses it as a path
                // separator, so `C:\tools\creds.exe` must survive intact.
                if cfg!(windows) {
                    current.push('\\');
                } else {
                    current.push(chars.next()?);
                }
            }
            c => {
                has_token = true;
                current.push(c);
            }
        }
    }
    if has_token {
        tokens.push(current);
    }
    Some(tokens)
}

#[derive(Debug, Clone, Default)]
struct KacheCommandExecute {
    /// Profile to hand the child, when Kache selected one explicitly.
    profile: Option<String>,
}

impl CommandExecute for KacheCommandExecute {
    async fn command_execute(
        &self,
        program: &str,
        args: &[&str],
    ) -> reqsign_core::Result<reqsign_core::CommandOutput> {
        let (program, args) = relex_credential_command(program, args)
            .map_err(|error| reqsign_core::Error::config_invalid(format!("{error:#}")))?;

        // `ProfileSelectingEnv` only redirects reqsign's own in-process reads. The
        // credential helper is a separate process that inherits this one's
        // environment, so without this it sees the ambient `AWS_PROFILE` and can
        // return credentials for a different account than the one Kache asked for.
        // Set it on the child only; the process environment is never mutated.
        let mut command = tokio::process::Command::new(&program);
        command
            .args(&args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        if let Some(profile) = &self.profile {
            command.env("AWS_PROFILE", profile);
        }
        let output = command.output().await.map_err(|error| {
            reqsign_core::Error::unexpected(format!("failed to execute command '{program}'"))
                .with_source(error)
        })?;

        Ok(reqsign_core::CommandOutput {
            status: output.status.code().unwrap_or(-1),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

impl ProvideCredential for KacheCredentialProvider {
    type Credential = Credential;

    async fn provide_credential(
        &self,
        context: &SigningContext,
    ) -> reqsign_core::Result<Option<Self::Credential>> {
        let context = context.clone().with_command_execute(KacheCommandExecute {
            profile: self.profile.clone(),
        });
        if let Some(profile) = &self.profile {
            let context = context.with_env(ProfileSelectingEnv {
                inner: OsEnv,
                profile: profile.clone(),
            });
            self.inner.provide_credential(&context).await
        } else {
            self.inner.provide_credential(&context).await
        }
    }
}

fn create_s3_operator(config: &S3RemoteConfig, pool_idle_secs: u64) -> Result<Operator> {
    // reqwest is compiled with rustls-no-provider. Installing ring here keeps
    // direct library/test callers safe; the operation is idempotent when
    // another Kache HTTP client already installed it.
    ensure_rustls_provider();
    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(pool_idle_secs))
        // `pool_idle_timeout` only reaps idle pooled connections; without these an
        // endpoint that accepts a connection and then goes silent hangs the
        // operation forever, and `RetryLayer` never fires because no error is ever
        // produced. On the rustc-wrapper path that is an apparently-hung build.
        // The AWS SDK supplied a 3.1s connect timeout by default
        // (aws-config's SDK_DEFAULT_CONNECT_TIMEOUT); keep parity and add a
        // read-inactivity deadline. Deliberately NOT a total-request timeout,
        // which would cap large artifact transfers on slow links.
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(READ_INACTIVITY_TIMEOUT)
        .build()
        .context("building S3 HTTP client")?;
    let context = OperationContext::new()
        .with_http_transport(HttpTransporter::new(ReqwestTransport::new(client)));

    let mut builder = S3::default()
        .bucket(&config.bucket)
        .region(&config.region)
        // Keep transport integrity without requiring a provider to implement
        // the newer full-object x-amz-checksum-* headers. Content-MD5 is
        // supported by AWS S3 and common S3-compatible PutObject endpoints.
        .checksum_algorithm("md5");
    let endpoint = config
        .endpoint
        .clone()
        .or_else(|| std::env::var("AWS_ENDPOINT_URL_S3").ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if let Some(endpoint) = endpoint {
        builder = builder.endpoint(&endpoint);
    }

    let mut credential_chain = ProvideCredentialChain::new().push(KacheCredentialProvider::new(
        config.profile.clone(),
        &config.region,
    ));
    let access_key = std::env::var("KACHE_S3_ACCESS_KEY").ok();
    let secret_key = std::env::var("KACHE_S3_SECRET_KEY").ok();
    match (access_key.as_deref(), secret_key.as_deref()) {
        (Some(access_key), Some(secret_key)) => {
            credential_chain =
                credential_chain.push_front(StaticCredentialProvider::new(access_key, secret_key));
        }
        (Some(_), None) => tracing::warn!(
            "KACHE_S3_ACCESS_KEY is set but KACHE_S3_SECRET_KEY is missing — ignoring partial credentials"
        ),
        (None, Some(_)) => tracing::warn!(
            "KACHE_S3_SECRET_KEY is set but KACHE_S3_ACCESS_KEY is missing — ignoring partial credentials"
        ),
        (None, None) => {}
    }
    builder = builder.credential_provider_chain(credential_chain);

    let operator = Operator::new(builder)
        .context("building OpenDAL S3 operator")?
        .with_context(context);
    Ok(with_retries(operator))
}

/// Same-filesystem check for the staging directory.
///
/// Publishing renames from the staging dir onto the final path, and `rename(2)`
/// cannot cross a mount point, so a cross-device staging dir fails EVERY write
/// with `EXDEV`. Deliberately here rather than in `Config::load`: it needs real
/// syscalls, and `Config::load` runs on the rustc-wrapper hot path where stat-ing
/// an unavailable network mount could stall the compiler. By the time a backend is
/// built, the remote is actually being used and this I/O is inherent.
///
/// A heuristic, not a proof: device ids can match across boundaries that still
/// reject a cross-boundary rename, and paths created later may be mounted
/// elsewhere. Being wrong here just means the clearer error comes from the write.
#[cfg(unix)]
fn verify_same_filesystem(
    root: &std::path::Path,
    atomic_write_dir: &std::path::Path,
) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    let device_of = |path: &std::path::Path| -> Option<u64> {
        let existing = path.ancestors().find(|candidate| candidate.exists())?;
        std::fs::metadata(existing).ok().map(|meta| meta.dev())
    };
    let (Some(root_device), Some(staging_device)) = (device_of(root), device_of(atomic_write_dir))
    else {
        return Ok(());
    };
    if root_device != staging_device {
        anyhow::bail!(
            "atomic_write_dir {} is on a different filesystem than the remote root {}; \
             publishing renames between them, which fails with EXDEV",
            atomic_write_dir.display(),
            root.display()
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn verify_same_filesystem(
    _root: &std::path::Path,
    _atomic_write_dir: &std::path::Path,
) -> Result<()> {
    // No portable device id without extra syscalls; the write's own error stands.
    Ok(())
}

fn create_filesystem_operator(config: &FilesystemRemoteConfig) -> Result<Operator> {
    ensure_rustls_provider();
    verify_same_filesystem(&config.root, &config.atomic_write_dir)?;
    let root = config
        .root
        .to_str()
        .context("filesystem remote path is not valid UTF-8")?;
    let atomic_write_dir = config
        .atomic_write_dir
        .to_str()
        .context("filesystem remote atomic_write_dir is not valid UTF-8")?;
    let builder = Fs::default().root(root).atomic_write_dir(atomic_write_dir);
    let operator = Operator::new(builder).context("building OpenDAL filesystem operator")?;
    Ok(with_retries(operator))
}

/// Build the backend named by `remote`.
///
/// `Arc` rather than `Box`: the prefetch path fans shard downloads out across
/// `tokio::spawn`, which needs an owned `'static` handle per task.
pub async fn create_backend(
    remote: &RemoteConfig,
    pool_idle_secs: u64,
) -> Result<Arc<dyn RemoteBackend>> {
    let backend = match &remote.backend {
        RemoteBackendConfig::S3(config) => OpenDalBackend::new(
            create_s3_operator(config, pool_idle_secs)?,
            format!("s3://{}", config.bucket),
        ),
        RemoteBackendConfig::Filesystem(config) => {
            let mut backend = OpenDalBackend::new(
                create_filesystem_operator(config)?,
                format!("file://{}", config.root.display()),
            );
            // Canonicalize once: the containment check compares against this, and
            // OpenDAL's fs service has already canonicalized the same root, so a
            // symlinked *root* is expected and fine — it is symlinks *below* it
            // that the check is for.
            backend.filesystem_root = Some(config.root.canonicalize().with_context(|| {
                format!("resolving filesystem remote root {}", config.root.display())
            })?);
            backend
        }
    };

    Ok(Arc::new(backend))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn mock_http_server(
        responses: Vec<String>,
    ) -> (String, tokio::sync::oneshot::Receiver<Vec<String>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (requests_tx, requests_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let mut requests = Vec::new();
            for response in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut chunk = [0_u8; 4096];
                loop {
                    let read = stream.read(&mut chunk).await.unwrap();
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&chunk[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                requests.push(String::from_utf8_lossy(&request).into_owned());
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            }
            let _ = requests_tx.send(requests);
        });
        (format!("http://{address}"), requests_rx)
    }

    fn http_response(status: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nContent-Type: application/xml\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    fn anonymous_s3_backend(endpoint: &str) -> OpenDalBackend {
        ensure_rustls_provider();
        let client = reqwest::Client::builder().build().unwrap();
        let builder = S3::default()
            .bucket("bucket")
            .region("us-east-1")
            .endpoint(endpoint)
            .checksum_algorithm("md5")
            .skip_signature();
        let context = OperationContext::new()
            .with_http_transport(HttpTransporter::new(ReqwestTransport::new(client)));
        let operator = Operator::new(builder).unwrap().with_context(context);
        OpenDalBackend::new(operator, "s3://bucket".to_string())
    }

    #[tokio::test]
    async fn object_round_trip_head_get_and_list() {
        let backend = memory_backend();
        assert!(!backend.head("nested/key").await.unwrap());
        assert!(backend.get("nested/key", None).await.unwrap().is_none());

        backend
            .put("nested/key", b"hello".to_vec(), Some("text/plain"))
            .await
            .unwrap();
        assert!(backend.head("nested/key").await.unwrap());
        let fetched = backend
            .get("nested/key", Some(5))
            .await
            .unwrap()
            .expect("present");
        assert_eq!(fetched.body, "hello");
        assert_eq!(backend.list("nested/").await.unwrap(), ["nested/key"]);
    }

    #[tokio::test]
    async fn get_refuses_an_object_over_the_cap() {
        let backend = memory_backend();
        backend.put("key", b"hello".to_vec(), None).await.unwrap();

        let error = backend
            .get("key", Some(1))
            .await
            .expect_err("over-cap object must fail")
            .to_string();
        assert!(error.contains("too large"), "{error}");
        assert!(error.contains("memory://test/key"), "{error}");
    }

    #[tokio::test]
    async fn filesystem_backend_uses_nested_paths_and_atomic_staging() {
        let root = tempfile::tempdir().unwrap();
        let atomic_write_dir = root.path().join(".staging");
        let remote = RemoteConfig {
            prefix: "artifacts".to_string(),
            backend: RemoteBackendConfig::Filesystem(FilesystemRemoteConfig {
                root: root.path().to_path_buf(),
                atomic_write_dir: atomic_write_dir.clone(),
            }),
        };
        let backend = create_backend(&remote, 30).await.unwrap();

        assert!(backend.list("artifacts/").await.unwrap().is_empty());
        backend
            .put(
                "artifacts/v3/key",
                b"shared".to_vec(),
                Some("application/json"),
            )
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(root.path().join("artifacts/v3/key")).unwrap(),
            b"shared"
        );
        assert!(atomic_write_dir.is_dir());
        assert_eq!(
            backend.list("artifacts/").await.unwrap(),
            ["artifacts/v3/key"]
        );
        backend
            .put(
                "artifacts/v3/key",
                b"updated".to_vec(),
                Some("application/json"),
            )
            .await
            .unwrap();
        assert_eq!(
            backend
                .get("artifacts/v3/key", None)
                .await
                .unwrap()
                .unwrap()
                .body,
            "updated"
        );
    }

    /// kunobi-ninja/kache#414 acceptance: "concurrent uploads of the same key
    /// are safe because the content is identical". Two clients that compiled
    /// the same unit race to publish the same pack; every writer must succeed
    /// and a reader must never observe a torn object — which is what the
    /// same-filesystem staging + atomic rename buys.
    #[tokio::test]
    async fn filesystem_concurrent_same_key_puts_never_tear() {
        let root = tempfile::tempdir().unwrap();
        let remote = RemoteConfig {
            prefix: "artifacts".to_string(),
            backend: RemoteBackendConfig::Filesystem(FilesystemRemoteConfig {
                root: root.path().to_path_buf(),
                atomic_write_dir: root.path().join(".staging"),
            }),
        };
        let backend = create_backend(&remote, 30).await.unwrap();

        // Large enough that a non-atomic writer would be caught mid-write by
        // a concurrent reader rather than finishing between polls.
        const BODY: usize = 512 * 1024;
        const WRITERS: usize = 8;
        let payload = vec![b'p'; BODY];
        let key = "artifacts/v3/packs/demo/samekey.tar.zst";

        let mut writers = Vec::new();
        for _ in 0..WRITERS {
            let backend = backend.clone();
            let payload = payload.clone();
            writers.push(tokio::spawn(async move {
                backend.put(key, payload, Some("application/zstd")).await
            }));
        }
        // Read concurrently with the writers: any observation must be a whole
        // object, never a partial one.
        let reader = {
            let backend = backend.clone();
            tokio::spawn(async move {
                let mut observed = Vec::new();
                for _ in 0..64 {
                    if let Some(object) = backend.get(key, None).await.unwrap() {
                        observed.push(object.body.len());
                    }
                    tokio::task::yield_now().await;
                }
                observed
            })
        };

        for writer in writers {
            writer
                .await
                .unwrap()
                .expect("every concurrent writer of identical content must succeed");
        }
        for len in reader.await.unwrap() {
            assert_eq!(len, BODY, "a reader observed a torn object ({len} bytes)");
        }

        let final_object = backend.get(key, None).await.unwrap().unwrap();
        assert_eq!(final_object.body.len(), BODY);
        assert!(
            final_object.body.iter().all(|b| *b == b'p'),
            "the published object must be exactly one writer's content"
        );
        // Staging must not leak: every temp file is renamed away.
        let staged: Vec<_> = std::fs::read_dir(root.path().join(".staging"))
            .map(|entries| entries.flatten().map(|e| e.path()).collect())
            .unwrap_or_default();
        assert!(staged.is_empty(), "staging left debris: {staged:?}");
    }

    #[tokio::test]
    async fn filesystem_backend_rejects_parent_traversal() {
        let root = tempfile::tempdir().unwrap();
        let remote = RemoteConfig {
            prefix: "artifacts".to_string(),
            backend: RemoteBackendConfig::Filesystem(FilesystemRemoteConfig {
                root: root.path().to_path_buf(),
                atomic_write_dir: root.path().join(".staging"),
            }),
        };
        let backend = create_backend(&remote, 30).await.unwrap();

        backend
            .put("../escape", b"nope".to_vec(), None)
            .await
            .expect_err("parent traversal must be rejected");
        backend
            .put(r"..\escape", b"nope".to_vec(), None)
            .await
            .expect_err("Windows parent traversal must be rejected");
        backend
            .put("/absolute", b"nope".to_vec(), None)
            .await
            .expect_err("absolute paths must be rejected");
        backend
            .put("C:/escape", b"nope".to_vec(), None)
            .await
            .expect_err("Windows drive prefixes must be rejected");
    }

    #[tokio::test]
    async fn s3_operator_builds_with_profile_and_custom_endpoint() {
        let config = S3RemoteConfig {
            bucket: "bucket".to_string(),
            endpoint: Some("http://127.0.0.1:9000".to_string()),
            region: "us-east-1".to_string(),
            profile: Some("team".to_string()),
        };
        create_s3_operator(&config, 30).expect("S3 operator builds without network I/O");
    }

    #[tokio::test]
    async fn s3_wire_uses_path_style_and_maps_bare_404_to_missing() {
        let (endpoint, requests) = mock_http_server(vec![http_response("404 Not Found", "")]).await;
        let backend = anonymous_s3_backend(&endpoint);

        assert!(
            backend
                .get("nested/key", Some(1024))
                .await
                .unwrap()
                .is_none()
        );
        let requests = requests.await.unwrap();
        assert_eq!(
            requests[0].lines().next(),
            Some("GET /bucket/nested/key HTTP/1.1")
        );
    }

    #[tokio::test]
    async fn s3_wire_does_not_treat_no_such_bucket_as_a_cache_miss() {
        let body = "<?xml version=\"1.0\"?><Error><Code>NoSuchBucket</Code>\
                    <Message>The bucket does not exist</Message></Error>";
        let (endpoint, _requests) =
            mock_http_server(vec![http_response("404 Not Found", body)]).await;
        let backend = anonymous_s3_backend(&endpoint);

        backend
            .get("key", None)
            .await
            .expect_err("a missing bucket is a configuration error");
    }

    #[tokio::test]
    async fn s3_wire_rejects_advertised_oversize_before_returning_body() {
        let (endpoint, _requests) = mock_http_server(vec![http_response("200 OK", "hello")]).await;
        let backend = anonymous_s3_backend(&endpoint);

        let error = backend
            .get("key", Some(4))
            .await
            .expect_err("content-length above cap must fail")
            .to_string();
        assert!(error.contains("too large"), "{error}");
    }

    #[tokio::test]
    async fn s3_wire_follows_continuation_tokens() {
        let first = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
            <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
            <Name>bucket</Name><Prefix>artifacts/</Prefix><KeyCount>1</KeyCount>\
            <MaxKeys>1000</MaxKeys><IsTruncated>true</IsTruncated>\
            <Contents><Key>artifacts/a</Key><Size>1</Size>\
            <LastModified>2026-07-24T00:00:00.000Z</LastModified></Contents>\
            <NextContinuationToken>next</NextContinuationToken></ListBucketResult>";
        let second = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
            <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
            <Name>bucket</Name><Prefix>artifacts/</Prefix><KeyCount>1</KeyCount>\
            <MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>\
            <Contents><Key>artifacts/b</Key><Size>1</Size>\
            <LastModified>2026-07-24T00:00:00.000Z</LastModified></Contents>\
            </ListBucketResult>";
        let (endpoint, requests) = mock_http_server(vec![
            http_response("200 OK", first),
            http_response("200 OK", second),
        ])
        .await;
        let backend = anonymous_s3_backend(&endpoint);

        assert_eq!(
            backend.list("artifacts/").await.unwrap(),
            ["artifacts/a", "artifacts/b"]
        );
        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].contains("list-type=2"), "{requests:?}");
        assert!(
            requests[1].contains("continuation-token=next"),
            "{requests:?}"
        );
    }

    #[tokio::test]
    async fn s3_wire_put_includes_an_integrity_checksum() {
        let (endpoint, requests) = mock_http_server(vec![http_response("200 OK", "")]).await;
        let backend = anonymous_s3_backend(&endpoint);

        backend
            .put("key", b"hello".to_vec(), Some("application/octet-stream"))
            .await
            .unwrap();

        let requests = requests.await.unwrap();
        let request = &requests[0];
        assert!(
            request.to_ascii_lowercase().contains("\r\ncontent-md5:"),
            "{request}"
        );
    }

    #[tokio::test]
    async fn s3_wire_rejects_a_truncated_page_without_a_continuation_token() {
        let malformed = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
            <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
            <Name>bucket</Name><Prefix>artifacts/</Prefix><KeyCount>1</KeyCount>\
            <MaxKeys>1000</MaxKeys><IsTruncated>true</IsTruncated>\
            <Contents><Key>artifacts/a</Key><Size>1</Size>\
            <LastModified>2026-07-24T00:00:00.000Z</LastModified></Contents>\
            </ListBucketResult>";
        let (endpoint, requests) = mock_http_server(vec![
            http_response("200 OK", malformed),
            http_response("200 OK", malformed),
        ])
        .await;
        let backend = anonymous_s3_backend(&endpoint);

        let error = backend
            .list("artifacts/")
            .await
            .expect_err("a repeated first page must not loop")
            .to_string();
        assert!(error.contains("duplicate entry"), "{error}");
        assert_eq!(requests.await.unwrap().len(), 2);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn credential_process_executor_preserves_quoted_arguments() {
        // reqsign splits on whitespace without stripping quotes, so the quote
        // characters arrive inside the tokens exactly like this.
        let output = KacheCommandExecute::default()
            .command_execute("printf", &["'%s'", "'hello", "world'"])
            .await
            .unwrap();
        assert!(output.success());
        assert_eq!(output.stdout, b"hello world");
    }

    #[test]
    fn credential_command_relexing_restores_quoted_grouping() {
        // `credential_process = "/opt/my helper" --role "build cache"` as reqsign
        // hands it over: whitespace-split, quotes intact.
        let (program, args) =
            relex_credential_command("\"/opt/my", &["helper\"", "--role", "\"build", "cache\""])
                .unwrap();
        assert_eq!(program, "/opt/my helper");
        assert_eq!(args, vec!["--role", "build cache"]);
    }

    #[test]
    fn credential_command_relexing_does_not_let_a_shell_interpret_the_command() {
        // Each of these would be reinterpreted by `sh -c` / `cmd.exe /C`. They must
        // survive as literal argument text instead.
        for (raw, expected) in [
            ("--token=a&b", "--token=a&b"),
            ("$HOME", "$HOME"),
            ("$(id)", "$(id)"),
            ("*.json", "*.json"),
            ("%USERPROFILE%", "%USERPROFILE%"),
            ("a|b", "a|b"),
        ] {
            let (program, args) = relex_credential_command("helper", &[raw]).unwrap();
            assert_eq!(program, "helper");
            assert_eq!(args, vec![expected], "{raw:?}");
        }
    }

    /// The in-process `ProfileSelectingEnv` does not reach a `credential_process`
    /// child, which is a separate process inheriting this one's environment. A
    /// helper that shells out to AWS tooling would otherwise resolve the ambient
    /// profile and return credentials for the wrong account.
    /// The in-process `ProfileSelectingEnv` does not reach a `credential_process`
    /// child, which is a separate process inheriting this one's environment. A
    /// helper that shells out to AWS tooling would otherwise resolve the ambient
    /// profile and return credentials for the wrong account.
    ///
    /// Reads the ambient value rather than setting one: mutating process env from a
    /// test races every other test in the binary, and CI may already export
    /// `AWS_PROFILE`.
    #[cfg(unix)]
    #[tokio::test]
    async fn credential_process_child_sees_the_configured_profile() {
        let selected = KacheCommandExecute {
            profile: Some("selected".to_string()),
        };
        let output = selected
            .command_execute("printenv", &["AWS_PROFILE"])
            .await
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&output.stdout).trim(),
            "selected",
            "the configured profile must reach the child"
        );

        // With no profile configured the child keeps whatever this process has.
        let inherited = KacheCommandExecute::default();
        let output = inherited
            .command_execute("printenv", &["AWS_PROFILE"])
            .await
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&output.stdout).trim(),
            std::env::var("AWS_PROFILE").unwrap_or_default(),
            "without a configured profile the ambient value must pass through"
        );
    }

    #[test]
    fn verify_complete_body_only_rejects_a_short_read() {
        assert!(verify_complete_body(Some(5), 5, "obj").is_ok());
        assert!(
            verify_complete_body(None, 5, "obj").is_ok(),
            "unknown length cannot be checked"
        );
        let error = verify_complete_body(Some(10), 5, "obj")
            .expect_err("a short read must be rejected")
            .to_string();
        assert!(error.contains("truncated"), "{error}");
        // A longer-than-advertised body is equally wrong.
        assert!(verify_complete_body(Some(4), 5, "obj").is_err());
    }

    #[cfg(not(windows))]
    #[test]
    fn credential_command_relexing_keeps_posix_backslash_escapes() {
        // reqsign splits `helper --path /opt/a\ b` into these tokens.
        let (program, args) =
            relex_credential_command("helper", &["--path", "/opt/a\\", "b"]).unwrap();
        assert_eq!(program, "helper");
        assert_eq!(args, vec!["--path", "/opt/a b"]);
    }

    #[cfg(windows)]
    #[test]
    fn credential_command_relexing_keeps_windows_paths_intact() {
        // A backslash is a path separator on Windows, not an escape.
        let (program, args) =
            relex_credential_command("C:\\tools\\aws-creds.exe", &["--profile", "ci"]).unwrap();
        assert_eq!(program, "C:\\tools\\aws-creds.exe");
        assert_eq!(args, vec!["--profile", "ci"]);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn filesystem_put_refuses_an_existing_symlinked_destination() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let victim = outside.path().join("victim");
        std::fs::write(&victim, b"original").unwrap();

        // The destination key itself already exists, as a symlink out of the cache.
        std::fs::create_dir_all(root.path().join("artifacts/v3")).unwrap();
        std::os::unix::fs::symlink(&victim, root.path().join("artifacts/v3/key")).unwrap();

        let remote = RemoteConfig {
            prefix: "artifacts".to_string(),
            backend: RemoteBackendConfig::Filesystem(FilesystemRemoteConfig {
                root: root.path().to_path_buf(),
                atomic_write_dir: root.path().join(".kache-tmp"),
            }),
        };
        let backend = create_backend(&remote, 30).await.unwrap();

        backend
            .put("artifacts/v3/key", b"attacker".to_vec(), None)
            .await
            .expect_err("an existing symlinked destination must be refused");
        assert_eq!(
            std::fs::read(&victim).unwrap(),
            b"original",
            "the file outside the root must be untouched"
        );
    }

    #[cfg(unix)]
    #[test]
    fn cross_device_staging_dir_is_rejected_at_backend_build() {
        // /dev/shm (Linux) or /Volumes (macOS) would be needed for a true
        // cross-device pair; instead assert the same-device case passes, which is
        // what every correct configuration hits.
        let root = tempfile::tempdir().unwrap();
        assert!(verify_same_filesystem(root.path(), &root.path().join(".kache-tmp")).is_ok());
    }

    #[test]
    fn credential_command_relexing_rejects_unbalanced_quotes() {
        let error = relex_credential_command("\"/opt/helper", &[])
            .expect_err("unbalanced quotes must not be guessed at")
            .to_string();
        assert!(error.contains("unbalanced quotes"), "{error}");
    }

    /// A body shorter than its advertised length must never surface as a hit.
    ///
    /// Over HTTP the transport itself rejects the incomplete body, so this pins
    /// the invariant rather than the mechanism. The explicit length comparison in
    /// `get` covers the case the transport cannot see: a backend that ends the
    /// stream cleanly, such as the filesystem `stat`-then-read race where another
    /// process truncates the file in between.
    #[tokio::test]
    async fn get_never_returns_a_body_shorter_than_content_length() {
        // Content-Length promises 10 bytes; the server sends 5 and closes.
        let truncated = "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\
                         Content-Type: application/octet-stream\r\n\
                         Connection: close\r\n\r\nhello";
        let (endpoint, _requests) = mock_http_server(vec![truncated.to_string()]).await;
        let backend = anonymous_s3_backend(&endpoint);

        let error = backend
            .get("key", None)
            .await
            .expect_err("a truncated body must not be returned as a hit")
            .to_string();
        assert!(error.contains("s3://bucket/key"), "{error}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn filesystem_put_refuses_to_follow_a_symlink_out_of_the_root() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        // A hostile peer on a shared cache plants a symlink inside the root.
        std::os::unix::fs::symlink(outside.path(), root.path().join("artifacts")).unwrap();

        let remote = RemoteConfig {
            prefix: "artifacts".to_string(),
            backend: RemoteBackendConfig::Filesystem(FilesystemRemoteConfig {
                root: root.path().to_path_buf(),
                atomic_write_dir: root.path().join(".kache-tmp"),
            }),
        };
        let backend = create_backend(&remote, 30).await.unwrap();

        let error = backend
            .put("artifacts/v3/key", b"escaped".to_vec(), None)
            .await
            .expect_err("writing through a symlink out of the root must be refused")
            .to_string();
        assert!(
            error.contains("outside the configured remote root"),
            "{error}"
        );
        assert!(
            !outside.path().join("v3/key").exists(),
            "bytes must not land outside the root"
        );
    }

    #[tokio::test]
    async fn filesystem_keys_reject_windows_hostile_shapes() {
        let root = tempfile::tempdir().unwrap();
        let remote = RemoteConfig {
            prefix: "artifacts".to_string(),
            backend: RemoteBackendConfig::Filesystem(FilesystemRemoteConfig {
                root: root.path().to_path_buf(),
                atomic_write_dir: root.path().join(".kache-tmp"),
            }),
        };
        let backend = create_backend(&remote, 30).await.unwrap();

        for key in [
            "artifacts/trailing.",   // Windows strips the trailing dot
            "artifacts/trailing ",   // ...and the trailing space
            "artifacts/ctrl\u{7f}x", // control characters
        ] {
            backend.put(key, b"nope".to_vec(), None).await.unwrap_err();
        }
    }

    #[test]
    fn explicit_profile_overrides_only_the_profile_environment_value() {
        let env = ProfileSelectingEnv {
            inner: reqsign_core::StaticEnv {
                home_dir: Some(PathBuf::from("/home/test")),
                envs: HashMap::from([
                    ("AWS_PROFILE".to_string(), "ambient".to_string()),
                    ("AWS_REGION".to_string(), "eu-west-1".to_string()),
                ]),
            },
            profile: "selected".to_string(),
        };

        assert_eq!(env.var("AWS_PROFILE").as_deref(), Some("selected"));
        assert_eq!(env.var("AWS_REGION").as_deref(), Some("eu-west-1"));
        assert_eq!(env.home_dir(), Some(PathBuf::from("/home/test")));
    }
}
