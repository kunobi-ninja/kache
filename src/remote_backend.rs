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
use opendal::layers::{HttpClientLayer, RetryLayer};
use opendal::raw::HttpClient;
use opendal::{ErrorKind, Operator, services};
use reqsign_aws_v4::{
    AssumeRoleWithWebIdentityCredentialProvider, Credential, DefaultCredentialProvider,
    ECSCredentialProvider, EnvCredentialProvider, IMDSv2CredentialProvider,
    ProcessCredentialProvider, ProfileCredentialProvider, SSOCredentialProvider,
    StaticCredentialProvider,
};
use reqsign_command_execute_tokio::TokioCommandExecute;
use reqsign_core::{
    CommandExecute, Context as SigningContext, Env, OsEnv, ProvideCredential,
    ProvideCredentialChain,
};

use crate::config::{FilesystemRemoteConfig, RemoteBackendConfig, RemoteConfig, S3RemoteConfig};

/// Abort a LIST that cannot yield an entry or completion. Repeated entries are
/// detected separately because a malformed continuation response can keep
/// yielding the first page without ever stalling.
const LIST_PROGRESS_TIMEOUT: Duration = Duration::from_secs(60);

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
    is_filesystem: bool,
}

impl OpenDalBackend {
    pub(crate) fn new(operator: Operator, root_description: String) -> Self {
        Self {
            operator,
            root_description,
            is_filesystem: false,
        }
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
                // On Windows a colon can introduce a drive prefix or alternate
                // data stream. Reject it for filesystem keys on every platform
                // so a shared config stays portable and contained by its root.
                && !(self.is_filesystem && key.contains(':'))
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
    let operator = Operator::new(services::Memory::default())
        .expect("memory operator")
        .finish();
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
        loop {
            let next = tokio::time::timeout(LIST_PROGRESS_TIMEOUT, lister.try_next())
                .await
                .with_context(|| {
                    format!(
                        "LIST {} made no progress for {}s",
                        self.describe(prefix),
                        LIST_PROGRESS_TIMEOUT.as_secs()
                    )
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
    // OpenDAL 0.57 initializes its process-wide reqwest client lazily even for
    // non-HTTP operators. Install ring before constructing any operator so a
    // filesystem or in-memory remote cannot poison that LazyLock.
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

/// OpenDAL 0.57 exposes a custom credential chain but does not expose the
/// selected profile or command executor on its S3 builder. Wrap reqsign's
/// default provider so Kache can preserve both behaviors without mutating the
/// process environment.
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

/// Reassemble reqsign's tokenized `credential_process` and run it through the
/// platform shell, matching the AWS SDK's support for quoted arguments and
/// executable paths containing spaces.
#[derive(Debug, Clone, Copy)]
struct KacheCommandExecute;

impl CommandExecute for KacheCommandExecute {
    async fn command_execute(
        &self,
        program: &str,
        args: &[&str],
    ) -> reqsign_core::Result<reqsign_core::CommandOutput> {
        let mut command = program.to_string();
        for arg in args {
            command.push(' ');
            command.push_str(arg);
        }
        #[cfg(windows)]
        let (shell, shell_args) = ("cmd.exe", ["/C", command.as_str()]);
        #[cfg(not(windows))]
        let (shell, shell_args) = ("sh", ["-c", command.as_str()]);
        TokioCommandExecute
            .command_execute(shell, &shell_args)
            .await
    }
}

impl ProvideCredential for KacheCredentialProvider {
    type Credential = Credential;

    async fn provide_credential(
        &self,
        context: &SigningContext,
    ) -> reqsign_core::Result<Option<Self::Credential>> {
        let context = context.clone().with_command_execute(KacheCommandExecute);
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
        .build()
        .context("building S3 HTTP client")?;
    let http_client_layer = HttpClientLayer::new(HttpClient::with(client));

    let mut builder = services::S3::default()
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
        .layer(http_client_layer)
        .finish();
    Ok(with_retries(operator))
}

fn create_filesystem_operator(config: &FilesystemRemoteConfig) -> Result<Operator> {
    ensure_rustls_provider();
    let root = config
        .root
        .to_str()
        .context("filesystem remote path is not valid UTF-8")?;
    let atomic_write_dir = config
        .atomic_write_dir
        .to_str()
        .context("filesystem remote atomic_write_dir is not valid UTF-8")?;
    let builder = services::Fs::default()
        .root(root)
        .atomic_write_dir(atomic_write_dir);
    let operator = Operator::new(builder)
        .context("building OpenDAL filesystem operator")?
        .finish();
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
            backend.is_filesystem = true;
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
        let builder = services::S3::default()
            .bucket("bucket")
            .region("us-east-1")
            .endpoint(endpoint)
            .checksum_algorithm("md5")
            .skip_signature();
        let operator = Operator::new(builder)
            .unwrap()
            .layer(HttpClientLayer::new(HttpClient::with(client)))
            .finish();
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
        let output = KacheCommandExecute
            .command_execute("printf", &["'%s'", "'hello world'"])
            .await
            .unwrap();
        assert!(output.success());
        assert_eq!(output.stdout, b"hello world");
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
