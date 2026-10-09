//! Transport abstraction for the remote cache.
//!
//! The remote layout ([`crate::remote_layout`]) and manifest/shard sync
//! ([`crate::remote`]) speak in opaque byte objects addressed by key. OpenDAL
//! supplies S3, GCS, and shared-filesystem transports. The OCI transport uses
//! a native registry client behind the same interface.

mod download_memory;
mod oci;
mod s3_credentials;

use download_memory::{BudgetedBody, DOWNLOAD_MEMORY, DownloadMemory};
pub(crate) use s3_credentials::CredentialFailure;
use s3_credentials::{CredentialStatus, KacheCredentialProvider};

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
#[cfg(test)]
use opendal::services::Memory;
use opendal::{ErrorKind, HttpTransporter, OperationContext, Operator};
use opendal_http_transport_reqwest::ReqwestTransport;
use opendal_service_fs::Fs;
use opendal_service_gcs::Gcs;
use opendal_service_s3::S3;
use reqsign_aws_v4::Credential;
use reqsign_core::{CommandExecute, ProvideCredential, ProvideCredentialChain};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::config::{
    FilesystemRemoteConfig, GcsRemoteConfig, RemoteBackendConfig, RemoteConfig, S3RemoteConfig,
};

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
    /// Clones share the allocation and its memory reservation. Consume or drop
    /// buffered objects before waiting for more downloads from the same budget.
    pub body: Bytes,
    /// Time to response headers, ms.
    pub request_ms: u64,
    /// Time spent reading the body, ms.
    pub body_ms: u64,
}

/// Byte count and timings for a download written to a caller-owned sink.
#[derive(Debug)]
pub struct GetTransfer {
    pub bytes: u64,
    pub request_ms: u64,
    pub body_ms: u64,
}

/// Outcome of an atomic create-only remote publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutIfAbsentResult {
    Created,
    AlreadyExists,
    /// The store cannot make the write create-only. Nothing was written.
    Unsupported,
}

/// Outcome of [`RemoteBackend::put_if_match`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConditionalPut {
    Stored,
    /// The object changed since it was read, or appeared when absence was
    /// expected. Nothing was written; read again and retry.
    Conflict,
    /// The store cannot make this write conditional. Nothing was written.
    Unsupported,
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

    /// Fetch into `destination`, flushing it before returning. A failure may
    /// leave partial bytes in the sink; callers must discard them.
    ///
    /// Backends should override this to stream. The buffered fallback keeps
    /// transports that implement only `get` usable by the restore path.
    async fn get_into(
        &self,
        key: &str,
        max_bytes: Option<u64>,
        destination: &mut (dyn AsyncWrite + Unpin + Send),
    ) -> Result<Option<GetTransfer>> {
        let Some(object) = self.get(key, max_bytes).await? else {
            return Ok(None);
        };
        destination.write_all(&object.body).await?;
        destination.flush().await?;
        Ok(Some(GetTransfer {
            bytes: object.body.len() as u64,
            request_ms: object.request_ms,
            body_ms: object.body_ms,
        }))
    }

    /// Fetch `key` with the entity tag of the bytes returned, for a later
    /// [`Self::put_if_match`]. The tag is `None` when the transport cannot tie
    /// one to the body it read.
    async fn get_versioned(
        &self,
        key: &str,
        max_bytes: Option<u64>,
    ) -> Result<Option<(GetObject, Option<String>)>> {
        Ok(self.get(key, max_bytes).await?.map(|object| (object, None)))
    }

    /// Store `body` at `key` only if the object still carries `expected`, an
    /// entity tag from [`Self::get_versioned`]. With `expected` as `None`,
    /// store it only if `key` is absent.
    async fn put_if_match(
        &self,
        _key: &str,
        _body: Vec<u8>,
        _content_type: Option<&str>,
        _expected: Option<&str>,
    ) -> Result<ConditionalPut> {
        Ok(ConditionalPut::Unsupported)
    }

    /// Store `body` at `key`.
    async fn put(&self, key: &str, body: Vec<u8>, content_type: Option<&str>) -> Result<()>;

    /// Store `body` only when `key` is absent, atomically.
    ///
    /// Implementations must never emulate this with HEAD followed by PUT: that
    /// race would let two publishers overwrite an immutable transport object.
    /// A transport that cannot do it answers `Unsupported` and writes nothing.
    async fn put_if_absent(
        &self,
        _key: &str,
        _body: Vec<u8>,
        _content_type: Option<&str>,
    ) -> Result<PutIfAbsentResult> {
        Ok(PutIfAbsentResult::Unsupported)
    }

    /// File keys under `prefix`.
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// Where `key` lives, for logs and errors.
    fn describe(&self, key: &str) -> String;

    /// Where the credentials of the last signed request came from, for
    /// `kache doctor`. `None` when the transport does not track it.
    fn credential_source(&self) -> Option<String> {
        None
    }
}

/// OpenDAL-backed object transport.
pub struct OpenDalBackend {
    operator: Operator,
    root_description: String,
    /// Canonical filesystem root, for the filesystem backend only. Present means
    /// "this backend writes real paths", which enables the extra key rules and the
    /// write-containment check.
    filesystem_root: Option<PathBuf>,
    download_memory: Arc<DownloadMemory>,
    /// Set once a refused read has been reported as a miss.
    refusal_reported: AtomicBool,
    /// The configured S3 region, to explain a request sent to the wrong one.
    region: Option<String>,
    /// Whether a refused read can mean a missing object. S3 answers 403 for a
    /// missing key without `s3:ListBucket`; GCS does not, so there a refusal
    /// stays an error.
    refusal_may_be_absence: bool,
    /// What the S3 credential chain last did, for the S3 backend only.
    credentials: Option<Arc<CredentialStatus>>,
}

impl OpenDalBackend {
    pub(crate) fn new(operator: Operator, root_description: String) -> Self {
        Self {
            operator,
            root_description,
            filesystem_root: None,
            download_memory: DOWNLOAD_MEMORY.clone(),
            refusal_reported: AtomicBool::new(false),
            region: None,
            refusal_may_be_absence: true,
            credentials: None,
        }
    }

    /// True for the first refused read only, so the explanation is logged once.
    fn first_refusal(&self) -> bool {
        !self.refusal_reported.swap(true, Ordering::Relaxed)
    }

    /// Whether a refused read means the object is absent.
    ///
    /// Without `s3:ListBucket`, S3 answers `403` instead of `404` for a key
    /// that does not exist, so a reader granted only `s3:GetObject` sees every
    /// miss as a refusal. Credentials S3 rejected outright stay errors. A
    /// filesystem permission error is a real error.
    fn refusal_means_absent(&self, error: &opendal::Error) -> bool {
        if self.is_filesystem()
            || !self.refusal_may_be_absence
            || error.kind() != ErrorKind::PermissionDenied
            || credentials_rejected(error)
        {
            return false;
        }
        if self.first_refusal() {
            tracing::warn!(
                "{} refused a read; treating refusals as cache misses, which is what S3 \
                 returns for a missing key without s3:ListBucket. If the cache never hits, \
                 these credentials may not be allowed to read it",
                self.root_description
            );
        }
        true
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
        let explanation = explain_opendal_failure(&error, self.region.as_deref());
        let credentials = self.credential_failure(&error);
        let mut error =
            anyhow::Error::new(error).context(format!("{operation} {}", self.describe(key)));
        if let Some(failure) = credentials {
            error = error.context(failure);
        }
        match explanation {
            Some(explanation) => error.context(explanation),
            None => error,
        }
    }

    /// Why the credential chain stopped, when that is why `error` was never
    /// sent. OpenDAL reports only that signing failed.
    fn credential_failure(&self, error: &opendal::Error) -> Option<CredentialFailure> {
        let status = self.credentials.as_ref()?;
        if !signing_failed(error) {
            return None;
        }
        status.failure()
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

#[cfg(test)]
pub(crate) fn memory_backend_with_download_budget(kib: u32) -> OpenDalBackend {
    let mut backend = memory_backend();
    backend.download_memory = Arc::new(DownloadMemory::new(kib));
    backend
}

impl OpenDalBackend {
    /// [`RemoteBackend::get_into`], also returning the entity tag the read
    /// response carried. The filesystem backend has none to give.
    async fn read_into(
        &self,
        key: &str,
        max_bytes: Option<u64>,
        destination: &mut (dyn AsyncWrite + Unpin + Send),
    ) -> Result<Option<(GetTransfer, Option<String>)>> {
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
        let (advertised_length, etag) = match stream.metadata().await {
            Ok(metadata) => (
                Some(metadata.content_length()),
                metadata.etag().map(str::to_string),
            ),
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) if self.refusal_means_absent(&error) => return Ok(None),
            Err(error) if error.kind() == ErrorKind::Unsupported => {
                // A separate stat can describe a newer object than the one
                // read, so its tag is not offered for a conditional write.
                match self.operator.stat(key).await {
                    Ok(metadata) => (Some(metadata.content_length()), None),
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
            for bytes in chunk {
                destination
                    .write_all(&bytes)
                    .await
                    .with_context(|| format!("writing body of {}", self.describe(key)))?;
            }
        }
        // A stream that ends early is not a valid object. The layout layer's
        // blake3 gate would reject a truncated pack anyway, but catching it here
        // keeps the failure at the transport (where the key and byte counts are
        // known) instead of surfacing as a confusing hash mismatch, and it also
        // covers objects fetched outside that gate.
        verify_complete_body(advertised_length, length, &self.describe(key))?;
        destination
            .flush()
            .await
            .context("flushing downloaded body")?;

        let body_ms = body_start.elapsed().as_millis() as u64;

        Ok(Some((
            GetTransfer {
                bytes: length,
                request_ms,
                body_ms,
            },
            etag,
        )))
    }
}

#[async_trait]
impl RemoteBackend for OpenDalBackend {
    async fn head(&self, key: &str) -> Result<bool> {
        self.validate_key("HEAD", key, false)?;
        match self.operator.stat(key).await {
            Ok(metadata) => Ok(metadata.is_file()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
            Err(error) if self.refusal_means_absent(&error) => Ok(false),
            Err(error) => Err(self.contextual_error("HEAD", key, error)),
        }
    }

    async fn get(&self, key: &str, max_bytes: Option<u64>) -> Result<Option<GetObject>> {
        self.validate_key("GET", key, false)?;
        let memory = self.download_memory.acquire(max_bytes).await?;
        let mut body = Vec::new();
        let Some(transfer) = self.get_into(key, max_bytes, &mut body).await? else {
            return Ok(None);
        };
        Ok(Some(GetObject {
            body: Bytes::from_owner(BudgetedBody {
                body: Bytes::from(body),
                _memory: memory,
            }),
            request_ms: transfer.request_ms,
            body_ms: transfer.body_ms,
        }))
    }

    async fn get_into(
        &self,
        key: &str,
        max_bytes: Option<u64>,
        destination: &mut (dyn AsyncWrite + Unpin + Send),
    ) -> Result<Option<GetTransfer>> {
        Ok(self
            .read_into(key, max_bytes, destination)
            .await?
            .map(|(transfer, _)| transfer))
    }

    async fn get_versioned(
        &self,
        key: &str,
        max_bytes: Option<u64>,
    ) -> Result<Option<(GetObject, Option<String>)>> {
        self.validate_key("GET", key, false)?;
        let memory = self.download_memory.acquire(max_bytes).await?;
        let mut body = Vec::new();
        let Some((transfer, etag)) = self.read_into(key, max_bytes, &mut body).await? else {
            return Ok(None);
        };
        let object = GetObject {
            body: Bytes::from_owner(BudgetedBody {
                body: Bytes::from(body),
                _memory: memory,
            }),
            request_ms: transfer.request_ms,
            body_ms: transfer.body_ms,
        };
        Ok(Some((object, etag)))
    }

    async fn put_if_match(
        &self,
        key: &str,
        body: Vec<u8>,
        content_type: Option<&str>,
        expected: Option<&str>,
    ) -> Result<ConditionalPut> {
        self.validate_key("PUT", key, false)?;
        self.verify_write_containment(key)?;
        let capability = self.operator.info().capability();
        let supported = match expected {
            Some(_) => capability.write_with_if_match,
            None => capability.write_with_if_not_exists,
        };
        if !supported {
            return Ok(ConditionalPut::Unsupported);
        }
        let request = self.operator.write_with(key, body);
        let request = match expected {
            Some(tag) => request.if_match(tag),
            None => request.if_not_exists(true),
        };
        let result = match content_type {
            Some(content_type) => request.content_type(content_type).await,
            None => request.await,
        };
        match result {
            Ok(_) => Ok(ConditionalPut::Stored),
            Err(error) => match classify_conditional_error(&error, expected.is_some()) {
                Some(outcome) => Ok(outcome),
                None => Err(self.contextual_error("PUT", key, error)),
            },
        }
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

    async fn put_if_absent(
        &self,
        key: &str,
        body: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<PutIfAbsentResult> {
        self.validate_key("CREATE", key, false)?;
        self.verify_write_containment(key)?;
        if !self.operator.info().capability().write_with_if_not_exists {
            return Ok(PutIfAbsentResult::Unsupported);
        }
        let request = self.operator.write_with(key, body).if_not_exists(true);
        let result = match content_type {
            Some(content_type) => request.content_type(content_type).await,
            None => request.await,
        };
        match result {
            Ok(_) => Ok(PutIfAbsentResult::Created),
            Err(error) => match classify_create_error(&error) {
                Some(outcome) => Ok(outcome),
                None => Err(self.contextual_error("CREATE", key, error)),
            },
        }
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

    fn credential_source(&self) -> Option<String> {
        self.credentials.as_ref()?.source()
    }
}

/// Whether `error` came from signing the request, which happens before it is
/// sent.
fn signing_failed(error: &opendal::Error) -> bool {
    std::iter::successors(std::error::Error::source(error), |cause| cause.source())
        .any(|cause| cause.is::<reqsign_core::Error>())
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

/// Only failed create preconditions mean that an immutable object already won
/// the publication race. Authentication, transport, and storage failures must
/// remain errors rather than being reported as a harmless duplicate.
fn classify_create_error(error: &opendal::Error) -> Option<PutIfAbsentResult> {
    match error.kind() {
        // S3 answers a create racing another conditional write with 409
        // `ConditionalRequestConflict`: the other writer holds the same bytes.
        ErrorKind::ConditionNotMatch | ErrorKind::AlreadyExists | ErrorKind::Conflict => {
            Some(PutIfAbsentResult::AlreadyExists)
        }
        ErrorKind::Unsupported => Some(PutIfAbsentResult::Unsupported),
        ErrorKind::Unexpected if lacks_conditional_writes(error) => {
            Some(PutIfAbsentResult::Unsupported)
        }
        _ => None,
    }
}

/// A conditional write that lost its precondition changed nothing: another
/// writer got there first (`412`, `409`), or deleted the object an `If-Match`
/// named (S3 answers `404`). A store that does not implement the condition
/// says so with `501`, or `400` carrying the `NotImplemented` code, which
/// OpenDAL reports as an unexpected error; nothing was written then either.
/// Every other failure stays an error: after a timeout the write may have
/// landed, so it must not be retried unconditionally.
fn classify_conditional_error(error: &opendal::Error, replacing: bool) -> Option<ConditionalPut> {
    match error.kind() {
        ErrorKind::ConditionNotMatch | ErrorKind::AlreadyExists | ErrorKind::Conflict => {
            Some(ConditionalPut::Conflict)
        }
        ErrorKind::NotFound if replacing => Some(ConditionalPut::Conflict),
        ErrorKind::Unsupported => Some(ConditionalPut::Unsupported),
        ErrorKind::Unexpected if lacks_conditional_writes(error) => {
            Some(ConditionalPut::Unsupported)
        }
        _ => None,
    }
}

/// Whether an S3-compatible store rejected a request because it does not
/// implement part of it.
fn lacks_conditional_writes(error: &opendal::Error) -> bool {
    error.message().contains("\"NotImplemented\"") || error.to_string().contains("status: 501")
}

/// Whether S3 rejected the request's credentials themselves, rather than
/// refusing one object. These codes arrive in an error body, so a `HEAD` never
/// carries them.
pub(crate) fn credentials_rejected(error: &opendal::Error) -> bool {
    const CODES: [&str; 7] = [
        "InvalidAccessKeyId",
        "SignatureDoesNotMatch",
        "ExpiredToken",
        "InvalidToken",
        "TokenRefreshRequired",
        "RequestTimeTooSkewed",
        "InvalidSecurity",
    ];
    let message = error.message();
    CODES
        .iter()
        .any(|code| message.contains(&format!("\"{code}\"")))
}

/// The S3 endpoint in use: the configured one, else `AWS_ENDPOINT_URL_S3`.
pub(crate) fn s3_endpoint(config: &S3RemoteConfig) -> Option<String> {
    config
        .endpoint
        .clone()
        .or_else(|| std::env::var("AWS_ENDPOINT_URL_S3").ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(crate) const PLAIN_HTTP_ENDPOINT: &str = "plain http to another machine: request \
    signatures and cached objects travel unencrypted, and anyone on the path can \
    replace an object. Use https://, or keep plain http to a loopback address";

/// A plain `http://` endpoint on another machine.
pub(crate) fn plain_http_remote_endpoint(endpoint: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(endpoint) else {
        return false;
    };
    let host = url.host_str().unwrap_or_default();
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let loopback = host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
    url.scheme() == "http" && !loopback
}

/// What a failed remote request most likely means, in terms a user can act
/// on, when the store or the credential chain said. `region` is the
/// configured S3 region.
pub(crate) fn explain_remote_failure(
    error: &anyhow::Error,
    region: Option<&str>,
) -> Option<String> {
    if let Some(failure) = error.downcast_ref::<CredentialFailure>() {
        return Some(failure.fix());
    }
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<opendal::Error>())
        .and_then(|error| explain_opendal_failure(error, region))
}

fn explain_opendal_failure(error: &opendal::Error, region: Option<&str>) -> Option<String> {
    let text = error.to_string();
    let code = |name: &str| text.contains(&format!("\"{name}\""));
    if code("ExpiredToken") || code("TokenRefreshRequired") || code("InvalidToken") {
        return Some("the S3 credentials have expired or were revoked; refresh them".into());
    }
    if code("InvalidAccessKeyId") {
        return Some("the store does not know this access key ID".into());
    }
    if code("SignatureDoesNotMatch") {
        return Some("the secret key does not match the access key ID".into());
    }
    if code("RequestTimeTooSkewed") {
        return Some("this machine's clock is too far from the store's; correct the clock".into());
    }
    if let Some(actual) = bucket_region(&text) {
        let configured = region
            .map(|region| format!(", not {region}"))
            .unwrap_or_default();
        return Some(format!(
            "the bucket is in {actual}{configured}; set cache.remote.region = \"{actual}\""
        ));
    }
    if code("NoSuchBucket") {
        return Some("the bucket does not exist; check cache.remote.bucket".into());
    }
    None
}

/// The bucket's real region, from the `x-amz-bucket-region` header S3 sends
/// with a redirect, or from the message of a request signed for the wrong one.
fn bucket_region(text: &str) -> Option<&str> {
    let quoted = |start: &str, end: char| {
        let from = text.find(start)? + start.len();
        let rest = &text[from..];
        let region = &rest[..rest.find(end)?];
        (!region.is_empty()).then_some(region)
    };
    quoted("\"x-amz-bucket-region\": \"", '"').or_else(|| quoted("expecting '", '\''))
}

fn create_gcs_operator(config: &GcsRemoteConfig, pool_idle_secs: u64) -> Result<Operator> {
    ensure_rustls_provider();
    // Same connection and inactivity bounds as S3; see create_s3_operator.
    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(pool_idle_secs))
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(READ_INACTIVITY_TIMEOUT)
        .build()
        .context("building GCS HTTP client")?;
    let context = OperationContext::new()
        .with_http_transport(HttpTransporter::new(ReqwestTransport::new(client)));
    let mut builder = Gcs::default().bucket(&config.bucket);
    if let Some(endpoint) = &config.endpoint {
        // A custom endpoint is for an emulator, which takes unsigned requests.
        builder = builder.endpoint(endpoint).skip_signature();
    }
    let operator = Operator::new(builder)
        .context("building OpenDAL GCS operator")?
        .with_context(context);
    Ok(without_retry_layer(operator))
}

fn without_retry_layer(operator: Operator) -> Operator {
    // Retry ownership lives at the daemon operation boundary. Layering an
    // opaque transport retry underneath daemon retries multiplied deadlines,
    // and its backoff slept while callers held scarce concurrency permits.
    // One attempt per admission keeps queue/deadline/breaker accounting exact.
    operator
}

fn ensure_rustls_provider() {
    // Kache's reqwest client is compiled with rustls-no-provider. Install ring
    // before constructing the S3 transport; the operation is process-wide and
    // idempotent.
    let _ = rustls::crypto::ring::default_provider().install_default();
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
    /// `AWS_PROFILE` for the child. `None` keeps the inherited value.
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

        // The profile sources read the selected profile in-process. The
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

/// `KACHE_S3_ACCESS_KEY` and `KACHE_S3_SECRET_KEY`, when both are set.
fn kache_s3_keys() -> Option<Credential> {
    let access_key = std::env::var("KACHE_S3_ACCESS_KEY").ok();
    let secret_key = std::env::var("KACHE_S3_SECRET_KEY").ok();
    match (access_key, secret_key) {
        (Some(access_key_id), Some(secret_access_key)) => Some(Credential {
            access_key_id,
            secret_access_key,
            session_token: None,
            expires_in: None,
        }),
        (Some(_), None) => {
            tracing::warn!(
                "KACHE_S3_ACCESS_KEY is set but KACHE_S3_SECRET_KEY is missing — ignoring partial credentials"
            );
            None
        }
        (None, Some(_)) => {
            tracing::warn!(
                "KACHE_S3_SECRET_KEY is set but KACHE_S3_ACCESS_KEY is missing — ignoring partial credentials"
            );
            None
        }
        (None, None) => None,
    }
}

/// The S3 backend, signing with `kache_keys` when given and otherwise with
/// the credential chain.
fn s3_backend(
    config: &S3RemoteConfig,
    pool_idle_secs: u64,
    kache_keys: Option<Credential>,
) -> Result<OpenDalBackend> {
    let status = Arc::new(CredentialStatus::default());
    let credentials = KacheCredentialProvider::new(
        kache_keys,
        config.profile.clone(),
        s3_credentials::ambient_sources(&config.region),
        Arc::clone(&status),
    );
    let mut backend = OpenDalBackend::new(
        create_s3_operator(config, pool_idle_secs, credentials)?,
        format!("s3://{}", config.bucket),
    );
    backend.region = Some(config.region.clone());
    backend.credentials = Some(status);
    Ok(backend)
}

fn create_s3_operator(
    config: &S3RemoteConfig,
    pool_idle_secs: u64,
    credentials: impl ProvideCredential<Credential = Credential>,
) -> Result<Operator> {
    // reqwest is compiled with rustls-no-provider. Installing ring here keeps
    // direct library/test callers safe; the operation is idempotent when
    // another Kache HTTP client already installed it.
    ensure_rustls_provider();
    let mut client_builder = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(pool_idle_secs))
        // `pool_idle_timeout` only reaps idle pooled connections; without these an
        // endpoint that accepts a connection and then goes silent hangs the
        // operation forever. On the rustc-wrapper path that is an apparently-hung build.
        // The AWS SDK supplied a 3.1s connect timeout by default
        // (aws-config's SDK_DEFAULT_CONNECT_TIMEOUT); keep parity and add a
        // read-inactivity deadline. Deliberately NOT a total-request timeout,
        // which would cap large artifact transfers on slow links.
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(READ_INACTIVITY_TIMEOUT);

    if let Some(user_agent) = config
        .user_agent
        .as_deref()
        .filter(|ua| !ua.trim().is_empty())
    {
        client_builder = client_builder.user_agent(user_agent);
    }

    let client = client_builder.build().context("building S3 HTTP client")?;
    let context = OperationContext::new()
        .with_http_transport(HttpTransporter::new(ReqwestTransport::new(client)));

    let mut builder = S3::default()
        .bucket(&config.bucket)
        .region(&config.region)
        // Keep transport integrity without requiring a provider to implement
        // the newer full-object x-amz-checksum-* headers. Content-MD5 is
        // supported by AWS S3 and common S3-compatible PutObject endpoints.
        .checksum_algorithm("md5");
    if let Some(endpoint) = s3_endpoint(config) {
        if plain_http_remote_endpoint(&endpoint) {
            tracing::warn!("{endpoint}: {PLAIN_HTTP_ENDPOINT}");
        }
        builder = builder.endpoint(&endpoint);
    }

    // OpenDAL takes only a reqsign chain, which skips a provider that fails.
    // Kache's chain is its one provider and decides itself when to stop.
    builder = builder.credential_provider_chain(ProvideCredentialChain::new().push(credentials));

    let operator = Operator::new(builder)
        .context("building OpenDAL S3 operator")?
        .with_context(context);
    Ok(without_retry_layer(operator))
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
    Ok(without_retry_layer(operator))
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
        RemoteBackendConfig::S3(config) => s3_backend(config, pool_idle_secs, kache_s3_keys())?,
        RemoteBackendConfig::Gcs(config) => {
            let mut backend = OpenDalBackend::new(
                create_gcs_operator(config, pool_idle_secs)?,
                format!("gs://{}", config.bucket),
            );
            backend.refusal_may_be_absence = false;
            backend
        }
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
        RemoteBackendConfig::Oci(config) => {
            return Ok(Arc::new(oci::OciBackend::new(config).await?));
        }
    };

    Ok(Arc::new(backend))
}

/// An S3 backend that sends unsigned requests, as an anonymous reader does.
#[cfg(test)]
pub(crate) fn anonymous_s3_backend_for_bucket(endpoint: &str, bucket: &str) -> OpenDalBackend {
    ensure_rustls_provider();
    let client = reqwest::Client::builder().build().unwrap();
    let builder = S3::default()
        .bucket(bucket)
        .region("us-east-1")
        .endpoint(endpoint)
        .checksum_algorithm("md5")
        .skip_signature();
    let context = OperationContext::new()
        .with_http_transport(HttpTransporter::new(ReqwestTransport::new(client)));
    let operator = Operator::new(builder).unwrap().with_context(context);
    OpenDalBackend::new(operator, format!("s3://{bucket}"))
}

/// [`create_backend`], wrapped in a [`PullRequestBackend`] when this pull
/// request job has a prefix of its own.
pub async fn create_backend_for(
    remote: &RemoteConfig,
    pull_request_prefix: Option<&str>,
    pool_idle_secs: u64,
) -> Result<Arc<dyn RemoteBackend>> {
    let backend = create_backend(remote, pool_idle_secs).await?;
    Ok(match pull_request_prefix {
        Some(pull_request) => Arc::new(PullRequestBackend {
            inner: backend,
            base: remote.prefix.clone(),
            pull_request: pull_request.to_string(),
        }),
        None => backend,
    })
}

/// The remote as a pull request job sees it. Every key names an object under
/// the base prefix; a read looks there first and then under the pull request
/// prefix, and every write goes under the pull request prefix, so nothing a
/// pull request builds reaches the base prefix that trusted builds read.
/// Merged objects (manifests, shards) are read and written under the pull
/// request prefix only, so their conditional updates stay consistent.
pub struct PullRequestBackend {
    inner: Arc<dyn RemoteBackend>,
    base: String,
    pull_request: String,
}

impl PullRequestBackend {
    /// `key` moved from the base prefix to the pull request prefix.
    fn pull_request_key(&self, key: &str) -> String {
        let rest = key
            .strip_prefix(&self.base)
            .and_then(|rest| rest.strip_prefix('/'))
            .unwrap_or(key);
        crate::config::join_remote_key(&self.pull_request, rest)
    }

    /// A key under the pull request prefix, named as the base key it mirrors.
    fn base_key(&self, key: &str) -> String {
        let rest = key
            .strip_prefix(&self.pull_request)
            .and_then(|rest| rest.strip_prefix('/'))
            .unwrap_or(key);
        crate::config::join_remote_key(&self.base, rest)
    }
}

#[async_trait]
impl RemoteBackend for PullRequestBackend {
    async fn head(&self, key: &str) -> Result<bool> {
        Ok(self.inner.head(key).await? || self.inner.head(&self.pull_request_key(key)).await?)
    }

    async fn get(&self, key: &str, max_bytes: Option<u64>) -> Result<Option<GetObject>> {
        match self.inner.get(key, max_bytes).await? {
            Some(object) => Ok(Some(object)),
            None => self.inner.get(&self.pull_request_key(key), max_bytes).await,
        }
    }

    async fn get_into(
        &self,
        key: &str,
        max_bytes: Option<u64>,
        destination: &mut (dyn AsyncWrite + Unpin + Send),
    ) -> Result<Option<GetTransfer>> {
        // A missing object is reported before any bytes are written, so the
        // second read starts from an untouched `destination`.
        match self.inner.get_into(key, max_bytes, destination).await? {
            Some(transfer) => Ok(Some(transfer)),
            None => {
                self.inner
                    .get_into(&self.pull_request_key(key), max_bytes, destination)
                    .await
            }
        }
    }

    async fn get_versioned(
        &self,
        key: &str,
        max_bytes: Option<u64>,
    ) -> Result<Option<(GetObject, Option<String>)>> {
        self.inner
            .get_versioned(&self.pull_request_key(key), max_bytes)
            .await
    }

    async fn put_if_match(
        &self,
        key: &str,
        body: Vec<u8>,
        content_type: Option<&str>,
        expected: Option<&str>,
    ) -> Result<ConditionalPut> {
        self.inner
            .put_if_match(&self.pull_request_key(key), body, content_type, expected)
            .await
    }

    async fn put(&self, key: &str, body: Vec<u8>, content_type: Option<&str>) -> Result<()> {
        self.inner
            .put(&self.pull_request_key(key), body, content_type)
            .await
    }

    async fn put_if_absent(
        &self,
        key: &str,
        body: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<PutIfAbsentResult> {
        self.inner
            .put_if_absent(&self.pull_request_key(key), body, content_type)
            .await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = self.inner.list(prefix).await?;
        let pull_request = self.inner.list(&self.pull_request_key(prefix)).await?;
        keys.extend(pull_request.iter().map(|key| self.base_key(key)));
        keys.sort();
        keys.dedup();
        Ok(keys)
    }

    fn describe(&self, key: &str) -> String {
        self.inner.describe(key)
    }
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
                let (mut stream, _) =
                    tokio::time::timeout(Duration::from_secs(10), listener.accept())
                        .await
                        .expect("the operation must issue the expected HTTP request")
                        .unwrap();
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
        anonymous_s3_backend_for_bucket(endpoint, "bucket")
    }

    #[tokio::test]
    async fn get_into_delivers_bytes_before_the_remote_finishes() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(socket.read_u8().await.unwrap());
            }
            assert!(request.starts_with(b"GET /bucket/key HTTP/1.1\r\n"));
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhe")
                .await
                .unwrap();
            finish_rx.await.unwrap();
            socket.write_all(b"llo").await.unwrap();
        });
        let backend = anonymous_s3_backend(&endpoint);
        let (mut destination, mut received) = tokio::io::duplex(8);
        let download =
            tokio::spawn(async move { backend.get_into("key", Some(5), &mut destination).await });
        let mut prefix = [0; 2];
        tokio::time::timeout(Duration::from_secs(5), received.read_exact(&mut prefix))
            .await
            .expect("the sink must receive bytes without waiting for EOF")
            .unwrap();
        assert_eq!(&prefix, b"he");
        finish_tx.send(()).unwrap();
        let mut suffix = Vec::new();
        received.read_to_end(&mut suffix).await.unwrap();
        assert_eq!(suffix, b"llo");
        let transfer = download.await.unwrap().unwrap().unwrap();
        assert_eq!(transfer.bytes, 5);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn get_into_flushes_a_file_and_reports_write_failures() {
        let backend = memory_backend();
        backend.put("key", b"hello".to_vec(), None).await.unwrap();
        let mut file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let transfer = backend
            .get_into("key", Some(5), &mut file)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(transfer.bytes, 5);
        let mut file = file
            .try_into_std()
            .expect("the download must finish pending writes");
        std::io::Seek::rewind(&mut file).unwrap();
        let mut body = String::new();
        std::io::Read::read_to_string(&mut file, &mut body).unwrap();
        assert_eq!(body, "hello");

        let (mut closed_sink, reader) = tokio::io::duplex(1);
        drop(reader);
        let error = backend
            .get_into("key", Some(5), &mut closed_sink)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("writing body of memory://test/key"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn separate_backends_share_memory_until_the_last_body_clone_is_dropped() {
        let mut first = memory_backend();
        let mut second = memory_backend();
        assert!(Arc::ptr_eq(&first.download_memory, &second.download_memory));
        let budget = Arc::new(DownloadMemory::new(10));
        first.download_memory = budget.clone();
        second.download_memory = budget;
        first.put("key", b"hello".to_vec(), None).await.unwrap();
        second.put("key", b"world".to_vec(), None).await.unwrap();
        let body = first.get("key", Some(5 << 10)).await.unwrap().unwrap().body;
        let clone = body.clone();
        drop(body);
        let deadline = crate::remote_resilience::RemoteDeadline::from_millis(10);
        let error = deadline
            .run("download memory", second.get("key", Some(5 << 10)))
            .await
            .unwrap_err();
        assert!(
            error
                .downcast_ref::<crate::remote_resilience::RemoteDeadlineElapsed>()
                .is_some()
        );
        drop(clone);
        let object = tokio::time::timeout(Duration::from_secs(1), second.get("key", Some(5 << 10)))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(object.body, "world");
    }

    #[tokio::test]
    async fn misses_and_failed_reads_release_download_memory() {
        let backend = memory_backend_with_download_budget(1);
        backend.put("key", b"hello".to_vec(), None).await.unwrap();
        assert!(backend.get("absent", Some(5)).await.unwrap().is_none());
        assert!(backend.get("key", Some(4)).await.is_err());
        let object = tokio::time::timeout(Duration::from_secs(1), backend.get("key", Some(5)))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(object.body, "hello");
    }

    #[tokio::test]
    async fn streaming_downloads_do_not_wait_for_buffered_memory() {
        let backend = memory_backend_with_download_budget(1);
        backend.put("key", b"hello".to_vec(), None).await.unwrap();
        let buffered = backend.get("key", Some(5)).await.unwrap().unwrap();
        let mut file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let transfer = tokio::time::timeout(
            Duration::from_secs(1),
            backend.get_into("key", Some(5), &mut file),
        )
        .await
        .expect("a disk download must not wait for a body-buffer reservation")
        .unwrap()
        .unwrap();
        assert_eq!(transfer.bytes, 5);
        assert_eq!(buffered.body, "hello");
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
    async fn create_only_put_preserves_the_first_object() {
        let backend = memory_backend();

        assert_eq!(
            backend
                .put_if_absent("immutable/key", b"first".to_vec(), None)
                .await
                .unwrap(),
            PutIfAbsentResult::Created
        );
        assert_eq!(
            backend
                .put_if_absent("immutable/key", b"second".to_vec(), None)
                .await
                .unwrap(),
            PutIfAbsentResult::AlreadyExists
        );
        assert_eq!(
            backend
                .get("immutable/key", None)
                .await
                .unwrap()
                .unwrap()
                .body,
            "first"
        );
    }

    #[tokio::test]
    async fn gcs_wire_reads_writes_create_only_and_keeps_refusals_as_errors() {
        let (endpoint, requests) = mock_http_server(vec![
            "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello".to_string(),
            http_response("404 Not Found", ""),
            http_response("412 Precondition Failed", ""),
            http_response("403 Forbidden", ""),
        ])
        .await;
        let remote = RemoteConfig {
            prefix: "artifacts".to_string(),
            backend: RemoteBackendConfig::Gcs(GcsRemoteConfig {
                bucket: "builds".to_string(),
                endpoint: Some(endpoint),
            }),
        };
        let backend = create_backend(&remote, 30).await.unwrap();

        // Copy the body out and drop the object at once: a buffered object
        // holds its share of the process-wide download budget until then.
        let body = backend
            .get("artifacts/present", Some(64))
            .await
            .unwrap()
            .unwrap()
            .body
            .to_vec();
        assert_eq!(body, b"hello");
        assert!(
            backend
                .get("artifacts/absent", Some(64))
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            backend
                .put_if_absent("artifacts/present", b"again".to_vec(), None)
                .await
                .unwrap(),
            PutIfAbsentResult::AlreadyExists
        );
        assert!(
            backend.get("artifacts/refused", Some(64)).await.is_err(),
            "a GCS refusal is not a missing object"
        );

        let requests = requests.await.unwrap();
        assert!(
            requests[0].starts_with("GET /storage/v1/b/builds/o/artifacts%2Fpresent?alt=media"),
            "{}",
            requests[0]
        );
        assert!(
            requests[2].contains("ifGenerationMatch=0"),
            "{}",
            requests[2]
        );
    }

    fn pull_request_view(inner: Arc<dyn RemoteBackend>) -> PullRequestBackend {
        PullRequestBackend {
            inner,
            base: "artifacts".to_string(),
            pull_request: "artifacts-pr".to_string(),
        }
    }

    #[tokio::test]
    async fn a_pull_request_job_writes_only_under_its_own_prefix() {
        let inner: Arc<dyn RemoteBackend> = Arc::new(memory_backend());
        let view = pull_request_view(inner.clone());
        view.put("artifacts/v3/packs/a", b"pr".to_vec(), None)
            .await
            .unwrap();
        view.put_if_absent("artifacts/v3/packs/b", b"pr".to_vec(), None)
            .await
            .unwrap();
        assert!(!inner.head("artifacts/v3/packs/a").await.unwrap());
        assert!(!inner.head("artifacts/v3/packs/b").await.unwrap());
        assert!(inner.head("artifacts-pr/v3/packs/a").await.unwrap());
        assert!(inner.head("artifacts-pr/v3/packs/b").await.unwrap());
    }

    #[tokio::test]
    async fn a_pull_request_job_reads_the_base_prefix_first() {
        let inner: Arc<dyn RemoteBackend> = Arc::new(memory_backend());
        inner
            .put("artifacts/k/both", b"base".to_vec(), None)
            .await
            .unwrap();
        inner
            .put("artifacts-pr/k/both", b"pr".to_vec(), None)
            .await
            .unwrap();
        inner
            .put("artifacts-pr/k/pr-only", b"pr".to_vec(), None)
            .await
            .unwrap();
        let view = pull_request_view(inner);
        let read = |key: &'static str| {
            let view = &view;
            async move {
                view.get(key, Some(4096))
                    .await
                    .unwrap()
                    .map(|o| o.body.to_vec())
            }
        };
        assert_eq!(read("artifacts/k/both").await.unwrap(), b"base");
        assert_eq!(read("artifacts/k/pr-only").await.unwrap(), b"pr");
        assert_eq!(read("artifacts/k/none").await, None);
        assert!(view.head("artifacts/k/pr-only").await.unwrap());
        assert!(!view.head("artifacts/k/none").await.unwrap());

        let mut body = Vec::new();
        let transfer = view
            .get_into("artifacts/k/pr-only", None, &mut body)
            .await
            .unwrap();
        assert!(transfer.is_some());
        assert_eq!(body, b"pr");

        let listed = view.list("artifacts/k/").await.unwrap();
        assert_eq!(listed, ["artifacts/k/both", "artifacts/k/pr-only"]);

        // Merged objects are read from the pull request prefix alone.
        let (object, _etag) = view
            .get_versioned("artifacts/k/both", Some(4096))
            .await
            .unwrap()
            .expect("the pull request's copy");
        assert_eq!(object.body.to_vec(), b"pr");
        drop(object);
        assert!(
            view.get_versioned("artifacts/k/none", Some(4096))
                .await
                .unwrap()
                .is_none()
        );

        assert_eq!(
            view.describe("artifacts/k/both"),
            "memory://test/artifacts/k/both"
        );
    }

    #[tokio::test]
    async fn a_pull_request_job_merges_manifests_inside_its_own_prefix() {
        let inner: Arc<dyn RemoteBackend> = Arc::new(memory_backend());
        let base_manifest =
            b"{\"version\":3,\"created\":\"x\",\"manifest_key\":\"id/t\",\"entries\":[]}";
        inner
            .put(
                "artifacts/_manifests/id/t.json",
                base_manifest.to_vec(),
                None,
            )
            .await
            .unwrap();
        let view = pull_request_view(inner.clone());
        let manifest = crate::remote::BuildManifest {
            version: 3,
            created: "now".to_string(),
            manifest_key: "id/t".to_string(),
            entries: vec![crate::remote::ManifestEntry {
                cache_key: "pr-entry".to_string(),
                crate_name: "c".to_string(),
                compile_time_ms: 1,
                artifact_size: 1,
            }],
        };
        crate::remote::upload_manifest(&view, "artifacts", "id/t", &manifest, None)
            .await
            .unwrap();
        // Bodies are copied out at once: a held buffered object keeps its share
        // of the process-wide download budget.
        let base = inner
            .get("artifacts/_manifests/id/t.json", Some(4096))
            .await
            .unwrap()
            .unwrap()
            .body
            .to_vec();
        assert_eq!(base, base_manifest, "the base manifest is untouched");
        let pr = inner
            .get("artifacts-pr/_manifests/id/t.json", Some(4096))
            .await
            .unwrap()
            .expect("the pull request's own manifest")
            .body
            .to_vec();
        assert!(String::from_utf8_lossy(&pr).contains("pr-entry"));
    }

    #[test]
    fn s3_failures_are_explained_by_their_code_or_region() {
        let explain = |message: &str, region: Option<&str>| {
            explain_opendal_failure(
                &opendal::Error::new(ErrorKind::PermissionDenied, message.to_string()),
                region,
            )
        };
        let code = |name: &str| format!(r#"S3Error {{ code: "{name}" }}"#);
        for name in ["ExpiredToken", "TokenRefreshRequired", "InvalidToken"] {
            assert!(
                explain(&code(name), None).unwrap().contains("expired"),
                "{name}"
            );
        }
        assert!(
            explain(&code("InvalidAccessKeyId"), None)
                .unwrap()
                .contains("access key ID")
        );
        assert!(
            explain(&code("SignatureDoesNotMatch"), None)
                .unwrap()
                .contains("secret key")
        );
        assert!(
            explain(&code("RequestTimeTooSkewed"), None)
                .unwrap()
                .contains("clock")
        );
        assert!(
            explain(&code("NoSuchBucket"), None)
                .unwrap()
                .contains("bucket does not exist")
        );
        assert_eq!(
            explain(
                r#"headers: {"x-amz-bucket-region": "eu-west-1"}"#,
                Some("us-east-1")
            )
            .unwrap(),
            r#"the bucket is in eu-west-1, not us-east-1; set cache.remote.region = "eu-west-1""#
        );
        assert_eq!(
            explain(
                "the region 'us-east-1' is wrong; expecting 'eu-west-2'",
                None
            )
            .unwrap(),
            r#"the bucket is in eu-west-2; set cache.remote.region = "eu-west-2""#
        );
        assert_eq!(explain(&code("AccessDenied"), None), None);
        assert_eq!(explain("expecting ''", None), None);
    }

    #[test]
    fn plain_http_is_flagged_only_for_another_machine() {
        assert!(plain_http_remote_endpoint("http://10.0.0.5:9000"));
        assert!(plain_http_remote_endpoint("http://minio.internal"));
        for endpoint in [
            "http://127.0.0.1:9000",
            "http://localhost:9000",
            "http://[::1]:9000",
            "https://minio.internal",
            "not a url",
        ] {
            assert!(!plain_http_remote_endpoint(endpoint), "{endpoint}");
        }
    }

    #[tokio::test]
    async fn s3_wire_errors_carry_the_explanation() {
        let error = |code: &str| {
            format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{code}</Code>\
                 <Message>m</Message><RequestId>test</RequestId></Error>"
            )
        };
        let redirect = "HTTP/1.1 301 Moved Permanently\r\nx-amz-bucket-region: eu-west-1\r\n\
            Content-Length: 0\r\nConnection: close\r\n\r\n"
            .to_string();
        let (endpoint, _requests) = mock_http_server(vec![
            http_response("403 Forbidden", &error("ExpiredToken")),
            redirect,
        ])
        .await;
        let mut backend = anonymous_s3_backend(&endpoint);
        backend.region = Some("us-east-1".to_string());
        let expired = backend.get("key", Some(1024)).await.unwrap_err();
        assert!(
            format!("{expired:#}").contains("expired or were revoked"),
            "{expired:#}"
        );
        let moved = backend.get("key", Some(1024)).await.unwrap_err();
        assert!(
            format!("{moved:#}").contains("the bucket is in eu-west-1, not us-east-1"),
            "{moved:#}"
        );
    }

    #[test]
    fn create_only_error_classification_is_exact() {
        let classify = |kind, message: &str| {
            classify_create_error(&opendal::Error::new(kind, message.to_string()))
        };
        assert_eq!(
            classify(ErrorKind::ConditionNotMatch, ""),
            Some(PutIfAbsentResult::AlreadyExists)
        );
        assert_eq!(
            classify(ErrorKind::AlreadyExists, ""),
            Some(PutIfAbsentResult::AlreadyExists)
        );
        assert_eq!(
            classify(ErrorKind::Conflict, ""),
            Some(PutIfAbsentResult::AlreadyExists)
        );
        assert_eq!(
            classify(ErrorKind::Unsupported, ""),
            Some(PutIfAbsentResult::Unsupported)
        );
        assert_eq!(
            classify(
                ErrorKind::Unexpected,
                r#"S3Error { code: "NotImplemented" }"#
            ),
            Some(PutIfAbsentResult::Unsupported)
        );
        assert_eq!(classify(ErrorKind::PermissionDenied, ""), None);
        assert_eq!(classify(ErrorKind::Unexpected, "timed out"), None);
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
            user_agent: Some("custom-ua/1.0".to_string()),
        };
        s3_backend(&config, 30, None).expect("S3 backend builds without network I/O");
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
        // Send headers alone: the size check must reject without reading a
        // body. Reading it would produce a truncation error instead.
        let response = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\n";
        let (endpoint, _requests) = mock_http_server(vec![response.to_string()]).await;
        let backend = anonymous_s3_backend(&endpoint);

        let error = backend
            .get("key", Some(4))
            .await
            .expect_err("content-length above cap must fail")
            .to_string();
        assert!(error.contains("too large"), "{error}");
    }

    #[tokio::test]
    async fn s3_wire_accepts_a_body_at_the_size_limit() {
        let (endpoint, _requests) = mock_http_server(vec![http_response("200 OK", "hello")]).await;
        let backend = anonymous_s3_backend(&endpoint);

        let fetched = backend.get("key", Some(5)).await.unwrap().unwrap();
        assert_eq!(fetched.body, "hello");
    }

    #[tokio::test]
    async fn v3_download_rejects_oversize_before_publishing_an_entry() {
        // Advertise one byte above 8 GiB without allocating a large body. Pin
        // the public download path and its ceiling, not just Backend::get.
        let response = "HTTP/1.1 200 OK\r\nContent-Length: 8589934593\r\n\
                        Connection: close\r\n\r\n";
        let (endpoint, requests) = mock_http_server(vec![response.to_string()]).await;
        let backend = anonymous_s3_backend(&endpoint);
        let remote = RemoteConfig::test_s3("bucket", "artifacts");
        let layout = crate::remote_layout::RemoteLayout::new(&backend, &remote);
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("entry");
        let blobs = temp.path().join("blobs");

        let error = layout
            .download_entry_until("key123", "foo", &destination, &blobs, None)
            .await
            .err()
            .expect("an oversized v3 pack must be rejected before extraction");
        let message = format!("{error:#}");
        assert!(
            message.contains("too large: 8589934593 bytes (max 8589934592)"),
            "{message}"
        );
        assert!(
            std::fs::read_dir(temp.path()).unwrap().next().is_none(),
            "a rejected download must not publish files or leave extraction debris"
        );
        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].lines().next(),
            Some("GET /bucket/artifacts/v3/packs/foo/key123.tar.zst HTTP/1.1")
        );
    }

    #[tokio::test]
    async fn v3_download_timeout_discards_the_partial_file() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(socket.read_u8().await.unwrap());
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\npartial")
                .await
                .unwrap();
            started_tx.send(()).unwrap();
            // Keep the response open until the caller cancels it.
            let mut remaining = Vec::new();
            let _ = socket.read_to_end(&mut remaining).await;
        });
        let backend = anonymous_s3_backend(&endpoint);
        let remote = RemoteConfig::test_s3("bucket", "artifacts");
        let layout = crate::remote_layout::RemoteLayout::new(&backend, &remote);
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("entry");
        let blobs = temp.path().join("blobs");
        let deadline = Instant::now() + Duration::from_secs(1);
        let download =
            layout.download_entry_until("key123", "foo", &destination, &blobs, Some(deadline));
        let (result, started) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(download, started_rx)
        })
        .await
        .expect("the restore must start its GET before the test deadline");
        started.expect("the timeout must occur during a response body");
        let error = result
            .err()
            .expect("an incomplete response must hit its deadline");
        assert!(format!("{error:#}").contains("deadline"), "{error:#}");
        assert!(std::fs::read_dir(temp.path()).unwrap().next().is_none());
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
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
    async fn s3_wire_create_only_put_uses_a_conditional_request() {
        let conflict = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
            <Error><Code>PreconditionFailed</Code><Message>already exists</Message>\
            <RequestId>test</RequestId></Error>";
        let (endpoint, requests) = mock_http_server(vec![
            http_response("200 OK", ""),
            http_response("412 Precondition Failed", conflict),
        ])
        .await;
        let backend = anonymous_s3_backend(&endpoint);

        assert_eq!(
            backend
                .put_if_absent("immutable", b"first".to_vec(), None)
                .await
                .unwrap(),
            PutIfAbsentResult::Created
        );
        assert_eq!(
            backend
                .put_if_absent("immutable", b"second".to_vec(), None)
                .await
                .unwrap(),
            PutIfAbsentResult::AlreadyExists
        );

        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 2);
        for request in requests {
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("\r\nif-none-match: *\r\n"),
                "{request}"
            );
        }
    }

    #[tokio::test]
    async fn s3_wire_a_racing_conditional_write_is_a_conflict() {
        let racing = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
            <Error><Code>ConditionalRequestConflict</Code><Message>in progress</Message>\
            <RequestId>test</RequestId></Error>";
        let (endpoint, _requests) = mock_http_server(vec![
            http_response("409 Conflict", racing),
            http_response("409 Conflict", racing),
        ])
        .await;
        let backend = anonymous_s3_backend(&endpoint);

        assert_eq!(
            backend
                .put_if_absent("immutable", b"same".to_vec(), None)
                .await
                .unwrap(),
            PutIfAbsentResult::AlreadyExists
        );
        assert_eq!(
            backend
                .put_if_match("manifest", b"b".to_vec(), None, Some("\"v1\""))
                .await
                .unwrap(),
            ConditionalPut::Conflict
        );
    }

    #[tokio::test]
    async fn s3_wire_versioned_get_returns_the_response_etag() {
        let response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nETag: \"v1\"\r\n\
            Content-Type: application/json\r\nConnection: close\r\n\r\n{}";
        let (endpoint, _requests) = mock_http_server(vec![response.to_string()]).await;
        let backend = anonymous_s3_backend(&endpoint);

        let (object, etag) = backend
            .get_versioned("manifest", Some(1024))
            .await
            .unwrap()
            .expect("the object exists");
        assert_eq!(&object.body[..], b"{}");
        assert_eq!(etag.as_deref(), Some("\"v1\""));
    }

    #[tokio::test]
    async fn s3_wire_conditional_put_sends_the_precondition_and_reports_conflicts() {
        let conflict = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
            <Error><Code>PreconditionFailed</Code><Message>changed</Message>\
            <RequestId>test</RequestId></Error>";
        let (endpoint, requests) = mock_http_server(vec![
            http_response("200 OK", ""),
            http_response("412 Precondition Failed", conflict),
            http_response("200 OK", ""),
        ])
        .await;
        let backend = anonymous_s3_backend(&endpoint);

        let stored = backend
            .put_if_match(
                "manifest",
                b"a".to_vec(),
                Some("application/json"),
                Some("\"v1\""),
            )
            .await
            .unwrap();
        assert_eq!(stored, ConditionalPut::Stored);
        let conflict = backend
            .put_if_match("manifest", b"b".to_vec(), None, Some("\"v1\""))
            .await
            .unwrap();
        assert_eq!(conflict, ConditionalPut::Conflict);
        let created = backend
            .put_if_match("manifest", b"c".to_vec(), None, None)
            .await
            .unwrap();
        assert_eq!(created, ConditionalPut::Stored);

        let requests: Vec<String> = requests
            .await
            .unwrap()
            .into_iter()
            .map(|request| request.to_ascii_lowercase())
            .collect();
        assert_eq!(requests.len(), 3);
        assert!(
            requests[0].contains("\r\ncontent-type: application/json\r\n"),
            "{}",
            requests[0]
        );
        for request in &requests[..2] {
            assert!(request.contains("\r\nif-match: \"v1\"\r\n"), "{request}");
            assert!(!request.contains("if-none-match"), "{request}");
        }
        assert!(
            requests[2].contains("\r\nif-none-match: *\r\n"),
            "{}",
            requests[2]
        );
        assert!(!requests[2].contains("\r\nif-match:"), "{}", requests[2]);
    }

    /// A transport that implements only the required methods.
    struct GetOnly;

    #[async_trait]
    impl RemoteBackend for GetOnly {
        async fn head(&self, _key: &str) -> Result<bool> {
            Ok(true)
        }

        async fn get(&self, _key: &str, _max_bytes: Option<u64>) -> Result<Option<GetObject>> {
            Ok(Some(GetObject {
                body: Bytes::from_static(b"body"),
                request_ms: 0,
                body_ms: 0,
            }))
        }

        async fn put(&self, _key: &str, _body: Vec<u8>, _content_type: Option<&str>) -> Result<()> {
            Ok(())
        }

        async fn list(&self, _prefix: &str) -> Result<Vec<String>> {
            Ok(Vec::new())
        }

        fn describe(&self, key: &str) -> String {
            key.to_string()
        }
    }

    #[tokio::test]
    async fn default_conditional_methods_offer_no_entity_tag_and_no_condition() {
        let (object, etag) = GetOnly
            .get_versioned("key", None)
            .await
            .unwrap()
            .expect("the object exists");
        assert_eq!(&object.body[..], b"body");
        assert_eq!(etag, None);
        let outcome = GetOnly
            .put_if_match("key", Vec::new(), None, Some("\"v1\""))
            .await
            .unwrap();
        assert_eq!(outcome, ConditionalPut::Unsupported);
    }

    #[tokio::test]
    async fn s3_wire_refused_reads_are_misses_unless_the_credentials_were_rejected() {
        let error = |code: &str| {
            format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{code}</Code>\
                 <Message>m</Message><RequestId>test</RequestId></Error>"
            )
        };
        let (endpoint, _requests) = mock_http_server(vec![
            http_response("404 Not Found", ""),
            http_response("500 Internal Server Error", ""),
            // A HEAD response has no body to explain itself.
            http_response("403 Forbidden", ""),
            http_response("403 Forbidden", &error("AccessDenied")),
            http_response("403 Forbidden", &error("ExpiredToken")),
        ])
        .await;
        let backend = anonymous_s3_backend(&endpoint);
        assert!(!backend.head("absent").await.unwrap());
        assert!(backend.head("failing").await.is_err());
        assert!(!backend.head("refused").await.unwrap());
        assert!(backend.get("refused", Some(1024)).await.unwrap().is_none());
        assert!(backend.get("expired", Some(1024)).await.is_err());
    }

    #[test]
    fn only_the_first_refusal_is_reported() {
        let backend = memory_backend();
        assert!(backend.first_refusal());
        assert!(!backend.first_refusal());
    }

    #[test]
    fn rejected_credentials_are_recognised_by_their_s3_code() {
        let error =
            |message: &str| opendal::Error::new(ErrorKind::PermissionDenied, message.to_string());
        for code in [
            "InvalidAccessKeyId",
            "SignatureDoesNotMatch",
            "ExpiredToken",
            "InvalidToken",
            "TokenRefreshRequired",
            "RequestTimeTooSkewed",
            "InvalidSecurity",
        ] {
            assert!(credentials_rejected(&error(&format!(
                r#"S3Error {{ code: "{code}" }}"#
            ))));
        }
        assert!(!credentials_rejected(&error(
            r#"S3Error { code: "AccessDenied" }"#
        )));
        assert!(!credentials_rejected(&error("")));
    }

    #[test]
    fn a_filesystem_permission_error_is_not_a_miss() {
        let root = tempfile::tempdir().unwrap();
        let config = FilesystemRemoteConfig {
            root: root.path().to_path_buf(),
            atomic_write_dir: root.path().join(".staging"),
        };
        let mut backend = OpenDalBackend::new(
            create_filesystem_operator(&config).unwrap(),
            "file://".into(),
        );
        backend.filesystem_root = Some(root.path().canonicalize().unwrap());
        let refused = opendal::Error::new(ErrorKind::PermissionDenied, "EACCES");
        assert!(!backend.refusal_means_absent(&refused));
        let s3 = anonymous_s3_backend("http://127.0.0.1:1");
        assert!(s3.refusal_means_absent(&refused));
        let other = opendal::Error::new(ErrorKind::Unexpected, "boom");
        assert!(!s3.refusal_means_absent(&other));
    }

    #[test]
    fn conditional_error_classification_is_exact() {
        let classify = |kind, message: &str, replacing| {
            classify_conditional_error(&opendal::Error::new(kind, message.to_string()), replacing)
        };
        for replacing in [true, false] {
            for kind in [
                ErrorKind::ConditionNotMatch,
                ErrorKind::AlreadyExists,
                ErrorKind::Conflict,
            ] {
                assert_eq!(
                    classify(kind, "", replacing),
                    Some(ConditionalPut::Conflict)
                );
            }
            assert_eq!(
                classify(ErrorKind::Unsupported, "", replacing),
                Some(ConditionalPut::Unsupported)
            );
            assert_eq!(classify(ErrorKind::PermissionDenied, "", replacing), None);
            assert_eq!(
                classify(ErrorKind::Unexpected, "timed out", replacing),
                None
            );
        }
        // The object an If-Match named was deleted: read again.
        assert_eq!(
            classify(ErrorKind::NotFound, "", true),
            Some(ConditionalPut::Conflict)
        );
        assert_eq!(classify(ErrorKind::NotFound, "", false), None);
        assert_eq!(
            classify(
                ErrorKind::Unexpected,
                r#"S3Error { code: "NotImplemented" }"#,
                true
            ),
            Some(ConditionalPut::Unsupported)
        );
        let status_501 = opendal::Error::new(ErrorKind::Unexpected, "no body")
            .with_context("response", "Parts { status: 501, version: HTTP/1.1 }");
        assert_eq!(
            classify_conditional_error(&status_501, true),
            Some(ConditionalPut::Unsupported)
        );
    }

    #[tokio::test]
    async fn s3_wire_conditional_put_separates_unsupported_from_failed_writes() {
        let error = |code: &str| {
            format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{code}</Code>\
                 <Message>m</Message><RequestId>test</RequestId></Error>"
            )
        };
        let (endpoint, _requests) = mock_http_server(vec![
            http_response("501 Not Implemented", &error("NotImplemented")),
            http_response("400 Bad Request", &error("NotImplemented")),
            http_response("400 Bad Request", &error("InvalidArgument")),
            http_response("404 Not Found", &error("NoSuchKey")),
        ])
        .await;
        let backend = anonymous_s3_backend(&endpoint);
        let replace = || backend.put_if_match("manifest", b"{}".to_vec(), None, Some("\"v1\""));

        assert_eq!(replace().await.unwrap(), ConditionalPut::Unsupported);
        assert_eq!(replace().await.unwrap(), ConditionalPut::Unsupported);
        assert!(replace().await.is_err());
        assert_eq!(replace().await.unwrap(), ConditionalPut::Conflict);
    }

    #[tokio::test]
    async fn filesystem_backend_declines_a_conditional_replace() {
        let root = tempfile::tempdir().unwrap();
        let remote = RemoteConfig {
            prefix: "artifacts".to_string(),
            backend: RemoteBackendConfig::Filesystem(FilesystemRemoteConfig {
                root: root.path().to_path_buf(),
                atomic_write_dir: root.path().join(".staging"),
            }),
        };
        let backend = create_backend(&remote, 30).await.unwrap();

        let outcome = backend
            .put_if_match(
                "artifacts/manifest.json",
                b"{}".to_vec(),
                None,
                Some("\"v1\""),
            )
            .await
            .unwrap();
        assert_eq!(outcome, ConditionalPut::Unsupported);
        assert!(
            backend
                .get("artifacts/manifest.json", None)
                .await
                .unwrap()
                .is_none()
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

    /// The profile the chain selects in-process does not reach a
    /// `credential_process` child, which is a separate process inheriting this
    /// one's environment. A helper that shells out to AWS tooling would
    /// otherwise resolve the ambient profile and return credentials for the
    /// wrong account.
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

    struct ScopedEnvVar {
        key: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl ScopedEnvVar {
        fn set(key: &'static str, val: &str) -> Self {
            let previous = std::env::var_os(key);
            unsafe { std::env::set_var(key, val) };
            Self { key, previous }
        }
    }

    impl Drop for ScopedEnvVar {
        fn drop(&mut self) {
            match &self.previous {
                Some(previous) => unsafe { std::env::set_var(self.key, previous) },
                None => unsafe { std::env::remove_var(self.key) },
            }
        }
    }

    /// An S3 backend whose requests go to `endpoint`, signed with the
    /// `KACHE_S3_*` keys read the way `create_backend` reads them.
    fn backend_with_kache_keys(endpoint: String, user_agent: Option<&str>) -> OpenDalBackend {
        let config = S3RemoteConfig {
            bucket: "bucket".to_string(),
            endpoint: Some(endpoint),
            region: "us-east-1".to_string(),
            profile: None,
            user_agent: user_agent.map(str::to_string),
        };
        let _lock = crate::test_support::process_state_test_lock();
        let _access = ScopedEnvVar::set("KACHE_S3_ACCESS_KEY", "mock-access-key");
        let _secret = ScopedEnvVar::set("KACHE_S3_SECRET_KEY", "mock-secret-key");
        s3_backend(&config, 30, kache_s3_keys()).unwrap()
    }

    #[tokio::test]
    async fn s3_wire_sends_custom_user_agent() {
        let (endpoint, requests) = mock_http_server(vec![http_response("404 Not Found", "")]).await;
        let backend = backend_with_kache_keys(endpoint, Some("kache-custom-agent/9.9"));

        assert!(
            backend
                .get("nested/key", Some(1024))
                .await
                .unwrap()
                .is_none()
        );
        let requests = requests.await.unwrap();
        let request_text = &requests[0];
        assert!(
            request_text.lines().any(|line| line
                .to_ascii_lowercase()
                .starts_with("user-agent: kache-custom-agent/9.9")),
            "expected custom User-Agent header in request: {request_text}"
        );
    }

    /// `kache doctor` names the source of the credentials the probe signed with.
    #[tokio::test]
    async fn s3_backend_names_the_credentials_it_signed_with() {
        let (endpoint, requests) = mock_http_server(vec![http_response("404 Not Found", "")]).await;
        let backend = backend_with_kache_keys(endpoint, None);
        assert_eq!(backend.credential_source(), None, "nothing is signed yet");

        assert!(backend.get("key", Some(1024)).await.unwrap().is_none());
        assert_eq!(
            backend.credential_source().as_deref(),
            Some("KACHE_S3_ACCESS_KEY and KACHE_S3_SECRET_KEY")
        );
        let requests = requests.await.unwrap();
        assert!(
            requests[0].contains("Credential=mock-access-key/"),
            "{}",
            requests[0]
        );
    }

    #[test]
    fn a_backend_without_a_credential_chain_names_no_source() {
        assert_eq!(GetOnly.credential_source(), None);
        assert_eq!(memory_backend().credential_source(), None);
    }

    fn partial_keys() -> CredentialFailure {
        CredentialFailure::PartialEnvironmentKeys {
            present: "AWS_ACCESS_KEY_ID",
            missing: "AWS_SECRET_ACCESS_KEY",
        }
    }

    /// A credential chain that always stops, recording why as Kache's does.
    #[derive(Debug)]
    struct StoppedChain(Arc<CredentialStatus>);

    impl ProvideCredential for StoppedChain {
        type Credential = Credential;

        async fn provide_credential(
            &self,
            _context: &reqsign_core::Context,
        ) -> reqsign_core::Result<Option<Credential>> {
            self.0.failed(&partial_keys());
            Err(reqsign_core::Error::config_invalid(
                partial_keys().to_string(),
            ))
        }
    }

    /// OpenDAL reports only that signing failed. The request error must say
    /// why, and the request must not go out unsigned.
    #[tokio::test]
    async fn a_stopped_credential_chain_names_its_reason_in_the_request_error() {
        // A signed or unsigned GET would get this miss and succeed.
        let (endpoint, _requests) =
            mock_http_server(vec![http_response("404 Not Found", "")]).await;
        let config = S3RemoteConfig {
            bucket: "bucket".to_string(),
            endpoint: Some(endpoint),
            region: "us-east-1".to_string(),
            profile: None,
            user_agent: None,
        };
        let status = Arc::new(CredentialStatus::default());
        let operator = create_s3_operator(&config, 30, StoppedChain(Arc::clone(&status))).unwrap();
        let mut backend = OpenDalBackend::new(operator, "s3://bucket".to_string());
        backend.credentials = Some(status);

        let error = backend
            .get("key", Some(1024))
            .await
            .expect_err("no credentials, no request");
        assert_eq!(
            error.downcast_ref::<CredentialFailure>(),
            Some(&partial_keys())
        );
        let text = format!("{error:#}");
        assert!(
            text.starts_with(
                "AWS_ACCESS_KEY_ID is set without AWS_SECRET_ACCESS_KEY: GET s3://bucket/key: "
            ),
            "{text}"
        );
        assert_eq!(
            explain_remote_failure(&error, Some("us-east-1")).as_deref(),
            Some("set both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or neither")
        );
    }

    /// A failure the chain recorded is not blamed for an error that did not
    /// come from signing.
    #[test]
    fn a_recorded_credential_failure_stays_out_of_unrelated_errors() {
        let status = Arc::new(CredentialStatus::default());
        status.failed(&partial_keys());
        let mut backend = memory_backend();
        backend.credentials = Some(status);

        let error = backend.contextual_error(
            "GET",
            "key",
            opendal::Error::new(ErrorKind::Unexpected, "connection reset"),
        );
        assert!(
            error.downcast_ref::<CredentialFailure>().is_none(),
            "{error:#}"
        );
    }
}
