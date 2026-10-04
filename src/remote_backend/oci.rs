//! OCI Distribution storage. Each object is a single-layer artifact. Ordinary
//! keys are encoded in tags for cheap listing; long keys use hashed tags and
//! manifest annotations. Reads validate the manifest's original key binding.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::Instant;

use anyhow::{Context, Result};
use async_trait::async_trait;
use base64::Engine;
use bytes::Bytes;
use docker_credential::{CredentialRetrievalError, DockerCredential};
use oci_client::client::{ClientConfig, ClientProtocol, Config, ImageLayer};
use oci_client::errors::{OciDistributionError, OciErrorCode};
use oci_client::manifest::{OciDescriptor, OciImageManifest};
use oci_client::secrets::RegistryAuth;
use oci_client::token_cache::RegistryOperation;
use oci_client::{Client, Reference};
use opendal::{Error, ErrorKind};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use super::download_memory::{BudgetedBody, DOWNLOAD_MEMORY};
use super::{GetObject, GetTransfer, RemoteBackend};
use crate::config::{OciRemoteConfig, normalize_remote_prefix};

const TAG_PREFIX: &str = "kache-v2-";
const LEGACY_TAG_PREFIX: &str = "kache-v1-";
const KEY_ANNOTATION: &str = "ninja.kunobi.kache.key";
const ARTIFACT_TYPE: &str = "application/vnd.kache.cache-object.v1";
const EMPTY_CONFIG_TYPE: &str = "application/vnd.oci.empty.v1+json";
const PAGE_SIZE: usize = 100;
const MAX_REGISTRY_METADATA_BYTES: usize = 8 << 20;
const MAX_EMBEDDED_BYTES: usize = 16 << 10;
// 87 binary bytes encode to 116 characters; the tag prefix adds 11.
const MAX_TAG_KEY_BYTES: usize = 87;

mod auth;
mod transport;

pub(super) struct OciBackend {
    repository: Reference,
    transport: transport::Transport,
    base_url: String,
}

impl OciBackend {
    pub(super) async fn new(config: &OciRemoteConfig) -> Result<Self> {
        let repository = config.reference()?;
        let source = auth::CredentialSource::from_environment()?;
        // Validate the provider before starting a daemon with unusable auth.
        source.load(repository.resolve_registry()).await?;
        Self::with_credentials(config, source)
    }

    #[cfg(test)]
    fn with_auth(config: &OciRemoteConfig, auth: RegistryAuth) -> Result<Self> {
        Self::with_credentials(config, auth::CredentialSource::Fixed(auth))
    }

    fn with_credentials(
        config: &OciRemoteConfig,
        credentials: auth::CredentialSource,
    ) -> Result<Self> {
        super::ensure_rustls_provider();
        let repository = config.reference()?;
        let transport = transport::Transport::new(config, credentials)?;
        let base_url = format!(
            "{}://{}/v2/{}",
            if config.insecure { "http" } else { "https" },
            repository.resolve_registry(),
            repository.repository()
        );
        Ok(Self {
            repository,
            transport,
            base_url,
        })
    }

    async fn object_manifest(
        &self,
        key: &str,
    ) -> Result<Option<(OciImageManifest, Option<String>)>> {
        if let Some(manifest) = self.manifest(&object_tag(key)).await? {
            return Ok(Some(manifest));
        }
        self.manifest(&legacy_object_tag(key)).await
    }

    async fn manifest(&self, tag: &str) -> Result<Option<(OciImageManifest, Option<String>)>> {
        let Some((body, digest)) = self
            .metadata_get(&format!("{}/manifests/{tag}", self.base_url))
            .await?
        else {
            return Ok(None);
        };
        let actual = ImageLayer::new(body.clone(), String::new(), None).sha256_digest();
        if digest.is_some_and(|digest| digest != actual) {
            return Err(
                Error::new(ErrorKind::RangeNotSatisfied, "OCI manifest digest mismatch").into(),
            );
        }
        let mut value: serde_json::Value =
            serde_json::from_slice(&body).context("invalid OCI cache manifest")?;
        let data = value
            .get_mut("layers")
            .and_then(serde_json::Value::as_array_mut)
            .and_then(|layers| layers.first_mut())
            .and_then(|layer| layer.as_object_mut())
            .and_then(|layer| layer.remove("data"))
            .map(serde_json::from_value::<String>)
            .transpose()
            .context("invalid OCI embedded data")?;
        Ok(Some((
            serde_json::from_value(value).context("invalid OCI cache manifest")?,
            data,
        )))
    }

    /// Metadata has a total request deadline and a bounded response body.
    async fn metadata_get(&self, url: &str) -> Result<Option<(Bytes, Option<String>)>> {
        let mut response = self
            .transport
            .request(
                reqwest::Method::GET,
                url,
                RegistryOperation::Pull,
                None,
                None,
                true,
            )
            .await
            .context("reading OCI registry metadata")?;
        let status = response.status().as_u16();
        if status == 404 {
            return Ok(None);
        }
        if status != 200 {
            return Err(registry_error(OciDistributionError::ServerError {
                code: status,
                url: url.to_string(),
                message: "metadata request failed".to_string(),
            }));
        }
        let digest = response
            .headers()
            .get("docker-content-digest")
            .map(|value| value.to_str().map(str::to_string))
            .transpose()?;
        if response
            .content_length()
            .is_some_and(|size| size > MAX_REGISTRY_METADATA_BYTES as u64)
        {
            anyhow::bail!("OCI registry metadata too large");
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .context("reading OCI registry metadata body")?
        {
            if chunk.len() > MAX_REGISTRY_METADATA_BYTES - body.len() {
                anyhow::bail!("OCI registry metadata too large");
            }
            body.extend_from_slice(&chunk);
        }
        Ok(Some((body.into(), digest)))
    }
}

fn client_config(insecure: bool) -> ClientConfig {
    ClientConfig {
        protocol: if insecure {
            ClientProtocol::Http
        } else {
            ClientProtocol::Https
        },
        connect_timeout: Some(super::CONNECT_TIMEOUT),
        read_timeout: Some(super::READ_INACTIVITY_TIMEOUT),
        ..Default::default()
    }
}

fn check_list_limits(entries: usize, key_bytes: usize) -> Result<()> {
    if entries > super::LIST_MAX_ENTRIES || key_bytes > super::LIST_MAX_KEY_BYTES {
        anyhow::bail!("OCI LIST exceeded limits");
    }
    Ok(())
}

fn object_tag(key: &str) -> String {
    // OCI permits 128-byte tags, shorter than most hex-digest object paths.
    // Encode 64-character lowercase hex runs as 32 bytes with a NUL marker.
    // Valid keys cannot contain NUL, so the transformation is reversible.
    // Ordinary Kache keys fit and LIST can recover them from tags alone.
    let mut encoded = Vec::new();
    let mut rest = key.as_bytes();
    while !rest.is_empty() {
        if rest.len() >= 64 && rest[..64].iter().all(|byte| is_lower_hex(*byte)) {
            encoded.push(0);
            encoded.extend_from_slice(&hex::decode(&rest[..64]).expect("validated hex run"));
            rest = &rest[64..];
        } else {
            encoded.push(rest[0]);
            rest = &rest[1..];
        }
    }
    if encoded.len() <= MAX_TAG_KEY_BYTES {
        format!(
            "{TAG_PREFIX}b-{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(encoded)
        )
    } else {
        format!("{TAG_PREFIX}h-{}", blake3::hash(key.as_bytes()).to_hex())
    }
}

fn legacy_object_tag(key: &str) -> String {
    format!(
        "{LEGACY_TAG_PREFIX}{}",
        blake3::hash(key.as_bytes()).to_hex()
    )
}

fn tag_key(tag: &str) -> Result<Option<String>> {
    let Some(encoded) = tag.strip_prefix(&format!("{TAG_PREFIX}b-")) else {
        return Ok(None);
    };
    // The codec emits at most 127 bytes, below OCI's 128-byte ceiling.
    if tag.len() > 127 {
        anyhow::bail!("OCI object tag too long");
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .context("invalid OCI object tag")?;
    let mut key = Vec::new();
    let mut rest = bytes.as_slice();
    while !rest.is_empty() {
        if rest[0] == 0 {
            if rest.len() < 33 {
                anyhow::bail!("truncated OCI object tag digest");
            }
            key.extend_from_slice(hex::encode(&rest[1..33]).as_bytes());
            rest = &rest[33..];
        } else {
            key.push(rest[0]);
            rest = &rest[1..];
        }
    }
    let key = String::from_utf8(key).context("invalid OCI object tag key")?;
    validate_key(&key)?;
    if object_tag(&key) != tag {
        anyhow::bail!("noncanonical OCI object tag");
    }
    Ok(Some(key))
}

fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
}

fn validate_key(key: &str) -> Result<()> {
    if key.is_empty()
        || key.len() > 4096
        || key.chars().any(char::is_control)
        || normalize_remote_prefix(key)? != key
    {
        anyhow::bail!("invalid OCI cache key {key:?}");
    }
    Ok(())
}

fn validate_blob_digest(digest: &str) -> Result<()> {
    let Some(hash) = digest.strip_prefix("sha256:") else {
        anyhow::bail!("unsupported OCI object digest");
    };
    if hash.len() != 64 || !hash.bytes().all(is_lower_hex) {
        anyhow::bail!("invalid OCI object digest");
    }
    Ok(())
}

fn verify_object_digest(expected: &str, actual: &[u8]) -> Result<()> {
    validate_blob_digest(expected)?;
    if format!("sha256:{}", hex::encode(actual)) != expected {
        return Err(Error::new(ErrorKind::RangeNotSatisfied, "OCI object digest mismatch").into());
    }
    Ok(())
}

fn object_descriptor<'a>(manifest: &'a OciImageManifest, key: &str) -> Result<&'a OciDescriptor> {
    let annotated = manifest
        .annotations
        .as_ref()
        .and_then(|map| map.get(KEY_ANNOTATION));
    if manifest.schema_version != 2
        || manifest.artifact_type.as_deref() != Some(ARTIFACT_TYPE)
        || annotated.map(String::as_str) != Some(key)
        || manifest.layers.len() != 1
    {
        anyhow::bail!("invalid OCI cache artifact for {key:?}");
    }
    let descriptor = &manifest.layers[0];
    u64::try_from(descriptor.size).context("negative OCI object size")?;
    Ok(descriptor)
}

fn credentials(
    result: std::result::Result<DockerCredential, CredentialRetrievalError>,
) -> Result<RegistryAuth> {
    match result {
        Ok(DockerCredential::UsernamePassword(username, password)) => {
            Ok(RegistryAuth::Basic(username, password))
        }
        Ok(DockerCredential::IdentityToken(token)) => Ok(RegistryAuth::Bearer(token)),
        Err(CredentialRetrievalError::NoCredentialConfigured) => Ok(RegistryAuth::Anonymous),
        // Helper output can contain credentials. Do not include it in errors.
        Err(_) => Err(Error::new(
            ErrorKind::PermissionDenied,
            "cannot read OCI credentials from Docker config or credential helper",
        )
        .into()),
    }
}

fn load_credentials(directory: &std::path::Path, registry: &str) -> Result<RegistryAuth> {
    let file = match std::fs::File::open(directory.join("config.json")) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RegistryAuth::Anonymous);
        }
        Err(error) => return Err(error).context("opening Docker credential config"),
    };
    credentials(docker_credential::get_credential_from_reader(
        std::io::BufReader::new(file),
        registry,
    ))
}

/// Preserve typed transport failures for Kache's existing retry and auth policy.
fn registry_error(error: OciDistributionError) -> anyhow::Error {
    let error = match error {
        OciDistributionError::RequestError(error) => return error.into(),
        OciDistributionError::IoError(error) => return error.into(),
        error => error,
    };
    let kind = match &error {
        OciDistributionError::ImageManifestNotFoundError(_) => ErrorKind::NotFound,
        OciDistributionError::AuthenticationFailure(_)
        | OciDistributionError::UnauthorizedError { .. } => ErrorKind::PermissionDenied,
        OciDistributionError::ServerError { code: 404, .. } => ErrorKind::NotFound,
        OciDistributionError::ServerError {
            code: 401 | 403, ..
        } => ErrorKind::PermissionDenied,
        OciDistributionError::ServerError { code: 429, .. } => ErrorKind::RateLimited,
        OciDistributionError::ServerError {
            code: 500..=599, ..
        } => {
            return Error::new(ErrorKind::Unexpected, "OCI registry unavailable")
                .set_temporary()
                .set_source(error)
                .into();
        }
        OciDistributionError::RegistryError { envelope, .. } => {
            if envelope.errors.iter().any(|error| {
                matches!(
                    error.code,
                    OciErrorCode::Unauthorized | OciErrorCode::Denied
                )
            }) {
                ErrorKind::PermissionDenied
            } else if !envelope.errors.is_empty()
                && envelope.errors.iter().all(|error| {
                    matches!(
                        error.code,
                        OciErrorCode::ManifestUnknown
                            | OciErrorCode::NameUnknown
                            | OciErrorCode::BlobUnknown
                            | OciErrorCode::NotFound
                    )
                })
            {
                ErrorKind::NotFound
            } else if envelope
                .errors
                .iter()
                .any(|error| error.code == OciErrorCode::Toomanyrequests)
            {
                ErrorKind::RateLimited
            } else {
                ErrorKind::Unexpected
            }
        }
        OciDistributionError::DigestError(_) => ErrorKind::RangeNotSatisfied,
        _ => ErrorKind::ConfigInvalid,
    };
    Error::new(kind, "OCI registry request failed")
        .set_source(error)
        .into()
}

#[async_trait]
impl RemoteBackend for OciBackend {
    async fn head(&self, key: &str) -> Result<bool> {
        validate_key(key)?;
        let Some((manifest, _)) = self.object_manifest(key).await? else {
            return Ok(false);
        };
        object_descriptor(&manifest, key)?;
        Ok(true)
    }

    async fn get(&self, key: &str, max_bytes: Option<u64>) -> Result<Option<GetObject>> {
        let memory = DOWNLOAD_MEMORY.acquire(max_bytes).await?;
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
        validate_key(key)?;
        let started = Instant::now();
        let Some((manifest, embedded)) = self.object_manifest(key).await? else {
            return Ok(None);
        };
        let descriptor = object_descriptor(&manifest, key)?;
        let expected = u64::try_from(descriptor.size)?;
        if max_bytes.is_some_and(|limit| expected > limit) {
            anyhow::bail!("OCI object {key:?} too large: {expected} bytes");
        }
        if let Some(embedded) = embedded {
            if expected > MAX_EMBEDDED_BYTES as u64
                || embedded.len() > MAX_EMBEDDED_BYTES.div_ceil(3) * 4
            {
                anyhow::bail!("OCI embedded object too large");
            }
            let body = base64::engine::general_purpose::STANDARD
                .decode(embedded)
                .context("invalid OCI embedded data")?;
            super::verify_complete_body(Some(expected), body.len() as u64, &self.describe(key))?;
            verify_object_digest(&descriptor.digest, &Sha256::digest(&body))?;
            let request_ms = started.elapsed().as_millis() as u64;
            let started = Instant::now();
            destination
                .write_all(&body)
                .await
                .context("writing OCI embedded object")?;
            destination
                .flush()
                .await
                .context("flushing OCI embedded object")?;
            return Ok(Some(GetTransfer {
                bytes: expected,
                request_ms,
                body_ms: started.elapsed().as_millis() as u64,
            }));
        }
        // Objects published by Kache use SHA-256. Reject a malformed or
        // unsupported digest before constructing the repository blob URL.
        validate_blob_digest(&descriptor.digest)?;
        // Only use the repository digest. Descriptor URLs must not redirect
        // cache reads to a different host.
        let mut response = self
            .transport
            .request(
                reqwest::Method::GET,
                &format!("{}/blobs/{}", self.base_url, descriptor.digest),
                RegistryOperation::Pull,
                None,
                None,
                false,
            )
            .await
            .context("reading OCI object")?;
        transport::require_status(&response, 200)?;
        if max_bytes
            .zip(response.content_length())
            .is_some_and(|(limit, length)| length > limit)
        {
            anyhow::bail!("OCI object {key:?} advertised body too large");
        }
        let request_ms = started.elapsed().as_millis() as u64;
        let started = Instant::now();
        let mut length = 0_u64;
        let mut digest = Sha256::new();
        while let Some(chunk) = response.chunk().await.context("reading OCI object body")? {
            length = length
                .checked_add(chunk.len() as u64)
                .context("OCI object size overflow")?;
            if max_bytes.is_some_and(|limit| length > limit) {
                anyhow::bail!("OCI object {key:?} streamed body too large");
            }
            digest.update(&chunk);
            destination
                .write_all(&chunk)
                .await
                .context("writing OCI object body")?;
        }
        super::verify_complete_body(Some(expected), length, &self.describe(key))?;
        verify_object_digest(&descriptor.digest, &digest.finalize())?;
        destination
            .flush()
            .await
            .context("flushing OCI object body")?;
        Ok(Some(GetTransfer {
            bytes: length,
            request_ms,
            body_ms: started.elapsed().as_millis() as u64,
        }))
    }

    async fn put(&self, key: &str, body: Vec<u8>, content_type: Option<&str>) -> Result<()> {
        validate_key(key)?;
        let layer = ImageLayer::new(
            body,
            content_type
                .unwrap_or("application/octet-stream")
                .to_string(),
            Some(BTreeMap::from([(
                "org.opencontainers.image.title".to_string(),
                "object".to_string(),
            )])),
        );
        let config = Config::new(b"{}".to_vec(), EMPTY_CONFIG_TYPE.to_string(), None);
        let mut manifest = OciImageManifest::build(
            std::slice::from_ref(&layer),
            &config,
            Some(BTreeMap::from([(
                KEY_ANNOTATION.to_string(),
                key.to_string(),
            )])),
        );
        manifest.artifact_type = Some(ARTIFACT_TYPE.to_string());
        let mut value = serde_json::to_value(&manifest)?;
        if content_type == Some("application/json") && layer.data.len() <= MAX_EMBEDDED_BYTES {
            value["layers"][0]["data"] = base64::engine::general_purpose::STANDARD
                .encode(&layer.data)
                .into();
        }
        self.transport
            .put_blob(layer.data, &manifest.layers[0].digest)
            .await?;
        self.transport
            .ensure_config(config.data, &manifest.config.digest)
            .await?;
        let body: Bytes = serde_json::to_vec(&value)?.into();
        let digest = ImageLayer::new(body.clone(), String::new(), None).sha256_digest();
        let response = self
            .transport
            .request(
                reqwest::Method::PUT,
                &format!("{}/manifests/{}", self.base_url, object_tag(key)),
                RegistryOperation::Push,
                Some(body),
                Some("application/vnd.oci.image.manifest.v1+json"),
                true,
            )
            .await
            .with_context(|| format!("publishing OCI object {}", self.describe(key)))?;
        transport::require_status(&response, 201)?;
        transport::verify_digest_header(&response, &digest)?;
        Ok(())
    }

    // OCI Distribution does not guarantee conditional tag writes. The trait's
    // Unsupported defaults avoid pretending HEAD followed by PUT is atomic.

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = BTreeSet::new();
        let mut seen = HashSet::new();
        let mut key_bytes = 0_usize;
        let mut last: Option<String> = None;
        let started = Instant::now();
        loop {
            let remaining = super::LIST_TOTAL_TIMEOUT
                .checked_sub(started.elapsed())
                .context("OCI LIST exceeded total deadline")?;
            let mut url = reqwest::Url::parse(&format!("{}/tags/list", self.base_url))?;
            url.query_pairs_mut()
                .append_pair("n", &PAGE_SIZE.to_string());
            if let Some(last) = &last {
                url.query_pairs_mut().append_pair("last", last);
            }
            let Some((body, _)) = tokio::time::timeout(remaining, self.metadata_get(url.as_str()))
                .await
                .context("OCI LIST exceeded total deadline")??
            else {
                break;
            };
            let page: oci_client::client::TagResponse =
                serde_json::from_slice(&body).context("invalid OCI tag page")?;
            if page.tags.is_empty() {
                break;
            }
            for tag in &page.tags {
                if !seen.insert(tag.clone()) {
                    anyhow::bail!("OCI LIST returned duplicate tag {tag:?}");
                }
                key_bytes = key_bytes.saturating_add(tag.len());
                check_list_limits(seen.len(), key_bytes)?;
                let legacy = tag.starts_with(LEGACY_TAG_PREFIX);
                if !legacy && !tag.starts_with(TAG_PREFIX) {
                    continue;
                }
                if let Some(key) = tag_key(tag)? {
                    key_bytes = key_bytes.saturating_add(key.len());
                    check_list_limits(seen.len(), key_bytes)?;
                    if key.starts_with(prefix) {
                        keys.insert(key);
                    }
                    continue;
                }
                if !legacy && !tag.starts_with(&format!("{TAG_PREFIX}h-")) {
                    anyhow::bail!("invalid OCI object tag");
                }
                let remaining = super::LIST_TOTAL_TIMEOUT
                    .checked_sub(started.elapsed())
                    .context("OCI LIST exceeded total deadline")?;
                let Some((manifest, _)) = tokio::time::timeout(remaining, self.manifest(tag))
                    .await
                    .context("OCI LIST exceeded total deadline")??
                else {
                    continue;
                };
                let key = manifest
                    .annotations
                    .as_ref()
                    .and_then(|map| map.get(KEY_ANNOTATION))
                    .context("OCI cache artifact missing key annotation")?;
                validate_key(key)?;
                let expected_tag = if legacy {
                    legacy_object_tag(key)
                } else {
                    object_tag(key)
                };
                if expected_tag != *tag {
                    anyhow::bail!("OCI cache tag does not match its key annotation");
                }
                object_descriptor(&manifest, key)?;
                if key.starts_with(prefix) {
                    keys.insert(key.clone());
                }
                key_bytes = key_bytes.saturating_add(key.len());
                check_list_limits(seen.len(), key_bytes)?;
            }
            // Fetch again even for a short page: registries may use a smaller
            // page size than requested.
            last = page.tags.last().cloned();
        }
        Ok(keys.into_iter().collect())
    }

    fn describe(&self, key: &str) -> String {
        format!(
            "oci://{}/{}",
            self.repository.whole().trim_end_matches(":latest"),
            key
        )
    }
}

#[cfg(test)]
mod tests;
