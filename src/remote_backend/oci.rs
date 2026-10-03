//! OCI Distribution transport. Each object is a single-layer artifact; a tag
//! derived from its key points to the manifest. The original key is annotated
//! on the manifest so LIST can recover keys without a shared mutable index.

use std::collections::{BTreeMap, HashSet};
use std::time::Instant;

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use docker_credential::{CredentialRetrievalError, DockerCredential};
use futures::TryStreamExt;
use oci_client::client::{ClientConfig, ClientProtocol, Config, ImageLayer};
use oci_client::errors::{OciDistributionError, OciErrorCode};
use oci_client::manifest::{OciDescriptor, OciImageManifest};
use oci_client::secrets::RegistryAuth;
use oci_client::token_cache::RegistryOperation;
use oci_client::{Client, Reference};
use opendal::{Error, ErrorKind};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use super::download_memory::{BudgetedBody, DOWNLOAD_MEMORY};
use super::{GetObject, GetTransfer, RemoteBackend};
use crate::config::{OciRemoteConfig, normalize_remote_prefix};

const TAG_PREFIX: &str = "kache-v1-";
const KEY_ANNOTATION: &str = "ninja.kunobi.kache.key";
const ARTIFACT_TYPE: &str = "application/vnd.kache.cache-object.v1";
const EMPTY_CONFIG_TYPE: &str = "application/vnd.oci.empty.v1+json";
const PAGE_SIZE: usize = 100;
const MAX_REGISTRY_METADATA_BYTES: usize = 8 << 20;

pub(super) struct OciBackend {
    client: Client,
    repository: Reference,
    auth: RegistryAuth,
    http: reqwest::Client,
    base_url: String,
}

impl OciBackend {
    pub(super) async fn new(config: &OciRemoteConfig) -> Result<Self> {
        let repository = config.reference()?;
        let registry = repository.resolve_registry().to_string();
        let directory = std::env::var_os("DOCKER_CONFIG")
            .map(std::path::PathBuf::from)
            .or_else(|| dirs::home_dir().map(|home| home.join(".docker")))
            .context("cannot find Docker credential config directory")?;
        let auth = tokio::task::spawn_blocking(move || load_credentials(&directory, &registry))
            .await
            .context("loading OCI credentials")??;
        Self::with_auth(config, auth)
    }

    fn with_auth(config: &OciRemoteConfig, auth: RegistryAuth) -> Result<Self> {
        super::ensure_rustls_provider();
        let repository = config.reference()?;
        let client = Client::try_from(client_config(config.insecure))
            .context("building OCI registry client")?;
        let http = reqwest::Client::builder()
            .connect_timeout(super::CONNECT_TIMEOUT)
            .read_timeout(super::READ_INACTIVITY_TIMEOUT)
            .timeout(super::LIST_PROGRESS_TIMEOUT)
            .build()
            .context("building OCI metadata client")?;
        let base_url = format!(
            "{}://{}/v2/{}",
            if config.insecure { "http" } else { "https" },
            repository.resolve_registry(),
            repository.repository()
        );
        Ok(Self {
            client,
            repository,
            auth,
            http,
            base_url,
        })
    }

    fn reference(&self, tag: &str) -> Reference {
        Reference::with_tag(
            self.repository.registry().to_string(),
            self.repository.repository().to_string(),
            tag.to_string(),
        )
    }

    async fn manifest(&self, tag: &str) -> Result<Option<OciImageManifest>> {
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
        Ok(Some(
            serde_json::from_slice(&body).context("invalid OCI cache manifest")?,
        ))
    }

    /// oci-client handles registry auth and blob transfers. Its metadata APIs
    /// buffer entire responses without a limit, so read manifests and tag
    /// pages through a bounded HTTP client after negotiating pull credentials.
    async fn metadata_get(&self, url: &str) -> Result<Option<(Bytes, Option<String>)>> {
        let token = self
            .client
            .auth(&self.repository, &self.auth, RegistryOperation::Pull)
            .await
            .map_err(registry_error)?;
        let request = self.http.get(url).header(
            "accept",
            "application/vnd.oci.image.manifest.v1+json, application/json",
        );
        let request = if let Some(token) = token {
            request.bearer_auth(token)
        } else if let RegistryAuth::Basic(username, password) = &self.auth {
            request.basic_auth(username, Some(password))
        } else {
            request
        };
        let mut response = request
            .send()
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
    format!("{TAG_PREFIX}{}", blake3::hash(key.as_bytes()).to_hex())
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
        Err(_) => {
            anyhow::bail!("cannot read OCI credentials from Docker config or credential helper")
        }
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
        let Some(manifest) = self.manifest(&object_tag(key)).await? else {
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
        let Some(manifest) = self.manifest(&object_tag(key)).await? else {
            return Ok(None);
        };
        let descriptor = object_descriptor(&manifest, key)?;
        let expected = u64::try_from(descriptor.size)?;
        if max_bytes.is_some_and(|limit| expected > limit) {
            anyhow::bail!("OCI object {key:?} too large: {expected} bytes");
        }
        // Only use the repository digest. Descriptor URLs must not redirect
        // cache reads to a different host.
        let mut stream = self
            .client
            .pull_blob_stream(&self.repository, descriptor.digest.as_str())
            .await
            .map_err(registry_error)?;
        if max_bytes
            .zip(stream.content_length)
            .is_some_and(|(limit, length)| length > limit)
        {
            anyhow::bail!("OCI object {key:?} advertised body too large");
        }
        let request_ms = started.elapsed().as_millis() as u64;
        let started = Instant::now();
        let mut length = 0_u64;
        while let Some(chunk) = stream.try_next().await.context("reading OCI object body")? {
            length = length
                .checked_add(chunk.len() as u64)
                .context("OCI object size overflow")?;
            if max_bytes.is_some_and(|limit| length > limit) {
                anyhow::bail!("OCI object {key:?} streamed body too large");
            }
            destination
                .write_all(&chunk)
                .await
                .context("writing OCI object body")?;
        }
        super::verify_complete_body(Some(expected), length, &self.describe(key))?;
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
        self.client
            .push(
                &self.reference(&object_tag(key)),
                &[layer],
                config,
                &self.auth,
                Some(manifest),
            )
            .await
            .map_err(registry_error)
            .with_context(|| format!("publishing OCI object {}", self.describe(key)))?;
        Ok(())
    }

    // OCI Distribution does not guarantee conditional tag writes. The trait's
    // Unsupported defaults avoid pretending HEAD followed by PUT is atomic.

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
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
                if !tag.starts_with(TAG_PREFIX) {
                    continue;
                }
                let remaining = super::LIST_TOTAL_TIMEOUT
                    .checked_sub(started.elapsed())
                    .context("OCI LIST exceeded total deadline")?;
                let Some(manifest) = tokio::time::timeout(remaining, self.manifest(tag))
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
                if object_tag(key) != *tag {
                    anyhow::bail!("OCI cache tag does not match its key annotation");
                }
                object_descriptor(&manifest, key)?;
                if key.starts_with(prefix) {
                    keys.push(key.clone());
                }
                key_bytes = key_bytes.saturating_add(key.len());
                check_list_limits(seen.len(), key_bytes)?;
            }
            // Fetch again even for a short page: registries may use a smaller
            // page size than requested.
            last = page.tags.last().cloned();
        }
        Ok(keys)
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
