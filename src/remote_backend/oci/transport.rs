//! Registry HTTP operations, independent of Kache's object layout.

use super::*;
use reqwest::{Method, Response};
use tokio::sync::Mutex;

pub(super) struct Transport {
    auth_client: Client,
    repository: Reference,
    credentials: RegistryAuth,
    http: reqwest::Client,
    base: reqwest::Url,
    pull: Mutex<Option<Option<String>>>,
    push: Mutex<Option<Option<String>>>,
    config_uploaded: Mutex<bool>,
}

impl Transport {
    pub(super) fn new(config: &OciRemoteConfig, credentials: RegistryAuth) -> Result<Self> {
        let repository = config.reference()?;
        let base = reqwest::Url::parse(&format!(
            "{}://{}/v2/{}/",
            if config.insecure { "http" } else { "https" },
            repository.resolve_registry(),
            repository.repository()
        ))?;
        let insecure = config.insecure;
        let http = reqwest::Client::builder()
            .connect_timeout(super::super::CONNECT_TIMEOUT)
            .read_timeout(super::super::READ_INACTIVITY_TIMEOUT)
            .redirect(reqwest::redirect::Policy::custom(move |attempt| {
                if attempt.previous().len() >= 10 {
                    attempt.error("too many OCI redirects")
                } else if !insecure && attempt.url().scheme() != "https" {
                    attempt.error("OCI registry redirect requires HTTPS")
                } else {
                    attempt.follow()
                }
            }))
            .build()
            .context("building OCI HTTP client")?;
        Ok(Self {
            auth_client: Client::try_from(client_config(config.insecure))?,
            repository,
            credentials,
            http,
            base,
            pull: Mutex::new(None),
            push: Mutex::new(None),
            config_uploaded: Mutex::new(false),
        })
    }

    async fn token(
        &self,
        operation: RegistryOperation,
        rejected: Option<&Option<String>>,
    ) -> Result<Option<String>> {
        let cache = match operation {
            RegistryOperation::Pull => &self.pull,
            RegistryOperation::Push => &self.push,
        };
        // Coalesce concurrent negotiations, including token refreshes. A 401
        // invalidates only the token that failed, never a newer replacement.
        let mut cache = cache.lock().await;
        if let Some(current) = cache.as_ref()
            && rejected != Some(current)
        {
            return Ok(current.clone());
        }
        let token = self
            .auth_client
            .auth(&self.repository, &self.credentials, operation)
            .await
            .map_err(registry_error)?;
        *cache = Some(token.clone());
        Ok(token)
    }

    pub(super) async fn request(
        &self,
        method: Method,
        url: &str,
        operation: RegistryOperation,
        body: Option<Bytes>,
        content_type: Option<&str>,
        metadata: bool,
    ) -> Result<Response> {
        let url = reqwest::Url::parse(url)?;
        let same_origin = url.origin() == self.base.origin();
        let mut token = if same_origin {
            self.token(operation, None).await?
        } else {
            None
        };
        let response = self
            .http
            .execute(self.build_request(&method, &url, &body, content_type, metadata, &token)?)
            .await?;
        if response.status() != reqwest::StatusCode::UNAUTHORIZED || !same_origin {
            return Ok(response);
        }
        // Tokens may expire or be revoked. Retry once after renegotiating;
        // body bytes remain available, including for a rejected upload.
        drop(response);
        token = self.token(operation, Some(&token)).await?;
        Ok(self
            .http
            .execute(self.build_request(&method, &url, &body, content_type, metadata, &token)?)
            .await?)
    }

    pub(super) fn build_request(
        &self,
        method: &Method,
        url: &reqwest::Url,
        body: &Option<Bytes>,
        content_type: Option<&str>,
        metadata: bool,
        token: &Option<String>,
    ) -> Result<reqwest::Request> {
        let request = self.http.request(method.clone(), url.clone()).header(
            "accept",
            "application/vnd.oci.image.manifest.v1+json, application/json",
        );
        let request = if url.origin() != self.base.origin() {
            request
        } else if let Some(token) = token {
            request.bearer_auth(token)
        } else if let RegistryAuth::Basic(username, password) = &self.credentials {
            request.basic_auth(username, Some(password))
        } else {
            request
        };
        let request = if let Some(body) = &body {
            request.body(body.clone())
        } else {
            request
        };
        let request = if let Some(content_type) = content_type {
            request.header("content-type", content_type)
        } else {
            request
        };
        let request = if metadata {
            request.timeout(super::super::LIST_PROGRESS_TIMEOUT)
        } else {
            request
        };
        Ok(request.build()?)
    }

    pub(super) async fn ensure_config(&self, data: Bytes, digest: &str) -> Result<()> {
        let mut uploaded = self.config_uploaded.lock().await;
        if !*uploaded {
            self.put_blob(data, digest).await?;
            *uploaded = true;
        }
        Ok(())
    }

    pub(super) async fn put_blob(&self, body: Bytes, digest: &str) -> Result<()> {
        let url = self.base.join(&format!("blobs/{digest}"))?;
        let response = self
            .request(
                Method::HEAD,
                url.as_str(),
                RegistryOperation::Push,
                None,
                None,
                true,
            )
            .await?;
        if response.status() == reqwest::StatusCode::OK {
            return Ok(());
        }
        require_status(&response, 404)?;
        drop(response);
        let start = self.base.join("blobs/uploads/")?;
        let response = self
            .request(
                Method::POST,
                start.as_str(),
                RegistryOperation::Push,
                Some(Bytes::new()),
                None,
                true,
            )
            .await?;
        require_status(&response, 202)?;
        let location = response
            .headers()
            .get("location")
            .context("OCI upload missing Location")?
            .to_str()?;
        let mut upload = response.url().join(location)?;
        if !matches!(upload.scheme(), "http" | "https")
            || (self.base.scheme() == "https" && upload.scheme() != "https")
            || !upload.username().is_empty()
            || upload.password().is_some()
            || upload.fragment().is_some()
        {
            anyhow::bail!("invalid OCI upload Location");
        }
        upload.query_pairs_mut().append_pair("digest", digest);
        drop(response);
        // Kache already owns the complete compressed body. A monolithic PUT
        // saves the separate PATCH used by a chunked upload.
        let response = self
            .request(
                Method::PUT,
                upload.as_str(),
                RegistryOperation::Push,
                Some(body),
                Some("application/octet-stream"),
                false,
            )
            .await?;
        require_status(&response, 201)?;
        verify_digest_header(&response, digest)?;
        Ok(())
    }
}

pub(super) fn require_status(response: &Response, expected: u16) -> Result<()> {
    let status = response.status().as_u16();
    if status != expected {
        return Err(registry_error(OciDistributionError::ServerError {
            code: status,
            url: response.url().to_string(),
            message: "registry request failed".to_string(),
        }));
    }
    Ok(())
}

pub(super) fn verify_digest_header(response: &Response, expected: &str) -> Result<()> {
    if let Some(digest) = response.headers().get("docker-content-digest")
        && digest.to_str()? != expected
    {
        return Err(Error::new(
            ErrorKind::RangeNotSatisfied,
            "OCI published digest mismatch",
        )
        .into());
    }
    Ok(())
}
