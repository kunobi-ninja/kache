//! Transport abstraction for the remote cache.
//!
//! The remote layout ([`crate::remote_layout`]) and the manifest/shard sync
//! ([`crate::remote`]) speak in opaque byte objects addressed by key. This
//! module is the seam between that key/bytes vocabulary and whatever actually
//! stores the bytes — today S3, next a plain shared folder (#414).
//!
//! Everything above the trait (pack framing, zstd, blake3 validation, key
//! naming) is transport-independent and stays shared.

use anyhow::{Context, Result};
use async_trait::async_trait;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use bytes::Bytes;
use std::sync::Arc;

use crate::config::RemoteConfig;

/// A fetched object plus the timing split callers report as transfer telemetry.
#[derive(Debug)]
pub struct GetObject {
    /// `Bytes` so a restore doesn't copy the whole pack a second time.
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
    /// `max_bytes` checks the object's *advertised* size before the body is
    /// buffered. A remote that advertises no size is not bounded.
    async fn get(&self, key: &str, max_bytes: Option<u64>) -> Result<Option<GetObject>>;

    /// Store `body` at `key`.
    async fn put(&self, key: &str, body: Vec<u8>, content_type: Option<&str>) -> Result<()>;

    /// Keys under `prefix`.
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// Where `key` lives, for logs and errors (e.g. `s3://bucket/key`).
    fn describe(&self, key: &str) -> String;
}

/// S3 (and S3-compatible: MinIO, Ceph RGW, R2) transport.
pub struct S3Backend {
    client: aws_sdk_s3::Client,
    bucket: String,
}

impl S3Backend {
    pub fn new(client: aws_sdk_s3::Client, bucket: String) -> Self {
        Self { client, bucket }
    }
}

/// True when a GetObject failure means "no such object" rather than a
/// transport/service error.
///
/// A structured code is authoritative: `NoSuchBucket` is also a 404, and
/// treating it as absence would turn a misconfigured bucket into a cache miss.
/// The raw status is consulted only when there is no code, because some S3
/// clones answer a missing key with a bare 404 and no XML error body.
fn is_missing_get_object(
    err: &aws_sdk_s3::error::SdkError<
        aws_sdk_s3::operation::get_object::GetObjectError,
        aws_smithy_runtime_api::client::orchestrator::HttpResponse,
    >,
) -> bool {
    match err.as_service_error() {
        Some(se) if se.code().is_some() || se.is_no_such_key() => {
            se.is_no_such_key()
                || matches!(
                    se.code(),
                    Some("NotFound" | "NoSuchKey" | "404" | "NoSuchObject")
                )
        }
        _ => err
            .raw_response()
            .is_some_and(|r| r.status().as_u16() == 404),
    }
}

fn is_missing_head_object(err: &HeadObjectError) -> bool {
    err.is_not_found()
        || matches!(
            err.code(),
            Some("NotFound" | "NoSuchKey" | "404" | "NoSuchObject")
        )
}

fn describe_head_object_error(err: &HeadObjectError, http_status: Option<u16>) -> String {
    let mut details = Vec::new();

    if let Some(status) = http_status {
        details.push(format!("HTTP {status}"));
    }
    if let Some(code) = err.code() {
        details.push(format!("code={code}"));
    }
    if let Some(message) = err.message() {
        details.push(format!("message={message}"));
    }
    // Fallback: if no structured info, include Display output
    if details.is_empty() || (http_status.is_none() && err.code().is_none()) {
        details.push(err.to_string());
    }

    details.join(", ")
}

#[async_trait]
impl RemoteBackend for S3Backend {
    async fn head(&self, key: &str) -> Result<bool> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                let http_status = e.raw_response().map(|r| r.status().as_u16());
                let err = e.into_service_error();
                if is_missing_head_object(&err) {
                    Ok(false)
                } else {
                    Err(anyhow::anyhow!(
                        "S3 head_object error for {}: {}",
                        self.describe(key),
                        describe_head_object_error(&err, http_status)
                    ))
                }
            }
        }
    }

    async fn get(&self, key: &str, max_bytes: Option<u64>) -> Result<Option<GetObject>> {
        let request_start = std::time::Instant::now();
        let resp = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) if is_missing_get_object(&e) => return Ok(None),
            Err(e) => {
                return Err(anyhow::Error::new(e))
                    .with_context(|| format!("GET {}", self.describe(key)));
            }
        };
        let request_ms = request_start.elapsed().as_millis() as u64;

        // Refuse before collecting: the point of the cap is to not buffer the
        // body at all.
        if let Some(max) = max_bytes
            && let Some(len) = resp.content_length()
            && len as u64 > max
        {
            anyhow::bail!("{} too large: {len} bytes (max {max})", self.describe(key));
        }

        let body_start = std::time::Instant::now();
        let body = resp
            .body
            .collect()
            .await
            .with_context(|| format!("reading body of {}", self.describe(key)))?;
        let body_ms = body_start.elapsed().as_millis() as u64;

        Ok(Some(GetObject {
            body: body.into_bytes(),
            request_ms,
            body_ms,
        }))
    }

    async fn put(&self, key: &str, body: Vec<u8>, content_type: Option<&str>) -> Result<()> {
        let mut req = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body.into());

        if let Some(ct) = content_type {
            req = req.content_type(ct);
        }

        req.send()
            .await
            .with_context(|| format!("PUT {}", self.describe(key)))?;

        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);

            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req
                .send()
                .await
                .with_context(|| format!("listing s3://{}/{prefix}", self.bucket))?;

            keys.extend(
                resp.contents()
                    .iter()
                    .filter_map(|o| o.key())
                    .map(String::from),
            );

            // A truncated response must carry a continuation token. Some
            // non-AWS S3 implementations (MinIO/Ceph RGW/R2/proxies) have
            // emitted is_truncated=true with a missing/empty token; treat that
            // as end-of-pagination rather than looping forever re-listing page 1.
            match resp.next_continuation_token() {
                Some(token) if resp.is_truncated == Some(true) && !token.is_empty() => {
                    continuation_token = Some(token.to_string());
                }
                _ => break,
            }
        }

        Ok(keys)
    }

    fn describe(&self, key: &str) -> String {
        format!("s3://{}/{}", self.bucket, key)
    }
}

/// Build the backend named by `remote`.
///
/// `Arc` rather than `Box`: the prefetch path fans shard downloads out across
/// `tokio::spawn`, which needs an owned `'static` handle per task.
pub async fn create_backend(
    remote: &RemoteConfig,
    pool_idle_secs: u64,
) -> Result<Arc<dyn RemoteBackend>> {
    let client = crate::remote::create_s3_client(remote, pool_idle_secs).await?;
    Ok(Arc::new(S3Backend::new(client, remote.bucket.clone())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::error::ErrorMetadata;
    use aws_smithy_http_client::test_util::wire::{ReplayedEvent, WireMockServer};

    async fn mock_backend(events: Vec<ReplayedEvent>) -> (WireMockServer, S3Backend) {
        let server = WireMockServer::start(events).await;
        let conf = aws_sdk_s3::config::Builder::new()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "AK", "SK", None, None, "test",
            ))
            .endpoint_url(server.endpoint_url())
            .http_client(server.http_client())
            .force_path_style(true)
            .build();
        let backend = S3Backend::new(aws_sdk_s3::Client::from_conf(conf), "bucket".to_string());
        (server, backend)
    }

    #[tokio::test]
    async fn get_under_the_cap_is_allowed() {
        let (_server, backend) = mock_backend(vec![ReplayedEvent::with_body("hello")]).await;

        let fetched = backend
            .get("k", Some(MAX_METADATA_BYTES_TEST))
            .await
            .expect("under-cap GET succeeds")
            .expect("object is present");
        assert_eq!(fetched.body, "hello");
    }

    #[tokio::test]
    async fn get_over_the_cap_is_refused() {
        // The cap is checked against the advertised Content-Length before the
        // body is buffered, so a 5-byte body still trips a 1-byte cap.
        let (_server, backend) = mock_backend(vec![ReplayedEvent::with_body("hello")]).await;

        let err = backend
            .get("k", Some(1))
            .await
            .expect_err("over-cap length must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("too large"), "unexpected message: {msg}");
        assert!(
            msg.contains("s3://bucket/k"),
            "should name the object: {msg}"
        );
    }

    #[tokio::test]
    async fn get_without_a_cap_is_unbounded() {
        let (_server, backend) = mock_backend(vec![ReplayedEvent::with_body("hello")]).await;

        let fetched = backend
            .get("k", None)
            .await
            .expect("uncapped GET succeeds")
            .expect("object is present");
        assert_eq!(fetched.body, "hello");
    }

    #[tokio::test]
    async fn get_absent_object_is_none_not_an_error() {
        let (_server, backend) = mock_backend(vec![ReplayedEvent::status(404)]).await;

        let fetched = backend.get("k", None).await.expect("404 is a clean miss");
        assert!(fetched.is_none());
    }

    #[tokio::test]
    async fn get_at_exactly_the_cap_is_allowed() {
        // The guard is strictly greater, so a body equal to the cap passes.
        let (_server, backend) = mock_backend(vec![ReplayedEvent::with_body("hello")]).await;

        let fetched = backend
            .get("k", Some(5))
            .await
            .expect("at-cap GET succeeds")
            .expect("object is present");
        assert_eq!(fetched.body, "hello");
    }

    /// A 404 carrying a structured S3 error code.
    fn s3_error(code: &str) -> ReplayedEvent {
        ReplayedEvent::HttpResponse {
            status: 404,
            body: format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                 <Error><Code>{code}</Code><Message>test</Message></Error>"
            )
            .into(),
        }
    }

    #[tokio::test]
    async fn get_no_such_key_is_a_miss() {
        let (_server, backend) = mock_backend(vec![s3_error("NoSuchKey")]).await;

        let fetched = backend.get("k", None).await.expect("NoSuchKey is a miss");
        assert!(fetched.is_none());
    }

    #[tokio::test]
    async fn get_no_such_bucket_is_an_error_not_a_miss() {
        // NoSuchBucket is also a 404. Reporting it as absence would turn a
        // misconfigured bucket into an ordinary cache miss.
        let (_server, backend) = mock_backend(vec![s3_error("NoSuchBucket")]).await;

        backend
            .get("k", None)
            .await
            .expect_err("NoSuchBucket must not classify as absence");
    }

    #[tokio::test]
    async fn get_bare_404_without_a_code_is_a_miss() {
        // Some S3 clones answer a missing key with a bare 404 and no XML body.
        let (_server, backend) = mock_backend(vec![ReplayedEvent::status(404)]).await;

        let fetched = backend.get("k", None).await.expect("bare 404 is a miss");
        assert!(fetched.is_none());
    }

    #[tokio::test]
    async fn list_follows_the_continuation_token_across_pages() {
        fn page(key: &str, next: Option<&str>) -> ReplayedEvent {
            let (truncated, token) = match next {
                Some(t) => (
                    "true",
                    format!("<NextContinuationToken>{t}</NextContinuationToken>"),
                ),
                None => ("false", String::new()),
            };
            ReplayedEvent::with_body(format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                 <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                 <Name>bucket</Name><KeyCount>1</KeyCount><MaxKeys>1</MaxKeys>\
                 <IsTruncated>{truncated}</IsTruncated>{token}\
                 <Contents><Key>{key}</Key><Size>10</Size></Contents></ListBucketResult>"
            ))
        }

        let (_server, backend) =
            mock_backend(vec![page("a.json", Some("tok")), page("b.json", None)]).await;

        let keys = backend.list("p/").await.expect("paginated LIST succeeds");
        assert_eq!(keys, vec!["a.json".to_string(), "b.json".to_string()]);
    }

    /// Mirrors `remote::MAX_METADATA_BYTES`; the cap under test is the
    /// backend's `max_bytes` argument, not that specific constant.
    const MAX_METADATA_BYTES_TEST: u64 = 64 * 1024 * 1024;

    #[test]
    fn generic_head_object_not_found_codes_are_treated_as_misses() {
        let err = HeadObjectError::generic(ErrorMetadata::builder().code("NotFound").build());
        assert!(is_missing_head_object(&err));

        let err = HeadObjectError::generic(ErrorMetadata::builder().code("NoSuchKey").build());
        assert!(is_missing_head_object(&err));
    }

    #[test]
    fn generic_head_object_non_miss_code_is_not_treated_as_missing() {
        let err = HeadObjectError::generic(
            ErrorMetadata::builder()
                .code("SignatureDoesNotMatch")
                .build(),
        );
        assert!(!is_missing_head_object(&err));
    }
}
