use super::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::{Method, Response, StatusCode};

#[derive(Default)]
struct RegistryState {
    manifests: BTreeMap<String, Bytes>,
    blobs: HashMap<String, Bytes>,
    uploads: HashMap<String, Vec<u8>>,
    next_upload: usize,
    requests: Vec<(Method, String)>,
    manifest_status: Option<u16>,
    tags_status: Option<u16>,
    blob_override: Option<(Bytes, bool)>,
    repeat_page: bool,
    page_size: usize,
    authorization: Option<String>,
    metadata_override: Option<(Bytes, bool)>,
    bad_manifest_digest: bool,
    token_generation: Option<usize>,
    token_requests: usize,
    token_authorization: Option<String>,
    bad_published_digest: bool,
    upload_location: Option<String>,
    upload_query: Option<String>,
}

struct Registry {
    config: OciRemoteConfig,
    state: Arc<Mutex<RegistryState>>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Registry {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Registry {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(RegistryState::default()));
        let app = Router::new().fallback(handle).with_state(state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            config: OciRemoteConfig {
                repository: format!("{address}/team/cache"),
                insecure: true,
            },
            state,
            server,
        }
    }

    fn backend(&self) -> OciBackend {
        OciBackend::with_auth(&self.config, RegistryAuth::Anonymous).unwrap()
    }

    fn edit_manifest(&self, key: &str, edit: impl FnOnce(&mut OciImageManifest)) {
        let mut state = self.state.lock().unwrap();
        let bytes = state.manifests.get_mut(&object_tag(key)).unwrap();
        let mut manifest: OciImageManifest = serde_json::from_slice(bytes).unwrap();
        edit(&mut manifest);
        *bytes = serde_json::to_vec(&manifest).unwrap().into();
    }
}

fn response(status: u16, body: impl Into<Body>) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(body.into())
        .unwrap()
}

fn registry_failure(status: u16) -> Response<Body> {
    let code = match status {
        404 => "MANIFEST_UNKNOWN",
        401 => "UNAUTHORIZED",
        403 => "DENIED",
        429 => "TOOMANYREQUESTS",
        _ => "UNKNOWN",
    };
    response(status, format!(r#"{{"errors":[{{"code":"{code}"}}]}}"#))
}

async fn handle(
    State(shared): State<Arc<Mutex<RegistryState>>>,
    request: Request,
) -> Response<Body> {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let authorization = request
        .headers()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let host = request
        .headers()
        .get("host")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let path = uri.path();
    let query: HashMap<_, _> = reqwest::Url::parse(&format!("http://registry{uri}"))
        .unwrap()
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    let body = to_bytes(request.into_body(), 16 << 20).await.unwrap();
    let mut state = shared.lock().unwrap();
    state.requests.push((method.clone(), path.to_string()));
    if path == "/v2/" && state.token_generation.is_some() {
        return Response::builder()
            .status(401)
            .header(
                "www-authenticate",
                format!("Bearer realm=\"http://{host}/tokens\",service=\"registry\""),
            )
            .body(Body::empty())
            .unwrap();
    }
    if path == "/tokens" {
        if authorization.as_deref()
            != Some(
                state
                    .token_authorization
                    .as_deref()
                    .unwrap_or("Basic dXNlcjpwYXNz"),
            )
        {
            return registry_failure(401);
        }
        assert!(matches!(
            query.get("scope").map(String::as_str),
            Some("repository:team/cache:pull" | "repository:team/cache:pull,push")
        ));
        state.token_requests += 1;
        return response(
            200,
            serde_json::to_vec(
                &serde_json::json!({"token":format!("issued-{}", state.token_generation.unwrap())}),
            )
            .unwrap(),
        );
    }
    if path == "/incoming" {
        assert!(
            authorization.is_none(),
            "credentials leaked to an upload host"
        );
        state.upload_query = uri.query().map(str::to_string);
        let digest = ImageLayer::new(body, String::new(), None).sha256_digest();
        return Response::builder()
            .status(201)
            .header("docker-content-digest", digest)
            .body(Body::empty())
            .unwrap();
    }
    if path == "/redirect-loop" {
        return Response::builder()
            .status(302)
            .header("location", path)
            .body(Body::empty())
            .unwrap();
    }
    let required_auth = state
        .token_generation
        .map(|generation| format!("Bearer issued-{generation}"))
        .or_else(|| state.authorization.clone());
    if required_auth.is_some() && required_auth != authorization {
        return Response::builder()
            .status(401)
            .header("www-authenticate", "Basic realm=registry")
            .body(Body::from(r#"{"errors":[{"code":"UNAUTHORIZED"}]}"#))
            .unwrap();
    }
    if path == "/v2/" {
        return response(200, "");
    }
    if method == Method::GET
        && (path.contains("/manifests/") || path.ends_with("/tags/list"))
        && let Some((body, chunked)) = &state.metadata_override
    {
        return response(
            200,
            if *chunked {
                Body::from_stream(futures::stream::iter([Ok::<_, std::io::Error>(
                    body.clone(),
                )]))
            } else {
                Body::from(body.clone())
            },
        );
    }
    if let Some(tag) = path.strip_prefix("/v2/team/cache/manifests/") {
        if let Some(status) = state.manifest_status {
            return registry_failure(status);
        }
        if method == Method::PUT {
            let manifest: OciImageManifest = serde_json::from_slice(&body).unwrap();
            for descriptor in manifest
                .layers
                .iter()
                .chain(std::iter::once(&manifest.config))
            {
                assert!(
                    state.blobs.contains_key(&descriptor.digest),
                    "manifest published before blob"
                );
            }
            let digest = if state.bad_published_digest {
                "sha256:wrong".into()
            } else {
                ImageLayer::new(body.clone(), String::new(), None).sha256_digest()
            };
            state.manifests.insert(tag.to_string(), body);
            return Response::builder()
                .status(201)
                .header("docker-content-digest", digest)
                .header("location", path)
                .body(Body::empty())
                .unwrap();
        }
        let Some(body) = state.manifests.get(tag) else {
            return registry_failure(404);
        };
        let digest = if state.bad_manifest_digest {
            "sha256:wrong".to_string()
        } else {
            ImageLayer::new(body.clone(), String::new(), None).sha256_digest()
        };
        return Response::builder()
            .status(200)
            .header("content-type", "application/vnd.oci.image.manifest.v1+json")
            .header("docker-content-digest", digest)
            .body(Body::from(body.clone()))
            .unwrap();
    }
    if path == "/v2/team/cache/tags/list" {
        assert_eq!(query.get("n").map(String::as_str), Some("100"));
        if let Some(status) = state.tags_status {
            return registry_failure(status);
        }
        let tags: Vec<_> = state
            .manifests
            .keys()
            .filter(|tag| state.repeat_page || query.get("last").is_none_or(|last| *tag > last))
            .take(state.page_size.max(1))
            .cloned()
            .collect(); // Registry imposes a smaller page size than requested.
        return response(
            200,
            serde_json::to_vec(&serde_json::json!({"name":"team/cache", "tags":tags})).unwrap(),
        );
    }
    if path == "/v2/team/cache/blobs/uploads/" && method == Method::POST {
        state.next_upload += 1;
        let id = state.next_upload.to_string();
        state.uploads.insert(id.clone(), Vec::new());
        return Response::builder()
            .status(202)
            .header(
                "location",
                state
                    .upload_location
                    .clone()
                    .unwrap_or_else(|| format!("{path}{id}")),
            )
            .header("range", "0-0")
            .body(Body::empty())
            .unwrap();
    }
    if let Some(id) = path.strip_prefix("/v2/team/cache/blobs/uploads/") {
        if method == Method::PATCH {
            let upload = state.uploads.get_mut(id).unwrap();
            upload.extend_from_slice(&body);
            return Response::builder()
                .status(202)
                .header("location", path)
                .header("range", format!("0-{}", upload.len() - 1))
                .body(Body::empty())
                .unwrap();
        }
        if method == Method::PUT {
            let mut upload = state.uploads.remove(id).unwrap();
            upload.extend_from_slice(&body);
            let digest = query.get("digest").unwrap();
            assert_eq!(
                *digest,
                ImageLayer::new(upload.clone(), String::new(), None).sha256_digest()
            );
            state.blobs.insert(digest.clone(), upload.into());
            return Response::builder()
                .status(201)
                .header("location", format!("/v2/team/cache/blobs/{digest}"))
                .header(
                    "docker-content-digest",
                    if state.bad_published_digest {
                        "sha256:wrong"
                    } else {
                        digest.as_str()
                    },
                )
                .body(Body::empty())
                .unwrap();
        }
    }
    if let Some(digest) = path.strip_prefix("/v2/team/cache/blobs/") {
        let Some(body) = state.blobs.get(digest) else {
            return registry_failure(404);
        };
        let (body, chunked) = state.blob_override.clone().unwrap_or((body.clone(), false));
        let body = if chunked {
            Body::from_stream(futures::stream::iter([Ok::<_, std::io::Error>(body)]))
        } else {
            Body::from(body)
        };
        return response(200, body);
    }
    response(StatusCode::NOT_FOUND.as_u16(), "unknown route")
}

#[test]
fn keys_have_stable_tags_and_reject_ambiguous_paths() {
    assert_eq!(object_tag("hello"), "kache-v2-b-aGVsbG8");
    assert_ne!(object_tag("a/b"), object_tag("a_b"));
    let key = "long/".repeat(500) + "object";
    validate_key(&key).unwrap();
    assert_eq!(object_tag(&key).len(), 75);
    for key in [
        "", "/key", "key/", "a//b", "a/../b", "a/./b", "a\\b", "a\nb", " key",
    ] {
        assert!(validate_key(key).is_err(), "{key:?}");
    }
    assert!(validate_key(&"x".repeat(4096)).is_ok());
    assert!(validate_key(&"x".repeat(4097)).is_err());
}

#[test]
fn short_and_digest_keys_are_recoverable_from_canonical_tags() {
    for key in [
        "plain/key".to_string(),
        "unicode/é/🦀".to_string(),
        "z".repeat(87),
        format!(
            "artifacts/v3/manifests/unicode_normalization/{}.json",
            "a1".repeat(32)
        ),
        "a".repeat(63),
        "a".repeat(64),
        "a".repeat(65),
        "a".repeat(128),
    ] {
        let tag = object_tag(&key);
        assert!(tag.len() <= 128);
        assert_eq!(tag_key(&tag).unwrap(), Some(key));
    }
    assert_eq!(object_tag(&"z".repeat(87)).len(), 127);
    let long = object_tag(&"z".repeat(88));
    assert!(long.starts_with("kache-v2-h-"));
    assert_eq!(tag_key(&long).unwrap(), None);
    let encode = |bytes: &[u8]| {
        format!(
            "kache-v2-b-{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
        )
    };
    assert_eq!(
        tag_key(&encode(&[0])).unwrap_err().to_string(),
        "truncated OCI object tag digest"
    );
    assert_eq!(
        tag_key(&encode(&[0; 32])).unwrap_err().to_string(),
        "truncated OCI object tag digest"
    );
    assert_eq!(tag_key(&encode(&[0; 33])).unwrap(), Some("0".repeat(64)));
    assert_eq!(
        tag_key(&encode(&[b'z'; 88])).unwrap_err().to_string(),
        "OCI object tag too long"
    );
    assert_eq!(
        tag_key(&encode(&[b'a'; 64])).unwrap_err().to_string(),
        "noncanonical OCI object tag"
    );
    for invalid in [
        encode(&[255]),
        encode(b"a/../b"),
        "kache-v2-b-!".to_string(),
    ] {
        assert!(tag_key(&invalid).is_err());
    }
    for byte in b"09af" {
        assert!(is_lower_hex(*byte));
    }
    for byte in b"/:`gAF" {
        assert!(!is_lower_hex(*byte));
    }
}

#[test]
fn only_repository_sha256_digests_are_accepted() {
    validate_blob_digest(&format!("sha256:{}", "a".repeat(64))).unwrap();
    for digest in [
        format!("sha256:{}", "a".repeat(63)),
        format!("sha256:{}", "a".repeat(65)),
        format!("sha256:{}", "g".repeat(64)),
        format!("sha256:{}", "A".repeat(64)),
        format!("sha512:{}", "a".repeat(128)),
        "sha256:../elsewhere".into(),
    ] {
        assert!(validate_blob_digest(&digest).is_err(), "{digest}");
    }
}

#[tokio::test]
async fn warm_requests_skip_auth_discovery_and_listing_skips_manifests() {
    let registry = Registry::start().await;
    let backend = registry.backend();
    backend
        .put("metadata/key", b"{}".to_vec(), Some("application/json"))
        .await
        .unwrap();
    assert_eq!(registry.state.lock().unwrap().requests.len(), 6);
    backend.get("metadata/key", Some(2)).await.unwrap().unwrap();
    registry.state.lock().unwrap().requests.clear();
    backend.get("metadata/key", Some(2)).await.unwrap().unwrap();
    assert_eq!(registry.state.lock().unwrap().requests.len(), 1);
    assert!(
        registry.state.lock().unwrap().requests[0]
            .1
            .contains("/manifests/")
    );
    registry.state.lock().unwrap().requests.clear();
    backend.put("packs/new", vec![7; 1024], None).await.unwrap();
    assert_eq!(registry.state.lock().unwrap().requests.len(), 4);
    assert!(
        registry
            .state
            .lock()
            .unwrap()
            .requests
            .iter()
            .all(|(method, path)| *method != Method::PATCH && path != "/v2/")
    );
    registry.state.lock().unwrap().requests.clear();
    backend.get("packs/new", Some(1024)).await.unwrap().unwrap();
    assert_eq!(registry.state.lock().unwrap().requests.len(), 2);
    for i in 0..8 {
        backend
            .put(&format!("packs/{i}"), vec![i; 1024], None)
            .await
            .unwrap();
    }
    registry.state.lock().unwrap().requests.clear();
    assert_eq!(backend.list("metadata/").await.unwrap(), ["metadata/key"]);
    assert_eq!(registry.state.lock().unwrap().requests.len(), 11);
    assert!(
        registry
            .state
            .lock()
            .unwrap()
            .requests
            .iter()
            .all(|(method, path)| *method == Method::GET && path.ends_with("/tags/list"))
    );
    registry.state.lock().unwrap().requests.clear();
    backend.put("packs/new", vec![7; 1024], None).await.unwrap();
    assert_eq!(registry.state.lock().unwrap().requests.len(), 2);
}

#[tokio::test]
async fn embedded_json_has_a_small_cap_and_is_verified_before_writing() {
    let registry = Registry::start().await;
    let backend = registry.backend();
    for size in [0, 16_384, 16_385] {
        let key = format!("metadata/{size}");
        backend
            .put(&key, vec![b' '; size], Some("application/json"))
            .await
            .unwrap();
        {
            let state = registry.state.lock().unwrap();
            let manifest: serde_json::Value =
                serde_json::from_slice(&state.manifests[&object_tag(&key)]).unwrap();
            assert_eq!(manifest["layers"][0].get("data").is_some(), size <= 16_384);
        }
        assert_eq!(
            backend
                .get(&key, Some(size as u64))
                .await
                .unwrap()
                .unwrap()
                .body
                .len(),
            size
        );
    }
    backend
        .put("bad", b"{}".to_vec(), Some("application/json"))
        .await
        .unwrap();
    let original = registry.state.lock().unwrap().manifests[&object_tag("bad")].clone();
    let mut oversized: serde_json::Value = serde_json::from_slice(&original).unwrap();
    oversized["layers"][0]["size"] = 16_385.into();
    registry.state.lock().unwrap().manifests.insert(
        object_tag("bad"),
        serde_json::to_vec(&oversized).unwrap().into(),
    );
    assert_eq!(
        backend.get("bad", None).await.unwrap_err().to_string(),
        "OCI embedded object too large"
    );
    for (value, message) in [
        (serde_json::json!("!"), "invalid OCI embedded data"),
        (
            serde_json::json!("e30=".repeat(6000)),
            "OCI embedded object too large",
        ),
        (serde_json::json!("eA=="), "truncated"),
        (serde_json::json!("eHg="), "digest mismatch"),
        (serde_json::json!(12), "invalid OCI embedded data"),
    ] {
        let mut manifest: serde_json::Value = serde_json::from_slice(&original).unwrap();
        manifest["layers"][0]["data"] = value;
        registry.state.lock().unwrap().manifests.insert(
            object_tag("bad"),
            serde_json::to_vec(&manifest).unwrap().into(),
        );
        let mut output = Vec::new();
        let error = backend
            .get_into("bad", Some(2), &mut output)
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains(message), "{error:#}");
        assert!(output.is_empty());
    }
}

#[tokio::test]
async fn expired_tokens_refresh_once_and_parallel_reads_share_the_replacement() {
    let registry = Registry::start().await;
    registry.state.lock().unwrap().token_generation = Some(1);
    let backend = OciBackend::with_auth(
        &registry.config,
        RegistryAuth::Basic("user".into(), "pass".into()),
    )
    .unwrap();
    backend
        .put("key", b"{}".to_vec(), Some("application/json"))
        .await
        .unwrap();
    backend.get("key", Some(2)).await.unwrap().unwrap();
    assert_eq!(registry.state.lock().unwrap().token_requests, 2); // One per permission scope.
    registry.state.lock().unwrap().token_generation = Some(2);
    let reads = (0..8).map(|_| backend.get("key", Some(2)));
    for result in futures::future::join_all(reads).await {
        assert_eq!(result.unwrap().unwrap().body, "{}");
    }
    assert_eq!(registry.state.lock().unwrap().token_requests, 3);
    backend
        .put("other", b"{}".to_vec(), Some("application/json"))
        .await
        .unwrap();
    assert_eq!(registry.state.lock().unwrap().token_requests, 4);
    registry.state.lock().unwrap().requests.clear();
    let invalid =
        OciBackend::with_auth(&registry.config, RegistryAuth::Bearer("wrong".into())).unwrap();
    assert!(invalid.head("key").await.is_err());
    assert_eq!(registry.state.lock().unwrap().requests.len(), 2);
}

#[tokio::test]
async fn uploaded_digests_are_checked_and_external_uploads_do_not_receive_credentials() {
    let registry = Registry::start().await;
    let backend = registry.backend();
    registry.state.lock().unwrap().bad_published_digest = true;
    assert!(
        backend
            .put("key", b"hello".to_vec(), None)
            .await
            .unwrap_err()
            .to_string()
            .contains("published digest mismatch")
    );
    registry.state.lock().unwrap().bad_published_digest = false;
    backend.put("key", b"hello".to_vec(), None).await.unwrap();
    registry.state.lock().unwrap().bad_published_digest = true;
    assert!(
        backend
            .put("key", b"hello".to_vec(), None)
            .await
            .unwrap_err()
            .to_string()
            .contains("published digest mismatch")
    );
    registry.state.lock().unwrap().bad_published_digest = false;
    let external = Registry::start().await;
    registry.state.lock().unwrap().upload_location = Some(format!(
        "http://{}/incoming?ticket=keep",
        external.config.repository.split('/').next().unwrap()
    ));
    registry.state.lock().unwrap().authorization = Some("Basic dXNlcjpwYXNz".into());
    let authenticated = OciBackend::with_auth(
        &registry.config,
        RegistryAuth::Basic("user".into(), "pass".into()),
    )
    .unwrap();
    let digest = ImageLayer::new(b"external".as_slice(), String::new(), None).sha256_digest();
    authenticated
        .transport
        .put_blob(Bytes::from_static(b"external"), &digest)
        .await
        .unwrap();
    let query = external.state.lock().unwrap().upload_query.clone().unwrap();
    assert!(query.contains("ticket=keep"));
    assert!(query.contains("digest=sha256%3A"));
    for location in [
        "file:///tmp/object",
        "http://user:secret@localhost/object",
        "http://localhost/object#fragment",
    ] {
        registry.state.lock().unwrap().upload_location = Some(location.into());
        assert_eq!(
            authenticated
                .transport
                .put_blob(Bytes::from_static(b"invalid"), "sha256:unused")
                .await
                .unwrap_err()
                .to_string(),
            "invalid OCI upload Location"
        );
    }
}

#[tokio::test]
async fn request_deadlines_and_redirects_preserve_transport_policy() {
    let registry = Registry::start().await;
    let backend = registry.backend();
    let url = reqwest::Url::parse(&format!("{}/manifests/key", backend.base_url)).unwrap();
    for metadata in [true, false] {
        let request = backend
            .transport
            .build_request(
                &Method::PUT,
                &url,
                &Some(Bytes::from_static(b"body")),
                Some("application/octet-stream"),
                metadata,
                &RegistryAuth::Bearer("token".into()),
            )
            .unwrap();
        assert_eq!(
            request.timeout().copied(),
            if metadata {
                Some(std::time::Duration::from_secs(60))
            } else {
                None
            }
        );
        assert_eq!(request.method(), Method::PUT);
        assert_eq!(request.headers()["authorization"], "Bearer token");
        assert_eq!(
            request.headers()["content-type"],
            "application/octet-stream"
        );
        assert_eq!(request.body().unwrap().as_bytes(), Some(b"body".as_slice()));
    }
    let redirect_url = format!(
        "http://{}/redirect-loop",
        registry.config.repository.split('/').next().unwrap()
    );
    let error = backend
        .transport
        .request(
            Method::GET,
            &redirect_url,
            RegistryOperation::Pull,
            None,
            None,
            true,
        )
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("too many OCI redirects"));
    assert_eq!(
        registry
            .state
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|(_, path)| path == "/redirect-loop")
            .count(),
        10
    );
    let mut secure = registry.config.clone();
    secure.insecure = false;
    let secure = OciBackend::with_auth(&secure, RegistryAuth::Anonymous).unwrap();
    let error = secure
        .transport
        .request(
            Method::GET,
            &redirect_url,
            RegistryOperation::Pull,
            None,
            None,
            true,
        )
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("requires HTTPS"));
    let external = Registry::start().await;
    external.state.lock().unwrap().authorization = Some("Bearer private".into());
    let external_url = format!(
        "http://{}/private",
        external.config.repository.split('/').next().unwrap()
    );
    registry.state.lock().unwrap().requests.clear();
    let response = backend
        .transport
        .request(
            Method::GET,
            &external_url,
            RegistryOperation::Pull,
            None,
            None,
            true,
        )
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(external.state.lock().unwrap().requests.len(), 1);
    assert!(registry.state.lock().unwrap().requests.is_empty());
}

#[tokio::test]
async fn objects_round_trip_over_oci_and_expose_their_artifact_format() {
    let registry = Registry::start().await;
    let backend = registry.backend();
    assert!(!backend.head("absent").await.unwrap());
    assert!(backend.get("absent", Some(10)).await.unwrap().is_none());
    for (key, data) in [
        ("artifacts/metadata.json", b"hello".as_slice()),
        ("other/empty", b"".as_slice()),
    ] {
        backend
            .put(key, data.to_vec(), Some("application/json"))
            .await
            .unwrap();
        assert!(backend.head(key).await.unwrap());
        let object = backend
            .get(key, Some(data.len() as u64))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(object.body.as_ref(), data);
        drop(object);
        let mut destination = Vec::new();
        let transfer = backend
            .get_into(key, None, &mut destination)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(destination, data);
        assert_eq!(transfer.bytes, data.len() as u64);
    }
    assert_eq!(
        backend.list("artifacts/").await.unwrap(),
        ["artifacts/metadata.json"]
    );
    assert_eq!(backend.list("").await.unwrap().len(), 2);
    assert!(backend.list("none").await.unwrap().is_empty());
    assert_eq!(
        backend.describe("key"),
        format!("oci://{}/key", registry.config.repository)
    );
    assert_eq!(
        backend.describe(""),
        format!("oci://{}/", registry.config.repository)
    );
    let state = registry.state.lock().unwrap();
    let manifest: OciImageManifest =
        serde_json::from_slice(&state.manifests[&object_tag("artifacts/metadata.json")]).unwrap();
    assert_eq!(
        manifest.artifact_type.as_deref(),
        Some("application/vnd.kache.cache-object.v1")
    );
    assert_eq!(
        manifest.config.media_type,
        "application/vnd.oci.empty.v1+json"
    );
    assert_eq!(manifest.layers[0].media_type, "application/json");
    assert_eq!(
        manifest.layers[0].annotations.as_ref().unwrap()["org.opencontainers.image.title"],
        "object"
    );
    assert!(
        state
            .requests
            .iter()
            .any(|(method, path)| *method == Method::PUT && path.contains("/blobs/uploads/"))
    );
}

#[tokio::test]
async fn conditionals_write_nothing_and_plain_put_replaces_metadata() {
    use crate::remote_backend::{ConditionalPut, PutIfAbsentResult};
    let registry = Registry::start().await;
    let backend = registry.backend();
    assert_eq!(
        backend
            .put_if_absent("key", b"first".to_vec(), None)
            .await
            .unwrap(),
        PutIfAbsentResult::Unsupported
    );
    assert_eq!(
        backend
            .put_if_match("key", b"first".to_vec(), None, None)
            .await
            .unwrap(),
        ConditionalPut::Unsupported
    );
    assert!(registry.state.lock().unwrap().requests.is_empty());
    backend.put("key", b"first".to_vec(), None).await.unwrap();
    let fetched = backend
        .get_versioned("key", Some(5))
        .await
        .unwrap()
        .unwrap();
    assert!(fetched.1.is_none());
    assert_eq!(fetched.0.body, "first");
    drop(fetched);
    assert_eq!(
        backend
            .put_if_match("key", b"second".to_vec(), None, Some("etag"))
            .await
            .unwrap(),
        ConditionalPut::Unsupported
    );
    backend.put("key", b"second".to_vec(), None).await.unwrap();
    assert_eq!(
        backend.get("key", Some(6)).await.unwrap().unwrap().body,
        "second"
    );
}

#[tokio::test]
async fn reads_enforce_advertised_and_streamed_sizes_and_verify_digests() {
    let registry = Registry::start().await;
    let backend = registry.backend();
    backend.put("key", b"hello".to_vec(), None).await.unwrap();
    let mut destination = Vec::new();
    let error = backend
        .get_into("key", Some(4), &mut destination)
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "OCI object \"key\" too large: 5 bytes");
    assert!(destination.is_empty());
    registry.edit_manifest("key", |manifest| manifest.layers[0].size = 4);
    assert!(
        backend
            .get("key", Some(4))
            .await
            .unwrap_err()
            .to_string()
            .contains("advertised body too large")
    );
    registry.state.lock().unwrap().blob_override = Some((Bytes::from_static(b"hello"), true));
    assert!(
        backend
            .get("key", Some(4))
            .await
            .unwrap_err()
            .to_string()
            .contains("streamed body too large")
    );
    assert!(
        backend
            .get("key", Some(5))
            .await
            .unwrap_err()
            .to_string()
            .contains("truncated")
    );
    registry.edit_manifest("key", |manifest| manifest.layers[0].size = 5);
    registry.state.lock().unwrap().blob_override = Some((Bytes::from_static(b"jello"), false));
    let error = backend.get("key", Some(5)).await.unwrap_err();
    assert!(format!("{error:#}").contains("digest"), "{error:#}");
    registry.state.lock().unwrap().blob_override = Some((Bytes::from_static(b"hell"), true));
    assert!(backend.get("key", None).await.is_err());
}

#[tokio::test]
async fn invalid_artifacts_and_pagination_are_rejected() {
    let registry = Registry::start().await;
    let backend = registry.backend();
    let key = "long/".repeat(30) + "key";
    backend.put(&key, b"hello".to_vec(), None).await.unwrap();
    let original = registry.state.lock().unwrap().manifests[&object_tag(&key)].clone();
    let edits: [fn(&mut OciImageManifest); 7] = [
        |manifest| manifest.schema_version = 1,
        |manifest| manifest.artifact_type = None,
        |manifest| manifest.annotations = None,
        |manifest| {
            manifest
                .annotations
                .as_mut()
                .unwrap()
                .insert(KEY_ANNOTATION.to_string(), "other".to_string());
        },
        |manifest| manifest.layers.clear(),
        |manifest| manifest.layers.push(manifest.layers[0].clone()),
        |manifest| manifest.layers[0].size = -1,
    ];
    for edit in edits {
        registry
            .state
            .lock()
            .unwrap()
            .manifests
            .insert(object_tag(&key), original.clone());
        registry.edit_manifest(&key, edit);
        assert!(backend.head(&key).await.is_err());
        assert!(backend.get(&key, Some(5)).await.is_err());
        assert!(backend.list("").await.is_err());
    }
    registry
        .state
        .lock()
        .unwrap()
        .manifests
        .insert(object_tag(&key), original);
    registry.state.lock().unwrap().repeat_page = true;
    assert!(
        backend
            .list("")
            .await
            .unwrap_err()
            .to_string()
            .contains("duplicate")
    );
    registry.state.lock().unwrap().repeat_page = false;
    registry.state.lock().unwrap().manifests.insert(
        "unrelated-tag".to_string(),
        Bytes::from_static(b"not a kache artifact"),
    );
    assert_eq!(backend.list("").await.unwrap(), [key]);
}

#[tokio::test]
async fn registry_auth_and_outages_remain_errors_and_empty_repositories_are_misses() {
    use crate::remote_resilience::{RemoteErrorClass, classify_remote_error};
    let registry = Registry::start().await;
    let backend = registry.backend();
    for (status, class) in [
        (401, RemoteErrorClass::Authentication),
        (403, RemoteErrorClass::Authentication),
        (429, RemoteErrorClass::Transient),
        (503, RemoteErrorClass::Transient),
    ] {
        registry.state.lock().unwrap().manifest_status = Some(status);
        let error = backend.head("key").await.unwrap_err();
        assert_eq!(classify_remote_error(&error), class, "{error:#}");
        assert!(backend.get("key", Some(10)).await.is_err());
        registry.state.lock().unwrap().tags_status = Some(status);
        assert_eq!(
            classify_remote_error(&backend.list("").await.unwrap_err()),
            class
        );
    }
    registry.state.lock().unwrap().manifest_status = None;
    registry.state.lock().unwrap().tags_status = Some(404);
    assert!(backend.list("").await.unwrap().is_empty());
    registry.state.lock().unwrap().tags_status = None;
    registry.state.lock().unwrap().authorization = Some("Basic dXNlcjpwYXNz".to_string());
    assert!(backend.head("key").await.is_err());
    let backend = OciBackend::with_auth(
        &registry.config,
        RegistryAuth::Basic("user".to_string(), "pass".to_string()),
    )
    .unwrap();
    backend.put("key", b"private".to_vec(), None).await.unwrap();
    assert_eq!(
        backend.get("key", Some(7)).await.unwrap().unwrap().body,
        "private"
    );
}

#[test]
fn docker_credentials_support_passwords_tokens_and_anonymous_access_without_leaking_helper_output()
{
    assert_eq!(
        credentials(Ok(DockerCredential::UsernamePassword(
            "user".into(),
            "password".into()
        )))
        .unwrap(),
        RegistryAuth::Basic("user".into(), "password".into())
    );
    assert_eq!(
        credentials(Ok(DockerCredential::IdentityToken("token".into()))).unwrap(),
        RegistryAuth::Bearer("token".into())
    );
    assert_eq!(
        credentials(Err(CredentialRetrievalError::NoCredentialConfigured)).unwrap(),
        RegistryAuth::Anonymous
    );
    let error = credentials(Err(CredentialRetrievalError::HelperFailure {
        helper: "helper".into(),
        stdout: "secret token".into(),
        stderr: "secret password".into(),
    }))
    .unwrap_err();
    assert!(!format!("{error:#}").contains("secret"));
    assert_eq!(
        crate::remote_resilience::classify_remote_error(&error),
        crate::remote_resilience::RemoteErrorClass::Authentication
    );
    let credential = docker_credential::get_credential_from_reader(
        std::io::Cursor::new(r#"{"auths":{"ghcr.io":{"auth":"dXNlcjpwYXNz"}}}"#),
        "ghcr.io",
    );
    assert_eq!(
        credentials(credential).unwrap(),
        RegistryAuth::Basic("user".into(), "pass".into())
    );
}

#[test]
fn registry_error_classification_does_not_hide_mixed_or_empty_envelopes() {
    use oci_client::errors::{OciEnvelope, OciError};
    for (codes, kind) in [
        (vec![], ErrorKind::Unexpected),
        (vec![OciErrorCode::NameUnknown], ErrorKind::NotFound),
        (
            vec![OciErrorCode::BlobUnknown, OciErrorCode::NotFound],
            ErrorKind::NotFound,
        ),
        (
            vec![OciErrorCode::ManifestUnknown, OciErrorCode::Denied],
            ErrorKind::PermissionDenied,
        ),
        (
            vec![OciErrorCode::ManifestUnknown, OciErrorCode::ManifestInvalid],
            ErrorKind::Unexpected,
        ),
        (vec![OciErrorCode::Toomanyrequests], ErrorKind::RateLimited),
    ] {
        let envelope = OciEnvelope {
            errors: codes
                .into_iter()
                .map(|code| OciError {
                    code,
                    message: String::new(),
                    detail: serde_json::Value::Null,
                })
                .collect(),
        };
        let error = registry_error(OciDistributionError::RegistryError {
            envelope,
            url: "fixture".into(),
        });
        assert_eq!(error.downcast_ref::<Error>().unwrap().kind(), kind);
    }
}

#[test]
fn registry_transport_policy_bounds_network_waits_and_listing_memory() {
    use std::time::Duration;
    for insecure in [false, true] {
        let config = client_config(insecure);
        assert_eq!(config.connect_timeout, Some(Duration::from_millis(3100)));
        assert_eq!(config.read_timeout, Some(Duration::from_secs(30)));
        assert!(matches!(
            (insecure, config.protocol),
            (true, ClientProtocol::Http) | (false, ClientProtocol::Https)
        ));
    }
    check_list_limits(1_000_000, 268_435_456).unwrap();
    check_list_limits(0, 0).unwrap();
    assert!(check_list_limits(1_000_001, 0).is_err());
    assert!(check_list_limits(0, 268_435_457).is_err());
}

#[test]
fn typed_registry_failures_preserve_retry_and_integrity_classification() {
    use crate::remote_resilience::{RemoteErrorClass, classify_remote_error};
    use oci_client::errors::DigestError;
    for (error, class) in [
        (
            OciDistributionError::ImageManifestNotFoundError("fixture".into()),
            RemoteErrorClass::Miss,
        ),
        (
            OciDistributionError::AuthenticationFailure("fixture".into()),
            RemoteErrorClass::Authentication,
        ),
        (
            OciDistributionError::UnauthorizedError {
                url: "fixture".into(),
            },
            RemoteErrorClass::Authentication,
        ),
        (
            OciDistributionError::IoError(std::io::Error::from(std::io::ErrorKind::TimedOut)),
            RemoteErrorClass::Timeout,
        ),
        (
            OciDistributionError::DigestError(DigestError::VerificationError {
                expected: "digest".into(),
                actual: "other".into(),
            }),
            RemoteErrorClass::Integrity,
        ),
        (
            OciDistributionError::UnsupportedMediaTypeError("fixture".into()),
            RemoteErrorClass::Configuration,
        ),
    ] {
        assert_eq!(classify_remote_error(&registry_error(error)), class);
    }
    let runtime = tokio::runtime::Runtime::new().unwrap();
    super::super::ensure_rustls_provider();
    let error = runtime
        .block_on(reqwest::get("http://[invalid-url"))
        .unwrap_err();
    assert_eq!(
        classify_remote_error(&registry_error(OciDistributionError::RequestError(error))),
        RemoteErrorClass::Configuration
    );
    for (status, class) in [
        (404, RemoteErrorClass::Miss),
        (401, RemoteErrorClass::Authentication),
        (403, RemoteErrorClass::Authentication),
        (429, RemoteErrorClass::Transient),
        (503, RemoteErrorClass::Transient),
        (400, RemoteErrorClass::Configuration),
    ] {
        let error = registry_error(OciDistributionError::ServerError {
            code: status,
            url: "fixture".into(),
            message: String::new(),
        });
        assert_eq!(classify_remote_error(&error), class);
    }
}

#[test]
fn credential_files_are_loaded_without_reading_live_process_environment() {
    let directory = tempfile::tempdir().unwrap();
    assert_eq!(
        load_credentials(directory.path(), "ghcr.io").unwrap(),
        RegistryAuth::Anonymous
    );
    std::fs::write(
        directory.path().join("config.json"),
        r#"{"auths":{"ghcr.io":{"auth":"dXNlcjpwYXNz"}}}"#,
    )
    .unwrap();
    assert_eq!(
        load_credentials(directory.path(), "ghcr.io").unwrap(),
        RegistryAuth::Basic("user".into(), "pass".into())
    );
    assert_eq!(
        load_credentials(directory.path(), "other.io").unwrap(),
        RegistryAuth::Anonymous
    );
    std::fs::write(directory.path().join("config.json"), "invalid").unwrap();
    assert!(load_credentials(directory.path(), "ghcr.io").is_err());
    std::fs::remove_file(directory.path().join("config.json")).unwrap();
    std::fs::create_dir(directory.path().join("config.json")).unwrap();
    assert!(load_credentials(directory.path(), "ghcr.io").is_err());
    #[cfg(unix)]
    {
        // Unix reports ENOTDIR here; Windows reports a missing path instead.
        let file = directory.path().join("file");
        std::fs::write(&file, "file").unwrap();
        assert!(load_credentials(&file, "ghcr.io").is_err());
    }
}

#[tokio::test]
async fn registry_metadata_is_bounded_and_manifest_digests_are_verified() {
    let registry = Registry::start().await;
    let backend = registry.backend();
    backend.put("key", b"hello".to_vec(), None).await.unwrap();
    registry.state.lock().unwrap().bad_manifest_digest = true;
    assert!(
        backend
            .get("key", Some(5))
            .await
            .unwrap_err()
            .to_string()
            .contains("manifest digest mismatch")
    );
    registry.state.lock().unwrap().bad_manifest_digest = false;
    let url = format!("{}/manifests/fixture", backend.base_url);
    registry.state.lock().unwrap().metadata_override =
        Some((Bytes::from(vec![b' '; 8_388_608]), false));
    assert_eq!(
        backend.metadata_get(&url).await.unwrap().unwrap().0.len(),
        8_388_608
    );
    for chunked in [false, true] {
        registry.state.lock().unwrap().metadata_override =
            Some((Bytes::from(vec![b' '; 8_388_609]), chunked));
        assert!(
            backend
                .head("key")
                .await
                .unwrap_err()
                .to_string()
                .contains("metadata too large")
        );
        assert!(
            backend
                .list("")
                .await
                .unwrap_err()
                .to_string()
                .contains("metadata too large")
        );
    }
    registry.state.lock().unwrap().metadata_override =
        Some((Bytes::from_static(b"invalid json"), false));
    assert!(
        backend
            .head("key")
            .await
            .unwrap_err()
            .to_string()
            .contains("invalid OCI cache manifest")
    );
    assert!(
        backend
            .list("")
            .await
            .unwrap_err()
            .to_string()
            .contains("invalid OCI tag page")
    );
}

#[tokio::test]
async fn bearer_credentials_are_sent_and_https_never_falls_back_to_http() {
    let registry = Registry::start().await;
    registry.state.lock().unwrap().authorization = Some("Bearer token".to_string());
    let backend =
        OciBackend::with_auth(&registry.config, RegistryAuth::Bearer("token".to_string())).unwrap();
    backend.put("key", b"hello".to_vec(), None).await.unwrap();
    assert_eq!(
        backend.get("key", Some(5)).await.unwrap().unwrap().body,
        "hello"
    );
    assert_eq!(backend.list("").await.unwrap(), ["key"]);
    let mut config = registry.config.clone();
    config.insecure = false;
    let backend = OciBackend::with_auth(&config, RegistryAuth::Anonymous).unwrap();
    assert!(backend.head("key").await.is_err());
    assert!(backend.put("key", b"hello".to_vec(), None).await.is_err());
}

#[tokio::test]
async fn cache_pack_upload_and_restore_use_the_existing_layout() {
    use crate::config::{RemoteBackendConfig, RemoteConfig};
    use crate::remote_layout::RemoteLayout;
    use kache_format::{CachedFile, EntryMeta};
    let registry = Registry::start().await;
    let backend = registry.backend();
    let remote = RemoteConfig {
        prefix: "artifacts".into(),
        backend: RemoteBackendConfig::Oci(registry.config.clone()),
    };
    let layout = RemoteLayout::new(&backend, &remote);
    let directory = tempfile::tempdir().unwrap();
    let entry = directory.path().join("entry");
    let blobs = directory.path().join("blobs");
    let output = directory.path().join("restored");
    std::fs::create_dir(&entry).unwrap();
    let contents = b"compiled artifact";
    let hash = blake3::hash(contents).to_hex().to_string();
    let blob_path = blobs.join(&hash[..2]).join(&hash);
    std::fs::create_dir_all(blob_path.parent().unwrap()).unwrap();
    std::fs::write(&blob_path, contents).unwrap();
    let meta = EntryMeta {
        cache_key: blake3::hash(b"oci-fixture-cache-key").to_hex().to_string(),
        key_schema: crate::cache_key::CACHE_KEY_VERSION,
        crate_name: "fixture".into(),
        crate_types: vec!["lib".into()],
        files: vec![CachedFile {
            name: "libfixture.rlib".into(),
            hash,
            size: contents.len() as u64,
            executable: false,
        }],
        stdout: String::new(),
        stderr: String::new(),
        features: Vec::new(),
        target: "x86_64-unknown-linux-gnu".into(),
        profile: "debug".into(),
        compile_time_ms: 100,
        emit_kinds: vec!["link".into()],
    };
    std::fs::write(entry.join("meta.json"), serde_json::to_vec(&meta).unwrap()).unwrap();
    layout
        .upload_entry_until(&meta.cache_key, &meta.crate_name, &entry, &blobs, 1, None)
        .await
        .unwrap();
    let keys = layout.list_keys().await.unwrap();
    assert_eq!(
        keys.get(&meta.cache_key).map(String::as_str),
        Some("fixture")
    );
    layout
        .download_entry_until(&meta.cache_key, &meta.crate_name, &output, &blobs, None)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(output.join("libfixture.rlib")).unwrap(),
        contents
    );
    let restored: EntryMeta =
        serde_json::from_slice(&std::fs::read(output.join("meta.json")).unwrap()).unwrap();
    assert_eq!(restored, meta);
}

#[tokio::test]
async fn legacy_objects_remain_readable_and_listing_deduplicates_versions() {
    let registry = Registry::start().await;
    let backend = registry.backend();
    backend.put("old/key", b"old".to_vec(), None).await.unwrap();
    {
        let mut state = registry.state.lock().unwrap();
        let body = state.manifests.remove(&object_tag("old/key")).unwrap();
        state.manifests.insert(legacy_object_tag("old/key"), body);
    }
    assert!(backend.head("old/key").await.unwrap());
    assert_eq!(
        backend.get("old/key", Some(3)).await.unwrap().unwrap().body,
        "old"
    );
    assert!(!backend.head("absent").await.unwrap());
    assert!(backend.get("absent", Some(3)).await.unwrap().is_none());
    assert_eq!(backend.list("old/").await.unwrap(), ["old/key"]);
    backend.put("old/key", b"new".to_vec(), None).await.unwrap();
    assert_eq!(
        backend.get("old/key", Some(3)).await.unwrap().unwrap().body,
        "new"
    );
    assert_eq!(backend.list("").await.unwrap(), ["old/key"]);
    let bytes = registry.state.lock().unwrap().manifests[&legacy_object_tag("old/key")].clone();
    registry
        .state
        .lock()
        .unwrap()
        .manifests
        .insert(legacy_object_tag("wrong"), bytes);
    assert!(
        backend
            .list("")
            .await
            .unwrap_err()
            .to_string()
            .contains("does not match")
    );
}

#[tokio::test]
async fn docker_credentials_are_reloaded_when_a_daemons_auth_expires() {
    for bearer_tokens in [false, true] {
        let registry = Registry::start().await;
        let directory = tempfile::tempdir().unwrap();
        let host = registry.config.repository.split('/').next().unwrap();
        let configure = |password: &str| {
            let encoded =
                base64::engine::general_purpose::STANDARD.encode(format!("user:{password}"));
            std::fs::write(
                directory.path().join("config.json"),
                serde_json::to_vec(&serde_json::json!({"auths":{host:{"auth":encoded}}})).unwrap(),
            )
            .unwrap();
        };
        configure("pass");
        {
            let mut state = registry.state.lock().unwrap();
            if bearer_tokens {
                state.token_generation = Some(1);
            } else {
                state.authorization = Some("Basic dXNlcjpwYXNz".into());
            }
        }
        let backend = OciBackend::with_credentials(
            &registry.config,
            auth::CredentialSource::Docker(directory.path().into()),
        )
        .unwrap();
        backend
            .put("key", b"{}".to_vec(), Some("application/json"))
            .await
            .unwrap();
        assert_eq!(
            backend.get("key", Some(2)).await.unwrap().unwrap().body,
            "{}"
        );
        configure("renewed");
        let new_auth = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode("user:renewed")
        );
        {
            let mut state = registry.state.lock().unwrap();
            if bearer_tokens {
                state.token_generation = Some(2);
                state.token_authorization = Some(new_auth);
            } else {
                state.authorization = Some(new_auth);
            }
        }
        // Reuse the same backend, as a running daemon does after rotation.
        assert_eq!(
            backend.get("key", Some(2)).await.unwrap().unwrap().body,
            "{}"
        );
        backend
            .put("other", b"{}".to_vec(), Some("application/json"))
            .await
            .unwrap();
        assert!(backend.head("other").await.unwrap());
        if bearer_tokens {
            assert_eq!(registry.state.lock().unwrap().token_requests, 4);
        }
        std::fs::write(
            directory.path().join("config.json"),
            "secret malformed credentials",
        )
        .unwrap();
        registry.state.lock().unwrap().authorization = Some("rejected".into());
        registry.state.lock().unwrap().token_generation = None;
        let error = backend.head("key").await.unwrap_err();
        assert!(format!("{error:#}").contains("cannot read OCI credentials"));
        assert!(!format!("{error:#}").contains("secret"));
    }
}

#[tokio::test]
async fn ten_thousand_keys_are_listed_without_fetching_object_manifests() {
    let registry = Registry::start().await;
    {
        let mut state = registry.state.lock().unwrap();
        state.page_size = 100;
        for index in 0..10_000 {
            let prefix = if index % 2 == 0 { "artifacts" } else { "other" };
            let key = format!(
                "{prefix}/v3/{}.pack",
                blake3::hash(index.to_string().as_bytes()).to_hex()
            );
            state.manifests.insert(object_tag(&key), Bytes::new());
        }
    }
    let started = Instant::now();
    let keys = registry.backend().list("artifacts/").await.unwrap();
    assert_eq!(keys.len(), 5_000);
    assert!(keys.iter().all(|key| key.starts_with("artifacts/")));
    let state = registry.state.lock().unwrap();
    assert_eq!(state.requests.len(), 102); // Initial auth, 100 pages, final empty page.
    assert!(
        state
            .requests
            .iter()
            .all(|(method, path)| method == Method::GET
                && (path == "/v2/" || path.ends_with("/tags/list")))
    );
    eprintln!(
        "OCI LIST: 10000 tags, 5000 matching keys, 102 HTTP requests, {} ms",
        started.elapsed().as_millis()
    );
}

// The child runs only this test, so environment reads and the helper's PATH
// cannot race the other tests that change the parent process environment.
#[cfg(unix)]
#[tokio::test]
async fn credentials_follow_the_daemons_startup_environment() {
    use std::os::unix::fs::PermissionsExt;
    const MODE: &str = "KACHE_OCI_TEST_CREDENTIAL_MODE";
    if let Ok(mode) = std::env::var(MODE) {
        let registry = Registry::start().await;
        let directory = std::path::PathBuf::from(std::env::var_os("DOCKER_CONFIG").unwrap());
        let host = registry.config.repository.split('/').next().unwrap();
        let response = directory.join("helper-response.json");
        let set_helper_secret = |password: &str| {
            std::fs::write(
                &response,
                serde_json::to_vec(&serde_json::json!({"Username":"user", "Secret":password}))
                    .unwrap(),
            )
            .unwrap();
        };
        if mode == "helper" {
            std::fs::write(
                directory.join("config.json"),
                serde_json::to_vec(&serde_json::json!({"credHelpers":{host:"kache-test"}}))
                    .unwrap(),
            )
            .unwrap();
            set_helper_secret("pass");
        } else {
            // Environment credentials take precedence even over a broken file.
            std::fs::write(directory.join("config.json"), "invalid Docker config").unwrap();
        }
        if mode == "token" {
            registry.state.lock().unwrap().authorization = Some("Bearer token".into());
        } else {
            registry.state.lock().unwrap().token_generation = Some(1);
        }
        let backend = OciBackend::new(&registry.config).await.unwrap();
        backend
            .put("key", b"{}".to_vec(), Some("application/json"))
            .await
            .unwrap();
        assert_eq!(
            backend.get("key", Some(2)).await.unwrap().unwrap().body,
            "{}"
        );
        if mode == "helper" {
            set_helper_secret("renewed");
            {
                let mut state = registry.state.lock().unwrap();
                state.token_generation = Some(2);
                state.token_authorization = Some(format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD.encode("user:renewed")
                ));
            }
            assert_eq!(
                backend.get("key", Some(2)).await.unwrap().unwrap().body,
                "{}"
            );
            backend
                .put("other", b"{}".to_vec(), Some("application/json"))
                .await
                .unwrap();
            assert_eq!(registry.state.lock().unwrap().token_requests, 4);
        }
        return;
    }
    for mode in ["token", "basic", "helper"] {
        let directory = tempfile::tempdir().unwrap();
        let helper = directory.path().join("docker-credential-kache-test");
        std::fs::write(
            &helper,
            "#!/bin/sh\n/bin/cat \"$DOCKER_CONFIG/helper-response.json\"\n",
        )
        .unwrap();
        std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "remote_backend::oci::tests::credentials_follow_the_daemons_startup_environment",
                "--nocapture",
            ])
            .env(MODE, mode)
            .env("DOCKER_CONFIG", directory.path())
            .env("PATH", directory.path())
            .env_remove("KACHE_OCI_TOKEN")
            .env_remove("KACHE_OCI_USERNAME")
            .env_remove("KACHE_OCI_PASSWORD");
        if mode == "token" {
            command.env("KACHE_OCI_TOKEN", "token");
        } else if mode == "basic" {
            command
                .env("KACHE_OCI_USERNAME", "user")
                .env("KACHE_OCI_PASSWORD", "pass");
        }
        let output = tokio::task::spawn_blocking(move || command.output().unwrap())
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "{mode}: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
