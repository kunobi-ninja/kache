//! Process-level acceptance for the S3 sync path (kunobi-ninja/kache#695).
//!
//! The in-process remote tests prove individual object operations. This test
//! instead drives the shipping binary across two isolated caches and source
//! trees through OpenDAL's S3 transport. A small local wire store verifies
//! SigV4 header emission (not the cryptographic signature) and keeps the gate
//! deterministic while exercising the v3 manifest, compressed pack, download,
//! import, and restore path together.

use axum::{
    Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, OriginalUri, Query, State},
    http::{
        HeaderMap, Method, StatusCode,
        header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, ETAG, IF_NONE_MATCH},
    },
    response::{IntoResponse, Response},
};
use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    process::{Output, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tempfile::TempDir;
use tokio::sync::oneshot;

mod common;
use common::{build_kache, isolated_config_path, kache_binary};

const BUCKET: &str = "kache-test";
const PREFIX: &str = "artifacts";

#[derive(Clone, Default)]
struct MockS3 {
    objects: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
    requests: Arc<Mutex<Vec<String>>>,
    sigv4_requests: Arc<AtomicUsize>,
}

impl MockS3 {
    fn keys(&self) -> Vec<String> {
        self.objects.lock().unwrap().keys().cloned().collect()
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }

    fn sigv4_request_count(&self) -> usize {
        self.sigv4_requests.load(Ordering::Relaxed)
    }
}

fn xml_response(status: StatusCode, body: String) -> Response {
    (status, [(CONTENT_TYPE, "application/xml")], body).into_response()
}

fn s3_error(status: StatusCode, code: &str, message: &str) -> Response {
    xml_response(
        status,
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <Error><Code>{code}</Code><Message>{message}</Message></Error>"
        ),
    )
}

fn list_objects(store: &MockS3, prefix: &str) -> Response {
    let objects = store.objects.lock().unwrap();
    let matches: Vec<_> = objects
        .iter()
        .filter(|(key, _)| key.starts_with(prefix))
        .collect();
    let contents = matches
        .iter()
        .map(|(key, value)| {
            format!(
                "<Contents><Key>{key}</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified>\
                 <ETag>&quot;mock&quot;</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
                value.len()
            )
        })
        .collect::<String>();
    xml_response(
        StatusCode::OK,
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Name>{BUCKET}</Name><Prefix>{prefix}</Prefix><KeyCount>{}</KeyCount>\
             <MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>",
            matches.len()
        ),
    )
}

async fn s3_handler(
    State(store): State<MockS3>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    store
        .requests
        .lock()
        .unwrap()
        .push(format!("{method} {uri}"));

    let has_sigv4 = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.starts_with("AWS4-HMAC-SHA256 ")
                && value.contains("Credential=test-access-key/")
                && value.contains("SignedHeaders=")
                && value.contains("Signature=")
        })
        && headers.contains_key("x-amz-date")
        && headers.contains_key("x-amz-content-sha256");
    if !has_sigv4 {
        return s3_error(
            StatusCode::FORBIDDEN,
            "SignatureDoesNotMatch",
            "required SigV4 headers are missing",
        );
    }
    store.sigv4_requests.fetch_add(1, Ordering::Relaxed);

    let bucket_path = format!("/{BUCKET}");
    if (uri.path() == bucket_path || uri.path() == format!("{bucket_path}/"))
        && method == Method::GET
        && query.get("list-type").is_some_and(|value| value == "2")
    {
        return list_objects(&store, query.get("prefix").map_or("", String::as_str));
    }

    let object_prefix = format!("/{BUCKET}/");
    let Some(key) = uri.path().strip_prefix(&object_prefix) else {
        return s3_error(StatusCode::NOT_FOUND, "NoSuchBucket", "bucket not found");
    };

    match method {
        Method::PUT => {
            let create_only = headers
                .get(IF_NONE_MATCH)
                .and_then(|value| value.to_str().ok())
                == Some("*");
            let mut objects = store.objects.lock().unwrap();
            if create_only && objects.contains_key(key) {
                return s3_error(
                    StatusCode::PRECONDITION_FAILED,
                    "PreconditionFailed",
                    "object already exists",
                );
            }
            objects.insert(key.to_string(), body.to_vec());
            Response::builder()
                .status(StatusCode::OK)
                .header(ETAG, "\"mock\"")
                .body(Body::empty())
                .unwrap()
        }
        Method::HEAD => {
            let objects = store.objects.lock().unwrap();
            let Some(value) = objects.get(key) else {
                return Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .unwrap();
            };
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_LENGTH, value.len())
                .header(ETAG, "\"mock\"")
                .body(Body::empty())
                .unwrap()
        }
        Method::GET => {
            let objects = store.objects.lock().unwrap();
            let Some(value) = objects.get(key) else {
                return s3_error(StatusCode::NOT_FOUND, "NoSuchKey", "object not found");
            };
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_LENGTH, value.len())
                .header(CONTENT_TYPE, "application/octet-stream")
                .header(ETAG, "\"mock\"")
                .body(Body::from(value.clone()))
                .unwrap()
        }
        _ => s3_error(
            StatusCode::METHOD_NOT_ALLOWED,
            "MethodNotAllowed",
            "unsupported method",
        ),
    }
}

struct Client {
    _root: TempDir,
    cache_dir: PathBuf,
    runtime_dir: PathBuf,
    config_path: PathBuf,
    command_seq: usize,
}

impl Client {
    fn new() -> Self {
        let root = TempDir::new().unwrap();
        let cache_dir = root.path().join("cache");
        let runtime_dir = root.path().join("run");
        std::fs::create_dir_all(&cache_dir).unwrap();
        std::fs::create_dir_all(&runtime_dir).unwrap();
        let config_path = isolated_config_path(&cache_dir);
        Self {
            _root: root,
            cache_dir,
            runtime_dir,
            config_path,
            command_seq: 0,
        }
    }

    fn configure_local_only(&self) {
        std::fs::write(
            &self.config_path,
            format!(
                "[cache]\nlocal_only = true\nignore_env = true\nlocal_store = {}\nruntime_dir = {}\n",
                toml_path(&self.cache_dir),
                toml_path(&self.runtime_dir)
            ),
        )
        .unwrap();
    }

    fn configure_s3(&self, endpoint: &str) {
        std::fs::write(
            &self.config_path,
            format!(
                "[cache]\nignore_env = true\nlocal_store = {}\nruntime_dir = {}\n\
                 prefetch_enabled = false\n\n\
                 [cache.remote]\ntype = \"s3\"\nbucket = \"{BUCKET}\"\n\
                 endpoint = \"{endpoint}\"\nregion = \"us-east-1\"\nprefix = \"{PREFIX}\"\n",
                toml_path(&self.cache_dir),
                toml_path(&self.runtime_dir)
            ),
        )
        .unwrap();
    }

    fn command(&self) -> std::process::Command {
        let mut command = std::process::Command::new(kache_binary());
        command
            .env("KACHE_CACHE_DIR", &self.cache_dir)
            .env("KACHE_RUNTIME_DIR", &self.runtime_dir)
            .env("KACHE_CONFIG", &self.config_path)
            .env("KACHE_LOG", "off")
            .env("KACHE_S3_ACCESS_KEY", "test-access-key")
            .env("KACHE_S3_SECRET_KEY", "test-secret-key")
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .env_remove("KACHE_DISABLED")
            .env_remove("KACHE_NAMESPACE")
            .env_remove("KACHE_BASE_DIR")
            .env_remove("KACHE_LOCAL_ONLY")
            .env_remove("KACHE_S3_BUCKET")
            .env_remove("KACHE_S3_ENDPOINT")
            .env_remove("KACHE_S3_REGION")
            .env_remove("KACHE_S3_PREFIX")
            .env_remove("KACHE_S3_PROFILE")
            .env_remove("KACHE_SOCKET_PATH")
            .env_remove("RUSTC_WRAPPER")
            .env_remove("CARGO_BUILD_RUSTC_WRAPPER")
            .env_remove("GITHUB_ACTIONS")
            .env_remove("GITLAB_CI")
            .env_remove("CI");
        command
    }

    fn run(&mut self, cwd: &Path, args: &[String]) -> Output {
        let seq = self.command_seq;
        self.command_seq += 1;
        let out_path = self.runtime_dir.join(format!("command-{seq}.out"));
        let err_path = self.runtime_dir.join(format!("command-{seq}.err"));
        let stdout = std::fs::File::create(&out_path).unwrap();
        let stderr = std::fs::File::create(&err_path).unwrap();
        let mut child = self
            .command()
            .args(args)
            .current_dir(cwd)
            .stdin(Stdio::null())
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .expect("spawn kache command");
        let deadline = Instant::now() + Duration::from_secs(90);
        let status = loop {
            if let Some(status) = child.try_wait().expect("poll kache command") {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "kache command {args:?} timed out\nstdout: {}\nstderr: {}",
                    std::fs::read_to_string(&out_path).unwrap_or_default(),
                    std::fs::read_to_string(&err_path).unwrap_or_default(),
                );
            }
            std::thread::sleep(Duration::from_millis(25));
        };
        Output {
            status,
            stdout: std::fs::read(out_path).unwrap(),
            stderr: std::fs::read(err_path).unwrap(),
        }
    }

    fn compile(&mut self, project: &Path, output_dir: &Path) {
        std::fs::create_dir_all(output_dir).unwrap();
        let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
        let output = self.run(
            project,
            &[
                rustc,
                "--crate-name".to_string(),
                "s3remote".to_string(),
                "--crate-type".to_string(),
                "lib".to_string(),
                "--edition".to_string(),
                "2021".to_string(),
                "--emit=link".to_string(),
                "--out-dir".to_string(),
                output_dir.display().to_string(),
                "lib.rs".to_string(),
            ],
        );
        assert!(
            output.status.success(),
            "kache rustc failed\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    fn sync(&mut self, project: &Path, direction: &str) {
        let output = self.run(
            project,
            &[
                "sync".to_string(),
                direction.to_string(),
                "--all".to_string(),
            ],
        );
        assert!(
            output.status.success(),
            "kache sync {direction} failed\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    fn report(&mut self, project: &Path) -> serde_json::Value {
        let output = self.run(
            project,
            &[
                "report".to_string(),
                "--format".to_string(),
                "json".to_string(),
                "--since".to_string(),
                "1h".to_string(),
            ],
        );
        assert!(output.status.success(), "kache report failed");
        serde_json::from_slice(&output.stdout).expect("report must be valid JSON")
    }
}

fn toml_path(path: &Path) -> String {
    toml::Value::String(path.to_string_lossy().into_owned()).to_string()
}

fn write_fixture(root: &Path) {
    std::fs::write(
        root.join("lib.rs"),
        "pub fn remotely_restored() -> u64 { 0x5_3_000_695 }\n",
    )
    .unwrap();
}

fn only_rlib(root: &Path) -> PathBuf {
    let files: Vec<_> = std::fs::read_dir(root)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("rlib"))
        .collect();
    assert_eq!(files.len(), 1, "expected one rlib in {}", root.display());
    files.into_iter().next().unwrap()
}

fn run_round_trip(endpoint: &str) {
    build_kache();

    let producer_source = TempDir::new().unwrap();
    let consumer_source = TempDir::new().unwrap();
    write_fixture(producer_source.path());
    write_fixture(consumer_source.path());

    let producer_output = TempDir::new().unwrap();
    let mut producer = Client::new();
    producer.configure_local_only();
    producer.compile(producer_source.path(), producer_output.path());
    let producer_report = producer.report(producer_source.path());
    assert!(
        producer_report["all_events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["crate_name"] == "s3remote" && event["result"] == "miss"),
        "producer must compile cold: {producer_report}"
    );

    producer.configure_s3(endpoint);
    producer.sync(producer_source.path(), "--push");

    let consumer_output = TempDir::new().unwrap();
    let mut consumer = Client::new();
    consumer.configure_s3(endpoint);
    consumer.sync(consumer_source.path(), "--pull");
    consumer.compile(consumer_source.path(), consumer_output.path());
    let consumer_report = consumer.report(consumer_source.path());
    assert!(
        consumer_report["all_events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| {
                event["crate_name"] == "s3remote"
                    && event["result"] == "local_hit"
                    && event["compiler_runs"] == 0
            }),
        "consumer must restore the pulled entry without rustc: {consumer_report}"
    );

    assert_eq!(
        std::fs::read(only_rlib(producer_output.path())).unwrap(),
        std::fs::read(only_rlib(consumer_output.path())).unwrap(),
        "the S3-restored artifact must match the producer byte-for-byte"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_sync_round_trip_across_isolated_caches() {
    let store = MockS3::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .fallback(s3_handler)
        .layer(DefaultBodyLimit::max(16 * 1024 * 1024))
        .with_state(store.clone());
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let round_trip = tokio::task::spawn_blocking(move || run_round_trip(&endpoint)).await;
    let _ = shutdown_tx.send(());
    server.await.unwrap().unwrap();
    round_trip.expect("round-trip worker panicked");

    let keys = store.keys();
    assert!(
        keys.iter().any(
            |key| key.starts_with(&format!("{PREFIX}/v3/manifests/")) && key.ends_with(".json")
        ),
        "S3 must contain a v3 manifest: {keys:?}"
    );
    assert!(
        keys.iter().any(|key| key.starts_with(&format!("{PREFIX}/v3/packs/")) && key.ends_with(".tar.zst")),
        "S3 must contain a compressed v3 pack: {keys:?}"
    );

    let requests = store.requests();
    assert_eq!(
        store.sigv4_request_count(),
        requests.len(),
        "every S3 wire request must carry the expected SigV4 headers: {requests:?}"
    );
    assert!(
        requests
            .iter()
            .any(|request| request.starts_with("PUT /kache-test/artifacts/v3/manifests/")),
        "manifest upload did not traverse S3: {requests:?}"
    );
    assert!(
        requests
            .iter()
            .any(|request| request.starts_with("GET /kache-test/artifacts/v3/packs/")),
        "pack download did not traverse S3: {requests:?}"
    );
    assert!(
        requests
            .iter()
            .any(|request| request.contains("list-type=2")),
        "pull did not list S3 manifests: {requests:?}"
    );
}
