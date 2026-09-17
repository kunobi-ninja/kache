//! Build timeline submissions: `POST /v1/build-timelines`.
//!
//! A kache client sends one zstd-compressed [`BuildTimeline`] per build
//! session. The service checks it can read the record and stores it as sent,
//! with a few indexed columns. Nothing is derived on write.

use std::io::Read;

use anyhow::Result;
use async_trait::async_trait;
use axum::{
    Json,
    body::Body,
    extract::State,
    http::{HeaderMap, StatusCode, header::CONTENT_ENCODING},
};
use kache_core::timeline::{BUILD_TIMELINE_SCHEMA, BuildTimeline};
use kunobi_auth::server::OptionalAuth;
use serde::{Deserialize, Serialize};

use crate::{AppState, metrics, metrics::TimelineOutcome};

/// Default largest compressed body accepted. A kache workspace of 3,654
/// compiles makes a 126 KiB body, so this is a guard against a broken or
/// hostile client, not a size anyone should approach. It also leaves room for
/// a record at the decompressed limit that compresses poorly.
pub const DEFAULT_MAX_COMPRESSED_BYTES: usize = 8 << 20;
/// Default largest decompressed record accepted: about 100,000 compiles at
/// the ~330 bytes per unit a real record costs.
pub const DEFAULT_MAX_DECODED_BYTES: u64 = 32 << 20;

/// Body sizes this service accepts. Set with `--timeline-max-compressed-bytes`
/// and `--timeline-max-decoded-bytes` or their environment variables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimelineLimits {
    pub max_compressed_bytes: usize,
    pub max_decoded_bytes: u64,
}

impl Default for TimelineLimits {
    fn default() -> Self {
        Self {
            max_compressed_bytes: DEFAULT_MAX_COMPRESSED_BYTES,
            max_decoded_bytes: DEFAULT_MAX_DECODED_BYTES,
        }
    }
}

/// What happened to a submitted record.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StoreOutcome {
    /// No earlier submission of this record.
    Inserted,
    /// Replaced an earlier submission with no more units than this one.
    Replaced,
    /// An earlier submission had more units, so it was kept. A later push can
    /// hold fewer units once the client's event log has been rotated.
    Kept,
}

/// Where accepted records are stored.
#[async_trait]
pub trait TimelineStore {
    async fn store_timeline(
        &self,
        server_id: String,
        received_at_ms: u64,
        record: BuildTimeline,
        body: Vec<u8>,
    ) -> Result<StoreOutcome>;
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SubmitResponse {
    pub stored: StoreOutcome,
}

/// Flag and environment variable an operator raises to accept a larger body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SizeSetting {
    Compressed,
    Decoded,
}

impl SizeSetting {
    fn flag(self) -> &'static str {
        match self {
            SizeSetting::Compressed => "--timeline-max-compressed-bytes",
            SizeSetting::Decoded => "--timeline-max-decoded-bytes",
        }
    }

    fn env(self) -> &'static str {
        match self {
            SizeSetting::Compressed => "KACHE_TIMELINE_MAX_COMPRESSED_BYTES",
            SizeSetting::Decoded => "KACHE_TIMELINE_MAX_DECODED_BYTES",
        }
    }

    fn what(self) -> &'static str {
        match self {
            SizeSetting::Compressed => "compressed body",
            SizeSetting::Decoded => "decompressed record",
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Rejection {
    UnsupportedEncoding,
    TooLarge { setting: SizeSetting, limit: u64 },
    BadBody(String),
}

impl Rejection {
    fn too_large(setting: SizeSetting, limit: u64) -> Self {
        Rejection::TooLarge { setting, limit }
    }

    fn status(&self) -> StatusCode {
        match self {
            Rejection::UnsupportedEncoding => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Rejection::TooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            Rejection::BadBody(_) => StatusCode::BAD_REQUEST,
        }
    }

    fn outcome(&self) -> TimelineOutcome {
        match self {
            Rejection::UnsupportedEncoding => TimelineOutcome::UnsupportedEncoding,
            Rejection::TooLarge { .. } => TimelineOutcome::TooLarge,
            Rejection::BadBody(_) => TimelineOutcome::BadBody,
        }
    }

    /// What the client is told. A size rejection names the setting that
    /// decides it, so an operator reading one CI warning knows what to raise.
    fn message(&self) -> String {
        match self {
            Rejection::UnsupportedEncoding => {
                "build timelines must be sent with Content-Encoding: zstd".to_string()
            }
            Rejection::TooLarge { setting, limit } => format!(
                "{} is over this service's {limit} byte limit; raise {} ({})",
                setting.what(),
                setting.flag(),
                setting.env()
            ),
            Rejection::BadBody(detail) => detail.clone(),
        }
    }
}

/// What a rejected submission answers with.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ErrorResponse {
    pub error: String,
}

impl From<Rejection> for (StatusCode, Json<ErrorResponse>) {
    fn from(rejection: Rejection) -> Self {
        (
            rejection.status(),
            Json(ErrorResponse {
                error: rejection.message(),
            }),
        )
    }
}

/// Check the encoding, decompress within `max_decoded` bytes and parse the
/// record. Returns the record and the decompressed JSON it was read from.
pub(crate) fn decode_submission(
    content_encoding: Option<&str>,
    body: &[u8],
    max_decoded: u64,
) -> std::result::Result<(BuildTimeline, Vec<u8>), Rejection> {
    if !content_encoding.is_some_and(|value| value.trim().eq_ignore_ascii_case("zstd")) {
        return Err(Rejection::UnsupportedEncoding);
    }
    let decoded = decode_bounded(body, max_decoded)?;
    let record: BuildTimeline = serde_json::from_slice(&decoded)
        .map_err(|error| Rejection::BadBody(format!("not a build timeline: {error}")))?;
    if record.schema != BUILD_TIMELINE_SCHEMA {
        return Err(Rejection::BadBody(format!(
            "unsupported build timeline schema {}",
            record.schema
        )));
    }
    if record.client_record_id.trim().is_empty() {
        return Err(Rejection::BadBody("empty client_record_id".to_string()));
    }
    Ok((record, decoded))
}

/// Decompress without ever holding more than `max + 1` bytes.
fn decode_bounded(body: &[u8], max: u64) -> std::result::Result<Vec<u8>, Rejection> {
    let decoder = zstd::stream::read::Decoder::new(body)
        .map_err(|error| Rejection::BadBody(format!("not zstd: {error}")))?;
    let mut decoded = Vec::new();
    decoder
        .take(max.saturating_add(1))
        .read_to_end(&mut decoded)
        .map_err(|error| Rejection::BadBody(format!("not zstd: {error}")))?;
    if decoded.len() as u64 > max {
        return Err(Rejection::too_large(SizeSetting::Decoded, max));
    }
    Ok(decoded)
}

/// Whether reading a body failed because it passed the length limit, as
/// opposed to the client going away.
fn is_length_limit(error: &axum::Error) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(current) = source {
        if current.is::<http_body_util::LengthLimitError>() {
            return true;
        }
        source = current.source();
    }
    false
}

pub(crate) async fn submit_timeline(
    State(state): State<AppState>,
    OptionalAuth(identity): OptionalAuth,
    headers: HeaderMap,
    body: Body,
) -> std::result::Result<Json<SubmitResponse>, (StatusCode, Json<ErrorResponse>)> {
    let record = |outcome| metrics::metrics().record_timeline(outcome);
    let refuse = |status: StatusCode, error: &str| {
        (
            status,
            Json(ErrorResponse {
                error: error.to_string(),
            }),
        )
    };

    if state.auth.is_some() && identity.is_none() {
        record(TimelineOutcome::Unauthorized);
        return Err(refuse(
            StatusCode::UNAUTHORIZED,
            "build timelines need the service's credentials",
        ));
    }

    let repository = state.repository.read().await.clone();
    let Some(repository) = repository.filter(|_| state.is_ready()) else {
        record(TimelineOutcome::NotReady);
        return Err(refuse(
            StatusCode::SERVICE_UNAVAILABLE,
            "this service is not currently storing build timelines",
        ));
    };

    let bytes = match axum::body::to_bytes(body, state.timeline_limits.max_compressed_bytes).await {
        Ok(bytes) => bytes,
        Err(error) => {
            let rejection = if is_length_limit(&error) {
                Rejection::too_large(
                    SizeSetting::Compressed,
                    state.timeline_limits.max_compressed_bytes as u64,
                )
            } else {
                Rejection::BadBody(format!("reading body: {error}"))
            };
            record(rejection.outcome());
            return Err(rejection.into());
        }
    };

    let content_encoding = headers
        .get(CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok());
    let (timeline, decoded) = match decode_submission(
        content_encoding,
        &bytes,
        state.timeline_limits.max_decoded_bytes,
    ) {
        Ok(decoded) => decoded,
        Err(rejection) => {
            tracing::info!(planner = %state.planner_name, ?rejection, "timeline rejected");
            record(rejection.outcome());
            return Err(rejection.into());
        }
    };

    let unit_count = timeline.units.len();
    match repository
        .store_timeline(state.planner_name.clone(), now_millis(), timeline, decoded)
        .await
    {
        Ok(stored) => {
            tracing::info!(planner = %state.planner_name, unit_count, ?stored, "timeline stored");
            record(match stored {
                StoreOutcome::Inserted => TimelineOutcome::Inserted,
                StoreOutcome::Replaced => TimelineOutcome::Replaced,
                StoreOutcome::Kept => TimelineOutcome::Kept,
            });
            Ok(Json(SubmitResponse { stored }))
        }
        Err(error) => {
            tracing::warn!(planner = %state.planner_name, %error, "storing timeline failed");
            record(TimelineOutcome::StoreError);
            Err(refuse(
                StatusCode::INTERNAL_SERVER_ERROR,
                "storing the build timeline failed",
            ))
        }
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PlannerDataSource, SharedRepository, router};
    use axum::http::{Request, header};
    use http_body_util::BodyExt;
    use kache_core::PrefetchCandidate;
    use std::sync::{Arc, Mutex, atomic::AtomicBool};
    use tokio::sync::RwLock;
    use tower::util::ServiceExt;

    type StoredCall = (String, u64, BuildTimeline, Vec<u8>);

    struct RecordingRepository {
        result: std::result::Result<StoreOutcome, String>,
        calls: Mutex<Vec<StoredCall>>,
    }

    impl RecordingRepository {
        fn returning(result: std::result::Result<StoreOutcome, String>) -> Arc<Self> {
            Arc::new(Self {
                result,
                calls: Mutex::new(Vec::new()),
            })
        }

        fn calls(&self) -> Vec<StoredCall> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl PlannerDataSource for RecordingRepository {
        async fn shard_candidates(
            &self,
            _namespace: &str,
            _deps: &[(String, String)],
        ) -> Result<Vec<PrefetchCandidate>> {
            Ok(Vec::new())
        }

        async fn history_candidates(
            &self,
            _crate_names: &[String],
        ) -> Result<Vec<PrefetchCandidate>> {
            Ok(Vec::new())
        }

        async fn key_cache_keys_for_crate(&self, _crate_name: &str) -> Result<Vec<String>> {
            Ok(Vec::new())
        }

        async fn identity_candidates(&self, _identity_key: &str) -> Result<Vec<PrefetchCandidate>> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl TimelineStore for RecordingRepository {
        async fn store_timeline(
            &self,
            server_id: String,
            received_at_ms: u64,
            record: BuildTimeline,
            body: Vec<u8>,
        ) -> Result<StoreOutcome> {
            self.calls
                .lock()
                .unwrap()
                .push((server_id, received_at_ms, record, body));
            self.result.clone().map_err(anyhow::Error::msg)
        }
    }

    fn app(token: Option<&str>, ready: bool, repository: Option<SharedRepository>) -> axum::Router {
        app_with_limits(token, ready, repository, TimelineLimits::default())
    }

    fn app_with_limits(
        token: Option<&str>,
        ready: bool,
        repository: Option<SharedRepository>,
        timeline_limits: TimelineLimits,
    ) -> axum::Router {
        let settings = crate::AuthSettings {
            token: token.map(str::to_string),
            ..Default::default()
        };
        router(AppState {
            auth: crate::PlannerAuth::from_settings(&settings),
            discovery: None,
            planner_name: "edge-1".to_string(),
            repository: Arc::new(RwLock::new(repository)),
            ready: Arc::new(AtomicBool::new(ready)),
            timeline_limits,
        })
    }

    #[tokio::test]
    async fn configured_limits_replace_the_defaults() {
        let repository = RecordingRepository::returning(Ok(StoreOutcome::Inserted));
        let (json, body) = valid_body();

        let tight_compressed = app_with_limits(
            None,
            true,
            Some(repository.clone()),
            TimelineLimits {
                max_compressed_bytes: body.len() - 1,
                ..TimelineLimits::default()
            },
        )
        .oneshot(submission("zstd", body.clone(), None))
        .await
        .unwrap();
        assert_eq!(tight_compressed.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let compressed_message = error_message(tight_compressed).await;
        assert!(
            compressed_message.contains("compressed body")
                && compressed_message.contains("--timeline-max-compressed-bytes")
                && compressed_message.contains(&(body.len() - 1).to_string()),
            "{compressed_message}"
        );

        let tight_decoded = app_with_limits(
            None,
            true,
            Some(repository.clone()),
            TimelineLimits {
                max_decoded_bytes: json.len() as u64 - 1,
                ..TimelineLimits::default()
            },
        )
        .oneshot(submission("zstd", body.clone(), None))
        .await
        .unwrap();
        assert_eq!(tight_decoded.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let decoded_message = error_message(tight_decoded).await;
        assert!(
            decoded_message.contains("decompressed record")
                && decoded_message.contains("--timeline-max-decoded-bytes")
                && decoded_message.contains("KACHE_TIMELINE_MAX_DECODED_BYTES")
                && decoded_message.contains(&(json.len() as u64 - 1).to_string()),
            "{decoded_message}"
        );
        assert!(repository.calls().is_empty());

        let generous = app_with_limits(
            None,
            true,
            Some(repository.clone()),
            TimelineLimits {
                max_compressed_bytes: body.len(),
                max_decoded_bytes: json.len() as u64,
            },
        )
        .oneshot(submission("zstd", body, None))
        .await
        .unwrap();
        assert_eq!(generous.status(), StatusCode::OK);
        assert_eq!(repository.calls().len(), 1);
    }

    async fn error_message(response: axum::response::Response) -> String {
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice::<ErrorResponse>(&bytes)
            .unwrap()
            .error
    }

    fn submission(encoding: &str, body: Vec<u8>, token: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder()
            .method("POST")
            .uri("/v1/build-timelines")
            .header(CONTENT_ENCODING, encoding);
        if let Some(token) = token {
            builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        builder.body(Body::from(body)).unwrap()
    }

    fn valid_body() -> (Vec<u8>, Vec<u8>) {
        let json = record_json(BUILD_TIMELINE_SCHEMA, "r1");
        let body = zstd(&json);
        (json, body)
    }

    #[tokio::test]
    async fn stores_a_submission_under_this_servers_name() {
        let repository = RecordingRepository::returning(Ok(StoreOutcome::Inserted));
        let (json, body) = valid_body();
        let before = now_millis();
        let response = app(None, true, Some(repository.clone()))
            .oneshot(submission("zstd", body, None))
            .await
            .unwrap();
        let after = now_millis();

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            serde_json::from_slice::<SubmitResponse>(&bytes).unwrap(),
            SubmitResponse {
                stored: StoreOutcome::Inserted
            }
        );
        let calls = repository.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "edge-1");
        // The receipt time is the wall clock at the request, not a constant:
        // the stored row is what "how stale is this record" queries read.
        assert!(
            (before..=after).contains(&calls[0].1),
            "received_at_ms {} outside {before}..={after}",
            calls[0].1
        );
        assert!(
            before > 1_600_000_000_000,
            "clock must be a real epoch time"
        );
        assert_eq!(calls[0].2.client_record_id, "r1");
        assert_eq!(calls[0].3, json);
    }

    #[tokio::test]
    async fn requires_the_configured_token() {
        let repository = RecordingRepository::returning(Ok(StoreOutcome::Inserted));
        for token in [None, Some("wrong")] {
            let response = app(Some("secret"), true, Some(repository.clone()))
                .oneshot(submission("zstd", valid_body().1, token))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{token:?}");
        }
        assert!(repository.calls().is_empty());

        let response = app(Some("secret"), true, Some(repository.clone()))
            .oneshot(submission("zstd", valid_body().1, Some("secret")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn refuses_while_not_ready_or_without_a_store() {
        let repository = RecordingRepository::returning(Ok(StoreOutcome::Inserted));
        let not_ready = app(None, false, Some(repository.clone()))
            .oneshot(submission("zstd", valid_body().1, None))
            .await
            .unwrap();
        assert_eq!(not_ready.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(repository.calls().is_empty());

        let no_store = app(None, true, None)
            .oneshot(submission("zstd", valid_body().1, None))
            .await
            .unwrap();
        assert_eq!(no_store.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn rejects_other_encodings_before_storing() {
        let repository = RecordingRepository::returning(Ok(StoreOutcome::Inserted));
        let (json, _) = valid_body();
        let response = app(None, true, Some(repository.clone()))
            .oneshot(submission("gzip", json, None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_eq!(
            error_message(response).await,
            "build timelines must be sent with Content-Encoding: zstd"
        );
        assert!(repository.calls().is_empty());
    }

    #[tokio::test]
    async fn rejects_a_body_over_the_compressed_limit() {
        let repository = RecordingRepository::returning(Ok(StoreOutcome::Inserted));
        let response = app(None, true, Some(repository.clone()))
            .oneshot(submission(
                "zstd",
                vec![0; DEFAULT_MAX_COMPRESSED_BYTES + 1],
                None,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let message = error_message(response).await;
        assert!(
            message.contains("--timeline-max-compressed-bytes")
                && message.contains("KACHE_TIMELINE_MAX_COMPRESSED_BYTES")
                && message.contains(&DEFAULT_MAX_COMPRESSED_BYTES.to_string()),
            "a refused body must say what to raise: {message}"
        );
        assert!(repository.calls().is_empty());
    }

    #[tokio::test]
    async fn accepts_a_body_above_the_default_axum_limit() {
        // Incompressible bytes inside a zstd frame: the request body is larger
        // than axum's 2 MiB default, and the record still has to arrive.
        let mut record = BuildTimeline {
            schema: BUILD_TIMELINE_SCHEMA,
            client_record_id: "big".to_string(),
            ..BuildTimeline::default()
        };
        let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
        record.kache_version = (0..5 << 20)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                char::from(b'a' + (seed % 26) as u8)
            })
            .collect();
        let body = zstd(&serde_json::to_vec(&record).unwrap());
        assert!(
            body.len() > 2 << 20,
            "test body must exceed axum's default limit"
        );

        let repository = RecordingRepository::returning(Ok(StoreOutcome::Replaced));
        let response = app(None, true, Some(repository.clone()))
            .oneshot(submission("zstd", body, None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(repository.calls().len(), 1);
    }

    #[tokio::test]
    async fn refusals_before_the_body_say_why() {
        let repository = RecordingRepository::returning(Ok(StoreOutcome::Inserted));
        let unauthorized = app(Some("secret"), true, Some(repository.clone()))
            .oneshot(submission("zstd", valid_body().1, None))
            .await
            .unwrap();
        assert_eq!(
            error_message(unauthorized).await,
            "build timelines need the service's credentials"
        );

        let not_ready = app(None, false, Some(repository))
            .oneshot(submission("zstd", valid_body().1, None))
            .await
            .unwrap();
        assert_eq!(
            error_message(not_ready).await,
            "this service is not currently storing build timelines"
        );
    }

    #[tokio::test]
    async fn reports_a_failed_write_as_a_server_error() {
        let repository = RecordingRepository::returning(Err("disk full".to_string()));
        let response = app(None, true, Some(repository.clone()))
            .oneshot(submission("zstd", valid_body().1, None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            error_message(response).await,
            "storing the build timeline failed"
        );
        assert_eq!(repository.calls().len(), 1);
    }

    #[tokio::test]
    async fn length_limit_errors_are_told_apart_from_other_body_errors() {
        let limited = axum::body::to_bytes(Body::from(vec![0_u8; 10]), 1)
            .await
            .unwrap_err();
        assert!(is_length_limit(&limited));

        let other = axum::Error::new(std::io::Error::other("connection reset"));
        assert!(!is_length_limit(&other));
    }

    fn record_json(schema: u32, client_record_id: &str) -> Vec<u8> {
        serde_json::to_vec(&BuildTimeline {
            schema,
            client_record_id: client_record_id.to_string(),
            session_id: "s".to_string(),
            ..BuildTimeline::default()
        })
        .unwrap()
    }

    fn zstd(bytes: &[u8]) -> Vec<u8> {
        zstd::encode_all(bytes, 3).unwrap()
    }

    #[test]
    fn limits_match_the_documented_sizes() {
        assert_eq!(DEFAULT_MAX_COMPRESSED_BYTES, 8_388_608);
        assert_eq!(DEFAULT_MAX_DECODED_BYTES, 33_554_432);
    }

    #[test]
    fn decodes_a_zstd_record_and_returns_its_json() {
        let json = record_json(BUILD_TIMELINE_SCHEMA, "r1");
        let (record, decoded) =
            decode_submission(Some("zstd"), &zstd(&json), DEFAULT_MAX_DECODED_BYTES).unwrap();
        assert_eq!(record.client_record_id, "r1");
        assert_eq!(decoded, json);
    }

    #[test]
    fn encoding_name_is_trimmed_and_case_insensitive() {
        let body = zstd(&record_json(BUILD_TIMELINE_SCHEMA, "r1"));
        assert!(decode_submission(Some(" ZStd "), &body, DEFAULT_MAX_DECODED_BYTES).is_ok());
    }

    #[test]
    fn rejects_anything_but_zstd() {
        let json = record_json(BUILD_TIMELINE_SCHEMA, "r1");
        for encoding in [None, Some("gzip"), Some("identity"), Some("")] {
            assert_eq!(
                decode_submission(encoding, &json, DEFAULT_MAX_DECODED_BYTES).unwrap_err(),
                Rejection::UnsupportedEncoding,
                "{encoding:?}"
            );
        }
    }

    #[test]
    fn decoded_size_limit_is_inclusive() {
        let json = record_json(BUILD_TIMELINE_SCHEMA, "r1");
        let body = zstd(&json);
        let exact = json.len() as u64;
        assert!(decode_submission(Some("zstd"), &body, exact).is_ok());
        assert_eq!(
            decode_submission(Some("zstd"), &body, exact - 1).unwrap_err(),
            Rejection::too_large(SizeSetting::Decoded, exact - 1)
        );
    }

    #[test]
    fn rejects_bodies_that_are_not_zstd() {
        let err = decode_submission(Some("zstd"), b"not compressed", DEFAULT_MAX_DECODED_BYTES)
            .unwrap_err();
        assert!(matches!(err, Rejection::BadBody(message) if message.starts_with("not zstd")));
    }

    #[test]
    fn rejects_json_that_is_not_a_record() {
        let err = decode_submission(Some("zstd"), &zstd(b"{\"x\":1}"), DEFAULT_MAX_DECODED_BYTES)
            .unwrap_err();
        assert!(matches!(err, Rejection::BadBody(message) if message.starts_with("not a build")));
    }

    #[test]
    fn rejects_unknown_schema_versions() {
        for schema in [0, BUILD_TIMELINE_SCHEMA + 1] {
            let body = zstd(&record_json(schema, "r1"));
            let err =
                decode_submission(Some("zstd"), &body, DEFAULT_MAX_DECODED_BYTES).unwrap_err();
            assert!(
                matches!(&err, Rejection::BadBody(message) if message.contains("schema")),
                "{schema}: {err:?}"
            );
        }
    }

    #[test]
    fn rejects_a_blank_client_record_id() {
        let body = zstd(&record_json(BUILD_TIMELINE_SCHEMA, "  "));
        let err = decode_submission(Some("zstd"), &body, DEFAULT_MAX_DECODED_BYTES).unwrap_err();
        assert_eq!(
            err,
            Rejection::BadBody("empty client_record_id".to_string())
        );
    }

    #[test]
    fn rejections_map_to_status_and_outcome() {
        let cases = [
            (
                Rejection::UnsupportedEncoding,
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                TimelineOutcome::UnsupportedEncoding,
            ),
            (
                Rejection::too_large(SizeSetting::Decoded, 10),
                StatusCode::PAYLOAD_TOO_LARGE,
                TimelineOutcome::TooLarge,
            ),
            (
                Rejection::BadBody(String::new()),
                StatusCode::BAD_REQUEST,
                TimelineOutcome::BadBody,
            ),
        ];
        for (rejection, status, outcome) in cases {
            assert_eq!(rejection.status(), status);
            assert_eq!(rejection.outcome(), outcome);
        }

        assert_eq!(
            Rejection::too_large(SizeSetting::Compressed, 7).message(),
            "compressed body is over this service's 7 byte limit; raise \
             --timeline-max-compressed-bytes (KACHE_TIMELINE_MAX_COMPRESSED_BYTES)"
        );
        assert_eq!(
            Rejection::too_large(SizeSetting::Decoded, 9).message(),
            "decompressed record is over this service's 9 byte limit; raise \
             --timeline-max-decoded-bytes (KACHE_TIMELINE_MAX_DECODED_BYTES)"
        );
        assert_eq!(Rejection::BadBody("detail".to_string()).message(), "detail");
    }

    #[test]
    fn store_outcomes_use_snake_case() {
        let json = serde_json::to_string(&SubmitResponse {
            stored: StoreOutcome::Replaced,
        })
        .unwrap();
        assert_eq!(json, r#"{"stored":"replaced"}"#);
    }
}
