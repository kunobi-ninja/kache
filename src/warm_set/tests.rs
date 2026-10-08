use super::*;
use crate::build_reports::{ArtifactStatus, ReportEntry};
use crate::remote_backend::memory_backend;

fn identity() -> BuildIdentity {
    BuildIdentity {
        repository: Some("repository".into()),
        target: Some("target".into()),
        toolchain_hash: Some("a".repeat(64)),
        profile: Some("debug".into()),
        build_shape: Some("cargo-build-default".into()),
        lock_digest: Some("b".repeat(64)),
    }
}

fn report(session: &str, finished: u64, keys: &[(&str, &str)]) -> BuildReport {
    BuildReport {
        schema: 1,
        session_id: session.into(),
        root_hash: "c".repeat(64),
        namespace: "fixture".into(),
        identity: identity(),
        commit: None,
        git_ref: None,
        parent_commits: vec![],
        started_at_ms: finished - 1,
        finished_at_ms: finished,
        producer_captured_at_ms: None,
        entries: keys
            .iter()
            .map(|(key, crate_name)| ReportEntry {
                cache_key: key.to_string(),
                crate_name: crate_name.to_string(),
                result: crate::events::EventResult::Miss,
                started_at_ms: finished - 1,
                finished_at_ms: finished,
                start_offset_ms: 0,
                ci_start_offset_ms: None,
                elapsed_ms: 1,
                compile_time_ms: 10,
                artifact_size: 123,
                event_schema: 25,
                demands: vec![],
                artifact_status: ArtifactStatus::Observed,
            })
            .collect(),
    }
}

#[test]
fn every_compatibility_dimension_and_unknown_fact_is_rejected() {
    let expected = identity();
    assert!(compatible(&expected, &expected));
    for field in 0..6 {
        let mut changed = expected.clone();
        let value = match field {
            0 => &mut changed.repository,
            1 => &mut changed.target,
            2 => &mut changed.toolchain_hash,
            3 => &mut changed.profile,
            4 => &mut changed.build_shape,
            _ => &mut changed.lock_digest,
        };
        *value = Some("different".into());
        assert!(!compatible(&changed, &expected), "dimension {field}");
        *match field {
            0 => &mut changed.repository,
            1 => &mut changed.target,
            2 => &mut changed.toolchain_hash,
            3 => &mut changed.profile,
            4 => &mut changed.build_shape,
            _ => &mut changed.lock_digest,
        } = None;
        assert!(
            !compatible(&changed, &expected),
            "unknown dimension {field}"
        );
        assert!(
            !compatible(&changed, &changed),
            "unknown expected dimension {field}"
        );
    }
}

#[test]
fn replay_keeps_first_observed_order_and_deduplicates_keys() {
    let r = report(
        "session",
        10,
        &[("b", "second"), ("a", "first"), ("b", "second")],
    );
    assert_eq!(
        ordered_entries(&r)
            .into_iter()
            .map(|entry| entry.cache_key.as_str())
            .collect::<Vec<_>>(),
        ["b", "a"]
    );
}

#[tokio::test]
async fn discovery_selects_latest_matching_report_and_ignores_newer_incompatible_reports() {
    let backend: Arc<dyn RemoteBackend> = Arc::new(memory_backend());
    let key = "d".repeat(64);
    let old = report("old", 10, &[(&key, "alpha")]);
    let latest = report("latest", 20, &[(&key, "alpha")]);
    let mut wrong = report("wrong", 30, &[(&key, "alpha")]);
    wrong.identity.target = Some("different-target".into());
    for r in [&old, &latest, &wrong] {
        crate::build_reports::upload_report(backend.as_ref(), "cache", r)
            .await
            .unwrap();
    }
    let corrupt = report("corrupt", 40, &[(&key, "alpha")]);
    backend
        .put(
            &crate::build_reports::object_key("cache", &corrupt).unwrap(),
            b"{".to_vec(),
            None,
        )
        .await
        .unwrap();
    let selected = select_report(
        backend.as_ref(),
        "cache",
        "fixture",
        &identity(),
        Instant::now() + Duration::from_secs(10),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(selected.session_id, "latest");
    let none = select_report(
        backend.as_ref(),
        "cache",
        "other",
        &identity(),
        Instant::now() + Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert!(none.is_none());
    let mut unknown = identity();
    unknown.build_shape = None;
    assert!(
        select_report(
            backend.as_ref(),
            "cache",
            "fixture",
            &unknown,
            Instant::now() + Duration::from_secs(10)
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(
        select_report(
            backend.as_ref(),
            "cache",
            "fixture",
            &identity(),
            Instant::now() - Duration::from_secs(1)
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn equal_finish_times_use_session_and_root_for_deterministic_selection() {
    let backend = memory_backend();
    let key = "d".repeat(64);
    let mut earlier_session = report("alpha", 20, &[(&key, "alpha")]);
    earlier_session.root_hash = "f".repeat(64);
    let mut earlier_root = report("zulu", 20, &[(&key, "alpha")]);
    earlier_root.root_hash = "1".repeat(64);
    let mut winner = earlier_root.clone();
    winner.root_hash = "a".repeat(64);
    for r in [&winner, &earlier_session, &earlier_root] {
        crate::build_reports::upload_report(&backend, "cache", r)
            .await
            .unwrap();
    }
    let selected = select_report(
        &backend,
        "cache",
        "fixture",
        &identity(),
        Instant::now() + Duration::from_secs(10),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(selected.session_id, "zulu");
    assert_eq!(selected.root_hash, "a".repeat(64));
}

#[tokio::test]
async fn total_body_budget_covers_streamed_and_buffered_gets_and_cannot_be_refunded_after_failure()
{
    let inner: Arc<dyn RemoteBackend> = Arc::new(memory_backend());
    inner.put("first", b"123".to_vec(), None).await.unwrap();
    inner.put("second", b"12".to_vec(), None).await.unwrap();
    let backend = BudgetedBackend::new(inner.clone(), 5);
    assert!(backend.head("first").await.unwrap());
    assert!(!backend.head("absent").await.unwrap());
    assert_eq!(backend.list("").await.unwrap().len(), 2);
    assert!(backend.describe("first").contains("first"));
    let mut sink = Vec::new();
    assert_eq!(
        backend
            .get_into("first", Some(20), &mut sink)
            .await
            .unwrap()
            .unwrap()
            .bytes,
        3
    );
    assert_eq!(sink, b"123");
    assert_eq!(backend.remaining.load(Ordering::Relaxed), 2);
    assert_eq!(
        backend
            .get("second", Some(20))
            .await
            .unwrap()
            .unwrap()
            .body
            .as_ref(),
        b"12"
    );
    assert_eq!(backend.remaining.load(Ordering::Relaxed), 0);
    assert!(backend.get("first", None).await.is_err());
    assert!(backend.put("other", vec![], None).await.is_err());

    let failed = BudgetedBackend::new(inner.clone(), 2);
    assert!(failed.get("first", None).await.is_err());
    assert_eq!(failed.remaining.load(Ordering::Relaxed), 0);
    let missing = BudgetedBackend::new(inner, 2);
    assert!(missing.get("absent", Some(1)).await.unwrap().is_none());
    assert_eq!(missing.remaining.load(Ordering::Relaxed), 2);
    assert!(missing.take(Some(0)).is_err());
    assert!(missing.refund(1, 2).is_err());
    assert_eq!(missing.remaining.load(Ordering::Relaxed), 2);
}

fn put(store: &Store, key: &str, name: &str, source: &Path) {
    store
        .put(
            key,
            name,
            &["lib".into()],
            &[],
            "target",
            "debug",
            &[(source.to_path_buf(), format!("lib{name}.rlib"))],
            "",
            "",
        )
        .unwrap();
}

#[tokio::test]
async fn bounded_replay_restores_packs_skips_local_and_locked_entries_and_summarizes_missing_packs()
{
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    std::fs::write(&source, b"compiled bytes").unwrap();
    let config = crate::test_support::test_config(dir.path().join("producer"));
    let producer = Store::open(&config).unwrap();
    let backend: Arc<dyn RemoteBackend> = Arc::new(memory_backend());
    let remote = crate::config::RemoteConfig::test_s3("bucket", "cache");
    let client = crate::cache_remote::V3Remote::new(backend.clone(), remote.clone());
    let keys = [
        "1".repeat(64),
        "2".repeat(64),
        "3".repeat(64),
        "4".repeat(64),
    ];
    for key in &keys[..2] {
        put(&producer, key, "alpha", &source);
        client
            .upload_entry(
                key,
                "alpha",
                &producer.entry_dir(key),
                &config.store_dir().join("blobs"),
                3,
                None,
            )
            .await
            .unwrap();
    }
    let consumer_config = crate::test_support::test_config(dir.path().join("consumer"));
    let consumer = Store::open(&consumer_config).unwrap();
    put(&consumer, &keys[1], "alpha", &source);
    let lock = consumer.try_lock(&keys[2]).unwrap().unwrap();
    let r = report(
        "session",
        10,
        &keys
            .iter()
            .map(|key| (key.as_str(), "alpha"))
            .collect::<Vec<_>>(),
    );
    let remaining = AtomicU64::new(1_000_000);
    let summary = replay(
        &client,
        &consumer_config,
        &r,
        10,
        Instant::now() + Duration::from_secs(10),
        &remaining,
    )
    .await
    .unwrap();
    assert_eq!(
        (
            summary.restored,
            summary.local,
            summary.busy,
            summary.failed
        ),
        (1, 1, 1, 1)
    );
    assert!(summary.downloaded_bytes > 0);
    assert!(consumer.contains(&keys[0]));
    assert!(consumer.contains(&keys[1]));
    assert!(!consumer.contains(&keys[2]));
    assert!(!consumer.contains(&keys[3]));
    drop(lock);

    let empty_config = crate::test_support::test_config(dir.path().join("empty"));
    let max_key = replay(
        &client,
        &empty_config,
        &r,
        1,
        Instant::now() + Duration::from_secs(10),
        &remaining,
    )
    .await
    .unwrap();
    assert_eq!((max_key.restored, max_key.omitted), (1, 3));
    assert!(max_key.budget_exhausted);
    let expired = replay(
        &client,
        &empty_config,
        &r,
        10,
        Instant::now() - Duration::from_secs(1),
        &remaining,
    )
    .await
    .unwrap();
    assert_eq!(
        (expired.local, expired.restored, expired.omitted),
        (1, 0, 3)
    );
    assert!(expired.budget_exhausted);
    let no_bytes = AtomicU64::new(0);
    let exhausted = replay(
        &client,
        &empty_config,
        &r,
        10,
        Instant::now() + Duration::from_secs(10),
        &no_bytes,
    )
    .await
    .unwrap();
    assert_eq!(
        (exhausted.local, exhausted.restored, exhausted.omitted),
        (1, 0, 3)
    );
    assert!(exhausted.budget_exhausted);
}

/// Exercise an independent publisher that does not participate in the replay
/// lock, as an older client or a restore path on another process may do.
type DownloadInterference = Box<dyn Fn(&Path) -> Result<()> + Send + Sync>;

struct InterferingRemote {
    inner: crate::cache_remote::V3Remote,
    after_download: DownloadInterference,
}

#[async_trait]
impl CacheRemote for InterferingRemote {
    async fn exists_entry(&self, key: &str, name: &str) -> Result<bool> {
        self.inner.exists_entry(key, name).await
    }

    async fn download_entry(
        &self,
        key: &str,
        name: &str,
        entry_dir: &Path,
        blobs_dir: &Path,
        deadline: Option<Instant>,
    ) -> Result<crate::remote::DownloadResult> {
        let result = self
            .inner
            .download_entry(key, name, entry_dir, blobs_dir, deadline)
            .await?;
        (self.after_download)(entry_dir)?;
        Ok(result)
    }

    async fn download_entry_observed(
        &self,
        key: &str,
        name: &str,
        entry_dir: &Path,
        blobs_dir: &Path,
        deadline: Option<Instant>,
        observer: &mut dyn crate::remote_layout::DownloadObserver,
    ) -> Result<crate::remote::DownloadResult> {
        self.inner
            .download_entry_observed(key, name, entry_dir, blobs_dir, deadline, observer)
            .await
    }

    async fn upload_entry(
        &self,
        key: &str,
        name: &str,
        entry_dir: &Path,
        blobs_dir: &Path,
        level: i32,
        deadline: Option<Instant>,
    ) -> Result<crate::remote_layout::RemoteUploadResult> {
        self.inner
            .upload_entry(key, name, entry_dir, blobs_dir, level, deadline)
            .await
    }

    async fn list_keys(&self) -> Result<std::collections::HashMap<String, String>> {
        self.inner.list_keys().await
    }

    async fn list_keys_observed(
        &self,
        observer: &mut dyn crate::remote_layout::ListObserver,
    ) -> Result<std::collections::HashMap<String, String>> {
        self.inner.list_keys_observed(observer).await
    }

    async fn list_keys_for_crates(
        &self,
        names: &HashSet<String>,
    ) -> Result<std::collections::HashMap<String, String>> {
        self.inner.list_keys_for_crates(names).await
    }
}

#[tokio::test]
async fn a_generation_published_during_download_is_preserved_as_busy() {
    let dir = tempfile::tempdir().unwrap();
    let incoming_source = dir.path().join("remote-source");
    let competing_source = dir.path().join("local-source");
    std::fs::write(&incoming_source, b"remote incoming generation").unwrap();
    let competing_bytes = b"competing locally committed generation";
    std::fs::write(&competing_source, competing_bytes).unwrap();
    let key = "e".repeat(64);
    let producer_config = crate::test_support::test_config(dir.path().join("producer"));
    let producer = Store::open(&producer_config).unwrap();
    put(&producer, &key, "alpha", &incoming_source);
    let backend: Arc<dyn RemoteBackend> = Arc::new(memory_backend());
    let remote = crate::config::RemoteConfig::test_s3("bucket", "cache");
    let inner = crate::cache_remote::V3Remote::new(backend, remote);
    inner
        .upload_entry(
            &key,
            "alpha",
            &producer.entry_dir(&key),
            &producer_config.store_dir().join("blobs"),
            3,
            None,
        )
        .await
        .unwrap();
    let consumer_config = crate::test_support::test_config(dir.path().join("consumer"));
    let published = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = published.clone();
    let competing_config = consumer_config.clone();
    let competing_key = key.clone();
    let client = InterferingRemote {
        inner,
        after_download: Box::new(move |_| {
            let competing = Store::open(&competing_config)?;
            assert!(!competing.entry_dir(&competing_key).exists());
            assert!(
                competing.try_lock(&competing_key)?.is_none(),
                "replay must own the key lock"
            );
            put(&competing, &competing_key, "alpha", &competing_source);
            assert!(competing.contains(&competing_key));
            observed.store(true, Ordering::SeqCst);
            Ok(())
        }),
    };
    let r = report("session", 10, &[(&key, "alpha")]);
    let remaining = AtomicU64::new(1_000_000);
    let summary = replay(
        &client,
        &consumer_config,
        &r,
        10,
        Instant::now() + Duration::from_secs(10),
        &remaining,
    )
    .await
    .unwrap();
    assert!(
        published.load(Ordering::SeqCst),
        "the competing publication must actually occur"
    );
    assert_eq!(
        (
            summary.restored,
            summary.local,
            summary.busy,
            summary.failed
        ),
        (0, 0, 1, 0)
    );
    assert!(
        summary.downloaded_bytes > 0,
        "the incoming pack must actually be downloaded"
    );
    let consumer = Store::open(&consumer_config).unwrap();
    assert!(consumer.contains(&key));
    let meta: crate::store::EntryMeta =
        serde_json::from_slice(&std::fs::read(consumer.entry_dir(&key).join("meta.json")).unwrap())
            .unwrap();
    assert_eq!(meta.cache_key, key);
    assert_eq!(meta.files.len(), 1);
    assert_eq!(
        meta.files[0].hash,
        blake3::hash(competing_bytes).to_hex().to_string()
    );
    assert_eq!(
        std::fs::read(consumer.blob_path(&meta.files[0].hash)).unwrap(),
        competing_bytes
    );
    assert!(
        !std::fs::read_dir(consumer_config.store_dir())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .any(|name| name.to_string_lossy().starts_with("warm-set-")),
        "discarded incoming staging directory must be removed"
    );
}

#[tokio::test]
async fn staged_files_disappearing_before_publish_or_import_are_failures() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    std::fs::write(&source, b"verified compiled bytes").unwrap();
    let candidate_hash = blake3::hash(b"verified compiled bytes")
        .to_hex()
        .to_string();
    let retained_source = dir.path().join("retained-source");
    let retained_bytes = b"existing independent compiled unit";
    std::fs::write(&retained_source, retained_bytes).unwrap();
    let retained_hash = blake3::hash(retained_bytes).to_hex().to_string();
    let producer_config = crate::test_support::test_config(dir.path().join("producer"));
    let producer = Store::open(&producer_config).unwrap();
    let key = "e".repeat(64);
    put(&producer, &key, "alpha", &source);
    let backend: Arc<dyn RemoteBackend> = Arc::new(memory_backend());
    let remote = crate::config::RemoteConfig::test_s3("bucket", "cache");
    let uploader = crate::cache_remote::V3Remote::new(backend.clone(), remote.clone());
    uploader
        .upload_entry(
            &key,
            "alpha",
            &producer.entry_dir(&key),
            &producer_config.store_dir().join("blobs"),
            3,
            None,
        )
        .await
        .unwrap();
    for missing in ["entry", "meta.json", "libalpha.rlib"] {
        let config =
            crate::test_support::test_config(dir.path().join(format!("consumer-{missing}")));
        let consumer = Store::open(&config).unwrap();
        let retained = "a".repeat(64);
        put(&consumer, &retained, "alpha", &retained_source);
        assert!(!consumer.blob_path(&candidate_hash).exists());
        let client = InterferingRemote {
            inner: crate::cache_remote::V3Remote::new(backend.clone(), remote.clone()),
            after_download: Box::new(move |entry| {
                if missing == "entry" {
                    assert!(entry.is_dir());
                    std::fs::remove_dir_all(entry)?;
                } else {
                    assert!(entry.join(missing).is_file());
                    std::fs::remove_file(entry.join(missing))?;
                }
                Ok(())
            }),
        };
        let r = report("session", 10, &[(&key, "alpha"), (&retained, "alpha")]);
        let summary = replay(
            &client,
            &config,
            &r,
            10,
            Instant::now() + Duration::from_secs(10),
            &AtomicU64::new(1_000_000),
        )
        .await
        .unwrap();
        assert_eq!(
            (
                summary.restored,
                summary.local,
                summary.busy,
                summary.failed
            ),
            (0, 1, 0, 1),
            "disappearing stage is a failure: {missing}"
        );
        assert!(summary.downloaded_bytes > 0);
        assert!(!consumer.contains(&key));
        assert!(
            !consumer.entry_dir(&key).exists(),
            "failed extraction must be discarded"
        );
        assert!(consumer.contains(&retained));
        assert!(consumer.entry_dir(&retained).join("meta.json").is_file());
        assert_eq!(
            std::fs::read(consumer.blob_path(&retained_hash)).unwrap(),
            retained_bytes
        );
        assert!(
            !std::fs::read_dir(config.store_dir())
                .unwrap()
                .any(|entry| entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("warm-set-"))
        );
    }
}

async fn assert_alternate_pack_is_rejected(pack_key: &str, pack_crate: &str) {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("alternate-source");
    std::fs::write(&source, b"compiled alternate unit").unwrap();
    let producer_config = crate::test_support::test_config(dir.path().join("producer"));
    let producer = Store::open(&producer_config).unwrap();
    put(&producer, pack_key, pack_crate, &source);
    let backend: Arc<dyn RemoteBackend> = Arc::new(memory_backend());
    let remote = crate::config::RemoteConfig::test_s3("bucket", "cache");
    let client = crate::cache_remote::V3Remote::new(backend.clone(), remote);
    client
        .upload_entry(
            pack_key,
            pack_crate,
            &producer.entry_dir(pack_key),
            &producer_config.store_dir().join("blobs"),
            3,
            None,
        )
        .await
        .unwrap();
    let requested = "a".repeat(64);
    let alternate = backend
        .get(
            &format!("cache/v3/packs/{pack_crate}/{pack_key}.tar.zst"),
            Some(1 << 20),
        )
        .await
        .unwrap()
        .unwrap()
        .body
        .to_vec();
    // Serve a valid, hash-verified alternate unit at the requested object key.
    backend
        .put(
            &format!("cache/v3/packs/alpha/{requested}.tar.zst"),
            alternate,
            None,
        )
        .await
        .unwrap();
    let config = crate::test_support::test_config(dir.path().join("consumer"));
    let consumer = Store::open(&config).unwrap();
    let retained = "c".repeat(64);
    put(&consumer, &retained, "alpha", &source);
    let retained_meta = std::fs::read(consumer.entry_dir(&retained).join("meta.json")).unwrap();
    assert_eq!(consumer.entry_count().unwrap(), 1);
    let r = report(
        "session",
        10,
        &[(&requested, "alpha"), (&retained, "alpha")],
    );
    let summary = replay(
        &client,
        &config,
        &r,
        10,
        Instant::now() + Duration::from_secs(10),
        &AtomicU64::new(1_000_000),
    )
    .await
    .unwrap();
    assert_eq!(
        (
            summary.restored,
            summary.local,
            summary.busy,
            summary.failed
        ),
        (0, 1, 0, 1),
        "alternate pack must not be imported: {pack_key}/{pack_crate}"
    );
    assert!(summary.downloaded_bytes > 0);
    assert!(!consumer.contains(&requested));
    assert!(!consumer.entry_dir(&requested).exists());
    assert_eq!(consumer.entry_count().unwrap(), 1, "no new index rows");
    assert!(consumer.contains(&retained));
    assert_eq!(
        std::fs::read(consumer.entry_dir(&retained).join("meta.json")).unwrap(),
        retained_meta
    );
    assert!(
        !std::fs::read_dir(config.store_dir())
            .unwrap()
            .any(|entry| entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("warm-set-"))
    );
}

#[tokio::test]
async fn replay_rejects_pack_with_different_cache_key() {
    assert_alternate_pack_is_rejected(&"b".repeat(64), "alpha").await;
}

#[tokio::test]
async fn replay_rejects_pack_with_different_crate_name() {
    assert_alternate_pack_is_rejected(&"a".repeat(64), "beta").await;
}

#[test]
fn staged_binding_requires_readable_metadata_within_the_exact_size_limit() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    std::fs::write(&source, b"compiled unit").unwrap();
    let config = crate::test_support::test_config(dir.path().join("store"));
    let store = Store::open(&config).unwrap();
    let key = "a".repeat(64);
    put(&store, &key, "alpha", &source);
    let incoming = store.entry_dir(&key);
    let path = incoming.join("meta.json");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.resize(8_388_608, b' ');
    std::fs::write(&path, &bytes).unwrap();
    validate_staged_binding(&incoming, &key, "alpha").unwrap();
    bytes.push(b' ');
    std::fs::write(&path, bytes).unwrap();
    assert!(
        validate_staged_binding(&incoming, &key, "alpha")
            .unwrap_err()
            .to_string()
            .contains("size limit")
    );
    std::fs::write(&path, b"{").unwrap();
    assert!(validate_staged_binding(&incoming, &key, "alpha").is_err());
    std::fs::remove_file(&path).unwrap();
    assert!(validate_staged_binding(&incoming, &key, "alpha").is_err());
}

#[test]
fn publication_preserves_an_existing_directory_and_reports_other_filesystem_errors() {
    let dir = tempfile::tempdir().unwrap();
    let incoming = dir.path().join("incoming");
    let destination = dir.path().join("destination");
    std::fs::create_dir(&incoming).unwrap();
    std::fs::write(incoming.join("artifact"), b"incoming bytes").unwrap();
    std::fs::create_dir(&destination).unwrap();
    std::fs::write(destination.join("artifact"), b"existing bytes").unwrap();
    assert!(!publish_staged_entry(&incoming, &destination).unwrap());
    assert_eq!(
        std::fs::read(destination.join("artifact")).unwrap(),
        b"existing bytes"
    );
    assert_eq!(
        std::fs::read(incoming.join("artifact")).unwrap(),
        b"incoming bytes"
    );
    let missing = dir.path().join("missing");
    assert!(publish_staged_entry(&missing, &dir.path().join("absent")).is_err());
    let fresh = dir.path().join("fresh");
    assert!(publish_staged_entry(&incoming, &fresh).unwrap());
    assert_eq!(
        std::fs::read(fresh.join("artifact")).unwrap(),
        b"incoming bytes"
    );
    assert!(!incoming.exists());
}
