//! A rejected host hit must become a reusable entry in the job's own store.
use std::path::{Path, PathBuf};

mod common;
use common::{build_kache, hermetic_command, isolated_config_path, kache_binary, settle_writes};

fn write_config(cache: &Path, readonly: Option<&Path>) -> PathBuf {
    std::fs::create_dir_all(cache).unwrap();
    let path = isolated_config_path(cache);
    let quote = |path: &Path| toml::Value::String(path.to_string_lossy().into_owned());
    let fallback = readonly
        .map(|dir| format!("readonly_store = {}\n", quote(dir)))
        .unwrap_or_default();
    std::fs::write(
        &path,
        format!(
            "[cache]\nlocal_only = true\nignore_env = true\ninput_predictions = false\nlocal_store = {}\nruntime_dir = {}\n{fallback}",
            quote(cache), quote(cache),
        ),
    ).unwrap();
    path
}

fn compile(cache: &Path, config: &Path, source: &Path, out: &Path) {
    std::fs::create_dir_all(out).unwrap();
    settle_writes(&[source.parent().expect("a source in a directory")]);
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".into());
    let output = hermetic_command(kache_binary(), cache, Some(config))
        .args([
            rustc.as_str(),
            "--crate-name",
            "readonly_probe",
            "--crate-type",
            "lib",
            "--edition",
            "2021",
            "--emit=dep-info,metadata,link",
            "--out-dir",
            out.to_str().unwrap(),
            source.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn last_event(cache: &Path) -> serde_json::Value {
    last_event_for(cache, "readonly_probe")
}

fn last_event_for(cache: &Path, crate_name: &str) -> serde_json::Value {
    std::fs::read_to_string(cache.join("events.jsonl"))
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .rfind(|event| event["crate_name"] == crate_name)
        .expect("a compiler event")
}

#[cfg(unix)]
fn compile_cc(work: &Path, cache: &Path, config: &Path) {
    let prefix_map = format!("-ffile-prefix-map={}=/readonly-probe", work.display());
    settle_writes(&[work]);
    let output = hermetic_command(kache_binary(), cache, Some(config))
        .current_dir(work)
        .args([
            "cc",
            "-c",
            "readonly_probe.c",
            "-o",
            "readonly_probe.o",
            &prefix_map,
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn rejected_readonly_hits_are_recompiled_and_cached_locally() {
    build_kache();
    for rejection in ["coverage", "missing-dependency", "malformed-metadata"] {
        let root = tempfile::tempdir().unwrap();
        let host = root.path().join("host-cache");
        let job = root.path().join("job-cache");
        let source = root.path().join("lib.rs");
        std::fs::write(&source, "pub fn answer() -> u32 { 42 }\n").unwrap();
        let host_config = write_config(&host, None);
        compile(&host, &host_config, &source, &root.path().join("host-out"));
        let host_event = last_event(&host);
        assert_eq!(host_event["result"], "miss", "{host_event}");
        let key = host_event["cache_key"].as_str().unwrap();
        let metadata = host.join("store").join(key).join("meta.json");
        let mut meta: kache_format::EntryMeta =
            serde_json::from_slice(&std::fs::read(&metadata).unwrap()).unwrap();
        match rejection {
            "coverage" => meta.emit_kinds = vec!["metadata".into()],
            "missing-dependency" => {
                let dep = meta
                    .files
                    .iter_mut()
                    .find(|file| file.name.ends_with(".d"))
                    .unwrap();
                let content = format!(
                    "readonly_probe: {}\n",
                    root.path().join("missing.rs").display()
                );
                dep.hash = blake3::hash(content.as_bytes()).to_hex().to_string();
                dep.size = content.len() as u64;
                let blob = kache_store::blob_path_in_store_dir(&host.join("store"), &dep.hash);
                std::fs::create_dir_all(blob.parent().unwrap()).unwrap();
                std::fs::write(blob, content).unwrap();
            }
            _ => {}
        }
        if rejection == "malformed-metadata" {
            std::fs::write(&metadata, "{broken JSON").unwrap();
        } else {
            std::fs::write(&metadata, serde_json::to_vec(&meta).unwrap()).unwrap();
        }
        let original_metadata = std::fs::read(&metadata).unwrap();
        // Keep the WAL side files alive while the job opens its reader.
        let owner = rusqlite::Connection::open(host.join("index.db")).unwrap();
        let entries: i64 = owner
            .query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0))
            .unwrap();
        assert_eq!(entries, 1);
        assert!(host.join("index.db-wal").exists());
        assert!(host.join("index.db-shm").exists());
        let job_config = write_config(&job, Some(&host));
        let out = root.path().join("job-out");
        compile(&job, &job_config, &source, &out);
        let cold = last_event(&job);
        assert_eq!(
            cold["cache_key"], key,
            "the host entry must have been considered"
        );
        assert_eq!(cold["result"], "miss", "{rejection}: {cold}");
        assert!(job.join("store").join(key).join("meta.json").is_file());
        std::fs::remove_dir_all(&out).unwrap();
        compile(&job, &job_config, &source, &out);
        let warm = last_event(&job);
        assert_eq!(warm["result"], "local_hit", "{rejection}: {warm}");
        assert_eq!(std::fs::read(&metadata).unwrap(), original_metadata);
        drop(owner);
    }
}

#[cfg(unix)]
#[test]
fn empty_job_store_uses_the_readonly_cc_hit_before_compiling() {
    build_kache();
    let root = tempfile::tempdir().unwrap();
    let work = root.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    std::fs::write(
        work.join("readonly_probe.c"),
        "int answer(void) { return 42; }\n",
    )
    .unwrap();
    let host = root.path().join("host-cache");
    let job = root.path().join("job-cache");
    let host_config = write_config(&host, None);
    compile_cc(&work, &host, &host_config);
    let cold = last_event_for(&host, "readonly_probe.c");
    assert_eq!(cold["result"], "miss", "{cold}");
    assert_eq!(
        cold["preprocessor_runs"], 0,
        "the host must exercise compile-before-key; otherwise this test cannot catch the gate regression: {cold}"
    );
    assert_eq!(cold["compiler_runs"], 1, "{cold}");
    let expected_object = std::fs::read(work.join("readonly_probe.o")).unwrap();
    std::fs::remove_file(work.join("readonly_probe.o")).unwrap();
    let owner = rusqlite::Connection::open(host.join("index.db")).unwrap();
    let entries: i64 = owner
        .query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0))
        .unwrap();
    assert_eq!(entries, 1);
    assert!(host.join("index.db-wal").exists());
    assert!(host.join("index.db-shm").exists());
    let job_config = write_config(&job, Some(&host));
    compile_cc(&work, &job, &job_config);
    let hit = last_event_for(&job, "readonly_probe.c");
    assert_eq!(hit["cache_key"], cold["cache_key"]);
    assert_eq!(hit["result"], "local_hit", "{hit}");
    assert_eq!(hit["compiler_runs"], 0, "{hit}");
    assert_eq!(
        std::fs::read(work.join("readonly_probe.o")).unwrap(),
        expected_object
    );
    drop(owner);
}

#[cfg(unix)]
#[test]
fn failed_readonly_cc_restore_is_cached_locally_and_writable_failure_uses_passthrough() {
    build_kache();
    for read_only in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let work = root.path().join("work");
        std::fs::create_dir_all(&work).unwrap();
        std::fs::write(
            work.join("readonly_probe.c"),
            "int answer(void) { return 42; }\n",
        )
        .unwrap();
        let host = root.path().join("host-cache");
        let host_config = write_config(&host, None);
        compile_cc(&work, &host, &host_config);
        let seed = last_event_for(&host, "readonly_probe.c");
        assert_eq!(seed["result"], "miss", "{seed}");
        let key = seed["cache_key"].as_str().unwrap();
        let metadata = host.join("store").join(key).join("meta.json");
        let mut meta: kache_format::EntryMeta =
            serde_json::from_slice(&std::fs::read(&metadata).unwrap()).unwrap();
        let mut duplicate = meta
            .files
            .iter()
            .find(|file| file.name.ends_with(".o"))
            .expect("the cold compile must store an object")
            .clone();
        duplicate.name = "duplicate-readonly-probe.o".into();
        meta.files.push(duplicate);
        // Both metadata records describe a valid blob, so lookup accepts the
        // entry. Restore maps both objects to the same output and rejects it
        // before publication. This needs no race or injected I/O failure.
        let broken_metadata = serde_json::to_vec(&meta).unwrap();
        std::fs::write(&metadata, &broken_metadata).unwrap();
        let object = work.join("readonly_probe.o");
        std::fs::remove_file(&object).unwrap();
        let owner = rusqlite::Connection::open(host.join("index.db")).unwrap();
        let entries: i64 = owner
            .query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0))
            .unwrap();
        assert_eq!(entries, 1);
        let cache = if read_only {
            root.path().join("job-cache")
        } else {
            host.clone()
        };
        let config = if read_only {
            write_config(&cache, Some(&host))
        } else {
            // Force a lookup even if this driver's initial compile did not
            // publish a preprocess memo; the case tests restore rejection.
            let original = std::fs::read_to_string(&host_config).unwrap();
            std::fs::write(
                &host_config,
                format!("{original}deferred_discovery = false\n"),
            )
            .unwrap();
            host_config
        };
        compile_cc(&work, &cache, &config);
        let first = last_event_for(&cache, "readonly_probe.c");
        assert_eq!(std::fs::read(&metadata).unwrap(), broken_metadata);
        assert!(object.is_file(), "restore rejection must still compile");
        if read_only {
            assert_eq!(first["cache_key"], key, "{first}");
            assert_eq!(first["result"], "miss", "{first}");
            assert_eq!(first["compiler_runs"], 1, "{first}");
            assert!(
                first["lookup_rejection"]
                    .as_str()
                    .unwrap()
                    .contains("maps multiple artifacts"),
                "the host lookup must reach the restore rejection: {first}"
            );
            let repaired: kache_format::EntryMeta = serde_json::from_slice(
                &std::fs::read(cache.join("store").join(key).join("meta.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(
                repaired
                    .files
                    .iter()
                    .filter(|file| file.name.ends_with(".o"))
                    .count(),
                1,
                "the job must store its own usable entry"
            );
        } else {
            assert_eq!(first["result"], "passthrough", "{first}");
            assert!(
                first["passthrough_reason"]
                    .as_str()
                    .unwrap()
                    .contains("maps multiple artifacts"),
                "the writable entry must reach the same restore rejection: {first}"
            );
        }
        std::fs::remove_file(&object).unwrap();
        compile_cc(&work, &cache, &config);
        let second = last_event_for(&cache, "readonly_probe.c");
        if read_only {
            assert_eq!(second["result"], "local_hit", "{second}");
            assert_eq!(second["compiler_runs"], 0, "{second}");
        } else {
            assert_eq!(second["result"], "passthrough", "{second}");
        }
        assert!(object.is_file());
        assert_eq!(std::fs::read(&metadata).unwrap(), broken_metadata);
        drop(owner);
    }
}
