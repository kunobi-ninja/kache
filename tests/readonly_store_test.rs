//! A rejected host hit must become a reusable entry in the job's own store.
use std::path::{Path, PathBuf};

mod common;
use common::{build_kache, hermetic_command, kache_binary};

fn write_config(cache: &Path, readonly: Option<&Path>) -> PathBuf {
    std::fs::create_dir_all(cache).unwrap();
    let path = cache.join("config.toml");
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
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".into());
    let output = hermetic_command(kache_binary(), cache, Some(config))
        .args([
            rustc.as_str(), "--crate-name", "readonly_probe", "--crate-type", "lib",
            "--edition", "2021", "--emit=dep-info,metadata,link", "--out-dir",
            out.to_str().unwrap(), source.to_str().unwrap(),
        ])
        .output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
}

fn last_event(cache: &Path) -> serde_json::Value {
    std::fs::read_to_string(cache.join("events.jsonl")).unwrap().lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .rfind(|event| event["crate_name"] == "readonly_probe")
        .expect("a compiler event")
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
                let dep = meta.files.iter_mut().find(|file| file.name.ends_with(".d")).unwrap();
                let content = format!("readonly_probe: {}\n", root.path().join("missing.rs").display());
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
        let entries: usize = owner.query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0)).unwrap();
        assert_eq!(entries, 1);
        assert!(host.join("index.db-wal").exists());
        assert!(host.join("index.db-shm").exists());
        let job_config = write_config(&job, Some(&host));
        let out = root.path().join("job-out");
        compile(&job, &job_config, &source, &out);
        let cold = last_event(&job);
        assert_eq!(cold["cache_key"], key, "the host entry must have been considered");
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
