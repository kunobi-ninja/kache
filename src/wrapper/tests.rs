mod hit;
mod rustc_hit;

use super::*;
use crate::cache_key::FileHasher;
use crate::transport::{ListenerOptions, socket_name};
use std::ffi::OsString;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct TestEnvGuard {
    key: &'static str,
    previous: Option<OsString>,
}

impl TestEnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var_os(key);
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, previous }
    }

    fn remove(key: &'static str) -> Self {
        let previous = std::env::var_os(key);
        unsafe {
            std::env::remove_var(key);
        }
        Self { key, previous }
    }
}

impl Drop for TestEnvGuard {
    fn drop(&mut self) {
        unsafe {
            match &self.previous {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }
}

fn s(args: &[&str]) -> Vec<String> {
    args.iter().map(|arg| (*arg).to_string()).collect()
}

fn rustc_args(args: &[&str]) -> RustcArgs {
    RustcCompiler::new().parse(&s(args)).unwrap()
}

/// The rule that keeps a prediction from ever being an authority.
///
/// Reached only once both lookups have missed, so it is not asking "did we
/// find it" — a hit has already returned. It asks whether this key is
/// still a guess, and a guess may not claim or store.
#[test]
fn a_predicted_key_owes_a_rederivation_until_it_has_had_one() {
    assert!(
        owes_rederivation(true, false),
        "a key that came from a record and matched nothing is still a guess"
    );
    assert!(
        !owes_rederivation(true, true),
        "one re-derivation is enough; a second would be the same pre-pass"
    );
    assert!(
        !owes_rederivation(false, false),
        "a key that was never predicted is already the discovered one"
    );
    assert!(!owes_rederivation(false, true));
}

#[test]
fn only_a_fresh_closure_is_worth_recording() {
    assert!(
        should_record_closure(false, false),
        "a closure discovered by the pre-pass is what the record is for"
    );
    assert!(
        !should_record_closure(true, false),
        "a closure that CAME from the record is already in it; rewriting \
             it would be a database write per compile for nothing"
    );
    assert!(
        should_record_closure(true, true),
        "a re-derivation is what repairs a stale row"
    );
    assert!(should_record_closure(false, true));
}

/// Recording is opt-in and needs somewhere to record. Neither refusal may
/// leave the discovered closure sitting in the thread-local stash, where
/// the next key computed on this thread would find it and record another
/// unit's inputs under its own identity.
#[test]
fn input_predictions_record_only_when_enabled_and_backed_by_a_store() {
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let args = rustc_args(&["rustc", "src/lib.rs", "--crate-name", "demo"]);
    let closure = crate::cache_key::DepInfo {
        source_files: vec![std::path::PathBuf::from("src/lib.rs")],
        env_deps: Vec::new(),
    };
    let stash = || crate::cache_key::stash_last_dep_info_for_test(closure.clone());
    let identity = crate::cache_key::rustc_prediction_identity(&args)
        .expect("an invocation with a crate root has an identity");
    let recorded = |store: &Store| store.file_hasher().input_prediction(&identity).is_some();

    // Off by default, which is the state every user is in today.
    assert!(!config.input_predictions);
    stash();
    record_input_prediction(&config, Some(&store), &args, true);
    assert!(
        !recorded(&store),
        "the feature is off; nothing may be written"
    );
    assert!(
        crate::cache_key::take_last_dep_info().is_none(),
        "a declined recording must still clear the stash"
    );

    // On, but with no store to record into: the daemon's store-free path.
    config.input_predictions = true;
    stash();
    record_input_prediction(&config, None, &args, true);
    assert!(!recorded(&store));
    assert!(crate::cache_key::take_last_dep_info().is_none());

    // On, with a store, and a closure to record.
    stash();
    record_input_prediction(&config, Some(&store), &args, true);
    assert!(
        recorded(&store),
        "an enabled build with a store must remember what it discovered"
    );
    let record = store.file_hasher().input_prediction(&identity).unwrap();
    assert_eq!(record.sources, closure.source_files);

    // And with nothing in the stash there is nothing to record: an
    // invocation that never ran a pre-pass must not write an empty closure
    // over a good one.
    record_input_prediction(&config, Some(&store), &args, true);
    assert_eq!(
        store
            .file_hasher()
            .input_prediction(&identity)
            .unwrap()
            .sources,
        closure.source_files,
        "a recording with no closure must leave the existing one alone"
    );
}

/// A registry unit whose closure reads its own OUT_DIR cannot have the
/// shared row, so it gets the relocated one; without such a source it
/// gets the shared row.
#[test]
fn a_registry_unit_reading_its_out_dir_records_a_relocated_row() {
    if std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .is_err()
    {
        eprintln!("skipped: no rustc");
        return;
    }
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().join("cache"));
    config.input_predictions = true;
    let store = Store::open(&config).unwrap();
    let package = dir.path().join("home/registry/src/index-1/kt-1.0.0");
    std::fs::create_dir_all(package.join("src")).unwrap();
    let target = dir.path().join("a/target");
    let out = target.join("debug/build/kt-1/out");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::write(out.join("gen.rs"), "pub fn g() {}\n").unwrap();
    let _manifest =
        crate::config::tests::set_env_for_test("CARGO_MANIFEST_DIR", Some(package.as_os_str()));
    let _out = crate::config::tests::set_env_for_test("OUT_DIR", Some(out.as_os_str()));
    let lib = package.join("src/lib.rs");
    let args = rustc_args(&[
        "rustc",
        "--crate-name",
        "kt",
        lib.to_str().unwrap(),
        "--out-dir",
        target.join("debug/deps").to_str().unwrap(),
    ]);
    let shared = crate::cache_key::rustc_shared_prediction_identity(&args)
        .expect("a registry unit with an absolute target has a shared identity");
    let reads_out_dir = crate::cache_key::DepInfo {
        source_files: vec![lib.clone(), out.join("gen.rs")],
        env_deps: Vec::new(),
    };
    let (relocatable, _) =
        crate::cache_key::relocatable_record(&args, &reads_out_dir, Some("tree")).unwrap();

    crate::cache_key::stash_last_dep_info_for_test(reads_out_dir);
    crate::cache_key::stash_last_tree_digest_for_test("tree");
    record_input_prediction(&config, Some(&store), &args, true);
    let hasher = store.file_hasher();
    assert!(hasher.input_prediction(&shared).is_none());
    let record = hasher.portable_prediction(&relocatable).unwrap();
    assert_eq!(record.tree, "tree");

    let package_only = crate::cache_key::DepInfo {
        source_files: vec![lib],
        env_deps: Vec::new(),
    };
    crate::cache_key::stash_last_dep_info_for_test(package_only.clone());
    crate::cache_key::stash_last_tree_digest_for_test("tree-2");
    record_input_prediction(&config, Some(&store), &args, true);
    assert_eq!(
        hasher.input_prediction(&shared).unwrap().sources,
        package_only.source_files
    );
}

fn eligible_incremental_args(temp: &tempfile::TempDir, crate_name: &str) -> RustcArgs {
    let profile = temp.path().join("target/debug");
    let out_dir = profile.join("deps");
    let incremental = profile.join("incremental");
    std::fs::create_dir_all(&out_dir).unwrap();
    std::fs::create_dir_all(&incremental).unwrap();
    RustcCompiler::new()
        .parse(&[
            "rustc".to_string(),
            "--crate-name".to_string(),
            crate_name.to_string(),
            temp.path()
                .join("src/lib.rs")
                .to_string_lossy()
                .into_owned(),
            "--out-dir".to_string(),
            out_dir.to_string_lossy().into_owned(),
            "-C".to_string(),
            format!("incremental={}", incremental.display()),
            "-Cextra-filename=-1234abcd".to_string(),
        ])
        .unwrap()
}

#[test]
fn store_unavailable_message_is_actionable() {
    let err = anyhow::anyhow!("disk I/O error")
        .context("opening index database /mnt/c/Users/x/kache/index.db");
    let msg = store_unavailable_message(&err);

    // Surfaces the real underlying error so users can diagnose.
    assert!(msg.contains("disk I/O error"), "msg = {msg}");
    // States the impact plainly.
    assert!(
        msg.to_lowercase().contains("caching is disabled"),
        "msg = {msg}"
    );
    // Points at the general cause (locking / multi-machine) — not anything
    // specific to containers/cross/podman.
    assert!(
        msg.contains("locking") || msg.contains("more than one machine"),
        "msg = {msg}"
    );
    // Gives the actionable remediation.
    assert!(msg.contains("KACHE_CACHE_DIR"), "msg = {msg}");
    // Reassures the build still succeeds.
    assert!(
        msg.contains("uncached") || msg.contains("succeeds"),
        "msg = {msg}"
    );
    // Stays generic: must NOT name the specific reporter's environment.
    assert!(!msg.to_lowercase().contains("podman"), "msg = {msg}");
    assert!(!msg.to_lowercase().contains("container"), "msg = {msg}");
}

#[test]
fn store_warn_marker_is_local_and_keyed_by_cache_dir() {
    let a = warn_marker_path("store", Path::new("/mnt/c/Users/x/kache"));
    let b = warn_marker_path("store", Path::new("/home/y/.cache/kache"));
    let tmp = std::env::temp_dir();

    // Lives in the OS temp dir (local), NOT under the (possibly broken)
    // cache dir — the whole point is that the cache mount can't be relied
    // on for locking, so the dedup marker must not live there.
    assert!(a.starts_with(&tmp), "marker {a:?} not under temp {tmp:?}");
    assert!(
        !a.starts_with("/mnt/c"),
        "marker must not live on the cache mount: {a:?}"
    );
    // Distinct cache dirs get distinct markers (independent dedup).
    assert_ne!(a, b);
    // Same cache dir is stable across calls, so the 300+ parallel wrapper
    // processes all agree on one marker and only one of them warns.
    assert_eq!(
        a,
        warn_marker_path("store", Path::new("/mnt/c/Users/x/kache"))
    );
}

#[test]
fn store_unavailable_warning_dedups_within_session() {
    // Unique synthetic cache dir so this test's marker can't collide with
    // other tests running in the same binary. The dir need not exist — the
    // warning only ever touches the local marker, never the cache dir.
    let cache_dir = PathBuf::from("/nonexistent/kache-469-dedup-test-cache-dir-7f3a2b1c");
    let marker = warn_marker_path("store", &cache_dir);
    let _ = std::fs::remove_file(&marker);
    assert!(
        !marker_is_fresh(&marker, 300),
        "precondition: no marker yet"
    );

    let cfg = test_config(cache_dir);
    let err = anyhow::anyhow!("disk I/O error");

    warn_store_unavailable_once(&cfg, &err);

    // After the first warning the marker is fresh, so the remaining parallel
    // wrappers in the same session stay silent.
    assert!(
        marker_is_fresh(&marker, 300),
        "marker should be fresh after the first warning"
    );

    let _ = std::fs::remove_file(&marker);
}

#[test]
#[cfg(unix)]
fn maybe_trigger_prefetch_refuses_symlinked_build_session() {
    let temp = tempfile::TempDir::new().unwrap();
    let target = temp.path().join("target_file");
    std::fs::write(&target, "target content").unwrap();

    let marker = temp.path().join(".build-session");
    std::os::unix::fs::symlink(&target, &marker).unwrap();

    let mut config = test_config(temp.path().to_path_buf());
    // Enable remote so prefetch actually triggers its path
    config.remote = Some(crate::config::RemoteConfig::test_s3(
        "test-bucket",
        "kache/",
    ));

    // Use dummy args
    let args = rustc_args(&["rustc", "foo.rs"]);

    // This must NOT modify the target file
    super::maybe_trigger_prefetch(&config, &args);

    // Verify target file remains completely untouched
    let content = std::fs::read_to_string(&target).unwrap();
    assert_eq!(content, "target content");
}

// ── Opportunistic size-pressure GC (kunobi-ninja/kache#497) ─────────────

#[test]
fn volume_route_path_rustc_prefers_out_dir_then_output_parent() {
    let mut args = rustc_args(&["rustc", "foo.rs"]);
    args.out_dir = Some(PathBuf::from("/mnt/biglake/target/debug"));
    args.output = Some(PathBuf::from("/other/libfoo.rlib"));
    assert_eq!(
        super::volume_route_path_rustc(&args),
        PathBuf::from("/mnt/biglake/target/debug")
    );
    args.out_dir = None;
    assert_eq!(
        super::volume_route_path_rustc(&args),
        PathBuf::from("/other")
    );
    args.output = Some(PathBuf::from("libfoo.rlib"));
    assert_eq!(
        super::volume_route_path_rustc(&args),
        PathBuf::from("libfoo.rlib")
    );
}

#[test]
fn volume_route_path_cc_uses_output_parent() {
    let mut parsed = crate::compiler::cc::CcArgs {
        program: "cc".into(),
        rest: Vec::new(),
        sources: vec![PathBuf::from("a.c")],
        output: Some(PathBuf::from("/mnt/biglake/build/a.o")),
        mode: crate::compiler::cc::CompileMode::Compile,
        includes: Vec::new(),
        defines: Vec::new(),
        optimization: None,
        debug_level: None,
        std: None,
        pic: false,
        depinfo: None,
        language_override: None,
        family: crate::compiler::cc::ToolFamily::Gnu,
    };
    assert_eq!(
        super::volume_route_path_cc(&parsed),
        PathBuf::from("/mnt/biglake/build")
    );
    parsed.output = Some(PathBuf::from("a.o"));
    assert_eq!(super::volume_route_path_cc(&parsed), PathBuf::from("a.o"));
}

#[test]
fn volume_cache_dirs_match_is_path_equality() {
    assert!(super::volume_cache_dirs_match(
        Path::new("/cache/main"),
        Path::new("/cache/main")
    ));
    assert!(!super::volume_cache_dirs_match(
        Path::new("/cache/shard"),
        Path::new("/cache/main")
    ));
}

#[test]
fn open_primary_and_fallback_skips_fallback_when_unmapped() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path().to_path_buf());
    let (_, fallback) =
        super::open_primary_and_fallback(&cfg, Path::new("/unmapped/out.rlib")).unwrap();
    assert!(
        fallback.is_none(),
        "the main store must not open itself as a fallback"
    );
}

#[test]
fn lookup_local_entry_prefers_primary_then_falls_back() {
    let dir = tempfile::tempdir().unwrap();
    let primary_cfg = test_config(dir.path().join("primary"));
    let main_cfg = test_config(dir.path().join("main"));
    let primary = Store::open(&primary_cfg).unwrap();
    let fallback = Store::open(&main_cfg).unwrap();
    put_test_entry(&fallback, dir.path(), "vol-fallback-key");
    let miss = super::lookup_local_entry(&primary, Some(&fallback), "no-such-key").unwrap();
    assert!(miss.is_none());
    let hit = super::lookup_local_entry(&primary, Some(&fallback), "vol-fallback-key")
        .unwrap()
        .expect("fallback must serve a key the shard does not have");
    assert_eq!(hit.1.crate_name, "test-crate");
    put_test_entry(&primary, dir.path(), "vol-primary-key");
    let primary_hit =
        super::lookup_local_entry(&primary, Some(&fallback), "vol-primary-key").unwrap();
    assert!(primary_hit.is_some());
}

/// Store a small entry so the store has a nonzero size.
fn put_test_entry(store: &Store, dir: &std::path::Path, key: &str) {
    let src = dir.join(format!("{key}.o"));
    std::fs::write(&src, vec![0xABu8; 4096]).unwrap();
    store
        .put(
            key,
            "test-crate",
            &[],
            &[],
            "host",
            "dev",
            &[(src.clone(), format!("{key}.o"))],
            "",
            "",
        )
        .unwrap();
    // The store may have cloned it: left in place, the output would hold
    // the entry's blocks the way a target directory does.
    store.remove_clone_for_test(&src);
}

#[test]
fn auto_gc_wanted_fires_only_over_budget_and_respects_throttle() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dir.path().to_path_buf());
    let store = Store::open(&cfg).unwrap();
    put_test_entry(&store, dir.path(), "auto-gc-key-1");

    // Under budget (max_size = 1 MiB, entry = 4 KiB): no GC wanted,
    // but the stamp is written to throttle future check intervals.
    assert!(
        !auto_gc_wanted(&cfg, &store),
        "under budget must not trigger"
    );
    let stamp = auto_gc_stamp_path(&cfg.cache_dir);
    assert!(stamp.exists(), "under budget must create the stamp");

    // Over budget but with a fresh stamp: check is throttled.
    cfg.max_size = 1024; // 1 KiB budget, store holds 4 KiB (> +10% slack)
    assert!(
        !auto_gc_wanted(&cfg, &store),
        "fresh stamp must throttle the check even if over budget"
    );

    // Age the stamp past the interval → over-budget check now fires.
    let old = std::time::SystemTime::now() - (AUTO_GC_CHECK_INTERVAL * 2);
    let stamp_file = std::fs::OpenOptions::new()
        .write(true)
        .open(&stamp)
        .unwrap();
    stamp_file.set_modified(old).unwrap();
    drop(stamp_file);
    assert!(
        auto_gc_wanted(&cfg, &store),
        "over budget with an expired stamp must trigger"
    );
    // ... and the successful check re-stamps, throttling the next one.
    assert!(
        !auto_gc_wanted(&cfg, &store),
        "the triggering check must re-claim the stamp"
    );
}

#[test]
fn auto_gc_wanted_respects_disable_and_slack() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dir.path().to_path_buf());
    let store = Store::open(&cfg).unwrap();
    put_test_entry(&store, dir.path(), "auto-gc-key-2");

    // Disabled: never triggers, regardless of size.
    cfg.auto_gc = false;
    cfg.max_size = 1;
    assert!(!auto_gc_wanted(&cfg, &store), "auto_gc=false must disable");

    // Enabled but within the +10% slack band: no trigger. The store holds
    // exactly 4096 bytes; max_size 4000 → threshold 4400 ≥ 4096.
    cfg.auto_gc = true;
    cfg.max_size = 4000;
    assert!(
        !auto_gc_wanted(&cfg, &store),
        "inside the slack band must not trigger"
    );
}

/// Age the auto-GC throttle stamp past the check interval.
fn expire_auto_gc_stamp(cfg: &Config) {
    let stamp = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(auto_gc_stamp_path(&cfg.cache_dir))
        .unwrap();
    stamp
        .set_modified(std::time::SystemTime::now() - AUTO_GC_CHECK_INTERVAL * 2)
        .unwrap();
}

/// Store an idle entry whose blob a target directory still hardlinks:
/// eviction reaches it and cannot free it.
fn put_retained_entry(store: &Store, dir: &std::path::Path, key: &str) {
    let src = dir.join(format!("{key}.o"));
    std::fs::write(&src, &key.as_bytes().repeat(4096)[..4096]).unwrap();
    store
        .put(
            key,
            "test-crate",
            &[],
            &[],
            "host",
            "dev",
            &[(src, format!("{key}.o"))],
            "",
            "",
        )
        .unwrap();
    let meta = store.get(key).unwrap().unwrap();
    std::fs::hard_link(
        store.blob_path(&meta.files[0].hash),
        dir.join(format!("{key}-target.o")),
    )
    .unwrap();
    store.set_last_accessed_for_test(key, "-1 hour");
}

/// An entry a build is using right now: no sweep may evict it, and no
/// target directory holds its blocks.
fn put_in_use_entry(store: &Store, dir: &std::path::Path, key: &str) {
    let src = dir.join(format!("{key}.o"));
    std::fs::write(&src, &key.as_bytes().repeat(4096)[..4096]).unwrap();
    store
        .put(
            key,
            "test-crate",
            &[],
            &[],
            "host",
            "dev",
            &[(src.clone(), format!("{key}.o"))],
            "",
            "",
        )
        .unwrap();
    store.remove_clone_for_test(&src);
    store.set_last_accessed_for_test(key, "+0 seconds");
}

/// The production thrash: a store over budget on bytes target
/// directories hold. Every sweep freed nothing and the next check, five
/// minutes later, spawned another one around the clock, evicting
/// whatever else it could each time (#1206). The sweep now records what
/// it found held and the trigger leaves it out until the figure expires.
#[test]
fn auto_gc_leaves_bytes_target_directories_hold_out_of_the_trigger() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dir.path().to_path_buf());
    cfg.max_size = 1024;
    let store = Store::open(&cfg).unwrap();
    put_retained_entry(&store, dir.path(), "retained-1");
    put_retained_entry(&store, dir.path(), "retained-2");

    crate::cli::run_auto_gc_worker(&cfg, std::time::Duration::ZERO);
    assert!(store.contains("retained-1") && store.contains("retained-2"));
    let record = read_auto_gc_backoff(&cfg.cache_dir).expect("held bytes recorded");
    assert_eq!((record.held, record.size_after), (8192, 8192));
    assert_eq!(record.interval_secs, 0, "nothing to back off from");

    expire_auto_gc_stamp(&cfg);
    assert!(!auto_gc_wanted(&cfg, &store));

    // A deleted worktree frees what it held without changing the size:
    // the figure expires, and the next check measures again.
    age_auto_gc_record_for_test(&cfg.cache_dir, AUTO_GC_HELD_TTL.as_secs());
    expire_auto_gc_stamp(&cfg);
    assert!(auto_gc_wanted(&cfg, &store));
}

/// Bytes no sweep may evict yet, such as entries builds are using, still
/// back the trigger off, so a sweep that could not clear the pressure is
/// not re-run at every check.
#[test]
fn auto_gc_backs_off_while_a_sweep_leaves_the_store_over_budget() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dir.path().to_path_buf());
    cfg.max_size = 1024;
    let store = Store::open(&cfg).unwrap();
    put_in_use_entry(&store, dir.path(), "retained-1");
    put_in_use_entry(&store, dir.path(), "retained-2");

    crate::cli::run_auto_gc_worker(&cfg, std::time::Duration::ZERO);
    assert!(store.contains("retained-1") && store.contains("retained-2"));
    let backoff = read_auto_gc_backoff(&cfg.cache_dir).expect("backoff recorded");
    assert_eq!(backoff.interval_secs, 600);
    assert_eq!(backoff.size_after, 8192);

    expire_auto_gc_stamp(&cfg);
    assert!(
        !auto_gc_wanted(&cfg, &store),
        "a sweep that could not clear the pressure must not re-run at the next check"
    );

    // The worker honours the backoff too: nothing to double yet.
    crate::cli::run_auto_gc_worker(&cfg, std::time::Duration::ZERO);
    assert_eq!(auto_gc_backoff_interval_for_test(&cfg.cache_dir), Some(600));

    expire_auto_gc_backoff_for_test(&cfg.cache_dir);
    crate::cli::run_auto_gc_worker(&cfg, std::time::Duration::ZERO);
    assert_eq!(
        read_auto_gc_backoff(&cfg.cache_dir).unwrap().interval_secs,
        1200,
        "another fruitless sweep doubles the wait"
    );

    // New bytes past the slack may be reclaimable: the backoff yields.
    put_test_entry(&store, dir.path(), "fresh");
    expire_auto_gc_stamp(&cfg);
    assert!(auto_gc_wanted(&cfg, &store));
}

#[test]
fn auto_gc_worker_leaves_the_backoff_alone_when_it_did_not_sweep() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dir.path().to_path_buf());
    cfg.max_size = 1024;
    let store = Store::open(&cfg).unwrap();
    put_retained_entry(&store, dir.path(), "retained");

    let gc_lock = store.try_gc_lock().unwrap().expect("gc lock");
    crate::cli::run_auto_gc_worker(&cfg, std::time::Duration::ZERO);
    assert_eq!(read_auto_gc_backoff(&cfg.cache_dir), None);

    drop(gc_lock);
    crate::cli::run_auto_gc_worker(&cfg, std::time::Duration::ZERO);
    assert!(read_auto_gc_backoff(&cfg.cache_dir).is_some());
}

/// Store an idle entry of exactly `size` bytes.
fn put_sized_entry(store: &Store, dir: &std::path::Path, key: &str, size: usize) {
    let src = dir.join(format!("{key}.o"));
    std::fs::write(&src, &key.as_bytes().repeat(size)[..size]).unwrap();
    store
        .put(
            key,
            "test-crate",
            &[],
            &[],
            "host",
            "dev",
            &[(src.clone(), format!("{key}.o"))],
            "",
            "",
        )
        .unwrap();
    // A source left behind can share the blob's blocks and retain it.
    store.remove_clone_for_test(&src);
    store.set_last_accessed_for_test(key, "-1 hour");
}

/// Sweeps the recorder has seen, one line each.
fn recorded_gc_runs(cfg: &Config) -> usize {
    std::fs::read_to_string(crate::report::gc_runs_log_path(&cfg.cache_dir))
        .map_or(0, |log| log.lines().count())
}

/// Under a compiler shim `current_exe` can be the shim. The worker must
/// still run as `kache gc`: before the fix it ran as `cc gc`, the real
/// compiler failed on it, and automatic GC never ran.
#[test]
fn auto_gc_worker_runs_gc_even_from_a_shim_path() {
    let exe = Path::new("/x/shims/cc");
    let cmd = auto_gc_worker_command(exe);
    assert_eq!(cmd.get_program(), exe.as_os_str());
    let args: Vec<&std::ffi::OsStr> = cmd.get_args().collect();
    assert_eq!(args, ["gc"]);
    let env = |name: &str| {
        cmd.get_envs()
            .find(|(key, _)| *key == name)
            .and_then(|(_, value)| value)
    };
    assert_eq!(env("KACHE_AUTO_GC_WORKER"), Some(std::ffi::OsStr::new("1")));
    let argv = ["/x/shims/cc".to_string(), "gc".to_string()];
    assert!(
        crate::platform::is_self_spawn(&argv, env(crate::platform::SELF_SPAWN_ENV)),
        "the child must route to the CLI despite its shim-named argv[0]"
    );
    // argv[0] has no getter. The worker must be `self_command` plus its
    // own variable, and platform's tests check that command's argv[0]
    // with a real child. Comparing the two Debug strings does not depend
    // on how std formats them.
    let mut expected = crate::platform::self_command(exe, "gc");
    expected.env("KACHE_AUTO_GC_WORKER", "1");
    assert_eq!(format!("{cmd:?}"), format!("{expected:?}"));
}

#[test]
fn auto_gc_constants_are_pinned() {
    assert_eq!(AUTO_GC_CHECK_INTERVAL.as_secs(), 300);
    assert_eq!(AUTO_GC_SLACK_PERCENT, 10);
    assert_eq!(AUTO_GC_MAX_BACKOFF.as_secs(), 7200);
    assert_eq!(AUTO_GC_HELD_TTL.as_secs(), 21_600);
    // Start above 110% of max_size, stop at 90%.
    assert_eq!(auto_gc_threshold(1000), 1100);
    assert_eq!(kache_store::eviction::eviction_target(1000), 900);
}

#[test]
fn the_outcome_line_mentions_a_wait_only_when_there_is_one() {
    let held_only = AutoGcBackoff {
        since: 1,
        interval_secs: 0,
        size_after: 5000,
        held: 3900,
    };
    let line = auto_gc_outcome_line(&held_only, 1000);
    assert!(
        line.contains("3900 of it held") && line.contains("no sweep can free"),
        "{line}"
    );
    assert!(!line.contains("next automatic sweep"), "{line}");
    let backoff = AutoGcBackoff {
        interval_secs: 1,
        ..held_only
    };
    let line = auto_gc_outcome_line(&backoff, 1000);
    assert!(line.contains("next automatic sweep in 1s"), "{line}");
}

/// max 1000, so the trigger is 1100.
#[test]
fn held_bytes_are_left_out_of_the_trigger_until_they_expire() {
    let ttl = AUTO_GC_HELD_TTL.as_secs();
    // Under the trigger only once the held bytes are left out: no
    // backoff, but a record that carries them.
    let record = next_auto_gc_backoff(None, 50, 5000, 3900, 1000).unwrap();
    assert_eq!(
        record,
        AutoGcBackoff {
            since: 50,
            interval_secs: 0,
            size_after: 5000,
            held: 3900,
        }
    );
    assert_eq!(
        next_auto_gc_backoff(None, 50, 5000, 0, 1000).map(|r| r.interval_secs),
        Some(600)
    );
    assert_eq!(next_auto_gc_backoff(None, 50, 1100, 0, 1000), None);
    // Over it even so: a backoff, carrying the held bytes, doubling from
    // the check interval after a record that had none.
    let backoff = next_auto_gc_backoff(Some(record), 60, 5000, 3899, 1000).unwrap();
    assert_eq!((backoff.interval_secs, backoff.held), (600, 3899));

    assert_eq!(recent_held_bytes(None, 50), 0);
    assert_eq!(recent_held_bytes(Some(record), 50 + ttl - 1), 3900);
    assert_eq!(recent_held_bytes(Some(record), 50 + ttl), 0);

    assert!(!auto_gc_due_at(Some(record), 60, 5000, 1000));
    assert!(
        auto_gc_due_at(Some(record), 60, 5001, 1000),
        "new bytes past the trigger"
    );
    assert!(
        auto_gc_due_at(Some(record), 50 + ttl, 5000, 1000),
        "the figure expired"
    );
    assert!(auto_gc_due_at(None, 60, 1101, 1000));
    assert!(!auto_gc_due_at(None, 60, 1100, 1000));
}

#[test]
fn auto_gc_sweep_due_starts_above_the_trigger_and_honours_the_backoff() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dir.path().to_path_buf());
    cfg.max_size = 1000;
    assert!(!auto_gc_sweep_due(&cfg, 1099));
    assert!(!auto_gc_sweep_due(&cfg, 1100));
    assert!(auto_gc_sweep_due(&cfg, 1101));

    record_auto_gc_outcome(&cfg, 1101, 0);
    assert!(!auto_gc_sweep_due(&cfg, 1101), "a held backoff defers");
    assert!(!auto_gc_sweep_due(&cfg, 1201));
    assert!(auto_gc_sweep_due(&cfg, 1202), "growth past the slack");
    expire_auto_gc_backoff_for_test(&cfg.cache_dir);
    assert!(auto_gc_sweep_due(&cfg, 1101));
}

#[test]
fn auto_gc_wanted_starts_one_byte_above_the_trigger() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dir.path().to_path_buf());
    cfg.max_size = 1000;
    let store = Store::open(&cfg).unwrap();
    put_sized_entry(&store, dir.path(), "at-the-trigger", 1100);
    assert_eq!(store.physical_size().unwrap(), 1100);
    assert!(!auto_gc_wanted(&cfg, &store));

    put_sized_entry(&store, dir.path(), "x", 1);
    assert_eq!(store.physical_size().unwrap(), 1101);
    expire_auto_gc_stamp(&cfg);
    assert!(auto_gc_wanted(&cfg, &store));
}

/// A build that stored into a `[cache.volumes]` shard asks whether that
/// shard is over its own budget, not the main store's (#974).
#[test]
fn auto_gc_wanted_judges_a_shard_against_its_own_budget() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dir.path().join("main"));
    cfg.max_size = 1_000_000;
    let shard = crate::config::VolumeStore {
        volume: "/mnt/shard/".into(),
        store: dir.path().join("shard"),
        max_size: Some(1000),
    };
    let shard_cfg = cfg.for_volume_store(&shard, |_| None);
    cfg.volume_stores = vec![shard];
    let main = Store::open(&cfg).unwrap();
    let store = Store::open(&shard_cfg).unwrap();
    put_sized_entry(&main, dir.path(), "main-entry", 1200);
    put_sized_entry(&store, dir.path(), "shard-entry", 1200);

    expire_auto_gc_stamp(&cfg);
    assert!(!auto_gc_wanted(&cfg, &main), "main is far under its budget");
    expire_auto_gc_stamp(&cfg);
    assert!(
        auto_gc_wanted(&cfg, &store),
        "the shard is over its 1000 bytes"
    );
}

#[test]
fn auto_gc_check_hints_a_reachable_daemon_and_spawns_no_worker() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dir.path().to_path_buf());
    cfg.max_size = 1024;
    let store = Store::open(&cfg).unwrap();
    put_test_entry(&store, dir.path(), "over-budget");
    let daemon =
        RemoteCheckReplyDaemon::with_reply(cfg.socket_path(), serde_json::json!({ "ok": true }));
    wait_until_reachable(&cfg.socket_path());

    let spawned = AtomicUsize::new(0);
    let spawn = |_: &Config| {
        spawned.fetch_add(1, Ordering::SeqCst);
    };
    run_auto_gc_check(&cfg, &store, crate::daemon::send_gc_hint, spawn);
    assert_eq!(daemon.request_count(), 1);
    assert_eq!(spawned.load(Ordering::SeqCst), 0);

    // A fresh stamp: the rest of the build sends nothing.
    run_auto_gc_check(&cfg, &store, crate::daemon::send_gc_hint, spawn);
    assert_eq!(daemon.request_count(), 1);
    assert_eq!(spawned.load(Ordering::SeqCst), 0);
}

#[test]
fn auto_gc_check_spawns_the_worker_when_no_daemon_listens() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dir.path().to_path_buf());
    cfg.max_size = 1024;
    let store = Store::open(&cfg).unwrap();
    put_test_entry(&store, dir.path(), "over-budget");

    let spawned = AtomicUsize::new(0);
    let spawn = |_: &Config| {
        spawned.fetch_add(1, Ordering::SeqCst);
    };
    run_auto_gc_check(&cfg, &store, crate::daemon::send_gc_hint, spawn);
    assert_eq!(spawned.load(Ordering::SeqCst), 1);

    run_auto_gc_check(&cfg, &store, crate::daemon::send_gc_hint, spawn);
    assert_eq!(spawned.load(Ordering::SeqCst), 1, "fresh stamp");
}

/// A daemon from before the hint answers it with an error.
#[test]
fn auto_gc_check_spawns_the_worker_when_the_daemon_rejects_the_hint() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dir.path().to_path_buf());
    cfg.max_size = 1024;
    let store = Store::open(&cfg).unwrap();
    put_test_entry(&store, dir.path(), "over-budget");
    let daemon = RemoteCheckReplyDaemon::with_reply(
        cfg.socket_path(),
        serde_json::json!({ "ok": false, "error": "invalid request: unknown variant" }),
    );
    wait_until_reachable(&cfg.socket_path());

    let spawned = AtomicUsize::new(0);
    run_auto_gc_check(&cfg, &store, crate::daemon::send_gc_hint, |_: &Config| {
        spawned.fetch_add(1, Ordering::SeqCst);
    });
    assert_eq!(daemon.request_count(), 1);
    assert_eq!(spawned.load(Ordering::SeqCst), 1);
}

#[test]
fn auto_gc_check_does_nothing_under_the_trigger() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path().to_path_buf());
    let store = Store::open(&cfg).unwrap();
    put_test_entry(&store, dir.path(), "fits");
    let called = AtomicUsize::new(0);
    let hint = |_: &Config| {
        called.fetch_add(1, Ordering::SeqCst);
        false
    };
    let spawn = |_: &Config| {
        called.fetch_add(1, Ordering::SeqCst);
    };
    run_auto_gc_check(&cfg, &store, hint, spawn);
    assert_eq!(called.load(Ordering::SeqCst), 0);
}

/// Another driver's fruitless sweep holds the worker back as well.
#[test]
fn auto_gc_worker_waits_out_a_backoff_another_driver_left() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dir.path().to_path_buf());
    cfg.max_size = 1024;
    cfg.record_sessions = true;
    let store = Store::open(&cfg).unwrap();
    put_sized_entry(&store, dir.path(), "evictable", 4096);
    record_auto_gc_outcome(&cfg, 4096, 0);

    crate::cli::run_auto_gc_worker(&cfg, std::time::Duration::ZERO);
    assert!(store.contains("evictable"));
    assert_eq!(recorded_gc_runs(&cfg), 0);
    assert_eq!(auto_gc_backoff_interval_for_test(&cfg.cache_dir), Some(600));

    // Once it expires the worker sweeps, fits the store and clears it.
    expire_auto_gc_backoff_for_test(&cfg.cache_dir);
    crate::cli::run_auto_gc_worker(&cfg, std::time::Duration::ZERO);
    assert!(!store.contains("evictable"));
    assert_eq!(
        recorded_gc_runs(&cfg),
        1,
        "a store that fits needs no retry"
    );
    assert_eq!(auto_gc_backoff_interval_for_test(&cfg.cache_dir), None);
}

/// The second sweep exists for entries a live build pins. Entries a
/// target directory retains stay retained, so the worker sweeps once.
#[test]
fn auto_gc_worker_retries_only_for_pinned_entries() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dir.path().to_path_buf());
    cfg.max_size = 1024;
    cfg.record_sessions = true;
    let store = Store::open(&cfg).unwrap();
    put_retained_entry(&store, dir.path(), "retained");
    crate::cli::run_auto_gc_worker(&cfg, std::time::Duration::ZERO);
    assert_eq!(recorded_gc_runs(&cfg), 1);

    let pinned_dir = tempfile::tempdir().unwrap();
    let mut pinned_cfg = test_config(pinned_dir.path().to_path_buf());
    pinned_cfg.max_size = 1024;
    pinned_cfg.record_sessions = true;
    let pinned_store = Store::open(&pinned_cfg).unwrap();
    // Just stored: inside the idle grace, so eviction pins it.
    put_test_entry(&pinned_store, pinned_dir.path(), "pinned");
    crate::cli::run_auto_gc_worker(&pinned_cfg, std::time::Duration::ZERO);
    assert_eq!(recorded_gc_runs(&pinned_cfg), 2);
    assert_eq!(
        auto_gc_backoff_interval_for_test(&pinned_cfg.cache_dir),
        Some(600),
        "two sweeps of one worker record one outcome"
    );
}

/// The backoff is stored and read back across processes, so its clock
/// must be the wall clock, not a constant every process agrees on.
#[test]
fn unix_now_secs_reads_the_wall_clock() {
    let wall = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    };
    let before = wall();
    let now = unix_now_secs();
    let after = wall();
    assert!(
        before <= now && now <= after,
        "{before} <= {now} <= {after}"
    );
}

#[test]
fn next_auto_gc_backoff_doubles_to_the_cap_and_clears_under_budget() {
    // max 1000: the trigger is 1100.
    assert_eq!(next_auto_gc_backoff(None, 50, 1100, 0, 1000), None);
    let first = next_auto_gc_backoff(None, 50, 1101, 0, 1000).unwrap();
    assert_eq!(
        first,
        AutoGcBackoff {
            since: 50,
            interval_secs: 600,
            size_after: 1101,
            held: 0,
        }
    );
    let second = next_auto_gc_backoff(Some(first), 90, 2000, 0, 1000).unwrap();
    assert_eq!(
        second,
        AutoGcBackoff {
            since: 90,
            interval_secs: 1200,
            size_after: 2000,
            held: 0,
        }
    );
    let long = AutoGcBackoff {
        interval_secs: 5000,
        ..second
    };
    assert_eq!(
        next_auto_gc_backoff(Some(long), 90, 2000, 0, 1000)
            .unwrap()
            .interval_secs,
        7200
    );
    assert_eq!(next_auto_gc_backoff(Some(second), 100, 900, 0, 1000), None);
}

#[test]
fn auto_gc_backoff_holds_until_it_expires_or_the_store_grows() {
    let backoff = Some(AutoGcBackoff {
        since: 1000,
        interval_secs: 600,
        size_after: 5000,
        held: 0,
    });
    // max 1000: the slack is 100 bytes.
    assert!(!auto_gc_backoff_holds(None, 1000, 5000, 1000));
    assert!(auto_gc_backoff_holds(backoff, 1599, 5000, 1000));
    assert!(!auto_gc_backoff_holds(backoff, 1600, 5000, 1000));
    assert!(auto_gc_backoff_holds(backoff, 1000, 5100, 1000));
    assert!(!auto_gc_backoff_holds(backoff, 1000, 5101, 1000));
    assert!(auto_gc_backoff_holds(backoff, 1000, 4000, 1000));
}

#[test]
fn record_auto_gc_outcome_persists_the_backoff_until_the_store_fits() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dir.path().to_path_buf());
    cfg.max_size = 1000;
    let before = unix_now_secs();
    record_auto_gc_outcome(&cfg, 5000, 0);
    let backoff = read_auto_gc_backoff(&cfg.cache_dir).unwrap();
    assert_eq!((backoff.interval_secs, backoff.size_after), (600, 5000));
    assert!(backoff.since >= before && backoff.since <= unix_now_secs());
    assert!(!auto_gc_sweep_due(&cfg, 5000));

    record_auto_gc_outcome(&cfg, 5000, 0);
    assert_eq!(
        read_auto_gc_backoff(&cfg.cache_dir).unwrap().interval_secs,
        1200
    );

    record_auto_gc_outcome(&cfg, 900, 0);
    assert!(!auto_gc_backoff_path(&cfg.cache_dir).exists());
    assert!(auto_gc_sweep_due(&cfg, 5000));
}

/// #131: explain_miss names exactly the key groups whose digests changed
/// vs the crate's last hit in the same tree — and stays silent (and
/// log-read-free) when disabled, on hits, or with no prior hit.
#[test]
fn explain_miss_diff_names_changed_groups() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().to_path_buf());
    config.explain_miss = true;

    let fields_now: std::collections::BTreeMap<String, String> = [
        ("args".to_string(), "bbbb".to_string()),
        ("sources".to_string(), "ssss".to_string()),
    ]
    .into();

    // No prior hit in the log → nothing to diff against.
    assert!(
        explain_miss_diff(
            &config,
            "/w",
            "gkrust",
            EventResult::Miss,
            "newkey",
            &fields_now
        )
        .is_empty()
    );

    let hit: crate::events::BuildEvent = serde_json::from_str(
        r#"{"ts":"2026-07-23T00:00:00Z","crate_name":"gkrust","root":"/w",
                "result":"local_hit","elapsed_ms":1,"size":1,
                "key_fields":{"args":"aaaa","sources":"ssss","link":"llll"}}"#,
    )
    .unwrap();
    events::log_event(&config.event_log_path(), &hit).unwrap();

    let diff = explain_miss_diff(
        &config,
        "/w",
        "gkrust",
        EventResult::Miss,
        "newkey",
        &fields_now,
    );
    assert_eq!(
        diff,
        vec!["args".to_string(), "link".to_string()],
        "changed digest + group missing from the new key both count"
    );

    // Same fields as the hit → the difference must be in post-hoc folds.
    let unchanged: std::collections::BTreeMap<String, String> = [
        ("args".to_string(), "aaaa".to_string()),
        ("sources".to_string(), "ssss".to_string()),
        ("link".to_string(), "llll".to_string()),
    ]
    .into();
    assert_eq!(
        explain_miss_diff(
            &config,
            "/w",
            "gkrust",
            EventResult::Miss,
            "newkey",
            &unchanged
        ),
        vec!["salt_or_extra_inputs".to_string()],
    );

    // Off by default / hits: no diagnostics.
    assert!(
        explain_miss_diff(
            &config,
            "/w",
            "gkrust",
            EventResult::LocalHit,
            "newkey",
            &fields_now
        )
        .is_empty()
    );
    config.explain_miss = false;
    assert!(
        explain_miss_diff(
            &config,
            "/w",
            "gkrust",
            EventResult::Miss,
            "newkey",
            &fields_now
        )
        .is_empty()
    );
}

/// After a deferred compile the keyed flow must never run the compiler
/// again: Cargo has already consumed the first run's artifact
/// notifications, and a second set makes it finish the unit twice.
#[test]
fn a_passthrough_after_a_deferred_compile_keeps_the_first_exit_code() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().to_path_buf());
    let args = RustcArgs::parse(&[
        dir.path().join("no-such-rustc").display().to_string(),
        "--crate-name".into(),
        "kt".into(),
        "src/lib.rs".into(),
    ])
    .unwrap();
    PRECOMPILED_EXIT.with(|cell| cell.set(Some(0)));
    let exit = passthrough_with_event(
        &config,
        &args,
        "kt",
        "root",
        std::time::Instant::now(),
        "build lock wait failed",
    );
    PRECOMPILED_EXIT.with(|cell| cell.set(None));
    assert_eq!(exit.unwrap(), 0, "the compiler must not run a second time");
    assert!(
        passthrough_with_event(
            &config,
            &args,
            "kt",
            "root",
            std::time::Instant::now(),
            "build lock wait failed",
        )
        .is_err(),
        "without a deferred compile the passthrough runs the (missing) compiler"
    );
}

/// The failure handling after a passthrough reads rustc's words only: the
/// key error a failed pre-pass left in the reason, never kache's own.
#[test]
fn only_an_uncacheable_reason_quotes_rustc_output() {
    let error = anyhow::anyhow!("writing /x: Permission denied (os error 13)")
        .context("dep-info pre-pass failed");
    let reason = uncacheable_reason(&error);
    assert_eq!(
        reason,
        "uncacheable|dep-info pre-pass failed: writing /x: Permission denied (os error 13)"
    );
    assert_eq!(
        rustc_output_in(&reason),
        "dep-info pre-pass failed: writing /x: Permission denied (os error 13)"
    );
    for reason in [
        "store lookup failed: Permission denied (os error 13)",
        "adaptive passthrough: uncacheable|x",
        "",
    ] {
        assert_eq!(rustc_output_in(reason), "", "{reason}");
    }
}

#[test]
fn a_prestage_candidate_is_a_stored_file_of_at_least_the_threshold() {
    let dir = tempfile::tempdir().unwrap();
    let file = |name: &str, len: u64| {
        let path = dir.path().join(name);
        std::fs::File::create(&path).unwrap().set_len(len).unwrap();
        (path, name.to_string())
    };
    let below = file("below", crate::prestage::MIN_BYTES - 1);
    let at = file("at", crate::prestage::MIN_BYTES);
    assert!(!stores_prestage_candidate(std::slice::from_ref(&below)));
    assert!(stores_prestage_candidate(&[below, at]));
}

/// A staged copy is taken only for a large file restored as a private
/// copy; a linked restore leaves it alone.
#[test]
fn only_a_large_private_copy_takes_a_staged_copy() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("blob");
    std::fs::write(&blob, b"an executable").unwrap();
    let hash = blake3::hash(b"an executable").to_hex().to_string();
    let dest = dir.path().join("app-0123456789abcdef");
    crate::prestage::stage_for_test(&dest, &blob, &hash);
    let staged = crate::prestage::staged_path(&dest, &hash).unwrap();
    let mut file = cached_file("app-0123456789abcdef", &hash);

    file.size = crate::prestage::MIN_BYTES;
    assert!(!take_prestaged(link::LinkStrategy::Hardlink, &file, &dest));
    assert!(staged.exists() && !dest.exists());
    file.size = crate::prestage::MIN_BYTES - 1;
    assert!(!take_prestaged(link::LinkStrategy::Copy, &file, &dest));
    assert!(staged.exists() && !dest.exists());
    file.size = crate::prestage::MIN_BYTES;
    assert!(take_prestaged(link::LinkStrategy::Copy, &file, &dest));
    assert_eq!(std::fs::read(&dest).unwrap(), b"an executable");
}

/// Only large executables restored as private copies are recorded: not
/// small ones, and not large outputs that restore as links.
#[test]
fn only_large_private_copies_are_recorded_for_prestaging() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let out_dir = dir.path().join("target/debug/deps");
    let args = rustc_args(&[
        "rustc",
        "main.rs",
        "--crate-name",
        "app",
        "--crate-type",
        "bin",
        "--out-dir",
        out_dir.to_str().unwrap(),
        "-C",
        "extra-filename=-0123456789abcdef",
    ]);
    let file = |name: &str, size: u64, executable: bool| {
        let mut file = cached_file(name, &"a".repeat(64));
        file.size = size;
        file.executable = executable;
        file
    };
    let meta = entry_meta(
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        vec![
            file("app-0123456789abcdef", crate::prestage::MIN_BYTES, true),
            file(
                "tool-0123456789abcdef",
                crate::prestage::MIN_BYTES - 1,
                true,
            ),
            file(
                "libapp-0123456789abcdef.rlib",
                crate::prestage::MIN_BYTES,
                false,
            ),
        ],
        &["link"],
    );
    remember_prestaged_executables(&config, &RustcCompiler::new(), &args, &out_dir, None, &meta);
    assert_eq!(
        crate::prestage::recorded_dests(&config.cache_dir),
        vec![out_dir.join("app-0123456789abcdef")]
    );
}

/// In `-o` mode the primary output goes to the exact `-o` path, and
/// every other output into the output directory.
#[test]
fn artifact_target_path_keeps_the_exact_output_path() {
    let args = rustc_args(&["rustc", "main.rs", "--crate-name", "app", "-o", "out/app"]);
    let elsewhere = Path::new("/elsewhere");
    assert_eq!(
        artifact_target_path(&args, elsewhere, "app"),
        PathBuf::from("out/app")
    );
    assert_eq!(
        artifact_target_path(&args, elsewhere, "app.d"),
        elsewhere.join("app.d")
    );
    assert_eq!(rustc_output_dir(&args), Some(PathBuf::from("out")));
}

/// Prestage records and hints go to the target directory itself, also
/// for a build script compiled into `<profile>/build/<unit>`, where
/// `RustcArgs::target_dir` answers the profile directory.
#[test]
fn prestage_target_dir_is_the_target_root_for_every_unit() {
    let parse = |out_dir: &str, target: Option<&str>| {
        let mut argv = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "build_script_build".to_string(),
            "build.rs".to_string(),
            "--out-dir".to_string(),
            out_dir.to_string(),
        ];
        if let Some(target) = target {
            argv.extend(["--target".to_string(), target.to_string()]);
        }
        RustcArgs::parse(&argv).unwrap()
    };
    let root = Some(PathBuf::from("/w/target"));
    for (out_dir, target) in [
        ("/w/target/debug/build/app-0123456789abcdef", None),
        ("/w/target/debug/build/app/0123456789abcdef/out", None),
        ("/w/target/debug/deps", None),
        (
            "/w/target/x86_64-unknown-linux-gnu/debug/deps",
            Some("x86_64-unknown-linux-gnu"),
        ),
    ] {
        assert_eq!(
            prestage_target_dir(&parse(out_dir, target)),
            root,
            "{out_dir}"
        );
    }
    assert_eq!(
        parse("/w/target/debug/build/app-0123456789abcdef", None).target_dir(),
        Some(PathBuf::from("/w/target/debug")),
        "the case prestage_target_dir corrects"
    );
}

#[test]
fn compile_before_key_needs_a_local_store() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().to_path_buf());
    let parse = |argv: &[&str]| {
        RustcArgs::parse(&argv.iter().map(|a| (*a).to_string()).collect::<Vec<_>>()).unwrap()
    };
    let cargo_like = parse(&[
        "rustc",
        "--crate-name",
        "kt",
        "src/lib.rs",
        "--emit=dep-info,metadata",
        "--out-dir",
        "/t/debug/deps",
    ]);
    let no_dep_info = parse(&[
        "rustc",
        "--crate-name",
        "kt",
        "src/lib.rs",
        "--emit=link",
        "--out-dir",
        "/t/debug/deps",
    ]);
    assert!(
        deferral_allowed(&config, &cargo_like, false, None),
        "predictions off still defers a provable miss"
    );
    config.input_predictions = true;
    assert!(deferral_allowed(&config, &cargo_like, false, None));
    assert!(
        !deferral_allowed(&config, &no_dep_info, false, None),
        "nothing to key from"
    );
    config.deferred_discovery = false;
    assert!(
        !deferral_allowed(&config, &cargo_like, false, None),
        "switched off"
    );
    config.deferred_discovery = true;
    assert!(
        !deferral_allowed(&config, &cargo_like, true, None),
        "adaptive unit"
    );
    config.fallback = Some("sccache".to_string());
    assert!(
        !deferral_allowed(&config, &cargo_like, false, None),
        "fallback store"
    );
    config.fallback = None;
    config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
    assert!(
        !deferral_allowed(&config, &cargo_like, false, None),
        "remote configured"
    );
}

fn test_config(cache_dir: PathBuf) -> Config {
    crate::test_support::test_config(cache_dir)
}

#[test]
fn rows_are_asked_of_the_remote_only_with_predictions_and_a_remote() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().to_path_buf());
    config.remote = None;
    config.input_predictions = true;
    assert!(!fetches_remote_predictions(&config));
    config.remote = Some(crate::config::RemoteConfig {
        prefix: "artifacts".to_string(),
        backend: crate::config::RemoteBackendConfig::Filesystem(
            crate::config::FilesystemRemoteConfig {
                root: dir.path().join("remote"),
                atomic_write_dir: dir.path().join("staging"),
            },
        ),
    });
    assert!(fetches_remote_predictions(&config));
    config.input_predictions = false;
    assert!(!fetches_remote_predictions(&config));
}

#[test]
fn configured_rustc_depinfo_roots_cover_every_restorable_anchor() {
    // This is the set the store side relativizes dep-info against. Losing a
    // root leaves a live producer path in the stored `.d`, so a relocated
    // hit lets cargo validate freshness against the donor's worktree
    // instead of the consumer's (#760).
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().canonicalize().unwrap();
    let workspace = base.join("workspace");
    let target = base.join("shared-target");
    let vendored = base.join("vendored-sources");
    for path in [&workspace, &target, &vendored] {
        std::fs::create_dir_all(path).unwrap();
    }

    let config = Config {
        base_dirs: vec![vendored.to_string_lossy().into_owned()],
        ..test_config(base.join("cache"))
    };
    let roots = configured_rustc_depinfo_roots(&config, Some(&workspace), Some(&target));

    let found = |root: &Path, sentinel: &str| {
        roots
            .iter()
            .any(|(path, depinfo_sentinel, _)| path == root && depinfo_sentinel == sentinel)
    };
    assert!(
        found(&workspace, "__kache_workspace__/"),
        "workspace root missing from {roots:?}"
    );
    assert!(
        found(&target, "__kache_target_rule__/"),
        "external target root missing from {roots:?}"
    );
    assert!(
        found(&vendored, "__kache_base_dir_0__/"),
        "configured base dir missing from {roots:?}"
    );

    // Priorities are what break ties when roots nest, so they must be the
    // real ranks rather than a uniform placeholder.
    let workspace_priority = roots
        .iter()
        .find(|(path, _, _)| path == &workspace)
        .map(|(_, _, priority)| *priority)
        .unwrap();
    let target_priority = roots
        .iter()
        .find(|(path, _, _)| path == &target)
        .map(|(_, _, priority)| *priority)
        .unwrap();
    assert!(
        workspace_priority > target_priority,
        "the workspace must outrank an external target ({workspace_priority} vs {target_priority})"
    );
}

#[test]
fn input_race_store_suppression_truth_table() {
    for (extra_inputs_racy, guard_enabled, key_too_new, expected) in [
        (false, false, false, false),
        (false, false, true, false),
        (false, true, false, false),
        (false, true, true, true),
        (true, false, false, true),
        (true, false, true, true),
        (true, true, false, true),
        (true, true, true, true),
    ] {
        assert_eq!(
            should_skip_cache_store_for_input_race(extra_inputs_racy, guard_enabled, key_too_new,),
            expected
        );
    }
}

#[test]
fn key_inputs_changed_excuses_skewed_clocks_but_not_real_changes() {
    use crate::cache_key::FileFingerprint;

    // No tripped flag: nothing to excuse, whatever was recorded.
    assert!(!key_inputs_changed_during_compile(false, &[]));
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("input.rs");
    std::fs::write(&file, b"pub fn x() {}").unwrap();
    let recorded = FileFingerprint::from_path(&file).unwrap();
    assert!(!key_inputs_changed_during_compile(
        false,
        std::slice::from_ref(&recorded)
    ));
    // Tripped flag with nothing verifiable stays a refusal.
    assert!(key_inputs_changed_during_compile(true, &[]));
    #[cfg(unix)]
    {
        // Tripped flag, untouched inputs: a skewed clock, not a race.
        assert!(!key_inputs_changed_during_compile(
            true,
            std::slice::from_ref(&recorded)
        ));
        // Tripped flag, rewritten inputs: a real race.
        std::fs::write(&file, b"pub fn x() { 1 }").unwrap();
        assert!(key_inputs_changed_during_compile(
            true,
            std::slice::from_ref(&recorded)
        ));
    }
}

#[test]
fn key_measurements_include_extra_input_time_stats_and_races() {
    let local = FileHashStats {
        cache_hits: 11,
        cache_misses: 13,
        bytes_hashed: 17,
    };
    let extra = FileHashStats {
        cache_hits: 2,
        cache_misses: 3,
        bytes_hashed: 5,
    };

    let (key_ms, combined, too_new) = combine_key_measurements(19, 7, local, extra, false, true);
    assert_eq!(key_ms, 26);
    assert_eq!(combined.cache_hits, 13);
    assert_eq!(combined.cache_misses, 16);
    assert_eq!(combined.bytes_hashed, 22);
    assert!(too_new, "an extra input race must propagate");

    assert!(combine_key_measurements(0, 0, local, extra, true, false).2);
    assert!(!combine_key_measurements(0, 0, local, extra, false, false).2);
}

#[test]
fn only_active_extra_inputs_make_unreadable_cached_dep_info_immediately_fatal() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing.d");
    assert_eq!(read_cached_dep_info_blob(&missing, false).unwrap(), None);
    assert!(read_cached_dep_info_blob(&missing, true).is_err());

    let readable = dir.path().join("readable.d");
    std::fs::write(&readable, "foo: src/lib.rs\n").unwrap();
    assert_eq!(
        read_cached_dep_info_blob(&readable, true).unwrap(),
        Some("foo: src/lib.rs\n".to_string())
    );
}

#[test]
fn adaptive_mode_requires_opt_in_without_explicit_preservation() {
    let mut config = test_config(PathBuf::from("cache"));
    for (adaptive, preserve, expected) in [
        (false, false, false),
        (false, true, false),
        (true, true, false),
        (true, false, true),
    ] {
        config.adaptive_incremental = adaptive;
        config.preserve_incremental = preserve;
        assert_eq!(adaptive_mode_enabled(&config), expected);
    }

    let without_incremental = rustc_args(&["rustc", "src/lib.rs"]);
    let with_incremental = rustc_args(&["rustc", "src/lib.rs", "-Cincremental=incremental"]);
    config.preserve_incremental = false;
    assert!(!preserve_incremental_requested(&config, &with_incremental));
    config.preserve_incremental = true;
    assert!(!preserve_incremental_requested(
        &config,
        &without_incremental
    ));
    assert!(preserve_incremental_requested(&config, &with_incremental));
}

#[test]
fn incremental_force_list_requires_incremental_and_managed_layout() {
    let mut config = test_config(PathBuf::from("cache"));
    config.adaptive_incremental = false;
    assert!(
        !config.incremental_crate_forced("tap_lib"),
        "empty force-list must force nothing"
    );
    config.incremental_crates =
        crate::config::normalize_incremental_crates(["tap-lib".to_string()]);
    // Matching is against rustc's crate name; spelling normalization does
    // not make the Cargo package name authoritative.
    assert!(config.incremental_crate_forced("tap_lib"));
    assert!(config.incremental_crate_forced("tap-lib"));
    assert!(!config.incremental_crate_forced("other"));

    let no_incremental = rustc_args(&["rustc", "--crate-name", "tap_lib", "src/lib.rs"]);
    assert!(!force_incremental_requested(&config, &no_incremental));
    assert!(
        managed_incremental_unit(&config, &no_incremental, true, || {
            panic!("hidden-input discovery must not run for an ineligible invocation")
        })
        .is_none()
    );

    let temp = tempfile::tempdir().unwrap();
    let args = eligible_incremental_args(&temp, "tap_lib");
    assert!(force_incremental_requested(&config, &args));
    let unit = managed_incremental_unit(&config, &args, true, || false).unwrap();
    let lease = unit.try_immediate().unwrap();
    let compiler_args = lease.compiler_args(&args);
    let original = args.incremental.as_ref().unwrap().display().to_string();
    assert!(
        compiler_args
            .iter()
            .any(|arg| arg.contains("incremental.kache-auto") && arg.ends_with("rustc")),
        "force-list must use policy-owned incremental state: {compiler_args:?}"
    );
    assert!(
        !compiler_args.iter().any(|arg| arg.ends_with(&original)),
        "the original Cargo incremental path must never reach rustc"
    );
    assert!(!lease.finish(false));
}

#[test]
fn force_list_never_retries_through_adaptive_seed_policy() {
    let mut config = test_config(PathBuf::from("cache"));
    config.adaptive_incremental = true;
    config.incremental_crates = vec!["tap_lib".to_string()];
    let args = rustc_args(&[
        "rustc",
        "--crate-name",
        "tap_lib",
        "src/lib.rs",
        "-Cincremental=incremental",
    ]);

    assert!(force_incremental_requested(&config, &args));
    assert!(adaptive_mode_enabled(&config));
    assert!(
        !adaptive_seed_allowed(&config, &args),
        "a force-listed invocation must not enter adaptive seed policy"
    );

    config.incremental_crates.clear();
    assert!(adaptive_seed_allowed(&config, &args));
    config.adaptive_incremental = false;
    assert!(!adaptive_seed_allowed(&config, &args));
}

#[test]
fn force_list_hidden_inputs_and_cache_exclusions_fail_closed() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = test_config(temp.path().join("cache"));
    config.adaptive_incremental = false;
    config.incremental_crates = vec!["tap_lib".to_string()];
    let args = eligible_incremental_args(&temp, "tap_lib");

    assert!(managed_incremental_unit(&config, &args, true, || true).is_none());
    assert!(incremental_fast_path_allowed(false, false, false));
    assert!(!incremental_fast_path_allowed(false, true, false));
    assert!(!incremental_fast_path_allowed(false, false, true));
    assert!(!incremental_fast_path_allowed(true, false, false));
    // Either refusal alone keeps a unit off the fast path.
    assert!(!unit_refuses_caching(false, false));
    assert!(unit_refuses_caching(true, false));
    assert!(unit_refuses_caching(false, true));
    assert!(unit_refuses_caching(true, true));

    let stripped: Vec<_> = compile::strip_incremental_flags(&args.all_args)
        .into_iter()
        .cloned()
        .collect();
    assert!(
        !stripped.iter().any(|arg| arg.contains("incremental=")),
        "a rejected force-list invocation must retain the safe cache argv"
    );
}

#[test]
fn incremental_cleanup_requires_opt_in_without_preservation() {
    let mut config = test_config(PathBuf::from("cache"));
    for (clean, preserve, expected) in [
        (false, false, false),
        (false, true, false),
        (true, true, false),
        (true, false, true),
    ] {
        config.clean_incremental = clean;
        config.preserve_incremental = preserve;
        assert_eq!(incremental_cleanup_enabled(&config), expected);
    }

    assert!(disable_incremental_env(false));
    assert!(!disable_incremental_env(true));
}

#[test]
fn adaptive_policy_guard_tracks_kache_semantic_inputs() {
    const ENV_KEY: &str = "KACHE_WRAPPER_POLICY_GUARD_TEST";

    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().join("cache"));
    let baseline = adaptive_policy_guard(&config);

    config.key_salt = Some("salt-a".to_string());
    assert_ne!(adaptive_policy_guard(&config), baseline);
    config.key_salt = None;

    let _env = TestEnvGuard::set(ENV_KEY, "value-a");
    config.key_env_vars = vec![ENV_KEY.to_string()];
    let env_a = adaptive_policy_guard(&config);
    unsafe { std::env::set_var(ENV_KEY, "value-b") };
    assert_ne!(adaptive_policy_guard(&config), env_a);
    config.key_env_vars.clear();

    config.base_dirs = vec![dir.path().display().to_string()];
    assert_ne!(adaptive_policy_guard(&config), baseline);
}

fn meta_with_diagnostics(stdout: &str, stderr: &str) -> crate::store::EntryMeta {
    crate::store::EntryMeta {
        cache_key: "k".to_string(),
        key_schema: crate::cache_key::CACHE_KEY_VERSION,
        crate_name: "c".to_string(),
        crate_types: vec![],
        files: vec![],
        stdout: stdout.to_string(),
        stderr: stderr.to_string(),
        features: vec![],
        target: String::new(),
        profile: String::new(),
        compile_time_ms: 0,
        emit_kinds: vec![],
    }
}

fn entry_meta_with_files(names: &[&str]) -> crate::store::EntryMeta {
    let mut meta = meta_with_diagnostics("", "");
    meta.files = names
        .iter()
        .map(|name| crate::store::CachedFile {
            name: (*name).to_string(),
            size: 1,
            hash: "0123456789abcdef".to_string(),
            executable: false,
        })
        .collect();
    meta
}

#[test]
fn replay_cached_diagnostics_writes_nonempty_and_skips_empty() {
    // Non-empty streams are replayed verbatim, each to its own sink. This is
    // the contract the coalesced-restore (and every cache-hit) path relies on
    // to avoid swallowing the original compiler warnings/notes.
    let m = meta_with_diagnostics("warning: unused\n", "error: boom\n");
    let mut out = Vec::new();
    let mut err = Vec::new();
    replay_cached_diagnostics(&m, &mut out, &mut err);
    assert_eq!(out, b"warning: unused\n");
    assert_eq!(err, b"error: boom\n");

    // Empty streams write nothing — the `!is_empty()` guard is load-bearing:
    // dropping it (as a mutant does) would make the non-empty case above emit
    // nothing, which the assertions catch.
    let empty = meta_with_diagnostics("", "");
    let mut out2 = Vec::new();
    let mut err2 = Vec::new();
    replay_cached_diagnostics(&empty, &mut out2, &mut err2);
    assert!(out2.is_empty(), "empty stdout must not be written");
    assert!(err2.is_empty(), "empty stderr must not be written");
}

#[test]
fn replay_diagnostics_forwards_both_compiler_streams() {
    let mut out = Vec::new();
    let mut err = Vec::new();
    replay_diagnostics("compiler stdout\n", "compiler stderr\n", &mut out, &mut err);
    assert_eq!(out, b"compiler stdout\n");
    assert_eq!(err, b"compiler stderr\n");
}

#[test]
fn passthrough_direct_args_preserve_only_unchanged_response_transport() {
    let dir = tempfile::tempdir().unwrap();
    let response = dir.path().join("rustc.args");
    std::fs::write(&response, "--crate-name\nfixture\nsrc/lib.rs\n").unwrap();
    let response_arg = format!("@{}", response.display());
    let args = RustcArgs::parse(&["rustc".to_string(), response_arg.clone()]).unwrap();

    let unchanged = passthrough_direct_args(&args, &args.all_args, false);
    assert!(!compiler_args_changed(&args, &args.all_args));
    assert_eq!(stripped_incremental_count(&args, &args.all_args), None);
    assert_eq!(
        unchanged.iter().map(|arg| arg.as_str()).collect::<Vec<_>>(),
        vec![response_arg.as_str()]
    );

    let rewritten = vec!["--crate-name".to_string(), "rewritten".to_string()];
    assert!(compiler_args_changed(&args, &rewritten));
    assert_eq!(stripped_incremental_count(&args, &rewritten), Some(1));
    let changed = passthrough_direct_args(&args, &rewritten, true);
    assert_eq!(
        changed.iter().map(|arg| arg.as_str()).collect::<Vec<_>>(),
        vec!["--crate-name", "rewritten"]
    );

    assert!(
        handle_response_file_error(anyhow::anyhow!("unchanged transport"), false)
            .unwrap()
            .is_none()
    );
    assert!(handle_response_file_error(anyhow::anyhow!("rewritten transport"), true).is_err());
}

fn cached_file(name: &str, hash: &str) -> crate::store::CachedFile {
    crate::store::CachedFile {
        name: name.to_string(),
        size: 1,
        hash: hash.to_string(),
        executable: false,
    }
}

fn entry_meta(
    cache_key: &str,
    files: Vec<crate::store::CachedFile>,
    emit_kinds: &[&str],
) -> crate::store::EntryMeta {
    crate::store::EntryMeta {
        cache_key: cache_key.to_string(),
        key_schema: crate::cache_key::CACHE_KEY_VERSION,
        crate_name: "foo".to_string(),
        crate_types: vec!["lib".to_string()],
        files,
        stdout: String::new(),
        stderr: String::new(),
        features: Vec::new(),
        target: "host".to_string(),
        profile: "dev".to_string(),
        compile_time_ms: 7,
        emit_kinds: emit_kinds.iter().map(|kind| (*kind).to_string()).collect(),
    }
}

fn create_blob(store: &Store, hash: &str, content: &[u8]) {
    let blob = store.blob_path(hash);
    std::fs::create_dir_all(blob.parent().unwrap()).unwrap();
    std::fs::write(blob, content).unwrap();
}

#[test]
fn active_extra_inputs_store_requires_the_expected_dep_info_artifact() {
    let dir = tempfile::tempdir().unwrap();
    let project = dir.path().join("project");
    let source = project.join("src/lib.rs");
    let out_dir = dir.path().join("target/debug/deps");
    std::fs::create_dir_all(source.parent().unwrap()).unwrap();
    std::fs::create_dir_all(&out_dir).unwrap();
    std::fs::write(
        project.join("Cargo.toml"),
        "[package]\nname='foo'\nversion='0.1.0'\n",
    )
    .unwrap();
    std::fs::write(project.join("kache.toml"), "extra_inputs = []\n").unwrap();
    std::fs::write(&source, "pub fn f() {}\n").unwrap();

    let args = rustc_args(&[
        "rustc",
        source.to_str().unwrap(),
        "--crate-name",
        "foo",
        "--emit",
        "dep-info",
        "--out-dir",
        out_dir.to_str().unwrap(),
    ]);
    let snapshot = crate::extra_inputs::ExtraInputsSnapshot::resolve(
        args.source_file.as_deref(),
        "foo",
        args.is_primary,
        &FileHasher::new(),
    )
    .unwrap()
    .unwrap();
    let artifacts = ArtifactSet::new(Vec::new());
    let error = validate_extra_inputs_dep_info_before_store(&args, &artifacts, &snapshot)
        .expect_err("active extra_inputs requires the dep-info Cargo requested");
    assert!(
        format!("{error:#}").contains("no expected dep-info artifact"),
        "{error:#}"
    );
}

#[test]
fn active_extra_inputs_store_accepts_the_expected_dep_info_artifact() {
    let dir = tempfile::tempdir().unwrap();
    let project = dir.path().join("project");
    let source = project.join("src/lib.rs");
    let out_dir = dir.path().join("target/debug/deps");
    std::fs::create_dir_all(source.parent().unwrap()).unwrap();
    std::fs::create_dir_all(&out_dir).unwrap();
    std::fs::write(
        project.join("Cargo.toml"),
        "[package]\nname='foo'\nversion='0.1.0'\n",
    )
    .unwrap();
    std::fs::write(project.join("kache.toml"), "extra_inputs = []\n").unwrap();
    std::fs::write(&source, "pub fn f() {}\n").unwrap();

    let args = rustc_args(&[
        "rustc",
        source.to_str().unwrap(),
        "--crate-name",
        "foo",
        "--emit",
        "dep-info",
        "--out-dir",
        out_dir.to_str().unwrap(),
    ]);
    let snapshot = crate::extra_inputs::ExtraInputsSnapshot::resolve(
        args.source_file.as_deref(),
        "foo",
        args.is_primary,
        &FileHasher::new(),
    )
    .unwrap()
    .unwrap();
    let metadata = out_dir.join("libfoo.rmeta");
    std::fs::write(&metadata, b"metadata").unwrap();
    let dep_info = out_dir.join("foo.d");
    std::fs::write(&dep_info, format!("foo: {}\n", source.display())).unwrap();
    let artifacts = ArtifactSet::new(vec![
        crate::compiler::Artifact {
            path: metadata,
            store_name: "libfoo.rmeta".to_string(),
            kind: ArtifactKind::Metadata,
            required: true,
        },
        crate::compiler::Artifact {
            path: dep_info,
            store_name: "foo.d".to_string(),
            kind: ArtifactKind::DepInfo,
            required: true,
        },
    ]);

    validate_extra_inputs_dep_info_before_store(&args, &artifacts, &snapshot)
        .expect("the expected producer dep-info artifact is valid");
}

#[test]
fn restore_rejects_dep_info_with_no_dependencies() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let project = dir.path().join("project");
    let source = project.join("src/lib.rs");
    let out_dir = dir.path().join("target/debug/deps");
    std::fs::create_dir_all(source.parent().unwrap()).unwrap();
    std::fs::write(
        project.join("Cargo.toml"),
        "[package]\nname='foo'\nversion='0.1.0'\n",
    )
    .unwrap();
    std::fs::write(&source, "pub fn f() {}\n").unwrap();

    let args = rustc_args(&[
        "rustc",
        source.to_str().unwrap(),
        "--crate-name",
        "foo",
        "--emit",
        "dep-info",
        "--out-dir",
        out_dir.to_str().unwrap(),
    ]);
    let dep_info = "foo: \n";
    let hash = blake3::hash(dep_info.as_bytes()).to_hex().to_string();
    create_blob(&store, &hash, dep_info.as_bytes());
    let mut file = cached_file("foo.d", &hash);
    file.size = dep_info.len() as u64;
    let meta = entry_meta("empty-dependencies", vec![file], &["dep-info"]);

    let error = restore_from_cache(&config, &RustcCompiler::new(), &store, &args, &meta, None)
        .expect_err("empty dependency rules must be evicted");
    assert!(
        format!("{error:#}").contains("has no dependencies"),
        "{error:#}"
    );
}

#[test]
fn cc_store_freezes_private_artifacts_without_mutating_compiler_outputs() {
    let dir = tempfile::tempdir().unwrap();
    let object = dir.path().join("foo.o");
    let depinfo = dir.path().join("foo.d");
    let original = format!(
        "{}/foo.o: {}/src/foo.c\n",
        dir.path().display(),
        dir.path().display()
    );
    std::fs::write(&object, b"object").unwrap();
    std::fs::write(&depinfo, &original).unwrap();
    let artifacts = ArtifactSet::new(vec![
        crate::compiler::Artifact {
            path: object.clone(),
            store_name: "foo.o".to_string(),
            kind: ArtifactKind::Object,
            required: true,
        },
        crate::compiler::Artifact {
            path: depinfo.clone(),
            store_name: "foo.d".to_string(),
            kind: ArtifactKind::DepInfo,
            required: true,
        },
    ]);

    let prepared = prepare_cc_store_files(&artifacts, Some(dir.path())).unwrap();

    assert_eq!(std::fs::read_to_string(&depinfo).unwrap(), original);
    assert_ne!(prepared.files[0].0, object);
    assert_ne!(prepared.files[1].0, depinfo);
    assert_eq!(std::fs::read(&prepared.files[0].0).unwrap(), b"object");
    assert!(
        std::fs::read_to_string(&prepared.files[1].0)
            .unwrap()
            .contains("__kache_root__/")
    );

    std::fs::write(&object, b"concurrent replacement").unwrap();
    std::fs::write(&depinfo, b"concurrent replacement").unwrap();
    assert_eq!(
        std::fs::read(&prepared.files[0].0).unwrap(),
        b"object",
        "Store::put must read the frozen object snapshot"
    );
    assert!(
        std::fs::read_to_string(&prepared.files[1].0)
            .unwrap()
            .contains("__kache_root__/"),
        "Store::put must read the frozen normalized dep-info snapshot"
    );
}

/// Rust store staging leaves compiler outputs untouched while the cached
/// dep-info round trip re-roots target, package, and workspace paths.
#[test]
fn rustc_store_staging_round_trips_depinfo_without_mutating_outputs() {
    let dir = tempfile::tempdir().unwrap();
    let producing_workspace = dir.path().join("worktree-a");
    let producing_working_dir = producing_workspace.join("member");
    let producing_target = producing_workspace.join("target");
    let depfile = producing_target.join("release/deps/foo-abc.d");
    let rlib = producing_target.join("release/deps/libfoo-abc.rlib");
    std::fs::create_dir_all(depfile.parent().unwrap()).unwrap();
    std::fs::create_dir_all(&producing_working_dir).unwrap();
    let original = format!(
        "{}: {} {}\n",
        rlib.display(),
        producing_working_dir.join("src/lib.rs").display(),
        producing_workspace.join("shared/asset.txt").display(),
    );
    std::fs::write(&depfile, &original).unwrap();
    std::fs::write(&rlib, b"rlib bytes").unwrap();
    let outputs = ArtifactSet::new(vec![
        crate::compiler::Artifact {
            path: depfile.clone(),
            store_name: "foo-abc.d".to_string(),
            kind: ArtifactKind::DepInfo,
            required: true,
        },
        crate::compiler::Artifact {
            path: rlib.clone(),
            store_name: "libfoo-abc.rlib".to_string(),
            kind: ArtifactKind::Library,
            required: true,
        },
    ]);

    let prepared = prepare_rustc_store_files(
        &outputs,
        Some(&producing_target),
        &producing_working_dir,
        Some(&producing_workspace),
        &[],
    )
    .unwrap();
    assert_eq!(std::fs::read_to_string(&depfile).unwrap(), original);
    assert_eq!(std::fs::read(&rlib).unwrap(), b"rlib bytes");
    assert_ne!(prepared.files[0].0, depfile);
    assert_eq!(prepared.files[1].0, rlib);
    assert_eq!(std::fs::read(&prepared.files[1].0).unwrap(), b"rlib bytes");

    let stored = std::fs::read_to_string(&prepared.files[0].0).unwrap();
    assert!(stored.contains("__kache_root__/release/deps/libfoo-abc.rlib"));
    assert!(stored.contains("__kache_cwd__/src/lib.rs"));
    assert!(stored.contains("__kache_workspace__/shared/asset.txt"));
    assert!(!stored.contains(producing_workspace.to_str().unwrap()));

    let restoring_workspace = dir.path().join("worktree-b");
    let restoring_working_dir = restoring_workspace.join("member");
    let restoring_target = restoring_workspace.join("target");
    let restored = link::rewrite_rustc_depinfo_content(
        &stored,
        &restoring_target,
        &restoring_working_dir,
        Some(&restoring_workspace),
        link::DepInfoMode::Expand,
    );
    assert!(
        restored.contains(
            restoring_target
                .join("release/deps/libfoo-abc.rlib")
                .to_str()
                .unwrap()
        )
    );
    assert!(restored.contains(restoring_working_dir.join("src/lib.rs").to_str().unwrap()));
    assert!(
        restored.contains(
            restoring_workspace
                .join("shared/asset.txt")
                .to_str()
                .unwrap()
        )
    );
}

/// Any staging failure skips cache publication without changing an output
/// that was already read successfully.
#[test]
fn rustc_store_staging_refuses_missing_depinfo_without_mutating_outputs() {
    let dir = tempfile::tempdir().unwrap();
    let valid = dir.path().join("valid.d");
    let original = format!(
        "{}/valid: {}/input.rs\n",
        dir.path().display(),
        dir.path().display()
    );
    std::fs::write(&valid, &original).unwrap();
    let outputs = ArtifactSet::new(vec![
        crate::compiler::Artifact {
            path: valid.clone(),
            store_name: "valid.d".to_string(),
            kind: ArtifactKind::DepInfo,
            required: true,
        },
        crate::compiler::Artifact {
            path: dir.path().join("missing.d"),
            store_name: "missing.d".to_string(),
            kind: ArtifactKind::DepInfo,
            required: true,
        },
    ]);
    let error = prepare_rustc_store_files(
        &outputs,
        Some(dir.path()),
        dir.path(),
        Some(dir.path()),
        &[],
    )
    .expect_err("a missing dep-info must prevent cache publication");
    assert!(format!("{error:#}").contains("opening dep-info"));
    assert_eq!(std::fs::read_to_string(valid).unwrap(), original);
}

#[test]
fn cc_cache_entry_requires_depinfo_when_invocation_requests_it() {
    fn meta(names: &[&str]) -> crate::store::EntryMeta {
        crate::store::EntryMeta {
            cache_key: "key".to_string(),
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            crate_name: "foo.c".to_string(),
            crate_types: vec![],
            files: names
                .iter()
                .map(|name| crate::store::CachedFile {
                    name: (*name).to_string(),
                    size: 1,
                    hash: "0123456789abcdef".to_string(),
                    executable: false,
                })
                .collect(),
            stdout: String::new(),
            stderr: String::new(),
            features: vec![],
            target: String::new(),
            profile: String::new(),
            compile_time_ms: 0,
            emit_kinds: Vec::new(),
        }
    }

    let with_depinfo_args: Vec<String> = ["cc", "-c", "foo.c", "-o", "foo.o", "-MMD"]
        .into_iter()
        .map(String::from)
        .collect();
    let with_depinfo = CcCompiler::new().parse(&with_depinfo_args).unwrap();
    assert!(!cc_cache_entry_satisfies_invocation(
        &with_depinfo,
        &meta(&["foo.o"])
    ));
    assert!(cc_cache_entry_satisfies_invocation(
        &with_depinfo,
        &meta(&["foo.o", "foo.d"])
    ));
    assert!(cc_cache_entry_satisfies_invocation(
        &with_depinfo,
        &meta(&["foo.o", "foo.o.pp"])
    ));
    assert!(
        !cc_cache_entry_satisfies_invocation(&with_depinfo, &meta(&["foo.o", "foo.d.tmp"])),
        "a pre-fix raw compound dep-info entry must self-heal, not become trusted"
    );
    assert_eq!(
        cc_cache_entry_rejection_reason(&with_depinfo, &meta(&["foo.o", "foo.d.tmp"])),
        Some("matching entry lacks dep-info required by this invocation")
    );
    assert!(cc_cache_entry_satisfies_invocation(
        &with_depinfo,
        &meta(&["foo.o", crate::compiler::cc::CC_DEPINFO_STORE_NAME])
    ));

    let object_only_args: Vec<String> = ["cc", "-c", "foo.c", "-o", "foo.o"]
        .into_iter()
        .map(String::from)
        .collect();
    let object_only = CcCompiler::new().parse(&object_only_args).unwrap();
    assert!(cc_cache_entry_satisfies_invocation(
        &object_only,
        &meta(&["foo.o", "foo.d"])
    ));
}

/// No dep-info output means there is no safe anchor for `.d` rewriting, so
/// the cc helper must leave the compile output untouched.
#[test]
fn cc_depinfo_rewrite_root_none_without_depinfo_request() {
    let args = s(&["cc", "-c", "foo.c", "-o", "foo.o"]);
    let parsed = CcCompiler::new().parse(&args).unwrap();

    assert_eq!(
        cc_depinfo_rewrite_root_from_cwd(&parsed, Path::new("/work/repo")),
        None
    );
}

#[test]
fn cc_depinfo_rewrite_root_uses_common_source_and_object_root() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("repo");
    let cwd = root.join("obj-kache-bench").join("config");
    let source = root.join("config").join("pathsub.c");
    let args: Vec<String> = vec![
        "cc".to_string(),
        "-c".to_string(),
        source.to_string_lossy().into_owned(),
        "-o".to_string(),
        "host_pathsub.o".to_string(),
        "-MMD".to_string(),
        "-MF".to_string(),
        ".deps/host_pathsub.o.pp".to_string(),
    ];
    let parsed = CcCompiler::new().parse(&args).unwrap();

    assert_eq!(cc_depinfo_rewrite_root_from_cwd(&parsed, &cwd), Some(root));
}

/// When source and object paths only share the filesystem root, the helper
/// falls back to the object anchor rather than relativizing against `/`.
#[cfg(unix)]
#[test]
fn cc_depinfo_rewrite_root_falls_back_to_object_anchor_for_unrelated_paths() {
    let cwd = Path::new("/work/build");
    let source = Path::new("/src-only/foo.c");
    let object_dir = Path::new("/obj-only");
    let object = object_dir.join("foo.o");
    let args = vec![
        "cc".to_string(),
        "-c".to_string(),
        source.to_string_lossy().into_owned(),
        "-o".to_string(),
        object.to_string_lossy().into_owned(),
        "-MMD".to_string(),
    ];
    let parsed = CcCompiler::new().parse(&args).unwrap();

    assert_eq!(
        cc_depinfo_rewrite_root_from_cwd(&parsed, cwd),
        Some(object_dir.to_path_buf())
    );
}

/// Refusal reasons are serialized as `category|detail` for reporting; an
/// empty list keeps the defensive default category with an empty detail.
#[test]
fn refuse_reason_string_formats_category_and_joined_details() {
    use crate::compiler::RefuseReason;

    assert_eq!(refuse_reason_string(&[]), "unsupported|");
    assert_eq!(
        refuse_reason_string(&[
            RefuseReason::Unsupported("first unsupported — not yet"),
            RefuseReason::Unsupported("second unsupported — not yet"),
        ]),
        "unsupported|first unsupported — not yet; second unsupported — not yet"
    );
    assert_eq!(
        refuse_reason_string(&[RefuseReason::NotPrimary]),
        "not-a-compile|query / probe (--print, -vV)"
    );
}

/// A cc restore should skip cached dep-info when this invocation did not
/// request it, and skip unsupported sidecars without needing their blobs.
#[test]
fn restore_cc_from_cache_skips_unrequested_depinfo_and_unknown_artifacts() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let args = s(&["cc", "-c", "foo.c", "-o", "foo.o"]);
    let parsed = CcCompiler::new().parse(&args).unwrap();
    let meta = entry_meta(
        "cc-skip-key",
        vec![
            cached_file("foo.d", "0123456789abcdef"),
            cached_file("readme.txt", "fedcba9876543210"),
        ],
        &[],
    );

    restore_cc_from_cache(&store, &parsed, &meta).unwrap();
}

/// Degenerate cc invocations with no object path fail before blob access,
/// giving callers a clean miss instead of materializing to an unknown path.
/// A preprocess hit has to land the expansion where `-o` asked for it.
/// Nothing else in the restore path knows how to place a `.i`, so if this
/// arm stops firing the caller reports a hit and leaves no output at all.
#[test]
fn restore_cc_from_cache_writes_the_preprocessed_output() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let hash = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
    create_blob(&store, hash, b"# 1 \"unit.c\"\nint expanded;\n");

    let output = dir.path().join("unit.i");
    let output_str = output.to_string_lossy().into_owned();
    let parsed = CcCompiler::new()
        .parse(&s(&["cc", "-E", "unit.c", "-o", &output_str]))
        .unwrap();
    let meta = entry_meta("cc-preprocess-key", vec![cached_file("unit.i", hash)], &[]);

    restore_cc_from_cache(&store, &parsed, &meta).unwrap();
    assert_eq!(
        std::fs::read(&output).unwrap(),
        b"# 1 \"unit.c\"\nint expanded;\n",
        "the cached expansion must reach the path -o named"
    );

    let stdout_parsed = CcCompiler::new()
        .parse(&s(&["cc", "-E", "unit.c"]))
        .unwrap();
    let stdout_meta = entry_meta(
        "cc-stdout-key",
        vec![cached_file(crate::compiler::cc::CC_STDOUT_STORE_NAME, hash)],
        &[],
    );
    let mut restored = Vec::new();
    restore_cc_stdout_from_cache(&store, &stdout_meta, &mut restored).unwrap();
    assert_eq!(
        restored, b"# 1 \"unit.c\"\nint expanded;\n",
        "a stdout -E hit must replay the blob on stdout"
    );
    assert_eq!(
        cc_cache_entry_rejection_reason(&stdout_parsed, &stdout_meta),
        None
    );
    assert_eq!(
        cc_cache_entry_rejection_reason(&stdout_parsed, &meta),
        Some("matching entry lacks the preprocessor stdout artifact required by this invocation")
    );

    // An entry naming some other file is not this invocation's output, and
    // skipping it must not invent one.
    let other = dir.path().join("other.i");
    let other_str = other.to_string_lossy().into_owned();
    let mismatched = CcCompiler::new()
        .parse(&s(&["cc", "-E", "unit.c", "-o", &other_str]))
        .unwrap();
    restore_cc_from_cache(&store, &mismatched, &meta).unwrap();
    assert!(
        !other.exists(),
        "an entry for a different name must leave nothing behind"
    );
}

/// The rule that decides where a cached preprocess artifact goes, and
/// whether it belongs to this invocation at all. Restore and hit
/// qualification both use it, so they cannot disagree.
#[test]
fn cc_preprocess_restore_target_matches_only_the_named_output() {
    use crate::compiler::cc::CcArgs;
    let parse = |args: &[&str]| {
        CcArgs::parse(&args.iter().map(|a| (*a).to_string()).collect::<Vec<_>>()).unwrap()
    };

    let preprocess = parse(&["cc", "-E", "unit.c", "-o", "build/unit.i"]);
    assert_eq!(
        cc_preprocess_restore_target(&preprocess, "unit.i"),
        Some(std::path::PathBuf::from("build/unit.i")),
        "the cached name is the file this invocation asked for"
    );
    assert_eq!(
        cc_preprocess_restore_target(&preprocess, "other.i"),
        None,
        "an entry naming a different file must not be written here"
    );
    assert_eq!(
        cc_preprocess_restore_target(&preprocess, "unit.o"),
        None,
        "an object is not this invocation's output"
    );

    // Every other mode is somebody else's business, whatever the name.
    for args in [
        ["cc", "-c", "unit.c", "-o", "unit.i"].as_slice(),
        ["cc", "unit.c", "-o", "unit.i"].as_slice(),
    ] {
        let other = parse(args);
        assert_eq!(
            cc_preprocess_restore_target(&other, "unit.i"),
            None,
            "{args:?} is not a preprocess and must not take this path"
        );
    }
}

/// A preprocess entry qualifies on the file the invocation asked for,
/// not on an object it was never going to produce. Getting this wrong
/// reports a hit and leaves the build without its output.
#[test]
fn cc_entry_qualification_accepts_a_preprocess_output() {
    use crate::compiler::cc::CcArgs;
    let args: Vec<String> = ["cc", "-E", "unit.c", "-o", "unit.i"]
        .iter()
        .map(|a| (*a).to_string())
        .collect();
    let parsed = CcArgs::parse(&args).unwrap();

    let entry = |name: &str| entry_meta_with_files(&[name]);

    assert_eq!(
        cc_cache_entry_rejection_reason(&parsed, &entry("unit.i")),
        None,
        "the named expansion is what this invocation needs"
    );
    assert_eq!(
        cc_cache_entry_rejection_reason(&parsed, &entry("other.i")),
        Some("matching entry lacks the preprocessed output required by this invocation"),
        "an entry naming a different output cannot serve this one"
    );
    assert_eq!(
        cc_cache_entry_rejection_reason(&parsed, &entry("unit.o")),
        Some("matching entry lacks the preprocessed output required by this invocation"),
        "an object is not a preprocess output"
    );

    // And an ordinary compile still requires its object.
    let compile_args: Vec<String> = ["cc", "-c", "unit.c", "-o", "unit.o"]
        .iter()
        .map(|a| (*a).to_string())
        .collect();
    let compile = CcArgs::parse(&compile_args).unwrap();
    assert_eq!(
        cc_cache_entry_rejection_reason(&compile, &entry("unit.o")),
        None
    );
    assert_eq!(
        cc_cache_entry_rejection_reason(&compile, &entry("unit.i")),
        Some("matching entry lacks the object artifact required by this invocation")
    );

    let link_args: Vec<String> = ["cc", "a.o", "b.o", "-o", "prog"]
        .iter()
        .map(|a| (*a).to_string())
        .collect();
    let link = CcArgs::parse(&link_args).unwrap();
    assert_eq!(
        cc_cache_entry_rejection_reason(&link, &entry_meta_with_files(&[])),
        Some("matching entry lacks the link artifact required by this invocation"),
        "an empty link entry cannot serve the binary"
    );
    assert_eq!(
        cc_cache_entry_rejection_reason(&link, &entry("prog")),
        None,
        "any stored file is enough for a link hit"
    );
    assert!(
        cc_store_revalidates_include_dirs(crate::compiler::cc::CompileMode::Compile),
        "object compiles re-check include-dir names before store"
    );
    assert!(
        !cc_store_revalidates_include_dirs(crate::compiler::cc::CompileMode::Link),
        "links have no include-dir snapshot and must still store"
    );
}

#[test]
fn restore_cc_from_cache_writes_the_link_binary_and_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let bin_hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let map_hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    create_blob(&store, bin_hash, b"ELF");
    create_blob(&store, map_hash, b"MAP");

    let output = dir.path().join("out");
    let output_str = output.to_string_lossy().into_owned();
    let parsed = CcCompiler::new()
        .parse(&s(&["cc", "a.o", "b.o", "-o", &output_str]))
        .unwrap();
    let meta = entry_meta(
        "cc-link-key",
        vec![
            cached_file("app", bin_hash),
            cached_file("app.map", map_hash),
        ],
        &[],
    );

    restore_cc_from_cache(&store, &parsed, &meta).unwrap();
    assert_eq!(
        std::fs::read(&output).unwrap(),
        b"ELF",
        "the primary link artifact must land at -o even when the stored name differs"
    );
    assert!(
        !dir.path().join("app").exists(),
        "the stored extensionless name is not the restore destination"
    );
    assert_eq!(
        std::fs::read(dir.path().join("app.map")).unwrap(),
        b"MAP",
        "link sidecars must land next to the binary under their stored name"
    );

    let object = dir.path().join("unit.o");
    let object_str = object.to_string_lossy().into_owned();
    let compile = CcCompiler::new()
        .parse(&s(&["cc", "-c", "unit.c", "-o", &object_str]))
        .unwrap();
    restore_cc_from_cache(&store, &compile, &meta).unwrap();
    assert!(
        !object.exists(),
        "an executable blob must not restore onto a compile -o path"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_ne!(
            std::fs::metadata(&output).unwrap().permissions().mode() & 0o100,
            0,
            "restored link outputs must be owner-executable"
        );
    }
}

#[test]
fn restore_cc_from_cache_signs_an_exe_and_a_dylib() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let exe_hash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    let so_hash = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    create_blob(&store, exe_hash, b"MZ exe");
    create_blob(&store, so_hash, b"\x7fELF so");

    let exe = dir.path().join("out.exe");
    let exe_str = exe.to_string_lossy().into_owned();
    let parsed = CcCompiler::new()
        .parse(&s(&["cc", "a.o", "-o", &exe_str]))
        .unwrap();
    restore_cc_from_cache(
        &store,
        &parsed,
        &entry_meta("cc-exe-key", vec![cached_file("app.exe", exe_hash)], &[]),
    )
    .unwrap();
    assert_eq!(std::fs::read(&exe).unwrap(), b"MZ exe");

    let dylib = dir.path().join("libfoo.so");
    let dylib_str = dylib.to_string_lossy().into_owned();
    let parsed = CcCompiler::new()
        .parse(&s(&["cc", "a.o", "-o", &dylib_str]))
        .unwrap();
    restore_cc_from_cache(
        &store,
        &parsed,
        &entry_meta("cc-so-key", vec![cached_file("libfoo.so", so_hash)], &[]),
    )
    .unwrap();
    assert_eq!(std::fs::read(&dylib).unwrap(), b"\x7fELF so");
}

#[test]
fn prepare_cc_store_files_tars_a_dsym_directory_and_restore_unpacks_it() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();

    let dsym = dir.path().join("prog.dSYM");
    std::fs::create_dir_all(dsym.join("Contents/Resources/DWARF")).unwrap();
    std::fs::write(dsym.join("Contents/Resources/DWARF/prog"), b"dwarf-bytes").unwrap();

    let artifacts = ArtifactSet::new(vec![crate::compiler::Artifact {
        path: dsym,
        kind: ArtifactKind::DebugBundle,
        store_name: "prog.dsym.tar".to_string(),
        required: false,
    }]);
    let prepared = prepare_cc_store_files(&artifacts, None).unwrap();
    assert_eq!(prepared.files.len(), 1);
    assert_eq!(prepared.files[0].1, "prog.dsym.tar");
    let tar_bytes = std::fs::read(&prepared.files[0].0).unwrap();
    assert!(
        !tar_bytes.is_empty(),
        "a .dSYM directory must be stored as a tar, not copied as a file"
    );

    let tar_hash = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    let bin_hash = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    create_blob(&store, tar_hash, &tar_bytes);
    create_blob(&store, bin_hash, b"ELF");
    let output = dir.path().join("build").join("prog");
    std::fs::create_dir_all(output.parent().unwrap()).unwrap();
    let output_str = output.to_string_lossy().into_owned();
    let parsed = CcCompiler::new()
        .parse(&s(&["cc", "a.o", "-o", &output_str]))
        .unwrap();
    restore_cc_from_cache(
        &store,
        &parsed,
        &entry_meta(
            "cc-dsym-key",
            vec![
                cached_file("prog", bin_hash),
                cached_file("prog.dsym.tar", tar_hash),
            ],
            &[],
        ),
    )
    .unwrap();
    assert_eq!(std::fs::read(&output).unwrap(), b"ELF");

    let tar_path = dir.path().join("build").join("prog.dsym.tar");
    assert!(tar_path.is_file(), "the bundle tar itself must be restored");
    let dwarf = dir
        .path()
        .join("build")
        .join("prog.dSYM/Contents/Resources/DWARF/prog");
    assert_eq!(std::fs::read(&dwarf).unwrap(), b"dwarf-bytes");
}

#[test]
fn prepare_cc_store_files_copies_an_already_packed_dsym_tar() {
    let dir = tempfile::tempdir().unwrap();
    let tar_path = dir.path().join("prog.dsym.tar");
    std::fs::write(&tar_path, b"already-packed-tar").unwrap();
    let artifacts = ArtifactSet::new(vec![crate::compiler::Artifact {
        path: tar_path,
        kind: ArtifactKind::DebugBundle,
        store_name: "prog.dsym.tar".to_string(),
        required: false,
    }]);
    let prepared = prepare_cc_store_files(&artifacts, None).unwrap();
    assert_eq!(
        std::fs::read(&prepared.files[0].0).unwrap(),
        b"already-packed-tar",
        "a DebugBundle that is already a tar file must be copied, not re-tarred as a directory"
    );
}

#[test]
fn restore_cc_from_cache_requires_object_output_for_object_blob() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let args = s(&["cc", "-c"]);
    let parsed = CcCompiler::new().parse(&args).unwrap();
    let meta = entry_meta(
        "cc-object-key",
        vec![cached_file("foo.o", "0123456789abcdef")],
        &[],
    );

    let err = restore_cc_from_cache(&store, &parsed, &meta)
        .unwrap_err()
        .to_string();

    assert!(
        err.contains("cannot determine object output path"),
        "unexpected error: {err}"
    );
}

#[cfg(unix)]
#[test]
fn restore_cc_object_is_writable_private_and_keeps_blob_immutable() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let hash = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    create_blob(&store, hash, b"cached object");
    std::fs::set_permissions(
        store.blob_path(hash),
        std::fs::Permissions::from_mode(0o400),
    )
    .unwrap();

    let output = dir.path().join("output.o");
    let output_str = output.to_string_lossy().into_owned();
    let parsed = CcCompiler::new()
        .parse(&s(&["cc", "-c", "foo.c", "-o", &output_str]))
        .unwrap();
    let meta = entry_meta("cc-private-key", vec![cached_file("foo.o", hash)], &[]);

    restore_cc_from_cache(&store, &parsed, &meta).unwrap();

    let output_meta = std::fs::metadata(&output).unwrap();
    let blob_meta = std::fs::metadata(store.blob_path(hash)).unwrap();
    assert_ne!(output_meta.permissions().mode() & 0o200, 0);
    assert_eq!(output_meta.permissions().mode() & 0o111, 0);
    assert_ne!(output_meta.ino(), blob_meta.ino());
    std::fs::write(&output, b"changed").unwrap();
    assert_eq!(
        std::fs::read(store.blob_path(hash)).unwrap(),
        b"cached object"
    );
    assert!(blob_meta.permissions().readonly());
}

#[test]
fn restore_cc_from_cache_replaces_existing_plain_object() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let hash = "abababababababababababababababababababababababababababababababab";
    create_blob(&store, hash, b"cached object");

    let output = dir.path().join("output.o");
    std::fs::write(&output, b"stale object").unwrap();
    let output_str = output.to_string_lossy().into_owned();
    let parsed = CcCompiler::new()
        .parse(&s(&["cc", "-c", "foo.c", "-o", &output_str]))
        .unwrap();
    let meta = entry_meta("cc-replace-key", vec![cached_file("foo.o", hash)], &[]);

    restore_cc_from_cache(&store, &parsed, &meta).unwrap();

    assert_eq!(std::fs::read(&output).unwrap(), b"cached object");
    assert_eq!(
        std::fs::read(store.blob_path(hash)).unwrap(),
        b"cached object"
    );
}

#[test]
fn cc_restore_revalidates_existing_target_before_replacing_it() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("output.o");
    std::fs::write(&output, b"race winner").unwrap();
    let prepared =
        vec![link::prepare_writable_target_from_bytes(&output, b"cached object").unwrap()];

    let error = publish_prepared_cc_artifacts_with(prepared, |_, target| {
        let mut permissions = std::fs::metadata(target)?.permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(target, permissions)?;
        Ok(())
    })
    .unwrap_err();

    assert!(error.to_string().contains("requires compiler passthrough"));
    assert_eq!(std::fs::read(&output).unwrap(), b"race winner");

    // TempDir cleanup cannot remove a read-only Windows file.
    #[cfg(windows)]
    {
        let mut permissions = std::fs::metadata(&output).unwrap().permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        permissions.set_readonly(false);
        std::fs::set_permissions(&output, permissions).unwrap();
    }
}

#[test]
fn cc_restore_marks_partial_publication_and_preserves_race_winner() {
    let dir = tempfile::tempdir().unwrap();
    let object = dir.path().join("foo.o");
    let depinfo = dir.path().join("foo.d");
    let prepared = vec![
        link::prepare_writable_target_from_bytes(&object, b"cached object").unwrap(),
        link::prepare_writable_target_from_bytes(&depinfo, b"cached depinfo").unwrap(),
    ];

    let error = publish_prepared_cc_artifacts_with(prepared, |index, target| {
        if index == 1 {
            std::fs::write(target, b"race winner")?;
        }
        Ok(())
    })
    .unwrap_err();

    assert!(
        error.downcast_ref::<PartialCcRestore>().is_some(),
        "{error:#}"
    );
    assert_eq!(
        error.to_string(),
        "cc cache restore published only part of the output set"
    );
    assert_eq!(std::fs::read(&object).unwrap(), b"cached object");
    assert_eq!(std::fs::read(&depinfo).unwrap(), b"race winner");
}

#[test]
fn cc_restore_classifies_hook_failures_by_publication_progress() {
    let dir = tempfile::tempdir().unwrap();
    let first_target = dir.path().join("first.o");
    let first = vec![link::prepare_writable_target_from_bytes(&first_target, b"first").unwrap()];
    let first_error = publish_prepared_cc_artifacts_with(first, |_, _| {
        anyhow::bail!("fail before first publication")
    })
    .unwrap_err();

    assert!(first_error.downcast_ref::<PartialCcRestore>().is_none());
    assert!(!first_target.exists());

    let object = dir.path().join("object.o");
    let depinfo = dir.path().join("object.d");
    let prepared = vec![
        link::prepare_writable_target_from_bytes(&object, b"cached object").unwrap(),
        link::prepare_writable_target_from_bytes(&depinfo, b"cached depinfo").unwrap(),
    ];
    let later_error = publish_prepared_cc_artifacts_with(prepared, |index, _| {
        if index == 1 {
            anyhow::bail!("fail after first publication");
        }
        Ok(())
    })
    .unwrap_err();

    assert!(
        later_error.downcast_ref::<PartialCcRestore>().is_some(),
        "{later_error:#}"
    );
    assert_eq!(std::fs::read(&object).unwrap(), b"cached object");
    assert!(!depinfo.exists());
}

/// Regression for #645: cache restore must not choose symlink semantics on
/// the compiler's behalf. GCC writes through this path while some clang
/// versions replace it, so the wrapper must refuse the hit and passthrough.
#[cfg(unix)]
#[test]
fn restore_cc_from_cache_refuses_symlinked_object_output() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let hash = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    create_blob(&store, hash, b"cached object");

    let target = dir.path().join("real.o");
    let output = dir.path().join("link.o");
    std::fs::write(&target, b"original").unwrap();
    symlink(&target, &output).unwrap();

    let output_str = output.to_string_lossy().into_owned();
    let args = s(&["cc", "-c", "foo.c", "-o", &output_str]);
    let parsed = CcCompiler::new().parse(&args).unwrap();
    let meta = entry_meta("cc-symlink-key", vec![cached_file("foo.o", hash)], &[]);

    let err = restore_cc_from_cache(&store, &parsed, &meta)
        .unwrap_err()
        .to_string();

    assert!(err.contains("requires compiler passthrough"), "{err}");
    assert!(
        std::fs::symlink_metadata(&output)
            .unwrap()
            .file_type()
            .is_symlink(),
        "refused cache restore must leave the -o symlink in place"
    );
    assert_eq!(std::fs::read(&target).unwrap(), b"original");
    assert_eq!(
        std::fs::read(store.blob_path(hash)).unwrap(),
        b"cached object",
        "refusing the hit must not mutate the cache blob"
    );
}

/// Missing store blobs are surfaced as restore misses, which lets callers
/// recompile instead of serving a partial cache hit.
#[test]
fn materialize_cached_artifact_reports_missing_blob_as_cache_miss() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let cached = cached_file("libfoo.rlib", "0123456789abcdef");
    let target = dir.path().join("target").join("libfoo.rlib");
    let platform = platform::current();

    let err = materialize_cached_artifact(
        &store,
        &cached,
        &target,
        ArtifactKind::Library,
        None,
        dir.path(),
        dir.path(),
        None,
        &[],
        &*platform,
        "test restore",
        None,
    )
    .unwrap_err()
    .to_string();

    assert!(
        err.contains("was evicted before restore"),
        "unexpected error: {err}"
    );
}

/// Dep-info blobs are transformed before materialization so the store blob
/// stays rooted at the producing build while the target is restored here.
#[test]
fn materialize_cached_artifact_expands_depinfo_blob_without_rewriting_store_blob() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let stored = "__kache_root__/debug/deps/libfoo.rlib: __kache_cwd__/src/lib.rs\n";
    create_blob(&store, hash, stored.as_bytes());
    let cached = cached_file("foo.d", hash);
    let target = dir
        .path()
        .join("target")
        .join("debug")
        .join("deps")
        .join("foo.d");
    let anchor = dir.path().join("target");
    let platform = platform::current();

    materialize_cached_artifact(
        &store,
        &cached,
        &target,
        ArtifactKind::DepInfo,
        None,
        &anchor,
        dir.path(),
        None,
        &[],
        &*platform,
        "test restore",
        None,
    )
    .unwrap();

    let restored = std::fs::read_to_string(&target).unwrap();
    assert!(
        restored.starts_with(&format!(
            "{}{}debug/deps/libfoo.rlib:",
            anchor.display(),
            std::path::MAIN_SEPARATOR
        )),
        "dep-info should be expanded at restore anchor, got: {restored}"
    );
    assert!(
        restored.contains(&format!(
            "{}{}src/lib.rs",
            dir.path().display(),
            std::path::MAIN_SEPARATOR
        )),
        "dep-info source should be expanded at the consumer cwd: {restored}"
    );
    assert_eq!(
        std::fs::read_to_string(store.blob_path(hash)).unwrap(),
        stored,
        "content transforms must not mutate the store blob"
    );
}

/// A `[[test]] harness = false` target is compiled without `--test` and
/// without `--crate-type`, so its extensionless output classifies as
/// `Other("rustc:unknown")` — the compile context simply never says
/// "executable". The mode bit recorded at insert time does, and restore
/// must honour it: otherwise the restored test binary comes back 0o644 and
/// cargo fails the run with "Permission denied (os error 13)".
#[cfg(unix)]
#[test]
fn materialize_cached_artifact_restores_executable_bit_recorded_at_insert() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    create_blob(&store, hash, b"\x7fELF harness=false test binary");
    // Store blobs are read-only and carry no executable bit, so a restore
    // that never chmods cannot produce a runnable file.
    std::fs::set_permissions(
        store.blob_path(hash),
        std::fs::Permissions::from_mode(0o444),
    )
    .unwrap();

    let mut cached = cached_file("harness-a1b2c3d4e5f60718", hash);
    cached.executable = true;
    let target = dir.path().join("target").join("harness-a1b2c3d4e5f60718");
    let platform = platform::current();

    materialize_cached_artifact(
        &store,
        &cached,
        &target,
        ArtifactKind::Other("rustc:unknown"),
        None,
        dir.path(),
        dir.path(),
        None,
        &[],
        &*platform,
        "test restore",
        None,
    )
    .unwrap();

    let mode = std::fs::metadata(&target).unwrap().permissions().mode();
    assert_ne!(
        mode & 0o111,
        0,
        "restored test binary must stay executable, got {mode:o}"
    );
}

/// The converse: an artifact that was not executable at insert time must
/// not acquire the bit on restore.
#[cfg(unix)]
#[test]
fn materialize_cached_artifact_leaves_non_executable_artifacts_unexecutable() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let hash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    create_blob(&store, hash, b"rlib bytes");

    let cached = cached_file("libfoo.rlib", hash);
    let target = dir.path().join("target").join("libfoo.rlib");
    let platform = platform::current();

    materialize_cached_artifact(
        &store,
        &cached,
        &target,
        ArtifactKind::Library,
        None,
        dir.path(),
        dir.path(),
        None,
        &[],
        &*platform,
        "test restore",
        None,
    )
    .unwrap();

    let mode = std::fs::metadata(&target).unwrap().permissions().mode();
    assert_eq!(
        mode & 0o111,
        0,
        "library must not become executable, got {mode:o}"
    );
}

// ── shared-inode restores of proc-macros and build scripts ─────────

#[test]
fn restore_link_strategy_shares_only_the_named_loadable_kind() {
    use link::LinkStrategy::{Copy, ExecutableHardlink, Hardlink};
    let proc_macro = Some(ArtifactKind::DynamicLibrary);
    let build_script = Some(ArtifactKind::Executable);
    assert_eq!(
        restore_link_strategy(ArtifactKind::DynamicLibrary, true, proc_macro),
        ExecutableHardlink
    );
    assert_eq!(
        restore_link_strategy(ArtifactKind::Executable, true, build_script),
        ExecutableHardlink
    );
    // The rest of the same compile keeps its ordinary strategy.
    assert_eq!(
        restore_link_strategy(ArtifactKind::DepInfo, false, proc_macro),
        Hardlink
    );
    assert_eq!(
        restore_link_strategy(ArtifactKind::Executable, true, proc_macro),
        Copy
    );
    // Without the gate every executable is a private copy.
    assert_eq!(
        restore_link_strategy(ArtifactKind::DynamicLibrary, true, None),
        Copy
    );
    assert_eq!(
        restore_link_strategy(ArtifactKind::Other("rustc:unknown"), true, None),
        Copy
    );
    assert_eq!(
        restore_link_strategy(ArtifactKind::Library, false, None),
        Hardlink
    );
}

#[test]
fn only_proc_macros_and_build_scripts_may_share_a_restored_inode() {
    let compile = |crate_name: &str, crate_type: &str, out_dir: &str| {
        rustc_args(&[
            "rustc",
            "--crate-name",
            crate_name,
            "--crate-type",
            crate_type,
            "src/lib.rs",
            "--out-dir",
            out_dir,
            "-C",
            "extra-filename=-1",
        ])
    };
    let proc_macro = compile("serde_derive", "proc-macro", "/t/debug/deps");
    let build_script = compile("build_script_build", "bin", "/t/debug/build/pkg-1");
    assert_eq!(
        shared_inode_loadable(&proc_macro, true),
        Some(ArtifactKind::DynamicLibrary)
    );
    assert_eq!(
        shared_inode_loadable(&build_script, true),
        Some(ArtifactKind::Executable)
    );
    // `strip` may rewrite these in place after the build.
    for (label, other) in [
        ("user bin", compile("hk", "bin", "/t/debug/deps")),
        ("cdylib", compile("plugin", "cdylib", "/t/debug/deps")),
        ("dylib", compile("shared", "dylib", "/t/debug/deps")),
        ("lib", compile("serde", "lib", "/t/debug/deps")),
        (
            "bin named like a build script",
            compile("build_script_x", "bin", "/t/debug/deps"),
        ),
        (
            "test harness",
            rustc_args(&["rustc", "--crate-name", "hk", "--test", "src/main.rs"]),
        ),
    ] {
        assert_eq!(shared_inode_loadable(&other, true), None, "{label}");
    }
    // Platforms that may rewrite a restored loadable share nothing.
    assert_eq!(shared_inode_loadable(&proc_macro, false), None);
    assert_eq!(shared_inode_loadable(&build_script, false), None);
}

/// The Linux restore path for a proc-macro: the file comes back loadable,
/// the mtime stamp works on a read-only shared inode, and the blob keeps
/// its bytes and mode. Without reflink the file shares the blob's inode.
#[cfg(unix)]
#[test]
fn materialize_shared_loadable_restores_a_loadable_file_and_leaves_the_blob() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let hash = "5555555555555555555555555555555555555555555555555555555555555555";
    create_blob(&store, hash, b"proc-macro dylib");
    let blob = store.blob_path(hash);
    std::fs::set_permissions(&blob, std::fs::Permissions::from_mode(0o555)).unwrap();
    let reflinks = crate::link::try_reflink(&blob, &dir.path().join("probe")).is_ok();

    let mut cached = cached_file("libfoo_macros-1.so", hash);
    cached.executable = true;
    let target = dir.path().join("target/debug/deps/libfoo_macros-1.so");
    let restored = materialize_cached_artifact(
        &store,
        &cached,
        &target,
        ArtifactKind::DynamicLibrary,
        Some(ArtifactKind::DynamicLibrary),
        dir.path(),
        dir.path(),
        None,
        &[],
        &crate::compiler::platform::LinuxPlatform,
        "test restore",
        None,
    )
    .unwrap();

    assert!(matches!(restored, RestoredBytes::ExactBlobCopy(_)));
    let restored_meta = std::fs::metadata(&target).unwrap();
    assert_eq!(restored_meta.permissions().mode() & 0o111, 0o111);
    assert_eq!(std::fs::read(&target).unwrap(), b"proc-macro dylib");
    let blob_meta = std::fs::metadata(&blob).unwrap();
    assert_eq!(blob_meta.permissions().mode() & 0o777, 0o555);
    assert_eq!(std::fs::read(&blob).unwrap(), b"proc-macro dylib");
    assert_eq!(restored_meta.ino() == blob_meta.ino(), !reflinks);
}

/// End to end on Linux without reflink: a proc-macro hit shares the
/// blob's inode, while a user binary restored the same way stays a
/// private, writable copy that `strip` cannot turn into a blob write.
#[cfg(target_os = "linux")]
#[test]
fn restore_from_cache_shares_a_proc_macro_inode_but_copies_a_user_binary() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let out_dir = dir.path().join("target/debug/deps");
    let out_dir_str = out_dir.to_str().unwrap();
    let restore = |crate_name: &str, crate_type: &str, file: &str, hash: &str| {
        create_blob(&store, hash, b"linked output");
        let blob = store.blob_path(hash);
        std::fs::set_permissions(&blob, std::fs::Permissions::from_mode(0o555)).unwrap();
        let args = rustc_args(&[
            "rustc",
            "--crate-name",
            crate_name,
            "--crate-type",
            crate_type,
            "src/lib.rs",
            "--emit",
            "link",
            "--out-dir",
            out_dir_str,
            "-C",
            "extra-filename=-1",
        ]);
        let mut cached = cached_file(file, hash);
        cached.executable = true;
        let meta = entry_meta(crate_name, vec![cached], &["link"]);
        restore_from_cache(&config, &RustcCompiler::new(), &store, &args, &meta, None).unwrap();
        (blob, out_dir.join(file))
    };

    let (macro_blob, macro_target) = restore(
        "foo_macros",
        "proc-macro",
        "libfoo_macros-1.so",
        "6666666666666666666666666666666666666666666666666666666666666666",
    );
    if crate::link::try_reflink(&macro_blob, &dir.path().join("probe")).is_ok() {
        eprintln!("reflink available; the no-CoW restore path is not reachable here");
        return;
    }
    let ino = |path: &Path| std::fs::metadata(path).unwrap().ino();
    assert_eq!(ino(&macro_target), ino(&macro_blob));

    let (bin_blob, bin_target) = restore(
        "hk",
        "bin",
        "hk-1",
        "7777777777777777777777777777777777777777777777777777777777777777",
    );
    assert_ne!(ino(&bin_target), ino(&bin_blob));
    let mode = std::fs::metadata(&bin_target).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o755);
}

// ── restored-blob digest reuse (kunobi-ninja/kache#540) ──────────

/// A plain library restore is a verbatim blob copy, so the entry's recorded
/// digest still describes the file on disk and may be reused as its hash.
#[test]
fn materialize_reports_a_plain_library_restore_as_an_exact_blob_copy() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let hash = "1111111111111111111111111111111111111111111111111111111111111111";
    create_blob(&store, hash, b"rlib bytes");
    let cached = cached_file("libfoo.rlib", hash);
    let target = dir.path().join("target").join("libfoo.rlib");
    let platform = platform::current();

    let restored = materialize_cached_artifact(
        &store,
        &cached,
        &target,
        ArtifactKind::Library,
        None,
        dir.path(),
        dir.path(),
        None,
        &[],
        &*platform,
        "test restore",
        None,
    )
    .unwrap();

    assert!(matches!(restored, RestoredBytes::ExactBlobCopy(_)));
    assert_eq!(std::fs::read(&target).unwrap(), b"rlib bytes");
}

/// Dep-info is re-rooted for this consumer on the way out of the store, so
/// what lands on disk is not what the blob's digest describes.
#[test]
fn materialize_reports_a_rewritten_depinfo_restore_as_not_exact() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let hash = "2222222222222222222222222222222222222222222222222222222222222222";
    create_blob(
        &store,
        hash,
        b"__kache_root__/debug/deps/libfoo.rlib: __kache_cwd__/src/lib.rs\n",
    );
    let cached = cached_file("foo.d", hash);
    let target = dir.path().join("target").join("foo.d");
    let platform = platform::current();

    let restored = materialize_cached_artifact(
        &store,
        &cached,
        &target,
        ArtifactKind::DepInfo,
        None,
        &dir.path().join("target"),
        dir.path(),
        None,
        &[],
        &*platform,
        "test restore",
        None,
    )
    .unwrap();

    assert_eq!(restored, RestoredBytes::Rewritten);
}

/// An external post-restore action that leaves the file alone — every
/// platform but macOS-arm64 signing, plus macOS when the existing signature
/// is still valid — keeps the restore exact.
#[test]
fn materialize_reports_an_untouched_external_action_as_an_exact_blob_copy() {
    use crate::compiler::platform::tests::CountingPlatform;

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let hash = "3333333333333333333333333333333333333333333333333333333333333333";
    create_blob(&store, hash, b"\x7fELF proc-macro");
    let cached = cached_file("libmac.so", hash);
    let target = dir.path().join("target").join("libmac.so");
    let platform = CountingPlatform::new();

    let restored = materialize_cached_artifact(
        &store,
        &cached,
        &target,
        ArtifactKind::DynamicLibrary,
        None,
        dir.path(),
        dir.path(),
        None,
        &[],
        &platform,
        "test restore",
        None,
    )
    .unwrap();

    assert_eq!(platform.ensure_calls(), 1, "signing hook should have run");
    assert!(matches!(restored, RestoredBytes::ExactBlobCopy(_)));
}

/// A restored artifact that is overwritten before the seed lands must not
/// hand the new bytes the old blob's digest. Seeding records the
/// fingerprint observed at restore, so the overwritten file misses the memo
/// and is hashed for real; re-stating the path at seed time instead would
/// pair the new file's fingerprint with the old file's hash.
#[test]
fn seeding_does_not_attach_the_blobs_digest_to_a_file_overwritten_since_restore() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();

    let content = vec![b'r'; 128 * 1024];
    let source = dir.path().join("source.rlib");
    std::fs::write(&source, &content).unwrap();
    let hash = crate::cache_key::hash_file(&source).unwrap();
    create_blob(&store, &hash, &content);

    let cached = cached_file("libfoo.rlib", &hash);
    let target = dir.path().join("target").join("libfoo.rlib");
    let platform = platform::current();

    let RestoredBytes::ExactBlobCopy(fingerprint) = materialize_cached_artifact(
        &store,
        &cached,
        &target,
        ArtifactKind::Library,
        None,
        dir.path(),
        dir.path(),
        None,
        &[],
        &*platform,
        "test restore",
        None,
    )
    .unwrap() else {
        panic!("a plain library restore should be exact");
    };

    // Someone else lands on the same output path before we get to record.
    let replacement = vec![b'z'; 256 * 1024];
    std::fs::remove_file(&target).unwrap();
    std::fs::write(&target, &replacement).unwrap();

    record_known_file_hashes(&store, &[(fingerprint, hash.as_str())]);

    assert!(
        matches!(
            store.file_hash_lookup(&target),
            crate::cache_key::FileHashLookup::NeedsHash(_)
        ),
        "the overwritten file must not inherit the restored blob's digest"
    );
    assert_ne!(
        store.file_hasher().hash(&target).unwrap(),
        hash,
        "hashing the overwritten file must return its own digest"
    );
}

/// The guard that matters: an external tool that DOES rewrite the artifact
/// (macOS re-signing an invalidated binary) leaves bytes the entry's digest
/// no longer describes, so the restore must not be reported as exact.
#[test]
fn materialize_reports_a_mutating_external_action_as_not_exact() {
    /// Stands in for `codesign` re-signing a restored binary.
    struct RewritingPlatform(PathBuf);
    impl crate::compiler::Platform for RewritingPlatform {
        fn name(&self) -> &'static str {
            "rewriting"
        }
        fn ensure_binary_loadable(&self, path: &Path) -> Result<crate::compiler::Loadability> {
            let mut content = std::fs::read(path)?;
            content.extend_from_slice(b"signature");
            std::fs::write(path, content)?;
            // Claims a pass, so only the rewrite keeps it from the memo.
            Ok(crate::compiler::Loadability::Verified)
        }
        fn verified_loadable_dir(&self) -> Option<PathBuf> {
            Some(self.0.clone())
        }
        fn package_debug_bundle(
            &self,
            _binary: &Path,
            _staging_dir: &Path,
        ) -> Result<Option<PathBuf>> {
            Ok(None)
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let hash = "4444444444444444444444444444444444444444444444444444444444444444";
    create_blob(&store, hash, b"\x7fELF unsigned");
    let cached = cached_file("libmac.so", hash);
    let target = dir.path().join("target").join("libmac.so");

    let restored = materialize_cached_artifact(
        &store,
        &cached,
        &target,
        ArtifactKind::DynamicLibrary,
        None,
        dir.path(),
        dir.path(),
        None,
        &[],
        &RewritingPlatform(dir.path().join("verified")),
        "test restore",
        None,
    )
    .unwrap();

    assert_eq!(restored, RestoredBytes::Rewritten);
    assert_eq!(
        std::fs::read(&target).unwrap(),
        b"\x7fELF unsignedsignature",
        "the double should have rewritten the restored artifact"
    );
    assert!(
        !dir.path().join("verified").join(hash).exists(),
        "a check that changed the file proves nothing about the blob"
    );
}

/// Restore `hash` as a dynamic library at `target` through `platform`.
fn restore_loadable(
    store: &Store,
    hash: &str,
    target: &Path,
    platform: &dyn crate::compiler::Platform,
) -> RestoredBytes {
    let anchor = target.parent().unwrap();
    materialize_cached_artifact(
        store,
        &cached_file("libmac.so", hash),
        target,
        ArtifactKind::DynamicLibrary,
        None,
        anchor,
        anchor,
        None,
        &[],
        platform,
        "test restore",
        None,
    )
    .unwrap()
}

/// A blob restored unchanged that passed the check once is not checked
/// again on this host, and both restores stay exact.
#[test]
fn a_blob_that_passed_the_loadability_check_is_not_checked_again() {
    use crate::compiler::platform::tests::CountingPlatform;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&test_config(dir.path().join("cache"))).unwrap();
    let hash = "5555555555555555555555555555555555555555555555555555555555555555";
    create_blob(&store, hash, b"\x7fELF signed");
    let memo = dir.path().join("verified");
    let platform = CountingPlatform::verifying_into(memo.clone());

    let first = restore_loadable(&store, hash, &dir.path().join("a/libmac.so"), &platform);
    assert_eq!(platform.ensure_calls(), 1);
    assert!(
        memo.join(hash).is_file(),
        "the pass is remembered by blob hash"
    );

    let second = restore_loadable(&store, hash, &dir.path().join("b/libmac.so"), &platform);
    assert_eq!(
        platform.ensure_calls(),
        1,
        "the second restore skips the check"
    );
    assert!(matches!(first, RestoredBytes::ExactBlobCopy(_)));
    assert!(matches!(second, RestoredBytes::ExactBlobCopy(_)));
}

/// Only a pass is remembered: a host that proved nothing checks every time.
#[test]
fn an_unproven_loadability_check_runs_on_every_restore() {
    struct Unproven(PathBuf, std::sync::atomic::AtomicUsize);
    impl crate::compiler::Platform for Unproven {
        fn name(&self) -> &'static str {
            "unproven"
        }
        fn ensure_binary_loadable(&self, _path: &Path) -> Result<crate::compiler::Loadability> {
            self.1.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(crate::compiler::Loadability::Unverified)
        }
        fn verified_loadable_dir(&self) -> Option<PathBuf> {
            Some(self.0.clone())
        }
        fn package_debug_bundle(
            &self,
            _binary: &Path,
            _staging_dir: &Path,
        ) -> Result<Option<PathBuf>> {
            Ok(None)
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&test_config(dir.path().join("cache"))).unwrap();
    let hash = "6666666666666666666666666666666666666666666666666666666666666666";
    create_blob(&store, hash, b"\x7fELF unchecked");
    let platform = Unproven(dir.path().join("verified"), Default::default());

    restore_loadable(&store, hash, &dir.path().join("a/libmac.so"), &platform);
    restore_loadable(&store, hash, &dir.path().join("b/libmac.so"), &platform);
    assert_eq!(platform.1.load(std::sync::atomic::Ordering::Relaxed), 2);
    assert!(!dir.path().join("verified").join(hash).exists());
}

// ── debug bundles (kunobi-ninja/kache#319) ───────────────────────

/// The bundle is baked from the EXECUTABLE output, never a sibling
/// artifact — an inverted classification match would hand dsymutil the
/// dep-info file.
#[test]
fn find_executable_output_picks_the_binary_not_siblings() {
    let compiler = RustcCompiler::new();
    let args = rustc_args(&[
        "rustc",
        "src/main.rs",
        "--crate-name",
        "tool",
        "--crate-type",
        "bin",
        "--emit",
        "dep-info,link",
        "--out-dir",
        "target/debug/deps",
    ]);
    let artifacts = crate::compiler::ArtifactSet::new(vec![
        crate::compiler::Artifact {
            path: std::path::PathBuf::from("target/debug/deps/tool.d"),
            store_name: "tool.d".to_string(),
            kind: crate::compiler::ArtifactKind::DepInfo,
            required: false,
        },
        crate::compiler::Artifact {
            path: std::path::PathBuf::from("target/debug/deps/tool"),
            store_name: "tool".to_string(),
            kind: crate::compiler::ArtifactKind::Executable,
            required: true,
        },
    ]);
    let (path, name) = find_executable_output(&compiler, &args, &artifacts)
        .expect("the bin invocation has an executable output");
    assert_eq!(name, "tool");
    assert_eq!(path, std::path::PathBuf::from("target/debug/deps/tool"));
}

#[test]
fn rustc_debuginfo_enabled_treats_absent_zero_and_none_as_off() {
    let base = ["rustc", "src/main.rs", "--crate-name", "foo"];
    // rustc's default is no debug info.
    assert!(!rustc_debuginfo_enabled(&rustc_args(&base)));
    // The two explicit "off" spellings.
    let mut with = base.to_vec();
    with.extend(["-C", "debuginfo=0"]);
    assert!(!rustc_debuginfo_enabled(&rustc_args(&with)));
    let mut with = base.to_vec();
    with.extend(["-C", "debuginfo=none"]);
    assert!(!rustc_debuginfo_enabled(&rustc_args(&with)));
}

#[test]
fn rustc_debuginfo_enabled_recognizes_debug_levels() {
    let base = ["rustc", "src/main.rs", "--crate-name", "foo"];
    for level in ["1", "2", "line-tables-only"] {
        let mut with = base.to_vec();
        let opt = format!("debuginfo={level}");
        with.extend(["-C", &opt]);
        assert!(
            rustc_debuginfo_enabled(&rustc_args(&with)),
            "debuginfo={level} must count as debug info on"
        );
    }
    // `-g` desugars to `-Cdebuginfo=2` at parse time.
    let mut with = base.to_vec();
    with.push("-g");
    assert!(rustc_debuginfo_enabled(&rustc_args(&with)));
    // A later value wins over an earlier one (rustc's last-wins rule).
    let mut with = base.to_vec();
    with.extend(["-g", "-C", "debuginfo=0"]);
    assert!(!rustc_debuginfo_enabled(&rustc_args(&with)));
}

#[test]
fn wants_debug_bundle_requires_user_facing_and_debuginfo() {
    // Both legs of the conjunction must hold — a lib with `-g` never
    // stores an executable, and a bin without `-g` has no DWARF for a
    // `.dSYM` to carry (#319).
    let bin_g = rustc_args(&[
        "rustc",
        "src/main.rs",
        "--crate-name",
        "foo",
        "--crate-type",
        "bin",
        "-g",
    ]);
    assert!(wants_debug_bundle(&bin_g));

    let test_g = rustc_args(&["rustc", "src/lib.rs", "--crate-name", "foo", "--test", "-g"]);
    assert!(wants_debug_bundle(&test_g));

    let bin_nodebug = rustc_args(&[
        "rustc",
        "src/main.rs",
        "--crate-name",
        "foo",
        "--crate-type",
        "bin",
    ]);
    assert!(!wants_debug_bundle(&bin_nodebug));

    let lib_g = rustc_args(&[
        "rustc",
        "src/lib.rs",
        "--crate-name",
        "foo",
        "--crate-type",
        "lib",
        "-g",
    ]);
    assert!(!wants_debug_bundle(&lib_g));
}

/// Tar bytes shaped like a store-time debug bundle: entries relative to
/// the bundle root, the layout `unpack_debug_bundle` re-creates.
fn debug_bundle_tar(dwarf_name: &str, dwarf: &[u8]) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    for (path, content) in [
        ("Contents/Info.plist".to_string(), b"plist".as_slice()),
        (format!("Contents/Resources/DWARF/{dwarf_name}"), dwarf),
    ] {
        let mut header = tar::Header::new_gnu();
        header.set_size(content.len() as u64);
        header.set_mode(0o644);
        header.set_mtime(0);
        header.set_entry_type(tar::EntryType::Regular);
        builder.append_data(&mut header, path, content).unwrap();
    }
    builder.into_inner().unwrap()
}

/// End-to-end restore of a cached DebugBundle artifact through the same
/// `materialize_cached_artifact` path the wrapper's restore loop uses:
/// the tar is hardlinked from the blob, then the external unpack action
/// publishes the sibling `.dSYM` bundle (#319).
#[test]
fn materialize_cached_artifact_unpacks_debug_bundle_beside_binary() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let hash = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    create_blob(&store, hash, &debug_bundle_tar("foo-abc123", b"dwarf!"));

    let cached = cached_file("foo-abc123.dsym.tar", hash);
    let deps = dir.path().join("target").join("debug").join("deps");
    std::fs::create_dir_all(&deps).unwrap();
    let target = deps.join("foo-abc123.dsym.tar");
    let platform = platform::current();

    materialize_cached_artifact(
        &store,
        &cached,
        &target,
        ArtifactKind::DebugBundle,
        None,
        dir.path(),
        dir.path(),
        None,
        &[],
        &*platform,
        "test restore",
        None,
    )
    .unwrap();

    // The tar is materialized (it is the cached artifact)...
    assert!(target.is_file(), "the bundle tar itself must be restored");
    // ...and the unpack action published the sibling bundle dir.
    let dwarf = deps.join("foo-abc123.dSYM/Contents/Resources/DWARF/foo-abc123");
    assert_eq!(std::fs::read(&dwarf).unwrap(), b"dwarf!");
}

/// macOS-only integration leg (no-op elsewhere): package a REAL `-g`
/// binary's debug map into a bundle tar, restore that tar into a
/// different directory via `materialize_cached_artifact`, and assert
/// the restored `.dSYM`'s UUID equals the binary's. UUID identity is
/// the exact criterion lldb uses to adopt an adjacent bundle, so this
/// pins the property that makes the stale `N_OSO` records inert (#319).
#[test]
fn debug_bundle_round_trip_preserves_dwarf_uuid_on_macos() {
    if !cfg!(target_os = "macos") {
        return;
    }
    let dwarfdump_uuid = |path: &Path| -> String {
        let out = std::process::Command::new("dwarfdump")
            .arg("--uuid")
            .arg(path)
            .output()
            .expect("dwarfdump must be runnable on the macOS test host");
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        // "UUID: <uuid> (<arch>) <path>" — take the UUID token.
        stdout
            .split_whitespace()
            .nth(1)
            .unwrap_or_default()
            .to_string()
    };

    // A real `-g` binary whose DWARF still lives in a per-build `.o` —
    // compile and link separately so that `.o` persists (the N_OSO debug
    // map shape this whole feature exists for).
    let build_dir = tempfile::tempdir().unwrap();
    let source = build_dir.path().join("hello.c");
    std::fs::write(&source, "int main(void) { return 0; }\n").unwrap();
    let object = build_dir.path().join("hello.o");
    let binary = build_dir.path().join("hello-bin");
    let compile = std::process::Command::new("cc")
        .args(["-g", "-c"])
        .arg(&source)
        .arg("-o")
        .arg(&object)
        .status()
        .expect("cc must be runnable on the macOS test host");
    assert!(compile.success(), "cc -g -c failed");
    let link = std::process::Command::new("cc")
        .arg(&object)
        .arg("-o")
        .arg(&binary)
        .status()
        .expect("cc link must be runnable on the macOS test host");
    assert!(link.success(), "cc link failed");

    // Store side: bake + tar the bundle while the `.o` exists.
    use crate::compiler::platform::Platform as _;
    let staging = tempfile::tempdir().unwrap();
    let tar_path = crate::compiler::platform::MacOsPlatform
        .package_debug_bundle(&binary, staging.path())
        .unwrap()
        .expect("macOS host must package a bundle for a -g binary");

    // Cache + restore side, in a directory the `.o` never existed in.
    let restore_dir = tempfile::tempdir().unwrap();
    let config = test_config(restore_dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let hash = crate::cache_key::hash_file(&tar_path).unwrap();
    let blob = store.blob_path(&hash);
    std::fs::create_dir_all(blob.parent().unwrap()).unwrap();
    std::fs::copy(&tar_path, &blob).unwrap();
    let cached = cached_file("hello-bin.dsym.tar", &hash);
    let deps = restore_dir.path().join("deps");
    std::fs::create_dir_all(&deps).unwrap();
    let target = deps.join("hello-bin.dsym.tar");
    let platform = platform::current();
    materialize_cached_artifact(
        &store,
        &cached,
        &target,
        ArtifactKind::DebugBundle,
        None,
        restore_dir.path(),
        restore_dir.path(),
        None,
        &[],
        &*platform,
        "test restore",
        None,
    )
    .unwrap();

    let bundle = deps.join("hello-bin.dSYM");
    let bundle_uuid = dwarfdump_uuid(&bundle);
    let binary_uuid = dwarfdump_uuid(&binary);
    assert!(
        !binary_uuid.is_empty(),
        "dwarfdump produced no UUID for the binary"
    );
    assert_eq!(
        bundle_uuid, binary_uuid,
        "restored .dSYM UUID must match the binary's — that match is \
             what makes lldb adopt the bundle over the stale debug map"
    );
}

// ── fallback wrapper ─────────────────────────────────────────────

#[cfg(unix)]
#[test]
fn stripped_fallback_receives_incremental_disabled_env() {
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let fallback = dir.path().join("fallback");
    let env_dump = dir.path().join("incremental-env.txt");
    kache_fs::testutil::write_executable(
        &fallback,
        format!(
            "#!/bin/sh\nprintf '%s' \"${{CARGO_INCREMENTAL-unset}}\" > '{}'\nexit 0\n",
            env_dump.display()
        ),
    );

    let source = dir.path().join("lib.rs");
    std::fs::write(&source, "pub fn answer() -> u8 { 42 }\n").unwrap();
    let args = RustcArgs::parse(&[
        dir.path().join("missing-rustc").display().to_string(),
        source.display().to_string(),
        format!("-Cincremental={}", dir.path().join("incremental").display()),
    ])
    .unwrap();
    let compiler_args: Vec<String> = compile::strip_incremental_flags(&args.all_args)
        .into_iter()
        .cloned()
        .collect();
    let _incremental = TestEnvGuard::set("CARGO_INCREMENTAL", "1");

    let output = passthrough_args(&args, fallback.to_str(), &compiler_args, false).unwrap();
    assert!(output.fallback);
    assert_eq!(std::fs::read_to_string(env_dump).unwrap(), "0");
}

#[cfg(unix)]
#[test]
fn immediate_adaptive_compile_keeps_passthrough_remap_policy() {
    let dir = tempfile::tempdir().unwrap();
    let profile = dir.path().join("target/debug");
    let deps = profile.join("deps");
    let incremental = profile.join("incremental");
    std::fs::create_dir_all(&deps).unwrap();
    std::fs::create_dir(&incremental).unwrap();

    let source = dir.path().join("lib.rs");
    let rustc = dir.path().join("rustc");
    let argv_dump = dir.path().join("argv.txt");
    std::fs::write(&source, "pub fn answer() -> u8 { 42 }\n").unwrap();
    kache_fs::testutil::write_executable(
        &rustc,
        format!(
            r#"#!/bin/sh
printf '%s\n' "$@" > '{}'
incremental=
for arg in "$@"; do
    case "$arg" in
        -Cincremental=*) incremental=${{arg#-Cincremental=}} ;;
        --codegen=incremental=*) incremental=${{arg#--codegen=incremental=}} ;;
    esac
done
if [ -n "$incremental" ]; then
    mkdir -p "$incremental"
    printf 'state' > "$incremental/state.bin"
fi
exit 0
"#,
            argv_dump.display()
        ),
    );

    let mut args = RustcArgs::parse(&[
        rustc.display().to_string(),
        "--crate-name".to_string(),
        "adaptive_fixture".to_string(),
        "--crate-type".to_string(),
        "lib".to_string(),
        source.display().to_string(),
        "--out-dir".to_string(),
        deps.display().to_string(),
        "--emit=metadata".to_string(),
        "-Cextra-filename=-1234abcd".to_string(),
        format!("-Cincremental={}", incremental.display()),
    ])
    .unwrap();
    args.is_primary = true;
    args.path_normalize_disabled = false;

    let mut config = test_config(dir.path().join("cache"));
    config.base_dirs = vec![dir.path().display().to_string()];
    let guard = adaptive_policy_guard(&config);
    let unit = AdaptiveUnit::eligible(&args, true, &guard).unwrap();
    let lease = unit.try_immediate().unwrap();

    let exit = adaptive_incremental_with_event(
        &config,
        &args,
        "adaptive_fixture",
        &dir.path().display().to_string(),
        std::time::Instant::now(),
        lease,
        "adaptive passthrough",
        None,
    )
    .unwrap();
    assert_eq!(exit, 0);

    let argv = std::fs::read_to_string(argv_dump).unwrap();
    let rustc_incremental = argv
        .lines()
        .find_map(|arg| {
            arg.strip_prefix("-Cincremental=")
                .or_else(|| arg.strip_prefix("--codegen=incremental="))
        })
        .expect("adaptive compilation did not receive an incremental directory");
    assert!(
        std::path::Path::new(rustc_incremental)
            .join("state.bin")
            .is_file(),
        "successful adaptive compilation discarded reusable rustc state"
    );

    assert!(
        !argv
            .lines()
            .any(|arg| arg.starts_with("--remap-path-prefix")),
        "an immediate passthrough unexpectedly injected remap arguments: {argv:?}"
    );
}

#[test]
fn clean_path_collapses_dot_and_dotdot() {
    assert_eq!(clean_path(Path::new("a/./b")), PathBuf::from("a/b"));
    assert_eq!(clean_path(Path::new("a/b/../c")), PathBuf::from("a/c"));
    assert_eq!(clean_path(Path::new("./a/b")), PathBuf::from("a/b"));
    // A leading `..` with nothing to pop is preserved.
    assert_eq!(clean_path(Path::new("../a")), PathBuf::from("../a"));
    // Cleaning down to nothing yields ".".
    assert_eq!(clean_path(Path::new("a/..")), PathBuf::from("."));
    assert_eq!(clean_path(Path::new(".")), PathBuf::from("."));
}

#[cfg(unix)]
#[test]
fn clean_path_preserves_absolute_root() {
    assert_eq!(clean_path(Path::new("/a/./b/../c")), PathBuf::from("/a/c"));
}

#[test]
fn absolute_clean_path_joins_relative_to_cwd() {
    let cwd = Path::new("/work/project");
    assert_eq!(
        absolute_clean_path(Path::new("src/../lib.rs"), cwd),
        PathBuf::from("/work/project/lib.rs")
    );
    // An already-absolute path ignores cwd but is still cleaned.
    assert_eq!(
        absolute_clean_path(Path::new("/etc/./hosts"), cwd),
        PathBuf::from("/etc/hosts")
    );
}

#[test]
fn common_path_prefix_returns_shared_ancestor() {
    assert_eq!(
        common_path_prefix(Path::new("/a/b/c"), Path::new("/a/b/d")),
        Some(PathBuf::from("/a/b"))
    );
    assert_eq!(
        common_path_prefix(Path::new("/a/b"), Path::new("/a/b")),
        Some(PathBuf::from("/a/b"))
    );
}

#[test]
fn common_path_prefix_none_when_nothing_shared() {
    // Different roots / first components share nothing.
    assert_eq!(common_path_prefix(Path::new("a/b"), Path::new("x/y")), None);
}

#[test]
fn progress_label_gates_by_result_and_verbosity() {
    // Hits always show at level 1+.
    assert_eq!(progress_label(EventResult::LocalHit, 1), Some("local hit"));
    assert_eq!(
        progress_label(EventResult::PrefetchHit, 1),
        Some("prefetch hit")
    );
    assert_eq!(
        progress_label(EventResult::RemoteHit, 1),
        Some("remote hit")
    );
    assert_eq!(progress_label(EventResult::Error, 1), Some("error"));

    // Dup / Miss are suppressed at level 1 but shown at verbose level 2.
    assert_eq!(progress_label(EventResult::Dup, 1), None);
    assert_eq!(progress_label(EventResult::Miss, 1), None);
    assert_eq!(progress_label(EventResult::Dup, 2), Some("dup"));
    assert_eq!(progress_label(EventResult::Miss, 2), Some("miss"));

    // Passthrough / Skipped never produce a line, even when verbose.
    assert_eq!(progress_label(EventResult::Passthrough, 2), None);
    assert_eq!(progress_label(EventResult::Skipped, 2), None);
}

#[test]
fn heartbeat_lines_require_verbose_progress() {
    assert!(!heartbeat_lines_enabled(0));
    assert!(!heartbeat_lines_enabled(1));
    assert!(heartbeat_lines_enabled(2));
}

/// `KACHE_PROGRESS` parsing is the only env-dependent part of progress
/// output; the scoped guard keeps the process-global var restored.
#[test]
fn progress_level_parses_supported_env_values() {
    let _lock = crate::test_support::process_state_test_lock();
    let _guard = TestEnvGuard::remove("KACHE_PROGRESS");
    assert_eq!(progress_level(), 0);

    unsafe {
        std::env::set_var("KACHE_PROGRESS", "1");
    }
    assert_eq!(progress_level(), 1);
    unsafe {
        std::env::set_var("KACHE_PROGRESS", "hits");
    }
    assert_eq!(progress_level(), 1);
    unsafe {
        std::env::set_var("KACHE_PROGRESS", "verbose");
    }
    assert_eq!(progress_level(), 2);
    unsafe {
        std::env::set_var("KACHE_PROGRESS", "all");
    }
    assert_eq!(progress_level(), 2);
    unsafe {
        std::env::set_var("KACHE_PROGRESS", "nope");
    }
    assert_eq!(progress_level(), 0);
}

/// The probe-forwarder resolves a kache-wrapped `CC` without spawning it;
/// `run_cc_probe` itself is left untested here because it runs a compiler.
#[test]
fn probe_forward_compiler_recovers_real_compiler_from_cc_env() {
    // The probe cache key fingerprints the WHOLE process environment
    // (`probe::cache::env_fingerprint`), so mutating `CC`/`TARGET` here
    // mid-flight flips a concurrently-running probe test's key and makes
    // its memoization assertion flake. Serialize behind the same lock the
    // env-mutating probe tests hold.
    let _lock = crate::config::config_path_lock();
    let self_stem = std::env::current_exe()
        .ok()
        .as_deref()
        .and_then(Path::file_stem)
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "kache".to_string());
    let wrapped = format!("{self_stem} clang");
    let _target = TestEnvGuard::remove("TARGET");
    let _cc = TestEnvGuard::set("CC", &wrapped);

    assert_eq!(probe_forward_compiler(), "clang");
}

#[test]
fn event_result_for_store_put_maps_dup_vs_miss() {
    use crate::store::StorePutResult;
    // Every output blob was a duplicate -> Dup.
    let dup = StorePutResult {
        output_blobs: 2,
        duplicate_blobs: 2,
        new_blobs: 0,
    };
    assert!(matches!(event_result_for_store_put(dup), EventResult::Dup));
    // At least one new blob -> Miss.
    let partial = StorePutResult {
        output_blobs: 2,
        duplicate_blobs: 1,
        new_blobs: 1,
    };
    assert!(matches!(
        event_result_for_store_put(partial),
        EventResult::Miss
    ));
    // No output blobs -> not a full dup -> Miss.
    let empty = StorePutResult {
        output_blobs: 0,
        duplicate_blobs: 0,
        new_blobs: 0,
    };
    assert!(matches!(
        event_result_for_store_put(empty),
        EventResult::Miss
    ));
}

#[test]
fn store_admission_preserves_writable_remote_publication() {
    let mut config = test_config(PathBuf::from("cache"));
    config.min_store_compile_ms = 1_000;

    assert!(!store_admits_compile(&config, 999, true));
    assert!(store_admits_compile(&config, 1_000, true));

    config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
    assert!(store_admits_compile(&config, 1, true));
    assert!(
        !store_admits_compile(&config, 1, false),
        "a path without remote publication must still apply local admission"
    );

    config.remote_readonly = true;
    assert!(!store_admits_compile(&config, 1, true));
}

#[test]
fn disabled_store_admission_accepts_every_compile() {
    let config = test_config(PathBuf::from("cache"));
    assert!(store_admits_compile(&config, 0, false));
    assert!(store_admits_compile(&config, 5, true));
}

#[test]
fn cc_admission_skip_is_reported_only_for_cacheable_outputs() {
    let put = StorePutResult::default();
    assert!(matches!(
        event_result_for_store_admission(true, false, put),
        EventResult::Skipped
    ));
    assert!(matches!(
        event_result_for_store_admission(false, false, put),
        EventResult::Miss
    ));
}

#[test]
fn cc_store_decision_distinguishes_every_candidate_and_admission_state() {
    let cases = [
        (false, false, false, false),
        (false, true, false, false),
        (true, false, true, false),
        (true, true, false, true),
    ];

    for (candidate, admitted, admission_skipped, should_store) in cases {
        assert_eq!(
            cc_store_decision(candidate, admitted),
            CcStoreDecision {
                admission_skipped,
                should_store,
            },
            "candidate={candidate}, admitted={admitted}"
        );
    }
}

#[test]
fn cc_store_gate_requires_success_and_artifacts() {
    let cases = [
        (0, true, true),
        (1, true, false),
        (0, false, false),
        (1, false, false),
    ];

    for (exit_code, has_artifacts, expected) in cases {
        assert_eq!(
            should_store_cc_result(exit_code, has_artifacts),
            expected,
            "exit={exit_code}, artifacts={has_artifacts}"
        );
    }
}

fn parse_cc(args: &[&str]) -> crate::compiler::cc::CcArgs {
    crate::compiler::cc::CcArgs::parse(&s(args)).unwrap()
}

/// `cc_event_root` honors the override exactly (pins whole-body
/// mutants on a helper the diff would otherwise leave uncovered).
#[test]
fn cc_event_root_honors_override() {
    let _lock = crate::test_support::process_state_test_lock();
    let sentinel = if cfg!(windows) {
        r"C:\cc-root-sentinel"
    } else {
        "/cc-root-sentinel"
    };
    let _guard = TestEnvGuard::set("KACHE_EVENT_ROOT", sentinel);
    let parsed = parse_cc(&["gcc", "-c", "foo.c", "-o", "foo.o"]);
    assert_eq!(
        cc_event_root(&parsed),
        std::path::PathBuf::from(sentinel)
            .to_string_lossy()
            .as_ref()
    );
}

fn spool_intent_count(config: &Config) -> usize {
    let dir = config.upload_spool_dir();
    match std::fs::read_dir(&dir) {
        Ok(entries) => entries.filter_map(Result::ok).count(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => panic!("reading upload spool {}: {error}", dir.display()),
    }
}

fn seed_cc_object_entry(store: &Store, cache_key: &str, dir: &Path) {
    let object = dir.join("foo.o");
    std::fs::write(&object, b"object bytes").unwrap();
    store
        .put_with_compile_time_independent(
            cache_key,
            "foo.c",
            &[],
            &[],
            "x86_64-unknown-linux-gnu",
            "",
            &[(object, "foo.o".to_string())],
            "",
            "",
            12,
        )
        .unwrap();
}

/// Keep auto-start from `send_upload_job` off the developer's daemon.
/// Persist uses the test `Config`; daemon spawn reloads `KACHE_CONFIG`.
fn isolate_daemon_autostart(dir: &Path) -> (TestEnvGuard, TestEnvGuard) {
    let config_path = dir.join("isolated-kache.toml");
    let cache_dir = dir.join("isolated-cache");
    std::fs::create_dir_all(&cache_dir).unwrap();
    let store = toml::Value::String(cache_dir.to_string_lossy().into_owned());
    std::fs::write(
        &config_path,
        format!(
            "[cache]\n\
                 local_only = true\n\
                 ignore_env = true\n\
                 local_store = {store}\n\
                 runtime_dir = {store}\n\
                 daemon_idle_timeout_secs = 1\n"
        ),
    )
    .unwrap();
    (
        TestEnvGuard::set(
            "KACHE_CONFIG",
            config_path.to_str().expect("utf-8 isolated config path"),
        ),
        TestEnvGuard::set(
            "KACHE_CACHE_DIR",
            cache_dir.to_str().expect("utf-8 isolated cache path"),
        ),
    )
}

/// Answers `RemoteCheck` with a fixed `found` flag. `send_remote_check`
/// probes reachability before the real request, so the accept loop must
/// survive empty connections. Local-only tests also use this listener to
/// intercept accidental uploads without starting a real daemon.
struct RemoteCheckReplyDaemon {
    stop: Arc<AtomicBool>,
    requests: Arc<AtomicUsize>,
    handle: Option<std::thread::JoinHandle<()>>,
    socket_path: PathBuf,
}

impl RemoteCheckReplyDaemon {
    fn spawn(socket_path: PathBuf, found: bool) -> Self {
        Self::with_reply(
            socket_path,
            serde_json::json!({ "ok": true, "found": found }),
        )
    }

    fn with_reply(socket_path: PathBuf, reply: serde_json::Value) -> Self {
        Self::with_delayed_reply(socket_path, reply, std::time::Duration::ZERO)
    }

    fn with_delayed_reply(
        socket_path: PathBuf,
        reply: serde_json::Value,
        delay: std::time::Duration,
    ) -> Self {
        let body = format!("{reply}\n");
        if let Some(parent) = socket_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let name = socket_name(&socket_path).expect("fake remote-check socket name");
        let listener = ListenerOptions::new()
            .name(name)
            .create_sync()
            .expect("bind fake remote-check daemon");
        let stop = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(AtomicUsize::new(0));
        let stop_thread = Arc::clone(&stop);
        let requests_thread = Arc::clone(&requests);
        let handle = std::thread::spawn(move || {
            use crate::transport::prelude::*;
            while !stop_thread.load(Ordering::SeqCst) {
                let mut stream = match listener.accept() {
                    Ok(stream) => stream,
                    Err(_) => break,
                };
                if stop_thread.load(Ordering::SeqCst) {
                    break;
                }
                let mut buf = Vec::new();
                let mut chunk = [0u8; 1024];
                loop {
                    match stream.read(&mut chunk) {
                        Ok(0) => break,
                        Ok(n) => {
                            buf.extend_from_slice(&chunk[..n]);
                            if buf.contains(&b'\n') {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                if buf.is_empty() {
                    continue;
                }
                requests_thread.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(delay);
                let _ = stream.write_all(body.as_bytes());
            }
        });
        Self {
            stop,
            requests,
            handle: Some(handle),
            socket_path,
        }
    }

    fn request_count(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}

impl Drop for RemoteCheckReplyDaemon {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = crate::transport::is_reachable(&self.socket_path);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn wait_until_reachable(path: &Path) {
    let start = std::time::Instant::now();
    while !crate::transport::is_reachable(path) {
        assert!(
            start.elapsed() < std::time::Duration::from_secs(2),
            "fake remote-check daemon did not become reachable at {}",
            path.display()
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

fn try_cc_remote_hit(
    config: &Config,
    store: &Store,
    parsed: &crate::compiler::cc::CcArgs,
    cache_key: &str,
) -> Option<i32> {
    cc_try_remote_hit(
        config,
        store,
        &CcCompiler::new(),
        parsed,
        &FileHasher::new(),
        cache_key,
        "foo.c",
        "foo.c",
        std::time::Instant::now(),
        0,
        0,
    )
    .unwrap()
}

#[test]
fn remote_demand_wait_includes_success_and_failed_reply() {
    for ok in [true, false] {
        let _ = crate::demand::take();
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path().join("cache"));
        config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
        let store = Store::open(&config).unwrap();
        let key = blake3::hash(b"remote-demand-wait").to_hex().to_string();
        seed_cc_object_entry(&store, &key, dir.path());
        let daemon = RemoteCheckReplyDaemon::with_delayed_reply(
            config.socket_path(),
            serde_json::json!({ "ok": ok, "found": true }),
            std::time::Duration::from_millis(25),
        );
        wait_until_reachable(&config.socket_path());
        let result = acquire_entry(
            &config,
            &store,
            &key,
            "foo.c",
            NegativeReply::ContinueCompile,
        );
        assert_eq!(result.is_some(), ok);
        let demands = crate::demand::take();
        assert_eq!(demands.len(), 1);
        assert_eq!(demands[0].cache_key, key);
        assert!(demands[0].first_demand_at_ms > 0);
        assert!(demands[0].remote_wait_ms >= 25);
        assert_eq!(daemon.request_count(), 1);
    }
}

#[test]
fn remote_disabled_does_not_create_a_demand_or_wait() {
    let _ = crate::demand::take();
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    assert!(
        acquire_entry(
            &config,
            &store,
            "key",
            "foo.c",
            NegativeReply::ContinueCompile
        )
        .is_none()
    );
    assert!(crate::demand::take().is_empty());
}

#[test]
fn remote_acquisition_preserves_negative_reply_policy_and_provenance() {
    for (found, prefetched, policy, expected) in [
        (false, false, NegativeReply::ContinueCompile, None),
        (false, true, NegativeReply::ContinueCompile, None),
        (
            false,
            false,
            NegativeReply::CheckConcurrentEntry,
            Some(EventResult::LocalHit),
        ),
        (
            false,
            true,
            NegativeReply::CheckConcurrentEntry,
            Some(EventResult::LocalHit),
        ),
        (
            true,
            false,
            NegativeReply::ContinueCompile,
            Some(EventResult::RemoteHit),
        ),
        (
            true,
            true,
            NegativeReply::ContinueCompile,
            Some(EventResult::PrefetchHit),
        ),
        (
            true,
            false,
            NegativeReply::CheckConcurrentEntry,
            Some(EventResult::RemoteHit),
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path().join("cache"));
        config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
        let store = Store::open(&config).unwrap();
        let key = blake3::hash(b"remote-entry-provenance")
            .to_hex()
            .to_string();
        seed_cc_object_entry(&store, &key, dir.path());
        let daemon = RemoteCheckReplyDaemon::with_reply(
            config.socket_path(),
            serde_json::json!({ "ok": true, "found": found, "prefetched": prefetched }),
        );
        wait_until_reachable(&config.socket_path());
        let result = acquire_entry(&config, &store, &key, "foo.c", policy);
        assert_eq!(result.as_ref().map(|(_, origin)| *origin), expected);
        if let Some((meta, _)) = result {
            assert_eq!(meta.cache_key, key);
            assert_eq!(meta.crate_name, "foo.c");
        }
        assert!(daemon.request_count() >= 1);
    }
}

#[test]
fn remote_acquisition_requires_a_reply_and_readable_entry() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().join("cache"));
    config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
    let store = Store::open(&config).unwrap();
    let key = blake3::hash(b"remote-entry-validation")
        .to_hex()
        .to_string();
    seed_cc_object_entry(&store, &key, dir.path());
    assert!(
        acquire_entry(
            &config,
            &store,
            &key,
            "foo.c",
            NegativeReply::CheckConcurrentEntry
        )
        .is_none()
    );
    let daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), true);
    wait_until_reachable(&config.socket_path());
    let missing = blake3::hash(b"missing-entry").to_hex().to_string();
    assert!(
        acquire_entry(
            &config,
            &store,
            &missing,
            "foo.c",
            NegativeReply::ContinueCompile
        )
        .is_none()
    );
    std::fs::write(store.entry_dir(&key).join("meta.json"), b"invalid json").unwrap();
    assert!(
        acquire_entry(
            &config,
            &store,
            &key,
            "foo.c",
            NegativeReply::ContinueCompile
        )
        .is_none()
    );
    assert!(daemon.request_count() >= 2);
}

#[test]
fn clang_cl_debug_does_not_bypass_local_admission() {
    let mut config = test_config(PathBuf::from("cache"));
    config.min_store_compile_ms = 1_000;
    config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));

    let cl_debug = parse_cc(&["clang-cl", "-c", "foo.c", "-Fofoo.obj", "/Z7"]);
    assert!(
        !store_admits_compile(&config, 1, cc_publishes_to_remote(&cl_debug)),
        "clang-cl debug must keep the local cheap-compile threshold"
    );

    let gnu = parse_cc(&["gcc", "-c", "foo.c", "-o", "foo.o"]);
    assert!(
        store_admits_compile(&config, 1, cc_publishes_to_remote(&gnu)),
        "publishable cc compiles must store so they can upload"
    );
}

#[test]
fn cc_remote_publication_skips_clang_cl_debug_only() {
    let gnu = parse_cc(&["gcc", "-c", "foo.c", "-o", "foo.o"]);
    let gnu_debug = parse_cc(&["gcc", "-c", "foo.c", "-o", "foo.o", "-g2"]);
    let cl = parse_cc(&["clang-cl", "-c", "foo.c", "-Fofoo.obj"]);
    let cl_debug = parse_cc(&["clang-cl", "-c", "foo.c", "-Fofoo.obj", "/Z7"]);
    let cl_g = parse_cc(&["clang-cl", "-c", "foo.c", "-Fofoo.obj", "-g"]);

    assert!(cc_publishes_to_remote(&gnu));
    assert!(cc_publishes_to_remote(&gnu_debug));
    assert!(cc_publishes_to_remote(&cl));
    assert!(!cc_publishes_to_remote(&cl_debug));
    assert!(!cc_publishes_to_remote(&cl_g));
}

#[test]
fn cc_upload_enqueue_requires_a_configured_remote_and_publication() {
    let mut config = test_config(PathBuf::from("cache"));
    assert!(!compiler_remote_enabled(&config, true));
    assert!(!compiler_remote_enabled(&config, false));

    config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
    assert!(compiler_remote_enabled(&config, true));
    assert!(!compiler_remote_enabled(&config, false));

    config.remote_readonly = true;
    assert!(
        compiler_remote_enabled(&config, true),
        "readonly is enforced inside send_upload_job, matching rustc"
    );
}

#[test]
fn daemon_handoff_preserves_the_wrappers_upload_policy() {
    let mut config = test_config(PathBuf::from("cache"));
    assert!(!compiler_upload_enabled(&config, true));
    config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
    assert!(compiler_upload_enabled(&config, true));
    assert!(!compiler_upload_enabled(&config, false));
    config.remote_readonly = true;
    assert!(!compiler_upload_enabled(&config, true));
}

#[test]
fn cc_try_remote_hit_skips_the_daemon_when_enqueue_is_false() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let key = blake3::hash(b"cc-remote-skip-enqueue").to_hex().to_string();
    seed_cc_object_entry(&store, &key, dir.path());

    let output = dir.path().join("restored.o");
    let output_arg = output.to_string_lossy().into_owned();
    let parsed = parse_cc(&["gcc", "-c", "foo.c", "-o", &output_arg]);

    let daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), true);
    wait_until_reachable(&config.socket_path());
    let requests_before = daemon.request_count();

    assert!(try_cc_remote_hit(&config, &store, &parsed, &key).is_none());
    assert_eq!(
        daemon.request_count(),
        requests_before,
        "enqueue=false must not send RemoteCheck"
    );
    assert!(
        !output.exists(),
        "skipping the daemon must not restore a local store entry"
    );
}

#[test]
fn cc_try_remote_hit_does_not_restore_on_a_remote_miss() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().join("cache"));
    config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
    let store = Store::open(&config).unwrap();
    let key = blake3::hash(b"cc-remote-miss-no-restore")
        .to_hex()
        .to_string();
    seed_cc_object_entry(&store, &key, dir.path());

    let output = dir.path().join("restored.o");
    let output_arg = output.to_string_lossy().into_owned();
    let parsed = parse_cc(&["gcc", "-c", "foo.c", "-o", &output_arg]);

    let daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), false);
    wait_until_reachable(&config.socket_path());

    assert!(try_cc_remote_hit(&config, &store, &parsed, &key).is_none());
    assert!(
        daemon.request_count() >= 1,
        "a configured remote must still ask the daemon"
    );
    assert!(
        !output.exists(),
        "found=false must not restore even when the local store already has the entry"
    );
}

/// A declined hand-off from the main store re-claims the key; a peer
/// that took it in the gap publishes, and the wrapper only logs its
/// event.
#[test]
fn a_declined_hand_off_yields_to_a_peer_that_took_the_key() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let key = blake3::hash(b"cc-handoff-peer").to_hex().to_string();
    let BuildClaim::Acquired(_peer) = store.claim_build(&key).unwrap() else {
        panic!("fresh key");
    };
    let object = dir.path().join("foo.o");
    std::fs::write(&object, b"object bytes").unwrap();
    let files = vec![(object, "foo.o".to_string())];
    let now = std::time::Instant::now();
    let handoff = CcHandoff {
        cache_key: &key,
        crate_name: "foo.c",
        target: "x86_64",
        files: &files,
        stdout: "",
        stderr: "",
        compile_time_ms: 5,
        publishes_to_remote: false,
        event_root: "",
        start: now,
        size: 12,
        key_ms: 0,
        lookup_ms: 0,
        lookup_rejection: "",
        store_start: now,
        memo: None,
    };

    // No daemon: the offer is declined and the key is the peer's.
    let mut lock = None;
    let outcome = hand_off_cc_store(&config, &store, &mut lock, handoff);

    assert!(matches!(outcome, CcHandoffOutcome::Done));
    assert!(lock.is_none());
    let events = crate::events::read_events(&config.event_log_path()).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].result, EventResult::Miss);
    assert!(!events[0].store_handed_off);
}

#[test]
fn cc_store_enqueues_an_upload_intent_when_a_writable_remote_is_configured() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().join("cache"));
    config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
    let store = Store::open(&config).unwrap();
    let key = blake3::hash(b"cc-upload-intent").to_hex().to_string();
    seed_cc_object_entry(&store, &key, dir.path());

    let _lock = crate::config::config_path_lock();
    let _isolated = isolate_daemon_autostart(dir.path());
    maybe_enqueue_upload(&config, &store, &key, "foo.c", true);

    assert_eq!(spool_intent_count(&config), 1);
    assert!(
        config
            .upload_spool_dir()
            .join(format!("{key}.json"))
            .is_file()
    );
}

#[test]
fn clang_cl_debug_store_does_not_enqueue_an_upload_intent() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().join("cache"));
    config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
    let store = Store::open(&config).unwrap();
    let key = blake3::hash(b"cc-cl-debug-no-upload").to_hex().to_string();
    seed_cc_object_entry(&store, &key, dir.path());

    let _daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), false);
    let _lock = crate::config::config_path_lock();
    let _isolated = isolate_daemon_autostart(dir.path());
    maybe_enqueue_upload(&config, &store, &key, "foo.c", false);

    assert_eq!(spool_intent_count(&config), 0);
}

#[test]
fn cc_store_does_not_enqueue_when_remote_is_readonly_or_absent() {
    let dir = tempfile::tempdir().unwrap();
    let key = blake3::hash(b"cc-no-upload-gates").to_hex().to_string();

    let mut readonly = test_config(dir.path().join("readonly"));
    readonly.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
    readonly.remote_readonly = true;
    let readonly_store = Store::open(&readonly).unwrap();
    seed_cc_object_entry(&readonly_store, &key, dir.path());

    let local = test_config(dir.path().join("local"));
    let local_store = Store::open(&local).unwrap();
    seed_cc_object_entry(&local_store, &key, dir.path());

    let _readonly_daemon = RemoteCheckReplyDaemon::spawn(readonly.socket_path(), false);
    let _local_daemon = RemoteCheckReplyDaemon::spawn(local.socket_path(), false);
    let _lock = crate::config::config_path_lock();
    let _isolated = isolate_daemon_autostart(dir.path());
    maybe_enqueue_upload(&readonly, &readonly_store, &key, "foo.c", true);
    maybe_enqueue_upload(&local, &local_store, &key, "foo.c", true);

    assert_eq!(spool_intent_count(&readonly), 0);
    assert_eq!(spool_intent_count(&local), 0);
}

#[test]
fn cc_output_path_passthrough_allows_plain_files_but_refuses_symlinks() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("output.o");

    assert!(!cc_output_path_requires_passthrough(&output));
    std::fs::write(&output, b"existing").unwrap();
    assert!(!cc_output_path_requires_passthrough(&output));

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let dangling = dir.path().join("dangling.o");
        symlink(dir.path().join("missing-target"), &dangling).unwrap();
        assert!(cc_output_path_requires_passthrough(&dangling));
    }
}

#[cfg(unix)]
#[test]
fn cc_passthrough_forwards_the_original_arguments_only() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let fake_cc = dir.path().join("cc");
    let capture = dir.path().join("capture.c");
    let output = dir.path().join("output.o");
    let fallback = dir.path().join("fallback");
    let shell =
        crate::compiler::resolve_program_on_path("sh").expect("sh must be available on PATH");
    kache_fs::testutil::write_executable(
        &fake_cc,
        format!(
            "#!{}\ncapture=\"$1\"\nshift\nprintf '%s\\n' \"$@\" > \"$capture\"\n",
            shell.display()
        ),
    );
    kache_fs::testutil::write_executable(
        &fallback,
        format!("#!{}\nprintf 'fallback\\n' > \"$2\"\n", shell.display()),
    );
    std::fs::write(&output, b"existing output").unwrap();
    std::fs::set_permissions(&output, std::fs::Permissions::from_mode(0o444)).unwrap();

    let capture_arg = capture.to_string_lossy().into_owned();
    let output_arg = output.to_string_lossy().into_owned();
    let parsed = CcCompiler::new()
        .parse(&s(&[
            &fake_cc.to_string_lossy(),
            &capture_arg,
            "-c",
            "-o",
            &output_arg,
        ]))
        .unwrap();

    let mut config = test_config(dir.path().join("cache"));
    config.fallback = fallback.to_str().map(ToOwned::to_owned);
    let result = cc_passthrough(&config, &parsed).unwrap();

    assert_eq!(result.exit_code, 0);
    assert_eq!(
        std::fs::read_to_string(&capture).unwrap(),
        format!("-c\n-o\n{output_arg}\n"),
        "unsafe output passthrough must bypass fallback and cache-only flags"
    );
}

/// Phase 1 nvcc (#1024): a refused invocation runs the real compiler
/// with the original argv and propagates its exit code — and records
/// the passthrough reason. The nonzero exit kills the `Ok(0)` /
/// `Ok(1)` / `Ok(-1)` body mutants in both `run_nvcc` and
/// `nvcc_passthrough_with_event`; the sentinel root kills the
/// `nvcc_event_root` value mutants.
#[cfg(unix)]
#[test]
fn nvcc_passthrough_propagates_exit_code_and_reasons() {
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let fake_nvcc = dir.path().join("nvcc");
    let shell =
        crate::compiler::resolve_program_on_path("sh").expect("sh must be available on PATH");
    kache_fs::testutil::write_executable(&fake_nvcc, format!("#!{}\nexit 3\n", shell.display()));

    let _root_guard = TestEnvGuard::set("KACHE_EVENT_ROOT", "/nvcc-phase1-root");
    let config = test_config(dir.path().join("cache"));
    let exit = run_nvcc(
        &config,
        &s(&[
            &fake_nvcc.to_string_lossy(),
            "-dlink",
            "a.o",
            "-o",
            "dlink.o",
        ]),
    )
    .unwrap();
    assert_eq!(exit, 3);

    let events = crate::events::read_events(&config.event_log_path()).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].root, "/nvcc-phase1-root");
    assert_eq!(events[0].result, EventResult::Passthrough);
    assert!(
        events[0].passthrough_reason.contains("device-link"),
        "unexpected reason: {}",
        events[0].passthrough_reason
    );
}

/// Write the fake nvcc toolchain with baked-in paths (no
/// environment needed, so tests stay parallel): `nvcc` answers
/// `--version`, `--dryrun` (naming `host`), `-M` (the source plus
/// `headers`), and compiles by writing `-o`/`-MF` outputs while
/// recording invocations, argv, and `SOURCE_DATE_EPOCH`. Only the
/// exit codes (`exit_code`, `m_exit_code`) baked in as literals.
#[cfg(unix)]
#[allow(clippy::too_many_arguments)]
fn write_fake_nvcc_toolchain(
    dir: &Path,
    source: &Path,
    headers: &str,
    version: &str,
    host_version: &str,
    count: &Path,
    argv_record: Option<&Path>,
    epoch_record: Option<&Path>,
    exit_code: i32,
    m_exit_code: i32,
) -> (PathBuf, PathBuf) {
    let shell =
        crate::compiler::resolve_program_on_path("sh").expect("sh must be available on PATH");
    let host = dir.join("host-gcc");
    kache_fs::testutil::write_executable(
        &host,
        format!(
            "#!{}\nprintf '%s\\n' '{hv}'\n",
            shell.display(),
            hv = host_version
        ),
    );
    let nvcc = dir.join("nvcc");
    let argv_line = argv_record
        .map(|p| {
            format!(
                "if [ -n \"{p}\" ]; then printf '%s\\n' \"$@\" >> \"{p}\"; fi\n",
                p = p.display()
            )
        })
        .unwrap_or_default();
    let epoch_line = epoch_record
            .map(|p| {
                format!(
                    "if [ -n \"{p}\" ]; then printf '%s\\n' \"${{SOURCE_DATE_EPOCH:-<unset>}}\" >> \"{p}\"; fi\n",
                    p = p.display()
                )
            })
            .unwrap_or_default();
    kache_fs::testutil::write_executable(
        &nvcc,
        format!(
            "#!{sh}\n\
                 if [ \"$1\" = \"--version\" ]; then\n\
                 printf '%s\\n' '{ver}'\n\
                 exit 0\n\
                 fi\n\
                 if [ \"$1\" = \"--dryrun\" ]; then\n\
                 printf '%s\\n' '#$ _HERE_=/usr/local/cuda/bin' '{host} -c -x c++'\n\
                 exit 0\n\
                 fi\n\
                 if [ \"$1\" = \"-M\" ]; then\n\
                 shift\n\
                 printf '%s:' \"fake.o\"\n\
                 printf ' %s' \"$1\"\n\
                 for f in {headers}; do printf ' %s' \"$f\"; done\n\
                 printf '\\n'\n\
                 exit {m_exit_code}\n\
                 fi\n\
                 printf 'run\\n' >> \"{count}\"\n\
                 {argv_line}                 {epoch_line}                 out=\"\"; dep=\"\"\n\
                 while [ $# -gt 0 ]; do\n\
                 case \"$1\" in\n\
                 -o) out=\"$2\"; shift 2;;\n\
                 -o*) out=\"${{1#-o}}\"; shift;;\n\
                 -MF) dep=\"$2\"; shift 2;;\n\
                 -MF*) dep=\"${{1#-MF}}\"; shift;;\n\
                 *) shift;;\n\
                 esac\n\
                 done\n\
                 printf 'object-bytes\\n' > \"$out\"\n\
                 if [ -n \"$dep\" ]; then printf '%s: %s %s\\n' \"$out\" \"{source}\" \"{headers}\" > \"$dep\"; fi\n\
                 exit {exit_code}\n",
            sh = shell.display(),
            host = host.display(),
            source = source.display(),
            count = count.display(),
            ver = version,
            headers = headers,
        ),
    );
    (nvcc, host)
}

/// One standard fake-toolchain project: `work/kernel.cu` including
/// `work/inc/h.h`, the toolchain scripts, and the standard compile
/// argv. All paths are baked into the scripts, so tests need no
/// environment and run parallel. `argv_record` / `epoch_record` name
/// optional record files under `dir` (argv capture, epoch capture).
/// Returns `(work, nvcc, count, argv)`.
#[cfg(unix)]
#[allow(clippy::too_many_arguments)]
fn setup_nvcc_case(
    dir: &tempfile::TempDir,
    argv_record: Option<&str>,
    epoch_record: Option<&str>,
    exit_code: i32,
    m_exit_code: i32,
) -> (PathBuf, PathBuf, PathBuf, Vec<String>) {
    let work = dir.path().join("work");
    std::fs::create_dir_all(work.join("inc")).unwrap();
    std::fs::write(
        work.join("kernel.cu"),
        "#include \"inc/h.h\"\n__global__ void k() {}\n",
    )
    .unwrap();
    std::fs::write(work.join("inc").join("h.h"), "#pragma once\n").unwrap();
    let count = dir.path().join("count");
    let argv_path = argv_record.map(|name| dir.path().join(name));
    let epoch_path = epoch_record.map(|name| dir.path().join(name));
    let (nvcc, _host) = write_fake_nvcc_toolchain(
        dir.path(),
        &work.join("kernel.cu"),
        work.join("inc/h.h").to_str().unwrap(),
        "nvcc: NVIDIA (R) Cuda compiler driver",
        "gcc (GCC) 13.2.0",
        &count,
        argv_path.as_deref(),
        epoch_path.as_deref(),
        exit_code,
        m_exit_code,
    );
    let argv = nvcc_compile_argv(&nvcc, &work, &[]);
    (work, nvcc, count, argv)
}

/// A fake-toolchain nvcc invocation over absolute tempdir paths (the
/// wrapper never chdirs in tests). Returns the argv vector.
#[cfg(unix)]
fn nvcc_compile_argv(nvcc: &Path, work: &Path, extra: &[&str]) -> Vec<String> {
    let mut argv = vec![
        nvcc.to_string_lossy().into_owned(),
        "-c".to_string(),
        work.join("kernel.cu").to_string_lossy().into_owned(),
        "-o".to_string(),
        work.join("kernel.o").to_string_lossy().into_owned(),
        "-MF".to_string(),
        work.join("kernel.d").to_string_lossy().into_owned(),
        format!("-I{}", work.join("inc").display()),
        "-DUSE_CUDA".to_string(),
        "-gencode".to_string(),
        "arch=compute_80,code=sm_80".to_string(),
    ];
    argv.extend(extra.iter().map(|e| e.to_string()));
    argv
}

/// Miss, then hit: the second identical invocation restores the
/// object and the dep-info without reforking the compiler, and the
/// dep-info round-trips byte-identically (store relativize +
/// restore expand are inverses under one anchor).
#[cfg(unix)]
#[test]
fn nvcc_miss_then_hit_round_trips_object_and_depinfo() {
    // Pinned epoch: Nix builders (and reproducibility-minded
    // developers) export SOURCE_DATE_EPOCH, which the wrapper
    // honors verbatim — fix both epoch inputs for determinism.
    let _lock = crate::test_support::process_state_test_lock();
    let _epoch_env = TestEnvGuard::remove("SOURCE_DATE_EPOCH");
    let _epoch_knob = TestEnvGuard::remove("KACHE_NVCC_SOURCE_DATE_EPOCH");
    let dir = tempfile::tempdir().unwrap();
    let (work, _nvcc, count, argv) = setup_nvcc_case(&dir, Some("argv"), Some("epoch"), 0, 0);
    let argv_file = dir.path().join("argv");
    let epoch_file = dir.path().join("epoch");
    let config = test_config(dir.path().join("cache"));
    let _daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), false);

    assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
    assert_eq!(
        spool_intent_count(&config),
        0,
        "local-only compile queued an upload"
    );
    assert_eq!(std::fs::read_to_string(&count).unwrap(), "run\n");
    assert_eq!(
        std::fs::read_to_string(work.join("kernel.o")).unwrap(),
        "object-bytes\n"
    );
    // The execute path injected the prefix maps and pinned the epoch.
    let recorded_argv = std::fs::read_to_string(&argv_file).unwrap();
    assert!(
        recorded_argv.contains("-ffile-prefix-map="),
        "missing prefix-map injection in: {recorded_argv}"
    );
    assert_eq!(std::fs::read_to_string(&epoch_file).unwrap(), "0\n");
    let stored_depinfo = std::fs::read(work.join("kernel.d")).unwrap();

    std::fs::remove_file(work.join("kernel.o")).unwrap();
    std::fs::remove_file(work.join("kernel.d")).unwrap();
    assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
    assert_eq!(
        std::fs::read_to_string(&count).unwrap(),
        "run\n",
        "hit must not refork the compiler"
    );
    assert_eq!(
        std::fs::read(work.join("kernel.o")).unwrap(),
        b"object-bytes\n"
    );
    assert_eq!(
        std::fs::read(work.join("kernel.d")).unwrap(),
        stored_depinfo
    );

    let events = crate::events::read_events(&config.event_log_path()).unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].result, EventResult::Miss);
    assert_eq!(events[1].result, EventResult::LocalHit);
    // No remote configured: nothing is queued for upload.
    assert_eq!(spool_intent_count(&config), 0);
}

/// A dep-info the entry lacks evicts and recompiles: an object-only
/// entry cannot satisfy `-MF`, and the recompiled entry (object +
/// dep-info) hits afterwards.
#[cfg(unix)]
#[test]
fn nvcc_depinfo_demand_evicts_and_recompiles() {
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let (work, nvcc, count, _) = setup_nvcc_case(&dir, None, None, 0, 0);
    let config = test_config(dir.path().join("cache"));
    let _daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), false);

    // Without -MF: object-only entry.
    let bare = vec![
        nvcc.to_string_lossy().into_owned(),
        "-c".to_string(),
        work.join("kernel.cu").to_string_lossy().into_owned(),
        "-o".to_string(),
        work.join("kernel.o").to_string_lossy().into_owned(),
    ];
    assert_eq!(run_nvcc(&config, &bare).unwrap(), 0);
    assert_eq!(
        spool_intent_count(&config),
        0,
        "local-only compile queued an upload"
    );
    // With -MF: the object-only entry cannot satisfy it — evict,
    // recompile, store both artifacts…
    let with_dep = nvcc_compile_argv(&nvcc, &work, &[]);
    assert_eq!(run_nvcc(&config, &with_dep).unwrap(), 0);
    // …and the combined entry hits.
    std::fs::remove_file(work.join("kernel.o")).unwrap();
    std::fs::remove_file(work.join("kernel.d")).unwrap();
    assert_eq!(run_nvcc(&config, &with_dep).unwrap(), 0);
    assert_eq!(
        std::fs::read_to_string(&count).unwrap(),
        "run\nrun\n",
        "exactly two compiles: miss, then evict-and-recompile"
    );
    assert!(work.join("kernel.o").is_file());
    assert!(work.join("kernel.d").is_file());
}

/// A configured remote without a reachable daemon falls through to
/// the local path: check (absent) → compile → queued upload intent.
#[cfg(unix)]
#[test]
fn nvcc_remote_absent_daemon_falls_through_to_compile() {
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    std::fs::create_dir_all(work.join("inc")).unwrap();
    std::fs::write(work.join("kernel.cu"), "__global__ void k() {}\n").unwrap();
    std::fs::write(work.join("inc").join("h.h"), "#pragma once\n").unwrap();
    let count = dir.path().join("count");
    let headers = work.join("inc").join("h.h");
    let (nvcc, _host) = write_fake_nvcc_toolchain(
        dir.path(),
        &work.join("kernel.cu"),
        headers.to_str().unwrap(),
        "nvcc: NVIDIA (R) Cuda compiler driver",
        "gcc (GCC) 13.2.0",
        &count,
        None,
        None,
        0,
        0,
    );

    let _isolated = isolate_daemon_autostart(dir.path());
    let mut config = test_config(dir.path().join("cache"));
    config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));

    // A reachable fake daemon answers the upload send instantly, so
    // no real daemon is auto-started (that path costs ~15s and would
    // leak a daemon onto the machine).
    let daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), false);
    wait_until_reachable(&config.socket_path());

    let argv = nvcc_compile_argv(&nvcc, &work, &[]);
    assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
    assert_eq!(std::fs::read_to_string(&count).unwrap(), "run\n");
    assert_eq!(spool_intent_count(&config), 1);
    assert!(
        daemon.request_count() >= 1,
        "the upload send must reach the daemon"
    );
}

/// Remote-hit plumbing without a real download: seed the entry
/// with a live miss, then drive `nvcc_try_remote_hit` against the
/// fake daemon directly (mirrors the cc remote-hit tests — the
/// seeded local entry stands in for the download).
#[cfg(unix)]
fn seed_nvcc_entry(
    config: &Config,
    argv: &[String],
) -> (Store, crate::compiler::nvcc::NvccArgs, String) {
    let store = Store::open(config).unwrap();
    let compiler =
        NvccCompiler::with_extra_allowlist_flags(config.cc_extra_allowlist_flags.clone());
    let parsed = compiler.parse(argv).unwrap();
    assert_eq!(run_nvcc(config, argv).unwrap(), 0);
    let file_hasher = FileHasher::new();
    let path_normalizer = crate::path_normalizer::PathNormalizer::empty();
    let ctx = KeyCtx {
        file_hasher: &file_hasher,
        path_normalizer: &path_normalizer,
        cache_dir: &config.cache_dir,
        key_salt: config.key_salt.as_deref(),
        key_env_vars: &config.key_env_vars,
        extra_inputs_digest: None,
    };
    let key = compiler.cache_key(&parsed, &ctx).unwrap();
    (store, parsed, key)
}

/// No configured remote: the daemon is never contacted and the
/// local entry is left alone.
#[cfg(unix)]
#[test]
fn nvcc_try_remote_hit_skips_daemon_without_remote() {
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    std::fs::create_dir_all(work.join("inc")).unwrap();
    std::fs::write(work.join("kernel.cu"), "__global__ void k() {}\n").unwrap();
    std::fs::write(work.join("inc").join("h.h"), "#pragma once\n").unwrap();

    let count = dir.path().join("count");
    let headers = work.join("inc").join("h.h");
    let (nvcc, _host) = write_fake_nvcc_toolchain(
        dir.path(),
        &work.join("kernel.cu"),
        headers.to_str().unwrap(),
        "nvcc: NVIDIA (R) Cuda compiler driver",
        "gcc (GCC) 13.2.0",
        &count,
        None,
        None,
        0,
        0,
    );

    let config = test_config(dir.path().join("cache"));
    let argv = nvcc_compile_argv(&nvcc, &work, &[]);
    let daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), true);
    wait_until_reachable(&config.socket_path());
    let (store, parsed, key) = seed_nvcc_entry(&config, &argv);
    assert_eq!(
        spool_intent_count(&config),
        0,
        "seeding must remain local-only"
    );
    std::fs::remove_file(work.join("kernel.o")).unwrap();
    std::fs::remove_file(work.join("kernel.d")).unwrap();

    let requests_before = daemon.request_count();
    let start = std::time::Instant::now();

    assert!(
        nvcc_try_remote_hit(
            &config,
            &store,
            &parsed,
            &key,
            "kernel.cu",
            "/nvcc-root",
            start,
            0,
            0,
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(
        daemon.request_count(),
        requests_before,
        "no remote must mean no RemoteCheck"
    );
    assert!(
        !work.join("kernel.o").exists(),
        "skipping the daemon must not restore"
    );
}

/// A remote miss restores nothing, even with a seeded local entry —
/// but the daemon is asked.
#[cfg(unix)]
#[test]
fn nvcc_try_remote_hit_does_not_restore_on_remote_miss() {
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    std::fs::create_dir_all(work.join("inc")).unwrap();
    std::fs::write(work.join("kernel.cu"), "__global__ void k() {}\n").unwrap();
    std::fs::write(work.join("inc").join("h.h"), "#pragma once\n").unwrap();

    let count = dir.path().join("count");
    let headers = work.join("inc").join("h.h");
    let (nvcc, _host) = write_fake_nvcc_toolchain(
        dir.path(),
        &work.join("kernel.cu"),
        headers.to_str().unwrap(),
        "nvcc: NVIDIA (R) Cuda compiler driver",
        "gcc (GCC) 13.2.0",
        &count,
        None,
        None,
        0,
        0,
    );

    let mut config = test_config(dir.path().join("cache"));
    config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
    // Up before the seed: the seeding miss uploads, and the send
    // must reach a daemon instantly instead of auto-starting a real
    // one (~15s, plus a stray daemon on the machine).
    let daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), false);
    wait_until_reachable(&config.socket_path());
    let argv = nvcc_compile_argv(&nvcc, &work, &[]);
    let (store, parsed, key) = seed_nvcc_entry(&config, &argv);
    std::fs::remove_file(work.join("kernel.o")).unwrap();
    std::fs::remove_file(work.join("kernel.d")).unwrap();

    let start = std::time::Instant::now();

    assert!(
        nvcc_try_remote_hit(
            &config,
            &store,
            &parsed,
            &key,
            "kernel.cu",
            "/nvcc-root",
            start,
            0,
            0,
        )
        .unwrap()
        .is_none()
    );
    assert!(
        daemon.request_count() >= 1,
        "a configured remote must ask the daemon"
    );
    assert!(
        !work.join("kernel.o").exists(),
        "found=false must not restore even with a seeded entry"
    );
}

/// A found remote entry restores without compiling and reports
/// RemoteHit.
#[cfg(unix)]
#[test]
fn nvcc_try_remote_hit_restores_on_found() {
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    std::fs::create_dir_all(work.join("inc")).unwrap();
    std::fs::write(work.join("kernel.cu"), "__global__ void k() {}\n").unwrap();
    std::fs::write(work.join("inc").join("h.h"), "#pragma once\n").unwrap();

    let count = dir.path().join("count");
    let headers = work.join("inc").join("h.h");
    let (nvcc, _host) = write_fake_nvcc_toolchain(
        dir.path(),
        &work.join("kernel.cu"),
        headers.to_str().unwrap(),
        "nvcc: NVIDIA (R) Cuda compiler driver",
        "gcc (GCC) 13.2.0",
        &count,
        None,
        None,
        0,
        0,
    );

    let mut config = test_config(dir.path().join("cache"));
    config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
    // Up before the seed (see the miss test): the empty store keeps
    // the seeding compile a miss even with found=true.
    let daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), true);
    wait_until_reachable(&config.socket_path());
    let argv = nvcc_compile_argv(&nvcc, &work, &[]);
    let (store, parsed, key) = seed_nvcc_entry(&config, &argv);
    std::fs::remove_file(work.join("kernel.o")).unwrap();
    std::fs::remove_file(work.join("kernel.d")).unwrap();

    let start = std::time::Instant::now();

    assert_eq!(
        nvcc_try_remote_hit(
            &config,
            &store,
            &parsed,
            &key,
            "kernel.cu",
            "/nvcc-root",
            start,
            0,
            0,
        )
        .unwrap(),
        Some(0)
    );
    assert!(work.join("kernel.o").is_file());
    assert!(work.join("kernel.d").is_file());
    assert!(
        daemon.request_count() >= 1,
        "a configured remote must ask the daemon"
    );
    let events = crate::events::read_events(&config.event_log_path()).unwrap();
    assert_eq!(events.last().unwrap().result, EventResult::RemoteHit);
}

/// A failing compile stores nothing and propagates the exit code:
/// the rerun compiles again.
#[cfg(unix)]
#[test]
fn nvcc_failed_compile_stores_nothing() {
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let (_work, _nvcc, count, argv) = setup_nvcc_case(&dir, None, None, 1, 0);
    let config = test_config(dir.path().join("cache"));

    assert_eq!(run_nvcc(&config, &argv).unwrap(), 1);
    assert_eq!(run_nvcc(&config, &argv).unwrap(), 1);
    assert_eq!(
        std::fs::read_to_string(&count).unwrap(),
        "run\nrun\n",
        "failures must never store"
    );
}

/// A failing key (here: `-M` exits) passes through to a live
/// compile and stores nothing.
#[cfg(unix)]
#[test]
fn nvcc_key_failure_passes_through_uncached() {
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let (_work, _nvcc, count, argv) = setup_nvcc_case(&dir, None, None, 0, 1);
    let config = test_config(dir.path().join("cache"));
    assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
    assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
    assert_eq!(
        std::fs::read_to_string(&count).unwrap(),
        "run\nrun\n",
        "key failures must never store"
    );
    let events = crate::events::read_events(&config.event_log_path()).unwrap();
    assert!(
        events.iter().all(|e| e.result == EventResult::Passthrough
            && e.passthrough_reason.contains("uncacheable")),
        "key failures pass through with reason"
    );
}

/// Key sensitivity: a header edit and a flag change each bust the
/// key (closure contents and verbatim flags are folded).
#[cfg(unix)]
#[test]
fn nvcc_header_and_flag_edits_bust_the_key() {
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    std::fs::create_dir_all(work.join("inc")).unwrap();
    std::fs::write(work.join("kernel.cu"), "#include \"inc/h.h\"\n").unwrap();
    std::fs::write(work.join("inc").join("h.h"), "#pragma once\n").unwrap();
    let count = dir.path().join("count");
    let headers = work.join("inc").join("h.h");
    let (nvcc, _host) = write_fake_nvcc_toolchain(
        dir.path(),
        &work.join("kernel.cu"),
        headers.to_str().unwrap(),
        "nvcc: NVIDIA (R) Cuda compiler driver",
        "gcc (GCC) 13.2.0",
        &count,
        None,
        None,
        0,
        0,
    );

    let config = test_config(dir.path().join("cache"));
    let _daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), false);

    let argv = nvcc_compile_argv(&nvcc, &work, &[]);
    assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
    assert_eq!(
        spool_intent_count(&config),
        0,
        "local-only compile queued an upload"
    );
    assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
    assert_eq!(std::fs::read_to_string(&count).unwrap(), "run\n");

    std::fs::write(work.join("inc").join("h.h"), "#pragma once\n// edit\n").unwrap();
    assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
    assert_eq!(
        std::fs::read_to_string(&count).unwrap(),
        "run\nrun\n",
        "header edit must bust the key"
    );

    let changed = nvcc_compile_argv(&nvcc, &work, &["-DOTHER"]);
    assert_eq!(run_nvcc(&config, &changed).unwrap(), 0);
    assert_eq!(
        std::fs::read_to_string(&count).unwrap(),
        "run\nrun\nrun\n",
        "flag change must bust the key"
    );
}

/// Invisible driver inputs join the key: setting
/// `NVCC_PREPEND_FLAGS` busts it (miss, then hit under the new
/// environment), while smuggled preprocessor inputs pass through
/// uncached instead of miscaching.
#[cfg(unix)]
#[test]
fn nvcc_driver_env_joins_the_key() {
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let (_work, _nvcc, count, argv) = setup_nvcc_case(&dir, None, None, 0, 0);
    let config = test_config(dir.path().join("cache"));
    let _daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), false);

    assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
    assert_eq!(
        spool_intent_count(&config),
        0,
        "local-only compile queued an upload"
    );
    assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
    assert_eq!(std::fs::read_to_string(&count).unwrap(), "run\n");

    let _prepend = TestEnvGuard::set("NVCC_PREPEND_FLAGS", "-O2");
    assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
    assert_eq!(
        std::fs::read_to_string(&count).unwrap(),
        "run\nrun\n",
        "driver env change must bust the key"
    );
    assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
    assert_eq!(
        std::fs::read_to_string(&count).unwrap(),
        "run\nrun\n",
        "same environment must hit"
    );
    drop(_prepend);

    let _smuggled = TestEnvGuard::set("NVCC_PREPEND_FLAGS", "-I/secret");
    assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
    assert_eq!(
        std::fs::read_to_string(&count).unwrap(),
        "run\nrun\nrun\n",
        "smuggled preprocessor inputs must recompile, never store"
    );
    let events = crate::events::read_events(&config.event_log_path()).unwrap();
    assert!(
        events
            .last()
            .unwrap()
            .passthrough_reason
            .contains("uncacheable"),
        "smuggled inputs pass through with reason"
    );
}

/// A too-cheap compile is skipped (never stored): the rerun
/// compiles again.
#[cfg(unix)]
#[test]
fn nvcc_admission_skipped_when_too_cheap() {
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    std::fs::create_dir_all(work.join("inc")).unwrap();
    std::fs::write(work.join("kernel.cu"), "__global__ void k() {}\n").unwrap();
    std::fs::write(work.join("inc").join("h.h"), "#pragma once\n").unwrap();
    let count = dir.path().join("count");
    let headers = work.join("inc").join("h.h");
    let (nvcc, _host) = write_fake_nvcc_toolchain(
        dir.path(),
        &work.join("kernel.cu"),
        headers.to_str().unwrap(),
        "nvcc: NVIDIA (R) Cuda compiler driver",
        "gcc (GCC) 13.2.0",
        &count,
        None,
        None,
        0,
        0,
    );

    let mut config = test_config(dir.path().join("cache"));
    config.min_store_compile_ms = u64::MAX;

    let argv = nvcc_compile_argv(&nvcc, &work, &[]);
    assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
    assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
    assert_eq!(
        std::fs::read_to_string(&count).unwrap(),
        "run\nrun\n",
        "skipped compiles must never store"
    );
    let events = crate::events::read_events(&config.event_log_path()).unwrap();
    assert!(
        events.iter().all(|e| e.result == EventResult::Skipped),
        "cheap compiles report Skipped"
    );
}

/// Compiling over an output that still shares a read-only cache
/// blob refuses loudly instead of failing with EACCES (or worse).
#[cfg(unix)]
#[test]
fn nvcc_legacy_blob_check_refuses_shared_inode() {
    let _lock = crate::test_support::process_state_test_lock();
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let content = b"cached object";
    let hash = blake3::hash(content).to_hex().to_string();
    create_blob(&store, &hash, content);
    let blob = store.blob_path(&hash);
    std::fs::set_permissions(&blob, std::fs::Permissions::from_mode(0o444)).unwrap();

    let output = dir.path().join("kernel.o");
    std::fs::hard_link(&blob, &output).unwrap();
    drop(store);

    let nvcc = dir.path().join("nvcc");
    let argv = vec![
        nvcc.to_string_lossy().into_owned(),
        "-c".to_string(),
        "kernel.cu".to_string(),
        "-o".to_string(),
        output.to_string_lossy().into_owned(),
    ];
    let err = run_nvcc(&config, &argv).unwrap_err();
    assert!(
        format!("{err:#}").contains("read-only cache blob"),
        "unexpected error: {err:#}"
    );
    assert_eq!(
        output.metadata().unwrap().ino(),
        blob.metadata().unwrap().ino()
    );
}

/// Entry/invocation compatibility, all four combinations: the
/// object is always required; the dep-info only when requested.
#[test]
fn nvcc_cache_entry_rejection_covers_all_combinations() {
    let with_dep = |extra: &[&str]| {
        let mut argv = vec!["nvcc", "-c", "k.cu", "-o", "k.o"];
        argv.extend(extra);
        NvccCompiler::with_extra_allowlist_flags(Vec::new())
            .parse(&argv.iter().map(|a| a.to_string()).collect::<Vec<_>>())
            .unwrap()
    };
    let requested = with_dep(&["-MF", "k.d"]);
    let unrequested = with_dep(&[]);
    let object_only = entry_meta_with_files(&["k.o"]);
    let both = entry_meta_with_files(&["k.o", "k.d"]);
    let dep_only = entry_meta_with_files(&["k.d"]);
    let empty = entry_meta_with_files(&[]);

    assert!(nvcc_cache_entry_rejection_reason(&requested, &both).is_none());
    assert!(nvcc_cache_entry_rejection_reason(&unrequested, &object_only).is_none());
    assert!(nvcc_cache_entry_rejection_reason(&unrequested, &both).is_none());
    assert!(nvcc_cache_entry_rejection_reason(&requested, &object_only).is_some());
    assert!(nvcc_cache_entry_rejection_reason(&requested, &dep_only).is_some());
    assert!(nvcc_cache_entry_rejection_reason(&requested, &empty).is_some());
    assert!(nvcc_cache_entry_rejection_reason(&unrequested, &empty).is_some());
}

/// A cached entry whose blob is gone fails the restore loudly
/// (fail-closed) instead of restoring thin air.
#[cfg(unix)]
#[test]
fn nvcc_restore_fails_closed_on_missing_blob() {
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    std::fs::create_dir_all(work.join("inc")).unwrap();
    std::fs::write(work.join("kernel.cu"), "__global__ void k() {}\n").unwrap();
    std::fs::write(work.join("inc").join("h.h"), "#pragma once\n").unwrap();

    let count = dir.path().join("count");
    let headers = work.join("inc").join("h.h");
    let (nvcc, _host) = write_fake_nvcc_toolchain(
        dir.path(),
        &work.join("kernel.cu"),
        headers.to_str().unwrap(),
        "nvcc: NVIDIA (R) Cuda compiler driver",
        "gcc (GCC) 13.2.0",
        &count,
        None,
        None,
        0,
        0,
    );

    let config = test_config(dir.path().join("cache"));
    let _daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), false);

    let argv = nvcc_compile_argv(&nvcc, &work, &[]);
    let (store, parsed, key) = seed_nvcc_entry(&config, &argv);
    assert_eq!(
        spool_intent_count(&config),
        0,
        "local-only compile queued an upload"
    );

    let meta = store.get(&key).unwrap().unwrap();
    for file in &meta.files {
        std::fs::remove_file(store.blob_path(&file.hash)).unwrap();
    }
    let err = restore_nvcc_from_cache(&store, &parsed, &meta).unwrap_err();
    assert!(
        format!("{err:#}").contains("evicted"),
        "unexpected error: {err:#}"
    );
}

/// The env-reading wrapper honors the same anchor rule against the
/// real working directory (kills whole-body mutants on the wrapper
/// that the `_from_cwd` tests cannot see).
#[test]
fn nvcc_depinfo_rewrite_root_uses_current_dir() {
    let _lock = crate::test_support::process_state_test_lock();
    let parsed = crate::compiler::nvcc::NvccCompiler::with_extra_allowlist_flags(Vec::new())
        .parse(&[
            "nvcc".to_string(),
            "-c".to_string(),
            "src/k.cu".to_string(),
            "-o".to_string(),
            "build/k.o".to_string(),
            "-MF".to_string(),
            "build/k.d".to_string(),
        ])
        .unwrap();
    let cwd = std::env::current_dir().unwrap();
    let anchor = nvcc_depinfo_rewrite_root(&parsed).unwrap();
    assert!(
        anchor.starts_with(&cwd),
        "anchor {anchor:?} must live under the current dir {cwd:?}"
    );
    assert_eq!(
        Some(anchor),
        nvcc_depinfo_rewrite_root_from_cwd(&parsed, &cwd)
    );
}

/// Dep-info anchors: same-tree outputs anchor on the common prefix,
/// disjoint trees fall back to the object dir, and no dep-info
/// request needs no anchor at all.
#[test]
fn nvcc_depinfo_rewrite_root_anchors() {
    let _lock = crate::test_support::process_state_test_lock();
    let parsed = crate::compiler::nvcc::NvccCompiler::with_extra_allowlist_flags(Vec::new())
        .parse(&[
            "nvcc".to_string(),
            "-c".to_string(),
            "src/k.cu".to_string(),
            "-o".to_string(),
            "build/k.o".to_string(),
            "-MF".to_string(),
            "build/k.d".to_string(),
        ])
        .unwrap();
    let cwd = Path::new("/work");
    assert_eq!(
        nvcc_depinfo_rewrite_root_from_cwd(&parsed, cwd),
        Some(PathBuf::from("/work"))
    );

    let parsed = crate::compiler::nvcc::NvccCompiler::with_extra_allowlist_flags(Vec::new())
        .parse(&[
            "nvcc".to_string(),
            "-c".to_string(),
            "src/k.cu".to_string(),
            "-o".to_string(),
            "/elsewhere/k.o".to_string(),
            "-MF".to_string(),
            "/elsewhere/k.d".to_string(),
        ])
        .unwrap();
    assert_eq!(
        nvcc_depinfo_rewrite_root_from_cwd(&parsed, cwd),
        Some(PathBuf::from("/elsewhere"))
    );

    let parsed = crate::compiler::nvcc::NvccCompiler::with_extra_allowlist_flags(Vec::new())
        .parse(&["nvcc".to_string(), "-c".to_string(), "k.cu".to_string()])
        .unwrap();
    assert_eq!(nvcc_depinfo_rewrite_root_from_cwd(&parsed, cwd), None);
}

/// #1015: a fallback wrapper such as sccache keys on the same preprocessor
/// output, so it would serve the stale object kache just refused to cache.
/// Only that refusal skips it; other key failures keep the fallback.
#[test]
fn only_hidden_input_key_failures_skip_the_fallback() {
    let hidden = anyhow::Error::new(crate::compiler::cc::CcHiddenInput {
        construct: ".incbin",
    });
    assert!(cc_key_error_skips_fallback(&hidden));
    assert!(!cc_key_error_skips_fallback(&anyhow::anyhow!(
        "cc -E key probe exited 1"
    )));
}

#[test]
fn untrusted_codegen_backend_bypasses_unless_trusted_and_keyable() {
    assert_eq!(untrusted_codegen_backend(None, false), None);
    assert_eq!(untrusted_codegen_backend(None, true), None);
    assert_eq!(
        untrusted_codegen_backend(Some("/b/backend.so"), false),
        Some("/b/backend.so"),
        "an untrusted backend always bypasses"
    );
    assert_eq!(
        untrusted_codegen_backend(Some("/b/backend.so"), true),
        None,
        "a trusted backend passed as a path is cached"
    );
    assert_eq!(
        untrusted_codegen_backend(Some("backend.so"), true),
        Some("backend.so"),
        "a trusted bare file name cannot be keyed"
    );
}

#[cfg(unix)]
#[test]
fn cc_direct_passthrough_bypasses_configured_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let fake_cc = dir.path().join("cc");
    let fallback = dir.path().join("fallback");
    let compiler_marker = dir.path().join("compiler-ran");
    let fallback_marker = dir.path().join("fallback-ran");
    let output = dir.path().join("output.o");
    let shell =
        crate::compiler::resolve_program_on_path("sh").expect("sh must be available on PATH");

    kache_fs::testutil::write_executable(
        &fake_cc,
        format!(
            "#!{}\nprintf direct > '{}'\n",
            shell.display(),
            compiler_marker.display()
        ),
    );
    kache_fs::testutil::write_executable(
        &fallback,
        format!(
            "#!{}\nprintf fallback > '{}'\n",
            shell.display(),
            fallback_marker.display()
        ),
    );

    let parsed = CcCompiler::new()
        .parse(&s(&[
            &fake_cc.to_string_lossy(),
            "-c",
            "foo.c",
            "-o",
            &output.to_string_lossy(),
        ]))
        .unwrap();
    assert!(
        !parsed.requires_compiler_output_semantics(),
        "the fallback branch must be eligible except for force_direct"
    );

    let mut config = test_config(dir.path().join("cache"));
    config.fallback = fallback.to_str().map(ToOwned::to_owned);
    let result = cc_direct_passthrough(&config, &parsed).unwrap();

    assert_eq!(result.exit_code, 0);
    assert!(!result.fallback);
    assert!(compiler_marker.exists());
    assert!(!fallback_marker.exists());
}

#[cfg(unix)]
#[test]
fn cc_direct_passthrough_refuses_legacy_cache_blob_hardlink() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let content = b"cached object";
    let hash = blake3::hash(content).to_hex().to_string();
    create_blob(&store, &hash, content);
    let blob = store.blob_path(&hash);
    std::fs::set_permissions(&blob, std::fs::Permissions::from_mode(0o444)).unwrap();

    let output = dir.path().join("output.o");
    std::fs::hard_link(&blob, &output).unwrap();
    drop(store);
    let index_path = config.index_db_path();
    std::fs::remove_file(&index_path).unwrap();
    std::fs::create_dir(&index_path).unwrap();

    let marker = dir.path().join("compiler-ran");
    let fake_cc = dir.path().join("cc");
    let shell =
        crate::compiler::resolve_program_on_path("sh").expect("sh must be available on PATH");
    kache_fs::testutil::write_executable(
        &fake_cc,
        format!(
            "#!{}\nprintf ran > '{}'\n",
            shell.display(),
            marker.display()
        ),
    );

    let parsed = CcCompiler::new()
        .parse(&s(&[
            &fake_cc.to_string_lossy(),
            "-c",
            "foo.c",
            "-o",
            &output.to_string_lossy(),
        ]))
        .unwrap();
    let error = cc_direct_passthrough(&config, &parsed).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("shares the read-only cache blob")
    );
    assert!(
        !marker.exists(),
        "compiler must not run over a shared blob inode"
    );
    assert!(
        index_path.is_dir(),
        "the direct-mode safety check must not open or repair the cache index"
    );
    assert_eq!(std::fs::read(&blob).unwrap(), content);
    assert_eq!(
        std::fs::metadata(&blob).unwrap().ino(),
        std::fs::metadata(&output).unwrap().ino()
    );
}

#[test]
fn local_hit_demand_reaches_event_without_remote_wait() {
    let _ = crate::demand::take();
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    put_test_entry(&store, dir.path(), "local-demand-key");
    let before = chrono::Utc::now().timestamp_millis() as u64;
    assert!(
        lookup_local_entry(&store, None, "local-demand-key")
            .unwrap()
            .is_some()
    );
    let after = chrono::Utc::now().timestamp_millis() as u64;
    log_event_with_store_stats(
        &config,
        "/repo",
        "foo",
        EventResult::LocalHit,
        10,
        20,
        30,
        "local-demand-key",
        0,
        FileHashStats::default(),
        0,
        0,
        0,
        StorePutResult::default(),
    );
    let events = crate::events::read_events(&config.event_log_path()).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].schema, 21);
    let demands = &events[0].demands;
    assert_eq!(demands.len(), 1);
    assert_eq!(demands[0].cache_key, "local-demand-key");
    assert!((before..=after).contains(&demands[0].first_demand_at_ms));
    assert_eq!(demands[0].remote_wait_ms, 0);
    assert!(crate::demand::take().is_empty());
}

/// Store stats and hash stats should be carried into the event JSONL entry
/// because reports rely on these schema-9 fields.
#[test]
fn log_event_with_store_stats_persists_timing_hash_and_store_fields() {
    let _ = crate::verify_compare::take_last_report();
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store_put = StorePutResult {
        output_blobs: 3,
        duplicate_blobs: 1,
        new_blobs: 2,
    };
    let hash_stats = FileHashStats {
        cache_hits: 4,
        cache_misses: 5,
        bytes_hashed: 6,
    };

    log_event_with_store_stats(
        &config,
        "/repo",
        "foo",
        EventResult::Miss,
        10,
        20,
        30,
        "cache-key",
        40,
        hash_stats,
        50,
        60,
        70,
        store_put,
    );

    let events = crate::events::read_events(&config.event_log_path()).unwrap();
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.root, "/repo");
    assert_eq!(event.crate_name, "foo");
    assert_eq!(event.result, EventResult::Miss);
    assert_eq!(event.elapsed_ms, 10);
    assert_eq!(event.compile_time_ms, 20);
    assert_eq!(event.size, 30);
    assert_eq!(event.cache_key, "cache-key");
    assert_eq!(event.schema, 21);
    assert_eq!(event.key_ms, 40);
    assert_eq!(event.key_hash_hits, 4);
    assert_eq!(event.key_hash_misses, 5);
    assert_eq!(event.key_hash_bytes, 6);
    assert_eq!(event.lookup_ms, 50);
    assert_eq!(event.restore_ms, 60);
    assert_eq!(event.store_ms, 70);
    assert_eq!(event.store_output_blobs, 3);
    assert_eq!(event.store_duplicate_blobs, 1);
    assert_eq!(event.store_new_blobs, 2);
    assert!(
        event.store_error.is_empty(),
        "a successful store records no failure reason"
    );
    assert!(
        event.verify_compare.is_empty(),
        "verify off must not invent a verify_compare class"
    );
}

/// Schema 17: the phases measured outside the wrapper's own timers reach
/// the event through the process-global accumulators. The counters only
/// grow and other tests in this binary add real milliseconds to them, so
/// each field is fed a magnitude ten times the last: a lower bound and a
/// band catch a zeroed field and a swapped one, whatever ran before.
#[test]
fn log_event_records_the_wrapper_phase_accumulators() {
    let _ = crate::verify_compare::take_last_report();
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));

    const STARTUP_MS: u64 = 5_000;
    const DEP_INFO_MS: u64 = 50_000;
    const FLIGHT_MS: u64 = 500_000;
    const PERMIT_MS: u64 = 5_000_000;
    let before = [
        crate::opcounts::startup_ms(),
        crate::opcounts::dep_info_ms(),
        crate::opcounts::flight_wait_ms(),
        crate::opcounts::permit_wait_ms(),
    ];
    let runs_before = crate::opcounts::dep_info_runs();
    crate::opcounts::record_startup(std::time::Duration::from_millis(STARTUP_MS));
    crate::opcounts::record_dep_info_run(std::time::Duration::from_millis(DEP_INFO_MS));
    crate::opcounts::record_flight_wait(std::time::Duration::from_millis(FLIGHT_MS));
    crate::opcounts::record_permit_wait(std::time::Duration::from_millis(PERMIT_MS));

    log_event_with_hash_stats(
        &config,
        "/repo",
        "foo",
        EventResult::Miss,
        100,
        20,
        30,
        "cache-key",
        40,
        FileHashStats::default(),
        50,
        0,
        60,
    );

    let events = crate::events::read_events(&config.event_log_path()).unwrap();
    let event = &events[0];
    assert_eq!(event.schema, 21);
    // Whatever other tests add is real time, far under the next band.
    for (name, value, floor, fed) in [
        ("startup_ms", event.startup_ms, before[0], STARTUP_MS),
        ("dep_info_ms", event.dep_info_ms, before[1], DEP_INFO_MS),
        ("flight_wait_ms", event.flight_wait_ms, before[2], FLIGHT_MS),
        ("permit_wait_ms", event.permit_wait_ms, before[3], PERMIT_MS),
    ] {
        assert!(value >= floor + fed, "{name} = {value}, fed {fed}");
        assert!(
            value < floor + fed * 10,
            "{name} = {value} carries another field's magnitude"
        );
    }
    assert!(event.dep_info_runs > runs_before);
    assert_eq!(event.wait_ms(), event.flight_wait_ms + event.permit_wait_ms);
}

/// With a pinned process start the wrapper clock is anchored there and
/// the time already spent is recorded as startup.
#[test]
fn wrapper_entry_anchors_at_the_pinned_process_start() {
    crate::opcounts::mark_process_start();
    let pinned = crate::opcounts::process_start().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    let before = crate::opcounts::startup_ms();
    let start = wrapper_entry();
    assert_eq!(start, pinned, "elapsed_ms must span from process start");
    assert!(
        crate::opcounts::startup_ms() >= before + 5,
        "the time before wrapper entry must be recorded as startup"
    );
}

#[test]
fn store_error_for_event_keeps_the_chain_but_bounds_the_shape() {
    // The whole anyhow chain, not just the outermost context — that is the
    // half that names the cause.
    let err =
        anyhow::anyhow!("Permission denied (os error 13)").context("creating blob shard directory");
    assert_eq!(
        store_error_for_event(&err),
        "creating blob shard directory: Permission denied (os error 13)"
    );

    // A newline would break the report row this string is printed inside.
    let multiline = anyhow::anyhow!("line one\nline two\r\tline three");
    let flattened = store_error_for_event(&multiline);
    assert!(!flattened.contains('\n') && !flattened.contains('\r'));
    assert_eq!(flattened, "line one line two  line three");

    // And it cannot grow without bound: this reason is persisted on every
    // failing compile.
    let huge = anyhow::anyhow!("x".repeat(5000));
    let capped = store_error_for_event(&huge);
    assert!(capped.ends_with("… [truncated]"));
    assert_eq!(
        capped.chars().count(),
        2048 + "… [truncated]".chars().count()
    );
}

/// A failed `Store::put` stays a `Miss` (the compiler ran) but carries the
/// reason, so the report can tell a cold miss from one that repeats forever
/// (kunobi-ninja/kache#629).
#[test]
fn log_event_with_store_outcome_persists_the_store_failure_reason() {
    let _ = crate::verify_compare::take_last_report();
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));

    log_event_with_store_outcome(
        &config,
        "/repo",
        "foo",
        EventResult::Miss,
        10,
        20,
        30,
        "cache-key",
        0,
        FileHashStats::default(),
        0,
        0,
        0,
        StorePutResult::default(),
        "refusing to cache zero-byte artifact: libfoo.rlib".to_string(),
    );

    let events = crate::events::read_events(&config.event_log_path()).unwrap();
    let event = &events[0];
    assert_eq!(
        event.result,
        EventResult::Miss,
        "the compiler ran, so it stays a miss and stays in the hit-rate denominator"
    );
    assert_eq!(
        event.store_error,
        "refusing to cache zero-byte artifact: libfoo.rlib"
    );
    assert!(
        event.verify_compare.is_empty(),
        "a store-failure miss must not invent a verify_compare class"
    );
}

#[test]
fn log_event_persists_same_key_lookup_rejection() {
    let _ = crate::verify_compare::take_last_report();
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));

    log_event_with_store_and_lookup_outcome(
        &config,
        "/repo",
        "foo.c",
        EventResult::Miss,
        10,
        20,
        30,
        "same-key",
        0,
        FileHashStats::default(),
        1,
        0,
        2,
        StorePutResult {
            output_blobs: 2,
            duplicate_blobs: 0,
            new_blobs: 2,
        },
        String::new(),
        "matching entry lacks dep-info required by this invocation".to_string(),
    );

    let events = crate::events::read_events(&config.event_log_path()).unwrap();
    let event = &events[0];
    assert_eq!(event.result, EventResult::Miss);
    assert_eq!(event.cache_key, "same-key");
    assert_eq!(event.schema, 21);
    assert_eq!(
        event.lookup_rejection,
        "matching entry lacks dep-info required by this invocation"
    );
    assert!(event.store_error.is_empty());
    assert!(
        event.verify_compare.is_empty(),
        "lookup rejection must not invent a verify_compare class"
    );
}

/// `verify_compare` (schema 16) is read from the hit-qualification stash.
/// Empty when verify did not run; the class string when it did.
#[test]
fn log_event_persists_verify_compare_class_on_hit() {
    let _ = crate::verify_compare::take_last_report();
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));

    log_event_with_hash_stats(
        &config,
        "/repo",
        "foo",
        EventResult::LocalHit,
        1,
        20,
        30,
        "hit-key",
        0,
        FileHashStats::default(),
        0,
        0,
        0,
    );
    let events = crate::events::read_events(&config.event_log_path()).unwrap();
    assert_eq!(events[0].schema, 21);
    assert_eq!(events[0].result, EventResult::LocalHit);
    assert!(
        events[0].verify_compare.is_empty(),
        "no qualification run must leave verify_compare empty"
    );

    crate::verify_compare::record_report("content: libfoo.rlib (byte mismatch)".to_string());
    log_event_with_hash_stats(
        &config,
        "/repo",
        "foo",
        EventResult::LocalHit,
        2,
        20,
        30,
        "hit-key",
        0,
        FileHashStats::default(),
        0,
        1,
        0,
    );
    let events = crate::events::read_events(&config.event_log_path()).unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[1].schema, 21);
    assert_eq!(
        events[1].verify_compare,
        "content: libfoo.rlib (byte mismatch)"
    );
}

/// Passthrough events intentionally omit cache timings but preserve the
/// structured reason, fallback marker, and compiler exit code.
#[test]
fn log_passthrough_event_persists_reason_fallback_and_exit_code() {
    let _ = crate::verify_compare::take_last_report();
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let output = PassthroughOutput {
        exit_code: 42,
        fallback: true,
        fallback_attempt: None,
    };

    log_passthrough_event(
        &config,
        "/repo",
        "foo",
        17,
        "unsupported|cc link mode — not yet".to_string(),
        &output,
    );

    let events = crate::events::read_events(&config.event_log_path()).unwrap();
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.result, EventResult::Passthrough);
    assert_eq!(event.elapsed_ms, 17);
    assert_eq!(
        event.passthrough_reason,
        "unsupported|cc link mode — not yet"
    );
    assert!(event.fallback);
    assert_eq!(event.exit_code, Some(42));
    assert_eq!(event.cache_key, "");
    assert!(
        event.verify_compare.is_empty(),
        "passthrough must not invent a verify_compare class"
    );
}

/// The emit gate must reject a missing requested output kind, while
/// accepting supersets and ignoring kinds kache cannot classify.
#[test]
fn missing_requested_emit_detects_only_gated_absent_outputs() {
    let mut args = rustc_args(&[
        "rustc",
        "src/lib.rs",
        "--crate-name",
        "foo",
        "--emit",
        "metadata,link,llvm-ir,llvm-bc",
    ]);
    let artifacts = ArtifactSet::from_output_files(
        vec![
            (PathBuf::from("libfoo.rlib"), "libfoo.rlib".to_string()),
            (PathBuf::from("libfoo.rmeta"), "libfoo.rmeta".to_string()),
            (PathBuf::from("foo.ll"), "foo.ll".to_string()),
        ],
        classify_by_filename,
    );

    assert_eq!(
        missing_requested_emit(&args, &artifacts),
        Some("llvm-bc".to_string())
    );

    args.emit = vec![
        "metadata".to_string(),
        "link".to_string(),
        "debug-info".to_string(),
    ];
    assert_eq!(missing_requested_emit(&args, &artifacts), None);
}

fn names(items: &[&str]) -> Vec<String> {
    items.iter().map(|item| item.to_string()).collect()
}

#[test]
fn unaudited_bundled_member_finds_only_uncovered_archive_members() {
    let rustc_own = [
        "/",
        "//",
        "/SYM64/",
        "__.SYMDEF SORTED",
        "lib.rmeta",
        "lib.rmeta-link",
        "mylib-0123.mylib.a1b2-cgu.0.rcgu.o",
        "mylib-0123.mylib.a1b2-cgu.0.rcgu.dwo",
    ];
    let mut candidates = names(&rustc_own);
    candidates.extend(names(&["util.o", "libfoo.a"]));
    assert_eq!(
        unaudited_bundled_member(&names(&rustc_own), &[], &candidates),
        None,
        "rustc's own members are never foreign"
    );

    let with = |extra: &[&str]| {
        let mut members = names(&rustc_own);
        members.extend(names(extra));
        members
    };
    assert_eq!(
        unaudited_bundled_member(&with(&["util.o"]), &names(&["util.o"]), &candidates),
        None,
        "a keyed archive covers its member"
    );
    assert_eq!(
        unaudited_bundled_member(&with(&["libfoo.a"]), &[], &candidates),
        Some("libfoo.a".to_string()),
        "a packed +whole-archive library matches its file name"
    );
    assert_eq!(
        unaudited_bundled_member(
            &with(&["util.o", "util.o"]),
            &names(&["util.o"]),
            &candidates
        ),
        Some("util.o".to_string()),
        "one keyed name covers one member"
    );
    assert_eq!(
        unaudited_bundled_member(&with(&["other.o"]), &[], &candidates),
        None,
        "a member no candidate holds came from elsewhere"
    );
}

/// A GNU `ar` archive of short-named members, each holding `data` (of
/// even length, so no member needs padding).
fn ar_archive_of(members: &[(&str, &[u8])]) -> Vec<u8> {
    let mut bytes = b"!<arch>\n".to_vec();
    for (member, data) in members {
        assert_eq!(data.len() % 2, 0);
        let name = format!("{member}/");
        bytes.extend_from_slice(
            format!(
                "{name:<16}{:<12}{:<6}{:<6}{:<8}{:<10}`\n",
                0,
                0,
                0,
                644,
                data.len()
            )
            .as_bytes(),
        );
        bytes.extend_from_slice(data);
    }
    bytes
}

/// [`ar_archive_of`] with two bytes in each member.
fn ar_archive(members: &[&str]) -> Vec<u8> {
    let members: Vec<(&str, &[u8])> = members.iter().map(|name| (*name, &b"xx"[..])).collect();
    ar_archive_of(&members)
}

fn keyed_native(
    archives: Vec<PathBuf>,
    bundled: Vec<(PathBuf, bool)>,
    dirs: Vec<PathBuf>,
) -> crate::cache_key::KeyedNativeArchives {
    crate::cache_key::KeyedNativeArchives {
        archives,
        bundled: bundled
            .into_iter()
            .map(|(path, packed)| crate::cache_key::BundledArchive { path, packed })
            .collect(),
        dirs,
    }
}

/// An `ArtifactSet` holding one rlib with these members.
fn rlib_artifacts(dir: &Path, members: &[(&str, &[u8])]) -> ArtifactSet {
    let rlib = dir.join("libmylib.rlib");
    std::fs::write(&rlib, ar_archive_of(members)).unwrap();
    ArtifactSet::from_output_files(
        vec![(rlib, "libmylib.rlib".to_string())],
        classify_by_filename,
    )
}

/// An rlib that carries a member of an unkeyed archive in its `-L` dir is
/// refused; the same rlib with that archive keyed, a unit that is no rlib,
/// and an rlib with no native dir are not audited.
#[test]
fn unaudited_native_bundle_reads_the_rlib_and_its_native_dirs() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let archive = out.join("libbundled.a");
    std::fs::write(&archive, ar_archive(&["value.o"])).unwrap();
    std::fs::write(out.join("libother.a"), ar_archive(&["other.o"])).unwrap();
    let artifacts = rlib_artifacts(dir.path(), &[("lib.rmeta", b"xx"), ("value.o", b"xx")]);
    let lib = rustc_args(&["rustc", "src/lib.rs", "--crate-type", "lib"]);

    let unkeyed = keyed_native(vec![], vec![], vec![out.clone()]);
    assert_eq!(
        unaudited_native_bundle(&lib, &artifacts, &unkeyed).unwrap(),
        Some("value.o".to_string())
    );
    let keyed = keyed_native(
        vec![archive.clone()],
        vec![(archive.clone(), false)],
        vec![out.clone()],
    );
    assert_eq!(
        unaudited_native_bundle(&lib, &artifacts, &keyed).unwrap(),
        None
    );
    let bin = rustc_args(&["rustc", "src/main.rs", "--crate-type", "bin"]);
    assert_eq!(
        unaudited_native_bundle(&bin, &artifacts, &unkeyed).unwrap(),
        None
    );
    assert_eq!(
        unaudited_native_bundle(&lib, &artifacts, &keyed_native(vec![], vec![], vec![])).unwrap(),
        None
    );
    assert_eq!(
        unaudited_native_bundle(&lib, &ArtifactSet::default(), &unkeyed).unwrap(),
        None,
        "no rlib output, nothing to audit"
    );

    // glibc ships `libm.a` as a linker script; rustc cannot bundle it.
    std::fs::write(out.join("libm.a"), b"/* GNU ld script */\nGROUP ( x )\n").unwrap();
    assert_eq!(
        unaudited_native_bundle(&lib, &artifacts, &unkeyed).unwrap(),
        Some("value.o".to_string()),
        "a file that is no archive is no candidate"
    );

    std::fs::write(out.join("libthin.a"), b"!<thin>\n").unwrap();
    assert!(
        unaudited_native_bundle(&lib, &artifacts, &unkeyed).is_err(),
        "a candidate that cannot be read refuses the store"
    );
    assert_eq!(
        unaudited_native_bundle(&lib, &artifacts, &keyed).unwrap(),
        None,
        "candidates are read only for an uncovered member"
    );
}

/// A keyed archive accounts only for the members the rlib takes from it.
/// A packed `+whole-archive` library is one member under its file name,
/// so its own members must not cover an unkeyed archive's same-named one.
#[test]
fn bundle_audit_credits_only_what_the_rlib_carries() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let foo = out.join("libfoo.a");
    std::fs::write(&foo, ar_archive(&["util.o"])).unwrap();
    std::fs::write(out.join("libbar.a"), ar_archive(&["util.o"])).unwrap();
    let lib = rustc_args(&["rustc", "src/lib.rs", "--crate-type", "lib"]);
    let audit = |members: &[(&str, &[u8])], bundled: Vec<(PathBuf, bool)>| {
        let artifacts = rlib_artifacts(dir.path(), members);
        let native = keyed_native(vec![foo.clone()], bundled, vec![out.clone()]);
        unaudited_native_bundle(&lib, &artifacts, &native).unwrap()
    };

    let packed = [
        ("lib.rmeta", &b"xx"[..]),
        ("libfoo.a", b"xx"),
        ("util.o", b"xx"),
    ];
    assert_eq!(
        audit(&packed, vec![(foo.clone(), true)]),
        Some("util.o".to_string()),
        "bar's util.o beside a packed foo"
    );
    assert_eq!(
        audit(&packed, vec![]),
        Some("util.o".to_string()),
        "a keyed archive the rlib does not bundle covers nothing"
    );
    let unpacked = [("lib.rmeta", &b"xx"[..]), ("util.o", b"xx")];
    assert_eq!(audit(&unpacked, vec![(foo.clone(), false)]), None);

    assert_eq!(
        bundle_credits(&keyed_native(vec![], vec![(foo.clone(), true)], vec![]).bundled).unwrap(),
        ["libfoo.a"]
    );
    assert_eq!(
        bundle_credits(&keyed_native(vec![], vec![(foo.clone(), false)], vec![]).bundled).unwrap(),
        ["util.o"]
    );
}

/// rustc writes `raw-dylib` imports into the rlib as import-library
/// members named after the DLL; an import library in the `-L` dirs holds
/// members of the same names, and neither is a bundled archive.
#[test]
fn bundle_audit_leaves_raw_dylib_import_members_alone() {
    const STUB: &[u8] = b"\0\0\xff\xff\x64\x86stub";
    let member = |name: &str, short_import| crate::native_archive::ArchiveMember {
        name: name.to_string(),
        short_import,
    };
    assert_eq!(
        without_import_members(&[
            member("lib.rmeta", false),
            member("kernel32.dll", true),
            member("kernel32.dll", false),
            member("util.o", false),
        ]),
        ["lib.rmeta", "util.o"]
    );

    let dir = tempfile::tempdir().unwrap();
    let lib_dir = dir.path().join("lib");
    std::fs::create_dir_all(&lib_dir).unwrap();
    std::fs::write(
        lib_dir.join("windows.0.53.0.lib"),
        ar_archive_of(&[("kernel32.dll", STUB), ("kernel32.dll", b"desc")]),
    )
    .unwrap();
    let artifacts = rlib_artifacts(
        dir.path(),
        &[
            ("lib.rmeta", b"xx"),
            ("kernel32.dll", STUB),
            ("kernel32.dll", b"desc"),
        ],
    );
    let lib = rustc_args(&["rustc", "src/lib.rs", "--crate-type", "lib"]);
    let native = keyed_native(vec![], vec![], vec![lib_dir]);
    assert_eq!(
        unaudited_native_bundle(&lib, &artifacts, &native).unwrap(),
        None
    );
}

/// An entry whose recorded emit set is narrower than the invocation is
/// evicted and reported as a restore miss instead of serving a partial hit.
/// kunobi-ninja/kache#330: a cached `.d` whose expanded paths do not
/// resolve for THIS consumer poisons cargo's freshness check into a
/// permanent recompile loop (the recompile restores the same broken
/// `.d`). The restore gate must evict the entry and miss so the
/// recompile stores a portable one.
#[test]
fn restore_evicts_entry_whose_depinfo_references_missing_paths() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let out_dir = dir.path().join("target/debug/deps");
    std::fs::create_dir_all(&out_dir).unwrap();
    let args = rustc_args(&[
        "rustc",
        "src/lib.rs",
        "--crate-name",
        "foo",
        "--emit",
        "dep-info,link",
        "--out-dir",
        out_dir.to_str().unwrap(),
    ]);

    // A real source the .d also lists, so only the donor-absolute path
    // is missing — the exact field-report shape.
    let real_src = dir.path().join("lib.rs");
    std::fs::write(&real_src, "pub fn f() {}\n").unwrap();
    let dep_content = format!(
        "{}/foo.rlib: {} /donor/project/target/debug/build/gen-8a22/out/generated.rs\n",
        out_dir.display(),
        real_src.display(),
    );
    let dep_hash = blake3::hash(dep_content.as_bytes()).to_hex().to_string();
    let rlib_hash = blake3::hash(b"rlib bytes").to_hex().to_string();
    create_blob(&store, &dep_hash, dep_content.as_bytes());
    create_blob(&store, &rlib_hash, b"rlib bytes");

    let mut dep_file = cached_file("foo.d", &dep_hash);
    dep_file.size = dep_content.len() as u64;
    let mut rlib_file = cached_file("libfoo.rlib", &rlib_hash);
    rlib_file.size = "rlib bytes".len() as u64;
    let meta = entry_meta(
        "poisoned-key",
        vec![dep_file, rlib_file],
        &["dep-info", "link"],
    );
    let entry_dir = store.entry_dir(&meta.cache_key);
    std::fs::create_dir_all(&entry_dir).unwrap();
    std::fs::write(
        entry_dir.join("meta.json"),
        serde_json::to_string(&meta).unwrap(),
    )
    .unwrap();
    store.insert_entry_row_for_test("poisoned-key");

    let err = restore_from_cache(&config, &RustcCompiler::new(), &store, &args, &meta, None)
        .unwrap_err()
        .to_string();

    assert!(
        err.contains("does not resolve here"),
        "unexpected error: {err}"
    );
    assert!(
        !store.entry_dir(&meta.cache_key).join("meta.json").exists(),
        "the poisoned entry must be evicted so the recompile stores a portable one"
    );
    assert!(
        !out_dir.join("foo.d").exists(),
        "nothing may be materialized before the gate"
    );
}

#[test]
fn restore_evicts_incomplete_extra_inputs_depinfo_entries() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let project = dir.path().join("project");
    let source = project.join("src/lib.rs");
    std::fs::create_dir_all(project.join("src")).unwrap();
    std::fs::create_dir_all(project.join("data")).unwrap();
    std::fs::write(
        project.join("Cargo.toml"),
        "[package]\nname='foo'\nversion='0.1.0'\n",
    )
    .unwrap();
    std::fs::write(&source, "pub fn f() {}\n").unwrap();
    std::fs::write(
        project.join("kache.toml"),
        "extra_inputs = [\"data/**/*.txt\"]\n",
    )
    .unwrap();
    std::fs::write(project.join("data/value.txt"), "v1").unwrap();

    let out_dir = dir.path().join("target/debug/deps");
    let args = rustc_args(&[
        "rustc",
        source.to_str().unwrap(),
        "--crate-name",
        "foo",
        "--emit",
        "dep-info",
        "--out-dir",
        out_dir.to_str().unwrap(),
    ]);
    let snapshot = crate::extra_inputs::ExtraInputsSnapshot::resolve(
        args.source_file.as_deref(),
        "foo",
        args.is_primary,
        &crate::cache_key::FileHasher::new(),
    )
    .unwrap()
    .unwrap();

    let malformed = "not rustc dep-info\n";
    let dep_hash = blake3::hash(malformed.as_bytes()).to_hex().to_string();
    create_blob(&store, &dep_hash, malformed.as_bytes());
    let mut dep_file = cached_file("foo.d", &dep_hash);
    dep_file.size = malformed.len() as u64;
    let meta = entry_meta("malformed-extra-key", vec![dep_file], &["dep-info"]);
    let entry_dir = store.entry_dir(&meta.cache_key);
    std::fs::create_dir_all(&entry_dir).unwrap();
    std::fs::write(
        entry_dir.join("meta.json"),
        serde_json::to_string(&meta).unwrap(),
    )
    .unwrap();
    store.insert_entry_row_for_test(&meta.cache_key);

    let error = restore_from_cache(
        &config,
        &RustcCompiler::new(),
        &store,
        &args,
        &meta,
        Some(&snapshot),
    )
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("cannot be completed safely"),
        "{error:#}"
    );
    assert!(
        !entry_dir.join("meta.json").exists(),
        "malformed cached dep-info must be evicted before hit publication"
    );
    assert!(!out_dir.join("foo.d").exists());

    // A pre-emit-gate entry has no `emit_kinds`, so the generic coverage
    // check intentionally accepts it. Active extra inputs must still
    // require a concrete dep-info artifact before publishing a hit.
    let rlib_bytes = b"legacy rlib";
    let rlib_hash = blake3::hash(rlib_bytes).to_hex().to_string();
    create_blob(&store, &rlib_hash, rlib_bytes);
    let mut rlib_file = cached_file("libfoo.rlib", &rlib_hash);
    rlib_file.size = rlib_bytes.len() as u64;
    let legacy_meta = entry_meta("legacy-no-depinfo-key", vec![rlib_file], &[]);
    let legacy_entry_dir = store.entry_dir(&legacy_meta.cache_key);
    std::fs::create_dir_all(&legacy_entry_dir).unwrap();
    std::fs::write(
        legacy_entry_dir.join("meta.json"),
        serde_json::to_string(&legacy_meta).unwrap(),
    )
    .unwrap();
    store.insert_entry_row_for_test(&legacy_meta.cache_key);

    let error = restore_from_cache(
        &config,
        &RustcCompiler::new(),
        &store,
        &args,
        &legacy_meta,
        Some(&snapshot),
    )
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("has no dep-info artifact"),
        "{error:#}"
    );
    assert!(
        !legacy_entry_dir.join("meta.json").exists(),
        "legacy entry without dep-info must be evicted before hit publication"
    );

    // A differently named `.d` is not the output Cargo expects for this
    // unit. Treat it exactly like a missing legacy dep-info artifact.
    let wrong_dep = "other: src/lib.rs\n";
    let wrong_hash = blake3::hash(wrong_dep.as_bytes()).to_hex().to_string();
    create_blob(&store, &wrong_hash, wrong_dep.as_bytes());
    let mut wrong_file = cached_file("other.d", &wrong_hash);
    wrong_file.size = wrong_dep.len() as u64;
    let wrong_meta = entry_meta("legacy-wrong-depinfo-key", vec![wrong_file], &[]);
    let wrong_entry_dir = store.entry_dir(&wrong_meta.cache_key);
    std::fs::create_dir_all(&wrong_entry_dir).unwrap();
    std::fs::write(
        wrong_entry_dir.join("meta.json"),
        serde_json::to_string(&wrong_meta).unwrap(),
    )
    .unwrap();
    store.insert_entry_row_for_test(&wrong_meta.cache_key);

    let error = restore_from_cache(
        &config,
        &RustcCompiler::new(),
        &store,
        &args,
        &wrong_meta,
        Some(&snapshot),
    )
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("has no dep-info artifact named foo.d"),
        "{error:#}"
    );
    assert!(!wrong_entry_dir.join("meta.json").exists());

    // Cargo skips env-dep records even when their values contain `: `.
    // Restore validation must inspect the following Make rule and evict a
    // consumer-invalid path instead of accepting an empty dependency set.
    let missing_dependency = project.join("does-not-exist.rs");
    let env_prefixed = format!(
        "# env-dep:CFG=foo: bar\nfoo: {}\n",
        missing_dependency.display()
    );
    let env_hash = blake3::hash(env_prefixed.as_bytes()).to_hex().to_string();
    create_blob(&store, &env_hash, env_prefixed.as_bytes());
    let mut env_file = cached_file("foo.d", &env_hash);
    env_file.size = env_prefixed.len() as u64;
    let env_meta = entry_meta("env-prefixed-depinfo-key", vec![env_file], &["dep-info"]);
    let env_entry_dir = store.entry_dir(&env_meta.cache_key);
    std::fs::create_dir_all(&env_entry_dir).unwrap();
    std::fs::write(
        env_entry_dir.join("meta.json"),
        serde_json::to_string(&env_meta).unwrap(),
    )
    .unwrap();
    store.insert_entry_row_for_test(&env_meta.cache_key);

    let error = restore_from_cache(
        &config,
        &RustcCompiler::new(),
        &store,
        &args,
        &env_meta,
        Some(&snapshot),
    )
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("does not resolve here"),
        "{error:#}"
    );
    assert!(!env_entry_dir.join("meta.json").exists());
}

#[test]
fn compile_revalidation_rejects_nested_directory_aba() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let project = dir.path().join("project");
    let source = project.join("src/lib.rs");
    let nested = project.join("data/deep");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::create_dir_all(source.parent().unwrap()).unwrap();
    std::fs::write(
        project.join("Cargo.toml"),
        "[package]\nname='foo'\nversion='0.1.0'\n",
    )
    .unwrap();
    std::fs::write(&source, "pub fn f() {}\n").unwrap();
    std::fs::write(
        project.join("kache.toml"),
        "extra_inputs = [\"data/**/*.txt\"]\n",
    )
    .unwrap();
    std::fs::write(project.join("data/stable.txt"), "v1").unwrap();

    let args = rustc_args(&[
        "rustc",
        source.to_str().unwrap(),
        "--crate-name",
        "foo",
        "--emit",
        "dep-info",
        "--out-dir",
        dir.path().join("out").to_str().unwrap(),
    ]);
    let before = crate::extra_inputs::ExtraInputsSnapshot::resolve(
        args.source_file.as_deref(),
        "foo",
        args.is_primary,
        &crate::cache_key::FileHasher::new(),
    )
    .unwrap()
    .unwrap();

    let transient = nested.join("transient.txt");
    std::fs::write(&transient, "transient").unwrap();
    std::fs::remove_file(&transient).unwrap();
    filetime::set_file_mtime(
        &nested,
        filetime::FileTime::from_unix_time(2_000_000_000, 123),
    )
    .unwrap();
    let after = crate::extra_inputs::ExtraInputsSnapshot::resolve(
        args.source_file.as_deref(),
        "foo",
        args.is_primary,
        &crate::cache_key::FileHasher::new(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(before.digest(), after.digest());
    assert_ne!(before, after);
    assert!(extra_inputs_changed_during_compile(
        &config,
        &args,
        Some(&before),
        i64::MAX,
    ));
}

#[test]
fn activation_from_none_is_rejected_on_miss_hit_and_success_paths() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let project = dir.path().join("project");
    let source = project.join("src/lib.rs");
    let out_dir = dir.path().join("target/debug/deps");
    std::fs::create_dir_all(source.parent().unwrap()).unwrap();
    std::fs::create_dir_all(project.join("data")).unwrap();
    std::fs::write(
        project.join("Cargo.toml"),
        "[package]\nname='foo'\nversion='0.1.0'\n",
    )
    .unwrap();
    std::fs::write(&source, "pub fn f() {}\n").unwrap();
    std::fs::write(project.join("data/value.txt"), "v1").unwrap();

    let args = rustc_args(&[
        "rustc",
        source.to_str().unwrap(),
        "--crate-name",
        "foo",
        "--emit",
        "dep-info",
        "--out-dir",
        out_dir.to_str().unwrap(),
    ]);
    let initial = crate::extra_inputs::ExtraInputsSnapshot::resolve(
        args.source_file.as_deref(),
        "foo",
        args.is_primary,
        &crate::cache_key::FileHasher::new(),
    )
    .unwrap();
    assert!(initial.is_none());

    std::fs::write(
        project.join("kache.toml"),
        "extra_inputs = [\"data/**/*.txt\"]\n",
    )
    .unwrap();

    // Miss/store and uncached passthrough lanes both use these two guards:
    // publication is suppressed, then a successful compiler exit is turned
    // into a retry instead of accepting dep-info that omitted the new config.
    assert!(extra_inputs_changed_during_compile(
        &config,
        &args,
        initial.as_ref(),
        i64::MAX,
    ));
    let error =
        complete_current_extra_inputs_after_success(&config, &args, initial.as_ref()).unwrap_err();
    assert!(
        format!("{error:#}").contains("extra_inputs declaration changed"),
        "{error:#}"
    );

    // A cache hit must reject the same None -> Some transition before any
    // artifact is materialized.
    let dep_info = format!("foo: {}\n", source.display());
    let dep_hash = blake3::hash(dep_info.as_bytes()).to_hex().to_string();
    create_blob(&store, &dep_hash, dep_info.as_bytes());
    let mut dep_file = cached_file("foo.d", &dep_hash);
    dep_file.size = dep_info.len() as u64;
    let meta = entry_meta("pre-activation-key", vec![dep_file], &["dep-info"]);
    let error = restore_from_cache(
        &config,
        &RustcCompiler::new(),
        &store,
        &args,
        &meta,
        initial.as_ref(),
    )
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("changed during cache lookup"),
        "{error:#}"
    );
    assert!(!out_dir.join("foo.d").exists());
}

#[test]
fn active_extra_inputs_reject_checksum_freshness_with_actionable_fallback() {
    let checksum_args = rustc_args(&[
        "rustc",
        "src/lib.rs",
        "--crate-name",
        "foo",
        "--emit",
        "dep-info,link",
        "-Z",
        "checksum-hash-algorithm=blake3",
    ]);
    let error = validate_extra_inputs_freshness_mode(&checksum_args, true).unwrap_err();
    let rendered = format!("{error:#}");
    for expected in [
        "extra_inputs cannot safely complete Cargo checksum-freshness dep-info yet",
        "disable -Z checksum-freshness",
        "KACHE_DISABLED=1",
        "cargo:rerun-if-changed",
    ] {
        assert!(rendered.contains(expected), "{rendered}");
    }
    assert!(validate_extra_inputs_freshness_mode(&checksum_args, false).is_ok());

    let normal_args = rustc_args(&[
        "rustc",
        "src/lib.rs",
        "--crate-name",
        "foo",
        "--emit",
        "dep-info,link",
    ]);
    assert!(validate_extra_inputs_freshness_mode(&normal_args, true).is_ok());
}

#[test]
fn restore_from_cache_rejects_entry_missing_requested_emit_kind() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let args = rustc_args(&[
        "rustc",
        "src/lib.rs",
        "--crate-name",
        "foo",
        "--emit",
        "metadata,link",
        "--out-dir",
        "target/debug/deps",
    ]);
    let meta = entry_meta(
        "partial-key",
        vec![cached_file("libfoo.rmeta", "0123456789abcdef")],
        &["metadata"],
    );
    // Production reaches this path through `get()`, so the entry always
    // has a DB row — and removal only cleans a directory whose row it
    // owns (#670). Register a real entry, then overwrite its meta.json
    // with the partial one under test.
    let seed = dir.path().join("seed.rmeta");
    std::fs::write(&seed, b"seed").unwrap();
    store
        .put(
            &meta.cache_key,
            "foo",
            &["lib".into()],
            &[],
            "",
            "dev",
            &[(seed, "libfoo.rmeta".into())],
            "",
            "",
        )
        .unwrap();
    let entry_dir = store.entry_dir(&meta.cache_key);
    std::fs::write(
        entry_dir.join("meta.json"),
        serde_json::to_string(&meta).unwrap(),
    )
    .unwrap();

    let err = restore_from_cache(&config, &RustcCompiler::new(), &store, &args, &meta, None)
        .unwrap_err()
        .to_string();

    assert!(
        err.contains("evicting partial entry"),
        "unexpected error: {err}"
    );
    assert!(
        !store.entry_dir(&meta.cache_key).exists(),
        "partial entry directory should be evicted"
    );
}

/// kunobi-ninja/kache#540: the restored `.rlib` is a compiler input for
/// every downstream crate in this build, and the entry already carries its
/// verified digest — so the restore must leave that digest in the file-hash
/// memo instead of letting the next cache key re-read the whole file.
#[test]
fn restore_seeds_the_file_hash_memo_with_the_restored_blobs_digest() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let out_dir = dir.path().join("target/debug/deps");

    // Above the memo's persistence floor, or no row would be kept at all.
    let content = vec![b'r'; 128 * 1024];
    let source = dir.path().join("source.rlib");
    std::fs::write(&source, &content).unwrap();
    let hash = crate::cache_key::hash_file(&source).unwrap();
    create_blob(&store, &hash, &content);

    let args = rustc_args(&[
        "rustc",
        "src/lib.rs",
        "--crate-name",
        "foo",
        "--emit",
        "link",
        "--out-dir",
        &out_dir.to_string_lossy(),
    ]);
    let meta = entry_meta("seed-key", vec![cached_file("libfoo.rlib", &hash)], &[]);

    restore_from_cache(&config, &RustcCompiler::new(), &store, &args, &meta, None).unwrap();

    let restored = out_dir.join("libfoo.rlib");
    assert_eq!(std::fs::read(&restored).unwrap(), content);

    match store.file_hash_lookup(&restored) {
        crate::cache_key::FileHashLookup::Hit(memoized) => assert_eq!(
            memoized, hash,
            "the memo must serve the digest the entry recorded"
        ),
        _ => panic!("restored artifact was not memoized"),
    }

    // And the payoff: hashing it as an input reads no bytes.
    let hasher = store.file_hasher();
    assert_eq!(hasher.hash(&restored).unwrap(), hash);
    let stats = hasher.stats();
    assert_eq!(stats.cache_hits, 1);
    assert_eq!(
        stats.bytes_hashed, 0,
        "a memoized restore must not re-read the artifact"
    );
}

/// The converse, and the reason the memo stays sound: a dep-info file is
/// re-rooted on restore, so its bytes are not the blob's and its recorded
/// digest must never be memoized for the restored path.
#[test]
fn restore_does_not_memoize_rewritten_dep_info() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    // `--out-dir` is what the dep-info anchor is derived from: the restored
    // `.d` is re-rooted at `<tmp>/target`.
    let out_dir = dir.path().join("target/debug/deps");
    std::fs::create_dir_all(&out_dir).unwrap();

    let source = dir.path().join("src/lib.rs");
    std::fs::create_dir_all(source.parent().unwrap()).unwrap();
    std::fs::write(&source, "pub fn f() {}\n").unwrap();

    // Padded past the memo's size floor so the assertion below can only
    // fail because seeding was skipped, not because the file was too small.
    let stored = format!(
        "__kache_root__/debug/deps/libfoo.rlib: {}\n#{}\n",
        source.display(),
        "p".repeat(128 * 1024)
    );
    let hash = {
        let blob = dir.path().join("blob.d");
        std::fs::write(&blob, &stored).unwrap();
        crate::cache_key::hash_file(&blob).unwrap()
    };
    create_blob(&store, &hash, stored.as_bytes());

    let args = rustc_args(&[
        "rustc",
        "src/lib.rs",
        "--crate-name",
        "foo",
        "--emit",
        "dep-info",
        "--out-dir",
        &out_dir.to_string_lossy(),
    ]);
    let meta = entry_meta("depinfo-key", vec![cached_file("foo.d", &hash)], &[]);

    restore_from_cache(&config, &RustcCompiler::new(), &store, &args, &meta, None).unwrap();

    let restored = out_dir.join("foo.d");
    assert_ne!(
        std::fs::read(&restored).unwrap(),
        stored.as_bytes(),
        "dep-info should have been re-rooted for this consumer"
    );
    assert!(
        matches!(
            store.file_hash_lookup(&restored),
            crate::cache_key::FileHashLookup::NeedsHash(_)
        ),
        "a rewritten artifact must not inherit the blob's digest"
    );
}

/// Restore refuses artifact names that would escape `--out-dir`; this is a
/// local trust-boundary check independent of remote import validation.
#[test]
fn restore_from_cache_rejects_unsafe_artifact_name() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let args = rustc_args(&[
        "rustc",
        "src/lib.rs",
        "--crate-name",
        "foo",
        "--emit",
        "link",
        "--out-dir",
        "target/debug/deps",
    ]);
    let meta = entry_meta(
        "unsafe-key",
        vec![cached_file("../escape.rlib", "0123456789abcdef")],
        &[],
    );

    let err = restore_from_cache(&config, &RustcCompiler::new(), &store, &args, &meta, None)
        .unwrap_err()
        .to_string();

    assert!(
        err.contains("unsafe artifact name"),
        "unexpected error: {err}"
    );
}

/// A rustc cache entry cannot be restored unless the invocation gives an
/// exact `-o` path or an `--out-dir` for artifact placement.
#[test]
fn restore_from_cache_requires_output_location() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let args = rustc_args(&["rustc", "src/lib.rs", "--crate-name", "foo"]);
    let meta = entry_meta("no-output-key", Vec::new(), &[]);

    let err = restore_from_cache(&config, &RustcCompiler::new(), &store, &args, &meta, None)
        .unwrap_err()
        .to_string();

    assert!(err.contains("no output path"), "unexpected error: {err}");
}

#[test]
fn retarget_rustc_args_rewrites_out_dir_output_and_emit_paths() {
    let staging = PathBuf::from("/tmp/kache-verify-stage");
    let args = rustc_args(&[
        "rustc",
        "src/lib.rs",
        "--crate-name",
        "foo",
        "--out-dir",
        "/orig/target/debug/deps",
        "-o",
        "/orig/target/debug/deps/libfoo.rlib",
        "--emit",
        "dep-info=/orig/target/debug/deps/foo.d,link",
    ]);
    let original_root = args.path_normalization_root().map(Path::to_path_buf);
    let staged = retarget_rustc_args_for_staging(&args, &staging);
    assert_eq!(staged.out_dir.as_deref(), Some(staging.as_path()));
    assert_eq!(
        staged.output.as_deref(),
        Some(staging.join("libfoo.rlib").as_path())
    );
    assert_eq!(
        staged.dep_info_output.as_deref(),
        Some(staging.join("foo.d").as_path())
    );
    assert_eq!(
        staged.path_normalization_root(),
        original_root.as_deref(),
        "qualification compile must keep the frozen remap root"
    );
    assert!(
        staged
            .all_args
            .iter()
            .any(|arg| arg == staging.to_str().unwrap()),
        "argv --out-dir value must move to staging: {:?}",
        staged.all_args
    );
    assert!(
        staged
            .all_args
            .iter()
            .any(|arg| arg.contains("dep-info=") && arg.contains("foo.d")),
        "explicit dep-info emit path must move to staging: {:?}",
        staged.all_args
    );
    assert!(
        !staged
            .all_args
            .iter()
            .any(|arg| arg.contains("/orig/target")),
        "original output paths must not remain in argv: {:?}",
        staged.all_args
    );
}

#[test]
fn verify_recompile_exit_status_reports_the_first_plain_stderr_line() {
    assert!(verify_recompile_exit_status(0, "error: ignored on success").is_ok());

    let error = verify_recompile_exit_status(
        2,
        "\n{\"message\":\"json diagnostic\"}\n  error: compile failed  \n",
    )
    .unwrap_err();
    assert_eq!(error.to_string(), "rustc exited 2 (error: compile failed)");
    assert_eq!(verify_recompile_stderr_hint(""), "");
    assert_eq!(
        verify_recompile_stderr_hint("\n{\"message\":\"json only\"}\n"),
        ""
    );
    assert_eq!(
        verify_recompile_stderr_hint("\n warning: plain diagnostic \n"),
        " (warning: plain diagnostic)"
    );
}

#[test]
fn rewrite_output_argv_rewrites_separate_values_without_shifting_other_args() {
    let staging = Path::new("/tmp/stage");
    let rewritten = rewrite_output_argv(
        &[
            "rustc".into(),
            "--out-dir".into(),
            "/old/deps".into(),
            "-o".into(),
            "/old/deps/libfoo.rlib".into(),
            "--emit".into(),
            "metadata,dep-info=/old/deps/foo.d".into(),
            "--cfg".into(),
            "feature=\"x\"".into(),
        ],
        staging,
    );
    assert_eq!(
        rewritten,
        vec![
            "rustc".to_string(),
            "--out-dir".to_string(),
            staging.display().to_string(),
            "-o".to_string(),
            staging.join("libfoo.rlib").display().to_string(),
            "--emit".to_string(),
            format!("metadata,dep-info={}", staging.join("foo.d").display()),
            "--cfg".to_string(),
            "feature=\"x\"".to_string(),
        ]
    );
}

#[test]
fn rewrite_output_argv_preserves_dangling_flags_and_empty_emit_paths() {
    let staging = Path::new("/tmp/stage");
    for flag in ["--out-dir", "-o", "--emit"] {
        let argv = vec!["rustc".to_string(), flag.to_string()];
        assert_eq!(rewrite_output_argv(&argv, staging), argv);
    }
    assert_eq!(
        rewrite_emit_value("metadata,dep-info=", staging),
        "metadata,dep-info="
    );
    assert_eq!(
        rewrite_emit_value("dep-info=/old/foo.d", staging),
        format!("dep-info={}", staging.join("foo.d").display())
    );
}

#[test]
fn rewrite_output_argv_handles_attached_out_dir_and_emit() {
    let staging = Path::new("/tmp/stage");
    let rewritten = rewrite_output_argv(
        &[
            "rustc".into(),
            "--out-dir=/old/deps".into(),
            "--emit=metadata,dep-info=/old/foo.d".into(),
            "-C".into(),
            "extra-filename=-abc".into(),
        ],
        staging,
    );
    assert_eq!(
        rewritten,
        vec![
            "rustc".to_string(),
            format!("--out-dir={}", staging.display()),
            format!(
                "--emit=metadata,dep-info={}",
                staging.join("foo.d").display()
            ),
            "-C".to_string(),
            "extra-filename=-abc".to_string(),
        ]
    );
}

#[test]
fn restore_from_cache_with_verify_on_is_fail_open_when_recompile_fails() {
    let _lock = crate::test_support::process_state_test_lock();
    let _verify = TestEnvGuard::set("KACHE_VERIFY", "1");
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let out_dir = dir.path().join("target/debug/deps");
    let content = b"restored-bytes";
    let source = dir.path().join("source.rlib");
    std::fs::write(&source, content).unwrap();
    let hash = crate::cache_key::hash_file(&source).unwrap();
    create_blob(&store, &hash, content);

    let args = rustc_args(&[
        "/no-such-rustc-kache-verify",
        "src/lib.rs",
        "--crate-name",
        "foo",
        "--emit",
        "link",
        "--out-dir",
        &out_dir.to_string_lossy(),
    ]);
    let mut file = cached_file("libfoo.rlib", &hash);
    file.size = content.len() as u64;
    let meta = entry_meta("verify-fail-open", vec![file], &["link"]);

    restore_from_cache(&config, &RustcCompiler::new(), &store, &args, &meta, None)
        .expect("qualification recompile failure must not fail the restore");

    assert_eq!(std::fs::read(out_dir.join("libfoo.rlib")).unwrap(), content);
    let summary = crate::verify_compare::take_last_report();
    assert!(
        summary.starts_with("recompile-failed:"),
        "expected a recompile-failed note, got {summary:?}"
    );
}

#[test]
fn restore_from_cache_skips_verify_when_flag_off() {
    let _lock = crate::test_support::process_state_test_lock();
    let _verify = TestEnvGuard::remove("KACHE_VERIFY");
    let _ = crate::verify_compare::take_last_report();
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().join("cache"));
    let store = Store::open(&config).unwrap();
    let out_dir = dir.path().join("target/debug/deps");
    let content = b"off-path-bytes";
    let source = dir.path().join("source.rlib");
    std::fs::write(&source, content).unwrap();
    let hash = crate::cache_key::hash_file(&source).unwrap();
    create_blob(&store, &hash, content);

    let args = rustc_args(&[
        "rustc",
        "src/lib.rs",
        "--crate-name",
        "foo",
        "--emit",
        "link",
        "--out-dir",
        &out_dir.to_string_lossy(),
    ]);
    let mut file = cached_file("libfoo.rlib", &hash);
    file.size = content.len() as u64;
    let meta = entry_meta("verify-off", vec![file], &["link"]);

    restore_from_cache(&config, &RustcCompiler::new(), &store, &args, &meta, None).unwrap();

    assert_eq!(std::fs::read(out_dir.join("libfoo.rlib")).unwrap(), content);
    assert!(
        crate::verify_compare::take_last_report().is_empty(),
        "flag off must not stash a verify_compare note"
    );
}

#[test]
fn session_marker_paths_differ_per_root() {
    // Root-scoped markers: parallel repos sharing one cache dir must not
    // suppress each other's sessions (#583 P0.5).
    let dir = tempfile::TempDir::new().unwrap();
    let config = test_config(dir.path().to_path_buf());
    let a = session_marker_path(&config, "/repo/a");
    let b = session_marker_path(&config, "/repo/b");
    assert_ne!(a, b);
    assert!(a.parent().unwrap().ends_with(".build-sessions"));
}

#[test]
fn session_markers_are_job_scoped_when_store_is_shared() {
    let dir = tempfile::tempdir().unwrap();
    let shared_cache = dir.path().join("shared-cache");
    let mut a = test_config(shared_cache.clone());
    let mut b = test_config(shared_cache);
    a.runtime_dir = dir.path().join("job-a");
    b.runtime_dir = dir.path().join("job-b");

    assert_eq!(a.store_dir(), b.store_dir());
    assert_ne!(
        session_marker_path(&a, "/repo"),
        session_marker_path(&b, "/repo")
    );
    assert!(session_marker_path(&a, "/repo").starts_with(&a.runtime_dir));
    assert!(session_marker_path(&b, "/repo").starts_with(&b.runtime_dir));
}

#[test]
fn remote_prefetch_creates_a_fresh_marker_in_the_job_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().join("workspace");
    let source = workspace.join("src/lib.rs");
    let out_dir = workspace.join("target/debug/deps");
    std::fs::create_dir_all(source.parent().unwrap()).unwrap();
    std::fs::create_dir_all(&out_dir).unwrap();
    std::fs::write(
        workspace.join("Cargo.toml"),
        "[package]\nname = 'runtime-prefetch-test'\nversion = '0.1.0'\n",
    )
    .unwrap();
    std::fs::write(&source, "pub fn value() -> u8 { 1 }\n").unwrap();

    let mut config = test_config(dir.path().join("shared-cache"));
    config.runtime_dir = dir.path().join("job-runtime");
    config.remote = Some(crate::config::RemoteConfig::test_s3(
        "test-bucket",
        "artifacts",
    ));
    let args = rustc_args(&[
        "rustc",
        source.to_str().unwrap(),
        "--crate-name",
        "runtime_prefetch_test",
        "--out-dir",
        out_dir.to_str().unwrap(),
    ]);

    maybe_trigger_prefetch(&config, &args);

    let root = std::fs::canonicalize(&workspace).unwrap();
    let marker = session_marker_path(&config, root.to_str().unwrap());
    assert!(marker.starts_with(&config.runtime_dir));
    let content = std::fs::read_to_string(&marker).expect("session marker created");
    let (_, session_id) = parse_session_marker(&content).expect("valid v1 marker");
    assert!(!session_id.is_empty());
    assert!(timestamp_is_fresh(&content, BUILD_SESSION_SECS));
    assert!(!config.cache_dir.join(".build-sessions").exists());
    // The hint is recorded against the session it was sent for.
    let sent = prefetch_marker_path(&config, root.to_str().unwrap());
    assert_eq!(std::fs::read_to_string(sent).unwrap(), session_id);
}

#[test]
fn only_a_cargo_compile_outside_every_layout_is_unrecognized() {
    let args = |out_dir: &str| {
        let mut args = RustcArgs::default();
        args.crate_name = Some("demo".into());
        args.out_dir = Some(PathBuf::from(out_dir));
        args
    };
    let cargo = Some(std::ffi::OsStr::new("demo"));
    let unknown = args("/w/target/debug/units/demo/out");
    assert_eq!(
        unrecognized_cargo_layout(&unknown, cargo),
        Some(Path::new("/w/target/debug/units/demo/out"))
    );
    for known in [
        "/w/target/debug/deps",
        "/w/target/debug/build/demo/0123456789abcdef/out",
    ] {
        assert_eq!(
            unrecognized_cargo_layout(&args(known), cargo),
            None,
            "{known}"
        );
    }
    assert_eq!(
        unrecognized_cargo_layout(&unknown, Some(std::ffi::OsStr::new("probe"))),
        None,
        "a build script's probe compiles another crate"
    );
    assert_eq!(unrecognized_cargo_layout(&unknown, None), None, "no Cargo");
    let mut no_crate = unknown.clone();
    no_crate.crate_name = None;
    assert_eq!(unrecognized_cargo_layout(&no_crate, cargo), None);
}

#[test]
fn the_unknown_layout_notice_shows_once_per_session() {
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::TempDir::new().unwrap();
    let config = test_config(dir.path().to_path_buf());
    let root = "/some/workspace";
    let target = tagged_target(&dir.path().join("w"));
    let mut unknown = RustcArgs::default();
    unknown.crate_name = Some("demo".into());
    unknown.out_dir = Some(target.join("debug/units/demo/out"));
    let mut known = unknown.clone();
    known.out_dir = Some(target.join("debug/deps"));
    let mut untagged = unknown.clone();
    untagged.out_dir = Some(dir.path().join("bazel-out/units/demo/out"));
    let previous = std::env::var_os("CARGO_CRATE_NAME");
    // SAFETY: the process-state lock serialises environment edits.
    unsafe { std::env::set_var("CARGO_CRATE_NAME", "demo") };
    // A minted session records the real clock, so the test runs on it.
    let now = now_epoch_secs();
    let shown = [
        maybe_notice_unknown_layout(&config, &untagged, root, now),
        maybe_notice_unknown_layout(&config, &known, root, now),
        maybe_notice_unknown_layout(&config, &unknown, root, now),
        maybe_notice_unknown_layout(&config, &unknown, root, now + 1),
        maybe_notice_unknown_layout(&config, &unknown, root, now + BUILD_SESSION_SECS + 2),
    ];
    // Another wrapper holding the marker is showing the line itself.
    let later = now + 3 * BUILD_SESSION_SECS;
    let marker = session_marker_path(&config, root).with_extension("layout");
    let held = open_marker_for_lock(&marker).unwrap();
    held.lock().unwrap();
    let while_held = maybe_notice_unknown_layout(&config, &unknown, root, later);
    drop(held);
    match previous {
        Some(value) => unsafe { std::env::set_var("CARGO_CRATE_NAME", value) },
        None => unsafe { std::env::remove_var("CARGO_CRATE_NAME") },
    }
    assert_eq!(shown, [false, false, true, false, true]);
    assert!(!while_held);
}

#[test]
fn session_id_for_event_joins_the_open_session_and_opens_one_after_idle() {
    let dir = tempfile::TempDir::new().unwrap();
    let config = test_config(dir.path().to_path_buf());
    let root = "/some/workspace";
    let marker = session_marker_path(&config, root);
    std::fs::create_dir_all(marker.parent().unwrap()).unwrap();

    // Touched within the window before the invocation started: join it.
    std::fs::write(&marker, "v1 10000 sess42").unwrap();
    assert_eq!(session_id_for_event(&config, root, 10000), "sess42");
    assert_eq!(
        session_id_for_event(&config, root, 10000 + BUILD_SESSION_SECS - 1),
        "sess42"
    );

    // A long compile that started while the session was open stays in
    // it, however late it logs (#583).
    assert_eq!(session_id_for_event(&config, root, 10100), "sess42");

    // Idle for the whole window: a new build, recorded in the marker.
    let started = 10000 + BUILD_SESSION_SECS;
    let minted = session_id_for_event(&config, root, started);
    assert!(!minted.is_empty());
    assert_ne!(minted, "sess42");
    let (_, recorded) = parse_session_marker(&std::fs::read_to_string(&marker).unwrap()).unwrap();
    assert_eq!(recorded, minted);
    // Every later compile of that build joins it.
    assert_eq!(
        session_id_for_event(&config, root, now_epoch_secs()),
        minted
    );
}

#[test]
fn session_id_for_event_opens_a_session_without_a_usable_marker() {
    let dir = tempfile::TempDir::new().unwrap();
    let config = test_config(dir.path().to_path_buf());

    // No marker yet: the first event opens the session (#1081).
    let first = session_id_for_event(&config, "/fresh", now_epoch_secs());
    assert_eq!(first.len(), 16);

    // A corrupt or id-less marker is not a session to join.
    let marker = session_marker_path(&config, "/broken");
    std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
    for content in ["garbage", &format!("v1 {} ", now_epoch_secs())] {
        std::fs::write(&marker, content).unwrap();
        assert_eq!(
            session_id_for_event(&config, "/broken", now_epoch_secs()).len(),
            16
        );
    }

    // Empty root: never a session, never a marker.
    assert_eq!(session_id_for_event(&config, "", now_epoch_secs()), "");
}

#[cfg(unix)]
#[test]
fn session_id_for_event_refuses_a_symlinked_marker() {
    let dir = tempfile::TempDir::new().unwrap();
    let config = test_config(dir.path().to_path_buf());
    let marker = session_marker_path(&config, "/linked");
    std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
    let target = dir.path().join("target_file");
    std::fs::write(&target, "untouched").unwrap();
    std::os::unix::fs::symlink(&target, &marker).unwrap();

    assert_eq!(
        session_id_for_event(&config, "/linked", now_epoch_secs()),
        ""
    );
    assert_eq!(std::fs::read_to_string(target).unwrap(), "untouched");
}

#[test]
fn prune_session_markers_removes_only_markers_idle_past_retention() {
    let dir = tempfile::TempDir::new().unwrap();
    let config = test_config(dir.path().to_path_buf());
    let sessions = config.runtime_dir.join(".build-sessions");
    std::fs::create_dir_all(sessions.join("not-a-marker")).unwrap();
    let now = std::time::SystemTime::now();
    let retention = std::time::Duration::from_secs(3600);
    let marker = |name: &str, age: std::time::Duration| {
        let path = sessions.join(name);
        let file = std::fs::File::create(&path).unwrap();
        file.set_modified(now - age).unwrap();
        path
    };
    let idle = marker("idle", retention);
    let idle_prefetch = marker("idle.prefetch", retention * 2);
    let recent = marker("recent", retention - std::time::Duration::from_secs(1));

    assert_eq!(prune_session_markers(&config, retention, now), 2);
    assert!(!idle.exists());
    assert!(!idle_prefetch.exists());
    assert!(recent.exists());
    assert!(sessions.join("not-a-marker").is_dir());

    // GC keeps a marker for a day after its last touch.
    assert_eq!(SESSION_MARKER_RETENTION.as_secs(), 24 * 3600);

    // Nothing to prune, or no directory at all, is not an error.
    assert_eq!(prune_session_markers(&config, retention, now), 0);
    let empty = test_config(dir.path().join("elsewhere"));
    assert_eq!(prune_session_markers(&empty, retention, now), 0);
}

#[test]
fn invocation_started_secs_counts_back_whole_seconds() {
    assert_eq!(invocation_started_secs(1000, 0), 1000);
    assert_eq!(invocation_started_secs(1000, 1999), 999);
    assert_eq!(invocation_started_secs(1000, 400_000), 600);
    assert_eq!(invocation_started_secs(10, 400_000), 0);
}

#[test]
fn locked_marker_helpers_replace_and_read_the_whole_content() {
    let dir = tempfile::TempDir::new().unwrap();
    let marker = dir.path().join("marker");
    std::fs::write(&marker, "a much longer previous record").unwrap();
    let file = open_marker_for_lock(&marker).unwrap();
    assert_eq!(read_locked_marker(&file), "a much longer previous record");
    write_locked_marker(&file, "short");
    assert_eq!(read_locked_marker(&file), "short");
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "short");
}

#[test]
fn prefetch_markers_parse_and_reject_malformed_failure_records() {
    assert_eq!(parse_prefetch_marker("  \n"), PrefetchMarker::Unset);
    assert_eq!(
        parse_prefetch_marker("abcd\n"),
        PrefetchMarker::Sent("abcd")
    );
    assert_eq!(
        parse_prefetch_marker("fail:abcd:100:2\n"),
        PrefetchMarker::Failed {
            until: 100,
            attempts: 2
        }
    );
    for malformed in [
        "fail:",
        "fail:abcd:100",
        "fail:abcd:soon:2",
        "fail:abcd:100:x",
    ] {
        assert_eq!(
            parse_prefetch_marker(malformed),
            PrefetchMarker::Unset,
            "{malformed}"
        );
    }
}

#[test]
fn a_failed_discovery_is_retried_only_once_its_wait_is_over() {
    let failed = PrefetchMarker::Failed {
        until: 100,
        attempts: 1,
    };
    assert!(!prefetch_due(&failed, "abcd", 99));
    assert!(prefetch_due(&failed, "abcd", 100));
    assert!(!prefetch_due(&PrefetchMarker::Sent("abcd"), "abcd", 100));
    assert!(prefetch_due(&PrefetchMarker::Sent("abcd"), "efgh", 100));
    assert!(prefetch_due(&PrefetchMarker::Unset, "abcd", 0));
}

#[test]
fn the_retry_wait_doubles_from_thirty_seconds_to_ten_minutes() {
    let waits: Vec<u64> = (0..=7).map(prefetch_retry_secs).collect();
    assert_eq!(waits, [30, 30, 60, 120, 240, 480, 600, 600]);
    assert_eq!(prefetch_retry_secs(u32::MAX), 600);
}

#[test]
fn a_failure_run_carries_across_sessions() {
    assert_eq!(
        failed_prefetch_marker("abcd", 1000, &PrefetchMarker::Unset),
        "fail:abcd:1030:1"
    );
    assert_eq!(
        failed_prefetch_marker("abcd", 1000, &PrefetchMarker::Sent("abcd")),
        "fail:abcd:1030:1"
    );
    assert_eq!(
        failed_prefetch_marker(
            "efgh",
            1000,
            &PrefetchMarker::Failed {
                until: 900,
                attempts: 3
            }
        ),
        "fail:efgh:1240:4"
    );
}

#[test]
fn a_failed_discovery_backs_off_and_a_success_clears_it() {
    // `KACHE_EVENT_ROOT` would move the event root, and with it the
    // marker, between calls.
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::TempDir::new().unwrap();
    let mut config = test_config(dir.path().to_path_buf());
    let mut args = rustc_args(&["rustc", "foo.rs"]);
    args.out_dir = Some(dir.path().join("target/debug/deps"));
    assert_eq!(
        maybe_trigger_prefetch_with(&config, &args, 0, || unreachable!()),
        PrefetchTrigger::NoRemote
    );
    config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "kache/"));
    let root = rustc_event_root(&args);
    let marker = prefetch_marker_path(&config, &root);
    let intent = || kache_core::BuildIntent {
        crate_names: vec!["serde".to_string()],
        namespace: None,
        cargo_lock_deps: Vec::new(),
        identity_key: None,
    };
    // The marker's lock is shared with every process that inherited its
    // descriptor, and a test elsewhere in this binary can spawn a child
    // while one of these calls holds it. That is another wrapper sending
    // the hint as far as the code can tell, so wait it out.
    let trigger = |now: u64, discover: &dyn Fn() -> Option<kache_core::BuildIntent>| {
        for _ in 0..200 {
            let outcome = maybe_trigger_prefetch_with(&config, &args, now, discover);
            if outcome != PrefetchTrigger::Busy {
                return outcome;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        PrefetchTrigger::Busy
    };

    let attempts = std::cell::Cell::new(0);
    let failing = || {
        attempts.set(attempts.get() + 1);
        None
    };
    let start = now_epoch_secs();
    assert_eq!(trigger(start, &failing), PrefetchTrigger::DiscoveryFailed);
    assert_eq!(attempts.get(), 1);
    assert!(std::fs::read_to_string(&marker).unwrap().ends_with(":1"));

    // Inside the 30 s wait: no second attempt.
    assert_eq!(trigger(start + 29, &failing), PrefetchTrigger::NotDue);
    assert_eq!(attempts.get(), 1);

    // After it: a second failure doubles the wait.
    assert_eq!(
        trigger(start + 30, &failing),
        PrefetchTrigger::DiscoveryFailed
    );
    assert_eq!(attempts.get(), 2);
    assert!(prefetch_due(
        &parse_prefetch_marker(&std::fs::read_to_string(&marker).unwrap()),
        "any",
        start + 90
    ));
    assert_eq!(trigger(start + 89, &failing), PrefetchTrigger::NotDue);
    assert_eq!(attempts.get(), 2);

    // While another process holds the marker's lock, nothing is sent.
    // Taking it here can itself find it busy, for the same reason as
    // `trigger`: a child spawned meanwhile shares the descriptor.
    let held = open_marker_for_lock(&marker).unwrap();
    let mut waited = 0;
    while held.try_lock().is_err() {
        assert!(waited < 200, "the marker lock stayed busy for 2 s");
        waited += 1;
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert_eq!(
        maybe_trigger_prefetch_with(&config, &args, start + 90, || unreachable!()),
        PrefetchTrigger::Busy
    );
    drop(held);

    // A success replaces the failure record with the session it sent for,
    // and later compiles of that session do not discover again.
    let sent = std::cell::Cell::new(0);
    let working = || {
        sent.set(sent.get() + 1);
        Some(intent())
    };
    assert_eq!(trigger(start + 90, &working), PrefetchTrigger::Sent);
    assert_eq!(sent.get(), 1);
    let recorded = std::fs::read_to_string(&marker).unwrap();
    assert!(matches!(
        parse_prefetch_marker(&recorded),
        PrefetchMarker::Sent(_)
    ));
    assert_eq!(trigger(start + 91, &working), PrefetchTrigger::NotDue);
    assert_eq!(sent.get(), 1);
}

#[test]
fn prefetch_marker_is_per_root_and_names_one_session() {
    let dir = tempfile::TempDir::new().unwrap();
    let config = test_config(dir.path().to_path_buf());
    let path = prefetch_marker_path(&config, "/repo");
    assert_eq!(
        path.parent(),
        session_marker_path(&config, "/repo").parent()
    );
    assert_ne!(path, session_marker_path(&config, "/repo"));
    assert_ne!(path, prefetch_marker_path(&config, "/other"));

    assert!(!prefetch_due(&parse_prefetch_marker("abc\n"), "abc", 0));
    assert!(prefetch_due(&parse_prefetch_marker("abd"), "abc", 0));
    assert!(prefetch_due(&parse_prefetch_marker(""), "abc", 0));
}

/// Cargo's target directory, tagged the way Cargo tags it.
fn tagged_target(workspace: &Path) -> PathBuf {
    let target = workspace.join("target");
    std::fs::create_dir_all(target.join("debug/build/anyhow-1234/out")).unwrap();
    std::fs::create_dir_all(target.join("debug/deps")).unwrap();
    std::fs::write(
        target.join("CACHEDIR.TAG"),
        "Signature: 8a477f597d28d172789f06886806bc55\n\
             # This file is a cache directory tag created by cargo.\n",
    )
    .unwrap();
    target
}

#[test]
fn cargo_workspace_of_finds_the_tagged_target_from_any_unit_dir() {
    let dir = tempfile::TempDir::new().unwrap();
    let workspace = dir.path().join("app");
    let target = tagged_target(&workspace);
    for unit_dir in [
        "debug/deps",
        "debug/build/anyhow-1234",
        "debug/build/anyhow-1234/out",
    ] {
        assert_eq!(
            cargo_workspace_of(&target.join(unit_dir)).as_deref(),
            Some(workspace.as_path()),
            "{unit_dir}"
        );
    }

    // Without a tag, or with another tool's tag, there is no answer.
    let untagged = dir.path().join("plain/target/debug/deps");
    std::fs::create_dir_all(&untagged).unwrap();
    assert_eq!(cargo_workspace_of(&untagged), None);
    let other = dir.path().join("other/cache");
    std::fs::create_dir_all(&other).unwrap();
    std::fs::write(
        other.join("CACHEDIR.TAG"),
        "Signature: 8a477f597d28d172789f06886806bc55\n",
    )
    .unwrap();
    assert_eq!(cargo_workspace_of(&other.join("x")), None);
}

#[test]
fn build_script_units_share_the_workspace_event_root() {
    let dir = tempfile::TempDir::new().unwrap();
    let workspace = dir.path().join("app");
    let target = tagged_target(&workspace);
    let expected = std::fs::canonicalize(&workspace)
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let out = |dir: &str| target.join(dir).to_string_lossy().into_owned();

    // A crate, a build script, and a probe the build script runs: one
    // build, one root (#1081).
    for out_dir in [
        out("debug/deps"),
        out("debug/build/anyhow-1234"),
        out("debug/build/anyhow-1234/out"),
    ] {
        let args = rustc_args(&[
            "rustc",
            "src/lib.rs",
            "--crate-name",
            "x",
            "--out-dir",
            &out_dir,
        ]);
        assert_eq!(rustc_event_root(&args), expected, "{out_dir}");
    }
    let output = target.join("debug/build/anyhow-1234/out/probe.rlib");
    let args = rustc_args(&["rustc", "probe.rs", "-o", output.to_str().unwrap()]);
    assert_eq!(rustc_event_root(&args), expected);

    // A build-script run joins it through its OUT_DIR; outside a tagged
    // target it keeps its package directory.
    assert_eq!(
        build_script_event_root(
            &target.join("debug/build/anyhow-1234/out"),
            &dir.path().join("pkg")
        ),
        expected
    );
    let untagged = dir.path().join("pkg");
    std::fs::create_dir_all(&untagged).unwrap();
    assert_eq!(
        build_script_event_root(&untagged.join("out"), &untagged),
        std::fs::canonicalize(&untagged).unwrap().to_string_lossy()
    );

    // cc and nvcc under a build script find it through OUT_DIR.
    let out_dir = Some(std::ffi::OsString::from(out("debug/build/anyhow-1234/out")));
    let parsed = parse_cc(&["gcc", "-c", "foo.c", "-o", "foo.o"]);
    assert_eq!(cc_event_root_in(&parsed, out_dir.clone()), expected);
    assert_eq!(nvcc_event_root_in(out_dir), expected);
    assert_eq!(out_dir_workspace(Some(std::ffi::OsString::new())), None);
    assert_eq!(out_dir_workspace(None), None);
}

#[test]
fn refresh_session_marker_extends_own_session_but_never_clobbers_newer() {
    let dir = tempfile::TempDir::new().unwrap();
    let config = test_config(dir.path().to_path_buf());
    let root = "/ws";
    let marker = session_marker_path(&config, root);
    std::fs::create_dir_all(marker.parent().unwrap()).unwrap();

    // Refreshing our own (stale) session bumps the timestamp.
    std::fs::write(&marker, "v1 1000 mine").unwrap();
    refresh_session_marker(&config, root, "mine");
    let (ts, id) = parse_session_marker(&std::fs::read_to_string(&marker).unwrap()).unwrap();
    assert_eq!(id, "mine");
    assert!(ts > 1000, "timestamp must be refreshed");

    // A newer session re-minted the marker: our refresh must not
    // resurrect the old id over it.
    std::fs::write(&marker, format!("v1 {} newer", now_epoch_secs())).unwrap();
    refresh_session_marker(&config, root, "mine");
    let (_, id) = parse_session_marker(&std::fs::read_to_string(&marker).unwrap()).unwrap();
    assert_eq!(id, "newer");
}

#[test]
fn mint_session_id_is_opaque_and_distinct() {
    // A tight loop is the point: it drives the interval between calls below
    // the clock's resolution, which is exactly the case where the old
    // nanos-only digest repeated itself.
    let ids: Vec<String> = (0..256).map(|_| mint_session_id("/repo")).collect();

    assert!(ids.iter().all(|id| id.len() == 16));
    let unique: std::collections::HashSet<&String> = ids.iter().collect();
    assert_eq!(
        unique.len(),
        ids.len(),
        "the seq counter makes ids distinct even when the clock does not move"
    );
}

/// With no remote configured there is no hint to send, so prefetch
/// detection touches nothing; sessions come from event logging instead.
#[test]
fn maybe_trigger_prefetch_returns_immediately_without_remote() {
    let dir = tempfile::tempdir().unwrap();
    let cache_dir = dir.path().join("cache");
    let config = test_config(cache_dir.clone());
    let args = rustc_args(&["rustc", "src/lib.rs", "--crate-name", "foo"]);

    maybe_trigger_prefetch(&config, &args);

    assert!(!cache_dir.join(".build-session").exists());
    assert!(!config.runtime_dir.join(".build-sessions").exists());
}

/// Incremental cleanup only removes a real directory when the config flag
/// is enabled; absent paths and disabled cleanup are silent no-ops.
#[test]
fn clean_incremental_dir_respects_config_and_existing_directory() {
    let dir = tempfile::tempdir().unwrap();
    let incremental = dir.path().join("incremental");
    std::fs::create_dir_all(&incremental).unwrap();
    std::fs::write(incremental.join("state.bin"), b"state").unwrap();
    let mut config = test_config(dir.path().join("cache"));
    let mut args = rustc_args(&["rustc", "src/lib.rs", "--crate-name", "foo"]);
    args.incremental = Some(incremental.clone());

    config.clean_incremental = false;
    clean_incremental_dir(&config, &args);
    assert!(incremental.exists());

    config.clean_incremental = true;
    clean_incremental_dir(&config, &args);
    assert!(!incremental.exists());

    clean_incremental_dir(&config, &args);
}

#[test]
fn event_root_string_none_is_empty() {
    assert_eq!(event_root_string(None), "");
}

#[test]
fn event_root_string_absolute_path_is_canonicalized() {
    // An existing absolute path canonicalizes to its real path.
    let dir = tempfile::tempdir().unwrap();
    let real = std::fs::canonicalize(dir.path()).unwrap();
    let got = event_root_string(Some(dir.path().to_path_buf()));
    assert_eq!(got, real.to_string_lossy());
}

#[test]
fn event_root_string_relative_path_is_joined_to_cwd_and_absolute() {
    // A relative root is resolved against the current dir, yielding an
    // absolute path (canonicalize falls back to the joined path when the
    // target doesn't exist). Covers the relative-branch join.
    let got = event_root_string(Some(PathBuf::from("kache-nonexistent-rel-xyz")));
    assert!(
        Path::new(&got).is_absolute(),
        "relative root must resolve to an absolute path: {got}"
    );
    assert!(
        got.ends_with("kache-nonexistent-rel-xyz"),
        "resolved path should retain the relative segment: {got}"
    );
}

#[test]
fn event_root_override_reads_kache_event_root_env() {
    // KACHE_EVENT_ROOT, when set and non-empty, overrides the event root.
    let _lock = crate::test_support::process_state_test_lock();
    let _guard = TestEnvGuard::set("KACHE_EVENT_ROOT", "/some/forest/root");
    assert_eq!(
        event_root_override(),
        Some(PathBuf::from("/some/forest/root"))
    );
    // Empty value is treated as unset.
    unsafe {
        std::env::set_var("KACHE_EVENT_ROOT", "");
    }
    assert_eq!(event_root_override(), None);
}

#[test]
fn cache_entry_has_files_rejects_empty_entries() {
    assert!(!cache_entry_has_files(&entry_meta_with_files(&[])));
    assert!(cache_entry_has_files(&entry_meta_with_files(&[
        "libfoo.rlib"
    ])));
}

#[test]
fn cc_scheduled_hit_ok_rejects_empty_files_and_incomplete_entries() {
    let with_depinfo = CcCompiler::new()
        .parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o", "-MMD"]))
        .unwrap();
    let object_only = CcCompiler::new()
        .parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o"]))
        .unwrap();

    assert!(
        !cc_scheduled_hit_ok(&object_only, &entry_meta_with_files(&[])),
        "an empty file list must not restore"
    );
    assert!(
        !cc_scheduled_hit_ok(&with_depinfo, &entry_meta_with_files(&["foo.o"])),
        "a rejection reason must not restore"
    );
    assert!(cc_scheduled_hit_ok(
        &object_only,
        &entry_meta_with_files(&["foo.o"])
    ));
    assert!(cc_scheduled_hit_ok(
        &with_depinfo,
        &entry_meta_with_files(&["foo.o", "foo.d"])
    ));
}

fn seed_store_entry(dir: &std::path::Path, key: &str) -> (Config, Store, EntryMeta) {
    let config = test_config(dir.to_path_buf());
    let store = Store::open(&config).unwrap();
    let artifact = dir.join("seed.rlib");
    std::fs::write(&artifact, b"cached-bytes").unwrap();
    store
        .put(
            key,
            "seed",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(artifact, "libseed.rlib".to_string())],
            "stdout-diag",
            "stderr-diag",
        )
        .unwrap();
    let meta = store.get(key).unwrap().expect("seeded entry");
    (config, store, meta)
}

#[test]
fn take_recheck_hit_returns_only_entries_the_predicate_accepts() {
    let dir = tempfile::tempdir().unwrap();
    let key = "recheck-key";
    let (_config, store, meta) = seed_store_entry(dir.path(), key);

    let hit = take_recheck_hit(&store, key, &|_| true).expect("accepted meta");
    assert_eq!(hit.cache_key, meta.cache_key);
    assert!(
        take_recheck_hit(&store, key, &|_| false).is_none(),
        "a rejecting predicate must not return the stored meta"
    );
    assert!(take_recheck_hit(&store, "missing", &|_| true).is_none());
}

/// With deferred durability the entry is left for the daemon when one is
/// listening, and flushed here when none is: either way nothing outlives
/// the build, and a store never sees a daemon does not accumulate
/// unflushed entries. With the feature off, a put is already durable.
#[test]
fn a_pending_entry_waits_for_the_daemon_or_is_flushed_here() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().join("cache"));
    config.deferred_durability = true;
    config.socket_path_override = Some(dir.path().join("absent.sock"));
    let store = Store::open(&config).unwrap();
    let output = dir.path().join("out.rlib");
    let put = |key: &str, bytes: &[u8]| {
        let output = dir.path().join(format!("{key}.rlib"));
        std::fs::write(&output, bytes).unwrap();
        store
            .put(
                key,
                "pending_crate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(output, "libout.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
    };
    let _ = &output;

    put("no_daemon", b"artifact-one");
    assert_eq!(store.pending_durability().unwrap(), 1);
    flush_or_hand_off_durability(&config, &store, "no_daemon");
    assert_eq!(
        store.pending_durability().unwrap(),
        0,
        "without a daemon the entry is flushed here"
    );

    // A listening socket: the daemon owns the flush, so this leaves the
    // entry pending and starts nothing. Bound through the same transport
    // the wrapper probes, which on Windows is a named pipe.
    let socket = dir.path().join("live.sock");
    let listener = crate::transport::ListenerOptions::new()
        .name(crate::transport::socket_name(&socket).expect("socket name"))
        .create_sync()
        .expect("bind listener");
    config.socket_path_override = Some(socket);
    put("with_daemon", b"artifact-two");
    assert_eq!(store.pending_durability().unwrap(), 1);
    flush_or_hand_off_durability(&config, &store, "with_daemon");
    assert_eq!(
        store.pending_durability().unwrap(),
        1,
        "a reachable daemon is left to flush it"
    );
    drop(listener);

    config.deferred_durability = false;
    flush_or_hand_off_durability(&config, &store, "with_daemon");
    assert_eq!(
        store.pending_durability().unwrap(),
        1,
        "the switch being off says nothing about an entry already pending"
    );
}

#[test]
fn a_deferred_cc_compile_is_stored_unless_a_peer_beat_it_or_an_input_moved() {
    assert!(
        !cc_peer_committed_precompile(false, true),
        "an ordinary miss restores instead"
    );
    assert!(!cc_peer_committed_precompile(true, false));
    assert!(cc_peer_committed_precompile(true, true));
    assert!(cc_store_candidate(true, false, false));
    assert!(
        !cc_store_candidate(false, false, false),
        "a failed or output-less compile"
    );
    assert!(
        !cc_store_candidate(true, true, false),
        "an input written during the build"
    );
    assert!(
        !cc_store_candidate(true, false, true),
        "the peer's entry stands"
    );
    assert!(cc_restore_committed(false, true));
    assert!(
        !cc_restore_committed(false, false),
        "an entry that does not fit is not restored"
    );
    assert!(
        !cc_restore_committed(true, true),
        "never over a compile's own outputs"
    );
    assert!(!cc_restore_committed(true, false));
}

#[test]
fn admit_scheduler_miss_off_returns_empty_without_leases() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().to_path_buf());
    config.scheduler = false;
    let store = Store::open(&config).unwrap();
    let (guard, hit) = admit_scheduler_miss(
        &config,
        &store,
        "key",
        FlightIdentity::cc("a.c"),
        "a.c",
        false,
        |_| true,
    );
    assert!(guard.is_empty());
    assert!(hit.is_none());
    assert!(
        !dir.path().join("scheduler").exists(),
        "off switch must not create lease files"
    );
}

#[test]
fn admit_scheduler_miss_on_owns_the_flight() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path().to_path_buf());
    let store = Store::open(&config).unwrap();
    let (guard, hit) = admit_scheduler_miss(
        &config,
        &store,
        "key",
        FlightIdentity::rustc("owned", &["lib".into()], false),
        "owned",
        false,
        |_| true,
    );
    assert!(hit.is_none());
    assert!(
        !guard.is_empty(),
        "an enabled miss must take a flight and permit"
    );
}

fn spawn_flight_holder(dir: &std::path::Path, crate_name: &str, key: &str) -> std::process::Child {
    std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "wrapper::tests::hold_scheduler_flight_fixture",
            "--ignored",
            "--nocapture",
        ])
        .env("KACHE_TEST_SCHEDULER_ROOT", dir)
        .env("KACHE_TEST_FLIGHT_CRATE", crate_name)
        .env("KACHE_TEST_FLIGHT_KEY", key)
        .spawn()
        .unwrap()
}

fn wait_flight_ready(dir: &std::path::Path, child: &mut std::process::Child) {
    let ready = dir.join("lock-ready");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !ready.exists() && std::time::Instant::now() < deadline {
        assert!(
            child.try_wait().unwrap().is_none(),
            "scheduler fixture exited before becoming ready"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(ready.exists(), "scheduler fixture did not become ready");
}

#[test]
fn admit_scheduler_miss_recheck_returns_accepted_meta() {
    let dir = tempfile::tempdir().unwrap();
    let crate_name = "recheck_accept";
    let key = "recheck-accept-key";
    let (_config, store, seeded) = seed_store_entry(dir.path(), key);
    let expected_key = seeded.cache_key.clone();
    drop(store);
    let mut child = spawn_flight_holder(dir.path(), crate_name, key);
    wait_flight_ready(dir.path(), &mut child);

    let cache = dir.path().to_path_buf();
    let waiter = std::thread::spawn(move || {
        let config = test_config(cache);
        let store = Store::open(&config).unwrap();
        admit_scheduler_miss(
            &config,
            &store,
            key,
            FlightIdentity::rustc(crate_name, &["lib".into()], false),
            crate_name,
            false,
            |_| true,
        )
    });
    std::thread::sleep(std::time::Duration::from_millis(200));
    std::fs::write(dir.path().join("go"), b"go").unwrap();
    let (guard, hit) = waiter.join().unwrap();
    assert!(
        guard.is_empty(),
        "a Recheck hit must not keep the flight or permit"
    );
    let hit = hit.expect("accepted Recheck meta");
    assert_eq!(hit.cache_key, expected_key);
    let _ = child.wait();
}

#[test]
fn admit_scheduler_miss_recheck_skips_rejected_meta() {
    let dir = tempfile::tempdir().unwrap();
    let crate_name = "recheck_reject";
    let key = "recheck-reject-key";
    let (_config, store, _seeded) = seed_store_entry(dir.path(), key);
    drop(store);
    let mut child = spawn_flight_holder(dir.path(), crate_name, key);
    wait_flight_ready(dir.path(), &mut child);

    let cache = dir.path().to_path_buf();
    let waiter = std::thread::spawn(move || {
        let config = test_config(cache);
        let store = Store::open(&config).unwrap();
        admit_scheduler_miss(
            &config,
            &store,
            key,
            FlightIdentity::rustc(crate_name, &["lib".into()], false),
            crate_name,
            false,
            |_| false,
        )
    });
    std::thread::sleep(std::time::Duration::from_millis(200));
    std::fs::write(dir.path().join("go"), b"go").unwrap();
    let (guard, hit) = waiter.join().unwrap();
    assert!(
        hit.is_none(),
        "a rejecting predicate must not restore the stored meta"
    );
    assert!(
        !guard.is_empty(),
        "rejected Recheck must loop and compile as the next owner"
    );
    let _ = child.wait();
}

#[test]
#[ignore = "subprocess fixture for admit_scheduler_miss recheck tests"]
fn hold_scheduler_flight_fixture() {
    let root = PathBuf::from(std::env::var_os("KACHE_TEST_SCHEDULER_ROOT").expect("fixture root"));
    let crate_name = std::env::var("KACHE_TEST_FLIGHT_CRATE").expect("fixture crate name");
    let key = std::env::var("KACHE_TEST_FLIGHT_KEY").expect("fixture cache key");
    let identity = FlightIdentity::rustc(&crate_name, &["lib".into()], false).with_key(&key);
    let guard = match scheduler::begin_miss(&root, true, &identity, &crate_name, false, None) {
        scheduler::BeginMiss::Compile(guard) => guard,
        scheduler::BeginMiss::Recheck => panic!("fixture must own the flight"),
    };
    std::fs::write(root.join("lock-ready"), b"ready").unwrap();
    let go = root.join("go");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !go.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    drop(guard);
}

/// A key derived from the emitted dep-info arms the too-new guard even
/// with the modified-input guard off: the compile already ran, so an input
/// written since it started may not match what rustc read.
#[test]
fn a_key_from_emitted_dep_info_always_arms_the_too_new_guard() {
    if std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .is_err()
    {
        eprintln!("skipped: no rustc");
        return;
    }
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().join("cache"));
    config.modified_input_guard = false;
    config.input_predictions = true;
    let src = dir.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    let lib = src.join("lib.rs");
    let out = dir.path().join("debug").join("deps");
    std::fs::create_dir_all(&out).unwrap();
    let args = RustcCompiler::new()
        .parse(&s(&[
            "rustc",
            "--crate-name",
            "kt",
            lib.to_str().unwrap(),
            "--emit=dep-info,metadata",
            "--out-dir",
            out.to_str().unwrap(),
        ]))
        .unwrap();
    let invocation_start_ns = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    )
    .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));
    std::fs::write(&lib, "pub fn v() {}\n").unwrap();
    let closure = crate::cache_key::DepInfo {
        source_files: vec![lib.clone()],
        env_deps: Vec::new(),
    };
    let compiler = RustcCompiler::new();
    let keyed = compute_rustc_cache_key(
        &config,
        &compiler,
        &args,
        None,
        invocation_start_ns,
        None,
        None,
        FileHashStats::default(),
        false,
        0,
        Vec::new(),
        KeyDiscovery::Emitted(closure),
    )
    .unwrap();
    assert!(!keyed.cache_key.is_empty());
    assert!(!keyed.deferred);
    assert!(
        keyed.key_too_new,
        "a source written after the invocation started is too new"
    );
}

#[test]
fn a_rederivation_never_waits_on_a_discovery_flight() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().join("cache"));
    config.scheduler = true;
    let flights = |discovery: KeyDiscovery| discovery_flight_dir(&config, &discovery);
    assert_eq!(
        flights(KeyDiscovery::Immediate),
        Some(config.cache_dir.clone())
    );
    assert_eq!(
        flights(KeyDiscovery::Deferrable),
        Some(config.cache_dir.clone())
    );
    assert_eq!(
        flights(KeyDiscovery::Rederived),
        None,
        "the caller may already hold the flight"
    );
    config.scheduler = false;
    assert_eq!(
        discovery_flight_dir(&config, &KeyDiscovery::Immediate),
        None
    );
}

/// A predicted key that missed is re-derived while the invocation may
/// still hold the unit's discovery flight. A flight lock is not
/// re-entrant, so joining it again would wait on itself for the whole
/// flight timeout. The re-derivation must not join one at all.
#[test]
fn a_rederived_key_holds_no_discovery_flight() {
    if std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .is_err()
    {
        eprintln!("skipped: no rustc");
        return;
    }
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path().join("cache"));
    config.scheduler = true;
    config.input_predictions = true;
    let store = Store::open(&config).unwrap();
    let lib = dir.path().join("src/lib.rs");
    std::fs::create_dir_all(lib.parent().unwrap()).unwrap();
    std::fs::write(&lib, "pub fn v() {}\n").unwrap();
    let out = dir.path().join("target/debug/deps");
    std::fs::create_dir_all(&out).unwrap();
    let args = RustcCompiler::new()
        .parse(&s(&[
            "rustc",
            "--crate-name",
            "kt",
            lib.to_str().unwrap(),
            "--crate-type",
            "lib",
            "--emit=dep-info,metadata",
            "--out-dir",
            out.to_str().unwrap(),
        ]))
        .unwrap();
    let keyed = recompute_key_without_prediction(
        &config,
        &RustcCompiler::new(),
        &args,
        None,
        0,
        Some(&store),
        None,
    )
    .unwrap();
    assert!(!keyed.cache_key.is_empty());
    assert!(
        keyed.discovery_flight.is_none(),
        "a re-derivation joined a discovery flight"
    );
}
