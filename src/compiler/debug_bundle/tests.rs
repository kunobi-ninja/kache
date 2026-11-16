use super::*;
use crate::store::{CachedFile, Store};

const KEY: &str = "1905190519051905190519051905190519051905190519051905190519051905";
const CAP: u64 = crate::config::DEFAULT_RESTORE_MAX_SIZE;

fn open_store(dir: &Path) -> Store {
    Store::open(&crate::test_support::test_config(dir.join("cache"))).unwrap()
}

/// A dsymutil-shaped bundle with a DWARF file large enough that copying it
/// would be visible, a relocation map, and an empty file.
fn write_bundle(bundle: &Path) -> Vec<(&'static str, Vec<u8>)> {
    let files = vec![
        ("Contents/Info.plist", b"plist".to_vec()),
        (
            "Contents/Resources/DWARF/foo",
            (0..256 * 1024).map(|i| (i % 251) as u8).collect(),
        ),
        (
            "Contents/Resources/Relocations/aarch64/foo.yml",
            b"yml".to_vec(),
        ),
        ("Contents/Resources/empty", Vec::new()),
    ];
    for (path, content) in &files {
        let path = bundle.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }
    files
}

/// Stage `bundle` and store it as one entry, as the rustc store path does.
fn store_bundle(store: &Store, bundle: &Path, staging: &Path) -> Vec<CachedFile> {
    let artifacts = stage(bundle, "foo", staging).unwrap().into_artifacts();
    let files: Vec<_> = artifacts
        .iter()
        .map(|artifact| (artifact.path.clone(), artifact.store_name.clone()))
        .collect();
    store
        .put_with_compile_time(KEY, "foo", &["bin".into()], &[], "", "", &files, "", "", 0)
        .unwrap();
    store.get(KEY).unwrap().unwrap().files
}

/// Store a hand-written manifest and return its artifact.
fn put_manifest(store: &Store, json: &str) -> CachedFile {
    let hash = blake3::hash(json.as_bytes()).to_hex().to_string();
    let blob = store.blob_path(&hash);
    std::fs::create_dir_all(blob.parent().unwrap()).unwrap();
    std::fs::write(blob, json).unwrap();
    CachedFile {
        name: "foo.dsym.json".into(),
        size: json.len() as u64,
        hash,
        executable: false,
    }
}

fn put_file(store: &Store, name: &str, content: &[u8]) -> CachedFile {
    let hash = blake3::hash(content).to_hex().to_string();
    let blob = store.blob_path(&hash);
    std::fs::create_dir_all(blob.parent().unwrap()).unwrap();
    std::fs::write(blob, content).unwrap();
    CachedFile {
        name: name.into(),
        size: content.len() as u64,
        hash,
        executable: false,
    }
}

fn leftovers(dir: &Path) -> Vec<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with(".kache-dsym-"))
        .collect()
}

fn clone_by_copy(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::copy(src, dst).map(drop)
}

fn no_clone(_: &Path, _: &Path) -> std::io::Result<()> {
    Err(std::io::ErrorKind::Unsupported.into())
}

#[test]
fn names_classify_by_layout() {
    use super::super::classify_by_filename;
    for (name, kind) in [
        ("foo-abc.dsym.tar", Some(ArtifactKind::DebugBundle)),
        ("foo-abc.dsym.json", Some(ArtifactKind::DebugBundleManifest)),
        ("foo-abc.dsym.0", Some(ArtifactKind::DebugBundleFile)),
        ("foo-abc.dsym.12", Some(ArtifactKind::DebugBundleFile)),
        ("foo-abc.dsym.", None),
        ("foo-abc.dsym.1a", None),
        ("foo-abc.dsym.json.bak", None),
        ("foo-abc.json", None),
        ("foo-abc.tar", None),
        ("foo-abc", None),
    ] {
        assert_eq!(classify(name), kind, "{name}");
        if let Some(kind) = kind {
            assert_eq!(classify_by_filename(name), kind, "{name}");
        }
    }
}

#[test]
fn every_bundle_kind_and_no_other_is_a_debug_bundle() {
    for kind in [
        ArtifactKind::DebugBundle,
        ArtifactKind::DebugBundleManifest,
        ArtifactKind::DebugBundleFile,
    ] {
        assert!(kind.is_debug_bundle(), "{kind:?}");
        assert_eq!(kind.link_strategy(), crate::link::LinkStrategy::Hardlink);
    }
    for kind in [
        ArtifactKind::DebugSidecar,
        ArtifactKind::Executable,
        ArtifactKind::Other("unknown-ext"),
    ] {
        assert!(!kind.is_debug_bundle(), "{kind:?}");
    }
}

/// The store holds one artifact per non-empty file plus a manifest, and a
/// restore rebuilds the bundle from clones of those blobs without writing
/// the manifest beside it.
#[test]
fn a_staged_bundle_restores_as_clones_of_its_store_blobs() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(dir.path());
    let source = dir.path().join("build/foo.dSYM");
    let expected = write_bundle(&source);
    let staging = tempfile::tempdir_in(dir.path()).unwrap();
    let files = store_bundle(&store, &source, staging.path());
    let names: Vec<_> = files.iter().map(|file| file.name.as_str()).collect();
    assert_eq!(
        names,
        ["foo.dsym.json", "foo.dsym.0", "foo.dsym.1", "foo.dsym.2"]
    );
    assert!(files.iter().all(|file| !file.executable));

    let deps = dir.path().join("target/debug/deps");
    std::fs::create_dir_all(&deps).unwrap();
    let restored = restore(&store, &files, &files[0], &deps.join("foo.dsym.json"), CAP).unwrap();

    let bundle = deps.join("foo.dSYM");
    for (path, content) in &expected {
        assert_eq!(
            &std::fs::read(bundle.join(path)).unwrap(),
            content,
            "{path}"
        );
    }
    let total: u64 = expected
        .iter()
        .map(|(_, content)| content.len() as u64)
        .sum();
    assert_eq!(restored.cloned + restored.copied, total);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let dwarf = std::fs::metadata(bundle.join("Contents/Resources/DWARF/foo")).unwrap();
        assert_eq!(dwarf.permissions().mode() & 0o777, 0o644);
    }
    if cfg!(target_os = "macos") {
        // APFS: every byte came from a clone, so the DWARF file shares all of
        // its blocks with the store blob.
        assert_eq!(
            restored,
            RestoredBundle {
                cloned: total,
                copied: 0
            }
        );
        let sizing = kache_fs::measure_file(&bundle.join("Contents/Resources/DWARF/foo")).unwrap();
        assert_eq!(sizing.sharing, kache_fs::Sharing::Full, "{sizing:?}");
    }
    assert!(!deps.join("foo.dsym.json").exists());
    assert!(leftovers(&deps).is_empty());
}

/// Both branches of the clone call count their bytes, on any filesystem.
#[test]
fn a_restore_counts_cloned_and_copied_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(dir.path());
    let source = dir.path().join("build/foo.dSYM");
    write_bundle(&source);
    let staging = tempfile::tempdir_in(dir.path()).unwrap();
    let files = store_bundle(&store, &source, staging.path());
    let total = 5 + 256 * 1024 + 3;
    let deps = dir.path().join("deps");
    std::fs::create_dir_all(&deps).unwrap();
    let target = deps.join("foo.dsym.json");

    let cloned = restore_via(&store, &files, &files[0], &target, CAP, clone_by_copy).unwrap();
    assert_eq!(
        cloned,
        RestoredBundle {
            cloned: total,
            copied: 0
        }
    );
    let copied = restore_via(&store, &files, &files[0], &target, CAP, no_clone).unwrap();
    assert_eq!(
        copied,
        RestoredBundle {
            cloned: 0,
            copied: total
        }
    );
    assert_eq!(
        std::fs::read(deps.join("foo.dSYM/Contents/Resources/DWARF/foo")).unwrap(),
        std::fs::read(source.join("Contents/Resources/DWARF/foo")).unwrap()
    );
}

/// A stale bundle, or a stray file at the bundle path, is replaced whole.
#[test]
fn a_restore_replaces_what_was_at_the_bundle_path() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(dir.path());
    let source = dir.path().join("build/foo.dSYM");
    write_bundle(&source);
    let staging = tempfile::tempdir_in(dir.path()).unwrap();
    let files = store_bundle(&store, &source, staging.path());
    let deps = dir.path().join("deps");
    let bundle = deps.join("foo.dSYM");

    std::fs::create_dir_all(bundle.join("Contents/Resources/DWARF")).unwrap();
    std::fs::write(bundle.join("Contents/Resources/DWARF/stale"), b"old").unwrap();
    restore(&store, &files, &files[0], &deps.join("foo.dsym.json"), CAP).unwrap();
    assert!(!bundle.join("Contents/Resources/DWARF/stale").exists());
    assert!(bundle.join("Contents/Resources/DWARF/foo").is_file());

    std::fs::remove_dir_all(&bundle).unwrap();
    std::fs::write(&bundle, b"not a bundle").unwrap();
    restore(&store, &files, &files[0], &deps.join("foo.dsym.json"), CAP).unwrap();
    assert!(bundle.join("Contents/Info.plist").is_file());
}

/// Every rejected restore leaves the previous bundle as it was and no
/// private assembly directory behind.
#[test]
fn a_failed_restore_leaves_no_partial_bundle() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(dir.path());
    let plist = put_file(&store, "foo.dsym.0", b"plist");
    let dwarf = put_file(&store, "foo.dsym.1", b"dwarf");
    let deps = dir.path().join("deps");
    let bundle = deps.join("foo.dSYM");
    std::fs::create_dir_all(&bundle).unwrap();
    std::fs::write(bundle.join("previous"), b"kept").unwrap();
    let manifest = |files: &str, empty: &str| {
        format!(r#"{{"version":1,"files":[{files}],"empty_files":[{empty}]}}"#)
    };
    let both = r#"{"path":"Contents/Info.plist","name":"foo.dsym.0"},
                  {"path":"Contents/Resources/DWARF/foo","name":"foo.dsym.1"}"#;

    let cases = [
        (manifest(both, ""), Some(dwarf.hash.as_str()), "copying"),
        (
            manifest(r#"{"path":"Contents/x","name":"foo.dsym.9"}"#, ""),
            None,
            "names a file the entry lacks: foo.dsym.9",
        ),
        (
            manifest(r#"{"path":"../escape","name":"foo.dsym.0"}"#, ""),
            None,
            "unsafe path: ../escape",
        ),
        (
            manifest(r#"{"path":"/etc/x","name":"foo.dsym.0"}"#, ""),
            None,
            "unsafe path: /etc/x",
        ),
        (manifest(both, r#""""#), None, "unsafe path: "),
        (manifest(both, r#""./x""#), None, "unsafe path: ./x"),
        (manifest("", ""), None, "lists no files"),
        (manifest("", r#""Contents/empty""#), None, "lists no files"),
        (
            manifest(both, r#""contents/INFO.plist""#),
            None,
            "lists contents/INFO.plist twice",
        ),
        (
            manifest(
                r#"{"path":"Contents/Info.plist","name":"foo.dsym.0"},
                   {"path":"contents/info.PLIST","name":"foo.dsym.1"}"#,
                "",
            ),
            None,
            "lists contents/info.PLIST twice",
        ),
        (
            manifest(both, r#""Contents/Info.plist""#),
            None,
            "lists Contents/Info.plist twice",
        ),
        (
            r#"{"version":2,"files":[],"empty_files":[]}"#.to_string(),
            None,
            "unsupported debug bundle manifest version 2",
        ),
        (
            "not json".to_string(),
            None,
            "parsing debug bundle manifest",
        ),
    ];
    for (json, evicted, expected) in cases {
        let manifest = put_manifest(&store, &json);
        // The first case loses the DWARF blob after the plist is cloned.
        let blob = evicted.map(|hash| store.blob_path(hash));
        if let Some(blob) = &blob {
            std::fs::rename(blob, blob.with_extension("gone")).unwrap();
        }
        let files = [manifest.clone(), plist.clone(), dwarf.clone()];
        let error =
            restore(&store, &files, &manifest, &deps.join("foo.dsym.json"), CAP).unwrap_err();
        if let Some(blob) = &blob {
            std::fs::rename(blob.with_extension("gone"), blob).unwrap();
        }
        let message = format!("{error:#}");
        assert!(message.contains(expected), "{json}: {message}");
        assert_eq!(
            std::fs::read(bundle.join("previous")).unwrap(),
            b"kept",
            "{json}"
        );
        assert!(!bundle.join("Contents").exists(), "{json}");
        assert!(leftovers(&deps).is_empty(), "{json}");
    }
    // The evicted-blob case failed after the manifest checks, on the copy.
    let manifest = put_manifest(&store, &manifest(both, ""));
    let files = [manifest.clone(), plist, dwarf];
    restore(&store, &files, &manifest, &deps.join("foo.dsym.json"), CAP).unwrap();
    assert!(!bundle.join("previous").exists());
}

/// The cap covers the bundle's total size and allows exactly the limit.
#[test]
fn the_restore_cap_counts_every_file_and_is_inclusive() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_store(dir.path());
    let files = [
        put_manifest(
            &store,
            r#"{"version":1,"files":[{"path":"a","name":"foo.dsym.0"},{"path":"b","name":"foo.dsym.1"}],"empty_files":[]}"#,
        ),
        put_file(&store, "foo.dsym.0", &[1; 60]),
        put_file(&store, "foo.dsym.1", &[2; 40]),
    ];
    let deps = dir.path().join("deps");
    std::fs::create_dir_all(&deps).unwrap();
    let target = deps.join("foo.dsym.json");

    let error = restore(&store, &files, &files[0], &target, 99).unwrap_err();
    assert_eq!(
        error.to_string(),
        "debug bundle exceeds the 99-byte extraction cap"
    );
    assert!(!deps.join("foo.dSYM").exists());
    restore(&store, &files, &files[0], &target, 100).unwrap();
    assert_eq!(std::fs::read(deps.join("foo.dSYM/b")).unwrap(), [2; 40]);
}

#[test]
fn only_a_manifest_name_locates_a_bundle() {
    assert_eq!(
        bundle_dir_for(Path::new("/t/deps/foo-1.dsym.json"), MANIFEST_SUFFIX).unwrap(),
        Path::new("/t/deps/foo-1.dSYM")
    );
    let error = bundle_dir_for(Path::new("/t/deps/foo-1.dsym.tar"), MANIFEST_SUFFIX)
        .unwrap_err()
        .to_string();
    assert!(error.contains("`.dsym.json`"), "{error}");
}

#[test]
fn staging_snapshots_the_bundle_it_was_given() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("foo.dSYM");
    write_bundle(&source);
    let staging = tempfile::tempdir_in(dir.path()).unwrap();
    let artifacts = stage(&source, "foo", staging.path())
        .unwrap()
        .into_artifacts();
    // Later rewrites of the live bundle do not reach the staged snapshot.
    std::fs::write(source.join("Contents/Info.plist"), b"changed").unwrap();

    let kinds: Vec<_> = artifacts.iter().map(|artifact| artifact.kind).collect();
    assert_eq!(
        kinds,
        [
            ArtifactKind::DebugBundleManifest,
            ArtifactKind::DebugBundleFile,
            ArtifactKind::DebugBundleFile,
            ArtifactKind::DebugBundleFile,
        ]
    );
    assert!(artifacts.iter().all(|artifact| !artifact.required));
    assert!(
        artifacts
            .iter()
            .all(|artifact| artifact.path.starts_with(staging.path()))
    );
    assert_eq!(std::fs::read(&artifacts[1].path).unwrap(), b"plist");
    let manifest: Manifest =
        serde_json::from_slice(&std::fs::read(&artifacts[0].path).unwrap()).unwrap();
    let paths: Vec<_> = manifest
        .files
        .iter()
        .map(|file| (file.path.as_str(), file.name.as_str()))
        .collect();
    assert_eq!(
        paths,
        [
            ("Contents/Info.plist", "foo.dsym.0"),
            ("Contents/Resources/DWARF/foo", "foo.dsym.1"),
            (
                "Contents/Resources/Relocations/aarch64/foo.yml",
                "foo.dsym.2"
            ),
        ]
    );
    assert_eq!(manifest.empty_files, ["Contents/Resources/empty"]);
    assert_eq!(manifest.version, MANIFEST_VERSION);
}

/// A bundle with no file content is not stored: its manifest could never
/// restore.
#[test]
fn a_bundle_without_content_is_not_staged() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("foo.dSYM");
    std::fs::create_dir_all(source.join("Contents")).unwrap();
    std::fs::write(source.join("Contents/empty"), b"").unwrap();
    let staging = tempfile::tempdir_in(dir.path()).unwrap();
    let error = stage(&source, "foo", staging.path()).unwrap_err();
    assert!(
        error.to_string().contains("no files with content"),
        "{error}"
    );
}

fn complete_bundle(root: &Path, marker: &[u8]) {
    std::fs::create_dir_all(root.join("Contents/Resources/DWARF")).unwrap();
    std::fs::write(root.join("Contents/Info.plist"), marker).unwrap();
    std::fs::write(root.join("Contents/Resources/DWARF/foo"), marker).unwrap();
}

/// What a reader sees at `bundle`: nothing, or which complete bundle.
fn observe(bundle: &Path) -> &'static str {
    if bundle.symlink_metadata().is_err() {
        return "absent";
    }
    let read = |path: &str| std::fs::read(bundle.join(path)).ok();
    match (
        read("Contents/Info.plist"),
        read("Contents/Resources/DWARF/foo"),
    ) {
        (Some(a), Some(b)) if a == b && a == b"old" => "old",
        (Some(a), Some(b)) if a == b && a == b"new" => "new",
        _ => "partial",
    }
}

/// The bundle path holds the old bundle, then nothing, then the new one:
/// the old bundle is moved aside whole before the new one is renamed in,
/// and deleted only afterwards.
#[test]
fn publishing_never_exposes_a_partial_bundle() {
    let dir = tempfile::tempdir().unwrap();
    let bundle = dir.path().join("foo.dSYM");
    complete_bundle(&bundle, b"old");
    let staging = new_bundle_staging(&bundle).unwrap();
    complete_bundle(staging.path(), b"new");

    let mut seen = Vec::new();
    publish_bundle_via(staging, &bundle, &mut |from, to| {
        seen.push(observe(&bundle));
        std::fs::rename(from, to)
    })
    .unwrap();
    seen.push(observe(&bundle));
    assert_eq!(seen, ["old", "absent", "new"]);
    assert!(leftovers(dir.path()).is_empty());
}

/// If the new bundle cannot be renamed into place, the old one is put back
/// and both private directories are removed.
#[test]
fn a_failed_publish_puts_the_old_bundle_back() {
    let dir = tempfile::tempdir().unwrap();
    let bundle = dir.path().join("foo.dSYM");
    complete_bundle(&bundle, b"old");
    let staging = new_bundle_staging(&bundle).unwrap();
    complete_bundle(staging.path(), b"new");
    let staged = staging.path().to_path_buf();

    let error = publish_bundle_via(staging, &bundle, &mut |from, to| {
        if from == staged {
            return Err(std::io::ErrorKind::PermissionDenied.into());
        }
        std::fs::rename(from, to)
    })
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("publishing debug bundle"),
        "{error:#}"
    );
    assert_eq!(observe(&bundle), "old");
    assert!(leftovers(dir.path()).is_empty());

    // With nothing at the bundle path, a failed publish leaves nothing there.
    std::fs::remove_dir_all(&bundle).unwrap();
    let staging = new_bundle_staging(&bundle).unwrap();
    complete_bundle(staging.path(), b"new");
    publish_bundle_via(staging, &bundle, &mut |_, _| {
        Err(std::io::ErrorKind::NotFound.into())
    })
    .unwrap_err();
    assert_eq!(observe(&bundle), "absent");
    assert!(leftovers(dir.path()).is_empty());
}

/// Moving the old bundle aside can fail for reasons other than absence;
/// the publish then stops without touching it.
#[test]
fn a_bundle_that_cannot_be_moved_aside_is_left_alone() {
    let dir = tempfile::tempdir().unwrap();
    let bundle = dir.path().join("foo.dSYM");
    complete_bundle(&bundle, b"old");
    let staging = new_bundle_staging(&bundle).unwrap();
    complete_bundle(staging.path(), b"new");
    let mut calls = 0;
    let error = publish_bundle_via(staging, &bundle, &mut |_, _| {
        calls += 1;
        Err(std::io::ErrorKind::PermissionDenied.into())
    })
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("moving aside stale bundle"),
        "{error:#}"
    );
    assert_eq!(calls, 1);
    assert_eq!(observe(&bundle), "old");
    assert!(leftovers(dir.path()).is_empty());
}
