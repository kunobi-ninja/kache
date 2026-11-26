//! Every sealed store blob has one mode, whatever its source mode and however
//! it was ingested: `0o555` when it carries an execute bit, `0o444` otherwise.
//! The store directory decides who may reach a blob; the blob's own bits never
//! narrow that. A build output that a hardlinked ingest sealed gets back only
//! its owner write bit once no blob shares its inode, and removing a store
//! name never changes the mode of an inode a build output may share.

use super::*;
use std::os::unix::fs::{MetadataExt, PermissionsExt};

fn mode(path: &Path) -> u32 {
    fs::metadata(path).unwrap().permissions().mode() & 0o777
}

fn set_mode(path: &Path, mode: u32) {
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
}

fn sealed_mode(source_mode: u32) -> u32 {
    if source_mode & 0o111 == 0 {
        0o444
    } else {
        0o555
    }
}

/// Store a single output and return the path of its blob.
fn put_one(store: &Store, key: &str, output: &Path, store_name: &str) -> PathBuf {
    store
        .put(
            key,
            "probe",
            &["lib".to_string()],
            &[],
            "x86_64-unknown-linux-gnu",
            "dev",
            &[(output.to_path_buf(), store_name.to_string())],
            "",
            "",
        )
        .unwrap();
    let meta = store.get(key).unwrap().unwrap();
    store.blob_path(&meta.files[0].hash)
}

/// A build output a hardlinked ingest sealed must be writable by its owner
/// again, and never by group or other.
fn assert_owner_writable_only(output: &Path, case: &str) {
    let mode = mode(output);
    assert_eq!(
        mode & 0o022,
        0,
        "{case}: output {mode:03o} is writable by others"
    );
    assert_ne!(
        mode & 0o200,
        0,
        "{case}: output {mode:03o} is not writable by its owner"
    );
}

/// The complete finite domain of permission bits a blob can arrive with.
#[test]
fn sealing_maps_every_permission_mode_to_the_store_mode() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("blob");
    fs::write(&blob, b"blob bytes").unwrap();
    let wrong: Vec<String> = (0o000..=0o777)
        .filter_map(|before| {
            set_mode(&blob, before);
            set_blob_readonly_checked(&blob).unwrap();
            let after = mode(&blob);
            (after != sealed_mode(before)).then(|| format!("{before:03o} -> {after:03o}"))
        })
        .collect();
    assert!(
        wrong.is_empty(),
        "sealed blobs must be 0444 (0555 with any execute bit): {wrong:?}"
    );
}

/// Outputs written under a restrictive umask, or private executables, are
/// published readable by everyone, through every ingest path.
#[test]
fn owner_only_outputs_publish_world_readable_blobs() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    let ingests = ["copy", "reflink", "hardlink"];
    for (index, ingest) in ingests.into_iter().enumerate() {
        for source_mode in [0o600, 0o640, 0o700, 0o750] {
            let output = dir
                .path()
                .join(format!("libprobe-{index}-{source_mode:o}.rlib"));
            fs::write(&output, format!("{ingest} {source_mode:o}")).unwrap();
            set_mode(&output, source_mode);
            let _reflink = (ingest == "reflink").then(ModeDroppingIngest::enable);
            let _hardlink = (ingest == "hardlink").then(ForceStoreHardlink::enable);
            let blob = put_one(
                &store,
                &format!("owner-only-{index}-{source_mode:o}"),
                &output,
                "libprobe.rlib",
            );
            // A Linux reflink snapshot lands at the umask, without the execute
            // bits; the entry records `executable` from the output instead.
            let expected = if ingest == "reflink" {
                0o444
            } else {
                sealed_mode(source_mode)
            };
            assert_eq!(
                mode(&blob),
                expected,
                "{ingest} put of a {source_mode:03o} output"
            );
        }
    }
}

/// A hardlinked ingest that publishes keeps the output sealed: it is the
/// blob's own inode.
#[test]
fn a_published_hardlinked_output_stays_sealed() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    let output = dir.path().join("libprobe.rlib");
    fs::write(&output, b"rlib bytes").unwrap();
    set_mode(&output, 0o644);
    let _hardlink = ForceStoreHardlink::enable();
    let blob = put_one(&store, "published", &output, "libprobe.rlib");
    assert_eq!(
        fs::metadata(&blob).unwrap().ino(),
        fs::metadata(&output).unwrap().ino(),
        "the forced ingest must share the output inode"
    );
    assert_eq!(mode(&output), 0o444);
}

/// A hardlinked snapshot of bytes the store already holds is discarded, which
/// leaves the output with its own inode: only its owner write bit returns.
#[test]
fn a_hardlinked_ingest_of_an_existing_blob_returns_only_the_owner_write_bit() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    let first = dir.path().join("first.rlib");
    fs::write(&first, b"rlib bytes").unwrap();
    let blob = put_one(&store, "existing", &first, "libprobe.rlib");
    for output_mode in [0o600, 0o644, 0o664, 0o666] {
        let output = dir.path().join(format!("libprobe-{output_mode:o}.rlib"));
        fs::write(&output, b"rlib bytes").unwrap();
        set_mode(&output, output_mode);
        let _hardlink = ForceStoreHardlink::enable();
        put_one(
            &store,
            &format!("dedup-{output_mode:o}"),
            &output,
            "libprobe.rlib",
        );
        assert_ne!(
            fs::metadata(&blob).unwrap().ino(),
            fs::metadata(&output).unwrap().ino()
        );
        assert_owner_writable_only(&output, &format!("dedup put of a {output_mode:03o} output"));
        assert_eq!(mode(&blob), 0o444);
    }
}

/// An older writer may have sealed a blob at a narrower mode. Content
/// addressing makes a later put of the same bytes reuse that blob, so the put
/// seals the blob it reuses, through every ingest path.
#[test]
fn a_put_that_reuses_a_narrow_blob_seals_it() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    let ingests = ["copy", "reflink", "hardlink"];
    for (index, ingest) in ingests.into_iter().enumerate() {
        for (narrow, expected) in [(0o400, 0o444), (0o500, 0o555)] {
            let output = dir.path().join(format!("libprobe-{index}-{narrow:o}.rlib"));
            let write_output = || {
                fs::write(&output, format!("{ingest} {narrow:o}")).unwrap();
                set_mode(&output, narrow | 0o200);
            };
            write_output();
            let blob = put_one(
                &store,
                &format!("old-{index}-{narrow:o}"),
                &output,
                "libprobe.rlib",
            );
            fs::remove_file(&output).unwrap();
            set_mode(&blob, narrow);

            write_output();
            let _reflink = (ingest == "reflink").then(ModeDroppingIngest::enable);
            let _hardlink = (ingest == "hardlink").then(ForceStoreHardlink::enable);
            let reused = put_one(
                &store,
                &format!("new-{index}-{narrow:o}"),
                &output,
                "libprobe.rlib",
            );
            assert_eq!(reused, blob);
            assert_eq!(
                mode(&blob),
                expected,
                "{ingest} put reusing a {narrow:03o} blob"
            );
        }
    }
}

/// The early return for an already present blob seals it.
#[test]
fn materializing_onto_an_existing_blob_seals_it() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("libprobe.rlib");
    fs::write(&source, b"rlib bytes").unwrap();
    let blob = dir.path().join("blobs").join("aa").join("a".repeat(64));
    fs::create_dir_all(blob.parent().unwrap()).unwrap();
    fs::write(&blob, b"rlib bytes").unwrap();
    set_mode(&blob, 0o400);
    assert!(!materialize_blob(&source, &blob, false).unwrap());
    assert_eq!(mode(&blob), 0o444);
}

/// A hardlinked snapshot that loses the publish race to another writer is
/// discarded without changing the output's mode; the put then gives the output
/// back only its owner write bit.
#[test]
fn a_lost_race_leaves_the_hardlinked_output_owner_writable_only() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    let output = dir.path().join("libprobe.rlib");
    let bytes = b"raced rlib bytes";
    fs::write(&output, bytes).unwrap();
    set_mode(&output, 0o644);
    let _hardlink = ForceStoreHardlink::enable();
    let (staged, ingest) = store.stage_blob_from_source(&output, true).unwrap();
    assert!(matches!(ingest, StoreIngest::Hardlink));
    let mut sealed_output = SealedOutput::new(&output, &staged, ingest);
    let hash = crate::file_hash::hash_file(&staged).unwrap();
    let blob = store.blob_path(&hash);
    sealed_output.held_by(blob.clone());
    let published = store
        .publish_staged_blob_before_lock(&staged, ingest, &hash, bytes.len() as u64, || {
            fs::create_dir_all(blob.parent().unwrap()).unwrap();
            fs::write(&blob, bytes).unwrap();
            set_blob_readonly(&blob);
        })
        .unwrap();
    assert!(!published, "the other writer won the race");
    assert!(!staged.exists());
    assert_eq!(
        mode(&output),
        0o444,
        "discarding the snapshot changed the output mode"
    );

    drop(sealed_output);
    assert_eq!(mode(&output), 0o644);
    assert_eq!(mode(&blob), 0o444);
}

/// The put's guard leaves an output sealed while the blob it published is the
/// output's own inode, and lets it go once nothing shares it.
#[test]
fn a_sealed_output_stays_sealed_while_its_blob_shares_the_inode() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    let output = dir.path().join("libprobe.rlib");
    fs::write(&output, b"published rlib bytes").unwrap();
    set_mode(&output, 0o644);
    let _hardlink = ForceStoreHardlink::enable();
    let (staged, ingest) = store.stage_blob_from_source(&output, true).unwrap();
    let mut sealed_output = SealedOutput::new(&output, &staged, ingest);
    let hash = crate::file_hash::hash_file(&staged).unwrap();
    let blob = store.blob_path(&hash);
    sealed_output.held_by(blob.clone());
    assert!(
        store
            .publish_staged_blob(&staged, ingest, &hash, 20)
            .unwrap()
    );
    drop(sealed_output);
    assert_eq!(mode(&output), 0o444, "the output is the published blob");

    unlink_blob(&blob);
    drop(SealedOutput::new(&output, &blob, ingest));
    assert_eq!(mode(&output), 0o644, "nothing shares the output any more");
}

/// Unlinking a blob or a staging leftover never changes the mode of the inode
/// it may share with a restored or ingested build output.
#[test]
fn unlinking_a_shared_blob_or_staging_name_leaves_the_output_mode_alone() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    let output = dir.path().join("libshared.rlib");
    fs::write(&output, b"shared rlib bytes").unwrap();
    let _hardlink = ForceStoreHardlink::enable();
    let blob = put_one(&store, "shared", &output, "libshared.rlib");
    assert_eq!(
        fs::metadata(&blob).unwrap().ino(),
        fs::metadata(&output).unwrap().ino()
    );
    store.remove_entry("shared").unwrap();
    assert!(!blob.exists(), "removing the only entry unlinks its blob");
    assert_eq!(
        mode(&output),
        0o444,
        "unlinking the blob changed the output mode"
    );

    let leftover = store.staging_dir().join("stage-1-0.tmp");
    fs::create_dir_all(store.staging_dir()).unwrap();
    fs::hard_link(&output, &leftover).unwrap();
    assert_eq!(store.sweep_stale_staging(Duration::ZERO).removed, 1);
    assert!(!leftover.exists(), "the sweep must unlink the leftover");
    assert_eq!(
        mode(&output),
        0o444,
        "sweeping the staging name changed the output mode"
    );
}

/// A refused hardlinked ingest discards its snapshot and gives the output it
/// sealed back its owner write bit.
#[test]
fn a_refused_hardlinked_ingest_returns_only_the_owner_write_bit() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    let output = dir.path().join("empty.rlib");
    fs::write(&output, b"").unwrap();
    set_mode(&output, 0o644);
    let _hardlink = ForceStoreHardlink::enable();
    let refused = store.put(
        "zero",
        "probe",
        &["lib".to_string()],
        &[],
        "x86_64-unknown-linux-gnu",
        "dev",
        &[(output.clone(), "libempty.rlib".to_string())],
        "",
        "",
    );
    assert!(refused.is_err(), "a zero-byte rlib is refused");
    assert_eq!(mode(&output), 0o644);
}

/// An output staged as an independent copy is never sealed, so a put leaves
/// its mode alone, read-only included.
#[test]
fn a_copied_ingest_leaves_a_read_only_output_read_only() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    let output = dir.path().join("libprobe.rlib");
    fs::write(&output, b"rlib bytes").unwrap();
    set_mode(&output, 0o440);
    let _reflink = ModeDroppingIngest::enable();
    put_one(&store, "copied", &output, "libprobe.rlib");
    assert_eq!(mode(&output), 0o440);
}

/// Giving a file write access adds the owner write bit and nothing else.
#[test]
fn owner_writable_adds_only_the_owner_write_bit() {
    for (before, after) in [
        (0o444, 0o644),
        (0o644, 0o644),
        (0o400, 0o600),
        (0o555, 0o755),
    ] {
        let writable = crate::link::owner_writable(fs::Permissions::from_mode(before));
        assert_eq!(writable.mode() & 0o777, after, "{before:03o}");
    }
}
