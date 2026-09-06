use kache_store::link::{prepare_writable_target_from_bytes, prepare_writable_target_from_file};
use kache_store::opcounts::copied_bytes;
use std::fs;

// One test owns this process's restore counters; other integration test binaries
// have separate statics, so exact deltas do not race concurrent store operations.
#[test]
fn publication_counts_only_committed_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let target = dir.path().join("output");
    fs::write(&source, b"file artifact").unwrap();
    let before = copied_bytes();

    let prepared = prepare_writable_target_from_file(&source, &target).unwrap();
    assert_eq!(prepared.target(), target);
    assert!(!target.exists());
    assert_eq!(copied_bytes(), before);
    prepared.publish().unwrap();
    assert_eq!(fs::read(&target).unwrap(), b"file artifact");
    assert_eq!(copied_bytes(), before + 13);

    let replacement = prepare_writable_target_from_bytes(&target, b"new artifact").unwrap();
    assert_eq!(fs::read(&target).unwrap(), b"file artifact");
    assert_eq!(copied_bytes(), before + 13);
    replacement.publish_replacing().unwrap();
    assert_eq!(fs::read(&target).unwrap(), b"new artifact");
    assert_eq!(fs::read(&source).unwrap(), b"file artifact");
    assert_eq!(copied_bytes(), before + 25);

    assert!(
        prepare_writable_target_from_bytes(&target, b"refused")
            .unwrap()
            .publish()
            .is_err()
    );
    assert_eq!(fs::read(&target).unwrap(), b"new artifact");
    assert_eq!(copied_bytes(), before + 25);

    let directory = dir.path().join("directory");
    fs::create_dir(&directory).unwrap();
    assert!(
        prepare_writable_target_from_bytes(&directory, b"refused")
            .unwrap()
            .publish_replacing()
            .is_err()
    );
    assert!(directory.is_dir());
    assert_eq!(copied_bytes(), before + 25);
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 3);
}
