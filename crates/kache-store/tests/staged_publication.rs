use kache_store::link::{prepare_writable_target_from_bytes, prepare_writable_target_from_file};
use kache_store::opcounts::{thread_copied_bytes, thread_reflinked_bytes};
use std::fs;

fn committed() -> (u64, u64) {
    (thread_copied_bytes(), thread_reflinked_bytes())
}

// One test owns this process's restore counters; other integration test binaries
// have separate statics, so exact deltas do not race concurrent store operations.
#[test]
fn publication_counts_only_committed_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let target = dir.path().join("output");
    fs::write(&source, b"file artifact").unwrap();
    let probe = dir.path().join("probe");
    let clones = kache_store::link::try_reflink(&source, &probe).is_ok();
    if clones {
        fs::remove_file(&probe).unwrap();
    }
    assert_eq!(committed(), (0, 0));

    let prepared = prepare_writable_target_from_file(&source, &target).unwrap();
    assert_eq!(prepared.target(), target);
    assert!(!target.exists());
    assert_eq!(committed(), (0, 0));
    prepared.publish().unwrap();
    assert_eq!(fs::read(&target).unwrap(), b"file artifact");
    if clones {
        assert_eq!(committed(), (0, 13));
    } else {
        assert_eq!(committed(), (13, 0));
    }

    let replacement = prepare_writable_target_from_bytes(&target, b"new artifact").unwrap();
    assert_eq!(fs::read(&target).unwrap(), b"file artifact");
    if clones {
        assert_eq!(committed(), (0, 13));
    } else {
        assert_eq!(committed(), (13, 0));
    }
    replacement.publish_replacing().unwrap();
    assert_eq!(fs::read(&target).unwrap(), b"new artifact");
    assert_eq!(fs::read(&source).unwrap(), b"file artifact");
    if clones {
        assert_eq!(committed(), (12, 13));
    } else {
        assert_eq!(committed(), (25, 0));
    }

    assert!(
        prepare_writable_target_from_bytes(&target, b"refused")
            .unwrap()
            .publish()
            .is_err()
    );
    assert_eq!(fs::read(&target).unwrap(), b"new artifact");
    if clones {
        assert_eq!(committed(), (12, 13));
    } else {
        assert_eq!(committed(), (25, 0));
    }

    let directory = dir.path().join("directory");
    fs::create_dir(&directory).unwrap();
    assert!(
        prepare_writable_target_from_bytes(&directory, b"refused")
            .unwrap()
            .publish_replacing()
            .is_err()
    );
    assert!(directory.is_dir());
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 3);
    if clones {
        assert_eq!(committed(), (12, 13));
    } else {
        assert_eq!(committed(), (25, 0));
    }
}
