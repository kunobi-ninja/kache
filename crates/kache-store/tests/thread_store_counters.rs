use kache_store::opcounts::*;

fn record_all(bytes: u64) {
    record_store_reflinked(bytes);
    record_store_hardlinked(bytes);
    record_store_copied(bytes);
    record_store_copy_cross_device(bytes);
    record_store_copy_permission(bytes);
    record_store_copy_ineligible(bytes);
    record_store_copy_other(bytes);
}

fn expected(bytes: u64) -> StoreThreadBytes {
    StoreThreadBytes {
        reflinked: bytes,
        hardlinked: bytes,
        copied: bytes,
        copy_cross_device: bytes,
        copy_permission: bytes,
        copy_ineligible: bytes,
        copy_other: bytes,
    }
}

#[test]
fn publication_bytes_exclude_work_on_other_threads() {
    assert_eq!(store_thread_bytes(), expected(0));
    record_all(11);
    let other = std::thread::spawn(|| {
        assert_eq!(store_thread_bytes(), expected(0));
        record_all(17);
        assert_eq!(store_thread_bytes(), expected(17));
    });
    other.join().unwrap();
    record_all(3);
    assert_eq!(store_thread_bytes(), expected(14));
    assert_eq!(store_copied_bytes(), 31);
}
