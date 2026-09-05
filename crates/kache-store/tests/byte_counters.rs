// A separate test binary keeps these process-wide counters isolated from
// storage tests that also materialize files.
use kache_store::opcounts::*;

#[test]
fn restore_bytes_accumulate_for_each_materialization_method() {
    record_reflinked(64);
    record_hardlinked(32);
    record_copied(16);
    assert_eq!(
        (reflinked_bytes(), hardlinked_bytes(), copied_bytes()),
        (64, 32, 16)
    );

    record_reflinked(1);
    record_hardlinked(2);
    record_copied(3);
    assert_eq!(
        (reflinked_bytes(), hardlinked_bytes(), copied_bytes()),
        (65, 34, 19)
    );
}

#[test]
fn store_bytes_accumulate_for_each_materialization_method() {
    record_store_reflinked(128);
    record_store_hardlinked(32);
    record_store_copied(64);
    assert_eq!(
        (
            store_reflinked_bytes(),
            store_hardlinked_bytes(),
            store_copied_bytes()
        ),
        (128, 32, 64)
    );

    record_store_reflinked(3);
    record_store_hardlinked(2);
    record_store_copied(1);
    assert_eq!(
        (
            store_reflinked_bytes(),
            store_hardlinked_bytes(),
            store_copied_bytes()
        ),
        (131, 34, 65)
    );
}
