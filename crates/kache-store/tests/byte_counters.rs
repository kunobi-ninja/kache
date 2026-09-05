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

#[test]
fn store_copy_reasons_accumulate_independently() {
    record_store_copy_cross_device(11);
    record_store_copy_permission(13);
    record_store_copy_ineligible(17);
    record_store_copy_other(19);
    assert_eq!(
        (
            store_copy_cross_device_bytes(),
            store_copy_permission_bytes(),
            store_copy_ineligible_bytes(),
            store_copy_other_bytes(),
        ),
        (11, 13, 17, 19)
    );
    record_store_copy_cross_device(2);
    record_store_copy_permission(3);
    record_store_copy_ineligible(5);
    record_store_copy_other(7);
    assert_eq!(
        (
            store_copy_cross_device_bytes(),
            store_copy_permission_bytes(),
            store_copy_ineligible_bytes(),
            store_copy_other_bytes(),
        ),
        (13, 16, 22, 26)
    );
}

#[test]
fn restore_copy_reasons_accumulate_independently() {
    record_restore_copy_cross_device(23);
    record_restore_copy_permission(29);
    record_restore_copy_other(37);
    assert_eq!(
        (
            restore_copy_cross_device_bytes(),
            restore_copy_permission_bytes(),
            restore_copy_other_bytes(),
        ),
        (23, 29, 37)
    );
    record_restore_copy_cross_device(3);
    record_restore_copy_permission(5);
    record_restore_copy_other(7);
    assert_eq!(
        (
            restore_copy_cross_device_bytes(),
            restore_copy_permission_bytes(),
            restore_copy_other_bytes(),
        ),
        (26, 34, 44)
    );
}

#[cfg(unix)]
#[test]
fn exclusive_restore_copy_bytes_accumulate() {
    record_restore_copy_exclusive(31);
    assert_eq!(restore_copy_exclusive_bytes(), 31);
    record_restore_copy_exclusive(7);
    assert_eq!(restore_copy_exclusive_bytes(), 38);
}
