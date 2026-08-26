//! Repository-only formal verification harnesses.

#[cfg(kani)]
mod checked_regions_under_proof {
    // Compile the production implementation itself while keeping the proof
    // harness out of both published crates and ordinary mutation discovery.
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../src/checked_regions.rs"
    ));
}

#[cfg(kani)]
mod kani_proofs {
    use super::checked_regions_under_proof::{
        checked_file_region, checked_nonempty_contiguous_region,
    };

    #[kani::proof]
    fn checked_file_region_matches_its_mathematical_contract() {
        let file_len: usize = kani::any();
        let offset: u64 = kani::any();
        let size: u64 = kani::any();
        let minimum_offset: usize = kani::any();
        let exact_end = u128::from(offset) + u128::from(size);
        let valid = exact_end <= file_len as u128
            && (size == 0 || u128::from(offset) >= minimum_offset as u128);
        let region = checked_file_region(file_len, offset, size, minimum_offset);
        kani::cover!(valid && exact_end == file_len as u128);

        assert_eq!(region.is_some(), valid);
        if let Some((start, end)) = region {
            assert_eq!(start as u128, u128::from(offset));
            assert_eq!(end as u128, exact_end);
        }
    }

    #[kani::proof]
    fn checked_contiguous_region_matches_its_mathematical_contract() {
        let expected_offset: u64 = kani::any();
        let offset: u64 = kani::any();
        let length: u64 = kani::any();
        let payload_len: usize = kani::any();
        let exact_end = u128::from(offset) + u128::from(length);
        let valid = offset == expected_offset
            && length != 0
            && exact_end <= u128::from(u64::MAX)
            && exact_end <= payload_len as u128;
        let region =
            checked_nonempty_contiguous_region(expected_offset, offset, length, payload_len);
        kani::cover!(valid && exact_end == payload_len as u128);

        assert_eq!(region.is_some(), valid);
        if let Some((next_offset, (start, end))) = region {
            assert_eq!(u128::from(next_offset), exact_end);
            assert_eq!(start as u128, u128::from(offset));
            assert_eq!(end as u128, exact_end);
            assert!(start < end);
        }
    }
}
