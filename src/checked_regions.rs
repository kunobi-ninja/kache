/// Return `(start, end)` after proving a file-offset range is representable,
/// does not overlap `minimum_offset` when non-empty, and lies within
/// `file_len`.
///
/// Empty regions carry no bytes, so their offset need not be at or after
/// `minimum_offset`; it must still be representable and within the file.
pub(crate) fn checked_file_region(
    file_len: usize,
    offset: u64,
    size: u64,
    minimum_offset: usize,
) -> Option<(usize, usize)> {
    let start = usize::try_from(offset).ok()?;
    let size = usize::try_from(size).ok()?;
    if size != 0 && start < minimum_offset {
        return None;
    }
    let end = start.checked_add(size)?;
    if end > file_len {
        return None;
    }
    Some((start, end))
}

/// Validate one non-empty region in a gap-free sequence.
///
/// The returned `u64` is the expected offset of the next region. The nested
/// pair is safe to use directly as a slice range for the `payload_len` passed
/// here.
pub(crate) fn checked_nonempty_contiguous_region(
    expected_offset: u64,
    offset: u64,
    length: u64,
    payload_len: usize,
) -> Option<(u64, (usize, usize))> {
    if offset != expected_offset || length == 0 {
        return None;
    }
    let next_offset = offset.checked_add(length)?;
    let range = checked_file_region(payload_len, offset, length, 0)?;
    Some((next_offset, range))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_file_region_enforces_bounds_and_empty_region_semantics() {
        assert_eq!(checked_file_region(32, 8, 8, 8), Some((8, 16)));
        assert_eq!(checked_file_region(32, 24, 8, 8), Some((24, 32)));
        assert_eq!(checked_file_region(32, 0, 0, 8), Some((0, 0)));
        assert_eq!(checked_file_region(32, 7, 1, 8), None);
        assert_eq!(checked_file_region(32, 25, 8, 8), None);
        assert_eq!(checked_file_region(32, u64::MAX, 1, 8), None);
    }

    #[test]
    fn checked_contiguous_region_rejects_gaps_overlaps_empty_and_overflow() {
        assert_eq!(
            checked_nonempty_contiguous_region(0, 0, 8, 16),
            Some((8, (0, 8)))
        );
        assert_eq!(
            checked_nonempty_contiguous_region(8, 8, 8, 16),
            Some((16, (8, 16)))
        );
        assert_eq!(checked_nonempty_contiguous_region(8, 9, 1, 16), None);
        assert_eq!(checked_nonempty_contiguous_region(8, 7, 1, 16), None);
        assert_eq!(checked_nonempty_contiguous_region(8, 8, 0, 16), None);
        assert_eq!(checked_nonempty_contiguous_region(8, 8, 9, 16), None);
        assert_eq!(
            checked_nonempty_contiguous_region(u64::MAX, u64::MAX, 1, usize::MAX),
            None
        );
    }
}
