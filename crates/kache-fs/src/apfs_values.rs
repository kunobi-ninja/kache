use crate::Sharing;

pub(crate) const MAY_SHARE: u64 = 0x0000_0001;
pub(crate) const SHARES_ALL: u64 = 0x0000_0040;

pub(crate) fn private_bytes(unique: Option<u64>, sharing: Sharing, allocated: u64) -> Option<u64> {
    // Pending writes can report allocation before private extents are visible.
    if unique == Some(0) && !matches!(sharing, Sharing::Full | Sharing::Partial) && allocated > 0 {
        None
    } else {
        unique
    }
}

pub(crate) fn sharing(
    have_flags: bool,
    flags: u64,
    unique: Option<u64>,
    allocated: u64,
) -> Sharing {
    if have_flags {
        if flags & SHARES_ALL != 0 {
            Sharing::Full
        } else if flags & MAY_SHARE != 0 {
            if unique == Some(0) && allocated > 0 {
                Sharing::Full
            } else {
                Sharing::Partial
            }
        } else {
            Sharing::None
        }
    } else {
        match unique {
            Some(0) if allocated > 0 => Sharing::Unknown,
            Some(bytes) if bytes < allocated => Sharing::Partial,
            Some(_) => Sharing::None,
            None => Sharing::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_allocation_is_not_evidence_of_a_clone() {
        assert_eq!(sharing(true, 0, Some(0), 0), Sharing::None);
        assert_eq!(sharing(false, 0, Some(0), 0), Sharing::None);
        assert_eq!(sharing(true, 0, Some(0), 4096), Sharing::None);
        assert_eq!(sharing(true, MAY_SHARE, Some(0), 0), Sharing::Partial);
        assert_eq!(sharing(true, SHARES_ALL, Some(0), 4096), Sharing::Full);
    }

    #[test]
    fn only_returned_fields_contribute_sharing_evidence() {
        assert_eq!(sharing(false, SHARES_ALL, None, 4096), Sharing::Unknown);
        assert_eq!(sharing(false, 0, Some(2048), 4096), Sharing::Partial);
        assert_eq!(sharing(false, 0, Some(4096), 4096), Sharing::None);
        assert_eq!(sharing(false, 0, Some(0), 4096), Sharing::Unknown);
        assert_eq!(sharing(true, MAY_SHARE, Some(2048), 4096), Sharing::Partial);
        assert_eq!(sharing(true, MAY_SHARE, Some(0), 4096), Sharing::Full);
    }

    #[test]
    fn pending_private_bytes_remain_unknown() {
        assert_eq!(private_bytes(Some(0), Sharing::None, 4096), None);
        assert_eq!(private_bytes(Some(0), Sharing::Unknown, 4096), None);
        assert_eq!(private_bytes(Some(0), Sharing::Full, 4096), Some(0));
        assert_eq!(private_bytes(Some(0), Sharing::None, 0), Some(0));
        assert_eq!(private_bytes(Some(8192), Sharing::None, 8192), Some(8192));
        assert_eq!(private_bytes(None, Sharing::None, 4096), None);
    }
}
