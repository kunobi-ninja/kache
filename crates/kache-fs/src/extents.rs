//! Bounded FIEMAP accounting, separate from the ioctl so malformed replies can
//! be tested on every host. Values follow Linux's `uapi/linux/fiemap.h`.

use crate::Sharing;
use std::io;

pub(crate) const LAST: u32 = 0x1;
pub(crate) const SHARED: u32 = 0x2000;
// Only aligned, unencoded extents have lengths usable as allocation counts.
const ACCOUNTABLE_FLAGS: u32 = 0x3801; // LAST | SHARED | UNWRITTEN | MERGED
pub(crate) const EXTENTS_PER_PAGE: usize = 128;
const MAX_PAGES: usize = 64;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Extent {
    pub logical: u64,
    pub length: u64,
    pub flags: u32,
}

#[derive(Debug, Default)]
pub(crate) struct Accounting {
    pub unique: u64,
    shared: u64,
}

impl Accounting {
    pub fn sharing(&self) -> Sharing {
        match (self.shared > 0, self.unique > 0) {
            (true, true) => Sharing::Partial,
            (true, false) => Sharing::Full,
            (false, _) => Sharing::None,
        }
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub(crate) fn walk(
    mut next_batch: impl FnMut(u64) -> io::Result<Vec<Extent>>,
) -> io::Result<Option<Accounting>> {
    let mut offset = 0;
    let mut accounting = Accounting::default();
    for _ in 0..MAX_PAGES {
        let batch = next_batch(offset)?;
        if batch.len() > EXTENTS_PER_PAGE {
            return Err(invalid("FIEMAP returned more extents than requested"));
        }
        if batch.is_empty() {
            return Ok(Some(accounting));
        }
        for (index, extent) in batch.iter().enumerate() {
            if extent.flags & !ACCOUNTABLE_FLAGS != 0 {
                return Ok(None);
            }
            if extent.length == 0 || extent.logical < offset {
                return Err(invalid("FIEMAP extents overlap or make no progress"));
            }
            offset = extent
                .logical
                .checked_add(extent.length)
                .ok_or_else(|| invalid("FIEMAP logical range overflow"))?;
            let total = if extent.flags & SHARED != 0 {
                &mut accounting.shared
            } else {
                &mut accounting.unique
            };
            *total = total
                .checked_add(extent.length)
                .ok_or_else(|| invalid("FIEMAP extent lengths overflow"))?;
            if extent.flags & LAST != 0 {
                if index + 1 != batch.len() {
                    return Err(invalid("FIEMAP contains extents after LAST"));
                }
                return Ok(Some(accounting));
            }
        }
        // No complete map was reported. Do not issue a zero-length window.
        if offset == u64::MAX {
            return Ok(None);
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(logical: u64, length: u64, flags: u32) -> Extent {
        Extent {
            logical,
            length,
            flags,
        }
    }

    #[test]
    fn multiple_pages_keep_holes_out_of_the_private_total() {
        let mut offsets = Vec::new();
        let result = walk(|offset| {
            offsets.push(offset);
            Ok(match offset {
                0 => vec![e(4096, 4096, SHARED)],
                8192 => vec![e(16384, 8192, LAST)],
                _ => panic!("unexpected page offset {offset}"),
            })
        })
        .unwrap()
        .unwrap();
        assert_eq!(offsets, [0, 8192]);
        assert_eq!(result.unique, 8192);
        assert_eq!(result.sharing(), Sharing::Partial);
    }

    #[test]
    fn last_and_empty_page_are_completion_signals() {
        let all_shared = walk(|_| Ok(vec![e(0, 4096, SHARED | LAST)]))
            .unwrap()
            .unwrap();
        assert_eq!(all_shared.unique, 0);
        assert_eq!(all_shared.sharing(), Sharing::Full);
        let private = walk(|offset| {
            Ok(if offset == 0 {
                vec![e(0, 4096, 0x800 | 0x1000)]
            } else {
                vec![]
            })
        })
        .unwrap()
        .unwrap();
        assert_eq!(private.unique, 4096);
        assert_eq!(private.sharing(), Sharing::None);
        let sparse = walk(|_| Ok(vec![])).unwrap().unwrap();
        assert_eq!(sparse.unique, 0);
        assert_eq!(sparse.sharing(), Sharing::None);
    }

    #[test]
    fn ambiguous_or_encoded_extents_discard_all_evidence() {
        for flag in [0x2, 0x4, 0x8, 0x80, 0x100, 0x200, 0x400, 0x4000, u32::MAX] {
            assert!(
                walk(|_| Ok(vec![e(0, 4096, 0), e(4096, 4096, flag | LAST)]))
                    .unwrap()
                    .is_none(),
                "flag {flag:#x}"
            );
        }
    }

    #[test]
    fn malformed_maps_never_become_partial_answers() {
        for batch in [
            vec![e(0, 0, LAST)],
            vec![e(u64::MAX, 1, LAST)],
            vec![e(0, 4096, 0), e(0, 4096, LAST)],
            vec![e(0, 4096, LAST), e(4096, 4096, 0)],
            vec![e(0, 4096, 0); EXTENTS_PER_PAGE + 1],
        ] {
            assert_eq!(
                walk(|_| Ok(batch.clone())).unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
        }
        assert!(
            walk(|offset| Ok(vec![e(offset, 4096, 0)]))
                .unwrap()
                .is_none()
        );
        assert!(walk(|_| Ok(vec![e(0, u64::MAX, 0)])).unwrap().is_none());
        assert!(walk(|_| Err(io::Error::from(io::ErrorKind::PermissionDenied))).is_err());
    }

    #[test]
    fn a_full_page_is_valid_but_a_backwards_next_page_is_not() {
        let result = walk(|offset| {
            Ok(if offset == 0 {
                (0..EXTENTS_PER_PAGE)
                    .map(|i| e(i as u64 * 4096, 4096, 0))
                    .collect()
            } else {
                vec![]
            })
        })
        .unwrap()
        .unwrap();
        assert_eq!(result.unique, EXTENTS_PER_PAGE as u64 * 4096);
        let result = walk(|offset| Ok(vec![e(0, 4096, if offset == 0 { 0 } else { LAST })]));
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }
}
