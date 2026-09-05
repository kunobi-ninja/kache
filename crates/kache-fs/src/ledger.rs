//! Hard-link accounting.
//!
//! A hard-linked file frees nothing until its *last* name goes, so a sum that
//! counts each name separately overstates reclaim. The ledger counts an inode
//! once, and tracks which inodes still have links *outside* the measured set —
//! those bytes will not actually come back, and callers must be able to say so.

use std::collections::HashMap;

use crate::FileSizing;

/// Filesystem-unique file identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct InodeId {
    pub dev: u64,
    pub ino: u64,
}

#[derive(Debug, Clone, Copy)]
struct Entry {
    links_seen: u64,
    nlink: u64,
    allocated: u64,
    unique: u64,
}

#[derive(Debug, Default)]
pub struct InodeLedger {
    seen: HashMap<InodeId, Entry>,
}

impl InodeLedger {
    pub fn new() -> Self {
        Self::default()
    }

    /// Should this file's bytes be added to the running total?
    ///
    /// `false` for a second or later link to an inode already counted.
    pub fn admit(&mut self, s: &FileSizing) -> bool {
        if s.nlink <= 1 {
            return true;
        }
        match self.seen.get_mut(&s.inode) {
            Some(e) => {
                e.links_seen += 1;
                false
            }
            None => {
                self.seen.insert(
                    s.inode,
                    Entry {
                        links_seen: 1,
                        nlink: s.nlink,
                        allocated: s.allocated,
                        unique: s.unique.unwrap_or(0),
                    },
                );
                true
            }
        }
    }

    /// Inodes counted that still have names outside the measured set. Their
    /// bytes were included in the total but will NOT be freed by deleting the
    /// set — report this rather than over-promising.
    pub fn pinned_outside(&self) -> PinnedOutside {
        let mut p = PinnedOutside::default();
        for e in self.seen.values() {
            if e.links_seen < e.nlink {
                p.inodes += 1;
                p.bytes += e.allocated;
                p.unique_bytes += e.unique;
            }
        }
        p
    }

    pub fn distinct_inodes(&self) -> usize {
        self.seen.len()
    }
}

/// Multiply-linked data counted in a total but retained by names elsewhere.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PinnedOutside {
    pub inodes: u64,
    /// Allocated bytes retained by another hard-link name.
    pub bytes: u64,
    /// Bytes that would otherwise have been counted as physically reclaimable.
    #[cfg_attr(feature = "serde", serde(default))]
    pub unique_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Sharing;

    fn f(ino: u64, nlink: u64, allocated: u64) -> FileSizing {
        FileSizing {
            logical: allocated,
            allocated,
            unique: Some(allocated),
            sharing: Sharing::None,
            inode: InodeId { dev: 1, ino },
            nlink,
            clone_id: None,
        }
    }

    #[test]
    fn counts_an_inode_once_regardless_of_link_count() {
        let mut l = InodeLedger::new();
        assert_eq!(l.distinct_inodes(), 0);
        assert!(l.admit(&f(7, 2, 100)));
        assert!(!l.admit(&f(7, 2, 100)));
        assert_eq!(l.distinct_inodes(), 1);
        assert!(l.admit(&f(8, 2, 100)));
        assert_eq!(l.distinct_inodes(), 2);
    }

    #[test]
    fn unlinked_files_always_admitted() {
        let mut l = InodeLedger::new();
        assert!(l.admit(&f(1, 1, 10)));
        assert!(l.admit(&f(2, 1, 10)));
        assert_eq!(l.pinned_outside(), PinnedOutside::default());
    }

    #[test]
    fn reports_bytes_pinned_by_links_outside_the_set() {
        let mut l = InodeLedger::new();
        // nlink=3 but only two names live inside the walked set.
        l.admit(&f(9, 3, 4096));
        l.admit(&f(9, 3, 4096));
        assert_eq!(
            l.pinned_outside(),
            PinnedOutside {
                inodes: 1,
                bytes: 4096,
                unique_bytes: 4096,
            }
        );
    }

    #[test]
    fn fully_contained_hardlinks_are_not_pinned() {
        let mut l = InodeLedger::new();
        l.admit(&f(9, 2, 4096));
        l.admit(&f(9, 2, 4096));
        assert_eq!(l.pinned_outside(), PinnedOutside::default());
    }
}
