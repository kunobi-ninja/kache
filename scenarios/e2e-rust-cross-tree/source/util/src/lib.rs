/// A plain rlib member: its restored artifact is the hardlink carrier the
/// cross-tree contract needs (proc-macro dylibs and bins may restore via
/// copy, which would leave no shared inodes to perturb).
pub fn offset() -> u32 {
    0
}
