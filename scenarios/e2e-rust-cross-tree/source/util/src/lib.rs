/// A plain rlib member: tree A's artifact is the hardlink carrier used by
/// the active-reader contract. Later consumers must restore privately before
/// applying their mtime stamp (#794).
pub fn offset() -> u32 {
    0
}
