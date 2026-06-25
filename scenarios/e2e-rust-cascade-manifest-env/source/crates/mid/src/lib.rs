/// Middle of the chain: depends on `leaf`, depended on by `app`. Its own
/// key is path-clean; it diverges across clones ONLY if `leaf`'s rmeta
/// (which feeds mid's extern hash) diverges. That is the cascade.
pub fn bump() -> u32 {
    leaf::base() + 2
}
