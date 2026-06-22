/// Middle of the chain: depends on `leaf`, depended on by `app`.
pub fn bump() -> u32 {
    leaf::base() + 2
}
