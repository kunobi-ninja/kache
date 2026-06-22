/// Bottom of the chain. Its rmeta feeds `mid`, whose rmeta feeds `app` —
/// a path leak here is what cascades downstream across clones.
pub fn base() -> u32 {
    40
}
