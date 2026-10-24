include!(env!("GEN_BUILD_CONSTS"));
/// Exposes the generated value so the scenario can execute the restored library.
pub fn n() -> u32 {
    let _alias = include_str!(env!("GEN_BUILD_CONSTS_ALIAS"));
    N
}
