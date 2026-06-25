/// CONTROL: cascade root candidate via `file!()`. Unlike `env!`, `file!()` IS
/// subject to `--remap-path-prefix`, so kache's remap injection should rewrite
/// it to a `<WORKSPACE>`-relative sentinel identically in both clones. This
/// fixture is the negative control: it should CONVERGE (relocate hits) and
/// proves remap covers `file!()` even though it does not cover `env!`.
pub const SRC_FILE: &str = file!();

pub fn base() -> u32 {
    (SRC_FILE.len() as u32).min(0) + 40
}
