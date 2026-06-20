use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    // Write generated Rust under the out-of-tree $OUT_DIR. main.rs includes it
    // by path only (no runtime-embedded path), so the artifact is portable and
    // a relocated build must converge (kunobi-ninja/kache#394).
    let out_dir: PathBuf = env::var_os("OUT_DIR")
        .expect("OUT_DIR is always set by cargo")
        .into();
    fs::write(
        out_dir.join("generated.rs"),
        b"pub fn generated_marker() -> u8 { 7 }\n",
    )
    .expect("writing $OUT_DIR/generated.rs must succeed");
    println!("cargo:rerun-if-changed=build.rs");
}
