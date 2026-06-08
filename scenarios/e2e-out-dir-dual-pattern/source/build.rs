use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let out_dir: PathBuf = env::var_os("OUT_DIR")
        .expect("OUT_DIR is always set by cargo")
        .into();

    fs::write(
        out_dir.join("generated.rs"),
        b"pub fn generated_marker() -> u8 { 7 }\n",
    )
    .expect("writing $OUT_DIR/generated.rs must succeed");
    fs::write(out_dir.join("data.txt"), b"hello from build.rs\n")
        .expect("writing $OUT_DIR/data.txt must succeed");
    println!("cargo:rerun-if-changed=build.rs");
}
