// Mirrors typenum-1.16.0/build/main.rs: point a rustc-env var at an
// OUT_DIR-generated include file.
use std::{env, fs, path::Path};
fn main() {
    let out = env::var("OUT_DIR").unwrap();
    let generated = "consts.rs";
    let dest = Path::new(&out).join(generated);
    fs::write(&dest, "pub const N: u32 = 42;\n").unwrap();
    println!("cargo:rustc-env=GEN_BUILD_CONSTS={}", dest.display());
    println!("cargo:rustc-env=GEN_BUILD_CONSTS_ALIAS={out}/./{generated}");
    println!("cargo:rerun-if-changed=build.rs");
}
