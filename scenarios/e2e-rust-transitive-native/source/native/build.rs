// Compiles csrc/value.c into a static library that rustc bundles into
// native's rlib.
fn main() {
    cc::Build::new().file("csrc/value.c").compile("value");
    println!("cargo:rerun-if-changed=csrc/value.c");
}
