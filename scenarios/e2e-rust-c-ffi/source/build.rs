// Builds csrc/adder.c into a static library that the Rust crate links
// against. The cc crate spawns the C compiler indicated by $CC (or
// platform default), so when the e2e script sets `CC="kache cc"` this
// invocation flows through the kache wrapper.

fn main() {
    cc::Build::new()
        .file("csrc/adder.c")
        .compile("adder");
    println!("cargo:rerun-if-changed=csrc/adder.c");
    println!("cargo:rerun-if-changed=build.rs");
}
