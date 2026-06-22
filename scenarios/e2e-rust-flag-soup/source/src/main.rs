fn main() {
    // Use the dependency so its rlib is compiled, cached, and restored
    // under the realistic RUSTFLAGS this fixture sets.
    let mut buf = itoa::Buffer::new();
    println!("rust-flag-soup: {}", buf.format(42u32));
}
