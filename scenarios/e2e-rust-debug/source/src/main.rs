fn main() {
    // Use the dependency so it is actually compiled and linked —
    // its debug `.rlib` is what this fixture exercises.
    let mut buf = itoa::Buffer::new();
    println!("rust-debug: {}", buf.format(42u32));
}
