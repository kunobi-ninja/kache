fn main() {
    // Use the dependency so it is actually compiled and linked in.
    let mut buf = itoa::Buffer::new();
    println!("rust-dsym: {}", buf.format(42u32));
}
