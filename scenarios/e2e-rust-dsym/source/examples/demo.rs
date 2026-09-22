fn main() {
    let mut buf = itoa::Buffer::new();
    println!("demo: {}", buf.format(7u8));
}
