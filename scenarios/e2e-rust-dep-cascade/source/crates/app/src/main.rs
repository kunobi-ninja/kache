fn main() {
    // Exercises the whole chain: app -> mid -> leaf.
    println!("dep-cascade: {}", mid::bump());
}
