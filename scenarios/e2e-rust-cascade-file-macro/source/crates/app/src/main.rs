fn main() {
    // Path-independent output so `verify` is stable across relocate.
    println!("dep-cascade: {}", mid::bump());
}
