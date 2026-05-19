fn main() {
    // Invoke the sibling crate's proc-macro so `pm` is actually
    // compiled, cached, and linked.
    println!("rust-workspace: {}", pm::answer!());
}
