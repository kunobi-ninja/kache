//! A test target with `harness = false`: cargo runs this binary directly, so
//! it must come back from the cache executable.

fn main() {
    assert_eq!(custom_harness::answer(), 42);
    println!("custom harness ok");
}
