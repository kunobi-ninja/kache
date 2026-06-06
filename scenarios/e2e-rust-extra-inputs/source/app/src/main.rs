fn main() {
    // `bake!()` expands to the contents of `data/value.txt`, read at compile
    // time by the proc-macro WITHOUT rustc tracking the file. The co-located
    // `kache.toml` is what makes a change to that file re-key this crate.
    println!("rust-extra-inputs: {}", pm::bake!());
}
