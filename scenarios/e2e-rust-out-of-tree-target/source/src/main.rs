// Path-only OUT_DIR use: include the generated source by path, but never embed
// the path as a runtime value. With the target dir out-of-tree, this exercises
// whether kache normalizes the out-of-tree `OUT_DIR` env-dep consistently across
// locations (kunobi-ninja/kache#394, from #304).
include!(concat!(env!("OUT_DIR"), "/generated.rs"));

fn main() {
    println!("generated marker: {}", generated_marker());
}
