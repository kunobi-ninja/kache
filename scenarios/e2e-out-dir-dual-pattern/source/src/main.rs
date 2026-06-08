include!(concat!(env!("OUT_DIR"), "/generated.rs"));

const OUT_DIR: &str = env!("OUT_DIR");

fn main() {
    let path = format!("{OUT_DIR}/data.txt");
    let contents = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!("read({path}) failed: {e}");
    });
    print!("{contents}");
    println!("generated include: {}", generated_marker());
}
