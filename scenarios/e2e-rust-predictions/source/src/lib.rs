mod a;

/// Exercises every closure shape predictions must reproduce: a module file,
/// an included text file, and compile-time environment reads.
pub fn answer() -> &'static str {
    let _ = include_str!("data.txt");
    let _ = env!("CARGO_PKG_NAME");
    a::greet()
}
