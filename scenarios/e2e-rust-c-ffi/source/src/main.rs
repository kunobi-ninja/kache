// Demonstrates Rust calling a C function built by build.rs.
// The print + assert lets the e2e smoke test verify both the rustc
// path (compiling this source) and the cc path (compiling the linked
// adder.c) produced a working binary.

unsafe extern "C" {
    fn adder_add(a: i32, b: i32) -> i32;
}

fn main() {
    let result = unsafe { adder_add(3, 4) };
    println!("rust+C FFI: 3 + 4 = {result}");
    assert_eq!(result, 7, "FFI call returned wrong sum");
}
