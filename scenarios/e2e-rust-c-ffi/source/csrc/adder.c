// Trivial C function exposed to the Rust crate via FFI.
// Kept tiny so the e2e doesn't depend on heavyweight system headers
// beyond the basics — caching behavior of stdlib headers is exercised
// by test-projects/c-hello and test-projects/cpp-hello.

int adder_add(int a, int b) {
    return a + b;
}
