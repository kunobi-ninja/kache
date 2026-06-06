// Demonstrates the `const X: &str = env!("OUT_DIR")` pattern that
// makes a compiled binary depend on the OUT_DIR path at RUNTIME, not
// just at compile time. Issue kunobi-ninja/kache#75 flagged this as
// the case where blanket OUT_DIR normalization in the cache key
// would produce false hits — a binary cached at /worktree-A would be
// restored at /worktree-B with /worktree-A's path baked in, and
// runtime reads from /worktree-B/.../out would fail because
// build.rs only wrote to /worktree-A's OUT_DIR.
//
// The relocate e2e phase exercises this: same source built from a
// different absolute path with a shared cache. If kache (mistakenly)
// returns the cached binary, the runtime read fails and `verify`
// catches it via the missing stdout. The cache_key code MUST keep
// this entry's OUT_DIR path absolute (no normalization) so the keys
// diverge across paths and a fresh build runs at the relocated path.

const OUT_DIR: &str = env!("OUT_DIR");

fn main() {
    let path = format!("{OUT_DIR}/data.txt");
    let contents = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        // Surface the actual path in the error so the harness's
        // failure mode shows the leak — `expected 'hello from build.rs'
        // but read failed: <path A> (running at <path B>)`.
        panic!("read({path}) failed: {e}");
    });
    print!("{contents}");
}
