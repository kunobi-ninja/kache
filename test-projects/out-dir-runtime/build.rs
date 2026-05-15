// Writes a small data file into $OUT_DIR. The fixture's main.rs reads
// this file at runtime via the absolute path embedded by `env!()`.
//
// Deterministic content — file bytes are identical regardless of
// where the build runs. That isolates the bug surface to the embedded
// PATH (different across worktrees) rather than the file CONTENT
// (same across worktrees). Without that isolation, a relocate failure
// could be either the path or the content; with it, the only thing
// the relocate test exposes is the path-leak-via-env! pattern.

use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let out_dir: PathBuf = env::var_os("OUT_DIR")
        .expect("OUT_DIR is always set by cargo")
        .into();
    fs::write(out_dir.join("data.txt"), b"hello from build.rs\n")
        .expect("writing $OUT_DIR/data.txt must succeed");
    println!("cargo:rerun-if-changed=build.rs");
}
