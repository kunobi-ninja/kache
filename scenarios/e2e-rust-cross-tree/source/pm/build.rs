// A deliberately trivial build script. Its presence is the point: cargo
// compiles it (a cacheable unit kache restores and stamps) and then RUNS
// it, writing the run `output` file AFTER the restore. That write is the
// comparison site for cargo's StaleDependency freshness rule — the #677
// clock inversion happened exactly between a restored script binary's
// stamp and this file's write timestamp.
fn main() {}
