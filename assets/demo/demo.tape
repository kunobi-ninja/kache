# kache demo — recorded inside the Docker image.
# Workload: a tiny synthetic Rust project at /workspace/project-a (see
# fixtures/). Output goes to /workspace/project-a/demo.gif which the
# multi-stage Dockerfile copies to a scratch image so
# `docker build --output` extracts it.

Output demo.gif

Set FontSize 20
Set Width 1000
Set Height 640
Set Theme "Catppuccin Mocha"
Set Padding 16
Set TypingSpeed 30ms

# Banner
Type "# kache: zero-copy Rust build cache via RUSTC_WRAPPER"
Sleep 1500ms
Ctrl+U

# 1. Empty cache
Type "kache stats"
Sleep 300ms
Enter
Sleep 2500ms

# 2. Cold build — first time, everything compiles
Type "cargo build"
Sleep 300ms
Enter
Sleep 4s

# 3. Stats after cold — entries cached, all misses
Type "kache stats"
Sleep 300ms
Enter
Sleep 4s

# 4. Wipe target/ and rebuild — kache restores via hardlinks. Watch the time.
Type "cargo clean && time cargo build"
Sleep 300ms
Enter
Sleep 5s

# 5. Stats: hit rate jumps, miss-time saved is the headline.
#    This is the punchline — end here. The README's Screenshots section
#    has full ASCII renderings of `kache monitor` and `kache clean`.
Type "kache stats"
Sleep 300ms
Enter
Sleep 5s
