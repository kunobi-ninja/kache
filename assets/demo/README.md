# Recordings

The GIFs and WebM files in `assets/` are rendered from the tapes in this
directory with [VHS](https://github.com/charmbracelet/vhs). The README embeds
the GIFs; the docs pages embed the WebM files.

## Regenerate

VHS needs `ttyd` and `ffmpeg` on `PATH`. VHS 0.12.0 exits without writing any
output ([charmbracelet/vhs#787](https://github.com/charmbracelet/vhs/issues/787));
use 0.11.0.

```sh
cargo build --release
assets/demo/prepare.sh target/release/kache
cd assets/demo
vhs init.tape && vhs demo.tape && vhs why-miss.tape && vhs monitor.tape && vhs clean.tape
```

`env.sh` picks the root (`/Users/Shared/kache-demo` on macOS, where `kache clean`
skips `/private`; `/tmp/kache-demo` elsewhere) and points Kache at a scratch
store and configuration there, so the recordings never touch your own cache.
Every tape sources it off screen. `prepare.sh` builds a small crate with a
committed lockfile under that root. The
tapes build on each other's state: run them in the order above, and re-run
`prepare.sh` to start over.

## Scenes

| Tape | Shows |
| --- | --- |
| `init.tape` | `kache init --check`, then `kache init` answering its prompts, then `kache doctor`. HOME and CARGO_HOME point into the demo root, so nothing on the recording machine changes. |
| `demo.tape` | The crate is built cold off screen. On screen: the same commit in a second worktree with an empty target directory, every crate a hit, then `kache report --last-build`. |
| `why-miss.tape` | One source edit, one recompile, and `kache why-miss` naming the key that changed. |
| `monitor.tape` | `kache monitor` following a build in a third worktree, then the Why, Projects, and Store tabs. |
| `clean.tape` | `kache clean` listing the target directories under the tree and how much of each is already in the store. |
