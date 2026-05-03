# Demo GIF

Self-contained, reproducible recording of the kache cold→warm flow.
Everything runs inside a Docker container — no host install needed beyond
Docker itself.

## Regenerate

From the repo root:

```sh
docker buildx build --output type=local,dest=./assets ./assets/demo
```

Produces `assets/demo.gif` and `assets/monitor.gif`.

## What the demo shows

Workload: a tiny synthetic Rust project ([fixtures/project-a/](fixtures/project-a/))
that depends on `serde`, `serde_json`, and `anyhow`. Sized to fit Docker
Desktop's default CPU/RAM allocation. The flow:

1. `kache stats` on an empty cache.
2. `cargo build` — cold compile. Every artifact is a miss; kache writes
   each one to its content-addressed store under `~/.cache/kache`.
3. `kache stats` — cache now holds the dep blobs.
4. `cargo clean && time cargo build` — `target/` is wiped, then the
   second build pulls every artifact back via hardlinks. The `time`
   output is the punchline: typically ~50% of cold time, with the savings
   coming from rustc work avoided rather than disk I/O.
5. `kache stats` — hit rate and miss-time saved climb visibly.
6. `kache monitor` Build tab — entries, hardlink count, live hit rate.

## Files

- `Dockerfile` — multi-stage. Stage 1 sets up Rust 1.95, kache v0.1.1
  from the musl release tarball, and the fixture workspace; then runs
  both vhs tapes in sequence. Stage 2 is `FROM scratch` with both GIFs,
  so `docker build --output` extracts them.
- `demo.tape` — main flow recording (cold→warm). 1000×640, FontSize 20.
- `monitor.tape` — `kache monitor` showcase against the populated cache
  left behind by `demo.tape`. Same 1000×640 / FontSize 20 so both GIFs
  render at a consistent scale on GitHub. Stays on the Build tab — the
  Store/Projects/Transfer tables are too column-dense to read clearly
  at this size; readers can run `kache monitor` locally to inspect them.
- `fixtures/project-a/` — minimal Rust binary depending on serde stack.

## Tuning the workload

To swap the demo crate, edit
[`fixtures/project-a/Cargo.toml`](fixtures/project-a/Cargo.toml). Larger
deps (e.g. `tokio` with `full` features, `aws-sdk-s3`, `ratatui`) make the
cold-build delta more dramatic but stretch the image build and the
recording, and may exceed Docker Desktop's default resource budget — vhs
runs Chrome headless internally and competes with cargo for CPU. Bump
Docker's allocated CPUs/RAM if you want to demo a heavier workload.
