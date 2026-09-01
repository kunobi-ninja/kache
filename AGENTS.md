# Agent notes

`just check` is not the merge gate. CI mutation-tests changed Rust lines in the kache binary. A PR that only ran `cargo test` on the new tests will go red on Mutation testing. That is the usual failure.

## Before `gh pr create`

```sh
just pr
```

That is `just check` (fmt, clippy `-D warnings`, tests) plus `just mutants-diff` against `origin/main`. Docs-only diffs skip mutants. The PR gate reuses the baseline that `just check` just established; standalone `just mutants-diff` still runs its own baseline.

```sh
cargo install --locked cargo-mutants --version 27.1.0
```

Do not invoke `cargo mutants` with `RUSTC_WRAPPER=kache`. Every Just mutation recipe clears it, including when `KACHE_SELF_HOST=1`. Hand-running mutants without `just` wraps kache in itself and the unmutated baseline dies.

On macOS, the mutation recipes default to one cargo-mutants job and two Rust test threads. Set `CARGO_MUTANTS_JOBS` or `RUST_TEST_THREADS` explicitly when the machine can handle more.

Do not open the PR until `just pr` exits 0.

## How mutants die here

Prefer tests and deleting dead branches over skip annotations.

- `if !s.is_empty() { print!("{s}") }` — printing `""` is a no-op, so `delete !` is missed. Call a helper that already has empty vs nonempty tests (`replay_cached_diagnostics`).
- `a && b` / `a || b` — one test where the left is false, one where the right is false.
- Named size constants built with `*` (`5 * 1024 * 1024 * 1024`) — do not `assert_eq!` against that same constant. Use an independent literal (`5 << 30`).
- `u64::try_from` on a value that is already `u64` — Linux clippy `-D warnings`. macOS clippy does not type-check `#[cfg(target_os = "linux")]`.
- Unix-only test helpers under `-D warnings` on Windows are unused. Mark them `#[cfg(unix)]`.
- macOS-only helpers Linux never calls: `#[cfg(any(test, target_os = "macos"))]`.
- `Drop` / file locks — a second process must acquire after the first drops.
- New event-log fields — grep `event.schema` in the same commit and bump the tests.

## PRs

Every PR targets `main`. Do not stack PRs on each other's branches. After one merges, rebase the others onto `main`.

Work in a git worktree. Do not mix unrelated dirty-tree WIP into the PR.

People keep running `cargo`. There is no `kache build`. Caching is `kache init` / `RUSTC_WRAPPER`.
