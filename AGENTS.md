# Agent notes

`just check` is not the merge gate. CI mutation-tests changed Rust lines in the kache binary. A PR that only ran `cargo test` on the new tests will go red on Mutation testing. That is the usual failure.

## Before `gh pr create`

```sh
just check
```

That is fmt, clippy `-D warnings`, and the workspace tests. Do not open the PR until it exits 0.

Mutants run in CI, not on your machine. Open the PR as a draft; the `Mutation testing (diff k/n)` shards test every changed Rust line in parallel on Linux runners and finish in well under an hour. A red shard prints the missed mutants in its log and uploads them as `missed.txt` in its `mutation-diff-report-<k>` artifact; add the test that kills each one, push, and mark the PR ready when every shard is green. A laptop running the same diff at one to three jobs takes hours and the load makes the fixture-based tests flake.

`just pr` still exists for a Linux box with cores to spare: it is `just check` plus `just mutants-diff` against `origin/main`, the same scope CI runs. If you use it:

```sh
cargo install --locked cargo-mutants --version 27.1.0
```

Do not invoke `cargo mutants` with `RUSTC_WRAPPER=kache`. The Justfile clears it. Hand-running mutants without `just` wraps kache in itself and the unmutated baseline dies.

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
