# extra_inputs: workspace-root declarations and transitive re-keying

Design notes for the "central table" successor to the co-located `extra_inputs`
feature (issue #220, Phase 1). Surfaced by lovesegfault's review of kache in
[rio-build#51](https://github.com/lovesegfault/rio-build/pull/51#issuecomment-4674696268),
which carries a `rio-buildhash` crate as a workaround "until the central table".

Status: **proposal, not implemented.** This captures the shape so it can be
reviewed before any code lands.

## Background

`extra_inputs` lets a crate declare files the compiler reads at build time but
never reports (sqlx's `.sqlx/`, `migrations/`, `include!`'d data), so editing one
re-keys the crate instead of serving a stale hit. Today it is **co-located**: a
`kache.toml` next to the crate's `Cargo.toml`, folded into *that crate's* key
only ([`src/extra_inputs.rs`](../../src/extra_inputs.rs)).

Phase 1 was deliberately scoped to co-located, per-crate declarations to avoid a
"central table hazard" (one declaration silently affecting other crates). This
doc is that deferred Phase 2.

## Problems with co-located-only

Three gaps, all from the review:

1. **Shared workspace-root inputs break cross-worktree sharing.** A `.sqlx/` at
   the workspace root is read by several member crates. Each would need a
   co-located `kache.toml` with `../.sqlx/**`. An out-of-crate pattern
   (absolute or `..`) is *folded*, but its path is folded host-/layout-specific
   ([`normalize_pattern`](../../src/extra_inputs.rs), the "reaches outside the
   crate" branch), so the key stops sharing across machines and worktrees —
   which is the entire reason to run kache. The reviewer needs:
   workspace-relative fold paths.

2. **No dependent re-keying.** Dependents fold each `--extern` dep as
   `extern:<name>=<blake3(rlib bytes)>` ([`src/cache_key.rs`](../../src/cache_key.rs)).
   An `extra_inputs` change moves crate A's *own* key, but the change is
   metadata-only — A's `.rlib` bytes don't change — so a dependent of A hashes
   the same rlib and **replays against stale metadata**. There is no transitive
   propagation. (When the input flows through a build script's
   `rerun-if-changed`, the rlib *does* change and dependents re-key transitively;
   the gap is specifically the macro/compile-time-read class `extra_inputs`
   targets.)

3. **Warm-target evaluation timing.** `extra_inputs` is only evaluated when
   cargo invokes rustc; editing a tracked file in a warm in-place target won't
   re-key without a matching `rerun-if-changed`. Partially mitigated by the
   `kache doctor` warning (#359). A workspace-level declaration doesn't fix this
   by itself but is where any future "force re-eval" hint would live.

## Proposed shape

A **workspace-root declaration** with three properties the reviewer named:
workspace-root location, workspace-relative fold paths, and per-crate opt-in.

### Declaration

A `[workspace.extra_inputs]`-style table in the workspace-root config (the
existing project `.kache.toml`, which already sits at/above the workspace), e.g.:

```toml
# <workspace-root>/.kache.toml
[[workspace.extra_inputs]]
# Which member crates this rule applies to (opt-in; package-name globs).
crates = ["rio-store", "rio-api", "rio-worker"]
# Patterns resolved relative to the WORKSPACE ROOT, folded workspace-relative.
inputs = [".sqlx/**/*.json"]
# Whether a change also re-keys dependents of the listed crates (see below).
propagate_to_dependents = true
```

- **Workspace-relative paths.** Both the glob anchor and the folded path are
  workspace-root-relative (`/`-normalized), so a worktree move or cross-machine
  restore doesn't change the key — fixing problem (1) without the host-specific
  absolute-path fold.
- **Per-crate opt-in.** `crates` scopes the rule explicitly; a crate not listed
  is untouched. This is what keeps it from being the "central table hazard"
  Phase 1 avoided — nothing applies implicitly.

### Transitive re-keying (problem 2)

The crux. A crate's `extra_inputs` digest must reach its dependents' keys even
though the dependency's `.rlib` bytes are unchanged. Options, roughly in
increasing precision and cost:

- **A — workspace-wide fold (coarse).** Fold the entire workspace
  `extra_inputs` digest into *every* in-workspace crate's key. Trivial and
  fail-safe, but any tracked-file change re-keys the whole workspace (over-
  invalidation). Acceptable for small workspaces; wasteful for large ones.

- **B — central digest table (precise).** Build a table `crate -> extra_inputs
  digest` for the workspace, then when folding each `--extern` dep also fold
  *that dep's* `extra_inputs` digest looked up from the table. A change to A's
  inputs then moves A's key *and* every dependent that folds A. This is the
  "central table" and gives precise transitive re-keying. Cost: the table must
  be available at key-computation time in the rustc-wrapper hot path.

- **C — rlib-identity fold.** Make a crate's `extra_inputs` digest part of the
  identity dependents already hash, by folding it alongside
  `extern:<name>=<blake3(rlib)>`. Equivalent to B in effect; same table
  requirement.

B/C are the target. The open problem is **where the table comes from in the hot
path** — the wrapper sees one rustc invocation at a time and does not run
`cargo metadata`. Candidates: (a) parse the workspace `.kache.toml` + the
`--extern` names already on the command line (we know the dep names; we'd map
each to its declared digest); (b) have the daemon compute and cache the table
once per workspace and serve it; (c) a build-session artifact written by the
first invocation. (a) is the most self-contained — the digest is derived purely
from the declaration + on-disk files, and `--extern` already enumerates the
deps to fold.

## Safety and compatibility

- **Union-only, fail-safe.** Like Phase 1, a misdeclared rule can only add
  inputs → an extra miss, never a wrong artifact.
- **Opt-in, no global invalidation.** Folded only when the workspace declaration
  exists and only for listed crates; crates/workspaces without it stay
  byte-identical, so **no `CACHE_KEY_VERSION` bump** is required.
- **Coexists with co-located Phase 1.** The co-located `kache.toml` stays; the
  workspace table is additive. A crate may be covered by both (both digests
  fold).

## Open questions

- Table source in the hot path (A/B/C above) — the main unknown.
- Scoping syntax: package-name globs vs. path globs vs. both.
- Performance: re-globbing workspace-root trees on every compile; reuse the
  memoized `FileHasher` and consider a daemon-side cache.
- Interaction with the warm-target limitation (problem 3): is a "force
  re-evaluation" signal in scope, or left to `rerun-if-changed` + the doctor
  warning?
- Migration/comms: this is the change that lets the `rio-buildhash` workaround
  be retired; coordinate with that consumer.

## References

- Phase 1 implementation: [`src/extra_inputs.rs`](../../src/extra_inputs.rs)
- Extern dep keying: [`src/cache_key.rs`](../../src/cache_key.rs)
- Doctor warm-target warning: `audit_rerun_coverage` in
  [`src/extra_inputs.rs`](../../src/extra_inputs.rs) (#359)
- Review thread: https://github.com/lovesegfault/rio-build/pull/51
