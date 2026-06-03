# Bench profiles

`kache-bench` measures kache against a real project by building it twice
against one shared cache — **cold** (empty cache, fresh worktree) then
**warm** (a second fresh worktree, cache populated) — and reporting the
hit rate, wall-clock delta, cross-clone cache-key stability, and a
pass/fail verdict.

Everything *project-specific* (which repo, how to inject kache, how to
build) lives in a **profile** in this directory; everything *measurement*
(the cold→warm lifecycle, report capture, key-divergence analysis, the
verdict) lives in the `kache-bench` engine and never changes per project.
This is the same split the e2e harness uses with `kache-fixture.toml`:
the harness owns the lifecycle, the file owns *what this project is*.

```sh
kache-bench --profile firefox          # bench-profiles/firefox.toml
kache-bench --profile substrate        # bench-profiles/substrate.toml
kache-bench --profile ./my.toml        # or an explicit path
kache-bench --profile firefox --ref FIREFOX_152_0_RELEASE   # override the pinned ref
```

(Via the Justfile: `just bench firefox`, `just bench substrate`.)

## How a run uses the profile

1. **Clone** `repo` at `ref` into a persistent reference checkout (a
   sibling of the work dir, so `rm -rf <work_dir>` doesn't wipe it).
2. Run the one-time `setup` once after a fresh clone (skipped when
   `setup_marker` exists, unless `--force-setup`).
3. Create two fresh git worktrees (`clone-a`, `clone-b`) off the
   reference, and apply every `[[file]]` injection to each.
4. **cold** phase: build in `clone-a` against an empty cache.
5. **warm** phase: build in `clone-b` against the now-populated cache.
6. Capture kache's JSON report + raw event log for each phase; compute
   the cross-clone key stability and the verdict.

For every build the engine injects this baseline env, so you never
declare it: `KACHE_CACHE_DIR`, `KACHE_CONFIG`, `RUSTC_WRAPPER={kache}`,
`KACHE_LOG`, `KACHE_LOG_FILE`. The `objdir` is wiped before each build so
every phase is genuinely from-scratch.

## Schema

| Field | Type | Required | Meaning |
|---|---|---|---|
| `name` | string | yes | Profile id; must match the file stem. |
| `repo` | string | yes | Git URL to clone. |
| `ref` | string | yes | Tag/branch/commit to pin. Override per-run with `--ref`. |
| `objdir` | string | yes | Build output dir, wiped before each phase (`obj-kache-bench` for Firefox, `target` for Cargo). |
| `requires` | string[] | no | Executables that must be on `PATH`; the run is **skipped** (not failed) if any is missing. |
| `setup` | string[] | no | One-time shell steps after a fresh clone (bootstrap, `rustup target add`), run via `sh -c` in the checkout. |
| `setup_marker` | string | no | Path whose existence means "setup done, skip it" (`~` expands to `$HOME`). `--force-setup` overrides. |
| `build` | string | yes | Build command, run via `sh -c` in the checkout with the kache env injected. Its wall-clock is the measured time. |
| `[env]` | table | no | Extra env vars for every build, on top of the baseline. |
| `[[file]]` | array | no | File injections — see below. |

### Interpolation

`{kache}` (absolute path to the kache binary under test) and `{objdir}`
are substituted in `env` values, `setup`, `build`, and `[[file]]`
content. (Mirrors the e2e fixture's `$KACHE`.)

## File injection: `[[file]]`

One uniform shape; `mode` selects how the `content` payload is applied
and defaults to `write`.

| `mode` | What it does | Use when the repo… | `content` is |
|---|---|---|---|
| `write` *(default)* | create / replace the file | does **not** own the file (Firefox ships no `mozconfig`) | the literal file body |
| `append` | add the payload to the end of the file | **owns** the file (its `.cargo/config.toml`) | the literal block to add |
| `patch` | `git apply` a unified diff | needs an existing source/build file *edited* | a unified diff |

```toml
[[file]]
path    = ".cargo/config.toml"
mode    = "append"             # write | append | patch  (default: write)
content = """

[build]
rustc-wrapper = "{kache}"
"""
# Or read the payload from a sibling file instead of inline `content`:
# content_file = "substrate-nodefault.patch"
```

### Choosing a mechanism

Prefer the least invasive option, in this order:

1. **`[env]`** (no `[[file]]` at all) — for Cargo projects `RUSTC_WRAPPER`
   from the baseline env is usually enough; add `CC`/`CXX` for C deps.
2. **`write`** — only for files you own. Overwriting a repo-owned file
   would clobber its settings.
3. **`append`** — extend a repo-owned config without clobbering it.
4. **`patch`** — escape hatch. A diff is tied to the target's exact
   lines/whitespace, so it **breaks when you bump `ref`**; re-generate it
   then. Use only for edits that can't be a `write`/`append`.

## Examples

`firefox.toml` writes a mozconfig it owns; `substrate.toml` needs no
file at all (env only). See those two files in this directory.

## Authoring a new profile

1. Drop `bench-profiles/<name>.toml` (`name` must match the stem).
2. Set `repo` + `ref`; list build-tool prereqs in `requires`.
3. Wire kache in with the least invasive mechanism (`env` → `append` →
   `write` → `patch`).
4. Set `build` and `objdir`; put one-time bootstrap in `setup` (+ a
   `setup_marker` if it's expensive to repeat).
5. Iterate fast with `kache-bench --profile <name> --skip-clone`.

## Gotchas

- **Don't overwrite repo-owned files** — `write` clobbers; use `append`.
- **`patch` is ref-fragile** — re-generate the diff when you bump `ref`.
- Injections are applied once per fresh worktree. `--skip-clone` reuses a
  worktree without re-cloning, so `write` overwrites cleanly but
  `append`/`patch` would double-apply — use `--skip-clone` only with
  `write`/`env` profiles, or do a full (re-cloning) run.
- A profile is **skipped** (not failed) when a `requires` tool is missing
  — the same falsifiability convention as e2e fixtures.
