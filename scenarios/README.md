# Scenarios

Every runnable e2e or benchmark case lives under one scenario directory:

```text
scenarios/
  e2e-c-hello/
    scenario.toml
    source/

  bench-firefox/
    scenario.toml
    patches/
```

The scenario name must match its directory name. Use prefixes for readability
(`e2e-*`, `bench-*`) and tags for automation (`suite:e2e`, `suite:bench`,
`tier:gate`, `tier:nightly`, `lang:rust`, `project:firefox`).

## Selection

Selectors are ANDed.

```sh
kache-scenario --select suite:e2e --select tier:gate
kache-scenario --select suite:e2e --select lang:rust --select tier:gate
kache-scenario --list --select suite:bench --select backend:kache
kache-scenario --select suite:bench --select backend:kache --profile firefox
kache-scenario --cache-backend sccache --select suite:bench --select backend:sccache --profile firefox
kache-scenario --select suite:bench --select project:firefox
```

`--profile firefox` is shorthand for `--select name:firefox`; it does not add
`suite:bench`. Automation should pass suite tags explicitly.

## Fixture Scenario

Fixture scenarios use checked-in source under `source/`.

```toml
name = "e2e-c-hello"
tags = ["suite:e2e", "lang:cc"]

[source]
kind = "fixture"
path = "source"

[env]
CC = "$KACHE cc"

[commands]
build = "make"
clean = "make clean"

[checks.measure.warm]
min_hit_rate_pct = 90.0
```

Existing fixture `[assertions.<phase>]` tables remain the blocking e2e
correctness contract. `[checks.measure.<phase>]` is advisory only and never
fails the run.

## Clone Benchmark Scenario

Clone scenarios describe an external repository and optional file injections.

```toml
name = "bench-firefox"
tags = ["suite:bench", "project:firefox", "backend:kache", "lang:cc", "lang:cpp", "lang:rust"]

setup = ["./mach bootstrap --application-choice browser"]
setup_marker = "~/.mozbuild"
build = "./mach build"

[source]
kind = "clone"
repo = "https://github.com/mozilla-firefox/firefox.git"
ref = "FIREFOX_151_0_RELEASE"
objdir = "obj-kache-bench"

[[file]]
path = "mozconfig"
content = "mk_add_options \"export RUSTC_WRAPPER={cache}\"\n"

[checks.assert.warm]
min_key_stability_pct = 50.0
max_passthrough_pct = 40.0
max_errors = 0
```

`checks.assert` drives the bench verdict and exit code; omitted assertion
fields are not evaluated. `checks.measure` warnings are advisory only.
`{cache}` expands to the selected compiler-cache binary (`kache` by default,
`sccache` with `--cache-backend sccache`); `{kache}` remains supported for
older scenarios.

Every benchmark keeps root-level `report-*`, `build-*`, `wrapper-*`, and result
JSON files as the latest run for `--retry`, and also archives those artifacts to
`runs/<YYYYMMDDTHHMMSSZ>-<backend>-<pid>/` so repeated runs are preserved.
Kache benchmark runs also write `trace-cold.json` and `trace-warm.json` in
Perfetto/Chrome trace format; sccache benchmark runs write
`report-*.sccache.json` plus `report-*.sccache-adv.txt` from sccache's own
stats commands. `--trace-keys` is kache-only and adds `key-diff.{json,md}` for
cache-key divergence analysis.

Backend-specific benchmark scenarios should keep their source patches under the
scenario's own `patches/` directory so kache and sccache requirements remain
auditable if they diverge.

## File Injection

Clone scenarios can modify a fresh checkout before each build:

| `mode` | Behavior |
|---|---|
| `write` | create or replace a file; default |
| `append` | append to an existing repo-owned file |
| `patch` | apply a unified diff with `git apply` |

Use `content_file` for larger payloads, relative to the scenario directory:

```toml
[[file]]
path = "gfx/wr/webrender/build.rs"
mode = "patch"
content_file = "patches/firefox-generated-source-relative.patch"
```

Prefer `[env]` first, then `write`, then `append`, then `patch`. Patches are
ref-fragile and usually need regeneration when `source.ref` changes.
