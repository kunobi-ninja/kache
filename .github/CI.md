# Running CI in another repository

Public repositories, including forks, run validation on GitHub-hosted Linux,
macOS, and Windows runners. No runner variables or publication credentials
are needed. Pull requests run the normal checks; pushes to `main` and `dev` do too.

## Optional workloads

The official repository retains its existing benchmark and performance pools.
Other repositories opt in after provisioning suitable runners:

| Workload | Enable variable | Runner variables (JSON selectors) |
| --- | --- | --- |
| PR performance measurements | `ENABLE_PERF_GATE=true` | `PERF_RUNNER_LINUX` |
| Nightly/manual benchmarks | `ENABLE_BENCHMARKS=true` | `BENCH_RUNNER_LINUX`, `BENCH_RUNNER_LINUX_LARGE`, `BENCH_RUNNER_WINDOWS` |
| Scheduled fuzzing | `ENABLE_SCHEDULED_JOBS=true` | `CI_RUNNER_LINUX` |

Only benchmark classes selected by the scenario need a matching runner.
Large scenarios require up to 120 GB free disk; ordinary hosted runners are
not substitutes for those pools. A missing large-runner selector never falls
back to the smaller pool. Retain the same hardware for paired performance
measurements and size concurrent jobs for the available capacity.

The performance workflow retains its default-branch authorization and
refuses measurements of code from fork pull requests. Its check is optional.
Fuzz compilation still runs on matching pull requests, and fuzzing can be
dispatched manually without enabling the nightly schedule.

## Publication

Release signing, the stable branch, crates.io, package repositories, Helm
charts, and service image publication run only in `kunobi-ninja/kache`.
Copies build the service image without publishing it. Their normal CI and
packaging validation do not require upstream release credentials.

To publish a separately maintained distribution, configure its destinations
and credentials and review the publication guards for that repository.

## Windows release cache

Windows release jobs restore a pinned Microsoft CRT and SDK prepared on
`main` by `warm-windows-sysroot.yml`. The warmer runs when its workflow changes,
weekly, or by manual dispatch. It runs only in `kunobi-ninja/kache`.

The shared action checks the SDK layout and file digests before reuse. A
cache miss downloads and prepares the SDK again. Release tags only restore;
the default-branch warmer saves the cache so later tags can read it.

## Which jobs a pull request runs

`Detect changes` classifies the files a pull request touches with
`scripts/ci-changes.py` and turns on only the job groups that cover them.
Pushes to `main`, `dev` and tags always run everything.

| Files | Jobs |
| --- | --- |
| Markdown, `docs/`, `notes/`, `assets/`, `LICENSE`, issue templates | none |
| `scripts/bench-short.py` and its test, `perf-gate-report.py`, `install-bench-mbx.sh`, the bench and perf-gate workflows | Check (Linux) |
| `scenarios/` | Check (Linux), E2E smoke on every platform |
| `packaging/`, `flake.nix`, `flake.lock`, apt/AUR/release scripts, publication workflows | Check (Linux), Nix package |
| anything else, including `ci.yml` and the classifier itself | every job |

A mixed pull request runs the union. Required checks that a group skips
still count as passed. Run the mapping's tests with
`python3 scripts/test-ci-changes.py`.

## Checking workflow changes

The Repository consistency job evaluates runner and publication expressions
with GitHub's expression library across public, fork, and private repository
fixtures. Run the same checks locally:

```sh
cd .github/tests
npm ci --ignore-scripts --no-audit --no-fund
npm test
```

## Nix cache

The Nix package jobs restore the store with
[nix-community/cache-nix-action](https://github.com/nix-community/cache-nix-action).
It restores the Nix database and store together. No cache account or secret is
needed. Pull requests only restore; successful pushes to `main` save the store.
GitHub's branch scopes let PRs read the default branch's cache without letting
PRs replace entries that `main` can restore.

Keys separate operating systems, architectures, and the locked Nix, Rust, and
Cargo dependencies. Source-only changes reuse the same immutable entry. They
rebuild changed project outputs without uploading another full store; this
limits competition with the other CI caches. Dependency changes start a fresh
entry. Both flake evaluation and all native flake checks still run.

The action logs the selected key, cache hit or miss, and restore/save sizes.
Each successful job also reports the uncompressed Nix store size.
Check those logs before attributing a faster run to caching. GitHub's cache
quota and eviction policy apply; an evicted entry causes a normal cold build.

Store GC is left disabled because `nix flake check` does not create permanent
roots for its outputs. Collecting them before saving would lose those builds.
