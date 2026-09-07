# Running CI in another repository

Public repositories, including forks, run validation on GitHub-hosted Linux,
macOS, and Windows runners. No runner variables or publication credentials
are needed. Pull requests run the normal checks; pushes to `main` do too.

Private repositories use the same workflows with repository Actions variables.
Set these before enabling Actions. Each value is JSON: a quoted runner label
or an array of labels that all have to match.

| Variable | Example JSON value |
| --- | --- |
| `CI_RUNNER_LINUX` | `"your-linux-scale-set"` |
| `CI_RUNNER_MACOS` | `["self-hosted", "macOS", "ARM64", "ci"]` |
| `CI_RUNNER_WINDOWS` | `["self-hosted", "Windows", "X64", "ci"]` |

The examples are placeholders for labels provisioned in your organization.
ARC scale sets use their installation name as a single label. Missing or
invalid selectors fail workflow evaluation instead of selecting a paid
GitHub-hosted runner. Public validation ignores these overrides and keeps
the hosted defaults.

Private runners need the tools and permissions used by the selected jobs.
Linux checks include Docker Buildx, Nix, and a privileged loop-mounted btrfs
filesystem test. Platform jobs check their compiler tools before testing.
Use disposable runners for untrusted code and restrict runner-group access
to the intended repositories. Labels and workflow conditions do not replace
that access policy. Keep validation runners separate from signing privileges.

See GitHub's [runner selection](https://docs.github.com/en/actions/how-tos/write-workflows/choose-where-workflows-run/choose-the-runner-for-a-job)
and [runner-group access](https://docs.github.com/en/actions/how-tos/manage-runners/self-hosted-runners/manage-access)
documentation.

## Optional workloads

The official repository retains its existing benchmark and performance pools.
Other repositories opt in after provisioning suitable runners:

| Workload | Enable variable | Runner variables, using the JSON format above |
| --- | --- | --- |
| PR performance measurements | `ENABLE_PERF_GATE=true` | `PERF_RUNNER_LINUX` |
| Nightly/manual benchmarks | `ENABLE_BENCHMARKS=true` | `BENCH_RUNNER_LINUX`, `BENCH_RUNNER_LINUX_LARGE`, `BENCH_RUNNER_WINDOWS` |
| Scheduled fuzzing | `ENABLE_SCHEDULED_JOBS=true` | Uses `CI_RUNNER_LINUX` in private repositories |

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

## Checking workflow changes

The Repository consistency job evaluates runner and publication expressions
with GitHub's expression library across public, fork, and private repository
fixtures. Run the same checks locally:

```sh
cd .github/tests
npm ci --ignore-scripts --no-audit --no-fund
npm test
```
