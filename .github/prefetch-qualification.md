# Cold CI prefetch qualification

This experiment compares automatic prefetch on and off for eza v0.23.5
(`98442ab17c2c3738701b62a7e060b1431ae2d6ea`) with Rust 1.90.0 on the existing
Linux benchmark runners. The backend is an isolated filesystem remote. It
qualifies correctness and demand timeliness; it does not model R2 latency.

A protected `main` push changing the qualification files creates one immutable
seed containing the Kache binary and remote packs/indexes. Six fresh consumer
jobs run in order: off-1, on-1, on-2, off-2, off-3, on-3. Each verifies the seed,
uses empty cache/runtime/target/Cargo directories, preserves the real CI
environment and sets the remote read-only. There is no explicit warming step.
The only arm setting that changes is `prefetch_enabled`.

The producer needs the demand telemetry changes from #618 merged first. Its
workflow must run on a protected branch, as required by Kache's existing write
policy. No production credentials, planner endpoint or storage are involved.
A manual run on `main` must name an existing seed run; authorization checks its
repository, workflow, push event, main ancestry and successful producer job.
The seed's recorded binary revision is used by every arm, even when current
main has advanced. Regenerate the seed to qualify a newer binary.

After the workflow and prerequisites merge, its protected-main push seeds and
runs all controls. To repeat an existing seed:

```sh
gh workflow run prefetch-qualification.yml --ref main -f seed_run_id=RUN_ID
```

The final collector reads GitHub job start/end timestamps after all consumers
finish. Those durations include checkout, tools, artifact staging, fetch,
build, drain/export and artifact upload. Phase logs give narrower timings;
none replaces total job time. Producer time is reported separately. The first
step records wall time before checkout or tool installation. Runner resources
and compiler versions are retained; concurrent unrelated pool jobs remain a
source of noise. Three pairs are a qualification, not a statistical benchmark.

Each artifact must list a deterministic fixture correctly and report the
pinned eza version. Every arm needs schema-3 timelines with schema-20 demand
records on every keyed unit and remote restores. On arms must exercise speculative transfers;
off arms must have none. The report joins first demands by immutable session and key, including
prefetched local hits. Useful credit requires consumption by a successful hit,
and a key receives credit at most once. The denominator includes recorded
compressed bytes from failed imports, duplicate and unscoped transfers. Completion before demand, equal timestamps, in-flight,
late, unused and unknown-demand bytes are separate. Blocking milliseconds are observed wait,
not estimated savings. Fallback plans may legitimately have empty plan IDs.
Raw events, transfers, summaries, dry-run timelines and immutable seed identity
are retained under `prefetch-qualification-*`; these names do not enter the
existing `telemetry-otlp-v1*` ingestion path.

The schema-3 report covers ordinary logged transfers. Packed operations and
partial physical transfer bytes may be missing, so `controls_valid` does not
claim complete precision qualification. New timeline schemas or transfer fields
fail admission until their adapter and physical-byte accounting are reviewed.

Review complete job pairs and raw demand evidence before accepting #618's
real-CI control. A failed admission check is an inconclusive experiment, not
proof of a performance regression. No whole-job speedup is claimed until the
six actual jobs finish. This filesystem experiment cannot qualify R2 latency,
planner recommendation quality, or a broader population of projects.
