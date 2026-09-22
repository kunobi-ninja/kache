# Cold CI prefetch qualification

This experiment compares automatic prefetch on and off for eza v0.23.5
(`98442ab17c2c3738701b62a7e060b1431ae2d6ea`) with Rust 1.90.0 on the existing
Linux benchmark runners. The backend is an isolated filesystem remote. These
results do not model R2 latency.

A protected `main` push changing the qualification files creates an immutable
seed containing the Kache binary and remote packs/indexes. Six fresh consumer
jobs run in order: off-1, on-1, on-2, off-2, off-3, on-3. Each verifies the seed,
uses empty cache/runtime/target/Cargo directories, preserves the real CI
environment and sets the remote read-only. There is no explicit warming step.
The only arm setting that changes is `prefetch_enabled`.

Merge the #618 demand, lifecycle, packed and ordinary-receipt prerequisites
before triggering complete precision qualification. The producer must run on
a protected branch under Kache's existing write policy. No production
credentials, planner endpoint or storage are involved. A manual run on `main`
must name an existing seed run. Authorization checks its repository, workflow,
push event, main ancestry and successful producer job. Every arm uses the seed's
recorded binary revision; regenerate the seed to qualify a newer binary.

After the workflow and prerequisites merge, its protected-main push seeds and
runs all controls. To repeat an existing seed:

```sh
gh workflow run prefetch-qualification.yml --ref main -f seed_run_id=RUN_ID
```

The final collector reads GitHub job start/end timestamps after all consumers
finish. Durations include checkout, tools, artifact staging, fetch, build,
drain/export and artifact upload. Phase logs give narrower timings; none
replaces total job time. Producer time is reported separately. The first step
records wall time before checkout or tool installation. Runner resources and
compiler versions are retained. Unrelated jobs in the shared pool remain a
source of noise. Three pairs are a qualification, not a statistical benchmark.

Every executable must list a fixed fixture correctly and report the pinned eza
version. The collector also requires identical executable SHA256 and version
output across all six arms. The pinned input, fixed build timestamp, path
remapping and matched compilers are intended to produce identical artifacts.
A hash mismatch fails admission and requires investigation; semantic output
alone does not override it.

Schemas 3 and 4 retain the tested ordinary-transfer report. Schema 4 adds the
lifecycle incomplete flag, reviewed against
`0ac603f944ba546dfc78cb457462537c99a35739`. Schema 5 adds physical operation
receipts and nested payload entries, reviewed against
`129a31ce4debb8c97d9698fe5ff6d48ed4387750`. Schema 6 adds the wrapper-demand
join from #1160 and #1162, reviewed against `src/timeline.rs`. Schema 7 marks
each unit with the session's earliest delivery of its key: plan, candidate
rank, GET start, import time and timing against first demand. Unknown future
schemas or fields fail admission. Every keyed compiler unit must carry exact
first-demand observations.

Demand and usefulness come from unit events and transfer receipts. The daemon's
`used_keys` can omit local hits, so it cannot establish byte precision.

Schema 6 carries the daemon's own join under `daemon_join`: consumed and useful
prefetch keys and bytes, summed remote wait, and GET 404 and error counts. The
harness reports these beside its own derivation rather than asserting the two
agree, because they do not share a base. The join counts the per-key payload it
credited; the harness denominator is received GET-body bytes, which also carry
catalog metadata and pack headers. The join credits a delivery that finished on
the same millisecond as first demand, where the harness calls that ordering
unknown. On identical input the harness therefore credits no more keys than the
join. What the harness does enforce is that useful never exceeds consumed, in
keys or bytes, and that every join counter is a non-negative integer.

Schema 7 adds `in_flight_prefetch_keys` and `in_flight_prefetch_bytes`, consumed
deliveries whose GET was still running at first demand, and `get_cancelled`.
Useful and in-flight deliveries are disjoint, so together they cannot exceed
consumption. `unit_prefetch_outcomes` counts unit result against timing: a
`local_hit/before_demand` is a prefetched local hit, and `in_flight` means the
demand waited on the rest of a running GET instead of starting its own.

The schema-5 report groups receipts by immutable session, plan ID and source.
An empty fallback plan ID remains scoped to its session. Its denominator is
received GET-body bytes, including catalog metadata and pack headers. Nested
entry bytes describe payload attribution and are never added to that physical
total. Fully received bodies remain in the denominator after failed validation
or import. Useful payload requires successful cache consumption in the same
session and import completion before the earliest demand. A key receives useful
credit at most once across plans. Equal-millisecond ordering is unknown and
prevents an exact point ratio for the affected plan.

LIST result counts and backend invocation counts are reported separately.
Unknown LIST response sizes do not invalidate GET-body precision. Background
key-cache LISTs may appear in either arm and keep their original source.
On/off admission tests candidate GET work, not the mere presence of a LIST.
Counts describe backend invocations, excluding SDK retries and LIST pages.
Observed blocking milliseconds are reported per demanded key and arm; they
are not estimates of time saved.

Raw transfer logs are reconciled against timeline projections as multisets.
Identical physical operations both count. Duplicate timeline snapshots are
rejected. Unprojected receipts remain visible under their original plan or
unscoped source. Unknown partial GET bytes, missing accounting, unreconciled
GET receipts or missing drained shutdown evidence deny complete precision.
Inactivity and supersession summaries are snapshots, not shutdown barriers.

After drain, the harness preserves lifecycle summaries and any log-rotation
markers. Any marker for events, transfers or summaries fails admission, even
when the current log is small. Raw events, transfers, summaries, dry-run
timelines and verified identities use `prefetch-qualification-*` artifacts;
these names do not enter the `telemetry-otlp-v1*` ingestion path.

`controls_valid` records successful controls and artifact checks.
`complete_precision_qualification` additionally requires complete GET-body
evidence from schema 5 or later across all arms. Legacy reports cannot set that
flag. Review both flags, complete job pairs and raw demand evidence before
accepting #618.
This experiment cannot establish R2 performance or planner recommendation
quality, and no whole-job speedup is claimed before the actual jobs finish.
