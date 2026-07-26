# Prefetch budgets and cost-aware ranking (#616, #617)

Design note for the work that closes #616 (plans have no budget) and #617 (the plan path
dropped cost weighting). Cross-model reviewed; where the two reviews disagreed with each
other or with the first draft, the disagreement is recorded rather than averaged away.

## Why this is staged

The two issues share a `PrefetchCandidate` wire change, which is why they were scoped
together. Working the design through showed the combined change spans the planner, the
`PlannerDataSource` trait, the shard object format, the store schema query, the daemon's
enforcement path, the remote backend's read path, config, and telemetry. Landing that as one
change would be hard to review and hard to revert.

It splits cleanly because **enforcement does not need the metadata**, and that is also the
half that carries the safety risk:

1. **Daemon hard budgets** (this stage). Keys, bytes, deadline, enforced daemon-side on the
   existing request shape. Bounds the pathology today. No wire change.
2. **Bound the low-confidence source.** Per-crate and plan-wide caps on key-cache expansion,
   plus a per-crate cap on local history.
3. **Candidate metadata on the wire.** `compile_time_ms`, size, source, demand index, all
   `Option`, plus shard writers persisting what `ManifestEntry` already carries.
4. **Cost-aware ranking** using that metadata, behind a versioned policy.

## Decisions that hold across the stages

### Unknown must neither win nor lose

Every metadata field is `Option`. Zero is not unknown: the store's `size` and
`compile_time_ms` columns default to 0 for rows predating their migrations, so a raw 0 has to
map to `None` or an un-backfilled entry reads as "free to fetch and worthless to have".

Unknown-cost candidates are not scored against known-cost ones and are not assigned a
fabricated average. They get a bounded lane of their own so they are neither dominant nor
invisible. Unknown-size candidates are charged a configured nominal estimate against the byte
budget, so they cannot be free.

### The daemon is the only trust boundary

The planner may be a remote HTTP service and is an untrusted boundary distinct from S3. So:

- The planner MAY apply its own limits. That is an efficiency measure (smaller responses,
  smaller DB result sets, less cloning), **not** a security boundary.
- The daemon applies its own configured limits unconditionally, and a request may only
  *lower* them, never raise them.
- Candidate metadata coming from a planner is advisory. Byte accounting uses bytes actually
  read from the remote, never a claimed `artifact_size`.
- `warm_all` is subject to the same budgets. It is a request field, so it cannot be an escape
  hatch from them.

### Estimates rank; measurements enforce

`artifact_size` (and the store's `size`) are admission and ranking inputs. They are not a
promise about compressed transfer bytes. Anything enforced has to be counted on the wire.

A **hard** byte cap needs a counted, cancellable read path, and concurrent tasks must reserve
against the budget atomically or N workers each observe the same remaining balance. Until
that exists, this is a **soft** cap: the coordinator stops launching new downloads once the
budget is exhausted, and overshoot is bounded by what was already in flight. That bound must
be stated wherever the limit is documented, rather than described as a hard cap.

### Ranking: three signals that disagree

Confidence (which source), value (avoided compile time per byte), and urgency (roughly when
the build will ask). Prefetch is a race against the build, so a high-value artifact needed at
minute eight is less urgent than a medium-value one needed at second five.

Admission and dispatch are separate concerns and should not share one comparator:

- **Admission** decides what is worth spending a finite budget on: confidence-weighted value
  density.
- **Dispatch** decides what starts first: coarse urgency windows, value descending inside a
  window. Coarse because guppy position is a weak proxy for demand time, so treating position
  40 versus 45 as meaningful is false precision while 40 versus 400 is real.

The weights are guardrail constants, not measured probabilities, and they should be versioned
as one policy rather than exposed as a dozen environment variables.

**What would settle them:** cold-CI traces carrying candidate source, demand time, actual
compressed bytes, and whether the candidate was demanded at all. #618 has to land first,
because today's telemetry cannot say whether a candidate arrived before it was needed. Until
then, offline instrumented benchmarks beat pretending the weights are learned.

### The key cache needs dimensioning, not just capping

It maps a crate name to every cache key in the bucket, with no target, toolchain, profile, or
feature filtering, and it is built from object names so it has neither size, cost, nor
timestamps. A crate name is not a build identity.

Capping it is the available move; it is not a fix. Selection among variants uses a
deterministic hash over the intent fingerprint plus crate name plus cache key rather than
lexicographic first-N, because cache keys carry no recency or compatibility ordering and
lexicographic choice would bake in a persistent arbitrary bias. Sampling does not make a
variant likelier to match; it only avoids that bias.

Not done here, and deliberately: per-key HEAD or GET to rank variants (that reinstates the
per-object latency prefetch exists to remove), and extending `RemoteBackend::list` to return
sizes (touches every backend for the benefit of the source being capped).

Recording variant population per crate is cheap and useful: choosing 2 of 3 variants is a
very different bet from choosing 2 of 100.

### Truncated must not look like exhausted

The failure this has to avoid is a plan cut short by a budget being indistinguishable from a
plan that had nothing more to offer.

Plan statistics are optional on the wire, and **absent means "completeness unknown", not
"complete"** — an old planner makes no claim either way. Truncation is a list of reasons, not
a boolean, so "hit the key cap" and "hit the byte cap" stay distinguishable.

Planner-reported counts and daemon-observed counts are recorded separately and labelled as
such: the former is advice, the latter is fact.

Nothing here may be labelled a prefetch hit rate or an on-time arrival. That is exactly what
current telemetry cannot measure (#618). These numbers show whether the budgets behaved, not
whether prefetch helped.

## Recorded disagreements

- **The cheap-crate filter.** The older manifest path drops candidates below
  `KACHE_MIN_COMPILE_MS` (default 1000ms). The first draft argued for removing it, on the
  grounds that with a real budget cheap crates simply lose, and a hard filter throws away
  value when budget is spare. Both independent reviews argued for keeping it for known-cost
  candidates while exempting unknown-cost ones into their own lane. Keeping it: it preserves
  existing configurable behavior, and the exemption is what stops it becoming an
  unknown-swallows-everything rule.
- **Scoring arithmetic.** Integer cross-multiplication versus float scores. Integer, for
  determinism across platforms; a plan should not depend on floating-point rounding.
- **Budget defaults.** The two reviews proposed 60s and 90s deadlines and 4 MiB versus 16 MiB
  unknown-size charges. These are guardrails against pathology rather than tuned optima, and
  the note should say so wherever they are documented.

## Not in scope for any stage here

- Encoding target, toolchain, profile, or features into the remote key layout. That is the
  real key-cache fix and needs its own index version and migration.
- Feedback-driven or learned weighting (needs #618).
- Changing adaptive cancellation (#620-adjacent).
- Replacing the older monolithic manifest path, though it should inherit daemon enforcement.
- Bundling or multi-object GETs.
