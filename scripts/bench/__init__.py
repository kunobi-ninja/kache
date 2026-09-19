"""Benchmark orchestration and analysis over what `kache-scenario` measures.

The harness is the instrument: it runs a scenario's phases and writes the
results, reports and traces. This package drives it and reads what it wrote.
Nothing here measures a build on its own.

- `engine`: running the instrument, finding the tools it measures.
- `stats`: validity of one result, distributions, the paired verdict.
- `report`: the perf-gate comment rendered from one or more runs.
- `short`: the perf gate's measurement, repeated samples of every arm.
- `gate_local`: the same comparison run by hand against a merge base.
- `phases`: roll-up of a `KACHE_PHASE_TRACE_DIR`.

The scripts beside this directory are its entry points; the perf gate copies
them and this package together to measure with (see `gate_local`).
"""
