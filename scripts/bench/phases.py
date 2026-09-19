#!/usr/bin/env python3
"""Roll up the phase traces Kache writes under KACHE_PHASE_TRACE_DIR.

Each wrapper invocation drops one Chrome-format trace. Opening them one at a
time says nothing; the question is always which phase costs the build, summed
over every process. That is self time: a phase's own duration minus the
phases nested inside it.

    KACHE_PHASE_TRACE_DIR=/tmp/tr cargo build
    scripts/trace-phases.py /tmp/tr

Self time is summed across processes, so it exceeds wall clock on a parallel
build. It answers "where does the work go", not "how long did this take".
"""

import argparse
import collections
import json
import sys
from pathlib import Path


def load(path):
    """Events from one trace file, or None if it is not one.

    A run that is still going leaves partly written files; they are skipped
    rather than failing the roll-up.
    """
    try:
        payload = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    events = payload.get("traceEvents") if isinstance(payload, dict) else payload
    return events if isinstance(events, list) else None


def intervals(events):
    return [
        event
        for event in events
        if isinstance(event, dict)
        and event.get("ph") == "X"
        and isinstance(event.get("ts"), int)
        and isinstance(event.get("dur"), int)
    ]


def encloses(outer, inner):
    return (
        outer is not inner
        and outer["ts"] <= inner["ts"]
        and outer["ts"] + outer["dur"] >= inner["ts"] + inner["dur"]
        and outer["dur"] > inner["dur"]
    )


def self_time(interval, siblings):
    """`interval`'s duration minus the time its immediate children hold.

    Only immediate children are subtracted. Counting every descendant would
    charge a grandchild twice and can drive a phase negative.
    """
    inside = [other for other in siblings if encloses(interval, other)]
    immediate = [
        child
        for child in inside
        if not any(encloses(other, child) for other in inside)
    ]
    return max(0, interval["dur"] - sum(child["dur"] for child in immediate))


def unit_of(spans):
    """The crate or translation unit this trace belongs to.

    The wrapper opens one interval named after the unit and everything else
    nests inside it, so the longest span carries the name.
    """
    return max(spans, key=lambda span: span["dur"])["name"] if spans else None


def collect(directory):
    totals = collections.Counter()
    counts = collections.Counter()
    decisions = collections.Counter()
    units = collections.defaultdict(collections.Counter)
    traces = 0
    for path in sorted(Path(directory).rglob("*")):
        if not path.is_file():
            continue
        events = load(path)
        if events is None:
            continue
        traces += 1
        spans = intervals(events)
        unit = unit_of(spans)
        for span in spans:
            totals[span["name"]] += self_time(span, spans)
            counts[span["name"]] += 1
            if unit is not None:
                units[span["name"]][unit] += 1
        for event in events:
            if isinstance(event, dict) and event.get("ph") == "i":
                outcome = (event.get("args") or {}).get("outcome")
                decisions[(event.get("name"), outcome)] += 1
    return totals, counts, decisions, units, traces


def report(totals, counts, decisions, units, traces, top, phase):
    if not traces:
        return ["No trace files found. Was KACHE_PHASE_TRACE_DIR set for the build?"]
    total = sum(totals.values()) or 1
    lines = [
        f"{traces} traces",
        "",
        f"{'phase':30s}{'n':>8s}{'self_s':>10s}{'per_ms':>10s}{'share':>8s}",
    ]
    for name, micros in totals.most_common(top):
        lines.append(
            f"{name:30s}{counts[name]:8d}{micros / 1e6:10.2f}"
            f"{micros / counts[name] / 1000:10.2f}{micros / total * 100:7.1f}%"
        )
    if decisions:
        lines += ["", "decisions"]
        for (name, outcome), n in decisions.most_common():
            lines.append(f"  {name}={outcome}  {n}")
    if phase:
        lines += ["", f"units that ran {phase}"]
        if phase not in units:
            lines.append("  none")
        for unit, n in units[phase].most_common():
            lines.append(f"  {unit:30s}{n:5d}")
    return lines


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="a KACHE_PHASE_TRACE_DIR")
    parser.add_argument("--top", type=int, default=18, help="phases to print")
    parser.add_argument(
        "--phase",
        help="also list the units that ran this phase, e.g. dep-info",
    )
    args = parser.parse_args()
    if not args.directory.is_dir():
        parser.error(f"not a directory: {args.directory}")
    print("\n".join(report(*collect(args.directory), args.top, args.phase)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
