#!/usr/bin/env python3
"""Summarize Kache phase traces by exclusive elapsed time.

    scripts/trace-phases.py /tmp/tr --phase key
    scripts/trace-phases.py /tmp/run/kache.trace.json --phase restore

Accepts per-wrapper files or a combined Perfetto trace. Intervals nest only
within one process and thread. Summed elapsed time includes waits and overlaps
across threads; it is neither CPU time nor the build's critical path.
"""

import argparse
import collections
from dataclasses import dataclass, field
import json
import math
import statistics
import sys
from pathlib import Path


def load(path):
    """Skip unreadable or partly written files from builds still in flight."""
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
        and isinstance(event.get("name"), str)
        and isinstance(event.get("ts"), int)
        and isinstance(event.get("dur"), int)
        and event["dur"] >= 0
    ]


def lane(event):
    return event.get("pid", 0), event.get("tid", 0)


def encloses(outer, inner):
    return (
        outer is not inner
        and lane(outer) == lane(inner)
        and outer["ts"] <= inner["ts"]
        and outer["ts"] + outer["dur"] >= inner["ts"] + inner["dur"]
    )


def covered(ranges):
    """Length of a union, so overlapping children are subtracted once."""
    total = 0
    end = None
    for start, stop in sorted(ranges):
        total += max(0, stop - max(start, end if end is not None else start))
        end = max(stop, end if end is not None else stop)
    return total


def self_time(interval, siblings):
    return interval["dur"] - covered(
        (other["ts"], other["ts"] + other["dur"])
        for other in siblings
        if encloses(interval, other)
    )


@dataclass
class Summary:
    totals: collections.Counter = field(default_factory=collections.Counter)
    counts: collections.Counter = field(default_factory=collections.Counter)
    decisions: collections.Counter = field(default_factory=collections.Counter)
    samples: dict = field(default_factory=lambda: collections.defaultdict(list))
    units: dict = field(default_factory=lambda: collections.defaultdict(collections.Counter))
    unit_counts: dict = field(default_factory=lambda: collections.defaultdict(collections.Counter))
    roots: list = field(default_factory=list)
    traces: int = 0
    dropped: int = 0


def roll_up(spans, summary):
    """Build an interval tree per thread, naming each invocation's own unit.

    Equal-boundary phases arrive inner-first because Kache records on drop.
    Invocation roots are the exception: they carry dropped_events and arrive
    first. Keep roots first, then reverse ties to recover phase nesting.
    """
    lanes = collections.defaultdict(list)
    for span in spans:
        lanes[lane(span)].append(span)
    for same_thread in lanes.values():
        ordered = sorted(
            enumerate(same_thread),
            key=lambda item: (
                item[1]["ts"], -item[1]["dur"],
                0 if "dropped_events" in (item[1].get("args") or {}) else 1,
                -item[0],
            ),
        )
        stack = []
        nodes = []
        for _, span in ordered:
            while stack and not encloses(stack[-1][0], span):
                previous = stack.pop()[0]
                if previous["ts"] + previous["dur"] > span["ts"]:
                    raise ValueError(f"overlapping non-nested intervals in process/thread {lane(span)}")
            parent = stack[-1] if stack else None
            unit = parent[2] if parent else span["name"]
            node = (span, [], unit, parent is None)
            if parent:
                parent[1].append(span)
            else:
                summary.roots.append((span["ts"], span["ts"] + span["dur"]))
                summary.dropped += (span.get("args") or {}).get("dropped_events", 0)
            nodes.append(node)
            stack.append(node)
        for span, children, unit, root in nodes:
            own = self_time(span, children)
            name = "wrapper-unattributed" if root else span["name"]
            summary.totals[name] += own
            summary.counts[name] += 1
            summary.samples[name].append(own)
            summary.units[name][unit] += own
            summary.unit_counts[name][unit] += 1


def collect(source):
    summary = Summary()
    source = Path(source)
    paths = [source] if source.is_file() else sorted(source.rglob("*.json"))
    seen = collections.Counter()
    spans = []
    for path in paths:
        events = load(path)
        if events is None:
            continue
        summary.traces += 1
        occurrences = collections.Counter()
        fresh = []
        for event in events:
            if not isinstance(event, dict):
                continue
            if not all(field in event for field in ("pid", "tid", "ts")):
                fresh.append(event)
                continue
            key = json.dumps(event, sort_keys=True)
            occurrences[key] += 1
            # Raw files and their merged copy can coexist in a CI artifact.
            # Preserve repeated events within a file, count copies only once.
            if occurrences[key] > seen[key]:
                fresh.append(event)
        for key, count in occurrences.items():
            seen[key] = max(seen[key], count)
        spans.extend(intervals(fresh))
        for event in fresh:
            if event.get("ph") == "i":
                outcome = (event.get("args") or {}).get("outcome")
                summary.decisions[(event.get("name"), outcome)] += 1
    roll_up(spans, summary)
    return summary


def percentile95(samples):
    return sorted(samples)[math.ceil(len(samples) * 0.95) - 1]


def report(summary, top, phase):
    if not summary.roots:
        return ["No trace intervals found. Was KACHE_PHASE_TRACE_DIR set for the build?"]
    total = sum(summary.totals.values())
    window = max(end for _, end in summary.roots) - min(start for start, _ in summary.roots)
    lines = [
        f"{summary.traces} trace files, {len(summary.roots)} invocations",
        f"Observed trace window: {window / 1e6:.3f} s; summed thread elapsed: {total / 1e6:.3f} s",
        "Elapsed time includes waits and overlaps across threads; not CPU time or a build critical path.",
        "wrapper-unattributed is time inside the recorded invocation outside named phases.",
        "",
        f"{'phase':30s}{'n':>8s}{'self_s':>10s}{'p50_ms':>10s}{'p95_ms':>10s}{'max_ms':>10s}{'share':>8s}",
    ]
    if summary.dropped:
        lines.insert(1, f"WARNING: {summary.dropped} dropped events; phase attribution is incomplete.")
    for name, micros in summary.totals.most_common(top):
        samples = summary.samples[name]
        lines.append(
            f"{name:30s}{summary.counts[name]:8d}{micros / 1e6:10.2f}"
            f"{statistics.median(samples) / 1000:10.2f}"
            f"{percentile95(samples) / 1000:10.2f}{max(samples) / 1000:10.2f}"
            f"{micros / (total or 1) * 100:7.1f}%"
        )
    if summary.decisions:
        lines += ["", "decisions"]
        for (name, outcome), n in summary.decisions.most_common():
            lines.append(f"  {name}={outcome}  {n}")
    if phase:
        lines += ["", f"units by self time in {phase}", f"  {'unit':30s}{'n':>8s}{'self_s':>10s}"]
        if phase not in summary.units:
            lines.append("  none")
        for unit, micros in summary.units[phase].most_common(top):
            lines.append(f"  {unit:30s}{summary.unit_counts[phase][unit]:8d}{micros / 1e6:10.3f}")
    return lines


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="a trace directory or combined trace JSON")
    parser.add_argument("--top", type=int, default=18, help="maximum phases and units to print")
    parser.add_argument("--phase", help="also rank units by this phase's self time, e.g. key")
    args = parser.parse_args()
    if not args.source.exists():
        parser.error(f"not a file or directory: {args.source}")
    if args.top < 1:
        parser.error("--top must be positive")
    try:
        summary = collect(args.source)
    except ValueError as error:
        parser.error(str(error))
    print("\n".join(report(summary, args.top, args.phase)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
