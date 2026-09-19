#!/usr/bin/env python3
"""The roll-up charges each phase its own time and nothing else."""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

from bench import phases as trace


def span(name, ts, dur):
    return {"ph": "X", "name": name, "ts": ts, "dur": dur, "cat": "rustc", "pid": 1, "tid": 0}


def write(directory, name, events):
    (directory / name).write_text(json.dumps({"traceEvents": events}))


class TraceTests(unittest.TestCase):
    def test_a_nested_phase_is_not_charged_to_its_parent_twice(self):
        """The number that matters is self time.

        `compiler` enclosing `store` enclosing `fsync` must read 1000, 500 and
        200, not 1700 spread over three rows. Subtracting every descendant
        rather than the immediate children is the easy way to get this wrong,
        and it drives the outermost phase negative.
        """
        spans = [
            span("compiler", 0, 1700),
            span("store", 100, 700),
            span("fsync", 200, 200),
        ]
        self.assertEqual(trace.self_time(spans[0], spans), 1000)
        self.assertEqual(trace.self_time(spans[1], spans), 500)
        self.assertEqual(trace.self_time(spans[2], spans), 200)

    def test_the_longest_span_names_the_unit(self):
        spans = [span("libc", 0, 900), span("store", 10, 40)]
        self.assertEqual(trace.unit_of(spans), "libc")
        self.assertIsNone(trace.unit_of([]))

    def test_it_totals_across_traces_and_names_the_units_of_a_phase(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(
                root,
                "a.json",
                [
                    span("libc", 0, 1000),
                    span("dep-info", 100, 300),
                    {"ph": "i", "name": "prediction", "args": {"outcome": "no-record"}},
                ],
            )
            write(
                root,
                "b.json",
                [
                    span("rustix", 0, 1000),
                    span("dep-info", 100, 500),
                    {"ph": "i", "name": "prediction", "args": {"outcome": "no-record"}},
                ],
            )
            # A half-written file from a run still in flight is skipped, not fatal.
            (root / "partial.json").write_text('{"traceEvents": [')

            totals, counts, decisions, units, traces = trace.collect(root)

            self.assertEqual(traces, 2)
            self.assertEqual(counts["dep-info"], 2)
            self.assertEqual(totals["dep-info"], 800)
            self.assertEqual(decisions[("prediction", "no-record")], 2)
            self.assertEqual(
                sorted(units["dep-info"]), ["libc", "rustix"]
            )

            lines = trace.report(totals, counts, decisions, units, traces, 18, "dep-info")
            text = "\n".join(lines)
            self.assertIn("dep-info", text)
            self.assertIn("prediction=no-record  2", text)
            self.assertIn("libc", text)

    def test_an_empty_directory_says_so_instead_of_printing_an_empty_table(self):
        with tempfile.TemporaryDirectory() as tmp:
            lines = trace.report(*trace.collect(Path(tmp)), 18, None)
            self.assertIn("KACHE_PHASE_TRACE_DIR", "\n".join(lines))


if __name__ == "__main__":
    unittest.main()
