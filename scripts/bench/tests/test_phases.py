#!/usr/bin/env python3
"""The roll-up charges each phase its own time and nothing else."""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from bench import phases as trace


def span(name, ts, dur, pid=1, tid=0):
    return {"ph": "X", "name": name, "ts": ts, "dur": dur, "cat": "rustc", "pid": pid, "tid": tid}


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
                    span("rustix", 0, 1000, pid=2),
                    span("dep-info", 100, 500, pid=2),
                    {"ph": "i", "name": "prediction", "args": {"outcome": "no-record"}},
                ],
            )
            # A half-written file from a run still in flight is skipped, not fatal.
            (root / "partial.json").write_text('{"traceEvents": [')

            summary = trace.collect(root)

            self.assertEqual(summary.traces, 2)
            self.assertEqual(summary.counts["dep-info"], 2)
            self.assertEqual(summary.totals["dep-info"], 800)
            self.assertEqual(summary.decisions[("prediction", "no-record")], 2)
            self.assertEqual(
                sorted(summary.units["dep-info"]), ["libc", "rustix"]
            )

            lines = trace.report(summary, 18, "dep-info")
            text = "\n".join(lines)
            self.assertIn("dep-info", text)
            self.assertIn("prediction=no-record  2", text)
            self.assertIn("libc", text)

    def test_an_empty_directory_says_so_instead_of_printing_an_empty_table(self):
        with tempfile.TemporaryDirectory() as tmp:
            lines = trace.report(trace.collect(Path(tmp)), 18, None)
            self.assertIn("KACHE_PHASE_TRACE_DIR", "\n".join(lines))

    def test_other_processes_and_threads_do_not_subtract_time(self):
        outer = span("compiler", 0, 1700)
        siblings = [outer, span("store", 100, 700, pid=2), span("store", 200, 300, tid=1)]
        self.assertEqual(trace.self_time(outer, siblings), 1700)

    def test_overlapping_descendants_are_subtracted_once(self):
        outer = span("compiler", 0, 1000)
        siblings = [outer, span("a", 100, 500), span("b", 200, 100), span("c", 400, 400)]
        self.assertEqual(trace.self_time(outer, siblings), 300)

    def test_a_merged_trace_keeps_each_threads_unit_and_elapsed_time(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root, "merged.json", [
                span("first", 0, 1000), span("key", 100, 400),
                span("second", 100, 800, pid=2), span("key", 150, 700, pid=2),
                span("worker", 200, 600, tid=1), span("key", 250, 300, tid=1),
                span("later", 2000, 1000), span("key", 2100, 500),
            ])
            summary = trace.collect(root / "merged.json")
            self.assertEqual(len(summary.roots), 4)
            self.assertEqual(summary.totals["key"], 1900)
            self.assertEqual(summary.totals["wrapper-unattributed"], 1500)
            self.assertEqual(summary.units["key"], {"first": 400, "second": 700, "worker": 300, "later": 500})
            text = "\n".join(trace.report(summary, 18, "key"))
            self.assertIn("summed thread elapsed: 0.003 s", text)
            self.assertIn("not CPU time", text)

    def test_equal_boundaries_do_not_double_count_nested_phases(self):
        invocation = span("subject", 0, 1000)
        invocation["args"] = {"dropped_events": 0}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            # Root first, then drop order: child before parent.
            write(root, "equal.json", [invocation, span("lookup", 0, 1000), span("key", 0, 1000)])
            summary = trace.collect(root)
            self.assertEqual(summary.totals["lookup"], 1000)
            self.assertEqual(summary.totals["key"], 0)
            self.assertEqual(summary.totals["wrapper-unattributed"], 0)
            self.assertEqual(sum(summary.totals.values()), 1000)

    def test_crossing_intervals_fail_instead_of_inventing_self_time(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root, "crossing.json", [span("unit", 0, 1000), span("a", 100, 500), span("b", 400, 400)])
            with self.assertRaisesRegex(ValueError, "overlapping non-nested"):
                trace.collect(root)

    def test_a_merged_copy_does_not_double_count_the_raw_files(self):
        a = [span("a", 0, 1000), span("key", 100, 300)]
        b = [span("b", 0, 1000, pid=2), span("key", 100, 500, pid=2)]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root, "a.json", a)
            write(root, "b.json", b)
            write(root, "merged.json", a + b)
            summary = trace.collect(root)
            self.assertEqual(len(summary.roots), 2)
            self.assertEqual(summary.counts["key"], 2)
            self.assertEqual(sum(summary.totals.values()), 2000)

    def test_repeated_decisions_in_a_file_survive_deduplication(self):
        decision = {"ph": "i", "name": "prediction", "pid": 1, "tid": 0, "ts": 10, "args": {"outcome": "used"}}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            events = [span("a", 0, 1000), decision, decision]
            write(root, "raw.json", events)
            write(root, "merged.json", events)
            self.assertEqual(trace.collect(root).decisions[("prediction", "used")], 2)

    def test_percentiles_and_unit_ranking_expose_slow_invocations(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            events = []
            for index in range(20):
                duration = 100_000 if index == 19 else 1000
                unit = "slow" if index == 19 else "fast"
                events.extend([span(unit, index * 200_000, duration + 100), span("key", index * 200_000, duration)])
            write(root, "traces.json", events)
            summary = trace.collect(root)
            self.assertEqual(trace.percentile95(summary.samples["key"]), 1000)
            self.assertEqual(max(summary.samples["key"]), 100_000)
            text = "\n".join(trace.report(summary, 18, "key"))
            self.assertIn("p50_ms", text)
            self.assertIn("p95_ms", text)
            units = text.split("units by self time in key")[1]
            self.assertLess(units.index("slow"), units.index("fast"))

    def test_dropped_events_are_reported_as_incomplete_attribution(self):
        invocation = span("subject", 0, 1000)
        invocation["args"] = {"dropped_events": 7}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root, "traces.json", [invocation])
            text = "\n".join(trace.report(trace.collect(root), 18, "missing"))
            self.assertIn("7 dropped events", text)
            self.assertIn("incomplete", text)
            self.assertIn("none", text)

    def test_cli_accepts_a_combined_trace_and_rejects_nonpositive_top(self):
        script = Path(__file__).resolve().parents[2] / "trace-phases.py"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root, "traces.json", [span("subject", 0, 1000)])
            result = subprocess.run([sys.executable, str(script), str(root / "traces.json")], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("1 invocations", result.stdout)
            result = subprocess.run([sys.executable, str(script), str(root), "--top", "0"], capture_output=True, text=True)
            self.assertEqual(result.returncode, 2)
            self.assertIn("--top must be positive", result.stderr)


if __name__ == "__main__":
    unittest.main()
