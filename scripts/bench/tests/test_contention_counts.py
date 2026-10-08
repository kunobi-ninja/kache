"""Keep single cold contention samples from declaring a source regression."""

import argparse
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bench import report, short, stats


def records(base_counts, head_counts):
    rows = []
    for sample, (base, head) in enumerate(zip(base_counts, head_counts, strict=True)):
        for arm, count in (("base", base), ("head", head)):
            for phase in ("cold", "warm"):
                rows.append(
                    {
                        "sample": sample,
                        "arm": arm,
                        "phase": phase,
                        "wall_ms": 10000,
                        "events": {
                            "duplicate_key_compiles": count if phase == "cold" else 0,
                            "results": {"miss": int(phase == "cold")},
                        },
                    }
                )
    return rows


class ContentionCountTests(unittest.TestCase):
    def test_one_cold_pair_is_inconclusive_in_either_direction(self):
        for base, head in ((8, 16), (16, 8), (13, 16)):
            comparisons, failures = stats.contention_comparison(records([base], [head]))
            self.assertEqual(failures, [])
            self.assertEqual(
                comparisons[0]["duplicate_key_compiles"]["outcome"], "inconclusive"
            )

    def test_repeated_cold_growth_still_fails(self):
        _, failures = stats.contention_comparison(records([8] * 6, [16] * 6))
        self.assertIn("contention cold: repeated duplicate key compile growth", failures)

    def test_mixed_cold_changes_do_not_establish_growth(self):
        _, failures = stats.contention_comparison(
            records([8, 13, 16, 12, 15, 10], [16, 8, 13, 14, 12, 9])
        )
        self.assertEqual(failures, [])

    def test_warm_counts_still_fail_on_one_increase(self):
        for metric in ("duplicate_key_compiles", "miss", "passthrough"):
            rows = records([8], [16])
            event = rows[-1]["events"]
            if metric == "duplicate_key_compiles":
                event[metric] = 1
            else:
                event["results"][metric] = 1
            _, failures = stats.contention_comparison(rows)
            self.assertEqual(len(failures), 1)
            self.assertIn("contention warm", failures[0])

    def test_cold_timing_regressions_still_fail(self):
        rows = records([8] * 6, [8] * 6)
        for row in rows:
            if row["arm"] == "head" and row["phase"] == "cold":
                row["wall_ms"] = 12000
        _, failures = stats.contention_comparison(rows)
        self.assertEqual(failures, ["contention cold: paired timing regression"])

    def test_ties_do_not_count_as_directional_evidence(self):
        growth = stats.paired_count_growth([0] * 6, [0] * 6)
        self.assertEqual(growth["nonzero_pairs"], 0)
        self.assertEqual(growth["p_value"], 1)
        self.assertEqual(growth["outcome"], "inconclusive")
        growth = stats.paired_count_growth([0] * 6, [1] * 5 + [0])
        self.assertEqual(growth["nonzero_pairs"], 5)
        self.assertEqual(growth["outcome"], "inconclusive")

    def test_three_subjects_share_the_false_positive_budget(self):
        growth = stats.paired_count_growth([0] * 6, [40] * 6)
        self.assertAlmostEqual(growth["p_value"], 1 / 64)
        self.assertEqual(growth["outcome"], "regression")
        growth = stats.paired_count_growth([1] * 6, [2] * 5 + [0])
        self.assertEqual(growth["outcome"], "inconclusive")

    def test_report_shows_inconclusive_count_evidence(self):
        comparisons, failures = stats.contention_comparison(records([9], [17]))
        text = "\n".join(report.comparison_detail([{"name": "aube", "summary": {"comparisons": comparisons}}]))
        self.assertIn("1/1 non-tied pairs grew (1 measured), p=0.5000, inconclusive", text)
        self.assertEqual(failures, [])

    def test_headline_exposes_count_failure_with_inconclusive_timing(self):
        comparisons, failures = stats.contention_comparison(records([0] * 6, [40] * 6))
        project = {"name": "aube", "summary": {
            "comparisons": comparisons, "statistics": [],
            "contention": {"statistics": []},
        }}
        self.assertTrue(failures)
        self.assertIn("duplicate compiles regressed", "\n".join(report.head_vs_base([project])))

    def test_ci_sampling_rejects_sustained_cold_growth(self):
        # The workflow test supplies values from its real authorization and
        # measurement steps. Running this directly uses the ordinary CI shape.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args = argparse.Namespace(
                output=root,
                project="aube",
                scenarios=root,
                samples=6,
                order_seed=0,
                contention_samples=int(os.environ.get("PERF_TEST_CONTENTION_SAMPLES", 6)),
                contention_cold_every=int(os.environ.get("PERF_TEST_COLD_EVERY", 1)),
                context_samples=int(os.environ.get("PERF_TEST_CONTEXT_SAMPLES", 1)),
            )
            arms = [
                ("head", "kache", "/head"),
                ("base", "kache", "/base"),
                ("mbx", "mbx", "/mbx"),
            ]

            def measure(command, **kwargs):
                samples = int(command[command.index("--samples") + 1])
                cold_every = int(command[command.index("--cold-every") + 1])
                context = int(command[command.index("--context-samples") + 1])
                rows = records([0] * samples, [40] * samples)
                rows = [r for r in rows if r["phase"] == "warm" or r["sample"] % cold_every == 0]
                rows += [
                    {**row, "arm": "mbx"}
                    for row in list(rows) if row["arm"] == "base" and row["sample"] < context
                ]
                out = root / "contention"
                out.mkdir(exist_ok=True)
                (out / "samples.json").write_text(json.dumps({"records": rows}))
                (out / "summary.json").write_text("[]")

            with patch.object(short, "run_measurement", measure):
                result = short.run_contention(args, arms)
            self.assertIn("contention cold: repeated duplicate key compile growth", result["failures"])
            growth = result["comparisons"][0]["duplicate_key_compiles"]
            self.assertEqual(growth["n"], 6)
            self.assertEqual(growth["outcome"], "regression")

    def test_missing_contention_records_are_invalid(self):
        rows = records([0] * 6, [0] * 6)
        with self.assertRaisesRegex(ValueError, "incomplete"):
            stats.contention_comparison(rows[:-1])
