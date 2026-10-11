"""Keep single cold contention samples from declaring a source regression."""

import argparse
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bench import design, report, short, stats


def records(base_counts, head_counts, schedule=None):
    rows = []
    for sample, (base, head) in enumerate(zip(base_counts, head_counts, strict=True)):
        for arm, count in (("base", base), ("head", head)):
            for phase in ("cold", "warm"):
                rows.append(
                    {
                        "sample": sample,
                        "arm": arm,
                        "phase": phase,
                        "cold_seed": sample,
                        "position": schedule["blocks"][sample]["order"].index(arm) if schedule else (0 if arm == "base" else 1),
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
        schedule = design.plan(12, 1, 0, 3)
        _, failures = stats.contention_comparison(records([8] * 12, [16] * 12, schedule), schedule)
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
        schedule = design.plan(12, 1, 0, 3)
        rows = records([8] * 12, [8] * 12, schedule)
        for row in rows:
            if row["arm"] == "head" and row["phase"] == "cold":
                row["wall_ms"] = 12000
        _, failures = stats.contention_comparison(rows, schedule)
        self.assertEqual(failures, ["contention cold: paired timing regression"])

    def test_ties_do_not_count_as_directional_evidence(self):
        growth = stats.paired_count_growth([0] * 6, [0] * 6)
        self.assertEqual(growth["nonzero_pairs"], 0)
        self.assertIsNone(growth["p_value"])
        self.assertEqual(growth["outcome"], "inconclusive")
        growth = stats.paired_count_growth([0] * 6, [1] * 5 + [0])
        self.assertEqual(growth["nonzero_pairs"], 5)
        self.assertEqual(growth["outcome"], "inconclusive")

    def test_predeclared_family_adjusts_count_probabilities(self):
        schedule = design.plan(12, 1, 0, 18)
        growth = stats.paired_count_growth([0] * 12, [40] * 12, schedule)
        self.assertAlmostEqual(growth["p_value"], 2 / 924)
        self.assertAlmostEqual(growth["p_adjusted"], 36 / 924)
        self.assertEqual(growth["outcome"], "regression")
        six = stats.paired_count_growth([0] * 6, [40] * 6, design.plan(6, 1, 0, 18))
        self.assertEqual(six["outcome"], "inconclusive")

    def test_report_shows_inconclusive_count_evidence(self):
        comparisons, failures = stats.contention_comparison(records([9], [17]))
        text = "\n".join(report.comparison_detail([{"name": "aube", "summary": {"comparisons": comparisons}}]))
        self.assertIn("1/1 non-tied pairs grew (1 measured), descriptive, inconclusive", text)
        self.assertEqual(failures, [])

    def test_headline_exposes_count_failure_with_inconclusive_timing(self):
        schedule = design.plan(12, 1, 0, 3)
        comparisons, failures = stats.contention_comparison(records([0] * 12, [40] * 12, schedule), schedule)
        project = {"name": "aube", "summary": {
            "comparisons": comparisons, "statistics": [],
            "contention": {"statistics": []},
        }}
        self.assertTrue(failures)
        self.assertIn("duplicate compiles regressed", "\n".join(report.head_vs_base([project])))

    def test_ci_sampling_keeps_low_resolution_evidence_inconclusive(self):
        # The workflow test supplies values from its real authorization and
        # measurement steps. Running this directly uses the ordinary CI shape.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args = argparse.Namespace(
                output=root,
                project="aube",
                scenarios=root,
                samples=6,
                order_seed=int(os.environ.get("PERF_TEST_ORDER_SEED", 314159)),
                comparison_family_size=int(os.environ.get("PERF_TEST_FAMILY_SIZE", 18)),
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
                order_seed = int(command[command.index("--order-seed") + 1])
                family_size = int(command[command.index("--comparison-family-size") + 1])
                self.assertEqual(order_seed, args.order_seed + 1)
                self.assertEqual(family_size, args.comparison_family_size)
                schedule = design.plan(samples, cold_every, order_seed, family_size)
                rows = records([0] * samples, [40] * samples, schedule)
                rows = [r for r in rows if r["phase"] == "warm" or r["sample"] % cold_every == 0]
                rows += [
                    {**row, "arm": "mbx"}
                    for row in list(rows) if row["arm"] == "base" and row["sample"] < context
                ]
                out = root / "contention"
                out.mkdir(exist_ok=True)
                (out / "samples.json").write_text(json.dumps({"records": rows, "randomization": schedule}))
                (out / "summary.json").write_text("[]")

            with patch.object(short, "run_measurement", measure):
                result = short.run_contention(args, arms)
            self.assertEqual(result["failures"], [])
            growth = result["comparisons"][0]["duplicate_key_compiles"]
            self.assertEqual(growth["n"], 6)
            self.assertEqual(growth["blocks"], 6)
            self.assertEqual(growth["assignments"], 20)
            self.assertEqual(growth["family_size"], 18)
            self.assertAlmostEqual(growth["p_value"], 0.1)
            self.assertEqual(growth["p_adjusted"], 1.0)
            self.assertEqual(growth["outcome"], "inconclusive")

    def test_missing_contention_records_are_invalid(self):
        rows = records([0] * 6, [0] * 6)
        with self.assertRaisesRegex(ValueError, "incomplete"):
            stats.contention_comparison(rows[:-1])
