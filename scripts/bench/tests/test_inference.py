"""Check randomization design, finite-sample resolution and inference boundaries."""

import copy
import math
import unittest

from bench import design, inference, stats
from bench.tests.test_short import record


def observations(schedule, ratios):
    rows = []
    for sample, ratio in enumerate(ratios):
        order = schedule["blocks"][sample // schedule["cold_every"]]["order"]
        for arm in order:
            row = record(arm, sample, 10000 if arm == "base" else 10000 * ratio)
            row["cold_reused"] = sample % schedule["cold_every"] != 0
            row["position"] = order.index(arm)
            rows.append(row)
    return rows


class RandomizationTests(unittest.TestCase):
    def test_balanced_and_independent_are_distinct_designs(self):
        balanced = design.plan(12, 1, 19, 3)
        self.assertEqual(sum(b["order"][0] == "base" for b in balanced["blocks"]), 6)
        independent = design.plan(11, 1, 19, 3)
        self.assertEqual(independent["kind"], "independent")
        self.assertEqual(inference.analyze([0.1] * 11, independent)["assignments"], 2048)
        self.assertEqual(inference.analyze([0.1] * 12, balanced)["assignments"], 924)

    def test_small_consistent_slowdown_has_no_practical_waiver(self):
        schedule = design.plan(12, 1, 0, 18)
        summary = stats.summarize(observations(schedule, [1.001] * 12), schedule)
        self.assertEqual(len(summary["failures"]), 3)
        change = summary["comparisons"][0]
        self.assertEqual(change["outcome"], "regression")
        self.assertAlmostEqual(change["geometric_pct"], 0.1)
        self.assertAlmostEqual(change["p_adjusted"], 36 / 924)

    def test_no_change_is_not_a_claim_of_parity(self):
        schedule = design.plan(12, 1, 0, 3)
        for change in stats.summarize(observations(schedule, [1] * 12), schedule)["comparisons"]:
            self.assertEqual(change["p_value"], 1)
            self.assertEqual(change["outcome"], "inconclusive")

    def test_warm_repeats_use_two_seed_blocks_not_six_samples(self):
        schedule = design.plan(6, 3, 0, 3)
        summary = stats.summarize(observations(schedule, [2] * 6), schedule)
        self.assertEqual([c["n"] for c in summary["comparisons"]], [2, 6, 6])
        for change in summary["comparisons"]:
            self.assertEqual(change["blocks"], 2)
            self.assertEqual(change["interval_pct"], [None, None])
            self.assertEqual(change["outcome"], "inconclusive")

    def test_a_single_outlier_does_not_become_repeated_evidence(self):
        schedule = design.plan(12, 1, 0, 3)
        change = stats.summarize(observations(schedule, [1] * 11 + [3]), schedule)["comparisons"][0]
        self.assertGreater(change["geometric_pct"], 0)
        self.assertEqual(change["outcome"], "inconclusive")
        self.assertEqual(change["n"], 12)

    def test_second_position_penalty_is_not_a_tool_effect(self):
        schedule = design.plan(12, 1, 0, 3)
        ratios = [1.2 if b["order"][0] == "base" else 1 / 1.2 for b in schedule["blocks"]]
        change = stats.summarize(observations(schedule, ratios), schedule)["comparisons"][0]
        self.assertAlmostEqual(change["geometric_pct"], 0)
        self.assertEqual(change["outcome"], "inconclusive")

    def test_family_is_shared_across_phases_before_observing_data(self):
        schedule = design.plan(12, 1, 0, 30)
        summary = stats.summarize(observations(schedule, [1.2] * 12), schedule)
        for change in summary["comparisons"]:
            self.assertAlmostEqual(change["p_adjusted"], 60 / 924)
            self.assertEqual(change["outcome"], "inconclusive")
            self.assertEqual(change["interval_pct"], [None, None])

    def test_incomplete_duplicate_bad_order_and_bad_plan_fail_closed(self):
        schedule = design.plan(12, 1, 0, 3)
        rows = observations(schedule, [1.2] * 12)
        with self.assertRaisesRegex(ValueError, "incomplete"):
            stats.summarize(rows[:-2], schedule)
        with self.assertRaisesRegex(ValueError, "duplicate"):
            stats.summarize(rows + [rows[0]], schedule)
        wrong = copy.deepcopy(rows)
        wrong[0]["position"] = 1 - wrong[0]["position"]
        with self.assertRaisesRegex(ValueError, "observed order"):
            stats.summarize(wrong, schedule)
        bad_plan = copy.deepcopy(schedule)
        bad_plan["kind"] = "independent"
        with self.assertRaisesRegex(ValueError, "declared design"):
            stats.summarize(rows, bad_plan)
        partial = stats.summarize(rows[:-2], schedule, allow_incomplete=True)
        self.assertTrue(all(c["p_value"] is None for c in partial["comparisons"]))

    def test_invalid_times_and_asymmetric_seeds_fail_closed(self):
        schedule = design.plan(12, 1, 0, 3)
        rows = observations(schedule, [1.2] * 12)
        for bad in (0, -1, float("nan"), float("inf"), True):
            broken = copy.deepcopy(rows)
            broken[0]["result"]["cold"]["wall_ms"] = bad
            with self.assertRaises(ValueError):
                stats.summarize(broken, schedule)
        rows[0]["cold_reused"] = True
        with self.assertRaisesRegex(ValueError, "different cold seeds"):
            stats.summarize(rows, schedule)

    def test_context_tools_keep_their_slots_after_the_pair(self):
        schedule = design.plan(12, 1, 0, 3)
        arms = [("head",), ("context",), ("base",)]
        for sample in range(12):
            order = [arm[0] for arm in design.ordered(sample, arms, schedule)]
            self.assertEqual(order[:2], schedule["blocks"][sample]["order"])
            self.assertEqual(order[2:], ["context"])

    def test_interval_sweep_matches_direct_test_at_boundaries(self):
        schedule = design.plan(8, 1, 3, 3)
        effects = [math.log(x) for x in (1.1, 1.2, 0.9, 1.3, 1.4, 1.05, 0.95, 1.25)]
        orders = [1 if b["order"][0] == "base" else -1 for b in schedule["blocks"]]
        coeffs = inference.coefficients(effects, orders, "balanced")
        observed = sum(effects) / 8
        bounds = inference.interval(coeffs, observed, 0.05)
        self.assertTrue(all(bound is not None for bound in bounds))
        for theta in (bounds[0] - 0.001, bounds[0] + 0.001,
                      bounds[1] - 0.001, bounds[1] + 0.001):
            p = sum(abs(a - b * theta) >= abs(observed - theta) for a, b in coeffs) / len(coeffs)
            self.assertEqual(p > 0.05, bounds[0] < theta < bounds[1])


if __name__ == "__main__":
    unittest.main()
