#!/usr/bin/env python3
"""Exercise validity, paired regressions, independent cold counts and run isolation."""

import argparse
import copy
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from bench import engine, report, short, stats


def result(backend="kache", ms=10000):
    phase = {
        "wall_ms": ms,
        "hits": 10,
        "cache_hits": 10,
        "misses": 0,
        "storage": {"restored_bytes": 1000},
        "event_log": {"passed_through": 0},
    }
    return {
        "git_ref": "subject-sha",
        "cache_tool_version": backend + " 1.0",
        "verdict": {"ok": True},
        "warm_same_tree_verdict": {"ok": True},
        **{p: copy.deepcopy(phase) for p in stats.PHASES},
    }


def record(arm, sample, ms=10000):
    return {
        "arm": arm,
        "sample": sample,
        "cold_reused": sample % 3 != 0,
        "result": result(ms=ms),
    }


class BenchTests(unittest.TestCase):
    def test_process_exit_and_timeout_cleanup(self):
        engine.run_measurement([sys.executable, "-c", "pass"])
        with self.assertRaises(subprocess.CalledProcessError):
            engine.run_measurement([sys.executable, "-c", "raise SystemExit(4)"])
        process = MagicMock()
        process.pid = 1234
        process.wait.side_effect = [subprocess.TimeoutExpired("engine", 1200), 0]
        with (
            patch.object(engine.subprocess, "Popen") as spawn,
            patch.object(engine.os, "killpg") as kill,
        ):
            spawn.return_value.__enter__.return_value = process
            with self.assertRaises(subprocess.TimeoutExpired):
                engine.run_measurement(["engine"])
            kill.assert_called_once_with(1234, engine.signal.SIGKILL)

    def test_refuses_invalid_measurements(self):
        for backend in ("kache", "sccache", "mbx"):
            stats.validate(result(backend), backend)
            for phase in stats.PHASES:
                for value in (None, 0, -1, float("nan"), True):
                    r = result(backend)
                    r[phase]["wall_ms"] = value
                    with self.assertRaises(ValueError):
                        stats.validate(r, backend)
            for phase in stats.PHASES[1:]:
                r = result(backend)
                r[phase]["hits"] = r[phase]["cache_hits"] = 0
                with self.assertRaises(ValueError):
                    stats.validate(r, backend)
        r = result()
        r["warm_same_tree_verdict"]["ok"] = False
        with self.assertRaises(ValueError):
            stats.validate(r, "kache")
        r = result()
        r["warm"]["invalid_reasons"] = ["store error"]
        with self.assertRaises(ValueError):
            stats.validate(r, "kache")

    def test_cold_reuse_does_not_inflate_samples(self):
        summary = stats.summarize([record("kache", i) for i in range(6)])
        self.assertEqual([s["n"] for s in summary["statistics"]], [2, 6, 6])

    def test_paired_regression_and_noise(self):
        self.assertEqual(
            stats.paired_change([10000] * 6, [12000] * 6)["outcome"], "regression"
        )
        self.assertEqual(
            stats.paired_change([10000] * 6, [8000] * 6)["outcome"], "improvement"
        )
        self.assertEqual(
            stats.paired_change([10000] * 6, [9000, 12000] * 3)["outcome"],
            "inconclusive",
        )
        self.assertEqual(
            stats.paired_change([10000], [15000])["outcome"], "inconclusive"
        )
        self.assertEqual(
            stats.paired_change([1000] * 6, [1100] * 6)["outcome"], "inconclusive"
        )

    def test_pair_and_identity_validation(self):
        with self.assertRaises(ValueError):
            stats.summarize([record("base", 0)])
        records = [record("base", 0), record("head", 0)]
        records[1]["result"]["git_ref"] = "different"
        with self.assertRaises(ValueError):
            stats.summarize(records)
        records = [record("kache", 0), record("kache", 1)]
        records[1]["result"]["cache_tool_version"] = "changed"
        with self.assertRaises(ValueError):
            stats.summarize(records)

    def test_count_regression_cannot_hide_in_faster_timing(self):
        records = [record("base", 0), record("head", 0, 5000)]
        records[1]["result"]["warm"]["misses"] = 1
        self.assertIn("misses rose", stats.summarize(records)["failures"][0])

    def test_tool_path_sees_through_mise_shims(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            install = root / "installs" / "sccache-1.0"
            install.mkdir(parents=True)
            real = install / "sccache"
            real.write_text("#!/bin/sh\necho real\n")
            real.chmod(0o755)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            mise = bin_dir / "mise"
            mise.write_text(
                '#!/bin/sh\ncase "$1 $2" in "which sccache") echo "%s" ;; *) exit 1 ;; esac\n'
                % real
            )
            mise.chmod(0o755)
            shims = root / "shims"
            shims.mkdir()
            (shims / "sccache").symlink_to(mise)
            (shims / "unknown").symlink_to(mise)
            plain = root / "plain"
            plain.mkdir()
            link = plain / "kache"
            link.symlink_to(real)

            with patch.dict(
                "os.environ",
                {"PATH": os.pathsep.join(map(str, (shims, plain, bin_dir)))},
            ):
                self.assertEqual(engine.tool_path("sccache"), str(real))
                self.assertEqual(
                    engine.tool_path("unknown"),
                    str(shims / "unknown"),
                    "a shim mise cannot locate is kept as the shim itself",
                )
                self.assertEqual(
                    engine.tool_path("kache"),
                    os.path.realpath(real),
                    "an ordinary symlink is followed as before",
                )
                self.assertEqual(
                    engine.tool_path(str(root / "absent")),
                    os.path.realpath(root / "absent"),
                    "a missing tool keeps its name so the failure names it",
                )

    def test_driver_alternates_arms_and_saves_samples(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args = argparse.Namespace(
                output=root / "output",
                project="hk",
                engine=root / "engine",
                scenarios=root / "scenarios",
                kache="/kache",
                base="/base",
                sccache="/sccache",
                mbx="/mbx",
                samples=6,
                order_seed=0,
                cold_every=3,
                skip_contention=False,
            )
            calls = []

            def invoke(command, **kwargs):
                calls.append(command)
                work = Path(command[command.index("--work-dir") + 1])
                work.mkdir(parents=True, exist_ok=True)
                scenario = command[command.index("--profile") + 1]
                backend = command[command.index("--cache-backend") + 1]
                (work / f"{scenario}.json").write_text(json.dumps(result(backend)))
                points = [
                    {
                        "attributes": [
                            {
                                "key": "kache.bench.phase",
                                "value": {"stringValue": phase},
                            }
                        ]
                    }
                    for phase in ("cold", "warm")
                ]
                (work / "metrics.otlp.json").write_text(
                    json.dumps(
                        {
                            "resourceMetrics": [
                                {
                                    "scopeMetrics": [
                                        {"metrics": [{"gauge": {"dataPoints": points}}]}
                                    ]
                                }
                            ]
                        }
                    )
                )

                if backend == "kache":
                    for phase in ("cold", "warm-same-tree", "warm"):
                        dest = work / f"cache-otlp-{phase}"
                        dest.mkdir(exist_ok=True)
                        (dest / "metrics.otlp.json").write_text(
                            (work / "metrics.otlp.json").read_text()
                        )

            def contention(args, arms):
                self.assertFalse((args.output / "scratch").exists())
                self.assertEqual(
                    [arm[0] for arm in arms], ["base", "head", "sccache", "mbx"]
                )
                output = args.output / "contention"
                output.mkdir()
                (output / "samples.json").write_text(
                    json.dumps({"revision": "subject-sha"})
                )
                (output / "report.md").write_text("## Contention: hk\n")
                (output / "metrics.otlp.json").write_text(
                    json.dumps({"resourceMetrics": []})
                )
                return {"statistics": [], "comparisons": [], "failures": []}

            with (
                patch.object(short, "run_measurement", invoke),
                patch.object(short, "run_contention", contention),
            ):
                self.assertEqual(short.run(args), 0)
            self.assertEqual(len(calls), 24)
            self.assertTrue(calls[0][calls[0].index("--kache") + 1].endswith("/base"))
            self.assertEqual(calls[4][calls[4].index("--cache-backend") + 1], "mbx")
            self.assertNotIn("--retry", calls[0])
            self.assertIn("--retry", calls[4])
            self.assertNotIn("--retry", calls[12])
            self.assertIn("--skip-clone", calls[12])
            payload = json.loads((args.output / "samples.json").read_text())
            self.assertEqual(len(payload["records"]), 24)
            self.assertEqual(payload["contention_samples"], "contention/samples.json")
            text = (args.output / "perf-gate.md").read_text()
            self.assertTrue(text.startswith("## Perf gate: pass (hk)\n"))
            self.assertIn("| Build | hk base | hk head | hk change |", text)
            self.assertIn("<summary>All tools, isolated builds</summary>", text)
            self.assertFalse((args.output / "scratch").exists())
            metrics = json.loads((args.output / "metrics.otlp.json").read_text())
            points = [
                point
                for resource in metrics["resourceMetrics"]
                for scope in resource["scopeMetrics"]
                for metric in scope["metrics"]
                for point in metric["gauge"]["dataPoints"]
            ]
            cold = [
                point
                for point in points
                if point["attributes"][0]["value"]["stringValue"] == "cold"
            ]
            self.assertEqual(len(cold), 8)
            self.assertIn("inconclusive", (args.output / "perf-gate.md").read_text())
            for phase, expected in (("cold", 4), ("warm", 12)):
                cached = json.loads(
                    (
                        args.output / f"cache-otlp-{phase}" / "metrics.otlp.json"
                    ).read_text()
                )
                self.assertEqual(len(cached["resourceMetrics"]), expected)

    def test_contention_compares_paired_work_and_rejects_incomplete_pairs(self):
        records = []
        for sample in range(6):
            for arm, ms in (("base", 10000), ("head", 12000)):
                for phase in ("cold", "warm"):
                    records.append(
                        {
                            "sample": sample,
                            "arm": arm,
                            "phase": phase,
                            "wall_ms": ms,
                            "events": {
                                "duplicate_key_compiles": 0,
                                "results": {"miss": int(phase == "cold")},
                            },
                        }
                    )
        comparisons, failures = stats.contention_comparison(records)
        self.assertEqual(len(comparisons), 2)
        self.assertEqual(len(failures), 2)
        for row in records:
            row["wall_ms"] = 10000
        records[-1]["events"]["results"]["miss"] = 1
        self.assertIn(
            "miss count increased", stats.contention_comparison(records)[1][0]
        )
        with self.assertRaisesRegex(ValueError, "incomplete"):
            stats.contention_comparison(records[:-1])

    def test_contention_driver_requires_all_tool_phase_samples(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args = argparse.Namespace(
                output=root,
                project="eza",
                scenarios=root / "scenarios",
                samples=6,
                order_seed=1,
            )
            arms = [
                ("kache", "kache", "/kache"),
                ("sccache", "sccache", "/sccache"),
                ("mbx", "mbx", "/mbx"),
            ]
            records = [
                {"sample": sample, "arm": arm, "phase": phase}
                for sample in range(6)
                for arm, _, _ in arms
                for phase in ("cold", "warm")
                if phase == "warm" or sample % 3 == 0
            ]
            calls = []

            def run(command, **kwargs):
                calls.append(command)
                output = root / "contention"
                output.mkdir(exist_ok=True)
                (output / "samples.json").write_text(json.dumps({"records": records}))
                (output / "summary.json").write_text("[]")

            with patch.object(short.subprocess, "run", run):
                self.assertEqual(short.run_contention(args, arms)["failures"], [])
                self.assertIn("kache=/kache,1", calls[0])
                self.assertIn("--sccache", calls[0])
                self.assertIn("--mbx", calls[0])
                self.assertEqual(calls[0][calls[0].index("--samples") + 1], "6")
                records[-1] = records[0]
                with self.assertRaisesRegex(ValueError, "incomplete contention"):
                    short.run_contention(args, arms)

    def test_a_missing_context_tool_skips_its_arm_and_a_named_one_does_not(self):
        """A laptop without sccache should still measure head against base.

        Before this, the run cloned the subject, built it, and only then died
        because a bare `sccache` was not on PATH.
        """
        args = argparse.Namespace(sccache="sccache", mbx="mbx", base="/base")
        with patch.object(engine.shutil, "which", return_value=None):
            self.assertFalse(short.wanted_arm(("sccache", "sccache", "sccache"), args))
            self.assertFalse(short.wanted_arm(("mbx", "mbx", "mbx"), args))
            # The arms that decide the verdict are never skipped away.
            self.assertTrue(short.wanted_arm(("head", "kache", "/kache"), args))
            self.assertTrue(short.wanted_arm(("base", "kache", "/base"), args))

        # Asking for a specific binary keeps the arm, so a wrong path is still
        # an error rather than a silently missing comparison.
        named = argparse.Namespace(sccache="/opt/sccache", mbx="mbx", base=None)
        with patch.object(engine.shutil, "which", return_value=None):
            self.assertTrue(short.wanted_arm(("sccache", "sccache", "/opt/sccache"), named))

        with patch.object(engine.shutil, "which", return_value="/usr/bin/sccache"):
            self.assertTrue(short.wanted_arm(("sccache", "sccache", "sccache"), args))

    def test_cold_every_one_measures_every_cold_build(self):
        """A change aimed at cold needs more than one cold measurement.

        The default reuses a cold build for two samples out of three, so
        `--samples 3` yields three warm pairs and a single cold one.
        """
        default = [short.cold_is_reused(sample, 3) for sample in range(6)]
        self.assertEqual(default, [False, True, True, False, True, True])

        every = [short.cold_is_reused(sample, 1) for sample in range(4)]
        self.assertEqual(every, [False, False, False, False])

    def test_subprocess_failure_keeps_logs_and_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args = argparse.Namespace(
                output=root / "output",
                project="eza",
                engine=root / "engine",
                scenarios=root / "scenarios",
                kache="/kache",
                base=None,
                sccache="/sccache",
                mbx="/mbx",
                samples=1,
                order_seed=0,
                cold_every=3,
                skip_contention=True,
            )
            with patch.object(
                short,
                "run_measurement",
                side_effect=subprocess.CalledProcessError(1, ["engine"]),
            ):
                self.assertEqual(short.run(args), 1)
            self.assertIn(
                "INVALID MEASUREMENT", (args.output / "perf-gate.md").read_text()
            )
            self.assertTrue((args.output / "logs/00-kache/engine.log").exists())


def subject(root, name, records, contention=None, error=None):
    """A bench-short output directory as the report job downloads it."""
    directory = root / name
    directory.mkdir()
    payload = {"project": name, "records": records}
    if error:
        payload["error"] = error
    (directory / "samples.json").write_text(json.dumps(payload))
    if error:
        return directory
    summary = stats.summarize(records)
    if contention:
        summary["contention"] = contention
    (directory / "summary.json").write_text(json.dumps(summary))
    return directory


def paired(ms_base, ms_head, samples=1):
    return [
        record(arm, i, ms)
        for i in range(samples)
        for arm, ms in (("base", ms_base), ("head", ms_head))
    ]


class ReportTests(unittest.TestCase):
    render = staticmethod(report.render)

    def test_head_against_base_leads_and_the_rest_folds_away(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            contention = {
                "statistics": [
                    {
                        "arm": arm,
                        "phase": phase,
                        "n": 1,
                        "median_ms": 9000,
                        "min_ms": 9000,
                        "max_ms": 9000,
                        "compiler_runs": 3,
                        "duplicate_key_compiles": 0,
                        "flight_wait_ms": 0,
                        "permit_wait_ms": None,
                    }
                    for arm in ("base", "head")
                    for phase in ("cold", "warm")
                ],
            }
            hk = subject(root, "hk", paired(84615, 97269), contention)
            summary = json.loads((hk / "summary.json").read_text())
            summary["comparisons"].append(
                {"phase": "contention_warm", "n": 1, "median_pct": -10.2,
                 "interval_95_pct": None, "outcome": "inconclusive"}
            )
            (hk / "summary.json").write_text(json.dumps(summary))
            eza = subject(root, "eza", paired(1869, 2885))
            text = self.render([hk, eza])

        self.assertTrue(text.startswith("## Perf gate: pass (hk, eza)\n"))
        self.assertIn("did not rise against base", text)
        self.assertIn(
            "| Build | hk base | hk head | hk change | eza base | eza head | eza change |",
            text,
        )
        self.assertIn("| Cold | 84.6 s | 97.3 s | +15.0% | 1.87 s | 2.88 s | +54.4% |", text)
        self.assertIn("| Contention, warm | 9.00 s | 9.00 s | -10.2% | — | — | — |", text)
        self.assertIn("every change is inconclusive", text)
        self.assertEqual(text.count("<details>"), 3)
        self.assertEqual(text.count("<details>"), text.count("</details>"))
        self.assertIn("| head, warm | 3 | 0 | 0.00 s | — |", text)
        self.assertNotIn("<details open>", text)
        # One table per subject inside a fold, never a row per tool and phase.
        self.assertIn("| hk | head | base |", text)
        self.assertIn("Versions: head `kache 1.0`, base `kache 1.0`.", text)

    def test_versions_name_each_tool_and_split_only_where_subjects_differ(self):
        def run(arm, version):
            r = record(arm, 0)
            r["result"]["cache_tool_version"] = version
            return r

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            hk = subject(root, "hk", [run("kache", "kache 0.24.0"), run("mbx", "mbx 1.10.1"),
                                      run("sccache", "sccache 0.10.0")])
            eza = subject(root, "eza", [run("kache", "kache 0.24.0"), run("mbx", "mbx 1.11.0")])
            text = self.render([hk, eza])
        self.assertIn(
            "Versions: kache `kache 0.24.0`, mbx `mbx 1.10.1` (hk), "
            "mbx `mbx 1.11.0` (eza), sccache `sccache 0.10.0`.",
            text,
        )

    def test_regression_and_count_failures_are_the_first_thing_read(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            records = paired(10000, 12000, samples=6)
            records[-1]["result"]["warm"]["misses"] = 2
            text = self.render([subject(root, "hk", records)])
        headline, _, failed, _, first = text.splitlines()[:5]
        self.assertEqual(headline, "## Perf gate: FAIL (hk)")
        self.assertEqual(failed, "**Failed checks**")
        self.assertIn("misses rose 0 → 2", first)
        self.assertIn("**+20.0% regression**", text)
        self.assertIn("fewer than 5 pairs stay inconclusive", text)
        self.assertIn("<sub>10.0–10.0</sub>", text)
        self.assertIn("(+20.0% to +20.0%), 6 pairs, regression", text)

    def test_invalid_subject_is_named_and_the_valid_one_still_reports(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            hk = subject(root, "hk", [], error="warm: restored nothing")
            eza = subject(root, "eza", paired(10000, 10000))
            text = self.render([hk, eza], verdict="INVALID MEASUREMENT")
            missing = root / "gone"
            missing.mkdir()
            (missing / "perf-gate.md").write_text("## Perf gate: INVALID MEASUREMENT (gone)\n\nno disk\n")
            self.assertIn("**gone: invalid measurement.** no disk", self.render([missing]))
        self.assertTrue(text.startswith("## Perf gate: INVALID MEASUREMENT (hk, eza)\n"))
        self.assertIn("**hk: invalid measurement.** warm: restored nothing", text)
        self.assertIn("| Build | eza base | eza head | eza change |", text)

    def test_single_kache_run_opens_the_tool_table(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            text = self.render([subject(root, "hk", [record("kache", 0)])])
        self.assertIn("<details open>", text)
        self.assertNotIn("| Build |", text)
        self.assertNotIn("did not rise", text)


if __name__ == "__main__":
    unittest.main()
