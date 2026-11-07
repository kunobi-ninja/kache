#!/usr/bin/env python3
"""Pressure benchmark guards must fail closed without stressing the test host."""

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


def module(name, relative):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).parent / relative)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


bench = module("bench_pressure", "bench-pressure.py")
control = module("legacy_pressure_control", "bench/legacy-pressure-control.py")


def sample():
    group = {"memory_current_bytes": 200, "memory_peak_bytes": 400,
             "swap_current_bytes": 0, "memory_max": "1000",
             "memory_events": {"oom": 0, "oom_kill": 0},
             "pressure": {"some": {"avg10": 12}, "full": {"avg10": 2}}}
    return {"parent": dict(group), "groups": [dict(group)],
            "host_available_bytes": 10000, "compiler_rss_bytes": 300}


class PressureBenchTests(unittest.TestCase):
    def test_stop_guard_handles_parent_and_tighter_child_limits(self):
        current = sample()
        self.assertIsNone(bench.safety_reason(current, 1000, 100))
        current["groups"][0]["memory_current_bytes"] = 900
        self.assertIn("build cgroup", bench.safety_reason(current, 1000, 100))
        current = sample()
        current["parent"]["memory_current_bytes"] = 900
        self.assertIn("workload", bench.safety_reason(current, 1000, 100))

    def test_oom_and_host_floor_abort_even_below_workload_cap(self):
        for event in ("oom", "oom_kill", "oom_group_kill"):
            current = sample()
            current["parent"]["memory_events"] = {event: 1}
            self.assertIn("OOM", bench.safety_reason(current, 1000, 100))
        current = sample()
        current["host_available_bytes"] = 99
        self.assertIn("host", bench.safety_reason(current, 1000, 100))

    def test_missing_measurements_are_errors_instead_of_zero(self):
        current = sample()
        del current["parent"]["memory_current_bytes"]
        with self.assertRaises(KeyError):
            bench.safety_reason(current, 1000, 100)
        with self.assertRaises(FileNotFoundError):
            bench.snapshot(Path("/nonexistent/kache-pressure-test"))

    @unittest.skipUnless(os.name == "posix", "exec helper uses POSIX niceness")
    def test_exec_helper_joins_before_running_the_command(self):
        # A plain temporary file stands in for cgroup.procs. No limits change,
        # no compilers run, and the child must see its own PID already written.
        with tempfile.TemporaryDirectory() as directory:
            group = Path(directory)
            output = group / "observed.json"
            code = (
                "import json,os,pathlib,sys; p=pathlib.Path(sys.argv[1]); "
                "p.joinpath('observed.json').write_text(json.dumps({"
                "'joined':p.joinpath('cgroup.procs').read_text()==str(os.getpid()),"
                "'nice':os.getpriority(os.PRIO_PROCESS,0)}))"
            )
            nice = "10" if sys.platform == "linux" else "0"
            subprocess.run([sys.executable, str(bench.SCRIPT), "_exec", str(group), nice,
                            sys.executable, "-c", code, directory], check=True, timeout=5)
            observed = json.loads(output.read_text())
            self.assertTrue(observed["joined"])
            self.assertGreaterEqual(observed["nice"], int(nice))

    def test_foreground_failures_cannot_improve_reported_success_latency(self):
        jobs = [{"kind": "foreground", "duration_s": 9, "returncode": 0},
                {"kind": "foreground", "duration_s": 1, "returncode": 1},
                {"kind": "background", "duration_s": 60, "returncode": 0}]
        result = bench.summarize([sample()], jobs, 60)
        self.assertEqual(result["foreground_count"], 1)
        self.assertEqual(result["foreground_mean_s"], 9)
        self.assertEqual(result["completed_builds"], 2)
        self.assertEqual(result["builds_per_minute"], 2)
        self.assertEqual(result["sampled_peak_compiler_rss_bytes"], 300)
        self.assertEqual(result["peak_cgroup_memory_bytes"], 400)
        self.assertEqual(result["warning_samples"], 1)
        self.assertEqual(result["critical_samples"], 0)

    def test_foreground_projects_do_not_share_background_keys(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("background", "foreground"):
                bench.workspace(root / name, name, 1, 3)
            first = (root / "background/unit-0/src/lib.rs").read_text()
            second = (root / "foreground/unit-0/src/lib.rs").read_text()
            self.assertNotEqual(first, second)

    def test_control_patch_applies_to_this_source_and_fails_on_drift(self):
        root = Path(__file__).resolve().parents[1]
        patch = control.control_patch(root)
        subprocess.run(["git", "apply", "--check", "-"], input=patch,
                       text=True, cwd=root, check=True, timeout=5)
        # Both the larger ordinary request and its retained slots are needed.
        self.assertIn("Some(Level::Warning | Level::Critical) => 1", patch)
        self.assertIn("-            slots.truncate(keep);", patch)
        with tempfile.TemporaryDirectory() as directory:
            changed = Path(directory)
            (changed / "src/scheduler").mkdir(parents=True)
            (changed / "src/scheduler/pressure.rs").write_text("changed source")
            with self.assertRaisesRegex(ValueError, "source changed"):
                control.control_patch(changed)


if __name__ == "__main__":
    unittest.main()
