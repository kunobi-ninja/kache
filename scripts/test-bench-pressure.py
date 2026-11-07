#!/usr/bin/env python3
"""Pressure benchmark guards must fail closed without stressing the test host."""

import importlib.util
import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


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
    @staticmethod
    def fake_pair(root, engine_bytes=b"fake engine; never execute"):
        root.mkdir(parents=True, exist_ok=True)
        frontend, engine = root / "kache", root / "kache-engine"
        frontend.write_bytes(b"fake frontend; never execute")
        engine.write_bytes(engine_bytes)
        frontend.chmod(0o755)
        engine.chmod(0o755)
        return frontend, engine

    @unittest.skipUnless(os.name == "posix", "Linux benchmark executable permissions")
    def test_pair_uses_the_canonical_frontends_sibling_not_the_alias_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            frontend, engine = self.fake_pair(root / "installed")
            alias, decoy = self.fake_pair(root / "alias", b"wrong sibling")
            alias.unlink()
            alias.symlink_to(frontend)
            with patch.object(bench.subprocess, "Popen", side_effect=AssertionError("must not execute fixtures")):
                pair = bench.executable_pair(alias)
            self.assertEqual(pair["frontend"]["path"], str(frontend.resolve()))
            self.assertEqual(pair["engine"], bench.identity(engine))
            self.assertNotEqual(pair["engine"]["sha256"], bench.identity(decoy)["sha256"])

    @unittest.skipUnless(os.name == "posix", "Linux benchmark executable permissions")
    def test_missing_engine_refuses_the_batch_before_creating_workload_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            frontend, engine = self.fake_pair(root / "installed")
            pair = bench.executable_pair(frontend)
            engine.unlink()
            output = root / "results"
            output.mkdir()
            with self.assertRaises(FileNotFoundError):
                bench.run_batch(SimpleNamespace(output=output), "base", frontend, 0, "calm", pair)
            self.assertEqual(list(output.iterdir()), [])

    @unittest.skipUnless(os.name == "posix", "Linux benchmark executable permissions")
    def test_nonexecutable_companion_is_rejected_without_running_it(self):
        with tempfile.TemporaryDirectory() as directory:
            frontend, engine = self.fake_pair(Path(directory))
            engine.chmod(0o644)
            with self.assertRaisesRegex(ValueError, "not executable"):
                bench.executable_pair(frontend)

    @unittest.skipUnless(os.name == "posix", "Linux benchmark executable permissions")
    def test_changed_engine_invalidates_the_record_before_creating_workload_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            frontend, engine = self.fake_pair(root / "installed")
            pair = bench.executable_pair(frontend)
            engine.write_bytes(b"different engine after the receipt was captured")
            output = root / "results"
            output.mkdir()
            with self.assertRaisesRegex(ValueError, "pair changed"):
                bench.run_batch(SimpleNamespace(output=output), "base", frontend, 0, "calm", pair)
            self.assertEqual(list(output.iterdir()), [])

    @unittest.skipUnless(os.name == "posix", "Linux benchmark executable permissions")
    def test_metadata_identifies_both_engines_and_flags_identical_engine_controls(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base, _ = self.fake_pair(root / "base")
            head, engine = self.fake_pair(root / "head")
            pairs = {"base": bench.executable_pair(base), "head": bench.executable_pair(head)}
            metadata = bench.pair_metadata(pairs)
            self.assertEqual(metadata["base_engine"], pairs["base"]["engine"])
            self.assertEqual(metadata["head_engine"], pairs["head"]["engine"])
            self.assertTrue(metadata["identical_engine_bytes"])
            bench.verify_pair(head, pairs["head"])
            engine.write_bytes(b"new policy engine")
            pairs["head"] = bench.executable_pair(head)
            self.assertFalse(bench.pair_metadata(pairs)["identical_engine_bytes"])

    @unittest.skipUnless(os.name == "posix", "writer helpers use process groups")
    def test_limit_writer_runs_outside_the_supervisor_and_updates_the_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            leaf = Path(directory)
            (leaf / "memory.high").write_text("4096")
            writer = bench.HighLimits()
            try:
                writer.update(leaf, "2048")
                deadline = time.monotonic() + 3
                while writer.pending and time.monotonic() < deadline:
                    time.sleep(.01)
                    writer.update(leaf, "2048")
                self.assertFalse(writer.pending)
                self.assertEqual((leaf / "memory.high").read_text(), "2048")
            finally:
                writer.stop()
                writer.wait()

    @unittest.skipUnless(os.name == "posix", "writer helpers use process groups")
    def test_a_blocked_limit_writer_can_be_stopped_without_blocking_the_supervisor(self):
        real_popen = subprocess.Popen
        children = []
        def blocked_writer(*_args, **_kwargs):
            child = real_popen([sys.executable, "-c", "import time; time.sleep(60)"],
                               start_new_session=True, stdout=subprocess.DEVNULL,
                               stderr=subprocess.PIPE)
            children.append(child)
            return child
        with tempfile.TemporaryDirectory() as directory:
            leaf = Path(directory)
            (leaf / "memory.high").write_text("4096")
            writer = bench.HighLimits()
            try:
                with patch.object(bench.subprocess, "Popen", side_effect=blocked_writer):
                    writer.update(leaf, "2048")
                with self.assertRaisesRegex(RuntimeError, "write exceeded"):
                    writer.update(leaf, "2048", timeout=0)
            finally:
                writer.stop()
                writer.wait()
            self.assertEqual(children[0].returncode, -9)

    def test_nonfinite_timeout_cannot_disable_the_stop_deadline(self):
        required = ["--base", __file__, "--head", __file__,
                    "--base-revision", "same", "--head-revision", "same",
                    "--control-patch", __file__, "--cargo", __file__, "--rustc", __file__,
                    "--output", "/tmp/unused-pressure-test", "--cgroup-parent", "/tmp",
                    "--dedicated-host"]
        for value in ("nan", "inf", "-inf"):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                bench.arguments([*required, "--timeout-s=" + value])

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

    @staticmethod
    def pressure_source_shapes():
        original = '''pub(crate) fn ceiling(root: &Path, pool: u32) -> u32 {
    match now() {
        Some(level) => shared(root, pool, level, super::queue::millis(SystemTime::now())),
        None => pool,
    }
}
'''
        observed = '''pub(crate) fn ceiling_observed(root: &Path, pool: u32) -> (u32, Option<Level>) {
    let level = now();
    let ceiling = match level {
        Some(level) => shared(root, pool, level, super::queue::millis(SystemTime::now())),
        None => pool,
    };
    (ceiling, level)
}
'''
        return original, observed

    def test_control_keeps_observation_and_legacy_policy_for_both_source_shapes(self):
        scheduler = (Path(__file__).resolve().parents[1] / "src/scheduler.rs").read_text()
        for index, source in enumerate(self.pressure_source_shapes()):
            with self.subTest(observed=index == 1), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "src/scheduler").mkdir(parents=True)
                pressure = root / "src/scheduler/pressure.rs"
                pressure.write_text(source)
                (root / "src/scheduler.rs").write_text(scheduler)
                patch = control.control_patch(root)
                subprocess.run(["git", "apply", "--check", "-"], input=patch,
                               text=True, cwd=root, check=True, timeout=5)
                subprocess.run(["git", "apply", "-"], input=patch,
                               text=True, cwd=root, check=True, timeout=5)
                result = pressure.read_text()
                self.assertEqual(result.count("let level = now();"), 1)
                self.assertEqual(result.count("shared(root, pool, level,"), 1)
                self.assertIn("if let Some(level) = level {", result)
                self.assertIn("Some(Level::Warning | Level::Critical) => 1,", result)
                self.assertIn("_ => pool,", result)
                self.assertEqual("(ceiling, level)" in result, index == 1)

    def test_control_rejects_ambiguous_or_duplicate_pressure_source_shapes(self):
        original, observed = self.pressure_source_shapes()
        for source in (original + observed, original + original, observed + observed):
            with self.subTest(source=source), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "src/scheduler").mkdir(parents=True)
                (root / "src/scheduler/pressure.rs").write_text(source)
                with self.assertRaisesRegex(ValueError, "source changed"):
                    control.control_patch(root)

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
