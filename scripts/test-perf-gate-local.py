#!/usr/bin/env python3
"""The local gate measures the same way CI does, and cleans up after itself."""

import argparse
import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

_spec = importlib.util.spec_from_file_location(
    "perf_gate_local", Path(__file__).with_name("perf-gate-local.py")
)
local = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(local)


class WorktreeTests(unittest.TestCase):
    def test_a_failure_inside_the_build_still_removes_the_worktree(self):
        """A leaked worktree wedges the next run and the repo it came from.

        This is why the helper is a context manager: an earlier draft looped
        over a generator, which leaves cleanup to the garbage collector.
        """
        calls = []

        def fake_git(*arguments, **kwargs):
            calls.append(arguments)
            return ""

        with patch.object(local, "git", fake_git):
            with patch.object(local.subprocess, "run", side_effect=RuntimeError("build blew up")):
                with self.assertRaises(RuntimeError):
                    with local.at_commit("abc123", "kache", "kache", "head"):
                        pass

        self.assertIn("add", calls[0])
        self.assertEqual(calls[-1][:2], ("worktree", "remove"))

    def test_the_body_runs_with_the_worktree_and_it_is_removed_after(self):
        calls = []

        with patch.object(local, "git", lambda *a, **k: calls.append(a) or ""):
            with patch.object(local.subprocess, "run"):
                with local.at_commit("abc123", "kache", "kache", "head") as tree:
                    self.assertTrue(str(tree).endswith("tree"))
        self.assertEqual(calls[-1][:2], ("worktree", "remove"))


class MeasureTests(unittest.TestCase):
    def command_for(self, **overrides):
        args = argparse.Namespace(
            samples=3,
            cold_every=1,
            skip_contention=True,
            head_sha="h" * 40,
            base_sha="b" * 40,
            instrument_sha="i" * 40,
        )
        for key, value in overrides.items():
            setattr(args, key, value)
        captured = {}

        def fake_run(command, env=None, check=None):
            captured["command"] = command
            captured["env"] = env
            return subprocess.CompletedProcess(command, 0)

        with patch.object(local.subprocess, "run", fake_run):
            local.measure(
                "eza", args, Path("/stage"), Path("/head"), Path("/base"), Path("/out")
            )
        return captured

    def test_it_drives_the_staged_instrument_not_the_working_tree(self):
        """A change to the engine or the threshold must not measure itself.

        CI takes the engine, the scenarios and the scripts from the default
        branch for this reason; taking any of them from the checkout is a slip
        that never shows up in the report.
        """
        captured = self.command_for()
        command = captured["command"]

        self.assertIn("/stage/bench-short.py", command)
        self.assertEqual(command[command.index("--engine") + 1], "/stage/kache-scenario")
        self.assertEqual(command[command.index("--scenarios") + 1], "/stage/scenarios")
        self.assertEqual(command[command.index("--kache") + 1], "/head")
        self.assertEqual(command[command.index("--base") + 1], "/base")

    def test_cold_every_reaches_the_harness(self):
        """The whole point of the flag is measuring cold more than once."""
        for value in (1, 3):
            command = self.command_for(cold_every=value)["command"]
            self.assertEqual(command[command.index("--cold-every") + 1], str(value))

    def test_contention_is_opt_in(self):
        self.assertIn("--skip-contention", self.command_for()["command"])
        self.assertNotIn(
            "--skip-contention", self.command_for(skip_contention=False)["command"]
        )

    def test_the_run_identifies_both_sides_and_the_instrument(self):
        """`samples.json` records these, which is how a stray report is traced."""
        env = self.command_for()["env"]
        self.assertEqual(env["BENCH_HEAD_SHA"], "h" * 40)
        self.assertEqual(env["BENCH_BASE_SHA"], "b" * 40)
        self.assertEqual(env["BENCH_INSTRUMENT_SHA"], "i" * 40)
        # Measuring kache through kache would put the subject in its own baseline.
        self.assertEqual(env["RUSTC_WRAPPER"], "")


class ResolveTests(unittest.TestCase):
    def test_an_unfetched_ref_says_to_fetch_it(self):
        error = subprocess.CalledProcessError(1, ["git"])
        with patch.object(local, "git", side_effect=error):
            with self.assertRaises(SystemExit) as raised:
                local.resolve("origin/main")
        self.assertIn("git fetch", str(raised.exception))

    def test_a_real_repository_resolves_head(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            subprocess.run(["git", "init", "--quiet", str(root)], check=True)
            subprocess.run(["git", "-C", str(root), "config", "user.email", "t@t"], check=True)
            subprocess.run(["git", "-C", str(root), "config", "user.name", "t"], check=True)
            (root / "f").write_text("x")
            subprocess.run(["git", "-C", str(root), "add", "f"], check=True)
            subprocess.run(
                ["git", "-C", str(root), "commit", "--quiet", "-m", "c", "--no-gpg-sign"],
                check=True,
            )
            with patch.object(local, "REPO", root):
                self.assertEqual(len(local.resolve("HEAD")), 40)


if __name__ == "__main__":
    unittest.main()
