import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from bench.event_capture import detect_event_capture, event_drop_counts, read_overflow_event


PROBE = {"result": "passthrough", "crate_name": "unknown",
         "passthrough_reason": "not-a-compile|query / probe (--print, -vV)"}
LEGACY = "DEBUG kache::wrapper: could not record compiler event: locking event log\n"
MODERN = LEGACY.replace("wrapper:", "wrapper::event_log:")


class OverflowFormatTests(unittest.TestCase):
    def test_preserves_the_original_json_line(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "event-one.jsonl"
            raw = '{ "result": "local_hit", "crate_name": "library" }\n'
            path.write_bytes(raw.encode())
            self.assertEqual(read_overflow_event(path), (raw, json.loads(raw)))

    def test_rejects_incomplete_fragments_and_non_build_records(self):
        line = json.dumps(PROBE) + "\n"
        with tempfile.TemporaryDirectory() as tmp:
            for name, content in (("event-one", line), ("random.jsonl", line),
                                  ("event-one.jsonl", line.rstrip()), ("event-one.jsonl", line * 2),
                                  ("event-one.jsonl", "corrupt\n"), ("event-one.jsonl", "{}\n")):
                with self.subTest(name=name, content=content):
                    path = Path(tmp) / name
                    path.write_bytes(content.encode())
                    with self.assertRaises(ValueError):
                        read_overflow_event(path)

    def test_only_wrapper_file_log_diagnostics_count(self):
        self.assertEqual(event_drop_counts("2026-11-24T19:00:00Z " + LEGACY + MODERN), (1, 1))
        self.assertEqual(event_drop_counts("INFO other: " + LEGACY + "DEBUG compiler: " + MODERN), (0, 0))


@unittest.skipUnless(os.name == "posix", "requires the Unix sidecar file lock")
class CapturePreflightTests(unittest.TestCase):
    def probe(self, entries=(), log="", primary=False, failure=False):
        import fcntl

        binary = Path("/tool/kache")
        rustc = Path("/toolchain/rustc")
        env = {"PATH": "/toolchain", "RUSTUP_TOOLCHAIN": "1.97.1"}
        roots = []
        stopped = []

        def produce(command, *, cwd, env, **kwargs):
            self.assertEqual(command, [str(binary), str(rustc), "-vV"])
            self.assertEqual(env["PATH"], "/toolchain")
            self.assertEqual(env["RUSTUP_TOOLCHAIN"], "1.97.1")
            self.assertEqual(env["RUSTUP_AUTO_INSTALL"], "0")
            roots.append(cwd)
            runtime = Path(env["KACHE_RUNTIME_DIR"])
            spool = runtime / "events.jsonl.overflow"
            self.assertEqual(stat.S_IMODE(spool.stat().st_mode), 0o700)
            with (runtime / "events.jsonl.lock").open("rb") as lock:
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if failure:
                raise subprocess.TimeoutExpired(command, 30)
            for i, event in enumerate(entries):
                (spool / f"event-{i}.jsonl").write_text(json.dumps(event) + "\n")
            if primary:
                (runtime / "events.jsonl").write_text(json.dumps(PROBE) + "\n")
            Path(env["KACHE_LOG_FILE_PATH"]).write_text(log)
            return subprocess.CompletedProcess(command, 0, stdout="rustc 1.97.1\nhost: test\n")

        def stop(cmd, env, root):
            self.assertEqual(cmd, binary)
            with (Path(env["KACHE_RUNTIME_DIR"]) / "events.jsonl.lock").open("rb") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            stopped.append(root)

        try:
            with patch("bench.event_capture.subprocess.run", side_effect=produce):
                return detect_event_capture(binary, rustc, env, stop)
        finally:
            self.assertEqual(len(roots), 1)
            self.assertEqual(stopped, roots)
            self.assertFalse(roots[0].exists())

    def test_detects_both_producer_contracts(self):
        probe = dict(PROBE, crate_name="rustc", passthrough_reason="not-a-compile|version query")
        self.assertEqual(self.probe(entries=[probe]), "overflow")
        self.assertEqual(self.probe(log=LEGACY), "diagnostic")

    def test_rejects_ambiguous_missing_or_incorrect_capture(self):
        for entries, log, primary in (
            ([], "", False), ([], MODERN, False), ([], LEGACY * 2, False),
            ([PROBE], MODERN, False), ([PROBE], LEGACY, False),
            ([PROBE, PROBE], "", False), ([dict(PROBE, passthrough_reason="refused|some build")], "", False),
            ([], LEGACY, True),
        ):
            with self.subTest(entries=entries, log=log, primary=primary):
                with self.assertRaises(ValueError):
                    self.probe(entries, log, primary)

    def test_cleans_up_after_timeout(self):
        with self.assertRaises(subprocess.TimeoutExpired):
            self.probe(failure=True)
