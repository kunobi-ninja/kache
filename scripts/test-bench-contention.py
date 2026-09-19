#!/usr/bin/env python3
"""Contention measurements must distinguish elapsed time, work, and waiting."""

import importlib.util
import os
import stat
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "bench", Path(__file__).with_name("bench-contention.py")
)
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


class ContentionTests(unittest.TestCase):
    def test_storage_counts_hardlinks_once_and_does_not_follow_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = root / "cache"
            blobs = store / "store" / "blobs"
            blobs.mkdir(parents=True)
            repo = root / "repo"
            target = repo / "target"
            target.mkdir(parents=True)
            blob = blobs / "artifact"
            blob.write_bytes(b"x" * 8192)
            os.link(blob, target / "artifact")
            os.link(blob, target / "second-name")
            (root / "unrelated").write_bytes(b"x" * 100_000)
            (target / "link").symlink_to(root / "unrelated")
            (target / "cycle").symlink_to(target, target_is_directory=True)
            result = bench.measure_storage(store, [repo], "kache")
            groups = result["groups"]
            self.assertEqual(result["cache_target_shared_inodes"], 1)
            self.assertEqual(result["cache_target_shared_logical_bytes"], 8192)
            self.assertEqual(groups["cache_blobs"]["logical_bytes"], 8192)
            self.assertEqual(
                groups["cache_and_targets"]["logical_bytes"]
                - groups["cache_and_targets"]["unique_logical_bytes"],
                2 * 8192,
            )
            self.assertLess(groups["cache_and_targets"]["unique_logical_bytes"], 10_000)
            self.assertEqual(
                groups["cache"]["allocated_bytes"] + groups["targets"]["allocated_bytes"]
                - groups["cache_and_targets"]["allocated_bytes"],
                blob.stat().st_blocks * 512,
            )

    def test_storage_missing_target_fails_instead_of_reporting_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(FileNotFoundError):
                bench.measure_storage(root, [root / "missing"], "mbx")

    def test_kache_trace_capture_merges_real_intervals_and_rejects_partial_data(self):
        import json

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            traces = root / "kache-phases"
            traces.mkdir()
            with self.assertRaisesRegex(ValueError, "produced no traces"):
                bench.capture_kache_traces(root)
            for pid in (17, 29):
                data = {
                    "traceEvents": [
                        {"name": "unit", "pid": pid, "args": {"dropped_events": 0}}
                    ],
                    "kache_trace": {"clock": "monotonic"},
                }
                (traces / f"{pid}.trace.json").write_text(json.dumps(data))
            self.assertEqual(bench.capture_kache_traces(root), ["kache.trace.json"])
            merged = json.loads((root / "kache.trace.json").read_text())
            self.assertEqual(
                [event["pid"] for event in merged["traceEvents"]], [17, 29]
            )
            data["traceEvents"][0]["args"]["dropped_events"] = 1
            (traces / "29.trace.json").write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError, "event limit"):
                bench.capture_kache_traces(root)

    def test_batch_uses_fresh_independent_targets_and_preserves_failure_evidence(self):
        class QuietSampler:
            def __init__(self):
                self.groups = set()
                self.lock = threading.Lock()
                self.stop = threading.Event()
                self.samples = []
                self.thread = threading.Thread(target=lambda: None)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bindir = root / "bin"
            bindir.mkdir()
            cargo = bindir / "cargo"
            cargo.write_text(
                f"#!{sys.executable}\n"
                + """import json, os, pathlib, sys
target = pathlib.Path("target")
assert not (target / "old-artifact").exists(), "artifacts were not cleared"
target.mkdir(exist_ok=True)
(target / "old-artifact").write_text("artifact")
if "MBX_STATS_REPORT" in os.environ:
    cold = os.environ["TEST_RESULT"] == "miss"
    pathlib.Path(os.environ["MBX_STATS_REPORT"]).write_text(json.dumps({"hits": int(not cold), "misses": 0, "unconsulted": int(cold)}))
    sessions = pathlib.Path(os.environ["MBX_CACHE_DIR"]) / "actions" / "sessions" / "v1"
    sessions.mkdir(parents=True, exist_ok=True)
    (sessions / (pathlib.Path.cwd().name + str(cold) + ".jsonl")).write_text("session")
    sys.exit(0)
runtime = pathlib.Path(os.environ["KACHE_RUNTIME_DIR"])
runtime.mkdir(parents=True, exist_ok=True)
result = os.environ["TEST_RESULT"]
event = {"crate_name": pathlib.Path.cwd().name, "cache_key": pathlib.Path.cwd().name,
         "result": result, "compiler_runs": int(result == "miss")}
if os.environ.get("TEST_DAEMON"):
    assert (runtime / "daemon.sock").exists(), "daemon was not started"
    event["store_handed_off"] = True
    (runtime / (pathlib.Path.cwd().name + ".pending")).write_text(json.dumps(event) + "\\n")
else:
    with (runtime / "events.jsonl").open("a") as stream:
        stream.write(json.dumps(event) + "\\n")
print("ran", *sys.argv[1:])
sys.exit(int(os.environ.get("TEST_FAIL", "0")))
"""
            )
            cargo.chmod(0o755)
            kache = bindir / "kache"
            kache.write_text(
                f"#!{sys.executable}\n"
                + """import os, pathlib, sys
runtime = pathlib.Path(os.environ["KACHE_RUNTIME_DIR"])
socket = runtime / "daemon.sock"
if sys.argv[1:] == ["daemon", "start"]:
    socket.touch()
else:
    with (runtime / "events.jsonl").open("a") as stream:
        for pending in sorted(runtime.glob("*.pending")):
            stream.write(pending.read_text())
            pending.unlink()
    socket.unlink(missing_ok=True)
"""
            )
            kache.chmod(0o755)
            repos = []
            for name, _ in bench.JOBS:
                repo = root / name
                (repo / "target").mkdir(parents=True)
                (repo / "target" / "old-artifact").touch()
                repos.append(repo)
            args = SimpleNamespace(
                output=root,
                jobs_per_build=2,
                parallelism=3,
                toolchain="test",
                timeout=10,
            )
            snapshot = {"cpu": {}, "memory_events": {}}
            env = {"PATH": f"{bindir}:{os.environ['PATH']}", "TEST_RESULT": "miss"}
            with (
                patch.object(bench, "Sampler", QuietSampler),
                patch.object(bench, "machine", return_value=snapshot),
                patch.dict(os.environ, env),
            ):
                cold = bench.run_phase(
                    args,
                    kache,
                    repos,
                    root / "cache",
                    root / "runtime",
                    1,
                    "cold",
                    root / "cold",
                )
                self.assertEqual(cold["events"]["compiler_runs"], 6)
                self.assertEqual(len(cold["jobs"]), 6)
                args.daemon = True
                with patch.dict(os.environ, {"TEST_DAEMON": "1"}):
                    queued = bench.run_phase(
                        args, kache, repos, root / "cache", root / "runtime",
                        1, "cold", root / "queued",
                    )
                args.daemon = False
                self.assertEqual(queued["events"]["results"], {"miss": 6})
                self.assertEqual(queued["events"]["daemon_stores"], 6)
                self.assertFalse((root / "runtime" / "daemon.sock").exists())
                with patch.dict(os.environ, {"TEST_RESULT": "local_hit"}):
                    warm = bench.run_phase(
                        args,
                        kache,
                        repos,
                        root / "cache",
                        root / "runtime",
                        1,
                        "warm",
                        root / "warm",
                    )
                self.assertEqual(warm["events"]["results"], {"local_hit": 6})
                self.assertEqual(warm["events"]["compiler_runs"], 0)
                mbx = bindir / "mbx"
                mbx.write_text(
                    f"#!{sys.executable}\n"
                    "import os, sys\n"
                    'if sys.argv[1:3] == ["cache", "trace"]:\n'
                    '    print(\'{"traceEvents": [{"name": "key"}]}\')\n'
                    "else:\n"
                    '    os.execvp("cargo", ["cargo", *sys.argv[1:]])\n'
                )
                mbx.chmod(0o755)
                mbx_cold = bench.run_phase(
                    args,
                    mbx,
                    repos,
                    root / "mbx-cache",
                    root / "mbx-runtime",
                    1,
                    "cold",
                    root / "mbx-cold",
                    "mbx",
                )
                self.assertEqual(mbx_cold["events"]["results"]["unconsulted"], 6)
                self.assertEqual(mbx_cold["events"]["results"]["miss"], 0)
                self.assertEqual(len(mbx_cold["traces"]), 6)
                with patch.dict(os.environ, {"TEST_RESULT": "local_hit"}):
                    mbx_warm = bench.run_phase(
                        args,
                        mbx,
                        repos,
                        root / "mbx-cache",
                        root / "mbx-runtime",
                        1,
                        "warm",
                        root / "mbx-warm",
                        "mbx",
                    )
                self.assertEqual(mbx_warm["events"]["results"]["local_hit"], 6)
                self.assertEqual(len(mbx_warm["traces"]), 6)
                with (
                    patch.dict(os.environ, {"TEST_FAIL": "3"}),
                    self.assertRaisesRegex(ValueError, "Cargo job failed"),
                ):
                    bench.run_phase(
                        args,
                        kache,
                        repos,
                        root / "cache",
                        root / "runtime",
                        1,
                        "cold",
                        root / "failed",
                    )
                self.assertTrue((root / "failed" / "result.json").exists())
                self.assertTrue((root / "failed" / "events.jsonl").exists())
                self.assertIn(
                    "ran check --locked", (root / "failed" / "check.log").read_text()
                )

    def test_counts_actual_compiles_and_repeated_keys_separately_from_hits(self):
        events = [
            {
                "crate_name": "lib",
                "cache_key": "key",
                "result": "miss",
                "compiler_runs": 1,
                "permit_wait_ms": 2,
            },
            {
                "crate_name": "lib",
                "cache_key": "key",
                "result": "dup",
                "compiler_runs": 1,
                "flight_wait_ms": 4,
            },
            {
                "crate_name": "lib",
                "cache_key": "key",
                "result": "local_hit",
                "compiler_runs": 0,
                "flight_wait_ms": 9,
            },
            {
                "crate_name": "other",
                "cache_key": "other",
                "result": "miss",
                "compiler_runs": 1,
            },
            {
                "crate_name": "probe",
                "cache_key": "",
                "result": "passthrough",
                "compiler_runs": 1,
            },
            {"type": "heartbeat", "compiler_runs": 99},
        ]
        summary = bench.aggregate_events(events)
        self.assertEqual(summary["compiler_runs"], 4)
        self.assertEqual(summary["duplicate_key_compiles"], 1)
        self.assertEqual(summary["flight_wait_ms"], 13)
        self.assertEqual(summary["permit_wait_ms"], 2)
        self.assertEqual(summary["results"]["local_hit"], 1)
        self.assertEqual(summary["by_unit"]["lib"]["compiler_runs"], 2)

    def test_empty_or_heartbeat_only_events_are_invalid(self):
        for events in ([], [{"type": "heartbeat"}]):
            with self.assertRaisesRegex(ValueError, "no Kache build events"):
                bench.aggregate_events(events)

    def test_arms_share_all_controls_except_scheduler_and_binary(self):
        args = SimpleNamespace(
            output=Path("/bench"), jobs_per_build=4, toolchain="1.97.1"
        )
        inherited = {
            "CARGO_TARGET_DIR": "/stale",
            "KACHE_CACHE_DIR": "/shared",
            "RUSTC_WRAPPER": "/wrong",
            "RUSTC_WORKSPACE_WRAPPER": "/wrong",
            "CFLAGS": "-O0",
            "CC": "/wrong",
            "PATH": os.environ["PATH"],
        }
        with patch.dict(os.environ, inherited, clear=True):
            on = bench.job_environment(
                args,
                Path("/base/kache"),
                Path("/repo"),
                Path("/store"),
                Path("/runtime"),
                1,
            )
            off = bench.job_environment(
                args,
                Path("/base/kache"),
                Path("/repo"),
                Path("/store"),
                Path("/runtime"),
                0,
            )
        self.assertEqual(on["CARGO_TARGET_DIR"], "/repo/target")
        self.assertNotIn("RUSTC_WORKSPACE_WRAPPER", on)
        self.assertNotIn("CC", on)
        self.assertEqual(on["CARGO_INCREMENTAL"], "0")
        self.assertEqual(on["HOST_CC"], "/base/kache cc")
        self.assertEqual(on["CARGO_BUILD_JOBS"], "4")
        self.assertEqual(on["KACHE_VERIFY_INPUT_PREDICTIONS"], "sampled")
        self.assertEqual(on.pop("KACHE_SCHEDULER"), "1")
        self.assertEqual(off.pop("KACHE_SCHEDULER"), "0")
        self.assertEqual(on, off)

    def test_summary_keeps_arms_and_cache_phases_separate(self):
        records = []
        for arm, phase, values in [
            ("base", "cold", [100, 200, 300]),
            ("head", "cold", [80]),
            ("base", "warm", [10, 20]),
        ]:
            for value in values:
                records.append(
                    {
                        "arm": arm,
                        "phase": phase,
                        "wall_ms": value,
                        "events": {
                            key: value
                            for key in (
                                "compiler_runs",
                                "duplicate_key_compiles",
                                "flight_wait_ms",
                                "permit_wait_ms",
                                "key_ms",
                                "dep_info_ms",
                            )
                        },
                    }
                )
        rows = {(r["arm"], r["phase"]): r for r in bench.summarize(records)}
        self.assertEqual(rows["base", "cold"]["n"], 3)
        self.assertEqual(rows["base", "cold"]["mean_ms"], 200)
        self.assertEqual(rows["base", "warm"]["median_ms"], 15)
        self.assertEqual(rows["head", "cold"]["min_ms"], 80)

    def test_backend_environments_and_counts_do_not_invent_kache_work(self):
        args = SimpleNamespace(
            output=Path("/bench"), jobs_per_build=4, toolchain="test"
        )
        repos = [Path("/one"), Path("/two")]
        with patch.dict(
            os.environ, {"SCCACHE_BUCKET": "unwanted-remote", "MBX_CACHE_DIR": "/stale"}
        ):
            env = bench.job_environment(
                args,
                Path("/sccache"),
                repos[0],
                Path("/store"),
                Path("/runtime"),
                1,
                "sccache",
                repos,
            )
            self.assertEqual(
                env["SCCACHE_BASEDIRS"], os.pathsep.join(str(r) for r in repos)
            )
            self.assertNotIn("SCCACHE_BUCKET", env)
            self.assertNotIn("KACHE_CACHE_DIR", env)
            env = bench.job_environment(
                args,
                Path("/mbx"),
                repos[0],
                Path("/store"),
                Path("/runtime"),
                1,
                "mbx",
                repos,
            )
            self.assertNotIn("RUSTC_WRAPPER", env)
            self.assertNotIn("HOST_CC", env)
            self.assertEqual(env["CARGO_TARGET_DIR"], "/one/target")
        counts = bench.external_counts(
            "mbx", [{"hits": 3, "misses": 2}, {"hits": 4, "misses": 1}]
        )
        self.assertEqual(
            counts["results"], {"local_hit": 7, "miss": 3, "unconsulted": 0, "error": 0}
        )
        self.assertNotIn("compiler_runs", counts)
        with self.assertRaises(KeyError):
            bench.external_counts("mbx", [{}])
        counts = bench.external_counts(
            "sccache",
            [
                {
                    "stats": {
                        "cache_hits": {"counts": {"Rust": 3, "C": 4}},
                        "cache_misses": {"counts": {"Rust": 1}},
                        "cache_errors": {"counts": {}},
                        "cache_write_errors": 2,
                    }
                }
            ],
        )
        self.assertEqual(counts["results"], {"local_hit": 7, "miss": 1, "error": 2})

    def test_otlp_keeps_contention_separate_and_marks_invalid_runs(self):
        data = {
            "project": "hk",
            "revision": "sha",
            "arms": [
                {"name": "sccache", "backend": "sccache", "version": "sccache test"}
            ],
            "records": [
                {
                    "arm": "sccache",
                    "phase": "warm",
                    "time_ns": 123,
                    "wall_ms": 1500,
                    "events": {"results": {"local_hit": 7, "miss": 1}},
                }
            ],
        }
        metrics = bench.otlp(data)["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
        duration = metrics[0]["gauge"]["dataPoints"][0]
        self.assertEqual(duration["asDouble"], 1.5)
        attrs = {a["key"]: a["value"]["stringValue"] for a in duration["attributes"]}
        self.assertEqual(attrs["kache.bench.project"], "bench-hk-contention")
        self.assertEqual(attrs["kache.bench.phase"], "warm")
        self.assertEqual(attrs["kache.bench.cache_tool"], "sccache")
        data["error"] = "warm build failed"
        invalid = bench.otlp(data)["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][
            -1
        ]
        self.assertEqual(invalid["gauge"]["dataPoints"][0]["asDouble"], 0)
        data["records"][0]["storage"] = {
            "groups": {
                "cache_and_targets": {
                    "logical_bytes": 30,
                    "unique_logical_bytes": 20,
                    "allocated_bytes": 4096,
                }
            }
        }
        metrics = bench.otlp(data)["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
        allocated = next(m for m in metrics if m["name"] == "kache.bench.storage.allocated_bytes")
        point = allocated["gauge"]["dataPoints"][0]
        self.assertEqual(point["asDouble"], 4096)
        self.assertIn(
            {"key": "kache.bench.storage_scope", "value": {"stringValue": "cache_and_targets"}},
            point["attributes"],
        )

    def test_mbx_trace_capture_preserves_only_the_current_phase(self):
        import json

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sessions, dest = root / "sessions", root / "phase"
            sessions.mkdir()
            dest.mkdir()
            old = sessions / "old.jsonl"
            old.write_text("old session")
            current = sessions / "current.jsonl"
            current.write_text("current session")
            with patch.object(
                bench,
                "capture",
                return_value=json.dumps({"traceEvents": [{"name": "key"}]}),
            ) as export:
                traces = bench.capture_mbx_traces(Path("/mbx"), sessions, {old}, dest)
            self.assertEqual(traces, ["mbx-sessions/current.trace.json"])
            self.assertFalse((dest / "mbx-sessions" / "old.jsonl").exists())
            self.assertEqual(
                (dest / "mbx-sessions" / "current.jsonl").read_text(), "current session"
            )
            self.assertEqual(export.call_args.args[0][:3], ["/mbx", "cache", "trace"])

    def test_repeated_warm_batches_restore_cold_snapshots_and_alternate_arms(self):
        for cold_every, samples in ((1, 3), (3, 6)):
            with (
                self.subTest(cold_every=cold_every),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                work = root / "work"
                mirror = work / "source"
                mirror.mkdir(parents=True)
                args = SimpleNamespace(
                    output=root,
                    project="hk",
                    samples=samples,
                    cold_every=cold_every,
                    order_seed=0,
                    keep_work=False,
                )
                arms = [
                    ("base", Path("/base/kache"), 1, "kache"),
                    ("head", Path("/head/kache"), 1, "kache"),
                ]
                data = {"records": []}

                def clone(command, **kwargs):
                    Path(command[-1]).mkdir()

                def phase(
                    args, binary, repos, store, runtime, scheduler, phase, dest, backend
                ):
                    for repo in repos:
                        (repo / "target").mkdir(exist_ok=True)
                    if phase == "cold":
                        self.assertFalse(store.exists())
                        store.mkdir()
                        (store / "seed").write_text(store.parent.name)
                        locked = store / "out"
                        locked.mkdir()
                        (locked / "obj").write_text("cached")
                        (locked / "obj").chmod(0o444)
                        locked.chmod(0o555)
                    else:
                        self.assertEqual(
                            (store / "seed").read_text(), store.parent.name
                        )
                        self.assertFalse(
                            (store / "warm-learning").exists(),
                            "a warm sample inherited the previous warm sample",
                        )
                        (store / "warm-learning").touch()
                    return {"phase": phase}

                with (
                    patch.object(bench.subprocess, "run", clone),
                    patch.object(bench, "run_phase", phase),
                    patch.object(bench, "write_report"),
                ):
                    bench.run_batches(args, arms, mirror, work, data)
                records = data["records"]
                for arm in ("base", "head"):
                    self.assertEqual(
                        [
                            r["sample"]
                            for r in records
                            if r["arm"] == arm and r["phase"] == "cold"
                        ],
                        list(range(0, samples, cold_every)),
                    )
                    self.assertEqual(
                        [
                            r["sample"]
                            for r in records
                            if r["arm"] == arm and r["phase"] == "warm"
                        ],
                        list(range(samples)),
                    )
                for sample in range(samples):
                    expected = ["base", "head"] if sample % 2 == 0 else ["head", "base"]
                    self.assertEqual(
                        [
                            r["arm"]
                            for r in records
                            if r["sample"] == sample and r["phase"] == "warm"
                        ],
                        expected,
                    )
                self.assertEqual(list(work.iterdir()), [mirror])

    def test_remove_owned_tree_deletes_nested_readonly_directories(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "cache"
            nested = root / "out" / "obj"
            nested.mkdir(parents=True)
            artifact = nested / "artifact"
            artifact.write_bytes(b"cached")
            artifact.chmod(0o444)
            nested.chmod(0o555)
            (root / "out").chmod(0o555)
            bench.remove_owned_tree(root)
            self.assertFalse(root.exists())

    def test_remove_owned_tree_unlinks_symlinks_without_following_them(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            outside = root / "outside"
            outside.mkdir()
            secret = outside / "keep"
            secret.write_text("keep")
            secret.chmod(0o444)
            cache = root / "cache"
            cache.mkdir()
            (cache / "link").symlink_to(outside, target_is_directory=True)
            bench.remove_owned_tree(cache)
            self.assertFalse(cache.exists())
            self.assertEqual(secret.read_text(), "keep")
            self.assertEqual(stat.S_IMODE(secret.stat().st_mode), 0o444)

    def test_remove_owned_tree_does_not_chmod_hardlinked_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cache = root / "cache"
            cache.mkdir()
            blob = cache / "blob"
            blob.write_bytes(b"shared")
            blob.chmod(0o444)
            sibling = root / "sibling"
            os.link(blob, sibling)
            before = stat.S_IMODE(sibling.stat().st_mode)
            bench.remove_owned_tree(cache)
            self.assertFalse(cache.exists())
            self.assertEqual(sibling.read_bytes(), b"shared")
            self.assertEqual(stat.S_IMODE(sibling.stat().st_mode), before)

    def test_remove_owned_tree_raises_when_the_path_cannot_be_removed(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory) / "cache"
            cache.mkdir()
            with patch.object(bench.shutil, "rmtree", lambda *args, **kwargs: None):
                with self.assertRaises(FileExistsError):
                    bench.remove_owned_tree(cache)
            self.assertTrue(cache.exists())

    def test_short_eza_workload_retains_six_jobs_and_the_full_graph_is_explicit(self):
        short = bench.workload_for("eza")
        self.assertEqual(len(short), 6)
        self.assertEqual(
            sum("--no-default-features" in command for _, command in short), 3
        )
        self.assertFalse(any("--all-features" in command for _, command in short))
        self.assertEqual(bench.workload_for("eza", full_features=True), bench.JOBS)
        self.assertEqual(bench.workload_for("hk"), bench.JOBS)

    def test_cgroup_counter_units_remain_raw_and_missing_is_unknown(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cpu.stat"
            self.assertEqual(bench.counters(path), {})
            path.write_text("usage_usec 999\nnr_throttled 7\nthrottled_usec 312\n")
            self.assertEqual(
                bench.counters(path),
                {"usage_usec": 999, "nr_throttled": 7, "throttled_usec": 312},
            )

    def test_event_window_rejects_rotation_even_when_the_log_has_regrown(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            path.write_bytes(b"old event\n")
            prefix, offset = path.read_bytes(), path.stat().st_size
            path.write_bytes(b"replacement event has more bytes\n")
            with self.assertRaisesRegex(ValueError, "rotated"):
                bench.event_window(path, offset, prefix)
            path.write_bytes(b"short\n")
            with self.assertRaisesRegex(ValueError, "rotated"):
                bench.event_window(path, offset, prefix)
            path.write_bytes(prefix + b"new event\n")
            self.assertEqual(bench.event_window(path, offset, prefix), "new event\n")
            stat = path.stat()
            replacement = path.with_suffix(".new")
            replacement.write_bytes(path.read_bytes())
            replacement.replace(path)
            with self.assertRaisesRegex(ValueError, "rotated"):
                bench.event_window(path, 0, b"", (stat.st_dev, stat.st_ino))


if __name__ == "__main__":
    unittest.main()
