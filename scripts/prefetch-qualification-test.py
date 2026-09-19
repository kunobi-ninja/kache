#!/usr/bin/env python3
"""Admission oracles; no builds, network, credentials, or daemon required."""

import copy
import importlib.util
import io
import json
import os
import tarfile
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "qualification", Path(__file__).with_name("prefetch-qualification.py")
)
q = importlib.util.module_from_spec(spec)
spec.loader.exec_module(q)


class Policy(unittest.TestCase):
    def test_only_protected_main_push_can_seed(self):
        env = {
            "GITHUB_ACTIONS": "true",
            "GITHUB_EVENT_NAME": "push",
            "GITHUB_REF": "refs/heads/main",
            "GITHUB_REF_TYPE": "branch",
            "GITHUB_REF_PROTECTED": "true",
        }
        self.assertTrue(q.trusted_push(env))
        for key, values in {
            "GITHUB_ACTIONS": ["", "false"],
            "GITHUB_EVENT_NAME": ["workflow_dispatch", "pull_request", "schedule"],
            "GITHUB_REF": ["refs/heads/other", "refs/tags/v1"],
            "GITHUB_REF_TYPE": ["tag"],
            "GITHUB_REF_PROTECTED": ["false", ""],
        }.items():
            for value in values:
                self.assertFalse(q.trusted_push(env | {key: value}), (key, value))

    def test_real_ci_environment_survives_isolation(self):
        original = {
            "CI": "true",
            "GITHUB_ACTIONS": "true",
            "GITHUB_REF_PROTECTED": "false",
            "GITHUB_EVENT_NAME": "workflow_dispatch",
            "KACHE_PLANNER_ENDPOINT": "forbidden",
            "KACHE_REMOTE": "forbidden",
            "RUSTC_WRAPPER": "old",
            "CC": "old",
        }
        with patch.dict(os.environ, original, clear=True):
            env = q.environment(Path("/scratch"), Path("/scratch/kache"))
        for name in (
            "CI",
            "GITHUB_ACTIONS",
            "GITHUB_REF_PROTECTED",
            "GITHUB_EVENT_NAME",
        ):
            self.assertEqual(env[name], original[name])
        self.assertNotIn("KACHE_PLANNER_ENDPOINT", env)
        self.assertNotIn("KACHE_REMOTE", env)
        self.assertNotIn("CC", env)
        self.assertEqual(env["KACHE_HOST_CONFIG"], "")

    def test_manual_seed_admission_requires_trusted_provenance(self):
        run = {
            "event": "push",
            "head_branch": "main",
            "path": q.WORKFLOW,
            "status": "completed",
            "repository": {"full_name": "kunobi-ninja/kache"},
        }
        self.enterContext(
            patch.dict(os.environ, {"GITHUB_REPOSITORY": "kunobi-ninja/kache"})
        )
        jobs = [{"name": "seed", "conclusion": "success"}]
        q.authorize_seed(run, jobs, {"status": "ahead"})
        for key, value in (
            ("event", "pull_request"),
            ("path", "other.yml"),
            ("head_branch", "topic"),
            ("status", "in_progress"),
        ):
            with self.assertRaises(ValueError):
                q.authorize_seed(run | {key: value}, jobs, {"status": "identical"})
        with self.assertRaises(ValueError):
            q.authorize_seed(run, jobs, {"status": "diverged"})
        with self.assertRaises(ValueError):
            q.authorize_seed(
                run, [{"name": "seed", "conclusion": "failure"}], {"status": "ahead"}
            )


class Integrity(unittest.TestCase):
    def test_seed_corruption_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle = root / "bundle"
            bundle.mkdir()
            (bundle / "kache").write_bytes(b"binary")
            manifest = {
                "project": q.PROJECT,
                "toolchain": q.TOOLCHAIN,
                "trusted_push": True,
                "files": q.files(bundle),
            }
            q.dump(bundle / "manifest.json", manifest)
            archive = root / "seed.tar.gz"
            with tarfile.open(archive, "w:gz") as tar:
                for path in bundle.iterdir():
                    tar.add(path, arcname=path.name)
            self.assertEqual(q.unpack(archive, root / "good"), manifest)
            (bundle / "kache").write_bytes(b"changed")
            with tarfile.open(archive, "w:gz") as tar:
                for path in bundle.iterdir():
                    tar.add(path, arcname=path.name)
            with self.assertRaisesRegex(ValueError, "hash mismatch"):
                q.unpack(archive, root / "bad")

    def test_archive_traversal_and_links_are_rejected(self):
        for name, kind in (
            ("../outside", tarfile.REGTYPE),
            ("/outside", tarfile.REGTYPE),
            ("link", tarfile.SYMTYPE),
            ("hardlink", tarfile.LNKTYPE),
        ):
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                with tarfile.open(root / "bad.tar.gz", "w:gz") as tar:
                    item = tarfile.TarInfo(name)
                    item.type = kind
                    tar.addfile(item, io.BytesIO())
                with self.assertRaises(ValueError):
                    q.unpack(root / "bad.tar.gz", root / "unpacked")


def record():
    return {
        "schema": 3,
        "session_id": "session",
        "units": [
            {
                "cache_key": "key",
                "result": "local_hit",
                "event_schema": 20,
                "demands": [
                    {"cache_key": "key", "first_demand_at_ms": 100, "remote_wait_ms": 7}
                ],
            }
        ],
        "transfers": [
            {
                "cache_key": "key",
                "direction": "download",
                "ok": True,
                "original_bytes": 50,
                "compressed_bytes": 30,
                "started_at_ms": 10,
                "finished_at_ms": 90,
                "outcome": "completed",
                "prefetch": {
                    "session_id": "session",
                    "plan_id": "",
                    "source": "fallback",
                },
            }
        ],
    }


class Telemetry(unittest.TestCase):
    def test_best_prefetched_local_hit_counts(self):
        result = q.summarize([record()], True)
        self.assertEqual(
            result["speculative_compressed_bytes"], {"useful_before_demand": 30}
        )
        self.assertEqual(result["unit_outcomes"], {"local_hit": 1})
        self.assertEqual(result["remote_wait_ms"], 7)

    def test_equal_inflight_late_unused_and_failure_stay_separate(self):
        for demand, start, end, ok, expected in (
            (100, 10, 100, True, "equal_timestamp"),
            (100, 10, 110, True, "inflight_at_demand"),
            (100, 101, 110, True, "late"),
            (101, 10, 90, True, "unused"),
            (100, 10, 90, False, "failed"),
        ):
            rec = record()
            transfer = rec["transfers"][0]
            transfer.update(started_at_ms=start, finished_at_ms=end, ok=ok)
            if demand == 101:
                transfer["cache_key"] = "unused-key"
            # Keep an independent successful demand download for admission.
            rec["transfers"].append(dict(transfer, prefetch=None, ok=True))
            self.assertEqual(
                q.summarize([rec], True)["speculative_compressed_bytes"], {expected: 30}
            )

    def test_first_demand_wins_and_duplicate_transfer_is_not_double_counted(self):
        rec = record()
        rec["units"][0]["demands"].append(
            {"cache_key": "key", "first_demand_at_ms": 200, "remote_wait_ms": 0}
        )
        rec["transfers"].append(copy.deepcopy(rec["transfers"][0]))
        result = q.summarize([rec], True)
        self.assertEqual(result["speculative_attempts"], 1)
        self.assertEqual(result["speculative"][0]["first_demand_at_ms"], 100)

    def test_origin_session_cannot_borrow_another_sessions_demand(self):
        rec = record()
        rec["transfers"][0]["prefetch"]["session_id"] = "other-session"
        result = q.summarize([rec], True)
        self.assertEqual(result["recorded_useful_prefetch_bytes"], 0)
        self.assertEqual(result["speculative_compressed_bytes"], {"unknown_demand": 30})
        other = copy.deepcopy(rec)
        other.update(session_id="other-session", units=[], transfers=[])
        result = q.summarize([rec, other], True)
        self.assertEqual(result["speculative_compressed_bytes"], {"unused": 30})

    def test_useful_credit_requires_actual_same_key_consumption(self):
        rec = record()
        rec["units"][0]["result"] = "miss"
        other = copy.deepcopy(rec["units"][0])
        other.update(cache_key="other", result="local_hit")
        other["demands"][0]["cache_key"] = "other"
        rec["units"].append(other)
        result = q.summarize([rec], True)
        self.assertEqual(result["recorded_useful_prefetch_bytes"], 0)
        self.assertEqual(
            result["speculative_compressed_bytes"], {"demanded_unconsumed": 30}
        )

    def test_duplicate_and_failed_import_bytes_remain_in_denominator(self):
        rec = record()
        duplicate = dict(rec["transfers"][0], started_at_ms=20, finished_at_ms=95)
        failure = dict(
            rec["transfers"][0],
            started_at_ms=30,
            finished_at_ms=98,
            compressed_bytes=40,
            original_bytes=0,
            ok=False,
            outcome="import_error",
        )
        rec["transfers"].extend([duplicate, failure])
        result = q.summarize([rec], True)
        self.assertEqual(result["recorded_received_prefetch_bytes"], 100)
        self.assertEqual(result["recorded_useful_prefetch_bytes"], 30)
        self.assertEqual(result["ordinary_recorded_byte_precision"], 0.3)
        self.assertEqual(
            result["speculative_compressed_bytes"],
            {"useful_before_demand": 30, "duplicate": 30, "failed": 40},
        )
        self.assertFalse(result["complete_precision_qualification"])

    def test_missing_keyed_demand_or_future_transfer_schema_fails(self):
        for change in ("missing", "wrong-key", "new-schema", "nested-entries"):
            rec = record()
            if change == "missing":
                rec["units"][0]["demands"] = []
            elif change == "wrong-key":
                rec["units"][0]["demands"][0]["cache_key"] = "other"
            elif change == "new-schema":
                rec["schema"] = 5
            else:
                rec["transfers"][0]["entries"] = []
            with self.assertRaises(ValueError):
                q.summarize([rec], True)

    def test_invalid_or_unexercised_arms_fail(self):
        with self.assertRaises(ValueError):
            q.summarize([record()], False)
        rec = record()
        rec["transfers"][0]["prefetch"] = None
        q.summarize([rec], False)
        with self.assertRaises(ValueError):
            q.summarize([rec], True)
        for change in ({"schema": 2}, {"units": []}, {"transfers": []}):
            with self.assertRaises(ValueError):
                q.summarize([record() | change], True)


class Lifecycle(unittest.TestCase):
    def record(self, **changes):
        rec = record()
        rec.update(
            schema=4,
            summary={"incomplete": False, "closure_reason": "shutdown"} | changes,
        )
        return rec

    def test_reviewed_schema4_preserves_ordinary_precision(self):
        result = q.summarize([self.record()], True)
        self.assertEqual(result["recorded_useful_prefetch_bytes"], 30)
        self.assertEqual(result["lifecycle"]["timeline_schemas"], [4])
        self.assertEqual(
            result["lifecycle"]["summaries"]["session"]["closure_reason"], "shutdown"
        )
        self.assertFalse(result["lifecycle"]["legacy_shutdown_evidence_unknown"])
        self.assertFalse(result["complete_precision_qualification"])

    def test_incomplete_or_timed_out_shutdown_is_not_admitted(self):
        for changes in ({"incomplete": True}, {"closure_reason": "shutdown_timeout"}):
            rec = self.record(**changes)
            evidence = q.lifecycle_evidence([rec])
            self.assertTrue(evidence["problems"])
            self.assertEqual(evidence["summaries"]["session"], rec["summary"])
            with self.assertRaises(ValueError):
                q.summarize([rec], True)

    def test_missing_or_malformed_schema4_summary_is_not_admitted(self):
        for summary in (None, {}, {"incomplete": "false"}):
            rec = self.record()
            rec["summary"] = summary
            with self.assertRaises(ValueError):
                q.summarize([rec], True)

    def test_off_arm_without_plan_needs_no_summary(self):
        rec = self.record()
        rec["summary"] = None
        rec["transfers"][0]["prefetch"] = None
        result = q.summarize([rec], False)
        self.assertFalse(result["lifecycle"]["problems"])

    def test_raw_summary2_incomplete_cannot_hide_behind_newest_complete_summary(self):
        raw = {
            "schema": 2,
            "session_id": "session",
            "incomplete": True,
            "closure_reason": "shutdown",
        }
        result = q.lifecycle_evidence([self.record()], [raw])
        self.assertEqual(result["raw_summary_schemas"], [2])
        self.assertTrue(result["problems"])
        self.assertTrue(
            q.lifecycle_evidence(
                [self.record()], [raw | {"schema": 1, "incomplete": False}]
            )["problems"]
        )
        for bad in (raw | {"schema": 3}, raw | {"incomplete": None}):
            with self.assertRaises(ValueError):
                q.lifecycle_evidence([self.record()], [bad])

    def test_schema3_stays_explicitly_unknown_and_packed_fields_stay_blocked(self):
        self.assertTrue(
            q.summarize([record()], True)["lifecycle"][
                "legacy_shutdown_evidence_unknown"
            ]
        )
        rec = self.record()
        rec["transfers"][0]["entries"] = []
        with self.assertRaises(ValueError):
            q.summarize([rec], True)


class Timing(unittest.TestCase):
    def jobs(self):
        return [
            {
                "name": f"{arm} / consume",
                "status": "completed",
                "conclusion": "success",
                "started_at": f"2026-09-19T00:{i * 2:02}:00Z",
                "completed_at": f"2026-09-19T00:{i * 2 + 1:02}:00Z",
                "html_url": "job",
            }
            for i, arm in enumerate(q.ARMS)
        ]

    def test_full_job_time_comes_from_github(self):
        result, problems = q.job_durations(self.jobs())
        self.assertFalse(problems)
        self.assertEqual([v["seconds"] for v in result.values()], [60] * 6)

    def test_missing_duplicate_incomplete_or_overlapping_jobs_fail(self):
        jobs = self.jobs()
        for bad in (
            jobs[:-1],
            jobs + [jobs[0]],
            [jobs[0] | {"status": "in_progress"}] + jobs[1:],
            [jobs[0] | {"completed_at": "2026-09-19T00:03:00Z"}] + jobs[1:],
        ):
            with self.assertRaises(ValueError):
                q.job_durations(bad)

    def test_failed_and_skipped_jobs_retain_partial_timings(self):
        jobs = self.jobs()
        jobs[0]["conclusion"] = "failure"
        jobs[1].update(conclusion="skipped", started_at=None, completed_at=None)
        result, problems = q.job_durations(jobs, strict=False)
        self.assertEqual(result["off-1"]["seconds"], 60)
        self.assertEqual(result["off-1"]["conclusion"], "failure")
        self.assertIsNone(result["on-1"]["seconds"])
        self.assertTrue(problems)

    def test_collector_persists_identity_and_refuses_partial_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args = SimpleNamespace(
                output=root / "output", results=root / "results", seed_run_id="123"
            )
            for arm in q.ARMS:
                folder = args.results / ("prefetch-qualification-" + arm)
                q.dump(
                    folder / "identity.json",
                    {"files": {"kache": "binary-sha"}, "project": q.PROJECT},
                )
                q.dump(folder / "artifact.json", {"sha256": "artifact-sha"})
                q.dump(folder / "admission.json", {"demanded_keys": 2})
            with (
                patch.object(q, "jobs", return_value=self.jobs()),
                patch.dict(
                    os.environ, {"GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "2"}
                ),
            ):
                q.collect(args)
                report = json.loads((args.output / "job-times.json").read_text())
                self.assertTrue(report["controls_valid"])
                self.assertEqual(report["run_attempt"], "2")
                self.assertEqual(
                    report["identities"]["off-1"]["files"]["kache"], "binary-sha"
                )
                (
                    args.results / "prefetch-qualification-on-3" / "admission.json"
                ).unlink()
                with self.assertRaises(ValueError):
                    q.collect(args)
                report = json.loads((args.output / "job-times.json").read_text())
                self.assertFalse(report["controls_valid"])
                self.assertEqual(report["consumers"]["on-3"]["seconds"], 60)
                self.assertTrue(report["problems"])
                q.dump(
                    args.results / "prefetch-qualification-on-3" / "lifecycle.json",
                    {
                        "problems": ["session: shutdown timed out"],
                        "summaries": {"session": {"incomplete": True}},
                    },
                )
                with self.assertRaises(ValueError):
                    q.collect(args)
                report = json.loads((args.output / "job-times.json").read_text())
                self.assertIn("on-3: session: shutdown timed out", report["problems"])
                self.assertTrue(
                    report["lifecycle"]["on-3"]["summaries"]["session"]["incomplete"]
                )

    def test_job_api_paginates(self):
        with patch.object(
            q, "api", side_effect=[{"jobs": [1] * 100}, {"jobs": [2]}]
        ) as api:
            self.assertEqual(len(q.jobs("123", "2")), 101)
            self.assertIn("attempts/2/jobs?per_page=100&page=2", api.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
