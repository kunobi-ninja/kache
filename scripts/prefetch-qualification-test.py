#!/usr/bin/env python3
"""Admission oracles; no builds, network, credentials, or daemon required."""

import copy
import importlib.util
import io
import json
import os
import sys
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
        "client_record_id": "record",
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

    def test_first_demand_wins_and_identical_physical_transfers_both_count(self):
        rec = record()
        rec["units"][0]["demands"].append(
            {"cache_key": "key", "first_demand_at_ms": 200, "remote_wait_ms": 0}
        )
        rec["transfers"].append(copy.deepcopy(rec["transfers"][0]))
        result = q.summarize([rec], True)
        self.assertEqual(result["speculative_attempts"], 2)
        self.assertEqual(result["recorded_received_prefetch_bytes"], 60)
        self.assertEqual(result["recorded_useful_prefetch_bytes"], 30)
        self.assertEqual(result["speculative"][0]["first_demand_at_ms"], 100)

    def test_origin_session_cannot_borrow_another_sessions_demand(self):
        rec = record()
        rec["transfers"][0]["prefetch"]["session_id"] = "other-session"
        result = q.summarize([rec], True)
        self.assertEqual(result["recorded_useful_prefetch_bytes"], 0)
        self.assertEqual(result["speculative_compressed_bytes"], {"unknown_demand": 30})
        other = copy.deepcopy(rec)
        other.update(
            session_id="other-session",
            client_record_id="other-record",
            units=[],
            transfers=[],
        )
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
                rec["schema"] = 7
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


class PackedAccounting(unittest.TestCase):
    def inputs(self):
        rec = record()
        rec.update(
            schema=5,
            summary={
                "incomplete": False,
                "closure_reason": "shutdown",
                "plan_id": "",
                "plan_source": "fallback",
            },
        )
        ordinary = rec["transfers"][0]
        entry = {
            key: copy.deepcopy(ordinary[key])
            for key in (
                "cache_key",
                "compressed_bytes",
                "finished_at_ms",
                "outcome",
                "prefetch",
            )
        }
        entry["crate_name"] = "example"
        pack = dict(
            ordinary,
            cache_key="",
            compressed_bytes=100,
            original_bytes=200,
            accounting={
                "operation": "get",
                "bytes_complete": True,
                "requests_complete": True,
                "entries": [entry],
            },
            request_count=1,
        )
        catalog = dict(
            pack,
            compressed_bytes=10,
            original_bytes=0,
            finished_at_ms=20,
            accounting=dict(pack["accounting"], entries=[]),
        )
        listing = dict(
            pack,
            compressed_bytes=0,
            original_bytes=0,
            finished_at_ms=5,
            accounting={
                "operation": "list",
                "bytes_complete": False,
                "requests_complete": True,
                "list_result_count": 4,
            },
        )
        rec["transfers"] = [listing, catalog, pack]
        summary = dict(rec["summary"], schema=2, session_id="session")
        return rec, [summary]

    def raw(self, transfers):
        raw = copy.deepcopy(transfers)
        for transfer in raw:
            transfer["schema"] = 5
            for name in ("started_at", "finished_at"):
                transfer[name + "_unix_ms"] = transfer.pop(name + "_ms")
        return raw

    def summarize(self, rec, summaries, raw=None):
        return q.summarize(
            [rec], True, self.raw(rec["transfers"]) if raw is None else raw, summaries
        )

    def test_physical_body_denominator_excludes_nested_double_count_and_list_size(self):
        rec, summaries = self.inputs()
        result = self.summarize(rec, summaries)
        self.assertTrue(result["complete_precision_qualification"])
        self.assertEqual(result["recorded_received_prefetch_bytes"], 110)
        self.assertEqual(result["recorded_useful_prefetch_bytes"], 30)
        self.assertEqual(result["get_body_byte_precision"], 30 / 110)
        plan = result["plans"][0]
        self.assertEqual(plan["get_backend_invocations"], 2)
        self.assertEqual(plan["list_backend_invocations"], 1)
        self.assertEqual(plan["list_result_count"], 4)
        self.assertEqual(plan["list_response_bytes_unknown"], 1)
        self.assertEqual(result["demand_waits"][0]["observed_remote_wait_ms"], 7)

    def test_identical_physical_pack_operations_both_count_but_useful_key_once(self):
        rec, summaries = self.inputs()
        rec["transfers"].append(copy.deepcopy(rec["transfers"][-1]))
        result = self.summarize(rec, summaries)
        self.assertTrue(result["complete_precision_qualification"])
        self.assertEqual(result["recorded_received_prefetch_bytes"], 210)
        self.assertEqual(result["recorded_useful_prefetch_bytes"], 30)
        self.assertEqual(result["plans"][0]["payload_bytes"]["duplicate"], 30)
        self.assertEqual(result["plans"][0]["get_backend_invocations"], 3)

    def test_per_plan_groups_do_not_borrow_denominator_or_credit_same_key_twice(self):
        rec, summaries = self.inputs()
        second = copy.deepcopy(rec["transfers"][-1])
        second["prefetch"]["plan_id"] = "second"
        second["accounting"]["entries"][0]["prefetch"]["plan_id"] = "second"
        second["accounting"]["entries"][0]["finished_at_ms"] = 95
        rec["transfers"].append(second)
        summaries.append(dict(summaries[0], plan_id="second"))
        result = self.summarize(rec, summaries)
        by_plan = {p["plan_id"]: p for p in result["plans"]}
        self.assertEqual(by_plan[""]["get_received_body_bytes"], 110)
        self.assertEqual(by_plan["second"]["get_received_body_bytes"], 100)
        self.assertEqual(by_plan["second"]["useful_payload_bytes"], 0)
        self.assertTrue(result["complete_precision_qualification"])

    def test_pre_followup_ordinary_schema5_receipt_stays_incomplete(self):
        rec, summaries = self.inputs()
        ordinary = record()["transfers"][0]
        rec["transfers"] = [ordinary]
        result = self.summarize(rec, summaries)
        self.assertFalse(result["complete_precision_qualification"])
        self.assertEqual(result["recorded_received_prefetch_bytes"], 30)
        self.assertEqual(result["recorded_useful_prefetch_bytes"], 30)
        self.assertIsNone(result["get_body_byte_precision"])

    def test_partial_get_or_unknown_receipt_or_request_count_denies_completeness(self):
        for change in ("partial", "missing", "requests"):
            rec, summaries = self.inputs()
            if change == "missing":
                rec["transfers"][-1]["accounting"] = None
            else:
                rec["transfers"][-1]["accounting"][
                    "bytes_complete" if change == "partial" else "requests_complete"
                ] = False
            result = self.summarize(rec, summaries)
            self.assertFalse(result["complete_precision_qualification"])
            self.assertIsNone(result["get_body_byte_precision"])
            self.assertTrue(result["plans"][0]["problems"])

    def test_received_failed_import_and_zero_byte_404_and_cancel_are_accounted(self):
        rec, summaries = self.inputs()
        base = rec["transfers"][-1]
        for outcome, size, calls in (
            ("import_error", 40, 1),
            ("not_found", 0, 1),
            ("cancelled", 0, 0),
        ):
            rec["transfers"].append(
                dict(
                    base,
                    outcome=outcome,
                    ok=False,
                    compressed_bytes=size,
                    request_count=calls,
                    original_bytes=0,
                    accounting=dict(base["accounting"], entries=[]),
                )
            )
        result = self.summarize(rec, summaries)
        self.assertTrue(result["complete_precision_qualification"])
        self.assertEqual(result["recorded_received_prefetch_bytes"], 150)
        self.assertEqual(result["plans"][0]["get_backend_invocations"], 4)
        self.assertEqual(
            result["plans"][0]["get_outcomes"],
            {"completed": 2, "import_error": 1, "not_found": 1, "cancelled": 1},
        )

    def test_nested_origin_mismatch_or_normal_closure_denies_completeness(self):
        for change in ("origin", "closure"):
            rec, summaries = self.inputs()
            if change == "origin":
                rec["transfers"][-1]["accounting"]["entries"][0]["prefetch"][
                    "plan_id"
                ] = "other"
            else:
                summaries[0]["closure_reason"] = "inactivity"
                rec["summary"]["closure_reason"] = "inactivity"
            result = self.summarize(rec, summaries)
            self.assertFalse(result["complete_precision_qualification"])

    def test_unprojected_raw_receipt_is_retained_as_its_own_unknown_group(self):
        rec, summaries = self.inputs()
        raw = self.raw(rec["transfers"])
        extra = copy.deepcopy(raw[1])
        extra["prefetch"].update(session_id="", plan_id="", source="unscoped")
        extra["compressed_bytes"] = 17
        raw.append(extra)
        result = self.summarize(rec, summaries, raw)
        self.assertFalse(result["complete_precision_qualification"])
        self.assertEqual(result["recorded_received_prefetch_bytes"], 127)
        self.assertEqual(len(result["receipt_coverage"]["unprojected"]), 1)
        self.assertEqual(result["plans"][1]["source"], "unscoped")
        self.assertEqual(result["plans"][1]["useful_payload_bytes"], 0)

    def test_reconciliation_is_a_multiset_and_duplicate_snapshots_are_rejected(self):
        rec, summaries = self.inputs()
        raw = self.raw(rec["transfers"])
        rec["transfers"].append(copy.deepcopy(rec["transfers"][-1]))
        result = self.summarize(rec, summaries, raw)
        self.assertFalse(result["complete_precision_qualification"])
        self.assertEqual(len(result["receipt_coverage"]["projected_without_raw"]), 1)
        with self.assertRaises(ValueError):
            q.summarize([rec, copy.deepcopy(rec)], True, raw, summaries)

    def test_background_unscoped_list_is_separate_and_does_not_invalidate_get_precision(
        self,
    ):
        rec, summaries = self.inputs()
        raw = self.raw(rec["transfers"])
        background = copy.deepcopy(raw[0])
        background["prefetch"].update(session_id="", plan_id="", source="key_cache")
        raw.append(background)
        result = self.summarize(rec, summaries, raw)
        self.assertTrue(result["complete_precision_qualification"])
        self.assertFalse(result["receipt_coverage"]["complete"])
        self.assertTrue(result["receipt_coverage"]["get_complete"])
        self.assertEqual(result["plans"][1]["source"], "key_cache")
        self.assertEqual(result["plans"][1]["list_backend_invocations"], 1)
        self.assertEqual(result["recorded_received_prefetch_bytes"], 110)

    def test_off_arm_accepts_common_background_list_but_no_candidate_get(self):
        rec, summaries = self.inputs()
        background = rec["transfers"][0]
        background["prefetch"].update(session_id="", plan_id="", source="key_cache")
        demand = record()["transfers"][0]
        demand["prefetch"] = None
        rec["transfers"] = [demand]
        raw = self.raw([demand, background])
        result = q.summarize([rec], False, raw, summaries)
        self.assertTrue(result["complete_precision_qualification"])
        self.assertEqual(result["recorded_received_prefetch_bytes"], 0)
        self.assertEqual(result["backend_totals"]["list_backend_invocations"], 1)
        self.assertIsNone(result["get_body_byte_precision"])

    def test_nested_before_equal_after_and_unconsumed_use_entry_import_time(self):
        for time, outcome, result_name, useful in (
            (99, "completed", "local_hit", 30),
            (100, "completed", "local_hit", 0),
            (101, "completed", "local_hit", 0),
            (0, "import_error", "local_hit", 0),
        ):
            rec, summaries = self.inputs()
            entry = rec["transfers"][-1]["accounting"]["entries"][0]
            entry.update(finished_at_ms=time, outcome=outcome)
            rec["units"][0]["result"] = result_name
            result = self.summarize(rec, summaries)
            self.assertEqual(result["recorded_useful_prefetch_bytes"], useful)
            if time == 100:
                self.assertFalse(result["complete_precision_qualification"])
                self.assertIsNone(result["get_body_byte_precision"])
                self.assertEqual(
                    result["plans"][0]["payload_bytes"]["equal_timestamp"], 30
                )


class JoinSchema6(PackedAccounting):
    """The schema-6 wrapper-demand join (#1160, #1162)."""

    JOIN = {
        "consumed_prefetch_keys": 1,
        "consumed_prefetch_bytes": 30,
        "useful_prefetch_keys": 1,
        "useful_prefetch_bytes": 30,
        "remote_wait_ms": 7,
        "get_not_found": 0,
        "get_errors": 0,
    }

    def inputs(self, **join):
        rec, summaries = super().inputs()
        rec["schema"] = 6
        rec["summary"].update(self.JOIN | join)
        return rec, summaries

    def test_schema6_qualifies_and_reports_the_daemon_join(self):
        rec, summaries = self.inputs()
        result = self.summarize(rec, summaries)
        self.assertTrue(result["complete_precision_qualification"])
        join = result["daemon_join"]
        self.assertTrue(join["available"])
        self.assertEqual(join["useful_prefetch_bytes"], 30)
        self.assertEqual(join["consumed_prefetch_keys"], 1)
        self.assertEqual(join["useful_share_of_consumed_bytes"], 1.0)

    def test_the_join_is_reported_beside_the_harness_own_derivation(self):
        # Different byte bases: the join counts the per-key payload, the
        # harness counts GET-body bytes including the catalog object.
        rec, summaries = self.inputs()
        result = self.summarize(rec, summaries)
        self.assertEqual(result["recorded_received_prefetch_bytes"], 110)
        self.assertEqual(result["daemon_join"]["consumed_prefetch_bytes"], 30)
        self.assertNotEqual(
            result["get_body_byte_precision"],
            result["daemon_join"]["useful_share_of_consumed_bytes"],
        )

    def test_useful_cannot_exceed_consumed(self):
        for field in ("useful_prefetch_keys", "useful_prefetch_bytes"):
            rec, summaries = self.inputs(**{field: 99})
            with self.assertRaisesRegex(ValueError, "exceeds consumed"):
                self.summarize(rec, summaries)

    def test_join_counters_must_be_non_negative_integers(self):
        for value in (-1, "1", 1.5, None):
            rec, summaries = self.inputs(get_errors=value)
            with self.assertRaisesRegex(ValueError, "non-negative count"):
                self.summarize(rec, summaries)

    def test_unknown_schema6_summary_field_still_fails(self):
        rec, summaries = self.inputs()
        rec["summary"]["invented_counter"] = 1
        with self.assertRaisesRegex(ValueError, "Unknown summary fields"):
            self.summarize(rec, summaries)

    def test_schema5_records_report_no_join(self):
        rec, summaries = super().inputs()
        self.assertFalse(self.summarize(rec, summaries)["daemon_join"]["available"])


class SetupEnvironment(unittest.TestCase):
    """Pre-build steps must not create the state the cold control asserts."""

    def env(self):
        with tempfile.TemporaryDirectory() as tmp:
            return q.environment(Path(tmp), Path(tmp) / "kache")

    def test_setup_drops_every_kache_entry_point(self):
        setup = q.setup_environment(self.env())
        for key in q.WRAPPER_ENV_KEYS:
            self.assertNotIn(key, setup)
        # The empty host-config override stays: it suppresses any host config
        # rather than pointing Kache at one.
        self.assertEqual(
            [k for k in setup if k.startswith("KACHE_")], ["KACHE_HOST_CONFIG"]
        )
        self.assertEqual(setup["KACHE_HOST_CONFIG"], "")

    def test_setup_keeps_the_toolchain_and_cargo_pins(self):
        env = self.env()
        setup = q.setup_environment(env)
        for key in (
            "RUSTUP_TOOLCHAIN",
            "CARGO_HOME",
            "CARGO_TARGET_DIR",
            "CARGO_INCREMENTAL",
            "SOURCE_DATE_EPOCH",
        ):
            self.assertEqual(setup[key], env[key])

    def test_measurement_env_still_routes_through_kache(self):
        env = self.env()
        self.assertTrue(env["RUSTC_WRAPPER"].endswith("kache"))
        self.assertIn("KACHE_CONFIG", env)

    def test_command_uses_the_given_env(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runner = q.Run(root, root / "out", {"MARKER": "measurement"})
            plain = runner.command(
                "probe", [sys.executable, "-c", "import os;print(os.environ['MARKER'])"]
            )
            self.assertEqual(plain.strip(), "measurement")
            overridden = runner.command(
                "probe-setup",
                [sys.executable, "-c", "import os;print(os.environ['MARKER'])"],
                env={"MARKER": "setup"},
            )
            self.assertEqual(overridden.strip(), "setup")


class RetentionAndArtifacts(unittest.TestCase):
    def test_any_rotation_marker_is_preserved_and_rejected_after_drain(self):
        for log in ("events.jsonl", "transfers.jsonl", "summaries.jsonl"):
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                runtime, output = root / "runtime", root / "output"
                runtime.mkdir()
                output.mkdir()
                (runtime / log).write_text("small rotated remainder\n")
                (runtime / (log + ".rotation")).write_text('{"generation":1}')
                with self.assertRaisesRegex(ValueError, "rotation"):
                    q.preserve_logs(runtime, output)
                self.assertTrue((output / (log + ".rotation")).exists())
                self.assertEqual(
                    json.loads((output / "retention.json").read_text())[
                        "rotation_markers"
                    ],
                    [log + ".rotation"],
                )

    def test_executable_hash_listing_and_version_mismatch_are_rejected(self):
        good = {"sha256": "same", "listing": "alpha\nbeta\n", "version": "eza v0.23.5"}
        artifacts = {arm: dict(good) for arm in q.ARMS}
        self.assertEqual(q.artifact_problems(artifacts), [])
        for field, value in (
            ("sha256", "different"),
            ("listing", "wrong"),
            ("version", "v0.23.6"),
        ):
            artifacts["on-3"] = good | {field: value}
            self.assertTrue(q.artifact_problems(artifacts))


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
                q.dump(
                    folder / "artifact.json",
                    {
                        "sha256": "artifact-sha",
                        "version": "v0.23.5",
                        "listing": "alpha\nbeta\n",
                    },
                )
                q.dump(folder / "admission.json", {"demanded_keys": 2})
                q.dump(folder / "lifecycle.json", {"problems": []})
                q.dump(folder / "retention.json", {"rotation_markers": []})
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
                for name, contents in (
                    ("lifecycle", {"problems": []}),
                    ("retention", {"rotation_markers": []}),
                ):
                    evidence_path = (
                        args.results / "prefetch-qualification-on-3" / (name + ".json")
                    )
                    evidence_path.unlink()
                    with self.assertRaises(ValueError):
                        q.collect(args)
                    incomplete = json.loads(
                        (args.output / "job-times.json").read_text()
                    )
                    self.assertFalse(incomplete["controls_valid"])
                    self.assertFalse(incomplete["complete_precision_qualification"])
                    self.assertIn(f"on-3 lacks {name} evidence", incomplete["problems"])
                    q.dump(evidence_path, contents)
                artifact_path = (
                    args.results / "prefetch-qualification-on-3" / "artifact.json"
                )
                artifact = json.loads(artifact_path.read_text())
                q.dump(artifact_path, artifact | {"sha256": "different"})
                with self.assertRaises(ValueError):
                    q.collect(args)
                mismatch = json.loads((args.output / "job-times.json").read_text())
                self.assertFalse(mismatch["controls_valid"])
                self.assertIn(
                    "Consumer executable SHA256 values differ", mismatch["problems"]
                )
                q.dump(artifact_path, artifact)
                retention_path = (
                    args.results / "prefetch-qualification-on-3" / "retention.json"
                )
                q.dump(
                    retention_path, {"rotation_markers": ["transfers.jsonl.rotation"]}
                )
                with self.assertRaises(ValueError):
                    q.collect(args)
                rotated = json.loads((args.output / "job-times.json").read_text())
                self.assertFalse(rotated["controls_valid"])
                self.assertIn(
                    "on-3: retained rotation marker transfers.jsonl.rotation",
                    rotated["problems"],
                )
                q.dump(retention_path, {"rotation_markers": []})
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
