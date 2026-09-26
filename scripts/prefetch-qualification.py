#!/usr/bin/env python3
"""Isolated real-CI prefetch control. Uses only Python's standard library."""

import argparse
import fcntl
import hashlib
import itertools
import json
import os
import shutil
import subprocess
import tarfile
import time
import urllib.request
from collections import Counter
from datetime import datetime
from pathlib import Path

PROJECT = "98442ab17c2c3738701b62a7e060b1431ae2d6ea"
TOOLCHAIN = "1.90.0"
# The seed publishes its manifest and shards under these; every arm plans from
# them, as a CI job with kache-action's manifest-key and namespace does.
PROFILE = "release"
NAMESPACE = f"prefetch-qualification/{TOOLCHAIN}/{PROFILE}"
WORKFLOW = ".github/workflows/prefetch-qualification.yml"
ARMS = ("off-1", "on-1", "on-2", "off-2", "off-3", "on-3")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def dump(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n")


def digest(path):
    with path.open("rb") as stream:
        value = hashlib.sha256()
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
        return value.hexdigest()


def files(root):
    result = {}
    for path in sorted(root.rglob("*")):
        require(not path.is_symlink(), f"Symlink in seed: {path}")
        if path.is_file() and path != root / "manifest.json":
            result[str(path.relative_to(root))] = digest(path)
    return result


def trusted_push(env):
    return (
        env.get("GITHUB_ACTIONS") == "true"
        and env.get("GITHUB_EVENT_NAME") == "push"
        and env.get("GITHUB_REF") == "refs/heads/main"
        and env.get("GITHUB_REF_TYPE") == "branch"
        and env.get("GITHUB_REF_PROTECTED", "").lower() in ("true", "1", "yes")
    )


def environment(root, binary):
    # Remove inherited Kache settings, never CI trust variables.
    env = {
        key: value for key, value in os.environ.items() if not key.startswith("KACHE_")
    }
    for key in (
        "RUSTC_WRAPPER",
        "RUSTC_WORKSPACE_WRAPPER",
        "CARGO_BUILD_RUSTC_WRAPPER",
        "RUSTFLAGS",
        "CARGO_ENCODED_RUSTFLAGS",
        "CFLAGS",
        "CXXFLAGS",
        "CC",
        "CXX",
    ):
        env.pop(key, None)
    source = root / "source"
    remap = " ".join(
        f"-{kind}-prefix-map={source}=." for kind in ("ffile", "fdebug", "fmacro")
    )
    env.update(
        KACHE_CONFIG=str(root / "config.toml"),
        KACHE_HOST_CONFIG="",
        RUSTUP_TOOLCHAIN=TOOLCHAIN,
        CARGO_INCREMENTAL="0",
        CARGO_BUILD_JOBS="2",
        CARGO_HOME=str(root / "cargo"),
        CARGO_TARGET_DIR=str(source / "target"),
        RUSTC_WRAPPER=str(binary),
        HOST_CC=f"{binary} cc",
        HOST_CXX=f"{binary} c++",
        CC_KNOWN_WRAPPER_CUSTOM="kache",
        KACHE_BASE_DIR=str(source),
        KACHE_INPUT_PREDICTIONS="1",
        KACHE_VERIFY_INPUT_PREDICTIONS="sampled",
        KACHE_PROFILE=PROFILE,
        KACHE_NAMESPACE=NAMESPACE,
        CFLAGS=remap,
        CXXFLAGS=remap,
        SOURCE_DATE_EPOCH="1735689600",
        ZERO_AR_DATE="1",
        LC_ALL="C",
        TZ="UTC",
        NO_COLOR="1",
    )
    return env


# Variables that route a compile through Kache. Setup steps run without them so
# that nothing creates the local store or the runtime directory before the cold
# control is asserted: `cargo fetch` alone probes rustc through RUSTC_WRAPPER,
# which is enough for the daemon to lay down its runtime directory.
WRAPPER_ENV_KEYS = (
    "RUSTC_WRAPPER",
    "HOST_CC",
    "HOST_CXX",
    "CC_KNOWN_WRAPPER_CUSTOM",
    "KACHE_CONFIG",
    "KACHE_BASE_DIR",
    "KACHE_INPUT_PREDICTIONS",
    "KACHE_VERIFY_INPUT_PREDICTIONS",
    "KACHE_PROFILE",
    "KACHE_NAMESPACE",
)


def setup_environment(env):
    """`env` with every Kache entry point removed, for pre-build steps."""
    return {key: value for key, value in env.items() if key not in WRAPPER_ENV_KEYS}


class Run:
    def __init__(self, root, output, env):
        self.root, self.output, self.env = root, output, env
        self.phases = []
        output.mkdir(parents=True, exist_ok=True)

    def command(self, name, argv, cwd=None, timeout=3600, env=None):
        begin, start = time.time(), time.monotonic()
        code = None
        try:
            with (
                (self.output / f"{name}.out").open("w") as out,
                (self.output / f"{name}.err").open("w") as err,
            ):
                code = subprocess.run(
                    [str(x) for x in argv],
                    cwd=cwd or self.root,
                    env=self.env if env is None else env,
                    stdout=out,
                    stderr=err,
                    timeout=timeout,
                    check=False,
                ).returncode
            require(code == 0, f"{name} failed ({code}); see phase logs")
            return (self.output / f"{name}.out").read_text()
        finally:
            self.phases.append(
                {
                    "name": name,
                    "started_at": begin,
                    "finished_at": time.time(),
                    "seconds": time.monotonic() - start,
                    "exit_code": code,
                }
            )
            dump(self.output / "phases.json", self.phases)


def unpack(archive, destination):
    require(not destination.exists(), "Seed destination must be fresh")
    with tarfile.open(archive, "r:gz") as tar:
        for item in tar.getmembers():
            path = Path(item.name)
            require(
                not path.is_absolute() and ".." not in path.parts, "Unsafe archive path"
            )
            require(
                item.isfile() or item.isdir(), "Seed links/special files are forbidden"
            )
        destination.mkdir()
        for item in tar.getmembers():
            target = destination / item.name
            if item.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                with tar.extractfile(item) as source, target.open("xb") as output:
                    shutil.copyfileobj(source, output)
    manifest = json.loads((destination / "manifest.json").read_text())
    require(
        manifest["project"] == PROJECT and manifest["toolchain"] == TOOLCHAIN,
        "Seed workload mismatch",
    )
    require(manifest["files"] == files(destination), "Seed content hash mismatch")
    require(manifest["trusted_push"] is True, "Seed was not produced by a trusted push")
    return manifest


def drain(runtime):
    with (runtime / "daemon.run.lock").open("a") as lock:
        deadline = time.monotonic() + 120
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return
            except BlockingIOError:
                require(time.monotonic() < deadline, "Daemon did not drain")
                time.sleep(0.1)


# Schema 4 adds TimelineSummary.incomplete to schema 3's ordinary transfer model.
# Reviewed against lifecycle 0ac603f944ba546dfc78cb457462537c99a35739 and
# packed/physical accounting 129a31ce4debb8c97d9698fe5ff6d48ed4387750.
# Schema 6 adds the wrapper-demand join (#1160, #1162): per-key consumed and
# useful prefetch counters plus GET failure counts. Reviewed against
# src/timeline.rs `summarize`.
# Schema 7 marks each unit with the delivery of its key (plan, rank, timing
# against first demand) and adds in-flight and cancelled counters to the join.
SUPPORTED_TIMELINE_SCHEMAS = (3, 4, 5, 6, 7)

# Units Kache keeps in the local store and never publishes to a remote
# (src/build_script.rs, #1072). They carry a cache key but can never be
# prefetched or checked against the remote.
LOCAL_ONLY_UNITS = frozenset({"build_script_run"})

# TimelineSummary fields the schema-6 join adds.
JOIN_SUMMARY_FIELDS = (
    "consumed_prefetch_keys",
    "consumed_prefetch_bytes",
    "useful_prefetch_keys",
    "useful_prefetch_bytes",
    "remote_wait_ms",
    "get_not_found",
    "get_errors",
)

# TimelineSummary fields schema 7 adds to the join.
JOIN_SUMMARY_FIELDS_7 = (
    "in_flight_prefetch_keys",
    "in_flight_prefetch_bytes",
    "get_cancelled",
)

# Where a unit's prefetch delivery fell against first demand (schema 7).
PREFETCH_TIMINGS = frozenset({"before_demand", "in_flight", "after_demand"})


def join_fields(schema):
    if schema >= 7:
        return JOIN_SUMMARY_FIELDS + JOIN_SUMMARY_FIELDS_7
    if schema >= 6:
        return JOIN_SUMMARY_FIELDS
    return ()


def lifecycle_evidence(records, raw_summaries=None):
    require(
        records and all(r["schema"] in SUPPORTED_TIMELINE_SCHEMAS for r in records),
        "Only reviewed timeline schemas 3, 4, 5, 6 and 7 are supported",
    )
    summaries, problems, missing = {}, [], []
    known_summary_fields = {
        "plan_id",
        "plan_source",
        "closure_reason",
        "started_at_ms",
        "last_activity_ms",
        "candidate_keys",
        "downloaded_keys",
        "downloaded_bytes",
        "used_keys",
        "demanded_keys",
        "demanded_candidate_keys",
        "cancelled",
    }
    for record in records:
        session, schema = record["session_id"], record["schema"]
        summary = record.get("summary")
        if summary is None:
            if schema >= 4 and any(
                t.get("prefetch", {}).get("session_id") == session
                for t in record["transfers"]
                if t.get("prefetch")
            ):
                missing.append(session)
                problems.append(f"{session}: speculative session has no final summary")
            continue
        allowed = known_summary_fields | ({"incomplete"} if schema >= 4 else set())
        allowed = allowed | set(join_fields(schema))
        require(
            not (set(summary) - allowed),
            "Unknown summary fields: review the schema adapter",
        )
        if schema >= 6:
            for field in join_fields(schema):
                value = summary.get(field, 0)
                require(
                    type(value) is int and value >= 0,
                    f"{session}: schema-{schema} {field} must be a non-negative count",
                )
            # Useful deliveries landed at or before first demand; in-flight
            # ones were still downloading then. They are disjoint subsets of
            # consumed deliveries, so together they cannot exceed consumption.
            for unit in ("keys", "bytes"):
                used = summary.get(f"useful_prefetch_{unit}", 0) + summary.get(
                    f"in_flight_prefetch_{unit}", 0
                )
                if used > summary.get(f"consumed_prefetch_{unit}", 0):
                    problems.append(
                        f"{session}: useful and in-flight prefetch exceed consumed prefetch"
                    )
        summaries[session] = summary
        if schema >= 4 and type(summary.get("incomplete")) is not bool:
            problems.append(
                f"{session}: schema-4 summary lacks an explicit incomplete flag"
            )
        if summary.get("incomplete") is True:
            problems.append(f"{session}: shutdown left outcomes incomplete")
        if summary.get("closure_reason") == "shutdown_timeout":
            problems.append(f"{session}: shutdown timed out")
    raw_schemas = []
    for summary in raw_summaries or []:
        raw_schemas.append(summary["schema"])
        require(summary["schema"] in (1, 2), "Unknown raw summary schema")
        if all(r["schema"] >= 4 for r in records) and summary["schema"] != 2:
            problems.append(
                "Schema-4 timeline contains a legacy raw summary without lifecycle evidence"
            )
        if summary["schema"] == 2:
            require(
                type(summary.get("incomplete")) is bool,
                "Raw summary schema 2 requires an explicit incomplete flag",
            )
        if (
            summary.get("incomplete") is True
            or summary.get("closure_reason") == "shutdown_timeout"
        ):
            problems.append(
                f"{summary.get('session_id', '')}: raw summary is incomplete"
            )
    return {
        "timeline_schemas": sorted({r["schema"] for r in records}),
        "raw_summary_schemas": sorted(set(raw_schemas)),
        "summaries": summaries,
        "legacy_shutdown_evidence_unknown": any(r["schema"] == 3 for r in records),
        "missing_summary_sessions": missing,
        "problems": problems,
    }


def join_evidence(records, lifecycle):
    """Daemon-side schema-6 join counters, summed across this arm's sessions.

    These are reported beside the harness's own derivation, not asserted equal
    to it. Two differences are expected and are not defects:

    - Byte base. `useful_payload_bytes` counts GET-body bytes, including
      catalog and pack overhead. `useful_prefetch_bytes` counts the per-key
      payload the join credited, so the harness denominator is the larger one.
    - Tie-break. The join credits a delivery that finished at exactly the first
      demand millisecond; the harness buckets that as `equal_timestamp` and
      refuses to call the ordering known. On identical input the harness
      therefore credits no more keys than the join does.
    """
    if not all(record["schema"] >= 6 for record in records):
        return {"available": False, "reason": "Timeline schema predates the join"}
    fields = join_fields(min(record["schema"] for record in records))
    totals = Counter()
    for summary in lifecycle["summaries"].values():
        for field in fields:
            totals[field] += summary.get(field, 0)
    consumed_bytes = totals["consumed_prefetch_bytes"]
    return {
        "available": True,
        **{field: totals[field] for field in fields},
        "useful_share_of_consumed_bytes": (
            totals["useful_prefetch_bytes"] / consumed_bytes
            if consumed_bytes
            else None
        ),
        "scope": "Per-key payload bytes the join credited; "
        "not the GET-body base used by get_body_byte_precision",
    }


def plan_identity(origin):
    return (
        origin.get("session_id", ""),
        origin.get("plan_id", ""),
        origin.get("source", ""),
    )


def receipt_projection(transfer, raw=False):
    """The exact fields copied by src/timeline.rs; multiplicity is significant."""
    result = {
        key: transfer.get(key, 0)
        for key in (
            "compressed_bytes",
            "original_bytes",
            "network_ms",
            "semaphore_wait_ms",
            "request_count",
            "import_ms",
        )
    }
    result.update(
        {
            key: transfer.get(key, "")
            for key in ("cache_key", "crate_name", "direction", "outcome")
        }
    )
    result.update(
        ok=transfer["ok"],
        prefetch=transfer.get("prefetch"),
        accounting=transfer.get("accounting"),
    )
    for key in ("started_at", "finished_at"):
        result[key + "_ms"] = transfer.get(key + ("_unix_ms" if raw else "_ms"), 0)
    return result


def reconcile_receipts(projected, raw_transfers):
    projected = [receipt_projection(t) for t in projected]
    if raw_transfers is None:
        return projected, {
            "complete": False,
            "reason": "Raw transfer log was not supplied",
            "get_complete": False,
            "unprojected": [],
            "projected_without_raw": [],
        }
    require(
        all(t.get("schema") == 5 for t in raw_transfers),
        "Schema-5 controls require raw transfer schema 5",
    )
    raw = [
        receipt_projection(t, raw=True)
        for t in raw_transfers
        if t["direction"] == "download" and t.get("prefetch") is not None
    ]
    canonical = lambda t: json.dumps(t, sort_keys=True)
    raw_count, projected_count = (
        Counter(map(canonical, raw)),
        Counter(map(canonical, projected)),
    )
    unprojected = [
        json.loads(key)
        for key, count in (raw_count - projected_count).items()
        for _ in range(count)
    ]
    missing = [
        json.loads(key)
        for key, count in (projected_count - raw_count).items()
        for _ in range(count)
    ]
    return raw, {
        "complete": not unprojected and not missing,
        "get_complete": not any(
            (t.get("accounting") or {}).get("operation") != "list"
            for t in unprojected + missing
        ),
        "unprojected": unprojected,
        "projected_without_raw": missing,
    }


def summarize_accounted(
    records,
    projected,
    demands,
    consumed,
    units,
    enabled,
    lifecycle,
    raw_transfers,
    raw_summaries,
):
    speculative = [
        t
        for t in projected
        if t["direction"] == "download" and t.get("prefetch") is not None
    ]
    receipts, coverage = reconcile_receipts(speculative, raw_transfers)
    require(
        any(
            (t.get("accounting") or {}).get("operation") == "get"
            or (t.get("accounting") is None and bool(t.get("cache_key")))
            for t in receipts + speculative
        )
        == enabled,
        "Prefetch arm did not match observed transfers",
    )
    restored = any(
        t["direction"] == "download" and t["ok"] and t.get("original_bytes", 0) > 0
        for t in projected
    ) or any(
        entry["outcome"] == "completed"
        and (entry["prefetch"]["session_id"], entry["cache_key"]) in consumed
        for t in receipts
        for entry in (t.get("accounting") or {}).get("entries", [])
    )
    require(restored, "No remote restoration: control is inconclusive")
    groups, candidates = {}, []

    def group_for(origin):
        key = plan_identity(origin)
        if key not in groups:
            groups[key] = {
                "session_id": key[0],
                "plan_id": key[1],
                "source": key[2],
                "get_received_body_bytes": 0,
                "get_receipts": 0,
                "unknown_operations": 0,
                "backend_count_problems": [],
                "useful_payload_bytes": 0,
                "get_backend_invocations": 0,
                "list_backend_invocations": 0,
                "list_result_count": 0,
                "list_results_unknown": 0,
                "list_response_bytes_unknown": 0,
                "unclassified_received_bytes": 0,
                "get_outcomes": Counter(),
                "list_outcomes": Counter(),
                "payload_bytes": Counter(),
                "problems": [],
            }
        return groups[key]

    accounting_fields = {
        "operation",
        "bytes_complete",
        "requests_complete",
        "list_result_count",
        "entries",
    }
    entry_fields = {
        "cache_key",
        "crate_name",
        "compressed_bytes",
        "finished_at_ms",
        "outcome",
        "prefetch",
    }
    for receipt in receipts:
        group = group_for(receipt["prefetch"])
        accounting = receipt.get("accounting")
        require(
            type(receipt["compressed_bytes"]) is int
            and receipt["compressed_bytes"] >= 0,
            "Invalid physical received bytes",
        )
        if accounting is None:
            group["unknown_operations"] += 1
            group["problems"].append(
                "Receipt lacks operation/byte completeness accounting"
            )
            if receipt["cache_key"]:
                # Reviewed pre-followup schema-5 ordinary keyed downloads are GETs,
                # but their counters do not establish full-body or call completeness.
                accounting = {
                    "operation": "get",
                    "bytes_complete": False,
                    "requests_complete": False,
                    "entries": [],
                }
            else:
                group["unclassified_received_bytes"] += receipt["compressed_bytes"]
                continue
        require(not (set(accounting) - accounting_fields), "Unknown accounting fields")
        require(accounting["operation"] in ("get", "list"), "Unknown backend operation")
        require(
            type(accounting["bytes_complete"]) is bool
            and type(accounting["requests_complete"]) is bool,
            "Completeness flags must be explicit booleans",
        )
        require(
            type(receipt["request_count"]) is int and receipt["request_count"] >= 0,
            "Invalid backend invocation count",
        )
        operation = accounting["operation"]
        group[operation + "_backend_invocations"] += receipt["request_count"]
        group[operation + "_outcomes"][receipt["outcome"] or "unknown"] += 1
        if not accounting["requests_complete"]:
            group["backend_count_problems"].append(
                "Backend invocation count is incomplete"
            )
            if operation == "get":
                group["problems"].append("GET invocation count is incomplete")
        entries = accounting.get("entries", [])
        if operation == "list":
            require(not entries, "LIST cannot carry imported payload entries")
            count = accounting.get("list_result_count")
            if count is None:
                group["list_results_unknown"] += 1
            else:
                require(type(count) is int and count >= 0, "Invalid LIST result count")
                group["list_result_count"] += count
            if not accounting["bytes_complete"]:
                group["list_response_bytes_unknown"] += 1
            continue
        group["get_receipts"] += 1
        group["get_received_body_bytes"] += receipt["compressed_bytes"]
        if not accounting["bytes_complete"]:
            group["problems"].append("Partial GET body bytes are unknown")
        require(
            not entries or not receipt["cache_key"],
            "Packed receipt must have an empty outer key",
        )
        require(
            sum(entry["compressed_bytes"] for entry in entries)
            <= receipt["compressed_bytes"],
            "Nested payload exceeds physical GET body",
        )
        for entry in entries:
            require(not (set(entry) - entry_fields), "Unknown packed entry fields")
            require(
                type(entry["compressed_bytes"]) is int
                and entry["compressed_bytes"] >= 0,
                "Invalid entry payload bytes",
            )
            if plan_identity(entry["prefetch"]) != plan_identity(receipt["prefetch"]):
                group["problems"].append(
                    "Entry origin differs from physical receipt plan"
                )
                continue
            candidates.append((entry, receipt, group))
        if not entries and receipt["cache_key"]:
            candidates.append(
                (
                    {
                        "cache_key": receipt["cache_key"],
                        "compressed_bytes": receipt["compressed_bytes"],
                        "finished_at_ms": receipt["finished_at_ms"],
                        "outcome": receipt["outcome"],
                        "prefetch": receipt["prefetch"],
                    },
                    receipt,
                    group,
                )
            )

    credited, payload_rows = set(), []
    sessions = {record["session_id"] for record in records}
    for entry, receipt, group in sorted(
        candidates, key=lambda value: value[0]["finished_at_ms"]
    ):
        key = (entry["prefetch"]["session_id"], entry["cache_key"])
        demand = demands.get(key)
        finished = entry["finished_at_ms"]
        if entry["outcome"] != "completed":
            bucket = "failed"
        elif finished <= 0:
            bucket = "unknown_import_time"
            group["problems"].append(
                "Completed payload has no import completion timestamp"
            )
        elif not key[0] or key[0] not in sessions:
            bucket = "unknown_demand"
        elif demand is None:
            bucket = "unused"
        elif key not in consumed:
            bucket = "demanded_unconsumed"
        elif key in credited:
            bucket = "duplicate"
        elif finished < demand:
            bucket = "useful_before_demand"
            credited.add(key)
            group["useful_payload_bytes"] += entry["compressed_bytes"]
        elif finished == demand:
            bucket = "equal_timestamp"
            group["problems"].append(
                "Equal-millisecond import/demand ordering is unknown"
            )
        else:
            bucket = "late"
        group["payload_bytes"][bucket] += entry["compressed_bytes"]
        payload_rows.append(
            {"entry": entry, "bucket": bucket, "first_demand_at_ms": demand}
        )

    # A normal closure is a snapshot, not a barrier against tasks or log writers.
    summaries = raw_summaries if raw_summaries is not None else []
    shutdown_groups = {
        (s.get("session_id", ""), s.get("plan_id", ""), s.get("plan_source", ""))
        for s in summaries
        if s.get("schema") == 2
        and s.get("closure_reason") == "shutdown"
        and s.get("incomplete") is False
    }
    observed_sessions = {session for session, _ in demands}
    shutdown_sessions = {key[0] for key in shutdown_groups}
    shutdown_complete = observed_sessions <= shutdown_sessions
    incomplete_groups = {
        plan_identity(t["prefetch"])
        for t in coverage["unprojected"] + coverage["projected_without_raw"]
        if (t.get("accounting") or {}).get("operation") != "list"
    }
    for key, group in groups.items():
        if (
            group["get_receipts"] or group["unknown_operations"]
        ) and key not in shutdown_groups:
            group["problems"].append("Plan lacks a drained shutdown summary")
        if key in incomplete_groups or raw_transfers is None:
            group["problems"].append("Raw/projected GET receipt coverage is incomplete")
        group["complete"] = not group["problems"] and not lifecycle["problems"]
        size = group["get_received_body_bytes"]
        group["get_body_byte_precision"] = (
            group["useful_payload_bytes"] / size if size and group["complete"] else None
        )
        group["payload_bytes"] = dict(group["payload_bytes"])
        group["get_outcomes"] = dict(group["get_outcomes"])
        group["list_outcomes"] = dict(group["list_outcomes"])
    physical = sum(group["get_received_body_bytes"] for group in groups.values())
    useful = sum(group["useful_payload_bytes"] for group in groups.values())
    complete = (
        coverage["get_complete"]
        and shutdown_complete
        and all(group["complete"] for group in groups.values())
    )
    daemon_join = join_evidence(records, lifecycle)
    waits = Counter()
    for record in records:
        for unit in record["units"]:
            for demand in unit.get("demands", []):
                waits[(record["session_id"], demand["cache_key"])] += demand[
                    "remote_wait_ms"
                ]
    return {
        "scope": "GET-body bytes including catalog and pack overhead; LIST response bytes excluded",
        "complete_precision_qualification": complete,
        "backend_invocations_complete": raw_transfers is not None
        and shutdown_complete
        and not coverage["projected_without_raw"]
        and not any(
            g["backend_count_problems"] or g["unknown_operations"]
            for g in groups.values()
        ),
        "lifecycle": lifecycle,
        "shutdown_complete": shutdown_complete,
        "receipt_coverage": coverage,
        "plans": list(groups.values()),
        "backend_totals": {
            name: sum(group[name] for group in groups.values())
            for name in (
                "get_backend_invocations",
                "list_backend_invocations",
                "list_result_count",
                "list_results_unknown",
                "list_response_bytes_unknown",
                "unclassified_received_bytes",
            )
        },
        "demand_waits": [
            {
                "session_id": session,
                "cache_key": key,
                "first_demand_at_ms": demands[(session, key)],
                "observed_remote_wait_ms": wait,
            }
            for (session, key), wait in sorted(waits.items())
        ],
        "recorded_received_prefetch_bytes": physical,
        "recorded_useful_prefetch_bytes": useful,
        "get_body_byte_precision": useful / physical if complete and physical else None,
        "daemon_join": daemon_join,
        "demanded_keys": len(demands),
        "observed_remote_wait_ms": sum(
            d["remote_wait_ms"] for u in units for d in u.get("demands", [])
        ),
        "unit_outcomes": dict(Counter(u["result"] for u in units)),
        # Schema 7: a local_hit with before_demand timing is a prefetched
        # local hit; in_flight means the demand waited on a running GET.
        "unit_prefetch_outcomes": dict(
            Counter(
                f"{u['result']}/{u['prefetch']['timing']}"
                for u in units
                if "prefetch" in u
            )
        ),
        "payloads": payload_rows,
        "backend_count_scope": "Backend invocations only; SDK retries and LIST pages excluded",
    }


def summarize(records, enabled, raw_transfers=None, raw_summaries=None):
    lifecycle = lifecycle_evidence(records, raw_summaries)
    require(len({r["schema"] for r in records}) == 1, "Mixed timeline schemas")
    sessions = [r["session_id"] for r in records]
    record_ids = [r.get("client_record_id") for r in records]
    require(
        all(record_ids)
        and len(set(sessions)) == len(sessions)
        and len(set(record_ids)) == len(record_ids),
        "Duplicate timeline snapshots are not independent physical operations",
    )
    require(not lifecycle["problems"], "; ".join(lifecycle["problems"]))
    demands, consumed, units, transfers = {}, set(), [], []
    sessions = {record["session_id"] for record in records}
    allowed_transfer_fields = {
        "crate_name",
        "cache_key",
        "direction",
        "ok",
        "compressed_bytes",
        "original_bytes",
        "started_at_ms",
        "finished_at_ms",
        "network_ms",
        "semaphore_wait_ms",
        "request_count",
        "import_ms",
        "attribution",
        "prefetch",
        "outcome",
    }
    if records[0]["schema"] >= 5:
        allowed_transfer_fields.add("accounting")
    for record in records:
        session = record["session_id"]
        units.extend(record["units"])
        for unit in record["units"]:
            observations = unit.get("demands", [])
            if unit.get("crate_name") in LOCAL_ONLY_UNITS:
                # Never published or fetched, so there is no demand to record,
                # and a remote or prefetch outcome would be a telemetry defect.
                require(
                    not observations
                    and "prefetch" not in unit
                    and unit["result"] not in ("remote_hit", "prefetch_hit"),
                    "Local-only unit reports remote demand or outcome",
                )
                continue
            if "prefetch" in unit:
                require(
                    record["schema"] >= 7,
                    "Unit prefetch marker needs schema 7: review the schema adapter",
                )
                require(
                    unit["prefetch"].get("timing") in PREFETCH_TIMINGS,
                    "Unknown unit prefetch timing: review the schema adapter",
                )
            if unit["cache_key"]:
                require(
                    unit.get("event_schema", 0) >= 20 and observations,
                    "Every keyed unit must carry exact schema-20 demands",
                )
                require(
                    any(d["cache_key"] == unit["cache_key"] for d in observations),
                    "Keyed unit lacks its own first-demand observation",
                )
            for demand in observations:
                key = (session, demand["cache_key"])
                at = demand["first_demand_at_ms"]
                require(at > 0, "Invalid demand timestamp")
                demands[key] = min(demands.get(key, at), at)
            if unit["result"] in ("local_hit", "prefetch_hit", "remote_hit"):
                consumed.add((session, unit["cache_key"]))
        for transfer in record["transfers"]:
            require(
                not (set(transfer) - allowed_transfer_fields),
                "Unknown transfer fields: update the schema adapter before qualification",
            )
            transfers.append(transfer)
    require(demands, "No exact demand records")
    require(consumed, "No cache artifact was consumed")
    if records[0]["schema"] >= 5:
        return summarize_accounted(
            records,
            transfers,
            demands,
            consumed,
            units,
            enabled,
            lifecycle,
            raw_transfers,
            raw_summaries,
        )
    restored = [
        t
        for t in transfers
        if t["direction"] == "download" and t["ok"] and t["original_bytes"] > 0
    ]
    require(restored, "No remote restoration: control is inconclusive")
    speculative = [
        t
        for t in transfers
        if t["direction"] == "download" and t.get("prefetch") is not None
    ]
    require(
        bool(speculative) == enabled, "Prefetch arm did not match observed transfers"
    )
    totals, outcomes, rows, credited = Counter(), Counter(), [], set()
    for transfer in sorted(speculative, key=lambda t: t["finished_at_ms"]):
        origin = transfer["prefetch"]
        key = (origin["session_id"], transfer["cache_key"])
        outcomes[transfer.get("outcome", "unknown")] += 1
        demand = demands.get(key)
        finished = transfer["finished_at_ms"]
        size = transfer["compressed_bytes"]
        require(size >= 0, "Negative received byte count")
        if not transfer["ok"] or transfer.get("outcome") != "completed":
            bucket = "failed"
        elif not key[0] or key[0] not in sessions:
            bucket = "unknown_demand"
        elif demand is None:
            bucket = "unused"
        elif key not in consumed:
            bucket = "demanded_unconsumed"
        elif key in credited:
            bucket = "duplicate"
        elif finished < demand:
            bucket = "useful_before_demand"
            credited.add(key)
        elif finished == demand:
            bucket = "equal_timestamp"
        elif transfer["started_at_ms"] <= demand:
            bucket = "inflight_at_demand"
        else:
            bucket = "late"
        totals[bucket] += size
        rows.append(
            {"transfer": transfer, "first_demand_at_ms": demand, "bucket": bucket}
        )
    denominator = sum(t["compressed_bytes"] for t in speculative)
    return {
        "scope": "Ordinary schema-3/4 logged transfers only; packed and partial physical transfers may be absent",
        "lifecycle": lifecycle,
        "complete_precision_qualification": False,
        "demanded_keys": len(demands),
        "unit_outcomes": dict(Counter(u["result"] for u in units)),
        "remote_wait_ms": sum(
            d["remote_wait_ms"] for u in units for d in u.get("demands", [])
        ),
        "speculative_attempts": len(speculative),
        "speculative_outcomes": dict(outcomes),
        "recorded_received_prefetch_bytes": denominator,
        "recorded_useful_prefetch_bytes": totals["useful_before_demand"],
        "ordinary_recorded_byte_precision": totals["useful_before_demand"] / denominator
        if denominator
        else None,
        "speculative_compressed_bytes": dict(totals),
        "speculative": rows,
    }


def preserve_logs(runtime, output):
    markers = []
    for name in ("events.jsonl", "transfers.jsonl", "summaries.jsonl"):
        if (runtime / name).exists():
            shutil.copy2(runtime / name, output / name)
        marker = runtime / (name + ".rotation")
        if marker.exists():
            shutil.copy2(marker, output / marker.name)
            markers.append(marker.name)
    dump(output / "retention.json", {"rotation_markers": markers})
    require(
        not markers, "Log rotation may have removed observations: " + ", ".join(markers)
    )


def artifact_problems(artifacts):
    problems = []
    for arm, artifact in artifacts.items():
        if artifact.get("listing") != "alpha\nbeta\n" or "v0.23.5" not in artifact.get(
            "version", ""
        ):
            problems.append(f"{arm}: artifact failed the pinned workload oracle")
    if len({value.get("sha256") for value in artifacts.values()}) != 1:
        problems.append("Consumer executable SHA256 values differ")
    if len({value.get("version") for value in artifacts.values()}) != 1:
        problems.append("Consumer executable versions differ")
    return problems


def measure(args):
    root, output = args.root.resolve(), args.output.resolve()
    require(not root.exists(), "Measurement root must be new")
    root.mkdir(parents=True)
    output.mkdir(parents=True, exist_ok=True)
    producer = args.mode == "seed"
    enabled = args.arm.startswith("on") if not producer else False
    if producer:
        require(trusted_push(os.environ), "Seed requires a protected main push")
        bundle = root / "bundle"
        bundle.mkdir()
        binary = bundle / "kache"
        shutil.copy2(args.binary, binary)
        remote = bundle / "remote"
        remote.mkdir()
        manifest = {}
    else:
        bundle = root / "bundle"
        manifest = unpack(args.archive, bundle)
        require(str(manifest["run_id"]) == args.seed_run_id, "Unexpected seed run")
        require(manifest["kache_revision"] == args.seed_sha, "Unexpected seed revision")
        binary, remote = bundle / "kache", bundle / "remote"
        # Preserve verified identity even if setup, compilation or admission fails.
        dump(output / "identity.json", manifest)
    binary.chmod(0o755)
    env = environment(root, binary)
    runner = Run(root, output, env)
    cache, runtime = root / "cache", root / "runtime"
    config = f"""[cache]
ignore_env = true
local_store = {json.dumps(str(cache))}
runtime_dir = {json.dumps(str(runtime))}
min_store_compile_ms = 0
record_sessions = true
prefetch_enabled = {str(enabled).lower()}
remote_readonly = {str(not producer).lower()}
event_log_max_size = "8GiB"
[cache.remote]
type = "filesystem"
path = {json.dumps(str(remote))}
prefix = "artifacts"
"""
    (root / "config.toml").write_text(config)
    shutil.copy2(root / "config.toml", output / "config.toml")
    resources = {"cpu_count": os.cpu_count(), "load": os.getloadavg()}
    for path in (
        "/proc/meminfo",
        "/sys/fs/cgroup/cpu.max",
        "/sys/fs/cgroup/memory.max",
    ):
        if Path(path).exists():
            resources[path] = Path(path).read_text()
    dump(output / "resources.json", resources)
    setup = setup_environment(env)
    runner.command(
        "clone",
        [
            "git",
            "clone",
            "--depth",
            "1",
            "--branch",
            "v0.23.5",
            "https://github.com/eza-community/eza.git",
            root / "source",
        ],
        env=setup,
    )
    source = root / "source"
    require(
        runner.command(
            "source-revision", ["git", "rev-parse", "HEAD"], source, env=setup
        ).strip()
        == PROJECT,
        "eza tag moved",
    )
    if not producer:
        require(
            digest(source / "Cargo.lock") == manifest["lock_sha256"], "Lockfile drift"
        )
    compiler = runner.command("compiler", ["rustc", "-Vv"], env=setup)
    cc = runner.command("cc", ["cc", "--version"], env=setup)
    if not producer:
        require(
            compiler == manifest["compiler"] and cc == manifest["cc"], "Compiler drift"
        )
    runner.command("fetch", ["cargo", "fetch", "--locked"], source, env=setup)
    for path in (cache, runtime, source / "target"):
        require(not path.exists(), f"Cold control already contains {path.name}")
    fixture = root / "fixture"
    fixture.mkdir()
    for name in ("alpha", "beta"):
        (fixture / name).write_text(name + "\n")
    try:
        runner.command("daemon-start", [binary, "daemon", "start"])
        runner.command(
            "build", ["cargo", "build", "--release", "--locked", "--offline"], source
        )
        artifact = source / "target/release/eza"
        listing = runner.command(
            "artifact-list", [artifact, "--oneline", "--color=never", fixture]
        )
        require(listing == "alpha\nbeta\n", "Incorrect artifact listing")
        version = runner.command("artifact-version", [artifact, "--version"])
        require("v0.23.5" in version, "Incorrect artifact version")
        dump(
            output / "artifact.json",
            {"sha256": digest(artifact), "version": version, "listing": listing},
        )
        if producer:
            # What kache-action's post step does: without it the arms find no
            # manifest and plan from crate names alone (#1264).
            runner.command("save-manifest", [binary, "save-manifest"], source)
            require(
                any((remote / "artifacts/_manifests/id").rglob("*.json")),
                "Seed published no identity manifest",
            )
        runner.command("stats", [binary, "stats", "--json"])
        require(
            (runtime / "events.jsonl").stat().st_size < 8 * 1024**3,
            "Event log reached its rotation limit",
        )
    finally:
        runner.command("daemon-stop", [binary, "daemon", "stop"])
        start = time.monotonic()
        drain(runtime)
        dump(output / "drain.json", {"seconds": time.monotonic() - start})
        preserve_logs(runtime, output)
    runner.command("report", [binary, "report", "--format", "json", "--record"])
    timeline = runner.command(
        "timeline",
        [
            binary,
            "telemetry",
            "push",
            "--all",
            "--dry-run",
            "--label",
            "prefetch=" + ("on" if enabled else "off"),
        ],
    )
    records = json.loads(timeline)
    raw_path = output / "summaries.jsonl"
    raw_summaries = (
        [json.loads(line) for line in raw_path.read_text().splitlines() if line.strip()]
        if raw_path.exists()
        else []
    )
    lifecycle = lifecycle_evidence(records, raw_summaries)
    dump(output / "lifecycle.json", lifecycle)
    require(not lifecycle["problems"], "; ".join(lifecycle["problems"]))
    if producer:
        require(
            any(
                u.get("demands") and u.get("event_schema", 0) >= 20
                for r in records
                for u in r["units"]
            ),
            "Seed lacks exact demand telemetry",
        )
        require(list(remote.rglob("*.tar.zst")), "Producer published no remote packs")
        manifest = {
            "project": PROJECT,
            "toolchain": TOOLCHAIN,
            "lock_sha256": digest(source / "Cargo.lock"),
            "compiler": compiler,
            "cc": cc,
            "kache_revision": os.environ["GITHUB_SHA"],
            "run_id": os.environ["GITHUB_RUN_ID"],
            "trusted_push": True,
            "files": files(bundle),
        }
        dump(bundle / "manifest.json", manifest)
        with tarfile.open(output / "seed.tar.gz", "w:gz") as tar:
            for path in sorted(bundle.iterdir()):
                tar.add(path, arcname=path.name)
    else:
        require(manifest["files"] == files(bundle), "Consumer mutated read-only seed")
        raw_transfers = [
            json.loads(line)
            for line in (output / "transfers.jsonl").read_text().splitlines()
            if line.strip()
        ]
        dump(
            output / "admission.json",
            summarize(records, enabled, raw_transfers, raw_summaries),
        )
    dump(output / "identity.json", manifest)


def api(path):
    request = urllib.request.Request(
        "https://api.github.com/repos/" + os.environ["GITHUB_REPOSITORY"] + "/" + path,
        headers={
            "Authorization": "Bearer " + os.environ["GH_TOKEN"],
            "Accept": "application/vnd.github+json",
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def jobs(run_id, attempt=None):
    result, page = [], 1
    prefix = f"actions/runs/{run_id}"
    if attempt is not None:
        prefix += f"/attempts/{attempt}"
    while True:
        batch = api(f"{prefix}/jobs?per_page=100&page={page}")["jobs"]
        result.extend(batch)
        if len(batch) < 100:
            return result
        page += 1


def authorize_seed(run, producer_jobs, comparison):
    require(
        run["repository"]["full_name"] == os.environ["GITHUB_REPOSITORY"],
        "Seed belongs to another repository",
    )
    require(
        run["event"] == "push"
        and run["head_branch"] == "main"
        and run["path"] == WORKFLOW
        and run["status"] == "completed",
        "Seed must come from a completed main push of this workflow",
    )
    require(
        comparison["status"] in ("ahead", "identical"),
        "Seed is not an ancestor of main",
    )
    require(
        any(
            j["name"] == "seed" and j["conclusion"] == "success" for j in producer_jobs
        ),
        "Seed producer did not succeed",
    )


def authorize(args):
    require(os.environ.get("GITHUB_REF") == "refs/heads/main", "Run only on main")
    if os.environ["GITHUB_EVENT_NAME"] == "push":
        require(trusted_push(os.environ), "Seed requires protected main")
        run_id = os.environ["GITHUB_RUN_ID"]
        seed_sha = os.environ["GITHUB_SHA"]
    else:
        require(args.seed_run_id.isdecimal(), "Manual run requires numeric seed_run_id")
        run_id = args.seed_run_id
        run = api(f"actions/runs/{run_id}")
        seed_sha = run["head_sha"]
        authorize_seed(
            run,
            jobs(run_id, run["run_attempt"]),
            api(f"compare/{run['head_sha']}...main"),
        )
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write(f"seed_run_id={run_id}\nseed_sha={seed_sha}\n")


def job_durations(all_jobs, strict=True):
    selected, problems = {}, []
    parse = lambda value: datetime.fromisoformat(value.replace("Z", "+00:00"))
    for arm in ARMS:
        matches = [j for j in all_jobs if j["name"] == f"{arm} / consume"]
        if len(matches) != 1:
            problems.append(f"Expected one job for {arm}, got {len(matches)}")
            selected[arm] = {"seconds": None, "conclusion": "missing_or_duplicate"}
            continue
        job = matches[0]
        complete = (
            job["status"] == "completed"
            and job.get("started_at")
            and job.get("completed_at")
        )
        if not complete:
            problems.append(f"{arm} has no complete duration")
        seconds = (
            (parse(job["completed_at"]) - parse(job["started_at"])).total_seconds()
            if complete
            else None
        )
        selected[arm] = {
            "seconds": seconds,
            "conclusion": job["conclusion"],
            "started_at": job.get("started_at"),
            "completed_at": job.get("completed_at"),
            "url": job["html_url"],
        }
    for previous, following in itertools.pairwise(ARMS):
        end, start = (
            selected[previous].get("completed_at"),
            selected[following].get("started_at"),
        )
        if end and start and end > start:
            problems.append("Consumers overlapped or ran out of order")
    if strict:
        require(not problems, "; ".join(problems))
    return selected, problems


def collect(args):
    raw = jobs(os.environ["GITHUB_RUN_ID"], os.environ["GITHUB_RUN_ATTEMPT"])
    dump(args.output / "github-jobs.json", raw)
    durations, problems = job_durations(raw, strict=False)
    producer_jobs = [j for j in raw if j["name"] == "seed"]
    if args.seed_run_id != os.environ["GITHUB_RUN_ID"]:
        seed_run = api(f"actions/runs/{args.seed_run_id}")
        producer_jobs = [
            j
            for j in jobs(args.seed_run_id, seed_run["run_attempt"])
            if j["name"] == "seed"
        ]
        dump(args.output / "seed-run.json", seed_run)
    identities, artifacts, lifecycles, admissions, retention = {}, {}, {}, {}, {}
    for arm in ARMS:
        folder = args.results / ("prefetch-qualification-" + arm)
        for name, target in (("identity", identities), ("artifact", artifacts)):
            path = folder / (name + ".json")
            if path.exists():
                target[arm] = json.loads(path.read_text())
            else:
                problems.append(f"{arm} lacks {name}")
        lifecycle_path = folder / "lifecycle.json"
        if lifecycle_path.exists():
            lifecycles[arm] = json.loads(lifecycle_path.read_text())
            problems.extend(
                f"{arm}: {problem}" for problem in lifecycles[arm]["problems"]
            )
        else:
            problems.append(f"{arm} lacks lifecycle evidence")
        retention_path = folder / "retention.json"
        if retention_path.exists():
            retention[arm] = json.loads(retention_path.read_text())
            problems.extend(
                f"{arm}: retained rotation marker {marker}"
                for marker in retention[arm]["rotation_markers"]
            )
        else:
            problems.append(f"{arm} lacks retention evidence")
        admission = folder / "admission.json"
        if admission.exists():
            admissions[arm] = json.loads(admission.read_text())
        else:
            problems.append(f"{arm} did not pass telemetry admission")
    problems.extend(artifact_problems(artifacts))
    require_same = {json.dumps(value, sort_keys=True) for value in identities.values()}
    if len(require_same) != 1:
        problems.append("Consumers did not share an identical immutable seed")
    pairs = []
    for i in range(1, 4):
        on, off = durations[f"on-{i}"]["seconds"], durations[f"off-{i}"]["seconds"]
        pairs.append(
            {
                "pair": i,
                "on_seconds": on,
                "off_seconds": off,
                "on_minus_off_seconds": on - off
                if on is not None and off is not None
                else None,
            }
        )
    success = not problems and all(
        j["conclusion"] == "success" for j in durations.values()
    )
    dump(
        args.output / "job-times.json",
        {
            "controls_valid": success,
            "complete_precision_qualification": success
            and len(admissions) == len(ARMS)
            and all(
                a.get("complete_precision_qualification") is True
                for a in admissions.values()
            ),
            "problems": problems,
            "consumers": durations,
            "pairs": pairs,
            "identities": identities,
            "artifacts": artifacts,
            "lifecycle": lifecycles,
            "admissions": admissions,
            "retention": retention,
            "run_id": os.environ["GITHUB_RUN_ID"],
            "run_attempt": os.environ["GITHUB_RUN_ATTEMPT"],
            "producer": producer_jobs,
        },
    )
    require(
        success,
        "Controls incomplete or failed; inspect job-times.json and raw artifacts",
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("seed", "consume", "authorize", "collect"))
    parser.add_argument("--root", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--arm", choices=ARMS, default="off-1")
    parser.add_argument("--seed-run-id", default="")
    parser.add_argument("--seed-sha", default="")
    parser.add_argument("--results", type=Path)
    args = parser.parse_args()
    if args.mode == "authorize":
        authorize(args)
    elif args.mode == "collect":
        collect(args)
    else:
        measure(args)


if __name__ == "__main__":
    main()
