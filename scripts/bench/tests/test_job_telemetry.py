#!/usr/bin/env python3
"""A job's duration and conclusion, as the collector will read them."""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from bench import job_telemetry, short


def points(body):
    return {
        metric["name"]: metric["gauge"]["dataPoints"][0]
        for resource in body["resourceMetrics"]
        for scope in resource["scopeMetrics"]
        for metric in scope["metrics"]
    }


def attributes(point):
    return {a["key"]: a["value"]["stringValue"] for a in point["attributes"]}


class JobTelemetryTests(unittest.TestCase):
    def test_a_finished_job_reports_its_duration_and_success(self):
        body = job_telemetry.payload("bench-aube", "kache", 1000, 4840.5, "success", 7)
        p = points(body)
        self.assertEqual(p["kache.bench.job.duration"]["asDouble"], 3840.5)
        self.assertEqual(p["kache.bench.job.ok"]["asInt"], "1")
        self.assertEqual(
            attributes(p["kache.bench.job.ok"]),
            {
                "kache.bench.project": "bench-aube",
                "kache.bench.cache_tool": "kache",
                "kache.bench.conclusion": "success",
            },
        )
        self.assertEqual(p["kache.bench.job.ok"]["timeUnixNano"], "7")
        # One resource and one scope, well inside what the collector accepts.
        self.assertEqual(len(short.group_resources(body["resourceMetrics"])), 1)

    def test_a_failed_or_cancelled_job_is_not_ok(self):
        for status in ("failure", "cancelled"):
            p = points(job_telemetry.payload("bench-hk", "kache", 0, 60, status, 1))
            self.assertEqual(p["kache.bench.job.ok"]["asInt"], "0")
            self.assertEqual(
                attributes(p["kache.bench.job.ok"])["kache.bench.conclusion"], status
            )

    def test_an_unexpected_status_is_not_sent_as_a_value(self):
        p = points(job_telemetry.payload("bench-hk", "kache", 0, 60, "Success ", 1))
        self.assertEqual(
            attributes(p["kache.bench.job.ok"])["kache.bench.conclusion"], "other"
        )
        self.assertEqual(p["kache.bench.job.ok"]["asInt"], "0")

    def test_a_start_after_the_end_is_not_a_negative_duration(self):
        p = points(job_telemetry.payload("bench-hk", "kache", 100, 40, "success", 1))
        self.assertEqual(p["kache.bench.job.duration"]["asDouble"], 0)

    def test_the_command_writes_an_artifact_directory(self):
        script = Path(job_telemetry.__file__)
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "job"
            subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--project",
                    "bench-eza",
                    "--cache-tool",
                    "kache",
                    "--started",
                    "0",
                    "--status",
                    "failure",
                    "--output",
                    str(out),
                ],
                check=True,
            )
            body = json.loads((out / "metrics.otlp.json").read_text())
            self.assertEqual(points(body)["kache.bench.job.ok"]["asInt"], "0")
            self.assertEqual((out / "schema_version").read_text(), "1\n")


if __name__ == "__main__":
    unittest.main()
