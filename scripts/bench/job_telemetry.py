#!/usr/bin/env python3
"""Write a benchmark job's own duration and conclusion as an OTLP artifact.

The measurements say how fast each build was. They cannot say that a job ran
for five hours, or that it died before measuring anything: a killed job writes
no measurement at all. The workflow runs this as its last step, whatever came
before, so every job reports how long it took and how it ended.
"""

import argparse
import json
import time
from pathlib import Path

# The values GitHub gives `job.status`. Anything else is reported as `other`
# rather than sent as an attribute value nobody reviewed.
CONCLUSIONS = ("success", "failure", "cancelled")


def payload(project, cache_tool, started, finished, status, time_unix_nano):
    conclusion = status if status in CONCLUSIONS else "other"
    attributes = [
        {"key": "kache.bench.project", "value": {"stringValue": project}},
        {"key": "kache.bench.cache_tool", "value": {"stringValue": cache_tool}},
        {"key": "kache.bench.conclusion", "value": {"stringValue": conclusion}},
    ]

    def gauge(name, unit, point):
        return {
            "name": name,
            "unit": unit,
            "gauge": {
                "dataPoints": [
                    {
                        **point,
                        "timeUnixNano": str(time_unix_nano),
                        "attributes": attributes,
                    }
                ]
            },
        }

    return {
        "resourceMetrics": [
            {
                "resource": {"attributes": []},
                "scopeMetrics": [
                    {
                        "scope": {"name": "kache.bench.job"},
                        "metrics": [
                            gauge(
                                "kache.bench.job.duration",
                                "s",
                                {"asDouble": max(finished - started, 0)},
                            ),
                            gauge(
                                "kache.bench.job.ok",
                                "1",
                                {"asInt": str(int(conclusion == "success"))},
                            ),
                        ],
                    }
                ],
            }
        ]
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True, help="bench-<subject>, as the measurements name it")
    parser.add_argument("--cache-tool", required=True)
    parser.add_argument("--started", type=float, required=True, help="job start, seconds since the epoch")
    parser.add_argument("--status", required=True, help="the job's status so far, from `job.status`")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    now = time.time()
    args.output.mkdir(parents=True, exist_ok=True)
    body = payload(
        args.project, args.cache_tool, args.started, now, args.status, time.time_ns()
    )
    (args.output / "metrics.otlp.json").write_text(json.dumps(body) + "\n")
    (args.output / "schema_version").write_text("1\n")


if __name__ == "__main__":
    main()
