#!/usr/bin/env python3
"""Convert cargo-llvm-cov totals into a small OTLP/HTTP JSON artifact."""

import argparse
import json
import math
import time
from pathlib import Path


SCHEMA_VERSION = 1


def _gauge(name: str, unit: str, value: int | float, timestamp: str) -> dict:
    field = "asInt" if isinstance(value, int) else "asDouble"
    return {
        "name": name,
        "unit": unit,
        "gauge": {
            "dataPoints": [
                {
                    field: value,
                    "timeUnixNano": timestamp,
                    "attributes": [],
                }
            ]
        },
    }


def build_request(report: dict, timestamp: str) -> dict:
    try:
        lines = report["data"][0]["totals"]["lines"]
        count = lines["count"]
        covered = lines["covered"]
        percent = lines["percent"]
    except (KeyError, IndexError, TypeError) as error:
        raise ValueError("llvm-cov report has no line totals") from error

    if (
        not isinstance(count, int)
        or isinstance(count, bool)
        or not isinstance(covered, int)
        or isinstance(covered, bool)
        or not isinstance(percent, (int, float))
        or isinstance(percent, bool)
        or count < 0
        or covered < 0
        or covered > count
        or not math.isfinite(percent)
        or not 0 <= percent <= 100
    ):
        raise ValueError("llvm-cov line totals are invalid")

    return {
        "resourceMetrics": [
            {
                "resource": {
                    "attributes": [
                        {
                            "key": "kache.telemetry.schema_version",
                            "value": {"stringValue": str(SCHEMA_VERSION)},
                        }
                    ]
                },
                "scopeMetrics": [
                    {
                        "scope": {"name": "kache.ci.coverage", "version": "1"},
                        "metrics": [
                            _gauge("kache.ci.coverage.lines", "%", float(percent), timestamp),
                            _gauge(
                                "kache.ci.coverage.lines.covered",
                                "{line}",
                                covered,
                                timestamp,
                            ),
                            _gauge(
                                "kache.ci.coverage.lines.total",
                                "{line}",
                                count,
                                timestamp,
                            ),
                        ],
                    }
                ],
            }
        ]
    }


def write_artifact(report_path: Path, output_dir: Path, timestamp: str) -> None:
    with report_path.open(encoding="utf-8") as stream:
        report = json.load(stream)
    request = build_request(report, timestamp)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "metrics.otlp.json").write_text(
        json.dumps(request, separators=(",", ":")) + "\n", encoding="utf-8"
    )
    (output_dir / "schema_version").write_text(
        f"{SCHEMA_VERSION}\n", encoding="utf-8"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    parser.add_argument("output_dir", type=Path)
    parser.add_argument("--time-unix-nano", default=str(time.time_ns()))
    args = parser.parse_args()
    if not args.time_unix_nano.isdigit():
        parser.error("--time-unix-nano must be an unsigned integer")
    write_artifact(args.report, args.output_dir, args.time_unix_nano)


if __name__ == "__main__":
    main()
