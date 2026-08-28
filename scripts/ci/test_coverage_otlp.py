import json
import tempfile
import unittest
from pathlib import Path

import coverage_otlp


class CoverageOtlpTests(unittest.TestCase):
    def test_writes_line_percentage_and_counts(self):
        report = {
            "data": [
                {
                    "totals": {
                        "lines": {"count": 200, "covered": 178, "percent": 89.0}
                    }
                }
            ]
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_path = root / "coverage.json"
            report_path.write_text(json.dumps(report), encoding="utf-8")
            output = root / "telemetry"

            coverage_otlp.write_artifact(report_path, output, "123")

            body = json.loads((output / "metrics.otlp.json").read_text())
            metrics = body["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
            self.assertEqual(
                [metric["name"] for metric in metrics],
                [
                    "kache.ci.coverage.lines",
                    "kache.ci.coverage.lines.covered",
                    "kache.ci.coverage.lines.total",
                ],
            )
            self.assertEqual(metrics[0]["gauge"]["dataPoints"][0]["asDouble"], 89.0)
            self.assertEqual(metrics[1]["gauge"]["dataPoints"][0]["asInt"], 178)
            self.assertEqual(metrics[2]["gauge"]["dataPoints"][0]["asInt"], 200)
            self.assertEqual((output / "schema_version").read_text(), "1\n")

    def test_rejects_missing_or_invalid_totals(self):
        for report in (
            {},
            {"data": []},
            {
                "data": [
                    {
                        "totals": {
                            "lines": {"count": 10, "covered": 11, "percent": 110.0}
                        }
                    }
                ]
            },
        ):
            with self.subTest(report=report):
                with self.assertRaises(ValueError):
                    coverage_otlp.build_request(report, "123")


if __name__ == "__main__":
    unittest.main()
