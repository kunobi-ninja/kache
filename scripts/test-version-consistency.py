#!/usr/bin/env python3
"""Exercise the release gate against isolated workspace manifests."""

import subprocess
import tempfile
import unittest
from pathlib import Path


class VersionConsistencyTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)
        source = Path(__file__).with_name("check-version-consistency.sh")
        self.write("scripts/check-version-consistency.sh", source.read_text())
        self.write(
            "Cargo.toml",
            '[package]\nname = "kache"\nversion = "0.16.1"\n'
            '[workspace]\nmembers = ["crates/*"]\n'
            '[dependencies]\nkache-format = { path = "crates/kache-format", version = "0.16.1" }\n',
        )
        self.write(
            "crates/kache-format/Cargo.toml",
            '[package]\nname = "kache-format"\nversion = "0.16.1"\n',
        )
        self.write(
            "crates/kache-tests/Cargo.toml",
            '[package]\nname = "kache-tests"\nversion = "0.1.0"\npublish = false\n',
        )
        self.write(
            "packaging/charts/kache-service/Chart.yaml",
            'version: 0.16.1\nappVersion: "0.16.1"\n',
        )

    def write(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def run_gate(self, *args):
        return subprocess.run(
            ["bash", str(self.root / "scripts/check-version-consistency.sh"), *args],
            text=True,
            capture_output=True,
            check=False,
        )

    def test_versions_and_tag_agree_while_unpublished_version_differs(self):
        result = self.run_gate("v0.16.1")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_new_publishable_crate_is_checked_without_a_script_edit(self):
        self.write(
            "crates/kache-store/Cargo.toml",
            '[package]\nname = "kache-store"\nversion = "0.16.0"\n',
        )
        result = self.run_gate()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("kache-store version", result.stderr)

    def test_missing_or_stale_dependency_pin_is_rejected(self):
        manifest = (self.root / "Cargo.toml").read_text()
        for pin in ['', ', version = "0.16.0"', ', version = "^0.16.1"']:
            with self.subTest(pin=pin):
                self.write("Cargo.toml", manifest.replace(', version = "0.16.1"', pin))
                result = self.run_gate()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("dependency pin for kache-format", result.stderr)

    def test_unpublished_dependency_cannot_ship(self):
        manifest = (self.root / "crates/kache-format/Cargo.toml").read_text()
        self.write("crates/kache-format/Cargo.toml", manifest + "publish = false\n")
        result = self.run_gate()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unpublished local crate kache-format", result.stderr)

    def test_chart_and_release_tag_must_match(self):
        result = self.run_gate("v0.16.2")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("tag version", result.stderr)
        self.write("packaging/charts/kache-service/Chart.yaml", "version: 0.16.0\nappVersion: 0.16.1\n")
        result = self.run_gate()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Chart.yaml version", result.stderr)


if __name__ == "__main__":
    unittest.main()
