#!/usr/bin/env python3
"""Check install pins against the published release and action step boundaries."""

import json
import subprocess
import tempfile
import unittest
from pathlib import Path


class DocumentationTests(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)
        self.write("scripts/check-docs.sh", Path(__file__).with_name("check-docs.sh").read_text())
        self.write("Cargo.toml", '[package]\nversion = "2.0.0-rc.4"\n')
        self.write("docs/install-version.json", json.dumps({"version": "2.0.0-rc.3"}))
        self.write("README.md", "cargo install kache --version 2.0.0-rc.3\n")
        self.write("docs/commands/reference.mdx", "---\ntitle: Commands\n---\n`kache report`\n")
        self.write("crates/kache-client/src/commands.rs", "enum Commands {\n    Report,\n}\n")
        self.write(
            "crates/kache-config/src/config.rs",
            'struct CacheFileConfig {\n    pub read_only: bool,\n}\n/// Deliberately NOT\n'
            'cfg!(target_os = "linux") || cfg!(target_os = "macos")\n',
        )
        self.write(
            "docs/getting-started/configuration.mdx",
            "---\ntitle: Configuration\n---\ncache.read_only cc.extra_allowlist_flags "
            "paths.base_dirs workspace.extra_inputs Linux/macOS: `true`; Windows: `false`\n",
        )

    def write(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def check(self, prose, success=True):
        self.write("README.md", prose)
        result = subprocess.run(
            ["bash", str(self.root / "scripts/check-docs.sh")],
            capture_output=True, text=True, check=False,
        )
        if success:
            self.assertEqual(result.returncode, 0, result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("differs from published install version 2.0.0-rc.3", result.stderr)

    def test_workspace_bump_does_not_change_published_install_pin(self):
        self.check("cargo install kache --version 2.0.0-rc.3\n")
        self.write("Cargo.toml", '[package]\nversion = "3.0.0"\n')
        self.check("cargo binstall kache --version 2.0.0-rc.3\n")

    def test_stale_and_unpublished_cargo_and_nix_pins_fail(self):
        for version in ["2.0.0-rc.2", "2.0.0-rc.4"]:
            for command in [
                f"cargo install kache --locked --version {version}",
                f"cargo binstall kache --version {version}",
                f"nix profile install github:kunobi-ninja/kache/v{version}",
            ]:
                with self.subTest(command=command):
                    self.check(command, success=False)

    def test_published_nix_pin_passes(self):
        self.check("nix profile install github:kunobi-ninja/kache/v2.0.0-rc.3")

    def test_kache_action_pins_are_checked_with_or_without_quotes(self):
        for uses in ["- uses: kunobi-ninja/kache-action@v1", "  uses: 'kunobi-ninja/kache-action@v1'"]:
            for pin in ["v2.0.0-rc.3", '"v2.0.0-rc.3"', "'2.0.0-rc.3' # preview"]:
                with self.subTest(uses=uses, pin=pin):
                    self.check(f"```yaml\n{uses}\n  with:\n    version: {pin}\n```\n")
            self.check(f"```yaml\n{uses}\n  with:\n    version: v2.0.0-rc.2\n```\n", success=False)

    def test_other_actions_and_later_steps_are_ignored(self):
        self.check(
            "```yaml\n- uses: someone/other-action@v1\n  with:\n    version: v9.0.0\n"
            "- uses: kunobi-ninja/kache-action@v1\n  with:\n    version: v2.0.0-rc.3\n"
            "- name: Other version\n  with:\n    version: v9.0.0\n```\n"
        )

    def test_version_outside_action_inputs_is_ignored(self):
        self.check(
            "```yaml\n- uses: kunobi-ninja/kache-action@v1\n  env:\n    version: v9.0.0\n```\n"
            "```yaml\nversion: v9.0.0\n```\n"
        )


if __name__ == "__main__":
    unittest.main()
