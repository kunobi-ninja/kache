#!/usr/bin/env python3
"""APT indexes advertise only payloads already present in the repository."""

import json
import os
import shutil
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent

RCLONE_FIXTURE = r'''
import json
import os
from pathlib import Path
import shutil
import sys

operation, source, target = sys.argv[1:4]
remote = Path(os.environ["APT_TEST_REMOTE"])
prefix = "r2:fixture/kache/"
if not target.startswith(prefix):
    raise SystemExit("unexpected remote")
destination = remote / target[len(prefix):]
missing = []
if operation == "sync" and "/dists/" in target:
    for line in (Path(source) / "Packages").read_text().splitlines():
        if line.startswith("Filename: "):
            filename = line.removeprefix("Filename: ")
            if not (remote / "apt" / filename).is_file():
                missing.append(filename)
with open(os.environ["APT_TEST_CALLS"], "a") as log:
    log.write(json.dumps({"args": sys.argv[1:], "missing_payloads": missing}) + "\n")
if target.endswith("/pool/") and os.environ["APT_TEST_FAIL_POOL"] != "0":
    if os.environ["APT_TEST_FAIL_POOL"] == "partial":
        payload = sorted(path for path in Path(source).rglob("*") if path.is_file())[0]
        uploaded = destination / payload.relative_to(source)
        uploaded.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(payload, uploaded)
    print("injected package upload failure", file=sys.stderr)
    raise SystemExit(9)
if operation == "sync":
    if destination.exists():
        shutil.rmtree(destination)
    shutil.copytree(source, destination)
elif operation == "copy":
    shutil.copytree(source, destination, dirs_exist_ok=True)
elif operation == "copyto":
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
else:
    raise SystemExit("unexpected operation")
'''


class AptPublicationTests(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory(prefix="apt-publication-")
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)
        self.local = self.root / "repo"
        self.remote = self.root / "remote"
        self.calls_path = self.root / "calls.jsonl"
        tools = self.root / "bin"
        tools.mkdir()
        rclone = tools / "rclone"
        rclone.write_text(f"#!{sys.executable}\n" + RCLONE_FIXTURE)
        rclone.chmod(0o755)
        self.env = {"PATH": os.defpath}
        self.env.update(
            PATH=str(tools) + os.pathsep + self.env["PATH"],
            HOME=str(self.root / "home"),
            R2_ACCESS_KEY_ID="fixture",
            R2_SECRET_ACCESS_KEY="fixture",
            R2_ACCOUNT_ID="fixture",
            R2_BUCKET="fixture",
            APT_REPO_DIR=str(self.local),
            APT_TEST_REMOTE=str(self.remote),
            APT_TEST_CALLS=str(self.calls_path),
        )
        (self.local / "pool/main/k/kache").mkdir(parents=True)
        (self.local / "pool/main/k/kache/new.deb").write_bytes(b"new package")
        (self.local / "pool/main/k/kache/second.deb").write_bytes(b"second package")
        (self.local / "db").mkdir()
        (self.local / "db/packages.db").write_bytes(b"new state")
        (self.local / "gpg.key").write_bytes(b"public key")

    def reset_remote(self):
        if self.remote.exists():
            shutil.rmtree(self.remote)
        self.calls_path.unlink(missing_ok=True)
        (self.remote / "apt/pool").mkdir(parents=True)
        (self.remote / "apt/pool/old.deb").write_bytes(b"old package")
        for suite in ("stable", "unstable"):
            local_suite = self.local / "dists" / suite
            local_suite.mkdir(parents=True, exist_ok=True)
            (local_suite / "Packages").write_text(
                "Package: kache\nFilename: pool/main/k/kache/new.deb\n"
            )
            (local_suite / "InRelease").write_bytes(b"new signed metadata")
            remote_suite = self.remote / "apt/dists" / suite
            remote_suite.mkdir(parents=True)
            (remote_suite / "Packages").write_bytes(b"old index")
            (remote_suite / "InRelease").write_bytes(b"old signed metadata")
        (self.remote / "apt-state/db").mkdir(parents=True)
        (self.remote / "apt-state/db/packages.db").write_bytes(b"old state")

    def publish(self, channel, fail_pool=False):
        if fail_pool == "partial":
            pool_failure = "partial"
        else:
            pool_failure = "1" if fail_pool else "0"
        env = {**self.env, "APT_CHANNEL": channel,
               "APT_TEST_FAIL_POOL": pool_failure}
        result = subprocess.run(
            ["bash", str(ROOT / "scripts/apt/publish.sh")],
            cwd=ROOT, env=env, capture_output=True, text=True, timeout=10,
        )
        calls = ([json.loads(line) for line in self.calls_path.read_text().splitlines()]
                 if self.calls_path.exists() else [])
        return result, calls

    def command(self, operation, relative, progress=True):
        target = "apt-state/db/" if relative == "db/" else "apt/" + relative
        return [operation, str(self.local / relative) + ("/" if relative.endswith("/") else ""),
                "r2:fixture/kache/" + target,
                *(["--progress"] if progress else []), "--checksum"]

    def test_packages_exist_before_each_suite_index_is_published(self):
        for channel, suites in (("stable", ("stable", "unstable")),
                                ("unstable", ("unstable",))):
            with self.subTest(channel=channel):
                self.reset_remote()
                result, calls = self.publish(channel)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(
                    [call["missing_payloads"] for call in calls if call["args"][0] == "sync"],
                    [[] for _ in range(len(suites) + 1)],
                    "publication exposed an index before its package payload existed",
                )
                expected = [self.command("copy", "pool/")]
                expected += [self.command("sync", f"dists/{suite}/") for suite in suites]
                expected += [self.command("copyto", "gpg.key", False),
                             self.command("sync", "db/", False)]
                self.assertEqual([call["args"] for call in calls], expected)
                self.assertEqual((self.remote / "apt/pool/old.deb").read_bytes(), b"old package")
                self.assertEqual((self.remote / "apt/pool/main/k/kache/new.deb").read_bytes(), b"new package")
                self.assertEqual((self.remote / "apt/pool/main/k/kache/second.deb").read_bytes(), b"second package")
                for suite in suites:
                    self.assertEqual((self.remote / "apt/dists" / suite / "Packages").read_bytes(),
                                     (self.local / "dists" / suite / "Packages").read_bytes())
                if channel == "unstable":
                    self.assertEqual((self.remote / "apt/dists/stable/Packages").read_bytes(), b"old index")
                self.assertEqual((self.remote / "apt-state/db/packages.db").read_bytes(), b"new state")

    def test_partial_package_upload_keeps_live_indexes_and_state_unchanged(self):
        for channel in ("stable", "unstable"):
            with self.subTest(channel=channel):
                self.reset_remote()
                result, calls = self.publish(channel, fail_pool="partial")
                self.assertEqual(result.returncode, 9, result.stderr)
                self.assertEqual((self.remote / "apt/pool/main/k/kache/new.deb").read_bytes(),
                                 b"new package")
                self.assertFalse((self.remote / "apt/pool/main/k/kache/second.deb").exists())
                for suite in ("stable", "unstable"):
                    self.assertEqual((self.remote / "apt/dists" / suite / "Packages").read_bytes(),
                                     b"old index")
                    self.assertEqual((self.remote / "apt/dists" / suite / "InRelease").read_bytes(),
                                     b"old signed metadata")
                self.assertEqual((self.remote / "apt-state/db/packages.db").read_bytes(), b"old state")
                self.assertEqual((self.remote / "apt/pool/old.deb").read_bytes(), b"old package")
                self.assertEqual([call["args"] for call in calls], [self.command("copy", "pool/")])

    def test_inherited_shell_startup_file_is_not_executed(self):
        startup = self.root / "inherited-startup.sh"
        startup.write_text("exit 42\n")
        with patch.dict(os.environ, {"BASH_ENV": str(startup)}):
            self.setUp()
        self.reset_remote()
        result, _ = self.publish("stable")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_package_upload_failure_keeps_live_indexes_unchanged(self):
        for channel in ("stable", "unstable"):
            with self.subTest(channel=channel):
                self.reset_remote()
                result, calls = self.publish(channel, fail_pool=True)
                self.assertEqual(result.returncode, 9)
                self.assertIn("injected package upload failure", result.stderr)
                for suite in ("stable", "unstable"):
                    self.assertEqual((self.remote / "apt/dists" / suite / "Packages").read_bytes(),
                                     b"old index", "package upload failure replaced a live index")
                    self.assertEqual((self.remote / "apt/dists" / suite / "InRelease").read_bytes(),
                                     b"old signed metadata")
                self.assertEqual([call["args"] for call in calls], [self.command("copy", "pool/")])
                self.assertEqual((self.remote / "apt-state/db/packages.db").read_bytes(), b"old state")
                self.assertFalse((self.remote / "apt/pool/main/k/kache/new.deb").exists())
                self.assertEqual((self.remote / "apt/pool/old.deb").read_bytes(), b"old package")


if __name__ == "__main__":
    unittest.main()
