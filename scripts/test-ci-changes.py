#!/usr/bin/env python3
"""Every category turns on exactly the job groups that cover it."""

import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location(
    "ci_changes", Path(__file__).with_name("ci-changes.py")
)
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)

ALL = {"check": True, "tests": True, "e2e": True, "nix": True}
NONE = {"check": False, "tests": False, "e2e": False, "nix": False}


class ChangeTests(unittest.TestCase):
    def groups(self, *paths):
        decision, _ = ci.decide(paths)
        return decision

    def test_docs_only_runs_nothing(self):
        self.assertEqual(
            self.groups(
                "README.md",
                "docs/guide.mdx",
                "crates/kache-core/README.md",
                "notes/todo.md",
                "assets/logo.svg",
                "LICENSE",
                ".github/CODEOWNERS",
                ".github/ISSUE_TEMPLATE/bug_report.md",
            ),
            NONE,
        )
        self.assertEqual(self.groups(), NONE, "an empty pull request runs nothing")

    def test_rust_and_unknown_files_run_everything(self):
        for path in (
            "src/cache_key.rs",
            "crates/kache-store/src/lib.rs",
            "tests/integration_test.rs",
            "Cargo.lock",
            "rust-toolchain.toml",
            ".cargo/config.toml",
            ".config/nextest.toml",
            "Justfile",
            "mise.toml",
            "deny.toml",
            "test-projects/hello/Cargo.toml",
            "fuzz/Cargo.toml",
            ".github/workflows/ci.yml",
            ".github/workflows/fuzz.yml",
            ".github/actions/setup-pgp-kms-signing/action.yml",
            ".github/tests/workflow-policy.mjs",
            "scripts/with-test-resources.sh",
            "scripts/ci-changes.py",
            "scripts/test-ci-changes.py",
            "scripts/something-new.sh",
            "flake.nix.bak",
            "docs.rs",
        ):
            self.assertEqual(self.groups(path), ALL, path)

    def test_bench_scripts_run_only_the_linux_check(self):
        self.assertEqual(
            self.groups(
                "scripts/bench-short.py",
                "scripts/test-bench-short.py",
                "scripts/install-bench-mbx.sh",
                ".github/workflows/perf-gate.yml",
                ".github/workflows/perf-gate-preflight.yml",
                ".github/workflows/bench.yml",
            ),
            {"check": True, "tests": False, "e2e": False, "nix": False},
        )

    def test_scenarios_run_check_and_e2e(self):
        self.assertEqual(
            self.groups("scenarios/e2e-cc-parallel/scenario.toml"),
            {"check": True, "tests": False, "e2e": True, "nix": False},
        )

    def test_packaging_runs_check_and_nix(self):
        self.assertEqual(
            self.groups(
                "packaging/nix/package.nix",
                "packaging/charts/kache-service/values.yaml",
                "packaging/docker-bake.hcl",
                "flake.lock",
                "scripts/apt/publish.sh",
                "scripts/aur/vcs-pkgver.sh",
                "scripts/ci/resolve-version.sh",
                "scripts/check-version-consistency.sh",
                "scripts/crates-io.py",
                ".github/workflows/package-publish.yml",
                ".github/workflows/publish-crates.yaml",
                ".github/workflows/service-image.yml",
            ),
            {"check": True, "tests": False, "e2e": False, "nix": True},
        )

    def test_mixed_changes_take_the_union(self):
        self.assertEqual(
            self.groups("scenarios/x/scenario.toml", "flake.lock", "README.md"),
            {"check": True, "tests": False, "e2e": True, "nix": True},
        )
        self.assertEqual(self.groups("README.md", "src/main.rs"), ALL)

    def test_categories_are_reported_per_path(self):
        _, labelled = ci.decide(["README.md", "src/main.rs", "scenarios/a/b"])
        self.assertEqual(
            labelled,
            [("README.md", "docs"), ("src/main.rs", "code"), ("scenarios/a/b", "scenarios")],
        )

    def test_main_prints_github_output_lines(self):
        import contextlib
        import io

        out = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
            with patch_stdin("scripts/bench-short.py\n\n"):
                self.assertEqual(ci.main([]), 0)
        self.assertEqual(
            out.getvalue(), "check=true\ntests=false\ne2e=false\nnix=false\n"
        )
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(ci.main(["--all"]), 0)
        self.assertEqual(out.getvalue(), "check=true\ntests=true\ne2e=true\nnix=true\n")


class patch_stdin:
    def __init__(self, text):
        self.text = text

    def __enter__(self):
        import io
        import sys

        self.saved = sys.stdin
        sys.stdin = io.StringIO(self.text)

    def __exit__(self, *exc):
        import sys

        sys.stdin = self.saved


if __name__ == "__main__":
    unittest.main()
