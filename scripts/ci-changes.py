#!/usr/bin/env python3
"""Decide which CI job groups a pull request needs from the files it touches.

Reads one path per line on stdin and prints `key=true|false` lines for
`$GITHUB_OUTPUT`. A file matches the first category whose pattern fits it;
anything unmatched is `code`, which turns every group on. `--all` turns
every group on without reading stdin (pushes, tags, API failures).

Groups:
  check  Check (Linux): fmt, clippy, coverage tests, helm lint, the perf gate
         comparison tests, and everything else `just ci` runs.
  tests  the platform and hardening matrix: cargo test (macOS/Windows),
         mutation testing, Kani, dependency audit, CUDA and CoW jobs.
  e2e    the E2E smoke scenarios on the three platforms.
  nix    the Nix package builds.
"""

import re
import sys

GROUPS = ("check", "tests", "e2e", "nix")

# Ordered: the first matching category wins.
CATEGORIES = (
    (
        "docs",
        (
            r"\.mdx?$",
            r"^docs/",
            r"^notes/",
            r"^assets/",
            r"^LICENSE$",
            r"^\.github/(CODEOWNERS|ISSUE_TEMPLATE/)",
        ),
        (),
    ),
    (
        "bench",
        (
            r"^scripts/(bench-short|test-bench-short|perf-gate-report)\.py$",
            r"^scripts/install-bench-mbx\.sh$",
            # Every `bench*.yml`: the benchmark is split across several
            # workflows now (the Firefox/Windows arms and the sccache
            # comparison have their own), and an unmatched path falls through
            # to "unknown", which runs the entire suite for a comment change.
            r"^\.github/workflows/(bench(-[a-z0-9-]+)?|perf-gate|perf-gate-preflight)\.yml$",
        ),
        ("check",),
    ),
    ("scenarios", (r"^scenarios/",), ("check", "e2e")),
    (
        "packaging",
        (
            r"^packaging/",
            r"^flake\.(nix|lock)$",
            r"^scripts/(apt|aur|ci)/",
            r"^scripts/(check-version-consistency\.sh|crates-io\.py|test-crates-io\.py)$",
            r"^\.github/workflows/(package-publish|service-image)\.yml$",
            r"^\.github/workflows/publish-crates\.yaml$",
        ),
        ("check", "nix"),
    ),
)


def category(path):
    for name, patterns, groups in CATEGORIES:
        if any(re.search(pattern, path) for pattern in patterns):
            return name, groups
    return "code", GROUPS


def decide(paths):
    """Map changed paths to the groups to run, plus each path's category."""
    wanted = set()
    labelled = []
    for path in paths:
        name, groups = category(path)
        labelled.append((path, name))
        wanted.update(groups)
    return {group: group in wanted for group in GROUPS}, labelled


def main(argv):
    if "--all" in argv:
        decision = {group: True for group in GROUPS}
        labelled = []
    else:
        paths = [line.strip() for line in sys.stdin if line.strip()]
        decision, labelled = decide(paths)
    for path, name in labelled:
        print(f"{name:10} {path}", file=sys.stderr)
    for group in GROUPS:
        print(f"{group}={'true' if decision[group] else 'false'}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
