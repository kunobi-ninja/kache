#!/usr/bin/env python3
"""Run the perf gate's comparison outside CI, on this machine or a sandbox.

`perf-gate.yml` is the only thing that measures a change against its merge base
across every phase, and it only runs on a pull request. Measuring by hand
instead means reassembling it: two kache builds, one instrument, the right
flags, then the renderer. Skipping a step is easy and the usual casualty is a
phase the change was not aimed at.

    scripts/perf-gate-local.py                       # HEAD against origin/main
    scripts/perf-gate-local.py --projects eza --samples 5 --cold-every 1

Same shape as the gate: two kache binaries under test, one measuring
instrument built from the default branch so neither side supplies its own
ruler, and `perf-gate-report.py` deciding the verdict. Exit status is the
gate's: non-zero when a phase regressed.

The working tree is never switched. Each build happens in its own temporary
worktree, so this is safe to run with uncommitted changes -- though what gets
measured is HEAD, not the dirty tree. Commit first.
"""

import argparse
import contextlib
import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
REPO = SCRIPTS.parent


def renderer_from(staging):
    """Load the staged `perf-gate-report.py`, not the checkout's copy.

    The threshold lives in that file. Importing the working tree's version
    would let a change to the threshold pass judgement on itself, which is the
    one thing staging the instrument exists to prevent.
    """
    spec = importlib.util.spec_from_file_location(
        "perf_gate_report", staging / "perf-gate-report.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _fresh(directory):
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def git(*arguments, cwd=REPO):
    return subprocess.run(
        ["git", *arguments], cwd=cwd, capture_output=True, text=True, check=True
    ).stdout.strip()


def resolve(ref):
    try:
        return git("rev-parse", "--verify", f"{ref}^{{commit}}")
    except subprocess.CalledProcessError:
        sys.exit(
            f"perf-gate-local: cannot resolve {ref}. "
            "Fetch it first, for example `git fetch origin main`."
        )


@contextlib.contextmanager
def at_commit(commit, package, binary, label):
    """Build one binary from `commit` in a throwaway worktree.

    A worktree rather than `git switch`: the gate owns a clean checkout and can
    switch freely, but here the caller's tree is very likely dirty, and
    switching it under them to measure something is not a trade worth making.

    Yields the worktree so the caller can take other files from the same
    commit before it is removed.
    """
    print(f"perf-gate-local: building {label} from {commit[:12]}", flush=True)
    tmp = tempfile.mkdtemp(prefix="perf-gate-local-")
    tree = Path(tmp) / "tree"
    git("worktree", "add", "--detach", "--quiet", str(tree), commit)
    try:
        # RUSTC_WRAPPER cleared for the same reason CI clears it: measuring
        # kache through kache would put the thing under test in its own
        # baseline.
        environment = dict(os.environ, RUSTC_WRAPPER="", RUSTUP_TOOLCHAIN="")
        subprocess.run(
            ["cargo", "build", "--release", "-p", package, "--bin", binary],
            cwd=tree,
            env=environment,
            check=True,
        )
        yield tree
    finally:
        git("worktree", "remove", "--force", str(tree))
        shutil.rmtree(tmp, ignore_errors=True)


def build(commit, package, binary, into, label):
    with at_commit(commit, package, binary, label) as tree:
        shutil.copy2(tree / "target" / "release" / binary, into)
    return into


def stage_instrument(commit, staging):
    """The engine, the scenarios and the comparison scripts, all from `commit`.

    All four come from the same place for the reason the gate spells out: a
    change that touches the engine, a scenario or the threshold must not be
    measured by its own version of them. Taking the scripts from the working
    tree instead is the easy slip, and it is invisible in the output.
    """
    with at_commit(commit, "kache-e2e", "kache-scenario", "instrument") as tree:
        shutil.copy2(tree / "target" / "release" / "kache-scenario", staging / "kache-scenario")
        shutil.copytree(tree / "scenarios", staging / "scenarios", dirs_exist_ok=True)
        for name in ("bench-short.py", "bench-contention.py", "perf-gate-report.py"):
            shutil.copy2(tree / "scripts" / name, staging / name)


def measure(project, args, staging, head, base, output):
    command = [
        sys.executable,
        str(staging / "bench-short.py"),
        "--project",
        project,
        "--samples",
        str(args.samples),
        "--cold-every",
        str(args.cold_every),
        "--engine",
        str(staging / "kache-scenario"),
        "--scenarios",
        str(staging / "scenarios"),
        "--kache",
        str(head),
        "--base",
        str(base),
        "--output",
        str(output / project),
    ]
    if args.skip_contention:
        command.append("--skip-contention")
    environment = dict(
        os.environ,
        RUSTC_WRAPPER="",
        RUSTUP_TOOLCHAIN="",
        BENCH_HEAD_SHA=args.head_sha,
        BENCH_BASE_SHA=args.base_sha,
        BENCH_INSTRUMENT_SHA=args.instrument_sha,
    )
    print(f"perf-gate-local: measuring {project}", flush=True)
    return subprocess.run(command, env=environment, check=False).returncode


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-ref",
        default="origin/main",
        help="what HEAD is a delta against; the merge base with it is the baseline",
    )
    parser.add_argument(
        "--instrument-ref",
        default="origin/main",
        help="commit the measuring instrument is built from; neither side under test should supply its own ruler",
    )
    parser.add_argument(
        "--projects",
        nargs="+",
        choices=("hk", "eza"),
        default=["hk", "eza"],
        help="subjects to measure",
    )
    parser.add_argument("--samples", type=int, choices=range(1, 21), default=3)
    parser.add_argument(
        "--cold-every",
        type=int,
        choices=range(1, 21),
        default=3,
        help="pass 1 when the change is aimed at cold, so every sample measures one",
    )
    parser.add_argument(
        "--contention",
        action="store_true",
        help="also run the six-job contention cells (Linux only, and much slower)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="directory for the reports; defaults to a fresh one under target/perf-gate-local",
    )
    args = parser.parse_args()
    args.skip_contention = not args.contention

    head_sha = resolve("HEAD")
    base_sha = git("merge-base", resolve(args.base_ref), head_sha)
    instrument_sha = resolve(args.instrument_ref)
    if base_sha == head_sha:
        sys.exit(
            f"perf-gate-local: HEAD is the merge base with {args.base_ref}; "
            "there is nothing to compare. Commit the change first."
        )
    args.head_sha, args.base_sha, args.instrument_sha = head_sha, base_sha, instrument_sha

    output = args.output or Path(
        tempfile.mkdtemp(prefix="run-", dir=_fresh(REPO / "target" / "perf-gate-local"))
    )
    output.mkdir(parents=True, exist_ok=True)
    staging = output / "instrument"
    staging.mkdir(parents=True, exist_ok=True)

    print(
        f"perf-gate-local: head {head_sha[:12]}, base {base_sha[:12]}, "
        f"instrument {instrument_sha[:12]}\nperf-gate-local: reports in {output}",
        flush=True,
    )

    stage_instrument(instrument_sha, staging)
    base = build(base_sha, "kache", "kache", output / "kache-base", "base kache")
    head = build(head_sha, "kache", "kache", output / "kache-head", "head kache")

    status = 0
    measured = []
    for project in args.projects:
        status = measure(project, args, staging, head, base, output) or status
        if (output / project / "samples.json").exists():
            measured.append(output / project)

    if not measured:
        sys.exit("perf-gate-local: no subject produced a measurement")
    report = renderer_from(staging).render(measured)
    (output / "perf-gate.md").write_text(report)
    print("\n" + report)
    print(f"perf-gate-local: {output / 'perf-gate.md'}")
    return status


if __name__ == "__main__":
    sys.exit(main())
