#!/usr/bin/env python3
"""Repeated hk/eza measurements; each arm owns its cache and checkout paths."""

import argparse
import json
import math
import os
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path

from bench import report as perf_gate_report
from bench.engine import installed, run_measurement, tool_path
from bench.stats import contention_comparison, summarize, validate

# Lives beside the package, in the instrument directory the gate stages.
CONTENTION_SCRIPT = Path(__file__).resolve().parent.parent / "bench-contention.py"

# Context arms and the bare names they default to. A default that is not
# installed skips its arm; an explicitly named binary never does.
DEFAULT_TOOL = {"sccache": "sccache", "mbx": "mbx"}


def run_contention(args, arms):
    output = args.output.resolve() / "contention"
    samples = getattr(args, "contention_samples", None) or args.samples
    command = [
        sys.executable,
        str(CONTENTION_SCRIPT),
        "--project",
        args.project,
        "--scenarios",
        str(args.scenarios.resolve()),
        "--samples",
        str(samples),
        "--cold-every",
        "3",
        "--order-seed",
        str(args.order_seed),
        "--output",
        str(output),
    ]
    for arm, backend, binary in arms:
        command += (
            ["--arm", f"{arm}={binary},1"]
            if backend == "kache"
            else ["--" + backend, binary]
        )
    # The child enforces a timeout per Cargo job and stops its process groups.
    # Workflow timeouts bound the whole suite; setup and all samples can exceed
    # the isolated engine's 20-minute timeout.
    print(
        f"{args.project}: contention, {samples} warm batches and {math.ceil(samples / 3)} cold seeds per arm; see contention.log",
        flush=True,
    )
    with (args.output / "contention.log").open("w") as stream:
        subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT, check=True)
    data = json.loads((output / "samples.json").read_text())
    expected = {
        (sample, arm, phase)
        for sample in range(samples)
        for arm, _, _ in arms
        for phase in ("cold", "warm")
        if phase == "warm" or sample % 3 == 0
    }
    actual = [(r["sample"], r["arm"], r["phase"]) for r in data["records"]]
    if data.get("error") or set(actual) != expected or len(actual) != len(expected):
        raise ValueError("incomplete contention measurements")
    comparisons, failures = contention_comparison(data["records"])
    return {
        "statistics": json.loads((output / "summary.json").read_text()),
        "comparisons": comparisons,
        "failures": failures,
    }


def cold_is_reused(sample, cold_every):
    """Whether this sample reuses the previous cold build instead of measuring one.

    A cold build is the expensive part of a sample, so the default measures one
    every third and reuses it in between. The comparison then has three warm
    pairs and one cold pair, which is the right trade for a change aimed at
    warm and the wrong one for a change aimed at cold.
    """
    return sample % cold_every != 0


def wanted_arm(arm, args):
    """Whether to measure this arm.

    The Kache arms decide the verdict, so a missing binary there is fatal and
    is caught before any cloning. sccache and mbx are context: their defaults
    are bare names, and on a machine where neither is installed the honest
    answer is a run without them, not a run that clones a subject and then
    dies. Naming one explicitly asks for it, so that stays fatal too.
    """
    name, _, binary = arm
    if name in ("kache", "head", "base") or binary != DEFAULT_TOOL.get(name):
        return True
    if installed(binary):
        return True
    print(f"{name}: not installed, skipping this arm", flush=True)
    return False


def run(args):
    root = args.output.resolve()
    if (root / "samples.json").exists() or (root / "scratch").exists():
        raise ValueError(f"output already contains a run: {root}")
    root.mkdir(parents=True, exist_ok=True)
    arms = [
        ("head" if args.base else "kache", "kache", args.kache),
        ("sccache", "sccache", args.sccache),
        ("mbx", "mbx", args.mbx),
    ]
    if args.base:
        arms.insert(0, ("base", "kache", args.base))
    arms = [arm for arm in arms if wanted_arm(arm, args)]
    arms = [(name, backend, tool_path(binary)) for name, backend, binary in arms]
    records = []
    started = time.monotonic()
    payload = {
        "schema_version": 1,
        "project": args.project,
        "host": platform.node(),
        "platform": f"{platform.system()} {platform.release()} {platform.machine()}",
        "samples_requested": args.samples,
        "cold_every": args.cold_every,
        "identity": {
            key: os.environ.get(key, "")
            for key in (
                "BENCH_HEAD_SHA",
                "BENCH_BASE_SHA",
                "BENCH_INSTRUMENT_SHA",
                "GITHUB_RUN_ID",
            )
        },
        "records": records,
    }
    try:
        for sample in range(args.samples):
            order = (
                arms if (sample + args.order_seed) % 2 == 0 else list(reversed(arms))
            )
            for position, (arm, backend, binary) in enumerate(order):
                scratch = root / "scratch" / arm
                scenario = f"bench-{args.project}" + (
                    "" if backend == "kache" else f"-{backend}"
                )
                command = [
                    str(args.engine.resolve()),
                    "--cache-backend",
                    backend,
                    f"--{backend}",
                    binary,
                    "--scenarios",
                    str(args.scenarios.resolve()),
                    "--select",
                    "suite:bench",
                    "--select",
                    f"backend:{backend}",
                    "--profile",
                    scenario,
                    "--warm-same-tree",
                    "--work-dir",
                    str(scratch),
                ]
                reused = cold_is_reused(sample, args.cold_every)
                if sample:
                    command.append("--skip-clone")
                if reused:
                    command.append("--retry")
                logs = root / "logs" / f"{sample:02d}-{arm}"
                logs.mkdir(parents=True)
                print(
                    f"{args.project}: sample {sample + 1}/{args.samples}, {arm}, cold {'reused' if reused else 'measured'}",
                    flush=True,
                )
                env = dict(os.environ, RUSTC_WRAPPER="", RUSTUP_TOOLCHAIN="")
                try:
                    with (logs / "engine.log").open("w") as stream:
                        run_measurement(
                            command,
                            env=env,
                            stdout=stream,
                            stderr=subprocess.STDOUT,
                        )
                finally:
                    for artifact in scratch.glob("*"):
                        if artifact.is_file() and artifact.suffix in (
                            ".json",
                            ".log",
                            ".txt",
                        ):
                            shutil.copy2(artifact, logs / artifact.name)
                        elif artifact.is_dir() and artifact.name.startswith(
                            "cache-otlp-"
                        ):
                            shutil.copytree(artifact, logs / artifact.name)
                result = json.loads((scratch / f"{scenario}.json").read_text())
                validate(result, backend)
                records.append(
                    {
                        "sample": sample,
                        "position": position,
                        "arm": arm,
                        "backend": backend,
                        "cold_reused": reused,
                        "result": result,
                    }
                )
                payload["elapsed_s"] = time.monotonic() - started
                (root / "samples.json").write_text(json.dumps(payload, indent=2) + "\n")
        summary = summarize(records)
        # Release isolated-build scratch before allocating six contention targets.
        shutil.rmtree(root / "scratch", ignore_errors=True)
        if not getattr(args, "skip_contention", False):
            summary["contention"] = run_contention(args, arms)
            summary["comparisons"].extend(summary["contention"]["comparisons"])
            summary["failures"].extend(summary["contention"]["failures"])
            contention_data = json.loads(
                (root / "contention" / "samples.json").read_text()
            )
            if contention_data["revision"] != records[0]["result"]["git_ref"]:
                raise ValueError(
                    "contention and isolated builds used different source revisions"
                )
            payload["contention_samples"] = "contention/samples.json"
            payload["elapsed_s"] = time.monotonic() - started
            (root / "samples.json").write_text(json.dumps(payload, indent=2) + "\n")
        (root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        (root / "perf-gate.md").write_text(perf_gate_report.render([root]))
        # Every sample has its own timestamp. Reused cold observations are not new samples.
        resources = []
        if "contention" in summary:
            resources.extend(
                json.loads((root / "contention" / "metrics.otlp.json").read_text())[
                    "resourceMetrics"
                ]
            )
        for record in records:
            logs = root / "logs" / f"{record['sample']:02d}-{record['arm']}"
            otlp = json.loads((logs / "metrics.otlp.json").read_text())
            for resource in otlp["resourceMetrics"]:
                for scope in resource["scopeMetrics"]:
                    for metric in scope["metrics"]:
                        if record["cold_reused"]:
                            gauge = metric.get("gauge", {})
                            gauge["dataPoints"] = [
                                point
                                for point in gauge.get("dataPoints", [])
                                if not any(
                                    a["key"] == "kache.bench.phase"
                                    and a["value"].get("stringValue") == "cold"
                                    for a in point.get("attributes", [])
                                )
                            ]
                resources.append(resource)
        (root / "metrics.otlp.json").write_text(
            json.dumps({"resourceMetrics": resources}) + "\n"
        )
        (root / "schema_version").write_text("1\n")
        for phase in ("cold", "warm-same-tree", "warm"):
            cache_resources = []
            for record in records:
                if record["backend"] != "kache" or (
                    phase == "cold" and record["cold_reused"]
                ):
                    continue
                source = (
                    root
                    / "logs"
                    / f"{record['sample']:02d}-{record['arm']}"
                    / f"cache-otlp-{phase}"
                    / "metrics.otlp.json"
                )
                if source.exists():
                    cache_resources.extend(
                        json.loads(source.read_text())["resourceMetrics"]
                    )
            if cache_resources:
                dest = root / f"cache-otlp-{phase}"
                dest.mkdir()
                (dest / "metrics.otlp.json").write_text(
                    json.dumps({"resourceMetrics": cache_resources}) + "\n"
                )
                (dest / "schema_version").write_text("1\n")
        return int(bool(summary["failures"]))
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as error:
        payload["error"] = str(error)
        (root / "samples.json").write_text(json.dumps(payload, indent=2) + "\n")
        (root / "perf-gate.md").write_text(
            f"## Perf gate: INVALID MEASUREMENT ({args.project})\n\n{error}\n"
        )
        return 1
    finally:
        shutil.rmtree(root / "scratch", ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", choices=("hk", "eza"), required=True)
    parser.add_argument("--engine", type=Path, required=True)
    parser.add_argument("--scenarios", type=Path, default=Path("scenarios"))
    parser.add_argument("--kache", required=True)
    parser.add_argument("--base")
    parser.add_argument(
        "--sccache",
        default=DEFAULT_TOOL["sccache"],
        help="sccache binary; the arm is skipped when the default is not installed",
    )
    parser.add_argument(
        "--mbx",
        default=DEFAULT_TOOL["mbx"],
        help="mbx binary; the arm is skipped when the default is not installed",
    )
    parser.add_argument("--samples", type=int, choices=range(1, 21), default=1)
    parser.add_argument("--order-seed", type=int, default=0)
    parser.add_argument(
        "--contention-samples",
        type=int,
        choices=range(1, 21),
        help="warm batches per arm; defaults to --samples, with a fresh cold seed every third sample",
    )
    parser.add_argument(
        "--skip-contention",
        action="store_true",
        help="run only isolated builds for a focused local experiment",
    )
    parser.add_argument(
        "--cold-every",
        type=int,
        choices=range(1, 21),
        default=3,
        help="measure a fresh cold build every Nth sample and reuse it in between; 1 measures every cold build, which is what a change aimed at cold needs",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="directory to write samples.json, logs and scratch into; must not already hold a run",
    )
    args = parser.parse_args()
    if platform.system() != "Linux" and not args.skip_contention:
        parser.error(
            "contention requires Linux; use a Linux runner or --skip-contention for isolated local builds"
        )
    # Checked here rather than on first use: the engine is spawned after the
    # subject is cloned and built, so a wrong path costs minutes before it
    # says so. `--engine` wants kache-scenario, and passing the kache binary
    # instead is the easy mistake.
    if not args.engine.is_file() or not os.access(args.engine, os.X_OK):
        parser.error(
            f"--engine is not an executable file: {args.engine} "
            "(it wants the measuring instrument, target/release/kache-scenario, not the kache binary)"
        )
    for flag, binary in (("--kache", args.kache), ("--base", args.base)):
        if binary is not None and not installed(binary):
            parser.error(f"{flag} is not installed: {binary}")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
