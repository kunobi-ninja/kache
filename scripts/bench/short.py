#!/usr/bin/env python3
"""Repeated benchmark measurements; each arm owns its cache and checkout paths."""

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
from bench.engine import MEASUREMENT_TIMEOUT, installed, run_measurement, tool_path
from bench.stats import contention_comparison, summarize, validate

# Subjects with a `bench-<subject>` scenario. The contention stage drives
# Cargo commands of its own and knows only the first three.
PROJECTS = ("hk", "eza", "aube", "opendal", "lance", "llvm")
CONTENTION_PROJECTS = ("hk", "eza", "aube")
# Longer limits for one engine pass where a cold build alone can exceed the
# default. LLVM's takes about 26 minutes on the shared runners.
PASS_TIMEOUT = {"lance": 2400, "llvm": 3600}

# Lives beside the package, in the instrument directory the gate stages.
CONTENTION_SCRIPT = Path(__file__).resolve().parent.parent / "bench-contention.py"

# What the telemetry collector accepts in one artifact. It drops a larger
# payload whole and says so only in its own log, so the limits are enforced
# here, where a run can fail on them.
COLLECTOR_MAX_RESOURCES = 4
COLLECTOR_MAX_SCOPES = 8
INSTRUMENTS = ("gauge", "sum", "histogram")


def group_resources(entries):
    """Merge `resourceMetrics` entries that describe the same resource.

    Every sample writes its own entry, so a run of 14 samples per tool arrives
    as dozens of entries over three resources. Entries with an equal resource
    become one, their scopes and metrics likewise, and every data point is
    kept.
    """

    def identity(value, without):
        return json.dumps(
            {k: v for k, v in value.items() if k != without}, sort_keys=True
        )

    resources = {}
    for entry in entries:
        resource = resources.setdefault(
            identity(entry, "scopeMetrics"),
            ({k: v for k, v in entry.items() if k != "scopeMetrics"}, {}),
        )
        for scope in entry.get("scopeMetrics", []):
            merged_scope = resource[1].setdefault(
                identity(scope, "metrics"),
                ({k: v for k, v in scope.items() if k != "metrics"}, {}),
            )
            for metric in scope.get("metrics", []):
                kind = next((k for k in INSTRUMENTS if k in metric), None)
                if kind is None:
                    raise ValueError(
                        f"telemetry metric {metric.get('name')!r} has no known instrument"
                    )
                shape = {**metric, kind: {**metric[kind], "dataPoints": None}}
                merged = merged_scope[1].setdefault(
                    json.dumps(shape, sort_keys=True),
                    {**metric, kind: {**metric[kind], "dataPoints": []}},
                )
                merged[kind]["dataPoints"].extend(metric[kind].get("dataPoints", []))
    grouped = [
        {
            **head,
            "scopeMetrics": [
                {**scope_head, "metrics": list(metrics.values())}
                for scope_head, metrics in scopes.values()
            ],
        }
        for head, scopes in resources.values()
    ]
    if len(grouped) > COLLECTOR_MAX_RESOURCES:
        raise ValueError(
            f"telemetry has {len(grouped)} resources; the collector accepts {COLLECTOR_MAX_RESOURCES}"
        )
    widest = max((len(r["scopeMetrics"]) for r in grouped), default=0)
    if widest > COLLECTOR_MAX_SCOPES:
        raise ValueError(
            f"telemetry has {widest} scopes in one resource; the collector accepts {COLLECTOR_MAX_SCOPES}"
        )
    return grouped


class DeadlineReached(Exception):
    """The run's time budget ran out. What finished is still reported."""


def run_contention(args, arms, timeout=None):
    output = args.output.resolve() / "contention"
    samples = getattr(args, "contention_samples", None) or args.samples
    cold_every = getattr(args, "contention_cold_every", 3)
    context_samples = min(getattr(args, "context_samples", None) or samples, samples)
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
        str(cold_every),
        "--context-samples",
        str(context_samples),
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
    # Only a run's deadline bounds the whole stage; it can take longer than the
    # isolated engine's timeout.
    print(
        f"{args.project}: contention, {samples} warm batches and {math.ceil(samples / cold_every)} cold seeds per arm; see contention.log",
        flush=True,
    )
    with (args.output / "contention.log").open("w") as stream:
        try:
            run_measurement(
                command,
                timeout=timeout,
                grace=60,
                stdout=stream,
                stderr=subprocess.STDOUT,
            )
        except subprocess.TimeoutExpired:
            raise DeadlineReached(
                "contention was still running when the time budget ran out"
            ) from None
    data = json.loads((output / "samples.json").read_text())
    expected = {
        (sample, arm, phase)
        for sample in range(samples)
        for arm, _, _ in arms
        for phase in ("cold", "warm")
        if (phase == "warm" or sample % cold_every == 0)
        and (arm in VERDICT_ARMS or sample < context_samples)
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
    """Whether to measure this arm: the Kache arms always, sccache and mbx
    only when named.

    The other tools never decide the verdict. The per-PR gate leaves them
    out; the nightly names mbx and the weekly reference names both.
    """
    return arm[2] is not None


# The arms whose timings the gate compares; every other tool is context.
VERDICT_ARMS = ("head", "base", "kache")


def sample_arms(sample, arms, args):
    """The arms a sample measures, in the order it measures them."""
    order = arms if (sample + args.order_seed) % 2 == 0 else list(reversed(arms))
    if sample >= (getattr(args, "context_samples", None) or args.samples):
        order = [a for a in order if a[0] in VERDICT_ARMS]
    return order


def complete_samples(records, arms, args):
    """Records of the samples every arm finished.

    A run stopped by its deadline can end halfway through a sample, and a
    head without its base is not a pair.
    """
    finished = {}
    for record in records:
        finished.setdefault(record["sample"], set()).add(record["arm"])
    return [
        record
        for record in records
        if finished[record["sample"]]
        == {arm for arm, _, _ in sample_arms(record["sample"], arms, args)}
    ]


def run_marker(project, complete, resource):
    """`kache.bench.run.complete`: 1 when every requested measurement ran.

    Each sample reports its own verdict, so a run stopped halfway looks
    healthy sample by sample. This point is what says it stopped.
    """
    return {
        **resource,
        "scopeMetrics": [
            {
                "scope": {"name": "kache.bench.short"},
                "metrics": [
                    {
                        "name": "kache.bench.run.complete",
                        "unit": "1",
                        "gauge": {
                            "dataPoints": [
                                {
                                    "timeUnixNano": str(time.time_ns()),
                                    "asInt": str(int(complete)),
                                    "attributes": [
                                        {
                                            "key": "kache.bench.project",
                                            "value": {"stringValue": f"bench-{project}"},
                                        },
                                        {
                                            "key": "kache.bench.cache_tool",
                                            "value": {"stringValue": "kache"},
                                        },
                                    ],
                                }
                            ]
                        },
                    }
                ],
            }
        ],
    }


def write_telemetry(root, project, records, contention, complete):
    """Every finished measurement's telemetry, plus whether the run completed."""
    # Every sample has its own timestamp. Reused cold observations are not new samples.
    resources = []
    if contention:
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
    if not resources:
        # Nothing finished: the workflow's seeded failure point stands.
        return
    # Beside a resource the payload already has, so it adds no new one.
    first = next(r for r in resources if "scopeMetrics" in r)
    resources.append(
        run_marker(
            project,
            complete,
            {k: v for k, v in first.items() if k != "scopeMetrics"},
        )
    )
    (root / "metrics.otlp.json").write_text(
        json.dumps({"resourceMetrics": group_resources(resources)}) + "\n"
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
                cache_resources.extend(json.loads(source.read_text())["resourceMetrics"])
        if cache_resources:
            dest = root / f"cache-otlp-{phase}"
            dest.mkdir(exist_ok=True)
            (dest / "metrics.otlp.json").write_text(
                json.dumps({"resourceMetrics": group_resources(cache_resources)}) + "\n"
            )
            (dest / "schema_version").write_text("1\n")


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
    # Wall-clock seconds since the epoch, so a workflow can set it when its
    # step starts, before this driver's own setup.
    deadline = getattr(args, "deadline_at", None)
    deadline = None if deadline is None else started + (deadline - time.time())
    # Longest pass so far per arm and cold mode: what the next one will need.
    took = {}
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

    def left():
        return None if deadline is None else deadline - time.monotonic()

    def save():
        payload["elapsed_s"] = time.monotonic() - started
        (root / "samples.json").write_text(json.dumps(payload, indent=2) + "\n")

    contention = False
    try:
        try:
            for sample in range(args.samples):
                for position, (arm, backend, binary) in enumerate(
                    sample_arms(sample, arms, args)
                ):
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
                    remaining = left()
                    needed = took.get((arm, reused))
                    if remaining is not None and (
                        remaining <= 0 or (needed and needed > remaining)
                    ):
                        raise DeadlineReached(
                            f"stopped before sample {sample + 1} of {args.samples} ({arm}): "
                            f"{max(remaining, 0) / 60:.0f} min left"
                            + (f", and the last one took {needed / 60:.0f}" if needed else "")
                        )
                    logs = root / "logs" / f"{sample:02d}-{arm}"
                    logs.mkdir(parents=True)
                    print(
                        f"{args.project}: sample {sample + 1}/{args.samples}, {arm}, cold {'reused' if reused else 'measured'}",
                        flush=True,
                    )
                    env = dict(os.environ, RUSTC_WRAPPER="", RUSTUP_TOOLCHAIN="")
                    limit = PASS_TIMEOUT.get(args.project, MEASUREMENT_TIMEOUT)
                    timeout = limit
                    if remaining is not None and remaining < timeout:
                        timeout = remaining
                    began = time.monotonic()
                    try:
                        with (logs / "engine.log").open("w") as stream:
                            run_measurement(
                                command,
                                timeout=timeout,
                                env=env,
                                stdout=stream,
                                stderr=subprocess.STDOUT,
                            )
                    except subprocess.TimeoutExpired:
                        if timeout < limit:
                            raise DeadlineReached(
                                f"sample {sample + 1} of {args.samples} ({arm}) was still "
                                "running when the time budget ran out"
                            ) from None
                        raise
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
                    took[(arm, reused)] = max(
                        took.get((arm, reused), 0), time.monotonic() - began
                    )
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
                    save()
            measured = records
        except DeadlineReached as reason:
            payload["truncated"] = str(reason)
            measured = complete_samples(records, arms, args)
            if not measured:
                raise ValueError(
                    f"the time budget ran out before one full sample: {reason}"
                ) from None
        summary = summarize(measured)
        # Release isolated-build scratch before allocating six contention targets.
        shutil.rmtree(root / "scratch", ignore_errors=True)
        if (
            args.project in CONTENTION_PROJECTS
            and not getattr(args, "skip_contention", False)
            and "truncated" not in payload
        ):
            try:
                remaining = left()
                if remaining is not None and remaining <= 0:
                    raise DeadlineReached("no time left for contention")
                summary["contention"] = run_contention(args, arms, timeout=remaining)
            except DeadlineReached as reason:
                payload["truncated"] = str(reason)
            else:
                contention = True
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
        if "truncated" in payload:
            # A missing measurement fails the job, but not before the rest is
            # summarized and reported.
            summary["failures"].insert(0, f"incomplete: {payload['truncated']}")
        save()
        (root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        (root / "perf-gate.md").write_text(perf_gate_report.render([root]))
        write_telemetry(
            root, args.project, records, contention, "truncated" not in payload
        )
        return int(bool(summary["failures"]))
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as error:
        payload["error"] = str(error)
        save()
        (root / "perf-gate.md").write_text(
            f"## Perf gate: INVALID MEASUREMENT ({args.project})\n\n{error}\n"
        )
        try:
            write_telemetry(root, args.project, records, False, False)
        except (ValueError, KeyError, OSError) as telemetry:
            print(f"{args.project}: no telemetry written: {telemetry}", flush=True)
        return 1
    finally:
        shutil.rmtree(root / "scratch", ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project",
        choices=PROJECTS,
        required=True,
        help=f"subject to measure; contention runs for {', '.join(CONTENTION_PROJECTS)} only",
    )
    parser.add_argument("--engine", type=Path, required=True)
    parser.add_argument("--scenarios", type=Path, default=Path("scenarios"))
    parser.add_argument("--kache", required=True)
    parser.add_argument("--base")
    parser.add_argument(
        "--sccache",
        help="sccache binary to measure as well; without it there is no sccache arm",
    )
    parser.add_argument(
        "--mbx",
        help="mbx binary to measure as well; without it there is no mbx arm",
    )
    parser.add_argument("--samples", type=int, choices=range(1, 21), default=1)
    parser.add_argument(
        "--order-seed",
        type=int,
        default=int(os.environ.get("BENCH_ORDER_SEED") or 0),
        help="which tool goes first in the first sample (default: $BENCH_ORDER_SEED, else 0). The nightly passes its run number, so the tool that measures cold first changes from night to night",
    )
    parser.add_argument(
        "--contention-samples",
        type=int,
        choices=range(1, 21),
        help="warm batches of Kache per arm; defaults to --samples",
    )
    parser.add_argument(
        "--contention-cold-every",
        type=int,
        choices=range(1, 21),
        default=3,
        help="fresh cold contention seed every N warm batches; use 1 for paired cold verdicts",
    )
    parser.add_argument(
        "--context-samples",
        type=int,
        choices=range(1, 21),
        help="samples of the other tools (sccache, mbx); defaults to --samples. They never decide the verdict, so the gate measures them once and repeats only the kache arms",
    )
    parser.add_argument(
        "--deadline-at",
        type=float,
        metavar="EPOCH_SECONDS",
        default=float(os.environ["BENCH_DEADLINE_AT"])
        if os.environ.get("BENCH_DEADLINE_AT")
        else None,
        help="when the run must end, in seconds since the epoch (default: $BENCH_DEADLINE_AT). A measurement that would not fit is not started, one still running then is stopped, and what finished is summarized, reported and exported before the run fails",
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
    if (
        platform.system() != "Linux"
        and not args.skip_contention
        and args.project in CONTENTION_PROJECTS
    ):
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
    for flag, binary in (
        ("--kache", args.kache),
        ("--base", args.base),
        ("--sccache", args.sccache),
        ("--mbx", args.mbx),
    ):
        if binary is not None and not installed(binary):
            parser.error(f"{flag} is not installed: {binary}")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
