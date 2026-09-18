#!/usr/bin/env python3
"""Repeated hk/eza measurements; each arm owns its cache and checkout paths."""

import argparse
import importlib.util
import json
import math
import os
import platform
import random
import shutil
import signal
import statistics
import subprocess
import sys
import time
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "perf_gate_report", Path(__file__).with_name("perf-gate-report.py")
)
perf_gate_report = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(perf_gate_report)

PHASES = ("cold", "warm_same_tree", "warm")

# Context arms and the bare names they default to. A default that is not
# installed skips its arm; an explicitly named binary never does.
DEFAULT_TOOL = {"sccache": "sccache", "mbx": "mbx"}


def positive(value):
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    )


def validate(result, backend):
    if backend == "kache":
        for key in ("verdict", "warm_same_tree_verdict"):
            if result.get(key, {}).get("ok") is not True:
                raise ValueError(f"{key} is not ok")
    for phase in PHASES:
        metrics = result.get(phase, {})
        if not positive(metrics.get("wall_ms")):
            raise ValueError(f"{phase}: missing or invalid wall_ms")
        if metrics.get("invalid_reasons"):
            raise ValueError(f"{phase}: {metrics['invalid_reasons']}")
        if any(
            metrics.get(key, 0)
            for key in (
                "errors",
                "cache_errors",
                "cache_read_errors",
                "cache_write_errors",
            )
        ):
            raise ValueError(f"{phase}: cache errors")
        if phase != "cold":
            hits = metrics.get("cache_hits" if backend == "sccache" else "hits")
            if not positive(hits):
                raise ValueError(f"{phase}: restored nothing")
            if backend == "kache" and not positive(
                metrics.get("storage", {}).get("restored_bytes")
            ):
                raise ValueError(f"{phase}: restored zero bytes")
    if not result.get("git_ref") or not result.get("cache_tool_version"):
        raise ValueError("missing source revision or cache tool version")


def distribution(values):
    return {
        "n": len(values),
        "mean_ms": statistics.mean(values),
        "median_ms": statistics.median(values),
        "min_ms": min(values),
        "max_ms": max(values),
    }


def paired_change(base, head):
    """Fixed-seed bootstrap of paired relative changes, never independent arms."""
    changes = [(h - b) / b * 100 for b, h in zip(base, head, strict=True)]
    median = statistics.median(changes)
    if len(changes) < 5:
        return {
            "median_pct": median,
            "interval_95_pct": None,
            "outcome": "inconclusive",
        }
    rng = random.Random(0)
    boot = sorted(
        statistics.median(rng.choices(changes, k=len(changes))) for _ in range(10000)
    )
    lo, hi = boot[249], boot[9749]
    absolute = statistics.median(h - b for b, h in zip(base, head, strict=True))
    outcome = "inconclusive"
    if lo > 5 and absolute > 250:
        outcome = "regression"
    elif hi < -5 and absolute < -250:
        outcome = "improvement"
    return {"median_pct": median, "interval_95_pct": [lo, hi], "outcome": outcome}


def summarize(records):
    groups = {}
    identities = {}
    for record in records:
        arm, result = record["arm"], record["result"]
        identity = (result["git_ref"], result["cache_tool_version"])
        if identities.setdefault(arm, identity) != identity:
            raise ValueError(f"{arm}: source or tool changed between samples")
        for phase in PHASES:
            if phase == "cold" and record["cold_reused"]:
                continue
            groups.setdefault((arm, phase), []).append(result[phase]["wall_ms"])
    stats = [
        {"arm": arm, "phase": phase, **distribution(values)}
        for (arm, phase), values in groups.items()
    ]
    if len({identity[0] for identity in identities.values()}) != 1:
        raise ValueError("tools measured different source revisions")
    comparisons = []
    failures = []
    base = {r["sample"]: r for r in records if r["arm"] == "base"}
    head = {r["sample"]: r for r in records if r["arm"] == "head"}
    if base or head:
        if not base or base.keys() != head.keys():
            raise ValueError("incomplete head/base sample pairs")
        for index in base:
            if base[index]["result"]["git_ref"] != head[index]["result"]["git_ref"]:
                raise ValueError("head/base measured different source revisions")
            for phase in PHASES[1:]:
                b, h = base[index]["result"][phase], head[index]["result"][phase]
                for key, bv, hv in (
                    ("misses", b.get("misses"), h.get("misses")),
                    (
                        "passthroughs",
                        b.get("event_log", {}).get("passed_through"),
                        h.get("event_log", {}).get("passed_through"),
                    ),
                ):
                    if bv is None or hv is None:
                        raise ValueError(f"{phase}: missing {key} counts")
                    if hv > bv:
                        failures.append(
                            f"sample {index}, {phase}: {key} rose {bv} → {hv}"
                        )
        for phase in PHASES:
            indices = [i for i in base if phase != "cold" or not base[i]["cold_reused"]]
            b = [base[i]["result"][phase]["wall_ms"] for i in indices]
            h = [head[i]["result"][phase]["wall_ms"] for i in indices]
            change = paired_change(b, h)
            comparisons.append({"phase": phase, "n": len(b), **change})
            if change["outcome"] == "regression":
                failures.append(f"{phase}: paired timing regression")
    return {"statistics": stats, "comparisons": comparisons, "failures": failures}


def run_measurement(command, **kwargs):
    # A timed-out engine can leave compiler children alive. Stop the whole
    # measurement group before its scratch directory is removed.
    with subprocess.Popen(command, start_new_session=True, **kwargs) as process:
        try:
            status = process.wait(timeout=1200)
        except BaseException:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            raise
    if status:
        raise subprocess.CalledProcessError(status, command)


def contention_comparison(records):
    pairs = {
        arm: {(r["sample"], r["phase"]): r for r in records if r["arm"] == arm}
        for arm in ("base", "head")
    }
    base, head = pairs["base"], pairs["head"]
    if not base and not head:
        return [], []
    if not base or base.keys() != head.keys():
        raise ValueError("incomplete contention head/base sample pairs")
    failures, comparisons = [], []
    for phase in ("cold", "warm"):
        keys = sorted(key for key in base if key[1] == phase)
        change = paired_change(
            [base[key]["wall_ms"] for key in keys],
            [head[key]["wall_ms"] for key in keys],
        )
        comparisons.append({"phase": "contention_" + phase, "n": len(keys), **change})
        if change["outcome"] == "regression":
            failures.append(f"contention {phase}: paired timing regression")
        for key in keys:
            b, h = base[key]["events"], head[key]["events"]
            # Cold hit/miss counts vary with overlap. Duplicate compilation of
            # the same key is the useful cold correctness signal.
            if h["duplicate_key_compiles"] > b["duplicate_key_compiles"]:
                failures.append(
                    f"contention {phase} sample {key[0]}: duplicate key compiles increased"
                )
            if phase == "warm":
                for result in ("miss", "passthrough"):
                    if h["results"].get(result, 0) > b["results"].get(result, 0):
                        failures.append(
                            f"contention warm sample {key[0]}: {result} count increased"
                        )
    return comparisons, failures


def run_contention(args, arms):
    output = args.output.resolve() / "contention"
    samples = getattr(args, "contention_samples", None) or args.samples
    command = [
        sys.executable,
        str(Path(__file__).with_name("bench-contention.py")),
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


def tool_path(binary):
    """Absolute path of a tool, real binary rather than a mise shim.

    A mise shim is a symlink to the mise binary that dispatches on argv[0].
    Following it gives `.../mise`, which invoked as `mise --start-server`
    fails; ask mise where the tool really is instead.
    """
    found = Path(shutil.which(binary) or binary).absolute()
    if found.is_symlink() and Path(os.path.realpath(found)).stem == "mise":
        real = subprocess.run(
            ["mise", "which", binary], capture_output=True, text=True, check=False
        )
        target = real.stdout.strip()
        if real.returncode == 0 and target:
            return str(Path(target).absolute())
        return str(found)
    return str(found.resolve())


def cold_is_reused(sample, cold_every):
    """Whether this sample reuses the previous cold build instead of measuring one.

    A cold build is the expensive part of a sample, so the default measures one
    every third and reuses it in between. The comparison then has three warm
    pairs and one cold pair, which is the right trade for a change aimed at
    warm and the wrong one for a change aimed at cold.
    """
    return sample % cold_every != 0


def installed(binary):
    return Path(binary).is_file() or shutil.which(binary) is not None


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
