#!/usr/bin/env python3
"""Repeated hk/eza measurements; each arm owns its cache and checkout paths."""

import argparse
import json
import math
import os
from pathlib import Path
import platform
import random
import shutil
import signal
import statistics
import subprocess
import time

PHASES = ("cold", "warm_same_tree", "warm")
LABELS = (
    "cold (empty store)",
    "warm (same path, fresh artifacts)",
    "warm (other checkout)",
)


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


def report(summary, project, path):
    failed = bool(summary["failures"])
    lines = [
        f"## Perf gate: {'FAIL' if failed else 'pass'} — {project}",
        "",
        "Times exclude setup. Each warm phase starts from the cold cache snapshot and an empty build directory.",
        "",
        "| Tool | Phase | Samples | Mean | Median | Min–max |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for s in summary["statistics"]:
        label = LABELS[PHASES.index(s["phase"])]
        lines.append(
            f"| {s['arm']} | {label} | {s['n']} | {s['mean_ms'] / 1000:.3f}s | {s['median_ms'] / 1000:.3f}s | {s['min_ms'] / 1000:.3f}–{s['max_ms'] / 1000:.3f}s |"
        )
    lines += [
        "",
        "Head vs base uses paired samples; positive means slower. Timing needs at least five pairs, a 95% bootstrap interval beyond ±5%, and a median change beyond ±250 ms. Otherwise it is inconclusive. Increased Kache misses or passthroughs fail independently.",
        "",
    ]
    for c in summary["comparisons"]:
        interval = c["interval_95_pct"]
        bounds = (
            "insufficient samples"
            if interval is None
            else f"95% interval {interval[0]:+.1f}% to {interval[1]:+.1f}%"
        )
        lines.append(
            f"- {c['phase']}: {c['median_pct']:+.1f}%, {bounds}; **{c['outcome']}** ({c['n']} pairs)."
        )
    lines += [
        "",
        "Competitor timings are context; they do not decide the PR verdict. Inspect `samples.json` for source/tool versions, run order, and raw reports.",
    ]
    lines += ["", *[f"- {failure}" for failure in summary["failures"]]]
    path.write_text("\n".join(lines) + "\n")


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
    arms = [(name, backend, tool_path(binary)) for name, backend, binary in arms]
    records = []
    started = time.monotonic()
    payload = {
        "schema_version": 1,
        "project": args.project,
        "host": platform.node(),
        "platform": f"{platform.system()} {platform.release()} {platform.machine()}",
        "samples_requested": args.samples,
        "cold_every": 3,
        "identity": dict(
            (key, os.environ.get(key, ""))
            for key in (
                "BENCH_HEAD_SHA",
                "BENCH_BASE_SHA",
                "BENCH_INSTRUMENT_SHA",
                "GITHUB_RUN_ID",
            )
        ),
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
                reused = sample % 3 != 0
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
        (root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        report(summary, args.project, root / "perf-gate.md")
        # Every sample has its own timestamp. Reused cold observations are not new samples.
        resources = []
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
            f"## Perf gate: INVALID MEASUREMENT — {args.project}\n\n{error}\n"
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
    parser.add_argument("--sccache", default="sccache")
    parser.add_argument("--mbx", default="mbx")
    parser.add_argument("--samples", type=int, choices=range(1, 21), default=1)
    parser.add_argument("--order-seed", type=int, default=0)
    parser.add_argument("--output", type=Path, required=True)
    return run(parser.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
