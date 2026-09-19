"""Validity, distributions and the paired verdict over harness results."""

import math
import random
import statistics

PHASES = ("cold", "warm_same_tree", "warm")


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
