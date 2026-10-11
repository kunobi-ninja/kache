"""Validity, distributions and the paired verdict over harness results."""

import math
import statistics

from bench.design import checked, verify_pairs
from bench.inference import analyze

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
        # Not Kache's `errors`: it counts wrapped compiles that exited non-zero,
        # such as build-script probes that fail on purpose. Its cache faults
        # arrive as `invalid_reasons` above.
        if any(
            metrics.get(key, 0)
            for key in (
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


def paired_change(base, head, design=None, indices=None):
    """Describe paired times; infer only from a complete recorded randomization."""
    if not base or not all(positive(x) for x in (*base, *head)):
        raise ValueError("missing or invalid paired wall times")
    changes = [(h / b - 1) * 100 for b, h in zip(base, head, strict=True)]
    effects = [math.log(h) - math.log(b) for b, h in zip(base, head, strict=True)]
    result = {
        "median_pct": statistics.median(changes),
        "geometric_pct": math.expm1(statistics.fmean(effects)) * 100,
        "median_delta_ms": statistics.median(h - b for b, h in zip(base, head, strict=True)),
        "interval_95_pct": None,
        "interval_pct": None,
        "p_value": None,
        "p_adjusted": None,
        "blocks": None,
        "method": "descriptive",
        "outcome": "inconclusive",
        "reason": "no complete recorded randomization; parity is not established",
    }
    if design is None:
        return result
    indices = list(range(len(base))) if indices is None else indices
    by_index = dict(zip(indices, effects, strict=True))
    blocks = [statistics.fmean(by_index[i] for i in block["samples"] if i in by_index)
              for block in design["blocks"]]
    inference = analyze(blocks, design)
    result.update({k: v for k, v in inference.items() if k not in ("effect", "interval")})
    result["geometric_pct"] = math.expm1(inference["effect"]) * 100
    result["interval_pct"] = [None if x is None else math.expm1(x) * 100
                              for x in inference["interval"]]
    result["reason"] = "test against zero change; inconclusive does not establish parity"
    return result


def indexed(records, arm, phase=None):
    selected = {}
    for record in records:
        if record["arm"] != arm or (phase is not None and record["phase"] != phase):
            continue
        sample = record["sample"]
        if type(sample) is not int or sample < 0 or sample in selected:
            raise ValueError(f"{arm}: invalid or duplicate sample")
        selected[sample] = record
    return selected


def summarize(records, design=None, allow_incomplete=False):
    if design is not None:
        checked(design)
        if design["family_size"] < 3:
            raise ValueError("comparison family must include all three isolated phases")
    for arm in {r["arm"] for r in records}:
        indexed(records, arm)
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
            if not positive(result[phase]["wall_ms"]):
                raise ValueError(f"{arm}, {phase}: invalid wall_ms")
            groups.setdefault((arm, phase), []).append(result[phase]["wall_ms"])
    stats = [
        {"arm": arm, "phase": phase, **distribution(values)}
        for (arm, phase), values in groups.items()
    ]
    if len({identity[0] for identity in identities.values()}) != 1:
        raise ValueError("tools measured different source revisions")
    comparisons = []
    failures = []
    base = indexed(records, "base")
    head = indexed(records, "head")
    if base or head:
        if not base or base.keys() != head.keys():
            raise ValueError("incomplete head/base sample pairs")
        for index in base:
            if base[index]["cold_reused"] != head[index]["cold_reused"]:
                raise ValueError("head/base reused different cold seeds")
            if design is not None and base[index]["cold_reused"] != (index % design["cold_every"] != 0):
                raise ValueError("cold seed reuse differs from randomization plan")
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
                    if any(type(value) is not int or value < 0 for value in (bv, hv)):
                        raise ValueError(f"{phase}: invalid {key} counts")
                    if hv > bv:
                        failures.append(
                            f"sample {index}, {phase}: {key} rose {bv} → {hv}"
                        )
        for phase in PHASES:
            indices = [i for i in base if phase != "cold" or not base[i]["cold_reused"]]
            b = [base[i]["result"][phase]["wall_ms"] for i in indices]
            h = [head[i]["result"][phase]["wall_ms"] for i in indices]
            phase_base = {i: base[i] for i in indices}
            phase_head = {i: head[i] for i in indices}
            measured_design = verify_pairs(phase_base, phase_head, design, phase, allow_incomplete)
            change = paired_change(b, h, measured_design, indices)
            comparisons.append({"phase": phase, "n": len(b), **change})
            if change["outcome"] == "regression":
                failures.append(f"{phase}: paired timing regression")
    return {"statistics": stats, "comparisons": comparisons, "failures": failures}


def paired_count_growth(base, head, design=None):
    """Cold duplicate growth uses the same assigned seed blocks as timings."""
    if not all(type(x) is int and x >= 0 for x in (*base, *head)):
        raise ValueError("invalid duplicate compile counts")
    deltas = [h - b for b, h in zip(base, head, strict=True)]
    result = {
        "n": len(deltas),
        "median_delta": statistics.median(deltas),
        "positive_pairs": sum(delta > 0 for delta in deltas),
        "nonzero_pairs": sum(delta != 0 for delta in deltas),
        "p_value": None,
        "p_adjusted": None,
        "outcome": "inconclusive",
        "method": "descriptive",
    }
    if design is not None:
        result.update(analyze(deltas, design))
    return result


def contention_comparison(records, design=None):
    if design is not None:
        checked(design)
        if design["family_size"] < 3:
            raise ValueError("comparison family must include contention timings and counts")
    pairs = {
        arm: {(r["sample"], r["phase"]): r for phase in ("cold", "warm")
              for r in indexed(records, arm, phase).values()}
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
        phase_base = {key[0]: base[key] for key in keys}
        phase_head = {key[0]: head[key] for key in keys}
        measured_design = verify_pairs(phase_base, phase_head, design, phase)
        if design is not None:
            for key in keys:
                expected_seed = key[0] // design["cold_every"]
                if any(row.get("cold_seed") != expected_seed for row in (base[key], head[key])):
                    raise ValueError("contention cold seed differs from randomization plan")
        change = paired_change(
            [base[key]["wall_ms"] for key in keys],
            [head[key]["wall_ms"] for key in keys],
            measured_design, [key[0] for key in keys],
        )
        comparisons.append({"phase": "contention_" + phase, "n": len(keys), **change})
        if change["outcome"] == "regression":
            failures.append(f"contention {phase}: paired timing regression")
        if phase == "cold":
            growth = paired_count_growth(
                [base[key]["events"]["duplicate_key_compiles"] for key in keys],
                [head[key]["events"]["duplicate_key_compiles"] for key in keys],
                measured_design,
            )
            comparisons[-1]["duplicate_key_compiles"] = growth
            if growth["outcome"] == "regression":
                failures.append("contention cold: repeated duplicate key compile growth")
        for key in keys:
            b, h = base[key]["events"], head[key]["events"]
            # Cold work varies with overlap and needs repeated paired evidence.
            # A warm cache must not add compiles, misses or passthroughs.
            if (
                phase == "warm"
                and h["duplicate_key_compiles"] > b["duplicate_key_compiles"]
            ):
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
