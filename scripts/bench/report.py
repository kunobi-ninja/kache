#!/usr/bin/env python3
"""Render the perf-gate pull-request comment from bench-short output directories.

Each directory is one subject's `--output` from bench-short.py. The comment puts
this PR against the branch it merges into first, and folds every other table
away. Readers see "this PR" and the base branch's name; `head` and `base` stay
the arm names in the data.
"""

import argparse
import json
import sys
from pathlib import Path

PHASES = (
    ("cold", "Cold"),
    ("warm_same_tree", "Warm, same path"),
    ("warm", "Warm, other checkout"),
)
CONTENTION_PHASES = (("cold", "Cold"), ("warm", "Warm"))
COMPARISONS = PHASES + tuple(
    (f"contention_{phase}", f"Contention, {label.lower()}")
    for phase, label in CONTENTION_PHASES
)
ARM_ORDER = ("head", "kache", "base", "mbx", "sccache")
MIN_PAIRS = 5
# How far one run of identical builds moves on a CI runner. PRs that did not
# touch the build path swung by up to 22% (eza's sub-second warm build, hk's
# cold build), so a smaller unconfirmed change is shown as noise.
RUNNER_NOISE_PCT = 30
# The paired test's outcomes, in the words the top table uses.
OUTCOMES = {"inconclusive": "unconfirmed", "regression": "slower", "improvement": "faster"}
# How each arm is named in the comment. `base` is set to the base branch.
LABELS = {"head": "this PR", "base": "main"}


def label(arm):
    return LABELS.get(arm, arm)


def number(ms):
    s = ms / 1000
    if s >= 100:
        return f"{s:.0f}"
    if s >= 10:
        return f"{s:.1f}"
    return f"{s:.2f}"


def seconds(ms):
    return "—" if ms is None else f"{number(ms)} s"


def count(value):
    if value is None:
        return "—"
    return f"{value:.0f}" if float(value).is_integer() else f"{value:.1f}"


def timing(row):
    """Median, with the range only when there is more than one sample."""
    if row["n"] == 1:
        return seconds(row["median_ms"])
    return f"{seconds(row['median_ms'])} <sub>{number(row['min_ms'])}–{number(row['max_ms'])}</sub>"


def change(comparison):
    """The change in words. Bold when the paired test confirms it; plain when
    it is larger than CI runners vary between identical builds; otherwise
    "within noise"."""
    if comparison is None:
        return "—"
    pct = comparison["median_pct"]
    size = abs(pct)
    amount = f"{size:.0f}%" if size >= 10 else f"{size:.1f}%"
    text = f"{amount} {'slower' if pct > 0 else 'faster'}"
    if comparison["outcome"] != "inconclusive":
        return f"**{text}**"
    if size < RUNNER_NOISE_PCT:
        return "within noise"
    return text


def arms(rows):
    present = {row["arm"] for row in rows}
    return [arm for arm in ARM_ORDER if arm in present] + sorted(
        present - set(ARM_ORDER)
    )


def load(directory):
    """One subject: its summary, contention samples, or the reason it is invalid."""
    directory = Path(directory)
    project = {"name": directory.name}
    samples = directory / "samples.json"
    if samples.exists():
        payload = json.loads(samples.read_text())
        project["name"] = payload.get("project", project["name"])
        if payload.get("error"):
            project["error"] = payload["error"]
            return project
        project["versions"] = {
            r["arm"]: r["result"]["cache_tool_version"]
            for r in payload.get("records", [])
            if r.get("result", {}).get("cache_tool_version")
        }
        identity = payload.get("identity") or {}
        project["commits"] = {
            arm: identity[key]
            for arm, key in (("head", "BENCH_HEAD_SHA"), ("base", "BENCH_BASE_SHA"))
            if identity.get(key)
        }
    summary = directory / "summary.json"
    if not summary.exists():
        report = directory / "perf-gate.md"
        lines = report.read_text().splitlines()[1:] if report.exists() else []
        project["error"] = (
            "\n".join(lines).strip() or "the measurement produced no summary"
        )
        return project
    project["summary"] = json.loads(summary.read_text())
    contention = directory / "contention" / "samples.json"
    if "contention" in project["summary"] and contention.exists():
        project["contention"] = json.loads(contention.read_text())
    return project


def details(summary, body):
    return [
        "<details>",
        f"<summary>{summary}</summary>",
        "",
        *body,
        "",
        "</details>",
        "",
    ]


def overview(projects):
    """One open table per subject: this PR against its base branch with the
    change, then every other tool, for isolated and contended builds."""
    lines = []
    compared = [p for p in projects if p["summary"]["comparisons"]]
    for p in projects:
        summary = p["summary"]
        contended = summary.get("contention", {}).get("statistics", [])
        columns = arms(summary["statistics"] + contended)
        paired = p in compared and {"head", "base"} <= set(columns)
        if paired:
            others = [arm for arm in columns if arm not in ("head", "base")]
            columns = ["head", "base"] + others
        header = [p["name"]] + [label(arm) for arm in columns]
        align = ["---"] + ["---:"] * len(columns)
        if paired:
            header.insert(3, "Change")
            align.insert(3, "---")
        lines += ["| " + " | ".join(header) + " |", "| " + " | ".join(align) + " |"]
        rows = [(phase, name, summary["statistics"], phase) for phase, name in PHASES]
        if contended:
            rows += [
                (phase, f"Contention, {name.lower()}", contended, f"contention_{phase}")
                for phase, name in CONTENTION_PHASES
            ]
        for phase, name, stats, key in rows:
            by_arm = {row["arm"]: row for row in stats if row["phase"] == phase}
            cells = [name] + [
                timing(by_arm[arm]) if arm in by_arm else "—" for arm in columns
            ]
            if paired:
                comparison = next(
                    (c for c in summary["comparisons"] if c["phase"] == key), None
                )
                cells.insert(3, change(comparison))
            lines.append("| " + " | ".join(cells) + " |")
        lines.append("")
    if compared:
        counts = [c["n"] for p in compared for c in p["summary"]["comparisons"]]
        pairs, fewest = max(counts), min(counts)
        runs = (
            "1 run"
            if pairs == 1
            else f"{pairs} runs" if fewest == pairs else f"{fewest} to {pairs} runs"
        )
        lines.append(
            f"Times are medians of {runs} of each. Identical builds vary by up to {RUNNER_NOISE_PCT}% between runs on CI, so a smaller change is shown as within noise. "
            f"Timing decides the verdict only in bold, where the paired test over {MIN_PAIRS} or more runs confirms it; with fewer, the gate decides on cache misses alone."
        )
        lines.append("")
    notes = [
        "Times exclude setup. Each warm build starts from the cold cache snapshot and an empty build directory. Only this PR against its base decides the verdict; other tools are context."
    ]
    first = next((p.get("contention", {}) for p in projects if "contention" in p), {})
    if "parallelism" in first:
        notes.append(
            f"Contention runs six Cargo builds, {first['parallelism']} at once, each with {first['jobs_per_build']} jobs and its own empty target directory, and each tool shares one store across them."
        )
    return lines + [" ".join(notes), ""]


def contention_counters(projects):
    counters = []
    for p in projects:
        rows = [
            row
            for row in p["summary"].get("contention", {}).get("statistics", [])
            if row.get("compiler_runs") is not None
        ]
        if not rows:
            continue
        counters += [
            f"| {p['name']} | Compiler runs | Duplicate keys | Flight wait | Permit wait |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
        for phase, name in CONTENTION_PHASES:
            by_arm = {row["arm"]: row for row in rows if row["phase"] == phase}
            for arm in arms(by_arm.values()):
                row = by_arm[arm]
                counters.append(
                    f"| {label(arm)}, {name.lower()} | {count(row['compiler_runs'])} | {count(row['duplicate_key_compiles'])} | "
                    f"{seconds(row['flight_wait_ms'])} | {seconds(row['permit_wait_ms'])} |"
                )
        counters.append("")
    if not counters:
        return []
    first = next((p.get("contention", {}) for p in projects if "contention" in p), {})
    reseed = (
        f" Every warm batch starts from its cold seed, and a new seed is measured every {first['cold_every']} samples."
        if "cold_every" in first
        else ""
    )
    counters.append(
        "Medians per batch. Waits add up overlapping events across jobs, so they are not wall-clock savings."
        + reseed
    )
    return details("Contention counters", counters)


def tool_versions(projects):
    """This PR and its base by commit, every other tool by `--version`, per
    subject only where the subjects disagree."""
    per_arm = {}
    for p in projects:
        commits = p.get("commits", {})
        for arm, version in p.get("versions", {}).items():
            shown = commits[arm][:8] if arm in commits else version.removeprefix(f"{arm} ")
            per_arm.setdefault(arm, {}).setdefault(shown, []).append(p["name"])
    if not per_arm:
        return []
    parts = []
    for arm in arms([{"arm": arm} for arm in per_arm]):
        versions = per_arm[arm]
        if len(versions) == 1:
            parts.append(f"{label(arm)} `{next(iter(versions))}`")
        else:
            parts += [
                f"{label(arm)} `{version}` ({', '.join(names)})"
                for version, names in versions.items()
            ]
    return ["Measured: " + ", ".join(parts) + ".", ""]


def disk_use(projects):
    storage = []
    for p in projects:
        records = [r for r in p.get("contention", {}).get("records", []) if "storage" in r]
        if not records:
            continue
        columns = arms(records)
        storage += [
            f"| {p['name']} (GiB) | " + " | ".join(label(arm) for arm in columns) + " |",
            "| --- |" + " ---: |" * len(columns),
        ]
        for phase, name in CONTENTION_PHASES:
            for scope, scope_label in (
                ("cache", "cache"),
                ("targets", "targets"),
                ("cache_and_targets", "total"),
            ):
                cells = []
                for arm in columns:
                    sizes = [
                        r["storage"]["groups"][scope]["allocated_bytes"]
                        for r in records
                        if r["arm"] == arm and r["phase"] == phase
                    ]
                    cells.append(
                        f"{sum(sizes) / len(sizes) / 2**30:.2f}" if sizes else "—"
                    )
                storage.append(
                    f"| After {name.lower()}, {scope_label} | " + " | ".join(cells) + " |"
                )
        storage.append("")
    if not storage:
        return []
    storage.append(
        "Mean allocated blocks after each contention batch. Hardlinks count once, and the total counts blocks shared by the cache and targets once. Reflinked extents are not resolved, so these are not exclusive disk bytes. Sources, Cargo home, logs and cold snapshots are excluded."
    )
    return details("All tools, disk use after contention", storage)


def comparison_detail(projects):
    compared = [p for p in projects if p["summary"]["comparisons"]]
    if not compared:
        return []
    body = [
        "| Build | " + " | ".join(p["name"] for p in compared) + " |",
        "| --- |" + " --- |" * len(compared),
    ]
    for phase, name in COMPARISONS:
        cells = []
        for p in compared:
            c = next((c for c in p["summary"]["comparisons"] if c["phase"] == phase), None)
            if c is None:
                cells.append("—")
                continue
            interval = c["interval_95_pct"]
            bounds = (
                ""
                if interval is None
                else f" ({interval[0]:+.1f}% to {interval[1]:+.1f}%)"
            )
            noun = "pair" if c["n"] == 1 else "pairs"
            cells.append(
                f"{c['median_pct']:+.1f}%{bounds}, {c['n']} {noun}, {OUTCOMES.get(c['outcome'], c['outcome'])}"
            )
        body.append(f"| {name} | " + " | ".join(cells) + " |")
    body += [
        "",
        f"This PR and {label('base')} run as paired samples; positive changes are slower. Parentheses hold the 95% bootstrap interval. A regression needs at least {MIN_PAIRS} pairs, an interval entirely above +5% and a median change above 250 ms; an improvement is the mirror image. More Kache misses or passthroughs than {label('base')} fail the gate whatever the timing. `samples.json` in the run artifacts has source and tool versions, run order and raw reports.",
    ]
    return details("Paired comparison", body)


def render(directories, verdict=None, base_label="main"):
    LABELS["base"] = base_label
    projects = [load(d) for d in directories]
    valid = [p for p in projects if "summary" in p]
    invalid = [p for p in projects if "error" in p]
    failures = [(p["name"], f) for p in valid for f in p["summary"]["failures"]]
    if verdict is None:
        verdict = "INVALID MEASUREMENT" if invalid else "FAIL" if failures else "pass"
    names = ", ".join(p["name"] for p in projects)
    lines = [f"## Perf gate: {verdict} ({names})", ""]
    for p in invalid:
        lines += [f"**{p['name']}: invalid measurement.** {p['error']}", ""]
    if failures:
        lines += ["**Failed checks**", ""]
        lines += [f"- {name}: {failure}" for name, failure in failures]
        lines.append("")
    elif valid and any(p["summary"]["comparisons"] for p in valid):
        lines += [f"This PR adds no cache misses or passthroughs over {label('base')}.", ""]
    if valid:
        lines += overview(valid)
        lines += tool_versions(valid)
        lines += contention_counters(valid)
        lines += disk_use([p for p in valid if "contention" in p])
        lines += comparison_detail(valid)
    return "\n".join(lines).rstrip() + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directories", nargs="+", type=Path)
    parser.add_argument(
        "--verdict", help="headline verdict; derived from the data when omitted"
    )
    parser.add_argument(
        "--base-label", default="main", help="the base branch, as readers know it"
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    text = render(args.directories, args.verdict, args.base_label)
    if args.output:
        args.output.write_text(text)
    else:
        sys.stdout.write(text)


if __name__ == "__main__":
    main()
