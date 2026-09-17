#!/usr/bin/env python3
"""Render the perf-gate pull-request comment from bench-short output directories.

Each directory is one subject's `--output` from bench-short.py. The comment puts
Kache head against its merge base first, and folds every other table away.
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
    if comparison is None:
        return "—"
    text = f"{comparison['median_pct']:+.1f}%"
    if comparison["outcome"] != "inconclusive":
        text = f"**{text} {comparison['outcome']}**"
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


def pivot(title, phases, rows):
    columns = arms(rows)
    by_key = {(row["arm"], row["phase"]): row for row in rows}
    lines = [
        f"| {title} | " + " | ".join(columns) + " |",
        "| --- |" + " ---: |" * len(columns),
    ]
    for phase, label in phases:
        cells = [
            timing(by_key[(arm, phase)]) if (arm, phase) in by_key else "—"
            for arm in columns
        ]
        lines.append(f"| {label} | " + " | ".join(cells) + " |")
    return lines


def details(summary, body, open_=False):
    return [
        "<details open>" if open_ else "<details>",
        f"<summary>{summary}</summary>",
        "",
        *body,
        "",
        "</details>",
        "",
    ]


def head_vs_base(projects):
    compared = [p for p in projects if p["summary"]["comparisons"]]
    if not compared:
        return []
    header = ["Build"]
    for p in compared:
        header += [f"{p['name']} base", f"{p['name']} head", f"{p['name']} change"]
    lines = [
        "| " + " | ".join(header) + " |",
        "| --- |" + " ---: |" * (len(header) - 1),
    ]
    rows = [(phase, label, "statistics") for phase, label in PHASES]
    if any("contention" in p["summary"] for p in compared):
        rows += [
            (phase, f"Contention, {label.lower()}", "contention")
            for phase, label in CONTENTION_PHASES
        ]
    for phase, label, source in rows:
        cells = [label]
        for p in compared:
            summary = p["summary"]
            stats = (
                summary["statistics"]
                if source == "statistics"
                else summary.get("contention", {}).get("statistics", [])
            )
            by_arm = {row["arm"]: row for row in stats if row["phase"] == phase}
            key = phase if source == "statistics" else f"contention_{phase}"
            comparison = next(
                (c for c in summary["comparisons"] if c["phase"] == key), None
            )
            cells += [
                seconds(by_arm["base"]["median_ms"]) if "base" in by_arm else "—",
                seconds(by_arm["head"]["median_ms"]) if "head" in by_arm else "—",
                change(comparison),
            ]
        lines.append("| " + " | ".join(cells) + " |")
    pairs = [c["n"] for p in compared for c in p["summary"]["comparisons"]]
    lines.append("")
    if max(pairs) < MIN_PAIRS:
        noun = "pair" if max(pairs) == 1 else "pairs"
        lines.append(
            f"Times are medians. With {max(pairs)} {noun} per build and {MIN_PAIRS} needed for a timing verdict, every change is inconclusive."
        )
    else:
        lines.append(
            f"Times are medians; positive changes are slower. A change is marked only when it clears the paired test below, so builds with fewer than {MIN_PAIRS} pairs stay inconclusive."
        )
    return lines


def all_tools(projects, open_):
    body = []
    for p in projects:
        body += pivot(p["name"], PHASES, p["summary"]["statistics"]) + [""]
    body.append(
        "Times exclude setup. Each warm build starts from the cold cache snapshot and an empty build directory. Other tools are context and never decide the verdict."
    )
    return details("All tools, isolated builds", body, open_)


def contention(projects):
    measured = [p for p in projects if "contention" in p["summary"]]
    if not measured:
        return []
    body = []
    for p in measured:
        if p["summary"]["contention"]["statistics"]:
            body += pivot(
                p["name"], CONTENTION_PHASES, p["summary"]["contention"]["statistics"]
            ) + [""]
    first = measured[0].get("contention", {})
    if "parallelism" in first:
        body.append(
            f"Six Cargo jobs, {first['parallelism']} at once, each with {first['jobs_per_build']} Cargo jobs and its own empty target directory. Each tool shares one store across the six jobs. Every warm batch starts from its cold seed, and a new seed is measured every {first['cold_every']} samples."
        )
    counters = []
    for p in measured:
        rows = [
            row
            for row in p["summary"]["contention"]["statistics"]
            if row.get("compiler_runs") is not None
        ]
        if not rows:
            continue
        counters += [
            f"| {p['name']} | Compiler runs | Duplicate keys | Flight wait | Permit wait |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
        for phase, label in CONTENTION_PHASES:
            by_arm = {row["arm"]: row for row in rows if row["phase"] == phase}
            for arm in arms(by_arm.values()):
                row = by_arm[arm]
                counters.append(
                    f"| {arm}, {label.lower()} | {count(row['compiler_runs'])} | {count(row['duplicate_key_compiles'])} | "
                    f"{seconds(row['flight_wait_ms'])} | {seconds(row['permit_wait_ms'])} |"
                )
        counters.append("")
    if counters:
        body += [
            "",
            *counters,
            "Medians per batch. Waits add up overlapping events across jobs, so they are not wall-clock savings.",
        ]
    return details("All tools, contention", body)


def disk_use(projects):
    storage = []
    for p in projects:
        records = [r for r in p.get("contention", {}).get("records", []) if "storage" in r]
        if not records:
            continue
        columns = arms(records)
        storage += [
            f"| {p['name']} (GiB) | " + " | ".join(columns) + " |",
            "| --- |" + " ---: |" * len(columns),
        ]
        for phase, label in CONTENTION_PHASES:
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
                    f"| After {label.lower()}, {scope_label} | " + " | ".join(cells) + " |"
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
    for phase, label in COMPARISONS:
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
                f"{c['median_pct']:+.1f}%{bounds}, {c['n']} {noun}, {c['outcome']}"
            )
        body.append(f"| {label} | " + " | ".join(cells) + " |")
    body += [
        "",
        f"Head and base run as paired samples; positive changes are slower. Parentheses hold the 95% bootstrap interval. A regression needs at least {MIN_PAIRS} pairs, an interval entirely above +5% and a median change above 250 ms; an improvement is the mirror image. More Kache misses or passthroughs than base fail the gate whatever the timing. `samples.json` in the run artifacts has source and tool versions, run order and raw reports.",
    ]
    return details("Paired comparison", body)


def render(directories, verdict=None):
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
        lines += ["Kache misses and passthroughs did not rise against base.", ""]
    if valid:
        table = head_vs_base(valid)
        lines += table + ([""] if table else [])
        lines += all_tools(valid, open_=not table)
        lines += contention(valid)
        lines += disk_use([p for p in valid if "contention" in p])
        lines += comparison_detail(valid)
    return "\n".join(lines).rstrip() + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directories", nargs="+", type=Path)
    parser.add_argument(
        "--verdict", help="headline verdict; derived from the data when omitted"
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    text = render(args.directories, args.verdict)
    if args.output:
        args.output.write_text(text)
    else:
        sys.stdout.write(text)


if __name__ == "__main__":
    main()
