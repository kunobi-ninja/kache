#!/usr/bin/env python3
"""Isolated real-CI prefetch control. Uses only Python's standard library."""

import argparse
import fcntl
import hashlib
import itertools
import json
import os
import shutil
import subprocess
import tarfile
import time
import urllib.request
from collections import Counter
from datetime import datetime
from pathlib import Path

PROJECT = "98442ab17c2c3738701b62a7e060b1431ae2d6ea"
TOOLCHAIN = "1.90.0"
WORKFLOW = ".github/workflows/prefetch-qualification.yml"
ARMS = ("off-1", "on-1", "on-2", "off-2", "off-3", "on-3")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def dump(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n")


def digest(path):
    with path.open("rb") as stream:
        value = hashlib.sha256()
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
        return value.hexdigest()


def files(root):
    result = {}
    for path in sorted(root.rglob("*")):
        require(not path.is_symlink(), f"Symlink in seed: {path}")
        if path.is_file() and path != root / "manifest.json":
            result[str(path.relative_to(root))] = digest(path)
    return result


def trusted_push(env):
    return (
        env.get("GITHUB_ACTIONS") == "true"
        and env.get("GITHUB_EVENT_NAME") == "push"
        and env.get("GITHUB_REF") == "refs/heads/main"
        and env.get("GITHUB_REF_TYPE") == "branch"
        and env.get("GITHUB_REF_PROTECTED", "").lower() in ("true", "1", "yes")
    )


def environment(root, binary):
    # Remove inherited Kache settings, never CI trust variables.
    env = {
        key: value for key, value in os.environ.items() if not key.startswith("KACHE_")
    }
    for key in (
        "RUSTC_WRAPPER",
        "RUSTC_WORKSPACE_WRAPPER",
        "CARGO_BUILD_RUSTC_WRAPPER",
        "RUSTFLAGS",
        "CARGO_ENCODED_RUSTFLAGS",
        "CFLAGS",
        "CXXFLAGS",
        "CC",
        "CXX",
    ):
        env.pop(key, None)
    source = root / "source"
    remap = " ".join(
        f"-{kind}-prefix-map={source}=." for kind in ("ffile", "fdebug", "fmacro")
    )
    env.update(
        KACHE_CONFIG=str(root / "config.toml"),
        KACHE_HOST_CONFIG="",
        RUSTUP_TOOLCHAIN=TOOLCHAIN,
        CARGO_INCREMENTAL="0",
        CARGO_BUILD_JOBS="2",
        CARGO_HOME=str(root / "cargo"),
        CARGO_TARGET_DIR=str(source / "target"),
        RUSTC_WRAPPER=str(binary),
        HOST_CC=f"{binary} cc",
        HOST_CXX=f"{binary} c++",
        CC_KNOWN_WRAPPER_CUSTOM="kache",
        KACHE_BASE_DIR=str(source),
        KACHE_INPUT_PREDICTIONS="1",
        KACHE_VERIFY_INPUT_PREDICTIONS="sampled",
        CFLAGS=remap,
        CXXFLAGS=remap,
        SOURCE_DATE_EPOCH="1735689600",
        ZERO_AR_DATE="1",
        LC_ALL="C",
        TZ="UTC",
        NO_COLOR="1",
    )
    return env


class Run:
    def __init__(self, root, output, env):
        self.root, self.output, self.env = root, output, env
        self.phases = []
        output.mkdir(parents=True, exist_ok=True)

    def command(self, name, argv, cwd=None, timeout=3600):
        begin, start = time.time(), time.monotonic()
        code = None
        try:
            with (
                (self.output / f"{name}.out").open("w") as out,
                (self.output / f"{name}.err").open("w") as err,
            ):
                code = subprocess.run(
                    [str(x) for x in argv],
                    cwd=cwd or self.root,
                    env=self.env,
                    stdout=out,
                    stderr=err,
                    timeout=timeout,
                    check=False,
                ).returncode
            require(code == 0, f"{name} failed ({code}); see phase logs")
            return (self.output / f"{name}.out").read_text()
        finally:
            self.phases.append(
                {
                    "name": name,
                    "started_at": begin,
                    "finished_at": time.time(),
                    "seconds": time.monotonic() - start,
                    "exit_code": code,
                }
            )
            dump(self.output / "phases.json", self.phases)


def unpack(archive, destination):
    require(not destination.exists(), "Seed destination must be fresh")
    with tarfile.open(archive, "r:gz") as tar:
        for item in tar.getmembers():
            path = Path(item.name)
            require(
                not path.is_absolute() and ".." not in path.parts, "Unsafe archive path"
            )
            require(
                item.isfile() or item.isdir(), "Seed links/special files are forbidden"
            )
        destination.mkdir()
        for item in tar.getmembers():
            target = destination / item.name
            if item.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                with tar.extractfile(item) as source, target.open("xb") as output:
                    shutil.copyfileobj(source, output)
    manifest = json.loads((destination / "manifest.json").read_text())
    require(
        manifest["project"] == PROJECT and manifest["toolchain"] == TOOLCHAIN,
        "Seed workload mismatch",
    )
    require(manifest["files"] == files(destination), "Seed content hash mismatch")
    require(manifest["trusted_push"] is True, "Seed was not produced by a trusted push")
    return manifest


def drain(runtime):
    with (runtime / "daemon.run.lock").open("a") as lock:
        deadline = time.monotonic() + 120
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return
            except BlockingIOError:
                require(time.monotonic() < deadline, "Daemon did not drain")
                time.sleep(0.1)


# Schema 4 adds TimelineSummary.incomplete to schema 3's ordinary transfer model.
# Reviewed against lifecycle commit 0ac603f944ba546dfc78cb457462537c99a35739.
SUPPORTED_TIMELINE_SCHEMAS = (3, 4)


def lifecycle_evidence(records, raw_summaries=None):
    require(
        records and all(r["schema"] in SUPPORTED_TIMELINE_SCHEMAS for r in records),
        "Only reviewed ordinary timeline schemas 3 and 4 are supported; packed schemas need an adapter",
    )
    summaries, problems, missing = {}, [], []
    known_summary_fields = {
        "plan_id",
        "plan_source",
        "closure_reason",
        "started_at_ms",
        "last_activity_ms",
        "candidate_keys",
        "downloaded_keys",
        "downloaded_bytes",
        "used_keys",
        "demanded_keys",
        "demanded_candidate_keys",
        "cancelled",
    }
    for record in records:
        session, schema = record["session_id"], record["schema"]
        summary = record.get("summary")
        if summary is None:
            if schema == 4 and any(
                t.get("prefetch", {}).get("session_id") == session
                for t in record["transfers"]
                if t.get("prefetch")
            ):
                missing.append(session)
                problems.append(f"{session}: speculative session has no final summary")
            continue
        allowed = known_summary_fields | ({"incomplete"} if schema == 4 else set())
        require(
            not (set(summary) - allowed),
            "Unknown summary fields: review the schema adapter",
        )
        summaries[session] = summary
        if schema == 4 and type(summary.get("incomplete")) is not bool:
            problems.append(
                f"{session}: schema-4 summary lacks an explicit incomplete flag"
            )
        if summary.get("incomplete") is True:
            problems.append(f"{session}: shutdown left outcomes incomplete")
        if summary.get("closure_reason") == "shutdown_timeout":
            problems.append(f"{session}: shutdown timed out")
    raw_schemas = []
    for summary in raw_summaries or []:
        raw_schemas.append(summary["schema"])
        require(summary["schema"] in (1, 2), "Unknown raw summary schema")
        if all(r["schema"] == 4 for r in records) and summary["schema"] != 2:
            problems.append(
                "Schema-4 timeline contains a legacy raw summary without lifecycle evidence"
            )
        if summary["schema"] == 2:
            require(
                type(summary.get("incomplete")) is bool,
                "Raw summary schema 2 requires an explicit incomplete flag",
            )
        if (
            summary.get("incomplete") is True
            or summary.get("closure_reason") == "shutdown_timeout"
        ):
            problems.append(
                f"{summary.get('session_id', '')}: raw summary is incomplete"
            )
    return {
        "timeline_schemas": sorted({r["schema"] for r in records}),
        "raw_summary_schemas": sorted(set(raw_schemas)),
        "summaries": summaries,
        "legacy_shutdown_evidence_unknown": any(r["schema"] == 3 for r in records),
        "missing_summary_sessions": missing,
        "problems": problems,
    }


def summarize(records, enabled):
    lifecycle = lifecycle_evidence(records)
    require(not lifecycle["problems"], "; ".join(lifecycle["problems"]))
    demands, consumed, units, transfers = {}, set(), [], {}
    sessions = {record["session_id"] for record in records}
    allowed_transfer_fields = {
        "crate_name",
        "cache_key",
        "direction",
        "ok",
        "compressed_bytes",
        "original_bytes",
        "started_at_ms",
        "finished_at_ms",
        "network_ms",
        "semaphore_wait_ms",
        "request_count",
        "import_ms",
        "attribution",
        "prefetch",
        "outcome",
    }
    for record in records:
        session = record["session_id"]
        units.extend(record["units"])
        for unit in record["units"]:
            observations = unit.get("demands", [])
            if unit["cache_key"]:
                require(
                    unit.get("event_schema", 0) >= 20 and observations,
                    "Every keyed unit must carry exact schema-20 demands",
                )
                require(
                    any(d["cache_key"] == unit["cache_key"] for d in observations),
                    "Keyed unit lacks its own first-demand observation",
                )
            for demand in observations:
                key = (session, demand["cache_key"])
                at = demand["first_demand_at_ms"]
                require(at > 0, "Invalid demand timestamp")
                demands[key] = min(demands.get(key, at), at)
            if unit["result"] in ("local_hit", "prefetch_hit", "remote_hit"):
                consumed.add((session, unit["cache_key"]))
        for transfer in record["transfers"]:
            require(
                not (set(transfer) - allowed_transfer_fields),
                "Unknown transfer fields: update the schema adapter before qualification",
            )
            # Record association may differ; count a repeated physical event once.
            identity = json.dumps(
                {k: v for k, v in transfer.items() if k != "attribution"},
                sort_keys=True,
            )
            transfers[identity] = transfer
    require(demands, "No exact demand records")
    require(consumed, "No cache artifact was consumed")
    restored = [
        t
        for t in transfers.values()
        if t["direction"] == "download" and t["ok"] and t["original_bytes"] > 0
    ]
    require(restored, "No remote restoration: control is inconclusive")
    speculative = [
        t
        for t in transfers.values()
        if t["direction"] == "download" and t.get("prefetch") is not None
    ]
    require(
        bool(speculative) == enabled, "Prefetch arm did not match observed transfers"
    )
    totals, outcomes, rows, credited = Counter(), Counter(), [], set()
    for transfer in sorted(speculative, key=lambda t: t["finished_at_ms"]):
        origin = transfer["prefetch"]
        key = (origin["session_id"], transfer["cache_key"])
        outcomes[transfer.get("outcome", "unknown")] += 1
        demand = demands.get(key)
        finished = transfer["finished_at_ms"]
        size = transfer["compressed_bytes"]
        require(size >= 0, "Negative received byte count")
        if not transfer["ok"] or transfer.get("outcome") != "completed":
            bucket = "failed"
        elif not key[0] or key[0] not in sessions:
            bucket = "unknown_demand"
        elif demand is None:
            bucket = "unused"
        elif key not in consumed:
            bucket = "demanded_unconsumed"
        elif key in credited:
            bucket = "duplicate"
        elif finished < demand:
            bucket = "useful_before_demand"
            credited.add(key)
        elif finished == demand:
            bucket = "equal_timestamp"
        elif transfer["started_at_ms"] <= demand:
            bucket = "inflight_at_demand"
        else:
            bucket = "late"
        totals[bucket] += size
        rows.append(
            {"transfer": transfer, "first_demand_at_ms": demand, "bucket": bucket}
        )
    denominator = sum(t["compressed_bytes"] for t in speculative)
    return {
        "scope": "Ordinary schema-3/4 logged transfers only; packed and partial physical transfers may be absent",
        "lifecycle": lifecycle,
        "complete_precision_qualification": False,
        "demanded_keys": len(demands),
        "unit_outcomes": dict(Counter(u["result"] for u in units)),
        "remote_wait_ms": sum(
            d["remote_wait_ms"] for u in units for d in u.get("demands", [])
        ),
        "speculative_attempts": len(speculative),
        "speculative_outcomes": dict(outcomes),
        "recorded_received_prefetch_bytes": denominator,
        "recorded_useful_prefetch_bytes": totals["useful_before_demand"],
        "ordinary_recorded_byte_precision": totals["useful_before_demand"] / denominator
        if denominator
        else None,
        "speculative_compressed_bytes": dict(totals),
        "speculative": rows,
    }


def measure(args):
    root, output = args.root.resolve(), args.output.resolve()
    require(not root.exists(), "Measurement root must be new")
    root.mkdir(parents=True)
    output.mkdir(parents=True, exist_ok=True)
    producer = args.mode == "seed"
    enabled = args.arm.startswith("on") if not producer else False
    if producer:
        require(trusted_push(os.environ), "Seed requires a protected main push")
        bundle = root / "bundle"
        bundle.mkdir()
        binary = bundle / "kache"
        shutil.copy2(args.binary, binary)
        remote = bundle / "remote"
        remote.mkdir()
        manifest = {}
    else:
        bundle = root / "bundle"
        manifest = unpack(args.archive, bundle)
        require(str(manifest["run_id"]) == args.seed_run_id, "Unexpected seed run")
        require(manifest["kache_revision"] == args.seed_sha, "Unexpected seed revision")
        binary, remote = bundle / "kache", bundle / "remote"
        # Preserve verified identity even if setup, compilation or admission fails.
        dump(output / "identity.json", manifest)
    binary.chmod(0o755)
    env = environment(root, binary)
    runner = Run(root, output, env)
    cache, runtime = root / "cache", root / "runtime"
    config = f"""[cache]
ignore_env = true
local_store = {json.dumps(str(cache))}
runtime_dir = {json.dumps(str(runtime))}
min_store_compile_ms = 0
record_sessions = true
prefetch_enabled = {str(enabled).lower()}
remote_readonly = {str(not producer).lower()}
event_log_max_size = "8GiB"
[cache.remote]
type = "filesystem"
path = {json.dumps(str(remote))}
prefix = "artifacts"
"""
    (root / "config.toml").write_text(config)
    shutil.copy2(root / "config.toml", output / "config.toml")
    resources = {"cpu_count": os.cpu_count(), "load": os.getloadavg()}
    for path in (
        "/proc/meminfo",
        "/sys/fs/cgroup/cpu.max",
        "/sys/fs/cgroup/memory.max",
    ):
        if Path(path).exists():
            resources[path] = Path(path).read_text()
    dump(output / "resources.json", resources)
    runner.command(
        "clone",
        [
            "git",
            "clone",
            "--depth",
            "1",
            "--branch",
            "v0.23.5",
            "https://github.com/eza-community/eza.git",
            root / "source",
        ],
    )
    source = root / "source"
    require(
        runner.command("source-revision", ["git", "rev-parse", "HEAD"], source).strip()
        == PROJECT,
        "eza tag moved",
    )
    if not producer:
        require(
            digest(source / "Cargo.lock") == manifest["lock_sha256"], "Lockfile drift"
        )
    compiler = runner.command("compiler", ["rustc", "-Vv"])
    cc = runner.command("cc", ["cc", "--version"])
    if not producer:
        require(
            compiler == manifest["compiler"] and cc == manifest["cc"], "Compiler drift"
        )
    runner.command("fetch", ["cargo", "fetch", "--locked"], source)
    for path in (cache, runtime, source / "target"):
        require(not path.exists(), f"Cold control already contains {path.name}")
    fixture = root / "fixture"
    fixture.mkdir()
    for name in ("alpha", "beta"):
        (fixture / name).write_text(name + "\n")
    try:
        runner.command("daemon-start", [binary, "daemon", "start"])
        runner.command(
            "build", ["cargo", "build", "--release", "--locked", "--offline"], source
        )
        artifact = source / "target/release/eza"
        listing = runner.command(
            "artifact-list", [artifact, "--oneline", "--color=never", fixture]
        )
        require(listing == "alpha\nbeta\n", "Incorrect artifact listing")
        version = runner.command("artifact-version", [artifact, "--version"])
        require("v0.23.5" in version, "Incorrect artifact version")
        dump(
            output / "artifact.json",
            {"sha256": digest(artifact), "version": version, "listing": listing},
        )
        runner.command("stats", [binary, "stats", "--json"])
        require(
            (runtime / "events.jsonl").stat().st_size < 8 * 1024**3,
            "Event log reached its rotation limit",
        )
    finally:
        runner.command("daemon-stop", [binary, "daemon", "stop"])
        start = time.monotonic()
        drain(runtime)
        dump(output / "drain.json", {"seconds": time.monotonic() - start})
        for name in ("events.jsonl", "transfers.jsonl", "summaries.jsonl"):
            if (runtime / name).exists():
                shutil.copy2(runtime / name, output / name)
    runner.command("report", [binary, "report", "--format", "json", "--record"])
    timeline = runner.command(
        "timeline",
        [
            binary,
            "telemetry",
            "push",
            "--all",
            "--dry-run",
            "--label",
            "prefetch=" + ("on" if enabled else "off"),
        ],
    )
    records = json.loads(timeline)
    raw_path = output / "summaries.jsonl"
    raw_summaries = (
        [json.loads(line) for line in raw_path.read_text().splitlines() if line.strip()]
        if raw_path.exists()
        else []
    )
    lifecycle = lifecycle_evidence(records, raw_summaries)
    dump(output / "lifecycle.json", lifecycle)
    require(not lifecycle["problems"], "; ".join(lifecycle["problems"]))
    if producer:
        require(
            any(
                u.get("demands") and u.get("event_schema", 0) >= 20
                for r in records
                for u in r["units"]
            ),
            "Seed lacks exact demand telemetry",
        )
        require(list(remote.rglob("*.tar.zst")), "Producer published no remote packs")
        manifest = {
            "project": PROJECT,
            "toolchain": TOOLCHAIN,
            "lock_sha256": digest(source / "Cargo.lock"),
            "compiler": compiler,
            "cc": cc,
            "kache_revision": os.environ["GITHUB_SHA"],
            "run_id": os.environ["GITHUB_RUN_ID"],
            "trusted_push": True,
            "files": files(bundle),
        }
        dump(bundle / "manifest.json", manifest)
        with tarfile.open(output / "seed.tar.gz", "w:gz") as tar:
            for path in sorted(bundle.iterdir()):
                tar.add(path, arcname=path.name)
    else:
        require(manifest["files"] == files(bundle), "Consumer mutated read-only seed")
        dump(output / "admission.json", summarize(records, enabled))
    dump(output / "identity.json", manifest)


def api(path):
    request = urllib.request.Request(
        "https://api.github.com/repos/" + os.environ["GITHUB_REPOSITORY"] + "/" + path,
        headers={
            "Authorization": "Bearer " + os.environ["GH_TOKEN"],
            "Accept": "application/vnd.github+json",
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def jobs(run_id, attempt=None):
    result, page = [], 1
    prefix = f"actions/runs/{run_id}"
    if attempt is not None:
        prefix += f"/attempts/{attempt}"
    while True:
        batch = api(f"{prefix}/jobs?per_page=100&page={page}")["jobs"]
        result.extend(batch)
        if len(batch) < 100:
            return result
        page += 1


def authorize_seed(run, producer_jobs, comparison):
    require(
        run["repository"]["full_name"] == os.environ["GITHUB_REPOSITORY"],
        "Seed belongs to another repository",
    )
    require(
        run["event"] == "push"
        and run["head_branch"] == "main"
        and run["path"] == WORKFLOW
        and run["status"] == "completed",
        "Seed must come from a completed main push of this workflow",
    )
    require(
        comparison["status"] in ("ahead", "identical"),
        "Seed is not an ancestor of main",
    )
    require(
        any(
            j["name"] == "seed" and j["conclusion"] == "success" for j in producer_jobs
        ),
        "Seed producer did not succeed",
    )


def authorize(args):
    require(os.environ.get("GITHUB_REF") == "refs/heads/main", "Run only on main")
    if os.environ["GITHUB_EVENT_NAME"] == "push":
        require(trusted_push(os.environ), "Seed requires protected main")
        run_id = os.environ["GITHUB_RUN_ID"]
        seed_sha = os.environ["GITHUB_SHA"]
    else:
        require(args.seed_run_id.isdecimal(), "Manual run requires numeric seed_run_id")
        run_id = args.seed_run_id
        run = api(f"actions/runs/{run_id}")
        seed_sha = run["head_sha"]
        authorize_seed(
            run,
            jobs(run_id, run["run_attempt"]),
            api(f"compare/{run['head_sha']}...main"),
        )
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write(f"seed_run_id={run_id}\nseed_sha={seed_sha}\n")


def job_durations(all_jobs, strict=True):
    selected, problems = {}, []
    parse = lambda value: datetime.fromisoformat(value.replace("Z", "+00:00"))
    for arm in ARMS:
        matches = [j for j in all_jobs if j["name"] == f"{arm} / consume"]
        if len(matches) != 1:
            problems.append(f"Expected one job for {arm}, got {len(matches)}")
            selected[arm] = {"seconds": None, "conclusion": "missing_or_duplicate"}
            continue
        job = matches[0]
        complete = (
            job["status"] == "completed"
            and job.get("started_at")
            and job.get("completed_at")
        )
        if not complete:
            problems.append(f"{arm} has no complete duration")
        seconds = (
            (parse(job["completed_at"]) - parse(job["started_at"])).total_seconds()
            if complete
            else None
        )
        selected[arm] = {
            "seconds": seconds,
            "conclusion": job["conclusion"],
            "started_at": job.get("started_at"),
            "completed_at": job.get("completed_at"),
            "url": job["html_url"],
        }
    for previous, following in itertools.pairwise(ARMS):
        end, start = (
            selected[previous].get("completed_at"),
            selected[following].get("started_at"),
        )
        if end and start and end > start:
            problems.append("Consumers overlapped or ran out of order")
    if strict:
        require(not problems, "; ".join(problems))
    return selected, problems


def collect(args):
    raw = jobs(os.environ["GITHUB_RUN_ID"], os.environ["GITHUB_RUN_ATTEMPT"])
    dump(args.output / "github-jobs.json", raw)
    durations, problems = job_durations(raw, strict=False)
    producer_jobs = [j for j in raw if j["name"] == "seed"]
    if args.seed_run_id != os.environ["GITHUB_RUN_ID"]:
        seed_run = api(f"actions/runs/{args.seed_run_id}")
        producer_jobs = [
            j
            for j in jobs(args.seed_run_id, seed_run["run_attempt"])
            if j["name"] == "seed"
        ]
        dump(args.output / "seed-run.json", seed_run)
    identities, artifacts, lifecycles = {}, {}, {}
    for arm in ARMS:
        folder = args.results / ("prefetch-qualification-" + arm)
        for name, target in (("identity", identities), ("artifact", artifacts)):
            path = folder / (name + ".json")
            if path.exists():
                target[arm] = json.loads(path.read_text())
            else:
                problems.append(f"{arm} lacks {name}")
        lifecycle_path = folder / "lifecycle.json"
        if lifecycle_path.exists():
            lifecycles[arm] = json.loads(lifecycle_path.read_text())
            problems.extend(
                f"{arm}: {problem}" for problem in lifecycles[arm]["problems"]
            )
        if not (folder / "admission.json").exists():
            problems.append(f"{arm} did not pass telemetry admission")
    require_same = {json.dumps(value, sort_keys=True) for value in identities.values()}
    if len(require_same) != 1:
        problems.append("Consumers did not share an identical immutable seed")
    pairs = []
    for i in range(1, 4):
        on, off = durations[f"on-{i}"]["seconds"], durations[f"off-{i}"]["seconds"]
        pairs.append(
            {
                "pair": i,
                "on_seconds": on,
                "off_seconds": off,
                "on_minus_off_seconds": on - off
                if on is not None and off is not None
                else None,
            }
        )
    success = not problems and all(
        j["conclusion"] == "success" for j in durations.values()
    )
    dump(
        args.output / "job-times.json",
        {
            "controls_valid": success,
            "complete_precision_qualification": False,
            "problems": problems,
            "consumers": durations,
            "pairs": pairs,
            "identities": identities,
            "artifacts": artifacts,
            "lifecycle": lifecycles,
            "run_id": os.environ["GITHUB_RUN_ID"],
            "run_attempt": os.environ["GITHUB_RUN_ATTEMPT"],
            "producer": producer_jobs,
        },
    )
    require(
        success,
        "Controls incomplete or failed; inspect job-times.json and raw artifacts",
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("seed", "consume", "authorize", "collect"))
    parser.add_argument("--root", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--arm", choices=ARMS, default="off-1")
    parser.add_argument("--seed-run-id", default="")
    parser.add_argument("--seed-sha", default="")
    parser.add_argument("--results", type=Path)
    args = parser.parse_args()
    if args.mode == "authorize":
        authorize(args)
    elif args.mode == "collect":
        collect(args)
    else:
        measure(args)


if __name__ == "__main__":
    main()
