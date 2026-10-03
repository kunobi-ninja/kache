#!/usr/bin/env python3
"""Measure overlapping hk/eza/aube Cargo jobs against a shared compiler cache on Linux."""

import argparse
import hashlib
import json
import os
import platform
import shutil
import signal
import stat
import statistics
import subprocess
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import tomllib

JOBS = (
    ("check", ["check", "--locked"]),
    ("check-all", ["check", "--all-features", "--all-targets", "--locked"]),
    ("clippy", ["clippy", "--locked"]),
    ("clippy-all", ["clippy", "--all-features", "--all-targets", "--locked"]),
    ("test", ["test", "--no-run", "--locked"]),
    ("test-all", ["test", "--all-features", "--all-targets", "--no-run", "--locked"]),
)


def workload_for(project, full_features=False):
    # eza's all-features graph enables vendored OpenSSL. Keep that stress
    # graph opt-in; default/minimal still exercises Rust and bundled libgit2.
    if project == "eza" and not full_features:
        return tuple(
            (
                name.replace("-all", "-minimal"),
                [
                    "--no-default-features" if arg == "--all-features" else arg
                    for arg in command
                ],
            )
            for name, command in JOBS
        )
    return JOBS


TOTALS = (
    "compiler_runs",
    "preprocessor_runs",
    "dep_info_runs",
    "key_ms",
    "dep_info_ms",
    "flight_wait_ms",
    "permit_wait_ms",
    "restore_ms",
    "lookup_ms",
    "store_ms",
    "startup_ms",
    "daemon_store_ms",
    "prediction_mismatches",
)


def capture(command, **kwargs):
    return subprocess.check_output(command, text=True, **kwargs).strip()


def counters(path):
    try:
        return {
            key: int(value)
            for key, value in (line.split() for line in path.read_text().splitlines())
        }
    except (OSError, ValueError):
        return {}


def cgroup_path():
    for line in Path("/proc/self/cgroup").read_text().splitlines():
        if line.startswith("0::"):
            relative = line[3:].lstrip("/")
            candidate = Path("/sys/fs/cgroup") / relative
            if (candidate / "cpu.stat").exists():
                return candidate
    return Path("/sys/fs/cgroup")


def machine():
    root = cgroup_path()
    result = {
        "host": platform.node(),
        "platform": platform.platform(),
        "affinity": sorted(os.sched_getaffinity(0)),
        "cgroup": str(root),
    }
    for name in ("cpu.max", "memory.max", "cpuset.cpus.effective"):
        path = root / name
        result[name] = path.read_text().strip() if path.exists() else None
    result["cpu"] = counters(root / "cpu.stat")
    result["memory_events"] = counters(root / "memory.events")
    return result


class Sampler:
    """Count real compiler processes in this batch's process groups, not wrappers."""

    def __init__(self):
        self.groups = set()
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.samples = []
        self.root = cgroup_path()
        self.thread = threading.Thread(target=self.collect, daemon=True)

    def collect(self):
        while not self.stop.is_set():
            counts = Counter()
            with self.lock:
                groups = self.groups.copy()
            for proc in Path("/proc").iterdir():
                if not proc.name.isdigit():
                    continue
                try:
                    fields = (proc / "stat").read_text().rsplit(") ", 1)[1].split()
                    if int(fields[2]) not in groups:
                        continue
                    executable = (proc / "exe").resolve().name
                    if executable in ("rustc", "clippy-driver"):
                        counts["rustc"] += 1
                    elif executable in (
                        "cc1",
                        "cc1plus",
                        "lto1",
                    ) or executable.startswith("clang"):
                        counts["native"] += 1
                    elif executable in (
                        "ld",
                        "ld.bfd",
                        "ld.gold",
                        "ld.lld",
                        "rust-lld",
                    ):
                        counts["linker"] += 1
                except (OSError, ValueError, IndexError):
                    continue
            try:
                memory = int((self.root / "memory.current").read_text())
            except (OSError, ValueError):
                memory = None
            self.samples.append(
                {
                    "time_ns": time.time_ns(),
                    **dict(counts),
                    "memory_current_bytes": memory,
                }
            )
            self.stop.wait(0.1)


def aggregate_events(events):
    builds = [event for event in events if "result" in event and "crate_name" in event]
    if not builds:
        raise ValueError("no Kache build events were recorded")
    results = Counter(event["result"] for event in builds)
    totals = {key: sum(event.get(key, 0) for event in builds) for key in TOTALS}
    compiled_keys = Counter(
        event["cache_key"]
        for event in builds
        if event.get("cache_key") and event.get("compiler_runs", 0)
    )
    by_unit = {}
    for event in builds:
        unit = by_unit.setdefault(event["crate_name"], Counter())
        for key in TOTALS:
            unit[key] += event.get(key, 0)
        unit[event["result"]] += 1
    return {
        "results": dict(results),
        "daemon_stores": sum(bool(e.get("store_handed_off")) for e in builds),
        "cache_errors": sum(
            bool(e.get("store_error") or e.get("lookup_rejection") or e.get("fallback"))
            for e in builds
        ),
        **totals,
        "duplicate_key_compiles": sum(count - 1 for count in compiled_keys.values()),
        "by_unit": by_unit,
    }


def event_window(path, offset, prefix, identity=None):
    """Refuse a rotated/truncated log instead of silently losing measurements."""
    with path.open("rb") as stream:
        stat = os.fstat(stream.fileno())
        if (
            (identity is not None and (stat.st_dev, stat.st_ino) != identity)
            or stat.st_size < offset
            or stream.read(len(prefix)) != prefix
        ):
            raise ValueError("event log rotated during the measurement")
        stream.seek(offset)
        return stream.read().decode("utf-8")


def job_environment(
    args, binary, repo, store, runtime, scheduler, backend="kache", repos=None
):
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("KACHE_", "SCCACHE_", "MBX_", "RUSTC_", "CARGO_"))
        and key
        not in ("RUSTFLAGS", "CFLAGS", "CXXFLAGS", "CC", "CXX", "HOST_CC", "HOST_CXX")
    }
    env.update(
        {
            "CARGO_HOME": str(args.output / "cargo-home"),
            "CARGO_INCREMENTAL": "0",
            "CARGO_BUILD_JOBS": str(args.jobs_per_build),
            "CARGO_TERM_COLOR": "never",
            "RUSTUP_TOOLCHAIN": args.toolchain,
            "RUSTC_WRAPPER": str(binary),
            "KACHE_CACHE_DIR": str(store),
            "KACHE_RUNTIME_DIR": str(runtime),
            "KACHE_CONFIG": str(args.output / "kache.toml"),
            "KACHE_LOCAL_ONLY": "1",
            "KACHE_SCHEDULER": str(scheduler),
            "KACHE_BASE_DIR": str(repo),
            "KACHE_INPUT_PREDICTIONS": "1",
            "KACHE_VERIFY_INPUT_PREDICTIONS": "sampled",
            "KACHE_PROGRESS": "0",
            "HOST_CC": f"{binary} cc",
            "HOST_CXX": f"{binary} c++",
            "CC_KNOWN_WRAPPER_CUSTOM": binary.name,
            "SOURCE_DATE_EPOCH": "1735689600",
            "ZERO_AR_DATE": "1",
            "CI": "1",
        }
    )
    env["CFLAGS"] = env["CXXFLAGS"] = " ".join(
        f"-{flag}-prefix-map={repo}=." for flag in ("ffile", "fdebug", "fmacro")
    )
    env["CARGO_TARGET_DIR"] = str(repo / "target")
    if backend != "kache":
        env = {key: value for key, value in env.items() if not key.startswith("KACHE_")}
        env.pop("CC_KNOWN_WRAPPER_CUSTOM", None)
    if backend == "sccache":
        env.update(
            SCCACHE_DIR=str(store),
            SCCACHE_CACHE_SIZE="10G",
            SCCACHE_IDLE_TIMEOUT="0",
            SCCACHE_SERVER_UDS=str(runtime / "server.sock"),
            SCCACHE_BASEDIRS=os.pathsep.join(str(r) for r in (repos or [repo])),
            SCCACHE_CONF=str(args.output / "sccache.toml"),
        )
    elif backend == "mbx":
        for key in ("RUSTC_WRAPPER", "HOST_CC", "HOST_CXX"):
            env.pop(key, None)
        env.update(MBX_CACHE_DIR=str(store), MBX_SCHEDULER="1", MBX_SUMMARY="full")
    return env


def external_counts(backend, reports):
    if backend == "sccache":
        stats = reports[0]["stats"]
        counts = {
            name: sum(stats[name]["counts"].values())
            for name in ("cache_hits", "cache_misses", "cache_errors")
        }
        return {
            "results": {
                "local_hit": counts["cache_hits"],
                "miss": counts["cache_misses"],
                "error": counts["cache_errors"]
                + stats.get("cache_read_errors", 0)
                + stats.get("cache_write_errors", 0),
            }
        }
    phases = Counter()
    for report in reports:
        phases.update(report.get("wrapper_phases_ns", {}))
    return {
        "wrapper_phases_ns": dict(phases),
        "results": {
            "local_hit": sum(r["hits"] for r in reports),
            "miss": sum(r["misses"] for r in reports),
            "unconsulted": sum(r.get("unconsulted", 0) for r in reports),
            "error": sum(
                r.get("divergences", 0)
                + r.get("remote_failures", 0)
                + r.get("background_upload_failures", 0)
                for r in reports
            ),
        },
    }


def capture_mbx_traces(binary, sessions, previous, dest):
    """Preserve each completed session before its task-owned store is removed."""
    paths = sorted(set(sessions.glob("*.jsonl")) - previous)
    if not paths:
        return []
    target = dest / "mbx-sessions"
    target.mkdir()
    traces = []
    for path in paths:
        saved = target / path.name
        shutil.copy2(path, saved)
        trace = json.loads(
            capture([str(binary), "cache", "trace", str(saved)], timeout=30)
        )
        if "traceEvents" not in trace:
            raise ValueError(f"mbx returned an invalid trace for {saved}")
        output = saved.with_suffix(".trace.json")
        output.write_text(json.dumps(trace) + "\n")
        traces.append(str(output.relative_to(dest)))
    return traces


def capture_kache_traces(dest):
    paths = sorted((dest / "kache-phases").glob("*.trace.json"))
    if not paths:
        raise ValueError("phase tracing requested but Kache produced no traces")
    events = []
    for path in paths:
        data = json.loads(path.read_text())
        spans = data["traceEvents"]
        if not spans or data["kache_trace"]["clock"] != "monotonic":
            raise ValueError(f"invalid Kache phase trace: {path}")
        if spans[0]["args"]["dropped_events"]:
            raise ValueError(f"Kache phase trace exceeded its event limit: {path}")
        events.extend(spans)
    output = dest / "kache.trace.json"
    output.write_text(
        json.dumps({"traceEvents": events, "displayTimeUnit": "ms"}) + "\n"
    )
    return [output.name]


def stop_kache(binary, env, dest):
    """Drain accepted publications before reading events or copying the cache."""
    with (dest / "daemon-stop.log").open("w") as stream:
        subprocess.run(
            [str(binary), "daemon", "stop"],
            env=env,
            stdout=stream,
            stderr=subprocess.STDOUT,
            timeout=30,
            check=False,
        )
    wait_for_daemon_lock(Path(env["KACHE_RUNTIME_DIR"]) / "daemon.run.lock", 40)


def stop_sccache(binary, env, server, stop_timeout=30, exit_timeout=10):
    """Ask the foreground sccache server to stop; kill it if it does not.

    This runs after the batch is timed and its statistics are read, so a slow
    shutdown takes nothing from the measurement. Returns whether the server had
    to be killed.
    """
    try:
        subprocess.run(
            [str(binary), "--stop-server"],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=stop_timeout,
            check=False,
        )
        server.wait(timeout=exit_timeout)
    except subprocess.TimeoutExpired:
        pass
    killed = server.poll() is None
    if killed:
        os.killpg(server.pid, signal.SIGKILL)
        server.wait()
    return killed


def wait_for_daemon_lock(path, timeout):
    import fcntl

    try:
        lock = path.open("r+")
    except FileNotFoundError:
        return
    deadline = time.monotonic() + timeout
    with lock:
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise ValueError(f"daemon did not drain: {path}")
                time.sleep(0.05)


def run_phase(
    args, binary, repos, store, runtime, scheduler, phase, dest, backend="kache"
):
    dest.mkdir()
    workload = getattr(args, "workload", JOBS)
    # Every Cargo command owns its checkout and target, including sequential runs.
    # Sequential/parallel comparisons therefore do not change artifact reuse.
    for repo in repos:
        remove_owned_tree(repo / "target")
    events_path = runtime / "events.jsonl"
    events_path.parent.mkdir(parents=True, exist_ok=True)
    events_path.touch(exist_ok=True)
    stat = events_path.stat()
    identity, offset = (stat.st_dev, stat.st_ino), stat.st_size
    with events_path.open("rb") as stream:
        prefix = stream.read(128)
    sessions = store / "actions" / "sessions" / "v1"
    previous_sessions = set(sessions.glob("*.jsonl")) if backend == "mbx" else set()
    sampler = Sampler()
    server = None
    server_log = None
    kache_stopped = False
    control_env = job_environment(
        args, binary, repos[0], store, runtime, scheduler, backend, repos
    )
    if backend == "kache" and getattr(args, "daemon", False):
        with (dest / "daemon-start.log").open("w") as stream:
            subprocess.run(
                [str(binary), "daemon", "start"],
                env=control_env,
                stdout=stream,
                stderr=subprocess.STDOUT,
                timeout=30,
                check=True,
            )
    if backend == "sccache":
        # Foreground server keeps compiler children in a process group we own.
        server_log = (dest / "server.log").open("w")
        server = subprocess.Popen(
            [str(binary)],
            env=dict(control_env, SCCACHE_START_SERVER="1", SCCACHE_NO_DAEMON="1"),
            stdout=server_log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        sampler.groups.add(server.pid)
        deadline = time.monotonic() + 30
        while not (runtime / "server.sock").exists():
            if server.poll() is not None or time.monotonic() >= deadline:
                if server.poll() is None:
                    os.killpg(server.pid, signal.SIGKILL)
                server.wait()
                server_log.close()
                raise ValueError(f"sccache server did not start; inspect {dest}")
            time.sleep(0.05)
    before = machine()
    started = time.monotonic()
    sampler.thread.start()

    def build(item):
        index, (name, command) = item
        env = job_environment(
            args, binary, repos[index], store, runtime, scheduler, backend, repos
        )
        if backend == "mbx":
            env["MBX_STATS_REPORT"] = str(dest / f"{name}.mbx.json")
        elif backend == "kache" and getattr(args, "trace_phases", False):
            env["KACHE_PHASE_TRACE_DIR"] = str(dest / "kache-phases")
        command = [str(binary), *command] if backend == "mbx" else ["cargo", *command]
        launch = time.monotonic()
        with (
            (dest / f"{name}.log").open("w") as stream,
            subprocess.Popen(
                command,
                cwd=repos[index],
                env=env,
                stdout=stream,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            ) as proc,
        ):
            with sampler.lock:
                sampler.groups.add(proc.pid)
            try:
                status = proc.wait(timeout=args.timeout)
            except BaseException:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                proc.wait()
                raise
            finally:
                with sampler.lock:
                    sampler.groups.discard(proc.pid)
        return {
            "job": name,
            "command": command,
            "status": status,
            "start_ms": (launch - started) * 1000,
            "wall_ms": (time.monotonic() - launch) * 1000,
        }

    try:
        with ThreadPoolExecutor(max_workers=args.parallelism) as pool:
            jobs = list(pool.map(build, enumerate(workload)))
        wall_ms = (time.monotonic() - started) * 1000
        after = machine()
        if backend == "kache":
            stop_kache(binary, control_env, dest)
            kache_stopped = True
            raw = event_window(events_path, offset, prefix, identity)
            (dest / "events.jsonl").write_text(raw)
            aggregate = aggregate_events(
                [json.loads(line) for line in raw.splitlines() if line.strip()]
            )
        elif backend == "sccache":
            raw = capture(
                [str(binary), "--show-stats", "--stats-format", "json"],
                env=control_env,
                timeout=30,
            )
            (dest / "sccache.json").write_text(raw + "\n")
            stats = json.loads(raw)
            if str(store) not in stats.get("cache_location", "") or {
                str(Path(path)) for path in stats.get("basedirs", [])
            } != {str(repo) for repo in repos}:
                raise ValueError(
                    "sccache used unexpected cache location or base directories"
                )
            aggregate = external_counts(backend, [stats])
        else:
            aggregate = external_counts(
                backend,
                [
                    json.loads((dest / f"{name}.mbx.json").read_text())
                    for name, _ in workload
                ],
            )
    finally:
        sampler.stop.set()
        sampler.thread.join()
        # Daemon shutdown and statistics collection are outside the timed batch.
        if backend == "kache" and not kache_stopped:
            stop_kache(binary, control_env, dest)
        if server is not None:
            try:
                if stop_sccache(binary, control_env, server):
                    print(
                        f"{phase}: sccache server did not stop in time and was killed",
                        flush=True,
                    )
            finally:
                server_log.close()
    traces = []
    if backend == "mbx":
        traces = capture_mbx_traces(binary, sessions, previous_sessions, dest)
    elif backend == "kache" and getattr(args, "trace_phases", False):
        traces = capture_kache_traces(dest)
    result = {
        "traces": traces,
        "phase": phase,
        "backend": backend,
        "time_ns": time.time_ns(),
        "wall_ms": wall_ms,
        "jobs": jobs,
        "events": aggregate,
        "machine_before": before,
        "machine_after": after,
        "peak_compilers": max(
            (s.get("rustc", 0) + s.get("native", 0) for s in sampler.samples), default=0
        ),
        "peak_rustc": max((s.get("rustc", 0) for s in sampler.samples), default=0),
        "peak_native": max((s.get("native", 0) for s in sampler.samples), default=0),
        "peak_cgroup_memory_bytes": max(
            (
                s["memory_current_bytes"]
                for s in sampler.samples
                if s["memory_current_bytes"] is not None
            ),
            default=None,
        ),
        "cpu_delta": {
            key: value - before["cpu"][key]
            for key, value in after["cpu"].items()
            if key in before["cpu"]
        },
        "sampler": sampler.samples,
    }
    (dest / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    if any(job["status"] for job in jobs):
        raise ValueError(f"{phase}: Cargo job failed; inspect {dest}")
    if (
        aggregate["results"].get("error", 0)
        or aggregate.get("prediction_mismatches", 0)
        or aggregate.get("cache_errors", 0)
    ):
        raise ValueError(f"{phase}: cache errors or prediction mismatches")
    if phase == "cold" and not (
        aggregate.get("compiler_runs", 0)
        if backend == "kache"
        else aggregate["results"].get("miss", 0)
        + aggregate["results"].get("unconsulted", 0)
    ):
        raise ValueError("cold batch compiled nothing")
    if phase == "warm" and not aggregate["results"].get("local_hit", 0):
        raise ValueError("warm batch restored nothing")
    if after["memory_events"].get("oom_kill", 0) > before["memory_events"].get(
        "oom_kill", 0
    ):
        raise ValueError("measurement encountered an OOM kill")
    return result


def summarize(records):
    groups = {}
    for record in records:
        groups.setdefault((record["arm"], record["phase"]), []).append(record)
    result = []
    for (arm, phase), group in groups.items():
        times = [r["wall_ms"] for r in group]
        row = {
            "arm": arm,
            "phase": phase,
            "n": len(group),
            "mean_ms": statistics.mean(times),
            "median_ms": statistics.median(times),
            "min_ms": min(times),
            "max_ms": max(times),
        }
        for key in (
            "compiler_runs",
            "duplicate_key_compiles",
            "flight_wait_ms",
            "permit_wait_ms",
            "key_ms",
            "dep_info_ms",
        ):
            row[key] = (
                statistics.median(r["events"][key] for r in group)
                if all(key in r["events"] for r in group)
                else None
            )
        result.append(row)
    return result


def otlp(data):
    """Use the existing benchmark metric schema with a distinct project name."""

    def attr(key, value):
        return {"key": key, "value": {"stringValue": value}}

    identities = {arm["name"]: arm for arm in data["arms"]}
    resources = []
    for record in data["records"]:
        identity = identities[record["arm"]]
        common = [
            attr(
                "kache.bench.project",
                f"bench-{data['project']}-contention"
                + ("-full-features" if data.get("variant") == "full-features" else ""),
            ),
            attr("kache.bench.cache_tool", identity["backend"]),
        ]
        attrs = [*common, attr("kache.bench.phase", record["phase"])]
        timestamp = str(record["time_ns"])

        def point(value, attributes=attrs, timestamp=timestamp):
            return {
                "asDouble": value,
                "timeUnixNano": timestamp,
                "attributes": attributes,
            }

        def gauge(name, unit, points):
            return {
                "name": "kache.bench." + name,
                "unit": unit,
                "gauge": {"dataPoints": points},
            }

        counts = record["events"]["results"]
        hits = sum(
            counts.get(key, 0) for key in ("local_hit", "prefetch_hit", "remote_hit")
        )
        misses = counts.get("miss", 0) + counts.get("dup", 0)
        units = [
            point(counts.get(key, 0), [*attrs, attr("kache.bench.result", label)])
            for key, label in (
                ("local_hit", "hit"),
                ("miss", "miss"),
                ("dup", "dup"),
                ("error", "error"),
            )
            if key in counts or identity["backend"] == "kache"
        ]
        resources.append(
            {
                "resource": {
                    "attributes": [
                        attr("kache.telemetry.schema_version", "1"),
                        attr("kache.bench.git_ref", data["revision"]),
                        attr("kache.bench.cache_tool_version", identity["version"]),
                    ]
                },
                "scopeMetrics": [
                    {
                        "scope": {"name": "kache.bench", "version": "1"},
                        "metrics": [
                            gauge(
                                "build.duration", "s", [point(record["wall_ms"] / 1000)]
                            ),
                            gauge("compile.units", "{unit}", units),
                            *(
                                [
                                    gauge(
                                        "cache.unconsulted",
                                        "{unit}",
                                        [point(counts["unconsulted"])],
                                    )
                                ]
                                if "unconsulted" in counts
                                else []
                            ),
                            gauge(
                                "cache.hit_rate",
                                "%",
                                [
                                    point(
                                        hits / (hits + misses) * 100
                                        if hits + misses
                                        else 0
                                    )
                                ],
                            ),
                            gauge(
                                "verdict.ok",
                                "1",
                                [point(0 if data.get("error") else 1, common)],
                            ),
                            *[
                                gauge(
                                    "storage." + metric,
                                    "By",
                                    [
                                        point(
                                            group[metric],
                                            [
                                                *attrs,
                                                attr("kache.bench.storage_scope", scope),
                                            ],
                                        )
                                        for scope, group in record.get("storage", {})
                                        .get("groups", {})
                                        .items()
                                    ],
                                )
                                for metric in (
                                    "logical_bytes",
                                    "unique_logical_bytes",
                                    "allocated_bytes",
                                )
                                if "storage" in record
                            ],
                        ],
                    }
                ],
            }
        )
    return {"resourceMetrics": resources}


def optional_seconds(value):
    return "—" if value is None else f"{value / 1000:.2f}s"


def write_report(args, data):
    summary = summarize(data["records"])
    (args.output / "metrics.otlp.json").write_text(json.dumps(otlp(data)) + "\n")
    (args.output / "schema_version").write_text("1\n")
    (args.output / "samples.json").write_text(json.dumps(data, indent=2) + "\n")
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    lines = [
        f"## Contention: {args.project}",
        "",
        f"Six Cargo jobs, {args.parallelism} at once, {args.jobs_per_build} Cargo jobs per build. Independent empty targets; shared store within each arm. Each warm batch starts from its cold seed; a new seed is measured every {args.cold_every} samples.",
        "",
        "| Arm | Phase | n | Mean | Median | Range | Compiler runs | Duplicate keys | Flight wait (sum) | Permit wait (sum) |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for r in summary:
        lines.append(
            f"| {r['arm']} | {r['phase']} | {r['n']} | {r['mean_ms'] / 1000:.2f}s | {r['median_ms'] / 1000:.2f}s | {r['min_ms'] / 1000:.2f}–{r['max_ms'] / 1000:.2f}s | {r['compiler_runs'] if r['compiler_runs'] is not None else '—'} | {r['duplicate_key_compiles'] if r['duplicate_key_compiles'] is not None else '—'} | {optional_seconds(r['flight_wait_ms'])} | {optional_seconds(r['permit_wait_ms'])} |"
        )
    lines += [
        "",
        "Waits sum overlapping wrapper events and are not wall-clock savings. Compiler peaks are sampled; cgroup counters include all work in the dedicated runner. Raw samples, identities, process samples, events, and job logs are retained.",
    ]
    if any("storage" in r for r in data["records"]):
        lines += [
            "",
            "| Arm | Phase | n | Cache GiB | Targets GiB | Cache + targets GiB |",
            "| --- | --- | ---: | ---: | ---: | ---: |",
        ]
        for row in summary:
            records = [
                r for r in data["records"]
                if r["arm"] == row["arm"]
                and r["phase"] == row["phase"]
                and "storage" in r
            ]
            if not records:
                continue
            sizes = [
                statistics.mean(
                    r["storage"]["groups"][scope]["allocated_bytes"] for r in records
                ) / 2**30
                for scope in ("cache", "targets", "cache_and_targets")
            ]
            lines.append(
                f"| {row['arm']} | {row['phase']} | {len(records)} | "
                + " | ".join(f"{size:.3f}" for size in sizes) + " |"
            )
        lines += [
            "",
            "Mean allocated blocks after each batch, outside the timer. Hardlinks count once within each scope; the combined column deduplicates across cache and targets. On Linux a shared extent counts once when every regular file in the scope has a usable map; otherwise the figure stays on st_blocks. Sources, Cargo home, runtime logs and cold snapshots are excluded. Cache-resident session logs are included and broken out in samples.json.",
        ]
    if data.get("error"):
        lines += ["", f"INVALID MEASUREMENT: {data['error']}"]
    (args.output / "report.md").write_text("\n".join(lines) + "\n")


# FIEMAP flags from Linux uapi. Lengths are usable only for this subset;
# anything else (encoded, delayed, unaligned) discards the map.
FIEMAP_EXTENT_LAST = 0x1
FIEMAP_EXTENT_SHARED = 0x2000
FIEMAP_ACCOUNTABLE_FLAGS = 0x3801
_FIEMAP_EXTENTS_PER_PAGE = 128
_FIEMAP_MAX_PAGES = 64
_FS_IOC_FIEMAP = 0xC020660B


def resolve_allocated(items):
    """Bytes to charge for one scope.

    Each item is `(device, blocks, extents, regular)`. `extents` is a list of
    `(physical, length, flags)` or `None` when a regular file could not be
    mapped. One unmapped regular file keeps the `st_blocks` sum. A shared
    extent is charged on the first file that has it and subtracted from later
    files, so clones count once while hardlinks stay on their single inode.
    """
    blocks_sum = sum(item[1] for item in items)
    if any(regular and extents is None for _, _, extents, regular in items):
        return blocks_sum, False
    seen = set()
    total = 0
    for device, blocks, extents, regular in items:
        if not regular or not any(
            flags & FIEMAP_EXTENT_SHARED for _, _, flags in extents
        ):
            total += blocks
            continue
        charge = blocks
        for physical, length, flags in extents:
            if flags & FIEMAP_EXTENT_SHARED == 0:
                continue
            key = (device, physical, length)
            if key in seen:
                charge -= length
            else:
                seen.add(key)
        if charge < 0:
            charge = 0
        total += charge
    return total, True


def linux_extent_map(path):
    """Shared-extent map for a regular file, or `None` when it cannot be used."""
    if sys.platform != "linux":
        return None
    import ctypes
    import fcntl

    class FiemapExtent(ctypes.Structure):
        _fields_ = [
            ("logical", ctypes.c_uint64),
            ("physical", ctypes.c_uint64),
            ("length", ctypes.c_uint64),
            ("reserved64", ctypes.c_uint64 * 2),
            ("flags", ctypes.c_uint32),
            ("reserved", ctypes.c_uint32 * 3),
        ]

    class Fiemap(ctypes.Structure):
        _fields_ = [
            ("start", ctypes.c_uint64),
            ("length", ctypes.c_uint64),
            ("flags", ctypes.c_uint32),
            ("mapped_extents", ctypes.c_uint32),
            ("extent_count", ctypes.c_uint32),
            ("reserved", ctypes.c_uint32),
            ("extents", FiemapExtent * _FIEMAP_EXTENTS_PER_PAGE),
        ]

    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError:
        return None
    try:
        offset = 0
        found = []
        for _ in range(_FIEMAP_MAX_PAGES):
            request = Fiemap()
            request.start = offset
            request.length = (1 << 64) - 1 - offset
            request.flags = 1
            request.extent_count = _FIEMAP_EXTENTS_PER_PAGE
            try:
                fcntl.ioctl(fd, _FS_IOC_FIEMAP, request, True)
            except OSError:
                return None
            mapped = request.mapped_extents
            if mapped > _FIEMAP_EXTENTS_PER_PAGE:
                return None
            if mapped == 0:
                return found
            for index in range(mapped):
                extent = request.extents[index]
                if extent.flags & ~FIEMAP_ACCOUNTABLE_FLAGS:
                    return None
                if extent.length == 0 or extent.logical < offset:
                    return None
                offset = extent.logical + extent.length
                found.append((extent.physical, extent.length, extent.flags))
                if extent.flags & FIEMAP_EXTENT_LAST:
                    if index + 1 != mapped:
                        return None
                    return found
            if offset == (1 << 64) - 1:
                return None
        return None
    finally:
        os.close(fd)


def measure_storage(store, repos, backend):
    """Scan completed outputs without following symlinks or counting hardlinks twice.

    Allocated bytes start as st_blocks. On Linux, a shared FIEMAP extent is
    counted once across the inodes in the scope. An unusable map keeps the
    st_blocks sum for the whole scope. Source, Cargo home, runtime diagnostics
    and cold snapshots are excluded.
    """
    groups = {}

    def add(group, identity, size, blocks, kind, path, regular):
        bucket = groups.setdefault(group, {"logical_bytes": 0, "paths": 0, "inodes": {}})
        bucket["logical_bytes"] += size
        bucket["paths"] += 1
        bucket["inodes"][identity] = (size, blocks, kind, path, regular)

    def visit(path, scope, relative=()):
        info = path.lstat()
        kind = "directory" if stat.S_ISDIR(info.st_mode) else "file"
        size = 0 if kind == "directory" else info.st_size
        identity = (info.st_dev, info.st_ino)
        blocks = info.st_blocks * 512
        regular = stat.S_ISREG(info.st_mode)
        categories = [scope, "cache_and_targets"]
        if scope == "cache":
            if backend == "kache" and relative[:2] == ("store", "blobs"):
                component = "blobs"
            elif backend == "mbx" and relative[:2] == ("actions", "cas"):
                component = "blobs"
            elif relative and relative[0] in ("index.db", "index.db-wal", "index.db-shm"):
                component = "index"
            elif backend == "mbx" and relative[:2] == ("actions", "sessions"):
                component = "sessions"
            else:
                component = "other"
            categories.append("cache_" + component)
        for group in categories:
            add(group, identity, size, blocks, kind, path, regular)
        if kind == "directory":
            for child in path.iterdir():
                visit(child, scope, (*relative, child.name))

    visit(store, "cache")
    for repo in repos:
        visit(repo / "target", "targets")
    sharing_resolved = True
    allocated_by_group = {}
    for name, bucket in groups.items():
        items = []
        for (device, _ino), record in bucket["inodes"].items():
            _size, blocks, _kind, file_path, regular = record
            extents = []
            if regular and sys.platform == "linux":
                extents = linux_extent_map(file_path)
            elif regular:
                extents = None
            items.append((device, blocks, extents, regular))
        allocated, resolved = resolve_allocated(items)
        allocated_by_group[name] = allocated
        sharing_resolved = sharing_resolved and resolved
    if not sharing_resolved:
        for name, bucket in groups.items():
            allocated_by_group[name] = sum(
                record[1] for record in bucket["inodes"].values()
            )
    result = {
        "schema_version": 1,
        "point": "after_batch",
        "allocation_method": (
            "st_blocks_minus_repeated_shared_extents"
            if sharing_resolved and sys.platform == "linux"
            else "st_blocks_times_512_unique_device_inode"
        ),
        "reflink_sharing_resolved": sharing_resolved and sys.platform == "linux",
        "symlinks_followed": False,
        "excluded": ["source", "cargo_home", "runtime", "cold_snapshot", "reports"],
        "groups": {},
    }
    for name, bucket in groups.items():
        entries = bucket["inodes"].values()
        result["groups"][name] = {
            "logical_bytes": bucket["logical_bytes"],
            "unique_logical_bytes": sum(entry[0] for entry in entries),
            "allocated_bytes": allocated_by_group[name],
            "paths": bucket["paths"],
            "unique_inodes": len(bucket["inodes"]),
        }
    shared = groups["cache"]["inodes"].keys() & groups["targets"]["inodes"].keys()
    result["cache_target_shared_inodes"] = len(shared)
    result["cache_target_shared_logical_bytes"] = sum(
        groups["cache"]["inodes"][identity][0] for identity in shared
    )
    return result


def remove_owned_tree(path):
    """Delete a cache or work tree.

    mbx 1.15 writes nested output directories without owner write. shutil.rmtree
    then fails with EACCES; ignore_errors=True hides that and the next copytree
    raises FileExistsError. Restore owner write/traverse on real directories we
    own and retry. Never chmod files: they may be hard-linked into other trees.
    Never follow symlinks.
    """
    path = Path(path)
    if path.is_symlink():
        path.unlink()
        return
    if not path.exists():
        return

    def restore_dir_and_retry(func, victim, exc):
        error = exc if isinstance(exc, BaseException) else exc[1]
        victim = Path(victim)
        if victim.is_symlink():
            raise error
        try:
            info = victim.lstat()
        except FileNotFoundError:
            return
        directories = [victim.parent]
        if stat.S_ISDIR(info.st_mode):
            directories.insert(0, victim)
        restored = False
        for directory in directories:
            if directory.is_symlink():
                continue
            try:
                info = directory.lstat()
            except FileNotFoundError:
                continue
            if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
                continue
            os.chmod(
                directory,
                stat.S_IMODE(info.st_mode) | stat.S_IWUSR | stat.S_IXUSR,
            )
            restored = True
        if not restored:
            raise error
        func(victim)

    kwargs = (
        {"onexc": restore_dir_and_retry}
        if sys.version_info >= (3, 12)
        else {"onerror": restore_dir_and_retry}
    )
    shutil.rmtree(path, **kwargs)
    if path.is_symlink() or path.exists():
        raise FileExistsError(path)


def write_scenario_files(repo, files):
    """The scenario's `[[file]]` entries, as the benchmark engine writes them
    into its own checkouts (aube's is an emptied mise.toml)."""
    for entry in files:
        path = repo / entry["path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(entry["content"])


def run_batches(args, arms, mirror, work, data):
    workload = getattr(args, "workload", JOBS)
    for sample in range(args.samples):
        for arm, binary, scheduler, backend in (
            arms if (sample + args.order_seed) % 2 == 0 else list(reversed(arms))
        ):
            cell = work / f"{sample // args.cold_every:02d}-{arm}"
            cold = sample % args.cold_every == 0
            if cold:
                remove_owned_tree(cell)
                cell.mkdir()
                for name, _ in workload:
                    subprocess.run(
                        [
                            "git",
                            "clone",
                            "-q",
                            "--shared",
                            str(mirror),
                            str(cell / name),
                        ],
                        check=True,
                    )
                    write_scenario_files(cell / name, getattr(args, "scenario_files", ()))
            repos = [cell / name for name, _ in workload]
            store, runtime, snapshot = (
                cell / "cache",
                cell / "runtime",
                cell / "cold-snapshot",
            )
            for phase in ("cold", "warm") if cold else ("warm",):
                if phase == "warm" and args.cold_every > 1:
                    remove_owned_tree(store)
                    shutil.copytree(snapshot, store, symlinks=True)
                print(
                    f"{args.project} sample {sample + 1}/{args.samples}: {arm} {phase}",
                    flush=True,
                )
                result = run_phase(
                    args,
                    binary,
                    repos,
                    store,
                    runtime,
                    scheduler,
                    phase,
                    args.output / f"{sample:02d}-{arm}-{phase}",
                    backend,
                )
                # Outside run_phase's wall-clock timer and before cleanup/snapshot.
                result["storage"] = measure_storage(store, repos, backend)
                data["records"].append(
                    {
                        "sample": sample,
                        "cold_seed": sample // args.cold_every,
                        "arm": arm,
                        **result,
                    }
                )
                write_report(args, data)
                if phase == "cold" and args.cold_every > 1:
                    shutil.copytree(store, snapshot, symlinks=True)
            if not args.keep_work:
                # Keep only one cold snapshot per arm between samples. Empty
                # targets and active stores do not accumulate across arms.
                for repo in repos:
                    remove_owned_tree(repo / "target")
                remove_owned_tree(store)
                if (sample + 1) % args.cold_every == 0 or sample + 1 == args.samples:
                    remove_owned_tree(cell)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", choices=("hk", "eza", "aube"), default="hk")
    parser.add_argument(
        "--arm", action="append", default=[], metavar="NAME=BINARY,SCHEDULER"
    )
    parser.add_argument("--samples", type=int, choices=range(1, 21), default=3)
    parser.add_argument(
        "--cold-every",
        type=int,
        choices=range(1, 21),
        default=1,
        help="measure a fresh cold seed every N warm samples",
    )
    parser.add_argument("--parallelism", type=int, choices=range(1, 7), default=6)
    parser.add_argument("--jobs-per-build", type=int, default=4)
    parser.add_argument("--toolchain")
    parser.add_argument(
        "--full-features",
        action="store_true",
        help="include eza's vendored OpenSSL stress graph",
    )
    parser.add_argument(
        "--scenarios",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "scenarios",
    )
    parser.add_argument("--sccache")
    parser.add_argument("--mbx")
    parser.add_argument("--order-seed", type=int, default=0)
    parser.add_argument("--timeout", type=int, default=1200)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--keep-work", action="store_true")
    parser.add_argument(
        "--daemon", action="store_true",
        help="start Kache daemons before each timed batch and drain them afterward",
    )
    parser.add_argument(
        "--trace-phases",
        action="store_true",
        help="capture real Kache wrapper intervals; diagnostic runs include tracing overhead",
    )
    args = parser.parse_args()
    if platform.system() != "Linux" or args.jobs_per_build < 1:
        parser.error("requires Linux and a positive jobs-per-build")
    args.workload = workload_for(args.project, args.full_features)
    args.output = args.output.resolve()
    arms = []
    for value in args.arm:
        name, command = value.split("=", 1)
        binary, scheduler = command.rsplit(",", 1)
        if (
            not name.isascii()
            or not name.replace("-", "").isalnum()
            or scheduler not in ("0", "1")
        ):
            parser.error("arms must be NAME=BINARY,0 or NAME=BINARY,1")
        binary = Path(binary).resolve()
        arms.append((name, binary, int(scheduler), "kache"))
    for backend in ("sccache", "mbx"):
        if command := getattr(args, backend):
            arms.append(
                (backend, Path(shutil.which(command) or command).resolve(), 1, backend)
            )
    if not arms:
        parser.error("provide at least one --arm, --sccache, or --mbx")
    if len({arm[0] for arm in arms}) != len(arms):
        parser.error("arm names must be unique")
    profile = tomllib.loads(
        (args.scenarios / f"bench-{args.project}" / "scenario.toml").read_text()
    )
    source = profile["source"]
    args.scenario_files = profile.get("file", [])
    args.toolchain = args.toolchain or {"hk": "1.97.1", "eza": "1.90.0", "aube": "1.97.1"}[args.project]
    data = {
        "schema_version": 1,
        "project": args.project,
        "workload": "contention",
        "diagnostic_phase_tracing": args.trace_phases,
        "daemon": args.daemon,
        "order_seed": args.order_seed,
        "cargo_jobs": args.workload,
        "variant": "full-features"
        if args.full_features and args.project == "eza"
        else "short",
        "revision": source["ref"],
        "machine": machine(),
        "samples": args.samples,
        "cold_every": args.cold_every,
        "parallelism": args.parallelism,
        "jobs_per_build": args.jobs_per_build,
        "toolchain": args.toolchain,
        "instrument_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "arms": [
            {
                "name": name,
                "binary": str(binary),
                "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                "scheduler": scheduler if backend != "sccache" else None,
                "backend": backend,
                "version": capture([str(binary), "--version"]),
            }
            for name, binary, scheduler, backend in arms
        ],
        "records": [],
    }
    args.output.mkdir(parents=True, exist_ok=False)
    work = args.output / "work"
    work.mkdir()
    # A six-job cold+warm batch can exceed the normal 10 MiB event log cap.
    # This task-owned log is archived per phase and removed after each trial.
    (args.output / "kache.toml").write_text(
        '[cache]\nevent_log_max_size = "256MiB"\nevent_log_keep_lines = 1000000\n'
        # Every cell's target directories are single-use, so a blob may be
        # linked into all of them (the CI-job posture this benchmark models).
        'shared_hardlink_restores = true\n'
    )
    (args.output / "sccache.toml").write_text("")
    try:
        mirror = work / "source"
        subprocess.run(["git", "clone", "-q", source["repo"], str(mirror)], check=True)
        subprocess.run(
            ["git", "checkout", "-q", "--detach", source["ref"]], cwd=mirror, check=True
        )
        if capture(["git", "rev-parse", "HEAD"], cwd=mirror) != source["ref"]:
            raise ValueError("source revision did not match the pinned scenario")
        env = dict(
            os.environ,
            CARGO_HOME=str(args.output / "cargo-home"),
            RUSTUP_TOOLCHAIN=args.toolchain,
            RUSTC_WRAPPER="",
        )
        subprocess.run(
            [
                "rustup",
                "toolchain",
                "install",
                args.toolchain,
                "--profile",
                "minimal",
                "--component",
                "clippy",
            ],
            check=True,
        )
        # Keep fetch and all benchmark arms on the selected Rust toolchain,
        # even when PATH contains mise shims and the source has its own config.
        toolchain_bin = Path(
            capture(["rustup", "which", "--toolchain", args.toolchain, "cargo"])
        ).parent
        os.environ["PATH"] = f"{toolchain_bin}{os.pathsep}{os.environ['PATH']}"
        env["PATH"] = os.environ["PATH"]
        subprocess.run(["cargo", "fetch", "--locked"], cwd=mirror, env=env, check=True)
        run_batches(args, arms, mirror, work, data)
    except Exception as error:
        data["error"] = str(error)
        write_report(args, data)
        raise
    finally:
        if not args.keep_work:
            shutil.rmtree(work, ignore_errors=True)
            shutil.rmtree(args.output / "cargo-home", ignore_errors=True)
    write_report(args, data)


if __name__ == "__main__":
    main()
