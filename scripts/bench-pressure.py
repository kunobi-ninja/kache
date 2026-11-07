#!/usr/bin/env python3
"""Compare real offline Rust builds in a bounded, disposable Linux cgroup.

Run only on a dedicated machine. This script never changes an existing cgroup's
limits, downloads dependencies, or allocates a memory ballast. memory.high
applies reclaim pressure to real compiler processes inside a hard memory.max.
"""

import argparse
import hashlib
import json
import math
import os
import platform
import random
import signal
import subprocess
import sys
import time
from pathlib import Path

MIB = 1 << 20
SCRIPT = Path(__file__).resolve()


def read(path):
    return Path(path).read_text().strip()


def counters(path):
    return {key: int(value) for key, value in (line.split() for line in read(path).splitlines())}


def memory_info():
    return {line.split(":")[0]: int(line.split()[1]) * 1024
            for line in read("/proc/meminfo").splitlines() if len(line.split()) == 3}


def psi(text):
    return {line.split()[0]: {key: float(value) for key, value in
            (field.split("=") for field in line.split()[1:])}
            for line in text.splitlines()}


def identity(path):
    path = Path(path).resolve(strict=True)
    return {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def snapshot(root):
    return {
        "memory_current_bytes": int(read(root / "memory.current")),
        "memory_peak_bytes": int(read(root / "memory.peak")),
        "swap_current_bytes": int(read(root / "memory.swap.current")),
        "memory_events": counters(root / "memory.events"),
        "pressure": psi(read(root / "memory.pressure")),
        "cpu": counters(root / "cpu.stat"),
        "memory_max": read(root / "memory.max"),
        "memory_high": read(root / "memory.high"),
        "cpu_max": read(root / "cpu.max"),
    }


def compiler_rss(groups):
    """Sample aggregate compiler RSS; shared pages can be counted more than once."""
    rss, count = 0, 0
    for group in groups:
        for pid in read(group / "cgroup.procs").splitlines():
            proc = Path("/proc") / pid
            try:
                if (proc / "exe").resolve(strict=True).name not in ("rustc", "clippy-driver"):
                    continue
                rss += int(read(proc / "statm").split()[1]) * os.sysconf("SC_PAGE_SIZE")
                count += 1
            except (OSError, ValueError, IndexError):
                continue  # A process can exit between reading cgroup.procs and statm.
    return {"compiler_rss_bytes": rss, "compiler_processes": count}


def safety_reason(sample, memory_max, host_floor):
    if sample["host_available_bytes"] < host_floor:
        return "host available memory crossed the stop floor"
    if sample["parent"]["memory_current_bytes"] >= memory_max * 9 // 10:
        return "workload reached 90 percent of its hard memory cap"
    if any(group["memory_current_bytes"] >= int(group["memory_max"]) * 9 // 10
           for group in sample["groups"]):
        return "a build cgroup reached 90 percent of its hard memory cap"
    events = sample["parent"]["memory_events"]
    if any(events.get(key, 0) for key in ("oom", "oom_kill", "oom_group_kill")):
        return "workload recorded an OOM event"
    return None


def pressure_high(scenario, elapsed, high, maximum):
    if scenario == "calm":
        return maximum
    if scenario == "sustained":
        return high
    # Keep changes slower than the controller's ten-second settling period.
    return high if int(elapsed // 15) % 2 else maximum


class HighLimits:
    """Keep synchronous reclaim on older kernels out of the supervisor."""

    def __init__(self):
        self.pending = {}

    def update(self, leaf, desired, timeout=5):
        path = leaf / "memory.high"
        if path in self.pending:
            child, started = self.pending[path]
            if child.poll() is None:
                if time.monotonic() - started >= timeout:
                    raise RuntimeError(f"memory.high write exceeded {timeout}s: {leaf}")
                return
            _, error = child.communicate(timeout=1)
            del self.pending[path]
            if child.returncode:
                raise RuntimeError(f"memory.high write failed: {error.decode(errors='replace')}")
        if read(path) != desired:
            child = subprocess.Popen([sys.executable, str(SCRIPT), "_write_high", str(path), desired],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                                     start_new_session=True)
            self.pending[path] = (child, time.monotonic())

    def stop(self):
        for child, _ in self.pending.values():
            if child.poll() is None:
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

    def wait(self):
        for child, _ in self.pending.values():
            child.communicate(timeout=5)
        self.pending.clear()


class Cgroup:
    def __init__(self, parent, name, maximum, cpus, second_maximum=None):
        self.root = parent / name
        self.leaves = []
        self.root.mkdir()  # Refuse to reuse a group containing another workload.
        try:
            for key, value in (("memory.max", maximum), ("memory.swap.max", 0),
                               ("memory.oom.group", 1), ("pids.max", 512),
                               ("cpu.max", f"{cpus * 100000} 100000")):
                (self.root / key).write_text(str(value))
            if not (self.root / "cgroup.kill").exists():
                raise ValueError("cgroup.kill is required for bounded cleanup")
            (self.root / "cgroup.subtree_control").write_text("+memory +cpu +pids")
            for index, limit in enumerate((maximum,) if second_maximum is None
                                          else (maximum, second_maximum)):
                leaf = self.root / f"builds-{index}"
                leaf.mkdir()
                self.leaves.append(leaf)
                (leaf / "memory.max").write_text(str(limit))
                (leaf / "memory.swap.max").write_text("0")
        except BaseException:
            self.close()
            raise

    def close(self):
        if not self.root.exists():
            return
        kill = self.root / "cgroup.kill"
        if kill.exists():
            kill.write_text("1")
        deadline = time.monotonic() + 10
        while counters(self.root / "cgroup.events").get("populated", 0):
            if time.monotonic() >= deadline:
                raise RuntimeError(f"task cgroup did not drain; retained {self.root}")
            time.sleep(0.05)
        for leaf in reversed(self.leaves):
            leaf.rmdir()
        self.root.rmdir()


def workspace(root, name, crates, functions):
    root.mkdir(parents=True)
    (root / "Cargo.toml").write_text(
        '[workspace]\nresolver = "2"\nmembers = ['
        + ",".join(json.dumps(f"unit-{index}") for index in range(crates))
        + ']\n[profile.release]\nopt-level = 1\ndebug = 0\ncodegen-units = 1\n'
    )
    for index in range(crates):
        unit = root / f"unit-{index}"
        (unit / "src").mkdir(parents=True)
        (unit / "Cargo.toml").write_text(
            f'[package]\nname = "{name}_{index}"\nversion = "0.0.0"\nedition = "2021"\n'
        )
        # Each job has distinct source and keys. Equal inputs across the two
        # arms are essential; equal inputs across jobs would exercise dedup.
        salt = int.from_bytes(hashlib.sha256(name.encode()).digest()[:4], "big")
        (unit / "src/lib.rs").write_text("\n".join(
            f"#[inline(never)] pub fn f_{number}(mut x: u64) -> u64 {{ "
            f"for i in 0..16 {{ x = x.wrapping_mul({salt + number + 1})"
            f".rotate_left(i).wrapping_add({index + number}); }} x }}"
            for number in range(functions)
        ))


def environment(args, binary, batch, project):
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("KACHE_", "CARGO_", "RUST", "SCCACHE_", "MBX_"))
           and key not in ("CC", "CXX", "CFLAGS", "CXXFLAGS")}
    env.update({
        "RUSTC": str(args.rustc), "RUSTC_WRAPPER": str(binary),
        "CARGO_HOME": str(batch / "cargo-home"), "CARGO_INCREMENTAL": "0",
        "CARGO_TARGET_DIR": str(project / "target"), "CARGO_TERM_COLOR": "never",
        "KACHE_CACHE_DIR": str(batch / "cache"),
        "KACHE_RUNTIME_DIR": str(batch / "runtime"),
        "KACHE_CONFIG": str(batch / "kache.toml"),
        "KACHE_HOST_CONFIG": str(batch / "kache.toml"),
        "KACHE_LOCAL_ONLY": "1", "KACHE_SCHEDULER": "1",
        "KACHE_SCHEDULER_MEMORY_PRESSURE": "1", "KACHE_PROGRESS": "0",
        "KACHE_DAEMON_PUBLISH": "0", "KACHE_TRUST_DAEMON_EXECUTION": "0",
        "KACHE_BLAZE": "0", "KACHE_EVENT_ROOT": str(project), "CI": "1",
    })
    return env


def summarize(samples, jobs, elapsed):
    foreground = [job["duration_s"] for job in jobs if job["kind"] == "foreground"
                  and job.get("returncode") == 0]
    ordered = sorted(foreground)
    return {
        "elapsed_s": elapsed,
        "completed_builds": sum(job.get("returncode") == 0 for job in jobs),
        "builds_per_minute": sum(job.get("returncode") == 0 for job in jobs) * 60 / elapsed,
        "foreground_count": len(foreground),
        "foreground_during_background_count": sum(job.get("overlapped_background", False)
                                                   for job in jobs if job["kind"] == "foreground"),
        "foreground_mean_s": sum(foreground) / len(foreground) if foreground else None,
        "foreground_p95_s": ordered[math.ceil(len(ordered) * .95) - 1] if ordered else None,
        "sampled_peak_compiler_rss_bytes": max((s["compiler_rss_bytes"] for s in samples), default=0),
        "peak_cgroup_memory_bytes": max((s["parent"]["memory_peak_bytes"] for s in samples), default=0),
        "sampled_peak_swap_bytes": max((s["parent"]["swap_current_bytes"] for s in samples), default=0),
        "warning_samples": sum(any(g["pressure"]["some"]["avg10"] >= 10 for g in s["groups"])
                               for s in samples),
        "critical_samples": sum(any(g["pressure"]["full"]["avg10"] >= 10 for g in s["groups"])
                                for s in samples),
        "warning_without_critical_samples": sum(
            any(g["pressure"]["some"]["avg10"] >= 10 and g["pressure"]["full"]["avg10"] < 10
                for g in s["groups"]) for s in samples),
        "memory_events": samples[-1]["parent"]["memory_events"] if samples else None,
    }


def run_batch(args, label, binary, sample_index, scenario):
    batch = args.output / f"{scenario}-{sample_index:02}-{label}"
    batch.mkdir()
    (batch / "kache.toml").write_text(
        '[cache]\nlocal_only = true\nscheduler = true\nscheduler_memory_pressure = true\n'
        'daemon_publish = false\ntrust_daemon_execution = false\n'
        'auto_gc = false\nauto_recover_targets = false\n'
    )
    (batch / "runtime").mkdir()
    specifications = []
    for index in range(args.background_builds):
        project = batch / f"background-{index}"
        workspace(project, f"background_{index}", args.crates, args.functions)
        specifications.append((0.0, "background", project))
    for index, at in enumerate(args.foreground_at):
        project = batch / f"foreground-{index}"
        workspace(project, f"foreground_{index}", 1, args.foreground_functions)
        specifications.append((at, "foreground", project))
    specifications.sort(key=lambda item: item[0])
    group = Cgroup(args.cgroup_parent, f"kache-pressure-{os.getpid()}-{scenario}-{sample_index}-{label}",
                   args.memory_max_mib * MIB, args.cpus,
                   args.second_memory_max_mib * MIB if args.second_memory_max_mib else None)
    jobs, samples, logs = [], [], []
    high_limits = HighLimits()
    abort = None
    started = time.monotonic()
    try:
        while True:
            elapsed = time.monotonic() - started
            for leaf in group.leaves:
                maximum = int(read(leaf / "memory.max"))
                high = min(args.memory_high_mib * MIB, maximum * 2 // 3)
                desired = str(pressure_high(scenario, elapsed, high, maximum))
                high_limits.update(leaf, desired)
            while specifications and specifications[0][0] <= elapsed:
                at, kind, project = specifications.pop(0)
                leaf = group.leaves[len(jobs) % len(group.leaves)]
                overlapped = any(job["kind"] == "background" and job["process"].poll() is None
                                 for job in jobs)
                stream = (batch / f"{project.name}.log").open("w")
                logs.append(stream)
                command = [str(args.cargo), "build", "--offline", "--release", "--workspace",
                           "--jobs", str(args.jobs_per_build), "--manifest-path", str(project / "Cargo.toml")]
                child = subprocess.Popen(
                    [sys.executable, str(SCRIPT), "_exec", str(leaf),
                     "10" if kind == "background" else "0", *command],
                    cwd=project, env=environment(args, binary, batch, project),
                    stdout=stream, stderr=subprocess.STDOUT, start_new_session=True,
                )
                jobs.append({"kind": kind, "project": project.name, "scheduled_s": at,
                             "overlapped_background": overlapped,
                             "started_s": time.monotonic() - started, "process": child})
            for job in jobs:
                if "returncode" not in job and job["process"].poll() is not None:
                    job["returncode"] = job["process"].returncode
                    job["duration_s"] = time.monotonic() - started - job["started_s"]
            current = {"elapsed_s": elapsed, "parent": snapshot(group.root),
                       "groups": [snapshot(leaf) for leaf in group.leaves],
                       "host_available_bytes": memory_info()["MemAvailable"],
                       **compiler_rss(group.leaves)}
            ceiling = batch / "cache/scheduler/pressure"
            current["shared_pressure_record"] = read(ceiling) if ceiling.exists() else None
            samples.append(current)
            abort = safety_reason(current, args.memory_max_mib * MIB, args.host_available_floor_mib * MIB)
            if any(job.get("returncode", 0) != 0 for job in jobs):
                abort = abort or "Cargo failed; inspect retained job logs"
            if elapsed >= args.timeout_s:
                abort = abort or "batch wall-clock deadline reached"
            if abort or (not specifications and all("returncode" in job for job in jobs)):
                break
            time.sleep(args.sample_interval_s)
    except BaseException as error:
        abort = f"interrupted: {type(error).__name__}: {error}"
        raise
    finally:
        elapsed = time.monotonic() - started
        try:
            high_limits.stop()
            try:
                group.close()
            finally:
                high_limits.wait()
        except BaseException as error:
            abort = abort or f"cleanup failed: {error}"
            raise
        finally:
            for job in jobs:
                child = job.pop("process")
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait(timeout=10)
                job.setdefault("returncode", child.returncode)
                job.setdefault("duration_s", elapsed - job["started_s"])
            for stream in logs:
                stream.close()
            result = {"arm": label, "scenario": scenario, "sample": sample_index,
                      "abort": abort, "jobs": jobs, "unstarted_jobs": len(specifications),
                      "summary": summarize(samples, jobs, elapsed), "samples": samples}
            (batch / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    if abort:
        raise RuntimeError(f"{batch.name}: {abort}; evidence retained in {batch}")
    return result


def arguments(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--head", type=Path, required=True)
    parser.add_argument("--base-revision", required=True)
    parser.add_argument("--head-revision", required=True)
    parser.add_argument("--control-patch", type=Path, required=True)
    parser.add_argument("--cargo", type=Path, required=True, help="absolute real Cargo executable")
    parser.add_argument("--rustc", type=Path, required=True, help="absolute real rustc executable")
    parser.add_argument("--output", type=Path, required=True, help="new, task-owned directory")
    parser.add_argument("--cgroup-parent", type=Path, required=True, help="delegated cgroup v2 parent")
    parser.add_argument("--dedicated-host", action="store_true", required=True)
    parser.add_argument("--memory-max-mib", type=int, default=3072)
    parser.add_argument("--memory-high-mib", type=int, default=512)
    parser.add_argument("--second-memory-max-mib", type=int)
    parser.add_argument("--host-available-floor-mib", type=int, default=1024)
    parser.add_argument("--cpus", type=int, default=8)
    parser.add_argument("--background-builds", type=int, default=4)
    parser.add_argument("--jobs-per-build", type=int, default=4)
    parser.add_argument("--crates", type=int, default=8)
    parser.add_argument("--functions", type=int, default=3000)
    parser.add_argument("--foreground-functions", type=int, default=300)
    parser.add_argument("--foreground-at", type=float, nargs="+", default=[5, 20, 35])
    parser.add_argument("--timeout-s", type=float, default=240)
    parser.add_argument("--sample-interval-s", type=float, default=.2)
    parser.add_argument("--samples", type=int, default=2)
    parser.add_argument("--seed", type=int, default=1773)
    parser.add_argument("--scenario", choices=("calm", "sustained", "alternating"),
                        nargs="+", default=["calm", "sustained", "alternating"])
    args = parser.parse_args(argv)
    positive = ("memory_max_mib", "memory_high_mib", "host_available_floor_mib", "cpus",
                "background_builds", "jobs_per_build", "crates", "functions",
                "foreground_functions", "timeout_s", "sample_interval_s", "samples")
    if any(getattr(args, name) <= 0 for name in positive):
        parser.error("counts, memory limits and time bounds must be positive")
    if not all(math.isfinite(value) for value in
               (args.timeout_s, args.sample_interval_s, *args.foreground_at)):
        parser.error("time bounds and foreground starts must be finite")
    if not .05 <= args.sample_interval_s <= 1 or args.timeout_s > 600:
        parser.error("sample interval must be 0.05–1 s; batch deadline must be at most 600 s")
    if args.memory_high_mib >= args.memory_max_mib * .8:
        parser.error("memory.high must be below 80 percent of memory.max")
    if args.second_memory_max_mib is not None and not 128 <= args.second_memory_max_mib < args.memory_max_mib:
        parser.error("second cgroup memory.max must be at least 128 MiB and below the shared cap")
    if any(at < 0 or at >= args.timeout_s for at in args.foreground_at):
        parser.error("foreground starts must fall within the batch deadline")
    if args.base_revision != args.head_revision:
        parser.error("policy comparison requires the same source revision plus the control patch")
    if args.samples > 10 or args.background_builds > 16 or args.jobs_per_build > 16:
        parser.error("at most 10 pairs, 16 background builds and 16 jobs per build")
    for name in ("base", "head", "control_patch", "cargo", "rustc", "cgroup_parent"):
        setattr(args, name, getattr(args, name).resolve(strict=True))
    args.output = args.output.absolute()
    return args


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] == "_write_high":
        # Linux 6.1 ignores O_NONBLOCK for memory.high. Use a separate process
        # even on newer kernels so reclaim cannot stall stop checks.
        fd = os.open(argv[1], os.O_WRONLY | os.O_NONBLOCK)
        try:
            os.write(fd, argv[2].encode())
        finally:
            os.close(fd)
        return
    if argv and argv[0] == "_exec":
        # The helper joins before exec: no Cargo/compiler can start outside
        # the task cgroup, and no preexec_fn runs in a multithreaded parent.
        group, nice, *command = argv[1:]
        Path(group, "cgroup.procs").write_text(str(os.getpid()))
        if int(nice):
            os.nice(int(nice))
        os.execvpe(command[0], command, os.environ)
    args = arguments(argv)
    if sys.platform != "linux":
        raise ValueError("real pressure runs require a dedicated Linux machine")
    if os.getpriority(os.PRIO_PROCESS, 0) != 0:
        raise ValueError("supervisor must start at nice 0 for the foreground comparison")
    if args.cpus > len(os.sched_getaffinity(0)):
        raise ValueError("CPU cap exceeds visible affinity; this would not exercise the declared pool")
    host = memory_info()
    if args.memory_max_mib * MIB > host["MemTotal"] * .6:
        raise ValueError("hard workload cap must be at most 60 percent of host memory")
    if host["MemAvailable"] < (args.memory_max_mib + args.host_available_floor_mib) * MIB:
        raise ValueError("insufficient host memory for the cap plus the stop floor")
    if not {"memory", "cpu", "pids"} <= set(read(args.cgroup_parent / "cgroup.controllers").split()):
        raise ValueError("parent must delegate memory, cpu and pids controllers")
    args.output.mkdir(parents=False)  # Never overwrite or clear a previous result.
    schedule = []
    rng = random.Random(args.seed)
    for scenario in args.scenario:
        orders = ["base", "head"] * (args.samples // 2)
        if args.samples % 2:
            orders.append(rng.choice(("base", "head")))
        rng.shuffle(orders)
        for sample, first in enumerate(orders):
            schedule.extend((scenario, sample, label) for label in
                            (["base", "head"] if first == "base" else ["head", "base"]))
    metadata = {"version": 1, "measurement": "real_generated_rust_builds",
                "platform": platform.platform(), "host_memory": host,
                "affinity": sorted(os.sched_getaffinity(0)),
                "instrument": identity(SCRIPT), "base": identity(args.base),
                "head": identity(args.head), "control_patch": identity(args.control_patch),
                "base_revision": args.base_revision, "head_revision": args.head_revision,
                "cargo": subprocess.check_output([str(args.cargo), "-V"], text=True).strip(),
                "rustc": subprocess.check_output([str(args.rustc), "-Vv"], text=True).strip(),
                "arguments": {key: str(value) if isinstance(value, Path) else value
                              for key, value in vars(args).items()}, "schedule": schedule,
                "swap_policy": "disabled in every task cgroup"}
    (args.output / "experiment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    (args.output / "control.patch").write_text(args.control_patch.read_text())
    def terminate(_signal, _frame):
        raise KeyboardInterrupt("termination requested")
    signal.signal(signal.SIGTERM, terminate)
    results = []
    for scenario, sample, label in schedule:
        print(f"{scenario} sample {sample}: {label}", flush=True)
        results.append(run_batch(args, label, getattr(args, label), sample, scenario))
        (args.output / "summary.json").write_text(json.dumps(
            [{key: value for key, value in result.items() if key != "samples"} for result in results],
            indent=2) + "\n")


if __name__ == "__main__":
    main()
