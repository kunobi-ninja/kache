#!/usr/bin/env python3
"""Measure sampled build and idle memory on macOS using isolated Cargo builds."""

from __future__ import annotations

import argparse
import ctypes
import errno
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import time


class Usage(ctypes.Structure):
    # Darwin's rusage_info_v0 from sys/resource.h.
    _fields_ = [
        ("uuid", ctypes.c_uint8 * 16),
        ("counters", ctypes.c_uint64 * 7),
        ("footprint", ctypes.c_uint64),
        ("started", ctypes.c_uint64),
        ("exited", ctypes.c_uint64),
    ]


class Footprint:
    """Track the build and daemon together, including workers in their own groups."""

    def __init__(self) -> None:
        libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
        self.usage = libproc.proc_pid_rusage
        self.usage.argtypes = (ctypes.c_int, ctypes.c_int, ctypes.c_void_p)
        self.children = libproc.proc_listchildpids
        self.children.argtypes = (ctypes.c_int, ctypes.c_void_p, ctypes.c_int)
        self.tracked: dict[int, int] = {}
        if self.read(os.getpid()) is None:
            raise RuntimeError("could not read a physical footprint with libproc")

    def read(self, pid: int) -> Usage | None:
        usage = Usage()
        if self.usage(pid, 0, ctypes.byref(usage)) == 0:
            return usage
        error = ctypes.get_errno()
        if error == errno.ESRCH:
            return None
        raise OSError(error, f"cannot read memory for PID {pid}: {os.strerror(error)}")

    def child_pids(self, pid: int) -> list[int]:
        capacity = 64
        while True:
            buffer = (ctypes.c_int * capacity)()
            ctypes.set_errno(0)
            count = self.children(pid, buffer, ctypes.sizeof(buffer))
            error = ctypes.get_errno()
            if count < 0 or (count == 0 and error not in (0, errno.ESRCH)):
                raise OSError(error, f"cannot list children of PID {pid}: {os.strerror(error)}")
            if count < capacity:
                return [child for child in buffer[:max(0, count)] if child > 0]
            capacity *= 2

    def sample(self, roots: list[int]) -> int:
        pending = list(self.tracked)
        pending.extend(roots)
        seen: set[int] = set()
        live: dict[int, int] = {}
        total = 0
        while pending:
            pid = pending.pop()
            if pid in seen:
                continue
            seen.add(pid)
            usage = self.read(pid)
            if usage is None or usage.exited:
                continue
            if pid in self.tracked and usage.started != self.tracked[pid]:
                # A formerly observed worker may have exited and its PID been reused.
                continue
            live[pid] = usage.started
            total += usage.footprint
            pending.extend(self.child_pids(pid))
        self.tracked = live
        return total


def terminate(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


def await_daemon(process: subprocess.Popen, path: Path) -> None:
    deadline = time.monotonic() + 10
    while process.poll() is None and time.monotonic() < deadline:
        with socket.socket(socket.AF_UNIX) as client:
            client.settimeout(0.1)
            try:
                client.connect(str(path))
                return
            except (FileNotFoundError, ConnectionRefusedError, TimeoutError):
                time.sleep(0.05)
    raise RuntimeError("the isolated Kache daemon did not become ready")


def measure(
    command: list[str], cwd: Path, env: dict[str, str], log: Path,
    sampler: Footprint, daemon: subprocess.Popen | None, interval: float,
    timeout: float,
) -> dict:
    with log.open("wb") as output:
        start = time.monotonic()
        process = subprocess.Popen(
            command, cwd=cwd, env=env, stdout=output, stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        samples = []
        try:
            while process.poll() is None:
                elapsed = time.monotonic() - start
                if elapsed >= timeout:
                    raise TimeoutError(f"build exceeded {timeout:g} seconds")
                roots = [process.pid]
                if daemon is not None:
                    if daemon.poll() is not None:
                        raise RuntimeError("the isolated Kache daemon exited during a build")
                    roots.append(daemon.pid)
                samples.append([round(elapsed, 6), sampler.sample(roots)])
                time.sleep(interval)
            if process.returncode:
                raise RuntimeError(f"cargo exited with status {process.returncode}")
            if not any(footprint for _, footprint in samples):
                raise RuntimeError("the sampler captured no nonzero build memory")
            return {
                "seconds": round(time.monotonic() - start, 6),
                "sampled_peak_bytes": max(footprint for _, footprint in samples),
                "samples": samples,
            }
        finally:
            terminate(process)


def git(*arguments: str, cwd: Path | None = None) -> str:
    return subprocess.check_output(
        ["git", "-c", "core.hooksPath=/dev/null", *arguments], cwd=cwd, text=True,
    ).strip()


def arm(args: argparse.Namespace, revision: str, root: Path, mode: str) -> dict:
    root.mkdir()
    checkout = root / "src"
    git("clone", "--quiet", "--shared", "--no-checkout", "--", str(args.project), str(checkout))
    git("checkout", "--quiet", "--detach", revision, cwd=checkout)
    git("apply", "--check", str(args.patch), cwd=checkout)
    cargo = args.cargo or Path(subprocess.check_output(
        ["rustup", "which", "cargo"], cwd=checkout, text=True,
    ).strip())
    env = {key: value for key, value in os.environ.items() if not key.startswith("KACHE_")}
    env.update(
        CARGO_TARGET_DIR=str(root / "target"),
        CARGO_BUILD_BUILD_DIR=str(root / "build"),
        RUSTC_WRAPPER=str(args.kache) if mode == "kache" else "",
        RUSTC_WORKSPACE_WRAPPER="",
    )
    command = [str(cargo), "build", "--locked", "--offline"]
    result = {
        "mode": mode,
        "cargo": subprocess.check_output([str(cargo), "--version"], cwd=checkout, env=env, text=True).strip(),
    }
    sampler = Footprint()
    daemon = None
    daemon_log = root / "daemon.log"
    try:
        if mode == "kache":
            config = root / "kache.toml"
            config.write_text("[cache]\nlocal_only = true\n", encoding="utf-8")
            daemon_socket = root / "k.sock"
            env.update(
                KACHE_CONFIG=str(config), KACHE_HOST_CONFIG="",
                KACHE_CACHE_DIR=str(root / "cache"), KACHE_RUNTIME_DIR=str(root),
                KACHE_SOCKET_PATH=str(daemon_socket), KACHE_DAEMON_IDLE_TIMEOUT="0",
            )
            with daemon_log.open("wb") as output:
                daemon = subprocess.Popen(
                    [str(args.kache), "daemon", "run"], cwd=checkout, env=env,
                    stdout=output, stderr=subprocess.STDOUT, start_new_session=True,
                )
            await_daemon(daemon, daemon_socket)
        for phase in ("cold", "warm_edit"):
            if phase == "warm_edit":
                git("apply", str(args.patch), cwd=checkout)
            result[phase] = measure(
                command, checkout, env, root / f"{phase}.log", sampler, daemon,
                args.interval_ms / 1000, args.timeout,
            )
            time.sleep(args.idle_seconds)
            if daemon is not None and daemon.poll() is not None:
                raise RuntimeError("the isolated Kache daemon exited before the idle sample")
            result[phase]["retained_bytes"] = sampler.sample(
                [daemon.pid] if daemon is not None else [],
            )
        return result
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        for log in (root / "cold.log", root / "warm_edit.log", daemon_log):
            if log.exists():
                with log.open("rb") as source:
                    source.seek(max(0, log.stat().st_size - 2000))
                    print(f"{mode} {log.name}:\n{source.read().decode(errors='replace')}", file=sys.stderr)
        raise RuntimeError(f"{mode}: {error}") from error
    finally:
        if daemon is not None:
            terminate(daemon)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", type=Path, required=True, help="Git repository with a locked Cargo workspace")
    parser.add_argument("--patch", type=Path, required=True, help="warm edit as a git diff against HEAD")
    parser.add_argument("--kache", type=Path, required=True, help="Kache executable to benchmark")
    parser.add_argument("--cargo", type=Path, help="real Cargo executable (default: rustup which cargo)")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--interval-ms", type=float, default=20)
    parser.add_argument("--idle-seconds", type=float, default=1)
    parser.add_argument("--timeout", type=float, default=1800, help="seconds allowed per build")
    args = parser.parse_args()
    if sys.platform != "darwin":
        parser.error("this benchmark requires macOS")
    for key in ("project", "patch", "kache", "cargo"):
        if path := getattr(args, key):
            setattr(args, key, path.resolve(strict=True))
    if args.repeats < 1 or not all(
        math.isfinite(value) and value > 0
        for value in (args.interval_ms, args.idle_seconds, args.timeout)
    ):
        parser.error("repeats, sample interval, idle delay and timeout must be positive")
    revision = git("rev-parse", "HEAD", cwd=args.project)
    kache_version = subprocess.check_output([str(args.kache), "--version"], text=True).strip()
    patch_sha256 = hashlib.sha256(args.patch.read_bytes()).hexdigest()
    runs = []
    for index in range(args.repeats):
        order = ("cargo", "kache") if index % 2 == 0 else ("kache", "cargo")
        for mode in order:
            # A short root also keeps the isolated Unix socket within Darwin's path limit.
            with tempfile.TemporaryDirectory(prefix="kmm-", dir="/tmp") as tmp:
                runs.append(arm(args, revision, Path(tmp) / mode, mode))
    medians = {
        mode: {
            phase: {
                metric: statistics.median(run[phase][metric] for run in runs if run["mode"] == mode)
                for metric in ("seconds", "sampled_peak_bytes", "retained_bytes")
            }
            for phase in ("cold", "warm_edit")
        }
        for mode in ("cargo", "kache")
    }
    json.dump({
        "revision": revision, "patch_sha256": patch_sha256, "kache": kache_version,
        "interval_ms": args.interval_ms, "idle_seconds": args.idle_seconds,
        "measurement": "sampled sum of Darwin per-process physical footprints",
        "medians": medians, "runs": runs,
    }, sys.stdout, indent=2)
    print()


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        sys.exit(str(error))
