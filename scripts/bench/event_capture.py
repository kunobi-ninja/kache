"""Check whether a Kache build event remains observable under lock contention."""

import json
from pathlib import Path
import re
import stat
import subprocess
import tempfile


_DROP = re.compile(
    r"^(?:\S+[ \t]+)?DEBUG[ \t]+kache::wrapper(?P<modern>::event_log)?: "
    r"could not record compiler event:", re.MULTILINE
)


def event_drop_counts(log_text):
    """Return (legacy, modern) wrapper diagnostics from an uncolored file log."""
    targets = _DROP.findall(log_text)
    return targets.count(""), targets.count("::event_log")


def read_overflow_event(path):
    """Read one atomically published JSONL build event with its original LF."""
    if not path.match("event-?*.jsonl") or not stat.S_ISREG(path.lstat().st_mode):
        raise ValueError(f"unfinished or unexpected Kache overflow entry: {path}")
    raw = path.read_bytes()
    if not raw.endswith(b"\n") or raw.count(b"\n") != 1:
        raise ValueError(f"incomplete Kache overflow event in {path}")
    line = raw.decode("utf-8")
    event = json.loads(line)
    if not isinstance(event, dict) or not all(
        isinstance(event.get(key), str) and event[key] for key in ("crate_name", "result")
    ):
        raise ValueError(f"invalid Kache build event in {path}")
    return line, event


def detect_event_capture(binary, rustc, env, stop_kache):
    """Exercise the measured producer once before the timed campaign."""
    import fcntl

    binary = Path(binary).absolute()
    rustc = Path(rustc)
    if not rustc.is_absolute():
        raise ValueError("the event capture probe requires an absolute rustc path")

    # Keep the Unix socket short even if the campaign output path is long.
    with tempfile.TemporaryDirectory(prefix="kec-", dir="/tmp") as temp:
        root = Path(temp)
        runtime = root / "run"
        runtime.mkdir(mode=0o700)
        overflow = runtime / "events.jsonl.overflow"
        overflow.mkdir(mode=0o700)
        config = root / "kache.toml"
        config.touch()
        diagnostic = root / "wrapper.log"
        probe_env = dict(
            env,
            HOME=str(root),
            KACHE_HOST_CONFIG="",
            KACHE_CONFIG=str(config),
            KACHE_CACHE_DIR=str(root / "cache"),
            KACHE_RUNTIME_DIR=str(runtime),
            KACHE_SOCKET_PATH=str(runtime / "daemon.sock"),
            KACHE_LOCAL_ONLY="1",
            KACHE_LOG_FILE="kache::wrapper=debug",
            KACHE_LOG_FILE_PATH=str(diagnostic),
            RUSTUP_AUTO_INSTALL="0",
        )
        try:
            with (runtime / "events.jsonl.lock").open("wb") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                probe = subprocess.run(
                    [str(binary), str(rustc), "-vV"], cwd=root, env=probe_env,
                    capture_output=True, text=True, check=True, timeout=30,
                )
                if not probe.stdout.startswith("rustc ") or "\nhost: " not in probe.stdout:
                    raise ValueError(f"{binary}: expected rustc -vV output during event capture probe")
                primary = runtime / "events.jsonl"
                if primary.exists() and primary.stat().st_size:
                    raise ValueError(f"{binary}: wrote to the locked primary event log")
                entries = list(overflow.iterdir())
                log = diagnostic.read_text() if diagnostic.is_file() else ""
                legacy, modern = event_drop_counts(log)
                if not entries and (legacy, modern) == (1, 0):
                    return "diagnostic"
                if len(entries) == 1 and (legacy, modern) == (0, 0):
                    _, event = read_overflow_event(entries[0])
                    reason = event.get("passthrough_reason")
                    if (
                        event["result"] == "passthrough"
                        and isinstance(reason, str)
                        and reason.partition("|")[0] == "not-a-compile"
                    ):
                        return "overflow"
                raise ValueError(
                    f"{binary}: event capture probe expected one overflow probe or one legacy "
                    f"drop diagnostic, found {len(entries)} entries and {legacy}/{modern} diagnostics"
                )
        finally:
            # Even compiler queries can autostart a daemon on older releases.
            stop_kache(binary, probe_env, root)
