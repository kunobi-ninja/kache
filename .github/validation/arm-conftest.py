"""Bounded first-party configure probe for a disposable Windows ARM runner."""
import contextlib
import hashlib
import json
import msvcrt
import os
from pathlib import Path
import platform
import shutil
import struct
import subprocess
import tempfile
import time
import urllib.request
from zipfile import ZipFile

VERSION = "v2.3.1"
ARCHIVE = "kache-aarch64-pc-windows-msvc.zip"
SHA256 = "c27cfe13fdbfcc1c3ee14b8ecdff0b981163fd040a9e55fe8c59aa247b7589c0"
ARM64 = 0xAA64


def image_machine(path):
    with path.open("rb") as f:
        f.seek(0x3C)
        f.seek(struct.unpack("<I", f.read(4))[0] + 4)
        return struct.unpack("<H", f.read(2))[0]


def native_clang():
    candidates = []
    on_path = shutil.which("clang-cl")
    if on_path:
        candidates.append(Path(on_path))
    programs = Path(os.environ["ProgramFiles"])
    candidates.append(programs / "LLVM/bin/clang-cl.exe")
    vs = programs / "Microsoft Visual Studio"
    if vs.exists():
        candidates.extend(vs.glob("*/*/VC/Tools/Llvm/*/bin/clang-cl.exe"))
        candidates.extend(vs.glob("*/*/VC/Tools/Llvm/bin/clang-cl.exe"))
    print("Compiler candidates", [(str(p), hex(image_machine(p))) for p in candidates if p.is_file()])
    return next(p for p in candidates if p.is_file() and image_machine(p) == ARM64)


def main():
    root = Path(tempfile.mkdtemp(prefix="kache-arm-", dir=os.environ["RUNNER_TEMP"]))
    out = Path(os.environ["GITHUB_WORKSPACE"]) / "arm-conftest.json"
    observations = []
    env = {k: v for k, v in os.environ.items() if not k.startswith("KACHE_")}
    env.update(KACHE_CONFIG=str(root / "config.toml"), KACHE_CACHE_DIR=str(root / "cache"),
               KACHE_RUNTIME_DIR=str(root / "runtime"), KACHE_PHASE_TRACE_DIR=str(root / "traces"))
    (root / "config.toml").touch()
    (root / "runtime").mkdir()
    (root / "sdk").mkdir()
    assert os.environ["RUNNER_ARCH"] == "ARM64", platform.machine()
    archive = root / ARCHIVE
    urllib.request.urlretrieve(f"https://github.com/kunobi-ninja/kache/releases/download/{VERSION}/{ARCHIVE}", archive)
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == SHA256
    with ZipFile(archive) as release:
        release.extractall(root / "tool")
    binary = next((root / "tool").rglob("kache.exe"))
    engine = next((root / "tool").rglob("kache-engine.exe"))
    assert image_machine(binary) == image_machine(engine) == ARM64
    compiler = native_clang()
    (root / "conftest.c").write_text("int probe(void) {return 17;}\n")

    def run(label, argv, expected=0):
        start = time.monotonic()
        result = subprocess.run(argv, cwd=root, env=env, capture_output=True, timeout=42)
        elapsed = round((time.monotonic() - start) * 1000, 1)
        item = {"phase": label, "wall_ms": elapsed, "exit_code": result.returncode}
        if "compile" not in label:
            item["detail"] = (result.stdout + result.stderr).decode(errors="replace").strip()[:400]
        observations.append(item)
        print(json.dumps(item), flush=True)
        out.write_text(json.dumps(observations, indent=2), encoding="utf8")
        if expected is not None and result.returncode != expected:
            raise RuntimeError(f"{label}: {result.stderr.decode(errors='replace')[:1000]}")
        return item

    def compile_probe(phase, wrapped=True):
        obj = root / "conftest.obj"
        obj.unlink(missing_ok=True)
        args = [compiler, "-fms-compatibility-version=19.51", "-winsysroot", "sdk", "conftest.c", "-c"]
        run(phase + "_compile", ([binary] if wrapped else []) + args)
        assert obj.is_file() and int.from_bytes(obj.read_bytes()[:2], "little") == ARM64
        event_path = root / "runtime/events.jsonl"
        if wrapped and event_path.exists():
            events = [json.loads(line) for line in event_path.read_text(encoding="utf8").splitlines()]
            if events:
                event = events[-1]
                observations[-1]["event"] = {k: event.get(k) for k in ("crate_name", "result", "passthrough_reason", "startup_ms", "elapsed_ms")}
                print("wrapper_event", json.dumps(observations[-1]["event"]), flush=True)

    run("tool_version", [binary, "--version"])
    run("compiler_version", [compiler, "--version"])
    try:
        compile_probe("direct", wrapped=False)
        compile_probe("cold")
        run("explicit_start", [binary, "daemon", "start"])
        run("running_status", [binary, "daemon", "status"])
        for _ in range(4):
            compile_probe("warm")
        # Hold only our fixture's existing start lock. This models another
        # concurrent starter, while keeping the real compiler and CLI intact.
        run("stop", [binary, "daemon", "stop"])
        locks = list((root / "runtime").rglob("*.lock")) + list((root / "cache").rglob("*.lock"))
        print("coordination files", [str(p.relative_to(root)) for p in locks], flush=True)
        start_lock = next(p for p in locks if p.name == "daemon.lock")
        with start_lock.open("r+b") as lock:
            lock.seek(0)
            msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
            try:
                run("contended_explicit_start", [binary, "daemon", "start"], expected=None)
                for _ in range(2):
                    compile_probe("contended")
            finally:
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
        run("recovered_start", [binary, "daemon", "start"])
        compile_probe("recovered")
    finally:
        with contextlib.suppress(Exception):
            run("cleanup", [binary, "daemon", "stop"], expected=None)
        out.write_text(json.dumps(observations, indent=2), encoding="utf8")


if __name__ == "__main__":
    main()
