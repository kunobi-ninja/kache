#!/usr/bin/env python3
"""Retain native configure timings and the pinned #1792 workload comparison."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import time


def environment():
    # Build scripts do not need the runner's Actions, GitHub or cloud tokens.
    allowed = {
        "PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "TEMP", "TMP",
        "PROGRAMFILES", "PROGRAMFILES(X86)", "PROGRAMW6432", "APPDATA",
        "LOCALAPPDATA", "USERPROFILE", "HOME", "CARGO_HOME", "RUSTUP_HOME",
        "NUMBER_OF_PROCESSORS", "PROCESSOR_ARCHITECTURE", "PROCESSOR_ARCHITEW6432",
        "VSINSTALLDIR", "VCINSTALLDIR", "VCTOOLSINSTALLDIR", "INCLUDE", "LIB",
        "LIBPATH", "WINDOWSSDKDIR", "WINDOWSSDKVERSION", "VSCMD_ARG_TGT_ARCH",
    }
    return {key: value for key, value in os.environ.items() if key.upper() in allowed}


def capture(argv, cwd, env, prefix, timeout=120):
    started = time.perf_counter()
    try:
        # Firefox configure captures both pipes with close_fds=False on Windows.
        result = subprocess.run(argv, cwd=cwd, env=env, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, close_fds=False,
                                timeout=timeout, check=False)
    except subprocess.TimeoutExpired as error:
        prefix.with_suffix(".stdout").write_bytes(error.stdout or b"")
        prefix.with_suffix(".stderr").write_bytes(error.stderr or b"")
        prefix.with_suffix(".json").write_text(json.dumps({
            "argv": list(map(str, argv)), "timed_out": True,
            "elapsed_ms": (time.perf_counter() - started) * 1000,
        }, indent=2), encoding="utf-8")
        raise
    prefix.with_suffix(".stdout").write_bytes(result.stdout)
    prefix.with_suffix(".stderr").write_bytes(result.stderr)
    record = {"argv": list(map(str, argv)), "elapsed_ms": (time.perf_counter() - started) * 1000,
              "exit_code": result.returncode}
    prefix.with_suffix(".json").write_text(json.dumps(record, indent=2), encoding="utf-8")
    print(json.dumps(record), flush=True)
    if result.returncode:
        raise RuntimeError(f"Command failed; inspect {prefix}.stderr")
    return result, record


def config(root, cache_executables=False, trust=False):
    root.mkdir(parents=True, exist_ok=False)
    config_file = root / "config.toml"
    config_file.write_text(
        "[cache]\nlocal_only=true\nauto_gc=false\nindex_auto_compact=false\n"
        "daemon_publish=false\nprefetch_enabled=false\nnative_runtime_warmup=true\n"
        f"cache_executables={str(cache_executables).lower()}\n"
        f"trust_daemon_execution={str(trust).lower()}\n", encoding="utf-8")
    env = environment()
    env.update(KACHE_CONFIG=str(config_file), KACHE_HOST_CONFIG="",
               KACHE_CACHE_DIR=str(root / "cache"), KACHE_RUNTIME_DIR=str(root / "run"),
               KACHE_LOG="kache=debug", KACHE_LOG_FILE="kache=debug,kache_store=debug")
    return env


def stop(kache, root, env):
    # Only this fixture's private runtime is addressed.
    subprocess.run([kache, "daemon", "stop"], cwd=root, env=env,
                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, check=False)


def configure(kache, output, host):
    clang = shutil.which("clang-cl.exe")
    if not clang:
        candidate = Path(os.environ["ProgramFiles"]) / "LLVM/bin/clang-cl.exe"
        if candidate.is_file():
            clang = str(candidate)
    if not clang:
        raise RuntimeError("Native clang-cl is required; no silent skip")
    capture([clang, "--version"], output, environment(), output / "clang-identity")
    records = []
    for trust in (False, True):
        root = output / f"configure-trust-{trust}"
        env = config(root, trust=trust)
        try:
            for index, flags in enumerate((['-E'], ['-c'], ['-E'], ['-c'])):
                source = root / f"conftest-{index}.c"
                source.write_text("int configure_answer(void) { return 42; }\n", encoding="utf-8")
                args = [clang, "/nologo", "--target=" + host, *flags, str(source)]
                if flags == ['-c']:
                    args.append('/Fo' + str(root / f'conftest-{index}.obj'))
                baseline, bare = capture(args, root, env, root / f"bare-{index}")
                wrapped, timed = capture([kache, *args], root, env, root / f"wrapped-{index}")
                if wrapped.stdout != baseline.stdout:
                    raise RuntimeError("Preprocessing output changed through the wrapper")
                if flags == ['-c']:
                    actual = (root / f'conftest-{index}.obj').read_bytes()[:2]
                    expected = b'\x64\xaa' if host.startswith('aarch64') else b'\x64\x86'
                    if actual != expected:
                        raise RuntimeError(f"Wrong COFF target machine: {actual.hex()}")
                records.append({"trust_daemon_execution": trust, "flags": flags,
                                "bare_ms": bare['elapsed_ms'], "wrapped_ms": timed['elapsed_ms']})
        finally:
            stop(kache, root, env)
    (output / "configure-summary.json").write_text(json.dumps(records, indent=2), encoding="utf-8")


def abundance(kache, workload, output, host):
    toolchain = "nightly-2026-08-25-" + host
    flags = "-C strip=symbols -C linker=rust-lld -Cllvm-args=--inline-threshold=10000 -C embed-bitcode -C lto -Z dylib-lto -C target-cpu=native"
    records = []
    for cache_executables in (False, True):
        for crate in ("ab-blake3", "ab-riscv-interpreter"):
            root = output / f"abundance-executables-{cache_executables}-{crate}"
            env = config(root, cache_executables=cache_executables)
            env.update(RUSTFLAGS=flags, RUSTUP_TOOLCHAIN=toolchain, CARGO_INCREMENTAL="0",
                       CARGO_BUILD_WARNINGS="deny", CARGO_TERM_COLOR="always", RUST_BACKTRACE="full")
            try:
                command = ["cargo", "+" + toolchain, "-Zgitoxide", "-Zgit", "build", "--locked",
                           "--release", "--all-targets", "--features", "no-panic", "--package", crate,
                           "--timings"]
                modes = ["warmup", "bare", "wrapped"] if crate == "ab-blake3" else ["warmup", "wrapped", "bare"]
                for mode in modes:
                    run_env = env.copy()
                    if mode == 'wrapped':
                        run_env['RUSTC_WRAPPER'] = kache
                    target = root / f"target-{crate}-{mode}"
                    run_env['CARGO_TARGET_DIR'] = str(target)
                    _, result = capture(command, workload, run_env, root / f"{crate}-{mode}", timeout=1200)
                    if mode != 'warmup':
                        records.append({"crate": crate, "mode": mode, "cache_executables": cache_executables,
                                        "elapsed_ms": result['elapsed_ms']})
                    timings = target / "cargo-timings"
                    if timings.exists():
                        shutil.copytree(timings, root / f"timings-{crate}-{mode}")
                    # This directory was created above in the fixture's owned root.
                    shutil.rmtree(target)
                for name, args in (("report", ["report", "--format", "json"]), ("daemon", ["daemon", "status", "--json"])):
                    capture([kache, *args], workload, env, root / name)
            finally:
                stop(kache, root, env)
    (output / "abundance-summary.json").write_text(json.dumps(records, indent=2), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kache", required=True, type=Path)
    parser.add_argument("--workload", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--host", required=True, choices=("x86_64-pc-windows-msvc", "aarch64-pc-windows-msvc"))
    args = parser.parse_args()
    if os.name != "nt":
        parser.error("This acceptance fixture requires native Windows")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    kache = str(args.kache.resolve())
    configure(kache, output, args.host)
    abundance(kache, args.workload.resolve(), output, args.host)


if __name__ == '__main__':
    main()
