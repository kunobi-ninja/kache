#!/usr/bin/env python3
"""One bounded, cold-store ARM64 diagnostic; no timing-parity acceptance claim."""

import argparse
from collections import Counter
import ctypes
from ctypes import wintypes
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import threading
import time

SOURCE = "a31c14b58e8a58f283e7af86ee982519ee0e72cc"
WORKLOAD = "287cf9c6d266ce28d644bed050ce9a835f31fd80"
HOST = "aarch64-pc-windows-msvc"
TIMED_UNIT = "ab_riscv_interpreter"
# The pinned all-targets workload reports one library and one final test compile.
EXPECTED_TIMED_COMPILATIONS = 2
KNOWN_ROUTES = {'daemon_pipeline', 'local_engine_pipeline', 'client_pipeline'}
TOOLCHAIN = "nightly-2026-08-25-" + HOST
FLAGS = "-C strip=symbols -C linker=rust-lld -Cllvm-args=--inline-threshold=10000 -C embed-bitcode -C lto -Z dylib-lto -C target-cpu=native"
ROUNDS = (("bare", "default", "disabled"), ("default", "disabled", "bare"), ("disabled", "bare", "default"))
PIPELINE_MARKERS = {"key", "dep-info", "compiler", "store", "restore"}
FILETIME_EPOCH_US = 11644473600000000


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2), encoding="utf-8")


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def initialize_sdk(host, output):
    architecture = "arm64" if host.startswith("aarch64") else "x64"
    component = "Microsoft.VisualStudio.Component.VC.Tools." + ("ARM64" if architecture == "arm64" else "x86.x64")
    vswhere = Path(os.environ['ProgramFiles(x86)']) / 'Microsoft Visual Studio/Installer/vswhere.exe'
    installation = subprocess.check_output(
        [str(vswhere), '-latest', '-products', '*', '-requires', component,
         '-property', 'installationPath'], text=True, timeout=30).strip()
    developer = Path(installation) / 'Common7/Tools/VsDevCmd.bat'
    if not installation or not developer.is_file():
        raise RuntimeError("The installed native Visual Studio developer command is required")
    if any(character in str(developer) for character in '\"%!^&|<>\r\n\0'):
        raise RuntimeError("Unsupported command expansion character in the installed SDK path")
    cmd = str(Path(os.environ['SYSTEMROOT']) / 'System32/cmd.exe')
    # Match the maintained msvc-manifest-probe.ps1 quoting. SET stays in memory;
    # only the existing allowlist reaches untrusted workload build scripts.
    command = (f'"{cmd}" /d /u /s /c ""{developer}" -no_logo -arch={architecture} '
               f'-host_arch={architecture} >nul && set"')
    result = subprocess.run(command, executable=cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, timeout=120, check=True)
    for line in result.stdout.decode('utf-16-le').splitlines():
        name, separator, value = line.partition('=')
        if name and separator:
            os.environ[name] = value
    if os.environ.get('VSCMD_ARG_TGT_ARCH') != architecture:
        raise RuntimeError("Developer environment selected the wrong target architecture")
    libraries = [Path(path) for path in os.environ.get('LIB', '').split(';') if path]
    for filename in ('kernel32.lib', 'ucrt.lib', 'vcruntime.lib'):
        if not any((directory / filename).is_file() for directory in libraries):
            raise RuntimeError(f"Developer environment cannot locate {filename}")
    (output / 'sdk-identity.json').write_text(json.dumps({
        'developer_command': str(developer), 'architecture': architecture,
        'command_sha256': hashlib.sha256(developer.read_bytes()).hexdigest(),
        'sdk_version': os.environ.get('WindowsSDKVersion'),
    }, indent=2), encoding='utf-8')


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


class Budget:
    def __init__(self, started):
        # Eight minutes remain before the enclosing 90-minute job limit.
        self.deadline = started + 82 * 60

    def limit(self, requested):
        remaining = self.deadline - time.time()
        if remaining < 15:
            raise TimeoutError("Global diagnostic deadline reached")
        return min(requested, remaining)


def capture(argv, cwd, env, prefix, budget, timeout=120, required=True):
    timeout = budget.limit(timeout)
    started = time.perf_counter()
    process = subprocess.Popen(list(map(str, argv)), cwd=cwd, env=env,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               close_fds=False)
    timed_out = False
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        # Address only the still-owned command tree, before its parent is reaped.
        subprocess.run([str(Path(os.environ['SYSTEMROOT']) / 'System32/taskkill.exe'),
                        '/PID', str(process.pid), '/T', '/F'], env=environment(),
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, check=False)
        try:
            stdout, stderr = process.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate(timeout=15)
    prefix.with_suffix('.stdout').write_bytes(stdout)
    prefix.with_suffix('.stderr').write_bytes(stderr)
    record = {'argv': list(map(str, argv)), 'exit_code': process.returncode,
              'elapsed_ms': (time.perf_counter() - started) * 1000, 'timed_out': timed_out}
    write_json(prefix.with_suffix('.json'), record)
    print(json.dumps(record), flush=True)
    if timed_out or (required and process.returncode):
        raise RuntimeError(f"Command incomplete; inspect {prefix.name}.stderr")
    return stdout, record


def private_config(root, scheduler=True):
    root.mkdir(parents=True, exist_ok=False)
    config_file = root / 'config.toml'
    config_file.write_text(
        '[cache]\nlocal_only=true\nauto_gc=false\nindex_auto_compact=false\n'
        'daemon_publish=false\nprefetch_enabled=false\nnative_runtime_warmup=true\n'
        'cache_executables=true\ntrust_daemon_execution=false\n'
        f'scheduler={str(scheduler).lower()}\n', encoding='utf-8')
    env = environment()
    env.update(KACHE_CONFIG=str(config_file), KACHE_HOST_CONFIG='',
               KACHE_CACHE_DIR=str(root / 'cache'), KACHE_RUNTIME_DIR=str(root / 'run'),
               KACHE_PHASE_TRACE_DIR=str(root / 'phase-traces'),
               KACHE_LOG='kache=debug', KACHE_LOG_FILE='kache=debug,kache_store=debug',
               KACHE_LOG_FILE_PATH=str(root / 'kache.log'))
    return env


def candidate_process_name(name, expected_names):
    # NTFS may expose KACHE-~1.EXE for kache-engine.exe (issue #1792).
    # This widens only the cheap prefilter; the resolved image must still match.
    lowered = name.lower()
    return lowered in expected_names or re.fullmatch(r'kache[-_]?~[1-9][0-9]*\.exe', lowered) is not None


def pinned_image(path, expected):
    return expected.get(os.path.normcase(str(Path(path).resolve())))


def windows_processes(expected):
    """Retain only PID/start/image provenance for our two pinned binary paths."""
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    handle_type = wintypes.HANDLE
    kernel.CreateToolhelp32Snapshot.argtypes = [wintypes.DWORD, wintypes.DWORD]
    kernel.CreateToolhelp32Snapshot.restype = handle_type
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = handle_type
    kernel.CloseHandle.argtypes = [handle_type]
    kernel.QueryFullProcessImageNameW.argtypes = [handle_type, wintypes.DWORD,
                                                wintypes.LPWSTR, ctypes.POINTER(wintypes.DWORD)]
    kernel.GetProcessTimes.argtypes = [handle_type] + [ctypes.POINTER(wintypes.FILETIME)] * 4

    class Entry(ctypes.Structure):
        _fields_ = [('dwSize', wintypes.DWORD), ('cntUsage', wintypes.DWORD),
                    ('th32ProcessID', wintypes.DWORD), ('th32DefaultHeapID', ctypes.c_size_t),
                    ('th32ModuleID', wintypes.DWORD), ('cntThreads', wintypes.DWORD),
                    ('th32ParentProcessID', wintypes.DWORD), ('pcPriClassBase', wintypes.LONG),
                    ('dwFlags', wintypes.DWORD), ('szExeFile', wintypes.WCHAR * 260)]

    kernel.Process32FirstW.argtypes = [handle_type, ctypes.POINTER(Entry)]
    kernel.Process32NextW.argtypes = [handle_type, ctypes.POINTER(Entry)]
    snapshot = kernel.CreateToolhelp32Snapshot(2, 0)
    if snapshot == ctypes.c_void_p(-1).value:
        raise OSError(ctypes.get_last_error(), 'Process snapshot failed')
    found = []
    try:
        entry = Entry()
        entry.dwSize = ctypes.sizeof(entry)
        present = kernel.Process32FirstW(snapshot, ctypes.byref(entry))
        names = {Path(path).name.lower() for path in expected}
        while present:
            if candidate_process_name(entry.szExeFile, names):
                handle = kernel.OpenProcess(0x1000, False, entry.th32ProcessID)
                if handle:
                    try:
                        image = ctypes.create_unicode_buffer(32768)
                        length = wintypes.DWORD(len(image))
                        times = [wintypes.FILETIME() for _ in range(4)]
                        if (kernel.QueryFullProcessImageNameW(handle, 0, image, ctypes.byref(length))
                                and kernel.GetProcessTimes(handle, *[ctypes.byref(t) for t in times])):
                            binary = pinned_image(image.value, expected)
                            if binary is not None:
                                creation = (times[0].dwHighDateTime << 32) | times[0].dwLowDateTime
                                found.append({'pid': int(entry.th32ProcessID),
                                              'parent_pid': int(entry.th32ParentProcessID),
                                              'start_filetime': creation,
                                              'start_us': creation // 10 - FILETIME_EPOCH_US,
                                              'observed_us': time.time_ns() // 1000,
                                              'binary': binary})
                    finally:
                        kernel.CloseHandle(handle)
            present = kernel.Process32NextW(snapshot, ctypes.byref(entry))
    finally:
        kernel.CloseHandle(snapshot)
    return found


class RouteMonitor:
    def __init__(self, root, binaries):
        self.root = root
        self.expected = {os.path.normcase(str(path.resolve())): path.name for path in binaries}
        self.stop_event = threading.Event()
        self.processes = {}
        self.daemons = set()
        self.errors = set()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        while not self.stop_event.is_set():
            try:
                identities = windows_processes(self.expected)
                for identity in identities:
                    key = (identity['pid'], identity['start_filetime'])
                    self.processes[key] = identity
                # Whitelist fields in memory. Never export the private record.
                state = json.loads((self.root / 'run/daemon.state.json').read_text(encoding='utf-8'))
                if state.get('phase') == 'ready':
                    for identity in identities:
                        if identity['pid'] == state.get('pid') and identity['binary'] == 'kache-engine.exe':
                            self.daemons.add((identity['pid'], identity['start_filetime']))
            except (FileNotFoundError, json.JSONDecodeError):
                pass  # Normal during the cold daemon's startup.
            except Exception as error:
                self.errors.add(type(error).__name__)
            self.stop_event.wait(0.05)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop_event.set()
        self.thread.join(timeout=5)

    def receipt(self):
        return {'processes': list(self.processes.values()),
                'daemon_identities': [{'pid': pid, 'start_filetime': start} for pid, start in sorted(self.daemons)],
                'monitor_errors': sorted(self.errors), 'poll_interval_ms': 50}


def classify_trace(document, receipt):
    metadata = document.get('kache_trace', {})
    if metadata != {'version': 1, 'clock': 'monotonic', 'scope': 'wrapper-thread'}:
        return {'route': 'unknown', 'reason': 'unsupported or worker trace'}
    events = document.get('traceEvents', [])
    if not events or not isinstance(events[0], dict):
        return {'route': 'unknown', 'reason': 'missing root event'}
    root = events[0]
    result = {'unit': root.get('name'), 'pid': root.get('pid'),
              'start_us': root.get('ts'), 'duration_us': root.get('dur')}
    pipeline = [event for event in events[1:] if event.get('name') in PIPELINE_MARKERS
                and event.get('ph') == 'X' and event.get('cat') == 'rustc']
    result['pipeline_markers'] = sorted({event['name'] for event in pipeline})
    if not pipeline:
        return {**result, 'route': 'unknown', 'reason': 'proxy or no recorded pipeline work'}
    if (root.get('cat') != 'rustc' or root.get('ph') != 'X'
            or root.get('args', {}).get('dropped_events') != 0
            or receipt.get('monitor_errors')
            or any(event.get('pid') != root.get('pid') for event in events)
            or not isinstance(root.get('ts'), (int, float))):
        return {**result, 'route': 'unknown', 'reason': 'dropped, mixed or unsupported trace'}
    identities = [item for item in receipt.get('processes', []) if item['pid'] == root['pid']
                  and item['start_us'] <= root['ts'] <= item['observed_us']]
    if len(identities) != 1:
        return {**result, 'route': 'unknown', 'reason': 'no unique live binary/start identity'}
    identity = identities[0]
    daemon_ids = {(item['pid'], item['start_filetime']) for item in receipt.get('daemon_identities', [])}
    key = (identity['pid'], identity['start_filetime'])
    if len(daemon_ids) != 1:
        return {**result, 'route': 'unknown', 'reason': 'private daemon identity absent or changed'}
    if key in daemon_ids:
        route = 'daemon_pipeline'
    elif identity['binary'] == 'kache.exe':
        route = 'client_pipeline'
    elif identity['binary'] == 'kache-engine.exe':
        parents = [item for item in receipt.get('processes', [])
                   if item['pid'] == identity.get('parent_pid') and item['binary'] == 'kache.exe'
                   and item['start_us'] <= identity['start_us'] <= root['ts'] <= item['observed_us']]
        if len(parents) != 1:
            return {**result, 'route': 'unknown', 'reason': 'engine without positive live client-parent identity'}
        route = 'local_engine_pipeline'
    else:
        return {**result, 'route': 'unknown', 'reason': 'unexpected pipeline binary'}
    return {**result, 'route': route, 'start_filetime': identity['start_filetime'],
            'fallback_reason': 'not observed; placement does not identify why delegation was declined'}


def route_receipt(root, evidence, monitor):
    receipt = monitor.receipt()
    records = []
    for path in sorted((root / 'phase-traces').glob('*.trace.json')):
        try:
            document = json.loads(path.read_text(encoding='utf-8'))
            # Warmup worker traces are retained separately, never compiler placement.
            if document.get('kache_trace', {}).get('scope') == 'worker-thread':
                continue
            records.append({'trace': path.name, **classify_trace(document, receipt)})
        except (OSError, ValueError, TypeError, KeyError) as error:
            records.append({'trace': path.name, 'route': 'unknown', 'reason': type(error).__name__})
    receipt.update(traces=records, counts=dict(Counter(record['route'] for record in records)),
                   ownership_scope='Pipeline process only; Windows native compiler-child ownership is not observed.')
    write_json(evidence / 'route.json', receipt)
    return receipt


def copy_evidence(root, target, evidence):
    # Explicit allowlist: no private coordination/auth records, cache DB or blobs.
    for relative in ('config.toml', 'kache.log', 'run/events.jsonl', 'run/summaries.jsonl'):
        source = root / relative
        if source.is_file():
            destination = evidence / relative.replace('/', '-')
            shutil.copy2(source, destination)
    for source, name in ((root / 'phase-traces', 'phase-traces'),
                         (target / 'cargo-timings', 'cargo-timings')):
        if source.is_dir():
            shutil.copytree(source, evidence / name, dirs_exist_ok=True)


def stop_private(kache, root, env, evidence):
    # This command addresses only the case's own private runtime.
    try:
        result = subprocess.run([str(kache), 'daemon', 'stop'], cwd=root, env=env,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, check=False)
        write_json(evidence / 'stop.json', {'exit_code': result.returncode})
        if result.returncode:
            raise RuntimeError('Private daemon did not stop; preserve scratch and abort')
    except subprocess.TimeoutExpired:
        write_json(evidence / 'stop.json', {'timed_out': True})
        raise RuntimeError('Private daemon stop timed out; preserve scratch and abort')


def export_stats(kache, workload, env, evidence, budget):
    for name, command in (
            ('report', ['report', '--format', 'json']),
            ('stats', ['stats', '--format', 'json']),
            ('timeline', ['stats', '--format', 'trace'])):
        capture([kache, *command], workload, env, evidence / name, budget, timeout=60)


def require_timed_route(receipt, evidence):
    relevant = [record for record in receipt['traces'] if record.get('unit') == TIMED_UNIT]
    compilers = [record for record in relevant if 'compiler' in record.get('pipeline_markers', [])]
    # Use the original raw report schema, not the stats machine-envelope schema.
    report = json.loads((evidence / 'report.stdout').read_text(encoding='utf-8'))
    reported = [event for event in report['all_events']
                if event.get('crate_name') == TIMED_UNIT and event.get('compiler_runs', 0) > 0]
    identities = {(record.get('pid'), record.get('start_us'), record.get('duration_us')) for record in compilers}
    passed = (len(reported) == EXPECTED_TIMED_COMPILATIONS
              and all(event['compiler_runs'] == 1 for event in reported)
              and len(compilers) == len(identities) == EXPECTED_TIMED_COMPILATIONS
              and all(record['route'] in KNOWN_ROUTES for record in compilers))
    write_json(evidence / 'timed-route-gate.json', {
        'unit': TIMED_UNIT, 'passed': passed, 'compiler_traces': compilers,
        'expected_compiler_invocations': EXPECTED_TIMED_COMPILATIONS,
        'reported_compiler_invocations': len(reported),
        'other_unit_traces': len(relevant) - len(compilers),
        'rule': 'Both reported compiler invocations require distinct, positively placed real compiler traces; proxy roots do not qualify.'})
    if not passed:
        raise RuntimeError('Timed RISC-V unit has absent or ambiguous pipeline placement; preserve evidence and stop')


def workload_environment(base, target, kache=None):
    env = base.copy()
    env.update(RUSTFLAGS=FLAGS, RUSTUP_TOOLCHAIN=TOOLCHAIN, CARGO_INCREMENTAL='0',
               CARGO_BUILD_WARNINGS='deny', CARGO_TERM_COLOR='always', RUST_BACKTRACE='full',
               CARGO_TARGET_DIR=str(target), RUSTC_WORKSPACE_WRAPPER='',
               RUSTC_WRAPPER=str(kache) if kache is not None else '')
    # Match cc2891aba: its allowlist filtered the workflow's setup-only jobs=2.
    env.pop('CARGO_BUILD_JOBS', None)
    return env


def native_preflight(kache, scratch, output, budget, binaries):
    root = scratch / 'owned-preflight'
    env = private_config(root)
    evidence = output / 'owned-preflight'
    evidence.mkdir()
    target = root / 'target'
    target.mkdir()
    try:
        # The owned probe gets its own store, runtime, target and trace directory.
        capture(['rustc', '+' + TOOLCHAIN, '--version', '--verbose'], root, env,
                evidence / 'rustc-identity', budget)
        source = root / 'route.rs'
        source.write_text('\n'.join(f'pub fn owned_{n}(x:u64)->u64 {{ x.wrapping_mul({n + 1}) }}'
                                    for n in range(1500)), encoding='utf-8')
        rustc, _ = capture(['rustup', 'which', '--toolchain', TOOLCHAIN, 'rustc'], root,
                           env, evidence / 'rustc-path', budget)
        with RouteMonitor(root, binaries) as monitor:
            capture([kache, rustc.decode().strip(), '--crate-name', 'owned_route_probe',
                     '--crate-type', 'rlib', '--edition=2024', str(source),
                     '-o', str(target / 'owned_route_probe.rlib')], root, env,
                    evidence / 'compile', budget, timeout=120)
            time.sleep(0.15)
        if not (target / 'owned_route_probe.rlib').is_file() or not (target / 'owned_route_probe.rlib').stat().st_size:
            raise RuntimeError('Owned Rust preflight produced no rlib')
        route = route_receipt(root, evidence, monitor)
        if not any(route['counts'].get(name) for name in KNOWN_ROUTES):
            raise RuntimeError('Route preflight is ambiguous; do not spend workload budget')
        clang = shutil.which('clang-cl.exe')
        if not clang:
            candidate = Path(os.environ['ProgramFiles']) / 'LLVM/bin/clang-cl.exe'
            clang = str(candidate) if candidate.is_file() else None
        if not clang:
            raise RuntimeError('Native clang-cl required')
        c_source = root / 'owned.c'
        c_source.write_text('int owned_answer(void) { return 42; }\n', encoding='utf-8')
        capture([clang, '--version'], root, env, evidence / 'clang-identity', budget)
        bare, _ = capture([clang, '/nologo', '--target=' + HOST, '-E', c_source], root,
                          env, evidence / 'bare-preprocess', budget)
        wrapped, _ = capture([kache, clang, '/nologo', '--target=' + HOST, '-E', c_source],
                             root, env, evidence / 'wrapped-preprocess', budget)
        if bare != wrapped:
            raise RuntimeError('Preprocessing output changed through the wrapper')
        obj = target / 'owned.obj'
        capture([kache, clang, '/nologo', '--target=' + HOST, '-c', c_source, '/Fo' + str(obj)],
                root, env, evidence / 'native-coff', budget)
        if obj.read_bytes()[:2] != b'\x64\xaa':
            raise RuntimeError('Owned native probe is not ARM64 COFF')
        write_json(evidence / 'native-output.json', {'coff_machine': 'aa64', 'object_sha256': digest(obj),
                                                    'rlib_sha256': digest(target / 'owned_route_probe.rlib')})
    finally:
        try:
            stop_private(kache, root, env, evidence)
        finally:
            copy_evidence(root, target, evidence)
    shutil.rmtree(root)


def run_case(name, arm, kache, workload, scratch, output, budget, binaries):
    root = scratch / name
    evidence = output / name
    evidence.mkdir()
    if shutil.disk_usage(scratch).free < 8 * (1 << 30):
        raise RuntimeError('Less than 8 GiB free before a fresh LTO case; abort without retry')
    target = root / 'target'
    wrapped = arm in ('default', 'disabled')
    if wrapped:
        env = private_config(root, scheduler=arm != 'disabled')
    else:
        root.mkdir(parents=True, exist_ok=False)
        env = environment()
    env = workload_environment(env, target, kache if wrapped else None)
    command = ['cargo', '+' + TOOLCHAIN, '-Zgitoxide', '-Zgit', 'build', '--locked',
               '--release', '--all-targets', '--features', 'no-panic', '--package',
               'ab-riscv-interpreter', '--timings']
    write_json(evidence / 'provenance.json', {'arm': arm, 'cold_store': wrapped, 'fresh_target': True,
               'source': SOURCE, 'workload': WORKLOAD, 'flags': FLAGS, 'toolchain': TOOLCHAIN,
               'wrappers': {'rustc': env['RUSTC_WRAPPER'], 'workspace': env['RUSTC_WORKSPACE_WRAPPER']},
               'jobs_policy': {'mode': 'Cargo default/config resolution, matching cc2891aba',
                               'cli_override': None, 'cargo_build_jobs_env': None,
                               'setup_jobs_2_not_inherited': True, 'numeric_result': 'not asserted'},
               'free_bytes_before': shutil.disk_usage(scratch).free,
               'instrumentation': '50ms identity polling and phase tracing' if wrapped else 'none'})
    result = None
    monitor = None
    try:
        if wrapped:
            with RouteMonitor(root, binaries) as monitor:
                _, result = capture(command, workload, env, evidence / 'cargo', budget, timeout=1200)
                time.sleep(0.15)
            receipt = route_receipt(root, evidence, monitor)
            # Collection follows the timed command. Bare never runs these commands.
            export_stats(kache, workload, env, evidence, budget)
            require_timed_route(receipt, evidence)
        else:
            _, result = capture(command, workload, env, evidence / 'cargo', budget, timeout=1200)
    finally:
        if monitor is not None and not (evidence / 'route.json').exists():
            route_receipt(root, evidence, monitor)
        try:
            if wrapped:
                stop_private(kache, root, env, evidence)
        finally:
            copy_evidence(root, target, evidence)
    # Export measurements and provenance before deleting only this owned case.
    shutil.rmtree(root)
    return {'case': name, 'arm': arm, 'elapsed_ms': result['elapsed_ms'],
            'free_bytes_after_cleanup': shutil.disk_usage(scratch).free}


def main():
    parser = argparse.ArgumentParser()
    for name in ('kache', 'workload', 'output', 'scratch'):
        parser.add_argument('--' + name, required=True, type=Path)
    parser.add_argument('--started', required=True, type=int)
    parser.add_argument('--helper-source', required=True)
    args = parser.parse_args()
    if os.name != 'nt' or os.environ.get('PROCESSOR_ARCHITECTURE') != 'ARM64':
        parser.error('Native Windows ARM64 required')
    output, scratch = args.output.resolve(), args.scratch.resolve()
    output.mkdir(parents=True, exist_ok=False)
    scratch.mkdir(parents=True, exist_ok=False)
    budget = Budget(args.started)
    kache = args.kache.resolve()
    binaries = (kache, kache.with_name('kache-engine.exe'))
    write_json(output / 'experiment.json', {'source': SOURCE, 'workload': WORKLOAD,
               'helper_source': args.helper_source, 'host': HOST, 'rounds': ROUNDS,
               'binaries': {path.name: digest(path) for path in binaries},
               'deadline_epoch': budget.deadline, 'job_timeout_minutes': 90,
               'interpretation': 'Three diagnostic rounds; phase/monitor overhead is present in both Kache arms. This does not prove default timing parity.'})
    records = []
    try:
        initialize_sdk(HOST, output)
        native_preflight(kache, scratch, output, budget, binaries)
        # Prior max is 356.927 seconds; ten builds plus three minutes collection.
        forecast = 10 * 356.927 + 180
        if budget.deadline - time.time() < forecast:
            raise TimeoutError('Insufficient global budget for warmup plus nine builds; no workload started')
        records.append(run_case('common-warmup', 'bare', kache, args.workload.resolve(),
                                scratch, output, budget, binaries))
        for round_number, arms in enumerate(ROUNDS, 1):
            for position, arm in enumerate(arms, 1):
                records.append(run_case(f'round-{round_number}-position-{position}-{arm}', arm,
                                        kache, args.workload.resolve(), scratch, output, budget, binaries))
                write_json(output / 'measurements.json', records)
        write_json(output / 'result.json', {'complete': True, 'timed_cases': 9,
                   'acceptance': 'diagnostic only; issue 1792 remains open'})
    except Exception as error:
        write_json(output / 'measurements.json', records)
        write_json(output / 'result.json', {'complete': False, 'error': str(error),
                   'error_type': type(error).__name__, 'acceptance': 'none; preserve partial evidence'})
        raise


if __name__ == '__main__':
    main()
