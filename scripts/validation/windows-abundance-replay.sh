#!/usr/bin/env bash
# Runs only on a fresh GitHub-hosted Windows qualification runner.
set -euo pipefail

root=$(pwd -P)
candidate="$root/candidate"
evidence="$root/evidence"
workload="$root/workload"
mkdir -p "$evidence"
[[ "$CANDIDATE_SHA" =~ ^[0-9a-f]{40}$ ]]
test "$WORKLOAD_SHA" = 287cf9c6d266ce28d644bed050ce9a835f31fd80
test "$(git -C "$workload" rev-parse HEAD)" = "$WORKLOAD_SHA"
test "$(cat "$candidate/source-sha.txt")" = "$CANDIDATE_SHA"

# The workload receives no authentication token or inherited cache routing.
unset GH_TOKEN GITHUB_TOKEN
while IFS= read -r variable; do
  unset "$variable"
done < <(compgen -e | grep -i '^KACHE_' || true)

export PATH="$candidate:$PATH"
if ! command -v clang-cl >/dev/null; then
  vswhere='/c/Program Files (x86)/Microsoft Visual Studio/Installer/vswhere.exe'
  installation=$("$vswhere" -latest -products '*' -property installationPath)
  installation=$(cygpath -u "${installation//$'\r'/}")
  clang=$(find "$installation/VC/Tools/Llvm" -maxdepth 4 -name clang-cl.exe -print -quit)
  test -n "$clang"
  export PATH="$(dirname "$clang"):$PATH"
fi
clang-cl --version > "$evidence/clang-version.txt"

cd "$workload"
toolchain="nightly-2026-08-25-$EXPECTED_ARCH-pc-windows-msvc"
rustup toolchain install "$toolchain" --profile minimal
rustc +"$toolchain" -vV > "$evidence/rustc-version.txt"
host=$(sed -n 's/^host: //p' "$evidence/rustc-version.txt" | tr -d '\r')
test "$host" = "$EXPECTED_ARCH-pc-windows-msvc"
export RUSTUP_TOOLCHAIN="$toolchain"
export CARGO_INCREMENTAL=0 CARGO_TERM_COLOR=never CARGO_BUILD_WARNINGS=deny
export RUSTFLAGS='-C strip=symbols -C linker=rust-lld -Cllvm-args=--inline-threshold=10000 -C embed-bitcode -C lto -Z dylib-lto -C target-cpu=native'
unset RUSTC_BOOTSTRAP RUSTC RUSTDOC CARGO_ENCODED_RUSTFLAGS RUSTC_WORKSPACE_WRAPPER
unset CARGO_BUILD_RUSTC_WRAPPER CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER
unset CARGO_TARGET_X86_64_PC_WINDOWS_MSVC_RUSTFLAGS
unset CARGO_TARGET_AARCH64_PC_WINDOWS_MSVC_RUSTFLAGS
unset RUSTC_WRAPPER CC_KNOWN_WRAPPER_CUSTOM CMAKE_C_COMPILER_LAUNCHER CMAKE_CXX_COMPILER_LAUNCHER
unset CC CXX HOST_CC HOST_CXX TARGET_CC TARGET_CXX
while IFS= read -r variable; do
  unset "$variable"
done < <(compgen -e | grep -E '^(CC|CXX)_' || true)
cargo -Zgitoxide -Zgit fetch --locked > "$evidence/fetch.log" 2>&1

python - "$evidence/provenance.json" "$CANDIDATE_SHA" "$WORKLOAD_SHA" "$host" "$CACHE_EXECUTABLES" "$RUSTFLAGS" <<'PY'
import json, os, platform, sys
path, candidate, workload, host, executables, flags = sys.argv[1:]
with open(path, 'w') as f:
    json.dump({'candidate': candidate, 'workload': workload, 'host': host,
               'cache_executables': executables, 'rustflags': flags,
               'runner': os.environ.get('RUNNER_NAME'), 'image': os.environ.get('ImageVersion'),
               'platform': platform.platform(), 'repetitions': 3,
               'control': 'Original unwrapped control: clears action CC/CXX wrappers; cc may choose cl rather than candidate clang-cl.'}, f, indent=2)
PY

build() {
  local crate="$1" mode="$2" rep="$3"
  local target="$workload/qualification-targets/$rep-$crate-$mode"
  local -a command=(env)
  if [[ "$mode" = baseline ]]; then
    while IFS= read -r variable; do
      command+=(-u "$variable")
    done < <(compgen -e | grep -i '^KACHE_' || true)
  else
    command+=("RUSTC_WRAPPER=$(cygpath -w "$candidate/kache.exe")"
      "KACHE_LOG=kache=debug"
      "CC_${host//-/_}=kache clang-cl"
      "CXX_${host//-/_}=kache clang-cl"
      "CC_KNOWN_WRAPPER_CUSTOM=kache"
      "CMAKE_C_COMPILER_LAUNCHER=$(cygpath -w "$candidate/kache.exe")"
      "CMAKE_CXX_COMPILER_LAUNCHER=$(cygpath -w "$candidate/kache.exe")")
    if [[ "$rep" = trace ]]; then
      command+=("KACHE_PHASE_TRACE_DIR=$(cygpath -w "$evidence/phases-$crate")")
    fi
  fi
  command+=("CARGO_TARGET_DIR=$(cygpath -w "$target")" cargo -Zgitoxide -Zgit
    build --locked --release --all-targets --features no-panic --package "$crate" --timings)
  printf 'Building %s (%s), repetition %s\n' "$crate" "$mode" "$rep"
  python - "$evidence/timings.jsonl" "$evidence/$rep-$crate-$mode.log" "$rep" "$crate" "$mode" "${command[@]}" <<'PY'
import json, subprocess, sys, time
output, log, rep, crate, mode = sys.argv[1:6]
with open(log, 'wb') as f:
    start = time.monotonic_ns()
    result = subprocess.run(sys.argv[6:], stdout=f, stderr=subprocess.STDOUT)
    elapsed = (time.monotonic_ns() - start) / 1e6
with open(output, 'a') as f:
    f.write(json.dumps({'rep': rep, 'crate': crate, 'mode': mode, 'elapsed_ms': elapsed, 'exit': result.returncode}) + '\n')
print(f'{crate} {mode}: {elapsed / 1000:.3f}s, exit={result.returncode}', flush=True)
if result.returncode:
    with open(log, errors='replace') as f:
        print(''.join(f.readlines()[-100:]))
sys.exit(result.returncode)
PY
  mkdir -p "$evidence/timings-$rep-$crate-$mode"
  cp "$target"/cargo-timings/cargo-timing-*.html "$evidence/timings-$rep-$crate-$mode/"
  # Only this invocation's freshly created targets may be removed.
  [[ "$target" = "$workload/qualification-targets/"* ]]
  rm -rf -- "$target"
}

# Warm compiler/linker/source and scanner state without populating Kache.
build ab-blake3 baseline warmup
build ab-riscv-interpreter baseline warmup

for rep in 1 2 3 trace; do
  cache="$root/qualification-cache-$rep"
  runtime="$evidence/runtime-$rep"
  config="$evidence/config-$rep.toml"
  test ! -e "$cache"
  test ! -e "$runtime"
  mkdir -p "$cache" "$runtime"
  owner="$GITHUB_RUN_ID/$GITHUB_RUN_ATTEMPT/$rep"
  printf '%s\n' "$owner" > "$cache/.qualification-owner"
  cat > "$config" <<EOF
[cache]
local_store = '$(cygpath -m "$cache")'
runtime_dir = '$(cygpath -m "$runtime")'
local_only = true
cache_executables = $CACHE_EXECUTABLES
daemon_idle_timeout_secs = 120
EOF
  export KACHE_CONFIG="$(cygpath -w "$config")"
  if [[ "$rep" = trace ]]; then
    export KACHE_PHASE_TRACE_DIR="$(cygpath -w "$evidence/daemon-phases-$rep")"
  fi
  python - "$evidence/startup-$rep.json" "$evidence/startup-$rep.log" kache.exe daemon start <<'PY'
import json, subprocess, sys, time
path, log = sys.argv[1:3]
with open(log, 'wb') as f:
    start = time.monotonic_ns()
    result = subprocess.run(sys.argv[3:], stdout=f, stderr=subprocess.STDOUT)
    elapsed = (time.monotonic_ns() - start) / 1e6
with open(path, 'w') as f:
    json.dump({'elapsed_ms': elapsed, 'exit': result.returncode}, f)
if result.returncode:
    print(open(log, errors='replace').read())
sys.exit(result.returncode)
PY
  unset KACHE_PHASE_TRACE_DIR
  kache.exe --json daemon status > "$evidence/readiness-$rep.json" 2> "$evidence/readiness-$rep.stderr"
  python - "$evidence/readiness-$rep.json" "$(cygpath -w "$runtime")" "$(cygpath -w "$config")" <<'PY'
import json, pathlib, sys
status = json.load(open(sys.argv[1]))
assert status['success'] and status['command'] == 'daemon-status', status
assert status['daemon_running'], status
assert status['daemon_version'] == status['version'], status
assert pathlib.Path(status['socket']).parent.resolve() == pathlib.Path(sys.argv[2]).resolve(), status
assert pathlib.Path(status['daemon_config_path']).resolve() == pathlib.Path(sys.argv[3]).resolve(), status
PY
  # Alternate pair order between crates and repetitions.
  if [[ "$rep" = trace ]]; then
    build ab-blake3 candidate "$rep"
    build ab-riscv-interpreter candidate "$rep"
  elif [[ "$rep" = 2 ]]; then
    build ab-blake3 candidate "$rep"
    build ab-blake3 baseline "$rep"
    build ab-riscv-interpreter baseline "$rep"
    build ab-riscv-interpreter candidate "$rep"
  else
    build ab-blake3 baseline "$rep"
    build ab-blake3 candidate "$rep"
    build ab-riscv-interpreter candidate "$rep"
    build ab-riscv-interpreter baseline "$rep"
  fi
  kache.exe doctor --verify > "$evidence/doctor-$rep.txt" 2>&1
  kache.exe --json stats > "$evidence/stats-$rep.json" 2> "$evidence/stats-$rep.stderr"
  kache.exe --json daemon status > "$evidence/daemon-status-$rep.json" 2> "$evidence/daemon-status-$rep.stderr"
  kache.exe daemon stop > "$evidence/daemon-stop-$rep.txt" 2>&1
  # Cargo has exited and this repetition's daemon has released ownership.
  # Keep evidence/runtime, but do not accumulate four copies of the object store.
  [[ "$cache" = "$root/qualification-cache-$rep" ]]
  test ! -L "$cache"
  test -f "$cache/.qualification-owner"
  test ! -L "$cache/.qualification-owner"
  test "$(cat "$cache/.qualification-owner")" = "$owner"
  rm -rf -- "$cache"
done

python - "$evidence" <<'PY'
import json, pathlib, statistics, sys
p = pathlib.Path(sys.argv[1]); rows = [json.loads(s) for s in (p/'timings.jsonl').read_text().splitlines()]
report = {'complete': True, 'samples': rows, 'paired_summary': {},
          'interpretation': 'Measurement only. Original-workload acceptance requires inspecting compiler identity, key phases and paired timings. Three paired repetitions have no phase tracing; a separate candidate-only repetition records phases.'}
for crate in ['ab-blake3', 'ab-riscv-interpreter']:
    ratios = []
    for rep in ['1', '2', '3']:
        pair = {r['mode']: r for r in rows if r['crate'] == crate and r['rep'] == rep}
        assert set(pair) == {'baseline', 'candidate'}
        assert all(r['exit'] == 0 for r in pair.values())
        ratios.append(pair['candidate']['elapsed_ms'] / pair['baseline']['elapsed_ms'])
    report['paired_summary'][crate] = {'candidate_over_baseline': ratios, 'median_ratio': statistics.median(ratios)}
(p/'summary.json').write_text(json.dumps(report, indent=2))
print(json.dumps(report['paired_summary'], indent=2))
PY
