"""Compare fixed Kache revisions with alternating warm trials on one runner."""
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import time

BASE = '54fa65c27089a9754cd9aac8bf8439a456952319'
HEAD = '34184fac4ecd65f4a532b04c32abb4457f658063'
repo = Path.cwd()
root = Path(os.environ['RUNNER_TEMP']) / 'store-perf-diagnostics'
root.mkdir()
results = root / 'results'
results.mkdir()
env = dict(os.environ, RUSTC_WRAPPER='', RUSTUP_TOOLCHAIN='')
env.pop('KACHE_VERSION', None)


def command(args, log):
    print('Running:', ' '.join(map(str, args)), flush=True)
    with log.open('w') as output:
        subprocess.run(list(map(str, args)), cwd=repo, env=env,
                       stdout=output, stderr=subprocess.STDOUT, check=True)


def host():
    data = {'wall_time': time.time(), 'load': os.getloadavg(),
            'cpu_affinity_count': len(os.sched_getaffinity(0)),
            'free_disk': shutil.disk_usage(repo).free}
    for name in ('cpu.stat', 'cpu.max', 'memory.current', 'memory.events',
                 'cpu.pressure', 'io.pressure'):
        path = Path('/sys/fs/cgroup') / name
        if path.exists():
            data[name] = path.read_text()
    return data


assert shutil.disk_usage(repo).free >= 40 * 1024**3, 'Need 40 GiB free'
(results / 'identity.json').write_text(json.dumps({
    'base': BASE, 'head': HEAD, 'platform': platform.platform(),
    'host': host(), 'repetitions_per_side': 4,
}, indent=2))
shutil.copytree(repo / 'scenarios/bench-pr-cargo', root / 'scenarios/bench-pr-cargo')
command(['cargo', 'build', '--release', '--locked', '-p', 'kache-e2e',
         '--bin', 'kache-scenario'], results / 'build-instrument.log')
engine = root / 'kache-scenario'
shutil.copy2(repo / 'target/release/kache-scenario', engine)

for side, revision in (('base', BASE), ('head', HEAD)):
    command(['git', 'switch', '--detach', revision], results / f'checkout-{side}.log')
    command(['cargo', 'build', '--release', '--locked', '-p', 'kache'],
            results / f'build-{side}.log')
    binary = root / f'kache-{side}'
    shutil.copy2(repo / 'target/release/kache', binary)
    (results / f'binary-{side}.json').write_text(json.dumps({
        'revision': revision, 'bytes': binary.stat().st_size,
        'sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
    }))

samples = []


def sample(side, repetition, retry):
    label = f'{repetition}-{side}'
    work = repo / 'tmp/store-perf-diagnostics' / side
    before = host()
    args = [engine, '--kache', root / f'kache-{side}', '--scenarios', root / 'scenarios',
            '--select', 'suite:bench', '--select', 'backend:kache',
            '--profile', 'pr-cargo', '--warm-same-tree', '--work-dir', work]
    if retry:
        args.append('--retry')
    command(args, results / f'{label}.log')
    after = host()
    phase_result = json.loads((work / 'bench-pr-cargo.json').read_text())
    assert phase_result['warm_same_tree_verdict']['ok'], phase_result['warm_same_tree_verdict']
    assert phase_result['verdict']['ok'], phase_result['verdict']
    record = {'side': side, 'repetition': repetition, 'retry': retry,
              'before': before, 'after': after}
    for phase, report_name in (('warm_same_tree', 'warm-same-tree'), ('warm', 'warm')):
        raw_path = work / f'report-{report_name}.json'
        raw = json.loads(raw_path.read_text())
        shutil.copy2(raw_path, results / f'{label}-report-{report_name}.json')
        record[phase] = dict(phase_result[phase], timing=raw['timing'])
    shutil.copy2(work / 'bench-pr-cargo.json', results / f'{label}-bench.json')
    samples.append(record)
    (results / 'samples.json').write_text(json.dumps(samples, indent=2))
    print(label, {p: record[p]['wall_ms'] for p in ('warm_same_tree', 'warm')}, flush=True)


# Populate each side once. Keep those observations separately from warm trials.
sample('base', 'initial', False)
sample('head', 'initial', False)
for repetition, order in enumerate((('base', 'head'), ('head', 'base'),
                                    ('base', 'head'), ('head', 'base')), 1):
    for side in order:
        sample(side, repetition, True)

summary = {}
for phase in ('warm_same_tree', 'warm'):
    summary[phase] = {}
    for side in ('base', 'head'):
        rows = [s[phase] for s in samples if s['retry'] and s['side'] == side]
        times = [r['wall_ms'] for r in rows]
        summary[phase][side] = {
            'wall_ms': times, 'median_ms': statistics.median(times),
            'min_ms': min(times), 'max_ms': max(times),
            'timing_medians': {key: statistics.median(r['timing'][key] for r in rows)
                               for key in rows[0]['timing'] if key.startswith('total_')},
        }
    summary[phase]['median_delta_pct'] = 100 * (
        summary[phase]['head']['median_ms'] / summary[phase]['base']['median_ms'] - 1)
(results / 'summary.json').write_text(json.dumps(summary, indent=2))
print(json.dumps(summary, indent=2), flush=True)
