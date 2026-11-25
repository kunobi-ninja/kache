"""Run the official registry crate and two COFF controls under native Windows MSVC."""
import collections
import hashlib
import platform
import time
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(tempfile.mkdtemp(prefix='kr-', dir=Path(os.environ['RUNNER_TEMP']).anchor))
OUT = Path(os.environ['RUNNER_TEMP']) / 'ring-evidence'
OUT.mkdir()
HOME = ROOT / 'cargo'
BASE = {**os.environ, 'CARGO_HOME': str(HOME), 'RUSTUP_TOOLCHAIN': '1.100.0-x86_64-pc-windows-msvc',
        'CARGO_INCREMENTAL': '0', 'CARGO_PROFILE_DEV_DEBUG':'0', 'KACHE_LOCAL_ONLY':'1'}


def run(argv, cwd, env, log=None):
    p = subprocess.run([str(a) for a in argv], cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if log: (OUT / log).write_bytes(p.stdout)
    if p.returncode: raise RuntimeError(f'{argv[0]} exited {p.returncode}: {p.stdout[-5000:]!r}')
    return p.stdout.decode(errors='replace')


def sha(b): return hashlib.sha256(b).hexdigest()


def inspect(file, manifest):
    blob = file.read_bytes(); assert blob[:8] == b'!<arch>\n'
    spellings = sorted({s for p in (str(manifest), manifest.as_posix()) for s in (p.encode(), p.encode('utf-16-le'))})
    categories = collections.Counter(); names = []; objects = []; offset = 8; table = b''
    def count(category, b): categories[category] += sum(b.lower().count(p.lower()) for p in spellings)
    while offset < len(blob):
        h = blob[offset:offset+60]; assert h[58:] == b'`\n', (file,offset)
        n = int(h[48:58]); name = h[:16].rstrip(); data = blob[offset+60:offset+60+n]; assert len(data)==n
        if name == b'//': table = data; count('long_name_table', data)
        elif name in (b'/',b'/SYM64/'): count('symbol_table',data)
        else:
            if name.startswith(b'#1/'):
                size=int(name[3:]); name,data = data[:size].rstrip(b'\0'),data[size:]; count('inline_name',name)
            elif name.startswith(b'/') and name[1:].isdigit():
                field=table[int(name[1:]):]; name=field.split(b'\0',1)[0].split(b'/\n',1)[0]
            else: count('header_name',name)
            count('member_payload',data)
            objects.append([name.decode(errors='replace'), sha(data)])
            if b'pregenerated' in name.lower(): names.append(name.decode(errors='replace'))
        offset += 60+n+(n%2)
    assert offset==len(blob)
    return {'file':file.name,'bytes':len(blob),'sha256':sha(blob),'manifest':str(manifest),
            'objects':objects,'matches_by_ar_field':dict(categories),'absolute_pregenerated_member_names':names[:20]}


def project(parent, name):
    path=parent/name; (path/'src').mkdir(parents=True)
    (path/'Cargo.toml').write_text(f'[package]\nname="{name}"\nversion="0.1.0"\nedition="2021"\n')
    (path/'src/lib.rs').write_text('pub fn answer() -> u8 { 42 }\n'); return path


def fixture(name, payload):
    p=project(ROOT,name); pre=p/'pregenerated';pre.mkdir(); obj=pre/'a-long-absolute-native-member-name.obj'
    outside=ROOT/('compile-'+name);outside.mkdir(); src=outside/'input.c'; text=json.dumps(str(p)) if payload else '"test"';src.write_text(f'const char fixture_data[] = {text};\nconst char content_marker[] = "KACHE_TEST_VALUE_A";\n')
    run(['cl','/nologo','/c','/Od','/Fo'+obj.name,src.name],outside,BASE); shutil.copyfile(outside/obj.name,obj)
    data=obj.read_bytes(); assert (str(p).encode() in data)==payload
    def entry(n,b):
        h=f'{n:<16}{0:<12}{0:<6}{0:<6}{644:<8}{len(b):<10}`\n'.encode(); assert len(h)==60
        return h+b+(b'\n' if len(b)%2 else b'')
    archive=(b'!<arch>\n'+entry('member.obj/',data)) if payload else (b'!<arch>\n'+entry('//',str(obj).encode()+b'\0')+entry('/0',data))
    f=pre/'opaque.lib';f.write_bytes(archive)
    (p/'build.rs').write_text('fn main() { println!("cargo:rerun-if-changed=pregenerated/opaque.lib"); println!("cargo:rustc-link-search=native={}/pregenerated",env!("CARGO_MANIFEST_DIR")); println!("cargo:rustc-link-lib=static=opaque"); }')
    return p,inspect(f,p)


def rewrite_archive(path, original, phase):
    """Change only member dates, or one byte of a known data symbol."""
    data = bytearray(original)
    position = 8
    while position < len(data):
        size = int(data[position+48:position+58])
        stamp = 0 if phase in ('cold', 'same') else 86400
        data[position+16:position+28] = f'{stamp:<12}'.encode()
        position += 60 + size + size % 2
    if phase == 'payload':
        assert data.count(b'fixture_data') >= 1
        # The symbol name stays unchanged. Replace its constant data.
        marker = b'KACHE_TEST_VALUE_A\x00'
        assert data.count(marker) == 1, marker
        place = data.index(marker) + len(marker) - 2
        data[place] = (data[place] + 1) % 256
    path.write_bytes(data)


def archived_objects(products, manifest):
    archives = {}
    for item in products:
        if item.get('reason') != 'build-script-executed' or 'ring@' not in item.get('package_id',''):
            continue
        for file in sorted(Path(item['out_dir']).rglob('*.lib')):
            archives[file.name] = inspect(file, manifest)
    return archives


def measure(arm, binary, project_dir, crate, manifest, mode, *, phases=None, group=None, keep_daemon=False, sample=None, reference=False):
    log_name = sample or arm
    base = ROOT / ('cache-' + (group or arm) + '-' + crate)
    base.mkdir(exist_ok=True)
    config = base / 'config.toml'
    config.write_text('[cache]\nlocal_only=true\n')
    env = {**BASE, 'RUSTC_WRAPPER':str(binary) if binary else '', 'KACHE_CACHE_DIR':str(base/'cache'),
           'KACHE_RUNTIME_DIR':str(base/'ipc'), 'KACHE_CONFIG':str(config), 'KACHE_HOST_CONFIG':str(config),
           'KACHE_BASE_DIR':str(project_dir)}
    if mode == 'cfg':
        env['RUSTC_BOOTSTRAP'] = crate
    if crate == 'ring':
        env.pop('CARGO_PROFILE_DEV_DEBUG', None)
    if reference:
        env.update({'RUSTC':str(REFERENCE_RUSTC), 'KACHE_PROBE_REAL_RUSTC': REAL_RUSTC, 'KACHE_PROBE_CRATE':crate,
                    'KACHE_PROBE_RECEIPT':str(OUT / f'{arm}-{crate}-direct.txt')})
    if binary:
        run([binary,'daemon','start'], project_dir, env, f'{log_name}-{crate}-start.log')
    native = project_dir / 'pregenerated' / 'opaque.lib'
    initial = native.read_bytes() if native.exists() else None
    phases = phases or (('cold','same','new') if mode == 'ring' else ('cold','same','date','payload'))
    reports = {}
    try:
        for phase in phases:
            target = ROOT / ('targets-' + crate) / ('b' if phase == 'new' else 'a')
            if target.exists():
                shutil.rmtree(target)
            if initial is not None:
                rewrite_archive(native, initial, phase)
            if mode == 'ring':
                # Force lib.exe timestamps to advance between independent builds.
                time.sleep(1.1)
            event_file = base / 'ipc' / 'events.jsonl'
            before = event_file.stat().st_size if event_file.exists() else 0
            messages = run(['cargo','build','--message-format=json-render-diagnostics','--locked','--offline',
                            '--lib','--target-dir',target], project_dir, env, f'{log_name}-{crate}-{phase}.log')
            products = [json.loads(line) for line in messages.splitlines() if line.startswith('{')]
            files = [Path(path) for m in products if m.get('reason') == 'compiler-artifact'
                     and m['target']['name'] == crate for path in m['filenames'] if path.endswith('.rlib')]
            assert len(files) == 1, files
            reports[phase] = row = {'phase':phase,'rlib_sha256':sha(files[0].read_bytes())}
            if reference:
                receipt = Path(env['KACHE_PROBE_RECEIPT']).read_text()
                assert receipt.startswith('direct-replay-equal '+crate+' '), receipt
                assert '--remap-path-prefix=' in receipt, receipt
                row['same_argv_direct_replay'] = True
            if mode == 'ring':
                row['native_inputs'] = archived_objects(products,manifest)
                assert row['native_inputs']
            else:
                row['input'] = inspect(native, project_dir)
                row['packed_input'] = native.read_bytes() in files[0].read_bytes()
            if binary:
                with event_file.open('rb') as stream:
                    stream.seek(before)
                    events = [json.loads(line) for line in stream if line.strip()]
                lookups = [e for e in events if e.get('crate_name') == crate and e.get('cache_key')]
                assert len(lookups) == 1, [(e['result'],e['cache_key']) for e in lookups]
                row['lookup'] = {k:lookups[0].get(k) for k in ('result','compiler_runs','cache_key','store_error','key_ms',
                                                                              'key_hash_hits','key_hash_misses','key_hash_bytes','lookup_ms')}
            print(arm,crate,phase,row['rlib_sha256'],row.get('lookup'),row.get('packed_input'),flush=True)
            (OUT / f'{sample or arm}-{crate}.json').write_text(json.dumps(reports,indent=2))
    finally:
        if initial is not None:
            native.write_bytes(initial)
        if binary and not keep_daemon:
            run([binary,'daemon','stop'],project_dir,env,f'{log_name}-{crate}-stop.log')
    return reports


def verify(results, modes):
    failures = []
    def check(ok, message):
        if not ok:
            failures.append(message)
    for crate,mode in modes.items():
        plain, baseline, candidate = (results[key,crate] for key in ('rustc','baseline','candidate'))
        reference = results['reference',crate]
        for phase,control in plain.items():
            row = candidate[phase]
            for name,example in [('baseline',baseline[phase]),('candidate',row)]:
                check(example['rlib_sha256'] == reference[phase]['rlib_sha256'], f'{name}/{crate}/{phase}: different .rlib than direct rustc with identical compiler flags')
            check(reference[phase]['same_argv_direct_replay'], f'{crate}/{phase}: no direct compiler receipt')
            event = row['lookup']
            check(not event['store_error'], f'{crate}/{phase}: store error')
            check(event['cache_key'].startswith('checkout-') if mode in ('names','payload','ring') else True,
                  f'{crate}/{phase}: checkout scope lost')
        check(candidate['cold']['lookup']['compiler_runs'] == 1, f'{crate}/cold: expected a compile')
        event = candidate['same']['lookup']
        check((event['result'],event['compiler_runs']) == ('local_hit',0), f'{crate}/same: expected a local hit with no compiler invocation')
        if mode == 'ring':
            for arm_name,arm in [('rustc',plain),('baseline',baseline),('candidate',candidate)]:
                old = arm['cold']['native_inputs']
                same = arm['same']['native_inputs']
                new = arm['new']['native_inputs']
                def members(data):
                    return {k:[digest for _,digest in v['objects']] for k,v in data.items()}
                check(members(old)==members(same), f'{arm_name}/ring: inconsistent same-target input objects')
                check(any(old[k]['sha256'] != same[k]['sha256'] for k in old), f'{arm_name}/ring: no changing lib.exe headers')
                check(members(old)!=members(new), f'{arm_name}/ring: renamed target did not change an object payload')
            for phase in plain:
                for archive in plain[phase]['native_inputs']:
                    raw_members = [v[1] for v in plain[phase]['native_inputs'][archive]['objects']]
                    for arm in (baseline, candidate, reference):
                        compared = [v[1] for v in arm[phase]['native_inputs'][archive]['objects']]
                        check(raw_members == compared, f'{crate}/{phase}: plain cargo produced different COFF object bytes')
            check(baseline['same']['lookup']['compiler_runs'] == 1, 'baseline/ring: expected a recompile')
            check(candidate['new']['lookup']['compiler_runs'] == 1, 'candidate/ring: different COFF payload restored')
        else:
            check(candidate['payload']['lookup']['compiler_runs'] == 1,f'{crate}: changed object restored')
            check(plain['payload']['rlib_sha256'] != plain['date']['rlib_sha256'], f'{crate}: plain rustc insensitive to changed object')
            for phase in plain:
                for arm in (baseline, candidate, reference):
                    check(arm[phase]['input']['sha256'] == plain[phase]['input']['sha256'], f'{crate}/{phase}: mismatched native input')
                check(plain[phase]['packed_input'] == (mode in ('cfg','explicit')), f'{crate}/{phase}: packing assumption wrong')
            if mode in ('cfg','explicit'):
                check(plain['same']['rlib_sha256'] != plain['date']['rlib_sha256'], f'{crate}: plain rustc insensitive to new date inside packed input')
                check(candidate['date']['lookup']['compiler_runs'] == 1, f'{crate}: changed packed date restored')
    if failures:
        raise AssertionError('\n'.join(failures))


ring = project(ROOT,'consumer')
with (ring/'Cargo.toml').open('a') as manifest_file:
    manifest_file.write('\n[dependencies]\nring="=0.17.14"\n')
shutil.copyfile(Path(__file__).with_name('ring-probe.lock'),ring/'Cargo.lock')
run(['cargo','fetch','--locked'],ring,BASE)
packages = json.loads(run(['cargo','metadata','--locked','--offline','--format-version','1'],ring,BASE))['packages']
ring_dir = next(Path(q['manifest_path']).parent for q in packages if q['name']=='ring' and q['version']=='0.17.14')
subjects = {'ring':(ring,ring_dir,'ring')}
for crate,mode in [('names_only','names'),('payload_only','payload'),('cfg_packed','cfg'),('explicit_packed','explicit')]:
    work,archive = fixture(crate,mode=='payload')
    if mode in ('cfg','explicit'):
        # Use the native librarian and both COFF linker indexes here. A plain
        # ar fixture would take the conservative raw-key fallback without
        # reaching the production date-normalization path.
        directory = work/'pregenerated'
        library = directory/'opaque.lib'
        library.unlink()
        obj = directory/'a-long-absolute-native-member-name.obj'
        run(['lib.exe','/nologo','/out:'+str(library),str(obj)], directory, BASE, f'{crate}-lib.log')
        coff = library.read_bytes()
        position = 8
        regions = []
        while position < len(coff):
            regions.append(coff[position:position+16].rstrip())
            member_size = int(coff[position+48:position+58])
            position += 60 + member_size + (member_size % 2)
        assert regions.count(b'/') == 2, regions
    if mode == 'cfg':
        (work/'src/lib.rs').write_text('#![feature(link_cfg)]\n#[link(name="opaque", kind="static", cfg(windows))]\nextern "C" {}\npub fn value() -> u32 { 1 }\n')
    if mode == 'explicit':
        build = work/'build.rs'
        build.write_text(build.read_text().replace('rustc-link-lib=static=opaque','rustc-link-lib=static:+whole-archive=opaque'))
    run(['cargo','generate-lockfile','--offline'],work,BASE)
    subjects[crate] = (work,work,mode)
workspace = Path(os.environ['GITHUB_WORKSPACE'])
REAL_RUSTC = run(['rustup','which','rustc'],ROOT,BASE).strip()
REFERENCE_RUSTC = ROOT/'direct-shim'/'rustc.exe'
REFERENCE_RUSTC.parent.mkdir()
run([REAL_RUSTC,'--edition=2021','-O',Path(__file__).with_name('native-rustc-reference.rs'),'-o',REFERENCE_RUSTC], ROOT, BASE)
binaries = {'rustc':None, **{arm:workspace/arm/'target/release/kache.exe' for arm in ('baseline','candidate')}}
provenance = {arm:{'commit':run(['git','rev-parse','HEAD'],workspace/arm,BASE).strip(),
                   'launcher':sha(binary.read_bytes()),'engine':sha(binary.with_name('kache-engine.exe').read_bytes())}
              for arm,binary in binaries.items() if binary}
provenance['rustc'] = run(['rustc','-Vv'],ROOT,BASE)
provenance['platform'] = platform.platform()
(OUT/'provenance.json').write_text(json.dumps(provenance,indent=2))
assert provenance['baseline']['engine'] != provenance['candidate']['engine']
outputs = {}
execution_errors = []
for crate,(work,manifest,mode) in subjects.items():
    for arm,binary in binaries.items():
        try:
            outputs[arm,crate] = measure(arm,binary,work,crate,manifest,mode)
        except Exception as error:
            execution_errors.append(f'{arm}/{crate}: {error!r}')
            print(execution_errors[-1], flush=True)
    reference_result = {}
    for phase in (('cold','same','new') if mode=='ring' else ('cold','same','date','payload')):
        try:
            reference_result.update(measure(f'reference-{phase}',binaries['candidate'],work,crate,manifest,mode,
                                            phases=(phase,),reference=True))
        except Exception as error:
            execution_errors.append(f'reference/{phase}/{crate}: {error!r}')
            print(execution_errors[-1], flush=True)
    outputs['reference',crate] = reference_result

# Prime both independent stores. Interleave matched measurements on the exact
# same target and wait for timestamp granularity in every build.
pairs = []
for arm in ('baseline','candidate'):
    measure(arm,binaries[arm],ring,'ring',ring_dir,'ring',phases=('cold',),group='perf-'+arm,
            keep_daemon=True,sample='perf-prime-'+arm)
for block in range(8):
    order = ('baseline','candidate') if block % 2 == 0 else ('candidate','baseline')
    pair = {}
    for arm in order:
        row = measure(arm,binaries[arm],ring,'ring',ring_dir,'ring',phases=('same',),
                      group='perf-'+arm,keep_daemon=block<7,sample=f'perf-{block}-{arm}')['same']
        pair[arm] = row['lookup']
        expected = 1 if arm == 'baseline' else 0
        if pair[arm]['compiler_runs'] != expected:
            execution_errors.append(f'perf/{block}/{arm}: expected compiler_runs={expected}')
    pairs.append(pair)
    (OUT/'matched-cost.json').write_text(json.dumps(pairs,indent=2))
print('Matched release key_ms', [(p['baseline']['key_ms'],p['candidate']['key_ms']) for p in pairs],flush=True)
if execution_errors:
    raise RuntimeError('\n'.join(execution_errors))

verify(outputs,{crate:data[2] for crate,data in subjects.items()})
print('Native ring and archive controls passed.',flush=True)
