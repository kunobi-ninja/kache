"""Run the official registry crate and two COFF controls under native Windows MSVC."""
import collections
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import urllib.request
import zipfile

ROOT = Path(tempfile.mkdtemp(prefix='kr-', dir=Path(os.environ['RUNNER_TEMP']).anchor))
OUT = Path(os.environ['RUNNER_TEMP']) / 'ring-evidence'
OUT.mkdir()
HOME = ROOT / 'cargo'
BASE = {**os.environ, 'CARGO_HOME': str(HOME), 'RUSTUP_TOOLCHAIN': '1.100.0-x86_64-pc-windows-msvc',
        'CARGO_INCREMENTAL': '0', 'CARGO_PROFILE_DEV_DEBUG':'0', 'KACHE_LOCAL_ONLY':'1'}
OIDS = {'2.2.0':'83c460ddd9e4baa4aca20c9f8e0929dccb557605dc1c5d1d6fbb1e8465ae20b7',
        '2.3.1':'daf0f08e3b624e77c8e85a92261d7be6f382596c43dc065e8aafc2fe6f1aedcb'}


def run(argv, cwd, env, log=None):
    p = subprocess.run([str(a) for a in argv], cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if log: (OUT / log).write_bytes(p.stdout)
    if p.returncode: raise RuntimeError(f'{argv[0]} exited {p.returncode}: {p.stdout[-5000:]!r}')
    return p.stdout.decode(errors='replace')


def sha(b): return hashlib.sha256(b).hexdigest()


def inspect(file, manifest):
    blob = file.read_bytes(); assert blob[:8] == b'!<arch>\n'
    spellings = sorted({s for p in (str(manifest), manifest.as_posix()) for s in (p.encode(), p.encode('utf-16-le'))})
    categories = collections.Counter(); names = []; offset = 8; table = b''
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
            if b'pregenerated' in name.lower(): names.append(name.decode(errors='replace'))
        offset += 60+n+(n%2)
    assert offset==len(blob)
    return {'file':file.name,'bytes':len(blob),'sha256':sha(blob),'manifest':str(manifest),
            'matches_by_ar_field':dict(categories),'absolute_pregenerated_member_names':names[:20]}


def project(parent, name):
    path=parent/name; (path/'src').mkdir(parents=True)
    (path/'Cargo.toml').write_text(f'[package]\nname="{name}"\nversion="0.1.0"\nedition="2021"\n')
    (path/'src/lib.rs').write_text('pub fn answer() -> u8 { 42 }\n'); return path


def fixture(name, payload):
    p=project(ROOT,name); pre=p/'pregenerated';pre.mkdir(); obj=pre/'a-long-absolute-native-member-name.obj'
    src=pre/'input.c'; text=json.dumps(str(p)) if payload else '"test"';src.write_text(f'const char fixture_data[] = {text};\n')
    run(['cl','/nologo','/c','/Od','/Fo'+obj.name,src.name],pre,BASE)
    data=obj.read_bytes(); assert (str(p).encode() in data)==payload
    def entry(n,b):
        h=f'{n:<16}{0:<12}{0:<6}{0:<6}{644:<8}{len(b):<10}`\n'.encode(); assert len(h)==60
        return h+b+(b'\n' if len(b)%2 else b'')
    archive=(b'!<arch>\n'+entry('member.obj/',data)) if payload else (b'!<arch>\n'+entry('//',str(obj).encode()+b'\0')+entry('/0',data))
    f=pre/'opaque.lib';f.write_bytes(archive)
    (p/'build.rs').write_text('fn main() { println!("cargo:rerun-if-changed=pregenerated/opaque.lib"); println!("cargo:rustc-link-search=native={}/pregenerated",env!("CARGO_MANIFEST_DIR")); println!("cargo:rustc-link-lib=static=opaque"); }')
    return p,inspect(f,p)


def measure(label,binary,work,crate,manifest):
    base=ROOT/(label+'-'+crate);base.mkdir();cache=base/'cache';config=base/'config.toml';config.write_text('[cache]\nlocal_only=true\n');
    env={**BASE,'RUSTC_WRAPPER':str(binary),'KACHE_CACHE_DIR':str(cache),'KACHE_RUNTIME_DIR':str(base/'ipc'),
         'KACHE_CONFIG':str(config),'KACHE_HOST_CONFIG':str(config),'KACHE_BASE_DIR':str(work)}
    run([binary,'daemon','start'],work,env,label+'-'+crate+'-start.log');rows=[]
    try:
        for phase,targetname in [('cold','a'),('same','a'),('new','b')]:
            target=base/targetname
            if target.exists():shutil.rmtree(target)
            before={f:f.stat().st_size for f in base.rglob('events.jsonl')}
            run(['cargo','build','--locked','--offline','--lib','--target-dir',target],work,env,f'{label}-{crate}-{phase}.log')
            artifacts=list((target/'debug/deps').glob('lib'+crate+'-*.rlib'));assert len(artifacts)==1,artifacts
            analysis=inspect(artifacts[0],manifest); events=[]
            for f in base.rglob('events.jsonl'):
                with f.open('rb') as stream:
                    stream.seek(before.get(f,0));events.extend(json.loads(line) for line in stream if line.strip())
            filtered=[{k:e.get(k) for k in ('result','cache_key','compiler_runs','skip_reason','store_error','passthrough_reason')} for e in events if e.get('crate_name')==crate]
            assert filtered,(crate,len(events));row={'phase':phase,'events':filtered,'archive':analysis};rows.append(row)
            print(label,crate,phase,json.dumps(filtered),analysis['matches_by_ar_field'],flush=True)
            (OUT/f'{label}-{crate}.json').write_text(json.dumps(rows,indent=2))
    finally: run([binary,'daemon','stop'],work,env,label+'-'+crate+'-stop.log')


ring=project(ROOT,'consumer');(ring/'Cargo.toml').open('a').write('\n[dependencies]\nring="=0.17.14"\n');run(['cargo','generate-lockfile'],ring,BASE);run(['cargo','fetch','--locked'],ring,BASE)
metadata=json.loads(run(['cargo','metadata','--locked','--offline','--format-version','1'],ring,BASE)); package=next(Path(q['manifest_path']).parent for q in metadata['packages'] if q['name']=='ring' and q['version']=='0.17.14');shutil.copyfile(ring/'Cargo.lock',OUT/'Cargo.lock')
binaries={}
for version in [*OIDS,'main']:
    if version=='main':binary=Path(os.environ['GITHUB_WORKSPACE'])/'target/debug/kache.exe'
    else:
        arm=ROOT/version;arm.mkdir();a=arm/'bin.zip';urllib.request.urlretrieve(f'https://github.com/kunobi-ninja/kache/releases/download/v{version}/kache-x86_64-pc-windows-msvc.zip',a);assert sha(a.read_bytes())==OIDS[version];zipfile.ZipFile(a).extractall(arm);binary=next(arm.rglob('kache.exe'))
    print('BINARY',version,run([binary,'--version'],ring,BASE).strip(),sha(binary.read_bytes()),flush=True)
    measure(version,binary,ring,'ring',package)
    binaries[version]=binary
work={}; inputs={}
for name,content in [('names_only',False),('payload_only',True)]:
    work[name],inputs[name]=fixture(name,content);run(['cargo','generate-lockfile','--offline'],work[name],BASE)
(OUT/'archive-inputs.json').write_text(json.dumps(inputs,indent=2))
for version,binary in binaries.items():
    for name,p in work.items():measure(version,binary,p,name,p)
