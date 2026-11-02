"""Run diagnostic and archive controls on a disposable Linux runner."""

from pathlib import Path
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import time


evidence = Path(os.environ["EVIDENCE_DIR"])
wrapper = Path("src/wrapper.rs")
explain = Path("src/explain.rs")
baseline_test = "wrapper::tests::explain_miss_diff_uses_the_current_compilation_unit"
advice_test = "explain::tests::every_cause_says_what_would_help"
controls = [
    (
        "unit-filter", wrapper, "            && e.unit_id == unit_id\n", "",
        baseline_test, "none:entry-evicted",
    ),
    (
        "empty-unit", wrapper, "        || unit_id.is_empty()\n", "",
        baseline_test, "is_empty()",
    ),
    (
        "gnu-advice", explain,
        "GNU ar's `D` mode (`HOST_ARFLAGS=D` for native builds using `cc`)",
        "the old generic archive advice", advice_test,
        "the old generic archive advice",
    ),
]
for name, path, before, after, test, expected in controls:
    original = path.read_text(encoding="utf-8")
    assert original.count(before) == 1, (name, "control target changed")
    command = ["cargo", "test", "--locked", "-p", "kache", "--lib", test, "--", "--exact", "--nocapture"]
    try:
        path.write_text(original.replace(before, after, 1), encoding="utf-8")
        result = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        (evidence / f"{name}-regression.log").write_text(result.stdout)
        assert result.returncode != 0, (name, "regression escaped")
        assert "running 1 test" in result.stdout, (name, "test did not execute")
        assert f"test {test} ... FAILED" in result.stdout, (name, "wrong failure")
        assert expected in result.stdout, (name, "wrong assertion")
        print(f"CAUGHT {name}", flush=True)
    finally:
        path.write_text(original, encoding="utf-8")
    result = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    (evidence / f"{name}-restored.log").write_text(result.stdout)
    assert result.returncode == 0, (name, "restored candidate failed", result.stdout[-4000:])
    assert f"test {test} ... ok" in result.stdout, (name, "restored test did not execute")
    print(f"RESTORED {name}", flush=True)


def dates(archive):
    assert archive.startswith(b"!<arch>\n")
    offset = 8
    result = []
    while offset < len(archive):
        header = archive[offset:offset + 60]
        assert len(header) == 60 and header[58:] == b"`\n"
        date = header[16:28].strip()
        if date:
            result.append(int(date))
        size = int(header[48:58])
        offset += 60 + size + size % 2
    assert offset == len(archive) and result
    return result


archiver = shutil.which("ar")
compiler = shutil.which("cc")
assert archiver and compiler
sysroot = Path(subprocess.check_output(["rustc", "--print", "sysroot"], text=True).strip())
cargo = sysroot / "bin/cargo"
assert cargo.is_file()
version = subprocess.check_output([archiver, "--version"], text=True)
assert "GNU ar" in version
(evidence / "archiver-version.txt").write_text(version)
archive_results = []
with tempfile.TemporaryDirectory(prefix="kache-archive-recipe-", dir=os.environ["RUNNER_TEMP"]) as temporary:
    scratch = Path(temporary)
    for cc_version, modes in [("1.2.11", ["U", "D"]), ("1.9.0", ["D"])]:
        fixture = scratch / cc_version
        fixture.mkdir()
        (fixture / "src").mkdir()
        (fixture / "Cargo.toml").write_text(
            '[package]\nname = "kache-archive-probe"\nversion = "0.0.0"\nedition = "2021"\n'
            '[build-dependencies]\ncc = "=' + cc_version + '"\n[workspace]\n'
        )
        (fixture / "src/lib.rs").write_text("pub fn answer() -> i32 { 7 }\n")
        (fixture / "unit.c").write_text("int archive_probe(void) { return 7; }\n")
        (fixture / "build.rs").write_text(
            'fn main() { cc::Build::new().file("unit.c").debug(false).compile("probe"); }\n'
        )
        environment = dict(os.environ)
        for key in list(environment):
            if key in {"ARFLAGS", "HOST_ARFLAGS", "TARGET_ARFLAGS", "HOST_AR", "TARGET_AR", "CFLAGS", "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS"} or key.startswith(("ARFLAGS_", "AR_", "CFLAGS_")):
                environment.pop(key)
        environment.update(
            AR=archiver, CC=compiler, CARGO_HOME=str(scratch / "cargo-home"),
            RUSTC_WRAPPER="", RUSTC_WORKSPACE_WRAPPER="",
            CARGO_BUILD_RUSTC_WRAPPER="", CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER="",
            CARGO_INCREMENTAL="0", CFLAGS="-g0",
            RUSTC=str(sysroot / "bin/rustc"),
        )
        target = fixture / "target"
        for mode in modes:
            environment["HOST_ARFLAGS"] = mode
            archives = []
            objects = []
            for attempt in range(2):
                clean = subprocess.run(
                    [str(cargo), "clean", "--package", "kache-archive-probe", "--target-dir", str(target)],
                    cwd=fixture, env=environment, text=True, capture_output=True,
                )
                assert clean.returncode == 0, clean.stderr
                build = subprocess.run(
                    [str(cargo), "build", "--target-dir", str(target)],
                    cwd=fixture, env=environment, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                )
                (evidence / f"archive-{cc_version}-{mode}-{attempt}.log").write_text(build.stdout)
                assert build.returncode == 0, build.stdout[-4000:]
                matches = list(target.glob("debug/build/kache-archive-probe-*/out/libprobe.a"))
                assert len(matches) == 1, matches
                archives.append(matches[0].read_bytes())
                object_files = sorted(matches[0].parent.glob("*.o"))
                assert len(object_files) == 1, object_files
                objects.append(object_files[0].read_bytes())
                if attempt == 0:
                    time.sleep(1.2)
            assert objects[0] == objects[1], (cc_version, mode, "object code changed")
            if mode == "U":
                assert archives[0] != archives[1], "nondeterministic control did not change"
                assert all(value > 0 for value in dates(archives[0]))
            else:
                assert archives[0] == archives[1], (cc_version, "HOST_ARFLAGS=D did not stabilize the archive")
                assert all(value == 0 for value in dates(archives[0]))
            archive_results.append({
                "cc": cc_version, "HOST_ARFLAGS": mode,
                "identical_objects": True, "identical_archives": archives[0] == archives[1],
                "dates": [dates(value) for value in archives],
                "sha256": [hashlib.sha256(value).hexdigest() for value in archives],
            })
            print(f"ARCHIVE cc={cc_version} HOST_ARFLAGS={mode} verified", flush=True)
(evidence / "archive-recipe-results.json").write_text(json.dumps(archive_results, indent=2) + "\n")
