"""Check that bounded regressions catch each launcher protection being removed.

Run only on the disposable hosted preflight runner, after `just check`.
"""

import os
from pathlib import Path
import re
import subprocess


ROOT = Path.cwd()
EVIDENCE = Path(os.environ["EVIDENCE_DIR"])
EVIDENCE.mkdir(parents=True, exist_ok=True)
ENV = {**os.environ, "RUSTC_WRAPPER": "", "RUSTC_WORKSPACE_WRAPPER": ""}


def run_test(test, label, must_fail=False):
    result = subprocess.run(
        ["cargo", "test", "--locked", "--offline", "-p", "kache", "--lib", test,
         "--", "--exact", "--nocapture"],
        cwd=ROOT,
        env=ENV,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=300,
        check=False,
    )
    output = result.stdout
    EVIDENCE.joinpath(f"{label}.log").write_text(output)
    print(output, flush=True)
    assert "running 1 test" in output, f"{label}: target test did not run"
    expected = "FAILED" if must_fail else "ok"
    assert re.search(rf"test {re.escape(test)} \.\.\. {expected}\b", output), (
        f"{label}: target test did not report {expected}"
    )
    assert (result.returncode != 0) == must_fail, (
        f"{label}: unexpected exit status {result.returncode}"
    )


CONTROLS = [
    (
        "fallback",
        "launcher/build_script.rs",
        "if invoked.ends_with(REAL_SUFFIX) {",
        "if false {",
        "build_script::tests::a_launcher_without_kache_stops_when_the_preserved_script_is_a_launcher",
    ),
    (
        "publication",
        "src/build_script.rs",
        "!is_launcher(staged.path()),",
        "true,",
        "build_script::tests::a_script_snapshot_survives_launcher_installation_and_rejects_launcher_bytes",
    ),
    (
        "namespace",
        "src/cache_key.rs",
        'hasher.update(b"build_script_artifact.v2\\n");',
        'hasher.update(b"");',
        "cache_key::tests::build_script_artifact_recipe_retires_only_launcher_compilations",
    ),
]


for label, filename, before, after, test in CONTROLS:
    path = ROOT / filename
    original = path.read_bytes()
    text = original.decode()
    assert text.count(before) == 1, f"{label}: expected one mutation site"
    run_test(test, f"{label}-baseline")
    try:
        path.write_text(text.replace(before, after, 1))
        run_test(test, f"{label}-negative", must_fail=True)
    finally:
        path.write_bytes(original)
    run_test(test, f"{label}-restored")

print("All three negative controls failed at the intended assertion; restored tests passed.")
