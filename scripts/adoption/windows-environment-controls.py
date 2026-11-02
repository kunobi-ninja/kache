"""Check the hidden-environment regression on a disposable Windows runner."""

from pathlib import Path
import os
import subprocess


source = Path("src/daemon_execution_windows.rs")
original = source.read_text(encoding="utf-8")
evidence = Path(os.environ["EVIDENCE_DIR"])
test = "daemon::execution::tests::preparation_preserves_windows_hidden_environment_entries"
command = [
    "cargo", "test", "--locked", "-p", "kache", "--lib", test,
    "--", "--exact", "--nocapture",
]
fixed = '''                !key.is_empty()
                    && (key[0] != u16::from(b'=') || key.len() > 1)
                    && !key[1..].contains(&u16::from(b'='))'''
regression = '''                !key.is_empty()
                    && (!key.contains(&u16::from(b'='))
                        || (key.len() == 3
                            && key[0] == u16::from(b'=')
                            && key[2] == u16::from(b':')
                            && u8::try_from(key[1]).is_ok_and(|drive| drive.is_ascii_alphabetic())))'''
assert original.count(fixed) == 1, "control target changed"
try:
    source.write_text(original.replace(fixed, regression, 1), encoding="utf-8")
    result = subprocess.run(command, encoding="utf-8", errors="replace", stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    (evidence / "hidden-env-regression.log").write_text(result.stdout, encoding="utf-8")
    assert result.returncode != 0, "regression escaped"
    assert "running 1 test" in result.stdout, "test did not execute"
    assert f"test {test} ... FAILED" in result.stdout, "wrong failure"
    assert "invalid compiler environment key" in result.stdout, "wrong assertion"
    print("CAUGHT hidden-environment regression", flush=True)
finally:
    source.write_text(original, encoding="utf-8")
result = subprocess.run(command, encoding="utf-8", errors="replace", stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
(evidence / "hidden-env-restored.log").write_text(result.stdout, encoding="utf-8")
assert result.returncode == 0, ("restored candidate failed", result.stdout[-4000:])
assert f"test {test} ... ok" in result.stdout, "restored test did not execute"
print("RESTORED hidden-environment candidate", flush=True)
