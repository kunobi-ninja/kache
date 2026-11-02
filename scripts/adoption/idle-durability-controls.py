"""Check the pinned durability regression on a disposable hosted runner."""

from pathlib import Path
import os
import subprocess


source = Path("crates/kache-store/src/store.rs")
original = source.read_text()
evidence = Path(os.environ["EVIDENCE_DIR"])
test = "store::tests::durability_poll_and_batch_use_pending_index_after_open_and_upgrade"
command = [
    "cargo", "test", "--locked", "-p", "kache-store", "--lib", test,
    "--", "--exact", "--nocapture",
]
index = '''    db.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_entries_durability_pending
         ON entries(created_at, cache_key)
         WHERE committed = 1 AND durable = 0;",
    )?;
'''
controls = [
    ("missing-index", index, "", "upgrade=false:"),
    (
        "missing-upgrade",
        "const INDEX_SCHEMA_GENERATION: i64 = 14;",
        "const INDEX_SCHEMA_GENERATION: i64 = 13;",
        "upgrade=true:",
    ),
    (
        "newest-first",
        "ORDER BY created_at LIMIT ?1",
        "ORDER BY created_at DESC LIMIT ?1",
        'store.entry_is_durable("pending-old")',
    ),
]
for name, before, after, expected in controls:
    assert original.count(before) == 1, (name, "control target changed")
    try:
        source.write_text(original.replace(before, after, 1))
        result = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        (evidence / f"{name}.log").write_text(result.stdout)
        assert result.returncode != 0, (name, "regression escaped")
        assert "running 1 test" in result.stdout, (name, "test did not execute")
        assert f"test {test} ... FAILED" in result.stdout, (name, "wrong failure")
        assert expected in result.stdout, (name, "wrong assertion")
        print(f"CAUGHT {name}", flush=True)
    finally:
        source.write_text(original)
    result = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    (evidence / f"{name}-restored.log").write_text(result.stdout)
    assert result.returncode == 0, (name, "restored candidate failed", result.stdout[-4000:])
    assert f"test {test} ... ok" in result.stdout, (name, "restored test did not execute")
    print(f"RESTORED {name}", flush=True)
