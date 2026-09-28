#!/usr/bin/env python3
"""Exercise real compiler routing and shard inspection on bind mounts/drive letters."""

import argparse
import contextlib
import errno
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile


def run(argv, **kwargs):
    result = subprocess.run([str(a) for a in argv], capture_output=True, text=True,
                            timeout=120, **kwargs)
    if result.returncode:
        raise RuntimeError(f"{argv}: {result.returncode}\n{result.stdout}\n{result.stderr}")
    return result.stdout


@contextlib.contextmanager
def volumes(root, plain):
    paths = []
    cleanup = []
    try:
        for index in range(2):
            backing = root / f"backing-{index}"
            backing.mkdir()
            if plain:
                paths.append(backing)
            elif os.name == "nt":
                letter = next(c for c in "ZYXWVUTSRQPONMLKJIHGFED" if not Path(f"{c}:/").exists())
                run(["subst", f"{letter}:", backing])
                paths.append(Path(f"{letter}:/"))
                cleanup.append(["subst", f"{letter}:", "/D"])
            elif sys.platform == "linux":
                mount = root / f"mount-{index}"
                mount.mkdir()
                run(["sudo", "-n", "mount", "--bind", backing, mount])
                paths.append(mount)
                cleanup.append(["sudo", "-n", "umount", mount])
            else:
                raise RuntimeError("use --plain outside Linux and Windows")
        if sys.platform == "linux" and not plain:
            source, destination = paths[0] / "link-source", paths[1] / "link-target"
            source.write_bytes(b"bind mount probe")
            try:
                os.link(source, destination)
            except OSError as error:
                assert error.errno == errno.EXDEV, error
            else:
                raise AssertionError("the fixture must exercise separate bind mounts")
            finally:
                source.unlink()
                destination.unlink(missing_ok=True)
        yield paths
    finally:
        for command in reversed(cleanup):
            run(command)


def check(binary, rustc, root, mounts):
    stores = [mount / "cache" for mount in mounts]
    config = root / "config.toml"
    config.write_text(
        "[cache]\nlocal_store = " + json.dumps(str(root / "main"))
        + "\nruntime_dir = " + json.dumps(str(root / "runtime"))
        + "\nlocal_only = true\nscheduler = false\ndaemon_publish = false\n"
        + "[cache.volumes]\n"
        + "\n".join(json.dumps(str(mount)) + " = " + json.dumps(str(store))
                    for mount, store in zip(mounts, stores)) + "\n"
    )
    env = {k: v for k, v in os.environ.items()
           if not k.startswith("KACHE_") and k not in ("RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER")}
    env.update(KACHE_CONFIG=str(config), KACHE_HOST_CONFIG="", KACHE_FALLBACK="off")

    def kache(*args, cwd=root):
        return run([binary, *args], env=env, cwd=cwd)

    try:
        for index, mount in enumerate(mounts):
            build = mount / "build"
            build.mkdir()
            source = build / "lib.rs"
            source.write_text(f"pub fn value() -> u32 {{ {index + 42} }}\n")
            output = build / "out"
            output.mkdir()
            args = [rustc, "--crate-name", f"shard_{index}", "--crate-type", "rlib",
                    "--emit=metadata,dep-info", source, "--out-dir", output]
            kache(*args, cwd=build)
            for artifact in output.iterdir():
                if os.name == "nt":
                    artifact.chmod(0o600)
                artifact.unlink()
            kache(*args, cwd=build)
            assert list(output.glob("*.rmeta")), "warm restore produced no metadata"

            # The same artifact must have independent physical storage in both
            # shards. Distinct artifacts alone cannot detect cross-store links.
            common = build / "common.rs"
            common.write_text("pub fn value() -> u32 { 42 }\n")
            kache(rustc, "--crate-name", "shard_common", "--crate-type", "rlib",
                  "--emit=metadata", "--remap-path-prefix", f"{build}=/src",
                  common, "--out-dir", output, cwd=build)

        entries = json.loads(kache("list", "--json"))
        assert {e["crate_name"] for e in entries["entries"]} == {"shard_0", "shard_1", "shard_common"}, entries
        for index, store in enumerate(stores):
            row = next(e for e in entries["entries"] if e["crate_name"] == f"shard_{index}")
            assert [os.path.normcase(p) for p in row["store_dirs"]] == [os.path.normcase(str(store))], row
            with contextlib.closing(sqlite3.connect(store / "index.db")) as db:
                assert db.execute("SELECT count(*) FROM entries WHERE committed=1").fetchone()[0] == 2
            why = json.loads(kache("why-miss", f"shard_{index}", "--json"))
            assert why["stored_entries"] == 1, why
            assert [os.path.normcase(p) for p in why["store_dirs"]] == [os.path.normcase(str(store))], why
            doctor = json.loads(kache("doctor", "--json", cwd=mounts[index] / "build"))
            assert all(c["pass"] for c in doctor["checks"] if c["label"] == "Volume store"), doctor
            layout = next(c for c in doctor["checks"] if c["label"] == "Link layout")
            assert os.path.normcase(str(store)) in os.path.normcase(layout["detail"]), layout
            if sys.platform == "linux":
                assert layout["pass"], layout

        # Each physical store owns its blobs, even when both mounts share a disk.
        identities = []
        hashes = []
        for store in stores:
            blobs = [p for p in (store / "store" / "blobs").rglob("*") if p.is_file()]
            identities.append({(p.stat().st_dev, p.stat().st_ino) for p in blobs})
            hashes.append({p.name for p in blobs})
        assert all(identities), "the fixture produced no blobs"
        assert hashes[0] & hashes[1], "the fixture must store identical content in both shards"
        assert identities[0].isdisjoint(identities[1]), "shards share blob inodes"
        stats = json.loads(kache("stats", "--json"))
        assert stats["entries"] == len(entries["entries"]), stats
        assert len(stats["stores"]) == 3, stats
        assert stats["stores"][0]["entries"] == 0, stats
        report = json.loads(kache("report", "--format", "json"))
        assert report["storage"]["store_entries"] == len(entries["entries"]), report["storage"]
        assert report["storage"]["logical_bytes"] == sum(s["bytes"] for s in stats["stores"])
        assert report["summary"]["local_hits"] >= 2, report["summary"]
        kache("doctor", "--verify")
        print("Volume routing, warm hits, independent blobs, stats, report, list and doctor passed.")
    finally:
        # Only this fixture's runtime/socket is selected by the isolated config.
        subprocess.run([binary, "daemon", "stop"], env=env, cwd=root,
                       capture_output=True, timeout=30, check=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--plain", action="store_true", help="smoke-test ordinary directories without mounts")
    args = parser.parse_args()
    binary = args.binary.resolve()
    rustc = shutil.which("rustc")
    if not rustc:
        raise RuntimeError("rustc is required")
    with tempfile.TemporaryDirectory(prefix="kache-vol-") as directory:
        root = Path(directory)
        with volumes(root, args.plain) as mounts:
            check(binary, rustc, root, mounts)


if __name__ == "__main__":
    main()
