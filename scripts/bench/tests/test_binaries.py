"""Measured installations keep the package's declared companion binaries."""

import contextlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from bench.binaries import stage
from bench import gate_local


def metadata(*binaries):
    return {"packages": [{"name": "kache", "targets": [
        {"name": binary, "kind": ["bin"]} for binary in binaries
    ] + [{"name": "library", "kind": ["lib"]}]}]}


@unittest.skipUnless(os.name == "posix", "the staging fixtures execute shell scripts")
class BinaryStagingTests(unittest.TestCase):
    def exercise(self, companions, local=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "target" / "release"
            source.mkdir(parents=True)
            frontend = source / "kache"
            frontend.write_text(
                '#!/bin/sh\nexec "$(dirname "$0")/kache-engine" "$@"\n'
                if companions else '#!/bin/sh\nprintf "legacy\\n"\n'
            )
            frontend.chmod(0o755)
            # A previous checkout may leave an engine in Cargo's target directory.
            engine = source / "kache-engine"
            engine.write_text('#!/bin/sh\nprintf "paired\\n"\n')
            engine.chmod(0o755)
            for name in companions:
                if name != "kache-engine":
                    (source / name).write_text("another companion")
            declared = metadata("kache", *companions)
            destination = root / "measured"
            if local:
                @contextlib.contextmanager
                def built(*args, **kwargs):
                    yield root

                result = subprocess.CompletedProcess([], 0, stdout=json.dumps(declared))
                with patch.object(gate_local, "at_commit", built):
                    with patch.object(gate_local.subprocess, "run", return_value=result):
                        gate_local.build("commit", "kache", "kache", destination / "kache", "head")
            else:
                subprocess.run(
                    ["python3", str(Path(__file__).parents[1] / "binaries.py"),
                     str(source), str(destination)],
                    input=json.dumps(declared), text=True, check=True,
                )
            self.assertEqual({path.name for path in destination.iterdir()}, {"kache", *companions})
            output = subprocess.check_output([str(destination / "kache"), "--version"], text=True)
            self.assertEqual(output, "paired\n" if companions else "legacy\n")

    def test_legacy_build_ignores_a_stale_engine(self):
        self.exercise([])

    def test_split_build_keeps_an_executable_engine(self):
        self.exercise(["kache-engine"])

    def test_all_declared_companions_are_staged(self):
        self.exercise(["kache-engine", "kache-helper"])

    def test_local_gate_keeps_the_pair(self):
        self.exercise(["kache-engine"], local=True)

    def test_a_declared_missing_companion_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "kache").write_text("frontend")
            with self.assertRaises(FileNotFoundError):
                stage(metadata("kache", "kache-engine"), root, root / "measured")
