#!/usr/bin/env python3
"""Exercise the shipped AUR package functions with both release layouts."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
ENGINE = """#!/bin/sh
if [ "$1" = completions ]; then
  printf 'completion:%s\\n' "$2"
else
  printf 'package fixture\\n'
fi
"""
FRONTEND = """#!/bin/sh
exec "$(dirname "$0")/kache-engine" "$@"
"""


class AurInstallationTests(unittest.TestCase):
    def install(self, package, companion):
        with tempfile.TemporaryDirectory(prefix="kache aur ") as directory:
            root = Path(directory)
            source = root / "source"
            destination = root / "installed"
            work = source / "kache" if package == "kache-git" else source
            binaries = work / "target/release" if package == "kache-git" else work
            binaries.mkdir(parents=True)
            (work / "LICENSE").write_text("fixture license\n")
            frontend = binaries / "kache"
            frontend.write_text(FRONTEND if companion else ENGINE)
            frontend.chmod(0o755)
            if companion:
                engine = binaries / "kache-engine"
                engine.write_text(ENGINE)
                engine.chmod(0o755)
            environment = dict(os.environ, srcdir=str(source), pkgdir=str(destination))
            subprocess.run(
                ["bash", "-euc", 'source "$1"; package', "package-test",
                 str(ROOT / "packaging/aur" / package / "PKGBUILD")],
                env=environment,
                check=True,
                capture_output=True,
                text=True,
            )
            installed = destination / "usr/bin"
            expected = {"kache", "kache-engine"} if companion else {"kache"}
            self.assertEqual({file.name for file in installed.iterdir()}, expected)
            result = subprocess.run(
                [str(installed / "kache"), "--version"],
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.stdout, "package fixture\n")
            for shell, relative in [
                ("bash", "bash-completion/completions/kache"),
                ("zsh", "zsh/site-functions/_kache"),
                ("fish", "fish/vendor_completions.d/kache.fish"),
                ("elvish", "elvish/lib/kache.elv"),
            ]:
                self.assertEqual(
                    (destination / "usr/share" / relative).read_text(),
                    f"completion:{shell}\n",
                )
            self.assertTrue((destination / "usr/lib/kache/.kache-shims").is_file())

    def test_legacy_archives_install_and_run(self):
        for package in ["kache-bin", "kache-git"]:
            with self.subTest(package=package):
                self.install(package, companion=False)

    def test_companion_archives_install_and_run(self):
        for package in ["kache-bin", "kache-git"]:
            with self.subTest(package=package):
                self.install(package, companion=True)


if __name__ == "__main__":
    unittest.main()
