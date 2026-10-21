import hashlib
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import unittest
import warnings
import zipfile

from inspect_archives import inspect_archives


def pe(machine):
    data = bytearray(128)
    data[:2] = b"MZ"
    struct.pack_into("<I", data, 60, 64)
    data[64:68] = b"PE\0\0"
    struct.pack_into("<H", data, 68, machine)
    return bytes(data)


def checksum(path):
    value = hashlib.sha256(path.read_bytes()).hexdigest()
    path.with_name(path.name + ".sha256").write_text(f"{value}  {path.name}\n")


def write_zip(directory, target, files):
    path = directory / f"kache-{target}-pc-windows-msvc.zip"
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        with zipfile.ZipFile(path, "w") as archive:
            for name, data in files:
                archive.writestr(name, data)
    checksum(path)
    return path


def write_assets(directory, legacy=False):
    directory.mkdir(parents=True, exist_ok=True)
    for target, machine in [("x86_64", 0x8664), ("aarch64", 0xAA64)]:
        files = [("kache.exe", pe(machine))]
        if not legacy:
            files.append(("kache-engine.exe", pe(machine)))
        write_zip(directory, target, files)
        if legacy:
            bare = directory / f"kache-{target}-pc-windows-msvc.exe"
            bare.write_bytes(pe(machine))
            checksum(bare)


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        write_assets(self.root)

    def test_paired_archives_select_zip_urls_and_exact_checksums(self):
        result = inspect_archives(self.root, "1.2.3")
        self.assertEqual(result["layout"], "paired")
        self.assertEqual([row["architecture"] for row in result["installers"]], ["x64", "arm64"])
        for row in result["installers"]:
            self.assertTrue(row["url"].startswith("https://github.com/kunobi-ninja/kache/releases/download/v1.2.3/"))
            self.assertTrue(row["url"].endswith(".zip"))
            self.assertEqual(row["sha256"], hashlib.sha256((self.root / row["url"].split("/")[-1]).read_bytes()).hexdigest())

    def test_legacy_assets_match_their_zip_payloads(self):
        write_assets(self.root, legacy=True)
        result = inspect_archives(self.root, "0.15.0-rc.3")
        self.assertEqual(result["layout"], "legacy")
        self.assertTrue(all(row["url"].endswith(".exe") for row in result["installers"]))
        bare = self.root / "kache-x86_64-pc-windows-msvc.exe"
        bare.write_bytes(pe(0x8664) + b"different executable")
        checksum(bare)
        with self.assertRaisesRegex(ValueError, "differs from ZIP"):
            inspect_archives(self.root, "0.15.0-rc.3")

    def test_selection_precedes_legacy_download_but_validation_requires_it(self):
        write_assets(self.root, legacy=True)
        for path in self.root.glob("*.exe*"):
            path.unlink()
        self.assertEqual(inspect_archives(self.root, "0.15.0-rc.3", select=True)["layout"], "legacy")
        with self.assertRaises(FileNotFoundError):
            inspect_archives(self.root, "0.15.0-rc.3")

    def test_architectures_must_have_the_same_layout(self):
        write_zip(self.root, "x86_64", [("kache.exe", pe(0x8664))])
        with self.assertRaisesRegex(ValueError, "different installation layouts"):
            inspect_archives(self.root, "1.2.3", select=True)

    def test_companion_architecture_is_checked(self):
        write_zip(self.root, "aarch64", [("kache.exe", pe(0xAA64)), ("kache-engine.exe", pe(0x8664))])
        with self.assertRaisesRegex(ValueError, "kache-engine.exe has the wrong architecture"):
            inspect_archives(self.root, "1.2.3")

    def test_incomplete_ambiguous_and_unexpected_payloads_are_rejected(self):
        for files, message in [
            ([("kache-engine.exe", pe(0x8664))], "missing kache.exe"),
            ([("kache.exe", pe(0x8664)), ("kache.exe", pe(0x8664))], "duplicate ZIP"),
            ([("kache.exe", pe(0x8664)), ("nested/kache-engine.exe", pe(0x8664))], "unexpected ZIP"),
            ([("kache.exe", pe(0x8664)), ("../kache-engine.exe", pe(0x8664))], "unexpected ZIP"),
        ]:
            with self.subTest(message=message):
                write_zip(self.root, "x86_64", files)
                with self.assertRaisesRegex(ValueError, message):
                    inspect_archives(self.root, "1.2.3")

    def test_checksum_mismatch_and_wrong_filename_are_rejected(self):
        path = self.root / "kache-x86_64-pc-windows-msvc.zip"
        sidecar = path.with_name(path.name + ".sha256")
        for text, message in [
            ("bad checksum", "invalid checksum"),
            ("0" * 64, "checksum mismatch"),
            (hashlib.sha256(path.read_bytes()).hexdigest() + "  other.zip", "another file"),
        ]:
            with self.subTest(message=message):
                sidecar.write_text(text)
                with self.assertRaisesRegex(ValueError, message):
                    inspect_archives(self.root, "1.2.3")

    def test_invalid_pe_headers_are_rejected(self):
        for data in [b"not PE", b"MZ" + bytes(62), pe(0xAA64)]:
            with self.subTest(data=data[:4]):
                write_zip(self.root, "x86_64", [("kache.exe", data), ("kache-engine.exe", pe(0x8664))])
                with self.assertRaises(ValueError):
                    inspect_archives(self.root, "1.2.3")

    def test_select_cli_only_emits_bounded_github_outputs(self):
        result = subprocess.run([sys.executable, str(Path(__file__).with_name("inspect_archives.py")), str(self.root), "--version", "1.2.3-beta.1", "--select"], capture_output=True, text=True, check=True)
        rows = dict(line.split("=", 1) for line in result.stdout.splitlines())
        self.assertEqual(set(rows), {"layout", "url_x64", "url_arm64"})
        self.assertEqual(rows["layout"], "paired")
        self.assertEqual(rows["url_arm64"], "https://github.com/kunobi-ninja/kache/releases/download/v1.2.3-beta.1/kache-aarch64-pc-windows-msvc.zip")
        for version in ["../1", "1.2.3\n", "1.2.3;echo bad", "1.2.3/next"]:
            with self.subTest(version=version), self.assertRaisesRegex(ValueError, "invalid release version"):
                inspect_archives(self.root, version, select=True)


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "fixture":
        write_assets(Path(sys.argv[2]), legacy=sys.argv[3] == "legacy")
    else:
        unittest.main()
