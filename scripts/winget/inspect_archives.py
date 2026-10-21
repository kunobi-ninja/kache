#!/usr/bin/env python3
"""Check Windows release payloads before generating Winget manifests."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import struct
import zipfile


TARGETS = {"x64": ("x86_64", 0x8664), "arm64": ("aarch64", 0xAA64)}
PRIMARY = "kache.exe"
COMPANION = "kache-engine.exe"


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def verify_checksum(path):
    value = digest(path)
    checksum = path.with_name(path.name + ".sha256").read_text().strip().split()
    if not checksum or not re.fullmatch(r"[a-fA-F0-9]{64}", checksum[0]):
        raise ValueError(f"invalid checksum for {path.name}")
    if len(checksum) > 2 or (len(checksum) == 2 and checksum[1].lstrip("*") != path.name):
        raise ValueError(f"checksum names another file: {path.name}")
    if value != checksum[0].lower():
        raise ValueError(f"checksum mismatch for {path.name}")
    return value


def verify_pe(stream, size, machine, label):
    header = stream.read(64)
    if len(header) != 64 or header[:2] != b"MZ":
        raise ValueError(f"{label} is not a Windows executable")
    offset = struct.unpack_from("<I", header, 60)[0]
    if offset < 64 or offset > size - 6:
        raise ValueError(f"{label} has an invalid PE header offset")
    stream.seek(offset)
    pe = stream.read(6)
    if pe[:4] != b"PE\0\0" or len(pe) != 6:
        raise ValueError(f"{label} has an invalid PE signature")
    if struct.unpack_from("<H", pe, 4)[0] != machine:
        raise ValueError(f"{label} has the wrong architecture")


def inspect_archives(directory, version, select=False):
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?", version):
        raise ValueError("invalid release version")
    base = f"https://github.com/kunobi-ninja/kache/releases/download/v{version}"
    results = []
    layouts = set()
    for architecture, (target, machine) in TARGETS.items():
        stem = f"kache-{target}-pc-windows-msvc"
        path = directory / (stem + ".zip")
        archive_hash = verify_checksum(path)
        with zipfile.ZipFile(path) as archive:
            entries = archive.infolist()
            names = [entry.filename for entry in entries]
            if len(names) != len(set(names)):
                raise ValueError(f"duplicate ZIP members in {path.name}")
            if PRIMARY not in names:
                raise ValueError(f"{path.name} is missing {PRIMARY}")
            layout = "paired" if COMPANION in names else "legacy"
            allowed = {PRIMARY, COMPANION, PRIMARY + ".asc", COMPANION + ".asc"}
            if any(name not in allowed for name in names):
                raise ValueError(f"unexpected ZIP member in {path.name}")
            required = [PRIMARY, COMPANION] if layout == "paired" else [PRIMARY]
            for name in required:
                info = archive.getinfo(name)
                with archive.open(info) as stream:
                    verify_pe(stream, info.file_size, machine, f"{path.name}/{name}")
            if archive.testzip() is not None:
                raise ValueError(f"corrupt ZIP member in {path.name}")
            primary_hash = hashlib.sha256(archive.read(PRIMARY)).hexdigest()
        layouts.add(layout)
        if layout == "paired":
            installer_path, installer_hash = path, archive_hash
        elif select:
            installer_path, installer_hash = directory / (stem + ".exe"), None
        else:
            installer_path = directory / (stem + ".exe")
            installer_hash = verify_checksum(installer_path)
            with installer_path.open("rb") as stream:
                verify_pe(stream, installer_path.stat().st_size, machine, installer_path.name)
            if installer_hash != primary_hash:
                raise ValueError(f"bare executable differs from ZIP payload: {installer_path.name}")
        results.append({
            "architecture": architecture,
            "url": f"{base}/{installer_path.name}",
            "sha256": installer_hash,
        })
    if len(layouts) != 1:
        raise ValueError("Windows architectures have different installation layouts")
    return {"layout": layouts.pop(), "installers": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--version", required=True)
    parser.add_argument("--select", action="store_true", help="Select URLs before downloading legacy EXEs")
    args = parser.parse_args()
    result = inspect_archives(args.directory, args.version, args.select)
    if args.select:
        print(f"layout={result['layout']}")
        for installer in result["installers"]:
            print(f"url_{installer['architecture']}={installer['url']}")
    else:
        print(json.dumps(result))


if __name__ == "__main__":
    main()
