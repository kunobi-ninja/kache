"""Stage the binaries declared by the package being measured."""

import json
from pathlib import Path
import shutil
import sys


def stage(metadata, source, destination, package="kache"):
    selected = next(item for item in metadata["packages"] if item["name"] == package)
    binaries = [target["name"] for target in selected["targets"] if "bin" in target["kind"]]
    if not binaries:
        raise ValueError("the measured package declares no binaries")
    source, destination = Path(source), Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    suffix = ".exe" if sys.platform == "win32" else ""
    for binary in binaries:
        name = binary + suffix
        shutil.copy2(source / name, destination / name)


if __name__ == "__main__":
    stage(json.load(sys.stdin), sys.argv[1], sys.argv[2])
