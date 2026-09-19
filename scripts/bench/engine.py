"""Running the measuring instrument and finding the tools it measures."""

import os
import shutil
import signal
import subprocess
from pathlib import Path


def run_measurement(command, **kwargs):
    # A timed-out engine can leave compiler children alive. Stop the whole
    # measurement group before its scratch directory is removed.
    with subprocess.Popen(command, start_new_session=True, **kwargs) as process:
        try:
            status = process.wait(timeout=1200)
        except BaseException:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            raise
    if status:
        raise subprocess.CalledProcessError(status, command)


def tool_path(binary):
    """Absolute path of a tool, real binary rather than a mise shim.

    A mise shim is a symlink to the mise binary that dispatches on argv[0].
    Following it gives `.../mise`, which invoked as `mise --start-server`
    fails; ask mise where the tool really is instead.
    """
    found = Path(shutil.which(binary) or binary).absolute()
    if found.is_symlink() and Path(os.path.realpath(found)).stem == "mise":
        real = subprocess.run(
            ["mise", "which", binary], capture_output=True, text=True, check=False
        )
        target = real.stdout.strip()
        if real.returncode == 0 and target:
            return str(Path(target).absolute())
        return str(found)
    return str(found.resolve())


def installed(binary):
    return Path(binary).is_file() or shutil.which(binary) is not None
