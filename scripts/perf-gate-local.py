#!/usr/bin/env python3
"""Entry point for `bench.gate_local`; the package lives in scripts/bench."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from bench.gate_local import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
