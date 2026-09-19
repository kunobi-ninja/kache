#!/usr/bin/env python3
"""Entry point for `bench.phases`; the package lives in scripts/bench."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from bench.phases import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
