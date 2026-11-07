#!/usr/bin/env python3
"""Print a same-source legacy-admission control patch; do not apply or build it."""

import difflib
import sys
from pathlib import Path


def control_patch(root):
    replacements = {
        "src/scheduler/pressure.rs": (
            '''    match now() {
        Some(level) => shared(root, pool, level, super::queue::millis(SystemTime::now())),
        None => pool,
    }
}''',
            '''    let level = now();
    // Keep observation and persistence costs in both benchmark arms.
    if let Some(level) = level {
        let _ = shared(root, pool, level, super::queue::millis(SystemTime::now()));
    }
    match level {
        Some(Level::Warning | Level::Critical) => 1,
        _ => pool,
    }
}''',
        ),
        "src/scheduler.rs": (
            '''    fn keep(&self, mut slots: Vec<(u32, StoreLock)>, weight: u32) -> Permit {
        let keep = weight.max(1) as usize;
        if slots.len() > keep {
            slots.truncate(keep);
            // A test may be waiting for exactly these slots.
            permit_wait::notify(&self.root);
        }
        Permit {''',
            '''    fn keep(&self, slots: Vec<(u32, StoreLock)>, _weight: u32) -> Permit {
        // The legacy policy held the full ordinary request until completion.
        Permit {''',
        ),
    }
    chunks = []
    for name, (before, after) in replacements.items():
        source = (root / name).read_text()
        if source.count(before) != 1:
            raise ValueError(f"{name}: source changed; review the control before benchmarking")
        chunks.extend(difflib.unified_diff(source.splitlines(keepends=True),
                                        source.replace(before, after).splitlines(keepends=True),
                                        fromfile=f"a/{name}", tofile=f"b/{name}"))
    return "".join(chunks)


if __name__ == "__main__":
    print(control_patch(Path(sys.argv[1])), end="")
